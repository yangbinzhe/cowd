use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use harness_contract::tool::ToolExposureProjection;
use model_protocol::provider_capability::{CapabilityState, ProviderCapabilityProfile};
use model_protocol::provider_config::{ParallelToolCallsMode, ProviderProtocol};
use model_protocol::usage::TokenUsage;
use provider::{
    ApiError, ContentBlockDelta, ImageSource, InputContentBlock, InputMessage, MessageRequest,
    MessageResponse, OutputContentBlock, ProviderClient, StreamEvent as ApiStreamEvent, ToolChoice,
    ToolDefinition, ToolResultContentBlock,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ApiClient, ApiClientStream, ApiRequest, AssistantEvent, ContentBlock, ConversationMessage,
    MessageRole, ProviderContextInventory, RuntimeError,
};

use crate::provider_registry::{ProviderRegistry, ProviderRegistrySnapshot};

const PROVIDER_EVENT_QUEUE_CAPACITY: usize = 64;
const MAX_COALESCED_TEXT_BYTES: usize = 64 * 1024;
const DEFAULT_PROVIDER_TEMPLATE_CACHE_ENTRIES: usize = 64;

pub use provider::OutputContentBlock as ProviderOutputContentBlock;
pub use provider::ToolDefinition as ProviderToolDefinition;

fn request_reasoning_effort(
    model: &str,
    protocol_capabilities: &[String],
    request_override: Option<String>,
    configured: Option<String>,
) -> Option<String> {
    let lowered = model.to_ascii_lowercase();
    let canonical = lowered.rsplit('/').next().unwrap_or(lowered.as_str());
    // This is a configured wire capability, not a model-family heuristic.
    // Providers can introduce a new hybrid-thinking model without requiring a
    // Runtime release: the registry snapshot travels in the request profile.
    let supports_hybrid_thinking_control = protocol_capabilities
        .iter()
        .any(|capability| capability == "openai_compat_enable_thinking");
    let deepseek_v4 = matches!(canonical, "deepseek-v4-pro" | "deepseek-v4-flash");
    if deepseek_v4 {
        return request_override
            .filter(|effort| matches!(effort.as_str(), "high" | "max"))
            .or_else(|| {
                configured.map(|effort| match effort.as_str() {
                    "low" | "medium" => "high".to_string(),
                    _ => effort,
                })
            });
    }
    request_override
        .filter(|effort| effort == "none" && supports_hybrid_thinking_control)
        .or(configured)
}

struct PreparationSourceCounter(usize);
impl std::io::Write for PreparationSourceCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("prepared source byte overflow"))?;
        if self.0 > provider::MAX_PREPARED_WIRE_BYTES {
            return Err(std::io::Error::other("prepared source exceeds body limit"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderPreparationFailure {
    SourceChanged,
    ProfileChanged,
    Consumed,
    MissingResourceOwner,
}

/// One original request's frozen preparation and resource handoff. Cloning this
/// handle does not allocate another body or admit another Provider slot.
#[derive(Clone, Default)]
pub struct ProviderRequestPreparation {
    prepared: Option<Arc<PreparedRuntimeRequest>>,
    skill_tool_refs: std::collections::BTreeSet<String>,
    dispatch_lease: Option<Arc<crate::execution_core::graph::ExecutionResourceLease>>,
    cancellation: Option<crate::CancellationToken>,
    policy_source: Option<(crate::permissions::SessionExecutionPolicyControl, u64)>,
    preparation_scope: Option<(crate::execution_core::graph::ExecutionServiceClass, String)>,
    skill_source: Option<(
        Arc<dyn crate::RuntimeSkillInstructionSource>,
        Vec<harness_contract::skill::SkillInstructionPin>,
    )>,
}
impl std::fmt::Debug for ProviderRequestPreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderRequestPreparation")
            .field(
                "prepared",
                &self
                    .prepared
                    .as_ref()
                    .map(|p| p.entry.request_context.request_id.as_str()),
            )
            .field(
                "provider_lease",
                &self.dispatch_lease.as_ref().map(|lease| lease.id()),
            )
            .finish()
    }
}
impl PartialEq for ProviderRequestPreparation {
    fn eq(&self, other: &Self) -> bool {
        match (&self.prepared, &other.prepared) {
            (Some(a), Some(b)) if !Arc::ptr_eq(a, b) => return false,
            (None, None) | (Some(_), Some(_)) => {}
            _ => return false,
        }
        self.dispatch_lease.as_ref().map(|l| l.id())
            == other.dispatch_lease.as_ref().map(|l| l.id())
    }
}
impl Eq for ProviderRequestPreparation {}
impl ProviderRequestPreparation {
    pub fn invalidate(&mut self) {
        self.prepared = None;
    }
    pub(crate) fn bind_preparation_scope(
        &mut self,
        class: crate::execution_core::graph::ExecutionServiceClass,
        key: &str,
    ) {
        self.preparation_scope = Some((class, key.to_owned()));
    }
    pub(crate) fn bind_policy_source(
        &mut self,
        source: crate::permissions::SessionExecutionPolicyControl,
        revision: u64,
    ) {
        self.policy_source = Some((source, revision));
    }
    pub(crate) fn bind_skill_tool_refs(&mut self, tools: std::collections::BTreeSet<String>) {
        self.skill_tool_refs = tools;
    }
    fn retain_actual_skill_pins(
        &mut self,
        prompt_pins: &[harness_contract::skill::SkillInstructionPin],
        tool_ids: &std::collections::BTreeSet<String>,
    ) -> Result<(), RuntimeError> {
        let Some((_, pins)) = self.skill_source.as_mut() else {
            if !prompt_pins.is_empty()
                || self
                    .skill_tool_refs
                    .iter()
                    .any(|tool| tool_ids.contains(tool))
            {
                return Err(RuntimeError::preparation(
                    ProviderPreparationFailure::SourceChanged,
                    "actual Skill inputs have no original dispatch source",
                ));
            }
            return Ok(());
        };
        if prompt_pins.iter().any(|pin| !pins.contains(pin)) {
            return Err(RuntimeError::preparation(
                ProviderPreparationFailure::SourceChanged,
                "actual Skill packet does not match its dispatch source",
            ));
        }
        pins.retain(|pin| {
            prompt_pins.contains(pin)
                || pin
                    .tool_refs
                    .iter()
                    .any(|tool| self.skill_tool_refs.contains(tool) && tool_ids.contains(tool))
        });
        if pins.is_empty() {
            self.skill_source = None;
        }
        Ok(())
    }
    pub(crate) fn bind_skill_source(
        &mut self,
        source: Arc<dyn crate::RuntimeSkillInstructionSource>,
        pins: Vec<harness_contract::skill::SkillInstructionPin>,
    ) {
        self.skill_source = Some((source, pins));
    }
    pub(crate) fn retain_dispatch_lease(
        &mut self,
        lease: Option<Arc<crate::execution_core::graph::ExecutionResourceLease>>,
    ) {
        self.dispatch_lease = lease;
    }

    pub(crate) fn retain_cancellation(&mut self, cancellation: crate::CancellationToken) {
        self.cancellation = Some(cancellation);
    }
}

struct ProviderDispatchSourceFence {
    registry: Arc<ProviderRegistry>,
    revision: u64,
    transport: Arc<crate::ProviderTransportPool>,
    transport_fingerprint: crate::TransportProfileFingerprint,
    policy: Option<(crate::permissions::SessionExecutionPolicyControl, u64)>,
    skill_source: Option<(
        Arc<dyn crate::RuntimeSkillInstructionSource>,
        Vec<harness_contract::skill::SkillInstructionPin>,
    )>,
}
impl ProviderDispatchSourceFence {
    fn validate(&self) -> Result<(), RuntimeError> {
        if self.registry.revision() != self.revision
            || self
                .transport
                .checkout_default()
                .map_err(|e| RuntimeError::new(e.to_string()))?
                .0
                != self.transport_fingerprint
        {
            return Err(RuntimeError::preparation(
                ProviderPreparationFailure::ProfileChanged,
                "provider profile/transport changed before HTTP; prepare again",
            ));
        }
        if self
            .policy
            .as_ref()
            .is_some_and(|(source, revision)| source.revision() != *revision)
        {
            return Err(RuntimeError::preparation(
                ProviderPreparationFailure::SourceChanged,
                "Session policy changed before HTTP; bind a fresh Step",
            ));
        }
        if let Some((source, pins)) = &self.skill_source {
            source.validate_pins(pins).map_err(|error| {
                RuntimeError::preparation(
                    ProviderPreparationFailure::SourceChanged,
                    format!("Skill approval changed before HTTP: {error}"),
                )
            })?;
        }
        Ok(())
    }
}

struct PreparedRuntimeRequest {
    source: PreparedRuntimeSource,
    entry: ProviderEntry,
    message: MessageRequest,
    wire: provider::PreparedProviderWire,
    consumed: std::sync::atomic::AtomicBool,
    _resident: Option<crate::execution_core::graph::ExecutionResourceLease>,
}
struct PreparedRuntimeSource {
    prompt: crate::PromptAssembly,
    history: crate::HistoryView,
    model: String,
    reasoning: Option<String>,
    budget: crate::context_ledger::RequestBudgetReport,
    inventory: ProviderContextInventory,
    required: bool,
    required_name: Option<String>,
    configured_reasoning: Option<String>,
}
impl PreparedRuntimeSource {
    fn matches(&self, request: &ApiRequest, client: &ProviderRuntimeClient) -> bool {
        self.prompt == request.prompt
            && self.history == request.messages
            && self.model == request.model
            && self.reasoning == request.reasoning_effort_override
            && self.budget == request.budget
            && self.inventory == client.compiled_tool_schema().inventory
            && self.required == client.tool_choice_required
            && self.required_name == client.tool_choice_required_tool_name
            && self.configured_reasoning == client.reasoning_effort
    }
}

#[derive(Clone)]
struct ProviderEntry {
    model: String,
    client: ProviderClient,
    request_context: ProviderRequestContext,
}

#[derive(Clone)]
struct ProviderEntryTemplate {
    model: String,
    client: ProviderClient,
    profile: ResolvedProviderProfile,
    transport_fingerprint: crate::TransportProfileFingerprint,
    diagnostic_send_reason: Option<String>,
}

impl ProviderEntryTemplate {
    fn request_entry(&self) -> ProviderEntry {
        let request_id = uuid::Uuid::new_v4().to_string();
        ProviderEntry {
            model: self.model.clone(),
            client: self.client.clone(),
            request_context: ProviderRequestContext {
                request_id,
                profile: self.profile.clone(),
                transport_fingerprint: self.transport_fingerprint,
                diagnostic_send_reason: self.diagnostic_send_reason.clone(),
                attempt: 1,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProviderTemplateCacheKey {
    registry_revision: u64,
    model: String,
    transport_fingerprint: crate::TransportProfileFingerprint,
}

#[derive(Default)]
struct ProviderTemplateCacheState {
    entries: HashMap<ProviderTemplateCacheKey, ProviderEntryTemplate>,
    lru: VecDeque<ProviderTemplateCacheKey>,
}

/// Host-scoped cache for immutable Provider client templates.
///
/// Request identity and retry state are never cached. Registry revisions and
/// transport fingerprints are part of the key, so reloads and proxy changes
/// cannot reuse a stale route.
pub struct ProviderClientTemplateCache {
    state: std::sync::Mutex<ProviderTemplateCacheState>,
    max_entries: usize,
    hits: AtomicU64,
    builds: AtomicU64,
    evictions: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderClientTemplateCacheStats {
    pub entries: usize,
    pub hits: u64,
    pub builds: u64,
    pub evictions: u64,
}

impl Default for ProviderClientTemplateCache {
    fn default() -> Self {
        Self::new(DEFAULT_PROVIDER_TEMPLATE_CACHE_ENTRIES)
    }
}

impl ProviderClientTemplateCache {
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            state: std::sync::Mutex::new(ProviderTemplateCacheState::default()),
            max_entries: max_entries.max(1),
            hits: AtomicU64::new(0),
            builds: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn resolve(
        &self,
        snapshot: &ProviderRegistrySnapshot,
        transport_pool: &crate::ProviderTransportPool,
        model: &str,
    ) -> Result<ProviderEntry, String> {
        let resolved = model.trim().to_string();
        let (transport_fingerprint, http) = transport_pool
            .checkout_default()
            .map_err(|error| error.to_string())?;
        let key = ProviderTemplateCacheKey {
            registry_revision: snapshot.revision(),
            model: resolved.clone(),
            transport_fingerprint,
        };
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(template) = state.entries.get(&key).cloned() {
                touch_template_lru(&mut state.lru, &key);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(template.request_entry());
            }
        }

        let template = build_provider_template(
            snapshot,
            transport_fingerprint,
            http,
            &resolved,
        )?;
        let entry = template.request_entry();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = state.entries.get(&key).cloned() {
            touch_template_lru(&mut state.lru, &key);
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(existing.request_entry());
        }
        while state.entries.len() >= self.max_entries {
            let Some(oldest) = state.lru.pop_front() else {
                break;
            };
            if state.entries.remove(&oldest).is_some() {
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        state.entries.insert(key.clone(), template);
        state.lru.push_back(key);
        self.builds.fetch_add(1, Ordering::Relaxed);
        Ok(entry)
    }

    #[must_use]
    pub fn stats(&self) -> ProviderClientTemplateCacheStats {
        ProviderClientTemplateCacheStats {
            entries: self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entries
                .len(),
            hits: self.hits.load(Ordering::Relaxed),
            builds: self.builds.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

fn touch_template_lru(
    lru: &mut VecDeque<ProviderTemplateCacheKey>,
    key: &ProviderTemplateCacheKey,
) {
    if let Some(position) = lru.iter().position(|candidate| candidate == key) {
        lru.remove(position);
    }
    lru.push_back(key.clone());
}

/// Immutable provider configuration selected for one request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedProviderProfile {
    pub registry_revision: u64,
    pub provider_name: String,
    pub model: String,
    pub base_url: Option<String>,
    pub protocol: Option<String>,
    pub parallel_tool_calls_mode: ParallelToolCallsMode,
    pub effective_parallel_tool_calls: Option<bool>,
    pub effective_early_tool_start: bool,
    /// Model-specific wire facts captured from the user model registry.
    pub model_capabilities: Vec<String>,
    pub capabilities: ProviderCapabilityProfile,
}

/// Request-local provider state. It is created after pinning a registry
/// snapshot and is never stored in a shared transport/client slot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRequestContext {
    pub request_id: String,
    pub profile: ResolvedProviderProfile,
    pub transport_fingerprint: crate::TransportProfileFingerprint,
    /// Explicitly unauthenticated operator diagnostic; never acceptance evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_send_reason: Option<String>,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderGraphOwner {
    pub activity: harness_contract::projection::RuntimeActivityBinding,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderRequestEvidenceContext {
    pub session_id: String,
    /// Runtime execution that owns this physical Provider attempt.
    pub execution_id: String,
    /// Turn epoch that owns this physical Provider attempt.
    pub turn_id: String,
    pub request_sequence: usize,
    pub graph_owner: Option<Box<ProviderGraphOwner>>,
    pub request_compiler_cache_hit: bool,
    pub budget: crate::context_ledger::RequestBudgetReport,
    /// Monotonic Runtime-owned attempt within this model-step operation.
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderWireEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_render: Option<harness_contract::step::StepRenderRef>,
    pub request_context: ProviderRequestContext,
    pub wire_request: provider::ProviderWireRequest,
    pub prefix_observation: crate::ProviderWirePrefixObservation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderAttemptOutcomeEvidence {
    pub request_id: String,
    pub logical_attempt: u32,
    pub terminal_status: String,
    pub usage: Option<TokenUsage>,
    /// True only when the Provider explicitly returned cache dimensions. A
    /// numeric zero without this receipt is not evidence of a cache miss.
    pub cache_dimensions_observed: bool,
    /// Runtime-minted evidence only: no deserialization or public constructor.
    /// Raw response bytes are not part of this outcome capability.
    pub protocol_diagnostic: Option<BoundProviderProtocolDiagnostic>,
}

/// Identity of one returned, locally owned SDK response, not a logical retry
/// group or an inference from historical output bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPhysicalIdentity {
    pub session_id: String,
    pub execution_id: String,
    pub turn_id: String,
    pub request_sequence: usize,
    pub request_id: String,
    pub response_id: String,
    /// Captured by the original producer when this SDK response is returned.
    pub opened_at_ms: u64,
    pub logical_attempt: u32,
    pub step: harness_contract::step::StepIdentity,
    pub provider_attempt: harness_contract::step::ProviderAttemptIdentity,
    pub resource_lease: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_owner: Option<Box<ProviderGraphOwner>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPhysicalReceipt {
    pub identity: ProviderPhysicalIdentity,
    pub session_sequence: usize,
    pub observation_digest: String,
}

/// Live acknowledgement from the original writer; unlike its serialized
/// reference this cannot be deserialized or supplied by a model/client DTO.
pub struct AcknowledgedProviderPhysicalObservation {
    receipt: ProviderPhysicalReceipt,
}

// Live capability: only the original writer-acknowledged physical observation
// can construct this value. It is never deserialized from an external DTO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthenticatedProviderCancellation {
    observation: ProviderPhysicalObservation,
    receipt: ProviderPhysicalReceipt,
}
impl AuthenticatedProviderCancellation {
    pub(crate) fn matches_original(&self, owner: &ProviderGraphOwner, ticket: &crate::execution_core::NodeExecutionTicket) -> bool {
        let ProviderPhysicalObservation::CancellationObserved { identity, .. } = &self.observation else { return false; };
        identity.graph_owner.as_deref() == Some(owner)
            && owner.attempt == ticket.attempt && owner.activity.node_id.as_deref() == Some(ticket.node_id.as_str())
            && owner.activity.root_execution_id == ticket.graph_id && identity.execution_id == ticket.graph_id
            && identity.session_id == owner.activity.session_id && identity.turn_id == owner.activity.turn_id
            && self.receipt.identity == *identity
            && self.receipt.observation_digest == self.observation.digest().unwrap_or_default()
    }
    pub(crate) fn durable_reference(&self) -> String {
        format!("session:{}:physical:{}:{}", self.receipt.identity.session_id,
            self.receipt.session_sequence, self.receipt.observation_digest)
    }
}

impl AcknowledgedProviderPhysicalObservation {
    pub(crate) fn from_written(
        observation: &ProviderPhysicalObservation,
        sequence: usize,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            receipt: ProviderPhysicalReceipt {
                identity: observation.identity().clone(),
                session_sequence: sequence,
                observation_digest: observation.digest()?,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestedCancellationReference {
    pub cancellation_id: String,
    pub stream_id: String,
    pub event_sequence: u64,
    pub commit_cursor: u64,
    pub payload_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderPhysicalObservation {
    ResponseOpened {
        identity: ProviderPhysicalIdentity,
    },
    CancellationObserved {
        identity: ProviderPhysicalIdentity,
        observed_at_ms: u64,
        opened: ProviderPhysicalReceipt,
        requested: RequestedCancellationReference,
    },
}

impl ProviderPhysicalObservation {
    pub(crate) fn occurred_at_ms(&self) -> u64 {
        match self {
            Self::ResponseOpened { identity } => identity.opened_at_ms,
            Self::CancellationObserved { observed_at_ms, .. } => *observed_at_ms,
        }
    }

    pub(crate) fn identity(&self) -> &ProviderPhysicalIdentity {
        match self {
            Self::ResponseOpened { identity } | Self::CancellationObserved { identity, .. } => {
                identity
            }
        }
    }

    pub(crate) fn validate(
        &self,
        context: &ProviderRequestEvidenceContext,
    ) -> Result<(), RuntimeError> {
        let id = self.identity();
        if id.session_id != context.session_id
            || id.execution_id != context.execution_id
            || id.turn_id != context.turn_id
            || id.request_sequence != context.request_sequence
            || id.graph_owner != context.graph_owner
            || id.logical_attempt != context.attempt
            || id.request_id.is_empty()
            || id.response_id.is_empty()
            || id.opened_at_ms == 0
            || self.occurred_at_ms() == 0
            || id.resource_lease.is_empty()
            || id.step.session_id != id.session_id
            || id.step.turn_id != id.turn_id
            || id.step.execution_id.as_deref() != Some(id.execution_id.as_str())
            || id
                .step
                .attempt
                .is_some_and(|attempt| attempt != context.attempt)
            || id.provider_attempt.origin_step_id != id.step.step_id
            || id.provider_attempt.attempt_id.is_empty()
        {
            return Err(RuntimeError::new(
                "physical Provider identity does not match its original Step",
            ));
        }
        if let Some(owner) = &id.graph_owner {
            if owner.attempt == 0 || owner.activity.validate().is_err()
                || owner.activity.root_execution_id != id.execution_id
                || owner.activity.session_id != id.session_id || owner.activity.turn_id != id.turn_id
                || owner.activity.node_id.as_deref().is_none_or(str::is_empty) {
                return Err(RuntimeError::new("physical Provider graph owner does not match its original activity"));
            }
        }
        if let Self::CancellationObserved {
            opened, requested, ..
        } = self
        {
            let original = Self::ResponseOpened {
                identity: id.clone(),
            };
            if opened.identity != *id
                || opened.observation_digest != original.digest()?
                || requested.cancellation_id.is_empty()
                || requested.stream_id != format!("cancellation:{}", requested.cancellation_id)
                || requested.event_sequence != 1
                || requested.commit_cursor == 0
                || !requested
                    .payload_digest
                    .strip_prefix("sha256:")
                    .is_some_and(|digest| {
                        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
                    })
            {
                return Err(RuntimeError::new(
                    "physical cancellation has a foreign open or missing Requested reference",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<String, RuntimeError> {
        let value =
            serde_json::to_value(self).map_err(|error| RuntimeError::new(error.to_string()))?;
        harness_contract::step::canonical_digest("provider-physical-observation", &value)
            .map_err(|error| RuntimeError::new(error.to_string()))
    }
}

struct ProviderAttemptControl {
    stop: crate::CancellationToken,
    caller: Option<crate::CancellationToken>,
    turn: Option<Arc<crate::execution_core::services::TurnCancellationControl>>,
    context: Option<ProviderRequestEvidenceContext>,
    step: Option<Arc<crate::step_snapshot::BoundStep>>,
    lease: Option<Arc<crate::execution_core::graph::ExecutionResourceLease>>,
    resources: Option<Arc<crate::execution_core::graph::ExecutionResourceManager>>,
    governed: bool,
}

impl ProviderAttemptControl {
    fn bound_turn(&self) -> Option<&Arc<crate::execution_core::services::TurnCancellationControl>> {
        let context = self.context.as_ref()?;
        self.turn.as_ref().filter(|turn| {
            turn.matches_target(&context.session_id, &context.turn_id, &context.execution_id)
        })
    }

    fn is_cancelled(&self) -> bool {
        self.stop.is_cancelled()
            || self.caller.as_ref().is_some_and(|c| c.is_cancelled())
            || self
                .bound_turn()
                .is_some_and(|turn| turn.token().is_cancelled())
    }

    async fn cancelled(&self) {
        let caller = async {
            match &self.caller {
                Some(token) => token.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let turn = async {
            match self.bound_turn() {
                Some(control) => control.token().cancelled().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! { () = self.stop.cancelled() => {}, () = caller => {}, () = turn => {} }
    }

    fn physical_identity(
        &self,
        request_id: &str,
    ) -> Result<Option<ProviderPhysicalIdentity>, RuntimeError> {
        let (Some(context), Some(step), Some(lease)) = (&self.context, &self.step, &self.lease)
        else {
            return if self.governed {
                Err(RuntimeError::new(
                    "physical Provider has no original context/Step/admission",
                ))
            } else {
                Ok(None)
            };
        };
        let id = ProviderPhysicalIdentity {
            session_id: context.session_id.clone(),
            execution_id: context.execution_id.clone(),
            turn_id: context.turn_id.clone(),
            request_sequence: context.request_sequence,
            request_id: request_id.into(),
            response_id: uuid::Uuid::new_v4().to_string(),
            opened_at_ms: u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| RuntimeError::new("physical response clock is invalid"))?,
            logical_attempt: context.attempt,
            step: step.snapshot().identity.clone(),
            provider_attempt: step.snapshot().provider_attempt.clone(),
            resource_lease: lease.projection_reference(),
            graph_owner: context.graph_owner.clone(),
        };
        ProviderPhysicalObservation::ResponseOpened {
            identity: id.clone(),
        }
        .validate(context)?;
        Ok(Some(id))
    }

    async fn record_cancel(
        &self,
        writer: Option<&dyn ProviderWireEvidenceWriter>,
        opened: Option<&AcknowledgedProviderPhysicalObservation>,
    ) -> Result<Option<AuthenticatedProviderCancellation>, RuntimeError> {
        // Freeze once in the original producer, not on each persistence retry.
        let observed_at_ms = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| RuntimeError::new("physical cancellation clock is invalid"))?;
        let (Some(turn), Some(caller), Some(context), Some(opened)) =
            (&self.turn, &self.caller, &self.context, opened)
        else {
            return Ok(None);
        };
        if !turn.matches_target(&context.session_id, &context.turn_id, &context.execution_id)
            || !turn.token().precedes_other_signals(caller, &self.stop)
        {
            return Ok(None);
        }
        let Some(source) = turn.requested_source() else {
            return Ok(None);
        };
        source.verify_current().await.map_err(RuntimeError::new)?;
        let lease = self
            .lease
            .as_ref()
            .ok_or_else(|| RuntimeError::new("cancelled Provider lost original admission"))?;
        let resources = self
            .resources
            .as_ref()
            .ok_or_else(|| RuntimeError::new("cancelled Provider lost original resource owner"))?;
        if !matches!(
            resources.lease_is_active(&lease.projection_reference()),
            Ok(Some(true))
        ) {
            return Err(RuntimeError::new(
                "cancelled Provider admission is no longer active",
            ));
        }
        let (stream, sequence, cursor, digest) = source.event_reference();
        let observation = ProviderPhysicalObservation::CancellationObserved {
            identity: opened.receipt.identity.clone(),
            observed_at_ms,
            opened: opened.receipt.clone(),
            requested: RequestedCancellationReference {
                cancellation_id: source.receipt().cancellation_id.clone(),
                stream_id: stream.into(),
                event_sequence: sequence,
                commit_cursor: cursor,
                payload_digest: digest.into(),
            },
        };
        observation.validate(context)?;
        let writer = writer.ok_or_else(|| {
            RuntimeError::new("cancelled Provider has no durable observation writer")
        })?;
        let ack = writer
            .persist_physical(context, observation.clone(), Some(&source))
            .await?;
        if ack.receipt.identity != opened.receipt.identity
            || ack.receipt.observation_digest != observation.digest()?
        {
            return Err(RuntimeError::new(
                "cancel observation acknowledgement belongs to another physical response",
            ));
        }
        Ok(Some(AuthenticatedProviderCancellation { observation, receipt: ack.receipt }))
    }
}
#[async_trait::async_trait]
pub trait ProviderWireEvidenceWriter: Send + Sync {
    async fn persist_physical(
        &self,
        context: &ProviderRequestEvidenceContext,
        observation: ProviderPhysicalObservation,
        source: Option<&crate::execution_core::services::AuthorizedRequestedCancellation>,
    ) -> Result<AcknowledgedProviderPhysicalObservation, RuntimeError>;
    async fn persist(
        &self,
        context: &ProviderRequestEvidenceContext,
        evidence: ProviderWireEvidence,
    ) -> Result<(), RuntimeError>;

    async fn persist_outcome(
        &self,
        context: &ProviderRequestEvidenceContext,
        outcome: ProviderAttemptOutcomeEvidence,
    ) -> Result<(), RuntimeError>;
}

#[derive(Clone)]
pub struct ProviderRuntimeClient {
    preparation_resources: Option<Arc<crate::execution_core::graph::ExecutionResourceManager>>,
    control_admission: Option<ControlResourceAdmission>,
    wire_preparations: Arc<AtomicU64>,
    registry: Arc<ProviderRegistry>,
    transport_pool: Arc<crate::ProviderTransportPool>,
    template_cache: Arc<ProviderClientTemplateCache>,
    tool_definitions: Vec<ToolDefinition>,
    tool_exposure: Option<ToolExposureProjection>,
    tool_choice_required: bool,
    tool_choice_required_tool_name: Option<String>,
    /// Delegated hosts receive an already capability/resource-cropped tool
    /// catalog. Keeping that bounded schema stable across logical exposure
    /// overlays preserves Provider prefixes; Runtime still owns and validates
    /// the narrower per-request execution lease.
    stable_tool_schema: bool,
    reasoning_effort: Option<String>,
    emit_output: bool,
    stream_callback: Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    tool_schema_cache: Arc<std::sync::Mutex<Option<CompiledToolSchema>>>,
    tool_schema_compilations: Arc<AtomicU64>,
    tool_schema_cache_hits: Arc<AtomicU64>,
    execution_supervisor: Option<std::sync::Weak<crate::RuntimeExecutionSupervisor>>,
    provider_wire_evidence_writer: Option<Arc<dyn ProviderWireEvidenceWriter>>,
    turn_cancellation: Option<Arc<crate::execution_core::services::TurnCancellationControl>>,
    /// PLAN D1b: how long a provider stream may go without semantic progress
    /// before a probe is warranted, and how many probes are allowed. The wall
    /// clock is deliberately absent: a long healthy generation is never
    /// interrupted.
    liveness_contract: crate::liveness::LivenessContract,
    /// PLAN D1b: verdicts observed by the stream guard, drained by the turn owner
    /// so each stall becomes a non-forcing push-forward nudge. Kept on the client
    /// so nothing new has to be threaded through Runtime construction.
    liveness_stalls: Arc<std::sync::Mutex<Vec<crate::liveness::LivenessVerdict>>>,
}

#[derive(Clone)]
struct CompiledToolSchema {
    /// Digest of the actual model-visible tool identities. Control-plane
    /// revisions and reasons are evidence, not Provider cache identity.
    exposure_digest: u64,
    tools: Arc<[ToolDefinition]>,
    inventory: ProviderContextInventory,
}

#[cfg(test)]
fn provider_tool_choice(
    has_active_tools: bool,
    required: bool,
    explicit_tool_choice: CapabilityState,
) -> Result<Option<ToolChoice>, RuntimeError> {
    provider_tool_choice_for_required_tool(has_active_tools, required, explicit_tool_choice, None)
}

fn provider_tool_choice_for_required_tool(
    has_active_tools: bool,
    required: bool,
    explicit_tool_choice: CapabilityState,
    required_tool_name: Option<&str>,
) -> Result<Option<ToolChoice>, RuntimeError> {
    match (has_active_tools, required, explicit_tool_choice) {
        (false, false, _) => Ok(None),
        (false, true, _) => Err(RuntimeError::new(
            "provider request requires a governed tool action, but no eligible tool schema is exposed",
        )),
        (true, _, CapabilityState::Unknown) => Err(RuntimeError::new(
            "provider explicit tool_choice capability is unknown; refusing to guess from its model name",
        )),
        // Models with explicit tool_choice known-unsupported (DeepSeek v4)
        // reject the field on the wire; omitting it is the supported path and
        // the required intent is preserved by exposing only the single
        // required tool schema. Never fail a governed synthesis on this.
        (true, true, CapabilityState::Unsupported) => {
            tracing::debug!("provider tool_choice omitted: explicit field unsupported on the wire");
            Ok(None)
        }
        (true, false, CapabilityState::Unsupported) => Ok(None),
        (true, true, CapabilityState::Supported) => {
            Ok(Some(required_tool_name.map_or(ToolChoice::Any, |name| {
                ToolChoice::Tool {
                    name: name.to_string(),
                }
            })))
        }
        (true, false, CapabilityState::Supported) => Ok(Some(ToolChoice::Auto)),
    }
}

fn provider_attempt(context: Option<&ProviderRequestEvidenceContext>) -> u32 {
    context.map_or(1, |context| context.attempt.max(1))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ToolSchemaCacheStats {
    pub compilations: u64,
    pub cache_hits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderControlCompletion {
    pub text: String,
    pub model: String,
    pub request_id: Option<String>,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub stop_reason: Option<String>,
}

#[derive(Clone)]
struct ControlResourceAdmission {
    config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
    class: crate::execution_core::graph::ExecutionServiceClass,
    fairness_key: String,
}

/// A single non-streaming request. The original Task may record this wire
/// identity before dispatch; ownership prevents a second send of the carrier.
/// Body and credentials are deliberately absent from Debug and public access.
pub(crate) struct PreparedControlAnalysis {
    entry: ProviderEntry,
    message: MessageRequest,
    wire: provider::PreparedProviderWire,
    fence: ProviderDispatchSourceFence,
    configured_reasoning: Option<String>,
    _resident: Option<crate::execution_core::graph::ExecutionResourceLease>,
}
impl PreparedControlAnalysis {
    pub(crate) fn wire(&self) -> &provider::ProviderWireRequest {
        self.wire.wire_request()
    }
    pub(crate) fn context(&self) -> &ProviderRequestContext {
        &self.entry.request_context
    }
    pub(crate) fn estimated_total_tokens(&self) -> u64 {
        (self.wire.body_len() as u64).saturating_add(3) / 4 + u64::from(self.message.max_tokens)
    }
}

/// Bridges one provider request into the runtime's lazy `ApiClient` stream.
///
/// The original backend polls the producer alongside this bounded receiver.
/// There is no separately spawned request for Drop to detach.
struct ProviderEventStream {
    receiver: tokio::sync::mpsc::Receiver<Result<AssistantEvent, RuntimeError>>,
}

impl futures::Stream for ProviderEventStream {
    type Item = Result<AssistantEvent, RuntimeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl ProviderRuntimeClient {
    pub fn new(
        registry: Arc<ProviderRegistry>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
    ) -> Result<Self, String> {
        Self::new_with_transport_pool(
            registry,
            Arc::new(crate::ProviderTransportPool::default()),
            model,
            tool_definitions,
        )
    }

    pub fn new_with_transport_pool(
        registry: Arc<ProviderRegistry>,
        transport_pool: Arc<crate::ProviderTransportPool>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
    ) -> Result<Self, String> {
        Self::new_with_transport_and_template_cache(
            registry,
            transport_pool,
            Arc::new(ProviderClientTemplateCache::default()),
            model,
            tool_definitions,
        )
    }

    pub fn new_with_transport_and_template_cache(
        registry: Arc<ProviderRegistry>,
        transport_pool: Arc<crate::ProviderTransportPool>,
        template_cache: Arc<ProviderClientTemplateCache>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
    ) -> Result<Self, String> {
        let snapshot = registry.pin();
        template_cache.resolve(&snapshot, &transport_pool, &model)?;
        Ok(Self {
            preparation_resources: None,
            control_admission: None,
            wire_preparations: Arc::new(AtomicU64::new(0)),
            registry,
            transport_pool,
            template_cache,
            tool_definitions,
            tool_exposure: None,
            tool_choice_required: false,
            tool_choice_required_tool_name: None,
            stable_tool_schema: false,
            reasoning_effort: None,
            emit_output: false,
            stream_callback: None,
            tool_schema_cache: Arc::new(std::sync::Mutex::new(None)),
            tool_schema_compilations: Arc::new(AtomicU64::new(0)),
            tool_schema_cache_hits: Arc::new(AtomicU64::new(0)),
            execution_supervisor: None,
            provider_wire_evidence_writer: None,
            turn_cancellation: None,
            liveness_contract: crate::liveness::LivenessContract::default(),
            liveness_stalls: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    #[must_use]
    pub(crate) fn with_execution_supervisor(
        mut self,
        supervisor: &Arc<crate::RuntimeExecutionSupervisor>,
    ) -> Self {
        self.execution_supervisor = Some(Arc::downgrade(supervisor));
        self
    }

    #[must_use]
    pub fn with_preparation_resources(
        mut self,
        manager: Arc<crate::execution_core::graph::ExecutionResourceManager>,
    ) -> Self {
        self.preparation_resources = Some(manager);
        self
    }

    /// Bind the composition root's existing Provider resource owner. Preparing
    /// content uses its resident quota; only the actual control leaf acquires
    /// Provider/account/model/token demands.
    #[must_use]
    pub fn with_control_resources(
        mut self,
        manager: Arc<crate::execution_core::graph::ExecutionResourceManager>,
        config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
        class: crate::execution_core::graph::ExecutionServiceClass,
        fairness_key: impl Into<String>,
    ) -> Self {
        self.preparation_resources = Some(manager);
        self.control_admission = Some(ControlResourceAdmission {
            config,
            class,
            fairness_key: fairness_key.into(),
        });
        self
    }

    pub fn wire_preparation_count(&self) -> u64 {
        self.wire_preparations.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn provider_registry(&self) -> &Arc<ProviderRegistry> {
        &self.registry
    }

    #[must_use]
    pub fn with_emit_output(mut self, emit_output: bool) -> Self {
        self.emit_output = emit_output;
        self
    }

    #[must_use]
    pub fn with_stream_callback(
        mut self,
        stream_callback: Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    ) -> Self {
        self.stream_callback = stream_callback;
        self
    }

    pub fn set_reasoning_effort(&mut self, effort: Option<String>) {
        self.reasoning_effort = effort;
    }

    /// Execute one provider-backed control-plane analysis without exposing
    /// tools, conversation history, memory packets, or stream callbacks.
    ///
    /// Background governance uses the same pinned provider registry,
    /// transport pool, and client-template cache as ordinary Runtime requests,
    /// while remaining outside the foreground Session execution graph.
    pub async fn complete_control_analysis(
        &self,
        model: &str,
        system: impl AsRef<str>,
        input: impl AsRef<str>,
        max_tokens: u32,
    ) -> Result<ProviderControlCompletion, String> {
        let prepared = self
            .prepare_control_analysis(model, system.as_ref(), input.as_ref(), max_tokens)
            .await?;
        if let Some(admission) = &self.control_admission {
            let manager = self
                .preparation_resources
                .as_ref()
                .ok_or("control resource owner missing")?;
            let config = admission
                .config
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;
            let request = crate::provider_resources::provider_leaf_admission(
                &config,
                Some(&prepared.context().profile.provider_name),
                &prepared.entry.model,
                prepared.estimated_total_tokens(),
                admission.class,
                &admission.fairness_key,
                now.saturating_add(1500),
            );
            let crate::execution_core::graph::ResourceAdmissionDecision::Granted { lease, .. } =
                manager.admit(request).await.map_err(|e| e.to_string())?
            else {
                return Err("control Provider capacity unavailable".into());
            };
            return self
                .complete_admitted_control_analysis(prepared, &lease, &config)
                .await;
        }
        self.send_control_analysis(prepared).await
    }

    pub(crate) async fn prepare_control_analysis(
        &self,
        model: &str,
        system: &str,
        input: &str,
        max_tokens: u32,
    ) -> Result<PreparedControlAnalysis, String> {
        let snapshot = self.registry.pin();
        let entry = self.template_cache.resolve(
            &snapshot,
            &self.transport_pool,
            model,
        )?;
        self.validate_control_account(&entry)?;
        let capacity = snapshot.context_window_resolution(model);
        if capacity.source == provider::ModelContextWindowSource::Assumed {
            return Err(
                "control context capacity is unknown; configure a verified model window".into(),
            );
        }

        // Count the borrowed input before allocating the protocol message or
        // rendering JSON. This uses the same original resident quota as streams.
        let mut counter = PreparationSourceCounter(0);
        serde_json::to_writer(&mut counter, &(system, input)).map_err(|e| e.to_string())?;
        if (counter.0 as u64).saturating_add(3) / 4 + u64::from(max_tokens.max(1))
            > u64::from(capacity.tokens)
        {
            return Err(
                "control request exceeds verified context capacity before materialization".into(),
            );
        }
        let resident_bytes = counter
            .0
            .checked_add(65536)
            .and_then(|n| n.checked_mul(32))
            .ok_or("control resident budget overflow")?;
        let resident = if let Some(manager) = &self.preparation_resources {
            use crate::execution_core::graph::*;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;
            let (class, key) = self.control_admission.as_ref().map_or(
                (
                    ExecutionServiceClass::Foreground,
                    "provider-control-prepare",
                ),
                |admission| (admission.class, admission.fairness_key.as_str()),
            );
            let admission = ResourceAdmissionRequest::new(
                class,
                [(ExecutionResourceKind::ContextBytes, resident_bytes)],
            )
            .with_parent_class_ceiling(class)
            .with_fairness_key(key)
            .with_deadline_at_ms(now.saturating_add(5000));
            let decision =
                match admit_preparation_resources(manager, admission, self.turn_cancellation.as_ref())
                    .await
                    .map_err(|error| error.to_string())?
                {
                    Some(decision) => decision,
                    None => {
                        return Err(
                            "control preparation cancelled while waiting for context capacity"
                                .into(),
                        );
                    }
                };
            match decision {
                ResourceAdmissionDecision::Granted { lease, .. } => Some(lease),
                decision => {
                    return Err(preparation_capacity_failure(
                        "control prepared content capacity unavailable",
                        &decision,
                    ))
                }
            }
        } else {
            if self.execution_supervisor.is_some() {
                return Err("governed control preparation has no original resource owner".into());
            }
            None
        };
        let message = MessageRequest {
            model: entry.model.clone(),
            max_tokens: max_tokens.max(1),
            messages: vec![InputMessage::user_text(input)],
            system: Some(system.to_owned()),
            reasoning_effort: request_reasoning_effort(
                &entry.model,
                &entry.request_context.profile.model_capabilities,
                None,
                self.reasoning_effort.clone().or_else(|| snapshot.resolve(&entry.model).and_then(|provider| provider.reasoning_effort.clone())),
            ),
            protocol_capabilities: entry.request_context.profile.model_capabilities.clone(),
            context_window_limit: Some(capacity.tokens),
            ..MessageRequest::default()
        };
        provider::preflight_message_request(&message).map_err(|e| e.to_string())?;
        let wire = entry
            .client
            .prepare_wire(&message)
            .map_err(|e| e.to_string())?;
        if (wire.body_len() as u64).saturating_add(3) / 4 + u64::from(message.max_tokens)
            > u64::from(capacity.tokens)
        {
            return Err("control prepared wire exceeds verified context capacity".into());
        }
        self.wire_preparations.fetch_add(1, Ordering::Relaxed);
        let fence = ProviderDispatchSourceFence {
            registry: Arc::clone(&self.registry),
            revision: snapshot.revision(),
            transport: Arc::clone(&self.transport_pool),
            transport_fingerprint: entry.request_context.transport_fingerprint,
            policy: None,
            skill_source: None,
        };
        Ok(PreparedControlAnalysis {
            entry,
            message,
            wire,
            fence,
            configured_reasoning: self.reasoning_effort.clone(),
            _resident: resident,
        })
    }

    /// Consume the existing leaf admission, never acquire a second Provider slot.
    pub(crate) async fn complete_admitted_control_analysis(
        &self,
        prepared: PreparedControlAnalysis,
        lease: &crate::execution_core::graph::ExecutionResourceLease,
        config: &crate::ProviderResourceConfig,
    ) -> Result<ProviderControlCompletion, String> {
        let manager = self
            .preparation_resources
            .as_ref()
            .ok_or("control dispatch has no original resource owner")?;
        let expected = config.admission_demands(
            &prepared.entry.request_context.profile.provider_name,
            &prepared.entry.model,
            prepared.estimated_total_tokens(),
        );
        if manager
            .lease_is_active(&lease.projection_reference())
            .map_err(|e| e.to_string())?
            != Some(true)
            || lease.demands().len() != expected.len()
            || expected.iter().any(|d| !lease.demands().contains(d))
        {
            return Err("control dispatch lease does not match original Provider admission".into());
        }
        self.send_control_analysis(prepared).await
    }

    fn validate_control_account(&self, entry: &ProviderEntry) -> Result<(), String> {
        if self.transport_pool.provider_account_is_fenced(
            entry.request_context.profile.registry_revision,
            &entry.request_context.profile.provider_name,
        ) {
            return Err(provider_account_fenced_message(
                &entry.request_context.profile.provider_name,
            ));
        }
        Ok(())
    }

    async fn send_control_analysis(
        &self,
        prepared: PreparedControlAnalysis,
    ) -> Result<ProviderControlCompletion, String> {
        if !Arc::ptr_eq(&self.registry, &prepared.fence.registry)
            || !Arc::ptr_eq(&self.transport_pool, &prepared.fence.transport)
            || self.reasoning_effort != prepared.configured_reasoning
        {
            return Err("control preparation belongs to a different source; prepare again".into());
        }
        prepared.fence.validate().map_err(|e| e.to_string())?;
        self.validate_control_account(&prepared.entry)?;
        let _request_guard = self.transport_pool.begin_request();
        let response = match prepared
            .entry
            .client
            .send_prepared_message(&prepared.message, &prepared.wire)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.transport_pool.record_provider_failure(
                    prepared.entry.request_context.profile.registry_revision,
                    &prepared.entry.request_context.profile.provider_name,
                    error.failure_scope(),
                );
                return Err(error.to_string());
            }
        };
        control_completion(response)
    }

    /// Install the explicit tool schema set selected by Runtime.
    ///
    /// An unconfigured client exposes no tools. Older projections are ignored,
    /// which prevents a delayed planner update from rolling schema visibility
    /// back after a newer activation revision has reached the client.
    pub fn configure_tool_exposure(&mut self, projection: ToolExposureProjection) {
        let is_stale = self.tool_exposure.as_ref().is_some_and(|current| {
            projection.catalog_revision < current.catalog_revision
                || (projection.catalog_revision == current.catalog_revision
                    && projection.exposure_revision < current.exposure_revision)
        });
        if is_stale {
            tracing::warn!(
                catalog_revision = projection.catalog_revision,
                exposure_revision = projection.exposure_revision,
                "ignoring stale provider tool exposure projection"
            );
            return;
        }
        self.tool_exposure = Some(projection);
    }

    pub fn configure_tool_choice_required(&mut self, required: bool) {
        self.tool_choice_required = required;
        if !required {
            self.tool_choice_required_tool_name = None;
        }
    }

    pub fn configure_tool_choice(&mut self, required: bool, required_tool_name: Option<String>) {
        self.tool_choice_required = required;
        self.tool_choice_required_tool_name = required.then_some(required_tool_name).flatten();
    }

    #[must_use]
    pub(crate) fn with_stable_tool_schema(mut self, stable: bool) -> Self {
        self.stable_tool_schema = stable;
        self
    }

    fn active_tool_definitions(&self) -> Vec<ToolDefinition> {
        if self.stable_tool_schema {
            self.tool_definitions.clone()
        } else {
            tool_definitions_for_exposure(&self.tool_definitions, self.tool_exposure.as_ref())
        }
    }

    fn compiled_tool_schema(&self) -> CompiledToolSchema {
        let catalog_revision = self
            .tool_exposure
            .as_ref()
            .map_or(0, |projection| projection.catalog_revision);
        let exposure_revision = self
            .tool_exposure
            .as_ref()
            .map_or(0, |projection| projection.exposure_revision);
        let provider_registry_revision = self.registry.revision();
        let exposure_digest = if self.stable_tool_schema {
            tool_definition_identity_digest(&self.tool_definitions)
        } else {
            tool_exposure_digest(self.tool_exposure.as_ref())
        };
        let mut cache = self
            .tool_schema_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(compiled) = cache.as_ref().filter(|compiled| {
            compiled.exposure_digest == exposure_digest
                && compiled.inventory.provider_registry_revision == provider_registry_revision
        }) {
            self.tool_schema_cache_hits.fetch_add(1, Ordering::Relaxed);
            let mut reused = compiled.clone();
            reused.inventory.catalog_revision = catalog_revision;
            reused.inventory.exposure_revision = exposure_revision;
            return reused;
        }
        let tools: Arc<[ToolDefinition]> = self.active_tool_definitions().into();
        let schema_json = serde_json::to_vec(tools.as_ref()).unwrap_or_default();
        let inventory = ProviderContextInventory {
            tool_count: tools.len(),
            tool_schema_tokens: crate::context_ledger::estimate_text_tokens(
                std::str::from_utf8(&schema_json).unwrap_or_default(),
            ),
            catalog_revision,
            exposure_revision,
            schema_fingerprint: model_protocol::fingerprint::stable_hash_bytes(&schema_json),
            provider_registry_revision,
        };
        let compiled = CompiledToolSchema {
            exposure_digest,
            tools,
            inventory,
        };
        *cache = Some(compiled.clone());
        self.tool_schema_compilations
            .fetch_add(1, Ordering::Relaxed);
        compiled
    }

    #[must_use]
    pub fn tool_schema_cache_stats(&self) -> ToolSchemaCacheStats {
        ToolSchemaCacheStats {
            compilations: self.tool_schema_compilations.load(Ordering::Relaxed),
            cache_hits: self.tool_schema_cache_hits.load(Ordering::Relaxed),
        }
    }
}

fn control_completion(response: MessageResponse) -> Result<ProviderControlCompletion, String> {
    let text = response
        .content
        .iter()
        .filter_map(|block| match block {
            OutputContentBlock::Text { text } | OutputContentBlock::ReasoningSummary { text } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    if text.trim().is_empty() {
        return Err("provider control analysis returned no text".to_string());
    }
    Ok(ProviderControlCompletion {
        text,
        model: response.model,
        request_id: response.request_id,
        input_tokens: response
            .usage_observed
            .then_some(response.usage.input_tokens),
        output_tokens: response
            .usage_observed
            .then_some(response.usage.output_tokens),
        stop_reason: response.stop_reason,
    })
}

fn tool_exposure_digest(exposure: Option<&ToolExposureProjection>) -> u64 {
    let Some(exposure) = exposure else {
        return model_protocol::fingerprint::stable_hash_bytes(b"no-tools");
    };
    let ids = exposure
        .bootstrap_ids
        .iter()
        .chain(exposure.active_ids.iter())
        .collect::<std::collections::BTreeSet<_>>();
    let mut canonical = String::new();
    for id in ids {
        canonical.push_str(&id.len().to_string());
        canonical.push(':');
        canonical.push_str(id);
        canonical.push('\n');
    }
    model_protocol::fingerprint::stable_hash_bytes(canonical.as_bytes())
}

fn tool_definition_identity_digest(definitions: &[ToolDefinition]) -> u64 {
    let mut ids = definitions
        .iter()
        .map(|definition| definition.name.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    let mut canonical = String::new();
    for id in ids {
        canonical.push_str(&id.len().to_string());
        canonical.push(':');
        canonical.push_str(id);
        canonical.push('\n');
    }
    model_protocol::fingerprint::stable_hash_bytes(canonical.as_bytes())
}

fn tool_definitions_for_exposure(
    definitions: &[ToolDefinition],
    exposure: Option<&ToolExposureProjection>,
) -> Vec<ToolDefinition> {
    let Some(exposure) = exposure else {
        return Vec::new();
    };
    let active_ids = exposure
        .bootstrap_ids
        .iter()
        .chain(exposure.active_ids.iter())
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    definitions
        .iter()
        .filter(|definition| active_ids.contains(definition.name.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
fn build_provider_entry(
    snapshot: &ProviderRegistrySnapshot,
    transport_pool: &crate::ProviderTransportPool,
    model: &str,
) -> Result<ProviderEntry, String> {
    let resolved = model.trim().to_string();
    let (transport_fingerprint, http) = transport_pool
        .checkout_default()
        .map_err(|error| error.to_string())?;
    build_provider_template(
        snapshot,
        transport_fingerprint,
        http,
        &resolved,
    )
    .map(|template| template.request_entry())
}

fn build_provider_template(
    snapshot: &ProviderRegistrySnapshot,
    transport_fingerprint: crate::TransportProfileFingerprint,
    http: reqwest::Client,
    resolved: &str,
) -> Result<ProviderEntryTemplate, String> {
    let (
        client,
        provider_name,
        base_url,
        protocol,
        parallel_tool_calls_mode,
        effective_parallel_tool_calls,
        effective_early_tool_start,
        capabilities,
    ) = match snapshot.resolve(resolved) {
        Some(provider) => {
            let protocol = crate::config::ProviderProtocol::effective_for_provider(provider)
                .map_err(|error| error.to_string())?;
            let capabilities = ProviderCapabilityProfile::resolve(protocol, resolved);
            let effective_parallel_tool_calls = provider
                .parallel_tool_calls
                .effective_request(&capabilities)
                .map_err(|error| format!("provider '{}': {error}", provider.name))?;
            let effective_early_tool_start = provider.early_tool_start.effective(resolved);
            (
                ProviderClient::from_config_with_effective_protocol_and_http(
                    provider, protocol, http,
                )
                .map_err(|e| e.to_string())?
                .without_retries(),
                provider.name.clone(),
                Some(provider.base_url.clone()),
                Some(protocol.as_str().to_string()),
                provider.parallel_tool_calls,
                effective_parallel_tool_calls,
                effective_early_tool_start,
                capabilities,
            )
        }
        None => {
            return Err(format!(
                "model '{resolved}' is not declared by any configured provider; Gateway Runtime does not infer provider credentials from the process environment"
            ));
        }
    };
    Ok(ProviderEntryTemplate {
        model: resolved.to_string(),
        client,
        profile: ResolvedProviderProfile {
            registry_revision: snapshot.revision(),
            provider_name,
            model: resolved.to_string(),
            base_url,
            protocol,
            parallel_tool_calls_mode,
            effective_parallel_tool_calls,
            effective_early_tool_start,
            model_capabilities: model_protocol::model_registry::global_registry()
                .capacity_model_info(resolved)
                .map(|info| info.capabilities.clone())
                .unwrap_or_default(),
            capabilities,
        },
        transport_fingerprint,
        diagnostic_send_reason: None,
    })
}

impl ApiClient for ProviderRuntimeClient {
    fn configured_context_window(&self, model: &str) -> Option<u32> {
        self.registry.pin().configured_context_window(model)
    }

    fn prepare_request_budget<'a>(
        &'a self,
        request: &'a mut ApiRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(async move {
            self.prepare_frozen_request(request, &self.registry.pin())
                .await
        })
    }

    fn step_tool_definitions(&self) -> Option<Vec<ProviderToolDefinition>> {
        Some(self.compiled_tool_schema().tools.to_vec())
    }

    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<
        Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
    > {
        let provider_snapshot = self.registry.pin();
        self.stream_with_provider_snapshot(request, provider_snapshot)
    }

    fn stream_with_transport_activity(&mut self, request: ApiRequest) -> ApiClientStream<'_> {
        let provider_snapshot = self.registry.pin();
        self.stream_with_activity_and_provider_snapshot(request, provider_snapshot)
    }

    fn provider_name_for_model(&self, model: &str) -> Option<String> {
        self.registry.pin().provider_name_for_model(model)
    }

    fn configure_tool_exposure(&mut self, projection: ToolExposureProjection) {
        ProviderRuntimeClient::configure_tool_exposure(self, projection);
    }

    fn configure_tool_choice_required(&mut self, required: bool) {
        ProviderRuntimeClient::configure_tool_choice_required(self, required);
    }

    fn configure_tool_choice(&mut self, required: bool, required_tool_name: Option<String>) {
        ProviderRuntimeClient::configure_tool_choice(self, required, required_tool_name);
    }

    fn context_inventory(&self) -> ProviderContextInventory {
        self.compiled_tool_schema().inventory
    }

    fn tool_schema_cache_stats(&self) -> (u64, u64) {
        let stats = ProviderRuntimeClient::tool_schema_cache_stats(self);
        (stats.compilations, stats.cache_hits)
    }

    fn configure_provider_wire_evidence(
        &mut self,
        writer: Option<Arc<dyn ProviderWireEvidenceWriter>>,
    ) {
        self.provider_wire_evidence_writer = writer;
    }

    fn configure_turn_cancellation(
        &mut self,
        control: Option<Arc<crate::execution_core::services::TurnCancellationControl>>,
    ) -> Result<(), RuntimeError> {
        self.turn_cancellation = control;
        Ok(())
    }

    fn take_liveness_stalls(&mut self) -> Vec<crate::liveness::LivenessVerdict> {
        self.liveness_stalls
            .lock()
            .map(|mut recorded| std::mem::take(&mut *recorded))
            .unwrap_or_default()
    }
}

/// The message one preparation's admission failure carries.
///
/// The admission decision has three states and the wait reason has eight; a
/// preparation that was not granted says WHICH of them held it back, so the
/// failure names its cause instead of collapsing every wait into one
/// sentence that reads like a capacity wall.
fn preparation_capacity_failure(
    prefix: &str,
    decision: &crate::execution_core::graph::ResourceAdmissionDecision,
) -> String {
    match decision {
        crate::execution_core::graph::ResourceAdmissionDecision::Granted { .. } => {
            format!("{prefix}: wait_reason=granted")
        }
        crate::execution_core::graph::ResourceAdmissionDecision::Deferred { wait_reason, .. }
        | crate::execution_core::graph::ResourceAdmissionDecision::Overloaded {
            wait_reason, ..
        } => format!("{prefix}: wait_reason={wait_reason:?}"),
    }
}

/// The fairness key one preparation's admission queues on.
///
/// The sibling provider path admits on `session:{id}`, so this path queues on
/// the same session's admission instead of on a second queue of its own.
fn preparation_fairness_key(session: Option<&str>) -> String {
    match session {
        Some(session) if !session.is_empty() => format!("session:{session}"),
        _ => "provider-prepare".to_string(),
    }
}

/// Admit one preparation's resources, and stop waiting when the turn ends.
///
/// The sibling provider path binds the turn's cancellation to its admission
/// wait; a preparation waits the same way, so a cancelled turn does not sit
/// out the whole admission window and then fail for a capacity that no
/// longer matters.
///
/// `Ok(None)` means the turn was cancelled while the admission was pending.
async fn admit_preparation_resources(
    manager: &crate::execution_core::graph::ExecutionResourceManager,
    admission: crate::execution_core::graph::ResourceAdmissionRequest,
    cancellation: Option<&Arc<crate::execution_core::services::TurnCancellationControl>>,
) -> Result<Option<crate::execution_core::graph::ResourceAdmissionDecision>, String> {
    let admit = manager.admit(admission);
    tokio::select! {
        () = wait_for_turn_cancellation(cancellation) => Ok(None),
        decision = admit => decision.map(Some).map_err(|error| error.to_string()),
    }
}

/// Await the turn's cancellation, when this client has one.
async fn wait_for_turn_cancellation(
    cancellation: Option<&Arc<crate::execution_core::services::TurnCancellationControl>>,
) {
    match cancellation {
        Some(control) => control.token().cancelled().await,
        None => std::future::pending::<()>().await,
    }
}

impl ProviderRuntimeClient {
    pub(crate) fn stream_with_provider_snapshot(
        &mut self,
        request: ApiRequest,
        provider_snapshot: ProviderRegistrySnapshot,
    ) -> Pin<
        Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
    > {
        let contract = self.liveness_contract.clone();
        let recorded_stalls = Arc::clone(&self.liveness_stalls);
        let events = self
            .stream_with_activity_and_provider_snapshot(request, provider_snapshot)
            .into_stream();
        // PLAN D1b (corrected): a stream that goes semantically inactive is
        // REPORTED with the idle time and the ladder step so the turn owner can
        // HELP the model (refocus / summarize / request commit / ask). The stream
        // is never cut off and no typed failure is synthesised: the earlier
        // version terminated the turn on a symptom that turned out to be a
        // measurement artefact, which killed work the model was still doing.
        //
        // The verdict is recorded on the client so the turn owner can drain it
        // without a new value being threaded through Runtime construction; the
        // optional `liveness_stall_sink` remains for callers that want an
        // immediate out-of-band reaction.
        // The guard reads the answer, not the byte count. A model that streams
        // the same answer again and again used to reset the inactivity window
        // on every delta, so the ladder never fired and nothing in the
       // framework helped it: one live run sat at 0.958 consecutive-answer
        // similarity while the tracker reported steady progress, and the eval
        // monitor stopped the run because the runtime would not.
        Box::pin(crate::liveness::guard_liveness_with_progress(
            events,
            contract,
            move |verdict| {
                if let Ok(mut recorded) = recorded_stalls.lock() {
                    recorded.push(verdict);
                }
            },
            |event| match event {
                // A failed event carries no answer, so it is not progress.
                Ok(crate::conversation::AssistantEvent::TextDelta(text)) => {
                    crate::liveness::LivenessProgress::Text(text.clone())
                }
                Ok(crate::conversation::AssistantEvent::ItemCompleted { .. }) => {
                    crate::liveness::LivenessProgress::AnswerComplete
                }
                _ => crate::liveness::LivenessProgress::Ignore,
            },
        ))
    }

    fn stream_with_activity_and_provider_snapshot(
        &mut self,
        mut request: ApiRequest,
        snapshot: ProviderRegistrySnapshot,
    ) -> ApiClientStream<'_> {
        let owner = self.clone();
        self.tool_choice_required = false;
        self.tool_choice_required_tool_name = None;
        let (sender, receiver) = tokio::sync::mpsc::channel(PROVIDER_EVENT_QUEUE_CAPACITY);
        let activity = provider::TransportActivity::default();
        let dispatch_activity = activity.clone();
        let stop = crate::CancellationToken::new();
        let producer_stop = stop.clone();
        let producer = crate::conversation::ProviderExecution::new(
            async move {
                if producer_stop.is_cancelled() {
                    return Err(RuntimeError::new(
                        "provider operation stopped before dispatch",
                    ));
                }
                let pins = request.prompt.skill_instruction_pins()?;
                let tool_ids = owner
                    .active_tool_definitions()
                    .into_iter()
                    .map(|definition| definition.name)
                    .collect();
                request
                    .preparation
                    .retain_actual_skill_pins(&pins, &tool_ids)?;
                tokio::select! {
                    biased;
                    result = owner.prepare_frozen_request(&mut request, &snapshot) => result?,
                    () = producer_stop.cancelled() => return Err(RuntimeError::new("provider preparation stopped")),
                }
                if owner.registry.revision() != snapshot.revision() {
                    return Err(RuntimeError::preparation(
                        ProviderPreparationFailure::ProfileChanged,
                        "provider registry changed after preparation",
                    ));
                }
                if let Some(step) = request.bound_step.as_ref() {
                    if step.snapshot().related_revisions.get("provider_registry")
                        != Some(&snapshot.revision().to_string())
                    {
                        return Err(RuntimeError::preparation(
                            ProviderPreparationFailure::ProfileChanged,
                            "provider registry changed after Step binding",
                        ));
                    }
                    step.validate_provider_request(
                        &request,
                        owner.compiled_tool_schema().tools.as_ref(),
                    )?;
                    step.consume_provider_attempt()?;
                }
                let prepared = request
                    .preparation
                    .prepared
                    .take()
                    .ok_or_else(|| RuntimeError::new("original preparation is missing"))?;
                if prepared
                    .consumed
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return Err(RuntimeError::preparation(
                        ProviderPreparationFailure::Consumed,
                        "prepared Provider attempt already consumed",
                    ));
                }
                let mut entry = prepared.entry.clone();
                entry.request_context.attempt =
                    provider_attempt(request.provider_evidence_context.as_ref());
                if owner.transport_pool.provider_account_is_fenced(
                    entry.request_context.profile.registry_revision,
                    &entry.request_context.profile.provider_name,
                ) {
                    return Err(provider_account_fenced_error(
                        &entry.request_context.profile.provider_name,
                    ));
                }
                let lease = request.preparation.dispatch_lease.take();
                if owner.execution_supervisor.is_some() && lease.is_none() {
                    return Err(RuntimeError::preparation(
                        ProviderPreparationFailure::MissingResourceOwner,
                        "governed Provider dispatch has no original admission lease",
                    ));
                }
                if let (Some(manager), Some(lease)) = (&owner.preparation_resources, lease.as_ref())
                {
                    if !matches!(
                        manager.lease_is_active(&lease.projection_reference()),
                        Ok(Some(true))
                    ) {
                        return Err(RuntimeError::new(
                            "original Provider admission lease is not active",
                        ));
                    }
                }
                let control = ProviderAttemptControl {
                    stop: producer_stop,
                    caller: request.preparation.cancellation.take(),
                    turn: owner.turn_cancellation.clone(),
                    context: request.provider_evidence_context.clone(),
                    step: request.bound_step.clone(),
                    lease: lease.clone(),
                    resources: owner.preparation_resources.clone(),
                    governed: owner.execution_supervisor.is_some(),
                };
                let fence = ProviderDispatchSourceFence {
                    registry: owner.registry.clone(),
                    revision: snapshot.revision(),
                    transport: owner.transport_pool.clone(),
                    transport_fingerprint: entry.request_context.transport_fingerprint,
                    policy: request.preparation.policy_source.take(),
                    skill_source: request.preparation.skill_source.take(),
                };
                fence.validate()?;
                let result = forward_provider_attempt(
                    entry,
                    Some(request.prompt.composition()),
                    prepared,
                    fence,
                    request.prompt.cache_cohort_system_text(),
                    request.prompt.cache_cohort_user_prefix_messages(),
                    owner.emit_output,
                    owner.stream_callback.clone(),
                    request.provider_evidence_context,
                    owner.provider_wire_evidence_writer.clone(),
                    request.bound_step,
                    owner.transport_pool.clone(),
                    dispatch_activity,
                    sender,
                    &control,
                )
                .await;
                drop(lease);
                result
            },
            stop,
        );
        ApiClientStream {
            events: Box::pin(ProviderEventStream { receiver }),
            transport_activity: Some(activity),
            execution: Some(producer),
        }
    }
}

impl ProviderRuntimeClient {
    async fn prepare_frozen_request(
        &self,
        request: &mut ApiRequest,
        snapshot: &ProviderRegistrySnapshot,
    ) -> Result<(), RuntimeError> {
        if matches!(
            request.budget.context_window_source.as_str(),
            "unknown" | "assumed"
        ) {
            return Err(RuntimeError::context_capacity(
                crate::context_ledger::ContextCapacityFailure::UnknownProfile,
                "context_capacity_unknown: configure a verified model window before dispatch",
            ));
        }
        if request
            .budget
            .compiled_provider_registry_revision
            .is_some_and(|revision| revision != snapshot.revision())
        {
            return Err(RuntimeError::preparation(
                ProviderPreparationFailure::ProfileChanged,
                "provider profile changed after compile",
            ));
        }
        if let Some(prepared) = request.preparation.prepared.as_ref() {
            if prepared.entry.request_context.profile.registry_revision != snapshot.revision() {
                return Err(RuntimeError::preparation(
                    ProviderPreparationFailure::ProfileChanged,
                    "provider profile changed after preparation",
                ));
            }
            if !prepared.source.matches(request, self)
                || prepared.entry.request_context.profile.registry_revision != snapshot.revision()
            {
                return Err(RuntimeError::preparation(
                    ProviderPreparationFailure::SourceChanged,
                    "request/schema changed; prepare again",
                ));
            }
            if prepared.consumed.load(Ordering::Acquire) {
                return Err(RuntimeError::preparation(
                    ProviderPreparationFailure::Consumed,
                    "prepared Provider attempt already consumed",
                ));
            }
            return Ok(());
        }
        // Count original resident sources without rendering prompt packets,
        // converting history, or cloning the protocol message beforehand.
        let mut counter = PreparationSourceCounter(0);
        let structures = request
            .messages
            .iter()
            .map(|m| m.blocks.len())
            .sum::<usize>()
            .saturating_add(request.prompt.contextual_packets.len());
        counter.0 = structures.checked_mul(512).ok_or_else(|| {
            RuntimeError::context_capacity(
                crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                "prepared source structure budget overflow",
            )
        })?;

        for message in request.messages.iter() {
            serde_json::to_writer(&mut counter, &(message.role, &message.blocks)).map_err(|e| {
                RuntimeError::context_capacity(
                    crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                    e.to_string(),
                )
            })?;
        }
        let prompt = &request.prompt;
        serde_json::to_writer(
            &mut counter,
            &(
                &prompt.trusted_system,
                &prompt.cache_cohort_user_prefix,
                &prompt.immutable_user_prefix,
                &prompt.runtime_context,
                &prompt.volatile_tail,
            ),
        )
        .map_err(|e| {
            RuntimeError::context_capacity(
                crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                e.to_string(),
            )
        })?;
        for packet in &prompt.contextual_packets {
            serde_json::to_writer(
                &mut counter,
                &(&packet.content, &packet.source_id, &packet.evidence),
            )
            .map_err(|e| {
                RuntimeError::context_capacity(
                    crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                    e.to_string(),
                )
            })?;
        }
        let schema = self.compiled_tool_schema();
        serde_json::to_writer(&mut counter, &*schema.tools).map_err(|e| {
            RuntimeError::context_capacity(
                crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                e.to_string(),
            )
        })?;
        let resident = counter
            .0
            .checked_add(65536)
            .and_then(|n| n.checked_mul(32))
            .ok_or_else(|| {
                RuntimeError::context_capacity(
                    crate::context_ledger::ContextCapacityFailure::RequiredOverflow,
                    "prepared request resident budget overflow",
                )
            })?;
        let reservation = if let Some(manager) = &self.preparation_resources {
            use crate::execution_core::graph::*;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| RuntimeError::new(e.to_string()))?
                .as_millis() as u64;
            let (class, session) = request
                .preparation
                .preparation_scope
                .as_ref()
                .map(|(class, key)| (*class, Some(key.as_str())))
                .unwrap_or_else(|| {
                    (
                        ExecutionServiceClass::Foreground,
                        request
                            .provider_evidence_context
                            .as_ref()
                            .map(|context| context.session_id.as_str()),
                    )
                });
            // The sibling provider path admits on `session:{id}`; this path
            // queues on the same session's admission instead of on a second
            // queue of its own.
            let fairness_key = preparation_fairness_key(session);
            let admission = ResourceAdmissionRequest::new(
                class,
                [(ExecutionResourceKind::ContextBytes, resident)],
            )
            .with_parent_class_ceiling(class)
            .with_fairness_key(fairness_key.as_str())
            .with_deadline_at_ms(now.saturating_add(5000));
            let decision = match admit_preparation_resources(
                manager,
                admission,
                self.turn_cancellation.as_ref(),
            )
            .await
            .map_err(|error| {
                RuntimeError::context_capacity(
                    crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                    error,
                )
            })? {
                Some(decision) => decision,
                None => {
                    return Err(RuntimeError::new(
                        "provider preparation cancelled while waiting for context capacity",
                    ));
                }
            };
            match decision {
                ResourceAdmissionDecision::Granted { lease, .. } => Some(lease),
                decision => {
                    return Err(RuntimeError::context_capacity(
                        crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                        preparation_capacity_failure(
                            "prepared content capacity unavailable",
                            &decision,
                        ),
                    ))
                }
            }
        } else {
            if self.execution_supervisor.is_some() {
                return Err(RuntimeError::preparation(
                    ProviderPreparationFailure::MissingResourceOwner,
                    "governed Provider preparation has no original resident resource owner",
                ));
            }
            None
        };
        let (entry, message) = self.prepare_message_request(request, snapshot)?;
        provider::preflight_message_request(&message).map_err(|e| {
            RuntimeError::context_capacity(
                crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                e.to_string(),
            )
        })?;
        self.wire_preparations.fetch_add(1, Ordering::Relaxed);
        crate::execution_core::performance::observe_count("provider_wire_preparations_total", 1);
        let wire = entry
            .client
            .prepare_wire(&message)
            .map_err(|e| RuntimeError::new(format!("provider_render_failed:{e}")))?;
        request.budget.record_rendered_input(
            (wire.body_len() as u64).saturating_add(3) / 4,
            "provider_wire_utf8_bytes_div4_estimated_v1",
        );
        if !request.budget.executable {
            return Err(RuntimeError::context_capacity(
                crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                "rendered Provider body exceeds input capacity",
            ));
        }
        let source = PreparedRuntimeSource {
            prompt: request.prompt.clone(),
            history: request.messages.clone(),
            model: request.model.clone(),
            reasoning: request.reasoning_effort_override.clone(),
            budget: request.budget.clone(),
            inventory: schema.inventory,
            required: self.tool_choice_required,
            required_name: self.tool_choice_required_tool_name.clone(),
            configured_reasoning: self.reasoning_effort.clone(),
        };
        request.preparation.prepared = Some(Arc::new(PreparedRuntimeRequest {
            source,
            entry,
            message,
            wire,
            consumed: std::sync::atomic::AtomicBool::new(false),
            _resident: reservation,
        }));
        Ok(())
    }

    fn prepare_message_request(
        &self,
        request: &ApiRequest,
        snapshot: &ProviderRegistrySnapshot,
    ) -> Result<(ProviderEntry, MessageRequest), RuntimeError> {
        if request.budget.compiled_provider_registry_revision
            .is_some_and(|revision| revision != snapshot.revision())
        {
            return Err(RuntimeError::new("provider_profile_changed_after_compile"));
        }
        let active_tools = self.compiled_tool_schema().tools.to_vec();
        let required_tool_name = self.tool_choice_required_tool_name.as_deref();
        if required_tool_name.is_some_and(|name| !active_tools.iter().any(|tool| tool.name == name)) {
            return Err(RuntimeError::new("required governed tool is not exposed"));
        }
        let mut entry = self.template_cache.resolve(
            snapshot,
            &self.transport_pool,
            &request.model,
        )
            .map_err(|error| RuntimeError::new(format!("provider candidate unavailable: {error}")))?;
        if self.transport_pool.provider_account_is_fenced(
            entry.request_context.profile.registry_revision,
            &entry.request_context.profile.provider_name,
        ) {
            return Err(provider_account_fenced_error(&entry.request_context.profile.provider_name));
        }
        let reasoning_effort = request_reasoning_effort(
            &entry.model,
            &entry.request_context.profile.model_capabilities,
            request.reasoning_effort_override.clone(),
            self.reasoning_effort.clone().or_else(|| snapshot.resolve(&entry.model).and_then(|provider| provider.reasoning_effort.clone())),
        );
        if let Some(protocol) = entry.request_context.profile.protocol.as_deref()
            .and_then(ProviderProtocol::parse)
        {
            entry.request_context.profile.capabilities =
                ProviderCapabilityProfile::resolve_for_reasoning_mode(
                    protocol, &entry.model, reasoning_effort.as_deref(),
                );
        }
        let tool_choice = provider_tool_choice_for_required_tool(
            !active_tools.is_empty(),
            self.tool_choice_required,
            entry.request_context.profile.capabilities.supports_explicit_tool_choice.state,
            required_tool_name,
        ).map_err(|error| RuntimeError::new(format!(
            "{error} (model={}, reasoning_effort={reasoning_effort:?})", entry.model,
        )))?;
        entry.request_context.attempt = provider_attempt(request.provider_evidence_context.as_ref());
        let message_request = MessageRequest {
            model: entry.model.clone(),
            protocol_capabilities: entry.request_context.profile.model_capabilities.clone(),
            max_tokens: request.budget.requested_output_tokens.min(u64::from(u32::MAX)) as u32,
            context_window_limit: Some(request.budget.context_window_tokens.min(u64::from(u32::MAX)) as u32),
            messages: self.provider_input_messages(request),
            system: request.prompt.wire_system_text(),
            system_cache_prefix: request.prompt.cache_cohort_system_text(),
            tools: (!active_tools.is_empty()).then_some(active_tools),
            tool_choice,
            parallel_tool_calls: entry.request_context.profile.effective_parallel_tool_calls,
            stream: true,
            reasoning_effort,
            temperature: evaluation_request_temperature(),
            ..Default::default()
        };
        Ok((entry, message_request))
    }

    fn provider_input_messages(&self, request: &ApiRequest) -> Vec<InputMessage> {
        let history = convert_messages(request.messages.iter());
        let immutable_prefix = request
            .prompt
            .cache_cohort_user_prefix_messages()
            .into_iter()
            .chain(request.prompt.immutable_user_prefix_messages())
            .map(InputMessage::user_text)
            .collect::<Vec<_>>();
        // Keep all immutable material before append-only history. Sibling
        // Agents still share the cohort portion, while each Agent's private
        // role brief becomes a stable prefix for all of its continuations.
        // Moving that brief behind history makes every new assistant/tool
        // message splice in front of it and destroys provider prefix reuse.
        let context = request
            .prompt
            .contextual_messages()
            .into_iter()
            .map(InputMessage::user_text)
            .collect::<Vec<_>>();
        compile_provider_input_messages(immutable_prefix, history, context)
    }
}

fn compile_provider_input_messages(
    immutable_prefix: Vec<InputMessage>,
    history: Vec<InputMessage>,
    context: Vec<InputMessage>,
) -> Vec<InputMessage> {
    // Provider input is rebuilt from canonical sources on every request:
    // stable cohort material first, the actual append-only conversation next,
    // and exactly one current Runtime capsule at the tail. Dynamic clock,
    // policy, evidence, and checkpoint messages never become history, so old
    // capsules cannot accumulate or invalidate the reusable prefix.
    let mut wire = Vec::with_capacity(immutable_prefix.len() + history.len() + context.len());
    wire.extend(immutable_prefix);
    wire.extend(history);
    wire.extend(context);
    wire
}

#[allow(clippy::too_many_arguments)]
async fn forward_provider_attempt(
    entry: ProviderEntry,
    prompt_composition: Option<crate::PromptComposition>,
    prepared: Arc<PreparedRuntimeRequest>,
    dispatch_fence: ProviderDispatchSourceFence,
    cache_cohort_system: Option<String>,
    cache_cohort_user_prefix: Vec<String>,
    emit_output: bool,
    stream_callback: Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    evidence_context: Option<ProviderRequestEvidenceContext>,
    evidence_writer: Option<Arc<dyn ProviderWireEvidenceWriter>>,
    bound_step: Option<Arc<crate::step_snapshot::BoundStep>>,
    transport_pool: Arc<crate::ProviderTransportPool>,
    transport_activity: provider::TransportActivity,
    sender: tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    control: &ProviderAttemptControl,
) -> Result<crate::conversation::ProviderLocalExit, RuntimeError> {
    if control.is_cancelled() {
        return Err(RuntimeError::new("provider operation stopped before HTTP"));
    }
    let request_context = &entry.request_context;
    tracing::debug!(
        provider_request_id = %request_context.request_id,
        provider = %request_context.profile.provider_name,
        model = %request_context.profile.model,
        registry_revision = request_context.profile.registry_revision,
        transport_fingerprint = request_context.transport_fingerprint.0,
        attempt = request_context.attempt,
        "starting request-local provider attempt"
    );
    let message_request = &prepared.message;
    let wire_request = prepared.wire.wire_request();
    // The tool schema array as it is serialized on this request. It is the region
    // covered by cache breakpoint 2 and sits ahead of every other segment in the
    // reusable prefix, so its size is what decides whether "append a contract
    // summary" can reduce reuse at all. Measured from the wire body rather than from
    // the definitions, so it is comparable with `model_visible_prompt_bytes`.
    let step_render = if let Some(step) = &bound_step {
        let snapshot = step.snapshot();
        if evidence_context.as_ref().is_none_or(|context| {
            context.session_id != snapshot.identity.session_id
                || context.turn_id != snapshot.identity.turn_id
                || context.execution_id
                    != snapshot
                        .identity
                        .execution_id
                        .as_deref()
                        .unwrap_or_default()
                || snapshot
                    .identity
                    .attempt
                    .is_some_and(|attempt| context.attempt != attempt)
        }) || evidence_writer.is_none()
        {
            return Err(RuntimeError::new(
                "bound provider step requires matching durable wire evidence",
            ));
        }
        let value = match serde_json::to_value(&wire_request) {
            Ok(value) => value,
            Err(error) => return Err(RuntimeError::new(error.to_string())),
        };
        let render_digest =
            match harness_contract::step::canonical_digest("provider-wire-render", &value) {
                Ok(digest) => digest,
                Err(error) => return Err(RuntimeError::new(error.to_string())),
            };
        Some(harness_contract::step::StepRenderRef {
            step_id: snapshot.identity.step_id.clone(),
            attempt_id: snapshot.provider_attempt.attempt_id.clone(),
            catalog_digest: snapshot.catalog_digest.clone(),
            render_digest,
        })
    } else {
        None
    };
    let tool_array_bytes = wire_request.body.get("tools").map_or(0, |tools| {
        serde_json::to_vec(tools).unwrap_or_default().len()
    });
    let prompt_composition =
        prompt_composition.map(|composition| composition.with_tool_array_bytes(tool_array_bytes));
    let security_domain = evidence_context
        .as_ref()
        .map_or(entry.request_context.request_id.as_str(), |context| {
            context.session_id.as_str()
        });
    let (cache_identity_sha256, canonical_prompt, stable_head_len) = provider_cache_material(
        &entry.request_context,
        &wire_request,
        security_domain,
        cache_cohort_system.as_deref(),
        &cache_cohort_user_prefix,
    );
    let cache_behavior = ProviderCapabilityProfile::prompt_cache_behavior(&entry.model);
    let warmup = tokio::select! {
        warmup = transport_pool.acquire_cache_warmup(
            &cache_identity_sha256,
            &canonical_prompt,
            std::time::Duration::from_millis(cache_behavior.persistence_barrier_ms),
            cache_behavior.requires_common_prefix_discovery,
        ) => warmup,
        () = control.cancelled() => return Err(RuntimeError::new("provider operation stopped during warmup")),
    };
    // PLAN A2: the stable leading cohort is the first wire item for
    // OpenAI-compatible transports (the system message) or the separately carried
    // system bytes. Its length in the canonical prompt is the boundary between
    // "must hold still" and "expected to grow".
    let mut prefix_observation = transport_pool.observe_prompt_prefix(
        cache_identity_sha256,
        entry.request_context.request_id.clone(),
        canonical_prompt,
        stable_head_len,
    );
    prefix_observation.cold_leader = warmup.is_first_request_leader();
    prefix_observation.composition = prompt_composition;
    prefix_observation.common_prefix_leader = warmup.is_common_prefix_leader();
    prefix_observation.warmup_bypassed_low_reuse = warmup.bypassed_low_reuse;
    prefix_observation.waited_for_warmup = warmup.waited;
    prefix_observation.warmup_unobserved_reason = warmup.unobserved_reason.map(str::to_owned);
    if let (Some(context), Some(writer)) = (evidence_context.as_ref(), evidence_writer.as_ref()) {
        if let Err(error) = writer
            .persist(
                context,
                ProviderWireEvidence {
                    step_render,
                    request_context: entry.request_context.clone(),
                    wire_request: wire_request.clone(),
                    prefix_observation,
                },
            )
            .await
        {
            return Err(error);
        }
    }
    // Persistence/cohort waits never turn a formerly prepared request into
    // fresh authorization. Read original live owners immediately before HTTP.
    if let Err(error) = dispatch_fence.validate() {
        return Err(error);
    }
    if control.is_cancelled() {
        return Err(RuntimeError::new("provider operation stopped before HTTP"));
    }
    if transport_pool.provider_account_is_fenced(
        entry.request_context.profile.registry_revision,
        &entry.request_context.profile.provider_name,
    ) {
        return Err(provider_account_fenced_error(
            &entry.request_context.profile.provider_name,
        ));
    }
    let request_guard = transport_pool.begin_request();
    let mut attempt_usage = None;
    let mut attempt_cache_dimensions_observed = false;
    let stream_result = Box::pin(forward_provider_stream(
        &entry.client,
        &message_request,
        &prepared.wire,
        &entry.model,
        &entry.request_context.profile,
        emit_output,
        stream_callback,
        transport_activity,
        &sender,
        &mut attempt_usage,
        &mut attempt_cache_dimensions_observed,
        control,
        &entry.request_context.request_id,
        evidence_writer.as_deref(),
    ))
    .await;
    // Provider transport capacity is unrelated to disk-cache persistence.
    // Release it before the bounded cohort barrier so different cache keys
    // and already-warm traffic keep their full concurrency.
    drop(request_guard);
    if let (Some(context), Some(writer)) = (evidence_context.as_ref(), evidence_writer.as_ref()) {
        let terminal_status = match &stream_result {
            Ok(ForwardedProviderStream::Completed) => "completed",
            Ok(ForwardedProviderStream::ConsumerDropped) => "consumer_dropped",
            Ok(ForwardedProviderStream::Stopped | ForwardedProviderStream::UserCancelled(_)) => "cancelled",
            Err(_) => "failed",
        };
        if let Err(error) = writer
            .persist_outcome(
                context,
                ProviderAttemptOutcomeEvidence {
                    request_id: entry.request_context.request_id.clone(),
                    logical_attempt: context.attempt,
                    terminal_status: terminal_status.to_string(),
                    usage: attempt_usage,
                    cache_dimensions_observed: attempt_cache_dimensions_observed,
                    protocol_diagnostic: stream_result
                        .as_ref()
                        .err()
                        .and_then(|error| error.protocol_diagnostic.clone()),
                },
            )
            .await
        {
            return Err(error);
        }
    }
    // A dropped Runtime consumer is not evidence that the Provider completed
    // the cold request and committed its prefix.  Publishing Warm here would
    // let followers stampede into a cache entry that may never have existed.
    let provider_completed = matches!(stream_result, Ok(ForwardedProviderStream::Completed));
    let local_exit = match &stream_result {
        Ok(ForwardedProviderStream::UserCancelled(proof)) => crate::conversation::ProviderLocalExit::UserCancelled(proof.clone()),
        Ok(ForwardedProviderStream::Stopped | ForwardedProviderStream::ConsumerDropped) => crate::conversation::ProviderLocalExit::Stopped,
        _ => crate::conversation::ProviderLocalExit::Completed,
    };
    // `MessageStop` is the Runtime consumer's terminal boundary. Publishing
    // it before the Provider outcome and cache-persistence barrier are
    // committed lets the consumer drop this stream and abort the producer in
    // that narrow window, which rolls a successfully primed cache cohort back
    // to Cold. Keep terminal publication last. A consumer cancellation before
    // this point still drops the sender and correctly prevents false warming.
    if !commit_provider_cache_before_terminal(warmup, provider_completed, &sender).await {
        return Ok(local_exit);
    }
    if let Err(error) = stream_result {
        if let Some(error) = error.physical_failure.as_ref() {
            return Err(error.clone());
        }
        tracing::warn!(
            model = %entry.model,
            error = %error,
            protocol_diagnostic = ?error.protocol_diagnostic.as_ref().map(BoundProviderProtocolDiagnostic::metadata),
            "provider stream attempt failed before terminal completion"
        );
        let provider_context_window_limit = error.error.context_window_limit_hint();
        let provider_tool_protocol_failure = error.error.is_compatibility_tool_protocol_failure();
        let provider_failure_scope = error.error.failure_scope();
        transport_pool.record_provider_failure(
            entry.request_context.profile.registry_revision,
            &entry.request_context.profile.provider_name,
            provider_failure_scope,
        );
        let provider_resource_result = if error.error.is_downstream_overload() {
            crate::execution_core::graph::ResourceResultClass::DownstreamOverload
        } else if error.error.is_timeout() {
            crate::execution_core::graph::ResourceResultClass::TimedOut
        } else {
            crate::execution_core::graph::ResourceResultClass::Failed
        };
        let _ = sender
            .send(Err(
                RuntimeError::with_provider_failure_metadata_retry_after_and_scope(
                    error.error.to_string(),
                    provider_context_window_limit,
                    provider_tool_protocol_failure,
                    provider_resource_result,
                    error.error.retry_after(),
                    error.error.is_retryable(),
                    provider_failure_scope,
                ),
            ))
            .await;
    }
    Ok(local_exit)
}
async fn commit_provider_cache_before_terminal(
    mut warmup: crate::provider_transport_pool::ProviderCacheWarmupGuard,
    provider_completed: bool,
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
) -> bool {
    warmup
        .finish_after_persistence_barrier(provider_completed)
        .await;
    !provider_completed || sender.send(Ok(AssistantEvent::MessageStop)).await.is_ok()
}

fn provider_cache_material(
    context: &ProviderRequestContext,
    wire: &provider::ProviderWireRequest,
    security_domain: &str,
    cache_cohort_system: Option<&str>,
    _cache_cohort_user_prefix: &[String],
) -> (String, Vec<u8>, usize) {
    let mut properties = wire.body.clone();
    let prompt_value = properties
        .as_object_mut()
        .map(|object| {
            let prompt = object
                .remove("messages")
                .or_else(|| object.remove("input"))
                .unwrap_or(serde_json::Value::Null);
            for transport_or_output_key in [
                "max_tokens",
                "max_completion_tokens",
                "max_output_tokens",
                "stream",
                "stream_options",
            ] {
                object.remove(transport_or_output_key);
            }
            prompt
        })
        .unwrap_or(serde_json::Value::Null);
    let mut canonical_prompt = Vec::new();
    let mut stable_head_len = 0_usize;
    let wire_stable_cohort_bytes = if let Some(items) = prompt_value.as_array() {
        let first = items
            .first()
            .map(|item| serde_json::to_vec(item).unwrap_or_default())
            .unwrap_or_default();
        for item in items {
            let encoded = serde_json::to_vec(item).unwrap_or_default();
            canonical_prompt.extend_from_slice(encoded.len().to_string().as_bytes());
            canonical_prompt.push(b':');
            canonical_prompt.extend_from_slice(&encoded);
            canonical_prompt.push(b'\n');
            if stable_head_len == 0 {
                stable_head_len = canonical_prompt.len();
            }
        }
        first
    } else {
        canonical_prompt = serde_json::to_vec(&prompt_value).unwrap_or_default();
        canonical_prompt.clone()
    };
    let stable_cohort_bytes = cache_cohort_system
        .map(str::as_bytes)
        .unwrap_or(wire_stable_cohort_bytes.as_slice());
    // Singleflight is scoped to the immutable leading cohort, not merely to
    // provider/model/schema. Otherwise two unrelated cold prompts could share
    // one coordinator key: the first would publish Warm even though it built
    // no reusable prefix for the second. The first wire item is the stable
    // system message for OpenAI-compatible transports; providers with a
    // separate system field already carry it in `properties`.
    let stable_cohort_sha256 = format!("sha256:{:x}", Sha256::digest(stable_cohort_bytes));
    // The cache coordinator key is a *provider/account cohort*, not a copy of
    // the complete request schema. Tool exposure, tool_choice, sampling and
    // other request-local controls are intentionally excluded: delegated
    // Agents may have different least-privilege overlays while still sharing
    // the same immutable system/history prefix. The transport/provider remains
    // the source of truth for actual prompt-cache billing; this key only lets
    // the Runtime compare the real canonical prompt bytes and single-flight
    // compatible prefixes instead of manufacturing one cold leader per role.
    // Security fencing stays session/account scoped so no cache coordination
    // crosses an authenticated execution domain.
    let identity = serde_json::json!({
        "provider": context.profile.provider_name,
        "base_url": context.profile.base_url,
        "model": context.profile.model,
        "protocol": wire.protocol,
        "endpoint": wire.endpoint,
        "transport_fingerprint": context.transport_fingerprint.0,
        "security_domain": security_domain,
        "cache_cohort_schema_version": 2,
        "stable_cohort_sha256": stable_cohort_sha256,
    });
    let identity_bytes = serde_json::to_vec(&identity).unwrap_or_default();
    let cache_identity_sha256 = format!("sha256:{:x}", Sha256::digest(identity_bytes));
    // PLAN A2: hand back how many bytes of the canonical prompt are the stable
    // leading cohort, so the prefix observer can attribute a divergence to
    // "the stable head moved" versus "the history grew" instead of only
    // reporting a ratio.
    (cache_identity_sha256, canonical_prompt, stable_head_len)
}

fn provider_account_fenced_message(provider_account: &str) -> String {
    format!(
        "provider account `{provider_account}` is temporarily fenced after an account-scoped failure; retry after the burst window or reload provider configuration"
    )
}

fn provider_account_fenced_error(provider_account: &str) -> RuntimeError {
    RuntimeError::with_provider_failure_metadata_retry_after_and_scope(
        provider_account_fenced_message(provider_account),
        None,
        false,
        crate::execution_core::graph::ResourceResultClass::Failed,
        None,
        false,
        model_protocol::provider_failure::ProviderFailureScope::Account,
    )
}

fn evaluation_request_temperature() -> Option<f64> {
    (std::env::var("COWD_EVAL_HARNESS").as_deref() == Ok("1")
        && matches!(
            std::env::var("COWD_EVAL_CORPUS_ID").ok().as_deref(),
            Some("auto-strategy-v1" | "live-scenarios-v1")
        ))
    .then(|| {
        std::env::var("COWD_MODEL_TEMPERATURE")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    })
    .flatten()
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ForwardedProviderStream {
    Completed,
    ConsumerDropped,
    Stopped,
    UserCancelled(Box<AuthenticatedProviderCancellation>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundProviderProtocolDiagnostic {
    identity: ProviderPhysicalIdentity,
    opened_sequence: usize,
    opened_digest: String,
    decoder: serde_json::Value,
}

impl BoundProviderProtocolDiagnostic {
    fn metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "physical_identity": self.identity,
            "opened_sequence": self.opened_sequence,
            "opened_digest": self.opened_digest,
            "decoder": self.decoder,
            "persistence_authority": "original_outcome_receipt_only"
        })
    }

    /// Revalidate the sealed binding against the original packed request, not
    /// just its logical sequence. Success constructs a pending payload; only
    /// the original journal append receipt can establish that it was recorded.
    pub(crate) fn outcome_metadata(
        &self,
        context: &ProviderRequestEvidenceContext,
        outcome: &ProviderAttemptOutcomeEvidence,
        packed_step: &harness_contract::step::StepRenderRef,
    ) -> Result<serde_json::Value, RuntimeError> {
        let opened = ProviderPhysicalObservation::ResponseOpened {
            identity: self.identity.clone(),
        };
        opened.validate(context)?;
        if self.identity.request_id != outcome.request_id
            || self.identity.logical_attempt != outcome.logical_attempt
            || outcome.logical_attempt != context.attempt
            || outcome.terminal_status != "failed"
            || packed_step.step_id != self.identity.step.step_id
            || packed_step.attempt_id != self.identity.provider_attempt.attempt_id
            || opened.digest()? != self.opened_digest
        {
            return Err(RuntimeError::new(
                "Provider diagnostic does not match its original packed request/open acknowledgement",
            ));
        }
        // Do not copy the decoder's in-memory capture status into a claim that
        // ordinary session visibility provides encrypted response storage.
        let mut metadata = serde_json::json!({
            "schema_version": 1,
            "physical_identity": self.identity,
            "opened_sequence": self.opened_sequence,
            "opened_digest": self.opened_digest,
            "stage": self.decoder["stage"], "class": self.decoder["class"],
            "scope": self.decoder["scope"],
            "observed_bytes": self.decoder["observed_bytes"],
            "sha256": self.decoder["sha256"],
            "capture_omitted": self.decoder["omitted"],
            "capture_limit": self.decoder["capture_limit"],
            "raw_omitted": true, "raw_persisted": false, "raw_replayable": false,
            "raw_omission_reason": "private_encrypted_artifact_unavailable"
        });
        if let Some(predicate) = self.decoder.get("closure_predicate") {
            metadata["closure_predicate"] = predicate.clone();
        }
        Ok(metadata)
    }
}

/// Only the original acknowledged response can bind decoder-local evidence.
/// This is not a persistence receipt and never mints a replacement identity.
pub(crate) fn bind_provider_protocol_diagnostic(
    error: &ApiError,
    context: &ProviderRequestEvidenceContext,
    step: &harness_contract::step::StepIdentity,
    attempt: &harness_contract::step::ProviderAttemptIdentity,
    request_id: &str,
    lease: &str,
    opened: &AcknowledgedProviderPhysicalObservation,
) -> Option<BoundProviderProtocolDiagnostic> {
    let decoder = error.compatibility_tool_frame_metadata()?;
    let id = &opened.receipt.identity;
    let observation = ProviderPhysicalObservation::ResponseOpened {
        identity: id.clone(),
    };
    if observation.validate(context).is_err()
        || &id.step != step
        || &id.provider_attempt != attempt
        || id.request_id != request_id
        || id.resource_lease != lease
        || observation.digest().ok().as_ref() != Some(&opened.receipt.observation_digest)
    {
        return None;
    }
    Some(BoundProviderProtocolDiagnostic {
        identity: id.clone(),
        opened_sequence: opened.receipt.session_sequence,
        opened_digest: opened.receipt.observation_digest.clone(),
        decoder,
    })
}

#[derive(Debug)]
struct ProviderStreamError {
    error: ApiError,
    physical_failure: Option<RuntimeError>,
    protocol_diagnostic: Option<BoundProviderProtocolDiagnostic>,
}

impl ProviderStreamError {
    fn physical(error: RuntimeError) -> Self {
        Self {
            error: ApiError::InvalidSseFrame("physical Provider evidence rejected"),
            physical_failure: Some(error),
            protocol_diagnostic: None,
        }
    }

    fn unbound(error: ApiError) -> Self {
        Self {
            error,
            physical_failure: None,
            protocol_diagnostic: None,
        }
    }

    fn after_open(
        error: ApiError,
        control: &ProviderAttemptControl,
        request_id: &str,
        opened: Option<&AcknowledgedProviderPhysicalObservation>,
    ) -> Self {
        let protocol_diagnostic = match (&control.context, &control.step, &control.lease, opened) {
            (Some(context), Some(step), Some(lease), Some(opened)) => {
                let snapshot = step.snapshot();
                bind_provider_protocol_diagnostic(
                    &error,
                    context,
                    &snapshot.identity,
                    &snapshot.provider_attempt,
                    request_id,
                    &lease.projection_reference(),
                    opened,
                )
            }
            _ => None,
        };
        Self {
            error,
            physical_failure: None,
            protocol_diagnostic,
        }
    }
}

impl std::fmt::Display for ProviderStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

// Opt-in, bounded, private diagnostics: metadata only, never request/response
// bytes, headers, tokens or errors. A missing/unusable file cannot alter the
// provider path. The launcher creates the owned 0600 file in its 0700 root.
struct ProviderStreamDiag {
    file: Option<std::fs::File>,
    request: String,
    response: String,
    transport_records: u8,
    activity: provider::TransportActivity,
}
impl ProviderStreamDiag {
    fn new(request: &str, response: &str, activity: provider::TransportActivity) -> Self {
        let file = std::env::var_os("COWD_PROVIDER_STREAM_DIAG_FILE").and_then(|path| {
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
                let path = std::path::Path::new(&path);
                let parent = std::fs::symlink_metadata(path.parent()?).ok()?;
                if !parent.is_dir() || parent.uid() != unsafe { libc::geteuid() } || parent.mode() & 0o777 != 0o700 { return None; }
                let file = std::fs::OpenOptions::new().append(true).custom_flags(libc::O_NOFOLLOW).open(path).ok()?;
                let meta = file.metadata().ok()?;
                if !meta.is_file() || meta.uid() != parent.uid() || meta.mode() & 0o777 != 0o600 || meta.len() >= 1_048_576 { return None; }
                Some(file)
            }
            #[cfg(not(unix))]
            { let _ = path; None }
        });
        Self { file, request: request.to_owned(), response: response.to_owned(), transport_records: 0, activity }
    }
    fn record(&mut self, stage: &str, activity: &provider::TransportActivity) {
        let Some(file) = self.file.as_mut() else { return; };
        if stage == "transport" {
            if self.transport_records >= 8 { return; }
            self.transport_records += 1;
        }
        let (chunks, bytes, frames, decoded, first_byte_ms, first_frame_ms, first_decoded_ms, first_consumed_ms) = activity.counters();
        let at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_millis());
        let row = serde_json::json!({"request_id":self.request,"response_id":self.response,"stage":stage,"at_ms":at,"chunks":chunks,"bytes":bytes,"frames":frames,"decoded_chunks":decoded,"first_byte_ms":first_byte_ms,"first_frame_ms":first_frame_ms,"first_decoded_ms":first_decoded_ms,"first_consumed_ms":first_consumed_ms});
        if let Ok(mut data) = serde_json::to_vec(&row) { data.push(b'\n'); let _ = file.write_all(&data); }
    }
}
impl Drop for ProviderStreamDiag {
    fn drop(&mut self) {
        let activity = self.activity.clone();
        self.record("producer_future_dropped", &activity);
    }
}

#[allow(clippy::too_many_lines)]
async fn forward_provider_stream(
    client: &ProviderClient,
    message_request: &MessageRequest,
    prepared_wire: &provider::PreparedProviderWire,
    effective_model: &str,
    resolved_profile: &ResolvedProviderProfile,
    emit_output: bool,
    stream_callback: Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    transport_activity: provider::TransportActivity,
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    attempt_usage: &mut Option<TokenUsage>,
    attempt_cache_dimensions_observed: &mut bool,
    control: &ProviderAttemptControl,
    request_id: &str,
    writer: Option<&dyn ProviderWireEvidenceWriter>,
) -> Result<ForwardedProviderStream, ProviderStreamError> {
    let mut stream = tokio::select! {
        biased;
        result = client.stream_prepared_message(message_request, prepared_wire) => result.map_err(ProviderStreamError::unbound)?,
        () = control.cancelled() => return Ok(ForwardedProviderStream::Stopped),
    };
    stream.set_transport_activity(transport_activity.clone());
    let identity = control
        .physical_identity(request_id)
        .map_err(ProviderStreamError::physical)?;
    let mut diag = ProviderStreamDiag::new(request_id, identity.as_ref().map_or("", |id| id.response_id.as_str()), transport_activity.clone());
    diag.record("response_opened", &transport_activity);
    let opened = if let Some(identity) = identity {
        let context = control
            .context
            .as_ref()
            .expect("physical identity validated context");
        let observation = ProviderPhysicalObservation::ResponseOpened { identity };
        let writer = writer.ok_or_else(|| {
            ProviderStreamError::physical(RuntimeError::new(
                "physical Provider has no original observation writer",
            ))
        })?;
        let mut persistence = Box::pin(writer.persist_physical(context, observation.clone(), None));
        let ack = tokio::select! {
            biased;
            ack = persistence.as_mut() => ack.map_err(ProviderStreamError::physical)?,
            () = control.cancelled() => {
                // No current-read observation yet: stop IO, retain the original
                // writer future, and never turn historical open into active C.
                diag.record("cancel_during_open_ack_start", &transport_activity);
                drop(stream);
                persistence.await.map_err(ProviderStreamError::physical)?;
                diag.record("cancel_during_open_ack_done", &transport_activity);
                return Ok(ForwardedProviderStream::Stopped);
            }
        };
        if ack.receipt.identity != *observation.identity()
            || ack.receipt.observation_digest
                != observation
                    .digest()
                    .map_err(ProviderStreamError::physical)?
        {
            return Err(ProviderStreamError::physical(RuntimeError::new(
                "open acknowledgement belongs to another physical response",
            )));
        }
        Some(ack)
    } else {
        None
    };
    diag.record("opened_ack", &transport_activity);
    let mut transport_generation = transport_activity.generation();
    let mut pending_tools: BTreeMap<u32, (String, String, String)> = BTreeMap::new();
    let mut saw_stop = false;
    let mut emitted = false;
    let mut provider_model_emitted = false;
    let mut semantic_recorded = false;
    let mut pending_text = String::new();
    let mut cumulative_usage = None;

    loop {
        let next = tokio::select! {
            biased;
            next = stream.next_event() => match next {
                Ok(next) => next,
                Err(error) => { diag.record("stream_error", &transport_activity); return Err(ProviderStreamError::after_open(error, control, request_id, opened.as_ref())); }
            },
            () = transport_activity.changed_since(transport_generation), if diag.file.is_some() => {
                transport_generation = transport_activity.generation();
                diag.record("transport", &transport_activity);
                continue;
            },
            () = control.cancelled() => {
                diag.record("cancel_start", &transport_activity);
                drop(stream);
                if saw_stop { diag.record("cancel_after_finish", &transport_activity); return Ok(ForwardedProviderStream::Completed); }
                let proof = control.record_cancel(writer, opened.as_ref()).await.map_err(ProviderStreamError::physical)?;
                diag.record("cancel_recorded", &transport_activity);
                return Ok(match proof {
                    Some(proof) => ForwardedProviderStream::UserCancelled(Box::new(proof)),
                    None => ForwardedProviderStream::Stopped,
                });
            }
        };
        let Some(event) = next else {
            diag.record("stream_eof", &transport_activity);
            break;
        };
        if !provider_model_emitted { diag.record("first_decoded_event", &transport_activity); }
        if matches!(&event, ApiStreamEvent::MessageStop(_)) { diag.record("decoded_finish", &transport_activity); }
        let first_semantic = !semantic_recorded && matches!(&event, ApiStreamEvent::ContentBlockDelta(_) | ApiStreamEvent::ContentBlockStart(_));
        if first_semantic { diag.record("first_content_event", &transport_activity); }
        if !provider_model_emitted {
            diag.record("runtime_queue_send_start", &transport_activity);
            if !forward_event(
                sender,
                AssistantEvent::ProviderModel {
                    identity: outcome_provider_identity(resolved_profile, effective_model),
                },
                emit_output,
                &stream_callback,
                &mut emitted,
            )
            .await
            {
                diag.record("runtime_queue_send_dropped", &transport_activity);
                return Ok(ForwardedProviderStream::ConsumerDropped);
            }
            diag.record("runtime_queue_send_accepted", &transport_activity);
            provider_model_emitted = true;
        }
        // A single event advances one action at a time. The shared action
        // executor keeps the original send/flush order and backpressure.
        enum EventAction {
            Flush,
            Send(AssistantEvent),
            SendMany(Vec<AssistantEvent>),
            Text(String),
            Continue,
            Done,
        }
        let mut event = Some(event);
        let mut phase = 0_u8;
        loop {
            let action = if phase == 0 {
                phase = 1;
                if matches!(event.as_ref(), Some(
                    ApiStreamEvent::MessageStart(_)
                    | ApiStreamEvent::ContentBlockStart(_)
                    | ApiStreamEvent::ContentBlockStop(_)
                    | ApiStreamEvent::MessageDelta(_)
                    | ApiStreamEvent::MessageStop(_)
                )) { EventAction::Flush } else { EventAction::Continue }
            } else if phase == 1 {
                match event.as_mut().expect("event remains until its action is prepared") {
                    ApiStreamEvent::MessageStart(start) => {
                        let mut events = Vec::new();
                        for (index, block) in std::mem::take(&mut start.message.content).into_iter().enumerate() {
                            let Ok(index) = u32::try_from(index) else { break; };
                            push_provider_output_block(block, index, &mut events, &mut pending_tools, true);
                        }
                        phase = 2;
                        EventAction::SendMany(events)
                    }
                    ApiStreamEvent::ContentBlockStart(_) => {
                        let Some(ApiStreamEvent::ContentBlockStart(start)) = event.take() else { unreachable!() };
                        let mut events = Vec::new();
                        push_provider_output_block(start.content_block, start.index, &mut events, &mut pending_tools, true);
                        phase = 3;
                        EventAction::SendMany(events)
                    }
                    ApiStreamEvent::ContentBlockDelta(_) => {
                        let Some(ApiStreamEvent::ContentBlockDelta(delta)) = event.take() else { unreachable!() };
                        phase = 3;
                        match delta.delta {
                            ContentBlockDelta::TextDelta { text } if !text.is_empty() => EventAction::Text(text),
                            ContentBlockDelta::TextDelta { .. } => EventAction::Continue,
                            ContentBlockDelta::InputJsonDelta { partial_json } => {
                                if let Some((_, _, input)) = pending_tools.get_mut(&delta.index) { input.push_str(&partial_json); }
                                EventAction::Continue
                            }
                            ContentBlockDelta::ReasoningSummaryDelta { text } => EventAction::Send(AssistantEvent::ReasoningSummaryDelta(text)),
                            ContentBlockDelta::ThinkingDelta { thinking } => EventAction::Send(AssistantEvent::PrivateReasoningDelta(thinking)),
                            ContentBlockDelta::SignatureDelta { signature } => EventAction::Send(AssistantEvent::SignatureDelta(signature)),
                        }
                    }
                    ApiStreamEvent::ContentBlockStop(stop) => {
                        phase = 2;
                        match pending_tools.remove(&stop.index) {
                            Some((id, name, input)) => EventAction::Send(AssistantEvent::ToolUse { id, name, input }),
                            None => EventAction::Continue,
                        }
                    }
                    ApiStreamEvent::MessageDelta(delta) => {
                        if delta.usage_observed {
                            let usage = merge_provider_usage_max(cumulative_usage, delta.usage.token_usage());
                            cumulative_usage = Some(usage);
                            *attempt_usage = Some(usage);
                            *attempt_cache_dimensions_observed |= delta.cache_dimensions_observed;
                        }
                        phase = 3;
                        match delta.delta.stop_reason.clone() {
                            Some(stop_reason) => EventAction::Send(AssistantEvent::MessageBoundary { stop_reason: Some(stop_reason) }),
                            None => EventAction::Continue,
                        }
                    }
                    ApiStreamEvent::MessageStop(_) => {
                        phase = 2;
                        match cumulative_usage {
                            Some(usage) => EventAction::Send(AssistantEvent::Usage(usage)),
                            None => EventAction::Continue,
                        }
                    }
                }
            } else if phase == 2 {
                phase = 3;
                match event.take().expect("event retained for its post-action") {
                    ApiStreamEvent::MessageStart(start) => {
                        if start.message.usage_observed {
                            let usage = merge_provider_usage_max(cumulative_usage, start.message.usage.token_usage());
                            cumulative_usage = Some(usage);
                            *attempt_usage = Some(usage);
                            *attempt_cache_dimensions_observed |= start.message.cache_dimensions_observed;
                        }
                        EventAction::Continue
                    }
                    ApiStreamEvent::ContentBlockStop(stop) => EventAction::Send(AssistantEvent::ItemCompleted { index: stop.index }),
                    ApiStreamEvent::MessageStop(_) => { saw_stop = true; EventAction::Continue }
                    _ => unreachable!("only events with a post-action reach this phase"),
                }
            } else {
                EventAction::Done
            };
            let accepted = match action {
                EventAction::Flush => flush_pending_text(sender, &mut pending_text, emit_output, &stream_callback, &mut emitted).await,
                EventAction::Send(event) => forward_event(sender, event, emit_output, &stream_callback, &mut emitted).await,
                EventAction::SendMany(events) => forward_events(sender, events, emit_output, &stream_callback, &mut emitted).await,
                EventAction::Text(text) => forward_text_delta(sender, text, &mut pending_text, emit_output, &stream_callback, &mut emitted).await,
                EventAction::Continue => true,
                EventAction::Done => break,
            };
            if !accepted { return Ok(ForwardedProviderStream::ConsumerDropped); }
        }
        if first_semantic { diag.record("first_content_handler_complete", &transport_activity); semantic_recorded = true; }
    }

    if !flush_pending_text(
        sender,
        &mut pending_text,
        emit_output,
        &stream_callback,
        &mut emitted,
    )
    .await
    {
        return Ok(ForwardedProviderStream::ConsumerDropped);
    }

    if saw_stop {
        return Ok(ForwardedProviderStream::Completed);
    }
    if emitted {
        // A transport EOF is not a Provider terminal. Treating partial text
        // or a finalized early-read item as MessageStop can silently discard
        // an unfinished tool call and bypass Runtime recovery. The graph Host
        // retains any durable read-only receipt and decides whether to retry.
        return Err(ProviderStreamError {
            error: ApiError::InvalidSseFrame("provider stream ended before terminal message_stop"),
            physical_failure: None,
            protocol_diagnostic: None,
        });
    }

    // This is a distinct nonstream SDK send. The preceding response-open is
    // deliberately out of scope: it cannot certify active cancellation here.
    drop(stream);
    forward_provider_nonstream_fallback(
        client, message_request, effective_model, resolved_profile, emit_output,
        &stream_callback, sender, attempt_usage, attempt_cache_dimensions_observed,
        control, &mut emitted,
    ).await
}

// Entered only after the original stream has been dropped at empty EOF.
async fn forward_provider_nonstream_fallback(
    client: &ProviderClient,
    message_request: &MessageRequest,
    effective_model: &str,
    resolved_profile: &ResolvedProviderProfile,
    emit_output: bool,
    stream_callback: &Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    attempt_usage: &mut Option<TokenUsage>,
    attempt_cache_dimensions_observed: &mut bool,
    control: &ProviderAttemptControl,
    emitted: &mut bool,
) -> Result<ForwardedProviderStream, ProviderStreamError> {
    let fallback_request = MessageRequest {
        stream: false,
        ..message_request.clone()
    };
    let response = tokio::select! {
        biased;
        response = client.send_message(&fallback_request) => response.map_err(ProviderStreamError::unbound)?,
        () = control.cancelled() => return Ok(ForwardedProviderStream::Stopped),
    };
    if response.usage_observed {
        *attempt_usage = Some(response.usage.token_usage());
        *attempt_cache_dimensions_observed = response.cache_dimensions_observed;
    }
    let mut events = response_to_events(response);
    events.retain(|event| !matches!(event, AssistantEvent::MessageStop));
    events.insert(
        0,
        AssistantEvent::ProviderModel {
            identity: outcome_provider_identity(resolved_profile, effective_model),
        },
    );
    if forward_events(sender, events, emit_output, stream_callback, emitted).await {
        Ok(ForwardedProviderStream::Completed)
    } else {
        Ok(ForwardedProviderStream::ConsumerDropped)
    }
}
fn merge_provider_usage_max(current: Option<TokenUsage>, next: TokenUsage) -> TokenUsage {
    let current = current.unwrap_or_default();
    TokenUsage {
        input_tokens: current.input_tokens.max(next.input_tokens),
        output_tokens: current.output_tokens.max(next.output_tokens),
        cache_creation_input_tokens: current
            .cache_creation_input_tokens
            .max(next.cache_creation_input_tokens),
        cache_read_input_tokens: current
            .cache_read_input_tokens
            .max(next.cache_read_input_tokens),
    }
}

fn outcome_provider_identity(
    profile: &ResolvedProviderProfile,
    effective_model: &str,
) -> harness_contract::outcome::ProviderIdentity {
    let mut capabilities = std::collections::BTreeMap::new();
    let mut insert = |name: &str, fact: model_protocol::provider_capability::CapabilityFact| {
        capabilities.insert(
            name.to_string(),
            format!("{:?}/{:?}", fact.state, fact.source).to_ascii_lowercase(),
        );
    };
    insert("tool_calls", profile.capabilities.supports_tool_calls);
    insert(
        "explicit_tool_choice",
        profile.capabilities.supports_explicit_tool_choice,
    );
    insert(
        "multiple_tool_calls",
        profile.capabilities.supports_multiple_tool_calls,
    );
    insert(
        "parallel_tool_calls_request",
        profile.capabilities.supports_parallel_tool_calls_request,
    );
    insert(
        "stream_tool_arguments",
        profile.capabilities.streams_tool_arguments,
    );
    insert(
        "public_reasoning_summary",
        profile.capabilities.supports_public_reasoning_summary,
    );
    insert(
        "reasoning_signature_roundtrip",
        profile.capabilities.requires_reasoning_signature_roundtrip,
    );
    for capability in &profile.model_capabilities {
        capabilities.insert(format!("model:{capability}"), "configured".to_string());
    }
    capabilities.insert(
        "early_tool_start".to_string(),
        if profile.effective_early_tool_start {
            "enabled"
        } else {
            "disabled"
        }
        .to_string(),
    );
    harness_contract::outcome::ProviderIdentity {
        registry_revision: Some(profile.registry_revision),
        provider_name: profile.provider_name.clone(),
        model: effective_model.to_string(),
        profile: None,
        protocol: profile.protocol.clone(),
        capabilities,
    }
}

async fn forward_events(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    events: Vec<AssistantEvent>,
    emit_output: bool,
    stream_callback: &Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    for event in events {
        if !forward_event(sender, event, emit_output, stream_callback, emitted).await {
            return false;
        }
    }
    true
}

async fn forward_event(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    event: AssistantEvent,
    emit_output: bool,
    stream_callback: &Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    if let AssistantEvent::TextDelta(text) = &event {
        if emit_output {
            print!("{text}");
            let _ = std::io::stdout().flush();
        }
        if let Some(callback) = stream_callback {
            let _ = callback.try_send(crate::CowdEvent::TextDelta { text: text.clone() });
        }
    }
    *emitted = true;
    let producer_wait_started = Instant::now();
    let sent = sender.send(Ok(event)).await.is_ok();
    crate::execution_core::performance::observe_duration(
        "provider_producer_wait_ms",
        producer_wait_started.elapsed(),
    );
    sent
}

async fn forward_text_delta(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    text: String,
    pending_text: &mut String,
    emit_output: bool,
    stream_callback: &Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    if pending_text.is_empty() && sender.capacity() > 0 {
        return forward_event(
            sender,
            AssistantEvent::TextDelta(text),
            emit_output,
            stream_callback,
            emitted,
        )
        .await;
    }
    pending_text.push_str(&text);
    if pending_text.len() < MAX_COALESCED_TEXT_BYTES && sender.capacity() == 0 {
        return true;
    }
    flush_pending_text(sender, pending_text, emit_output, stream_callback, emitted).await
}

async fn flush_pending_text(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    pending_text: &mut String,
    emit_output: bool,
    stream_callback: &Option<tokio::sync::mpsc::Sender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    if pending_text.is_empty() {
        return true;
    }
    let text = std::mem::take(pending_text);
    forward_event(
        sender,
        AssistantEvent::TextDelta(text),
        emit_output,
        stream_callback,
        emitted,
    )
    .await
}

pub(crate) fn convert_messages<'a>(
    messages: impl IntoIterator<Item = &'a ConversationMessage>,
) -> Vec<InputMessage> {
    messages
        .into_iter()
        .filter_map(|message| {
            let role = match message.role {
                MessageRole::System | MessageRole::User | MessageRole::Tool => "user",
                MessageRole::Assistant => "assistant",
            };
            let content = message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => {
                        Some(InputContentBlock::Text { text: text.clone() })
                    }
                    // Public summaries are a Surface/history projection. The
                    // opaque private Provider transcript below is the only
                    // reasoning state returned on the wire.
                    ContentBlock::ReasoningSummary { .. } => None,
                    ContentBlock::Image {
                        media_type, data, ..
                    } => Some(InputContentBlock::Image {
                        source: ImageSource::base64(media_type.clone(), data.clone()),
                    }),
                    ContentBlock::Thinking {
                        thinking,
                        signature,
                    } => Some(InputContentBlock::Thinking {
                        thinking: thinking.clone(),
                        signature: signature.clone(),
                    }),
                    ContentBlock::ToolUse { id, name, input } => Some(InputContentBlock::ToolUse {
                        id: id.clone(),
                        name: name.clone(),
                        input: serde_json::from_str(input)
                            .unwrap_or_else(|_| serde_json::json!({ "raw": input })),
                    }),
                    ContentBlock::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                        ..
                    } => Some(InputContentBlock::ToolResult {
                        tool_use_id: tool_use_id.clone(),
                        content: vec![ToolResultContentBlock::Text {
                            text: output.clone(),
                        }],
                        is_error: *is_error,
                    }),
                })
                .collect::<Vec<_>>();
            (!content.is_empty()).then(|| InputMessage {
                role: role.to_string(),
                content,
            })
        })
        .collect()
}

pub fn push_provider_output_block(
    block: OutputContentBlock,
    block_index: u32,
    events: &mut Vec<AssistantEvent>,
    pending_tools: &mut BTreeMap<u32, (String, String, String)>,
    streaming_tool_input: bool,
) {
    match block {
        OutputContentBlock::Text { text } => {
            events.push(AssistantEvent::ItemStarted {
                index: block_index,
                provider_item_id: None,
                kind: crate::AssistantItemKind::Text,
            });
            if !text.is_empty() {
                events.push(AssistantEvent::TextDelta(text));
            }
        }
        OutputContentBlock::ReasoningSummary { text } => {
            events.push(AssistantEvent::ItemStarted {
                index: block_index,
                provider_item_id: None,
                kind: crate::AssistantItemKind::PublicReasoning,
            });
            if !text.is_empty() {
                events.push(AssistantEvent::ReasoningSummaryDelta(text));
            }
        }
        OutputContentBlock::ToolUse { id, name, input } => {
            events.push(AssistantEvent::ItemStarted {
                index: block_index,
                provider_item_id: Some(id.clone()),
                kind: crate::AssistantItemKind::ToolCall,
            });
            let initial_input = if streaming_tool_input
                && (input.is_null()
                    || (input.is_object()
                        && input.as_object().is_some_and(serde_json::Map::is_empty)))
            {
                String::new()
            } else {
                input.to_string()
            };
            pending_tools.insert(block_index, (id, name, initial_input));
        }
        OutputContentBlock::Thinking {
            thinking,
            signature,
        } => {
            events.push(AssistantEvent::ItemStarted {
                index: block_index,
                provider_item_id: None,
                kind: crate::AssistantItemKind::PrivateReasoning,
            });
            if !thinking.is_empty() {
                events.push(AssistantEvent::PrivateReasoningDelta(thinking));
            }
            if let Some(signature) = signature.filter(|value| !value.is_empty()) {
                events.push(AssistantEvent::SignatureDelta(signature));
            }
        }
        // Redacted/private reasoning is a Provider transcript artifact. It is
        // intentionally not projected into Runtime's public causal stream.
        OutputContentBlock::RedactedThinking { .. } => {}
    }
}

fn response_to_events(response: MessageResponse) -> Vec<AssistantEvent> {
    let mut events = Vec::new();
    let mut pending_tools = BTreeMap::new();

    for (index, block) in response.content.into_iter().enumerate() {
        let Ok(index) = u32::try_from(index) else {
            break;
        };
        push_provider_output_block(block, index, &mut events, &mut pending_tools, false);
        if let Some((id, name, input)) = pending_tools.remove(&index) {
            events.push(AssistantEvent::ToolUse { id, name, input });
        }
        events.push(AssistantEvent::ItemCompleted { index });
    }

    if response.usage_observed {
        events.push(AssistantEvent::Usage(response.usage.token_usage()));
    }
    events.push(AssistantEvent::MessageStop);
    events
}

#[cfg(test)]
mod tests {
    use super::{
        build_provider_entry, commit_provider_cache_before_terminal,
        compile_provider_input_messages, forward_text_delta, merge_provider_usage_max,
        provider_attempt, provider_cache_material, provider_tool_choice,
        provider_tool_choice_for_required_tool, push_provider_output_block,
        request_reasoning_effort, response_to_events, tool_definitions_for_exposure,
        ProviderRequestContext, ResolvedProviderProfile,
    };
    use crate::config::{ProviderConfig, ProvidersConfig};
    use crate::{
        ApiClient, ApiRequest, AssistantEvent, PromptAssembly, ProviderRegistry,
        ProviderRuntimeClient, ProviderTransportPool, RuntimeError,
    };
    use futures::StreamExt;
    use harness_contract::tool::ToolExposureProjection;
    use model_protocol::provider_capability::{CapabilityState, ProviderCapabilityProfile};
    use provider::{
        InputContentBlock, MessageResponse, OutputContentBlock, ToolChoice, ToolDefinition, Usage,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// P-2: a preparation that was not granted says WHICH wait held it
    /// back, so the failure names its cause instead of reading like a
    /// capacity wall.
    #[test]
    fn p2_a_preparation_that_is_not_granted_names_its_wait_reason() {
        use crate::execution_core::graph::{ResourceAdmissionDecision, ResourceWaitReason};
        let deferred = ResourceAdmissionDecision::Deferred {
            request_id: uuid::Uuid::new_v4(),
            wait_reason: ResourceWaitReason::DeadlineExpired,
            policy_revision: 1,
        };
        assert_eq!(
            super::preparation_capacity_failure(
                "prepared content capacity unavailable",
                &deferred,
            ),
            "prepared content capacity unavailable: wait_reason=DeadlineExpired",
        );
        let overloaded = ResourceAdmissionDecision::Overloaded {
            request_id: uuid::Uuid::new_v4(),
            wait_reason: ResourceWaitReason::KeyPendingLimit,
            policy_revision: 1,
            pending: 3,
        };
        assert_eq!(
            super::preparation_capacity_failure(
                "prepared content capacity unavailable",
                &overloaded,
            ),
            "prepared content capacity unavailable: wait_reason=KeyPendingLimit",
        );
    }

    /// P-2: the preparation queues on the session's admission, the queue the
    /// sibling provider path already uses.
    #[test]
    fn p2_the_frozen_preparation_queues_on_the_session_fairness_key() {
        assert_eq!(
            super::preparation_fairness_key(Some("session-1")),
            "session:session-1",
        );
        assert_eq!(
            super::preparation_fairness_key(None),
            "provider-prepare",
        );
    }

    /// P-2: a cancelled turn stops waiting for the admission instead of
    /// sitting out the whole window and then failing for a capacity that no
    /// longer matters.
    #[tokio::test]
    async fn p2_a_cancelled_turn_stops_waiting_for_the_admission() {
        use crate::execution_core::graph::{
            ExecutionResourceKind, ExecutionResourceManager, ResourceAdmissionRequest,
            ResourceQuota,
        };
        let manager = ExecutionResourceManager::new([(
            ExecutionResourceKind::ContextBytes,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )]);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or_default();
        let request = |bytes: usize, deadline_at_ms: u64| {
            ResourceAdmissionRequest::new(
                crate::execution_core::graph::ExecutionServiceClass::Foreground,
                [(ExecutionResourceKind::ContextBytes, bytes)],
            )
            .with_deadline_at_ms(deadline_at_ms)
        };
        // Hold the only byte the quota has, so the next request has to wait
        // for it instead of being refused on the spot.
        let held = manager
            .admit(request(1, now.saturating_add(30_000)))
            .await
            .expect("the held admission");
        let control = crate::execution_core::services::TurnCancellationControl::operational(
            "session-1",
            "turn-1",
            "execution-1",
        );
        control.token().cancel();
        let decision = super::admit_preparation_resources(
            &manager,
            request(1, now.saturating_add(300)),
            Some(&control),
        )
        .await
        .expect("the admission decision");
        assert!(
            decision.is_none(),
            "a cancelled turn stops waiting for the admission"
        );
        drop(held);
    }

    #[test]
    fn control_completion_keeps_absent_usage_unknown_and_reported_zero_known() {
        for observed in [false, true] {
            let completion = super::control_completion(MessageResponse {
                id: "control-usage".into(),
                kind: "message".into(),
                role: "assistant".into(),
                content: vec![OutputContentBlock::Text {
                    text: "valid business result".into(),
                }],
                model: "fixture-model".into(),
                stop_reason: Some("stop".into()),
                stop_sequence: None,
                usage: Usage::default(),
                usage_observed: observed,
                cache_dimensions_observed: false,
                request_id: None,
            })
            .unwrap();
            assert_eq!(completion.text, "valid business result");
            assert_eq!(
                completion.input_tokens,
                if observed { Some(0) } else { None }
            );
            assert_eq!(
                completion.output_tokens,
                if observed { Some(0) } else { None }
            );
        }
    }

    #[test]
    fn absent_provider_usage_is_not_promoted_to_known_zero() {
        let events = response_to_events(MessageResponse {
            id: "no-usage".to_string(),
            kind: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![OutputContentBlock::Text {
                text: "completed".to_string(),
            }],
            model: "deepseek-v4-flash".to_string(),
            stop_reason: Some("end_turn".to_string()),
            stop_sequence: None,
            usage: Usage::default(),
            usage_observed: false,
            cache_dimensions_observed: false,
            request_id: None,
        });
        assert!(
            events
                .iter()
                .all(|event| !matches!(event, AssistantEvent::Usage(_)))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, AssistantEvent::MessageStop))
        );
    }

    #[test]
    fn stream_usage_merges_start_and_delta_dimensions_by_max() {
        let start = model_protocol::usage::TokenUsage {
            input_tokens: 8,
            output_tokens: 0,
            cache_creation_input_tokens: 13,
            cache_read_input_tokens: 21,
        };
        let delta = model_protocol::usage::TokenUsage {
            input_tokens: 0,
            output_tokens: 5,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        };

        assert_eq!(
            merge_provider_usage_max(Some(start), delta),
            model_protocol::usage::TokenUsage {
                input_tokens: 8,
                output_tokens: 5,
                cache_creation_input_tokens: 13,
                cache_read_input_tokens: 21,
            }
        );
    }

    #[test]
    fn cache_cohort_identity_tracks_stable_first_item_but_not_dynamic_tail() {
        let context = ProviderRequestContext {
            request_id: "request-a".to_string(),
            profile: ResolvedProviderProfile {
                registry_revision: 1,
                provider_name: "deepseek".to_string(),
                model: "deepseek-v4-flash".to_string(),
                base_url: Some("https://api.deepseek.com".to_string()),
                protocol: Some("openai".to_string()),
                parallel_tool_calls_mode:
                    model_protocol::provider_config::ParallelToolCallsMode::Auto,
                effective_parallel_tool_calls: Some(true),
                effective_early_tool_start: false,
                model_capabilities: Vec::new(),
                capabilities: ProviderCapabilityProfile::unknown(),
            },
            transport_fingerprint: crate::TransportProfileFingerprint(1),
            diagnostic_send_reason: None,
            attempt: 1,
        };
        let wire = |stable: &str, tail: &str| provider::ProviderWireRequest {
            method: "POST".to_string(),
            endpoint: "/chat/completions".to_string(),
            protocol: "openai".to_string(),
            headers: Vec::new(),
            body: json!({
                "model": "deepseek-v4-flash",
                "messages": [
                    {"role": "system", "content": stable},
                    {"role": "user", "content": tail}
                ],
                "stream": true,
                "max_tokens": 100
            }),
            body_sha256: "body".to_string(),
            tool_schema_sha256: None,
        };
        let (agent_a, agent_a_canonical, _) = provider_cache_material(
            &context,
            &wire("shared-program", "agent-a"),
            "session-a",
            None,
            &[],
        );
        let (agent_b, _, _) = provider_cache_material(
            &context,
            &wire("shared-program", "agent-b"),
            "session-a",
            None,
            &[],
        );
        let (other_program, _, _) = provider_cache_material(
            &context,
            &wire("other-program", "agent-b"),
            "session-a",
            None,
            &[],
        );
        assert_eq!(agent_a, agent_b);
        assert_ne!(agent_a, other_program);
        for _ in 0..1_000 {
            let (identity, canonical, _) = provider_cache_material(
                &context,
                &wire("shared-program", "agent-a"),
                "session-a",
                None,
                &[],
            );
            assert_eq!(identity, agent_a);
            assert_eq!(canonical, agent_a_canonical);
        }
    }

    #[test]
    fn explicit_cache_cohort_groups_execution_specific_stable_system_suffixes() {
        let context = ProviderRequestContext {
            request_id: "request-a".to_string(),
            profile: ResolvedProviderProfile {
                registry_revision: 1,
                provider_name: "deepseek".to_string(),
                model: "deepseek-v4-flash".to_string(),
                base_url: Some("https://api.deepseek.com".to_string()),
                protocol: Some("openai".to_string()),
                parallel_tool_calls_mode:
                    model_protocol::provider_config::ParallelToolCallsMode::Auto,
                effective_parallel_tool_calls: Some(true),
                effective_early_tool_start: false,
                model_capabilities: Vec::new(),
                capabilities: ProviderCapabilityProfile::unknown(),
            },
            transport_fingerprint: crate::TransportProfileFingerprint(1),
            diagnostic_send_reason: None,
            attempt: 1,
        };
        let wire = |system: &str| provider::ProviderWireRequest {
            method: "POST".to_string(),
            endpoint: "/chat/completions".to_string(),
            protocol: "openai".to_string(),
            headers: Vec::new(),
            body: json!({
                "model": "deepseek-v4-flash",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": "task"}
                ],
                "stream": true,
                "max_tokens": 100
            }),
            body_sha256: "body".to_string(),
            tool_schema_sha256: None,
        };
        let (agent_a, _, _) = provider_cache_material(
            &context,
            &wire("shared-program\nagent-a"),
            "session-a",
            Some("shared-program"),
            &["shared-package".to_string()],
        );
        let (agent_b, _, _) = provider_cache_material(
            &context,
            &wire("shared-program\nagent-b"),
            "session-a",
            Some("shared-program"),
            &["shared-package".to_string()],
        );
        assert_eq!(agent_a, agent_b);
        let (other_package, _, _) = provider_cache_material(
            &context,
            &wire("shared-program\nagent-b"),
            "session-a",
            Some("shared-program"),
            &["other-package".to_string()],
        );
        // User-prefix/package differences are represented in the canonical
        // prompt bytes, not in the coordinator identity. This keeps one
        // provider/account cohort warm while the prefix observer still
        // reports the exact reusable-byte ratio for each request.
        assert_eq!(agent_a, other_package);
    }

    #[test]
    fn cache_coordinator_keeps_role_tool_overlays_in_one_provider_cohort() {
        let context = ProviderRequestContext {
            request_id: "request-a".to_string(),
            profile: ResolvedProviderProfile {
                registry_revision: 1,
                provider_name: "deepseek".to_string(),
                model: "deepseek-v4-flash".to_string(),
                base_url: Some("https://api.deepseek.com".to_string()),
                protocol: Some("openai".to_string()),
                parallel_tool_calls_mode:
                    model_protocol::provider_config::ParallelToolCallsMode::Auto,
                effective_parallel_tool_calls: Some(true),
                effective_early_tool_start: false,
                model_capabilities: Vec::new(),
                capabilities: ProviderCapabilityProfile::unknown(),
            },
            transport_fingerprint: crate::TransportProfileFingerprint(1),
            diagnostic_send_reason: None,
            attempt: 1,
        };
        let wire = |tool: &str| provider::ProviderWireRequest {
            method: "POST".to_string(),
            endpoint: "/chat/completions".to_string(),
            protocol: "openai".to_string(),
            headers: Vec::new(),
            body: json!({
                "model": "deepseek-v4-flash",
                "messages": [
                    {"role": "system", "content": "shared-program\nrole-private-tail"},
                    {"role": "user", "content": "task"}
                ],
                "tools": [{"type": "function", "function": {"name": tool}}],
                "parallel_tool_calls": true,
                "temperature": 0.0,
                "stream": true,
                "max_tokens": 100
            }),
            body_sha256: format!("body-{tool}"),
            tool_schema_sha256: Some(format!("schema-{tool}")),
        };
        let (reader, _, _) = provider_cache_material(
            &context,
            &wire("read_file"),
            "session-a",
            Some("shared-program"),
            &[],
        );
        let (researcher, _, _) = provider_cache_material(
            &context,
            &wire("web_search"),
            "session-a",
            Some("shared-program"),
            &[],
        );
        assert_eq!(reader, researcher);
    }

    /// The precondition for moving turn-stable content into the cohort prefix.
    ///
    /// `provider_input_messages` builds `immutable_prefix` from the cohort prefix
    /// plus the private user brief, and this function emits
    /// `immutable_prefix ++ history ++ context`. If that ordering ever changed,
    /// moving stable content into the cohort prefix would silently stop working:
    /// nothing would fail, the bytes would simply stay behind the growing history
    /// and become permanently unreusable. The existing test pins context AFTER
    /// history; this pins the immutable material BEFORE it.
    #[test]
    fn immutable_cohort_material_is_emitted_before_history() {
        let history = vec![provider::InputMessage::user_text("history-entry")];
        let context = vec![provider::InputMessage::user_text("turn-local-tail")];
        let immutable = vec![provider::InputMessage::user_text("COHORT-MARKER")];

        let messages =
            compile_provider_input_messages(immutable.clone(), history.clone(), context.clone());

        assert_eq!(messages.len(), 3, "immutable + history + context");
        assert!(
            matches!(
                messages[0].content.as_slice(),
                [InputContentBlock::Text { text }] if text == "COHORT-MARKER"
            ),
            "the cohort prefix must be the very first message"
        );
        assert!(
            matches!(
                messages[1].content.as_slice(),
                [InputContentBlock::Text { text }] if text == "history-entry"
            ),
            "history must follow the immutable prefix"
        );
        assert!(
            matches!(
                messages[2].content.as_slice(),
                [InputContentBlock::Text { text }] if text == "turn-local-tail"
            ),
            "request-local context must stay last"
        );

        // GroW history must not splice anything in front of the immutable prefix.
        let mut grown = history;
        grown.push(provider::InputMessage {
            role: "assistant".to_string(),
            content: vec![InputContentBlock::Text {
                text: "provider-output".to_string(),
            }],
        });
        let regrown = compile_provider_input_messages(immutable, grown, context);
        assert!(
            matches!(
                regrown[0].content.as_slice(),
                [InputContentBlock::Text { text }] if text == "COHORT-MARKER"
            ),
            "a longer history must not push the cohort prefix back"
        );
    }

    #[test]
    fn request_local_context_is_appended_after_exact_history() {
        let prompt = PromptAssembly::new(vec![
            "stable-system".to_string(),
            crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string(),
            "runtime-clock-a".to_string(),
        ]);
        let history = vec![provider::InputMessage::user_text("history-entry")];
        let context = prompt
            .contextual_messages()
            .into_iter()
            .map(provider::InputMessage::user_text)
            .collect::<Vec<_>>();
        let messages =
            compile_provider_input_messages(Vec::new(), history.clone(), context.clone());
        assert_eq!(messages.len(), 2);
        assert!(matches!(
            messages[0].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "history-entry"
        ));
        assert!(matches!(
            messages[1].content.as_slice(),
            [InputContentBlock::Text { text }] if text.contains("runtime-clock-a")
        ));
        assert_eq!(prompt.wire_system_text().as_deref(), Some("stable-system"));

        let retry = compile_provider_input_messages(Vec::new(), history.clone(), context.clone());
        assert_eq!(
            retry, messages,
            "an identical retry must not duplicate context"
        );

        let mut extended_history = history;
        extended_history.push(provider::InputMessage {
            role: "assistant".to_string(),
            content: vec![InputContentBlock::Text {
                text: "provider-output".to_string(),
            }],
        });
        let continuation = compile_provider_input_messages(
            Vec::new(),
            extended_history,
            vec![provider::InputMessage::user_text("runtime-clock-b")],
        );
        assert_eq!(continuation.len(), 3);
        assert!(matches!(
            continuation[0].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "history-entry"
        ));
        assert!(matches!(
            continuation[1].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "provider-output"
        ));
        assert!(matches!(
            continuation[2].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "runtime-clock-b"
        ));
        assert!(continuation.iter().all(|message| !matches!(
            message.content.as_slice(),
            [InputContentBlock::Text { text }] if text.contains("runtime-clock-a")
        )));
    }

    #[test]
    fn immutable_user_prefix_precedes_private_history_and_resets_on_change() {
        let prefix = vec![
            provider::InputMessage::user_text("shared-team-context"),
            provider::InputMessage::user_text("private-role-brief"),
        ];
        let history = vec![provider::InputMessage::user_text("role-a objective")];
        let first = compile_provider_input_messages(prefix.clone(), history.clone(), Vec::new());
        assert_eq!(first.len(), 3);
        assert!(matches!(
            first[0].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "shared-team-context"
        ));
        assert!(matches!(
            first[1].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "private-role-brief"
        ));
        assert!(matches!(
            first[2].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "role-a objective"
        ));

        let retry = compile_provider_input_messages(prefix.clone(), history.clone(), Vec::new());
        assert_eq!(retry, first, "exact retry keeps the frozen prefix");

        let changed = compile_provider_input_messages(
            vec![provider::InputMessage::user_text("other-team-context")],
            history,
            Vec::new(),
        );
        assert_eq!(
            changed.len(),
            2,
            "changed package resets prior wire history"
        );
        assert!(matches!(
            changed[0].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "other-team-context"
        ));
    }

    #[test]
    fn role_private_prefix_precedes_history_after_shared_cohort() {
        let first = compile_provider_input_messages(
            vec![
                provider::InputMessage::user_text("shared-cohort"),
                provider::InputMessage::user_text("private-role"),
            ],
            vec![provider::InputMessage::user_text("turn-1")],
            Vec::new(),
        );
        assert_eq!(first.len(), 3);
        assert!(matches!(
            first[0].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "shared-cohort"
        ));
        assert!(matches!(
            first[1].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "private-role"
        ));
        assert!(matches!(
            first[2].content.as_slice(),
            [InputContentBlock::Text { text }] if text == "turn-1"
        ));
    }

    #[test]
    fn incomplete_provider_stream_is_never_promoted_to_terminal_success() {
        let error = super::ProviderStreamError {
            error: provider::ApiError::InvalidSseFrame(
                "provider stream ended before terminal message_stop",
            ),
            physical_failure: None,
            protocol_diagnostic: None,
        };

        assert!(
            error
                .to_string()
                .contains("provider stream ended before terminal message_stop")
        );
    }

    #[tokio::test]
    async fn r302_decoder_diagnostic_requires_the_exact_acknowledged_step_and_attempt() {
        let root = tempfile::tempdir().unwrap();
        let (_, request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), root.path());
        let context = request.provider_evidence_context.as_ref().unwrap();
        let step = request.bound_step.as_ref().unwrap().snapshot();
        let identity = super::ProviderPhysicalIdentity {
            graph_owner: None,
            session_id: context.session_id.clone(),
            execution_id: context.execution_id.clone(),
            turn_id: context.turn_id.clone(),
            request_sequence: context.request_sequence,
            request_id: "original-request".into(),
            response_id: "original-response".into(),
            opened_at_ms: 1,
            logical_attempt: context.attempt,
            step: step.identity.clone(),
            provider_attempt: step.provider_attempt.clone(),
            resource_lease: "original-lease".into(),
        };
        let error = provider::ApiError::CompatibilityToolProtocol(
            provider::CompatibilityToolProtocolFailure::MalformedFrame,
        )
        .with_compatibility_tool_frame_diagnostic(
            "stream_compat_finish",
            "unterminated",
            b"PRIVATE_BOUND_FRAME_302",
        );
        let bind = |ack: &super::AcknowledgedProviderPhysicalObservation| {
            super::bind_provider_protocol_diagnostic(
                &error,
                context,
                &step.identity,
                &step.provider_attempt,
                "original-request",
                "original-lease",
                ack,
            )
        };
        let opened = super::ProviderPhysicalObservation::ResponseOpened {
            identity: identity.clone(),
        };
        let ack =
            super::AcknowledgedProviderPhysicalObservation::from_written(&opened, 17).unwrap();
        let bound = bind(&ack).expect("original acknowledged response must bind");
        assert_eq!(bound.identity, identity);
        assert_eq!(bound.opened_sequence, 17);
        assert_eq!(
            bound.metadata()["persistence_authority"],
            "original_outcome_receipt_only"
        );
        assert!(!format!("{bound:?}").contains("PRIVATE_BOUND_FRAME_302"));
        assert!(
            !bound
                .metadata()
                .to_string()
                .contains("PRIVATE_BOUND_FRAME_302")
        );
        let outcome = super::ProviderAttemptOutcomeEvidence {
            request_id: "original-request".into(),
            logical_attempt: context.attempt,
            terminal_status: "failed".into(),
            usage: None,
            cache_dimensions_observed: false,
            protocol_diagnostic: Some(bound.clone()),
        };
        let packed = harness_contract::step::StepRenderRef {
            step_id: step.identity.step_id.clone(),
            attempt_id: step.provider_attempt.attempt_id.clone(),
            catalog_digest: "fixture-catalog".into(),
            render_digest: "fixture-render".into(),
        };
        let safe = bound.outcome_metadata(context, &outcome, &packed).unwrap();
        assert_eq!(safe["raw_omitted"], true);
        assert_eq!(safe["raw_persisted"], false);
        assert_eq!(safe["raw_replayable"], false);
        assert!(!safe.to_string().contains("PRIVATE_BOUND_FRAME_302"));
        assert!(
            safe.get("closure_predicate").is_none(),
            "old diagnostics remain unannotated"
        );
        // The parser's finite labels must survive both the bound diagnostic
        // view and the exact payload passed to the original outcome writer.
        // This adds no new physical binding or response/storage authority.
        for predicate in [
            "dsml_tool_calls_close",
            "dsml_invoke_tag_end",
            "dsml_invoke_close",
            "dsml_parameter_tag_end",
            "dsml_parameter_close",
            "legacy_tool_call_close",
            "legacy_function_tag_end",
            "legacy_function_close",
            "legacy_parameter_tag_end",
            "legacy_parameter_close",
            "legacy_tool_name_close",
        ] {
            let error = provider::ApiError::CompatibilityToolProtocol(
                provider::CompatibilityToolProtocolFailure::MalformedFrame,
            )
            .with_compatibility_tool_frame_diagnostic(
                "stream_compat_finish",
                "unterminated",
                b"PRIVATE_BOUND_FRAME_302",
            )
            .with_compatibility_tool_closure_predicate(Some(predicate));
            let branch = super::bind_provider_protocol_diagnostic(
                &error,
                context,
                &step.identity,
                &step.provider_attempt,
                "original-request",
                "original-lease",
                &ack,
            )
            .unwrap();
            assert_eq!(branch.metadata()["decoder"]["closure_predicate"], predicate);
            let mut branch_payload = branch.outcome_metadata(context, &outcome, &packed).unwrap();
            assert_eq!(
                branch_payload
                    .as_object_mut()
                    .unwrap()
                    .remove("closure_predicate"),
                Some(serde_json::json!(predicate))
            );
            assert_eq!(
                branch_payload, safe,
                "all original identity/raw-omission fields must remain unchanged"
            );
            assert!(!format!("{branch:?}").contains("PRIVATE_BOUND_FRAME_302"));
        }
        let mut tampered = bound.clone();
        tampered.opened_digest.push('x');
        assert!(
            tampered
                .outcome_metadata(context, &outcome, &packed)
                .is_err()
        );
        let mut other_step = packed.clone();
        other_step.step_id.push_str("-foreign");
        assert!(
            bound
                .outcome_metadata(context, &outcome, &other_step)
                .is_err()
        );
        let large = provider::ApiError::CompatibilityToolProtocol(
            provider::CompatibilityToolProtocolFailure::FrameTooLarge,
        )
        .with_compatibility_tool_frame_diagnostic(
            "stream_compat_accumulate",
            "frame_too_large",
            &vec![b'x'; 64 * 1024 + 1],
        );
        let large_bound = super::bind_provider_protocol_diagnostic(
            &large,
            context,
            &step.identity,
            &step.provider_attempt,
            "original-request",
            "original-lease",
            &ack,
        )
        .unwrap();
        let omitted = large_bound
            .outcome_metadata(context, &outcome, &packed)
            .unwrap();
        assert_eq!(omitted["capture_omitted"], true);
        assert_eq!(omitted["raw_omitted"], true);
        assert_eq!(omitted["raw_persisted"], false);
        assert_eq!(omitted["raw_replayable"], false);
        for mutation in [
            "step",
            "attempt",
            "session",
            "turn",
            "execution",
            "request",
            "response",
            "lease",
            "digest",
            "actor",
            "run",
            "step_attempt",
            "epoch",
            "program",
            "task",
            "request_digest",
            "profile_digest",
        ] {
            let mut changed = identity.clone();
            match mutation {
                "step" => {
                    changed.step.step_id.push_str("-foreign");
                    changed.provider_attempt.origin_step_id = changed.step.step_id.clone();
                }
                "attempt" => changed.provider_attempt.attempt_id.push_str("-foreign"),
                "actor" => changed.step.actor_ref = Some("foreign-actor".into()),
                "run" => changed.step.run_ref = Some("foreign-run".into()),
                "step_attempt" => changed.step.attempt = Some(context.attempt.saturating_add(1)),
                "epoch" => changed.step.epoch = Some("foreign-epoch".into()),
                "program" => changed.step.program_ref = Some("foreign-program".into()),
                "task" => changed.step.task_ref = Some("foreign-task".into()),
                "request_digest" => changed.provider_attempt.request_digest.push_str("-foreign"),
                "profile_digest" => changed
                    .provider_attempt
                    .model_profile_digest
                    .push_str("-foreign"),
                "session" => changed.session_id.push_str("-foreign"),
                "turn" => changed.turn_id.push_str("-foreign"),
                "execution" => changed.execution_id.push_str("-foreign"),
                "request" => changed.request_id.push_str("-foreign"),
                "response" => changed.response_id.push_str("-foreign"),
                "lease" => changed.resource_lease.push_str("-foreign"),
                "digest" => {}
                _ => unreachable!(),
            }
            // Preserve the sequence deliberately: sequence alone is not identity.
            assert_eq!(changed.request_sequence, identity.request_sequence);
            let observation =
                super::ProviderPhysicalObservation::ResponseOpened { identity: changed };
            if mutation == "step" || mutation == "attempt" {
                // A well-formed foreign binding must still fail exact identity,
                // rather than merely failing the structural validation above.
                observation.validate(context).unwrap();
            }
            let mut foreign =
                super::AcknowledgedProviderPhysicalObservation::from_written(&observation, 17)
                    .unwrap();
            if mutation == "digest" || mutation == "response" {
                // Tampering an existing ack cannot be certified by a new digest.
                foreign.receipt.observation_digest = ack.receipt.observation_digest.clone();
                if mutation == "digest" {
                    foreign.receipt.observation_digest.push('x');
                }
            }
            assert!(bind(&foreign).is_none(), "must refuse {mutation}");
        }
        assert!(
            super::ProviderStreamError::unbound(error)
                .protocol_diagnostic
                .is_none()
        );
    }

    #[tokio::test]
    async fn r189_physical_wire_rejects_wrong_attempt_old_open_and_missing_requested() {
        let root = tempfile::tempdir().unwrap();
        let (_, request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), root.path());
        let context = request.provider_evidence_context.as_ref().unwrap();
        let step = request.bound_step.as_ref().unwrap().snapshot();
        let identity = super::ProviderPhysicalIdentity {
            graph_owner: None,
            session_id: context.session_id.clone(),
            execution_id: context.execution_id.clone(),
            turn_id: context.turn_id.clone(),
            request_sequence: context.request_sequence,
            request_id: "original-request".into(),
            response_id: "actual-response-A".into(),
            opened_at_ms: 1,
            logical_attempt: context.attempt,
            step: step.identity.clone(),
            provider_attempt: step.provider_attempt.clone(),
            resource_lease: "resource-lease:fixture:original".into(),
        };
        let opened = super::ProviderPhysicalObservation::ResponseOpened {
            identity: identity.clone(),
        };
        opened.validate(context).unwrap();
        let receipt = super::AcknowledgedProviderPhysicalObservation::from_written(&opened, 0)
            .unwrap()
            .receipt;
        let baseline = super::ProviderPhysicalObservation::CancellationObserved {
            identity,
            observed_at_ms: 2,
            opened: receipt,
            requested: super::RequestedCancellationReference {
                cancellation_id: "original-cancel".into(),
                stream_id: "cancellation:original-cancel".into(),
                event_sequence: 1,
                commit_cursor: 9,
                payload_digest: format!("sha256:{}", "a".repeat(64)),
            },
        };
        baseline.validate(context).unwrap();
        assert_eq!(
            serde_json::from_value::<super::ProviderPhysicalObservation>(
                serde_json::to_value(&baseline).unwrap()
            )
            .unwrap(),
            baseline
        );
        for mutation in [
            "attempt",
            "old_open",
            "step",
            "missing_requested",
            "cursor",
            "digest",
            "opened_clock",
            "observed_clock",
        ] {
            let mut candidate = baseline.clone();
            let super::ProviderPhysicalObservation::CancellationObserved {
                identity,
                opened,
                requested,
                observed_at_ms,
            } = &mut candidate
            else {
                unreachable!()
            };
            match mutation {
                "attempt" => identity.logical_attempt += 1,
                "old_open" => opened.identity.response_id = "previous-response".into(),
                "step" => identity.provider_attempt.origin_step_id = "foreign-step".into(),
                "missing_requested" => requested.cancellation_id.clear(),
                "cursor" => requested.commit_cursor = 0,
                "digest" => requested.payload_digest = "not-an-original-event-digest".into(),
                "opened_clock" => identity.opened_at_ms = 0,
                "observed_clock" => *observed_at_ms = 0,
                _ => unreachable!(),
            }
            assert!(candidate.validate(context).is_err(), "{mutation}");
        }
        let mut invented = serde_json::to_value(baseline).unwrap();
        invented["phase"] = json!("provider_closed");
        assert!(serde_json::from_value::<super::ProviderPhysicalObservation>(invented).is_err());
    }

    #[tokio::test]
    async fn r194_original_writer_replays_frozen_observation_and_rejects_stale_claim() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        // This is the writer/CAS boundary with a synthetic response identity,
        // not evidence that a real SDK response was opened or cancelled.
        let root = tempfile::tempdir().unwrap();
        let (services, turn, receipt, ingress, store) =
            crate::execution_core::services::requested_cancellation_source_tests::live_requested_fixture_with_store(root.path()).await;
        let source = crate::execution_core::services::requested_cancellation_source_tests::authorized_fixture_source(
            &services, &turn, &receipt,
        ).await;
        services
            .publish_requested_cancellation(&turn, source)
            .unwrap();
        let source = turn.requested_source().unwrap();
        let (_, request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), root.path());
        let mut context = request.provider_evidence_context.as_ref().unwrap().clone();
        context.session_id = ingress.session_id.clone();
        context.turn_id = ingress.turn_id.clone();
        context.execution_id = receipt.execution_id.clone();
        let step = request.bound_step.as_ref().unwrap().snapshot();
        let mut step_identity = step.identity.clone();
        step_identity.session_id = context.session_id.clone();
        step_identity.turn_id = context.turn_id.clone();
        step_identity.execution_id = Some(context.execution_id.clone());
        let identity = super::ProviderPhysicalIdentity {
            graph_owner: None,
            session_id: context.session_id.clone(),
            execution_id: context.execution_id.clone(),
            turn_id: context.turn_id.clone(),
            request_sequence: context.request_sequence,
            request_id: "writer-boundary-request".into(),
            response_id: "synthetic-writer-boundary".into(),
            opened_at_ms: 1,
            logical_attempt: context.attempt,
            step: step_identity,
            provider_attempt: step.provider_attempt.clone(),
            resource_lease: "fixture-not-physical-witness".into(),
        };
        let writer = crate::conversation::r189_session_provider_writer_for_test(&services);
        let opened = writer
            .persist_physical(
                &context,
                super::ProviderPhysicalObservation::ResponseOpened {
                    identity: identity.clone(),
                },
                None,
            )
            .await
            .unwrap();
        let (stream, sequence, cursor, digest) = source.event_reference();
        let observation = super::ProviderPhysicalObservation::CancellationObserved {
            identity,
            observed_at_ms: 2,
            opened: opened.receipt,
            requested: super::RequestedCancellationReference {
                cancellation_id: receipt.cancellation_id.clone(),
                stream_id: stream.into(),
                event_sequence: sequence,
                commit_cursor: cursor,
                payload_digest: digest.into(),
            },
        };
        assert!(writer
            .persist_physical(&context, observation.clone(), None)
            .await
            .is_err());
        let first = writer
            .persist_physical(&context, observation.clone(), Some(&source))
            .await
            .unwrap();
        let second = writer
            .persist_physical(&context, observation.clone(), Some(&source))
            .await
            .unwrap();
        assert_eq!(first.receipt, second.receipt);
        let page = store
            .session_domain_events_page(&ingress.session_id, 0, 100)
            .await
            .unwrap();
        let physical: Vec<_> = page
            .events
            .iter()
            .filter(|event| event.kind == "context.provider_physical_observation")
            .collect();
        assert_eq!(
            physical.len(),
            2,
            "one opened fixture and one idempotent cancellation fixture"
        );
        let cancelled = physical
            .iter()
            .find(|event| event.status.as_deref() == Some("cancel_observed"))
            .unwrap();
        assert_eq!(
            cancelled.created_at_ms, 2,
            "producer timestamp, not writer wall clock"
        );
        assert_eq!(
            cancelled.payload,
            serde_json::to_value(&observation).unwrap()
        );
        let current = store
            .get_session_runtime_outbox(&ingress.request_id)
            .await
            .unwrap()
            .unwrap();
        store
            .requeue_claimed_session_runtime_outbox(
                &ingress.request_id,
                &ingress.claim_owner,
                ingress.session_generation,
                &ingress.claim_token,
                current.revision,
                harness_contract::turn::InputRoutingDecision::StartNewTurn,
                None,
                None,
                "revoke writer-boundary claim",
                u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap(),
            )
            .await
            .unwrap();
        assert!(writer
            .persist_physical(&context, observation, Some(&source))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn r189_actual_sdk_cancel_uses_original_requested_and_session_writer() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        use crate::execution_core::graph::*;
        struct WriterBarrier {
            original: Arc<dyn super::ProviderWireEvidenceWriter>,
            fail_open: bool,
            pause_outcome: bool,
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }
        #[async_trait::async_trait]
        impl super::ProviderWireEvidenceWriter for WriterBarrier {
            async fn persist(
                &self,
                context: &super::ProviderRequestEvidenceContext,
                evidence: super::ProviderWireEvidence,
            ) -> Result<(), RuntimeError> {
                self.original.persist(context, evidence).await
            }
            async fn persist_outcome(
                &self,
                context: &super::ProviderRequestEvidenceContext,
                outcome: super::ProviderAttemptOutcomeEvidence,
            ) -> Result<(), RuntimeError> {
                if self.pause_outcome {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                self.original.persist_outcome(context, outcome).await
            }
            async fn persist_physical(
                &self,
                context: &super::ProviderRequestEvidenceContext,
                observation: super::ProviderPhysicalObservation,
                source: Option<&crate::execution_core::services::AuthorizedRequestedCancellation>,
            ) -> Result<super::AcknowledgedProviderPhysicalObservation, RuntimeError> {
                if self.fail_open
                    && matches!(
                        observation,
                        super::ProviderPhysicalObservation::ResponseOpened { .. }
                    )
                {
                    return Err(RuntimeError::new(
                        "injected physical open persistence failure",
                    ));
                }
                self.original
                    .persist_physical(context, observation, source)
                    .await
            }
        }
        for condition in [
            "user_cancel",
            "operational_first",
            "eof_first",
            "open_failure",
        ] {
            let operational_first = condition == "operational_first";
            let eof_first = condition == "eof_first";
            let open_failure = condition == "open_failure";
            let root = tempfile::tempdir().unwrap();
            let (services, turn, original_receipt, ingress, store) = crate::execution_core::services::requested_cancellation_source_tests::live_requested_fixture_with_store(root.path()).await;
            // For the user-cancel branch, bind the real Session/SDK producer
            // to an actual original Graph attempt, then reconstruct its effect
            // through a fresh commit service and Session reader below.
            let cold_graph = if condition == "user_cancel" {
                use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec};
                let runner = ExecutionGraphRunner::new(
                    services.executor_registry().clone(), services.graph_state_store().clone(),
                    services.commit_service().clone(), Arc::new(ExecutionResourceManager::new([])),
                    Arc::new(ScopeLockManager::persistent(root.path().join("k05-cold-scopes")).unwrap()),
                    Arc::new(WorktreeLeaseManager::open(root.path().join("k05-cold-leases.json")).unwrap()),
                    "k05-cold-sdk", root.path());
                let mut graph = ExecutionGraph::new("original SDK cancellation cold replay");
                graph.id = original_receipt.execution_id.clone();
                graph.lineage = Some(ExecutionGraphLineage { session_id:ingress.session_id.clone(),
                    turn_id:ingress.turn_id.clone(), root_task_id:ingress.root_task_id.clone(),
                    task_id:ingress.primary_task_id.clone(), generation:ingress.session_generation });
                let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "cold-sdk-model");
                let node_id = node.id.clone(); graph.nodes.push(node);
                runner.register(graph).await.unwrap();
                let snapshot = runner.pump_snapshot(&original_receipt.execution_id, &Default::default()).await.unwrap();
                let prepared = runner.prepare_ready_wave(&original_receipt.execution_id, snapshot.ready).await.unwrap();
                assert_eq!(prepared.len(), 1);
                let current = services.graph_state_store().load(&original_receipt.execution_id).unwrap();
                let binding = services.graph_state_store().current_binding(&current, &node_id, 1).unwrap();
                let (ticket, _) = services.graph_state_store().binding_start_ticket(&current, &node_id, &binding).unwrap();
                let owner = services.commit_service().k05_test_original_provider_owner(&ticket).unwrap();
                Some((runner, prepared, ticket, owner))
            } else { None };
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buffer = [0; 4096];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0 && bytes.len() < 1024 * 1024);
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(offset) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..offset]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if bytes.len() >= offset + 4 + length {
                            break;
                        }
                    }
                }
                let mut data = "data: {\"id\":\"source-held\",\"object\":\"chat.completion.chunk\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"physical-body\"},\"finish_reason\":null}]}\n\n".to_string();
                if eof_first {
                    data.push_str("data: {\"id\":\"source-held\",\"object\":\"chat.completion.chunk\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n");
                }
                let length = if eof_first { data.len() } else { 999999 };
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{data}").as_bytes()).await.unwrap();
                let mut byte = [0; 1];
                let ended =
                    tokio::time::timeout(std::time::Duration::from_secs(5), socket.read(&mut byte))
                        .await
                        .unwrap();
                assert!(matches!(ended, Ok(0)) || ended.is_err());
            });
            let (mut client, mut request) =
                step_wire_fixture(format!("http://{address}/v1"), root.path());
            request.budget.set_context_window_source("configured");
            let context = request.provider_evidence_context.as_mut().unwrap();
            context.session_id = ingress.session_id.clone();
            context.turn_id = ingress.turn_id.clone();
            context.execution_id = original_receipt.execution_id.clone();
            if let Some((_, _, _, owner)) = &cold_graph { context.graph_owner = Some(Box::new(owner.clone())); }
            let original_step = request.bound_step.take().unwrap();
            let mut identity = original_step.snapshot().identity.clone();
            identity.session_id = ingress.session_id.clone();
            identity.turn_id = ingress.turn_id.clone();
            identity.execution_id = Some(original_receipt.execution_id.clone());
            request.bound_step = Some(Arc::new(
                crate::step_snapshot::BoundStep::bind(
                    identity,
                    original_step.lease().clone(),
                    &client.step_tool_definitions().unwrap(),
                    &json!({}),
                    &crate::step_snapshot::request_material(&request),
                    original_step.snapshot().related_revisions.clone(),
                    i64::MAX,
                )
                .unwrap(),
            ));
            let _skill = bind_skill_fixture(&client, &mut request);
            let expected_step = request.bound_step.as_ref().unwrap().snapshot().clone();
            let (manager, config) = control_resource_fixture(&client);
            client = client.with_preparation_resources(manager.clone());
            client
                .configure_turn_cancellation(Some(turn.clone()))
                .unwrap();
            let outcome_entered = Arc::new(tokio::sync::Notify::new());
            let outcome_release = Arc::new(tokio::sync::Notify::new());
            client.configure_provider_wire_evidence(Some(Arc::new(WriterBarrier {
                original: crate::conversation::r189_session_provider_writer_for_test(&services),
                fail_open: open_failure,
                pause_outcome: eof_first,
                entered: outcome_entered.clone(),
                release: outcome_release.clone(),
            })));
            request.preparation.retain_cancellation(turn.token());
            client.prepare_request_budget(&mut request).await.unwrap();
            let prepared = request.preparation.prepared.as_ref().unwrap();
            let total =
                (prepared.wire.body_len() as u64 + 3) / 4 + u64::from(prepared.message.max_tokens);
            let ResourceAdmissionDecision::Granted { lease, .. } = manager
                .admit(ResourceAdmissionRequest::new(
                    ExecutionServiceClass::Foreground,
                    config.admission_demands("fixture", "fixture-model", total),
                ))
                .await
                .unwrap()
            else {
                panic!("original Provider admission");
            };
            let expected_lease = lease.projection_reference();
            request
                .preparation
                .retain_dispatch_lease(Some(Arc::new(lease)));
            let super::ApiClientStream {
                mut events,
                execution,
                ..
            } = client.stream_with_transport_activity(request);
            let delivered = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = delivered.clone();
            let (consumer, physical) = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                execution.unwrap().drive(async {
                    while let Some(event) = events.next().await {
                        if let AssistantEvent::TextDelta(body) = event.unwrap() {
                            assert_eq!(body, "physical-body");
                            assert!(!observed.swap(true, Ordering::SeqCst));
                            if eof_first {
                                outcome_entered.notified().await;
                            }
                            if operational_first {
                                turn.signal_operational();
                            }
                            let mut receipt = original_receipt.clone();
                            receipt.cancellation_id = "after-real-open".into();
                            receipt.requested_at_ms =
                                u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap();
                            receipt.journal_sequence = 0;
                            receipt.projection_revision = 0;
                            let receipt = services.commit_cancellation_receipt(receipt).unwrap();
                            let source = crate::execution_core::services::requested_cancellation_source_tests::authorized_fixture_source(
                                &services, &turn, &receipt,
                            ).await;
                            assert_eq!(
                                services
                                    .publish_requested_cancellation(&turn, source)
                                    .unwrap(),
                                if operational_first {
                                    crate::execution_core::services::CancellationNotificationDisposition::OperationalFirst
                                } else {
                                    crate::execution_core::services::CancellationNotificationDisposition::Delivered
                                }
                            );
                            if eof_first {
                                outcome_release.notify_one();
                            }
                        }
                    }
                }),
            )
            .await
            .unwrap();
            assert!(consumer.is_ok());
            let mut original_cancel_reference = None;
            if open_failure {
                assert!(physical
                    .unwrap_err()
                    .to_string()
                    .contains("injected physical open persistence failure"));
            } else {
                let actual = physical.unwrap();
                if eof_first {
                    assert!(matches!(actual, crate::conversation::ProviderLocalExit::Completed));
                } else if operational_first {
                    assert!(matches!(actual, crate::conversation::ProviderLocalExit::Stopped));
                } else {
                    let crate::conversation::ProviderLocalExit::UserCancelled(proof) = actual else { panic!("original user cancellation proof missing") };
                    original_cancel_reference = Some(proof.durable_reference());
                }
            }
            if let Some((_runner, _prepared, ticket, _owner)) = cold_graph {
                use harness_contract::execution_graph::{ExecutionGraphCommand, ExecutionNodeResult, ExecutionNodeStatus};
                let current = services.graph_state_store().load(&ticket.graph_id).unwrap();
                let result = ExecutionNodeResult { status:ExecutionNodeStatus::Cancelled,
                    result_ref:original_cancel_reference, summary:Some("original local provider response observed authorized cancellation".into()),
                    wait_class:None, evidence_refs:Vec::new(), failure:None, usage:Default::default(), finished_at_ms:1 };
                let command = ExecutionGraphCommand::Cancel { expected_revision:current.revision, reason:"original requested cancellation".into() };
                services.commit_service().apply_command_with_cancelled_outcomes(&current, &command,
                    vec![ExecutionTerminalCommit { node_id:ticket.node_id.clone(), outcome:NodeExecutionOutcome::new(result.clone()), effect_ticket:Some(ticket.clone()) }]).unwrap();
                let committed_graph = services.graph_state_store().load(&ticket.graph_id).unwrap();
                assert_eq!(services.authenticated_provider_graph_cancellation(&committed_graph).unwrap().unwrap().0, "after-real-open");
                let mut failed_graph = committed_graph.clone();
                failed_graph.node_statuses.insert(ticket.node_id.clone(), ExecutionNodeStatus::Failed);
                assert!(services.authenticated_provider_graph_cancellation(&failed_graph).unwrap().is_none());
                failed_graph.node_statuses.insert(ticket.node_id.clone(), ExecutionNodeStatus::Blocked);
                assert!(services.authenticated_provider_graph_cancellation(&failed_graph).unwrap().is_none());
                let slot = Arc::new(std::sync::OnceLock::new());
                let query: Arc<dyn crate::SessionRuntimeQueryPort> = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
                assert!(slot.set(query).is_ok());
                let cold = ExecutionCommitService::new(Arc::clone(services.event_store())).with_session_query_slot(slot);
                assert!(matches!(cold.inspect_execution_effect(&ticket).unwrap(), ExecutionEffectState::Completed(_)));
                let mut forged = result.clone(); forged.result_ref = Some(format!("session:{}:physical:1:sha256:{}", ingress.session_id, "0".repeat(64)));
                assert!(cold.validate_cold_provider_cancellation(&ticket, &forged).is_err());
                let mut foreign = ticket.clone(); foreign.attempt += 1;
                assert!(!matches!(cold.inspect_execution_effect(&foreign), Ok(ExecutionEffectState::Completed(_))));
            }
            assert_eq!(
                delivered.load(Ordering::SeqCst),
                !open_failure,
                "{condition}"
            );
            server.await.unwrap();
            assert_eq!(
                manager
                    .snapshot(&ExecutionResourceKind::Provider)
                    .unwrap()
                    .active_leases,
                0
            );
            let page = store
                .session_domain_events_page(&ingress.session_id, 0, 100)
                .await
                .unwrap();
            let physical_events: Vec<_> = page
                .events
                .iter()
                .filter(|event| event.kind == "context.provider_physical_observation")
                .map(|event| {
                    (
                        event.sequence,
                        serde_json::from_value::<super::ProviderPhysicalObservation>(
                            event.payload.clone(),
                        )
                        .unwrap(),
                    )
                })
                .collect();
            assert_eq!(
                physical_events.len(),
                if open_failure {
                    0
                } else if operational_first || eof_first {
                    1
                } else {
                    2
                },
                "{condition}"
            );
            if open_failure {
                continue;
            }
            let super::ProviderPhysicalObservation::ResponseOpened { identity: opened } =
                &physical_events[0].1
            else {
                panic!("actual opened source");
            };
            assert_eq!(opened.step, expected_step.identity);
            assert_eq!(opened.provider_attempt, expected_step.provider_attempt);
            assert_eq!(opened.resource_lease, expected_lease);
            if !operational_first && !eof_first {
                let super::ProviderPhysicalObservation::CancellationObserved {
                    identity,
                    opened: reference,
                    requested,
                    ..
                } = &physical_events[1].1
                else {
                    panic!("actual cancellation source");
                };
                assert_eq!(identity, opened);
                assert_eq!(reference.session_sequence, physical_events[0].0);
                assert_eq!(reference.identity, *opened);
                assert_eq!(requested.cancellation_id, "after-real-open");
                let original = services
                    .cancellation_receipt("after-real-open")
                    .unwrap()
                    .unwrap();
                assert_eq!(requested.commit_cursor, original.journal_sequence);
                assert_eq!(requested.event_sequence, original.projection_revision);
            }
        }
    }

    #[tokio::test]
    async fn r188_stopped_consumer_does_not_start_prepared_sdk_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let (mut client, mut request) = step_wire_fixture(
            format!("http://{}/v1", listener.local_addr().unwrap()),
            workspace.path(),
        );
        request.budget.set_context_window_source("configured");
        let _skill = bind_skill_fixture(&client, &mut request);
        let recorder = Arc::new(StepWireRecorder::default());
        client.configure_provider_wire_evidence(Some(recorder.clone()));
        let super::ApiClientStream {
            events, execution, ..
        } = client.stream_with_transport_activity(request);
        let (consumer, producer) = execution
            .unwrap()
            .drive(async move {
                drop(events);
            })
            .await;
        assert!(consumer.is_ok());
        assert!(producer.is_err());
        assert_eq!(recorder.records.lock().unwrap().len(), 0);
        assert_eq!(client.transport_pool.stats().in_flight, 0);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn r188_sdk_consumer_panic_closes_the_original_held_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut buffer = [0; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0 && bytes.len() < 1024 * 1024);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(offset) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..offset]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= offset + 4 + length {
                        break;
                    }
                }
            }
            let data = "data: {\"id\":\"held\",\"object\":\"chat.completion.chunk\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"held\"},\"finish_reason\":null}]}\n\n";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 999999\r\nConnection: close\r\n\r\n{data}").as_bytes()).await.unwrap();
            let mut byte = [0; 1];
            let closed =
                tokio::time::timeout(std::time::Duration::from_secs(3), socket.read(&mut byte))
                    .await
                    .unwrap();
            assert!(
                matches!(closed, Ok(0)) || closed.is_err(),
                "original socket must close without a producer task"
            );
        });
        let workspace = tempfile::tempdir().unwrap();
        let (mut client, mut request) =
            step_wire_fixture(format!("http://{address}/v1"), workspace.path());
        request.budget.set_context_window_source("configured");
        let _skill = bind_skill_fixture(&client, &mut request);
        client.configure_provider_wire_evidence(Some(Arc::new(StepWireRecorder::default())));
        let super::ApiClientStream {
            mut events,
            execution,
            ..
        } = client.stream_with_transport_activity(request);
        let delivered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = delivered.clone();
        let (consumer, producer) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            execution.unwrap().drive(async move {
                while let Some(event) = events.next().await {
                    if matches!(event.unwrap(), AssistantEvent::TextDelta(_)) {
                        observed.store(true, Ordering::SeqCst);
                        panic!("controlled SDK receiver panic after actual response body");
                    }
                }
                panic!("fixture did not deliver its body");
            }),
        )
        .await
        .unwrap();
        assert!(consumer.is_err());
        assert!(
            producer.is_ok(),
            "physical cleanup acknowledges stop without relabeling the consumer panic"
        );
        assert!(delivered.load(Ordering::SeqCst));
        server.await.unwrap();
        assert_eq!(client.transport_pool.stats().in_flight, 0);
    }
    #[tokio::test]
    async fn runtime_message_stop_is_published_only_after_cache_warm_commit() {
        let pool = Arc::new(ProviderTransportPool::new(2));
        let leader = pool
            .acquire_cache_warmup(
                "terminal-order",
                b"shared|agent-a",
                std::time::Duration::from_millis(25),
                true,
            )
            .await;
        assert!(leader.is_leader());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let terminal = tokio::spawn(async move {
            commit_provider_cache_before_terminal(leader, true, &sender).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
                .await
                .expect("terminal publication"),
            Some(Ok(AssistantEvent::MessageStop))
        ));
        assert!(terminal.await.expect("terminal task"));

        let extension = pool
            .acquire_cache_warmup(
                "terminal-order",
                b"shared|agent-a|next",
                std::time::Duration::ZERO,
                true,
            )
            .await;
        assert!(
            !extension.is_leader(),
            "warm commit must survive stream drop"
        );
    }

    #[test]
    fn streaming_tool_start_with_null_input_waits_for_json_deltas() {
        let mut events = Vec::new();
        let mut pending_tools = BTreeMap::new();

        push_provider_output_block(
            OutputContentBlock::ToolUse {
                id: "call-test".to_string(),
                name: "read_file".to_string(),
                input: serde_json::Value::Null,
            },
            1,
            &mut events,
            &mut pending_tools,
            true,
        );

        assert!(matches!(
            events.as_slice(),
            [AssistantEvent::ItemStarted { index: 1, .. }]
        ));
        assert_eq!(
            pending_tools.remove(&1),
            Some((
                "call-test".to_string(),
                "read_file".to_string(),
                String::new()
            ))
        );
    }

    fn tool(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_string(),
            description: None,
            input_schema: json!({"type": "object"}),
        }
    }

    #[derive(Default)]
    struct StepWireRecorder {
        records: std::sync::Mutex<Vec<super::ProviderWireEvidence>>,
        attempts: std::sync::atomic::AtomicUsize,
        refuse: std::sync::atomic::AtomicBool,
        advance_policy: Option<crate::permissions::SessionExecutionPolicyControl>,
        revoke_skill: Option<Arc<std::sync::atomic::AtomicBool>>,
    }

    #[async_trait::async_trait]
    impl super::ProviderWireEvidenceWriter for StepWireRecorder {
        async fn persist_physical(
            &self,
            context: &super::ProviderRequestEvidenceContext,
            observation: super::ProviderPhysicalObservation,
            _source: Option<&crate::execution_core::services::AuthorizedRequestedCancellation>,
        ) -> Result<super::AcknowledgedProviderPhysicalObservation, RuntimeError> {
            observation.validate(context)?;
            super::AcknowledgedProviderPhysicalObservation::from_written(&observation, 1)
        }

        async fn persist(
            &self,
            _: &super::ProviderRequestEvidenceContext,
            evidence: super::ProviderWireEvidence,
        ) -> Result<(), crate::RuntimeError> {
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.refuse.load(Ordering::SeqCst) {
                return Err(crate::RuntimeError::new("fixture durable write refused"));
            }
            self.records.lock().unwrap().push(evidence);
            if let Some(revoked) = &self.revoke_skill {
                revoked.store(true, Ordering::SeqCst);
            }
            if let Some(source) = &self.advance_policy {
                let mut policy = source.snapshot();
                policy.revision += 1;
                source.replace(policy).unwrap();
            }
            Ok(())
        }
        async fn persist_outcome(&self, _: &super::ProviderRequestEvidenceContext, _: super::ProviderAttemptOutcomeEvidence) -> Result<(), crate::RuntimeError> { Ok(()) }
    }

    fn step_wire_fixture(
        endpoint: String,
        workspace: &std::path::Path,
    ) -> (ProviderRuntimeClient, ApiRequest) {
        let registry = Arc::new(
            ProviderRegistry::new_with_context_windows(
                ProvidersConfig {
                    providers: HashMap::from([(
                        "fixture".into(),
                        ProviderConfig {
                            reasoning_effort: None,
                            name: "fixture".into(),
                            base_url: endpoint,
                            api_key: "fixture-secret".into(),
                            models: vec!["fixture-model".into()],
                            protocol: Some("completions".into()),
                            parallel_tool_calls: Default::default(),
                            early_tool_start: Default::default(),
                        },
                    )]),
                },
                BTreeMap::from([("fixture-model".into(), 32768)]),
            )
            .unwrap(),
        );
        let host = tools::ToolHost::builtin("wire-fixture", workspace);
        let lease = host.pin_snapshot();
        let definitions = lease.snapshot().catalog.definitions(None).into_iter().filter(|definition| definition.name == "read_file")
            .map(|definition| ToolDefinition { name: definition.name, description: definition.description, input_schema: definition.input_schema }).collect();
        let mut client = ProviderRuntimeClient::new(registry, "fixture-model".into(), definitions).unwrap();
        client.configure_tool_exposure(exposure(&["read_file"], &[], 1));
        let budget = crate::context_ledger::RequestBudgetReport::for_attempt("fixture-model", 32_768, 4_096, 0, 0, 0);
        let mut request = ApiRequest {
            bound_step: None,
            prompt: PromptAssembly::new(vec!["fixture system".into()]),
            messages: Vec::new().into(),
            model: "fixture-model".into(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: budget.clone(),
            preparation: Default::default(),
            provider_evidence_context: Some(super::ProviderRequestEvidenceContext {
            graph_owner: None,
                session_id: "wire-session".into(),
                execution_id: "wire-execution".into(),
                turn_id: "wire-turn".into(),
                request_sequence: 1,
                request_compiler_cache_hit: false,
                budget,
                attempt: 1,
            }),
        };
        let step = crate::step_snapshot::BoundStep::bind(harness_contract::step::StepIdentity {
            step_id: "wire-step".into(), session_id: "wire-session".into(), turn_id: "wire-turn".into(), execution_id: Some("wire-execution".into()),
            actor_ref: None, run_ref: None, attempt: Some(1), epoch: None, goal_ref: None, program_ref: None, task_ref: None,
            build_ref: "isolated-wire-fixture".into(),
        }, lease, &client.step_tool_definitions().unwrap(), &json!({}), &crate::step_snapshot::request_material(&request),
            BTreeMap::from([("provider_registry".into(), client.context_inventory().provider_registry_revision.to_string())]), i64::MAX).unwrap();
        request.bound_step = Some(Arc::new(step));
        (client, request)
    }

    fn control_resource_fixture(
        client: &ProviderRuntimeClient,
    ) -> (
        Arc<crate::execution_core::graph::ExecutionResourceManager>,
        crate::ProviderResourceConfig,
    ) {
        use crate::execution_core::graph::*;
        let one = crate::ProviderQuotaPolicy::new(1, 1, 1, 0);
        let config = crate::ProviderResourceConfig {
            global: one,
            fallback: one,
            ..Default::default()
        };
        let mut quotas = config.materialize(&client.registry.pin()).quotas;
        quotas.push((
            ExecutionResourceKind::ContextBytes,
            ResourceQuota::new(4 * 1024 * 1024, 4 * 1024 * 1024, 4 * 1024 * 1024).unwrap(),
        ));
        (Arc::new(ExecutionResourceManager::new(quotas)), config)
    }

    #[tokio::test]
    async fn control_auto_admission_revalidates_after_original_quota_queue() {
        use crate::execution_core::graph::*;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let (client, _) = step_wire_fixture(
            format!("http://{}/v1", listener.local_addr().unwrap()),
            workspace.path(),
        );
        let (manager, config) = control_resource_fixture(&client);
        let client = client.with_control_resources(
            Arc::clone(&manager),
            Arc::new(std::sync::RwLock::new(config)),
            ExecutionServiceClass::Foreground,
            "queued-control-session",
        );
        let ResourceAdmissionDecision::Granted { lease: blocker, .. } = manager
            .admit(ResourceAdmissionRequest::new(
                ExecutionServiceClass::Foreground,
                [(ExecutionResourceKind::Provider, 1)],
            ))
            .await
            .unwrap()
        else {
            panic!("fixture blocker");
        };
        let running_client = client.clone();
        let running = tokio::spawn(async move {
            running_client
                .complete_control_analysis("fixture-model", "system", "input", 32)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .queued_waiters
                != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(client.wire_preparation_count(), 1);
        assert!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases
                > 0
        );
        let mut replacement = client.registry.pin().config().clone();
        replacement.providers.get_mut("fixture").unwrap().api_key = "new-route-credential".into();
        client.registry.replace(replacement).unwrap();
        drop(blocker);
        let failure = tokio::time::timeout(std::time::Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(failure.contains("profile/transport changed"));
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases,
            0
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            0
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .queued_waiters,
            0
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn control_auto_admission_cancel_drops_actual_local_future_without_claiming_remote_exit()
    {
        use crate::execution_core::graph::*;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 1024];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            drop(socket);
        });
        let workspace = tempfile::tempdir().unwrap();
        let (client, _) = step_wire_fixture(format!("http://{address}/v1"), workspace.path());
        let (manager, config) = control_resource_fixture(&client);
        let client = client.with_control_resources(
            Arc::clone(&manager),
            Arc::new(std::sync::RwLock::new(config)),
            ExecutionServiceClass::Foreground,
            "cancelled-control-session",
        );
        let running_client = client.clone();
        let running = tokio::spawn(async move {
            running_client
                .complete_control_analysis("fixture-model", "system", "input", 32)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            1
        );
        assert!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases
                > 0
        );
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            0
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases,
            0
        );
        assert_eq!(client.transport_pool.stats().in_flight, 0);
        assert!(
            !server.is_finished(),
            "remote completion is still unknown after the local future exits"
        );
        release_tx.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn control_prepared_body_uses_exact_wire_and_existing_capacity_one_lease() {
        use crate::execution_core::graph::*;
        use sha2::Digest;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let body = loop {
                let mut buffer = [0; 1024];
                let n = socket.read(&mut buffer).await.unwrap();
                assert!(n > 0 && bytes.len() < 65536);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(offset) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..offset]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .parse()
                        .unwrap();
                    if bytes.len() >= offset + 4 + length {
                        break bytes[offset + 4..offset + 4 + length].to_vec();
                    }
                }
            };
            let response = r#"{"id":"control-response","model":"fixture-model","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2}}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).as_bytes()).await.unwrap();
            body
        });
        let workspace = tempfile::tempdir().unwrap();
        let (client, _) = step_wire_fixture(format!("http://{address}/v1"), workspace.path());
        let (manager, config) = control_resource_fixture(&client);
        let client = client.with_preparation_resources(Arc::clone(&manager));
        let prepared = client
            .prepare_control_analysis("fixture-model", "control system", "untrusted input", 32)
            .await
            .unwrap();
        let digest = prepared.wire().body_sha256.clone();
        let total = prepared.estimated_total_tokens();
        assert!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases
                > 0
        );
        let demand = config.admission_demands("fixture", "fixture-model", total);
        let ResourceAdmissionDecision::Granted { lease, .. } = manager
            .admit(ResourceAdmissionRequest::new(
                ExecutionServiceClass::Foreground,
                demand,
            ))
            .await
            .unwrap()
        else {
            panic!("one slot must grant");
        };
        let completion = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.complete_admitted_control_analysis(prepared, &lease, &config),
        )
        .await
        .unwrap()
        .unwrap();
        let bytes = server.await.unwrap();
        assert_eq!(format!("{:x}", sha2::Sha256::digest(&bytes)), digest);
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["model"], "fixture-model");
        assert_eq!(body["messages"][0]["content"], "control system");
        assert_eq!(body["messages"][1]["content"], "untrusted input");
        assert_eq!(total, (bytes.len() as u64 + 3) / 4 + 32);
        assert_eq!(completion.text, "ok");
        assert_eq!(
            (completion.input_tokens, completion.output_tokens),
            (Some(7), Some(2))
        );
        assert_eq!(client.wire_preparation_count(), 1);
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases,
            0
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            1
        );
        drop(lease);
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            0
        );
    }

    #[tokio::test]
    async fn control_prepared_source_changes_refuse_before_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let workspace = tempfile::tempdir().unwrap();
        for mutation in ["registry", "reasoning", "account", "owner"] {
            let (mut client, _) = step_wire_fixture(
                format!("http://{}/v1", listener.local_addr().unwrap()),
                workspace.path(),
            );
            let prepared = client
                .prepare_control_analysis("fixture-model", "system", "input", 32)
                .await
                .unwrap();
            match mutation {
                "registry" => {
                    let mut config = client.registry.pin().config().clone();
                    config.providers.get_mut("fixture").unwrap().api_key =
                        "different-account-secret".into();
                    client.registry.replace(config).unwrap();
                }
                "reasoning" => client.set_reasoning_effort(Some("high".into())),
                "account" => {
                    client.transport_pool.record_provider_failure(
                        client.registry.revision(),
                        "fixture",
                        model_protocol::provider_failure::ProviderFailureScope::Account,
                    );
                }
                "owner" => {
                    client.transport_pool = Arc::new(ProviderTransportPool::default());
                }
                _ => unreachable!(),
            }
            assert!(
                client.send_control_analysis(prepared).await.is_err(),
                "{mutation}"
            );
            assert_eq!(client.transport_pool.stats().in_flight, 0);
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn control_admitted_lease_rejects_foreign_owner_wrong_model_and_underweighted_tokens() {
        use crate::execution_core::graph::*;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let workspace = tempfile::tempdir().unwrap();
        for mutation in ["foreign_owner", "model", "pressure"] {
            let (client, _) = step_wire_fixture(
                format!("http://{}/v1", listener.local_addr().unwrap()),
                workspace.path(),
            );
            let (manager, config) = control_resource_fixture(&client);
            let client = client.with_preparation_resources(Arc::clone(&manager));
            let prepared = client
                .prepare_control_analysis("fixture-model", "system", &"x".repeat(20000), 32)
                .await
                .unwrap();
            let mut demands = config.admission_demands(
                "fixture",
                "fixture-model",
                prepared.estimated_total_tokens(),
            );
            let mut lease_manager = Arc::clone(&manager);
            if mutation == "foreign_owner" {
                lease_manager = control_resource_fixture(&client).0;
            }
            if mutation == "model" {
                demands.retain(|(k, _)| !matches!(k, ExecutionResourceKind::ProviderModel(_)));
            }
            if mutation == "pressure" {
                for (kind, weight) in &mut demands {
                    if matches!(kind, ExecutionResourceKind::ProviderTokenPool(_)) {
                        assert!(*weight > 1);
                        *weight = 1;
                    }
                }
            }
            let ResourceAdmissionDecision::Granted { lease, .. } = lease_manager
                .admit(ResourceAdmissionRequest::new(
                    ExecutionServiceClass::Foreground,
                    demands,
                ))
                .await
                .unwrap()
            else {
                panic!("fixture lease");
            };
            assert!(
                client
                    .complete_admitted_control_analysis(prepared, &lease, &config)
                    .await
                    .unwrap_err()
                    .contains("does not match"),
                "{mutation}"
            );
            assert_eq!(
                manager
                    .snapshot(&ExecutionResourceKind::ContextBytes)
                    .unwrap()
                    .active_leases,
                0
            );
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn control_unknown_model_capacity_and_declared_overflow_cannot_render() {
        let workspace = tempfile::tempdir().unwrap();
        for mode in ["unknown", "overflow"] {
            let (mut client, _) =
                step_wire_fixture("http://127.0.0.1:9/v1".into(), workspace.path());
            let config = client.registry.pin().config().clone();
            client.registry = Arc::new(
                ProviderRegistry::new_with_context_windows(
                    config,
                    if mode == "overflow" {
                        BTreeMap::from([("fixture-model".into(), 1024)])
                    } else {
                        BTreeMap::new()
                    },
                )
                .unwrap(),
            );
            let failure = match client
                .prepare_control_analysis(
                    "fixture-model",
                    "system",
                    "input",
                    if mode == "overflow" { 1024 } else { 32 },
                )
                .await
            {
                Ok(_) => panic!("invalid capacity must refuse"),
                Err(error) => error,
            };
            assert!(
                failure.contains(if mode == "unknown" {
                    "capacity is unknown"
                } else {
                    "exceeds verified context"
                }),
                "{failure}"
            );
            assert_eq!(client.wire_preparation_count(), 0);
            assert_eq!(client.transport_pool.stats().in_flight, 0);
        }
    }

    #[tokio::test]
    async fn control_preparation_capacity_failure_does_not_render_or_dispatch() {
        use crate::execution_core::graph::*;
        let workspace = tempfile::tempdir().unwrap();
        let (client, _) = step_wire_fixture("http://127.0.0.1:9/v1".into(), workspace.path());
        let manager = Arc::new(ExecutionResourceManager::new([(
            ExecutionResourceKind::ContextBytes,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )]));
        let client = client.with_preparation_resources(manager);
        assert!(client
            .prepare_control_analysis("fixture-model", "system", "input", 32)
            .await
            .is_err());
        assert_eq!(client.wire_preparation_count(), 0);
        assert_eq!(client.transport_pool.stats().in_flight, 0);
    }

    #[tokio::test]
    async fn provider_config_effort_reaches_actual_wire_and_follows_selected_model() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut root, mut request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), workspace.path());
        let prior_snapshot = root.registry.pin();
        let (prior_entry, prior_message) = root.prepare_message_request(&request, &prior_snapshot).unwrap();
        assert!(prior_entry.client.wire_request(&prior_message).unwrap().body.get("reasoning_effort").is_none());
        std::fs::write(workspace.path().join("config.yaml"),
            "model: fixture-model\nproviders:\n  fixture:\n    base_url: http://127.0.0.1:1/v1\n    api_key: fixture-secret\n    protocol: completions\n    models: [fixture-model]\n    reasoning_effort: high\n").unwrap();
        let config = crate::ConfigLoader::new(workspace.path(), workspace.path()).load().unwrap();
        let mut providers = config.providers().clone();
        assert_eq!(providers.providers["fixture"].reasoning_effort.as_deref(), Some("high"));
        let mut other = providers.providers["fixture"].clone();
        other.name = "other".into();
        other.models = vec!["other-model".into()];
        other.reasoning_effort = None;
        providers.providers.insert("other".into(), other);
        root.registry.replace(providers).unwrap();
        let snapshot = root.registry.pin();
        let (old_entry, old_message) = root.prepare_message_request(&request, &prior_snapshot).unwrap();
        assert!(old_entry.client.wire_request(&old_message).unwrap().body.get("reasoning_effort").is_none());
        let (entry, message) = root.prepare_message_request(&request, &snapshot).unwrap();
        assert_eq!(entry.client.wire_request(&message).unwrap().body["reasoning_effort"], "high");
        let control = root.prepare_control_analysis("fixture-model", "system", "input", 64).await.unwrap();
        assert_eq!(control.wire.wire_request().body["reasoning_effort"], "high");
        let member = ProviderRuntimeClient::new(Arc::clone(&root.registry), "fixture-model".into(), Vec::new()).unwrap();
        let (entry, message) = member.prepare_message_request(&request, &snapshot).unwrap();
        assert_eq!(entry.client.wire_request(&message).unwrap().body["reasoning_effort"], "high");
        request.model = "other-model".into();
        let (entry, message) = root.prepare_message_request(&request, &snapshot).unwrap();
        assert!(entry.client.wire_request(&message).unwrap().body.get("reasoning_effort").is_none());
        assert_eq!(request_reasoning_effort("fixture-model", &["openai_compat_enable_thinking".into()], Some("none".into()), Some("high".into())).as_deref(), Some("none"));
        root.set_reasoning_effort(Some("medium".into()));
        request.model = "fixture-model".into();
        let (entry, message) = root.prepare_message_request(&request, &snapshot).unwrap();
        assert_eq!(entry.client.wire_request(&message).unwrap().body["reasoning_effort"], "medium");
    }

    #[tokio::test]
    async fn wire_budget_preview_preserves_required_choice_and_counts_the_actual_adapter_body() {
        let workspace = tempfile::tempdir().unwrap();
        let (mut client, mut request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), workspace.path());
        request.budget.set_context_window_source("configured");
        request.budget.compiled_provider_registry_revision = Some(client.registry.revision());
        client.configure_tool_choice(true, Some("read_file".into()));
        client.prepare_request_budget(&mut request).await.unwrap();
        client.prepare_request_budget(&mut request).await.unwrap();
        assert!(client.tool_choice_required);
        assert_eq!(
            client.tool_choice_required_tool_name.as_deref(),
            Some("read_file")
        );
        assert_eq!(client.wire_preparation_count(), 1);
        let prepared = request.preparation.prepared.as_ref().unwrap();
        assert_eq!(prepared.wire.wire_request().body["model"], "fixture-model");
        assert!(prepared.message.tool_choice.is_some());
        assert_eq!(
            prepared.wire.wire_request().body["tool_choice"]["function"]["name"],
            "read_file"
        );
        assert_eq!(
            request.budget.render_estimate_source.as_deref(),
            Some("provider_wire_utf8_bytes_div4_estimated_v1")
        );
        assert!(request.budget.executable);
    }

    #[tokio::test]
    async fn wire_budget_refuses_unknown_profile_overflow_and_changed_profile_without_network() {
        let workspace = tempfile::tempdir().unwrap();
        let (client, mut request) =
            step_wire_fixture("http://127.0.0.1:1/v1".into(), workspace.path());
        let error = client
            .prepare_request_budget(&mut request)
            .await
            .unwrap_err();
        assert_eq!(
            error.context_capacity_failure(),
            Some(crate::context_ledger::ContextCapacityFailure::UnknownProfile)
        );
        request.budget.set_context_window_source("configured");
        request.budget.compiled_provider_registry_revision =
            Some(client.registry.revision().saturating_add(1));
        assert_eq!(
            client
                .prepare_request_budget(&mut request)
                .await
                .unwrap_err()
                .preparation_failure(),
            Some(super::ProviderPreparationFailure::ProfileChanged)
        );
        request.budget.compiled_provider_registry_revision = Some(client.registry.revision());
        request.budget.hard_input_cap_tokens = 1;
        let error = client
            .prepare_request_budget(&mut request)
            .await
            .unwrap_err();
        assert_eq!(
            error.context_capacity_failure(),
            Some(crate::context_ledger::ContextCapacityFailure::RenderOverflow)
        );
        assert!(!request.budget.executable);
    }

    struct SkillFenceSource {
        pin: harness_contract::skill::SkillInstructionPin,
        revoked: Arc<std::sync::atomic::AtomicBool>,
        reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl crate::RuntimeSkillInstructionSource for SkillFenceSource {
        fn validate_pins(
            &self,
            pins: &[harness_contract::skill::SkillInstructionPin],
        ) -> Result<(), String> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if !self.revoked.load(Ordering::SeqCst) && pins == std::slice::from_ref(&self.pin) {
                Ok(())
            } else {
                Err("original approval revoked or mismatched".into())
            }
        }
        async fn load_instruction(
            &self,
            _: &crate::SkillInvocation,
            _: &crate::RuntimeSkillUsageContext,
        ) -> Result<Option<crate::RuntimeSkillPromptAsset>, String> {
            Err("wire fence fixture does not page in".into())
        }
    }
    fn bind_skill_fixture(
        client: &ProviderRuntimeClient,
        request: &mut ApiRequest,
    ) -> Arc<SkillFenceSource> {
        use sha2::{Digest, Sha256};
        let item = crate::context_runtime::ContextItem::new(
            "skill:approved",
            crate::context_runtime::ContextSourceKind::Task,
            crate::context_runtime::ContextRole::Instruction,
            "ORIGINAL_APPROVED_WIRE_SKILL",
        );
        let pin = crate::skill::test_support::pin(
            "approved",
            None,
            "ORIGINAL_APPROVED_WIRE_SKILL",
            &["read_file"],
        );
        request.prompt.push_context_item(&item);
        request.prompt = request
            .prompt
            .clone()
            .with_skill_instruction_pins(&std::collections::BTreeMap::from([(
                "skill:approved".into(),
                (
                    format!("sha256:{:x}", Sha256::digest(item.content.as_bytes())),
                    pin.clone(),
                ),
            )]))
            .unwrap();
        let source = Arc::new(SkillFenceSource {
            pin: pin.clone(),
            revoked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        request
            .preparation
            .bind_skill_source(source.clone(), vec![pin]);
        let step = request.bound_step.as_ref().unwrap();
        let snapshot = step.snapshot().clone();
        request.bound_step = Some(Arc::new(
            crate::step_snapshot::BoundStep::bind(
                snapshot.identity,
                step.lease().clone(),
                &client.step_tool_definitions().unwrap(),
                &json!({}),
                &crate::step_snapshot::request_material(request),
                snapshot.related_revisions,
                snapshot.expires_at_ms,
            )
            .unwrap(),
        ));
        source
    }

    #[test]
    fn omitted_skill_without_contributing_tools_drops_its_dispatch_dependency() {
        let pin = crate::skill::test_support::pin("omitted", None, "OMITTED_BODY", &["skill_tool"]);
        let source = Arc::new(SkillFenceSource {
            pin: pin.clone(),
            revoked: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        for (contributed, actual) in [
            (vec!["skill_tool"], vec!["independent_tool"]),
            // Even the same tool name is independent when it entered through
            // the original explicit capability grant, rather than this Skill.
            (vec![], vec!["skill_tool"]),
        ] {
            let mut preparation = super::ProviderRequestPreparation::default();
            preparation.bind_skill_source(source.clone(), vec![pin.clone()]);
            preparation.bind_skill_tool_refs(contributed.into_iter().map(str::to_owned).collect());
            preparation
                .retain_actual_skill_pins(&[], &actual.into_iter().map(str::to_owned).collect())
                .unwrap();
            assert!(preparation.skill_source.is_none());
            assert_eq!(source.reads.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn skill_revocation_at_original_wire_commit_fences_actual_http_and_missing_source_refuses(
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = tempfile::tempdir().unwrap();
        for condition in [
            "revoked_before_dispatch",
            "revoked_at_wire_commit",
            "missing_source",
            "tool_without_instruction",
        ] {
            let (mut client, mut request) = step_wire_fixture(
                format!("http://{}/v1", listener.local_addr().unwrap()),
                root.path(),
            );
            request.budget.set_context_window_source("configured");
            let source = bind_skill_fixture(&client, &mut request);
            let recorder = Arc::new(StepWireRecorder {
                revoke_skill: (matches!(
                    condition,
                    "revoked_at_wire_commit" | "tool_without_instruction"
                ))
                .then(|| source.revoked.clone()),
                ..Default::default()
            });
            client.configure_provider_wire_evidence(Some(recorder.clone()));
            if condition == "revoked_before_dispatch" {
                source.revoked.store(true, Ordering::SeqCst);
            }
            if condition == "missing_source" {
                request.preparation.skill_source = None;
            }
            if condition == "tool_without_instruction" {
                request.prompt.contextual_packets.clear();
                request
                    .preparation
                    .bind_skill_tool_refs(std::collections::BTreeSet::from(["read_file".into()]));
                let step = request.bound_step.as_ref().unwrap();
                let snapshot = step.snapshot().clone();
                request.bound_step = Some(Arc::new(
                    crate::step_snapshot::BoundStep::bind(
                        snapshot.identity,
                        step.lease().clone(),
                        &client.step_tool_definitions().unwrap(),
                        &json!({}),
                        &crate::step_snapshot::request_material(&request),
                        snapshot.related_revisions,
                        snapshot.expires_at_ms,
                    )
                    .unwrap(),
                ));
            }
            let events = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.stream(request).collect::<Vec<_>>(),
            )
            .await
            .unwrap();
            assert!(events.iter().any(|event|matches!(event,Err(error) if error.preparation_failure()==Some(super::ProviderPreparationFailure::SourceChanged))),"{condition}: {events:?}");
            assert_eq!(
                recorder.records.lock().unwrap().len(),
                usize::from(matches!(
                    condition,
                    "revoked_at_wire_commit" | "tool_without_instruction"
                )),
                "{condition}"
            );
            assert_eq!(client.transport_pool.stats().in_flight, 0);
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn bound_step_wire_digest_matches_sent_bytes_and_is_recorded_before_http() {
        use sha2::Digest;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let recorder = Arc::new(StepWireRecorder::default());
        let observed = Arc::clone(&recorder);
        let fixture = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(observed.records.lock().unwrap().len(), 1);
            let mut request_bytes = Vec::new();
            let body = loop {
                let mut buffer = [0_u8; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0 && request_bytes.len() < 1024 * 1024);
                request_bytes.extend_from_slice(&buffer[..count]);
                if let Some(offset) = request_bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request_bytes[..offset]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if request_bytes.len() >= offset + 4 + length {
                        break request_bytes[offset + 4..offset + 4 + length].to_vec();
                    }
                }
            };
            let response = "data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
            body
        });
        let workspace = tempfile::tempdir().unwrap();
        let (mut client, mut request) = step_wire_fixture(format!("http://{address}/v1"), workspace.path());
        request.budget.set_context_window_source("configured");
        let skill_source = bind_skill_fixture(&client, &mut request);
        client.prepare_request_budget(&mut request).await.unwrap();
        let budget_input = request.budget.rendered_input_tokens;
        let snapshot = request.bound_step.as_ref().unwrap().snapshot().clone();
        client.configure_provider_wire_evidence(Some(recorder.clone()));
        let events = tokio::time::timeout(std::time::Duration::from_secs(5), client.stream(request).collect::<Vec<_>>()).await.unwrap();
        assert!(events.iter().all(Result::is_ok), "{events:?}");
        assert!(events.iter().any(|event| matches!(event, Ok(AssistantEvent::MessageStop))));
        let sent_body = fixture.await.unwrap();
        assert!(String::from_utf8_lossy(&sent_body).contains("ORIGINAL_APPROVED_WIRE_SKILL"));
        assert!(
            skill_source.reads.load(Ordering::SeqCst) >= 2,
            "original source checked again at actual HTTP"
        );
        let records = recorder.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].wire_request.body,
            serde_json::from_slice::<serde_json::Value>(&sent_body).unwrap()
        );
        assert_eq!(
            records[0].wire_request.body_sha256,
            format!("{:x}", sha2::Sha256::digest(&sent_body))
        );
        assert_eq!(budget_input, Some((sent_body.len() as u64 + 3) / 4));
        assert_eq!(client.wire_preparation_count(), 1);
        let render = records[0].step_render.as_ref().unwrap();
        assert_eq!(render.step_id, snapshot.identity.step_id);
        assert_eq!(render.catalog_digest, snapshot.catalog_digest);
        assert_eq!(render.render_digest, harness_contract::step::canonical_digest("provider-wire-render", &serde_json::to_value(&records[0].wire_request).unwrap()).unwrap());
        assert!(!serde_json::to_string(&records[0]).unwrap().contains("fixture-secret"));
        assert_eq!(client.transport_pool.stats().in_flight, 0);
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn missing_mismatched_or_failed_step_evidence_refuses_before_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        for condition in ["missing_writer", "wrong_session", "persist_failed", "changed_request", "stale_registry"] {
            let (mut client, mut request) = step_wire_fixture(format!("http://{address}/v1"), workspace.path());
            request.budget.set_context_window_source("configured");
            let recorder = Arc::new(StepWireRecorder::default());
            if condition != "missing_writer" { client.configure_provider_wire_evidence(Some(recorder.clone())); }
            if condition == "wrong_session" { request.provider_evidence_context.as_mut().unwrap().session_id = "other-session".into(); }
            if condition == "persist_failed" { recorder.refuse.store(true, Ordering::SeqCst); }
            if condition == "changed_request" { request.prompt = PromptAssembly::new(vec!["changed system".into()]); }
            if condition == "stale_registry" {
                let step = request.bound_step.as_ref().unwrap();
                let mut snapshot = step.snapshot().clone();
                snapshot
                    .related_revisions
                    .insert("provider_registry".into(), "999999".into());
                request.bound_step = Some(Arc::new(
                    crate::step_snapshot::BoundStep::bind(
                        snapshot.identity,
                        step.lease().clone(),
                        &client.step_tool_definitions().unwrap(),
                        &json!({}),
                        &crate::step_snapshot::request_material(&request),
                        snapshot.related_revisions,
                        snapshot.expires_at_ms,
                    )
                    .unwrap(),
                ));
            }
            let events = tokio::time::timeout(std::time::Duration::from_secs(2), client.stream(request).collect::<Vec<_>>()).await.unwrap();
            assert!(events.iter().any(Result::is_err), "{condition}: {events:?}");
            assert_eq!(recorder.records.lock().unwrap().len(), 0, "{condition}");
            assert_eq!(
                recorder.attempts.load(std::sync::atomic::Ordering::SeqCst),
                usize::from(condition == "persist_failed"),
                "intended evidence gate must be reached: {condition}"
            );
            assert_eq!(client.transport_pool.stats().in_flight, 0);
        }
        assert!(tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept()).await.is_err());
        assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn policy_change_during_durable_wire_write_refuses_before_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let (mut client, mut request) = step_wire_fixture(
            format!("http://{}/v1", listener.local_addr().unwrap()),
            root.path(),
        );
        request.budget.set_context_window_source("configured");
        let source = crate::permissions::SessionExecutionPolicyControl::new(
            crate::permissions::PermissionMode::ReadOnly,
        );
        request
            .preparation
            .bind_policy_source(source.clone(), source.revision());
        let recorder = Arc::new(StepWireRecorder {
            advance_policy: Some(source),
            ..Default::default()
        });
        client.configure_provider_wire_evidence(Some(recorder.clone()));
        let events = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.stream(request).collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert_eq!(recorder.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(recorder.records.lock().unwrap().len(), 1);
        assert!(events.iter().any(|event|matches!(event,Err(error) if error.preparation_failure()==Some(super::ProviderPreparationFailure::SourceChanged))));
        assert_eq!(client.transport_pool.stats().in_flight, 0);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn prepared_request_rejects_changed_inputs_and_allows_explicit_repreparation() {
        let root = tempfile::tempdir().unwrap();
        let (client, mut request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), root.path());
        request.bound_step = None;
        request.budget.set_context_window_source("configured");
        client.prepare_request_budget(&mut request).await.unwrap();
        let first = request
            .preparation
            .prepared
            .as_ref()
            .unwrap()
            .entry
            .request_context
            .request_id
            .clone();
        request.prompt.push_runtime_context("new required material");
        let error = client
            .prepare_request_budget(&mut request)
            .await
            .unwrap_err();
        assert_eq!(
            error.preparation_failure(),
            Some(super::ProviderPreparationFailure::SourceChanged)
        );
        assert_eq!(client.wire_preparation_count(), 1);
        request.preparation.invalidate();
        client.prepare_request_budget(&mut request).await.unwrap();
        assert_eq!(client.wire_preparation_count(), 2);
        assert_ne!(
            first,
            request
                .preparation
                .prepared
                .as_ref()
                .unwrap()
                .entry
                .request_context
                .request_id
        );
        assert!(serde_json::to_string(
            &request
                .preparation
                .prepared
                .as_ref()
                .unwrap()
                .wire
                .wire_request()
                .body
        )
        .unwrap()
        .contains("new required material"));
    }

    #[tokio::test]
    async fn prepared_capacity_is_reserved_before_render_and_released_with_the_body() {
        use crate::execution_core::graph::*;
        let root = tempfile::tempdir().unwrap();
        for capacity in [1, 16 * 1024 * 1024] {
            let manager = Arc::new(ExecutionResourceManager::new([(
                ExecutionResourceKind::ContextBytes,
                ResourceQuota::new(capacity, capacity, capacity).unwrap(),
            )]));
            let (client, mut request) =
                step_wire_fixture("http://127.0.0.1:1/v1".into(), root.path());
            let client = client.with_preparation_resources(manager.clone());
            request.budget.set_context_window_source("configured");
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                client.prepare_request_budget(&mut request),
            )
            .await
            .unwrap();
            if capacity == 1 {
                assert!(result.is_err());
                assert_eq!(client.wire_preparation_count(), 0);
                assert_eq!(
                    manager
                        .snapshot(&ExecutionResourceKind::ContextBytes)
                        .unwrap()
                        .active_leases,
                    0
                );
            } else {
                result.unwrap();
                assert_eq!(client.wire_preparation_count(), 1);
                assert!((1..=16 * 1024 * 1024).contains(
                    &manager
                        .snapshot(&ExecutionResourceKind::ContextBytes)
                        .unwrap()
                        .active_leases
                ));
                request.preparation.invalidate();
                assert_eq!(
                    manager
                        .snapshot(&ExecutionResourceKind::ContextBytes)
                        .unwrap()
                        .active_leases,
                    0
                );
            }
        }
    }

    #[tokio::test]
    async fn original_provider_lease_survives_handoff_and_local_cancel_releases_capacity() {
        use crate::execution_core::graph::*;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut block = [0; 4096];
                let n = socket.read(&mut block).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&block[..n]);
                if let Some(offset) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..offset]);
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= offset + 4 + len {
                        break;
                    }
                }
                assert!(bytes.len() < 256 * 1024);
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
        });
        let manager = Arc::new(ExecutionResourceManager::new([
            (
                ExecutionResourceKind::Provider,
                ResourceQuota::new(1, 1, 1).unwrap(),
            ),
            (
                ExecutionResourceKind::ContextBytes,
                ResourceQuota::new(16 * 1024 * 1024, 16 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
            ),
        ]));
        let admission = || {
            ResourceAdmissionRequest::new(
                ExecutionServiceClass::Foreground,
                [(ExecutionResourceKind::Provider, 1)],
            )
            .with_fairness_key("wire-session")
        };
        let ResourceAdmissionDecision::Granted { lease, .. } =
            manager.admit(admission()).await.unwrap()
        else {
            panic!("fixture admission")
        };
        let root = tempfile::tempdir().unwrap();
        let (client, mut request) = step_wire_fixture(format!("http://{address}/v1"), root.path());
        let mut client = client.with_preparation_resources(manager.clone());
        client.configure_provider_wire_evidence(Some(Arc::new(StepWireRecorder::default())));
        request.budget.set_context_window_source("configured");
        request
            .preparation
            .retain_dispatch_lease(Some(Arc::new(lease)));
        let stream = client.stream(request);
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            1
        );
        assert!((1..=16 * 1024 * 1024).contains(
            &manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases
        ));
        assert!(!server.is_finished(), "remote completion is still unknown");
        drop(stream);
        let ResourceAdmissionDecision::Granted { lease: next, .. } = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            manager.admit(admission()),
        )
        .await
        .unwrap()
        .unwrap() else {
            panic!("local producer failed to release the original slot")
        };
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            1
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases,
            0
        );
        assert!(
            !server.is_finished(),
            "local cancellation does not certify remote completion"
        );
        drop(next);
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            0
        );
        release_tx.send(()).unwrap();
        server.await.unwrap();
    }

    #[test]
    fn used_and_recovered_step_provider_authorizations_cannot_dispatch_twice() {
        let root = tempfile::tempdir().unwrap();
        let (_, request) = step_wire_fixture("http://127.0.0.1:1/v1".into(), root.path());
        let step = request.bound_step.unwrap();
        let clone = step.clone();
        step.consume_provider_attempt().unwrap();
        assert_eq!(
            clone
                .consume_provider_attempt()
                .unwrap_err()
                .preparation_failure(),
            Some(super::ProviderPreparationFailure::Consumed)
        );
        let recovered =
            crate::step_snapshot::BoundStep::restore(step.snapshot().clone(), step.lease().clone())
                .unwrap();
        assert_eq!(
            recovered
                .consume_provider_attempt()
                .unwrap_err()
                .preparation_failure(),
            Some(super::ProviderPreparationFailure::Consumed)
        );
    }

    fn exposure(bootstrap: &[&str], active: &[&str], revision: u64) -> ToolExposureProjection {
        ToolExposureProjection {
            catalog_revision: 7,
            exposure_revision: revision,
            bootstrap_ids: bootstrap.iter().map(|id| (*id).to_string()).collect(),
            active_ids: active.iter().map(|id| (*id).to_string()).collect(),
            deferred_ids: Vec::new(),
            fallback_full: false,
            reason: "test".to_string(),
            schema_tokens: 0,
        }
    }

    #[test]
    fn unconfigured_client_exposes_no_tool_schema() {
        let tools = vec![tool("tool_search"), tool("read_file")];
        assert!(tool_definitions_for_exposure(&tools, None).is_empty());
    }

    #[test]
    fn explicit_projection_selects_bootstrap_and_active_ids_only() {
        let tools = vec![tool("tool_search"), tool("read_file"), tool("edit")];
        let projection = exposure(&["tool_search"], &["read_file"], 3);
        let visible = tool_definitions_for_exposure(&tools, Some(&projection))
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();

        assert_eq!(visible, vec!["tool_search", "read_file"]);
    }

    #[test]
    fn fallback_flag_does_not_bypass_explicit_ids() {
        let tools = vec![tool("tool_search"), tool("dangerous_tool")];
        let mut projection = exposure(&["tool_search"], &[], 1);
        projection.fallback_full = true;
        let visible = tool_definitions_for_exposure(&tools, Some(&projection));

        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].name, "tool_search");
    }

    #[test]
    fn admitted_action_uses_required_provider_tool_choice() {
        assert_eq!(
            provider_tool_choice(true, true, CapabilityState::Supported).unwrap(),
            Some(ToolChoice::Any)
        );
        assert_eq!(
            provider_tool_choice(true, false, CapabilityState::Supported).unwrap(),
            Some(ToolChoice::Auto)
        );
        assert_eq!(
            provider_tool_choice(true, false, CapabilityState::Unsupported).unwrap(),
            None
        );
        assert_eq!(
            provider_tool_choice(true, true, CapabilityState::Unsupported).unwrap(),
            None,
            "wire-unsupported tool_choice is omitted; the required intent is enforced by single-tool exposure"
        );
        assert!(provider_tool_choice(true, false, CapabilityState::Unknown).is_err());
        assert_eq!(
            provider_tool_choice(false, false, CapabilityState::Unknown).unwrap(),
            None
        );
        assert!(provider_tool_choice(false, true, CapabilityState::Supported).is_err());
    }

    #[test]
    fn named_governed_action_uses_provider_function_selection() {
        assert_eq!(
            provider_tool_choice_for_required_tool(
                true,
                true,
                CapabilityState::Supported,
                Some("task_publish"),
            )
            .expect("named native action must compile"),
            Some(ToolChoice::Tool {
                name: "task_publish".to_string(),
            })
        );
    }

    #[test]
    fn deepseek_v4_thinking_required_tool_choice_omits_before_network() {
        let thinking = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            model_protocol::provider_config::ProviderProtocol::Completions,
            "deepseek-v4-flash",
            Some("high"),
        );
        assert_eq!(
            thinking.supports_explicit_tool_choice.state,
            CapabilityState::Unsupported
        );
        assert_eq!(
            provider_tool_choice(true, true, thinking.supports_explicit_tool_choice.state).unwrap(),
            None
        );
        assert_eq!(
            provider_tool_choice(true, false, thinking.supports_explicit_tool_choice.state)
                .unwrap(),
            None
        );

        let non_thinking = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            model_protocol::provider_config::ProviderProtocol::Completions,
            "deepseek-v4-flash",
            None,
        );
        assert_eq!(
            non_thinking.supports_explicit_tool_choice.state,
            CapabilityState::Unsupported
        );
        assert_eq!(
            provider_tool_choice(true, true, non_thinking.supports_explicit_tool_choice.state)
                .unwrap(),
            None
        );
        assert_eq!(
            provider_tool_choice(
                true,
                false,
                non_thinking.supports_explicit_tool_choice.state
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn runtime_attempt_is_propagated_and_never_zero() {
        let context = super::ProviderRequestEvidenceContext {
            graph_owner: None,
            session_id: "session-1".to_string(),
            execution_id: "execution-1".to_string(),
            turn_id: "turn-1".to_string(),
            request_sequence: 1,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "model", 8_192, 1_024, 0, 0, 1,
            ),
            attempt: 7,
        };
        assert_eq!(provider_attempt(Some(&context)), 7);
        assert_eq!(provider_attempt(None), 1);
        assert_eq!(
            provider_attempt(Some(&super::ProviderRequestEvidenceContext {
            graph_owner: None,
                attempt: 0,
                ..context
            })),
            1
        );
    }

    #[test]
    fn provider_client_accepts_one_runtime_selected_model() {
        let registry = Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".to_string(),
                    ProviderConfig {
                        reasoning_effort: None,
                        name: "test".to_string(),
                        base_url: "https://example.test/v1".to_string(),
                        api_key: "test".to_string(),
                        models: vec!["primary".to_string(), "fallback".to_string()],
                        protocol: Some("completions".to_string()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("registry"),
        );
        ProviderRuntimeClient::new(registry, "primary".to_string(), Vec::new())
            .expect("single selected model must be valid");
    }

    #[tokio::test]
    async fn shared_account_fence_blocks_control_and_streaming_before_network_dispatch() {
        let registry = Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "token-plan".to_string(),
                    ProviderConfig {
                        reasoning_effort: None,
                        name: "token-plan".to_string(),
                        base_url: "http://127.0.0.1:9/v1".to_string(),
                        api_key: "test".to_string(),
                        models: vec!["qwen3.8-max".to_string()],
                        protocol: Some("completions".to_string()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("registry"),
        );
        let pool = Arc::new(ProviderTransportPool::new(1));
        let mut client = ProviderRuntimeClient::new_with_transport_pool(
            Arc::clone(&registry),
            Arc::clone(&pool),
            "qwen3.8-max".to_string(),
            Vec::new(),
        )
        .expect("client");
        assert!(pool.record_provider_failure(
            registry.revision(),
            "token-plan",
            model_protocol::provider_failure::ProviderFailureScope::Account,
        ));

        let control_error = client
            .complete_control_analysis("qwen3.8-max", "system", "input", 32)
            .await
            .expect_err("control analysis must honor the shared fence");
        assert!(control_error.contains("temporarily fenced"));

        let mut request = ApiRequest {
            bound_step: None,
            prompt: PromptAssembly::new(vec!["system".to_string()]),
            messages: Vec::new().into(),
            model: "qwen3.8-max".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "qwen3.8-max",
                32_768,
                4_096,
                0,
                0,
                0,
            ),
            preparation: Default::default(),
            provider_evidence_context: None,
        };
        request.budget.set_context_window_source("configured");
        let error = client
            .stream(request)
            .next()
            .await
            .expect("one fenced event")
            .expect_err("stream must honor the shared fence");
        assert_eq!(
            error.provider_failure_scope(),
            model_protocol::provider_failure::ProviderFailureScope::Account
        );
        assert!(error.to_string().contains("temporarily fenced"));
        assert_eq!(pool.stats().in_flight, 0, "fenced calls must not dispatch");
    }

    #[tokio::test]
    async fn token_plan_control_failure_fences_the_sibling_stream_after_one_http_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fixture listener");
        let address = listener.local_addr().expect("fixture address");
        let requests = Arc::new(AtomicUsize::new(0));
        let observed_requests = Arc::clone(&requests);
        let fixture = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("first request");
            let mut buffer = vec![0_u8; 16 * 1024];
            let _ = socket.read(&mut buffer).await.expect("read request");
            observed_requests.fetch_add(1, Ordering::SeqCst);
            let body = r#"{"error":{"message":"Your token-plan 1-week quota has been exhausted. The quota will reset later.","type":"insufficient_quota","code":"insufficient_quota"}}"#;
            let response = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });
        let registry = Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "token-plan".to_string(),
                    ProviderConfig {
                        reasoning_effort: None,
                        name: "token-plan".to_string(),
                        base_url: format!("http://{address}/v1"),
                        api_key: "test".to_string(),
                        models: vec!["qwen3.8-max".to_string()],
                        protocol: Some("completions".to_string()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("registry"),
        );
        let pool = Arc::new(ProviderTransportPool::new(1));
        let mut client = ProviderRuntimeClient::new_with_transport_pool(
            Arc::clone(&registry),
            Arc::clone(&pool),
            "qwen3.8-max".to_string(),
            Vec::new(),
        )
        .expect("client");

        let control_error = client
            .complete_control_analysis("qwen3.8-max", "system", "input", 32)
            .await
            .expect_err("quota response must fail");
        assert!(control_error.contains("quota has been exhausted"));
        fixture.await.expect("fixture task");
        assert!(pool.provider_account_is_fenced(registry.revision(), "token-plan"));

        let mut request = ApiRequest {
            bound_step: None,
            prompt: PromptAssembly::new(vec!["system".to_string()]),
            messages: Vec::new().into(),
            model: "qwen3.8-max".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "qwen3.8-max",
                32_768,
                4_096,
                0,
                0,
                0,
            ),
            preparation: Default::default(),
            provider_evidence_context: None,
        };
        request.budget.set_context_window_source("configured");
        let error = client
            .stream(request)
            .next()
            .await
            .expect("one fenced event")
            .expect_err("sibling stream must be fenced");
        assert_eq!(
            error.provider_failure_scope(),
            model_protocol::provider_failure::ProviderFailureScope::Account
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(pool.stats().in_flight, 0);
    }

    #[test]
    fn provider_entry_rejects_unconfigured_model_without_environment_fallback() {
        let registry = ProviderRegistry::empty();
        let snapshot = registry.pin();
        let pool = ProviderTransportPool::new(1);

        let error = match build_provider_entry(&snapshot, &pool, "claude-implicit") {
            Ok(_) => panic!("unconfigured models must not infer a provider from the environment"),
            Err(error) => error,
        };

        assert!(error.contains("not declared by any configured provider"));
        assert!(error.contains("does not infer provider credentials"));
    }

    #[test]
    fn tool_schema_cache_uses_model_visible_content_not_control_revision() {
        let providers = |base_url: &str| ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                ProviderConfig {
                    reasoning_effort: None,
                    name: "test".to_string(),
                    base_url: base_url.to_string(),
                    api_key: "test".to_string(),
                    models: vec!["primary".to_string()],
                    protocol: Some("completions".to_string()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        };
        let registry = Arc::new(ProviderRegistry::new(providers("https://one.test/v1")).unwrap());
        let mut client = ProviderRuntimeClient::new(
            Arc::clone(&registry),
            "primary".to_string(),
            vec![tool("read_file")],
        )
        .unwrap();
        client.configure_tool_exposure(exposure(&[], &["read_file"], 1));

        let first = client.compiled_tool_schema();
        let second = client.compiled_tool_schema();
        assert_eq!(first.inventory, second.inventory);
        assert_eq!(
            client.tool_schema_cache_stats(),
            super::ToolSchemaCacheStats {
                compilations: 1,
                cache_hits: 1,
            }
        );

        registry
            .replace(providers("https://two.test/v1"))
            .expect("valid provider reload");
        let recompiled = client.compiled_tool_schema();
        assert_eq!(recompiled.inventory.provider_registry_revision, 2);
        assert_eq!(
            client.tool_schema_cache_stats(),
            super::ToolSchemaCacheStats {
                compilations: 2,
                cache_hits: 1,
            }
        );

        client.configure_tool_exposure(exposure(&[], &["read_file"], 2));
        let exposure_recompiled = client.compiled_tool_schema();
        assert_eq!(exposure_recompiled.inventory.exposure_revision, 2);
        assert_eq!(client.tool_schema_cache_stats().compilations, 2);
        assert_eq!(client.tool_schema_cache_stats().cache_hits, 2);

        let mut delegated = ProviderRuntimeClient::new(
            Arc::clone(&registry),
            "primary".to_string(),
            vec![tool("read_file"), tool("edit")],
        )
        .unwrap()
        .with_stable_tool_schema(true);
        delegated.configure_tool_exposure(exposure(&[], &["read_file"], 1));
        let read_overlay = delegated.compiled_tool_schema();
        delegated.configure_tool_exposure(exposure(&[], &[], 2));
        let terminal_overlay = delegated.compiled_tool_schema();
        assert_eq!(read_overlay.inventory.tool_count, 2);
        assert_eq!(terminal_overlay.inventory.tool_count, 2);
        assert_eq!(
            read_overlay.inventory.schema_fingerprint,
            terminal_overlay.inventory.schema_fingerprint
        );
        assert_eq!(delegated.tool_schema_cache_stats().compilations, 1);
        assert_eq!(delegated.tool_schema_cache_stats().cache_hits, 1);
    }

    #[test]
    fn latest_deepseek_v4_reasoning_effort_is_normalized_without_none() {
        assert_eq!(
            request_reasoning_effort(
                "deepseek-v4-pro",
                &[],
                Some("none".to_string()),
                Some("medium".to_string()),
            ),
            Some("high".to_string())
        );
        assert_eq!(
            request_reasoning_effort(
                "deepseek/deepseek-v4-flash",
                &[],
                Some("max".to_string()),
                Some("high".to_string()),
            ),
            Some("max".to_string())
        );
    }

    #[test]
    fn configured_hybrid_model_honors_the_one_request_none_override() {
        assert_eq!(
            request_reasoning_effort(
                "configured-hybrid",
                &["openai_compat_enable_thinking".to_string()],
                Some("none".to_string()),
                Some("high".to_string()),
            ),
            Some("none".to_string())
        );
        assert_eq!(
            request_reasoning_effort(
                "ordinary-model",
                &[],
                Some("none".to_string()),
                Some("high".to_string()),
            ),
            Some("high".to_string())
        );
    }

    #[test]
    fn request_context_is_fresh_while_transport_is_reused() {
        let registry = ProviderRegistry::new(ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                ProviderConfig {
                    reasoning_effort: None,
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["primary".to_string()],
                    protocol: Some("completions".to_string()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        })
        .expect("registry");
        let snapshot = registry.pin();
        let pool = ProviderTransportPool::new(2);

        let first = build_provider_entry(&snapshot, &pool, "primary").expect("first");
        let second = build_provider_entry(&snapshot, &pool, "primary").expect("second");

        assert_ne!(
            first.request_context.request_id,
            second.request_context.request_id
        );
        assert_eq!(
            first.request_context.transport_fingerprint,
            second.request_context.transport_fingerprint
        );
        assert_eq!(pool.stats().builds, 1);
        assert_eq!(pool.stats().hits, 1);
    }

    #[test]
    fn host_template_cache_reuses_route_but_not_request_identity() {
        let registry = ProviderRegistry::new(ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                ProviderConfig {
                    reasoning_effort: None,
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["primary".to_string()],
                    protocol: Some("completions".to_string()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        })
        .expect("registry");
        let snapshot = registry.pin();
        let pool = ProviderTransportPool::new(2);
        let cache = super::ProviderClientTemplateCache::new(2);

        let first = cache
            .resolve(&snapshot, &pool, "primary")
            .expect("first route");
        let second = cache
            .resolve(&snapshot, &pool, "primary")
            .expect("cached route");

        assert_ne!(
            first.request_context.request_id,
            second.request_context.request_id
        );
        assert_eq!(cache.stats().builds, 1);
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().entries, 1);
    }

    #[tokio::test]
    async fn text_deltas_coalesce_while_bounded_queue_is_full() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(Ok(AssistantEvent::ProviderModel {
                identity: harness_contract::outcome::ProviderIdentity {
                    registry_revision: Some(1),
                    provider_name: "test".to_string(),
                    model: "test".to_string(),
                    profile: None,
                    protocol: Some("completions".to_string()),
                    capabilities: std::collections::BTreeMap::new(),
                },
            }))
            .await
            .unwrap();
        let mut pending = String::new();
        let mut emitted = false;

        assert!(
            forward_text_delta(
                &sender,
                "a".to_string(),
                &mut pending,
                false,
                &None,
                &mut emitted
            )
            .await
        );
        assert!(
            forward_text_delta(
                &sender,
                "b".to_string(),
                &mut pending,
                false,
                &None,
                &mut emitted
            )
            .await
        );
        assert_eq!(pending, "ab");
        let _ = receiver.recv().await;
        assert!(super::flush_pending_text(&sender, &mut pending, false, &None, &mut emitted).await);
        assert!(pending.is_empty());
        assert!(matches!(
            receiver.recv().await,
            Some(Ok(AssistantEvent::TextDelta(text))) if text == "ab"
        ));
    }
}
