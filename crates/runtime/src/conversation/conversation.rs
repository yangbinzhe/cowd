use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use fact_kernel::FactExtractionTokenUsage;
use tokio::sync::RwLock;

#[path = "context_plane.rs"]
mod context_plane;
#[path = "evidence_terminal_plane.rs"]
mod evidence_terminal_plane;
#[path = "provider_plane.rs"]
mod provider_plane;
use provider_plane::TurnProviderState;
#[path = "tool_plane.rs"]
mod tool_plane;
#[path = "turn_engine.rs"]
mod turn_engine;

/// T35: Lightweight cancellation token (tokio-util not available in dep tree).
#[derive(Default, Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

#[derive(Clone, Default, Debug)]
pub struct CancellationToken(Arc<CancellationState>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(CancellationState::default()))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        if !self.0.cancelled.swap(true, Ordering::SeqCst) {
            self.0.notify.notify_waiters();
        }
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.0.notify.notified();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

use futures::stream::Stream;
use harness_contract::{
    context::{
        ArtifactWriteDescriptor, CompactionReceipt, ContextGovernanceDecision,
        ContextPressureState, ContextTurnReport, EvidenceAccessRef, EvidenceAuditProjection,
        EvidenceRef, StablePrefixMetrics, ToolExposureMetrics, ToolObservation,
    },
    knowledge::KnowledgeTurnReport,
    skill::{AgentSkillProfile, SkillCapabilityProfile},
    strategy::{
        understand, ExecutionCandidateKind, StrategyCandidateCostSummary, StrategyExperienceRecord,
        StrategyExperienceSummary, StrategyInput, StrategyWorkloadFingerprint,
    },
    turn::{
        SessionInputEnvelope, SessionInputProjection, SessionInputReceipt, TurnId,
        TurnInboxSnapshot, TurnInputCheckpoint,
    },
};
use memory::cognitive::CognitiveContextManager;
use memory::compression::session::{
    CompactionSourceRange, SessionCheckpointBuildContext, SessionCompactor,
    SessionSemanticCheckpoint,
};
use memory::config::MemoryConfig as CcMemoryConfig;
use memory::types::{Message as MemMessage, MessageRole as MemMessageRole};

const MAX_RUNTIME_PROVIDER_RETRIES_PER_MODEL: u8 = 1;
const DEFAULT_RUNTIME_PROVIDER_RETRY_DELAY: Duration = Duration::from_millis(250);
pub(crate) const MAX_EVALUATION_PROVIDER_TOKEN_LEASE: u64 = 20_000_000;
use memory::{MemoryKernel, MemoryTurnContext};
use model_protocol::telemetry::SessionTracer;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::budget_policy::{
    clamp_context_budget_ratio_bp, ProviderOutputBudget, ProviderOutputBudgetInputs,
    RuntimeBudgetInputs, RuntimeBudgetPlan,
};

const fn provider_retry_is_fenced(
    prior_effect_receipt_observed: bool,
    current_effect_receipts: usize,
) -> bool {
    prior_effect_receipt_observed || current_effect_receipts > 0
}

#[derive(Debug)]
struct EvaluationProviderTokenLeaseState {
    lease_id: String,
    limit: u64,
    remaining: u64,
    input_consumed: u64,
    output_consumed: u64,
    cached_consumed: u64,
    outstanding: usize,
    breached: bool,
}

#[derive(Debug)]
pub(crate) struct EvaluationProviderTokenLease {
    state: std::sync::Mutex<EvaluationProviderTokenLeaseState>,
}

impl EvaluationProviderTokenLease {
    fn new(lease_id: &str, limit: u64) -> Result<Self, RuntimeError> {
        if lease_id.trim().is_empty() || limit == 0 || limit > MAX_EVALUATION_PROVIDER_TOKEN_LEASE {
            return Err(RuntimeError::new(
                "evaluation provider token lease identity/limit is invalid",
            ));
        }
        Ok(Self {
            state: std::sync::Mutex::new(EvaluationProviderTokenLeaseState {
                lease_id: lease_id.to_string(),
                limit,
                remaining: limit,
                input_consumed: 0,
                output_consumed: 0,
                cached_consumed: 0,
                outstanding: 0,
                breached: false,
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> Result<EvaluationProviderTokenLeaseSnapshot, RuntimeError> {
        let lease = self
            .state
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token lease lock poisoned"))?;
        Ok(EvaluationProviderTokenLeaseSnapshot {
            lease_id: lease.lease_id.clone(),
            limit: lease.limit,
            consumed: lease.limit.saturating_sub(lease.remaining),
            input_consumed: lease.input_consumed,
            output_consumed: lease.output_consumed,
            cached_consumed: lease.cached_consumed,
            outstanding: lease.outstanding,
            breached: lease.breached,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct EvaluationProviderTokenLeaseRegistry {
    leases: std::sync::Mutex<BTreeMap<String, Arc<EvaluationProviderTokenLease>>>,
}

impl EvaluationProviderTokenLeaseRegistry {
    pub(crate) fn install(
        self: &Arc<Self>,
        session_id: &str,
        lease_id: &str,
        limit: u64,
    ) -> Result<EvaluationProviderTokenLeaseGuard, RuntimeError> {
        if session_id.trim().is_empty() {
            return Err(RuntimeError::new(
                "evaluation provider token lease session identity is invalid",
            ));
        }
        let lease = Arc::new(EvaluationProviderTokenLease::new(lease_id, limit)?);
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token registry lock poisoned"))?;
        if leases.contains_key(session_id) {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease already exists for session `{session_id}`"
            )));
        }
        leases.insert(session_id.to_string(), Arc::clone(&lease));
        Ok(EvaluationProviderTokenLeaseGuard {
            session_id: session_id.to_string(),
            registry: Arc::clone(self),
            lease,
        })
    }

    pub(crate) fn get(
        &self,
        session_id: &str,
    ) -> Result<Option<Arc<EvaluationProviderTokenLease>>, RuntimeError> {
        self.leases
            .lock()
            .map(|leases| leases.get(session_id).cloned())
            .map_err(|_| RuntimeError::new("evaluation provider token registry lock poisoned"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvaluationProviderTokenLeaseSnapshot {
    pub lease_id: String,
    pub limit: u64,
    pub consumed: u64,
    pub input_consumed: u64,
    pub output_consumed: u64,
    pub cached_consumed: u64,
    pub outstanding: usize,
    pub breached: bool,
}

pub(crate) struct EvaluationProviderTokenLeaseGuard {
    session_id: String,
    registry: Arc<EvaluationProviderTokenLeaseRegistry>,
    lease: Arc<EvaluationProviderTokenLease>,
}

impl Drop for EvaluationProviderTokenLeaseGuard {
    fn drop(&mut self) {
        let Ok(mut leases) = self.registry.leases.lock() else {
            return;
        };
        if leases
            .get(&self.session_id)
            .is_some_and(|current| Arc::ptr_eq(current, &self.lease))
        {
            leases.remove(&self.session_id);
        }
    }
}

impl EvaluationProviderTokenLeaseGuard {
    pub(crate) fn lease(&self) -> Arc<EvaluationProviderTokenLease> {
        Arc::clone(&self.lease)
    }

    pub(crate) fn snapshot(&self) -> Result<EvaluationProviderTokenLeaseSnapshot, RuntimeError> {
        self.lease.snapshot()
    }
}

struct EvaluationProviderTokenReservation {
    lease: Arc<EvaluationProviderTokenLease>,
    reserved: u64,
    reconciled: bool,
    dispatched: bool,
}

impl EvaluationProviderTokenReservation {
    fn acquire(
        lease: Option<&Arc<EvaluationProviderTokenLease>>,
        request: &mut ApiRequest,
    ) -> Result<Option<Self>, RuntimeError> {
        let Some(lease) = lease else {
            return Ok(None);
        };
        let mut state = lease
            .state
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token lease lock poisoned"))?;
        if state.breached {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` is already breached",
                state.lease_id
            )));
        }
        // Reserve a conservative upper bound before touching the provider.
        // Input estimation, protocol framing and the normal request safety
        // margin are all charged; the remainder becomes the provider-enforced
        // maximum output. The Session-scoped Arc is shared by Team children
        // and their parent without charging unrelated conversations.
        let input_reserve = request
            .budget
            .input_total_tokens()
            .saturating_add(request.budget.protocol_overhead_tokens)
            .saturating_add(request.budget.safety_margin_tokens);
        if input_reserve >= state.remaining {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` has {} tokens remaining but request input reserves {}",
                state.lease_id, state.remaining, input_reserve
            )));
        }
        let output_reserve = request
            .budget
            .requested_output_tokens
            .min(state.remaining.saturating_sub(input_reserve));
        if output_reserve == 0 {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` has no output capacity",
                state.lease_id
            )));
        }
        request.budget.requested_output_tokens = output_reserve;
        let reserved = input_reserve.saturating_add(output_reserve);
        state.remaining = state.remaining.saturating_sub(reserved);
        state.outstanding = state.outstanding.saturating_add(1);
        drop(state);
        Ok(Some(Self {
            lease: Arc::clone(lease),
            reserved,
            reconciled: false,
            dispatched: false,
        }))
    }

    fn mark_dispatched(&mut self) {
        self.dispatched = true;
    }

    fn reconcile(&mut self, usage: TokenUsage) {
        if self.reconciled {
            return;
        }
        let input = u64::from(usage.input_tokens);
        let output = u64::from(usage.output_tokens);
        let cached = u64::from(usage.cache_creation_input_tokens)
            .saturating_add(u64::from(usage.cache_read_input_tokens));
        let actual = input.saturating_add(output).saturating_add(cached);
        if actual == 0 {
            // Missing provider usage is not permission to refund a hard
            // reservation. Drop will close the outstanding request while
            // retaining the conservative charge.
            return;
        }
        if let Ok(mut lease) = self.lease.state.lock() {
            lease.input_consumed = lease.input_consumed.saturating_add(input);
            lease.output_consumed = lease.output_consumed.saturating_add(output);
            lease.cached_consumed = lease.cached_consumed.saturating_add(cached);
            if actual <= self.reserved {
                lease.remaining = lease
                    .remaining
                    .saturating_add(self.reserved.saturating_sub(actual))
                    .min(lease.limit);
            } else {
                // Provider tokenizers are authoritative while the preflight
                // budget is necessarily an estimate. A measured variance is
                // not a global lease breach when still-unreserved Session
                // headroom can pay it. Outstanding concurrent reservations
                // have already been removed from `remaining`, so this charge
                // cannot steal their capacity or exceed the hard lease.
                let unreserved_delta = actual.saturating_sub(self.reserved);
                if unreserved_delta <= lease.remaining {
                    lease.remaining = lease.remaining.saturating_sub(unreserved_delta);
                } else {
                    lease.breached = true;
                    lease.remaining = 0;
                }
            }
            lease.outstanding = lease.outstanding.saturating_sub(1);
            self.reconciled = true;
        }
    }
}

impl Drop for EvaluationProviderTokenReservation {
    fn drop(&mut self) {
        if self.reconciled {
            return;
        }
        if let Ok(mut lease) = self.lease.state.lock() {
            if !self.dispatched {
                lease.remaining = lease
                    .remaining
                    .saturating_add(self.reserved)
                    .min(lease.limit);
            }
            lease.outstanding = lease.outstanding.saturating_sub(1);
        }
    }
}

/// One pre-dispatch reservation transaction across the explicitly bound
/// evaluation budget and the delegated child budget. If either admission
/// fails, every reservation already acquired by this function is dropped
/// before the caller can reach Provider IO.
struct ProviderTokenReservationSet {
    evaluation: Option<EvaluationProviderTokenReservation>,
    delegated: Option<crate::execution_core::budget::DurableProviderBudgetReservation>,
}

impl ProviderTokenReservationSet {
    fn acquire(
        evaluation_lease: Option<&Arc<EvaluationProviderTokenLease>>,
        delegated_budget: Option<&(
            crate::execution_core::budget::ParentExecutionBudgetLedger,
            harness_contract::context::ChildExecutionBudgetReservation,
        )>,
        model: &str,
        request: &mut ApiRequest,
    ) -> Result<Self, RuntimeError> {
        let evaluation = EvaluationProviderTokenReservation::acquire(evaluation_lease, request)?;
        let delegated = delegated_budget
            .map(|(ledger, child)| {
                let input_reserve = request
                    .budget
                    .input_total_tokens()
                    .saturating_add(request.budget.protocol_overhead_tokens)
                    .saturating_add(request.budget.safety_margin_tokens);
                ledger
                    .reserve_provider(
                        child,
                        format!("provider-budget:{}", uuid::Uuid::new_v4()),
                        model,
                        input_reserve,
                        request.budget.requested_output_tokens,
                    )
                    .map_err(RuntimeError::new)
            })
            .transpose()?;
        if let Some(reservation) = delegated.as_ref() {
            request.budget.requested_output_tokens = reservation.granted_output_tokens;
        }
        Ok(Self {
            evaluation,
            delegated,
        })
    }

    fn mark_dispatched(&mut self) {
        if let Some(reservation) = self.evaluation.as_mut() {
            reservation.mark_dispatched();
        }
        // A durable delegated reservation is intentionally not rolled back
        // after this point: crash/unknown usage conservatively consumes it.
    }

    fn reconcile(&mut self, usage: TokenUsage) -> Result<(), RuntimeError> {
        if let Some(reservation) = self.evaluation.as_mut() {
            reservation.reconcile(usage);
        }
        if let Some(reservation) = self.delegated.as_mut() {
            reservation.reconcile(usage).map_err(RuntimeError::new)?;
        }
        Ok(())
    }
}
use crate::compact::{
    apply_compaction_summary, estimate_session_tokens, plan_session_compaction, CompactionConfig,
};
use crate::config::{RuntimeFeatureConfig, SessionCompactConfig as RuntimeSessionCompactConfig};
use crate::context_runtime::{
    ContextAuthority, ContextEnvelope, ContextEnvelopeRequest, ContextIdentity, ContextItem,
    ContextOmission, ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind,
    ContextVisibility, PersistedContextEnvelope, ResumeContextPacket, RuntimeContextFactDecision,
    RuntimeContextGovernanceReport, ToolTracePacket, ToolTraceStatus,
    CONTEXT_RENDER_FORMATTER_VERSION, PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
};
use crate::context_tool_exposure::{ToolExposurePlanner, ToolExposurePolicy, ToolExposureState};
use crate::fact_extraction::{
    FactExtractionRuntimeEvent, RuleFactExtractor, RuntimeFactExtractionInput,
    RuntimeFactExtractionPolicy, RuntimeFactExtractionScheduler, RuntimeFactExtractionTrigger,
    RuntimeFactExtractor,
};
use crate::governed_tool_executor::{
    GovernedToolAdmission, GovernedToolExecutionContext, GovernedToolExecutor, GovernedToolFuture,
    GovernedToolTaskTerminal,
};
use crate::governed_tool_plan::{
    GovernedToolCompiler, GovernedToolPlan, GovernedToolPolicyValidationReport,
};
use crate::hooks::{HookAbortSignal, HookProgressReporter, HookRunResult, HookRunner};
use crate::knowledge_activation::KnowledgeActivationRuntime;
use crate::permissions::{PermissionContext, PermissionPolicy};
use crate::runtime_control::RuntimeControlPolicy;
use crate::runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};
use crate::session::{ContentBlock, ConversationMessage, MessageEvent, Session, SessionEventLog};
use crate::skill::{
    memory_candidate_from_skill_activation, skill_memory_candidate_session_event,
    RuntimeSkillPromptAsset, SkillActivationEngine, SkillActivationInput, SkillMemoryPolicy,
};
use crate::tool_invocation::{
    now_ms, ToolFailureKind, ToolInvocationRecord, DEFAULT_OUTPUT_REF_MIN_LINES,
};
use crate::usage::UsageTracker;
use crate::PromptAssembly;
use crate::{
    HistoryView, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
};
use model_protocol::usage::TokenUsage;

fn provider_output_budget_hint(
    model: &str,
    context_window: u32,
    max_output_override: Option<u32>,
) -> u32 {
    let max_output = provider::model_max_output_resolution(model, max_output_override);
    let budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
        context_window_tokens: u64::from(context_window),
        max_output_tokens: u64::from(max_output.tokens),
        fixed_input_tokens: 0,
        required_input_tokens: 0,
        protocol_overhead_tokens: 0,
        safety_margin_tokens: 0,
    });
    u32::try_from(budget.requested_output_tokens).unwrap_or(u32::MAX)
}

fn provider_transport_policy(
    context_window: u32,
    request: &ApiRequest,
) -> crate::ProviderTransportPolicy {
    let prompt_chars = request.prompt.estimated_chars();
    let message_chars = request
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .map(|block| match block {
            ContentBlock::Text { text } => text.chars().count(),
            ContentBlock::ToolResult { output, .. } => output.chars().count(),
            _ => 0,
        })
        .sum::<usize>();
    crate::ProviderTransportPolicy::derive(
        context_window,
        prompt_chars.saturating_add(message_chars),
    )
}

fn retrieve_tool_evidence_from_sandbox(
    sandbox: Option<&Arc<std::sync::Mutex<memory::ToolOutputSandbox>>>,
    input: &str,
) -> Result<String, String> {
    let request: serde_json::Value = serde_json::from_str(input)
        .map_err(|error| format!("invalid evidence retrieval input: {error}"))?;
    let evidence_ref = request
        .get("evidence_ref")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "evidence_ref is required".to_string())?;
    let evidence_id = evidence_ref.strip_prefix("tool://").unwrap_or(evidence_ref);
    let query = request
        .get("query")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let limit = request
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(4)
        .clamp(1, 16) as usize;
    let sandbox = sandbox.ok_or_else(|| "tool evidence sandbox is unavailable".to_string())?;
    let sandbox = sandbox
        .lock()
        .map_err(|_| "tool evidence sandbox lock poisoned".to_string())?;
    let mut snippets = if query.is_empty() {
        sandbox.read(evidence_id, limit)
    } else {
        sandbox.search(evidence_id, query, limit)
    };
    if snippets.is_empty() && !query.is_empty() {
        let normalized_query = query
            .split(|character: char| !character.is_alphanumeric())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if normalized_query != query && !normalized_query.is_empty() {
            snippets = sandbox.search(evidence_id, &normalized_query, limit);
        }
    }
    if snippets.is_empty() {
        return Err(format!(
            "no indexed evidence matched `{evidence_ref}`; use the session evidence API for the immutable raw payload"
        ));
    }
    serde_json::to_string_pretty(
        &snippets
            .iter()
            .map(|snippet| {
                serde_json::json!({
                    "range_start": snippet.line_start,
                    "range_end": snippet.line_end,
                    "content": snippet.content,
                })
            })
            .collect::<Vec<_>>(),
    )
    .map_err(|error| format!("evidence retrieval serialization failed: {error}"))
}

fn classify_model_step_intent(text: String, calls: Vec<ModelToolCall>) -> ModelStepIntent {
    if calls.is_empty() {
        ModelStepIntent::FinalAnswer { text }
    } else {
        // Function identity is owned by the exposed tool catalog and the
        // tool's typed input contract. Names such as `team_board` or
        // `permission_status` are ordinary tools; they must never create an
        // Agent, Team, approval, or replan merely because of a substring.
        // Stateful orchestration remains the responsibility of Runtime's
        // validated native control contracts.
        ModelStepIntent::ToolCalls { calls }
    }
}

fn unexposed_model_tool_names(
    calls: &[ModelToolCall],
    exposed_tool_ids: &BTreeSet<String>,
) -> Vec<String> {
    calls
        .iter()
        .filter(|call| !exposed_tool_ids.contains(&call.name))
        .map(|call| call.name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalized_tool_identity(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn canonicalize_model_tool_names<T: ToolExecutor>(calls: &mut [ModelToolCall], tool_executor: &T) {
    for call in calls {
        if let Some(canonical) = tool_executor.resolve_tool_name(&call.name) {
            if canonical == call.name {
                continue;
            }
            tracing::info!(
                provider_tool_name = %call.name,
                canonical_tool_name = %canonical,
                "resolved provider tool alias through the authoritative tool catalog"
            );
            call.name = canonical;
        }
    }
}

/// An explicit user requirement to actually form a team is an acceptance
/// constraint, not merely a prose preference. It takes precedence over a
/// heuristic strategy recommendation: otherwise a correctly parsed user
/// requirement can disappear just because the lightweight classifier chose a
/// different execution pattern. Generic complex work remains model-directed
/// and is never forced through this path.
fn enforce_explicit_team_requirement(
    _objective: &str,
    first_step: bool,
    decision: &crate::execution_core::RuntimeExecutionDecision,
    intent: ModelStepIntent,
) -> ModelStepIntent {
    let Some(obligation) = decision.collaboration_obligation.as_ref() else {
        return intent;
    };
    if !first_step {
        return intent;
    }

    tracing::info!(
        explicit_team = true,
        obligation_source = ?obligation.source,
        minimum_team_count = obligation.minimum_team_count,
        intent_kind = ?match &intent {
            ModelStepIntent::ToolCalls { .. } => "tool_calls",
            ModelStepIntent::FinalAnswer { .. } => "final_answer",
            ModelStepIntent::Replan { .. } => "replan",
        },
        "explicit team requirement enforcing orchestration call"
    );

    // The first move belongs entirely to the model. Team/role names are
    // display metadata authored by the model, never a hardcoded Runtime
    // decision; Runtime never substitutes a builtin Team here. If the model
    // finishes without a verified Team execution, the parent final-answer
    // acceptance gate re-prompts it to orchestrate (see host.rs), and only
    // after bounded attempts reports an honest partial result.
    intent
}

fn apply_explicit_team_requirement(
    enabled: bool,
    objective: &str,
    first_step: bool,
    decision: &crate::execution_core::RuntimeExecutionDecision,
    intent: ModelStepIntent,
) -> ModelStepIntent {
    if enabled {
        enforce_explicit_team_requirement(objective, first_step, decision, intent)
    } else {
        intent
    }
}

fn is_runtime_team_orchestration_call(call: &ModelToolCall) -> bool {
    if call.name.eq_ignore_ascii_case(
        harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
    ) {
        return serde_json::from_str::<
            harness_contract::orchestration::ModelCollaborationControlDecisionV2,
        >(&call.input)
        .ok()
        .is_some_and(|decision| !decision.workstreams.is_empty());
    }
    if !is_runtime_team_orchestration_call_name(&call.name) {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(&call.input)
        .ok()
        .is_some_and(|input| {
            input.get("operation").and_then(serde_json::Value::as_str) == Some("propose")
                && input
                    .pointer("/proposal/nodes")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|nodes| {
                        nodes.iter().any(|node| {
                            node.get("recipe").and_then(serde_json::Value::as_str) == Some("team")
                        })
                    })
        })
}

#[cfg(test)]
fn runtime_team_orchestration_count(call: &ModelToolCall) -> usize {
    if !is_runtime_team_orchestration_call_name(&call.name) {
        return 0;
    }
    serde_json::from_str::<serde_json::Value>(&call.input)
        .ok()
        .and_then(|input| {
            input
                .pointer("/proposal/nodes")
                .and_then(serde_json::Value::as_array)
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter(|node| {
                            node.get("recipe").and_then(serde_json::Value::as_str) == Some("team")
                        })
                        .map(|node| {
                            node.get("multiplicity")
                                .and_then(serde_json::Value::as_u64)
                                .and_then(|value| usize::try_from(value).ok())
                                .unwrap_or(1)
                        })
                        .sum()
                })
        })
        .unwrap_or_default()
}

fn is_runtime_team_orchestration_call_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("runtime_orchestrate")
        || name.eq_ignore_ascii_case(
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID,
        )
}

#[cfg(test)]
pub(crate) fn required_team_orchestration_call_with_understanding(
    objective: &str,
    understanding: &harness_contract::strategy::TaskUnderstanding,
) -> ModelToolCall {
    let requires_external_facts = understanding.requires_external_facts;
    let requires_write = understanding.requires_write;
    let team_owns_write = requires_write
        && harness_contract::strategy::explicit_team_owns_persisted_artifact(objective);
    let team_count = usize::from(understanding.required_team_count.max(1));
    let node_ids = (0..team_count)
        .map(|index| {
            if team_count == 1 {
                "explicit-team".to_string()
            } else {
                format!("explicit-team-{}", index + 1)
            }
        })
        .collect::<Vec<_>>();
    let nodes = node_ids
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            // A compound request such as "one Team researches, another Team
            // writes the report" is not N copies of one template. Research
            // Teams may run independently, while the final writer consumes
            // every preceding result and owns the workspace artifact.
            let is_followup_writer = team_owns_write && index + 1 == team_count;
            let contract = crate::orchestration::team_authority::explicit_team_node_contract(
                index,
                team_count,
                team_owns_write,
                requires_external_facts,
            );
            let node_requires_write = is_followup_writer;
            serde_json::json!({
                "node_id": node_id,
                "recipe": "team",
                "objective": objective,
                "depends_on": if node_requires_write && index > 0 {
                    node_ids[..index].to_vec()
                } else {
                    Vec::<String>::new()
                },
                "template": contract.template,
                "output_artifacts": contract.output_artifacts,
                "evidence_contract": contract.evidence_contract,
                "required": true
            })
        })
        .collect::<Vec<_>>();
    let mut mutation_digest = Sha256::new();
    mutation_digest.update(b"cowd:explicit-team:v1\0");
    mutation_digest.update(objective.trim().as_bytes());
    mutation_digest.update(b"\0");
    mutation_digest.update(team_count.to_string().as_bytes());
    mutation_digest.update([u8::from(team_owns_write)]);
    mutation_digest.update([u8::from(requires_external_facts)]);
    let mutation_digest = mutation_digest.finalize();
    let mutation_id = format!("explicit-team-{mutation_digest:x}");
    ModelToolCall {
        id: "runtime-required-team".to_string(),
        name: "runtime_orchestrate".to_string(),
        input: serde_json::json!({
            "intent": objective,
            "operation": "propose",
            "proposal": {
                "mutation_id": mutation_id,
                "reason": "the user explicitly requires an actually started collaboration team",
                "nodes": nodes,
                "completion": {
                    "required_node_ids": node_ids,
                    "required_artifact_kinds": if team_owns_write {
                        serde_json::json!(["workspace_change", "terminal_synthesis"])
                    } else {
                        serde_json::json!(["terminal_synthesis"])
                    },
                    "allow_unresolved_conflicts": false
                }
            },
            "constraints": {
                "risk": "low",
                "requires_write": team_owns_write,
                "surface_latency_sensitive": false,
            }
        })
        .to_string(),
        depends_on: Vec::new(),
    }
}

#[cfg(test)]
fn required_team_orchestration_call(objective: &str) -> ModelToolCall {
    let understanding = understand(&StrategyInput::from_prompt(objective));
    required_team_orchestration_call_with_understanding(objective, &understanding)
}

/// Fully assembled request payload sent to the upstream model client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRequest {
    pub prompt: PromptAssembly,
    pub messages: HistoryView,
    /// Runtime-selected primary model ID.
    pub model: String,
    /// Runtime-owned one-shot reasoning policy for this provider attempt.
    /// The transport adapter decides whether the selected model supports the
    /// requested effort; unsupported backends retain their configured policy.
    pub reasoning_effort_override: Option<String>,
    /// Whether Runtime reused a previously compiled immutable request basis.
    /// This is diagnostic metadata only and is never serialized to Provider.
    pub request_compiler_cache_hit: bool,
    /// Request-local capacity contract used for diagnostics and ledger
    /// reconciliation. Provider must not mutate routing or budget ownership.
    pub budget: crate::context_ledger::RequestBudgetReport,
    /// Request-local durable evidence coordinates. The Provider adapter uses
    /// this only after producing its exact protocol body and before network IO.
    pub provider_evidence_context: Option<crate::ProviderRequestEvidenceContext>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderContextInventory {
    pub tool_count: usize,
    pub tool_schema_tokens: u64,
    pub catalog_revision: u64,
    pub exposure_revision: u64,
    pub schema_fingerprint: u64,
    pub provider_registry_revision: u64,
}

/// Streamed events emitted while processing a single assistant turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssistantItemKind {
    Text,
    PublicReasoning,
    PrivateReasoning,
    ToolCall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssistantEvent {
    /// The provider/model that actually accepted this request. This is emitted
    /// only after the provider has produced a protocol event, so it is never
    /// mistaken for a configured fallback that was merely considered.
    ProviderModel {
        identity: harness_contract::outcome::ProviderIdentity,
    },
    ItemStarted {
        index: u32,
        provider_item_id: Option<String>,
        kind: AssistantItemKind,
    },
    ItemCompleted {
        index: u32,
    },
    TextDelta(String),
    /// Provider-approved reasoning summary safe for public projection.
    ReasoningSummaryDelta(String),
    /// Provider-private reasoning retained only for protocol round-trip. It
    /// must never be projected as a public reasoning summary.
    PrivateReasoningDelta(String),
    /// P1-7: Thinking signature that must be preserved and passed back
    /// to the provider in subsequent requests.
    SignatureDelta(String),
    ToolUse {
        id: String,
        name: String,
        input: String,
    },
    Usage(TokenUsage),
    MessageStop,
    /// P0-2: Tool execution lifecycle events for real-time SSE visualization
    ToolStart {
        id: String,
        name: String,
        preview: String,
    },
    ToolProgress {
        id: String,
        name: String,
        progress: String,
    },
    ToolComplete {
        id: String,
        name: String,
        result_summary: String,
        exit_code: Option<i32>,
    },
}

#[derive(Debug)]
struct ModelStreamItemState {
    identity: crate::CausalItemIdentity,
    kind: AssistantItemKind,
    content: String,
    completed: bool,
}

struct ModelStreamReducer {
    bus: Option<Arc<crate::CowdEventBus>>,
    event_store: Option<Arc<RuntimeEventStore>>,
    session_id: String,
    model_step_id: String,
    items: BTreeMap<u32, ModelStreamItemState>,
    active_text: Option<u32>,
    active_public_reasoning: Option<u32>,
    active_private_reasoning: Option<u32>,
    synthetic_index: u32,
    text: String,
    public_reasoning: String,
    private_reasoning: String,
    signature: String,
    calls: Vec<ModelToolCall>,
    usage: TokenUsage,
    effective_provider_identity: Option<harness_contract::outcome::ProviderIdentity>,
    first_event_at: Option<Instant>,
    first_text_at: Option<Instant>,
    terminal_presentation: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub(crate) struct EarlyToolCandidate {
    pub call: ModelToolCall,
    pub identity: crate::CausalItemIdentity,
    pub ready_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EarlyToolExecutionReceipt {
    pub call: ModelToolCall,
    pub outcome: crate::RuntimeToolExecutionOutcome,
    pub ready_at_ms: u64,
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EarlyToolDeferral {
    pub tool_call_id: String,
    pub reason: String,
    pub ready_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EarlyToolDispatchResult {
    Executed(EarlyToolExecutionReceipt),
    Deferred(EarlyToolDeferral),
}

pub(crate) type EarlyToolDispatchFuture =
    Pin<Box<dyn Future<Output = EarlyToolDispatchResult> + Send + 'static>>;

pub(crate) trait EarlyToolDispatcher: Send + Sync {
    fn dispatch(&self, candidate: EarlyToolCandidate) -> EarlyToolDispatchFuture;
}

/// Append-only model-step plan used while Provider items are still arriving.
///
/// It does not compile or execute an open DAG. It only freezes every explicitly
/// completed Tool call once, rejects identity reuse with changed arguments, and
/// lets the finalized batch compiler remain the sole DAG authority at seal.
#[derive(Debug, Default)]
struct ModelStepToolPlan {
    calls: BTreeMap<String, ModelToolCall>,
    order: Vec<String>,
    sealed: bool,
}

impl ModelStepToolPlan {
    fn append(
        &mut self,
        candidate: EarlyToolCandidate,
    ) -> Result<Option<EarlyToolCandidate>, String> {
        if self.sealed {
            return Err("model step tool plan is already sealed".to_string());
        }
        if let Some(existing) = self.calls.get(&candidate.call.id) {
            if existing == &candidate.call {
                return Ok(None);
            }
            return Err(format!(
                "provider reused tool call id `{}` with changed name, arguments, or dependencies",
                candidate.call.id
            ));
        }
        self.order.push(candidate.call.id.clone());
        self.calls
            .insert(candidate.call.id.clone(), candidate.call.clone());
        Ok(Some(candidate))
    }

    fn seal(&mut self, finalized_calls: &[ModelToolCall]) -> Result<(), String> {
        if self.sealed {
            return Err("model step tool plan was sealed more than once".to_string());
        }
        self.sealed = true;

        let mut finalized = BTreeMap::new();
        for call in finalized_calls {
            if finalized.insert(call.id.clone(), call).is_some() {
                return Err(format!(
                    "provider finalized duplicate tool call id `{}`",
                    call.id
                ));
            }
        }
        for call_id in &self.order {
            let appended = self
                .calls
                .get(call_id)
                .expect("model step append order references a missing call");
            let Some(sealed) = finalized.get(call_id) else {
                return Err(format!(
                    "completed tool call `{call_id}` disappeared before model step seal"
                ));
            };
            if appended != *sealed {
                return Err(format!(
                    "completed tool call `{call_id}` changed before model step seal"
                ));
            }
        }
        Ok(())
    }
}

impl ModelStreamReducer {
    fn new(
        bus: Option<Arc<crate::CowdEventBus>>,
        event_store: Option<Arc<RuntimeEventStore>>,
        session_id: String,
    ) -> Self {
        let model_step_id = bus.as_ref().map_or_else(
            || format!("{session_id}:model-step:unscoped"),
            |bus| bus.next_model_step_id(),
        );
        if let Some(bus) = &bus {
            bus.emit(crate::CowdEvent::ModelStepStarted {
                model_step_id: model_step_id.clone(),
            });
        }
        Self {
            bus,
            event_store,
            session_id,
            model_step_id,
            items: BTreeMap::new(),
            active_text: None,
            active_public_reasoning: None,
            active_private_reasoning: None,
            synthetic_index: u32::MAX,
            text: String::new(),
            public_reasoning: String::new(),
            private_reasoning: String::new(),
            signature: String::new(),
            calls: Vec::new(),
            usage: TokenUsage::default(),
            effective_provider_identity: None,
            first_event_at: None,
            first_text_at: None,
            terminal_presentation: None,
        }
    }

    fn with_terminal_presentation(
        mut self,
        presentation_id: impl Into<String>,
        attempt_id: impl Into<String>,
    ) -> Self {
        self.terminal_presentation = Some((presentation_id.into(), attempt_id.into()));
        self
    }

    fn next_synthetic_index(&mut self) -> u32 {
        let index = self.synthetic_index;
        self.synthetic_index = self.synthetic_index.saturating_sub(1);
        index
    }

    fn item_identity(
        &self,
        index: u32,
        provider_item_id: Option<&str>,
        kind: AssistantItemKind,
    ) -> crate::CausalItemIdentity {
        let item_id = provider_item_id
            .filter(|id| !id.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("{}:item:{index}", self.model_step_id));
        let segment_kind = match kind {
            AssistantItemKind::Text => "text",
            AssistantItemKind::PublicReasoning => "reasoning-summary",
            AssistantItemKind::PrivateReasoning => "private-reasoning",
            AssistantItemKind::ToolCall => "tool-call",
        };
        crate::CausalItemIdentity {
            model_step_id: self.model_step_id.clone(),
            item_id: item_id.clone(),
            segment_id: format!("{item_id}:{segment_kind}:0"),
            causal_sequence: self
                .bus
                .as_ref()
                .map_or(0, |bus| bus.next_causal_sequence()),
            delta_sequence: 0,
            tool_call_id: (kind == AssistantItemKind::ToolCall).then_some(item_id),
            causal_parent_ids: Vec::new(),
        }
    }

    fn start_item(&mut self, index: u32, provider_item_id: Option<&str>, kind: AssistantItemKind) {
        if self.items.contains_key(&index) {
            return;
        }
        let identity = self.item_identity(index, provider_item_id, kind);
        if kind != AssistantItemKind::PrivateReasoning {
            if let (Some(bus), Some(public_kind)) = (&self.bus, public_causal_item_kind(kind)) {
                let binding = reasoning_activity_binding(bus, &identity, kind);
                bus.emit_causal_with_activity_binding(
                    identity.clone(),
                    crate::CowdEvent::ItemStarted { kind: public_kind },
                    binding,
                );
            }
        }
        match kind {
            AssistantItemKind::Text => self.active_text = Some(index),
            AssistantItemKind::PublicReasoning => self.active_public_reasoning = Some(index),
            AssistantItemKind::PrivateReasoning => self.active_private_reasoning = Some(index),
            AssistantItemKind::ToolCall => {}
        }
        self.items.insert(
            index,
            ModelStreamItemState {
                identity,
                kind,
                content: String::new(),
                completed: false,
            },
        );
    }

    fn ensure_active(&mut self, kind: AssistantItemKind) -> u32 {
        let active = match kind {
            AssistantItemKind::Text => self.active_text,
            AssistantItemKind::PublicReasoning => self.active_public_reasoning,
            AssistantItemKind::PrivateReasoning => self.active_private_reasoning,
            AssistantItemKind::ToolCall => None,
        };
        if let Some(index) = active {
            return index;
        }
        let index = self.next_synthetic_index();
        self.start_item(index, None, kind);
        index
    }

    fn append_public_delta(&mut self, index: u32, value: &str, reasoning: bool) {
        let Some(item) = self.items.get_mut(&index) else {
            return;
        };
        item.content.push_str(value);
        item.identity.delta_sequence = item.identity.delta_sequence.saturating_add(1);
        // Hard gate: a structured JSON contract must never leak into the
        // visible Markdown stream as raw JSON deltas. Once the item starts
        // with `{`, buffer it and defer a single synthesized Markdown item
        // until the item completes.
        if !reasoning && item.content.trim_start().starts_with('{') {
            return;
        }
        if let Some(bus) = &self.bus {
            if reasoning && item.identity.delta_sequence == 1 {
                bus.emit(crate::CowdEvent::ExecutionPhase {
                    status: harness_contract::projection::ExecutionLiveStatus::Thinking,
                    detail: Some("public_reasoning_summary".to_string()),
                });
            }
            let event = if reasoning {
                crate::CowdEvent::ReasoningSummaryDelta {
                    summary: value.to_string(),
                }
            } else {
                crate::CowdEvent::TextDelta {
                    text: value.to_string(),
                }
            };
            let binding = reasoning_activity_binding(bus, &item.identity, item.kind);
            bus.emit_causal_with_activity_binding(item.identity.clone(), event, binding);
            if !reasoning {
                if let Some((presentation_id, attempt_id)) = &self.terminal_presentation {
                    let byte_end = u64::try_from(self.text.len()).unwrap_or(u64::MAX);
                    let byte_start =
                        byte_end.saturating_sub(u64::try_from(value.len()).unwrap_or(u64::MAX));
                    bus.emit(crate::CowdEvent::TerminalDelivery {
                        delivery: harness_contract::live::TerminalDeliveryEvent::TextDelta {
                            presentation_id: presentation_id.clone(),
                            attempt_id: attempt_id.clone(),
                            byte_start,
                            byte_end,
                            delta: value.to_string(),
                        },
                    });
                }
            }
        }
    }

    fn complete_item(&mut self, index: u32) -> Result<Option<EarlyToolCandidate>, RuntimeError> {
        let Some(item) = self.items.get_mut(&index) else {
            return Ok(None);
        };
        if item.completed {
            return Ok(None);
        }
        item.completed = true;
        let ready_tool_identity =
            (item.kind == AssistantItemKind::ToolCall).then(|| item.identity.clone());
        match item.kind {
            AssistantItemKind::Text if self.active_text == Some(index) => self.active_text = None,
            AssistantItemKind::PublicReasoning if self.active_public_reasoning == Some(index) => {
                self.active_public_reasoning = None;
            }
            AssistantItemKind::PrivateReasoning if self.active_private_reasoning == Some(index) => {
                self.active_private_reasoning = None;
            }
            _ => {}
        }
        if item.kind == AssistantItemKind::Text && item.content.trim_start().starts_with('{') {
            let visible = visible_markdown_from_json(&item.content);
            if let Some(bus) = &self.bus {
                bus.emit_synthetic_text_item("json-answer", &visible);
            }
        }
        let Some(kind) = public_causal_item_kind(item.kind) else {
            return Ok(None);
        };
        let planned_tool = item
            .identity
            .tool_call_id
            .as_deref()
            .and_then(|tool_call_id| self.calls.iter().find(|call| call.id == tool_call_id))
            .cloned();
        if let Some(store) = &self.event_store {
            let mut refs = vec![
                RuntimeEventRef {
                    kind: "model_step".to_string(),
                    id: item.identity.model_step_id.clone(),
                },
                RuntimeEventRef {
                    kind: "model_item".to_string(),
                    id: item.identity.item_id.clone(),
                },
            ];
            if let Some(context) = self
                .bus
                .as_ref()
                .and_then(|bus| bus.current_execution_context())
            {
                refs.push(RuntimeEventRef {
                    kind: "execution".to_string(),
                    id: context.execution_id,
                });
                refs.push(RuntimeEventRef {
                    kind: "session".to_string(),
                    id: context.session_id,
                });
                refs.push(RuntimeEventRef {
                    kind: "turn".to_string(),
                    id: context.turn_id,
                });
            }
            let payload = serde_json::json!({
                "model_step_id": item.identity.model_step_id,
                "item_id": item.identity.item_id,
                "segment_id": item.identity.segment_id,
                "causal_sequence": item.identity.causal_sequence,
                "kind": kind,
                "content": item.content,
                "tool_call_id": item.identity.tool_call_id,
                "tool_name": planned_tool.as_ref().map(|call| call.name.as_str()),
                "causal_parent_ids": item.identity.causal_parent_ids,
            });
            let mut input = RuntimeEventInput {
                stream_id: format!("session:{}", self.session_id),
                scope: RuntimeEventScope::Session,
                kind: "model.item_completed".to_string(),
                status: Some("completed".to_string()),
                actor: Some("conversation_runtime.model_stream".to_string()),
                refs,
                payload,
            };
            if let Some(binding) = self
                .bus
                .as_ref()
                .and_then(|bus| reasoning_activity_binding(bus, &item.identity, item.kind))
            {
                input = input.with_activity_binding(binding).map_err(|error| {
                    RuntimeError::new(format!(
                        "public reasoning activity binding is invalid: {error}"
                    ))
                })?;
            }
            store.append(input).map_err(|error| {
                RuntimeError::new(format!(
                    "persist completed model item `{}`: {error}",
                    item.identity.item_id
                ))
            })?;
        }
        if let Some(bus) = &self.bus {
            let binding = reasoning_activity_binding(bus, &item.identity, item.kind);
            bus.emit_causal_with_activity_binding(
                item.identity.clone(),
                crate::CowdEvent::ItemCompleted {
                    kind,
                    tool_name: planned_tool.as_ref().map(|call| call.name.clone()),
                    tool_input: planned_tool.as_ref().map(|call| call.input.clone()),
                },
                binding,
            );
        }
        Ok(ready_tool_identity.and_then(|identity| {
            planned_tool.map(|call| EarlyToolCandidate {
                call,
                identity,
                ready_at_ms: now_ms(),
            })
        }))
    }

    fn complete_incomplete_items(&mut self) -> Result<(), RuntimeError> {
        let incomplete = self
            .items
            .iter()
            .filter_map(|(index, item)| (!item.completed).then_some(*index))
            .collect::<Vec<_>>();
        for index in incomplete {
            let _ = self.complete_item(index)?;
        }
        Ok(())
    }

    fn early_tool_start_enabled(&self) -> bool {
        self.effective_provider_identity
            .as_ref()
            .and_then(|identity| identity.capabilities.get("early_tool_start"))
            .is_some_and(|value| value == "enabled")
    }

    fn apply(
        &mut self,
        event: AssistantEvent,
        observed_at: Instant,
    ) -> Result<(bool, Option<EarlyToolCandidate>), RuntimeError> {
        self.first_event_at.get_or_insert(observed_at);
        let mut ready = None;
        let stop = match event {
            AssistantEvent::ProviderModel { identity } => {
                self.effective_provider_identity = Some(identity);
                false
            }
            AssistantEvent::ItemStarted {
                index,
                provider_item_id,
                kind,
            } => {
                self.start_item(index, provider_item_id.as_deref(), kind);
                false
            }
            AssistantEvent::ItemCompleted { index } => {
                ready = self.complete_item(index)?;
                false
            }
            AssistantEvent::TextDelta(delta) => {
                self.first_text_at.get_or_insert(observed_at);
                self.text.push_str(&delta);
                let index = self.ensure_active(AssistantItemKind::Text);
                self.append_public_delta(index, &delta, false);
                false
            }
            AssistantEvent::ReasoningSummaryDelta(delta) => {
                self.public_reasoning.push_str(&delta);
                let index = self.ensure_active(AssistantItemKind::PublicReasoning);
                self.append_public_delta(index, &delta, true);
                false
            }
            AssistantEvent::PrivateReasoningDelta(delta) => {
                self.private_reasoning.push_str(&delta);
                let index = self.ensure_active(AssistantItemKind::PrivateReasoning);
                if let Some(item) = self.items.get_mut(&index) {
                    item.content.push_str(&delta);
                }
                false
            }
            AssistantEvent::SignatureDelta(delta) => {
                self.signature.push_str(&delta);
                false
            }
            AssistantEvent::ToolUse { id, name, input } => {
                let index = self
                    .items
                    .iter()
                    .find_map(|(index, item)| {
                        (item.kind == AssistantItemKind::ToolCall
                            && item.identity.tool_call_id.as_deref() == Some(id.as_str()))
                        .then_some(*index)
                    })
                    .unwrap_or_else(|| {
                        let index = self.next_synthetic_index();
                        self.start_item(index, Some(id.as_str()), AssistantItemKind::ToolCall);
                        index
                    });
                if let Some(item) = self.items.get_mut(&index) {
                    item.content = input.clone();
                    item.identity.tool_call_id = Some(id.clone());
                }
                self.calls.push(ModelToolCall {
                    id,
                    name,
                    input,
                    depends_on: Vec::new(),
                });
                false
            }
            AssistantEvent::Usage(value) => {
                self.usage = value;
                false
            }
            AssistantEvent::MessageStop => {
                // A successful Provider terminal closes synthetic items that
                // did not have a protocol-level item-stop frame. Persistence
                // must succeed before the model step can become terminal.
                self.emit_public_action_summary_if_needed()?;
                self.complete_incomplete_items()?;
                true
            }
            AssistantEvent::ToolStart { .. }
            | AssistantEvent::ToolProgress { .. }
            | AssistantEvent::ToolComplete { .. } => false,
        };
        Ok((stop, ready))
    }

    fn emit_public_action_summary_if_needed(&mut self) -> Result<(), RuntimeError> {
        if !self.public_reasoning.trim().is_empty() || self.calls.is_empty() {
            return Ok(());
        }
        // A public action summary is a business activity, not transport
        // diagnostics. Internal probes without a canonical activity owner
        // must not create an orphan lifecycle event.
        if self
            .bus
            .as_ref()
            .and_then(|bus| bus.current_activity_binding())
            .is_none()
        {
            return Ok(());
        }
        let summary = public_action_summary(&self.text, &self.calls);
        if summary.is_empty() {
            return Ok(());
        }
        let index = self.next_synthetic_index();
        self.start_item(
            index,
            Some("runtime-public-action-summary"),
            AssistantItemKind::PublicReasoning,
        );
        self.append_public_delta(index, &summary, true);
        Ok(())
    }

    fn finish(self, status: &str) -> CollectedProviderStream {
        if let Some(bus) = &self.bus {
            bus.emit(crate::CowdEvent::ModelStepCompleted {
                model_step_id: self.model_step_id.clone(),
                status: status.to_string(),
            });
        }
        CollectedProviderStream {
            text: self.text,
            public_reasoning: self.public_reasoning,
            private_reasoning: self.private_reasoning,
            signature: self.signature,
            calls: self.calls,
            usage: self.usage,
            effective_provider_identity: self.effective_provider_identity,
            first_event_at: self.first_event_at,
            first_text_at: self.first_text_at,
            early_tool_receipts: Vec::new(),
            early_tool_deferrals: Vec::new(),
            response_completed_at_ms: now_ms(),
        }
    }
}

fn visible_markdown_from_json(text: &str) -> String {
    let zh = user_reply_language(text) == "zh";
    let fallback = if zh {
        "正在整理最终答案…".to_string()
    } else {
        "Preparing final answer…".to_string()
    };
    let Ok(serde_json::Value::Object(object)) =
        serde_json::from_str::<serde_json::Value>(text.trim())
    else {
        return fallback;
    };
    for field in ["answer", "summary", "final_answer", "content"] {
        if let Some(serde_json::Value::String(value)) = object.get(field) {
            if !value.trim().is_empty() {
                return value.clone();
            }
        }
    }
    fallback
}

/// Detect the language of the user's original message so runtime-generated
/// replies follow it instead of hardcoding one language.
pub(crate) fn user_reply_language(text: &str) -> &'static str {
    if text
        .chars()
        .any(|character| matches!(character, '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}'))
    {
        "zh"
    } else {
        "en"
    }
}

fn public_causal_item_kind(kind: AssistantItemKind) -> Option<crate::CausalItemKind> {
    match kind {
        AssistantItemKind::Text => Some(crate::CausalItemKind::Text),
        AssistantItemKind::PublicReasoning => Some(crate::CausalItemKind::PublicReasoning),
        AssistantItemKind::ToolCall => Some(crate::CausalItemKind::ToolCall),
        AssistantItemKind::PrivateReasoning => None,
    }
}

fn reasoning_activity_binding(
    bus: &crate::CowdEventBus,
    identity: &crate::CausalItemIdentity,
    kind: AssistantItemKind,
) -> Option<harness_contract::projection::RuntimeActivityBinding> {
    if kind != AssistantItemKind::PublicReasoning {
        return bus.current_activity_binding();
    }
    let mut binding = bus.current_activity_binding()?;
    let owner_activity_id = binding.activity_id.clone();
    binding.activity_id = crate::cowd_event::owned_child_activity_id(
        &binding,
        "reasoning",
        &format!("{}:{}", identity.model_step_id, identity.item_id),
    );
    binding.parent_activity_id = Some(owner_activity_id.clone());
    binding.initiator_activity_id = Some(owner_activity_id);
    binding.node_id = None;
    binding.skill_id = None;
    binding.skill_revision = None;
    binding.skill_activation_id = None;
    binding.tool_contract_id = None;
    binding.tool_call_id = None;
    binding.approval_id = None;
    Some(binding)
}

struct CollectedProviderStream {
    text: String,
    public_reasoning: String,
    private_reasoning: String,
    signature: String,
    calls: Vec<ModelToolCall>,
    usage: TokenUsage,
    effective_provider_identity: Option<harness_contract::outcome::ProviderIdentity>,
    first_event_at: Option<Instant>,
    first_text_at: Option<Instant>,
    early_tool_receipts: Vec<EarlyToolExecutionReceipt>,
    early_tool_deferrals: Vec<EarlyToolDeferral>,
    response_completed_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct ProviderStreamTimeoutPolicy {
    idle: Duration,
    heartbeat_grace: Duration,
}

struct ProviderStreamRun {
    collected: CollectedProviderStream,
    failure: Option<RuntimeError>,
    resource_result_class: crate::execution_core::graph::ResourceResultClass,
}

const FAILED_PROVIDER_EARLY_TOOL_DRAIN_GRACE: Duration = Duration::from_millis(100);

#[cfg(test)]
async fn consume_provider_stream(
    stream: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>,
    cancellation: CancellationToken,
    timeout_policy: Option<ProviderStreamTimeoutPolicy>,
    reducer: ModelStreamReducer,
    early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
) -> ProviderStreamRun {
    consume_provider_stream_with_activity(
        stream,
        cancellation,
        timeout_policy,
        reducer,
        early_dispatcher,
        None,
    )
    .await
}

async fn consume_provider_stream_with_activity(
    mut stream: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>,
    cancellation: CancellationToken,
    timeout_policy: Option<ProviderStreamTimeoutPolicy>,
    mut reducer: ModelStreamReducer,
    early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
    transport_activity: Option<provider::TransportActivity>,
) -> ProviderStreamRun {
    use futures::StreamExt;

    let mut failure = None;
    let mut resource_result_class = crate::execution_core::graph::ResourceResultClass::Completed;
    let mut early_workers = Vec::new();
    let mut early_tool_deferrals = Vec::new();
    let mut tool_plan = ModelStepToolPlan::default();
    loop {
        let next = if let Some(policy) = timeout_policy {
            loop {
                let generation = transport_activity
                    .as_ref()
                    .map(provider::TransportActivity::generation)
                    .unwrap_or_default();
                let activity_changed = async {
                    if let Some(activity) = &transport_activity {
                        activity.changed_since(generation).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                };
                let idle_window = policy.idle.saturating_add(policy.heartbeat_grace);
                let polled = tokio::select! {
                    () = cancellation.cancelled() => {
                        resource_result_class =
                            crate::execution_core::graph::ResourceResultClass::Cancelled;
                        failure = Some(RuntimeError::new(
                            "turn cancelled during provider stream",
                        ));
                        None
                    }
                    next = tokio::time::timeout(idle_window, stream.next()) => {
                        match next {
                            Ok(next) => next,
                            Err(_) => {
                                resource_result_class =
                                    crate::execution_core::graph::ResourceResultClass::TimedOut;
                                failure = Some(RuntimeError::new(format!(
                                    "stream stalled after {}s without transport or semantic activity",
                                    idle_window.as_secs()
                                )));
                                None
                            }
                        }
                    }
                    () = activity_changed => {
                        crate::execution_core::performance::observe_count(
                            "provider_transport_pulse_total",
                            1,
                        );
                        continue;
                    }
                };
                break polled;
            }
        } else {
            tokio::select! {
                () = cancellation.cancelled() => {
                    resource_result_class =
                        crate::execution_core::graph::ResourceResultClass::Cancelled;
                    failure = Some(RuntimeError::new(
                        "turn cancelled during provider stream",
                    ));
                    None
                }
                next = stream.next() => next,
            }
        };
        let Some(event) = next else {
            break;
        };
        match event {
            Ok(event) => {
                let (stop, ready) = match reducer.apply(event, Instant::now()) {
                    Ok(applied) => applied,
                    Err(error) => {
                        resource_result_class =
                            crate::execution_core::graph::ResourceResultClass::Failed;
                        failure = Some(error);
                        break;
                    }
                };
                if let Some(candidate) = ready {
                    match tool_plan.append(candidate) {
                        Ok(Some(candidate)) => {
                            if reducer.early_tool_start_enabled() {
                                if let Some(dispatcher) = &early_dispatcher {
                                    let dispatcher = Arc::clone(dispatcher);
                                    early_workers.push(tokio::spawn(async move {
                                        dispatcher.dispatch(candidate).await
                                    }));
                                }
                            } else if early_dispatcher.is_some() {
                                early_tool_deferrals.push(EarlyToolDeferral {
                                    tool_call_id: candidate.call.id,
                                    reason:
                                        "provider early_tool_start gate is not performance-certified"
                                            .to_string(),
                                    ready_at_ms: candidate.ready_at_ms,
                                });
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            resource_result_class =
                                crate::execution_core::graph::ResourceResultClass::Failed;
                            failure = Some(RuntimeError::with_provider_failure_metadata(
                                format!("tool_protocol_violation: {error}"),
                                None,
                                true,
                                crate::execution_core::graph::ResourceResultClass::Failed,
                            ));
                            break;
                        }
                    }
                }
                if stop {
                    break;
                }
            }
            Err(error) => {
                resource_result_class = error.provider_resource_result();
                failure = Some(error);
                break;
            }
        }
    }
    drop(stream);
    let response_completed_at_ms = now_ms();
    let status = if failure.is_some() {
        "failed"
    } else {
        "completed"
    };
    let mut collected = reducer.finish(status);
    if failure.is_none() {
        if let Err(error) = tool_plan.seal(&collected.calls) {
            resource_result_class = crate::execution_core::graph::ResourceResultClass::Failed;
            failure = Some(RuntimeError::with_provider_failure_metadata(
                format!("tool_protocol_violation: {error}"),
                None,
                true,
                crate::execution_core::graph::ResourceResultClass::Failed,
            ));
        }
    }

    // An early read is speculative until the whole provider frame is valid.
    // Preserve workers that finish inside a tiny global drain window, but do
    // not let an approval wait, host stall or malformed trailing frame hold
    // the foreground turn indefinitely. Aborting is safe here because only
    // descriptor-certified read-only candidates enter this lane.
    let provider_failed = failure.is_some();
    let joined = if provider_failed && !early_workers.is_empty() {
        match tokio::time::timeout(
            FAILED_PROVIDER_EARLY_TOOL_DRAIN_GRACE,
            futures::future::join_all(early_workers.iter_mut()),
        )
        .await
        {
            Ok(results) => results,
            Err(_) => {
                let mut aborted = 0_u64;
                for worker in &early_workers {
                    if !worker.is_finished() {
                        worker.abort();
                        aborted = aborted.saturating_add(1);
                    }
                }
                crate::execution_core::performance::observe_count(
                    "early_tool_aborted_after_provider_failure_total",
                    aborted,
                );
                futures::future::join_all(early_workers).await
            }
        }
    } else {
        futures::future::join_all(early_workers).await
    };
    let mut early_tool_receipts = Vec::new();
    for result in joined {
        match result {
            Ok(EarlyToolDispatchResult::Executed(receipt)) => {
                early_tool_receipts.push(receipt);
            }
            Ok(EarlyToolDispatchResult::Deferred(deferral)) => {
                early_tool_deferrals.push(deferral);
            }
            Err(error) => {
                tracing::warn!(%error, "early-safe tool worker failed to join");
            }
        }
    }
    collected.early_tool_receipts = early_tool_receipts;
    collected.early_tool_deferrals = early_tool_deferrals;
    collected.response_completed_at_ms = response_completed_at_ms;
    ProviderStreamRun {
        collected,
        failure,
        resource_result_class,
    }
}

fn preview_chars(value: &str, max_chars: usize) -> String {
    let mut preview: String = value.chars().take(max_chars).collect();
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

fn public_action_summary(text: &str, calls: &[ModelToolCall]) -> String {
    const MAX_PUBLIC_ACTION_CHARS: usize = 640;
    let visible = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if !visible.is_empty() {
        return preview_chars(&visible, MAX_PUBLIC_ACTION_CHARS);
    }
    let mut names = Vec::new();
    for name in calls
        .iter()
        .map(|call| call.name.trim())
        .filter(|name| !name.is_empty())
    {
        if !names.iter().any(|existing| *existing == name) {
            names.push(name);
        }
    }
    if names.is_empty() {
        return String::new();
    }
    let zh = user_reply_language(text) == "zh";
    let shown = names.iter().take(8).copied().collect::<Vec<_>>().join(", ");
    let omitted = names.len().saturating_sub(8);
    if omitted == 0 {
        if zh {
            format!("准备调用 {} 个工具：{shown}", calls.len())
        } else {
            format!("About to call {} tools: {shown}", calls.len())
        }
    } else {
        if zh {
            format!(
                "准备调用 {} 个工具：{shown}，另有 {omitted} 类",
                calls.len()
            )
        } else {
            format!(
                "About to call {} tools: {shown}, plus {omitted} more",
                calls.len()
            )
        }
    }
}

fn millis_since(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

fn rate_per_second(count: u64, duration_ms: u64) -> Option<f64> {
    if count == 0 || duration_ms == 0 {
        return None;
    }
    Some(count as f64 / (duration_ms as f64 / 1_000.0))
}

fn bootstrap_tool_ids(
    maximum_permission: harness_contract::tool::ToolPermissionMode,
) -> Vec<String> {
    let mut bootstrap = vec![
        "tool_search".to_string(),
        "context_retrieve".to_string(),
        "runtime_capabilities".to_string(),
        // Managed Team roles must acquire their first bounded source receipt
        // before a required escalation.  These core read-only evidence tools
        // therefore cannot be deferred: an exposure miss terminates the
        // provider step before the role reaches its Runtime-owned checkpoint.
        "glob_search".to_string(),
        "grep_search".to_string(),
        "read_file".to_string(),
        // A managed Agent may carry a Runtime-issued, terminal escalation
        // obligation.  Deferred discovery is unsafe for that contract: the
        // first native request otherwise ends the model step with an
        // exposure-miss before the Agent can retry.  Gateway still verifies
        // the Agent binding and parent Program before executing this tool, so
        // schema visibility grants no standalone escalation authority.
        "request_collaboration_escalation".to_string(),
    ];
    // Stateful orchestration is a runtime-native capability. When the current
    // policy already permits workspace writes, exposing it up front lets the
    // model intentionally start a real team or execution graph instead of
    // repeatedly querying a catalog that it cannot act on. Read-only turns
    // never expose it and still retain explicit discovery for evidence tools.
    if !matches!(
        maximum_permission,
        harness_contract::tool::ToolPermissionMode::ReadOnly
    ) {
        bootstrap.push("runtime_orchestrate".to_string());
        bootstrap.push(
            harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID.to_string(),
        );
    }
    bootstrap
}

fn tool_exposure_for_catalog(
    discovery: &harness_contract::tool::ToolDiscoveryReceipt,
    maximum_permission: harness_contract::tool::ToolPermissionMode,
) -> ToolExposureState {
    let policy = ToolExposurePolicy {
        allowed_ids: discovery
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect(),
        maximum_permission,
        supports_dynamic_exposure: true,
    };
    ToolExposurePlanner.plan(discovery, bootstrap_tool_ids(maximum_permission), &policy)
}

#[derive(Debug, Default)]
struct TurnToolExposureMetrics {
    projection: ToolExposureMetrics,
    activated_ids: BTreeSet<String>,
    invoked_ids: BTreeSet<String>,
    pending_search_round: bool,
    schema_stats_baseline: Option<(u64, u64)>,
}

impl TurnToolExposureMetrics {
    fn reset(&mut self, schema_stats_baseline: (u64, u64)) {
        *self = Self::default();
        self.schema_stats_baseline = Some(schema_stats_baseline);
    }

    fn observe_catalog_lookup(&mut self, elapsed: Duration) {
        self.projection.catalog_lookups = self.projection.catalog_lookups.saturating_add(1);
        self.projection.catalog_lookup_micros = self
            .projection
            .catalog_lookup_micros
            .saturating_add(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);
    }

    fn observe_provider_request(
        &mut self,
        inventory: ProviderContextInventory,
        schema_stats: (u64, u64),
    ) {
        self.projection.provider_requests = self.projection.provider_requests.saturating_add(1);
        if self.pending_search_round {
            self.projection.tool_search_additional_rounds = self
                .projection
                .tool_search_additional_rounds
                .saturating_add(1);
            self.pending_search_round = false;
        }
        self.projection.schema_tokens_max = self
            .projection
            .schema_tokens_max
            .max(inventory.tool_schema_tokens);
        let baseline = self.schema_stats_baseline.get_or_insert(schema_stats);
        self.projection.schema_compilations = schema_stats.0.saturating_sub(baseline.0);
        self.projection.schema_cache_hits = schema_stats.1.saturating_sub(baseline.1);
    }

    fn observe_search(&mut self, receipt: &harness_contract::tool::ToolActivationReceipt) {
        self.projection.tool_search_calls = self.projection.tool_search_calls.saturating_add(1);
        self.observe_activation(receipt);
    }

    fn observe_activation(&mut self, receipt: &harness_contract::tool::ToolActivationReceipt) {
        use harness_contract::tool::ToolActivationStatus;

        self.projection.activation_candidates = self
            .projection
            .activation_candidates
            .saturating_add(receipt.decisions.len() as u64);
        self.pending_search_round = true;
        for decision in &receipt.decisions {
            match decision.status {
                ToolActivationStatus::Activated => {
                    if self.activated_ids.insert(decision.canonical_id.clone()) {
                        self.projection.activations = self.projection.activations.saturating_add(1);
                    }
                }
                ToolActivationStatus::NotFound => {
                    self.projection.descriptor_misses =
                        self.projection.descriptor_misses.saturating_add(1);
                }
                ToolActivationStatus::Denied => {
                    self.projection.permission_rejections =
                        self.projection.permission_rejections.saturating_add(1);
                }
                ToolActivationStatus::Unavailable => {
                    self.projection.unavailable_descriptors =
                        self.projection.unavailable_descriptors.saturating_add(1);
                }
            }
        }
    }

    fn observe_invalid_search(&mut self) {
        self.projection.tool_search_calls = self.projection.tool_search_calls.saturating_add(1);
        self.projection.descriptor_misses = self.projection.descriptor_misses.saturating_add(1);
        self.pending_search_round = true;
    }

    fn observe_invocation(&mut self, tool_name: &str) {
        if self.activated_ids.contains(tool_name) {
            self.invoked_ids.insert(tool_name.to_string());
        }
    }

    fn projection(&self) -> ToolExposureMetrics {
        let mut projection = self.projection.clone();
        projection.activated_invocations =
            self.activated_ids.intersection(&self.invoked_ids).count() as u64;
        projection.activation_precision_bp = (!self.activated_ids.is_empty()).then(|| {
            ratio_bp(
                projection.activated_invocations,
                self.activated_ids.len() as u64,
            )
        });
        // Runtime cannot know the task-specific set of Tools that should have
        // been exposed. A paired evaluator with frozen ground truth owns recall.
        projection.activation_recall_bp = None;
        projection
    }
}

fn ratio_bp(numerator: u64, denominator: u64) -> u16 {
    if denominator == 0 {
        return 0;
    }
    u16::try_from(
        numerator
            .saturating_mul(10_000)
            .saturating_div(denominator)
            .min(10_000),
    )
    .unwrap_or(10_000)
}

#[derive(Debug, Default)]
struct TurnStablePrefixMetrics {
    projection: StablePrefixMetrics,
}

impl TurnStablePrefixMetrics {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn observe_request(&mut self, request: &ApiRequest) {
        let wire = request.prompt.wire_system_text().unwrap_or_default();
        let stable_bytes = request.prompt.stable_system_bytes();
        let stable = wire.as_bytes().get(..stable_bytes).unwrap_or_default();
        let fingerprint = format!(
            "{:016x}",
            model_protocol::fingerprint::stable_hash_bytes(stable)
        );
        if !self.projection.stable_prefix_fingerprint.is_empty()
            && self.projection.stable_prefix_fingerprint != fingerprint
        {
            self.projection.wire_identity_failures =
                self.projection.wire_identity_failures.saturating_add(1);
        }
        if stable.len() != stable_bytes {
            self.projection.wire_identity_failures =
                self.projection.wire_identity_failures.saturating_add(1);
        }
        self.projection.provider_requests = self.projection.provider_requests.saturating_add(1);
        self.projection.stable_prefix_fingerprint = fingerprint;
        self.projection.stable_prefix_bytes = stable.len() as u64;
        self.projection.runtime_system_bytes_max = self.projection.runtime_system_bytes_max.max(
            request
                .prompt
                .runtime_system_text()
                .map_or(0, |text| text.len()) as u64,
        );
        if request.request_compiler_cache_hit {
            self.projection.request_compiler_cache_hits = self
                .projection
                .request_compiler_cache_hits
                .saturating_add(1);
        } else {
            self.projection.request_compiler_compilations = self
                .projection
                .request_compiler_compilations
                .saturating_add(1);
        }
    }

    fn observe_usage(&mut self, usage: TokenUsage) {
        self.projection.native_cache_creation_input_tokens = self
            .projection
            .native_cache_creation_input_tokens
            .saturating_add(u64::from(usage.cache_creation_input_tokens));
        self.projection.native_cache_read_input_tokens = self
            .projection
            .native_cache_read_input_tokens
            .saturating_add(u64::from(usage.cache_read_input_tokens));
    }
}

#[cfg(test)]
mod tool_exposure_contract_tests {
    use super::bootstrap_tool_ids;
    use harness_contract::tool::ToolPermissionMode;

    #[test]
    fn required_runtime_control_tools_are_bootstrapped_while_orchestration_stays_write_gated() {
        assert_eq!(
            bootstrap_tool_ids(ToolPermissionMode::ReadOnly),
            vec![
                "tool_search",
                "context_retrieve",
                "runtime_capabilities",
                "glob_search",
                "grep_search",
                "read_file",
                "request_collaboration_escalation"
            ]
        );
        assert_eq!(
            bootstrap_tool_ids(ToolPermissionMode::WorkspaceWrite),
            vec![
                "tool_search",
                "context_retrieve",
                "runtime_capabilities",
                "glob_search",
                "grep_search",
                "read_file",
                "request_collaboration_escalation",
                "runtime_orchestrate",
                harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID
            ]
        );
    }
}

fn fallback_tool_discovery_receipt(
    mut available_ids: Vec<String>,
) -> harness_contract::tool::ToolDiscoveryReceipt {
    use harness_contract::tool::{
        ToolDescriptorHealth, ToolDescriptorRef, ToolDiscoveryReceipt, ToolPermissionMode,
    };

    available_ids.sort();
    available_ids.dedup();
    let descriptors = available_ids
        .iter()
        .map(|id| ToolDescriptorRef {
            canonical_id: id.clone(),
            display_name: id.clone(),
            source: "executor-fallback".to_string(),
            schema_hash: format!("fallback:{id}"),
            required_permission: ToolPermissionMode::ReadOnly,
            permission_source: "executor-fallback".to_string(),
            health: ToolDescriptorHealth::Healthy,
        })
        .collect();
    ToolDiscoveryReceipt {
        query: "catalog-fallback".to_string(),
        catalog_revision: 0,
        descriptors,
        activation_candidates: available_ids,
    }
}

fn contract_permission_mode(
    mode: crate::PermissionMode,
) -> harness_contract::tool::ToolPermissionMode {
    match mode {
        crate::PermissionMode::ReadOnly => harness_contract::tool::ToolPermissionMode::ReadOnly,
        crate::PermissionMode::WorkspaceWrite => {
            harness_contract::tool::ToolPermissionMode::WorkspaceWrite
        }
        crate::PermissionMode::DangerFullAccess => {
            harness_contract::tool::ToolPermissionMode::DangerFullAccess
        }
    }
}

fn denied_capability_assessment(
    mut assessment: harness_contract::policy::CapabilityAssessment,
    reason: &str,
    approval_ref: &str,
) -> harness_contract::policy::CapabilityAssessment {
    assessment.path = harness_contract::policy::AuthorizationPath::HardDeny;
    assessment.lease = None;
    assessment.gap = Some(harness_contract::policy::CapabilityGap {
        fingerprint: assessment
            .gap
            .as_ref()
            .map(|gap| gap.fingerprint.clone())
            .unwrap_or_else(|| assessment.assessment_id.clone()),
        kind: harness_contract::policy::CapabilityGapKind::ApprovalRequired,
        capability: assessment.capability.clone(),
        requested_scopes: assessment.requested_scopes.clone(),
        required_mode: assessment.required_mode,
        active_ceiling: assessment.active_ceiling,
        parent_ceiling: assessment.parent_ceiling,
        reason: reason.to_string(),
        safe_alternatives: assessment
            .gap
            .as_ref()
            .map(|gap| gap.safe_alternatives.clone())
            .unwrap_or_default(),
        recoverable: false,
    });
    assessment
        .evidence_refs
        .push(format!("approval:{approval_ref}:{reason}"));
    assessment
}

fn emit_approval_resolution_event(
    cowd: Option<&crate::CowdEventBus>,
    queue: &crate::ApprovalQueue,
    resolution: &Result<crate::ApprovalResolution, String>,
) {
    let (Some(cowd), Ok(resolution)) = (cowd, resolution) else {
        return;
    };
    let Some(request) = queue.get(resolution.approval_id()) else {
        return;
    };
    cowd.emit(crate::cowd_event::CowdEvent::ApprovalResolved {
        request_id: request.approval_id,
        status: request.status,
        scope: request.decision.as_ref().map(|decision| decision.scope),
        actor_id: request
            .decision
            .as_ref()
            .map(|decision| decision.actor.actor_id.clone()),
    });
}

fn apply_runtime_budget_to_control_policy(
    policy: &mut RuntimeControlPolicy,
    budget_plan: &RuntimeBudgetPlan,
) {
    policy.context.yolo_budget_tokens = budget_plan.runtime_control_budget.yolo_budget_tokens;
    policy.context.collaboration_budget_tokens = budget_plan
        .runtime_control_budget
        .collaboration_budget_tokens;
    policy.context.review_budget_tokens = budget_plan.runtime_control_budget.review_budget_tokens;
}

fn knowledge_hard_gate_active(system_prompt: &[String]) -> bool {
    system_prompt
        .iter()
        .any(|fragment| fragment.contains("<hard_gate action=\"block\">"))
}

/// Streaming API contract. Implementors produce AssistantEvents lazily.
/// Consumers poll the stream and process each event as it arrives.
///
/// For backward compatibility, a `collect()` call gathers all events
/// into a Vec (same as the old sync signature).
pub trait ApiClient {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>;

    fn stream_with_transport_activity(&mut self, request: ApiRequest) -> ApiClientStream<'_> {
        ApiClientStream {
            events: self.stream(request),
            transport_activity: None,
        }
    }

    fn provider_available(&self) -> bool {
        true
    }

    fn provider_name_for_model(&self, _model: &str) -> Option<String> {
        None
    }

    fn configure_tool_exposure(
        &mut self,
        _projection: harness_contract::tool::ToolExposureProjection,
    ) {
    }

    /// Require the next provider request to select one of the currently
    /// exposed tools. The production provider client consumes this setting
    /// exactly once; non-provider test clients may ignore it.
    fn configure_tool_choice_required(&mut self, _required: bool) {}

    /// Optionally bind a required request to one already-exposed native tool.
    /// This is a wire-level selection constraint, not permission or semantic
    /// authority: Runtime still validates the tool input and every resulting
    /// receipt. The default keeps lightweight test clients source-compatible.
    fn configure_tool_choice(&mut self, required: bool, _required_tool_name: Option<String>) {
        self.configure_tool_choice_required(required);
    }

    fn configure_provider_wire_evidence(
        &mut self,
        _writer: Option<Arc<dyn crate::ProviderWireEvidenceWriter>>,
    ) {
    }

    fn context_inventory(&self) -> ProviderContextInventory {
        ProviderContextInventory::default()
    }

    /// Lifetime Tool schema compilation/cache counters for this client.
    fn tool_schema_cache_stats(&self) -> (u64, u64) {
        (0, 0)
    }

    /// Convenience: collect all events synchronously (backward compat).
    fn stream_collect(&mut self, request: ApiRequest) -> Result<Vec<AssistantEvent>, RuntimeError> {
        // Provider streams create their producer task when `stream` is called.
        // Build the stream *inside* the runtime that will poll it so synchronous
        // callers (evaluation, diagnostics, CLI checks) retain the same
        // cancellation and streaming behavior as an ordinary Runtime turn.
        let collect = async {
            let mut pinned = self.stream(request);
            use futures::StreamExt;
            let mut events = Vec::new();
            while let Some(event) = pinned.next().await {
                events.push(event?);
            }
            Ok(events)
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(collect)
        } else {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    RuntimeError::new(format!("build stream collector runtime: {error}"))
                })?;
            runtime.block_on(collect)
        }
    }
}

pub struct ApiClientStream<'a> {
    pub events: Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + 'a>>,
    pub transport_activity: Option<provider::TransportActivity>,
}

/// Per-invocation model-delivery policy resolved from an immutable execution
/// binding. This is not proof that delivery occurred; Conversation owns that
/// proof after a matching Provider request returns a valid response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ToolModelDeliveryRequirement {
    #[default]
    Bounded,
    Exact {
        obligation_ids: Vec<String>,
    },
}

impl ToolModelDeliveryRequirement {
    #[must_use]
    pub fn exact(mut obligation_ids: Vec<String>) -> Self {
        obligation_ids.sort();
        obligation_ids.dedup();
        if obligation_ids.is_empty() {
            Self::Bounded
        } else {
            Self::Exact { obligation_ids }
        }
    }

    #[must_use]
    pub fn obligation_ids(&self) -> &[String] {
        match self {
            Self::Bounded => &[],
            Self::Exact { obligation_ids } => obligation_ids,
        }
    }

    #[must_use]
    pub const fn is_exact(&self) -> bool {
        matches!(self, Self::Exact { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedModelReceipt {
    provider_invocation_id: String,
    tool_name: String,
    obligation_ids: Vec<String>,
    raw_ref: EvidenceRef,
    model_receipt_sha256: String,
    raw_tokens: u64,
    receipt_tokens: u64,
    omitted_tokens: u64,
    complete: bool,
}

/// Object-safe asynchronous contract for model-requested Tool execution.
///
/// Implementors must keep native asynchronous work on the calling Runtime.
/// Explicitly blocking libraries and process adapters are isolated by
/// `ToolExecutionPlane`; callers must not construct private runtimes or
/// threads.
#[async_trait::async_trait]
pub trait ToolExecutor: Send + Sync + 'static {
    /// Execute a Tool using the bounded-output contract consumed by Runtime.
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError>;

    /// Execute a Provider-originated invocation while preserving its identity
    /// into the acquisition receipt. Compatibility executors may ignore the
    /// identity; production scoped executors override this method.
    async fn execute_invocation_output(
        &self,
        _provider_invocation_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        self.execute_output(tool_name, input).await
    }

    /// Resolve whether this concrete invocation carries a frozen exact
    /// Provider-model observation obligation. The default preserves ordinary
    /// bounded tool behavior.
    fn model_delivery_requirement(
        &self,
        _tool_name: &str,
        _input: &str,
    ) -> ToolModelDeliveryRequirement {
        ToolModelDeliveryRequirement::Bounded
    }

    /// Runtime-attested evidence produced by this executor so far.
    ///
    /// The default is empty for lightweight/test executors. Production
    /// delegated executors override it with their typed receipt ledger. This
    /// keeps ConversationHost acceptance on the same facts consumed by the
    /// outer Agent validator instead of reparsing tool-result text.
    fn observed_evidence_snapshot(&self) -> Vec<harness_contract::context::ObservedEvidence> {
        Vec::new()
    }

    /// Validate provider-supplied input before Runtime publishes the ToolUse
    /// block, negotiates permission, or reserves execution capacity.
    /// Production executors bind this check to their pinned catalog; the
    /// fallback keeps lightweight test executors compatible while still
    /// rejecting malformed JSON at the protocol boundary.
    fn validate_tool_input(&self, _tool_name: &str, input: &str) -> Result<(), ToolError> {
        serde_json::from_str::<serde_json::Value>(input)
            .map(|_| ())
            .map_err(|error| ToolError::new(format!("invalid tool input JSON: {error}")))
    }

    /// Production executors override this with a receipt from their pinned
    /// ToolHost. The fallback is deliberately read-only for small embedded and
    /// test executors that do not own a catalog.
    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        fallback_tool_discovery_receipt(self.available_tool_names())
    }

    fn registered_tool_effect(
        &self,
        _tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        None
    }

    fn prepare_governed_invocations(
        &self,
        requests: &[crate::tool_dispatch::ToolRequest],
    ) -> Vec<harness_contract::tool::GovernedToolInvocation> {
        let catalog_revision = self.tool_discovery_receipt().catalog_revision;
        requests
            .iter()
            .filter_map(|request| {
                let input = serde_json::from_str::<serde_json::Value>(&request.input).ok()?;
                let effect = self.registered_tool_effect(&request.tool_name, &input)?;
                Some(harness_contract::tool::GovernedToolInvocation {
                    contract_version: 1,
                    invocation_id: request.tool_use_id.clone(),
                    intent: harness_contract::tool::ToolIntent {
                        invocation_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        normalized_input: input,
                    },
                    resource_demand: harness_contract::tool::ResourceDemand {
                        tool_slots: 1,
                        process_slots: u32::from(effect.spawns_process),
                        network_slots: u32::from(effect.uses_network),
                        cpu_weight: if effect.spawns_process { 2 } else { 1 },
                        memory_bytes: 0,
                        scopes: effect
                            .scopes
                            .iter()
                            .filter_map(|scope| {
                                scope.target.clone().map(|key| {
                                    harness_contract::tool::ResourceScopeDemand {
                                        key,
                                        access: if scope.operation
                                            == harness_contract::policy::PermissionOperation::Read
                                        {
                                            harness_contract::tool::ResourceAccess::Read
                                        } else {
                                            harness_contract::tool::ResourceAccess::Write
                                        },
                                    }
                                })
                            })
                            .collect(),
                    },
                    explicit_dependencies: request
                        .depends_on
                        .iter()
                        .map(|depends_on| harness_contract::tool::ToolDependency {
                            invocation_id: request.tool_use_id.clone(),
                            depends_on: depends_on.clone(),
                            reason: "model_explicit_dependency".to_string(),
                        })
                        .collect(),
                    compiled_dependencies: Vec::new(),
                    catalog_revision,
                    descriptor_set_hash: effect.descriptor_hash.clone(),
                    idempotency_key: format!(
                        "{}:{}:{}",
                        request.tool_name, request.tool_use_id, effect.descriptor_hash
                    ),
                    effect,
                })
            })
            .collect()
    }

    async fn execute_authorized_output(
        &self,
        _authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        Err(ToolError::new(format!(
            "tool `{tool_name}` has no authorized execution implementation"
        )))
    }

    async fn execute_authorized_invocation_output(
        &self,
        _provider_invocation_id: &str,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        self.execute_authorized_output(authorization, tool_name, input)
            .await
    }

    fn has_registered_tools(&self) -> bool {
        !self.available_tool_names().is_empty()
    }

    fn available_tool_names(&self) -> Vec<String> {
        Vec::new()
    }

    /// Resolve a provider-emitted spelling to the one catalog-owned tool id.
    ///
    /// Production executors should delegate to their pinned catalog. The
    /// fallback supports embedded/test executors while remaining fail-closed
    /// when normalized names are ambiguous.
    fn resolve_tool_name(&self, requested: &str) -> Option<String> {
        let available = self.available_tool_names();
        if available.iter().any(|name| name == requested) {
            return Some(requested.to_string());
        }
        let identity = normalized_tool_identity(requested);
        if let Some(canonical) = match identity.as_str() {
            "read" => Some("read_file"),
            "write" => Some("write_file"),
            "edit" => Some("edit_file"),
            "glob" => Some("glob_search"),
            "grep" => Some("grep_search"),
            _ => None,
        }
        .filter(|canonical| available.iter().any(|name| name == canonical))
        {
            return Some(canonical.to_string());
        }
        let mut matches = available
            .into_iter()
            .filter(|name| normalized_tool_identity(name) == identity);
        let resolved = matches.next()?;
        matches.next().is_none().then_some(resolved)
    }

    fn has_tool(&self, tool_name: &str) -> bool {
        self.resolve_tool_name(tool_name).is_some()
    }

    fn classify_tool_safety(
        &self,
        _tool_name: &str,
        _input: &str,
    ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
        None
    }

    fn collaboration_runtime_available(&self) -> bool {
        false
    }

    fn mission_runtime_available(&self) -> bool {
        false
    }

    fn bind_execution_decision(&self, _decision: crate::execution_core::RuntimeExecutionDecision) {}
}

/// Tool execution lifecycle callback for real-time visualization.
/// Inspired by hermes-agent stream_consumer.py tool_progress_callback.
pub trait ToolCallback: Send + Sync {
    /// Called when a tool starts executing.
    fn on_tool_start(&self, id: &str, name: &str, preview: &str);
    /// Called when a tool reports progress.
    fn on_tool_progress(&self, id: &str, name: &str, progress: &str);
    /// Called when a tool finishes executing.
    fn on_tool_complete(&self, id: &str, name: &str, result_summary: &str, exit_code: Option<i32>);
    /// Called when token usage data is available (typically after each stream completes).
    /// Default implementation is a no-op so existing implementors don't break.
    fn on_usage(&self, _usage: &TokenUsage) {}
}

/// Memory lifecycle callback for real-time TUI visualization.
/// Follows the same pattern as [`ToolCallback`] so the CLI crate can
/// forward memory events to the TUI render loop.
pub trait MemoryCallback: Send + Sync {
    /// Called when memory context entries are prepared for injection into
    /// the system prompt. Each tuple is `(layer, content, relevance)`.
    fn on_memory_update(&self, entries: Vec<(String, String, f64)>, status: &str);
    /// Called after post-turn memory housekeeping completes
    /// (micro-compact, drift, seeds).
    fn on_memory_stats(&self, total_entries: usize, vector_count: usize, layers: Vec<String>);
}

/// Error returned when a tool invocation fails locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    message: String,
    failure: Option<harness_contract::tool::ToolExecutionFailure>,
}

impl ToolError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            failure: None,
        }
    }

    #[must_use]
    pub fn from_failure(failure: harness_contract::tool::ToolExecutionFailure) -> Self {
        Self {
            message: failure.message.clone(),
            failure: Some(failure),
        }
    }

    #[must_use]
    pub fn failure(&self) -> Option<&harness_contract::tool::ToolExecutionFailure> {
        self.failure.as_ref()
    }

    #[must_use]
    pub fn model_text(&self) -> String {
        self.failure.as_ref().map_or_else(
            || self.message.clone(),
            |failure| {
                serde_json::json!({
                    "status": "failed",
                    "error": failure,
                    "recovery": if failure.class == harness_contract::tool::ToolExecutionFailureClass::InputContract {
                        "repair_arguments_once"
                    } else {
                        "replan"
                    }
                })
                .to_string()
            },
        )
    }
}

impl Display for ToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.model_text())
    }
}

impl std::error::Error for ToolError {}

/// Error returned when a conversation turn cannot be completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    message: String,
    provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope,
    provider_account_key: Option<String>,
    provider_context_window_limit: Option<u32>,
    provider_tool_protocol_failure: bool,
    tool_exposure_miss: bool,
    provider_resource_result: crate::execution_core::graph::ResourceResultClass,
    provider_retry_after: Option<Duration>,
    provider_retryable: bool,
    provider_usage: Option<TokenUsage>,
    effect_receipts: Vec<EarlyToolExecutionReceipt>,
}

impl RuntimeError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope::Request,
            provider_account_key: None,
            provider_context_window_limit: None,
            provider_tool_protocol_failure: false,
            tool_exposure_miss: false,
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Failed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
            effect_receipts: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_provider_context_window_limit(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
    ) -> Self {
        Self {
            message: message.into(),
            provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope::Request,
            provider_account_key: None,
            provider_context_window_limit,
            provider_tool_protocol_failure: false,
            tool_exposure_miss: false,
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Failed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
            effect_receipts: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_provider_failure_metadata(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
        provider_tool_protocol_failure: bool,
        provider_resource_result: crate::execution_core::graph::ResourceResultClass,
    ) -> Self {
        Self::with_provider_failure_metadata_and_retry_after(
            message,
            provider_context_window_limit,
            provider_tool_protocol_failure,
            provider_resource_result,
            None,
            false,
        )
    }

    #[must_use]
    pub fn with_provider_failure_metadata_and_retry_after(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
        provider_tool_protocol_failure: bool,
        provider_resource_result: crate::execution_core::graph::ResourceResultClass,
        provider_retry_after: Option<Duration>,
        provider_retryable: bool,
    ) -> Self {
        Self::with_provider_failure_metadata_retry_after_and_scope(
            message,
            provider_context_window_limit,
            provider_tool_protocol_failure,
            provider_resource_result,
            provider_retry_after,
            provider_retryable,
            model_protocol::provider_failure::ProviderFailureScope::Request,
        )
    }

    #[must_use]
    pub fn with_provider_failure_metadata_retry_after_and_scope(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
        provider_tool_protocol_failure: bool,
        provider_resource_result: crate::execution_core::graph::ResourceResultClass,
        provider_retry_after: Option<Duration>,
        provider_retryable: bool,
        provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope,
    ) -> Self {
        Self {
            message: message.into(),
            provider_failure_scope,
            provider_account_key: None,
            provider_context_window_limit,
            provider_tool_protocol_failure,
            tool_exposure_miss: false,
            provider_resource_result,
            provider_retry_after,
            provider_retryable,
            provider_usage: None,
            effect_receipts: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_tool_exposure_miss(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            provider_failure_scope: model_protocol::provider_failure::ProviderFailureScope::Request,
            provider_account_key: None,
            provider_context_window_limit: None,
            provider_tool_protocol_failure: true,
            tool_exposure_miss: true,
            // Exposure activation is a local Runtime continuation, not a
            // provider-capacity failure and must not reduce provider limits.
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Completed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
            effect_receipts: Vec::new(),
        }
    }

    #[must_use]
    pub const fn with_provider_usage(mut self, usage: TokenUsage) -> Self {
        self.provider_usage = Some(usage);
        self
    }

    #[must_use]
    pub(crate) fn with_provider_account_key(mut self, account_key: Option<String>) -> Self {
        self.provider_account_key = account_key;
        self
    }

    #[must_use]
    pub(crate) fn with_effect_receipts(mut self, receipts: Vec<EarlyToolExecutionReceipt>) -> Self {
        self.effect_receipts = receipts;
        self
    }

    #[must_use]
    pub(crate) fn effect_receipts(&self) -> &[EarlyToolExecutionReceipt] {
        &self.effect_receipts
    }

    #[must_use]
    pub const fn provider_context_window_limit(&self) -> Option<u32> {
        self.provider_context_window_limit
    }

    #[must_use]
    pub const fn is_provider_tool_protocol_failure(&self) -> bool {
        self.provider_tool_protocol_failure
    }

    #[must_use]
    pub const fn is_tool_exposure_miss(&self) -> bool {
        self.tool_exposure_miss
    }

    #[must_use]
    pub const fn provider_resource_result(
        &self,
    ) -> crate::execution_core::graph::ResourceResultClass {
        self.provider_resource_result
    }

    #[must_use]
    pub const fn provider_retry_after(&self) -> Option<Duration> {
        self.provider_retry_after
    }

    #[must_use]
    pub const fn provider_retryable(&self) -> bool {
        self.provider_retryable
    }

    #[must_use]
    pub const fn provider_failure_scope(
        &self,
    ) -> model_protocol::provider_failure::ProviderFailureScope {
        self.provider_failure_scope
    }

    #[must_use]
    pub fn provider_account_key(&self) -> Option<&str> {
        self.provider_account_key.as_deref()
    }

    #[must_use]
    pub const fn provider_usage(&self) -> Option<TokenUsage> {
        self.provider_usage
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RuntimeError {}

/// Summary of one completed runtime turn, including tool results and usage.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnSummary {
    /// The terminal answer selected by the execution graph synthesizer.
    /// Callers must not infer the result from the session transcript.
    pub final_answer: String,
    /// Canonical goal outcome committed by the graph terminal. Text alone is
    /// insufficient here: a blocked execution may intentionally produce an
    /// honest explanatory message, which must not be reported as success to a
    /// parent Agent or protocol reducer.
    pub terminal_completion: harness_contract::goal::GoalCompletion,
    pub assistant_messages: Vec<ConversationMessage>,
    pub tool_results: Vec<ConversationMessage>,
    pub iterations: usize,
    pub usage: TokenUsage,
    pub model_telemetry: crate::cowd_event::RunModelTelemetry,
    pub auto_compaction: Option<AutoCompactionEvent>,
    pub ai_kernel_trace: RuntimeAiKernelTrace,
    pub context_turn_report: ContextTurnReport,
    /// Invocation-level proof that an exact ToolResult survived packing and
    /// was consumed by a valid Provider continuation in this turn.
    pub model_observations: Vec<harness_contract::context::ProviderModelObservationAttestation>,
    pub duplicate_tool_calls: u64,
    /// Canonical workspace paths targeted by write-capable tool calls during
    /// this turn, including calls rejected before execution. This is distinct
    /// from the final workspace diff: mutate-and-restore and same-bytes writes
    /// remain observable to evaluation and audit consumers.
    pub write_attempt_paths: Vec<String>,
    pub max_tool_concurrency_observed: usize,
    pub parallel_tool_batches: usize,
}

/// One provider decision. Model steps are deliberately side-effect free with
/// respect to tools: they may request work, but only a ToolBatch executor may
/// perform it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelStepIntent {
    FinalAnswer { text: String },
    ToolCalls { calls: Vec<ModelToolCall> },
    Replan { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelToolCall {
    pub id: String,
    pub name: String,
    pub input: String,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelStepResult {
    pub intent: ModelStepIntent,
    pub assistant_message: ConversationMessage,
    pub usage: TokenUsage,
    pub model: Option<String>,
    /// Ordered provider/model candidates actually attempted for this
    /// successful step, including failed fallbacks before the winner.
    pub models_used: Vec<String>,
    pub first_token_latency_ms: Option<u64>,
    pub active_stream_duration_ms: Option<u64>,
    pub wall_duration_ms: u64,
    /// Read-only calls completed through the governed early lane while the
    /// Provider was still streaming. The graph ToolBatch consumes these
    /// durable receipts instead of executing the effect a second time.
    pub(crate) early_tool_receipts: Vec<EarlyToolExecutionReceipt>,
    /// Calls observed at item completion but deliberately retained for the
    /// finalized ToolBatch, with a machine-auditable safety reason.
    pub(crate) early_tool_deferrals: Vec<EarlyToolDeferral>,
    pub(crate) response_completed_at_ms: u64,
    /// Whether this response was requested under the one-shot, zero-tool
    /// terminal checkpoint. Graph owners must enforce the boundary from the
    /// returned step rather than infer it from local scheduling state.
    pub text_only_response: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolBatchStepResult {
    pub messages: Vec<ConversationMessage>,
    pub failed: usize,
    pub max_concurrency_observed: usize,
    pub parallel_batches: usize,
}

/// Details about automatic session compaction applied during a turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoCompactionEvent {
    pub removed_message_count: usize,
    pub compaction_receipt: Option<CompactionReceipt>,
}

/// P1-05: Callback for generator-style turn injection after tool results.
pub struct TurnCallback {
    pub on_tool_result: Box<dyn Fn(&str, &str) -> Option<String> + Send + Sync>,
}
impl TurnCallback {
    pub fn new<F: Fn(&str, &str) -> Option<String> + Send + Sync + 'static>(f: F) -> Self {
        Self {
            on_tool_result: Box::new(f),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct PreparedVisionPayload {
    tool: String,
    status: String,
    image_path: String,
    media_type: String,
    prompt: String,
    image_base64: String,
    size_bytes: Option<u64>,
}

fn prepared_vision_payload(
    tool_name: &str,
    output: &str,
    is_error: bool,
) -> Option<PreparedVisionPayload> {
    if is_error || tool_name != "vision_analyze" {
        return None;
    }
    let payload = serde_json::from_str::<PreparedVisionPayload>(output).ok()?;
    if payload.tool != "vision_analyze"
        || payload.status != "prepared"
        || payload.image_base64.trim().is_empty()
        || !payload.media_type.starts_with("image/")
    {
        return None;
    }
    Some(payload)
}

fn vision_index_summary(payload: &PreparedVisionPayload) -> String {
    format!(
        "vision_analyze prepared image input: path={}, media_type={}, size_bytes={}, prompt={}",
        payload.image_path,
        payload.media_type,
        payload.size_bytes.unwrap_or_default(),
        payload.prompt
    )
}

fn vision_tool_model_receipt(payload: &PreparedVisionPayload, raw_ref: &EvidenceRef) -> String {
    format!(
        "Tool `vision_analyze` completed. Raw evidence ref: tool://{}. Image input is attached as a structured vision block for the next model call. path={}, media_type={}, size_bytes={}, prompt={}",
        raw_ref.id(),
        payload.image_path,
        payload.media_type,
        payload.size_bytes.unwrap_or_default(),
        payload.prompt
    )
}

fn vision_user_message(payload: &PreparedVisionPayload) -> ConversationMessage {
    ConversationMessage {
        role: crate::session::MessageRole::User,
        blocks: vec![
            ContentBlock::Text {
                text: format!(
                    "Analyze the attached image for this request. Original image path: {}. Prompt: {}",
                    payload.image_path, payload.prompt
                ),
            },
            ContentBlock::Image {
                media_type: payload.media_type.clone(),
                data: payload.image_base64.clone(),
                source_path: Some(payload.image_path.clone()),
            },
        ],
        usage: None,
    }
}

/// Build a structured user message that carries an image as multimodal input.
///
/// Surface adapters use this to hand already-downloaded media to runtime without
/// asking the model to first call a preparation tool. The caller is still
/// expected to pass the natural-language request as the actual turn prompt.
pub fn image_user_message_from_path(
    image_path: impl AsRef<Path>,
    media_type: impl AsRef<str>,
    prompt: impl AsRef<str>,
) -> Result<ConversationMessage, RuntimeError> {
    let image_path = image_path.as_ref();
    let media_type =
        normalize_image_media_type(image_path, media_type.as_ref()).ok_or_else(|| {
            RuntimeError::new(format!(
                "unsupported image media type `{}` for {}",
                media_type.as_ref(),
                image_path.display()
            ))
        })?;
    let image_data = std::fs::read(image_path).map_err(|error| {
        RuntimeError::new(format!(
            "failed to read image attachment {}: {error}",
            image_path.display()
        ))
    })?;
    let image_base64 = base64::engine::general_purpose::STANDARD.encode(image_data);
    Ok(ConversationMessage {
        role: crate::session::MessageRole::User,
        blocks: vec![
            ContentBlock::Text {
                text: format!(
                    "Structured image attachment for the current turn. Source path: {}. Request: {}",
                    image_path.display(),
                    prompt.as_ref()
                ),
            },
            ContentBlock::Image {
                media_type,
                data: image_base64,
                source_path: Some(image_path.display().to_string()),
            },
        ],
        usage: None,
    })
}

fn normalize_image_media_type(path: &Path, media_type: &str) -> Option<String> {
    let media_type = media_type.trim();
    if media_type.starts_with("image/") {
        return Some(media_type.to_string());
    }
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("png") => Some("image/png".to_string()),
        Some("jpg" | "jpeg") => Some("image/jpeg".to_string()),
        Some("gif") => Some("image/gif".to_string()),
        Some("webp") => Some("image/webp".to_string()),
        _ => None,
    }
}

/// Coordinates the model loop, tool execution, hooks, and session updates.
struct RecoveredTurnStrategyIdentity {
    decision_id: String,
    decision_lease: String,
    revision: u64,
    policy_version: String,
    selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
    status: crate::execution_core::TurnStrategyDecisionStatus,
    resource_snapshot: harness_contract::strategy::StrategyResourceSnapshot,
    candidate_estimates: Vec<harness_contract::strategy::ExecutionCandidateEstimate>,
    collaboration_receipt: Option<serde_json::Value>,
    collaboration_obligation: Option<harness_contract::strategy::CollaborationExecutionObligation>,
    focus_partition_plans: Vec<harness_contract::team::FocusPartitionPlan>,
    pattern: harness_contract::core::ExecutionPattern,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionContextProjectionCacheKey {
    session_id: String,
    projection_generation: u64,
    index_revision: u64,
    memory_revision: u64,
    reality_snapshot: String,
    binding_fingerprint: String,
    query_digest: String,
    model_window: u32,
}

#[derive(Debug, Clone)]
struct SessionContextProjectionCacheEntry {
    key: SessionContextProjectionCacheKey,
    items: Vec<ContextItem>,
}

#[derive(Debug, Clone)]
struct SessionMemoryProjection {
    initialized: bool,
    history_revision: u64,
    source_count: usize,
    messages: Arc<Vec<MemMessage>>,
    converted_messages: u64,
    rebuilds: u64,
}

impl Default for SessionMemoryProjection {
    fn default() -> Self {
        Self {
            initialized: false,
            history_revision: 0,
            source_count: 0,
            messages: Arc::new(Vec::new()),
            converted_messages: 0,
            rebuilds: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReadHead {
    pub message_count: usize,
    pub history_revision: u64,
    pub history_bytes: usize,
    pub history_tokens: u64,
    pub updated_at_ms: u64,
    pub model: Option<String>,
}

#[derive(Clone)]
struct SessionProviderWireEvidenceWriter {
    artifacts: Arc<crate::ArtifactStore>,
    session_port: Arc<dyn crate::SessionRuntimeJournalPort>,
}

#[async_trait::async_trait]
impl crate::ProviderWireEvidenceWriter for SessionProviderWireEvidenceWriter {
    async fn persist(
        &self,
        context: &crate::ProviderRequestEvidenceContext,
        evidence: crate::ProviderWireEvidence,
    ) -> Result<(), RuntimeError> {
        let payload = serde_json::to_vec(&serde_json::json!({
            "schema_version": 2,
            "session_id": context.session_id,
            "request_sequence": context.request_sequence,
            "request_compiler_cache_hit": context.request_compiler_cache_hit,
            "budget": context.budget,
            "provider_request": &evidence,
        }))
        .map_err(|error| RuntimeError::new(error.to_string()))?;
        let visibility_scope = format!("session:{}", context.session_id);
        let artifact = self
            .artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "application/vnd.cowd.provider-wire-request+json".to_string(),
                    visibility_scope: visibility_scope.clone(),
                    expected_bytes: Some(payload.len() as u64),
                    original_name: Some(format!(
                        "provider-wire-request-{}-{}.json",
                        context.request_sequence, evidence.request_context.request_id
                    )),
                },
                &payload,
            )
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let staging_owner = format!(
            "staging:provider-request:{}:{}",
            context.session_id, evidence.request_context.request_id
        );
        self.artifacts
            .pin(
                &artifact,
                &staging_owner,
                now_ms().saturating_add(crate::ARTIFACT_STAGING_PIN_TTL_MS),
            )
            .map_err(|error| RuntimeError::new(error.to_string()))?;

        let event = crate::RuntimeSessionEvent::new(
            context.session_id.clone(),
            context.request_sequence,
            crate::RuntimeSessionEventKind::ProviderRequestPacked,
            serde_json::json!({
                "type": "ProviderRequestPacked",
                "schema_version": 2,
                "request_sequence": context.request_sequence,
                "request_id": evidence.request_context.request_id,
                "model": evidence.request_context.profile.model,
                "protocol": evidence.wire_request.protocol,
                "body_sha256": evidence.wire_request.body_sha256,
                "artifact": artifact.clone(),
            }),
            now_ms(),
        )
        .with_ref(crate::RuntimeSessionEventRef {
            ref_type: "artifact".to_string(),
            id: artifact.selector.clone(),
            label: Some("exact provider wire request".to_string()),
        });
        if let Err(error) = self.session_port.append_event(&event).await {
            if let Err(cleanup_error) = self.artifacts.unpin(&artifact, &staging_owner) {
                tracing::warn!(
                    error = %cleanup_error,
                    artifact = %artifact.selector,
                    "provider evidence staging pin cleanup failed"
                );
            }
            if let Err(cleanup_error) = self.artifacts.delete(&artifact, &visibility_scope) {
                tracing::warn!(
                    error = %cleanup_error,
                    artifact = %artifact.selector,
                    "unreferenced provider evidence artifact cleanup failed"
                );
            }
            return Err(RuntimeError::new(error.to_string()));
        }

        let durable_owner = format!(
            "provider-request:{}:{}",
            context.session_id, evidence.request_context.request_id
        );
        if let Err(error) = self.artifacts.pin(
            &artifact,
            &durable_owner,
            crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
        ) {
            let _ = self.artifacts.pin(
                &artifact,
                &staging_owner,
                crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            );
            return Err(RuntimeError::new(error.to_string()));
        }
        if let Err(error) = self.artifacts.unpin(&artifact, &staging_owner) {
            tracing::warn!(
                error = %error,
                artifact = %artifact.selector,
                "provider evidence retained an extra staging pin"
            );
        }
        Ok(())
    }
}

pub struct ConversationRuntime<C, T> {
    session_id: String,
    session: Arc<RwLock<Session>>, // tokio::sync::RwLock
    session_input_stream: crate::session_input::SessionInputStream,
    /// Inputs consumed at a provider checkpoint. The graph-owned host drains
    /// this compact receipt list and converts relevant corrections into Goal
    /// observations/revisions in the same graph commit; raw input remains in
    /// the durable Session store.
    consumed_session_inputs: std::sync::Mutex<Vec<crate::session_input::SessionInputRecord>>,
    api_client: C,
    tool_executor: Arc<T>,
    permission_policy: PermissionPolicy,
    permission_fingerprint: u64,
    system_prompt: Vec<String>,
    usage_tracker: UsageTracker,
    hook_runner: HookRunner,
    cowd_bus: Option<Arc<crate::cowd_event::CowdEventBus>>,
    turn_callback: Option<Arc<TurnCallback>>,
    profiler: crate::context_profiler::ContextProfiler,
    subsystem_budget_ratio_bp: u32,
    session_compaction_config: RuntimeSessionCompactConfig,
    semantic_checkpoint_enabled: bool,
    model_context_window: u32,
    model_context_window_source: provider::ModelContextWindowSource,
    model_context_windows: BTreeMap<String, u32>,
    provider_max_output_override: Option<u32>,
    /// Hard evaluation budget explicitly bound to this Session execution
    /// domain. Root and delegated conversations share the same Arc; unrelated
    /// Sessions never consult ambient process state.
    evaluation_provider_token_lease: Option<Arc<EvaluationProviderTokenLease>>,
    delegated_provider_budget: Option<(
        crate::execution_core::budget::ParentExecutionBudgetLedger,
        harness_contract::context::ChildExecutionBudgetReservation,
    )>,
    calibrated_model_context_windows: std::sync::Mutex<BTreeMap<String, u32>>,
    hook_abort_signal: HookAbortSignal,
    hook_progress_reporter: Arc<std::sync::Mutex<Option<Box<dyn HookProgressReporter + Send>>>>,
    session_tracer: Option<SessionTracer>,
    /// Optional cognitive memory manager – `None` when memory is disabled.
    memory_manager: Option<Arc<CognitiveContextManager>>,
    checkpoint_workspace_id: String,
    execution_identity: Option<harness_contract::execution::ExecutionIdentity>,
    /// Runtime-owned owner for post-turn maintenance tasks.
    maintenance_supervisor:
        Option<Arc<crate::execution_core::services::RuntimeMaintenanceSupervisor>>,
    /// Human-readable memory status message. `None` when healthy; `Some(msg)` when degraded.
    memory_status: Option<String>,
    /// Runtime-owned Fact/Matrix recall boundary for this conversation.  It
    /// is populated only from a compiled Binding, never from a Surface field.
    reality_recall: Option<(
        crate::RealityRecallPort,
        harness_contract::agent::AgentBindingSnapshot,
    )>,
    /// Startup-selected Knowledge adapter. It is cloned from RuntimeServices;
    /// turns never reopen a backend or infer one from the config directory.
    knowledge_activation: Option<KnowledgeActivationRuntime>,
    /// Latest lease-filtered Fact/Matrix recall report, retained for runtime
    /// audit and projections without turning Gateway into a context assembler.
    last_reality_recall_report: std::sync::Mutex<Option<crate::RealityRecallReport>>,
    /// Optional tool callback for real-time visualization (P0-2).
    tool_callback: Option<Arc<dyn ToolCallback>>,
    /// Optional Gateway-owned Session application port for durable Runtime
    /// evidence and context records.
    session_journal_port: Option<Arc<dyn crate::SessionRuntimeJournalPort>>,
    /// Authorized durable history reader used for current-Session context
    /// navigation. It never broadens retrieval to another Session implicitly.
    session_history_reader: Option<Arc<session::SessionHistoryReader>>,
    /// Shared process-local hot plane. Current-Session context is cold-paged
    /// from the durable reader once and then queried here without database I/O.
    hot_state: Option<Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>>,
    session_context_projection_cache: std::sync::Mutex<Option<SessionContextProjectionCacheEntry>>,
    /// Incremental user/assistant text projection consumed by Memory recall.
    /// Session history remains canonical; this avoids rebuilding its full
    /// converted form on every turn.
    session_memory_projection: tokio::sync::Mutex<SessionMemoryProjection>,
    memory_context_revision: AtomicU64,
    current_context_cache_hit: AtomicBool,
    current_context_source_latency_ms: std::sync::Mutex<BTreeMap<String, u64>>,
    /// Runtime-selected Artifact plane shared by attachments and raw evidence.
    artifact_store: Option<Arc<crate::ArtifactStore>>,
    /// Durable execution lifecycle store. Session-domain events never use it.
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    /// Sole durable Outcome writer supplied by RuntimeServices.
    outcome_service: Option<Arc<crate::execution_core::OutcomeService>>,
    /// Immutable, asynchronously maintained Outcome read projection.
    outcome_projector: Option<Arc<crate::OutcomeProjector>>,
    routing_mode: crate::RoutingMode,
    runtime_config_revision: String,
    active_provider_identity: std::sync::Mutex<Option<harness_contract::outcome::ProviderIdentity>>,
    provider_selection_receipt: std::sync::Mutex<Option<crate::ProviderSelectionReceipt>>,
    /// Optional event log for time-travel debugging and session rebuild.
    event_log: Option<std::sync::Mutex<SessionEventLog>>,
    /// Runtime-local searchable index for oversized tool outputs.
    tool_output_sandbox: Option<Arc<std::sync::Mutex<memory::ToolOutputSandbox>>>,
    /// Optional SSE callback for real-time streaming events to WebUI.
    /// Receives pre-formatted JSON event strings.
    sse_callback: Option<Arc<dyn Fn(String) + Send + Sync>>,
    /// Optional memory lifecycle callback for TUI memory events.
    memory_callback: Option<Arc<dyn MemoryCallback>>,
    /// Runtime-owned approval policy, durable queue, Grant registry and waiter.
    approval_coordinator: Option<Arc<crate::ApprovalCoordinator>>,
    /// Skill capability profiles already inspected by the Skill asset layer and
    /// visible to this runtime.
    skill_profiles: Vec<SkillCapabilityProfile>,
    /// Agent-scoped Skill visibility and adapter policy.
    agent_skill_profile: AgentSkillProfile,
    /// Gateway-inspected PromptOnly assets keyed by Skill identity. Runtime
    /// chooses among these assets but never discovers or reads packages.
    skill_prompt_assets: Vec<RuntimeSkillPromptAsset>,
    skill_instruction_source: Option<Arc<dyn crate::RuntimeSkillInstructionSource>>,
    /// Immutable identity supplied by Runtime for memory operations. A child
    /// Agent Run uses its Binding instance ID rather than the primary-turn
    /// placeholder, so concurrent instances do not share an ambient author.
    memory_agent_id: String,
    /// Optional reusable Definition lineage for Binding-scoped memory recall.
    memory_definition_lineage_id: Option<String>,
    /// Optional Team visibility boundary supplied by the Agent Binding.
    memory_team_id: Option<String>,
    /// Explicit Binding lease for memory recall. Both primary conversations
    /// and child Agents receive only the scopes in their compiled Binding.
    memory_read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    /// P2-2: Current project phase (Discovery→Planning→Building→Reviewing→Shipping→Graduated).
    project_phase: String,
    /// Current model ID (used for provider fallback chain lookup).
    model: Option<String>,
    /// RuntimeServices-owned provider fallback policy. A turn snapshots this
    /// list once before dispatch, so config reloads affect subsequent turns
    /// without changing an in-flight candidate order.
    fallbacks: Arc<std::sync::RwLock<Vec<String>>>,
    /// T35: Cancellation token for graceful shutdown.
    cancellation_token: CancellationToken,
    /// Latest assembled context envelope used by a real turn.
    last_context_envelope: std::sync::Mutex<Option<ContextEnvelope>>,
    /// Active context profile used to assemble the next runtime envelope.
    context_profile: std::sync::Mutex<ContextProfile>,
    /// Effective runtime control policy loaded from configuration.
    runtime_control_policy: RuntimeControlPolicy,
    /// Runtime-owned context supplied by outer orchestration layers.
    external_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// One-shot instructions injected by an owner checkpoint for exactly the
    /// next provider request. They become part of that request's durable
    /// context envelope, but never mutate the user transcript or leak into
    /// unrelated later model steps.
    next_model_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// One governed checkpoint can require a conclusion from evidence already
    /// held by the turn. It affects only the next provider request.
    next_model_text_only: AtomicBool,
    /// One governed checkpoint can narrow exactly the next provider request to
    /// a named subset of already-discovered tools. This never grants a tool or
    /// widens its permission/resource lease, and normal exposure is restored
    /// after the request.
    next_model_tool_allowlist: std::sync::Mutex<Option<BTreeSet<String>>>,
    /// The narrowed tool set represents an already-admitted business
    /// obligation and one actual call is required on the next request.
    next_model_tool_required: AtomicBool,
    /// A stricter one-shot provider selection for a governed native action.
    /// It is only used after Runtime has reduced the eligible schema set to a
    /// single control-plane action; it never encodes a Team topology.
    next_model_required_tool_name: std::sync::Mutex<Option<String>>,
    /// A successful tool_search activation creates a one-request execution
    /// handoff. The following automatic provider request receives the newly
    /// activated schemas but temporarily hides tool_search so discovery cannot
    /// loop in place. Normal discovery visibility resumes afterwards.
    next_model_tool_activation_notice: std::sync::Mutex<Option<BTreeSet<String>>>,
    /// One governed checkpoint can lower the cognitive budget of exactly one
    /// provider request after deterministic evidence acquisition is complete.
    next_model_reasoning_effort: std::sync::Mutex<Option<String>>,
    /// Bounded short-term tool trace context for subsequent turns.
    tool_trace_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// Governance observations produced by tool calls in the active turn.
    turn_tool_observations: std::sync::Mutex<Vec<ToolObservation>>,
    /// Stable projections emitted by the sole governed Tool compiler during
    /// the active turn. Finalization forwards these exact plan identities to
    /// harness, policy, evidence, and growth receipts.
    turn_governed_tool_plans:
        std::sync::Mutex<Vec<harness_contract::tool::GovernedToolPlanProjection>>,
    /// Sole strategy identity for the admitted turn. Host creates it before
    /// graph compilation; every later checkpoint reads or revises this state.
    active_turn_strategy:
        std::sync::Mutex<Option<crate::execution_core::TurnStrategyDecisionState>>,
    /// Revisioned tool set visible to the next provider request.
    tool_exposure_state: std::sync::Mutex<Option<ToolExposureState>>,
    /// Turn-local Tool exposure evidence and provider-account circuit state.
    /// The circuit survives Host model-node replans and resets at the next
    /// turn epoch together with request metrics.
    turn_tool_exposure_metrics: std::sync::Mutex<TurnProviderState>,
    /// Tools coupled to the PromptOnly Skill selected for the active turn.
    /// Runtime folds these into the first provider exposure so Skill guidance
    /// and its executable capability arrive atomically.
    active_skill_tool_refs: std::sync::Mutex<BTreeSet<String>>,
    /// Provider visibility changes must be monotonically ordered. A governed
    /// text-only checkpoint temporarily withdraws every schema; the next
    /// normal model step must be able to restore the catalog rather than be
    /// rejected as an older projection by the provider client.
    tool_exposure_revision: AtomicU64,
    request_compiler: crate::PreparedRequestCompiler,
    /// Actual Provider wire-prefix and native-cache evidence for the active turn.
    turn_stable_prefix_metrics: std::sync::Mutex<TurnStablePrefixMetrics>,
    /// Stable evidence projections emitted during the active turn.
    turn_evidence_audits: std::sync::Mutex<Vec<EvidenceAuditProjection>>,
    /// Exact receipts generated in this turn. These contain metadata and
    /// digests only; canonical bytes remain in Session/artifact storage.
    turn_generated_model_receipts: std::sync::Mutex<Vec<GeneratedModelReceipt>>,
    /// Generated receipts promoted only after their byte-identical ToolResult
    /// appears in a valid, committed Provider continuation.
    turn_model_observations:
        std::sync::Mutex<Vec<harness_contract::context::ProviderModelObservationAttestation>>,
    /// Per-turn component accounting and tool-result lease consumption.
    turn_context_ledger: std::sync::Mutex<crate::context_ledger::ContextLedger>,
    /// Latest context governance report emitted by a completed turn.
    last_context_turn_report: std::sync::Mutex<Option<ContextTurnReport>>,
    /// Compaction is decided before a provider request but reported with the
    /// completed turn so UI/audit consumers retain a single turn receipt.
    turn_preflight_compaction: std::sync::Mutex<Option<AutoCompactionEvent>>,
    /// Knowledge activation report prepared from the active memory packet.
    turn_knowledge_report:
        std::sync::Mutex<Option<harness_contract::knowledge::KnowledgeTurnReport>>,
    /// Sole bounded execution boundary for synchronous Tool implementations.
    tool_execution_plane: Arc<crate::ToolExecutionPlane>,
    authorization_negotiator: crate::AuthorizationNegotiator,
    /// Every root and delegated Conversation shares the Runtime Provider
    /// admission owner. AgentTask leases govern Agent slots, not Provider
    /// transport, so child conversations must not bypass this boundary.
    provider_admission: Option<Arc<crate::execution_core::graph::ExecutionResourceManager>>,
    provider_resource_config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
    /// Runtime-owned service class propagated from the durable graph/Agent
    /// binding. Models and surfaces cannot promote this value.
    execution_service_class: crate::execution_core::graph::ExecutionServiceClass,
    /// Maximum duration for a single tool execution. `None` means no timeout.
    tool_timeout: Option<Duration>,
    /// Only the root user turn may turn an explicit team requirement into a
    /// mandatory orchestration call. Delegated AgentTask turns retain their
    /// inherited wording as context but remain leaf protocol work unless a
    /// future packet explicitly grants subdelegation.
    explicit_team_escalation: bool,
    /// Runtime-issued absolute model-step ceiling for the next and subsequent
    /// turns owned by this host. `0` means derive the normal main-turn lease.
    /// Delegated Agent workers bind this from their immutable budget packet.
    model_step_limit_override: AtomicUsize,
    /// Runtime-owned Focus policy copied from the immutable AgentTaskPacket.
    /// `0` disables the delegated novelty gate for normal root turns.
    delegated_focus_novelty_target_bp: AtomicU64,
    delegated_focus_acceptance_scopes: std::sync::Mutex<Vec<String>>,
    delegated_focus_required_output_fields: std::sync::Mutex<Vec<String>>,
    /// Present only for a Gateway-owned durable Session ingress. It fences
    /// provider/tool/terminal side effects against generation and claim loss.
    session_execution_fence: Option<crate::SessionExecutionFence>,
}

pub(crate) enum ToolAuthorizationDecision {
    Authorized(crate::ToolExecutionPolicyDecision),
    Gap {
        assessment: harness_contract::policy::CapabilityAssessment,
        effective: crate::EffectiveToolAuthorizationDescriptor,
    },
}

struct ConversationGovernedToolContext<'a, C, T> {
    runtime: &'a ConversationRuntime<C, T>,
    pending_tool_uses: &'a [(String, String, String)],
    prompter: &'a crate::permissions::SharedPrompter,
    iterations: usize,
    plan_id: &'a str,
    plan_revision: u64,
}

impl<C, T> GovernedToolExecutionContext for ConversationGovernedToolContext<'_, C, T>
where
    C: ApiClient + Sync,
    T: ToolExecutor,
{
    type Output = (ConversationMessage, Option<String>);
    type Admission = Option<crate::ToolExecutionAdmission>;
    type Receipt = (ConversationMessage, Option<String>);

    fn local_ceiling(&self) -> usize {
        crate::governed_tool_plan::default_parallel_tool_concurrency()
    }

    fn is_cancelled(&self) -> bool {
        self.runtime.cancellation_token.is_cancelled()
    }

    fn wait_for_cancellation(&self) -> GovernedToolFuture<'_, ()> {
        Box::pin(self.runtime.cancellation_token.cancelled())
    }

    fn try_admit<'a>(
        &'a self,
        _task: &'a crate::governed_tool_plan::GovernedToolPlanTask,
    ) -> GovernedToolFuture<'a, GovernedToolAdmission<Self::Admission>> {
        Box::pin(async { GovernedToolAdmission::Granted(None) })
    }

    fn execute<'a>(
        &'a self,
        task: &'a crate::governed_tool_plan::GovernedToolPlanTask,
        admission: &'a mut Self::Admission,
    ) -> GovernedToolFuture<'a, Result<Self::Output, String>> {
        Box::pin(async move {
            let input = self
                .pending_tool_uses
                .get(task.original_call_index)
                .map(|(_, _, input)| input.as_str())
                .ok_or_else(|| {
                    format!(
                        "governed tool task `{}` references missing original call index {}",
                        task.tool_call_id, task.original_call_index
                    )
                })?;
            self.runtime
                .execute_single_tool(
                    task,
                    self.plan_id,
                    self.plan_revision,
                    input,
                    self.prompter,
                    self.iterations,
                    admission,
                )
                .await
                .map(|message| self.runtime.collect_tool_result_message(message).1)
                .map_err(|error| error.to_string())
        })
    }

    fn classify_output(&self, output: &Self::Output) -> Result<(), String> {
        if conversation_tool_result_is_error(&output.0) {
            Err(conversation_tool_result_text(&output.0))
        } else {
            Ok(())
        }
    }

    fn commit_terminal<'a>(
        &'a self,
        task: &'a crate::governed_tool_plan::GovernedToolPlanTask,
        terminal: &'a GovernedToolTaskTerminal<Self::Output>,
    ) -> GovernedToolFuture<'a, Result<Self::Receipt, String>> {
        Box::pin(async move {
            match terminal {
                GovernedToolTaskTerminal::Succeeded(output)
                | GovernedToolTaskTerminal::FailedOutput { output, .. } => Ok(output.clone()),
                _ => {
                    let message = ConversationMessage::tool_result(
                        task.tool_call_id.clone(),
                        task.tool_name.clone(),
                        conversation_tool_terminal_reason(terminal),
                        true,
                    );
                    self.runtime
                        .session
                        .write()
                        .await
                        .push_message(message.clone())
                        .map_err(|error| error.to_string())?;
                    let sequence = self
                        .runtime
                        .session_head()
                        .await
                        .message_count
                        .wrapping_sub(1);
                    self.runtime.record_message_event(&message, sequence);
                    Ok(self.runtime.collect_tool_result_message(message).1)
                }
            }
        })
    }

    fn on_task_started(&self, task: &crate::governed_tool_plan::GovernedToolPlanTask) {
        let input = self
            .pending_tool_uses
            .get(task.original_call_index)
            .map_or("", |(_, _, input)| input.as_str());
        self.runtime.emit_tool_started(
            &task.tool_call_id,
            &task.tool_name,
            input,
            &task.depends_on,
        );
    }

    fn on_task_terminal(
        &self,
        task: &crate::governed_tool_plan::GovernedToolPlanTask,
        terminal: &GovernedToolTaskTerminal<Self::Output>,
        receipt: Option<&Self::Receipt>,
    ) {
        let (summary, failed) = receipt.map_or_else(
            || (conversation_tool_terminal_reason(terminal), true),
            |(message, _)| {
                (
                    conversation_tool_result_text(message),
                    conversation_tool_result_is_error(message),
                )
            },
        );
        self.runtime.emit_tool_completed(
            &task.tool_call_id,
            &task.tool_name,
            &summary,
            Some(i32::from(failed)),
            &task.depends_on,
        );
    }
}

enum MemoryManagerComposition {
    Automatic,
    HostSelected(Option<Arc<CognitiveContextManager>>),
}

fn close_controlled_recovery_gap(
    assessment: &mut harness_contract::policy::CapabilityAssessment,
    reason: &str,
    evidence: &str,
) {
    if let Some(gap) = assessment.gap.as_mut() {
        gap.recoverable = false;
        gap.reason.push_str("; ");
        gap.reason.push_str(reason);
    }
    assessment.evidence_refs.push(evidence.to_string());
}

fn turn_strategy_status_name(
    status: crate::execution_core::TurnStrategyDecisionStatus,
) -> &'static str {
    use crate::execution_core::TurnStrategyDecisionStatus;
    match status {
        TurnStrategyDecisionStatus::Selected => "selected",
        TurnStrategyDecisionStatus::Running => "running",
        TurnStrategyDecisionStatus::Downgraded => "downgraded",
        TurnStrategyDecisionStatus::EarlyStopped => "early_stopped",
        TurnStrategyDecisionStatus::Partial => "partial",
        TurnStrategyDecisionStatus::WaitingExternalDecision => "waiting_external_decision",
        TurnStrategyDecisionStatus::Completed => "completed",
        TurnStrategyDecisionStatus::Cancelled => "cancelled",
        TurnStrategyDecisionStatus::Failed => "failed",
    }
}

fn turn_strategy_event_kind_allowed(kind: &str) -> bool {
    matches!(
        kind,
        "runtime.strategy.selected"
            | "runtime.strategy.downgraded"
            | "runtime.strategy.early_stopped"
            | "runtime.strategy.outcome"
    )
}

/// Initialise standalone Memory when no embedding composition root exists.
#[must_use]
fn initialize_automatic_memory_manager(
    feature_config: &RuntimeFeatureConfig,
    budget_plan: &RuntimeBudgetPlan,
) -> (Option<Arc<CognitiveContextManager>>, Option<String>) {
    let mem_cfg = build_cc_memory_config_with_budget(feature_config, budget_plan);
    let llm_summarizer = mem_cfg
        .compression
        .llm
        .is_configured()
        .then(|| {
            let registry = Arc::new(
                crate::ProviderRegistry::new(feature_config.providers().clone())
                    .map_err(|rejected| rejected.diagnostics.errors.join("; "))?,
            );
            crate::RuntimeMemorySummarizer::new(
                registry,
                Arc::new(crate::ProviderTransportPool::default()),
                Arc::new(crate::ProviderClientTemplateCache::default()),
                mem_cfg.compression.llm.model.clone(),
                2048,
            )
        })
        .transpose();
    let llm_summarizer = match llm_summarizer {
        Ok(summarizer) => summarizer.map(|summarizer| {
            Arc::new(summarizer) as Arc<dyn memory::compression::llm_summarizer::LlmSummarizer>
        }),
        Err(error) => {
            tracing::warn!(%error, "standalone Memory summarizer unavailable; using fallback");
            None
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(_) => {
            // Standalone callers inside a runtime need a separate runtime to
            // avoid nested enter_runtime. Host-composed callers never enter
            // this branch because they inject their selected owner directly.
            let handle = std::thread::spawn(move || -> Result<_, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("failed to create memory init runtime: {error}"))?;
                rt.block_on(open_automatic_memory_manager(mem_cfg, llm_summarizer))
                    .map_err(|error| error.to_string())
            });
            match handle.join() {
                Ok(Ok(manager)) => {
                    tracing::debug!(
                        "memory: standalone CognitiveContextManager initialised with explicit per-turn identity"
                    );
                    (Some(Arc::new(manager)), None)
                }
                Ok(Err(error)) => automatic_memory_initialization_failure(error),
                Err(_) => {
                    let message = "Memory system unavailable: initialization thread panicked. \
                                   Context will NOT persist between turns."
                        .to_string();
                    tracing::error!("{message}");
                    (None, Some(message))
                }
            }
        }
        Err(_) => match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                match runtime.block_on(open_automatic_memory_manager(mem_cfg, llm_summarizer)) {
                    Ok(manager) => {
                        tracing::debug!(
                        "memory: standalone CognitiveContextManager initialised with explicit per-turn identity"
                    );
                        (Some(Arc::new(manager)), None)
                    }
                    Err(error) => automatic_memory_initialization_failure(error.to_string()),
                }
            }
            Err(error) => {
                let message = format!(
                    "Memory system unavailable: failed to create runtime: {error}. \
                     Memory features will NOT work."
                );
                tracing::error!("{message}");
                (None, Some(message))
            }
        },
    }
}

async fn open_automatic_memory_manager(
    config: memory::config::MemoryConfig,
    llm_summarizer: Option<Arc<dyn memory::compression::llm_summarizer::LlmSummarizer>>,
) -> Result<CognitiveContextManager, memory::MemoryError> {
    match llm_summarizer {
        Some(summarizer) => CognitiveContextManager::new_with_summarizer(config, summarizer).await,
        None => CognitiveContextManager::new(config).await,
    }
}

fn automatic_memory_initialization_failure(
    error: impl std::fmt::Display,
) -> (Option<Arc<CognitiveContextManager>>, Option<String>) {
    let message = format!(
        "Memory system unavailable: {error}. Context will NOT persist between turns. \
         Check your memory store paths, vector API credentials, and ~/.cowd/memory/ directory."
    );
    tracing::error!("{message}");
    (None, Some(message))
}

/// Convert a [`RuntimeFeatureConfig`] memory section into a [`CcMemoryConfig`]
/// suitable for [`CognitiveContextManager::new`].
#[doc(alias = "memory")]
#[doc(alias = "CognitiveContextManager")]
#[must_use]
pub fn build_cc_memory_config(feature_config: &RuntimeFeatureConfig) -> CcMemoryConfig {
    let model = feature_config.resolved_model();
    let model_context_window = model.as_deref().map_or(0, |model| {
        provider::model_context_window_with_overrides(
            model,
            Some(feature_config.model_context_windows()),
        )
    });
    let model_max_output = model.as_deref().map_or(0, |model| {
        provider_output_budget_hint(
            model,
            model_context_window,
            feature_config
                .provider_resources()
                .max_output_tokens_override(),
        )
    });
    let ratio_bp =
        clamp_context_budget_ratio_bp(feature_config.context_budget().subsystem_budget_ratio_bp);
    let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
        model_context_window,
        model_max_output_tokens: model_max_output,
        subsystem_budget_ratio_bp: ratio_bp,
        profile: ContextProfile::MainTurn,
        autonomy_mode: None,
        expected_parallel_branches: 1,
        expected_verification_passes: 0,
    });
    build_cc_memory_config_with_budget(feature_config, &plan)
}

pub fn build_cc_memory_config_with_budget(
    feature_config: &RuntimeFeatureConfig,
    budget_plan: &RuntimeBudgetPlan,
) -> CcMemoryConfig {
    use memory::config::{
        BudgetConfig, CompressionConfig, DriftConfig, ExtractorConfig, StoreConfig,
    };

    let mem = feature_config.memory();
    let config_home = crate::cowd_dirs::config_home_dir();
    let (sqlite_path, blob_dir) = if let Some(store_path) = mem.store_path.as_ref() {
        (store_path.join("memory.db"), store_path.join("blobs"))
    } else {
        let registry = storage::StorageRegistry::default_for_config_home(&config_home);
        let sqlite_path = registry
            .endpoint(&storage::StorageDomainId::Memory)
            .map(|endpoint| endpoint.as_handle().path)
            .unwrap_or_else(|_| registry.layout.root.join("memory.sqlite"));
        let blob_dir = registry
            .endpoint(&storage::StorageDomainId::Blobs)
            .map(|endpoint| endpoint.as_handle().path)
            .unwrap_or_else(|_| registry.layout.blobs.clone());
        (sqlite_path, blob_dir)
    };

    let mut vector_config = memory::config::VectorConfig {
        enabled: mem.vector.enabled,
        model: mem.vector.model.clone(),
        api_url: mem.vector.api_url.clone(),
        api_key: mem.vector.api_key.clone(),
        dimension: mem.vector.dimension,
        timeout_secs: mem.vector.timeout_secs,
        batch_size: mem.vector.batch_size,
        max_input_tokens: mem.vector.max_input_tokens,
    };
    if vector_config.enabled
        && !vector_config.model.trim().is_empty()
        && vector_config.api_url.trim().is_empty()
    {
        if let Some(provider) = feature_config
            .providers()
            .resolve_full(&vector_config.model)
        {
            if !matches!(
                model_protocol::provider_config::ProviderProtocol::effective_for_provider(provider),
                Ok(model_protocol::provider_config::ProviderProtocol::Anthropic) | Err(_)
            ) && !provider.base_url.trim().is_empty()
            {
                vector_config.api_url =
                    format!("{}/embeddings", provider.base_url.trim_end_matches('/'));
                if vector_config.api_key.trim().is_empty() {
                    vector_config.api_key = provider.api_key.clone();
                }
            }
        }
    }

    let mut llm_config = memory::config::LlmSummarizerConfig::default();
    let explicit_llm = &feature_config.compression().llm;
    if explicit_llm.is_configured() {
        llm_config.enabled = true;
        llm_config.model = explicit_llm.model.clone();
    } else if mem.extraction.auto_extract {
        if let Some(model) = feature_config.resolved_model() {
            llm_config.enabled = true;
            llm_config.model = model;
        }
    }

    CcMemoryConfig {
        store: StoreConfig {
            sqlite_path,
            blob_dir,
            enable_vector_index: mem.store_enable_vector_index && mem.vector.enabled,
            cache_capacity: 512,
            vector: vector_config,
        },
        compression: CompressionConfig {
            micro_threshold: 50,
            session_threshold: 10,
            enable_deep_compression: feature_config.compression().deep.enabled,
            aggressiveness: 0.5,
            llm: llm_config,
        },
        budget: BudgetConfig {
            context_window: budget_plan.memory_retrieval_budget.context_window,
            reserved_system: u64::from(mem.layers.l1_max_tokens)
                + u64::from(mem.layers.l2_max_tokens),
            reserved_response: budget_plan.memory_retrieval_budget.reserved_response,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
            runtime_managed: false,
            l0_reserved: 0,
            l1_working: 0,
            l2_project: 0,
            l3_deep: 0,
            l3_checkpoint: 0,
            l4_shared: 0,
        },
        layers: memory::config::LayerConfig {
            l0_enabled: mem.layers.l0_enabled,
            l1_max_tokens: mem.layers.l1_max_tokens,
            l2_max_tokens: mem.layers.l2_max_tokens,
            l3_search_limit: mem.layers.l3_search_limit,
            l4_enabled: mem.layers.l4_enabled,
        },
        extractor: ExtractorConfig {
            enabled: mem.extraction.auto_extract,
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
            extractor_debounce_secs: 30,
        },
        governance: memory::config::GovernanceConfig {
            enabled: mem.governance.enabled,
            startup_delay_secs: mem.governance.startup_delay_secs,
            deep_scan_hour_local: mem.governance.deep_scan_hour_local,
            max_candidates: mem.governance.max_candidates,
            stale_threshold_bp: mem.governance.stale_threshold_bp,
            low_confidence_threshold_bp: mem.governance.low_confidence_threshold_bp,
        },
        drift: DriftConfig::default(),
        perf: memory::config::PerfBudget::default(),
        tuning: Default::default(),
        identity: memory::config::MemoryIdentityConfig {
            role: mem.identity.role.clone(),
            language: mem.identity.language.clone(),
        },
    }
}

fn extract_tool_info(msg: &ConversationMessage) -> (String, String) {
    if let Some(ContentBlock::ToolResult {
        tool_use_id,
        tool_name,
        ..
    }) = msg.blocks.first()
    {
        (tool_use_id.clone(), tool_name.clone())
    } else {
        (String::new(), String::new())
    }
}

fn conversation_tool_result_is_error(message: &ConversationMessage) -> bool {
    message
        .blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))
}

fn conversation_tool_result_text(message: &ConversationMessage) -> String {
    message
        .blocks
        .iter()
        .find_map(|block| match block {
            ContentBlock::ToolResult { output, .. } => Some(output.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "tool returned an error without a result payload".to_string())
}

fn conversation_tool_terminal_reason(
    terminal: &GovernedToolTaskTerminal<(ConversationMessage, Option<String>)>,
) -> String {
    match terminal {
        GovernedToolTaskTerminal::Succeeded(_) => "tool completed".to_string(),
        GovernedToolTaskTerminal::FailedOutput { error, .. }
        | GovernedToolTaskTerminal::Failed { error } => error.clone(),
        GovernedToolTaskTerminal::Refused { reason }
        | GovernedToolTaskTerminal::Cancelled { reason }
        | GovernedToolTaskTerminal::Panicked { reason } => reason.clone(),
        GovernedToolTaskTerminal::Blocked {
            predecessor_id,
            reason,
        } => format!("blocked by predecessor `{predecessor_id}`: {reason}"),
    }
}

fn compacted_source_messages(
    messages: &[ConversationMessage],
    start: usize,
    end_exclusive: usize,
) -> &[ConversationMessage] {
    let start = start.min(messages.len());
    let end_exclusive = end_exclusive.min(messages.len()).max(start);
    &messages[start..end_exclusive]
}

fn source_message_evidence_refs(
    session_id: &str,
    messages: &[ConversationMessage],
    start: usize,
    end_exclusive: usize,
) -> Vec<EvidenceRef> {
    let start = start.min(messages.len());
    let end_exclusive = end_exclusive.min(messages.len()).max(start);
    messages[start..end_exclusive]
        .iter()
        .enumerate()
        .map(|(offset, message)| {
            let index = start + offset;
            EvidenceRef::observed("session-message", format!("{session_id}:{index}"))
                .with_source(message_index_label(message))
        })
        .collect()
}

fn deterministic_checkpoint_id(
    session_id: &str,
    message_start: usize,
    message_end: usize,
    previous_summary: Option<&str>,
) -> String {
    // This identifier crosses process boundaries through durable events, so
    // it cannot use DefaultHasher (whose algorithm is not a persistence
    // contract). Length-prefix every field to avoid ambiguous concatenation.
    let mut hasher = Sha256::new();
    let message_start_bytes = message_start.to_le_bytes();
    let message_end_bytes = message_end.to_le_bytes();
    for field in [
        session_id.as_bytes(),
        message_start_bytes.as_slice(),
        message_end_bytes.as_slice(),
        previous_summary.unwrap_or_default().as_bytes(),
    ] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field);
    }
    format!("checkpoint-{session_id}-{:x}", hasher.finalize())
}

fn conversation_messages_to_mem_messages(messages: &[ConversationMessage]) -> Vec<MemMessage> {
    messages
        .iter()
        .enumerate()
        .map(|(idx, msg)| {
            let role = match msg.role {
                crate::session::MessageRole::System => MemMessageRole::System,
                crate::session::MessageRole::User => MemMessageRole::User,
                crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                crate::session::MessageRole::Tool => MemMessageRole::Tool,
            };
            let content = msg
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    ContentBlock::ReasoningSummary { text } => {
                        Some(format!("[reasoning summary]\n{text}"))
                    }
                    ContentBlock::Image {
                        media_type,
                        source_path,
                        ..
                    } => Some(format!(
                        "[image media_type={} source_path={}]",
                        media_type,
                        source_path.as_deref().unwrap_or("<inline>")
                    )),
                    // Private Provider reasoning is never copied into Memory.
                    ContentBlock::Thinking { .. } => None,
                    ContentBlock::ToolUse { id, name, input } => {
                        Some(format!("[tool_use id={id} name={name}]\n{input}"))
                    }
                    ContentBlock::ToolResult {
                        tool_use_id,
                        tool_name,
                        output,
                        is_error,
                    } => Some(format!(
                        "[tool_result id={tool_use_id} name={tool_name} error={is_error}]\n{output}"
                    )),
                })
                .collect::<Vec<_>>()
                .join("\n\n");
            let (tool_use_id, tool_name) = msg.blocks.iter().fold((None, None), |acc, block| {
                if let ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    ..
                } = block
                {
                    (Some(tool_use_id.clone()), Some(tool_name.clone()))
                } else {
                    acc
                }
            });
            MemMessage {
                turn_index: idx,
                role,
                content,
                tool_use_id,
                tool_name,
                pinned: false,
            }
        })
        .collect()
}

fn conversation_messages_to_context_mem_messages(
    messages: &[ConversationMessage],
    start_index: usize,
) -> Vec<MemMessage> {
    messages
        .iter()
        .enumerate()
        .map(|(offset, message)| {
            let role = match message.role {
                crate::session::MessageRole::User => MemMessageRole::User,
                crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                crate::session::MessageRole::Tool => MemMessageRole::Tool,
                crate::session::MessageRole::System => MemMessageRole::User,
            };
            let content = message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ");
            let (tool_use_id, tool_name) = match message.role {
                crate::session::MessageRole::Tool => {
                    let tool_use_id = message.blocks.iter().find_map(|block| match block {
                        ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.clone()),
                        _ => None,
                    });
                    let tool_name = message.blocks.iter().find_map(|block| match block {
                        ContentBlock::ToolResult { tool_name, .. } if !tool_name.is_empty() => {
                            Some(tool_name.clone())
                        }
                        _ => None,
                    });
                    (tool_use_id, tool_name)
                }
                _ => (None, None),
            };
            MemMessage {
                turn_index: start_index + offset,
                role,
                content,
                tool_use_id,
                tool_name,
                pinned: false,
            }
        })
        .collect()
}

fn is_append_only_projection(
    initialized: bool,
    previous_revision: u64,
    previous_count: usize,
    current_revision: u64,
    current_count: usize,
) -> bool {
    initialized
        && current_count >= previous_count
        && current_revision.wrapping_sub(previous_revision)
            == current_count.saturating_sub(previous_count) as u64
}

fn conversation_message_text(message: &ConversationMessage) -> String {
    message
        .blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn current_turn_messages<'a>(
    messages: &'a [ConversationMessage],
    user_input: &str,
) -> &'a [ConversationMessage] {
    let requested = user_input.trim();
    let turn_start = messages
        .iter()
        .rposition(|message| {
            message.role == crate::session::MessageRole::User
                && !requested.is_empty()
                && conversation_message_text(message).trim() == requested
        })
        .or_else(|| {
            messages
                .iter()
                .rposition(|message| message.role == crate::session::MessageRole::User)
        })
        .unwrap_or(0);
    &messages[turn_start..]
}

fn context_query_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| {
            character.is_whitespace()
                || matches!(
                    character,
                    ',' | '.'
                        | ';'
                        | ':'
                        | '!'
                        | '?'
                        | '，'
                        | '。'
                        | '；'
                        | '：'
                        | '！'
                        | '？'
                        | '('
                        | ')'
                        | '（'
                        | '）'
                )
        })
        .map(str::trim)
        .filter(|term| term.chars().count() >= 2)
        .map(str::to_lowercase)
        .collect()
}

fn context_text_relevance(text: &str, query_terms: &[String]) -> f32 {
    if query_terms.is_empty() {
        return 0.0;
    }
    let searchable = text.to_lowercase();
    let matches = query_terms
        .iter()
        .filter(|term| searchable.contains(term.as_str()))
        .count();
    matches as f32 / query_terms.len() as f32
}

fn session_message_context_text(content_json: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(content_json) else {
        return String::new();
    };
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn revalidate_context_binding(
    current_session_id: &str,
    items: Vec<ContextItem>,
) -> (Vec<ContextItem>, Vec<ContextOmission>) {
    let expected_prefix = format!("session://{current_session_id}/");
    let mut selected = Vec::with_capacity(items.len());
    let mut omitted = Vec::new();
    for item in items {
        let session_scoped = item.source == ContextSourceKind::Conversation
            && item.source_lifecycle == crate::context_runtime::ContextSourceLifecycle::Session;
        let valid = !session_scoped
            || item
                .evidence
                .iter()
                .all(|reference| reference.starts_with(&expected_prefix));
        if valid {
            selected.push(item);
        } else {
            omitted.push(ContextOmission {
                source: item.source,
                reason: "final Binding fence rejected cross-Session candidate".to_string(),
                token_estimate: item.token_estimate,
            });
        }
    }
    (selected, omitted)
}

fn message_index_label(message: &ConversationMessage) -> String {
    let mut checksum = 0_u64;
    for byte in message
        .blocks
        .iter()
        .flat_map(|block| format!("{block:?}").into_bytes())
    {
        checksum = checksum.wrapping_mul(31).wrapping_add(u64::from(byte));
    }
    format!("{}:{checksum:x}", message.role.role_str())
}

fn memory_project_id_for_session(session: &Session) -> Option<String> {
    let root = session.workspace_root.clone()?;
    Some(memory_project_id_for_workspace(&root))
}

/// Derive the Memory project scope used by every Runtime entry for a workspace.
///
/// Active context tools use this function as well, so passive injection and
/// model-initiated retrieval cannot drift into different project namespaces.
#[must_use]
pub fn memory_project_id_for_workspace(root: &Path) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root.display().to_string().hash(&mut hasher);
    let hash = hasher.finish();
    let name = root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workspace")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{name}-{hash:016x}")
}

fn fact_extraction_trigger_for_turn(
    user_input: &str,
    profile: ContextProfile,
) -> Option<RuntimeFactExtractionTrigger> {
    if profile == ContextProfile::DeepInvestigation {
        return Some(RuntimeFactExtractionTrigger::DeepInvestigation);
    }
    let lowered = user_input.to_ascii_lowercase();
    let normalized = user_input.replace(char::is_whitespace, " ");
    let contains = |needle: &str| lowered.contains(needle) || normalized.contains(needle);
    if [
        "FACT:",
        "事实",
        "记忆",
        "规则",
        "原则",
        "约定",
        "偏好",
        "冲突",
        "矛盾",
        "更新",
        "remember",
        "rule",
        "preference",
        "conflict",
        "contradiction",
    ]
    .iter()
    .any(|needle| contains(needle))
    {
        return Some(RuntimeFactExtractionTrigger::TurnEnd);
    }
    None
}

fn count_failed_tool_results(messages: &[ConversationMessage]) -> usize {
    messages
        .iter()
        .filter(|message| {
            message
                .blocks
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))
        })
        .count()
}

fn eval_override_selection(
    override_: &str,
    _requires_write: bool,
    _sample_source: &str,
) -> Result<
    Option<(
        harness_contract::strategy::ExecutionCandidateKind,
        harness_contract::core::ExecutionPattern,
    )>,
    RuntimeError,
> {
    let normalized = override_.trim().to_ascii_lowercase();
    let selection = match normalized.as_str() {
        "auto" => return Ok(None),
        // Direct 是评测候选身份，不是移除安全门的许可。只读复杂任务和
        // 盲审规约同样可能携带 policy gates；裁剪为 Direct 图会让请求在
        // 模型调用前物化失败。Execute 图不会强制调用工具或启用并行，
        // 因而既保留完整策略门，也保持 Direct 候选的串行执行语义。
        "direct" => (
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            harness_contract::core::ExecutionPattern::Execute,
        ),
        // Parallel 同样只增加并行修饰符，不以 Explore 图替换原有安全门。
        "parallel_tools" | "parallel" => (
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
            harness_contract::core::ExecutionPattern::Execute,
        ),
        unknown => {
            return Err(RuntimeError::new(format!(
                "unsupported eval-only strategy override `{unknown}`"
            )));
        }
    };
    Ok(Some(selection))
}

fn apply_eval_strategy_override(
    decision: &mut crate::execution_core::RuntimeExecutionDecision,
) -> Result<(), RuntimeError> {
    if std::env::var("COWD_EVAL_HARNESS").as_deref() != Ok("1")
        || std::env::var("COWD_EVAL_CORPUS_ID").as_deref() != Ok("auto-strategy-v1")
    {
        return Ok(());
    }
    let Ok(override_) = std::env::var("COWD_EVAL_STRATEGY_OVERRIDE") else {
        return Ok(());
    };
    let Some((candidate, pattern)) = eval_override_selection(
        &override_,
        decision.strategy.understanding.requires_write,
        &decision.strategy.resource_snapshot.sample_source,
    )?
    else {
        return Ok(());
    };
    decision
        .strategy
        .retarget(
            pattern,
            format!(
                "pre-registered auto-strategy-v1 evaluation override selected {}",
                candidate.as_str()
            ),
        )
        .map_err(RuntimeError::new)?;
    decision.strategy.selected_candidate = candidate;
    if candidate == harness_contract::strategy::ExecutionCandidateKind::ParallelTools
        && !decision
            .strategy
            .modifiers
            .contains(&harness_contract::core::ExecutionModifier::Parallel)
    {
        decision
            .strategy
            .modifiers
            .push(harness_contract::core::ExecutionModifier::Parallel);
    }
    decision.compile_target = crate::execution_core::ExecutionPatternCatalog::current()
        .find(pattern)
        .map_or(
            crate::execution_core::RuntimeCompileTarget::InlineModel,
            |spec| spec.compile_target,
        );
    decision.lease.locked_pattern = pattern;
    Ok(())
}

/// Install a deterministic, marker-scoped cost fixture only for the isolated
/// browser acceptance harness.  The fixture is deliberately unavailable in a
/// normal Gateway process and does not accept data from the HTTP request: the
/// test process selects a fixed name and the submitted prompt must carry the
/// matching marker.  This lets STR-07 exercise the real admission,
/// persistence, projection and surface path without pretending that prose in
/// a prompt can alter the strategy cost model.
fn apply_e2e_strategy_fixture(input: &mut StrategyInput, prompt: &str) -> Result<(), RuntimeError> {
    if std::env::var("COWD_E2E_HARNESS").as_deref() != Ok("1") {
        return Ok(());
    }
    let Ok(fixture) = std::env::var("COWD_E2E_STRATEGY_FIXTURE") else {
        return Ok(());
    };
    apply_named_e2e_strategy_fixture(input, prompt, fixture.trim())
}

fn apply_named_e2e_strategy_fixture(
    input: &mut StrategyInput,
    prompt: &str,
    fixture: &str,
) -> Result<(), RuntimeError> {
    match fixture {
        "" => Ok(()),
        "explicit-team-negative" => {
            if !prompt.contains("[cowd-e2e:explicit-team-negative]") {
                return Ok(());
            }
            input.candidate_costs.insert(
                ExecutionCandidateKind::Team,
                StrategyCandidateCostSummary {
                    sample_count: 3,
                    average_critical_path_ms: 200_000,
                    average_total_tokens: 50_000,
                    average_coordination_cost_ms: 20_000,
                    calibration_source: "e2e:explicit-team-negative".to_string(),
                },
            );
            input.resource_snapshot.provider_concurrency_penalty_bp = 10_000;
            input.resource_snapshot.sample_source = format!(
                "{};e2e-fixture=explicit-team-negative",
                input.resource_snapshot.sample_source
            );
            input.resource_snapshot.provenance =
                harness_contract::core::MeasureProvenance::Observed;
            Ok(())
        }
        unknown => Err(RuntimeError::new(format!(
            "unsupported e2e-only strategy fixture `{unknown}`"
        ))),
    }
}

fn strategy_experience_record(trace: &RuntimeAiKernelTrace) -> StrategyExperienceRecord {
    let context_pressure = !trace.context_epoch.omitted.is_empty()
        || trace
            .context_alignment
            .as_ref()
            .map(|alignment| !alignment.aligned)
            .unwrap_or(false);
    let succeeded = trace.verification_report.can_finalize
        && trace.bench_result.passed
        && trace.regression_gate.allowed;
    StrategyExperienceRecord::from_decision(
        &trace.execution_decision.strategy,
        succeeded,
        trace.verification_blocked,
        context_pressure,
        false,
        now_ms(),
    )
}

fn strategy_experience_projection(trace: &RuntimeAiKernelTrace) -> serde_json::Value {
    let record = strategy_experience_record(trace);
    serde_json::json!({
        "domain": format!("{:?}", record.domain),
        "complexity": format!("{:?}", record.complexity),
        "risk": format!("{:?}", record.risk),
        "selected_pattern": record.selected_pattern.as_str(),
        "succeeded": record.succeeded,
        "verification_blocked": record.verification_blocked,
        "context_pressure": record.context_pressure,
        "multi_agent_positive_lift": record.multi_agent_positive_lift,
        "calibration_status": "outcome_projection_only",
        "persisted_for_routing": true,
        "store_ref": "runtime_event_store/outcome-projector",
    })
}

fn matrix_missing_evidence(trace: &RuntimeAiKernelTrace) -> Vec<String> {
    let mut missing = trace
        .verification_report
        .unsupported_required_claims
        .iter()
        .map(|claim| format!("unsupported_required_claim: {}", claim.statement))
        .collect::<Vec<_>>();
    missing.extend(
        trace
            .verification_report
            .not_run_claims
            .iter()
            .map(|claim| format!("not_run_claim: {}", claim.statement)),
    );
    if trace
        .context_alignment
        .as_ref()
        .map(|alignment| !alignment.aligned)
        .unwrap_or(false)
    {
        missing.push("context_epoch_envelope_alignment".to_string());
    }
    if !trace.context_epoch.omitted.is_empty() {
        missing.push(format!(
            "context_omitted_items:{}",
            trace.context_epoch.omitted.len()
        ));
    }
    missing
}

fn growth_maintenance_candidates(
    trace: &RuntimeAiKernelTrace,
) -> Vec<memory::MaintenanceCandidate> {
    trace
        .growth_event
        .memory_candidates
        .iter()
        .map(|candidate| {
            let now = chrono::Utc::now();
            memory::MaintenanceCandidate {
                id: candidate.id.clone(),
                kind: match candidate.kind {
                    harness_contract::growth::GrowthMemoryCandidateKind::Conflict => {
                        memory::MaintenanceCandidateKind::Conflict
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::Stale => {
                        memory::MaintenanceCandidateKind::Stale
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::AuthorityPromotion => {
                        memory::MaintenanceCandidateKind::AuthorityPromotion
                    }
                    harness_contract::growth::GrowthMemoryCandidateKind::RelationshipRefresh => {
                        memory::MaintenanceCandidateKind::RelationshipRefresh
                    }
                },
                status: memory::MaintenanceCandidateStatus::Open,
                entry_ids: Vec::new(),
                summary: candidate.summary.clone(),
                reason: format!(
                    "ai_growth:{}; confidence_bp={}",
                    candidate.reason, candidate.confidence_bp
                ),
                confidence: candidate.confidence_bp as f32 / 10_000.0,
                source: Some("ai_growth".to_string()),
                source_ref: Some(trace.growth_event.id.clone()),
                created_at: now,
                updated_at: now,
            }
        })
        .collect()
}

#[path = "static_tool_executor.rs"]
mod static_tool_executor;
pub use static_tool_executor::StaticToolExecutor;
