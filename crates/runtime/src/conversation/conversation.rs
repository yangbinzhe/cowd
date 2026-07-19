use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use fact_kernel::FactExtractionTokenUsage;
use tokio::sync::{RwLock, Semaphore};

/// T35: Lightweight cancellation token (tokio-util not available in dep tree).
#[derive(Default, Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

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
        CompactionReceipt, ContextGovernanceDecision, ContextPressureState, ContextTurnReport,
        EvidenceAccessRef, EvidenceAuditProjection, EvidenceRef, ToolObservation,
    },
    core::KernelRef,
    knowledge::KnowledgeTurnReport,
    skill::{AgentSkillProfile, SkillCapabilityProfile},
    strategy::{
        ExecutionCandidateKind, StrategyCandidateCostSummary, StrategyExperienceRecord,
        StrategyExperienceStore, StrategyInput,
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
use memory::{MemoryKernel, MemoryTurnContext};
use model_protocol::telemetry::SessionTracer;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::budget_policy::{RuntimeBudgetInputs, RuntimeBudgetPlan, clamp_context_budget_ratio_bp};

static STRATEGY_EXPERIENCE_IO_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
static EVALUATION_PROVIDER_TOKEN_LEASE: OnceLock<
    std::sync::Mutex<Option<EvaluationProviderTokenLeaseState>>,
> = OnceLock::new();

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

pub(crate) fn install_evaluation_provider_token_lease(
    lease_id: &str,
    limit: u64,
) -> Result<(), RuntimeError> {
    if lease_id.trim().is_empty() || limit == 0 || limit > 2_000_000 {
        return Err(RuntimeError::new(
            "evaluation provider token lease identity/limit is invalid",
        ));
    }
    let lease = EVALUATION_PROVIDER_TOKEN_LEASE.get_or_init(|| std::sync::Mutex::new(None));
    let mut lease = lease
        .lock()
        .map_err(|_| RuntimeError::new("evaluation provider token lease lock poisoned"))?;
    if lease
        .as_ref()
        .is_some_and(|current| current.outstanding > 0)
    {
        return Err(RuntimeError::new(
            "evaluation provider token lease cannot reset while a request is outstanding",
        ));
    }
    *lease = Some(EvaluationProviderTokenLeaseState {
        lease_id: lease_id.to_string(),
        limit,
        remaining: limit,
        input_consumed: 0,
        output_consumed: 0,
        cached_consumed: 0,
        outstanding: 0,
        breached: false,
    });
    Ok(())
}

pub(crate) fn evaluation_provider_token_lease_snapshot()
-> Option<EvaluationProviderTokenLeaseSnapshot> {
    EVALUATION_PROVIDER_TOKEN_LEASE
        .get()
        .and_then(|lease| lease.lock().ok())
        .and_then(|lease| {
            lease
                .as_ref()
                .map(|lease| EvaluationProviderTokenLeaseSnapshot {
                    lease_id: lease.lease_id.clone(),
                    limit: lease.limit,
                    consumed: lease.limit.saturating_sub(lease.remaining),
                    input_consumed: lease.input_consumed,
                    output_consumed: lease.output_consumed,
                    cached_consumed: lease.cached_consumed,
                    outstanding: lease.outstanding,
                    breached: lease.breached,
                })
        })
}

struct EvaluationProviderTokenReservation {
    reserved: u64,
    reconciled: bool,
}

impl EvaluationProviderTokenReservation {
    fn acquire(request: &mut ApiRequest) -> Result<Option<Self>, RuntimeError> {
        let Some(lease) = EVALUATION_PROVIDER_TOKEN_LEASE.get() else {
            return Ok(None);
        };
        let mut lease = lease
            .lock()
            .map_err(|_| RuntimeError::new("evaluation provider token lease lock poisoned"))?;
        let Some(lease) = lease.as_mut() else {
            return Ok(None);
        };
        if lease.breached {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` is already breached",
                lease.lease_id
            )));
        }
        // Reserve a conservative upper bound before touching the provider.
        // Input estimation, protocol framing and the normal request safety
        // margin are all charged; the remainder becomes the provider-enforced
        // maximum output. This lease is process-wide in the dedicated
        // evaluator Gateway, so Team children and their parent share it.
        let input_reserve = request
            .budget
            .input_total_tokens()
            .saturating_add(request.budget.protocol_overhead_tokens)
            .saturating_add(request.budget.safety_margin_tokens);
        if input_reserve >= lease.remaining {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` has {} tokens remaining but request input reserves {}",
                lease.lease_id, lease.remaining, input_reserve
            )));
        }
        let output_reserve = request
            .budget
            .requested_output_tokens
            .min(lease.remaining.saturating_sub(input_reserve));
        if output_reserve == 0 {
            return Err(RuntimeError::new(format!(
                "evaluation provider token lease `{}` has no output capacity",
                lease.lease_id
            )));
        }
        request.budget.requested_output_tokens = output_reserve;
        let reserved = input_reserve.saturating_add(output_reserve);
        lease.remaining = lease.remaining.saturating_sub(reserved);
        lease.outstanding = lease.outstanding.saturating_add(1);
        Ok(Some(Self {
            reserved,
            reconciled: false,
        }))
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
        if let Some(lease) = EVALUATION_PROVIDER_TOKEN_LEASE.get() {
            if let Ok(mut lease) = lease.lock() {
                if let Some(lease) = lease.as_mut() {
                    lease.input_consumed = lease.input_consumed.saturating_add(input);
                    lease.output_consumed = lease.output_consumed.saturating_add(output);
                    lease.cached_consumed = lease.cached_consumed.saturating_add(cached);
                    if actual <= self.reserved {
                        lease.remaining = lease
                            .remaining
                            .saturating_add(self.reserved.saturating_sub(actual))
                            .min(lease.limit);
                    } else {
                        lease.breached = true;
                        lease.remaining = 0;
                    }
                    lease.outstanding = lease.outstanding.saturating_sub(1);
                    self.reconciled = true;
                }
            }
        }
    }
}

impl Drop for EvaluationProviderTokenReservation {
    fn drop(&mut self) {
        if self.reconciled {
            return;
        }
        if let Some(lease) = EVALUATION_PROVIDER_TOKEN_LEASE.get() {
            if let Ok(mut lease) = lease.lock() {
                if let Some(lease) = lease.as_mut() {
                    lease.outstanding = lease.outstanding.saturating_sub(1);
                }
            }
        }
    }
}
use crate::PromptAssembly;
use crate::compact::{
    CompactionConfig, apply_compaction_summary, estimate_session_tokens, plan_session_compaction,
};
use crate::config::{RuntimeFeatureConfig, SessionCompactConfig as RuntimeSessionCompactConfig};
use crate::context_runtime::{
    ContextAuthority, ContextEnvelope, ContextEnvelopeRequest, ContextIdentity, ContextItem,
    ContextOmission, ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind,
    ContextVisibility, ResumeContextPacket, RuntimeContextFactDecision,
    RuntimeContextGovernanceReport, ToolTracePacket, ToolTraceStatus,
};
use crate::context_tool_exposure::{ToolExposurePlanner, ToolExposurePolicy, ToolExposureState};
use crate::fact_extraction::{
    FactExtractionRuntimeEvent, RuleFactExtractor, RuntimeFactExtractionInput,
    RuntimeFactExtractionPolicy, RuntimeFactExtractionScheduler, RuntimeFactExtractionTrigger,
    RuntimeFactExtractor,
};
use crate::hooks::{HookAbortSignal, HookProgressReporter, HookRunResult, HookRunner};
use crate::knowledge_activation::KnowledgeActivationRuntime;
use crate::permissions::{PermissionContext, PermissionOutcome, PermissionPolicy};
use crate::runtime_control::RuntimeControlPolicy;
use crate::runtime_harness::{RuntimeAiKernel, RuntimeAiKernelTrace};
use crate::session::{ContentBlock, ConversationMessage, MessageEvent, Session, SessionEventLog};
use crate::skill::{
    RuntimeSkillPromptAsset, SkillActivationEngine, SkillActivationInput, SkillMemoryPolicy,
    memory_candidate_from_skill_activation, skill_memory_candidate_session_event,
};
use crate::tool_execution_plan::{ToolExecutionPlan, ToolExecutionPolicyValidationReport};
use crate::tool_invocation::{
    DEFAULT_OUTPUT_REF_MIN_LINES, ToolFailureKind, ToolInvocationRecord, now_ms,
};
use crate::usage::{ModelPerformanceRegistry, ModelRouteIntent, UsageTracker};
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};
use model_protocol::usage::TokenUsage;

/// Keep enough request capacity for fixed instructions, current history and a
/// meaningful continuation even when a provider calibrates an unknown model
/// to a much smaller context window.
const MIN_PROVIDER_INPUT_RESERVE_TOKENS: u32 = 4_096;

fn bounded_provider_output_tokens(model: &str, context_window: u32) -> u32 {
    let provider_cap = provider::max_tokens_for_model(model);
    let window_cap = context_window
        .saturating_sub(MIN_PROVIDER_INPUT_RESERVE_TOKENS)
        .max(1_024);
    provider_cap.min(window_cap)
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

fn conversation_messages_token_estimate(messages: &[ConversationMessage]) -> u64 {
    messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .fold(0u64, |total, block| match block {
            ContentBlock::Text { text } => {
                total.saturating_add(crate::context_ledger::estimate_text_tokens(text))
            }
            ContentBlock::Image {
                media_type, data, ..
            } => total
                .saturating_add(crate::context_ledger::estimate_text_tokens(media_type))
                .saturating_add((data.len() as u64).div_ceil(4)),
            ContentBlock::Thinking { thinking, .. } => {
                total.saturating_add(crate::context_ledger::estimate_text_tokens(thinking))
            }
            ContentBlock::ToolUse { id, name, input } => total
                .saturating_add(crate::context_ledger::estimate_text_tokens(id))
                .saturating_add(crate::context_ledger::estimate_text_tokens(name))
                .saturating_add(crate::context_ledger::estimate_text_tokens(input)),
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                ..
            } => total
                .saturating_add(crate::context_ledger::estimate_text_tokens(tool_use_id))
                .saturating_add(crate::context_ledger::estimate_text_tokens(tool_name))
                .saturating_add(crate::context_ledger::estimate_text_tokens(output)),
        })
}

fn latest_user_prompt_text(messages: &[ConversationMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|message| message.role == crate::session::MessageRole::User)
        .map(|message| {
            message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn classify_model_step_intent(text: String, calls: Vec<ModelToolCall>) -> ModelStepIntent {
    if calls.is_empty() {
        return ModelStepIntent::FinalAnswer { text };
    }
    let normalized = calls
        .iter()
        .map(|call| call.name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if normalized
        .iter()
        .any(|name| name.contains("approval") || name.contains("permission"))
    {
        ModelStepIntent::ApprovalRequired { calls }
    } else if normalized
        .iter()
        .any(|name| name.contains("team") || name.contains("collaborat"))
    {
        ModelStepIntent::TeamProposal { calls }
    } else if normalized
        .iter()
        .any(|name| name.contains("agent") || name.contains("subagent"))
    {
        ModelStepIntent::AgentProposal { calls }
    } else if normalized.iter().any(|name| name.contains("replan")) {
        ModelStepIntent::Replan {
            reason: if text.is_empty() {
                "model requested execution graph replanning".to_string()
            } else {
                text
            },
        }
    } else {
        ModelStepIntent::ToolCalls { calls }
    }
}

/// An explicit user requirement to actually form a team is an acceptance
/// constraint, not merely a prose preference. It takes precedence over a
/// heuristic strategy recommendation: otherwise a correctly parsed user
/// requirement can disappear just because the lightweight classifier chose a
/// different execution pattern. Generic complex work remains model-directed
/// and is never forced through this path.
fn enforce_explicit_team_requirement(
    objective: &str,
    first_step: bool,
    _decision: &crate::execution_core::RuntimeExecutionDecision,
    intent: ModelStepIntent,
) -> ModelStepIntent {
    if !first_step || !explicit_team_execution_required(objective) {
        return intent;
    }

    match intent {
        ModelStepIntent::ToolCalls { mut calls } => {
            if !calls.iter().any(is_runtime_team_orchestration_call) {
                calls.push(required_team_orchestration_call(objective));
            }
            ModelStepIntent::ToolCalls { calls }
        }
        ModelStepIntent::FinalAnswer { .. } => ModelStepIntent::ToolCalls {
            calls: vec![required_team_orchestration_call(objective)],
        },
        // Provider tool naming is not a reliable contract: an otherwise
        // ordinary evidence tool can contain "agent", and a provider-native
        // team helper may be classified as a proposal. Keep those calls in
        // the regular ToolBatch, but add the one canonical Runtime request so
        // the requirement is materialized by the team compiler.
        ModelStepIntent::AgentProposal { mut calls }
        | ModelStepIntent::TeamProposal { mut calls } => {
            if !calls.iter().any(is_runtime_team_orchestration_call) {
                calls.push(required_team_orchestration_call(objective));
            }
            ModelStepIntent::ToolCalls { calls }
        }
        // A human approval request is an explicit safety boundary. It is the
        // only model intent that may defer an otherwise explicit team request.
        ModelStepIntent::ApprovalRequired { calls } => ModelStepIntent::ApprovalRequired { calls },
        ModelStepIntent::Replan { .. } => ModelStepIntent::ToolCalls {
            calls: vec![required_team_orchestration_call(objective)],
        },
    }
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

fn explicit_team_execution_required(objective: &str) -> bool {
    let normalized = objective.to_ascii_lowercase();
    let mentions_team = [
        "团队",
        "协作",
        "多agent",
        "多 agent",
        "team",
        "multi-agent",
        "multi agent",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    let requires_execution = [
        "实际启动",
        "启动",
        "创建",
        "组建",
        "必须",
        "必须要",
        "must",
        "actually",
        "launch",
        "start",
        "create",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    let explicitly_disabled = [
        "不要组队",
        "不要团队",
        "不要启动团队",
        "不要启动协作",
        "不启动团队",
        "无需团队",
        "不需要团队",
        "don't use team",
        "do not use team",
        "do not start a team",
        "single agent",
        "single-agent",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    mentions_team && requires_execution && !explicitly_disabled
}

fn is_runtime_team_orchestration_call(call: &ModelToolCall) -> bool {
    if !call.name.eq_ignore_ascii_case("runtime_orchestrate") {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(&call.input)
        .ok()
        .and_then(|input| {
            input
                .get("action")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|action| action == "request_team")
}

/// Stateful tool execution normally means a guarded `Execute` turn. A team
/// orchestration call is the one exception: retargeting it to `Execute` would
/// make the leased strategy reject the very `request_team` action we exposed
/// to the provider. Keep the decision and the typed action aligned.
fn tool_batch_pattern(calls: &[ModelToolCall]) -> harness_contract::core::ExecutionPattern {
    if calls.iter().any(is_runtime_team_orchestration_call) {
        harness_contract::core::ExecutionPattern::Collaborate
    } else {
        harness_contract::core::ExecutionPattern::Execute
    }
}

fn model_team_request_conflicts_with_admission(
    candidate: harness_contract::strategy::ExecutionCandidateKind,
    calls: &[ModelToolCall],
) -> bool {
    tool_batch_pattern(calls) == harness_contract::core::ExecutionPattern::Collaborate
        && candidate != harness_contract::strategy::ExecutionCandidateKind::Team
}

fn required_team_orchestration_call(objective: &str) -> ModelToolCall {
    ModelToolCall {
        id: "runtime-required-team".to_string(),
        name: "runtime_orchestrate".to_string(),
        input: serde_json::json!({
            "intent": objective,
            "action": "request_team",
            "reason": "the user explicitly requires an actually started collaboration team",
            "template_hint": "cowd/parallel-research-synthesis",
            "constraints": {
                "max_parallel_agents": 3,
                "risk": "low",
                "requires_write": false,
                "surface_latency_sensitive": false,
            }
        })
        .to_string(),
        depends_on: Vec::new(),
    }
}

/// Fully assembled request payload sent to the upstream model client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiRequest {
    pub prompt: PromptAssembly,
    pub messages: Vec<ConversationMessage>,
    /// Runtime-selected primary model ID.
    pub model: String,
    /// Runtime-owned one-shot reasoning policy for this provider attempt.
    /// The transport adapter decides whether the selected model supports the
    /// requested effort; unsupported backends retain their configured policy.
    pub reasoning_effort_override: Option<String>,
    /// Request-local capacity contract used for diagnostics and ledger
    /// reconciliation. Provider must not mutate routing or budget ownership.
    pub budget: crate::context_ledger::RequestBudgetReport,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProviderContextInventory {
    pub tool_count: usize,
    pub tool_schema_tokens: u64,
}

/// Streamed events emitted while processing a single assistant turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssistantEvent {
    /// The provider/model that actually accepted this request. This is emitted
    /// only after the provider has produced a protocol event, so it is never
    /// mistaken for a configured fallback that was merely considered.
    ProviderModel {
        model: String,
    },
    TextDelta(String),
    /// P1-7: Extended thinking delta (reasoning model output)
    ThinkingDelta(String),
    /// P1-7: Thinking signature that must be preserved and passed back
    /// to the provider in subsequent requests.
    SignatureDelta(String),
    ToolUse {
        id: String,
        name: String,
        input: String,
    },
    Usage(TokenUsage),
    PromptCache(PromptCacheEvent),
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

/// Prompt-cache telemetry captured from the provider response stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptCacheEvent {
    pub unexpected: bool,
    pub reason: String,
    pub previous_cache_read_input_tokens: u32,
    pub current_cache_read_input_tokens: u32,
    pub token_drop: u32,
}

fn preview_chars(value: &str, max_chars: usize) -> String {
    let mut preview: String = value.chars().take(max_chars).collect();
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
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
    let mut bootstrap = vec!["ToolSearch".to_string(), "runtime_capabilities".to_string()];
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

#[cfg(test)]
mod tool_exposure_contract_tests {
    use super::bootstrap_tool_ids;
    use harness_contract::tool::ToolPermissionMode;

    #[test]
    fn stateful_runtime_orchestration_is_bootstrapped_only_when_policy_allows_write() {
        assert_eq!(
            bootstrap_tool_ids(ToolPermissionMode::ReadOnly),
            vec!["ToolSearch", "runtime_capabilities"]
        );
        assert_eq!(
            bootstrap_tool_ids(ToolPermissionMode::WorkspaceWrite),
            vec!["ToolSearch", "runtime_capabilities", "runtime_orchestrate"]
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
        crate::PermissionMode::DangerFullAccess
        | crate::PermissionMode::Prompt
        | crate::PermissionMode::Allow => {
            harness_contract::tool::ToolPermissionMode::DangerFullAccess
        }
    }
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

    fn provider_available(&self) -> bool {
        true
    }

    fn configure_tool_exposure(
        &mut self,
        _projection: harness_contract::tool::ToolExposureProjection,
    ) {
    }

    fn context_inventory(&self) -> ProviderContextInventory {
        ProviderContextInventory::default()
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

/// Trait implemented by tool dispatchers that execute model-requested tools.
pub trait ToolExecutor: Send + Sync + 'static {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError>;

    /// Production executors override this with a receipt from their pinned
    /// ToolHost. The fallback is deliberately read-only for small embedded and
    /// test executors that do not own a catalog.
    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        fallback_tool_discovery_receipt(self.available_tool_names())
    }

    fn describe_tool_effect(
        &self,
        _tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        None
    }

    fn execute_authorized(
        &self,
        _authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        _input: &str,
    ) -> Result<String, ToolError> {
        Err(ToolError::new(format!(
            "tool `{tool_name}` has no authorized execution implementation"
        )))
    }

    fn has_registered_tools(&self) -> bool {
        !self.available_tool_names().is_empty()
    }

    fn available_tool_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn has_tool(&self, tool_name: &str) -> bool {
        self.available_tool_names()
            .iter()
            .any(|available| available == tool_name)
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
}

impl ToolError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ToolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ToolError {}

/// Error returned when a conversation turn cannot be completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError {
    message: String,
    provider_context_window_limit: Option<u32>,
}

impl RuntimeError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            provider_context_window_limit: None,
        }
    }

    #[must_use]
    pub fn with_provider_context_window_limit(
        message: impl Into<String>,
        provider_context_window_limit: Option<u32>,
    ) -> Self {
        Self {
            message: message.into(),
            provider_context_window_limit,
        }
    }

    #[must_use]
    pub const fn provider_context_window_limit(&self) -> Option<u32> {
        self.provider_context_window_limit
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
    pub prompt_cache_events: Vec<PromptCacheEvent>,
    pub iterations: usize,
    pub usage: TokenUsage,
    pub model_telemetry: crate::cowd_event::RunModelTelemetry,
    pub auto_compaction: Option<AutoCompactionEvent>,
    pub ai_kernel_trace: RuntimeAiKernelTrace,
    pub context_turn_report: ContextTurnReport,
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
    AgentProposal { calls: Vec<ModelToolCall> },
    TeamProposal { calls: Vec<ModelToolCall> },
    ApprovalRequired { calls: Vec<ModelToolCall> },
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
    pub prompt_cache_events: Vec<PromptCacheEvent>,
    pub model: Option<String>,
    pub wall_duration_ms: u64,
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
    focus_partition_plans: Vec<harness_contract::team::FocusPartitionPlan>,
    pattern: harness_contract::core::ExecutionPattern,
}

pub struct ConversationRuntime<C, T> {
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
    system_prompt: Vec<String>,
    usage_tracker: UsageTracker,
    model_performance_registry: std::sync::Mutex<ModelPerformanceRegistry>,
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
    calibrated_model_context_windows: std::sync::Mutex<BTreeMap<String, u32>>,
    hook_abort_signal: HookAbortSignal,
    hook_progress_reporter: Arc<std::sync::Mutex<Option<Box<dyn HookProgressReporter + Send>>>>,
    session_tracer: Option<SessionTracer>,
    /// Optional cognitive memory manager – `None` when memory is disabled.
    memory_manager: Option<Arc<CognitiveContextManager>>,
    /// Human-readable memory status message. `None` when healthy; `Some(msg)` when degraded.
    memory_status: Option<String>,
    /// Runtime-owned Fact/Matrix recall boundary for this conversation.  It
    /// is populated only from a compiled Binding, never from a Surface field.
    reality_recall: Option<(
        crate::RealityRecallPort,
        harness_contract::agent::AgentBindingSnapshot,
    )>,
    /// Latest lease-filtered Fact/Matrix recall report, retained for runtime
    /// audit and projections without turning Gateway into a context assembler.
    last_reality_recall_report: std::sync::Mutex<Option<crate::RealityRecallReport>>,
    /// Optional tool callback for real-time visualization (P0-2).
    tool_callback: Option<Arc<dyn ToolCallback>>,
    /// Optional managed SQLite session store for messages and domain events.
    session_store: Option<Arc<memory::session_store::UnifiedSessionStore>>,
    /// Whether the in-memory transcript may also write message rows directly.
    /// Gateway ingress owns durable user/terminal writes through its outboxes;
    /// disabling this for an ingress turn prevents a second transcript writer.
    transcript_persistence: bool,
    /// Durable execution lifecycle store. Session-domain events never use it.
    runtime_event_store: Option<Arc<RuntimeEventStore>>,
    /// Optional event log for time-travel debugging and session rebuild.
    event_log: Option<std::sync::Mutex<SessionEventLog>>,
    /// Runtime-local searchable index for oversized tool outputs.
    tool_output_sandbox: Option<Arc<std::sync::Mutex<memory::ToolOutputSandbox>>>,
    /// Optional SSE callback for real-time streaming events to WebUI.
    /// Receives pre-formatted JSON event strings.
    sse_callback: Option<Arc<dyn Fn(String) + Send + Sync>>,
    /// Optional memory lifecycle callback for TUI memory events.
    memory_callback: Option<Arc<dyn MemoryCallback>>,
    /// Optional smart approval gate for intelligent command approval (P0-1).
    approval_gate: Option<Arc<crate::approval_gate::SmartApprovalGate>>,
    /// Skill capability profiles already inspected by the Skill asset layer and
    /// visible to this runtime.
    skill_profiles: Vec<SkillCapabilityProfile>,
    /// Agent-scoped Skill visibility and adapter policy.
    agent_skill_profile: AgentSkillProfile,
    /// Gateway-inspected PromptOnly assets keyed by Skill identity. Runtime
    /// chooses among these assets but never discovers or reads packages.
    skill_prompt_assets: Vec<RuntimeSkillPromptAsset>,
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
    /// Optional commit quality gate evaluator (PreFlight, Revision, Escalation, Abort).
    gate_evaluator: Option<Arc<crate::gates::GateEvaluator>>,
    /// Current model ID (used for provider fallback chain lookup).
    model: Option<String>,
    /// Provider fallback configuration for automatic retry on 429/5xx errors.
    fallbacks: Vec<String>,
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
    /// One governed checkpoint can lower the cognitive budget of exactly one
    /// provider request after deterministic evidence acquisition is complete.
    next_model_reasoning_effort: std::sync::Mutex<Option<String>>,
    /// Bounded short-term tool trace context for subsequent turns.
    tool_trace_context_items: std::sync::Mutex<Vec<ContextItem>>,
    /// Governance observations produced by tool calls in the active turn.
    turn_tool_observations: std::sync::Mutex<Vec<ToolObservation>>,
    /// Sole strategy identity for the admitted turn. Host creates it before
    /// graph compilation; every later checkpoint reads or revises this state.
    active_turn_strategy:
        std::sync::Mutex<Option<crate::execution_core::TurnStrategyDecisionState>>,
    /// Revisioned tool set visible to the next provider request.
    tool_exposure_state: std::sync::Mutex<Option<ToolExposureState>>,
    /// Tools coupled to the PromptOnly Skill selected for the active turn.
    /// Runtime folds these into the first provider exposure so Skill guidance
    /// and its executable capability arrive atomically.
    active_skill_tool_refs: std::sync::Mutex<BTreeSet<String>>,
    /// Provider visibility changes must be monotonically ordered. A governed
    /// text-only checkpoint temporarily withdraws every schema; the next
    /// normal model step must be able to restore the catalog rather than be
    /// rejected as an older projection by the provider client.
    tool_exposure_revision: AtomicU64,
    /// Stable evidence projections emitted during the active turn.
    turn_evidence_audits: std::sync::Mutex<Vec<EvidenceAuditProjection>>,
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
    /// T4: Semaphore for WriteLocal tool concurrency (permits: 4).
    write_semaphore: Arc<Semaphore>,
    /// T4: Semaphore for Network tool concurrency (permits: 3).
    network_semaphore: Arc<Semaphore>,
    /// T4: Semaphore for Destructive tool concurrency (permits: 1).
    destructive_semaphore: Arc<Semaphore>,
    /// Per-turn ReadOnly admission. The process-wide permit is acquired in
    /// addition to this one for every category.
    default_semaphore: Arc<Semaphore>,
    /// 普通 Conversation 与 ExecutionGraph 共享的 Provider admission owner。
    /// Graph-owned child host 已由外层 node lease 覆盖，因此不会重复申请。
    provider_admission: Option<Arc<crate::execution_core::graph::ExecutionResourceManager>>,
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
}

impl<C, T> ConversationRuntime<C, T>
where
    C: ApiClient,
    T: ToolExecutor,
{
    #[must_use]
    pub fn new(
        session: Session,
        api_client: C,
        tool_executor: T,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
    ) -> Self {
        Self::new_with_features(
            session,
            api_client,
            Arc::new(tool_executor),
            permission_policy,
            system_prompt,
            &RuntimeFeatureConfig::default(),
        )
    }

    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new_with_features(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
    ) -> Self {
        let usage_tracker = UsageTracker::from_session(&session);
        let subsystem_budget_ratio_bp = feature_config.context_budget().subsystem_budget_ratio_bp;
        let initial_window_resolution = feature_config.model().map_or(
            provider::ModelContextWindowResolution {
                tokens: 128_000,
                source: provider::ModelContextWindowSource::Assumed,
            },
            |model| {
                provider::model_context_window_resolution(
                    model,
                    Some(feature_config.model_context_windows()),
                )
            },
        );
        let initial_model_context_window = initial_window_resolution.tokens;
        let initial_model_max_output = feature_config.model().map_or(0, |model| {
            bounded_provider_output_tokens(model, initial_model_context_window)
        });
        let initial_budget_plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: initial_model_context_window,
            model_max_output_tokens: initial_model_max_output,
            subsystem_budget_ratio_bp,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        });
        // Initialise the cognitive memory manager if the memory subsystem is enabled.
        let (memory_manager, memory_status) = if feature_config.memory().enabled {
            let mem_cfg = build_cc_memory_config_with_budget(feature_config, &initial_budget_plan);
            match tokio::runtime::Handle::try_current() {
                Ok(_) => {
                    // Inside a runtime — spawn a fresh thread with its own runtime
                    // to avoid nested enter_runtime panic.
                    let mem_cfg = mem_cfg.clone();
                    let handle = std::thread::spawn(move || -> Result<_, String> {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|error| {
                                format!("failed to create memory init runtime: {error}")
                            })?;
                        rt.block_on(CognitiveContextManager::new(mem_cfg))
                            .map_err(|error| error.to_string())
                    });
                    match handle.join() {
                        Ok(Ok(mgr)) => {
                            tracing::debug!(
                                "memory: CognitiveContextManager initialised with explicit per-turn identity"
                            );
                            (Some(Arc::new(mgr)), None)
                        }
                        Ok(Err(err)) => {
                            let msg = format!(
                                "Memory system unavailable: {err}. Context will NOT persist between turns. Check your memory store paths, vector API credentials, and ~/.cowd/memory/ directory."
                            );
                            tracing::error!("{msg}");
                            (None, Some(msg))
                        }
                        Err(_) => {
                            let msg = "Memory system unavailable: initialization thread panicked. Context will NOT persist between turns.".to_string();
                            tracing::error!("{msg}");
                            (None, Some(msg))
                        }
                    }
                }
                Err(_) => {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => match rt.block_on(CognitiveContextManager::new(mem_cfg)) {
                            Ok(mgr) => {
                                tracing::debug!(
                                    "memory: CognitiveContextManager initialised with explicit per-turn identity"
                                );
                                (Some(Arc::new(mgr)), None)
                            }
                            Err(err) => {
                                let msg = format!(
                                    "Memory system unavailable: {err}. Context will NOT persist between turns. Check your memory store paths, vector API credentials, and ~/.cowd/memory/ directory."
                                );
                                tracing::error!("{msg}");
                                (None, Some(msg))
                            }
                        },
                        Err(e) => {
                            let msg = format!(
                                "Memory system unavailable: failed to create runtime: {e}. Memory features will NOT work."
                            );
                            tracing::error!("{msg}");
                            (None, Some(msg))
                        }
                    }
                }
            }
        } else {
            (None, None)
        };
        let session_id = session.session_id.clone();
        let session = Arc::new(RwLock::new(session));
        let mut runtime_control_policy = feature_config.runtime_control().policy.clone();
        apply_runtime_budget_to_control_policy(&mut runtime_control_policy, &initial_budget_plan);
        Self {
            session,
            session_input_stream: crate::session_input::SessionInputStream::new(session_id),
            consumed_session_inputs: std::sync::Mutex::new(Vec::new()),
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            usage_tracker,
            model_performance_registry: std::sync::Mutex::new(ModelPerformanceRegistry::new()),
            hook_runner: HookRunner::from_feature_config(feature_config),
            cowd_bus: None,
            turn_callback: None,
            profiler: crate::context_profiler::ContextProfiler::new(),
            subsystem_budget_ratio_bp,
            session_compaction_config: feature_config.compression().session.clone(),
            semantic_checkpoint_enabled: feature_config
                .memory()
                .runtime
                .semantic_checkpoint_enabled,
            model_context_window: initial_model_context_window,
            model_context_window_source: initial_window_resolution.source,
            model_context_windows: feature_config.model_context_windows().clone(),
            calibrated_model_context_windows: std::sync::Mutex::new(BTreeMap::new()),
            hook_abort_signal: HookAbortSignal::default(),
            hook_progress_reporter: Arc::new(std::sync::Mutex::new(None)),
            session_tracer: None,
            memory_manager,
            memory_status,
            reality_recall: None,
            last_reality_recall_report: std::sync::Mutex::new(None),
            tool_callback: None,
            session_store: None,
            transcript_persistence: true,
            runtime_event_store: None,
            event_log: None,
            tool_output_sandbox: memory::ToolOutputSandbox::new()
                .map(|sandbox| Arc::new(std::sync::Mutex::new(sandbox)))
                .map_err(|error| {
                    tracing::warn!(%error, "tool output sandbox unavailable");
                    error
                })
                .ok(),
            sse_callback: None,
            memory_callback: None,
            approval_gate: None,
            skill_profiles: Vec::new(),
            agent_skill_profile: AgentSkillProfile::default(),
            skill_prompt_assets: Vec::new(),
            memory_agent_id: "primary".to_string(),
            memory_definition_lineage_id: None,
            memory_team_id: None,
            memory_read_scopes: vec![
                harness_contract::agent::CognitiveReadScope::Session,
                harness_contract::agent::CognitiveReadScope::Team,
                harness_contract::agent::CognitiveReadScope::WorkspaceKnowledge,
                harness_contract::agent::CognitiveReadScope::Project,
                harness_contract::agent::CognitiveReadScope::DefinitionLineage,
            ],
            project_phase: "Discovery".to_string(),
            gate_evaluator: Some(Arc::new(
                crate::gates::GateEvaluator::new().with_default_gates(),
            )),
            model: feature_config.model().map(str::to_string),
            fallbacks: feature_config.fallbacks().to_vec(),
            cancellation_token: CancellationToken::new(),
            last_context_envelope: std::sync::Mutex::new(None),
            context_profile: std::sync::Mutex::new(ContextProfile::MainTurn),
            runtime_control_policy,
            external_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_text_only: AtomicBool::new(false),
            next_model_tool_allowlist: std::sync::Mutex::new(None),
            next_model_reasoning_effort: std::sync::Mutex::new(None),
            tool_trace_context_items: std::sync::Mutex::new(Vec::new()),
            turn_tool_observations: std::sync::Mutex::new(Vec::new()),
            active_turn_strategy: std::sync::Mutex::new(None),
            tool_exposure_state: std::sync::Mutex::new(None),
            active_skill_tool_refs: std::sync::Mutex::new(BTreeSet::new()),
            tool_exposure_revision: AtomicU64::new(0),
            turn_evidence_audits: std::sync::Mutex::new(Vec::new()),
            turn_context_ledger: std::sync::Mutex::new(crate::context_ledger::ContextLedger::new(
                initial_budget_plan.subsystem_budget_tokens,
                initial_budget_plan.tool_result_budget.max_total_tokens as u64,
            )),
            last_context_turn_report: std::sync::Mutex::new(None),
            turn_preflight_compaction: std::sync::Mutex::new(None),
            turn_knowledge_report: std::sync::Mutex::new(None),
            write_semaphore: Arc::new(Semaphore::new(
                crate::tool_orchestrator::ToolSafetyCategory::WriteLocal.max_concurrency(),
            )),
            network_semaphore: Arc::new(Semaphore::new(
                crate::tool_orchestrator::ToolSafetyCategory::Network.max_concurrency(),
            )),
            destructive_semaphore: Arc::new(Semaphore::new(
                crate::tool_orchestrator::ToolSafetyCategory::Destructive.max_concurrency(),
            )),
            default_semaphore: Arc::new(Semaphore::new(
                crate::execution_scheduler::DEFAULT_PARALLEL_READ_CONCURRENCY,
            )),
            provider_admission: None,
            tool_timeout: Some(Duration::from_secs(120)),
            explicit_team_escalation: true,
            model_step_limit_override: AtomicUsize::new(0),
            delegated_focus_novelty_target_bp: AtomicU64::new(0),
            delegated_focus_acceptance_scopes: std::sync::Mutex::new(Vec::new()),
            delegated_focus_required_output_fields: std::sync::Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn with_tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = Some(timeout);
        self
    }

    #[must_use]
    pub fn with_provider_admission(
        mut self,
        manager: Arc<crate::execution_core::graph::ExecutionResourceManager>,
    ) -> Self {
        self.provider_admission = Some(manager);
        self
    }

    #[must_use]
    pub fn with_explicit_team_escalation(mut self, enabled: bool) -> Self {
        self.explicit_team_escalation = enabled;
        self
    }

    /// Provider context capacity bound to this runtime instance. Execution
    /// safety derives its lease from this value rather than Gateway prompt
    /// classes or a fixed whole-turn iteration limit.
    #[must_use]
    pub const fn model_context_window(&self) -> u32 {
        self.model_context_window
    }

    #[must_use]
    pub(crate) fn current_model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Return a human-readable description of memory subsystem health.
    /// `None` when healthy; `Some(msg)` when degraded or unavailable.
    pub fn memory_status(&self) -> Option<&str> {
        self.memory_status.as_deref()
    }

    /// Return the current project lifecycle phase.
    pub fn phase(&self) -> &str {
        &self.project_phase
    }

    /// Return the latest context envelope assembled for an actual model turn.
    pub fn last_context_envelope(&self) -> Option<ContextEnvelope> {
        self.last_context_envelope
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Return the latest context governance report emitted by a completed turn.
    pub fn last_context_turn_report(&self) -> Option<ContextTurnReport> {
        self.last_context_turn_report
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Return the active context profile used for the next envelope.
    pub fn context_profile(&self) -> ContextProfile {
        self.context_profile
            .lock()
            .map(|guard| *guard)
            .unwrap_or(ContextProfile::MainTurn)
    }

    /// Set the active context profile used for subsequent envelope assembly.
    pub fn set_context_profile(&self, profile: ContextProfile) {
        if let Ok(mut guard) = self.context_profile.lock() {
            *guard = profile;
        }
    }

    /// Bind an absolute model-step ceiling supplied by the Runtime owner.
    /// This is not model-visible prompt text and cannot be raised by a
    /// delegated provider response.
    pub fn set_model_step_limit_override(&self, limit: usize) {
        self.model_step_limit_override
            .store(limit.max(1), Ordering::SeqCst);
    }

    /// Return the Runtime-issued model-step ceiling, if one is active.
    #[must_use]
    pub fn model_step_limit_override(&self) -> Option<usize> {
        match self.model_step_limit_override.load(Ordering::SeqCst) {
            0 => None,
            limit => Some(limit),
        }
    }

    /// Bind the validated Focus acceptance/novelty policy for a delegated
    /// child. The values are Runtime-owned and cannot be changed by provider
    /// output or model-visible prompt text.
    pub fn set_delegated_focus_policy(
        &self,
        novelty_target_bp: u16,
        acceptance_scopes: Vec<String>,
        required_output_fields: Vec<String>,
    ) {
        self.delegated_focus_novelty_target_bp
            .store(u64::from(novelty_target_bp.min(10_000)), Ordering::SeqCst);
        if let Ok(mut guard) = self.delegated_focus_acceptance_scopes.lock() {
            *guard = acceptance_scopes;
        }
        if let Ok(mut guard) = self.delegated_focus_required_output_fields.lock() {
            *guard = required_output_fields;
        }
    }

    #[must_use]
    pub fn delegated_focus_policy(&self) -> (u16, Vec<String>, Vec<String>) {
        (
            u16::try_from(
                self.delegated_focus_novelty_target_bp
                    .load(Ordering::SeqCst),
            )
            .unwrap_or(10_000),
            self.delegated_focus_acceptance_scopes
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default(),
            self.delegated_focus_required_output_fields
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default(),
        )
    }

    /// Replace runtime-owned context supplied by orchestration layers.
    pub fn set_external_context_items(&self, items: Vec<ContextItem>) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            *guard = items;
        }
    }

    /// Add one runtime-owned context item supplied by orchestration layers.
    pub fn push_external_context_item(&self, item: ContextItem) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            guard.push(item);
        }
    }

    /// Add a checkpoint-owned instruction to the next provider request only.
    /// This is intentionally distinct from persistent external context: graph
    /// recovery may steer one request without accumulating hidden prompt
    /// state across the remainder of a session.
    pub(crate) fn push_next_model_context_item(&self, item: ContextItem) {
        if let Ok(mut guard) = self.next_model_context_items.lock() {
            guard.push(item);
        }
    }

    fn take_next_model_context_items(&self) -> Vec<ContextItem> {
        self.next_model_context_items
            .lock()
            .map(|mut guard| std::mem::take(&mut *guard))
            .unwrap_or_default()
    }

    async fn activate_skills_for_turn(&self, user_input: &str) -> Result<(), RuntimeError> {
        if let Ok(mut tool_refs) = self.active_skill_tool_refs.lock() {
            tool_refs.clear();
        }
        if self.skill_profiles.is_empty() {
            return Ok(());
        }

        let session = self.session();
        let activation = SkillActivationEngine::activate(SkillActivationInput {
            session_id: session.session_id,
            turn_index: session.messages.len(),
            query: user_input.to_string(),
            capability_refs: Vec::new(),
            available_profiles: self.skill_profiles.clone(),
            agent_profile: self.agent_skill_profile.clone(),
        });

        if let Some(invocation) = activation.selected_invocation.as_ref() {
            if let Some(asset) = self
                .skill_prompt_assets
                .iter()
                .find(|asset| asset.skill_id == invocation.skill_id)
            {
                if let Ok(mut tool_refs) = self.active_skill_tool_refs.lock() {
                    tool_refs.extend(asset.tool_refs.iter().cloned());
                }
                let mut item = ContextItem::new(
                    format!(
                        "runtime-skill:{}:{}",
                        asset.skill_id, activation.activation.turn_index
                    ),
                    ContextSourceKind::Task,
                    ContextRole::Instruction,
                    format!(
                        "# Activated skill: {}\nversion: {}\nsource: {}\n\n{}",
                        asset.skill_id,
                        asset.version.as_deref().unwrap_or("unversioned"),
                        asset.source_ref,
                        asset.content
                    ),
                );
                item.authority = ContextAuthority::Project;
                item.source_id = Some(format!("skill:{}", asset.skill_id));
                item.source_version = asset.version.clone();
                item.source_reason = Some("runtime selected prompt-only skill".to_string());
                item.evidence = vec![asset.source_ref.clone()];
                self.push_next_model_context_item(item);
            }
        }

        let Some(store) = self.session_store.as_ref() else {
            return Ok(());
        };
        let activation_event = activation.activation.to_session_domain_event(0);
        store
            .append_session_domain_event_allocating_sequence(&activation_event)
            .await
            .map_err(|error| {
                RuntimeError::new(format!(
                    "runtime skill activation persistence failed for session {}: {error}",
                    activation.activation.session_id
                ))
            })?;
        if let Some(candidate) = memory_candidate_from_skill_activation(
            &activation.activation,
            &SkillMemoryPolicy::default(),
        ) {
            if let Some(event) =
                skill_memory_candidate_session_event(&activation.activation, &candidate, 0)
            {
                store
                    .append_session_domain_event_allocating_sequence(&event)
                    .await
                    .map_err(|error| {
                        RuntimeError::new(format!(
                            "runtime skill memory bridge persistence failed for session {}: {error}",
                            activation.activation.session_id
                        ))
                    })?;
            }
        }
        Ok(())
    }

    /// Require one text-only provider response after a governed evidence
    /// checkpoint. The normal dynamic tool exposure is restored afterwards.
    pub(crate) fn require_next_model_final_response(&self) {
        self.next_model_text_only.store(true, Ordering::SeqCst);
    }

    /// Restrict exactly one provider request to an existing subset of tools.
    /// Tool discovery, authorization and resource ceilings remain authoritative;
    /// unknown names are omitted rather than activated.
    pub(crate) fn require_next_model_tools(&self, tool_ids: impl IntoIterator<Item = String>) {
        if let Ok(mut allowlist) = self.next_model_tool_allowlist.lock() {
            *allowlist = Some(tool_ids.into_iter().collect());
        }
    }

    /// Override reasoning effort for exactly one provider request. Provider
    /// adapters ignore this when the selected model has no compatible control.
    pub(crate) fn require_next_model_reasoning_effort(&self, effort: impl Into<String>) {
        if let Ok(mut next) = self.next_model_reasoning_effort.lock() {
            *next = Some(effort.into());
        }
    }

    /// Run one clean, zero-tool synthesis request from the original objective
    /// and already-committed evidence receipts. Unlike the normal continuation
    /// path, this request carries no exploratory assistant/tool-call history,
    /// so a provider that became stuck repeating its prior tool protocol gets
    /// one bounded opportunity to convert evidence into a deliverable.
    pub(crate) async fn execute_clean_terminal_synthesis(
        &mut self,
        objective: &str,
        evidence: &str,
    ) -> Result<ModelStepResult, RuntimeError> {
        let started_at = Instant::now();
        let revision = self
            .tool_exposure_revision
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let discovery = self.tool_executor.tool_discovery_receipt();
        let deferred = discovery
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect();
        self.api_client.configure_tool_exposure(
            ToolExposureState {
                catalog_revision: discovery.catalog_revision,
                bootstrap: Default::default(),
                active: Default::default(),
                deferred,
                reason: "clean terminal synthesis exposes no executable tools".to_string(),
                revision,
                fallback_full: false,
            }
            .projection(0),
        );

        let mut prompt = PromptAssembly::new(self.system_prompt.clone());
        prompt.push_trusted_system(
            "## Clean terminal synthesis\n\
             Produce the final user-facing answer for the supplied objective from the checked \
             evidence receipts only. This request has no tools and no continuation work. Do not \
             emit function calls, simulated tool markup, plans to inspect more data, or promises \
             to continue. Give the best supported conclusion now and state unresolved facts \
             explicitly.",
        );
        let evidence = if evidence.trim().is_empty() {
            "No checked tool receipt was available; give an honest bounded answer and name the missing evidence."
        } else {
            evidence
        };
        let messages = vec![ConversationMessage::user_text(format!(
            "Original objective:\n{objective}\n\nChecked evidence receipts:\n{evidence}\n\nReturn the final answer now."
        ))];
        let inventory = self.api_client.context_inventory();
        let mut last_error = None;

        for model in self.model_candidates_for_turn(objective) {
            let mut request =
                match self.pack_provider_attempt(&prompt, &messages, &model, inventory) {
                    Ok(request) => request,
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
            let mut evaluation_reservation =
                match EvaluationProviderTokenReservation::acquire(&mut request) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
            let cancellation = self.cancellation_token.clone();
            let mut stream = self.api_client.stream(request);
            let mut text = String::new();
            let mut thinking = String::new();
            let mut signature = String::new();
            let mut calls = Vec::new();
            let mut usage = TokenUsage::default();
            let mut cache_events = Vec::new();
            let mut effective_model = None;
            let mut failed = None;
            use futures::StreamExt;
            loop {
                let event = tokio::select! {
                    () = cancellation.cancelled() => {
                        failed = Some(RuntimeError::new(
                            "turn cancelled during clean terminal provider stream",
                        ));
                        break;
                    }
                    event = stream.next() => match event {
                        Some(event) => event,
                        None => break,
                    }
                };
                match event {
                    Ok(AssistantEvent::ProviderModel { model }) => {
                        effective_model = Some(model);
                    }
                    Ok(AssistantEvent::TextDelta(delta)) => text.push_str(&delta),
                    Ok(AssistantEvent::ThinkingDelta(delta)) => thinking.push_str(&delta),
                    Ok(AssistantEvent::SignatureDelta(delta)) => signature.push_str(&delta),
                    Ok(AssistantEvent::ToolUse { id, name, input }) => {
                        calls.push(ModelToolCall {
                            id,
                            name,
                            input,
                            depends_on: Vec::new(),
                        });
                    }
                    Ok(AssistantEvent::Usage(value)) => usage = value,
                    Ok(AssistantEvent::PromptCache(value)) => cache_events.push(value),
                    Ok(AssistantEvent::MessageStop) => break,
                    Ok(
                        AssistantEvent::ToolStart { .. }
                        | AssistantEvent::ToolProgress { .. }
                        | AssistantEvent::ToolComplete { .. },
                    ) => {}
                    Err(error) => {
                        failed = Some(error);
                        break;
                    }
                }
            }
            drop(stream);
            if let Some(reservation) = evaluation_reservation.as_mut() {
                reservation.reconcile(usage);
            }
            if let Some(error) = failed {
                last_error = Some(error);
                continue;
            }

            let mut blocks = Vec::new();
            if !thinking.is_empty() {
                blocks.push(ContentBlock::Thinking {
                    thinking,
                    signature: (!signature.is_empty()).then_some(signature),
                });
            }
            blocks.push(ContentBlock::Text { text: text.clone() });
            for call in &calls {
                blocks.push(ContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone(),
                });
            }
            return Ok(ModelStepResult {
                intent: classify_model_step_intent(text, calls),
                assistant_message: ConversationMessage {
                    role: crate::session::MessageRole::Assistant,
                    blocks,
                    usage: Some(usage),
                },
                usage,
                prompt_cache_events: cache_events,
                model: effective_model.or(Some(model)),
                wall_duration_ms: millis_since(started_at).max(1),
                text_only_response: true,
            });
        }

        Err(last_error.unwrap_or_else(|| {
            RuntimeError::new("clean terminal synthesis exhausted all provider candidates")
        }))
    }

    /// Remove runtime-owned context items from a given source.
    pub fn clear_external_context_source(&self, source: ContextSourceKind) {
        if let Ok(mut guard) = self.external_context_items.lock() {
            guard.retain(|item| item.source != source);
        }
    }

    /// Inject resume/handoff state into the next runtime context envelope.
    pub fn inject_resume_context(&self, packet: ResumeContextPacket) {
        let item = ContextRuntimeKernel::resume_item(&packet);
        self.clear_external_context_source(item.source);
        self.push_external_context_item(item);
    }

    fn external_context_items(&self) -> Vec<ContextItem> {
        self.external_context_items
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn tool_trace_context_items(&self) -> Vec<ContextItem> {
        self.tool_trace_context_items
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn clear_turn_tool_observations(&self) {
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.clear();
        }
        if let Ok(mut guard) = self.turn_evidence_audits.lock() {
            guard.clear();
        }
    }

    fn push_turn_tool_observation(&self, observation: ToolObservation) {
        if let Ok(mut guard) = self.turn_tool_observations.lock() {
            guard.push(observation);
        }
    }

    fn turn_tool_observations(&self) -> Vec<ToolObservation> {
        self.turn_tool_observations
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn push_turn_evidence_audit(&self, projection: EvidenceAuditProjection) {
        if let Ok(mut guard) = self.turn_evidence_audits.lock() {
            if let Some(existing) = guard
                .iter_mut()
                .find(|existing| existing.evidence_ref == projection.evidence_ref)
            {
                *existing = projection;
            } else {
                guard.push(projection);
            }
        }
    }

    fn turn_evidence_audits(&self) -> Vec<EvidenceAuditProjection> {
        self.turn_evidence_audits
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }

    fn existing_evidence_access(&self, evidence_ref: &EvidenceRef) -> Option<EvidenceAccessRef> {
        self.turn_evidence_audits.lock().ok().and_then(|guard| {
            guard
                .iter()
                .find(|projection| &projection.evidence_ref == evidence_ref)
                .and_then(|projection| projection.access.clone())
        })
    }

    fn current_tool_exposure_projection(
        &self,
    ) -> Option<harness_contract::tool::ToolExposureProjection> {
        let schema_tokens = self.api_client.context_inventory().tool_schema_tokens;
        self.tool_exposure_state
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|state| state.projection(schema_tokens)))
    }

    /// Overlay Gateway's catalog-level capability result with the Runtime-owned
    /// provider schema projection for this exact request. Gateway can describe
    /// every registered backend tool, but only Conversation knows which schemas
    /// were actually sent to the model after discovery and permission filtering.
    fn project_runtime_capabilities_for_model(&self, output: &str) -> String {
        let Ok(mut response) = serde_json::from_str::<serde_json::Value>(output) else {
            tracing::warn!("runtime_capabilities returned non-JSON output");
            return output.to_string();
        };
        let Some(object) = response.as_object_mut() else {
            tracing::warn!("runtime_capabilities returned a non-object JSON value");
            return output.to_string();
        };
        let Some(exposure) = self.current_tool_exposure_projection() else {
            return output.to_string();
        };

        let catalog_tool_names = object
            .remove("available_tool_names")
            .unwrap_or_else(|| serde_json::json!([]));
        let active_function_schemas = exposure.active_ids.clone();
        let runtime_orchestrate_active = active_function_schemas
            .iter()
            .any(|name| name == "runtime_orchestrate");
        let tool_search_active = active_function_schemas
            .iter()
            .any(|name| name == "ToolSearch");

        object.insert("catalog_tool_names".to_string(), catalog_tool_names);
        object.insert(
            "tool_visibility".to_string(),
            serde_json::json!({
                "active_function_schemas": active_function_schemas,
                "deferred_catalog_tools": exposure.deferred_ids,
                "catalog_revision": exposure.catalog_revision,
                "exposure_revision": exposure.exposure_revision,
                "activation_protocol": if tool_search_active {
                    "Call ToolSearch once with a focused query. Accepted candidates become callable native function schemas on the next model request."
                } else {
                    "No discovery schema is active on this request; do not simulate a deferred catalog tool."
                }
            }),
        );

        if let Some(strategy) = object
            .get_mut("strategy")
            .and_then(serde_json::Value::as_object_mut)
        {
            strategy.insert(
                "model_callable_tools".to_string(),
                serde_json::json!(exposure.active_ids),
            );
        }

        let orchestration_backend_available = object
            .get("runtime_orchestrate")
            .and_then(|value| value.get("available"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if let Some(orchestration) = object
            .get_mut("runtime_orchestrate")
            .and_then(serde_json::Value::as_object_mut)
        {
            orchestration.insert(
                "schema_active".to_string(),
                serde_json::Value::Bool(runtime_orchestrate_active),
            );
            orchestration.insert(
                "available".to_string(),
                serde_json::Value::Bool(
                    orchestration_backend_available && runtime_orchestrate_active,
                ),
            );
            if !runtime_orchestrate_active {
                let reasons = orchestration
                    .entry("blocked_reasons")
                    .or_insert_with(|| serde_json::json!([]));
                if let Some(reasons) = reasons.as_array_mut() {
                    if !reasons
                        .iter()
                        .any(|reason| reason == "runtime_orchestrate_not_active_in_current_schema")
                    {
                        reasons.push(serde_json::json!(
                            "runtime_orchestrate_not_active_in_current_schema"
                        ));
                    }
                }
            }
        }
        if let Some(action_plane) = object
            .get_mut("action_plane")
            .and_then(serde_json::Value::as_object_mut)
        {
            action_plane.insert(
                "can_execute_now".to_string(),
                serde_json::Value::Bool(
                    orchestration_backend_available && runtime_orchestrate_active,
                ),
            );
            if !runtime_orchestrate_active {
                action_plane.insert(
                    "recommended_next_tool".to_string(),
                    serde_json::Value::String(if tool_search_active {
                        "ToolSearch".to_string()
                    } else {
                        "none".to_string()
                    }),
                );
            }
        }

        serde_json::to_string(&response).unwrap_or_else(|error| {
            tracing::warn!(%error, "failed to serialize projected runtime capabilities");
            output.to_string()
        })
    }

    fn activate_tool_discovery(&self, output: &str) {
        let Ok(discovery) =
            serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(output)
        else {
            tracing::warn!("ToolSearch returned a non-canonical discovery receipt");
            return;
        };
        let Ok(mut guard) = self.tool_exposure_state.lock() else {
            tracing::warn!("tool exposure state lock poisoned");
            return;
        };
        let Some(state) = guard.as_mut() else {
            tracing::warn!("ToolSearch completed before tool exposure was initialized");
            return;
        };
        let allowed_ids = state
            .bootstrap
            .iter()
            .chain(state.active.iter())
            .chain(state.deferred.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let policy = ToolExposurePolicy {
            allowed_ids,
            maximum_permission: contract_permission_mode(self.permission_policy.active_mode()),
            supports_dynamic_exposure: true,
        };
        let activation = ToolExposurePlanner.activate(state, &discovery, &policy);
        tracing::info!(
            catalog_revision = activation.catalog_revision,
            previous_exposure_revision = activation.previous_exposure_revision,
            exposure_revision = activation.exposure_revision,
            activated = ?activation.activated_ids().collect::<Vec<_>>(),
            "ToolSearch activation applied to the next provider request"
        );
    }

    fn remember_tool_trace_from_message(&self, message: &ConversationMessage) {
        let Some(ContentBlock::ToolResult {
            tool_use_id,
            tool_name,
            output,
            is_error,
        }) = message.blocks.first()
        else {
            return;
        };
        let summary = output
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(600)
            .collect::<String>();
        let packet = ToolTracePacket {
            tool_name: tool_name.clone(),
            invocation_id: tool_use_id.clone(),
            status: if *is_error {
                ToolTraceStatus::Failed
            } else {
                ToolTraceStatus::Succeeded
            },
            summary,
            changed_files: Vec::new(),
            evidence_ids: vec![tool_use_id.clone()],
            token_estimate: (output.len() as u64).div_ceil(4).min(256).max(1),
        };
        let mut item = ContextRuntimeKernel::tool_trace_item(&packet);
        item.score = if *is_error { 0.9 } else { 0.65 };
        if let Ok(mut guard) = self.tool_trace_context_items.lock() {
            guard.retain(|existing| existing.id != item.id);
            guard.push(item);
            let overflow = guard.len().saturating_sub(8);
            if overflow > 0 {
                guard.drain(0..overflow);
            }
        }
    }

    fn remember_context_envelope(&self, envelope: ContextEnvelope) {
        if let Ok(mut guard) = self.last_context_envelope.lock() {
            *guard = Some(envelope.clone());
        }
        self.persist_context_envelope(envelope.clone());
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::ContextEnvelope { envelope });
        }
    }

    fn persist_context_envelope(&self, envelope: ContextEnvelope) {
        let Some(store) = self.session_store.as_ref() else {
            return;
        };
        let session_id = envelope.identity.session_id.clone();
        let envelope_id = envelope.id.clone();
        let payload = serde_json::json!({
            "type": "ContextEnvelope",
            "envelope_id": envelope_id,
            "session_id": session_id,
            "agent_id": envelope.identity.agent_id.clone(),
            "profile": envelope.profile,
            "diagnostics": envelope.diagnostics.clone(),
            "budget": envelope.budget.clone(),
            "hashes": {
                "stable_head": envelope.diagnostics.stable_head_hash,
                "runtime_header": envelope.diagnostics.runtime_header_hash,
                "dynamic_tail": envelope.diagnostics.dynamic_tail_hash,
            },
            "envelope": envelope,
        });
        let store = Arc::clone(store);
        tokio::spawn(async move {
            let created_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);
            let event = memory::SessionEvent {
                session_id: session_id.clone(),
                event_type: "ContextEnvelope".to_string(),
                event_json: payload.to_string(),
                sequence: 0,
                created_at_ms,
            };
            match store
                .append_context_envelope_event_if_absent_allocating_sequence(&event)
                .await
            {
                Ok(Some(_)) => {}
                Ok(None) => {
                    tracing::debug!(session_id, "context envelope event already persisted");
                }
                Err(error) => {
                    tracing::warn!(%error, session_id, "context envelope event append failed");
                }
            }
        });
    }

    async fn remember_context_turn_report(
        &self,
        report: ContextTurnReport,
    ) -> Result<(), RuntimeError> {
        self.persist_context_turn_report(&report).await?;
        if let Ok(mut guard) = self.last_context_turn_report.lock() {
            *guard = Some(report);
        }
        Ok(())
    }

    async fn persist_context_turn_report(
        &self,
        report: &ContextTurnReport,
    ) -> Result<(), RuntimeError> {
        let Some(store) = self.session_store.as_ref() else {
            // Embedding callers may intentionally run without a durable
            // session carrier. They receive the in-memory report but cannot
            // claim restart/audit durability.
            return Ok(());
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "type": "ContextTurnReport",
            "report": report,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let event = memory::SessionDomainEvent::new(
            session_id.clone(),
            0,
            memory::SessionDomainScope::Context,
            "context.turn_report",
            payload,
            created_at_ms,
        );
        store
            .append_session_domain_event_allocating_sequence(&event)
            .await
            .map_err(|error| {
                RuntimeError::new(format!(
                    "context governance persistence failed for session `{session_id}`: {error}"
                ))
            })?;
        Ok(())
    }

    fn finalize_context_prompt(
        &self,
        user_input: &str,
        envelope: ContextEnvelope,
        knowledge: Option<KnowledgeTurnReport>,
    ) -> PromptAssembly {
        let fact_decision = self.runtime_fact_decision_for_context(user_input, &envelope);
        let report = ContextRuntimeKernel::governance_report(
            &envelope,
            knowledge.as_ref(),
            fact_decision,
            None,
        );
        self.remember_context_governance_report(report);
        let prompt = Self::provider_prompt_from_envelope(&envelope);
        self.remember_context_envelope(envelope);
        prompt
    }

    fn remember_context_governance_report(&self, report: RuntimeContextGovernanceReport) {
        self.persist_context_governance_report(report);
    }

    fn persist_context_governance_report(&self, report: RuntimeContextGovernanceReport) {
        let Some(store) = self.session_store.as_ref() else {
            return;
        };
        let session_id = report.session_id.clone();
        let envelope_id = report.envelope_id.clone();
        let context_epoch = report.context_epoch.clone();
        let payload = serde_json::json!({
            "type": "RuntimeContextGovernanceReport",
            "report": report,
        });
        let store = Arc::clone(store);
        tokio::spawn(async move {
            let created_at_ms = now_ms();
            let mut event = memory::SessionDomainEvent::new(
                session_id.clone(),
                0,
                memory::SessionDomainScope::Context,
                "context.governance_report",
                payload,
                created_at_ms,
            );
            event.status = Some("recorded".to_string());
            event.refs.extend([
                memory::SessionDomainRef {
                    ref_type: "context_envelope".to_string(),
                    id: envelope_id,
                    label: None,
                },
                memory::SessionDomainRef {
                    ref_type: "context_epoch".to_string(),
                    id: context_epoch,
                    label: None,
                },
            ]);
            if let Err(error) = store.append_session_domain_event(&event).await {
                tracing::warn!(%error, session_id, "context governance domain event append failed");
            }
        });
    }

    fn runtime_fact_decision_for_context(
        &self,
        user_input: &str,
        envelope: &ContextEnvelope,
    ) -> Option<RuntimeContextFactDecision> {
        let trigger = fact_extraction_trigger_for_turn(user_input, envelope.profile)?;
        let policy = RuntimeFactExtractionPolicy {
            provider_available: false,
            ..RuntimeFactExtractionPolicy::default()
        };
        let scheduler = RuntimeFactExtractionScheduler::new(policy);
        let decision = scheduler.decide(trigger);
        let evidence_refs = envelope
            .source_registry
            .iter()
            .map(|source| source.source_id.clone())
            .take(32)
            .collect::<Vec<_>>();
        let input = RuntimeFactExtractionInput::new(trigger, user_input)
            .with_session_id(Some(envelope.identity.session_id.clone()))
            .with_project_id(envelope.identity.project_id.clone())
            .with_task_id(envelope.identity.task_id.clone())
            .with_team_id(envelope.identity.team_id.clone())
            .with_agent_id(Some(envelope.identity.agent_id.clone()))
            .with_evidence_refs(evidence_refs)
            .with_token_budget(Some(envelope.budget.total_tokens));
        let extractor = RuleFactExtractor;
        let batch = extractor.extract(&input);
        let event = FactExtractionRuntimeEvent::from_decision(
            &decision,
            extractor.extractor_version(),
            batch.candidates.len(),
            batch.source_evidence.len(),
            batch.token_usage,
        );
        if let Some(store) = self.session_store.as_ref() {
            let mut domain_event = memory::SessionDomainEvent::new(
                envelope.identity.session_id.clone(),
                0,
                memory::SessionDomainScope::Context,
                "context.fact_candidate_review",
                serde_json::json!({
                    "event": event,
                    "batch_id": batch.batch_id.as_str(),
                    "candidate_count": batch.candidates.len(),
                    "candidates": batch.candidates,
                    "promotion": "review_required",
                }),
                now_ms(),
            );
            domain_event.status = Some("reviewable".to_string());
            domain_event.refs.push(memory::SessionDomainRef {
                ref_type: "context_envelope".to_string(),
                id: envelope.id.clone(),
                label: None,
            });
            let store = Arc::clone(store);
            let session_id = envelope.identity.session_id.clone();
            tokio::spawn(async move {
                if let Err(error) = store.append_session_domain_event(&domain_event).await {
                    tracing::warn!(%error, session_id, "fact candidate domain event append failed");
                }
            });
        }
        Some(RuntimeContextFactDecision {
            trigger: format!("{:?}", decision.trigger),
            mode: decision.mode.as_str().to_string(),
            degraded: decision.degraded,
            reason: decision.reason,
            candidate_count: batch.candidates.len(),
            review_required: true,
        })
    }

    fn context_budget_tokens(&self) -> u64 {
        self.runtime_budget_plan().subsystem_budget_tokens
    }

    fn runtime_budget_plan(&self) -> RuntimeBudgetPlan {
        let model_max_output = self
            .model
            .as_deref()
            .filter(|model| !model.is_empty())
            .map_or(0, |model| {
                bounded_provider_output_tokens(model, self.context_window_for_model(model))
            });
        RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: self.model_context_window,
            model_max_output_tokens: model_max_output,
            subsystem_budget_ratio_bp: self.subsystem_budget_ratio_bp,
            profile: self.context_profile(),
            autonomy_mode: None,
        })
    }

    /// A fallback route is only safe when the prepared context fits every
    /// candidate that may receive it. Use the narrowest configured candidate
    /// window and output reservation before context selection, rather than
    /// constructing a large primary-only packet and hoping fallback accepts it.
    fn runtime_budget_plan_for_candidates(&self, candidates: &[String]) -> RuntimeBudgetPlan {
        let mut windows = candidates
            .iter()
            .filter(|model| !model.trim().is_empty())
            .map(|model| self.context_window_for_model(model));
        let model_context_window = windows.next().map_or(self.model_context_window, |first| {
            windows.fold(first, u32::min)
        });
        let mut outputs = candidates
            .iter()
            .filter(|model| !model.trim().is_empty())
            .map(|model| {
                bounded_provider_output_tokens(model, self.context_window_for_model(model))
            });
        let model_max_output_tokens = outputs
            .next()
            .map_or(0, |first| outputs.fold(first, u32::min));
        RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window,
            model_max_output_tokens,
            subsystem_budget_ratio_bp: self.subsystem_budget_ratio_bp,
            profile: self.context_profile(),
            autonomy_mode: None,
        })
    }

    fn context_window_resolution_for_model(
        &self,
        model: &str,
    ) -> provider::ModelContextWindowResolution {
        let mut resolution = if self.model.as_deref() == Some(model) {
            provider::ModelContextWindowResolution {
                tokens: self.model_context_window,
                source: self.model_context_window_source,
            }
        } else {
            provider::model_context_window_resolution(model, Some(&self.model_context_windows))
        };
        if let Ok(calibrated) = self.calibrated_model_context_windows.lock() {
            if let Some(&tokens) = calibrated
                .get(model)
                .filter(|tokens| **tokens < resolution.tokens)
            {
                resolution.tokens = tokens;
                resolution.source = provider::ModelContextWindowSource::Calibrated;
            }
        }
        resolution
    }

    fn context_window_for_model(&self, model: &str) -> u32 {
        self.context_window_resolution_for_model(model).tokens
    }

    fn calibrate_model_context_window(&self, model: &str, observed_tokens: u32) -> bool {
        if observed_tokens < 1_024 {
            return false;
        }
        let current = self.context_window_for_model(model);
        if observed_tokens >= current {
            return false;
        }
        let Ok(mut calibrated) = self.calibrated_model_context_windows.lock() else {
            return false;
        };
        let next = calibrated
            .get(model)
            .copied()
            .map_or(observed_tokens, |existing| existing.min(observed_tokens));
        calibrated.insert(model.to_string(), next);
        true
    }

    fn memory_turn_context(&self) -> MemoryTurnContext {
        let session = self.session();
        let project_id = memory_project_id_for_session(&session);
        let task_id = Some(format!("session-task-{}", session.session_id));
        MemoryTurnContext::new(session.session_id, self.memory_agent_id.clone())
            .with_definition_lineage_id(self.memory_definition_lineage_id.clone())
            .with_project_id(project_id)
            .with_task_id(task_id)
            .with_team_id(self.memory_team_id.clone())
            .with_cognitive_read_scopes(self.memory_read_scopes.clone())
    }

    fn build_context_turn_report(
        &self,
        turn_id: &str,
        usage: TokenUsage,
        auto_compaction: Option<AutoCompactionEvent>,
    ) -> ContextTurnReport {
        let used_tokens = estimate_session_tokens(&self.session()) as u64;
        let pressure = ContextPressureState::new(
            format!("{:?}", self.context_profile()),
            self.context_budget_tokens(),
            used_tokens,
        )
        .with_reserved_tokens(u64::from(usage.output_tokens));
        let mut decision = ContextGovernanceDecision::new(
            pressure.clone(),
            if pressure.compaction_recommended {
                "context pressure exceeded governance threshold"
            } else {
                "context pressure within governance budget"
            },
        );
        let compaction_receipt = auto_compaction
            .as_ref()
            .and_then(|compaction| compaction.compaction_receipt.clone());
        if let Some(compaction) = auto_compaction.as_ref() {
            decision.compact = true;
            decision.estimated_tokens_to_reclaim = compaction.removed_message_count as u64;
        }
        let mut report = ContextTurnReport::new(turn_id.to_string(), pressure)
            .with_output_token_estimate(u64::from(usage.output_tokens))
            .with_governance_decision(decision);
        if let Ok(ledger) = self.turn_context_ledger.lock() {
            report = report.with_ledger(ledger.projection());
        }
        if let Some(receipt) = compaction_receipt {
            report = report.with_compaction_receipt(receipt);
        }
        for observation in self.turn_tool_observations() {
            report = report.with_observation(observation);
        }
        if let Some(exposure) = self.current_tool_exposure_projection() {
            report = report.with_tool_exposure(exposure);
        }
        for projection in self.turn_evidence_audits() {
            report = report.with_audit_projection(projection);
        }
        if let Some(knowledge) = self.take_turn_knowledge_report() {
            report = report.with_knowledge(knowledge);
        }
        report
    }

    fn set_turn_knowledge_report(&self, report: harness_contract::knowledge::KnowledgeTurnReport) {
        if let Ok(mut guard) = self.turn_knowledge_report.lock() {
            *guard = Some(report);
        }
    }

    fn take_turn_knowledge_report(
        &self,
    ) -> Option<harness_contract::knowledge::KnowledgeTurnReport> {
        self.turn_knowledge_report
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }

    fn build_context_envelope(
        &self,
        user_input: &str,
        dynamic_items: Vec<ContextItem>,
        omitted: Vec<ContextOmission>,
        degraded_sources: Vec<ContextSourceKind>,
        total_budget_tokens: u64,
    ) -> ContextEnvelope {
        let session_id = self.session().session_id;
        let profile = self.context_profile();
        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = ContextRuntimeKernel::mode_for_profile(profile);
        let governance_report_id =
            ContextRuntimeKernel::governance_report_id(&session_id, user_input);
        let mut runtime_header = ContextRuntimeKernel::runtime_header(&identity, profile);
        runtime_header.push(format!(
            "context_governance_report_id:{governance_report_id}"
        ));
        let mut selected_items = self.external_context_items();
        if let Ok(cwd) = std::env::current_dir() {
            selected_items.extend(crate::prompt::discover_project_context_items_for_profile(
                &cwd, profile,
            ));
        }
        selected_items.extend(self.tool_trace_context_items());
        selected_items.extend(dynamic_items);
        let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile,
            runtime_header,
            identity,
            intent: user_input.to_string(),
            stable_head: self.system_prompt.clone(),
            dynamic_items: selected_items,
            omitted,
            total_budget_tokens,
        });
        envelope.diagnostics.degraded_sources = degraded_sources;
        envelope
    }

    fn provider_prompt_from_envelope(envelope: &ContextEnvelope) -> PromptAssembly {
        let mut prompt = PromptAssembly::new(envelope.assembled.stable_head.clone());
        for header in &envelope.assembled.runtime_header {
            prompt.push_trusted_system(header.clone());
        }
        for item in &envelope.selected {
            prompt.push_context_item(item);
        }
        prompt
    }

    /// Pack a previously collected context snapshot for one concrete provider
    /// attempt. This is deliberately pure: a fallback never re-reads memory
    /// or mutates the session, it only applies the narrower candidate budget.
    fn pack_provider_attempt(
        &self,
        prompt: &PromptAssembly,
        messages: &[ConversationMessage],
        model: &str,
        inventory: ProviderContextInventory,
    ) -> Result<ApiRequest, RuntimeError> {
        let window_resolution = self.context_window_resolution_for_model(model);
        let context_window_tokens = u64::from(window_resolution.tokens);
        let requested_output_tokens = u64::from(bounded_provider_output_tokens(
            model,
            window_resolution.tokens,
        ));
        // Protocol framing is deliberately explicit and conservative. Schema
        // payload itself is accounted separately from fixed wire framing.
        let protocol_overhead_tokens =
            128u64.saturating_add(u64::from(inventory.tool_count as u32).saturating_mul(12));
        let safety_margin_tokens = (context_window_tokens / 100).clamp(128, 2_048);
        let fixed_input_tokens =
            crate::context_ledger::estimate_text_tokens(&prompt.trusted_system.join("\n\n"))
                .saturating_add(conversation_messages_token_estimate(messages))
                .saturating_add(inventory.tool_schema_tokens);
        let mut budget = crate::context_ledger::RequestBudgetReport::for_attempt(
            model,
            context_window_tokens,
            requested_output_tokens,
            protocol_overhead_tokens,
            safety_margin_tokens,
            fixed_input_tokens,
        );
        budget.set_context_window_source(window_resolution.source.as_str());
        if !budget.executable {
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` cannot fit fixed request components: fixed={} hard_input_cap={} window={} output_reserve={}",
                budget.fixed_input_tokens,
                budget.hard_input_cap_tokens,
                budget.context_window_tokens,
                budget.requested_output_tokens,
            )));
        }
        let (packed_prompt, dynamic_tokens, omitted_packet_ids, omitted_packet_reasons) = prompt
            .pack_for_hard_cap(budget.dynamic_hard_remaining())
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        budget.record_dynamic_packets(dynamic_tokens, omitted_packet_ids, omitted_packet_reasons);
        if !budget.executable {
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` exceeded its hard request budget after context packing"
            )));
        }
        Ok(ApiRequest {
            prompt: packed_prompt,
            messages: messages.to_vec(),
            model: model.to_string(),
            reasoning_effort_override: None,
            budget,
        })
    }

    pub fn with_model_context_window(mut self, ctx_window: u32) -> Self {
        if ctx_window >= 1_024 {
            self.model_context_window = ctx_window;
            // Hosts often pass the same registry/config resolution explicitly
            // for workspace sizing. Preserve its real provenance instead of
            // falsely reporting every host value as a user override.
            self.model_context_window_source = self
                .model
                .as_deref()
                .map(|model| {
                    provider::model_context_window_resolution(
                        model,
                        Some(&self.model_context_windows),
                    )
                })
                .filter(|resolution| resolution.tokens == ctx_window)
                .map_or(
                    provider::ModelContextWindowSource::Configured,
                    |resolution| resolution.source,
                );
        }
        let plan = self.runtime_budget_plan();
        apply_runtime_budget_to_control_policy(&mut self.runtime_control_policy, &plan);
        self
    }

    pub fn set_active_model(&mut self, model: impl Into<String>) {
        let model = model.into();
        if !model.trim().is_empty() {
            // A session model switch must not inherit the previous model's
            // window. Resolve this model independently so explicit per-model
            // configuration remains authoritative across a live session.
            if self.model.as_deref() != Some(model.as_str()) {
                let resolution = provider::model_context_window_resolution(
                    &model,
                    Some(&self.model_context_windows),
                );
                self.model_context_window = resolution.tokens;
                self.model_context_window_source = resolution.source;
            }
            self.model = Some(model);
        }
    }

    #[must_use]
    pub(crate) fn active_model_lease(&self) -> String {
        self.model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
            .unwrap_or("default")
            .to_string()
    }

    /// Set a tool callback for real-time execution visualization (P0-2).
    ///
    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_tool_callback(mut self, callback: Arc<dyn ToolCallback>) -> Self {
        self.tool_callback = Some(callback);
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_sse_callback(mut self, callback: Arc<dyn Fn(String) + Send + Sync>) -> Self {
        self.sse_callback = Some(callback);
        self
    }

    /// Set the SSE callback on an already-constructed runtime instance.
    pub fn set_sse_callback(&mut self, callback: Arc<dyn Fn(String) + Send + Sync>) {
        self.sse_callback = Some(callback);
    }

    /// Clear the SSE callback from this runtime instance.
    pub fn clear_sse_callback(&mut self) {
        self.sse_callback = None;
    }

    #[must_use]
    pub fn with_session_store(
        mut self,
        store: Arc<memory::session_store::UnifiedSessionStore>,
    ) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Select whether `dual_write_message` may persist transcript rows. Runtime
    /// domain events and context/evidence persistence remain enabled.
    pub fn set_transcript_persistence(&mut self, enabled: bool) {
        self.transcript_persistence = enabled;
    }

    /// Attach the durable store that owns tool, graph, agent, and task execution state.
    #[must_use]
    pub(crate) fn with_runtime_event_store(mut self, store: Arc<RuntimeEventStore>) -> Self {
        self.runtime_event_store = Some(store);
        self
    }

    /// Attach a [`SessionEventLog`] for time-travel debugging and session rebuild.
    #[must_use]
    pub fn with_event_log(mut self, log: SessionEventLog) -> Self {
        self.event_log = Some(std::sync::Mutex::new(log));
        self
    }

    /// # Safety
    /// The callback MUST NOT capture an `Arc` to the `ConversationRuntime`
    /// itself, as this would create a reference cycle and leak memory.
    /// The runtime uses `Arc` ownership; callbacks should use `Weak` if
    /// they need to reference the runtime.
    #[must_use]
    pub fn with_memory_callback(mut self, callback: Arc<dyn MemoryCallback>) -> Self {
        self.memory_callback = Some(callback);
        self
    }

    pub fn set_memory_callback(&mut self, callback: Arc<dyn MemoryCallback>) {
        self.memory_callback = Some(callback);
    }

    /// Set the smart approval gate for intelligent command approval (P0-1).
    #[must_use]
    pub fn with_approval_gate(
        mut self,
        gate: Arc<crate::approval_gate::SmartApprovalGate>,
    ) -> Self {
        self.approval_gate = Some(gate);
        self
    }

    /// Provide Skill capability profiles already inspected by the Skill asset
    /// layer. Runtime consumes these profiles during activation, but does not
    /// inspect packages or own the registry.
    #[must_use]
    pub fn with_skill_profiles(mut self, profiles: Vec<SkillCapabilityProfile>) -> Self {
        self.skill_profiles = profiles;
        self
    }

    /// Configure the agent-scoped Skill visibility and adapter ceiling used by
    /// runtime activation.
    #[must_use]
    pub fn with_agent_skill_profile(mut self, profile: AgentSkillProfile) -> Self {
        self.agent_skill_profile = profile;
        self
    }

    /// Provide prompt assets already inspected by the Skill layer. Only an
    /// asset selected by Runtime is injected for a single model request.
    #[must_use]
    pub fn with_skill_prompt_assets(mut self, assets: Vec<RuntimeSkillPromptAsset>) -> Self {
        self.skill_prompt_assets = assets;
        self
    }

    /// Bind the Runtime's immutable Agent instance identity to memory
    /// operations for this conversation. This is set only by Runtime-owned
    /// child execution, never by a Surface request field.
    #[must_use]
    pub fn with_memory_identity(
        mut self,
        agent_id: impl Into<String>,
        definition_lineage_id: Option<String>,
        team_id: Option<String>,
        read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    ) -> Self {
        self.memory_agent_id = agent_id.into();
        self.memory_definition_lineage_id = definition_lineage_id;
        self.memory_team_id = team_id;
        self.memory_read_scopes = read_scopes;
        self
    }

    #[must_use]
    pub fn with_runtime_control_policy(mut self, policy: RuntimeControlPolicy) -> Self {
        self.runtime_control_policy = policy;
        self
    }

    /// T35: Set a cancellation token for graceful shutdown.
    #[must_use]
    pub fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    #[must_use]
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Attach a CowdEventBus for domain event emission.
    #[must_use]
    pub fn with_cowd_event_bus(mut self, bus: crate::cowd_event::CowdEventBus) -> Self {
        self.cowd_bus = Some(Arc::new(bus.clone()));
        self
    }

    /// Get a reference to the attached CowdEventBus, if any.
    pub fn cowd_bus(&self) -> Option<&crate::cowd_event::CowdEventBus> {
        self.cowd_bus.as_deref()
    }

    pub fn admit_session_input(
        &self,
        envelope: SessionInputEnvelope,
        state: crate::input_classifier::RuntimeInputState,
    ) -> SessionInputReceipt {
        let mut state = state;
        if state.active_turn_id.is_none() {
            state.active_turn_id = self.session_input_stream.active_turn_id();
        }
        let receipt = self.session_input_stream.admit(envelope, state);
        self.emit_session_input_projection(Some(receipt.clone()));
        receipt
    }

    #[must_use]
    pub fn session_input_projection(&self) -> SessionInputProjection {
        self.session_input_stream.projection()
    }

    #[must_use]
    pub fn active_turn_inbox(&self, turn_id: Option<TurnId>) -> TurnInboxSnapshot {
        self.session_input_stream.inbox_snapshot(turn_id)
    }

    #[must_use]
    pub fn session_input_stream(&self) -> crate::session_input::SessionInputStream {
        self.session_input_stream.clone()
    }

    fn emit_session_input_projection(&self, receipt: Option<SessionInputReceipt>) {
        if let Some(ref cowd) = self.cowd_bus {
            if let Some(receipt) = receipt {
                cowd.emit(crate::cowd_event::CowdEvent::SessionInputReceived { receipt });
            }
            cowd.emit(crate::cowd_event::CowdEvent::SessionInputProjection {
                projection: self.session_input_stream.projection(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnInboxUpdated {
                inbox: self.session_input_stream.inbox_snapshot(None),
            });
        }
    }

    fn consume_runtime_inputs_at_checkpoint(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
        prompt: &mut PromptAssembly,
    ) -> usize {
        let consumed = self
            .session_input_stream
            .consume_for_checkpoint(turn_id, checkpoint, 32);
        if !consumed.is_empty() {
            if let Ok(mut pending) = self.consumed_session_inputs.lock() {
                pending.extend(consumed.iter().cloned());
            }
        }
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            prompt.push_trusted_system(guidance);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            prompt.push_context_item(&item);
        }
        if let Some(ref cowd) = self.cowd_bus {
            if !consumed.is_empty() {
                cowd.emit(crate::cowd_event::CowdEvent::TurnInputCheckpointConsumed {
                    checkpoint,
                    consumed: consumed
                        .iter()
                        .map(crate::session_input::SessionInputRecord::to_inbox_item)
                        .collect(),
                });
            }
            cowd.emit(crate::cowd_event::CowdEvent::SessionInputProjection {
                projection: self.session_input_stream.projection(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::TurnInboxUpdated {
                inbox: self
                    .session_input_stream
                    .inbox_snapshot(Some(turn_id.clone())),
            });
        }
        consumed.len()
    }

    /// Drain compact receipts for inputs consumed during the current provider
    /// step. This does not mutate routing or create tasks; the graph host is
    /// the only caller allowed to decide whether a correction revises a Goal.
    pub fn take_consumed_session_inputs(&self) -> Vec<crate::session_input::SessionInputRecord> {
        self.consumed_session_inputs
            .lock()
            .map_or_else(|_| Vec::new(), |mut pending| std::mem::take(&mut *pending))
    }

    /// P1-05: Register a TurnCallback for generator-style injection after tool results.
    #[must_use]
    pub fn with_turn_callback(mut self, cb: TurnCallback) -> Self {
        self.turn_callback = Some(Arc::new(cb));
        self
    }

    #[must_use]
    pub fn with_hook_abort_signal(mut self, hook_abort_signal: HookAbortSignal) -> Self {
        self.hook_abort_signal = hook_abort_signal;
        self
    }

    #[must_use]
    pub fn with_hook_progress_reporter(
        self,
        hook_progress_reporter: Box<dyn HookProgressReporter + Send>,
    ) -> Self {
        *self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook_progress_reporter);
        self
    }

    #[must_use]
    pub fn with_session_tracer(mut self, session_tracer: SessionTracer) -> Self {
        self.session_tracer = Some(session_tracer);
        self
    }

    /// Override the memory manager with a pre-constructed instance.
    ///
    /// This is primarily useful in tests or when the caller wants full control
    /// over the [`CognitiveContextManager`] lifecycle.
    #[must_use]
    pub fn with_memory_manager(mut self, manager: Arc<CognitiveContextManager>) -> Self {
        self.memory_manager = Some(manager);
        self
    }

    /// Attach the Runtime-owned Fact/Matrix recall port to this conversation.
    /// The Binding is immutable for the host lifetime, so each turn evaluates
    /// the same data lease rather than re-resolving a surface default.
    #[must_use]
    pub fn with_reality_binding(
        mut self,
        port: crate::RealityRecallPort,
        binding: harness_contract::agent::AgentBindingSnapshot,
    ) -> Self {
        self.reality_recall = Some((port, binding));
        self
    }

    /// Return the source-level report for the most recently assembled model
    /// context.  The report proves a lease was applied even when it selected
    /// no Fact or Matrix evidence.
    #[must_use]
    pub fn last_reality_recall_report(&self) -> Option<crate::RealityRecallReport> {
        self.last_reality_recall_report
            .lock()
            .ok()
            .and_then(|report| report.clone())
    }

    pub fn with_gate_evaluator(mut self, evaluator: crate::gates::GateEvaluator) -> Self {
        self.gate_evaluator = Some(Arc::new(evaluator));
        self
    }

    /// Run all commit quality gates against the current state.
    pub fn check_commit_gates(
        &self,
        context: crate::gates::GateContext,
    ) -> Option<(bool, Vec<crate::gates::GateResult>)> {
        self.gate_evaluator
            .as_ref()
            .map(|evaluator| evaluator.evaluate_all(&context))
    }

    /// Explicitly disable the memory subsystem, regardless of feature config.
    #[must_use]
    pub fn without_memory(mut self) -> Self {
        self.memory_manager = None;
        self
    }

    /// Access the cognitive memory manager, if memory is enabled.
    ///
    /// Returns `None` when memory is disabled or failed to initialise.
    #[must_use]
    pub fn memory_manager(&self) -> Option<&Arc<CognitiveContextManager>> {
        self.memory_manager.as_ref()
    }

    /// Determine whether the current user message warrants multi-agent collaboration.

    fn record_context_event(
        &mut self,
        event_type: &str,
        category: &str,
        summary: &str,
        priority: u8,
    ) {
        let project_dir = std::env::current_dir()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_string()));
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.profiler
            .record_dedup(crate::context_profiler::SessionEvent {
                event_type: event_type.into(),
                category: category.into(),
                data_summary: summary.into(),
                priority,
                data_hash: 0, // computed by record_dedup
                timestamp,
                project_dir,
                attribution_confidence: 0.9,
            });
    }

    fn run_pre_tool_use_hook(&self, tool_name: &str, input: &str) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_pre_tool_use_with_context(
                tool_name,
                input,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    fn run_post_tool_use_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_with_context(
                tool_name,
                input,
                output,
                is_error,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    fn run_post_tool_use_failure_hook(
        &self,
        tool_name: &str,
        input: &str,
        output: &str,
    ) -> HookRunResult {
        let mut reporter_guard = self
            .hook_progress_reporter
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(reporter) = reporter_guard.as_mut() {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                Some(reporter.as_mut()),
            )
        } else {
            self.hook_runner.run_post_tool_use_failure_with_context(
                tool_name,
                input,
                output,
                Some(&self.hook_abort_signal),
                None,
            )
        }
    }

    /// Run a session health probe to verify the runtime is functional after compaction.
    /// Returns Ok(()) if healthy, Err if the session appears broken.
    /// Execute exactly one provider request and translate its response into a
    /// typed graph intent. This method never invokes ToolExecutor.
    pub(crate) async fn execute_model_step(
        &mut self,
        user_input: &str,
        first_step: bool,
    ) -> Result<ModelStepResult, RuntimeError> {
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new(
                "turn cancelled before provider execution",
            ));
        }
        let started_at = Instant::now();
        if first_step {
            self.clear_turn_tool_observations();
            if let Ok(mut preflight_compaction) = self.turn_preflight_compaction.lock() {
                *preflight_compaction = None;
            }
            let budget = self.runtime_budget_plan();
            if let Ok(mut ledger) = self.turn_context_ledger.lock() {
                ledger.reset(
                    budget.subsystem_budget_tokens,
                    budget.tool_result_budget.max_total_tokens as u64,
                );
            }
            self.record_turn_started(user_input);
            self.record_context_event("user_input", "user", &preview_chars(user_input, 200), 8);
            self.session
                .write()
                .await
                .push_user_text(user_input.to_string())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            self.dual_write_message(
                &ConversationMessage::user_text(user_input.to_string()),
                self.session().messages.len().wrapping_sub(1),
            );
            self.activate_skills_for_turn(user_input).await?;
        }

        if self.active_turn_strategy().is_none() {
            return Err(RuntimeError::new(
                "model execution requires the Host-admitted turn strategy owner",
            ));
        }
        let decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("turn strategy was not admitted"))?;
        if !decision.executable {
            return Err(RuntimeError::new(format!(
                "runtime strategy is not executable: {}",
                decision.blocked_reasons.join("; ")
            )));
        }

        let text_only_response = self.next_model_text_only.swap(false, Ordering::SeqCst);
        let one_shot_tool_allowlist = self
            .next_model_tool_allowlist
            .lock()
            .ok()
            .and_then(|mut allowlist| allowlist.take());
        let one_shot_reasoning_effort = self
            .next_model_reasoning_effort
            .lock()
            .ok()
            .and_then(|mut effort| effort.take());
        let explicitly_forbids_tool_use =
            harness_contract::strategy::prompt_explicitly_forbids_tool_use(user_input);
        let discovery = self.tool_executor.tool_discovery_receipt();
        let available_tools = discovery
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect::<Vec<_>>();
        let mut exposure = if first_step {
            tool_exposure_for_catalog(
                &discovery,
                contract_permission_mode(self.permission_policy.active_mode()),
            )
        } else {
            self.tool_exposure_state
                .lock()
                .ok()
                .and_then(|state| state.clone())
                .filter(|state| state.catalog_revision == discovery.catalog_revision)
                .unwrap_or_else(|| {
                    tool_exposure_for_catalog(
                        &discovery,
                        contract_permission_mode(self.permission_policy.active_mode()),
                    )
                })
        };
        let active_skill_tool_refs = self
            .active_skill_tool_refs
            .lock()
            .map(|tool_refs| tool_refs.clone())
            .unwrap_or_default();
        if !active_skill_tool_refs.is_empty() {
            let mut skill_discovery = discovery.clone();
            skill_discovery.activation_candidates = active_skill_tool_refs.into_iter().collect();
            let allowed_ids = exposure
                .bootstrap
                .iter()
                .chain(exposure.active.iter())
                .chain(exposure.deferred.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            let policy = ToolExposurePolicy {
                allowed_ids,
                maximum_permission: contract_permission_mode(self.permission_policy.active_mode()),
                supports_dynamic_exposure: true,
            };
            let activation = ToolExposurePlanner.activate(&mut exposure, &skill_discovery, &policy);
            tracing::info!(
                activated = ?activation.activated_ids().collect::<Vec<_>>(),
                "runtime Skill tool references applied to the current provider request"
            );
        }
        let one_shot_tool_overlay = one_shot_tool_allowlist.is_some();
        let mut exposure = if text_only_response || explicitly_forbids_tool_use {
            ToolExposureState {
                catalog_revision: exposure.catalog_revision,
                bootstrap: Default::default(),
                active: Default::default(),
                deferred: available_tools.iter().cloned().collect(),
                reason: if text_only_response {
                    "governed low-novelty checkpoint requires a text-only conclusion".to_string()
                } else {
                    "user explicitly prohibited tool calls for this request".to_string()
                },
                revision: exposure.revision.saturating_add(1),
                fallback_full: false,
            }
        } else if let Some(allowlist) = one_shot_tool_allowlist {
            let eligible_tools = exposure
                .bootstrap
                .iter()
                .chain(exposure.active.iter())
                .chain(exposure.deferred.iter())
                .cloned()
                .collect::<BTreeSet<_>>();
            let active = eligible_tools
                .iter()
                .filter(|tool_id| allowlist.contains(*tool_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            let deferred = available_tools
                .iter()
                .filter(|tool_id| !active.contains(*tool_id))
                .cloned()
                .collect::<BTreeSet<_>>();
            ToolExposureState {
                catalog_revision: exposure.catalog_revision,
                bootstrap: Default::default(),
                active,
                deferred,
                reason:
                    "governed focus checkpoint restricts the next action to required mutation tools"
                        .to_string(),
                revision: exposure.revision.saturating_add(1),
                fallback_full: false,
            }
        } else {
            exposure
        };
        exposure.revision = self
            .tool_exposure_revision
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        // A text-only checkpoint is a one-request overlay. Keep the normal
        // catalog state for discovery/projection, while still sending an
        // explicit empty schema set for this provider request.
        if !text_only_response && !explicitly_forbids_tool_use && !one_shot_tool_overlay {
            if let Ok(mut state) = self.tool_exposure_state.lock() {
                *state = Some(exposure.clone());
            }
        }
        self.api_client
            .configure_tool_exposure(exposure.projection(0));

        // Tool schemas are part of the request budget. Read their inventory
        // only after Runtime has made the exposure decision.
        let inventory = self.api_client.context_inventory();
        let model_candidates = self.model_candidates_for_turn(user_input);
        let collection_budget = model_candidates
            .iter()
            .map(|model| {
                let window = u64::from(self.context_window_for_model(model));
                let output = u64::from(bounded_provider_output_tokens(model, window as u32));
                let protocol = 128u64
                    .saturating_add(u64::from(inventory.tool_count as u32).saturating_mul(12));
                let safety = (window / 100).clamp(128, 2_048);
                window
                    .saturating_sub(output)
                    .saturating_sub(protocol)
                    .saturating_sub(safety)
            })
            .max()
            .unwrap_or_else(|| self.context_budget_tokens());
        // Collect memory/knowledge/fact/matrix data once against the largest
        // physically usable input window. The per-attempt packer below still
        // applies each model's hard cap, schema and history. If preflight
        // compacts the transcript, this snapshot is rebuilt before dispatch.
        let one_shot_context_items = self.take_next_model_context_items();
        let mut prompt = self
            .prepare_reality_context_with_budget_and_items(
                user_input,
                collection_budget,
                one_shot_context_items.clone(),
            )
            .await;
        let evidence = crate::evidence_planner::plan_evidence_with_understanding(
            user_input,
            &decision.strategy.understanding,
        );
        let apply_runtime_controls = |prompt: &mut PromptAssembly| {
            prompt.push_trusted_system(crate::evidence_planner::evidence_plan_prompt(&evidence));
            prompt.push_trusted_system(
                crate::execution_core::runtime_execution_guidance_prompt_with_tool_exposure(
                    &decision,
                    Some(&exposure.projection(0)),
                ),
            );
            if text_only_response {
                prompt.push_trusted_system(
                    "## Terminal response boundary\nThis request is a text-only terminal checkpoint. The executable tool set for this request is empty, regardless of any earlier capability inventory or historical tool receipts in the context. Do not emit native function calls, simulated tool markup, JSON commands, new plans, or more work. Use only retained evidence receipts to produce the best final answer now. State unresolved facts explicitly instead of performing another search.".to_string(),
                );
            }
            if explicitly_forbids_tool_use {
                prompt.push_trusted_system(
                    "## User-selected execution boundary\nThe user explicitly prohibited tool calls for this request. The executable tool set is empty. Answer from the supplied prompt and retained conversation evidence only; do not emit native function calls, simulated tool markup, or JSON commands.".to_string(),
                );
            }
        };
        apply_runtime_controls(&mut prompt);
        self.record_runtime_policy_decision(&decision, self.session().messages.len());
        self.record_context_event(
            "evidence_plan",
            "runtime",
            &format!("{:?}: {}", evidence.mode, evidence.reason),
            7,
        );
        self.record_context_event(
            "execution_decision",
            "runtime",
            &format!(
                "{}: {:?}",
                decision.pattern().as_str(),
                decision.recommended_actions
            ),
            8,
        );
        let mut request_messages = self.session.read().await.messages.clone();

        // Compression is a request-preflight recovery path, never a fixed
        // transcript-ratio timer. Optional packets have already been allowed
        // to compete for hard capacity; compact only when no configured
        // candidate can carry the fixed history plus required continuity.
        let no_candidate_can_fit = model_candidates.iter().all(|model| {
            self.pack_provider_attempt(&prompt, &request_messages, model, inventory)
                .is_err()
        });
        if no_candidate_can_fit {
            let compaction = self
                .compact_session_with_checkpoint(self.compaction_config_for_session(1))
                .await?;
            if compaction.is_none() {
                return Err(RuntimeError::new(
                    "all provider candidates reject the required request context and no semantic compaction boundary is available",
                ));
            }
            request_messages = self.session.read().await.messages.clone();
            prompt = self
                .prepare_reality_context_with_budget_and_items(
                    user_input,
                    collection_budget,
                    one_shot_context_items,
                )
                .await;
            apply_runtime_controls(&mut prompt);
            self.record_context_event(
                "context_preflight_compaction",
                "runtime",
                "all provider candidates required semantic compaction before request dispatch",
                9,
            );
            if let Ok(mut preflight_compaction) = self.turn_preflight_compaction.lock() {
                *preflight_compaction = compaction;
            }
        }
        if let Some(turn_id) = self.session_input_stream.active_turn_id() {
            self.consume_runtime_inputs_at_checkpoint(
                &turn_id,
                TurnInputCheckpoint::BeforeProviderRequest,
                &mut prompt,
            );
        }
        if knowledge_hard_gate_active(&prompt.trusted_system) {
            return Err(RuntimeError::new(
                "knowledge compliance hard gate blocked turn",
            ));
        }

        let mut last_error = None;
        let mut candidates = VecDeque::from(model_candidates);
        // One retry per model is sufficient: calibration only accepts a
        // smaller explicit provider limit, so the second request is strictly
        // smaller. Repeating beyond that would mask malformed provider errors.
        let mut calibration_retries = BTreeSet::new();
        while let Some(model) = candidates.pop_front() {
            let mut request =
                match self.pack_provider_attempt(&prompt, &request_messages, &model, inventory) {
                    Ok(request) => request,
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
            request.reasoning_effort_override = one_shot_reasoning_effort.clone();
            let mut evaluation_reservation =
                match EvaluationProviderTokenReservation::acquire(&mut request) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        last_error = Some(error);
                        continue;
                    }
                };
            self.record_provider_context_request(&request, self.session().messages.len());
            let attempt_budget = self.runtime_budget_plan_for_candidates(&[model.clone()]);
            let transport_policy = provider_transport_policy(
                attempt_budget.model_context_window.min(u64::from(u32::MAX)) as u32,
                &request,
            );
            let idle_timeout = transport_policy.idle_timeout;
            let heartbeat_grace = transport_policy.heartbeat_grace;
            let cancellation = self.cancellation_token.clone();
            let provider_started = Instant::now();
            let provider_lease = if let Some(manager) = &self.provider_admission {
                let acquire = manager.acquire(
                    crate::execution_core::graph::ExecutionResourceKind::Provider,
                    Some(Duration::from_secs(30)),
                );
                Some(tokio::select! {
                    () = cancellation.cancelled() => {
                        return Err(RuntimeError::new("turn cancelled while waiting for provider capacity"));
                    }
                    lease = acquire => lease.map_err(|error| RuntimeError::new(format!(
                        "provider capacity admission failed: {error}"
                    )))?,
                })
            } else {
                None
            };
            let mut stream = self.api_client.stream(request);
            let mut text = String::new();
            let mut effective_model = None;
            let mut thinking = String::new();
            let mut signature = None;
            let mut calls = Vec::new();
            let mut usage = TokenUsage::default();
            let mut cache_events = Vec::new();
            let mut failed = None;
            use futures::StreamExt;
            loop {
                let next = tokio::select! {
                    () = cancellation.cancelled() => {
                        failed = Some(RuntimeError::new(
                            "turn cancelled during provider stream",
                        ));
                        break;
                    }
                    next = tokio::time::timeout(idle_timeout, stream.next()) => next,
                };
                let event = match next {
                    Ok(Some(event)) => event,
                    Ok(None) => break,
                    Err(_) => {
                        // An upstream stream can be temporarily quiet while it
                        // flushes a heartbeat or a long reasoning segment.
                        // Give the provider a bounded, policy-derived grace
                        // period before declaring a real transport stall.
                        let heartbeat = tokio::select! {
                            () = cancellation.cancelled() => {
                                failed = Some(RuntimeError::new(
                                    "turn cancelled during provider heartbeat grace",
                                ));
                                break;
                            }
                            heartbeat = tokio::time::timeout(heartbeat_grace, stream.next()) => heartbeat,
                        };
                        match heartbeat {
                            Ok(Some(event)) => event,
                            Ok(None) => break,
                            Err(_) => {
                                failed = Some(RuntimeError::new(format!(
                                    "stream stalled after {}s idle plus {}s heartbeat grace",
                                    idle_timeout.as_secs(),
                                    heartbeat_grace.as_secs()
                                )));
                                break;
                            }
                        }
                    }
                };
                match event {
                    Ok(AssistantEvent::ProviderModel { model }) => {
                        effective_model = Some(model);
                    }
                    Ok(AssistantEvent::TextDelta(delta)) => {
                        text.push_str(&delta);
                        if let Some(ref cowd) = self.cowd_bus {
                            cowd.emit(crate::cowd_event::CowdEvent::TextDelta {
                                text: delta.clone(),
                            });
                        }
                        if let Some(ref callback) = self.sse_callback {
                            callback(
                                serde_json::json!({"type":"TextDelta","content":delta}).to_string(),
                            );
                        }
                    }
                    Ok(AssistantEvent::ThinkingDelta(delta)) => {
                        thinking.push_str(&delta);
                        if let Some(ref cowd) = self.cowd_bus {
                            cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                                status: harness_contract::projection::ExecutionLiveStatus::Thinking,
                                detail: Some("reasoning".to_string()),
                            });
                            cowd.emit(crate::cowd_event::CowdEvent::ThinkingDelta {
                                thinking: delta.clone(),
                            });
                        }
                        if let Some(ref callback) = self.sse_callback {
                            callback(
                                serde_json::json!({"type":"ThinkingDelta","content":delta})
                                    .to_string(),
                            );
                        }
                    }
                    Ok(AssistantEvent::SignatureDelta(value)) => signature = Some(value),
                    Ok(AssistantEvent::ToolUse { id, name, input }) => calls.push(ModelToolCall {
                        id,
                        name,
                        input,
                        depends_on: Vec::new(),
                    }),
                    Ok(AssistantEvent::Usage(value)) => {
                        usage = value;
                    }
                    Ok(AssistantEvent::PromptCache(value)) => cache_events.push(value),
                    Ok(AssistantEvent::MessageStop) => break,
                    Ok(
                        AssistantEvent::ToolStart { .. }
                        | AssistantEvent::ToolProgress { .. }
                        | AssistantEvent::ToolComplete { .. },
                    ) => {}
                    Err(error) => {
                        failed = Some(error);
                        break;
                    }
                }
            }
            drop(stream);
            if let Some(manager) = &self.provider_admission {
                let _ = manager.observe_runtime_pressure(
                    &crate::execution_core::graph::ExecutionResourceKind::Provider,
                    provider_started.elapsed(),
                    failed.is_some(),
                );
            }
            drop(provider_lease);
            if let Some(reservation) = evaluation_reservation.as_mut() {
                reservation.reconcile(usage);
            }
            if let Some(error) = failed {
                if let Some(observed_limit) = error.provider_context_window_limit() {
                    if calibration_retries.insert(model.clone())
                        && self.calibrate_model_context_window(&model, observed_limit)
                    {
                        tracing::info!(
                            model,
                            observed_limit,
                            "provider context window calibrated; retrying candidate once"
                        );
                        candidates.push_front(model);
                        continue;
                    }
                }
                last_error = Some(error);
                continue;
            }

            let mut blocks = Vec::new();
            if !thinking.is_empty() {
                blocks.push(ContentBlock::Thinking {
                    thinking,
                    signature,
                });
            }
            blocks.push(ContentBlock::Text { text: text.clone() });
            for call in &calls {
                blocks.push(ContentBlock::ToolUse {
                    id: call.id.clone(),
                    name: call.name.clone(),
                    input: call.input.clone(),
                });
            }
            let assistant_message = ConversationMessage {
                role: crate::session::MessageRole::Assistant,
                blocks,
                usage: Some(usage),
            };
            self.session
                .write()
                .await
                .push_message(assistant_message.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            self.dual_write_message(
                &assistant_message,
                self.session().messages.len().wrapping_sub(1),
            );
            self.reconcile_provider_context_usage(usage);
            self.usage_tracker.record(usage);
            if let Some(callback) = &self.tool_callback {
                callback.on_usage(&usage);
            }
            self.record_assistant_iteration(
                self.session().messages.len(),
                &assistant_message,
                calls.len(),
            );
            let classified = classify_model_step_intent(text, calls);
            let intent = apply_explicit_team_requirement(
                self.explicit_team_escalation,
                user_input,
                first_step,
                &decision,
                classified,
            );
            return Ok(ModelStepResult {
                intent,
                assistant_message,
                usage,
                prompt_cache_events: cache_events,
                // Preserve the model that actually produced the provider stream,
                // not merely Runtime's preferred candidate.
                model: effective_model.or(Some(model)),
                wall_duration_ms: millis_since(started_at).max(1),
                text_only_response,
            });
        }
        Err(last_error.unwrap_or_else(|| RuntimeError::new("all provider fallbacks exhausted")))
    }

    /// Execute one graph-owned tool wave. All tool side effects in a normal
    /// conversation turn enter through this method.
    pub(crate) async fn execute_tool_batch_step(
        &self,
        calls: &[ModelToolCall],
        prompter: &crate::permissions::SharedPrompter,
        iteration: usize,
    ) -> Result<ToolBatchStepResult, RuntimeError> {
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new("turn cancelled before tool execution"));
        }
        use crate::execution_scheduler::schedule_tool_execution_plan_for_decision;
        use crate::tool_dispatch::ToolRequest;

        let mut requests = calls
            .iter()
            .map(|call| ToolRequest {
                tool_use_id: call.id.clone(),
                tool_name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect::<Vec<_>>();
        let _ = crate::intent_planner::infer_tool_dependencies(&mut requests);
        let pending = calls
            .iter()
            .map(|call| (call.id.clone(), call.name.clone(), call.input.clone()))
            .collect::<Vec<_>>();
        let mut decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("tool batch has no admitted turn strategy"))?;
        let plan = ToolExecutionPlan::from_requests_with_classifier(&requests, |name, input| {
            self.tool_executor.classify_tool_safety(name, input)
        });
        self.record_tool_execution_plan(&plan, self.session().messages.len());
        let model_team_conflicts_with_admission = model_team_request_conflicts_with_admission(
            decision.strategy.selected_candidate,
            calls,
        );
        if plan.tasks.iter().any(|task| {
            task.safety_category != crate::tool_orchestrator::ToolSafetyCategory::ReadOnly
        }) && !model_team_conflicts_with_admission
        {
            let target_pattern = tool_batch_pattern(calls);
            decision
                .strategy
                .retarget(
                    target_pattern,
                    if target_pattern == harness_contract::core::ExecutionPattern::Collaborate {
                        "provider requested a governed team lifecycle through ToolBatch"
                    } else {
                        "provider emitted a governed tool intent; execute through ToolBatch"
                    },
                )
                .map_err(RuntimeError::new)?;
            if target_pattern == harness_contract::core::ExecutionPattern::Collaborate
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
            if !decision
                .strategy
                .modifiers
                .contains(&harness_contract::core::ExecutionModifier::WithGuardrails)
            {
                decision
                    .strategy
                    .modifiers
                    .push(harness_contract::core::ExecutionModifier::WithGuardrails);
            }
            if !decision
                .strategy
                .gates
                .contains(&harness_contract::core::ExecutionPolicyGate::Permission)
            {
                decision
                    .strategy
                    .gates
                    .push(harness_contract::core::ExecutionPolicyGate::Permission);
            }
            let selected_candidate =
                if target_pattern == harness_contract::core::ExecutionPattern::Collaborate {
                    harness_contract::strategy::ExecutionCandidateKind::Team
                } else if decision
                    .strategy
                    .modifiers
                    .contains(&harness_contract::core::ExecutionModifier::Parallel)
                {
                    harness_contract::strategy::ExecutionCandidateKind::ParallelTools
                } else {
                    harness_contract::strategy::ExecutionCandidateKind::Direct
                };
            decision = self.retarget_active_turn_strategy(
                selected_candidate,
                target_pattern,
                "provider tool batch retained the admitted decision lease",
            )?;
            if target_pattern == harness_contract::core::ExecutionPattern::Collaborate
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
            if !decision
                .strategy
                .modifiers
                .contains(&harness_contract::core::ExecutionModifier::WithGuardrails)
            {
                decision
                    .strategy
                    .modifiers
                    .push(harness_contract::core::ExecutionModifier::WithGuardrails);
            }
            if !decision
                .strategy
                .gates
                .contains(&harness_contract::core::ExecutionPolicyGate::Permission)
            {
                decision
                    .strategy
                    .gates
                    .push(harness_contract::core::ExecutionPolicyGate::Permission);
            }
            decision.compile_target = crate::execution_core::RuntimeCompileTarget::ExecutionGraph;
        }
        self.tool_executor.bind_execution_decision(decision.clone());
        let mut validation = plan.validate_against_execution_decision(&decision);
        if model_team_conflicts_with_admission {
            validation.allowed = false;
            validation.findings.push(
                "model_team_request_conflicts_with_admitted_strategy; Team must be selected by the sole admission owner"
                    .to_string(),
            );
        }
        if validation.allowed {
            self.satisfy_tool_strategy_gates(&plan, &decision, &mut validation)
                .await;
        }
        self.record_tool_strategy_validation(&validation, self.session().messages.len());
        let mut result_map = std::collections::HashMap::new();
        let mut max_concurrency_observed = 0;
        let mut parallel_batches = 0;
        if validation.allowed {
            let schedule = schedule_tool_execution_plan_for_decision(&requests, &plan, &decision);
            max_concurrency_observed = schedule
                .batches
                .iter()
                .map(|batch| batch.max_concurrency.min(batch.indices.len()))
                .max()
                .unwrap_or(0);
            parallel_batches = schedule
                .batches
                .iter()
                .filter(|batch| batch.max_concurrency > 1 && batch.indices.len() > 1)
                .count();
            self.record_tool_schedule(&schedule, &requests, self.session().messages.len());
            for batch in &schedule.batches {
                self.execute_tool_schedule_batch(
                    batch,
                    &requests,
                    &pending,
                    prompter,
                    iteration,
                    validation.approval_satisfied,
                    &mut result_map,
                )
                .await?;
            }
        } else {
            let reason = format!(
                "runtime strategy lease `{}` denied tool batch: {}",
                validation.lease_id,
                validation.findings.join(", ")
            );
            for call in calls {
                let message = ConversationMessage::tool_result(
                    call.id.clone(),
                    call.name.clone(),
                    reason.clone(),
                    true,
                );
                self.session
                    .write()
                    .await
                    .push_message(message.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                self.dual_write_message(&message, self.session().messages.len().wrapping_sub(1));
                result_map.insert(call.id.clone(), (message, None));
            }
        }
        let mut messages = Vec::with_capacity(calls.len());
        for call in calls {
            if let Some((message, _)) = result_map.remove(&call.id) {
                self.remember_tool_trace_from_message(&message);
                messages.push(message);
            }
        }
        let failed = count_failed_tool_results(&messages);
        Ok(ToolBatchStepResult {
            messages,
            failed,
            max_concurrency_observed,
            parallel_batches,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn finalize_graph_turn(
        &mut self,
        user_input: &str,
        final_answer: String,
        assistant_messages: Vec<ConversationMessage>,
        tool_results: Vec<ConversationMessage>,
        prompt_cache_events: Vec<PromptCacheEvent>,
        iterations: usize,
        model: Option<String>,
        input_tokens: u64,
        output_tokens: u64,
        wall_duration_ms: u64,
        duplicate_tool_calls: u64,
        write_attempt_paths: Vec<String>,
        max_tool_concurrency_observed: usize,
        parallel_tool_batches: usize,
        terminal_completion: harness_contract::goal::GoalCompletion,
        defer_post_turn_memory_maintenance: bool,
    ) -> Result<TurnSummary, RuntimeError> {
        let finalize_started = Instant::now();
        if final_answer.trim().is_empty() {
            return Err(RuntimeError::new("model produced an empty final answer"));
        }
        if self.active_turn_strategy().is_none() {
            return Err(RuntimeError::new(
                "turn finalization requires the Host-admitted turn strategy owner",
            ));
        }
        let decision = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("turn finalization has no strategy owner"))?;
        let mut kernel = RuntimeAiKernel::begin_turn_with_execution_decision(
            self.session().session_id.clone(),
            user_input.to_string(),
            self.context_profile(),
            &self.system_prompt,
            decision,
        );
        if !matches!(
            terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        ) {
            kernel.record_terminal_blocked(
                "the execution graph reached a non-satisfied terminal completion",
            );
        }
        let failed_tools = count_failed_tool_results(&tool_results);
        let ai_kernel_trace = kernel.finalize(
            &final_answer,
            tool_results.len().saturating_sub(failed_tools),
            failed_tools,
        );
        // Request-preflight owns compaction. Finalization never rewrites a
        // healthy transcript merely because an aggregate token estimate grew.
        let auto_compaction = self
            .turn_preflight_compaction
            .lock()
            .ok()
            .and_then(|mut receipt| receipt.take());
        let compaction_elapsed = Duration::ZERO;
        let memory_started = Instant::now();
        if defer_post_turn_memory_maintenance {
            self.schedule_memory_post_turn().await;
        } else {
            let _ = self.run_memory_post_turn().await;
        }
        let memory_elapsed = memory_started.elapsed();
        let usage = self.usage_tracker.cumulative_usage();
        let telemetry = crate::cowd_event::RunModelTelemetry {
            model: model.clone(),
            models_used: model.into_iter().collect(),
            first_token_latency_ms: None,
            active_stream_duration_ms: None,
            wall_duration_ms: wall_duration_ms.max(1),
            output_chars: final_answer.chars().count() as u64,
            output_chunks: iterations as u64,
            input_tokens,
            output_tokens,
            cache_create_tokens: u64::from(usage.cache_creation_input_tokens),
            cache_read_tokens: u64::from(usage.cache_read_input_tokens),
            total_tokens: input_tokens.saturating_add(output_tokens),
            usage_source: "provider".to_string(),
            wall_chars_per_second: rate_per_second(
                final_answer.chars().count() as u64,
                wall_duration_ms.max(1),
            ),
            wall_tokens_per_second: rate_per_second(output_tokens, wall_duration_ms.max(1)),
            active_chars_per_second: None,
            active_tokens_per_second: None,
            chars_per_second: rate_per_second(
                final_answer.chars().count() as u64,
                wall_duration_ms.max(1),
            ),
            tokens_per_second: rate_per_second(output_tokens, wall_duration_ms.max(1)),
        };
        let context_turn_report = self.build_context_turn_report(
            &ai_kernel_trace.harness_receipt.id,
            usage,
            auto_compaction.clone(),
        );
        self.remember_context_turn_report(context_turn_report.clone())
            .await?;
        let mut assistant_messages = assistant_messages;
        if !matches!(
            terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied
        ) {
            assistant_messages.push(ConversationMessage::assistant(vec![ContentBlock::Text {
                text: final_answer.clone(),
            }]));
        }
        let summary = TurnSummary {
            final_answer,
            terminal_completion,
            assistant_messages,
            tool_results,
            prompt_cache_events,
            iterations,
            usage,
            model_telemetry: telemetry,
            auto_compaction,
            ai_kernel_trace,
            context_turn_report,
            duplicate_tool_calls,
            write_attempt_paths,
            max_tool_concurrency_observed,
            parallel_tool_batches,
        };
        self.record_turn_completed(&summary);
        self.record_ai_kernel_trace_event(&summary.ai_kernel_trace, self.session().messages.len());
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::RunModelTelemetry {
                telemetry: summary.model_telemetry.clone(),
            });
        }
        tracing::debug!(
            total_ms = finalize_started.elapsed().as_millis(),
            compaction_ms = compaction_elapsed.as_millis(),
            memory_post_turn_ms = memory_elapsed.as_millis(),
            post_turn_memory_deferred = defer_post_turn_memory_maintenance,
            "graph turn finalization completed"
        );
        Ok(summary)
    }

    async fn satisfy_tool_strategy_gates(
        &self,
        execution_plan: &ToolExecutionPlan,
        execution_decision: &crate::execution_core::RuntimeExecutionDecision,
        validation: &mut ToolExecutionPolicyValidationReport,
    ) {
        if validation.requires_approval {
            let Some(gate) = &self.approval_gate else {
                validation.allowed = false;
                validation
                    .findings
                    .push("critical_mutation_missing_approval_runtime".to_string());
                return;
            };
            let operations = execution_plan
                .tasks
                .iter()
                .map(|task| {
                    serde_json::json!({
                        "tool_call_id": task.tool_call_id,
                        "tool_name": task.tool_name,
                        "safety_category": task.safety_category,
                        "resource_scope": task.resource_scope,
                    })
                })
                .collect::<Vec<_>>();
            let approval_input = serde_json::json!({
                "strategy_lease_id": execution_decision.lease.lease_id,
                "risk": execution_decision.risk(),
                "operations": operations,
            })
            .to_string();
            if let Some(cowd) = self.cowd_bus() {
                cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                    status: harness_contract::projection::ExecutionLiveStatus::WaitingApproval,
                    detail: Some("runtime_strategy_tool_batch".to_string()),
                });
                cowd.emit(crate::cowd_event::CowdEvent::ApprovalRequested {
                    tool: "runtime_strategy_tool_batch".to_string(),
                });
            }
            match gate
                .require_explicit_approval("runtime_strategy_tool_batch", &approval_input)
                .await
            {
                crate::approval_gate::ApprovalGateResult::Approved { .. }
                | crate::approval_gate::ApprovalGateResult::AutoPass { .. } => {
                    validation.approval_satisfied = true;
                }
                crate::approval_gate::ApprovalGateResult::Denied { reason } => {
                    validation.allowed = false;
                    validation
                        .findings
                        .push(format!("critical_mutation_approval_denied:{reason}"));
                    return;
                }
                crate::approval_gate::ApprovalGateResult::TimedOut => {
                    validation.allowed = false;
                    validation
                        .findings
                        .push("critical_mutation_approval_timed_out".to_string());
                    return;
                }
            }
        }

        if !validation.requires_checkpoint {
            return;
        }
        if !self.tool_executor.has_tool("checkpoint_create") {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_tool_unavailable".to_string());
            return;
        }

        let checkpoint_input = serde_json::json!({
            "label": format!(
                "runtime strategy lease {} before high-risk mutation",
                execution_decision.lease.lease_id
            )
        })
        .to_string();
        let executor = Arc::clone(&self.tool_executor);
        let timeout = self.tool_timeout.unwrap_or_else(|| {
            Duration::from_secs(crate::tool_execution_profile("checkpoint_create").timeout_secs)
        });
        let checkpoint_value = match serde_json::from_str::<serde_json::Value>(&checkpoint_input) {
            Ok(value) => value,
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_input_invalid:{error}"));
                return;
            }
        };
        let Some(descriptor) =
            executor.describe_tool_effect("checkpoint_create", &checkpoint_value)
        else {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_missing_effect_descriptor".to_string());
            return;
        };
        let authorization = match crate::ToolPolicy.authorize(
            &descriptor,
            format!(
                "{}:checkpoint:{}",
                self.session().session_id,
                execution_decision.lease.lease_id
            ),
            self.permission_policy.active_mode(),
            timeout.as_secs(),
        ) {
            Ok(decision) => decision,
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_authorization_denied:{error}"));
                return;
            }
        };
        let result = tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || {
                executor.execute_authorized(
                    &authorization.authorization,
                    "checkpoint_create",
                    &checkpoint_input,
                )
            }),
        )
        .await;
        match result {
            Ok(Ok(Ok(output))) => {
                validation.checkpoint_created = true;
                tracing::info!(
                    strategy_lease_id = %execution_decision.lease.lease_id,
                    checkpoint = %preview_chars(&output, 240),
                    "strategy checkpoint created before mutation"
                );
            }
            Ok(Ok(Err(error))) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_creation_failed:{error}"));
            }
            Ok(Err(error)) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_creation_panicked:{error}"));
            }
            Err(_) => {
                validation.allowed = false;
                validation.findings.push(format!(
                    "checkpoint_creation_timed_out:{}s",
                    timeout.as_secs()
                ));
            }
        }
    }

    /// Extract the per-tool execution logic from run_turn for reuse.
    async fn execute_single_tool(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        strategy_approval_satisfied: bool,
    ) -> Result<ConversationMessage, RuntimeError> {
        let pre_hook_result = self.run_pre_tool_use_hook(tool_name, input);
        let effective_input = pre_hook_result
            .updated_input()
            .map_or_else(|| input.to_string(), ToOwned::to_owned);
        let permission_context = PermissionContext::new(
            pre_hook_result.permission_override(),
            pre_hook_result.permission_reason().map(ToOwned::to_owned),
        );

        let permission_outcome = if pre_hook_result.is_cancelled() {
            PermissionOutcome::Deny {
                reason: format!("PreToolUse hook cancelled tool `{tool_name}`"),
            }
        } else if pre_hook_result.is_failed() {
            let hook_msgs = pre_hook_result.messages().join("; ");
            PermissionOutcome::Deny {
                reason: if hook_msgs.is_empty() {
                    format!("PreToolUse hook failed for tool `{tool_name}`")
                } else {
                    format!("PreToolUse hook failed for tool `{tool_name}`: {hook_msgs}")
                },
            }
        } else if pre_hook_result.is_denied() {
            PermissionOutcome::Deny {
                reason: format!("PreToolUse hook denied tool `{tool_name}`"),
            }
        } else if let Some(prompt) = prompter.lock().as_mut() {
            self.permission_policy.authorize_with_context(
                tool_name,
                &effective_input,
                &permission_context,
                Some(prompt.as_mut()),
            )
        } else {
            self.permission_policy.authorize_with_context(
                tool_name,
                &effective_input,
                &permission_context,
                None,
            )
        };

        match permission_outcome {
            PermissionOutcome::Allow => {
                // Smart approval gate check
                if !strategy_approval_satisfied {
                    if let Some(gate) = &self.approval_gate {
                        let gate_result = gate.evaluate(tool_name, &effective_input).await;
                        let denial_reason = match gate_result {
                            crate::approval_gate::ApprovalGateResult::Denied { reason } => {
                                Some(reason)
                            }
                            crate::approval_gate::ApprovalGateResult::TimedOut => {
                                Some(format!("approval timed out for tool `{tool_name}`"))
                            }
                            crate::approval_gate::ApprovalGateResult::AutoPass { .. }
                            | crate::approval_gate::ApprovalGateResult::Approved { .. } => None,
                        };
                        if let Some(reason) = denial_reason {
                            self.record_tool_invocation_denied(
                                tool_use_id,
                                tool_name,
                                &effective_input,
                                iterations,
                                ToolFailureKind::ApprovalDenied,
                                &reason,
                            );
                            self.emit_tool_completed(tool_use_id, tool_name, &reason, Some(1));
                            let denied = ConversationMessage::tool_result(
                                tool_use_id.to_string(),
                                tool_name.to_string(),
                                reason,
                                true,
                            );
                            self.session
                                .write()
                                .await
                                .push_message(denied.clone())
                                .map_err(|error| RuntimeError::new(error.to_string()))?;
                            self.dual_write_message(
                                &denied,
                                self.session().messages.len().wrapping_sub(1),
                            );
                            return Ok(denied);
                        }
                    }
                }

                // Gate evaluator check — runs commit quality gates (PreFlight,
                // Abort, Revision, Escalation) against the tool input before
                // allowing execution.
                if let Some(gate_evaluator) = &self.gate_evaluator {
                    let context = crate::gates::GateContext {
                        repo_path: std::env::current_dir()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string(),
                        branch: String::new(),
                        commit_message: tool_name.to_string(),
                        changed_files: Vec::new(),
                        diff: effective_input.clone(),
                        author: String::new(),
                        author_email: String::new(),
                        violations: Vec::new(),
                    };
                    let (all_passed, results) = gate_evaluator.evaluate_all(&context);
                    if !all_passed {
                        let reasons: Vec<String> = results
                            .iter()
                            .filter(|r| !r.passed)
                            .map(|r| {
                                let mut msg = format!("[{}] {}", r.gate_name, r.message);
                                if !r.suggestions.is_empty() {
                                    msg.push_str(&format!(
                                        " Suggestions: {}",
                                        r.suggestions.join(", ")
                                    ));
                                }
                                msg
                            })
                            .collect();
                        let reason = format!("Gate check failed: {}", reasons.join("; "));
                        self.record_tool_invocation_denied(
                            tool_use_id,
                            tool_name,
                            &effective_input,
                            iterations,
                            ToolFailureKind::GateDenied,
                            &reason,
                        );
                        self.emit_tool_completed(tool_use_id, tool_name, &reason, Some(1));
                        let denied = ConversationMessage::tool_result(
                            tool_use_id.to_string(),
                            tool_name.to_string(),
                            reason,
                            true,
                        );
                        self.session
                            .write()
                            .await
                            .push_message(denied.clone())
                            .map_err(|error| RuntimeError::new(error.to_string()))?;
                        self.dual_write_message(
                            &denied,
                            self.session().messages.len().wrapping_sub(1),
                        );
                        return Ok(denied);
                    }
                }

                let invocation_record = self.start_tool_invocation_record(
                    tool_use_id,
                    tool_name,
                    &effective_input,
                    iterations,
                );
                self.record_tool_invocation_event(
                    &invocation_record,
                    "tool.invocation.started",
                    self.session().messages.len(),
                );
                self.record_tool_started(iterations, tool_name);
                self.emit_tool_started(tool_use_id, tool_name, &effective_input);

                if let Some(callback) = &self.tool_callback {
                    let preview: String = effective_input.chars().take(200).collect();
                    callback.on_tool_start(tool_use_id, tool_name, &preview);
                }

                let start = Instant::now();
                let tname = tool_name.to_string();
                let tname_for_err = tname.clone();
                let tinput = effective_input.clone();
                let profile_timeout =
                    Duration::from_secs(crate::tool_execution_profile(tool_name).timeout_secs);
                let tool_timeout = self
                    .tool_timeout
                    .map_or(profile_timeout, |t| t.min(profile_timeout));
                let (output, mut is_error, mut failure_kind) = if tool_name == "evidence_retrieve" {
                    match self.retrieve_tool_evidence(&effective_input) {
                        Ok(output) => (output, false, None),
                        Err(error) => (error, true, Some(ToolFailureKind::ExecutionError)),
                    }
                } else {
                    let tool_exec = Arc::clone(&self.tool_executor);
                    let parsed_input = serde_json::from_str::<serde_json::Value>(&tinput)
                        .unwrap_or(serde_json::Value::Null);
                    let authorization = tool_exec
                        .describe_tool_effect(&tname, &parsed_input)
                        .map(|descriptor| {
                            crate::ToolPolicy.authorize(
                                &descriptor,
                                format!("{}:{tool_use_id}:{iterations}", self.session().session_id),
                                self.permission_policy.active_mode(),
                                tool_timeout.as_secs(),
                            )
                        })
                        .transpose()
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    match tokio::time::timeout(
                        tool_timeout,
                        tokio::task::spawn_blocking(move || match authorization.as_ref() {
                            Some(decision) => tool_exec.execute_authorized(
                                &decision.authorization,
                                &tname,
                                &tinput,
                            ),
                            None if matches!(
                                tname.as_str(),
                                "ToolSearch" | "runtime_capabilities"
                            ) =>
                            {
                                tool_exec.execute(&tname, &tinput)
                            }
                            None => Err(ToolError::new(format!(
                                "tool `{tname}` is missing a Runtime authorization descriptor"
                            ))),
                        }),
                    )
                    .await
                    {
                        Ok(Ok(Ok(output))) => (output, false, None),
                        Ok(Ok(Err(error))) => (
                            error.to_string(),
                            true,
                            Some(ToolFailureKind::ExecutionError),
                        ),
                        Ok(Err(join_error)) => (
                            format!("tool execution panicked: {join_error}"),
                            true,
                            Some(ToolFailureKind::Panic),
                        ),
                        Err(_elapsed) => {
                            tracing::warn!(tool = %tname_for_err, timeout_secs = tool_timeout.as_secs(), "tool execution timed out, returning partial result");
                            (
                                format!("tool `{tname_for_err}` timed out after {tool_timeout:?}"),
                                true,
                                Some(ToolFailureKind::Timeout),
                            )
                        }
                    }
                };
                let elapsed_ms = start.elapsed().as_millis() as u64;
                self.hook_runner
                    .fire_post_tool(tool_name, &output, is_error, elapsed_ms);

                if let Some(callback) = &self.tool_callback {
                    let summary: String = output.chars().take(500).collect();
                    let exit_code = if is_error { Some(1) } else { Some(0) };
                    callback.on_tool_complete(tool_use_id, tool_name, &summary, exit_code);
                }

                let post_hook_result = if is_error {
                    self.run_post_tool_use_failure_hook(tool_name, &effective_input, &output)
                } else {
                    self.run_post_tool_use_hook(tool_name, &effective_input, &output, false)
                };
                if post_hook_result.is_denied()
                    || post_hook_result.is_failed()
                    || post_hook_result.is_cancelled()
                {
                    is_error = true;
                    if failure_kind.is_none() {
                        failure_kind = Some(ToolFailureKind::HookDenied);
                    }
                }

                let elapsed_ms = start.elapsed().as_millis() as u64;
                if let Some(ref cowd) = self.cowd_bus {
                    cowd.emit(crate::cowd_event::CowdEvent::ToolExecuted {
                        name: tool_name.to_string(),
                        duration_ms: elapsed_ms,
                    });
                }

                // T36: Truncate oversized tool results before storing.
                // Append hook feedback messages to the tool output.
                if tool_name == "ToolSearch" && !is_error {
                    self.activate_tool_discovery(&output);
                }
                let mut combined = if tool_name == "runtime_capabilities" && !is_error {
                    self.project_runtime_capabilities_for_model(&output)
                } else {
                    output
                };
                for msg in pre_hook_result.messages() {
                    combined.push('\n');
                    combined.push_str(msg);
                }
                for msg in post_hook_result.messages() {
                    combined.push('\n');
                    combined.push_str(msg);
                }
                let completed_record = if is_error {
                    invocation_record.failed_with_output_policy(
                        failure_kind.unwrap_or(ToolFailureKind::Unknown),
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                } else {
                    invocation_record.completed_with_output_policy(
                        &combined,
                        now_ms(),
                        DEFAULT_OUTPUT_REF_MIN_LINES,
                    )
                };
                let prepared_vision = prepared_vision_payload(tool_name, &combined, is_error);
                let indexable_output = prepared_vision
                    .as_ref()
                    .map(vision_index_summary)
                    .unwrap_or_else(|| combined.clone());
                let (raw_ref, raw_access) = self
                    .record_tool_raw_evidence(
                        tool_use_id,
                        tool_name,
                        &completed_record.input_hash,
                        &combined,
                        is_error,
                        elapsed_ms,
                        None,
                    )
                    .await;
                self.maybe_index_tool_output(
                    raw_ref.id(),
                    tool_name,
                    &indexable_output,
                    raw_access.as_ref(),
                );
                let completed_record = if raw_access.is_some() {
                    completed_record.with_full_output_ref(format!("tool://{}", raw_ref.id()))
                } else {
                    completed_record
                };
                let mut model_receipt = self.tool_model_receipt(
                    tool_name,
                    &combined,
                    is_error,
                    &raw_ref,
                    raw_access.as_ref(),
                );
                if let Some(payload) = prepared_vision.as_ref() {
                    model_receipt.summary = if raw_access.is_some() {
                        vision_tool_model_receipt(payload, &raw_ref)
                    } else {
                        format!(
                            "Tool `vision_analyze` completed, but raw evidence persistence is unavailable. Image input is attached as a structured vision block for the next model call. path={}, media_type={}, size_bytes={}, prompt={}",
                            payload.image_path,
                            payload.media_type,
                            payload.size_bytes.unwrap_or_default(),
                            payload.prompt
                        )
                    };
                    model_receipt.receipt_tokens =
                        crate::context_ledger::estimate_text_tokens(&model_receipt.summary);
                    model_receipt.omitted_tokens = model_receipt
                        .raw_tokens
                        .saturating_sub(model_receipt.receipt_tokens);
                    model_receipt.truncated =
                        model_receipt.receipt_tokens < model_receipt.raw_tokens;
                }
                let audit_projection =
                    crate::context_evidence::audit_projection(&model_receipt, raw_access.as_ref());
                self.push_turn_evidence_audit(audit_projection);
                let model_summary = model_receipt.summary;
                self.emit_tool_completed(
                    tool_use_id,
                    tool_name,
                    &indexable_output,
                    if is_error { Some(1) } else { Some(0) },
                );
                self.push_turn_tool_observation(ToolObservation::new(
                    tool_name.to_string(),
                    completed_record.invocation_id.clone(),
                    raw_ref,
                    model_summary.clone(),
                ));
                let result = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    model_summary,
                    is_error,
                );
                self.session
                    .write()
                    .await
                    .push_message(result.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                self.dual_write_message(&result, self.session().messages.len().wrapping_sub(1));
                if let Some(payload) = prepared_vision {
                    let image_message = vision_user_message(&payload);
                    self.session
                        .write()
                        .await
                        .push_message(image_message.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    self.dual_write_message(
                        &image_message,
                        self.session().messages.len().wrapping_sub(1),
                    );
                }
                self.record_tool_invocation_event(
                    &completed_record,
                    if is_error {
                        "tool.invocation.failed"
                    } else {
                        "tool.invocation.completed"
                    },
                    self.session().messages.len().wrapping_sub(1),
                );
                self.record_tool_finished(iterations, &result);
                Ok(result)
            }
            PermissionOutcome::Deny { reason } => {
                let failure_kind = if reason.starts_with("PreToolUse hook") {
                    ToolFailureKind::HookDenied
                } else {
                    ToolFailureKind::PermissionDenied
                };
                self.record_tool_invocation_denied(
                    tool_use_id,
                    tool_name,
                    &effective_input,
                    iterations,
                    failure_kind,
                    &reason,
                );
                self.emit_tool_completed(tool_use_id, tool_name, &reason, Some(1));
                let denied = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    reason,
                    true,
                );
                self.session
                    .write()
                    .await
                    .push_message(denied.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                self.dual_write_message(&denied, self.session().messages.len().wrapping_sub(1));
                Ok(denied)
            }
        }
    }

    async fn execute_tool_schedule_batch(
        &self,
        batch: &crate::execution_scheduler::ExecutionBatch,
        _requests: &[crate::tool_dispatch::ToolRequest],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        strategy_approval_satisfied: bool,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        match batch.mode {
            crate::execution_scheduler::ExecutionBatchMode::Wave
            | crate::execution_scheduler::ExecutionBatchMode::SerialStrategy => {
                self.execute_tool_indices_serial_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    true,
                    strategy_approval_satisfied,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::ParallelRead => {
                self.execute_tool_indices_concurrently_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    batch.max_concurrency,
                    true,
                    strategy_approval_satisfied,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::LimitedNetwork => {
                self.execute_tool_indices_concurrently_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    batch.max_concurrency,
                    true,
                    strategy_approval_satisfied,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::LimitedWrite => {
                self.execute_write_scope_groups_into_map(
                    batch,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    strategy_approval_satisfied,
                    result_map,
                )
                .await
            }
            crate::execution_scheduler::ExecutionBatchMode::SerialDestructive => {
                self.execute_tool_indices_serial_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    true,
                    strategy_approval_satisfied,
                    result_map,
                )
                .await
            }
        }
    }

    async fn execute_tool_indices_concurrently_into_map(
        &self,
        indices: &[usize],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        max_concurrency: usize,
        acquire_category_permit: bool,
        strategy_approval_satisfied: bool,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        use futures::stream::{FuturesUnordered, StreamExt};

        let limit = bounded_tool_concurrency(max_concurrency, indices.len());
        for chunk in indices.chunks(limit) {
            let mut futures = FuturesUnordered::new();
            for &idx in chunk {
                futures.push(self.execute_tool_index_collect(
                    idx,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    acquire_category_permit,
                    strategy_approval_satisfied,
                ));
            }
            while let Some(result) = futures.next().await {
                let (id, message) = result?;
                result_map.insert(id, message);
            }
        }
        Ok(())
    }

    async fn execute_write_scope_groups_into_map(
        &self,
        batch: &crate::execution_scheduler::ExecutionBatch,
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        strategy_approval_satisfied: bool,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        use futures::stream::{FuturesUnordered, StreamExt};

        if batch.scope_groups.is_empty() {
            return self
                .execute_tool_indices_concurrently_into_map(
                    &batch.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    batch.max_concurrency,
                    true,
                    strategy_approval_satisfied,
                    result_map,
                )
                .await;
        }

        let limit = bounded_tool_concurrency(batch.max_concurrency, batch.scope_groups.len());
        for chunk in batch.scope_groups.chunks(limit) {
            let mut futures = FuturesUnordered::new();
            for group in chunk {
                futures.push(self.execute_tool_indices_serial_collect(
                    &group.indices,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    true,
                    strategy_approval_satisfied,
                ));
            }
            while let Some(result) = futures.next().await {
                for (id, message) in result? {
                    result_map.insert(id, message);
                }
            }
        }
        Ok(())
    }

    async fn execute_tool_indices_serial_into_map(
        &self,
        indices: &[usize],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        acquire_category_permit: bool,
        strategy_approval_satisfied: bool,
        result_map: &mut std::collections::HashMap<String, (ConversationMessage, Option<String>)>,
    ) -> Result<(), RuntimeError> {
        for (id, message) in self
            .execute_tool_indices_serial_collect(
                indices,
                pending_tool_uses,
                prompter,
                iterations,
                acquire_category_permit,
                strategy_approval_satisfied,
            )
            .await?
        {
            result_map.insert(id, message);
        }
        Ok(())
    }

    async fn execute_tool_indices_serial_collect(
        &self,
        indices: &[usize],
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        acquire_category_permit: bool,
        strategy_approval_satisfied: bool,
    ) -> Result<Vec<(String, (ConversationMessage, Option<String>))>, RuntimeError> {
        let mut results = Vec::with_capacity(indices.len());
        for &idx in indices {
            results.push(
                self.execute_tool_index_collect(
                    idx,
                    pending_tool_uses,
                    prompter,
                    iterations,
                    acquire_category_permit,
                    strategy_approval_satisfied,
                )
                .await?,
            );
        }
        Ok(results)
    }

    async fn execute_tool_index_collect(
        &self,
        idx: usize,
        pending_tool_uses: &[(String, String, String)],
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        acquire_category_permit: bool,
        strategy_approval_satisfied: bool,
    ) -> Result<(String, (ConversationMessage, Option<String>)), RuntimeError> {
        let Some((tool_use_id, tool_name, input)) = pending_tool_uses.get(idx) else {
            return Err(RuntimeError::new(format!(
                "tool schedule referenced missing tool index {idx}"
            )));
        };

        let _process_permit = crate::execution_scheduler::acquire_process_tool_permit()
            .await
            .map_err(RuntimeError::new)?;
        let result_msg = if acquire_category_permit {
            let sem = self.tool_category_semaphore(tool_name, input);
            let _permit = sem.acquire().await.map_err(|error| {
                RuntimeError::new(format!("tool category semaphore closed: {error}"))
            })?;
            self.execute_single_tool(
                tool_use_id,
                tool_name,
                input,
                prompter,
                iterations,
                strategy_approval_satisfied,
            )
            .await?
        } else {
            self.execute_single_tool(
                tool_use_id,
                tool_name,
                input,
                prompter,
                iterations,
                strategy_approval_satisfied,
            )
            .await?
        };
        Ok(self.collect_tool_result_message(result_msg))
    }

    fn collect_tool_result_message(
        &self,
        result_msg: ConversationMessage,
    ) -> (String, (ConversationMessage, Option<String>)) {
        let (msg_id, tool_name) = extract_tool_info(&result_msg);
        let inject = self.turn_callback.as_ref().and_then(|callback| {
            let output = result_msg
                .blocks
                .first()
                .and_then(|block| match block {
                    ContentBlock::ToolResult { output, .. } => Some(output.as_str()),
                    _ => None,
                })
                .unwrap_or("");
            (callback.on_tool_result)(&tool_name, output)
        });
        (msg_id, (result_msg, inject))
    }

    fn tool_category_semaphore(&self, tool_name: &str, input: &str) -> &Semaphore {
        let category = self
            .tool_executor
            .classify_tool_safety(tool_name, input)
            .unwrap_or_else(|| crate::classify_tool_request(tool_name, input));
        match category {
            crate::tool_orchestrator::ToolSafetyCategory::WriteLocal => &self.write_semaphore,
            crate::tool_orchestrator::ToolSafetyCategory::Network => &self.network_semaphore,
            crate::tool_orchestrator::ToolSafetyCategory::Destructive => {
                &self.destructive_semaphore
            }
            crate::tool_orchestrator::ToolSafetyCategory::ReadOnly => &self.default_semaphore,
        }
    }

    /// Compact the active transcript through the sole semantic checkpoint
    /// pipeline. Both automatic preflight compaction and operator-triggered
    /// compaction use this path so a session never receives a second,
    /// timeline-only summary representation.
    pub async fn compact_active_session(
        &mut self,
    ) -> Result<Option<AutoCompactionEvent>, RuntimeError> {
        // Operator-triggered compaction shares the configured preservation and
        // checkpoint limits with request-preflight compaction. `1` makes the
        // operation explicit without introducing a second threshold policy.
        self.compact_session_with_checkpoint(self.compaction_config_for_session(1))
            .await
    }

    #[must_use]
    pub fn estimated_tokens(&self) -> usize {
        estimate_session_tokens(&self.session.blocking_read())
    }

    fn model_candidates_for_turn(&self, user_input: &str) -> Vec<String> {
        let primary = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(ToString::to_string);
        let mut fallback_models: Vec<String> = self
            .fallbacks
            .iter()
            .map(|model| model.trim())
            .filter(|model| !model.is_empty())
            .map(ToString::to_string)
            .collect();
        fallback_models.dedup();
        if let Some(primary) = primary.as_ref() {
            fallback_models.retain(|model| model != primary);
        }

        let Ok(registry) = self.model_performance_registry.lock() else {
            return primary
                .into_iter()
                .chain(fallback_models)
                .collect::<Vec<_>>();
        };
        let routable = fallback_models.clone();
        let decision = registry.route(ModelRouteIntent::from_task(user_input), &routable);
        let mut routed = Vec::with_capacity(fallback_models.len() + usize::from(primary.is_some()));
        if let Some(primary) = primary {
            routed.push(primary);
        }
        if routable
            .iter()
            .any(|model| model == &decision.selected_model)
            && !routed.iter().any(|known| known == &decision.selected_model)
        {
            routed.push(decision.selected_model);
        }
        for candidate in decision.candidates {
            if routable.iter().any(|model| model == &candidate.model)
                && !routed.iter().any(|model| model == &candidate.model)
            {
                routed.push(candidate.model);
            }
        }
        for model in fallback_models {
            if !routed.iter().any(|known| known == &model) {
                routed.push(model);
            }
        }
        if routed.is_empty() {
            // An empty model delegates selection to the configured provider. This keeps
            // embedded runtimes valid when they intentionally rely on a provider default.
            routed.push(String::new());
        }
        routed
    }

    #[must_use]
    pub fn usage(&self) -> &UsageTracker {
        &self.usage_tracker
    }

    #[must_use]
    pub fn tool_executor(&self) -> &Arc<T> {
        &self.tool_executor
    }

    #[must_use]
    pub fn permission_policy(&self) -> &PermissionPolicy {
        &self.permission_policy
    }

    #[must_use]
    pub fn tool_timeout(&self) -> Option<std::time::Duration> {
        self.tool_timeout
    }

    #[must_use]
    #[allow(
        clippy::panic,
        reason = "the synchronous compatibility API cannot return an error; an OS worker that cannot join would violate the session read contract"
    )]
    pub fn session(&self) -> Session {
        if let Ok(session) = self.session.try_read() {
            return session.clone();
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                return tokio::task::block_in_place(|| self.session.blocking_read().clone());
            }

            // `block_in_place` is unsupported by a current-thread Tokio runtime.
            // The async API remains preferred; this compatibility accessor moves the
            // blocking read to a short-lived OS thread instead of panicking inside
            // Tokio when legacy synchronous callers use it from that runtime.
            let session = Arc::clone(&self.session);
            return std::thread::scope(|scope| {
                scope
                    .spawn(move || session.blocking_read().clone())
                    .join()
                    .unwrap_or_else(|_| {
                        panic!("session read worker terminated before returning a session")
                    })
            });
        }
        self.session.blocking_read().clone()
    }

    pub async fn session_async(&self) -> Session {
        self.session.read().await.clone()
    }

    pub fn api_client_mut(&mut self) -> &mut C {
        &mut self.api_client
    }

    pub fn session_mut(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
        self.session.blocking_write()
    }

    pub async fn session_mut_async(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
        self.session.write().await
    }

    pub async fn append_external_message(
        &self,
        message: ConversationMessage,
    ) -> Result<(), RuntimeError> {
        let mut session = self.session.write().await;
        session
            .push_message(message.clone())
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let sequence = session.messages.len().wrapping_sub(1);
        drop(session);
        self.dual_write_message(&message, sequence);
        Ok(())
    }

    #[must_use]
    pub fn fork_session(&self, branch_name: Option<String>) -> Session {
        self.session.blocking_read().fork(branch_name)
    }

    #[must_use]
    pub fn into_session(self) -> Session {
        Arc::try_unwrap(self.session)
            .map(|lock| lock.into_inner())
            .unwrap_or_else(|arc| arc.blocking_read().clone())
    }

    async fn compact_session_with_checkpoint(
        &mut self,
        config: CompactionConfig,
    ) -> Result<Option<AutoCompactionEvent>, RuntimeError> {
        if self.session_store.is_none() {
            return Err(RuntimeError::new(
                "semantic compaction requires a durable UnifiedSessionStore; transcript was retained",
            ));
        }
        let original_session = self.session.read().await.clone();
        let Some(plan) = plan_session_compaction(&original_session, config) else {
            return Ok(None);
        };

        let source_messages = compacted_source_messages(
            &original_session.messages,
            plan.source_message_start,
            plan.source_message_end,
        );
        let raw_refs = source_message_evidence_refs(
            &original_session.session_id,
            &original_session.messages,
            plan.source_message_start,
            plan.source_message_end,
        );
        let checkpoint = if self.semantic_checkpoint_enabled && !source_messages.is_empty() {
            let mem_messages = conversation_messages_to_mem_messages(source_messages);
            let source_range = CompactionSourceRange {
                session_id: original_session.session_id.clone(),
                message_start: plan.source_message_start,
                message_end_exclusive: plan.source_message_end,
                event_start: Some(plan.source_message_start),
                event_end_exclusive: Some(plan.source_message_end),
                raw_refs: raw_refs.clone(),
            };
            let ctx = self.memory_turn_context();
            let checkpoint_id = deterministic_checkpoint_id(
                &original_session.session_id,
                plan.source_message_start,
                plan.source_message_end,
                plan.existing_summary.as_deref(),
            );
            let build_context = SessionCheckpointBuildContext::new(
                original_session.session_id.clone(),
                ctx.agent_id.clone(),
                source_range,
            )
            .with_checkpoint_id(checkpoint_id)
            .with_project_id(ctx.project_id.clone())
            .with_task_id(ctx.task_id.clone())
            .with_team_id(ctx.team_id.clone());
            match SessionCompactor::new()
                .with_max_summary_tokens(self.session_compaction_config.summary_max_tokens)
                .build_checkpoint(
                    &mem_messages,
                    plan.existing_summary.as_deref(),
                    build_context,
                )
                .await
            {
                Ok(checkpoint) => checkpoint,
                Err(error) => {
                    return Err(RuntimeError::new(format!(
                        "semantic compaction checkpoint build failed; transcript was retained: {error}"
                    )));
                }
            }
        } else {
            return Err(RuntimeError::new(
                "semantic compaction requires an enabled memory checkpoint; transcript was retained",
            ));
        };

        // Runtime never synthesizes a second lossy summary. The Memory
        // checkpoint is the sole continuation artifact and the source of all
        // durable fact extraction below.
        let result = apply_compaction_summary(&original_session, plan, checkpoint.summary.clone());

        let fact_extraction_decision = RuntimeFactExtractionScheduler::default()
            .decide(RuntimeFactExtractionTrigger::SessionCompaction);

        let mut receipt = Some({
            let fact_extraction_event = FactExtractionRuntimeEvent::from_decision(
                &fact_extraction_decision,
                "memory-session-checkpoint:v1",
                checkpoint.facts.len(),
                checkpoint.source_range.raw_refs.len(),
                FactExtractionTokenUsage {
                    input_tokens: checkpoint.token_stats.before,
                    output_tokens: checkpoint.token_stats.after,
                    total_tokens: checkpoint
                        .token_stats
                        .before
                        .saturating_add(checkpoint.token_stats.after),
                },
            );
            let mut receipt = CompactionReceipt::new(
                "runtime_auto_compaction",
                checkpoint.token_stats.before,
                checkpoint.token_stats.after,
            )
            .with_evidence_ref(EvidenceRef(
                KernelRef::new("checkpoint", checkpoint.checkpoint_id.clone())
                    .with_label("semantic_compaction_checkpoint"),
            ))
            .with_evidence_ref(EvidenceRef(
                KernelRef::new(
                    "fact-extraction",
                    fact_extraction_decision.mode.as_str().to_string(),
                )
                .with_label(fact_extraction_event.evidence_label()),
            ));
            receipt
                .retained_artifact_ids
                .push(format!("checkpoint:{}", checkpoint.checkpoint_id));
            receipt.retained_artifact_ids.push(format!(
                "fact-extraction:{}",
                fact_extraction_decision.mode.as_str()
            ));
            for evidence in &checkpoint.source_range.raw_refs {
                receipt.evidence_refs.push(evidence.clone());
                receipt
                    .dropped_artifact_ids
                    .push(format!("{}:{}", evidence.0.ref_type, evidence.0.id));
            }
            receipt
        });

        tracing::info!(removed = result.removed_message_count, "compaction");
        let compacted_len = result.compacted_session.messages.len();
        let compaction = result.compacted_session.compaction.clone().ok_or_else(|| {
            RuntimeError::new("semantic compaction did not produce a session compaction record")
        })?;
        let newly_committed = self
            .record_session_compacted(
                compaction,
                compacted_len,
                receipt.clone(),
                checkpoint.clone(),
            )
            .await?;
        // The checkpoint boundary is now durable. Fact projection is
        // intentionally replayable: if a process stopped after the event
        // transaction but before Memory writes, the next attempt recreates
        // exactly the same deterministic memory IDs instead of losing facts
        // or emitting duplicates.
        if !newly_committed {
            tracing::info!(checkpoint_id = %checkpoint.checkpoint_id, "replaying semantic checkpoint fact projection");
        }
        if let (Some(mgr), Some(receipt_mut)) = (&self.memory_manager, receipt.as_mut()) {
            let ctx = self.memory_turn_context();
            let kernel = MemoryKernel::new(Arc::clone(mgr));
            match kernel.checkpoint_compaction(&ctx, checkpoint).await {
                Ok(memory_receipt) => {
                    receipt_mut.retained_artifact_ids.extend(
                        memory_receipt
                            .memory_ids
                            .iter()
                            .map(|id| format!("memory:{id}")),
                    );
                    receipt_mut.retained_artifact_ids.push(format!(
                        "fact-review:{}",
                        memory_receipt.fact_review.batch_id.as_str()
                    ));
                    receipt_mut.evidence_refs.push(EvidenceRef(
                        KernelRef::new(
                            "fact-review",
                            memory_receipt.fact_review.batch_id.as_str().to_string(),
                        )
                        .with_label(format!(
                            "promoted={} held={} rejected={} conflicts={}",
                            memory_receipt.fact_review.promoted.len(),
                            memory_receipt.fact_review.held.len(),
                            memory_receipt.fact_review.rejected.len(),
                            memory_receipt.fact_review.conflicts.len()
                        )),
                    ));
                }
                Err(error) => {
                    tracing::warn!(%error, "semantic compaction fact projection deferred");
                    receipt_mut.evidence_refs.push(EvidenceRef(
                        KernelRef::new("memory", "semantic_checkpoint_fact_projection_deferred")
                            .with_label(error.to_string()),
                    ));
                }
            }
        }
        *self.session.write().await = result.compacted_session;
        Ok(Some(AutoCompactionEvent {
            removed_message_count: result.removed_message_count,
            compaction_receipt: receipt,
        }))
    }

    fn compaction_config_for_session(&self, max_estimated_tokens: usize) -> CompactionConfig {
        CompactionConfig {
            preserve_recent_messages: self.session_compaction_config.preserve_recent as usize,
            max_estimated_tokens,
            priority_threshold: 3,
            keep_high_priority: true,
        }
    }

    async fn record_session_compacted(
        &self,
        compaction: crate::session::SessionCompaction,
        sequence: usize,
        receipt: Option<CompactionReceipt>,
        semantic_checkpoint: SessionSemanticCheckpoint,
    ) -> Result<bool, RuntimeError> {
        let store = self.session_store.as_ref().ok_or_else(|| {
            RuntimeError::new(
                "semantic compaction requires a durable UnifiedSessionStore; transcript was retained",
            )
        })?;
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "type": "SessionCompacted",
            "sequence": sequence,
            "compaction": {
                "count": compaction.count,
                "removed_message_count": compaction.removed_message_count,
                "summary": compaction.summary,
            },
            "receipt": receipt,
        });
        let created_at_ms = now_ms();
        let context_event = memory::SessionDomainEvent::new(
            session_id.clone(),
            0,
            memory::SessionDomainScope::Context,
            "context.session_compacted",
            payload,
            created_at_ms,
        );
        let checkpoint_id = semantic_checkpoint.checkpoint_id.clone();
        let compaction_event_id = context_event.event_id.clone();
        let events = vec![
            context_event,
            memory::SessionDomainEvent::new(
                session_id.clone(),
                0,
                memory::SessionDomainScope::Memory,
                "memory.semantic_checkpoint.created",
                serde_json::json!({
                    "source": "conversation_runtime.compaction",
                    "compaction_event_id": compaction_event_id,
                    "checkpoint": semantic_checkpoint,
                    "receipt": receipt,
                }),
                created_at_ms,
            ),
        ];
        let committed = store
            .append_session_domain_events_if_checkpoint_absent(&events, &checkpoint_id)
            .await;
        let committed = match committed {
            Ok(true) => true,
            Ok(false) => {
                tracing::info!(session_id, checkpoint_id = %checkpoint_id, "reusing committed semantic compaction bundle");
                false
            }
            Err(error) => {
                return Err(RuntimeError::new(format!(
                    "atomic compaction persistence failed for session `{session_id}`; transcript was retained: {error}"
                )));
            }
        };
        Ok(committed)
    }

    fn record_turn_started(&self, user_input: &str) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert(
            "user_input".to_string(),
            Value::String(user_input.to_string()),
        );
        session_tracer.record("turn_started", attributes);
    }

    #[allow(dead_code)]
    fn record_assistant_iteration(
        &self,
        iteration: usize,
        assistant_message: &ConversationMessage,
        pending_tool_use_count: usize,
    ) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert(
            "assistant_blocks".to_string(),
            Value::from(assistant_message.blocks.len() as u64),
        );
        attributes.insert(
            "pending_tool_use_count".to_string(),
            Value::from(pending_tool_use_count as u64),
        );
        session_tracer.record("assistant_iteration_completed", attributes);
    }

    fn record_tool_started(&self, iteration: usize, tool_name: &str) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert(
            "tool_name".to_string(),
            Value::String(tool_name.to_string()),
        );
        session_tracer.record("tool_execution_started", attributes);
    }

    #[allow(dead_code)]
    fn record_tool_finished(&self, iteration: usize, result_message: &ConversationMessage) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let Some(ContentBlock::ToolResult {
            tool_name,
            is_error,
            ..
        }) = result_message.blocks.first()
        else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert("iteration".to_string(), Value::from(iteration as u64));
        attributes.insert("tool_name".to_string(), Value::String(tool_name.clone()));
        attributes.insert("is_error".to_string(), Value::Bool(*is_error));
        session_tracer.record("tool_execution_finished", attributes);
    }

    fn emit_tool_started(&self, tool_use_id: &str, tool_name: &str, input: &str) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
            status: harness_contract::projection::ExecutionLiveStatus::CallingTool,
            detail: Some(tool_name.to_string()),
        });
        cowd.emit(crate::cowd_event::CowdEvent::ToolStart {
            id: tool_use_id.to_string(),
            name: tool_name.to_string(),
            preview: preview_chars(input, 200),
        });
    }

    fn emit_tool_completed(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        output: &str,
        exit_code: Option<i32>,
    ) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit(crate::cowd_event::CowdEvent::ToolComplete {
            id: tool_use_id.to_string(),
            name: tool_name.to_string(),
            summary: preview_chars(output, 500),
            exit_code,
        });
    }

    fn record_turn_completed(&self, summary: &TurnSummary) {
        let Some(session_tracer) = &self.session_tracer else {
            return;
        };

        let mut attributes = Map::new();
        attributes.insert(
            "iterations".to_string(),
            Value::from(summary.iterations as u64),
        );
        attributes.insert(
            "assistant_messages".to_string(),
            Value::from(summary.assistant_messages.len() as u64),
        );
        attributes.insert(
            "tool_results".to_string(),
            Value::from(summary.tool_results.len() as u64),
        );
        attributes.insert(
            "prompt_cache_events".to_string(),
            Value::from(summary.prompt_cache_events.len() as u64),
        );
        session_tracer.record("turn_completed", attributes);
    }

    // Memory helpers (private)
    // -----------------------------------------------------------------------

    /// Build an effective system-prompt list that prepends memory context
    /// entries when the memory subsystem is active.
    ///
    /// Returns a clone of `self.system_prompt` when memory is disabled so the
    /// hot path has zero cost.
    #[cfg(test)]
    async fn prepare_reality_context(&self, user_input: &str) -> PromptAssembly {
        self.prepare_reality_context_with_budget(user_input, self.context_budget_tokens())
            .await
    }

    #[cfg(test)]
    async fn prepare_reality_context_with_budget(
        &self,
        user_input: &str,
        total_budget_tokens: u64,
    ) -> PromptAssembly {
        let next_model_context_items = self.take_next_model_context_items();
        self.prepare_reality_context_with_budget_and_items(
            user_input,
            total_budget_tokens,
            next_model_context_items,
        )
        .await
    }

    async fn prepare_reality_context_with_budget_and_items(
        &self,
        user_input: &str,
        total_budget_tokens: u64,
        next_model_context_items: Vec<ContextItem>,
    ) -> PromptAssembly {
        let _perf_start = std::time::Instant::now();

        let runtime_reality_context_items = self.runtime_reality_context_items(user_input);

        let Some(mgr) = self.memory_manager.as_ref() else {
            let unavailable_sources = vec![ContextSourceKind::Memory];
            let mut dynamic_items = runtime_reality_context_items;
            dynamic_items.extend(next_model_context_items);
            let envelope = self.build_context_envelope(
                user_input,
                dynamic_items,
                Vec::new(),
                unavailable_sources,
                total_budget_tokens,
            );
            return self.finalize_context_prompt(user_input, envelope, None);
        };

        // Convert session messages to memory's Message type for context scoring.
        // DESIGN: Tool blocks (ToolUse, ToolResult, Thinking) are explicitly excluded
        // from memory extraction. Only user/assistant text content is persisted.
        // Tool execution results are machine-optimised data, not knowledge worth retaining
        // in long-term memory (they can be re-derived by re-running the tool).
        let mem_messages: Vec<MemMessage> = self
            .session
            .read()
            .await
            .messages
            .iter()
            .enumerate()
            .map(|(idx, msg)| {
                let role = match msg.role {
                    crate::session::MessageRole::User => MemMessageRole::User,
                    crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                    crate::session::MessageRole::Tool => MemMessageRole::Tool,
                    crate::session::MessageRole::System => MemMessageRole::User,
                };
                // Extract only Text content blocks; ToolUse, ToolResult, Thinking blocks
                // are deliberately omitted from the memory extraction stream.
                let content: String = msg
                    .blocks
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                // Extract tool identity for tool result messages so the memory extractor
                // can properly attribute error-fix sequences.
                let (tool_use_id, tool_name) = match msg.role {
                    crate::session::MessageRole::Tool => {
                        let tid = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_use_id, .. } => {
                                Some(tool_use_id.clone())
                            }
                            _ => None,
                        });
                        let tname = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_name, .. } if !tool_name.is_empty() => {
                                Some(tool_name.clone())
                            }
                            _ => None,
                        });
                        (tid, tname)
                    }
                    _ => (None, None),
                };
                MemMessage {
                    turn_index: idx,
                    role,
                    content,
                    tool_use_id,
                    tool_name,
                    pinned: false,
                }
            })
            .collect();

        let session_id = self.session().session_id;
        let memory_ctx = self.memory_turn_context();
        let kernel = MemoryKernel::new(Arc::clone(mgr));
        let memory_budget = self.runtime_budget_plan().memory_retrieval_budget;
        let memory_budget_tokens = memory_budget.retrieval_budget.min(u64::from(u32::MAX));
        match kernel
            .context_packet(
                &memory_ctx,
                user_input,
                &mem_messages,
                memory_budget.selected_item_limit,
                memory_budget_tokens,
            )
            .await
        {
            Ok(packet) => {
                let packet =
                    crate::knowledge_activation::filter_packet_for_turn_intent(&packet, user_input);
                if packet.selected.is_empty() {
                    tracing::debug!(entries = 0, "memory context packet prepared");
                    if let Some(cb) = &self.memory_callback {
                        cb.on_memory_update(Vec::new(), "no memories found");
                    }
                    let omissions = packet
                        .omitted
                        .iter()
                        .map(|omitted| ContextOmission {
                            source: ContextSourceKind::Memory,
                            reason: format!("{}: {}", omitted.reason, omitted.title),
                            token_estimate: 0,
                        })
                        .collect();
                    let mut dynamic_items = runtime_reality_context_items;
                    dynamic_items.extend(next_model_context_items);
                    let envelope = self.build_context_envelope(
                        user_input,
                        dynamic_items,
                        omissions,
                        Vec::new(),
                        total_budget_tokens,
                    );
                    return self.finalize_context_prompt(user_input, envelope, None);
                }

                if let Some(cb) = &self.memory_callback {
                    let entries: Vec<(String, String, f64)> = packet
                        .selected
                        .iter()
                        .map(|item| {
                            (
                                format!("{:?}", item.atom.layer),
                                item.atom.title.clone(),
                                item.atom.confidence as f64,
                            )
                        })
                        .collect();
                    let status = format!("{} memory entries loaded", entries.len());
                    cb.on_memory_update(entries, &status);
                }

                tracing::debug!(
                    selected = packet.selected.len(),
                    omitted = packet.omitted.len(),
                    "memory context packet prepared"
                );
                let dynamic_items = packet
                    .selected
                    .iter()
                    .map(|item| {
                        let role = match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        };
                        let mut context_item = ContextItem::new(
                            item.atom.id.to_string(),
                            ContextSourceKind::Memory,
                            role,
                            format!(
                                "{}\ncontent: {}\nreason: {}\nevidence: {}",
                                item.atom.title,
                                item.content_preview,
                                item.reason,
                                item.atom.evidence_pointer.as_deref().unwrap_or("")
                            ),
                        );
                        context_item.authority = ContextAuthority::Session;
                        context_item.visibility = ContextVisibility::Private;
                        context_item.score = item.atom.confidence;
                        context_item.source_id = Some(item.atom.id.to_string());
                        context_item.source_reason = Some(item.reason.clone());
                        context_item.source_version = item
                            .atom
                            .evidence_pointer
                            .as_ref()
                            .map(|evidence| format!("evidence:{evidence}"));
                        if let Some(evidence) = item.atom.evidence_pointer.as_ref() {
                            context_item.evidence.push(evidence.clone());
                        }
                        context_item
                    })
                    .collect::<Vec<_>>();
                let knowledge_activation = match KnowledgeActivationRuntime::for_config_home(
                    crate::cowd_dirs::config_home_dir(),
                ) {
                    Ok(runtime) => runtime.activate_from_packet(
                        &session_id,
                        user_input,
                        &format!("{:?}", self.context_profile()),
                        &packet,
                    ),
                    Err(error) => {
                        tracing::warn!(%error, "durable knowledge activation unavailable");
                        None
                    }
                };
                let omissions = packet
                    .omitted
                    .iter()
                    .map(|omitted| ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    })
                    .collect::<Vec<_>>();
                let mut dynamic_items = dynamic_items;
                dynamic_items.extend(runtime_reality_context_items);
                dynamic_items.extend(next_model_context_items);
                let mut knowledge_report = None;
                if let Some(activation) = knowledge_activation {
                    knowledge_report = Some(activation.report.clone());
                    dynamic_items.extend(activation.items);
                    self.set_turn_knowledge_report(activation.report);
                }
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    omissions,
                    Vec::new(),
                    total_budget_tokens,
                );
                self.finalize_context_prompt(user_input, envelope, knowledge_report)
            }
            Err(err) => {
                tracing::warn!(%err, "memory: prepare_context failed, using base system prompt");
                if let Some(cb) = &self.memory_callback {
                    cb.on_memory_update(Vec::new(), &format!("memory error: {err}"));
                }
                let unavailable_sources = vec![ContextSourceKind::Memory];
                let mut dynamic_items = runtime_reality_context_items;
                dynamic_items.extend(next_model_context_items);
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    Vec::new(),
                    unavailable_sources,
                    total_budget_tokens,
                );
                self.finalize_context_prompt(user_input, envelope, None)
            }
        }
    }

    fn runtime_reality_context_items(&self, user_input: &str) -> Vec<ContextItem> {
        let Some((port, binding)) = &self.reality_recall else {
            return Vec::new();
        };
        let report = port.recall_for_binding(binding, user_input, 16);
        for source in &report.sources {
            if source.status == "degraded" {
                tracing::warn!(
                    source = ?source.source,
                    detail = ?source.detail,
                    "Runtime Fact/Matrix recall degraded"
                );
            }
        }
        if let Ok(mut last_report) = self.last_reality_recall_report.lock() {
            *last_report = Some(report.clone());
        }
        report.items
    }

    /// Perform post-turn memory housekeeping (micro-compact, drift, seeds).
    ///
    /// Errors are logged and swallowed so a memory failure never aborts a turn.
    async fn run_memory_post_turn(&self) -> Result<(), RuntimeError> {
        let Some((mgr, memory_ctx, mem_messages, callback)) = self.memory_post_turn_work().await
        else {
            return Ok(());
        };
        Self::complete_memory_post_turn(mgr, memory_ctx, mem_messages, callback).await;
        Ok(())
    }

    /// Gateway ingress already has a durable terminal receipt before this
    /// maintenance runs. Keep extraction, drift, and index work off the
    /// surface-critical path, while retaining the exact same maintenance
    /// implementation and telemetry used by synchronous Agent turns.
    async fn schedule_memory_post_turn(&self) {
        let Some((mgr, memory_ctx, mem_messages, callback)) = self.memory_post_turn_work().await
        else {
            return;
        };
        tokio::spawn(async move {
            Self::complete_memory_post_turn(mgr, memory_ctx, mem_messages, callback).await;
        });
    }

    async fn memory_post_turn_work(
        &self,
    ) -> Option<(
        Arc<CognitiveContextManager>,
        MemoryTurnContext,
        Vec<MemMessage>,
        Option<Arc<dyn MemoryCallback>>,
    )> {
        let mgr = Arc::clone(self.memory_manager.as_ref()?);
        let memory_ctx = self.memory_turn_context();

        // Convert session messages to memory's Message type for post-turn extraction.
        // DESIGN: Tool blocks are excluded (same rationale as prepare_reality_context).
        let mem_messages: Vec<MemMessage> = self
            .session
            .read()
            .await
            .messages
            .iter()
            .enumerate()
            .map(|(idx, msg)| {
                let role = match msg.role {
                    crate::session::MessageRole::User => MemMessageRole::User,
                    crate::session::MessageRole::Assistant => MemMessageRole::Assistant,
                    crate::session::MessageRole::Tool => MemMessageRole::Tool,
                    crate::session::MessageRole::System => MemMessageRole::User,
                };
                // Extract only Text blocks; tool blocks are deliberately omitted.
                let content: String = msg
                    .blocks
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                // Pass tool identity for tool result messages.
                let (tool_use_id, tool_name) = match msg.role {
                    crate::session::MessageRole::Tool => {
                        let tid = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_use_id, .. } => {
                                Some(tool_use_id.clone())
                            }
                            _ => None,
                        });
                        let tname = msg.blocks.iter().find_map(|b| match b {
                            ContentBlock::ToolResult { tool_name, .. } if !tool_name.is_empty() => {
                                Some(tool_name.clone())
                            }
                            _ => None,
                        });
                        (tid, tname)
                    }
                    _ => (None, None),
                };
                MemMessage {
                    turn_index: idx,
                    role,
                    content,
                    tool_use_id,
                    tool_name,
                    pinned: false,
                }
            })
            .collect();

        Some((mgr, memory_ctx, mem_messages, self.memory_callback.clone()))
    }

    async fn complete_memory_post_turn(
        mgr: Arc<CognitiveContextManager>,
        memory_ctx: MemoryTurnContext,
        mem_messages: Vec<MemMessage>,
        callback: Option<Arc<dyn MemoryCallback>>,
    ) {
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        let start = Instant::now();
        let mut maintenance_messages = mem_messages;
        let post_turn_result = kernel
            .post_turn(&memory_ctx, &mut maintenance_messages)
            .await;
        let elapsed = start.elapsed();
        tracing::info!(
            elapsed_ms = elapsed.as_millis(),
            "post_turn: memory kernel completed"
        );

        if let Err(ref e) = post_turn_result {
            tracing::warn!(%e, "post_turn: memory kernel failed");
        }

        if let Some(cb) = callback {
            let layers_data = mgr.list_layers().await;
            let total_entries: usize = layers_data
                .iter()
                .filter_map(|l| {
                    l.get("entry_count")
                        .and_then(|c| c.as_u64())
                        .map(|c| c as usize)
                })
                .sum();
            let layer_names: Vec<String> = layers_data
                .iter()
                .filter_map(|l| {
                    l.get("layer")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect();
            let vector_count = mgr.vector_index_count();
            cb.on_memory_stats(total_entries, vector_count, layer_names);
        }
    }

    /// Write a message to the durable SQLite session store via a spawned
    /// background task. The in-memory session remains the hot turn state;
    /// SQLite is the managed session source of truth. JSONL is only used by
    /// explicit import/export codecs.

    fn maybe_index_tool_output(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        output: &str,
        access: Option<&EvidenceAccessRef>,
    ) {
        if output.lines().count() < DEFAULT_OUTPUT_REF_MIN_LINES && output.chars().count() < 16_000
        {
            return;
        }
        let Some(ref sandbox) = self.tool_output_sandbox else {
            return;
        };
        let Ok(mut guard) = sandbox.lock() else {
            tracing::warn!(
                tool_call_id = tool_use_id,
                "tool output sandbox lock poisoned"
            );
            return;
        };
        let content_hash = format!(
            "{:016x}",
            model_protocol::prompt_cache::stable_hash_bytes(output.as_bytes())
        );
        let summary = if let Some(access) = access {
            let evidence = memory::types::CanonicalRawEvidence::new(
                access.clone(),
                preview_chars(output, 600),
            );
            guard.index_tool_output_with_evidence(
                tool_use_id,
                tool_name,
                output,
                DEFAULT_OUTPUT_REF_MIN_LINES,
                &evidence,
            )
        } else {
            guard.index_tool_output_ephemeral(
                tool_use_id,
                output,
                DEFAULT_OUTPUT_REF_MIN_LINES,
                tool_use_id,
                &content_hash,
            )
        };
        if let Some(summary) = summary {
            tracing::debug!(
                tool_call_id = tool_use_id,
                tool_name,
                total_lines = summary.total_lines,
                full_size_bytes = summary.full_size_bytes,
                "indexed oversized tool output"
            );
        }
    }

    fn retrieve_tool_evidence(&self, input: &str) -> Result<String, String> {
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
        let Some(sandbox) = self.tool_output_sandbox.as_ref() else {
            return Err("tool evidence sandbox is unavailable".to_string());
        };
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

    fn record_context_component(
        &self,
        component: crate::context_ledger::ContextComponentKind,
        tokens: u64,
        reference: Option<String>,
        request_sequence: usize,
    ) {
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger.record(component, tokens, reference, request_sequence);
        }
    }

    fn record_provider_context_request(&self, request: &ApiRequest, request_sequence: usize) {
        let mut system_tokens = crate::context_ledger::estimate_text_tokens(
            &request.prompt.trusted_system.join("\n\n"),
        );
        let capability_tokens = request
            .prompt
            .trusted_system
            .iter()
            .filter(|fragment| {
                fragment.starts_with("## Runtime evidence plan")
                    || fragment.starts_with("## Runtime execution decision")
            })
            .map(|fragment| crate::context_ledger::estimate_text_tokens(fragment))
            .sum::<u64>();
        let mut history_tokens = 0u64;
        let mut tool_input_tokens = 0u64;
        let mut tool_result_tokens = 0u64;
        for block in request
            .messages
            .iter()
            .flat_map(|message| message.blocks.iter())
        {
            match block {
                ContentBlock::Text { text } => {
                    history_tokens = history_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(text));
                }
                ContentBlock::Image {
                    media_type, data, ..
                } => {
                    history_tokens = history_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(media_type))
                        .saturating_add((data.len() as u64).div_ceil(4));
                }
                ContentBlock::Thinking { thinking, .. } => {
                    history_tokens = history_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(thinking));
                }
                ContentBlock::ToolUse { id, name, input } => {
                    tool_input_tokens = tool_input_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(id))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(name))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(input));
                }
                ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    ..
                } => {
                    tool_result_tokens = tool_result_tokens
                        .saturating_add(crate::context_ledger::estimate_text_tokens(tool_use_id))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(tool_name))
                        .saturating_add(crate::context_ledger::estimate_text_tokens(output));
                }
            }
        }
        let mut memory_tokens = 0u64;
        let mut handoff_tokens = 0u64;
        let mut contextual_tokens = 0u64;
        for packet in &request.prompt.contextual_packets {
            let tokens =
                crate::context_ledger::estimate_text_tokens(&packet.render_for_user_context());
            match packet.source {
                ContextSourceKind::Memory
                | ContextSourceKind::Knowledge
                | ContextSourceKind::Fact
                | ContextSourceKind::Matrix => {
                    memory_tokens = memory_tokens.saturating_add(tokens);
                }
                ContextSourceKind::AgentPeer | ContextSourceKind::Handoff => {
                    handoff_tokens = handoff_tokens.saturating_add(tokens);
                }
                _ => {
                    contextual_tokens = contextual_tokens.saturating_add(tokens);
                }
            }
        }
        system_tokens = system_tokens
            .saturating_add(contextual_tokens)
            .saturating_sub(capability_tokens);
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger
                .begin_request_with_budget(request_sequence, request.budget.hard_input_cap_tokens);
        }
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::System,
            system_tokens,
            Some(format!("provider-request:{request_sequence}:system")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::History,
            history_tokens,
            Some(format!("provider-request:{request_sequence}:history")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::Memory,
            memory_tokens,
            Some(format!("provider-request:{request_sequence}:memory")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::AgentHandoff,
            handoff_tokens,
            Some(format!("provider-request:{request_sequence}:handoff")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::Capability,
            capability_tokens,
            Some(format!(
                "provider-request:{request_sequence}:runtime-capability"
            )),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolInput,
            tool_input_tokens,
            Some(format!("provider-request:{request_sequence}:tool-input")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolResult,
            tool_result_tokens,
            Some(format!("provider-request:{request_sequence}:tool-result")),
            request_sequence,
        );
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolSchema,
            request.budget.fixed_input_tokens.saturating_sub(
                crate::context_ledger::estimate_text_tokens(
                    &request.prompt.trusted_system.join("\n\n"),
                )
                .saturating_add(history_tokens),
            ),
            Some(format!("provider-request:{request_sequence}:tools")),
            request_sequence,
        );
    }

    fn reconcile_provider_context_usage(&self, usage: TokenUsage) {
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            ledger.reconcile_input_tokens(u64::from(usage.input_tokens));
        }
    }

    async fn record_tool_raw_evidence(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input_hash: &str,
        output: &str,
        is_error: bool,
        duration_ms: u64,
        source_evidence_ref: Option<&str>,
    ) -> (EvidenceRef, Option<EvidenceAccessRef>) {
        let content_hash = model_protocol::prompt_cache::stable_hash_bytes(output.as_bytes());
        let evidence_id = format!("tool-raw-{tool_use_id}-{content_hash:016x}");
        let evidence_ref = EvidenceRef::new("tool", evidence_id.clone());
        if let Some(access) = self.existing_evidence_access(&evidence_ref) {
            return (evidence_ref, Some(access));
        }
        let Some(ref store) = self.session_store else {
            return (evidence_ref, None);
        };
        let session_id = self.session().session_id;
        let metadata = serde_json::json!({
            "type": "ToolObservationRaw",
            "evidence_id": evidence_id,
            "session_id": session_id,
            "tool_call_id": tool_use_id,
            "tool_name": tool_name,
            "input_hash": input_hash,
            "is_error": is_error,
            "duration_ms": duration_ms,
            "line_count": output.lines().count(),
            "byte_count": output.len(),
            "source_evidence_ref": source_evidence_ref,
        });
        let facade = crate::context_evidence::raw::RawEvidenceFacade::new(
            crate::context_evidence::raw::SessionStoreRawEvidenceStore::new(Arc::clone(store)),
        );
        let access = match facade
            .persist(crate::context_evidence::raw::RawEvidenceWrite {
                evidence_ref: evidence_ref.clone(),
                session_id: session_id.clone(),
                media_type: "text/plain; charset=utf-8".to_string(),
                visibility_scope: format!("session:{session_id}"),
                payload: output.as_bytes().to_vec(),
                metadata,
            })
            .await
        {
            Ok(access) => access,
            Err(error) => {
                tracing::warn!(
                    %error,
                    session_id,
                    evidence_id,
                    "tool raw evidence append failed; retaining bounded ephemeral receipt"
                );
                return (evidence_ref, None);
            }
        };
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            let _ = ledger.register_evidence_hash(evidence_id);
        }
        (evidence_ref, Some(access))
    }

    /// Produce a bounded model receipt for an outcome already executed by the
    /// graph-owned tool host. The graph remains responsible for publication;
    /// this method only persists raw evidence and updates context governance.
    pub(crate) async fn prepare_governed_tool_result(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> ConversationMessage {
        let input_hash = format!(
            "{:016x}",
            model_protocol::prompt_cache::stable_hash_bytes(input.as_bytes())
        );
        let source_evidence_ref = format!("runtime-tool:{tool_use_id}");
        let (raw_ref, raw_access) = self
            .record_tool_raw_evidence(
                tool_use_id,
                tool_name,
                &input_hash,
                output,
                is_error,
                0,
                Some(&source_evidence_ref),
            )
            .await;
        self.maybe_index_tool_output(raw_ref.id(), tool_name, output, raw_access.as_ref());
        let receipt =
            self.tool_model_receipt(tool_name, output, is_error, &raw_ref, raw_access.as_ref());
        self.push_turn_evidence_audit(crate::context_evidence::audit_projection(
            &receipt,
            raw_access.as_ref(),
        ));
        let summary = receipt.summary;
        self.emit_tool_completed(
            tool_use_id,
            tool_name,
            output,
            if is_error { Some(1) } else { Some(0) },
        );
        self.push_turn_tool_observation(ToolObservation::new(
            tool_name.to_string(),
            tool_use_id.to_string(),
            raw_ref,
            summary.clone(),
        ));
        ConversationMessage::tool_result(
            tool_use_id.to_string(),
            tool_name.to_string(),
            summary,
            is_error,
        )
    }

    fn tool_model_receipt(
        &self,
        tool_name: &str,
        output: &str,
        is_error: bool,
        raw_ref: &EvidenceRef,
        access: Option<&EvidenceAccessRef>,
    ) -> crate::context_evidence::ModelReceipt {
        let raw_tokens = crate::context_ledger::estimate_text_tokens(output);
        let per_tool_limit = self
            .runtime_budget_plan()
            .tool_result_budget
            .per_tool_max_tokens as u64;
        // `build_tool_receipt` spends part of the granted budget on its
        // evidence URI and structured summary prefix. Reserving only the raw
        // body size made even a tiny exact `read_file` JSON lose its `content`
        // field to head-tail truncation. Keep bounded headroom for the receipt
        // envelope while preserving the existing per-tool hard ceiling.
        let requested = raw_tokens.saturating_add(96).min(per_tool_limit).max(1);
        let granted = self
            .turn_context_ledger
            .lock()
            .map(|mut ledger| ledger.reserve_tool_result(requested))
            .unwrap_or(requested);
        let mut receipt = crate::context_evidence::build_tool_receipt(
            tool_name,
            output,
            is_error,
            raw_ref.clone(),
            granted.max(24),
        );
        // `runtime_orchestrate` already returns a deliberately bounded model
        // receipt. Preserve a completed terminal summary as valid JSON so the
        // parent graph can consume it directly even on embedded/legacy hosts;
        // generic head-tail evidence compaction can otherwise split the JSON
        // and force an unnecessary parent model round.
        if !is_error
            && tool_name.eq_ignore_ascii_case("runtime_orchestrate")
            && output.len() <= 24_000
            && serde_json::from_str::<serde_json::Value>(output)
                .ok()
                .is_some_and(|value| {
                    value.get("status").and_then(serde_json::Value::as_str) == Some("completed")
                        && value
                            .get("terminal_summary")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|summary| !summary.trim().is_empty())
                })
        {
            receipt.summary = output.to_string();
            receipt.receipt_tokens = raw_tokens;
            receipt.omitted_tokens = 0;
            receipt.truncated = false;
        }
        if access.is_none() {
            if receipt.summary.starts_with("Tool `") {
                receipt.summary = receipt.summary.replacen(
                    "Evidence: tool://",
                    "Ephemeral evidence (active runtime only): tool://",
                    1,
                );
            }
            receipt.receipt_tokens = crate::context_ledger::estimate_text_tokens(&receipt.summary);
            receipt.omitted_tokens = raw_tokens.saturating_sub(receipt.receipt_tokens);
            receipt.truncated = receipt.receipt_tokens < raw_tokens;
        }
        self.record_context_component(
            crate::context_ledger::ContextComponentKind::ToolResult,
            receipt.receipt_tokens,
            access.map(|_| format!("tool://{}", raw_ref.id())),
            self.session().messages.len(),
        );
        receipt
    }

    #[allow(dead_code)]
    fn start_tool_invocation_record(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
    ) -> ToolInvocationRecord {
        let session_id = self.session().session_id;
        let safety_category = self
            .tool_executor
            .classify_tool_safety(tool_name, input)
            .unwrap_or_else(|| crate::classify_tool_request(tool_name, input));
        ToolInvocationRecord::started(
            session_id,
            iterations,
            tool_use_id.to_string(),
            tool_name.to_string(),
            input,
            safety_category,
            now_ms(),
        )
    }

    fn record_tool_invocation_denied(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        iterations: usize,
        failure_kind: ToolFailureKind,
        reason: &str,
    ) {
        let record = self
            .start_tool_invocation_record(tool_use_id, tool_name, input, iterations)
            .failed(failure_kind, reason, now_ms());
        self.record_tool_invocation_event(
            &record,
            "tool.invocation.denied",
            self.session().messages.len(),
        );
    }

    fn record_tool_invocation_event(
        &self,
        record: &ToolInvocationRecord,
        kind: &'static str,
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            kind,
            Some(record.status.as_str().to_string()),
            vec![
                RuntimeEventRef {
                    kind: "tool_invocation".to_string(),
                    id: record.invocation_id.clone(),
                },
                RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: record.tool_call_id.clone(),
                },
            ],
            serde_json::to_value(record).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        );
    }

    fn record_tool_execution_plan(&self, plan: &ToolExecutionPlan, _sequence: usize) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.execution_plan.created",
            Some("planned".to_string()),
            vec![RuntimeEventRef {
                kind: "tool_execution_plan".to_string(),
                id: plan.plan_id.clone(),
            }],
            serde_json::to_value(plan).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        );
    }

    fn record_tool_strategy_validation(
        &self,
        report: &ToolExecutionPolicyValidationReport,
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.strategy_validation.completed",
            Some(if report.allowed { "allowed" } else { "denied" }.to_string()),
            vec![RuntimeEventRef {
                kind: "strategy_lease".to_string(),
                id: report.lease_id.clone(),
            }],
            serde_json::to_value(report).unwrap_or_else(|_| {
                serde_json::json!({
                    "allowed": false,
                    "findings": ["strategy_validation_serialization_failed"],
                    "lease_id": report.lease_id,
                })
            }),
        );
    }

    fn record_tool_schedule(
        &self,
        schedule: &crate::execution_scheduler::ToolSchedule,
        requests: &[crate::tool_dispatch::ToolRequest],
        _sequence: usize,
    ) {
        self.append_execution_runtime_event(
            RuntimeEventScope::Schedule,
            "tool.schedule.created",
            Some("planned".to_string()),
            requests
                .iter()
                .map(|request| RuntimeEventRef {
                    kind: "tool_call".to_string(),
                    id: request.tool_use_id.clone(),
                })
                .collect(),
            serde_json::json!({
                "schedule": schedule,
                "tool_count": requests.len(),
            }),
        );
    }

    fn record_ai_kernel_trace_event(&self, trace: &RuntimeAiKernelTrace, sequence: usize) {
        if self.runtime_event_store.is_none() {
            return;
        }
        let payload = serde_json::json!({
            "strategy": {
                "pattern": trace.execution_decision.strategy.pattern.as_str(),
                "confidence": trace.execution_decision.strategy.confidence,
                "policy_version": trace.execution_decision.strategy.policy_version,
                "reasons": trace.execution_decision.strategy.reasons,
                "required_capabilities": trace.execution_decision.strategy.required_capabilities.iter().map(|item| format!("{item:?}")).collect::<Vec<_>>(),
                "complexity": format!("{:?}", trace.execution_decision.strategy.understanding.complexity),
                "risk": format!("{:?}", trace.execution_decision.strategy.understanding.risk),
                "modifiers": trace.execution_decision.strategy.modifiers.iter().map(|item| item.as_str()).collect::<Vec<_>>(),
            },
            "collaboration": {
                "template_id": trace.collaboration_decision.template_id.as_str(),
                "rationale": trace.collaboration_decision.rationale,
            },
            "context": {
                "epoch_id": trace.context_epoch.epoch_id,
                "envelope_id": trace.context_envelope_id,
                "token_total": trace.context_epoch.token_total,
                "selected_count": trace.context_epoch.selected.len(),
                "omitted_count": trace.context_epoch.omitted.len(),
                "alignment": trace.context_alignment,
            },
            "verification": {
                "can_finalize": trace.verification_report.can_finalize,
                "verification_blocked": trace.verification_blocked,
                "severity": format!("{:?}", trace.verification_report.severity),
                "blocking_reasons": trace.verification_report.blocking_reasons,
                "claim_count": trace.verification_report.claim_count,
                "evidence_count": trace.verification_report.evidence_count,
                "unsupported_required_count": trace.verification_report.unsupported_required_claims.len(),
                "not_run_count": trace.verification_report.not_run_claims.len(),
                "matrix_missing_evidence": matrix_missing_evidence(trace),
            },
            "tool_transaction": trace.tool_transaction.as_ref().map(|plan| serde_json::json!({
                "id": plan.id,
                "batch_count": plan.batches.len(),
                "requires_checkpoint": plan.requires_checkpoint,
                "requires_human_confirm": plan.requires_human_confirm,
                "warning_count": plan.warnings.len(),
            })),
            "harness": {
                "receipt_id": trace.harness_receipt.id,
                "harness_id": trace.harness_receipt.harness_id,
                "agent_spec_id": trace.harness_receipt.agent_spec_id,
                "strategy_pattern": trace.harness_receipt.strategy_pattern,
                "context_epoch_id": trace.harness_receipt.context_epoch_id,
                "tool_transaction_id": trace.harness_receipt.tool_transaction_id,
                "verification_can_finalize": trace.harness_receipt.verification_can_finalize,
                "policy_receipts": trace.harness_receipt.policy_receipts,
                "output_summary": trace.harness_receipt.output_summary,
            },
            "policy_receipts": trace.policy_receipts.iter().map(|receipt| serde_json::json!({
                "id": receipt.id,
                "scope": format!("{:?}", receipt.scope),
                "decision": format!("{:?}", receipt.decision),
                "reasons": receipt.reasons,
                "evidence_refs": receipt.evidence_refs,
                "source_policy": receipt.source_policy,
                "created_at": receipt.created_at,
            })).collect::<Vec<_>>(),
            "behavior_policy": {
                "necessity": trace.behavior_policy.necessity,
                "reuse_opportunities": trace.behavior_policy.reuse_opportunities,
                "overengineering_risks": trace.behavior_policy.overengineering_risks,
                "safety_exceptions": trace.behavior_policy.safety_exceptions,
                "recommended_scope": format!("{:?}", trace.behavior_policy.recommended_scope),
                "enforcement": {
                    "allow_execution": trace.behavior_policy.enforcement.allow_execution,
                    "requires_scope_downgrade": trace.behavior_policy.enforcement.requires_scope_downgrade,
                    "requires_human_review": trace.behavior_policy.enforcement.requires_human_review,
                },
                "eval_checks": trace.behavior_policy.eval_checks,
            },
            "execution_graph": trace.execution_graph.as_ref().map(|graph| serde_json::json!({
                "id": graph.id,
                "node_count": graph.nodes.len(),
                "edge_count": graph.edges.len(),
            })),
            "execution_graph_quality": trace.execution_graph_quality.as_ref().map(|quality| serde_json::json!({
                "node_count": quality.node_count,
                "edge_count": quality.edge_count,
                "ready_count": quality.ready_count,
                "blocked_count": quality.blocked_count,
                "failed_count": quality.failed_count,
                "has_verify_node": quality.has_verify_node,
                "has_synthesize_node": quality.has_synthesize_node,
                "is_dag": quality.is_dag,
                "warnings": quality.warnings,
            })),
            "bench": {
                "passed": trace.bench_result.passed,
                "score": trace.bench_result.score,
                "case_id": trace.bench_result.case_id,
                "reasons": trace.bench_result.reasons,
            },
            "regression_gate": {
                "allowed": trace.regression_gate.allowed,
                "average_score": trace.regression_gate.average_score,
                "failed": trace.regression_gate.failed,
                "reasons": trace.regression_gate.reasons,
            },
            "growth": {
                "record_id": trace.learning_record.id,
                "event_id": trace.growth_event.id,
                "policy": trace.learning_record.policy,
                "has_blocker": trace.learning_record.has_blocker(),
                "signals": trace.learning_record.signals.iter().map(|signal| serde_json::json!({
                    "kind": format!("{:?}", signal.kind),
                    "severity": format!("{:?}", signal.severity),
                    "summary": signal.summary,
                })).collect::<Vec<_>>(),
                "next_strategy_hints": trace.learning_record.next_strategy_hints,
            },
            "strategy_experience": strategy_experience_projection(trace),
            "maintenance_candidates": growth_maintenance_candidates(trace),
            "matrix_evidence_signal": {
                "source": "ai_kernel_trace",
                "growth_event_id": trace.growth_event.id,
                "packet_contract": {
                    "problem_statement": "AI harness execution quality",
                    "trace_ref": format!("runtime:event:{sequence}"),
                    "strategy_pattern": trace.execution_decision.strategy.pattern.as_str(),
                    "verification_can_finalize": trace.verification_report.can_finalize,
                    "regression_allowed": trace.regression_gate.allowed,
                    "harness_receipt_id": trace.harness_receipt.id,
                },
                "evidence_refs": trace.growth_event.evidence_refs,
                "signals": trace.growth_event.matrix_signals,
                "missing_evidence": matrix_missing_evidence(trace),
            },
        });
        self.append_execution_runtime_event(
            RuntimeEventScope::Task,
            "runtime.harness_contract.trace",
            Some(if trace.verification_report.can_finalize {
                "completed".to_string()
            } else {
                "degraded".to_string()
            }),
            vec![RuntimeEventRef {
                kind: "harness_receipt".to_string(),
                id: trace.harness_receipt.id.clone(),
            }],
            payload,
        );
    }

    fn strategy_input_for_turn(&self, user_input: &str) -> StrategyInput {
        let _io_guard = STRATEGY_EXPERIENCE_IO_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = strategy_experience_path();
        let mut store = StrategyExperienceStore::load_or_default(path.clone());
        if let Ok(report_path) = std::env::var("COWD_STRATEGY_CALIBRATION_REPORT") {
            match std::fs::read(&report_path)
                .map_err(|error| error.to_string())
                .and_then(|bytes| {
                    serde_json::from_slice::<serde_json::Value>(&bytes)
                        .map_err(|error| error.to_string())
                })
            {
                Ok(report) => {
                    let positive = store.import_paired_evaluation_report(&report);
                    let negative = store.import_negative_benefit_report(&report);
                    let imported = positive.as_ref().copied().unwrap_or(0)
                        + negative.as_ref().copied().unwrap_or(0);
                    if imported > 0 {
                        if let Err(error) = store.save(&path) {
                            tracing::warn!(
                                %error,
                                report_path,
                                "failed to persist imported strategy calibration"
                            );
                        }
                    }
                    if let (Err(positive), Err(negative)) = (positive, negative) {
                        tracing::warn!(
                            positive_error = %positive,
                            negative_error = %negative,
                            report_path,
                            "rejected strategy calibration report"
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    %error,
                    report_path,
                    "failed to read strategy calibration report"
                ),
            }
        }
        store.enrich_input(StrategyInput::from_prompt(user_input.to_string()))
    }

    /// Admit exactly one strategy identity for a turn. This is the only
    /// conversation-layer call site allowed to create a decision.
    pub(crate) fn begin_turn_strategy(
        &self,
        turn_ref: impl Into<String>,
        user_input: &str,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        self.begin_turn_strategy_with_resource_snapshot(turn_ref, user_input, None)
    }

    pub(crate) fn begin_turn_strategy_with_resource_snapshot(
        &self,
        turn_ref: impl Into<String>,
        user_input: &str,
        resource_snapshot: Option<harness_contract::strategy::StrategyResourceSnapshot>,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let turn_ref = turn_ref.into();
        let mut guard = self
            .active_turn_strategy
            .lock()
            .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
        if let Some(active) = guard.as_ref() {
            if active.turn_ref == turn_ref {
                return Ok(active.clone());
            }
            return Err(RuntimeError::new(format!(
                "turn `{turn_ref}` cannot replace active strategy turn `{}`",
                active.turn_ref
            )));
        }
        let evaluation_isolated = resource_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.sample_source.contains("corpus="));
        let mut strategy_input = if evaluation_isolated {
            StrategyInput::from_prompt(user_input.to_string())
        } else {
            self.strategy_input_for_turn(user_input)
        };
        if let Some(resource_snapshot) = resource_snapshot {
            strategy_input = strategy_input.with_resource_snapshot(resource_snapshot);
        }
        apply_e2e_strategy_fixture(&mut strategy_input, user_input)?;
        let mut decision = crate::execution_core::StrategyDecisionEngine.decide_with_input(
            strategy_input,
            Some(self.context_profile()),
            crate::execution_core::StrategyResourceHealth {
                provider_available: self.api_client.provider_available(),
                tools_available: self.tool_executor.has_registered_tools(),
                collaboration_available: self.runtime_control_policy.enabled
                    && self.runtime_control_policy.agent.enabled
                    && self.context_profile() != ContextProfile::SubAgent
                    && self.tool_executor.collaboration_runtime_available(),
                mission_available: self.runtime_control_policy.enabled
                    && self.tool_executor.mission_runtime_available(),
                observed: true,
            },
        );
        apply_eval_strategy_override(&mut decision)?;
        if !decision.executable {
            return Err(RuntimeError::new(format!(
                "runtime strategy is not executable: {}",
                decision.blocked_reasons.join("; ")
            )));
        }
        let state = crate::execution_core::TurnStrategyDecisionState::admitted(
            decision,
            self.session().session_id,
            turn_ref,
        );
        *guard = Some(state.clone());
        Ok(state)
    }

    #[must_use]
    pub(crate) fn active_turn_strategy(
        &self,
    ) -> Option<crate::execution_core::TurnStrategyDecisionState> {
        self.active_turn_strategy
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    pub(crate) fn bind_turn_strategy_execution(
        &self,
        turn_ref: &str,
        execution_graph_ref: &str,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let recovered = self.recover_turn_strategy_identity(turn_ref, execution_graph_ref);
        let recovered_identity = recovered.is_some();
        let (state, should_emit, previous) = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let previous = guard.clone();
            let state = guard
                .as_mut()
                .filter(|state| state.turn_ref == turn_ref)
                .ok_or_else(|| RuntimeError::new("turn strategy binding scope mismatch"))?;
            let should_emit = state.execution_graph_ref.is_none() && !recovered_identity;
            if let Some(recovered) = recovered {
                state.decision_id = recovered.decision_id.clone();
                state.decision_lease = recovered.decision_lease.clone();
                state.revision = recovered.revision;
                state.policy_version.clone_from(&recovered.policy_version);
                state.selected_candidate = recovered.selected_candidate;
                state.status = recovered.status;
                state.resource_snapshot = recovered.resource_snapshot;
                state.collaboration_receipt = recovered.collaboration_receipt;
                state.focus_partition_plans = recovered.focus_partition_plans;
                state.decision.decision_id = recovered.decision_id;
                state.decision.decision_revision = recovered.revision;
                state.decision.lease.lease_id = recovered.decision_lease;
                state.decision.strategy.policy_version = recovered.policy_version;
                state.decision.strategy.selected_candidate = recovered.selected_candidate;
                state.decision.strategy.resource_snapshot = state.resource_snapshot.clone();
                state.decision.strategy.candidate_estimates = recovered.candidate_estimates;
                let recovered_pattern = recovered.pattern;
                state
                    .decision
                    .strategy
                    .retarget(
                        recovered_pattern,
                        "recovered the durable turn strategy identity before graph resume",
                    )
                    .map_err(RuntimeError::new)?;
                state.decision.lease.locked_pattern = recovered_pattern;
                state.decision.compile_target =
                    crate::execution_core::ExecutionPatternCatalog::current()
                        .find(recovered_pattern)
                        .map_or(
                            crate::execution_core::RuntimeCompileTarget::InlineModel,
                            |spec| spec.compile_target,
                        );
            }
            match state.execution_graph_ref.as_deref() {
                Some(graph_id) if graph_id == execution_graph_ref => {}
                Some(_) => {
                    return Err(RuntimeError::new(
                        "turn strategy cannot be rebound to another execution graph",
                    ));
                }
                None if should_emit || recovered_identity => {
                    // A recovered identity was filtered by this exact graph
                    // reference above.  Rehydrate that durable binding without
                    // producing a second selected event.
                    state.bind_execution_graph(execution_graph_ref);
                }
                None => {
                    return Err(RuntimeError::new(
                        "turn strategy cannot be rebound to another execution graph",
                    ));
                }
            }
            (state.clone(), should_emit, previous)
        };
        if should_emit {
            if let Err(error) = self.append_turn_strategy_event(
                "runtime.strategy.selected",
                &state,
                "turn admitted and parent execution graph bound",
            ) {
                *self
                    .active_turn_strategy
                    .lock()
                    .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                    previous;
                return Err(error);
            }
        }
        Ok(state)
    }

    fn recover_turn_strategy_identity(
        &self,
        turn_ref: &str,
        execution_graph_ref: &str,
    ) -> Option<RecoveredTurnStrategyIdentity> {
        let store = self.runtime_event_store.as_ref()?;
        let session_id = self.session().session_id;
        let events = store
            .list_stream(&format!("session:{session_id}"))
            .map_err(|error| {
                tracing::warn!(%error, session_id, turn_ref, "failed to inspect durable strategy identity");
                error
            })
            .ok()?;
        events
            .into_iter()
            .rev()
            .filter(|event| {
                matches!(
                    event.kind.as_str(),
                    "runtime.strategy.selected"
                        | "runtime.strategy.downgraded"
                        | "runtime.strategy.early_stopped"
                        | "runtime.strategy.outcome"
                )
            })
            .find_map(|event| {
                let payload = event.payload;
                if payload.get("turn_ref").and_then(serde_json::Value::as_str) != Some(turn_ref)
                    || payload
                        .get("execution_graph_ref")
                        .and_then(serde_json::Value::as_str)
                        != Some(execution_graph_ref)
                {
                    return None;
                }
                let pattern = match payload
                    .get("selected_pattern")
                    .and_then(serde_json::Value::as_str)?
                {
                    "direct" => harness_contract::core::ExecutionPattern::Direct,
                    "explore" => harness_contract::core::ExecutionPattern::Explore,
                    "execute" => harness_contract::core::ExecutionPattern::Execute,
                    "deliberate" => harness_contract::core::ExecutionPattern::Deliberate,
                    "collaborate" => harness_contract::core::ExecutionPattern::Collaborate,
                    "supervise" => harness_contract::core::ExecutionPattern::Supervise,
                    _ => return None,
                };
                Some(RecoveredTurnStrategyIdentity {
                    decision_id: payload.get("decision_id")?.as_str()?.to_string(),
                    decision_lease: payload.get("decision_lease")?.as_str()?.to_string(),
                    revision: payload.get("decision_revision")?.as_u64()?,
                    policy_version: payload.get("policy_version")?.as_str()?.to_string(),
                    selected_candidate: serde_json::from_value(
                        payload.get("selected_candidate")?.clone(),
                    )
                    .ok()?,
                    status: serde_json::from_value(payload.get("status")?.clone()).ok()?,
                    resource_snapshot: serde_json::from_value(
                        payload.get("resource_snapshot")?.clone(),
                    )
                    .ok()?,
                    candidate_estimates: serde_json::from_value(
                        payload.get("candidate_estimates")?.clone(),
                    )
                    .ok()?,
                    collaboration_receipt: payload
                        .get("collaboration_receipt")
                        .filter(|value| !value.is_null())
                        .cloned(),
                    focus_partition_plans: payload
                        .get("evidence_scopes")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default(),
                    pattern,
                })
            })
    }

    fn revise_active_turn_strategy(
        &self,
        selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
        pattern: harness_contract::core::ExecutionPattern,
        status: crate::execution_core::TurnStrategyDecisionStatus,
        reason: &str,
        event_kind: Option<&'static str>,
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        let (state, previous) = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let state = guard
                .as_mut()
                .ok_or_else(|| RuntimeError::new("turn strategy revision has no owner"))?;
            let previous = state.clone();
            if state.selected_candidate == selected_candidate
                && state.decision.pattern() == pattern
                && status == crate::execution_core::TurnStrategyDecisionStatus::Running
            {
                return Ok(state.decision.clone());
            }
            state
                .revise_to_pattern(selected_candidate, pattern, status, reason)
                .map_err(RuntimeError::new)?;
            (state.clone(), previous)
        };
        if let Some(kind) = event_kind {
            if let Err(error) = self.append_turn_strategy_event(kind, &state, reason) {
                *self
                    .active_turn_strategy
                    .lock()
                    .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                    Some(previous);
                return Err(error);
            }
        }
        Ok(state.decision)
    }

    fn retarget_active_turn_strategy(
        &self,
        selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
        pattern: harness_contract::core::ExecutionPattern,
        reason: &str,
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        // A provider ToolBatch can change the selected candidate in either
        // direction. It is therefore a new revision of the allowed
        // `selected` fact, not an invented fifth transition kind and not
        // necessarily a downgrade.
        self.revise_active_turn_strategy(
            selected_candidate,
            pattern,
            crate::execution_core::TurnStrategyDecisionStatus::Running,
            reason,
            Some("runtime.strategy.selected"),
        )
    }

    pub(crate) fn downgrade_turn_strategy(
        &self,
        candidate: harness_contract::strategy::ExecutionCandidateKind,
        reason: &str,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let understanding = self
            .active_turn_strategy()
            .map(|state| state.decision.strategy.understanding)
            .ok_or_else(|| RuntimeError::new("downgraded turn strategy has no owner"))?;
        let requires_guarded_pattern = understanding.requires_write
            || matches!(
                understanding.risk,
                harness_contract::core::TaskRisk::High | harness_contract::core::TaskRisk::Critical
            );
        let pattern = match candidate {
            harness_contract::strategy::ExecutionCandidateKind::Direct => {
                if requires_guarded_pattern {
                    harness_contract::core::ExecutionPattern::Execute
                } else {
                    harness_contract::core::ExecutionPattern::Direct
                }
            }
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools => {
                if requires_guarded_pattern {
                    harness_contract::core::ExecutionPattern::Execute
                } else {
                    harness_contract::core::ExecutionPattern::Explore
                }
            }
            harness_contract::strategy::ExecutionCandidateKind::Team => {
                harness_contract::core::ExecutionPattern::Collaborate
            }
        };
        self.revise_active_turn_strategy(
            candidate,
            pattern,
            crate::execution_core::TurnStrategyDecisionStatus::Downgraded,
            reason,
            Some("runtime.strategy.downgraded"),
        )?;
        self.active_turn_strategy()
            .ok_or_else(|| RuntimeError::new("downgraded turn strategy disappeared"))
    }

    pub(crate) fn record_turn_strategy_early_stop(&self, reason: &str) -> Result<(), RuntimeError> {
        let active = self
            .active_turn_strategy()
            .ok_or_else(|| RuntimeError::new("early stop has no turn strategy"))?;
        self.revise_active_turn_strategy(
            active.selected_candidate,
            active.decision.pattern(),
            crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped,
            reason,
            Some("runtime.strategy.early_stopped"),
        )?;
        Ok(())
    }

    pub(crate) fn record_turn_strategy_collaboration_receipt(
        &self,
        receipt: serde_json::Value,
    ) -> Result<(), RuntimeError> {
        let mut guard = self
            .active_turn_strategy
            .lock()
            .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
        let state = guard
            .as_mut()
            .ok_or_else(|| RuntimeError::new("collaboration receipt has no turn strategy"))?;
        if state.collaboration_receipt.is_none() {
            state.collaboration_receipt = Some(receipt);
        }
        Ok(())
    }

    pub(crate) fn set_turn_strategy_focus_partitions(
        &self,
        plans: Vec<harness_contract::team::FocusPartitionPlan>,
    ) -> Result<crate::execution_core::TurnStrategyDecisionState, RuntimeError> {
        let mut guard = self
            .active_turn_strategy
            .lock()
            .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
        let state = guard
            .as_mut()
            .ok_or_else(|| RuntimeError::new("focus partitions have no turn strategy owner"))?;
        state.focus_partition_plans = plans;
        Ok(state.clone())
    }

    pub(crate) fn finish_turn_strategy(
        &self,
        turn_ref: &str,
        status: crate::execution_core::TurnStrategyDecisionStatus,
        mut outcome: crate::execution_core::TurnStrategyActualOutcome,
    ) -> Result<(), RuntimeError> {
        let state = {
            let mut guard = self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))?;
            let Some(mut state) = guard.take() else {
                return Ok(());
            };
            if state.turn_ref != turn_ref {
                *guard = Some(state);
                return Err(RuntimeError::new("turn strategy finish scope mismatch"));
            }
            if let Some(receipt) = state.collaboration_receipt.as_ref() {
                let metric = |name: &str| receipt.get(name).and_then(serde_json::Value::as_u64);
                // A process-wide evaluation lease already includes Team
                // children and every fallback request. Adding the receipt a
                // second time inflated projected usage and broke the hard
                // budget equality gate. Production turns have no evaluation
                // lease and still merge child telemetry here.
                if outcome.evaluation_token_limit == 0 {
                    outcome.input_tokens = outcome
                        .input_tokens
                        .saturating_add(metric("child_input_tokens").unwrap_or(0));
                    outcome.output_tokens = outcome
                        .output_tokens
                        .saturating_add(metric("child_output_tokens").unwrap_or(0));
                    outcome.cached_tokens = outcome
                        .cached_tokens
                        .saturating_add(metric("child_cached_tokens").unwrap_or(0));
                }
                outcome.tool_calls = outcome
                    .tool_calls
                    .saturating_add(metric("child_tool_calls").unwrap_or(0));
                outcome.duplicate_tool_calls = outcome
                    .duplicate_tool_calls
                    .saturating_add(metric("duplicate_tool_calls").unwrap_or(0));
                let child_write_attempt_paths = receipt
                    .get("write_attempt_paths")
                    .and_then(serde_json::Value::as_array)
                    .map(|paths| {
                        paths
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                outcome
                    .write_attempt_paths
                    .extend(child_write_attempt_paths);
                outcome.write_attempt_paths.sort();
                outcome.write_attempt_paths.dedup();
                outcome.evidence_overlap_bp = metric("evidence_overlap_bp")
                    .and_then(|value| u16::try_from(value).ok())
                    .unwrap_or(outcome.evidence_overlap_bp);
                outcome.evidence_overlap_observed = receipt
                    .get("evidence_overlap_observed")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(outcome.evidence_overlap_observed);
                outcome.working_state_verified = receipt
                    .get("working_state_verified")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(outcome.working_state_verified);
                outcome.actual_speedup_ratio_bp = metric("actual_speedup_ratio_bp")
                    .and_then(|value| u16::try_from(value).ok())
                    .or(outcome.actual_speedup_ratio_bp);
            }
            state.revision = state.revision.saturating_add(1);
            state.decision.decision_revision = state.revision;
            state.status = status;
            state.outcome = Some(outcome);
            state
        };
        if state.execution_graph_ref.is_some() {
            if let Err(error) = self.append_turn_strategy_event(
                "runtime.strategy.outcome",
                &state,
                "turn terminal owner recorded actual outcome",
            ) {
                *self
                    .active_turn_strategy
                    .lock()
                    .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                    Some(state);
                return Err(error);
            }
        }
        self.record_turn_strategy_experience(&state);
        Ok(())
    }

    fn append_turn_strategy_event(
        &self,
        kind: &'static str,
        state: &crate::execution_core::TurnStrategyDecisionState,
        reason: &str,
    ) -> Result<(), RuntimeError> {
        if !turn_strategy_event_kind_allowed(kind) {
            return Err(RuntimeError::new(format!(
                "unsupported durable turn strategy event kind `{kind}`"
            )));
        }
        let mut refs = vec![
            RuntimeEventRef {
                kind: "strategy_decision".to_string(),
                id: state.decision_id.clone(),
            },
            RuntimeEventRef {
                kind: "strategy_lease".to_string(),
                id: state.decision_lease.clone(),
            },
            RuntimeEventRef {
                kind: "session".to_string(),
                id: state.session_ref.clone(),
            },
            RuntimeEventRef {
                kind: "turn".to_string(),
                id: state.turn_ref.clone(),
            },
        ];
        if let Some(graph_id) = &state.execution_graph_ref {
            refs.push(RuntimeEventRef {
                kind: "execution_graph".to_string(),
                id: graph_id.clone(),
            });
        }
        let store = self
            .runtime_event_store
            .as_ref()
            .ok_or_else(|| RuntimeError::new("turn strategy event store is unavailable"))?;
        store
            .append(RuntimeEventInput {
                stream_id: format!("session:{}", state.session_ref),
                // Turn-level strategy evidence belongs to the Session stream.
                // Treating it as an ExecutionGraph event makes graph discovery
                // attempt to deserialize this non-graph payload as a graph.
                scope: RuntimeEventScope::Session,
                kind: kind.to_string(),
                status: Some(turn_strategy_status_name(state.status).to_string()),
                actor: Some("conversation_runtime.strategy_owner".to_string()),
                refs,
                payload: serde_json::json!({
                    "decision_id": state.decision_id,
                    "decision_lease": state.decision_lease,
                    "decision_revision": state.revision,
                    "policy_version": state.policy_version,
                    "decision_source": state.decision.strategy.source,
                    "confidence": state.decision.strategy.confidence,
                    "selected_candidate": state.selected_candidate,
                    "selected_pattern": state.decision.pattern().as_str(),
                    "candidate_estimates": state.decision.strategy.candidate_estimates,
                    "selection_reasons": state.decision.strategy.reasons,
                    "resource_snapshot": state.resource_snapshot,
                    "execution_graph_ref": state.execution_graph_ref,
                    "session_ref": state.session_ref,
                    "turn_ref": state.turn_ref,
                    "status": state.status,
                    "reason": reason,
                    "collaboration_receipt": state.collaboration_receipt,
                    "evidence_scopes": state.focus_partition_plans,
                    "outcome": state.outcome,
                }),
            })
            .map(|_| ())
            .map_err(|error| {
                RuntimeError::new(format!(
                    "durable turn strategy event `{kind}` append failed: {error}"
                ))
            })
    }

    fn record_turn_strategy_experience(
        &self,
        state: &crate::execution_core::TurnStrategyDecisionState,
    ) {
        if state.resource_snapshot.sample_source.contains("corpus=") {
            return;
        }
        let Some(outcome) = state.outcome.as_ref() else {
            return;
        };
        let _io_guard = STRATEGY_EXPERIENCE_IO_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let understanding = &state.decision.strategy.understanding;
        let path = strategy_experience_path();
        let mut store = StrategyExperienceStore::load_or_default(path.clone());
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_pattern: state.decision.pattern(),
            selected_candidate: Some(state.selected_candidate),
            succeeded: state.status == crate::execution_core::TurnStrategyDecisionStatus::Completed,
            verification_blocked: outcome.quality_score_bp == Some(0),
            context_pressure: false,
            composite_execution: state.selected_candidate
                != harness_contract::strategy::ExecutionCandidateKind::Team
                && state.collaboration_receipt.is_some(),
            // One live turn has no paired counterfactual. Keep absolute cost
            // telemetry, but never infer causal lift from graph shape.
            multi_agent_positive_lift: false,
            created_at_ms: now_ms(),
            actual_duration_ms: outcome.duration_ms,
            actual_input_tokens: outcome.input_tokens,
            actual_output_tokens: outcome.output_tokens,
            actual_cached_tokens: outcome.cached_tokens,
            actual_coordination_cost_ms: outcome.merge_cost_ms,
            paired_calibration: None,
        });
        if let Err(error) = store.save(path) {
            tracing::warn!(%error, "failed to persist AI strategy experience");
        }
    }

    fn append_execution_runtime_event(
        &self,
        scope: RuntimeEventScope,
        kind: &'static str,
        status: Option<String>,
        refs: Vec<RuntimeEventRef>,
        payload: serde_json::Value,
    ) {
        let Some(store) = self.runtime_event_store.as_ref() else {
            return;
        };
        let session_id = self.session().session_id;
        if let Err(error) = store.append(RuntimeEventInput {
            stream_id: format!("session:{session_id}"),
            scope,
            kind: kind.to_string(),
            status,
            actor: Some("conversation_runtime".to_string()),
            refs,
            payload,
        }) {
            tracing::warn!(%error, session_id, event_kind = kind, "execution runtime event append failed");
        }
    }

    fn dual_write_message(&self, msg: &crate::session::ConversationMessage, sequence: usize) {
        // Record the message in the event log for time-travel debugging.
        if let Some(ref log) = self.event_log {
            if let Ok(mut guard) = log.lock() {
                guard.push(MessageEvent::MessageAppended {
                    message: msg.clone(),
                });
            }
        }
        if !self.transcript_persistence {
            return;
        }
        if let Some(ref store) = self.session_store {
            let session_id = self.session().session_id;
            let record = msg.to_session_message(&session_id, sequence);
            let event =
                message_appended_session_event(msg, &session_id, sequence, record.created_at_ms);
            let store = Arc::clone(store);
            tokio::spawn(async move {
                if let Err(e) = store.insert_message(&record).await {
                    tracing::warn!(%e, session_id, sequence, "dual_write: SQLite insert failed, retrying");
                    if let Err(retry_error) = store.insert_message(&record).await {
                        tracing::warn!(%retry_error, session_id, sequence, "dual_write: SQLite retry failed");
                        return;
                    }
                }
                if let Err(e) = store.append_event_allocating_sequence(&event).await {
                    tracing::warn!(%e, session_id, sequence, "dual_write: session event append failed");
                }
            });
        }
    }

    fn record_runtime_policy_decision(
        &self,
        decision: &crate::execution_core::RuntimeExecutionDecision,
        sequence: usize,
    ) {
        let requires_review = decision.modifiers().iter().any(|modifier| {
            matches!(
                modifier,
                harness_contract::core::ExecutionModifier::WithVerifier
                    | harness_contract::core::ExecutionModifier::WithReviewer
            )
        }) || decision
            .gates()
            .contains(&harness_contract::core::ExecutionPolicyGate::Approval);
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::RuntimePolicyDecision {
                summary: crate::cowd_event::RuntimePolicyDecisionSummary {
                    level: format!("{:?}", decision.complexity()),
                    score: (decision.confidence * 100.0).round() as u16,
                    recommended_profile: format!("{:?}", self.context_profile()),
                    agent_mode: decision.pattern().as_str().to_string(),
                    requires_review,
                    signal_count: decision.reasons.len(),
                },
            });
        }

        let Some(ref store) = self.session_store else {
            return;
        };
        let session_id = self.session().session_id;
        let payload = serde_json::json!({
            "decision_id": decision.decision_id,
            "pattern": decision.pattern(),
            "complexity": decision.complexity(),
            "risk": decision.risk(),
            "confidence": decision.confidence,
            "modifiers": decision.modifiers(),
            "gates": decision.gates(),
            "collaboration_lift": decision.collaboration_lift(),
            "compile_target": decision.compile_target,
            "strategy_lease": decision.lease,
            "decision_source": decision.strategy.source,
            "requires_review": requires_review,
            "reasons": decision.reasons,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let mut event = memory::SessionDomainEvent::new(
            session_id.clone(),
            sequence,
            memory::SessionDomainScope::Policy,
            "runtime.policy.decided",
            payload,
            created_at_ms,
        );
        event.status = Some("completed".to_string());
        let store = Arc::clone(store);
        tokio::spawn(async move {
            if let Err(error) = store.append_session_domain_event(&event).await {
                tracing::warn!(%error, session_id, sequence, "runtime policy domain event append failed");
            }
        });
    }
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

fn message_appended_session_event(
    msg: &crate::session::ConversationMessage,
    session_id: &str,
    sequence: usize,
    created_at_ms: u64,
) -> memory::SessionEvent {
    let message = serde_json::from_str::<serde_json::Value>(&msg.to_json().render())
        .unwrap_or(serde_json::Value::Null);
    memory::SessionEvent {
        session_id: session_id.to_string(),
        event_type: "message_appended".to_string(),
        event_json: serde_json::json!({
            "type": "message_appended",
            "sequence": sequence,
            "role": msg.role.role_str(),
            "message": message,
        })
        .to_string(),
        sequence,
        created_at_ms,
    }
}

/// Reads the automatic compaction threshold from the environment.
#[must_use]
/// Convert a [`RuntimeFeatureConfig`] memory section into a [`CcMemoryConfig`]
/// suitable for [`CognitiveContextManager::new`].
#[doc(alias = "memory")]
#[doc(alias = "CognitiveContextManager")]
pub fn build_cc_memory_config(feature_config: &RuntimeFeatureConfig) -> CcMemoryConfig {
    let model_context_window = feature_config.model().map_or(0, |model| {
        provider::model_context_window_with_overrides(
            model,
            Some(feature_config.model_context_windows()),
        )
    });
    let model_max_output = feature_config.model().map_or(0, |model| {
        bounded_provider_output_tokens(model, model_context_window)
    });
    let ratio_bp =
        clamp_context_budget_ratio_bp(feature_config.context_budget().subsystem_budget_ratio_bp);
    let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
        model_context_window,
        model_max_output_tokens: model_max_output,
        subsystem_budget_ratio_bp: ratio_bp,
        profile: ContextProfile::MainTurn,
        autonomy_mode: None,
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
    let storage_layout =
        storage::StorageLayout::default_for_config_home(crate::cowd_dirs::config_home_dir());
    let (sqlite_path, blob_dir) = if let Some(store_path) = mem.store_path.as_ref() {
        (store_path.join("memory.db"), store_path.join("blobs"))
    } else {
        (
            storage_layout
                .sqlite_path("memory")
                .map(std::path::Path::to_path_buf)
                .unwrap_or_else(|| {
                    crate::cowd_dirs::config_home_dir().join("storage/memory.sqlite")
                }),
            storage_layout.blobs.join("memory"),
        )
    };

    CcMemoryConfig {
        store: StoreConfig {
            sqlite_path,
            blob_dir,
            enable_vector_index: mem.store_enable_vector_index && mem.vector.enabled,
            cache_capacity: 512,
            vector: memory::config::VectorConfig {
                enabled: mem.vector.enabled,
                model: mem.vector.model.clone(),
                api_url: mem.vector.api_url.clone(),
                api_key: mem.vector.api_key.clone(),
                dimension: mem.vector.dimension,
                timeout_secs: mem.vector.timeout_secs,
                batch_size: mem.vector.batch_size,
            },
        },
        compression: CompressionConfig {
            micro_threshold: 50,
            session_threshold: 10,
            enable_deep_compression: feature_config.compression().deep.enabled,
            aggressiveness: 0.5,
            llm: Default::default(),
        },
        budget: BudgetConfig {
            context_window: budget_plan.memory_retrieval_budget.context_window,
            reserved_system: u64::from(mem.layers.l1_max_tokens)
                + u64::from(mem.layers.l2_max_tokens),
            reserved_response: budget_plan.memory_retrieval_budget.reserved_response,
            warning_threshold: 0.70,
            critical_threshold: 0.90,
            runtime_managed: mem.runtime.use_runtime_budget,
            selected_item_limit: budget_plan.memory_retrieval_budget.selected_item_limit,
            l0_reserved: budget_plan.memory_retrieval_budget.l0_reserved,
            l1_working: budget_plan.memory_retrieval_budget.l1_working,
            l2_project: budget_plan.memory_retrieval_budget.l2_project,
            l3_deep: budget_plan.memory_retrieval_budget.l3_deep,
            l3_checkpoint: budget_plan.memory_retrieval_budget.l3_checkpoint,
            l4_shared: budget_plan.memory_retrieval_budget.l4_shared,
        },
        extractor: ExtractorConfig {
            poll_interval_secs: 30,
            batch_size: 20,
            min_confidence: 0.6,
            extractor_debounce_secs: 30,
        },
        drift: DriftConfig::default(),
        perf: memory::config::PerfBudget::default(),
        tuning: Default::default(),
        model: None,
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
            EvidenceRef(
                KernelRef::new("session-message", format!("{session_id}:{index}"))
                    .with_label(message_index_label(message)),
            )
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
                .map(|block| match block {
                    ContentBlock::Text { text } => text.clone(),
                    ContentBlock::Image {
                        media_type,
                        source_path,
                        ..
                    } => format!(
                        "[image media_type={} source_path={}]",
                        media_type,
                        source_path.as_deref().unwrap_or("<inline>")
                    ),
                    ContentBlock::Thinking { thinking, .. } => format!("[thinking]\n{thinking}"),
                    ContentBlock::ToolUse { id, name, input } => {
                        format!("[tool_use id={id} name={name}]\n{input}")
                    }
                    ContentBlock::ToolResult {
                        tool_use_id,
                        tool_name,
                        output,
                        is_error,
                    } => format!(
                        "[tool_result id={tool_use_id} name={tool_name} error={is_error}]\n{output}"
                    ),
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
    let root = session
        .workspace_root
        .clone()
        .or_else(|| std::env::current_dir().ok())?;
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
    Some(format!("{name}-{hash:016x}"))
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

fn bounded_tool_concurrency(max_concurrency: usize, item_count: usize) -> usize {
    if item_count == 0 {
        return 1;
    }
    max_concurrency
        .max(1)
        .min(item_count)
        .min(crate::execution_scheduler::MAX_PARALLEL_READ_CONCURRENCY)
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

fn strategy_experience_path() -> std::path::PathBuf {
    crate::cowd_dirs::config_home_dir()
        .join("ai")
        .join("strategy-experience.json")
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
            input.resource_snapshot.assumed = false;
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
        "calibration_status": "assumed_structural_only",
        "persisted_for_routing": false,
        "store_ref": strategy_experience_path().display().to_string(),
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

type ToolHandler = Box<dyn Fn(&str) -> Result<String, ToolError> + Send + Sync>;

/// Simple in-memory tool executor for tests and lightweight integrations.
#[derive(Default)]
pub struct StaticToolExecutor {
    handlers: BTreeMap<String, ToolHandler>,
}

impl StaticToolExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register(
        mut self,
        tool_name: impl Into<String>,
        handler: impl Fn(&str) -> Result<String, ToolError> + Send + Sync + 'static,
    ) -> Self {
        self.handlers.insert(tool_name.into(), Box::new(handler));
        self
    }
}

impl ToolExecutor for StaticToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        self.handlers
            .get(tool_name)
            .ok_or_else(|| ToolError::new(format!("unknown tool: {tool_name}")))?(input)
    }

    fn describe_tool_effect(
        &self,
        tool_name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        };

        self.handlers.contains_key(tool_name).then(|| {
            let safety = crate::tool_orchestrator::ToolSafetyCategory::from_tool_name(tool_name);
            let (effect_kind, required_permission, scope, approval_class) = match safety {
                crate::tool_orchestrator::ToolSafetyCategory::ReadOnly => (
                    ToolEffectKind::Read,
                    ToolPermissionMode::ReadOnly,
                    PermissionScope::new(PermissionResource::File, PermissionOperation::Read),
                    ToolApprovalClass::None,
                ),
                crate::tool_orchestrator::ToolSafetyCategory::WriteLocal => (
                    ToolEffectKind::Write,
                    ToolPermissionMode::WorkspaceWrite,
                    PermissionScope::new(PermissionResource::File, PermissionOperation::Write),
                    ToolApprovalClass::Policy,
                ),
                crate::tool_orchestrator::ToolSafetyCategory::Network => (
                    ToolEffectKind::Network,
                    ToolPermissionMode::DangerFullAccess,
                    PermissionScope::new(PermissionResource::Network, PermissionOperation::Execute),
                    ToolApprovalClass::Policy,
                ),
                crate::tool_orchestrator::ToolSafetyCategory::Destructive => (
                    ToolEffectKind::Destructive,
                    ToolPermissionMode::DangerFullAccess,
                    PermissionScope::new(PermissionResource::Tool, PermissionOperation::Execute),
                    ToolApprovalClass::User,
                ),
            };
            ToolEffectDescriptor {
                tool_id: tool_name.to_string(),
                descriptor_hash: format!("static:{tool_name}:{effect_kind:?}"),
                effect_kind,
                idempotency: ToolIdempotency::Unknown,
                scopes: vec![scope],
                required_permission,
                approval_class,
                uses_network: matches!(
                    safety,
                    crate::tool_orchestrator::ToolSafetyCategory::Network
                ),
                spawns_process: false,
                mutates_packages: false,
                mutates_system: matches!(
                    safety,
                    crate::tool_orchestrator::ToolSafetyCategory::Destructive
                ),
            }
        })
    }

    fn execute_authorized(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<String, ToolError> {
        if authorization.tool_id != tool_name {
            return Err(ToolError::new(
                "static tool authorization names a different tool",
            ));
        }
        self.execute(tool_name, input)
    }

    fn has_registered_tools(&self) -> bool {
        !self.handlers.is_empty()
    }

    fn available_tool_names(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {

    use super::{
        ApiClient, ApiRequest, AssistantEvent, CognitiveContextManager, ConversationRuntime,
        ModelStepIntent, ModelToolCall, RuntimeError, StaticToolExecutor, ToolExposureState,
        apply_explicit_team_requirement, apply_named_e2e_strategy_fixture,
        build_cc_memory_config_with_budget, deterministic_checkpoint_id,
        enforce_explicit_team_requirement, eval_override_selection, image_user_message_from_path,
        is_runtime_team_orchestration_call, memory_project_id_for_session,
        model_team_request_conflicts_with_admission, prepared_vision_payload, preview_chars,
        provider_transport_policy, rate_per_second, required_team_orchestration_call,
        tool_batch_pattern, turn_strategy_event_kind_allowed, vision_user_message,
    };
    use crate::config::RuntimeFeatureConfig;
    use crate::context_runtime::{
        ContextAuthority, ContextItem, ContextMode, ContextProfile, ContextRole, ContextSourceKind,
        ResumeContextPacket, ResumeContextSource,
    };
    use crate::execution_core::build_runtime_execution_decision;
    use crate::permissions::{PermissionMode, PermissionPolicy};
    use crate::runtime_event_store::{RuntimeEventScope, RuntimeEventStore};
    use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};
    use crate::{
        COWD_IDENTITY_CONTRACT_VERSION, PromptAssembly, RealityRecallPort, RuntimeBudgetInputs,
        RuntimeBudgetPlan, SystemPromptBuilder, resolve_context_budget_tokens,
    };
    use futures::{StreamExt, stream::Stream};
    use harness_contract::agent::{
        AgentBindingSnapshot, AgentCapability, AgentDataLease, AgentDefinitionId,
        AgentDefinitionRevisionRef, AgentExecutorPolicy, AgentInstanceRef, AgentModelPolicy,
        CognitiveReadScope, CognitiveWriteMode, DefinitionScope,
    };
    use harness_contract::skill::{
        AgentSkillProfile, SkillAdapterKind, SkillCapabilityProfile, SkillDetectedRuntime,
        SkillEntrypoint, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
    };
    use harness_contract::team::{FocusPartitionPlan, FocusPartitionSlot};
    use model_protocol::usage::TokenUsage;
    use std::fs;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn small_exact_tool_result_retains_content_after_receipt_envelope() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let output = serde_json::json!({
            "type": "text",
            "path": "fixtures/target.txt",
            "content": "implemented-v546-0\n",
            "totalLines": 1,
            "truncated": false,
        })
        .to_string();
        let receipt = runtime.tool_model_receipt(
            "read_file",
            &output,
            false,
            &harness_contract::core::EvidenceRef::new("tool", "small-exact-read"),
            None,
        );

        assert!(!receipt.truncated, "{}", receipt.summary);
        assert!(receipt.summary.contains("implemented-v546-0"));
        assert!(!receipt.summary.contains("omitted; retrieve"));
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    use storage::{SqliteConnectionFactory, StorageRegistry};

    fn rendered_prompt(prompt: &PromptAssembly) -> String {
        let mut segments = prompt.trusted_system.clone();
        segments.extend(prompt.contextual_messages());
        segments.join("\n")
    }

    #[test]
    fn direct_eval_candidate_keeps_policy_complete_graph() {
        use harness_contract::core::ExecutionPattern;
        use harness_contract::strategy::ExecutionCandidateKind;

        let judge = eval_override_selection(
            "direct",
            false,
            "runtime-execution-resource-manager:corpus=auto-strategy-v1:provider_constraint=judge:temperature_milli=0",
        )
        .expect("judge override")
        .expect("judge selection");
        assert_eq!(
            judge,
            (ExecutionCandidateKind::Direct, ExecutionPattern::Execute)
        );

        let business = eval_override_selection(
            "direct",
            false,
            "runtime-execution-resource-manager:corpus=auto-strategy-v1:provider_constraint=business",
        )
        .expect("business override")
        .expect("business selection");
        assert_eq!(
            business,
            (ExecutionCandidateKind::Direct, ExecutionPattern::Execute)
        );

        let parallel = eval_override_selection(
            "parallel_tools",
            false,
            "runtime-execution-resource-manager:corpus=auto-strategy-v1:provider_constraint=business",
        )
        .expect("parallel override")
        .expect("parallel selection");
        assert_eq!(
            parallel,
            (
                ExecutionCandidateKind::ParallelTools,
                ExecutionPattern::Execute
            )
        );
    }

    #[test]
    fn explicit_collaboration_requirement_becomes_a_runtime_team_tool_call() {
        let objective = "这是复杂架构审查，必须实际启动一个多 Agent 协作团队完成分析。";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "我会开始分析。".to_string(),
            },
        );
        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("explicit team requirement must materialize an orchestration call");
        };
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "runtime_orchestrate");
        let input: serde_json::Value = serde_json::from_str(&calls[0].input).unwrap();
        assert_eq!(input["action"], "request_team");
    }

    #[test]
    fn e2e_negative_team_fixture_is_marker_scoped_and_produces_real_cost_warning() {
        let prompt =
            "must start a Team for runtime gateway frontend [cowd-e2e:explicit-team-negative]";
        let mut input = harness_contract::strategy::StrategyInput::from_prompt(prompt);
        apply_named_e2e_strategy_fixture(&mut input, prompt, "explicit-team-negative")
            .expect("known fixture is accepted");
        let decision = harness_contract::strategy::decide_strategy(&input);
        let team = decision
            .candidate_estimates
            .iter()
            .find(|estimate| {
                estimate.candidate == harness_contract::strategy::ExecutionCandidateKind::Team
            })
            .expect("fixture retains Team estimate");

        assert_eq!(
            decision.selected_candidate,
            harness_contract::strategy::ExecutionCandidateKind::Team
        );
        assert!(team.net_benefit_score < 0);
        assert!(
            decision
                .reasons
                .iter()
                .any(|reason| reason.contains("negative estimated lift"))
        );

        let mut unmarked = harness_contract::strategy::StrategyInput::from_prompt(
            "must start a Team for runtime gateway frontend",
        );
        let unmarked_prompt = unmarked.prompt.clone();
        apply_named_e2e_strategy_fixture(&mut unmarked, &unmarked_prompt, "explicit-team-negative")
            .expect("known fixture is inert without its marker");
        assert!(unmarked.candidate_costs.is_empty());
    }

    #[test]
    fn provider_cannot_retarget_a_non_team_admission_into_an_unowned_team() {
        let call = required_team_orchestration_call("review");
        assert!(model_team_request_conflicts_with_admission(
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            std::slice::from_ref(&call),
        ));
        assert!(!model_team_request_conflicts_with_admission(
            harness_contract::strategy::ExecutionCandidateKind::Team,
            &[call],
        ));
    }

    #[test]
    fn explicit_team_requirement_overrides_a_non_collaboration_strategy_hint() {
        let objective = "先自主选择并实际启动合适的协作团队，分别完成三个独立审查。";
        let decision = build_runtime_execution_decision(objective, None);

        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "我会开始分析。".to_string(),
            },
        );

        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("an explicit team requirement must override the heuristic");
        };
        assert!(calls.iter().any(is_runtime_team_orchestration_call));
    }

    #[test]
    fn explicit_team_requirement_cannot_be_bypassed_by_agent_named_tool_calls() {
        let objective = "必须实际启动协作团队，再分析这些模块。";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::AgentProposal {
                calls: vec![ModelToolCall {
                    id: "provider-agent-helper".to_string(),
                    name: "agent_helper".to_string(),
                    input: "{}".to_string(),
                    depends_on: Vec::new(),
                }],
            },
        );

        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("provider-specific agent proposals must enter the canonical tool batch");
        };
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(is_runtime_team_orchestration_call));
    }

    #[test]
    fn team_orchestration_tool_batch_keeps_the_collaboration_strategy() {
        let calls = vec![required_team_orchestration_call("必须实际启动团队")];
        assert_eq!(
            tool_batch_pattern(&calls),
            harness_contract::core::ExecutionPattern::Collaborate
        );
    }

    #[test]
    fn required_team_orchestration_uses_a_published_builtin_template() {
        let call = required_team_orchestration_call("必须实际启动团队");
        assert_eq!(call.name, "runtime_orchestrate");
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        assert_eq!(
            input["template_hint"],
            serde_json::json!("cowd/parallel-research-synthesis")
        );
    }

    #[test]
    fn ordinary_complex_work_keeps_model_directed_team_choice() {
        let objective = "分析 runtime、memory 和 gateway 的边界。";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "普通复杂分析。".to_string(),
            },
        );
        assert!(matches!(intent, ModelStepIntent::FinalAnswer { .. }));
    }

    #[test]
    fn explicit_team_requirement_recognizes_negative_start_constraint() {
        assert!(!super::explicit_team_execution_required(
            "请单人完成审查，不要启动团队。"
        ));
    }

    #[test]
    fn delegated_leaf_turn_does_not_force_a_second_team_from_inherited_wording() {
        let objective = "必须实际启动协作团队，再分析这些模块。";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = apply_explicit_team_requirement(
            false,
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "leaf evidence".to_string(),
            },
        );
        assert!(matches!(intent, ModelStepIntent::FinalAnswer { .. }));
    }

    #[test]
    fn prepared_vision_payload_becomes_user_image_message() {
        let output = serde_json::json!({
            "tool": "vision_analyze",
            "status": "prepared",
            "image_path": "/tmp/cowd-test.png",
            "media_type": "image/png",
            "prompt": "describe it",
            "image_base64": "aW1hZ2U=",
            "size_bytes": 5
        })
        .to_string();

        let payload = prepared_vision_payload("vision_analyze", &output, false)
            .expect("prepared vision payload should parse");
        let message = vision_user_message(&payload);

        assert_eq!(message.role, MessageRole::User);
        assert!(matches!(
            message.blocks.get(1),
            Some(ContentBlock::Image {
                media_type,
                data,
                source_path
            }) if media_type == "image/png"
                && data == "aW1hZ2U="
                && source_path.as_deref() == Some("/tmp/cowd-test.png")
        ));
    }

    #[test]
    fn image_user_message_from_path_reads_image_as_structured_block() {
        let path = std::env::temp_dir().join(format!(
            "cowd-runtime-image-message-{}.jpg",
            uuid::Uuid::new_v4()
        ));
        fs::write(&path, b"fake-jpeg-bytes").expect("test image should write");

        let message = image_user_message_from_path(&path, "image/jpeg", "describe it")
            .expect("image message should be prepared");

        assert_eq!(message.role, MessageRole::User);
        assert!(message.blocks.iter().any(|block| {
            matches!(block, ContentBlock::Image { media_type, data, source_path }
                if media_type == "image/jpeg"
                    && data == "ZmFrZS1qcGVnLWJ5dGVz"
                    && source_path.as_deref() == Some(path.to_string_lossy().as_ref()))
        }));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn provider_transport_policy_scales_with_actual_request_size() {
        let small = ApiRequest {
            prompt: PromptAssembly::new(vec!["system".to_string()]),
            messages: vec![ConversationMessage::user_text("status".to_string())],
            model: "test".to_string(),
            reasoning_effort_override: None,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 32_768, 4_096, 128, 256, 0,
            ),
        };
        let large = ApiRequest {
            prompt: PromptAssembly::new(vec!["system".repeat(5_000)]),
            messages: vec![ConversationMessage::user_text("evidence".repeat(10_000))],
            model: "test".to_string(),
            reasoning_effort_override: None,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 1_000_000, 32_000, 128, 256, 0,
            ),
        };

        assert!(
            provider_transport_policy(1_000_000, &large).idle_timeout
                > provider_transport_policy(32_768, &small).idle_timeout
        );
    }

    #[test]
    fn candidate_packer_accounts_for_history_schema_and_omits_packet_tail() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["builtin policy".to_string()],
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime.set_active_model("test");
        let mut prompt = PromptAssembly::new(vec!["runtime control".repeat(60)]);
        for source_id in (0..64).map(|index| format!("packet-{index}")) {
            prompt.contextual_packets.push(crate::PromptContextPacket {
                authority: ContextAuthority::Project,
                source: ContextSourceKind::Workspace,
                role: ContextRole::Evidence,
                source_id,
                content: "evidence ".repeat(900),
                evidence: Vec::new(),
                utility_score_milli: 0,
            });
        }

        let request = runtime
            .pack_provider_attempt(
                &prompt,
                &[ConversationMessage::user_text("history ".repeat(300))],
                "test",
                super::ProviderContextInventory {
                    tool_count: 2,
                    tool_schema_tokens: 1_200,
                },
            )
            .expect("candidate request should fit after contextual packing");

        assert!(request.budget.input_total_tokens() <= request.budget.hard_input_cap_tokens);
        assert_eq!(
            request.budget.target_input_cap_tokens,
            request.budget.hard_input_cap_tokens
        );
        assert!(!request.prompt.contextual_packets.is_empty());
        assert!(!request.budget.omitted_packet_ids.is_empty());
    }

    #[derive(Clone)]
    struct RouteRecordingApi {
        requests: Arc<std::sync::Mutex<Vec<ApiRequest>>>,
    }

    impl ApiClient for RouteRecordingApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let model = request.model.clone();
            self.requests.lock().expect("requests").push(request);
            let events = if model == "primary" {
                vec![Err(RuntimeError::new("primary unavailable"))]
            } else {
                vec![
                    Ok(AssistantEvent::ProviderModel { model }),
                    Ok(AssistantEvent::TextDelta("fallback answer".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    #[derive(Clone)]
    struct CapacityRecordingApi {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    impl ApiClient for CapacityRecordingApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let active_now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active_now, Ordering::SeqCst);
            let active = Arc::clone(&self.active);
            Box::pin(
                futures::stream::once(async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(AssistantEvent::TextDelta("capacity answer".to_string()))
                })
                .chain(futures::stream::iter([Ok(AssistantEvent::MessageStop)])),
            )
        }
    }

    #[tokio::test]
    async fn ordinary_conversations_share_one_provider_admission_owner() {
        use crate::execution_core::graph::{
            ExecutionResourceKind, ExecutionResourceManager, ResourceQuota,
        };

        let manager = Arc::new(ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )]));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let runtime = |turn: &str| {
            let runtime = ConversationRuntime::new(
                Session::new(),
                CapacityRecordingApi {
                    active: Arc::clone(&active),
                    max_active: Arc::clone(&max_active),
                },
                StaticToolExecutor::new(),
                PermissionPolicy::new(PermissionMode::WorkspaceWrite),
                SystemPromptBuilder::new().build(),
            )
            .without_memory()
            .with_model_context_window(128_000)
            .with_provider_admission(Arc::clone(&manager));
            runtime
                .begin_turn_strategy(turn, "answer with current evidence")
                .unwrap();
            runtime
        };
        let mut first = runtime("provider-capacity-1");
        let mut second = runtime("provider-capacity-2");
        let (first, second) = tokio::join!(
            first.execute_model_step("answer with current evidence", true),
            second.execute_model_step("answer with current evidence", true),
        );
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
        let snapshot = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
        assert_eq!(snapshot.active_leases, 0);
        assert_eq!(snapshot.queued_waiters, 0);
    }

    #[tokio::test]
    async fn runtime_owns_fallback_attempts_and_repacks_each_candidate() {
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = RouteRecordingApi {
            requests: Arc::clone(&requests),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            SystemPromptBuilder::new().build(),
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime.set_active_model("primary");
        runtime.fallbacks = vec!["fallback".to_string()];
        runtime
            .begin_turn_strategy("test-fallback-turn", "summarize the current state")
            .expect("test turn strategy admission");

        let result = runtime
            .execute_model_step("summarize the current state", true)
            .await
            .expect("fallback candidate should complete");
        assert_eq!(result.model.as_deref(), Some("fallback"));
        let requests = requests.lock().expect("requests");
        assert_eq!(
            requests
                .iter()
                .map(|request| request.model.as_str())
                .collect::<Vec<_>>(),
            vec!["primary", "fallback"]
        );
        assert!(requests.iter().all(|request| request.budget.executable));
        assert!(requests.iter().all(|request| {
            request.prompt.trusted_system.first().is_some_and(|head| {
                head.contains("You are Cowd") && head.contains(COWD_IDENTITY_CONTRACT_VERSION)
            })
        }));
    }

    #[tokio::test]
    async fn one_shot_reasoning_effort_is_request_local_and_survives_fallback() {
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = RouteRecordingApi {
            requests: Arc::clone(&requests),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            SystemPromptBuilder::new().build(),
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime.set_active_model("primary");
        runtime.fallbacks = vec!["fallback".to_string()];
        runtime
            .begin_turn_strategy("test-reasoning-turn", "reduce verified receipts")
            .expect("test turn strategy admission");
        runtime.require_next_model_reasoning_effort("none");

        runtime
            .execute_model_step("reduce verified receipts", true)
            .await
            .expect("fallback candidate should complete");

        let requests = requests.lock().expect("requests");
        assert_eq!(requests.len(), 2);
        assert!(
            requests
                .iter()
                .all(|request| { request.reasoning_effort_override.as_deref() == Some("none") })
        );
        assert!(
            runtime
                .next_model_reasoning_effort
                .lock()
                .expect("reasoning effort")
                .is_none()
        );
    }

    #[derive(Clone)]
    struct CalibrationRecordingApi {
        windows: Arc<std::sync::Mutex<Vec<(u64, String)>>>,
    }

    impl ApiClient for CalibrationRecordingApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let attempt = {
                let mut windows = self.windows.lock().expect("windows");
                windows.push((
                    request.budget.context_window_tokens,
                    request.budget.context_window_source.clone(),
                ));
                windows.len()
            };
            let events = if attempt == 1 {
                vec![Err(RuntimeError::with_provider_context_window_limit(
                    "provider maximum context length is 32768 tokens",
                    Some(32_768),
                ))]
            } else {
                vec![
                    Ok(AssistantEvent::TextDelta("calibrated answer".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    #[tokio::test]
    async fn explicit_provider_limit_calibrates_once_and_repackages_the_same_model() {
        let windows = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = CalibrationRecordingApi {
            windows: Arc::clone(&windows),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["builtin policy".to_string()],
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime.set_active_model("private-model");
        runtime
            .begin_turn_strategy("test-calibration-turn", "give a concise answer")
            .expect("test turn strategy admission");

        let result = runtime
            .execute_model_step("give a concise answer", true)
            .await
            .expect("calibrated retry should complete");
        assert_eq!(result.model.as_deref(), Some("private-model"));
        let windows = windows.lock().expect("windows");
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].0, 128_000);
        assert_eq!(windows[1].0, 32_768);
        assert_eq!(windows[1].1, "calibrated");
    }

    #[test]
    fn switching_models_re_resolves_each_configured_context_window() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["builtin policy".to_string()],
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime
            .model_context_windows
            .insert("small-configured-model".to_string(), 32_768);

        runtime.set_active_model("small-configured-model");

        let resolution = runtime.context_window_resolution_for_model("small-configured-model");
        assert_eq!(resolution.tokens, 32_768);
        assert_eq!(resolution.source.as_str(), "configured");
    }

    #[test]
    fn semantic_checkpoint_id_is_stable_and_boundary_specific() {
        let first = deterministic_checkpoint_id("session-a", 2, 8, Some("prior"));
        let retry = deterministic_checkpoint_id("session-a", 2, 8, Some("prior"));
        let different_boundary = deterministic_checkpoint_id("session-a", 3, 8, Some("prior"));

        assert_eq!(first, retry);
        assert_ne!(first, different_boundary);
        assert!(first.starts_with("checkpoint-session-a-"));
    }

    #[tokio::test]
    async fn manual_compaction_uses_one_semantic_checkpoint_and_preserves_recent_turns() {
        let tmp = tempfile::tempdir().expect("temp memory root");
        let manager = Arc::new(
            CognitiveContextManager::new(memory::config::MemoryConfig {
                store: memory::config::StoreConfig {
                    sqlite_path: tmp.path().join("memory.sqlite"),
                    blob_dir: tmp.path().join("blobs"),
                    enable_vector_index: false,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("memory manager"),
        );
        let mut session = Session::new();
        session.messages = vec![
            ConversationMessage::user_text("old request ".repeat(200)),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old response ".repeat(200),
            }]),
            ConversationMessage::user_text("recent user request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "recent assistant response".to_string(),
            }]),
        ];
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().expect("session store"));
        store
            .create_session(&memory::SessionRecord {
                session_id: session.session_id.clone(),
                platform: "test".to_string(),
                chat_id: "semantic-compaction".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .expect("session record");
        let mut runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_memory_manager(manager)
        .with_session_store(store);
        runtime.session_compaction_config.preserve_recent = 2;

        let receipt = runtime
            .compact_active_session()
            .await
            .expect("semantic compaction")
            .expect("a compaction receipt");
        assert!(receipt.removed_message_count > 0);
        let compacted = runtime.session_async().await;
        assert_eq!(
            compacted.messages.len(),
            3,
            "configured preserve_recent=2 must win"
        );
        assert!(matches!(
            &compacted.messages[0].blocks[0],
            ContentBlock::Text { text }
                if text.contains("Compressed Session Summary")
                    && !text.contains("Conversation summary:")
        ));
        assert!(compacted.messages.iter().any(|message| {
            message.blocks.iter().any(|block| {
                matches!(
                    block,
                    ContentBlock::Text { text } if text == "recent user request"
                )
            })
        }));
    }

    #[tokio::test]
    async fn compaction_without_durable_session_store_retains_the_transcript() {
        let mut session = Session::new();
        session.messages = vec![
            ConversationMessage::user_text("old request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old response".to_string(),
            }]),
            ConversationMessage::user_text("recent request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "recent response".to_string(),
            }]),
        ];
        let before = session.clone();
        let mut runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        let error = runtime
            .compact_active_session()
            .await
            .expect_err("a non-durable runtime must not compact history");

        assert!(error.to_string().contains("durable UnifiedSessionStore"));
        assert_eq!(runtime.session_async().await, before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn synchronous_session_accessor_works_from_current_thread_runtime_when_contended() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let expected_session_id = runtime.session_async().await.session_id;
        let lock = Arc::clone(&runtime.session);
        let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
        let holder = std::thread::spawn(move || {
            let _guard = lock.blocking_write();
            let _ = locked_tx.send(());
            std::thread::sleep(std::time::Duration::from_millis(20));
        });
        locked_rx
            .recv()
            .expect("native holder must acquire the session lock before the compatibility read");

        let session = runtime.session();

        holder
            .join()
            .expect("native session-lock holder must finish");
        assert_eq!(session.session_id, expected_session_id);
    }

    #[test]
    fn runtime_decision_keeps_all_six_patterns_stable_for_same_input() {
        use harness_contract::core::ExecutionPattern;

        let cases = [
            ("解释一下这个函数有什么用", ExecutionPattern::Direct),
            (
                "调研最新 AI harness 实践并汇总证据",
                ExecutionPattern::Explore,
            ),
            ("实现并修复这个单文件小问题", ExecutionPattern::Execute),
            (
                "权衡两个架构方案并解决冲突方案",
                ExecutionPattern::Deliberate,
            ),
            (
                "使用多 Agent 协同完成复杂架构分析",
                ExecutionPattern::Collaborate,
            ),
            ("后台持续监控这项长期运行任务", ExecutionPattern::Supervise),
        ];

        for (prompt, expected_pattern) in cases {
            let first = crate::execution_core::build_runtime_execution_decision(prompt, None);
            let second = crate::execution_core::build_runtime_execution_decision(prompt, None);
            let wire = serde_json::to_value(&first).expect("runtime decision wire payload");

            assert_eq!(first.pattern(), expected_pattern, "prompt: {prompt}");
            assert_eq!(first.strategy, second.strategy, "prompt: {prompt}");
            assert_eq!(
                first.lease.input_fingerprint, second.lease.input_fingerprint,
                "prompt: {prompt}"
            );
            assert_eq!(first.lease.locked_pattern, expected_pattern);
            assert_eq!(wire["strategy"]["pattern"], expected_pattern.as_str());
        }
    }

    #[test]
    fn strategy_selected_event_failure_restores_unbound_owner_and_blocks_execution() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("durability-turn", "explain one function")
            .expect("strategy admission");

        let error = runtime
            .bind_turn_strategy_execution("durability-turn", "graph-without-store")
            .expect_err("selected event must be durable before graph execution");
        assert!(error.to_string().contains("event store is unavailable"));
        assert_eq!(
            runtime
                .active_turn_strategy()
                .and_then(|state| state.execution_graph_ref),
            None
        );
    }

    #[test]
    fn admitted_turn_has_one_strategy_identity_through_terminal_outcome() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        let first = runtime
            .begin_turn_strategy("turn-one", "explain this function")
            .expect("admit strategy");
        let replay = runtime
            .begin_turn_strategy("turn-one", "different wording cannot replace identity")
            .expect("same turn reuses strategy");
        assert_eq!(first.decision_id, replay.decision_id);
        assert_eq!(first.decision_lease, replay.decision_lease);

        let bound = runtime
            .bind_turn_strategy_execution("turn-one", "graph-one")
            .expect("bind graph");
        assert_eq!(bound.decision_id, first.decision_id);
        assert_eq!(bound.execution_graph_ref.as_deref(), Some("graph-one"));
        runtime
            .retarget_active_turn_strategy(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                harness_contract::core::ExecutionPattern::Execute,
                "provider tool batch retained the admitted decision lease",
            )
            .expect("running ToolBatch retarget is a selected revision");
        runtime
            .finish_turn_strategy(
                "turn-one",
                crate::execution_core::TurnStrategyDecisionStatus::Completed,
                crate::execution_core::TurnStrategyActualOutcome {
                    duration_ms: 10,
                    terminal_reason: "satisfied".to_string(),
                    ..Default::default()
                },
            )
            .expect("finish strategy");
        assert!(runtime.active_turn_strategy().is_none());

        let events = store
            .list_stream(&format!("session:{}", runtime.session().session_id))
            .expect("strategy events");
        let strategy_events = events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind.as_str(),
                    "runtime.strategy.selected"
                        | "runtime.strategy.downgraded"
                        | "runtime.strategy.early_stopped"
                        | "runtime.strategy.outcome"
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            strategy_events
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            vec![
                "runtime.strategy.selected",
                "runtime.strategy.selected",
                "runtime.strategy.outcome"
            ]
        );
        assert_eq!(strategy_events[1].status.as_deref(), Some("running"));
        assert_eq!(
            strategy_events[1].payload["selected_pattern"].as_str(),
            Some("execute")
        );
        assert!(turn_strategy_event_kind_allowed(
            "runtime.strategy.selected"
        ));
        assert!(!turn_strategy_event_kind_allowed(
            "runtime.strategy.retargeted"
        ));
        assert!(strategy_events.iter().all(|event| {
            event.scope == RuntimeEventScope::Session
                && event.payload["decision_id"].as_str() == Some(first.decision_id.as_str())
                && event.payload["decision_lease"].as_str() == Some(first.decision_lease.as_str())
                && event.payload["execution_graph_ref"].as_str() == Some("graph-one")
                && event.payload["session_ref"].as_str()
                    == Some(runtime.session().session_id.as_str())
                && event.payload["turn_ref"].as_str() == Some("turn-one")
        }));
    }

    #[test]
    fn high_overlap_publishes_downgrade_with_visible_reason() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        runtime
            .begin_turn_strategy("overlap-turn", "必须启动 Team 分别审查三个独立域并综合")
            .expect("admit team strategy");
        runtime
            .bind_turn_strategy_execution("overlap-turn", "overlap-graph")
            .expect("bind strategy graph");
        let selected = runtime.active_turn_strategy().expect("selected state");

        runtime
            .downgrade_turn_strategy(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                "measured evidence overlap 9100 bp exceeded the 800 bp Team budget; continue with one owner",
            )
            .expect("downgrade must be durable");

        let events = store
            .list_stream(&format!("session:{}", runtime.session().session_id))
            .expect("strategy events");
        let downgraded = events
            .iter()
            .find(|event| event.kind == "runtime.strategy.downgraded")
            .expect("overlap downgrade event");
        assert!(downgraded.sequence > 0);
        assert_eq!(
            downgraded.payload["decision_id"].as_str(),
            Some(selected.decision_id.as_str())
        );
        assert!(
            downgraded.payload["decision_revision"]
                .as_u64()
                .expect("downgrade revision")
                > selected.revision
        );
        assert_eq!(downgraded.payload["selected_candidate"], "direct");
        assert!(
            downgraded.payload["reason"]
                .as_str()
                .expect("visible reason")
                .contains("overlap 9100 bp")
        );
    }

    #[test]
    fn provider_constraint_publishes_monotonic_downgrade_and_retains_scope() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        runtime
            .begin_turn_strategy("provider-turn", "必须启动 Team 分别审查三个独立域并综合")
            .expect("admit team strategy");
        runtime
            .bind_turn_strategy_execution("provider-turn", "provider-graph")
            .expect("bind strategy graph");
        runtime
            .set_turn_strategy_focus_partitions(vec![FocusPartitionPlan {
                role_id: "reviewer".to_string(),
                shared_baseline: vec!["evidence:baseline".to_string()],
                slots: vec![FocusPartitionSlot {
                    focus_id: "runtime".to_string(),
                    boundary: "crates/runtime".to_string(),
                    evidence_responsibility: "Review the runtime boundary".to_string(),
                    capability_cropped_refs: vec!["read:crates/runtime".to_string()],
                    scope_hash: "sha256:provider-constraint-scope".to_string(),
                    overlap_budget_bp: 800,
                    novelty_target_bp: 6_000,
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                }],
            }])
            .expect("set evidence scope");
        let selected = runtime.active_turn_strategy().expect("selected state");
        {
            let mut guard = runtime
                .active_turn_strategy
                .lock()
                .expect("strategy owner lock");
            let state = guard.as_mut().expect("active strategy state");
            state.resource_snapshot.provider_concurrency_penalty_bp = 9_000;
        }

        runtime
            .downgrade_turn_strategy(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                "provider concurrency constraint 9000 bp removed the Team execution slot",
            )
            .expect("provider downgrade must be durable");

        let events = store
            .list_stream(&format!("session:{}", runtime.session().session_id))
            .expect("strategy events");
        let downgraded = events
            .iter()
            .find(|event| event.kind == "runtime.strategy.downgraded")
            .expect("provider downgrade event");
        assert!(
            downgraded.payload["decision_revision"]
                .as_u64()
                .expect("downgrade revision")
                > selected.revision
        );
        assert_eq!(downgraded.payload["selected_candidate"], "direct");
        assert_eq!(
            downgraded.payload["resource_snapshot"]["provider_concurrency_penalty_bp"],
            9_000
        );
        assert_eq!(
            downgraded.payload["evidence_scopes"][0]["slots"][0]["capability_cropped_refs"][0],
            "read:crates/runtime"
        );
    }

    #[test]
    fn low_novelty_publishes_bounded_early_stop() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        runtime
            .begin_turn_strategy("novelty-turn", "必须启动 Team 分别审查三个独立域并综合")
            .expect("admit team strategy");
        runtime
            .bind_turn_strategy_execution("novelty-turn", "novelty-graph")
            .expect("bind strategy graph");
        let selected = runtime.active_turn_strategy().expect("selected state");

        runtime
            .record_turn_strategy_early_stop(
                "low novelty: observed contribution 300 bp is below the 6000 bp target; stop further delegation",
            )
            .expect("early stop must be durable");

        let events = store
            .list_stream(&format!("session:{}", runtime.session().session_id))
            .expect("strategy events");
        let early_stops = events
            .iter()
            .filter(|event| event.kind == "runtime.strategy.early_stopped")
            .collect::<Vec<_>>();
        assert_eq!(
            early_stops.len(),
            1,
            "early stop is a single bounded transition"
        );
        let early_stop = early_stops[0];
        assert_eq!(
            early_stop.payload["decision_revision"].as_u64(),
            Some(selected.revision.saturating_add(1))
        );
        assert_eq!(early_stop.status.as_deref(), Some("early_stopped"));
        assert!(
            early_stop.payload["reason"]
                .as_str()
                .expect("visible early-stop reason")
                .contains("low novelty")
        );
    }

    #[test]
    fn recovered_strategy_restores_frozen_candidate_cost_estimates() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let first_runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        first_runtime
            .begin_turn_strategy(
                "recovery-cost-turn",
                "全面审查 runtime gateway webui 三个责任域",
            )
            .expect("first admission");
        {
            let mut active = first_runtime
                .active_turn_strategy
                .lock()
                .expect("strategy owner");
            let estimate = active
                .as_mut()
                .expect("active strategy")
                .decision
                .strategy
                .candidate_estimates
                .first_mut()
                .expect("candidate estimate");
            estimate.estimated_critical_path_ms = 987_654;
            estimate.calibration_source = "frozen-before-restart".to_string();
        }
        let frozen = first_runtime
            .bind_turn_strategy_execution("recovery-cost-turn", "recovery-cost-graph")
            .expect("durable selected event");
        let session_id = first_runtime.session().session_id;

        let mut resumed_session = Session::new();
        resumed_session.session_id = session_id;
        let resumed = ConversationRuntime::new(
            resumed_session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(store);
        resumed
            .begin_turn_strategy(
                "recovery-cost-turn",
                "the current router may have different live history",
            )
            .expect("resume admission");
        let recovered = resumed
            .bind_turn_strategy_execution("recovery-cost-turn", "recovery-cost-graph")
            .expect("recover frozen decision");

        assert_eq!(recovered.decision_id, frozen.decision_id);
        assert_eq!(
            recovered.decision.strategy.candidate_estimates,
            frozen.decision.strategy.candidate_estimates
        );
        assert_eq!(
            recovered.decision.strategy.candidate_estimates[0].calibration_source,
            "frozen-before-restart"
        );
    }

    #[test]
    fn preview_chars_handles_multibyte_text() {
        let text = "再次美化模型与状态展示，确保中文截断不会 panic".repeat(8);
        let preview = preview_chars(&text, 20);

        assert!(preview.ends_with("..."));
        assert!(text.starts_with(preview.trim_end_matches("...")));
    }

    #[test]
    fn model_can_retrieve_a_focused_chunk_from_tool_evidence() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let evidence_id = "tool-raw-call-1-deadbeef";
        let output = format!(
            "{} target_failure_code {}",
            "ordinary evidence ".repeat(1_200),
            "remaining evidence ".repeat(1_200)
        );
        let session_id = runtime.session().session_id;
        let access = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::core::EvidenceRef::new("tool", evidence_id),
            "sha256:test",
            output.len() as u64,
            "text/plain; charset=utf-8",
            format!("session-event://{session_id}/1"),
            format!("session:{session_id}"),
        );
        runtime.maybe_index_tool_output(evidence_id, "read_file", &output, Some(&access));

        let retrieved = runtime
            .retrieve_tool_evidence(&format!(
                r#"{{"evidence_ref":"tool://{evidence_id}","query":"target_failure_code","limit":2}}"#
            ))
            .expect("focused evidence should be retrievable");

        assert!(retrieved.contains("target_failure_code"));
        assert!(retrieved.len() < output.len());
    }

    #[test]
    fn context_profile_controls_runtime_envelope_profile() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.set_context_profile(ContextProfile::YoloGoal);
        let envelope = runtime.build_context_envelope(
            "continue task",
            vec![ContextItem::new(
                "task",
                ContextSourceKind::Task,
                ContextRole::TaskState,
                "active yolo task",
            )],
            Vec::new(),
            Vec::new(),
            runtime.context_budget_tokens(),
        );

        assert_eq!(runtime.context_profile(), ContextProfile::YoloGoal);
        assert_eq!(envelope.profile, ContextProfile::YoloGoal);
        assert_eq!(envelope.identity.mode, ContextMode::YoloGoal);
        assert!(envelope.assembled.runtime_header[0].contains("profile:YoloGoal"));
    }

    #[test]
    fn context_budget_defaults_to_seventy_percent_of_model_window() {
        assert_eq!(resolve_context_budget_tokens(1_000_000, 7_000), 700_000);
        assert_eq!(resolve_context_budget_tokens(128_000, 7_000), 89_600);
        assert_eq!(resolve_context_budget_tokens(32_000, 7_000), 22_400);
    }

    #[test]
    fn context_budget_ratio_is_clamped_to_safe_bounds() {
        assert_eq!(resolve_context_budget_tokens(1_000_000, 99_999), 950_000);
        assert_eq!(resolve_context_budget_tokens(1_000_000, 1), 100_000);
    }

    #[test]
    fn memory_config_consumes_runtime_budget_plan() {
        let feature_config = RuntimeFeatureConfig::default();
        let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: 1_000_000,
            model_max_output_tokens: 32_000,
            subsystem_budget_ratio_bp: 7_000,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        });

        let mem_cfg = build_cc_memory_config_with_budget(&feature_config, &plan);

        assert_eq!(mem_cfg.budget.context_window, 700_000);
        assert_eq!(mem_cfg.budget.reserved_response, 32_000);
        assert_ne!(mem_cfg.budget.context_window, 200_000);
        assert!(mem_cfg.budget.runtime_managed);
        assert_eq!(
            mem_cfg.budget.selected_item_limit,
            plan.memory_retrieval_budget.selected_item_limit
        );
        assert_eq!(
            mem_cfg.budget.l3_checkpoint,
            plan.memory_retrieval_budget.l3_checkpoint
        );
    }

    #[test]
    fn telemetry_wall_speed_uses_wall_duration() {
        let wall_speed = rate_per_second(8_562, 178_350).expect("wall speed");

        assert!((wall_speed - 48.01).abs() < 0.2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn high_risk_mutation_creates_real_checkpoint_before_dispatch() {
        use harness_contract::core::{
            ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, TaskRisk,
        };

        let checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let mutation_calls = Arc::new(AtomicUsize::new(0));
        let checkpoint_counter = Arc::clone(&checkpoint_calls);
        let mutation_counter = Arc::clone(&mutation_calls);
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new()
                .register("checkpoint_create", move |_| {
                    checkpoint_counter.fetch_add(1, Ordering::SeqCst);
                    Ok(r#"{"id":"checkpoint-test"}"#.to_string())
                })
                .register("write_file", move |_| {
                    mutation_counter.fetch_add(1, Ordering::SeqCst);
                    Ok("written".to_string())
                }),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let requests = vec![crate::tool_dispatch::ToolRequest {
            tool_use_id: "write-1".to_string(),
            tool_name: "write_file".to_string(),
            input: r#"{"path":"src/lib.rs","content":"x"}"#.to_string(),
            depends_on: Vec::new(),
        }];
        let plan = crate::tool_execution_plan::ToolExecutionPlan::from_requests(&requests);
        let mut decision =
            crate::execution_core::build_runtime_execution_decision("实现并修改这个文件", None);
        decision.strategy.pattern = ExecutionPattern::Execute;
        decision.strategy.understanding.risk = TaskRisk::High;
        decision.strategy.modifiers = vec![
            ExecutionModifier::WithGuardrails,
            ExecutionModifier::WithCheckpoint,
        ];
        decision.strategy.gates = vec![ExecutionPolicyGate::Permission, ExecutionPolicyGate::Risk];
        decision.compile_target = crate::execution_core::RuntimeCompileTarget::ExecutionGraph;
        decision.executable = true;
        decision.blocked_reasons.clear();
        let mut validation = plan.validate_against_execution_decision(&decision);

        runtime
            .satisfy_tool_strategy_gates(&plan, &decision, &mut validation)
            .await;

        assert!(validation.allowed, "{:?}", validation.findings);
        assert!(validation.checkpoint_created);
        assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);

        decision.strategy.understanding.risk = TaskRisk::Critical;
        decision.strategy.gates.push(ExecutionPolicyGate::Approval);
        let mut critical_validation = plan.validate_against_execution_decision(&decision);
        runtime
            .satisfy_tool_strategy_gates(&plan, &decision, &mut critical_validation)
            .await;
        assert!(!critical_validation.allowed);
        assert!(!critical_validation.checkpoint_created);
        assert!(
            critical_validation
                .findings
                .iter()
                .any(|finding| finding == "critical_mutation_missing_approval_runtime")
        );
        assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn model_router_delegates_to_provider_default_when_no_model_is_explicit() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        assert_eq!(runtime.model_candidates_for_turn("简单问题"), vec![""]);
    }

    #[test]
    fn model_router_keeps_primary_model_first_and_routes_fallbacks() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime.model = Some("balanced-model".to_string());
        runtime.fallbacks = vec!["stepfun-fast".to_string(), "deepseek-depth".to_string()];

        {
            let mut registry = runtime
                .model_performance_registry
                .lock()
                .expect("registry lock");
            registry.record_telemetry(
                &crate::cowd_event::RunModelTelemetry {
                    model: Some("stepfun-fast".to_string()),
                    models_used: vec!["stepfun-fast".to_string()],
                    first_token_latency_ms: Some(160),
                    active_stream_duration_ms: Some(1_000),
                    wall_duration_ms: 1_200,
                    output_chars: 1_000,
                    output_chunks: 10,
                    input_tokens: 400,
                    output_tokens: 180,
                    cache_create_tokens: 0,
                    cache_read_tokens: 0,
                    total_tokens: 580,
                    usage_source: "provider".to_string(),
                    wall_chars_per_second: Some(833.33),
                    wall_tokens_per_second: Some(150.0),
                    active_chars_per_second: Some(1_000.0),
                    active_tokens_per_second: Some(180.0),
                    chars_per_second: Some(833.33),
                    tokens_per_second: Some(150.0),
                },
                Some(0.72),
                false,
            );
            registry.record_telemetry(
                &crate::cowd_event::RunModelTelemetry {
                    model: Some("deepseek-depth".to_string()),
                    models_used: vec!["deepseek-depth".to_string()],
                    first_token_latency_ms: Some(950),
                    active_stream_duration_ms: Some(4_000),
                    wall_duration_ms: 5_000,
                    output_chars: 4_000,
                    output_chunks: 20,
                    input_tokens: 900,
                    output_tokens: 360,
                    cache_create_tokens: 0,
                    cache_read_tokens: 0,
                    total_tokens: 1_260,
                    usage_source: "provider".to_string(),
                    wall_chars_per_second: Some(800.0),
                    wall_tokens_per_second: Some(72.0),
                    active_chars_per_second: Some(1_000.0),
                    active_tokens_per_second: Some(90.0),
                    chars_per_second: Some(800.0),
                    tokens_per_second: Some(72.0),
                },
                Some(0.96),
                false,
            );
        }

        let quick = runtime.model_candidates_for_turn("快速回答这个简单问题");
        let deep = runtime.model_candidates_for_turn("深度审计复杂架构方案");

        assert_eq!(quick.first().map(String::as_str), Some("balanced-model"));
        assert_eq!(deep.first().map(String::as_str), Some("balanced-model"));
        assert_eq!(quick.get(1).map(String::as_str), Some("stepfun-fast"));
        assert_eq!(deep.get(1).map(String::as_str), Some("deepseek-depth"));
    }

    #[test]
    fn reconstructs_usage_tracker_from_restored_session() {
        struct SimpleApi;
        impl ApiClient for SimpleApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("done".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        let mut session = Session::new();
        session
            .messages
            .push(crate::session::ConversationMessage::assistant_with_usage(
                vec![ContentBlock::Text {
                    text: "earlier".to_string(),
                }],
                Some(TokenUsage {
                    input_tokens: 11,
                    output_tokens: 7,
                    cache_creation_input_tokens: 2,
                    cache_read_input_tokens: 1,
                }),
            ));

        let runtime = ConversationRuntime::new(
            session,
            SimpleApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        );

        assert_eq!(runtime.usage().turns(), 1);
        assert_eq!(runtime.usage().cumulative_usage().total_tokens(), 21);
    }

    // ── M2: Memory system tests ──────────────────────────────────────

    #[derive(Clone)]
    struct MockApi;
    impl ApiClient for MockApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
        }
    }

    #[derive(Clone)]
    struct PromptRecordingApi {
        requests: Arc<std::sync::Mutex<Vec<ApiRequest>>>,
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
    }

    impl ApiClient for PromptRecordingApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.requests
                .lock()
                .expect("request recorder")
                .push(request);
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::TextDelta("skill-aware result".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }

        fn configure_tool_exposure(
            &mut self,
            projection: harness_contract::tool::ToolExposureProjection,
        ) {
            self.projections
                .lock()
                .expect("projection recorder")
                .push(projection);
        }
    }

    #[tokio::test]
    async fn first_model_step_activates_skill_persists_bridge_and_injects_asset() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&memory::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "skill-activation".to_string(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "None".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let profile = SkillCapabilityProfile {
            skill_id: "release-evidence".to_string(),
            name: "Release Evidence".to_string(),
            version: Some("1.0.0".to_string()),
            source_root: "/skills/release-evidence".to_string(),
            package_fingerprint: "test".to_string(),
            kind: SkillKind::Workflow,
            lifecycle_status: SkillLifecycleStatus::UsablePrompt,
            adapters: vec![SkillAdapterKind::PromptOnly],
            risk_level: SkillRiskLevel::Low,
            entrypoints: vec![SkillEntrypoint {
                runtime: SkillDetectedRuntime::Markdown,
                path: "SKILL.md".to_string(),
                adapter: SkillAdapterKind::PromptOnly,
                command_hint: None,
            }],
            inspection_summary: vec!["release evidence planning".to_string()],
            structured_dependencies: Vec::new(),
        };
        let mut runtime = ConversationRuntime::new(
            session,
            PromptRecordingApi {
                requests: Arc::clone(&requests),
                projections: Arc::clone(&projections),
            },
            StaticToolExecutor::new().register("lark_cli_read", |_| Ok("{}".to_string())),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(Arc::clone(&store))
        .with_skill_profiles(vec![profile])
        .with_agent_skill_profile(AgentSkillProfile {
            adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
            ..AgentSkillProfile::default()
        })
        .with_skill_prompt_assets(vec![super::RuntimeSkillPromptAsset {
            skill_id: "release-evidence".to_string(),
            version: Some("1.0.0".to_string()),
            content: "Require release evidence before accepting completion.".to_string(),
            source_ref: "skill://release-evidence/SKILL.md".to_string(),
            tool_refs: vec!["lark_cli_read".to_string()],
        }]);
        runtime.model = Some("test-model".to_string());
        runtime
            .begin_turn_strategy("test-skill-turn", "prepare release evidence")
            .expect("test turn strategy admission");

        runtime
            .execute_model_step("prepare release evidence", true)
            .await
            .expect("first skill-aware model step");

        let events = store
            .session_domain_events_page(&session_id, 0, 20)
            .await
            .expect("skill domain events");
        assert!(events.events.iter().any(|event| {
            event.kind == "skill_candidates"
                && event.payload["source"] == "conversation_runtime.skill_activation"
                && event.payload["selected"] == "release-evidence"
        }));
        assert!(events.events.iter().any(|event| {
            event.kind == "skill_memory_candidate"
                && event.payload["source"] == "conversation_runtime.skill_memory_candidate"
                && event.payload["selected"] == "release-evidence"
        }));
        let requests = requests.lock().expect("request recorder");
        assert_eq!(requests.len(), 1);
        assert!(
            rendered_prompt(&requests[0].prompt)
                .contains("Require release evidence before accepting completion.")
        );
        let projections = projections.lock().expect("projection recorder");
        assert_eq!(projections.len(), 1);
        assert!(
            projections[0]
                .active_ids
                .iter()
                .any(|tool| tool == "lark_cli_read"),
            "the selected Skill tool must be visible in its first provider request"
        );
    }

    struct RuntimeAwareApi(Arc<std::sync::atomic::AtomicBool>);

    impl ApiClient for RuntimeAwareApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.0.store(
                tokio::runtime::Handle::try_current().is_ok(),
                std::sync::atomic::Ordering::SeqCst,
            );
            Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
        }
    }

    #[test]
    fn synchronous_stream_collection_creates_the_stream_inside_tokio() {
        let observed_runtime = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut api = RuntimeAwareApi(Arc::clone(&observed_runtime));
        let events = api
            .stream_collect(ApiRequest {
                prompt: PromptAssembly::default(),
                messages: Vec::new(),
                model: "test".to_string(),
                reasoning_effort_override: None,
                budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                    "test", 128_000, 4_096, 128, 256, 0,
                ),
            })
            .expect("synchronous collection should succeed");

        assert_eq!(events, vec![AssistantEvent::MessageStop]);
        assert!(
            observed_runtime.load(std::sync::atomic::Ordering::SeqCst),
            "ApiClient::stream must be constructed with an active Tokio runtime"
        );
    }

    #[derive(Clone)]
    struct ExposureRecordingApi {
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
    }

    impl ApiClient for ExposureRecordingApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::TextDelta("bounded conclusion".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }

        fn configure_tool_exposure(
            &mut self,
            projection: harness_contract::tool::ToolExposureProjection,
        ) {
            self.projections.lock().unwrap().push(projection);
        }
    }

    struct ExposureToolExecutor;

    impl crate::ToolExecutor for ExposureToolExecutor {
        fn execute(&self, _name: &str, _input: &str) -> Result<String, crate::ToolError> {
            Err(crate::ToolError::new("test executor must not run"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec![
                "ToolSearch".to_string(),
                "custom_reader".to_string(),
                "grep_search".to_string(),
            ]
        }
    }

    #[test]
    fn capability_receipt_projects_current_schema_separately_from_catalog() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            ExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        *runtime.tool_exposure_state.lock().expect("exposure state") = Some(ToolExposureState {
            catalog_revision: 5,
            bootstrap: ["ToolSearch".to_string(), "runtime_capabilities".to_string()]
                .into_iter()
                .collect(),
            active: ["ToolSearch".to_string(), "runtime_capabilities".to_string()]
                .into_iter()
                .collect(),
            deferred: ["read_many".to_string(), "runtime_orchestrate".to_string()]
                .into_iter()
                .collect(),
            reason: "bootstrap tools exposed".to_string(),
            revision: 2,
            fallback_full: false,
        });

        let projected = runtime.project_runtime_capabilities_for_model(
            &serde_json::json!({
                "available_tool_names": ["ToolSearch", "runtime_capabilities", "read_many", "runtime_orchestrate"],
                "runtime_orchestrate": {"available": true, "blocked_reasons": []},
                "action_plane": {"can_execute_now": true},
                "strategy": {"model_callable_tools": ["ToolSearch", "runtime_capabilities", "read_many", "runtime_orchestrate"]}
            })
            .to_string(),
        );
        let value: serde_json::Value =
            serde_json::from_str(&projected).expect("projected capability JSON");

        assert_eq!(
            value["catalog_tool_names"],
            serde_json::json!([
                "ToolSearch",
                "runtime_capabilities",
                "read_many",
                "runtime_orchestrate"
            ])
        );
        assert_eq!(
            value["tool_visibility"]["active_function_schemas"],
            serde_json::json!(["ToolSearch", "runtime_capabilities"])
        );
        assert_eq!(
            value["strategy"]["model_callable_tools"],
            serde_json::json!(["ToolSearch", "runtime_capabilities"])
        );
        assert_eq!(value["runtime_orchestrate"]["available"], false);
        assert_eq!(value["runtime_orchestrate"]["schema_active"], false);
        assert_eq!(value["action_plane"]["can_execute_now"], false);
        assert_eq!(value["action_plane"]["recommended_next_tool"], "ToolSearch");
    }

    #[tokio::test]
    async fn text_only_checkpoint_hides_tools_for_exactly_one_model_request() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = ExposureRecordingApi {
            projections: Arc::clone(&projections),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            ExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.require_next_model_final_response();
        runtime
            .begin_turn_strategy("test-text-only-turn", "summarize checked evidence")
            .expect("test turn strategy admission");
        runtime
            .execute_model_step("summarize checked evidence", true)
            .await
            .unwrap();
        runtime
            .execute_model_step("summarize checked evidence", false)
            .await
            .unwrap();

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 2);
        assert!(projections[0].active_ids.is_empty());
        assert_eq!(projections[0].deferred_ids.len(), 3);
        assert_eq!(projections[1].active_ids, vec!["ToolSearch", "grep_search"]);
        assert!(
            projections[1]
                .deferred_ids
                .contains(&"custom_reader".to_string())
        );
    }

    struct MutationExposureToolExecutor;

    impl crate::ToolExecutor for MutationExposureToolExecutor {
        fn execute(&self, _name: &str, _input: &str) -> Result<String, crate::ToolError> {
            Err(crate::ToolError::new("exposure test executor must not run"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec![
                "ToolSearch".to_string(),
                "read_file".to_string(),
                "grep_search".to_string(),
                "edit_file".to_string(),
                "write_file".to_string(),
            ]
        }
    }

    #[tokio::test]
    async fn mutation_checkpoint_exposes_only_writes_for_one_model_request() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = ExposureRecordingApi {
            projections: Arc::clone(&projections),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            MutationExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.require_next_model_tools([
            "edit_file".to_string(),
            "write_file".to_string(),
            "unknown_mutator".to_string(),
        ]);
        runtime
            .begin_turn_strategy("test-mutation-exposure-turn", "write the authorized file")
            .expect("test turn strategy admission");
        runtime
            .execute_model_step("write the authorized file", true)
            .await
            .expect("write-only model step");
        runtime
            .execute_model_step("write the authorized file", false)
            .await
            .expect("restored model step");

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 2);
        assert_eq!(
            projections[0].active_ids,
            vec!["edit_file".to_string(), "write_file".to_string()]
        );
        assert!(
            !projections[0]
                .active_ids
                .contains(&"unknown_mutator".to_string())
        );
        assert!(
            projections[0]
                .deferred_ids
                .contains(&"read_file".to_string())
        );
        assert!(
            projections[1]
                .active_ids
                .contains(&"ToolSearch".to_string())
        );
        assert!(projections[1].active_ids.contains(&"read_file".to_string()));
        assert!(
            projections[1]
                .active_ids
                .contains(&"grep_search".to_string())
        );
        assert!(projections[1].exposure_revision > projections[0].exposure_revision);
    }

    #[derive(Clone)]
    struct DynamicExposureApi {
        requests: Arc<std::sync::atomic::AtomicUsize>,
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
    }

    impl ApiClient for DynamicExposureApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let request = self
                .requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if request == 0 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "discover-1".to_string(),
                        name: "ToolSearch".to_string(),
                        input: r#"{"query":"read files"}"#.to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("discovery complete".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        fn configure_tool_exposure(
            &mut self,
            projection: harness_contract::tool::ToolExposureProjection,
        ) {
            self.projections.lock().unwrap().push(projection);
        }
    }

    struct DynamicExposureToolExecutor;

    impl crate::ToolExecutor for DynamicExposureToolExecutor {
        fn execute(&self, name: &str, _input: &str) -> Result<String, crate::ToolError> {
            if name != "ToolSearch" {
                return Err(crate::ToolError::new(
                    "only bootstrap discovery is executable",
                ));
            }
            Ok(serde_json::json!({
                "query": "read files",
                "catalog_revision": 0,
                "descriptors": [{
                    "canonical_id": "custom_reader",
                    "display_name": "custom_reader",
                    "source": "test",
                    "schema_hash": "read-v1",
                    "required_permission": "read_only",
                    "permission_source": "test",
                    "health": "healthy"
                }],
                "activation_candidates": ["custom_reader"]
            })
            .to_string())
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec!["ToolSearch".to_string(), "custom_reader".to_string()]
        }

        fn classify_tool_safety(
            &self,
            name: &str,
            _input: &str,
        ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
            (name == "ToolSearch").then_some(crate::tool_orchestrator::ToolSafetyCategory::ReadOnly)
        }
    }

    #[tokio::test]
    async fn dynamic_tool_exposure_defers_schema_until_discovery_activation() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = DynamicExposureApi {
            requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            projections: Arc::clone(&projections),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("test-dynamic-exposure-turn", "inspect files")
            .expect("test turn strategy admission");

        let first = runtime
            .execute_model_step("inspect files", true)
            .await
            .expect("first model step");
        let ModelStepIntent::ToolCalls { calls } = first.intent else {
            panic!("first request must invoke ToolSearch");
        };
        let batch = runtime
            .execute_tool_batch_step(&calls, &crate::SharedPrompter::none(), 1)
            .await
            .expect("ToolSearch execution");
        assert_eq!(batch.failed, 0, "ToolSearch batch must succeed: {batch:?}");
        assert!(
            runtime
                .tool_exposure_state
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|state| state.active.contains("custom_reader")),
            "ToolSearch must activate custom_reader before the following provider request"
        );
        runtime
            .execute_model_step("inspect files", false)
            .await
            .expect("second model step");

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 2);
        assert_eq!(projections[0].catalog_revision, 0);
        assert_eq!(projections[0].active_ids, vec!["ToolSearch"]);
        assert_eq!(projections[0].deferred_ids, vec!["custom_reader"]);
        assert!(
            projections[1]
                .active_ids
                .contains(&"custom_reader".to_string())
        );
        assert!(projections[1].exposure_revision > projections[0].exposure_revision);
    }

    #[tokio::test]
    async fn governed_tool_results_persist_raw_evidence_and_bound_model_receipt() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&memory::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "test-chat".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "None".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(Arc::clone(&store));
        let raw = format!("first\n{}\nlast", "middle-evidence ".repeat(8_000));

        let receipt = runtime
            .prepare_governed_tool_result(
                "governed-read-1",
                "read_file",
                r#"{"path":"README.md"}"#,
                &raw,
                false,
            )
            .await;

        let output = receipt
            .blocks
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolResult { output, .. } => Some(output),
                _ => None,
            })
            .expect("governed receipt must be a tool result");
        assert!(
            output.contains("tool://tool-raw-governed-read-1-"),
            "unexpected governed receipt: {output}"
        );
        assert!(
            output.len() < raw.len() / 10,
            "model must receive a receipt, not raw output"
        );
        let events = store
            .session_domain_events_page(&session_id, 0, 20)
            .await
            .expect("durable tool evidence");
        assert!(events.events.iter().any(|event| {
            event.kind == "evidence.raw.persisted"
                && event.payload.get("raw").and_then(serde_json::Value::as_str)
                    == Some(raw.as_str())
        }));
        let audit = runtime.turn_evidence_audits();
        assert_eq!(audit.len(), 1);
        assert!(audit[0].access.is_some());
        assert!(audit[0].omitted_tokens > 0);
    }

    #[tokio::test]
    async fn governed_tool_result_never_publishes_durable_access_after_raw_store_failure() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        // No matching SessionRecord is created: the SessionStore adapter must
        // fail instead of fabricating an evidence receipt.
        .with_session_store(Arc::new(
            memory::UnifiedSessionStore::open_in_memory().unwrap(),
        ));
        let raw = "raw output retained only in the active runtime when durable write fails\n"
            .repeat(1_000);

        let result = runtime
            .prepare_governed_tool_result(
                "raw-failure-1",
                "read_file",
                r#"{"path":"README.md"}"#,
                &raw,
                false,
            )
            .await;
        let output = result
            .blocks
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolResult { output, .. } => Some(output),
                _ => None,
            })
            .expect("fallback still produces a model-visible tool result");
        assert!(output.contains("Ephemeral evidence (active runtime only)"));
        assert!(!output.contains(raw.as_str()));
        let evidence_id = output
            .split("tool://")
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .map(|value| value.trim_end_matches('.'))
            .expect("bounded receipt should identify its evidence");
        let retrieved = runtime
            .retrieve_tool_evidence(&format!(
                r#"{{"evidence_ref":"tool://{evidence_id}","query":"durable write fails","limit":1}}"#
            ))
            .expect("active runtime should retain an ephemeral evidence spool");
        assert!(retrieved.contains(raw.lines().next().unwrap_or_default()));
        let audit = runtime.turn_evidence_audits();
        assert_eq!(audit.len(), 1);
        assert!(audit[0].access.is_none());
        assert!(!audit[0].raw_available);
    }

    #[tokio::test]
    async fn context_turn_report_is_durable_before_runtime_exposes_it() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&memory::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "context-report".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(Arc::clone(&store));
        let report = runtime.build_context_turn_report("turn-durable", TokenUsage::default(), None);

        runtime
            .remember_context_turn_report(report.clone())
            .await
            .expect("report persistence must finish before exposure");
        assert_eq!(runtime.last_context_turn_report(), Some(report.clone()));
        let events = store
            .session_domain_events_page(&session_id, 0, 20)
            .await
            .expect("report event");
        assert!(events.events.iter().any(|event| {
            event.kind == "context.turn_report"
                && event.payload.get("report") == Some(&serde_json::to_value(&report).unwrap())
        }));
    }

    #[tokio::test]
    async fn context_turn_report_write_failure_does_not_expose_a_successful_report() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(store);
        let report = runtime.build_context_turn_report("turn-failure", TokenUsage::default(), None);

        let error = runtime
            .remember_context_turn_report(report)
            .await
            .expect_err("a foreign-key persistence failure must fail the terminal report path");
        assert!(
            error
                .to_string()
                .contains("context governance persistence failed")
        );
        assert_eq!(runtime.last_context_turn_report(), None);
    }

    #[tokio::test]
    async fn compaction_event_failure_is_terminal_and_does_not_claim_durable_recovery() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_store(store);

        let error = runtime
            .record_session_compacted(
                crate::session::SessionCompaction {
                    count: 1,
                    removed_message_count: 3,
                    summary: "durability must precede local compaction".to_string(),
                },
                3,
                None,
                memory::compression::session::SessionSemanticCheckpoint {
                    schema_version: 2,
                    checkpoint_id: "checkpoint-failure".to_string(),
                    session_id: "missing-session".to_string(),
                    agent_id: "primary".to_string(),
                    project_id: None,
                    task_id: None,
                    team_id: None,
                    summary: "durability test".to_string(),
                    user_rules: Vec::new(),
                    goal: None,
                    constraints: Vec::new(),
                    decisions: Vec::new(),
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    file_changes: Vec::new(),
                    resume_cursor: memory::compression::session::SessionResumeCursor {
                        message_index: 0,
                        event_sequence: None,
                        checkpoint_id: "checkpoint-failure".to_string(),
                    },
                    token_stats: memory::compression::session::CheckpointTokenStats {
                        before: 1,
                        after: 1,
                        message_count: 0,
                    },
                    source_range: memory::compression::session::CompactionSourceRange {
                        session_id: "missing-session".to_string(),
                        message_start: 0,
                        message_end_exclusive: 0,
                        event_start: None,
                        event_end_exclusive: None,
                        raw_refs: Vec::new(),
                    },
                    facts: Vec::new(),
                },
            )
            .await
            .expect_err("missing session carrier must reject canonical compaction persistence");
        assert!(
            error
                .to_string()
                .contains("atomic compaction persistence failed")
        );
    }

    #[test]
    fn context_turn_report_includes_active_knowledge_activation_report() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.set_turn_knowledge_report(harness_contract::knowledge::KnowledgeTurnReport {
            activation_plan_id: Some("knowledge-plan-test".to_string()),
            active_pack_ids: vec!["pack-domain-default".to_string()],
            blocked_namespaces: vec!["project:irrelevant not relevant to intent".to_string()],
            compliance_warnings: Vec::new(),
            evidence_refs: vec![harness_contract::core::KernelRef::new(
                "knowledge_chunk",
                "chunk-1",
            )],
            usage_signals: Vec::new(),
        });

        let report = runtime.build_context_turn_report(
            "turn-1",
            TokenUsage {
                input_tokens: 128,
                output_tokens: 32,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            None,
        );

        let knowledge = report.knowledge.expect("knowledge report is attached");
        assert_eq!(
            knowledge.activation_plan_id.as_deref(),
            Some("knowledge-plan-test")
        );
        assert_eq!(knowledge.active_pack_ids, vec!["pack-domain-default"]);
        assert_eq!(knowledge.blocked_namespaces.len(), 1);
        assert_eq!(knowledge.evidence_refs[0].ref_type, "knowledge_chunk");
    }

    #[test]
    fn m2_layer_priority_l0_before_l3() {
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        assert!(
            rank(MemoryLayer::L0) > rank(MemoryLayer::L3),
            "L0 must rank higher than L3"
        );
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L1));
        assert!(rank(MemoryLayer::L1) > rank(MemoryLayer::L2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_empty_session_no_memory_crash() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        let _ = rt.prepare_reality_context("query").await;
        let _ = rt.run_memory_post_turn().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_budget_cap_without_memory_returns_system_prompt() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["test prompt".to_string()],
        );
        let result = rt.prepare_reality_context("test").await;
        assert_eq!(result.trusted_system[0], "test prompt");
        assert!(
            result
                .trusted_system
                .get(1)
                .is_some_and(|line| line.contains("profile:MainTurn")),
            "without memory manager, returns stable head followed by runtime header"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_prepare_without_memory_records_degraded_context_envelope() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();
        let prompt = rt.prepare_reality_context("remember this").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert_eq!(prompt.trusted_system[0], "stable system");
        assert!(prompt.trusted_system[1].contains("profile:MainTurn"));
        assert!(
            prompt
                .trusted_system
                .iter()
                .any(|segment| segment.contains("context_governance_report_id:"))
        );
        assert_eq!(envelope.intent, "remember this");
        assert_eq!(envelope.assembled.stable_head, vec!["stable system"]);
        assert_eq!(
            envelope.diagnostics.degraded_sources,
            vec![ContextSourceKind::Memory]
        );
        assert!(
            envelope
                .selected
                .iter()
                .all(|item| item.source != ContextSourceKind::Memory)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn external_resume_context_enters_prompt_and_envelope_without_memory() {
        let session = Session::new();
        let session_id = session.session_id.clone();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();

        rt.inject_resume_context(ResumeContextPacket {
            session_id: session_id.clone(),
            handoff_summary: Some("continue v0.8.13 context work".to_string()),
            active_task: Some("persist context timeline".to_string()),
            recent_decisions: vec!["DB session_events is the canonical timeline".to_string()],
            blockers: vec!["none".to_string()],
            source: ResumeContextSource::Mixed,
        });

        let prompt = rt.prepare_reality_context("resume").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(
            prompt
                .contextual_packets
                .iter()
                .any(|packet| packet.content.contains("continue v0.8.13 context work"))
        );
        let handoff = envelope
            .selected
            .iter()
            .find(|item| item.source == ContextSourceKind::Handoff)
            .expect("resume context should remain selected alongside workspace packets");
        assert_eq!(handoff.authority, ContextAuthority::Session);
        assert!(handoff.content.contains("Active task"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_tool_trace_enters_next_prompt_and_envelope() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();

        let tool_result = ConversationMessage::tool_result(
            "tool-1".to_string(),
            "bash".to_string(),
            "cargo test passed for context runtime".to_string(),
            false,
        );
        rt.remember_tool_trace_from_message(&tool_result);

        let prompt = rt.prepare_reality_context("next turn").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(
            prompt
                .contextual_packets
                .iter()
                .any(|packet| packet.content.contains("cargo test passed"))
        );
        assert!(
            envelope
                .selected
                .iter()
                .any(|item| item.source == ContextSourceKind::ToolTrace)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_structured_xml_format_present() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["base prompt".to_string()],
        );
        let prompt = rt.prepare_reality_context("hello").await;
        assert!(
            !prompt.trusted_system.is_empty(),
            "should have system prompt"
        );
    }

    #[test]
    fn m2_error_propagation_returns_result() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["sys".to_string()],
        );
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .handle()
                .clone()
        });
        let r = handle.block_on(rt.run_memory_post_turn());
        assert!(
            r.is_ok(),
            "run_memory_post_turn should return Ok when no memory manager"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_structured_injection_has_memory_context_tag() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        let prompt = rt.prepare_reality_context("test").await;
        assert!(!prompt.trusted_system.is_empty());
        // Without memory manager, should still return system prompt
        assert!(
            prompt.trusted_system[0] == "system" || prompt.trusted_system[0].starts_with("system")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_reality_context_suppresses_memory_conflicting_with_current_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("memory.db");
        let blob_dir = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_dir).unwrap();

        let mem_cfg = memory::config::MemoryConfig {
            store: memory::config::StoreConfig {
                sqlite_path: db_path,
                blob_dir,
                enable_vector_index: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mgr = Arc::new(CognitiveContextManager::new(mem_cfg).await.unwrap());
        let session = Session::new();
        let project_id = memory_project_id_for_session(&session).expect("workspace project id");
        let now = chrono::Utc::now();
        mgr.remember(memory::types::MemoryEntry {
            id: memory::types::MemoryId::new_v4(),
            layer: memory::types::MemoryLayer::L1,
            category: memory::types::MemoryCategory::UserPreference,
            priority: memory::types::Priority::High,
            source: memory::types::MemorySource::UserExplicit,
            title: "User preference: 不要使用工具或编排".to_string(),
            content: "用户历史偏好：不要使用工具或编排。".to_string(),
            embedding: None,
            tags: vec!["preference".to_string()],
            relations: Vec::new(),
            confidence: 0.95,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: memory::MemoryScope::Project(project_id.clone()),
            session_id: None,
            source_agent: None,
            visibility: memory::types::AgentVisibility::Shared,
        })
        .await
        .unwrap();
        let loaded_l1 = mgr
            .list_layer_full_entries(memory::types::MemoryLayer::L1)
            .await
            .unwrap();
        assert!(
            loaded_l1
                .iter()
                .any(|entry| entry.title == "User preference: 不要使用工具或编排")
        );
        let memory_turn = memory::MemoryTurnContext::new("test-session", "primary")
            .with_project_id(Some(project_id));
        let prepared = mgr
            .prepare_context_for_turn(
                &memory_turn,
                "请先使用 runtime_capabilities 调用工具分析",
                &[],
            )
            .await
            .unwrap();
        assert!(
            prepared
                .entries
                .iter()
                .any(|entry| entry.title == "User preference: 不要使用工具或编排"),
            "prepared entries: {:?}",
            prepared
                .entries
                .iter()
                .map(|entry| entry.title.as_str())
                .collect::<Vec<_>>()
        );

        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_memory_manager(mgr);

        let prompt = rendered_prompt(
            &rt.prepare_reality_context("请先使用 runtime_capabilities 调用工具分析")
                .await,
        );
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(
            envelope
                .omitted
                .iter()
                .any(|omission| omission.reason.contains("suppressed_for_current_turn"))
        );
        assert!(!prompt.contains("<title>User preference: 不要使用工具或编排</title>"));
        assert!(!prompt.contains("<knowledge_compliance>"));
    }

    #[test]
    fn m2_layer_ranking_verification() {
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        assert_eq!(rank(MemoryLayer::L0), 5);
        assert_eq!(rank(MemoryLayer::L4), 1);
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_budget_cap_applied_on_prepare() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["base".to_string()],
        );
        // Verify that prepare_reality_context doesn't panic with empty session
        let result = rt.prepare_reality_context("any query").await;
        assert!(
            !result.trusted_system.is_empty(),
            "should return at least the system prompt"
        );
    }

    // ── M2-L2: integration-level memory tests ──────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_l2_budget_enforcement_limits_system_prompt() {
        // M2-L2-2: verify memory context doesn't exceed budget proportions
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system prompt".to_string()],
        )
        .without_memory();
        let prompt = rt.prepare_reality_context("test query").await;
        // Without selected memories, the prompt still includes the stable head and
        // runtime governance context. Attachment/resource guidance may add more
        // bounded sections, so this must remain a semantic budget assertion.
        assert_eq!(prompt.trusted_system[0], "system prompt");
        assert!(
            prompt
                .trusted_system
                .iter()
                .any(|segment| segment.contains("profile:MainTurn"))
        );
        assert!(!rendered_prompt(&prompt).contains("<memory_context>"));
        let total_prompt_chars = prompt.estimated_chars();
        assert!(
            total_prompt_chars < 20_000,
            "memory-free runtime prompt should stay bounded"
        );
        // System prompt should be reasonably sized
        assert!(
            prompt.trusted_system[0].len() < 10000,
            "system prompt should not be oversized"
        );
    }

    #[test]
    fn m2_l2_layer_priority_preserves_l0_l1() {
        // M2-L2-3: L0/L1 should be ranked before L3 in sorted entries
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        // L0 > L1 > L2 > L3 > L4
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L1));
        assert!(rank(MemoryLayer::L1) > rank(MemoryLayer::L2));
        assert!(rank(MemoryLayer::L2) > rank(MemoryLayer::L3));
        assert!(rank(MemoryLayer::L3) > rank(MemoryLayer::L4));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_reality_binding_injects_only_leased_fact_evidence_into_the_prompt() {
        let home = tempfile::tempdir().expect("temporary config home");
        let registry = StorageRegistry::default_for_config_home(home.path());
        let handle = registry.sqlite_handle("fact").expect("fact handle");
        std::fs::create_dir_all(handle.path.parent().expect("fact parent")).expect("fact parent");
        let connection = SqliteConnectionFactory::default()
            .open_handle(handle)
            .expect("fact database");
        connection
            .execute_batch(
                "CREATE TABLE fact_records (
                    fact_id TEXT PRIMARY KEY,
                    fact_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );",
            )
            .expect("fact schema");
        let mut fact = fact_kernel::FactRecord::new(
            "supply.policy",
            "east allocation requires expedited approval",
        );
        fact.id = fact_kernel::FactId::from_string("primary-turn-fact");
        connection
            .execute(
                "INSERT INTO fact_records (fact_id, fact_type, status, payload_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    fact.id.as_str(),
                    &fact.fact_type,
                    &fact.status,
                    serde_json::to_string(&fact).expect("fact payload"),
                    fact.updated_at.to_rfc3339(),
                ],
            )
            .expect("persist fact");

        let binding = AgentBindingSnapshot {
            binding_id: "binding:primary-reality".to_string(),
            definition_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/explore")
                    .expect("definition id"),
                1,
            )
            .expect("revision ref"),
            definition_digest: "a".repeat(64),
            instructions: "# Test\n".to_string(),
            instance: AgentInstanceRef {
                instance_id: "instance:primary-reality".to_string(),
                role_slot_id: None,
            },
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "test".to_string(),
                allowed_models: vec!["test".to_string()],
                fallback_allowed: false,
            },
            effective_capabilities: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            tool_contract_refs: Vec::new(),
            data_lease: AgentDataLease {
                session_id: "session-primary".to_string(),
                task_id: "task-primary".to_string(),
                team_id: None,
                read_scopes: vec![CognitiveReadScope::Session],
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
                fact_boundaries: Vec::new(),
                fact_refs: vec!["fact:primary-turn-fact".to_string()],
                matrix_snapshot_refs: Vec::new(),
            },
            release: None,
            evaluation: None,
            binding_digest: "b".repeat(64),
        };
        let mut session = Session::new();
        session.session_id = "session-primary".to_string();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_reality_binding(RealityRecallPort::for_config_home(home.path()), binding);

        let prompt = runtime
            .prepare_reality_context("how should east allocation proceed")
            .await;
        let rendered = rendered_prompt(&prompt);
        assert!(rendered.contains("east allocation requires expedited approval"));
        let envelope = runtime.last_context_envelope().expect("context envelope");
        assert!(
            envelope
                .selected
                .iter()
                .any(|item| item.source == ContextSourceKind::Fact)
        );
        let report = runtime
            .last_reality_recall_report()
            .expect("reality recall report");
        assert_eq!(report.sources[0].status, "enabled_and_wired");
        assert_eq!(report.sources[0].selected_count, 1);
    }
}
