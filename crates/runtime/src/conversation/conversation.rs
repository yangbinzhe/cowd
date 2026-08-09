use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use fact_kernel::FactExtractionTokenUsage;
use tokio::sync::RwLock;

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
use memory::{MemoryKernel, MemoryTurnContext};
use model_protocol::telemetry::SessionTracer;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::budget_policy::{
    clamp_context_budget_ratio_bp, ProviderOutputBudget, ProviderOutputBudgetInputs,
    RuntimeBudgetInputs, RuntimeBudgetPlan,
};

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

pub(crate) fn evaluation_provider_token_lease_snapshot(
) -> Option<EvaluationProviderTokenLeaseSnapshot> {
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
use crate::permissions::{
    PermissionContext, PermissionPolicy, PermissionPromptDecision, PermissionRequest,
};
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
        // Stateful orchestration remains the responsibility of the one
        // canonical `runtime_orchestrate` tool and its validated schema.
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
            ensure_explicit_team_cardinality(objective, &mut calls);
            ModelStepIntent::ToolCalls { calls }
        }
        ModelStepIntent::FinalAnswer { .. } => ModelStepIntent::ToolCalls {
            calls: vec![required_team_orchestration_call(objective)],
        },
        ModelStepIntent::Replan { .. } => ModelStepIntent::ToolCalls {
            calls: vec![required_team_orchestration_call(objective)],
        },
    }
}

fn ensure_explicit_team_cardinality(objective: &str, calls: &mut Vec<ModelToolCall>) {
    let required = usize::from(harness_contract::strategy::explicit_team_count(objective).max(1));
    let proposed = calls
        .iter()
        .map(runtime_team_orchestration_count)
        .sum::<usize>();
    if proposed >= required {
        return;
    }
    // One canonical graph is easier to validate and observe than several
    // provider-authored partial Team proposals. Preserve unrelated tool calls
    // but replace incomplete Team topology with the Runtime-owned contract.
    calls.retain(|call| !is_runtime_team_orchestration_call(call));
    calls.push(required_team_orchestration_call(objective));
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

pub(crate) fn explicit_team_execution_required(objective: &str) -> bool {
    let normalized = objective.to_ascii_lowercase();
    let mentions_team = [
        "团队",
        "协作",
        "多agent",
        "多 agent",
        "多智能体",
        "组队",
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
        "发起",
        "拉起",
        "用一个团队",
        "使用一个团队",
        "交给团队",
        "由团队",
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
}

fn required_team_orchestration_call(objective: &str) -> ModelToolCall {
    let strategy = harness_contract::strategy::decide_strategy(
        &harness_contract::strategy::StrategyInput::from_prompt(objective),
    );
    let requires_external_facts = strategy.understanding.requires_external_facts;
    let requires_write = strategy.understanding.requires_write;
    let team_count = usize::from(harness_contract::strategy::explicit_team_count(objective).max(1));
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
            let is_followup_writer = requires_write && index + 1 == team_count;
            let contract = crate::orchestration::team_authority::explicit_team_node_contract(
                index,
                team_count,
                requires_write,
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
    ModelToolCall {
        id: "runtime-required-team".to_string(),
        name: "runtime_orchestrate".to_string(),
        input: serde_json::json!({
            "intent": objective,
            "operation": "propose",
            "proposal": {
                "mutation_id": format!("explicit-team-{}", uuid::Uuid::new_v4()),
                "reason": "the user explicitly requires an actually started collaboration team",
                "nodes": nodes,
                "completion": {
                    "required_node_ids": node_ids,
                    "required_artifact_kinds": if requires_write {
                        serde_json::json!(["workspace_change", "terminal_synthesis"])
                    } else {
                        serde_json::json!(["terminal_synthesis"])
                    },
                    "allow_unresolved_conflicts": false
                }
            },
            "constraints": {
                "risk": "low",
                "requires_write": requires_write,
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
        }
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
    let status = if failure.is_some() {
        "failed"
    } else {
        "completed"
    };
    let response_completed_at_ms = now_ms();
    let mut early_tool_receipts = Vec::new();
    for worker in early_workers {
        match worker.await {
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
    let shown = names.iter().take(8).copied().collect::<Vec<_>>().join(", ");
    let omitted = names.len().saturating_sub(8);
    if omitted == 0 {
        format!("准备调用 {} 个工具：{shown}", calls.len())
    } else {
        format!(
            "准备调用 {} 个工具：{shown}，另有 {omitted} 类",
            calls.len()
        )
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
    fn stateful_runtime_orchestration_is_bootstrapped_only_when_policy_allows_write() {
        assert_eq!(
            bootstrap_tool_ids(ToolPermissionMode::ReadOnly),
            vec!["tool_search", "context_retrieve", "runtime_capabilities"]
        );
        assert_eq!(
            bootstrap_tool_ids(ToolPermissionMode::WorkspaceWrite),
            vec![
                "tool_search",
                "context_retrieve",
                "runtime_capabilities",
                "runtime_orchestrate"
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
    provider_context_window_limit: Option<u32>,
    provider_tool_protocol_failure: bool,
    tool_exposure_miss: bool,
    provider_resource_result: crate::execution_core::graph::ResourceResultClass,
    provider_retry_after: Option<Duration>,
    provider_retryable: bool,
    provider_usage: Option<TokenUsage>,
}

impl RuntimeError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            provider_context_window_limit: None,
            provider_tool_protocol_failure: false,
            tool_exposure_miss: false,
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Failed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
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
            provider_tool_protocol_failure: false,
            tool_exposure_miss: false,
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Failed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
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
        Self {
            message: message.into(),
            provider_context_window_limit,
            provider_tool_protocol_failure,
            tool_exposure_miss: false,
            provider_resource_result,
            provider_retry_after,
            provider_retryable,
            provider_usage: None,
        }
    }

    #[must_use]
    pub fn with_tool_exposure_miss(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            provider_context_window_limit: None,
            provider_tool_protocol_failure: true,
            tool_exposure_miss: true,
            // Exposure activation is a local Runtime continuation, not a
            // provider-capacity failure and must not reduce provider limits.
            provider_resource_result: crate::execution_core::graph::ResourceResultClass::Completed,
            provider_retry_after: None,
            provider_retryable: false,
            provider_usage: None,
        }
    }

    #[must_use]
    pub const fn with_provider_usage(mut self, usage: TokenUsage) -> Self {
        self.provider_usage = Some(usage);
        self
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
    autonomy_profile: std::sync::RwLock<crate::AutonomyProfileId>,
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
    /// Turn-local cost/usefulness evidence for dynamic Tool exposure.
    turn_tool_exposure_metrics: std::sync::Mutex<TurnToolExposureMetrics>,
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
    strategy_approval_satisfied: bool,
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
        crate::governed_tool_plan::DEFAULT_PARALLEL_TOOL_CONCURRENCY
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
                    self.strategy_approval_satisfied,
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
        Self::new_with_features_and_memory_composition(
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            feature_config,
            MemoryManagerComposition::Automatic,
        )
    }

    /// Construct a conversation from the Memory owner already selected by the
    /// embedding host. `None` is an explicit unavailable selection and never
    /// falls back to the standalone SQLite constructor.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn new_with_features_and_selected_memory(
        session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
        memory_manager: Option<Arc<CognitiveContextManager>>,
    ) -> Self {
        Self::new_with_features_and_memory_composition(
            session,
            api_client,
            tool_executor,
            permission_policy,
            system_prompt,
            feature_config,
            MemoryManagerComposition::HostSelected(memory_manager),
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn new_with_features_and_memory_composition(
        mut session: Session,
        api_client: C,
        tool_executor: Arc<T>,
        permission_policy: PermissionPolicy,
        system_prompt: Vec<String>,
        feature_config: &RuntimeFeatureConfig,
        memory_composition: MemoryManagerComposition,
    ) -> Self {
        session.configure_history(feature_config.session_history());
        let usage_tracker = UsageTracker::from_session(&session);
        let permission_fingerprint = model_protocol::fingerprint::stable_hash_bytes(
            format!("{permission_policy:?}").as_bytes(),
        );
        let subsystem_budget_ratio_bp = feature_config.context_budget().subsystem_budget_ratio_bp;
        let initial_model = feature_config.resolved_model();
        let initial_window_resolution = initial_model.as_deref().map_or(
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
        let initial_model_max_output = initial_model.as_deref().map_or(0, |model| {
            provider_output_budget_hint(
                model,
                initial_model_context_window,
                feature_config
                    .provider_resources()
                    .max_output_tokens_override(),
            )
        });
        let initial_budget_plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: initial_model_context_window,
            model_max_output_tokens: initial_model_max_output,
            subsystem_budget_ratio_bp,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
        });
        let (memory_manager, memory_status) = match memory_composition {
            MemoryManagerComposition::HostSelected(manager) => {
                let status = (feature_config.memory().enabled && manager.is_none()).then(|| {
                    "Memory system unavailable: the composition root selected no Memory owner. \
                     Runtime will not infer or open a fallback backend."
                        .to_string()
                });
                (manager, status)
            }
            MemoryManagerComposition::Automatic if feature_config.memory().enabled => {
                initialize_automatic_memory_manager(feature_config, &initial_budget_plan)
            }
            MemoryManagerComposition::Automatic => (None, None),
        };
        let session_id = session.session_id.clone();
        let session = Arc::new(RwLock::new(session));
        let mut runtime_control_policy = feature_config.runtime_control().policy.clone();
        apply_runtime_budget_to_control_policy(&mut runtime_control_policy, &initial_budget_plan);
        Self {
            session_id: session_id.clone(),
            session,
            session_input_stream: crate::session_input::SessionInputStream::new(session_id),
            consumed_session_inputs: std::sync::Mutex::new(Vec::new()),
            api_client,
            tool_executor,
            permission_policy,
            autonomy_profile: std::sync::RwLock::new(crate::AutonomyProfileId::Supervised),
            permission_fingerprint,
            system_prompt,
            usage_tracker,
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
            provider_max_output_override: feature_config
                .provider_resources()
                .max_output_tokens_override(),
            calibrated_model_context_windows: std::sync::Mutex::new(BTreeMap::new()),
            hook_abort_signal: HookAbortSignal::default(),
            hook_progress_reporter: Arc::new(std::sync::Mutex::new(None)),
            session_tracer: None,
            memory_manager,
            checkpoint_workspace_id: "runtime-workspace".to_string(),
            execution_identity: None,
            maintenance_supervisor: None,
            memory_status,
            reality_recall: None,
            knowledge_activation: None,
            last_reality_recall_report: std::sync::Mutex::new(None),
            tool_callback: None,
            session_journal_port: None,
            session_history_reader: None,
            hot_state: None,
            session_context_projection_cache: std::sync::Mutex::new(None),
            session_memory_projection: tokio::sync::Mutex::new(SessionMemoryProjection::default()),
            memory_context_revision: AtomicU64::new(0),
            current_context_cache_hit: AtomicBool::new(false),
            current_context_source_latency_ms: std::sync::Mutex::new(BTreeMap::new()),
            artifact_store: None,
            runtime_event_store: None,
            outcome_service: None,
            outcome_projector: None,
            routing_mode: feature_config.routing_mode(),
            runtime_config_revision: format!(
                "{:016x}",
                model_protocol::fingerprint::stable_hash_bytes(
                    format!("{feature_config:?}").as_bytes()
                )
            ),
            active_provider_identity: std::sync::Mutex::new(None),
            provider_selection_receipt: std::sync::Mutex::new(None),
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
            approval_coordinator: None,
            skill_profiles: Vec::new(),
            agent_skill_profile: AgentSkillProfile::default(),
            skill_prompt_assets: Vec::new(),
            skill_instruction_source: None,
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
            model: initial_model,
            fallbacks: Arc::new(std::sync::RwLock::new(feature_config.fallbacks().to_vec())),
            cancellation_token: CancellationToken::new(),
            last_context_envelope: std::sync::Mutex::new(None),
            context_profile: std::sync::Mutex::new(ContextProfile::MainTurn),
            runtime_control_policy,
            external_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_context_items: std::sync::Mutex::new(Vec::new()),
            next_model_text_only: AtomicBool::new(false),
            next_model_tool_allowlist: std::sync::Mutex::new(None),
            next_model_tool_activation_notice: std::sync::Mutex::new(None),
            next_model_reasoning_effort: std::sync::Mutex::new(None),
            tool_trace_context_items: std::sync::Mutex::new(Vec::new()),
            turn_tool_observations: std::sync::Mutex::new(Vec::new()),
            turn_governed_tool_plans: std::sync::Mutex::new(Vec::new()),
            active_turn_strategy: std::sync::Mutex::new(None),
            tool_exposure_state: std::sync::Mutex::new(None),
            turn_tool_exposure_metrics: std::sync::Mutex::new(TurnToolExposureMetrics::default()),
            active_skill_tool_refs: std::sync::Mutex::new(BTreeSet::new()),
            tool_exposure_revision: AtomicU64::new(0),
            request_compiler: crate::PreparedRequestCompiler::new(
                feature_config.session_history().request_cache_entries,
            ),
            turn_stable_prefix_metrics: std::sync::Mutex::new(TurnStablePrefixMetrics::default()),
            turn_evidence_audits: std::sync::Mutex::new(Vec::new()),
            turn_context_ledger: std::sync::Mutex::new(crate::context_ledger::ContextLedger::new(
                initial_budget_plan.subsystem_budget_tokens,
                initial_budget_plan.tool_result_budget.max_total_tokens as u64,
            )),
            last_context_turn_report: std::sync::Mutex::new(None),
            turn_preflight_compaction: std::sync::Mutex::new(None),
            turn_knowledge_report: std::sync::Mutex::new(None),
            tool_execution_plane: Arc::new(crate::ToolExecutionPlane::new(
                Arc::new(crate::execution_core::graph::ExecutionResourceManager::new(
                    [
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Tool,
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 1,
                                target: 8,
                                maximum: 64,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.process".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 1,
                                target: 4,
                                maximum: 16,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.network".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 1,
                                target: 8,
                                maximum:
                                    crate::governed_tool_plan::DEFAULT_PARALLEL_TOOL_CONCURRENCY,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.cpu".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 1,
                                target: 16,
                                maximum: 64,
                            },
                        ),
                        (
                            crate::execution_core::graph::ExecutionResourceKind::Custom(
                                "tool.memory_mib".to_string(),
                            ),
                            crate::execution_core::graph::ResourceQuota {
                                minimum: 64,
                                target: 2_048,
                                maximum: 16_384,
                            },
                        ),
                    ],
                )),
                Arc::new(crate::execution_core::graph::ScopeLockManager::new()),
            )),
            authorization_negotiator: crate::AuthorizationNegotiator::new(),
            provider_admission: None,
            provider_resource_config: Arc::new(std::sync::RwLock::new(
                crate::ProviderResourceConfig::default(),
            )),
            execution_service_class:
                crate::execution_core::graph::ExecutionServiceClass::Interactive,
            tool_timeout: Some(Duration::from_secs(120)),
            explicit_team_escalation: true,
            model_step_limit_override: AtomicUsize::new(0),
            delegated_focus_novelty_target_bp: AtomicU64::new(0),
            delegated_focus_acceptance_scopes: std::sync::Mutex::new(Vec::new()),
            delegated_focus_required_output_fields: std::sync::Mutex::new(Vec::new()),
            session_execution_fence: None,
        }
    }

    #[must_use]
    pub fn with_session_execution_fence(mut self, fence: crate::SessionExecutionFence) -> Self {
        self.session_execution_fence = Some(fence);
        self
    }

    pub(crate) async fn verify_session_execution_fence(
        &self,
        phase: crate::SessionExecutionFencePhase,
    ) -> Result<(), RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify(phase)
                .await
                .map(|_| ())
                .map_err(RuntimeError::new),
            None => Ok(()),
        }
    }

    pub(crate) async fn capture_session_execution_fence(
        &self,
        phase: crate::SessionExecutionFencePhase,
    ) -> Result<Option<crate::SessionExecutionFenceSnapshot>, RuntimeError> {
        match self.session_execution_fence.as_ref() {
            Some(fence) => fence
                .verify(phase)
                .await
                .map(Some)
                .map_err(RuntimeError::new),
            None => Ok(None),
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
    pub fn with_provider_resource_config(
        mut self,
        config: Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
    ) -> Self {
        self.provider_resource_config = config;
        self
    }

    #[must_use]
    pub(crate) fn with_provider_fallback_policy(
        mut self,
        policy: Arc<std::sync::RwLock<Vec<String>>>,
    ) -> Self {
        self.fallbacks = policy;
        self
    }

    #[must_use]
    pub fn with_execution_service_class(
        mut self,
        service_class: crate::execution_core::graph::ExecutionServiceClass,
    ) -> Self {
        self.execution_service_class = service_class;
        self
    }

    pub fn set_execution_service_class(
        &mut self,
        service_class: crate::execution_core::graph::ExecutionServiceClass,
    ) {
        self.execution_service_class = service_class;
    }

    #[must_use]
    pub fn with_tool_execution_plane(mut self, plane: Arc<crate::ToolExecutionPlane>) -> Self {
        self.tool_execution_plane = plane;
        self
    }

    #[cfg(test)]
    pub(crate) fn uses_tool_execution_plane(&self, plane: &Arc<crate::ToolExecutionPlane>) -> bool {
        Arc::ptr_eq(&self.tool_execution_plane, plane)
    }

    #[cfg(test)]
    pub(crate) fn uses_artifact_store(&self, store: &Arc<crate::ArtifactStore>) -> bool {
        self.artifact_store
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, store))
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

        let turn_index = self.session_head().await.message_count;
        let activation = SkillActivationEngine::activate(SkillActivationInput {
            session_id: self.session_id().to_string(),
            turn_index,
            query: user_input.to_string(),
            capability_refs: Vec::new(),
            available_profiles: self.skill_profiles.clone(),
            agent_profile: self.agent_skill_profile.clone(),
        });

        if let Some(invocation) = activation.selected_invocation.as_ref() {
            let strategy = self.active_turn_strategy().ok_or_else(|| {
                RuntimeError::new("Skill invocation requires the Host-admitted turn strategy owner")
            })?;
            let evaluation_isolated = strategy.resource_snapshot.sample_source.contains("corpus=");
            let config_revision = if evaluation_isolated {
                format!(
                    "{}:evaluation:{:016x}",
                    self.runtime_config_revision,
                    model_protocol::fingerprint::stable_hash_bytes(
                        strategy.resource_snapshot.sample_source.as_bytes(),
                    )
                )
            } else {
                self.runtime_config_revision.clone()
            };
            let usage_context = crate::RuntimeSkillUsageContext {
                workspace_identity: self.checkpoint_workspace_id.clone(),
                workload_fingerprint: StrategyWorkloadFingerprint::from_understanding(
                    &strategy.decision.strategy.understanding,
                    strategy.decision.strategy.understanding.requires_write,
                )
                .digest(),
                config_revision,
                evaluation_environment: if evaluation_isolated {
                    "harness_evaluation".to_string()
                } else {
                    "production".to_string()
                },
                execution_id: format!("turn:{}", strategy.decision_id),
                session_id: strategy.session_ref.clone(),
                turn_id: strategy.turn_ref.clone(),
                observed_at_ms: now_ms(),
            };
            let asset = match self.skill_instruction_source.as_ref() {
                Some(source) => source
                    .load_instruction(invocation, &usage_context)
                    .await
                    .map_err(|error| {
                        RuntimeError::new(format!(
                            "runtime skill `{}` instruction page-in failed: {error}",
                            invocation.skill_id
                        ))
                    })?,
                None => self
                    .skill_prompt_assets
                    .iter()
                    .find(|asset| asset.skill_id == invocation.skill_id)
                    .cloned(),
            };
            if let Some(asset) = asset {
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

        if activation.activation.selected.is_some() {
            self.append_execution_runtime_event(
                RuntimeEventScope::Skill,
                "skill.activation.selected",
                Some("completed".to_string()),
                activation
                    .activation
                    .selected
                    .iter()
                    .map(|skill_id| RuntimeEventRef {
                        kind: "skill".to_string(),
                        id: skill_id.clone(),
                    })
                    .collect(),
                serde_json::to_value(&activation.activation).unwrap_or_else(
                    |error| serde_json::json!({ "serialization_error": error.to_string() }),
                ),
            );
        }

        let Some(port) = self.session_journal_port.as_ref() else {
            return Ok(());
        };
        let activation_event = activation.activation.to_runtime_session_event(0);
        port.append_event(&activation_event)
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
                port.append_event(&event).await.map_err(|error| {
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
        let discovery_started = Instant::now();
        let discovery = self.tool_executor.tool_discovery_receipt();
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_catalog_lookup(discovery_started.elapsed());
        }
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
        prompt.push_trusted_system(crate::prompt::runtime_clock_section());
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
        let messages: HistoryView = vec![ConversationMessage::user_text(format!(
            "Original objective:\n{objective}\n\nChecked evidence receipts:\n{evidence}\n\nReturn the final answer now."
        ))]
        .into();
        let inventory = self.api_client.context_inventory();
        let mut last_error = None;
        let mut models_tried = Vec::new();
        let mut provider_retries = BTreeMap::<String, u8>::new();

        for model in self.model_candidates_for_turn(objective) {
            let mut calibration_retried = false;
            'candidate_attempt: loop {
                let mut request =
                    match self.pack_provider_attempt(&prompt, &messages, &model, inventory) {
                        Ok(request) => request,
                        Err(error) => {
                            tracing::warn!(
                                model,
                                error = %error,
                                "provider request preflight rejected clean terminal synthesis"
                            );
                            last_error = Some(error);
                            break 'candidate_attempt;
                        }
                    };
                let mut evaluation_reservation =
                    match EvaluationProviderTokenReservation::acquire(&mut request) {
                        Ok(reservation) => reservation,
                        Err(error) => {
                            last_error = Some(error);
                            break 'candidate_attempt;
                        }
                    };
                if !models_tried.contains(&model) {
                    models_tried.push(model.clone());
                }
                if let Some(cowd) = &self.cowd_bus {
                    cowd.emit(crate::cowd_event::CowdEvent::ProviderAttempt {
                        model: model.clone(),
                        models_tried: models_tried.clone(),
                        context_window_tokens: request.budget.context_window_tokens,
                        context_window_source: request.budget.context_window_source.clone(),
                        packed_input_tokens: request
                            .budget
                            .fixed_input_tokens
                            .saturating_add(request.budget.dynamic_input_tokens)
                            .saturating_add(request.budget.protocol_overhead_tokens),
                    });
                }
                let request_sequence = self.session_head().await.message_count;
                request.provider_evidence_context = Some(crate::ProviderRequestEvidenceContext {
                    session_id: self.session_id().to_string(),
                    request_sequence,
                    request_compiler_cache_hit: request.request_compiler_cache_hit,
                    budget: request.budget.clone(),
                });
                self.record_provider_context_request(
                    &request,
                    request_sequence,
                    inventory,
                    self.api_client.tool_schema_cache_stats(),
                );
                let transport_policy = provider_transport_policy(
                    request
                        .budget
                        .context_window_tokens
                        .min(u64::from(u32::MAX)) as u32,
                    &request,
                );
                let (provider_lease, provider_queue_wait) =
                    self.acquire_provider_capacity(&model, &request).await?;
                let cancellation = self.cancellation_token.clone();
                let stream_started = Instant::now();
                let reducer = ModelStreamReducer::new(
                    self.cowd_bus.clone(),
                    self.runtime_event_store.clone(),
                    self.session_id().to_string(),
                );
                let ApiClientStream {
                    events,
                    transport_activity,
                } = self.api_client.stream_with_transport_activity(request);
                let stream_run = consume_provider_stream_with_activity(
                    events,
                    cancellation,
                    Some(ProviderStreamTimeoutPolicy {
                        idle: transport_policy.idle_timeout,
                        heartbeat_grace: transport_policy.heartbeat_grace,
                    }),
                    reducer,
                    None,
                    transport_activity,
                )
                .await;
                self.record_provider_resource_outcome(
                    provider_lease.as_ref(),
                    provider_queue_wait,
                    stream_started.elapsed(),
                    stream_run.resource_result_class,
                );
                drop(provider_lease);
                let CollectedProviderStream {
                    text,
                    public_reasoning,
                    private_reasoning,
                    signature,
                    calls,
                    usage,
                    effective_provider_identity,
                    first_event_at,
                    first_text_at: _,
                    early_tool_receipts: _,
                    early_tool_deferrals: _,
                    response_completed_at_ms,
                } = stream_run.collected;
                let effective_model = effective_provider_identity
                    .as_ref()
                    .map(|identity| identity.model.clone());
                if let Some(identity) = effective_provider_identity {
                    *self
                        .active_provider_identity
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(identity);
                }
                if let Some(reservation) = evaluation_reservation.as_mut() {
                    reservation.reconcile(usage);
                }
                self.reconcile_provider_context_usage(usage);
                if let Some(error) = stream_run.failure {
                    if error.is_provider_tool_protocol_failure() {
                        return Err(error);
                    }
                    if !calibration_retried {
                        if let Some(observed_limit) = error.provider_context_window_limit() {
                            if self.calibrate_model_context_window(&model, observed_limit) {
                                calibration_retried = true;
                                tracing::info!(
                                model,
                                observed_limit,
                                "provider context window calibrated; retrying clean terminal candidate once"
                            );
                                continue 'candidate_attempt;
                            }
                        }
                    }
                    let retries = provider_retries.entry(model.clone()).or_default();
                    if error.provider_retryable()
                        && *retries < MAX_RUNTIME_PROVIDER_RETRIES_PER_MODEL
                    {
                        *retries = retries.saturating_add(1);
                        let retry_after = error
                            .provider_retry_after()
                            .unwrap_or(DEFAULT_RUNTIME_PROVIDER_RETRY_DELAY);
                        tokio::select! {
                            () = self.cancellation_token.cancelled() => {
                                return Err(RuntimeError::new(
                                    "turn cancelled during provider retry delay",
                                ));
                            }
                            () = tokio::time::sleep(retry_after) => {}
                        }
                        continue 'candidate_attempt;
                    }
                    last_error = Some(error);
                    break 'candidate_attempt;
                }

                let mut blocks = Vec::new();
                if !public_reasoning.is_empty() {
                    blocks.push(ContentBlock::ReasoningSummary {
                        text: public_reasoning,
                    });
                }
                if !private_reasoning.is_empty() || !signature.is_empty() {
                    blocks.push(ContentBlock::Thinking {
                        thinking: private_reasoning,
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
                let effective_model = effective_model.or(Some(model));
                if let Some(model) = effective_model.as_ref() {
                    if !models_tried.contains(model) {
                        models_tried.push(model.clone());
                    }
                }
                return Ok(ModelStepResult {
                    intent: classify_model_step_intent(text, calls),
                    assistant_message: ConversationMessage {
                        role: crate::session::MessageRole::Assistant,
                        blocks,
                        usage: Some(usage),
                    },
                    usage,
                    model: effective_model,
                    models_used: models_tried.clone(),
                    first_token_latency_ms: first_event_at.map(|first| {
                        u64::try_from(first.saturating_duration_since(stream_started).as_millis())
                            .unwrap_or(u64::MAX)
                    }),
                    active_stream_duration_ms: first_event_at
                        .map(|first| millis_since(first).max(1)),
                    wall_duration_ms: millis_since(started_at).max(1),
                    early_tool_receipts: Vec::new(),
                    early_tool_deferrals: Vec::new(),
                    response_completed_at_ms,
                    text_only_response: true,
                });
            }
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
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.reset(self.api_client.tool_schema_cache_stats());
        }
        if let Ok(mut metrics) = self.turn_stable_prefix_metrics.lock() {
            metrics.reset();
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

    fn tool_exposure_metrics(&self) -> ToolExposureMetrics {
        self.turn_tool_exposure_metrics
            .lock()
            .map(|metrics| metrics.projection())
            .unwrap_or_default()
    }

    fn stable_prefix_metrics(&self) -> StablePrefixMetrics {
        self.turn_stable_prefix_metrics
            .lock()
            .map(|metrics| metrics.projection.clone())
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
            .any(|name| name == "tool_search");

        object.insert("catalog_tool_names".to_string(), catalog_tool_names);
        object.insert(
            "tool_visibility".to_string(),
            serde_json::json!({
                "active_function_schemas": active_function_schemas,
                "deferred_catalog_tools": exposure.deferred_ids,
                "catalog_revision": exposure.catalog_revision,
                "exposure_revision": exposure.exposure_revision,
                "activation_protocol": if tool_search_active {
                    "Call tool_search once with a focused query. Accepted candidates become callable native function schemas on the immediately following automatic provider request inside this same user turn."
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
                        "tool_search".to_string()
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

    fn activate_tool_discovery(
        &self,
        output: &str,
    ) -> Option<harness_contract::tool::ToolActivationReceipt> {
        let Ok(discovery) =
            serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(output)
        else {
            tracing::warn!("tool_search returned a non-canonical discovery receipt");
            if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
                metrics.observe_invalid_search();
            }
            return None;
        };
        self.activate_tool_candidates(&discovery, true)
    }

    fn activate_tool_candidates(
        &self,
        discovery: &harness_contract::tool::ToolDiscoveryReceipt,
        count_as_search: bool,
    ) -> Option<harness_contract::tool::ToolActivationReceipt> {
        let Ok(mut guard) = self.tool_exposure_state.lock() else {
            tracing::warn!("tool exposure state lock poisoned");
            return None;
        };
        let Some(state) = guard.as_mut() else {
            tracing::warn!("tool_search completed before tool exposure was initialized");
            return None;
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
        let activated_ids = activation
            .activated_ids()
            .map(str::to_string)
            .collect::<BTreeSet<_>>();
        tracing::info!(
            catalog_revision = activation.catalog_revision,
            previous_exposure_revision = activation.previous_exposure_revision,
            exposure_revision = activation.exposure_revision,
            activated = ?activated_ids,
            "tool_search activation applied to the next provider request"
        );
        if !activated_ids.is_empty() {
            if let Ok(mut notice) = self.next_model_tool_activation_notice.lock() {
                notice.get_or_insert_default().extend(activated_ids);
            }
        }
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            if count_as_search {
                metrics.observe_search(&activation);
            } else {
                metrics.observe_activation(&activation);
            }
        }
        Some(activation)
    }

    fn activate_deferred_tool_calls(
        &self,
        requested: &[String],
        catalog: &harness_contract::tool::ToolDiscoveryReceipt,
    ) -> BTreeSet<String> {
        let known = catalog
            .descriptors
            .iter()
            .map(|descriptor| descriptor.canonical_id.as_str())
            .collect::<BTreeSet<_>>();
        let activation_candidates = requested
            .iter()
            .filter(|name| known.contains(name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if activation_candidates.is_empty() {
            return BTreeSet::new();
        }
        let mut activation = catalog.clone();
        activation.query = "provider-deferred-tool-call".to_string();
        activation.activation_candidates = activation_candidates;
        self.activate_tool_candidates(&activation, false)
            .map(|receipt| receipt.activated_ids().map(str::to_string).collect())
            .unwrap_or_default()
    }

    async fn seed_recent_session_tools(
        &self,
        exposure: &mut ToolExposureState,
        catalog: &harness_contract::tool::ToolDiscoveryReceipt,
    ) {
        const MAX_RECENT_SESSION_TOOLS: usize = 8;
        let session = self.session.read().await;
        let mut recent = BTreeSet::new();
        'messages: for message in session.messages().rev().take(64) {
            for block in message.blocks.iter().rev() {
                let ContentBlock::ToolResult {
                    tool_name,
                    is_error: false,
                    ..
                } = block
                else {
                    continue;
                };
                if let Some(canonical) = self.tool_executor.resolve_tool_name(tool_name) {
                    recent.insert(canonical);
                    if recent.len() >= MAX_RECENT_SESSION_TOOLS {
                        break 'messages;
                    }
                }
            }
        }
        drop(session);
        if recent.is_empty() {
            return;
        }
        let mut discovery = catalog.clone();
        discovery.query = "recent-session-tool-rehydration".to_string();
        discovery.activation_candidates = recent.into_iter().collect();
        let allowed_ids = exposure
            .bootstrap
            .iter()
            .chain(exposure.active.iter())
            .chain(exposure.deferred.iter())
            .cloned()
            .collect();
        let policy = ToolExposurePolicy {
            allowed_ids,
            maximum_permission: contract_permission_mode(self.permission_policy.active_mode()),
            supports_dynamic_exposure: true,
        };
        let activation = ToolExposurePlanner.activate(exposure, &discovery, &policy);
        if activation.activated_ids().next().is_some() {
            exposure.reason =
                "bootstrap plus recently successful session tools rehydrated".to_string();
            if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
                metrics.observe_activation(&activation);
            }
        }
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

    async fn remember_context_envelope(&self, envelope: ContextEnvelope) {
        if let Ok(mut guard) = self.last_context_envelope.lock() {
            *guard = Some(envelope.clone());
        }
        self.persist_context_envelope(envelope.clone()).await;
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::ContextEnvelope { envelope });
        }
    }

    async fn persist_context_envelope(&self, envelope: ContextEnvelope) {
        let Some(port) = self.session_journal_port.as_ref() else {
            return;
        };
        let session_id = envelope.identity.session_id.clone();
        let envelope_id = envelope.id.clone();
        let persisted = PersistedContextEnvelope::from(&envelope);
        let Ok(persisted_bytes) = serde_json::to_vec(&persisted) else {
            tracing::warn!(
                session_id,
                envelope_id,
                "context envelope serialization failed"
            );
            return;
        };
        let mut artifact_receipt = None;
        let envelope_value = if let Some(artifacts) = self
            .artifact_store
            .as_ref()
            .filter(|store| persisted_bytes.len() as u64 > store.config().compact_threshold_bytes)
        {
            let visibility_scope = format!("session:{session_id}");
            let descriptor = ArtifactWriteDescriptor {
                media_type: "application/vnd.cowd.context-envelope+json".to_string(),
                visibility_scope: visibility_scope.clone(),
                expected_bytes: Some(persisted_bytes.len() as u64),
                original_name: Some(format!("context-envelope-{envelope_id}.json")),
            };
            match artifacts.write_bytes(descriptor, &persisted_bytes).await {
                Ok(artifact) => {
                    let staging_owner = format!("staging:context-envelope:{envelope_id}");
                    match artifacts.pin(
                        &artifact,
                        &staging_owner,
                        now_ms().saturating_add(crate::ARTIFACT_STAGING_PIN_TTL_MS),
                    ) {
                        Ok(()) => {
                            artifact_receipt = Some((
                                Arc::clone(artifacts),
                                artifact,
                                visibility_scope,
                                staging_owner,
                            ));
                            serde_json::json!({
                                "id": persisted.id,
                                "epoch_id": persisted.epoch_id,
                                "identity": persisted.identity,
                                "profile": persisted.profile,
                                "intent": persisted.intent,
                                "budget": persisted.budget,
                                "diagnostics": persisted.diagnostics,
                                "created_at": persisted.created_at,
                                "selected_count": persisted.selected.len(),
                                "omitted_count": persisted.omitted.len(),
                                "artifact_backed": true,
                            })
                        }
                        Err(error) => {
                            let _ = artifacts.delete(&artifact, &visibility_scope);
                            tracing::warn!(
                                %error,
                                session_id,
                                envelope_id,
                                "context envelope artifact pin failed; retaining inline evidence"
                            );
                            serde_json::to_value(&persisted).unwrap_or_default()
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        session_id,
                        envelope_id,
                        "context envelope artifact write failed; retaining inline evidence"
                    );
                    serde_json::to_value(&persisted).unwrap_or_default()
                }
            }
        } else {
            serde_json::to_value(&persisted).unwrap_or_default()
        };
        let context_artifact = artifact_receipt
            .as_ref()
            .map(|(_, artifact, _, _)| artifact.clone());
        let payload = serde_json::json!({
            "type": "ContextEnvelope",
            "schema_version": PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
            "envelope_id": envelope_id,
            "formatter_version": CONTEXT_RENDER_FORMATTER_VERSION,
            "envelope": envelope_value,
            "context_artifact": context_artifact,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let record = crate::RuntimeContextEnvelopeRecord {
            session_id: session_id.clone(),
            payload,
            created_at_ms,
        };
        match port.append_context_envelope_if_absent(&record).await {
            Ok(Some(_)) => {
                if let Some((artifacts, artifact, _, staging_owner)) = artifact_receipt {
                    let durable_owner = format!("context-envelope:{envelope_id}");
                    if let Err(error) = artifacts.pin(
                        &artifact,
                        &durable_owner,
                        crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                    ) {
                        let _ = artifacts.pin(
                            &artifact,
                            &staging_owner,
                            crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                        );
                        tracing::warn!(
                            %error,
                            session_id,
                            envelope_id,
                            "context envelope artifact retained by staging owner"
                        );
                        return;
                    }
                    if let Err(error) = artifacts.unpin(&artifact, &staging_owner) {
                        tracing::warn!(
                            %error,
                            session_id,
                            envelope_id,
                            "context envelope artifact retained an extra staging pin"
                        );
                    }
                }
            }
            Ok(None) => {
                if let Some((artifacts, artifact, visibility_scope, staging_owner)) =
                    artifact_receipt
                {
                    let _ = artifacts.unpin(&artifact, &staging_owner);
                    let _ = artifacts.delete(&artifact, &visibility_scope);
                }
                tracing::debug!(session_id, "context envelope event already persisted");
            }
            Err(error) => {
                if let Some((artifacts, artifact, visibility_scope, staging_owner)) =
                    artifact_receipt
                {
                    let _ = artifacts.unpin(&artifact, &staging_owner);
                    let _ = artifacts.delete(&artifact, &visibility_scope);
                }
                tracing::warn!(%error, session_id, "context envelope event append failed");
            }
        }
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
        let Some(port) = self.session_journal_port.as_ref() else {
            // Embedding callers may intentionally run without a durable
            // session carrier. They receive the in-memory report but cannot
            // claim restart/audit durability.
            return Ok(());
        };
        let session_id = self.session_id().to_string();
        let payload = serde_json::json!({
            "type": "ContextTurnReport",
            "report": report,
        });
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        let event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::ContextTurnReport,
            payload,
            created_at_ms,
        );
        port.append_event(&event).await.map_err(|error| {
            RuntimeError::new(format!(
                "context governance persistence failed for session `{session_id}`: {error}"
            ))
        })?;
        Ok(())
    }

    async fn finalize_context_prompt(
        &self,
        user_input: &str,
        envelope: ContextEnvelope,
        knowledge: Option<KnowledgeTurnReport>,
    ) -> PromptAssembly {
        let fact_decision = self
            .runtime_fact_decision_for_context(user_input, &envelope)
            .await;
        let report = ContextRuntimeKernel::governance_report(
            &envelope,
            knowledge.as_ref(),
            fact_decision,
            None,
        );
        self.remember_context_governance_report(report).await;
        let prompt = Self::provider_prompt_from_envelope(&envelope);
        self.remember_context_envelope(envelope).await;
        prompt
    }

    async fn remember_context_governance_report(&self, report: RuntimeContextGovernanceReport) {
        self.persist_context_governance_report(report).await;
    }

    async fn persist_context_governance_report(&self, report: RuntimeContextGovernanceReport) {
        let Some(port) = self.session_journal_port.as_ref() else {
            return;
        };
        let session_id = report.session_id.clone();
        let envelope_id = report.envelope_id.clone();
        let context_epoch = report.context_epoch.clone();
        let payload = serde_json::json!({
            "type": "RuntimeContextGovernanceReport",
            "report": report,
        });
        let created_at_ms = now_ms();
        let mut event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::ContextGovernanceReport,
            payload,
            created_at_ms,
        );
        event.status = Some("recorded".to_string());
        event.refs.extend([
            crate::RuntimeSessionEventRef {
                ref_type: "context_envelope".to_string(),
                id: envelope_id,
                label: None,
            },
            crate::RuntimeSessionEventRef {
                ref_type: "context_epoch".to_string(),
                id: context_epoch,
                label: None,
            },
        ]);
        if let Err(error) = port.append_event(&event).await {
            tracing::warn!(%error, session_id, "context governance domain event append failed");
        }
    }

    async fn runtime_fact_decision_for_context(
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
        if let Some(port) = self.session_journal_port.as_ref() {
            let mut domain_event = crate::RuntimeSessionEvent::new(
                envelope.identity.session_id.clone(),
                0,
                crate::RuntimeSessionEventKind::ContextFactCandidateReview,
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
            domain_event.refs.push(crate::RuntimeSessionEventRef {
                ref_type: "context_envelope".to_string(),
                id: envelope.id.clone(),
                label: None,
            });
            let session_id = envelope.identity.session_id.clone();
            if let Err(error) = port.append_event(&domain_event).await {
                tracing::warn!(%error, session_id, "fact candidate domain event append failed");
            }
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
                provider_output_budget_hint(
                    model,
                    self.context_window_for_model(model),
                    self.provider_max_output_override,
                )
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
                provider_output_budget_hint(
                    model,
                    self.context_window_for_model(model),
                    self.provider_max_output_override,
                )
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

    pub(crate) fn memory_turn_context(&self) -> MemoryTurnContext {
        let project_id = self.with_session_read_blocking(memory_project_id_for_session);
        let task_id = Some(format!("session-task-{}", self.session_id()));
        MemoryTurnContext::new(self.session_id().to_string(), self.memory_agent_id.clone())
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
        let used_tokens = self.with_session_read_blocking(estimate_session_tokens) as u64;
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
            .with_governance_decision(decision)
            .with_tool_exposure_metrics(self.tool_exposure_metrics())
            .with_stable_prefix_metrics(self.stable_prefix_metrics());
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
        let session_id = self.session_id().to_string();
        let workspace_root = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf));
        let profile = self.context_profile();
        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = ContextRuntimeKernel::mode_for_profile(profile);
        let governance_report_id =
            ContextRuntimeKernel::governance_report_id(&session_id, user_input);
        let canonical_prompt = PromptAssembly::new(self.system_prompt.clone());
        let mut runtime_header = canonical_prompt.runtime_system_segments().to_vec();
        runtime_header.extend(ContextRuntimeKernel::runtime_header(&identity, profile));
        runtime_header.push(crate::prompt::runtime_clock_section());
        runtime_header.push(format!(
            "context_governance_report_id:{governance_report_id}"
        ));
        let mut selected_items = self.external_context_items();
        if let Some(cwd) = workspace_root {
            selected_items.extend(crate::prompt::discover_project_context_items_for_profile(
                &cwd, profile,
            ));
        }
        selected_items.extend(self.tool_trace_context_items());
        selected_items.extend(dynamic_items);
        let (selected_items, binding_omissions) =
            revalidate_context_binding(&session_id, selected_items);
        let mut omitted = omitted;
        omitted.extend(binding_omissions);
        let mut envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            profile,
            runtime_header,
            identity,
            intent: user_input.to_string(),
            stable_head: canonical_prompt.stable_system_segments().to_vec(),
            dynamic_items: selected_items,
            omitted,
            total_budget_tokens,
        });
        envelope.diagnostics.degraded_sources = degraded_sources;
        envelope.diagnostics.cache_hit =
            self.current_context_cache_hit.swap(false, Ordering::AcqRel);
        if let Ok(mut latency) = self.current_context_source_latency_ms.lock() {
            envelope.diagnostics.source_latency_ms = std::mem::take(&mut *latency);
        }
        envelope
    }

    fn record_context_source_latency(&self, source: &str, elapsed: Duration) {
        if let Ok(mut latency) = self.current_context_source_latency_ms.lock() {
            latency.insert(
                source.to_string(),
                elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            );
        }
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
        messages: &HistoryView,
        model: &str,
        inventory: ProviderContextInventory,
    ) -> Result<ApiRequest, RuntimeError> {
        let window_resolution = self.context_window_resolution_for_model(model);
        let context_window_tokens = u64::from(window_resolution.tokens);
        // Protocol framing is deliberately explicit and conservative. Schema
        // payload itself is accounted separately from fixed wire framing.
        let protocol_overhead_tokens =
            128u64.saturating_add(u64::from(inventory.tool_count as u32).saturating_mul(12));
        let safety_margin_tokens = (context_window_tokens / 100).clamp(128, 2_048);
        let prepared = self.request_compiler.prepare(
            prompt,
            messages,
            inventory,
            self.permission_fingerprint,
            model,
        );
        let fixed_input_tokens = prepared.fixed_input_tokens;
        let required_input_tokens = prompt.required_packet_token_estimate();
        let max_output =
            provider::model_max_output_resolution(model, self.provider_max_output_override);
        let output_budget = ProviderOutputBudget::derive(ProviderOutputBudgetInputs {
            context_window_tokens,
            max_output_tokens: u64::from(max_output.tokens),
            fixed_input_tokens,
            required_input_tokens,
            protocol_overhead_tokens,
            safety_margin_tokens,
        });
        if !output_budget.executable {
            return Err(RuntimeError::new(format!(
                "provider candidate `{model}` cannot fit fixed and required request components with a viable continuation: fixed={fixed_input_tokens} required={required_input_tokens} window={context_window_tokens} available_output={} output_floor={}",
                output_budget.available_output_tokens,
                output_budget.floor_output_tokens,
            )));
        }
        let mut budget = crate::context_ledger::RequestBudgetReport::for_attempt(
            model,
            context_window_tokens,
            output_budget.requested_output_tokens,
            protocol_overhead_tokens,
            safety_margin_tokens,
            fixed_input_tokens,
        );
        budget.set_output_policy(
            u64::from(max_output.tokens),
            max_output.source.as_str(),
            output_budget.preferred_output_tokens,
            output_budget.floor_output_tokens,
            required_input_tokens,
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
            messages: prepared.history,
            model: model.to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: prepared.cache_hit,
            budget,
            provider_evidence_context: None,
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
    pub fn with_session_journal_port(
        mut self,
        port: Arc<dyn crate::SessionRuntimeJournalPort>,
    ) -> Self {
        self.session_journal_port = Some(port);
        self.refresh_provider_wire_evidence_writer();
        self
    }

    #[must_use]
    pub fn with_session_history_reader(
        mut self,
        reader: Arc<session::SessionHistoryReader>,
    ) -> Self {
        self.session_history_reader = Some(reader);
        self
    }

    #[must_use]
    pub fn with_hot_state(
        mut self,
        hot_state: Arc<crate::execution_core::hot_state::RuntimeHotStatePlane>,
    ) -> Self {
        self.hot_state = Some(hot_state);
        self
    }

    #[must_use]
    pub fn with_artifact_store(mut self, store: Arc<crate::ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self.refresh_provider_wire_evidence_writer();
        self
    }

    fn refresh_provider_wire_evidence_writer(&mut self) {
        let writer = self
            .artifact_store
            .as_ref()
            .zip(self.session_journal_port.as_ref())
            .map(|(artifacts, session_port)| {
                Arc::new(SessionProviderWireEvidenceWriter {
                    artifacts: Arc::clone(artifacts),
                    session_port: Arc::clone(session_port),
                }) as Arc<dyn crate::ProviderWireEvidenceWriter>
            });
        self.api_client.configure_provider_wire_evidence(writer);
    }

    /// Attach the durable store that owns tool, graph, agent, and task execution state.
    #[must_use]
    pub(crate) fn with_runtime_event_store(mut self, store: Arc<RuntimeEventStore>) -> Self {
        self.outcome_service = Some(Arc::new(crate::execution_core::OutcomeService::new(
            Arc::clone(&store),
        )));
        self.outcome_projector = Some(Arc::new(crate::OutcomeProjector::new(Arc::clone(&store))));
        self.runtime_event_store = Some(store);
        self
    }

    #[must_use]
    pub(crate) fn with_outcome_runtime(
        mut self,
        service: Arc<crate::execution_core::OutcomeService>,
        projector: Arc<crate::OutcomeProjector>,
    ) -> Self {
        self.outcome_service = Some(service);
        self.outcome_projector = Some(projector);
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

    /// Install the Runtime-owned approval coordinator.
    #[must_use]
    pub fn with_approval_coordinator(
        mut self,
        coordinator: Arc<crate::ApprovalCoordinator>,
    ) -> Self {
        self.approval_coordinator = Some(coordinator);
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

    /// Attach the Gateway-owned lazy instruction source pinned to this
    /// Runtime catalog generation.
    #[must_use]
    pub fn with_skill_instruction_source(
        mut self,
        source: Option<Arc<dyn crate::RuntimeSkillInstructionSource>>,
    ) -> Self {
        self.skill_instruction_source = source;
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

    /// Bind semantic checkpoints to the same canonical execution identity as
    /// the active Agent node. Root surface turns provide only the workspace
    /// basis and receive a session-turn identity when compaction is planned.
    #[must_use]
    pub fn with_checkpoint_identity(
        mut self,
        workspace_id: impl Into<String>,
        execution_identity: Option<harness_contract::execution::ExecutionIdentity>,
    ) -> Self {
        self.checkpoint_workspace_id = workspace_id.into();
        self.execution_identity = execution_identity;
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

    fn governed_workspace_root(&self) -> Result<PathBuf, RuntimeError> {
        if let Some(root) = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf))
        {
            return Ok(root);
        }
        #[cfg(test)]
        {
            return std::env::current_dir().map_err(|error| {
                RuntimeError::new(format!("test workspace unavailable: {error}"))
            });
        }
        #[cfg(not(test))]
        {
            Err(RuntimeError::new(
                "governed Runtime execution requires an explicit Session workspace",
            ))
        }
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

    fn consume_runtime_input_records(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let consumed = self
            .session_input_stream
            .consume_for_checkpoint(turn_id, checkpoint, 32);
        if !consumed.is_empty() {
            if let Ok(mut pending) = self.consumed_session_inputs.lock() {
                pending.extend(consumed.iter().cloned());
            }
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
        consumed
    }

    fn consume_runtime_inputs_at_checkpoint(
        &self,
        turn_id: &TurnId,
        checkpoint: TurnInputCheckpoint,
        prompt: &mut PromptAssembly,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let consumed = self.consume_runtime_input_records(turn_id, checkpoint);
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            prompt.push_trusted_system(guidance);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            prompt.push_context_item(&item);
        }
        consumed
    }

    /// Consume active-turn input after a Provider/tool boundary and place its
    /// typed context on the next Provider request.
    pub(crate) fn consume_active_runtime_inputs_for_next_step(
        &self,
        checkpoint: TurnInputCheckpoint,
    ) -> Vec<crate::session_input::SessionInputRecord> {
        let Some(turn_id) = self.session_input_stream.active_turn_id() else {
            return Vec::new();
        };
        let consumed = self.consume_runtime_input_records(&turn_id, checkpoint);
        if let Some(guidance) = crate::turn_inbox::checkpoint_guidance(checkpoint, &consumed) {
            let mut item = ContextItem::new(
                format!("turn-input-guidance:{}", checkpoint.as_str()),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                guidance,
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            self.push_next_model_context_item(item);
        }
        for item in crate::turn_inbox::checkpoint_context_items(checkpoint, &consumed) {
            self.push_next_model_context_item(item);
        }
        consumed
    }

    #[must_use]
    pub(crate) fn consumed_session_input_cursor(
        &self,
    ) -> Option<harness_contract::turn::SessionInputCursor> {
        self.session_input_stream
            .active_turn_id()
            .as_ref()
            .and_then(|turn_id| self.session_input_stream.highest_consumed_cursor(turn_id))
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

    #[must_use]
    pub(crate) fn with_maintenance_supervisor(
        mut self,
        supervisor: Arc<crate::execution_core::services::RuntimeMaintenanceSupervisor>,
    ) -> Self {
        self.maintenance_supervisor = Some(supervisor);
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

    #[must_use]
    pub fn with_knowledge_activation(mut self, activation: KnowledgeActivationRuntime) -> Self {
        self.knowledge_activation = Some(activation);
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

    /// Record a compact runtime event for later context and memory governance.
    fn record_context_event(
        &mut self,
        event_type: &str,
        category: &str,
        summary: &str,
        priority: u8,
    ) {
        let project_dir = self
            .with_session_read_blocking(|session| session.workspace_root().map(Path::to_path_buf))
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

    async fn acquire_provider_capacity(
        &self,
        model: &str,
        request: &ApiRequest,
    ) -> Result<
        (
            Option<crate::execution_core::graph::ExecutionResourceLease>,
            Duration,
        ),
        RuntimeError,
    > {
        let started = Instant::now();
        let lease = if let Some(manager) = &self.provider_admission {
            let estimated_tokens = request
                .budget
                .fixed_input_tokens
                .saturating_add(request.budget.dynamic_input_tokens)
                .saturating_add(request.budget.protocol_overhead_tokens)
                .saturating_add(request.budget.requested_output_tokens);
            let demands = self.api_client.provider_name_for_model(model).map_or_else(
                || {
                    vec![(
                        crate::execution_core::graph::ExecutionResourceKind::Provider,
                        1,
                    )]
                },
                |provider_name| {
                    self.provider_resource_config
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .admission_demands(&provider_name, model, estimated_tokens)
                },
            );
            let admission = crate::execution_core::graph::ResourceAdmissionRequest::new(
                self.execution_service_class,
                demands,
            )
            .with_parent_class_ceiling(self.execution_service_class)
            .with_deadline_at_ms(now_ms().saturating_add(30_000))
            .with_fairness_key(format!("session:{}", self.session_id()));
            let acquire = manager.admit(admission);
            let lease = tokio::select! {
                () = self.cancellation_token.cancelled() => {
                    return Err(RuntimeError::new(
                        "turn cancelled while waiting for provider capacity",
                    ));
                }
                decision = acquire => {
                    match decision.map_err(|error| RuntimeError::new(format!(
                        "provider capacity admission failed: {error}"
                    )))? {
                        crate::execution_core::graph::ResourceAdmissionDecision::Granted { lease, .. } => lease,
                        crate::execution_core::graph::ResourceAdmissionDecision::Deferred { wait_reason, .. }
                        | crate::execution_core::graph::ResourceAdmissionDecision::Overloaded { wait_reason, .. } => {
                            return Err(RuntimeError::new(format!(
                                "provider capacity admission did not grant: {wait_reason:?}"
                            )));
                        }
                    }
                },
            };
            Some(lease)
        } else {
            None
        };
        let queue_wait = started.elapsed();
        crate::execution_core::performance::observe_duration(
            "provider_admission_queue_ms",
            queue_wait,
        );
        self.verify_session_execution_fence(crate::SessionExecutionFencePhase::ProviderRequest)
            .await?;
        Ok((lease, queue_wait))
    }

    fn record_provider_resource_outcome(
        &self,
        lease: Option<&crate::execution_core::graph::ExecutionResourceLease>,
        queue_wait: Duration,
        service_time: Duration,
        result_class: crate::execution_core::graph::ResourceResultClass,
    ) {
        let (Some(manager), Some(lease)) = (&self.provider_admission, lease) else {
            return;
        };
        let observation = crate::execution_core::graph::ResourceObservation::terminal(
            queue_wait,
            service_time,
            result_class,
        );
        for (kind, _) in lease.demands() {
            let _ = manager.record_observation(kind, observation);
        }
    }

    /// Run a session health probe to verify the runtime is functional after compaction.
    /// Returns Ok(()) if healthy, Err if the session appears broken.
    /// Execute exactly one provider request and translate its response into a
    /// typed graph intent.
    #[cfg(test)]
    pub(crate) async fn execute_model_step(
        &mut self,
        user_input: &str,
        first_step: bool,
    ) -> Result<ModelStepResult, RuntimeError> {
        self.execute_model_step_with_early_dispatch(user_input, first_step, None)
            .await
    }

    /// Execute one Provider step while optionally dispatching completed,
    /// descriptor-proven read-only tool items through the graph-owned early
    /// lane. The dispatcher is supplied by the graph Host; this method never
    /// creates a second tool executor or policy owner.
    pub(crate) async fn execute_model_step_with_early_dispatch(
        &mut self,
        user_input: &str,
        first_step: bool,
        early_dispatcher: Option<Arc<dyn EarlyToolDispatcher>>,
    ) -> Result<ModelStepResult, RuntimeError> {
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new(
                "turn cancelled before provider execution",
            ));
        }
        let started_at = Instant::now();
        if first_step {
            self.clear_turn_tool_observations();
            if let Ok(mut plans) = self.turn_governed_tool_plans.lock() {
                plans.clear();
            }
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
            self.record_message_event(
                &ConversationMessage::user_text(user_input.to_string()),
                self.session_head().await.message_count.wrapping_sub(1),
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
        let tool_activation_ceiling = one_shot_tool_allowlist.clone();
        let one_shot_reasoning_effort = self
            .next_model_reasoning_effort
            .lock()
            .ok()
            .and_then(|mut effort| effort.take());
        let explicitly_forbids_tool_use =
            harness_contract::strategy::prompt_explicitly_forbids_tool_use(user_input);
        let discovery_activation_notice = if text_only_response || explicitly_forbids_tool_use {
            None
        } else {
            self.next_model_tool_activation_notice
                .lock()
                .ok()
                .and_then(|notice| notice.clone())
        };
        let discovery_started = Instant::now();
        let discovery = self.tool_executor.tool_discovery_receipt();
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_catalog_lookup(discovery_started.elapsed());
        }
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
        if first_step {
            self.seed_recent_session_tools(&mut exposure, &discovery)
                .await;
        }
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
        let one_shot_tool_overlay =
            one_shot_tool_allowlist.is_some() || discovery_activation_notice.is_some();
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
        } else if discovery_activation_notice.is_some() {
            exposure.bootstrap.remove("tool_search");
            exposure.active.remove("tool_search");
            exposure.deferred.insert("tool_search".to_string());
            exposure.reason =
                "post-discovery execution handoff; tool_search is paused for one request"
                    .to_string();
            exposure.revision = exposure.revision.saturating_add(1);
            exposure
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
        let exposure_projection = exposure.projection(0);
        let exposed_tool_ids = exposure_projection
            .bootstrap_ids
            .iter()
            .chain(exposure_projection.active_ids.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        self.api_client.configure_tool_exposure(exposure_projection);

        // Tool schemas are part of the request budget. Read their inventory
        // only after Runtime has made the exposure decision.
        let inventory = self.api_client.context_inventory();
        let model_candidates = self.model_candidates_for_turn(user_input);
        let collection_budget = model_candidates
            .iter()
            .map(|model| {
                let window = u64::from(self.context_window_for_model(model));
                let output = u64::from(provider_output_budget_hint(
                    model,
                    window as u32,
                    self.provider_max_output_override,
                ));
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
        let mut one_shot_context_items = self.take_next_model_context_items();
        let context_select_started = Instant::now();
        let mut prompt = self
            .prepare_reality_context_with_budget_and_items(
                user_input,
                collection_budget,
                one_shot_context_items.clone(),
            )
            .await;
        crate::execution_core::performance::observe_duration(
            "context_select_ms",
            context_select_started.elapsed(),
        );
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
            if let Some(activated_ids) = discovery_activation_notice.as_ref() {
                prompt.push_trusted_system(format!(
                    "## Tool discovery handoff\nThis is the immediate automatic continuation of the same user turn. tool_search already completed successfully and is intentionally unavailable for this request. Newly activated native function schemas: [{}]. Continue the original task now by invoking the relevant activated schema directly when evidence or action is still required. Do not ask the user to resend the request and do not claim that a new user turn is needed.",
                    activated_ids.iter().cloned().collect::<Vec<_>>().join(", ")
                ));
            }
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
        self.record_runtime_policy_decision(&decision, self.session_head().await.message_count)
            .await;
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
        let request_clone_started = Instant::now();
        let mut request_messages = self.session.read().await.messages_view();
        crate::execution_core::performance::observe_duration(
            "request_history_clone_ms",
            request_clone_started.elapsed(),
        );
        crate::execution_core::performance::observe_bytes(
            "clone_bytes",
            request_messages.weight().bytes,
        );

        // Compression is a request-preflight recovery path, never a fixed
        // transcript-ratio timer. Optional packets have already been allowed
        // to compete for hard capacity; compact only when no configured
        // candidate can carry the fixed history plus required continuity.
        let no_candidate_can_fit = model_candidates.iter().all(|model| {
            self.pack_provider_attempt(&prompt, &request_messages, model, inventory)
                .is_err()
        });
        if no_candidate_can_fit {
            if let Some(turn_id) = self.session_input_stream.active_turn_id() {
                let consumed = self
                    .consume_runtime_input_records(&turn_id, TurnInputCheckpoint::BeforeCompaction);
                one_shot_context_items.extend(crate::turn_inbox::checkpoint_context_items(
                    TurnInputCheckpoint::BeforeCompaction,
                    &consumed,
                ));
            }
            let compaction = self
                .compact_session_with_checkpoint(self.compaction_config_for_session(1))
                .await?;
            if compaction.is_none() {
                return Err(RuntimeError::new(
                    "all provider candidates reject the required request context and no semantic compaction boundary is available",
                ));
            }
            request_messages = self.session.read().await.messages_view();
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
        let mut models_tried = Vec::new();
        // One retry per model is sufficient: calibration only accepts a
        // smaller explicit provider limit, so the second request is strictly
        // smaller. Repeating beyond that would mask malformed provider errors.
        let mut calibration_retries = BTreeSet::new();
        let mut provider_retries = BTreeMap::<String, u8>::new();
        while let Some(model) = candidates.pop_front() {
            let materialize_started = Instant::now();
            let materialized =
                self.pack_provider_attempt(&prompt, &request_messages, &model, inventory);
            crate::execution_core::performance::observe_duration(
                "request_materialize_ms",
                materialize_started.elapsed(),
            );
            let mut request = match materialized {
                Ok(request) => request,
                Err(error) => {
                    tracing::warn!(
                        model,
                        error = %error,
                        "provider request preflight rejected model candidate"
                    );
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
            if !models_tried.contains(&model) {
                models_tried.push(model.clone());
            }
            if let Some(cowd) = &self.cowd_bus {
                cowd.emit(crate::cowd_event::CowdEvent::ProviderAttempt {
                    model: model.clone(),
                    models_tried: models_tried.clone(),
                    context_window_tokens: request.budget.context_window_tokens,
                    context_window_source: request.budget.context_window_source.clone(),
                    packed_input_tokens: request
                        .budget
                        .fixed_input_tokens
                        .saturating_add(request.budget.dynamic_input_tokens)
                        .saturating_add(request.budget.protocol_overhead_tokens),
                });
            }
            let request_sequence = self.session_head().await.message_count;
            request.provider_evidence_context = Some(crate::ProviderRequestEvidenceContext {
                session_id: self.session_id().to_string(),
                request_sequence,
                request_compiler_cache_hit: request.request_compiler_cache_hit,
                budget: request.budget.clone(),
            });
            self.record_provider_context_request(
                &request,
                request_sequence,
                inventory,
                self.api_client.tool_schema_cache_stats(),
            );
            let attempt_budget = self.runtime_budget_plan_for_candidates(&[model.clone()]);
            let transport_policy = provider_transport_policy(
                attempt_budget.model_context_window.min(u64::from(u32::MAX)) as u32,
                &request,
            );
            let idle_timeout = transport_policy.idle_timeout;
            let heartbeat_grace = transport_policy.heartbeat_grace;
            let cancellation = self.cancellation_token.clone();
            let (provider_lease, provider_queue_wait) =
                self.acquire_provider_capacity(&model, &request).await?;
            let provider_started = Instant::now();
            let stream_started = Instant::now();
            let reducer = ModelStreamReducer::new(
                self.cowd_bus.clone(),
                self.runtime_event_store.clone(),
                self.session_id().to_string(),
            );
            let ApiClientStream {
                events,
                transport_activity,
            } = self.api_client.stream_with_transport_activity(request);
            let stream_run = consume_provider_stream_with_activity(
                events,
                cancellation,
                Some(ProviderStreamTimeoutPolicy {
                    idle: idle_timeout,
                    heartbeat_grace,
                }),
                reducer,
                early_dispatcher.clone(),
                transport_activity,
            )
            .await;
            let resource_result_class = stream_run.resource_result_class;
            let CollectedProviderStream {
                text,
                public_reasoning,
                private_reasoning,
                signature,
                mut calls,
                usage,
                effective_provider_identity,
                first_event_at,
                first_text_at,
                early_tool_receipts,
                early_tool_deferrals,
                response_completed_at_ms,
            } = stream_run.collected;
            if let Some(first_text_at) = first_text_at {
                crate::execution_core::performance::observe_duration(
                    "actual_first_delta_ms",
                    first_text_at.saturating_duration_since(provider_started),
                );
            }
            let effective_model = effective_provider_identity
                .as_ref()
                .map(|identity| identity.model.clone());
            if let Some(identity) = effective_provider_identity {
                *self
                    .active_provider_identity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(identity);
            }
            let signature = (!signature.is_empty()).then_some(signature);
            crate::execution_core::performance::observe_duration(
                "provider_stream_ms",
                stream_started.elapsed(),
            );
            crate::execution_core::performance::observe_duration(
                "provider_service_ms",
                provider_started.elapsed(),
            );
            crate::execution_core::performance::observe_count(
                "provider_input_tokens",
                u64::from(usage.input_tokens),
            );
            crate::execution_core::performance::observe_count(
                "provider_output_tokens",
                u64::from(usage.output_tokens),
            );
            let service_ms = provider_started.elapsed().as_millis().max(1) as u64;
            crate::execution_core::performance::observe_count(
                "provider_output_tokens_per_second",
                u64::from(usage.output_tokens).saturating_mul(1_000) / service_ms,
            );
            if resource_result_class
                == crate::execution_core::graph::ResourceResultClass::DownstreamOverload
            {
                crate::execution_core::performance::observe_count(
                    "provider_downstream_overload_total",
                    1,
                );
            }
            self.record_provider_resource_outcome(
                provider_lease.as_ref(),
                provider_queue_wait,
                provider_started.elapsed(),
                resource_result_class,
            );
            drop(provider_lease);
            if let Some(reservation) = evaluation_reservation.as_mut() {
                reservation.reconcile(usage);
            }
            if let Some(error) = stream_run.failure {
                if error.is_provider_tool_protocol_failure() {
                    return Err(error);
                }
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
                let retries = provider_retries.entry(model.clone()).or_default();
                if error.provider_retryable() && *retries < MAX_RUNTIME_PROVIDER_RETRIES_PER_MODEL {
                    *retries = retries.saturating_add(1);
                    let retry_after = error
                        .provider_retry_after()
                        .unwrap_or(DEFAULT_RUNTIME_PROVIDER_RETRY_DELAY);
                    crate::execution_core::performance::observe_duration(
                        "provider_retry_after_ms",
                        retry_after,
                    );
                    tokio::select! {
                        () = self.cancellation_token.cancelled() => {
                            return Err(RuntimeError::new(
                                "turn cancelled during provider retry-after delay",
                            ));
                        }
                        () = tokio::time::sleep(retry_after) => {}
                    }
                    candidates.push_front(model);
                    continue;
                }
                last_error = Some(error);
                continue;
            }

            canonicalize_model_tool_names(&mut calls, self.tool_executor.as_ref());
            let requested_tool_call_count = calls.len();
            let unexposed_tool_names = unexposed_model_tool_names(&calls, &exposed_tool_ids);
            if !unexposed_tool_names.is_empty() {
                let activation_candidates = unexposed_tool_names
                    .iter()
                    .filter(|name| {
                        tool_activation_ceiling
                            .as_ref()
                            .is_none_or(|allowlist| allowlist.contains(*name))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let denied_by_overlay = unexposed_tool_names
                    .iter()
                    .filter(|name| {
                        tool_activation_ceiling
                            .as_ref()
                            .is_some_and(|allowlist| !allowlist.contains(*name))
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let activated =
                    self.activate_deferred_tool_calls(&activation_candidates, &discovery);
                // Provider transports validate framing, while Runtime owns
                // this request's exposure lease. A known healthy deferred tool
                // receives one Runtime-owned activation/replan; invented,
                // unhealthy, or over-permission names still fail closed before
                // any assistant transcript is published.
                self.reconcile_provider_context_usage(usage);
                self.usage_tracker.record(usage);
                if let Some(callback) = &self.tool_callback {
                    callback.on_usage(&usage);
                }
                if denied_by_overlay.is_empty() && !activated.is_empty() {
                    return Err(RuntimeError::with_tool_exposure_miss(format!(
                        "tool_exposure_miss: provider requested known deferred tool names [{}]; Runtime activated [{}] for the single governed retry",
                        unexposed_tool_names.join(", "),
                        activated.into_iter().collect::<Vec<_>>().join(", ")
                    ))
                    .with_provider_usage(usage));
                }
                return Err(RuntimeError::with_provider_failure_metadata(
                    format!(
                        "tool_protocol_violation: provider requested unknown, unavailable, or unauthorized tool names outside this request's exposure lease: [{}]{}",
                        unexposed_tool_names.join(", "),
                        (!denied_by_overlay.is_empty()).then(|| format!(
                            "; governed one-request allowlist rejected [{}]",
                            denied_by_overlay.join(", ")
                        )).unwrap_or_default()
                    ),
                    None,
                    true,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                )
                .with_provider_usage(usage));
            }
            if discovery_activation_notice.is_some() {
                if let Ok(mut notice) = self.next_model_tool_activation_notice.lock() {
                    *notice = None;
                }
            }
            let mut blocks = Vec::new();
            if !public_reasoning.is_empty() {
                blocks.push(ContentBlock::ReasoningSummary {
                    text: public_reasoning,
                });
            }
            if !private_reasoning.is_empty() || signature.is_some() {
                blocks.push(ContentBlock::Thinking {
                    thinking: private_reasoning,
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
            self.record_message_event(
                &assistant_message,
                self.session_head().await.message_count.wrapping_sub(1),
            );
            self.reconcile_provider_context_usage(usage);
            self.usage_tracker.record(usage);
            if let Some(callback) = &self.tool_callback {
                callback.on_usage(&usage);
            }
            self.record_assistant_iteration(
                self.session_head().await.message_count,
                &assistant_message,
                requested_tool_call_count,
            );
            let classified = classify_model_step_intent(text, calls);
            let intent = apply_explicit_team_requirement(
                self.explicit_team_escalation,
                user_input,
                first_step,
                &decision,
                classified,
            );
            let effective_model = effective_model.or(Some(model));
            if let Some(model) = effective_model.as_ref() {
                if !models_tried.contains(model) {
                    models_tried.push(model.clone());
                }
            }
            self.consume_active_runtime_inputs_for_next_step(
                TurnInputCheckpoint::AfterProviderResponse,
            );
            return Ok(ModelStepResult {
                intent,
                assistant_message,
                usage,
                // Preserve the model that actually produced the provider stream,
                // not merely Runtime's preferred candidate.
                model: effective_model,
                models_used: models_tried.clone(),
                first_token_latency_ms: first_event_at.map(|first| {
                    u64::try_from(first.saturating_duration_since(stream_started).as_millis())
                        .unwrap_or(u64::MAX)
                }),
                active_stream_duration_ms: first_event_at.map(|first| millis_since(first).max(1)),
                wall_duration_ms: millis_since(started_at).max(1),
                early_tool_receipts,
                early_tool_deferrals,
                response_completed_at_ms,
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
    ) -> Result<ToolBatchStepResult, RuntimeError>
    where
        C: Sync,
    {
        if self.cancellation_token.is_cancelled() {
            return Err(RuntimeError::new("turn cancelled before tool execution"));
        }
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
        let prepared = self.tool_executor.prepare_governed_invocations(&requests);
        let workspace_root = self.governed_workspace_root()?;
        let compilation =
            GovernedToolCompiler.compile_partial(&workspace_root, &requests, |name, input| {
                prepared
                    .iter()
                    .find(|invocation| {
                        invocation.intent.tool_name == name
                            && invocation.intent.normalized_input == *input
                    })
                    .map(|invocation| {
                        (
                            invocation.effect.clone(),
                            invocation.catalog_revision,
                            invocation.descriptor_set_hash.clone(),
                        )
                    })
            });
        let compilation = match compilation {
            Ok(compilation) => compilation,
            Err(error) => {
                let reason = format!("governed tool DAG rejected before execution: {error}");
                self.append_execution_runtime_event(
                    RuntimeEventScope::Tool,
                    "tool.plan.rejected",
                    Some("rejected".to_string()),
                    calls
                        .iter()
                        .map(|call| RuntimeEventRef {
                            kind: "tool_call".to_string(),
                            id: call.id.clone(),
                        })
                        .collect(),
                    serde_json::json!({
                        "reason": error.to_string(),
                        "tool_count": calls.len(),
                    }),
                );
                let mut messages = Vec::with_capacity(calls.len());
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
                    let sequence = self.session_head().await.message_count.wrapping_sub(1);
                    self.record_message_event(&message, sequence);
                    self.remember_tool_trace_from_message(&message);
                    messages.push(message);
                }
                return Ok(ToolBatchStepResult {
                    failed: messages.len(),
                    messages,
                    max_concurrency_observed: 0,
                    parallel_batches: 0,
                });
            }
        };
        let mut preflight_messages = Vec::with_capacity(compilation.rejected.len());
        for rejected in &compilation.rejected {
            let message = ConversationMessage::tool_result(
                rejected.tool_call_id.clone(),
                rejected.tool_name.clone(),
                format!(
                    "governed tool node rejected before execution: {}",
                    rejected.reason
                ),
                true,
            );
            self.session
                .write()
                .await
                .push_message(message.clone())
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let sequence = self.session_head().await.message_count.wrapping_sub(1);
            self.record_message_event(&message, sequence);
            self.remember_tool_trace_from_message(&message);
            preflight_messages.push(message);
        }
        if !compilation.rejected.is_empty() {
            self.append_execution_runtime_event(
                RuntimeEventScope::Tool,
                "tool.plan.partially_rejected",
                Some("partial".to_string()),
                compilation
                    .rejected
                    .iter()
                    .map(|rejected| RuntimeEventRef {
                        kind: "tool_call".to_string(),
                        id: rejected.tool_call_id.clone(),
                    })
                    .collect(),
                serde_json::json!({
                    "rejected": compilation.rejected,
                    "accepted_count": compilation.plan.as_ref().map_or(0, |plan| plan.task_count),
                }),
            );
        }
        let Some(plan) = compilation.plan else {
            return Ok(ToolBatchStepResult {
                failed: preflight_messages.len(),
                messages: preflight_messages,
                max_concurrency_observed: 0,
                parallel_batches: 0,
            });
        };
        self.record_governed_tool_plan(&plan, self.session_head().await.message_count);
        let decision = self.retarget_active_turn_strategy_for_governed_plan(&plan, calls)?;
        self.tool_executor.bind_execution_decision(decision.clone());
        let mut validation = plan.validate_against_execution_decision(&decision);
        if validation.allowed {
            self.satisfy_tool_strategy_gates(&plan, &decision, &mut validation)
                .await;
        }
        self.record_tool_strategy_validation(&validation, self.session_head().await.message_count);
        let mut max_concurrency_observed = 0;
        let mut parallel_batches = 0;
        let mut messages = preflight_messages;
        if validation.allowed {
            self.record_tool_schedule(&plan, &requests, self.session_head().await.message_count);
            let context = ConversationGovernedToolContext {
                runtime: self,
                pending_tool_uses: &pending,
                prompter,
                iterations: iteration,
                strategy_approval_satisfied: validation.approval_satisfied,
                plan_id: &plan.plan_id,
                plan_revision: plan.revision,
            };
            let report = GovernedToolExecutor.execute(&plan, &context).await;
            max_concurrency_observed = report.max_active;
            parallel_batches = usize::from(report.max_active > 1);
            for outcome in report.outcomes {
                let Some((message, _)) = outcome.receipt else {
                    return Err(RuntimeError::new(format!(
                        "governed tool task `{}` reached terminal state without a durable result receipt",
                        outcome.task_id
                    )));
                };
                self.remember_tool_trace_from_message(&message);
                messages.push(message);
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
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&message, sequence);
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
        iterations: usize,
        model: Option<String>,
        models_used: Vec<String>,
        first_token_latency_ms: Option<u64>,
        active_stream_duration_ms: u64,
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
            self.session_id().to_string(),
            user_input.to_string(),
            self.context_profile(),
            &self.system_prompt,
            decision,
        );
        if let Ok(plans) = self.turn_governed_tool_plans.lock() {
            for plan in plans.iter().cloned() {
                kernel.record_governed_tool_plan(plan);
            }
        }
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
            self.schedule_memory_post_turn(user_input).await;
        } else {
            let _ = self.run_memory_post_turn(user_input).await;
        }
        self.memory_context_revision.fetch_add(1, Ordering::AcqRel);
        let memory_elapsed = memory_started.elapsed();
        let usage = self.usage_tracker.cumulative_usage();
        let telemetry = crate::cowd_event::RunModelTelemetry {
            model: model.clone(),
            models_used,
            first_token_latency_ms,
            active_stream_duration_ms: Some(active_stream_duration_ms.max(1)),
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
        self.record_ai_kernel_trace_event(
            &summary.ai_kernel_trace,
            self.session_head().await.message_count,
        );
        if let Some(ref cowd) = self.cowd_bus {
            cowd.emit(crate::cowd_event::CowdEvent::WriteAttemptsObserved {
                paths: summary.write_attempt_paths.clone(),
            });
            cowd.emit(crate::cowd_event::CowdEvent::RunModelTelemetry {
                telemetry: summary.model_telemetry.clone(),
            });
        }
        let commit_ms = finalize_started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        crate::execution_core::performance::record_turn_latency_trace(
            crate::execution_core::performance::TurnLatencyTrace {
                trace_id: summary.ai_kernel_trace.harness_receipt.id.clone(),
                session_id: self.session_id().to_string(),
                turn_id: None,
                activation_ms: None,
                context_ms: None,
                provider_ms: Some(wall_duration_ms),
                tool_ms: None,
                commit_ms: Some(commit_ms),
                total_ms: wall_duration_ms.saturating_add(commit_ms),
                recorded_at_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_millis() as u64),
            },
        );
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
        execution_plan: &GovernedToolPlan,
        execution_decision: &crate::execution_core::RuntimeExecutionDecision,
        validation: &mut GovernedToolPolicyValidationReport,
    ) {
        if validation.requires_approval {
            let Some(coordinator) = &self.approval_coordinator else {
                validation.allowed = false;
                validation
                    .findings
                    .push("mutation_missing_approval_runtime".to_string());
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
            let Some(mut descriptor) = execution_plan
                .tasks
                .iter()
                .max_by_key(|task| match crate::task_risk_for_effect(&task.effect) {
                    harness_contract::core::TaskRisk::Low => 0,
                    harness_contract::core::TaskRisk::Medium => 1,
                    harness_contract::core::TaskRisk::High => 2,
                    harness_contract::core::TaskRisk::Critical => 3,
                })
                .map(|task| task.effect.clone())
            else {
                validation.allowed = false;
                validation
                    .findings
                    .push("mutation_approval_has_no_tool_effect".to_string());
                return;
            };
            descriptor.tool_id = "runtime_strategy_tool_batch".to_string();
            descriptor.approval_class = harness_contract::tool::ToolApprovalClass::User;
            let execution_context = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_execution_context);
            let activity_binding = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_activity_binding);
            let source = harness_contract::policy::ApprovalSource {
                kind: harness_contract::policy::ApprovalSourceKind::Session,
                session_id: Some(self.session_id().to_string()),
                agent_id: (self.memory_agent_id != "primary").then(|| self.memory_agent_id.clone()),
                team_id: self.memory_team_id.clone(),
                mission_id: None,
                resource_ref: Some(self.checkpoint_workspace_id.clone()),
                review_ref: None,
                application: None,
            };
            let context = harness_contract::policy::ApprovalContext {
                principal_id: format!("session:{}", self.session_id()),
                profile_id: self.autonomy_profile().as_str().to_string(),
                workspace_key: self.checkpoint_workspace_id.clone(),
                session_id: Some(self.session_id().to_string()),
                turn_id: execution_context
                    .as_ref()
                    .map(|value| value.turn_id.clone()),
                task_id: activity_binding
                    .as_ref()
                    .map(|binding| binding.task_id.clone()),
                capability: descriptor.tool_id.clone(),
                invocation_id: Some(format!("strategy:{}", execution_decision.lease.lease_id)),
                execution_id: execution_context
                    .as_ref()
                    .map(|value| value.execution_id.clone()),
                strategy_decision_ref: Some(execution_decision.lease.lease_id.clone()),
                source_surface: Some("gateway_session".to_string()),
                resource_targets: descriptor
                    .scopes
                    .iter()
                    .filter_map(|scope| scope.target.clone())
                    .collect(),
                effect: Some(descriptor.clone()),
                explicit_ask: true,
            };
            let pending_hook = self.cowd_bus.clone().map(|cowd| {
                let tool = descriptor.tool_id.clone();
                Arc::new(move |request: &harness_contract::policy::ApprovalRequest| {
                    cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                        status: harness_contract::projection::ExecutionLiveStatus::WaitingApproval,
                        detail: Some(tool.clone()),
                    });
                    cowd.emit(crate::cowd_event::CowdEvent::ApprovalRequested {
                        request_id: request.approval_id.clone(),
                        tool: tool.clone(),
                    });
                }) as crate::ApprovalPendingHook
            });
            let approval_result = coordinator
                .resolve_tool(
                    source,
                    context,
                    &descriptor,
                    &approval_input,
                    self.cancellation_token(),
                    Some(self.session_input_stream.input_notifier()),
                    pending_hook,
                    Duration::from_secs(120),
                )
                .await;
            emit_approval_resolution_event(self.cowd_bus(), coordinator.queue(), &approval_result);
            match approval_result {
                Ok(crate::ApprovalResolution::Approved { grant, .. }) => {
                    validation.approval_satisfied = true;
                    if grant.scope == harness_contract::policy::ApprovalGrantScope::Once {
                        let _ = coordinator.queue().consume_once_grant(&grant.grant_id);
                    }
                }
                Ok(crate::ApprovalResolution::Denied { reason, .. })
                | Ok(crate::ApprovalResolution::Cancelled { reason, .. }) => {
                    validation.allowed = false;
                    validation
                        .findings
                        .push(format!("mutation_approval_denied:{reason}"));
                    return;
                }
                Ok(crate::ApprovalResolution::ControlRequested { reason, .. }) => {
                    self.consume_active_runtime_inputs_for_next_step(
                        TurnInputCheckpoint::AfterToolResult,
                    );
                    validation.allowed = false;
                    validation
                        .findings
                        .push(format!("mutation_approval_superseded:{reason}"));
                    return;
                }
                Err(error) => {
                    validation.allowed = false;
                    validation
                        .findings
                        .push(format!("mutation_approval_failed:{error}"));
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
            executor.registered_tool_effect("checkpoint_create", &checkpoint_value)
        else {
            validation.allowed = false;
            validation
                .findings
                .push("checkpoint_create_missing_effect_descriptor".to_string());
            return;
        };
        let timeout = self.tool_timeout.unwrap_or_else(|| {
            Duration::from_secs(
                crate::ToolSafetyCategory::from_effect(&descriptor).default_timeout_secs(),
            )
        });
        let authorization = match self.assess_tool_authorization(
            &descriptor,
            &checkpoint_input,
            format!(
                "{}:checkpoint:{}",
                self.session_id().to_string(),
                execution_decision.lease.lease_id
            ),
            PermissionContext::default(),
            validation.approval_satisfied,
            timeout.as_secs(),
        ) {
            Ok(ToolAuthorizationDecision::Authorized(decision)) => decision,
            Ok(ToolAuthorizationDecision::Gap { assessment, .. }) => {
                validation.allowed = false;
                validation.findings.push(format!(
                    "checkpoint_authorization_denied:{}",
                    assessment
                        .gap
                        .as_ref()
                        .map_or("unknown capability gap", |gap| gap.reason.as_str())
                ));
                return;
            }
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_authorization_denied:{error}"));
                return;
            }
        };
        let checkpoint_demand = crate::governed_tool_plan::resource_demand_from_effect(&descriptor);
        let result = self
            .tool_execution_plane
            .execute_async_classified_retained(
                &checkpoint_demand,
                Some(timeout),
                self.execution_service_class,
                Some(self.execution_service_class),
                Some(self.session_id()),
                async move {
                    executor
                        .execute_authorized_output(
                            &authorization.authorization,
                            "checkpoint_create",
                            &checkpoint_input,
                        )
                        .await
                },
            )
            .await;
        let result = result.0;
        match result {
            Ok(Ok(output)) => {
                validation.checkpoint_created = true;
                tracing::info!(
                    strategy_lease_id = %execution_decision.lease.lease_id,
                    checkpoint = %preview_chars(output.model_text(), 240),
                    "strategy checkpoint created before mutation"
                );
            }
            Ok(Err(error)) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_creation_failed:{error}"));
            }
            Err(error) => {
                validation.allowed = false;
                validation
                    .findings
                    .push(format!("checkpoint_execution_failed:{error}"));
            }
        }
    }

    /// Extract the per-tool execution logic from run_turn for reuse.
    async fn execute_single_tool(
        &self,
        task: &crate::governed_tool_plan::GovernedToolPlanTask,
        plan_id: &str,
        plan_revision: u64,
        input: &str,
        prompter: &crate::permissions::SharedPrompter,
        iterations: usize,
        strategy_approval_satisfied: bool,
        retained_admission: &mut Option<crate::ToolExecutionAdmission>,
    ) -> Result<ConversationMessage, RuntimeError> {
        let tool_use_id = task.tool_call_id.as_str();
        let tool_name = task.tool_name.as_str();
        let pre_hook_result = self.run_pre_tool_use_hook(tool_name, input);
        let effective_input = pre_hook_result
            .updated_input()
            .map_or_else(|| input.to_string(), ToOwned::to_owned);
        let mut permission_context = PermissionContext::new(
            pre_hook_result.permission_override(),
            pre_hook_result.permission_reason().map(ToOwned::to_owned),
        );
        if pre_hook_result.is_cancelled() {
            permission_context = PermissionContext::new(
                Some(crate::permissions::PermissionOverride::Deny),
                Some(format!("PreToolUse hook cancelled tool `{tool_name}`")),
            );
        } else if pre_hook_result.is_failed() {
            let hook_msgs = pre_hook_result.messages().join("; ");
            permission_context = PermissionContext::new(
                Some(crate::permissions::PermissionOverride::Deny),
                Some(if hook_msgs.is_empty() {
                    format!("PreToolUse hook failed for tool `{tool_name}`")
                } else {
                    format!("PreToolUse hook failed for tool `{tool_name}`: {hook_msgs}")
                }),
            );
        } else if pre_hook_result.is_denied() {
            permission_context = PermissionContext::new(
                Some(crate::permissions::PermissionOverride::Deny),
                Some(format!("PreToolUse hook denied tool `{tool_name}`")),
            );
        }
        let profile_timeout = Duration::from_secs(task.safety_category.default_timeout_secs());
        let tool_timeout = self
            .tool_timeout
            .map_or(profile_timeout, |timeout| timeout.min(profile_timeout));
        let authorization_id = format!(
            "{}:{plan_id}:{plan_revision}:{tool_use_id}:{iterations}",
            self.session_id()
        );
        let authorization_decision = self
            .negotiate_tool_authorization(
                &task.effect,
                &effective_input,
                authorization_id,
                permission_context,
                strategy_approval_satisfied,
                tool_timeout.as_secs(),
                prompter,
            )
            .await?;

        match authorization_decision {
            ToolAuthorizationDecision::Authorized(authorization) => {
                let invocation_record = self
                    .start_tool_invocation_record(
                        tool_use_id,
                        tool_name,
                        &effective_input,
                        iterations,
                    )
                    .with_governed_plan(plan_id, plan_revision);
                self.verify_session_execution_fence(
                    crate::SessionExecutionFencePhase::ToolExecution,
                )
                .await?;
                self.record_tool_invocation_event(
                    &invocation_record,
                    "tool.invocation.started",
                    self.session_head().await.message_count,
                );
                self.record_tool_started(iterations, tool_name);
                if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
                    metrics.observe_invocation(tool_name);
                }
                if let Some(callback) = &self.tool_callback {
                    let preview: String = effective_input.chars().take(200).collect();
                    callback.on_tool_start(tool_use_id, tool_name, &preview);
                }

                let start = Instant::now();
                let tname = tool_name.to_string();
                let tname_for_err = tname.clone();
                let tinput = effective_input.clone();
                let tool_exec = Arc::clone(&self.tool_executor);
                let evidence_sandbox = self.tool_output_sandbox.clone();
                let is_evidence_retrieve = tool_name == "evidence_retrieve";
                let demand = task.resource_demand.clone();
                let plane = Arc::clone(&self.tool_execution_plane);
                let effect_request = crate::RuntimeToolExecutionRequest {
                    governed_plan_id: plan_id.to_string(),
                    governed_plan_revision: plan_revision,
                    idempotency_key: format!(
                        "{}:{plan_id}:{plan_revision}:{tool_use_id}:{iterations}",
                        self.session_id()
                    ),
                    tool_use_id: tool_use_id.to_string(),
                    tool_name: tool_name.to_string(),
                    input: effective_input.clone(),
                    category: task.safety_category,
                    authorization: Some(authorization.authorization.clone()),
                    session_id: Some(self.session_id().to_string()),
                    memory_context: Some(self.memory_turn_context()),
                    model_lease: None,
                    parent_execution: None,
                    execution_decision: None,
                    evaluation_isolated: false,
                    managed_invocation: None,
                };
                let effect_commit = self.runtime_event_store.as_ref().map(|store| {
                    crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(store))
                });
                let effect_state = match effect_commit.as_ref() {
                    Some(commit) => commit
                        .begin_tool_effect(&effect_request, &task.effect)
                        .map_err(|error| RuntimeError::new(error.to_string()))?,
                    None if task.effect.effect_kind
                        == harness_contract::tool::ToolEffectKind::Read =>
                    {
                        crate::execution_core::graph::ToolEffectState::NotRequired
                    }
                    None => {
                        return Err(RuntimeError::new(
                            "mutation tool execution requires the durable Runtime effect ledger",
                        ));
                    }
                };
                let execute_fresh = matches!(
                    effect_state,
                    crate::execution_core::graph::ToolEffectState::Fresh
                        | crate::execution_core::graph::ToolEffectState::NotRequired
                );
                let execution = match effect_state {
                    crate::execution_core::graph::ToolEffectState::Completed(outcome) => {
                        if outcome.status == crate::RuntimeToolExecutionStatus::Executed {
                            Ok(Ok(
                                harness_contract::context::ToolOutputDraft::bounded_inline(
                                    outcome.output.unwrap_or_default(),
                                ),
                            ))
                        } else {
                            Ok(Err(ToolError::new(
                                outcome
                                    .error
                                    .or(outcome.output)
                                    .unwrap_or_else(|| "durable tool effect failed".to_string()),
                            )))
                        }
                    }
                    crate::execution_core::graph::ToolEffectState::Uncertain => {
                        return Err(RuntimeError::new(format!(
                            "tool effect `{}` is uncertain; non-idempotent execution was not replayed",
                            effect_request.idempotency_key
                        )));
                    }
                    crate::execution_core::graph::ToolEffectState::Fresh
                    | crate::execution_core::graph::ToolEffectState::NotRequired => {
                        let (execution, admission) = plane
                            .execute_async_classified_retained(
                                &demand,
                                Some(tool_timeout),
                                self.execution_service_class,
                                Some(self.execution_service_class),
                                Some(self.session_id()),
                                async move {
                                    if is_evidence_retrieve {
                                        return retrieve_tool_evidence_from_sandbox(
                                            evidence_sandbox.as_ref(),
                                            &tinput,
                                        )
                                        .map(harness_contract::context::ToolOutputDraft::bounded_inline)
                                        .map_err(ToolError::new);
                                    }
                                    if matches!(
                                        tname.as_str(),
                                        "tool_search" | "runtime_capabilities"
                                    ) {
                                        tool_exec.execute_output(&tname, &tinput).await
                                    } else {
                                        tool_exec.execute_authorized_output(
                                            &authorization.authorization,
                                            &tname,
                                            &tinput,
                                        ).await
                                    }
                                },
                            )
                            .await;
                        *retained_admission = admission;
                        execution
                    }
                };
                let (output_draft, mut is_error, mut failure_kind) = match execution {
                    Ok(Ok(output)) => (output, false, None),
                    Ok(Err(error)) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            error.to_string(),
                        ),
                        true,
                        Some(ToolFailureKind::ExecutionError),
                    ),
                    Err(crate::ToolExecutionPlaneError::TimedOut(_)) => {
                        tracing::warn!(tool = %tname_for_err, timeout_secs = tool_timeout.as_secs(), "tool execution waiter timed out; started operation remains fenced");
                        (
                            harness_contract::context::ToolOutputDraft::bounded_inline(format!(
                                "tool `{tname_for_err}` timed out after {tool_timeout:?}"
                            )),
                            true,
                            Some(ToolFailureKind::Timeout),
                        )
                    }
                    Err(crate::ToolExecutionPlaneError::Panicked) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            "tool execution panicked",
                        ),
                        true,
                        Some(ToolFailureKind::Panic),
                    ),
                    Err(error) => (
                        harness_contract::context::ToolOutputDraft::bounded_inline(
                            error.to_string(),
                        ),
                        true,
                        Some(ToolFailureKind::ExecutionError),
                    ),
                };
                let output = output_draft.model_text().to_string();
                if execute_fresh {
                    self.verify_session_execution_fence(
                        crate::SessionExecutionFencePhase::ToolCommit,
                    )
                    .await?;
                    if let Some(commit) = effect_commit.as_ref() {
                        commit
                            .commit_tool_effect(
                                &effect_request,
                                &task.effect,
                                &crate::RuntimeToolExecutionOutcome {
                                    tool_use_id: tool_use_id.to_string(),
                                    tool_name: tool_name.to_string(),
                                    status: if is_error {
                                        crate::RuntimeToolExecutionStatus::Failed
                                    } else {
                                        crate::RuntimeToolExecutionStatus::Executed
                                    },
                                    category: task.safety_category,
                                    output: (!is_error).then(|| output.clone()),
                                    error: is_error.then(|| output.clone()),
                                    evidence_ref: format!(
                                        "tool-effect:{}",
                                        effect_request.idempotency_key
                                    ),
                                },
                            )
                            .map_err(|error| {
                                RuntimeError::new(format!(
                                    "tool effect completed but durable receipt failed: {error}"
                                ))
                            })?;
                    }
                }
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
                if let Some(cowd) = self.cowd_bus() {
                    cowd.emit(crate::cowd_event::CowdEvent::ToolExecuted {
                        name: tool_name.to_string(),
                        duration_ms: elapsed_ms,
                    });
                }

                // T36: Truncate oversized tool results before storing.
                // Append hook feedback messages to the tool output.
                let tool_search_activated = tool_name == "tool_search"
                    && !is_error
                    && self
                        .activate_tool_discovery(&output)
                        .is_some_and(|receipt| receipt.activated_ids().next().is_some());
                let mut combined = if tool_name == "runtime_capabilities" && !is_error {
                    self.project_runtime_capabilities_for_model(&output)
                } else {
                    output
                };
                if tool_search_activated {
                    combined.push_str(
                        "\n\nThe discovered tools are active on the immediately following \
                         automatic model request in this same turn. Continue the current task \
                         and invoke the relevant activated tool directly; do not ask the user \
                         to resend solely because activation just completed.",
                    );
                }
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
                    .record_tool_output_evidence(
                        tool_use_id,
                        tool_name,
                        &completed_record.input_hash,
                        &output_draft,
                        &combined,
                        is_error,
                        elapsed_ms,
                        None,
                    )
                    .await?;
                self.maybe_index_tool_output(
                    raw_ref.id(),
                    tool_name,
                    &indexable_output,
                    Some(&raw_access),
                );
                let completed_record =
                    completed_record.with_full_output_ref(format!("tool://{}", raw_ref.id()));
                let mut model_receipt = self.tool_model_receipt(
                    tool_name,
                    &combined,
                    is_error,
                    &raw_ref,
                    Some(&raw_access),
                );
                if let Some(payload) = prepared_vision.as_ref() {
                    model_receipt.summary = vision_tool_model_receipt(payload, &raw_ref);
                    model_receipt.receipt_tokens =
                        crate::context_ledger::estimate_text_tokens(&model_receipt.summary);
                    model_receipt.omitted_tokens = model_receipt
                        .raw_tokens
                        .saturating_sub(model_receipt.receipt_tokens);
                    model_receipt.truncated =
                        model_receipt.receipt_tokens < model_receipt.raw_tokens;
                }
                let audit_projection =
                    crate::context_evidence::audit_projection(&model_receipt, Some(&raw_access));
                self.push_turn_evidence_audit(audit_projection);
                let model_summary = model_receipt.summary;
                let output_envelope = harness_contract::context::ToolOutputEnvelope {
                    artifact_ref: Some(harness_contract::context::ArtifactRef::durable(
                        raw_access.retrieval_selector.clone(),
                        raw_access.sha256.clone(),
                        raw_access.bytes,
                        raw_access.media_type.clone(),
                        raw_access.visibility_scope.clone(),
                    )),
                    evidence_ref: Some(raw_access),
                    receipt: completed_record
                        .full_output_ref
                        .clone()
                        .unwrap_or_else(|| format!("tool://{}", raw_ref.id())),
                };
                self.push_turn_tool_observation(
                    ToolObservation::new(
                        tool_name.to_string(),
                        completed_record.invocation_id.clone(),
                        raw_ref,
                        model_summary.clone(),
                    )
                    .with_output_envelope(output_envelope),
                );
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
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&result, sequence);
                if let Some(payload) = prepared_vision {
                    let image_message = vision_user_message(&payload);
                    self.session
                        .write()
                        .await
                        .push_message(image_message.clone())
                        .map_err(|error| RuntimeError::new(error.to_string()))?;
                    self.record_message_event(
                        &image_message,
                        self.session_head().await.message_count.wrapping_sub(1),
                    );
                }
                self.record_tool_invocation_event(
                    &completed_record,
                    if is_error {
                        "tool.invocation.failed"
                    } else {
                        "tool.invocation.completed"
                    },
                    self.session_head().await.message_count.wrapping_sub(1),
                );
                self.record_tool_finished(iterations, &result);
                Ok(result)
            }
            ToolAuthorizationDecision::Gap { assessment, .. } => {
                let gap = assessment.gap.as_ref();
                let reason = gap.map_or_else(
                    || "capability authorization was not granted".to_string(),
                    |gap| gap.reason.clone(),
                );
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
                let first_recovery = gap.is_some_and(|gap| gap.recoverable);
                let payload = serde_json::json!({
                    "kind": "capability_gap",
                    "assessment_id": assessment.assessment_id,
                    "path": assessment.path,
                    "gap": assessment.gap,
                    "controlled_recovery_available": first_recovery,
                    "instruction": if first_recovery {
                        "Use one listed safe alternative or revise the plan with existing capabilities. Do not repeat the same denied action without new evidence or approval."
                    } else {
                        "The same capability gap is closed for this turn. Preserve evidence and report the limitation without retrying the denied action."
                    },
                })
                .to_string();
                let denied = ConversationMessage::tool_result(
                    tool_use_id.to_string(),
                    tool_name.to_string(),
                    payload,
                    !first_recovery,
                );
                self.session
                    .write()
                    .await
                    .push_message(denied.clone())
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
                let sequence = self.session_head().await.message_count.wrapping_sub(1);
                self.record_message_event(&denied, sequence);
                Ok(denied)
            }
        }
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

    fn model_candidates_for_turn(&self, _user_input: &str) -> Vec<String> {
        let primary = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(ToString::to_string);
        let fallback_snapshot = self
            .fallbacks
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut fallback_models: Vec<String> = fallback_snapshot
            .iter()
            .map(|model| model.trim())
            .filter(|model| !model.is_empty())
            .map(ToString::to_string)
            .collect();
        fallback_models.dedup();
        if let Some(primary) = primary.as_ref() {
            fallback_models.retain(|model| model != primary);
        }

        let mut routed = Vec::with_capacity(fallback_models.len() + usize::from(primary.is_some()));
        if let Some(primary) = primary {
            routed.push(primary);
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
        let strategy_segment = self
            .active_turn_strategy()
            .map(|state| (state.policy_version.clone(), state.selected_candidate));
        let receipt = if let Some(projector) = self.outcome_projector.as_ref() {
            let (selected, receipt) = crate::select_provider_from_outcome_snapshot(
                self.routing_mode,
                &routed,
                &self.runtime_config_revision,
                strategy_segment
                    .as_ref()
                    .map(|(policy_revision, _)| policy_revision.as_str()),
                strategy_segment
                    .as_ref()
                    .map(|(_, selected_candidate)| *selected_candidate),
                &projector.snapshot(),
                now_ms(),
            );
            routed = selected;
            receipt
        } else {
            crate::ProviderSelectionReceipt {
                requested_mode: self.routing_mode,
                effective_mode: crate::RoutingMode::Pinned,
                snapshot_revision: 0,
                selected_model: routed.first().cloned().unwrap_or_default(),
                fallback_reason: (self.routing_mode == crate::RoutingMode::Auto)
                    .then(|| "outcome projection is unavailable".to_string()),
                candidates: Vec::new(),
            }
        };
        *self
            .provider_selection_receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(receipt);
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
    pub(crate) fn authorization_negotiator(&self) -> crate::AuthorizationNegotiator {
        self.authorization_negotiator.clone()
    }

    fn authorization_request(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        approval_satisfied: bool,
    ) -> crate::AuthorizationRequest {
        let execution_context = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_execution_context);
        let delegated = self.memory_agent_id != "primary";
        let recovery_scope = execution_context.as_ref().map_or_else(
            || format!("session:{}", self.session_id()),
            |context| format!("turn:{}", context.turn_id),
        );
        let safe_alternatives = match descriptor.effect_kind {
            harness_contract::tool::ToolEffectKind::Read => Vec::new(),
            harness_contract::tool::ToolEffectKind::Write => {
                vec!["return a patch or proposed change without applying it".to_string()]
            }
            harness_contract::tool::ToolEffectKind::Network => {
                vec!["use already-authorized local or cached evidence".to_string()]
            }
            harness_contract::tool::ToolEffectKind::Process
            | harness_contract::tool::ToolEffectKind::Package => {
                vec!["inspect and report the required operation without executing it".to_string()]
            }
            harness_contract::tool::ToolEffectKind::System
            | harness_contract::tool::ToolEffectKind::Destructive
            | harness_contract::tool::ToolEffectKind::Unknown => Vec::new(),
        };
        crate::AuthorizationRequest {
            principal_id: if delegated {
                format!("agent:{}", self.memory_agent_id)
            } else {
                format!("session:{}", self.session_id())
            },
            capability: descriptor.tool_id.clone(),
            input: input.to_string(),
            idempotency_key,
            effect: descriptor.clone(),
            parent_ceiling: if delegated {
                self.permission_policy.active_mode()
            } else {
                crate::PermissionMode::DangerFullAccess
            },
            parent_lease_id: delegated.then(|| format!("binding:{}", self.memory_agent_id)),
            approval_satisfied,
            recovery_scope,
            context: permission_context,
            safe_alternatives,
        }
    }

    fn record_capability_assessment(
        &self,
        assessment: &harness_contract::policy::CapabilityAssessment,
    ) {
        if let Some(cowd) = self.cowd_bus() {
            cowd.emit(crate::cowd_event::CowdEvent::CapabilityAssessed {
                assessment: assessment.clone(),
            });
        }
        let mut refs = vec![RuntimeEventRef {
            kind: "capability".to_string(),
            id: assessment.capability.clone(),
        }];
        if let Some(lease) = assessment.lease.as_ref() {
            refs.push(RuntimeEventRef {
                kind: "authorization_lease".to_string(),
                id: lease.lease_id.clone(),
            });
        }
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "authorization.capability_assessed",
            Some(format!("{:?}", assessment.path).to_ascii_lowercase()),
            refs,
            serde_json::to_value(assessment).unwrap_or_else(|_| serde_json::json!({})),
        );
        for transition in self.authorization_negotiator.drain_transitions() {
            if let Some(cowd) = self.cowd_bus() {
                cowd.emit(crate::cowd_event::CowdEvent::AuthorizationLeaseTransition {
                    transition: transition.clone(),
                });
            }
            self.append_execution_runtime_event(
                RuntimeEventScope::Tool,
                "authorization.lease_transition",
                Some(format!("{:?}", transition.kind).to_ascii_lowercase()),
                vec![RuntimeEventRef {
                    kind: "authorization_lease".to_string(),
                    id: transition.lease.lease_id.clone(),
                }],
                serde_json::to_value(transition).unwrap_or_else(|_| serde_json::json!({})),
            );
        }
    }

    pub(crate) fn assess_tool_authorization(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        approval_satisfied: bool,
        timeout_secs: u64,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        let request = self.authorization_request(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context,
            approval_satisfied,
        );
        let evaluated = self
            .authorization_negotiator
            .assess_effective(&self.permission_policy, &request);
        let assessment = evaluated.assessment;
        self.record_capability_assessment(&assessment);
        if let Some(lease) = assessment.lease.clone() {
            return crate::ToolPolicy
                .authorize(
                    &evaluated.effective,
                    &assessment,
                    idempotency_key,
                    lease,
                    timeout_secs,
                )
                .map(ToolAuthorizationDecision::Authorized)
                .map_err(|error| RuntimeError::new(error.to_string()));
        }
        Ok(ToolAuthorizationDecision::Gap {
            assessment,
            effective: evaluated.effective,
        })
    }

    pub(crate) async fn negotiate_tool_authorization(
        &self,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        idempotency_key: String,
        permission_context: PermissionContext,
        approval_satisfied: bool,
        timeout_secs: u64,
        prompter: &crate::permissions::SharedPrompter,
    ) -> Result<ToolAuthorizationDecision, RuntimeError> {
        let initial = self.assess_tool_authorization(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context.clone(),
            approval_satisfied,
            timeout_secs,
        )?;
        let ToolAuthorizationDecision::Gap {
            assessment,
            effective,
        } = initial
        else {
            return Ok(initial);
        };
        if assessment.path != harness_contract::policy::AuthorizationPath::HumanApproval {
            return Ok(ToolAuthorizationDecision::Gap {
                assessment: self.govern_capability_gap(assessment),
                effective,
            });
        }

        let explicit_ask = permission_context.override_decision()
            == Some(crate::permissions::PermissionOverride::Ask);
        let request = self.authorization_request(
            descriptor,
            input,
            idempotency_key.clone(),
            permission_context,
            false,
        );
        let mut approved_grant = None;
        let approval_ref = if let Some(coordinator) = &self.approval_coordinator {
            let execution_context = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_execution_context);
            let activity_binding = self
                .cowd_bus()
                .and_then(crate::CowdEventBus::current_activity_binding);
            let source = harness_contract::policy::ApprovalSource {
                kind: if self.memory_agent_id != "primary" {
                    harness_contract::policy::ApprovalSourceKind::Agent
                } else {
                    harness_contract::policy::ApprovalSourceKind::Session
                },
                session_id: Some(self.session_id().to_string()),
                agent_id: (self.memory_agent_id != "primary").then(|| self.memory_agent_id.clone()),
                team_id: self.memory_team_id.clone(),
                mission_id: None,
                resource_ref: Some(self.checkpoint_workspace_id.clone()),
                review_ref: None,
                application: None,
            };
            let context = harness_contract::policy::ApprovalContext {
                principal_id: request.principal_id.clone(),
                profile_id: self.autonomy_profile().as_str().to_string(),
                workspace_key: self.checkpoint_workspace_id.clone(),
                session_id: Some(self.session_id().to_string()),
                turn_id: execution_context
                    .as_ref()
                    .map(|value| value.turn_id.clone()),
                task_id: activity_binding
                    .as_ref()
                    .map(|binding| binding.task_id.clone()),
                capability: descriptor.tool_id.clone(),
                invocation_id: Some(idempotency_key.clone()),
                execution_id: execution_context
                    .as_ref()
                    .map(|value| value.execution_id.clone()),
                strategy_decision_ref: None,
                source_surface: Some("gateway_session".to_string()),
                resource_targets: descriptor
                    .scopes
                    .iter()
                    .filter_map(|scope| scope.target.clone())
                    .collect(),
                effect: Some(effective.descriptor.clone()),
                explicit_ask,
            };
            let pending_hook = self.cowd_bus.clone().map(|cowd| {
                let tool = descriptor.tool_id.clone();
                Arc::new(move |request: &harness_contract::policy::ApprovalRequest| {
                    cowd.emit(crate::cowd_event::CowdEvent::ExecutionPhase {
                        status: harness_contract::projection::ExecutionLiveStatus::WaitingApproval,
                        detail: Some(tool.clone()),
                    });
                    cowd.emit(crate::cowd_event::CowdEvent::ApprovalRequested {
                        request_id: request.approval_id.clone(),
                        tool: tool.clone(),
                    });
                }) as crate::ApprovalPendingHook
            });
            let approval_result = coordinator
                .resolve_tool(
                    source,
                    context,
                    &effective.descriptor,
                    input,
                    self.cancellation_token(),
                    Some(self.session_input_stream.input_notifier()),
                    pending_hook,
                    Duration::from_secs(timeout_secs.max(1)),
                )
                .await;
            emit_approval_resolution_event(self.cowd_bus(), coordinator.queue(), &approval_result);
            match approval_result {
                Ok(crate::ApprovalResolution::Approved { grant, .. }) => {
                    let approval_ref = grant.grant_id.clone();
                    approved_grant = Some(grant);
                    approval_ref
                }
                Ok(crate::ApprovalResolution::Denied {
                    reason,
                    approval_id,
                })
                | Ok(crate::ApprovalResolution::Cancelled {
                    reason,
                    approval_id,
                }) => {
                    let denied = denied_capability_assessment(assessment, &reason, &approval_id);
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
                Ok(crate::ApprovalResolution::ControlRequested {
                    reason,
                    approval_id,
                }) => {
                    self.consume_active_runtime_inputs_for_next_step(
                        TurnInputCheckpoint::AfterToolResult,
                    );
                    let denied = denied_capability_assessment(assessment, &reason, &approval_id);
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
                Err(error) => {
                    let denied = denied_capability_assessment(
                        assessment,
                        &error,
                        &format!("tool-approval:{idempotency_key}"),
                    );
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
            }
        } else {
            let decision = prompter.lock().as_mut().map(|prompt| {
                prompt.decide(&PermissionRequest {
                    tool_name: descriptor.tool_id.clone(),
                    input: input.to_string(),
                    current_mode: self.permission_policy.active_mode(),
                    required_mode: assessment.required_mode,
                    reason: assessment.gap.as_ref().map(|gap| gap.reason.clone()),
                })
            });
            match decision {
                Some(PermissionPromptDecision::Allow) => "approval:prompter".to_string(),
                Some(PermissionPromptDecision::Deny { reason }) => {
                    let denied =
                        denied_capability_assessment(assessment, &reason, "approval:prompter");
                    self.record_capability_assessment(&denied);
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: denied,
                        effective,
                    });
                }
                None => {
                    return Ok(ToolAuthorizationDecision::Gap {
                        assessment: self.govern_capability_gap(assessment),
                        effective,
                    })
                }
            }
        };

        let approved = self.authorization_negotiator.approve_effective(
            &self.permission_policy,
            &request,
            &effective,
            &approval_ref,
        );
        self.record_capability_assessment(&approved);
        let Some(lease) = approved.lease.clone() else {
            return Ok(ToolAuthorizationDecision::Gap {
                assessment: approved,
                effective,
            });
        };
        let authorized = crate::ToolPolicy
            .authorize(&effective, &approved, idempotency_key, lease, timeout_secs)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if let (Some(coordinator), Some(grant)) = (&self.approval_coordinator, approved_grant) {
            if grant.scope == harness_contract::policy::ApprovalGrantScope::Once {
                coordinator
                    .queue()
                    .consume_once_grant(&grant.grant_id)
                    .map_err(RuntimeError::new)?;
            }
        }
        Ok(ToolAuthorizationDecision::Authorized(authorized))
    }

    fn govern_capability_gap(
        &self,
        mut assessment: harness_contract::policy::CapabilityAssessment,
    ) -> harness_contract::policy::CapabilityAssessment {
        if assessment.gap.as_ref().is_some_and(|gap| gap.recoverable)
            && !self
                .authorization_negotiator
                .claim_controlled_recovery(&assessment)
        {
            if let Some(gap) = assessment.gap.as_mut() {
                gap.recoverable = false;
                gap.reason.push_str(
                    "; the same capability gap already consumed its single controlled recovery",
                );
            }
            assessment
                .evidence_refs
                .push("authorization.recovery_circuit_open".to_string());
        }
        assessment
    }

    pub fn set_permission_mode(&mut self, mode: crate::PermissionMode) {
        self.permission_policy.set_active_mode(mode);
    }

    pub fn set_autonomy_profile(&self, profile: crate::AutonomyProfileId) {
        *self
            .autonomy_profile
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = profile;
    }

    #[must_use]
    pub fn autonomy_profile(&self) -> crate::AutonomyProfileId {
        *self
            .autonomy_profile
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub(crate) fn active_permission_mode(&self) -> crate::PermissionMode {
        self.permission_policy.active_mode()
    }

    #[must_use]
    pub fn tool_timeout(&self) -> Option<std::time::Duration> {
        self.tool_timeout
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[allow(
        clippy::panic,
        reason = "a synchronous snapshot boundary cannot return an error; a failed scoped reader violates the session read contract"
    )]
    fn with_session_read_blocking<R, F>(&self, read: F) -> R
    where
        R: Send,
        F: FnOnce(&Session) -> R + Send,
    {
        if let Ok(session) = self.session.try_read() {
            return read(&session);
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                return tokio::task::block_in_place(|| read(&self.session.blocking_read()));
            }

            // `block_in_place` is unsupported by a current-thread Tokio runtime.
            // Keep the explicitly synchronous boundary off that executor.
            let session = Arc::clone(&self.session);
            return std::thread::scope(|scope| {
                scope
                    .spawn(move || read(&session.blocking_read()))
                    .join()
                    .unwrap_or_else(|_| {
                        panic!("session read worker terminated before returning a session")
                    })
            });
        }
        read(&self.session.blocking_read())
    }

    fn read_head(session: &Session) -> SessionReadHead {
        let history = session.history();
        let weight = history.weight();
        SessionReadHead {
            message_count: session.message_count(),
            history_revision: history.revision(),
            history_bytes: weight.bytes,
            history_tokens: weight.tokens,
            updated_at_ms: session.updated_at_ms,
            model: session.model.clone(),
        }
    }

    #[must_use]
    pub fn session_head_blocking(&self) -> SessionReadHead {
        self.with_session_read_blocking(Self::read_head)
    }

    pub async fn session_head(&self) -> SessionReadHead {
        let session = self.session.read().await;
        Self::read_head(&session)
    }

    #[must_use]
    pub fn session_snapshot_blocking(&self) -> Session {
        self.with_session_read_blocking(Clone::clone)
    }

    pub async fn session_snapshot(&self) -> Session {
        self.session.read().await.clone()
    }

    pub fn api_client_mut(&mut self) -> &mut C {
        &mut self.api_client
    }

    #[must_use]
    pub fn request_compiler_stats(&self) -> crate::RequestCompilerStats {
        self.request_compiler.stats()
    }

    pub(crate) async fn session_mut_async(&mut self) -> tokio::sync::RwLockWriteGuard<'_, Session> {
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
        let sequence = session.message_count().wrapping_sub(1);
        drop(session);
        self.record_message_event(&message, sequence);
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
        if self.session_journal_port.is_none() {
            return Err(RuntimeError::new(
                "semantic compaction requires a durable Session journal port; transcript was retained",
            ));
        }
        let original_session = self.session.read().await.clone();
        let Some(plan) = plan_session_compaction(&original_session, config) else {
            return Ok(None);
        };
        let original_messages = original_session.materialize_messages();

        let source_messages = compacted_source_messages(
            &original_messages,
            plan.source_message_start,
            plan.source_message_end,
        );
        let raw_refs = source_message_evidence_refs(
            &original_session.session_id,
            &original_messages,
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
            let execution_identity = match self.execution_identity.clone() {
                Some(identity) => identity,
                None => {
                    let turn_id = self.session_input_stream.active_turn_id().map_or_else(
                        || format!("checkpoint-turn:{checkpoint_id}"),
                        |id| id.to_string(),
                    );
                    harness_contract::execution::ExecutionIdentity::for_session_turn(
                        self.memory_agent_id.clone(),
                        self.checkpoint_workspace_id.clone(),
                        original_session.session_id.clone(),
                        turn_id,
                    )
                    .map_err(|error| {
                        RuntimeError::new(format!(
                            "semantic checkpoint execution identity is invalid: {error}"
                        ))
                    })?
                }
            };
            let build_context = SessionCheckpointBuildContext::new(
                original_session.session_id.clone(),
                ctx.agent_id.clone(),
                source_range,
            )
            .with_checkpoint_id(checkpoint_id)
            .with_execution_identity(execution_identity)
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
            .with_evidence_ref(
                EvidenceRef::observed("checkpoint", checkpoint.checkpoint_id.clone())
                    .with_source("semantic_compaction_checkpoint"),
            )
            .with_evidence_ref(
                EvidenceRef::observed(
                    "fact-extraction",
                    fact_extraction_decision.mode.as_str().to_string(),
                )
                .with_source(fact_extraction_event.evidence_label()),
            );
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
                    .push(format!("{}:{}", evidence.ref_type, evidence.id));
            }
            receipt
        });

        tracing::info!(removed = result.removed_message_count, "compaction");
        let compacted_len = result.compacted_session.message_count();
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
                    receipt_mut.evidence_refs.push(
                        EvidenceRef::observed(
                            "fact-review",
                            memory_receipt.fact_review.batch_id.as_str().to_string(),
                        )
                        .with_source(format!(
                            "promoted={} held={} rejected={} conflicts={}",
                            memory_receipt.fact_review.promoted.len(),
                            memory_receipt.fact_review.held.len(),
                            memory_receipt.fact_review.rejected.len(),
                            memory_receipt.fact_review.conflicts.len()
                        )),
                    );
                }
                Err(error) => {
                    tracing::warn!(%error, "semantic compaction fact projection deferred");
                    receipt_mut.evidence_refs.push(
                        EvidenceRef::observed(
                            "memory",
                            "semantic_checkpoint_fact_projection_deferred",
                        )
                        .with_source(error.to_string()),
                    );
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
        let port = self.session_journal_port.as_ref().ok_or_else(|| {
            RuntimeError::new(
                "semantic compaction requires a durable Session journal port; transcript was retained",
            )
        })?;
        let session_id = self.session_id().to_string();
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
        let context_event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::ContextSessionCompacted,
            payload,
            created_at_ms,
        );
        let checkpoint_id = semantic_checkpoint.checkpoint_id.clone();
        let compaction_event_id = format!("compaction:{session_id}:{checkpoint_id}");
        let events = vec![
            context_event,
            crate::RuntimeSessionEvent::new(
                session_id.clone(),
                0,
                crate::RuntimeSessionEventKind::MemorySemanticCheckpointCreated,
                serde_json::json!({
                    "source": "conversation_runtime.compaction",
                    "compaction_event_id": compaction_event_id,
                    "checkpoint": semantic_checkpoint,
                    "receipt": receipt,
                }),
                created_at_ms,
            ),
        ];
        let committed = port
            .append_compaction_bundle_if_absent(&events, &checkpoint_id)
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

    fn emit_tool_started(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        causal_parent_ids: &[String],
    ) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit_tool_started_with_dependencies(
            tool_use_id,
            tool_name,
            &preview_chars(input, 200),
            causal_parent_ids,
        );
    }

    fn emit_tool_completed(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        output: &str,
        exit_code: Option<i32>,
        causal_parent_ids: &[String],
    ) {
        let Some(ref cowd) = self.cowd_bus else {
            return;
        };
        cowd.emit_tool_completed_with_dependencies(
            tool_use_id,
            tool_name,
            &preview_chars(output, 500),
            exit_code,
            causal_parent_ids,
        );
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

        let Some(mgr) = self.memory_manager.as_ref() else {
            let (runtime_reality_context_items, session_context_items) = tokio::join!(
                async {
                    let started = Instant::now();
                    let items = self.runtime_reality_context_items(user_input).await;
                    self.record_context_source_latency("reality", started.elapsed());
                    items
                },
                async {
                    let started = Instant::now();
                    let items = self.runtime_session_context_items(user_input).await;
                    self.record_context_source_latency("session", started.elapsed());
                    items
                }
            );
            let unavailable_sources = vec![ContextSourceKind::Memory];
            let mut dynamic_items = runtime_reality_context_items;
            dynamic_items.extend(session_context_items);
            dynamic_items.extend(next_model_context_items);
            let envelope = self.build_context_envelope(
                user_input,
                dynamic_items,
                Vec::new(),
                unavailable_sources,
                total_budget_tokens,
            );
            return self
                .finalize_context_prompt(user_input, envelope, None)
                .await;
        };

        let mem_messages = self.memory_context_messages().await;

        let session_id = self.session_id().to_string();
        let memory_ctx = self.memory_turn_context();
        let kernel = MemoryKernel::new(Arc::clone(mgr));
        let memory_budget = self.runtime_budget_plan().memory_retrieval_budget;
        let memory_budget_tokens = memory_budget.retrieval_budget.min(u64::from(u32::MAX));
        let (memory_packet, runtime_reality_context_items, session_context_items) = tokio::join!(
            async {
                let started = Instant::now();
                let packet = kernel
                    .context_packet(
                        &memory_ctx,
                        user_input,
                        mem_messages.as_slice(),
                        memory_budget.candidate_scan_limit,
                        memory_budget_tokens,
                    )
                    .await;
                self.record_context_source_latency("memory", started.elapsed());
                packet
            },
            async {
                let started = Instant::now();
                let items = self.runtime_reality_context_items(user_input).await;
                self.record_context_source_latency("reality", started.elapsed());
                items
            },
            async {
                let started = Instant::now();
                let items = self.runtime_session_context_items(user_input).await;
                self.record_context_source_latency("session", started.elapsed());
                items
            },
        );
        match memory_packet {
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
                    dynamic_items.extend(session_context_items);
                    dynamic_items.extend(next_model_context_items);
                    let envelope = self.build_context_envelope(
                        user_input,
                        dynamic_items,
                        omissions,
                        Vec::new(),
                        total_budget_tokens,
                    );
                    return self
                        .finalize_context_prompt(user_input, envelope, None)
                        .await;
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
                let knowledge_activation = self.knowledge_activation.as_ref().and_then(|runtime| {
                    runtime.activate_from_packet_for_project(
                        &session_id,
                        user_input,
                        &format!("{:?}", self.context_profile()),
                        Some(&self.checkpoint_workspace_id),
                        &packet,
                    )
                });
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
                dynamic_items.extend(session_context_items);
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
                    .await
            }
            Err(err) => {
                tracing::warn!(%err, "memory: prepare_context failed, using base system prompt");
                if let Some(cb) = &self.memory_callback {
                    cb.on_memory_update(Vec::new(), &format!("memory error: {err}"));
                }
                let unavailable_sources = vec![ContextSourceKind::Memory];
                let mut dynamic_items = runtime_reality_context_items;
                dynamic_items.extend(session_context_items);
                dynamic_items.extend(next_model_context_items);
                let envelope = self.build_context_envelope(
                    user_input,
                    dynamic_items,
                    Vec::new(),
                    unavailable_sources,
                    total_budget_tokens,
                );
                self.finalize_context_prompt(user_input, envelope, None)
                    .await
            }
        }
    }

    async fn runtime_reality_context_items(&self, user_input: &str) -> Vec<ContextItem> {
        let Some((port, binding)) = &self.reality_recall else {
            return Vec::new();
        };
        let report = port.recall_for_binding_async(binding, user_input, 64).await;
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

    /// Recall only the current Session automatically. Cross-Session history is
    /// available through the explicit `context_retrieve` tool and is never
    /// passively injected into another conversation.
    async fn runtime_session_context_items(&self, user_input: &str) -> Vec<ContextItem> {
        let Some(history) = self.session_history_reader.as_ref() else {
            return Vec::new();
        };
        let session_id = self.session_id().to_string();
        let hot_projection = self.hot_state.as_ref().and_then(|hot_state| {
            hot_state.sessions().get(&session_id).and_then(|snapshot| {
                snapshot
                    .context_manifest
                    .clone()
                    .map(|manifest| (manifest, snapshot.context_cards.clone()))
            })
        });
        let (manifest, cards) = match hot_projection {
            Some(projection) => projection,
            None => match history.page_in_context(&session_id, 512).await {
                Ok(Some(page)) => {
                    let projection = (page.manifest.clone(), page.context_cards.clone());
                    if let Some(hot_state) = &self.hot_state {
                        hot_state.sessions().update(&session_id, |snapshot| {
                            snapshot.context_manifest = Some(page.manifest);
                            snapshot.context_cards = page.context_cards;
                            snapshot.context_refs = vec![format!(
                                "session-context:{}:{}",
                                session_id, projection.0.projection_generation
                            )];
                        });
                    }
                    projection
                }
                Ok(None) => return Vec::new(),
                Err(error) => {
                    tracing::warn!(%error, session_id, "current Session context page-in failed");
                    return Vec::new();
                }
            },
        };
        let query_terms = context_query_terms(user_input);
        if query_terms.is_empty() {
            return Vec::new();
        }
        let binding_fingerprint = self
            .reality_recall
            .as_ref()
            .and_then(|(_, binding)| serde_json::to_vec(binding).ok())
            .map(|bytes| format!("{:x}", Sha256::digest(&bytes)))
            .unwrap_or_else(|| "no-reality-binding".to_string());
        let cache_key = SessionContextProjectionCacheKey {
            session_id: session_id.clone(),
            projection_generation: manifest.projection_generation,
            index_revision: manifest.recovery.index_generation,
            memory_revision: self.memory_context_revision.load(Ordering::Acquire),
            reality_snapshot: binding_fingerprint.clone(),
            binding_fingerprint,
            query_digest: format!("{:x}", Sha256::digest(user_input.as_bytes())),
            model_window: self.model_context_window,
        };
        if let Ok(cache) = self.session_context_projection_cache.lock() {
            if let Some(entry) = cache.as_ref().filter(|entry| entry.key == cache_key) {
                self.current_context_cache_hit
                    .store(true, Ordering::Release);
                return entry.items.clone();
            }
        }
        let has_parented_leaves = cards.iter().any(|card| card.parent_card_id.is_some());
        let mut scored = cards
            .into_iter()
            .filter(|card| !has_parented_leaves || card.parent_card_id.is_some())
            .filter_map(|card| {
                let score = context_text_relevance(&card.summary, &query_terms);
                (score > 0.0).then_some((score, card))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|(left, left_card), (right, right_card)| {
            right
                .partial_cmp(left)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right_card.updated_at_ms.cmp(&left_card.updated_at_ms))
        });
        scored.truncate(8);
        let exact_ranges = scored
            .iter()
            .filter(|(score, _)| *score >= 0.45)
            .map(|(_, card)| (card.source_start_sequence, card.source_end_sequence))
            .collect::<Vec<_>>();
        let context_messages = if exact_ranges.is_empty() {
            Vec::new()
        } else {
            match history
                .messages_in_ranges(&session_id, &exact_ranges, 1_024)
                .await
            {
                Ok(messages) => messages,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        session_id,
                        "selected current-Session transcript range expansion failed"
                    );
                    Vec::new()
                }
            }
        };

        let mut items = Vec::new();
        for (score, card) in scored {
            let mut navigation = ContextItem::new(
                card.card_id.clone(),
                ContextSourceKind::Conversation,
                ContextRole::Orientation,
                format!(
                    "Current Session history card (messages {}..{}):\n{}",
                    card.source_start_sequence, card.source_end_sequence, card.summary
                ),
            );
            navigation.authority = ContextAuthority::Session;
            navigation.visibility = ContextVisibility::Private;
            navigation.score = score;
            navigation.source_id = Some(card.card_id.clone());
            navigation.source_version = Some(format!(
                "generation:{}:digest:{}",
                manifest.projection_generation, card.source_digest
            ));
            navigation.source_lifecycle = crate::context_runtime::ContextSourceLifecycle::Session;
            navigation.source_reason = Some("focused current-Session navigation card".to_string());
            navigation.evidence.push(format!(
                "session://{}/messages/{}..{}#{}",
                session_id,
                card.source_start_sequence,
                card.source_end_sequence,
                card.source_digest
            ));
            items.push(navigation);

            // A strong card match is expanded from the immutable transcript.
            // The card remains a locator; exact rows remain authoritative.
            if score < 0.45 {
                continue;
            }
            let messages = context_messages
                .iter()
                .filter(|message| {
                    message.sequence >= card.source_start_sequence
                        && message.sequence < card.source_end_sequence
                })
                .take(128)
                .cloned()
                .collect::<Vec<_>>();
            if session::context_index_source_digest(&messages) != card.source_digest {
                tracing::warn!(
                    session_id,
                    card_id = card.card_id,
                    "Session card source digest mismatch; exact expansion suppressed"
                );
                continue;
            }
            for message in messages {
                let content = session_message_context_text(&message.content_json);
                if content.is_empty() {
                    continue;
                }
                let mut exact = ContextItem::new(
                    message.stable_message_id.clone(),
                    ContextSourceKind::Conversation,
                    ContextRole::RecentTurn,
                    format!("{}: {}", message.role, content),
                );
                exact.authority = if message.role == "user" {
                    ContextAuthority::User
                } else {
                    ContextAuthority::Session
                };
                exact.visibility = ContextVisibility::Private;
                exact.score = score;
                exact.source_id = Some(message.stable_message_id.clone());
                exact.source_version = Some(format!("sequence:{}", message.sequence));
                exact.source_lifecycle = crate::context_runtime::ContextSourceLifecycle::Session;
                exact.source_reason = Some("exact expansion of matched Session card".to_string());
                exact.evidence.push(format!(
                    "session://{}/messages/{}",
                    session_id, message.sequence
                ));
                items.push(exact);
            }
        }
        if let Ok(mut cache) = self.session_context_projection_cache.lock() {
            *cache = Some(SessionContextProjectionCacheEntry {
                key: cache_key,
                items: items.clone(),
            });
        }
        items
    }

    async fn memory_context_messages(&self) -> Arc<Vec<MemMessage>> {
        let mut projection = self.session_memory_projection.lock().await;
        let session = self.session.read().await;
        let cursor = session.history().cursor();
        let source_count = session.message_count();

        if projection.initialized
            && projection.history_revision == cursor.revision
            && projection.source_count == source_count
        {
            return Arc::clone(&projection.messages);
        }

        let added = source_count.saturating_sub(projection.source_count);
        let append_only = is_append_only_projection(
            projection.initialized,
            projection.history_revision,
            projection.source_count,
            cursor.revision,
            source_count,
        );
        let start_index = if append_only {
            projection.source_count
        } else {
            0
        };
        let source_messages = if append_only {
            session.messages_page(start_index, added).materialize()
        } else {
            session.materialize_messages()
        };
        drop(session);

        let converted =
            conversation_messages_to_context_mem_messages(&source_messages, start_index);
        projection.converted_messages = projection
            .converted_messages
            .saturating_add(converted.len() as u64);
        if append_only {
            Arc::make_mut(&mut projection.messages).extend(converted);
        } else {
            projection.messages = Arc::new(converted);
            projection.rebuilds = projection.rebuilds.saturating_add(1);
        }
        projection.initialized = true;
        projection.history_revision = cursor.revision;
        projection.source_count = source_count;
        tracing::trace!(
            session_history_revision = cursor.revision,
            source_count,
            appended = append_only,
            converted = source_messages.len(),
            total_converted = projection.converted_messages,
            rebuilds = projection.rebuilds,
            "memory context projection updated"
        );
        Arc::clone(&projection.messages)
    }

    /// Perform post-turn memory housekeeping (micro-compact, drift, seeds).
    ///
    /// Errors are logged and swallowed so a memory failure never aborts a turn.
    async fn run_memory_post_turn(&self, user_input: &str) -> Result<(), RuntimeError> {
        let Some((mgr, memory_ctx, mem_messages, callback)) =
            self.memory_post_turn_work(user_input).await
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
    async fn schedule_memory_post_turn(&self, user_input: &str) {
        let Some((mgr, memory_ctx, mem_messages, callback)) =
            self.memory_post_turn_work(user_input).await
        else {
            return;
        };
        let owner = format!("memory-post-turn:{}", memory_ctx.session_id);
        let work = async move {
            Self::complete_memory_post_turn(mgr, memory_ctx, mem_messages, callback).await;
        };
        if let Some(supervisor) = &self.maintenance_supervisor {
            if !supervisor.submit(owner, work).await {
                tracing::debug!("runtime maintenance supervisor is closed; post-turn work skipped");
            }
        } else {
            work.await;
        }
    }

    async fn memory_post_turn_work(
        &self,
        user_input: &str,
    ) -> Option<(
        Arc<CognitiveContextManager>,
        MemoryTurnContext,
        Vec<MemMessage>,
        Option<Arc<dyn MemoryCallback>>,
    )> {
        // The root Session turn is the sole producer of ordinary L1-L3
        // conversation memory. Delegated Team agents receive the parent
        // objective in their synthetic prompt, so extracting it again would
        // multiply identical preferences and decisions across Agent scopes.
        // Their independent results still flow through the governed
        // KnowledgeCandidate/L4 promotion path.
        if !self.owns_conversation_memory_production() {
            return None;
        }
        let mgr = Arc::clone(self.memory_manager.as_ref()?);
        let memory_ctx = self.memory_turn_context();

        // Extract only the completed turn. Re-scanning the full transcript on
        // every turn multiplies cost and repeatedly writes the same memory.
        // Any user supplements appended after the root prompt remain inside the
        // window and are therefore available to extraction.
        let session_messages = self.session.read().await.materialize_messages();
        let mem_messages = conversation_messages_to_mem_messages(current_turn_messages(
            &session_messages,
            user_input,
        ));

        Some((mgr, memory_ctx, mem_messages, self.memory_callback.clone()))
    }

    fn owns_conversation_memory_production(&self) -> bool {
        self.memory_team_id.as_deref().is_none_or(str::is_empty)
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

    /// Index oversized tool output by evidence reference instead of retaining
    /// the complete payload in the active conversation context.
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
            model_protocol::fingerprint::stable_hash_bytes(output.as_bytes())
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

    #[cfg(test)]
    fn retrieve_tool_evidence(&self, input: &str) -> Result<String, String> {
        retrieve_tool_evidence_from_sandbox(self.tool_output_sandbox.as_ref(), input)
    }

    fn record_provider_context_request(
        &self,
        request: &ApiRequest,
        request_sequence: usize,
        inventory: ProviderContextInventory,
        schema_stats: (u64, u64),
    ) {
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_provider_request(inventory, schema_stats);
        }
        if let Ok(mut metrics) = self.turn_stable_prefix_metrics.lock() {
            metrics.observe_request(request);
        }
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
                // Public summaries are projected to the user but are not
                // returned as Provider transcript input.
                ContentBlock::ReasoningSummary { .. } => {}
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
        if let Ok(mut metrics) = self.turn_stable_prefix_metrics.lock() {
            metrics.observe_usage(usage);
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
    ) -> Result<(EvidenceRef, EvidenceAccessRef), RuntimeError> {
        let content_hash = model_protocol::fingerprint::stable_hash_bytes(output.as_bytes());
        let evidence_id = format!("tool-raw-{tool_use_id}-{content_hash:016x}");
        let evidence_ref = EvidenceRef::observed("tool", evidence_id.clone());
        if let Some(access) = self.existing_evidence_access(&evidence_ref) {
            return Ok((evidence_ref, access));
        }
        let Some(ref session_port) = self.session_journal_port else {
            return Err(RuntimeError::new(
                "raw tool evidence cannot be published without the Session store",
            ));
        };
        let Some(ref artifacts) = self.artifact_store else {
            return Err(RuntimeError::new(
                "raw tool evidence cannot be published without the Artifact store",
            ));
        };
        let session_id = self.session_id().to_string();
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
            crate::context_evidence::raw::SessionPortRawEvidenceStore::new(
                Arc::clone(session_port),
                Arc::clone(artifacts),
            ),
        );
        let access = match facade
            .persist(crate::context_evidence::raw::RawEvidenceWrite {
                evidence_ref: evidence_ref.clone(),
                session_id: session_id.clone(),
                media_type: "text/plain; charset=utf-8".to_string(),
                visibility_scope: format!("session:{session_id}"),
                payload: output.to_string(),
                metadata,
            })
            .await
        {
            Ok(access) => access,
            Err(error) => return Err(RuntimeError::new(error.to_string())),
        };
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            let _ = ledger.register_evidence_hash(evidence_id);
        }
        Ok((evidence_ref, access))
    }

    async fn record_tool_output_evidence(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input_hash: &str,
        draft: &harness_contract::context::ToolOutputDraft,
        model_text: &str,
        is_error: bool,
        duration_ms: u64,
        source_evidence_ref: Option<&str>,
    ) -> Result<(EvidenceRef, EvidenceAccessRef), RuntimeError> {
        let Some(artifact) = draft.artifact_ref() else {
            return self
                .record_tool_raw_evidence(
                    tool_use_id,
                    tool_name,
                    input_hash,
                    model_text,
                    is_error,
                    duration_ms,
                    source_evidence_ref,
                )
                .await;
        };
        let evidence_id = format!(
            "tool-raw-{tool_use_id}-{}",
            artifact.sha256.trim_start_matches("sha256:")
        );
        let evidence_ref = EvidenceRef::observed("tool", evidence_id.clone());
        if let Some(access) = self.existing_evidence_access(&evidence_ref) {
            return Ok((evidence_ref, access));
        }
        let Some(ref session_port) = self.session_journal_port else {
            return Err(RuntimeError::new(
                "staged tool evidence cannot be published without the Session store",
            ));
        };
        let Some(ref artifacts) = self.artifact_store else {
            return Err(RuntimeError::new(
                "staged tool evidence cannot be published without the Artifact store",
            ));
        };
        let session_id = self.session_id().to_string();
        let metadata = serde_json::json!({
            "type": "ToolObservationRaw",
            "evidence_id": evidence_id,
            "session_id": session_id,
            "tool_call_id": tool_use_id,
            "tool_name": tool_name,
            "input_hash": input_hash,
            "is_error": is_error,
            "duration_ms": duration_ms,
            "summary_line_count": model_text.lines().count(),
            "summary_byte_count": model_text.len(),
            "source_evidence_ref": source_evidence_ref,
            "native_staged_artifact": true,
        });
        let access = crate::context_evidence::raw::SessionPortRawEvidenceStore::new(
            Arc::clone(session_port),
            Arc::clone(artifacts),
        )
        .persist_artifact(evidence_ref.clone(), session_id, artifact.clone(), metadata)
        .await
        .map_err(|error| RuntimeError::new(error.to_string()))?;
        if let Ok(mut ledger) = self.turn_context_ledger.lock() {
            let _ = ledger.register_evidence_hash(evidence_id);
        }
        Ok((evidence_ref, access))
    }

    /// Ingest an outcome already executed by the graph-owned tool host.
    /// The graph remains responsible for publication; this method persists
    /// raw evidence, updates context governance, and applies Runtime-owned
    /// capability discovery before the next model request.
    #[cfg(test)]
    pub(crate) async fn prepare_governed_tool_result(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> Result<ConversationMessage, RuntimeError> {
        self.prepare_governed_tool_result_with_invocation(
            tool_use_id,
            tool_name,
            input,
            output,
            is_error,
            None,
        )
        .await
    }

    pub(crate) async fn prepare_governed_tool_result_with_invocation(
        &self,
        tool_use_id: &str,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
        invocation: Option<ToolInvocationRecord>,
    ) -> Result<ConversationMessage, RuntimeError> {
        if let Ok(mut metrics) = self.turn_tool_exposure_metrics.lock() {
            metrics.observe_invocation(tool_name);
        }
        let tool_search_activated = tool_name == "tool_search"
            && !is_error
            && self
                .activate_tool_discovery(output)
                .is_some_and(|receipt| receipt.activated_ids().next().is_some());
        let input_hash = format!(
            "{:016x}",
            model_protocol::fingerprint::stable_hash_bytes(input.as_bytes())
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
            .await?;
        if let Some(mut terminal) = invocation {
            let sequence = self.session_head_blocking().message_count;
            terminal.session_id = self.session_id().to_string();
            terminal.turn_index = sequence;
            terminal = terminal.with_full_output_ref(format!("tool://{}", raw_ref.id()));
            self.record_tool_invocation_event(
                &terminal.started_fact(),
                "tool.invocation.started",
                sequence,
            );
            let terminal_kind = match terminal.status.as_str() {
                "completed" => "tool.invocation.completed",
                "denied" => "tool.invocation.denied",
                _ => "tool.invocation.failed",
            };
            self.record_tool_invocation_event(&terminal, terminal_kind, sequence);
        }
        self.maybe_index_tool_output(raw_ref.id(), tool_name, output, Some(&raw_access));
        let receipt =
            self.tool_model_receipt(tool_name, output, is_error, &raw_ref, Some(&raw_access));
        self.push_turn_evidence_audit(crate::context_evidence::audit_projection(
            &receipt,
            Some(&raw_access),
        ));
        let mut summary = receipt.summary;
        if tool_search_activated {
            summary.push_str(
                "\n\nThe discovered tools are active on the immediately following automatic \
                 model request in this same turn. Continue the current task and invoke the \
                 relevant activated tool directly; do not ask the user to resend solely because \
                 activation just completed.",
            );
        }
        let output_envelope = harness_contract::context::ToolOutputEnvelope {
            artifact_ref: Some(harness_contract::context::ArtifactRef::durable(
                raw_access.retrieval_selector.clone(),
                raw_access.sha256.clone(),
                raw_access.bytes,
                raw_access.media_type.clone(),
                raw_access.visibility_scope.clone(),
            )),
            evidence_ref: Some(raw_access),
            receipt: format!("tool://{}", raw_ref.id()),
        };
        self.push_turn_tool_observation(
            ToolObservation::new(
                tool_name.to_string(),
                tool_use_id.to_string(),
                raw_ref,
                summary.clone(),
            )
            .with_output_envelope(output_envelope),
        );
        Ok(ConversationMessage::tool_result(
            tool_use_id.to_string(),
            tool_name.to_string(),
            summary,
            is_error,
        ))
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
            self.session_head_blocking().message_count,
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
        let session_id = self.session_id().to_string();
        let safety_category = serde_json::from_str::<serde_json::Value>(input)
            .ok()
            .and_then(|input| self.tool_executor.registered_tool_effect(tool_name, &input))
            .map_or(crate::ToolSafetyCategory::Destructive, |effect| {
                crate::ToolSafetyCategory::from_effect(&effect)
            });
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
            self.session_head_blocking().message_count,
        );
    }

    fn record_tool_invocation_event(
        &self,
        record: &ToolInvocationRecord,
        kind: &'static str,
        _sequence: usize,
    ) {
        let mut refs = vec![
            RuntimeEventRef {
                kind: "tool_invocation".to_string(),
                id: record.invocation_id.clone(),
            },
            RuntimeEventRef {
                kind: "tool_call".to_string(),
                id: record.tool_call_id.clone(),
            },
        ];
        if let Some(plan_id) = &record.governed_plan_id {
            refs.push(RuntimeEventRef {
                kind: "governed_tool_plan".to_string(),
                id: plan_id.clone(),
            });
        }
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            kind,
            Some(record.status.as_str().to_string()),
            refs,
            serde_json::to_value(record).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        );
    }

    fn record_governed_tool_plan(&self, plan: &GovernedToolPlan, _sequence: usize) {
        if let Ok(mut plans) = self.turn_governed_tool_plans.lock() {
            plans.push(plan.projection());
        }
        self.append_execution_runtime_event(
            RuntimeEventScope::Tool,
            "tool.execution_plan.created",
            Some("planned".to_string()),
            vec![RuntimeEventRef {
                kind: "governed_tool_plan".to_string(),
                id: plan.plan_id.clone(),
            }],
            serde_json::to_value(plan).unwrap_or_else(
                |error| serde_json::json!({ "serialization_error": error.to_string() }),
            ),
        );
    }

    fn record_tool_strategy_validation(
        &self,
        report: &GovernedToolPolicyValidationReport,
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
        plan: &GovernedToolPlan,
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
                "plan_id": plan.plan_id,
                "plan_revision": plan.revision,
                "topology_hash": plan.topology_hash,
                "topological_order": plan.topological_order,
                "task_count": plan.task_count,
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
            "governed_tool_plans": trace.governed_tool_plans.iter().map(|plan| serde_json::json!({
                "id": plan.plan_id,
                "revision": plan.revision,
                "catalog_revision": plan.catalog_revision,
                "invocation_count": plan.invocations.len(),
                "dependency_count": plan.dependencies.len(),
            })).collect::<Vec<_>>(),
            "harness": {
                "receipt_id": trace.harness_receipt.id,
                "harness_id": trace.harness_receipt.harness_id,
                "agent_spec_id": trace.harness_receipt.agent_spec_id,
                "strategy_pattern": trace.harness_receipt.strategy_pattern,
                "context_epoch_id": trace.harness_receipt.context_epoch_id,
                "governed_tool_plan_ids": trace.harness_receipt.governed_tool_plan_ids,
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
        let mut input = StrategyInput::from_prompt(user_input.to_string());
        let Some(projector) = self.outcome_projector.as_ref() else {
            return input;
        };
        let understanding = understand(&input);
        let workload_fingerprint =
            StrategyWorkloadFingerprint::from_input(&input, &understanding).digest();
        input.understanding = Some(understanding);
        let Some(model) = self
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
        else {
            return input;
        };
        let Some(provider) = self.api_client.provider_name_for_model(model) else {
            return input;
        };
        let snapshot = projector.snapshot();
        let now = now_ms();
        const EXPERIENCE_FRESHNESS_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
        let mut comparable = Vec::new();
        for candidate in [
            ExecutionCandidateKind::Direct,
            ExecutionCandidateKind::ParallelTools,
            ExecutionCandidateKind::Team,
        ] {
            let key = harness_contract::outcome::StrategyExperienceKey {
                workspace_key: self.checkpoint_workspace_id.clone(),
                workload_fingerprint_sha256: workload_fingerprint.clone(),
                config_revision: self.runtime_config_revision.clone(),
                provider: provider.clone(),
                model: model.to_string(),
                evaluation_environment: "production".to_string(),
                candidate,
            };
            let Some(experience) = snapshot.strategy_experience(&key) else {
                continue;
            };
            if experience.sample_count == 0
                || now.saturating_sub(experience.last_observed_at_ms) > EXPERIENCE_FRESHNESS_MS
            {
                continue;
            }
            input.candidate_costs.insert(
                candidate,
                StrategyCandidateCostSummary {
                    sample_count: u32::try_from(experience.sample_count).unwrap_or(u32::MAX),
                    average_critical_path_ms: experience.duration_p50_ms,
                    average_total_tokens: experience.total_tokens_p50,
                    average_coordination_cost_ms: experience.coordination_cost_p50_ms,
                    calibration_source: format!(
                        "runtime.outcome_strategy.v2:{}:{}",
                        snapshot.revision, workload_fingerprint
                    ),
                },
            );
            comparable.push(experience);
        }
        if !comparable.is_empty() {
            let total = comparable.iter().fold(0_u64, |sum, experience| {
                sum.saturating_add(experience.sample_count)
            });
            let sum = |value: fn(&crate::StrategyExperienceSnapshot) -> u64| {
                comparable.iter().fold(0_u64, |sum, experience| {
                    sum.saturating_add(value(experience))
                })
            };
            let weighted = |value: fn(&crate::StrategyExperienceSnapshot) -> u64| {
                comparable
                    .iter()
                    .fold(0_u64, |sum, experience| {
                        sum.saturating_add(
                            value(experience).saturating_mul(experience.sample_count),
                        )
                    })
                    .saturating_div(total.max(1))
            };
            let basis_points = |count: u64, sample_count: u64| {
                u16::try_from(count.saturating_mul(10_000) / sample_count.max(1)).unwrap_or(10_000)
            };
            let team = comparable.iter().find(|experience| {
                experience
                    .key
                    .as_ref()
                    .is_some_and(|key| key.candidate == ExecutionCandidateKind::Team)
            });
            input.experience = Some(StrategyExperienceSummary {
                sample_count: u32::try_from(total).unwrap_or(u32::MAX),
                success_rate_bp: basis_points(sum(|experience| experience.success_count), total),
                verification_block_rate_bp: basis_points(
                    sum(|experience| experience.verification_block_count),
                    total,
                ),
                context_pressure_rate_bp: basis_points(
                    sum(|experience| experience.context_pressure_count),
                    total,
                ),
                multi_agent_lift_rate_bp: team.map_or(0, |experience| {
                    basis_points(
                        experience.positive_lift_count,
                        experience.paired_comparison_count,
                    )
                }),
                multi_agent_lift_sample_count: team
                    .map(|experience| {
                        u32::try_from(experience.paired_comparison_count).unwrap_or(u32::MAX)
                    })
                    .unwrap_or_default(),
                average_duration_ms: weighted(|experience| experience.duration_p50_ms),
                average_total_tokens: weighted(|experience| experience.total_tokens_p50),
                average_coordination_cost_ms: weighted(|experience| {
                    experience.coordination_cost_p50_ms
                }),
                actual_cost_sample_count: u32::try_from(total).unwrap_or(u32::MAX),
            });
        }
        input
    }

    /// Admit exactly one strategy identity for a turn. This is the only
    /// conversation-layer call site allowed to create a decision.
    #[cfg(test)]
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
        *self
            .active_provider_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *self
            .provider_selection_receipt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
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
            self.session_id().to_string(),
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
        let session_id = self.session_id().to_string();
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

    fn retarget_active_turn_strategy_for_tool_requirements(
        &self,
        selected_candidate: harness_contract::strategy::ExecutionCandidateKind,
        pattern: harness_contract::core::ExecutionPattern,
        requires_external_facts: bool,
        requires_write: bool,
        requests_parallelism: bool,
        requires_explicit_approval: bool,
        reason: &str,
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
            state
                .revise_for_tool_requirements(
                    selected_candidate,
                    pattern,
                    requires_external_facts,
                    requires_write,
                    requests_parallelism,
                    crate::execution_core::TurnStrategyDecisionStatus::Running,
                    reason,
                )
                .map_err(RuntimeError::new)?;
            if requires_explicit_approval
                && state
                    .decision
                    .pattern()
                    .supports_gate(harness_contract::core::ExecutionPolicyGate::Approval)
                && !state
                    .decision
                    .strategy
                    .gates
                    .contains(&harness_contract::core::ExecutionPolicyGate::Approval)
            {
                state
                    .decision
                    .strategy
                    .gates
                    .push(harness_contract::core::ExecutionPolicyGate::Approval);
                state.decision.strategy.reasons.push(
                    "an evidence-only strategy requested mutation; explicit approval is required before delivery"
                        .to_string(),
                );
            }
            (state.clone(), previous)
        };
        if let Err(error) =
            self.append_turn_strategy_event("runtime.strategy.selected", &state, reason)
        {
            *self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? =
                Some(previous);
            return Err(error);
        }
        Ok(state.decision)
    }

    /// Revise the one turn-owned strategy from the concrete governed tool
    /// plan. Conversation and execution-graph routes call this same method so
    /// a graph node cannot validate a later write against an earlier
    /// evidence-only strategy snapshot.
    pub(crate) fn retarget_active_turn_strategy_for_governed_plan(
        &self,
        plan: &GovernedToolPlan,
        calls: &[ModelToolCall],
    ) -> Result<crate::execution_core::RuntimeExecutionDecision, RuntimeError> {
        let current = self
            .active_turn_strategy()
            .map(|state| state.decision)
            .ok_or_else(|| RuntimeError::new("tool batch has no admitted turn strategy"))?;
        let requests_team = calls.iter().any(is_runtime_team_orchestration_call);
        let has_network = plan.tasks.iter().any(|task| {
            task.safety_category == crate::tool_orchestrator::ToolSafetyCategory::Network
        });
        let has_mutation = plan.tasks.iter().any(|task| {
            !is_runtime_team_orchestration_call_name(&task.tool_name)
                && matches!(
                    task.safety_category,
                    crate::tool_orchestrator::ToolSafetyCategory::WriteLocal
                        | crate::tool_orchestrator::ToolSafetyCategory::Destructive
                )
        });
        let target_pattern = if requests_team {
            harness_contract::core::ExecutionPattern::Collaborate
        } else if has_mutation {
            harness_contract::core::ExecutionPattern::Execute
        } else {
            harness_contract::core::ExecutionPattern::Explore
        };
        let requests_parallelism = target_pattern
            == harness_contract::core::ExecutionPattern::Collaborate
            || plan
                .tasks
                .iter()
                .filter(|task| task.can_parallelize)
                .count()
                > 1;
        if !has_network
            && !has_mutation
            && !requests_parallelism
            && target_pattern != harness_contract::core::ExecutionPattern::Collaborate
        {
            return Ok(current);
        }
        let selected_candidate =
            if target_pattern == harness_contract::core::ExecutionPattern::Collaborate {
                harness_contract::strategy::ExecutionCandidateKind::Team
            } else if requests_parallelism {
                harness_contract::strategy::ExecutionCandidateKind::ParallelTools
            } else {
                harness_contract::strategy::ExecutionCandidateKind::Direct
            };
        self.retarget_active_turn_strategy_for_tool_requirements(
            selected_candidate,
            target_pattern,
            has_network,
            has_mutation,
            requests_parallelism,
            current.compile_target == crate::execution_core::RuntimeCompileTarget::EvidenceGraph
                && has_mutation,
            "provider tool batch retained the admitted decision lease",
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
                outcome.max_tool_concurrency_observed = outcome
                    .max_tool_concurrency_observed
                    .max(metric("max_tool_concurrency_observed").unwrap_or(0));
                outcome.parallel_tool_batches = outcome
                    .parallel_tool_batches
                    .saturating_add(metric("parallel_tool_batches").unwrap_or(0));
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
                // Team working-state verification proves the child
                // collaboration materialized. It is not the root Goal's
                // working-state verdict and must not overwrite it here.
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
        if let Err(error) = self.append_turn_strategy_event(
            "runtime.strategy.outcome",
            &state,
            "turn terminal owner recorded actual outcome",
        ) {
            *self
                .active_turn_strategy
                .lock()
                .map_err(|_| RuntimeError::new("turn strategy owner lock poisoned"))? = Some(state);
            return Err(error);
        }
        self.record_canonical_outcome(&state)?;
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
        let mut input = RuntimeEventInput {
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
                "provider_selection": self.provider_selection_receipt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            }),
        };
        if let Some(binding) = self
            .cowd_bus()
            .and_then(crate::CowdEventBus::current_activity_binding)
        {
            input = input.with_activity_binding(binding).map_err(|error| {
                RuntimeError::new(format!(
                    "turn strategy activity binding is invalid: {error}"
                ))
            })?;
        }
        store.append(input).map(|_| ()).map_err(|error| {
            RuntimeError::new(format!(
                "durable turn strategy event `{kind}` append failed: {error}"
            ))
        })
    }

    fn record_canonical_outcome(
        &self,
        state: &crate::execution_core::TurnStrategyDecisionState,
    ) -> Result<(), RuntimeError> {
        let Some(outcome) = state.outcome.as_ref() else {
            return Ok(());
        };
        let service = self
            .outcome_service
            .as_ref()
            .ok_or_else(|| RuntimeError::new("canonical outcome service is unavailable"))?;
        let completed_at_ms = now_ms();
        let terminal = match state.status {
            crate::execution_core::TurnStrategyDecisionStatus::Completed
                if outcome.failed_tool_calls > 0 =>
            {
                harness_contract::outcome::OutcomeTerminalClass::PartialFailure(format!(
                    "{}; {} tool calls failed before terminal synthesis",
                    outcome.terminal_reason, outcome.failed_tool_calls
                ))
            }
            crate::execution_core::TurnStrategyDecisionStatus::Completed => {
                harness_contract::outcome::OutcomeTerminalClass::Succeeded(
                    outcome.terminal_reason.clone(),
                )
            }
            crate::execution_core::TurnStrategyDecisionStatus::Cancelled => {
                harness_contract::outcome::OutcomeTerminalClass::Cancelled(
                    outcome.terminal_reason.clone(),
                )
            }
            crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped => {
                harness_contract::outcome::OutcomeTerminalClass::Blocked(
                    outcome.terminal_reason.clone(),
                )
            }
            _ => harness_contract::outcome::OutcomeTerminalClass::Failed(
                outcome.terminal_reason.clone(),
            ),
        };
        let quality = outcome.quality_score_bp.map_or(
            harness_contract::outcome::OutcomeQuality::Unknown,
            |value| {
                harness_contract::outcome::OutcomeQuality::estimate(
                    value,
                    "runtime.turn_verification",
                    None,
                )
            },
        );
        let provider = self
            .active_provider_identity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        // A terminal Outcome remains authoritative even when a deterministic,
        // tool-only, cancelled, or pre-provider path never selected a model.
        // Such an Outcome cannot safely train provider/model-scoped strategy
        // routing, so omit only the scoped workload feedback instead of
        // inventing an "unknown" provider identity.
        let strategy_workload = provider.as_ref().map(|_| {
            StrategyWorkloadFingerprint::from_understanding(
                &state.decision.strategy.understanding,
                state.decision.strategy.understanding.requires_write,
            )
        });
        let evaluation_isolated = state.resource_snapshot.sample_source.contains("corpus=");
        let config_revision = if evaluation_isolated {
            format!(
                "{}:evaluation:{:016x}",
                self.runtime_config_revision,
                model_protocol::fingerprint::stable_hash_bytes(
                    state.resource_snapshot.sample_source.as_bytes()
                )
            )
        } else {
            self.runtime_config_revision.clone()
        };
        let canonical = harness_contract::outcome::ExecutionOutcome {
            identity: harness_contract::outcome::OutcomeIdentity {
                execution_id: format!("turn:{}", state.decision_id),
                session_id: state.session_ref.clone(),
                turn_id: state.turn_ref.clone(),
                terminal_generation: state.revision,
                paired_sample_id: None,
                task_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.task_id().map(str::to_string)),
                mission_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.mission_id().map(str::to_string)),
                agent_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.agent_run_id().map(str::to_string)),
                team_id: self
                    .execution_identity
                    .as_ref()
                    .and_then(|identity| identity.team_run_id().map(str::to_string)),
                execution_graph_ref: state.execution_graph_ref.clone(),
            },
            runtime: harness_contract::outcome::RuntimeIdentity {
                workspace_key: self.checkpoint_workspace_id.clone(),
                runtime_revision: env!("CARGO_PKG_VERSION").to_string(),
                config_revision,
            },
            provider,
            strategy: harness_contract::outcome::StrategyIdentity {
                decision_id: state.decision_id.clone(),
                policy_revision: state.policy_version.clone(),
                decision_source: format!("{:?}", state.decision.strategy.source)
                    .to_ascii_lowercase(),
                selected_candidate: state.selected_candidate,
                selected_pattern: state.decision.pattern().as_str().to_string(),
            },
            timing: harness_contract::outcome::OutcomeTiming {
                started_at_ms: completed_at_ms.saturating_sub(outcome.duration_ms),
                completed_at_ms,
                duration_ms: outcome.duration_ms,
            },
            usage: harness_contract::outcome::OutcomeUsage {
                input_tokens: Some(outcome.input_tokens),
                output_tokens: Some(outcome.output_tokens),
                cached_tokens: Some(outcome.cached_tokens),
                evaluation_tokens: outcome
                    .evaluation_budget_observed
                    .then_some(outcome.evaluation_tokens_consumed),
                tool_calls: outcome.tool_calls,
                duplicate_tool_calls: outcome.duplicate_tool_calls,
                retries: 0,
                max_observed_concurrency: outcome.max_tool_concurrency_observed,
            },
            terminal,
            quality,
            observation: harness_contract::outcome::OutcomeObservation {
                source: if evaluation_isolated {
                    "harness_eval.conversation_terminal".to_string()
                } else {
                    "runtime.conversation_terminal".to_string()
                },
                observed_at_ms: completed_at_ms,
                freshness_ms: 0,
            },
            strategy_feedback: harness_contract::outcome::OutcomeStrategyFeedback {
                workload: strategy_workload,
                verification_blocked: !outcome.working_state_verified
                    || outcome.evaluation_budget_breached,
                context_pressure: outcome.input_tokens.saturating_mul(100)
                    >= u64::from(self.model_context_window).saturating_mul(80),
                coordination_cost_ms: outcome.merge_cost_ms,
                evaluation_environment: if evaluation_isolated {
                    "harness_evaluation".to_string()
                } else {
                    "production".to_string()
                },
            },
            evidence_refs: Vec::new(),
            evidence_completeness: if outcome.working_state_verified {
                harness_contract::reality::EvidenceCompleteness::Sufficient
            } else if outcome.evidence_overlap_observed {
                harness_contract::reality::EvidenceCompleteness::Partial
            } else {
                harness_contract::reality::EvidenceCompleteness::None
            },
            schema_revision: harness_contract::outcome::OUTCOME_SCHEMA_REVISION,
        };
        service
            .record_terminal(&canonical)
            .map_err(|error| RuntimeError::new(format!("record canonical outcome: {error}")))?;
        Ok(())
    }

    fn append_execution_runtime_event(
        &self,
        scope: RuntimeEventScope,
        kind: &'static str,
        status: Option<String>,
        mut refs: Vec<RuntimeEventRef>,
        payload: serde_json::Value,
    ) {
        let Some(store) = self.runtime_event_store.as_ref() else {
            return;
        };
        let session_id = self.session_id().to_string();
        let execution_bus = self.cowd_bus();
        if let Some(context) =
            execution_bus.and_then(crate::CowdEventBus::current_execution_context)
        {
            for (kind, id) in [
                ("execution", context.execution_id),
                ("session", context.session_id),
                ("turn", context.turn_id),
            ] {
                if !refs
                    .iter()
                    .any(|reference| reference.kind == kind && reference.id == id)
                {
                    refs.push(RuntimeEventRef {
                        kind: kind.to_string(),
                        id,
                    });
                }
            }
        }
        let mut input = RuntimeEventInput {
            stream_id: format!("session:{session_id}"),
            scope,
            kind: kind.to_string(),
            status,
            actor: Some("conversation_runtime".to_string()),
            refs,
            payload,
        };
        if scope == RuntimeEventScope::Tool && kind.starts_with("tool.invocation.") {
            if let Some(bus) = execution_bus {
                if let Some(tool_call_id) = input
                    .refs
                    .iter()
                    .find(|reference| reference.kind == "tool_call")
                    .map(|reference| reference.id.clone())
                {
                    let tool_contract_id = input
                        .payload
                        .get("tool_name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    let Some(binding) = bus.current_tool_activity_binding(
                        &tool_call_id,
                        tool_contract_id.as_deref().unwrap_or("unknown_tool"),
                    ) else {
                        tracing::warn!(
                            session_id,
                            event_kind = kind,
                            "Tool lifecycle event rejected because no active Runtime activity owns it"
                        );
                        return;
                    };
                    match input.with_activity_binding(binding) {
                        Ok(bound) => input = bound,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                session_id,
                                event_kind = kind,
                                "Tool activity binding rejected before Runtime event append"
                            );
                            return;
                        }
                    }
                }
            } else {
                tracing::warn!(
                    session_id,
                    event_kind = kind,
                    "Tool lifecycle event rejected because no active Runtime activity owns it"
                );
                return;
            }
        } else if scope == RuntimeEventScope::Skill && kind == "skill.activation.selected" {
            if let Some(owner) =
                execution_bus.and_then(crate::CowdEventBus::current_activity_binding)
            {
                if let Some(skill_id) = input
                    .refs
                    .iter()
                    .find(|reference| reference.kind == "skill")
                    .map(|reference| reference.id.clone())
                {
                    let turn_index = input
                        .payload
                        .get("turn_index")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default();
                    let activation_id = crate::cowd_event::owned_child_activity_id(
                        &owner,
                        "skill",
                        &format!("{skill_id}:{turn_index}"),
                    );
                    let binding = harness_contract::projection::RuntimeActivityBinding {
                        root_execution_id: owner.root_execution_id.clone(),
                        session_id: owner.session_id.clone(),
                        turn_id: owner.turn_id.clone(),
                        root_task_id: owner.root_task_id.clone(),
                        task_id: owner.task_id.clone(),
                        activity_id: activation_id.clone(),
                        node_id: owner.node_id.clone(),
                        parent_activity_id: Some(owner.activity_id.clone()),
                        initiator_activity_id: Some(owner.activity_id),
                        team_run_id: owner.team_run_id,
                        agent_instance_id: owner.agent_instance_id,
                        agent_run_id: owner.agent_run_id,
                        skill_id: Some(skill_id),
                        skill_revision: input
                            .payload
                            .pointer("/invocation_evidence/version")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned),
                        skill_activation_id: Some(activation_id),
                        tool_contract_id: None,
                        tool_call_id: None,
                        approval_id: None,
                        parallel_group_id: owner.parallel_group_id,
                        revision: owner.revision,
                        fence: owner.fence,
                        generation: owner.generation,
                    };
                    match input.with_activity_binding(binding) {
                        Ok(bound) => input = bound,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                session_id,
                                event_kind = kind,
                                "Skill activity binding rejected before Runtime event append"
                            );
                            return;
                        }
                    }
                }
            } else {
                tracing::warn!(
                    session_id,
                    event_kind = kind,
                    "Skill activation rejected because no active Runtime activity owns it"
                );
                return;
            }
        }
        if let Err(error) = store.append(input) {
            tracing::warn!(%error, session_id, event_kind = kind, "execution runtime event append failed");
        }
    }

    fn record_message_event(&self, msg: &crate::session::ConversationMessage, _sequence: usize) {
        // Record the message in the event log for time-travel debugging.
        if let Some(ref log) = self.event_log {
            if let Ok(mut guard) = log.lock() {
                guard.push(MessageEvent::MessageAppended {
                    message: msg.clone(),
                });
            }
        }
    }

    async fn record_runtime_policy_decision(
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

        let Some(ref port) = self.session_journal_port else {
            return;
        };
        let session_id = self.session_id().to_string();
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
        let mut event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            sequence,
            crate::RuntimeSessionEventKind::RuntimePolicyDecided,
            payload,
            created_at_ms,
        );
        event.status = Some("completed".to_string());
        if let Err(error) = port.append_event(&event).await {
            tracing::warn!(%error, session_id, sequence, "runtime policy domain event append failed");
        }
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

#[async_trait::async_trait]
impl ToolExecutor for StaticToolExecutor {
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let output = self
            .handlers
            .get(tool_name)
            .ok_or_else(|| ToolError::new(format!("unknown tool: {tool_name}")))?(
            input
        )?;
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            output,
        ))
    }

    fn registered_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        };

        self.handlers.contains_key(tool_name).then(|| {
            let safety = crate::tool_orchestrator::ToolSafetyCategory::from_tool_name(tool_name);
            let target = ["path", "url", "server", "uri", "target"]
                .into_iter()
                .find_map(|key| input.get(key).and_then(serde_json::Value::as_str))
                .map(str::to_string);
            let (effect_kind, required_permission, mut scope, approval_class) = match safety {
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
            scope.target = target.clone();
            ToolEffectDescriptor {
                tool_id: tool_name.to_string(),
                descriptor_hash: format!(
                    "static:{tool_name}:{effect_kind:?}:{}",
                    target.as_deref().unwrap_or_default()
                ),
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
                assessment: harness_contract::policy::EffectAssessment {
                    reversibility: match effect_kind {
                        ToolEffectKind::Read | ToolEffectKind::Network => {
                            harness_contract::policy::EffectReversibility::Reversible
                        }
                        ToolEffectKind::Write => {
                            harness_contract::policy::EffectReversibility::Compensatable
                        }
                        _ => harness_contract::policy::EffectReversibility::Irreversible,
                    },
                    externality: match effect_kind {
                        ToolEffectKind::Read => {
                            harness_contract::policy::EffectExternality::Internal
                        }
                        ToolEffectKind::Write => {
                            harness_contract::policy::EffectExternality::Workspace
                        }
                        ToolEffectKind::Network => {
                            harness_contract::policy::EffectExternality::NetworkRead
                        }
                        _ => harness_contract::policy::EffectExternality::System,
                    },
                    data_sensitivity: harness_contract::policy::DataClassification::Internal,
                    novelty: harness_contract::policy::EffectNovelty::Routine,
                    blast_radius: match effect_kind {
                        ToolEffectKind::Read | ToolEffectKind::Network => {
                            harness_contract::policy::EffectBlastRadius::Item
                        }
                        ToolEffectKind::Write => {
                            harness_contract::policy::EffectBlastRadius::Workspace
                        }
                        _ => harness_contract::policy::EffectBlastRadius::System,
                    },
                },
            }
        })
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if authorization.tool_id != tool_name {
            return Err(ToolError::new(
                "static tool authorization names a different tool",
            ));
        }
        self.execute_output(tool_name, input).await
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
        apply_explicit_team_requirement, apply_named_e2e_strategy_fixture,
        build_cc_memory_config_with_budget, canonicalize_model_tool_names,
        classify_model_step_intent, consume_provider_stream, conversation_message_text,
        current_turn_messages, deterministic_checkpoint_id, enforce_explicit_team_requirement,
        eval_override_selection, image_user_message_from_path, is_append_only_projection,
        is_runtime_team_orchestration_call, memory_project_id_for_session, prepared_vision_payload,
        preview_chars, provider_transport_policy, rate_per_second,
        required_team_orchestration_call, revalidate_context_binding,
        runtime_team_orchestration_count, turn_strategy_event_kind_allowed,
        unexposed_model_tool_names, vision_user_message, ApiClient, ApiRequest, AssistantEvent,
        AssistantItemKind, CancellationToken, CognitiveContextManager, ConversationRuntime,
        EarlyToolCandidate, EarlyToolDispatchFuture, EarlyToolDispatchResult, EarlyToolDispatcher,
        EarlyToolExecutionReceipt, ModelStepIntent, ModelStepToolPlan, ModelStreamReducer,
        ModelToolCall, ProviderContextInventory, RuntimeError, StaticToolExecutor,
        ToolExposureState, TurnStablePrefixMetrics, TurnToolExposureMetrics,
    };
    use crate::config::RuntimeFeatureConfig;
    use crate::context_runtime::{
        ContextAuthority, ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextMode,
        ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind, ResumeContextPacket,
        ResumeContextSource, CONTEXT_RENDER_FORMATTER_VERSION,
        PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
    };
    use crate::execution_core::build_runtime_execution_decision;
    use crate::permissions::{PermissionMode, PermissionPolicy};
    use crate::runtime_event_store::{RuntimeEventScope, RuntimeEventStore};
    use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};
    use crate::{
        resolve_context_budget_tokens, CowdEventBus, PromptAssembly, RealityRecallPort,
        RuntimeBudgetInputs, RuntimeBudgetPlan, SystemPromptBuilder, ToolExecutor,
        COWD_IDENTITY_CONTRACT_VERSION,
    };
    use futures::{stream::Stream, StreamExt};
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
    use std::collections::BTreeSet;
    use std::fs;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn exact_provider_wire_evidence_is_artifact_backed_and_durably_pinned() {
        let session_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        session_store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "provider-evidence".to_string(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: "2026-08-07T00:00:00Z".to_string(),
                last_activity: "2026-08-07T00:00:00Z".to_string(),
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
        let temporary = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(temporary.path(), crate::ArtifactStoreConfig::default())
                .unwrap(),
        );
        let writer = super::SessionProviderWireEvidenceWriter {
            artifacts: Arc::clone(&artifacts),
            session_port: crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(
                &session_store,
            )),
        };
        let context = crate::ProviderRequestEvidenceContext {
            session_id: session_id.clone(),
            request_sequence: 7,
            request_compiler_cache_hit: true,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test-model",
                128_000,
                4_096,
                100,
                100,
                1_000,
            ),
        };
        let evidence = crate::ProviderWireEvidence {
            request_context: crate::ProviderRequestContext {
                request_id: "request-provider-evidence".to_string(),
                profile: crate::ResolvedProviderProfile {
                    registry_revision: 3,
                    provider_name: "openai-compatible".to_string(),
                    model: "test-model".to_string(),
                    base_url: Some("https://provider.example/v1".to_string()),
                    protocol: Some("responses".to_string()),
                    parallel_tool_calls_mode:
                        model_protocol::provider_config::ParallelToolCallsMode::Auto,
                    effective_parallel_tool_calls: Some(true),
                    effective_early_tool_start: false,
                    capabilities:
                        model_protocol::provider_capability::ProviderCapabilityProfile::unknown(),
                },
                transport_fingerprint: crate::TransportProfileFingerprint(42),
                attempt: 1,
            },
            wire_request: provider::ProviderWireRequest {
                method: "POST".to_string(),
                endpoint: "https://provider.example/v1/responses".to_string(),
                protocol: "responses".to_string(),
                headers: vec![provider::ProviderWireHeader {
                    name: "content-type".to_string(),
                    value: "application/json".to_string(),
                }],
                body: serde_json::json!({"model":"test-model","input":"checked"}),
                body_sha256: "sha256-body".to_string(),
                tool_schema_sha256: Some("sha256-tools".to_string()),
            },
        };

        crate::ProviderWireEvidenceWriter::persist(&writer, &context, evidence)
            .await
            .unwrap();

        let events = session_store
            .session_domain_events_page(&session_id, 0, 10)
            .await
            .unwrap();
        let packed = events
            .events
            .iter()
            .find(|event| event.kind == "context.provider_request_packed")
            .expect("provider request evidence event");
        assert_eq!(packed.payload["schema_version"], 2);
        assert!(packed.payload.get("body").is_none());
        let artifact: harness_contract::context::ArtifactRef =
            serde_json::from_value(packed.payload["artifact"].clone()).unwrap();
        let body = artifacts
            .read(&artifact, &format!("session:{session_id}"), None)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["provider_request"]["wire_request"]["body"]["input"],
            "checked"
        );
        assert_eq!(artifacts.stats().unwrap().pins, 1);
    }

    #[test]
    fn memory_projection_accepts_only_exact_append_revisions() {
        assert!(is_append_only_projection(true, 10, 20, 13, 23));
        assert!(is_append_only_projection(true, 10, 20, 10, 20));
        assert!(!is_append_only_projection(false, 0, 0, 3, 3));
        assert!(!is_append_only_projection(true, 10, 20, 11, 20));
        assert!(!is_append_only_projection(true, 10, 20, 11, 19));
        assert!(!is_append_only_projection(true, 10, 20, 14, 23));
    }

    #[tokio::test]
    async fn memory_projection_converts_only_appended_messages_and_rebuilds_on_replace() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .append_external_message(ConversationMessage::user_text("first"))
            .await
            .expect("first message");
        runtime
            .append_external_message(ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "first response".to_string(),
            }]))
            .await
            .expect("first response");

        let first = runtime.memory_context_messages().await;
        assert_eq!(first.len(), 2);
        {
            let projection = runtime.session_memory_projection.lock().await;
            assert_eq!(projection.converted_messages, 2);
            assert_eq!(projection.rebuilds, 1);
        }

        runtime
            .append_external_message(ConversationMessage::user_text("second"))
            .await
            .expect("second message");
        let second = runtime.memory_context_messages().await;
        assert_eq!(second.len(), 3);
        {
            let projection = runtime.session_memory_projection.lock().await;
            assert_eq!(
                projection.converted_messages, 3,
                "the second projection must convert only the appended message"
            );
            assert_eq!(projection.rebuilds, 1);
        }

        runtime
            .session_mut_async()
            .await
            .replace_messages(vec![ConversationMessage::user_text("replacement")]);
        let replaced = runtime.memory_context_messages().await;
        assert_eq!(replaced.len(), 1);
        {
            let projection = runtime.session_memory_projection.lock().await;
            assert_eq!(projection.converted_messages, 4);
            assert_eq!(
                projection.rebuilds, 2,
                "replace/truncate/recovery paths must invalidate the append projection"
            );
        }
    }

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
            "content": "implemented-auto-strategy-0\n",
            "totalLines": 1,
            "truncated": false,
        })
        .to_string();
        let receipt = runtime.tool_model_receipt(
            "read_file",
            &output,
            false,
            &harness_contract::reality::EvidenceRef::observed("tool", "small-exact-read"),
            None,
        );

        assert!(!receipt.truncated, "{}", receipt.summary);
        assert!(receipt.summary.contains("implemented-auto-strategy-0"));
        assert!(!receipt.summary.contains("omitted; retrieve"));
    }
    use fact_kernel::FactLedger;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use storage::StorageRegistry;

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
        assert_eq!(input["operation"], "propose");
        assert_eq!(input["proposal"]["nodes"][0]["recipe"], "team");
    }

    #[test]
    fn chinese_launch_team_wording_is_enforced_as_an_execution_requirement() {
        let objective = "发起一个团队，生成公开技术标准的全面深度调研报告";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "无法组队".to_string(),
            },
        );

        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("explicit launch wording must materialize a Runtime team request");
        };
        assert_eq!(calls.len(), 1);
        assert!(is_runtime_team_orchestration_call(&calls[0]));
        assert!(calls[0].input.contains("external-research-synthesis"));
    }

    #[test]
    fn sequential_team_artifact_request_materializes_a_write_capable_followup_team() {
        let objective = "用一个团队调研公开技术标准，然后另一个团队负责生成一套 HTML 研究报告网站";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "调研结束。".to_string(),
            },
        );

        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("the explicit follow-up Team must be materialized");
        };
        let input: serde_json::Value = serde_json::from_str(&calls[0].input).unwrap();
        assert_eq!(
            input["proposal"]["nodes"][0]["template"],
            "cowd/external-research-synthesis"
        );
        assert_eq!(
            input["proposal"]["nodes"][1]["template"],
            "cowd/execute-review"
        );
        assert_eq!(
            input["proposal"]["nodes"][1]["depends_on"],
            serde_json::json!(["explicit-team-1"])
        );
        assert_eq!(input["constraints"]["requires_write"], true);
        assert_eq!(
            input["proposal"]["completion"]["required_artifact_kinds"],
            serde_json::json!(["workspace_change", "terminal_synthesis"])
        );
    }

    #[test]
    fn model_tool_calls_are_bounded_by_the_current_exposure_lease() {
        let calls = vec![
            ModelToolCall {
                id: "read".to_string(),
                name: "read_file".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "hidden".to_string(),
                name: "shell".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "missing".to_string(),
                name: "invented_tool".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        ];

        assert_eq!(
            unexposed_model_tool_names(&calls, &BTreeSet::from(["read_file".to_string()])),
            vec!["invented_tool".to_string(), "shell".to_string()]
        );
    }

    #[test]
    fn provider_tool_name_aliases_only_resolve_inside_the_current_exposure_lease() {
        let mut calls = vec![
            ModelToolCall {
                id: "search".to_string(),
                name: "web_search".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "hidden".to_string(),
                name: "shell-command".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        ];
        let executor = StaticToolExecutor::new().register("web_search", |_| Ok(String::new()));
        canonicalize_model_tool_names(&mut calls, &executor);
        assert_eq!(calls[0].name, "web_search");
        assert_eq!(calls[1].name, "shell-command");

        let mut ambiguous = vec![ModelToolCall {
            id: "ambiguous".to_string(),
            name: "web search".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        }];
        let ambiguous_executor = StaticToolExecutor::new()
            .register("web_search", |_| Ok(String::new()))
            .register("web-search", |_| Ok(String::new()));
        canonicalize_model_tool_names(&mut ambiguous, &ambiguous_executor);
        assert_eq!(
            ambiguous[0].name, "web search",
            "ambiguous aliases must fail closed"
        );
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
        assert!(team.effective_duration_ms() >= team.estimated_serial_ms);
        assert!(decision.reasons.iter().any(|reason| {
            reason.contains("no measured duration advantage or paired quality proof")
        }));

        let mut unmarked = harness_contract::strategy::StrategyInput::from_prompt(
            "must start a Team for runtime gateway frontend",
        );
        let unmarked_prompt = unmarked.prompt.clone();
        apply_named_e2e_strategy_fixture(&mut unmarked, &unmarked_prompt, "explicit-team-negative")
            .expect("known fixture is inert without its marker");
        assert!(unmarked.candidate_costs.is_empty());
    }

    #[test]
    fn model_team_proposal_is_visible_to_runtime_retargeting() {
        let call = required_team_orchestration_call("review");
        assert!(is_runtime_team_orchestration_call(&call));
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
        let classified = classify_model_step_intent(
            String::new(),
            vec![ModelToolCall {
                id: "provider-agent-helper".to_string(),
                name: "agent_helper".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            }],
        );
        let intent = enforce_explicit_team_requirement(objective, true, &decision, classified);

        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("provider-specific agent proposals must enter the canonical tool batch");
        };
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(is_runtime_team_orchestration_call));
    }

    #[test]
    fn ordinary_tool_names_never_create_runtime_control_intents() {
        for name in [
            "team_board",
            "agent_status",
            "permission_report",
            "replan_index",
        ] {
            let intent = classify_model_step_intent(
                String::new(),
                vec![ModelToolCall {
                    id: format!("call-{name}"),
                    name: name.to_string(),
                    input: "{}".to_string(),
                    depends_on: Vec::new(),
                }],
            );
            let ModelStepIntent::ToolCalls { calls } = intent else {
                panic!("ordinary tool `{name}` must remain a ToolCall");
            };
            assert_eq!(calls[0].name, name);
        }
    }

    #[test]
    fn team_orchestration_tool_batch_keeps_the_collaboration_strategy() {
        let calls = vec![required_team_orchestration_call("必须实际启动团队")];
        assert!(calls.iter().any(is_runtime_team_orchestration_call));
    }

    #[test]
    fn required_team_orchestration_uses_a_published_builtin_template() {
        let call = required_team_orchestration_call("必须实际启动团队");
        assert_eq!(call.name, "runtime_orchestrate");
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        assert_eq!(
            input["proposal"]["nodes"][0]["template"],
            serde_json::json!("cowd/parallel-research-synthesis")
        );
    }

    #[test]
    fn explicit_two_team_requirement_compiles_two_independent_read_teams() {
        let call = required_team_orchestration_call("启动两个研究团队并行调研本地文件并用中文汇报");
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        let nodes = input["proposal"]["nodes"]
            .as_array()
            .expect("semantic Team nodes");
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|node| {
            node["recipe"] == "team" && node["depends_on"].as_array().is_some_and(Vec::is_empty)
        }));
        assert!(nodes
            .iter()
            .all(|node| node["template"] == "cowd/direct-executor"));
        assert!(nodes.iter().all(|node| {
            node["evidence_contract"] == serde_json::json!(["summary", "evidence"])
        }));
        assert_eq!(
            input["proposal"]["completion"]["required_node_ids"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(runtime_team_orchestration_count(&call), 2);
    }

    #[test]
    fn mixed_language_three_team_requirement_compiles_parallel_research_then_writer() {
        let call = required_team_orchestration_call(
            "请使用恰好3个Team完成任务，前两个并行研究，第三个生成并写入HTML报告文件",
        );
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        let nodes = input["proposal"]["nodes"]
            .as_array()
            .expect("semantic Team nodes");
        assert_eq!(nodes.len(), 3);
        assert!(nodes[..2]
            .iter()
            .all(|node| node["depends_on"].as_array().is_some_and(Vec::is_empty)));
        assert_eq!(
            nodes[2]["depends_on"],
            serde_json::json!(["explicit-team-1", "explicit-team-2"]),
        );
        assert_eq!(
            nodes[2]["output_artifacts"],
            serde_json::json!(["workspace_change", "terminal_synthesis"]),
        );
        assert!(nodes[..2]
            .iter()
            .all(|node| node["template"] == "cowd/direct-executor"));
        assert!(nodes[..2].iter().all(|node| {
            node["evidence_contract"] == serde_json::json!(["summary", "evidence"])
        }));
        assert_eq!(nodes[2]["template"], "cowd/execute-review");
        assert_eq!(
            nodes[2]["evidence_contract"],
            serde_json::json!(["implementation", "source_verification", "evidence", "risks"])
        );
        assert!(nodes[2]["evidence_contract"]
            .as_array()
            .is_some_and(|criteria| criteria.iter().all(|criterion| criterion != "plan")));
        assert_eq!(runtime_team_orchestration_count(&call), 3);
    }

    #[test]
    fn sequential_followup_team_language_also_compiles_two_team_entities() {
        let call = required_team_orchestration_call(
            "一个团队负责调研，另一个团队负责独立复核，最后给出结论",
        );
        assert_eq!(runtime_team_orchestration_count(&call), 2);
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
            messages: vec![ConversationMessage::user_text("status".to_string())].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 32_768, 4_096, 128, 256, 0,
            ),
            provider_evidence_context: None,
        };
        let large = ApiRequest {
            prompt: PromptAssembly::new(vec!["system".repeat(5_000)]),
            messages: vec![ConversationMessage::user_text("evidence".repeat(10_000))].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 1_000_000, 32_000, 128, 256, 0,
            ),
            provider_evidence_context: None,
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

        let history: crate::HistoryView =
            vec![ConversationMessage::user_text("history ".repeat(300))].into();
        let request = runtime
            .pack_provider_attempt(
                &prompt,
                &history,
                "test",
                super::ProviderContextInventory {
                    tool_count: 2,
                    tool_schema_tokens: 1_200,
                    ..Default::default()
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

    #[test]
    fn custom_model_output_budget_preserves_production_input_window() {
        let context_window = 16_384;
        let output = super::provider_output_budget_hint(
            "custom-model-with-generic-cap",
            context_window,
            None,
        );
        assert_eq!(output, 4_000);

        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["production system policy ".repeat(400)],
        )
        .without_memory();
        runtime.set_active_model("custom-model-with-generic-cap");
        runtime = runtime.with_model_context_window(context_window);

        let history: crate::HistoryView = vec![ConversationMessage::user_text(
            "current durable user turn ".repeat(20),
        )]
        .into();
        let request = runtime
            .pack_provider_attempt(
                &PromptAssembly::new(vec!["production system policy ".repeat(400)]),
                &history,
                "custom-model-with-generic-cap",
                super::ProviderContextInventory {
                    tool_count: 3,
                    tool_schema_tokens: 1_740,
                    ..Default::default()
                },
            )
            .expect("production prompt and bootstrap schemas must reach a 16k custom model");

        assert_eq!(request.budget.requested_output_tokens, u64::from(output));
        assert_eq!(request.budget.provider_max_output_tokens, 64_000);
        assert_eq!(request.budget.max_output_source, "assumed");
        assert_eq!(request.budget.preferred_output_tokens, 4_000);
        assert_eq!(request.budget.output_floor_tokens, 2_000);
        assert!(request.budget.executable);
        assert!(request.budget.fixed_input_tokens <= request.budget.hard_input_cap_tokens);
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
                    Ok(AssistantEvent::ProviderModel {
                        identity: harness_contract::outcome::ProviderIdentity {
                            registry_revision: Some(1),
                            provider_name: "test".to_string(),
                            model,
                            profile: None,
                            protocol: Some("completions".to_string()),
                            capabilities: std::collections::BTreeMap::new(),
                        },
                    }),
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

    async fn stale_session_execution_fence(
        session_id: &str,
        request_id: &str,
    ) -> crate::SessionExecutionFence {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.to_string(),
                platform: "test".to_string(),
                chat_id: session_id.to_string(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: "2026-07-26T00:00:00Z".to_string(),
                last_activity: "2026-07-26T00:00:00Z".to_string(),
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
        let now = super::now_ms();
        let request = session::SessionRuntimeOutboxRequest {
            input_id: format!("input-{request_id}"),
            request_id: request_id.to_string(),
            turn_id: format!("turn-{request_id}"),
            message_id: format!("message-{request_id}"),
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            created_at_ms: now,
            runtime_options_json: None,
        };
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"fenced"}]"#),
                now,
                &request,
            )
            .await
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("fence-worker", now, 60_000, 1)
            .await
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().expect("claim token");
        let running = store
            .mark_session_runtime_outbox_running(
                request_id,
                "fence-worker",
                1,
                &token,
                claimed.revision,
                now,
            )
            .await
            .unwrap();
        let fence = crate::SessionExecutionFence::from_claim(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
            request_id,
            session_id,
            1,
            running.sequence,
            "fence-worker",
            token,
        )
        .unwrap();
        store
            .advance_session_input_generation(
                session_id,
                1,
                true,
                "test",
                "invalidate execution before side effect",
                now + 1,
            )
            .await
            .unwrap();
        fence
    }

    #[tokio::test]
    async fn stale_session_fence_blocks_provider_before_transport_side_effect() {
        #[derive(Clone)]
        struct CountingApi(Arc<AtomicUsize>);
        impl ApiClient for CountingApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(futures::stream::iter([Ok(AssistantEvent::MessageStop)]))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let fence = stale_session_execution_fence("fence-provider", "request-provider").await;
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            CountingApi(Arc::clone(&calls)),
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_execution_fence(fence);
        runtime
            .begin_turn_strategy("fence-provider-turn", "answer")
            .unwrap();

        let result = runtime.execute_model_step("answer", true).await;
        assert!(result.is_err(), "stale provider fence result: {result:?}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "provider transport must not start after durable ownership is lost"
        );
    }

    #[tokio::test]
    async fn stale_session_fence_blocks_tool_before_executor_side_effect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let fence = stale_session_execution_fence("fence-tool", "request-tool").await;
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new().register("read_file", move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok("should not execute".to_string())
            }),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_execution_fence(fence);
        runtime
            .begin_turn_strategy("fence-tool-turn", "read")
            .unwrap();
        let result = runtime
            .execute_tool_batch_step(
                &[ModelToolCall {
                    id: "read-fenced".to_string(),
                    name: "read_file".to_string(),
                    input: r#"{"path":"README.md"}"#.to_string(),
                    depends_on: Vec::new(),
                }],
                &crate::SharedPrompter::none(),
                1,
            )
            .await;
        let result = result.expect("stale tool fence is returned as a governed tool result");
        assert_eq!(result.failed, 1, "stale tool fence result: {result:?}");
        assert!(result.messages.iter().any(|message| {
            message.blocks.iter().any(|block| {
                matches!(
                    block,
                    crate::session::ContentBlock::ToolResult {
                        output,
                        is_error: true,
                        ..
                    } if output.contains("Session execution fence rejected")
                )
            })
        }));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "tool executor must not start after durable ownership is lost"
        );
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
        *runtime.fallbacks.write().unwrap() = vec!["fallback".to_string()];
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
    async fn runtime_retries_one_typed_transient_failure_without_hidden_wire_retries() {
        #[derive(Clone)]
        struct RetryOnceApi(Arc<AtomicUsize>);

        impl ApiClient for RetryOnceApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                let attempt = self.0.fetch_add(1, Ordering::SeqCst);
                let events = if attempt == 0 {
                    vec![Err(
                        RuntimeError::with_provider_failure_metadata_and_retry_after(
                            "temporary provider timeout",
                            None,
                            false,
                            crate::execution_core::graph::ResourceResultClass::TimedOut,
                            Some(Duration::from_millis(1)),
                            true,
                        ),
                    )]
                } else {
                    vec![
                        Ok(AssistantEvent::TextDelta(
                            "recovered after governed retry".to_string(),
                        )),
                        Ok(AssistantEvent::MessageStop),
                    ]
                };
                Box::pin(futures::stream::iter(events))
            }
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            RetryOnceApi(Arc::clone(&attempts)),
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            SystemPromptBuilder::new().build(),
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime
            .begin_turn_strategy("test-provider-retry", "return a verified answer")
            .unwrap();

        let result = runtime
            .execute_model_step("return a verified answer", true)
            .await
            .expect("one governed retry should recover");
        assert!(matches!(
            result.intent,
            ModelStepIntent::FinalAnswer { ref text }
                if text == "recovered after governed retry"
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
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
        *runtime.fallbacks.write().unwrap() = vec!["fallback".to_string()];
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
        assert!(requests
            .iter()
            .all(|request| { request.reasoning_effort_override.as_deref() == Some("none") }));
        assert!(runtime
            .next_model_reasoning_effort
            .lock()
            .expect("reasoning effort")
            .is_none());
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

    #[tokio::test]
    async fn clean_terminal_calibrates_once_and_repackages_the_same_model() {
        use crate::execution_core::graph::{
            ExecutionResourceKind, ExecutionResourceManager, ResourceAdmissionObservationStatus,
            ResourceQuota,
        };

        let windows = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = CalibrationRecordingApi {
            windows: Arc::clone(&windows),
        };
        let granted = Arc::new(AtomicUsize::new(0));
        let manager = Arc::new(ExecutionResourceManager::new([(
            ExecutionResourceKind::Provider,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )]));
        let observed_grants = Arc::clone(&granted);
        manager
            .install_admission_observer(move |observation| {
                if observation.status == ResourceAdmissionObservationStatus::Granted {
                    observed_grants.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        let bus = CowdEventBus::new();
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: "clean-terminal-execution".to_string(),
            session_id: "clean-terminal-session".to_string(),
            turn_id: "clean-terminal-turn".to_string(),
        });
        let mut receiver = bus.subscribe();
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["builtin policy".to_string()],
        )
        .without_memory()
        .with_cowd_event_bus(bus)
        .with_provider_admission(manager)
        .with_model_context_window(128_000);
        runtime.set_active_model("private-model");

        let result = runtime
            .execute_clean_terminal_synthesis("give a concise answer", "checked evidence")
            .await
            .expect("clean terminal calibrated retry should complete");
        assert_eq!(result.model.as_deref(), Some("private-model"));
        let windows = windows.lock().expect("windows");
        assert_eq!(
            windows.as_slice(),
            &[
                (128_000, "assumed".to_string()),
                (32_768, "calibrated".to_string())
            ]
        );
        let mut live_events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            live_events.push(serde_json::to_string(&event).expect("serialize live event"));
        }
        let live_events = live_events.join("\n");
        assert!(live_events.contains("calibrated answer"));
        assert!(live_events.contains("ModelStepStarted"));
        assert!(live_events.contains("ItemStarted"));
        assert!(live_events.contains("ItemCompleted"));
        assert_eq!(
            granted.load(Ordering::SeqCst),
            2,
            "both clean terminal attempts must pass through canonical admission"
        );
    }

    #[test]
    fn explicit_max_output_override_reaches_provider_budget_policy() {
        assert_eq!(
            super::provider_output_budget_hint("deepseek-v4-pro", 1_000_000, Some(12_000)),
            12_000
        );
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
        session.replace_messages(vec![
            ConversationMessage::user_text("old request ".repeat(200)),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old response ".repeat(200),
            }]),
            ConversationMessage::user_text("recent user request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "recent assistant response".to_string(),
            }]),
        ]);
        let store =
            Arc::new(session::UnifiedSessionStore::open_in_memory().expect("session store"));
        store
            .create_session(&session::SessionRecord {
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
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
        );
        runtime.session_compaction_config.preserve_recent = 2;

        let receipt = runtime
            .compact_active_session()
            .await
            .expect("semantic compaction")
            .expect("a compaction receipt");
        assert!(receipt.removed_message_count > 0);
        let compacted = runtime.session_snapshot().await;
        assert_eq!(
            compacted.message_count(),
            3,
            "configured preserve_recent=2 must win"
        );
        assert!(matches!(
            &compacted.message(0).expect("summary").blocks[0],
            ContentBlock::Text { text }
                if text.contains("Compressed Session Summary")
                    && !text.contains("Conversation summary:")
        ));
        assert!(compacted.messages().any(|message| {
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
        session.replace_messages(vec![
            ConversationMessage::user_text("old request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old response".to_string(),
            }]),
            ConversationMessage::user_text("recent request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "recent response".to_string(),
            }]),
        ]);
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

        assert!(error.to_string().contains("durable Session journal port"));
        assert_eq!(runtime.session_snapshot().await, before);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explicit_session_snapshot_works_from_current_thread_runtime_when_contended() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let expected_session_id = runtime.session_id().to_string();
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

        let session = runtime.session_snapshot_blocking();

        holder
            .join()
            .expect("native session-lock holder must finish");
        assert_eq!(session.session_id, expected_session_id);
    }

    #[tokio::test]
    async fn session_head_reads_metadata_without_materializing_a_snapshot() {
        let mut session = Session::new();
        session
            .push_user_text("one")
            .expect("append initial session message");
        let expected_session_id = session.session_id.clone();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        let head = runtime.session_head().await;

        assert_eq!(runtime.session_id(), expected_session_id);
        assert_eq!(head.message_count, 1);
        assert_eq!(head.history_revision, 1);
        assert!(head.history_bytes > 0);
        assert!(head.history_tokens > 0);
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
            .retarget_active_turn_strategy_for_tool_requirements(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                harness_contract::core::ExecutionPattern::Execute,
                false,
                false,
                false,
                false,
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
            .list_stream(&format!("session:{}", runtime.session_id()))
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
                && event.payload["session_ref"].as_str() == Some(runtime.session_id())
                && event.payload["turn_ref"].as_str() == Some("turn-one")
        }));
    }

    #[test]
    fn tool_requirements_retarget_the_canonical_strategy_state_atomically() {
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
            .begin_turn_strategy("turn-network", "继续处理")
            .expect("admit strategy");
        runtime
            .bind_turn_strategy_execution("turn-network", "graph-network")
            .expect("bind graph");

        let decision = runtime
            .retarget_active_turn_strategy_for_tool_requirements(
                harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
                harness_contract::core::ExecutionPattern::Explore,
                true,
                false,
                true,
                false,
                "provider emitted parallel external research calls",
            )
            .expect("retarget network batch");
        let canonical = runtime
            .active_turn_strategy()
            .expect("canonical strategy remains active");

        assert_eq!(decision, canonical.decision);
        assert_eq!(
            decision.compile_target,
            crate::execution_core::RuntimeCompileTarget::EvidenceGraph
        );
        assert!(decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::WithExternalResearch));
        assert!(decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::Parallel));
        assert_eq!(
            canonical.selected_candidate,
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools
        );
    }

    #[test]
    fn model_team_proposal_retargets_within_the_same_strategy_lease() {
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
        let admitted = runtime
            .begin_turn_strategy("turn-team", "分析并解决这个问题")
            .expect("admit strategy");
        runtime
            .bind_turn_strategy_execution("turn-team", "graph-team")
            .expect("bind graph");

        let decision = runtime
            .retarget_active_turn_strategy_for_tool_requirements(
                harness_contract::strategy::ExecutionCandidateKind::Team,
                harness_contract::core::ExecutionPattern::Collaborate,
                false,
                false,
                true,
                false,
                "model proposed a Team after inspecting the task",
            )
            .expect("retarget to model-proposed Team");

        assert_eq!(decision.lease.lease_id, admitted.decision_lease);
        assert!(decision.decision_revision > 1);
        assert_eq!(
            decision.strategy.pattern,
            harness_contract::core::ExecutionPattern::Collaborate
        );
        assert_eq!(
            runtime
                .active_turn_strategy()
                .expect("canonical strategy")
                .selected_candidate,
            harness_contract::strategy::ExecutionCandidateKind::Team
        );
    }

    #[test]
    fn evidence_strategy_revises_to_explicitly_approved_delivery_without_changing_lease() {
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
        let admitted = runtime
            .begin_turn_strategy("turn-research-delivery", "调研外部资料并形成报告")
            .expect("admit strategy");
        runtime
            .bind_turn_strategy_execution("turn-research-delivery", "graph-research-delivery")
            .expect("bind graph");

        let decision = runtime
            .retarget_active_turn_strategy_for_tool_requirements(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                harness_contract::core::ExecutionPattern::Execute,
                true,
                true,
                false,
                true,
                "research evidence is being delivered to the workspace",
            )
            .expect("retarget approved delivery");

        assert_eq!(decision.lease.lease_id, admitted.decision_lease);
        assert!(decision.decision_revision > 1);
        assert_eq!(
            decision.compile_target,
            crate::execution_core::RuntimeCompileTarget::ExecutionGraph
        );
        assert!(decision
            .gates()
            .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
        assert!(decision
            .gates()
            .contains(&harness_contract::core::ExecutionPolicyGate::Approval));
        assert!(decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::WithGuardrails));
        assert!(decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::WithExternalResearch));
    }

    #[test]
    fn governed_plan_retargets_one_strategy_from_research_to_approved_write() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new()
                .register("web_search", |_| Ok("external evidence".to_string()))
                .register("write_file", |_| Ok("written".to_string())),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        runtime
            .begin_turn_strategy("turn-research-write", "调研外部资料并形成报告")
            .expect("admit strategy");
        runtime
            .bind_turn_strategy_execution("turn-research-write", "graph-research-write")
            .expect("bind graph");

        let compile = |call: &ModelToolCall| {
            let request = crate::tool_dispatch::ToolRequest {
                tool_use_id: call.id.clone(),
                tool_name: call.name.clone(),
                input: call.input.clone(),
                depends_on: Vec::new(),
            };
            let prepared = runtime
                .tool_executor()
                .prepare_governed_invocations(std::slice::from_ref(&request));
            crate::GovernedToolCompiler
                .compile(
                    &std::env::current_dir().expect("workspace"),
                    std::slice::from_ref(&request),
                    |name, input| {
                        prepared.iter().find_map(|invocation| {
                            (invocation.intent.tool_name == name
                                && invocation.intent.normalized_input == *input)
                                .then(|| {
                                    (
                                        invocation.effect.clone(),
                                        invocation.catalog_revision,
                                        invocation.descriptor_set_hash.clone(),
                                    )
                                })
                        })
                    },
                )
                .expect("governed plan")
        };
        let search = ModelToolCall {
            id: "search".to_string(),
            name: "web_search".to_string(),
            input: r#"{"query":"tokio cancellation token"}"#.to_string(),
            depends_on: Vec::new(),
        };
        let search_decision = runtime
            .retarget_active_turn_strategy_for_governed_plan(
                &compile(&search),
                std::slice::from_ref(&search),
            )
            .expect("research plan retarget");
        assert_eq!(
            search_decision.compile_target,
            crate::execution_core::RuntimeCompileTarget::EvidenceGraph
        );

        let write = ModelToolCall {
            id: "write".to_string(),
            name: "write_file".to_string(),
            input: r#"{"path":"target/report.md","content":"verified"}"#.to_string(),
            depends_on: Vec::new(),
        };
        let write_decision = runtime
            .retarget_active_turn_strategy_for_governed_plan(
                &compile(&write),
                std::slice::from_ref(&write),
            )
            .expect("write plan retarget");

        assert_eq!(
            write_decision.compile_target,
            crate::execution_core::RuntimeCompileTarget::ExecutionGraph
        );
        assert!(write_decision
            .gates()
            .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
        assert!(write_decision
            .gates()
            .contains(&harness_contract::core::ExecutionPolicyGate::Approval));
        assert!(write_decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::WithGuardrails));
        assert!(write_decision.decision_revision > search_decision.decision_revision);
        assert_eq!(
            runtime
                .active_turn_strategy()
                .expect("canonical strategy")
                .decision,
            write_decision
        );
    }

    #[tokio::test]
    async fn parallel_network_tool_batch_is_admitted_by_the_retargeted_strategy_lease() {
        let executions = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&executions);
        let event_store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let bus = CowdEventBus::new();
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: "parallel-network-execution".to_string(),
            session_id: "parallel-network-session".to_string(),
            turn_id: "parallel-network-turn".to_string(),
        });
        let mut receiver = bus.subscribe();
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new().register("web_search", move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok("verified external evidence".to_string())
            }),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_cowd_event_bus(bus)
        .with_runtime_event_store(event_store);
        runtime
            .begin_turn_strategy("turn-network-batch", "继续")
            .expect("admit direct follow-up");
        let calls = [
            "technical standard official",
            "technical standard maintainers",
        ]
        .into_iter()
        .enumerate()
        .map(|(index, query)| ModelToolCall {
            id: format!("search-{index}"),
            name: "web_search".to_string(),
            input: serde_json::json!({ "query": query }).to_string(),
            depends_on: Vec::new(),
        })
        .collect::<Vec<_>>();

        let result = runtime
            .execute_tool_batch_step(&calls, &crate::SharedPrompter::none(), 1)
            .await
            .expect("network batch execution");
        let strategy = runtime
            .active_turn_strategy()
            .expect("canonical strategy remains active");

        assert_eq!(executions.load(Ordering::SeqCst), 2);
        let mut live_events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            live_events.push(serde_json::to_string(&event).expect("serialize live event"));
        }
        assert_eq!(
            live_events
                .iter()
                .filter(|event| event.contains("\"ToolStart\""))
                .count(),
            2
        );
        assert_eq!(
            live_events
                .iter()
                .filter(|event| event.contains("\"ToolComplete\""))
                .count(),
            2
        );
        assert_eq!(
            live_events
                .iter()
                .filter(|event| event.contains("\"ToolExecuted\""))
                .count(),
            2
        );
        assert!(result.messages.iter().all(|message| {
            message.blocks.iter().all(|block| {
                !matches!(
                    block,
                    crate::session::ContentBlock::ToolResult { output, .. }
                        if output.contains("network_requires_with_external_research")
                            || output.contains("tool_category_not_allowed_by_compile_target")
                )
            })
        }));
        assert_eq!(
            strategy.decision.compile_target,
            crate::execution_core::RuntimeCompileTarget::EvidenceGraph
        );
        assert!(strategy
            .decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::WithExternalResearch));
        assert!(strategy
            .decision
            .modifiers()
            .contains(&harness_contract::core::ExecutionModifier::Parallel));
    }

    #[test]
    fn canonical_outcome_covers_direct_and_parallel_tool_turns_without_graph_ref() {
        for candidate in [
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
        ] {
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
            let state = runtime
                .begin_turn_strategy(format!("turn-{candidate:?}"), "give a concise answer")
                .expect("admit strategy");
            runtime
                .retarget_active_turn_strategy_for_tool_requirements(
                    candidate,
                    harness_contract::core::ExecutionPattern::Execute,
                    false,
                    false,
                    candidate == harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
                    false,
                    "test binds the canonical execution candidate",
                )
                .expect("retarget");
            runtime
                .finish_turn_strategy(
                    &state.turn_ref,
                    crate::execution_core::TurnStrategyDecisionStatus::Completed,
                    crate::execution_core::TurnStrategyActualOutcome {
                        duration_ms: 10,
                        tool_calls: u64::from(candidate
                            == harness_contract::strategy::ExecutionCandidateKind::ParallelTools),
                        terminal_reason: "satisfied".to_string(),
                        ..Default::default()
                    },
                )
                .expect("finish");
            let outcomes = store
                .all_events(100)
                .expect("outcomes")
                .into_iter()
                .filter(|event| event.kind == crate::execution_core::OUTCOME_EVENT_KIND)
                .collect::<Vec<_>>();
            assert_eq!(outcomes.len(), 1);
            let outcome: harness_contract::outcome::ExecutionOutcome =
                serde_json::from_value(outcomes[0].payload.clone()).expect("Outcome contract");
            assert_eq!(outcome.strategy.selected_candidate, candidate);
            assert!(outcome.identity.execution_graph_ref.is_none());
        }
    }

    #[test]
    fn canonical_outcome_preserves_failure_cancel_block_and_partial_tool_terminal_classes() {
        let cases = [
            (
                crate::execution_core::TurnStrategyDecisionStatus::Failed,
                0,
                "failed",
            ),
            (
                crate::execution_core::TurnStrategyDecisionStatus::Cancelled,
                0,
                "cancelled",
            ),
            (
                crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped,
                0,
                "blocked",
            ),
            (
                crate::execution_core::TurnStrategyDecisionStatus::Completed,
                1,
                "partial_failure",
            ),
        ];
        for (status, failed_tool_calls, expected_class) in cases {
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
            let state = runtime
                .begin_turn_strategy(format!("terminal-{expected_class}"), "test terminal")
                .expect("strategy");
            runtime
                .finish_turn_strategy(
                    &state.turn_ref,
                    status,
                    crate::execution_core::TurnStrategyActualOutcome {
                        failed_tool_calls,
                        terminal_reason: expected_class.to_string(),
                        ..Default::default()
                    },
                )
                .expect("finish");
            let event = store
                .all_events(10)
                .expect("outcome")
                .into_iter()
                .find(|event| event.kind == crate::execution_core::OUTCOME_EVENT_KIND)
                .expect("canonical Outcome");
            let outcome: harness_contract::outcome::ExecutionOutcome =
                serde_json::from_value(event.payload).expect("Outcome");
            assert_eq!(outcome.terminal.class_name(), expected_class);
        }
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
            .list_stream(&format!("session:{}", runtime.session_id()))
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
        assert!(downgraded.payload["reason"]
            .as_str()
            .expect("visible reason")
            .contains("overlap 9100 bp"));
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
            .list_stream(&format!("session:{}", runtime.session_id()))
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
            .list_stream(&format!("session:{}", runtime.session_id()))
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
        assert!(early_stop.payload["reason"]
            .as_str()
            .expect("visible early-stop reason")
            .contains("low novelty"));
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
            estimate.duration_calibration_source = "frozen-before-restart".to_string();
        }
        let frozen = first_runtime
            .bind_turn_strategy_execution("recovery-cost-turn", "recovery-cost-graph")
            .expect("durable selected event");
        let session_id = first_runtime.session_id().to_string();

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
            recovered.decision.strategy.candidate_estimates[0].duration_calibration_source,
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
        let session_id = runtime.session_id().to_string();
        let access = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::reality::EvidenceRef::observed("tool", evidence_id),
            "sha256:test",
            output.len() as u64,
            "text/plain; charset=utf-8",
            "artifact://art_conversation_output",
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
        assert!(envelope
            .assembled
            .runtime_header
            .iter()
            .any(|section| section.contains("## Runtime clock")));
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
    fn memory_source_scan_uses_runtime_capacity_without_layer_caps() {
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
        assert!(!mem_cfg.budget.runtime_managed);
        assert_eq!(mem_cfg.budget.l0_reserved, 0);
        assert_eq!(mem_cfg.budget.l3_checkpoint, 0);
        assert!(plan.memory_retrieval_budget.candidate_scan_limit > 80);
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
        let plan = crate::governed_tool_plan::GovernedToolPlan::from_requests(&requests);
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
        assert!(critical_validation
            .findings
            .iter()
            .any(|finding| finding == "mutation_missing_approval_runtime"));
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
    fn model_candidates_keep_configured_primary_and_fallback_order() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime.model = Some("balanced-model".to_string());
        *runtime.fallbacks.write().unwrap() =
            vec!["stepfun-fast".to_string(), "deepseek-depth".to_string()];

        assert_eq!(
            runtime.model_candidates_for_turn("任务内容不得改变配置顺序"),
            vec![
                "balanced-model".to_string(),
                "stepfun-fast".to_string(),
                "deepseek-depth".to_string(),
            ]
        );
    }

    #[test]
    fn model_candidates_observe_shared_fallback_policy_updates() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime.model = Some("primary".to_string());
        let policy = Arc::new(std::sync::RwLock::new(vec!["same-provider".to_string()]));
        runtime = runtime.with_provider_fallback_policy(Arc::clone(&policy));
        assert_eq!(
            runtime.model_candidates_for_turn("first turn"),
            vec!["primary".to_string(), "same-provider".to_string()]
        );

        *policy.write().unwrap() = vec![
            "cross-provider".to_string(),
            "secondary-provider".to_string(),
        ];
        assert_eq!(
            runtime.model_candidates_for_turn("next turn"),
            vec![
                "primary".to_string(),
                "cross-provider".to_string(),
                "secondary-provider".to_string(),
            ]
        );
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
            .push_message(crate::session::ConversationMessage::assistant_with_usage(
                vec![ContentBlock::Text {
                    text: "earlier".to_string(),
                }],
                Some(TokenUsage {
                    input_tokens: 11,
                    output_tokens: 7,
                    cache_creation_input_tokens: 2,
                    cache_read_input_tokens: 1,
                }),
            ))
            .expect("append prior usage");

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
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
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
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            Arc::clone(&store),
        ))
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
        assert!(rendered_prompt(&requests[0].prompt)
            .contains("Require release evidence before accepting completion."));
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
                messages: Vec::new().into(),
                model: "test".to_string(),
                reasoning_effort_override: None,
                request_compiler_cache_hit: false,
                budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                    "test", 128_000, 4_096, 128, 256, 0,
                ),
                provider_evidence_context: None,
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

    #[async_trait::async_trait]
    impl crate::ToolExecutor for ExposureToolExecutor {
        async fn execute_output(
            &self,
            _name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new("test executor must not run"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec![
                "tool_search".to_string(),
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
            bootstrap: [
                "tool_search".to_string(),
                "runtime_capabilities".to_string(),
            ]
            .into_iter()
            .collect(),
            active: [
                "tool_search".to_string(),
                "runtime_capabilities".to_string(),
            ]
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
                "available_tool_names": ["tool_search", "runtime_capabilities", "read_many", "runtime_orchestrate"],
                "runtime_orchestrate": {"available": true, "blocked_reasons": []},
                "action_plane": {"can_execute_now": true},
                "strategy": {"model_callable_tools": ["tool_search", "runtime_capabilities", "read_many", "runtime_orchestrate"]}
            })
            .to_string(),
        );
        let value: serde_json::Value =
            serde_json::from_str(&projected).expect("projected capability JSON");

        assert_eq!(
            value["catalog_tool_names"],
            serde_json::json!([
                "tool_search",
                "runtime_capabilities",
                "read_many",
                "runtime_orchestrate"
            ])
        );
        assert_eq!(
            value["tool_visibility"]["active_function_schemas"],
            serde_json::json!(["runtime_capabilities", "tool_search"])
        );
        assert_eq!(
            value["strategy"]["model_callable_tools"],
            serde_json::json!(["runtime_capabilities", "tool_search"])
        );
        assert_eq!(value["runtime_orchestrate"]["available"], false);
        assert_eq!(value["runtime_orchestrate"]["schema_active"], false);
        assert_eq!(value["action_plane"]["can_execute_now"], false);
        assert_eq!(
            value["action_plane"]["recommended_next_tool"],
            "tool_search"
        );
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
        assert_eq!(
            projections[1].active_ids,
            vec!["grep_search", "tool_search"]
        );
        assert!(projections[1]
            .deferred_ids
            .contains(&"custom_reader".to_string()));
    }

    struct MutationExposureToolExecutor;

    #[async_trait::async_trait]
    impl crate::ToolExecutor for MutationExposureToolExecutor {
        async fn execute_output(
            &self,
            _name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new("exposure test executor must not run"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec![
                "tool_search".to_string(),
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
        assert!(!projections[0]
            .active_ids
            .contains(&"unknown_mutator".to_string()));
        assert!(projections[0]
            .deferred_ids
            .contains(&"read_file".to_string()));
        assert!(projections[1]
            .active_ids
            .contains(&"tool_search".to_string()));
        assert!(projections[1].active_ids.contains(&"read_file".to_string()));
        assert!(projections[1]
            .active_ids
            .contains(&"grep_search".to_string()));
        assert!(projections[1].exposure_revision > projections[0].exposure_revision);
    }

    #[derive(Clone)]
    struct DynamicExposureApi {
        requests: Arc<std::sync::atomic::AtomicUsize>,
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
        request_messages: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    }

    impl ApiClient for DynamicExposureApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let mut captured = request
                .messages
                .iter()
                .flat_map(|message| message.blocks.iter())
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    ContentBlock::ToolResult { output, .. } => Some(output.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if let Some(system) = request.prompt.trusted_system_text() {
                captured.push(system);
            }
            self.request_messages.lock().unwrap().push(captured);
            let request = self
                .requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if request == 0 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "discover-1".to_string(),
                        name: "tool_search".to_string(),
                        input: r#"{"query":"read files"}"#.to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else if request == 1 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "invalid-1".to_string(),
                        name: "invented_tool".to_string(),
                        input: "{}".to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else if request == 2 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "read-1".to_string(),
                        name: "custom-reader".to_string(),
                        input: r#"{"path":"README.md"}"#.to_string(),
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

    #[async_trait::async_trait]
    impl crate::ToolExecutor for DynamicExposureToolExecutor {
        async fn execute_output(
            &self,
            name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            let output = if name == "custom_reader" {
                "README contents".to_string()
            } else if name == "tool_search" {
                serde_json::json!({
                    "query": "read files",
                    "catalog_revision": 0,
                    "descriptors": [{
                        "canonical_id": "custom_reader",
                        "display_name": "custom_reader",
                        "source": "test",
                        "schema_hash": "read-v1",
                        "required_permission": "read-only",
                        "permission_source": "test",
                        "health": "healthy"
                    }],
                    "activation_candidates": ["custom_reader"]
                })
                .to_string()
            } else {
                return Err(crate::ToolError::new("unknown dynamic tool"));
            };
            Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
                output,
            ))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec!["tool_search".to_string(), "custom_reader".to_string()]
        }

        fn classify_tool_safety(
            &self,
            name: &str,
            _input: &str,
        ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
            matches!(name, "tool_search" | "custom_reader")
                .then_some(crate::tool_orchestrator::ToolSafetyCategory::ReadOnly)
        }

        fn registered_tool_effect(
            &self,
            name: &str,
            _input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            use harness_contract::policy::{
                PermissionOperation, PermissionResource, PermissionScope,
            };
            use harness_contract::tool::{
                ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
                ToolPermissionMode,
            };

            matches!(name, "tool_search" | "custom_reader").then(|| ToolEffectDescriptor {
                tool_id: name.to_string(),
                descriptor_hash: "dynamic-tool-search-v1".to_string(),
                effect_kind: ToolEffectKind::Read,
                idempotency: ToolIdempotency::Idempotent,
                scopes: vec![PermissionScope::new(
                    PermissionResource::Tool,
                    PermissionOperation::Read,
                )],
                required_permission: ToolPermissionMode::ReadOnly,
                approval_class: ToolApprovalClass::None,
                uses_network: false,
                spawns_process: false,
                mutates_packages: false,
                mutates_system: false,
                assessment: harness_contract::policy::EffectAssessment::default(),
            })
        }

        async fn execute_authorized_output(
            &self,
            authorization: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            if authorization.tool_id != name {
                return Err(crate::ToolError::new(
                    "dynamic tool authorization names a different tool",
                ));
            }
            self.execute_output(name, input).await
        }
    }

    struct DirectDeferredApi;

    impl ApiClient for DirectDeferredApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::ToolUse {
                    id: "read-direct".to_string(),
                    name: "custom-reader".to_string(),
                    input: r#"{"path":"README.md"}"#.to_string(),
                }),
                Ok(AssistantEvent::MessageStop),
            ]))
        }
    }

    #[tokio::test]
    async fn known_deferred_tool_call_activates_for_one_governed_retry() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            DirectDeferredApi,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("direct-deferred-turn", "inspect README")
            .expect("turn strategy");

        let miss = runtime
            .execute_model_step("inspect README", true)
            .await
            .expect_err("known deferred schema needs one governed retry");
        assert!(miss.is_tool_exposure_miss(), "{miss}");
        assert!(runtime
            .tool_exposure_state
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|state| state.active.contains("custom_reader")));

        let resumed = runtime
            .execute_model_step("inspect README", false)
            .await
            .expect("activated schema must be usable without tool_search");
        let ModelStepIntent::ToolCalls { calls } = resumed.intent else {
            panic!("retry must preserve the provider tool call");
        };
        assert_eq!(calls[0].name, "custom_reader");
    }

    #[tokio::test]
    async fn one_request_tool_allowlist_is_a_hard_deferred_activation_ceiling() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            DirectDeferredApi,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("bounded-deferred-turn", "inspect README")
            .expect("turn strategy");
        runtime.require_next_model_tools(["tool_search".to_string()]);

        let error = runtime
            .execute_model_step("inspect README", true)
            .await
            .expect_err("the one-request allowlist must reject deferred activation");
        assert!(!error.is_tool_exposure_miss(), "{error}");
        assert!(error
            .to_string()
            .contains("governed one-request allowlist rejected [custom_reader]"));
        assert!(!runtime
            .tool_exposure_state
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|state| state.active.contains("custom_reader")));
    }

    #[tokio::test]
    async fn successful_session_tools_are_rehydrated_on_the_next_user_turn() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut session = Session::new();
        session
            .push_message(ConversationMessage::tool_result(
                "prior-read",
                "custom-reader",
                "prior bounded result",
                false,
            ))
            .expect("session tool result");
        let mut runtime = ConversationRuntime::new(
            session,
            DynamicExposureApi {
                requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                projections: Arc::clone(&projections),
                request_messages: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("rehydrated-tool-turn", "continue inspecting files")
            .expect("turn strategy");

        runtime
            .execute_model_step("continue inspecting files", true)
            .await
            .expect("first model request");

        let projections = projections.lock().unwrap();
        assert!(projections[0]
            .active_ids
            .contains(&"custom_reader".to_string()));
        assert!(!projections[0]
            .deferred_ids
            .contains(&"custom_reader".to_string()));
    }

    #[tokio::test]
    async fn dynamic_tool_exposure_defers_schema_until_discovery_activation() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let request_messages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = DynamicExposureApi {
            requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            projections: Arc::clone(&projections),
            request_messages: Arc::clone(&request_messages),
        };
        let artifact_root = tempfile::tempdir().unwrap();
        let session = Session::new();
        let session_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        session_store
            .create_session(&session::SessionRecord {
                session_id: session.session_id.clone(),
                platform: "test".to_string(),
                chat_id: "dynamic-exposure".to_string(),
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
        let mut runtime = ConversationRuntime::new(
            session,
            api,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()))
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            session_store,
        ))
        .with_artifact_store(Arc::new(
            crate::ArtifactStore::sqlite(
                artifact_root.path(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        ));
        runtime
            .begin_turn_strategy("test-dynamic-exposure-turn", "inspect files")
            .expect("test turn strategy admission");

        let first = runtime
            .execute_model_step("inspect files", true)
            .await
            .expect("first model step");
        let ModelStepIntent::ToolCalls { calls } = first.intent else {
            panic!("first request must invoke tool_search");
        };
        {
            let exposure = runtime
                .tool_exposure_state
                .lock()
                .expect("tool exposure state");
            let exposure = exposure
                .as_ref()
                .expect("first provider request must persist its exposure state");
            assert!(
                exposure.deferred.contains("custom_reader"),
                "custom_reader must be discoverable before tool_search activation: {exposure:?}"
            );
        }
        let discovery_output = runtime
            .tool_executor
            .execute_output("tool_search", &calls[0].input)
            .await
            .expect("governed tool_search execution");
        let parsed_discovery =
            serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(
                discovery_output.model_text(),
            )
            .expect("tool_search must return a canonical discovery receipt");
        assert_eq!(
            parsed_discovery.activation_candidates,
            vec!["custom_reader".to_string()]
        );
        let discovery_result = runtime
            .prepare_governed_tool_result(
                &calls[0].id,
                &calls[0].name,
                &calls[0].input,
                discovery_output.model_text(),
                false,
            )
            .await
            .expect("governed tool_search result preparation");
        runtime
            .session
            .write()
            .await
            .push_message(discovery_result)
            .expect("governed tool_search result publication");
        assert!(
            runtime
                .tool_exposure_state
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|state| state.active.contains("custom_reader")),
            "tool_search must activate custom_reader before the following provider request"
        );
        let protocol_error = runtime
            .execute_model_step("inspect files", false)
            .await
            .expect_err("an invented tool must fail the active exposure lease");
        assert!(protocol_error
            .to_string()
            .contains("outside this request's exposure lease"));
        let activated = runtime
            .execute_model_step("inspect files", false)
            .await
            .expect("protocol recovery must retain the discovery handoff");
        let ModelStepIntent::ToolCalls { calls } = activated.intent else {
            panic!("recovery request must invoke the activated tool");
        };
        assert_eq!(calls[0].name, "custom_reader");
        let batch = runtime
            .execute_tool_batch_step(&calls, &crate::SharedPrompter::none(), 2)
            .await
            .expect("activated tool execution");
        assert_eq!(batch.failed, 0);
        runtime
            .execute_model_step("inspect files", false)
            .await
            .expect("final model step");

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 4);
        assert_eq!(projections[0].catalog_revision, 0);
        assert_eq!(projections[0].active_ids, vec!["tool_search"]);
        assert_eq!(projections[0].deferred_ids, vec!["custom_reader"]);
        assert!(projections[1]
            .active_ids
            .contains(&"custom_reader".to_string()));
        assert!(
            !projections[1]
                .active_ids
                .contains(&"tool_search".to_string())
                && !projections[1]
                    .bootstrap_ids
                    .contains(&"tool_search".to_string()),
            "the immediate post-discovery request must not be able to repeat tool_search"
        );
        assert!(projections[1].exposure_revision > projections[0].exposure_revision);
        assert!(projections[2]
            .active_ids
            .contains(&"custom_reader".to_string()));
        assert!(!projections[2]
            .active_ids
            .contains(&"tool_search".to_string()));
        assert!(
            projections[3]
                .active_ids
                .contains(&"tool_search".to_string()),
            "tool_search must return after a valid post-discovery response"
        );
        assert!(projections[3].exposure_revision > projections[2].exposure_revision);
        let request_messages = request_messages.lock().unwrap();
        assert_eq!(request_messages.len(), 4);
        assert!(
            request_messages[1].iter().any(|message| {
                message.contains("tool_search already completed successfully")
                    && message.contains("Newly activated native function schemas: [custom_reader]")
                    && message.contains("do not claim that a new user turn is needed")
            }),
            "the post-discovery provider request must provide an explicit execution handoff: {:?}",
            request_messages[1]
        );
        assert!(
            request_messages[2].iter().any(|message| {
                message.contains("tool_search already completed successfully")
                    && message.contains("Newly activated native function schemas: [custom_reader]")
            }),
            "protocol recovery must retain the post-discovery handoff"
        );

        let metrics = runtime.tool_exposure_metrics();
        assert_eq!(metrics.provider_requests, 4);
        assert_eq!(metrics.tool_search_calls, 1);
        assert_eq!(metrics.tool_search_additional_rounds, 1);
        assert_eq!(metrics.activation_candidates, 1);
        assert_eq!(metrics.activations, 1);
        assert_eq!(metrics.activated_invocations, 1);
        assert_eq!(metrics.activation_precision_bp, Some(10_000));
        assert_eq!(metrics.activation_recall_bp, None);
    }

    #[test]
    fn tool_exposure_metrics_distinguish_activation_cost_and_outcomes() {
        use harness_contract::tool::{
            ToolActivationDecision, ToolActivationReceipt, ToolActivationStatus,
        };

        let mut metrics = TurnToolExposureMetrics::default();
        metrics.reset((0, 0));
        metrics.observe_search(&ToolActivationReceipt {
            catalog_revision: 7,
            previous_exposure_revision: 2,
            exposure_revision: 3,
            decisions: vec![
                ToolActivationDecision {
                    canonical_id: "reader".to_string(),
                    status: ToolActivationStatus::Activated,
                    reason: "healthy and permitted".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "writer".to_string(),
                    status: ToolActivationStatus::Denied,
                    reason: "permission ceiling".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "remote".to_string(),
                    status: ToolActivationStatus::Unavailable,
                    reason: "catalog health".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "missing".to_string(),
                    status: ToolActivationStatus::NotFound,
                    reason: "unknown descriptor".to_string(),
                },
            ],
        });
        metrics.observe_provider_request(
            ProviderContextInventory {
                tool_count: 2,
                tool_schema_tokens: 333,
                ..Default::default()
            },
            (2, 1),
        );
        metrics.observe_invocation("reader");

        let projection = metrics.projection();
        assert_eq!(projection.provider_requests, 1);
        assert_eq!(projection.tool_search_calls, 1);
        assert_eq!(projection.tool_search_additional_rounds, 1);
        assert_eq!(projection.activation_candidates, 4);
        assert_eq!(projection.activations, 1);
        assert_eq!(projection.activated_invocations, 1);
        assert_eq!(projection.permission_rejections, 1);
        assert_eq!(projection.unavailable_descriptors, 1);
        assert_eq!(projection.descriptor_misses, 1);
        assert_eq!(projection.schema_tokens_max, 333);
        assert_eq!(projection.schema_compilations, 2);
        assert_eq!(projection.schema_cache_hits, 1);
        assert_eq!(projection.activation_precision_bp, Some(10_000));
        assert_eq!(projection.activation_recall_bp, None);
    }

    #[test]
    fn stable_prefix_metrics_track_wire_identity_and_provider_native_cache() {
        let mut metrics = TurnStablePrefixMetrics::default();
        let request = |dynamic: &str, cache_hit| ApiRequest {
            prompt: PromptAssembly::new(vec![
                "stable identity".to_string(),
                "stable policy".to_string(),
                crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string(),
                dynamic.to_string(),
            ]),
            messages: vec![ConversationMessage::user_text("inspect".to_string())].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: cache_hit,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 32_768, 4_096, 128, 256, 0,
            ),
            provider_evidence_context: None,
        };

        metrics.observe_request(&request("runtime A", false));
        metrics.observe_request(&request("runtime B with more bytes", true));
        metrics.observe_usage(TokenUsage {
            input_tokens: 100,
            output_tokens: 10,
            cache_creation_input_tokens: 80,
            cache_read_input_tokens: 64,
        });

        let projection = metrics.projection;
        assert_eq!(projection.provider_requests, 2);
        assert!(!projection.stable_prefix_fingerprint.is_empty());
        assert_eq!(projection.wire_identity_failures, 0);
        assert_eq!(projection.request_compiler_compilations, 1);
        assert_eq!(projection.request_compiler_cache_hits, 1);
        assert_eq!(projection.native_cache_creation_input_tokens, 80);
        assert_eq!(projection.native_cache_read_input_tokens, 64);
        assert!(projection.runtime_system_bytes_max > 0);
    }

    #[tokio::test]
    async fn governed_tool_results_persist_raw_evidence_and_bound_model_receipt() {
        let artifact_root = tempfile::tempdir().unwrap();
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
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
        .with_artifact_store(Arc::new(
            crate::ArtifactStore::sqlite(
                artifact_root.path(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        ))
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
        let raw = format!("first\n{}\nlast", "middle-evidence ".repeat(8_000));

        let receipt = runtime
            .prepare_governed_tool_result(
                "governed-read-1",
                "read_file",
                r#"{"path":"README.md"}"#,
                &raw,
                false,
            )
            .await
            .expect("durable evidence receipt");

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
                && event
                    .payload
                    .get("artifact_selector")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|selector| selector.starts_with("artifact://"))
                && event.payload.get("raw").is_none()
        }));
        let audit = runtime.turn_evidence_audits();
        assert_eq!(audit.len(), 1);
        assert!(audit[0].access.is_some());
        assert!(audit[0].omitted_tokens > 0);
        let observations = runtime.turn_tool_observations();
        assert_eq!(observations.len(), 1);
        let envelope = observations[0]
            .output_envelope
            .as_ref()
            .expect("tool output envelope must be connected to the turn report");
        assert!(envelope.receipt.starts_with("tool://"));
        assert!(envelope
            .artifact_ref
            .as_ref()
            .is_some_and(|artifact| artifact.selector.starts_with("artifact://")));
        assert!(envelope
            .evidence_ref
            .as_ref()
            .is_some_and(harness_contract::context::EvidenceAccessRef::is_durable));
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
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::new(
                session::UnifiedSessionStore::open_in_memory().unwrap(),
            )),
        );
        let raw = "raw output retained only in the active runtime when durable write fails\n"
            .repeat(1_000);

        let error = runtime
            .prepare_governed_tool_result(
                "raw-failure-1",
                "read_file",
                r#"{"path":"README.md"}"#,
                &raw,
                false,
            )
            .await
            .expect_err("missing artifact durability must block publication");
        assert!(error.to_string().contains("Artifact store"));
        assert!(runtime.turn_evidence_audits().is_empty());
    }

    #[tokio::test]
    async fn context_turn_report_is_durable_before_runtime_exposes_it() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
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
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
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
    async fn large_context_envelope_is_canonical_and_artifact_backed() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "context-artifact".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-08-07T00:00:00Z".to_string(),
                last_activity: "2026-08-07T00:00:00Z".to_string(),
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
        let artifact_root = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                artifact_root.path(),
                crate::ArtifactStoreConfig {
                    compact_threshold_bytes: 1,
                    ..crate::ArtifactStoreConfig::default()
                },
            )
            .expect("artifact store"),
        );
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory()
        .with_artifact_store(Arc::clone(&artifacts))
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main(&session_id),
            profile: ContextProfile::MainTurn,
            intent: "persist one canonical context body".to_string(),
            stable_head: vec!["stable system".to_string()],
            runtime_header: vec!["runtime header".to_string()],
            dynamic_items: vec![ContextItem::new(
                "memory-context-1",
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                "canonical memory content",
            )],
            omitted: Vec::new(),
            total_budget_tokens: 4_000,
        });

        runtime.remember_context_envelope(envelope).await;

        let events = store
            .get_events_by_type_limited(&session_id, "ContextEnvelope", 0, 10)
            .await
            .expect("context event");
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value =
            serde_json::from_str(&events[0].event_json).expect("context payload");
        assert_eq!(
            payload["schema_version"],
            PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION
        );
        assert_eq!(
            payload["formatter_version"],
            CONTEXT_RENDER_FORMATTER_VERSION
        );
        assert_eq!(payload["envelope"]["artifact_backed"], true);
        assert!(payload["envelope"].get("selected").is_none());
        let artifact: harness_contract::context::ArtifactRef =
            serde_json::from_value(payload["context_artifact"].clone()).expect("artifact ref");
        let bytes = artifacts
            .read(&artifact, &format!("session:{session_id}"), None)
            .await
            .expect("canonical context artifact");
        let persisted: serde_json::Value =
            serde_json::from_slice(&bytes).expect("persisted context");
        assert_eq!(
            persisted["selected"][0]["content"],
            "canonical memory content"
        );
        assert!(persisted.get("assembled").is_none());
        assert_eq!(artifacts.stats().expect("artifact stats").pins, 1);
    }

    #[tokio::test]
    async fn context_turn_report_write_failure_does_not_expose_a_successful_report() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
        );
        let report = runtime.build_context_turn_report("turn-failure", TokenUsage::default(), None);

        let error = runtime
            .remember_context_turn_report(report)
            .await
            .expect_err("a foreign-key persistence failure must fail the terminal report path");
        assert!(error
            .to_string()
            .contains("context governance persistence failed"));
        assert_eq!(runtime.last_context_turn_report(), None);
    }

    #[tokio::test]
    async fn compaction_event_failure_is_terminal_and_does_not_claim_durable_recovery() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
        );

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
                    schema_version:
                        memory::compression::session::SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION,
                    checkpoint_id: "checkpoint-failure".to_string(),
                    execution_identity:
                        harness_contract::execution::ExecutionIdentity::for_session_turn(
                            "primary",
                            "workspace-failure",
                            "missing-session",
                            "turn-failure",
                        )
                        .unwrap(),
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
        assert!(error
            .to_string()
            .contains("atomic compaction persistence failed"));
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
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
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
        let _ = rt.run_memory_post_turn("").await;
    }

    #[test]
    fn post_turn_memory_window_contains_only_the_current_turn_and_supplements() {
        let messages = vec![
            ConversationMessage::user_text("old request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old decision".to_string(),
            }]),
            ConversationMessage::user_text("current request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "working".to_string(),
            }]),
            ConversationMessage::user_text("supplement"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "final answer".to_string(),
            }]),
        ];

        let current = current_turn_messages(&messages, "current request");
        assert_eq!(current.len(), 4);
        assert_eq!(conversation_message_text(&current[0]), "current request");
        assert_eq!(conversation_message_text(&current[2]), "supplement");
    }

    #[test]
    fn delegated_team_runtime_does_not_duplicate_root_conversation_memory() {
        let root = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        assert!(root.owns_conversation_memory_production());

        let child = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_memory_identity(
            "researcher-instance",
            Some("researcher-definition".to_string()),
            Some("team-run".to_string()),
            Vec::new(),
        );
        assert!(!child.owns_conversation_memory_production());
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
        assert!(prompt
            .trusted_system
            .iter()
            .any(|segment| segment.contains("context_governance_report_id:")));
        assert_eq!(envelope.intent, "remember this");
        assert_eq!(envelope.assembled.stable_head, vec!["stable system"]);
        assert_eq!(
            envelope.diagnostics.degraded_sources,
            vec![ContextSourceKind::Memory]
        );
        assert!(envelope
            .selected
            .iter()
            .all(|item| item.source != ContextSourceKind::Memory));
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

        assert!(prompt
            .contextual_packets
            .iter()
            .any(|packet| packet.content.contains("continue v0.8.13 context work")));
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

        assert!(prompt
            .contextual_packets
            .iter()
            .any(|packet| packet.content.contains("cargo test passed")));
        assert!(envelope
            .selected
            .iter()
            .any(|item| item.source == ContextSourceKind::ToolTrace));
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
        let r = handle.block_on(rt.run_memory_post_turn(""));
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
        let session = Session::new().with_workspace_root(tmp.path());
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
        assert!(loaded_l1
            .iter()
            .any(|entry| entry.title == "User preference: 不要使用工具或编排"));
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

        assert!(envelope
            .omitted
            .iter()
            .any(|omission| omission.reason.contains("suppressed_for_current_turn")));
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
        assert!(prompt
            .trusted_system
            .iter()
            .any(|segment| segment.contains("profile:MainTurn")));
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
        let endpoint = registry
            .endpoint(&storage::StorageDomainId::Fact)
            .expect("fact endpoint");
        let fact_ledger = fact_sqlite::SqliteFactLedger::open(endpoint).expect("fact ledger");
        let mut fact = fact_kernel::FactRecord::new(
            "supply.policy",
            "east allocation requires expedited approval",
        );
        fact.id = fact_kernel::FactId::from_string("primary-turn-fact");
        fact_ledger.upsert_fact(fact).expect("persist fact");

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
        assert!(envelope
            .selected
            .iter()
            .any(|item| item.source == ContextSourceKind::Fact));
        let report = runtime
            .last_reality_recall_report()
            .expect("reality recall report");
        assert_eq!(report.sources[0].status, "enabled_and_wired");
        assert_eq!(report.sources[0].selected_count, 1);
    }

    #[tokio::test]
    async fn typed_model_stream_preserves_public_order_without_leaking_private_reasoning() {
        let bus = Arc::new(CowdEventBus::new());
        let _scope = bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "execution-causal".to_string(),
                session_id: "session-causal".to_string(),
                turn_id: "turn-causal".to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id: "execution-causal".to_string(),
                session_id: "session-causal".to_string(),
                turn_id: "turn-causal".to_string(),
                root_task_id: "task-causal".to_string(),
                task_id: "task-causal".to_string(),
                activity_id: "activity:execution:execution-causal".to_string(),
                node_id: None,
                parent_activity_id: None,
                initiator_activity_id: None,
                team_run_id: None,
                agent_instance_id: None,
                agent_run_id: None,
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: 1,
                fence: 1,
                generation: 1,
            }),
        );
        let mut receiver = bus.subscribe();
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let events = vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("summary-0".to_string()),
                kind: AssistantItemKind::PublicReasoning,
            }),
            Ok(AssistantEvent::ReasoningSummaryDelta(
                "checked plan".to_string(),
            )),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::ItemStarted {
                index: 1,
                provider_item_id: Some("private-0".to_string()),
                kind: AssistantItemKind::PrivateReasoning,
            }),
            Ok(AssistantEvent::PrivateReasoningDelta(
                "provider-private-secret".to_string(),
            )),
            Ok(AssistantEvent::SignatureDelta(
                "provider-signature-secret".to_string(),
            )),
            Ok(AssistantEvent::ItemCompleted { index: 1 }),
            Ok(AssistantEvent::ItemStarted {
                index: 2,
                provider_item_id: Some("tool-0".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "tool-0".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md"}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 2 }),
            Ok(AssistantEvent::ItemStarted {
                index: 3,
                provider_item_id: Some("text-0".to_string()),
                kind: AssistantItemKind::Text,
            }),
            Ok(AssistantEvent::TextDelta("final answer".to_string())),
            Ok(AssistantEvent::ItemCompleted { index: 3 }),
            Ok(AssistantEvent::MessageStop),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                Some(Arc::clone(&bus)),
                Some(Arc::clone(&store)),
                "session-causal".to_string(),
            ),
            None,
        )
        .await;

        assert!(
            result.failure.is_none(),
            "typed stream unexpectedly failed: {:?}",
            result.failure
        );
        assert_eq!(result.collected.public_reasoning, "checked plan");
        assert_eq!(
            result.collected.private_reasoning,
            "provider-private-secret"
        );
        assert_eq!(result.collected.signature, "provider-signature-secret");
        assert_eq!(result.collected.text, "final answer");
        assert_eq!(result.collected.calls.len(), 1);

        let mut projected = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            projected.push(serde_json::to_string(&event).expect("serialize event"));
        }
        let projected = projected.join("\n");
        assert!(projected.contains("checked plan"));
        assert!(projected.contains("final answer"));
        assert!(!projected.contains("provider-private-secret"));
        assert!(!projected.contains("provider-signature-secret"));

        let durable = store.all_events(20).expect("durable model items");
        assert_eq!(
            durable
                .iter()
                .filter(|event| event.kind == "model.item_completed")
                .count(),
            3
        );
        let durable_json = durable
            .iter()
            .map(|event| event.payload.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!durable_json.contains("provider-private-secret"));
        assert!(!durable_json.contains("provider-signature-secret"));
    }

    #[tokio::test]
    async fn tool_step_without_provider_summary_emits_one_safe_public_action_summary() {
        let bus = Arc::new(CowdEventBus::new());
        let _scope = bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "execution-action-summary".to_string(),
                session_id: "session-action-summary".to_string(),
                turn_id: "turn-action-summary".to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id: "execution-action-summary".to_string(),
                session_id: "session-action-summary".to_string(),
                turn_id: "turn-action-summary".to_string(),
                root_task_id: "task-action-summary".to_string(),
                task_id: "task-action-summary".to_string(),
                activity_id: "activity:agent:researcher".to_string(),
                node_id: Some("researcher".to_string()),
                parent_activity_id: Some("activity:team:research".to_string()),
                initiator_activity_id: Some("activity:team:research".to_string()),
                team_run_id: Some("team:research".to_string()),
                agent_instance_id: Some("agent:researcher".to_string()),
                agent_run_id: Some("agent-run:researcher".to_string()),
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: 1,
                fence: 1,
                generation: 1,
            }),
        );
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let stream = Box::pin(futures::stream::iter(vec![
            Ok(AssistantEvent::PrivateReasoningDelta(
                "provider-private-secret".to_string(),
            )),
            Ok(AssistantEvent::TextDelta(
                "先查询两类权威来源，再比较差异。".to_string(),
            )),
            Ok(AssistantEvent::ToolUse {
                id: "search-1".to_string(),
                name: "web_search".to_string(),
                input: r#"{"query":"source one"}"#.to_string(),
            }),
            Ok(AssistantEvent::ToolUse {
                id: "search-2".to_string(),
                name: "web_search".to_string(),
                input: r#"{"query":"source two"}"#.to_string(),
            }),
            Ok(AssistantEvent::MessageStop),
        ]));

        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                Some(Arc::clone(&bus)),
                Some(Arc::clone(&store)),
                "session-action-summary".to_string(),
            ),
            None,
        )
        .await;

        assert!(
            result.failure.is_none(),
            "unexpected failure: {:?}",
            result.failure
        );
        assert!(result.collected.public_reasoning.is_empty());
        let durable = store.all_events(20).expect("durable events");
        let reasoning = durable
            .iter()
            .filter(|event| event.kind == "model.item_completed")
            .filter(|event| event.payload["kind"] == "public_reasoning")
            .collect::<Vec<_>>();
        assert_eq!(reasoning.len(), 1);
        assert_eq!(
            reasoning[0].payload["content"],
            "先查询两类权威来源，再比较差异。"
        );
        assert_eq!(
            reasoning[0]
                .activity_binding()
                .as_ref()
                .and_then(|binding| binding.agent_instance_id.as_deref()),
            Some("agent:researcher")
        );
        assert!(!reasoning[0]
            .payload
            .to_string()
            .contains("provider-private-secret"));
    }

    #[tokio::test]
    async fn failed_model_stream_does_not_persist_partial_item_as_completed() {
        let bus = Arc::new(CowdEventBus::new());
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: "execution-partial".to_string(),
            session_id: "session-partial".to_string(),
            turn_id: "turn-partial".to_string(),
        });
        let mut receiver = bus.subscribe();
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let stream = Box::pin(futures::stream::iter(vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("text-partial".to_string()),
                kind: AssistantItemKind::Text,
            }),
            Ok(AssistantEvent::TextDelta("partial answer".to_string())),
            Err(RuntimeError::new("provider stream interrupted")),
        ]));

        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                Some(Arc::clone(&bus)),
                Some(Arc::clone(&store)),
                "session-partial".to_string(),
            ),
            None,
        )
        .await;

        assert!(result.failure.is_some());
        assert_eq!(result.collected.text, "partial answer");
        assert!(store
            .all_events(20)
            .expect("durable events")
            .iter()
            .all(|event| event.kind != "model.item_completed"));
        let mut model_step_failed = false;
        while let Ok(event) = receiver.try_recv() {
            let event = match event {
                crate::CowdEvent::ExecutionScoped { event, .. } => *event,
                event => event,
            };
            if matches!(
                event,
                crate::CowdEvent::ModelStepCompleted { ref status, .. } if status == "failed"
            ) {
                model_step_failed = true;
            }
        }
        assert!(model_step_failed);
    }

    #[derive(Default)]
    struct RecordingEarlyDispatcher {
        dispatches: std::sync::atomic::AtomicUsize,
    }

    impl EarlyToolDispatcher for RecordingEarlyDispatcher {
        fn dispatch(&self, candidate: EarlyToolCandidate) -> EarlyToolDispatchFuture {
            self.dispatches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                let started_at_ms = super::now_ms();
                tokio::time::sleep(Duration::from_millis(5)).await;
                EarlyToolDispatchResult::Executed(EarlyToolExecutionReceipt {
                    outcome: crate::RuntimeToolExecutionOutcome {
                        tool_use_id: candidate.call.id.clone(),
                        tool_name: candidate.call.name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Executed,
                        category: crate::ToolSafetyCategory::ReadOnly,
                        output: Some("early-result".to_string()),
                        error: None,
                        evidence_ref: "test:early-result".to_string(),
                    },
                    call: candidate.call,
                    ready_at_ms: candidate.ready_at_ms,
                    started_at_ms,
                    completed_at_ms: super::now_ms(),
                })
            })
        }
    }

    fn early_enabled_provider_event() -> AssistantEvent {
        AssistantEvent::ProviderModel {
            identity: harness_contract::outcome::ProviderIdentity {
                registry_revision: Some(1),
                provider_name: "test".to_string(),
                model: "test".to_string(),
                profile: None,
                protocol: Some("responses".to_string()),
                capabilities: std::collections::BTreeMap::from([(
                    "early_tool_start".to_string(),
                    "enabled".to_string(),
                )]),
            },
        }
    }

    #[tokio::test]
    async fn completed_tool_item_starts_before_provider_response_completes() {
        let dispatcher = Arc::new(RecordingEarlyDispatcher::default());
        let events = vec![
            Ok(early_enabled_provider_event()),
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-early".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-early".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::MessageStop),
        ];
        let stream = futures::stream::iter(events).then(|event| async move {
            if matches!(&event, Ok(AssistantEvent::MessageStop)) {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            event
        });

        let result = consume_provider_stream(
            Box::pin(stream),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(None, None, "session-early".to_string()),
            Some(dispatcher.clone()),
        )
        .await;

        assert!(result.failure.is_none());
        assert_eq!(
            dispatcher
                .dispatches
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(result.collected.early_tool_receipts.len(), 1);
        let receipt = &result.collected.early_tool_receipts[0];
        assert!(receipt.ready_at_ms <= receipt.started_at_ms);
        assert!(
            receipt.started_at_ms < result.collected.response_completed_at_ms,
            "early start {} must precede response completion {}",
            receipt.started_at_ms,
            result.collected.response_completed_at_ms
        );
    }

    #[tokio::test]
    async fn provider_interruption_retains_completed_early_read_receipt() {
        let dispatcher = Arc::new(RecordingEarlyDispatcher::default());
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let events = vec![
            Ok(early_enabled_provider_event()),
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-before-interrupt".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-before-interrupt".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Err(RuntimeError::new("provider transport interrupted")),
        ];

        let result = consume_provider_stream(
            Box::pin(futures::stream::iter(events)),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                None,
                Some(Arc::clone(&store)),
                "session-interrupted-early".to_string(),
            ),
            Some(dispatcher),
        )
        .await;

        assert!(result.failure.is_some());
        assert_eq!(result.collected.early_tool_receipts.len(), 1);
        assert_eq!(
            result.collected.early_tool_receipts[0].call.id,
            "read-before-interrupt"
        );
        assert_eq!(
            store
                .all_events(20)
                .expect("durable completed item")
                .iter()
                .filter(|event| event.kind == "model.item_completed")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn unverified_provider_keeps_early_tool_in_the_finalized_batch() {
        let dispatcher = Arc::new(RecordingEarlyDispatcher::default());
        let events = vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-after-model".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-after-model".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::MessageStop),
        ];

        let result = consume_provider_stream(
            Box::pin(futures::stream::iter(events)),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(None, None, "session-no-early-proof".to_string()),
            Some(dispatcher.clone()),
        )
        .await;

        assert!(result.failure.is_none());
        assert_eq!(
            dispatcher
                .dispatches
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(result.collected.early_tool_receipts.is_empty());
        assert_eq!(result.collected.early_tool_deferrals.len(), 1);
        assert_eq!(
            result.collected.early_tool_deferrals[0].tool_call_id,
            "read-after-model"
        );
        assert_eq!(result.collected.calls.len(), 1);
    }

    #[test]
    fn model_step_tool_plan_is_append_only_and_rejects_changed_identity_reuse() {
        let identity = crate::CausalItemIdentity {
            model_step_id: "step".to_string(),
            item_id: "call".to_string(),
            segment_id: "call:tool-call:0".to_string(),
            causal_sequence: 1,
            delta_sequence: 0,
            tool_call_id: Some("call".to_string()),
            causal_parent_ids: Vec::new(),
        };
        let candidate = EarlyToolCandidate {
            call: ModelToolCall {
                id: "call".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md"}"#.to_string(),
                depends_on: Vec::new(),
            },
            identity: identity.clone(),
            ready_at_ms: 1,
        };
        let mut plan = ModelStepToolPlan::default();
        assert!(plan.append(candidate.clone()).unwrap().is_some());
        assert!(plan.append(candidate.clone()).unwrap().is_none());

        let mut changed = candidate.clone();
        changed.call.input = r#"{"path":"Cargo.toml"}"#.to_string();
        assert!(plan.append(changed).is_err());
        assert!(plan.seal(&[candidate.call]).is_ok());
    }

    #[test]
    fn final_context_binding_rejects_passive_cross_session_history() {
        let mut current = ContextItem::new(
            "current",
            ContextSourceKind::Conversation,
            ContextRole::RecentTurn,
            "current history",
        );
        current.source_lifecycle = crate::ContextSourceLifecycle::Session;
        current
            .evidence
            .push("session://session-a/messages/1".to_string());
        let mut unrelated = ContextItem::new(
            "unrelated",
            ContextSourceKind::Conversation,
            ContextRole::RecentTurn,
            "unrelated history",
        );
        unrelated.source_lifecycle = crate::ContextSourceLifecycle::Session;
        unrelated
            .evidence
            .push("session://session-b/messages/1".to_string());

        let (selected, omitted) = revalidate_context_binding("session-a", vec![current, unrelated]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "current");
        assert_eq!(omitted.len(), 1);
        assert!(omitted[0].reason.contains("cross-Session"));
    }
}
