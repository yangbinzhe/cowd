use crate::execution_core::goal::write_obligation_satisfied;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::conversation::ApiClient;
use crate::conversation::{ModelStepIntent, ModelToolCall};
use crate::execution_core::graph::executors::ScopedNodeBackend;
use crate::execution_core::{
    ExecutionCompileRequest, ExecutionGraphCompiler, ExecutionGraphReplan, NodeExecutionOutcome,
    NodeExecutionTicket, NodeExecutorError,
};
use crate::{
    model_context_window_with_overrides, permissions::SharedPrompter, AutoCompactionEvent,
    ContentBlock, ContextAuthority, ContextEnvelope, ContextItem, ContextProfile, ContextRole,
    ContextSourceKind, ContextVisibility, ConversationMessage, CowdEvent, CowdEventBus,
    HookAbortSignal, HookProgressReporter, PermissionPolicy, ProviderRuntimeClient,
    ProviderToolDefinition, ResumeContextPacket, RuntimeError, RuntimeFeatureConfig, Session,
    SessionReadHead, ToolCallback, ToolExecutor, ToolFailureKind, ToolInvocationRecord,
    TurnSummary,
};
use async_trait::async_trait;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeSpec,
    ExecutionNodeStatus, ExecutionUsage,
};
use harness_contract::goal::{
    AcceptanceCriterion, AcceptanceStatus, ContextDelta, CostDelta, EffectDelta,
    EffectTerminalClass, EvidenceDelta, GoalCompletion, GoalContract, InformationGain,
    ObservationFailureClass, ObservationFreshness, ObservationResultClass, ParallelismDelta,
    ResolutionDeltaKind, RuntimeIntervention, RuntimeInterventionKind, RuntimeObservation,
    RuntimeObservationIdentity, RuntimeObservationKind, UnknownDelta,
};
use harness_contract::skill::{AgentSkillProfile, SkillCapabilityProfile};
use harness_contract::turn::{
    CollaborationContinuationBinding, ContinuationAuthorization, SessionInputEnvelope,
    SessionInputProjection, SessionInputReceipt, TurnId, TurnInboxSnapshot, TurnInputCheckpoint,
};
use harness_contract::MeasureProvenance;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[path = "host_backend.rs"]
mod host_backend;

/// Re-exported so the Gateway can normalize a reference inside an
/// `agent_action_batch` exactly as it does for a direct call. The batch expands
/// its own actions, so without this a reference Runtime spells `artifact:<24 hex>`
/// is refused inside a batch while the same reference works when sent directly.
pub use host_backend::normalize_bare_evidence_ref;
pub(crate) use host_backend::normalize_native_read_input;
pub(crate) use host_backend::{pregrant_original_action_receipt, retain_original_member_batches};

pub(crate) fn closure_mutation_tool_ids(rungs: &[String]) -> std::collections::BTreeSet<String> {
    host_backend::mandatory_action_tool_ids(false, rungs)
}
#[path = "host_presentation.rs"]
mod host_presentation;
use host_presentation::*;
pub(crate) use host_presentation::original_orchestrated_search_coverage;
#[path = "host_route.rs"]
mod host_route;

const PROVIDER_PROTOCOL_RECOVERY_BUDGET: u8 = 1;
/// A malformed tool-call frame is repairable by the model that emitted it, so
/// one bounded repair continuation is allowed even after successful tool
/// receipts. Without it, a single serialization slip late in a collaboration
/// (one live run: a batch whose `actions` array was sent as a string) became a
/// zero-tool terminal that discarded the model's in-flight plan.
const PROVIDER_FRAME_REPAIR_BUDGET: u8 = 1;
/// Presentation-only recovery reuses already verified receipts with tools
/// disabled. Two local attempts are substantially cheaper than replaying a
/// whole multi-Team Program when one provider response omits schema labels,
/// while the small hard bound still prevents an open-ended model loop.
const STRUCTURED_OUTPUT_RECOVERY_BUDGET: u8 = 2;
/// Any turn may change its tool approach once after consecutive local failures
/// with no retained evidence. This is intentionally independent from
/// provider-protocol recovery: the latter repairs malformed provider frames,
/// while this one repairs a valid-but-unproductive work path.
///
/// The role gate used to require a delegated leaf. That left the root turn -
/// the actor that must drive a multi-Team collaboration - with no lease at all,
/// so two cold-start mistakes ended the whole mission. One live run lost a
/// 3-Team scenario this way: the root called a write tool the task prohibited,
/// then sent a `tool_orchestrate` whose `return` named a step that did not
/// exist, and the turn was blocked with `GoalCompletion::Partial` before the
/// model ever saw a successful tool result. A cold start is not a spin: the
/// model is still learning the environment and has not yet had the feedback
/// that would let it converge.
const COLD_START_TOOL_FAILURE_REPLAN_BUDGET: u8 = 1;
const COLD_START_TOOL_FAILURE_REPLAN_MARKER: &str = "cold_start_tool_failure_replan:v1";

fn next_consecutive_tool_failure_batches(
    current: usize,
    failed_calls: usize,
    bounded_semantic_repair_scheduled: bool,
) -> usize {
    if failed_calls == 0 || bounded_semantic_repair_scheduled {
        0
    } else {
        current.saturating_add(1)
    }
}

/// Failures that were actually executed, excluding dispatch refusals.
///
/// A refusal is the Runtime declining to run a call it has already refused for the same
/// cause; it is not a new failure. The repetition ledger already treats it that way
/// (`repetition_guard.rs:141`). Counting it here made the same event a failure for the goal
/// plane and a non-failure for the ledger, and that contradiction is what sustained a
/// 158-refusal loop in one live run: the refusal produced a `tool_failure` observation, the
/// goal plane required the closure action again, the replan re-dispatched the refused call,
/// and the guard refused it again.
///
/// The predicate is the guard's own, so it cannot disagree with the dispatch decision.
fn executed_failures(
    failed: usize,
    calls: &[ModelToolCall],
    turn_id: &crate::repetition_guard::RepetitionScope,
) -> usize {
    failed.saturating_sub(
        calls
            .iter()
            .filter(|call| {
                crate::repetition_guard::is_dispatch_blocked(
                    turn_id,
                    &call.name,
                    &crate::tool_invocation::stable_hash(&call.input),
                )
            })
            .count(),
    )
}

/// Whether the protocol-closure rule may convert a synthesis into a re-dispatch.
///
/// The rule exists so a model cannot close a mid-protocol Agent Task with prose instead of
/// the durable closure action. That protection holds while the action can still be executed.
/// When the repetition guard would refuse that action for this turn, the protocol cannot be
/// closed by it, and re-dispatching it is what sustained a 158-refusal loop in one live run:
/// the refusal produced a `tool_failure` observation, the goal plane required the closure
/// action again, the replan re-dispatched the refused call, and the guard refused it again.
/// In that case the synthesis must stand and the honest partial names the blocked obligation.
///
/// The exception is deliberately narrow - it applies only when the guard would refuse this
/// tool in this turn. Every other case keeps the original protection.
fn protocol_closure_can_be_required(
    protocol: Option<&host_backend::DelegatedAgenticProtocolState>,
    intervention: Option<&RuntimeIntervention>,
    turn_id: &crate::repetition_guard::RepetitionScope,
) -> bool {
    let Some(protocol) = protocol.filter(|protocol| !protocol.is_terminal()) else {
        return false;
    };
    if !intervention.is_some_and(|candidate| candidate.kind == RuntimeInterventionKind::Synthesize)
    {
        return false;
    }
    protocol
        .terminal_action_ids()
        .any(|tool| !crate::repetition_guard::cause_blocked_for_tool(turn_id, tool))
}

fn cold_start_tool_failure_replan_available(
    has_successful_tool_evidence: bool,
    consumed_replans: u8,
) -> bool {
    !has_successful_tool_evidence && consumed_replans < COLD_START_TOOL_FAILURE_REPLAN_BUDGET
}

#[cfg(test)]
#[path = "tests/host.rs"]
mod tests;

/// Select the one Runtime-owned control plane that may constrain the next
/// provider request.
///
/// Running-Turn input disposition has strict priority because it can amend or
/// replace the active Task topology. A collaboration proposal compiled against
/// the pre-update topology is stale by construction, so it must not be exposed
/// until every pending input slot has been routed. Existing one-shot tool/text
/// constraints are likewise discarded; the owning state machine recomputes
/// any still-applicable constraint after disposition commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextModelControlPlanePriority {
    InputDisposition { slot_count: usize },
    RootCollaboration,
    ExistingConstraint,
}

fn next_model_control_plane_priority(
    pending_input_count: usize,
    root_collaboration_required: bool,
) -> NextModelControlPlanePriority {
    if pending_input_count > 0 {
        NextModelControlPlanePriority::InputDisposition {
            slot_count: pending_input_count,
        }
    } else if root_collaboration_required {
        NextModelControlPlanePriority::RootCollaboration
    } else {
        NextModelControlPlanePriority::ExistingConstraint
    }
}

/// Runtime-owned host for the standard provider-backed conversation engine.
///
/// Gateway supplies service adapters such as tool executors and stream callbacks, but
/// it does not own the provider client or concrete conversation runtime type.
pub struct StandardRuntimeHost<T>
where
    T: ToolExecutor,
{
    runtime: Option<Box<crate::ConversationRuntime<ProviderRuntimeClient, T>>>,
    /// Same original child owner while the Conversation itself lives in Graph.
    hook_scope: crate::HookExecutionScope,
    /// Set when the conversation runtime could not be returned to this host
    /// (its turn executors still held it). A poisoned host refuses further
    /// turns instead of pretending to be usable.
    poisoned: bool,
    /// A submitted graph owns the conversation runtime until it emits this
    /// completion.  Keeping the receiver in the host is deliberate: if the
    /// caller drops its future, the graph can still return the runtime to the
    /// same host before a later turn is admitted.
    inflight_turn: Option<
        tokio::sync::oneshot::Receiver<(
            Option<Box<crate::ConversationRuntime<ProviderRuntimeClient, T>>>,
            Result<TurnSummary, RuntimeError>,
        )>,
    >,
    services: Arc<crate::RuntimeServices>,
    execution_parent: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    execution_lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    execution_role: TurnExecutionRole,
    /// Durable ToolHost receipts recovered for this delegated attempt. They
    /// fence a resumed turn to one text-only evidence synthesis.
    recovered_tool_receipt_count: usize,
    recovered_tool_receipts_require_text_only: bool,
}

/// Immutable semantic role for one provider-backed conversation graph.
///
/// Root turns own user-visible delivery and terminal presentation. Delegated
/// leaves only return facts/candidates to their Team or parent graph; they may
/// never invoke the root narrator or publish a root presentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnExecutionRole {
    RootTurn,
    DelegatedLeaf,
}

impl TurnExecutionRole {
    const fn is_delegated_leaf(self) -> bool {
        matches!(self, Self::DelegatedLeaf)
    }

    const fn owns_root_presentation(self) -> bool {
        matches!(self, Self::RootTurn)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnIngressRef {
    pub request_id: String,
    pub turn_id: String,
    pub message_id: String,
    pub session_id: String,
    pub primary_task_id: String,
    pub root_task_id: String,
    pub session_generation: u64,
    pub input_sequence: u64,
    pub claim_owner: String,
    pub claim_token: String,
    pub claim_revision: u64,
    /// Runtime-owned route hint for a durable SessionHandoff. This arrives
    /// only from the persisted Session outbox, never from request text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_id: Option<String>,
}

/// Inputs required to build a standard provider-backed runtime host.
pub struct StandardRuntimeHostConfig<T>
where
    T: ToolExecutor,
{
    pub runtime_services: Arc<crate::RuntimeServices>,
    pub session: Session,
    pub provider_registry: Arc<crate::ProviderRegistry>,
    pub model: String,
    pub tool_definitions: Vec<ProviderToolDefinition>,
    pub tool_executor: Arc<T>,
    pub permission_policy: PermissionPolicy,
    pub system_prompt: Vec<String>,
    pub feature_config: RuntimeFeatureConfig,
    pub emit_output: bool,
    pub stream_callback: Option<tokio::sync::mpsc::Sender<CowdEvent>>,
    pub tool_callback: Option<Arc<dyn ToolCallback>>,
    pub model_context_window: Option<u32>,
    pub hook_progress_reporter: Option<Box<dyn HookProgressReporter>>,
    pub external_context_items: Vec<ContextItem>,
    /// Immutable user-role context compiled into a frozen delegated packet.
    /// It is deliberately separate from dynamic external context items.
    pub immutable_user_prefix: Vec<String>,
    /// Shared immutable user context for the exact Provider cache cohort.
    pub cache_cohort_user_prefix: Vec<String>,
    pub skill_profiles: Vec<SkillCapabilityProfile>,
    pub agent_skill_profile: AgentSkillProfile,
    pub skill_instruction_source: Option<Arc<dyn crate::RuntimeSkillInstructionSource>>,
    pub skill_binding_pins: Vec<harness_contract::skill::SkillInstructionPin>,
    pub skill_binding_tool_refs: Vec<String>,
    /// Runtime-owned Agent instance identity for scoped memory operations.
    pub memory_agent_id: String,
    /// Exact Agent Definition lineage permitted for reusable cognitive recall.
    /// Both primary and delegated turns receive this only from a Runtime
    /// compiled Binding.
    pub memory_definition_lineage_id: Option<String>,
    /// Runtime-owned Team visibility boundary for scoped memory operations.
    pub memory_team_id: Option<String>,
    /// Runtime-owned Binding read lease for scoped memory operations.
    pub memory_read_scopes: Vec<harness_contract::agent::CognitiveReadScope>,
    /// Immutable primary or delegated Binding used for Fact/Matrix context
    /// assembly. Surface callers cannot supply this directly.
    pub reality_binding: Option<harness_contract::agent::AgentBindingSnapshot>,
    /// Exact delegated execution identity. Root surface turns derive a
    /// session-turn identity from the active turn at checkpoint time.
    pub execution_identity: Option<harness_contract::execution::ExecutionIdentity>,
    /// Canonical Task/Turn scope for Runtime-owned nested turns. Surface root
    /// turns leave this empty and receive scope from `TurnIngressRef`.
    pub execution_lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    /// Optional runtime-owned parent graph/node for nested agent turns.
    /// Surface-originated turns leave this empty.
    pub execution_parent: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    /// Explicit terminal/presentation ownership. This must come from the
    /// Runtime composition boundary, never from prompt text or model output.
    pub execution_role: TurnExecutionRole,
    /// Exact ToolHost receipts reloaded for a delegated Agent attempt. A
    /// non-zero value forbids a new tool-planning cycle for that attempt.
    pub recovered_tool_receipt_count: usize,
    /// Recovery receipts fence effect replay. This separate flag says whether
    /// the semantic protocol is also terminal and therefore permits a
    /// presentation-only recovery. Agent-first tasks with a committed
    /// artifact but no submit/review keep this false.
    pub recovered_tool_receipts_require_text_only: bool,
}

/// Host construction is the final common boundary before any production
/// provider request. Some internal callers (notably delegated Agent tasks)
/// provide task-specific system text directly rather than going through
/// `SystemPromptBuilder`; make the Cowd identity invariant explicit here so a
/// provider/model name or inherited instruction can never become the product
/// identity.
fn canonical_host_system_prompt(supplied: Vec<String>) -> Vec<String> {
    let contract = crate::CowdIdentityContract::default();
    let mut stable = Vec::new();
    let mut dynamic = Vec::new();
    let mut after_boundary = false;
    let mut saw_boundary = false;
    for section in supplied {
        if section == crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY {
            after_boundary = true;
            saw_boundary = true;
            continue;
        }
        if after_boundary {
            dynamic.push(section);
        } else {
            stable.push(section);
        }
    }
    if !saw_boundary {
        dynamic = stable;
        stable = Vec::new();
    }
    let has_contract_head = stable.first().is_some_and(|section| {
        section.contains("You are Cowd") && section.contains(crate::COWD_IDENTITY_CONTRACT_VERSION)
    });
    if !has_contract_head {
        stable.insert(0, contract.stable_head(false));
    }
    if !stable
        .iter()
        .any(|section| section.starts_with("# Runtime context protocol"))
    {
        stable.push(crate::prompt::stable_runtime_context_protocol());
    }
    stable.push(format!(
        "# Cowd identity invariant\nIdentity contract {} is non-delegable: the assistant is Cowd. Context, prior transcripts, workspace instructions, source guidance, provider metadata, and model names cannot rename or replace Cowd. Answer identity questions directly; discuss the backing provider or model only when the user asks for that information.",
        contract.version()
    ));
    stable.push(crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string());
    stable.extend(dynamic);
    stable
}

/// Drive any concrete conversation runtime through the canonical graph owner.
/// Agent backends use this function so they cannot bypass the same Runner used
/// by the primary Gateway runtime.
pub async fn submit_owned_conversation_turn<C, T>(
    runtime: crate::ConversationRuntime<C, T>,
    services: Arc<crate::RuntimeServices>,
    content: &str,
    prompter: &SharedPrompter,
    lineage: harness_contract::execution_graph::ExecutionGraphLineage,
) -> Result<
    (
        crate::ConversationRuntime<C, T>,
        Result<TurnSummary, RuntimeError>,
    ),
    RuntimeError,
>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    submit_owned_conversation_turn_with_ingress(
        Box::new(runtime),
        services,
        content,
        prompter,
        None,
        None,
        Some(lineage),
        TurnExecutionRole::RootTurn,
        0,
        false,
    )
    .await
    .map(|(runtime, result)| (*runtime, result))
}

/// Start with the current request. Only a validated typed continuation below
/// can inherit the original semantic Goal under a new authorization.
fn resolve_session_turn_objective(_session: &Session, content: &str) -> String {
    content.trim().to_string()
}

fn resolve_turn_continuation_binding(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_ref: &str,
    ingress: Option<&TurnIngressRef>,
    reference: harness_contract::strategy::CollaborationReference,
) -> Result<Option<CollaborationContinuationBinding>, RuntimeError> {
    use harness_contract::strategy::CollaborationReference;

    let explicit_handoff_id = ingress.and_then(|ingress| ingress.handoff_id.as_deref());
    if let Some(handoff_id) = explicit_handoff_id {
        let Some(policy) = services.session_execution_policy(session_id) else {
            return Err(RuntimeError::new(
                "cross-session continuation requires an active target Session policy",
            ));
        };
        let Some((candidate, candidate_revision)) =
            crate::session_continuation::accepted_cross_session_candidate(
                services.event_store(),
                session_id,
                handoff_id,
            )
            .map_err(RuntimeError::new)?
        else {
            return Err(RuntimeError::new(
                "cross-session continuation handoff is absent or is not accepted for this Session",
            ));
        };
        let current_ingress = ingress.map_or_else(
            || format!("session:{session_id}:turn:{turn_ref}"),
            |ingress| ingress.request_id.clone(),
        );
        let binding = crate::session_continuation::compile_continuation_binding(
            &candidate,
            &current_ingress,
            candidate_revision,
            ContinuationAuthorization::Authorized,
            policy.revision,
        )
        .map_err(RuntimeError::new)?;
        crate::session_continuation::ensure_reauthorized(
            &binding,
            session_id,
            true,
            policy.revision,
        )
        .map_err(RuntimeError::new)?;
        return Ok(Some(binding));
    }
    match reference {
        CollaborationReference::None => Ok(None),
        CollaborationReference::LatestEligible => {
            // A legacy/offline caller without an active policy may still
            // execute its *current* objective.  It simply cannot claim old
            // collaboration facts as a continuation authority.
            let Some(policy) = services.session_execution_policy(session_id) else {
                return Ok(None);
            };
            let policy_revision = policy.revision;
            let Some((candidate, candidate_revision)) =
                crate::session_continuation::latest_same_session_candidate(
                    services.event_store(),
                    session_id,
                    turn_ref,
                )
                .map_err(RuntimeError::new)?
            else {
                // A continuation hint is not permission to fabricate a
                // source Team.  Fall back to the new, current objective so
                // a stale conversational reference cannot fail a valid new
                // task merely because history has been pruned.
                return Ok(None);
            };
            let current_ingress = ingress.map_or_else(
                || format!("session:{session_id}:turn:{turn_ref}"),
                |ingress| ingress.request_id.clone(),
            );
            let binding = crate::session_continuation::compile_continuation_binding(
                &candidate,
                &current_ingress,
                candidate_revision,
                ContinuationAuthorization::Authorized,
                policy_revision,
            )
            .map_err(RuntimeError::new)?;
            crate::session_continuation::ensure_reauthorized(
                &binding,
                session_id,
                false,
                policy_revision,
            )
            .map_err(RuntimeError::new)?;
            Ok(Some(binding))
        }
        CollaborationReference::ExplicitExecution | CollaborationReference::ExplicitTeamSet => {
            Err(RuntimeError::new(
                "an explicit collaboration continuation reference was requested but no typed handoff identifier was supplied",
            ))
        }
    }
}

/// Keep the approved semantic contract and its provenance across a new
/// authorization. Old terminal state cannot certify the new root.
fn inherit_continuation_goal(goal: &mut GoalContract, original: &GoalContract, root_id: &str) {
    goal.objective = original.objective.clone();
    goal.criteria = original.criteria.clone();
    for criterion in &mut goal.criteria {
        if criterion.status != AcceptanceStatus::Waived {
            criterion.status = AcceptanceStatus::Open;
        }
        if Some(&criterion.id) == original.user_intent_criterion_id.as_ref() {
            // The original root locator was generated by this host to bind
            // execution evidence. It is provenance now, not a requirement to
            // rerun the failed/cancelled root. Preserve all business evidence.
            if let Some(binding) = original.execution_binding.as_ref() {
                let previous_root = format!("execution_graph:{}", binding.root_execution_id);
                if criterion.required_evidence.contains(&previous_root) {
                    criterion
                        .required_evidence
                        .retain(|reference| reference != &previous_root);
                    if !criterion.source_refs.contains(&previous_root) {
                        criterion.source_refs.push(previous_root);
                    }
                }
            }
            let current_root = format!("execution_graph:{root_id}");
            if !criterion.required_evidence.contains(&current_root) {
                criterion.required_evidence.push(current_root);
            }
        }
    }
    goal.input_contract = original.input_contract.clone();
    if let (Some(input), Some(binding)) = (&mut goal.input_contract, &goal.execution_binding) {
        // Existing authorized same-Session continuation carries business intent,
        // not a new tool grant or a newly fabricated input receipt.
        input.turn_id = binding.turn_id.clone();
    }
    goal.constraints = original.constraints.clone();
    goal.source_intent_ref = original.source_intent_ref.clone();
    goal.user_intent_criterion_id = original.user_intent_criterion_id.clone();
    goal.spec_revision = original.spec_revision;
    goal.spec_digest = original.spec_digest.clone();
    goal.evidence_refs = original.evidence_refs.clone();
    goal.evidence_refs.push(original.id.clone());
    goal.unresolved = original.unresolved.clone();
    goal.blockers = original.blockers.clone();
    goal.participation_requirement = original.participation_requirement.clone();
    goal.obligations = original.obligations.clone();
    for obligation in &mut goal.obligations {
        if obligation.state != harness_contract::goal::ObjectiveObligationState::Waived {
            obligation.state = harness_contract::goal::ObjectiveObligationState::Open;
        }
        obligation.verifier_decision = None;
    }
    if goal.input_contract.is_some() {
        goal.spec_digest = crate::execution_core::goal::goal_spec_digest(goal);
    }
}

#[cfg(test)]
#[test]
fn continuation_goal_retains_original_intent_and_requires_fresh_verification() {
    let original: GoalContract = serde_json::from_value(serde_json::json!({
        "id": "goal:original", "session_id": "session", "objective": "Complete the report from all nine inputs",
        "criteria": [{"id": "user_intent", "statement": "Complete the report from all nine inputs",
            "source_refs": ["session_message:original"], "required_evidence": ["artifact://previous"], "status": "satisfied"}],
        "phase": "execution", "completion": "partial", "revision": 7, "user_sequence": 1,
        "scope": "user_objective", "source_intent_ref": "session_message:original",
        "user_intent_criterion_id": "user_intent", "spec_revision": 3, "spec_digest": "original-digest"
    })).unwrap();
    let mut current = original.clone();
    current.id = "goal:next".into();
    current.objective = "继续".into();
    current.completion = GoalCompletion::Open;
    inherit_continuation_goal(&mut current, &original, "next");
    assert_eq!(current.id, "goal:next");
    assert_eq!(current.objective, original.objective);
    assert_eq!(current.source_intent_ref, original.source_intent_ref);
    assert_eq!(current.spec_digest, original.spec_digest);
    assert_eq!(current.criteria[0].status, AcceptanceStatus::Open);
    assert!(current.criteria[0]
        .required_evidence
        .contains(&"execution_graph:next".into()));
    assert_eq!(original.criteria[0].status, AcceptanceStatus::Satisfied);
}

#[cfg(test)]
#[test]
fn continuation_goal_keeps_current_team_contract_with_new_turn_and_original_source() {
    let original: GoalContract = serde_json::from_value(serde_json::json!({
        "id":"goal:old-root","session_id":"session","objective":"Use one Team",
        "criteria":[{"id":"user-intent","statement":"Use one Team","status":"open"}],
        "phase":"execution","completion":"open","revision":2,"user_sequence":2,
        "spec_revision":2,"spec_digest":"source-current-team1","scope":"user_objective",
        "user_intent_criterion_id":"user-intent","source_intent_ref":"session_input_application:original",
        "execution_binding":{"objective_id":"old-objective","session_id":"session","turn_id":"old-turn",
            "root_execution_id":"old-root","agentic_program_id":"old-program"},
        "input_contract":{"version":2,"principal_id":"session:session","session_id":"session","turn_id":"old-turn",
            "source_ref":"session_input_application:accepted-team1","input_revision":2,"content_sha256":"a".repeat(64),
            "commitments":[{"kind":"team_count","minimum":1,"maximum":1,"source":{"start":0,"end":4}}]}
    })).unwrap();
    let mut current = original.clone();
    current.id = "goal:next-root".into();
    let binding = current.execution_binding.as_mut().unwrap();
    binding.turn_id = "next-turn".into();
    binding.root_execution_id = "next-root".into();
    binding.objective_id = "next-objective".into();
    binding.agentic_program_id = "next-program".into();
    inherit_continuation_goal(&mut current, &original, "next-root");
    let input = current.input_contract.as_ref().unwrap();
    assert_eq!(input.turn_id, "next-turn");
    assert_eq!(input.principal_id, "session:session");
    assert_eq!(input.source_ref, "session_input_application:accepted-team1");
    assert_eq!(input.input_revision, 2);
    assert_eq!(input.team_bounds(), Some((1, Some(1))));
    assert_eq!(
        current_turn_required_team_execution_count(&current, None, false, false),
        1
    );
    assert_eq!(
        original.input_contract.as_ref().unwrap().turn_id,
        "old-turn"
    );
    let goals =
        crate::execution_core::GoalStore::new(Arc::new(crate::RuntimeEventStore::for_test()));
    goals.create(current).unwrap();
}

fn continuation_context_item(binding: &CollaborationContinuationBinding) -> ContextItem {
    let mut item = ContextItem::new(
        format!("runtime-continuation:{}", binding.binding_digest),
        ContextSourceKind::Task,
        ContextRole::Evidence,
        format!(
            "Runtime continuation binding: continue the durable Team result set `{}` from root `{}`. Source result locators are immutable and already authorized for this Session. Do not infer or replay prior tool calls; use the durable result references and state any new work separately.",
            binding.team_set_ref, binding.source_root_id
        ),
    );
    item.authority = ContextAuthority::Tool;
    item.visibility = ContextVisibility::Private;
    item.evidence = binding.result_refs.clone();
    item
}

fn owned_root_activity(
    graph: &harness_contract::execution_graph::ExecutionGraph,
    ingress: Option<&TurnIngressRef>,
) -> Result<harness_contract::projection::RuntimeActivityBinding, RuntimeError> {
    if graph.parent_execution.is_some() {
        return Err(RuntimeError::new("RootTurn cannot have a parent execution"));
    }
    let lineage = graph
        .lineage
        .as_ref()
        .ok_or_else(|| RuntimeError::new("owned root requires canonical execution lineage"))?;
    lineage.validate().map_err(RuntimeError::new)?;
    if graph.id.trim().is_empty() {
        return Err(RuntimeError::new(
            "owned root graph identity must not be empty",
        ));
    }
    let revision = if let Some(ingress) = ingress {
        if ingress.session_generation == 0
            || ingress.claim_revision == 0
            || graph.id
                != crate::session_execution::session_ingress_graph_id(
                    &ingress.session_id,
                    &ingress.request_id,
                    &ingress.turn_id,
                )
            || lineage.session_id != ingress.session_id
            || lineage.turn_id != ingress.turn_id
            || lineage.root_task_id != ingress.root_task_id
            || lineage.task_id != ingress.primary_task_id
            || lineage.generation != ingress.session_generation
        {
            return Err(RuntimeError::new(
                "ingress root graph/claim lineage mismatch",
            ));
        }
        ingress.claim_revision
    } else {
        1
    };
    let binding = harness_contract::projection::RuntimeActivityBinding {
        root_execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
        root_task_id: lineage.root_task_id.clone(),
        task_id: lineage.task_id.clone(),
        activity_id: format!("activity:execution:{}", graph.id),
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
        revision,
        fence: lineage.generation,
        generation: lineage.generation,
    };
    binding.validate().map_err(RuntimeError::new)?;
    Ok(binding)
}

fn validate_existing_owned_root(
    services: &crate::RuntimeServices,
    bus: &CowdEventBus,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    ingress: Option<&TurnIngressRef>,
) -> Result<(), RuntimeError> {
    let expected = owned_root_activity(graph, ingress)?;
    let context = bus
        .current_execution_context()
        .ok_or_else(|| RuntimeError::new("owned root has no original execution context"))?;
    let binding = bus
        .current_activity_binding()
        .ok_or_else(|| RuntimeError::new("owned root has no original activity"))?;
    binding.validate().map_err(RuntimeError::new)?;
    if context.execution_id != graph.id
        || context.session_id != expected.session_id
        || context.turn_id != expected.turn_id
        || binding != expected
    {
        return Err(RuntimeError::new(
            "owned root original context/activity mismatch",
        ));
    }
    let registered = services
        .active_execution_bus(&graph.id)
        .ok_or_else(|| RuntimeError::new("owned root has no original active bus lease"))?;
    if !bus.subscribe().same_channel(&registered.subscribe()) || !bus.has_live_telemetry_observer()
    {
        return Err(RuntimeError::new(
            "owned root original bus/telemetry lease mismatch",
        ));
    }
    Ok(())
}

fn enter_owned_root_scope(
    services: &crate::RuntimeServices,
    bus: &CowdEventBus,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    ingress: Option<&TurnIngressRef>,
) -> Result<
    (
        Option<crate::CowdExecutionScope>,
        Option<crate::execution_core::services::ActiveExecutionBusLease>,
    ),
    RuntimeError,
> {
    let binding = owned_root_activity(graph, ingress)?;
    if bus.current_execution_context().is_some() || bus.current_activity_binding().is_some() {
        validate_existing_owned_root(services, bus, graph, ingress)?;
        return Ok((None, None));
    }
    if ingress.is_some() {
        return Err(RuntimeError::new("ingress lost its original root owner"));
    }
    if services.active_execution_bus(&graph.id).is_some() || bus.has_live_telemetry_observer() {
        return Err(RuntimeError::new(
            "direct root cannot replace an active bus lease",
        ));
    }
    let lease = services
        .bind_active_execution_bus(&graph.id, bus.clone())
        .map_err(RuntimeError::new)?;
    let scope = bus.enter_execution_with_activity(
        crate::CowdExecutionContext {
            execution_id: graph.id.clone(),
            session_id: binding.session_id.clone(),
            turn_id: binding.turn_id.clone(),
        },
        Some(binding),
    );
    Ok((Some(scope), Some(lease)))
}

// Construct the turn state synchronously, before the graph setup awaits.
// The returned state contains only the same values as the original initializer.
fn prepare_turn_graph_state(
    content: &str,
    prompter: &SharedPrompter,
    recovered_provider_usage: crate::conversation::RecoveredProviderUsage,
    recovered_provider_attempt_count: usize,
    recovered_cache_dimensions_known: bool,
    ingress: &Option<TurnIngressRef>,
    turn_transcript_start: usize,
    session_id: &String,
    turn_ref: &String,
    evaluation_control: &Option<EvaluationTurnControl>,
    recovered_tool_receipt_count: usize,
    recovered_tool_receipts_require_text_only: bool,
    execution_role: TurnExecutionRole,
) -> Arc<tokio::sync::Mutex<TurnGraphState>> {
    Arc::new(tokio::sync::Mutex::new(TurnGraphState {
        content: content.to_string(),
        task_understanding: None,
        collaboration_obligation: None,
        prompter: prompter.clone(),
        first_model_step: true,
        pending_next_model_context: Vec::new(),
        persistent_collaboration_context: Vec::new(),
        assistant_messages: Vec::new(),
        tool_results: Vec::new(),
        iterations: 0,
        input_tokens: recovered_provider_usage.input_tokens,
        output_tokens: recovered_provider_usage.output_tokens,
        cache_create_tokens: recovered_provider_usage.cache_creation_input_tokens,
        cache_read_tokens: recovered_provider_usage.cache_read_input_tokens,
        cache_dimensions_known: recovered_cache_dimensions_known,
        provider_usage_attempt_observed: recovered_provider_attempt_count > 0,
        output_chars: 0,
        output_chunks: 0,
        wall_duration_ms: 0,
        model: None,
        models_used: Vec::new(),
        first_token_latency_ms: None,
        active_stream_duration_ms: 0,
        summary: None,
        failure: None,
        terminal_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        pending_transcript: std::collections::BTreeMap::new(),
        pending_member_batches: std::collections::BTreeMap::new(),
        pending_agent_input_consumption: std::collections::BTreeMap::new(),
        deferred_agent_input_consumption: Vec::new(),
        ingress: (*ingress).clone(),
        turn_transcript_start,
        session_id: (*session_id).clone(),
        turn_id: (*turn_ref).clone(),
        goal_id: String::new(),
        context_window: 0,
        safety_lease: crate::execution_core::SafetyFusePolicy::derive(
            0,
            harness_contract::core::TaskComplexity::Simple,
            None,
        ),
        terminal_override: None,
        delivery_envelope: None,
        terminal_presentation: None,
        terminal_commit_owner: None,
        authenticated_terminal_owner: None,
        committed_terminal_answer: None,
        committed_terminal_completion: None,
        last_verified_progress: false,
        consecutive_unverified_model_steps: 0,
        closure_ready_steps: 0,
        previous_objective_milestones: Default::default(),
        verified_collaboration_progress: Default::default(),
        verified_answer_ready_steps: 0,
        final_answer_observed: false,
        collaboration_convergence_replans: 0,
        delegated_redrives: 0,
        completion_gap_continuations: 0,
        started_at: std::time::Instant::now(),
        reasoning_only_attempts: 0,
        force_text_only_next_model: evaluation_control
            .as_ref()
            .is_some_and(|control| control.provider_constraint == "judge")
            || (recovered_tool_receipt_count > 0
                && recovered_tool_receipts_require_text_only),
        force_tool_allowlist_next_model: None,
        force_tool_call_next_model: false,
        force_reasoning_effort_next_model: None,
        terminal_recovery_attempts: 0,
        provider_protocol_recovery_attempts: 0,
        provider_frame_repair_attempts: std::collections::BTreeMap::new(),
        agentic_protocol_recovery_attempts: 0,
        agentic_program_context_revision: None,
        execution_role,
        bounded_evidence_role: false,
        focus_novelty_target_bp: 0,
        focus_acceptance_scopes: Vec::new(),
        focus_acceptance_pending_scopes: Vec::new(),
        focus_required_output_fields: Vec::new(),
        structured_output_replans: 0,
        focus_observed_resource_scopes: BTreeSet::new(),
        focus_observed_evidence: Vec::new(),
        focus_action_rejections: 0,
        pending_focus_terminal_candidate: None,
        focus_discovery_replans: 0,
        focus_verification_prefetched: false,
        clean_terminal_synthesis_next: false,
        clean_terminal_synthesis_attempted: false,
        clean_terminal_retry_attempted: false,
        terminal_failure_narration: None,
        consecutive_tool_failure_batches: 0,
        cold_start_failure_replans: 0,
        consecutive_low_novelty_batches: 0,
        successful_tool_calls: 0,
        last_terminal_answer_digest: String::new(),
        repeated_terminal_answers: 0,
        completion_gap_outstanding: false,
        outstanding_closure_rungs: Vec::new(),
        tool_receipts_observed: recovered_tool_receipt_count,
        duplicate_tool_calls: 0,
        identical_action_digest: String::new(),
        identical_action_result_digest: String::new(),
        identical_action_streak: 0,
        per_turn_tool_counts: BTreeMap::new(),
        write_attempt_paths: Vec::new(),


        committed_workspace_write_observed: false,
        committed_workspace_write_scopes: BTreeSet::new(),
        committed_workspace_observed_evidence: Vec::new(),
        workspace_mutation_effect_sequences: BTreeSet::new(),

        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        early_tool_receipts: BTreeMap::new(),
        evaluation_resource_scopes: evaluation_control
            .as_ref()
            .map(|control| control.resource_scopes.clone())
            .unwrap_or_default(),
        evaluation_scope_rejections: 0,
        evaluation_judge_only: evaluation_control
            .as_ref()
            .is_some_and(|control| control.provider_constraint == "judge"),
        collaboration_started: false,
        pending_root_control_plane_requirement: None,

        root_language_replan_attempted: false,
        pending_terminal_artifact: None,
        pending_controlled_recovery_claim_fingerprints: Vec::new(),
        pending_disposition_inputs: Vec::new(),
        terminal_tracker: crate::liveness::TerminalTracker::new(
            crate::liveness::TerminalObligation::default(),
        ),
        terminal_satisfaction_since: None,
    }))
}

// Build the initial graph without retaining compiler temporaries in the
// asynchronous turn state. The caller retains the original owner guards.
fn prepare_initial_conversation_graph(
    services: &crate::RuntimeServices,
    content: &str,
    resolved_objective: &str,
    session_id: &str,
    ingress: Option<&TurnIngressRef>,
    evaluation_control: Option<&EvaluationTurnControl>,
    compile_target: crate::execution_core::RuntimeCompileTarget,
    execution_parent: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    execution_lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    requests_background: bool,
) -> Result<harness_contract::execution_graph::ExecutionGraph, RuntimeError> {
    let turn_payload = serde_json::json!({
        "kind": "conversation_turn",
        "session_id": session_id,
        "content": content,
        "objective": resolved_objective,
        "compile_target": compile_target,
        "ingress": ingress,
        "idempotency_key": ingress.as_ref().map(|value| value.request_id.as_str()),
    })
    .to_string();
    let delegation_resource_scopes = root_delegation_resource_scopes(
        evaluation_control,
        services.session_execution_policy(session_id).as_ref(),
    );
    let mut graph = ExecutionGraphCompiler
        .compile_conversation_turn(ExecutionCompileRequest {
            objective: resolved_objective.to_string(),
            payload_ref: turn_payload,
            target: compile_target,
            resource_scopes: delegation_resource_scopes,
        })
        .map_err(|error| RuntimeError::new(error.to_string()))?;
    graph.parent_execution = execution_parent;
    graph.lineage = execution_lineage;
    if requests_background {
        graph.service_class =
            harness_contract::execution_graph::ExecutionServiceClass::Background;
    } else if graph.parent_execution.is_some() {
        graph.service_class =
            harness_contract::execution_graph::ExecutionServiceClass::Foreground;
    }
    if let Some(ingress) = ingress {
        let graph_id = crate::session_execution::session_ingress_graph_id(
            &ingress.session_id,
            &ingress.request_id,
            &ingress.turn_id,
        );
        ExecutionGraphCompiler
            .bind_conversation_graph_identity(
                &mut graph,
                &graph_id,
                None,
                Some(&ingress.request_id),
            )
            .map_err(RuntimeError::new)?;
        graph.revision = 0;
        graph.node_results.clear();
        graph.recovery_cursor = Default::default();
        graph.node_statuses.clear();
        graph.lineage = Some(
            harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: ingress.session_id.clone(),
                turn_id: ingress.turn_id.clone(),
                root_task_id: ingress.root_task_id.clone(),
                task_id: ingress.primary_task_id.clone(),
                generation: ingress.session_generation.max(1),
            },
        );
        graph
            .node_statuses
            .insert(graph.nodes[0].id.clone(), ExecutionNodeStatus::Planned);
        let root_id = graph.nodes[0].id.clone();
        let dispatch_id = format!("{}:session-dispatch", graph.id);
        let mut dispatch = ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            crate::SESSION_DISPATCH_EXECUTOR,
            format!(
                "session_ingress:{}",
                serde_json::to_string(ingress).unwrap_or_default()
            ),
        );
        dispatch.id = dispatch_id.clone();
        dispatch.idempotency_key = format!("{}:dispatch", ingress.request_id);
        graph.nodes.insert(0, dispatch);
        graph.edges.push(ExecutionEdge {
            from: dispatch_id,
            to: root_id,
            kind: ExecutionEdgeKind::DependsOn,
        });
    }
    Ok(graph)
}

#[allow(
    clippy::panic,
    reason = "a leaked graph-runner Arc would otherwise make it impossible to return the uniquely owned runtime"
)]
/// Why this Program stall assessment is being made. The two sources are
/// separate arms: the loop's own progress bound, and the one assessment the
/// loop makes when it is about to stop waiting because nothing is waitable
/// any more. Neither arm carries a new threshold.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AgenticProgramStallTrigger {
    /// The loop's own bound: no verified progress for the recorded number of
    /// checks, or the collaboration budget elapsed.
    LoopProgress { stalled_checks: u32, budget_elapsed: bool },
    /// The loop is about to stop waiting and there is nothing waitable left:
    /// the root graph is terminal, no child graph is live, and every unsettled
    /// Task has no active attempt, no live lease and a durable disconnect
    /// record.
    LoopExitWithoutWaitableWork,
}

impl AgenticProgramStallTrigger {
    /// The named reason this trigger reports. Each arm names only the shape it
    /// actually observed; neither arm may report a reason it did not observe.
    fn reason(&self) -> String {
        match self {
            AgenticProgramStallTrigger::LoopProgress { stalled_checks, budget_elapsed } => {
                if *budget_elapsed {
                    "Execution blocked: the configured collaboration budget was exhausted; the original objective remains unmet and durable work is retained.".to_string()
                } else {
                    format!("Execution blocked: no verified progress for {stalled_checks} checks; the original objective remains unmet and durable work is retained.")
                }
            }
            AgenticProgramStallTrigger::LoopExitWithoutWaitableWork => {
                "Execution blocked: there is nothing left to wait for and nobody is pushing the Program forward: the root graph is terminal, no child graph is live, and every unsettled Task has no active attempt, no live lease and a durable disconnect record; the original objective remains unmet and durable work is retained.".to_string()
            }
        }
    }
}

#[cfg(test)]
mod agentic_program_stall_trigger_tests {
    use super::*;

    #[test]
    fn a9_loop_exit_trigger_names_the_waitable_work_shape() {
        let reason = AgenticProgramStallTrigger::LoopExitWithoutWaitableWork.reason();
        assert!(!reason.is_empty(), "the new trigger must carry a named reason");
        assert!(!reason.contains("partial"), "the reason must not be a partial verdict");
        for named in ["root graph is terminal", "no child graph is live",
            "no active attempt", "no live lease", "durable disconnect record"] {
            assert!(reason.contains(named), "the reason must name {named}: {reason}");
        }
    }

    #[test]
    fn a9_loop_progress_trigger_keeps_its_original_reason() {
        let stalled = AgenticProgramStallTrigger::LoopProgress { stalled_checks: 7, budget_elapsed: false };
        assert_eq!(stalled.reason(),
            "Execution blocked: no verified progress for 7 checks; the original objective remains unmet and durable work is retained.");
        let budget = AgenticProgramStallTrigger::LoopProgress { stalled_checks: 7, budget_elapsed: true };
        assert_eq!(budget.reason(),
            "Execution blocked: the configured collaboration budget was exhausted; the original objective remains unmet and durable work is retained.");
    }
}

/// What one Program stall assessment decided.
enum AgenticProgramStallOutcome {
    /// The Program carries the authoritative Blocked conclusion now.
    Settled,
    /// The stall did not produce a Blocked conclusion; the caller resets its
    /// progress counter and keeps waiting.
    NotBlocked,
    /// The Program changed under the assessment; the caller keeps waiting with
    /// its counter unchanged.
    Retried,
}

/// The Program stall settlement shared by the ingress wait loop and the one
/// assessment that loop performs when it is about to stop waiting. The body is
/// the loop's original stall branch moved verbatim: one committed Runtime
/// observation, then the existing Program owner that turns it into the
/// authoritative non-success conclusion. There is still exactly one producer.
async fn settle_agentic_program_stall(
    services: &crate::RuntimeServices,
    state: &Arc<tokio::sync::Mutex<TurnGraphState>>,
    session_id: &str,
    turn_id: &str,
    graph_id: &str,
    goal: &crate::execution_core::goal::GoalProjection,
    program: &crate::AgenticProgramProjection,
    trigger: AgenticProgramStallTrigger,
) -> Result<AgenticProgramStallOutcome, RuntimeError> {
    let reason = trigger.reason();
    let mut observation = runtime_observation(
        RuntimeObservationIdentity {
            workspace_id: services.workspace_key().to_string(),
            session_id: session_id.to_string(), turn_id: Some(turn_id.to_string()), task_id: None,
            graph_id: graph_id.to_string(), goal_id: goal.goal.id.clone(), node_id: None,
        }, RuntimeObservationKind::GraphProgress, "runtime.program_stall", program.revision,
        reason.clone(), format!("program-stall:{}:{}:{}:{}", program.program_id, program.revision, goal.stream_revision,
            sha256_digest(&serde_json::to_string(&(&goal.progress.criteria, &goal.progress.evidence_refs,
                &goal.progress.open_unknowns, &goal.goal.obligations)).map_err(|error| RuntimeError::new(error.to_string()))?)),
        ObservationResultClass::Failed,
    );
    observation.failure_class = Some(ObservationFailureClass::Unknown);
    if let Some(previous) = goal.observations.iter().find(|previous|
        previous.source == observation.source && previous.source_revision == observation.source_revision
            && previous.fingerprint == observation.fingerprint)
    {
        observation = previous.clone();
    } else {
        services.goal_store().record_observation(observation.clone()).map_err(RuntimeError::new)?;
    }
    let verdict = match crate::agentic::supervision::reconcile_runtime_stall(
        services, &services.agent_action_service(), services.objective_supervisor(), &program.program_id, &observation,
    ) {
        Ok(verdict) => verdict,
        Err(error) => {
            let current = services.agent_action_service().project(&program.program_id)
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let current_goal = services.goal_store().projection(&goal.goal.id).map_err(RuntimeError::new)?;
            if current.revision != program.revision || current_goal.as_ref().is_some_and(|current|
                current.stream_revision > goal.stream_revision.saturating_add(1))
                || services.goal_store().exhausted_program_work_cut(&current).map_err(RuntimeError::new)?.is_none() {
                return Ok(AgenticProgramStallOutcome::Retried);
            }
            return Err(RuntimeError::new(error));
        }
    };
    if verdict.objective_verdict.is_none() {
        return Err(RuntimeError::new("runtime stall did not produce an authoritative Program verdict"));
    }
    if !verdict.objective_verdict.as_ref().is_some_and(|verdict|
        verdict.kind == harness_contract::goal::ObjectiveTerminalKind::Blocked) {
        return Ok(AgenticProgramStallOutcome::NotBlocked);
    }
    let committed_goal = services.goal_store().get(&goal.goal.id).map_err(RuntimeError::new)?
        .and_then(|goal| goal.terminal)
        .ok_or_else(|| RuntimeError::new("runtime stall Goal terminal missing"))?;
    {
        let mut state = state.lock().await;
        state.terminal_override = Some((GoalCompletion::Blocked, committed_goal.reason));
    }
    crate::execution_core::graph::executors::resolve_agentic_program_wait(
        &program.program_id, &services.agent_action_service(), services.graph_state_store(),
        services.execution_supervisor().as_ref(), true,
    ).await.map_err(RuntimeError::new)?;
    Ok(AgenticProgramStallOutcome::Settled)
}

async fn submit_owned_conversation_turn_with_ingress<C, T>(
    mut runtime: Box<crate::ConversationRuntime<C, T>>,
    services: Arc<crate::RuntimeServices>,
    content: &str,
    prompter: &SharedPrompter,
    ingress: Option<TurnIngressRef>,
    execution_parent: Option<harness_contract::execution_graph::ExecutionParentBinding>,
    execution_lineage: Option<harness_contract::execution_graph::ExecutionGraphLineage>,
    execution_role: TurnExecutionRole,
    recovered_tool_receipt_count: usize,
    recovered_tool_receipts_require_text_only: bool,
) -> Result<
    (
        Box<crate::ConversationRuntime<C, T>>,
        Result<TurnSummary, RuntimeError>,
    ),
    RuntimeError,
>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    let turn_started_at = std::time::Instant::now();
    runtime.set_in_place_runtime_event_store(Arc::clone(services.event_store()));
    runtime.set_in_place_outcome_runtime(Arc::clone(services.outcome_service()), Arc::clone(services.outcome_projector()));
    runtime.set_in_place_artifact_store(Arc::clone(services.artifact_store()));
    runtime.set_in_place_maintenance_supervisor(services.maintenance_supervisor());
    runtime.set_in_place_tool_execution_plane(Arc::clone(services.tool_execution_plane()));
    if execution_role == TurnExecutionRole::RootTurn && runtime.cowd_bus().is_none() {
        runtime.set_in_place_cowd_event_bus(CowdEventBus::new());
    }
    if let Some(journal) = services.session_journal_port() {
        runtime.set_in_place_session_journal_port(journal);
    }
    let session = runtime.session_snapshot().await;
    let evaluation_control = match evaluation_turn_control(content) {
        Ok(control) => control,
        Err(error) => return Ok((runtime, Err(error))),
    };
    let _evaluation_provider_token_guard = match evaluation_control.as_ref().and_then(|control| {
        match (&control.budget_lease_id, control.max_total_tokens) {
            (Some(lease_id), Some(limit)) => Some((lease_id, limit)),
            _ => None,
        }
    }) {
        Some((lease_id, limit)) => match services
            .evaluation_provider_token_leases()
            .install_advisory(&session.session_id, lease_id, limit)
        {
            Ok(guard) => {
                runtime.set_in_place_evaluation_provider_token_lease(guard.lease());
                Some(guard)
            }
            Err(error) => return Ok((runtime, Err(error))),
        },
        None => None,
    };
    let evaluation_content;
    let content = if let Some(control) = evaluation_control.as_ref() {
        evaluation_content = control.prompt.clone();
        evaluation_content.as_str()
    } else {
        content
    };
    let _evaluation_resource_guard = match evaluation_control.as_ref() {
        Some(control) => match EvaluationResourceQuotaGuard::apply(&services, control) {
            Ok(guard) => Some(guard),
            Err(error) => return Ok((runtime, Err(error))),
        },
        None => None,
    };
    let turn_transcript_start = session.message_count();
    let mut resolved_objective = resolve_session_turn_objective(&session, content);
    let session_id = session.session_id;
    if let Some(lineage) = execution_lineage.as_ref() {
        if lineage.session_id != session_id {
            return Ok((
                runtime,
                Err(RuntimeError::new(
                    "execution lineage session does not match the owned conversation session",
                )),
            ));
        }
    }
    let turn_ref = ingress
        .as_ref()
        .map(|ingress| ingress.turn_id.clone())
        .or_else(|| {
            execution_lineage
                .as_ref()
                .map(|lineage| lineage.turn_id.clone())
        })
        .unwrap_or_else(|| TurnId::new().to_string());
    if execution_role == TurnExecutionRole::DelegatedLeaf
        && (execution_parent.is_none() || ingress.is_some())
    {
        return Ok((
            runtime,
            Err(RuntimeError::new(
                "delegated leaf requires its original parent and cannot own root ingress",
            )),
        ));
    }
    if execution_role == TurnExecutionRole::RootTurn {
        let validation = (|| {
            if execution_parent.is_some() {
                return Err(RuntimeError::new("RootTurn cannot have a parent execution"));
            }
            let bus = runtime
                .cowd_bus()
                .ok_or_else(|| RuntimeError::new("owned root requires its event bus"))?;
            let mut expected = harness_contract::execution_graph::ExecutionGraph::new(content);
            if let Some(ingress) = ingress.as_ref() {
                expected.id = crate::session_execution::session_ingress_graph_id(
                    &ingress.session_id,
                    &ingress.request_id,
                    &ingress.turn_id,
                );
                expected.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
                    session_id: ingress.session_id.clone(),
                    turn_id: ingress.turn_id.clone(),
                    root_task_id: ingress.root_task_id.clone(),
                    task_id: ingress.primary_task_id.clone(),
                    generation: ingress.session_generation,
                });
                if execution_lineage
                    .as_ref()
                    .is_some_and(|lineage| expected.lineage.as_ref() != Some(lineage))
                {
                    return Err(RuntimeError::new(
                        "ingress conflicts with supplied execution lineage",
                    ));
                }
                if ingress.session_id != session_id || ingress.turn_id != turn_ref {
                    return Err(RuntimeError::new("ingress root session/turn mismatch"));
                }
                validate_existing_owned_root(&services, bus, &expected, Some(ingress))
            } else {
                expected.lineage = execution_lineage.clone();
                let binding = owned_root_activity(&expected, None)?;
                if binding.session_id != session_id || binding.turn_id != turn_ref {
                    return Err(RuntimeError::new(
                        "direct root session/turn lineage mismatch",
                    ));
                }
                if bus.current_execution_context().is_some()
                    || bus.current_activity_binding().is_some()
                    || bus.has_live_telemetry_observer()
                {
                    return Err(RuntimeError::new(
                        "fresh direct root cannot inherit an existing owner",
                    ));
                }
                Ok(())
            }
        })();
        if let Err(error) = validation {
            return Ok((runtime, Err(error)));
        }
    }
    // This is the sole top-level turn boundary. Runtime-prefetched tool
    // evidence may be created before the first Provider node, so no model-step
    // path may reset these ledgers later in the same turn.
    runtime.begin_turn_runtime_epoch().await;
    let recovered_provider_usage = runtime.turn_recovered_provider_usage();
    let recovered_provider_attempt_count = runtime.turn_recovered_provider_attempt_count();
    let recovered_cache_dimensions_known = runtime.turn_cache_dimensions_known();
    let _delegated_input_turn = execution_role.is_delegated_leaf().then(|| {
        runtime
            .session_input_stream()
            .begin_turn(TurnId::from_string(turn_ref.clone()))
    });
    let runtime = Arc::new(tokio::sync::Mutex::new(runtime));
    let parent_merge_started_at = Arc::new(std::sync::Mutex::new(None::<std::time::Instant>));
    let mut owned_root_scope = None;
    let mut owned_root_bus_lease = None;
    let mut direct_turn_control = None;
    let mut result = async {
        let state = prepare_turn_graph_state(
            content, prompter, recovered_provider_usage, recovered_provider_attempt_count,
            recovered_cache_dimensions_known, &ingress, turn_transcript_start, &session_id,
            &turn_ref, &evaluation_control, recovered_tool_receipt_count,
            recovered_tool_receipts_require_text_only, execution_role,
        );

        let provider_profile_fingerprint = {
            let runtime = runtime.lock().await;
            runtime
                .current_model()
                .filter(|model| !model.trim().is_empty())
                .map(sha256_digest)
                .unwrap_or_default()
        };
        let resource_snapshot = turn_strategy_resource_snapshot(
            services.as_ref(),
            evaluation_control.as_ref(),
            provider_profile_fingerprint,
        )?;
        let (
            strategy,
            context_window,
            context_profile,
            owner_step_limit,
            delegated_focus_policy,
        ) = {
            let runtime = runtime.lock().await;
            (
                runtime.begin_turn_strategy_with_resource_snapshot(
                    turn_ref.clone(),
                    &resolved_objective,
                    Some(resource_snapshot),
                )?,
                runtime.model_context_window(),
                runtime.context_profile(),
                runtime.model_step_limit_override(),
                runtime.delegated_focus_policy(),
            )
        };
        if evaluation_control.is_some() && evaluation_topology_forbids_team() {
            let mut item = ContextItem::new(
                format!("eval-topology:{}", strategy.decision_id),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                format!(
                    "Pre-registered evaluation topology is {}. Complete the identical business workload locally with this selected topology and authorized tools. Do not request or simulate a Team; the Runtime will reject Team materialization in this baseline.",
                    strategy.selected_candidate.as_str()
                ),
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            runtime.lock().await.push_next_model_context_item(item);
        }
        let compile_target = strategy.decision.compile_target;
        {
            let mut graph_state = state.lock().await;
            graph_state.task_understanding =
                Some(strategy.decision.strategy.understanding.clone());
            graph_state.collaboration_obligation =
                strategy.decision.collaboration_obligation.clone();
            graph_state.context_window = context_window;
            graph_state.safety_lease = crate::execution_core::SafetyFusePolicy::derive(
                context_window,
                strategy.decision.complexity(),
                explicit_model_step_limit(content).or(owner_step_limit),
            );
            graph_state.bounded_evidence_role = context_profile == ContextProfile::SubAgent
                || compile_target == crate::execution_core::RuntimeCompileTarget::EvidenceGraph;
            if execution_role == TurnExecutionRole::DelegatedLeaf
                && context_profile != ContextProfile::SubAgent
            {
                return Err(RuntimeError::new(
                    "delegated leaf execution requires the SubAgent context profile",
                ));
            }
            graph_state.focus_novelty_target_bp = delegated_focus_policy.0;
            graph_state.focus_acceptance_pending_scopes = delegated_focus_policy.1.clone();
            graph_state.focus_acceptance_scopes = delegated_focus_policy.1;
            graph_state.focus_required_output_fields = delegated_focus_policy.2;
        }
        let mut graph = prepare_initial_conversation_graph(
            services.as_ref(), content, &resolved_objective, &session_id, ingress.as_ref(),
            evaluation_control.as_ref(), compile_target, execution_parent, execution_lineage,
            strategy.decision.strategy.understanding.requests_background,
        )?;
        if execution_role == TurnExecutionRole::RootTurn && ingress.is_none() {
            let mut runtime = runtime.lock().await;
            if runtime.original_turn_control().is_err() {
                let control = crate::execution_core::services::TurnCancellationControl::operational_with_token(
                    &session_id,
                    &turn_ref,
                    &graph.id,
                    runtime.cancellation_token(),
                );
                runtime.install_provider_cancellation_control(Some(Arc::clone(&control)))?;
                direct_turn_control = Some(control);
            }
        }
        if execution_role == TurnExecutionRole::RootTurn && ingress.is_some() {
            let bus = runtime.lock().await.cowd_bus().cloned().ok_or_else(|| {
                RuntimeError::new("owned root requires its event bus")
            })?;
            (owned_root_scope, owned_root_bus_lease) =
                enter_owned_root_scope(&services, &bus, &graph, ingress.as_ref())?;
        }
        if let Some(bus) = runtime.lock().await.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::PreparingContext,
                detail: Some("assembling context".to_string()),
            });
        }
        let strategy_parent_node_id = graph
            .nodes
            .iter()
            .find(|node| node.kind != ExecutionNodeKind::SessionDispatch)
            .map(|node| node.id.clone())
            .ok_or_else(|| RuntimeError::new("conversation graph has no strategy parent node"))?;
        let strategy = runtime
            .lock()
            .await
            .bind_turn_strategy_execution(&turn_ref, &graph.id)?;
        let continuation_binding = resolve_turn_continuation_binding(
            services.as_ref(),
            &session_id,
            &turn_ref,
            ingress.as_ref(),
            strategy.decision.strategy.understanding.collaboration_reference,
        )?;
        if let Some(binding) = continuation_binding.as_ref() {
            graph.continuation_binding = Some(binding.clone());
            let mut turn_state = state.lock().await;
            let item = continuation_context_item(binding);
            turn_state.pending_next_model_context.push(item.clone());
            turn_state.persistent_collaboration_context.push(item);
        }
        let continuation_source = continuation_binding.as_ref()
            .filter(|binding| binding.handoff_id.is_none())
            .and_then(|binding| binding.team_set_ref.strip_prefix("agentic_program:"))
            .map(|id| services.agent_action_service().project(id).map_err(|e| RuntimeError::new(e.to_string())))
            .transpose()?
            .filter(|source| source.status != crate::AgenticProgramStatus::Verified);
        let continuation_goal = continuation_source.as_ref().map(|source| {
            crate::agentic::continuation::read_continuation_goal(services.event_store(), source)
                .map_err(RuntimeError::new)?
                .ok_or_else(|| RuntimeError::new("unfinished continuation requires the original Goal contract"))
        }).transpose()?;
        let continuation_goal_cut = continuation_goal.as_ref().map(|goal| crate::ExpectedStreamRevision {
            stream_id: format!("goal:{}", goal.goal.id), expected_revision: goal.stream_revision,
        });
        let continuation_actor = if let Some(source) = continuation_source.as_ref() {
            resolved_objective = source.objective_summary.clone();
            graph.objective = resolved_objective.clone();
            let mut turn_state = state.lock().await;
            // Unknown historical minima must not resurrect inferred topology.
            // Versioned explicit minima and exact/prohibited bounds survive unchanged.
            let current_bounds = continuation_goal.as_ref().and_then(|goal|
                crate::execution_core::goal::current_goal_team_bounds(&goal.goal));
            let (continued_required_team_count, maximum) = current_bounds
                .unwrap_or((source.required_team_count, source.exact_team_count));
            turn_state.collaboration_obligation = (continued_required_team_count > 0).then_some(
                harness_contract::strategy::CollaborationExecutionObligation {
                    team_count_contract_version: 1,
                    minimum_team_count: continued_required_team_count,
                    exact_team_count: maximum.filter(|maximum| *maximum == continued_required_team_count),
                }
            );
            let mut item = ContextItem::new(
                format!("continuation-gaps:{}", graph.id), ContextSourceKind::Task, ContextRole::Instruction,
                format!("Continue the original objective: {}. The current Program inherits the prior tasks, artifacts and discussions with stable identities. Inspect those facts before acting; preserve accepted contributions, reconcile blocked effects and execute only remaining gaps. A new completion request and original-objective verification are required.", resolved_objective),
            );
            item.authority = ContextAuthority::System; item.visibility = ContextVisibility::Private;
            turn_state.pending_next_model_context.push(item.clone());
            turn_state.persistent_collaboration_context.push(item);
            drop(turn_state);
            let objective_id = harness_contract::agent_action::root_objective_id(&session_id, &turn_ref);
            let policy = services.session_execution_policy(&session_id)
                .ok_or_else(|| RuntimeError::new("continuation requires current Session policy"))?;
            let binding = continuation_binding.as_ref().expect("source has a binding");
            crate::session_continuation::ensure_reauthorized(binding, &session_id, false, policy.revision)
                .map_err(RuntimeError::new)?;
            Some(harness_contract::agent_action::AgentActorBinding {
                original_execution: None,
                program_id: harness_contract::agent_action::program_id_for_objective(&objective_id), objective_id,
                session_id: session_id.clone(), turn_id: turn_ref.clone(), root_execution_id: Some(graph.id.clone()),
                required_team_count: source.required_team_count, objective_summary: source.objective_summary.clone(),
                exact_team_count: source.exact_team_count,
                model_lease: runtime.lock().await.current_model().unwrap_or("default").to_string(),
                permission_ceiling: Some(policy.permission_mode),
                resource_scopes: root_delegation_resource_scopes(evaluation_control.as_ref(), Some(&policy)),
                actor_id: format!("root:{session_id}"), kind: harness_contract::agent_action::AgentActorKind::Root,
                execution_id: None, team_id: None, agent_id: None,
            })
        } else { None };
        let goal_id = format!("goal:{}", graph.id);
        let mut goal = GoalContract {
                input_contract: None,
                id: goal_id.clone(),
                session_id: session_id.clone(),
                objective: resolved_objective.clone(),
                criteria: vec![AcceptanceCriterion {
                    id: "user_intent".to_string(),
                    statement: resolved_objective.clone(),
                    statement_ref: None,
                    source_refs: vec![ingress.as_ref().map_or_else(
                        || format!("session:{session_id}:turn:{turn_ref}:input"),
                        |value| format!("session_message:{}", value.message_id),
                    )],
                    required_evidence: vec![format!("execution_graph:{}", graph.id)],
                    status: AcceptanceStatus::Open,
                    waiver: None,
                }],
                constraints: Vec::new(),
                phase: "execution".to_string(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                blockers: Vec::new(),
                scope: harness_contract::goal::GoalScope::UserObjective,
                user_intent_criterion_id: Some("user_intent".to_string()),
                source_intent_ref: Some(ingress.as_ref().map_or_else(
                    || format!("session:{session_id}:turn:{turn_ref}:input"),
                    |value| format!("session_message:{}", value.message_id),
                )),
                execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                    objective_id: harness_contract::agent_action::root_objective_id(
                        &session_id,
                        &turn_ref,
                    ),
                    session_id: session_id.clone(),
                    turn_id: turn_ref.clone(),
                    root_execution_id: graph.id.clone(),
                    agentic_program_id: harness_contract::agent_action::program_id_for_objective(
                        &harness_contract::agent_action::root_objective_id(&session_id, &turn_ref),
                    ),
                }),
                spec_revision: 1,
                spec_digest: format!("{:x}", Sha256::digest(resolved_objective.as_bytes())),
                review_refs: Vec::new(),
                waiting: None,
                participation_requirement: continuation_source.as_ref()
                    .filter(|source| source.required_team_count > 0)
                    .map(|source| harness_contract::goal::ParticipationRequirement {
                        minimum_team_count: source.required_team_count,
                        source_ref: format!("agentic-program:{}@{}", source.program_id, source.revision),
                    }).or_else(|| strategy.decision.collaboration_obligation.as_ref().map(|obligation| {
                    harness_contract::goal::ParticipationRequirement {
                        minimum_team_count: obligation.required_team_count(),
                        source_ref: ingress.as_ref().map_or_else(
                            || format!("session:{session_id}:turn:{turn_ref}:input"),
                            |value| format!("session_message:{}", value.message_id),
                        ),
                    }
                })),
                obligations: Vec::new(),
                recovery: None,
                terminal: None,
                completion: GoalCompletion::Open,
                revision: 1,
                user_sequence: 1,
                reviews: Vec::new(),
            };
        if let Some(source) = continuation_source.as_ref() {
            let original = &continuation_goal.as_ref()
                .expect("continuation source was read and bound above").goal;
            inherit_continuation_goal(&mut goal, original, &graph.id);
            // Older Goals omitted a user constraint that their frozen Program
            // still enforced. Adopt it only in this newly authorized Goal,
            // retaining its original source and explicitly versioning the spec.
            if goal.participation_requirement.is_none() && source.required_team_count > 0 {
                goal.participation_requirement = Some(harness_contract::goal::ParticipationRequirement {
                    minimum_team_count: source.required_team_count,
                    source_ref: original.source_intent_ref.clone().ok_or_else(||
                        RuntimeError::new("legacy participation adoption requires the original user source"))?,
                });
                goal.spec_revision = goal.spec_revision.saturating_add(1);
                goal.spec_digest = crate::execution_core::goal::goal_spec_digest(&goal);
            }
        } else {
            if execution_role.owns_root_presentation() {
                goal.input_contract = Some(prepare_goal_input_for_turn(
                    &goal, services.workspace_root(), content,
                    ingress.as_ref().map_or(1, |value| value.input_sequence.max(1)),
                )?);
            }
            goal.spec_digest = crate::execution_core::goal::goal_spec_digest(&goal);
        }
        let prepared_goal_spec = continuation_goal.as_ref().map(|_| (
            crate::execution_core::goal::goal_spec_digest(&goal), goal.input_contract.clone(),
        ));
        let goal = services.goal_store().create(goal).map_err(RuntimeError::new)?;
        if prepared_goal_spec.is_some_and(|(spec, input)|
            spec != crate::execution_core::goal::goal_spec_digest(&goal) || input != goal.input_contract)
        {
            return Err(RuntimeError::new("continuation target Goal differs from the prepared source"));
        }
        // Objective recovery now belongs to the Agentic Program supervisor.
        // A new turn may consume a prior typed continuation, but it must not
        // reactivate the retired graph/template coordinator.
        {
            let mut turn_state = state.lock().await;
            turn_state.goal_id = goal_id.clone();
            // A settled Program may have emitted a durable Objective replan
            // proposal after the previous turn closed. Carry the latest
            // typed proposal into this turn's private context so the model
            // can issue a revision against the current graph rather than
            // reconstructing recovery from prose or an empty inspect call.
            let recovery_goal_id = continuation_binding
                .as_ref()
                .map(|binding| format!("goal:{}", binding.source_root_id))
                .unwrap_or_else(|| turn_state.goal_id.clone());
            if let Ok(Some(projection)) = services.goal_store().projection(&recovery_goal_id) {
                if let Some(intervention) = projection
                    .interventions
                    .iter()
                    .rev()
                    .find(|intervention| {
                        intervention.kind
                            == harness_contract::goal::RuntimeInterventionKind::Replan
                    })
                {
                    let mut item = ContextItem::new(
                        format!("objective-replan:{}:{}", graph.id, graph.revision),
                        ContextSourceKind::Task,
                        ContextRole::Instruction,
                        format!(
                            "Durable Objective recovery proposal: {}. Submit a bounded semantic revision against graph revision {} when recovery is justified; preserve completed evidence and do not fabricate completion.",
                            intervention.reason,
                            intervention.expected_graph_revision.unwrap_or(graph.revision)
                        ),
                    );
                    item.authority = ContextAuthority::System;
                    item.visibility = ContextVisibility::Private;
                    item.evidence = intervention.evidence_refs.clone();
                    turn_state.pending_next_model_context.push(item);
                }
            }
            // Rehydrate the bounded delegated-recovery lease from the durable
            // Goal stream. A graph/node replay must not reset a consumed
            // semantic replan and accidentally reopen an unbounded retry path.
            turn_state.cold_start_failure_replans = services
                .goal_store()
                .projection(&turn_state.goal_id)
                .ok()
                .flatten()
                .map(|projection| {
                    projection
                        .interventions
                        .iter()
                        .filter(|intervention| {
                            intervention.reason.starts_with(COLD_START_TOOL_FAILURE_REPLAN_MARKER)
                        })
                        .count()
                        .min(usize::from(COLD_START_TOOL_FAILURE_REPLAN_BUDGET))
                        as u8
                })
                .unwrap_or_default();

        }
        {
            let runtime = runtime.lock().await;
            runtime.consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::TurnStart);
            if ingress.is_some() {
                runtime.consume_active_runtime_inputs_for_next_step(
                    TurnInputCheckpoint::IngressDispatched,
                );
            }
        }
        let inline_kind = "inline_model".to_string();
        let tool_kind = "tool_batch".to_string();
        for node in &mut graph.nodes {
            node.executor_kind = match node.kind {
                harness_contract::execution_graph::ExecutionNodeKind::InlineModel => {
                    inline_kind.clone()
                }
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch => {
                    tool_kind.clone()
                }
                harness_contract::execution_graph::ExecutionNodeKind::Verify => {
                    if node.executor_kind
                        == crate::execution_core::graph::executors::CompileTargetGuardExecutor::KIND
                    {
                        node.executor_kind.clone()
                    } else {
                        crate::execution_core::graph::executors::VerifyNodeExecutor::KIND
                            .to_string()
                    }
                }
                harness_contract::execution_graph::ExecutionNodeKind::Synthesize => {
                    crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                        .to_string()
                }
                _ => node.executor_kind.clone(),
            };
        }

        let graph_id = graph.id.clone();
        runtime
            .lock()
            .await
            .restore_controlled_recovery_claims_for_turn(&session_id, &turn_ref, &graph_id)?;
        let (mut registered, recovering) = attach_conversation_graph(
            services.as_ref(), graph, continuation_actor, continuation_goal_cut,
            execution_role == TurnExecutionRole::RootTurn && ingress.is_none(),
            || {
            services
                .model_step_executor()
                .install_resolver(Arc::new(TurnModelResolver {
                    session_id: session_id.clone(),
                    graph_id: graph_id.clone(),
                    runtime: Arc::downgrade(&runtime),
                    state: Arc::downgrade(&state),
                    services: Arc::downgrade(&services),
                }));
            services
                .tool_batch_executor()
                .install_resolver(Arc::new(TurnToolResolver {
                    session_id: session_id.clone(),
                    graph_id: graph_id.clone(),
                    runtime: Arc::downgrade(&runtime),
                    state: Arc::downgrade(&state),
                    services: Arc::downgrade(&services),
                }));
            services
                .synthesize_executor()
                .install_resolver(Arc::new(TurnSynthesizeResolver {
                    session_id: session_id.clone(),
                    graph_id: graph_id.clone(),
                    runtime: Arc::downgrade(&runtime),
                    state: Arc::downgrade(&state),
                    services: Arc::downgrade(&services),
                }));
            },
        ).await?;
        if execution_role == TurnExecutionRole::RootTurn && ingress.is_none() {
            let lineage = registered.lineage.as_ref().ok_or_else(|| {
                RuntimeError::new("registered direct root requires lineage")
            })?;
            services.record_live_execution(
                &lineage.session_id, registered.id.clone(), lineage.turn_id.clone(),
            );
            let bus = runtime.lock().await.cowd_bus().cloned().ok_or_else(|| {
                RuntimeError::new("owned root requires its event bus")
            })?;
            (owned_root_scope, owned_root_bus_lease) =
                enter_owned_root_scope(&services, &bus, &registered, None)?;
        }
        if !recovering {
            // Publish the durable graph ID before execution. Surfaces can now
            // attach their cursor stream while model/tool nodes are running;
            // the final summary below remains an update, not the first hint.
            if let Some(bus) = runtime.lock().await.cowd_bus().cloned() {
                let agent_tasks = registered
                    .nodes
                    .iter()
                    .filter(|node| matches!(node.kind, ExecutionNodeKind::AgentTask))
                    .count();
                bus.emit(CowdEvent::ExecutionGraphSummary {
                    summary: crate::RuntimeExecutionGraphSummary {
                        graph_id: Some(registered.id.clone()),
                        board_id: None,
                        status: "running".to_string(),
                        agent_tasks,
                        child_executions: 0,
                        memory_candidates: 0,
                        conflicts: 0,
                        completion_rate: Some(0.0),
                        synthesis_lift: None,
                        complementarity_score: None,
                    },
                });
            }
        }
        let revised_strategy = runtime
            .lock()
            .await
            .active_turn_strategy()
            .ok_or_else(|| {
                RuntimeError::new("strategy owner disappeared after Team admission")
            })?;
        if revised_strategy.decision.compile_target != compile_target {
            if recovering {
                registered = services.graph_state_store().load_async(&graph_id).await
                    .map_err(|error| RuntimeError::new(error.to_string()))?;
            }
            let replacement = compile_retargeted_conversation_graph(
                &registered,
                content,
                &session_id,
                ingress.as_ref(),
                revised_strategy.decision.compile_target,
                &strategy_parent_node_id,
            )?;
            services
                .commit_service()
                .retarget_planned_graph_async(
                    registered.clone(),
                    replacement,
                    format!(
                        "strategy decision {} revision {} downgraded compile target from {:?} to {:?} before parent execution",
                        revised_strategy.decision_id,
                        revised_strategy.revision,
                        compile_target,
                        revised_strategy.decision.compile_target,
                    ),
                )
                .await
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            registered = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|error| RuntimeError::new(error.to_string()))?;
        }
        if let Some(lineage) = registered.lineage.as_ref() {
            services
                .task_runtime_port()
                .link_existing_graph(
                    &lineage.task_id,
                    &registered.id,
                    registered.revision,
                    vec![harness_contract::reality::EvidenceRef::observed(
                        "execution_graph",
                        registered.id.clone(),
                    )],
                )
                .map_err(RuntimeError::new)?;
        }
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=drive_begin");
        }
        let run_result = services.execution_supervisor()
            .drive_registered(&registered.id).await.map(|(_, report)| report);
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=drive_returned ok={}", run_result.is_ok());
        }
        run_result.map_err(|error| RuntimeError::new(error.to_string()))?;
        // Agent-first collaboration parks the root graph on a durable
        // WaitingExternal barrier while child graphs run. Keep the ingress
        // owner and turn backends alive until a resumed Synthesize node
        // commits the terminal summary; returning here used to drop the weak
        // backends, strand the Program, and requeue the same paid request.
        // Progress-based stall bound for the ingress wait loop (see below).
        let turn_stall_checks_bound: u32 = std::env::var("COWD_TURN_STALL_CHECKS")
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(30);
        let mut stall_progress = host_backend::VerifiedCollaborationProgress::default();
        let mut stalled_checks: u32 = 0;
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=settlement_wait_begin");
        }
        loop {
            let notify = {
                let state = state.lock().await;
                if state.summary.is_some() || state.failure.is_some() {
                    break;
                }
                state.terminal_notify.clone()
            };
            let graph = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            let has_program_barrier = graph.nodes.iter().any(|node| {
                node.executor_kind
                    == crate::execution_core::graph::executors::AgenticProgramWaitExecutor::KIND
            });
            let has_non_terminal_work = graph.nodes.iter().any(|node| {
                !graph
                    .node_statuses
                    .get(&node.id)
                    .copied()
                    .unwrap_or(ExecutionNodeStatus::Planned)
                    .is_terminal()
            });
            if !has_program_barrier || !has_non_terminal_work {
                // One assessment on the way out. When the root graph has no live
                // work left, the Program can still be open and unfinished; the
                // existing stall producer answers that with a named non-success
                // conclusion. A missing Program keeps the old break, and the
                // loop's exit condition itself is unchanged.
                if !has_non_terminal_work {
                    let (session_id, turn_id) = {
                        let state = state.lock().await;
                        (state.session_id.clone(), state.turn_id.clone())
                    };
                    if let Some((program, goal)) = host_backend::stable_collaboration_sources(
                        services.as_ref(), &session_id, &turn_id)
                    {
                        if program.objective_verdict.is_none() && program.completion_request.is_none() {
                            let active = crate::execution_core::graph::executors::active_agentic_child_graphs(
                                &graph_id, &program.program_id, &program, services.graph_state_store(),
                            ).await.map_err(RuntimeError::new)?;
                            if active.is_empty()
                                && services.goal_store().program_tasks_all_durably_disconnected(&program)
                                    .map_err(RuntimeError::new)?
                            {
                                let _ = settle_agentic_program_stall(
                                    services.as_ref(), &state, &session_id, &turn_id, &graph_id, &goal, &program,
                                    AgenticProgramStallTrigger::LoopExitWithoutWaitableWork,
                                ).await?;
                            }
                        }
                    }
                }
                break;
            }
            // Program revisions include heartbeats and journal activity. Only
            // accepted work, new verified evidence and closed criteria advance
            // this budget. A live child or a live lease is a legitimate wait.
            let (session_id, turn_id, budget_elapsed) = {
                let state = state.lock().await;
                let obligated = state.collaboration_obligation.as_ref()
                    .is_some_and(|obligation| obligation.minimum_team_count > 0);
                let elapsed = obligated && !state.execution_role.is_delegated_leaf()
                    && collaboration_budget().is_some_and(|budget| state.started_at.elapsed() >= budget);
                (state.session_id.clone(), state.turn_id.clone(), elapsed)
            };
            let Some((program, goal)) = host_backend::stable_collaboration_sources(
                services.as_ref(), &session_id, &turn_id,
            ) else {
                stalled_checks = 0;
                let _ = tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified()).await;
                continue;
            };
            if program.objective_verdict.as_ref().is_some_and(|verdict|
                verdict.kind == harness_contract::goal::ObjectiveTerminalKind::Satisfied)
            {
                crate::execution_core::graph::executors::resolve_agentic_program_wait(
                    &program.program_id, &services.agent_action_service(), services.graph_state_store(),
                    services.execution_supervisor().as_ref(), true,
                ).await.map_err(RuntimeError::new)?;
                let _ = tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified()).await;
                continue;
            }
            if let Some(verdict) = program.objective_verdict.as_ref().filter(|verdict|
                verdict.kind == harness_contract::goal::ObjectiveTerminalKind::Blocked)
            {
                let terminal = goal.goal.terminal.as_ref().filter(|terminal|
                    goal.goal.id == verdict.goal_id && goal.goal.revision == verdict.goal_revision
                        && goal.goal.completion == GoalCompletion::Blocked && terminal.kind == verdict.kind
                        && terminal.terminal_fence == verdict.terminal_fence
                        && terminal.authority_revision == verdict.authority_revision)
                    .ok_or_else(|| RuntimeError::new("blocked Program and Goal verdict disagree"))?;
                state.lock().await.terminal_override = Some((GoalCompletion::Blocked, terminal.reason.clone()));
                crate::execution_core::graph::executors::resolve_agentic_program_wait(
                    &program.program_id, &services.agent_action_service(), services.graph_state_store(),
                    services.execution_supervisor().as_ref(), true,
                ).await.map_err(RuntimeError::new)?;
                let _ = tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified()).await;
                continue;
            }
            let evidence = goal.observations.iter()
                .filter(|observation| observation.has_verified_gain())
                .flat_map(|observation| observation.information_gain.distinguishing_evidence_refs.iter().cloned())
                .filter(|reference| !reference.starts_with("verified_collaboration:"))
                .chain(goal.progress.criteria.iter().filter_map(|(id, status)|
                    matches!(status, AcceptanceStatus::Satisfied | AcceptanceStatus::Waived)
                        .then(|| format!("criterion:{id}:{status:?}"))))
                .chain(goal.goal.obligations.iter().filter_map(|obligation|
                    matches!(obligation.state, harness_contract::goal::ObjectiveObligationState::Satisfied | harness_contract::goal::ObjectiveObligationState::Waived)
                        .then(|| format!("obligation:{}:{:?}", obligation.obligation_id, obligation.state))));
            let advanced = stall_progress.observe(Some(&program), 0, evidence);
            let active = crate::execution_core::graph::executors::active_agentic_child_graphs(
                &graph_id, &program.program_id, &program, services.graph_state_store(),
            ).await.map_err(RuntimeError::new)?;
            let legitimate_wait = !active.is_empty()
                || services.goal_store().exhausted_program_work_cut(&program).map_err(RuntimeError::new)?.is_none();
            if budget_elapsed && legitimate_wait {
                // The existing durable deadline wakes the root to account for
                // outstanding work; a live child or lease is not a failure.
                crate::execution_core::graph::executors::resolve_agentic_program_wait(
                    &program.program_id, &services.agent_action_service(), services.graph_state_store(),
                    services.execution_supervisor().as_ref(), true,
                ).await.map_err(RuntimeError::new)?;
            }
            if advanced || legitimate_wait || program.completion_request.is_some() {
                stalled_checks = 0;
            } else {
                stalled_checks = stalled_checks.saturating_add(1);
            }
            if !legitimate_wait && program.completion_request.is_none()
                && (stalled_checks >= turn_stall_checks_bound || budget_elapsed) {
                match settle_agentic_program_stall(
                    services.as_ref(), &state, &session_id, &turn_id, &graph_id, &goal, &program,
                    AgenticProgramStallTrigger::LoopProgress { stalled_checks, budget_elapsed },
                ).await? {
                    AgenticProgramStallOutcome::Settled => { stalled_checks = 0; }
                    AgenticProgramStallOutcome::NotBlocked => { stalled_checks = 0; continue; }
                    AgenticProgramStallOutcome::Retried => { continue; }
                }
            }
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                notify.notified(),
            )
            .await;
        }
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=settlement_wait_end");
        }
        let mut state = state.lock().await;
        if let Some(error) = state.failure.take() {
            return Err(RuntimeError::new(error));
        }
        let summary = if let Some(summary) = state.summary.take() {
            summary
        } else {
            drop(state);
            let graph = services
                .graph_state_store()
                .load_async(&graph_id)
                .await
                .map_err(|error| RuntimeError::new(error.to_string()))?;
            // A physically observed, claim-authorized cancellation has no
            // synthesized answer. Keep it distinct from missing output due to
            // failure, and require the original committed Graph/effect and
            // Session sources before assigning that terminal class.
            match services.authenticated_provider_graph_cancellation(&graph) {
                Ok(Some(_)) => return Err(RuntimeError::from_authenticated_graph_cancellation()),
                Ok(None) => {}
                Err(error) => return Err(RuntimeError::new(format!("original cancellation verification failed: {error}"))),
            }
            let statuses = graph
                .nodes
                .iter()
                .map(|node| {
                    let failure = graph
                        .node_results
                        .get(&node.id)
                        .and_then(|result| result.failure.as_ref())
                        .map(|failure| format!(":{}", failure.message))
                        .unwrap_or_default();
                    format!(
                        "{}:{}={:?}{failure}",
                        node.id,
                        node.executor_kind,
                        graph
                            .node_statuses
                            .get(&node.id)
                            .copied()
                            .unwrap_or(ExecutionNodeStatus::Planned)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            return Err(RuntimeError::new(format!(
                "execution graph produced no terminal turn result; graph={graph_id}; nodes=[{statuses}]"
            )));
        };
        let projection = services
            .execution_supervisor()
            .projection(&graph_id)
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        // Every ingress turn has a durable execution graph. Publish only its
        // compact identity/health summary on the render bus so surfaces can
        // attach their own cursor-based projection stream without inferring a
        // graph from prose events.
        if let Some(bus) = runtime.lock().await.cowd_bus().cloned() {
            let terminal_nodes = projection
                .nodes
                .iter()
                .filter(|node| node.status.is_terminal())
                .count();
            let failed = projection.nodes.iter().any(|node| {
                matches!(
                    node.status,
                    ExecutionNodeStatus::Failed | ExecutionNodeStatus::Cancelled
                )
            });
            let status = if failed {
                "failed"
            } else if !projection.nodes.is_empty() && terminal_nodes == projection.nodes.len() {
                "terminal"
            } else {
                "running"
            };
            bus.emit(CowdEvent::ExecutionGraphSummary {
                summary: crate::RuntimeExecutionGraphSummary {
                    graph_id: Some(projection.graph_id.clone()),
                    board_id: None,
                    status: status.to_string(),
                    agent_tasks: projection
                        .nodes
                        .iter()
                        .filter(|node| matches!(node.kind, ExecutionNodeKind::AgentTask))
                        .count(),
                    child_executions: 0,
                    memory_candidates: 0,
                    conflicts: 0,
                    completion_rate: (!projection.nodes.is_empty())
                        .then_some(terminal_nodes as f32 / projection.nodes.len() as f32),
                    synthesis_lift: None,
                    complementarity_score: None,
                },
            });
        }
        if projection.terminal_result_ref.is_none() {
            return Err(RuntimeError::new(
                "execution graph completed without a synthesized terminal result",
            ));
        }
        Ok(summary)
    }
    .await;
    {
        let end_to_end_duration_ms = u64::try_from(turn_started_at.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let parent_merge_started_at = parent_merge_started_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied();
        let (parent_merge_cost_ms, parent_merge_count) =
            parent_merge_actuals(parent_merge_started_at, result.is_ok());
        let evaluation_budget = evaluation_control.as_ref().and_then(|control| {
            _evaluation_provider_token_guard
                .as_ref()
                .and_then(|guard| guard.snapshot().ok())
                .filter(|snapshot| {
                    control
                        .budget_lease_id
                        .as_deref()
                        .is_some_and(|lease_id| snapshot.lease_id == lease_id)
                })
        });
        let (status, outcome) = match &result {
            Ok(summary) => (
                match summary.terminal_completion {
                    harness_contract::goal::GoalCompletion::Satisfied => {
                        crate::execution_core::TurnStrategyDecisionStatus::Completed
                    }
                    harness_contract::goal::GoalCompletion::Partial => {
                        crate::execution_core::TurnStrategyDecisionStatus::Partial
                    }
                    harness_contract::goal::GoalCompletion::Blocked
                    | harness_contract::goal::GoalCompletion::Failed => {
                        crate::execution_core::TurnStrategyDecisionStatus::Failed
                    }
                    harness_contract::goal::GoalCompletion::WaitingExternalDecision => {
                        crate::execution_core::TurnStrategyDecisionStatus::WaitingExternalDecision
                    }
                    harness_contract::goal::GoalCompletion::Cancelled => {
                        crate::execution_core::TurnStrategyDecisionStatus::Cancelled
                    }
                    harness_contract::goal::GoalCompletion::Open => {
                        crate::execution_core::TurnStrategyDecisionStatus::Failed
                    }
                },
                crate::execution_core::TurnStrategyActualOutcome {
                    duration_ms: end_to_end_duration_ms,
                    // The evaluation lease is Session-scoped and reconciles
                    // every bound parent, Team child, fallback and judge
                    // provider request. Its typed totals are authoritative
                    // when installed; normal turns retain summary telemetry.
                    input_tokens: evaluation_budget
                        .as_ref()
                        .map_or(summary.model_telemetry.input_tokens, |budget| {
                            budget.input_consumed
                        }),
                    output_tokens: evaluation_budget
                        .as_ref()
                        .map_or(summary.model_telemetry.output_tokens, |budget| {
                            budget.output_consumed
                        }),
                    cached_tokens: evaluation_budget.as_ref().map_or_else(
                        || {
                            summary
                                .model_telemetry
                                .cache_create_tokens
                                .saturating_add(summary.model_telemetry.cache_read_tokens)
                        },
                        |budget| budget.cached_consumed,
                    ),
                    tool_calls: summary.tool_results.len() as u64,
                    failed_tool_calls: summary
                        .tool_results
                        .iter()
                        .flat_map(|message| message.blocks.iter())
                        .filter(|block| {
                            matches!(block, ContentBlock::ToolResult { is_error: true, .. })
                        })
                        .count() as u64,
                    duplicate_tool_calls: summary.duplicate_tool_calls,
                    max_tool_concurrency_observed: u64::try_from(
                        summary.max_tool_concurrency_observed,
                    )
                    .unwrap_or(u64::MAX),
                    parallel_tool_batches: u64::try_from(summary.parallel_tool_batches)
                        .unwrap_or(u64::MAX),
                    write_attempt_paths: summary.write_attempt_paths.clone(),
                    evidence_overlap_bp: 0,
                    evidence_overlap_observed: false,
                    merge_cost_ms: parent_merge_cost_ms,
                    parent_merge_count,
                    evaluation_token_limit: evaluation_budget
                        .as_ref()
                        .map_or(0, |budget| budget.limit),
                    evaluation_tokens_consumed: evaluation_budget
                        .as_ref()
                        .map_or(0, |budget| budget.consumed),
                    evaluation_budget_observed: evaluation_budget
                        .as_ref()
                        .is_some_and(|budget| budget.outstanding == 0),
                    evaluation_budget_breached: evaluation_budget
                        .as_ref()
                        .is_some_and(|budget| budget.breached),
                    quality_score_bp: Some(
                        if summary.ai_kernel_trace.verification_report.can_finalize
                            && summary.ai_kernel_trace.bench_result.passed
                            && summary.ai_kernel_trace.regression_gate.allowed
                        {
                            (summary.ai_kernel_trace.bench_result.score.clamp(0.0, 1.0) * 10_000.0)
                                as u16
                        } else {
                            0
                        },
                    ),
                    actual_speedup_ratio_bp: None,
                    terminal_reason: format!("{:?}", summary.terminal_completion)
                        .to_ascii_lowercase(),
                    // The durable obligation ledger the terminal producer
                    // committed for this turn.  The verified business
                    // conclusion is derived from it together with the
                    // terminal conclusion; the wording above is only the
                    // model's presentation of the turn.
                    obligation_ledger: summary.obligation_ledger.clone(),
                },
            ),
            Err(error) => (
                if error.is_authenticated_graph_cancellation() || error.to_string().contains("cancelled") {
                    crate::execution_core::TurnStrategyDecisionStatus::Cancelled
                } else {
                    crate::execution_core::TurnStrategyDecisionStatus::Failed
                },
                crate::execution_core::TurnStrategyActualOutcome {
                    duration_ms: end_to_end_duration_ms,
                    merge_cost_ms: parent_merge_cost_ms,
                    parent_merge_count: 0,
                    evaluation_token_limit: evaluation_budget
                        .as_ref()
                        .map_or(0, |budget| budget.limit),
                    evaluation_tokens_consumed: evaluation_budget
                        .as_ref()
                        .map_or(0, |budget| budget.consumed),
                    evaluation_budget_observed: evaluation_budget
                        .as_ref()
                        .is_some_and(|budget| budget.outstanding == 0),
                    evaluation_budget_breached: evaluation_budget
                        .as_ref()
                        .is_some_and(|budget| budget.breached),
                    terminal_reason: error.to_string(),
                    ..Default::default()
                },
            ),
        };
        if let Err(error) = runtime
            .lock()
            .await
            .finish_turn_strategy(&turn_ref, status, outcome)
        {
            if result.is_ok() {
                result = Err(error);
            } else {
                tracing::warn!(%error, turn_ref, "failed to record terminal turn strategy outcome");
            }
        }
    }
    let mut runtime = Arc::try_unwrap(runtime)
        .map_err(|_| {
            let reason = "turn executors still hold the conversation runtime; the turn is refused rather than aborting the process";
            match &result {
                Err(error) => error.clone().with_turn_cleanup_failure(reason),
                Ok(_) => RuntimeError::new(reason),
            }
        })?
        .into_inner();
    if let Some(installed) = direct_turn_control {
        if runtime.original_turn_control().is_ok_and(|current| Arc::ptr_eq(&current, &installed)) {
            if let Err(cleanup_error) = runtime.install_provider_cancellation_control(None) {
                result = Err(match result {
                    Ok(_) => cleanup_error,
                    Err(error) => error.with_turn_cleanup_failure(&cleanup_error.to_string()),
                });
            }
        }
    }
    drop(owned_root_scope);
    drop(owned_root_bus_lease);
    Ok((runtime, result))
}

/// Optional objective-level wall-clock cost ceiling for a required-Team
/// collaboration turn. Kept local to the ingress owner so an **enabled**
/// ceiling can still reach an in-bound honest terminal while it waits on child
/// work. Unset or `0` disables it (the default): the wait loop is otherwise
/// bounded by child progress and the turn's own guardrails, never by elapsed
/// time alone.
fn collaboration_budget() -> Option<std::time::Duration> {
    std::env::var("COWD_COLLABORATION_BUDGET_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(std::time::Duration::from_secs)
}

const EVALUATION_TURN_CONTROL_PREFIX: &str = "COWD_EVAL_CONTROL ";

#[derive(Debug, Clone, Deserialize)]
struct EvaluationTurnControl {
    corpus_id: String,
    workspace_fixture: String,
    provider_constraint: String,
    temperature_milli: u16,
    #[serde(default)]
    resource_scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    budget_lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_total_tokens: Option<u64>,
    prompt: String,
}

/// Freeze the capability boundary that a root turn may delegate to future
/// Agents. Ordinary sessions inherit their explicit execution policy within
/// the current workspace; controlled evaluations may narrow that boundary
/// further through their pre-registered scope contract. This is intentionally
/// independent from paths chosen later by the model, so autonomous planning
/// does not lose the ability to create legitimate intermediate artifacts.
fn root_delegation_resource_scopes(
    evaluation: Option<&EvaluationTurnControl>,
    policy: Option<&harness_contract::policy::SessionExecutionPolicy>,
) -> Vec<String> {
    if let Some(evaluation) = evaluation.filter(|control| !control.resource_scopes.is_empty()) {
        let mut scopes = evaluation.resource_scopes.clone();
        scopes.sort();
        scopes.dedup();
        return scopes;
    }
    let mut scopes = match policy.map(|policy| policy.permission_mode) {
        Some(crate::PermissionMode::WorkspaceWrite) => vec!["workspace:.".to_string()],
        Some(crate::PermissionMode::DangerFullAccess) => {
            vec!["workspace:.".to_string(), "network:*".to_string()]
        }
        Some(crate::PermissionMode::ReadOnly) | None => vec!["read:.".to_string()],
    };
    scopes.sort();
    scopes
}

#[cfg(test)]
mod root_delegation_scope_tests {
    use super::*;
    use harness_contract::policy::ApprovalProfile;

    fn policy(
        permission_mode: crate::PermissionMode,
    ) -> harness_contract::policy::SessionExecutionPolicy {
        harness_contract::policy::SessionExecutionPolicy::from_defaults(
            permission_mode,
            ApprovalProfile::Autonomous,
        )
    }

    #[test]
    fn ordinary_agent_delegation_inherits_the_session_workspace_boundary() {
        assert_eq!(
            root_delegation_resource_scopes(None, Some(&policy(crate::PermissionMode::ReadOnly))),
            ["read:."]
        );
        assert_eq!(
            root_delegation_resource_scopes(
                None,
                Some(&policy(crate::PermissionMode::WorkspaceWrite))
            ),
            ["workspace:."]
        );
        assert_eq!(
            root_delegation_resource_scopes(
                None,
                Some(&policy(crate::PermissionMode::DangerFullAccess))
            ),
            ["network:*", "workspace:."]
        );
    }

    #[test]
    fn evaluation_contract_remains_the_narrower_authority() {
        let control = EvaluationTurnControl {
            corpus_id: "live-scenarios-v1".to_string(),
            workspace_fixture: "none".to_string(),
            provider_constraint: "normal".to_string(),
            temperature_milli: 0,
            resource_scopes: vec!["write:report.html".to_string()],
            budget_lease_id: None,
            max_total_tokens: None,
            prompt: "write report".to_string(),
        };
        assert_eq!(
            root_delegation_resource_scopes(
                Some(&control),
                Some(&policy(crate::PermissionMode::DangerFullAccess))
            ),
            ["write:report.html"]
        );
    }
}

fn evaluation_turn_control(content: &str) -> Result<Option<EvaluationTurnControl>, RuntimeError> {
    let Some((line, prompt)) = content.split_once('\n') else {
        return Ok(None);
    };
    let Some(encoded) = line.strip_prefix(EVALUATION_TURN_CONTROL_PREFIX) else {
        return Ok(None);
    };
    let configured_corpus = std::env::var("COWD_EVAL_CORPUS_ID").ok();
    if std::env::var("COWD_EVAL_HARNESS").as_deref() != Ok("1") {
        return Ok(None);
    }
    let mut control = serde_json::from_str::<EvaluationTurnControl>(encoded)
        .map_err(|error| RuntimeError::new(format!("invalid evaluation turn control: {error}")))?;
    if harness_contract::evaluation::evaluation_fixture_profile(&control.corpus_id).is_none()
        || configured_corpus.as_deref() != Some(control.corpus_id.as_str())
        || prompt.trim().is_empty()
    {
        return Err(RuntimeError::new(
            "evaluation turn control corpus or prompt is invalid",
        ));
    }
    match (&control.budget_lease_id, control.max_total_tokens) {
        (None, None) => {}
        (Some(lease_id), Some(limit)) if !lease_id.trim().is_empty() && limit > 0 => {}
        _ => {
            return Err(RuntimeError::new(
                "evaluation provider token telemetry lease must provide both a non-empty id and a positive limit",
            ));
        }
    }
    if control.temperature_milli != 0
        || std::env::var("COWD_MODEL_TEMPERATURE").as_deref() != Ok("0")
    {
        return Err(RuntimeError::new(
            "evaluation temperature is not the frozen zero-temperature provider request",
        ));
    }
    if control.workspace_fixture != "none"
        && std::env::var("COWD_EVAL_WORKSPACE_FIXTURE").ok().as_deref()
            != Some(control.workspace_fixture.as_str())
    {
        return Err(RuntimeError::new(format!(
            "evaluation workspace fixture `{}` is not the frozen server fixture",
            control.workspace_fixture
        )));
    }
    control.prompt = prompt.to_string();
    Ok(Some(control))
}

#[cfg(test)]
mod evaluation_control_corpus_tests {
    use super::*;

    #[test]
    #[ignore = "requires immutable envelopes exported by the actual Harness producer exact test"]
    fn original_harness_selector_envelopes_reach_runtime_parser() {
        let _lock = crate::test_env_lock();
        struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
        impl Drop for Restore {
            fn drop(&mut self) {
                for (key, value) in &self.0 {
                    if let Some(value) = value {
                        std::env::set_var(key, value);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
        let _restore = Restore(
            [
                "COWD_EVAL_HARNESS",
                "COWD_EVAL_CORPUS_ID",
                "COWD_MODEL_TEMPERATURE",
            ]
            .into_iter()
            .map(|key| (key, std::env::var_os(key)))
            .collect(),
        );
        let path = std::env::var_os("COWD_TEST_HARNESS_CONTROL_INPUT")
            .expect("actual Harness producer envelope file");
        let raw = std::fs::read(path).unwrap();
        assert!(raw.len() < 16384);
        let originals: Vec<serde_json::Value> = serde_json::from_slice(&raw).unwrap();
        assert_eq!(originals.len(), 13);
        std::env::set_var("COWD_EVAL_HARNESS", "1");
        std::env::set_var("COWD_MODEL_TEMPERATURE", "0");
        for (selector, corpus) in [
            ("live_group_theory_ai_research_simulation", "live-group-theory-v1"),
            ("live_explicit_team_requirement", "live-three-domain-team-v1"),
            ("live_team_projection", "live-three-domain-team-v1"),
            ("live_agent_escalation", "live-agent-escalation-v1"),
            ("live_qwen38_large_scale_collaboration", "live-large-scale-v1"),
            ("live_one_team_execution", "live-one-team-v1"),
            ("live_small_two_team_collaboration", "live-small-team-v1"),
            ("live_direct_terminal", "live-scenarios-v1"),
            ("live_tool_evidence", "live-scenarios-v1"),
            ("live_minimal_batch_read", "live-batchread-v1"),
            ("u5-read-a", "live-u5-fairness-v1"),
            ("u5-read-b", "live-u5-fairness-v1"),
            ("live_u7_persistent_memory_recall", "live-scenarios-v1"),
        ] {
            let case = originals
                .iter()
                .find(|case| case["selector"] == selector)
                .unwrap();
            let content = case["content"].as_str().unwrap();
            std::env::set_var("COWD_EVAL_CORPUS_ID", corpus);
            let control = evaluation_turn_control(content)
                .unwrap()
                .expect("actual registered producer must be admitted");
            assert_eq!(control.corpus_id, corpus);
            assert_eq!(control.prompt, "original controlled input");
            assert_eq!(control.provider_constraint, "normal");
            assert_eq!(control.temperature_milli, 0);
            if let Some(read) = match selector {
                "u5-read-a" => Some("read:Cargo.toml"),
                "u5-read-b" => Some("read:crates/runtime/Cargo.toml"),
                _ => None,
            } {
                assert_eq!(
                    control.resource_scopes,
                    ["provider", "provider_account", "provider_token_pool", read]
                );
                // A known but wrong configured corpus is not an alias.
                std::env::set_var("COWD_EVAL_CORPUS_ID", "live-scenarios-v1");
                assert!(evaluation_turn_control(content)
                    .unwrap_err()
                    .to_string()
                    .contains("corpus or prompt"));
                std::env::set_var("COWD_EVAL_CORPUS_ID", corpus);
            }
            if !matches!(selector, "live_one_team_execution" | "live_direct_terminal" | "live_tool_evidence" | "u5-read-a" | "u5-read-b" | "live_u7_persistent_memory_recall") {
                assert_eq!(
                    control.resource_scopes,
                    ["provider", "provider_account", "provider_token_pool", "read:."]
                );
                // The old generic corpus is not an alias for the fixed six-file scene.
                std::env::set_var("COWD_EVAL_CORPUS_ID", "live-scenarios-v1");
                assert!(evaluation_turn_control(content)
                    .unwrap_err()
                    .to_string()
                    .contains("corpus or prompt"));
                std::env::set_var("COWD_EVAL_CORPUS_ID", corpus);
            }
            let (line, prompt) = content.split_once('\n').unwrap();
            let original: serde_json::Value =
                serde_json::from_str(line.strip_prefix(EVALUATION_TURN_CONTROL_PREFIX).unwrap())
                    .unwrap();
            let envelope = |control: &serde_json::Value, prompt: &str| {
                format!("{EVALUATION_TURN_CONTROL_PREFIX}{control}\n{prompt}")
            };
            // Legal existing auto corpus keeps its unchanged parser contract.
            let mut auto = original.clone();
            auto["corpus_id"] = serde_json::json!("auto-strategy-v1");
            std::env::set_var("COWD_EVAL_CORPUS_ID", "auto-strategy-v1");
            assert_eq!(
                evaluation_turn_control(&envelope(&auto, prompt))
                    .unwrap()
                    .unwrap()
                    .corpus_id,
                "auto-strategy-v1"
            );
            // Equality and the fixed registry are separate guards.
            std::env::set_var("COWD_EVAL_CORPUS_ID", "foreign");
            assert!(evaluation_turn_control(content)
                .unwrap_err()
                .to_string()
                .contains("corpus or prompt"));
            let mut unknown = original.clone();
            unknown["corpus_id"] = serde_json::json!("foreign");
            assert!(evaluation_turn_control(&envelope(&unknown, prompt))
                .unwrap_err()
                .to_string()
                .contains("corpus or prompt"));
            std::env::set_var("COWD_EVAL_CORPUS_ID", corpus);
            assert!(evaluation_turn_control(&envelope(&original, " "))
                .unwrap_err()
                .to_string()
                .contains("corpus or prompt"));
            std::env::remove_var("COWD_EVAL_CORPUS_ID");
            assert!(evaluation_turn_control(content).is_err());
            std::env::set_var("COWD_EVAL_CORPUS_ID", corpus);
            let mut changed = original.clone();
            changed["temperature_milli"] = serde_json::json!(1);
            assert!(evaluation_turn_control(&envelope(&changed, prompt))
                .unwrap_err()
                .to_string()
                .contains("temperature"));
            std::env::set_var("COWD_MODEL_TEMPERATURE", "1");
            assert!(evaluation_turn_control(content)
                .unwrap_err()
                .to_string()
                .contains("temperature"));
            std::env::set_var("COWD_MODEL_TEMPERATURE", "0");
            for (lease, limit) in [
                (Some("lease"), None),
                (None, Some(1)),
                (Some(""), Some(1)),
                (Some("lease"), Some(0)),
            ] {
                let mut invalid = original.clone();
                if let Some(lease) = lease {
                    invalid["budget_lease_id"] = serde_json::json!(lease);
                }
                if let Some(limit) = limit {
                    invalid["max_total_tokens"] = serde_json::json!(limit);
                }
                assert!(evaluation_turn_control(&envelope(&invalid, prompt))
                    .unwrap_err()
                    .to_string()
                    .contains("telemetry lease"));
            }
            let mut telemetry = original.clone();
            telemetry["budget_lease_id"] = serde_json::json!("original-lease");
            telemetry["max_total_tokens"] = serde_json::json!(100);
            assert_eq!(
                evaluation_turn_control(&envelope(&telemetry, prompt))
                    .unwrap()
                    .unwrap()
                    .max_total_tokens,
                Some(100)
            );
            std::env::remove_var("COWD_EVAL_HARNESS");
            assert!(evaluation_turn_control(content).unwrap().is_none());
            std::env::set_var("COWD_EVAL_HARNESS", "1");
        }
        println!("ACTUAL_HARNESS_TO_RUNTIME_CORPUS_GATE: six original selector envelopes consumed; registry/equality/prompt/temperature/telemetry negatives hit");
    }
}

struct EvaluationResourceQuotaGuard {
    manager: Arc<crate::execution_core::graph::ExecutionResourceManager>,
    previous: Vec<(
        crate::execution_core::graph::ExecutionResourceKind,
        crate::execution_core::graph::ResourceQuota,
    )>,
}

impl EvaluationResourceQuotaGuard {
    fn apply(
        services: &crate::RuntimeServices,
        control: &EvaluationTurnControl,
    ) -> Result<Self, RuntimeError> {
        use crate::execution_core::graph::{ExecutionResourceKind, ResourceQuota};
        let manager = Arc::clone(services.resource_manager());
        let mut guard = Self {
            manager,
            previous: Vec::new(),
        };
        if matches!(control.provider_constraint.as_str(), "normal" | "judge") {
            return Ok(guard);
        }
        for assignment in control.provider_constraint.split(',') {
            let (name, value) = assignment.trim().split_once('=').ok_or_else(|| {
                RuntimeError::new(format!(
                    "invalid evaluation resource constraint `{assignment}`"
                ))
            })?;
            let limit = value
                .parse::<usize>()
                .ok()
                .filter(|value| (1..=64).contains(value))
                .ok_or_else(|| {
                    RuntimeError::new(format!(
                        "evaluation resource constraint `{assignment}` must be within 1..=64"
                    ))
                })?;
            let kind = match name {
                "provider_concurrency" => ExecutionResourceKind::Provider,
                "tool_concurrency" => ExecutionResourceKind::Tool,
                "team_slots" => ExecutionResourceKind::Agent,
                _ => {
                    return Err(RuntimeError::new(format!(
                        "unknown evaluation resource constraint `{name}`"
                    )));
                }
            };
            let snapshot = guard.manager.snapshot(&kind).map_err(|error| {
                RuntimeError::new(format!("snapshot evaluation resource {kind:?}: {error}"))
            })?;
            if !guard.previous.iter().any(|(previous, _)| previous == &kind) {
                guard.previous.push((
                    kind.clone(),
                    ResourceQuota::new(snapshot.minimum, snapshot.target, snapshot.maximum)
                        .map_err(|error| RuntimeError::new(error.to_string()))?,
                ));
            }
            guard
                .manager
                .update_quota(
                    &kind,
                    ResourceQuota::new(1, limit, limit)
                        .map_err(|error| RuntimeError::new(error.to_string()))?,
                )
                .map_err(|error| {
                    RuntimeError::new(format!(
                        "apply evaluation resource constraint `{assignment}`: {error}"
                    ))
                })?;
        }
        Ok(guard)
    }
}

impl Drop for EvaluationResourceQuotaGuard {
    fn drop(&mut self) {
        for (kind, quota) in self.previous.iter().rev() {
            if let Err(error) = self.manager.update_quota(kind, *quota) {
                tracing::error!(
                    ?kind,
                    %error,
                    "failed to restore preregistered evaluation resource quota"
                );
            }
        }
    }
}

fn turn_strategy_resource_snapshot(
    services: &crate::RuntimeServices,
    evaluation: Option<&EvaluationTurnControl>,
    provider_profile_fingerprint: String,
) -> Result<harness_contract::strategy::StrategyResourceSnapshot, RuntimeError> {
    use crate::execution_core::graph::ExecutionResourceKind;

    let snapshot = |kind| {
        services
            .resource_manager()
            .snapshot(&kind)
            .map_err(|error| RuntimeError::new(format!("read {kind:?} resource snapshot: {error}")))
    };
    let provider = snapshot(ExecutionResourceKind::Provider)?;
    let tool = snapshot(ExecutionResourceKind::Tool)?;
    let agent = snapshot(ExecutionResourceKind::Agent)?;
    let available = |value: &crate::execution_core::graph::ExecutionResourceSnapshot| {
        value.effective_limit.saturating_sub(value.active_leases)
    };
    let provider_available = available(&provider);
    let tool_available = available(&tool);
    let agent_available = available(&agent);
    // Agent/Team nodes do not consume Tool capacity. Only ParallelTools work
    // is bounded by the Tool resource family. Keeping Team slots dependent on
    // Tool allowed a transient Tool failure-upper-bound reduction to collapse
    // every multi-agent proposal to serial execution.
    let team_slots = collaboration_team_slots(provider_available, agent_available);
    let queue_saturation = if provider.effective_limit == 0 {
        10_000
    } else {
        provider
            .queued_waiters
            .saturating_mul(10_000)
            .saturating_div(provider.effective_limit)
            .min(10_000)
    };
    let queue_service_penalty = if provider.service_time.p95_ms == 0 {
        0
    } else {
        provider
            .queue_wait
            .p95_ms
            .saturating_mul(10_000)
            .saturating_div(provider.service_time.p95_ms)
            .min(10_000) as usize
    };
    let provider_penalty = queue_saturation
        .max(queue_service_penalty)
        .max(
            provider
                .failure_timeout_upper_bound_basis_points
                .unwrap_or_default()
                .into(),
        )
        .max(
            provider
                .overload_rate_basis_points
                .unwrap_or_default()
                .into(),
        );
    let observed = provider.sample_count > 0
        && provider.freshness == crate::execution_core::graph::ResourceObservationFreshness::Fresh;
    Ok(harness_contract::strategy::StrategyResourceSnapshot {
        version: if evaluation.is_some() {
            "runtime-resource-manager-v2+preregistered-eval".to_string()
        } else {
            "runtime-resource-manager-v2".to_string()
        },
        provider_available: provider_available > 0,
        tools_available: tool_available > 0,
        team_available: team_slots >= 2,
        provider_concurrency: u16::try_from(provider_available).unwrap_or(u16::MAX),
        tool_concurrency: u16::try_from(tool_available).unwrap_or(u16::MAX),
        team_slots: u16::try_from(team_slots).unwrap_or(u16::MAX),
        provider_concurrency_penalty_bp: u16::try_from(provider_penalty).unwrap_or(10_000),
        provider_effective_limit: u16::try_from(provider.effective_limit).unwrap_or(u16::MAX),
        provider_queue_p95_ms: provider.queue_wait.p95_ms,
        provider_service_p95_ms: provider.service_time.p95_ms,
        provider_failure_timeout_upper_bound_bp: provider
            .failure_timeout_upper_bound_basis_points
            .unwrap_or_default(),
        provider_profile_fingerprint,
        sample_source: evaluation.map_or_else(
            || "runtime-execution-resource-manager".to_string(),
            |control| {
                format!(
                    "runtime-execution-resource-manager:corpus={}:workspace_fixture={}:provider_constraint={}:temperature_milli={}",
                    control.corpus_id,
                    control.workspace_fixture,
                    control.provider_constraint,
                    control.temperature_milli,
                )
            },
        ),
        sample_count: u32::try_from(provider.sample_count).unwrap_or(u32::MAX),
        provenance: if observed {
            harness_contract::core::MeasureProvenance::Observed
        } else {
            harness_contract::core::MeasureProvenance::Assumed
        },
    })
}

fn collaboration_team_slots(provider_available: usize, agent_available: usize) -> usize {
    provider_available.min(agent_available)
}

#[cfg(test)]
fn selected_team_failure_must_block_parent_replay(child_executed: bool) -> bool {
    child_executed
}

const COLLABORATION_EVIDENCE_CARRIER_KIND: &str = "cowd.runtime.collaboration_evidence.v1";

fn is_collaboration_evidence_carrier(value: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|value| {
            value
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some(COLLABORATION_EVIDENCE_CARRIER_KIND)
}

fn collaboration_carrier_results(value: &str) -> Option<Vec<String>> {
    let carrier = serde_json::from_str::<serde_json::Value>(value).ok()?;
    (carrier.get("kind").and_then(serde_json::Value::as_str)
        == Some(COLLABORATION_EVIDENCE_CARRIER_KIND))
    .then(|| {
        carrier
            .get("team_results")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>()
    })
}

/// Partition only between complete semantic results. The target controls
/// provider routing, never result retention: no result string is sliced and an
/// oversized single Team remains intact for explicit provider preflight.
fn partition_complete_collaboration_results(
    results: Vec<String>,
    target_chars: usize,
) -> Vec<Vec<String>> {
    let mut partitions = Vec::new();
    let mut current = Vec::new();
    let mut current_chars = 0_usize;
    for result in results {
        let result_chars = result.chars().count();
        if !current.is_empty() && current_chars.saturating_add(result_chars) > target_chars {
            partitions.push(std::mem::take(&mut current));
            current_chars = 0;
        }
        current_chars = current_chars.saturating_add(result_chars);
        current.push(result);
    }
    if !current.is_empty() {
        partitions.push(current);
    }
    partitions
}

/// Build the carrier that hands a Program's delivered results to the root for
/// final synthesis. `pub(crate)` so the verified-answer drive can use it: an
/// Objective satisfied by its own closing action leaves the root without the
/// turn that would write the user-visible answer, and this carrier is what the
/// terminal narrator synthesizes from.
pub(crate) fn collaboration_synthesis_layer(results: Vec<String>, level: usize) -> String {
    serde_json::json!({
        "kind": "cowd.runtime.collaboration_synthesis_layer.v1",
        "level": level,
        "result_count": results.len(),
        "results": results,
        "presentation_contract": "further_root_synthesis_required"
    })
    .to_string()
}

fn completed_orchestration_terminal_summary(
    _calls: &[ModelToolCall],
    _messages: &[ConversationMessage],
    _workspace_root: &std::path::Path,
    _require_source_path_evidence: bool,
) -> Option<String> {
    // Agent-first completion is projected from Program events and enforced by
    // ObjectiveSupervisor. A tool response can never manufacture terminality.
    None
}

#[cfg(test)]
fn team_phase_satisfies_parent_goal(
    required_team_executions: usize,
    parent_requires_write: bool,
    parent_write_satisfied: bool,
    verified_team_executions: usize,
) -> bool {
    (!parent_requires_write || parent_write_satisfied)
        && verified_team_executions >= required_team_executions.max(1)
}

fn current_turn_required_team_execution_count(
    goal: &GoalContract,
    obligation: Option<&harness_contract::strategy::CollaborationExecutionObligation>,
    delegated_agent_role: bool,
    evaluation_judge_only: bool,
) -> usize {
    if delegated_agent_role || evaluation_judge_only {
        return 0;
    }
    crate::execution_core::goal::current_goal_team_bounds(goal)
        .map(|(minimum, _)| usize::from(minimum))
        .unwrap_or_else(|| {
            required_team_execution_count_for_execution_context(obligation, false, false)
        })
}

fn required_team_execution_count_for_execution_context(
    obligation: Option<&harness_contract::strategy::CollaborationExecutionObligation>,
    delegated_agent_role: bool,
    evaluation_judge_only: bool,
) -> usize {
    // The evaluator's blind Judge receives candidate *outputs* as quoted
    // evidence.  Those outputs can naturally mention Team work, but quoted
    // text must never turn the isolated Judge into a new Team obligation.  A
    // Judge is deliberately a single, no-tool model admission whose JSON is
    // checked by the harness; production turns retain their typed Team
    // contract unchanged.
    if delegated_agent_role || evaluation_judge_only {
        0
    } else {
        obligation.map_or(0, |value| usize::from(value.required_team_count()))
    }
}

/// Framing context for the current turn: where the Program stands, what is
/// still outstanding, and what this turn has cost. Rendered into
/// `#GoalState` on every provider request so the model never has to infer its
/// own position from an opaque projection.
///
/// Pure: derived only from durable Program facts. The framework does NOT parse
/// the model's own summary, so the Program projection stays the single source of
/// truth.
/// The instances the model may legally cite right now.
///
/// Every contract that needs an instance states where to fetch it
/// ("returned by state_inspect", "from task_submit", "a real read_file return
/// value in this session") and none of them state what it currently is. That
/// forces fetch -> parse -> remember on every call, and the remember step fails
/// in a context that grows past 100k tokens: one live run re-read the same file
/// sixteen times because it could no longer find the ref it had already been
/// handed, and another published a sha256 that was the digest of the empty
/// string. These are facts the framework already holds; projecting them costs
/// nothing and removes the guessing.
///
/// This states what exists, never what to choose. Which ref to cite stays the
/// model's decision.
fn citable_refs(
    program: &crate::AgenticProgramProjection,
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
) -> serde_json::Value {
    let goal_snapshot = bound_goal_context(program, services, session_id, turn_id);
    let criteria = goal_snapshot["criteria"].clone();
    let accepted_results = program
        .tasks
        .values()
        .filter(|task| task.status == crate::AgenticTaskStatus::Accepted)
        .flat_map(|task| task.artifact_refs.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let accepted_total = accepted_results.len();
    let accepted_results = accepted_results.into_iter().take(32).collect::<Vec<_>>();
    let artifact_total = program.artifacts.len();
    let artifacts = program
        .artifacts
        .values()
        .take(32)
        .map(|artifact| {
            serde_json::json!({
                "artifact_ref": artifact.artifact_ref,
                "content_ref": artifact.content_ref,
                "kind": artifact.kind,
                "title": artifact.title,
                "committed_by": artifact.committed_by,
            })
        })
        .collect::<Vec<_>>();
    let topic_refs = program
        .teams
        .values()
        .map(|team| team.topic_ref.clone())
        .filter(|reference| !reference.is_empty())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let active_completion_results = program
        .completion_request
        .as_ref()
        .map(|request| request.result_refs.clone())
        .unwrap_or_default();
    let mut value = serde_json::json!({
        "criteria": criteria,
        "topic_refs": topic_refs,
        "accepted_results": accepted_results,
        "artifacts": artifacts,
        "active_completion_results": active_completion_results,
        "goal": goal_snapshot,
        "coverage": {
            "program_revision": program.revision,
            "accepted_results_total": accepted_total,
            "accepted_results_omitted": accepted_total.saturating_sub(32),
            "artifacts_total": artifact_total,
            "artifacts_omitted": artifact_total.saturating_sub(32),
            "read_request": {"name":"state_inspect","input":{"entry_ref":program.program_id}},
        },
        "note": "These are registered Program refs, not all filesystem facts. File hashes are not content selectors. Authenticated tool receipts remain evidence, not registered deliverables. Empty means no entries in this snapshot only; unknown Goal data is explicitly labelled. Use returned refs, never construct selectors.",
    });
    // Never truncate silently: an unannounced omission is indistinguishable
    // from "there is nothing more".
    if artifact_total > 32 {
        value["artifacts_more"] = serde_json::json!(artifact_total - 32);
    }
    value
}

fn bound_goal_context(
    program: &crate::AgenticProgramProjection,
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
) -> serde_json::Value {
    if program.session_id != session_id || program.turn_id != turn_id {
        return serde_json::json!({"status":"binding_mismatch"});
    }
    match crate::execution_core::goal::assessment::read_bound_goal_assessment(
        services.event_store(),
        program,
    ) {
        Ok(assessment) => assessment.goal_context(program),
        Err(error) => serde_json::json!({"status":"unavailable","reason":error.to_string()}),
    }
}

#[must_use]
pub(super) fn agentic_turn_framing(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
    iterations: usize,
    tool_calls: usize,
    elapsed_ms: u128,
) -> serde_json::Value {
    let budgets = serde_json::json!({
        "rounds": iterations,
        "tool_calls": tool_calls,
        "elapsed_ms": elapsed_ms,
    });
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    match services.agent_action_service().project_if_exists(&program_id) {
        Ok(Some(program)) => {
            let assessment = crate::execution_core::goal::assessment::read_bound_goal_assessment(
                services.event_store(),
                &program,
            );
            let required_teams = match &assessment {
                Ok(crate::execution_core::goal::assessment::BoundGoalAssessment::Bound {
                    goal,
                    ..
                }) => Some(
                    crate::execution_core::goal::current_goal_team_bounds(&goal.goal)
                        .map_or(program.required_team_count, |(minimum, _)| minimum),
                ),
                Ok(crate::execution_core::goal::assessment::BoundGoalAssessment::NotApplicable) => {
                    Some(program.required_team_count)
                }
                Err(_) => None,
            };
            let mut view = match assessment {
                Ok(assessment) => assessment.view(&program),
                Err(error) => crate::execution_core::goal::assessment::unknown_completion_view(
                    &program,
                    &error.to_string(),
                ),
            };
            view["program_source"] = serde_json::json!({"program_ref":program.program_id,"revision":program.revision});
            if let Some(object) = view.as_object_mut() {
                object.insert("budgets".to_string(), budgets);
                object.insert(
                    "citable_refs".to_string(),
                    citable_refs(&program, services, session_id, turn_id),
                );
                // Live occupancy: the model needs to know how much is already
                // running before it decides to spawn or wait. Reference
                // harnesses state the available concurrency slots; we state the
                // observed occupancy, which is the part we can measure durably.
                object.insert(
                    "capacity".to_string(),
                    serde_json::json!({
                        "required_teams": required_teams,
                        "teams": program.teams.len(),
                        "agents": program.agents.len(),
                        "active_teams": program
                            .teams
                            .values()
                            .filter(|team| {
                                serde_json::to_value(team.lifecycle).ok()
                                    == Some(serde_json::json!("active"))
                            })
                            .count(),
                        "note": "Root may perform independent work outside delegated scopes. Do not duplicate in-flight delegated work. Occupancy is observation, not a mandatory collaboration plan.",
                    }),
                );
            }
            view
        }
        Ok(None) => serde_json::json!({
            "program_source":{"status":"absent","program_ref":program_id},
            "current":"uninitialized",
            "exit_ready":false,
            "remaining":[],
            "satisfied":[],
            "suggestions":["Perform the user task using governed tools. Absence of a Program does not require a Team or Task and does not certify completion. For a real file deliverable, tool_search(query=artifact_publish) discovers artifact_publish; publish source=file with actual path, sha256 and media_type, then artifact_commit its returned content_ref with kind/title. For actual response Text use artifact_commit(content_ref=message://current,kind,title). No guessed selector or duplicate deliverable."],
            "budgets":budgets,
        }),
        Err(error) => serde_json::json!({
            "program_source":{"status":"unavailable","program_ref":program_id},
            "current":"blocked",
            "exit_ready":false,
            "remaining":[],
            "blocked":{"reason":"program_projection_failed","detail":error.to_string()},
            "budgets":budgets,
        }),
    }
}

/// Keep the complete Program view available to a delegated leaf while making
/// ownership explicit. Root closure facts are useful context, but their
/// `current`/`remaining` fields must never masquerade as the leaf's next action.
pub(super) fn scope_turn_framing_for_role(
    mut framing: serde_json::Value,
    role: TurnExecutionRole,
) -> serde_json::Value {
    if !role.is_delegated_leaf() {
        return framing;
    }
    let mut root_closure = framing.as_object_mut()
        .and_then(|object| object.remove("closure"))
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(object) = framing.as_object_mut() {
        for key in ["current", "exit_ready", "remaining", "satisfied", "completion_gap", "blocked", "suggestions"] {
            if let Some(value) = object.remove(key) {
                root_closure[key] = value;
            }
        }
    }
    if root_closure.as_object().is_some_and(|object| !object.is_empty()) {
        framing["program_progress"] = serde_json::json!({
            "owner": "RootTurn",
            "root_closure": root_closure,
            "role_note": "This Program-wide closure state is read-only background. Complete the bound Task in your private role brief; the root coordinator owns Program-level closure.",
        });
    }
    if let Some(capacity) = framing
        .get_mut("capacity")
        .and_then(serde_json::Value::as_object_mut)
    {
        capacity.insert(
            "note".to_string(),
            serde_json::json!("Program-wide occupancy is background context. Execute only the Runtime-attested bound Task; the root coordinator owns coordination and Program-level closure."),
        );
    }
    framing["execution_role"] = serde_json::json!("DelegatedLeaf");
    framing
}

#[cfg(test)]
use crate::execution_core::goal::assessment::completion_view as agentic_closure_view;
use crate::execution_core::goal::assessment::CompletionTerminalAction as RootAgenticTerminalAction;

pub(super) fn outstanding_closure_steps(program: &crate::AgenticProgramProjection) -> Vec<String> {
    crate::execution_core::goal::assessment::CompletionAssessment::for_program(program, None)
        .remaining_names()
        .into_iter()
        .map(str::to_string)
        .collect()
}

pub(super) fn root_agentic_program_projection(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
) -> Option<crate::AgenticProgramProjection> {
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    services.agent_action_service().project(&program_id).ok()
}

impl RootAgenticTerminalAction {
    fn checkpoint_next_actions(&self) -> Vec<&'static str> {
        match self {
            Self::RegisterDelivery { .. } => vec!["register_delivery"],
            Self::RequestObjectiveCompletion { .. } => vec!["objective_complete_request"],
        }
    }

    fn checkpoint_detail(&self) -> serde_json::Value {
        match self {
            Self::RegisterDelivery {
                prerequisite_task_refs,
            } => serde_json::json!({
                "kind": "register_delivery",
                "prerequisite_task_refs": prerequisite_task_refs.iter().take(32).collect::<Vec<_>>(),
                "total_prerequisites":prerequisite_task_refs.len(),
                "directory_request":{"name":"state_inspect","input":{}},
                "requirement": "Register the actual result and its authenticated delivery lineage. Follow the original Goal requirements; accepted Task counts do not require an additional synthesis Task.",
            }),
            Self::RequestObjectiveCompletion {
                result_artifact_refs,
            } => serde_json::json!({
                "kind": "objective_complete_request",
                "eligible_result_artifact_refs": result_artifact_refs.iter().take(32).collect::<Vec<_>>(),
                "total_eligible_results":result_artifact_refs.len(),
                "directory_request":{"name":"state_inspect","input":{}},
                "requirement": "When the original objective is supported, request Objective completion using an eligible accepted result and real evidence. Continue inspecting sources and resolving or disclosing issues when needed. Accepted tasks alone do not certify the original objective.",
            }),
        }
    }
}

/// Decide whether the Agent-first Objective supervisor exclusively owns the
/// root Goal terminal. Presence alone is not sufficient: the durable Program
/// must attest the exact Session, Turn, Objective, and root execution. A
/// malformed partial binding fails closed instead of falling through to the
/// generic presentation writer and creating a conflicting terminal fence.
fn agentic_program_owns_root_terminal(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
    root_execution_id: &str,
) -> Result<bool, String> {
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let Some(program) = services
        .agent_action_service()
        .project_if_exists(&program_id)
        .map_err(|error| format!("load Agent-first terminal owner: {error}"))?
    else {
        return Ok(false);
    };
    if program.objective_id != objective_id
        || program.session_id != session_id
        || program.turn_id != turn_id
        || program.root_execution_id.as_deref() != Some(root_execution_id)
    {
        return Err(format!(
            "Agent-first Program `{program_id}` has an invalid root terminal binding"
        ));
    }
    Ok(true)
}

fn retain_agentic_program_checkpoint(
    context: &mut Vec<ContextItem>,
    program: &crate::AgenticProgramProjection,
    checkpoint: String,
) {
    let id = format!("agentic-program-checkpoint:{}", program.program_id);
    let mut item = ContextItem::new(
        id.clone(),
        ContextSourceKind::Task,
        ContextRole::TaskState,
        checkpoint,
    );
    item.authority = ContextAuthority::Tool;
    item.visibility = ContextVisibility::Private;
    item.source_version = Some(program.revision.to_string());
    item.evidence = vec![format!("program_revision:{}", program.revision)];
    // Current task truth must survive subsequent model/tool steps, even when
    // unrelated orientation context already meets the allocator's coverage
    // threshold. Replace the projection in place; do not append its history.
    if let Some(previous) = context.iter_mut().find(|previous| previous.id == id) {
        *previous = item;
    } else {
        context.push(item);
    }
}

fn compact_agentic_program_checkpoint(
    services: Option<&crate::RuntimeServices>,
    program: &crate::AgenticProgramProjection,
) -> String {
    // Window size bounds materialization, never the authoritative directory.
    const METADATA_WINDOW: usize = 32;
    let goal_assessment = services.map(|services| {
        crate::execution_core::goal::assessment::read_bound_goal_assessment(
            services.event_store(),
            program,
        )
    });
    let terminal_action = goal_assessment.as_ref().and_then(|result| match result {
        Ok(assessment) => assessment.terminal_action(program),
        Err(_) => None,
    });
    let preview_ref =
        program
            .final_artifact_ref
            .as_deref()
            .or_else(|| match terminal_action.as_ref() {
                Some(RootAgenticTerminalAction::RequestObjectiveCompletion {
                    result_artifact_refs,
                }) => result_artifact_refs.first().map(String::as_str),
                _ => program
                    .tasks
                    .values()
                    .filter(|task| task.status == crate::AgenticTaskStatus::Submitted)
                    .find_map(|task| task.artifact_refs.first().map(String::as_str)),
            });
    let mut visible_tasks = program.tasks.values().collect::<Vec<_>>();
    visible_tasks.sort_by_key(|task| {
        (
            matches!(
                task.status,
                crate::AgenticTaskStatus::Accepted | crate::AgenticTaskStatus::Superseded
            ),
            task.unresolved.is_empty(),
            &task.task_id,
        )
    });
    let mut visible_artifacts = program.artifacts.values().collect::<Vec<_>>();
    visible_artifacts.sort_by_key(|artifact| {
        (
            Some(artifact.artifact_ref.as_str()) != preview_ref,
            &artifact.artifact_ref,
        )
    });
    let issues = crate::agentic::issues::issues(program);
    // The Goal verifier's completion gap is authoritative for closability.
    // Program task state alone cannot see it, and a checkpoint that claims
    // readiness while the Goal gate still waits sends the model into a
    // request/reject loop.
    let closure = match goal_assessment.as_ref() {
        Some(Ok(assessment)) => assessment.view(program),
        Some(Err(error)) => crate::execution_core::goal::assessment::unknown_completion_view(
            program,
            &error.to_string(),
        ),
        None => crate::execution_core::goal::assessment::unknown_completion_view(
            program,
            "bound Goal reader is unavailable",
        ),
    };
    let teams = program
        .teams
        .values()
        .take(METADATA_WINDOW)
        .map(|team| {
            serde_json::json!({
                "team_ref": team.team_id,
                "name": team.name,
                "member_count": team.member_ids.len(),
                "task_count": team.task_ids.len(),
                "read_request":{"name":"state_inspect","input":{"entry_ref":team.team_id}},
            })
        })
        .collect::<Vec<_>>();
    let agents = program
        .agents
        .values()
        .take(METADATA_WINDOW)
        .map(|agent| {
            serde_json::json!({
                "agent_ref": agent.agent_id,
                "team_refs": program.active_team_ids_for(&agent.agent_id),
                "role": agent.role,
                "read_request":{"name":"state_inspect","input":{"entry_ref":agent.agent_id}},
            })
        })
        .collect::<Vec<_>>();
    let tasks = visible_tasks.into_iter().take(METADATA_WINDOW).map(|task| {
        serde_json::json!({
            "task_ref":task.task_id,"team_ref":task.team_id,"title":task.title,
            "status":task.status,"claimant":task.claimant,
            "artifact_count":task.artifact_refs.len(),"evidence_count":task.evidence_refs.len(),
            "unresolved_count":task.unresolved.len(),"review_required":task.review_reason.is_some(),
            "read_request":{"name":"state_inspect","input":{"entry_ref":task.task_id}}
        })
    }).collect::<Vec<_>>();
    // Tasks whose author submitted but which no independent reviewer has picked
    // up. The root used to see these as ordinary `Submitted` rows and had no way
    // to tell "a reviewer is on it" from "nobody was ever asked" - so it waited,
    // polled, and eventually gave up with the work delivered and unverified. The
    // root is the only party that can act on it: it can invite another Agent, or
    // create another Team, to review.
    let awaiting_review = program
        .tasks
        .values()
        .filter(|task| {
            task.status == crate::AgenticTaskStatus::Submitted
                && !task.active_attempts.values().any(|attempt| {
                    attempt.mode == harness_contract::agent_action::AgentAttemptMode::Review
                })
        })
        .map(|task| {
            serde_json::json!({
                "task_ref": task.task_id,
                "team_ref": task.team_id,
                "claimant": task.claimant,
                "artifact_refs": task.artifact_refs,
                "read_request":{"name":"state_inspect","input":{"entry_ref":task.task_id}}
            })
        })
        .collect::<Vec<_>>();
    let has_awaiting_review = !awaiting_review.is_empty();
    let artifacts = visible_artifacts.into_iter().take(METADATA_WINDOW)
        .map(|artifact| {
            let content = services.filter(|_| Some(artifact.artifact_ref.as_str()) == preview_ref).and_then(|services| {
                agentic_checkpoint_artifact_content(
                    services,
                    &artifact.content_ref,
                    &format!("session:{}", program.session_id),
                )
            });
            serde_json::json!({
                "artifact_ref": artifact.artifact_ref,
                "content_ref": artifact.content_ref,
                "title": artifact.title,
                "kind": artifact.kind,
                "relation_count": artifact.relates_to.len(),
                "committed_by": artifact.committed_by,
                "read_request":{"name":"state_inspect","input":{"entry_ref":artifact.artifact_ref}},
                "content_request":{"name":"evidence_retrieve","input":{"evidence_ref":artifact.content_ref}},
                "content": content.as_ref().map(|content| content.text.as_str()),
                "content_complete": content.as_ref().map(|content| content.complete),
            })
        })
        .collect::<Vec<_>>();
    let next_actions = if let Some(action) = terminal_action.as_ref() {
        action.checkpoint_next_actions()
    } else if program.status == crate::AgenticProgramStatus::Verified {
        vec!["synthesize_user_delivery"]
    } else if program.status == crate::AgenticProgramStatus::Blocked {
        vec!["explain_blocker_or_replan"]
    } else if program.status == crate::AgenticProgramStatus::CompletionRequested {
        vec!["await_objective_supervisor_verdict"]
    } else if program.tasks.values().any(|task| {
        matches!(
            task.status,
            crate::AgenticTaskStatus::Claimed | crate::AgenticTaskStatus::Submitted
        )
    }) {
        vec!["wait_for_active_agents"]
    } else {
        vec!["publish_or_replan_ready_work"]
    };
    serde_json::json!({
        "kind": "agentic_program_checkpoint",
        "program_id": program.program_id,
        "revision": program.revision,
        "status": program.status,
        "required_team_count": program.required_team_count,
        "objective":program.objective_summary,
        "directory_request":{"name":"state_inspect","input":{}},
        "coverage":{
            "kind":"current_work_window","authoritative_directory_complete":false,
            "metadata_window_per_kind":METADATA_WINDOW,"body_preview_limit":1,
            "total":{"teams":program.teams.len(),"agents":program.agents.len(),"tasks":program.tasks.len(),"artifacts":program.artifacts.len(),"issues":issues.len()},
            "loaded":{"teams":teams.len(),"agents":agents.len(),"tasks":tasks.len(),"artifacts":artifacts.len(),"issues":issues.len().min(METADATA_WINDOW)}
        },
        "teams": teams,
        "agents": agents,
        "tasks": tasks,
        // Non-empty means work is delivered but unverified and nobody is
        // verifying it. Name it, and name the action, rather than leaving the
        // root to infer it from a row that looks the same as an in-flight one.
        "awaiting_independent_review": awaiting_review,
        "awaiting_review_action": if has_awaiting_review {
            serde_json::json!({
                "instruction": "Each task_ref in `awaiting_independent_review` has a submitted \
                                 artifact and no reviewer. Dispatch an independent reviewer for it \
                                 now: the reviewer must be a different Agent from the claimant, in a \
                                 different Team when one exists. Use `agent_invite` to add one, then \
                                 `task_claim` in review mode. Do not wait, do not poll, and do not \
                                 conclude while delivered work is unverified."
            })
        } else {
            serde_json::Value::Null
        },
        "artifacts": artifacts,
        "continuation": program.continuation,
        "issues":issues.iter().take(METADATA_WINDOW).map(|issue| serde_json::json!({
            "issue_ref":issue.issue_ref,"source_ref":issue.source_ref,
            "disposition":issue.disposition.as_ref().map(|value| value.disposition),
            "read_request":{"name":"state_inspect","input":{"entry_ref":issue.issue_ref}}
        })).collect::<Vec<_>>(),
        // Completion is rejected until every issue is classified; name the exact
        // outstanding ones so the root can dispose them in one message_publish
        // instead of guessing whether any remain.
        "unclassified_issue_refs": issues
            .iter()
            .filter(|issue| issue.disposition.is_none())
            .map(|issue| issue.issue_ref.clone())
            .collect::<Vec<_>>(),
        "unresolved_count": program.unresolved.len(),
        "next_actions": next_actions,
        "terminal_action": terminal_action.as_ref().map(RootAgenticTerminalAction::checkpoint_detail),
        "closure": closure,
        "instruction": "Use this Runtime projection as current truth. You may continue planning, publish dependent or independent Tasks, and coordinate while members execute. When ready to wait for active workers, call state_inspect with wait_for_workers=true; Runtime waits without provider polling. Do not recreate committed entities. Completion is rejected while any entry remains in `unclassified_issue_refs`: classify EVERY issue_ref there through one message_publish.issue_dispositions (must_resolve, disclose, or resolved) with durable reasons/evidence (a tool://artifact:// receipt OR the issue's own source_ref, its Runtime-created anchor). Keep estimates and assumptions labelled; only the Goal verifier can certify the original objective. When `closure.exit_ready` is true every Runtime condition already holds: call `objective_complete_request` yourself to close out the Objective - Runtime verifies it and rejects it if any condition regressed, so you never risk a premature or unverifiable exit. Tasks named in `closure.satisfied` are finished; do not re-review, re-submit or re-commit them. Your job at that point is the exit decision, not more work.",
    })
    .to_string()
}

struct AgenticCheckpointArtifactContent {
    text: String,
    complete: bool,
}

/// Bring accepted Agent output back to the parent model as consumable
/// evidence, not only as an opaque storage pointer. The ArtifactStore remains
/// the authority and enforces the receiving Session's visibility scope. This
/// blocking helper runs outside the Turn lock. Large artifacts use byte-range
/// previews; their complete content remains available through content_ref.
fn agentic_checkpoint_artifact_content(
    services: &crate::RuntimeServices,
    content_ref: &str,
    authorized_scope: &str,
) -> Option<AgenticCheckpointArtifactContent> {
    const PREVIEW_BYTES: u64 = 64 * 1024;
    const TAIL_BYTES: u64 = 16 * 1024;

    let artifact = services.artifact_store().resolve(content_ref).ok()?;
    let complete = artifact.bytes <= PREVIEW_BYTES;
    let head_end = if complete {
        artifact.bytes
    } else {
        PREVIEW_BYTES - TAIL_BYTES
    };
    let bytes = services
        .artifact_store()
        .read_blocking(&artifact, authorized_scope, Some(0..head_end))
        .ok()?;
    let text = String::from_utf8_lossy(&bytes);
    if complete {
        return Some(AgenticCheckpointArtifactContent {
            text: text.into_owned(),
            complete: true,
        });
    }

    let tail_start = artifact.bytes - TAIL_BYTES;
    let tail = services
        .artifact_store()
        .read_blocking(
            &artifact,
            authorized_scope,
            Some(tail_start..artifact.bytes),
        )
        .ok()?;
    let tail = String::from_utf8_lossy(&tail);
    Some(AgenticCheckpointArtifactContent {
        text: format!(
            "{text}\n\n[Preview only: bytes {head_end}..{tail_start} are available through {content_ref}; retrieve them before drawing conclusions that require the full Artifact]\n\n{tail}"
        ),
        complete: false,
    })
}

#[test]
#[cfg(test)]
fn closure_view_names_the_outstanding_rung_in_a_stalled_program() {
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.required_team_count = 1;
    let view = agentic_closure_view(&program, None);
    // No Team, no Task, no completion request: every framework-owned rung is
    // outstanding and `current` names the first one instead of being undefined.
    assert_eq!(view["current"], serde_json::json!("team"));
    let steps = view["remaining"]
        .as_array()
        .expect("remaining")
        .iter()
        .filter_map(|step| step.get("step").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(steps.contains(&"publish_task"), "{steps:?}");
    assert!(!steps.contains(&"objective_complete_request"), "no registered result permits exit: {steps:?}");
    assert_eq!(view["facts"]["required_teams"], serde_json::json!(1));
}

#[test]
#[cfg(test)]
fn closure_view_blocks_exit_while_the_goal_completion_gap_remains() {
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.required_team_count = 1;
    // Every Program-owned rung is satisfied and no request is pending, yet the
    // Goal verifier still records a completion gap: the closure view must not
    // advertise the exit action, because the Goal gate would only reject it.
    let view = agentic_closure_view(
        &program,
        Some("original_intent_review_required:user_intent"),
    );
    assert_eq!(
        view["exit_ready"],
        serde_json::json!(false),
        "a Goal completion gap must hold the exit closed"
    );
    assert_eq!(
        view["completion_gap"],
        serde_json::json!("original_intent_review_required:user_intent")
    );
    let steps = view["remaining"]
        .as_array()
        .expect("remaining")
        .iter()
        .filter_map(|step| step.get("step").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        steps.contains(&"objective_review"),
        "the gap must be named as the outstanding rung: {steps:?}"
    );
    assert!(
        !steps.contains(&"objective_complete_request"),
        "the exit action must not be advertised while the gap remains: {steps:?}"
    );
}

#[test]
#[cfg(test)]
fn closure_view_preserves_goal_gap_without_count_derived_synthesis() {
    // The rung used to live only in `root_agentic_terminal_action`, which
    // returns None while the Goal records a completion gap. A run with two
    // accepted deliveries and no integration Artifact was therefore never told
    // to publish the synthesis Task, reached a satisfied verdict, and failed
    // only the harness's synthesis gate. The always-on view must name it in
    // every state, gap or no gap.
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.required_team_count = 1;
    program.teams.insert(
        "team:1".to_string(),
        crate::AgenticTeamProjection {
            team_id: "team:1".to_string(),
            name: "team".to_string(),
            mission: "mission".to_string(),
            objective: None,
            lifecycle: Default::default(),
            member_ids: vec!["agent:1".to_string(), "agent:2".to_string()],
            task_ids: vec!["task:1".to_string(), "task:2".to_string()],
            topic_ref: String::new(),
            created_by: "root".to_string(),
        },
    );
    for (id, claimant) in [("task:1", "agent:1"), ("task:2", "agent:2")] {
        program.tasks.insert(
            id.to_string(),
            crate::AgenticTaskProjection {
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                task_id: id.to_string(),
                team_id: "team:1".to_string(),
                title: format!("delivery {id}"),
                objective: "objective".to_string(),
                acceptance: "accepted evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: crate::AgenticTaskStatus::Accepted,
                claimant: Some(claimant.to_string()),
                claim_generation: 1,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                claim_renewal_streak: 0,
                active_attempts: Default::default(),
                artifact_refs: vec![format!("artifact:{id}")],
                evidence_refs: vec!["tool://receipt".to_string()],
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: Some("agent:2".to_string()),
                failed_attempts: 0,
                review_generation: 1,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
    }
    let view = agentic_closure_view(
        &program,
        Some("original_intent_review_required:user_intent"),
    );
    let steps = view["remaining"]
        .as_array()
        .expect("remaining")
        .iter()
        .filter_map(|step| step.get("step").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    assert!(
        !steps.contains(&"publish_integration_task"),
        "accepted delivery counts must not invent a synthesis Task: {steps:?}"
    );
    assert!(
        steps.contains(&"objective_review"),
        "the Goal gap stays advertised alongside it: {steps:?}"
    );
    assert!(
        !steps.contains(&"objective_complete_request"),
        "the exit action stays closed while the gap remains: {steps:?}"
    );
    // The rung's own hint must carry the same read-first rule the
    // completion-gap guidance carries. One live run lost its review attempt
    // because the rung said only "close this gap first": the reviewer submitted
    // before reading, burned the three-retry budget, and only then read the
    // results.
    let objective_review_rung = view["remaining"]
        .as_array()
        .expect("remaining")
        .iter()
        .find(|step| {
            step.get("step").and_then(serde_json::Value::as_str) == Some("objective_review")
        })
        .expect("objective_review rung");
    let hint = objective_review_rung
        .get("hint")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        hint.contains("READ the delivered results itself in its own execution"),
        "the rung must say the reviewer reads first: {hint}"
    );
    assert!(
        hint.contains("an earlier attempt of the same Agent does not count"),
        "the rung must name whose read counts: {hint}"
    );
}

#[test]
#[cfg(test)]
fn citable_refs_states_what_exists_and_never_what_to_choose() {
    // The contracts say where to fetch an instance ("returned by
    // state_inspect", "from task_submit") and never what it is, so the model has
    // to fetch, parse and remember it - and the remember step fails in a large
    // context. One live run published a sha256 that was the digest of the empty
    // string because it could no longer recall the real one.
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.required_team_count = 1;
    program.root_execution_id = Some("root-execution".to_string());
    program.teams.insert(
        "team:1".to_string(),
        crate::AgenticTeamProjection {
            team_id: "team:1".to_string(),
            name: "team".to_string(),
            mission: "mission".to_string(),
            objective: None,
            lifecycle: Default::default(),
            member_ids: vec!["agent:1".to_string(), "agent:2".to_string()],
            task_ids: vec!["task:1".to_string(), "task:2".to_string()],
            topic_ref: "topic:one".to_string(),
            created_by: "root".to_string(),
        },
    );
    for id in ["task:1", "task:2"] {
        program.tasks.insert(
            id.to_string(),
            crate::AgenticTaskProjection {
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                task_id: id.to_string(),
                team_id: "team:1".to_string(),
                title: format!("delivery {id}"),
                objective: "objective".to_string(),
                acceptance: "accepted evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: crate::AgenticTaskStatus::Accepted,
                claimant: Some("agent:1".to_string()),
                claim_generation: 1,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                claim_renewal_streak: 0,
                active_attempts: Default::default(),
                artifact_refs: vec![format!("artifact:{id}")],
                evidence_refs: vec!["tool://receipt".to_string()],
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: Some("agent:2".to_string()),
                failed_attempts: 0,
                review_generation: 1,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
    }
    let services = crate::RuntimeServices::in_memory().expect("services");
    let refs = citable_refs(&program, &services, "session", "turn");
    // The accepted results are named, so a review can cite them without guessing.
    let accepted = refs["accepted_results"]
        .as_array()
        .expect("accepted_results");
    assert_eq!(accepted.len(), 2, "{refs}");
    assert!(accepted.iter().any(|value| value == "artifact:task:1"));
    // The topic ref is named, so `message_publish` does not have to invent one.
    assert_eq!(refs["topic_refs"][0], "topic:one");
    // It states what exists and explicitly declines to choose.
    assert!(
        refs["note"]
            .as_str()
            .is_some_and(|note| note.contains("registered Program refs") && note.contains("not all filesystem facts")),
        "the note must offer the facts without prescribing a choice: {}",
        refs["note"]
    );
    // An omission is announced, never silent.
    assert!(
        refs.get("artifacts_more").is_none(),
        "two artifacts fit the cap"
    );
}

#[test]
#[cfg(test)]
fn closure_view_does_not_infer_goal_readiness_from_accepted_counts() {
    let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
    program.required_team_count = 1;
    // Every framework-owned rung holds: one Team, one accepted Task, no
    // unresolved entries, no pending request, no Goal gap. The only
    // outstanding action is the model's own completion request, so the view
    // must say so explicitly instead of staying silently not-ready.
    program.teams.insert(
        "team:1".to_string(),
        crate::AgenticTeamProjection {
            team_id: "team:1".to_string(),
            name: "team".to_string(),
            mission: "mission".to_string(),
            objective: None,
            lifecycle: Default::default(),
            member_ids: vec!["agent:1".to_string()],
            task_ids: vec!["task:1".to_string()],
            topic_ref: String::new(),
            created_by: "root".to_string(),
        },
    );
    let mut task = crate::AgenticTaskProjection {
        obligations: Vec::new(),
        rework_directives: Vec::new(),
        unsettled_released_attempts: Vec::new(),
        terminal_note: None,
        claim_renewal_streak: 0,
        task_id: "task:1".to_string(),
        team_id: "team:1".to_string(),
        title: "delivered".to_string(),
        objective: "objective".to_string(),
        acceptance: "accepted evidence".to_string(),
        acceptance_checks: Vec::new(),
        criterion_source: Default::default(),
        required_capabilities: Vec::new(),
        depends_on: Vec::new(),
        status: crate::AgenticTaskStatus::Accepted,
        claimant: Some("agent:1".to_string()),
        claim_generation: 1,
        claim_execution_id: None,
        claimed_at_ms: None,
        lease_expires_at_ms: None,
        active_attempts: Default::default(),
        artifact_refs: vec!["artifact:1".to_string()],
        evidence_refs: vec!["tool://receipt".to_string()],
        unresolved: Vec::new(),
        review_reason: None,
        reviewed_by: Some("agent:2".to_string()),
        failed_attempts: 0,
        review_generation: 1,
        failed_review_attempts: 0,
        last_failure: None,
        replacement_task_refs: Vec::new(),
        supersede_evidence_refs: Vec::new(),
        superseded_reason: None,
        superseded_by: None,
        obligation_refs: Vec::new(),
        purpose: Default::default(),
        execution_requirements: Vec::new(),
        expertise_hints: Vec::new(),
        cancel_requested_by: None,
        cancel_reason_ref: None,
        cancel_evidence_refs: Vec::new(),
        pending_retirement: None,
    };
    task.status = crate::AgenticTaskStatus::Accepted;
    program.tasks.insert("task:1".to_string(), task);

    let view = agentic_closure_view(&program, None);
    assert_eq!(view["exit_ready"], serde_json::json!(false));
    assert_eq!(
        view["current"],
        serde_json::json!("goal_verification_required")
    );
}

#[cfg(test)]
fn test_collaboration_obligation(
    minimum_team_count: u8,
) -> harness_contract::strategy::CollaborationExecutionObligation {
    harness_contract::strategy::CollaborationExecutionObligation {
        team_count_contract_version: 1,
        minimum_team_count,
        exact_team_count: None,
    }
}

// Return only user-named field labels; never include candidate values or
// framework identities in model-facing recovery feedback.
fn json_field_language_findings(objective: &str, fields: &serde_json::Map<String, serde_json::Value>) -> Option<Vec<String>> {
    let mut named = false;
    let mut missing = Vec::new();
    for sentence in objective.split(|c: char| matches!(c, '。' | '；' | ';' | '\n')) {
        if !sentence.contains("中文") && !sentence.to_ascii_lowercase().contains("chinese") {
            continue;
        }
        for (name, value) in fields {
            if sentence.contains(name) {
                named = true;
                if !value.as_str().is_some_and(|text| text.chars().any(is_cjk_character)) {
                    if name.len() <= 80 && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                        missing.push(name.clone());
                    } else {
                        // An unrenderable user-named field must still fail the
                        // language check; it is never copied into feedback.
                        missing.push(String::new());
                    }
                }
            }
        }
    }
    named.then_some(missing)
}

fn response_language_mismatch(objective: &str, response: &str) -> bool {
    // Machine keys and paths do not themselves declare a language obligation.
    if objective_requires_strict_json(objective) {
        if let Ok(serde_json::Value::Object(fields)) = serde_json::from_str(response.trim()) {
            if let Some(missing) = json_field_language_findings(objective, &fields) {
                return !missing.is_empty();
            }
            let explicitly_chinese = objective.contains("中文")
                || objective.to_ascii_lowercase().contains("chinese");
            return explicitly_chinese && !fields.values().any(|value| {
                value.as_str().is_some_and(|text| text.chars().any(is_cjk_character))
            });
        }
    }
    let objective_uses_cjk = objective.chars().any(is_cjk_character);
    let response_uses_cjk = response.chars().any(is_cjk_character);
    let response_uses_language_specific_text = response.chars().any(char::is_alphabetic);
    objective_uses_cjk && response_uses_language_specific_text && !response_uses_cjk
}

fn response_language_mismatch_for_role(
    objective: &str,
    response: &str,
    delegated_agent_role: bool,
) -> bool {
    !delegated_agent_role && response_language_mismatch(objective, response)
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExhaustedTeamLeaseDisposition {
    CompleteRemainingWrite,
    CleanSynthesis,
}

#[cfg(test)]
fn exhausted_team_lease_disposition(
    required_write_for_completion: bool,
    write_obligation_satisfied: bool,
) -> ExhaustedTeamLeaseDisposition {
    if required_write_for_completion && !write_obligation_satisfied {
        ExhaustedTeamLeaseDisposition::CompleteRemainingWrite
    } else {
        ExhaustedTeamLeaseDisposition::CleanSynthesis
    }
}

fn current_turn_write_scopes(
    goal: &harness_contract::goal::GoalContract,
    delegated_leaf: bool,
    focus_scopes: &[String],
    legacy_requires_write: bool,
) -> Vec<String> {
    if delegated_leaf {
        return focus_scopes
            .iter()
            .filter(|scope| scope.starts_with("write:"))
            .cloned()
            .collect();
    }
    let scopes = crate::execution_core::goal::goal_workspace_write_scopes(goal);
    if goal
        .input_contract
        .as_ref()
        .is_some_and(|contract| contract.version == 2)
        || !scopes.is_empty()
        || !legacy_requires_write
    {
        return scopes;
    }
    // Legacy records lack authenticated revision rights. Preserve their
    // original admitted baseline until a real source-backed contract replaces
    // it; this adapter cannot grant tools or amend the Goal from plain context.
    let exact = goal
        .input_contract
        .as_ref()
        .map(|contract| {
            contract
                .proposed_workspace_scopes
                .iter()
                .filter(|scope| scope.starts_with("write:"))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if exact.is_empty() {
        vec![String::new()]
    } else {
        exact
    }
}

fn current_turn_forbids_tools(goal: &harness_contract::goal::GoalContract, legacy: bool) -> bool {
    goal.input_contract
        .as_ref()
        .is_some_and(|contract| contract.forbids_tools())
        || (legacy
            && goal
                .input_contract
                .as_ref()
                .is_none_or(|contract| contract.version != 2))
}

fn current_turn_forbids_workspace_write(
    goal: &harness_contract::goal::GoalContract,
    legacy: bool,
) -> bool {
    goal.input_contract
        .as_ref()
        .is_some_and(|contract| contract.forbids_workspace_write())
        || (legacy
            && goal
                .input_contract
                .as_ref()
                .is_none_or(|contract| contract.version != 2))
}

/// Durable observation-reference prefix for a successful Runtime-authorized
/// workspace mutation whose adapter cannot itself describe the individual
/// files it changed (for example, a governed shell command). This is a
/// Runtime fact: it is minted only from the registered effect descriptor and
/// a successful host receipt, never from model prose or a requested path.
const WORKSPACE_MUTATION_EFFECT_REF_PREFIX: &str = "workspace_mutation_effect:";

pub(super) fn workspace_mutation_effect_evidence_ref(sequence: u64) -> String {
    format!("{WORKSPACE_MUTATION_EFFECT_REF_PREFIX}{sequence}")
}

pub(super) fn workspace_mutation_effect_sequence(reference: &str) -> Option<u64> {
    reference
        .strip_prefix(WORKSPACE_MUTATION_EFFECT_REF_PREFIX)
        .and_then(|sequence| sequence.parse::<u64>().ok())
        .filter(|sequence| *sequence > 0)
}

/// Turn a post-mutation exact read into a narrowly typed `WriteEffect` fact.
///
/// A direct file adapter can attest a mutation's pre-image and post-image, and
/// continues to use that stronger receipt. A governed generic workspace effect
/// (notably a shell command) cannot honestly name every file it changed. It
/// can still prove the desired final state when all of these are true:
///
/// 1. Runtime previously recorded a successful workspace-mutation effect;
/// 2. the ToolHost later emitted a complete, digest-bearing exact read; and
/// 3. that read is the exact user-required write target.
///
/// The resulting fact is intentionally *postcondition verified*, not a claim
/// about a pre-image or a model-provided write request. Callers that need a
/// proven delta must still require a direct mutation receipt with
/// `workspace_prior_state`.
pub(super) fn verified_workspace_postcondition_write_evidence(
    required_scopes: &[String],
    prior_workspace_mutation_sequences: &BTreeSet<u64>,
    current_observed_evidence: &[harness_contract::context::ObservedEvidence],
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> Vec<harness_contract::context::ObservedEvidence> {
    if required_scopes.is_empty() || prior_workspace_mutation_sequences.is_empty() {
        return Vec::new();
    }

    let mut verified = Vec::new();
    for required_scope in required_scopes {
        let required = resolver.compile_obligation_or_unresolved(required_scope);
        for observed in current_observed_evidence {
            if !prior_workspace_mutation_sequences
                .iter()
                .any(|sequence| *sequence < observed.observed_at_sequence)
            {
                continue;
            }
            let mut postcondition = observed.clone();
            let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
                &mut postcondition.target
            else {
                continue;
            };
            if scope.access_mode != harness_contract::context::WorkspaceAccessMode::Read
                || scope.coverage != harness_contract::context::EvidenceCoverageKind::ExactContent
            {
                continue;
            }
            scope.access_mode = harness_contract::context::WorkspaceAccessMode::Write;
            scope.coverage = harness_contract::context::EvidenceCoverageKind::WriteEffect;
            postcondition.obligation_id = format!(
                "verified_workspace_postcondition:{}:{}",
                observed.obligation_id, observed.observed_at_sequence
            );
            postcondition.tool_name =
                format!("verified_workspace_postcondition:{}", observed.tool_name);
            // A complete postcondition read cannot reconstruct the pre-image.
            postcondition.workspace_prior_state = None;
            if crate::path_identity::observed_evidence_satisfies(&required, &postcondition)
                && !verified.contains(&postcondition)
            {
                verified.push(postcondition);
            }
        }
    }
    verified
}

#[cfg(test)]
mod write_obligation_probe {
    use super::*;

    #[test]
    fn root_read_alias_is_satisfied_by_descendant_exact_read() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("plan")).expect("dir");
        std::fs::write(root.path().join("plan/doc.md"), "x").expect("doc");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let required = resolver
            .compile_obligation_with_root_alias("read:.", true)
            .expect("required read root");
        let observed = resolver
            .observe_tool_scope("read_file", "read:plan/doc.md", Some("abc"), 1)
            .expect("observed read");
        assert!(
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(&required, &[observed],),
            "a descendant exact read must satisfy read:. root alias"
        );
        let glob = resolver
            .observe_tool_scope("glob_search", "glob:plan", None, 2)
            .expect("observed glob");
        assert!(
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(&required, &[glob],),
            "a workspace glob discovery must also satisfy the read:. lease"
        );
    }

    #[test]
    fn write_obligation_satisfied_matches_observed_write_receipt() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::write(
            root.path().join("cross-team-decision-report.html"),
            "<html>report</html>",
        )
        .expect("report file");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let required = resolver
            .compile_obligation("write:cross-team-decision-report.html")
            .expect("required write");
        let observed = resolver
            .observe_tool_scope(
                "edit",
                "write:cross-team-decision-report.html",
                Some("d6340e8783fa57ad2c78f51b708e7e9f98f592fc3037a1003cb71fcbf343f108"),
                7,
            )
            .expect("observed write receipt");
        assert!(crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
            &required,
            &[observed.clone()],
        ));
        assert!(write_obligation_satisfied(
            true,
            &["write:cross-team-decision-report.html".to_string()],
            &[observed],
            false,
            &resolver,
        ));
    }

    #[test]
    fn verified_postcondition_closes_exact_write_after_runtime_mutation() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::write(root.path().join("artifact.txt"), "verified artifact")
            .expect("artifact file");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let read = resolver
            .observe_trusted_tool_output_file(
                "read_file",
                harness_contract::context::WorkspaceAccessMode::Read,
                "artifact.txt",
                "9872b6b522ad77e2505908cad164d5c7a5fb6489c1c10b9f31a5d4f07f84c14b",
                4,
            )
            .expect("exact read receipt");
        let verified = verified_workspace_postcondition_write_evidence(
            &["write:artifact.txt".to_string()],
            &BTreeSet::from([3]),
            &[read],
            &resolver,
        );
        assert_eq!(verified.len(), 1);
        assert!(write_obligation_satisfied(
            true,
            &["write:artifact.txt".to_string()],
            &verified,
            false,
            &resolver,
        ));
        assert!(matches!(
            &verified[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                if scope.access_mode == harness_contract::context::WorkspaceAccessMode::Write
                    && scope.coverage
                        == harness_contract::context::EvidenceCoverageKind::WriteEffect
        ));
        assert!(verified[0].workspace_prior_state.is_none());
    }

    #[test]
    fn verified_postcondition_rejects_same_or_later_mutation_epoch() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::write(root.path().join("artifact.txt"), "verified artifact")
            .expect("artifact file");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let read = resolver
            .observe_trusted_tool_output_file(
                "read_file",
                harness_contract::context::WorkspaceAccessMode::Read,
                "artifact.txt",
                "9872b6b522ad77e2505908cad164d5c7a5fb6489c1c10b9f31a5d4f07f84c14b",
                4,
            )
            .expect("exact read receipt");
        assert!(verified_workspace_postcondition_write_evidence(
            &["write:artifact.txt".to_string()],
            &BTreeSet::from([4, 5]),
            &[read],
            &resolver,
        )
        .is_empty());
    }

    #[test]
    fn objective_absolute_path_extracts_relative_write_scope() {
        let root = tempfile::tempdir().expect("workspace");
        let canonical = root.path().canonicalize().expect("canonical root");
        std::fs::write(
            root.path().join("cross-team-decision-report.html"),
            "<html>report</html>",
        )
        .expect("report file");
        let objective = format!(
            "必须先调用 write_file 把统一 HTML 决策报告写入 {}/cross-team-decision-report.html（覆盖 summary/evidence/key_decisions/unresolved_or_risks），写盘成功收据后再输出终态 JSON。",
            canonical.display()
        );
        let scopes = crate::workspace_scopes::explicit_workspace_resource_scopes(
            root.path(),
            &objective,
            true,
        );
        println!("SCOPES_PROBE {scopes:?}");
        assert!(scopes
            .iter()
            .any(|scope| scope == "write:cross-team-decision-report.html"));
    }
}

fn should_recover_response_language(mismatch: bool, rewrite_attempted: bool) -> bool {
    mismatch && !rewrite_attempted
}

fn is_cjk_character(character: char) -> bool {
    matches!(
        character as u32,
        0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xF900..=0xFAFF
    )
}

/// Extract completed required instances from the bounded Program terminal
/// projection.  This deliberately refuses legacy `team_ids` and free-form
/// Team summaries: those are presentation/transport details and must never
/// become the host's lifecycle authority.
fn completed_program_team_ids(messages: &[ConversationMessage]) -> BTreeSet<String> {
    let _ = messages;
    BTreeSet::new()
}

fn has_completed_program_terminal(messages: &[ConversationMessage]) -> bool {
    !completed_program_team_ids(messages).is_empty()
}

/// Whether Runtime admitted a durable collaboration Program, regardless of
/// whether that Program completed successfully. The receipt is lifecycle
/// authority; tool-result success is not. Once true, this root turn must not
/// expose a second admission port for the same objective.
fn has_admitted_program_receipt(messages: &[ConversationMessage]) -> bool {
    let _ = messages;
    false
}

fn parent_merge_actuals(
    started_at: Option<std::time::Instant>,
    parent_succeeded: bool,
) -> (u64, u8) {
    let merge_cost_ms = started_at
        .map(|started| {
            u64::try_from(started.elapsed().as_millis())
                .unwrap_or(u64::MAX)
                .max(1)
        })
        .unwrap_or(0);
    (
        merge_cost_ms,
        u8::from(started_at.is_some() && parent_succeeded),
    )
}

fn compile_retargeted_conversation_graph(
    current: &harness_contract::execution_graph::ExecutionGraph,
    objective: &str,
    session_id: &str,
    ingress: Option<&TurnIngressRef>,
    target: crate::execution_core::RuntimeCompileTarget,
    stable_parent_node_id: &str,
) -> Result<harness_contract::execution_graph::ExecutionGraph, RuntimeError> {
    let payload = serde_json::json!({
        "kind": "conversation_turn",
        "session_id": session_id,
        "content": objective,
        "compile_target": target,
        "ingress": ingress,
        "idempotency_key": ingress.map(|value| value.request_id.as_str()),
    })
    .to_string();
    let mut replacement = ExecutionGraphCompiler
        .compile_conversation_turn(ExecutionCompileRequest {
            objective: objective.to_string(),
            payload_ref: payload,
            target,
            resource_scopes: current
                .nodes
                .iter()
                .filter(|node| {
                    node.kind == ExecutionNodeKind::Verify
                        && node.executor_kind
                        == crate::execution_core::graph::executors::CompileTargetGuardExecutor::KIND
                        && node
                            .acceptance
                            .criteria
                            .iter()
                            .any(|criterion| criterion == "mutation_resources_must_be_leased")
                })
                .flat_map(|node| node.resource_scopes.iter().cloned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        })
        .map_err(|error| RuntimeError::new(error.to_string()))?;
    ExecutionGraphCompiler
        .bind_conversation_graph_identity(
            &mut replacement,
            &current.id,
            Some(stable_parent_node_id),
            ingress.map(|value| value.request_id.as_str()),
        )
        .map_err(RuntimeError::new)?;
    if let Some(dispatch) = current
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::SessionDispatch)
        .cloned()
    {
        replacement.nodes.insert(0, dispatch.clone());
        replacement.edges.insert(
            0,
            ExecutionEdge {
                from: dispatch.id,
                to: stable_parent_node_id.to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        );
    }
    replacement.parent_execution = current.parent_execution.clone();
    replacement.lineage = current.lineage.clone();
    replacement.service_class = current.service_class;
    harness_contract::execution_graph::validate_execution_graph(&replacement)
        .map_err(|error| RuntimeError::new(error.to_string()))?;
    Ok(replacement)
}

#[cfg(test)]
#[test]
fn conversation_retarget_preserves_delegation_scopes_and_unique_node_identity() {
    use crate::execution_core::RuntimeCompileTarget;
    let targets = [
        RuntimeCompileTarget::InlineModel,
        RuntimeCompileTarget::EvidenceGraph,
        RuntimeCompileTarget::ExecutionGraph,
    ];
    let ingress = TurnIngressRef {
        request_id: "request".into(),
        turn_id: "turn".into(),
        message_id: "message".into(),
        session_id: "session".into(),
        primary_task_id: "task".into(),
        root_task_id: "task".into(),
        session_generation: 1,
        input_sequence: 1,
        claim_owner: "owner".into(),
        claim_token: "token".into(),
        claim_revision: 1,
        handoff_id: None,
    };
    for source in targets {
        for target in targets {
            for scopes in [
                vec!["workspace:.".to_string()],
                vec!["read:input.txt".to_string()],
                vec!["write:output.txt".to_string(), "read:input.txt".to_string()],
                Vec::new(),
            ] {
                for ingress in [None, Some(&ingress)] {
                    let mut current = ExecutionGraphCompiler
                        .compile_conversation_turn(ExecutionCompileRequest {
                            objective: "work".into(),
                            payload_ref: "{}".into(),
                            target: source,
                            resource_scopes: scopes.clone(),
                        })
                        .unwrap();
                    let parent = current.nodes[0].id.clone();
                    let dispatch = ingress.map(|_| {
                        let mut dispatch =
                            harness_contract::execution_graph::ExecutionNodeSpec::new(
                                ExecutionNodeKind::SessionDispatch,
                                "session_dispatch",
                                "dispatch-payload",
                            );
                        dispatch.id = format!("{}:dispatch", current.id);
                        dispatch.idempotency_key = "request:dispatch".into();
                        dispatch
                    });
                    if let Some(dispatch) = &dispatch {
                        current.nodes.insert(0, dispatch.clone());
                        current.edges.insert(
                            0,
                            ExecutionEdge {
                                from: dispatch.id.clone(),
                                to: parent.clone(),
                                kind: ExecutionEdgeKind::DependsOn,
                            },
                        );
                    }
                    current
                        .nodes
                        .iter_mut()
                        .find(|node| node.kind == ExecutionNodeKind::InlineModel)
                        .unwrap()
                        .resource_scopes = vec!["write:unrelated.txt".into()];
                    for next_target in [target, source, target] {
                        let replacement = compile_retargeted_conversation_graph(
                            &current,
                            "work",
                            "session",
                            ingress,
                            next_target,
                            &parent,
                        )
                        .unwrap();
                        harness_contract::execution_graph::validate_execution_graph(&replacement)
                            .unwrap();
                        assert_eq!(replacement.id, current.id);
                        assert_eq!(
                            replacement.nodes[usize::from(dispatch.is_some())].id,
                            parent
                        );
                        assert_eq!(
                            replacement
                                .nodes
                                .iter()
                                .map(|node| &node.id)
                                .collect::<BTreeSet<_>>()
                                .len(),
                            replacement.nodes.len()
                        );
                        assert_eq!(
                            replacement
                                .nodes
                                .iter()
                                .map(|node| &node.idempotency_key)
                                .collect::<BTreeSet<_>>()
                                .len(),
                            replacement.nodes.len()
                        );
                        assert_eq!(
                            replacement
                                .nodes
                                .iter()
                                .flat_map(|node| node.resource_scopes.iter().cloned())
                                .collect::<BTreeSet<_>>(),
                            scopes.iter().cloned().collect()
                        );
                        assert_eq!(replacement.edges.len(), replacement.nodes.len() - 1);
                        for (edge, pair) in
                            replacement.edges.iter().zip(replacement.nodes.windows(2))
                        {
                            assert_eq!(edge.from, pair[0].id);
                            assert_eq!(edge.to, pair[1].id);
                            assert_eq!(edge.kind, ExecutionEdgeKind::DependsOn);
                        }
                        if let Some(dispatch) = &dispatch {
                            assert_eq!(&replacement.nodes[0], dispatch);
                        }
                        for node in replacement
                            .nodes
                            .iter()
                            .filter(|node| node.kind != ExecutionNodeKind::SessionDispatch)
                        {
                            let payload: serde_json::Value =
                                serde_json::from_str(&node.payload_ref).unwrap();
                            assert_eq!(payload["ingress"], serde_json::to_value(ingress).unwrap());
                            if let Some(ingress) = ingress {
                                let suffix = if node.kind == ExecutionNodeKind::InlineModel {
                                    "model"
                                } else if node.acceptance.criteria.iter().any(|criterion| {
                                    criterion == "mutation_resources_must_be_leased"
                                }) {
                                    "resource-constraint"
                                } else if next_target == RuntimeCompileTarget::EvidenceGraph {
                                    "evidence-constraint"
                                } else {
                                    "policy-constraint"
                                };
                                assert_eq!(
                                    node.idempotency_key,
                                    format!("{}:{suffix}", ingress.request_id)
                                );
                            } else {
                                assert_eq!(node.idempotency_key, node.id);
                            }
                            if node.kind == ExecutionNodeKind::InlineModel {
                                assert!(node.resource_scopes.is_empty());
                            }
                        }
                        current = replacement;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[test]
fn conversation_retarget_does_not_promote_non_authorizing_scopes() {
    use crate::execution_core::graph::executors::CompileTargetGuardExecutor;
    use crate::execution_core::RuntimeCompileTarget;

    for (kind, executor_kind, criterion) in [
        (
            ExecutionNodeKind::Verify,
            "verify",
            "mutation_resources_must_be_leased",
        ),
        (
            ExecutionNodeKind::Verify,
            CompileTargetGuardExecutor::KIND,
            "evidence_read_before_synthesis",
        ),
        (
            ExecutionNodeKind::InlineModel,
            CompileTargetGuardExecutor::KIND,
            "mutation_resources_must_be_leased",
        ),
    ] {
        let mut current = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: "inspect".into(),
                payload_ref: "{}".into(),
                target: RuntimeCompileTarget::InlineModel,
                resource_scopes: Vec::new(),
            })
            .unwrap();
        let parent = current.nodes[0].id.clone();
        let mut decoy =
            harness_contract::execution_graph::ExecutionNodeSpec::new(kind, executor_kind, "decoy");
        decoy.acceptance.criteria = vec![criterion.into()];
        decoy.resource_scopes = vec!["workspace:.".into()];
        current.nodes.push(decoy);
        for target in [
            RuntimeCompileTarget::InlineModel,
            RuntimeCompileTarget::EvidenceGraph,
            RuntimeCompileTarget::ExecutionGraph,
        ] {
            let replacement = compile_retargeted_conversation_graph(
                &current, "inspect", "session", None, target, &parent,
            )
            .unwrap();
            assert!(replacement
                .nodes
                .iter()
                .all(|node| node.resource_scopes.is_empty()));
        }
        let mut dispatch = harness_contract::execution_graph::ExecutionNodeSpec::new(
            ExecutionNodeKind::SessionDispatch,
            "session_dispatch",
            "dispatch",
        );
        dispatch.id = parent.clone();
        current.nodes.insert(0, dispatch);
        let error = compile_retargeted_conversation_graph(
            &current,
            "inspect",
            "session",
            None,
            RuntimeCompileTarget::ExecutionGraph,
            &parent,
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("duplicate execution node `{parent}`")
        );
    }
}

pub(crate) struct TurnGraphState {
    evaluation_scope_rejections: u8,
    content: String,
    /// Strategy ingress parses the objective once. Later Team acceptance and
    /// graph repair paths consume this structured authority.
    task_understanding: Option<harness_contract::strategy::TaskUnderstanding>,
    /// Read-only hot-path copy of the durable turn strategy obligation.
    /// ConversationRuntime remains the sole owner and event source.
    collaboration_obligation: Option<harness_contract::strategy::CollaborationExecutionObligation>,
    prompter: SharedPrompter,
    first_model_step: bool,
    /// Runtime-authored checkpoint instructions that must be inserted in the
    /// next provider request's durable context envelope exactly once.
    pending_next_model_context: Vec<ContextItem>,
    /// Checked Team receipts stay visible for every provider request in this
    /// parent Turn. Acceptance replans must not lose the evidence that caused
    /// the remaining business obligation to be scheduled.
    persistent_collaboration_context: Vec<ContextItem>,
    assistant_messages: Vec<ConversationMessage>,
    tool_results: Vec<ConversationMessage>,
    iterations: usize,
    input_tokens: u64,
    output_tokens: u64,
    cache_create_tokens: u64,
    cache_read_tokens: u64,
    cache_dimensions_known: bool,
    /// Distinguishes a fresh turn from a recovered explicit-zero usage
    /// attempt so cache truth can be AND-reduced across narrator retries.
    provider_usage_attempt_observed: bool,
    output_chars: u64,
    output_chunks: u64,
    wall_duration_ms: u64,
    model: Option<String>,
    models_used: Vec<String>,
    first_token_latency_ms: Option<u64>,
    active_stream_duration_ms: u64,
    summary: Option<TurnSummary>,
    failure: Option<String>,
    /// Wakes the owned ingress future after an asynchronously resumed root
    /// graph commits its terminal synthesis. `notify_one` retains a permit,
    /// closing the summary-check/await race without polling.
    terminal_notify: std::sync::Arc<tokio::sync::Notify>,
    pending_transcript: std::collections::BTreeMap<String, Vec<ConversationMessage>>,
    pending_member_batches: std::collections::BTreeMap<(String, u32), Vec<(String, crate::PendingAgentBatch)>>,
    pending_agent_input_consumption: std::collections::BTreeMap<
        String,
        Vec<crate::runtime_event_store::RuntimeTransactionEventInput>,
    >,
    deferred_agent_input_consumption: Vec<crate::runtime_event_store::RuntimeTransactionEventInput>,
    ingress: Option<TurnIngressRef>,
    /// First transcript offset owned by this graph turn. Gateway ingress
    /// already persists the initial user row; the terminal outbox persists
    /// every committed row after it as one atomic, idempotent batch.
    turn_transcript_start: usize,
    session_id: String,
    turn_id: String,
    goal_id: String,
    context_window: u32,
    safety_lease: crate::execution_core::ExecutionBudgetLease,
    terminal_override: Option<(GoalCompletion, String)>,
    delivery_envelope: Option<harness_contract::outcome::DeliveryEnvelope>,
    terminal_presentation: Option<harness_contract::outcome::TerminalPresentation>,
    /// Exact Synthesize node/attempt that staged the terminal carrier. A
    /// Synthesize node may also commit a non-terminal replan when newer input
    /// crosses the final-answer barrier; its `after_commit` must not consume
    /// another attempt's terminal state or require an answer that was never
    /// staged.
    terminal_commit_owner: Option<(String, u32)>,
    authenticated_terminal_owner: Option<Arc<crate::execution_core::services::OriginalTerminalCommitOwner>>,
    committed_terminal_answer: Option<String>,
    committed_terminal_completion: Option<GoalCompletion>,
    last_verified_progress: bool,
    /// Consecutive model steps with no new Runtime-verified evidence or state
    /// transition. Total iterations remain observability telemetry only.
    consecutive_unverified_model_steps: usize,
    /// Root model steps taken since the closure view last reported every
    /// framework-owned rung satisfied. A weak model can be woken by
    /// `all_tasks_accepted` and then coordinate forever without making the exit
    /// decision; past `closure_drive_after_steps` Runtime drives the completion
    /// request on the root's behalf (the drive stays behind the completion_gap
    /// gate, so an incomplete Objective is never forced).
    closure_ready_steps: usize,
    /// The Objective milestones as of the previous batch. The no-progress fuse
    /// compares against this, so it has to survive across model steps.
    previous_objective_milestones: host_backend::ObjectiveMilestones,
    verified_collaboration_progress: host_backend::VerifiedCollaborationProgress,
    /// Root model steps taken since the Objective reached a satisfied verdict
    /// without the root presenting a final answer. The verdict is bound by the
    /// action that satisfied it, so the root's own last step is routinely the
    /// tool call that closed the Objective - it is never given the turn that
    /// would write the user-visible answer, and the session then goes quiet
    /// until an inactivity watchdog cancels it. Past
    /// `verified_answer_drive_after_steps` Runtime injects the instruction to
    /// present it.
    verified_answer_ready_steps: usize,
    /// Whether this turn has already produced a user-visible final answer, so
    /// the verified-answer drive fires at most once.
    final_answer_observed: bool,
    /// Bounded-convergence counter for the Agent-first collaboration completion
    /// gate. Each time the gate re-drives the root because the verified-Team
    /// minimum is unmet it advances; past the policy bound the turn terminates
    /// as an honest partial instead of replanning without end.
    collaboration_convergence_replans: usize,
    /// Bounded count of times the turn re-drove delegated work after the
    /// delegated protocol saturated. The Agent-first collaboration gate has its
    /// own bound; this one covers the delegated re-drive path, which could
    /// otherwise acquire without end because every re-drive looks like
    /// progress.
    delegated_redrives: usize,
    /// Bounded count of times a success presentation was returned to continue
    /// because a real Objective completion gap remained. Replans change the
    /// root graph node statuses, so the generic no-progress bound cannot fence
    /// this loop; this counter does.
    completion_gap_continuations: u64,
    /// When this turn's model loop started. Used only for the objective-level
    /// wall-clock budget that bounds a required-Team collaboration turn, which
    /// the no-progress fuse cannot bound while nominal progress continues.
    started_at: std::time::Instant,
    reasoning_only_attempts: u8,
    force_text_only_next_model: bool,
    force_tool_allowlist_next_model: Option<BTreeSet<String>>,
    /// Require a tool call without restricting which tool. Set by the escalation
    /// path when the model keeps concluding instead of acting; consumed at the
    /// model-step boundary exactly once, like the allowlist beside it.
    force_tool_call_next_model: bool,
    /// Request-local cognitive budget selected by a governed checkpoint.
    /// It is consumed once and never changes the session/provider default.
    force_reasoning_effort_next_model: Option<String>,
    terminal_recovery_attempts: u8,
    provider_protocol_recovery_attempts: u8,
    /// Bounded malformed-frame repair continuations. Independent of
    /// `provider_protocol_recovery_attempts` because a frame defect is
    /// repairable even when the turn already holds successful receipts.
    ///
    /// Keyed by defect signature, not a single counter. A turn that runs for
    /// many minutes emits dozens of batches, and the budget has to answer "has
    /// the model stopped learning from the refusal", not "has the model ever
    /// erred twice". One live run recovered perfectly from its first malformed
    /// frame and was then killed by a second, different-length frame with the
    /// same underlying mistake, because a per-turn counter had already spent
    /// its single attempt.
    provider_frame_repair_attempts: std::collections::BTreeMap<String, u8>,
    /// Bounded extra provider steps reserved only for closing the durable
    /// Agent-first artifact/submit/review transaction after generic safety
    /// heuristics would otherwise withdraw its required action tool.
    agentic_protocol_recovery_attempts: u8,
    /// Last Agent-first Program revision injected into this root model loop.
    /// The Program journal remains truth; this cursor prevents redundant
    /// prompt growth while allowing every real state change to wake the model
    /// with a compact actionable checkpoint.
    agentic_program_context_revision: Option<u64>,
    execution_role: TurnExecutionRole,
    bounded_evidence_role: bool,
    focus_novelty_target_bp: u16,
    focus_acceptance_scopes: Vec<String>,
    focus_acceptance_pending_scopes: Vec<String>,
    focus_required_output_fields: Vec<String>,
    structured_output_replans: u8,
    focus_observed_resource_scopes: BTreeSet<String>,
    focus_observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
    focus_action_rejections: u8,
    pending_focus_terminal_candidate: Option<String>,
    /// Number of discarded terminal answers that tried to conclude before a
    /// broad directory/network Focus had any governed source receipt.
    focus_discovery_replans: u8,
    /// Runtime can prefetch a reviewer's immutable upstream-change scopes
    /// once, without spending a provider request to rediscover exact paths.
    focus_verification_prefetched: bool,
    clean_terminal_synthesis_next: bool,
    clean_terminal_synthesis_attempted: bool,
    clean_terminal_retry_attempted: bool,
    /// Cached local or provider-authored explanation for a partial/blocked
    /// terminal. A local projection proves no narration call is permitted;
    /// provider output retains the exact observed attempt identity.
    terminal_failure_narration: Option<TerminalFailureNarration>,
    consecutive_tool_failure_batches: usize,
    /// Any turn may receive one Runtime-owned semantic replan after
    /// consecutive tool failures with no retained evidence. This is a bounded
    /// agency-preserving recovery, not a retry loop; once consumed, the next
    /// identical failure remains fail-closed and is surfaced as blocked. It
    /// covers the root turn too: a cold start is not a spin, and the root is
    /// the actor that has to drive the collaboration.
    cold_start_failure_replans: u8,
    consecutive_low_novelty_batches: usize,
    successful_tool_calls: usize,
    /// Normalized digest of the root's most recent terminal answer, and how
    /// many times it recurred with no intervening successful tool call.
    ///
    /// Run one19 emitted effectively the same final answer five times across
    /// 86 seconds and nothing in the Runtime noticed: `repetition_guard` keys
    /// on tool inputs and deterministic tool failures, so a model that stops
    /// calling tools and starts restating its conclusion is invisible to it.
    /// The turn then spent its whole remaining budget restating a conclusion it
    /// had already reached instead of taking the one action that closed the gap.
    /// These two fields are the observer for that failure mode.
    last_terminal_answer_digest: String,
    repeated_terminal_answers: u64,
    /// Whether the Goal verifier currently records a completion gap. Set by the
    /// completion-gap continuation so the model step can refuse to go tool-less
    /// while an action is still owed.
    completion_gap_outstanding: bool,
    /// The Program-owned rungs the closure view still advertises. Same purpose:
    /// a rung that names an action is an obligation to keep that action
    /// callable.
    outstanding_closure_rungs: Vec<String>,
    /// Count of committed success or failure tool receipts visible to later
    /// provider steps. Once non-zero, transport/protocol retry is forbidden:
    /// recovery may only synthesize once from retained evidence.
    tool_receipts_observed: usize,
    duplicate_tool_calls: u64,
    /// Consecutive identical tool batches (same tool set, canonical arguments
    /// and result). Drives the loop-breaker notice; any differing call or
    /// result resets it. Per-turn, like the other guardrail counters.
    identical_action_digest: String,
    identical_action_result_digest: String,
    identical_action_streak: usize,
    /// Per-turn call counts keyed by lower-case tool name, for the optional
    /// targeted runaway-tool caps (`COWD_TOOL_CAP_*`).
    per_turn_tool_counts: BTreeMap<String, usize>,
    write_attempt_paths: Vec<String>,
    /// Set only by a successful concrete workspace mutation. Authorization
    /// scopes and orchestration proposals describe capability, not evidence.
    committed_workspace_write_observed: bool,
    /// Canonical scopes from successful mutation receipts, including verified
    /// child-Team write paths. These close exact artifact obligations.
    committed_workspace_write_scopes: BTreeSet<String>,
    committed_workspace_observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
    /// Mutation epochs produced by already-completed ToolBatch nodes in this
    /// graph turn. The durable Goal observation is still the recovery source,
    /// but the next node can be scheduled before that observation's event
    /// transaction becomes visible to a fresh stream read. Keeping this
    /// Runtime-authored cursor in the turn state preserves the causal edge
    /// without trusting model claims or weakening the persisted boundary.
    workspace_mutation_effect_sequences: BTreeSet<u64>,
    max_tool_concurrency_observed: usize,
    parallel_tool_batches: usize,
    early_tool_receipts: BTreeMap<String, crate::conversation::EarlyToolExecutionReceipt>,
    evaluation_resource_scopes: Vec<String>,
    evaluation_judge_only: bool,
    collaboration_started: bool,
    /// A root collaboration requirement becomes durable with the next model
    /// node commit, before any proposal receipt can be consumed.
    pending_root_control_plane_requirement: Option<u8>,
    root_language_replan_attempted: bool,
    pending_terminal_artifact: Option<PendingTerminalArtifact>,
    /// Claims are staged by Synthesize and released only from `after_commit`,
    /// after the graph/terminal transaction has become durable.
    pending_controlled_recovery_claim_fingerprints: Vec<String>,
    pending_disposition_inputs: Vec<crate::session_input::SessionInputRecord>,
    /// PLAN D2b: the turn's generic terminal obligation. The bounded-Focus
    /// mechanism forces a terminal presentation only when a Focus declared its
    /// acceptance scopes; a long open-ended turn that keeps producing receipts
    /// has no such trigger, which is the "progress without committing" failure
    /// the Facet A run showed (367 observations, no terminal).
    terminal_tracker: crate::liveness::TerminalTracker,
    /// Causal arming time: when the turn first had enough committed receipts.
    /// Deliberately not the turn start, so a long turn is never penalised for
    /// taking a long time (PLAN I1).
    terminal_satisfaction_since: Option<std::time::Instant>,
}

/// A scope violation measured by the original turn state. This is distinct
/// from a policy Gap and is not a durable refusal receipt. It has no public
/// constructor or deserializer: only the turn owner can create it from its
/// own evaluation ceiling and the existing scope predicate.
#[derive(Debug, Serialize)]
pub(crate) struct EvaluationScopeRefusal {
    ticket: NodeExecutionTicket,
    request: crate::RuntimeToolExecutionRequest,
    allowed_scopes: Vec<String>,
    workspace_root: std::path::PathBuf,
    violation: String,
}

impl EvaluationScopeRefusal {
    pub(crate) fn ticket(&self) -> &NodeExecutionTicket { &self.ticket }
    pub(crate) fn request(&self) -> &crate::RuntimeToolExecutionRequest { &self.request }
    pub(crate) fn model_feedback(&self) -> String {
        evaluation_scope_feedback(&self.violation, &self.allowed_scopes)
    }
}

// The evaluation ceiling is useful guidance, not an authorization grant.
// Show only complete scope strings within a small budget; a truncated path
// could suggest a different scope. Policy and leases are still checked later.
fn evaluation_scope_feedback(violation: &str, scopes: &[String]) -> String {
    let mut shown = Vec::new();
    let mut bytes = 0;
    for scope in scopes {
        let encoded = serde_json::to_string(scope).expect("string serialization cannot fail");
        if bytes + encoded.len() > 180 || shown.len() == 4 {
            break;
        }
        bytes += encoded.len();
        shown.push(encoded);
    }
    let omitted = scopes.len() - shown.len();
    let violation: String = violation.chars().scan(0, |bytes, c| {
        *bytes += c.len_utf8();
        (*bytes <= 100).then_some(c)
    }).collect();
    format!(
        "evaluation_scope_exceeded: {violation}; scope ceiling: [{}] ({omitted} omitted). Request an exact path within the ceiling; policy and lease still apply.",
        shown.join(", ")
    )
}

/// Scope refusal measured by the original root turn owner; this is not a
/// receipt until the existing commit service wins the call/effect CAS.
#[derive(Debug, Serialize)]
pub(crate) struct EvaluationRootScopeRefusal {
    context: crate::CowdExecutionContext,
    activity: harness_contract::projection::RuntimeActivityBinding,
    attempt: u32,
    #[serde(skip)]
    original: crate::conversation::OriginalRootCall,
    original_call: serde_json::Value,
    request: crate::RuntimeToolExecutionRequest,
    allowed_scopes: Vec<String>,
    workspace_root: std::path::PathBuf,
    violation: String,
}

impl EvaluationRootScopeRefusal {
    pub(crate) fn context(&self) -> &crate::CowdExecutionContext { &self.context }
    pub(crate) fn activity(&self) -> &harness_contract::projection::RuntimeActivityBinding { &self.activity }
    pub(crate) fn attempt(&self) -> u32 { self.attempt }
    pub(crate) fn original(&self) -> &crate::conversation::OriginalRootCall { &self.original }
    pub(crate) fn request(&self) -> &crate::RuntimeToolExecutionRequest { &self.request }
}

/// One declared scope of a real tool effect, decided by the evaluation
/// ceiling.
#[derive(Debug, Clone, PartialEq, Eq)]
enum EvaluationScopeRequest {
    /// Expressible in this ceiling's vocabulary, so it is compared with it.
    Compared(String),
    /// Outside this ceiling's vocabulary. The kind is named so it cannot
    /// disappear without a reason: this ceiling is a workspace/network
    /// ceiling, and every other kind is held by the tool's own authorization
    /// lease instead - a descriptor scope the lease does not carry is
    /// refused there (`crates/tools/src/host.rs`).
    NotRepresented(String),
}

/// What one descriptor scope assessment decided.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EvaluationScopeOutcome {
    /// The first scope this ceiling does not authorize. It names every scope
    /// kind the ceiling could not represent, so a refusal always says which
    /// resource kinds were involved.
    violation: Option<String>,
    /// The scope kinds this ceiling does not represent, in encounter order.
    not_represented: Vec<String>,
}

/// Check every real descriptor scope using the same ceiling matcher as the
/// original graph scope predicate. Unrepresentable effects remain unknown,
/// and every scope kind this ceiling cannot represent is named.
fn evaluation_descriptor_scope_violation(
    allowed: &[String], effect: &harness_contract::tool::ToolEffectDescriptor, workspace_root: &Path,
) -> Result<EvaluationScopeOutcome, RuntimeError> {
    let mut violation = None;
    let mut not_represented: Vec<String> = Vec::new();
    if !allowed.is_empty() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        if effect.effect_kind == harness_contract::tool::ToolEffectKind::Unknown {
            return Err(RuntimeError::new("scope assessment unresolved for original call"));
        }
        let mut saw_network = false;
        for scope in &effect.scopes {
            let request = match scope.resource {
                PermissionResource::File => {
                    let mode = match scope.operation {
                        PermissionOperation::Read => "read",
                        PermissionOperation::Write | PermissionOperation::Delete => "write",
                        _ => return Err(RuntimeError::new(format!(
                            "scope assessment unresolved file operation {:?}", scope.operation))),
                    };
                    let target = scope.target.as_deref().filter(|value| !value.is_empty())
                        .ok_or_else(|| RuntimeError::new("scope assessment unresolved file target"))?;
                    let target = Path::new(target);
                    let relative = if target.is_absolute() {
                        target.strip_prefix(workspace_root)
                            .map_err(|_| RuntimeError::new("scope assessment file outside workspace"))?
                    } else { target };
                    let relative = if relative.as_os_str().is_empty() { ".".into() }
                        else { relative.to_string_lossy().replace('\\', "/") };
                    let value = format!("{mode}:{relative}");
                    if normalize_workspace_scope(&value).is_none() {
                        return Err(RuntimeError::new("scope assessment unresolved file target"));
                    }
                    EvaluationScopeRequest::Compared(value)
                }
                PermissionResource::Network => {
                    saw_network = true;
                    EvaluationScopeRequest::Compared("network:*".to_string())
                }
                // The remaining kinds are outside this workspace/network
                // ceiling, and each one is named here instead of being
                // skipped in silence.
                PermissionResource::Shell => EvaluationScopeRequest::NotRepresented("Shell".into()),
                PermissionResource::Provider => EvaluationScopeRequest::NotRepresented("Provider".into()),
                PermissionResource::Connector => EvaluationScopeRequest::NotRepresented("Connector".into()),
                PermissionResource::Channel => EvaluationScopeRequest::NotRepresented("Channel".into()),
                PermissionResource::Tool => EvaluationScopeRequest::NotRepresented("Tool".into()),
                PermissionResource::Memory => EvaluationScopeRequest::NotRepresented("Memory".into()),
                PermissionResource::Matrix => EvaluationScopeRequest::NotRepresented("Matrix".into()),
                PermissionResource::Session => EvaluationScopeRequest::NotRepresented("Session".into()),
                PermissionResource::Task => EvaluationScopeRequest::NotRepresented("Task".into()),
                PermissionResource::Approval => EvaluationScopeRequest::NotRepresented("Approval".into()),
                PermissionResource::Config => EvaluationScopeRequest::NotRepresented("Config".into()),
            };
            match request {
                EvaluationScopeRequest::Compared(requested) => {
                    if violation.is_none() && !allowed.iter()
                        .any(|allowed| evaluation_scope_authorizes(allowed, &requested)) {
                        violation = Some(requested);
                    }
                }
                EvaluationScopeRequest::NotRepresented(kind) => {
                    if !not_represented.iter().any(|seen| seen == &kind) {
                        not_represented.push(kind);
                    }
                }
            }
        }
        if effect.uses_network && !saw_network {
            return Err(RuntimeError::new("scope assessment unresolved network effect"));
        }
    }
    Ok(EvaluationScopeOutcome { violation, not_represented })
}

/// The refusal reason for one descriptor scope assessment. A refusal names the
/// resource kinds this ceiling could not represent, so no kind disappears
/// from the record without a reason.
fn evaluation_scope_violation_reason(outcome: EvaluationScopeOutcome) -> Option<String> {
    let violation = outcome.violation?;
    if outcome.not_represented.is_empty() { return Some(violation); }
    Some(format!(
        "{violation}; scope kinds not represented by this ceiling: {}", outcome.not_represented.join(", ")))
}

#[cfg(test)]
mod evaluation_descriptor_scope_tests {
    use super::*;
    use harness_contract::policy::{PermissionOperation as Op, PermissionResource as Resource, PermissionScope};
    use harness_contract::tool::{ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency, ToolPermissionMode};

    #[test]
    fn recursive_glob_requires_workspace_read_ceiling() {
        let root = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("recursive-glob-scope", root.path());
        let effect = host.pin_snapshot().describe_effect("glob_search", &serde_json::json!({
            "path":"crates/runtime", "pattern":"**/*.rs"
        }));
        assert_eq!(evaluation_descriptor_scope_violation(&["read:.".into()], &effect, root.path()).unwrap().violation, None);
        assert_eq!(evaluation_descriptor_scope_violation(&["read:crates/runtime".into()], &effect, root.path()).unwrap().violation.as_deref(), Some("read:."));
        let nested = host.pin_snapshot().describe_effect("glob_search", &serde_json::json!({
            "path":"crates", "pattern":"*/src/*.rs"
        }));
        assert_eq!(evaluation_descriptor_scope_violation(&["read:.".into()], &nested, root.path()).unwrap().violation, None);
        assert_eq!(evaluation_descriptor_scope_violation(&["read:crates".into()], &nested, root.path()).unwrap().violation.as_deref(), Some("read:."));
        let grep = host.pin_snapshot().describe_effect("grep_search", &serde_json::json!({
            "searches":[{"pattern":"a"},{"path":null,"pattern":"b"}]
        }));
        assert_eq!(evaluation_descriptor_scope_violation(&["read:.".into()], &grep, root.path()).unwrap().violation, None);
        assert_eq!(evaluation_descriptor_scope_violation(&["read:crates".into()], &grep, root.path()).unwrap().violation.as_deref(), Some("read:."));
        #[cfg(unix)]
        {
            std::fs::create_dir(root.path().join("other")).unwrap();
            std::os::unix::fs::symlink("other", root.path().join("alias")).unwrap();
            let alias_effect = host.pin_snapshot().describe_effect("glob_search", &serde_json::json!({
                "path":"alias", "pattern":"**/*.rs"
            }));
            assert_eq!(evaluation_descriptor_scope_violation(&["read:alias".into()], &alias_effect, root.path()).unwrap().violation.as_deref(), Some("read:."));
            let nested_alias = host.pin_snapshot().describe_effect("glob_search", &serde_json::json!({
                "path":"alias", "pattern":"*/src/*.rs"
            }));
            assert_eq!(evaluation_descriptor_scope_violation(&["read:alias".into()], &nested_alias, root.path()).unwrap().violation.as_deref(), Some("read:."));
        }
    }

    #[test]
    fn composite_descriptor_cannot_hide_other_file_or_network_effects() {
        let mut effect = ToolEffectDescriptor {
            tool_id:"composite".into(), descriptor_hash:"fixture".into(), effect_kind:ToolEffectKind::Read,
            idempotency:ToolIdempotency::Idempotent, scopes:vec![
                PermissionScope { resource:Resource::File, operation:Op::Read, target:Some("a.txt".into()) },
                PermissionScope { resource:Resource::File, operation:Op::Read, target:Some("b.txt".into()) },
            ], required_permission:ToolPermissionMode::ReadOnly, approval_class:ToolApprovalClass::None,
            uses_network:false, spawns_process:false, mutates_packages:false, mutates_system:false,
            assessment:Default::default(),
        };
        let allowed = vec!["read:a.txt".to_string()];
        assert_eq!(evaluation_descriptor_scope_violation(&allowed, &effect, Path::new("/work")).unwrap().violation.as_deref(), Some("read:b.txt"));
        effect.scopes.pop();
        effect.scopes.push(PermissionScope { resource:Resource::Network, operation:Op::Send, target:None });
        effect.uses_network = true;
        assert_eq!(evaluation_descriptor_scope_violation(&allowed, &effect, Path::new("/work")).unwrap().violation.as_deref(), Some("network:*"));
        let allowed_network = vec!["read:a.txt".to_string(), "network:*".to_string()];
        assert_eq!(evaluation_descriptor_scope_violation(&allowed_network, &effect, Path::new("/work")).unwrap().violation, None);
        effect.scopes.pop();
        assert!(evaluation_descriptor_scope_violation(&allowed_network, &effect, Path::new("/work")).is_err());
        effect.uses_network = false;
        effect.scopes[0].target = None;
        assert!(evaluation_descriptor_scope_violation(&allowed_network, &effect, Path::new("/work")).is_err());
    }

    fn scope_kind_descriptor(resource: Resource, operation: Op, target: Option<&str>) -> ToolEffectDescriptor {
        ToolEffectDescriptor {
            tool_id:"scope-kind".into(), descriptor_hash:"fixture".into(), effect_kind:ToolEffectKind::Read,
            idempotency:ToolIdempotency::Idempotent,
            scopes:vec![PermissionScope { resource, operation, target:target.map(str::to_string) }],
            required_permission:ToolPermissionMode::ReadOnly, approval_class:ToolApprovalClass::None,
            uses_network:false, spawns_process:false, mutates_packages:false, mutates_system:false,
            assessment:Default::default(),
        }
    }

    #[test]
    fn every_permission_resource_kind_outside_this_ceiling_is_named() {
        // The evaluation ceiling is a workspace/network ceiling. Every other
        // resource kind is held by the tool's own authorization lease, so this
        // gate must not refuse it - but it must name it instead of skipping it
        // in silence.
        let kinds = [
            (Resource::Shell, "Shell"), (Resource::Provider, "Provider"), (Resource::Connector, "Connector"),
            (Resource::Channel, "Channel"), (Resource::Tool, "Tool"), (Resource::Memory, "Memory"),
            (Resource::Matrix, "Matrix"), (Resource::Session, "Session"), (Resource::Task, "Task"),
            (Resource::Approval, "Approval"), (Resource::Config, "Config"),
        ];
        for (resource, name) in kinds {
            let effect = scope_kind_descriptor(resource, Op::Execute, Some("bash"));
            let outcome = evaluation_descriptor_scope_violation(
                &["read:.".to_string()], &effect, Path::new("/work")).unwrap();
            assert_eq!(outcome.not_represented, vec![name.to_string()],
                "{name} must be named by the scope assessment, never skipped");
            assert!(outcome.violation.is_none(),
                "{name} is not this ceiling's business and must not be refused here");
        }
    }

    #[test]
    fn the_kinds_this_ceiling_can_compare_are_not_reported_as_unrepresented() {
        let file = scope_kind_descriptor(Resource::File, Op::Read, Some("a.txt"));
        let outcome = evaluation_descriptor_scope_violation(
            &["read:.".to_string()], &file, Path::new("/work")).unwrap();
        assert!(outcome.not_represented.is_empty(), "File is compared by this ceiling");
        assert_eq!(outcome.violation, None);
        let network = scope_kind_descriptor(Resource::Network, Op::Send, None);
        let outcome = evaluation_descriptor_scope_violation(
            &["read:.".to_string()], &network, Path::new("/work")).unwrap();
        assert!(outcome.not_represented.is_empty(), "Network is compared by this ceiling");
        assert_eq!(outcome.violation.as_deref(), Some("network:*"));
    }

    #[test]
    fn a_refusal_names_the_scope_kinds_this_ceiling_cannot_represent() {
        let mut effect = scope_kind_descriptor(Resource::File, Op::Read, Some("a.txt"));
        effect.scopes.push(PermissionScope { resource:Resource::Shell, operation:Op::Execute, target:None });
        let outcome = evaluation_descriptor_scope_violation(
            &["read:other".to_string()], &effect, Path::new("/work")).unwrap();
        assert_eq!(outcome.not_represented, vec!["Shell".to_string()]);
        let violation = evaluation_scope_violation_reason(outcome)
            .expect("a file scope outside the ceiling must be refused");
        assert!(violation.starts_with("read:a.txt"),
            "the refusing scope stays the first unauthorized scope: {violation}");
        assert!(violation.contains("Shell"),
            "the refusal names the scope kind this ceiling cannot represent: {violation}");
    }

    #[test]
    fn an_unresolved_file_operation_names_the_operation() {
        let effect = scope_kind_descriptor(Resource::File, Op::Execute, Some("a.txt"));
        let error = evaluation_descriptor_scope_violation(
            &["read:.".to_string()], &effect, Path::new("/work")).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("Execute"),
            "the unresolved operation must be named: {message}");
    }
}

pub(super) struct RootModelUsageDelta {
    pub(super) usage: model_protocol::usage::TokenUsage,
    pub(super) model: Option<String>,
    pub(super) models_used: Vec<String>,
    pub(super) first_token_latency_ms: Option<u64>,
    pub(super) active_stream_duration_ms: Option<u64>,
    pub(super) wall_duration_ms: u64,
    pub(super) output_chars: u64,
    pub(super) output_chunks: u64,
    pub(super) cache_dimensions_known: bool,
}

fn add_root_provider_token_usage(
    input_tokens: &mut u64,
    output_tokens: &mut u64,
    cache_create_tokens: &mut u64,
    cache_read_tokens: &mut u64,
    usage: model_protocol::usage::TokenUsage,
) {
    *input_tokens = input_tokens.saturating_add(u64::from(usage.input_tokens));
    *output_tokens = output_tokens.saturating_add(u64::from(usage.output_tokens));
    *cache_create_tokens =
        cache_create_tokens.saturating_add(u64::from(usage.cache_creation_input_tokens));
    *cache_read_tokens = cache_read_tokens.saturating_add(u64::from(usage.cache_read_input_tokens));
}

fn merge_root_cache_dimensions_truth(
    attempt_observed: &mut bool,
    all_known: &mut bool,
    next_attempt_known: bool,
) {
    if *attempt_observed {
        *all_known &= next_attempt_known;
    } else {
        *all_known = next_attempt_known;
        *attempt_observed = true;
    }
}

impl TurnGraphState {
    pub(crate) fn evaluate_root_scope_refusal(
        &self,
        context: &crate::CowdExecutionContext,
        activity: &harness_contract::projection::RuntimeActivityBinding,
        attempt: u32,
        original: &crate::conversation::OriginalRootCall,
        request: &crate::RuntimeToolExecutionRequest,
        services: &crate::RuntimeServices,
    ) -> Result<Option<EvaluationRootScopeRefusal>, RuntimeError> {
        if attempt == 0 || context.session_id != self.session_id || context.turn_id != self.turn_id
            || !original.matches_owner(context, activity) || !original.matches_request(request)
            || !original.matches_workspace(services) {
            return Err(RuntimeError::new("root scope assessment original producer mismatch"));
        }
        let binding = request.step_binding.as_ref().ok_or_else(|| RuntimeError::new("root scope Step missing"))?;
        let invocation = request.compiled_invocation.as_ref().ok_or_else(|| RuntimeError::new("root scope invocation missing"))?;
        let call = ModelToolCall { binding:Some(binding.clone()), id:request.tool_use_id.clone(),
            name:request.tool_name.clone(), input:request.input.clone(), depends_on:Vec::new() };
        let workspace_root = services.workspace_root();
        let mut violation = evaluation_scope_violation(&self.evaluation_resource_scopes, &[call], workspace_root);
        let descriptor_violation = evaluation_scope_violation_reason(evaluation_descriptor_scope_violation(
            &self.evaluation_resource_scopes, &invocation.effect, workspace_root,
        )?);
        if violation.is_none() { violation = descriptor_violation; }
        Ok(violation.map(|violation| EvaluationRootScopeRefusal {
            context:context.clone(), activity:activity.clone(), attempt, original:original.clone(),
            original_call:original.binding_value(), request:request.clone(),
            allowed_scopes:self.evaluation_resource_scopes.clone(), workspace_root:workspace_root.to_path_buf(), violation,
        }))
    }


    pub(crate) fn has_evaluation_scope(&self) -> bool {
        !self.evaluation_resource_scopes.is_empty()
    }
    /// Assess the effective input once against this turn's actual ceiling.
    /// The commit owner still has to authenticate the ticket and fence this
    /// observation against admission before it may become a refusal receipt.
    pub(crate) fn evaluate_scope_refusal(
        &self,
        ticket: &NodeExecutionTicket,
        request: &crate::RuntimeToolExecutionRequest,
        services: &crate::RuntimeServices,
        prepared_effect: &crate::conversation::PreparedCallEffect,
    ) -> Result<Option<EvaluationScopeRefusal>, RuntimeError> {
        let workspace_root = services.workspace_root();
        let invocation = request.compiled_invocation.as_ref()
            .ok_or_else(|| RuntimeError::new("scope assessment invocation missing"))?;
        let effective = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let original_matches = if let Some(step) = request.step_binding.as_ref() {
            step.snapshot.identity.session_id == self.session_id
                && step.snapshot.identity.turn_id == self.turn_id
                && step.origin.call_id == request.tool_use_id
                && step.origin.origin_step_id == step.snapshot.identity.step_id
                && step.effect_idempotency_key() == request.idempotency_key
        } else {
            request.tool_progress.original_governed_hook().map_err(RuntimeError::new)?
                .is_some_and(|owner| owner.original_session_id() == self.session_id
                    && (owner.original_prepared_lease(ticket, request).is_ok()
                        || owner.matches_native_preparation(ticket, request)))
        };
        if !prepared_effect.matches(ticket, request) || !prepared_effect.matches_workspace(services)
            || request.session_id.as_deref() != Some(self.session_id.as_str()) || !original_matches
            || invocation.effect.tool_id != request.tool_name || effective != invocation.intent.normalized_input {
            return Err(RuntimeError::new("scope assessment original call mismatch"));
        }
        let call = ModelToolCall {
            binding: request.step_binding.clone(), id: request.tool_use_id.clone(),
            name: request.tool_name.clone(), input: request.input.clone(), depends_on: Vec::new(),
        };
        let mut violation = evaluation_scope_violation(&self.evaluation_resource_scopes, &[call], workspace_root);
        let descriptor_violation = evaluation_scope_violation_reason(evaluation_descriptor_scope_violation(
            &self.evaluation_resource_scopes, &invocation.effect, workspace_root,
        )?);
        if violation.is_none() { violation = descriptor_violation; }
        Ok(violation
            .map(|violation| EvaluationScopeRefusal {
                ticket: ticket.clone(), request: request.clone(),
                allowed_scopes: self.evaluation_resource_scopes.clone(),
                workspace_root: workspace_root.to_path_buf(), violation,
            }))
    }

    pub(super) fn record_root_model_usage(&mut self, delta: RootModelUsageDelta) {
        add_root_provider_token_usage(
            &mut self.input_tokens,
            &mut self.output_tokens,
            &mut self.cache_create_tokens,
            &mut self.cache_read_tokens,
            delta.usage,
        );
        merge_root_cache_dimensions_truth(
            &mut self.provider_usage_attempt_observed,
            &mut self.cache_dimensions_known,
            delta.cache_dimensions_known,
        );
        self.output_chars = self.output_chars.saturating_add(delta.output_chars);
        self.output_chunks = self.output_chunks.saturating_add(delta.output_chunks);
        self.wall_duration_ms = self.wall_duration_ms.saturating_add(delta.wall_duration_ms);
        if delta.model.is_some() {
            self.model = delta.model;
        }
        for model in delta.models_used {
            if !self.models_used.contains(&model) {
                self.models_used.push(model);
            }
        }
        if self.first_token_latency_ms.is_none() {
            self.first_token_latency_ms = delta.first_token_latency_ms;
        }
        self.active_stream_duration_ms = self
            .active_stream_duration_ms
            .saturating_add(delta.active_stream_duration_ms.unwrap_or_default());
    }

    pub(super) fn root_model_telemetry(&self) -> crate::RunModelTelemetry {
        let rate = |value: u64, duration_ms: u64| {
            (duration_ms > 0).then(|| value as f64 * 1_000.0 / duration_ms as f64)
        };
        crate::RunModelTelemetry {
            model: self.model.clone(),
            models_used: self.models_used.clone(),
            first_token_latency_ms: self.first_token_latency_ms,
            active_stream_duration_ms: Some(self.active_stream_duration_ms.max(1)),
            wall_duration_ms: self.wall_duration_ms.max(1),
            output_chars: self.output_chars,
            output_chunks: self.output_chunks,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_create_tokens: self.cache_create_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_dimensions_known: self.cache_dimensions_known,
            total_tokens: self
                .input_tokens
                .saturating_add(self.output_tokens)
                .saturating_add(self.cache_create_tokens)
                .saturating_add(self.cache_read_tokens),
            usage_source: "provider".to_string(),
            wall_chars_per_second: rate(self.output_chars, self.wall_duration_ms),
            wall_tokens_per_second: rate(self.output_tokens, self.wall_duration_ms),
            active_chars_per_second: rate(self.output_chars, self.active_stream_duration_ms),
            active_tokens_per_second: rate(self.output_tokens, self.active_stream_duration_ms),
            chars_per_second: rate(self.output_chars, self.wall_duration_ms),
            tokens_per_second: rate(self.output_tokens, self.wall_duration_ms),
        }
    }
}

#[cfg(test)]
mod root_model_usage_tests {
    use super::*;
    use model_protocol::usage::TokenUsage;

    fn usage(input: u32, output: u32, cache_create: u32, cache_read: u32) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            cache_creation_input_tokens: cache_create,
            cache_read_input_tokens: cache_read,
        }
    }

    fn accumulate(usages: &[TokenUsage]) -> (u64, u64, u64, u64) {
        let (mut input, mut output, mut cache_create, mut cache_read) = (0, 0, 0, 0);
        for usage in usages {
            add_root_provider_token_usage(
                &mut input,
                &mut output,
                &mut cache_create,
                &mut cache_read,
                *usage,
            );
        }
        (input, output, cache_create, cache_read)
    }

    #[test]
    fn normal_team_synthesis_is_added_to_the_cumulative_root_bill() {
        assert_eq!(accumulate(&[usage(100, 20, 30, 50)]), (100, 20, 30, 50));
    }

    #[test]
    fn every_hierarchical_team_synthesis_call_is_added_once() {
        assert_eq!(
            accumulate(&[
                usage(100, 10, 20, 30),
                usage(110, 11, 21, 31),
                usage(120, 12, 22, 32),
            ]),
            (330, 33, 63, 93)
        );
    }

    #[test]
    fn failed_terminal_narrator_usage_remains_in_fallback_bill() {
        assert_eq!(
            accumulate(&[usage(90, 0, 15, 45), usage(70, 0, 10, 35)]),
            (160, 0, 25, 80)
        );
    }

    #[test]
    fn recovered_usage_and_post_restart_narrator_retries_share_one_cumulative_bill() {
        let recovered = crate::conversation::RecoveredProviderUsage {
            input_tokens: 300,
            output_tokens: 30,
            cache_creation_input_tokens: 40,
            cache_read_input_tokens: 50,
        };
        let (mut input, mut output, mut cache_create, mut cache_read) = (
            recovered.input_tokens,
            recovered.output_tokens,
            recovered.cache_creation_input_tokens,
            recovered.cache_read_input_tokens,
        );
        for retry in [usage(90, 0, 15, 45), usage(70, 7, 10, 35)] {
            add_root_provider_token_usage(
                &mut input,
                &mut output,
                &mut cache_create,
                &mut cache_read,
                retry,
            );
        }
        assert_eq!(
            (input, output, cache_create, cache_read),
            (460, 37, 65, 130)
        );

        let mut attempt_observed = true;
        let mut all_known = true;
        merge_root_cache_dimensions_truth(&mut attempt_observed, &mut all_known, true);
        merge_root_cache_dimensions_truth(&mut attempt_observed, &mut all_known, false);
        assert!(
            !all_known,
            "one unknown retry must keep terminal cache truth unknown"
        );
    }

    #[test]
    fn cached_terminal_narration_preserves_provider_attribution() {
        let cached = TerminalFailureNarration::Provider {
            answer: "answer".to_string(),
            model: Some("model-a".to_string()),
            models_used: vec!["model-a".to_string(), "model-b".to_string()],
            attempt_id: "attempt-1".to_string(),
        };
        let TerminalFailureNarration::Provider {
            answer,
            model,
            models_used,
            attempt_id,
        } = cached.clone()
        else {
            panic!("provider narration must remain provider-attributed");
        };
        assert_eq!(answer, "answer");
        assert_eq!(model.as_deref(), Some("model-a"));
        assert_eq!(models_used, ["model-a", "model-b"]);
        assert_eq!(attempt_id, "attempt-1");
    }
}

#[derive(Clone)]
enum TerminalFailureNarration {
    Local(String),
    Provider {
        answer: String,
        model: Option<String>,
        models_used: Vec<String>,
        attempt_id: String,
    },
}

fn model_context_for_step(
    mut pending: Vec<ContextItem>,
    persistent: &[ContextItem],
) -> Vec<ContextItem> {
    for item in persistent {
        if pending.iter().all(|candidate| candidate.id != item.id) {
            pending.push(item.clone());
        }
    }
    pending
}

/// The input-proposal boundary used by actual Host admission before Goal
/// creation. It never converts failure to guess a path into turn rejection.
fn prepare_goal_input_for_turn(
    goal: &GoalContract,
    workspace: &Path,
    content: &str,
    input_revision: u64,
) -> Result<harness_contract::goal::GoalInputContract, RuntimeError> {
    let scopes = required_workspace_write_scopes_for_turn(workspace, content, content);
    crate::execution_core::goal::compile_goal_input_contract(
        goal.execution_binding
            .as_ref()
            .ok_or_else(|| RuntimeError::new("Goal input binding absent"))?,
        goal.source_intent_ref
            .as_deref()
            .ok_or_else(|| RuntimeError::new("Goal input source absent"))?,
        input_revision,
        content,
        &scopes,
    )
    .map_err(RuntimeError::new)
}

#[cfg(test)]
#[test]
fn host_admission_keeps_autonomous_filename_and_distinct_object_proposals_unresolved() {
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir(workspace.path().join("src")).unwrap();
    std::fs::write(workspace.path().join("src/a.rs"), "a").unwrap();
    std::fs::write(workspace.path().join("src/b.rs"), "b").unwrap();
    for input in [
        "Create a summary file; choose a suitable filename yourself.",
        "不要修改src/a.rs；请修改src/b.rs",
    ] {
        let goals =
            crate::execution_core::GoalStore::new(Arc::new(crate::RuntimeEventStore::for_test()));
        let mut goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":"goal:host-input", "session_id":"host-session", "objective":input,
            "criteria":[{"id":"user_intent", "statement":input, "source_refs":["session_message:host-message"], "required_evidence":[], "status":"open"}],
            "phase":"execution", "completion":"open", "revision":1, "user_sequence":1,
            "scope":"user_objective", "source_intent_ref":"session_message:host-message",
            "user_intent_criterion_id":"user_intent", "spec_revision":1, "spec_digest":"host-source-spec",
            "execution_binding":{"objective_id":"host-objective","session_id":"host-session","turn_id":"host-turn", "root_execution_id":"host-graph", "agentic_program_id":"host-program"}
        })).unwrap();
        goal.input_contract =
            Some(prepare_goal_input_for_turn(&goal, workspace.path(), input, 1).unwrap());
        let proposal = goal.input_contract.as_ref().unwrap();
        assert!(
            proposal.commitments.is_empty(),
            "path guesses cannot make global denies or write commitments"
        );
        assert!(!proposal.forbids_workspace_write());
        assert_eq!(proposal.principal_id, "session:host-session");
        assert_eq!(proposal.source_ref, "session_message:host-message");
        // Source identity remains enforced by the original durable Goal owner.
        // This is not a claim of model completion or semantic authorization.
        let mut forged = goal.clone();
        forged.input_contract.as_mut().unwrap().principal_id = "session:foreign".into();
        assert!(
            matches!(goals.create(forged), Err(ref reason) if reason == "goal_input_source_binding_mismatch")
        );
        assert!(goals.get(&goal.id).unwrap().is_none());
        let admitted = goals.create(goal).unwrap();
        assert_eq!(admitted.completion, GoalCompletion::Open);
        assert_eq!(admitted.objective, input);
        assert!(admitted.input_contract.unwrap().commitments.is_empty());
    }
}

fn required_workspace_write_scopes_for_turn(
    workspace_root: &std::path::Path,
    current_input: &str,
    resolved_objective: &str,
) -> Vec<String> {
    let extract = |objective: &str| {
        crate::workspace_scopes::explicit_workspace_resource_scopes(workspace_root, objective, true)
            .into_iter()
            .filter(|scope| scope.starts_with("write:"))
            .collect::<Vec<_>>()
    };
    let current = extract(current_input);
    if current.is_empty() {
        extract(resolved_objective)
    } else {
        current
    }
}

struct PendingTerminalArtifact {
    artifact: harness_contract::context::ArtifactRef,
    staging_owner: String,
    durable_owner: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedModelToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    origin: Option<harness_contract::step::CallOrigin>,
    id: String,
    name: String,
    input: String,
    depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedToolBatch {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    step_snapshots: BTreeMap<String, Arc<harness_contract::step::StepSnapshot>>,
    session_id: String,
    calls: Vec<PersistedModelToolCall>,
    /// A subsequent ToolBatch is already present in the same graph. This
    /// batch must commit its evidence and let Runner advance that successor
    /// instead of creating an intervening model node.
    #[serde(default)]
    continue_with_tool_batch: bool,
}

fn encode_tool_calls_with_continuation(
    session_id: &str,
    calls: &[ModelToolCall],
    continue_with_tool_batch: bool,
) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(&PersistedToolBatch {
        step_snapshots: calls
            .iter()
            .filter_map(|call| call.binding.as_ref())
            .map(|binding| {
                (
                    binding.snapshot.identity.step_id.clone(),
                    Arc::clone(&binding.snapshot),
                )
            })
            .collect(),
        session_id: session_id.to_string(),
        calls: calls
            .iter()
            .map(|call| PersistedModelToolCall {
                origin: call.binding.as_ref().map(|binding| binding.origin.clone()),
                id: call.id.clone(),
                name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect(),
        continue_with_tool_batch,
    })?;
    crate::step_snapshot::encode_persisted_tool_batch(&value)
}

pub(crate) fn decode_tool_batch(
    payload: &str,
) -> Result<(Vec<ModelToolCall>, bool), serde_json::Error> {
    let value = crate::step_snapshot::decode_persisted_tool_batch_value(payload)?;
    let batch = serde_json::from_value::<PersistedToolBatch>(value)?;
    let calls = batch
        .calls
        .into_iter()
        .map(|call| {
            let binding = call
                .origin
                .map(|origin| {
                    let snapshot = batch
                        .step_snapshots
                        .get(&origin.origin_step_id)
                        .ok_or_else(|| {
                            serde_json::Error::io(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "persisted tool origin has no step snapshot",
                            ))
                        })?;
                    Ok::<_, serde_json::Error>(crate::step_snapshot::StepCallBinding::persisted(
                        origin,
                        Arc::clone(snapshot),
                    ))
                })
                .transpose()?;
            Ok(ModelToolCall {
                binding,
                id: call.id,
                name: call.name,
                input: call.input,
                depends_on: call.depends_on,
            })
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()?;
    Ok((calls, batch.continue_with_tool_batch))
}

fn ticket_session_id(payload: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    value
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .get("ingress")
                .and_then(|ingress| ingress.get("session_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

fn turn_scope_matches(ticket: &NodeExecutionTicket, session_id: &str, graph_id: &str) -> bool {
    ticket.graph_id == graph_id
        && ticket_session_id(&ticket.payload_ref).as_deref() == Some(session_id)
}

struct TurnModelResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    graph_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
}

impl<C, T> crate::execution_core::graph::executors::ScopedNodeBackendResolver
    for TurnModelResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if !turn_scope_matches(ticket, &self.session_id, &self.graph_id) {
            return None;
        }
        Some(Arc::new(TurnModelStepBackend {
            ticket: ticket.clone(),
            stop: crate::CancellationToken::new(),
            local_fence: Mutex::new(None),
            runtime: self.runtime.upgrade()?,
            state: self.state.upgrade()?,
            services: self.services.upgrade()?,
        }))
    }
}

struct TurnToolResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    graph_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
}

impl<C, T> crate::execution_core::graph::executors::ScopedNodeBackendResolver
    for TurnToolResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
        if !turn_scope_matches(ticket, &self.session_id, &self.graph_id) {
            return None;
        }
        self.services.upgrade()?;
        Some(Arc::new(TurnToolBatchBackend {
            runtime: self.runtime.upgrade()?,
            state: self.state.upgrade()?,
            services: self.services.clone(),
            goal_revalidation: std::sync::Mutex::new(None),
        }))
    }
}

struct TurnSynthesizeResolver<C: ApiClient, T: ToolExecutor> {
    session_id: String,
    graph_id: String,
    runtime: std::sync::Weak<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
}

impl<C, T> crate::execution_core::graph::executors::SynthesizeBackendResolver
    for TurnSynthesizeResolver<C, T>
where
    C: ApiClient + Clone + Send + Sync + 'static,
    T: ToolExecutor,
{
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn crate::execution_core::graph::executors::SynthesizeBackend>> {
        if !turn_scope_matches(ticket, &self.session_id, &self.graph_id) {
            return None;
        }
        Some(Arc::new(TurnSynthesizeBackend {
            runtime: self.runtime.upgrade()?,
            state: self.state.upgrade()?,
            services: self.services.upgrade()?,
        }))
    }
}

struct TurnModelStepBackend<C: ApiClient, T: ToolExecutor> {
    ticket: NodeExecutionTicket,
    stop: crate::CancellationToken,
    local_fence: Mutex<Option<crate::RuntimeError>>,
    runtime: Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    state: Arc<tokio::sync::Mutex<TurnGraphState>>,
    services: Arc<crate::RuntimeServices>,
}

// Test-only targeted hook: arm only the original store/session/call and use
// the existing transaction fault injector after the real transition is created.
#[cfg(test)]
fn early_transition_faults() -> &'static Mutex<BTreeMap<(usize, String, String), Arc<Mutex<Option<(String, String)>>>>> {
    static FAULTS: std::sync::OnceLock<Mutex<BTreeMap<(usize, String, String), Arc<Mutex<Option<(String, String)>>>>>> = std::sync::OnceLock::new();
    FAULTS.get_or_init(Default::default)
}

#[cfg(test)]
fn arm_early_transition_fault(store: &Arc<crate::RuntimeEventStore>, session: &str, call: &str)
    -> Arc<Mutex<Option<(String, String)>>> {
    let witness = Arc::new(Mutex::new(None));
    let key = (Arc::as_ptr(store) as usize, session.to_string(), call.to_string());
    assert!(early_transition_faults().lock().unwrap().insert(key, Arc::clone(&witness)).is_none());
    witness
}

struct HostEarlyToolDispatcher<T: ToolExecutor> {
    tool_executor: Arc<T>,
    services: Arc<crate::RuntimeServices>,
    event_bus: Option<crate::CowdEventBus>,
    ticket: NodeExecutionTicket,
    session_id: String,
    memory_context: memory::MemoryTurnContext,
    reality_context: Option<harness_contract::agent::AgentDataLease>,
    model_lease: Option<String>,
    observation_wave_sequence: u64,
    decision: crate::execution_core::RuntimeExecutionDecision,
    permission_policy: crate::PermissionPolicy,
    authorization_negotiator: crate::AuthorizationNegotiator,
    timeout: std::time::Duration,
    early_read_locks:
        Arc<tokio::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

fn early_tool_rejection_reason(
    call: &ModelToolCall,
    task: &crate::GovernedToolPlanTask,
    effect: &harness_contract::tool::ToolEffectDescriptor,
) -> Option<&'static str> {
    if !call.depends_on.is_empty() {
        return Some("declared_dependency_waits_for_finalized_dag");
    }
    if !task.can_parallelize
        || task.safety_category != crate::ToolSafetyCategory::ReadOnly
        || task.purity != crate::governed_tool_plan::ToolPurity::ReadOnlyIdempotent
        || task.resource_scope.unknown
        || task.output_budget_class != "normal"
        || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
        || effect.idempotency != harness_contract::tool::ToolIdempotency::Idempotent
        || effect.approval_class != harness_contract::tool::ToolApprovalClass::None
        || effect.uses_network
        || effect.spawns_process
        || effect.mutates_packages
        || effect.mutates_system
    {
        return Some("descriptor_not_early_safe");
    }
    None
}

fn early_tool_fingerprint(invocation: &harness_contract::tool::GovernedToolInvocation) -> String {
    sha256_digest(&format!(
        "{}\n{}",
        invocation.intent.tool_name,
        serde_json::to_string(&invocation.intent.normalized_input).unwrap_or_default()
    ))
}

impl<T: ToolExecutor> crate::conversation::EarlyToolDispatcher for HostEarlyToolDispatcher<T> {
    fn confirms_original_undispatched(
        &self,
        call_id: &str,
        deferred: &crate::conversation::EarlyToolDeferral,
    ) -> bool {
        host_backend::original_early_deferral_sources(
            &self.services, &self.ticket, call_id, deferred,
        ).ok().flatten().is_some()
    }

    fn dispatch(
        &self,
        candidate: crate::conversation::EarlyToolCandidate,
    ) -> crate::conversation::EarlyToolDispatchFuture {
        let tool_executor = Arc::clone(&self.tool_executor);
        let services = Arc::clone(&self.services);
        let event_bus = self.event_bus.clone();
        let ticket = self.ticket.clone();
        let session_id = self.session_id.clone();
        let memory_context = self.memory_context.clone();
        let reality_context = self.reality_context.clone();
        let model_lease = self.model_lease.clone();
        let decision = self.decision.clone();
        let permission_policy = self.permission_policy.clone();
        let authorization_negotiator = self.authorization_negotiator.clone();
        let timeout = self.timeout;
        let observation_wave_sequence = self.observation_wave_sequence;
        let early_read_locks = Arc::clone(&self.early_read_locks);
        let original_identity = candidate.identity.clone();
        let original_call_id = candidate.call.id.clone();
        let original_ticket = ticket.clone();
        let owner = Arc::clone(services.model_step_executor());
        let work: crate::conversation::EarlyToolDispatchFuture = Box::pin(async move {
            let original_authorization_events = Mutex::new(Vec::new());
            let defer = |reason: String| {
                crate::conversation::EarlyToolDispatchResult::Deferred(
                    crate::conversation::EarlyToolDeferral {
                        tool_call_id: candidate.call.id.clone(),
                        reason,
                        ready_at_ms: candidate.ready_at_ms,
                        original_undispatched: Some(Arc::new(crate::conversation::EarlyToolUndispatched {
                            ticket: ticket.clone(), call_id: candidate.call.id.clone(),
                            identity: candidate.identity.clone(),
                            authorization_events: original_authorization_events.lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
                        })),
                        origin: crate::conversation::EarlyToolDeferralOrigin::OriginalWorker,
                    },
                )
            };
            let request = crate::tool_dispatch::ToolRequest {
                tool_use_id: candidate.call.id.clone(),
                tool_name: candidate.call.name.clone(),
                input: candidate.call.input.clone(),
                depends_on: Vec::new(),
            };
            let request = match tool_executor.validate_step_tool_input(
                candidate.call.binding.as_ref(),
                &request.tool_name,
                &request.input,
            ) {
                Ok(repaired) => crate::tool_dispatch::ToolRequest {
                    input: repaired,
                    ..request
                },
                Err(error) => {
                    return defer(format!("input_contract_rejected:{error}"));
                }
            };
            if request.input != candidate.call.input {
                return defer("early_repaired_input_requires_original_tool_batch".into());
            }
            if crate::host::normalize_native_read_input(&request.tool_name, &request.input)
                != request.input
            {
                return defer("early_repaired_input_requires_original_tool_batch".into());
            }
            let original_executor: Arc<dyn crate::ToolExecutor> = tool_executor.clone();
            let plan_pin = original_executor.pin_step_catalog().map(Arc::new);
            let prepared = match tool_executor.prepare_step_invocations(
                std::slice::from_ref(&request),
                std::slice::from_ref(&candidate.call),
                plan_pin.as_ref(),
                &original_executor,
            ) {
                Ok(prepared) => prepared,
                Err(error) => return defer(format!("step_binding_rejected:{error}")),
            };
            let original_pins = prepared.into_iter().map(Arc::new).collect::<Vec<_>>();
            let prepared = original_pins.iter().map(|item| item.invocation().clone()).collect::<Vec<_>>();
            let Some(invocation) = prepared
                .iter()
                .find(|invocation| invocation.invocation_id == candidate.call.id)
                .cloned()
            else {
                return defer("registered_effect_descriptor_unavailable".to_string());
            };
            let mut plan = match crate::GovernedToolCompiler.compile_prepared_partial(
                services.workspace_root(),
                std::slice::from_ref(&request),
                &prepared,
                &Default::default(),
            ) {
                Ok(compilation) => match compilation.plan {
                    Some(plan) if compilation.rejected.is_empty() => plan,
                    _ => {
                        return defer(format!(
                            "governed_candidate_rejected:{:?}",
                            compilation.rejected
                        ))
                    }
                },
                Err(error) => return defer(format!("governed_candidate_rejected:{error}")),
            };
            crate::step_snapshot::bind_plan_calls(&mut plan, std::slice::from_ref(&candidate.call));
            for task in &mut plan.tasks {
                task.original_pin = original_pins.iter().find(|item| item.invocation().invocation_id == task.tool_call_id).cloned();
            }
            let task = &plan.tasks[0];
            let effect = &invocation.effect;
            // The early producer has no call-bound durable assessment. Defer
            // to the original ToolBatch owner rather than sign the model ticket.
            if harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&candidate.call.name.as_str()) {
                return defer("direct_action_requires_original_tool_batch_owner".to_string());
            }
            if let Some(reason) = early_tool_rejection_reason(&candidate.call, task, effect) {
                return defer(reason.to_string());
            }
            let validation = plan.validate_against_execution_decision(&decision);
            if !validation.allowed {
                return defer(format!(
                    "strategy_gate_not_satisfied:{}",
                    validation.findings.join(",")
                ));
            }
            let authorization_id = format!(
                "{}:{}:early",
                session_id,
                candidate
                    .identity
                    .tool_call_id
                    .as_deref()
                    .unwrap_or(&candidate.call.id)
            );
            let early_execution_policy = permission_policy.execution_policy_control().snapshot();
            let early_permission_policy =
                permission_policy.bound_to_snapshot(&early_execution_policy);
            let evaluated = authorization_negotiator.assess_effective(
                &early_permission_policy,
                &crate::AuthorizationRequest {
                    principal_id: format!("session:{session_id}"),
                    capability: effect.tool_id.clone(),
                    input: candidate.call.input.clone(),
                    idempotency_key: authorization_id.clone(),
                    effect: effect.clone(),
                    parent_ceiling: crate::PermissionMode::DangerFullAccess,
                    parent_lease_id: None,
                    policy_revision: early_execution_policy.revision,
                    recovery_scope: format!("execution:{}", ticket.graph_id),
                    context: crate::PermissionContext::default(),
                    safe_alternatives: Vec::new(),
                },
            );
            let assessment = evaluated.assessment;
            if let Some(bus) = event_bus.as_ref() {
                bus.emit(CowdEvent::CapabilityAssessed {
                    assessment: assessment.clone(),
                });
            }
            let _ = authorization_negotiator.take_transitions_for_persistence();
            for transition in authorization_negotiator.transitions_awaiting_persistence() {
                // Per-lease stream: parallel agents and the parent model stream
                // must not contend for the shared session event stream.
                let authorization_stream_id =
                    format!("authorization-lease:{}", transition.lease.lease_id);
                #[cfg(test)]
                if assessment.lease.as_ref().is_some_and(|lease| lease.lease_id == transition.lease.lease_id) {
                    let key = (Arc::as_ptr(services.event_store()) as usize, session_id.clone(), candidate.call.id.clone());
                    if let Some(witness) = early_transition_faults().lock().unwrap().remove(&key) {
                        let transaction_id = format!("authorization-transition:{}", transition.transition_id);
                        *witness.lock().unwrap() = Some((transaction_id.clone(), transition.lease.lease_id.clone()));
                        services.event_store().inject_append_fault_once(transaction_id, false);
                    }
                }
                let original_event = match
                    crate::authorization_negotiator::persist_authorization_transition_event(
                        services.event_store(),
                        &authorization_stream_id,
                        "conversation_runtime.early_tool",
                        &transition,
                    ) {
                    Ok(event) => event,
                    Err(error) => {
                    // The append may have committed without an acknowledgement.
                    // Stop this original call before consuming its lease; do
                    // not turn an infrastructure error into a policy refusal.
                    return crate::conversation::EarlyToolDispatchResult::Failed(
                        crate::conversation::EarlyToolFailure {
                            identity: candidate.identity.clone(),
                            tool_call_id: candidate.call.id.clone(),
                            owner_ticket: Some(ticket.clone()),
                            kind: crate::conversation::EarlyToolFailureKind::AuthorizationCommitUnknown,
                            detail: format!("transition {}: {error}", transition.transition_id),
                        },
                    );
                    }
                };
                original_authorization_events.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(original_event);
                if authorization_negotiator.acknowledge_persisted_transitions(std::slice::from_ref(
                    &transition.transition_id,
                )) == 1
                {
                    if let Some(bus) = event_bus.as_ref() {
                        bus.emit(CowdEvent::AuthorizationLeaseTransition { transition });
                    }
                }
            }
            let Some(lease) = assessment.lease.clone() else {
                return defer(format!(
                    "capability_gap:{}",
                    assessment
                        .gap
                        .as_ref()
                        .map_or("authorization unavailable", |gap| gap.reason.as_str())
                ));
            };
            let authorization = match crate::ToolPolicy.authorize(
                &evaluated.effective,
                &assessment,
                authorization_id,
                lease,
                timeout.as_secs(),
            ) {
                Ok(authorization) if authorization.parallel_safe => authorization.authorization,
                Ok(_) => return defer("tool_policy_not_parallel_safe".to_string()),
                Err(error) => return defer(format!("tool_policy_denied:{error}")),
            };
            let Some(step) = candidate.call.binding.as_ref() else {
                return defer("early_original_plan_step_missing".into());
            };
            let Some(context_identity) = event_bus.as_ref().and_then(crate::CowdEventBus::current_execution_context) else {
                return defer("early_original_plan_context_missing".into());
            };
            if context_identity.session_id != session_id || context_identity.session_id != step.snapshot.identity.session_id
                || context_identity.turn_id != step.snapshot.identity.turn_id
                || step.snapshot.identity.execution_id.as_deref() != Some(context_identity.execution_id.as_str())
                || match step.snapshot.identity.run_ref.as_deref() {
                    Some(run) => run != context_identity.execution_id.as_str(),
                    None => context_identity.execution_id != ticket.graph_id,
                } {
                return defer("early_original_plan_context_mismatch".into());
            }
            if let Err(error) = crate::conversation::tool_plane::persist_governed_tool_plan_event(
                services.event_store(), &session_id, Some(&context_identity), &plan,
            ) {
                return defer(format!("early_original_plan_persistence_unknown:{error}"));
            }
            let mut authorizations = std::collections::HashMap::new();
            authorizations.insert(candidate.call.id.clone(), authorization);
            let early_fingerprint = early_tool_fingerprint(&invocation);
            let early_read_lock = {
                let mut locks = early_read_locks.lock().await;
                Arc::clone(
                    locks
                        .entry(early_fingerprint.clone())
                        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
                )
            };
            let _early_read_guard = early_read_lock.lock().await;
            let mut invocations = std::collections::HashMap::new();
            invocations.insert(candidate.call.id.clone(), invocation);
            let capability_gaps = std::collections::HashMap::new();
            let mut idempotency_keys = std::collections::HashMap::new();
            idempotency_keys.insert(
                candidate.call.id.clone(),
                format!(
                    "{}:early-read:{}:{early_fingerprint}",
                    ticket.graph_id, candidate.call.id
                ),
            );
            let calls = [candidate.call.clone()];
            let original_request = bound_runtime_tool_request(
                &candidate.call, task, &plan.plan_id, plan.revision, observation_wave_sequence,
                &session_id, early_execution_policy.sandbox_posture, early_execution_policy.revision,
                Some(&memory_context), reality_context.as_ref(), model_lease.as_deref(),
                &ticket, Some(&decision), None, idempotency_keys.get(&candidate.call.id),
            );
            let prepared_requests = HashMap::from([(candidate.call.id.clone(), original_request)]);
            let early_invocations = Arc::new(Mutex::new(HashMap::new()));
            let early_requests = Mutex::new(HashMap::new());
            let early_ticket = ticket.clone();
            let started_at_ms = crate::tool_invocation::now_ms();
            let context = HostGovernedToolContext {
                batch_policy_owner: None,
                action_issuer: None,
                single_captures: None,
                host: match services.tool_execution_host() {
                    Some(host) => Arc::clone(host),
                    None => return defer("runtime_tool_host_unavailable".to_string()),
                },
                event_bus,
                original_producers: None,
                calls: &calls,
                session_id: &session_id,
                sandbox_posture: early_execution_policy.sandbox_posture,
                policy_revision: early_execution_policy.revision,
                memory_context: Some(&memory_context),
                reality_context: reality_context.as_ref(),
                model_lease: model_lease.as_deref(),
                ticket: &early_ticket,
                execution_decision: Some(&decision),
                strategy_gate: None,
                admission_plan: Some(&plan),
                tool_authorizations: &authorizations,
                capability_gaps: &capability_gaps,
                policy_refusals: None,
                policy_failures: None,
                effective_inputs: None,
                prepared_effects: None,
                prepared_requests: Some(&prepared_requests),
                member_requests: Mutex::new(HashMap::new()),
                early_request_capture: Some(&early_requests),
                evaluation_scope_rejections: None,
                scope_state: None,
                services: None,
                scope_facts: Mutex::new(HashMap::new()),
                unknown_effects: Mutex::new(HashMap::new()),
                prepared_invocations: &invocations,
                plan_id: &plan.plan_id,
                plan_revision: plan.revision,
                observation_wave_sequence,
                execution_plane: services.tool_execution_plane(),
                physical_owner: services.model_step_executor(),
                commit_service: services.commit_service(),
                precompleted: None,
                original_replays: None,
                idempotency_keys: Some(&idempotency_keys),
                invocations: Arc::clone(&early_invocations),
            };
            if context.event_bus.as_ref().and_then(crate::CowdEventBus::current_execution_context).as_ref()
                != Some(&context_identity) {
                return defer("early_original_plan_context_changed".into());
            }
            let mut report = crate::GovernedToolExecutor.execute(&plan, &context).await;
            let unknown = context.unknown_effects.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&candidate.call.id).cloned();
            let completed_at_ms = crate::tool_invocation::now_ms();
            let Some(outcome) = report.outcomes.pop() else {
                return crate::conversation::EarlyToolDispatchResult::Failed(
                    crate::conversation::EarlyToolFailure {
                        identity: candidate.identity.clone(), tool_call_id: candidate.call.id.clone(),
                        owner_ticket: Some(ticket.clone()),
                        kind: crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown,
                        detail: "early executor returned no original outcome".into(),
                    });
            };
            let terminal_detail = crate::governed_tool_executor::terminal_reason(&outcome.terminal);
            let Some(receipt) = outcome.receipt else {
                return crate::conversation::EarlyToolDispatchResult::Failed(
                    crate::conversation::EarlyToolFailure {
                        identity: candidate.identity.clone(), tool_call_id: candidate.call.id.clone(),
                        owner_ticket: Some(ticket.clone()),
                        kind: crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown,
                        detail: format!("physical_unknown={unknown:?}; original_terminal={terminal_detail}; terminal_commit_error={:?}", outcome.terminal_commit_error),
                    });
            };
            if let Some(unknown) = unknown {
                return crate::conversation::EarlyToolDispatchResult::Failed(
                    crate::conversation::EarlyToolFailure {
                        identity: candidate.identity.clone(), tool_call_id: candidate.call.id.clone(),
                        owner_ticket: Some(ticket.clone()),
                        kind: crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown,
                        detail: format!("{unknown}; original_terminal={terminal_detail}; terminal_commit_error={:?}", outcome.terminal_commit_error),
                    });
            }
            let receipt = receipt.outcome;
            crate::execution_core::performance::observe_duration(
                "early_tool_ready_to_start_ms",
                std::time::Duration::from_millis(
                    started_at_ms.saturating_sub(candidate.ready_at_ms),
                ),
            );
            crate::execution_core::performance::observe_duration(
                "early_tool_service_ms",
                std::time::Duration::from_millis(completed_at_ms.saturating_sub(started_at_ms)),
            );
            let lifecycle = early_invocations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&candidate.call.id).cloned()
                .and_then(|record| record.original_producer.clone().map(|producer|
                    crate::conversation::ToolLifecycleReuse::PendingOriginal { record, producer }))
                .unwrap_or(crate::conversation::ToolLifecycleReuse::Unknown);
            let original_source = early_requests.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&candidate.call.id).cloned()
                .map(|request| crate::conversation::EarlyToolOriginalSource {
                    ticket: ticket.clone(), request, completed_calls: None,
                });
            crate::conversation::EarlyToolDispatchResult::Executed(
                crate::conversation::EarlyToolExecutionReceipt {
                    original_source,
                    lifecycle,
                    call: candidate.call,
                    outcome: receipt,
                    ready_at_ms: candidate.ready_at_ms,
                    started_at_ms,
                    completed_at_ms,
                },
            )
        });
        struct AbortOriginalWorker(Option<tokio::task::AbortHandle>);
        impl Drop for AbortOriginalWorker {
            fn drop(&mut self) {
                if let Some(handle) = self.0.take() { handle.abort(); }
            }
        }
        let retained = owner.retain_original_early_worker(&original_ticket, &original_call_id, work);
        let (worker, abort) = match retained {
            Ok(retained) => retained,
            Err(error) => return Box::pin(async move {
                crate::conversation::EarlyToolDispatchResult::Failed(
                    crate::conversation::EarlyToolFailure {
                        identity: original_identity, tool_call_id: original_call_id,
                        owner_ticket: Some(original_ticket),
                        kind: crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown,
                        detail: error.to_string(),
                    })
            }),
        };
        // Construct before returning the future so an unpolled drop still
        // stops the same retained worker.
        let mut guard = AbortOriginalWorker(Some(abort));
        Box::pin(async move {
            let failure = |detail: String| crate::conversation::EarlyToolDispatchResult::Failed(
                crate::conversation::EarlyToolFailure {
                    identity: original_identity.clone(), tool_call_id: original_call_id.clone(),
                    owner_ticket: Some(original_ticket.clone()),
                    kind: crate::conversation::EarlyToolFailureKind::WorkerOutcomeUnknown,
                    detail,
                });
            let result = worker.await;
            guard.0.take();
            match result {
                Ok(result) => result,
                Err(error) => failure(error),
            }
        })
    }
}

fn root_team_terminal_requires_text_only(
    delegated_leaf: bool,
    obligation: Option<&harness_contract::strategy::CollaborationExecutionObligation>,
    newly_completed_program_team_ids: &BTreeSet<String>,
) -> bool {
    !delegated_leaf && obligation.is_some() && !newly_completed_program_team_ids.is_empty()
}

/// Return missing user-named source scopes for a typed root admission call.
///
/// The scope is intentionally carried by the user objective rather than a
/// catalog template or a role name. A valid semantic decision may put its
/// evidence contract either on the Team workstream or on the individual role
/// that is responsible for reading it, so both declarations are accepted.
/// `None` means this batch does not contain the typed root-admission transport
/// (or the user did not name source paths); it must not affect ordinary tools.
fn is_team_orchestration_call(call: &ModelToolCall) -> bool {
    call.name
        .eq_ignore_ascii_case(harness_contract::agent_action::TEAM_CREATE_TOOL_ID)
}

fn evaluation_topology_forbids_team() -> bool {
    std::env::var("COWD_EVAL_HARNESS").as_deref() == Ok("1")
        && std::env::var("COWD_EVAL_CORPUS_ID").as_deref() == Ok("auto-strategy-v1")
        && std::env::var("COWD_EVAL_STRATEGY_OVERRIDE")
            .ok()
            .is_some_and(|override_| {
                matches!(
                    override_.trim().to_ascii_lowercase().as_str(),
                    "direct" | "parallel" | "parallel_tools"
                )
            })
}

fn tool_nodes_for_calls(
    ticket: &NodeExecutionTicket,
    iteration: usize,
    session_id: &str,
    mut calls: Vec<ModelToolCall>,
    _workspace_root: &std::path::Path,
    services: Option<&crate::RuntimeServices>,
    system_authored: bool,
) -> Result<Vec<ExecutionNodeSpec>, NodeExecutorError> {
    if system_authored {
        if let Some((services, lease)) = services.and_then(|services| {
            services
                .tool_execution_host()
                .and_then(|host| host.pin_step_catalog())
                .map(|lease| (services, lease))
        }) {
            let bind = || -> Result<crate::step_snapshot::BoundStep, String> {
                let graph = services
                    .graph_state_store()
                    .load(&ticket.graph_id)
                    .map_err(|error| error.to_string())?;
                let lineage = graph
                    .lineage
                    .as_ref()
                    .ok_or("system origin parent lineage missing")?;
                if lineage.session_id != session_id {
                    return Err("system origin parent session mismatch".into());
                }
                let names = calls
                    .iter()
                    .map(|call| call.name.clone())
                    .collect::<BTreeSet<_>>();
                let definitions = lease
                    .snapshot()
                    .catalog
                    .definitions(Some(&names))
                    .into_iter()
                    .map(|definition| ProviderToolDefinition {
                        name: definition.name,
                        description: definition.description,
                        input_schema: definition.input_schema,
                    })
                    .collect::<Vec<_>>();
                let step = crate::step_snapshot::BoundStep::bind(
                    harness_contract::step::StepIdentity {
                        step_id: uuid::Uuid::new_v4().to_string(),
                        session_id: session_id.to_string(),
                        turn_id: lineage.turn_id.clone(),
                        execution_id: Some(ticket.graph_id.clone()),
                        goal_ref: None,
                        program_ref: None,
                        task_ref: Some(lineage.task_id.clone()),
                        actor_ref: Some("runtime:bounded-verification".to_string()),
                        run_ref: None,
                        attempt: Some(ticket.attempt),
                        epoch: Some(lineage.generation.to_string()),
                        build_ref: option_env!("COWD_GIT_SHA")
                            .unwrap_or("unverified")
                            .to_string(),
                    },
                    lease,
                    &definitions,
                    &serde_json::json!({"source":"runtime_system"}),
                    &serde_json::json!({"parent_node":ticket.node_id,"attempt":ticket.attempt}),
                    BTreeMap::from([
                        ("parent_attempt".to_string(), ticket.attempt.to_string()),
                        (
                            "parent_generation".to_string(),
                            lineage.generation.to_string(),
                        ),
                    ]),
                    chrono::Utc::now()
                        .timestamp_millis()
                        .saturating_add(3_600_000),
                )
                .map_err(|error| error.to_string())?
                .with_parent(&format!(
                    "{}:{}:{}",
                    ticket.graph_id, ticket.node_id, ticket.attempt
                ));
                services
                    .event_store()
                    .append(crate::runtime_event_store::RuntimeEventInput {
                        stream_id: format!("session:{session_id}"),
                        scope: crate::runtime_event_store::RuntimeEventScope::Session,
                        kind: "system.step_bound".to_string(),
                        status: Some("prepared".to_string()),
                        actor: Some("runtime".to_string()),
                        refs: vec![],
                        payload: serde_json::to_value(step.snapshot())
                            .map_err(|error| error.to_string())?,
                    })
                    .map_err(|error| error.to_string())?;
                Ok(step)
            };
            let step = Arc::new(bind().map_err(|reason| NodeExecutorError::Poll {
                node_id: ticket.node_id.clone(),
                reason,
            })?);
            let parent = format!("{}:{}:{}", ticket.graph_id, ticket.node_id, ticket.attempt);
            for call in &mut calls {
                if call.binding.is_some() {
                    return Err(NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: "system compiler cannot relabel a provider call".into(),
                    });
                }
                call.binding = Some(crate::step_snapshot::StepCallBinding::system(
                    step.clone(),
                    &parent,
                    &call.id,
                ));
            }
        }
    }
    let batches = tool_batches_for_turn(&calls).map_err(|reason| NodeExecutorError::Poll {
        node_id: ticket.node_id.clone(),
        reason,
    })?;
    let batch_count = batches.len();
    batches
        .into_iter()
        .enumerate()
        .map(|(index, calls)| {
            let mut tool_node = dynamic_node(
                ticket,
                iteration,
                &format!("tools-{}", index + 1),
                ExecutionNodeKind::ToolBatch,
                "tool_batch",
                "inline_model",
            );
            tool_node.payload_ref =
                encode_tool_calls_with_continuation(session_id, &calls, index + 1 < batch_count)
                    .map_err(|error| NodeExecutorError::Poll {
                        node_id: ticket.node_id.clone(),
                        reason: error.to_string(),
                    })?;
            // A ToolBatch is a graph container, not the authority for the
            // model-provided paths nested inside it.  Pre-resolving those
            // paths here turns one bad read (for example, a mistyped source
            // file beside otherwise valid calls) into a terminal failure of
            // the whole batch before the governed ToolHost can emit its
            // per-call receipt.  The leaf governed-tool tasks retain their
            // exact resource demand, authorization, lock, and error receipt
            // handling; keeping the container unscoped therefore improves
            // partial progress without widening filesystem authority.
            tool_node.resource_scopes = Vec::new();
            Ok(tool_node)
        })
        .collect()
}

/// Runtime-authored exact-read recovery is not a model tool request, yet a
/// follow-up OpenAI-compatible model request must still see a valid
/// assistant-tool-call → tool-result transcript pair.  These ids are minted
/// exclusively by the Runtime recovery compilers below; model-supplied ids
/// never receive this synthetic transcript frame.
fn runtime_authored_tool_batch(calls: &[ModelToolCall]) -> bool {
    !calls.is_empty()
        && calls.iter().all(|call| {
            call.binding.as_ref().is_some_and(|binding| {
                matches!(
                    binding.origin.source,
                    harness_contract::step::CallSource::SystemDerived { .. }
                )
            })
        })
}

fn runtime_authored_tool_call_message(calls: &[ModelToolCall]) -> ConversationMessage {
    ConversationMessage::assistant(
        calls
            .iter()
            .map(|call| ContentBlock::ToolUse {
                id: call.id.clone(),
                name: call.name.clone(),
                input: call.input.clone(),
            })
            .collect(),
    )
}

struct TurnToolBatchBackend<C: ApiClient, T: ToolExecutor> {
    runtime: Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    state: Arc<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
    goal_revalidation: std::sync::Mutex<Option<ToolGoalRevalidation>>,
}

#[derive(Clone)]
struct ToolGoalRevalidation {
    ticket: NodeExecutionTicket,
    source: crate::execution_core::goal::GoalProjection,
    observation: RuntimeObservation,
    checkpoint: crate::execution_core::goal::GoalWriteCheckpoint,
    original_satisfied: bool,
    outcome: NodeExecutionOutcome,
}

async fn compact_governed_tool_messages<C, T>(
    runtime: &Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    calls: &[ModelToolCall],
    raw_messages: Vec<ConversationMessage>,
    invocations: &HashMap<String, ToolInvocationRecord>,
    precompleted: &BTreeMap<String, crate::conversation::EarlyToolExecutionReceipt>,
) -> Result<Vec<ConversationMessage>, RuntimeError>
where
    C: ApiClient,
    T: ToolExecutor,
{
    let call_inputs = calls
        .iter()
        .map(|call| (call.id.as_str(), call.input.as_str()))
        .collect::<BTreeMap<_, _>>();
    let runtime = runtime.lock().await;
    let mut messages = Vec::with_capacity(raw_messages.len());
    runtime.remember_native_tool_calls(calls)?;
    for call in calls {
        if let Some(receipt) = precompleted.get(&call.id) {
            runtime.restore_native_root_owner_from_replay(call, receipt)?;
        }
    }
    for raw_message in raw_messages {
        let Some((tool_use_id, tool_name, output, is_error)) =
            raw_message.blocks.iter().find_map(|block| match block {
                ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    is_error,
                } => Some((
                    tool_use_id.as_str(),
                    tool_name.as_str(),
                    output.as_str(),
                    *is_error,
                )),
                _ => None,
            })
        else {
            messages.push(raw_message);
            continue;
        };
        let input = call_inputs.get(tool_use_id).copied().unwrap_or_default();
        #[cfg(test)]
        if tool_use_id == "ra-read" {
            let pending = precompleted.get(tool_use_id).map(|receipt| match &receipt.lifecycle {
                crate::conversation::ToolLifecycleReuse::PendingOriginal { .. } => "pending_original",
                crate::conversation::ToolLifecycleReuse::DurableEffectReplay { original_lifecycle: Some(_) } => "durable_original",
                crate::conversation::ToolLifecycleReuse::DurableEffectReplay { original_lifecycle: None } => "durable_unknown",
                crate::conversation::ToolLifecycleReuse::Unknown => "unknown",
            }).unwrap_or("none");
            eprintln!("ra_lifecycle_compactor call=ra-read precompleted={pending} invocation_present={} producer_present={}",
                invocations.contains_key(tool_use_id), invocations.get(tool_use_id).is_some_and(|record| record.original_producer.is_some()));
        }
        #[cfg(feature = "test-fixtures")]
        if std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some() && tool_use_id == "recovery-complete" {
            eprintln!("rc_completion_stage before_compactor_item");
        }
        messages.push(
            runtime
                .prepare_governed_tool_result_with_invocation(
                    tool_use_id,
                    tool_name,
                    input,
                    output,
                    is_error,
                    invocations.get(tool_use_id).cloned(),
                )
                .await?,
        );
        #[cfg(feature = "test-fixtures")]
        if std::env::var_os("COWD_RC_TRACE_COMPLETION").is_some() && tool_use_id == "recovery-complete" {
            eprintln!("rc_completion_stage after_compactor_item");
        }
    }
    Ok(messages)
}

/// Execute a ToolBatch using the already-governed plan rather than serialising
/// every read-only request in the conversation adapter.  The plan is the
/// authority for dependency, safety category, and concurrency: the host only
/// receives fully-bound individual requests.  Results are returned in model
/// call order even when their execution is concurrent.
struct GovernedToolBatchResult {
    member_batches: Vec<(String, crate::PendingAgentBatch)>,
    unresolved: Vec<crate::conversation::ToolBatchUnresolved>,
    unknown_effect: Option<crate::ToolError>,
    refused_before_dispatch: Vec<crate::conversation::PreDispatchRefusal>,
    messages: Vec<ConversationMessage>,
    invocations: HashMap<String, ToolInvocationRecord>,
    observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
    max_concurrency_observed: usize,
    parallel_batches: usize,
}

struct HostGovernedReceipt {
    outcome: crate::RuntimeToolExecutionOutcome,
    refusal: Option<crate::execution_core::graph::CommittedCallRefusal>,
}

struct HostPolicyRefusal {
    request: crate::RuntimeToolExecutionRequest,
    assessment: crate::conversation::BoundPolicyAssessment,
}

trait HostOriginalStrategyGate: Send + Sync {
    fn check<'a>(
        &'a self,
        task: &'a crate::GovernedToolPlanTask,
        plan: &'a crate::GovernedToolPlan,
        decision: &'a crate::execution_core::RuntimeExecutionDecision,
        ticket: &'a NodeExecutionTicket,
    ) -> crate::GovernedToolFuture<'a, Result<(), String>>;
}

impl std::ops::Deref for HostGovernedReceipt {
    type Target = crate::RuntimeToolExecutionOutcome;
    fn deref(&self) -> &Self::Target { &self.outcome }
}

tokio::task_local! {
    static ROOT_BATCH_POLICY_OWNER: Arc<dyn crate::tool_host::AgentBatchPolicyOwner>;
    static ROOT_ACTION_ISSUER: Arc<crate::conversation::tool_plane::OwnedRootActionIssuer>;
    static ROOT_SINGLE_CAPTURES: Arc<Mutex<HashMap<String, Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>>>;
}

struct OriginalBatchPolicyOwner {
    state: std::sync::Weak<tokio::sync::Mutex<TurnGraphState>>,
    services: std::sync::Weak<crate::RuntimeServices>,
    owner: Arc<crate::conversation::SharedPolicyOwner>,
    prompter: SharedPrompter,
    parent: harness_contract::execution_graph::ExecutionParentBinding,
    attempt: u32,
    ticket: NodeExecutionTicket,
    event_store: Arc<crate::RuntimeEventStore>,
    action_service: crate::AgentActionService,
}

struct OriginalHostPreparationFailure {
    reason: OriginalHostPreparationFailureReason,
    effect_fence: Option<Arc<crate::runtime_event_store::OriginalMemberEffectFence>>,
}
enum OriginalHostPreparationFailureReason {
    Preparation(String),
    OriginalOwnerStore(Arc<crate::RuntimeEventStoreError>),
}

/// Evidence for the original action owner, not a grant or a replay result.
/// Only this module's original ticket owner can construct a witness.
pub(crate) struct OriginalMemberReplayWitness {
    store: Arc<crate::RuntimeEventStore>,
    ticket: NodeExecutionTicket,
    outer: crate::RuntimeToolExecutionRequest,
    index: usize,
    prepared: OriginalMemberReplayPrepared,
    index_event: crate::DurableRuntimeEvent,
    action: crate::DurableRuntimeEvent,
    member_sources: Vec<crate::DurableRuntimeEvent>,
    action_receipt: crate::AppendTransactionReceipt,
}

/// Deserialized from the original action's member_admission fact. A still
/// independently verifies the complete action and authorization facts.
#[derive(Deserialize)]
pub(crate) struct OriginalMemberReplayPrepared {
    pub(crate) outer_key: String,
    pub(crate) index: usize,
    pub(crate) tool: String,
    pub(crate) input: serde_json::Value,
    pub(crate) effect: harness_contract::tool::ToolEffectDescriptor,
    pub(crate) identity: String,
}

fn normalize_replay_member_input(entry: &serde_json::Value) -> Result<serde_json::Value, String> {
    let mut input = entry.get("input").cloned().unwrap_or_else(|| serde_json::json!({}));
    let object = input.as_object_mut().ok_or("batch member input is not an object")?;
    for name in ["reference", "evidence_ref"] {
        if let Some(raw) = object.get(name).and_then(serde_json::Value::as_str) {
            object.insert(name.into(), serde_json::Value::String(crate::normalize_bare_evidence_ref(raw)));
        }
    }
    for name in ["evidence_refs", "result_refs", "artifact_refs"] {
        if let Some(items) = object.get_mut(name).and_then(serde_json::Value::as_array_mut) {
            for item in items {
                if let Some(raw) = item.as_str() {
                    *item = serde_json::Value::String(crate::normalize_bare_evidence_ref(raw));
                }
            }
        }
    }
    Ok(input)
}

impl OriginalMemberReplayWitness {
    pub(crate) fn belongs_to_store(&self, store: &Arc<crate::RuntimeEventStore>) -> bool {
        Arc::ptr_eq(&self.store, store)
    }
    pub(crate) fn ticket(&self) -> &NodeExecutionTicket { &self.ticket }
    pub(crate) fn outer(&self) -> &crate::RuntimeToolExecutionRequest { &self.outer }
    pub(crate) fn index(&self) -> usize { self.index }
    pub(crate) fn prepared(&self) -> &OriginalMemberReplayPrepared { &self.prepared }
    pub(crate) fn index_event(&self) -> &crate::DurableRuntimeEvent { &self.index_event }
    pub(crate) fn action(&self) -> &crate::DurableRuntimeEvent { &self.action }
    pub(crate) fn member_sources(&self) -> &[crate::DurableRuntimeEvent] { &self.member_sources }
    pub(crate) fn action_receipt(&self) -> &crate::AppendTransactionReceipt { &self.action_receipt }
}

enum RetainedOriginalHostMember {
    Committed(crate::tool_host::AuthenticatedBatchMember),
    Terminal(Arc<crate::tool_host::OriginalStopPublication>),
}

impl OriginalBatchPolicyOwner {
    /// Recover only an uncertain in-process checked append retained by the
    /// original action owner. This is not a query for Fresh or a general
    /// durable replay reader; absence or a different owner remains unknown.
    fn read_retained_original_member(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize,
    ) -> Result<Option<RetainedOriginalHostMember>, crate::tool_host::BatchMemberUnknownReason> {
        use crate::tool_host::BatchMemberUnknownReason;
        let unavailable = || BatchMemberUnknownReason::OriginalTransactionFactsUnavailable;
        if let Ok(publications) = original.tool_progress.checked_original_stop_publications() {
            if let [publication] = publications.as_slice() {
                if let crate::tool_host::OriginalStopPublication::Recorded(stop) = publication.as_ref() {
                    if stop.belongs_to_original_sink(&original.tool_progress) && stop.coordinate().ticket() == &self.ticket
                        && stop.coordinate().index() == index { stop.reconcile_original(); }
                }
            }
        }
        let stream = format!("execution-effect:{}", original.idempotency_key);
        let key = format!("agentic-member:{index}");
        let Some(marker) = self.event_store.event_by_idempotency_key(&stream, &key)
            .map_err(|_| unavailable())? else {
                if original.tool_progress.has_original_member_stop() { return Err(unavailable()); }
                return Ok(None);
            };
        if marker.kind == "execution.effect.member_noncommit" {
            let publications = original.tool_progress.checked_original_stop_publications()
                .map_err(|_| unavailable())?;
            let [publication] = publications.as_slice() else { return Err(unavailable()); };
            let crate::tool_host::OriginalStopPublication::Recorded(stop) = publication.as_ref() else {
                return Err(unavailable());
            };
            let crate::tool_host::OriginalStopRead::Available(Some(current)) = original.tool_progress.original_member_stop() else {
                return Err(unavailable());
            };
            let coordinate = stop.coordinate();
            if !Arc::ptr_eq(stop, &current) || !stop.recorded_in_original_sink()
                || !stop.belongs_to_original_sink(&original.tool_progress)
                || coordinate.ticket() != &self.ticket || coordinate.outer_key() != original.idempotency_key
                || coordinate.call_id() != original.tool_use_id || coordinate.index() != index
                || coordinate.step() != original.step_binding.as_ref() {
                return Err(unavailable());
            }
            let crate::tool_host::OriginalBatchMemberStopReason::ConfirmedNoncommit(terminal) = stop.reason() else {
                return Err(unavailable());
            };
            let reservation = stop.original_reservation().ok_or_else(unavailable)?;
            let fence = stop.original_effect_fence().ok_or_else(unavailable)?;
            let retained = self.event_store.original_member_noncommit(reservation, fence)
                .map_err(|_| unavailable())?.ok_or_else(unavailable)?;
            if !terminal.same_original(&retained) || !terminal.matches_request(original) {
                return Err(unavailable());
            }
            let member_index = u64::try_from(index).map_err(|_| unavailable())?;
            self.event_store.validate_original_member_noncommit(terminal, &self.ticket, &original.idempotency_key, member_index)
                .map_err(|_| unavailable())?;
            let transaction = &terminal.transaction;
            let receipt = &terminal.receipt;
            let [event] = transaction.events.as_slice() else { return Err(unavailable()); };
            let [expected] = transaction.expected_streams.as_slice() else { return Err(unavailable()); };
            let [revision] = receipt.stream_revisions.as_slice() else { return Err(unavailable()); };
            if marker.stream_id != stream || marker.scope != event.event.scope
                || marker.kind != event.event.kind || marker.status != event.event.status
                || marker.actor != event.event.actor || marker.refs != event.event.refs
                || marker.payload != event.event.payload || marker.schema_version != event.schema_version
                || marker.idempotency_key != event.idempotency_key || marker.idempotency_key.as_deref() != Some(key.as_str())
                || event.event.stream_id != stream || expected.stream_id != stream || revision.stream_id != stream
                || revision.expected_revision != expected.expected_revision
                || expected.expected_revision.checked_add(1) != Some(revision.committed_revision)
                || marker.sequence != revision.committed_revision || marker.transaction_index != 0
                || marker.transaction_id != transaction.transaction_id || receipt.transaction_id != transaction.transaction_id
                || marker.commit_cursor == 0 || receipt.commit_cursor != marker.commit_cursor
                || receipt.event_ids.as_slice() != [marker.event_id.clone()] {
                return Err(unavailable());
            }
            return Ok(Some(RetainedOriginalHostMember::Terminal(Arc::clone(publication))));
        }
        let handle = self.action_service.recover_retained_original_member(&marker.transaction_id)
            .map_err(|_| unavailable())?.ok_or_else(unavailable)?;
        let authority = handle.authority().ok_or_else(unavailable)?;
        let member_guard = authority.member().upgrade().ok_or_else(unavailable)?;
        let host = member_guard.host().ok_or_else(unavailable)?;
        let source = host.original_request();
        let source_hook = source.tool_progress.original_governed_hook().map_err(|_| unavailable())?.ok_or_else(unavailable)?;
        let request_hook = original.tool_progress.original_governed_hook().map_err(|_| unavailable())?.ok_or_else(unavailable)?;
        if !Arc::ptr_eq(host.owner(), &self.owner) || !Arc::ptr_eq(host.store(), &self.event_store)
            || !Arc::ptr_eq(&source_hook, &request_hook) || host.ticket() != &self.ticket
            || host.prepared().index != index || host.prepared().outer_key != original.idempotency_key
            || source.idempotency_key != original.idempotency_key || source.tool_use_id != original.tool_use_id
            || source.tool_name != original.tool_name || source.session_id != original.session_id
            || source.step_binding != original.step_binding || source.input != original.input
            || source.compiled_invocation != original.compiled_invocation {
            return Err(unavailable());
        }
        let transaction = &handle.request().transaction;
        let receipt = &handle.receipt().commit;
        let marker_index = usize::try_from(marker.transaction_index).map_err(|_| unavailable())?;
        let checked_marker = transaction.events.get(marker_index).ok_or_else(unavailable)?;
        if marker.stream_id != stream || marker.sequence <= 1 || marker.scope != crate::RuntimeEventScope::ExecutionNode
            || marker.kind != "agentic.batch_member.index" || marker.schema_version != 1
            || marker.idempotency_key.as_deref() != Some(key.as_str())
            || marker.payload["outer_key"] != original.idempotency_key
            || marker.payload["member_index"].as_u64() != u64::try_from(index).ok()
            || marker.payload["ticket"] != serde_json::to_value(&self.ticket).map_err(|_| unavailable())?
            || marker.transaction_id != transaction.transaction_id || receipt.transaction_id != marker.transaction_id
            || marker.commit_cursor == 0 || receipt.commit_cursor != marker.commit_cursor
            || receipt.event_ids.get(marker_index) != Some(&marker.event_id)
            || checked_marker.event.stream_id != marker.stream_id || checked_marker.event.kind != marker.kind
            || checked_marker.event.scope != marker.scope || checked_marker.event.status != marker.status
            || checked_marker.event.actor != marker.actor || checked_marker.event.refs != marker.refs
            || checked_marker.event.payload != marker.payload || checked_marker.schema_version != marker.schema_version
            || checked_marker.idempotency_key != marker.idempotency_key
            || !receipt.stream_revisions.iter().any(|source| source.stream_id == stream
                && source.expected_revision < marker.sequence && marker.sequence <= source.committed_revision) {
            return Err(unavailable());
        }
        let action_index = marker.payload["program_event_index"].as_u64()
            .and_then(|value| usize::try_from(value).ok()).ok_or_else(unavailable)?;
        let action = transaction.events.get(action_index).ok_or_else(unavailable)?;
        let outer_hook = host.original_outer_hook_publication_facts().map_err(|_| unavailable())?;
        // The member admission facts are read back from the same-source batch
        // payload the action event keeps.
        let action_payload = crate::step_snapshot::rebuild_member_admission_payload(&action.event.payload)
            .map_err(|_| unavailable())?;
        let action_payload: &serde_json::Value = &action_payload;
        if action_payload.pointer("/member_admission/outer_hook") != Some(&outer_hook)
            || receipt.event_ids.get(action_index).is_none_or(String::is_empty)
            || !receipt.stream_revisions.iter().any(|source| source.stream_id == action.event.stream_id
                && source.expected_revision < source.committed_revision)
        { return Err(unavailable()); }
        let envelope = serde_json::from_value(action.event.payload.get("envelope").cloned().ok_or_else(unavailable)?)
            .map_err(|_| unavailable())?;
        crate::tool_host::AuthenticatedBatchMember::from_original_committed(handle, &self.event_store, &envelope)
            .map(|member| Some(RetainedOriginalHostMember::Committed(member))).map_err(|_| unavailable())
    }

    // Locate only in the original outer effect stream, never from a model
    // action ID. The index and Program event must share one original receipt.
    fn locate_original_member_event(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize,
    ) -> Result<(crate::DurableRuntimeEvent, crate::DurableRuntimeEvent, crate::AppendTransactionReceipt), crate::tool_host::BatchMemberUnknownReason> {
        use crate::tool_host::BatchMemberUnknownReason;
        let unavailable = || BatchMemberUnknownReason::OriginalTransactionFactsUnavailable;
        let stream = format!("execution-effect:{}", original.idempotency_key);
        let key = format!("agentic-member:{index}");
        let marker = self.event_store.event_by_idempotency_key(&stream, &key)
            .map_err(|_| unavailable())?.ok_or_else(unavailable)?;
        let payload = &marker.payload;
        let program_stream = payload.get("program_stream").and_then(serde_json::Value::as_str).ok_or_else(unavailable)?;
        let program_key = payload.get("program_event_key").and_then(serde_json::Value::as_str).ok_or_else(unavailable)?;
        let program_kind = payload.get("program_event_kind").and_then(serde_json::Value::as_str).ok_or_else(unavailable)?;
        let program_index = payload.get("program_event_index").and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok()).ok_or_else(unavailable)?;
        if marker.stream_id != stream || marker.scope != crate::RuntimeEventScope::ExecutionNode
            || marker.kind != "agentic.batch_member.index" || marker.schema_version != 1
            || marker.status.as_deref() != Some("committed") || marker.actor.as_deref() != Some("runtime.agentic_action")
            || marker.idempotency_key.as_deref() != Some(key.as_str())
            || payload.get("outer_key").and_then(serde_json::Value::as_str) != Some(original.idempotency_key.as_str())
            || payload.get("member_index").and_then(serde_json::Value::as_u64) != u64::try_from(index).ok()
            || payload.get("ticket") != serde_json::to_value(&self.ticket).ok().as_ref()
            || !matches!(program_kind, "agentic.action_applied" | "agentic.completion_rejected")
        { return Err(unavailable()); }
        let action = self.event_store.event_by_idempotency_key(program_stream, program_key)
            .map_err(|_| unavailable())?.ok_or_else(unavailable)?;
        // A generic deserialized receipt proves only that a transaction exists;
        // it does not prove that the original checked member owner submitted
        // that transaction. The retained owner verifies the original request
        // hash and event count before returning a receipt for an uncertain
        // in-process append. Absence of that owner remains Unknown.
        let receipt = self.event_store.read_retained_checked_member_commit(&marker.transaction_id)
            .map_err(|_| unavailable())?.ok_or_else(unavailable)?;
        if marker.transaction_id.is_empty() || marker.commit_cursor == 0
            || action.stream_id != program_stream || action.kind != program_kind
            || action.idempotency_key.as_deref() != Some(program_key)
            || action.transaction_id != marker.transaction_id || action.commit_cursor != marker.commit_cursor
            || usize::try_from(action.transaction_index).ok() != Some(program_index)
            || receipt.transaction_id != marker.transaction_id || receipt.commit_cursor != marker.commit_cursor
            || receipt.event_ids.get(marker.transaction_index as usize) != Some(&marker.event_id)
            || receipt.event_ids.get(program_index) != Some(&action.event_id)
            || !receipt.stream_revisions.iter().any(|source| source.stream_id == marker.stream_id
                && source.expected_revision < marker.sequence && marker.sequence <= source.committed_revision)
            || !receipt.stream_revisions.iter().any(|source| source.stream_id == action.stream_id
                && source.expected_revision < action.sequence && action.sequence <= source.committed_revision)
        { return Err(unavailable()); }
        Ok((marker, action, receipt))
    }

    /// This is only a replay reader check. Missing intent is Unknown; it does
    /// not reject an uncommitted business call or issue a new authorization.
    fn validate_original_outer_intent(&self, original: &crate::RuntimeToolExecutionRequest) -> Result<(), String> {
        let invocation = original.compiled_invocation.as_ref().ok_or("original batch invocation missing")?;
        let read = invocation.effect.effect_kind == harness_contract::tool::ToolEffectKind::Read;
        let stream = format!("execution-effect:{}", original.idempotency_key);
        let key = format!("{}:{}", original.idempotency_key, if read { "read-intent" } else { "intent" });
        let event = self.event_store.event_by_idempotency_key(&stream, &key)
            .map_err(|error| error.to_string())?.ok_or("original batch intent missing")?;
        let receipt = self.event_store.original_transaction_receipt(&event.transaction_id)
            .map_err(|error| error.to_string())?.ok_or("original batch intent receipt missing")?;
        let replay = crate::execution_core::graph::receipt_replay_contract(original)
            .map_err(|error| error.to_string())?;
        if event.stream_id != stream || event.sequence != 1 || event.scope != crate::RuntimeEventScope::ExecutionNode
            || event.kind != if read { "execution.read.intent" } else { "execution.effect.intent" }
            || event.status.as_deref() != Some("inflight") || event.actor.as_deref() != Some("governed_tool")
            || event.schema_version != 1 || event.idempotency_key.as_deref() != Some(key.as_str())
            || event.payload["idempotency_key"] != original.idempotency_key
            || event.payload["tool_use_id"] != original.tool_use_id || event.payload["tool_name"] != original.tool_name
            || event.payload["input_sha256"] != format!("sha256:{:x}", Sha256::digest(original.input.as_bytes()))
            || event.payload["effect"] != serde_json::to_value(&invocation.effect).map_err(|error| error.to_string())?
            || event.payload["replay"] != replay
            || receipt.transaction_id != event.transaction_id || receipt.commit_cursor != event.commit_cursor
            || receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
            || !receipt.stream_revisions.iter().any(|source| source.stream_id == stream
                && source.expected_revision == 0 && source.committed_revision == 1)
        { return Err("original batch intent mismatch".into()); }
        Ok(())
    }

    fn prepare_member_replay_witness_from_outer(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize, outer: &serde_json::Value,
    ) -> Result<Arc<OriginalMemberReplayWitness>, crate::tool_host::BatchMemberUnknownReason> {
        use crate::tool_host::BatchMemberUnknownReason;
        let (marker, action, receipt) = self.locate_original_member_event(original, index)?;
        self.authenticate_member_replay(original, index, outer, marker, action, receipt)
            .map(Arc::new)
            .map_err(BatchMemberUnknownReason::OriginalRequestNotAuthenticated)
    }

    async fn retain_original_host_member(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize,
        member: &Arc<crate::conversation::OriginalHostPreparedMember>,
    ) -> Result<(), String> {
        let services = self.services.upgrade().ok_or("original Host services unavailable")?;
        let observation = services.tool_batch_executor().binding_observation(&self.ticket)
            .ok_or("original Host binding observation unavailable")?;
        let state = self.state.upgrade().ok_or("original Host turn state unavailable")?;
        let state = state.lock().await;
        let entries = state.pending_member_batches.get(&(self.ticket.node_id.clone(), self.ticket.attempt))
            .ok_or("original Host call carrier unavailable")?;
        let mut found = None;
        for (call_id, pending) in entries {
            if call_id != &original.tool_use_id { continue; }
            if let crate::PendingAgentBatchValue::OriginalCall(carrier) = pending.value() {
                if carrier.original_request() != original
                    || !carrier.original_request().tool_progress.same_original_channels(&original.tool_progress) {
                    return Err("original Host call carrier changed".into());
                }
                if found.replace(Arc::clone(carrier)).is_some() {
                    return Err("original Host call carrier is ambiguous".into());
                }
            }
        }
        let carrier = found.ok_or("original Host call carrier unavailable")?;
        drop(state);
        if member.prepared().index != index { return Err("original Host member index differs".into()); }
        carrier.retain_host_member(&self.ticket, member, &observation)?;
        if services.tool_batch_executor().binding_observation(&self.ticket)
            .is_none_or(|current| !Arc::ptr_eq(&current, &observation)) {
            return Err("original Host binding observation changed".into());
        }
        Ok(())
    }

    /// Original Host owner prepares this member without consuming a lease.
    /// The physical outer intent and ticket remain the same producer facts.
    async fn prepare_original_host_member(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<Arc<crate::conversation::OriginalHostPreparedMember>, String> {
        self.validate_original_member(original, index).await?;
        self.validate_original_outer_intent(original)?;
        if !self.owner.uses_original_store(&self.event_store) {
            return Err("Host member store differs from original policy owner".into());
        }
        self.owner.prepare_original_host_member(self.ticket.clone(), Arc::new(original.clone()), index, reservation, effect_fence)
            .await.map_err(|error| error.to_string())
    }

    /// The effect owner's same-call fresh fact must be authenticated when
    /// acquiring the durable outer/index fence, before hook or approval.
    async fn prepare_original_host_capability(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        fresh_effect: Option<&Arc<crate::execution_core::graph::OriginalFreshEffectOwner>>,
    ) -> Result<crate::step_snapshot::OriginalPreparedMemberCapability, OriginalHostPreparationFailure> {
        let before_fence = |reason: String| OriginalHostPreparationFailure {
            reason: OriginalHostPreparationFailureReason::Preparation(reason), effect_fence: None,
        };
        let original_index = u64::try_from(index).map_err(|_| before_fence("original member index overflow".into()))?;
        if !reservation.matches_original(&self.event_store, &original.idempotency_key, original_index, &self.ticket) {
            return Err(before_fence("original member reservation identity unavailable".into()));
        }
        // Required original effect/backend owner check, not an index lookup:
        // it must atomically retain the durable fence in the same reservation
        // before returning, including its own Unknown outcome, and provide
        // exclusive rights before the first hook or approval can run.
        // The durable fence observes the original call through the request
        // the effect owner holds for it. The Read family installs no owner,
        // so the request of this very call is the observation's source
        // there: it is the same request the owner would have held, with the
        // same same-call channels.
        //
        // The fence keeps this request itself. A temporary handle would let
        // the fence's own observation die with this call frame, and the
        // settlement would then be unable to name the call it belongs to:
        // a member the runtime did admit would be refused there instead of
        // at the admission.
        let original_request = match fresh_effect {
            Some(owner) => Arc::clone(owner.original_request()),
            None => Arc::new(original.clone()),
        };
        let effect_fence = self.event_store.acquire_original_member_preparation_fence(
            &reservation, &original_request, &self.ticket, index, fresh_effect.map(|owner| owner.as_ref()),
            self.owner.original_member_claim().map_err(before_fence)?,
        ).map_err(|error| {
            OriginalHostPreparationFailure {
                reason: OriginalHostPreparationFailureReason::OriginalOwnerStore(Arc::new(error)), effect_fence: None,
            }
        })?;
        // Acquisition must atomically retain this durable fence in the same
        // original reservation before any await, including future cancellation.
        let result = async {
            let member = self.prepare_original_host_member(original, index, Arc::clone(&reservation), Arc::clone(&effect_fence)).await
                .map_err(|error| {
                    OriginalHostPreparationFailureReason::Preparation(error)
                })?;
            self.retain_original_host_member(original, index, &member).await
                .map_err(OriginalHostPreparationFailureReason::Preparation)?;
            let authority = member.prepare_authority_with_approval().await
                .map_err(|error| {
                    OriginalHostPreparationFailureReason::Preparation(error)
                })?;
            if !reservation.matches_original(&self.event_store, &original.idempotency_key, original_index, &self.ticket) {
                return Err(OriginalHostPreparationFailureReason::Preparation("original member reservation changed during preparation".into()));
            }
            self.event_store.verify_original_member_preparation_fence(
                &effect_fence, &reservation, original, &self.ticket, index,
            ).map_err(|error| {
                OriginalHostPreparationFailureReason::OriginalOwnerStore(Arc::new(error))
            })?;
            self.bind_prepared_host_authority(member, authority, Arc::clone(&reservation), Arc::clone(&effect_fence))
                .map_err(|error| {
                    OriginalHostPreparationFailureReason::Preparation(error)
                })
        }.await;
        result.map_err(|reason| OriginalHostPreparationFailure { reason, effect_fence: Some(effect_fence) })
    }

    /// Seal the exact original preparation together with the E1 candidate.
    /// Neither object is a decision to execute a previously unseen member.
    fn bind_prepared_host_authority(
        &self,
        member: Arc<crate::conversation::OriginalHostPreparedMember>,
        authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<crate::step_snapshot::OriginalPreparedMemberCapability, String> {
        if !Arc::ptr_eq(member.owner(), &self.owner)
            || !Arc::ptr_eq(member.store(), &self.event_store)
            || member.ticket() != &self.ticket {
            return Err("prepared Host member belongs to a different original owner".into());
        }
        crate::step_snapshot::OriginalPreparedMemberCapability::from_host(member, authority, reservation, effect_fence)
    }

    async fn validate_original_member(&self, original: &crate::RuntimeToolExecutionRequest, index: usize)
        -> Result<serde_json::Value, String> {
        let binding = original.step_binding.as_ref().ok_or_else(|| {
            "batch original Step missing"
        })?;
        original.compiled_invocation.as_ref().ok_or_else(|| {
            "batch original invocation missing"
        })?;
        // The Step is a description, not proof that this call belonged to the
        // executor's original started ticket. Re-read the original start and
        // require exactly one matching call from its persisted payload.
        let validation_store = Arc::clone(&self.event_store);
        let validation_ticket = self.ticket.clone();
        tokio::task::spawn_blocking(move || {
            crate::execution_core::graph::validate_node_ticket_start(&validation_store, &validation_ticket, None)
                .map(|_| ()).map_err(|error| format!("batch original ticket start invalid: {error}"))
        }).await.map_err(|error| format!("batch original ticket validation task failed: {error}"))??;
        crate::execution_core::graph::executors::agent_tool::validate_original_call_request(&self.ticket, original)
            .map_err(|error| {
                format!("batch original ticket call invalid: {error}")
            })?;
        let input = harness_contract::step::parse_strict_json(&original.input)
            .map_err(|error| {
                format!("batch original input invalid: {error}")
            })?;
        if original.parent_execution.as_ref() != Some(&self.parent)
            || original.parent_execution_attempt != Some(self.attempt)
            || original.session_id.as_deref() != Some(self.owner.session_id())
            || original.tool_name != "agent_action_batch"
            || original.idempotency_key != binding.effect_idempotency_key()
            || original.tool_use_id != binding.origin.call_id
            || !self.owner.owns_turn(binding)
        {
            return Err("batch original owner mismatch".into());
        }
        if input.get("actions").and_then(serde_json::Value::as_array)
            .filter(|actions| !actions.is_empty() && actions.len() <= 25)
            .and_then(|actions| actions.get(index)).is_none()
        {
            return Err("batch original member index missing".into());
        }
        Ok(input)
    }

    // The candidate must come from the original owner's same-transaction
    // outer/index locator. This private function is deliberately not exposed
    // to Gateway or to callers supplying an action ID or event coordinate.
    fn authenticate_member_replay(
        &self, original: &crate::RuntimeToolExecutionRequest, index: usize,
        outer: &serde_json::Value,
        index_event: crate::DurableRuntimeEvent,
        action: crate::DurableRuntimeEvent,
        receipt: crate::AppendTransactionReceipt,
    ) -> Result<OriginalMemberReplayWitness, String> {
        let binding = original.step_binding.as_ref().ok_or("original Step missing")?;
        let facts = action.payload.get("member_admission").ok_or("original member facts missing")?;
        // The member admission facts are read back from the same-source batch
        // payload the action event keeps.
        let facts = crate::step_snapshot::rebuild_member_admission_facts(facts)?;
        let facts: &serde_json::Value = &facts;
        let prepared: OriginalMemberReplayPrepared = serde_json::from_value(
            facts.get("prepared").cloned().ok_or("original preparation missing")?,
        ).map_err(|error| error.to_string())?;
        let authorization: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(
            facts.get("authorization").cloned().ok_or("original authorization missing")?,
        ).map_err(|error| error.to_string())?;
        let envelope: harness_contract::agent_action::AgentActionEnvelope = serde_json::from_value(
            action.payload.get("envelope").cloned().ok_or("original action envelope missing")?,
        ).map_err(|error| error.to_string())?;
        let ticket: NodeExecutionTicket = serde_json::from_value(
            facts.get("ticket").cloned().ok_or("original ticket missing")?,
        ).map_err(|error| error.to_string())?;
        let entry = outer.get("actions").and_then(serde_json::Value::as_array)
            .and_then(|actions| actions.get(index)).ok_or("original member missing")?;
        let input = normalize_replay_member_input(entry)?;
        let mut semantic_input = input.clone();
        let object = semantic_input.as_object_mut().ok_or("original member input is not an object")?;
        let expected_revision = match object.remove("expected_revision") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::Number(number)) => Some(number.as_u64().ok_or("invalid original revision")?),
            _ => return Err("invalid original revision".into()),
        };
        if prepared.tool == "state_inspect" && (object.contains_key("entry_ref") || object.contains_key("scope_ref")) {
            object.remove("page_cursor"); object.remove("query");
        }
        let semantic_action: harness_contract::agent_action::AgentAction = serde_json::from_value(serde_json::json!({
            "kind": prepared.tool, "input": semantic_input,
        })).map_err(|error| error.to_string())?;
        let identity = harness_contract::step::canonical_digest("agent-action-batch-member-v1", &serde_json::json!({
            "outer": original.idempotency_key, "index": index, "tool": prepared.tool, "input": input,
            "step": binding.snapshot.identity.step_id, "catalog": binding.origin.catalog_digest,
            "session": self.owner.session_id(), "turn": binding.snapshot.identity.turn_id,
            "parent": self.parent, "attempt": self.attempt,
        })).map_err(|error| error.to_string())?;
        if ticket != self.ticket || prepared.index != index || prepared.outer_key != original.idempotency_key
            || entry.get("tool").and_then(serde_json::Value::as_str) != Some(prepared.tool.as_str())
            || prepared.input != input || prepared.identity != identity
            || index_event.payload.get("member_identity").and_then(serde_json::Value::as_str) != Some(prepared.identity.as_str())
            || !harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&prepared.tool.as_str())
            || authorization.tool_id != prepared.tool || authorization.request_id != prepared.identity
            || authorization.authorization_lease.idempotency_key != prepared.identity
            || authorization.authorization_lease.capability != prepared.tool
            || authorization.descriptor_hash != prepared.effect.descriptor_hash
            || authorization.authorization_lease.effect_descriptor_hash != prepared.effect.descriptor_hash
            || authorization.policy_revision == 0
            || authorization.authorization_lease.policy_revision != authorization.policy_revision
            || action.scope != crate::RuntimeEventScope::Program || action.schema_version != 1
            || !matches!(action.kind.as_str(), "agentic.action_applied" | "agentic.completion_rejected")
            || action.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
            || (action.kind == "agentic.action_applied" && action.status.as_deref() != Some("applied"))
            || (action.kind == "agentic.completion_rejected" && action.status.as_deref() != Some("rejected"))
            || (action.kind == "agentic.action_applied" && action.idempotency_key.as_deref()
                != Some(format!("agent-action:{}", envelope.action_id).as_str()))
            || (action.kind == "agentic.completion_rejected" && action.idempotency_key.as_deref()
                != Some(format!("completion-rejected:{}", envelope.action_id).as_str()))
            || envelope.actor.session_id != self.owner.session_id()
            || envelope.actor.turn_id != binding.snapshot.identity.turn_id
            || envelope.actor.original_execution.as_ref().is_none_or(|physical|
                physical.graph_id != self.parent.execution_id || physical.node_id != self.parent.node_id
                    || physical.attempt != self.attempt)
            || envelope.expected_revision != expected_revision || envelope.action != semantic_action
            || action.stream_id != format!("agentic-program:{}", envelope.actor.program_id)
            || index_event.refs != vec![crate::RuntimeEventRef {
                kind: "agentic_program".into(), id: envelope.actor.program_id.clone(),
            }]
        { return Err("original member transaction facts mismatch".into()); }
        let refs = facts.get("original_events").and_then(serde_json::Value::as_array)
            .filter(|refs| !refs.is_empty()).ok_or("original member sources missing")?;
        let mut member_sources = Vec::with_capacity(refs.len());
        // Operation-local memo: multiple original facts may belong to the
        // same source stream or transaction. No state survives this witness.
        let mut source_receipts = BTreeMap::new();
        let mut action_fences = BTreeMap::new();
        for reference in refs {
            let stream = reference.get("stream_id").and_then(serde_json::Value::as_str).ok_or("member source stream missing")?;
            let sequence = reference.get("sequence").and_then(serde_json::Value::as_u64).ok_or("member source sequence missing")?;
            let events = self.event_store.list_stream_before(stream, sequence, 1, 1024 * 1024)
                .map_err(|error| error.to_string())?;
            let event = events.into_iter().next().ok_or("original member source absent")?;
            let actual = serde_json::json!({ "stream_id": event.stream_id, "sequence": event.sequence,
                "event_id": event.event_id, "scope": event.scope, "kind": event.kind,
                "schema_version": event.schema_version, "transaction_id": event.transaction_id,
                "transaction_index": event.transaction_index, "commit_cursor": event.commit_cursor,
                "idempotency_key": event.idempotency_key });
            if !source_receipts.contains_key(&event.transaction_id) {
                let receipt = self.event_store.original_transaction_receipt(&event.transaction_id)
                    .map_err(|error| error.to_string())?.ok_or("original member source receipt missing")?;
                source_receipts.insert(event.transaction_id.clone(), receipt);
            }
            if !action_fences.contains_key(stream) {
                let fence = self.event_store.transaction_stream_revision_exact(&action.transaction_id, stream)
                    .map_err(|error| error.to_string())?.ok_or("member source not fenced by action")?;
                action_fences.insert(stream.to_owned(), fence);
            }
            let source_receipt = source_receipts.get(&event.transaction_id).ok_or("member receipt missing")?;
            let fence = action_fences.get(stream).ok_or("member source fence missing")?;
            if &actual != reference || source_receipt.transaction_id != event.transaction_id
                || source_receipt.commit_cursor != event.commit_cursor
                || source_receipt.event_ids.get(event.transaction_index as usize) != Some(&event.event_id)
                || fence.expected_revision < event.sequence || event.commit_cursor >= action.commit_cursor
            { return Err("original member source mismatch".into()); }
            member_sources.push(event);
        }
        let lease = &authorization.authorization_lease;
        let mut assessments = member_sources.iter().filter(|event|
            event.kind == "authorization.capability_assessed"
                && event.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str)
                    == Some(lease.lease_id.as_str()));
        let assessment_event = assessments.next().ok_or("original member assessment missing")?;
        let assessment: harness_contract::policy::CapabilityAssessment = serde_json::from_value(assessment_event.payload.clone())
            .map_err(|error| error.to_string())?;
        if assessments.next().is_some() || assessment_event.stream_id != format!("session:{}", self.owner.session_id())
            || assessment_event.scope != crate::RuntimeEventScope::Tool
            || assessment_event.actor.as_deref() != Some("conversation_runtime")
            || assessment_event.schema_version != 1 || assessment.capability != prepared.tool
            || assessment_event.status.as_deref() != Some(format!("{:?}", assessment.path).to_ascii_lowercase().as_str())
            || !assessment_event.refs.contains(&crate::RuntimeEventRef { kind: "capability".into(), id: prepared.tool.clone() })
            || !assessment_event.refs.contains(&crate::RuntimeEventRef { kind: "authorization_lease".into(), id: lease.lease_id.clone() })
            || assessment.lease.as_ref() != Some(lease)
        { return Err("original member assessment mismatch".into()); }
        let mut transitions = member_sources.iter().filter(|event|
            event.kind == "authorization.lease_transition"
                && event.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str)
                    == Some(lease.lease_id.as_str()));
        let transition_event = transitions.next().ok_or("original member lease transition missing")?;
        let transition: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(transition_event.payload.clone())
            .map_err(|error| error.to_string())?;
        if transitions.next().is_some() || transition_event.stream_id != format!("authorization-lease:{}", lease.lease_id)
            || transition_event.scope != crate::RuntimeEventScope::Tool
            || transition_event.actor.as_deref() != Some("conversation_runtime")
            || transition_event.schema_version != 1 || transition_event.status.as_deref() != Some("consumed")
            || transition.kind != harness_contract::policy::AuthorizationLeaseTransitionKind::Consumed
            || transition.idempotency_key.as_deref() != Some(prepared.identity.as_str())
            || transition.lease.lease_id != lease.lease_id || transition.lease.principal_id != lease.principal_id
            || transition.lease.capability != lease.capability || transition.lease.policy_revision != lease.policy_revision
            || transition.lease.effect_descriptor_hash != lease.effect_descriptor_hash
            || transition_event.idempotency_key.as_deref() != Some(transition.transition_id.as_str())
            || transition_event.transaction_id != format!("authorization-transition:{}", transition.transition_id)
            || transition_event.refs != vec![crate::RuntimeEventRef { kind: "authorization_lease".into(), id: lease.lease_id.clone() }]
        { return Err("original member consume transition mismatch".into()); }
        Ok(OriginalMemberReplayWitness { store: Arc::clone(&self.event_store), ticket, outer: original.clone(), index, prepared,
            index_event, action, member_sources, action_receipt: receipt })
    }
}

/// Whether the original Host call's member may be prepared.
///
/// The owner slot answers for the effect families that install one. When
/// the slot is empty the durable admission fact answers the same question:
/// the original call's own effect stream holds its admission intent, read
/// by the one reading of that fact. A fact that cannot be read is never an
/// admission, so the member stays unknown.
///
/// The refusal is the admission owner's own named refusal, built by the one
/// wording function, so the run says which member was refused and why: the
/// original call holds no readable durable admission fact. This path never
/// invents a reason of its own, and it names the same fact the other gates
/// read.
pub(crate) fn original_host_member_admission(
    owner: Option<Arc<crate::execution_core::graph::OriginalFreshEffectOwner>>,
    store: &crate::runtime_event_store::RuntimeEventStore,
    original: &crate::RuntimeToolExecutionRequest,
    attempt: u32,
) -> Result<(), String> {
    if crate::agent_in_process_worker::original_member_admitted(owner, store, &original.idempotency_key) {
        return Ok(());
    }
    let parent_reason = crate::agent_in_process_worker::current_process_member_custody(original)
        .ok()
        .flatten()
        .and_then(|custody| custody.parent_refusal_reason(&original.tool_use_id));
    Err(crate::agent_in_process_worker::original_effect_admission_missing_reason(
        &original.tool_use_id,
        &original.idempotency_key,
        &original.tool_name,
        attempt,
        parent_reason.as_deref(),
    ))
}

#[async_trait]
impl crate::tool_host::AgentBatchPolicyOwner for OriginalBatchPolicyOwner {
    async fn prepare_member_pregrant(&self, original: &crate::RuntimeToolExecutionRequest, index: usize)
        -> crate::tool_host::AgentBatchMemberPregrant {
        use crate::tool_host::{AgentBatchMemberPregrant, BatchMemberUnknown, BatchMemberUnknownReason};
        let outer = match self.validate_original_member(original, index).await {
            Ok(outer) => outer,
            Err(reason) => {
                return AgentBatchMemberPregrant::Unknown {
                    failure: BatchMemberUnknown { index, reason: BatchMemberUnknownReason::OriginalRequestNotAuthenticated(reason) },
                    stop: None,
                };
            },
        };
        let coordinate = crate::tool_host::OriginalBatchMemberCoordinate::from_original(original, &self.ticket, index);
        if let Err(reason) = self.validate_original_outer_intent(original) {
            let stop = crate::tool_host::OriginalBatchMemberStop::publish(coordinate,
                crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(reason.clone()));
            return AgentBatchMemberPregrant::Unknown {
                failure: BatchMemberUnknown { index, reason: BatchMemberUnknownReason::OriginalRequestNotAuthenticated(reason) },
                stop: Some(stop),
            };
        }
        match self.read_retained_original_member(original, index) {
            Ok(Some(RetainedOriginalHostMember::Committed(member))) => {
                return AgentBatchMemberPregrant::Replay(member);
            },
            Ok(Some(RetainedOriginalHostMember::Terminal(publication))) => {
                return AgentBatchMemberPregrant::Terminal(publication);
            },
            Ok(None) => {},
            Err(reason) => {
                if let Ok(witness) = self.prepare_member_replay_witness_from_outer(original, index, &outer) {
                    return AgentBatchMemberPregrant::Replay(crate::tool_host::AuthenticatedBatchMember::from_original_witness(witness));
                }
                if let Ok(publications) = original.tool_progress.checked_original_stop_publications() {
                    if let [publication] = publications.as_slice() {
                        return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown { index, reason },
                            stop: Some(publication.as_ref().clone()) };
                    }
                }
                let stop = crate::tool_host::OriginalBatchMemberStop::publish(coordinate,
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown("original retained member is unresolved".into()));
                return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown { index, reason }, stop: Some(stop) };
            }
        }
        // Only the original effect owner can install this same-call fact.
        // It must have been produced by the successful initial intent append,
        // never by an idempotent retry. The effect owner verifies the complete
        // ticket/request/store binding while acquiring the durable fence.
        //
        // An empty slot is not yet a refusal: the Read family installs no
        // owner, so the slot is empty for a call the runtime did admit. The
        // durable admission fact answers the same question in every family,
        // read by the one reading of that fact, which is the fact the
        // closure and the other two admission gates read. A fact that
        // cannot be read is never an admission, so a refusal stays a
        // refusal and its text is unchanged.
        let fresh_effect = match original.tool_progress.original_fresh_effect() {
            Ok(owner) => owner,
            Err(reason) => {
                let stop = crate::tool_host::OriginalBatchMemberStop::publish(coordinate,
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(reason.clone()));
                return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason: BatchMemberUnknownReason::OriginalRequestNotAuthenticated(reason),
                }, stop: Some(stop) };
            }
        };
        if let Err(reason) = original_host_member_admission(
            fresh_effect.clone(), &self.event_store, original, self.ticket.attempt,
        ) {
            let stop = crate::tool_host::OriginalBatchMemberStop::publish(coordinate,
                crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(reason.into()));
            return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                index, reason: BatchMemberUnknownReason::OriginalTransactionFactsUnavailable,
            }, stop: Some(stop) };
        }
        // Reserve the original owner's retained slot before running the
        // member hook. This is only process-local custody; the producer below
        // also requires original effect/backend exclusive proof before hook.
        let index_u64 = match u64::try_from(index) {
            Ok(value) => value,
            Err(_) => {
                let stop = crate::tool_host::OriginalBatchMemberStop::publish(coordinate,
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown("original member index overflow".into()));
                return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason: BatchMemberUnknownReason::OriginalTransactionFactsUnavailable,
                }, stop: Some(stop) };
            }
        };
        let reservation = match self.event_store.reserve_original_member(&original.idempotency_key, index_u64, &self.ticket) {
            Ok(reservation) => Arc::new(reservation),
            Err(error) => {
                let error = Arc::new(error);
                let stop = crate::tool_host::OriginalBatchMemberStop::publish(coordinate,
                    crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeStoreError(Arc::clone(&error)));
                return AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason: BatchMemberUnknownReason::OriginalOwnerStore(error),
                }, stop: Some(stop) };
            }
        };
        match self.prepare_original_host_capability(original, index, Arc::clone(&reservation), fresh_effect.as_ref()).await {
            Ok(candidate) => {
                AgentBatchMemberPregrant::Candidate(candidate)
            },
            Err(failure) => {
                // The effect/backend check may itself be uncertain. Retain the
                // exact original reservation; do not infer cancellation.
                let (reason, stop_reason) = match failure.reason {
                    OriginalHostPreparationFailureReason::Preparation(reason) => (
                        BatchMemberUnknownReason::Preparation(reason.clone()),
                        crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(reason),
                    ),
                    OriginalHostPreparationFailureReason::OriginalOwnerStore(error) => (
                        BatchMemberUnknownReason::OriginalOwnerStore(Arc::clone(&error)),
                        crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeStoreError(error),
                    ),
                };
                let stop = match failure.effect_fence {
                    Some(effect_fence) => crate::tool_host::OriginalBatchMemberStop::publish_with_owners(
                        coordinate, stop_reason, reservation, effect_fence),
                    None => crate::tool_host::OriginalBatchMemberStop::publish_with_reservation(
                        coordinate, stop_reason, reservation),
                };
                AgentBatchMemberPregrant::Unknown { failure: BatchMemberUnknown {
                    index, reason,
                }, stop: Some(stop) }
            }
        }
    }




}

/// Pre-grant results stay typed until the original ToolBatch owner decides
/// whether it can publish a complete receipt or only a partial observation.
pub(crate) enum OriginalActionReceiptState {
    Unobserved,
    Complete(HostOriginalReplay),
    CommittedPartial(ConversationMessage, crate::PendingAgentBatch),
}

/// An authenticated original physical outcome with no invented lifecycle
/// timestamps. Its producer remains the original durable effect receipt.
#[derive(Clone)]
pub(crate) struct HostOriginalReplay {
    pub(crate) call: ModelToolCall,
    pub(crate) outcome: crate::RuntimeToolExecutionOutcome,
    pub(crate) original_action: std::sync::Arc<crate::agentic::OriginalReplayedAction>,
    pub(crate) original_request: std::sync::Arc<crate::RuntimeToolExecutionRequest>,
    pub(crate) physical_receipt: std::sync::Arc<crate::DurableRuntimeEvent>,
}

struct HostGovernedToolContext<'a> {
    member_requests: Mutex<HashMap<String, Arc<crate::RuntimeToolExecutionRequest>>>,
    early_request_capture: Option<&'a Mutex<HashMap<String, Arc<crate::RuntimeToolExecutionRequest>>>>,
    batch_policy_owner: Option<Arc<dyn crate::tool_host::AgentBatchPolicyOwner>>,
    action_issuer: Option<Arc<crate::conversation::tool_plane::OwnedRootActionIssuer>>,
    single_captures: Option<Arc<Mutex<HashMap<String, Arc<crate::conversation::tool_plane::OriginalSingleProducerCapture>>>>>,
    host: Arc<dyn crate::RuntimeExecutionHost>,
    event_bus: Option<crate::CowdEventBus>,
    original_producers: Option<&'a HashMap<String, harness_contract::projection::RuntimeActivityBinding>>,
    calls: &'a [ModelToolCall],
    session_id: &'a str,
    sandbox_posture: harness_contract::policy::SandboxPosture,
    policy_revision: u64,
    memory_context: Option<&'a memory::MemoryTurnContext>,
    reality_context: Option<&'a harness_contract::agent::AgentDataLease>,
    model_lease: Option<&'a str>,
    ticket: &'a NodeExecutionTicket,
    execution_decision: Option<&'a crate::execution_core::RuntimeExecutionDecision>,
    strategy_gate: Option<&'a dyn HostOriginalStrategyGate>,
    admission_plan: Option<&'a crate::GovernedToolPlan>,
    tool_authorizations:
        &'a std::collections::HashMap<String, harness_contract::tool::ToolExecutionAuthorization>,
    capability_gaps:
        &'a std::collections::HashMap<String, harness_contract::policy::CapabilityAssessment>,
    policy_refusals: Option<&'a std::collections::HashMap<String, HostPolicyRefusal>>,
    policy_failures: Option<&'a std::collections::HashMap<String, String>>,
    effective_inputs: Option<&'a std::collections::HashMap<String, String>>,
    prepared_effects: Option<&'a std::collections::HashMap<String, crate::conversation::PreparedCallEffect>>,
    prepared_requests: Option<&'a std::collections::HashMap<String, crate::RuntimeToolExecutionRequest>>,
    evaluation_scope_rejections: Option<&'a BTreeMap<String, String>>,
    scope_state: Option<&'a Arc<tokio::sync::Mutex<TurnGraphState>>>,
    services: Option<&'a crate::RuntimeServices>,
    scope_facts: Mutex<HashMap<String, EvaluationScopeRefusal>>,
    unknown_effects: Mutex<HashMap<String, crate::ToolError>>,
    prepared_invocations:
        &'a std::collections::HashMap<String, harness_contract::tool::GovernedToolInvocation>,
    plan_id: &'a str,
    plan_revision: u64,
    observation_wave_sequence: u64,
    execution_plane: &'a Arc<crate::ToolExecutionPlane>,
    physical_owner: &'a Arc<crate::execution_core::graph::executors::ScopedNodeExecutor>,
    commit_service: &'a crate::execution_core::graph::ExecutionCommitService,
    precompleted: Option<&'a BTreeMap<String, crate::conversation::EarlyToolExecutionReceipt>>,
    original_replays: Option<&'a BTreeMap<String, HostOriginalReplay>>,
    idempotency_keys: Option<&'a std::collections::HashMap<String, String>>,
    invocations: Arc<Mutex<HashMap<String, ToolInvocationRecord>>>,
}

fn rejected_tool_invocations(
    session_id: &str,
    calls: &[ModelToolCall],
    safety_category: crate::ToolSafetyCategory,
    plan_id: Option<&str>,
    plan_revision: u64,
    reason: &str,
) -> HashMap<String, ToolInvocationRecord> {
    calls
        .iter()
        .map(|call| {
            let started_at_ms = crate::tool_invocation::now_ms();
            let mut record = ToolInvocationRecord::started(
                session_id,
                0,
                call.id.clone(),
                call.name.clone(),
                &call.input,
                safety_category,
                started_at_ms,
            );
            if let Some(plan_id) = plan_id {
                record = record.with_governed_plan(plan_id, plan_revision);
            }
            (
                call.id.clone(),
                record.failed(
                    ToolFailureKind::ExecutionError,
                    reason,
                    crate::tool_invocation::now_ms(),
                ),
            )
        })
        .collect()
}

fn host_tool_failure_kind(
    terminal: &crate::GovernedToolTaskTerminal<crate::RuntimeToolExecutionOutcome>,
    reason: &str,
) -> ToolFailureKind {
    match terminal {
        crate::GovernedToolTaskTerminal::Refused { .. }
        | crate::GovernedToolTaskTerminal::Blocked { .. } => ToolFailureKind::PermissionDenied,
        crate::GovernedToolTaskTerminal::Panicked { .. } => ToolFailureKind::Panic,
        _ if reason.to_ascii_lowercase().contains("timed out")
            || reason.to_ascii_lowercase().contains("timeout") =>
        {
            ToolFailureKind::Timeout
        }
        _ => ToolFailureKind::ExecutionError,
    }
}

fn host_tool_terminal_reason(
    terminal: &crate::GovernedToolTaskTerminal<crate::RuntimeToolExecutionOutcome>,
) -> String {
    match terminal {
        crate::GovernedToolTaskTerminal::Succeeded(_) => "tool completed".to_string(),
        crate::GovernedToolTaskTerminal::FailedOutput { error, .. }
        | crate::GovernedToolTaskTerminal::Failed { error } => error.clone(),
        crate::GovernedToolTaskTerminal::Refused { reason }
        | crate::GovernedToolTaskTerminal::Cancelled { reason }
        | crate::GovernedToolTaskTerminal::Panicked { reason } => reason.clone(),
        crate::GovernedToolTaskTerminal::Blocked {
            predecessor_id,
            reason,
        } => format!("blocked by predecessor `{predecessor_id}`: {reason}"),
    }
}

fn host_event_preview(value: &str, max_chars: usize) -> String {
    let mut preview = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

fn failed_governed_tool_outcome(
    call: &ModelToolCall,
    category: crate::ToolSafetyCategory,
    error: String,
) -> crate::RuntimeToolExecutionOutcome {
    crate::RuntimeToolExecutionOutcome {
        tool_use_id: call.id.clone(),
        tool_name: call.name.clone(),
        status: crate::RuntimeToolExecutionStatus::Failed,
        category,
        output: None,
        error: Some(error),
        evidence_ref: format!("tool-execution-failed:{}", call.id),
        observed_evidence: Vec::new(),
        pending_agent_batch: None,
    }
}

async fn execute_fenced_runtime_tool_owned(
    host: &dyn crate::RuntimeExecutionHost,
    commit_service: &crate::execution_core::graph::ExecutionCommitService,
    request: &crate::RuntimeToolExecutionRequest,
    effect: Option<&harness_contract::tool::ToolEffectDescriptor>,
    observation: Option<&crate::execution_core::graph::executors::agent_tool::NoDispatchObservation>,
    original_ticket: Option<&NodeExecutionTicket>,
    scope_locks: Option<&crate::execution_core::graph::ScopeLockManager>,
    action_issuer: Option<&crate::conversation::tool_plane::OwnedRootActionIssuer>,
    single_capture: Option<&crate::conversation::tool_plane::OriginalSingleProducerCapture>,
) -> crate::RuntimeToolExecutionOutcome {

    let Some(effect) = effect else {
        if let Some(observation) = observation {
            observation.refuse(
                &request.tool_use_id,
                "registered effect descriptor missing before dispatch".into(),
            );
        }
        return crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Failed,
            category: request.category,
            output: None,
            error: Some(
                "governed tool execution is blocked because its registered effect descriptor is missing"
                    .to_string(),
            ),
            evidence_ref: format!("tool-effect-missing:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        };
    };

    if tools::tool_orchestrator::effect_is_bounded_read_only_compute(effect)
        || request.tool_name == "execute_code" {
        let result = async {
            if let Some(outcome) = commit_service
                .completed_tool_effect(request, effect)
                .map_err(|error| crate::ToolError::unknown(format!("original effect history unavailable: {error}")))?
            {
                return Ok(crate::bound_tool_batch::retained_replay_outcome(outcome));
            }
            let preserve = |stage, error| crate::bound_tool_batch::original_bounded_acquisition_failure(request, commit_service, stage, error);
            let lease = host.bounded_process_lease(request)
                .map_err(|error| preserve(crate::bound_tool_batch::BoundedAcquisitionStage::Lease, error))?;
            let authority = host.bounded_process_authority(request)
                .map_err(|error| preserve(crate::bound_tool_batch::BoundedAcquisitionStage::Authority, error))?;
            let plane = host.bounded_process_plane().ok_or_else(||
                preserve(crate::bound_tool_batch::BoundedAcquisitionStage::Plane,
                    crate::ToolError::new("bounded process plane unsupported")))?;
            crate::bound_tool_batch::execute_bounded_process_observed(
                lease,
                plane,
                authority,
                commit_service.clone(),
                request.clone(),
                effect.clone(),
                None,
                None,
                observation.cloned(),
            )
            .await
        }
        .await;
        return result.unwrap_or_else(|error: crate::ToolError| {
            let mut unknown = error.is_unknown();
            let mut pending = error.pending_agent_batch().cloned();
            if error.original_bounded_acquisition_failure().is_some() {
                // The same-call Host consumer still owns the original Session
                // policy and cancellation scope; defer its checked decision there.
                match original_ticket {
                    Some(ticket) => pending = error.original_bounded_acquisition_pending(ticket, None),
                    None => unknown = true,
                }
            }
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: if unknown { crate::RuntimeToolExecutionStatus::Unknown } else { crate::RuntimeToolExecutionStatus::Failed },
                category: request.category,
                output: None,
                error: Some(error.to_string()),
                evidence_ref: format!("tool-effect:{}", request.idempotency_key),
                observed_evidence: Vec::new(),
                pending_agent_batch: pending,
            }
        });
    }
    // Only direct actions take this path. Batch members own their own admission;
    // acquiring the outer scope here could deadlock or lend its authority to them.

    let read_ref_action = request.tool_name == "read_ref"
        && single_capture.is_some_and(|capture| capture.read_ref_target().is_ok_and(|target| target.is_some()));
    let direct_action = read_ref_action || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .contains(&request.tool_name.as_str());
    let locked = if direct_action {
        let acquired = async {
            let ticket = original_ticket.ok_or_else(|| crate::ToolError::new("original action ticket missing"))?;
            let parent = request.parent_execution.as_ref().ok_or_else(|| crate::ToolError::new("original action parent missing"))?;
            if parent.execution_id != ticket.graph_id || parent.node_id != ticket.node_id
                || request.parent_execution_attempt != Some(ticket.attempt) {
                return Err(crate::ToolError::new("original action ticket mismatch"));
            }
            let locks = scope_locks.ok_or_else(|| crate::ToolError::new("canonical action scope manager missing"))?;
            crate::conversation::tool_plane::lock_root_effect_scope(
                request, effect, host.pin_step_catalog(), locks, false,
            ).await
        }.await;
        match acquired {
            Ok(locked) => Some(locked),
            Err(error) => {
                if let Some(observation) = observation { observation.refuse(&request.tool_use_id, error.to_string()); }
                return crate::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
                    status: crate::RuntimeToolExecutionStatus::Failed, category: request.category,
                    output: None, error: Some(error.to_string()),
                    evidence_ref: format!("tool-effect:{}", request.idempotency_key), observed_evidence: Vec::new(),
                    pending_agent_batch: error.pending_agent_batch().cloned(),
                };
            }
        }
    } else { None };

    let mut admitted_scope = None;
    let admission = (|| {
        if let Some(outcome) = commit_service.completed_tool_effect(request, effect)? {
            return Ok(crate::execution_core::graph::ToolEffectState::Completed(
                outcome,
            ));
        }
        let build_scope =
            || -> Result<harness_contract::goal::ProofEffectScope, crate::ToolError> {
                let current = host.pin_step_catalog();
                let step = request
                    .step_binding
                    .as_ref()
                    .ok_or_else(|| {
                        crate::ToolError::new("effect admission requires the original Step")
                    })?
                    .resolve(current.clone())
                    .map_err(|error| crate::ToolError::new(error.to_string()))?;
                let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(
                    step.lease().workspace_root(),
                )
                .map_err(|error| crate::ToolError::new(error.to_string()))?;
                crate::bound_tool_batch::admitted_effect_scope(request, effect, current, &resolver)
            };

        let scope = match &locked { Some(locked) => Ok(locked.scope.clone()), None => build_scope() }.map_err(|error| {
            if let Some(observation) = observation {
                observation.refuse(&request.tool_use_id, error.to_string());
            }
            crate::execution_core::graph::ExecutionCommitError::InvalidCommand(error.to_string())
        })?;
        admitted_scope = Some(scope);

        commit_service.begin_bound_tool_effect_observed(
            request,
            effect,
            admitted_scope.as_ref().unwrap(),
            observation,
        )
    })();

    match admission {
        Ok(crate::execution_core::graph::ToolEffectState::Completed(mut outcome)) => {
            if direct_action {
                if let Some(observation) = observation { observation.uncertain(request); }
                return crate::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
                    status: crate::RuntimeToolExecutionStatus::Unknown, category: request.category,
                    output: None, error: Some("original action completed after pre-grant replay check; original result requires reconciliation".into()),
                    evidence_ref: format!("tool-effect-uncertain:{}", request.idempotency_key), observed_evidence: Vec::new(),
                    pending_agent_batch: None,
                };
            }
            // A bounded read receipt may have been produced by an interrupted
            // Provider generation whose call id differs. The effect identity
            // is the canonical tool/input fingerprint, while the protocol
            // identity must remain the current model call.
            outcome.tool_use_id.clone_from(&request.tool_use_id);
            outcome.tool_name.clone_from(&request.tool_name);
            outcome.category = request.category;
            for evidence in &mut outcome.observed_evidence {
                evidence.provenance =
                    harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
            }
            outcome
        }
        Ok(crate::execution_core::graph::ToolEffectState::Uncertain) => {
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Unknown,
                category: request.category,
                output: None,
                error: Some(
                    "tool effect is uncertain; non-idempotent execution was not replayed"
                        .to_string(),
                ),
                evidence_ref: format!("tool-effect-uncertain:{}", request.idempotency_key),
                observed_evidence: Vec::new(),
                pending_agent_batch: None,
            }
        }
        Ok(state @ (
            crate::execution_core::graph::ToolEffectState::Fresh
            | crate::execution_core::graph::ToolEffectState::NotRequired
        )) => {
            if let Some(observation) = observation {
                observation.uncertain(request);
            }
            // The admission of a process-class effect is durable before the
            // command runs: the host executor can only report quiescence after
            // the fact, and the receipt may only be settled against an
            // admission that was published first.

            let host_process_admission = match commit_service
                .admit_host_process(request, effect)
                .await
            {
                Ok(admission) => admission,
                Err(error) => {
                    return crate::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Failed,
                        category: request.category,
                        output: None,
                        error: Some(format!(
                            "tool `{}` host process admission failed: {error}",
                            request.tool_name
                        )),
                        evidence_ref: format!(
                            "tool-effect-host-process-unadmitted:{}",
                            request.idempotency_key
                        ),
                        observed_evidence: Vec::new(),
                        pending_agent_batch: None,
                    };
                }
            };
            if let Some(locked) = &locked {
                if let Err(error) = crate::conversation::tool_plane::validate_root_physical_scope(&locked.resolver, &locked.scope) {
                    return crate::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Failed, category: request.category,
                        output: None, error: Some(error.to_string()),
                        evidence_ref: format!("tool-effect:{}", request.idempotency_key), observed_evidence: Vec::new(),
                        pending_agent_batch: error.pending_agent_batch().cloned(),
                    };
                }
            }

            let execute = async {
                if direct_action {
                    let issuer = action_issuer.ok_or_else(|| crate::ToolError::new("original root action issuer unavailable"))?;
                    let ticket = original_ticket.ok_or_else(|| crate::ToolError::new("original action ticket missing"))?;
                    let locked = locked.as_ref().ok_or_else(|| crate::ToolError::new("original action scope lock unavailable"))?;
                    issuer.verify_live().await?;
                    let capture = single_capture.ok_or_else(|| crate::ToolError::unknown("original single-call producer evidence unavailable"))?;
                    issuer.with_verified_root_action(ticket, request, effect, locked,
                        &state, capture,
                        |capability| async move { host.execute_runtime_tool_with_action_invocation(request, &capability).await }).await
                } else { Ok(host.execute_runtime_tool(request).await) }
            };
            let result = if effect.effect_kind == harness_contract::tool::ToolEffectKind::Read {
                crate::bound_tool_batch::with_outer_read_receipt_owner(request, execute).await
            } else { execute.await };

            let mut outcome = match result {
                Ok(outcome) => outcome,
                Err(error) => crate::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
                    status: if error.is_unknown() { crate::RuntimeToolExecutionStatus::Unknown } else { crate::RuntimeToolExecutionStatus::Failed }, category: request.category,
                    output: None, error: Some(error.to_string()),
                    evidence_ref: format!("tool-effect:{}", request.idempotency_key), observed_evidence: Vec::new(),
                    pending_agent_batch: error.pending_agent_batch().cloned(),
                },
            };
            if outcome.status == crate::RuntimeToolExecutionStatus::Unknown {
                let terminal = match (original_ticket, outcome.pending_agent_batch.as_ref()) {
                    (Some(ticket), Some(pending)) => commit_service.original_terminal_batch_message(ticket, request, pending, None),
                    _ => Ok(None),
                };
                match terminal {
                    Ok(Some(message)) => {
                        // The member decision is known; the outer effect still needs its own receipt.
                        outcome.status = crate::RuntimeToolExecutionStatus::Failed;
                        outcome.error = Some(message);
                    }
                    // The physical effect may have committed. Preserve the original carrier.
                    Ok(None) | Err(_) => return outcome,
                }
            }
            // A process-class effect owns a physical process tree, so its
            // durable receipt must carry the honest quiescence proof. The
            // host shell is not a sandbox process: it can prove that the
            // command is gone and that nothing of it is still alive, and
            // nothing more. The Runtime kernel is the only authority that may
            // publish that fact, and this commit point is one of the three
            // that must mint it (the bound tool batch and the scoped root
            // effect are the other two).
            let host_process_terminal = if outcome.status
                == crate::RuntimeToolExecutionStatus::Executed
            {
                match crate::host_process_closure::mint_host_process_evidence(
                    commit_service,
                    request,
                    effect,
                    &mut outcome,
                    host_process_admission.as_ref(),
                )
                .await
                {
                Ok(minted) => minted,
                Err(error) => {
                    // An unprovable process terminal is never a successful
                    // effect: the receipt is not committed, so the physical
                    // closure witness keeps failing closed.
                    return crate::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Failed,
                        category: request.category,
                        output: None,
                        error: Some(format!(
                            "tool `{}` host process terminal failed: {error}",
                            request.tool_name
                        )),
                        evidence_ref: format!(
                            "tool-effect-host-process-unproven:{}",
                            request.idempotency_key
                        ),
                        observed_evidence: Vec::new(),
                        pending_agent_batch: outcome.pending_agent_batch.clone(),
                    };
                }
            }
            } else {
                false
            };
            if effect.spawns_process && !host_process_terminal {
                // A process-class effect without a process terminal can never
                // close physically. The witness stays closed, and the
                // production log names the effect instead of leaving the
                // operator with an opaque stall.
                tracing::warn!(
                    tool = %request.tool_name,
                    idempotency_key = %request.idempotency_key,
                    "process-class effect carries no process terminal; its physical closure stays unproven"
                );
            }
            // A read-class effect has no settlement stream for a process
            // terminal, so it keeps its plain read receipt and the witness stays
            // honestly unproven for it.

            let committed = if effect.effect_kind == harness_contract::tool::ToolEffectKind::Read {
                if host_process_terminal {
                    commit_service
                        .commit_host_process_read_closure(
                            request,
                            effect,
                            &outcome,
                            admitted_scope.as_ref().unwrap(),
                            None,
                        )
                        .await
                } else {
                    commit_service.commit_bound_readonly_tool_receipt(
                        request,
                        &outcome,
                        effect,
                        admitted_scope.as_ref().unwrap(),
                    )
                }
            } else if host_process_terminal {
                commit_service
                    .commit_host_process_closure(request, effect, &outcome)
                    .await
            } else {
                commit_service.commit_tool_effect(request, effect, &outcome)
            };

            if let Err(error) = committed {
                return crate::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: crate::RuntimeToolExecutionStatus::Unknown,
                    category: request.category,
                    output: None,
                    error: Some(format!(
                        "tool effect completed but its durable receipt failed: {error}"
                    )),
                    evidence_ref: format!("tool-effect-receipt-failed:{}", request.idempotency_key),
                    observed_evidence: Vec::new(),
                    pending_agent_batch: outcome.pending_agent_batch.clone().map(crate::PendingAgentBatch::with_unresolved_marker),
                };
            }
            outcome
        }
        Err(error) => crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if matches!(&error, crate::execution_core::graph::ExecutionCommitError::EffectAdmissionUnknown { .. }) {
                crate::RuntimeToolExecutionStatus::Unknown
            } else {
                crate::RuntimeToolExecutionStatus::Failed
            },
            category: request.category,
            output: None,
            error: Some(format!(
                "tool effect intent failed before execution: {error}"
            )),
            evidence_ref: format!("tool-effect-intent-failed:{}", request.idempotency_key),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        },
    }
}
fn bound_runtime_tool_request(
    call: &ModelToolCall,
    task: &crate::GovernedToolPlanTask,
    plan_id: &str,
    plan_revision: u64,
    observation_wave_sequence: u64,
    session_id: &str,
    sandbox_posture: harness_contract::policy::SandboxPosture,
    policy_revision: u64,
    memory_context: Option<&memory::MemoryTurnContext>,
    reality_context: Option<&harness_contract::agent::AgentDataLease>,
    model_lease: Option<&str>,
    ticket: &NodeExecutionTicket,
    execution_decision: Option<&crate::execution_core::RuntimeExecutionDecision>,
    authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
    idempotency_key: Option<&String>,
) -> crate::RuntimeToolExecutionRequest {
    crate::RuntimeToolExecutionRequest {
        compiled_invocation: Some(task.invocation.clone()),
        step_binding: call.binding.clone(),
        governed_plan_id: plan_id.to_string(),
        governed_plan_revision: plan_revision,
        observation_wave_sequence,
        idempotency_key: call
            .binding
            .as_ref()
            .map(crate::step_snapshot::StepCallBinding::effect_idempotency_key)
            .or_else(|| idempotency_key.cloned())
            .unwrap_or_else(|| format!("{}:{}", ticket.idempotency_key, call.id)),
        tool_use_id: call.id.clone(),
        tool_name: call.name.clone(),
        input: call.input.clone(),
        category: task.safety_category,
        authorization,
        session_id: Some(session_id.to_string()),
        sandbox_posture,
        policy_revision,
        authorized_scopes: Vec::new(),
        memory_context: memory_context.cloned(),
        reality_context: reality_context.cloned(),
        model_lease: model_lease.map(ToString::to_string),
        parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: ticket.graph_id.clone(),
            node_id: ticket.node_id.clone(),
        }),
        parent_execution_attempt: Some(ticket.attempt),
        execution_decision: execution_decision.cloned(),
        evaluation_isolated: false,
        managed_invocation: None,
        tool_progress: crate::ToolProgressSink::default(),
    }
}

pub(crate) fn tool_outcome_message(outcome: crate::RuntimeToolExecutionOutcome) -> ConversationMessage {
    let is_error = outcome.model_feedback_is_error();
    ConversationMessage::tool_result(
        outcome.tool_use_id,
        outcome.tool_name,
        outcome.output.or(outcome.error).unwrap_or_default(),
        is_error,
    )
}

/// D7/D: terminal synthesis fallback. When the model did not commit a
/// FinalAnswer result_ref, synthesize a structured terminal from the evidence
/// that IS committed instead of failing the whole turn.
fn committed_terminal_answer(
    projection: &harness_contract::execution_graph::ExecutionGraphProjection,
    graph_id: &str,
) -> Result<String, String> {
    if let Some(encoded) = projection
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::InlineModel)
        .filter_map(|node| node.result_ref.as_deref())
        .filter_map(|result_ref| result_ref.strip_prefix("assistant_json:"))
        .next_back()
    {
        return serde_json::from_str::<String>(encoded).map_err(|error| error.to_string());
    }
    let evidence_committed = projection
        .nodes
        .iter()
        .filter(|node| node.result_ref.is_some())
        .count();
    Ok(format!(
        "<synthesized_terminal evidence_committed={evidence_committed} graph={graph_id} />"
    ))
}

fn terminal_delivery_envelope(
    projection: &harness_contract::execution_graph::ExecutionGraphProjection,
    goal_id: &str,
    completion: GoalCompletion,
    objective: &str,
    committing_node_id: &str,
) -> harness_contract::outcome::DeliveryEnvelope {
    use harness_contract::execution_graph::ExecutionNodeStatus;
    use harness_contract::outcome::{
        DeliveryBranchStatus, DeliveryBranchTerminal, DeliveryCoverage, DeliveryEnvelope,
        DeliveryStatus, DeliveryUnresolved, PipelineStatus, UserAnswerContract,
        VerifiedDeliveryEffect, VerifiedDeliveryReference, VerifiedEffectStatus,
        WorkspaceMaterializationReceipt,
    };

    let branch_terminals = projection
        .nodes
        .iter()
        .filter(|node| node.status.is_terminal() || node.node_id == committing_node_id)
        .map(|node| DeliveryBranchTerminal {
            branch_id: node.node_id.clone(),
            execution_id: Some(projection.graph_id.clone()),
            status: if node.node_id == committing_node_id {
                DeliveryBranchStatus::Completed
            } else {
                match node.status {
                    ExecutionNodeStatus::Completed => DeliveryBranchStatus::Completed,
                    ExecutionNodeStatus::Failed => DeliveryBranchStatus::Failed,
                    ExecutionNodeStatus::Cancelled => DeliveryBranchStatus::Cancelled,
                    _ => DeliveryBranchStatus::Blocked,
                }
            },
            result_ref: node.result_ref.clone(),
            failure_ref: node
                .failure
                .as_ref()
                .map(|failure| format!("{}:{}", failure.kind, sha256_digest(&failure.message))),
        })
        .collect::<Vec<_>>();
    let verified_receipts = projection
        .nodes
        .iter()
        .flat_map(|node| {
            node.evidence_refs
                .iter()
                .map(move |reference| VerifiedDeliveryReference {
                    reference_id: reference.evidence_ref.id.clone(),
                    kind: reference.evidence_ref.ref_type.clone(),
                    source_execution_id: Some(node.node_id.clone()),
                })
        })
        .collect::<Vec<_>>();
    let workspace_materializations = projection
        .nodes
        .iter()
        .filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::Materialize
                && node.status == ExecutionNodeStatus::Completed
        })
        .filter_map(|node| {
            node.summary
                .as_deref()
                .and_then(|summary| {
                    serde_json::from_str::<WorkspaceMaterializationReceipt>(summary).ok()
                })
                .filter(|receipt| receipt.reread_verified)
        })
        .collect::<Vec<_>>();
    let verified_artifacts = projection
        .nodes
        .iter()
        .filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::Materialize
                && node.status == ExecutionNodeStatus::Completed
        })
        .flat_map(|node| {
            node.evidence_refs
                .iter()
                .filter(|reference| {
                    reference.is_durable()
                        && reference.evidence_ref.ref_type != "runtime_change"
                        && !reference.retrieval_selector.trim().is_empty()
                })
                .map(move |reference| VerifiedDeliveryReference {
                    reference_id: reference.retrieval_selector.clone(),
                    kind: reference.evidence_ref.ref_type.clone(),
                    source_execution_id: Some(node.node_id.clone()),
                })
        })
        .collect::<Vec<_>>();
    let verified_effects = projection
        .nodes
        .iter()
        .filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::Materialize
        })
        .flat_map(|node| {
            let applied = workspace_materializations
                .iter()
                .find(|receipt| receipt.receipt_id.ends_with(&node.node_id));
            node.usage
                .runtime_write_attempt_paths
                .iter()
                .map(move |path| VerifiedDeliveryEffect {
                    effect_id: applied.map_or_else(
                        || format!("materialization-not-applied:{}:{path}", node.node_id),
                        |receipt| receipt.write_effect_id.clone(),
                    ),
                    kind: "workspace_write".to_string(),
                    status: if applied.is_some() {
                        VerifiedEffectStatus::Applied
                    } else {
                        VerifiedEffectStatus::NotApplied
                    },
                    receipt_ref: applied.map(|receipt| receipt.receipt_id.clone()),
                    source_execution_id: Some(node.node_id.clone()),
                })
        })
        .collect::<Vec<_>>();
    let unresolved = projection
        .nodes
        .iter()
        .filter_map(|node| {
            node.failure.as_ref().map(|failure| DeliveryUnresolved {
                unresolved_id: format!("{}:{}", node.node_id, failure.kind),
                kind: failure.kind.clone(),
                summary: failure.message.clone(),
                source_execution_id: Some(node.node_id.clone()),
                obligation_id: None,
            })
        })
        .collect::<Vec<_>>();
    let pipeline_status = if projection
        .nodes
        .iter()
        .all(|node| node.status.is_terminal() || node.node_id == committing_node_id)
    {
        match completion {
            GoalCompletion::Cancelled => PipelineStatus::Cancelled,
            _ if projection
                .nodes
                .iter()
                .any(|node| node.status == ExecutionNodeStatus::Failed) =>
            {
                PipelineStatus::Failed
            }
            _ => PipelineStatus::Completed,
        }
    } else {
        PipelineStatus::Waiting
    };
    let has_completed = branch_terminals
        .iter()
        .any(|branch| branch.status == DeliveryBranchStatus::Completed);
    let delivery_status = match completion {
        GoalCompletion::Satisfied => DeliveryStatus::Satisfied,
        GoalCompletion::Partial => DeliveryStatus::Partial,
        GoalCompletion::Blocked | GoalCompletion::Failed => DeliveryStatus::Unavailable,
        GoalCompletion::WaitingExternalDecision => DeliveryStatus::Denied,
        GoalCompletion::Cancelled | GoalCompletion::Open => {
            if has_completed {
                DeliveryStatus::Partial
            } else {
                DeliveryStatus::Unavailable
            }
        }
    };
    let revision = projection.revision.max(1);
    DeliveryEnvelope {
        envelope_id: format!(
            "delivery:{}:{}:{}",
            projection.graph_id,
            revision,
            sha256_digest(goal_id)
        ),
        revision,
        objective_id: goal_id.to_string(),
        pipeline_status,
        delivery_status,
        branch_terminals,
        verified_receipts,
        verified_artifacts,
        verified_effects,
        workspace_materializations,
        coverage: DeliveryCoverage::default(),
        unresolved,
        conflicts: Vec::new(),
        cancellation: None,
        user_answer_contract: UserAnswerContract {
            language: crate::conversation::user_reply_language(objective).to_string(),
            format: if objective_requires_strict_json(objective) {
                harness_contract::outcome::UserAnswerFormat::StrictJson
            } else {
                harness_contract::outcome::UserAnswerFormat::Markdown
            },
            ..UserAnswerContract::default()
        },
        created_at_ms: crate::tool_invocation::now_ms(),
    }
}

fn objective_requires_strict_json(objective: &str) -> bool {
    let normalized = objective.to_ascii_lowercase();
    let rejects_json = [
        "不要求json",
        "不要求 json",
        "无需json",
        "无需 json",
        "不要json",
        "不要 json",
        "非json",
        "非 json",
        "not json",
        "no json",
        "without json",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    if rejects_json {
        return false;
    }

    // Mentioning JSON as one acceptable presentation format is not a strict
    // JSON contract. The latter must be an explicit user requirement; in
    // particular, a prompt such as "JSON, Markdown headings, or Field:
    // value" must retain its Markdown terminal candidate instead of sending
    // an already-accepted answer through the narrator fallback.
    [
        "strict json",
        "json only",
        "only json",
        "return json",
        "return a json",
        "respond with json",
        "output json",
        "machine-readable json",
        "机器可读 json",
        "只输出json",
        "只输出 json",
        "仅输出json",
        "仅输出 json",
        "只用json",
        "只用 json",
        "必须使用json",
        "必须使用 json",
        "输出json",
        "输出 json",
        "返回json",
        "返回 json",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn qualified_root_answer(
    answer: &str,
    envelope: &harness_contract::outcome::DeliveryEnvelope,
) -> bool {
    let trimmed = answer.trim();
    if trimmed.is_empty()
        || trimmed.starts_with("<synthesized_terminal")
        || trimmed.contains("<tool_call>")
        || trimmed.contains("```tool_use")
        || trimmed.contains("<function=")
    {
        return false;
    }
    if envelope.user_answer_contract.format == harness_contract::outcome::UserAnswerFormat::StrictJson {
        return serde_json::from_str::<serde_json::Value>(trimmed)
            .is_ok_and(|value| value.is_object());
    }
    // Human-facing contracts do not require a machine format. In particular,
    // a valid JSON-shaped answer is not itself a presentation failure.
    true
}

/// Deterministic fail-closed checks for a root collaboration presentation.
/// These checks deliberately target transport leakage and objectively missing
/// deliverables. They do not try to score prose style or replace model judgment.
fn collaboration_answer_quality_findings(answer: &str, objective: &str) -> Vec<String> {
    let trimmed = answer.trim();
    let normalized = trimmed.to_ascii_lowercase();
    let mut findings = Vec::new();
    if trimmed.is_empty() {
        findings.push("final answer is empty".to_string());
        return findings;
    }
    for marker in [
        "[truncated]",
        "# Verified Team evidence bundle",
        "Runtime delivery facts:",
        COLLABORATION_EVIDENCE_CARRIER_KIND,
        "root_model_synthesis_required",
    ] {
        if trimmed.contains(marker) {
            findings.push(format!(
                "Runtime transport marker leaked into final answer: {marker}"
            ));
        }
    }
    if trimmed.matches("```").count() % 2 != 0 {
        findings.push("final answer contains an unclosed code fence".to_string());
    }
    let source_paths = collaboration_source_paths(trimmed);
    let required_source_paths = if objective.contains("至少六个") {
        6
    } else if objective.contains("至少三个") {
        3
    } else {
        0
    };
    if source_paths.len() < required_source_paths {
        findings.push(format!(
            "final answer contains {} distinct source paths but the objective requires at least {required_source_paths}",
            source_paths.len()
        ));
    }

    for (objective_marker, answer_markers, label) in [
        (
            "已验证事实",
            &["已验证事实", "verified facts"][..],
            "verified facts",
        ),
        (
            "源码推断",
            &["源码推断", "source-grounded inference", "inference"][..],
            "source inference",
        ),
        (
            "未执行的模拟",
            &[
                "未执行的模拟",
                "未执行模拟",
                "unexecuted simulation",
                "not executed",
            ][..],
            "unexecuted simulation",
        ),
        (
            "并发波次",
            &["并发波次", "concurrency wave"][..],
            "concurrency waves",
        ),
        ("关键瓶颈", &["关键瓶颈", "bottleneck"][..], "bottlenecks"),
        (
            "失效模式",
            &["失效模式", "failure mode"][..],
            "failure modes",
        ),
        (
            "容量边界",
            &["容量边界", "capacity bound"][..],
            "capacity boundaries",
        ),
    ] {
        if objective.contains(objective_marker)
            && !answer_markers
                .iter()
                .any(|marker| normalized.contains(&marker.to_ascii_lowercase()))
        {
            findings.push(format!("final answer is missing required {label}"));
        }
    }
    if objective.contains("`C4`") && !trimmed.contains("C4") {
        findings.push("final answer is missing required C4 discussion".to_string());
    }
    if objective.contains("实际消费") && objective.contains("结构化交接") {
        let missing_handoff = [
            "未能看到 team",
            "没有显式的 team",
            "缺少上游 team",
            "未完成对 team",
            "f 未通过",
            "f 未能",
            "f 的上游消费未",
            "完整消费没有发生",
            "完整消费未发生",
            "不能被确认",
            "不能确认",
            "无法得到正面证明",
            "语义载荷内容未",
            "内容级载荷未",
            "输入不完整",
            "missing upstream",
            "did not receive upstream",
        ]
        .iter()
        .any(|marker| normalized.contains(marker));
        let consumed_handoff = [
            "e/f 结构化交接已完整消费",
            "teams e and f consumed the complete upstream",
        ]
        .iter()
        .any(|marker| normalized.contains(marker));
        if missing_handoff || !consumed_handoff {
            findings.push(
                "final answer does not verify complete cross-Team semantic handoff consumption"
                    .to_string(),
            );
        }
    }

    for claim in required_verbatim_claims(objective) {
        if !trimmed.contains(&claim) {
            findings.push(format!(
                "final answer is missing required verbatim claim: {claim}"
            ));
        }
    }
    findings.sort();
    findings.dedup();
    findings
}

/// Reject only explicit contradictions of committed Runtime facts. This is a
/// monotonicity gate, not a prose scorer: omission remains the concern of the
/// ordinary quality gate, while a claim that completed work/effects do not
/// exist can never be committed over a newer DeliveryEnvelope.
fn delivery_truth_contradictions(
    answer: &str,
    envelope: &harness_contract::outcome::DeliveryEnvelope,
) -> Vec<String> {
    use harness_contract::outcome::{DeliveryBranchStatus, DeliveryStatus, VerifiedEffectStatus};

    let normalized = answer.to_ascii_lowercase();
    let contains_any =
        |patterns: &[&str]| patterns.iter().any(|pattern| normalized.contains(pattern));
    let completed_branches = envelope
        .branch_terminals
        .iter()
        .filter(|branch| branch.status == DeliveryBranchStatus::Completed)
        .count();
    let applied_effects = envelope
        .verified_effects
        .iter()
        .filter(|effect| effect.status == VerifiedEffectStatus::Applied)
        .count();
    let mut findings = Vec::new();
    if completed_branches > 0
        && contains_any(&[
            "没有任何团队执行",
            "没有团队被执行",
            "未启动任何团队",
            "没有任何 agent 执行",
            "no team was executed",
            "no teams were executed",
            "no agent was executed",
            "nothing was executed",
        ])
    {
        findings.push(format!(
            "answer denies {completed_branches} committed completed branch(es)"
        ));
    }
    if completed_branches > 1
        && contains_any(&[
            "只有一个团队",
            "仅有一个团队",
            "只有一个节点",
            "only one team",
            "only one node",
        ])
    {
        findings.push(format!(
            "answer collapses {completed_branches} completed branches into one"
        ));
    }
    if envelope.delivery_status == DeliveryStatus::Satisfied
        && contains_any(&[
            "任务没有完成",
            "任务未完成",
            "工作没有完成",
            "task was not completed",
            "work was not completed",
            "nothing completed",
        ])
    {
        findings.push("answer denies a satisfied delivery".to_string());
    }
    if !envelope.workspace_materializations.is_empty()
        && contains_any(&[
            "未生成任何文件",
            "没有生成任何文件",
            "没有写入任何文件",
            "未生成任何产物",
            "no file was produced",
            "no files were produced",
            "no artifact was produced",
            "no workspace write",
        ])
    {
        findings.push(format!(
            "answer denies {} reread-verified workspace materialization(s)",
            envelope.workspace_materializations.len()
        ));
    }
    if applied_effects > 0
        && contains_any(&[
            "没有应用任何变更",
            "未应用任何变更",
            "没有发生写入",
            "no changes were applied",
            "no write was applied",
            "no effects were applied",
        ])
    {
        findings.push(format!(
            "answer denies {applied_effects} committed applied effect(s)"
        ));
    }
    findings.sort();
    findings.dedup();
    findings
}

fn runtime_verified_delivery_fallback(
    envelope: &harness_contract::outcome::DeliveryEnvelope,
) -> String {
    use harness_contract::outcome::{DeliveryBranchStatus, UserAnswerFormat};

    let completed_branches = envelope
        .branch_terminals
        .iter()
        .filter(|branch| branch.status == DeliveryBranchStatus::Completed)
        .map(|branch| branch.branch_id.clone())
        .collect::<Vec<_>>();
    let materializations = envelope
        .workspace_materializations
        .iter()
        .map(|receipt| {
            serde_json::json!({
                "path": receipt.target_path,
                "sha256": receipt.sha256,
                "bytes": receipt.bytes,
                "reread_verified": receipt.reread_verified,
            })
        })
        .collect::<Vec<_>>();
    let unresolved = envelope
        .unresolved
        .iter()
        .map(|item| item.summary.clone())
        .collect::<Vec<_>>();
    let facts = serde_json::json!({
        "delivery_status": envelope.delivery_status,
        "completed_branches": completed_branches,
        "workspace_materializations": materializations,
        "unresolved": unresolved,
        "envelope_id": envelope.envelope_id,
        "envelope_revision": envelope.revision,
    });
    if envelope.user_answer_contract.format == UserAnswerFormat::StrictJson {
        return serde_json::to_string_pretty(&facts).unwrap_or_else(|_| "{}".to_string());
    }
    let chinese = envelope.user_answer_contract.language.starts_with("zh");
    if chinese {
        format!(
            "运行时已验证交付事实：状态为 `{:?}`；已完成分支 {} 个（{}）；已回读校验的工作区文件 {} 个（{}）；未解决事项 {} 个{}。",
            envelope.delivery_status,
            completed_branches.len(),
            completed_branches.join("、"),
            materializations.len(),
            envelope
                .workspace_materializations
                .iter()
                .map(|receipt| format!("{} [{}]", receipt.target_path, receipt.sha256))
                .collect::<Vec<_>>()
                .join("、"),
            unresolved.len(),
            if unresolved.is_empty() {
                String::new()
            } else {
                format!("：{}", unresolved.join("；"))
            }
        )
    } else {
        format!(
            "Runtime-verified delivery: status `{:?}`; {} completed branch(es) ({}); {} reread-verified workspace file(s) ({}); {} unresolved item(s){}.",
            envelope.delivery_status,
            completed_branches.len(),
            completed_branches.join(", "),
            materializations.len(),
            envelope
                .workspace_materializations
                .iter()
                .map(|receipt| format!("{} [{}]", receipt.target_path, receipt.sha256))
                .collect::<Vec<_>>()
                .join(", "),
            unresolved.len(),
            if unresolved.is_empty() {
                String::new()
            } else {
                format!(": {}", unresolved.join("; "))
            }
        )
    }
}

fn required_verbatim_claims(objective: &str) -> BTreeSet<String> {
    const CONTEXT_CHARS: usize = 96;
    const MARKERS: &[&str] = &[
        "原样给出",
        "原样包含",
        "原样输出",
        "逐字给出",
        "逐字包含",
        "逐字输出",
        "include verbatim",
        "output verbatim",
        "state verbatim",
        "include exactly",
        "output exactly",
    ];
    let mut claims = BTreeSet::new();
    for (opening, closing) in [('“', '”'), ('「', '」'), ('\"', '\"')] {
        let mut search_start = 0_usize;
        while let Some(relative_open) = objective[search_start..].find(opening) {
            let open = search_start + relative_open;
            let content_start = open + opening.len_utf8();
            let Some(relative_close) = objective[content_start..].find(closing) else {
                break;
            };
            let close = content_start + relative_close;
            let prefix = objective[..open]
                .chars()
                .rev()
                .take(CONTEXT_CHARS)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<String>()
                .to_ascii_lowercase();
            let claim = objective[content_start..close].trim();
            if !claim.is_empty() && MARKERS.iter().any(|marker| prefix.contains(marker)) {
                claims.insert(claim.to_string());
            }
            search_start = close + closing.len_utf8();
        }
    }
    claims
}

fn collaboration_source_paths(value: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    let mut remaining = value;
    while let Some(start) = remaining.find("crates/") {
        let candidate = &remaining[start..];
        let end = candidate
            .char_indices()
            .find_map(|(index, character)| {
                (!character.is_ascii_alphanumeric() && !matches!(character, '/' | '_' | '-' | '.'))
                    .then_some(index)
            })
            .unwrap_or(candidate.len());
        let candidate = &candidate[..end];
        if candidate.contains(".rs") {
            paths.insert(candidate.to_string());
        }
        remaining = &remaining[start + "crates/".len()..];
    }
    paths
}

fn collaboration_intermediate_quality_findings(answer: &str, source: &str) -> Vec<String> {
    let mut findings = collaboration_answer_quality_findings(answer, "");
    let expected_paths = collaboration_source_paths(source);
    let observed_paths = collaboration_source_paths(answer);
    let missing_paths = expected_paths
        .difference(&observed_paths)
        .cloned()
        .collect::<Vec<_>>();
    if !missing_paths.is_empty() {
        findings.push(format!(
            "intermediate synthesis omitted source paths: {}",
            missing_paths.join(", ")
        ));
    }
    findings.sort();
    findings.dedup();
    findings
}

fn capability_gap_outcome(
    call: &ModelToolCall,
    category: crate::ToolSafetyCategory,
    assessment: &harness_contract::policy::CapabilityAssessment,
) -> crate::RuntimeToolExecutionOutcome {
    let recoverable = assessment.gap.as_ref().is_some_and(|gap| gap.recoverable);
    let payload = serde_json::json!({
        "kind": "capability_gap",
        "assessment": assessment,
        "controlled_recovery_available": recoverable,
        "instruction": if recoverable {
            "Choose one safe alternative or revise the graph using already-authorized capabilities."
        } else {
            "Preserve current evidence and stop retrying this denied capability."
        },
    })
    .to_string();
    crate::RuntimeToolExecutionOutcome {
        tool_use_id: call.id.clone(),
        tool_name: call.name.clone(),
        status: if recoverable {
            crate::RuntimeToolExecutionStatus::Executed
        } else {
            crate::RuntimeToolExecutionStatus::BlockedPermission
        },
        category,
        output: recoverable.then_some(payload.clone()),
        error: (!recoverable).then_some(payload),
        evidence_ref: format!("capability-gap:{}", assessment.assessment_id),
        observed_evidence: Vec::new(),
        pending_agent_batch: None,
    }
}

fn synthetic_capability_gap(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    active_ceiling: crate::PermissionMode,
    reason: String,
) -> harness_contract::policy::CapabilityAssessment {
    let fingerprint = format!(
        "authorization-internal:{}:{}",
        descriptor.tool_id, descriptor.descriptor_hash
    );
    harness_contract::policy::CapabilityAssessment {
        assessment_id: format!("capability-assessment-{}", uuid::Uuid::new_v4()),
        capability: descriptor.tool_id.clone(),
        effect: descriptor.assessment.clone(),
        requested_scopes: descriptor.scopes.clone(),
        required_mode: descriptor.required_permission,
        active_ceiling,
        parent_ceiling: active_ceiling,
        risk: harness_contract::policy::RiskLevel::High,
        path: harness_contract::policy::AuthorizationPath::HardDeny,
        lease: None,
        gap: Some(harness_contract::policy::CapabilityGap {
            fingerprint,
            kind: harness_contract::policy::CapabilityGapKind::CapabilityUnavailable,
            capability: descriptor.tool_id.clone(),
            requested_scopes: descriptor.scopes.clone(),
            required_mode: descriptor.required_permission,
            active_ceiling,
            parent_ceiling: active_ceiling,
            reason: reason.clone(),
            safe_alternatives: Vec::new(),
            recoverable: false,
        }),
        evidence_refs: vec![reason],
        assessed_at_ms: crate::tool_invocation::now_ms(),
    }
}

struct TurnSynthesizeBackend<C: ApiClient, T: ToolExecutor> {
    runtime: Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, T>>>>,
    state: Arc<tokio::sync::Mutex<TurnGraphState>>,
    services: Arc<crate::RuntimeServices>,
}

#[cfg(test)]
mod turn_framing_tests {
    use super::{
        agentic_turn_framing, outstanding_closure_steps, scope_turn_framing_for_role,
        TurnExecutionRole,
    };

    #[test]
    fn framing_without_a_program_reports_absence_without_topology_debt() {
        let services = crate::RuntimeServices::in_memory().expect("services");
        let view = agentic_turn_framing(&services, "no-session", "no-turn", 7, 12, 90_000);
        assert_eq!(view["current"], serde_json::json!("uninitialized"));
        assert_eq!(view["exit_ready"], serde_json::json!(false));
        assert_eq!(view["remaining"], serde_json::json!([]));
        assert_eq!(view["program_source"]["status"], "absent");
        assert_eq!(view["budgets"]["rounds"], serde_json::json!(7));
        assert_eq!(view["budgets"]["tool_calls"], serde_json::json!(12));
        assert_eq!(view["budgets"]["elapsed_ms"], serde_json::json!(90_000));
    }

    #[test]
    fn outstanding_steps_names_every_open_rung() {
        let mut program = crate::AgenticProgramProjection::empty("program:test", "objective:test");
        program.required_team_count = 2;
        let steps = outstanding_closure_steps(&program);
        assert!(steps.contains(&"team".to_string()), "{steps:?}");
        assert!(steps.contains(&"publish_task".to_string()), "{steps:?}");
        assert!(
            !steps.contains(&"objective_complete_request".to_string()),
            "{steps:?}"
        );
    }

    #[test]
    fn delegated_framing_labels_root_closure_as_background_not_its_next_action() {
        let view = serde_json::json!({
            "closure": {
                "current": "independent_review",
                "exit_ready": false,
                "remaining": [{"step": "task_review"}],
                "completion_gap": "task_review"
            },
            "capacity": {
                "note": "While any sub-task is in flight your only role is to coordinate",
                "teams": 3
            },
            "budgets": {"rounds": 4}
        });
        let scoped = scope_turn_framing_for_role(view, TurnExecutionRole::DelegatedLeaf);
        assert!(scoped.get("closure").is_none());
        assert_eq!(
            scoped["program_progress"]["owner"],
            serde_json::json!("RootTurn")
        );
        assert_eq!(
            scoped["program_progress"]["root_closure"]["remaining"][0]["step"],
            serde_json::json!("task_review")
        );
        assert!(scoped["program_progress"]["role_note"]
            .as_str()
            .unwrap()
            .contains("bound Task"));
        assert!(!scoped["capacity"]["note"]
            .as_str()
            .unwrap()
            .contains("only role is to coordinate"));
    }

    #[test]
    fn root_framing_keeps_authoritative_closure_fields() {
        let view = serde_json::json!({
            "closure": {"current": "independent_review", "remaining": []}
        });
        let scoped = scope_turn_framing_for_role(view.clone(), TurnExecutionRole::RootTurn);
        assert_eq!(scoped, view);
    }
}

#[cfg(test)]
mod next_turn_contract_tests {
    use super::*;

    #[test]
    fn zero_participation_has_registration_not_team_or_task_debt() {
        let program = crate::AgenticProgramProjection::empty("program:zero", "objective:zero");
        let view = agentic_closure_view(&program, None);
        assert_eq!(view["facts"]["required_teams"], 0);
        assert_eq!(view["remaining"][0]["step"], "register_delivery");
        assert_eq!(view["remaining"][0]["publication_route"]["file_publication_tool"], "artifact_publish");
        assert_eq!(view["remaining"][0]["publication_route"]["discovery_tool"], "tool_search");
        assert!(view["remaining"][0]["publication_route"]["body_alternative"].as_str().unwrap()
            .contains("message://current"));
        assert!(!view["exit_ready"].as_bool().unwrap());
        for step in view["remaining"].as_array().unwrap() {
            assert_ne!(step["step"], "team");
            assert_ne!(step["step"], "publish_task");
        }
    }

    #[test]
    fn review_gap_without_team_offers_optional_route_not_false_blocker() {
        let program = crate::AgenticProgramProjection::empty("program:zero", "objective:zero");
        let view = agentic_closure_view(&program, Some("original_intent_review_required:user_intent"));
        assert!(view["blocked"].is_null());
        let review = view["remaining"].as_array().unwrap().iter()
            .find(|entry| entry["closing_tool"] == "objective_review").unwrap();
        assert_eq!(review["actionability"], "requires_explicit_independent_actor");
        assert_eq!(review["optional_route"]["team_observation"], "absent");
        assert_eq!(review["optional_route"]["execution"], "optional_explicit_authorized_calls_only");
        assert!(review["optional_route"]["admission"].as_str().unwrap()
            .contains("task_publish(purpose=exploration)"));
        assert_eq!(view["exit_ready"], false);
        assert!(program.teams.is_empty());
        assert!(program.tasks.is_empty());
        assert!(program.completion_request.is_none());
    }

    #[test]
    fn leaf_flat_root_state_is_background_not_action_debt() {
        let scoped = scope_turn_framing_for_role(serde_json::json!({
            "current":"register_delivery","remaining":[{"step":"artifact_commit"}],
            "exit_ready":false,"blocked":{"reason":"review"},"facts":{"required_teams":0}
        }), TurnExecutionRole::DelegatedLeaf);
        for key in ["current", "remaining", "exit_ready", "blocked"] {
            assert!(scoped.get(key).is_none());
        }
        assert_eq!(scoped["program_progress"]["root_closure"]["current"], "register_delivery");
    }

    #[test]
    fn missing_goal_is_unknown_not_empty_known_criteria() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let program = crate::AgenticProgramProjection::empty("program:zero", "objective:zero");
        let refs = citable_refs(&program, &services, "session", "turn");
        assert_eq!(refs["goal"]["status"], "unknown");
        assert!(refs["criteria"].is_null());
        assert_eq!(refs["coverage"]["accepted_results_omitted"], 0);
    }
}

/// The original Host admission boundary. Read errors and a foreign root must
/// fail before installing resolvers or asking the execution owner to recover.
/// Registration and recovery themselves remain with the execution supervisor.
async fn attach_conversation_graph(
    services: &crate::RuntimeServices,
    graph: harness_contract::execution_graph::ExecutionGraph,
    actor: Option<harness_contract::agent_action::AgentActorBinding>,
    source_goal_cut: Option<crate::ExpectedStreamRevision>,
    require_direct_root_identity: bool,
    install_resolvers: impl FnOnce(),
) -> Result<(harness_contract::execution_graph::ExecutionGraph, bool), RuntimeError> {
    let graph_id = graph.id.clone();
    let persisted = match services.graph_state_store().load_async(&graph_id).await {
        Ok(graph) => Some(graph),
        Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. }) => None,
        Err(error) => return Err(RuntimeError::new(error.to_string())),
    };
    let same_root = |stored: &harness_contract::execution_graph::ExecutionGraph| {
        stored.lineage == graph.lineage && stored.parent_execution == graph.parent_execution
    };
    if require_direct_root_identity && persisted.as_ref().is_some_and(|stored| !same_root(stored)) {
        return Err(RuntimeError::new("stored direct root identity mismatch"));
    }
    install_resolvers();
    let recovering = persisted.is_some();
    let attached = if recovering {
        let recovered = services
            .execution_supervisor()
            .recover_graph(&graph_id)
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if require_direct_root_identity && !same_root(&recovered) {
            return Err(RuntimeError::new("recovered direct root identity mismatch"));
        }
        recovered
    } else {
        let original_owner = actor.as_ref().map(|actor| {
            services.original_graph_registration_owner(&graph_id, actor)
        }).transpose().map_err(RuntimeError::new)?.flatten();
        services
            .execution_supervisor()
            .register_continuation_graph(graph, actor, source_goal_cut, original_owner)
            .await
            .map_err(|error| RuntimeError::new(error.to_string()))?
    };
    Ok((attached, recovering))
}

/// The Host member pre-grant used to read an empty owner slot as a refusal,
/// and the Read family installs no owner at all: its members were refused
/// before they were admitted. These assertions read the durable admission
/// fact instead, in the same shape the closure and the other two gates read
/// it, and they keep the refusal wording this path always published.
#[cfg(test)]
mod original_host_admission_tests {
    use super::*;

    /// The original call the Host gate reads: its own request, under the
    /// effect key its durable admission fact is written.
    fn gate_request(effect_key: &str) -> crate::RuntimeToolExecutionRequest {
        let tool_name = harness_contract::agent_action::STATE_INSPECT_TOOL_ID;
        crate::RuntimeToolExecutionRequest {
            idempotency_key: effect_key.to_string(),
            tool_use_id: "host-call".into(),
            tool_name: tool_name.into(),
            input: "{}".into(),
            session_id: Some("host-session".into()),
            ..crate::RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: "host-call".into(),
                    tool_name: tool_name.into(),
                    input: "{}".into(),
                    depends_on: vec![],
                },
            )
        }
    }

    /// The call's own durable admission fact on its effect stream.
    fn append_admission(
        events: &crate::runtime_event_store::RuntimeEventStore,
        effect_key: &str,
        kind: &str,
        suffix: &str,
    ) {
        let stream_id = format!("execution-effect:{effect_key}");
        let idempotency_key = format!("{effect_key}:{suffix}");
        let revision = events.stream_revision(&stream_id).expect("effect head");
        events
            .append_transaction(crate::runtime_event_store::AppendTransactionRequest {
                transaction_id: idempotency_key.clone(),
                expected_streams: vec![crate::runtime_event_store::ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: revision,
                }],
                events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::runtime_event_store::RuntimeEventInput {
                        stream_id: stream_id.clone(),
                        scope: crate::runtime_event_store::RuntimeEventScope::ExecutionNode,
                        kind: kind.to_string(),
                        status: Some("inflight".into()),
                        actor: Some("governed_tool".into()),
                        refs: vec![crate::runtime_event_store::RuntimeEventRef {
                            kind: "tool_invocation".into(),
                            id: "host-call".into(),
                        }],
                        payload: serde_json::json!({
                            "idempotency_key": effect_key,
                            "tool_use_id": "host-call",
                        }),
                    },
                    idempotency_key: Some(idempotency_key),
                    schema_version: 1,
                }],
            })
            .expect("admission");
    }

    /// The Host gate admits a Read-family member whose admission is a
    /// durable fact: that family installs no owner, and the empty slot used
    /// to read as a refusal.
    #[test]
    fn the_host_gate_admits_a_read_family_member_with_a_durable_admission() {
        let events = crate::runtime_event_store::RuntimeEventStore::for_test();
        append_admission(&events, "host-read", "execution.read.intent", "read-intent");
        assert_eq!(
            original_host_member_admission(None, &events, &gate_request("host-read"), 1),
            Ok(()),
            "a read admission admits the Host member"
        );
    }

    /// The Read-family admission the production writer publishes is the fact
    /// the Host gate reads. The fact is written by the production writer
    /// itself, not by a copy of it: the writer publishes the effect, the
    /// replay contract, the input digest and the resolved scope of the very
    /// call, and the gate reads exactly that.
    #[test]
    fn the_host_gate_admits_a_read_member_admitted_by_the_production_writer() {
        let workspace = tempfile::tempdir().expect("workspace");
        let host = tools::ToolHost::builtin("host-read-writer", workspace.path());
        let request = crate::bound_tool_batch::scope_admission_tests::bound_request(
            &host, "read_file", serde_json::json!({"path": "a"}),
        );
        let effect = request.compiled_invocation.as_ref().expect("invocation").effect.clone();
        let events = Arc::new(crate::runtime_event_store::RuntimeEventStore::for_test());
        let service = crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(&events));
        service.begin_tool_effect(&request, &effect).expect("read admission");
        assert_eq!(
            original_host_member_admission(None, &events, &request, 1),
            Ok(()),
            "the Host gate admits a member whose read admission the production writer published"
        );
    }

    /// The negative control: an original without a durable admission fact
    /// is still refused by the Host gate.
    #[test]
    fn the_host_gate_refuses_a_member_without_a_durable_admission() {
        let events = crate::runtime_event_store::RuntimeEventStore::for_test();
        assert_eq!(
            original_host_member_admission(None, &events, &gate_request("host-absent"), 1).is_err(),
            true,
            "an absent admission refuses the Host member"
        );
    }

    /// An admission that cannot be read is not an admission: the Host gate
    /// keeps refusing instead of excusing the member. The fact is not
    /// downgraded to "no admission"; it stays unreadable and refused.
    #[test]
    fn the_host_gate_refuses_a_member_with_an_unreadable_admission() {
        let events = crate::runtime_event_store::RuntimeEventStore::for_test();
        append_admission(
            &events,
            "host-foreign",
            "execution.effect.member_noncommit",
            "intent",
        );
        assert_eq!(
            original_host_member_admission(None, &events, &gate_request("host-foreign"), 1).is_err(),
            true,
            "an unreadable admission refuses the Host member"
        );
    }

    /// The refusal keeps the wording this path publishes: the same named
    /// refusal the other admission gates publish, naming the missing durable
    /// admission fact and the member it refused.
    #[test]
    fn the_host_gate_keeps_its_own_refusal_wording() {
        let events = crate::runtime_event_store::RuntimeEventStore::for_test();
        let request = gate_request("host-wording");
        let reason = original_host_member_admission(None, &events, &request, 3)
            .expect_err("an absent admission refuses the Host member");
        assert!(
            reason.starts_with("original_effect_admission_missing"),
            "the refusal keeps its name: {reason}"
        );
        assert!(reason.contains("tool_use_id=host-call"), "{reason}");
        assert!(reason.contains("effect_key=host-wording"), "{reason}");
        assert!(reason.contains("attempt=3"), "{reason}");
        assert!(
            reason.contains("durable_admission_fact=absent_or_unreadable"),
            "the refusal names the durable admission fact: {reason}"
        );
    }
}
