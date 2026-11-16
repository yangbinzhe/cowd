//! Durable goal state and projections owned by Runtime.
//!
//! The store uses RuntimeEventStore goal streams directly. It deliberately has
//! no scheduler, model loop, or Gateway dependency; interventions remain
//! proposals until RuntimeExecutionSupervisor applies them at a checkpoint.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::Digest;

pub(crate) mod assessment;
pub mod policy;
pub(crate) use assessment::AssessmentSourceFrame;
mod program_completion;
pub(crate) use program_completion::delivered_result_refs;
pub mod supervisor;

pub(crate) use program_completion::{OriginalConclusionProofCustody, PreparedProgramConclusion};

pub use supervisor::{ObjectiveReconcileDecision, ObjectiveSupervisor};

use harness_contract::goal::{
    AcceptanceStatus, GoalCompletion, GoalContract, GoalProgressSnapshot, GoalRevision,
    ObjectiveObligationState, ObjectiveRecoveryState, ObjectiveRecoveryStatus, ObjectiveTerminal,
    ObjectiveTerminalKind, ResolutionDeltaKind, RuntimeIntervention, RuntimeInterventionTrace,
    RuntimeObservation,
};

use crate::{
    runtime_event_store::RuntimeTransactionEventInput, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore,
};

/// Record the identity of the current admitted input and its unresolved path
/// proposals. A parser's output proves neither intent nor permission. The
/// original AI planning/Goal refinement route must resolve those proposals;
/// lack of a preselected filename is never an admission failure here.
pub(crate) fn compile_goal_input_contract(
    binding: &harness_contract::goal::GoalExecutionBinding,
    source_ref: &str,
    input_revision: u64,
    input: &str,
    scope_proposals: &[String],
) -> Result<harness_contract::goal::GoalInputContract, String> {
    if source_ref.trim().is_empty()
        || input_revision == 0
        || binding.session_id.is_empty()
        || binding.turn_id.is_empty()
    {
        return Err("goal_input_identity_incomplete".into());
    }
    Ok(harness_contract::goal::GoalInputContract {
        version: 1,
        principal_id: format!("session:{}", binding.session_id),
        session_id: binding.session_id.clone(),
        turn_id: binding.turn_id.clone(),
        source_ref: source_ref.into(),
        input_revision,
        content_sha256: format!("{:x}", sha2::Sha256::digest(input.as_bytes())),
        commitments: Vec::new(),
        proposed_workspace_scopes: scope_proposals.to_vec(),
    })
}

pub(crate) fn goal_workspace_write_scopes(goal: &GoalContract) -> Vec<String> {
    goal.input_contract
        .as_ref()
        .map(|contract| {
            contract
                .commitments
                .iter()
                .filter_map(|item| match item {
                    harness_contract::goal::GoalInputCommitment::WorkspaceWrite {
                        path, ..
                    } => Some(
                        path.as_ref()
                            .map(|path| format!("write:{path}"))
                            .unwrap_or_default(),
                    ),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Restrict an otherwise authorized invocation by the current accepted
/// business contract. This cannot grant a tool or workspace permission.
/// Registered descriptor targets, not model argument text, prove disjointness.
/// Current accepted business quantities, never a role or delegation grant.
/// None retains the original frozen contract for legacy, unauthenticated input.
pub(crate) fn current_goal_team_bounds(goal: &GoalContract) -> Option<(u8, Option<u8>)> {
    goal.input_contract
        .as_ref()
        .filter(|contract| contract.version == 2)
        .map(|contract| contract.team_bounds().unwrap_or((0, None)))
}

pub(crate) fn goal_tool_effect_rejection(
    goal: &GoalContract,
    effect: &harness_contract::tool::ToolEffectDescriptor,
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> Option<&'static str> {
    use harness_contract::{
        goal::GoalInputCommitment,
        policy::{PermissionOperation, PermissionResource},
    };
    let contract = goal.input_contract.as_ref()?;
    if contract.forbids_tools() {
        return Some("current Goal prohibits tool calls");
    }
    if effect.required_permission == harness_contract::tool::ToolPermissionMode::ReadOnly {
        return None;
    }
    for commitment in &contract.commitments {
        let GoalInputCommitment::ForbidWorkspaceWrite { path, .. } = commitment else {
            continue;
        };
        let Some(forbidden) = path else {
            return Some("current Goal prohibits workspace mutation");
        };
        let Ok(forbidden) = resolver.resolve_planned_file(forbidden) else {
            return Some("current Goal forbidden path cannot be resolved");
        };
        // A process/system/unknown scope cannot prove that it leaves the
        // prohibited object untouched. In particular shell text is not proof.
        if effect.spawns_process
            || effect.mutates_packages
            || effect.mutates_system
            || effect.scopes.is_empty()
        {
            return Some("current Goal requires disjoint workspace effects");
        }
        for scope in &effect.scopes {
            if scope.operation == PermissionOperation::Read {
                continue;
            }
            if scope.resource != PermissionResource::File {
                return Some("current Goal requires disjoint workspace effects");
            }
            let Some(target) = scope.target.as_deref() else {
                return Some("current Goal requires exact workspace targets");
            };
            let Ok(target) = resolver.resolve_planned_file(target) else {
                return Some("current Goal effect target cannot be resolved");
            };
            let a = std::path::Path::new(&forbidden.workspace_relative_path);
            let b = std::path::Path::new(&target.workspace_relative_path);
            if a.starts_with(b) || b.starts_with(a) {
                return Some("current Goal prohibits mutation of this workspace object");
            }
        }
    }
    None
}

/// Match required business artifacts against Runtime-authorized write receipts.
/// Prediction and model text never satisfy an effect obligation. This pure
/// predicate belongs to Goal; callers may collect evidence but not redefine it.
pub(crate) fn write_obligation_satisfied(
    required_write_for_completion: bool,
    required_scopes: &[String],
    observed_evidence: &[harness_contract::context::ObservedEvidence],
    unscoped_committed_write: bool,
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> bool {
    if !required_write_for_completion {
        return true;
    }
    if required_scopes.is_empty() {
        return unscoped_committed_write
            || observed_evidence.iter().any(|evidence| {
                matches!(
                    &evidence.target,
                    harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                        if scope.access_mode
                            == harness_contract::context::WorkspaceAccessMode::Write
                )
            });
    }
    required_scopes.iter().all(|required| {
        if required.is_empty() {
            return write_obligation_satisfied(
                true,
                &[],
                observed_evidence,
                unscoped_committed_write,
                resolver,
            );
        }
        let required = resolver.compile_obligation_or_unresolved(required);
        observed_evidence
            .iter()
            .any(|observed| crate::path_identity::observed_evidence_satisfies(&required, observed))
    })
}

/// Source binding for an answer already committed by a successful model
/// attempt. This is observation metadata in the original Goal journal, not a
/// second result store or a claim of semantic correctness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GoalTurnCandidate {
    pub goal_id: String,
    pub spec_revision: u64,
    pub spec_digest: String,
    pub source_ref: String,
    pub graph_id: String,
    pub node_id: String,
    pub attempt: u32,
    pub answer_sha256: String,
}

fn turn_candidate_key(graph: &str, node: &str, attempt: u32) -> String {
    format!("goal-final-candidate:{graph}:{node}:{attempt}")
}

impl GoalStore {
    pub(crate) fn turn_candidate_event_at(
        &self,
        source: &GoalProjection,
        observation: &RuntimeObservation,
        ticket: &crate::execution_core::NodeExecutionTicket,
        answer: &str,
    ) -> Result<RuntimeTransactionEventInput, String> {
        if answer.trim().is_empty()
            || ticket.attempt == 0
            || observation.identity.graph_id != ticket.graph_id
            || observation.identity.node_id.as_deref() != Some(ticket.node_id.as_str())
        {
            return Err("goal_final_candidate_identity_or_payload_missing".into());
        }
        let goal = &source.goal;
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or("goal_final_candidate_binding_missing")?;
        #[cfg(test)]
        if std::env::var_os("COWD_R622_TRACE").is_some() {
            eprintln!("r622_toolbatch phase=goal_candidate_binding graph_match={} session_match={} turn_present={} turn_match={}",
                binding.root_execution_id == ticket.graph_id,
                binding.session_id == observation.identity.session_id,
                observation.identity.turn_id.is_some(),
                observation.identity.turn_id.as_deref() == Some(binding.turn_id.as_str()));
        }
        if binding.root_execution_id != ticket.graph_id
            || binding.session_id != observation.identity.session_id
            || observation.identity.turn_id.as_deref() != Some(binding.turn_id.as_str())
        {
            return Err("goal_final_candidate_crosses_input_binding".into());
        }
        let candidate = GoalTurnCandidate {
            goal_id: goal.id.clone(),
            spec_revision: goal.spec_revision,
            spec_digest: goal.spec_digest.clone(),
            source_ref: goal
                .source_intent_ref
                .clone()
                .ok_or("goal_final_candidate_source_missing")?,
            graph_id: ticket.graph_id.clone(),
            node_id: ticket.node_id.clone(),
            attempt: ticket.attempt,
            answer_sha256: format!("{:x}", sha2::Sha256::digest(answer.as_bytes())),
        };
        let mut event = self.observation_event_at(
            source,
            observation,
            turn_candidate_key(&ticket.graph_id, &ticket.node_id, ticket.attempt),
        )?;
        let value = serde_json::to_value(candidate).map_err(|error| error.to_string())?;
        if event
            .event
            .payload
            .get("answer_candidate")
            .is_some_and(|original| original != &value)
        {
            return Err("goal_final_candidate_idempotency_conflict".into());
        }
        event.event.payload["answer_candidate"] = value;
        Ok(event)
    }

    /// Read only genuine observation bodies from this exact specification.
    /// Each page is bounded and the cut is fixed; there is no eight-event
    /// exception or full-stream materialisation. Prior-revision receipts
    /// cannot satisfy a newly admitted artifact contract.
    pub(crate) fn turn_writes_satisfied(
        &self,
        source: &GoalProjection,
        resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    ) -> Result<bool, String> {
        self.turn_writes_satisfied_with_pending(source, resolver, &[])
    }

    fn turn_writes_satisfied_with_pending(
        &self,
        source: &GoalProjection,
        resolver: &crate::path_identity::WorkspacePathIdentityResolver,
        pending: &[harness_contract::context::ObservedEvidence],
    ) -> Result<bool, String> {
        let scopes = goal_workspace_write_scopes(&source.goal);
        if scopes.is_empty() {
            return Ok(true);
        }
        // Current ToolHost facts have not yet entered the original Graph
        // transaction. They are evaluated here, against the same pinned spec,
        // and the caller must commit their observation with this source cut.
        let mut covered = scopes
            .iter()
            .map(|scope| {
                write_obligation_satisfied(
                    true,
                    if scope.is_empty() {
                        &[]
                    } else {
                        std::slice::from_ref(scope)
                    },
                    pending,
                    false,
                    resolver,
                )
            })
            .collect::<Vec<_>>();
        if covered.iter().all(|value| *value) {
            return Ok(true);
        }
        let mut cursor = 0;
        while cursor < source.stream_revision {
            let page = self.event_store.list_stream_after(
                &stream_id(&source.goal.id),
                cursor,
                source.stream_revision,
                100,
                1024 * 1024,
            )?;
            if page.is_empty() {
                return Err("goal_effect_observation_tail_gap".into());
            }
            for event in page {
                if event.sequence != cursor + 1 {
                    return Err("goal_effect_observation_tail_gap".into());
                }
                cursor = event.sequence;
                if event.kind != "goal.observation"
                    || event.payload["goal_source"]["spec_digest"] != source.goal.spec_digest
                {
                    continue;
                }
                let body = self.event_store.resolve_observation_record(&event)?;
                let observation: RuntimeObservation =
                    serde_json::from_value(body).map_err(|error| error.to_string())?;
                if observation.goal_id() != source.goal.id
                    || observation.identity.session_id != source.goal.session_id
                {
                    return Err("goal_effect_observation_identity_mismatch".into());
                }
                for (index, scope) in scopes.iter().enumerate() {
                    covered[index] |= write_obligation_satisfied(
                        true,
                        if scope.is_empty() {
                            &[]
                        } else {
                            std::slice::from_ref(scope)
                        },
                        &observation.observed_evidence,
                        false,
                        resolver,
                    );
                }
                if covered.iter().all(|covered| *covered) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// The original native Goal review identity, shared by its writer and reader.
pub(crate) fn objective_review_record_id(
    goal_id: &str,
    action_id: &str,
    criterion: &str,
) -> String {
    format!(
        "objective_review:{:x}",
        sha2::Sha256::digest(format!("{goal_id}|{action_id}|{criterion}").as_bytes())
    )
}

/// Transient decisions of the existing Goal owner, never another writer.
#[derive(Debug)]
pub(crate) enum GoalWriteDisposition {
    Satisfied,
    Pending,
    Recover(PreparedGoalMutation),
    Blocked(RuntimeIntervention),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum GoalWriteCheckpoint {
    FinalAnswer,
    ToolProgress {
        bounded_role: bool,
        saturated: bool,
        write_attempted: bool,
    },
}

#[derive(Clone)]
pub struct GoalStore {
    event_store: Arc<RuntimeEventStore>,
    proof_artifacts: Option<Arc<crate::ArtifactStore>>,
    process_input_verifier: Option<crate::agentic::review_evidence::ProcessInputVerifier>,
    proof_session_queries: Option<Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>>,
}

/// Read model reconstructed exclusively from the durable Goal stream. Runtime
/// keeps no second mutable goal map: callers can drop this projection at any
/// time and rebuild it from `RuntimeEventStore`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjection {
    pub goal: GoalContract,
    pub stream_revision: u64,
    pub observations: Vec<RuntimeObservation>,
    pub interventions: Vec<RuntimeIntervention>,
    pub intervention_traces: Vec<RuntimeInterventionTrace>,
    pub progress: GoalProgressSnapshot,
}

impl GoalProjection {
    /// Initial execution already adopted this exact Session message in the
    /// goal.created snapshot. This is classification, never a fresh grant or
    /// an amendment receipt. Continuations may carry a different original source.
    pub(crate) fn binds_initial_input(
        &self,
        session: &str,
        turn: &str,
        request: &str,
        message: &str,
    ) -> bool {
        if [session, turn, request, message].iter().any(|value| value.is_empty()) {
            return false;
        }
        self.goal.session_id == session
            && self.goal.source_intent_ref.as_deref()
                == Some(format!("session_message:{message}").as_str())
            && self.goal.execution_binding.as_ref().is_some_and(|binding| {
                binding.session_id == session
                    && binding.turn_id == turn
                    && binding.root_execution_id
                        == crate::session_execution::session_ingress_graph_id(session, request, turn)
            })
    }
}

pub struct GoalProgressReducer;

impl GoalProgressReducer {
    #[must_use]
    pub fn from_goal(goal: &GoalContract) -> GoalProgressSnapshot {
        GoalProgressSnapshot {
            goal_id: goal.id.clone(),
            goal_revision: goal.revision,
            observation_count: 0,
            criteria: goal
                .criteria
                .iter()
                .map(|criterion| (criterion.id.clone(), criterion.status))
                .collect(),
            evidence_refs: sorted_unique(goal.evidence_refs.clone()),
            invalidated_evidence_refs: Vec::new(),
            effects: std::collections::BTreeMap::new(),
            open_conflicts: sorted_unique(goal.blockers.clone()),
            open_unknowns: sorted_unique(goal.unresolved.clone()),
            cumulative_cost: Default::default(),
            last_observed_at_ms: 0,
            applied_observation_keys: Vec::new(),
        }
    }

    pub fn reconcile_goal(
        snapshot: &mut GoalProgressSnapshot,
        previous_goal: &GoalContract,
        goal: &GoalContract,
    ) {
        snapshot.goal_revision = goal.revision;
        for criterion in &goal.criteria {
            let previous_status = previous_goal
                .criteria
                .iter()
                .find(|previous| previous.id == criterion.id)
                .map(|previous| previous.status);
            if previous_status.is_none() || previous_status != Some(criterion.status) {
                snapshot
                    .criteria
                    .insert(criterion.id.clone(), criterion.status);
            }
        }
        snapshot.criteria.retain(|criterion_id, _| {
            goal.criteria
                .iter()
                .any(|criterion| &criterion.id == criterion_id)
        });
        snapshot
            .evidence_refs
            .extend(goal.evidence_refs.iter().cloned());
        normalize(&mut snapshot.evidence_refs);
        for removed in previous_goal
            .blockers
            .iter()
            .filter(|blocker| !goal.blockers.contains(blocker))
        {
            snapshot.open_conflicts.retain(|current| current != removed);
        }
        snapshot.open_conflicts.extend(
            goal.blockers
                .iter()
                .filter(|blocker| !previous_goal.blockers.contains(blocker))
                .cloned(),
        );
        normalize(&mut snapshot.open_conflicts);
        for removed in previous_goal
            .unresolved
            .iter()
            .filter(|unknown| !goal.unresolved.contains(unknown))
        {
            snapshot.open_unknowns.retain(|current| current != removed);
        }
        snapshot.open_unknowns.extend(
            goal.unresolved
                .iter()
                .filter(|unknown| !previous_goal.unresolved.contains(unknown))
                .cloned(),
        );
        normalize(&mut snapshot.open_unknowns);
    }

    /// Apply one typed observation exactly once. Human summaries and generic
    /// numeric scores are deliberately absent from the reducer.
    pub fn apply(
        snapshot: &mut GoalProgressSnapshot,
        observation: &RuntimeObservation,
    ) -> Result<bool, String> {
        validate_observation(observation)?;
        if snapshot.goal_id != observation.goal_id() {
            return Err(format!(
                "observation goal {} does not match snapshot {}",
                observation.goal_id(),
                snapshot.goal_id
            ));
        }
        let key = observation.idempotency_fingerprint();
        if snapshot
            .applied_observation_keys
            .iter()
            .any(|existing| existing == &key)
        {
            return Ok(false);
        }
        for delta in &observation.criterion_deltas {
            let current = snapshot
                .criteria
                .get(&delta.criterion_id)
                .copied()
                .ok_or_else(|| format!("unknown goal criterion {}", delta.criterion_id))?;
            if current != delta.previous {
                return Err(format!(
                    "criterion {} delta is stale: expected {:?}, actual {:?}",
                    delta.criterion_id, delta.previous, current
                ));
            }
            snapshot
                .criteria
                .insert(delta.criterion_id.clone(), delta.current);
            snapshot
                .evidence_refs
                .extend(delta.evidence_refs.iter().cloned());
        }
        for reference in &observation.evidence_delta.invalidated {
            snapshot
                .evidence_refs
                .retain(|current| current != reference);
            snapshot.invalidated_evidence_refs.push(reference.clone());
        }
        snapshot
            .evidence_refs
            .extend(observation.evidence_delta.added.iter().cloned());
        for effect in &observation.effect_deltas {
            snapshot
                .effects
                .insert(effect.effect_id.clone(), effect.terminal_class);
        }
        for conflict in &observation.conflict_deltas {
            apply_resolution_delta(
                &mut snapshot.open_conflicts,
                &conflict.conflict_id,
                conflict.change,
            );
        }
        for unknown in &observation.unknown_deltas {
            apply_resolution_delta(
                &mut snapshot.open_unknowns,
                &unknown.unknown_id,
                unknown.change,
            );
        }
        snapshot.cumulative_cost.model_steps = snapshot
            .cumulative_cost
            .model_steps
            .saturating_add(observation.cost_delta.model_steps);
        snapshot.cumulative_cost.tool_calls = snapshot
            .cumulative_cost
            .tool_calls
            .saturating_add(observation.cost_delta.tool_calls);
        snapshot.cumulative_cost.duration_ms = snapshot
            .cumulative_cost
            .duration_ms
            .saturating_add(observation.cost_delta.duration_ms);
        snapshot.cumulative_cost.input_tokens = snapshot
            .cumulative_cost
            .input_tokens
            .saturating_add(observation.cost_delta.input_tokens);
        snapshot.cumulative_cost.output_tokens = snapshot
            .cumulative_cost
            .output_tokens
            .saturating_add(observation.cost_delta.output_tokens);
        snapshot.cumulative_cost.cached_tokens = snapshot
            .cumulative_cost
            .cached_tokens
            .saturating_add(observation.cost_delta.cached_tokens);
        snapshot.observation_count = snapshot.observation_count.saturating_add(1);
        snapshot.last_observed_at_ms = snapshot
            .last_observed_at_ms
            .max(observation.freshness.observed_at_ms);
        snapshot.applied_observation_keys.push(key);
        normalize(&mut snapshot.evidence_refs);
        normalize(&mut snapshot.invalidated_evidence_refs);
        normalize(&mut snapshot.open_conflicts);
        normalize(&mut snapshot.open_unknowns);
        Ok(true)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreparedGoalMutation {
    pub stream_id: String,
    pub expected_stream_revision: u64,
    pub event: RuntimeTransactionEventInput,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PreparedUserGoalApplication {
    pub(crate) frame: crate::execution_core::goal::UserGoalInputFrame,
    pub(crate) goal: GoalContract,
    pub(crate) goal_stream_revision: u64,
    pub(crate) proposal: harness_contract::input_disposition::ModelInputDispositionBatch,
    pub(crate) mutation: Option<crate::execution_core::goal::PreparedGoalMutation>,
    pub(crate) ticket: crate::execution_core::NodeExecutionTicket,
    pub(crate) graph_revision: u64,
    pub(crate) program_cut: Option<(String, u64)>,
    // The original prepared outcome, never regenerated from a later Host view.
    pub(crate) continuation: crate::execution_core::ExecutionGraphReplan,
    pub(crate) finished_at_ms: u64,
    pub(crate) transcript_prefix: (u64, String),
    pub(crate) root_control_team_count: Option<u8>,
    pub(crate) usage: harness_contract::execution_graph::ExecutionUsage,
    pub(crate) provider_message: serde_json::Value,
    pub(crate) provider_usage: model_protocol::usage::TokenUsage,
}

pub(crate) fn user_application_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) async fn read_user_goal_application(
    services: &crate::RuntimeServices,
    receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
) -> Result<PreparedUserGoalApplication, String> {
    use sha2::Digest;
    let identity = receipt
        .prepared_application
        .as_ref()
        .ok_or("user application prepared reference missing")?;
    let artifact = services
        .artifact_store()
        .resolve(&identity.artifact_ref)
        .map_err(|e| e.to_string())?;
    if artifact.bytes > 2 * 1024 * 1024
        || artifact.sha256 != format!("sha256:{}", identity.artifact_sha256)
    {
        return Err("user application artifact identity/budget mismatch".into());
    }
    let body = services
        .artifact_store()
        .read(&artifact, &format!("session:{}", identity.session_id), None)
        .await
        .map_err(|e| e.to_string())?;
    if format!("{:x}", sha2::Sha256::digest(&body)) != identity.artifact_sha256 {
        return Err("user application artifact digest mismatch".into());
    }
    let prepared: PreparedUserGoalApplication =
        serde_json::from_slice(&body).map_err(|e| e.to_string())?;
    let binding = prepared
        .goal
        .execution_binding
        .as_ref()
        .ok_or("user application Goal binding missing")?;
    prepared.frame.validate(binding)?;
    if binding.session_id != identity.session_id
        || binding.turn_id != identity.turn_id
        || binding.root_execution_id != prepared.ticket.graph_id
        || prepared
            .frame
            .inputs
            .iter()
            .any(|(r, _, _)| r.session_generation != identity.session_generation)
        || prepared.mutation.as_ref().is_some_and(|m| {
            m.stream_id != stream_id(&prepared.goal.id)
                || m.expected_stream_revision != prepared.goal_stream_revision
                || m.event.event.payload["user_input_application"]["source_digest"]
                    != identity.source_digest
        })
        || prepared.goal.id != identity.goal_id
        || prepared.goal.revision != identity.goal_revision
        || prepared.goal.spec_digest != identity.goal_spec_digest
        || prepared.ticket.graph_id != identity.graph_id
        || prepared.ticket.node_id != identity.node_id
        || prepared.ticket.attempt.to_string() != identity.attempt_id
        || prepared.graph_revision != identity.graph_revision
        || prepared.program_cut != identity.program_cut
        || prepared.frame.digest()? != identity.source_digest
        || prepared
            .frame
            .inputs
            .iter()
            .map(|(r, _, _)| r.input_id.clone())
            .collect::<Vec<_>>()
            != receipt.input_ids
    {
        return Err("user application prepared content crosses source/attempt".into());
    }
    Ok(prepared)
}

pub(crate) fn user_goal_application_committed(
    services: &crate::RuntimeServices,
    prepared: &PreparedUserGoalApplication,
    artifact_ref: &str,
) -> Result<bool, String> {
    Ok(user_goal_application_committed_sources(services, prepared, artifact_ref)?.is_some())
}

pub(crate) fn user_goal_application_committed_sources(
    services: &crate::RuntimeServices,
    prepared: &PreparedUserGoalApplication,
    artifact_ref: &str,
) -> Result<Option<BTreeMap<String, u64>>, String> {
    if let Some(mutation) = &prepared.mutation {
        let committed = services
            .event_store()
            .event_by_idempotency_key(
                &mutation.stream_id,
                mutation
                    .event
                    .idempotency_key
                    .as_deref()
                    .ok_or("user application key missing")?,
            )
            .map_err(|e| e.to_string())?;
        if let Some(event) = committed {
            if event.payload != mutation.event.event.payload {
                return Err("user application Goal replay conflicts with frozen proposal".into());
            }
            let goal_cut = services
                .event_store()
                .transaction_stream_revision_exact(&event.transaction_id, &mutation.stream_id)
                .map_err(|error| error.to_string())?
                .ok_or("user application original Goal transaction cut missing")?;
            if goal_cut.expected_revision != mutation.expected_stream_revision
                || goal_cut.committed_revision != event.sequence
            {
                return Err("user application original Goal transaction cut mismatch".into());
            }
            let mut sources =
                BTreeMap::from([(mutation.stream_id.clone(), goal_cut.committed_revision)]);
            if let Some((program, expected)) = &prepared.program_cut {
                let stream = format!("agentic-program:{program}");
                let cut = services
                    .event_store()
                    .transaction_stream_revision_exact(&event.transaction_id, &stream)
                    .map_err(|error| error.to_string())?
                    .ok_or("user application original Program transaction cut missing")?;
                if cut.expected_revision != *expected || cut.committed_revision <= *expected {
                    return Err("user application original Program transaction cut mismatch".into());
                }
                sources.insert(stream, cut.committed_revision);
            }
            return Ok(Some(sources));
        }
        return Ok(None);
    }
    let graph = services
        .graph_state_store()
        .load(&prepared.ticket.graph_id)
        .map_err(|e| e.to_string())?;
    let committed = graph
        .recovery_cursor
        .node_attempts
        .get(&prepared.ticket.node_id)
        == Some(&prepared.ticket.attempt)
        && graph
            .node_results
            .get(&prepared.ticket.node_id)
            .is_some_and(|r| {
                r.status == harness_contract::execution_graph::ExecutionNodeStatus::Completed
                    && r.failure.is_none()
                    && r.result_ref.as_deref()
                        == Some(format!("user_input_application:{artifact_ref}").as_str())
            });
    Ok(committed
        .then(|| BTreeMap::from([(stream_id(&prepared.goal.id), prepared.goal_stream_revision)])))
}

pub(crate) async fn finish_user_goal_application(
    services: &crate::RuntimeServices,
    prepared: &PreparedUserGoalApplication,
) -> Result<(), String> {
    use harness_contract::input_disposition::InputApplicationState;
    let query = services
        .session_query_port()
        .ok_or("user application Session query missing")?;
    let journal = services
        .session_journal_port()
        .ok_or("user application Session journal missing")?;
    let mut rows = Vec::new();
    for (input, _, _) in &prepared.frame.inputs {
        rows.push(
            query
                .runtime_input_by_input_id(&input.input_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or("user application input disappeared")?,
        );
    }
    let mut receipt = rows[0]
        .application_receipt
        .clone()
        .ok_or("user application reservation disappeared")?;
    let checked = read_user_goal_application(services, &receipt).await?;
    if checked.frame.digest()? != prepared.frame.digest()?
        || checked.proposal != prepared.proposal
        || rows
            .iter()
            .any(|r| r.application_receipt.as_ref() != Some(&receipt))
    {
        return Err("user application batch reservation changed".into());
    }
    if !user_goal_application_committed(
        services,
        prepared,
        &receipt
            .prepared_application
            .as_ref()
            .ok_or("user input artifact missing")?
            .artifact_ref,
    )? {
        return Err("user application has no exact original commit".into());
    }
    if receipt.state == InputApplicationState::Applied {
        return Ok(());
    }
    if receipt.state != InputApplicationState::Materializing {
        return Err("user application was not reserved".into());
    }
    receipt.state = InputApplicationState::Applied;
    receipt.revision += 1;
    receipt.updated_at_ms = user_application_clock();
    // Preserve an intervening durable cancellation request. It is not evidence
    // that the Goal commit was undone, or that execution has physically exited.
    journal
        .commit_input_application_receipt(
            &receipt.input_ids,
            &rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
            &receipt,
            receipt.updated_at_ms,
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Recover receipt facts in the original settled lane. This never asks a model,
/// rebases a proposal, commits Goal, or synthesizes a physical exit.
pub(crate) async fn reconcile_user_goal_applications(
    services: &crate::RuntimeServices,
    root_graph_id: &str,
) -> Result<(), String> {
    use harness_contract::input_disposition::{
        InputApplicationCancellationState, InputApplicationState,
    };
    let graph = services
        .graph_state_store()
        .load(root_graph_id)
        .map_err(|e| e.to_string())?;
    let Some(lineage) = &graph.lineage else {
        return Ok(());
    };
    let Some(query) = services.session_query_port() else {
        return Ok(());
    };
    let Some(admission) = query
        .input_admission(&lineage.session_id)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(());
    };
    let mut after = None;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let page = query
            .pending_session_input_applications(
                &lineage.session_id,
                admission.generation,
                after.clone(),
                32,
            )
            .await
            .map_err(|e| e.to_string())?;
        if page.is_empty() {
            break;
        }
        for row in &page {
            let Some(receipt) = row.application_receipt.as_ref() else {
                continue;
            };
            let Some(identity) = receipt.prepared_application.as_ref() else {
                continue;
            };
            if identity.graph_id != root_graph_id || !seen.insert(receipt.disposition_id.clone()) {
                continue;
            }
            let prepared = read_user_goal_application(services, receipt).await?;
            let applied =
                user_goal_application_committed(services, &prepared, &identity.artifact_ref)?;
            if applied {
                finish_user_goal_application(services, &prepared).await?;
            }
            if !receipt
                .cancellation
                .as_ref()
                .is_some_and(|c| c.state == InputApplicationCancellationState::Requested)
            {
                continue;
            }
            services
                .cancel_execution_tree(root_graph_id, "original user-input cancellation requested")
                .await
                .map_err(|e| e.to_string())?;
            // Traverse the original durable lineage, including children of a
            // terminal parent, and use the canonical closure reader for every
            // admitted binding. No status-only or timeout-based settlement.
            let mut pending = std::collections::VecDeque::from([root_graph_id.to_string()]);
            let mut visited = std::collections::BTreeSet::new();
            let mut closed_sources = Vec::new();
            while let Some(id) = pending.pop_front() {
                if !visited.insert(id.clone()) {
                    continue;
                }
                let current = services
                    .graph_state_store()
                    .load(&id)
                    .map_err(|e| e.to_string())?;
                for link in services
                    .graph_state_store()
                    .child_links(&id)
                    .map_err(|e| e.to_string())?
                {
                    pending.push_back(link.child_execution_id);
                }
                if !current.nodes.iter().all(|n| {
                    current
                        .node_statuses
                        .get(&n.id)
                        .is_some_and(|s| s.is_terminal())
                }) {
                    return Err("user application cancellation still has live Graph work".into());
                }
                let mut cursor = 0;
                while cursor < current.revision {
                    let (next, starts) = services
                        .graph_state_store()
                        .binding_starts_page(&id, cursor, current.revision)
                        .map_err(|e| e.to_string())?;
                    if next <= cursor {
                        return Err("user application binding closure read did not advance".into());
                    }
                    for (node, binding, start) in starts {
                        if !services
                            .graph_state_store()
                            .binding_lease_closure(&current, &node, &binding, start)
                            .map_err(|e| e.to_string())?
                        {
                            return Err(
                                "user application cancellation physical closure is pending".into(),
                            );
                        }
                        closed_sources.push(serde_json::json!({"graph":id,"head":current.revision,"node":node,"binding":binding,"start":start}));
                    }
                    cursor = next;
                }
            }
            // The original Graph CAS can no longer accept the frozen proposal.
            // Re-read Goal AFTER that fence: a commit that won before cancel
            // must be recorded as Applied, not mislabeled as not applied.
            let current = services
                .graph_state_store()
                .load(root_graph_id)
                .map_err(|e| e.to_string())?;
            if current.revision <= prepared.graph_revision {
                return Err("user application original Graph cut is not fenced".into());
            }
            let actual =
                user_goal_application_committed(services, &prepared, &identity.artifact_ref)?;
            if actual {
                finish_user_goal_application(services, &prepared).await?;
            }
            let mut rows = Vec::new();
            for id in &receipt.input_ids {
                rows.push(
                    query
                        .runtime_input_by_input_id(id)
                        .await
                        .map_err(|e| e.to_string())?
                        .ok_or("user application cancel input missing")?,
                );
            }
            let mut next = rows[0]
                .application_receipt
                .clone()
                .ok_or("user application cancel reservation missing")?;
            if rows
                .iter()
                .any(|r| r.application_receipt.as_ref() != Some(&next))
            {
                return Err("user application cancel batch conflict".into());
            }
            if next
                .cancellation
                .as_ref()
                .is_some_and(|c| c.state == InputApplicationCancellationState::Settled)
            {
                continue;
            }
            if !actual {
                next.state = InputApplicationState::Failed;
                next.error =
                    Some("user_goal_not_applied_original_graph_cut_physically_closed".into());
            }
            let request = next
                .cancellation
                .as_mut()
                .ok_or("user application cancel request disappeared")?;
            request.state = InputApplicationCancellationState::Settled;
            request.settlement_ref = Some(format!(
                "graph-binding-closure:{}:{:x}",
                root_graph_id,
                sha2::Sha256::digest(
                    serde_json::to_vec(&closed_sources).map_err(|e| e.to_string())?
                )
            ));
            next.revision += 1;
            next.updated_at_ms = user_application_clock();
            services
                .session_journal_port()
                .ok_or("user application cancellation journal missing")?
                .commit_input_application_receipt(
                    &next.input_ids,
                    &rows.iter().map(|r| r.revision).collect::<Vec<_>>(),
                    &next,
                    next.updated_at_ms,
                )
                .await
                .map_err(|e| e.to_string())?;
        }
        let last = page.last().unwrap();
        let next = (last.sequence, last.input_id.clone());
        if after.as_ref().is_some_and(|old| old >= &next) {
            return Err("user application recovery cursor did not advance".into());
        }
        after = Some(next);
        if page.len() < 32 {
            break;
        }
    }
    Ok(())
}

/// Frozen originals read through the Session query/history owner, never model
/// JSON. Receipt revisions may advance while these immutable source IDs stay fixed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UserGoalInputFrame {
    pub(crate) inputs: Vec<(
        crate::session_runtime_port::RuntimeSessionInputRecord,
        harness_contract::turn::SessionInputUserSource,
        String,
    )>,
}

impl UserGoalInputFrame {
    pub(crate) fn validate(
        &self,
        binding: &harness_contract::goal::GoalExecutionBinding,
    ) -> Result<(), String> {
        use harness_contract::turn::{SessionInputUserSource, SESSION_INPUT_USER_SOURCE_KEY};
        if self.inputs.is_empty() || self.inputs.len() > 32 {
            return Err("user Goal source count outside 1..=32".into());
        }
        let principal = &self.inputs[0].1.principal_id;
        let generation = self.inputs[0].0.session_generation;
        let mut ids = std::collections::BTreeSet::new();
        for (record, source, content) in &self.inputs {
            let classification: serde_json::Value = serde_json::from_str(
                record
                    .classification_json
                    .as_deref()
                    .ok_or("user Goal source missing classification")?,
            )
            .map_err(|e| e.to_string())?;
            let persisted: SessionInputUserSource = serde_json::from_value(
                classification["metadata"][SESSION_INPUT_USER_SOURCE_KEY].clone(),
            )
            .map_err(|_| "user Goal source missing authenticated origin")?;
            if &persisted != source
                || source.principal_id.is_empty()
                || &source.principal_id != principal
                || source.session_id != binding.session_id
                || record.session_id != binding.session_id
                || source.input_id != record.input_id
                || source.request_id != record.request_id
                || source.message_id != record.message_id
                || source.session_generation != record.session_generation
                || source.session_generation != generation
                || source.content_sha256
                    != format!("{:x}", sha2::Sha256::digest(content.as_bytes()))
                || record.target_turn_id.as_deref().unwrap_or(&record.turn_id) != binding.turn_id
                || !ids.insert(&record.input_id)
                || matches!(
                    record.status,
                    crate::session_runtime_port::RuntimeSessionInputStatus::Cancelled
                        | crate::session_runtime_port::RuntimeSessionInputStatus::Expired
                        | crate::session_runtime_port::RuntimeSessionInputStatus::RejectedPolicy
                        | crate::session_runtime_port::RuntimeSessionInputStatus::RejectedDuplicate
                        | crate::session_runtime_port::RuntimeSessionInputStatus::Failed
                )
            {
                return Err("user Goal source identity mismatch".into());
            }
        }
        Ok(())
    }

    pub(crate) fn digest(&self) -> Result<String, String> {
        // Do not include mutable row/receipt revision in the adoption identity.
        let immutable = self.inputs.iter().map(|(record, source, _)|
            serde_json::json!({"source":source,"sequence":record.sequence})).collect::<Vec<_>>();
        Ok(format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(&immutable).map_err(|e| e.to_string())?)
        ))
    }
}

impl GoalStore {
    /// Prepare only. The original Graph or Program transaction commits this
    /// event with the frozen read set; this method cannot independently append.
    pub(crate) fn prepare_user_input_revision(
        &self,
        original: &GoalProjection,
        frame: &UserGoalInputFrame,
        proposal: &harness_contract::input_disposition::ModelInputDispositionBatch,
        ticket: &crate::execution_core::NodeExecutionTicket,
    ) -> Result<PreparedGoalMutation, String> {
        use harness_contract::input_disposition::InputDispositionAction;
        let binding = original
            .goal
            .execution_binding
            .as_ref()
            .ok_or("user Goal binding missing")?;
        frame.validate(binding)?;
        if binding.root_execution_id != ticket.graph_id || ticket.attempt == 0 {
            return Err("user Goal proposal crosses Graph attempt".into());
        }
        // Reuse the same strict finite validator for persisted proposals.
        let checked = harness_contract::input_disposition::ModelInputDispositionBatch::decode_business_proposal(
            &serde_json::to_string(proposal).map_err(|e|e.to_string())?, frame.inputs.len())?;
        let decision = &checked.decisions[0];
        if decision.action != InputDispositionAction::AmendCurrentTurn {
            return Err("user Goal amendment expected".into());
        }
        let business = decision
            .business_revision
            .as_ref()
            .ok_or("user Goal business revision missing")?;
        let anchor = original
            .goal
            .user_intent_criterion_id
            .as_deref()
            .ok_or("user Goal intent anchor missing")?;
        if business
            .retired_criteria
            .iter()
            .any(|id| id == anchor || !original.goal.criteria.iter().any(|c| &c.id == id))
            || business.retired_obligations.iter().any(|id| {
                !original
                    .goal
                    .obligations
                    .iter()
                    .any(|o| &o.obligation_id == id)
            })
        {
            return Err(
                "user Goal revision references foreign or immutable criterion/obligation".into(),
            );
        }
        let content_bytes = frame
            .inputs
            .iter()
            .map(|(_, _, text)| text.len())
            .sum::<usize>();
        for commitment in &business.commitments {
            use harness_contract::goal::GoalInputCommitment::*;
            let span = match commitment {
                WorkspaceWrite { source, .. }
                | ForbidWorkspaceWrite { source, .. }
                | ForbidTools { source }
                | TeamCount { source, .. } => source,
            };
            if span.start > span.end || span.end > content_bytes {
                return Err("user Goal proposal citation outside supplied source".into());
            }
            match commitment {
                WorkspaceWrite {
                    path: Some(path), ..
                }
                | ForbidWorkspaceWrite {
                    path: Some(path), ..
                } if path.trim().is_empty() => {
                    return Err(
                        "user Goal business path must be nonempty or autonomous null".into(),
                    )
                }
                TeamCount {
                    minimum,
                    maximum: Some(maximum),
                    ..
                } if maximum < minimum => return Err("user Goal team maximum below minimum".into()),
                _ => {}
            }
        }
        let source_ref = format!("session_input_application:{}", frame.digest()?);
        let principal = frame.inputs[0].1.principal_id.clone();
        let first = &frame.inputs[0].0;
        let contract = harness_contract::goal::GoalInputContract {
            version: 2,
            principal_id: principal,
            session_id: binding.session_id.clone(),
            turn_id: binding.turn_id.clone(),
            source_ref: source_ref.clone(),
            input_revision: first.sequence.saturating_add(1) as u64,
            content_sha256: frame.digest()?,
            commitments: business.commitments.clone(),
            proposed_workspace_scopes: vec![],
        };
        let (_, _, mut event) = self.revision_event(
            &original.goal.id,
            original.goal.revision,
            original.goal.user_sequence.saturating_add(1),
            "authenticated_user_input_revision",
            |goal| {
                goal.objective = decision.objective.clone();
                goal.criteria
                    .retain(|c| !business.retired_criteria.contains(&c.id));
                for criterion in &mut goal.criteria {
                    if criterion.id == anchor {
                        criterion.statement = decision.objective.clone();
                        criterion.statement_ref = None;
                        criterion.source_refs.push(source_ref.clone());
                    }
                    criterion.status = AcceptanceStatus::Open;
                }
                goal.obligations
                    .retain(|o| !business.retired_obligations.contains(&o.obligation_id));
                goal.constraints = business.constraints.clone();
                goal.input_contract = Some(contract.clone());
                goal.participation_requirement = contract
                    .team_bounds()
                    .filter(|(min, _)| *min > 0)
                    .map(|(minimum_team_count, _)| {
                        harness_contract::goal::ParticipationRequirement {
                            minimum_team_count,
                            source_ref: source_ref.clone(),
                        }
                    });
                goal.spec_revision = goal.spec_revision.saturating_add(1);
                goal.spec_digest = goal_spec_digest(goal);
                goal.waiting = None;
                goal.recovery = None;
                goal.terminal = None;
                goal.completion = GoalCompletion::Open;
                vec![
                    "objective".into(),
                    "criteria".into(),
                    "obligations".into(),
                    "constraints".into(),
                    "input_contract".into(),
                    "spec_digest".into(),
                ]
            },
        )?;
        event.event.payload["goal_source"] = serde_json::json!({
            "stream_id":stream_id(&original.goal.id),"sequence":original.stream_revision,
            "goal_revision":original.goal.revision,"spec_revision":original.goal.spec_revision,
            "spec_digest":original.goal.spec_digest,"source_intent_ref":original.goal.source_intent_ref,
        });
        event.event.payload["input_application_ticket"] = serde_json::json!(ticket);
        event.idempotency_key = Some(format!(
            "goal-user-input:{}:{}",
            original.goal.id,
            frame.digest()?
        ));
        event.event.payload["user_input_application"] = serde_json::json!({"source_digest":frame.digest()?,"sources":frame.inputs.iter().map(|(_,source,_)|source).collect::<Vec<_>>(),"proposal":proposal});
        Ok(PreparedGoalMutation {
            stream_id: stream_id(&original.goal.id),
            expected_stream_revision: original.stream_revision,
            event,
        })
    }
}

/// Whether a recorded ObjectiveReview has retired the `completion_gap` that
/// sent the model to review in the first place.
///
/// The Goal's criteria and obligations must already reflect this review (the
/// caller applies the decision first), so this reads the post-reduction state.
/// A `gap` or `blocked` decision never retires the descriptor: the gap it names
/// is real, and clearing it would let an unfinished objective report itself as
/// ready to complete.
fn satisfied_review_retires_the_waiting_gap(
    goal: &harness_contract::goal::GoalContract,
    decision: &str,
) -> bool {
    decision == "satisfied"
        && goal.blockers.is_empty()
        && goal
            .criteria
            .iter()
            .all(|criterion| criterion.status != harness_contract::goal::AcceptanceStatus::Open)
        && goal.obligations.iter().all(|obligation| {
            !obligation.required
                || obligation.state != harness_contract::goal::ObjectiveObligationState::Open
        })
}

impl GoalStore {
    /// Prepare the Goal-side half of an Agentic Objective action. The caller
    /// must commit the returned event in the same transaction as the Program
    /// action through AgentActionService::apply_with_goal_event.
    /// Resolve an over-qualified criterion/obligation ref (`criterion:goal:<graph>:user_intent`)
    /// to the plain id the specification knows. A model routinely emits the path
    /// form; the specification only stores the id. Resolves only when the tail
    /// matches a live criterion or obligation, so it can never invent a target.
    fn normalize_criterion_ref(
        action: &harness_contract::agent_action::AgentAction,
        current: &harness_contract::goal::GoalContract,
    ) -> harness_contract::agent_action::AgentAction {
        use harness_contract::agent_action::AgentAction;
        let known = |reference: &str| {
            current
                .criteria
                .iter()
                .any(|criterion| criterion.id == reference)
                || current
                    .obligations
                    .iter()
                    .any(|obligation| obligation.obligation_id == reference)
        };
        let resolve = |reference: &str| {
            if known(reference) {
                return reference.to_string();
            }
            match reference.rsplit_once(':') {
                Some((_, tail)) if known(tail) => tail.to_string(),
                _ => reference.to_string(),
            }
        };
        match action {
            AgentAction::ObjectiveReview(input) => {
                let mut input = input.clone();
                input.criterion_ref = resolve(&input.criterion_ref);
                AgentAction::ObjectiveReview(input)
            }
            AgentAction::ObjectiveUpdate(input) => {
                let mut input = input.clone();
                if let Some(reference) = input.criterion_ref.as_deref() {
                    input.criterion_ref = Some(resolve(reference));
                }
                AgentAction::ObjectiveUpdate(input)
            }
            other => other.clone(),
        }
    }

    pub(crate) fn prepare_agentic_objective_action(
        &self,
        envelope: &harness_contract::agent_action::AgentActionEnvelope,
        producer_refs: &[String],
        verification: Option<&harness_contract::goal::ObjectiveReviewVerification>,
    ) -> Result<PreparedGoalMutation, String> {
        use harness_contract::agent_action::{
            AgentAction, ObjectiveReviewDecision, ObjectiveUpdateOperation,
        };

        let root_execution_id = envelope
            .actor
            .root_execution_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "Objective action requires a root execution binding".to_string())?;
        let goal_id = format!("goal:{root_execution_id}");
        let current = self
            .get(&goal_id)?
            .ok_or_else(|| format!("Objective action target {goal_id} not found"))?;
        self.event_store
            .ensure_session_recovery_admitted(&current.session_id)?;
        // Resolve over-qualified criterion refs before any validation or event
        // building so both see the plain specification id.
        let action = Self::normalize_criterion_ref(&envelope.action, &current);
        let binding = current.execution_binding.as_ref().ok_or_else(|| {
            "Objective action target has no immutable GoalExecutionBinding".to_string()
        })?;
        if binding.objective_id != envelope.actor.objective_id
            || binding.session_id != envelope.actor.session_id
            || binding.turn_id != envelope.actor.turn_id
            || binding.root_execution_id != root_execution_id
            || binding.agentic_program_id != envelope.actor.program_id
        {
            return Err("Objective action binding does not match the durable Goal".to_string());
        }
        if let harness_contract::agent_action::AgentAction::ObjectiveUpdate(input) = &action {
            if matches!(
                input.operation,
                harness_contract::agent_action::ObjectiveUpdateOperation::Replace
                    | harness_contract::agent_action::ObjectiveUpdateOperation::Retire
            ) {
                let criterion_ref = input
                    .criterion_ref
                    .as_deref()
                    .ok_or_else(|| "Objective update requires criterion_ref".to_string())?;
                if current.user_intent_criterion_id.as_deref() == Some(criterion_ref) {
                    return Err(
                        "the original user_intent criterion cannot be replaced or retired"
                            .to_string(),
                    );
                }
                if !current
                    .criteria
                    .iter()
                    .any(|criterion| criterion.id == criterion_ref)
                {
                    // The ObjectiveReview branch below already lists the valid ids
                    // for the same condition. Two spellings of one rule in one
                    // function is how a model ends up guessing: run one19 guessed
                    // its way through a refusal that named the rule and nothing else.
                    let valid = current
                        .criteria
                        .iter()
                        .map(|criterion| criterion.id.as_str())
                        .chain(
                            current
                                .obligations
                                .iter()
                                .map(|obligation| obligation.obligation_id.as_str()),
                        )
                        .collect::<Vec<_>>();
                    return Err(format!(
                        "Objective update criterion {criterion_ref} does not exist in the current \
                         specification; valid ids: [{}]",
                        valid.join(", ")
                    ));
                }
            }
        }
        if let AgentAction::ObjectiveReview(input) = &action {
            if !current
                .criteria
                .iter()
                .any(|criterion| criterion.id == input.criterion_ref)
                && !current
                    .obligations
                    .iter()
                    .any(|obligation| obligation.obligation_id == input.criterion_ref)
            {
                let valid = current
                    .criteria
                    .iter()
                    .map(|criterion| criterion.id.as_str())
                    .chain(
                        current
                            .obligations
                            .iter()
                            .map(|obligation| obligation.obligation_id.as_str()),
                    )
                    .collect::<Vec<_>>();
                return Err(format!(
                    "Objective review criterion or obligation does not exist in the current specification; valid ids: [{}]",
                    valid.join(", ")
                ));
            }
            if input.decision == ObjectiveReviewDecision::Satisfied {
                let proof = verification
                    .ok_or("satisfied review requires Runtime physical-read verification")?;
                if proof.goal_spec_revision != Some(current.spec_revision)
                    || proof.goal_spec_digest.as_deref() != Some(current.spec_digest.as_str())
                {
                    // The review was prepared against one revision and the Goal has
                    // moved. Saying only "changed" leaves the model to re-read
                    // everything to find out what it is now.
                    return Err(format!(
                        "review source Goal specification changed: this review was prepared against \
                         revision {:?} digest {:?}, and the Goal is now at revision {} digest {}. \
                         Re-read the current Goal through state_inspect and submit the review against \
                         that revision.",
                        proof.goal_spec_revision,
                        proof.goal_spec_digest,
                        current.spec_revision,
                        current.spec_digest
                    ));
                }
            }
        }
        let expected_goal_stream_revision = self
            .event_store
            .stream_revision(&stream_id(&goal_id))
            .map_err(|error| error.to_string())?;
        let expected_goal_revision = current.revision;
        let next_sequence = current.user_sequence.saturating_add(1);
        let producer_refs = sorted_unique(producer_refs.to_vec());
        let (updated, _, event) = match &action {
            AgentAction::ObjectiveUpdate(input) => self.revision_event(
                &goal_id,
                expected_goal_revision,
                next_sequence,
                format!("agentic_objective_update:{}", envelope.action_id),
                |goal| {
                    let changed = match input.operation {
                        ObjectiveUpdateOperation::Add => {
                            let statement_ref = input.statement_ref.as_ref().expect(
                                "AgentAction validation requires statement_ref for objective add",
                            );
                            let criterion_id = format!(
                                "criterion:{:x}",
                                sha2::Sha256::digest(
                                    format!("{}|{}|{}", goal.id, envelope.action_id, statement_ref)
                                        .as_bytes()
                                )
                            );
                            goal.criteria.push(harness_contract::goal::AcceptanceCriterion {
                                id: criterion_id,
                                statement: "Model-proposed criterion (see statement_ref)".to_string(),
                                statement_ref: Some(statement_ref.clone()),
                                source_refs: input.source_refs.clone(),
                                required_evidence: input.evidence_requirements.clone(),
                                status: harness_contract::goal::AcceptanceStatus::Open,
                                waiver: None,
                            });
                            vec!["criteria".to_string()]
                        }
                        ObjectiveUpdateOperation::Replace => {
                            let criterion_ref = input.criterion_ref.as_ref().expect(
                                "AgentAction validation requires criterion_ref for objective replace",
                            );
                            let Some(criterion) =
                                goal.criteria.iter_mut().find(|item| item.id == *criterion_ref)
                            else {
                                return Vec::new();
                            };
                            if let Some(statement_ref) = input.statement_ref.as_ref() {
                                criterion.statement =
                                    "Model-proposed criterion (see statement_ref)".to_string();
                                criterion.statement_ref = Some(statement_ref.clone());
                            }
                            if !input.source_refs.is_empty() {
                                criterion.source_refs.clone_from(&input.source_refs);
                            }
                            if !input.evidence_requirements.is_empty() {
                                criterion
                                    .required_evidence
                                    .clone_from(&input.evidence_requirements);
                            }
                            criterion.status = harness_contract::goal::AcceptanceStatus::Open;
                            vec!["criteria".to_string()]
                        }
                        ObjectiveUpdateOperation::Retire => {
                            let criterion_ref = input.criterion_ref.as_ref().expect(
                                "AgentAction validation requires criterion_ref for objective retire",
                            );
                            let before = goal.criteria.len();
                            goal.criteria.retain(|criterion| criterion.id != *criterion_ref);
                            debug_assert!(before != goal.criteria.len());
                            vec!["criteria".to_string()]
                        }
                    };
                    goal.spec_revision = goal.spec_revision.saturating_add(1);
                    goal.spec_digest = goal_spec_digest(goal);
                    goal.waiting = None;
                    let mut fields = changed;
                    fields.extend([
                        "spec_revision".to_string(),
                        "spec_digest".to_string(),
                        "waiting".to_string(),
                    ]);
                    fields
                },
            )?,
            AgentAction::ObjectiveReview(input) => self.revision_event(
                &goal_id,
                expected_goal_revision,
                next_sequence,
                format!("agentic_objective_review:{}", envelope.action_id),
                |goal| {
                    let decision = match input.decision {
                        ObjectiveReviewDecision::Satisfied => "satisfied",
                        ObjectiveReviewDecision::Gap => "gap",
                        ObjectiveReviewDecision::Blocked => "blocked",
                    };
                    let review_id = objective_review_record_id(&goal.id, &envelope.action_id, &input.criterion_ref);
                    if let Some(criterion) = goal.criteria.iter_mut().find(|item| item.id == input.criterion_ref) {
                        criterion.status = match input.decision {
                            ObjectiveReviewDecision::Satisfied => AcceptanceStatus::Satisfied,
                            ObjectiveReviewDecision::Gap | ObjectiveReviewDecision::Blocked => AcceptanceStatus::Open,
                        };
                    } else if let Some(obligation) = goal.obligations.iter_mut().find(|item| item.obligation_id == input.criterion_ref) {
                        obligation.state = match input.decision {
                            ObjectiveReviewDecision::Satisfied => ObjectiveObligationState::Satisfied,
                            ObjectiveReviewDecision::Gap => ObjectiveObligationState::Open,
                            ObjectiveReviewDecision::Blocked => ObjectiveObligationState::Blocked,
                        };
                        obligation.artifact_refs.clone_from(&input.result_refs);
                        obligation.evidence_refs.clone_from(&input.evidence_refs);
                        obligation.reread_receipts = verification.map(|proof| proof.reads.iter()
                            .flat_map(|read| read.receipt_refs.iter().cloned()).collect()).unwrap_or_default();
                        obligation.verifier_decision = (input.decision == ObjectiveReviewDecision::Satisfied).then(|| review_id.clone());
                        obligation.diagnostic_code = (input.decision != ObjectiveReviewDecision::Satisfied).then(|| format!("objective_review_{decision}"));
                    }
                    goal.review_refs.push(review_id.clone());
                    goal.review_refs.sort();
                    goal.review_refs.dedup();
                    goal.reviews.push(harness_contract::goal::ObjectiveReviewRecord {
                        criterion_ref: input.criterion_ref.clone(),
                        spec_revision: goal.spec_revision,
                        input_manifest_digest: goal.spec_digest.clone(),
                        result_refs: input.result_refs.clone(),
                        evidence_refs: input.evidence_refs.clone(),
                        reviewer_actor: envelope.actor.actor_id.clone(),
                        reviewer_execution_id: envelope.actor.execution_id.clone(),
                        producer_refs: producer_refs.clone(),
                        decision: decision.to_string(),
                        // A review the model did not attach a report to still owes
                        // a durable reason_ref. Runtime points it at this review's
                        // own Goal record, which is durable by construction, so
                        // downstream readers keep a resolvable reference without
                        // the model having to write and commit a report first.
                        reason_ref: if input.reason_ref.trim().is_empty() {
                            format!("objective_review:{review_id}")
                        } else {
                            input.reason_ref.clone()
                        },
                        review_id,
                        verification: verification.cloned(),
                    });
                    // A satisfied review can retire the `completion_gap` that
                    // sent the model here. `ObjectiveUpdate` already clears
                    // `waiting`; leaving it in place after a passing review
                    // makes the Goal describe a gap that no longer exists, and
                    // the Program projection repeats that description while
                    // withholding the exit action.
                    let clears_waiting =
                        satisfied_review_retires_the_waiting_gap(goal, decision);
                    if clears_waiting {
                        goal.waiting = None;
                    }
                    let mut fields = vec![
                        "criteria".to_string(),
                        "obligations".to_string(),
                        "review_refs".to_string(),
                        "reviews".to_string(),
                    ];
                    if clears_waiting {
                        fields.push("waiting".to_string());
                    }
                    fields
                },
            )?,
            _ => return Err("action is not an Objective mutation".to_string()),
        };
        if updated
            .reviews
            .iter()
            .any(|review| review.reviewer_actor.trim().is_empty())
        {
            return Err("Objective review is missing reviewer identity".to_string());
        }
        if let harness_contract::agent_action::AgentAction::ObjectiveReview(input) = &action {
            let review = updated
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == input.criterion_ref)
                .ok_or_else(|| "Objective review was not durably recorded".to_string())?;
            if input.decision == ObjectiveReviewDecision::Satisfied
                && review.producer_refs.is_empty()
            {
                return Err(
                    "Objective review cannot establish independent review without a Runtime-resolved result producer"
                        .to_string(),
                );
            }
            if input.decision == ObjectiveReviewDecision::Satisfied
                && review
                    .verification
                    .as_ref()
                    .is_none_or(|proof| proof.independence_required)
                && review
                    .producer_refs
                    .iter()
                    .any(|producer| producer == &review.reviewer_actor)
            {
                return Err(
                    "Objective reviewer is also a producer of the reviewed result".to_string(),
                );
            }
        }
        let action_entity_ref = match &action {
            AgentAction::ObjectiveUpdate(input) => match input.operation {
                ObjectiveUpdateOperation::Add => updated
                    .criteria
                    .last()
                    .map(|criterion| criterion.id.clone()),
                _ => input.criterion_ref.clone(),
            },
            AgentAction::ObjectiveReview(_) => updated
                .reviews
                .last()
                .map(|review| review.review_id.clone()),
            _ => None,
        };
        let mut event = event;
        event.event.payload["action_entity_ref"] = serde_json::json!(action_entity_ref);
        Ok(PreparedGoalMutation {
            stream_id: stream_id(&goal_id),
            expected_stream_revision: expected_goal_stream_revision,
            event,
        })
    }
}

pub(crate) fn goal_spec_digest(goal: &GoalContract) -> String {
    let mut canonical = serde_json::json!({
        "source_intent_ref": goal.source_intent_ref,
        "criteria": goal.criteria.iter().map(|criterion| serde_json::json!({
            "id":criterion.id,"statement":criterion.statement,"statement_ref":criterion.statement_ref,
            "source_refs":criterion.source_refs,"required_evidence":criterion.required_evidence,
            "waiver":criterion.waiver,
        })).collect::<Vec<_>>(),
        "constraints": goal.constraints,
        "participation_requirement": goal.participation_requirement,
        "obligations": goal.obligations.iter().map(|obligation| serde_json::json!({
            "id":obligation.obligation_id,"required":obligation.required,
            "success_predicate":obligation.success_predicate,"producer":obligation.producer,
            "evidence_requirement":obligation.evidence_requirement,
        })).collect::<Vec<_>>(),
    });
    // Historical absent-field digests retain their exact original encoding.
    if let Some(contract) = &goal.input_contract {
        canonical["input_contract"] = serde_json::json!(contract);
    }
    format!(
        "{:x}",
        sha2::Sha256::digest(canonical.to_string().as_bytes())
    )
}

impl GoalStore {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self {
            event_store,
            proof_artifacts: None,
            process_input_verifier: None,
            proof_session_queries: None,
        }
    }

    #[must_use]
    pub(crate) fn with_process_input_verifier(
        mut self,
        verifier: crate::agentic::review_evidence::ProcessInputVerifier,
    ) -> Self {
        self.process_input_verifier = Some(verifier);
        self
    }

    pub(crate) fn with_artifact_store(mut self, artifacts: Arc<crate::ArtifactStore>) -> Self {
        self.proof_artifacts = Some(artifacts);
        self
    }

    #[must_use]
    pub(crate) fn with_session_query_slot(
        mut self,
        queries: Arc<OnceLock<Arc<dyn crate::SessionRuntimeQueryPort>>>,
    ) -> Self {
        self.proof_session_queries = Some(queries);
        self
    }

    pub fn create(&self, goal: GoalContract) -> Result<GoalContract, String> {
        validate_goal(&goal)?;
        let stream_id = stream_id(&goal.id);
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        if revision > 0 {
            return self
                .get(&goal.id)?
                .ok_or_else(|| format!("goal stream {stream_id} has no replayable goal snapshot"));
        }
        self.append_goal_event(
            &stream_id,
            revision,
            format!("goal-create:{}", goal.id),
            "goal.created",
            "open",
            &goal,
            "runtime.goal_store",
            Vec::new(),
        )?;
        Ok(goal)
    }

    pub fn get(&self, goal_id: &str) -> Result<Option<GoalContract>, String> {
        self.projection(goal_id)
            .map(|projection| projection.map(|projection| projection.goal))
    }

    pub fn projection(&self, goal_id: &str) -> Result<Option<GoalProjection>, String> {
        let stream_id = stream_id(goal_id);
        let mut goal = None;
        let mut observations = Vec::new();
        let mut interventions = Vec::new();
        let mut intervention_traces = Vec::new();
        let mut progress = None;
        let mut stream_revision = 0_u64;
        let head = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let checkpoint_id = format!("goal-projection:{goal_id}");
        let checkpoint = self
            .event_store
            .projection_checkpoint(&checkpoint_id)
            .map_err(|error| error.to_string())?;
        let mut checkpoint_valid = false;
        if let Some(saved) = checkpoint
            .as_ref()
            .filter(|saved| saved.source_cursor <= head)
        {
            let restored = saved.payload["scope"]
                .as_str()
                .ok_or_else(|| "Goal checkpoint scope missing".to_string())
                .and_then(|scope| {
                    self.event_store.resolve_content(
                        scope,
                        &saved.payload["body"],
                        &crate::PreparedArtifactOrigin::CheckpointBody {
                            projection_id: checkpoint_id.clone(),
                            source_cursor: saved.source_cursor,
                        },
                    )
                })
                .and_then(|value| {
                    serde_json::from_value::<GoalProjection>(value)
                        .map_err(|_| "invalid Goal checkpoint".to_string())
                });
            if let Ok(restored) = restored {
                if restored.goal.id == goal_id
                    && restored.stream_revision == saved.source_cursor
                    && saved.payload["schema"] == 2
                    && saved.payload["scope"] == format!("session:{}", restored.goal.session_id)
                {
                    checkpoint_valid = true;
                    stream_revision = restored.stream_revision;
                    goal = Some(restored.goal);
                    observations = restored.observations;
                    interventions = restored.interventions;
                    intervention_traces = restored.intervention_traces;
                    progress = Some(restored.progress);
                }
            }
        }
        while stream_revision < head {
            let page = self.event_store.list_stream_after(
                &stream_id,
                stream_revision,
                head,
                100,
                1024 * 1024,
            )?;
            if page.is_empty() {
                return Err("Goal tail has a sequence gap".into());
            }
            for event in page {
                if event.stream_id != stream_id
                    || event.sequence
                        != stream_revision
                            .checked_add(1)
                            .ok_or("Goal sequence overflow")?
                {
                    return Err("Goal tail is not continuous".into());
                }
                stream_revision = event.sequence;
                if (event.kind == "goal.observation" && event.payload.get("goal").is_some())
                    || (event.kind != "goal.observation" && event.schema_version != 1)
                {
                    return Err("unsupported Goal event schema".into());
                }
                if event.scope != RuntimeEventScope::Goal {
                    return Err(format!("goal stream {stream_id} contains a non-goal event"));
                }
                if let Some(value) = event.payload.get("goal") {
                    let event_goal = serde_json::from_value::<GoalContract>(value.clone())
                        .map_err(|error| error.to_string())?;
                    if event_goal.id != goal_id {
                        return Err("Goal snapshot identity mismatch".into());
                    }
                    if progress.is_none() {
                        progress = Some(GoalProgressReducer::from_goal(&event_goal));
                    } else if matches!(
                        event.kind.as_str(),
                        "goal.created"
                            | "goal.revised"
                            | "goal.completed"
                            | "goal.completion_waiting"
                    ) {
                        let previous_goal = goal
                            .as_ref()
                            .ok_or_else(|| "goal revision precedes goal creation".to_string())?;
                        let goal_progress = progress
                            .as_mut()
                            .ok_or_else(|| "goal progress was not initialized".to_string())?;
                        GoalProgressReducer::reconcile_goal(
                            goal_progress,
                            previous_goal,
                            &event_goal,
                        );
                    }
                    goal = Some(event_goal);
                }
                if event.kind == "goal.observation" {
                    let value = self.event_store.resolve_observation_record(&event)?;
                    let source = &event.payload["goal_source"];
                    let current = goal.as_ref().ok_or("observation precedes Goal creation")?;
                    if source["stream_id"] != stream_id
                        || source["goal_revision"] != current.revision
                        || source["spec_digest"] != current.spec_digest
                        || source["sequence"]
                            .as_u64()
                            .is_none_or(|sequence| sequence >= event.sequence)
                    {
                        return Err("observation Goal source mismatch".into());
                    }
                    let observation = serde_json::from_value::<RuntimeObservation>(value)
                        .map_err(|error| error.to_string())?;
                    if observation.goal_id() != goal_id {
                        return Err("observation Goal identity mismatch".into());
                    }
                    GoalProgressReducer::apply(
                        progress
                            .as_mut()
                            .ok_or_else(|| "goal observation precedes goal creation".to_string())?,
                        &observation,
                    )?;
                    observations.push(observation);
                    if observations.len() > 256 {
                        observations.remove(0);
                    }
                }
                if let Some(value) = event.payload.get("intervention") {
                    interventions.push(
                        serde_json::from_value::<RuntimeIntervention>(value.clone())
                            .map_err(|error| error.to_string())?,
                    );
                    intervention_traces.push(
                        serde_json::from_value::<RuntimeInterventionTrace>(
                            event
                                .payload
                                .get("intervention_trace")
                                .cloned()
                                .ok_or_else(|| {
                                    "goal intervention event has no typed trigger trace".to_string()
                                })?,
                        )
                        .map_err(|error| error.to_string())?,
                    );
                }
            }
        }
        let projection = goal.map(|goal| GoalProjection {
            progress: progress.unwrap_or_else(|| GoalProgressReducer::from_goal(&goal)),
            goal,
            stream_revision,
            observations,
            interventions,
            intervention_traces,
        });
        if let Some(projection) = &projection {
            if checkpoint.as_ref().is_none_or(|saved| {
                saved.source_cursor <= head && (!checkpoint_valid || saved.source_cursor < head)
            }) {
                let scope = format!("session:{}", projection.goal.session_id);
                let body = self.event_store.prepare_content(
                    &scope,
                    &format!("{checkpoint_id}:{head}"),
                    &serde_json::to_value(projection).map_err(|error| error.to_string())?,
                    &crate::PreparedArtifactOrigin::CheckpointBody {
                        projection_id: checkpoint_id.clone(),
                        source_cursor: head,
                    },
                )?;
                let payload = serde_json::json!({"schema":2,"scope":scope,"body":body});
                let expected = checkpoint.as_ref().map_or(0, |saved| saved.revision);
                let write = if checkpoint.is_some() && !checkpoint_valid {
                    RuntimeEventStore::compare_and_repair_projection_checkpoint
                } else {
                    RuntimeEventStore::compare_and_put_projection_checkpoint
                };
                match write(
                    &self.event_store,
                    &checkpoint_id,
                    head,
                    expected,
                    &payload,
                    0,
                ) {
                    Ok(_) | Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => {}
                    Err(error) => return Err(error.to_string()),
                }
            }
        }
        Ok(projection)
    }

    pub fn observations(&self, goal_id: &str) -> Result<Vec<RuntimeObservation>, String> {
        Ok(self
            .projection(goal_id)?
            .map_or_else(Vec::new, |projection| projection.observations))
    }

    pub fn revise(
        &self,
        goal_id: &str,
        expected_revision: u64,
        user_sequence: u64,
        reason: impl Into<String>,
        mutate: impl FnOnce(&mut GoalContract) -> Vec<String>,
    ) -> Result<(GoalContract, GoalRevision), String> {
        let (goal, revision, event) =
            self.revision_event(goal_id, expected_revision, user_sequence, reason, mutate)?;
        let stream_id = event.event.stream_id.clone();
        let stream_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                stream_revision,
                format!("goal-revise:{goal_id}:{}", revision.revision),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok((goal, revision))
    }

    /// Construct a Goal revision for the caller's canonical graph
    /// transaction. This lets a running turn apply a user correction together
    /// with the node transition that observed it instead of introducing a
    /// second mutable Goal write path.
    pub fn revision_event(
        &self,
        goal_id: &str,
        expected_revision: u64,
        user_sequence: u64,
        reason: impl Into<String>,
        mutate: impl FnOnce(&mut GoalContract) -> Vec<String>,
    ) -> Result<(GoalContract, GoalRevision, RuntimeTransactionEventInput), String> {
        let reason = reason.into();
        let mut goal = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        self.event_store
            .ensure_session_recovery_admitted(&goal.session_id)?;
        if goal.revision != expected_revision {
            return Err(format!(
                "goal revision stale: expected {expected_revision}, actual {}",
                goal.revision
            ));
        }
        if user_sequence <= goal.user_sequence {
            return Err(format!(
                "goal user sequence is stale: incoming {user_sequence}, actual {}",
                goal.user_sequence
            ));
        }
        let previous = goal.revision;
        let changed_fields = mutate(&mut goal);
        goal.revision = goal.revision.saturating_add(1);
        goal.user_sequence = user_sequence;
        validate_goal(&goal)?;
        let revision = GoalRevision {
            goal_id: goal.id.clone(),
            previous_revision: previous,
            revision: goal.revision,
            reason,
            user_sequence,
            changed_fields,
        };
        let event = goal_event(
            &goal,
            "goal.revised",
            "open",
            "runtime.goal_store".to_string(),
            vec![RuntimeEventRef {
                kind: "goal_revision".to_string(),
                id: revision.revision.to_string(),
            }],
            serde_json::json!({ "goal": goal, "revision": revision }),
            format!("goal-revise:{goal_id}:{}", goal.revision),
        );
        Ok((goal, revision, event))
    }

    pub fn record_observation(&self, observation: RuntimeObservation) -> Result<(), String> {
        let key = observation.idempotency_fingerprint();
        let projection = self
            .projection(observation.goal_id())?
            .ok_or_else(|| format!("goal {} not found", observation.goal_id()))?;
        if projection
            .progress
            .applied_observation_keys
            .iter()
            .any(|existing| existing == &key)
        {
            if self
                .event_store
                .event_by_idempotency_key(
                    &stream_id(observation.goal_id()),
                    &format!("direct:{key}"),
                )
                .map_err(|error| error.to_string())?
                .is_some()
            {
                self.observation_event_at(&projection, &observation, format!("direct:{key}"))?;
            }
            return Ok(());
        }
        let event =
            self.observation_event_at(&projection, &observation, format!("direct:{key}"))?;
        let stream_id = event.event.stream_id.clone();
        let revision = event.event.payload["goal_source"]["sequence"]
            .as_u64()
            .ok_or("observation source revision missing")?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                revision,
                format!("goal-observation:{}:{key}", observation.goal_id()),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn record_intervention(
        &self,
        intervention: RuntimeIntervention,
        trigger_observations: &[RuntimeObservation],
    ) -> Result<(), String> {
        if trigger_observations.is_empty() {
            return Err(
                "runtime intervention requires at least one typed trigger observation".to_string(),
            );
        }
        // Settled observers may be delivered more than once after a process
        // restart.  The intervention is a durable proposal, so its
        // idempotency identity must be derived from the typed trigger and
        // intervention—not from a fresh UUID on every projection pass.
        let mut identity = serde_json::to_vec(&intervention).map_err(|error| error.to_string())?;
        for observation in trigger_observations {
            identity.extend_from_slice(observation.idempotency_fingerprint().as_bytes());
        }
        let idempotency_key = format!(
            "goal-intervention:{}:{:x}",
            intervention.goal_id,
            sha2::Sha256::digest(identity),
        );
        let event =
            self.intervention_event(&intervention, trigger_observations, idempotency_key.clone())?;
        let stream_id = event.event.stream_id.clone();
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &idempotency_key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id,
                revision,
                format!("{}:append", idempotency_key,),
                vec![event],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    /// Reserve one bounded semantic recovery attempt in the durable Goal
    /// stream.  This is only the authority/fence reservation; orchestration
    /// must still compile and commit the physical recovery patch through the
    /// canonical graph pipeline.
    pub fn reserve_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        source_revision: u64,
        budget: u32,
        idempotency_key: impl Into<String>,
        diagnostic: impl Into<String>,
    ) -> Result<Option<GoalContract>, String> {
        self.reserve_recovery_for_obligation(
            goal_id,
            expected_revision,
            source_revision,
            budget,
            idempotency_key,
            diagnostic,
            None,
        )
    }

    /// Reserve recovery while binding it to the exact source obligation.  The
    /// legacy method above remains available for older callers that do not
    /// have an obligation identity; new orchestration paths should use this
    /// stronger fence whenever one is available.
    pub fn reserve_recovery_for_obligation(
        &self,
        goal_id: &str,
        expected_revision: u64,
        source_revision: u64,
        budget: u32,
        idempotency_key: impl Into<String>,
        diagnostic: impl Into<String>,
        source_obligation_id: Option<String>,
    ) -> Result<Option<GoalContract>, String> {
        let key = idempotency_key.into();
        let diagnostic = diagnostic.into();
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if current.revision != expected_revision {
            return Err(format!(
                "goal recovery revision stale: expected {expected_revision}, actual {}",
                current.revision
            ));
        }
        if let Some(recovery) = &current.recovery {
            if recovery.idempotency_key == key {
                return Ok(Some(current));
            }
            if recovery.attempts >= recovery.budget
                || matches!(
                    recovery.status,
                    ObjectiveRecoveryStatus::Exhausted | ObjectiveRecoveryStatus::Failed
                )
            {
                return Ok(None);
            }
        }
        let next_attempt = current
            .recovery
            .as_ref()
            .map_or(1, |state| state.attempts.saturating_add(1));
        if next_attempt > budget {
            return Ok(None);
        }
        let next_sequence = current.user_sequence.saturating_add(1);
        let (updated, _) = self.revise(
            goal_id,
            expected_revision,
            next_sequence,
            "objective_recovery_reservation",
            |goal| {
                goal.recovery = Some(ObjectiveRecoveryState {
                    source_revision,
                    attempts: next_attempt,
                    budget,
                    status: ObjectiveRecoveryStatus::Pending,
                    idempotency_key: key.clone(),
                    mutation_id: None,
                    source_obligation_id: source_obligation_id.clone(),
                    last_diagnostic: Some(diagnostic.clone()),
                });
                vec!["recovery".to_string()]
            },
        )?;
        Ok(Some(updated))
    }

    /// Decide from the original specification and typed effects. Task focus is
    /// a separate admitted contract; root work never falls back to predictions.
    /// The returned source cut fences even a satisfied/exhausted decision.
    pub(crate) fn assess_write_checkpoint(
        &self,
        source: &GoalProjection,
        ticket: &crate::execution_core::NodeExecutionTicket,
        resolver: &crate::path_identity::WorkspacePathIdentityResolver,
        checkpoint: GoalWriteCheckpoint,
        task_focus: Option<(
            &[String],
            &[harness_contract::context::ObservedEvidence],
            bool,
        )>,
        pending: &[harness_contract::context::ObservedEvidence],
    ) -> Result<
        (
            GoalWriteDisposition,
            crate::runtime_event_store::ExpectedStreamRevision,
        ),
        String,
    > {
        let cut = crate::runtime_event_store::ExpectedStreamRevision {
            stream_id: stream_id(&source.goal.id),
            expected_revision: source.stream_revision,
        };
        let Some(binding) = source.goal.execution_binding.as_ref() else {
            // A tool-progress revalidation observes a Goal that is not bound to
            // an execution graph yet. There is no write obligation to settle or
            // recover for this observation, so it stays pending; only the
            // terminal answer path requires the binding fence.
            if matches!(&checkpoint, GoalWriteCheckpoint::ToolProgress { .. }) {
                return Ok((GoalWriteDisposition::Pending, cut));
            }
            return Err("write recovery binding missing".into());
        };
        if binding.root_execution_id != ticket.graph_id || ticket.attempt == 0 {
            return Err("write recovery Graph binding mismatch".into());
        }
        let satisfied = if let Some((scopes, evidence, unscoped)) = task_focus {
            write_obligation_satisfied(!scopes.is_empty(), scopes, evidence, unscoped, resolver)
        } else {
            self.turn_writes_satisfied_with_pending(source, resolver, pending)?
        };
        if satisfied {
            return Ok((GoalWriteDisposition::Satisfied, cut));
        }
        if let GoalWriteCheckpoint::ToolProgress {
            bounded_role,
            saturated,
            write_attempted,
        } = checkpoint
        {
            if bounded_role
                || !saturated
                || write_attempted
                || source
                    .goal
                    .recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.attempts != 0)
            {
                return Ok((GoalWriteDisposition::Pending, cut));
            }
        }
        let key = format!(
            "goal-write-recovery:{}:{}",
            source.goal.spec_digest, ticket.idempotency_key
        );
        if source
            .goal
            .recovery
            .as_ref()
            .is_some_and(|recovery| recovery.idempotency_key == key)
        {
            return Ok((GoalWriteDisposition::Pending, cut));
        }
        if let Some(recovery) = self.prepare_write_recovery(source, ticket)? {
            return Ok((GoalWriteDisposition::Recover(recovery), cut));
        }
        if matches!(checkpoint, GoalWriteCheckpoint::ToolProgress { .. }) {
            return Ok((GoalWriteDisposition::Pending, cut));
        }
        let intervention = RuntimeIntervention {
            goal_id: source.goal.id.clone(), kind: harness_contract::goal::RuntimeInterventionKind::Block,
            reason: "The current Goal workspace artifact remains unproven after its bounded recovery budget".into(),
            evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
            expected_graph_revision: None,
        };
        Ok((GoalWriteDisposition::Blocked(intervention), cut))
    }

    /// Prepare the existing durable recovery reservation for the original
    /// Graph transaction. A Host retry counter is not a second business owner.
    fn prepare_write_recovery(
        &self,
        source: &GoalProjection,
        ticket: &crate::execution_core::NodeExecutionTicket,
    ) -> Result<Option<PreparedGoalMutation>, String> {
        let binding = source
            .goal
            .execution_binding
            .as_ref()
            .ok_or("write recovery binding missing")?;
        if binding.root_execution_id != ticket.graph_id || ticket.attempt == 0 {
            return Err("write recovery Graph binding mismatch".into());
        }
        let key = format!(
            "goal-write-recovery:{}:{}",
            source.goal.spec_digest, ticket.idempotency_key
        );
        if source.goal.recovery.as_ref().is_some_and(|recovery| {
            recovery.idempotency_key == key
                || recovery.attempts >= recovery.budget
                || matches!(
                    recovery.status,
                    ObjectiveRecoveryStatus::Exhausted | ObjectiveRecoveryStatus::Failed
                )
        }) {
            return Ok(None);
        }
        let attempt = source
            .goal
            .recovery
            .as_ref()
            .map_or(1, |recovery| recovery.attempts.saturating_add(1));
        if attempt > 2 {
            return Ok(None);
        }
        let event_key = key.clone();
        let (_, _, mut event) = self.revision_event(
            &source.goal.id,
            source.goal.revision,
            source.goal.user_sequence.saturating_add(1),
            "objective_recovery_reservation",
            |goal| {
                goal.recovery = Some(ObjectiveRecoveryState {
                    source_revision: source.stream_revision,
                    attempts: attempt,
                    budget: 2,
                    status: ObjectiveRecoveryStatus::Pending,
                    idempotency_key: key,
                    mutation_id: None,
                    source_obligation_id: Some(format!("goal-write:{}", source.goal.spec_digest)),
                    last_diagnostic: Some(
                        "current Goal workspace write has no committed receipt".into(),
                    ),
                });
                vec!["recovery".into()]
            },
        )?;
        if event.event.payload["goal"]["spec_digest"] != source.goal.spec_digest {
            return Err("write recovery specification changed".into());
        }
        // Concurrent user revisions may claim the same next Goal revision.
        // The original stream CAS, not a shared revision-number event key,
        // decides the winner. Replay stays bound to this original attempt.
        event.idempotency_key = Some(event_key);
        Ok(Some(PreparedGoalMutation {
            stream_id: stream_id(&source.goal.id),
            expected_stream_revision: source.stream_revision,
            event,
        }))
    }

    /// Claim the physical graph mutation phase. The mutation id is durable so
    /// a crash can safely resume the same patch instead of creating another
    /// Team instance.
    pub fn begin_recovery_graph(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        mutation_id: &str,
    ) -> Result<GoalContract, String> {
        self.transition_recovery(
            goal_id,
            expected_revision,
            idempotency_key,
            ObjectiveRecoveryStatus::ApplyingGraph,
            Some(mutation_id.to_string()),
            None,
            "objective_recovery_applying_graph",
        )
    }

    /// Record that the graph mutation receipt is durable. Goal terminal
    /// verification remains a separate canonical step, but this marker makes
    /// the two-step boundary crash-recoverable and idempotent.
    pub fn mark_recovery_graph_applied(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        mutation_id: &str,
    ) -> Result<GoalContract, String> {
        self.transition_recovery(
            goal_id,
            expected_revision,
            idempotency_key,
            ObjectiveRecoveryStatus::GraphApplied,
            Some(mutation_id.to_string()),
            None,
            "objective_recovery_graph_applied",
        )
    }

    /// Fail closed with an actionable diagnostic. This is intentionally not a
    /// model-visible retry loop; only a fresh reservation can ever create a
    /// new semantic attempt, and budget exhaustion is terminal.
    pub fn fail_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        diagnostic: impl Into<String>,
    ) -> Result<GoalContract, String> {
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let recovery = current
            .recovery
            .as_ref()
            .ok_or_else(|| "goal recovery failure has no reservation".to_string())?;
        if recovery.idempotency_key != idempotency_key {
            return Err("goal recovery failure key mismatch".to_string());
        }
        let status = if recovery.attempts >= recovery.budget {
            ObjectiveRecoveryStatus::Exhausted
        } else {
            ObjectiveRecoveryStatus::Failed
        };
        self.transition_recovery(
            goal_id,
            expected_revision,
            idempotency_key,
            status,
            recovery.mutation_id.clone(),
            Some(diagnostic.into()),
            "objective_recovery_failed",
        )
    }

    fn transition_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
        status: ObjectiveRecoveryStatus,
        mutation_id: Option<String>,
        diagnostic: Option<String>,
        reason: &str,
    ) -> Result<GoalContract, String> {
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if current.revision != expected_revision {
            return Err(format!(
                "goal recovery transition revision stale: expected {expected_revision}, actual {}",
                current.revision
            ));
        }
        let recovery = current
            .recovery
            .as_ref()
            .ok_or_else(|| "goal recovery transition has no reservation".to_string())?;
        if recovery.idempotency_key != idempotency_key {
            return Err("goal recovery transition key mismatch".to_string());
        }
        if recovery.status == status
            && (mutation_id.is_none() || recovery.mutation_id == mutation_id)
        {
            return Ok(current);
        }
        let next_sequence = current.user_sequence.saturating_add(1);
        self.revise(goal_id, expected_revision, next_sequence, reason, |goal| {
            if let Some(recovery) = goal.recovery.as_mut() {
                recovery.status = status;
                if mutation_id.is_some() {
                    recovery.mutation_id = mutation_id.clone();
                }
                if let Some(diagnostic) = diagnostic.clone() {
                    recovery.last_diagnostic = Some(diagnostic);
                }
            }
            vec!["recovery.status".to_string()]
        })
        .map(|(goal, _)| goal)
    }

    /// Mark the already-reserved recovery mutation as committed only after the
    /// graph transaction has admitted its replacement work.  A stale caller
    /// cannot overwrite a newer reservation.
    pub fn commit_recovery(
        &self,
        goal_id: &str,
        expected_revision: u64,
        idempotency_key: &str,
    ) -> Result<GoalContract, String> {
        let current = self
            .get(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        if current.revision != expected_revision {
            return Err(format!(
                "goal recovery commit revision stale: expected {expected_revision}, actual {}",
                current.revision
            ));
        }
        let recovery = current
            .recovery
            .as_ref()
            .ok_or_else(|| "goal recovery commit has no reservation".to_string())?;
        if recovery.idempotency_key != idempotency_key {
            return Err("goal recovery commit key mismatch".to_string());
        }
        if recovery.status == ObjectiveRecoveryStatus::Committed {
            return Ok(current);
        }
        let next_sequence = current.user_sequence.saturating_add(1);
        self.revise(
            goal_id,
            expected_revision,
            next_sequence,
            "objective_recovery_committed",
            |goal| {
                if let Some(recovery) = goal.recovery.as_mut() {
                    recovery.status = ObjectiveRecoveryStatus::Committed;
                }
                vec!["recovery.status".to_string()]
            },
        )
        .map(|(goal, _)| goal)
    }

    /// Build, but do not append, an observation event. Node executors attach
    /// this to their graph commit so evidence and graph progress are durable in
    /// one transaction.
    pub fn observation_event(
        &self,
        observation: &RuntimeObservation,
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        validate_observation(observation)?;
        let projection = self
            .projection(observation.goal_id())?
            .ok_or_else(|| format!("goal {} not found", observation.goal_id()))?;
        self.observation_event_at(&projection, observation, idempotency_key)
    }

    pub(crate) fn observation_event_at(
        &self,
        projection: &GoalProjection,
        observation: &RuntimeObservation,
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        validate_observation(observation)?;
        let goal = &projection.goal;
        self.event_store
            .ensure_session_recovery_admitted(&goal.session_id)?;
        if goal.id != observation.goal_id() {
            return Err("observation crosses frozen Goal source".into());
        }
        if observation.identity.session_id != goal.session_id {
            return Err("observation crosses Goal Session".into());
        }
        let value = serde_json::to_value(observation).map_err(|error| error.to_string())?;
        if let Some(original) = self
            .event_store
            .event_by_idempotency_key(&stream_id(&goal.id), &idempotency_key)
            .map_err(|error| error.to_string())?
        {
            if self.event_store.resolve_observation_record(&original)? != value {
                return Err(
                    "observation retry conflicts with original current-schema bytes".into(),
                );
            }
            return Ok(RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: original.stream_id,
                    scope: original.scope,
                    kind: original.kind,
                    status: original.status,
                    actor: original.actor,
                    refs: original.refs,
                    payload: original.payload,
                },
                idempotency_key: Some(idempotency_key),
                schema_version: original.schema_version,
            });
        }
        let body = self.event_store.prepare_content(
            &format!("session:{}", goal.session_id),
            &format!("{}:{idempotency_key}", stream_id(&goal.id)),
            &value,
            &crate::PreparedArtifactOrigin::EventBody {
                stream_id: stream_id(&goal.id),
                idempotency_key: idempotency_key.clone(),
                kind: "goal.observation".into(),
            },
        )?;
        let classifier = RuntimeEventStore::classify_observation(&value)?;
        let mut event = goal_event(
            &goal,
            "goal.observation",
            "observed",
            observation.source.clone(),
            observation
                .evidence_delta
                .added
                .iter()
                .chain(observation.evidence_refs.iter())
                .map(|id| RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id: id.clone(),
                })
                .collect(),
            serde_json::json!({ "observation_schema":2, "session_id":goal.session_id,
                "goal_source":{"stream_id":stream_id(&goal.id),"sequence":projection.stream_revision,"goal_revision":goal.revision,"spec_digest":goal.spec_digest},
                "classifier":classifier,"observation_body":body }),
            idempotency_key,
        );
        event.schema_version = 2;
        Ok(event)
    }

    /// Build, but do not append, an intervention event. Policy remains a
    /// producer; the Runner commits and therefore owns application order.
    pub fn intervention_event(
        &self,
        intervention: &RuntimeIntervention,
        trigger_observations: &[RuntimeObservation],
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        let trigger = trigger_observations
            .iter()
            .max_by_key(|observation| observation.freshness.observed_at_ms)
            .ok_or_else(|| {
                "runtime intervention requires at least one typed trigger observation".to_string()
            })?;
        if trigger_observations
            .iter()
            .any(|observation| observation.goal_id() != intervention.goal_id)
        {
            return Err("runtime intervention trigger crosses Goal identity".to_string());
        }
        let trace = RuntimeInterventionTrace {
            identity: trigger.identity.clone(),
            trigger_observation_keys: trigger_observations
                .iter()
                .map(RuntimeObservation::idempotency_fingerprint)
                .collect(),
        };
        let goal = self
            .get(&intervention.goal_id)?
            .ok_or_else(|| format!("goal {} not found", intervention.goal_id))?;
        self.event_store
            .ensure_session_recovery_admitted(&goal.session_id)?;
        Ok(goal_event(
            &goal,
            "goal.intervention",
            "proposed",
            "runtime.intervention_policy".to_string(),
            intervention
                .evidence_refs
                .iter()
                .map(|id| RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id: id.clone(),
                })
                .collect(),
            serde_json::json!({
                "goal": goal,
                "intervention": intervention,
                "intervention_trace": trace,
            }),
            idempotency_key,
        ))
    }

    /// Build the terminal Goal event for a graph commit. The Runner commits it
    /// together with the graph transition and (when present) session terminal
    /// outbox record, so a graph can never be terminal while its goal remains
    /// open or vice versa.
    /// A live Program owns its successful Goal conclusion through the atomic
    /// Goal/Program transaction. Standalone goals retain the ordinary writer.
    fn validate_terminal_owner(
        &self,
        goal: &GoalContract,
        completion: GoalCompletion,
    ) -> Result<(), String> {
        if let Some(binding) = &goal.execution_binding {
            let program = crate::AgentActionService::new(Arc::clone(&self.event_store))
                .project_if_exists(&binding.agentic_program_id)
                .map_err(|error| error.to_string())?;
            if program.is_some() {
                return Err(
                    "program_terminal_authority: use the atomic Program conclusion for this Goal"
                        .into(),
                );
            }
        }
        if completion == GoalCompletion::Satisfied && goal.participation_requirement.is_some() {
            return Err("original_participation_requires_program_conclusion: no bound Program contribution evidence".into());
        }
        Ok(())
    }

    pub fn terminal_event(
        &self,
        goal_id: &str,
        completion: GoalCompletion,
        evidence_refs: Vec<String>,
        reason: String,
        idempotency_key: String,
    ) -> Result<RuntimeTransactionEventInput, String> {
        let projection = self
            .projection(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let mut goal = projection.goal;
        self.event_store
            .ensure_session_recovery_admitted(&goal.session_id)?;
        if goal.completion != GoalCompletion::Open {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        self.validate_terminal_owner(&goal, completion)?;
        let mut durable_evidence = projection.progress.evidence_refs.clone();
        durable_evidence.extend(evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        if completion == GoalCompletion::Satisfied {
            let unresolved_obligations = goal
                .obligations
                .iter()
                .filter(|obligation| {
                    obligation.required
                        && (obligation.state != ObjectiveObligationState::Satisfied
                            || (obligation
                                .evidence_requirement
                                .independent_verifier_required
                                && obligation.verifier_decision.is_none())
                            || (obligation.evidence_requirement.reread_required
                                && obligation.reread_receipts.is_empty()))
                })
                .map(|obligation| obligation.obligation_id.clone())
                .collect::<Vec<_>>();
            if !unresolved_obligations.is_empty() {
                return Err(format!(
                    "cannot satisfy a goal while required obligations are unresolved: {}",
                    unresolved_obligations.join(", ")
                ));
            }
            for criterion in &mut goal.criteria {
                if let Some(status) = projection.progress.criteria.get(&criterion.id) {
                    criterion.status = *status;
                }
                if criterion.status == AcceptanceStatus::Open
                    && !criterion.required_evidence.is_empty()
                    && criterion
                        .required_evidence
                        .iter()
                        .all(|evidence| durable_evidence.contains(evidence))
                {
                    criterion.status = AcceptanceStatus::Satisfied;
                }
            }
            if goal.criteria.iter().any(|criterion| {
                !matches!(
                    criterion.status,
                    AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                )
            }) {
                return Err(
                    "cannot satisfy a goal until every criterion has required evidence or a waiver"
                        .to_string(),
                );
            }
            if !projection.progress.open_conflicts.is_empty() {
                return Err(format!(
                    "cannot satisfy a goal with unresolved conflicts: {}",
                    projection.progress.open_conflicts.join(", ")
                ));
            }
            if !projection.progress.open_unknowns.is_empty() {
                return Err(format!(
                    "cannot satisfy a goal with unresolved unknowns: {}",
                    projection.progress.open_unknowns.join(", ")
                ));
            }
            if projection
                .progress
                .effects
                .values()
                .any(|effect| *effect == harness_contract::goal::EffectTerminalClass::Uncertain)
            {
                return Err(
                    "cannot satisfy a goal while an effect has no terminal receipt".to_string(),
                );
            }
        }
        validate_completion(&goal, &projection.progress, completion, &durable_evidence)?;
        for criterion in &goal.criteria {
            if criterion.status == AcceptanceStatus::Waived {
                let Some(waiver) = &criterion.waiver else {
                    return Err(format!(
                        "criterion {} is waived without a durable waiver receipt",
                        criterion.id
                    ));
                };
                if waiver.actor.trim().is_empty()
                    || waiver.reason.trim().is_empty()
                    || waiver.permission_receipt.trim().is_empty()
                {
                    return Err(format!(
                        "criterion {} has an invalid waiver receipt",
                        criterion.id
                    ));
                }
            }
        }
        goal.evidence_refs = durable_evidence;
        goal.completion = completion;
        let terminal_kind = match completion {
            GoalCompletion::Satisfied => harness_contract::goal::ObjectiveTerminalKind::Satisfied,
            GoalCompletion::Partial => {
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied
            }
            GoalCompletion::Blocked => harness_contract::goal::ObjectiveTerminalKind::Blocked,
            GoalCompletion::Failed => harness_contract::goal::ObjectiveTerminalKind::Failed,
            GoalCompletion::WaitingExternalDecision => {
                harness_contract::goal::ObjectiveTerminalKind::Blocked
            }
            GoalCompletion::Cancelled => harness_contract::goal::ObjectiveTerminalKind::Cancelled,
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        goal.terminal = Some(ObjectiveTerminal {
            kind: terminal_kind,
            terminal_fence: idempotency_key.clone(),
            authority_revision: goal.revision.saturating_add(1),
            reason: reason.clone(),
            evidence_refs: goal.evidence_refs.clone(),
            diagnostics: Vec::new(),
            committed_at_ms: crate::tool_invocation::now_ms(),
        });
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Failed => "failed".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        goal.revision = goal.revision.saturating_add(1);
        let status = match completion {
            GoalCompletion::Satisfied => "satisfied",
            GoalCompletion::Partial => "partial",
            GoalCompletion::Blocked => "blocked",
            GoalCompletion::Failed => "failed",
            GoalCompletion::WaitingExternalDecision => "waiting_external",
            GoalCompletion::Cancelled => "cancelled",
            GoalCompletion::Open => return Err("terminal completion must not be open".to_string()),
        };
        Ok(goal_event(
            &goal,
            "goal.completed",
            status,
            "runtime.synthesize".to_string(),
            evidence_refs
                .into_iter()
                .map(|id| RuntimeEventRef {
                    kind: "evidence".to_string(),
                    id,
                })
                .collect(),
            serde_json::json!({ "goal": goal, "completion_reason": reason }),
            idempotency_key,
        ))
    }

    pub fn complete(
        &self,
        goal_id: &str,
        expected_revision: u64,
        completion: GoalCompletion,
        reason: impl Into<String>,
    ) -> Result<GoalContract, String> {
        let stream_id = stream_id(goal_id);
        let stream_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let projection = self
            .projection(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let mut goal = projection.goal;
        if goal.revision != expected_revision {
            return Err(format!(
                "goal completion has stale revision {expected_revision}"
            ));
        }
        if goal.completion != GoalCompletion::Open || goal.terminal.is_some() {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        self.validate_terminal_owner(&goal, completion)?;
        let durable_evidence = projection.progress.evidence_refs.clone();
        apply_completion_criterion_state(&mut goal, &projection.progress, &durable_evidence);
        validate_completion(&goal, &projection.progress, completion, &durable_evidence)?;
        match completion {
            GoalCompletion::Satisfied => {
                for criterion in &mut goal.criteria {
                    if let Some(status) = projection.progress.criteria.get(&criterion.id) {
                        criterion.status = *status;
                    }
                }
                if goal.criteria.iter().any(|criterion| {
                    !matches!(
                        criterion.status,
                        AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                    )
                }) {
                    return Err(
                        "cannot satisfy a goal until every acceptance criterion is satisfied or waived"
                            .to_string(),
                        );
                }
                if !projection.progress.open_conflicts.is_empty()
                    || !projection.progress.open_unknowns.is_empty()
                    || projection.progress.effects.values().any(|effect| {
                        *effect == harness_contract::goal::EffectTerminalClass::Uncertain
                    })
                {
                    return Err(
                        "cannot satisfy a goal while conflicts, unknowns, or uncertain effects remain unresolved"
                            .to_string(),
                    );
                }
            }
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string());
            }
            GoalCompletion::Partial
            | GoalCompletion::Blocked
            | GoalCompletion::Failed
            | GoalCompletion::WaitingExternalDecision
            | GoalCompletion::Cancelled => {}
        }
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Failed => "failed".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => {
                return Err("completion cannot transition back to open".to_string());
            }
        };
        goal.evidence_refs = durable_evidence;
        goal.revision = goal.revision.saturating_add(1);
        self.append_goal_event(
            &stream_id,
            stream_revision,
            format!("goal-complete:{goal_id}:{}", goal.revision),
            "goal.completed",
            match completion {
                GoalCompletion::Satisfied => "satisfied",
                GoalCompletion::Partial => "partial",
                GoalCompletion::Blocked => "blocked",
                GoalCompletion::Failed => "failed",
                GoalCompletion::WaitingExternalDecision => "waiting_external",
                GoalCompletion::Cancelled => "cancelled",
                GoalCompletion::Open => {
                    return Err("completion cannot transition back to open".to_string());
                }
            },
            &goal,
            "runtime.goal_store",
            vec![RuntimeEventRef {
                kind: "completion_reason".to_string(),
                id: reason.into(),
            }],
        )?;
        Ok(goal)
    }

    /// Commit the business Objective terminal exactly once.  Local Team or
    /// graph terminal facts are inputs to this method, never a substitute for
    /// the obligation/evidence checks below.
    pub fn complete_objective(
        &self,
        goal_id: &str,
        expected_revision: u64,
        terminal: ObjectiveTerminal,
    ) -> Result<GoalContract, String> {
        if terminal.terminal_fence.trim().is_empty() || terminal.reason.trim().is_empty() {
            return Err("objective terminal requires fence and reason".to_string());
        }
        let stream_id = stream_id(goal_id);
        let stream_revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        let projection = self
            .projection(goal_id)?
            .ok_or_else(|| format!("goal {goal_id} not found"))?;
        let mut goal = projection.goal;
        if let Some(existing) = goal.terminal.as_ref() {
            if existing.terminal_fence == terminal.terminal_fence {
                let mut retry = terminal.clone();
                // Retry wall time is not part of the terminal's semantic identity.
                retry.committed_at_ms = existing.committed_at_ms;
                if existing != &retry {
                    return Err("objective_terminal_fence_conflict: terminal fence names different outcome or evidence".into());
                }
                return Ok(goal);
            }
            return Err(format!(
                "goal {goal_id} already has a different terminal fence"
            ));
        }
        if goal.completion != GoalCompletion::Open {
            return Err(format!("goal {goal_id} is already terminal"));
        }
        if goal.revision != expected_revision {
            return Err(format!(
                "objective terminal has stale revision {expected_revision}, actual {}",
                goal.revision
            ));
        }
        let completion = match terminal.kind {
            ObjectiveTerminalKind::Satisfied => GoalCompletion::Satisfied,
            ObjectiveTerminalKind::PartiallySatisfied => GoalCompletion::Partial,
            ObjectiveTerminalKind::Blocked => GoalCompletion::Blocked,
            ObjectiveTerminalKind::Failed => GoalCompletion::Failed,
            ObjectiveTerminalKind::Cancelled => GoalCompletion::Cancelled,
        };
        self.validate_terminal_owner(&goal, completion)?;
        let mut durable_evidence = projection.progress.evidence_refs.clone();
        durable_evidence.extend(goal.evidence_refs.iter().cloned());
        durable_evidence.extend(terminal.evidence_refs.iter().cloned());
        durable_evidence.sort();
        durable_evidence.dedup();
        apply_completion_criterion_state(&mut goal, &projection.progress, &durable_evidence);
        if terminal.kind == ObjectiveTerminalKind::Satisfied {
            let required_open = goal.obligations.iter().any(|obligation| {
                obligation.required
                    && (obligation.state != ObjectiveObligationState::Satisfied
                        || (obligation
                            .evidence_requirement
                            .independent_verifier_required
                            && obligation.verifier_decision.is_none())
                        || (obligation.evidence_requirement.reread_required
                            && obligation.reread_receipts.is_empty()))
            });
            if required_open {
                return Err(
                    "cannot satisfy Objective while a required obligation is unresolved"
                        .to_string(),
                );
            }
            if goal.criteria.iter().any(|criterion| {
                !matches!(
                    criterion.status,
                    AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
                )
            }) {
                return Err(
                    "cannot satisfy Objective while acceptance criteria are open".to_string(),
                );
            }
        }
        goal.terminal = Some(terminal.clone());
        goal.completion = completion;
        goal.phase = match completion {
            GoalCompletion::Satisfied => "completed".to_string(),
            GoalCompletion::Partial => "partial".to_string(),
            GoalCompletion::Blocked => "blocked".to_string(),
            GoalCompletion::Failed => "failed".to_string(),
            GoalCompletion::WaitingExternalDecision => "waiting_external".to_string(),
            GoalCompletion::Cancelled => "cancelled".to_string(),
            GoalCompletion::Open => return Err("objective terminal cannot be open".to_string()),
        };
        validate_completion(&goal, &projection.progress, completion, &durable_evidence)?;
        goal.evidence_refs = durable_evidence;
        goal.revision = goal.revision.saturating_add(1);
        self.append_goal_event(
            &stream_id,
            stream_revision,
            format!("objective-terminal:{goal_id}:{}", terminal.terminal_fence),
            "goal.completed",
            match completion {
                GoalCompletion::Satisfied => "satisfied",
                GoalCompletion::Partial => "partial",
                GoalCompletion::Blocked => "blocked",
                GoalCompletion::Failed => "failed",
                GoalCompletion::WaitingExternalDecision => "waiting_external",
                GoalCompletion::Cancelled => "cancelled",
                GoalCompletion::Open => return Err("objective terminal cannot be open".to_string()),
            },
            &goal,
            "runtime.objective_supervisor",
            vec![RuntimeEventRef {
                kind: "terminal_fence".to_string(),
                id: terminal.terminal_fence,
            }],
        )?;
        Ok(goal)
    }

    fn append_goal_event(
        &self,
        stream_id: &str,
        expected_revision: u64,
        transaction_id: String,
        kind: &str,
        status: &str,
        goal: &GoalContract,
        actor: &str,
        refs: Vec<RuntimeEventRef>,
    ) -> Result<(), String> {
        self.event_store
            .ensure_session_recovery_admitted(&goal.session_id)?;
        self.append_goal_event_with_payload(
            stream_id,
            expected_revision,
            transaction_id,
            kind,
            status,
            actor,
            refs,
            serde_json::json!({ "goal": goal }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn append_goal_event_with_payload(
        &self,
        stream_id: &str,
        expected_revision: u64,
        transaction_id: String,
        kind: &str,
        status: &str,
        actor: &str,
        refs: Vec<RuntimeEventRef>,
        payload: serde_json::Value,
    ) -> Result<(), String> {
        self.event_store
            .append_batch_if_revision(
                stream_id.to_string(),
                expected_revision,
                transaction_id,
                vec![RuntimeEventInput {
                    stream_id: stream_id.to_string(),
                    scope: RuntimeEventScope::Goal,
                    kind: kind.to_string(),
                    status: Some(status.to_string()),
                    actor: Some(actor.to_string()),
                    refs,
                    payload,
                }
                .into()],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

pub(crate) fn validate_observation(observation: &RuntimeObservation) -> Result<(), String> {
    if observation.identity.workspace_id.trim().is_empty()
        || observation.identity.session_id.trim().is_empty()
        || observation.identity.graph_id.trim().is_empty()
        || observation.identity.goal_id.trim().is_empty()
        || observation.source.trim().is_empty()
        || observation.source_revision == 0
        || observation.freshness.observed_at_ms == 0
        || observation.freshness.policy_revision.trim().is_empty()
        || observation.fingerprint.trim().is_empty()
        || observation
            .identity
            .turn_id
            .as_deref()
            .is_some_and(|turn_id| turn_id.trim().is_empty())
    {
        return Err("runtime observation has incomplete identity or provenance".to_string());
    }
    if observation.failed() && observation.failure_class.is_none() {
        return Err("failed runtime observation requires a typed failure class".to_string());
    }
    if observation.result_class == harness_contract::goal::ObservationResultClass::Succeeded
        && observation.failure_class.is_some()
    {
        return Err("successful runtime observation cannot carry a failure class".to_string());
    }
    Ok(())
}

fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    normalize(&mut values);
    values
}

fn normalize(values: &mut Vec<String>) {
    values.retain(|value| !value.trim().is_empty());
    values.sort();
    values.dedup();
}

fn apply_resolution_delta(values: &mut Vec<String>, id: &str, change: ResolutionDeltaKind) {
    match change {
        ResolutionDeltaKind::Opened => values.push(id.to_string()),
        ResolutionDeltaKind::Resolved => values.retain(|value| value != id),
    }
}

fn goal_event(
    goal: &GoalContract,
    kind: &str,
    status: &str,
    actor: String,
    refs: Vec<RuntimeEventRef>,
    payload: serde_json::Value,
    idempotency_key: String,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: stream_id(&goal.id),
            scope: RuntimeEventScope::Goal,
            kind: kind.to_string(),
            status: Some(status.to_string()),
            actor: Some(actor),
            refs,
            payload,
        },
        idempotency_key: Some(idempotency_key),
        schema_version: 1,
    }
}

fn stream_id(goal_id: &str) -> String {
    format!("goal:{goal_id}")
}

fn apply_completion_criterion_state(
    goal: &mut GoalContract,
    progress: &GoalProgressSnapshot,
    durable_evidence: &[String],
) {
    for criterion in &mut goal.criteria {
        if let Some(status) = progress.criteria.get(&criterion.id) {
            criterion.status = *status;
        }
        // An empty requirement set means semantic review is still required;
        // all() over an empty iterator must never manufacture success.
        if criterion.status == AcceptanceStatus::Open
            && !criterion.required_evidence.is_empty()
            && criterion
                .required_evidence
                .iter()
                .all(|evidence| durable_evidence.contains(evidence))
        {
            criterion.status = AcceptanceStatus::Satisfied;
        }
    }
}

fn validate_completion(
    goal: &GoalContract,
    progress: &GoalProgressSnapshot,
    completion: GoalCompletion,
    durable_evidence: &[String],
) -> Result<(), String> {
    if completion == GoalCompletion::Open {
        return Err("terminal completion must not be open".to_string());
    }
    if completion == GoalCompletion::WaitingExternalDecision {
        return Err(
            "waiting_external_decision is a resumable wait state, not an immutable Goal terminal"
                .to_string(),
        );
    }
    for criterion in &goal.criteria {
        if criterion.status == AcceptanceStatus::Waived {
            let Some(waiver) = &criterion.waiver else {
                return Err(format!(
                    "criterion {} is waived without a durable waiver receipt",
                    criterion.id
                ));
            };
            if waiver.actor.trim().is_empty()
                || waiver.reason.trim().is_empty()
                || waiver.permission_receipt.trim().is_empty()
            {
                return Err(format!(
                    "criterion {} has an invalid waiver receipt",
                    criterion.id
                ));
            }
        }
    }
    if completion != GoalCompletion::Satisfied {
        return Ok(());
    }
    let unresolved_obligations = goal
        .obligations
        .iter()
        .filter(|obligation| {
            obligation.required
                && (obligation.state != ObjectiveObligationState::Satisfied
                    || (obligation
                        .evidence_requirement
                        .independent_verifier_required
                        && obligation.verifier_decision.is_none())
                    || (obligation.evidence_requirement.reread_required
                        && obligation.reread_receipts.is_empty()))
        })
        .map(|obligation| obligation.obligation_id.clone())
        .collect::<Vec<_>>();
    if !unresolved_obligations.is_empty() {
        return Err(format!(
            "cannot satisfy a goal while required obligations are unresolved: {}",
            unresolved_obligations.join(", ")
        ));
    }
    if goal.criteria.iter().any(|criterion| {
        !matches!(
            criterion.status,
            AcceptanceStatus::Satisfied | AcceptanceStatus::Waived
        )
    }) {
        return Err(
            "cannot satisfy a goal until every current criterion has semantic review or required durable evidence"
                .to_string(),
        );
    }
    let user_intent = goal
        .user_intent_criterion_id
        .as_deref()
        .and_then(|id| goal.criteria.iter().find(|criterion| criterion.id == id));
    if goal.scope == harness_contract::goal::GoalScope::UserObjective && user_intent.is_none() {
        return Err(
            "a user Objective cannot complete without its original user_intent criterion"
                .to_string(),
        );
    }
    if goal.scope == harness_contract::goal::GoalScope::UserObjective && durable_evidence.is_empty()
    {
        return Err(
            "a user Objective cannot complete with an empty durable evidence set".to_string(),
        );
    }
    if !progress.open_conflicts.is_empty() {
        return Err(format!(
            "cannot satisfy a goal with unresolved conflicts: {}",
            progress.open_conflicts.join(", ")
        ));
    }
    if !progress.open_unknowns.is_empty() {
        return Err(format!(
            "cannot satisfy a goal with unresolved unknowns: {}",
            progress.open_unknowns.join(", ")
        ));
    }
    if progress
        .effects
        .values()
        .any(|effect| *effect == harness_contract::goal::EffectTerminalClass::Uncertain)
    {
        return Err("cannot satisfy a goal while an effect has no terminal receipt".to_string());
    }
    Ok(())
}

fn validate_goal(goal: &GoalContract) -> Result<(), String> {
    if goal.id.trim().is_empty()
        || goal.session_id.trim().is_empty()
        || goal.objective.trim().is_empty()
    {
        return Err("goal id, session_id, and objective must be non-empty".to_string());
    }
    if goal.criteria.is_empty() {
        return Err("goal requires at least one acceptance criterion".to_string());
    }
    if goal
        .criteria
        .iter()
        .any(|criterion| criterion.id.trim().is_empty() || criterion.statement.trim().is_empty())
    {
        return Err("goal criteria require non-empty id and statement".to_string());
    }
    if goal.spec_revision == 0 || goal.spec_digest.trim().is_empty() {
        return Err(
            "goal requires a non-zero semantic specification revision and digest".to_string(),
        );
    }
    if let Some(contract) = &goal.input_contract {
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or("goal_input_missing_binding")?;
        if !matches!(contract.version, 1 | 2)
            || contract.input_revision == 0
            || contract.session_id != goal.session_id
            || contract.session_id != binding.session_id
            || contract.turn_id != binding.turn_id
            || contract.principal_id.trim().is_empty()
            || (contract.version == 1
                && (contract.principal_id != format!("session:{}", binding.session_id)
                    || Some(contract.source_ref.as_str()) != goal.source_intent_ref.as_deref()))
            || (contract.version == 2
                && !contract
                    .source_ref
                    .starts_with("session_input_application:"))
            || contract.content_sha256.len() != 64
            || !contract
                .content_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("goal_input_source_binding_mismatch".into());
        }
    }
    if goal.scope == harness_contract::goal::GoalScope::UserObjective {
        let source_intent = goal
            .source_intent_ref
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "user Objective requires a durable source_intent_ref".to_string())?;
        let intent_id = goal
            .user_intent_criterion_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "user Objective requires a user_intent criterion".to_string())?;
        if !goal
            .criteria
            .iter()
            .any(|criterion| criterion.id == intent_id)
        {
            return Err("user_intent criterion is not present in the Goal criteria".to_string());
        }
        let binding = goal
            .execution_binding
            .as_ref()
            .ok_or_else(|| "user Objective requires immutable GoalExecutionBinding".to_string())?;
        if binding.objective_id.trim().is_empty()
            || binding.session_id != goal.session_id
            || binding.turn_id.trim().is_empty()
            || binding.root_execution_id.trim().is_empty()
            || binding.agentic_program_id.trim().is_empty()
            || source_intent.trim().is_empty()
        {
            return Err(
                "GoalExecutionBinding is incomplete or crosses the Goal Session".to_string(),
            );
        }
    }
    if let Some(requirement) = goal.participation_requirement.as_ref() {
        if requirement.minimum_team_count == 0 || requirement.source_ref.trim().is_empty() {
            return Err(
                "participation requirement needs a user source and positive team count".to_string(),
            );
        }
    }
    for criterion in &goal.criteria {
        if criterion.status == AcceptanceStatus::Waived {
            let Some(waiver) = &criterion.waiver else {
                return Err(format!(
                    "criterion {} is waived without a durable waiver receipt",
                    criterion.id
                ));
            };
            if waiver.actor.trim().is_empty()
                || waiver.reason.trim().is_empty()
                || waiver.permission_receipt.trim().is_empty()
            {
                return Err(format!(
                    "criterion {} has an invalid waiver receipt",
                    criterion.id
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    fn input_binding() -> harness_contract::goal::GoalExecutionBinding {
        harness_contract::goal::GoalExecutionBinding {
            objective_id: "objective:s:t".into(),
            session_id: "s".into(),
            turn_id: "t".into(),
            root_execution_id: "g".into(),
            agentic_program_id: "p".into(),
        }
    }

    #[test]
    fn unresolved_scope_proposals_cannot_authorize_or_deny_an_input() {
        let binding = input_binding();
        for (input, proposals) in [
            (
                "Create a summary file; choose a suitable filename yourself.",
                vec![],
            ),
            (
                "不要修改src/a.rs；请修改src/b.rs",
                vec!["write:src/a.rs".into(), "write:src/b.rs".into()],
            ),
            ("Read out.md", vec!["write:out.md".into()]),
            (
                "The quoted command is write out.md",
                vec!["write:out.md".into()],
            ),
        ] {
            let contract = super::compile_goal_input_contract(
                &binding,
                "session_message:m",
                1,
                input,
                &proposals,
            )
            .unwrap();
            assert!(
                contract.commitments.is_empty(),
                "identity and parser output cannot create semantic authority"
            );
            assert_eq!(contract.proposed_workspace_scopes, proposals);
            assert_eq!(contract.source_ref, "session_message:m");
            assert_eq!(contract.principal_id, "session:s");
        }
    }

    #[test]
    fn historical_absent_input_contract_does_not_appear_in_encoded_goal() {
        let previous = goal();
        let encoded = serde_json::to_value(&previous).unwrap();
        assert!(encoded.get("input_contract").is_none());
        let decoded: harness_contract::goal::GoalContract =
            serde_json::from_value(encoded.clone()).unwrap();
        assert!(decoded.input_contract.is_none());
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }

    use super::*;
    use harness_contract::{
        core::MeasureProvenance,
        goal::{
            AcceptanceCriterion, AcceptanceStatus, ContextDelta, CostDelta, CriterionDelta,
            EvidenceDelta, InformationGain, ObservationFreshness, ObservationResultClass,
            ParallelismDelta, RuntimeObservationIdentity, RuntimeObservationKind, UnknownDelta,
        },
    };

    fn goal() -> GoalContract {
        GoalContract {
            input_contract: None,
            id: "goal-test".to_string(),
            session_id: "session-test".to_string(),
            objective: "complete governed work".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "checked".to_string(),
                statement: "result is checked".to_string(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: Vec::new(),
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            scope: harness_contract::goal::GoalScope::Internal,
            user_intent_criterion_id: Some("checked".to_string()),
            source_intent_ref: Some("session_message:test".to_string()),
            execution_binding: None,
            spec_revision: 1,
            spec_digest: "test".to_string(),
            review_refs: Vec::new(),
            waiting: None,
            participation_requirement: None,
            obligations: Vec::new(),
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        }
    }

    fn authenticated_revision_fixture() -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        GoalProjection,
        UserGoalInputFrame,
        crate::execution_core::NodeExecutionTicket,
    ) {
        use harness_contract::turn::{SessionInputUserSource, SESSION_INPUT_USER_SOURCE_KEY};
        let events = Arc::new(RuntimeEventStore::for_test());
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        let commits = crate::ExecutionCommitService::new(events.clone());
        let mut graph = ExecutionGraph::new("user revision fixture");
        graph.id = "user-revision-graph".into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session-test".into(),
            turn_id: "user-turn".into(),
            root_task_id: "user-task".into(),
            task_id: "user-task".into(),
            generation: 1,
        });
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph.nodes.push(node.clone());
        graph = commits.register_graph(graph).unwrap().graph;
        graph = commits
            .transition_node(&graph, &node.id, ExecutionNodeStatus::Ready, None, vec![])
            .unwrap()
            .graph;
        let ticket = crate::execution_core::NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: node.executor_kind.clone(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        let binding = crate::execution_core::graph::ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let graph = commits
            .bind_and_start_nodes(
                &graph,
                std::collections::BTreeMap::from([(node.id.clone(), binding)]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        let goals = GoalStore::new(events.clone());
        let mut original = goal();
        original.scope = harness_contract::goal::GoalScope::UserObjective;
        original.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
            objective_id: "user-objective".into(),
            session_id: original.session_id.clone(),
            turn_id: "user-turn".into(),
            root_execution_id: graph.id.clone(),
            agentic_program_id: "user-program".into(),
        });
        goals.create(original).unwrap();
        let source = goals.projection("goal-test").unwrap().unwrap();
        let text = "请直接解释结果，不再生成报告文件。";
        let origin = SessionInputUserSource {
            principal_id: "real-user-7".into(),
            session_id: "session-test".into(),
            input_id: "user-input".into(),
            request_id: "user-request".into(),
            message_id: "user-message".into(),
            session_generation: 1,
            content_sha256: format!("{:x}", sha2::Sha256::digest(text.as_bytes())),
        };
        let classification=serde_json::json!({"source_kind":"user","metadata":{SESSION_INPUT_USER_SOURCE_KEY:origin}}).to_string();
        let record:crate::session_runtime_port::RuntimeSessionInputRecord=serde_json::from_value(serde_json::json!({
            "input_id":"user-input","request_id":"user-request","turn_id":"user-turn","message_id":"user-message","session_id":"session-test",
            "sequence":4,"session_generation":1,"decision":"start_new_turn","classification_json":classification,"status":"running",
            "runtime_commit_cursor":null,"attempts":1,"next_attempt_at_ms":0,"claim_owner":"original-owner","claim_token":"claim",
            "claim_expires_at_ms":1000,"revision":3,"created_at_ms":1,"updated_at_ms":2
        })).unwrap();
        (
            events,
            goals,
            source,
            UserGoalInputFrame {
                inputs: vec![(record, origin, text.into())],
            },
            ticket,
        )
    }

    #[tokio::test]
    async fn initial_goal_binding_survives_runtime_reconstruction_without_reinterpreting_input() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        use harness_contract::turn::{SessionInputUserSource, SESSION_INPUT_USER_SOURCE_KEY};
        let workspace = tempfile::tempdir().unwrap();
        let events = Arc::new(RuntimeEventStore::for_test());
        let sessions = Arc::new(crate::test_support::session_store());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(workspace.path().join("original-artifacts")));
        sessions.create_session(&session::SessionRecord {
            session_id: "session-test".into(), platform: "test".into(),
            chat_id: "session-test".into(), user_id: None, model: None,
            created_at: "2026-10-28T00:00:00Z".into(), last_activity: "2026-10-28T00:00:00Z".into(),
            message_count: 0, reset_policy: "manual".into(), metadata_json: None,
            input_tokens: 0, output_tokens: 0, status: "active".into(),
        }).await.unwrap();
        for (n, id) in [(0, "initial"), (1, "append-a"), (2, "append-b")] {
            let origin = SessionInputUserSource {
                principal_id: "real-user".into(), session_id: "session-test".into(),
                input_id: id.into(), request_id: format!("request-{id}"),
                message_id: format!("message-{id}"), session_generation: 1,
                content_sha256: format!("{:x}", sha2::Sha256::digest(b"56")),
            };
            sessions.append_ingress_with_runtime_outbox("session-test", "user",
                Some(&serde_json::json!([{"type":"text","text":"56"}]).to_string()), n + 1,
                &session::SessionRuntimeOutboxRequest {
                    input_id: id.into(), request_id: origin.request_id.clone(),
                    turn_id: if n == 0 { "user-turn".into() } else { format!("turn-{id}") }, message_id: origin.message_id.clone(),
                    session_generation: 1,
                    decision: if n == 0 { harness_contract::turn::InputRoutingDecision::StartNewTurn } else { harness_contract::turn::InputRoutingDecision::SupplementCurrentTurn },
                    target_turn_id: (n > 0).then(|| "user-turn".into()),
                    classification_json: Some(serde_json::json!({"metadata":{SESSION_INPUT_USER_SOURCE_KEY:origin}}).to_string()),
                    task_route_hint: None, created_at_ms: n + 1, runtime_options_json: None,
                }).await.unwrap();
        }
        let mut initial = goal();
        initial.scope = harness_contract::goal::GoalScope::UserObjective;
        initial.source_intent_ref = Some("session_message:message-initial".into());
        initial.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
            objective_id: "user-objective".into(), session_id: "session-test".into(),
            turn_id: "user-turn".into(),
            root_execution_id: crate::session_execution::session_ingress_graph_id(
                "session-test", "request-initial", "user-turn"),
            agentic_program_id: "user-program".into(),
        });
        GoalStore::new(events.clone()).create(initial).unwrap();
        for _epoch in 0..2 {
            let services = crate::RuntimeServices::test_builder(workspace.path(), workspace.path())
                .runtime_event_store(events.clone()).artifact_store(artifacts.clone()).build().unwrap();
            services.install_test_session_store(sessions.clone()).unwrap();
            let source = services.goal_store().projection("goal-test").unwrap().unwrap();
            let ingress = crate::TurnIngressRef {
                session_id: "session-test".into(), turn_id: "user-turn".into(),
                request_id: "request-initial".into(), message_id: "message-initial".into(),
                primary_task_id: "task".into(), root_task_id: "task".into(),
                session_generation: 1, input_sequence: 1, claim_owner: "original".into(),
                claim_token: "claim".into(), claim_revision: 1, handoff_id: None,
            };
            assert!(crate::turn_inbox::authenticated_goal_inputs(
                services.as_ref(), &source, Some(&ingress), &[])
                .await.unwrap().is_none(), "initial input already belongs to durable Goal, not an amendment");
            let frame = crate::turn_inbox::authenticated_goal_inputs(
                services.as_ref(), &source, Some(&ingress),
                &["initial".into(), "append-a".into(), "append-b".into()])
                .await.unwrap().unwrap();
            assert_eq!(frame.inputs.iter().map(|(r,_,_)|r.input_id.as_str()).collect::<Vec<_>>(),
                vec!["append-a", "append-b"], "same text is not an input identity");
            assert_eq!(frame.inputs[0].2, "56");
            assert_eq!(frame.inputs[1].2, "56");
            let mut continuation = source.clone();
            continuation.goal.source_intent_ref = Some("session_message:old-original".into());
            let new_frame = crate::turn_inbox::authenticated_goal_inputs(
                services.as_ref(), &continuation, Some(&ingress), &[]).await.unwrap().unwrap();
            assert_eq!(new_frame.inputs[0].0.input_id, "initial",
                "a continuation carrying an old source must not skip its new ingress");
            for mismatch in ["session", "turn", "root", "source", "missing"] {
                let mut changed = source.clone();
                match mismatch {
                    "session" => changed.goal.execution_binding.as_mut().unwrap().session_id = "foreign".into(),
                    "turn" => changed.goal.execution_binding.as_mut().unwrap().turn_id = "foreign".into(),
                    "root" => changed.goal.execution_binding.as_mut().unwrap().root_execution_id = "foreign".into(),
                    "source" => changed.goal.source_intent_ref = Some("session_message:other".into()),
                    "missing" => changed.goal.execution_binding = None,
                    _ => unreachable!(),
                }
                assert!(!changed.binds_initial_input("session-test", "user-turn", "request-initial", "message-initial"), "{mismatch}");
            }
            services.shutdown_maintenance().await;
            assert_eq!(services.shutdown_execution().await.forced_aborts, 0);
            drop(services);
        }
        assert!(sessions.get_session_runtime_outbox("request-initial").await.unwrap().unwrap()
            .application_receipt.is_none(), "selection cannot fabricate application consumption");
    }

    fn current_write_goal_fixture() -> (
        Arc<RuntimeEventStore>,
        GoalStore,
        GoalProjection,
        crate::execution_core::NodeExecutionTicket,
    ) {
        use harness_contract::goal::{GoalInputCommitment, GoalInputContract, GoalInputSpan};
        let (events, goals, original, _, ticket) = authenticated_revision_fixture();
        goals
            .revise(
                "goal-test",
                original.goal.revision,
                original.goal.user_sequence + 1,
                "literal current business contract",
                |goal| {
                    goal.input_contract = Some(GoalInputContract {
                        version: 2,
                        principal_id: "literal-user".into(),
                        session_id: "session-test".into(),
                        turn_id: "user-turn".into(),
                        source_ref: "session_input_application:literal".into(),
                        input_revision: 1,
                        content_sha256: "a".repeat(64),
                        commitments: vec![GoalInputCommitment::WorkspaceWrite {
                            path: Some("report.txt".into()),
                            source: GoalInputSpan { start: 0, end: 1 },
                        }],
                        proposed_workspace_scopes: vec![],
                    });
                    goal.spec_revision += 1;
                    goal.spec_digest = goal_spec_digest(goal);
                    vec!["input_contract".into()]
                },
            )
            .unwrap();
        let source = goals.projection("goal-test").unwrap().unwrap();
        (events, goals, source, ticket)
    }

    #[test]
    fn goal_owns_write_checkpoint_effect_budget_and_exhaustion_without_host_verdicts() {
        let (_, goals, source, ticket) = current_write_goal_fixture();
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("report.txt"), "report").unwrap();
        std::fs::write(workspace.path().join("other.txt"), "other").unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let assess =
            |source: &GoalProjection,
             checkpoint,
             pending: &[harness_contract::context::ObservedEvidence]| {
                goals
                    .assess_write_checkpoint(source, &ticket, &resolver, checkpoint, None, pending)
                    .unwrap()
            };
        assert!(matches!(
            assess(&source, GoalWriteCheckpoint::FinalAnswer, &[]).0,
            GoalWriteDisposition::Recover(_)
        ));
        assert!(
            goals.get("goal-test").unwrap().unwrap().recovery.is_none(),
            "decision never consumes budget independently"
        );
        let correct = resolver
            .observe_tool_scope(
                "edit",
                "write:report.txt",
                Some(&format!("{:x}", sha2::Sha256::digest(b"report"))),
                7,
            )
            .unwrap();
        let wrong = resolver
            .observe_tool_scope(
                "edit",
                "write:other.txt",
                Some(&format!("{:x}", sha2::Sha256::digest(b"other"))),
                8,
            )
            .unwrap();
        assert!(matches!(
            assess(
                &source,
                GoalWriteCheckpoint::FinalAnswer,
                std::slice::from_ref(&correct)
            )
            .0,
            GoalWriteDisposition::Satisfied
        ));
        assert!(matches!(
            assess(
                &source,
                GoalWriteCheckpoint::FinalAnswer,
                std::slice::from_ref(&wrong)
            )
            .0,
            GoalWriteDisposition::Recover(_)
        ));
        let focus = vec!["write:other.txt".to_string()];
        assert!(
            matches!(
                goals
                    .assess_write_checkpoint(
                        &source,
                        &ticket,
                        &resolver,
                        GoalWriteCheckpoint::FinalAnswer,
                        Some((&focus, std::slice::from_ref(&wrong), false)),
                        &[]
                    )
                    .unwrap()
                    .0,
                GoalWriteDisposition::Satisfied
            ),
            "a legitimate Task focus is not a request for a duplicate root write"
        );
        for checkpoint in [
            GoalWriteCheckpoint::ToolProgress {
                bounded_role: true,
                saturated: true,
                write_attempted: false,
            },
            GoalWriteCheckpoint::ToolProgress {
                bounded_role: false,
                saturated: false,
                write_attempted: false,
            },
            GoalWriteCheckpoint::ToolProgress {
                bounded_role: false,
                saturated: true,
                write_attempted: true,
            },
        ] {
            assert!(matches!(
                assess(&source, checkpoint, &[]).0,
                GoalWriteDisposition::Pending
            ));
        }
        assert!(matches!(
            assess(
                &source,
                GoalWriteCheckpoint::ToolProgress {
                    bounded_role: false,
                    saturated: true,
                    write_attempted: false
                },
                &[]
            )
            .0,
            GoalWriteDisposition::Recover(_)
        ));
        let key = format!(
            "goal-write-recovery:{}:{}",
            source.goal.spec_digest, ticket.idempotency_key
        );
        goals
            .revise(
                "goal-test",
                source.goal.revision,
                source.goal.user_sequence + 1,
                "committed original recovery",
                |goal| {
                    goal.recovery = Some(ObjectiveRecoveryState {
                        source_revision: source.stream_revision,
                        attempts: 1,
                        budget: 2,
                        status: ObjectiveRecoveryStatus::Pending,
                        idempotency_key: key.clone(),
                        mutation_id: None,
                        source_obligation_id: Some("literal-original-write".into()),
                        last_diagnostic: None,
                    });
                    vec!["recovery".into()]
                },
            )
            .unwrap();
        let replay = goals.projection("goal-test").unwrap().unwrap();
        assert!(
            matches!(
                assess(&replay, GoalWriteCheckpoint::FinalAnswer, &[]).0,
                GoalWriteDisposition::Pending
            ),
            "same ticket cannot consume or invent another decision"
        );
        goals
            .revise(
                "goal-test",
                replay.goal.revision,
                replay.goal.user_sequence + 1,
                "original exhausted budget",
                |goal| {
                    let recovery = goal.recovery.as_mut().unwrap();
                    recovery.idempotency_key = "another-completed-attempt".into();
                    recovery.attempts = 2;
                    recovery.status = ObjectiveRecoveryStatus::Exhausted;
                    vec!["recovery".into()]
                },
            )
            .unwrap();
        let exhausted = goals.projection("goal-test").unwrap().unwrap();
        let (decision, cut) = assess(&exhausted, GoalWriteCheckpoint::FinalAnswer, &[]);
        assert_eq!(cut.expected_revision, exhausted.stream_revision);
        assert!(
            matches!(decision, GoalWriteDisposition::Blocked(ref intervention)
            if intervention.kind == harness_contract::goal::RuntimeInterventionKind::Block && intervention.goal_id == "goal-test")
        );
        assert!(matches!(
            assess(
                &exhausted,
                GoalWriteCheckpoint::ToolProgress {
                    bounded_role: false,
                    saturated: true,
                    write_attempted: false
                },
                &[]
            )
            .0,
            GoalWriteDisposition::Pending
        ));
        assert!(
            matches!(
                assess(&exhausted, GoalWriteCheckpoint::FinalAnswer, &[correct]).0,
                GoalWriteDisposition::Satisfied
            ),
            "actual correct effect wins over an obsolete recovery state"
        );
        let mut foreign = ticket.clone();
        foreign.graph_id = "foreign".into();
        assert!(goals
            .assess_write_checkpoint(
                &source,
                &foreign,
                &resolver,
                GoalWriteCheckpoint::FinalAnswer,
                None,
                &[]
            )
            .is_err());
        // Persist a real current-spec observation, then revise only the required
        // object. Its old Goal source cannot satisfy the new specification.
        let (_, revised_goals, revised_source, revised_ticket) = current_write_goal_fixture();
        let mut observed = observation(99);
        observed.identity.graph_id = revised_ticket.graph_id.clone();
        observed.identity.turn_id = Some("user-turn".into());
        observed.identity.node_id = Some(revised_ticket.node_id.clone());
        observed.observed_evidence = vec![resolver
            .observe_tool_scope(
                "edit",
                "write:report.txt",
                Some(&format!("{:x}", sha2::Sha256::digest(b"report"))),
                9,
            )
            .unwrap()];
        revised_goals.record_observation(observed).unwrap();
        let with_receipt = revised_goals.projection("goal-test").unwrap().unwrap();
        assert!(matches!(
            revised_goals
                .assess_write_checkpoint(
                    &with_receipt,
                    &revised_ticket,
                    &resolver,
                    GoalWriteCheckpoint::FinalAnswer,
                    None,
                    &[]
                )
                .unwrap()
                .0,
            GoalWriteDisposition::Satisfied
        ));
        revised_goals
            .revise(
                "goal-test",
                with_receipt.goal.revision,
                with_receipt.goal.user_sequence + 1,
                "new object contract",
                |goal| {
                    if let harness_contract::goal::GoalInputCommitment::WorkspaceWrite {
                        path,
                        ..
                    } = &mut goal.input_contract.as_mut().unwrap().commitments[0]
                    {
                        *path = Some("other.txt".into());
                    }
                    goal.spec_revision += 1;
                    goal.spec_digest = goal_spec_digest(goal);
                    vec!["input_contract".into()]
                },
            )
            .unwrap();
        let new_spec = revised_goals.projection("goal-test").unwrap().unwrap();
        assert_ne!(new_spec.goal.spec_digest, revised_source.goal.spec_digest);
        assert!(
            matches!(
                revised_goals
                    .assess_write_checkpoint(
                        &new_spec,
                        &revised_ticket,
                        &resolver,
                        GoalWriteCheckpoint::FinalAnswer,
                        None,
                        &[]
                    )
                    .unwrap()
                    .0,
                GoalWriteDisposition::Recover(_)
            ),
            "old-spec receipt must not satisfy the revised target"
        );
        let (_, empty_goals, empty, _, empty_ticket) = authenticated_revision_fixture();
        assert!(matches!(
            empty_goals
                .assess_write_checkpoint(
                    &empty,
                    &empty_ticket,
                    &resolver,
                    GoalWriteCheckpoint::FinalAnswer,
                    None,
                    &[]
                )
                .unwrap()
                .0,
            GoalWriteDisposition::Satisfied
        ));
    }

    #[test]
    fn current_goal_write_recovery_is_prepared_and_graph_atomic_with_a_stale_source_rejection() {
        use harness_contract::execution_graph::{
            ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
        };
        let (events, goals, source, ticket) = current_write_goal_fixture();
        let workspace = tempfile::tempdir().unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let (decision, _) = goals
            .assess_write_checkpoint(
                &source,
                &ticket,
                &resolver,
                GoalWriteCheckpoint::FinalAnswer,
                None,
                &[],
            )
            .unwrap();
        let GoalWriteDisposition::Recover(prepared) = decision else {
            panic!("literal missing write must prepare recovery")
        };
        assert!(
            goals.get("goal-test").unwrap().unwrap().recovery.is_none(),
            "prepare cannot append"
        );
        let mut foreign = ticket.clone();
        foreign.graph_id = "foreign".into();
        assert_eq!(
            goals.prepare_write_recovery(&source, &foreign).unwrap_err(),
            "write recovery Graph binding mismatch"
        );
        let graph = crate::ExecutionGraphStateStore::new(events.clone())
            .load(&ticket.graph_id)
            .unwrap();
        goals
            .revise(
                "goal-test",
                source.goal.revision,
                source.goal.user_sequence + 1,
                "concurrent user revision",
                |goal| {
                    goal.constraints.push("new constraint".into());
                    vec!["constraints".into()]
                },
            )
            .unwrap();
        let mut outcome = crate::execution_core::NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("recovery".into()),
            summary: None,
            wait_class: None,
            evidence_refs: vec![],
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: 1,
        });
        outcome
            .source_revisions
            .insert(prepared.stream_id, prepared.expected_stream_revision);
        outcome.domain_events.push(prepared.event);
        let committer = crate::ExecutionCommitService::new(events.clone());
        let sources = committer
            .execution_outcome_sources(&graph, &ticket, &outcome)
            .unwrap();
        let rejected = committer.transition_execution_outcome(&graph, &ticket, &outcome, &sources);
        let error = rejected
            .err()
            .expect("a changed Goal must reject the original reservation");
        assert!(
            matches!(error,crate::execution_core::graph::ExecutionCommitError::EventStore(
            crate::RuntimeEventStoreError::StaleRevision {ref stream_id, expected, actual})
            if stream_id == "goal:goal-test" && expected == source.stream_revision && actual == source.stream_revision + 1),
            "{error}"
        );
        assert!(goals.get("goal-test").unwrap().unwrap().recovery.is_none());
        assert_eq!(
            crate::ExecutionGraphStateStore::new(events)
                .load(&graph.id)
                .unwrap()
                .revision,
            graph.revision
        );

        let (events, goals, source, ticket) = current_write_goal_fixture();
        let workspace = tempfile::tempdir().unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let (decision, _) = goals
            .assess_write_checkpoint(
                &source,
                &ticket,
                &resolver,
                GoalWriteCheckpoint::FinalAnswer,
                None,
                &[],
            )
            .unwrap();
        let GoalWriteDisposition::Recover(prepared) = decision else {
            panic!("literal missing write must prepare recovery")
        };
        let graph = crate::ExecutionGraphStateStore::new(events.clone())
            .load(&ticket.graph_id)
            .unwrap();
        let mut outcome = crate::execution_core::NodeExecutionOutcome::new(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("recovery".into()),
            summary: None,
            wait_class: None,
            evidence_refs: vec![],
            failure: None,
            usage: ExecutionUsage::default(),
            finished_at_ms: 1,
        });
        outcome
            .source_revisions
            .insert(prepared.stream_id, prepared.expected_stream_revision);
        outcome.domain_events.push(prepared.event);
        let committer = crate::ExecutionCommitService::new(events);
        let sources = committer
            .execution_outcome_sources(&graph, &ticket, &outcome)
            .unwrap();
        committer
            .transition_execution_outcome(&graph, &ticket, &outcome, &sources)
            .unwrap();
        let current = goals.projection("goal-test").unwrap().unwrap();
        assert_eq!(current.goal.recovery.as_ref().unwrap().attempts, 1);
        assert_eq!(
            current.goal.recovery.as_ref().unwrap().source_obligation_id,
            Some(format!("goal-write:{}", source.goal.spec_digest))
        );
        assert!(
            goals
                .prepare_write_recovery(&current, &ticket)
                .unwrap()
                .is_none(),
            "original attempt cannot spend twice"
        );
    }

    #[test]
    fn current_goal_constraints_keep_object_a_immutable_and_allow_disjoint_object_b() {
        use harness_contract::{
            goal::{GoalInputCommitment, GoalInputContract, GoalInputSpan},
            policy::{PermissionOperation, PermissionResource, PermissionScope},
            tool::{
                ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
                ToolPermissionMode,
            },
        };
        let (_, _, source, _, _) = authenticated_revision_fixture();
        let mut current = source.goal;
        current.input_contract = Some(GoalInputContract {
            version: 2,
            principal_id: "user".into(),
            session_id: current.session_id.clone(),
            turn_id: "user-turn".into(),
            source_ref: "session_input_application:actual".into(),
            input_revision: 1,
            content_sha256: "a".repeat(64),
            proposed_workspace_scopes: vec![],
            commitments: vec![GoalInputCommitment::ForbidWorkspaceWrite {
                path: Some("a.txt".into()),
                source: GoalInputSpan { start: 0, end: 3 },
            }],
        });
        let workspace = tempfile::tempdir().unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let mut effect = ToolEffectDescriptor {
            tool_id: "edit".into(),
            descriptor_hash: "registered".into(),
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Write,
                target: Some("b.txt".into()),
            }],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };
        assert_eq!(
            goal_tool_effect_rejection(&current, &effect, &resolver),
            None
        );
        effect.scopes[0].target = Some("./a.txt".into());
        assert_eq!(
            goal_tool_effect_rejection(&current, &effect, &resolver),
            Some("current Goal prohibits mutation of this workspace object")
        );
        effect.scopes[0].target = None;
        assert_eq!(
            goal_tool_effect_rejection(&current, &effect, &resolver),
            Some("current Goal requires exact workspace targets")
        );
        effect.scopes[0].target = Some("b.txt".into());
        effect.spawns_process = true;
        assert_eq!(
            goal_tool_effect_rejection(&current, &effect, &resolver),
            Some("current Goal requires disjoint workspace effects")
        );
        current.input_contract.as_mut().unwrap().commitments.push(
            GoalInputCommitment::ForbidTools {
                source: GoalInputSpan { start: 0, end: 3 },
            },
        );
        effect.required_permission = ToolPermissionMode::ReadOnly;
        assert_eq!(
            goal_tool_effect_rejection(&current, &effect, &resolver),
            Some("current Goal prohibits tool calls")
        );
    }

    #[tokio::test]
    async fn authenticated_user_revision_program_transaction_fences_program_graph_and_foreign_source(
    ) {
        use harness_contract::agent_action::{
            AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
            TeamCreateInput,
        };
        for case in ["apply", "stale_program", "stale_graph", "foreign_source"] {
            let workspace = tempfile::tempdir().unwrap();
            let (events, goals, source, mut frame, ticket) = authenticated_revision_fixture();
            let services = crate::RuntimeServices::test_builder(workspace.path(), workspace.path())
                .runtime_event_store(events.clone())
                .build()
                .unwrap();
            let actor = AgentActorBinding {
                original_execution: None,
                objective_id: "user-objective".into(),
                program_id: "user-program".into(),
                session_id: "session-test".into(),
                turn_id: "user-turn".into(),
                root_execution_id: Some(ticket.graph_id.clone()),
                required_team_count: 1,
                exact_team_count: None,
                objective_summary: "original Program objective".into(),
                model_lease: "controlled-unit".into(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: vec![],
                actor_id: "original-root".into(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            };
            let create = |id: &str| AgentActionEnvelope {
                action_id: id.into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::TeamCreate(TeamCreateInput {
                    name: id.into(),
                    mission: "original bounded work".into(),
                    objective: None,
                }),
            };
            let created = services
                .agent_action_service()
                .apply(&create("original-team"))
                .unwrap();
            assert_eq!(created.status, AgentActionStatus::Applied);
            let program = services
                .agent_action_service()
                .project("user-program")
                .unwrap();
            let graph = services.graph_state_store().load(&ticket.graph_id).unwrap();
            let proposal=harness_contract::input_disposition::ModelInputDispositionBatch::decode_business_proposal(USER_REVISION_PROPOSAL,1).unwrap();
            let prepared = goals
                .prepare_user_input_revision(&source, &frame, &proposal, &ticket)
                .unwrap();
            let artifact = services
                .artifact_store()
                .write_bytes(
                    harness_contract::context::ArtifactWriteDescriptor {
                        media_type: "application/json".into(),
                        visibility_scope: "session:session-test".into(),
                        expected_bytes: Some(USER_REVISION_PROPOSAL.len() as u64),
                        original_name: None,
                    },
                    USER_REVISION_PROPOSAL.as_bytes(),
                )
                .await
                .unwrap();
            match case {
                "stale_program" => {
                    assert_eq!(
                        services
                            .agent_action_service()
                            .apply(&create("concurrent-team"))
                            .unwrap()
                            .status,
                        AgentActionStatus::Applied
                    );
                }
                "stale_graph" => {
                    services
                        .commit_service()
                        .replan(
                            &graph,
                            vec![harness_contract::execution_graph::ExecutionNodeSpec::new(
                                harness_contract::execution_graph::ExecutionNodeKind::InlineModel,
                                "inline_model",
                                "concurrent required work",
                            )],
                            vec![],
                            "concurrent graph change".into(),
                        )
                        .unwrap();
                }
                "foreign_source" => {
                    frame.inputs[0].1.principal_id = "foreign-user".into();
                }
                _ => {}
            }
            let before_program = services
                .agent_action_service()
                .project("user-program")
                .unwrap();
            let before_graph = services.graph_state_store().load(&ticket.graph_id).unwrap();
            let result = services.commit_user_input_goal_revision(
                &source.goal,
                &frame,
                &prepared,
                program.revision,
                graph.revision,
                &artifact.selector,
            );
            if case == "apply" {
                result.unwrap();
                let current = goals.get("goal-test").unwrap().unwrap();
                assert_eq!(current.revision, 2);
                assert_eq!(
                    current.objective,
                    "Explain the result directly without generating a report file"
                );
                assert_eq!(
                    current.source_intent_ref,
                    Some("session_message:test".into())
                );
                assert_eq!(
                    services
                        .graph_state_store()
                        .load(&ticket.graph_id)
                        .unwrap()
                        .revision,
                    graph.revision,
                    "Program transaction must not replay Goal through Graph"
                );
                let applied_program = services
                    .agent_action_service()
                    .project("user-program")
                    .unwrap();
                assert_eq!(applied_program.revision, program.revision + 1);
                assert_eq!(
                    applied_program.objective_summary, "original Program objective",
                    "frozen actor identity is not rewritten from business text"
                );
                services
                    .commit_user_input_goal_revision(
                        &source.goal,
                        &frame,
                        &prepared,
                        program.revision,
                        graph.revision,
                        &artifact.selector,
                    )
                    .unwrap();
                assert_eq!(
                    goals.get("goal-test").unwrap().unwrap().revision,
                    2,
                    "original action replay is exactly once"
                );
            } else {
                let reason =
                    result.expect_err("changed authenticated inputs must not partially adopt");
                match case {
                    "stale_program" => assert!(reason.contains("stale_revision"), "{reason}"),
                    "stale_graph" => assert!(
                        reason.contains(&ticket.graph_id) && reason.contains("revision"),
                        "{reason}"
                    ),
                    "foreign_source" => {
                        assert!(reason == "user Goal source identity mismatch", "{reason}")
                    }
                    _ => unreachable!(),
                }
                assert_eq!(goals.get("goal-test").unwrap().unwrap(), source.goal);
                assert_eq!(
                    services
                        .agent_action_service()
                        .project("user-program")
                        .unwrap(),
                    before_program
                );
                assert_eq!(
                    services.graph_state_store().load(&ticket.graph_id).unwrap(),
                    before_graph
                );
            }
            services.shutdown_maintenance().await;
            assert_eq!(services.shutdown_execution().await.forced_aborts, 0);
        }
    }

    const USER_REVISION_PROPOSAL: &str = r#"{"decisions":[{"input_slots":[0],"action":"amend_current_turn","relation":"supplement","objective":"Explain the result directly without generating a report file","required":true,"confidence_basis_points":1,"reason":"The user changed the requested deliverable","business_revision":{"commitments":[],"constraints":[],"retired_criteria":[],"retired_obligations":[]}}]}"#;

    #[tokio::test]
    async fn authenticated_model_proposal_reaches_the_original_goal_graph_transaction() {
        use crate::conversation::{ApiClient, ApiRequest, AssistantEvent, ModelStepIntent};
        use futures::Stream;
        use harness_contract::execution_graph::{
            ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
        };
        use std::pin::Pin;
        struct ProposalApi {
            empty_tools: bool,
            requests: Arc<std::sync::Mutex<Vec<bool>>>,
        }
        impl ApiClient for ProposalApi {
            fn step_tool_definitions(&self) -> Option<Vec<crate::ProviderToolDefinition>> {
                Some(if self.empty_tools {
                    vec![]
                } else {
                    vec![crate::ProviderToolDefinition {
                        name: "read_file".into(),
                        description: None,
                        input_schema: serde_json::json!({"type":"object"}),
                    }]
                })
            }
            fn configure_tool_exposure(
                &mut self,
                p: harness_contract::tool::ToolExposureProjection,
            ) {
                self.empty_tools = p.bootstrap_ids.is_empty() && p.active_ids.is_empty();
            }
            fn stream(
                &mut self,
                _: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, crate::RuntimeError>> + Send + '_>>
            {
                self.requests.lock().unwrap().push(self.empty_tools);
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta(USER_REVISION_PROPOSAL.into())),
                    Ok(AssistantEvent::Usage(model_protocol::usage::TokenUsage {
                        input_tokens: 17,
                        output_tokens: 23,
                        ..Default::default()
                    })),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }
        let requests = Arc::new(std::sync::Mutex::new(vec![]));
        let mut runtime = crate::ConversationRuntime::new(
            crate::Session::new(),
            ProposalApi {
                empty_tools: false,
                requests: requests.clone(),
            },
            crate::StaticToolExecutor::new().register("read_file", |_| {
                panic!("interpretation must never execute an external tool")
            }),
            crate::PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
            vec!["system".into()],
        )
        .without_memory()
        .with_model_context_window(128_000);
        runtime.set_active_model("controlled-local-unit");
        runtime
            .begin_turn_strategy("user-turn", "explain the result")
            .unwrap();
        let step = runtime
            .execute_user_input_interpretation(
                "explain the result",
                true,
                false,
                1,
                "input_slot0 is the original authenticated user revision",
            )
            .await
            .unwrap();
        assert_eq!(
            *requests.lock().unwrap(),
            vec![true],
            "the real Provider request must have an empty tool exposure"
        );
        assert!(
            !step.text_only_response,
            "interpretation is not terminal synthesis"
        );
        assert_eq!(step.usage.input_tokens, 17);
        assert_eq!(step.usage.output_tokens, 23);
        assert!(step.early_tool_receipts.is_empty());
        let proposal = step.input_interpretation.unwrap().unwrap();
        let (events, goals, source, frame, ticket) = authenticated_revision_fixture();
        let mutation = goals
            .prepare_user_input_revision(&source, &frame, &proposal, &ticket)
            .unwrap();
        assert_eq!(
            goals.get("goal-test").unwrap().unwrap(),
            source.goal,
            "preparation may not write Goal"
        );
        let graph = crate::ExecutionGraphStateStore::new(events.clone())
            .load(&ticket.graph_id)
            .unwrap();
        crate::ExecutionCommitService::new(events.clone())
            .transition_node(
                &graph,
                &ticket.node_id,
                ExecutionNodeStatus::Completed,
                Some(ExecutionNodeResult {
                    status: ExecutionNodeStatus::Completed,
                    result_ref: Some("controlled-interpreted-input".into()),
                    summary: None,
                    wait_class: None,
                    evidence_refs: vec![],
                    failure: None,
                    usage: ExecutionUsage {
                        input_tokens: 17,
                        output_tokens: 23,
                        provider_usage_observed: Some(true),
                        ..Default::default()
                    },
                    finished_at_ms: 3,
                }),
                vec![mutation.event],
            )
            .unwrap();
        let current = goals.get("goal-test").unwrap().unwrap();
        assert_eq!(
            current.objective,
            "Explain the result directly without generating a report file"
        );
        assert_eq!(
            current.criteria[0].statement,
            "Explain the result directly without generating a report file"
        );
        assert_eq!(current.user_intent_criterion_id, Some("checked".into()));
        assert_eq!(
            current.source_intent_ref,
            Some("session_message:test".into()),
            "original user source history remains"
        );
        assert_eq!(current.spec_revision, 2);
        assert_eq!(current.completion, GoalCompletion::Open);
        assert_eq!(current.input_contract.unwrap().principal_id, "real-user-7");
        let ordinary = runtime
            .execute_model_step("ordinary final JSON document", false)
            .await
            .unwrap();
        assert!(matches!(
            ordinary.intent,
            ModelStepIntent::FinalAnswer { .. }
        ));
        assert!(
            ordinary.input_interpretation.is_none(),
            "identical final-answer JSON is not a semantic adoption purpose"
        );
    }

    #[test]
    fn authenticated_user_revision_rejects_foreign_source_and_stale_goal_without_partial_commit() {
        let proposal=harness_contract::input_disposition::ModelInputDispositionBatch::decode_business_proposal(USER_REVISION_PROPOSAL,1).unwrap();
        let (events, goals, source, frame, ticket) = authenticated_revision_fixture();
        let mut foreign = frame.clone();
        foreign.inputs[0].1.session_id = "foreign-session".into();
        assert_eq!(
            goals
                .prepare_user_input_revision(&source, &foreign, &proposal, &ticket)
                .unwrap_err(),
            "user Goal source identity mismatch"
        );
        assert_eq!(goals.get("goal-test").unwrap().unwrap(), source.goal);
        let mutation = goals
            .prepare_user_input_revision(&source, &frame, &proposal, &ticket)
            .unwrap();
        let graph = crate::ExecutionGraphStateStore::new(events.clone())
            .load(&ticket.graph_id)
            .unwrap();
        goals
            .revise(
                "goal-test",
                1,
                2,
                "concurrent legitimate user revision",
                |goal| {
                    goal.objective = "newer actual objective".into();
                    vec!["objective".into()]
                },
            )
            .unwrap();
        let result = crate::ExecutionCommitService::new(events.clone()).transition_node(
            &graph,
            &ticket.node_id,
            harness_contract::execution_graph::ExecutionNodeStatus::Completed,
            None,
            vec![mutation.event],
        );
        assert!(
            matches!(
                result,
                Err(
                    crate::execution_core::graph::ExecutionCommitError::EventStore(
                        crate::RuntimeEventStoreError::StaleRevision { .. }
                    )
                )
            ),
            "{:?}",
            result.as_ref().err()
        );
        assert_eq!(
            crate::ExecutionGraphStateStore::new(events)
                .load(&ticket.graph_id)
                .unwrap()
                .revision,
            graph.revision
        );
        assert_eq!(
            goals.get("goal-test").unwrap().unwrap().objective,
            "newer actual objective"
        );
    }

    #[test]
    fn ordinary_turn_candidate_and_goal_source_are_fenced_by_the_original_graph_transaction() {
        use crate::execution_core::{
            ExecutionCommitService, ExecutionGraphStateStore, NodeExecutionTicket,
        };
        use harness_contract::execution_graph::{
            ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
        };
        for (stale_source, block_trigger) in [
            (false, None),
            (true, None),
            (false, Some(false)),
            (false, Some(true)),
        ] {
            let events = Arc::new(RuntimeEventStore::for_test());
            let commits = ExecutionCommitService::new(events.clone());
            crate::test_support::register_running_root(
                events.clone(),
                &crate::CowdExecutionContext {
                    execution_id: "graph".into(),
                    session_id: "session-test".into(),
                    turn_id: "turn".into(),
                },
            );
            let states = ExecutionGraphStateStore::new(events.clone());
            let graph = states.load("graph").unwrap();
            let model = graph.nodes[0].clone();
            let ticket = NodeExecutionTicket {
                graph_id: "graph".into(),
                node_id: model.id.clone(),
                executor_kind: model.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: model.idempotency_key.clone(),
                payload_ref: model.payload_ref.clone(),
            };
            let goals = Arc::new(GoalStore::new(events.clone()));
            let mut original = goal();
            original.scope = harness_contract::goal::GoalScope::UserObjective;
            original.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: "objective".into(),
                session_id: "session-test".into(),
                turn_id: "turn".into(),
                root_execution_id: "graph".into(),
                agentic_program_id: "program".into(),
            });
            original.criteria[0].required_evidence = vec!["execution_graph:graph".into()];
            goals.create(original).unwrap();
            let source = goals.projection("goal-test").unwrap().unwrap();
            let mut observed = observation(1);
            observed.kind = RuntimeObservationKind::GraphProgress;
            observed.source = "runtime.model_step".into();
            observed.identity.node_id = Some(model.id.clone());
            let candidate = goals
                .turn_candidate_event_at(&source, &observed, &ticket, "actual answer")
                .unwrap();
            let mut domain_events = vec![candidate];
            if let Some(foreign_trigger) = block_trigger {
                let mut trigger = observed.clone();
                if foreign_trigger {
                    trigger.identity.session_id = "foreign-session".into();
                }
                domain_events.push(
                    goals
                        .intervention_event(
                            &harness_contract::goal::RuntimeIntervention {
                                goal_id: "goal-test".into(),
                                kind: harness_contract::goal::RuntimeInterventionKind::Block,
                                reason: "original business decision".into(),
                                evidence_refs: vec![format!("execution_node:{}", model.id)],
                                expected_graph_revision: None,
                            },
                            &[trigger],
                            format!("{}:goal-intervention", model.idempotency_key),
                        )
                        .unwrap(),
                );
            }
            let synth = ExecutionNodeSpec::new(
                ExecutionNodeKind::Synthesize,
                "synthesize",
                "unit terminal",
            );
            let synth_id = synth.id.clone();
            let result = harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("assistant_json:\"actual answer\"".into()),
                summary: None,
                wait_class: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: harness_contract::execution_graph::ExecutionUsage::default(),
                finished_at_ms: 1,
            };
            let graph = commits
                .transition_node_with_replan(
                    &graph,
                    &model.id,
                    result,
                    domain_events,
                    vec![synth],
                    vec![],
                    "unit model result".into(),
                    true,
                )
                .unwrap()
                .graph;
            let graph = commits
                .transition_node(&graph, &synth_id, ExecutionNodeStatus::Ready, None, vec![])
                .unwrap()
                .graph;
            let synth = graph.nodes.iter().find(|node| node.id == synth_id).unwrap();
            let terminal_ticket = NodeExecutionTicket {
                graph_id: graph.id.clone(),
                node_id: synth_id.clone(),
                executor_kind: synth.executor_kind.clone(),
                service_class: graph.service_class,
                attempt: 1,
                idempotency_key: synth.idempotency_key.clone(),
                payload_ref: synth.payload_ref.clone(),
            };
            let graph = commits
                .bind_and_start_node(
                    &graph,
                    &synth_id,
                    crate::execution_core::graph::ExecutionNodeBinding {
                        executor_kind: terminal_ticket.executor_kind.clone(),
                        ticket_idempotency_key: terminal_ticket.idempotency_key.clone(),
                        attempt: 1,
                        resource_lease_refs: vec![],
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )
                .unwrap()
                .graph;
            let root = tempfile::tempdir().unwrap();
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
            let prepared = ObjectiveSupervisor::new(goals.clone()).prepare_turn_conclusion(
                "goal-test",
                &terminal_ticket,
                &resolver,
                "unit conclusion".into(),
                "unit-fence".into(),
            );
            if block_trigger == Some(true) {
                assert!(
                    matches!(prepared, Err(ref reason) if reason == "turn_block_intervention_binding_mismatch"),
                    "a foreign trigger cannot be borrowed for this candidate"
                );
                assert_eq!(
                    goals.get("goal-test").unwrap().unwrap().completion,
                    GoalCompletion::Open
                );
                continue;
            }
            let prepared = prepared.unwrap();
            let expected_completion = if block_trigger == Some(false) {
                GoalCompletion::Partial
            } else {
                GoalCompletion::Satisfied
            };
            assert_eq!(
                prepared.completion, expected_completion,
                "the original committed Block must survive an answer candidate"
            );
            assert_eq!(
                goals.get("goal-test").unwrap().unwrap().completion,
                GoalCompletion::Open,
                "preparation must not independently write Goal"
            );
            if stale_source {
                goals
                    .revise("goal-test", 1, 2, "actual user revision", |goal| {
                        goal.spec_revision = 2;
                        goal.spec_digest = "new-specification".into();
                        goal.source_intent_ref = Some("session_message:replacement".into());
                        vec![
                            "spec_revision".into(),
                            "spec_digest".into(),
                            "source_intent_ref".into(),
                        ]
                    })
                    .unwrap();
            }
            let result = harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("unit-final".into()),
                summary: None,
                wait_class: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: harness_contract::execution_graph::ExecutionUsage::default(),
                finished_at_ms: 2,
            };
            let committed = commits.transition_node(
                &graph,
                &synth_id,
                ExecutionNodeStatus::Completed,
                Some(result),
                vec![prepared.event],
            );
            if stale_source {
                assert!(
                    matches!(
                        committed,
                        Err(
                            crate::execution_core::graph::ExecutionCommitError::EventStore(
                                crate::RuntimeEventStoreError::StaleRevision { .. }
                            )
                        )
                    ),
                    "stale original Goal source must reject the checked Graph transaction"
                );
                assert_eq!(states.load("graph").unwrap().revision, graph.revision);
                assert_eq!(
                    goals.get("goal-test").unwrap().unwrap().completion,
                    GoalCompletion::Open
                );
                let rejected = ObjectiveSupervisor::new(goals.clone()).prepare_turn_conclusion(
                    "goal-test",
                    &terminal_ticket,
                    &resolver,
                    "stale candidate".into(),
                    "new-fence".into(),
                );
                assert!(
                    matches!(rejected, Err(ref error) if error == "turn candidate source/attempt mismatch")
                );
            } else {
                let receipt = committed.unwrap();
                assert_eq!(
                    receipt.graph.node_statuses[&synth_id],
                    ExecutionNodeStatus::Completed
                );
                assert_eq!(
                    goals.get("goal-test").unwrap().unwrap().completion,
                    expected_completion
                );
            }
        }
    }

    #[test]
    fn live_program_owns_every_business_conclusion_not_only_success() {
        use harness_contract::agent_action::{
            AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, TeamCreateInput,
        };
        for completion in [
            GoalCompletion::Satisfied,
            GoalCompletion::Partial,
            GoalCompletion::Blocked,
            GoalCompletion::Failed,
            GoalCompletion::Cancelled,
        ] {
            let events = Arc::new(RuntimeEventStore::for_test());
            let actions = crate::AgentActionService::new(events.clone());
            let actor = AgentActorBinding {
                original_execution: None,
                objective_id: "objective-1".into(),
                program_id: "program-1".into(),
                session_id: "session-1".into(),
                turn_id: "turn-1".into(),
                root_execution_id: Some("root-execution-1".into()),
                required_team_count: 0,
                exact_team_count: None,
                objective_summary: "original Program business".into(),
                model_lease: "test".into(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: vec![],
                actor_id: "root-1".into(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            };
            actions
                .apply(&AgentActionEnvelope {
                    action_id: "create-original-team".into(),
                    actor: actor.clone(),
                    expected_revision: None,
                    action: AgentAction::TeamCreate(TeamCreateInput {
                        name: "original team".into(),
                        mission: "bounded work".into(),
                        objective: None,
                    }),
                })
                .unwrap();
            let original_program = actions.project("program-1").unwrap();
            assert_eq!(
                original_program.teams.len(),
                1,
                "the original producer must establish a real Program before testing refusal"
            );
            let goals = GoalStore::new(events.clone());
            let mut original = goal();
            original.id = "goal:root-execution-1".into();
            original.session_id = "session-1".into();
            original.criteria[0].status = AcceptanceStatus::Satisfied;
            original.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: actor.objective_id,
                session_id: actor.session_id,
                turn_id: actor.turn_id,
                root_execution_id: "root-execution-1".into(),
                agentic_program_id: actor.program_id,
            });
            goals.create(original).unwrap();
            let before = events
                .stream_revision("goal:goal:root-execution-1")
                .unwrap();
            let refused = goals.terminal_event(
                "goal:root-execution-1",
                completion,
                vec![],
                "Host proposal".into(),
                "forged-host-terminal".into(),
            );
            assert!(
                matches!(refused, Err(ref reason) if reason == "program_terminal_authority: use the atomic Program conclusion for this Goal")
            );
            assert_eq!(
                events
                    .stream_revision("goal:goal:root-execution-1")
                    .unwrap(),
                before
            );
            assert_eq!(
                goals
                    .get("goal:root-execution-1")
                    .unwrap()
                    .unwrap()
                    .completion,
                GoalCompletion::Open
            );
            assert_eq!(
                actions.project("program-1").unwrap().revision,
                original_program.revision
            );
        }
    }

    #[test]
    fn r1_recovery_quarantine_blocks_original_goal_mutation_and_preserves_healthy_goal() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let goals = GoalStore::new(Arc::clone(&events));
        goals.create(goal()).unwrap();
        let mut healthy = goal();
        healthy.id = "healthy-goal".into();
        healthy.session_id = "healthy-session".into();
        goals.create(healthy).unwrap();
        let before = goals.projection("goal-test").unwrap().unwrap();
        let head = events.stream_revision("goal:goal-test").unwrap();
        events.replace_session_recovery_exclusions(std::collections::BTreeSet::from([
            "session-test".into(),
        ]));
        let called = std::cell::Cell::new(false);
        let error = goals
            .revise("goal-test", 1, 2, "must remain quarantined", |goal| {
                called.set(true);
                goal.objective = "must not apply".into();
                Vec::new()
            })
            .unwrap_err();
        assert_eq!(error, "session_recovery_quarantined:session-test");
        assert!(!called.get());
        let error = goals.record_observation(observation(1)).unwrap_err();
        assert_eq!(error, "session_recovery_quarantined:session-test");
        assert_eq!(events.stream_revision("goal:goal-test").unwrap(), head);
        assert_eq!(goals.projection("goal-test").unwrap().unwrap(), before);
        let updated = goals
            .revise(
                "healthy-goal",
                1,
                2,
                "healthy partition remains live",
                |goal| {
                    goal.objective = "healthy revision applied".into();
                    Vec::new()
                },
            )
            .unwrap();
        assert_eq!(updated.0.objective, "healthy revision applied");
    }

    #[test]
    fn r1_event_replay_current_schema_recovers_and_rejects_legacy() {
        if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
            return;
        }
        let root = std::path::PathBuf::from(
            std::env::var_os("COWD_R1_06_FIXTURE_ROOT")
                .expect("explicit private plan fixture root"),
        )
        .join(format!("goal-current-{}", uuid::Uuid::new_v4()));
        assert!(root.is_absolute());
        assert!(root.starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel"));
        let events = Arc::new(RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(&root));
        events.bind_artifact_store(Arc::clone(&artifacts)).unwrap();
        let goals = GoalStore::new(Arc::clone(&events));
        goals.create(goal()).unwrap();
        let mut observed = observation(1);
        observed.summary = "durable observation body ".repeat(2048);
        observed.cost_delta.tool_calls = 3;
        goals.record_observation(observed.clone()).unwrap();
        let original = goals.projection("goal-test").unwrap().unwrap();
        let event = events
            .latest_for_stream_kind("goal:goal-test", "goal.observation")
            .unwrap()
            .unwrap();
        assert_eq!(event.schema_version, 2);
        assert!(event.payload.get("goal").is_none());
        let reference: harness_contract::context::ArtifactRef =
            serde_json::from_value(event.payload["observation_body"]["body"]["artifact"].clone())
                .unwrap();
        let bytes = artifacts
            .read_blocking(&reference, "session:session-test", None)
            .unwrap();
        assert_eq!(
            format!("sha256:{:x}", sha2::Sha256::digest(&bytes)),
            reference.sha256
        );
        assert_eq!(bytes.len() as u64, reference.bytes);
        assert_eq!(
            serde_json::from_slice::<RuntimeObservation>(&bytes).unwrap(),
            observed
        );
        drop(goals);
        let recovered = GoalStore::new(Arc::clone(&events));
        assert_eq!(
            recovered.projection("goal-test").unwrap().unwrap(),
            original
        );
        let checkpoint = events
            .projection_checkpoint("goal-projection:goal-test")
            .unwrap()
            .unwrap();
        events
            .inject_content_checkpoint_fault(
                "goal-projection:goal-test",
                checkpoint.source_cursor,
                checkpoint.revision,
                &serde_json::json!({"schema":999,"body":"not a current checkpoint"}),
            )
            .unwrap();
        assert_eq!(
            GoalStore::new(events.clone())
                .projection("goal-test")
                .unwrap()
                .unwrap(),
            original
        );
        let repaired = events
            .projection_checkpoint("goal-projection:goal-test")
            .unwrap()
            .unwrap();
        assert_eq!(repaired.payload["schema"], 2);
        assert_eq!(repaired.source_cursor, checkpoint.source_cursor);
        recovered.record_observation(observed.clone()).unwrap();
        assert_eq!(
            recovered.projection("goal-test").unwrap().unwrap(),
            original
        );
        let mut changed_retry = observed.clone();
        changed_retry.summary.push_str(" changed bytes");
        assert!(recovered.record_observation(changed_retry).is_err());
        assert_eq!(
            recovered.projection("goal-test").unwrap().unwrap(),
            original
        );
        let prepared = recovered
            .observation_event(
                &observed,
                "direct:".to_string() + &observed.idempotency_fingerprint(),
            )
            .unwrap();
        let head = events.stream_revision("goal:goal-test").unwrap();
        for schema in [1, 999] {
            let mut unsupported = prepared.clone();
            unsupported.schema_version = schema;
            assert!(events
                .append_batch_if_revision(
                    "goal:goal-test",
                    head,
                    format!("unsupported-{schema}"),
                    vec![unsupported]
                )
                .is_err());
            assert_eq!(events.stream_revision("goal:goal-test").unwrap(), head);
        }
        assert_eq!(
            recovered.projection("goal-test").unwrap().unwrap(),
            original
        );
    }

    #[test]
    fn proof_readers_bind_original_resources_and_projection_stays_optional() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let unbound = GoalStore::new(Arc::clone(&events));
        assert!(unbound.proof_artifacts.is_none());
        assert!(unbound.proof_session_queries.is_none());
        let temporary = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(temporary.path()));
        let queries = Arc::new(OnceLock::new());
        let bound = GoalStore::new(Arc::clone(&events))
            .with_artifact_store(Arc::clone(&artifacts))
            .with_session_query_slot(Arc::clone(&queries));
        let cloned = bound.clone();
        assert!(Arc::ptr_eq(
            cloned.proof_artifacts.as_ref().unwrap(),
            &artifacts
        ));
        assert!(Arc::ptr_eq(
            cloned.proof_session_queries.as_ref().unwrap(),
            &queries
        ));
        assert!(queries.get().is_none());
        unbound.create(goal()).unwrap();
        assert_eq!(
            bound.get("goal-test").unwrap(),
            unbound.get("goal-test").unwrap()
        );
    }

    #[test]
    fn goal_spec_digest_tracks_requirements_without_progress_state() {
        let mut original = goal();
        original
            .obligations
            .push(harness_contract::goal::ObjectiveObligation {
                obligation_id: "delivery".into(),
                required: true,
                success_predicate: "verified source".into(),
                producer: Default::default(),
                evidence_requirement: Default::default(),
                state: ObjectiveObligationState::Open,
                artifact_refs: vec![],
                evidence_refs: vec![],
                reread_receipts: vec![],
                verifier_decision: None,
                diagnostic_code: None,
            });
        let digest = goal_spec_digest(&original);
        let mut progress = original.clone();
        progress.criteria[0].status = AcceptanceStatus::Satisfied;
        progress.obligations[0].state = ObjectiveObligationState::Satisfied;
        progress.obligations[0]
            .evidence_refs
            .push("actual-evidence".into());
        progress.obligations[0].verifier_decision = Some("review".into());
        progress.revision += 1;
        assert_eq!(goal_spec_digest(&progress), digest);
        progress.obligations[0].success_predicate = "different user requirement".into();
        assert_ne!(goal_spec_digest(&progress), digest);
        let mut changed = original.clone();
        changed.obligations[0].required = false;
        assert_ne!(goal_spec_digest(&changed), digest);
        changed = original.clone();
        changed.obligations[0].evidence_requirement.reread_required = true;
        assert_ne!(goal_spec_digest(&changed), digest);
        changed = original;
        changed.obligations[0].producer.capability_id = Some("write".into());
        assert_ne!(goal_spec_digest(&changed), digest);
    }

    fn observation(revision: u64) -> RuntimeObservation {
        RuntimeObservation {
            identity: RuntimeObservationIdentity {
                workspace_id: "workspace".to_string(),
                session_id: "session-test".to_string(),
                turn_id: Some("turn".to_string()),
                task_id: None,
                graph_id: "graph".to_string(),
                goal_id: "goal-test".to_string(),
                node_id: Some(format!("node-{revision}")),
            },
            kind: RuntimeObservationKind::ToolProgress,
            source: "test.tool".to_string(),
            source_revision: revision,
            freshness: ObservationFreshness {
                observed_at_ms: revision,
                valid_until_ms: None,
                policy_revision: "goal-observation-v2".to_string(),
            },
            summary: "summary is retained for people, not reduced".to_string(),
            fingerprint: format!("tool-{revision}"),
            evidence_refs: vec![format!("receipt:{revision}")],
            observed_evidence: Vec::new(),
            criterion_deltas: Vec::new(),
            evidence_delta: EvidenceDelta::default(),
            effect_deltas: Vec::new(),
            conflict_deltas: Vec::new(),
            unknown_deltas: Vec::new(),
            cost_delta: CostDelta::default(),
            information_gain: InformationGain::default(),
            context_delta: ContextDelta::default(),
            parallelism_delta: ParallelismDelta::default(),
            result_class: ObservationResultClass::Succeeded,
            failure_class: None,
        }
    }

    fn terminal_for_test() -> ObjectiveTerminal {
        ObjectiveTerminal {
            kind: ObjectiveTerminalKind::Satisfied,
            terminal_fence: "terminal-test".into(),
            authority_revision: 1,
            reason: "verified result".into(),
            evidence_refs: vec![],
            diagnostics: vec![],
            committed_at_ms: 1,
        }
    }

    #[test]
    fn all_public_terminal_writers_reject_open_obligations_uncertain_effects_and_empty_evidence() {
        for writer in ["terminal_event", "complete", "complete_objective"] {
            for defect in ["obligation", "effect", "empty", "participation"] {
                let events = Arc::new(RuntimeEventStore::for_test());
                let store = GoalStore::new(Arc::clone(&events));
                let mut goal = goal();
                goal.scope = harness_contract::goal::GoalScope::UserObjective;
                goal.criteria[0].status = AcceptanceStatus::Satisfied;
                goal.execution_binding = Some(harness_contract::goal::GoalExecutionBinding {
                    objective_id: "user-objective".into(),
                    session_id: goal.session_id.clone(),
                    turn_id: "turn".into(),
                    root_execution_id: "standalone-root".into(),
                    agentic_program_id: "not-created-program".into(),
                });
                if defect == "participation" {
                    goal.participation_requirement =
                        Some(harness_contract::goal::ParticipationRequirement {
                            minimum_team_count: 2,
                            source_ref: "session_message:original".into(),
                        });
                }
                if defect == "obligation" {
                    goal.obligations
                        .push(harness_contract::goal::ObjectiveObligation {
                            obligation_id: "required-user-work".into(),
                            required: true,
                            success_predicate: "verify the result".into(),
                            producer: Default::default(),
                            evidence_requirement: Default::default(),
                            state: ObjectiveObligationState::Open,
                            artifact_refs: vec![],
                            evidence_refs: vec![],
                            reread_receipts: vec![],
                            verifier_decision: None,
                            diagnostic_code: None,
                        });
                }
                store.create(goal).unwrap();
                if defect != "empty" {
                    let mut observed = observation(1);
                    observed.evidence_delta.added = vec!["receipt:actual".into()];
                    if defect == "effect" {
                        observed
                            .effect_deltas
                            .push(harness_contract::goal::EffectDelta {
                                effect_id: "external-effect".into(),
                                terminal_class:
                                    harness_contract::goal::EffectTerminalClass::Uncertain,
                                idempotency_ref: "effect-request".into(),
                            });
                    }
                    store.record_observation(observed).unwrap();
                }
                let before = events.stream_revision("goal:goal-test").unwrap();
                let result = match writer {
                    "terminal_event" => store
                        .terminal_event(
                            "goal-test",
                            GoalCompletion::Satisfied,
                            vec![],
                            "done".into(),
                            "terminal-test".into(),
                        )
                        .map(|_| ()),
                    "complete" => store
                        .complete("goal-test", 1, GoalCompletion::Satisfied, "done")
                        .map(|_| ()),
                    _ => store
                        .complete_objective("goal-test", 1, terminal_for_test())
                        .map(|_| ()),
                };
                let error = result.expect_err("writer must not bypass unmet acceptance");
                assert!(error.contains(defect), "{writer}/{defect}: {error}");
                assert_eq!(events.stream_revision("goal:goal-test").unwrap(), before);
                assert_eq!(
                    store.get("goal-test").unwrap().unwrap().completion,
                    GoalCompletion::Open
                );
            }
        }
    }

    #[test]
    fn legacy_complete_cannot_emit_a_second_terminal_or_be_replaced_by_an_objective_fence() {
        let events = Arc::new(RuntimeEventStore::for_test());
        let store = GoalStore::new(Arc::clone(&events));
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        store.create(goal).unwrap();
        let completed = store
            .complete("goal-test", 1, GoalCompletion::Satisfied, "done")
            .unwrap();
        let before = events.stream_revision("goal:goal-test").unwrap();
        assert!(store
            .complete(
                "goal-test",
                completed.revision,
                GoalCompletion::Satisfied,
                "again"
            )
            .is_err());
        assert!(store
            .complete_objective("goal-test", completed.revision, terminal_for_test())
            .is_err());
        assert_eq!(events.stream_revision("goal:goal-test").unwrap(), before);
    }

    #[test]
    fn objective_terminal_fence_replays_same_result_before_revision_check_and_rejects_changed_evidence(
    ) {
        let events = Arc::new(RuntimeEventStore::for_test());
        let store = GoalStore::new(Arc::clone(&events));
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        store.create(goal).unwrap();
        let completed = store
            .complete_objective("goal-test", 1, terminal_for_test())
            .unwrap();
        let before = events.stream_revision("goal:goal-test").unwrap();
        let mut retry = terminal_for_test();
        retry.committed_at_ms = 2;
        assert_eq!(
            store
                .complete_objective("goal-test", 1, retry.clone())
                .unwrap(),
            completed
        );
        retry.evidence_refs.push("different-evidence".into());
        assert!(store
            .complete_objective("goal-test", 1, retry)
            .unwrap_err()
            .contains("fence_conflict"));
        assert_eq!(events.stream_revision("goal:goal-test").unwrap(), before);
    }

    #[test]
    fn goal_revision_is_monotonic_and_completion_requires_acceptance() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
        store.create(goal()).unwrap();
        assert!(store
            .complete("goal-test", 1, GoalCompletion::Satisfied, "done")
            .is_err());
        let (revised, revision) = store
            .revise("goal-test", 1, 2, "evidence checked", |goal| {
                goal.criteria[0].status = AcceptanceStatus::Satisfied;
                vec!["criteria.checked".to_string()]
            })
            .unwrap();
        assert_eq!(revision.revision, 2);
        let completed = store
            .complete(
                &revised.id,
                2,
                GoalCompletion::Satisfied,
                "all criteria satisfied",
            )
            .unwrap();
        assert_eq!(completed.completion, GoalCompletion::Satisfied);
        assert_eq!(store.get("goal-test").unwrap(), Some(completed));
    }

    #[test]
    fn revision_event_carries_snapshot_and_revision_for_graph_atomic_commit() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
        store.create(goal()).unwrap();

        let (revised, revision, event) = store
            .revision_event("goal-test", 1, 2, "user correction", |goal| {
                goal.constraints.push("latest user correction".to_string());
                vec!["constraints".to_string()]
            })
            .unwrap();

        assert_eq!(revised.revision, 2);
        assert_eq!(event.event.kind, "goal.revised");
        assert_eq!(
            serde_json::from_value::<GoalRevision>(event.event.payload["revision"].clone())
                .unwrap(),
            revision
        );
        assert_eq!(
            serde_json::from_value::<GoalContract>(event.event.payload["goal"].clone()).unwrap(),
            revised
        );
    }

    #[test]
    fn typed_observation_reducer_is_idempotent_and_completion_uses_its_evidence() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
        store.create(goal()).unwrap();
        let mut observation = observation(1);
        observation.criterion_deltas.push(CriterionDelta {
            criterion_id: "checked".to_string(),
            previous: AcceptanceStatus::Open,
            current: AcceptanceStatus::Satisfied,
            evidence_refs: vec!["evidence:checked".to_string()],
        });
        observation.evidence_delta.added = vec!["evidence:checked".to_string()];
        observation.information_gain = InformationGain {
            distinguishing_evidence_refs: vec!["evidence:checked".to_string()],
            resolved_unknown_refs: Vec::new(),
            provenance: MeasureProvenance::Observed,
        };

        store.record_observation(observation.clone()).unwrap();
        store.record_observation(observation).unwrap();
        let projection = store.projection("goal-test").unwrap().unwrap();
        assert_eq!(projection.progress.observation_count, 1);
        assert_eq!(
            projection.progress.criteria["checked"],
            AcceptanceStatus::Satisfied
        );
        assert_eq!(
            projection.progress.evidence_refs,
            vec!["evidence:checked".to_string()]
        );

        let completed = store
            .complete("goal-test", 1, GoalCompletion::Satisfied, "typed evidence")
            .unwrap();
        assert_eq!(completed.completion, GoalCompletion::Satisfied);
        assert_eq!(
            completed.evidence_refs,
            vec!["evidence:checked".to_string()]
        );
    }

    #[test]
    fn unrelated_goal_revision_preserves_reduced_progress_and_unknown_resolution() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
        store.create(goal()).unwrap();
        let mut opened = observation(1);
        opened.unknown_deltas.push(UnknownDelta {
            unknown_id: "pending-check".to_string(),
            change: ResolutionDeltaKind::Opened,
            evidence_refs: Vec::new(),
        });
        store.record_observation(opened).unwrap();
        let mut resolved = observation(2);
        resolved.unknown_deltas.push(UnknownDelta {
            unknown_id: "pending-check".to_string(),
            change: ResolutionDeltaKind::Resolved,
            evidence_refs: vec!["evidence:checked".to_string()],
        });
        resolved.evidence_delta.added = vec!["evidence:checked".to_string()];
        store.record_observation(resolved).unwrap();
        store
            .revise("goal-test", 1, 2, "new user constraint", |goal| {
                goal.constraints.push("keep evidence".to_string());
                vec!["constraints".to_string()]
            })
            .unwrap();

        let projection = store.projection("goal-test").unwrap().unwrap();
        assert!(projection.progress.open_unknowns.is_empty());
        assert_eq!(
            projection.progress.evidence_refs,
            vec!["evidence:checked".to_string()]
        );
    }

    #[test]
    fn intervention_projection_retains_full_trigger_identity() {
        let store = GoalStore::new(Arc::new(RuntimeEventStore::for_test()));
        store.create(goal()).unwrap();
        let trigger = observation(1);
        store.record_observation(trigger.clone()).unwrap();
        store
            .record_intervention(
                RuntimeIntervention {
                    goal_id: "goal-test".to_string(),
                    kind: harness_contract::goal::RuntimeInterventionKind::Replan,
                    reason: "typed trigger".to_string(),
                    evidence_refs: Vec::new(),
                    expected_graph_revision: None,
                },
                std::slice::from_ref(&trigger),
            )
            .unwrap();
        store
            .record_intervention(
                RuntimeIntervention {
                    goal_id: "goal-test".to_string(),
                    kind: harness_contract::goal::RuntimeInterventionKind::Replan,
                    reason: "typed trigger".to_string(),
                    evidence_refs: Vec::new(),
                    expected_graph_revision: None,
                },
                std::slice::from_ref(&trigger),
            )
            .unwrap();

        let projection = store.projection("goal-test").unwrap().unwrap();
        assert_eq!(projection.interventions.len(), 1);
        assert_eq!(projection.intervention_traces.len(), 1);
        assert_eq!(projection.intervention_traces[0].identity, trigger.identity);
        assert_eq!(
            projection.intervention_traces[0].trigger_observation_keys,
            vec![trigger.idempotency_fingerprint()]
        );
    }

    #[test]
    fn over_qualified_criterion_ref_resolves_to_the_plain_id() {
        use harness_contract::agent_action::{AgentAction, ObjectiveReviewDecision, ObjectiveReviewInput};
        let action = AgentAction::ObjectiveReview(ObjectiveReviewInput {
            criterion_ref: "criterion:goal:goal:over-qualified:checked".into(),
            decision: ObjectiveReviewDecision::Gap,
            result_refs: vec![], evidence_refs: vec![], reason_ref: "evidence".into(),
            reason: "reviewed result does not yet meet the criterion".into(),
        });
        let AgentAction::ObjectiveReview(normalized) = GoalStore::normalize_criterion_ref(&action, &goal()) else {
            panic!("review action changed kind");
        };
        assert_eq!(normalized.criterion_ref, "checked");
        let AgentAction::ObjectiveReview(mut unknown) = action else { unreachable!() };
        unknown.criterion_ref = "criterion:goal:unknown".into();
        let AgentAction::ObjectiveReview(normalized) = GoalStore::normalize_criterion_ref(&AgentAction::ObjectiveReview(unknown), &goal()) else {
            unreachable!();
        };
        assert_eq!(normalized.criterion_ref, "criterion:goal:unknown");
    }

    /// A satisfied review retires the gap it was sent to close. This is the
    /// Goal-side half of the v14 deadlock: the criterion and the review were
    /// both satisfied, yet `waiting` still described a `completion_gap` that no
    /// longer existed.
    #[test]
    fn a_satisfied_review_retires_the_completion_gap() {
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        assert!(super::satisfied_review_retires_the_waiting_gap(
            &goal,
            "satisfied"
        ));
    }

    /// A gap or blocked decision leaves the descriptor alone: the gap it names
    /// is real, and clearing it would let an unfinished objective look ready.
    #[test]
    fn a_gap_or_blocked_review_keeps_the_waiting_descriptor() {
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        assert!(!super::satisfied_review_retires_the_waiting_gap(
            &goal, "gap"
        ));
        assert!(!super::satisfied_review_retires_the_waiting_gap(
            &goal, "blocked"
        ));
    }

    /// Another criterion still open means the objective is genuinely unfinished,
    /// whatever this one review said.
    #[test]
    fn another_open_criterion_keeps_the_waiting_descriptor() {
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        goal.criteria.push(AcceptanceCriterion {
            id: "second".to_string(),
            statement: "still open".to_string(),
            statement_ref: None,
            source_refs: Vec::new(),
            required_evidence: Vec::new(),
            status: AcceptanceStatus::Open,
            waiver: None,
        });
        assert!(!super::satisfied_review_retires_the_waiting_gap(
            &goal,
            "satisfied"
        ));
    }

    /// A blocker is a fact the model cannot review away.
    #[test]
    fn a_blocker_keeps_the_waiting_descriptor() {
        let mut goal = goal();
        goal.criteria[0].status = AcceptanceStatus::Satisfied;
        goal.blockers
            .push("operator permission pending".to_string());
        assert!(!super::satisfied_review_retires_the_waiting_gap(
            &goal,
            "satisfied"
        ));
    }
}
