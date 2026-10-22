//! Fresh side effects use the original graph and Program claim owners.
//! The returned revisions join the effect intent transaction; no ToolHost
//! operation runs while these stream locks are held.
use super::*;
use crate::{AgenticProgramStatus, AgenticTaskStatus};
use harness_contract::agent::AgenticExecutionFocus;
use harness_contract::agent_action::AgentAttemptMode;

impl ExecutionCommitService {
    pub fn validate_system_tool_parent(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<(), ExecutionCommitError> {
        self.validate_system_tool_parent_scoped(request, None)
    }

    pub(super) fn validate_system_tool_parent_scoped(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        root: Option<&crate::CowdExecutionContext>,
    ) -> Result<(), ExecutionCommitError> {
        let Some(binding) = request.step_binding.as_ref().filter(|binding| {
            matches!(
                binding.origin.source,
                harness_contract::step::CallSource::SystemDerived { .. }
            )
        }) else {
            return Ok(());
        };
        let denied =
            || ExecutionCommitError::InvalidCommand("system_tool_parent_fence_rejected".into());
        let execution_id = request
            .parent_execution
            .as_ref()
            .map(|parent| parent.execution_id.as_str())
            .or_else(|| root.map(|root| root.execution_id.as_str()))
            .ok_or_else(denied)?;
        if request.session_id.as_deref() != Some(binding.snapshot.identity.session_id.as_str()) {
            return Err(denied());
        }
        let graph = self
            .effect_authority_graphs
            .load(execution_id)
            .map_err(|_| denied())?;
        let lineage = graph.lineage.as_ref().ok_or_else(denied)?;
        self.event_store
            .ensure_session_recovery_admitted(&lineage.session_id)
            .map_err(|_| denied())?;
        if lineage.session_id != binding.snapshot.identity.session_id
            || lineage.turn_id != binding.snapshot.identity.turn_id
        {
            return Err(denied());
        }
        let source_id = binding
            .snapshot
            .identity
            .execution_id
            .as_deref()
            .ok_or_else(denied)?;
        if source_id != execution_id {
            let source = self
                .effect_authority_graphs
                .load(source_id)
                .map_err(|_| denied())?;
            if source.parent_execution.as_ref() != request.parent_execution.as_ref()
                || request.parent_execution.is_none()
                || source.lineage.as_ref().is_none_or(|source_lineage| {
                    source_lineage.session_id != lineage.session_id
                        || source_lineage.turn_id != lineage.turn_id
                })
                || !source.nodes.iter().any(|node| {
                    source.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Running)
                })
            {
                return Err(denied());
            }
        }
        if let Some(parent) = &request.parent_execution {
            if graph.node_statuses.get(&parent.node_id) != Some(&ExecutionNodeStatus::Running) {
                return Err(denied());
            }
        } else if root.is_none_or(|root| {
            root.session_id != lineage.session_id || root.turn_id != lineage.turn_id
        }) {
            return Err(denied());
        }
        if !graph
            .nodes
            .iter()
            .any(|node| graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Running))
        {
            return Err(denied());
        }
        Ok(())
    }

    pub(super) fn tool_effect_authority(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        self.tool_effect_authority_observed(request, None)
    }

    pub(super) fn tool_effect_authority_observed(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        observation: Option<&super::super::executors::agent_tool::NoDispatchObservation>,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        self.tool_effect_authority_with_refusal(request, &|message| {
            if let Some(observation) = observation {
                observation.refuse(&request.tool_use_id, message.to_string());
            }
        })
    }

    pub(super) fn tool_effect_authority_with_refusal(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
        refused: &dyn Fn(&str),
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        let Some(parent) = &request.parent_execution else {
            return Ok(Vec::new());
        };
        let denied = |reason: &str| {
            let message = format!("tool_effect_authority: {reason}");
            refused(&message);
            ExecutionCommitError::InvalidCommand(message)
        };
        let graph = self
            .effect_authority_graphs
            .load(&parent.execution_id)
            .map_err(|error| {
                ExecutionCommitError::InvalidCommand(format!(
                    "tool_effect_authority source unavailable: {error}"
                ))
            })?;
        let lineage = graph
            .lineage
            .as_ref()
            .ok_or_else(|| denied("parent lineage missing"))?;
        self.event_store
            .ensure_session_recovery_admitted(&lineage.session_id)
            .map_err(|error| {
                ExecutionCommitError::InvalidCommand(format!(
                    "tool_effect_authority source unavailable: {error}"
                ))
            })?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == parent.node_id)
            .ok_or_else(|| denied("parent node is missing"))?;
        if graph.node_statuses.get(&node.id) != Some(&ExecutionNodeStatus::Running) {
            return Err(denied("parent node is not running"));
        }
        let mut sources = vec![ExpectedStreamRevision {
            stream_id: graph.id.clone(),
            expected_revision: graph.revision,
        }];
        if node.kind != ExecutionNodeKind::AgentTask {
            if graph.parent_execution.is_none() {
                let lineage = graph
                    .lineage
                    .as_ref()
                    .ok_or_else(|| denied("Root graph has no lineage"))?;
                if request.session_id.as_deref() != Some(lineage.session_id.as_str()) {
                    return Err(denied("Root invocation Session mismatch"));
                }
                return self.root_effect_authority_with_refusal(
                    &crate::CowdExecutionContext {
                        execution_id: graph.id.clone(),
                        session_id: lineage.session_id.clone(),
                        turn_id: lineage.turn_id.clone(),
                    },
                    refused,
                );
            }
            return Ok(sources);
        }
        let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)?;
        let binding = packet
            .binding
            .as_ref()
            .ok_or_else(|| denied("Agent binding is missing"))?;
        if packet.graph_id() != graph.id
            || packet.node_id() != node.id
            || request.parent_execution_attempt != Some(packet.attempt)
            || request.session_id.as_deref() != Some(packet.session_id())
            || !packet.allowed_tools.contains(&request.tool_name)
            || !binding.tool_contract_refs.contains(&request.tool_name)
            || !crate::agent::binding::recompute_binding_digest(binding)
                .is_ok_and(|digest| digest == binding.binding_digest)
        {
            return Err(denied(
                "immutable execution grant does not match the invocation",
            ));
        }
        let Some(scope) = &packet.agentic_binding else {
            return Ok(sources);
        };
        let program = self
            .effect_authority_actions
            .project(&scope.program_id)
            .map_err(|error| {
                ExecutionCommitError::InvalidCommand(format!(
                    "tool_effect_authority source unavailable: {error}"
                ))
            })?;
        if !matches!(
            program.status,
            AgenticProgramStatus::Open | AgenticProgramStatus::Waiting
        ) || program.session_id != packet.session_id()
            || !graph.lineage.as_ref().is_some_and(|lineage| {
                lineage.session_id == program.session_id && lineage.turn_id == program.turn_id
            })
            || !program.agent_is_active_in(&scope.agent_id, &scope.team_id)
            || !program
                .membership_for(&scope.agent_id, &scope.team_id)
                .is_some_and(|member| member.membership_id == scope.membership_id)
        {
            return Err(denied("Program or membership no longer authorizes writes"));
        }
        if registered_collaboration_control_allows(
            &program, scope, packet.graph_id(), packet.attempt,
            &request.tool_name, &request.input,
        ) {
            sources.push(ExpectedStreamRevision {
                stream_id: format!("agentic-program:{}", program.program_id),
                expected_revision: program.revision,
            });
            return Ok(sources);
        }
        let task_ref = match &scope.focus {
            AgenticExecutionFocus::TaskExecute { task_ref }
            | AgenticExecutionFocus::TaskReview { task_ref } => task_ref,
            _ => return Err(denied("ordinary writes require an active Task execution claim")),
        };
        let task = program
            .tasks
            .get(task_ref)
            .ok_or_else(|| denied("Task is missing"))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if task.team_id == scope.task_team_id
            && task.cancel_requested_by.is_none()
            && task.pending_retirement.is_none()
            && task.active_attempts.get(packet.graph_id()).is_some_and(|attempt| {
                attempt.execution_id == packet.graph_id()
                    && attempt.agent_id == scope.agent_id
                    && attempt.membership_id == scope.membership_id
                    && attempt.generation.checked_add(1) == Some(u64::from(packet.attempt))
                    && task_control_phase_allows(task, scope, attempt, &request.tool_name, &request.input)
            })
        {
            sources.push(ExpectedStreamRevision {
                stream_id: format!("agentic-program:{}", program.program_id),
                expected_revision: program.revision,
            });
            return Ok(sources);
        }
        if !matches!(scope.focus, AgenticExecutionFocus::TaskExecute { .. }) {
            return Err(denied("ordinary writes require an active Task execution claim"));
        }
        if task.status != AgenticTaskStatus::Claimed
            || task.team_id != scope.task_team_id
            || task.claimant.as_deref() != Some(scope.agent_id.as_str())
            || task.claim_execution_id.as_deref() != Some(packet.graph_id())
            || task.claim_generation != u64::from(packet.attempt)
            || task
                .lease_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
            || task.cancel_requested_by.is_some()
            || task.pending_retirement.is_some()
            || !task
                .active_attempts
                .get(packet.graph_id())
                .is_some_and(|attempt| {
                    attempt.execution_id == packet.graph_id()
                        && attempt.agent_id == scope.agent_id
                        && attempt.membership_id == scope.membership_id
                        && attempt.mode == AgentAttemptMode::Execute
                        && attempt.generation.checked_add(1) == Some(u64::from(packet.attempt))
                })
        {
            return Err(denied(
                "Task claim, generation, lease or active attempt is stale",
            ));
        }
        sources.push(ExpectedStreamRevision {
            stream_id: format!("agentic-program:{}", program.program_id),
            expected_revision: program.revision,
        });
        Ok(sources)
    }
}


fn agent_control_input(input: &str) -> Option<serde_json::Value> {
    let mut value = serde_json::from_str::<serde_json::Value>(input).ok()?;
    let object = value.as_object_mut()?;
    if let Some(revision) = object.remove("expected_revision") {
        if !revision.is_null() && revision.as_u64().is_none() {
            return None;
        }
    }
    Some(value)
}

fn registered_collaboration_control_allows(
    program: &crate::agentic::AgenticProgramProjection,
    scope: &harness_contract::agent::AgenticExecutionBinding,
    execution_id: &str,
    packet_attempt: u32,
    tool_name: &str,
    input: &str,
) -> bool {
    use harness_contract::agent_action::AgentAction;
    let Some(input) = agent_control_input(input) else {
        return false;
    };
    let Ok(action) = serde_json::from_value::<AgentAction>(
        serde_json::json!({"kind": tool_name, "input": input}),
    ) else {
        return false;
    };
    let (task_ref, mode, generation) = match &scope.focus {
        AgenticExecutionFocus::TaskExecute { task_ref } => {
            let Some(task) = program.tasks.get(task_ref) else { return false };
            let Some(attempt) = task.active_attempts.get(execution_id) else { return false };
            let preclaim = matches!(task.status, AgenticTaskStatus::Published | AgenticTaskStatus::Rework)
                && task.claimant.is_none() && task.claim_execution_id.is_none()
                && task.claim_generation == attempt.generation;
            let release = matches!(&action, AgentAction::TaskRelease(input) if input.task_ref == *task_ref)
                && matches!(task.status, AgenticTaskStatus::Claimed | AgenticTaskStatus::CancelRequested)
                && task.claimant.as_deref() == Some(scope.agent_id.as_str())
                && task.claim_execution_id.as_deref() == Some(execution_id)
                && task.claim_generation == u64::from(packet_attempt);
            if !preclaim && !release {
                return false;
            }
            (task_ref.as_str(), AgentAttemptMode::Execute, attempt.generation)
        }
        AgenticExecutionFocus::TaskReview { task_ref } => {
            let Some(task) = program.tasks.get(task_ref) else { return false };
            if task.status != AgenticTaskStatus::Submitted
                || task.claimant.as_deref().is_none_or(|producer| producer == scope.agent_id)
            {
                return false;
            }
            (task_ref.as_str(), AgentAttemptMode::Review, task.review_generation)
        }
        AgenticExecutionFocus::Coordination { wake_ref } => {
            let Some((_, wake)) = program.coordination_wake_ref(wake_ref) else { return false };
            let Some(consumption) = wake.coordination.as_ref() else { return false };
            let Some(intent) = wake.intent.as_ref() else { return false };
            if consumption.settled || consumption.execution_id != execution_id
                || consumption.agent_id != scope.agent_id
                || consumption.membership_id != scope.membership_id
                || !program.coordination_request_current(wake)
                || program.coordination_team_id_for(&scope.agent_id, wake.revision)
                    != Some(scope.team_id.as_str())
            {
                return false;
            }
            (intent.task_ref.as_str(), AgentAttemptMode::Coordination, wake.revision)
        }
        AgenticExecutionFocus::ObjectiveReview { .. } => return false,
    };
    let Some(task) = program.tasks.get(task_ref) else { return false };
    let releasing = matches!(&action, AgentAction::TaskRelease(input) if input.task_ref == task_ref)
        && mode == AgentAttemptMode::Execute
        && matches!(task.status, AgenticTaskStatus::Claimed | AgenticTaskStatus::CancelRequested);
    if task.team_id != scope.task_team_id
        || (!releasing && (task.cancel_requested_by.is_some() || task.pending_retirement.is_some()))
        || !task.active_attempts.get(execution_id).is_some_and(|attempt| {
            attempt.execution_id == execution_id && attempt.agent_id == scope.agent_id
                && attempt.membership_id == scope.membership_id && attempt.mode == mode
                && attempt.generation == generation
                && attempt.generation.checked_add(1) == Some(u64::from(packet_attempt))
        })
    {
        return false;
    }
    match action {
        AgentAction::TeamCreate(_) | AgentAction::ArtifactCommit(_) => true,
        AgentAction::AgentInvite(input) => input.team_ref == scope.team_id,
        AgentAction::TaskPublish(input) => input.team_ref == scope.team_id,
        AgentAction::MembershipUpdate(input) => {
            input.agent_ref == scope.agent_id && input.team_ref == scope.team_id
        }
        AgentAction::TaskWithdraw(input) => {
            input.task_ref == task_ref && task.team_id == scope.team_id
        }
        AgentAction::MessagePublish(input) => {
            input.intent.as_ref().is_none_or(|intent| intent.task_ref == task_ref)
        }
        AgentAction::TaskRelease(input) => {
            input.task_ref == task_ref && (releasing
                || (mode == AgentAttemptMode::Coordination
                    && matches!(&scope.focus, AgenticExecutionFocus::Coordination { wake_ref }
                        if program.coordination_replied(wake_ref, execution_id, &scope.agent_id))))
        }
        _ => false,
    }
}

fn task_control_phase_allows(
    task: &crate::agentic::AgenticTaskProjection,
    scope: &harness_contract::agent::AgenticExecutionBinding,
    attempt: &crate::agentic::AgenticTaskAttemptProjection,
    tool_name: &str,
    input: &str,
) -> bool {
    use harness_contract::agent_action::{TaskClaimInput, TaskReviewInput};
    let Some(input) = agent_control_input(input) else { return false };
    match (&scope.focus, attempt.mode, tool_name) {
        (AgenticExecutionFocus::TaskExecute { task_ref }, AgentAttemptMode::Execute, "task_claim") => {
            task.task_id == *task_ref
                && matches!(task.status, AgenticTaskStatus::Published | AgenticTaskStatus::Rework)
                && task.claim_generation == attempt.generation
                && task.claimant.is_none()
                && task.claim_execution_id.is_none()
                && serde_json::from_value::<TaskClaimInput>(input)
                    .is_ok_and(|action| action.task_ref == *task_ref)
        }
        (AgenticExecutionFocus::TaskReview { task_ref }, AgentAttemptMode::Review, "task_review") => {
            task.task_id == *task_ref
                && task.status == AgenticTaskStatus::Submitted
                && task.review_generation == attempt.generation
                && task.claimant.as_deref().is_some_and(|claimant| claimant != scope.agent_id.as_str())
                && serde_json::from_value::<TaskReviewInput>(input)
                    .is_ok_and(|action| action.task_ref == *task_ref)
        }
        _ => false,
    }
}

#[cfg(test)]
mod task_control_700_tests {
    use super::*;
    use harness_contract::agent::AgenticExecutionBinding;
    use crate::agentic::{AgenticTaskAttemptProjection, AgenticTaskProjection};

    fn fixture(review: bool) -> (AgenticTaskProjection, AgenticExecutionBinding, AgenticTaskAttemptProjection) {
        let task = serde_json::from_value(serde_json::json!({
            "task_id":"task", "team_id":"team", "title":"task", "objective":"goal",
            "acceptance":"evidence", "required_capabilities":[], "depends_on":[],
            "status":if review { "submitted" } else { "published" },
            "claimant":if review { Some("producer") } else { None },
            "claim_generation":4, "claim_execution_id":null, "claimed_at_ms":null,
            "lease_expires_at_ms":null, "artifact_refs":[], "evidence_refs":[],
            "unresolved":[], "review_reason":null, "reviewed_by":null,
            "failed_attempts":0, "review_generation":4, "failed_review_attempts":0,
            "last_failure":null
        })).unwrap();
        let scope = AgenticExecutionBinding {
            program_id:"program".into(), agent_id:"worker".into(), membership_id:"membership".into(),
            team_id:"team".into(), task_team_id:"team".into(), source_spec_revision:1,
            task_purpose:None,
            focus:if review { AgenticExecutionFocus::TaskReview { task_ref:"task".into() } }
                else { AgenticExecutionFocus::TaskExecute { task_ref:"task".into() } },
        };
        let attempt = AgenticTaskAttemptProjection {
            execution_id:"execution".into(), agent_id:"worker".into(), membership_id:"membership".into(),
            mode:if review { AgentAttemptMode::Review } else { AgentAttemptMode::Execute }, generation:4,
        };
        (task, scope, attempt)
    }

    #[test]
    fn r1_700_preclaim_control_does_not_authorize_ordinary_writes_or_foreign_tasks() {
        let (mut task, scope, mut attempt) = fixture(false);
        let input = r#"{"task_ref":"task","reason":"ready"}"#;
        assert!(task_control_phase_allows(&task, &scope, &attempt, "task_claim", input));
        task.status = AgenticTaskStatus::Rework;
        assert!(task_control_phase_allows(&task, &scope, &attempt, "task_claim", input));
        for tool in ["edit", "execute_code", "task_submit", "task_review", "artifact_commit"] {
            assert!(!task_control_phase_allows(&task, &scope, &attempt, tool, input), "{tool}");
        }
        for input in [r#"{"task_ref":"foreign"}"#, "{}", "not-json"] {
            assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_claim", input));
        }
        attempt.generation += 1;
        assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_claim", input));
        attempt.generation -= 1;
        task.claimant = Some("worker".into());
        assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_claim", input));
        task.claimant = None;
        task.claim_execution_id = Some("foreign".into());
        assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_claim", input));
    }

    #[test]
    fn r1_700_independent_review_control_keeps_all_semantic_decisions_and_rejects_self_review() {
        let (mut task, mut scope, mut attempt) = fixture(true);
        for decision in ["accept", "rework", "challenge"] {
            let input = serde_json::json!({"task_ref":"task", "decision":decision, "reason":"observed evidence"}).to_string();
            assert!(task_control_phase_allows(&task, &scope, &attempt, "task_review", &input));
            assert!(!task_control_phase_allows(&task, &scope, &attempt, "edit", &input));
        }
        let input = r#"{"task_ref":"task","decision":"accept","reason":"evidence"}"#;
        task.claimant = Some(scope.agent_id.clone());
        assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_review", input));
        task.claimant = Some("producer".into());
        attempt.generation += 1;
        assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_review", input));
        attempt.generation -= 1;
        scope.focus = AgenticExecutionFocus::TaskExecute { task_ref:"task".into() };
        assert!(!task_control_phase_allows(&task, &scope, &attempt, "task_review", input));
    }

    #[tokio::test]
    async fn r1_711_preclaim_and_review_collaboration_do_not_grant_ordinary_writes() {
        let existing = crate::agentic::coordination::tests::fixture(false).await;
        let mut program = existing.services.agent_action_service().project("coord-program").unwrap();
        for review in [false, true] {
            let (mut task, scope, attempt) = fixture(review);
            task.active_attempts.insert("execution".into(), attempt);
            program.tasks.insert(task.task_id.clone(), task);
            let input = r#"{"topic_ref":"topic:team","summary":"Scope needs clarification"}"#;
            assert!(registered_collaboration_control_allows(
                &program, &scope, "execution", 5, "message_publish", input,
            ));
            for tool in ["edit", "execute_code", "task_submit", "task_release", "objective_complete_request"] {
                assert!(!registered_collaboration_control_allows(
                    &program, &scope, "execution", 5, tool, r#"{"task_ref":"task"}"#,
                ), "{review}/{tool}");
            }
            assert!(!registered_collaboration_control_allows(
                &program, &scope, "execution", 6, "message_publish", input,
            ));
            program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Withdrawn;
            assert!(!registered_collaboration_control_allows(
                &program, &scope, "execution", 5, "message_publish", input,
            ));
        }
        let (mut task, scope, attempt) = fixture(false);
        task.status = AgenticTaskStatus::CancelRequested;
        task.claimant = Some(scope.agent_id.clone());
        task.claim_execution_id = Some("execution".into());
        task.claim_generation = 5;
        task.cancel_requested_by = Some("operator".into());
        task.active_attempts.insert("execution".into(), attempt);
        program.tasks.insert(task.task_id.clone(), task);
        let release = r#"{"task_ref":"task","reason":"release original cancelled claim"}"#;
        assert!(registered_collaboration_control_allows(
            &program, &scope, "execution", 5, "task_release", release,
        ));
        assert!(!registered_collaboration_control_allows(
            &program, &scope, "foreign", 5, "task_release", release,
        ));
        assert!(!registered_collaboration_control_allows(
            &program, &scope, "execution", 5, "edit", release,
        ));
        assert!(!registered_collaboration_control_allows(
            &program, &scope, "execution", 5, "task_release", r#"{"task_ref":"foreign"}"#,
        ));
    }

    #[test]
    fn r1_700_control_phase_rejects_terminal_or_cancelled_tasks() {
        for review in [false, true] {
            let (mut task, scope, attempt) = fixture(review);
            let (tool, input) = if review {
                ("task_review", r#"{"task_ref":"task","decision":"accept","reason":"evidence"}"#)
            } else { ("task_claim", r#"{"task_ref":"task"}"#) };
            for status in [AgenticTaskStatus::Accepted, AgenticTaskStatus::Blocked,
                AgenticTaskStatus::CancelRequested, AgenticTaskStatus::Withdrawn, AgenticTaskStatus::Superseded] {
                task.status = status;
                assert!(!task_control_phase_allows(&task, &scope, &attempt, tool, input), "{status:?}");
            }
        }
    }
}

#[cfg(test)]
mod collaboration_control_711_tests {
    use super::*;

    #[test]
    fn r1_711_control_input_preserves_domain_revision_and_rejects_malformed_envelopes() {
        let original = r#"{"task_ref":"task","expected_revision":12}"#;
        let parsed = agent_control_input(original).unwrap();
        assert_eq!(parsed, serde_json::json!({"task_ref":"task"}));
        assert!(original.contains(r#""expected_revision":12"#));
        assert!(agent_control_input(r#"{"task_ref":"task","expected_revision":null}"#).is_some());
        for input in ["[]", "not-json", r#"{"expected_revision":-1}"#,
                      r#"{"expected_revision":"12"}"#, r#"{"expected_revision":1.5}"#] {
            assert!(agent_control_input(input).is_none(), "{input}");
        }
    }

    #[tokio::test]
    async fn r1_711_registered_coordination_control_keeps_attempt_and_write_fences() {
        let fixture = crate::agentic::coordination::tests::fixture(false).await;
        let scope = fixture.packet.agentic_binding.as_ref().unwrap();
        let program = fixture.services.agent_action_service().project(&scope.program_id).unwrap();
        let input = serde_json::json!({
            "topic_ref": format!("topic:{}", scope.team_id),
            "summary": "An independently checked boundary observation",
            "refs": [fixture.wake_ref]
        }).to_string();
        let allows = |program: &crate::agentic::AgenticProgramProjection,
                      scope: &harness_contract::agent::AgenticExecutionBinding,
                      attempt, tool, input: &str| {
            registered_collaboration_control_allows(
                program, scope, fixture.packet.graph_id(), attempt, tool, input,
            )
        };
        assert!(allows(&program, scope, fixture.packet.attempt, "message_publish", &input));
        assert!(!allows(&program, scope, fixture.packet.attempt + 1, "message_publish", &input));
        for tool in ["edit", "execute_code", "task_claim", "task_review", "task_submit",
                     "objective_update", "objective_complete_request", "task_attempt_dispatch_internal"] {
            assert!(!allows(&program, scope, fixture.packet.attempt, tool, "{}"), "{tool}");
        }
        let release = serde_json::json!({"task_ref":fixture.task_ref, "reason":"done"}).to_string();
        assert!(!allows(&program, scope, fixture.packet.attempt, "task_release", &release));
        let mut foreign = scope.clone();
        foreign.membership_id.push_str("-foreign");
        assert!(!allows(&program, &foreign, fixture.packet.attempt, "message_publish", &input));
        let mut retired = program.clone();
        retired.tasks.get_mut(&fixture.task_ref).unwrap().active_attempts.remove(fixture.packet.graph_id());
        assert!(!allows(&retired, scope, fixture.packet.attempt, "message_publish", &input));
        let mut settled = program.clone();
        settled.settle_coordination(fixture.packet.graph_id(), None);
        assert!(!allows(&settled, scope, fixture.packet.attempt, "message_publish", &input));
        let actor = crate::agentic::coordination::tests::actor(&fixture).await;
        let observation = fixture.services.submit_agent_action(
            &harness_contract::agent_action::AgentActionEnvelope {
                action_id:"r1-711-real-reply".into(), actor, expected_revision:None,
                action:serde_json::from_value(serde_json::json!({
                    "kind":"message_publish","input":serde_json::from_str::<serde_json::Value>(&input).unwrap()
                })).unwrap(),
            },
        ).await.unwrap();
        assert_eq!(observation.status, harness_contract::agent_action::AgentActionStatus::Applied);
        let replied = fixture.services.agent_action_service().project(&scope.program_id).unwrap();
        assert!(allows(&replied, scope, fixture.packet.attempt, "task_release", &release));
    }
}

impl ExecutionCommitService {
    /// Reads the original Root graph, Goal and Program. Callers may preflight
    /// with this method, but fresh effect admission also checks these revisions
    /// under ordered locks in the original intent transaction.
    pub(crate) fn root_effect_authority(
        &self,
        context: &crate::CowdExecutionContext,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        self.root_effect_authority_with_refusal(context, &|_| {})
    }

    pub(super) fn root_effect_authority_with_refusal(
        &self,
        context: &crate::CowdExecutionContext,
        refused: &dyn Fn(&str),
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        let denied = |reason: &str| {
            let message = format!("root_effect_authority: {reason}");
            refused(&message);
            ExecutionCommitError::InvalidCommand(message)
        };
        // A failed source read proves no refusal. Only known policy/identity
        // predicates above their original durable sources may certify it.
        let unavailable = |reason: String| {
            ExecutionCommitError::InvalidCommand(format!(
                "root_effect_authority source unavailable: {reason}"
            ))
        };
        let graph = self
            .effect_authority_graphs
            .load(&context.execution_id)
            .map_err(|error| unavailable(error.to_string()))?;
        self.event_store
            .ensure_session_recovery_admitted(&context.session_id)
            .map_err(unavailable)?;
        if graph.parent_execution.is_some()
            || !graph.lineage.as_ref().is_some_and(|lineage| {
                lineage.session_id == context.session_id && lineage.turn_id == context.turn_id
            })
            || !graph
                .node_statuses
                .values()
                .any(|status| *status == ExecutionNodeStatus::Running)
        {
            return Err(denied("Root graph is not running in this Session/turn"));
        }
        let goal_id = format!("goal:{}", context.execution_id);
        let goal_stream = format!("goal:{goal_id}");
        let goal = crate::execution_core::GoalStore::new(Arc::clone(&self.event_store))
            .projection(&goal_id)
            .map_err(unavailable)?;
        let mut sources = vec![
            ExpectedStreamRevision {
                stream_id: graph.id.clone(),
                expected_revision: graph.revision,
            },
            ExpectedStreamRevision {
                stream_id: goal_stream,
                expected_revision: goal.as_ref().map_or(0, |goal| goal.stream_revision),
            },
        ];
        let program_id = if let Some(projection) = &goal {
            let goal = &projection.goal;
            let binding = goal
                .execution_binding
                .as_ref()
                .ok_or_else(|| denied("Goal has no immutable execution binding"))?;
            if binding.root_execution_id != context.execution_id
                || binding.session_id != context.session_id
                || binding.turn_id != context.turn_id
                || goal.completion != harness_contract::goal::GoalCompletion::Open
                || goal.terminal.is_some()
            {
                return Err(denied("Goal is no longer open for this Root execution"));
            }
            binding.agentic_program_id.clone()
        } else {
            harness_contract::agent_action::program_id_for_objective(
                &harness_contract::agent_action::root_objective_id(
                    &context.session_id,
                    &context.turn_id,
                ),
            )
        };
        let program = self
            .effect_authority_actions
            .project_if_exists(&program_id)
            .map_err(|error| unavailable(error.to_string()))?;
        if let Some(program) = &program {
            if !matches!(
                program.status,
                AgenticProgramStatus::Open | AgenticProgramStatus::Waiting
            ) || program.session_id != context.session_id
                || program.turn_id != context.turn_id
                || program.root_execution_id.as_deref() != Some(context.execution_id.as_str())
            {
                return Err(denied("Program no longer authorizes fresh Root effects"));
            }
        }
        sources.push(ExpectedStreamRevision {
            stream_id: format!("agentic-program:{program_id}"),
            expected_revision: program.as_ref().map_or(0, |program| program.revision),
        });
        Ok(sources)
    }
}
