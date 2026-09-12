//! Fresh side effects use the original graph and Program claim owners.
//! The returned revisions join the effect intent transaction; no ToolHost
//! operation runs while these stream locks are held.
use super::*;
use crate::{AgenticProgramStatus, AgenticTaskStatus};
use harness_contract::agent::AgenticExecutionFocus;
use harness_contract::agent_action::AgentAttemptMode;

impl ExecutionCommitService {
    pub(super) fn tool_effect_authority(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        let Some(parent) = &request.parent_execution else {
            return Ok(Vec::new());
        };
        let denied = |reason: &str| {
            ExecutionCommitError::InvalidCommand(format!("tool_effect_authority: {reason}"))
        };
        let graph = self
            .effect_authority_graphs
            .load(&parent.execution_id)
            .map_err(|error| denied(&error.to_string()))?;
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
                return self.root_effect_authority(&crate::CowdExecutionContext {
                    execution_id: graph.id.clone(),
                    session_id: lineage.session_id.clone(),
                    turn_id: lineage.turn_id.clone(),
                });
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
            .map_err(|error| denied(&error.to_string()))?;
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
        let AgenticExecutionFocus::TaskExecute { task_ref } = &scope.focus else {
            return Err(denied(
                "ordinary writes require an active Task execution claim",
            ));
        };
        let task = program
            .tasks
            .get(task_ref)
            .ok_or_else(|| denied("Task is missing"))?;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
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

impl ExecutionCommitService {
    /// Reads the original Root graph, Goal and Program. Callers may preflight
    /// with this method, but fresh effect admission also checks these revisions
    /// under ordered locks in the original intent transaction.
    pub(crate) fn root_effect_authority(
        &self,
        context: &crate::CowdExecutionContext,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        let denied = |reason: &str| {
            ExecutionCommitError::InvalidCommand(format!("root_effect_authority: {reason}"))
        };
        let graph = self
            .effect_authority_graphs
            .load(&context.execution_id)
            .map_err(|error| denied(&error.to_string()))?;
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
            .map_err(|error| denied(&error))?;
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
            .map_err(|error| denied(&error.to_string()))?;
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
