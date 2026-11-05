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
        if crate::execution_core::graph::graph_id_for_lookup(source_id)
            != crate::execution_core::graph::graph_id_for_lookup(execution_id)
        {
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

    #[cfg(test)]
    pub(crate) fn bound_tool_sources_for_test(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        self.tool_effect_authority(request)
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
        self.tool_effect_authority_for_original_effect(request, None, refused)
    }

    /// Only the read-intent owner supplies the actual effect. Other callers
    /// retain the original Task claim rules without a read exemption.
    pub(super) fn reviewer_read_effect_authority(
        &self, request: &crate::RuntimeToolExecutionRequest, effect: &ToolEffectDescriptor,
    ) -> Result<Vec<ExpectedStreamRevision>, ExecutionCommitError> {
        self.tool_effect_authority_for_original_effect(request, Some(effect), &|_| {})
    }

    /// Whether this call is a read-only effect, by the product's own compiled
    /// effect kind: the same classification the tool registry produced, not
    /// a new rule invented here.
    fn is_read_only_effect(request: &crate::RuntimeToolExecutionRequest) -> bool {
        let Some(invocation) = request.compiled_invocation.as_ref() else {
            return false;
        };
        let effect = &invocation.effect;
        // The same shape the product already accepts for a read that may
        // stand in for its caller (the read_ref target check in
        // `turn_engine`): a pure read — no network, no spawned process, and
        // every declared scope a read. The scope resource is the tool's own
        // (a file read declares File/Read, a tool read declares Tool/Read);
        // only the operation has to be a read, so a file reader stays a pure
        // read. A `Read` effect that spawns a process (the builtin
        // readonly_process tools, `lsp` and `vision_analyze`) is not one of
        // them.
        if effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
            || effect.uses_network
            || effect.spawns_process
            || effect.mutates_packages
            || effect.mutates_system
            || effect.scopes.is_empty()
            || effect
                .scopes
                .iter()
                .any(|scope| scope.operation != harness_contract::policy::PermissionOperation::Read)
        {
            return false;
        }
        // The compiled invocation must be this call: the model cannot name a
        // read to obtain a read-only exemption for another one.
        let Ok(request_input) = harness_contract::step::parse_strict_json(&request.input) else {
            return false;
        };
        invocation.invocation_id == request.tool_use_id
            && invocation.intent.tool_name == request.tool_name
            && invocation.intent.normalized_input == request_input
    }

    /// The sources the read-only exemption declares: the Program source
    /// only. The claim path additionally fences the root goal stream; the
    /// exemption must never declare more than the claim path does.
    fn read_only_exemption_sources(
        program_id: &str,
        program_revision: u64,
    ) -> Vec<ExpectedStreamRevision> {
        vec![ExpectedStreamRevision {
            stream_id: format!("agentic-program:{program_id}"),
            expected_revision: program_revision,
        }]
    }

    /// Whether a read-only call may precede the Task claim. The product's own
    /// effect classification decides read-only, and the scope checks are a
    /// precondition: the exemption can never bypass them.
    fn read_only_claim_exemption(
        read_only: bool,
        focus: &AgenticExecutionFocus,
        scope_authorized: bool,
    ) -> bool {
        scope_authorized
            && read_only
            && matches!(focus, AgenticExecutionFocus::TaskExecute { .. })
    }

    /// The planned claim call for this Task in this agent's graph, when the
    /// same batch plans it next to the call being admitted. It is a
    /// diagnostic hint only: it never authorizes anything and never changes
    /// a verdict.
    fn planned_claim_for_task(
        &self,
        graph: &ExecutionGraph,
        task_ref: &str,
    ) -> Option<String> {
        for node in graph.nodes.iter() {
            if node.kind != ExecutionNodeKind::ToolBatch {
                continue;
            }
            let Ok(payload) = crate::step_snapshot::decode_persisted_tool_batch_value(&node.payload_ref)
            else {
                continue;
            };
            let Some(calls) = payload["calls"].as_array() else {
                continue;
            };
            for call in calls {
                if call["name"].as_str() != Some("task_claim") {
                    continue;
                }
                let input = call["input"].as_str().unwrap_or_default();
                let Ok(parsed) = serde_json::from_str::<serde_json::Value>(input) else {
                    continue;
                };
                if parsed.get("task_ref").and_then(|value| value.as_str()) == Some(task_ref) {
                    return Some(call["id"].as_str().unwrap_or("task_claim").to_string());
                }
            }
        }
        None
    }

    /// The named verdict of the Task claim rules. The Task claim and the
    /// attempt are the ones recorded for the graph that executes the effect,
    /// compared literally; every identity fact of the claim stays a strict
    /// equality. Each conjunct is named so the error text says which one was
    /// false.
    #[allow(clippy::too_many_arguments)]
    /// What the model can do next for each named conjunct. This is text
    /// only: it never changes the verdict, and it names no internal
    /// mechanism — the model is told what to do, not how the runtime
    /// decides.
    fn claim_next_step(reason: &str) -> Option<&'static str> {
        let name = reason.split_whitespace().next()?;
        Some(match name {
            "claim_status_mismatch" => {
                "this Task has no valid claim yet: claim it with task_claim first"
            }
            "claim_not_yet_durable_in_same_batch" => {
                "the claim for this Task is in the same batch and must be accepted first: call task_claim and wait for it before acting on the Task"
            }
            "claim_team_mismatch" => "this Task belongs to another team: do not act on it",
            "claim_owner_mismatch" => "another member owns this Task's claim: do not act on it",
            "claim_execution_graph_mismatch" => {
                "this Task's claim belongs to another run: claim it again in this run"
            }
            "claim_generation_mismatch" => {
                "this Task was claimed by an earlier attempt of yours: claim it again for this attempt"
            }
            "claim_lease_expired" => "this Task's claim expired: claim it again",
            "claim_cancelled" => "this Task's claim was cancelled: do not act on it",
            "claim_pending_retirement" => "this Task is being retired: do not act on it",
            "active_attempt_missing" => {
                "this Task has no running attempt in this run: claim it first"
            }
            "active_attempt_owner_mismatch" => {
                "another member is running this Task: do not act on it"
            }
            "active_attempt_membership_mismatch" => {
                "this Task is being run by another membership: do not act on it"
            }
            "active_attempt_mode_mismatch" => {
                "this Task is being reviewed, not executed: do not act on it"
            }
            "active_attempt_execution_mismatch" => {
                "this Task's attempt belongs to another run: claim it again in this run"
            }
            _ => return None,
        })
    }

    /// Append the next step the model can take. The verdict itself is
    /// unchanged; only the reason text grows.
    fn with_next_step(reason: String) -> String {
        match Self::claim_next_step(&reason) {
            Some(step) => format!("{reason}; next: {step}"),
            None => reason,
        }
    }

    fn task_claim_verdict(
        task_ref: &str,
        status: &AgenticTaskStatus,
        task_team_id: &str,
        scope_team_id: &str,
        claimant: Option<&str>,
        scope_agent_id: &str,
        claim_execution_id: Option<&str>,
        claim_generation: u64,
        lease_expires_at_ms: Option<u64>,
        cancel_requested_by: bool,
        pending_retirement: bool,
        attempt: Option<(&str, &str, &str, AgentAttemptMode, u64)>,
        scope_membership_id: &str,
        effect_graph_id: &str,
        now_ms: u64,
        same_batch_claim: Option<&str>,
    ) -> Result<(), String> {
        Self::task_claim_verdict_inner(
            task_ref,
            status,
            task_team_id,
            scope_team_id,
            claimant,
            scope_agent_id,
            claim_execution_id,
            claim_generation,
            lease_expires_at_ms,
            cancel_requested_by,
            pending_retirement,
            attempt,
            scope_membership_id,
            effect_graph_id,
            now_ms,
            same_batch_claim,
        )
        .map_err(Self::with_next_step)
    }

    fn task_claim_verdict_inner(
        task_ref: &str,
        status: &AgenticTaskStatus,
        task_team_id: &str,
        scope_team_id: &str,
        claimant: Option<&str>,
        scope_agent_id: &str,
        claim_execution_id: Option<&str>,
        claim_generation: u64,
        lease_expires_at_ms: Option<u64>,
        cancel_requested_by: bool,
        pending_retirement: bool,
        attempt: Option<(&str, &str, &str, AgentAttemptMode, u64)>,
        scope_membership_id: &str,
        effect_graph_id: &str,
        now_ms: u64,
        same_batch_claim: Option<&str>,
    ) -> Result<(), String> {
        if *status != AgenticTaskStatus::Claimed {
            return Err(match same_batch_claim {
                Some(planned) => format!(
                    "claim_not_yet_durable_in_same_batch task_ref={task_ref} status={status:?} the same batch plans this Task's claim (call={planned}); the claim's authorization must be durable before a sibling call is admitted"
                ),
                None => format!(
                    "claim_status_mismatch task_ref={task_ref} status={status:?}"
                ),
            });
        }
        if task_team_id != scope_team_id {
            return Err(format!(
                "claim_team_mismatch task_ref={task_ref} claim_team={task_team_id} scope_team={scope_team_id}"
            ));
        }
        if claimant != Some(scope_agent_id) {
            return Err(format!(
                "claim_owner_mismatch task_ref={task_ref} claimant={claimant:?} agent={scope_agent_id}"
            ));
        }
        if claim_execution_id != Some(effect_graph_id) {
            return Err(format!(
                "claim_execution_graph_mismatch task_ref={task_ref} claim_graph={} effect_graph={effect_graph_id}",
                claim_execution_id.unwrap_or("none")
            ));
        }
        let Some((attempt_execution_id, attempt_agent_id, attempt_membership_id, attempt_mode, attempt_generation)) =
            attempt
        else {
            return Err(format!(
                "active_attempt_missing task_ref={task_ref} effect_graph={effect_graph_id} claim_generation={claim_generation}"
            ));
        };
        if attempt_agent_id != scope_agent_id {
            return Err(format!(
                "active_attempt_owner_mismatch task_ref={task_ref} attempt_agent={attempt_agent_id} agent={scope_agent_id}"
            ));
        }
        if attempt_membership_id != scope_membership_id {
            return Err(format!(
                "active_attempt_membership_mismatch task_ref={task_ref} attempt_membership={attempt_membership_id} membership={scope_membership_id}"
            ));
        }
        if attempt_mode != AgentAttemptMode::Execute {
            return Err(format!(
                "active_attempt_mode_mismatch task_ref={task_ref} mode={attempt_mode:?}"
            ));
        }
        if attempt_generation.checked_add(1) != Some(claim_generation) {
            return Err(format!(
                "claim_generation_mismatch task_ref={task_ref} claim_generation={claim_generation} attempt_generation={attempt_generation}"
            ));
        }
        if attempt_execution_id != effect_graph_id {
            return Err(format!(
                "active_attempt_execution_mismatch task_ref={task_ref} attempt_graph={attempt_execution_id} effect_graph={effect_graph_id}"
            ));
        }
        if lease_expires_at_ms.is_none_or(|expires| expires <= now_ms) {
            return Err(format!(
                "claim_lease_expired task_ref={task_ref} expires={lease_expires_at_ms:?} now_ms={now_ms}"
            ));
        }
        if cancel_requested_by {
            return Err(format!("claim_cancelled task_ref={task_ref}"));
        }
        if pending_retirement {
            return Err(format!("claim_pending_retirement task_ref={task_ref}"));
        }
        Ok(())
    }

    fn tool_effect_authority_for_original_effect(
        &self, request: &crate::RuntimeToolExecutionRequest,
        read_effect: Option<&ToolEffectDescriptor>, refused: &dyn Fn(&str),
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
        let mut packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)?;
        let physical_attempt = request.parent_execution_attempt
            .filter(|attempt| *attempt > 0)
            .ok_or_else(|| denied("parent physical attempt is missing"))?;
        if graph.recovery_cursor.node_attempts.get(&node.id) != Some(&physical_attempt)
            || self.effect_authority_graphs.current_binding(&graph, &node.id, physical_attempt).is_err()
        {
            return Err(denied("parent physical attempt has no current original binding"));
        }
        packet.attempt = physical_attempt;
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
            &program, scope, packet.graph_id(), packet.attempt, physical_attempt,
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
                    && task_control_phase_allows(task, scope, attempt, &request.tool_name, &request.input)
            })
        {
            sources.push(ExpectedStreamRevision {
                stream_id: format!("agentic-program:{}", program.program_id),
                expected_revision: program.revision,
            });
            return Ok(sources);
        }
        if matches!(scope.focus, AgenticExecutionFocus::TaskReview { .. })
            && read_effect.is_some_and(|effect| strict_original_review_read(request, effect))
            && active_independent_review_read(task, scope, packet.graph_id())
        {
            sources.push(ExpectedStreamRevision {
                stream_id: format!("agentic-program:{}", program.program_id),
                expected_revision: program.revision,
            });
            return Ok(sources);
        }
        // A read-only call is not an effect on the Task's work: it may run
        // before the Task is claimed. The classification is the product's own
        // compiled effect kind, not a new rule, and every scope check above
        // (Program state, team membership, agent activity) still applies.
        // The scope checks above return Err when they fail, so reaching this
        // point means the Program, the team membership and the agent's
        // activity all authorize this call. The read-only exemption is
        // decided after them and never bypasses them.
        if Self::read_only_claim_exemption(
            Self::is_read_only_effect(request),
            &scope.focus,
            true,
        ) {
            // Known difference (B2): the exemption path declares the Program
            // source only and does not fence the root goal stream the claim
            // path fences. The direction is safe — the exemption is the
            // relaxation, and the claim path keeps its goal fence — so it is
            // recorded here rather than changed.
            sources.extend(Self::read_only_exemption_sources(
                &program.program_id,
                program.revision,
            ));
            return Ok(sources);
        }
        if !matches!(scope.focus, AgenticExecutionFocus::TaskExecute { .. }) {
            return Err(denied("ordinary writes require an active Task execution claim"));
        }
        // The claim and the attempt are the ones recorded for the graph that
        // executes this effect, compared literally.
        let attempt = task.active_attempts.get(packet.graph_id());
        // The same batch may plan this Task's claim next to the call that is
        // being admitted; the claim is then not durable yet. Name it, so the
        // next run's error text says "the claim was still in flight" instead
        // of a generic stale claim.
        let same_batch_claim = self.planned_claim_for_task(&graph, task_ref);
        // Every conjunct is named, so the next run's error text says which one
        // was false; the verdict itself is a pure function of the facts.
        if let Err(reason) = Self::task_claim_verdict(
            task_ref,
            &task.status,
            &task.team_id,
            &scope.task_team_id,
            task.claimant.as_deref(),
            &scope.agent_id,
            task.claim_execution_id.as_deref(),
            task.claim_generation,
            task.lease_expires_at_ms,
            task.cancel_requested_by.is_some(),
            task.pending_retirement.is_some(),
            attempt.map(|attempt| {
                (
                    attempt.execution_id.as_str(),
                    attempt.agent_id.as_str(),
                    attempt.membership_id.as_str(),
                    attempt.mode,
                    attempt.generation,
                )
            }),
            &scope.membership_id,
            packet.graph_id(),
            now_ms,
            same_batch_claim.as_deref(),
        ) {
            return Err(denied(&reason));
        }
        sources.push(ExpectedStreamRevision {
            stream_id: format!("agentic-program:{}", program.program_id),
            expected_revision: program.revision,
        });
        if let Some(root_execution_id) = program.root_execution_id.as_deref() {
            if root_execution_id.trim().is_empty() {
                return Err(denied("Task Program Root execution binding is empty"));
            }
            let (_, goal_source) = self.fresh_root_goal_source(
                &crate::CowdExecutionContext {
                    execution_id: root_execution_id.to_string(),
                    session_id: program.session_id.clone(),
                    turn_id: program.turn_id.clone(),
                },
                Some(&program.program_id),
                refused,
            )?;
            if let Some(previous) = sources
                .iter()
                .find(|source| source.stream_id == goal_source.stream_id)
            {
                if previous.expected_revision != goal_source.expected_revision {
                    return Err(ExecutionCommitError::InvalidCommand(
                        "tool_effect_authority source unavailable: conflicting Goal source revisions"
                            .into(),
                    ));
                }
            } else {
                sources.push(goal_source);
            }
        }
        Ok(sources)
    }
}


fn active_independent_review_read(
    task: &crate::agentic::AgenticTaskProjection,
    scope: &harness_contract::agent::AgenticExecutionBinding,
    execution_id: &str,
) -> bool {
    task.team_id == scope.task_team_id
        && task.status == AgenticTaskStatus::Submitted
        && task.cancel_requested_by.is_none() && task.pending_retirement.is_none()
        && task.claimant.as_deref().is_some_and(|producer| producer != scope.agent_id.as_str())
        && task.active_attempts.get(execution_id).is_some_and(|attempt| {
            attempt.execution_id == execution_id && attempt.agent_id == scope.agent_id
                && attempt.membership_id == scope.membership_id && attempt.mode == AgentAttemptMode::Review
                && attempt.generation == task.review_generation
        })
}

fn strict_original_review_read(request: &crate::RuntimeToolExecutionRequest, effect: &ToolEffectDescriptor) -> bool {
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    let Some(invocation) = request.compiled_invocation.as_ref() else { return false };
    effect.effect_kind == ToolEffectKind::Read
        && !effect.spawns_process && !effect.uses_network && !effect.mutates_packages && !effect.mutates_system
        && effect.tool_id == request.tool_name && invocation.effect == *effect
        && invocation.invocation_id == request.tool_use_id && invocation.intent.tool_name == request.tool_name
        && harness_contract::step::parse_strict_json(&request.input).is_ok_and(|input| input == invocation.intent.normalized_input)
        && !effect.scopes.is_empty()
        && effect.scopes.iter().all(|scope| matches!((&scope.resource, &scope.operation),
            (PermissionResource::File, PermissionOperation::Read) | (PermissionResource::Tool, PermissionOperation::Read)))
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
    verified_node_attempt: u32,
    tool_name: &str,
    input: &str,
) -> bool {
    use harness_contract::agent_action::AgentAction;
    if packet_attempt == 0 || packet_attempt != verified_node_attempt {
        return false;
    }
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
                && attempt.generation.checked_add(1) == Some(task.claim_generation);
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
                && packet_attempt > 0
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
mod system_parent_375_tests {
    use super::*;
    use harness_contract::execution_graph::{ExecutionGraphLineage, ExecutionParentBinding};

    fn running_graph(
        service: &ExecutionCommitService,
        id: &str,
        kind: ExecutionNodeKind,
        parent: Option<ExecutionParentBinding>,
    ) -> ExecutionGraph {
        let mut graph = ExecutionGraph::new("synthetic parent-fence discriminator, no executor");
        graph.id = id.into();
        graph.parent_execution = parent;
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session:375".into(),
            turn_id: "turn:375".into(),
            root_task_id: "task:375".into(),
            task_id: "task:375".into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(kind, "not-executed", "synthetic-payload");
        node.id = format!("{id}:node");
        graph.nodes.push(node);
        graph = service.register_graph(graph).unwrap().graph;
        for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
            graph = service
                .transition_node(&graph, &format!("{id}:node"), status, None, vec![])
                .unwrap()
                .graph;
        }
        graph
    }

    fn binding(graph: &ExecutionGraph) -> ExecutionParentBinding {
        ExecutionParentBinding {
            execution_id: graph.id.clone(),
            node_id: graph.nodes[0].id.clone(),
        }
    }

    fn system_request(parent: &ExecutionGraph, source: &str) -> crate::RuntimeToolExecutionRequest {
        // Use the original SystemDerived constructor, never a ProviderStep as
        // a stand-in for the live request. The catalog is pinned, not executed.
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("system-parent-375", workspace.path());
        let step = crate::step_snapshot::BoundStep::bind(
            harness_contract::step::StepIdentity {
                step_id: "step:system:375".into(),
                session_id: "session:375".into(),
                turn_id: "turn:375".into(),
                execution_id: Some(source.into()),
                actor_ref: Some("runtime:checkpoint-guard".into()),
                run_ref: None,
                attempt: Some(1),
                epoch: Some("1".into()),
                goal_ref: None,
                program_ref: None,
                task_ref: None,
                build_ref: "synthetic-375".into(),
            },
            host.pin_snapshot(),
            &[],
            &json!({}),
            &json!({}),
            BTreeMap::new(),
            10,
        )
        .unwrap()
        .with_parent("strategy:375");
        crate::RuntimeToolExecutionRequest {
            compiled_invocation: None,
            step_binding: Some(crate::step_snapshot::StepCallBinding::system(
                Arc::new(step),
                "strategy:375",
                "checkpoint:375",
            )),
            governed_plan_id: "plan:375".into(),
            governed_plan_revision: 1,
            observation_wave_sequence: 1,
            idempotency_key: "effect:375:not-dispatched".into(),
            tool_use_id: "checkpoint:375".into(),
            tool_name: "checkpoint".into(),
            input: r#"{"action":"create","paths":["proof.txt"]}"#.into(),
            category: crate::ToolSafetyCategory::WriteLocal,
            authorization: None,
            session_id: Some("session:375".into()),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 0,
            authorized_scopes: vec![],
            memory_context: None,
            reality_context: None,
            model_lease: None,
            parent_execution: Some(binding(parent)),
            parent_execution_attempt: Some(1),
            execution_decision: None,
            evaluation_isolated: true,
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        }
    }

    #[test]
    fn r379_same_graph_run_alias_keeps_the_original_parent_fence() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = ExecutionCommitService::new(store.clone());
        let root = running_graph(
            &service,
            "graph:375:root",
            ExecutionNodeKind::InlineModel,
            None,
        );
        let child = running_graph(
            &service,
            "graph:375:child",
            ExecutionNodeKind::AgentTask,
            Some(binding(&root)),
        );
        let canonical = system_request(&child, &child.id);
        service.validate_system_tool_parent(&canonical).unwrap();
        let alias = format!("run:{}", child.id);
        let loaded = service.effect_authority_graphs.load(&alias).unwrap();
        assert_eq!(loaded.id, "graph:375:child");
        assert_eq!(loaded.revision, child.revision);
        assert_eq!(loaded.parent_execution, Some(binding(&root)));
        let aliased = system_request(&child, &alias);
        assert_ne!(loaded.parent_execution, aliased.parent_execution);
        service.validate_system_tool_parent(&aliased).unwrap();
        // Only the original reader's run: convention is recognized. Unknown
        // graph names and distinct runner:/runs: namespaces remain refused.
        for source in [
            "graph:379:unknown".to_string(),
            format!("runner:{}", child.id),
            format!("runs:{}", child.id),
        ] {
            assert!(service.effect_authority_graphs.load(&source).is_err());
            assert!(service
                .validate_system_tool_parent(&system_request(&child, &source))
                .is_err());
        }
        assert_eq!(store.stream_revision(&child.id).unwrap(), child.revision);
        assert_eq!(store.stream_revision(&root.id).unwrap(), root.revision);
        assert_eq!(
            store
                .stream_event_count("execution-effect:effect:375:not-dispatched")
                .unwrap(),
            0
        );
    }

    #[test]
    fn r375_real_cross_graph_and_identity_refusals_have_no_effects() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = ExecutionCommitService::new(store.clone());
        let parent = running_graph(
            &service,
            "graph:375:parent",
            ExecutionNodeKind::ToolBatch,
            None,
        );
        let source = running_graph(
            &service,
            "graph:375:source",
            ExecutionNodeKind::InlineModel,
            Some(binding(&parent)),
        );
        let request = system_request(&parent, &source.id);
        service.validate_system_tool_parent(&request).unwrap();
        let mut wrong_session = request.clone();
        wrong_session.session_id = Some("foreign-session".into());
        assert!(service.validate_system_tool_parent(&wrong_session).is_err());
        let mut wrong_turn = request.clone();
        Arc::make_mut(&mut wrong_turn.step_binding.as_mut().unwrap().snapshot)
            .identity
            .turn_id = "foreign-turn".into();
        assert!(service.validate_system_tool_parent(&wrong_turn).is_err());
        let foreign = running_graph(
            &service,
            "graph:375:foreign",
            ExecutionNodeKind::InlineModel,
            None,
        );
        assert!(service
            .validate_system_tool_parent(&system_request(&parent, &foreign.id))
            .is_err());
        let terminal = service
            .transition_node(
                &source,
                &source.nodes[0].id,
                ExecutionNodeStatus::Completed,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        assert!(service.validate_system_tool_parent(&request).is_err());
        let valid_parent = system_request(&parent, &parent.id);
        service.validate_system_tool_parent(&valid_parent).unwrap();
        assert_eq!(store.stream_revision(&parent.id).unwrap(), parent.revision);
        assert_eq!(
            store.stream_revision(&source.id).unwrap(),
            terminal.revision
        );
        assert_eq!(
            store
                .stream_event_count("execution-effect:effect:375:not-dispatched")
                .unwrap(),
            0
        );
    }

    #[test]
    fn r375_missing_graph_load_and_false_predicate_collapse_to_the_same_refusal() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let service = ExecutionCommitService::new(store.clone());
        let graph = running_graph(
            &service,
            "graph:375:known",
            ExecutionNodeKind::ToolBatch,
            None,
        );
        let request = system_request(&graph, &graph.id);
        service.validate_system_tool_parent(&request).unwrap();
        let mut missing = request.clone();
        missing.parent_execution.as_mut().unwrap().execution_id = "graph:375:missing".into();
        assert!(matches!(
            service.effect_authority_graphs.load("graph:375:missing"),
            Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound { .. })
        ));
        let mut false_predicate = request;
        false_predicate.parent_execution.as_mut().unwrap().node_id = "foreign-node".into();
        for denied in [missing, false_predicate] {
            assert!(matches!(service.validate_system_tool_parent(&denied),
                Err(ExecutionCommitError::InvalidCommand(reason))
                if reason == "system_tool_parent_fence_rejected"));
        }
        assert_eq!(store.stream_revision(&graph.id).unwrap(), graph.revision);
        assert_eq!(
            store
                .stream_event_count("execution-effect:effect:375:not-dispatched")
                .unwrap(),
            0
        );
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

    #[test]
    fn reviewer_read_keeps_original_descriptor_and_review_attempt_boundaries() {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        let workspace = tempfile::tempdir().unwrap();
        let lease = tools::ToolHost::builtin("review-read-owner", workspace.path()).pin_snapshot();
        for (tool, input) in [("read_file", json!({"path":"proof.txt"})),
            ("tool_search", json!({"query":"read"}))] {
            let invocation = lease.prepare_governed_invocation("review-call", tool, &input, &[]);
            let effect = invocation.effect.clone();
            let mut request = crate::RuntimeToolExecutionRequest {
                compiled_invocation: Some(invocation), step_binding: None,
                governed_plan_id: "plan".into(), governed_plan_revision: 1, observation_wave_sequence: 1,
                idempotency_key: "effect".into(), tool_use_id: "review-call".into(), tool_name: tool.into(),
                input: input.to_string(), category: crate::ToolSafetyCategory::from_effect(&effect),
                authorization: None, session_id: Some("session".into()),
                sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
                policy_revision: 1, authorized_scopes: vec![], memory_context: None, reality_context: None,
                model_lease: None, parent_execution: None, parent_execution_attempt: None,
                execution_decision: None, evaluation_isolated: true, managed_invocation: None,
                tool_progress: crate::ToolProgressSink::default(),
            };
            assert!(strict_original_review_read(&request, &effect), "original {tool} descriptor rejected: {:?}", effect.scopes);
            let mut denied = effect.clone();
            for scopes in [vec![], vec![PermissionScope::new(PermissionResource::Tool, PermissionOperation::Call)],
                vec![PermissionScope::new(PermissionResource::Tool, PermissionOperation::Read),
                    PermissionScope::new(PermissionResource::File, PermissionOperation::Write)]] {
                denied.scopes = scopes;
                request.compiled_invocation.as_mut().unwrap().effect = denied.clone();
                assert!(!strict_original_review_read(&request, &denied));
            }
            for process in [false, true] {
                denied = effect.clone();
                if process { denied.spawns_process = true; } else { denied.uses_network = true; }
                request.compiled_invocation.as_mut().unwrap().effect = denied.clone();
                assert!(!strict_original_review_read(&request, &denied));
            }
        }
        let (mut task, scope, attempt) = fixture(true);
        task.active_attempts.insert("execution".into(), attempt);
        assert!(active_independent_review_read(&task, &scope, "execution"));
        task.claimant = Some(scope.agent_id.clone());
        assert!(!active_independent_review_read(&task, &scope, "execution"));
        task.claimant = Some("producer".into());
        task.review_generation += 1;
        assert!(!active_independent_review_read(&task, &scope, "execution"));
        task.review_generation -= 1;
        task.cancel_requested_by = Some("operator".into());
        assert!(!active_independent_review_read(&task, &scope, "execution"));
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
                &program, &scope, "execution", 5, 5, "message_publish", input,
            ));
            for tool in ["edit", "execute_code", "task_submit", "task_release", "objective_complete_request"] {
                assert!(!registered_collaboration_control_allows(
                    &program, &scope, "execution", 5, 5, tool, r#"{"task_ref":"task"}"#,
                ), "{review}/{tool}");
            }
            assert!(!registered_collaboration_control_allows(
                &program, &scope, "execution", 6, 5, "message_publish", input,
            ));
            program.tasks.get_mut("task").unwrap().status = AgenticTaskStatus::Withdrawn;
            assert!(!registered_collaboration_control_allows(
                &program, &scope, "execution", 5, 5, "message_publish", input,
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
            &program, &scope, "execution", 5, 5, "task_release", release,
        ));
        assert!(!registered_collaboration_control_allows(
            &program, &scope, "foreign", 5, 5, "task_release", release,
        ));
        assert!(!registered_collaboration_control_allows(
            &program, &scope, "execution", 5, 5, "edit", release,
        ));
        assert!(!registered_collaboration_control_allows(
            &program, &scope, "execution", 5, 5, "task_release", r#"{"task_ref":"foreign"}"#,
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
                program, scope, fixture.packet.graph_id(), attempt, fixture.packet.attempt, tool, input,
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
    }
}

impl ExecutionCommitService {
    /// One Goal reader for fresh Root and bound business effects. This does
    /// not grant tool permissions and is never used for admitted settlement.
    fn fresh_root_goal_source(
        &self,
        context: &crate::CowdExecutionContext,
        expected_program: Option<&str>,
        refused: &dyn Fn(&str),
    ) -> Result<(String, ExpectedStreamRevision), ExecutionCommitError> {
        let denied = |reason: &str| {
            let message = format!("root_effect_authority: {reason}");
            refused(&message);
            ExecutionCommitError::InvalidCommand(message)
        };
        let goal_id = format!("goal:{}", context.execution_id);
        let goal_stream = format!("goal:{goal_id}");
        let projection = crate::execution_core::GoalStore::new(Arc::clone(&self.event_store))
            .projection(&goal_id)
            .map_err(|error| {
                ExecutionCommitError::InvalidCommand(format!(
                    "root_effect_authority source unavailable: {error}"
                ))
            })?;
        let program_id = if let Some(projection) = &projection {
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
            if expected_program
                .is_some_and(|program| program != binding.agentic_program_id.as_str())
            {
                return Err(denied(
                    "Goal Program does not match the original Task Program",
                ));
            }
            binding.agentic_program_id.clone()
        } else {
            expected_program.map(str::to_string).unwrap_or_else(|| {
                harness_contract::agent_action::program_id_for_objective(
                    &harness_contract::agent_action::root_objective_id(
                        &context.session_id,
                        &context.turn_id,
                    ),
                )
            })
        };
        Ok((
            program_id,
            ExpectedStreamRevision {
                stream_id: goal_stream,
                expected_revision: projection.as_ref().map_or(0, |goal| goal.stream_revision),
            },
        ))
    }

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
        let (program_id, goal_source) = self.fresh_root_goal_source(context, None, refused)?;
        let mut sources = vec![
            ExpectedStreamRevision {
                stream_id: graph.id.clone(),
                expected_revision: graph.revision,
            },
            goal_source,
        ];
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

#[cfg(test)]
mod task_claim_verdict_tests {
    use super::ExecutionCommitService;
    use crate::AgenticTaskStatus;
    use harness_contract::agent::AgenticExecutionFocus;
    use harness_contract::agent_action::AgentAttemptMode;

    fn attempt<'a>(
        execution_id: &'a str,
        agent_id: &'a str,
        membership_id: &'a str,
        mode: AgentAttemptMode,
        generation: u64,
    ) -> (&'a str, &'a str, &'a str, AgentAttemptMode, u64) {
        (execution_id, agent_id, membership_id, mode, generation)
    }

    /// The Task claim is made in the graph that executes the effect and the
    /// attempt is registered under that graph: the admission holds.
    #[test]
    fn a_claim_on_the_effects_own_graph_is_accepted() {
        let verdict = ExecutionCommitService::task_claim_verdict(
            "task:1",
            &AgenticTaskStatus::Claimed,
            "team-1",
            "team-1",
            Some("agent-1"),
            "agent-1",
            Some("agentic-graph:agent"),
            1,
            Some(9_999_999_999_999),
            false,
            false,
            Some(attempt(
                "agentic-graph:agent",
                "agent-1",
                "membership-1",
                AgentAttemptMode::Execute,
                0,
            )),
            "membership-1",
            "agentic-graph:agent",
            1_000,
            None,
        );
        assert!(verdict.is_ok(), "{verdict:?}");
    }

    /// A claim recorded for another graph is refused, and the refusal names
    /// both graph ids.
    #[test]
    fn a_claim_from_another_graph_is_named() {
        let verdict = ExecutionCommitService::task_claim_verdict(
            "task:1",
            &AgenticTaskStatus::Claimed,
            "team-1",
            "team-1",
            Some("agent-1"),
            "agent-1",
            Some("agentic-graph:other"),
            1,
            Some(9_999_999_999_999),
            false,
            false,
            Some(attempt(
                "agentic-graph:other",
                "agent-1",
                "membership-1",
                AgentAttemptMode::Execute,
                0,
            )),
            "membership-1",
            "agentic-graph:agent",
            1_000,
            None,
        );
        assert!(
            verdict
                .as_ref().err().map(String::as_str)
                .is_some_and(|reason| reason.starts_with("claim_execution_graph_mismatch")),
            "{verdict:?}"
        );
    }

    /// The identity of the attempt stays strict: a different agent and a
    /// different membership are still refused.
    #[test]
    fn the_attempt_identity_stays_strict() {
        for (agent, membership, expected) in [
            ("agent-2", "membership-1", "active_attempt_owner_mismatch"),
            ("agent-1", "membership-2", "active_attempt_membership_mismatch"),
        ] {
            let verdict = ExecutionCommitService::task_claim_verdict(
                "task:1",
                &AgenticTaskStatus::Claimed,
                "team-1",
                "team-1",
                Some("agent-1"),
                "agent-1",
                Some("agentic-graph:agent"),
                1,
                Some(9_999_999_999_999),
                false,
                false,
                Some(attempt(
                    "agentic-graph:agent",
                    agent,
                    membership,
                    AgentAttemptMode::Execute,
                    0,
                )),
                "membership-1",
                "agentic-graph:agent",
                1_000,
                None,
            );
            assert!(
                verdict.as_ref().err().map(String::as_str).is_some_and(|reason| reason.starts_with(expected)),
                "{agent}/{membership}: {verdict:?}"
            );
        }
    }

    /// A call whose Task has no durable claim is refused: only a read-only
    /// effect may precede the claim, and that exemption is decided by the
    /// product's own effect classification before this verdict.
    #[test]
    fn a_call_without_a_durable_claim_is_denied() {
        let verdict = ExecutionCommitService::task_claim_verdict(
            "task:1",
            &AgenticTaskStatus::Published,
            "team-1",
            "team-1",
            None,
            "agent-1",
            None,
            0,
            None,
            false,
            false,
            Some(attempt(
                "agentic-graph:agent",
                "agent-1",
                "membership-1",
                AgentAttemptMode::Execute,
                0,
            )),
            "membership-1",
            "agentic-graph:agent",
            1_000,
            None,
        );
        assert!(
            verdict
                .as_ref().err().map(String::as_str)
                .is_some_and(|reason| reason.starts_with("claim_status_mismatch")),
            "{verdict:?}"
        );
    }

    /// When the same batch plans this Task's claim next to the call being
    /// admitted, the refusal says the claim was still in flight.
    #[test]
    fn a_claim_planned_in_the_same_batch_is_named() {
        let verdict = ExecutionCommitService::task_claim_verdict(
            "task:1",
            &AgenticTaskStatus::Published,
            "team-1",
            "team-1",
            None,
            "agent-1",
            None,
            0,
            None,
            false,
            false,
            Some(attempt(
                "agentic-graph:agent",
                "agent-1",
                "membership-1",
                AgentAttemptMode::Execute,
                0,
            )),
            "membership-1",
            "agentic-graph:agent",
            1_000,
            Some("chatcmpl-tool-claim-1"),
        );
        assert!(
            verdict.as_ref().err().map(String::as_str).is_some_and(|reason| reason
                .starts_with("claim_not_yet_durable_in_same_batch")),
            "{verdict:?}"
        );
        assert!(
            verdict
                .as_ref().err().map(String::as_str)
                .is_some_and(|reason| reason.contains("chatcmpl-tool-claim-1")),
            "{verdict:?}"
        );
    }

    /// A review-mode attempt and an expired lease are named separately.
    #[test]
    fn the_generation_mode_and_lease_conjuncts_are_named() {
        let base = |mode, lease, now| {
            ExecutionCommitService::task_claim_verdict(
                "task:1",
                &AgenticTaskStatus::Claimed,
                "team-1",
                "team-1",
                Some("agent-1"),
                "agent-1",
                Some("agentic-graph:agent"),
                1,
                lease,
                false,
                false,
                Some(attempt(
                    "agentic-graph:agent",
                    "agent-1",
                    "membership-1",
                    mode,
                    0,
                )),
                "membership-1",
                "agentic-graph:agent",
                now,
                None,
            )
        };
        assert!(
            base(AgentAttemptMode::Review, Some(9_999_999_999_999), 1_000)
                .as_ref().err().map(String::as_str)
                .is_some_and(|reason| reason.starts_with("active_attempt_mode_mismatch")),
        );
        assert!(
            base(AgentAttemptMode::Execute, Some(500), 1_000)
                .as_ref().err().map(String::as_str)
                .is_some_and(|reason| reason.starts_with("claim_lease_expired")),
        );
        assert!(base(AgentAttemptMode::Execute, Some(9_999_999_999_999), 1_000).is_ok());
    }

    /// The other side of the exemption: a read-only call is exempt only
    /// after the scope checks passed. With the scope checks not passed the
    /// exemption does not apply, so the call is refused as well.
    #[test]
    fn a_read_only_call_is_exempt_only_after_the_scope_checks_passed() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("effect-authority-read-test", workspace.path());
        let input = serde_json::json!({"path":"crates/runtime/src/lib.rs"});
        let effect = host.pin_snapshot().describe_effect("read_file", &input);
        assert_eq!(effect.effect_kind, harness_contract::tool::ToolEffectKind::Read);
        let invocation = harness_contract::tool::GovernedToolInvocation {
            origin: None,
            contract_version: crate::governed_tool_plan::GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
            invocation_id: "call-read".into(),
            intent: harness_contract::tool::ToolIntent {
                invocation_id: "call-read".into(),
                tool_name: "read_file".into(),
                normalized_input: input.clone(),
            },
            resource_demand: harness_contract::tool::ResourceDemand {
                tool_slots: 1,
                process_slots: 0,
                network_slots: 0,
                cpu_weight: 1,
                memory_bytes: 0,
                scopes: vec![],
            },
            effect,
            explicit_dependencies: vec![],
            compiled_dependencies: vec![],
            catalog_revision: 1,
            descriptor_set_hash: "sha256:test-descriptor-set".into(),
            idempotency_key: "call-read".into(),
        };
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "call-read".into(),
                tool_name: "read_file".into(),
                input: input.to_string(),
                depends_on: vec![],
            },
        );
        request.compiled_invocation = Some(invocation);
        // The product's own classification says this call is read-only.
        assert!(ExecutionCommitService::is_read_only_effect(&request));
        // The scope checks are a precondition: without them the exemption
        // does not apply, so the call is refused like any other.
        assert!(
            !ExecutionCommitService::read_only_claim_exemption(
                true,
                &AgenticExecutionFocus::TaskExecute {
                    task_ref: "task:1".into()
                },
                false,
            ),
            "the read-only exemption never bypasses the scope checks"
        );
        // With the scope checks passed the exemption does apply, and it stays
        // inside the TaskExecute focus.
        assert!(ExecutionCommitService::read_only_claim_exemption(
            true,
            &AgenticExecutionFocus::TaskExecute {
                task_ref: "task:1".into()
            },
            true,
        ));
        assert!(!ExecutionCommitService::read_only_claim_exemption(
            true,
            &AgenticExecutionFocus::TaskReview {
                task_ref: "task:1".into()
            },
            true,
        ));
    }

    /// B1: a `Read` effect that spawns a process (the builtin
    /// readonly_process tools, `lsp` and `vision_analyze`) is not a pure
    /// read, so the exemption must not cover it and a Task without a claim
    /// still refuses it.
    #[test]
    fn a_read_effect_that_spawns_a_process_is_not_exempt() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("effect-authority-process-test", workspace.path());
        let input = serde_json::json!({"path":"crates/runtime/src/lib.rs"});
        // The shape of the builtin readonly_process tools (`lsp` and
        // `vision_analyze`): a Read effect that spawns a process. The LSP
        // surface is not part of the builtin catalog, so the shape is built
        // from a real read effect instead of from that tool.
        let mut effect = host.pin_snapshot().describe_effect("read_file", &input);
        assert_eq!(effect.effect_kind, harness_contract::tool::ToolEffectKind::Read);
        effect.spawns_process = true;
        assert!(
            effect.spawns_process,
            "the builtin readonly_process tools spawn a process"
        );
        let invocation = harness_contract::tool::GovernedToolInvocation {
            origin: None,
            contract_version: crate::governed_tool_plan::GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
            invocation_id: "call-lsp".into(),
            intent: harness_contract::tool::ToolIntent {
                invocation_id: "call-lsp".into(),
                tool_name: "lsp".into(),
                normalized_input: input.clone(),
            },
            resource_demand: harness_contract::tool::ResourceDemand {
                tool_slots: 1,
                process_slots: 1,
                network_slots: 0,
                cpu_weight: 1,
                memory_bytes: 0,
                scopes: vec![],
            },
            effect,
            explicit_dependencies: vec![],
            compiled_dependencies: vec![],
            catalog_revision: 1,
            descriptor_set_hash: "sha256:test-descriptor-set".into(),
            idempotency_key: "call-lsp".into(),
        };
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "call-lsp".into(),
                tool_name: "lsp".into(),
                input: input.to_string(),
                depends_on: vec![],
            },
        );
        request.compiled_invocation = Some(invocation);
        assert!(
            !ExecutionCommitService::is_read_only_effect(&request),
            "a read that spawns a process never takes the read-only exemption"
        );
        let reason = ExecutionCommitService::task_claim_verdict(
            "task:1",
            &AgenticTaskStatus::Published,
            "team-1",
            "team-1",
            None,
            "agent-1",
            None,
            0,
            None,
            false,
            false,
            None,
            "membership-1",
            "agentic-graph:agent",
            1_000,
            None,
        )
        .expect_err("a Task without a claim must refuse it");
        assert!(reason.starts_with("claim_status_mismatch"), "{reason}");
    }

    /// The compiled invocation must be the same call as the request: a model
    /// cannot name a read to obtain the exemption for another call.
    #[test]
    fn a_compiled_invocation_that_is_not_this_call_is_not_exempt() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("effect-authority-mismatch-test", workspace.path());
        let read_input = serde_json::json!({"path":"crates/runtime/src/lib.rs"});
        let effect = host.pin_snapshot().describe_effect("read_file", &read_input);
        assert_eq!(effect.effect_kind, harness_contract::tool::ToolEffectKind::Read);
        let invocation = harness_contract::tool::GovernedToolInvocation {
            origin: None,
            contract_version: crate::governed_tool_plan::GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
            invocation_id: "call-read".into(),
            intent: harness_contract::tool::ToolIntent {
                invocation_id: "call-read".into(),
                tool_name: "read_file".into(),
                normalized_input: read_input.clone(),
            },
            resource_demand: harness_contract::tool::ResourceDemand {
                tool_slots: 1,
                process_slots: 0,
                network_slots: 0,
                cpu_weight: 1,
                memory_bytes: 0,
                scopes: vec![],
            },
            effect,
            explicit_dependencies: vec![],
            compiled_dependencies: vec![],
            catalog_revision: 1,
            descriptor_set_hash: "sha256:test-descriptor-set".into(),
            idempotency_key: "call-read".into(),
        };
        // The request is a different call than the compiled invocation.
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "call-other".into(),
                tool_name: "edit".into(),
                input: "{\"action\":\"write\"}".into(),
                depends_on: vec![],
            },
        );
        request.compiled_invocation = Some(invocation);
        assert!(
            !ExecutionCommitService::is_read_only_effect(&request),
            "a compiled invocation for another call never grants the exemption"
        );
    }

    /// The exemption declares the Program source only: it must never declare
    /// more sources than the claim path does (which also fences the root goal
    /// stream). The difference itself is recorded as known (B2).
    #[test]
    fn the_read_only_exemption_declares_the_program_source_only() {
        let sources =
            ExecutionCommitService::read_only_exemption_sources("program-1", 7);
        assert_eq!(sources.len(), 1, "the exemption declares one source");
        assert_eq!(sources[0].stream_id, "agentic-program:program-1");
        assert_eq!(sources[0].expected_revision, 7);
    }

    /// The exemption is confined to the TaskExecute focus.
    #[test]
    fn the_read_only_exemption_is_confined_to_task_execute() {
        for focus in [
            AgenticExecutionFocus::TaskReview {
                task_ref: "task:1".into(),
            },
            AgenticExecutionFocus::Coordination {
                wake_ref: "wake:1".into(),
            },
            AgenticExecutionFocus::ObjectiveReview {
                criterion_refs: vec!["criterion:1".into()],
            },
        ] {
            assert!(
                !ExecutionCommitService::read_only_claim_exemption(true, &focus, true),
                "{focus:?} never takes the read-only exemption"
            );
        }
        assert!(ExecutionCommitService::read_only_claim_exemption(
            true,
            &AgenticExecutionFocus::TaskExecute {
                task_ref: "task:1".into()
            },
            true,
        ));
    }

    /// The counter-example to the read-only exemption: an effective call is
    /// not read-only, so the exemption cannot apply to it, and a Task that was
    /// never claimed still refuses it. These are the two decisions the
    /// authority itself makes, in that order, for a TaskExecute scope.
    #[test]
    fn an_effective_call_without_a_claim_is_still_refused() {
        let workspace = tempfile::tempdir().unwrap();
        let host = tools::ToolHost::builtin("effect-authority-test", workspace.path());
        let input = serde_json::json!({"action":"write","path":"proof.txt","content":"x"});
        let effect = host.pin_snapshot().describe_effect("edit", &input);
        assert_ne!(effect.effect_kind, harness_contract::tool::ToolEffectKind::Read);
        let invocation = harness_contract::tool::GovernedToolInvocation {
            origin: None,
            contract_version: crate::governed_tool_plan::GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
            invocation_id: "call-1".into(),
            intent: harness_contract::tool::ToolIntent {
                invocation_id: "call-1".into(),
                tool_name: "edit".into(),
                normalized_input: input.clone(),
            },
            resource_demand: harness_contract::tool::ResourceDemand {
                tool_slots: 1,
                process_slots: 1,
                network_slots: 0,
                cpu_weight: 1,
                memory_bytes: 0,
                scopes: vec![],
            },
            effect,
            explicit_dependencies: vec![],
            compiled_dependencies: vec![],
            catalog_revision: 1,
            descriptor_set_hash: "sha256:test-descriptor-set".into(),
            idempotency_key: "call-1".into(),
        };
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "call-1".into(),
                tool_name: "edit".into(),
                input: input.to_string(),
                depends_on: vec![],
            },
        );
        request.compiled_invocation = Some(invocation);
        // The read-only exemption cannot apply to this call.
        assert!(!ExecutionCommitService::is_read_only_effect(&request));
        // And the claim conjuncts refuse a Task that was never claimed.
        let reason = ExecutionCommitService::task_claim_verdict(
            "task:1",
            &AgenticTaskStatus::Published,
            "team-1",
            "team-1",
            None,
            "agent-1",
            None,
            0,
            None,
            false,
            false,
            None,
            "membership-1",
            "agentic-graph:agent",
            1_000,
            None,
        )
        .expect_err("an effective call without a claim must be refused");
        assert!(reason.starts_with("claim_status_mismatch"), "{reason}");
    }

    /// Every named conjunct must tell the model what it can do next, and the
    /// hint must stay free of internal mechanism.
    #[test]
    fn every_claim_conjunct_tells_the_model_what_to_do_next() {
        for (name, expected) in [
            ("claim_status_mismatch", "task_claim"),
            ("claim_not_yet_durable_in_same_batch", "task_claim"),
            ("claim_team_mismatch", "do not act on it"),
            ("claim_owner_mismatch", "do not act on it"),
            ("claim_execution_graph_mismatch", "claim it again"),
            ("claim_generation_mismatch", "claim it again"),
            ("claim_lease_expired", "claim it again"),
            ("claim_cancelled", "do not act on it"),
            ("claim_pending_retirement", "do not act on it"),
            ("active_attempt_missing", "claim it first"),
            ("active_attempt_owner_mismatch", "do not act on it"),
            ("active_attempt_membership_mismatch", "do not act on it"),
            ("active_attempt_mode_mismatch", "do not act on it"),
            ("active_attempt_execution_mismatch", "claim it again"),
        ] {
            let step = ExecutionCommitService::claim_next_step(&format!("{name} task_ref=task:1"))
                .unwrap_or_else(|| panic!("{name} has no next step"));
            assert!(step.contains(expected), "{name}: {step}");
            for internal in ["projection", "generation", "membership_id", "epoch", "lease_expires"] {
                assert!(
                    !step.contains(internal),
                    "{name} leaks an internal detail: {step}"
                );
            }
        }
        // A reason that is not a claim conjunct carries no hint.
        assert!(
            ExecutionCommitService::claim_next_step("some_other_reason task_ref=task:1").is_none()
        );
    }

    /// A read effect that mutates packages or the system is not a pure read
    /// and must not take the read-only exemption.
    #[test]
    fn a_read_effect_that_mutates_packages_or_system_is_not_exempt() {
        for mutate in ["packages", "system"] {
            let workspace = tempfile::tempdir().unwrap();
            let host = tools::ToolHost::builtin("effect-authority-mutate-test", workspace.path());
            let input = serde_json::json!({"path":"crates/runtime/src/lib.rs"});
            let mut effect = host.pin_snapshot().describe_effect("read_file", &input);
            assert_eq!(effect.effect_kind, harness_contract::tool::ToolEffectKind::Read);
            if mutate == "packages" {
                effect.mutates_packages = true;
            } else {
                effect.mutates_system = true;
            }
            let invocation = harness_contract::tool::GovernedToolInvocation {
                origin: None,
                contract_version: crate::governed_tool_plan::GOVERNED_TOOL_PLAN_CONTRACT_VERSION,
                invocation_id: "call-read".into(),
                intent: harness_contract::tool::ToolIntent {
                    invocation_id: "call-read".into(),
                    tool_name: "read_file".into(),
                    normalized_input: input.clone(),
                },
                resource_demand: harness_contract::tool::ResourceDemand {
                    tool_slots: 1,
                    process_slots: 0,
                    network_slots: 0,
                    cpu_weight: 1,
                    memory_bytes: 0,
                    scopes: vec![],
                },
                effect,
                explicit_dependencies: vec![],
                compiled_dependencies: vec![],
                catalog_revision: 1,
                descriptor_set_hash: "sha256:test-descriptor-set".into(),
                idempotency_key: "call-read".into(),
            };
            let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: "call-read".into(),
                    tool_name: "read_file".into(),
                    input: input.to_string(),
                    depends_on: vec![],
                },
            );
            request.compiled_invocation = Some(invocation);
            assert!(
                !ExecutionCommitService::is_read_only_effect(&request),
                "a read that mutates {mutate} never takes the read-only exemption"
            );
        }
    }

}
