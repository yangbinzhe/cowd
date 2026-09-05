use super::*;
use super::{admission::*, heartbeat::agentic_graph_is_terminal, helpers::*};
use harness_contract::agent::{CohortPromptPackage, CohortPromptPacket};

// Accounting prediction only. `ParentExecutionBudgetLedger::reserve_provider`
// records overruns and borrowing but never rejects or shrinks an Agent model
// request, so this estimate cannot become a business-execution budget gate.
const AGENTIC_TASK_CAPACITY_ESTIMATE_TOKENS: u64 = 1_000_000;
const AGENTIC_SHARED_OBJECTIVE_MAX_CHARS: usize = 12_000;
const AGENTIC_SHARED_TEAM_FIELD_MAX_CHARS: usize = 4_000;

fn bounded_shared_context(value: &str, max_chars: usize) -> String {
    let trimmed = value.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut bounded = trimmed.chars().take(max_chars).collect::<String>();
    bounded.push_str("\n[bounded by Runtime]");
    bounded
}

pub(super) fn agentic_shared_prompt_package(
    projection: &AgenticProgramProjection,
    team_id: &str,
) -> Result<CohortPromptPackage, String> {
    let team = projection
        .teams
        .get(team_id)
        .ok_or_else(|| format!("agentic_team_not_found:{team_id}"))?;
    let team_objective = team
        .objective
        .as_deref()
        .unwrap_or("not separately declared");
    let content = format!(
        "Program objective:\n{}\n\nTeam name:\n{}\n\nTeam mission:\n{}\n\nTeam objective:\n{}\n\nPublic Team topic reference:\n{}",
        bounded_shared_context(
            &projection.objective_summary,
            AGENTIC_SHARED_OBJECTIVE_MAX_CHARS,
        ),
        bounded_shared_context(&team.name, AGENTIC_SHARED_TEAM_FIELD_MAX_CHARS),
        bounded_shared_context(&team.mission, AGENTIC_SHARED_TEAM_FIELD_MAX_CHARS),
        bounded_shared_context(team_objective, AGENTIC_SHARED_TEAM_FIELD_MAX_CHARS),
        bounded_shared_context(&team.topic_ref, AGENTIC_SHARED_TEAM_FIELD_MAX_CHARS),
    );
    let package = CohortPromptPackage::for_agentic_program(
        projection.session_id.clone(),
        projection.program_id.clone(),
        team.team_id.clone(),
        vec![CohortPromptPacket {
            source: "runtime.agentic_program.shared_context.v1".to_string(),
            content,
            evidence_refs: Vec::new(),
        }],
    );
    package.validate().map_err(|error| error.to_string())?;
    Ok(package)
}

pub(super) struct DispatchFlight {
    key: String,
    active: Arc<Mutex<BTreeSet<String>>>,
}

impl DispatchFlight {
    pub(super) fn acquire(active: Arc<Mutex<BTreeSet<String>>>, key: String) -> Option<Self> {
        let inserted = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone());
        inserted.then_some(Self { key, active })
    }
}

impl Drop for DispatchFlight {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

impl RuntimeServices {
    /// Translate committed semantic collaboration work into real Agent
    /// execution graphs. Runtime selects one eligible low-load member per
    /// ready Task, but the invited Agent remains the sole owner of the first
    /// durable claim. Semantic reservation, results and completion therefore
    /// all come from the Agent's own action protocol.
    pub async fn dispatch_agentic_followups(
        self: &Arc<Self>,
        envelope: &AgentActionEnvelope,
        context: AgenticDispatchContext,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        let projection = self
            .agent_action_service()
            .project(&envelope.actor.program_id)
            .map_err(|error| error.to_string())?;
        let mut requests = Vec::new();
        let observed_at_ms = now_ms();
        match &envelope.action {
            // Queries never create provider work. Mutations and startup
            // recovery feed the deterministic reconciler instead.
            AgentAction::StateInspect(_) => {}
            AgentAction::TaskPublish(_) => {
                if let Some(task_ref) = entity_ref_for_action(envelope) {
                    if task_is_ready(&projection, &task_ref) {
                        requests.push((task_ref, DispatchMode::Execute));
                    }
                }
            }
            AgentAction::AgentInvite(input) => {
                requests.extend(
                    projection
                        .tasks
                        .values()
                        .filter(|task| task.team_id == input.team_ref)
                        .filter(|task| task_ready_for_execution(task, observed_at_ms))
                        .filter(|task| dependencies_accepted(&projection, task))
                        .map(|task| (task.task_id.clone(), DispatchMode::Execute)),
                );
                requests.extend(
                    projection
                        .tasks
                        .values()
                        .filter(|task| task.status == AgenticTaskStatus::Submitted)
                        .map(|task| (task.task_id.clone(), DispatchMode::Review)),
                );
            }
            AgentAction::TaskSubmit(input) => {
                requests.push((input.task_ref.clone(), DispatchMode::Review));
            }
            AgentAction::TaskRelease(input) => {
                requests.push((input.task_ref.clone(), DispatchMode::Execute));
            }
            AgentAction::TaskSupersede(input) => {
                requests.extend(
                    input
                        .replacement_task_refs
                        .iter()
                        .filter(|task_ref| task_is_ready(&projection, task_ref))
                        .cloned()
                        .map(|task_ref| (task_ref, DispatchMode::Execute)),
                );
            }
            AgentAction::TaskReview(input) => match input.decision {
                harness_contract::agent_action::TaskReviewDecision::Accept => {
                    requests.extend(
                        projection
                            .tasks
                            .values()
                            .filter(|task| {
                                task.status == AgenticTaskStatus::Published
                                    && task.depends_on.contains(&input.task_ref)
                                    && dependencies_accepted(&projection, task)
                            })
                            .map(|task| (task.task_id.clone(), DispatchMode::Execute)),
                    );
                }
                harness_contract::agent_action::TaskReviewDecision::Challenge
                | harness_contract::agent_action::TaskReviewDecision::Rework => {
                    requests.push((input.task_ref.clone(), DispatchMode::Execute));
                }
            },
            AgentAction::TaskAttemptFail(input) => {
                if let Some(task) = projection.tasks.get(&input.task_ref) {
                    match input.mode {
                        harness_contract::agent_action::AgentAttemptMode::Execute
                            if task.status == AgenticTaskStatus::Rework =>
                        {
                            requests.push((input.task_ref.clone(), DispatchMode::Execute));
                        }
                        harness_contract::agent_action::AgentAttemptMode::Review
                            if task.status == AgenticTaskStatus::Submitted =>
                        {
                            requests.push((input.task_ref.clone(), DispatchMode::Review));
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
        requests.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        requests.dedup();

        let mut receipts = Vec::new();
        let mut deferred = Vec::new();
        for (task_ref, mode) in requests {
            match self
                .dispatch_agentic_task(&projection, &task_ref, mode, &context, envelope)
                .await
            {
                Ok(dispatched) => receipts.extend(dispatched),
                Err(error) => deferred.push(format!("{}:{}:{error}", mode.as_str(), task_ref)),
            }
        }
        if receipts.is_empty() && !deferred.is_empty() {
            return Err(format!(
                "Agent execution dispatch deferred after durable action commit: {}",
                deferred.join(" | ")
            ));
        }
        if !deferred.is_empty() {
            tracing::warn!(
                program_id = %projection.program_id,
                deferred = %deferred.join(" | "),
                dispatched = receipts.len(),
                "some Agent-first followups were deferred while independent work was admitted"
            );
        }
        Ok(receipts)
    }

    /// Reconcile all durable open Programs after graph recovery. This closes
    /// the crash window between an Agent action commit and physical graph
    /// admission without depending on a model or browser poll.
    pub async fn recover_agentic_programs_on_startup(
        self: &Arc<Self>,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        let streams = self
            .event_store()
            .stream_ids_for_scope(crate::RuntimeEventScope::Program)
            .map_err(|error| error.to_string())?;
        let mut receipts = Vec::new();
        for stream in streams {
            let Some(program_id) = stream.strip_prefix("agentic-program:") else {
                continue;
            };
            let mut projection = self
                .agent_action_service()
                .project(program_id)
                .map_err(|error| error.to_string())?;
            if projection.status != super::super::program::AgenticProgramStatus::Open {
                continue;
            }
            // An Agent claim is the durable reservation for its physical
            // execution. If Runtime died after that model-authored claim was
            // committed, release a missing/terminal graph immediately instead of making the
            // Program wait for lease expiry. A non-terminal graph is left to
            // the execution supervisor's recovery path; its actual Agent
            // worker owns the rolling claim heartbeat for exactly its future.
            let orphaned_claims = projection
                .tasks
                .values()
                .filter(|task| task.status == AgenticTaskStatus::Claimed)
                .filter_map(|task| {
                    task.claim_execution_id
                        .as_ref()
                        .map(|execution_id| (task.task_id.clone(), execution_id.clone()))
                })
                .collect::<Vec<_>>();
            let mut released_orphan = false;
            for (task_ref, execution_id) in orphaned_claims {
                let recovery_reason = match self.graph_state_store().load_async(&execution_id).await
                {
                    Ok(graph) if !agentic_graph_is_terminal(&graph) => continue,
                    Ok(_) => "startup recovered a terminal graph with an unsettled claim",
                    Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => {
                        "startup recovered an orphaned graph reservation"
                    }
                    Err(error) => return Err(error.to_string()),
                };
                let release = AgentActionEnvelope {
                    action_id: format!("startup-release-orphan:{task_ref}:{execution_id}"),
                    actor: harness_contract::agent_action::AgentActorBinding {
                        objective_id: projection.objective_id.clone(),
                        program_id: projection.program_id.clone(),
                        session_id: projection.session_id.clone(),
                        turn_id: projection.turn_id.clone(),
                        root_execution_id: projection.root_execution_id.clone(),
                        required_team_count: projection.required_team_count,
                        objective_summary: projection.objective_summary.clone(),
                        model_lease: projection.model_lease.clone(),
                        permission_ceiling: Some(projection.permission_ceiling),
                        resource_scopes: projection.resource_scopes.clone(),
                        actor_id: "runtime.program-supervisor".to_string(),
                        kind: harness_contract::agent_action::AgentActorKind::Supervisor,
                        execution_id: Some(execution_id),
                        team_id: None,
                        agent_id: None,
                    },
                    expected_revision: None,
                    action: AgentAction::TaskRelease(
                        harness_contract::agent_action::TaskReleaseInput {
                            task_ref,
                            reason: recovery_reason.to_string(),
                        },
                    ),
                };
                let observation = self
                    .agent_action_service()
                    .apply(&release)
                    .map_err(|error| error.to_string())?;
                if observation.status != harness_contract::agent_action::AgentActionStatus::Applied
                {
                    return Err(format!(
                        "orphaned claim recovery was rejected: {:?}",
                        observation.error
                    ));
                }
                released_orphan = true;
            }
            if released_orphan {
                projection = self
                    .agent_action_service()
                    .project(program_id)
                    .map_err(|error| error.to_string())?;
            }
            let context = AgenticDispatchContext {
                session_id: projection.session_id.clone(),
                turn_id: projection.turn_id.clone(),
                model_lease: projection.model_lease.clone(),
                permission_ceiling: projection.permission_ceiling,
                resource_scopes: projection.resource_scopes.clone(),
            };
            let trigger = AgentActionEnvelope {
                action_id: format!("startup-reconcile:{}:{}", program_id, projection.revision),
                actor: harness_contract::agent_action::AgentActorBinding {
                    objective_id: projection.objective_id.clone(),
                    program_id: projection.program_id.clone(),
                    session_id: projection.session_id.clone(),
                    turn_id: projection.turn_id.clone(),
                    root_execution_id: projection.root_execution_id.clone(),
                    required_team_count: projection.required_team_count,
                    objective_summary: projection.objective_summary.clone(),
                    model_lease: projection.model_lease.clone(),
                    permission_ceiling: Some(projection.permission_ceiling),
                    resource_scopes: projection.resource_scopes.clone(),
                    actor_id: "runtime.program-supervisor".to_string(),
                    kind: harness_contract::agent_action::AgentActorKind::Supervisor,
                    execution_id: None,
                    team_id: None,
                    agent_id: None,
                },
                expected_revision: None,
                action: AgentAction::StateInspect(
                    harness_contract::agent_action::StateInspectInput {
                        scope_ref: None,
                        after_revision: None,
                    },
                ),
            };
            let observed_at_ms = now_ms();
            let mut requests = projection
                .tasks
                .values()
                .filter(|task| {
                    task_ready_for_execution(task, observed_at_ms)
                        && dependencies_accepted(&projection, task)
                })
                .map(|task| (task.task_id.clone(), DispatchMode::Execute))
                .chain(
                    projection
                        .tasks
                        .values()
                        .filter(|task| task.status == AgenticTaskStatus::Submitted)
                        .map(|task| (task.task_id.clone(), DispatchMode::Review)),
                )
                .collect::<Vec<_>>();
            requests.sort();
            requests.dedup();
            for (task_ref, mode) in requests {
                match self
                    .dispatch_agentic_task(&projection, &task_ref, mode, &context, &trigger)
                    .await
                {
                    Ok(dispatched) => receipts.extend(dispatched),
                    Err(error) => tracing::warn!(
                        program_id,
                        task_ref,
                        mode = mode.as_str(),
                        %error,
                        "startup recovery deferred one Agent-first followup"
                    ),
                }
            }
        }
        Ok(receipts)
    }

    async fn dispatch_agentic_task(
        self: &Arc<Self>,
        projection: &AgenticProgramProjection,
        task_ref: &str,
        mode: DispatchMode,
        context: &AgenticDispatchContext,
        _trigger: &AgentActionEnvelope,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        let task = projection
            .tasks
            .get(task_ref)
            .ok_or_else(|| format!("agentic_task_not_found:{task_ref}"))?;
        let attempt_generation = match mode {
            DispatchMode::Execute => task.claim_generation,
            DispatchMode::Review => task.review_generation,
        };
        let Some(_flight) = DispatchFlight::acquire(
            self.agentic_dispatch_flights(),
            format!(
                "{}|{}|{}|{}",
                projection.program_id,
                task_ref,
                mode.as_str(),
                attempt_generation
            ),
        ) else {
            return Ok(Vec::new());
        };
        let mut members = eligible_members(projection, task, mode);
        if members.is_empty() {
            return Ok(Vec::new());
        }
        // Parallelism belongs between independent Tasks. Starting every
        // eligible model for the same Task merely burns tokens before the CAS
        // claim can reject all but one. Pick one deterministic executor and
        // one independent reviewer; task-level concurrency remains unbounded
        // by this selection and naturally spreads across roster members.
        members.sort_by_key(|member| member_dispatch_rank(projection, member, task, mode));
        members.truncate(1);
        let mut receipts = Vec::new();
        for member in members {
            // The executing Agent receives its own Team's stable shared
            // context. Cross-Team review keeps the source Team as a separate
            // task provenance ref and must not forge the reviewer's scope.
            let cohort_prompt_package = agentic_shared_prompt_package(projection, &member.team_id)?;
            let admission = resolve_agentic_execution_admission(self, member, task, context)?;
            let catalog_entry = &admission.catalog_entry;
            let graph_id = deterministic_graph_id(
                &projection.program_id,
                task_ref,
                &member.agent_id,
                mode,
                attempt_generation,
            );
            if self.graph_state_store().load_async(&graph_id).await.is_ok() {
                // The deterministic `(task, mode, generation)` outbox was
                // already consumed. A terminal graph is not silently retried;
                // its durable report remains the diagnostic for replanning.
                return Ok(Vec::new());
            }
            let node_id = format!("{graph_id}:agent");
            let run_id = format!("run:{graph_id}");
            // Agent-first work has no arbitrary wall-clock quality deadline.
            // Cancellation, provider/resource admission and the rolling claim
            // fence remain authoritative Runtime controls.
            let deadline_at_ms = u64::MAX;
            let objective = task_objective(projection, task, member, mode);
            let root_graph = projection
                .root_execution_id
                .as_deref()
                .and_then(|execution_id| self.graph_state_store().load(execution_id).ok());
            let root_lineage = root_graph.as_ref().and_then(|graph| graph.lineage.as_ref());
            let root_task_id = root_lineage
                .map(|lineage| lineage.root_task_id.clone())
                .unwrap_or_else(|| task_ref.to_string());
            let parent_task_id = root_lineage.map(|lineage| lineage.task_id.clone());
            let mut resource_scopes = context.resource_scopes.clone();
            for shared_scope in [
                format!("session:{}", projection.session_id),
                format!("program:{}", projection.program_id),
            ] {
                if !resource_scopes.contains(&shared_scope) {
                    resource_scopes.push(shared_scope);
                }
            }
            let intent = AgentTaskIntent {
                selected_agent_id: Some(catalog_entry.agent_id.clone()),
                definition_ref: Some(catalog_entry.definition_ref.clone()),
                granted_capabilities: admission.effective_capabilities,
                principal_id: "runtime.agentic".to_string(),
                source_turn_id: context.turn_id.clone(),
                run_id,
                task_id: task_ref.to_string(),
                root_task_id: root_task_id.clone(),
                parent_task_id,
                session_id: context.session_id.clone(),
                // Mission is a physical Runtime execution scope. The dynamic
                // Program remains the semantic collaboration scope carried in
                // context_refs and must not be forged into the mission registry.
                mission_id: self
                    .mission_runtime()
                    .default_mission_id()
                    .to_string(),
                // Agent-first Teams are dynamic semantic scopes. They are not
                // legacy immutable TeamTemplate executions.
                team_id: None,
                graph_id: graph_id.clone(),
                node_id: node_id.clone(),
                attempt: u32::try_from(attempt_generation.saturating_add(1))
                    .unwrap_or(u32::MAX),
                expected_graph_revision: 0,
                objective,
                // The Program Task's natural-language acceptance remains a
                // semantic contract enforced by task_submit/task_review and
                // the Program supervisor. It is not a machine-verifiable
                // Agent terminal field. Treating it as one makes every valid
                // action-driven worker terminal fail because no Runtime
                // receipt can literally satisfy arbitrary prose.
                required_acceptance: RequiredAcceptance::default(),
                output_acceptance: Vec::new(),
                requires_managed_collaboration_escalation: false,
                acceptance: Vec::new(),
                constraints: vec![
                    "Use Agent actions for collaboration state; prose never changes Program truth"
                        .to_string(),
                    "Put long reasoning in normal model content or files; tool JSON carries only compact references"
                        .to_string(),
                ],
                context_refs: vec![
                    format!("agentic_program:{}", projection.program_id),
                    format!("agentic_team:{}", member.team_id),
                    format!("agentic_task_team:{}", task.team_id),
                    format!("agentic_member:{}", member.agent_id),
                    format!("agentic_task:{}", task.task_id),
                    format!("agentic_mode:{}", mode.as_str()),
                ],
                evidence_refs: Vec::new(),
                resource_scopes,
                allowed_tools: admission.allowed_tools,
                allowed_skills: admission.allowed_skills,
                permission_ceiling: admission.permission_ceiling,
                model_lease: context.model_lease.clone(),
                budget_lease: ChildExecutionBudgetReservation::single(
                    format!("budget:{graph_id}"),
                    member.agent_id.clone(),
                    format!("agentic-task:{task_ref}"),
                    AGENTIC_TASK_CAPACITY_ESTIMATE_TOKENS,
                    deadline_at_ms,
                    1,
                ),
                deadline_at_ms,
                managed_invocation: None,
                idempotency_key: format!("agentic:{graph_id}:{}", member.agent_id),
            };
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::AgentTask,
                AgentTaskExecutor::KIND,
                serde_json::to_string(&intent).map_err(|error| error.to_string())?,
            );
            node.id = node_id.clone();
            node.idempotency_key = intent.idempotency_key.clone();
            // Physical graph completion only records whether this bounded
            // Agent invocation ran successfully. Business acceptance belongs
            // to the durable Program Task and its independent review chain.
            node.acceptance.criteria.clear();
            node.resource_scopes = intent.resource_scopes.clone();

            let mut graph =
                ExecutionGraph::new(format!("Agent-first {} for {}", mode.as_str(), task.title));
            graph.id = graph_id.clone();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: context.session_id.clone(),
                turn_id: context.turn_id.clone(),
                root_task_id,
                task_id: task_ref.to_string(),
                generation: 1,
            });
            if let (Some(root_execution_id), Some(root_graph)) =
                (projection.root_execution_id.as_deref(), root_graph.as_ref())
            {
                if let Some(parent_node) = root_graph
                    .nodes
                    .iter()
                    .find(|node| node.kind == ExecutionNodeKind::InlineModel)
                    .or_else(|| root_graph.nodes.first())
                {
                    graph.parent_execution = Some(ExecutionParentBinding {
                        execution_id: root_execution_id.to_string(),
                        node_id: parent_node.id.clone(),
                    });
                }
            }
            graph
                .node_statuses
                .insert(node_id.clone(), ExecutionNodeStatus::Planned);
            graph.nodes.push(node);
            let mut graph = self
                .compile_graph_agent_intents(graph)
                .map_err(|error| error.to_string())?;
            let compiled_node = graph
                .nodes
                .iter_mut()
                .find(|node| node.id == node_id)
                .ok_or_else(|| format!("agentic_compiled_node_not_found:{node_id}"))?;
            let mut packet = serde_json::from_str::<AgentTaskPacket>(&compiled_node.payload_ref)
                .map_err(|error| format!("agentic_compiled_packet_invalid:{error}"))?;
            packet.cohort_prompt_package = Some(cohort_prompt_package.clone());
            packet.validate_cohort_prompt_package()?;
            compiled_node.payload_ref =
                serde_json::to_string(&packet).map_err(|error| error.to_string())?;
            attach_agentic_display(&mut graph, member, task, &projection.program_id)?;
            if let Err(error) = self
                .execution_supervisor()
                .submit(
                    graph,
                    ExecutionGraphCommand::Start {
                        expected_revision: 0,
                    },
                )
                .await
            {
                return Err(error.to_string());
            }
            receipts.push(AgenticDispatchReceipt {
                graph_id,
                task_ref: task_ref.to_string(),
                agent_ref: member.agent_id.clone(),
                mode: mode.as_str().to_string(),
            });
        }
        Ok(receipts)
    }
}

#[cfg(test)]
mod cohort_tests {
    use super::*;

    fn program() -> AgenticProgramProjection {
        let mut projection = AgenticProgramProjection::empty("program-cache", "objective-cache");
        projection.session_id = "session-cache".to_string();
        projection.objective_summary = "Compare the candidate architectures".to_string();
        projection.teams.insert(
            "team-cache".to_string(),
            crate::AgenticTeamProjection {
                team_id: "team-cache".to_string(),
                name: "Architecture Review".to_string(),
                mission: "Produce a shared evidence-backed recommendation".to_string(),
                objective: Some("Review correctness and operability".to_string()),
                topic_ref: "topic:program-cache".to_string(),
                created_by: "root:session-cache".to_string(),
                member_ids: Vec::new(),
                task_ids: Vec::new(),
            },
        );
        projection
    }

    #[test]
    fn shared_prefix_ignores_dynamic_revision_agent_task_and_claim_state() {
        let base = program();
        let expected = agentic_shared_prompt_package(&base, "team-cache").expect("base package");

        let mut dynamic = base.clone();
        dynamic.revision = 41;
        dynamic.agents.insert(
            "agent-dynamic".to_string(),
            AgentMemberProjection {
                agent_id: "agent-dynamic".to_string(),
                team_id: "team-cache".to_string(),
                role: "Reviewer".to_string(),
                mission: "Review one task".to_string(),
                required_capabilities: vec!["read".to_string()],
                invited_by: "root:session-cache".to_string(),
            },
        );
        dynamic.tasks.insert(
            "task-dynamic".to_string(),
            AgenticTaskProjection {
                task_id: "task-dynamic".to_string(),
                team_id: "team-cache".to_string(),
                title: "Dynamic task".to_string(),
                objective: "Dynamic objective".to_string(),
                acceptance: "Dynamic acceptance".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Claimed,
                claimant: Some("agent-dynamic".to_string()),
                claim_generation: 7,
                claim_execution_id: Some("execution-dynamic".to_string()),
                claimed_at_ms: Some(1),
                lease_expires_at_ms: Some(2),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 0,
                failed_review_attempts: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
            },
        );

        let actual =
            agentic_shared_prompt_package(&dynamic, "team-cache").expect("dynamic package");
        assert_eq!(actual, expected);
        let rendered = actual.render_user_messages().join("\n");
        for forbidden in [
            "agent-dynamic",
            "task-dynamic",
            "execution-dynamic",
            "claim_generation",
            "program_revision",
        ] {
            assert!(!rendered.contains(forbidden), "leaked {forbidden}");
        }
    }

    #[test]
    fn shared_prefix_digest_changes_only_when_shared_semantics_change() {
        let base = program();
        let expected = agentic_shared_prompt_package(&base, "team-cache").expect("base package");
        let mut changed = base;
        changed
            .teams
            .get_mut("team-cache")
            .expect("team")
            .mission
            .push_str(" with an adversarial check");
        let actual =
            agentic_shared_prompt_package(&changed, "team-cache").expect("changed package");
        assert_ne!(actual.digest, expected.digest);
    }

    #[test]
    fn shared_prefix_body_is_bounded_before_provider_dispatch() {
        let mut oversized = program();
        oversized.objective_summary = "o".repeat(100_000);
        let team = oversized.teams.get_mut("team-cache").expect("team");
        team.name = "n".repeat(50_000);
        team.mission = "m".repeat(50_000);
        team.objective = Some("x".repeat(50_000));
        team.topic_ref = "t".repeat(50_000);

        let package =
            agentic_shared_prompt_package(&oversized, "team-cache").expect("bounded package");
        assert!(package.packets[0].content.chars().count() < 30_000);
        assert!(package.packets[0].content.contains("[bounded by Runtime]"));
    }
}
