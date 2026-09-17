use super::super::program::AgenticTaskAttemptProjection;
use super::*;
use super::{admission::*, heartbeat::agentic_graph_is_terminal, helpers::*};
use harness_contract::agent::{
    AgenticExecutionBinding, AgenticExecutionFocus, CohortPromptPackage, CohortPromptPacket,
};

// Accounting prediction only. `ParentExecutionBudgetLedger::reserve_provider`
// records overruns and borrowing but never rejects or shrinks an Agent model
// request, so this estimate cannot become a business-execution budget gate.
const AGENTIC_TASK_CAPACITY_ESTIMATE_TOKENS: u64 = 1_000_000;
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
        projection.objective_summary.trim(),
        team.name.trim(),
        team.mission.trim(),
        team_objective.trim(),
        team.topic_ref.trim(),
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
        // Construct the RAII owner only for the winner. `then_some` eagerly
        // constructs and drops the loser, which would remove the winner's
        // still-live reservation from the shared set.
        inserted.then(|| Self { key, active })
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

fn agentic_supervisor_actor(
    projection: &AgenticProgramProjection,
    execution_id: Option<String>,
) -> harness_contract::agent_action::AgentActorBinding {
    harness_contract::agent_action::AgentActorBinding {
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
        execution_id,
        team_id: None,
        agent_id: None,
    }
}

impl RuntimeServices {
    /// Reconsider durable ready work after a physical member exits. Busy
    /// admissions remain in the original journal; no timer or second queue
    /// is needed to discover that the member is free again.
    pub(crate) async fn dispatch_ready_agentic_work(
        self: &Arc<Self>,
        program_id: &str,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        let initial = self
            .agent_action_service()
            .project_snapshot(program_id)
            .map_err(|error| error.to_string())?;
        if initial.status != super::super::program::AgenticProgramStatus::Open {
            return Ok(Vec::new());
        }
        let mut requests = initial
            .tasks
            .values()
            .filter_map(|task| {
                if task_is_ready(&initial, &task.task_id) {
                    Some((task.task_id.clone(), DispatchMode::Execute))
                } else if task.status == AgenticTaskStatus::Submitted {
                    Some((task.task_id.clone(), DispatchMode::Review))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        requests.extend(coordination_requests(&initial));
        let mut receipts = Vec::new();
        let mut failures = Vec::new();
        for (task_ref, mode) in requests {
            // Earlier requests in this same pass may have occupied a member.
            let projection = self
                .agent_action_service()
                .project_snapshot(program_id)
                .map_err(|error| error.to_string())?;
            let trigger = AgentActionEnvelope {
                action_id: format!("worker-exit-reconcile:{program_id}:{}", projection.revision),
                actor: agentic_supervisor_actor(&projection, None),
                expected_revision: None,
                action: AgentAction::StateInspect(
                    harness_contract::agent_action::StateInspectInput {
                        query: None,
                        wait_for_workers: false,
                        scope_ref: None,
                        after_revision: None,
                        page_cursor: None,
                        entry_ref: None,
                    },
                ),
            };
            let context = AgenticDispatchContext {
                session_id: projection.session_id.clone(),
                turn_id: projection.turn_id.clone(),
                model_lease: projection.model_lease.clone(),
                permission_ceiling: projection.permission_ceiling,
                resource_scopes: projection.resource_scopes.clone(),
            };
            match self
                .dispatch_agentic_task(&projection, &task_ref, mode, &context, &trigger)
                .await
            {
                Ok(dispatched) => receipts.extend(dispatched),
                Err(error) => failures.push(format!("{task_ref}:{error}")),
            }
        }
        if failures.is_empty() {
            Ok(receipts)
        } else {
            Err(failures.join("; "))
        }
    }

    /// Common Native/Process terminal reconciliation. A typed decline or
    /// successful semantic action has already removed its own opportunity;
    /// only an abandoned current attempt is a physical failure here.
    pub(crate) async fn settle_abandoned_agentic_attempt(
        self: &Arc<Self>,
        packet: &AgentTaskPacket,
        reason: &str,
    ) -> Result<(), String> {
        use harness_contract::agent_action::AgentAttemptMode;
        let Some(binding) = packet.agentic_binding.as_ref() else {
            return Ok(());
        };
        if let AgenticExecutionFocus::Coordination { wake_ref } = &binding.focus {
            let projection = self
                .agent_action_service()
                .project_snapshot(&binding.program_id)
                .map_err(|error| error.to_string())?;
            let Some(attempt) = projection.coordination_attempt(packet.graph_id()) else {
                return Ok(());
            };
            if attempt.agent_id != binding.agent_id
                || attempt.membership_id != binding.membership_id
            {
                return Err("coordination_settlement_binding_mismatch".into());
            }
            let Some((_, wake)) = projection.coordination_wake_ref(wake_ref) else {
                return Err("coordination_wake_missing".into());
            };
            let task_ref = wake
                .intent
                .as_ref()
                .ok_or("coordination_intent_missing")?
                .task_ref
                .clone();
            let action = if projection.coordination_replied(
                wake_ref,
                packet.graph_id(),
                &binding.agent_id,
            ) {
                AgentAction::TaskRelease(harness_contract::agent_action::TaskReleaseInput {
                    task_ref,
                    reason: "coordination response committed".into(),
                })
            } else {
                AgentAction::TaskAttemptFail(harness_contract::agent_action::TaskAttemptFailInput {
                    task_ref,
                    execution_id: packet.graph_id().into(),
                    mode: AgentAttemptMode::Coordination,
                    reason: reason.into(),
                    retryable: false,
                })
            };
            let observation = self
                .submit_agent_action(&AgentActionEnvelope {
                    action_id: format!(
                        "runtime-coordination-settle:{}:{}",
                        projection.program_id,
                        packet.graph_id()
                    ),
                    actor: agentic_supervisor_actor(&projection, Some(packet.graph_id().into())),
                    expected_revision: None,
                    action,
                })
                .await?;
            if observation.status != harness_contract::agent_action::AgentActionStatus::Applied {
                return Err(format!(
                    "coordination_settlement_rejected:{:?}",
                    observation.error
                ));
            }
            return Ok(());
        }
        let (task_ref, mode) = match &binding.focus {
            AgenticExecutionFocus::TaskExecute { task_ref } => {
                (task_ref, AgentAttemptMode::Execute)
            }
            AgenticExecutionFocus::TaskReview { task_ref } => (task_ref, AgentAttemptMode::Review),
            _ => return Ok(()),
        };
        let projection = self
            .agent_action_service()
            .project_snapshot(&binding.program_id)
            .map_err(|error| error.to_string())?;
        let Some(task) = projection.tasks.get(task_ref) else {
            return Ok(());
        };
        let registered = task
            .active_attempts
            .get(packet.graph_id())
            .is_some_and(|attempt| {
                attempt.agent_id == binding.agent_id
                    && attempt.mode == mode
                    && attempt.membership_id == binding.membership_id
            });
        let own_claim = mode == AgentAttemptMode::Execute
            && task.status == AgenticTaskStatus::Claimed
            && task.claimant.as_deref() == Some(binding.agent_id.as_str())
            && task.claim_execution_id.as_deref() == Some(packet.graph_id());
        if !registered && !own_claim {
            return Ok(());
        }
        if task.status == AgenticTaskStatus::Claimed && !own_claim {
            return Ok(());
        }
        if !matches!(
            task.status,
            AgenticTaskStatus::Published
                | AgenticTaskStatus::Rework
                | AgenticTaskStatus::Claimed
                | AgenticTaskStatus::Submitted
                | AgenticTaskStatus::CancelRequested
        ) {
            return Ok(());
        }
        let envelope = AgentActionEnvelope {
            action_id: format!(
                "runtime-attempt-settle:{}:{}:{}",
                projection.program_id,
                task_ref,
                packet.graph_id()
            ),
            actor: agentic_supervisor_actor(&projection, Some(packet.graph_id().into())),
            expected_revision: None,
            action: AgentAction::TaskAttemptFail(
                harness_contract::agent_action::TaskAttemptFailInput {
                    task_ref: task_ref.clone(),
                    execution_id: packet.graph_id().into(),
                    mode,
                    reason: reason.into(),
                    retryable: true,
                },
            ),
        };
        let observation = self.submit_agent_action(&envelope).await?;
        if observation.status != harness_contract::agent_action::AgentActionStatus::Applied {
            return Err(format!(
                "agentic_attempt_settlement_rejected:{:?}",
                observation.error
            ));
        }
        self.dispatch_agentic_followups(
            &envelope,
            AgenticDispatchContext {
                session_id: projection.session_id.clone(),
                turn_id: projection.turn_id.clone(),
                model_lease: projection.model_lease.clone(),
                permission_ceiling: projection.permission_ceiling,
                resource_scopes: projection.resource_scopes.clone(),
            },
        )
        .await?;
        Ok(())
    }

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
            .project_snapshot(&envelope.actor.program_id)
            .map_err(|error| error.to_string())?;
        let mut requests = Vec::new();
        let mut cancellation_tasks = Vec::new();
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
                cancellation_tasks.push(input.task_ref.clone());
                requests.extend(
                    input
                        .replacement_task_refs
                        .iter()
                        .filter(|task_ref| task_is_ready(&projection, task_ref))
                        .cloned()
                        .map(|task_ref| (task_ref, DispatchMode::Execute)),
                );
            }
            AgentAction::TaskWithdraw(input) => {
                cancellation_tasks.push(input.task_ref.clone());
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
            AgentAction::MessagePublish(input) => {
                if let Some(intent) = input.intent.as_ref() {
                    // Intent is a typed wake signal, not an inferred prose
                    // command. Runtime merely re-runs normal readiness and
                    // admission; the receiving Agent still owns claim/replan.
                    if intent.kind == harness_contract::agent_action::TaskIntentKind::RequestHelp {
                        requests.extend(coordination_requests(&projection));
                    } else if task_is_ready(&projection, &intent.task_ref) {
                        requests.push((intent.task_ref.clone(), DispatchMode::Execute));
                    }
                }
            }
            _ => {}
        }
        if matches!(
            envelope.action,
            AgentAction::AgentInvite(_) | AgentAction::TaskRelease(_)
        ) {
            requests.extend(coordination_requests(&projection));
        }
        requests.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        requests.dedup();

        // A retirement action is durable before this point.  A cancellation
        // command that races a restart therefore leaves the Task explicitly
        // `CancelRequested` and is retried by startup recovery; it must never
        // be reported as if the semantic retirement itself had failed.
        cancellation_tasks.sort();
        cancellation_tasks.dedup();
        for task_ref in cancellation_tasks {
            if let Err(error) = self
                .reconcile_agentic_task_cancellation(&projection, &task_ref)
                .await
            {
                tracing::warn!(
                    program_id = projection.program_id,
                    task_ref,
                    %error,
                    "durable Agent Task cancellation awaits recovery reconciliation"
                );
            }
        }

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
        if !deferred.is_empty() {
            return Err(format!(
                "Agent execution dispatch deferred after durable action commit; {} idempotent dispatch receipt(s) were committed and will be reused on retry: {}",
                receipts.len(),
                deferred.join(" | ")
            ));
        }
        Ok(receipts)
    }

    /// Make a semantic Task retirement control every physical execution that
    /// Runtime admitted for it.  The Program's attempt records cover both
    /// the pre-claim interval and independent review, so a retirement cannot
    /// leave a model or tool effect running behind a `Withdrawn` projection.
    async fn reconcile_agentic_task_cancellation(
        self: &Arc<Self>,
        projection: &AgenticProgramProjection,
        task_ref: &str,
    ) -> Result<(), String> {
        let Some(task) = projection.tasks.get(task_ref) else {
            return Err(format!("agentic_task_not_found:{task_ref}"));
        };
        if task.status != AgenticTaskStatus::CancelRequested {
            return Ok(());
        }
        let mut attempts = task.active_attempts.values().cloned().collect::<Vec<_>>();
        // Old journals written before the effect outbox was introduced still
        // carry the execute fence.  This one-time recovery path does not make
        // that legacy representation authoritative for new work.
        if let Some(execution_id) = task.claim_execution_id.as_ref() {
            if !attempts
                .iter()
                .any(|attempt| &attempt.execution_id == execution_id)
            {
                attempts.push(AgenticTaskAttemptProjection {
                    execution_id: execution_id.clone(),
                    agent_id: task.claimant.clone().unwrap_or_default(),
                    membership_id: String::new(),
                    mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                    generation: task.claim_generation,
                });
            }
        }
        for attempt in &attempts {
            let terminal = match self
                .graph_state_store()
                .load_async(&attempt.execution_id)
                .await
            {
                Ok(graph) if agentic_graph_is_terminal(&graph) => true,
                Ok(_) => {
                    self.cancel_execution_tree(
                        &attempt.execution_id,
                        "Agentic Task was withdrawn or superseded",
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    self.graph_state_store()
                        .load_async(&attempt.execution_id)
                        .await
                        .map(|graph| agentic_graph_is_terminal(&graph))
                        .map_err(|error| error.to_string())?
                }
                Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => {
                    // Supervisor registration is durable before the executor
                    // can start.  No graph means no effect was admitted.
                    true
                }
                Err(error) => return Err(error.to_string()),
            };
            if !terminal {
                return Err(format!(
                    "agentic_task_cancellation_not_terminal:{}:{}",
                    task_ref, attempt.execution_id
                ));
            }
        }
        // All physical effects, including a legacy claim without an outbox
        // entry, must stop before any reducer can finalize retirement.
        for attempt in attempts {
            let current = self
                .agent_action_service()
                .project_snapshot(&projection.program_id)
                .map_err(|error| error.to_string())?;
            if !current.tasks.get(task_ref).is_some_and(|task| {
                task.active_attempts.contains_key(&attempt.execution_id)
                    || task.claim_execution_id.as_deref() == Some(attempt.execution_id.as_str())
            }) {
                continue;
            }
            self.settle_agentic_task_attempt(
                projection,
                task_ref,
                &attempt.execution_id,
                attempt.mode,
                "physical Agent execution cancelled after Task retirement",
                false,
            )
            .await?;
        }
        Ok(())
    }

    async fn settle_agentic_task_attempt(
        self: &Arc<Self>,
        projection: &AgenticProgramProjection,
        task_ref: &str,
        execution_id: &str,
        mode: harness_contract::agent_action::AgentAttemptMode,
        reason: &str,
        retryable: bool,
    ) -> Result<(), String> {
        let envelope = AgentActionEnvelope {
            action_id: format!(
                "runtime-attempt-settle:{}:{}:{}",
                projection.program_id, task_ref, execution_id
            ),
            actor: agentic_supervisor_actor(projection, Some(execution_id.to_string())),
            expected_revision: None,
            action: AgentAction::TaskAttemptFail(
                harness_contract::agent_action::TaskAttemptFailInput {
                    task_ref: task_ref.to_string(),
                    execution_id: execution_id.to_string(),
                    mode,
                    reason: reason.to_string(),
                    retryable,
                },
            ),
        };
        let observation = self.submit_agent_action(&envelope).await?;
        if observation.status != harness_contract::agent_action::AgentActionStatus::Applied {
            return Err(format!(
                "agentic_attempt_settlement_rejected:{}",
                observation
                    .error
                    .map(|error| error.code)
                    .unwrap_or_else(|| "unknown".to_string())
            ));
        }
        Ok(())
    }

    /// Reconcile all durable open Programs after graph recovery. This closes
    /// the crash window between an Agent action commit and physical graph
    /// admission without depending on a model or browser poll.
    pub async fn recover_agentic_programs_on_startup(
        self: &Arc<Self>,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        let excluded_sessions = BTreeSet::new();
        self.recover_agentic_programs_on_startup_excluding_sessions(&excluded_sessions)
            .await
    }

    /// Periodic reconciliation for every open Program: re-admit durable ready
    /// work and settle stranded attempts / outstanding cancellations. This is
    /// the recovery path for a deferred physical dispatch that no worker exit
    /// or model action will retrigger -- e.g. an invited member that had not
    /// started, or a Task that was `Submitted` while its independent Review
    /// graph was never admitted. `DispatchFlight` keeps it idempotent.
    pub async fn reconcile_open_agentic_programs(
        self: &Arc<Self>,
    ) -> Result<usize, String> {
        let excluded_sessions = BTreeSet::new();
        let receipts = self
            .recover_agentic_programs_on_startup_excluding_sessions(&excluded_sessions)
            .await?;
        Ok(receipts.len())
    }

    /// Reconcile open Programs only after their owning Session has hydrated.
    /// Programs for excluded Sessions remain durable and untouched for retry.
    pub async fn recover_agentic_programs_on_startup_excluding_sessions(
        self: &Arc<Self>,
        excluded_sessions: &BTreeSet<String>,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        let streams = self
            .event_store()
            .stream_ids_for_scope(crate::RuntimeEventScope::Program)
            .map_err(|error| error.to_string())?;
        let mut receipts = Vec::new();
        let mut failures = Vec::new();
        for stream in streams {
            let Some(program_id) = stream.strip_prefix("agentic-program:") else {
                continue;
            };
            // A corrupt or unavailable Program is not authority to suppress
            // independent Programs. Keep reconciliation ordered within one
            // Program and report aggregate failure only after checking all.
            let recovered: Result<(), String> = async {
            let mut projection = self
                .agent_action_service()
                .project_snapshot(program_id)
                .map_err(|error| error.to_string())?;
            if excluded_sessions.contains(&projection.session_id) {
                return Ok(());
            }
            let cancellation_tasks = projection
                .tasks
                .values()
                .filter(|task| task.status == AgenticTaskStatus::CancelRequested)
                .map(|task| task.task_id.clone())
                .collect::<Vec<_>>();
            let mut cancellation_reconciled = false;
            for task_ref in cancellation_tasks {
                self.reconcile_agentic_task_cancellation(&projection, &task_ref)
                    .await
                    .map_err(|error| {
                        format!(
                            "startup cancellation reconciliation failed for Program {program_id}, Task {task_ref}: {error}"
                        )
                    })?;
                cancellation_reconciled = true;
            }
            if cancellation_reconciled {
                projection = self
                    .agent_action_service()
                    .project_snapshot(program_id)
                    .map_err(|error| error.to_string())?;
            }
            if projection.status != super::super::program::AgenticProgramStatus::Open {
                return Ok(());
            }
            // An admitted graph that is terminal (or was never persisted)
            // cannot remain an invisible outbox entry.  Settle it through the
            // same Supervisor action used by normal worker failure so later
            // dispatch is based on durable Task truth, never stale memory.
            let stranded_attempts = projection
                .tasks
                .values()
                .flat_map(|task| {
                    task.active_attempts
                        .values()
                        .cloned()
                        .map(move |attempt| (task.task_id.clone(), attempt))
                })
                .collect::<Vec<_>>();
            let mut settled_attempt = false;
            for (task_ref, attempt) in stranded_attempts {
                let reason = match self
                    .graph_state_store()
                    .load_async(&attempt.execution_id)
                    .await
                {
                    Ok(graph) if !agentic_graph_is_terminal(&graph) => continue,
                    Ok(_) => {
                        "startup recovered a terminal Agent graph with an unsettled Task attempt"
                    }
                    Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => {
                        if attempt.mode == harness_contract::agent_action::AgentAttemptMode::Coordination
                            && projection.coordination_wake(attempt.generation).is_some_and(|(_, wake)| projection.coordination_request_current(wake))
                            && projection.coordination_team_id_for(&attempt.agent_id, attempt.generation)
                                .and_then(|team| projection.membership_for(&attempt.agent_id, team))
                                .is_some_and(|membership| membership.membership_id == attempt.membership_id) {
                            // The durable registration is not consumption by a
                            // model. Preserve it for the same graph factory.
                            continue;
                        }
                        "startup recovered an Agent attempt whose graph was never admitted"
                    }
                    Err(error) => return Err(error.to_string()),
                };
                if attempt.mode == harness_contract::agent_action::AgentAttemptMode::Coordination
                    && projection.coordination_wake(attempt.generation).is_some_and(|(_, wake)|
                        projection.coordination_replied(&wake.entry_id, &attempt.execution_id, &attempt.agent_id)) {
                    let release = AgentActionEnvelope {
                        action_id: format!("startup-coordination-reply:{}:{}", program_id, attempt.execution_id),
                        actor: agentic_supervisor_actor(&projection, Some(attempt.execution_id.clone())),
                        expected_revision: None,
                        action: AgentAction::TaskRelease(harness_contract::agent_action::TaskReleaseInput {
                            task_ref: task_ref.clone(), reason: "recovered durable coordination reply".into(),
                        }),
                    };
                    let observation = self.submit_agent_action(&release).await?;
                    if observation.status != harness_contract::agent_action::AgentActionStatus::Applied {
                        return Err(format!("coordination reply recovery rejected: {:?}", observation.error));
                    }
                    settled_attempt = true;
                    continue;
                }
                self.settle_agentic_task_attempt(
                    &projection,
                    &task_ref,
                    &attempt.execution_id,
                    attempt.mode,
                    reason,
                    true,
                )
                .await?;
                settled_attempt = true;
            }
            if settled_attempt {
                projection = self
                    .agent_action_service()
                    .project_snapshot(program_id)
                    .map_err(|error| error.to_string())?;
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
                .filter(|task| task.active_attempts.is_empty())
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
                let observation = self.submit_agent_action(&release).await?;
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
                    .project_snapshot(program_id)
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
                        query: None,
                        wait_for_workers: false,
                        scope_ref: None,
                        after_revision: None,
                        page_cursor: None,
                        entry_ref: None,
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
            requests.extend(coordination_requests(&projection));
            requests.dedup();
            for (task_ref, mode) in requests {
                match self
                    .dispatch_agentic_task(&projection, &task_ref, mode, &context, &trigger)
                    .await
                {
                    Ok(dispatched) => receipts.extend(dispatched),
                    Err(error) => {
                        tracing::warn!(
                            program_id,
                            task_ref,
                            mode = mode.as_str(),
                            %error,
                            "startup recovery failed one Agent-first followup"
                        );
                        failures.push(format!(
                            "program={program_id},task={task_ref},mode={}: {error}",
                            mode.as_str()
                        ));
                    }
                }
            }
            Ok(())
            }.await;
            if let Err(error) = recovered {
                failures.push(format!("program={program_id}: {error}"));
            }
        }
        if !failures.is_empty() {
            return Err(format!(
                "Agent-first startup dispatch reconciliation failed for {} followup(s) after {} idempotent dispatch receipt(s): {}",
                failures.len(),
                receipts.len(),
                failures.join("; ")
            ));
        }
        Ok(receipts)
    }

    pub(super) async fn dispatch_agentic_task(
        self: &Arc<Self>,
        projection: &AgenticProgramProjection,
        task_ref: &str,
        mode: DispatchMode,
        context: &AgenticDispatchContext,
        _trigger: &AgentActionEnvelope,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        // Program status is business truth, not renewed execution authority.
        // Late submissions and startup recovery must respect the owning root's
        // terminal fence, even when its business objective remains unresolved.
        let root_graph = if let Some(root_id) = projection.root_execution_id.as_deref() {
            let graph = self
                .graph_state_store()
                .load_async(root_id)
                .await
                .map_err(|error| format!("agentic_dispatch_root_unavailable:{root_id}:{error}"))?;
            if agentic_graph_is_terminal(&graph) {
                return Ok(Vec::new());
            }
            Some(graph)
        } else {
            None
        };
        // Multi-task action/startup passes may carry a semantic snapshot
        // from before their previous dispatch. Availability is always read
        // from the current durable Program, then fenced again at append.
        let availability = self
            .agent_action_service()
            .project_snapshot(&projection.program_id)
            .map_err(|error| error.to_string())?;
        let task = availability
            .tasks
            .get(task_ref)
            .ok_or_else(|| format!("agentic_task_not_found:{task_ref}"))?;
        let coordination_wake = if let DispatchMode::Coordination(revision) = mode {
            let Some((_, wake)) = availability.coordination_wake(revision) else {
                return Ok(Vec::new());
            };
            if wake
                .intent
                .as_ref()
                .is_none_or(|intent| intent.task_ref != task_ref)
                || !availability.coordination_request_current(wake)
                || wake
                    .coordination
                    .as_ref()
                    .is_some_and(|consumption| consumption.settled)
            {
                return Ok(Vec::new());
            }
            Some(wake)
        } else {
            None
        };
        let resumed = coordination_wake.and_then(|wake| wake.coordination.as_ref());
        if match mode {
            DispatchMode::Execute => !task_is_ready(&availability, task_ref),
            DispatchMode::Review => task.status != AgenticTaskStatus::Submitted,
            DispatchMode::Coordination(_) => false,
        } {
            return Ok(Vec::new());
        }
        let attempt_mode = mode.attempt_mode();
        if task.active_attempts.values().any(|attempt| {
            attempt.mode == attempt_mode
                && resumed.is_none_or(|resume| resume.execution_id != attempt.execution_id)
        }) {
            return Ok(Vec::new());
        }
        let attempt_generation = match mode {
            DispatchMode::Execute => task.claim_generation,
            DispatchMode::Review => task.review_generation,
            DispatchMode::Coordination(revision) => revision,
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
        let mut members = eligible_members(&availability, task, mode)
            .into_iter()
            .filter(|member| resumed.is_none_or(|resume| resume.agent_id == member.agent_id))
            .filter_map(|member| projection.agents.get(&member.agent_id))
            .collect::<Vec<_>>();
        let running_members = self
            .agent_runtime()
            .running_agentic_members(&projection.program_id);
        let member_busy = |member: &AgentMemberProjection| {
            running_members.contains(&member.agent_id)
                || availability.tasks.values().any(|candidate| {
                    candidate.active_attempts.values().any(|attempt| {
                        attempt.agent_id == member.agent_id
                            && resumed
                                .is_none_or(|resume| resume.execution_id != attempt.execution_id)
                    }) || (candidate.status == AgenticTaskStatus::Claimed
                        && candidate.claimant.as_deref() == Some(member.agent_id.as_str()))
                })
        };
        if members.is_empty() {
            tracing::info!(
                program_id = %projection.program_id,
                task_ref = %task.task_id,
                mode = mode.as_str(),
                "agentic dispatch deferred: roster has no eligible member"
            );
            return Ok(Vec::new());
        }
        // Parallelism belongs between independent Tasks. Starting every
        // eligible model for the same Task merely burns tokens before the CAS
        // claim can reject all but one. Pick one deterministic executor and
        // one independent reviewer; task-level concurrency remains unbounded
        // by this selection and naturally spreads across roster members.
        members.sort_by_key(|member| member_dispatch_rank(projection, member, task, mode));
        let mut rejected_members = Vec::new();
        let mut selected = None;
        let mut busy_eligible = false;
        for member in members {
            match resolve_agentic_execution_admission(self, member, task, context, mode) {
                Ok(admission) => {
                    if member_busy(member) {
                        busy_eligible = true;
                        continue;
                    }
                    let Some(member_flight) = DispatchFlight::acquire(
                        self.agentic_dispatch_flights(),
                        format!("member|{}|{}", projection.program_id, member.agent_id),
                    ) else {
                        busy_eligible = true;
                        continue;
                    };
                    selected = Some((member, admission, member_flight));
                    break;
                }
                Err(error) => rejected_members.push(format!("{}: {error}", member.agent_id)),
            }
        }
        if selected.is_none() && (busy_eligible || rejected_members.is_empty()) {
            // Silent deferral: every otherwise-eligible member is busy, or the
            // roster admitted no member at all. Record it so a wedged Program is
            // observable (and can be bounded) instead of only surfacing as the
            // root's indefinite wait.
            tracing::info!(
                program_id = %projection.program_id,
                task_ref = %task.task_id,
                mode = mode.as_str(),
                busy_eligible,
                rejected = rejected_members.len(),
                "agentic dispatch deferred: no free eligible member"
            );
            return Ok(Vec::new());
        }
        let selected = selected.ok_or_else(|| {
            format!(
                "agentic_no_admissible_member:{}:{}: {}",
                task.task_id,
                mode.as_str(),
                rejected_members.join("; ")
            )
        })?;
        let mut receipts = Vec::new();
        for (member, admission, _member_flight) in std::iter::once(selected) {
            let member_team_id = mode
                .team_id(&availability, &member.agent_id, task)
                .ok_or_else(|| {
                    format!(
                        "agentic_member_has_no_active_dispatch_membership:{}:{}",
                        member.agent_id, task.task_id
                    )
                })?;
            // The executing Agent receives its own Team's stable shared
            // context. Cross-Team review keeps the source Team as a separate
            // task provenance ref and must not forge the reviewer's scope.
            let cohort_prompt_package = agentic_shared_prompt_package(projection, member_team_id)?;
            let catalog_entry = &admission.catalog_entry;
            let graph_id = deterministic_graph_id(
                &projection.program_id,
                task_ref,
                &member.agent_id,
                mode,
                attempt_generation,
            );
            if let Some(resume) = resumed {
                if resume.execution_id != graph_id
                    || availability.coordination_attempt(&graph_id).is_none()
                {
                    return Err("coordination_registration_identity_mismatch".into());
                }
            }
            match self.graph_state_store().load_current_async(&graph_id).await {
                Ok(_) => {
                    // Existing durable admission is never replayed, including
                    // terminal graphs awaiting their normal settlement.
                    return Ok(Vec::new());
                }
                Err(crate::execution_core::graph::ExecutionStateStoreError::NotFound(_)) => {}
                Err(error) => {
                    return Err(format!(
                        "agentic_graph_admission_read_failed:{graph_id}:{error}"
                    ))
                }
            }
            let node_id = format!("{graph_id}:agent");
            let run_id = format!("run:{graph_id}");
            // Agent-first work has no arbitrary wall-clock quality deadline.
            // Cancellation, provider/resource admission and the rolling claim
            // fence remain authoritative Runtime controls.
            let deadline_at_ms = u64::MAX;
            let mut objective = task_objective(&availability, task, member, mode);
            // Resolved paths help the model orient itself, but mentioning a
            // path (including in a prohibition or example) does not require
            // reading it. Only the authored Task acceptance defines success;
            // never turn path discovery into extra terminal obligations.
            let path_hints = crate::workspace_scopes::explicit_workspace_resource_scopes(
                self.workspace_root(),
                &task.objective,
                false,
            );
            if !path_hints.is_empty() {
                objective.push_str(&format!(
                    "\nResolved path references relative to workspace {} (orientation only; follow the Task intent and permissions): {}",
                    self.workspace_root().display(),
                    path_hints.join(", ")
                ));
            }
            let root_lineage = root_graph.as_ref().and_then(|graph| graph.lineage.as_ref());
            let root_task_id = root_lineage
                .map(|lineage| lineage.root_task_id.clone())
                .unwrap_or_else(|| task_ref.to_string());
            let parent_task_id = root_lineage.map(|lineage| lineage.task_id.clone());
            let mission_id = self
                .task_aggregate_service()
                .get(&root_task_id)
                .map_err(|error| format!("load root Task mission for Agent dispatch: {error}"))?
                .map(|task| task.mission_id)
                .unwrap_or_else(|| self.mission_runtime().default_mission_id().to_string());
            let mut resource_scopes = context.resource_scopes.clone();
            for shared_scope in [
                format!("session:{}", projection.session_id),
                format!("program:{}", projection.program_id),
            ] {
                if !resource_scopes.contains(&shared_scope) {
                    resource_scopes.push(shared_scope);
                }
            }
            // Business Task references survive continuation. Physical Task
            // aggregates have immutable ingress/root lineage and need their
            // own identity in the newly authorized execution generation.
            let execution_task_id = projection.execution_task_id(task_ref)?;
            let intent = AgentTaskIntent {
                selected_agent_id: Some(catalog_entry.agent_id.clone()),
                definition_ref: Some(catalog_entry.definition_ref.clone()),
                granted_capabilities: admission.effective_capabilities,
                principal_id: "runtime.agentic".to_string(),
                source_turn_id: context.turn_id.clone(),
                run_id,
                task_id: execution_task_id.clone(),
                root_task_id: root_task_id.clone(),
                parent_task_id,
                session_id: context.session_id.clone(),
                // Mission is the physical execution-tree scope. Dynamic Team
                // semantics remain in Program context, while every descendant
                // Task must inherit the root Task's Mission so a canonical
                // root projection can aggregate its lineage without crossing
                // ownership scopes.
                mission_id,
                // Agent-first Teams are dynamic semantic scopes. They are not
                // immutable Agent Definition executions.
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
                acceptance: Vec::new(),
                constraints: vec![
                    "Use Agent actions for collaboration state; prose never changes Program truth"
                        .to_string(),
                    "Put long reasoning in normal model content or files; tool JSON carries only compact references"
                        .to_string(),
                ],
                context_refs: vec![
                    format!("program:{}", projection.program_id),
                    format!("task:{}", task.task_id),
                ],
                agentic_binding: Some(AgenticExecutionBinding {
                    program_id: projection.program_id.clone(),
                    agent_id: member.agent_id.clone(),
                    membership_id: AgenticProgramProjection::membership_id(
                        &member.agent_id,
                        member_team_id,
                    ),
                    team_id: member_team_id.to_string(),
                    task_team_id: task.team_id.clone(),
                    source_spec_revision: projection.revision,
                    focus: match mode {
                        DispatchMode::Execute => AgenticExecutionFocus::TaskExecute {
                            task_ref: task.task_id.clone(),
                        },
                        DispatchMode::Review => AgenticExecutionFocus::TaskReview {
                            task_ref: task.task_id.clone(),
                        },
                        DispatchMode::Coordination(_) => AgenticExecutionFocus::Coordination {
                            wake_ref: coordination_wake.ok_or("coordination_wake_missing")?.entry_id.clone(),
                        },
                    },
                }),
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
                task_id: execution_task_id,
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
            let attempt = AgentActionEnvelope {
                action_id: format!(
                    "runtime-attempt-dispatch:{}:{}:{}:{}:{}",
                    projection.program_id,
                    task_ref,
                    mode.as_str(),
                    attempt_generation,
                    member.agent_id
                ),
                actor: agentic_supervisor_actor(projection, Some(graph_id.clone())),
                expected_revision: None,
                action: AgentAction::TaskAttemptDispatch(
                    harness_contract::agent_action::TaskAttemptDispatchInput {
                        task_ref: task_ref.to_string(),
                        execution_id: graph_id.clone(),
                        agent_ref: member.agent_id.clone(),
                        membership_id: AgenticProgramProjection::membership_id(
                            &member.agent_id,
                            member_team_id,
                        ),
                        mode: mode.attempt_mode(),
                        generation: attempt_generation,
                    },
                ),
            };
            if resumed.is_none() {
                let observation = self.submit_agent_action(&attempt).await?;
                if observation.status != harness_contract::agent_action::AgentActionStatus::Applied
                {
                    return Err(format!(
                        "agentic_attempt_registration_rejected:{}",
                        observation
                            .error
                            .map(|error| error.code)
                            .unwrap_or_else(|| "unknown".to_string())
                    ));
                }
            }
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
                let reason = format!("physical Agent graph admission failed: {error}");
                if matches!(mode, DispatchMode::Coordination(_)) {
                    return Err(format!(
                        "coordination_admission_pending:{graph_id}:{reason}"
                    ));
                }
                self.settle_agentic_task_attempt(
                    projection,
                    task_ref,
                    &graph_id,
                    mode.attempt_mode(),
                    &reason,
                    true,
                )
                .await?;
                return Ok(Vec::new());
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
                lifecycle: Default::default(),
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
                display_name: "Reviewer".to_string(),
                role: "Reviewer".to_string(),
                mission: "Review one task".to_string(),
                required_capabilities: vec!["read".to_string()],
                invited_by: "root:session-cache".to_string(),
                membership_ids: Vec::new(),
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
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
                active_attempts: Default::default(),
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
    fn shared_prefix_preserves_complete_stable_semantics_before_provider_dispatch() {
        let mut oversized = program();
        oversized.objective_summary = "o".repeat(100_000);
        let team = oversized.teams.get_mut("team-cache").expect("team");
        team.name = "n".repeat(50_000);
        team.mission = "m".repeat(50_000);
        team.objective = Some("x".repeat(50_000));
        team.topic_ref = "t".repeat(50_000);

        let package = agentic_shared_prompt_package(&oversized, "team-cache")
            .expect("lossless shared package");
        let content = &package.packets[0].content;
        assert!(content.contains(&"o".repeat(100_000)));
        assert!(content.contains(&"n".repeat(50_000)));
        assert!(content.contains(&"m".repeat(50_000)));
        assert!(content.contains(&"x".repeat(50_000)));
        assert!(content.contains(&"t".repeat(50_000)));
        assert!(!content.contains("[bounded by Runtime]"));
    }
}
