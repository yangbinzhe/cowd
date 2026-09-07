use super::*;

const CLAIM_HEARTBEAT_INTERVAL_MS: u64 = super::super::work_market::CLAIM_LEASE_MS / 3;

pub(super) fn agentic_claim_actor(
    projection: &AgenticProgramProjection,
    member: &AgentMemberProjection,
    task_ref: &str,
    execution_id: &str,
) -> harness_contract::agent_action::AgentActorBinding {
    let team_id = projection
        .tasks
        .get(task_ref)
        .map(|task| task.team_id.clone())
        .unwrap_or_default();
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
        actor_id: member.agent_id.clone(),
        kind: harness_contract::agent_action::AgentActorKind::Agent,
        execution_id: Some(execution_id.to_string()),
        team_id: (!team_id.is_empty()).then_some(team_id),
        agent_id: Some(member.agent_id.clone()),
    }
}

pub(super) fn agentic_graph_is_terminal(graph: &ExecutionGraph) -> bool {
    !graph.node_statuses.is_empty()
        && graph
            .node_statuses
            .values()
            .copied()
            .all(ExecutionNodeStatus::is_terminal)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AgenticClaimDriverState {
    AwaitingAgentClaim,
    Owned,
    Stop,
}

pub(super) async fn agentic_claim_heartbeat_state(
    services: Weak<RuntimeServices>,
    task_ref: &str,
    execution_id: &str,
    actor: &harness_contract::agent_action::AgentActorBinding,
) -> AgenticClaimDriverState {
    let Some(services) = services.upgrade() else {
        return AgenticClaimDriverState::Stop;
    };
    let graph = match services.graph_state_store().load_async(execution_id).await {
        Ok(graph) if !agentic_graph_is_terminal(&graph) => graph,
        _ => return AgenticClaimDriverState::Stop,
    };
    debug_assert_eq!(graph.id, execution_id);
    let projection = match services.agent_action_service().project(&actor.program_id) {
        Ok(projection) => projection,
        Err(_) => return AgenticClaimDriverState::Stop,
    };
    let Some(task) = projection.tasks.get(task_ref) else {
        return AgenticClaimDriverState::Stop;
    };
    if matches!(
        task.status,
        AgenticTaskStatus::Published | AgenticTaskStatus::Rework
    ) && task.claimant.is_none()
        && task.claim_execution_id.is_none()
    {
        return AgenticClaimDriverState::AwaitingAgentClaim;
    }
    if task.status == AgenticTaskStatus::Claimed
        && task.claimant.as_deref() == actor.agent_id.as_deref()
        && task.claim_execution_id.as_deref() == Some(execution_id)
    {
        return AgenticClaimDriverState::Owned;
    }
    AgenticClaimDriverState::Stop
}

pub(super) async fn renew_agentic_claim_if_active(
    services: Weak<RuntimeServices>,
    task_ref: &str,
    execution_id: &str,
    actor: &harness_contract::agent_action::AgentActorBinding,
) -> bool {
    if agentic_claim_heartbeat_state(services.clone(), task_ref, execution_id, actor).await
        != AgenticClaimDriverState::Owned
    {
        return false;
    }
    let Some(services) = services.upgrade() else {
        return false;
    };
    let action_service = services.agent_action_service();
    let projection = match action_service.project(&actor.program_id) {
        Ok(projection) => projection,
        Err(_) => return false,
    };
    let Some(task) = projection.tasks.get(task_ref) else {
        return false;
    };
    if task.status != AgenticTaskStatus::Claimed
        || task.claimant.as_deref() != actor.agent_id.as_deref()
        || task.claim_execution_id.as_deref() != Some(execution_id)
    {
        return false;
    }
    let action_id = format!(
        "runtime-claim-heartbeat:{execution_id}:{}",
        task.lease_expires_at_ms.unwrap_or_default()
    );
    services
        .submit_agent_action(&AgentActionEnvelope {
            action_id,
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskClaim(harness_contract::agent_action::TaskClaimInput {
                task_ref: task_ref.to_string(),
                reason: Some("physical Agent graph remains active".to_string()),
            }),
        })
        .await
        .is_ok_and(|observation| {
            observation.status == harness_contract::agent_action::AgentActionStatus::Applied
        })
}

pub(crate) struct AgenticClaimHeartbeatGuard {
    pub(super) task: tokio::task::JoinHandle<()>,
}

impl Drop for AgenticClaimHeartbeatGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start the lease keeper from inside the real Agent worker. The returned
/// guard must live for exactly that physical execute future; dropping it on
/// success, failure, cancellation or task abort stops all renewal.
pub(crate) fn start_agentic_claim_heartbeat(
    services: Weak<RuntimeServices>,
    packet: &AgentTaskPacket,
) -> Result<Option<AgenticClaimHeartbeatGuard>, String> {
    let Some(agentic) = packet.agentic_binding.as_ref() else {
        return Ok(None);
    };
    let harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } = &agentic.focus
    else {
        return Ok(None);
    };
    let task_ref = task_ref.clone();
    let program_id = agentic.program_id.as_str();
    let agent_id = agentic.agent_id.as_str();
    let execution_id = packet.graph_id().to_string();
    let Some(runtime) = services.upgrade() else {
        return Ok(None);
    };
    let projection = runtime
        .agent_action_service()
        .project(program_id)
        .map_err(|error| error.to_string())?;
    let member = projection
        .agents
        .get(agent_id)
        .ok_or_else(|| format!("Agent-first Program has no bound member `{agent_id}`"))?;
    if projection
        .membership_for(agent_id, &agentic.team_id)
        .is_none_or(|membership| membership.membership_id != agentic.membership_id)
    {
        return Err(
            "Agent-first heartbeat packet membership no longer matches Program".to_string(),
        );
    }
    let actor = agentic_claim_actor(&projection, member, &task_ref, &execution_id);
    let commits = runtime.event_store().subscribe_commits();
    drop(runtime);
    let task = tokio::spawn(async move {
        run_agentic_claim_heartbeat(services, task_ref, execution_id, actor, commits).await;
    });
    Ok(Some(AgenticClaimHeartbeatGuard { task }))
}

pub(super) async fn run_agentic_claim_heartbeat(
    services: Weak<RuntimeServices>,
    task_ref: String,
    execution_id: String,
    actor: harness_contract::agent_action::AgentActorBinding,
    mut commits: tokio::sync::watch::Receiver<u64>,
) {
    // Program commits wake this guard immediately when the Agent claims. No
    // polling timer is needed while unclaimed: the physical worker owns this
    // guard and aborts it on every exit path.
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_millis(
        CLAIM_HEARTBEAT_INTERVAL_MS,
    ));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` ticks immediately; consume that tick so Runtime never
    // renews before the Agent has claimed.
    heartbeat.tick().await;
    loop {
        match agentic_claim_heartbeat_state(services.clone(), &task_ref, &execution_id, &actor)
            .await
        {
            AgenticClaimDriverState::AwaitingAgentClaim => {
                if commits.changed().await.is_err() {
                    return;
                }
            }
            AgenticClaimDriverState::Owned => {
                tokio::select! {
                    changed = commits.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    _ = heartbeat.tick() => {
                        if !renew_agentic_claim_if_active(
                            services.clone(),
                            &task_ref,
                            &execution_id,
                            &actor,
                        )
                        .await
                        {
                            return;
                        }
                    }
                }
            }
            AgenticClaimDriverState::Stop => return,
        }
    }
}
