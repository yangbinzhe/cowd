use super::*;
use super::dispatch::execution_graph_id;

/// How many times one Task's lease may be renewed before the keeper gives up.
///
/// Renewal exists to keep a live claim from expiring while the Agent works. It is
/// not a way to keep a claim alive forever: an Agent that claimed a Task and then
/// stopped making progress left this keeper renewing for minutes in one live run,
/// while the root turn waited for a Program closure that never came. Once the cap
/// is reached the lease is allowed to expire, which is a path Runtime already
/// owns - the Task becomes reclaimable (`task_claim_expired`) rather than stuck.
fn claim_heartbeat_renewal_cap() -> u32 {
    std::env::var("COWD_AGENTIC_CLAIM_RENEWAL_CAP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12)
}

fn claim_heartbeat_interval_ms() -> u64 {
    super::super::work_market::claim_lease_ms() / 3
}

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
        exact_team_count: None,
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
    // A claim records the id the Agent was admitted under, which is an agent-run
    // id (`run:<graph_id>`). The execution-graph store keys on the graph id
    // alone, so looking the run id up there can never succeed: one live run
    // failed the same `task_review` one hundred and twenty-two times on exactly
    // that mismatch while the graph it named existed the whole time.
    let graph = match services
        .graph_state_store()
        .load_async(execution_graph_id(execution_id))
        .await
    {
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
    if actor.agent_id.as_deref().is_some_and(|agent| {
        projection.declined_task_opportunity(task_ref, agent, None, Some(execution_id))
    }) {
        return AgenticClaimDriverState::Stop;
    }
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
    // A lease renewal is control-plane bookkeeping, not a semantic Program
    // mutation. Appending one compact in-place event avoids the full governed
    // action path (authorization, validation, projection apply and follow-up
    // dispatch) on every heartbeat, which dominated large runs where task_claim
    // accounted for ~92% of all actions.
    //
    // It must be resilient: a single CAS conflict under concurrent Program
    // writes must NOT terminate the heartbeat guard, or the claim would silently
    // expire and every later submit would be rejected as `task_claim_expired`.
    // Re-read the revision and retry a bounded number of times.
    const MAX_ATTEMPTS: u8 = 8;
    let stream_id = format!("agentic-program:{}", actor.program_id);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let generation = action_service
            .project(&actor.program_id)
            .map(|projection| projection.revision)
            .unwrap_or_default();
        let event = crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: stream_id.clone(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.task_lease_renewed".to_string(),
                status: Some("renewed".to_string()),
                actor: actor.agent_id.clone(),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "task_ref": task_ref,
                    "agent_id": actor.agent_id,
                    "execution_id": execution_id,
                }),
            },
            idempotency_key: Some(format!("claim-lease-renewed:{execution_id}:{generation}")),
            schema_version: 1,
        };
        match services
            .event_store()
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!(
                    "claim-lease-renewed:{}:{execution_id}:{generation}",
                    actor.program_id
                ),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision: generation,
                }],
                events: vec![event],
            }) {
            Ok(_) => return true,
            Err(crate::RuntimeEventStoreError::StaleRevision { .. }) if attempt < MAX_ATTEMPTS => {
                continue;
            }
            Err(error) => {
                tracing::debug!(%error, "agentic claim heartbeat renewal deferred");
                return false;
            }
        }
    }
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
        claim_heartbeat_interval_ms(),
    ));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // `interval` ticks immediately; consume that tick so Runtime never
    // renews before the Agent has claimed.
    heartbeat.tick().await;
    let mut renewals: u32 = 0;
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
                // A claim that is never worked on must not be kept alive by its
                // own keeper. Each renewal here means the Task is still `Claimed`
                // and nothing moved it forward, so after the cap the lease is
                // allowed to expire instead of being renewed forever.
                renewals += 1;
                if renewals > claim_heartbeat_renewal_cap() {
                    tracing::debug!(
                        %task_ref,
                        renewals,
                        "agentic claim heartbeat stopped renewing; the lease is allowed to expire"
                    );
                    return;
                }
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

#[cfg(test)]
mod renewal_cap_tests {
    use super::*;

    /// The keeper renews while a Task stays `Claimed`, so without a finite cap it
    /// renews forever. One live run spent twenty-three minutes after its last
    /// real action renewing a claim nobody was working on, while the root turn
    /// waited for a closure that never came. The cap is what makes the keeper
    /// terminate and hands the lease back to the reclaim path Runtime already has.
    #[test]
    fn the_renewal_cap_is_finite_and_positive() {
        assert!(
            claim_heartbeat_renewal_cap() > 0,
            "a zero cap would expire a lease before the Agent could work"
        );
        assert!(
            claim_heartbeat_renewal_cap() <= 64,
            "a cap this large is no cap at all"
        );
    }
}
