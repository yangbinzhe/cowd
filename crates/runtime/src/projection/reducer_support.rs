use super::*;

pub(super) fn execution_health_entity(
    execution_id: &str,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    full: bool,
) -> ProjectionEntity {
    ProjectionEntity {
        id: format!("execution-health:{execution_id}"),
        kind: "execution_health".to_string(),
        revision: graph.revision,
        status: Some(graph_status(&graph.nodes)),
        summary: Some("derived from canonical execution graph state".to_string()),
        evidence_refs: Vec::new(),
        payload: None,
        detail: full.then(|| {
            serde_json::json!({
                "commit_cursor": graph.commit_cursor,
                "terminal_result_ref": graph.terminal_result_ref,
            })
        }),
    }
}

pub(super) fn activity_binding_health_entities(
    events: &[crate::DurableRuntimeEvent],
    full: bool,
) -> Vec<ProjectionEntity> {
    events
        .iter()
        .filter(|event| {
            super::activity::requires_activity_binding(event) && event.activity_binding().is_none()
        })
        .map(|event| ProjectionEntity {
            id: format!("activity-binding-health:{}", event.event_id),
            kind: "activity_binding_health".to_string(),
            revision: event.sequence,
            status: Some("error".to_string()),
            summary: Some(format!(
                "business lifecycle event `{}` has no Runtime activity binding",
                event.kind
            )),
            evidence_refs: vec![format!("runtime-event:{}", event.event_id)],
            payload: None,
            detail: full.then(|| {
                serde_json::json!({
                    "event_id": event.event_id,
                    "stream_id": event.stream_id,
                    "scope": event.scope,
                    "kind": event.kind,
                    "commit_cursor": event.commit_cursor,
                })
            }),
        })
        .collect()
}

pub(super) fn redaction_revision(context: &ProjectionQueryContext) -> String {
    let mut grants = context.visibility_grants.clone();
    grants.sort();
    grants.dedup();
    let payload = serde_json::to_vec(&(
        context.principal.as_str(),
        context.workspace_id.as_str(),
        &context.session_scopes,
        &context.mission_scopes,
        grants,
        context.detail_scope,
        context.authorization_revision,
    ))
    .unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(payload))
}

pub(super) fn validate_context(
    services: &RuntimeServices,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if context.principal.trim().is_empty()
        || context.authorization_revision == 0
        || context.workspace_id != services.workspace_key()
    {
        return Err(RuntimeServicesError::ProjectionAccessDenied);
    }
    Ok(())
}

pub(super) fn validate_projection_scope(
    scope: &ExecutionProjectionScope,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    validate_session_scope(scope.session_id.as_deref(), context)?;
    validate_mission_scope(scope.mission_id.as_deref(), context)
}

pub(super) fn has_workspace_visibility(context: &ProjectionQueryContext) -> bool {
    context
        .visibility_grants
        .iter()
        .any(|grant| grant == &format!("workspace:{}", context.workspace_id))
}

pub(super) fn validate_session_scope(
    session_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(session_id) = session_id {
        if !has_workspace_visibility(context)
            && !context
                .session_scopes
                .iter()
                .any(|scope| scope == session_id)
        {
            return Err(RuntimeServicesError::ProjectionAccessDenied);
        }
    }
    Ok(())
}

pub(super) fn validate_mission_scope(
    mission_id: Option<&str>,
    context: &ProjectionQueryContext,
) -> Result<(), RuntimeServicesError> {
    if let Some(mission_id) = mission_id {
        if !has_workspace_visibility(context)
            && !context
                .mission_scopes
                .iter()
                .any(|scope| scope == mission_id)
        {
            return Err(RuntimeServicesError::ProjectionAccessDenied);
        }
    }
    Ok(())
}

/// The read scope is derived from durable graph bindings before any domain
/// projection is assembled. This prevents a workspace-wide query from
/// accidentally becoming an execution-wide response.
pub(super) struct ExecutionProjectionScope {
    pub(super) session_id: Option<String>,
    pub(super) mission_id: Option<String>,
    pub(super) task_id: Option<String>,
    pub(super) turn_id: Option<String>,
    pub(super) execution_ids: BTreeSet<String>,
    pub(super) node_ids: BTreeSet<String>,
    pub(super) entity_ids: BTreeSet<String>,
    pub(super) goals: Vec<ProjectionEntity>,
    pub(super) agents: Vec<ProjectionEntity>,
    pub(super) teams: Vec<ProjectionEntity>,
    pub(super) relations: Vec<ProjectionEntity>,
    pub(super) approvals: Vec<ProjectionEntity>,
    pub(super) interventions: Vec<ProjectionEntity>,
    pub(super) child_executions: Vec<ChildExecutionProjection>,
    pub(super) agentic_collaboration:
        harness_contract::projection::AgenticCollaborationProjectionV1,
    /// Canonical descendant graph projections used only by the Runtime
    /// reducer to materialize one inclusive Team/Agent activity tree. This is
    /// not serialized as a second public graph owner.
    pub(super) descendant_graphs: Vec<harness_contract::execution_graph::ExecutionGraphProjection>,
}

impl ExecutionProjectionScope {
    pub(super) fn load(
        services: &RuntimeServices,
        execution_id: &str,
        graph: &harness_contract::execution_graph::ExecutionGraphProjection,
        full: bool,
    ) -> Result<Self, RuntimeServicesError> {
        let (execution_ids, child_executions, node_ids, descendant_graphs) =
            execution_lineage(services, execution_id, graph)?;

        let agent_snapshots = services.agent_runtime().list_for_graphs(&execution_ids);
        let execution_id_list = execution_ids.iter().cloned().collect::<Vec<_>>();
        let tasks = services
            .task_aggregate_service()
            .for_graphs(&execution_id_list)
            .map_err(RuntimeServicesError::Invariant)?;
        let mut matching_tasks = tasks;
        matching_tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        matching_tasks.dedup_by(|left, right| left.task_id == right.task_id);
        let matching_task_ids = matching_tasks
            .iter()
            .map(|task| task.task_id.clone())
            .collect::<BTreeSet<_>>();
        let graph_lineage = graph.lineage.as_ref();
        if matching_tasks.len() > 1 {
            let anchor = &matching_tasks[0];
            let shares_one_scope = matching_tasks.iter().all(|task| {
                task.mission_id == anchor.mission_id && task.root_task_id == anchor.root_task_id
            });
            if !shares_one_scope {
                return Err(RuntimeServicesError::Invariant(format!(
                    "execution lineage `{execution_id}` crosses task scopes: {}",
                    matching_tasks
                        .iter()
                        .map(|task| task.task_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
        }
        // A Team graph is intentionally shared by all role tasks in the same
        // Mission/Session/Turn. Only a graph with one owner can expose a
        // singular task_id at the root.
        let task = (matching_tasks.len() == 1)
            .then(|| matching_tasks.first())
            .flatten();
        let identity = agent_snapshots
            .iter()
            .map(|agent| &agent.execution_identity)
            .find(|identity| {
                identity
                    .graph_id()
                    .is_some_and(|id| execution_ids.contains(id))
            });
        let identity_fallback = matching_tasks.is_empty().then_some(identity).flatten();
        let task_id = graph_lineage
            .map(|lineage| lineage.task_id.clone())
            .or_else(|| task.map(|task| task.task_id.clone()))
            .or_else(|| {
                identity_fallback.and_then(|identity| identity.task_id().map(str::to_owned))
            });
        let mission_id = matching_tasks
            .first()
            .map(|task| task.mission_id.clone())
            .or_else(|| identity.and_then(|identity| identity.mission_id().map(str::to_owned)));
        let session_id = graph_lineage
            .map(|lineage| lineage.session_id.clone())
            .or_else(|| identity.and_then(|identity| identity.session_id().map(str::to_owned)))
            .or_else(|| session_id_from_graph(services, execution_id))
            .or_else(|| {
                matching_tasks
                    .first()
                    .map(|task| task.origin_session_id.clone())
            });
        let turn_id = graph_lineage
            .map(|lineage| lineage.turn_id.clone())
            .or_else(|| identity.and_then(|identity| identity.turn_id().map(str::to_owned)))
            .or_else(|| {
                matching_tasks
                    .first()
                    .map(|task| task.origin_turn_id.clone())
            });
        for agent in &agent_snapshots {
            let identity = &agent.execution_identity;
            // Mission assignment may change while an immutable execution is
            // running; Task and current-Turn lineage remain the authority.
            let task_matches = agent_task_matches_projection_scope(
                matching_tasks.len(),
                &matching_task_ids,
                task_id.as_deref(),
                identity.task_id(),
            );
            if !task_matches || identity.session_id() != session_id.as_deref() {
                return Err(RuntimeServicesError::Invariant(format!(
                    "agent `{}` has lineage inconsistent with execution `{execution_id}`",
                    agent.agent_id
                )));
            }
        }
        let agentic_programs = agentic_programs_for_executions(services, &execution_ids)?;
        let agentic_collaboration = agentic_collaboration_projection(&agentic_programs);
        let collaboration = collaboration_entities(agent_snapshots, &agentic_programs, full);
        let agents = collaboration.agents;
        let teams = collaboration.teams;
        let agent_ids = collaboration.agent_ids;
        let team_ids = collaboration.team_ids;

        let goal_projections = goals_for_executions(services, &execution_ids);
        let goal_ids = goal_projections
            .iter()
            .map(|projection| projection.goal.id.clone())
            .collect::<BTreeSet<_>>();
        let goals = goal_projections
            .iter()
            .map(|projection| ProjectionEntity {
                id: projection.goal.id.clone(),
                kind: "goal".to_string(),
                revision: projection.stream_revision,
                status: Some(projection.goal.phase.clone()),
                summary: Some(projection.goal.objective.clone()),
                evidence_refs: projection.goal.evidence_refs.clone(),
                payload: None,
                detail: full.then(|| serde_json::to_value(projection).unwrap_or_default()),
            })
            .collect();
        let interventions = goal_projections
            .into_iter()
            .flat_map(|projection| projection.interventions.into_iter())
            .enumerate()
            .map(|(index, intervention)| {
                entity_from_value(
                    "intervention",
                    serde_json::to_value(intervention).unwrap_or_default(),
                    index as u64,
                    full,
                )
            })
            .collect();

        let relation_snapshots = session_id
            .as_deref()
            .map(|id| services.session_relations().relations_for(id))
            .unwrap_or_default();
        let relation_ids = relation_snapshots
            .iter()
            .map(|relation| relation.relation_id.clone())
            .collect::<BTreeSet<_>>();
        let relations = entities_from_details(
            "session_relation",
            relation_snapshots
                .into_iter()
                .filter_map(|relation| serde_json::to_value(relation).ok()),
            full,
        );

        let approvals = services.approval_queue().list_for_execution_scope(
            session_id.as_deref(),
            &agent_ids,
            &team_ids,
        );
        let approval_ids = approvals
            .iter()
            .map(|approval| approval.approval_id.clone())
            .collect::<BTreeSet<_>>();
        let approvals = entities_from_details(
            "approval",
            approvals
                .into_iter()
                .filter_map(|approval| serde_json::to_value(approval).ok()),
            full,
        );

        let mut entity_ids = agent_ids;
        entity_ids.extend(matching_task_ids);
        entity_ids.extend(team_ids);
        entity_ids.extend(goal_ids);
        entity_ids.extend(relation_ids);
        entity_ids.extend(approval_ids);
        Ok(Self {
            session_id,
            mission_id,
            task_id,
            turn_id,
            execution_ids,
            node_ids,
            entity_ids,
            goals,
            agents,
            teams,
            relations,
            approvals,
            interventions,
            child_executions,
            agentic_collaboration,
            descendant_graphs,
        })
    }

    pub(super) fn contains_event(&self, event: &crate::DurableRuntimeEvent) -> bool {
        self.contains_activity_event(event)
            || event.refs.iter().any(|reference| {
                reference.kind == "turn" && self.turn_id.as_deref() == Some(reference.id.as_str())
            })
    }

    /// Execution projection is execution/turn-scoped. A Session reference by
    /// itself is deliberately insufficient because one Session can own many
    /// current and historical Executions, possibly across different Missions.
    pub(super) fn contains_activity_event(&self, event: &crate::DurableRuntimeEvent) -> bool {
        self.execution_ids.contains(&event.stream_id)
            || self.execution_ids.iter().any(|execution_id| {
                event
                    .stream_id
                    .starts_with(&format!("{execution_id}:node:"))
            })
            || event.refs.iter().any(|reference| {
                (reference.kind == "execution_graph" && self.execution_ids.contains(&reference.id))
                    || (reference.kind == "execution" && self.execution_ids.contains(&reference.id))
                    || (reference.kind == "execution_node" && self.node_ids.contains(&reference.id))
                    || self.entity_ids.contains(&reference.id)
            })
            || ["goal:", "approval:", "agent:", "task:"]
                .iter()
                .filter_map(|prefix| event.stream_id.strip_prefix(prefix))
                .any(|id| self.entity_ids.contains(id))
    }
}

struct CollaborationEntities {
    agents: Vec<ProjectionEntity>,
    teams: Vec<ProjectionEntity>,
    agent_ids: BTreeSet<String>,
    team_ids: BTreeSet<String>,
}

fn collaboration_entities(
    agent_snapshots: Vec<crate::AgentRunSnapshot>,
    programs: &[crate::AgenticProgramProjection],
    full: bool,
) -> CollaborationEntities {
    let physical_agent_ids = agent_snapshots
        .iter()
        .flat_map(|agent| [agent.agent_id.clone(), agent.run_id.clone()])
        .collect::<BTreeSet<_>>();
    let mut agent_ids = physical_agent_ids.clone();
    agent_ids.extend(
        programs
            .iter()
            .flat_map(|program| program.agents.keys().cloned()),
    );
    let agents = entities_from_details(
        "agent",
        agent_snapshots
            .into_iter()
            .filter_map(|agent| serde_json::to_value(agent).ok()),
        full,
    );
    let team_ids = programs
        .iter()
        .flat_map(|program| program.teams.keys().cloned())
        .collect::<BTreeSet<_>>();
    let teams = Vec::new();
    CollaborationEntities {
        agents,
        teams,
        agent_ids,
        team_ids,
    }
}

fn agentic_collaboration_projection(
    programs: &[crate::AgenticProgramProjection],
) -> harness_contract::projection::AgenticCollaborationProjectionV1 {
    use harness_contract::projection::*;

    AgenticCollaborationProjectionV1 {
        schema_version: 5,
        programs: programs
            .iter()
            .map(|program| {
                let status = match program.status {
                    crate::AgenticProgramStatus::Open => AgenticCollaborationProgramStatus::Open,
                    crate::AgenticProgramStatus::Waiting => {
                        AgenticCollaborationProgramStatus::Waiting
                    }
                    crate::AgenticProgramStatus::CompletionRequested => {
                        AgenticCollaborationProgramStatus::CompletionRequested
                    }
                    crate::AgenticProgramStatus::Draining => {
                        AgenticCollaborationProgramStatus::Draining
                    }
                    crate::AgenticProgramStatus::Verified => {
                        AgenticCollaborationProgramStatus::Verified
                    }
                    crate::AgenticProgramStatus::Partial => {
                        AgenticCollaborationProgramStatus::Partial
                    }
                    crate::AgenticProgramStatus::Blocked => {
                        AgenticCollaborationProgramStatus::Blocked
                    }
                    crate::AgenticProgramStatus::Failed => {
                        AgenticCollaborationProgramStatus::Failed
                    }
                    crate::AgenticProgramStatus::Cancelled => {
                        AgenticCollaborationProgramStatus::Cancelled
                    }
                };
                let teams = program
                    .teams
                    .values()
                    .map(|team| AgenticCollaborationTeamProjectionV1 {
                        team_id: team.team_id.clone(),
                        name: team.name.clone(),
                        mission: team.mission.clone(),
                        objective: team.objective.clone(),
                        topic_ref: team.topic_ref.clone(),
                        created_by: team.created_by.clone(),
                        member_ids: team.member_ids.clone(),
                        task_ids: team.task_ids.clone(),
                        lifecycle: match team.lifecycle {
                            crate::agentic::AgenticTeamLifecycle::Active => {
                                AgenticCollaborationLifecycle::Active
                            }
                            crate::agentic::AgenticTeamLifecycle::Draining => {
                                AgenticCollaborationLifecycle::Draining
                            }
                            crate::agentic::AgenticTeamLifecycle::Retired => {
                                AgenticCollaborationLifecycle::Retired
                            }
                        },
                    })
                    .collect();
                let agents = program
                    .agents
                    .values()
                    .map(|agent| {
                        let mut active_task_refs = program
                            .tasks
                            .values()
                            .filter(|task| {
                                task.claimant.as_deref() == Some(agent.agent_id.as_str())
                                    && matches!(
                                        task.status,
                                        crate::AgenticTaskStatus::Claimed
                                            | crate::AgenticTaskStatus::CancelRequested
                                    )
                            })
                            .map(|task| task.task_id.clone())
                            .collect::<Vec<_>>();
                        let mut history_task_refs = program
                            .tasks
                            .values()
                            .filter(|task| {
                                task.claimant.as_deref() == Some(agent.agent_id.as_str())
                                    && !matches!(
                                        task.status,
                                        crate::AgenticTaskStatus::Claimed
                                            | crate::AgenticTaskStatus::CancelRequested
                                    )
                            })
                            .map(|task| task.task_id.clone())
                            .collect::<Vec<_>>();
                        let mut active_run_refs = program
                            .tasks
                            .values()
                            .flat_map(|task| task.active_attempts.values())
                            .filter(|attempt| attempt.agent_id == agent.agent_id)
                            .map(|attempt| attempt.execution_id.clone())
                            .collect::<Vec<_>>();
                        active_task_refs.sort();
                        active_task_refs.dedup();
                        history_task_refs.sort();
                        history_task_refs.dedup();
                        active_run_refs.sort();
                        active_run_refs.dedup();
                        AgenticCollaborationAgentProjectionV1 {
                            agent_id: agent.agent_id.clone(),
                            display_name: if agent.display_name.trim().is_empty() {
                                agent.role.clone()
                            } else {
                                agent.display_name.clone()
                            },
                            membership_ids: agent.membership_ids.clone(),
                            role: agent.role.clone(),
                            mission: agent.mission.clone(),
                            required_capabilities: agent.required_capabilities.clone(),
                            invited_by: agent.invited_by.clone(),
                            status: if !active_run_refs.is_empty() {
                                AgenticCollaborationAgentStatus::Running
                            } else if !active_task_refs.is_empty() {
                                AgenticCollaborationAgentStatus::Assigned
                            } else if program.active_team_ids_for(&agent.agent_id).is_empty() {
                                AgenticCollaborationAgentStatus::Retired
                            } else {
                                AgenticCollaborationAgentStatus::Idle
                            },
                            active_task_refs,
                            history_task_refs,
                            active_run_refs,
                        }
                    })
                    .collect();
                let memberships = program
                    .memberships
                    .values()
                    .map(|membership| AgenticCollaborationMembershipProjectionV1 {
                        membership_id: membership.membership_id.clone(),
                        agent_id: membership.agent_id.clone(),
                        team_id: membership.team_id.clone(),
                        lifecycle: match membership.lifecycle {
                            crate::agentic::AgenticMembershipLifecycle::Active => {
                                AgenticCollaborationLifecycle::Active
                            }
                            crate::agentic::AgenticMembershipLifecycle::Draining => {
                                AgenticCollaborationLifecycle::Draining
                            }
                            crate::agentic::AgenticMembershipLifecycle::Retired => {
                                AgenticCollaborationLifecycle::Retired
                            }
                        },
                        delegation_ref: membership.delegation_ref.clone(),
                        reason_ref: membership.reason_ref.clone(),
                    })
                    .collect();
                let tasks = program
                    .tasks
                    .values()
                    .map(|task| AgenticCollaborationTaskProjectionV1 {
                        task_id: task.task_id.clone(),
                        team_id: task.team_id.clone(),
                        title: task.title.clone(),
                        objective: task.objective.clone(),
                        acceptance: task.acceptance.clone(),
                        required_capabilities: task.required_capabilities.clone(),
                        obligation_refs: task.obligation_refs.clone(),
                        purpose: task.purpose,
                        execution_requirements: task.execution_requirements.clone(),
                        expertise_hints: task.expertise_hints.clone(),
                        depends_on: task.depends_on.clone(),
                        dependency_resolution: task
                            .depends_on
                            .iter()
                            .map(|dependency_ref| {
                                let (status, blocker_refs) = match program.tasks.get(dependency_ref)
                                {
                                    None => (
                                        AgenticCollaborationDependencyStatus::Invalid,
                                        vec![dependency_ref.clone()],
                                    ),
                                    Some(dependency)
                                        if dependency.status
                                            == crate::AgenticTaskStatus::Withdrawn =>
                                    {
                                        (
                                            AgenticCollaborationDependencyStatus::Invalid,
                                            vec![dependency_ref.clone()],
                                        )
                                    }
                                    Some(_)
                                        if crate::agentic::task_dependency_satisfied(
                                            &program,
                                            dependency_ref,
                                        ) =>
                                    {
                                        (AgenticCollaborationDependencyStatus::Resolved, Vec::new())
                                    }
                                    Some(_) => (
                                        AgenticCollaborationDependencyStatus::Waiting,
                                        vec![dependency_ref.clone()],
                                    ),
                                };
                                AgenticCollaborationDependencyResolutionV1 {
                                    dependency_ref: dependency_ref.clone(),
                                    status,
                                    blocker_refs,
                                }
                            })
                            .collect(),
                        status: match task.status {
                            crate::AgenticTaskStatus::Published => {
                                AgenticCollaborationTaskStatus::Published
                            }
                            crate::AgenticTaskStatus::Claimed => {
                                AgenticCollaborationTaskStatus::Claimed
                            }
                            crate::AgenticTaskStatus::Submitted => {
                                AgenticCollaborationTaskStatus::Submitted
                            }
                            crate::AgenticTaskStatus::Accepted => {
                                AgenticCollaborationTaskStatus::Accepted
                            }
                            crate::AgenticTaskStatus::Rework => {
                                AgenticCollaborationTaskStatus::Rework
                            }
                            crate::AgenticTaskStatus::Blocked => {
                                AgenticCollaborationTaskStatus::Blocked
                            }
                            crate::AgenticTaskStatus::CancelRequested => {
                                AgenticCollaborationTaskStatus::CancelRequested
                            }
                            crate::AgenticTaskStatus::Withdrawn => {
                                AgenticCollaborationTaskStatus::Withdrawn
                            }
                            crate::AgenticTaskStatus::Superseded => {
                                AgenticCollaborationTaskStatus::Superseded
                            }
                        },
                        claimant: task.claimant.clone(),
                        claim_generation: task.claim_generation,
                        claim_execution_id: task.claim_execution_id.clone(),
                        claimed_at_ms: task.claimed_at_ms,
                        lease_expires_at_ms: task.lease_expires_at_ms,
                        active_attempts: task
                            .active_attempts
                            .values()
                            .map(|attempt| AgenticCollaborationTaskAttemptProjectionV1 {
                                execution_id: attempt.execution_id.clone(),
                                agent_id: attempt.agent_id.clone(),
                                membership_id: attempt.membership_id.clone(),
                                mode: attempt.mode,
                                generation: attempt.generation,
                            })
                            .collect(),
                        artifact_refs: task.artifact_refs.clone(),
                        evidence_refs: task.evidence_refs.clone(),
                        unresolved: task.unresolved.clone(),
                        review_reason: task.review_reason.clone(),
                        reviewed_by: task.reviewed_by.clone(),
                        failed_attempts: task.failed_attempts,
                        review_generation: task.review_generation,
                        failed_review_attempts: task.failed_review_attempts,
                        last_failure: task.last_failure.clone(),
                        replacement_task_refs: task.replacement_task_refs.clone(),
                        supersede_evidence_refs: task.supersede_evidence_refs.clone(),
                        superseded_reason: task.superseded_reason.clone(),
                        superseded_by: task.superseded_by.clone(),
                        cancel_requested_by: task.cancel_requested_by.clone(),
                        cancel_reason_ref: task.cancel_reason_ref.clone(),
                        cancel_evidence_refs: task.cancel_evidence_refs.clone(),
                    })
                    .collect();
                let topics = program
                    .topics
                    .iter()
                    .map(
                        |(topic_ref, entries)| AgenticCollaborationTopicProjectionV1 {
                            topic_ref: topic_ref.clone(),
                            entries: entries
                                .iter()
                                .map(|entry| AgenticCollaborationTopicEntryProjectionV1 {
                                    entry_id: entry.entry_id.clone(),
                                    revision: entry.revision,
                                    actor_id: entry.actor_id.clone(),
                                    summary: entry.summary.clone(),
                                    content_ref: entry.content_ref.clone(),
                                    refs: entry.refs.clone(),
                                    recipients: entry.recipients.clone(),
                                    intent: entry.intent.clone(),
                                })
                                .collect(),
                        },
                    )
                    .collect();
                let artifacts = program
                    .artifacts
                    .values()
                    .map(|artifact| AgenticCollaborationArtifactProjectionV1 {
                        artifact_ref: artifact.artifact_ref.clone(),
                        content_ref: artifact.content_ref.clone(),
                        kind: artifact.kind.clone(),
                        title: artifact.title.clone(),
                        relates_to: artifact.relates_to.clone(),
                        committed_by: artifact.committed_by.clone(),
                    })
                    .collect();
                let completion = AgenticCollaborationCompletionProjectionV1 {
                    final_artifact_ref: program.final_artifact_ref.clone(),
                    wait: program.completion_request.as_ref().map(|request| {
                        AgenticCollaborationCompletionWaitProjectionV1 {
                            action_id: request.action_id.clone(),
                            requested_by: request.requested_by.clone(),
                            program_revision: request.program_revision,
                            result_refs: request.result_refs.clone(),
                            primary_artifact_ref: request.primary_artifact_ref.clone(),
                            evidence_refs: request.evidence_refs.clone(),
                            unresolved: request.unresolved.clone(),
                        }
                    }),
                    verdict: program.objective_verdict.as_ref().map(|verdict| {
                        AgenticCollaborationObjectiveVerdictProjectionV1 {
                            goal_id: verdict.goal_id.clone(),
                            goal_revision: verdict.goal_revision,
                            terminal_fence: verdict.terminal_fence.clone(),
                            authority_revision: verdict.authority_revision,
                            kind: verdict.kind,
                        }
                    }),
                };
                AgenticCollaborationProgramProjectionV1 {
                    program_id: program.program_id.clone(),
                    revision: program.revision,
                    status,
                    objective_id: program.objective_id.clone(),
                    objective_summary: program.objective_summary.clone(),
                    session_id: program.session_id.clone(),
                    turn_id: program.turn_id.clone(),
                    root_execution_id: program.root_execution_id.clone(),
                    required_team_count: program.required_team_count,
                    model_lease: program.model_lease.clone(),
                    permission_ceiling: program.permission_ceiling,
                    resource_scopes: program.resource_scopes.clone(),
                    teams,
                    agents,
                    memberships,
                    tasks,
                    topics,
                    artifacts,
                    completion,
                    semantic_refs: AgenticCollaborationSemanticRefsV1 {
                        program_ref: program.program_id.clone(),
                        objective_ref: program.objective_id.clone(),
                        team_refs: program.teams.keys().cloned().collect(),
                        agent_refs: program.agents.keys().cloned().collect(),
                        task_refs: program.tasks.keys().cloned().collect(),
                        topic_refs: program.topics.keys().cloned().collect(),
                        artifact_refs: program.artifacts.keys().cloned().collect(),
                    },
                    unresolved: program.unresolved.clone(),
                }
            })
            .collect(),
    }
}

fn agentic_programs_for_executions(
    services: &RuntimeServices,
    execution_ids: &BTreeSet<String>,
) -> Result<Vec<crate::AgenticProgramProjection>, RuntimeServicesError> {
    let action_service = services.agent_action_service();
    let mut programs = services
        .event_store()
        .stream_ids_for_scope(RuntimeEventScope::Program)
        .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))?
        .into_iter()
        .filter_map(|stream| {
            stream
                .strip_prefix("agentic-program:")
                .map(ToOwned::to_owned)
        })
        .map(|program_id| {
            action_service
                .project_if_exists(&program_id)
                .map_err(|error| RuntimeServicesError::Invariant(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .filter(|program| {
            program
                .root_execution_id
                .as_ref()
                .is_some_and(|execution_id| execution_ids.contains(execution_id))
        })
        .collect::<Vec<_>>();
    programs.sort_by(|left, right| left.program_id.cmp(&right.program_id));
    Ok(programs)
}

fn agent_task_matches_projection_scope(
    matching_task_count: usize,
    matching_task_ids: &BTreeSet<String>,
    singular_task_id: Option<&str>,
    agent_task_id: Option<&str>,
) -> bool {
    if matching_task_count > 1 {
        // A root execution may contain its primary Agent plus several nested
        // Team role Tasks. The graph lineage and shared Mission/Session
        // establish membership; no singular Task owns the whole projection.
        true
    } else if matching_task_ids.is_empty() {
        agent_task_id == singular_task_id
    } else {
        agent_task_id.is_some_and(|id| matching_task_ids.contains(id))
    }
}

pub(super) fn session_id_from_graph(
    services: &RuntimeServices,
    execution_id: &str,
) -> Option<String> {
    let graph = services.graph_state_store().load(execution_id).ok()?;
    graph.nodes.iter().find_map(|node| {
        serde_json::from_str::<serde_json::Value>(&node.payload_ref)
            .ok()
            .and_then(|payload| {
                payload
                    .get("session_id")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .or_else(|| {
                node.payload_ref
                    .strip_prefix("session_handoff:")
                    .and_then(|payload| {
                        serde_json::from_str::<harness_contract::turn::SessionDispatchCommand>(
                            payload,
                        )
                        .ok()
                        .map(|command| command.handoff.source_session_id)
                    })
            })
    })
}

pub(super) fn entities_from_details(
    kind: &str,
    details: impl IntoIterator<Item = serde_json::Value>,
    full: bool,
) -> Vec<ProjectionEntity> {
    details
        .into_iter()
        .enumerate()
        .map(|(index, detail)| entity_from_value(kind, detail, index as u64, full))
        .collect()
}

pub(super) fn entity_from_value(
    kind: &str,
    detail: serde_json::Value,
    revision: u64,
    full: bool,
) -> ProjectionEntity {
    let id = ["id", "agent_id", "team_id", "relation_id", "approval_id"]
        .iter()
        .find_map(|key| detail.get(*key).and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{kind}:{revision}"));
    let status = detail
        .get("status")
        .or_else(|| detail.get("state"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let summary = detail
        .get("summary")
        .or_else(|| detail.get("objective"))
        .or_else(|| detail.get("title"))
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    ProjectionEntity {
        id,
        kind: kind.to_string(),
        revision,
        status,
        summary,
        evidence_refs: detail
            .get("evidence_refs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        payload: None,
        detail: full.then_some(detail),
    }
}

pub(super) fn goals_for_executions(
    services: &RuntimeServices,
    execution_ids: &BTreeSet<String>,
) -> Vec<crate::execution_core::GoalProjection> {
    services
        .event_store()
        .stream_ids_for_scope(RuntimeEventScope::Goal)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|stream| stream.strip_prefix("goal:").map(ToOwned::to_owned))
        .filter_map(|goal_id| services.goal_store().projection(&goal_id).ok().flatten())
        .filter(|projection| {
            projection
                .goal
                .id
                .strip_prefix("goal:")
                .is_some_and(|execution_id| execution_ids.contains(execution_id))
        })
        .collect()
}

/// Resolves the durable execution lineage rooted at `execution_id`. Child
/// graphs retain an immutable parent binding in their canonical graph state;
/// registration atomically writes a reverse relation event for the same fact.
/// Therefore a root projection walks only its durable descendant index and
/// never scans every graph in the runtime or infers containment from prose.
pub(super) fn execution_lineage(
    services: &RuntimeServices,
    execution_id: &str,
    root: &harness_contract::execution_graph::ExecutionGraphProjection,
) -> Result<
    (
        BTreeSet<String>,
        Vec<ChildExecutionProjection>,
        BTreeSet<String>,
        Vec<harness_contract::execution_graph::ExecutionGraphProjection>,
    ),
    RuntimeServicesError,
> {
    let mut execution_ids = BTreeSet::from([execution_id.to_string()]);
    let mut child_executions = Vec::new();
    let mut discovered = vec![execution_id.to_string()];
    let mut lineage_graphs = Vec::new();
    while let Some(parent_execution_id) = discovered.pop() {
        for link in services
            .graph_state_store()
            .child_links(&parent_execution_id)?
        {
            if !execution_ids.insert(link.child_execution_id.clone()) {
                continue;
            }
            let graph = services
                .graph_state_store()
                .projection(&link.child_execution_id)?;
            let parent = graph.parent_execution.as_ref().ok_or_else(|| {
                RuntimeServicesError::Invariant(format!(
                    "lineage index references child graph `{}` without a parent binding",
                    graph.graph_id
                ))
            })?;
            if parent.execution_id != link.parent_execution_id
                || parent.node_id != link.parent_node_id
            {
                return Err(RuntimeServicesError::Invariant(format!(
                    "lineage index disagrees with child graph `{}` parent binding",
                    graph.graph_id
                )));
            }
            child_executions.push(ChildExecutionProjection {
                execution_id: graph.graph_id.clone(),
                parent_execution_id: parent.execution_id.clone(),
                parent_node_id: parent.node_id.clone(),
                revision: graph.revision,
                cursor: graph.commit_cursor,
                status: graph_status(&graph.nodes),
                objective: graph.objective.clone(),
            });
            discovered.push(graph.graph_id.clone());
            lineage_graphs.push(graph);
        }
    }
    child_executions.sort_by(|left, right| left.execution_id.cmp(&right.execution_id));
    let mut node_ids = root
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    for graph in &lineage_graphs {
        node_ids.extend(graph.nodes.iter().map(|node| node.node_id.clone()));
    }
    lineage_graphs.sort_by(|left, right| left.graph_id.cmp(&right.graph_id));
    Ok((execution_ids, child_executions, node_ids, lineage_graphs))
}

pub(super) fn graph_status(
    nodes: &[harness_contract::execution_graph::ExecutionNodeProjection],
) -> String {
    if nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::Failed)
    {
        "failed".to_string()
    } else if nodes.iter().all(|node| node.status.is_terminal()) {
        "terminal".to_string()
    } else if nodes
        .iter()
        .any(|node| node.status == ExecutionNodeStatus::WaitingExternal)
    {
        "waiting_external".to_string()
    } else {
        "running".to_string()
    }
}

pub(super) fn string_payload(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> ExecutionProjectionScope {
        ExecutionProjectionScope {
            session_id: Some("session-1".to_string()),
            mission_id: Some("mission-1".to_string()),
            task_id: Some("task-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            execution_ids: BTreeSet::from(["execution-1".to_string()]),
            node_ids: BTreeSet::new(),
            entity_ids: BTreeSet::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            interventions: Vec::new(),
            child_executions: Vec::new(),
            agentic_collaboration: Default::default(),
            descendant_graphs: Vec::new(),
        }
    }

    fn event(refs: Vec<crate::RuntimeEventRef>) -> crate::DurableRuntimeEvent {
        crate::DurableRuntimeEvent {
            event_id: "event-1".to_string(),
            stream_id: "session:session-1".to_string(),
            sequence: 1,
            scope: RuntimeEventScope::Session,
            kind: "session.event".to_string(),
            status: None,
            actor: None,
            refs,
            payload: serde_json::json!({}),
            created_at_ms: 1,
            commit_cursor: 1,
            transaction_id: "tx-1".to_string(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        }
    }

    #[test]
    fn session_reference_alone_does_not_enter_an_execution_projection() {
        assert!(!scope().contains_event(&event(vec![crate::RuntimeEventRef {
            kind: "session".to_string(),
            id: "session-1".to_string(),
        }])));
    }

    #[test]
    fn exact_turn_reference_enters_the_execution_projection() {
        assert!(scope().contains_event(&event(vec![crate::RuntimeEventRef {
            kind: "turn".to_string(),
            id: "turn-1".to_string(),
        }])));
    }

    #[test]
    fn multi_task_lineage_accepts_the_primary_agent_task() {
        assert!(agent_task_matches_projection_scope(
            2,
            &BTreeSet::from(["team-task-1".to_string(), "team-task-2".to_string()]),
            None,
            Some("primary-agent-task"),
        ));
    }

    #[test]
    fn singular_task_lineage_rejects_an_unrelated_agent_task() {
        assert!(!agent_task_matches_projection_scope(
            1,
            &BTreeSet::from(["task-1".to_string()]),
            Some("task-1"),
            Some("task-other"),
        ));
    }
}
