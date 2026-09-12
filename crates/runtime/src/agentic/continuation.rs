//! Program revision lineage prepared by Runtime and committed with the new root.
use super::program::{AgenticProgramContinuation, AgenticProgramStatus, AgenticTaskStatus};
use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};
use harness_contract::{agent_action::AgentActorBinding, execution_graph::ExecutionGraph};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) fn prepare(
    store: &Arc<RuntimeEventStore>,
    graph: &ExecutionGraph,
    actor: &AgentActorBinding,
) -> Result<(Vec<RuntimeTransactionEventInput>, BTreeMap<String, u64>), String> {
    let binding = graph
        .continuation_binding
        .as_ref()
        .ok_or("missing continuation binding")?;
    crate::session_continuation::graph_continuation_claim_event(binding, &graph.id)?;
    let lineage = graph
        .lineage
        .as_ref()
        .ok_or("missing continuation lineage")?;
    if binding.authorization != harness_contract::turn::ContinuationAuthorization::Authorized
        || actor.actor_id != format!("root:{}", lineage.session_id)
        || actor.execution_id.is_some()
        || actor.team_id.is_some()
        || actor.agent_id.is_some()
        || binding.handoff_id.is_some()
        || binding.source_session_id != lineage.session_id
        || actor.session_id != lineage.session_id
        || actor.turn_id != lineage.turn_id
        || actor.root_execution_id.as_deref() != Some(graph.id.as_str())
        || actor.kind != harness_contract::agent_action::AgentActorKind::Root
        || actor.objective_id
            != harness_contract::agent_action::root_objective_id(
                &lineage.session_id,
                &lineage.turn_id,
            )
        || actor.program_id
            != harness_contract::agent_action::program_id_for_objective(&actor.objective_id)
    {
        return Err("continuation target authorization mismatch".into());
    }
    let source_id = binding
        .team_set_ref
        .strip_prefix("agentic_program:")
        .ok_or("missing source Program")?;
    let source = crate::AgentActionService::new(Arc::clone(store))
        .project(source_id)
        .map_err(|e| e.to_string())?;
    let source_stream = format!("agentic-program:{source_id}");
    let cursor = store
        .list_stream_page_desc(&source_stream, 1, 0)?
        .first()
        .map(|e| e.commit_cursor);
    if source.status == AgenticProgramStatus::Verified
        || source.program_id == actor.program_id
        || source.session_id != binding.source_session_id
        || source.turn_id != binding.source_turn_id
        || source.root_execution_id.as_deref() != Some(binding.source_root_id.as_str())
        || cursor != Some(binding.delivery_revision)
        || actor.required_team_count != source.required_team_count
        || actor.objective_summary != source.objective_summary
    {
        return Err("continuation source changed or identity mismatched".into());
    }
    let successor_key = format!("program-successor:{}", source.program_id);
    if store
        .event_by_idempotency_key("continuation-cas", &successor_key)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err(
            "continuation source already has a successor; resolve its current revision".into(),
        );
    }
    let mut expected = crate::session_continuation::stopped_source_revisions(store, &source)?;
    expected.insert(source_stream, source.revision);
    let target_stream = format!("agentic-program:{}", actor.program_id);
    expected.insert(target_stream.clone(), 0);
    let seed = revision_seed(&source, binding, actor)?;
    let payload = json!({
        "program_id": seed.program_id, "objective_id": seed.objective_id,
        "session_id": seed.session_id, "turn_id": seed.turn_id,
        "root_execution_id": seed.root_execution_id, "required_team_count": seed.required_team_count,
        "objective_summary": seed.objective_summary, "model_lease": seed.model_lease,
        "permission_ceiling": seed.permission_ceiling, "resource_scopes": seed.resource_scopes,
        "continuation_seed": seed,
    });
    let successor = RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: "continuation-cas".into(),
            scope: RuntimeEventScope::Relation,
            kind: "team.continuation.program_successor_claimed.v1".into(),
            status: Some("claimed".into()),
            actor: Some(actor.actor_id.clone()),
            refs: Vec::new(),
            payload: json!({"source_program_id": source.program_id, "root_graph_id": graph.id,
                "program_id": actor.program_id, "binding_digest": binding.binding_digest}),
        },
        idempotency_key: Some(successor_key),
        schema_version: 1,
    };
    Ok((
        vec![
            RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: target_stream,
                    scope: RuntimeEventScope::Program,
                    kind: "agentic.program_opened".into(),
                    status: Some("open".into()),
                    actor: Some(actor.actor_id.clone()),
                    refs: vec![
                        RuntimeEventRef {
                            kind: "agentic_program".into(),
                            id: source.program_id,
                        },
                        RuntimeEventRef {
                            kind: "execution_graph".into(),
                            id: graph.id.clone(),
                        },
                    ],
                    payload,
                },
                idempotency_key: Some("agentic-program-opened".into()),
                schema_version: 1,
            },
            successor,
        ],
        expected,
    ))
}

fn revision_seed(
    source: &super::program::AgenticProgramProjection,
    binding: &harness_contract::turn::CollaborationContinuationBinding,
    actor: &AgentActorBinding,
) -> Result<super::program::AgenticProgramProjection, String> {
    let mut seed = source.clone();
    seed.continuation = Some(AgenticProgramContinuation {
        source_program_id: source.program_id.clone(),
        source_objective_id: source.objective_id.clone(),
        source_root_id: binding.source_root_id.clone(),
        source_revision: source.revision,
        source_status: source.status,
        authorization_generation: source
            .continuation
            .as_ref()
            .map_or(1, |c| c.authorization_generation)
            .checked_add(1)
            .ok_or("authorization generation exhausted")?,
        authorization_revision: binding.authorization_revision,
        binding_digest: binding.binding_digest.clone(),
    });
    seed.program_id = actor.program_id.clone();
    seed.objective_id = actor.objective_id.clone();
    seed.turn_id = actor.turn_id.clone();
    seed.root_execution_id = actor.root_execution_id.clone();
    seed.model_lease = actor.model_lease.clone();
    seed.permission_ceiling = actor
        .permission_ceiling
        .ok_or("missing current permission ceiling")?;
    seed.resource_scopes = actor.resource_scopes.clone();
    seed.status = AgenticProgramStatus::Open;
    seed.revision = 0;
    seed.completion_request = None;
    seed.objective_verdict = None;
    seed.final_artifact_ref = None;
    for task in seed.tasks.values_mut() {
        // A drained graph is not proof of an unknown effect's business outcome.
        // Keep completed facts, but require explicit reconciliation of old claims.
        if matches!(
            task.status,
            AgenticTaskStatus::Claimed | AgenticTaskStatus::CancelRequested
        ) || (!task.active_attempts.is_empty()
            && task.status != AgenticTaskStatus::Accepted
            && !task.status.is_retired())
        {
            task.status = AgenticTaskStatus::Blocked;
            let gap = format!(
                "continuation_effect_reconciliation:{}:{}",
                source.program_id, task.task_id
            );
            if !task.unresolved.contains(&gap) {
                task.unresolved.push(gap);
            }
        }
        task.claim_generation = task
            .claim_generation
            .checked_add(1)
            .ok_or("claim generation exhausted")?;
        task.claimant = None;
        task.claim_execution_id = None;
        task.claimed_at_ms = None;
        task.lease_expires_at_ms = None;
        task.active_attempts.clear();
        // Counters bound an authorization's physical retries, not all future
        // user-authorized attempts. Historical failures remain in the source.
        task.failed_attempts = 0;
        task.failed_review_attempts = 0;
    }
    Ok(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent_action::*;
    use harness_contract::execution_graph::*;
    use harness_contract::turn::ContinuationAuthorization;

    fn actor(turn: &str, root: &str) -> AgentActorBinding {
        let objective_id = root_objective_id("continuation-session", turn);
        AgentActorBinding {
            program_id: program_id_for_objective(&objective_id),
            objective_id,
            session_id: "continuation-session".into(),
            turn_id: turn.into(),
            root_execution_id: Some(root.into()),
            required_team_count: 1,
            objective_summary: "Deliver the original full report".into(),
            model_lease: "current-model".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec!["read:.".into()],
            actor_id: "root:continuation-session".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        }
    }

    fn graph(id: &str, turn: &str) -> ExecutionGraph {
        let mut graph = ExecutionGraph::new("original report");
        graph.id = id.into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "continuation-session".into(),
            turn_id: turn.into(),
            root_task_id: format!("task-{turn}"),
            task_id: format!("task-{turn}"),
            generation: 1,
        });
        let mut node =
            ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "source");
        node.id = format!("{id}:model");
        graph.nodes.push(node);
        graph
    }

    fn source(
        store: &Arc<RuntimeEventStore>,
        commits: &crate::ExecutionCommitService,
    ) -> (AgentActorBinding, String) {
        let root = actor("old-turn", "old-root");
        commits
            .register_graph(graph("old-root", "old-turn"))
            .unwrap();
        let service = crate::AgentActionService::new(Arc::clone(store));
        let apply = |id: &str, action| {
            let receipt = service
                .apply(&AgentActionEnvelope {
                    action_id: id.into(),
                    actor: root.clone(),
                    expected_revision: None,
                    action,
                })
                .unwrap();
            assert_eq!(receipt.status, AgentActionStatus::Applied, "{receipt:?}");
            receipt
        };
        let team = apply(
            "team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Evidence".into(),
                mission: "complete report".into(),
                objective: None,
            }),
        )
        .changed_refs[0]
            .clone();
        let task = apply(
            "task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Existing contribution".into(),
                objective: "preserve existing findings".into(),
                acceptance: "source supported".into(),
                required_capabilities: vec!["read".into()],
                depends_on: vec![],
                obligation_refs: vec![],
                purpose: Default::default(),
                execution_requirements: vec![],
                expertise_hints: vec![],
            }),
        )
        .changed_refs[0]
            .clone();
        (root, task)
    }

    fn stop(commits: &crate::ExecutionCommitService, store: &Arc<RuntimeEventStore>, id: &str) {
        let graph = crate::ExecutionGraphStateStore::new(Arc::clone(store))
            .load(id)
            .unwrap();
        commits
            .apply_command(
                &graph,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: graph.revision,
                    reason: "fixture source stopped".into(),
                },
            )
            .unwrap();
    }

    #[test]
    fn unfinished_continuation_is_atomic_fenced_and_recoverable() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
        let (old, task) = source(&store, &commits);
        assert!(crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "new-turn"
        )
        .is_err());
        stop(&commits, &store, "old-root");
        let before = crate::AgentActionService::new(Arc::clone(&store))
            .project(&old.program_id)
            .unwrap();
        let (candidate, revision) = crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "new-turn",
        )
        .unwrap()
        .unwrap();
        let binding = crate::session_continuation::compile_continuation_binding(
            &candidate,
            "new-ingress",
            revision,
            ContinuationAuthorization::Authorized,
            7,
        )
        .unwrap();
        let mut next = graph("new-root", "new-turn");
        next.continuation_binding = Some(binding.clone());
        let mut current = actor("new-turn", "new-root");
        current.model_lease = "replacement-model".into();
        assert!(
            commits.register_graph(next.clone()).is_err(),
            "cannot register an unfinished continuation without inherited Program facts"
        );
        let receipt = commits
            .register_graph_with_continuation(next.clone(), Some(current.clone()))
            .unwrap();
        let cold = crate::AgentActionService::new(Arc::clone(&store));
        let inherited = cold.project(&current.program_id).unwrap();
        assert_eq!(inherited.tasks[&task].task_id, task);
        assert_eq!(inherited.tasks.len(), before.tasks.len());
        assert_eq!(inherited.teams, before.teams);
        assert_eq!(inherited.model_lease, "replacement-model");
        assert_eq!(inherited.status, AgenticProgramStatus::Open);
        assert!(inherited.objective_verdict.is_none());
        assert_eq!(
            inherited
                .continuation
                .as_ref()
                .unwrap()
                .authorization_generation,
            2
        );
        assert_eq!(cold.project(&old.program_id).unwrap(), before);
        assert_eq!(
            cold.project(&current.program_id).unwrap(),
            inherited,
            "hot and cold projections agree"
        );
        let seed = store
            .list_stream(&format!("agentic-program:{}", current.program_id))
            .unwrap()
            .remove(0);
        let planned = store.list_stream(&receipt.graph.id).unwrap().remove(0);
        assert_eq!(seed.transaction_id, planned.transaction_id);
        assert_eq!(seed.commit_cursor, planned.commit_cursor);
        assert!(matches!(
            commits.register_graph_with_continuation(next, Some(current)),
            Err(crate::execution_core::graph::ExecutionCommitError::AlreadyAppliedSame { .. })
        ));
        let mut rival = graph("rival-root", "rival-turn");
        rival.continuation_binding = Some(
            crate::session_continuation::compile_continuation_binding(
                &candidate,
                "rival-ingress",
                revision,
                ContinuationAuthorization::Authorized,
                7,
            )
            .unwrap(),
        );
        assert!(commits
            .register_graph_with_continuation(rival, Some(actor("rival-turn", "rival-root")))
            .is_err());
        assert!(store.list_stream("rival-root").unwrap().is_empty());
    }

    #[test]
    fn continuation_refuses_live_descendants_and_changed_source() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
        let (old, _) = source(&store, &commits);
        let mut child = graph("old-child", "old-turn");
        child.parent_execution = Some(ExecutionParentBinding {
            execution_id: "old-root".into(),
            node_id: "old-root:model".into(),
        });
        commits.register_graph(child).unwrap();
        stop(&commits, &store, "old-root");
        assert!(crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "new-turn"
        )
        .is_err());
        stop(&commits, &store, "old-child");
        let (candidate, revision) = crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "new-turn",
        )
        .unwrap()
        .unwrap();
        let mut next = graph("new-root", "new-turn");
        next.continuation_binding = Some(
            crate::session_continuation::compile_continuation_binding(
                &candidate,
                "ingress",
                revision,
                ContinuationAuthorization::Authorized,
                1,
            )
            .unwrap(),
        );
        store
            .append(RuntimeEventInput {
                stream_id: format!("agentic-program:{}", old.program_id),
                scope: RuntimeEventScope::Program,
                kind: "test.source_advanced".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: json!({}),
            })
            .unwrap();
        assert!(commits
            .register_graph_with_continuation(next, Some(actor("new-turn", "new-root")))
            .is_err());
        assert!(store.list_stream("new-root").unwrap().is_empty());
    }
    #[test]
    fn continuation_preserves_accepted_content_and_fences_uncertain_claims() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
        let (old, task_id) = source(&store, &commits);
        stop(&commits, &store, "old-root");
        let (candidate, revision) = crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "new-turn",
        )
        .unwrap()
        .unwrap();
        let binding = crate::session_continuation::compile_continuation_binding(
            &candidate,
            "ingress",
            revision,
            ContinuationAuthorization::Authorized,
            1,
        )
        .unwrap();
        let mut projection = crate::AgentActionService::new(Arc::clone(&store))
            .project(&old.program_id)
            .unwrap();
        let accepted = projection.tasks.get_mut(&task_id).unwrap();
        accepted.status = AgenticTaskStatus::Accepted;
        accepted.artifact_refs = vec!["artifact:existing".into()];
        accepted.evidence_refs = vec!["artifact://immutable-body".into()];
        let accepted_facts = accepted.clone();
        let mut unresolved = accepted.clone();
        unresolved.task_id = "task:unknown-effect".into();
        unresolved.status = AgenticTaskStatus::Claimed;
        unresolved.claim_execution_id = Some("old-root".into());
        unresolved.claim_generation = 5;
        projection
            .tasks
            .insert(unresolved.task_id.clone(), unresolved);
        let inherited =
            revision_seed(&projection, &binding, &actor("new-turn", "new-root")).unwrap();
        let kept = &inherited.tasks[&task_id];
        assert_eq!(kept.status, AgenticTaskStatus::Accepted);
        assert_eq!(kept.artifact_refs, accepted_facts.artifact_refs);
        assert_eq!(kept.evidence_refs, accepted_facts.evidence_refs);
        let pending = &inherited.tasks["task:unknown-effect"];
        assert_eq!(pending.status, AgenticTaskStatus::Blocked);
        assert_eq!(pending.claim_generation, 6);
        assert!(pending.claim_execution_id.is_none());
        assert!(pending
            .unresolved
            .iter()
            .any(|gap| gap.starts_with("continuation_effect_reconciliation:")));
    }

    #[tokio::test]
    async fn continuation_dispatch_preserves_business_task_and_creates_current_execution_identity()
    {
        let services = crate::RuntimeServices::in_memory().unwrap();
        services.agent_task_executor().install_resolver(Arc::new(
            crate::agentic::coordination::tests::ControlledAgenticWorker,
        ));
        let store = Arc::clone(services.event_store());
        let commits = services.commit_service().clone();
        let (old, task_ref) = source(&store, &commits);
        services.publish_session_execution_policy(
            &old.session_id,
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Autonomous,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
                ),
            ),
        );
        let create_root = |id: &str, turn: &str| {
            let spec = services
                .task_runtime_port()
                .bind_task_spec(
                    &old.session_id,
                    Some(harness_contract::policy::PermissionMode::ReadOnly),
                    harness_contract::task::TaskSpec::new("original report"),
                )
                .unwrap();
            services
                .task_aggregate_service()
                .create(harness_contract::task::TaskCreateCommand {
                    task_id: id.into(),
                    mission_id: services.mission_runtime().default_mission_id().into(),
                    kind: harness_contract::task::TaskKind::Root,
                    origin: harness_contract::task::TaskOrigin::User,
                    origin_session_id: old.session_id.clone(),
                    origin_turn_id: turn.into(),
                    root_task_id: id.into(),
                    parent_task_id: None,
                    predecessor_task_id: None,
                    mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                    mission_assigned_by: "fixture".into(),
                    spec,
                    evidence_refs: vec![],
                })
                .unwrap();
        };
        create_root(&task_ref, "old-turn");
        create_root("task-new-turn", "new-turn");
        let old_aggregate = services
            .task_aggregate_service()
            .get(&task_ref)
            .unwrap()
            .unwrap();
        let actions = services.agent_action_service();
        let team = actions
            .project(&old.program_id)
            .unwrap()
            .teams
            .keys()
            .next()
            .unwrap()
            .clone();
        let invited = actions
            .apply(&AgentActionEnvelope {
                action_id: "invite-continuation-worker".into(),
                actor: old.clone(),
                expected_revision: None,
                action: AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team,
                    role: "Research worker".into(),
                    mission: "finish the remaining gap".into(),
                    required_capabilities: vec!["read".into()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            })
            .unwrap();
        assert_eq!(invited.status, AgentActionStatus::Applied);
        stop(&commits, &store, "old-root");
        let (candidate, revision) = crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "new-turn",
        )
        .unwrap()
        .unwrap();
        let mut next = graph("new-root", "new-turn");
        next.continuation_binding = Some(
            crate::session_continuation::compile_continuation_binding(
                &candidate,
                "new-ingress",
                revision,
                ContinuationAuthorization::Authorized,
                1,
            )
            .unwrap(),
        );
        let current = actor("new-turn", "new-root");
        commits
            .register_graph_with_continuation(next, Some(current.clone()))
            .unwrap();
        let dispatched = services
            .recover_agentic_programs_on_startup()
            .await
            .unwrap();
        assert_eq!(
            dispatched.len(),
            1,
            "only the inherited unfinished gap is dispatched"
        );
        assert_eq!(dispatched[0].task_ref, task_ref);
        let child = services
            .graph_state_store()
            .load(&dispatched[0].graph_id)
            .unwrap();
        let packet: harness_contract::agent::AgentTaskPacket =
            serde_json::from_str(&child.nodes[0].payload_ref).unwrap();
        assert_ne!(packet.task_id(), task_ref);
        assert_eq!(child.lineage.as_ref().unwrap().task_id, packet.task_id());
        let execution_task = services
            .task_aggregate_service()
            .get(packet.task_id())
            .unwrap()
            .unwrap();
        assert_eq!(execution_task.origin_turn_id, "new-turn");
        assert_eq!(execution_task.root_task_id, "task-new-turn");
        let binding = packet.agentic_binding.as_ref().unwrap();
        assert_eq!(binding.program_id, current.program_id);
        assert!(
            matches!(binding.focus, harness_contract::agent::AgenticExecutionFocus::TaskExecute {task_ref: ref business, ..} if business == &task_ref)
        );
        let resolved = services
            .resolve_agent_action_actor(
                &harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: child.id.clone(),
                    node_id: child.nodes[0].id.clone(),
                },
                None,
            )
            .await
            .expect("continued physical worker authenticates against its business Task");
        assert_eq!(resolved.program_id, current.program_id);
        let claimed = actions
            .apply(&AgentActionEnvelope {
                action_id: "continued-worker-real-claim".into(),
                actor: resolved,
                expected_revision: None,
                action: AgentAction::TaskClaim(harness_contract::agent_action::TaskClaimInput {
                    task_ref: task_ref.clone(),
                    reason: Some("resume the authorized remaining gap".into()),
                }),
            })
            .unwrap();
        assert_eq!(claimed.status, AgentActionStatus::Applied, "{claimed:?}");
        assert_eq!(
            actions.project(&current.program_id).unwrap().tasks[&task_ref]
                .claim_execution_id
                .as_deref(),
            Some(child.id.as_str())
        );
        assert_eq!(
            services
                .task_aggregate_service()
                .get(&task_ref)
                .unwrap()
                .unwrap(),
            old_aggregate
        );
        assert!(
            services
                .recover_agentic_programs_on_startup()
                .await
                .unwrap()
                .is_empty(),
            "recovery never starts the same gap twice"
        );

        // A valid packet/lease/lineage cannot be rebound to another real
        // business Task, even when both Tasks have the same Team authority.
        let other = actions
            .apply(&AgentActionEnvelope {
                action_id: "other-continued-task".into(),
                actor: current.clone(),
                expected_revision: None,
                action: AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: binding.task_team_id.clone(),
                    title: "Other contribution".into(),
                    objective: "separate business identity".into(),
                    acceptance: "separate result".into(),
                    required_capabilities: vec!["read".into()],
                    depends_on: vec![task_ref.clone()],
                    obligation_refs: vec![],
                    purpose: Default::default(),
                    execution_requirements: vec![],
                    expertise_hints: vec![],
                }),
            })
            .unwrap();
        assert_eq!(other.status, AgentActionStatus::Applied, "{other:?}");
        let mut corrupt = child.clone();
        corrupt.id = "continued-worker-wrong-business-focus".into();
        let mut wrong_packet = packet.clone();
        wrong_packet.assignment.graph_id = corrupt.id.clone();
        let identity = harness_contract::execution::ExecutionIdentity::for_task_graph(
            "test.principal",
            "test-workspace",
            &packet.assignment.mission_id,
            packet.task_id(),
            packet.session_id(),
            "new-turn",
            &corrupt.id,
        )
        .unwrap();
        wrong_packet.assignment.execution_identity =
            harness_contract::execution::ExecutionIdentity::for_agent_node(
                &identity,
                &packet.assignment.run_id,
                packet.node_id(),
            )
            .unwrap();
        wrong_packet.agentic_binding.as_mut().unwrap().focus =
            harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                task_ref: other.changed_refs[0].clone(),
            };
        corrupt.nodes[0].payload_ref = serde_json::to_string(&wrong_packet).unwrap();
        services
            .commit_service()
            .register_graph(corrupt.clone())
            .unwrap();
        assert_eq!(
            services
                .resolve_agent_action_actor(
                    &harness_contract::execution_graph::ExecutionParentBinding {
                        execution_id: corrupt.id,
                        node_id: corrupt.nodes[0].id.clone(),
                    },
                    None,
                )
                .await
                .unwrap_err(),
            "agent_actor_agentic_task_mismatch"
        );
    }
}
