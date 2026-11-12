//! Program revision lineage prepared by Runtime and committed with the new root.
use super::program::{AgenticProgramContinuation, AgenticProgramStatus, AgenticTaskStatus};
use crate::{
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    RuntimeTransactionEventInput,
};
use harness_contract::{agent_action::AgentActorBinding, execution_graph::ExecutionGraph};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

/// One original Goal read shared by Host preparation and the registration
/// transaction. Business quantities do not replace the frozen actor grant.
pub(crate) fn read_continuation_goal(
    store: &Arc<RuntimeEventStore>,
    source: &crate::AgenticProgramProjection,
) -> Result<Option<crate::execution_core::GoalProjection>, String> {
    let goal = source
        .root_execution_id
        .as_ref()
        .map(|root| {
            crate::execution_core::GoalStore::new(Arc::clone(store))
                .projection(&format!("goal:{root}"))
        })
        .transpose()?
        .flatten();
    if let Some(goal) = &goal {
        crate::execution_core::goal::assessment::assess_bound_goal_projection(source, Some(goal))
            .map_err(|error| error.to_string())?;
    } else {
        source.validate_team_quantity_contract()?;
    }
    Ok(goal)
}

pub(crate) fn prepare(
    store: &Arc<RuntimeEventStore>,
    graph: &ExecutionGraph,
    actor: &AgentActorBinding,
    source_goal_cut: Option<&crate::ExpectedStreamRevision>,
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
    let goal = read_continuation_goal(store, &source)?;
    let goal_cut = source
        .root_execution_id
        .as_ref()
        .map(|root| crate::ExpectedStreamRevision {
            stream_id: format!("goal:goal:{root}"),
            expected_revision: goal.as_ref().map_or(0, |goal| goal.stream_revision),
        });
    if source_goal_cut.is_some_and(|cut| Some(cut) != goal_cut.as_ref())
        || (goal.is_some() && source_goal_cut.is_none())
    {
        return Err("continuation original Goal source cut changed or missing".into());
    }
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
        || actor.exact_team_count != source.exact_team_count
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
    if let Some(cut) = goal_cut {
        if expected
            .insert(cut.stream_id, cut.expected_revision)
            .is_some_and(|previous| previous != cut.expected_revision)
        {
            return Err("continuation original Goal source cut conflicts".into());
        }
    }
    expected.insert(source_stream, source.revision);
    let target_stream = format!("agentic-program:{}", actor.program_id);
    expected.insert(target_stream.clone(), 0);
    let seed = revision_seed(&source, binding, actor)?;
    let payload = json!({
        "program_id": seed.program_id, "objective_id": seed.objective_id,
        "session_id": seed.session_id, "turn_id": seed.turn_id,
        "root_execution_id": seed.root_execution_id, "required_team_count": seed.required_team_count,
        "exact_team_count": seed.exact_team_count,
        "team_count_contract_version": seed.team_count_contract_version,
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
        // 032 K3 (B8): the continuation re-seed must not erase outstanding
        // attempt facts. Every attempt leaving the active set without a
        // durable terminal keeps its full identity in the unsettled list,
        // exactly like the rework release path in work_market.
        task.unsettled_released_attempts
            .extend(task.active_attempts.values().cloned());
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
            original_execution: None,
            program_id: program_id_for_objective(&objective_id),
            objective_id,
            session_id: "continuation-session".into(),
            turn_id: turn.into(),
            root_execution_id: Some(root.into()),
            required_team_count: 1,
            exact_team_count: None,
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
        source_with_revised_quantity(store, commits, false)
    }

    fn source_with_revised_quantity(
        store: &Arc<RuntimeEventStore>,
        commits: &crate::ExecutionCommitService,
        revised: bool,
    ) -> (AgentActorBinding, String) {
        let mut root = actor("old-turn", "old-root");
        if revised {
            root.required_team_count = 0;
            root.exact_team_count = Some(0);
            let goal: harness_contract::goal::GoalContract = serde_json::from_value(json!({
                "id":"goal:old-root","session_id":root.session_id,"objective":root.objective_summary,
                "criteria":[{"id":"user-intent","statement":"Use the current authenticated Team constraint","status":"open"}],
                "phase":"execution","completion":"open","revision":1,"user_sequence":1,
                "spec_revision":1,"spec_digest":"original-team0","scope":"user_objective",
                "user_intent_criterion_id":"user-intent","source_intent_ref":"session_input_application:original-team0",
                "execution_binding":{"objective_id":root.objective_id,"session_id":root.session_id,"turn_id":root.turn_id,
                    "root_execution_id":"old-root","agentic_program_id":root.program_id},
                "input_contract":{"version":2,"principal_id":"session:continuation-session","session_id":root.session_id,
                    "turn_id":root.turn_id,"source_ref":"session_input_application:original-team0","input_revision":1,
                    "content_sha256":"a".repeat(64),"commitments":[{"kind":"team_count","minimum":0,"maximum":0,"source":{"start":0,"end":4}}]}
            })).unwrap();
            let goals = crate::execution_core::GoalStore::new(Arc::clone(store));
            goals.create(goal).unwrap();
            goals
                .revise(
                    "goal:old-root",
                    1,
                    2,
                    "accepted Team0 to Team1 user business revision",
                    |goal| {
                        let input = goal.input_contract.as_mut().unwrap();
                        input.input_revision = 2;
                        input.source_ref = "session_input_application:accepted-team1".into();
                        input.commitments =
                            vec![harness_contract::goal::GoalInputCommitment::TeamCount {
                                minimum: 1,
                                maximum: Some(1),
                                source: harness_contract::goal::GoalInputSpan { start: 0, end: 4 },
                            }];
                        vec!["input_contract".into()]
                    },
                )
                .unwrap();
        }
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
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
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
    fn continuation_seed_preserves_minimum_exact_and_forbidden_quantity() {
        for (minimum, exact) in [(3, None), (3, Some(3)), (0, None), (0, Some(0))] {
            let mut original = super::super::program::AgenticProgramProjection::empty(
                "old-program",
                "old-objective",
            );
            original.required_team_count = minimum;
            original.exact_team_count = exact;
            let binding = harness_contract::turn::CollaborationContinuationBinding {
                source_session_id: "continuation-session".into(),
                source_turn_id: "old-turn".into(),
                source_root_id: "old-root".into(),
                team_set_ref: "agentic_program:old-program".into(),
                delivery_revision: 1,
                result_refs: vec![],
                current_ingress: "current-ingress".into(),
                candidate_revision: 1,
                binding_digest: "bound-identity".into(),
                authorization: ContinuationAuthorization::Authorized,
                authorization_revision: 1,
                handoff_id: None,
            };
            let seed =
                revision_seed(&original, &binding, &actor("next-turn", "next-root")).unwrap();
            assert_eq!(
                (
                    seed.required_team_count,
                    seed.exact_team_count,
                    seed.team_count_contract_version
                ),
                (minimum, exact, 1)
            );
            assert!(seed.validate_team_quantity_contract().is_ok());
        }
    }

    #[test]
    fn accepted_team_zero_to_one_continues_without_rewriting_frozen_actor() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
        let (old, _) = source_with_revised_quantity(&store, &commits, true);
        let actions = crate::AgentActionService::new(Arc::clone(&store));
        let before = actions.project(&old.program_id).unwrap();
        assert_eq!(
            (
                before.required_team_count,
                before.exact_team_count,
                before.teams.len()
            ),
            (0, Some(0), 1)
        );
        stop(&commits, &store, "old-root");
        let (candidate, revision) = crate::session_continuation::latest_same_session_candidate(
            &store,
            &old.session_id,
            "next-turn",
        )
        .unwrap()
        .unwrap();
        let binding = crate::session_continuation::compile_continuation_binding(
            &candidate,
            "current-user-ingress",
            revision,
            ContinuationAuthorization::Authorized,
            7,
        )
        .unwrap();
        let mut next = graph("next-root", "next-turn");
        next.continuation_binding = Some(binding);
        let mut target = actor("next-turn", "next-root");
        target.required_team_count = 0;
        target.exact_team_count = Some(0);
        let cut = |revision| crate::ExpectedStreamRevision {
            stream_id: "goal:goal:old-root".into(),
            expected_revision: revision,
        };
        let baseline = prepare(&store, &next, &target, Some(&cut(2))).unwrap();
        assert_eq!(baseline.1.get("goal:goal:old-root"), Some(&2));
        let mut changed_actor = target.clone();
        changed_actor.required_team_count = 1;
        changed_actor.exact_team_count = Some(1);
        assert!(
            prepare(&store, &next, &changed_actor, Some(&cut(2)))
                .unwrap_err()
                .contains("continuation source changed or identity mismatched"),
            "business quantities must not rewrite frozen ingress/grant metadata"
        );
        let goals = crate::execution_core::GoalStore::new(Arc::clone(&store));
        let original_binding = goals
            .get("goal:old-root")
            .unwrap()
            .unwrap()
            .execution_binding
            .unwrap();
        goals
            .revise(
                "goal:old-root",
                2,
                3,
                "one foreign Goal binding fault",
                |goal| {
                    goal.execution_binding.as_mut().unwrap().agentic_program_id =
                        "foreign-program".into();
                    vec!["execution_binding".into()]
                },
            )
            .unwrap();
        assert!(prepare(&store, &next, &target, Some(&cut(3)))
            .unwrap_err()
            .contains("assessment identity"));
        assert!(actions
            .project_if_exists(&target.program_id)
            .unwrap()
            .is_none());
        goals
            .revise(
                "goal:old-root",
                3,
                4,
                "restore original Goal binding",
                |goal| {
                    goal.execution_binding = Some(original_binding);
                    vec!["execution_binding".into()]
                },
            )
            .unwrap();
        let (domain_events, mut expected) = prepare(&store, &next, &target, Some(&cut(4))).unwrap();
        // The original graph append supplies output-stream fences in addition
        // to prepare's readset. This direct domain-transaction witness supplies
        // the same fresh successor output fence, without rebasing Goal4.
        assert!(expected.insert("continuation-cas".into(), 0).is_none());
        goals
            .revise(
                "goal:old-root",
                4,
                5,
                "concurrent original Goal business revision",
                |goal| {
                    goal.constraints.push("still preserve accepted work".into());
                    vec!["constraints".into()]
                },
            )
            .unwrap();
        let rejected = store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: "continuation-goal-only-stale-cut".into(),
            expected_streams: expected
                .into_iter()
                .map(
                    |(stream_id, expected_revision)| crate::ExpectedStreamRevision {
                        stream_id,
                        expected_revision,
                    },
                )
                .collect(),
            events: domain_events,
        });
        assert!(
            matches!(rejected,Err(crate::RuntimeEventStoreError::StaleRevision{ref stream_id,expected:4,actual:5}) if stream_id=="goal:goal:old-root"),
            "{rejected:?}"
        );
        assert!(actions
            .project_if_exists(&target.program_id)
            .unwrap()
            .is_none());
        assert!(
            store
                .event_by_idempotency_key(
                    "continuation-cas",
                    &format!("program-successor:{}", old.program_id)
                )
                .unwrap()
                .is_none(),
            "stale Goal must not partially reserve the unique successor"
        );
        assert!(prepare(&store, &next, &target, Some(&cut(4)))
            .unwrap_err()
            .contains("Goal source cut changed"));
        let outcome =
            commits.register_graph_with_continuation(next, Some(target.clone()), Some(cut(5)));
        assert!(outcome.is_ok(),"a current accepted Team1 contract must survive original continuation without rewriting old actor: {:?}",outcome.err());
        let after = actions.project(&target.program_id).unwrap();
        assert_eq!(
            (
                after.required_team_count,
                after.exact_team_count,
                after.teams.len()
            ),
            (0, Some(0), 1)
        );
        assert_eq!(
            actions.project(&old.program_id).unwrap().revision,
            before.revision
        );
    }

    #[test]
    fn unfinished_continuation_is_atomic_fenced_and_recoverable() {
        let store = Arc::new(RuntimeEventStore::for_test());
        let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
        let (old, task) = source(&store, &commits);
        assert!(
            crate::session_continuation::latest_same_session_candidate(
                &store,
                &old.session_id,
                "new-turn"
            )
            .is_err()
        );
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
        let mut changed_quantity = current.clone();
        changed_quantity.exact_team_count = Some(changed_quantity.required_team_count);
        assert!(
            commits
                .register_graph_with_continuation(next.clone(), Some(changed_quantity), None)
                .is_err(),
            "continuation cannot turn the original minimum into an exact ceiling"
        );
        assert!(
            crate::AgentActionService::new(Arc::clone(&store))
                .project(&current.program_id)
                .is_err(),
            "rejected continuation must leave no target Program"
        );
        let receipt = commits
            .register_graph_with_continuation(next.clone(), Some(current.clone()), None)
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
            commits.register_graph_with_continuation(next, Some(current), None),
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
            .register_graph_with_continuation(rival, Some(actor("rival-turn", "rival-root")), None)
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
        assert!(
            crate::session_continuation::latest_same_session_candidate(
                &store,
                &old.session_id,
                "new-turn"
            )
            .is_err()
        );
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
            .register_graph_with_continuation(next, Some(actor("new-turn", "new-root")), None)
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
        assert!(
            pending
                .unresolved
                .iter()
                .any(|gap| gap.starts_with("continuation_effect_reconciliation:"))
        );
    }

    /// Path 4 - a continuation cannot carry an uncertain claim across the
    /// turn.  The fence it inherits is fenced forward: the generation moves so
    /// the old execution's token cannot be replayed, the claim fields are
    /// cleared, the attempts are settled by name into the task's unsettled
    /// ledger (032 K3/B8: a re-seed must not erase attempt facts), and the
    /// Task waits for an explicit reconciliation instead of looking busy.
    #[test]
    fn a_continuation_fences_the_claim_it_cannot_carry_over() {
        let mut source = crate::AgenticProgramProjection::empty("program", "objective");
        source.tasks.insert(
            "task".to_string(),
            crate::AgenticTaskProjection {
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                task_id: "task".to_string(),
                team_id: "team".to_string(),
                title: "task".to_string(),
                objective: "objective".to_string(),
                acceptance: "durable evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                depends_on: Vec::new(),
                status: AgenticTaskStatus::Claimed,
                claimant: Some("agent".to_string()),
                claim_generation: 5,
                claim_execution_id: Some("old-root".to_string()),
                claimed_at_ms: Some(1_000),
                lease_expires_at_ms: Some(91_000),
                claim_renewal_streak: 0,
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
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        source
            .tasks
            .get_mut("task")
            .expect("task")
            .active_attempts
            .insert(
                "old-root".to_string(),
                crate::agentic::AgenticTaskAttemptProjection {
                    execution_id: "old-root".to_string(),
                    agent_id: "agent".to_string(),
                    membership_id: "membership:agent:team".to_string(),
                    mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                    generation: 4,
                },
            );
        let binding = harness_contract::turn::CollaborationContinuationBinding {
            source_session_id: "continuation-session".to_string(),
            source_turn_id: "old-turn".to_string(),
            source_root_id: "old-root".to_string(),
            team_set_ref: "team".to_string(),
            delivery_revision: 1,
            result_refs: Vec::new(),
            current_ingress: "ingress".to_string(),
            candidate_revision: 1,
            binding_digest: "digest".to_string(),
            authorization: ContinuationAuthorization::Authorized,
            authorization_revision: 1,
            handoff_id: None,
        };
        let seed = revision_seed(&source, &binding, &actor("new-turn", "new-root")).unwrap();
        let task = &seed.tasks["task"];
        assert_eq!(task.status, AgenticTaskStatus::Blocked);
        assert_eq!(task.claim_generation, 6);
        assert_eq!(task.claimant, None);
        assert_eq!(task.claim_execution_id, None);
        assert_eq!(task.claimed_at_ms, None);
        assert_eq!(task.lease_expires_at_ms, None);
        assert!(task.active_attempts.is_empty());
        let settled = &task.unsettled_released_attempts;
        assert_eq!(settled.len(), 1, "the released attempt keeps its identity");
        assert_eq!(settled[0].execution_id, "old-root");
        assert_eq!(settled[0].generation, 4);
        assert!(
            task.unresolved
                .iter()
                .any(|gap| gap.starts_with("continuation_effect_reconciliation:"))
        );
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
            .register_graph_with_continuation(next, Some(current.clone()), None)
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
                    criterion_source: Default::default(),
                    acceptance_checks: Vec::new(),
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
