//! Four original coordination behaviors on the real same-source PostgreSQL
//! topology. These migrate the runtime `agentic::coordination` PG originals to
//! the Gateway production assembly (`compose_for_test` + `RecoveryServices`),
//! where event, Session, Task and artifact stores share one scoped PG backend
//! with a checked cancellable transaction owner. Each test isolates its schema.
//!
//! Run explicitly: `--ignored --exact` (the executor lists all four). They are
//! ignored by default because they require the owned isolated PostgreSQL.

use super::r1_terminal_recovery::{
    durable_turn_until, install_native_definition, provider_registry, LoopbackProvider,
    RecoveryServices,
};
use harness_contract::agent::{AgentCapability, AgentTaskPacket};
use harness_contract::agent_action::{
    program_id_for_objective, root_objective_id, AgentAction, AgentActionEnvelope, AgentActorBinding,
    AgentActorKind, AgentInviteInput, MessagePublishInput, TaskClaimInput, TaskIntent, TaskIntentKind,
    TaskPublishInput, TaskReleaseInput, TeamCreateInput,
};
use serde_json::json;
use std::sync::Arc;

struct CoordFixture {
    _root_dir: tempfile::TempDir,
    services: RecoveryServices,
    root: LoopbackProvider,
    _owner: LoopbackProvider,
    _helper: LoopbackProvider,
    program_id: String,
    graph_id: String,
    owner_def: String,
    helper_def: String,
    root_actor: AgentActorBinding,
}

impl CoordFixture {
    fn runtime(&self) -> Arc<runtime::RuntimeServices> {
        self.services.runtime.runtime_services()
    }

    fn apply(&self, id: &str, actor: &AgentActorBinding, action: AgentAction) -> String {
        let out = self
            .runtime()
            .agent_action_service()
            .apply_for_test(&AgentActionEnvelope {
                action_id: id.into(),
                actor: actor.clone(),
                expected_revision: None,
                action,
            })
            .unwrap();
        assert_eq!(
            out.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{out:?}"
        );
        out.changed_refs[0].clone()
    }

    fn project(&self) -> runtime::AgenticProgramProjection {
        self.runtime()
            .agent_action_service()
            .project(&self.program_id)
            .unwrap()
    }
}

async fn setup() -> CoordFixture {
    let root_dir = tempfile::tempdir().unwrap();
    let home = root_dir.path().join("home");
    let workspace = root_dir.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let topology =
        crate::selected_storage::SelectedStorageTopology::compose_for_test(&home, &workspace)
            .unwrap();
    let root = LoopbackProvider::start("root").await;
    let owner = LoopbackProvider::start("owner").await;
    let helper = LoopbackProvider::start("helper").await;
    let providers = provider_registry(&[&root, &owner, &helper]);
    let services = RecoveryServices::start(root_dir.path(), topology, providers).await;
    let owner_def = install_native_definition(
        &services,
        "coord/owner",
        &owner,
        &[AgentCapability::Read, AgentCapability::Write],
    );
    let helper_def =
        install_native_definition(&services, "coord/helper", &helper, &[AgentCapability::Read]);
    let session_id = format!("coord:{}", uuid::Uuid::new_v4());
    let mut request = crate::services::session_service::activation::EnsureSessionRequest::new(
        &session_id,
        Some(root.model.clone()),
        crate::services::session_service::activation::SessionSource::Tui,
    );
    request.owner_principal_id = Some("coord-operator".into());
    services.session.create_user_session(request).await.unwrap();
    let envelope = harness_contract::turn::SessionInputEnvelope::text(
        &session_id,
        harness_contract::turn::InputSourceKind::Tui,
        "Coordinate evidence between the owner and helper agents on the real PostgreSQL topology.",
    );
    let admission = services.session.admit_input(envelope).await.unwrap();
    let graph_id = admission.execution_graph_id;
    let turn_id = admission.turn_id;
    let objective_id = root_objective_id(&session_id, &turn_id);
    let program_id = program_id_for_objective(&objective_id);
    let root_actor = AgentActorBinding {
        original_execution: None,
        objective_id,
        program_id: program_id.clone(),
        session_id,
        turn_id,
        root_execution_id: Some(graph_id.clone()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "coordinate evidence".into(),
        model_lease: "test-model".into(),
        permission_ceiling: Some(harness_contract::policy::PermissionMode::WorkspaceWrite),
        resource_scopes: vec!["read:.".into(), "write:.".into()],
        actor_id: "root:coord".into(),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    CoordFixture {
        _root_dir: root_dir,
        services,
        root,
        _owner: owner,
        _helper: helper,
        program_id,
        graph_id,
        owner_def,
        helper_def,
        root_actor,
    }
}

/// Establish the real primary execution and a registered coordination request,
/// returning (team, owner_member, helper_member, task_ref, author, primary_id).
async fn established(
    f: &mut CoordFixture,
) -> (String, String, String, String, AgentActorBinding, String) {
    let runtime = f.runtime();
    f.root
        .tool("coord-team", "team_create", json!({"name":"Evidence","mission":"check sources"}), None)
        .await;
    let team = durable_turn_until(&f.services, &f.graph_id, &f.program_id, "team_created", || {
        runtime
            .agent_action_service()
            .project_if_exists(&f.program_id)
            .unwrap()
            .and_then(|program| program.teams.keys().next().cloned())
    })
    .await;
    f.root
        .tool(
            "coord-owner",
            "agent_invite",
            json!({"team_ref":team,"role":"Reader","mission":"check evidence","required_capabilities":["read","write"],"definition_ref":f.owner_def}),
            None,
        )
        .await;
    f.root
        .tool(
            "coord-helper",
            "agent_invite",
            json!({"team_ref":team,"role":"Reader","mission":"check evidence","required_capabilities":["read"],"definition_ref":f.helper_def}),
            None,
        )
        .await;
    let (owner, helper) =
        durable_turn_until(&f.services, &f.graph_id, &f.program_id, "members_invited", || {
            let program = runtime
                .agent_action_service()
                .project_if_exists(&f.program_id)
                .unwrap()?;
            // Match members by their installed definition, not BTreeMap order:
            // agent refs are content hashes that vary with the session uuid.
            let owner = program
                .agents
                .values()
                .find(|m| m.definition_ref.as_deref() == Some(f.owner_def.as_str()))?
                .agent_id
                .clone();
            let helper = program
                .agents
                .values()
                .find(|m| m.definition_ref.as_deref() == Some(f.helper_def.as_str()))?
                .agent_id
                .clone();
            Some((owner, helper))
        })
        .await;
    f.root
        .tool(
            "coord-task",
            "task_publish",
            json!({"team_ref":team,"title":"Evidence","objective":"verify source","acceptance":"source checked","required_capabilities":["read","write"]}),
            None,
        )
        .await;
    let task_ref = durable_turn_until(&f.services, &f.graph_id, &f.program_id, "task_published", || {
        runtime
            .agent_action_service()
            .project_if_exists(&f.program_id)
            .unwrap()
            .and_then(|program| program.tasks.keys().next().cloned())
    })
    .await;
    // The production wire flow dispatches and claims the task automatically;
    // wait for the real primary claim, then resolve its authenticated actor.
    let (primary_id, node_id, attempt) =
        durable_turn_until(&f.services, &f.graph_id, &f.program_id, "primary_running", || {
            let program = runtime
                .agent_action_service()
                .project_if_exists(&f.program_id)
                .unwrap()?;
            let primary_id = program.tasks.get(&task_ref)?.claim_execution_id.clone()?;
            let graph = runtime.graph_state_store().load(&primary_id).ok()?;
            let node = graph.nodes.first()?;
            let attempt = graph.recovery_cursor.node_attempts.get(&node.id).copied()?;
            if graph.node_statuses.get(&node.id)
                != Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
            {
                return None;
            }
            Some((primary_id, node.id.clone(), attempt))
        })
        .await;
    let author = runtime
        .resolve_agent_action_actor_for_attempt(
            &harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: primary_id.clone(),
                node_id,
            },
            attempt,
            Some(f.root_actor.clone()),
        )
        .await
        .unwrap();
    if f.project().tasks[&task_ref].claim_execution_id.is_none() {
        f.apply(
            "primary-claim",
            &author,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_ref.clone(),
                reason: Some("own the original work".into()),
            }),
        );
    }
    (team, owner, helper, task_ref, author, primary_id)
}

fn coordination_request(
    f: &CoordFixture,
    author: &AgentActorBinding,
    team: &str,
    helper: &str,
    task_ref: &str,
    summary: &str,
) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: format!("help-request:{summary}"),
        actor: author.clone(),
        expected_revision: None,
        action: AgentAction::MessagePublish(MessagePublishInput {
            topic_ref: format!("topic:{team}"),
            summary: Some(summary.into()),
            content_ref: None,
            refs: Vec::new(),
            recipients: vec![helper.into()],
            intent: Some(TaskIntent {
                task_ref: task_ref.into(),
                kind: TaskIntentKind::RequestHelp,
                reason_ref: None,
                requested_capability_refs: Vec::new(),
            }),
            issue_dispositions: Vec::new(),
        }),
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and the Gateway real same-source topology; run with --ignored --exact"]
async fn coordination_visibility_respects_active_teams_recipients_and_program_topic() {
    let mut f = setup().await;
    let (team, _owner, helper, task_ref, author, _primary) = established(&mut f).await;
    let runtime = f.runtime();
    let request = coordination_request(&f, &author, &team, &helper, &task_ref, "Check source A");
    runtime
        .agent_action_service()
        .apply_for_test(&request)
        .unwrap();
    // A foreign team member is active in its own team but cannot read this
    // private Team's Topic, even as an explicit recipient. Root program-state
    // actions drive the authenticated root lane.
    f.root
        .tool("foreign-team", "team_create", json!({"name":"Other","mission":"other evidence"}), None)
        .await;
    let foreign_team = durable_turn_until(&f.services, &f.graph_id, &f.program_id, "foreign_team", || {
        runtime
            .agent_action_service()
            .project_if_exists(&f.program_id)
            .unwrap()
            .and_then(|p| p.teams.keys().find(|k| *k != &team).cloned())
    })
    .await;
    f.root
        .tool(
            "foreign-member",
            "agent_invite",
            json!({"team_ref":foreign_team,"role":"Reader","mission":"read public evidence","required_capabilities":["read"]}),
            None,
        )
        .await;
    let foreign_member = durable_turn_until(&f.services, &f.graph_id, &f.program_id, "foreign_member", || {
        runtime
            .agent_action_service()
            .project_if_exists(&f.program_id)
            .unwrap()
            .and_then(|p| p.agents.keys().find(|k| *k != &_owner && *k != &helper).cloned())
    })
    .await;
    let private = f.project();
    let revision = private.coordination_requests_for_test()[0].1.revision;
    assert!(private.agent_is_active_in(&foreign_member, &foreign_team));
    assert!(private
        .coordination_team_id_for_test(&foreign_member, revision)
        .is_none());
    // The Program topic is visible across teams.
    let program_topic = format!("topic:{}", private.program_id);
    let mut public_request = request.clone();
    public_request.action_id = "public-cross-team-request".into();
    if let AgentAction::MessagePublish(input) = &mut public_request.action {
        input.topic_ref = program_topic;
        input.recipients = vec![foreign_member.clone()];
    }
    assert_eq!(
        runtime
            .agent_action_service()
            .apply_for_test(&public_request)
            .unwrap()
            .status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let projection = f.project();
    let program_topic = format!("topic:{}", projection.program_id);
    let public_revision = projection
        .coordination_requests_for_test()
        .iter()
        .find(|(topic, _)| *topic == program_topic)
        .map(|(_, entry)| entry.revision)
        .expect("public program-topic coordination request");
    assert_eq!(
        projection.coordination_team_id_for_test(&foreign_member, public_revision),
        Some(foreign_team.clone())
    );
    assert!(projection
        .coordination_team_id_for_test(&author.actor_id, revision)
        .is_none());
}

/// Apply a coordination request as the authenticated author and dispatch the
/// real coordination attempt, returning its packet, wake ref and revision.
async fn dispatched_coordination(
    f: &CoordFixture,
    author: &AgentActorBinding,
    team: &str,
    helper: &str,
    task_ref: &str,
    summary: &str,
) -> (AgentTaskPacket, String, u64) {
    let runtime = f.runtime();
    let request = coordination_request(f, author, team, helper, task_ref, summary);
    runtime
        .agent_action_service()
        .apply_for_test(&request)
        .unwrap();
    let projection = f.project();
    let (wake_ref, revision) = {
        let requests = projection.coordination_requests_for_test();
        (requests[0].1.entry_id.clone(), requests[0].1.revision)
    };
    // The coordination dispatch can lag the request commit on the real PG
    // backend; retry until the coordination attempt is dispatched and its graph
    // is loadable, rather than asserting on a single dispatch pass.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    // The coordination receipt is produced once; capture its graph id, then wait
    // for the asynchronously created graph to become loadable independently.
    let coordination_graph_id = loop {
        let receipts = runtime
            .dispatch_ready_agentic_work_for_test(&f.program_id)
            .await
            .unwrap();
        if let Some(coordination) = receipts.iter().find(|receipt| receipt.mode == "coordination") {
            break coordination.graph_id.clone();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("coordination receipt not produced within 120s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    let packet = loop {
        if let Ok(graph) = runtime.graph_state_store().load(&coordination_graph_id) {
            if let Some(node) = graph.nodes.first() {
                if let Ok(packet) = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref) {
                    break packet;
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("coordination graph not loadable within 120s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    (packet, wake_ref, revision)
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and the Gateway real same-source topology; run with --ignored --exact"]
async fn coordination_failure_and_epoch_change_do_not_punish_primary_work() {
    let mut f = setup().await;
    let (team, _owner, helper, task_ref, author, primary_id) = established(&mut f).await;
    let (packet, _wake, _revision) =
        dispatched_coordination(&f, &author, &team, &helper, &task_ref, "Check source A").await;
    // The real PG backend settles an abandoned attempt only after its agent run
    // has a confirmed durable local exit; drive the helper lane to that exit.
    // Wait until the helper lane has actually claimed and started the coordination
    // run (node Running) so finish drives that exact run, not an earlier request.
    durable_turn_until(&f.services, &f.graph_id, &f.program_id, "coordination_running", || {
        let graph = f.runtime().graph_state_store().load(packet.graph_id()).ok()?;
        let node = graph.nodes.first()?;
        if graph.node_statuses.get(&node.id)
            != Some(&harness_contract::execution_graph::ExecutionNodeStatus::Running)
        {
            return None;
        }
        Some(())
    })
    .await;
    // Fail the coordination attempt before the run physically exits. The real
    // backend auto-settles an abandoned attempt on exit with a production reason
    // ("Agent execution ended without a durable task submission or review"),
    // which would otherwise race ahead of the fixture's explicit settlement and
    // overwrite its reason. Applying the production supervisor TaskAttemptFail
    // first settles the durable consumption with this fixture's reason; the
    // later auto-settlement then finds no active attempt and is a no-op.
    let fail_projection = f.project();
    let fail_supervisor = AgentActorBinding {
        original_execution: None,
        objective_id: fail_projection.objective_id.clone(),
        program_id: fail_projection.program_id.clone(),
        session_id: fail_projection.session_id.clone(),
        turn_id: fail_projection.turn_id.clone(),
        root_execution_id: fail_projection.root_execution_id.clone(),
        required_team_count: fail_projection.required_team_count,
        exact_team_count: fail_projection.exact_team_count,
        objective_summary: fail_projection.objective_summary.clone(),
        model_lease: fail_projection.model_lease.clone(),
        permission_ceiling: Some(fail_projection.permission_ceiling),
        resource_scopes: fail_projection.resource_scopes.clone(),
        actor_id: "runtime.program-supervisor".into(),
        kind: AgentActorKind::Supervisor,
        execution_id: Some(packet.graph_id().into()),
        team_id: None,
        agent_id: None,
    };
    let failed_first = f
        .runtime()
        .agent_action_service()
        .apply_runtime_supervisor_for_test(&AgentActionEnvelope {
            action_id: "coordination-fail-before-exit".into(),
            actor: fail_supervisor,
            expected_revision: None,
            action: AgentAction::TaskAttemptFail(
                harness_contract::agent_action::TaskAttemptFailInput {
                    task_ref: task_ref.clone(),
                    execution_id: packet.graph_id().into(),
                    mode: harness_contract::agent_action::AgentAttemptMode::Coordination,
                    reason: "fixture backend failed before replying".into(),
                    retryable: false,
                },
            ),
        })
        .unwrap();
    assert_eq!(
        failed_first.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{failed_first:?}"
    );
    // The coordination run is a bounded autonomy lifecycle: it parks at durable
    // collaboration checkpoints and only exits physically once the model-loop
    // liveness fuse trips (an identical checkpoint repeated without durable
    // progress). A single scripted finish answers just the first provider turn,
    // leaving the checkpoint continuations unanswered and the run alive, so no
    // agent.local_exit is ever published. Hold this exact run's native exit,
    // drive the helper lane through the bounded continuations until the run
    // reaches that exit, then release it so the authenticated local_exit is
    // published for this packet (settle requires confirmed_run_exit).
    let hold = runtime::InProcessAgentWorker::hold_exit_for_test(&packet).unwrap();
    let drive = async {
        loop {
            f._helper
                .finish("coordination backend failed before replying")
                .await;
        }
    };
    tokio::select! {
        _ = drive => {},
        entered = hold.entered() => { entered.unwrap(); },
    }
    hold.release();
    tokio::time::timeout(std::time::Duration::from_secs(30), hold.exited())
        .await
        .expect("coordination native exit hold did not close")
        .unwrap();
    // The native backend records the durable local exit asynchronously after the
    // lane returns; wait for the coordination run's exact exit event before the
    // owner settles the abandoned attempt (settle requires confirmed_run_exit).
    durable_turn_until(&f.services, &f.graph_id, &f.program_id, "coordination_local_exit", || {
        f.services
            .topology
            .runtime_event_store
            .event_by_idempotency_key(
                &format!("agent:{}", packet.agent_id()),
                &format!("agent.local_exit:{}", packet.run_id()),
            )
            .unwrap()
    })
    .await;
    f.runtime()
        .settle_abandoned_agentic_attempt_for_test(&packet, "fixture backend failed before replying")
        .await
        .unwrap();
    let failed = f.project();
    assert_eq!(
        failed.tasks[&task_ref].claim_execution_id.as_deref(),
        Some(primary_id.as_str())
    );
    assert_eq!(failed.tasks[&task_ref].failed_attempts, 0);
    assert!(failed.coordination_requests_for_test()[0]
        .1
        .coordination
        .as_ref()
        .unwrap()
        .reason
        .as_deref()
        .unwrap()
        .contains("failed before replying"));
    assert!(f
        .runtime()
        .dispatch_ready_agentic_work_for_test(&f.program_id)
        .await
        .unwrap()
        .is_empty());
    let generation = failed.tasks[&task_ref].claim_generation;
    let release = AgentActionEnvelope {
        action_id: "primary-release".into(),
        actor: author.clone(),
        expected_revision: Some(failed.revision),
        action: AgentAction::TaskRelease(TaskReleaseInput {
            task_ref: task_ref.clone(),
            reason: "replan the source".into(),
            expected_claim_generation: Some(generation),
        }),
    };
    assert_eq!(
        f.runtime()
            .agent_action_service()
            .apply_for_test(&release)
            .unwrap()
            .status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    // Release removed the primary attempt, but the owner's primary physical run
    // is still parked at its provider request, so `running_members` keeps the
    // owner busy and re-dispatch would starve ("no free eligible member"). Drive
    // the owner lane to exit that exact run; with the attempt already released,
    // settlement is a no-op and failed_attempts stays 0. Only then is the owner
    // free to claim the new epoch.
    let primary_graph = f.runtime().graph_state_store().load(&primary_id).unwrap();
    let primary_packet: AgentTaskPacket =
        serde_json::from_str(&primary_graph.nodes[0].payload_ref).unwrap();
    let owner_hold = runtime::InProcessAgentWorker::hold_exit_for_test(&primary_packet).unwrap();
    let owner_drive = async {
        loop {
            f._owner
                .finish("primary work parked before the new epoch")
                .await;
        }
    };
    tokio::select! {
        _ = owner_drive => {},
        entered = owner_hold.entered() => { entered.unwrap(); },
    }
    owner_hold.release();
    tokio::time::timeout(std::time::Duration::from_secs(30), owner_hold.exited())
        .await
        .expect("owner primary exit hold did not close")
        .unwrap();
    durable_turn_until(&f.services, &f.graph_id, &f.program_id, "primary_run_exited", || {
        let graph = f.runtime().graph_state_store().load(&primary_id).ok()?;
        graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
            .then_some(())
    })
    .await;
    // The released owner execution is no longer active, so the new epoch is
    // claimed through the production dispatch wire (as in `established`), not by
    // borrowing the released actor. Re-dispatch until the Task is claimed again
    // at a newer generation; that epoch advance invalidates the old request.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        f.runtime()
            .dispatch_ready_agentic_work_for_test(&f.program_id)
            .await
            .unwrap();
        let p = f.project();
        if p.tasks[&task_ref].claim_generation > generation
            && p.tasks[&task_ref].claim_execution_id.is_some()
        {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("primary next epoch not claimed within 120s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let after = f.project();
    assert!(!after.coordination_request_current_for_test(&after.coordination_requests_for_test()[0].1));
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and the Gateway real same-source topology; run with --ignored --exact"]
async fn coordination_request_coalesces_and_preserves_scope() {
    let mut f = setup().await;
    let (team, _owner, helper, task_ref, author, _primary) = established(&mut f).await;
    let (_packet, _wake, revision) =
        dispatched_coordination(&f, &author, &team, &helper, &task_ref, "Check source A").await;
    for index in 0..128 {
        let mut duplicate = coordination_request(&f, &author, &team, &helper, &task_ref, "Check source A");
        duplicate.action_id = format!("duplicate-{index}");
        assert_eq!(
            f.runtime()
                .agent_action_service()
                .apply_for_test(&duplicate)
                .unwrap()
                .status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
    }
    let projection = f.project();
    assert_eq!(projection.coordination_requests_for_test().len(), 1);
    assert!(f
        .runtime()
        .dispatch_ready_agentic_work_for_test(&f.program_id)
        .await
        .unwrap()
        .is_empty());
    assert!(projection
        .coordination_team_id_for_test(&author.actor_id, revision)
        .is_none());
    assert!(projection
        .coordination_team_id_for_test("foreign-agent", revision)
        .is_none());
    let mut changed = coordination_request(&f, &author, &team, &helper, &task_ref, "New source B contradicts A");
    changed.action_id = "new-evidence".into();
    assert_eq!(
        f.runtime()
            .agent_action_service()
            .apply_for_test(&changed)
            .unwrap()
            .status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    assert_eq!(f.project().coordination_requests_for_test().len(), 2);
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and the Gateway real same-source topology; run with --ignored --exact"]
async fn coordination_missing_graph_recovers_same_registered_attempt() {
    use harness_contract::agent_action::{AgentAttemptMode, TaskAttemptDispatchInput};
    use sha2::Digest;
    let mut f = setup().await;
    let (team, _owner, helper, task_ref, author, primary_id) = established(&mut f).await;
    let runtime = f.runtime();
    let request = coordination_request(&f, &author, &team, &helper, &task_ref, "Check source A");
    runtime
        .agent_action_service()
        .apply_for_test(&request)
        .unwrap();
    let projection = f.project();
    let revision = projection.coordination_requests_for_test()[0].1.revision;
    let graph_id = format!(
        "agentic-graph:{:x}",
        sha2::Sha256::digest(
            format!("{}|{}|{}|coordination|{}", f.program_id, task_ref, helper, revision).as_bytes()
        )
    );
    // Mirror the production supervisor actor (agentic_supervisor_actor): the
    // binding must equal the frozen Program binding established by the real
    // root run, not the fixture's synthetic root_actor.
    let supervisor = AgentActorBinding {
        original_execution: None,
        objective_id: projection.objective_id.clone(),
        program_id: projection.program_id.clone(),
        session_id: projection.session_id.clone(),
        turn_id: projection.turn_id.clone(),
        root_execution_id: projection.root_execution_id.clone(),
        required_team_count: projection.required_team_count,
        exact_team_count: projection.exact_team_count,
        objective_summary: projection.objective_summary.clone(),
        model_lease: projection.model_lease.clone(),
        permission_ceiling: Some(projection.permission_ceiling),
        resource_scopes: projection.resource_scopes.clone(),
        actor_id: "runtime.program-supervisor".into(),
        kind: AgentActorKind::Supervisor,
        execution_id: Some(graph_id.clone()),
        team_id: None,
        agent_id: None,
    };
    let registered = runtime
        .agent_action_service()
        .apply_runtime_supervisor_for_test(&AgentActionEnvelope {
            action_id: "registered-before-crash".into(),
            actor: supervisor,
            expected_revision: None,
            action: AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                task_ref: task_ref.clone(),
                execution_id: graph_id.clone(),
                agent_ref: helper.clone(),
                membership_id: runtime::AgenticProgramProjection::membership_id(&helper, &team),
                mode: AgentAttemptMode::Coordination,
                generation: revision,
            }),
        })
        .unwrap();
    assert_eq!(
        registered.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{registered:?}"
    );
    // The same graph factory admits the coordination graph (the real PG
    // membership matches the durable registration). Build it first; recovery
    // then observes a live non-terminal graph and preserves the registration
    // without producing a receipt. The helper lane is not driven here, so the
    // dispatched coordination run parks at its checkpoint and the graph stays
    // non-terminal, making the empty recovery deterministic.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        runtime
            .dispatch_ready_agentic_work_for_test(&f.program_id)
            .await
            .unwrap();
        // Wait for the registered node attempt, not merely a loadable graph: the
        // factory can persist the graph before its recovery_cursor node attempt
        // is registered, and indexing node_attempts too early panics (flake).
        if let Ok(graph) = runtime.graph_state_store().load(&graph_id) {
            if graph
                .nodes
                .first()
                .is_some_and(|node| graph.recovery_cursor.node_attempts.contains_key(&node.id))
            {
                break;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("coordination graph not admitted by the factory within 120s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(runtime
        .recover_agentic_programs_on_startup()
        .await
        .unwrap()
        .is_empty());
    let projection = f.project();
    assert_eq!(projection.tasks[&task_ref].failed_attempts, 0);
    let attempts = &projection.tasks[&task_ref].active_attempts;
    assert_eq!(
        attempts
            .values()
            .filter(|attempt| attempt.mode == AgentAttemptMode::Coordination)
            .count(),
        1,
        "{attempts:?}"
    );
    assert!(
        attempts
            .values()
            .any(|attempt| attempt.execution_id == primary_id
                && attempt.mode == AgentAttemptMode::Execute),
        "{attempts:?}"
    );
    let graph = runtime.graph_state_store().load(&graph_id).unwrap();
    let node_id = graph.nodes[0].id.clone();
    let attempt = graph.recovery_cursor.node_attempts[&node_id];
    runtime
        .resolve_agent_action_actor_for_attempt(
            &harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: graph_id.clone(),
                node_id,
            },
            attempt,
            None,
        )
        .await
        .unwrap();
}

