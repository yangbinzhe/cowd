use super::*;

fn plan736_context() -> AgenticDispatchContext {
    AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "unconfigured-no-provider".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    }
}

#[tokio::test]
async fn plan736_unpolled_dispatch_is_small_and_missing_program_never_admits() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    let envelope = super::reviewed_goal_fixture::root(
        "plan736-unpolled",
        AgentAction::StateInspect(harness_contract::agent_action::StateInspectInput {
            query: None,
            wait_for_workers: false,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
        }),
    );
    let future = services.dispatch_agentic_followups(&envelope, plan736_context());
    assert!(std::mem::size_of_val(&future) <= 2 * std::mem::size_of::<usize>());
    drop(future);
    assert!(
        services
            .agent_action_service()
            .project_if_exists(&envelope.actor.program_id)
            .unwrap()
            .is_none()
    );
    assert!(
        services
            .dispatch_agentic_followups(&envelope, plan736_context())
            .await
            .is_err()
    );
    assert!(
        services
            .agent_action_service()
            .project_if_exists(&envelope.actor.program_id)
            .unwrap()
            .is_none()
    );
    assert!(services.agent_runtime().list().is_empty());
}

#[test]
#[ignore = "R1 diagnostic gate: parent must supply newly measured equivalent production Runtime stack size (original crash stack lost) and retained events"]
fn plan736_retained_publish_no_members_isolated_stack_probe() {
    if std::env::var_os("PLAN736_STACK_PROBE_CHILD").is_none() {
        let current_thread = std::thread::current();
        let test_name = current_thread.name().expect("exact test name");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--ignored", "--nocapture"])
            .env("PLAN736_STACK_PROBE_CHILD", "1")
            .output()
            .expect("isolated test child");
        assert!(
            output.status.success(),
            "status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let stack_bytes: usize = std::env::var("PLAN736_WORKER_STACK_BYTES")
        .expect("parent must supply newly measured equivalent production Runtime stack size; original crash stack is lost and was not measured")
        .parse()
        .unwrap();
    let events_path = std::env::var("PLAN736_EVENTS_JSON").expect("738 retained events path");
    let events: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(events_path).unwrap()).unwrap();
    let retained = events
        .iter()
        .rev()
        .find(|event| event["kind"] == "agentic.action_applied")
        .unwrap()["payload"]["envelope"]["action"]
        .clone();
    assert_eq!(retained["kind"], "task_publish");
    std::thread::Builder::new()
        .name("plan736-production-stack-probe".into())
        .stack_size(stack_bytes)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let services = Arc::new(RuntimeServices::in_memory().unwrap());
                    let team = super::reviewed_goal_fixture::root(
                        "plan736-team",
                        AgentAction::TeamCreate(harness_contract::agent_action::TeamCreateInput {
                            name: "No members, no provider".into(),
                            mission: "isolate dispatch stack construction".into(),
                            objective: None,
                        }),
                    );
                    let team_ref = services
                        .agent_action_service()
                        .apply(&team)
                        .unwrap()
                        .changed_refs[0]
                        .clone();
                    for sequence in 0..10 {
                        let mut action = retained.clone();
                        action["input"]["team_ref"] = team_ref.clone().into();
                        let envelope = super::reviewed_goal_fixture::root(
                            &format!("plan736-retained-publish-{sequence}"),
                            serde_json::from_value(action).unwrap(),
                        );
                        let applied = services.agent_action_service().apply(&envelope).unwrap();
                        assert_eq!(
                            applied.status,
                            harness_contract::agent_action::AgentActionStatus::Applied
                        );
                        let receipts = services
                            .dispatch_agentic_followups(&envelope, plan736_context())
                            .await
                            .unwrap();
                        assert!(receipts.is_empty());
                    }
                });
        })
        .unwrap()
        .join()
        .unwrap();
}
