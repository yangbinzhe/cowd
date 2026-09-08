#[test]
fn continued_goal_verifier_requires_current_execution_and_original_business_evidence() {
    use harness_contract::goal::{AcceptanceStatus, GoalCompletion, GoalContract};
    let original: GoalContract = serde_json::from_value(serde_json::json!({
        "id":"goal:old-root", "session_id":"continued-session",
        "objective":"Deliver the report from nine preserved inputs",
        "criteria":[{"id":"user_intent","statement":"Deliver the approved report",
            "source_refs":["session_message:original"],
            "required_evidence":["execution_graph:old-root","artifact://business-evidence"],
            "status":"open"}],
        "phase":"execution","completion":"open","revision":1,"user_sequence":1,
        "scope":"user_objective","source_intent_ref":"session_message:original",
        "user_intent_criterion_id":"user_intent","spec_revision":1,"spec_digest":"approved-spec",
        "execution_binding":{"objective_id":"old-objective","session_id":"continued-session",
            "turn_id":"old-turn","root_execution_id":"old-root","agentic_program_id":"old-program"}
    }))
    .unwrap();
    for include_business_evidence in [false, true] {
        let services = crate::RuntimeServices::in_memory().unwrap();
        services.goal_store().create(original.clone()).unwrap();
        let mut current = original.clone();
        current.id = "goal:new-root".into();
        current.objective = "继续".into();
        let binding = current.execution_binding.as_mut().unwrap();
        binding.objective_id = "new-objective".into();
        binding.turn_id = "new-turn".into();
        binding.root_execution_id = "new-root".into();
        binding.agentic_program_id = "new-program".into();
        inherit_continuation_goal(&mut current, &original, "new-root");
        assert_eq!(
            current.criteria[0].required_evidence,
            vec!["artifact://business-evidence", "execution_graph:new-root"]
        );
        assert!(current.criteria[0]
            .source_refs
            .contains(&"execution_graph:old-root".into()));
        assert_eq!(
            current
                .execution_binding
                .as_ref()
                .unwrap()
                .root_execution_id,
            "new-root"
        );
        services.goal_store().create(current).unwrap();
        let mut evidence = vec!["execution_graph:new-root".into()];
        if include_business_evidence {
            evidence.push("artifact://business-evidence".into());
        }
        let supervisor = crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(
            services.goal_store(),
        ));
        let result = supervisor.reconcile(
            "goal:new-root",
            1,
            "current-verifier-fence",
            vec![],
            evidence,
            vec![],
            false,
            "test current execution evidence",
        );
        let persisted = services.goal_store().get("goal:new-root").unwrap().unwrap();
        if include_business_evidence {
            assert!(result.is_ok(), "{result:?}");
            assert_eq!(persisted.completion, GoalCompletion::Satisfied);
            assert_eq!(persisted.criteria[0].status, AcceptanceStatus::Satisfied);
        } else {
            assert_ne!(
                persisted.completion,
                GoalCompletion::Satisfied,
                "a fresh execution alone cannot replace business evidence"
            );
        }
        assert_eq!(
            services.goal_store().get(&original.id).unwrap().unwrap(),
            original
        );
    }
}

#[derive(Clone)]
struct ContinuationEntryProbe(Arc<Mutex<Vec<ApiRequest>>>);
impl ApiClient for ContinuationEntryProbe {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        self.0.lock().unwrap().push(request);
        Box::pin(stream::iter(vec![Err(RuntimeError::new(
            "HTTP 402 Payment Required: deterministic test provider rejection",
        ))]))
    }
}

#[tokio::test]
async fn bare_continue_actual_host_inherits_goal_and_program_before_current_provider_request() {
    use harness_contract::{agent_action::*, execution_graph::*};
    let services = crate::RuntimeServices::in_memory().unwrap();
    let mut session = Session::new();
    session.session_id = "continued-host-session".into();
    session.model = Some("qwen3.8-max".into());
    let session_id = session.session_id.clone();
    let objective_id = root_objective_id(&session_id, "old-turn");
    let old = AgentActorBinding {
        program_id: program_id_for_objective(&objective_id),
        objective_id,
        session_id: session_id.clone(),
        turn_id: "old-turn".into(),
        root_execution_id: Some("old-host-root".into()),
        required_team_count: 1,
        objective_summary: "Deliver the original source-backed report".into(),
        model_lease: "old-model".into(),
        permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
        resource_scopes: vec!["read:.".into()],
        actor_id: format!("root:{session_id}"),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let mut graph = ExecutionGraph::new(&old.objective_summary);
    graph.id = "old-host-root".into();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: session_id.clone(),
        turn_id: "old-turn".into(),
        root_task_id: "old-task".into(),
        task_id: "old-task".into(),
        generation: 1,
    });
    let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "source");
    node.id = "old-host-model".into();
    graph.nodes.push(node);
    services.commit_service().register_graph(graph).unwrap();
    let actions = crate::AgentActionService::new(Arc::clone(services.event_store()));
    let team = actions
        .apply(&AgentActionEnvelope {
            action_id: "continued-source-team".into(),
            actor: old.clone(),
            expected_revision: None,
            action: AgentAction::TeamCreate(TeamCreateInput {
                name: "Original evidence team".into(),
                mission: "Finish source report".into(),
                objective: None,
            }),
        })
        .unwrap();
    assert_eq!(team.status, AgentActionStatus::Applied);
    let source = actions.project(&old.program_id).unwrap();
    let stopped = services.graph_state_store().load("old-host-root").unwrap();
    services
        .commit_service()
        .apply_command(
            &stopped,
            &ExecutionGraphCommand::Cancel {
                expected_revision: stopped.revision,
                reason: "prior provider interruption fixture".into(),
            },
        )
        .unwrap();
    let original: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
        "id":"goal:old-host-root","session_id":session_id,"objective":old.objective_summary,
        "criteria":[{"id":"user_intent","statement":"Complete the original report","source_refs":["session_message:original"],
            "required_evidence":["execution_graph:old-host-root"],"status":"open"}],
        "phase":"execution","completion":"open","revision":1,"user_sequence":1,
        "source_intent_ref":"session_message:original","user_intent_criterion_id":"user_intent",
        "spec_revision":1,"spec_digest":"original-approved-contract",
        "execution_binding":{"objective_id":old.objective_id,"session_id":session_id,"turn_id":"old-turn",
            "root_execution_id":"old-host-root","agentic_program_id":old.program_id}
    })).unwrap();
    services.goal_store().create(original.clone()).unwrap();
    let captured = Arc::new(Mutex::new(vec![]));
    let mut runtime = crate::ConversationRuntime::new(
        session,
        ContinuationEntryProbe(Arc::clone(&captured)),
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        canonical_host_system_prompt(vec![]),
    )
    .without_memory();
    runtime.set_active_model("qwen3.8-max");
    let mut lineage = test_execution_lineage();
    lineage.turn_id = "continued-turn".into();
    lineage.task_id = "continued-task".into();
    lineage.root_task_id = lineage.task_id.clone();
    let (_runtime, result) = submit_test_owned_conversation_turn(
        runtime,
        Arc::clone(&services),
        "继续",
        &SharedPrompter::none(),
        lineage,
    )
    .await;
    if let Ok(summary) = &result {
        assert_ne!(
            summary.terminal_completion,
            harness_contract::goal::GoalCompletion::Satisfied,
            "provider rejection cannot complete the objective"
        );
    }
    let requests = captured.lock().unwrap();
    assert!(
        !requests.is_empty(),
        "continuation must reach provider: {result:?}"
    );
    let request_text = format!("{:?}", requests[0]);
    assert!(
        request_text.contains(&old.objective_summary),
        "original objective must reach actual request"
    );
    assert_eq!(
        requests[0].model, "qwen3.8-max",
        "current model must replace old lease"
    );
    let new_program_id =
        program_id_for_objective(&root_objective_id(&session_id, "continued-turn"));
    let inherited = actions.project(&new_program_id).unwrap();
    assert_eq!(inherited.teams, source.teams);
    assert_eq!(inherited.model_lease, "qwen3.8-max");
    assert_eq!(
        inherited.continuation.as_ref().unwrap().source_program_id,
        old.program_id
    );
    let new_root = inherited.root_execution_id.as_ref().unwrap();
    let new_goal = services
        .goal_store()
        .get(&format!("goal:{new_root}"))
        .unwrap()
        .unwrap();
    assert_eq!(new_goal.objective, original.objective);
    assert_eq!(new_goal.spec_digest, original.spec_digest);
    assert_eq!(
        new_goal
            .execution_binding
            .as_ref()
            .unwrap()
            .root_execution_id,
        *new_root
    );
    assert_eq!(actions.project(&old.program_id).unwrap(), source);
    assert_eq!(
        services.goal_store().get(&original.id).unwrap().unwrap(),
        original
    );
}
