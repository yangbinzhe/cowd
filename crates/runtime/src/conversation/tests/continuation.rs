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



// This test exercises pre-execution Goal genesis. Inventory availability is
// controlled; the rejecting Provider never asks to execute a Team tool.
