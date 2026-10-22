#[test]
fn all_task_purposes_keep_worker_bookkeeping_out_of_both_work_manifests() {
    for purpose in [
        harness_contract::agent_action::TaskPurpose::Delivery,
        harness_contract::agent_action::TaskPurpose::Exploration,
    ] {
        let mut program = AgenticProgramProjection::empty("program", "objective");
        let mut task = reviewed_task();
        task.task_id = "task:reviewed".into();
        task.purpose = purpose;
        task.artifact_refs.clear();
        task.active_attempts.clear();
        task.status = crate::AgenticTaskStatus::Accepted;
        program.tasks.insert(task.task_id.clone(), task);
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let selected = |program: &AgenticProgramProjection| {
            selected_work(
                &store,
                program,
                None,
                &["task:reviewed".into()],
                &mut BTreeMap::new(),
            )
            .unwrap()
            .2
        };
        // Expected contracts are authored independently of either production projection.
        let purpose_name = match purpose {
            harness_contract::agent_action::TaskPurpose::Delivery => "delivery",
            harness_contract::agent_action::TaskPurpose::Exploration => "exploration",
        };
        let expected_task = serde_json::json!({
            "task_id":"task:reviewed", "team_id":"team", "title":"long task",
            "objective":"finish without an arbitrary wall timeout", "acceptance":"durable evidence",
            "acceptance_checks":[], "criterion_source":"operator_derived",
            "required_capabilities":[], "obligation_refs":[], "purpose":purpose_name,
            "execution_requirements":[], "expertise_hints":[], "depends_on":[],
            "artifact_refs":[], "evidence_refs":[], "unresolved":[], "review_reason":null,
            "replacement_task_refs":[], "supersede_evidence_refs":[]
        });
        let expected_full = serde_json::json!({
            "program":"program", "objective":"objective", "continuation":null,
            "teams":{}, "memberships":{}, "agents":{}, "tasks":{"task:reviewed":expected_task},
            "artifacts":{}, "topics":{}, "required_team_count":0
        });
        let expected_selected = serde_json::json!({
            "domain":"cowd.proof.work.v2", "program":"program", "objective":"objective",
            "continuation":null, "tasks":{"task:reviewed":{
                "team":"team", "objective":"finish without an arbitrary wall timeout",
                "acceptance":"durable evidence", "acceptance_checks":[],
                "criterion_source":"operator_derived", "obligations":[], "capabilities":[],
                "execution_requirements":[], "purpose":purpose_name, "dependencies":[],
                "artifacts":[], "unresolved":[], "replacements":[]
            }}, "artifacts":{}, "required_teams":0, "exact_teams":null
        });
        let full_before = format!("{:x}", Sha256::digest(expected_full.to_string().as_bytes()));
        let selected_before = format!(
            "{:x}",
            Sha256::digest(expected_selected.to_string().as_bytes())
        );
        assert_eq!(work_manifest_digest(&program), full_before);
        assert_eq!(selected(&program), selected_before);
        let task = program.tasks.get_mut("task:reviewed").unwrap();
        task.claimant = Some("new-worker".into());
        task.claim_generation += 1;
        task.claim_execution_id = Some("new-execution".into());
        task.claim_renewal_streak += 1;
        task.claimed_at_ms = Some(100);
        task.lease_expires_at_ms = Some(200);
        task.failed_attempts += 1;
        task.review_generation += 1;
        task.failed_review_attempts += 1;
        task.last_failure = Some("settled attempt".into());
        task.reviewed_by = Some("independent-reviewer".into());
        task.cancel_requested_by = Some("root".into());
        task.cancel_reason_ref = Some("artifact://retire-reason".into());
        task.cancel_evidence_refs = vec!["tool://retire".into()];
        task.pending_retirement = Some(crate::agentic::program::AgenticTaskRetirement::Withdrawn);
        task.status = crate::AgenticTaskStatus::Withdrawn;
        assert_eq!(
            work_manifest_digest(&program),
            full_before,
            "{purpose:?}: bookkeeping is not work"
        );
        assert_eq!(
            selected(&program),
            selected_before,
            "{purpose:?}: actual selected proof path must agree"
        );
        program.tasks.get_mut("task:reviewed").unwrap().objective =
            "a different requested result".into();
        assert_ne!(
            work_manifest_digest(&program),
            full_before,
            "changed objective must be stale"
        );
        assert_ne!(
            selected(&program),
            selected_before,
            "changed objective must be stale in actual proof path"
        );
        program.tasks.get_mut("task:reviewed").unwrap().acceptance =
            "a different acceptance contract".into();
        assert_ne!(
            selected(&program),
            selected_before,
            "changed acceptance must remain material"
        );
    }
}

#[test]
fn source_advance_diagnostic_is_bounded_and_never_renders_payloads() {
    let store = crate::RuntimeEventStore::for_test();
    for _ in 0..12 {
        store
            .append(crate::RuntimeEventInput {
                stream_id: "diagnostic-source".into(),
                scope: crate::RuntimeEventScope::Relation,
                kind: "fixture.source_writer".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({"private_body":"do-not-render-this-body"}),
            })
            .unwrap();
    }
    let diagnostic = describe_source_advance(&store, "diagnostic-source", 0);
    assert!(diagnostic.contains("advanced=8 head=12"), "{diagnostic}");
    assert!(diagnostic.contains("fixture.source_writer"));
    assert!(!diagnostic.contains("#9:"));
    assert!(!diagnostic.contains("do-not-render-this-body"));
}
