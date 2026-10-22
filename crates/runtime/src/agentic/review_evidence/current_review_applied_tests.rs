#[test]
fn applied_review_uses_authenticated_rebound_without_rescuing_failed_live_identity() {
    let action = AgentAction::TaskReview(TaskReviewInput {
        task_ref: "task".into(),
        decision: TaskReviewDecision::Accept,
        reason: "actual independent evidence".into(),
        operator_requirement: None,
        evidence_refs: vec![],
    });
    let actor = actor();
    let live = CurrentReviewContext {
        action_id: "original",
        action: &action,
        actor: &actor,
    };
    let applied = AppliedReviewContext {
        action_id: "original",
        program_source: "agentic-program:program",
        pinned: 1,
        through: 1,
        program_events: &[],
    };
    assert_eq!(
        select_current_review_effect_key(
            Some(ReviewActionIdentity::Applied(&applied)),
            None,
            Some("authenticated".into())
        )
        .unwrap(),
        Some("authenticated".into())
    );
    assert_eq!(
        select_current_review_effect_key(
            Some(ReviewActionIdentity::Live(&live)),
            None,
            Some("historical".into())
        )
        .unwrap(),
        None
    );
    assert!(select_current_review_effect_key(
        Some(ReviewActionIdentity::Live(&live)),
        Some("current".into()),
        Some("different".into())
    )
    .is_err());
    assert_eq!(
        select_current_review_effect_key(Some(ReviewActionIdentity::Applied(&applied)), None, None)
            .unwrap(),
        None
    );
}

#[test]
fn applied_review_tail_can_cross_eight_records_but_still_refuses_business_and_unknown_events() {
    let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
    append_program_event(&store, OBJECTIVE_REVIEW_TOOL_ID, "original-review");
    for _ in 0..70 {
        store
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:program".into(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.completion_reopened".into(),
                status: Some("open".into()),
                actor: Some("runtime.objective_supervisor".into()),
                refs: vec![],
                payload: serde_json::json!({"gaps":["prior judgement failed"]}),
            })
            .unwrap();
    }
    let through = store.stream_revision("agentic-program:program").unwrap();
    assert_eq!(
        program_events_after_cut(&store, "agentic-program:program", 0, through)
            .unwrap()
            .len(),
        71
    );
    assert!(test_program_cut_absorbs_own_review(
        &store,
        "agentic-program:program",
        0,
        through,
        "original-review"
    ));
    assert!(!test_program_cut_absorbs_own_review(
        &store,
        "agentic-program:program",
        through + 1,
        through,
        "original-review"
    ));
    append_program_event(&store, "task_submit", "new-deliverable");
    let advanced = store.stream_revision("agentic-program:program").unwrap();
    assert!(!test_program_cut_absorbs_own_review(
        &store,
        "agentic-program:program",
        0,
        advanced,
        "original-review"
    ));
    assert!(
        program_events_after_cut(&store, "agentic-program:program", 0, 1025)
            .unwrap_err()
            .contains("budget")
    );
}
