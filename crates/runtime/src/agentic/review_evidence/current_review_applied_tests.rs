#[test]
fn r1_a_applied_review_recognises_its_own_call() {
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
            Some("original".into())
        )
        .unwrap(),
        Some("original".into()),
        "the durable action that carries the proof is the applied review's own call"
    );
}

#[test]
fn r1_a_live_review_that_authenticates_to_nothing_exempts_nothing() {
    let action = AgentAction::TaskReview(TaskReviewInput {
        gaps: Vec::new(),
        directed_actions: Vec::new(),
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
    assert_eq!(
        select_current_review_effect_key(
            Some(ReviewActionIdentity::Live(&live)),
            None,
            Some("historical".into())
        )
        .unwrap(),
        None,
        "a stored candidate cannot replace a live call that authenticates to nothing"
    );
}

#[test]
fn r1_a_live_review_binds_to_the_call_it_performs() {
    let action = AgentAction::TaskReview(TaskReviewInput {
        gaps: Vec::new(),
        directed_actions: Vec::new(),
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
    assert_eq!(
        select_current_review_effect_key(
            Some(ReviewActionIdentity::Live(&live)),
            Some("current".into()),
            None
        )
        .unwrap(),
        Some("current".into())
    );
    assert!(
        select_current_review_effect_key(
            Some(ReviewActionIdentity::Live(&live)),
            Some("current".into()),
            Some("different".into())
        )
        .is_err()
    );
}

#[test]
fn r1_root_applied_review_uses_the_durable_action_without_a_delivery_packet() {
    let root = AppliedReviewContext {
        action_id: "root-review-control",
        program_source: "agentic-program:program",
        pinned: 1,
        through: 2,
        program_events: &[],
    };
    assert_eq!(
        select_current_review_effect_key(Some(ReviewActionIdentity::Applied(&root)), None, None)
            .unwrap(),
        Some("root-review-control".to_owned()),
        "a root review has no delivery packet, but the durable action still authenticates its own control call"
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
