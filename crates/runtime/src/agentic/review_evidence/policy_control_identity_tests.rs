// Independent identities and expected digest authored from the original
// Session/Step/call tuple and the literal business manifest, not from the
// production identity selector or manifest reader.
const RETIRE_CALL_KEY: &str =
    "sha256:1317ef08880a276724a4c9c11a56fd6767bdb127c055fcae57abc89443fa2960";
const BUSINESS_ONLY_DIGEST: &str =
    "8860f2773dc9ebdfc3ed6a9de14042682a0e194831de54354094032d8b001ae2";

fn control_plan_task() -> serde_json::Value {
    serde_json::json!({
        "tool_name":"task_withdraw", "tool_call_id":"fixture-retire",
        "idempotency_key":"task_withdraw:fixture-retire:legacy-descriptor",
        "effect":{"effect_kind":"write"},
        "normalized_input":{"task_ref":"review-control","reason_ref":"artifact://reason","evidence_refs":["artifact://read"]},
        "step_binding":{
            "origin":{"origin_step_id":"fixture-step","call_id":"fixture-retire","catalog_digest":"catalog",
                "source":{"kind":"provider","attempt_id":"fixture-provider-attempt"}},
            "snapshot":{"contract_version":1,
                "identity":{"step_id":"fixture-step","session_id":"fixture-session","turn_id":"fixture-turn",
                    "execution_id":"fixture-root","actor_ref":"fixture-principal","run_ref":"fixture-root",
                    "attempt":1,"epoch":"fixture-epoch","program_ref":"fixture-program","task_ref":"review-control","build_ref":"fixture-build"},
                "provider_attempt":{"attempt_id":"fixture-provider-attempt","origin_step_id":"fixture-step","model_profile_digest":"profile","request_digest":"request"},
                "catalog_digest":"catalog","tools":[],"related_revisions":{},"context_refs":[],"expires_at_ms":0
            }
        }
    })
}

fn read_control_manifest(
    task: Option<serde_json::Value>,
    authenticated: bool,
    changed_business: bool,
) -> String {
    let store = crate::RuntimeEventStore::for_test();
    if let Some(task) = task {
        store
            .append(crate::RuntimeEventInput {
                stream_id: "session:fixture-session".into(),
                scope: crate::RuntimeEventScope::Tool,
                kind: "tool.execution_plan.created".into(),
                status: Some("planned".into()),
                actor: Some("conversation_runtime".into()),
                refs: vec![],
                payload: serde_json::json!({"plan_id":"original-plan","tasks":[task]}),
            })
            .unwrap();
    }
    let mut required = std::collections::BTreeMap::from([(
        "business-primary".to_owned(),
        serde_json::json!({"idempotency_key":"business-effect","effect_kind":"write",
            "bytes":if changed_business {"changed user work"} else {"original user work"}}),
    )]);
    let plans = pinned_stream_events(
        &store,
        "session:fixture-session",
        &mut Default::default(),
        0,
        PinnedEventProjection::Effects,
    )
    .unwrap();
    for plan in &plans {
        required.extend(delegated_plan_required_effects(plan).unwrap());
    }
    if !plans.is_empty() {
        required.insert(
            "control-primary".into(),
            serde_json::json!({"idempotency_key":RETIRE_CALL_KEY,"effect_kind":"write"}),
        );
        required.insert(
            "control-started".into(),
            serde_json::json!({"tool_call_id":"fixture-retire","governed_plan_id":"original-plan"}),
        );
        required.insert(
            "control-completed".into(),
            serde_json::json!({"tool_call_id":"fixture-retire","governed_plan_id":"original-plan"}),
        );
    }
    let controls = if authenticated {
        BTreeSet::from([RETIRE_CALL_KEY.to_owned()])
    } else {
        BTreeSet::new()
    };
    let lifecycle = std::collections::BTreeMap::from([
        ("control-started".into(), RETIRE_CALL_KEY.to_owned()),
        ("control-completed".into(), RETIRE_CALL_KEY.to_owned()),
    ]);
    strip_authenticated_runtime_control_effects(&mut required, &controls, &lifecycle);
    super::super::proof_value_digest(&required)
}

#[test]
fn r1_production_plan_reader_absorbs_own_control_with_legacy_plan_key() {
    assert_eq!(
        read_control_manifest(None, true, false),
        BUSINESS_ONLY_DIGEST
    );
    assert_eq!(
        read_control_manifest(Some(control_plan_task()), true, false),
        BUSINESS_ONLY_DIGEST,
        "the plan, primary and Session lifecycle all name the authenticated original Step effect"
    );
}

#[test]
fn r1_production_plan_reader_preserves_foreign_unknown_and_business_changes() {
    assert_ne!(
        read_control_manifest(Some(control_plan_task()), false, false),
        BUSINESS_ONLY_DIGEST,
        "a control tool name is not authentication"
    );
    let mut sibling = control_plan_task();
    sibling["step_binding"]["origin"]["call_id"] = serde_json::json!("foreign-call");
    sibling["idempotency_key"] = serde_json::json!(RETIRE_CALL_KEY);
    assert_ne!(
        read_control_manifest(Some(sibling), true, false),
        BUSINESS_ONLY_DIGEST,
        "a sibling Step cannot borrow another call's canonical key through the old plan key field"
    );
    let mut unknown = control_plan_task();
    unknown["step_binding"] = serde_json::json!({"unknown":"not an authenticated Step"});
    unknown["idempotency_key"] = serde_json::json!(RETIRE_CALL_KEY);
    assert_ne!(
        read_control_manifest(Some(unknown), true, false),
        BUSINESS_ONLY_DIGEST,
        "a malformed claimed Step never falls back to a control-looking key"
    );
    assert_ne!(
        read_control_manifest(Some(control_plan_task()), true, true),
        BUSINESS_ONLY_DIGEST,
        "a real business change must invalidate the original proof"
    );
}

#[test]
fn r1_session_frame_retains_read_plans_for_continuation_binding() {
    let store = crate::RuntimeEventStore::for_test();
    store.append(crate::RuntimeEventInput {
        stream_id: "session:original".into(), scope: crate::RuntimeEventScope::Tool,
        kind: "tool.execution_plan.created".into(), status: Some("planned".into()),
        actor: Some("conversation_runtime".into()), refs: vec![],
        payload: serde_json::json!({"plan_id":"read-plan", "tasks":[{"tool_name":"read_file","effect":{"effect_kind":"read"}}]}),
    }).unwrap();
    let mut sources = std::collections::BTreeMap::new();
    let frame = pinned_stream_events(
        &store,
        "session:original",
        &mut sources,
        0,
        PinnedEventProjection::EffectsAndPlans,
    )
    .unwrap();
    assert_eq!(
        frame.len(),
        1,
        "original governed plan remains available for identity checks"
    );
    assert_eq!(sources.get("session:original"), Some(&1));
    assert!(pinned_stream_events(
        &store,
        "session:original",
        &mut Default::default(),
        0,
        PinnedEventProjection::Effects
    )
    .unwrap()
    .is_empty());
    assert!(
        delegated_plan_required_effects(&frame[0])
            .unwrap()
            .is_empty(),
        "retaining identity does not invent a read-side business write"
    );
}
