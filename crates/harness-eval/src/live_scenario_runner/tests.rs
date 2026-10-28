use super::*;

// U5 fixtures test consumer decisions; the producer test uses real in-memory
// Runtime resource events, but supplies no live owner/Gateway/provider IO proof.
fn u5_fixture_owner() -> U5Owner {
    U5Owner {
        pid: 42,
        starttime: 17,
        image_sha256: format!("sha256:{}", "a".repeat(64)),
        candidate: "b".repeat(40),
        namespace: "fixture-run".into(),
        base_url: "http://127.0.0.1:12345".into(),
        principal_actor: "principal:fixture".into(),
    }
}

fn u5_fixture_id(side: &str) -> U5Identity {
    U5Identity {
        session: format!("session-{side}"),
        turn: format!("turn-{side}"),
        root: format!("root-{side}"),
    }
}

fn u5_fixture_report(side: &str) -> Value {
    let id = u5_fixture_id(side);
    json!({"status":"passed","session_id":id.session,"turn_id":id.turn,"execution_id":id.root,
        "acceptance":{"passed":true},"actor_cleanup":{"passed":true}})
}

fn u5_fixture_resources() -> U5ResourceEvidence {
    use runtime::execution_core::graph::*;
    let observation = |request_id, side: &str, status| ResourceAdmissionObservation {
        request_id,
        status,
        requested_priority: None,
        deadline_at_ms: Some(1000),
        requested_service_class: ExecutionServiceClass::Foreground,
        resolved_service_class: ExecutionServiceClass::Foreground,
        parent_class_ceiling: None,
        demands: vec![(ExecutionResourceKind::Provider, 1)],
        normalized_scope: None,
        fairness_key: format!("session:session-{side}"),
        enqueue_sequence: Some(if side == "a" { 1 } else { 2 }),
        enqueued_at_ms: Some(10),
        observed_at_ms: 20,
        queue_age_ms: 10,
        wait_reason: Some(ResourceWaitReason::Capacity),
        blocker: None,
        policy_revision: 1,
        pending: 0,
    };
    let a = uuid::Uuid::from_u128(1);
    let b = uuid::Uuid::from_u128(2);
    U5ResourceEvidence {
        owner: u5_fixture_owner(),
        selected_witnesses_complete: true,
        source_refs: vec![
            "fixture:grant-a".into(),
            "fixture:wait-b".into(),
            "fixture:grant-b".into(),
        ],
        observations: vec![
            observation(a, "a", ResourceAdmissionObservationStatus::Granted),
            ResourceAdmissionObservation {
                blocker: Some(a),
                ..observation(b, "b", ResourceAdmissionObservationStatus::Waiting)
            },
            ResourceAdmissionObservation {
                blocker: Some(a),
                ..observation(b, "b", ResourceAdmissionObservationStatus::Granted)
            },
        ],
    }
}

fn u5_fixture_cancelled() -> Value {
    let id = u5_fixture_id("a");
    let mut result = json!({"status":"cancelled","session_id":id.session,"turn_id":id.turn,"execution_id":id.root,
        "actor_cleanup":{"passed":true},
        "receipt":{"cancellation_id":"fixture-cancel","session_id":id.session,"turn_id":id.turn,"execution_id":id.root,
            "actor_id":"principal:fixture","cause":"user_requested","requested_at_ms":10,"effective_at_ms":20,
            "status":"cancelled","journal_sequence":2,"projection_revision":3},
        "projection":{"session_id":id.session,"turn_id":id.turn,"execution_id":id.root,"terminal_axes":{
            "outcome":{"goal_id":null,"goal_revision":null,"outcome":null,"terminal_fence":null},
            "execution":{"phase":"terminal","settlement":"proven_settled","coverage_complete":true,"evidence_refs":["fixture:closure-a"]},
            "delivery":{"runtime_terminal_ref":null,"session_client_evidence":"requires_fresh_context"},
            "source_cursor":30,"source_race":false}}});
    result["physical_events"] = json!(u5_fixture_physical_events());
    result
}

fn u5_fixture_cancel_context() -> U5CancelContext {
    U5CancelContext {
        identity: u5_fixture_id("a"),
        cancellation_id: "fixture-cancel".into(),
        started_source: "opened-a".into(),
        opened_event: u5_fixture_physical_events()[0].clone(),
    }
}

// Wire-only selected source records. These are not a live authorization or
// an SDK acknowledgement; the consumer must still reject each mutation.
fn u5_fixture_physical_events() -> Vec<Value> {
    use runtime::provider_runtime_client::{
        ProviderPhysicalIdentity, ProviderPhysicalObservation, ProviderPhysicalReceipt,
        RequestedCancellationReference,
    };
    let identity = ProviderPhysicalIdentity {
        session_id: "session-a".into(),
        turn_id: "turn-a".into(),
        execution_id: "root-a".into(),
        request_sequence: 1,
        request_id: "provider-call-a".into(),
        response_id: "response-a".into(),
        opened_at_ms: 5,
        logical_attempt: 1,
        step: harness_contract::step::StepIdentity {
            step_id: "step-a".into(),
            session_id: "session-a".into(),
            turn_id: "turn-a".into(),
            execution_id: Some("root-a".into()),
            actor_ref: Some("principal:fixture".into()),
            run_ref: None,
            attempt: Some(1),
            epoch: Some("epoch-a".into()),
            goal_ref: None,
            program_ref: None,
            task_ref: None,
            build_ref: "build-a".into(),
        },
        provider_attempt: harness_contract::step::ProviderAttemptIdentity {
            attempt_id: "attempt-a".into(),
            origin_step_id: "step-a".into(),
            model_profile_digest: "profile-a".into(),
            request_digest: "request-a".into(),
        },
        resource_lease: format!(
            "resource-lease:{}:{}",
            uuid::Uuid::from_u128(3),
            uuid::Uuid::from_u128(1)
        ),
    };
    let open = serde_json::to_value(ProviderPhysicalObservation::ResponseOpened {
        identity: identity.clone(),
    })
    .unwrap();
    let requested = RequestedCancellationReference {
        cancellation_id: "fixture-cancel".into(),
        stream_id: "cancellation:fixture-cancel".into(),
        event_sequence: 1,
        commit_cursor: 2,
        payload_digest: format!("sha256:{}", "d".repeat(64)),
    };
    let receipt: harness_contract::turn::CancellationReceipt = serde_json::from_value(json!({
        "cancellation_id":"fixture-cancel","session_id":"session-a","turn_id":"turn-a","execution_id":"root-a",
        "actor_id":"principal:fixture","cause":"user_requested","requested_at_ms":10,"effective_at_ms":null,
        "status":"requested","journal_sequence":2,"projection_revision":1
    })).unwrap();
    let binding = json!({"ingress_message_id":"message-a","request_id":"ingress-a","input_sequence":0,
        "session_generation":1,"claim_owner":"claim-a","claim_fence_epoch":1,
        "claim_token_digest":format!("sha256:{}","a".repeat(64))});
    let requested_ref = json!({"event_id":"requested-event-a","stream_id":requested.stream_id,
        "event_sequence":1,"commit_cursor":2,"payload_digest":requested.payload_digest});
    let mut payload = json!({"schema_version":1,"cancellation_id":"fixture-cancel",
        "receipt":receipt,"claim_binding":binding,"requested_ref":requested_ref});
    let digest = harness_contract::step::canonical_digest(
        "session-cancel-authorization-payload:v1",
        &json!({"kind":"session.claim.cancellation_authorized.v1","session_id":"session-a",
            "turn_id":"turn-a","payload":payload}),
    )
    .unwrap();
    payload["authorization_digest"] = json!(digest);
    let key = harness_contract::step::canonical_digest(
        "session-cancel-authorization-key:v1",
        &json!({"session_id":"session-a","request_id":"ingress-a","input_sequence":0,
            "session_generation":1,"claim_owner":"claim-a","claim_fence_epoch":1,
            "claim_token_digest":binding["claim_token_digest"],"cancellation_id":"fixture-cancel"}),
    )
    .unwrap();
    let cancelled = ProviderPhysicalObservation::CancellationObserved {
        identity: identity.clone(),
        observed_at_ms: 15,
        opened: ProviderPhysicalReceipt {
            identity,
            session_sequence: 1,
            observation_digest: harness_contract::step::canonical_digest(
                "provider-physical-observation",
                &open,
            )
            .unwrap(),
        },
        requested,
    };
    vec![
        json!({"event_id":"opened-a","sequence":1,"session_id":"session-a","type":"context.provider_physical_observation",
            "status":"opened","payload":open}),
        json!({"event_id":format!("session-cancel-authorization:v1:{key}"),"sequence":2,"session_id":"session-a",
            "type":"session.claim.cancellation_authorized.v1","status":"authorized","payload":payload}),
        json!({"event_id":"cancel-observed-a","sequence":3,"session_id":"session-a","type":"context.provider_physical_observation",
            "status":"cancel_observed","payload":cancelled}),
    ]
}

#[test]
fn r202_physical_consumer_requires_exact_original_open_authorization_and_lease() {
    let request = u5_fixture_cancel_context();
    let owner = u5_fixture_owner();
    let events = u5_fixture_physical_events();
    let resources = u5_fixture_resources();
    let proof = u5_physical_cancel_evidence(&request, &owner, &events, &resources).unwrap();
    assert_eq!(proof["remote_server_inference_stop"], "unknown");
    assert_eq!(proof["remote_billing_stop"], "unknown");
    for fault in [
        "wrong-actor",
        "old-response",
        "old-attempt",
        "missing-authorization",
        "unknown-authorization",
        "EOF-won",
        "missing-lease",
        "foreign-lease",
        "missing-open",
        "changed-open",
        "wrong-source",
        "wrong-auth-digest",
        "wrong-auth-key",
        "duplicate-authorization",
        "wrong-step",
        "wrong-requested-digest",
    ] {
        let mut events = events.clone();
        let mut resources = resources.clone();
        let mut request = request.clone();
        match fault {
            "wrong-actor" => {
                events[1]["payload"]["receipt"]["actor_id"] = json!("principal:foreign")
            }
            "old-response" => {
                events[2]["payload"]["identity"]["response_id"] = json!("old-response")
            }
            "old-attempt" => events[2]["payload"]["identity"]["logical_attempt"] = json!(0),
            "missing-authorization" => {
                events.remove(1);
            }
            "unknown-authorization" => events[1]["status"] = json!("unknown"),
            "EOF-won" => {
                events.pop();
            }
            "missing-lease" => {
                resources.observations.remove(0);
                resources.source_refs.remove(0);
            }
            "foreign-lease" => resources.observations[0].fairness_key = "session:foreign".into(),
            "missing-open" => {
                events.remove(0);
            }
            "changed-open" => events[0]["payload"]["identity"]["opened_at_ms"] = json!(6),
            "wrong-source" => request.started_source = "model-authored-active".into(),
            "wrong-auth-digest" => events[1]["payload"]["authorization_digest"] = json!("bad"),
            "wrong-auth-key" => events[1]["event_id"] = json!("other-auth"),
            "duplicate-authorization" => events.push(events[1].clone()),
            "wrong-step" => events[2]["payload"]["identity"]["step"]["step_id"] = json!("old-step"),
            "wrong-requested-digest" => {
                events[2]["payload"]["requested"]["payload_digest"] = json!("bad")
            }
            _ => unreachable!(),
        }
        assert!(
            u5_physical_cancel_evidence(&request, &owner, &events, &resources).is_none(),
            "{fault}"
        );
    }
}

#[test]
fn r202_expected_principal_comes_from_two_original_writer_leases_not_cancel_reply() {
    let sessions = ["session-a".into(), "session-b".into()];
    let original = json!({"kind":"runtime_session_leases","status":"available","attached":true,
        "leases":[{"session_id":"session-a","mode":"collaborative","owner":"principal:fixture:observer:harness-u5-a"},
            {"session_id":"session-b","mode":"collaborative","owner":"principal:fixture:observer:harness-u5-b"}]});
    assert_eq!(
        u5_cancel_principal(&original, &sessions).as_deref(),
        Some("principal:fixture")
    );
    for fault in [
        "foreign",
        "wrong-surface",
        "missing",
        "duplicate",
        "unavailable",
    ] {
        let mut value = original.clone();
        match fault {
            "foreign" => {
                value["leases"][1]["owner"] = json!("principal:other:observer:harness-u5-b")
            }
            "wrong-surface" => {
                value["leases"][0]["owner"] = json!("principal:fixture:observer:harness-u5-b")
            }
            "missing" => {
                value["leases"].as_array_mut().unwrap().pop();
            }
            "duplicate" => {
                let a = value["leases"][0].clone();
                value["leases"].as_array_mut().unwrap().push(a);
            }
            "unavailable" => value["status"] = json!("unavailable"),
            _ => unreachable!(),
        }
        assert!(u5_cancel_principal(&value, &sessions).is_none(), "{fault}");
    }
}

#[test]
fn r202_session_reader_retains_actual_payload_and_rejects_foreign_or_truncated_pages() {
    let original = u5_fixture_physical_events();
    for fault in [
        "none",
        "foreign",
        "truncated",
        "wrong-position",
        "count-mismatch",
        "missing-fields",
    ] {
        let mut page = json!({"session_id":"session-a","from_seq":0,"limit":100,
            "total":3,"has_more":false,"include_payload":true,"events":original});
        match fault {
            "none" => {}
            "foreign" => page["session_id"] = json!("foreign"),
            "truncated" => page["include_payload"] = json!(false),
            "wrong-position" => page["events"][1]["sequence"] = json!(1),
            "count-mismatch" => page["total"] = json!(1),
            "missing-fields" => {
                page.as_object_mut().unwrap().remove("total");
            }
            _ => unreachable!(),
        }
        let mut fixture = U5SingleCaseHttpFixture::start_wire(vec![(
            "GET /api/sessions/session-a/events?from_seq=0&limit=100&include_payload=true".into(),
            page,
            false,
            Duration::ZERO,
        )]);
        let runner = u5_fixture_runner(&fixture.url);
        let result =
            runner.read_u5_session_events("session-a", Instant::now() + Duration::from_secs(2));
        if fault == "none" {
            assert_eq!(result.unwrap(), original);
        } else {
            assert!(result.is_err(), "{fault}");
        }
        assert_eq!(fixture.finish().len(), 1);
    }
}

#[test]
fn u5_fairness_consumer_rejects_each_independent_counterexample() {
    let owner = u5_fixture_owner();
    let sessions = ["session-a".into(), "session-b".into()];
    let verdict = |a: &Value, b: &Value, e: &U5ResourceEvidence| {
        u5_pair_acceptance(U5Mode::Fairness, &owner, &sessions, a, b, e, None)["internal_passed"]
            == true
    };
    assert!(verdict(
        &u5_fixture_report("a"),
        &u5_fixture_report("b"),
        &u5_fixture_resources()
    ));
    // A/B are roles, not a model scheduling policy. Unrelated writer losses do
    // not invalidate selected witnesses: no global writer-health certificate.
    let mut swapped = u5_fixture_resources();
    for observation in &mut swapped.observations {
        observation.fairness_key = if observation.fairness_key == "session:session-a" {
            "session:session-b".into()
        } else {
            "session:session-a".into()
        };
    }
    assert!(verdict(
        &u5_fixture_report("a"),
        &u5_fixture_report("b"),
        &swapped
    ));
    let mut coalesced = u5_fixture_resources();
    coalesced.observations.remove(1);
    coalesced.source_refs.remove(1);
    assert!(verdict(
        &u5_fixture_report("a"),
        &u5_fixture_report("b"),
        &coalesced
    ));
    let mut token_pool = u5_fixture_resources();
    for o in &mut token_pool.observations {
        o.demands = vec![(
            runtime::execution_core::graph::ExecutionResourceKind::ProviderTokenPool(
                "shared".into(),
            ),
            1,
        )];
    }
    assert!(verdict(
        &u5_fixture_report("a"),
        &u5_fixture_report("b"),
        &token_pool
    ));
    for fault in [
        "foreign-owner",
        "URL-only",
        "serial",
        "incomplete",
        "wrong-session",
        "no-capacity-reason",
        "old-root",
        "bad-read",
        "bad-ACK",
        "missing-grant",
        "third-blocker",
        "conflicting-grant",
        "missing-enqueue-sequence",
        "missing-enqueue-time",
        "context-bytes",
        "mixed-tool",
        "disjoint-capacity",
    ] {
        let mut a = u5_fixture_report("a");
        let mut b = u5_fixture_report("b");
        let mut e = u5_fixture_resources();
        match fault {
            "conflicting-grant" => {
                let mut conflict = e.observations[0].clone();
                conflict.demands = vec![(
                    runtime::execution_core::graph::ExecutionResourceKind::ProviderModel(
                        "conflict".into(),
                    ),
                    1,
                )];
                e.observations.push(conflict);
                e.source_refs.push("fixture:conflicting-grant".into());
            }
            "missing-enqueue-sequence" => {
                for o in &mut e.observations {
                    o.enqueue_sequence = None;
                }
            }
            "missing-enqueue-time" => {
                for o in &mut e.observations {
                    o.enqueued_at_ms = None;
                }
            }
            "third-blocker" => {
                for o in &mut e.observations {
                    o.blocker = Some(uuid::Uuid::from_u128(99));
                }
            }
            "mixed-tool" => {
                // Shared Provider alone cannot identify the exhausted kind:
                // both atomic bundles also contend on Tool.
                for o in &mut e.observations {
                    o.demands.push((
                        runtime::execution_core::graph::ExecutionResourceKind::Tool,
                        1,
                    ));
                }
            }
            "context-bytes" => {
                for o in &mut e.observations {
                    o.demands = vec![(
                        runtime::execution_core::graph::ExecutionResourceKind::ContextBytes,
                        1,
                    )];
                }
            }
            "disjoint-capacity" => {
                for o in &mut e.observations[1..] {
                    o.demands = vec![(
                        runtime::execution_core::graph::ExecutionResourceKind::ProviderModel(
                            "other".into(),
                        ),
                        1,
                    )];
                }
            }
            "foreign-owner" => e.owner.pid += 1,
            "URL-only" => e.owner.starttime += 1,
            "serial" => {
                for o in &mut e.observations {
                    o.blocker = None;
                }
            }
            "incomplete" => e.selected_witnesses_complete = false,
            "wrong-session" => e.observations[2].fairness_key = "session:foreign".into(),
            "no-capacity-reason" => {
                for o in &mut e.observations {
                    o.wait_reason = None;
                }
            }
            "old-root" => b["execution_id"] = a["execution_id"].clone(),
            // An original single-case source/ACK failure cannot be promoted by pairing.
            "bad-read" => a["acceptance"]["passed"] = json!(false),
            "bad-ACK" => b["status"] = json!("failed"),
            "missing-grant" => {
                e.observations.pop();
                e.source_refs.pop();
            }
            _ => unreachable!(),
        }
        assert!(!verdict(&a, &b, &e), "{fault}");
    }
    assert!(!u5_valid_owner(
        &U5Owner {
            pid: 0,
            ..owner.clone()
        },
        &owner.base_url
    ));
}

#[test]
fn u5_cancel_consumer_requires_settlement_and_preserves_other_session() {
    let owner = u5_fixture_owner();
    let sessions = ["session-a".into(), "session-b".into()];
    let verdict = |a: &Value, b: &Value, request: Option<&U5CancelContext>| {
        u5_pair_acceptance(
            U5Mode::Cancel,
            &owner,
            &sessions,
            a,
            b,
            &u5_fixture_resources(),
            request,
        )["internal_passed"]
            == true
    };
    assert!(verdict(
        &u5_fixture_cancelled(),
        &u5_fixture_report("b"),
        Some(&u5_fixture_cancel_context())
    ));
    assert!(!verdict(
        &u5_fixture_cancelled(),
        &u5_fixture_report("b"),
        None
    ));
    for fault in [
        "requested",
        "already-terminal",
        "not-started",
        "wrong-start-root",
        "wrong-cancellation-id",
        "unsettled",
        "no-coverage",
        "source-race",
        "old-root",
        "wrong-actor",
        "wrong-turn",
        "goal-satisfied",
        "B-cancelled",
        "finish-only",
    ] {
        let mut a = u5_fixture_cancelled();
        let mut b = u5_fixture_report("b");
        let mut request = u5_fixture_cancel_context();
        match fault {
            "requested" => a["receipt"]["status"] = json!("requested"),
            "already-terminal" => a["receipt"]["status"] = json!("already_terminal"),
            "not-started" => request.started_source.clear(),
            "wrong-start-root" => request.identity.root = "foreign-root".into(),
            "wrong-cancellation-id" => a["receipt"]["cancellation_id"] = json!("other-request"),
            "unsettled" => {
                a["projection"]["terminal_axes"]["execution"]["settlement"] = json!("unsettled")
            }
            "no-coverage" => {
                a["projection"]["terminal_axes"]["execution"]["coverage_complete"] = json!(false)
            }
            "source-race" => a["projection"]["terminal_axes"]["source_race"] = json!(true),
            "old-root" => a["projection"]["execution_id"] = json!("old"),
            "wrong-actor" => a["receipt"]["actor_id"] = json!("principal:foreign"),
            "wrong-turn" => a["receipt"]["turn_id"] = json!("foreign"),
            "goal-satisfied" => {
                a["projection"]["terminal_axes"]["outcome"]["outcome"] = json!("satisfied")
            }
            "B-cancelled" => b["status"] = json!("cancelled"),
            "finish-only" => a
                .as_object_mut()
                .unwrap()
                .remove("projection")
                .map(|_| ())
                .unwrap(),
            _ => unreachable!(),
        }
        assert!(!verdict(&a, &b, Some(&request)), "{fault}");
    }
}

#[test]
fn u5_outer_recovery_is_separate_and_never_defaults_to_success() {
    let owner = u5_fixture_owner();
    let internal = json!({"internal_passed":true});
    assert!(u5_final_acceptance(
        &internal,
        &owner,
        Some((&owner, true, true, 0))
    ));
    assert!(!u5_final_acceptance(&internal, &owner, None));
    for (v1, clean, remaining) in [(false, true, 0), (true, false, 0), (true, true, 1)] {
        assert!(!u5_final_acceptance(
            &internal,
            &owner,
            Some((&owner, v1, clean, remaining))
        ));
    }
    assert!(!u5_final_acceptance(
        &internal,
        &owner,
        Some((
            &U5Owner {
                pid: 99,
                ..owner.clone()
            },
            true,
            true,
            0
        ))
    ));
}

fn u5_fixture_runner(url: &str) -> LiveScenarioRunner {
    LiveScenarioRunner {
        client: Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
        base_url: url.into(),
        timeout_cap: None,
        poll_interval: Duration::from_millis(1),
        model: None,
        claim_scope: LiveClaimScope::Focused,
        u5_deadline: None,
        u5_host: None,
    }
}

#[test]
fn u5_live_missing_evidence_is_refused_before_any_actor_or_HTTP() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let runner = u5_fixture_runner(&format!("http://{}", listener.local_addr().unwrap()));
    for (id, mode) in [(U5_FAIRNESS, U5Mode::Fairness), (U5_CANCEL, U5Mode::Cancel)] {
        let selected = BTreeSet::from([id.to_string()]);
        assert_eq!(
            u5_selection_preflight(Some(&selected)).unwrap()["model_work_started"],
            false
        );
        let report = runner.run_scenario(
            LiveScenarioSpec {
                id,
                prompt: "must not dispatch",
                acceptance: LiveAcceptance::U5(mode),
                timeout: LiveScenarioTimeout::tool(),
            },
            None,
        );
        assert_eq!(report["status"], "gated");
        assert!(report["reason"]
            .as_str()
            .unwrap()
            .contains("u5_evidence_missing"));
        assert!(
            !LiveAcceptance::U5(mode)
                .evaluate("anything", &json!({}), &[], None, "root")
                .passed
        );
    }
    assert!(u5_selection_preflight(Some(&BTreeSet::from(["live_tool_evidence".into()]))).is_none());
    assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
}

#[test]
fn u5_scoped_pair_runs_both_branches_and_honors_pre_dispatch_deadline() {
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let work = || {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        U5ActorOutcome {
            report: json!({"status":"fixture-only"}),
            cancellation: None,
        }
    };
    let (a, b) = u5_parallel(Instant::now() + Duration::from_secs(1), work, work);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(a.report, b.report);
    let _ = u5_parallel(Instant::now() - Duration::from_secs(1), work, work);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    let mut runner = u5_fixture_runner("http://127.0.0.1:1");
    runner.u5_deadline = Some(Instant::now() - Duration::from_millis(1));
    assert_eq!(
        runner.get_json("/must-not-connect").unwrap_err(),
        "U5 observation deadline reached"
    );
    assert_eq!(runner.observation_poll_delay(), Duration::ZERO);

    runner.poll_interval = Duration::from_secs(3600);
    runner.u5_deadline = None;
    assert_eq!(runner.observation_poll_delay(), Duration::from_secs(3600));
    let started = Instant::now();
    let deadline = started + Duration::from_millis(20);
    runner.u5_deadline = Some(deadline);
    let in_flight = || {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        thread::sleep(runner.observation_poll_delay());
        U5ActorOutcome {
            report: json!({"status":"fixture-only"}),
            cancellation: None,
        }
    };
    let (a, b) = u5_parallel(deadline, in_flight, in_flight);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 4);
    assert_eq!(a.report, b.report);
    assert_eq!(runner.observation_poll_delay(), Duration::ZERO);
}

// This maps real store records to the existing Gateway wire shape. It is a
// consumer fixture, not a Gateway invocation or authenticated public ACL proof.
fn u5_store_event_wire(event: runtime::DurableRuntimeEvent) -> Value {
    json!({"source":"runtime_lifecycle", "scope":event.scope.as_str(),
        "stream_id":event.stream_id, "sequence":event.sequence,
        "commit_cursor":event.commit_cursor, "transaction_index":event.transaction_index,
        "kind":event.kind, "status":event.status, "payload":event.payload,
        "created_at_ms":event.created_at_ms,
        "refs":event.refs.into_iter().map(|r| json!({"type":r.kind,"id":r.id})).collect::<Vec<_>>()})
}

fn u5_resource_page(
    session: &str,
    cursor: Option<&str>,
    next: Option<&str>,
    more: bool,
    events: Vec<Value>,
) -> Value {
    json!({"session_id":session,"cursor":cursor,"next_cursor":next,
        "has_more":more,"degraded":false,"events":events})
}

fn u5_resource_path(session: &str, cursor: Option<&str>, limit: usize) -> String {
    let mut url = reqwest::Url::parse("http://127.0.0.1/api/runtime/timeline").unwrap();
    url.query_pairs_mut()
        .append_pair("session_id", session)
        .append_pair("limit", &limit.to_string());
    if let Some(cursor) = cursor {
        url.query_pairs_mut().append_pair("cursor", cursor);
    }
    format!("GET {}?{}", url.path(), url.query().unwrap())
}

fn u5_fixture_resource_events() -> [Vec<Value>; 2] {
    let evidence = u5_fixture_resources();
    let events: Vec<_> = evidence
        .observations
        .into_iter()
        .enumerate()
        .map(|(i, o)| {
            let session = o.fairness_key.strip_prefix("session:").unwrap().to_string();
            let state = serde_json::to_value(o.status).unwrap();
            json!({"source":"runtime_lifecycle","scope":"schedule",
            "stream_id":format!("resource-admission:{}",o.request_id),"sequence":i+1,
            "commit_cursor":i+1,"transaction_index":0,
            "kind":format!("resource.admission.{}",state.as_str().unwrap()),"status":state,
            "refs":[{"type":"session","id":session},{"type":"resource_request","id":o.request_id}],
            "payload":o})
        })
        .collect();
    [vec![events[0].clone()], events[1..].to_vec()]
}

// ApiClient's original synchronous collector owns the current-thread Tokio
// context. No direct Tokio dependency, alternate observer, or model call.
struct U5LocalDriver<F>(Option<F>);
impl<F: std::future::Future<Output = ()> + Send> runtime::ApiClient for U5LocalDriver<F> {
    fn stream(
        &mut self,
        _: runtime::ApiRequest,
    ) -> std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<runtime::AssistantEvent, runtime::RuntimeError>>
                + Send
                + '_,
        >,
    > {
        use futures::StreamExt;
        Box::pin(
            futures::stream::once(self.0.take().unwrap())
                .filter_map(|()| futures::future::ready(None)),
        )
    }
}

fn u5_local_job(job: impl std::future::Future<Output = ()> + Send) {
    use runtime::ApiClient;
    let request = runtime::ApiRequest {
        bound_step: None,
        prompt: runtime::PromptAssembly::new(vec![]),
        messages: vec![].into(),
        model: "fixture-no-model".into(),
        reasoning_effort_override: None,
        request_compiler_cache_hit: false,
        budget: runtime::context_ledger::RequestBudgetReport::for_attempt(
            "fixture-no-model",
            1000,
            100,
            1,
            1,
            0,
        ),
        preparation: Default::default(),
        provider_evidence_context: None,
    };
    assert!(U5LocalDriver(Some(job))
        .stream_collect(request)
        .unwrap()
        .is_empty());
}

#[test]
fn u5_resource_reader_real_manager_writer_store_selected_witnesses_and_symmetric_roles() {
    use runtime::execution_core::graph::*;
    use std::future::Future;
    for reverse in [false, true] {
        let sessions = [
            format!("u5-{}", uuid::Uuid::new_v4()),
            format!("u5-{}", uuid::Uuid::new_v4()),
        ];
        let output = std::sync::Arc::new(std::sync::Mutex::new(None));
        let result = output.clone();
        let pair = sessions.clone();
        // Compose outside Tokio: retain the original writer installation without
        // starting unrelated reactor tasks on the local collector runtime.
        let services = runtime::RuntimeServices::in_memory().unwrap();
        u5_local_job(async move {
            let manager = services.resource_manager();
            manager
                .reconcile_quotas(
                    [(
                        ExecutionResourceKind::Provider,
                        ResourceQuota::new(1, 1, 1).unwrap(),
                    )],
                    [],
                )
                .unwrap();
            let deadline = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 1000;
            let request = |side: usize| {
                ResourceAdmissionRequest::new(
                    ExecutionServiceClass::Foreground,
                    [(ExecutionResourceKind::Provider, 1)],
                )
                .with_fairness_key(format!("session:{}", pair[side]))
                .with_deadline_at_ms(deadline)
            };
            let first = usize::from(reverse);
            let a = manager.admit(request(first)).await.unwrap();
            let ResourceAdmissionDecision::Granted { lease: a, .. } = a else {
                panic!("first admission must grant")
            };
            let blocking_id = a.id();
            let mut waiting = Box::pin(manager.admit(request(1 - first)));
            let pending = futures::future::poll_fn(|cx| {
                std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending())
            })
            .await;
            drop(a);
            let second = waiting.await.unwrap();
            let granted = matches!(&second, ResourceAdmissionDecision::Granted { .. });
            drop(second);
            services.shutdown_maintenance().await;
            assert!(pending && granted, "real Capacity wait then grant required");
            let mut pages = Vec::new();
            for session in &pair {
                let records = services
                    .event_reader()
                    .session_timeline_events(session, None, 100)
                    .unwrap().events;
                assert!(!records.is_empty());
                assert!(records.iter().all(|r| r
                    .refs
                    .iter()
                    .any(|r| r.kind == "session" && r.id == *session)));
                let events: Vec<_> = records.into_iter().map(u5_store_event_wire).collect();
                if session == &pair[1 - first] {
                    assert!(events.iter().any(|e| e["status"] == "granted"
                        && e["payload"]["wait_reason"] == "capacity"
                        && e["payload"]["blocker"] == blocking_id.to_string()));
                }
                let last = events.last().unwrap();
                let next = format!(
                    "v2:0:{}:{}",
                    last["commit_cursor"].as_u64().unwrap(),
                    last["transaction_index"].as_u64().unwrap()
                );
                pages.push(u5_resource_page(session, None, Some(&next), false, events));
            }
            *result.lock().unwrap() = Some(pages);
        });
        let pages = output.lock().unwrap().take().unwrap();
        let originals: Vec<_> = pages
            .iter()
            .flat_map(|p| p["events"].as_array().unwrap().iter().cloned())
            .collect();
        let script = sessions
            .iter()
            .zip(pages)
            .map(|(s, p)| (u5_resource_path(s, None, 100), p, false, Duration::ZERO))
            .collect();
        let mut fixture = U5SingleCaseHttpFixture::start_wire(script);
        let runner = u5_fixture_runner(&fixture.url);
        let mut owner = u5_fixture_owner();
        owner.base_url = fixture.url.clone();
        let evidence = runner
            .read_u5_resource_witnesses(&owner, &sessions, Instant::now() + Duration::from_secs(2))
            .unwrap();
        assert_eq!(fixture.finish().len(), 2);
        assert!(evidence.selected_witnesses_complete);
        for (observation, source) in evidence.observations.iter().zip(&evidence.source_refs) {
            assert!(
                originals.iter().any(|e| u5_resource_observation(
                    e,
                    observation.fairness_key.strip_prefix("session:").unwrap()
                )
                .ok()
                .flatten()
                .is_some_and(|(o, r)| o == *observation && r == *source)),
                "source must resolve to an original same-store record"
            );
        }
        let report = |i: usize| {
            json!({"status":"passed","session_id":sessions[i],"turn_id":format!("turn-{i}"),
            "execution_id":format!("root-{i}"),"acceptance":{"passed":true},"actor_cleanup":{"passed":true}})
        };
        assert_eq!(
            u5_pair_acceptance(
                U5Mode::Fairness,
                &owner,
                &sessions,
                &report(0),
                &report(1),
                &evidence,
                None
            )["internal_passed"],
            true
        );
    }
}

#[test]
fn u5_resource_reader_loopback_binding_cursor_and_page_counterexamples() {
    let sessions: [String; 2] = ["session-a".into(), "session-b".into()];
    for fault in [
        "none",
        "unrelated-writer-loss",
        "kind",
        "refs",
        "source",
        "scope",
        "stream",
        "status",
        "fairness",
        "zero-sequence",
        "zero-commit",
        "index-overflow",
        "refs-type",
        "third-session",
        "page-session",
        "page-cursor",
        "has-more-type",
        "degraded",
        "repeat-cursor",
        "replay-conflict",
        "same-position-kind",
        "replay-position",
        "next-cursor-skip",
        "missing-grant",
    ] {
        let [a, b] = u5_fixture_resource_events();
        let mut first = u5_resource_page(&sessions[0], None, Some("v2:0:1:0"), false, a);
        let mut second = u5_resource_page(&sessions[1], None, Some("v2:0:3:0"), false, b);
        match fault {
            "unrelated-writer-loss" => {
                // Metadata is not a full-history certificate. The selected
                // final grant retains Capacity/blocker even without Waiting.
                first["resource_writer_health"] =
                    json!({"coalesced":7,"coalescible_dropped":11,"priority_dropped":2});
                second["events"].as_array_mut().unwrap().remove(0);
            }
            "scope" => first["events"][0]["scope"] = json!("session"),
            "stream" => first["events"][0]["stream_id"] = json!("resource-admission:not-a-uuid"),
            "status" => first["events"][0]["status"] = json!("waiting"),
            "fairness" => {
                first["events"][0]["payload"]["fairness_key"] = json!("session:session-c")
            }
            "zero-sequence" => first["events"][0]["sequence"] = json!(0),
            "zero-commit" => first["events"][0]["commit_cursor"] = json!(0),
            "index-overflow" => {
                first["events"][0]["transaction_index"] = json!(u64::from(u32::MAX) + 1)
            }
            "refs-type" => first["events"][0]["refs"][1]["type"] = json!("other"),
            "kind" => first["events"][0]["kind"] = json!("resource.admission.waiting"),
            "refs" => first["events"][0]["refs"][1]["id"] = json!(uuid::Uuid::from_u128(99)),
            "source" => first["events"][0]["source"] = json!("foreign"),
            "third-session" => first["events"][0]["refs"][0]["id"] = json!("session-c"),
            "page-session" => first["session_id"] = json!("session-c"),
            "page-cursor" => first["cursor"] = json!("v2:0:0:0"),
            "has-more-type" => first["has_more"] = json!("false"),
            "degraded" => first["degraded"] = json!(true),
            "repeat-cursor" | "replay-conflict" | "same-position-kind" | "replay-position" => {
                first["has_more"] = json!(true)
            }
            "next-cursor-skip" => first["next_cursor"] = json!("v2:0:99:0"),
            "missing-grant" => {
                second["events"].as_array_mut().unwrap().pop();
                second["next_cursor"] = json!("v2:0:2:0");
            }
            _ => {}
        }
        let mut script = vec![(
            u5_resource_path(&sessions[0], None, 100),
            first.clone(),
            false,
            Duration::ZERO,
        )];
        if matches!(
            fault,
            "repeat-cursor" | "replay-conflict" | "same-position-kind" | "replay-position"
        ) {
            let mut replay = first.clone();
            replay["cursor"] = json!("v2:0:1:0");
            replay["has_more"] = json!(false);
            if fault != "repeat-cursor" {
                // An actual new source at commit 2 makes next_cursor legal;
                // the old position at commit 1 must still reject changed data.
                replay["next_cursor"] = json!("v2:0:2:0");
                if fault == "same-position-kind" {
                    replay["events"][0]["kind"] = json!("resource.admission.waiting");
                    replay["events"][0]["status"] = json!("waiting");
                    replay["events"][0]["payload"]["status"] = json!("waiting");
                } else if fault == "replay-position" {
                    // A different stream/sequence cannot reuse the same
                    // (commit,index), even when its own event is coherent.
                    replay["events"][0]["stream_id"] = json!("fixture-rebound");
                    replay["events"][0]["kind"] = json!("fixture.progress");
                    replay["events"][0]["status"] = json!("observed");
                    replay["events"][0]["payload"] = json!({});
                    replay["events"][0]["refs"] = json!([]);
                } else {
                    replay["events"][0]["payload"]["queue_age_ms"] = json!(999);
                }
                replay["events"].as_array_mut().unwrap().push(json!({
                    "source":"runtime_lifecycle","scope":"schedule","stream_id":"fixture-progress",
                    "sequence":1,"commit_cursor":2,"transaction_index":0,
                    "kind":"fixture.progress","status":"observed","refs":[],"payload":{}}));
            }
            script.push((
                u5_resource_path(&sessions[0], Some("v2:0:1:0"), 100),
                replay,
                false,
                Duration::ZERO,
            ));
        }
        script.push((
            u5_resource_path(&sessions[1], None, 100),
            second,
            false,
            Duration::ZERO,
        ));
        let mut fixture = U5SingleCaseHttpFixture::start_wire(script);
        let mut owner = u5_fixture_owner();
        owner.base_url = fixture.url.clone();
        let result = u5_fixture_runner(&fixture.url).read_u5_resource_witnesses(
            &owner,
            &sessions,
            Instant::now() + Duration::from_secs(2),
        );
        let requests = fixture.finish();
        match fault {
            "none" | "unrelated-writer-loss" => {
                let evidence = result.unwrap();
                assert!(u5_capacity_witness(&evidence, &sessions).is_some());
                assert_eq!(requests.len(), 2);
            }
            "missing-grant" => assert!(u5_capacity_witness(&result.unwrap(), &sessions).is_none()),
            "replay-conflict" | "same-position-kind" | "replay-position" => {
                let error = result.err().expect("immutable source conflict must reject");
                assert_eq!(requests.len(), 2, "must reach the replay page: {fault}");
                assert!(
                    error.contains("immutable source")
                        || error
                            .contains("stable timeline event identity carried a different payload"),
                    "must reject the source conflict, not a fabricated cursor: {fault}: {error}"
                );
            }
            "next-cursor-skip" => {
                let error = result.err().expect("unsupported next cursor must reject");
                assert!(
                    error.contains("not supported by page source positions"),
                    "{error}"
                );
            }
            _ => assert!(result.is_err(), "{fault}"),
        }
    }
}

#[test]
fn u5_resource_reader_shared_budgets_streaming_and_deadlines() {
    let sessions: [String; 2] = ["session-a".into(), "session-b".into()];
    for fault in [
        "events",
        "exact512-before-IO-more",
        "exact512-before-IO-second-session",
        "pages",
        "known-bytes",
        "unknown-bytes",
        "cumulative-bytes",
        "expired",
        "shared-expired",
        "body-timeout",
    ] {
        let [a, _] = u5_fixture_resource_events();
        let mut script = Vec::new();
        let mut cursor: Option<String> = None;
        let exact = fault.starts_with("exact512-before-IO");
        let count = match fault {
            "events" | "exact512-before-IO-more" | "exact512-before-IO-second-session" => 6,
            "pages" => 32,
            "cumulative-bytes" => 3,
            _ => 1,
        };
        let mut position = 0usize;
        for i in 0..count {
            let event_count = if exact && i == 5 {
                12
            } else if exact || fault == "events" {
                100
            } else {
                1
            };
            let events = (0..event_count)
                .map(|_| {
                    position += 1;
                    let mut event = a[0].clone();
                    event["sequence"] = json!(position);
                    event["commit_cursor"] = json!(position);
                    event
                })
                .collect();
            let next = format!("v2:0:{position}:0");
            let more = !(fault == "body-timeout"
                || (fault == "exact512-before-IO-second-session" && i == 5));
            let mut page =
                u5_resource_page(&sessions[0], cursor.as_deref(), Some(&next), more, events);
            // First five pages consume 500; events deliberately returns 100
            // for the sixth request's remaining limit of 12.
            if matches!(fault, "known-bytes" | "unknown-bytes") {
                page["padding"] = json!("x".repeat(2 * 1024 * 1024));
            }
            if fault == "cumulative-bytes" {
                page["padding"] = json!("x".repeat(750_000));
            }
            let limit = if (exact || fault == "events") && i == 5 {
                12
            } else {
                100
            };
            script.push((
                u5_resource_path(&sessions[0], cursor.as_deref(), limit),
                page,
                fault == "unknown-bytes",
                if fault == "body-timeout" {
                    Duration::from_millis(300)
                } else {
                    Duration::ZERO
                },
            ));
            cursor = Some(next);
        }
        if exact {
            // A sentinel catches any seventh HTTP request, including an attempt
            // to start the second Session after exactly exhausting the budget.
            let (session, request_cursor) = if fault == "exact512-before-IO-more" {
                (&sessions[0], cursor.as_deref())
            } else {
                (&sessions[1], None)
            };
            script.push((
                u5_resource_path(session, request_cursor, 1),
                u5_resource_page(session, request_cursor, request_cursor, false, vec![]),
                false,
                Duration::ZERO,
            ));
        }
        let mut fixture = U5SingleCaseHttpFixture::start_wire(script);
        let mut owner = u5_fixture_owner();
        owner.base_url = fixture.url.clone();
        let mut runner = u5_fixture_runner(&fixture.url);
        if fault == "shared-expired" {
            runner.u5_deadline = Some(Instant::now() - Duration::from_millis(1));
        }
        let deadline = if fault == "expired" {
            Instant::now() - Duration::from_millis(1)
        } else if fault == "body-timeout" {
            Instant::now() + Duration::from_millis(100)
        } else {
            Instant::now() + Duration::from_secs(3)
        };
        let started = Instant::now();
        let error = runner
            .read_u5_resource_witnesses(&owner, &sessions, deadline)
            .err()
            .expect(fault);
        let requests = fixture.finish();
        match fault {
            "expired" | "shared-expired" => {
                assert!(requests.is_empty());
                assert!(error.contains("before IO"), "{error}");
            }
            "events" => {
                assert_eq!(requests.len(), 6);
                assert!(error.contains("event budget"), "{error}");
            }
            "exact512-before-IO-more" | "exact512-before-IO-second-session" => {
                assert_eq!(
                    requests.len(),
                    6,
                    "no seventh HTTP after 512 events: {fault}"
                );
                assert!(error.contains("before IO"), "{error}");
            }
            "pages" => {
                assert_eq!(requests.len(), 32);
                assert!(error.contains("before IO"), "{error}");
            }
            "known-bytes" | "cumulative-bytes" => assert!(error.contains("byte budget"), "{error}"),
            "unknown-bytes" => assert!(error.contains("byte budget"), "{error}"),
            "body-timeout" => {
                assert_eq!(requests.len(), 1);
                assert!(started.elapsed() < Duration::from_secs(1));
            }
            _ => unreachable!(),
        }
    }
}

// NoRealU5: this bounded loopback double exercises the original single-case
// consumers; it supplies no real owner/resource/Runtime/provider evidence.
struct U5SingleCaseHttpFixture {
    url: String,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    server: Option<thread::JoinHandle<Vec<(String, String, Value)>>>,
}

impl U5SingleCaseHttpFixture {
    fn start(script: Vec<(&'static str, Value)>) -> Self {
        Self::start_wire(
            script
                .into_iter()
                .map(|(path, body)| (path.to_string(), body, false, Duration::ZERO))
                .collect(),
        )
    }

    // close-delimited bodies exercise unknown Content-Length; delay starts
    // after headers so timeout tests cover an in-flight body read.
    fn start_wire(script: Vec<(String, Value, bool, Duration)>) -> Self {
        Self::start_status(
            script
                .into_iter()
                .map(|(p, v, u, d)| (p, v, u, d, 200))
                .collect(),
        )
    }

    fn start_status(script: Vec<(String, Value, bool, Duration, u16)>) -> Self {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopping = stop.clone();
        let server = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut requests = Vec::new();
            for (expected, response, unknown_length, delay, status) in script {
                let mut stream = loop {
                    if stopping.load(std::sync::atomic::Ordering::SeqCst)
                        || Instant::now() >= deadline
                    {
                        return requests;
                    }
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("NoRealU5 accept: {error}"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
                let mut bytes = Vec::new();
                let (headers, body) = loop {
                    assert!(bytes.len() < 16_384 && Instant::now() < deadline);
                    let mut chunk = [0; 1024];
                    let count = stream.read(&mut chunk).unwrap();
                    assert!(count > 0, "NoRealU5 incomplete HTTP request");
                    bytes.extend_from_slice(&chunk[..count]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        assert!(length < 16_384);
                        if bytes.len() >= end + 4 + length {
                            let body = if length == 0 {
                                Value::Null
                            } else {
                                serde_json::from_slice(&bytes[end + 4..end + 4 + length]).unwrap()
                            };
                            break (headers, body);
                        }
                    }
                };
                let request = headers
                    .lines()
                    .next()
                    .unwrap()
                    .strip_suffix(" HTTP/1.1")
                    .unwrap()
                    .to_string();
                assert_eq!(request, expected, "NoRealU5 request order");
                requests.push((request, headers, body));
                if status == 0 {
                    return requests; // Original transport EOF, without an HTTP response.
                }
                let body = serde_json::to_vec(&response).unwrap();
                let header = if unknown_length {
                    format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n")
                } else {
                    format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())
                };
                // A bounded consumer may close early on size/deadline errors.
                // Request parsing/order and all consumer assertions still fail normally.
                if stream.write_all(header.as_bytes()).is_err() {
                    return requests;
                }
                let wake = Instant::now() + delay;
                while Instant::now() < wake {
                    if stopping.load(std::sync::atomic::Ordering::SeqCst) {
                        return requests;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
                if stream.write_all(&body).is_err() {
                    return requests;
                }
            }
            requests
        });
        Self {
            url,
            stop,
            server: Some(server),
        }
    }

    fn finish(&mut self) -> Vec<(String, String, Value)> {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        self.server.take().unwrap().join().unwrap()
    }
}

impl Drop for U5SingleCaseHttpFixture {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

// Exercise the original observer and SessionActor over the existing loopback
// assembly. The wire records are independent fixtures, not a live ACL claim.
fn u5_event_first_observation_case(case: &str) -> Result<(Value, U5CancelContext), String> {
    const EVENTS: &str =
        "GET /api/sessions/session-a/events?from_seq=0&limit=100&include_payload=true";
    const GRAPH: &str = "GET /api/runtime/executions/root-a?detail_scope=summary";
    let page = |events: Vec<Value>| {
        json!({"session_id":"session-a","from_seq":0,
        "limit":100,"include_payload":true,"total":events.len(),"has_more":false,"events":events})
    };
    let wire = |p: &str, v, status| (p.to_string(), v, false, Duration::ZERO, status);
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let mut script = vec![
        wire("POST /api/sessions", json!({"id":"session-a"}), 200),
        wire(
            "POST /api/sessions/session-a/attach",
            json!({"ok":true,"session_id":"session-a",
            "receipt_attachment":{"client_id":"a1111111-1111-4111-8111-111111111111",
            "client_expires_at_ms":expires,"receipt_protocol":"session-public-v1"}}),
            200,
        ),
        wire(
            "POST /api/runtime/session-leases/acquire",
            json!({"ok":true}),
            200,
        ),
    ];
    let mut open = u5_fixture_physical_events()[0].clone();
    if ["success", "foreign", "stale", "already-outcome"].contains(&case) {
        let mut initial = Vec::new();
        if case != "success" {
            let mut old = open.clone();
            match case {
                "foreign" => old["payload"]["identity"]["turn_id"] = json!("foreign-turn"),
                "stale" => old["payload"]["identity"]["step"]["turn_id"] = json!("stale-turn"),
                "already-outcome" => (),
                _ => unreachable!(),
            }
            initial.push(old);
            initial.push(json!({"session_id":"session-a","sequence":2,"event_id":"outcome-a",
                "type":"context.provider_attempt_outcome","payload":{"request_id":"provider-call-a"}}));
        }
        script.push(wire(EVENTS, page(initial.clone()), 200));
        if !initial.is_empty() {
            open["event_id"] = json!("fresh-open");
            open["sequence"] = json!(3);
            open["payload"]["identity"]["request_id"] = json!("fresh-call");
            open["payload"]["identity"]["response_id"] = json!("fresh-response");
        }
        initial.push(open.clone());
        script.push(wire(EVENTS, page(initial), 200));
    } else if case != "deadline" {
        let mut response = page(vec![open.clone()]);
        if case == "wrong-page" {
            response["session_id"] = json!("foreign");
        }
        if case == "event-budget" {
            response["total"] = json!(513);
        }
        script.push(wire(
            EVENTS,
            response,
            if case == "permission" { 403 } else { 200 },
        ));
    }
    let before_graph_failure =
        ["deadline", "permission", "wrong-page", "event-budget"].contains(&case);
    let selected_failure = [
        "selected-404",
        "selected-invariant",
        "selected-transport",
        "terminal",
    ]
    .contains(&case);
    if !before_graph_failure {
        let (status, projection) = match case {
            "selected-404" => (404, json!({"error":"original selected graph missing"})),
            "selected-invariant" => (404, json!({"error":"history prefix: graph source changed"})),
            "selected-transport" => (0, Value::Null),
            "terminal" => (
                200,
                json!({"terminal_axes":{"execution":{"phase":"terminal"}}}),
            ),
            _ => (
                200,
                json!({"terminal_axes":{"execution":{"phase":"active"}}}),
            ),
        };
        script.push(wire(GRAPH, projection, status));
        if !selected_failure {
            let mut receipt = u5_fixture_cancelled()["receipt"].clone();
            if case != "success" && case != "post-404" {
                receipt["status"] = json!("already_terminal");
            }
            script.push(wire("POST /api/sessions/session-a/cancel", receipt, 200));
            if case == "success" {
                script.push(wire(
                    GRAPH,
                    u5_fixture_cancelled()["projection"].clone(),
                    200,
                ));
                script.push(wire(EVENTS, page(u5_fixture_physical_events()), 200));
                let [a, b] = u5_fixture_resource_events();
                script.push(wire(
                    &u5_resource_path("session-a", None, 100),
                    u5_resource_page("session-a", None, Some("v2:0:1:0"), false, a),
                    200,
                ));
                script.push(wire(
                    &u5_resource_path("session-b", None, 100),
                    u5_resource_page("session-b", None, Some("v2:0:3:0"), false, b),
                    200,
                ));
            } else if case == "post-404" {
                script.push(wire(
                    GRAPH,
                    json!({"error":"post-cancel original graph missing"}),
                    404,
                ));
            }
        }
    }
    // A deliberate transport close also closes the scripted server; do not
    // turn the consequent cleanup refusal into evidence of business success.
    if case != "selected-transport" {
        script.push(wire(
            "POST /api/runtime/session-leases/release",
            json!({"ok":true}),
            200,
        ));
        script.push(wire(
            "POST /api/sessions/session-a/detach",
            json!({"ok":true}),
            200,
        ));
    }
    let expected = script.len();
    let mut fixture = U5SingleCaseHttpFixture::start_status(script);
    let mut runner = u5_fixture_runner(&fixture.url);
    let now = Instant::now();
    runner.u5_deadline = Some(now + Duration::from_secs(2));
    let mut owner = u5_fixture_owner();
    owner.base_url = fixture.url.clone();
    struct UnusedSource;
    impl U5EvidenceReader for UnusedSource {
        fn owner(&self) -> Result<U5Owner, String> {
            panic!("observer must use its original supplied owner")
        }
    }
    let mut actor = SessionActor::create(
        &runner.client,
        &fixture.url,
        None,
        "event-first-fixture",
        now + Duration::from_secs(2),
    )
    .unwrap();
    let result = runner.observe_u5_cancel(
        &u5_fixture_id("a"),
        &owner,
        &UnusedSource,
        &["session-a".into(), "session-b".into()],
        &mut actor,
        if case == "deadline" {
            now - Duration::from_secs(91)
        } else {
            now
        },
        "fixture-cancel",
    );
    let cleanup = actor.finish();
    let requests = fixture.finish();
    assert_eq!(
        requests.len(),
        expected,
        "{case}: every expected boundary must be reached"
    );
    if case != "selected-transport" {
        cleanup.unwrap();
    }
    if let Some((_, _, body)) = requests
        .iter()
        .find(|(path, _, _)| path == "POST /api/sessions/session-a/cancel")
    {
        assert_eq!(
            body,
            &json!({"cancellation_id":"fixture-cancel","expected_execution_id":"root-a","expected_turn_id":"turn-a"})
        );
    }
    result
}

#[test]
fn u5_event_first_original_observer_waits_for_open_then_proves_physical_cancel() {
    let (result, request) = u5_event_first_observation_case("success").unwrap();
    assert_eq!(request.started_source, "opened-a");
    assert_eq!(result["status"], "cancelled");
    assert!(result["physical_cancellation"].is_object());
    assert_eq!(
        result["projection"]["terminal_axes"]["execution"]["settlement"],
        "proven_settled"
    );
}

#[test]
fn u5_event_first_original_observer_rejects_foreign_closed_and_failed_boundaries() {
    for (case, expected) in [
        ("foreign", "AlreadyTerminal"),
        ("stale", "AlreadyTerminal"),
        ("already-outcome", "AlreadyTerminal"),
        ("already-terminal", "AlreadyTerminal"),
        ("terminal", "EOF/terminal"),
        ("selected-404", "404"),
        ("selected-invariant", "history prefix"),
        ("selected-transport", "error sending request"),
        ("post-404", "404"),
        ("permission", "evidence response rejected"),
        ("wrong-page", "page identity mismatch"),
        ("event-budget", "exceeds event budget"),
        ("deadline", "coverage missing"),
    ] {
        let error = u5_event_first_observation_case(case).err().expect(case);
        assert!(error.contains(expected), "{case}: {error}");
    }
}

#[test]
fn u5_no_real_u5_single_case_http_consumers_confirm_delivery_and_reject_wrong_ack() {
    use harness_contract::live::{
        ReceiptCursor, SessionMessageAck, SessionMessageReceiptContext, SessionPublicPayloadKind,
        SessionReceiptOutboxObservation, SessionReceiptSessionObservation,
    };
    const CLIENT: &str = "a1111111-1111-4111-8111-111111111111";
    const CONTEXT: &str = "GET /api/sessions/session-1/messages/assistant:input-1/receipt-context";
    const ACK: &str = "POST /api/sessions/session-1/messages/assistant:input-1/ack";
    const ROOT: &str = "GET /api/runtime/executions/root?detail_scope=summary";
    const TIMELINE: &str =
        "GET /api/runtime/timeline?session_id=session-1&limit=500&cursor=v2:0:30:0";
    let payload = r#"[{"type":"text","text":"56"}]"#;
    let digest = format!(
        "session-public-v1:sha256:{:x}",
        Sha256::digest(payload.as_bytes())
    );
    let cursor = ReceiptCursor::parse("9007199254740993").unwrap();
    let ack = SessionMessageAck {
        session_id: "session-1".into(),
        message_id: "assistant:input-1".into(),
        client_id: CLIENT.into(),
        runtime_commit_cursor: cursor.clone(),
        content_digest: digest.clone(),
        received_at_ms: 110,
    };
    let context = SessionMessageReceiptContext::Available {
        payload_kind: SessionPublicPayloadKind::SessionPublicV1,
        payload_json: payload.into(),
        content_digest: digest.clone(),
        session_id: "session-1".into(),
        message_id: "assistant:input-1".into(),
        client_id: CLIENT.into(),
        message_sequence: "0".into(),
        runtime_commit_cursor: cursor.clone(),
        receipt: None,
        session_source: SessionReceiptSessionObservation {
            request_id: "request-1".into(),
            turn_id: "turn-1".into(),
            session_generation: "1".into(),
            input_sequence: "1".into(),
            presence_revision: "3".into(),
            observed_at_ms: 100,
        },
        outbox_observation: SessionReceiptOutboxObservation::RuntimeOutbox {
            terminal_id: "turn-terminal:request-1".into(),
            revision: "1".into(),
            status: "pending".into(),
            materialized_at_ms: None,
            runtime_commit_cursor: cursor,
            execution_id: Some("root".into()),
            turn_id: "turn-1".into(),
            request_id: "request-1".into(),
        },
    };
    let mut current = context.clone();
    if let SessionMessageReceiptContext::Available {
        receipt,
        session_source,
        ..
    } = &mut current
    {
        *receipt = Some(ack.clone());
        session_source.observed_at_ms = 120;
    }
    let root = json!({"execution_id":"root","session_id":"session-1","turn_id":"turn-1",
        "revision":1,"cursor":30,"child_executions":[],
        "graph":{"nodes":[{"kind":"synthesize","status":"completed"}]},
        "terminal_axes":{
            "outcome":{"goal_id":null,"goal_revision":null,"outcome":null,"terminal_fence":null},
            "execution":{"phase":"terminal","settlement":"proven_settled","coverage_complete":true,"evidence_refs":["fixture:closure"]},
            "delivery":{"runtime_terminal_ref":null,"session_client_evidence":"requires_fresh_context"},
            "source_cursor":30,"source_race":false}});
    // Reuse the established root-outcome contract, adding the observer's page identity.
    let mut timeline =
        successful_root_outcome_timeline(json!({"has_more":false,"next_cursor":"v2:0:30:0"}));
    timeline["events"][0]["source"] = json!("runtime");
    timeline["events"][0]["sequence"] = json!(1);
    timeline["events"][0]["commit_cursor"] = json!(30);
    timeline["events"][0]["payload"]["identity"]["session_id"] = json!("session-1");
    timeline["events"][0]["payload"]["identity"]["turn_id"] = json!("turn-1");
    let empty_timeline = json!({"events":[],"has_more":false});
    for wrong_ack in [false, true] {
        let mut returned_ack = ack.clone();
        if wrong_ack {
            returned_ack.client_id = "b1111111-1111-4111-8111-111111111111".into();
        }
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let mut script = vec![
            ("POST /api/sessions", json!({"id":"session-1"})),
            (
                "POST /api/sessions/session-1/attach",
                json!({"ok":true,"session_id":"session-1",
                "receipt_attachment":{"client_id":CLIENT,"client_expires_at_ms":expires,"receipt_protocol":"session-public-v1"}}),
            ),
            (
                "POST /api/runtime/session-leases/acquire",
                json!({"ok":true}),
            ),
            (
                "POST /api/sessions/session-1/messages",
                json!({"execution":{"graph_id":"root","turn_id":"turn-1"}}),
            ),
            (
                "GET /api/sessions/session-1/messages?from_seq=0&limit=200",
                json!({"messages":[{"id":"assistant:input-1","role":"assistant","sequence":0}],"next_seq":1,"has_more":false}),
            ),
            (
                "GET /api/sessions/session-1/execution/live",
                json!({"execution_id":"root","live":{"revision":1,"status":"completed"}}),
            ),
            (ROOT, root.clone()),
            (
                "GET /api/runtime/timeline?session_id=session-1&limit=500",
                timeline.clone(),
            ),
            (
                "GET /api/sessions/session-1/messages?from_seq=1&limit=200",
                json!({"messages":[],"next_seq":1,"has_more":false}),
            ),
            (TIMELINE, empty_timeline.clone()),
            (CONTEXT, serde_json::to_value(&context).unwrap()),
            (CONTEXT, serde_json::to_value(&context).unwrap()),
            (ACK, serde_json::to_value(&returned_ack).unwrap()),
        ];
        if !wrong_ack {
            script.push((CONTEXT, serde_json::to_value(&current).unwrap()));
        }
        script.extend([
            (TIMELINE, empty_timeline.clone()),
            (ROOT, root.clone()),
            (ROOT, root.clone()),
            (
                "POST /api/runtime/session-leases/release",
                json!({"ok":true}),
            ),
            ("POST /api/sessions/session-1/detach", json!({"ok":true})),
        ]);
        let expected_requests = script.len();
        let mut fixture = U5SingleCaseHttpFixture::start(script);
        let mut runner = u5_fixture_runner(&fixture.url);
        runner.client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let started = Instant::now();
        let timeout = LiveScenarioTimeout {
            initial_wait: Duration::from_secs(2),
            inactivity_wait: Duration::from_secs(2),
            nominal_wait: Duration::from_secs(2),
            absolute_wait: Some(Duration::from_secs(2)),
        };
        let mut actor = SessionActor::create(
            &runner.client,
            &fixture.url,
            None,
            "NoRealU5",
            started + Duration::from_secs(2),
        )
        .unwrap();
        let report = runner.run_attached_scenario(
            LiveScenarioSpec {
                id: "u5-no-real-single-case",
                prompt: "Return 56",
                acceptance: LiveAcceptance::Contains("56"),
                timeout,
            },
            None,
            started,
            timeout,
            None,
            Vec::new(),
            &mut actor,
        );
        let cleanup = actor.finish();
        let trace = actor.drain_trace();
        drop(actor);
        let requests = fixture.finish();
        cleanup.unwrap();
        assert_eq!(requests.len(), expected_requests);
        assert_eq!(
            report["status"],
            if wrong_ack { "failed" } else { "passed" },
            "{report:#}"
        );
        assert_eq!(report["acceptance"]["passed"], !wrong_ack);
        assert!(crate::report::execution_observation_ready(
            &report["terminal_observation"],
            "root",
            "session-1",
            "turn-1"
        ));
        assert!(crate::report::business_observation_ready(&report));
        assert_eq!(
            crate::report::delivery_observation_ready(&report),
            !wrong_ack
        );
        assert_eq!(report["observation_integrity"]["status"], "passed");
        assert!(trace.contains(&json!({"operation":"confirm_terminal_delivery","status":if wrong_ack { "failed" } else { "ok" }})));
        let ack_index = requests
            .iter()
            .position(|(path, _, _)| path == ACK)
            .unwrap();
        let (_, headers, body) = &requests[ack_index];
        assert!(headers
            .to_ascii_lowercase()
            .contains(&format!("x-cowd-client-id: {CLIENT}")));
        assert_eq!(
            *body,
            json!({"runtime_commit_cursor":"9007199254740993","content_digest":digest})
        );
        assert_eq!(
            requests[ack_index + 1].0,
            if wrong_ack { TIMELINE } else { CONTEXT }
        );
        if wrong_ack {
            assert_eq!(
                report["delivery_observation"]["error"],
                "receipt_ack_tuple_mismatch"
            );
        } else {
            assert_eq!(report["terminal_response_summary"], "56");
            assert_eq!(
                report["delivery_observation"]["fresh_context_confirmed"],
                true
            );
        }
    }
}

#[test]
fn u5_subinput_envelopes_keep_exact_read_grants_and_original_business_oracles() {
    assert!(u5_package_answer(
        r#"{"package_name":"runtime","path":"crates/runtime/Cargo.toml"}"#
    ));
    for wrong in [
        "读取 crates/runtime/Cargo.toml，package name 是 wrong",
        r#"{"package_name":"wrong","path":"crates/runtime/Cargo.toml"}"#,
        r#"{"path":"crates/runtime/Cargo.toml"}"#,
        r#"{"package_name":"runtime","path":"Cargo.toml"}"#,
    ] {
        assert!(!u5_package_answer(wrong), "{wrong}");
    }
    for (spec, path, wrong) in [
        (u5_inputs()[0], "Cargo.toml", "crates/runtime/Cargo.toml"),
        (u5_inputs()[1], "crates/runtime/Cargo.toml", "Cargo.toml"),
    ] {
        let prompt = controlled_live_prompt(spec.id, spec.prompt.into(), None);
        let control: Value = serde_json::from_str(
            prompt
                .lines()
                .next()
                .unwrap()
                .strip_prefix("COWD_EVAL_CONTROL ")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(control["corpus_id"], "live-scenarios-v1");
        assert!(control["resource_scopes"]
            .as_array()
            .unwrap()
            .contains(&json!(format!("read:{path}"))));
        assert!(!control["resource_scopes"]
            .as_array()
            .unwrap()
            .contains(&json!("read:.")));
        assert!(!control["resource_scopes"]
            .as_array()
            .unwrap()
            .contains(&json!(format!("read:{wrong}"))));
    }
    let original = u5_inputs()[0].acceptance.evaluate(
        &format!("Cargo.toml {}", env!("CARGO_PKG_VERSION")),
        &typed_read_timeline_fixture(),
        &[],
        None,
        "root",
    );
    assert!(original.passed);
    assert!(
        !u5_inputs()[0]
            .acceptance
            .evaluate(
                "Cargo.toml wrong-version",
                &typed_read_timeline_fixture(),
                &[],
                None,
                "root"
            )
            .passed
    );
    let mut wrong_source = typed_read_timeline_fixture();
    wrong_source["read_evidence"]["pages"][0]["reads"][0]["path"]["repository_id"] =
        json!("foreign");
    assert!(
        !u5_inputs()[0]
            .acceptance
            .evaluate(
                &format!("Cargo.toml {}", env!("CARGO_PKG_VERSION")),
                &wrong_source,
                &[],
                None,
                "root"
            )
            .passed
    );
}

#[test]
fn agentic_progress_counts_unique_structured_action_events() {
    let event = |cursor, action| {
        json!({
            "kind": "agentic.action_applied",
            "commit_cursor": cursor,
            "payload": {"envelope": {"action": {"kind": action}}}
        })
    };
    let team = event(1, "team_create");
    let timeline = json!({"events": [
        team.clone(),
        team,
        event(2, "task_publish"),
        event(3, "task_publish"),
        event(4, "task_publish"),
        event(5, "task_submit"),
        event(6, "task_submit"),
        event(7, "task_submit"),
        event(8, "task_review"),
        event(9, "task_review"),
        event(10, "task_review"),
        event(11, "objective_complete_request"),
        {"kind": "context.provider_request_packed", "payload": {"text": "team_create task_publish"}}
    ]});

    assert_eq!(
        LiveScenarioRunner::agentic_progress_from_timeline(&timeline),
        json!({
            "teams_created": 1,
            "tasks_published": 3,
            "tasks_submitted": 3,
            "tasks_reviewed": 3,
            "objective_complete_requested": true,
            "remaining": []
        })
    );
}

fn successful_root_outcome_timeline(mut timeline: Value) -> Value {
    let event = json!({
        "kind": "runtime.outcome.recorded.v1",
        "status": "succeeded",
        "payload": {
            "identity": {"execution_graph_ref": "root", "execution_scope":"session"},
            "terminal": {"class": "succeeded"}
        }
    });
    let object = timeline
        .as_object_mut()
        .expect("acceptance timeline fixture must be an object");
    object
        .entry("events")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .expect("timeline events must be an array")
        .push(event);
    timeline
}

fn completed_tool_event() -> Value {
    json!({
        "kind": "tool.invocation.completed",
        "status": "completed",
        "payload": {
            "status": "completed",
            "invocation_id": "tool-invocation-1",
            "tool_call_id": "tool-call-1",
            "tool_name": "read_file",
            "input_preview": "{\"path\":\"Cargo.toml\"}",
            "is_error": false
        }
    })
}

#[test]
fn live_acceptance_requires_execution_terminal_durable_response_and_goal_success() {
    let timeline = successful_root_outcome_timeline(json!({}));
    let acceptance = LiveAcceptance::Contains("56").evaluate("56", &timeline, &[], None, "root");
    let finalized = finalize_live_acceptance(acceptance, &timeline, "root");
    assert!(finalized.passed);
    assert_eq!(
        finalized
            .checks
            .iter()
            .find(|check| check["name"] == "durable_response")
            .unwrap()["passed"],
        true
    );
    assert_eq!(
        finalized
            .checks
            .iter()
            .find(|check| check["name"] == "run_reached_terminal_state")
            .unwrap()["passed"],
        true
    );
    assert_eq!(
        finalized
            .checks
            .iter()
            .find(|check| check["name"] == "root_business_outcome_succeeded")
            .unwrap()["passed"],
        true
    );
}

#[test]
fn live_acceptance_cannot_pass_when_durable_response_evidence_is_missing() {
    let timeline = successful_root_outcome_timeline(json!({}));
    let acceptance = LiveAcceptanceResult {
        passed: true,
        checks: vec![json!({"name": "response_contains", "passed": true})],
        quality: None,
    };
    let finalized = finalize_live_acceptance(acceptance, &timeline, "root");
    assert!(!finalized.passed);
    assert_eq!(
        finalized
            .checks
            .iter()
            .find(|check| check["name"] == "durable_response")
            .unwrap()["passed"],
        false
    );
    assert_eq!(
        finalized
            .checks
            .iter()
            .find(|check| check["name"] == "run_reached_terminal_state")
            .unwrap()["passed"],
        true,
        "execution terminal must be reported independently from delivery evidence"
    );
}

fn tool_acceptance() -> LiveAcceptance {
    LiveAcceptance::RequiresToolEvidence {
        tool_name: "read_file",
        target_path: "Cargo.toml",
    }
}

#[test]
fn explicit_live_scenario_selection_is_the_activation_authority() {
    let selected = BTreeSet::from([GROUP_THEORY_SCENARIO_ID.to_string()]);
    assert!(scenario_enabled(
        Some(&selected),
        GROUP_THEORY_SCENARIO_ID,
        false
    ));
    assert!(!scenario_enabled(
        Some(&selected),
        LARGE_SCALE_SCENARIO_ID,
        true
    ));
}

#[test]
fn legacy_expensive_scenario_opt_in_only_applies_without_selection() {
    assert!(scenario_enabled(None, GROUP_THEORY_SCENARIO_ID, true));
    assert!(!scenario_enabled(None, GROUP_THEORY_SCENARIO_ID, false));
}

#[test]
fn autonomous_deepseek_template_is_frozen_and_complete() {
    let template: AutonomousDeepseekTemplate = serde_json::from_str(include_str!(
        "../../templates/autonomous-collaboration-deepseek-v1.json"
    ))
    .expect("autonomous template");
    assert_eq!(template.schema_version, 2);
    assert_eq!(template.scenario_id, AUTONOMOUS_DEEPSEEK_SCENARIO_ID);
    assert_eq!(template.output_path, AUTONOMOUS_DEEPSEEK_OUTPUT_PATH);
    assert!(template.prompt_template.contains("{minimum_teams}"));
    assert!(template.prompt_template.contains("{minimum_agents}"));
    assert!(template.prompt_template.contains("{minimum_tasks}"));
    assert!(template.prompt_template.contains("task_supersede"));
    assert!(template
        .prompt_template
        .contains("objective_complete_request"));
    assert!(!template.prompt_template.contains("Team A"));
    assert!(!template.prompt_template.contains("恰好"));
}

fn agentic_program_projection(
    status: &str,
    include_evidence: bool,
) -> runtime::AgenticProgramProjection {
    let evidence_refs = include_evidence
        .then(|| vec!["evidence:read".to_string()])
        .unwrap_or_default();
    let teams = json!({
        "team-research": {"team_id":"team-research","name":"Research","mission":"research","objective":null,"topic_ref":"topic:research","created_by":"root","member_ids":["agent-a","agent-review"],"task_ids":["task-a"]},
        "team-synthesis": {"team_id":"team-synthesis","name":"Synthesis","mission":"synthesize","objective":null,"topic_ref":"topic:synthesis","created_by":"root","member_ids":["agent-b","agent-review"],"task_ids":["task-b"]}
    });
    let agents = json!({
        "agent-a": {"agent_id":"agent-a","team_id":"team-research","role":"researcher","mission":"research","required_capabilities":["read"],"invited_by":"root"},
        "agent-b": {"agent_id":"agent-b","team_id":"team-synthesis","role":"synthesizer","mission":"synthesize","required_capabilities":["read"],"invited_by":"root"},
        "agent-review": {"agent_id":"agent-review","team_id":"team-synthesis","role":"reviewer","mission":"review","required_capabilities":["read"],"invited_by":"root"}
    });
    let tasks = json!({
        "task-a": {"task_id":"task-a","team_id":"team-research","title":"research","objective":"research","acceptance":"evidence","required_capabilities":["read"],"depends_on":[],"status":"accepted","claimant":"agent-a","claim_generation":1,"claim_execution_id":"execution-a","claimed_at_ms":1,"lease_expires_at_ms":2,"artifact_refs":["artifact:a"],"evidence_refs":evidence_refs.clone(),"unresolved":[],"review_reason":"accepted","reviewed_by":"agent-review","failed_attempts":0,"review_generation":1,"failed_review_attempts":0,"last_failure":null,"replacement_task_refs":[],"supersede_evidence_refs":[],"superseded_reason":null,"superseded_by":null},
        "task-b": {"task_id":"task-b","team_id":"team-synthesis","title":"synthesis","objective":"synthesize","acceptance":"evidence","required_capabilities":["read"],"depends_on":["task-a"],"status":"accepted","claimant":"agent-b","claim_generation":1,"claim_execution_id":"execution-b","claimed_at_ms":1,"lease_expires_at_ms":2,"artifact_refs":["artifact:final"],"evidence_refs":evidence_refs,"unresolved":[],"review_reason":"accepted","reviewed_by":"agent-review","failed_attempts":0,"review_generation":1,"failed_review_attempts":0,"last_failure":null,"replacement_task_refs":[],"supersede_evidence_refs":[],"superseded_reason":null,"superseded_by":null}
    });
    let artifacts = json!({
        "artifact:a": {"artifact_ref":"artifact:a","content_ref":"content:a","kind":"research","title":"research","relates_to":["task-a"],"committed_by":"agent-a"},
        "artifact:final": {"artifact_ref":"artifact:final","content_ref":AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,"kind":"report","title":AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,"relates_to":["task-a","task-b"],"committed_by":"agent-b"}
    });
    serde_json::from_value(json!({
        "program_id": "program:test",
        "objective_id": "objective:test",
        "session_id": "session-test",
        "turn_id": "turn-test",
        "root_execution_id": "root",
        "required_team_count": 2,
        "objective_summary": "test autonomous collaboration",
        "model_lease": "model:test",
        "permission_ceiling": "read-only",
        "resource_scopes": ["read:crates"],
        "revision": 9,
        "status": status,
        "teams": teams,
        "agents": agents,
        "tasks": tasks,
        "topics": {"topic:program:test": [{"entry_id":"entry-1","revision":8,"actor_id":"agent-a","summary":"finding","content_ref":null,"refs":["artifact:a"]}]},
        "artifacts": artifacts,
        "final_artifact_ref": "artifact:final",
        "completion_request": {"action_id":"complete-1","requested_by":"root","program_revision":8,"result_refs":["artifact:final"],"primary_artifact_ref":"artifact:final","evidence_refs":["evidence:read"],"unresolved":[]},
        "objective_verdict": (status == "verified").then(|| json!({"goal_id":"objective:test","goal_revision":1,"terminal_fence":"fence","authority_revision":9,"kind":"satisfied"})),
        "unresolved": []
    })).expect("typed Agent-first Program fixture")
}

#[test]
fn agentic_evidence_integrity_rejects_accepted_tasks_without_evidence() {
    assert!(agentic_evidence_integrity(&agentic_program_projection("verified", true)).passed());
    assert!(!agentic_evidence_integrity(&agentic_program_projection("verified", false)).passed());
}

#[test]
fn autonomous_acceptance_requires_verified_agent_first_facts_and_physical_overlap() {
    let acceptance = LiveAcceptance::AutonomousCollaboration {
        minimum_teams: 2,
        minimum_agents: 3,
        minimum_tasks: 2,
        minimum_reviews: 2,
        minimum_cross_team_edges: 1,
        minimum_topics: 1,
        output_path: AUTONOMOUS_DEEPSEEK_OUTPUT_PATH,
    };
    let response = format!("已提交 {AUTONOMOUS_DEEPSEEK_OUTPUT_PATH}");
    let timeline = successful_root_outcome_timeline(json!({}));
    let physical = json!({"activities":[
        {"agent_instance_id":"agent-a","team_run_id":"team-research","status":"completed","started_at_ms":1,"completed_at_ms":10},
        {"agent_instance_id":"agent-b","team_run_id":"team-synthesis","status":"completed","started_at_ms":2,"completed_at_ms":8}
    ]});
    let verified = agentic_program_projection("verified", true);
    assert!(
        acceptance
            .evaluate(
                &response,
                &timeline,
                &[physical.clone()],
                Some(&verified),
                "root"
            )
            .passed
    );
    let open = agentic_program_projection("open", true);
    assert!(
        !acceptance
            .evaluate(&response, &timeline, &[physical], Some(&open), "root")
            .passed
    );
}

#[test]
fn release_certification_requires_every_registered_core_scenario() {
    let complete = RELEASE_CERTIFICATION_SCENARIOS
        .iter()
        .map(|scenario_id| json!({"scenario_id": scenario_id, "status": "passed"}))
        .collect::<Vec<_>>();
    assert!(release_certification_scenarios_present(&complete));

    let incomplete = complete
        .into_iter()
        .filter(|scenario| {
            scenario.get("scenario_id").and_then(Value::as_str) != Some(EXPLICIT_TEAM_SCENARIO_ID)
        })
        .collect::<Vec<_>>();
    assert!(!release_certification_scenarios_present(&incomplete));
}

#[test]
fn unknown_or_unscheduled_explicit_scenarios_fail_before_dispatch() {
    let selected = BTreeSet::from([GROUP_THEORY_SCENARIO_ID.to_string()]);
    let unregistered = BTreeSet::new();
    let errors = live_scenario_selection_errors(Some(&selected), &unregistered);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["scenario_id"], GROUP_THEORY_SCENARIO_ID);
    assert!(!live_scenario_selection_passed(Some(&selected), &errors, 0));

    let registered = BTreeSet::from([GROUP_THEORY_SCENARIO_ID.to_string()]);
    let errors = live_scenario_selection_errors(Some(&selected), &registered);
    assert!(errors.is_empty());
    assert!(!live_scenario_selection_passed(Some(&selected), &errors, 0));
    assert!(live_scenario_selection_passed(Some(&selected), &errors, 1));
}

#[test]
fn live_prompt_uses_an_optional_advisory_provider_token_telemetry_lease() {
    let controlled = controlled_live_prompt(
        "live_group_theory_ai_research_simulation",
        "complete the research".to_string(),
        Some(5_000_000),
    );
    let (header, prompt) = controlled.split_once('\n').expect("control header");
    let encoded = header
        .strip_prefix("COWD_EVAL_CONTROL ")
        .expect("typed evaluation prefix");
    let control: Value = serde_json::from_str(encoded).expect("control JSON");
    assert_eq!(control["corpus_id"], "live-scenarios-v1");
    assert_eq!(control["provider_constraint"], "normal");
    assert_eq!(
        control["resource_scopes"],
        json!([
            "provider",
            "provider_account",
            "provider_token_pool",
            "read:."
        ])
    );
    assert_eq!(control["max_total_tokens"], 5_000_000);
    assert!(control["budget_lease_id"]
        .as_str()
        .is_some_and(|id| id.starts_with("live-scenario:live_group_theory")));
    assert_eq!(prompt, "complete the research");

    let tool_controlled =
        controlled_live_prompt("live_tool_evidence", "read the manifest".to_string(), None);
    let (tool_header, _) = tool_controlled
        .split_once('\n')
        .expect("tool control header");
    let tool_control: Value = serde_json::from_str(
        tool_header
            .strip_prefix("COWD_EVAL_CONTROL ")
            .expect("typed evaluation prefix"),
    )
    .expect("tool control JSON");
    assert_eq!(
        tool_control["resource_scopes"],
        json!([
            "provider",
            "provider_account",
            "provider_token_pool",
            "read:Cargo.toml",
            "read:."
        ])
    );
    assert!(tool_control.get("max_total_tokens").is_none());
    assert!(tool_control.get("budget_lease_id").is_none());

    let autonomous = controlled_live_prompt(
        AUTONOMOUS_DEEPSEEK_SCENARIO_ID,
        "run autonomous collaboration".to_string(),
        None,
    );
    let (autonomous_header, _) = autonomous.split_once('\n').expect("control header");
    let autonomous_control: Value = serde_json::from_str(
        autonomous_header
            .strip_prefix("COWD_EVAL_CONTROL ")
            .expect("typed evaluation prefix"),
    )
    .expect("autonomous control JSON");
    assert_eq!(
        autonomous_control["resource_scopes"],
        json!([
            "provider",
            "provider_account",
            "provider_token_pool",
            "workspace:.",
            "network:*"
        ])
    );
}

#[test]
fn live_health_contracts_accept_only_semantically_ready_payloads() {
    let fixtures = [
        (LiveHealthContract::Gateway, json!({"status": "healthy"})),
        (
            LiveHealthContract::Runtime,
            json!({"ok": true, "execution": {"lifecycle": "open", "last_error": null}}),
        ),
        (LiveHealthContract::RuntimeOutbox, json!({"healthy": true})),
        (
            LiveHealthContract::RuntimeControlPlane,
            json!({"readiness": {"production_ready": true, "required_blocked": 0}}),
        ),
        (
            LiveHealthContract::EvolutionProjectors,
            json!({
                "projector": {"worker_running": true, "consecutive_failures": 0, "dead_letter_count": 0},
                "outcome_projector": {"worker_running": true, "consecutive_failures": 0, "dlq_count": 0}
            }),
        ),
        (
            LiveHealthContract::SurfaceHost,
            json!({
                "status": "ready",
                "host": {
                    "failed_count": 0,
                    "circuit_open_count": 0,
                    "task_ownership": {"overloaded": false}
                }
            }),
        ),
    ];
    for (contract, payload) in fixtures {
        let observation = semantic_health_observation("/probe", contract, payload);
        assert_eq!(observation["status"], "passed", "{observation}");
        assert_eq!(observation["failed_checks"], json!([]));
    }
}

#[test]
fn http_success_with_non_ready_control_plane_fails_closed() {
    let observation = semantic_health_observation(
        "/api/runtime/control-plane",
        LiveHealthContract::RuntimeControlPlane,
        json!({
            "status": "attention",
            "readiness": {"production_ready": false, "required_blocked": 1}
        }),
    );

    assert_eq!(observation["status"], "failed");
    assert_eq!(observation["failed_checks"].as_array().unwrap().len(), 2);
    assert_eq!(
        observation["reason"],
        "HTTP transport succeeded but the endpoint semantic health contract failed"
    );
}

#[test]
fn missing_health_fields_never_default_to_success() {
    for contract in [
        LiveHealthContract::Gateway,
        LiveHealthContract::Runtime,
        LiveHealthContract::RuntimeOutbox,
        LiveHealthContract::RuntimeControlPlane,
        LiveHealthContract::EvolutionProjectors,
        LiveHealthContract::SurfaceHost,
    ] {
        let observation = semantic_health_observation("/probe", contract, json!({}));
        assert_eq!(observation["status"], "failed", "{observation}");
        assert!(!observation["failed_checks"].as_array().unwrap().is_empty());
    }
}

#[test]
fn root_terminal_requires_completed_synthesis_not_child_progress() {
    let pending = json!({
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "completed"},
            {"node_id": "tools", "kind": "tool_batch", "status": "running"}
        ]}
    });
    assert_eq!(
        root_execution_terminal_state(&pending),
        RootExecutionTerminal::Pending
    );

    let completed = json!({
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "completed"},
            {"node_id": "synthesis", "kind": "synthesize", "status": "completed"}
        ]}
    });
    assert_eq!(
        root_execution_terminal_state(&completed),
        RootExecutionTerminal::Completed
    );
}

#[test]
fn root_terminal_reports_terminal_failure_without_synthesis() {
    let failed = json!({
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "failed"}
        ]}
    });
    assert!(matches!(
        root_execution_terminal_state(&failed),
        RootExecutionTerminal::Failed(_)
    ));
}

#[test]
fn root_progress_fingerprint_tracks_streaming_output_without_graph_changes() {
    let first = json!({
        "revision": 7,
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "running"}
        ]},
        "live": {
            "revision": 11,
            "status": "calling_model",
            "output_bytes": 1024,
            "last_progress_at_ms": 100
        }
    });
    let second = json!({
        "revision": 7,
        "graph": {"nodes": [
            {"node_id": "model", "kind": "inline_model", "status": "running"}
        ]},
        "live": {
            "revision": 12,
            "status": "calling_model",
            "output_bytes": 2048,
            "last_progress_at_ms": 200
        }
    });
    let first_statuses = root_node_statuses(&first);
    let second_statuses = root_node_statuses(&second);

    assert_eq!(first_statuses, second_statuses);
    assert_ne!(
        root_progress_fingerprint(&first, &first_statuses),
        root_progress_fingerprint(&second, &second_statuses)
    );
}

#[test]
fn blocked_team_is_terminal_for_waiting_but_unfinished_agent_is_not() {
    let blocked_teams = ProjectedTeamHealth {
        agent_count: 12,
        completed_agents: 12,
        failed_agents: 0,
        team_count: 4,
        completed_teams: 1,
        failed_teams: 2,
        pending_tasks: 0,
        completion_verdict_pending: false,
    };
    assert!(!blocked_teams.has_pending_work());
    assert!(!blocked_teams.satisfies(1));

    let live_agent = ProjectedTeamHealth {
        agent_count: 12,
        completed_agents: 11,
        failed_agents: 0,
        team_count: 4,
        completed_teams: 1,
        failed_teams: 0,
        pending_tasks: 1,
        completion_verdict_pending: false,
    };
    assert!(live_agent.has_pending_work());
}

#[test]
fn descendant_live_terminal_never_forces_root_terminal_polling() {
    let child = ExecutionLiveObservation {
        fingerprint: "child".to_string(),
        summary: json!({
            "execution_id": "child-execution",
            "live_status": "complete",
        }),
        response_body_bytes: 1,
    };
    assert!(!live_terminal_belongs_to_root(&child, "root-execution"));

    let root = ExecutionLiveObservation {
        fingerprint: "root".to_string(),
        summary: json!({
            "execution_id": "root-execution",
            "live_status": "complete",
        }),
        response_body_bytes: 1,
    };
    assert!(live_terminal_belongs_to_root(&root, "root-execution"));
}

#[test]
fn team_acceptance_does_not_pass_without_a_real_projection_team_or_agents() {
    let answer =
        "runtime memory gateway event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
    let receipts = json!({"evidence": [
        {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
        {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
    ]});
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(answer, &receipts, &[], None, "root");
    assert!(!result.passed);
    let program = agentic_program_projection("verified", true);
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 2,
        minimum_claimed_cross_team_edges: 1,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(receipts.clone()),
        &[json!({
            "execution_id": "root",
            "revision": 1,
            "activities": [
                {"agent_instance_id":"agent-a","team_run_id":"team-research","status":"completed","started_at_ms":1,"completed_at_ms":10},
                {"agent_instance_id":"agent-b","team_run_id":"team-synthesis","status":"completed","started_at_ms":2,"completed_at_ms":8}
            ]
        })],
        Some(&program),
        "root",
    );
    assert!(result.passed);
}

#[test]
fn architecture_acceptance_rejects_failed_team_even_when_prose_claims_evidence() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs；但无法确认，因为没有任何文件内容的读取证据";
    let mut program = agentic_program_projection("blocked", true);
    program.tasks.get_mut("task-b").expect("task").status = runtime::AgenticTaskStatus::Blocked;
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]}),
        &[json!({"execution_id":"root","revision":1,"activities":[]})],
        Some(&program),
        "root",
    );
    assert!(!result.passed);
}

#[test]
fn architecture_acceptance_rejects_failed_physical_agent_graph() {
    let answer = "runtime memory gateway canonical event risk";
    let program = agentic_program_projection("verified", true);
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(json!({})),
        &[json!({
            "execution_id": "root",
            "revision": 1,
            "activities": [{
                "agent_instance_id": "reviewer-agent",
                "team_run_id": "review-team",
                "status": "failed",
                "started_at_ms": 1,
                "completed_at_ms": 2
            }]
        })],
        Some(&program),
        "root",
    );

    assert!(!result.passed);
    assert!(result.checks.iter().any(|check| {
        check["name"] == "physical_agent_graphs_succeeded"
            && check["passed"] == false
            && check["failed_activities"] == 1
    }));
}

#[test]
fn partial_team_is_terminal_unsuccessful_not_pending_work() {
    let mut program = agentic_program_projection("open", true);
    program.tasks.get_mut("task-b").expect("task").status = runtime::AgenticTaskStatus::Blocked;
    let health = projected_team_health(&program);

    assert_eq!(health.failed_teams, 1);
    assert!(!health.has_pending_work());
    assert!(!health.satisfies(1));
}

#[test]
fn architecture_quality_uses_durable_runtime_evidence_not_response_language() {
    let quality = architecture_quality(
        &json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]}),
        &[json!({
            "execution_id": "root",
            "revision": 3,
        })],
        None,
    );
    assert_eq!(quality.score, quality.required);
}

#[test]
fn large_scale_presentation_gate_rejects_old_concatenated_terminal() {
    let old = "team-runtime: # Verified Team evidence bundle\nRuntime delivery facts: 2/2\n[truncated]\n并发波次、关键瓶颈、失效模式、容量边界、扩大规模：Op";
    let checks = large_scale_presentation_checks(old);
    assert!(checks
        .iter()
        .any(|check| check["name"] == "presentation_transport_clean" && check["passed"] == false));
    assert!(checks
        .iter()
        .any(|check| check["name"] == "presentation_complete_ending" && check["passed"] == false));
}

#[test]
fn large_scale_presentation_gate_accepts_complete_synthesized_terminal() {
    let response = format!(
        "## 已验证事实\n{}\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF。\n\n## 源码推断\n边界推断。\n\n## 未执行的模拟\n本次未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩容结论\n\n判定：适合在当前单节点边界内继续扩大协作规模，但横向扩展必须先完成持久层分片。",
        LARGE_SCALE_SOURCE_PATHS[..6].join(" ")
    );
    assert!(large_scale_presentation_checks(&response)
        .iter()
        .all(|check| check["passed"] == true));
}

#[test]
fn scale_recommendation_requires_subject_and_decision_in_the_same_semantic_block() {
    assert!(!has_scale_recommendation("## 扩大规模结论\n结论完整。"));
    assert!(!has_scale_recommendation("- 扩容建议"));
    assert!(!has_scale_recommendation(
        "当前系统需要扩容。\n\n建议先完成持久层分片。"
    ));
    assert!(has_scale_recommendation(
        "暂不建议扩容，需先消除恢复串行瓶颈。"
    ));
    assert!(has_scale_recommendation(
        "The system is suitable to scale out, but must shard the event store first."
    ));
    assert!(!has_scale_recommendation(
        "## 扩大规模\n结论待定。\n\n## 其他事项\n系统适合当前工作。"
    ));
}

#[test]
fn observed_qwen_scale_conclusion_is_semantically_complete() {
    let observed = "判定：适合在当前单节点边界内继续扩大协作规模，但横向扩展存在明确架构前提。\n\n- 扩容建议：在单节点内可继续增加 Team/角色规模；若需跨节点横向扩展，应先行引入事件存储分片或 Postgres 后端。";
    assert!(has_scale_recommendation(observed));
}

#[test]
fn observed_deepseek_scale_section_is_semantically_complete() {
    let observed = "## 9. 是否适合继续扩大规模\n\n**结论：适合，以中等规模为当前安全边界。** 各层容量独立成闸；但在进一步扩大前应处理四项结构性项。";
    assert!(has_scale_recommendation(observed));
}

#[test]
fn large_scale_presentation_gate_rejects_positive_phrase_with_coverage_failure() {
    let response =
        "12/12 目标源码已完整读取到 EOF。源码完整覆盖维度：未通过；不能将本次任务判定为完全通过。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_complete_source_coverage" && check["passed"] == false
    }));
}

#[test]
fn large_scale_presentation_gate_rejects_independent_review_contradiction() {
    let response = "12/12 目标源码已完整读取到 EOF。12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF。但 reviewer 未独立重读源码。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_independent_source_review" && check["passed"] == false
    }));
}

#[test]
fn complete_source_receipt_gate_requires_attested_exact_content_for_every_target() {
    fn receipt(path: &str, sequence: u64) -> Value {
        json!({
            "observed_at_sequence": sequence,
            "tool_name": "read_file",
            "target": {
                "kind": "workspace",
                "scope": {
                    "access_mode": "read",
                    "coverage": "exact_content",
                    "path": {
                        "workspace_relative_path": path,
                        "observed_revision_or_digest": "a".repeat(64),
                    }
                }
            }
        })
    }

    let complete = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .enumerate()
        .map(|(index, path)| receipt(path, index as u64 + 1))
        .collect::<Vec<_>>();
    assert_eq!(
        complete_exact_source_receipt_paths(
            &json!({"observed_acceptance": {"observed_evidence": complete}}),
            &[],
        )
        .len(),
        LARGE_SCALE_SOURCE_PATHS.len()
    );

    let mut incomplete = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .take(11)
        .enumerate()
        .map(|(index, path)| receipt(path, index as u64 + 1))
        .collect::<Vec<_>>();
    let mut bounded = receipt(LARGE_SCALE_SOURCE_PATHS[11], 12);
    bounded["target"]["scope"]["coverage"] = json!("scoped_content");
    incomplete.push(bounded);
    let observed = complete_exact_source_receipt_paths(
        &json!({"observed_acceptance": {"observed_evidence": incomplete}}),
        &[],
    );
    assert_eq!(observed.len(), 11);
    assert!(!observed.contains(LARGE_SCALE_SOURCE_PATHS[11]));
}

#[test]
fn independent_source_review_gate_requires_distinct_role_receipts_for_every_target() {
    fn receipt(path: &str, sequence: u64, role: &str) -> Value {
        json!({
            "observed_at_sequence": sequence,
            "tool_name": "read_file",
            "target": {
                "kind": "workspace",
                "scope": {
                    "access_mode": "read",
                    "coverage": "exact_content",
                    "path": {
                        "workspace_relative_path": path,
                        "observed_revision_or_digest": "b".repeat(64),
                    }
                }
            },
            "evidence_ref": {
                "evidence_ref": {
                    "id": format!("agent-tool:team-graph:team-a:{role}:1:1:{sequence}:read_file:digest:read-receipt")
                }
            }
        })
    }

    let mut receipts = Vec::new();
    for (index, path) in LARGE_SCALE_SOURCE_PATHS.iter().enumerate() {
        receipts.push(receipt(path, index as u64 * 2 + 1, "team-a-investigator"));
        receipts.push(receipt(path, index as u64 * 2 + 2, "team-a-reviewer"));
    }
    assert_eq!(
        independently_reviewed_complete_source_receipt_paths(
            &json!({"observed_acceptance": {"observed_evidence": receipts}}),
            &[],
        )
        .len(),
        LARGE_SCALE_SOURCE_PATHS.len()
    );

    let investigator_only = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .enumerate()
        .map(|(index, path)| receipt(path, index as u64 + 1, "investigator"))
        .collect::<Vec<_>>();
    assert!(independently_reviewed_complete_source_receipt_paths(
        &json!({"observed_acceptance": {"observed_evidence": investigator_only}}),
        &[],
    )
    .is_empty());
    assert_eq!(
        receipt_agent_identity(
            "agent-tool:team-graph:program:team-a:0:role-a5684f8888daf18c:1:1:2:read_file:digest:read-receipt"
        ),
        Some("agent-tool:team-graph:program:team-a:0:role-a5684f8888daf18c")
    );
    assert!(receipt_agent_identity(
        "agent-tool:graph:role-a5684f8888daf18c:not-a-slot:1:2:read_file:receipt"
    )
    .is_none());

    let duplicate_reads_from_one_agent = LARGE_SCALE_SOURCE_PATHS
        .iter()
        .enumerate()
        .flat_map(|(index, path)| {
            [
                receipt(path, index as u64 * 2 + 1, "role-a5684f8888daf18c"),
                receipt(path, index as u64 * 2 + 2, "role-a5684f8888daf18c"),
            ]
        })
        .collect::<Vec<_>>();
    assert!(independently_reviewed_complete_source_receipt_paths(
        &json!({"observed_acceptance": {"observed_evidence": duplicate_reads_from_one_agent}}),
        &[],
    )
    .is_empty());
}

#[test]
fn group_theory_gate_requires_exact_reads_from_the_unique_sink_team() {
    fn receipt(path: &str, sequence: u64, semantic_team_id: &str) -> Value {
        json!({
            "observed_at_sequence": sequence,
            "tool_name": "read_file",
            "target": {
                "kind": "workspace",
                "scope": {
                    "access_mode": "read",
                    "coverage": "exact_content",
                    "path": {
                        "workspace_relative_path": path,
                        "observed_revision_or_digest": "d".repeat(64),
                    }
                }
            },
            "evidence_ref": {
                "evidence_ref": {
                    "id": format!("agent-tool:team-graph:program:{semantic_team_id}:0:role:1:1:{sequence}:read_file:digest:read-receipt")
                }
            }
        })
    }

    let observed = |receipt_team: &str| {
        let receipts = GROUP_THEORY_SOURCE_PATHS
            .iter()
            .enumerate()
            .map(|(index, path)| receipt(path, index as u64 + 1, receipt_team))
            .collect::<Vec<_>>();
        json!({"observed_acceptance": {"observed_evidence": receipts}})
    };
    let program = agentic_program_projection("verified", true);
    let terminal = terminal_semantic_team_ids(&program);
    assert_eq!(terminal, BTreeSet::from(["team-synthesis".to_string()]));
    assert!(complete_exact_source_receipt_paths_for_semantic_teams(
        &observed("team-research"),
        &[],
        &terminal,
    )
    .is_empty());
    assert_eq!(
        complete_exact_source_receipt_paths_for_semantic_teams(
            &observed("team-synthesis"),
            &[],
            &terminal,
        )
        .len(),
        GROUP_THEORY_SOURCE_PATHS.len()
    );
}

#[test]
fn source_receipt_gate_rejects_acquisition_receipts_not_promoted_to_agent_acceptance() {
    let raw_receipt = json!({
        "observed_at_sequence": 1,
        "tool_name": "read_file",
        "target": {
            "kind": "workspace",
            "scope": {
                "access_mode": "read",
                "coverage": "exact_content",
                "path": {
                    "workspace_relative_path": LARGE_SCALE_SOURCE_PATHS[0],
                    "observed_revision_or_digest": "c".repeat(64),
                }
            }
        },
        "evidence_ref": {
            "evidence_ref": {
                "id": "agent-tool:team-graph:team-a:reviewer:1:1:1:read_file:digest:read-receipt"
            }
        }
    });

    assert!(complete_exact_source_receipt_paths(
        &json!({"durable_tool_receipts": [raw_receipt]}),
        &[],
    )
    .is_empty());
}

#[test]
fn large_scale_presentation_gate_rejects_receipt_only_content_review_caveat() {
    let response = "## 已验证事实\ncrates/runtime/src/agentic/program.rs crates/runtime/src/agentic/action_service.rs crates/runtime/src/agentic/execution.rs crates/runtime/src/conversation/host.rs crates/runtime/src/execution_core/services.rs crates/runtime/src/recovery/runtime_event_reactor.rs\n\n12/12 目标源码已完整读取到 EOF。\n12/12 目标源码已由两个不同 Agent 身份独立完整读取到 EOF。\n\n## 源码推断\nreviewer 仅在收据层级确认，正文未保留，内容级复核未完成。\n\n## 未执行的模拟\n本次未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_independent_source_review" && check["passed"] == false
    }));
}

#[test]
fn large_scale_transport_gate_allows_generic_source_identifier_examples() {
    let response = "## 已验证事实\ncrates/runtime/src/agentic/program.rs\n\n源码中的通用图标识格式为 `team-graph:{team_id}`。\n\n## 源码推断\n推断。\n\n## 未执行的模拟\n未执行模拟。\n\n## 并发波次、关键瓶颈、失效模式、容量边界与扩大规模结论\n结论完整。";
    let checks = large_scale_presentation_checks(response);

    assert!(checks.iter().any(|check| {
        check["name"] == "presentation_transport_clean" && check["passed"] == true
    }));
}

#[test]
fn projected_team_health_uses_only_agent_first_task_facts() {
    let program = agentic_program_projection("verified", true);
    let health = projected_team_health(&program);

    assert!(health.satisfies(2));
    assert_eq!(health.team_count, 2);
    assert_eq!(health.completed_teams, 2);
    assert_eq!(health.agent_count, 3);
    assert_eq!(health.completed_agents, 3);
}

#[test]
fn team_acceptance_waits_for_running_descendant_work() {
    let mut program = agentic_program_projection("open", true);
    let pending = program.tasks.get_mut("task-b").expect("task");
    pending.status = runtime::AgenticTaskStatus::Published;
    pending.claimant = None;
    pending.reviewed_by = None;
    let health = projected_team_health(&program);

    assert!(
        health.has_pending_work(),
        "unclaimed Published Task can still run"
    );
    let mut completion_requested = agentic_program_projection("completion_requested", true);
    completion_requested.tasks.values_mut().for_each(|task| {
        task.status = runtime::AgenticTaskStatus::Accepted;
    });
    let completion_health = projected_team_health(&completion_requested);
    assert!(
        completion_health.has_pending_work(),
        "supervisor verdict remains a live Program transition"
    );
    assert!(LiveAcceptance::ArchitectureQuality {
        minimum_teams: 1,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .requires_descendant_team_closure());
    assert!(!LiveAcceptance::ArchitectureQuality {
        minimum_teams: 0,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .requires_descendant_team_closure());
}

#[test]
fn architecture_acceptance_requires_accepted_cross_team_dependencies() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/memory/src/lib.rs";
    let receipts = json!({"evidence": [
        {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
        {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
    ]});
    let program = agentic_program_projection("verified", true);
    assert_eq!(accepted_cross_team_dependency_count(&program), 1);
    let projection = json!({"execution_id":"root","revision":1,"activities":[
        {"agent_instance_id":"agent-a","team_run_id":"team-research","status":"completed","started_at_ms":1,"completed_at_ms":10},
        {"agent_instance_id":"agent-b","team_run_id":"team-synthesis","status":"completed","started_at_ms":2,"completed_at_ms":8}
    ]});
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 2,
        minimum_claimed_cross_team_edges: 1,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(receipts),
        &[projection],
        Some(&program),
        "root",
    );

    assert!(result.passed);
}

#[test]
fn architecture_acceptance_does_not_reject_durable_execution_for_hallucinated_paths_in_prose() {
    let answer = "runtime memory gateway canonical event risk crates/runtime/src/lib.rs crates/not-a-real-module/src/memory.rs";
    let result = LiveAcceptance::ArchitectureQuality {
        minimum_teams: 0,
        minimum_claimed_cross_team_edges: 0,
        evidence_profile: ArchitectureEvidenceProfile::Basic,
    }
    .evaluate(
        answer,
        &successful_root_outcome_timeline(json!({"evidence": [
            {"tool_name": "read_file", "is_error": false, "evidence_id": "read-1"},
            {"tool_name": "grep_search", "is_error": false, "evidence_id": "read-2"}
        ]})),
        &[json!({
            "execution_id": "root",
            "revision": 1,
        })],
        None,
        "root",
    );
    assert!(result.passed);
}

#[test]
fn source_path_extraction_stops_at_cjk_punctuation_before_explanation() {
    let paths = source_paths(
        "证据：`crates/runtime/src/lib.rs`：模块注释说明职责；另见 crates/memory/src/lib.rs。",
    );
    assert_eq!(
        paths,
        BTreeSet::from([
            "crates/memory/src/lib.rs".to_string(),
            "crates/runtime/src/lib.rs".to_string(),
        ])
    );
}

#[test]
fn tool_acceptance_rejects_answer_without_runtime_evidence() {
    let result =
        tool_acceptance().evaluate("Cargo.toml", &json!({"events": []}), &[], None, "root");
    assert!(!result.passed);
}

fn typed_read_timeline_fixture() -> Value {
    // Independent expected identity, rather than a path extracted from the
    // response/tool preview. This fixture exercises consumer semantics only.
    let expected = harness_contract::context::WorkspacePathIdentity {
        workspace_id: "workspace".into(),
        repository_id: "repository".into(),
        workspace_relative_path: "Cargo.toml".into(),
        repository_relative_path: "Cargo.toml".into(),
        object_kind: harness_contract::context::WorkspaceObjectKind::File,
        observed_revision_or_digest: None,
    };
    let mut page = crate::facet_gate::read_proof_tests::page();
    page.execution_id = "root".into();
    page.reads[0].path = expected.clone();
    page.reads[0].scope.target = Some("Cargo.toml".into());
    page.reads[0].step.execution_id = Some("root".into());
    if let harness_contract::projection::ReadEvidenceRecipient::Root { binding } = &mut page.reads[0].recipient {
        binding.root_execution_id = "root".into();
    }
    let mut evidence = crate::facet_gate::ReadEvidencePages::new(
        "root", "session", "turn",
        BTreeMap::from([("Cargo.toml".to_string(), expected)]),
    );
    evidence.accept(page).unwrap();
    let mut timeline = successful_root_outcome_timeline(json!({}));
    timeline["read_evidence"] = evidence.report();
    timeline
}

#[test]
fn tool_acceptance_requires_completed_runtime_receipt_and_succeeded_outcome() {
    let preview_only = tool_acceptance().evaluate(
        "Cargo.toml version 0.9.712",
        &successful_root_outcome_timeline(json!({"events": [completed_tool_event()]})),
        &[],
        None,
        "root",
    );
    assert!(!preview_only.passed, "completed input preview is not delivered content");

    let result = tool_acceptance().evaluate(
        "Cargo.toml version 0.9.712", &typed_read_timeline_fixture(), &[], None, "root",
    );
    assert!(result.passed, "typed read plus real root success: {:?}", result.checks);
    for fault in ["target", "grade", "cut", "omitted"] {
        let mut timeline = typed_read_timeline_fixture();
        let page = &mut timeline["read_evidence"]["pages"][0];
        match fault {
            "target" => page["reads"][0]["path"]["workspace_relative_path"] = json!("README.md"),
            "grade" => page["reads"][0]["grade"] = json!("agent_channel_send"),
            "cut" => page["reads"][0]["delivery"]["commit_cursor"] = json!(11),
            "omitted" => page["reads"][0]["body_delivered"] = json!(false),
            _ => unreachable!(),
        }
        assert!(!tool_acceptance().evaluate("Cargo.toml", &timeline, &[], None, "root").passed, "{fault}");
    }

    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [{
            "kind": "tool.invocation.failed",
            "status": "failed",
            "payload": {
                "status": "failed",
                "invocation_id": "tool-invocation-1",
                "tool_call_id": "tool-call-1",
                "tool_name": "read_file"
            }
        }]})),
        &[],
        None,
        "root",
    );
    assert!(!result.passed);

    let mut wrong_tool = completed_tool_event();
    wrong_tool["payload"]["tool_name"] = json!("glob_search");
    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [wrong_tool]})),
        &[],
        None,
        "root",
    );
    assert!(!result.passed, "an unrelated successful tool must not pass");

    let mut wrong_target = completed_tool_event();
    wrong_target["payload"]["input_preview"] = json!("{\"path\":\"README.md\"}");
    let result = tool_acceptance().evaluate(
        "Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [wrong_target]})),
        &[],
        None,
        "root",
    );
    assert!(
        !result.passed,
        "a successful read of the wrong target must not pass"
    );
}

#[test]
fn provider_metadata_and_zero_tool_count_are_not_live_tool_evidence() {
    let result = tool_acceptance().evaluate(
        "I read Cargo.toml",
        &successful_root_outcome_timeline(json!({"events": [{
                "kind": "provider.request.packed",
                "payload": {"capabilities": {"tool_calls": "supported/configured"}}
            }, {"tool_calls": 0}]})),
        &[json!({"usage": [{"detail": {"tool_calls": 0}}]})],
        None,
        "root",
    );
    assert!(
        !result.passed,
        "metadata and a declared zero count must never validate a claimed tool run"
    );
}

#[test]
fn local_outcome_cannot_replace_root_business_observation() {
    let mut timeline = successful_root_outcome_timeline(json!({}));
    for scope in ["agent", "team", "task", "unknown"] {
        timeline["events"][0]["payload"]["identity"]["execution_scope"] = json!(scope);
        assert!(root_business_observation(&timeline, "root").is_null());
        assert!(!root_business_outcome(&timeline, "root").passed);
    }
}

#[test]
fn partial_failed_or_missing_root_outcome_fails_closed() {
    for (status, class) in [("failed", "failed"), ("partial", "partial")] {
        let mut timeline = typed_read_timeline_fixture();
        timeline["events"][0]["status"] = json!(status);
        timeline["events"][0]["payload"]["terminal"]["class"] = json!(class);
        let result = tool_acceptance().evaluate("Cargo.toml", &timeline, &[], None, "root");
        assert!(!result.passed, "{status}/{class} must fail closed");
    }

    let mut no_root = typed_read_timeline_fixture();
    no_root["events"] = json!([]);
    let missing = tool_acceptance().evaluate(
        "Cargo.toml",
        &no_root,
        &[],
        None,
        "root",
    );
    assert!(!missing.passed);
}

#[test]
fn scenario_metrics_sum_only_canonical_token_usage_records() {
    let timeline = json!({
        "token_speed": {
            "token_usage": [
                {"input": 10, "output": 5, "cache_create": 2, "cache_read": 3},
                {"input": 7, "output": 11, "cache_create": 0, "cache_read": 4}
            ],
            "model_telemetry": {
                "first_token_latency_ms": 125,
                "wall_tokens_per_second": 42.5,
                "active_tokens_per_second": 56.0
            }
        },
        "tool_summary": {"count": 2},
        "team_session": {"runtime_run_count": 2}
    });
    let metrics = scenario_metrics(
        &timeline,
        &[json!({"agents": [{"id":"agent"}], "teams": [{"id":"team"}]})],
        None,
        Duration::from_secs(2),
    );

    assert_eq!(metrics["input_tokens"], 17);
    assert_eq!(metrics["output_tokens"], 16);
    assert_eq!(metrics["cache_creation_input_tokens"], 2);
    assert_eq!(metrics["cache_read_input_tokens"], 7);
    assert_eq!(metrics["cache_tokens"], 9);
    assert_eq!(metrics["total_tokens"], 42);
    assert_eq!(metrics["token_usage_records"], 2);
    assert_eq!(metrics["tool_calls"], 2);
    assert_eq!(metrics["model_rounds"], 2);
    assert_eq!(metrics["first_token_latency_ms"], 125);
    assert_eq!(metrics["wall_tokens_per_second"], 42.5);
}

#[test]
fn scenario_metrics_prefer_unique_provider_attempt_outcomes_and_exact_prefix_evidence() {
    let packed = |request_id: &str, prompt: u64, reusable: u64, cold: bool| {
        json!({
            "kind": "context.provider_request_packed",
            "payload": {
                "type": "ProviderRequestPacked",
                "request_id": request_id,
                "model": "deepseek-v4-flash",
                "cache_identity_sha256": "cohort-a",
                "model_visible_prompt_bytes": prompt,
                "reusable_prefix_bytes": reusable,
                "exact_prefix_extension": reusable > 0,
                "cache_cold_leader": cold,
                "waited_for_cache_warmup": !cold
            }
        })
    };
    let outcome = |request_id: &str, miss: u64, read: u64| {
        json!({
            "kind": "context.provider_attempt_outcome",
            "payload": {
                "type": "ProviderAttemptOutcome",
                "request_id": request_id,
                "terminal_status": "completed",
                "usage_status": "known",
                "cache_dimensions_status": "known",
                "usage": {
                    "input_tokens": miss,
                    "output_tokens": 2,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": read
                }
            }
        })
    };
    let first_outcome = outcome("request-1", 100, 0);
    let timeline = json!({
        "events": [
            packed("request-1", 1000, 0, true),
            first_outcome.clone(),
            packed("request-2", 1100, 1000, false),
            outcome("request-2", 10, 90),
            first_outcome
        ],
        "team_session": {"runtime_run_count": 2},
        "token_speed": {"token_usage": [{"input": 9999, "output": 9999}]}
    });
    let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));

    assert_eq!(metrics["input_tokens"], 110);
    assert_eq!(metrics["cache_read_input_tokens"], 90);
    assert_eq!(metrics["provider_attempt_count"], 2);
    assert_eq!(metrics["provider_attempt_usage_unknown_count"], 0);
    assert_eq!(
        metrics["provider_attempt_cache_dimensions_unknown_count"],
        0
    );
    assert_eq!(metrics["cache_cold_leader_count"], 1);
    assert_eq!(metrics["cache_waiter_count"], 1);
    assert_eq!(metrics["structural_reuse_ratio_bp"], 4_761);
    assert_eq!(metrics["warm_structural_reuse_ratio_bp"], 9_090);
}

#[test]
fn scenario_metrics_count_packed_attempt_without_outcome_as_usage_unknown() {
    let timeline = json!({
        "events": [{
            "kind": "context.provider_request_packed",
            "payload": {
                "type": "ProviderRequestPacked",
                "request_id": "orphan-after-wire-send",
                "model": "deepseek-v4-flash",
                "model_visible_prompt_bytes": 4096,
                "reusable_prefix_bytes": 4000,
                "cache_cold_leader": false
            }
        }],
        "team_session": {"runtime_run_count": 1}
    });
    let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));

    assert_eq!(metrics["provider_attempt_count"], 1);
    assert_eq!(metrics["provider_attempt_usage_unknown_count"], 1);
    assert_eq!(
        metrics["provider_attempt_cache_dimensions_unknown_count"],
        1
    );
    assert_eq!(metrics["usage_unknown_attempts"], 1);
    assert_eq!(metrics["provider_usage_records"], 0);
}

#[test]
fn known_usage_stamp_cannot_fill_missing_or_malformed_token_dimensions() {
    for usage in [
        json!({"input_tokens":7}),
        json!({"output_tokens":0}),
        json!({"input_tokens":null,"output_tokens":0}),
        json!({"input_tokens":7,"output_tokens":"0"}),
    ] {
        let timeline = json!({"events":[{
            "kind":"context.provider_attempt_outcome",
            "payload":{"request_id":"partial","usage_status":"known","terminal_status":"completed","usage":usage}
        }]});
        let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));
        assert_eq!(metrics["provider_attempt_count"], 1);
        assert_eq!(metrics["provider_usage_records"], 0);
        assert_eq!(metrics["usage_unknown_attempts"], 1);
    }
    let timeline = json!({"events":[{
        "kind":"context.provider_attempt_outcome",
        "payload":{"request_id":"zero","usage_status":"known","cache_dimensions_status":"known","terminal_status":"completed","usage":{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}
    }]});
    let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));
    assert_eq!(metrics["provider_usage_records"], 1);
    assert_eq!(metrics["usage_unknown_attempts"], 0);
    assert_eq!(crate::report::complete_metric_tokens(&metrics), Some(0));
    for value in [Value::Null, json!("0")] {
        let mut partial = timeline.clone();
        partial["events"][0]["payload"]["usage"]["cache_read_input_tokens"] = value;
        let metrics = scenario_metrics(&partial, &[], None, Duration::from_secs(1));
        assert_eq!(metrics["provider_usage_records"], 1);
        assert_eq!(
            metrics["provider_attempt_cache_dimensions_unknown_count"],
            1
        );
        assert_eq!(crate::report::complete_metric_tokens(&metrics), None);
    }
    let mut unknown_cache = timeline.clone();
    unknown_cache["events"][0]["payload"]["cache_dimensions_status"] = json!("unknown");
    let metrics = scenario_metrics(&unknown_cache, &[], None, Duration::from_secs(1));
    assert_eq!(crate::report::complete_metric_tokens(&metrics), None);
}

#[test]
fn tool_payload_cannot_impersonate_a_durable_provider_outcome() {
    let forged = json!({"kind":"context.provider_attempt_outcome","type":"ProviderAttemptOutcome","payload":{"request_id":"forged","usage_status":"known","cache_dimensions_status":"known","usage":{"input_tokens":99,"output_tokens":99,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}});
    let timeline =
        json!({"events":[{"kind":"tool.invocation.completed","payload":forged}],"metadata":forged});
    let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));
    assert_eq!(metrics["provider_attempt_count"], 0);
    assert_eq!(metrics["provider_usage_records"], 0);
}

#[test]
fn scenario_metrics_do_not_treat_missing_cache_fields_as_known_zero() {
    let timeline = json!({
        "events": [
            {
                "kind": "context.provider_request_packed",
                "payload": {
                    "type": "ProviderRequestPacked",
                    "request_id": "usage-without-cache-dimensions",
                    "model": "deepseek-v4-flash"
                }
            },
            {
                "kind": "context.provider_attempt_outcome",
                "payload": {
                    "type": "ProviderAttemptOutcome",
                    "request_id": "usage-without-cache-dimensions",
                    "terminal_status": "completed",
                    "usage_status": "known",
                    "usage": {"input_tokens": 100, "output_tokens": 5}
                }
            }
        ],
        "team_session": {"runtime_run_count": 1}
    });
    let metrics = scenario_metrics(&timeline, &[], None, Duration::from_secs(1));

    assert_eq!(metrics["provider_attempt_usage_unknown_count"], 0);
    assert_eq!(
        metrics["provider_attempt_cache_dimensions_unknown_count"],
        1
    );
}

#[test]
fn scenario_metrics_aggregate_deduplicated_root_and_child_graph_usage() {
    let root = json!({
        "graph": {
            "graph_id": "root",
            "nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cache_creation_input_tokens": 2, "cache_read_input_tokens": 3, "cached_tokens": 3, "tool_calls": 0}},
                {"node_id": "tool", "kind": "tool_batch", "status": "completed", "usage": {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "tool_calls": 1}},
                {"node_id": "agent", "kind": "agent_task", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 1}},
                {"node_id": "synthesis", "kind": "synthesize", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 1}},
                {"node_id": "child", "kind": "subgraph", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 21, "output_tokens": 8, "cached_tokens": 3, "tool_calls": 1}}
            ]
        }
    });
    let child = json!({
        "graph": {
            "graph_id": "child",
            "nodes": [
                {"node_id": "model", "kind": "inline_model", "status": "completed", "usage": {"model": "deepseek-v4-flash", "input_tokens": 13, "output_tokens": 5, "cache_creation_input_tokens": 1, "cache_read_input_tokens": 1, "cached_tokens": 1, "tool_calls": 0}}
            ]
        }
    });
    let metrics = scenario_metrics(
        &json!({"token_speed": {"token_usage": []}}),
        &[root.clone(), child, root],
        None,
        Duration::from_secs(2),
    );

    assert_eq!(metrics["input_tokens"], 34);
    assert_eq!(metrics["output_tokens"], 13);
    assert_eq!(metrics["cache_creation_input_tokens"], 3);
    assert_eq!(metrics["cache_read_input_tokens"], 4);
    assert_eq!(metrics["cache_tokens"], 7);
    assert_eq!(metrics["tool_calls"], 1);
    assert_eq!(metrics["model_rounds"], 2);
    assert_eq!(metrics["token_usage_records"], 3);
    assert_eq!(metrics["effective_models"], json!(["deepseek-v4-flash"]));
}

#[test]
fn scenario_metrics_preserve_agent_first_population_after_terminal_cleanup() {
    let program = agentic_program_projection("verified", true);
    let metrics = scenario_metrics(
        &json!({"token_speed": {"token_usage": []}}),
        &[],
        Some(&program),
        Duration::from_secs(1),
    );

    assert_eq!(metrics["agent_count"], 3);
    assert_eq!(metrics["team_count"], 2);
    assert_eq!(metrics["accepted_agentic_task_count"], 2);
}

#[test]
fn live_metric_summary_uses_observed_scenario_values() {
    let metrics = aggregate_scenario_metrics(&[
        json!({"metrics": {
            "input_tokens": 10,
            "output_tokens": 2,
            "cache_tokens": 3,
            "total_tokens": 15,
            "model_rounds": 1,
            "tool_calls": 0,
            "agent_count": 0,
            "team_count": 0,
            "wall_ms": 100,
            "first_token_latency_ms": 40
        }}),
        json!({"metrics": {
            "input_tokens": 20,
            "output_tokens": 5,
            "cache_tokens": 0,
            "total_tokens": 25,
            "model_rounds": 2,
            "tool_calls": 3,
            "agent_count": 4,
            "team_count": 1,
            "wall_ms": 300,
            "first_token_latency_ms": 80
        }}),
    ]);

    assert_eq!(metrics["total_tokens"], 40);
    assert_eq!(metrics["model_rounds"], 3);
    assert_eq!(metrics["tool_calls"], 3);
    assert_eq!(metrics["max_agent_count"], 4);
    assert_eq!(metrics["max_team_count"], 1);
    assert_eq!(metrics["wall_ms"]["p95"], 300);
    assert_eq!(metrics["first_token_latency_ms"]["min"], 40);
}

#[test]
fn collaboration_comparison_uses_public_child_team_evidence_not_root_metrics() {
    let comparison = collaboration_comparison(&[
        json!({
            "scenario_id": "live_single_architecture_baseline",
            "metrics": {"wall_ms": 100},
            "acceptance": {"quality": {"score": 9}}
        }),
        json!({
            "scenario_id": "live_team_projection",
            "status": "passed",
            // Root graph only: the actual Team Agents run in a child
            // graph and are represented by the public acceptance check.
            "metrics": {"agent_count": 0, "wall_ms": 200},
            "acceptance": {
                "quality": {"score": 9},
                "checks": [{
                    "name": "accepted_agent_first_teams",
                    "passed": true,
                    "engaged_agents": 3,
                    "accepted_agents": 3,
                    "teams": 3,
                    "completed_teams": 3
                }, {
                    "name": "accepted_cross_team_dependencies",
                    "passed": true,
                    "observed": 2
                }]
            }
        }),
    ]);

    assert_eq!(comparison["status"], "passed");
    assert_eq!(comparison["team_capability"]["passed"], true);
}

#[test]
fn live_timeout_is_complexity_aware_and_not_default_capped() {
    let direct = LiveScenarioTimeout::direct().with_cap(None);
    let team = LiveScenarioTimeout::team().with_cap(None);
    assert!(team.nominal_wait > direct.nominal_wait);
    assert_eq!(team.absolute_wait, None);
    assert!(direct.absolute_wait.is_some());

    let capped = team.with_cap(Some(Duration::from_secs(600)));
    assert_eq!(capped.nominal_wait, Duration::from_secs(600));
    assert_eq!(capped.absolute_wait, Some(Duration::from_secs(600)));
    assert_eq!(capped.inactivity_wait, Duration::from_secs(600));

    // An accidentally tiny operator cap cannot make the team scenario
    // fail before it has had one normal progress window.
    assert_eq!(
        team.with_cap(Some(Duration::from_secs(30))).absolute_wait,
        team.absolute_wait
    );

    let sixteen = LiveScenarioTimeout::large_scale(16);
    let twenty_four = LiveScenarioTimeout::large_scale(24);
    assert_eq!(sixteen.absolute_wait, None);
    assert_eq!(twenty_four.absolute_wait, None);
    assert!(twenty_four.nominal_wait > sixteen.nominal_wait);
    assert!(!twenty_four.should_abort_for_absolute_wait(Duration::from_secs(86_400)));
    let now = Instant::now();
    let old = now - Duration::from_secs(7_200);
    assert!(
        team.observation_deadline(old, old, Duration::ZERO, 1) > now + Duration::from_secs(500)
    );
    assert!(team.observation_deadline(old, old, Duration::from_secs(7_200), 1) <= Instant::now());
    assert!(team.observation_deadline(old, old, Duration::ZERO, 0) <= Instant::now());
    assert!(direct.observation_deadline(old, now, Duration::ZERO, 1) <= Instant::now());

    let explicitly_capped = twenty_four.with_cap(Some(Duration::from_secs(7_200)));
    assert_eq!(
        explicitly_capped.absolute_wait,
        Some(Duration::from_secs(7_200))
    );
    assert!(explicitly_capped.should_abort_for_absolute_wait(Duration::from_secs(7_200)));
}

#[test]
fn first_provider_response_uses_the_full_complexity_deadline() {
    let team = LiveScenarioTimeout::team();
    assert!(!team.should_abort_for_no_progress(Duration::from_secs(1_799), 0));
    assert!(team.should_abort_for_no_progress(Duration::from_secs(1_800), 0));
    assert!(!team.should_abort_for_no_progress(Duration::from_secs(1_800), 1));
    assert!(
        !team.should_abort_for_inactivity(Duration::from_secs(181), Duration::from_secs(181), 0,),
        "a submitted user message is not provider progress"
    );
    assert!(team.should_abort_for_inactivity(
        Duration::from_secs(361),
        Duration::from_secs(601),
        1,
    ));
}

fn failed_scenario_spec() -> LiveScenarioSpec {
    LiveScenarioSpec {
        id: "live_one_team_execution",
        prompt: "provider evidence fixture",
        acceptance: LiveAcceptance::AutonomousCollaboration {
            minimum_teams: 1,
            minimum_agents: 3,
            minimum_tasks: 3,
            minimum_reviews: 3,
            minimum_cross_team_edges: 0,
            minimum_topics: 0,
            output_path: "out.md",
        },
        timeout: LiveScenarioTimeout::team(),
    }
}

fn provider_attempt_diagnostics(attempts: usize) -> Value {
    let mut events = Vec::new();
    for index in 0..attempts {
        let request_id = format!("provider-request-{index}");
        events.push(json!({
            "kind": "context.provider_request_packed",
            "payload": {"request_id": request_id, "model": "step-3.7-flash"}
        }));
        events.push(json!({
            "kind": "context.provider_attempt_outcome",
            "payload": {
                "request_id": request_id,
                "usage_status": "known",
                "cache_dimensions_status": "known",
                "terminal_status": "completed",
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 10,
                    "cache_read_input_tokens": 0,
                    "cache_creation_input_tokens": 0
                }
            }
        }));
    }
    json!({"timeline": {"events": events}})
}

/// A failed scenario spent its provider budget before it failed, so it must report
/// the same metrics a passing scenario reports. Omitting them is what made the
/// provider-round gate read zero exactly when it was supposed to check the claim:
/// both of its sources sum `metrics.model_rounds` per scenario.
#[test]
fn a_failed_scenario_still_reports_its_provider_rounds() {
    let attempts = 3;
    let diagnostics = provider_attempt_diagnostics(attempts);
    let scenario = failed_scenario_with_session_and_execution(
        failed_scenario_spec(),
        Instant::now(),
        Vec::new(),
        "session-1".to_string(),
        Some("execution-1".to_string()),
        "scenario aborted for inactivity".to_string(),
        diagnostics,
    );

    let metrics = scenario
        .get("metrics")
        .expect("a failed scenario has metrics");
    assert_eq!(
        metrics.get("model_rounds").and_then(Value::as_u64),
        Some(attempts as u64),
        "one model round per completed provider attempt, counted for a failed scenario too"
    );
    assert_eq!(
        metrics
            .get("provider_attempt_count")
            .and_then(Value::as_u64),
        Some(attempts as u64),
    );
    assert_eq!(metrics.get("tool_calls").and_then(Value::as_u64), Some(0));
    assert_eq!(
        scenario.get("status").and_then(Value::as_str),
        Some("failed")
    );
}

/// A transient tool failure inside a retried agent is not a failed agent graph.
///
/// d6v10 reached a terminal state - `error = None`, six durable assistant messages,
/// zero of 140 tool calls failed, zero ERROR lines in the gateway log, seven of seven
/// `agent.terminal` records completed - and `physical_agent_graphs_succeeded` still
/// failed with `failed_activities: 1`. The one `partial_failure` outcome in that run's
/// record says what the count was really seeing: "1 tool calls failed before terminal
/// synthesis". Counting it turned a finished run into a failed one.
#[test]
fn a_recovered_agent_instance_is_not_a_failed_agent_graph() {
    let projection = json!({"activities": [
        {"activity_id": "a1", "agent_instance_id": "inst-1", "status": "failed"},
        {"activity_id": "a2", "agent_instance_id": "inst-1", "status": "completed"},
        {"activity_id": "a3", "agent_instance_id": "inst-2", "status": "failed"},
    ]});
    assert_eq!(
        physical_failed_agent_activity_count(std::slice::from_ref(&projection)),
        1,
        "the instance that later completed is recovered; the one that never did is failed"
    );
}

/// An agent still holding a non-terminal status must be named, not just counted.
///
/// `physical_agent_waits_resolved` used to publish a bare boolean, so a failure could
/// not distinguish "nothing was pending" from "one agent never settled".
#[test]
fn an_unresolved_agent_activity_is_named_in_the_evidence() {
    let projection = json!({"activities": [
        {"activity_id": "a1", "agent_instance_id": "inst-1", "status": "completed"},
        {"activity_id": "a2", "agent_instance_id": "inst-2", "status": "running",
         "execution_id": "exec-2", "public_summary": "still working"},
    ]});
    assert!(no_active_agent_waits(std::slice::from_ref(&projection)) == false);
    let unresolved = unresolved_agent_activities(std::slice::from_ref(&projection));
    assert_eq!(unresolved.len(), 1);
    assert_eq!(unresolved[0]["activity_id"], "a2");
    assert_eq!(unresolved[0]["execution_id"], "exec-2");
    assert_eq!(unresolved[0]["status"], "running");
}

/// A terminal run whose Program projection is not visible yet must still be read
/// once it appears, rather than reported as "the Objective was not verified".
/// The distinction between "did not achieve it" and "could not read it" is the
/// whole point of the retry.
#[test]
fn terminal_verdict_survives_an_empty_program_extraction() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = extract_root_program_with_retry(|| {
        let attempts = std::sync::Arc::clone(&attempts);
        let call = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if call < 2 {
            Ok::<_, String>(None)
        } else {
            Ok(Some(runtime::AgenticProgramProjection::empty(
                "program-1",
                "objective-1",
            )))
        }
    })
    .expect("extraction");
    assert!(
        seen.is_some(),
        "the projection must be read once it appears"
    );
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "two retries then a successful read"
    );
}

/// A projection that never appears must not be retried forever: the harness has
/// to keep its own wall clock. After the bound it gives up and reports `None`,
/// which is exactly the old behaviour.
#[test]
fn a_missing_program_extraction_is_bounded() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = extract_root_program_with_retry(|| {
        let attempts = std::sync::Arc::clone(&attempts);
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok::<_, String>(None)
    })
    .expect("extraction");
    assert!(seen.is_none());
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        PROGRAM_EXTRACTION_RETRIES as usize + 1,
        "the retry budget is a hard bound"
    );
}

/// A hard error is a real fault, not a timing gap, so it is returned on the
/// first attempt and never retried.
#[test]
fn a_failed_program_extraction_is_not_retried() {
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = extract_root_program_with_retry(|| {
        let attempts = std::sync::Arc::clone(&attempts);
        attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err::<Option<runtime::AgenticProgramProjection>, _>("gateway unreachable".to_string())
    });
    assert_eq!(result.unwrap_err(), "gateway unreachable");
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a fault must not be retried"
    );
}

/// The three-piece record names which piece is missing, so a reader never has to
/// reconstruct it from the aggregate. It records; it does not decide, because the
/// three checks above already do.
#[test]
fn terminal_three_piece_complete_names_the_missing_piece() {
    let timeline = successful_root_outcome_timeline(json!({}));
    let acceptance = LiveAcceptanceResult {
        passed: false,
        // A verdict with no user-visible delivery: exactly the state one live run
        // reached, where the business was finished and nothing was delivered.
        checks: vec![
            json!({"name": "agentic_program_verified", "passed": true}),
            json!({"name": "durable_response", "passed": false}),
        ],
        quality: None,
    };
    let finalized = finalize_live_acceptance(acceptance, &timeline, "root");
    let record = finalized
        .checks
        .iter()
        .find(|check| check["name"] == "terminal_three_piece_complete")
        .expect("three-piece record");
    assert_eq!(record["passed"], false);
    assert_eq!(record["pieces"]["goal_verified"], true);
    assert_eq!(record["pieces"]["user_visible_delivery"], false);
    assert_eq!(record["missing"][0], "user_visible_delivery");
}

/// With all three pieces present the record passes and names nothing missing.
#[test]
fn terminal_three_piece_complete_is_silent_when_nothing_is_missing() {
    let timeline = successful_root_outcome_timeline(json!({}));
    let acceptance = LiveAcceptanceResult {
        passed: true,
        checks: vec![
            json!({"name": "agentic_program_verified", "passed": true}),
            json!({"name": "durable_response", "passed": true}),
        ],
        quality: None,
    };
    let finalized = finalize_live_acceptance(acceptance, &timeline, "root");
    let record = finalized
        .checks
        .iter()
        .find(|check| check["name"] == "terminal_three_piece_complete")
        .expect("three-piece record");
    assert_eq!(record["passed"], true);
    assert_eq!(record["missing"].as_array().map(Vec::len), Some(0));
}

/// Build a minimal Program with `task_count` accepted Tasks and, optionally, one
/// synthesis artifact written by `synthesis_author`.
fn program_with_tasks(
    task_count: usize,
    synthesis: Option<(&str, &str)>,
) -> runtime::AgenticProgramProjection {
    let mut tasks = serde_json::Map::new();
    let mut artifacts = serde_json::Map::new();
    for index in 0..task_count {
        let task_id = format!("task-{index}");
        let artifact_ref = format!("artifact:report-{index}");
        let author = format!("agent-{index}");
        tasks.insert(
            task_id.clone(),
            json!({
                "task_id": task_id,
                "team_id": "team-1",
                "title": format!("report {index}"),
                "objective": "verify one fact",
                "acceptance": "one verifiable fact",
                "required_capabilities": ["read"],
                "depends_on": [],
                "status": "accepted",
                "claimant": author,
                "claim_generation": 1,
                "claim_execution_id": format!("execution-{index}"),
                "claimed_at_ms": 1,
                "lease_expires_at_ms": 2,
                "artifact_refs": [artifact_ref],
                "evidence_refs": ["evidence:read"],
                "unresolved": [],
                "review_reason": "accepted",
                "reviewed_by": "agent-review",
                "failed_attempts": 0,
                "review_generation": 1,
                "failed_review_attempts": 0,
                "last_failure": null,
                "replacement_task_refs": [],
                "supersede_evidence_refs": [],
                "superseded_reason": null,
                "superseded_by": null
            }),
        );
        artifacts.insert(
            artifact_ref,
            json!({
                "artifact_ref": format!("artifact:report-{index}"),
                "content_ref": format!("content:{index}"),
                "kind": "report",
                "title": format!("report {index}"),
                "relates_to": [task_id],
                "committed_by": format!("agent-{index}")
            }),
        );
    }
    let mut extra_task = None;
    if let Some((artifact_ref, author)) = synthesis {
        // The synthesis hangs off its own accepted Task and is written by
        // `author`, who is not the author of any Task it covers.
        extra_task = Some(format!("task-synthesis"));
        tasks.insert(
            "task-synthesis".to_string(),
            json!({
                "task_id": "task-synthesis",
                "team_id": "team-1",
                "title": "synthesis",
                "objective": "synthesize the reports",
                "acceptance": "one synthesis",
                "required_capabilities": ["read"],
                "depends_on": [],
                "status": "accepted",
                "claimant": author,
                "claim_generation": 1,
                "claim_execution_id": "execution-synthesis",
                "claimed_at_ms": 1,
                "lease_expires_at_ms": 2,
                "artifact_refs": [artifact_ref],
                "evidence_refs": ["evidence:read"],
                "unresolved": [],
                "review_reason": "accepted",
                "reviewed_by": "agent-review",
                "failed_attempts": 0,
                "review_generation": 1,
                "failed_review_attempts": 0,
                "last_failure": null,
                "replacement_task_refs": [],
                "supersede_evidence_refs": [],
                "superseded_reason": null,
                "superseded_by": null
            }),
        );
        // It relates to every report it synthesizes, which is how a synthesis is
        // told apart from a fourth report.
        let relates_to: Vec<String> = (0..task_count)
            .map(|index| format!("task-{index}"))
            .chain(std::iter::once("task-synthesis".to_string()))
            .collect();
        artifacts.insert(
            artifact_ref.to_string(),
            json!({
                "artifact_ref": artifact_ref,
                "content_ref": "content:synthesis",
                "kind": "report",
                "title": "synthesis",
                "relates_to": relates_to,
                "committed_by": author
            }),
        );
    }
    serde_json::from_value(json!({
        "program_id": "program:test",
        "objective_id": "objective:test",
        "session_id": "session-test",
        "turn_id": "turn-test",
        "root_execution_id": "root",
        "required_team_count": 1,
        "objective_summary": "synthesize the reports",
        "model_lease": "model:test",
        "permission_ceiling": "read-only",
        "resource_scopes": [],
        "revision": 9,
        "status": "verified",
        "teams": {},
        "agents": {},
        "tasks": Value::Object(tasks),
        "topics": {},
        "artifacts": Value::Object(artifacts),
        "final_artifact_ref": extra_task.as_ref().map(|_| "artifact:synthesis"),
        "completion_request": null,
        "objective_verdict": {"goal_id":"objective:test","goal_revision":1,"terminal_fence":"fence","authority_revision":9,"kind":"satisfied"},
        "unresolved": []
    }))
    .expect("program fixture")
}

/// A synthesis that draws on two Tasks, hangs off an accepted Task, and is
/// written by someone who authored neither, satisfies the operator's "final
/// artifact synthesizes the facts" requirement.
#[test]
fn a_synthesized_artifact_covering_two_tasks_passes() {
    let program = program_with_tasks(2, Some(("artifact:synthesis", "agent-synth")));
    let (present, unsynthesized) = synthesized_final_artifact_findings(Some(&program));
    assert!(present, "a real synthesis must pass: {unsynthesized:?}");
}

/// Three per-Task reports and no synthesis is exactly the state one live run
/// reached, and it used to pass on a non-empty answer alone.
#[test]
fn per_task_reports_without_a_synthesis_fail() {
    let program = program_with_tasks(3, None);
    let (present, unsynthesized) = synthesized_final_artifact_findings(Some(&program));
    assert!(!present, "per-Task reports are not a synthesis");
    assert_eq!(unsynthesized.len(), 3, "{unsynthesized:?}");
}

/// A "synthesis" written by the author of a Task it covers is a fourth report,
/// not a synthesis, so it does not satisfy the requirement.
#[test]
fn a_synthesis_by_one_of_the_authors_is_refused() {
    let program = program_with_tasks(2, Some(("artifact:synthesis", "agent-0")));
    let (present, _) = synthesized_final_artifact_findings(Some(&program));
    assert!(!present, "an author may not synthesize their own work");
}

/// One Task has nothing to synthesize, so there is no obligation.
#[test]
fn a_single_task_scenario_needs_no_synthesis() {
    let program = program_with_tasks(1, None);
    let (present, unsynthesized) = synthesized_final_artifact_findings(Some(&program));
    assert!(present, "{unsynthesized:?}");
}

#[test]
fn u5_inherited_host_fd_contract_probe() {
    // The shell double executes this exact reader through the original exec
    // chain. No API/model/PG is contacted. Without that owned host it must deny.
    let expected = std::env::var("COWD_U5_TEST_HOST_EXPECT").unwrap_or_else(|_| "deny".into());
    let mutation = std::env::var("COWD_U5_TEST_HOST_MUTATION").unwrap_or_default();
    let result = if mutation.is_empty() {
        U5Host::inherited()
    } else {
        U5Host::inherited_observed(|| {
            use std::os::unix::fs::PermissionsExt;
            let root = std::path::PathBuf::from(std::env::var("XDG_RUNTIME_DIR").unwrap());
            let path = root.join("u5-host.json");
            let original = std::fs::read(&path).unwrap();
            match mutation.as_str() {
                "content" | "size" => {
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    let mut bytes = original;
                    if mutation == "size" {
                        bytes.push(b' ');
                    } else {
                        let n = bytes.len() - 1;
                        bytes[n] = b' ';
                    }
                    std::fs::write(&path, bytes).unwrap();
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
                        .unwrap();
                }
                "inode" => {
                    std::fs::rename(&path, root.join("old-host.json")).unwrap();
                    std::fs::write(&path, original).unwrap();
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
                        .unwrap();
                }
                "root" => {
                    let old = root.with_extension("retained");
                    std::fs::rename(&root, &old).unwrap();
                    std::fs::create_dir(&root).unwrap();
                    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                        .unwrap();
                    std::fs::write(&path, original).unwrap();
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400))
                        .unwrap();
                }
                _ => panic!("unknown test-only mutation"),
            }
        })
    };
    if let Ok(reason) = std::env::var("COWD_U5_TEST_EXPECT_REASON") {
        assert!(
            result.as_ref().err().is_some_and(|e| e.contains(&reason)),
            "target reader rejection must be reached"
        );
    }
    match (expected.as_str(), result) {
        ("allow", Ok(host)) => {
            assert_eq!(host.owner().principal_actor, "");
            assert!(u5_valid_owner(&host.owner(), &host.base_url));
        }
        ("deny", Err(_)) => {}
        (_, result) => panic!(
            "host contract unexpected disposition: {}",
            if result.is_ok() { "allowed" } else { "denied" }
        ),
    }
}

fn u7_literal_fixture() -> U7MemoryFixture {
    U7MemoryFixture {
        scope: "global",
        lookup: "independent-project".into(),
        warehouse: "WH-literal-527".into(),
        passphrase: "OUT-literal-893".into(),
        content: "independent memory body 527/893".into(),
    }
}

fn u7_literal_history() -> Value {
    json!({"session_id":"s-original", "has_more":false, "envelopes":[{
    "session_id":"s-original", "type":"ContextEnvelope", "envelope":{
        "identity":{"session_id":"s-original"}, "intent":"query independent-project",
        "selected":[{"source":"Memory","content":"independent memory body 527/893"}]
    }}]})
}

fn u7_literal_case(fixture: &U7MemoryFixture) -> Value {
    let id = "00000000-0000-0000-0000-000000000001";
    let preview = serde_json::to_string_pretty(
        &json!({"selected":[{"memory_id":id,"preview":fixture.content}]}),
    )
    .unwrap();
    json!({"session_id":"s-original","execution_id":"graph-original","turn_id":"turn-original","terminal_id":"message-original",
    "business_observation":{"event_status":"succeeded","session_id":"s-original","execution_id":"graph-original","turn_id":"turn-original"},
    "delivery_observation":{"status":"confirmed","fresh_context_confirmed":true,"session_id":"s-original","execution_id":"graph-original","turn_id":"turn-original","message_id":"message-original","ack":{"session_id":"s-original","message_id":"message-original"}},
    "trace":[{"kind":"u7_preregistered_persistent_memory","evidence":{"memory_id":id,"input":fixture,"fixture_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec_pretty(fixture).unwrap())),"readback":{"entries":[{"id":id,"scope":"Global","content":fixture.content}]}}},
    {"channel":"timeline","response":{"events":[{"kind":"tool.invocation.completed","stream_id":"session:s-original","payload":{
        "tool_name":"context_retrieve","status":"completed","is_error":false,"session_id":"s-original","tool_call_id":"call-original",
        "input_preview":json!({"source":"memory","query":"independent-project"}).to_string(),"output_preview":preview,
        "_runtime_activity_binding":{"session_id":"s-original","turn_id":"turn-original","root_execution_id":"graph-original","tool_call_id":"call-original","tool_contract_id":"context_retrieve"}
    }}]}}]})
}

#[test]
fn u7_memory_requires_bound_retrieval_and_answer_not_one_context_implementation() {
    let fixture = u7_literal_fixture();
    let answer = "WH-literal-527 OUT-literal-893";
    let history = u7_literal_history();
    let case = u7_literal_case(&fixture);
    let checks = u7_memory_checks(&fixture, "s-original", answer, &history, &case);
    assert_eq!(checks["business_passed"], true);
    assert_eq!(checks["persisted_context_memory_selected"], true);
    for defect in [
        "foreign-session",
        "foreign-envelope",
        "wrong-source",
        "omitted",
        "partial-page",
        "wrong-body",
        "wrong-intent",
        "unavailable",
    ] {
        let mut changed = history.clone();
        match defect {
            "foreign-session" => changed["session_id"] = json!("foreign"),
            "foreign-envelope" => {
                changed["envelopes"][0]["envelope"]["identity"]["session_id"] = json!("foreign")
            }
            "wrong-source" => {
                changed["envelopes"][0]["envelope"]["selected"][0]["source"] = json!("user_request")
            }
            "omitted" => changed["envelopes"][0]["envelope"]["selected"] = json!([]),
            "partial-page" => changed["has_more"] = json!(true),
            "wrong-body" => {
                changed["envelopes"][0]["envelope"]["selected"][0]["content"] =
                    json!("different memory")
            }
            "wrong-intent" => {
                changed["envelopes"][0]["envelope"]["intent"] = json!("other project")
            }
            "unavailable" => changed = Value::Null,
            _ => unreachable!(),
        }
        let checks = u7_memory_checks(&fixture, "s-original", answer, &changed, &case);
        assert_eq!(
            checks["business_passed"], true,
            "{defect} does not invalidate separately proven retrieval"
        );
        assert_eq!(
            checks["persisted_context_memory_selected"], false,
            "{defect} remains unproved"
        );
        assert_eq!(checks["full_result_body_verified"], false);
    }
    for wrong in [
        "",
        "WH-literal-527",
        "OUT-literal-893",
        "WH-other OUT-other",
    ] {
        assert_eq!(
            u7_memory_checks(&fixture, "s-original", wrong, &history, &case)["business_passed"],
            false
        );
    }
}

#[test]
fn u7_correct_answer_without_original_seed_retrieval_or_ack_does_not_pass() {
    let fixture = u7_literal_fixture();
    let original = u7_literal_case(&fixture);
    for defect in [
        "no-seed",
        "wrong-seed-body",
        "wrong-seed-id",
        "wrong-fixture",
        "no-retrieval",
        "foreign-session",
        "foreign-graph",
        "foreign-turn",
        "foreign-call",
        "wrong-source",
        "failed-tool",
        "wrong-preview",
        "failed-root",
        "no-ack",
        "foreign-ack",
    ] {
        let mut case = original.clone();
        match defect {
            "no-seed" => case["trace"][0]["kind"] = json!("other"),
            "wrong-seed-body" => {
                case["trace"][0]["evidence"]["readback"]["entries"][0]["content"] =
                    json!("foreign body")
            }
            "wrong-seed-id" => case["trace"][0]["evidence"]["memory_id"] = json!("foreign"),
            "wrong-fixture" => {
                case["trace"][0]["evidence"]["input"]["warehouse"] = json!("foreign")
            }
            "no-retrieval" => case["trace"][1]["response"]["events"] = json!([]),
            "foreign-session" => {
                case["trace"][1]["response"]["events"][0]["payload"]["_runtime_activity_binding"]
                    ["session_id"] = json!("foreign")
            }
            "foreign-graph" => {
                case["trace"][1]["response"]["events"][0]["payload"]["_runtime_activity_binding"]
                    ["root_execution_id"] = json!("foreign")
            }
            "foreign-turn" => {
                case["trace"][1]["response"]["events"][0]["payload"]["_runtime_activity_binding"]
                    ["turn_id"] = json!("foreign")
            }
            "foreign-call" => {
                case["trace"][1]["response"]["events"][0]["payload"]["_runtime_activity_binding"]
                    ["tool_call_id"] = json!("foreign")
            }
            "wrong-source" => {
                case["trace"][1]["response"]["events"][0]["payload"]["input_preview"] =
                    json!(r#"{"source":"session_history","query":"independent-project"}"#)
            }
            "failed-tool" => {
                case["trace"][1]["response"]["events"][0]["payload"]["is_error"] = json!(true)
            }
            "wrong-preview" => {
                case["trace"][1]["response"]["events"][0]["payload"]["output_preview"] =
                    json!("unbound prose with independent memory body 527/893")
            }
            "failed-root" => case["business_observation"]["event_status"] = json!("failed"),
            "no-ack" => case["delivery_observation"]["status"] = json!("unconfirmed"),
            "foreign-ack" => case["delivery_observation"]["ack"]["session_id"] = json!("foreign"),
            _ => unreachable!(),
        }
        let result = u7_memory_checks(
            &fixture,
            "s-original",
            "WH-literal-527 OUT-literal-893",
            &u7_literal_history(),
            &case,
        );
        assert_eq!(
            result["business_passed"], false,
            "{defect}; an automatic context is not a substitute for this hard business proof"
        );
    }
}

#[test]
#[ignore = "offline original U7 case and independently frozen fixture must be provided"]
fn u7_offline_original_case_keeps_business_and_context_observations_separate() {
    let original =
        std::fs::read(std::env::var("COWD_U7_ORIGINAL_CASE").expect("original case path")).unwrap();
    let expected_sha =
        std::env::var("COWD_U7_ORIGINAL_CASE_SHA256").expect("frozen original digest");
    assert_eq!(format!("{:x}", Sha256::digest(&original)), expected_sha);
    let case: Value = serde_json::from_slice(&original).unwrap();
    let input: Value = serde_json::from_slice(
        &std::fs::read(
            std::env::var("COWD_U7_ORIGINAL_FIXTURE")
                .expect("original independently frozen fixture"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(input["scope"], "global");
    let fixture = U7MemoryFixture {
        scope: "global",
        lookup: input["lookup"].as_str().unwrap().into(),
        warehouse: input["warehouse"].as_str().unwrap().into(),
        passphrase: input["passphrase"].as_str().unwrap().into(),
        content: input["content"].as_str().unwrap().into(),
    };
    let result = u7_memory_checks(
        &fixture,
        case["session_id"].as_str().unwrap(),
        case["u7_terminal_answer"].as_str().unwrap(),
        &case["u7_memory_evidence"]["history"],
        &case,
    );
    assert_eq!(result["business_passed"], true);
    assert_eq!(result["persisted_context_memory_selected"], false);
    assert_eq!(result["automatic_context_selection_required"], false);
    assert_eq!(
        case["status"], "failed",
        "immutable historical report stays failed"
    );
    assert_eq!(
        case["u7_memory_evidence"]["passed"], false,
        "the old evaluator result is historical"
    );
    assert_eq!(format!("{:x}", Sha256::digest(&original)), expected_sha);
}

#[test]
fn u7_preparation_readback_requires_same_original_id_and_full_body() {
    let row = json!({"entries":[{"id":"original-id","content":"literal-body","scope":"Global"}]});
    assert!(u7_persisted_record(&row, "original-id", "literal-body"));
    let mut wrong_scope = row.clone();
    wrong_scope["entries"][0]["scope"] = json!({"Session":"api-memory-create"});
    assert!(!u7_persisted_record(
        &wrong_scope,
        "original-id",
        "literal-body"
    ));
    wrong_scope["entries"][0]
        .as_object_mut()
        .unwrap()
        .remove("scope");
    assert!(!u7_persisted_record(
        &wrong_scope,
        "original-id",
        "literal-body"
    ));

    assert!(!u7_persisted_record(&row, "foreign-id", "literal-body"));
    assert!(!u7_persisted_record(&row, "original-id", "literal"));
    assert!(!u7_persisted_record(
        &json!({"entries":[]}),
        "original-id",
        "literal-body"
    ));
    assert_eq!(
        u7_memory_checks(
            &u7_literal_fixture(),
            "s-original",
            "WH-literal-527 OUT-literal-893",
            &json!({}),
            &Value::Null,
        )["passed"],
        false,
        "CRUD/readback cannot stand in for real context consumption"
    );
}

#[test]
fn u7_exact_selector_preserves_other_corpora_and_does_not_leak_expected() {
    let spec = u7_memory_spec();
    assert_eq!(spec.id, "live_u7_persistent_memory_recall");
    assert!(spec.acceptance.read_obligations().is_empty());
    let registry = BTreeSet::from([spec.id.to_string()]);
    let good = BTreeSet::from(["live_u7_persistent_memory_recall".to_string()]);
    let bad = BTreeSet::from(["live_u7_persistent_memory_recall_foreign".to_string()]);
    assert!(live_scenario_selection_errors(Some(&good), &registry).is_empty());
    assert_eq!(
        live_scenario_selection_errors(Some(&bad), &registry).len(),
        1
    );
    let fixture = U7MemoryFixture::fresh();
    assert!(!spec.prompt.contains(&fixture.warehouse));
    assert!(!spec.prompt.contains(&fixture.passphrase));
    let prompt = controlled_live_prompt(spec.id, spec.prompt.to_string(), None);
    assert!(!prompt.contains("read:."));
    assert!(
        controlled_live_prompt("live_tool_evidence", "original".into(), None)
            .contains("read:Cargo.toml")
    );
}
#[test]
fn u7_lifecycle_oracles_keep_correction_absence_and_original_terminal_hard() {
    let prompt = controlled_live_prompt(U7_MEMORY_LIFECYCLE, U7_MEMORY_PROMPT.into(), None);
    let control: Value = serde_json::from_str(prompt.lines().next().unwrap().strip_prefix("COWD_EVAL_CONTROL ").unwrap()).unwrap();
    assert_eq!(control["resource_scopes"], json!(["provider", "provider_account", "provider_token_pool"]), "lifecycle cannot inherit source-file/network/Team permissions");
    let old = u7_literal_fixture();
    let new = U7MemoryFixture {
        warehouse: "WH-new-991".into(),
        passphrase: "OUT-new-337".into(),
        content: "new body".into(),
        ..old.clone()
    };
    assert!(u7_contains_any_secret("仓库代号WH-literal-527", &old, &new));
    assert!(u7_contains_any_secret("out-new-337", &old, &new));
    assert!(!u7_contains_any_secret("该项目记忆不存在。", &old, &new));
    assert!(u7_explicit_absence("未找到相关项目记忆，两个值均缺失。"));
    assert!(!u7_explicit_absence("已有项目记录，稍后提供口令。"));
    let absent = json!({"enabled":true,"layer":"L3","include_archived":false,"entries":[]});
    assert!(u7_active_memory_absent(&absent, "original-id"));
    for defect in [
        "disabled",
        "unknown-page",
        "wrong-layer",
        "archived-view",
        "resurrected-id",
    ] {
        let mut page = absent.clone();
        match defect {
            "disabled" => page["enabled"] = json!(false),
            "unknown-page" => {
                page.as_object_mut().unwrap().remove("entries");
            }
            "wrong-layer" => page["layer"] = json!("L2"),
            "archived-view" => page["include_archived"] = json!(true),
            "resurrected-id" => {
                page["entries"] = json!([{"id":"original-id","content":"old body"}])
            }
            _ => unreachable!(),
        }
        assert!(!u7_active_memory_absent(&page, "original-id"), "{defect}");
    }
    let mut case = u7_literal_case(&old);
    case["session_actor_cleanup"] = json!({"status":"passed"});
    case["acceptance"] = json!({"checks":[{"name":"current_public_terminal_delivery","passed":true},{"name":"terminal_three_piece_complete","passed":false,"missing":["goal_verified"]}]});
    assert!(u7_phase_delivery_passed(&case));
    let mut denied = case.clone();
    denied["acceptance"]["checks"][0]["passed"] = json!(false);
    assert!(!u7_phase_delivery_passed(&denied));
    denied = case.clone();
    denied["acceptance"]["checks"][1]["missing"] = json!(["user_visible_delivery"]);
    assert!(!u7_phase_delivery_passed(&denied));
    denied = case.clone();
    denied["session_actor_cleanup"]["status"] = json!("failed");
    assert!(!u7_phase_delivery_passed(&denied));
}

#[test]
fn u7_retained_restart_requires_real_distinct_epoch_and_original_f2() {
    let request = json!({"memory_id":"original-id","config_sha256":"7".repeat(64)});
    let receipt = json!({"kind":"u7_owned_restart_returned","memory_id":"original-id","request_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&request).unwrap())),"old_pid":101,"new_pid":202,"same_config_sha256":"7".repeat(64),"first_shutdown_validation":{"status":"passed","identity":{"pid":101}},"ready":true});
    assert!(u7_restart_matches(&receipt, &request));
    for defect in [
        "same-pid",
        "missing-pid",
        "foreign-memory",
        "foreign-config",
        "foreign-request",
        "foreign-first-owner",
        "failed-f2",
        "not-ready",
    ] {
        let mut changed = receipt.clone();
        match defect {
            "same-pid" => changed["new_pid"] = json!(101),
            "missing-pid" => changed["new_pid"] = Value::Null,
            "foreign-memory" => changed["memory_id"] = json!("foreign"),
            "foreign-config" => changed["same_config_sha256"] = json!("8".repeat(64)),
            "foreign-request" => changed["request_sha256"] = json!("8".repeat(64)),
            "foreign-first-owner" => {
                changed["first_shutdown_validation"]["identity"]["pid"] = json!(303)
            }
            "failed-f2" => changed["first_shutdown_validation"]["status"] = json!("failed"),
            "not-ready" => changed["ready"] = json!(false),
            _ => unreachable!(),
        }
        assert!(!u7_restart_matches(&changed, &request), "{defect}");
    }
}

#[test]
#[ignore = "local typed CRUD double requires an owner-supplied private U7 fixture path; zero model/PG"]
fn u7_correction_producer_patches_the_original_id_before_any_model_request() {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = std::thread::spawn(move || {
        let id = "00000000-0000-0000-0000-000000000007";
        let mut stored = Value::Null;
        let mut requests = Vec::new();
        for step in 0..4 {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let (header_end, length) = loop {
                let mut buffer = [0u8; 4096];
                let n = stream.read(&mut buffer).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                assert!(bytes.len() < 32768);
                if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..end]);
                    let length = head
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                .map(|(_, v)| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_end + length {
                let mut buffer = [0u8; 4096];
                let n = stream.read(&mut buffer).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
            }
            let first = String::from_utf8_lossy(&bytes[..header_end])
                .lines()
                .next()
                .unwrap()
                .to_owned();
            requests.push(first.clone());
            let expected = [
                "POST /api/memory/L3 HTTP/1.1",
                "GET /api/memory/L3 HTTP/1.1",
                "PATCH /api/memory/entry/00000000-0000-0000-0000-000000000007 HTTP/1.1",
                "GET /api/memory/L3 HTTP/1.1",
            ][step];
            assert_eq!(first, expected);
            let body = if length > 0 {
                serde_json::from_slice::<Value>(&bytes[header_end..header_end + length]).unwrap()
            } else {
                Value::Null
            };
            let response=match step {
                0=>{stored=body;json!({"id":id})},
                2=>{assert_ne!(stored["content"],body["content"]);stored["content"]=body["content"].clone();json!({"id":id,"updated":true})},
                _=>json!({"enabled":true,"layer":"L3","include_archived":false,"entries":[{"id":id,"scope":"Global","content":stored["content"]}]}),
            }.to_string();
            write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",response.len(),response).unwrap();
        }
        requests
    });
    let runner = LiveScenarioRunner {
        client: Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap(),
        base_url: base,
        timeout_cap: None,
        poll_interval: Duration::from_millis(1),
        model: None,
        claim_scope: LiveClaimScope::Focused,
        u5_deadline: None,
        u5_host: None,
    };
    let (old, new, id, evidence) = runner.prepare_u7_correction().unwrap();
    let frozen = std::path::PathBuf::from(std::env::var("COWD_EVAL_U7_FIXTURE_FILE").unwrap())
        .with_extension("corrected.json");
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(frozen).unwrap()).unwrap(),
        json!(&new),
        "independent corrected values must be frozen before the model request"
    );
    assert_eq!(id, "00000000-0000-0000-0000-000000000007");
    assert_ne!(old.warehouse, new.warehouse);
    assert_ne!(old.passphrase, new.passphrase);
    assert!(u7_persisted_record(
        &evidence["readback"],
        &id,
        &new.content
    ));
    assert!(!u7_persisted_record(
        &evidence["readback"],
        &id,
        &old.content
    ));
    assert_eq!(
        task.join().unwrap().len(),
        4,
        "no Provider request or baseline recall is permitted"
    );
}


#[test]
fn u8_fixed_order_preserves_revision_order_and_independent_integer_oracle() {
    let input = U8OrderInput::fixed();
    assert_eq!(input.rows.len(), 64);
    assert_eq!(input.total(false), 389_400);
    assert_eq!(input.total(true), 387_900);
    assert_eq!(input.revisions[0], ("PO447-003".into(), Some(12)));
    assert_eq!(input.revisions[2], ("PO447-003".into(), Some(4)));
    let mut reordered = input.clone();
    reordered.revisions.swap(0, 2);
    assert_eq!(
        reordered.total(true),
        404_700,
        "later quantity replaces rather than adds"
    );
    assert_eq!(input.prompts.len(), 3);
    assert!(!input.prompts[2].contains("387900"));
    assert!(!input.prompts[2].contains(U8_CLAUSE_BODY));
    assert!(!input.prompts[2].contains("389400"));
    assert_eq!(
        serde_json::to_vec(&input).unwrap(),
        serde_json::to_vec(&U8OrderInput::fixed()).unwrap()
    );
}

#[test]
fn u8_answer_requires_both_latest_total_and_complete_original_clause() {
    let input = U8OrderInput::fixed();
    let correct = json!({"total_cents":387900,"original_clause":U8_CLAUSE_BODY});
    assert!(u8_answer(&input, &correct.to_string()));
    assert!(u8_answer(&input, &format!("```json\n{correct}\n```")));
    for changed in [
        json!({"total_cents":389400,"original_clause":U8_CLAUSE_BODY}),
        json!({"total_cents":387900}),
        json!({"total_cents":"387900","original_clause":U8_CLAUSE_BODY}),
        json!({"total_cents":387900,"original_clause":"取消行不计费"}),
        json!({"total_cents":387900,"original_clause":"foreign clause"}),
    ] {
        assert!(!u8_answer(&input, &changed.to_string()));
    }
    assert!(!u8_answer(
        &input,
        &format!("工具失败，但猜到答案 {correct}")
    ));
}

#[test]
fn u8_source_and_terminal_are_required_without_inventing_history_mechanism_hit() {
    let input = U8OrderInput::fixed();
    let source = controlled_live_prompt(U8_HISTORY, input.prompts[0].clone(), None);
    let control: Value = serde_json::from_str(
        source
            .lines()
            .next()
            .unwrap()
            .strip_prefix("COWD_EVAL_CONTROL ")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        control["resource_scopes"],
        json!(["provider", "provider_account", "provider_token_pool"])
    );
    let mut case = json!({"session_id":"s","trace":[{"channel":"messages","response":{"messages":[{"id":"m","role":"user","session_id":"s","blocks":[{"text":source}]}]}}]});
    assert!(u8_source_observed(&case, &source));
    assert!(!u8_source_observed(&case, &input.prompts[1]));
    case["trace"][0]["response"]["messages"][0]["session_id"] = json!("foreign");
    assert!(!u8_source_observed(&case, &source));
    assert!(
        !u8_turn_passed(&case, "s"),
        "a source and a guessed answer cannot supply outcome or ACK"
    );
    // Correct arithmetic alone deliberately has no history offload/range/wire claim.
    assert!(u8_answer(
        &input,
        &json!({"total_cents":387900,"original_clause":U8_CLAUSE_BODY}).to_string()
    ));
    assert!(case.get("history_offload").is_none());
}


#[test]
fn u8_body_and_label_are_separate_without_relaxing_body_or_amount() {
    let input = U8OrderInput::fixed();
    let body = json!({"total_cents":387900,"original_clause":U8_CLAUSE_BODY}).to_string();
    assert!(u8_answer(&input, &body));
    assert!(!u8_label_matches(&input, &body));
    let tagged=json!({"total_cents":387900,"original_clause":format!("{U8_SOURCE_LABEL}：{U8_CLAUSE_BODY}")}).to_string();
    assert!(u8_answer(&input, &tagged));
    assert!(u8_label_matches(&input, &tagged));
    assert!(!u8_answer(
        &input,
        &json!({"total_cents":387900,"original_clause":format!("foreign：{U8_CLAUSE_BODY}")})
            .to_string()
    ));
    assert!(!u8_answer(
        &input,
        &json!({"total_cents":389400,"original_clause":U8_CLAUSE_BODY}).to_string()
    ));
    assert!(!u8_answer(
        &input,
        &json!({"total_cents":387900,"source_label":U8_SOURCE_LABEL}).to_string()
    ));
    assert!(!u8_answer(
        &input,
        &json!({"total_cents":387900,"original_clause":"取消行不计费"}).to_string()
    ));
}

#[test]
fn u8_long_input_uses_original_history_compiler_with_fixed_bounded_scale() {
    let input = U8OrderInput::long_history();
    assert_eq!(input.rows.len(), 4096);
    assert_eq!(input.total(false), 24_581_400);
    assert_eq!(input.total(true), 24_579_900);
    let offset = input.prompts[0].find(U8_CLAUSE_BODY).unwrap();
    assert!(
        offset > 48_000,
        "old clause is outside a1200byte catalog preview"
    );
    assert!(!input.prompts[2].contains(U8_CLAUSE_BODY));
    assert!(!input.prompts[2].contains("24579900"));
    let mut session = runtime::Session::default();
    let mut bases = Vec::new();
    let compiler = runtime::PreparedRequestCompiler::new(3);
    // Actual447 prompt-composition bytes were81979..84911, covered by a
    // conservative100000-token allowance (more than one token per old byte),
    // not a claim that these synthetic estimates are observed Provider usage.
    let inventory = runtime::ProviderContextInventory {
        tool_schema_tokens: 100_000,
        ..Default::default()
    };
    for (index, prompt) in input.prompts.iter().enumerate() {
        session
            .push_message(runtime::ConversationMessage::user_text(
                controlled_live_prompt(U8_HISTORY, prompt.clone(), None),
            ))
            .unwrap();
        let prepared = compiler.prepare(
            &runtime::PromptAssembly::new(vec![]),
            &session.messages_view(),
            inventory,
            0,
            "step-3.7-flash",
        );
        assert!(
            prepared.fixed_input_tokens < 196_608,
            "retain at least25percent of unchanged262144window as headroom"
        );
        bases.push(prepared.fixed_input_tokens);
        if index < 2 {
            session
                .push_message(runtime::ConversationMessage::assistant(vec![
                    runtime::ContentBlock::Text {
                        text: "已接收本轮资料，等待用户下一轮输入。".into(),
                    },
                ]))
                .unwrap();
        }
    }
    let estimated = runtime::estimate_session_tokens(&session);
    assert!(estimated > 30_000);
    // Original production configuration preserves6messages. Three short
    // assistant replies/user inputs do not ensure a compaction boundary.
    let config = runtime::CompactionConfig {
        preserve_recent_messages: 6,
        max_estimated_tokens: 235_929,
        ..Default::default()
    };
    assert!(!runtime::should_compact(&session, config));
    let report = json!({"rows":input.rows.len(),"prompt_bytes":input.prompts.iter().map(|s|s.len()).collect::<Vec<_>>(),"clause_body_offset":offset,"unchanged_model_context_window":262144,"original_history_compiler_fixed_input_tokens_with_conservative100000token_overhead":bases,"original_session_estimate_tokens":estimated,"production_preserve_recent_messages":6,"automatic_compaction_predicted":false,"planned_legal_original_read":"context_retrieve/session_history/current limit1, follow actualread_request only if needed; no retryof447denied computation tools","mechanism_hit_not_guaranteed":true});
    println!("U8_OFFLINE_PLAN {report}");
    if let Ok(path) = std::env::var("COWD_U8_OFFLINE_PLAN") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}

#[test]
#[ignore = "owner supplies frozen447originalcase; offline reinterpretation preserves oldreport"]
fn u8_original447_case_is_body_correct_under_separate_label_observation() {
    let path = std::env::var("COWD_U8_447_CASE").unwrap();
    let bytes = std::fs::read(path).unwrap();
    let expected = std::env::var("COWD_U8_447_CASE_SHA256").unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(&bytes)), expected);
    let case: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(case["status"], "failed", "oldfrozenreportnotrewritten");
    let phases = case["u8_history"]["phase_results"].as_array().unwrap();
    assert_eq!(phases.len(), 3);
    for p in phases {
        assert!(u8_turn_passed(p, p["session_id"].as_str().unwrap()));
        assert_eq!(p["u8_original_user_sources_present"], true);
    }
    let input = U8OrderInput::fixed();
    let answer = case["u8_terminal_answer"].as_str().unwrap();
    assert!(u8_answer(&input, answer));
    assert!(!u8_label_matches(&input, answer));
}

#[test]
fn u8_timeout_default_and_explicit_longer_observation_are_consistent() {
    let base = LiveScenarioTimeout::tool();
    let default = base.u8_with_cap(None);
    assert_eq!(default.initial_wait, Duration::from_secs(90));
    assert_eq!(default.inactivity_wait, Duration::from_secs(75));
    assert_eq!(default.nominal_wait, Duration::from_secs(300));
    assert_eq!(default.absolute_wait, Some(Duration::from_secs(600)));
    let extended = base.u8_with_cap(Some(Duration::from_secs(900)));
    assert_eq!(extended.inactivity_wait, Duration::from_secs(900));
    assert_eq!(extended.nominal_wait, Duration::from_secs(900));
    assert_eq!(extended.absolute_wait, Some(Duration::from_secs(900)));
    let now = Instant::now();
    assert!(extended.observation_deadline(now, now, Duration::ZERO, 1) > now + Duration::from_secs(899));
    let tightened = base.u8_with_cap(Some(Duration::from_secs(300)));
    assert_eq!(tightened.absolute_wait, Some(Duration::from_secs(300)));
}

// The Evolution background worker is optional and separately switchable. An
// instance that deliberately turned it off has no lane and therefore no
// running worker; that is the configured state, not a degraded subsystem.
// The archived evaluation reports failed because this health observation was
// treated as a hard failure and the live block status then failed with it.
#[test]
fn evolution_projector_health_contract_separates_disabled_from_broken() {
    let response = |enabled: bool, worker_running: bool| {
        json!({
            "kind": "evolution.signals",
            "envelope": {"status": "service_boundary_ready", "service": "evolution"},
            "count": 0,
            "projector": {
                "enabled": enabled,
                "worker_running": worker_running,
                "consecutive_failures": 0,
                "dead_letter_count": 0,
            },
            "outcome_projector": {"worker_running": true, "consecutive_failures": 0, "dlq_count": 0},
            "signals": [],
        })
    };
    let disabled = semantic_health_observation(
        "/api/evolution/signals",
        LiveHealthContract::EvolutionProjectors,
        response(false, false),
    );
    assert_eq!(
        disabled["status"], "passed",
        "a deliberately disabled projector is not a failed health observation: {disabled}"
    );
    let enabled_but_idle = semantic_health_observation(
        "/api/evolution/signals",
        LiveHealthContract::EvolutionProjectors,
        response(true, false),
    );
    assert_eq!(
        enabled_but_idle["status"], "failed",
        "an enabled projector that is not running is still a failed health observation"
    );
    let enabled_and_running = semantic_health_observation(
        "/api/evolution/signals",
        LiveHealthContract::EvolutionProjectors,
        response(true, true),
    );
    assert_eq!(enabled_and_running["status"], "passed");
    // An unknown effective switch keeps the strict requirement.
    let unknown = semantic_health_observation(
        "/api/evolution/signals",
        LiveHealthContract::EvolutionProjectors,
        json!({"projector": {"worker_running": false}}),
    );
    assert_eq!(unknown["status"], "failed");
}

// The archived business-passed reports failed for one reason only: the live
// block status is computed from the health observations, and a degraded
// optional subsystem made it "failed" even though the scenario business
// oracle passed. Both consumption points are pinned here: the live status
// (live_scenario_runner) and the report gate rows that read it.
#[test]
fn live_block_status_follows_the_business_scenarios_not_an_optional_subsystem() {
    let scenario = json!({
        "scenario_id": "live_direct_terminal",
        "status": "passed",
        "acceptance": {"passed": true, "checks": [{"name": "root_business_outcome_succeeded", "passed": true}]},
        "production_trace": {"session_id": "s", "terminal_id": "t", "execution_id": "e"},
        "observation_integrity": {"status": "passed", "cursor_monotonic": true, "timeline_drained": true, "message_pages_drained": true, "omitted_changes": 0, "stall_detected": false},
    });
    let live_block = |health_status: &str| {
        json!({
            "kind": "harness_eval.live_gateway_scenarios",
            "status": if health_status == "passed" { "passed" } else { "failed" },
            "claim_scope": "focused",
            "claim_status": if health_status == "passed" { "component_passed" } else { "component_failed" },
            "health_status": health_status,
            "selection_status": "passed",
            "passed": 1,
            "failed": 0,
            "scenario_count": 1,
            "selected_scenario_ids": ["live_direct_terminal"],
            "scenarios": [scenario.clone()],
        })
    };
    // With a healthy observation set the live block passes and the report
    // gate rows that consume it pass as well.
    let healthy = live_block("passed");
    assert_eq!(healthy["status"], "passed");
    let degraded = live_block("failed");
    assert_eq!(
        degraded["status"], "failed",
        "a degraded gateway still fails the live block; the fix only removes the optional-subsystem false positive"
    );
    let report = json!({
        "level": "deep",
        "live_gateway_scenarios": healthy,
        "scenarios": [
            {"capability": "stable_ai_scenario_matrix", "status": "passed"},
            {"capability": "live_gateway_scenarios", "status": "passed"},
        ],
    });
    let gate = crate::report::evaluate_report_gate(&report);
    let item = |name: &str| {
        gate.items
            .iter()
            .find(|item| item.name == name)
            .unwrap_or_else(|| panic!("missing gate item {name}"))
    };
    assert_eq!(item("scenario_capability_status").status, "passed");
}


// The U5 resource consumer used to cap the whole evidence-collection window
// at five seconds. With a real provider that publishes its first durable
// event later than that, the consumer reported "budget exhausted before IO"
// and the scenario ended without any acceptance fact. The window belongs to
// the scenario; only a single read stays short.
#[test]
fn u5_evidence_collection_uses_the_scenario_window_and_bounds_each_read() {
    let scenario_deadline = Instant::now() + Duration::from_secs(600);
    let window = u5_evidence_window(scenario_deadline, None);
    assert_eq!(
        window, scenario_deadline,
        "the collection window is the scenario deadline, not a five second cap"
    );
    let earlier = Instant::now() + Duration::from_secs(30);
    assert_eq!(
        u5_evidence_window(scenario_deadline, Some(earlier)),
        earlier,
        "a registered U5 deadline still bounds the window"
    );
    let read = u5_read_deadline(window);
    assert!(
        read <= window,
        "one read never outlives the collection window: {read:?} vs {window:?}"
    );
    assert!(
        read >= Instant::now() + Duration::from_secs(4),
        "one read keeps its own short bound"
    );
    let expired = Instant::now();
    assert_eq!(
        u5_read_deadline(expired),
        expired,
        "an expired window is not extended by the per-read bound"
    );
}

// A leaf that missed its own observation deadline keeps the evidence it had
// already collected. An empty acceptance block reads as "no evidence
// existed"; carrying the collected integrity report plus the deadline reason
// distinguishes that from "evidence was not collected before the deadline".
// The leaf stays failed and the deadline itself is unchanged.
#[test]
fn deadline_expired_leaf_retains_its_collected_evidence() {
    let spec = LiveScenarioSpec {
        id: "u5-read-b",
        prompt: "read the exact granted path",
        acceptance: LiveAcceptance::Contains("0.10.1"),
        timeout: LiveScenarioTimeout::tool(),
    };
    let diagnostics = json!({
        "timeline": {"events": []},
        "observation_integrity": {
            "status": "passed",
            "cursor_monotonic": true,
            "timeline_drained": true,
            "message_pages_drained": false,
            "omitted_changes": 0,
            "stall_detected": false,
        },
    });
    let leaf = failed_scenario_with_session_execution_and_program(
        spec,
        started_now(),
        Vec::new(),
        "session-b".to_string(),
        Some("execution-b".to_string()),
        "receipt_observation_deadline_elapsed".to_string(),
        diagnostics,
        None,
    );
    assert_eq!(leaf["status"], "failed", "the leaf still fails");
    assert_eq!(
        leaf["error"], "receipt_observation_deadline_elapsed",
        "the reason is preserved"
    );
    assert_eq!(
        leaf["collected_evidence"]["terminal_reason"],
        "evidence_collection_deadline_elapsed"
    );
    assert_eq!(
        leaf["collected_evidence"]["observation_integrity"]["status"],
        "passed",
        "the evidence collected before the deadline is not discarded"
    );
    assert_eq!(
        leaf["collected_evidence"]["observation_integrity"]["message_pages_drained"],
        false,
        "the evidence is carried verbatim, not restated"
    );
}

fn started_now() -> Instant {
    Instant::now()
}

// The execution's own projection read can fail (for example the history prefix
// refuses to publish because the graph head moved while it was being
// prepared). The observer must then report that progress is unobservable,
// instead of leaving a stale phase that claims the run is idle in a state it
// has already left.
#[test]
fn unreadable_execution_projection_marks_progress_unobservable() {
    let mut observer = LiveScenarioObserver::default();
    observer.mark_quiet();
    assert_eq!(observer.phase(), "quiet");
    observer.mark_unobservable();
    assert_eq!(
        observer.phase(),
        "unobservable",
        "a failed projection read must not leave the previous phase in place"
    );
    // A terminal observation is authoritative and is never downgraded: the
    // terminal phase is reached through the observer's own root summary, so
    // this asserts the guard's intent on the public surface.
    let mut observer = LiveScenarioObserver::default();
    observer.mark_stalled();
    observer.mark_unobservable();
    assert_eq!(observer.phase(), "unobservable");
}
