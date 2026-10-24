//! Source-carrier contracts: real native Goal/action writers and proof readers,
//! no Provider, task execution, result-coverage or business-completion claim.
use super::*;
use crate::agentic::review_evidence::current_review::{
    program_events_after_cut, AppliedReviewContext,
};
use crate::execution_core::GoalStore;
use harness_contract::agent_action::*;
use harness_contract::goal::{ObjectiveReviewVerification, ProofInputSet};
use std::{collections::BTreeMap, sync::Arc};

struct Fixture {
    store: Arc<RuntimeEventStore>,
    actor: AgentActorBinding,
}

impl Fixture {
    fn new() -> Self {
        use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
        let store = Arc::new(RuntimeEventStore::for_test());
        let mut graph = ExecutionGraph::new("two independent criterion records");
        graph.id = "carrier-root".into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "carrier-session".into(),
            turn_id: "carrier-turn".into(),
            root_task_id: "carrier-task".into(),
            task_id: "carrier-task".into(),
            generation: 1,
        });
        crate::ExecutionCommitService::new(Arc::clone(&store))
            .register_graph(graph)
            .unwrap();
        let actor: AgentActorBinding = serde_json::from_value(serde_json::json!({
            "objective_id":"carrier-objective", "program_id":"carrier-program",
            "session_id":"carrier-session", "turn_id":"carrier-turn",
            "root_execution_id":"carrier-root", "actor_id":"carrier-root-actor", "kind":"root",
            "objective_summary":"review both conditions", "model_lease":"test", "permission_ceiling":"read-only",
        }))
        .unwrap();
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":"goal:carrier-root", "session_id":"carrier-session", "objective":"review both conditions",
            "criteria":[
                {"id":"delivery", "statement":"delivered output", "status":"open"},
                {"id":"source", "statement":"source-backed output", "status":"satisfied"}
            ],
            "phase":"execution", "user_intent_criterion_id":"delivery", "source_intent_ref":"session_message:carrier-input",
            "execution_binding":{
                "objective_id":"carrier-objective", "session_id":"carrier-session", "turn_id":"carrier-turn",
                "root_execution_id":"carrier-root", "agentic_program_id":"carrier-program"
            },
            "spec_revision":1, "spec_digest":"immutable-user-spec", "completion":"open", "revision":1, "user_sequence":1,
        })).unwrap();
        GoalStore::new(Arc::clone(&store)).create(goal).unwrap();
        let fixture = Self { store, actor };
        let opened = fixture
            .service()
            .apply(&fixture.envelope(
                "open",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "review owner".into(),
                    mission: "review both conditions".into(),
                    objective: None,
                }),
            ))
            .unwrap();
        assert_eq!(opened.status, AgentActionStatus::Applied, "{opened:?}");
        fixture
    }

    fn service(&self) -> crate::AgentActionService {
        crate::AgentActionService::new(Arc::clone(&self.store))
    }
    fn program(&self) -> AgenticProgramProjection {
        self.service().project("carrier-program").unwrap()
    }
    fn goal(&self) -> GoalContract {
        GoalStore::new(Arc::clone(&self.store))
            .get("goal:carrier-root")
            .unwrap()
            .unwrap()
    }
    fn envelope(&self, id: &str, action: AgentAction) -> AgentActionEnvelope {
        AgentActionEnvelope {
            action_id: id.into(),
            actor: self.actor.clone(),
            expected_revision: None,
            action,
        }
    }

    fn pending_review(
        &self,
        id: &str,
        condition: &str,
    ) -> (AgentActionEnvelope, ObjectiveReviewVerification) {
        let mut proof = ObjectiveReviewVerification {
            input_set: Some(ProofInputSet {
                version: 2,
                condition_ref: Some(condition.into()),
                result_refs: vec![],
                auxiliary_refs: vec![],
                required_obligation_refs: vec![],
                task_refs: vec![],
                artifact_refs: vec![],
                packet_digests: BTreeMap::new(),
                material_events: vec![],
                source_revisions: BTreeMap::new(),
                required_scopes: vec![],
                required_scope_complete: true,
                acceptance_policy_digest: String::new(),
                semantic_digest: String::new(),
            }),
            result_source_revisions: BTreeMap::new(),
            goal_spec_revision: Some(1),
            goal_spec_digest: Some("immutable-user-spec".into()),
            work_manifest_digest: String::new(),
            independence_required: true,
            review_policy_digest: None,
            effect_manifest_digest: None,
            effect_source_refs: vec![],
            reviewer_execution_id: "root-execution:carrier-root".into(),
            producer_execution_ids: vec![],
            reads: vec![],
            current_review_effect_key: None,
        };
        // Native assembly discovers source cuts before its first snapshot.
        proof.input_set.as_mut().unwrap().source_revisions =
            actual_input_snapshot(&self.store, &self.program(), &[], Default::default(), None)
                .unwrap()
                .sources;
        let snapshot = proof_input_snapshot(
            &self.store,
            &self.program(),
            Some(&self.goal()),
            &proof,
            Default::default(),
        )
        .unwrap();
        proof.work_manifest_digest = snapshot.work_digest;
        proof.effect_manifest_digest = Some(snapshot.effects.digest);
        proof.review_policy_digest = Some(snapshot.policy_digest.clone());
        let inputs = proof.input_set.as_mut().unwrap();
        inputs.semantic_digest = snapshot.semantic_digest;
        inputs.acceptance_policy_digest = snapshot.policy_digest;
        inputs.source_revisions = snapshot
            .sources
            .into_iter()
            .map(|source| (source.stream_id, source.expected_revision))
            .collect();
        let envelope = self.envelope(
            id,
            AgentAction::ObjectiveReview(ObjectiveReviewInput {
                criterion_ref: condition.into(),
                decision: ObjectiveReviewDecision::Satisfied,
                result_refs: vec![],
                evidence_refs: vec!["review-report".into()],
                reason_ref: "review-report".into(),
                reason: String::new(),
            }),
        );
        (envelope, proof)
    }

    fn review(&self, id: &str, condition: &str) -> ObjectiveReviewVerification {
        let (envelope, proof) = self.pending_review(id, condition);
        // This source-level fixture does not manufacture result coverage. The
        // native record writer requires a producer attribution; acceptance of
        // real delivered results remains the separate coverage reader's job.
        let mutation = GoalStore::new(Arc::clone(&self.store))
            .prepare_agentic_objective_action(
                &envelope,
                &["original-producer-attribution".into()],
                Some(&proof),
            )
            .unwrap();
        let applied = self
            .service()
            .apply_with_goal_event(
                &envelope,
                mutation.stream_id,
                mutation.expected_stream_revision,
                mutation.event,
                Some(&proof),
            )
            .unwrap();
        assert_eq!(applied.status, AgentActionStatus::Applied, "{applied:?}");
        proof
    }

    fn events(&self, proof: &ObjectiveReviewVerification) -> Vec<crate::DurableRuntimeEvent> {
        let pinned =
            proof.input_set.as_ref().unwrap().source_revisions["agentic-program:carrier-program"];
        program_events_after_cut(
            &self.store,
            "agentic-program:carrier-program",
            pinned,
            self.program().revision,
        )
        .unwrap()
    }
    fn carrier(
        &self,
        proof: &ObjectiveReviewVerification,
        goal: &GoalContract,
        events: &[crate::DurableRuntimeEvent],
    ) -> bool {
        applied_program_tail_preserves_inputs(
            &self.store,
            &self.program(),
            Some(goal),
            proof,
            &AppliedReviewContext {
                action_id: "source-review",
                program_source: "agentic-program:carrier-program",
                pinned: proof.input_set.as_ref().unwrap().source_revisions
                    ["agentic-program:carrier-program"],
                through: self.program().revision,
                program_events: events,
            },
        )
        .unwrap()
    }
}

#[test]
fn different_required_reviews_preserve_each_original_proof_and_own_call() {
    let fixture = Fixture::new();
    let source = fixture.review("source-review", "source");
    let delivery = fixture.review("delivery-review", "delivery");
    let program = fixture.program();
    let goal = fixture.goal();
    assert_eq!(
        goal.reviews
            .iter()
            .map(|r| r.criterion_ref.as_str())
            .collect::<Vec<_>>(),
        vec!["source", "delivery"]
    );
    for (proof, own_call) in [(&source, "source-review"), (&delivery, "delivery-review")] {
        let snapshot = validate_applied_proof_inputs(
            &fixture.store,
            &program,
            Some(&goal),
            proof,
            Default::default(),
        )
        .unwrap();
        assert_eq!(
            snapshot.current_review_effect_key.as_deref(),
            Some(own_call)
        );
        let record = goal
            .reviews
            .iter()
            .find(|record| record.verification.as_ref() == Some(proof))
            .unwrap();
        assert_eq!(
            super::super::coverage::validate_review_coverage(
                &goal,
                &program,
                record,
                &[],
                &[],
                &snapshot
            ),
            Err(super::super::coverage::ReviewCoverageError::Independence),
            "a source-only proof does not manufacture delivered-result acceptance",
        );
        assert!(snapshot
            .sources
            .iter()
            .any(|cut| cut.stream_id == "agentic-program:carrier-program"
                && cut.expected_revision == 4));
    }
    let events = fixture.events(&source);
    assert!(
        !super::super::current_review::program_cut_absorbs_own_review(
            &events,
            "agentic-program:carrier-program",
            2,
            4,
            "source-review"
        )
    );
    assert!(fixture.carrier(&source, &goal, &events));
}

#[test]
fn other_review_requires_exact_native_record_proof_binding_and_primary() {
    let fixture = Fixture::new();
    let source = fixture.review("source-review", "source");
    fixture.review("delivery-review", "delivery");
    let goal = fixture.goal();
    let events = fixture.events(&source);
    assert!(fixture.carrier(&source, &goal, &events));
    for field in [
        "actor_id",
        "program_id",
        "objective_id",
        "session_id",
        "turn_id",
        "root_execution_id",
    ] {
        let mut changed = events.clone();
        changed[1].payload["envelope"]["actor"][field] = serde_json::json!("foreign");
        assert!(!fixture.carrier(&source, &goal, &changed), "{field}");
    }
    for field in ["review_verification", "envelope"] {
        let mut missing = events.clone();
        missing[1].payload[field] = serde_json::Value::Null;
        assert!(!fixture.carrier(&source, &goal, &missing), "{field}");
    }
    for change in [
        "spec",
        "proof",
        "actor",
        "execution",
        "criterion",
        "id",
        "result",
        "decision",
        "binding",
        "goal-id",
    ] {
        let mut changed = goal.clone();
        let record = changed.reviews.last_mut().unwrap();
        match change {
            "spec" => record.spec_revision += 1,
            "proof" => {
                record.verification.as_mut().unwrap().work_manifest_digest = "different-work".into()
            }
            "actor" => record.reviewer_actor = "foreign".into(),
            "execution" => record.reviewer_execution_id = Some("foreign".into()),
            "criterion" => record.criterion_ref = "foreign".into(),
            "id" => record.review_id = "foreign".into(),
            "result" => record.result_refs.push("artifact:foreign".into()),
            "decision" => record.decision = "gap".into(),
            "binding" => {
                changed
                    .execution_binding
                    .as_mut()
                    .unwrap()
                    .root_execution_id = "foreign".into()
            }
            "goal-id" => changed.id = "foreign-goal".into(),
            _ => unreachable!(),
        }
        assert!(!fixture.carrier(&source, &changed, &events), "{change}");
    }
    let mut no_primary = events.clone();
    no_primary[1].payload["envelope"]["action_id"] = serde_json::json!("no-primary");
    let mut corresponding = goal.clone();
    // Independently computed literal identity: even a matching Goal record
    // cannot replace the original action primary/index.
    corresponding.reviews[1].review_id =
        "objective_review:106fd328fc4fdb251f1457b52465a7b66b02c5719c2ad96b444da9a5f0b48462".into();
    assert!(!fixture.carrier(&source, &corresponding, &no_primary));
    let mut mismatch = events.clone();
    mismatch[1].event_id = "different-primary-event".into();
    assert!(!fixture.carrier(&source, &goal, &mismatch));
}

#[test]
fn authenticated_carrier_does_not_hide_business_or_unknown_tail() {
    let fixture = Fixture::new();
    let source = fixture.review("source-review", "source");
    fixture.review("delivery-review", "delivery");
    let goal = fixture.goal();
    assert!(fixture.carrier(&source, &goal, &fixture.events(&source)));
    fixture
        .service()
        .apply(&fixture.envelope(
            "new-work",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "new-required-work".into(),
                mission: "different semantic work".into(),
                objective: None,
            }),
        ))
        .unwrap();
    assert!(!fixture.carrier(&source, &goal, &fixture.events(&source)));
    assert!(validate_applied_proof_inputs(
        &fixture.store,
        &fixture.program(),
        Some(&goal),
        &source,
        Default::default()
    )
    .err()
    .unwrap()
    .contains("proof Program source changed"));
    let mut unknown = fixture.events(&source);
    unknown.last_mut().unwrap().kind = "unknown-control".into();
    assert!(!fixture.carrier(&source, &goal, &unknown));
}

#[test]
fn carrier_admission_still_recomputes_full_proof_and_preserves_original_cas() {
    let fixture = Fixture::new();
    let source = fixture.review("source-review", "source");
    fixture.review("delivery-review", "delivery");
    let program = fixture.program();
    let goal = fixture.goal();
    let events = fixture.events(&source);
    // Authenticate carriers using the real records, then require the production
    // snapshot to recompute the original semantic/work/effect inputs.
    let context = AppliedReviewContext {
        action_id: "source-review",
        program_source: "agentic-program:carrier-program",
        pinned: 2,
        through: 4,
        program_events: &events,
    };
    for dimension in ["work", "semantic", "effects"] {
        let mut changed = source.clone();
        match dimension {
            "work" => changed.work_manifest_digest = "foreign-work".into(),
            "semantic" => {
                changed.input_set.as_mut().unwrap().semantic_digest = "foreign-semantic".into()
            }
            "effects" => changed.effect_manifest_digest = Some("foreign-effects".into()),
            _ => unreachable!(),
        }
        let error = validate_proof_inputs_for_action(
            &fixture.store,
            &program,
            Some(&goal),
            &changed,
            Default::default(),
            Some(super::super::current_review::ReviewActionIdentity::Applied(
                &context,
            )),
        )
        .err()
        .unwrap();
        assert!(error.contains("stale v2 proof"), "{dimension}: {error}");
        assert!(error.contains(dimension), "{dimension}: {error}");
    }
    let snapshot = validate_applied_proof_inputs(
        &fixture.store,
        &program,
        Some(&goal),
        &source,
        Default::default(),
    )
    .unwrap();
    let original_goal_cut = fixture
        .store
        .stream_revision("goal:goal:carrier-root")
        .unwrap();
    let goals = GoalStore::new(Arc::clone(&fixture.store));
    goals
        .revise(
            &goal.id,
            goal.revision,
            goal.user_sequence + 1,
            "concurrent user change",
            |g| {
                g.objective = "new business requirement".into();
                vec!["objective".into()]
            },
        )
        .unwrap();
    let mut expected_streams = snapshot.sources;
    expected_streams.push(crate::ExpectedStreamRevision {
        stream_id: "conclusion-test".into(),
        expected_revision: 0,
    });
    let outcome =
        fixture
            .store
            .append_transaction(crate::runtime_event_store::AppendTransactionRequest {
                transaction_id: "conclusion-after-stale-goal".into(),
                expected_streams,
                events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: "conclusion-test".into(),
                        scope: crate::RuntimeEventScope::Program,
                        kind: "test.conclusion".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    },
                    idempotency_key: Some("never-committed".into()),
                    schema_version: 1,
                }],
            });
    let Err(crate::RuntimeEventStoreError::StaleRevision {
        stream_id,
        expected,
        actual,
    }) = outcome
    else {
        panic!("the changed Goal must reject the original read cut: {outcome:?}");
    };
    assert_eq!(stream_id, "goal:goal:carrier-root");
    assert_eq!(expected, original_goal_cut);
    assert_eq!(actual, original_goal_cut + 1);
    let output_revision = fixture.store.stream_revision("conclusion-test").unwrap();
    assert_eq!(output_revision, 0);
    eprintln!("CAS_REJECT Goal stream={stream_id} expected={expected} actual={actual} output_revision={output_revision}");
}

#[test]
fn applied_authentication_uses_original_action_without_a_tool_plan() {
    let fixture = Fixture::new();
    let proof = fixture.review("direct-original-review", "source");
    let program = fixture.program();
    let session_events = fixture
        .store
        .list_stream("session:carrier-session")
        .unwrap();
    assert!(session_events
        .iter()
        .all(|event| event.kind != "tool.execution_plan.created"));
    assert!(proof.current_review_effect_key.is_none());
    let snapshot = validate_applied_proof_inputs(
        &fixture.store,
        &program,
        Some(&fixture.goal()),
        &proof,
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        snapshot.current_review_effect_key.as_deref(),
        Some("direct-original-review")
    );
    // A foreign carrier is a single changed input after a real legal baseline.
    for field in ["session", "objective", "turn", "root"] {
        let mut foreign = program.clone();
        match field {
            "session" => foreign.session_id = "foreign-session".into(),
            "objective" => foreign.objective_id = "foreign-objective".into(),
            "turn" => foreign.turn_id = "foreign-turn".into(),
            "root" => foreign.root_execution_id = Some("foreign-root".into()),
            _ => unreachable!(),
        }
        let result =
            with_authenticated_applied_review(&fixture.store, &foreign, &proof, |_| Ok(()));
        assert_eq!(
            result.unwrap_err(),
            "original applied review identity mismatch",
            "{field}"
        );
    }
}

#[test]
fn applied_authentication_rejects_missing_ambiguous_and_unindexed_originals() {
    let fixture = Fixture::new();
    let proof = fixture.review("legal-original", "source");
    assert_eq!(
        with_authenticated_applied_review(&fixture.store, &fixture.program(), &proof, |identity| {
            Ok(identity.action_id().to_owned())
        })
        .unwrap(),
        "legal-original"
    );

    let missing = Fixture::new();
    let (_, pending) = missing.pending_review("not-applied", "source");
    assert_eq!(
        with_authenticated_applied_review(&missing.store, &missing.program(), &pending, |_| Ok(()))
            .unwrap_err(),
        "original applied review action missing"
    );

    // Public journal append only: no backend mutation/index override seam.
    // The authentic original remains, and one second same-proof application
    // is the only added factor. It must fail uniqueness before any exemption.
    let original = fixture.events(&proof).pop().unwrap();
    let mut duplicate = original.payload.clone();
    duplicate["envelope"]["action_id"] = serde_json::json!("second-original");
    fixture
        .store
        .append(crate::RuntimeEventInput {
            stream_id: original.stream_id.clone(),
            scope: original.scope,
            kind: original.kind.clone(),
            status: original.status.clone(),
            actor: original.actor.clone(),
            refs: original.refs.clone(),
            payload: duplicate,
        })
        .unwrap();
    assert_eq!(
        with_authenticated_applied_review(&fixture.store, &fixture.program(), &proof, |_| Ok(()))
            .unwrap_err(),
        "original review action is ambiguous"
    );

    // A separately valid original establishes the exact raw record shape.
    // Its exact payload in another original Fixture with no action primary
    // is journal data, not authority, even though the actor/proof match.
    let unindexed = Fixture::new();
    let (envelope, pending) = unindexed.pending_review("unindexed-review", "source");
    unindexed
        .store
        .append(crate::RuntimeEventInput {
            stream_id: "agentic-program:carrier-program".into(),
            scope: crate::RuntimeEventScope::Program,
            kind: "agentic.action_applied".into(),
            status: Some("applied".into()),
            actor: Some(envelope.actor.actor_id.clone()),
            refs: original.refs,
            payload: serde_json::json!({"envelope":envelope,"review_verification":pending}),
        })
        .unwrap();
    assert_eq!(
        with_authenticated_applied_review(
            &unindexed.store,
            &unindexed.program(),
            &pending,
            |_| Ok(())
        )
        .unwrap_err(),
        "original applied review primary missing"
    );
}

#[test]
fn applied_phase_keeps_the_accepted_policy_and_current_source_cuts() {
    let fixture = Fixture::new();
    let proof = fixture.review("phase-original", "source");
    let program = fixture.program();
    let goal = fixture.goal();
    let accepted = &proof.input_set.as_ref().unwrap().acceptance_policy_digest;
    let read = |accepted_policy| {
        with_authenticated_applied_review(&fixture.store, &program, &proof, |identity| {
            proof_input_snapshot_for_phase(
                &fixture.store,
                &program,
                Some(&goal),
                &proof,
                accepted_policy,
                Default::default(),
                Some(identity),
            )
        })
    };
    let before = read(Some(accepted.as_str())).unwrap();
    assert_eq!(before.policy_digest, *accepted);
    // A new real Session strategy record changes current policy. The already
    // accepted policy parameter must remain authoritative only for this phase;
    // neither this helper test nor source-only coverage grants completion.
    fixture
        .store
        .append(crate::RuntimeEventInput {
            stream_id: "session:carrier-session".into(),
            scope: crate::RuntimeEventScope::Session,
            kind: "runtime.strategy.selected".into(),
            status: None,
            actor: Some("conversation_runtime.strategy_owner".into()),
            refs: vec![],
            payload: serde_json::json!({"execution_graph_ref":"carrier-root",
            "session_ref":"carrier-session","turn_ref":"carrier-turn",
            "risk":"high","modifiers":[],"gates":[]}),
        })
        .unwrap();
    let current = read(None).unwrap();
    let phase = read(Some(accepted.as_str())).unwrap();
    assert_ne!(
        current.policy_digest, *accepted,
        "current strategy must reach the real policy reader"
    );
    assert_eq!(phase.policy_digest, *accepted);
    assert_ne!(current.semantic_digest, phase.semantic_digest);
    assert_eq!(
        phase.current_review_effect_key.as_deref(),
        Some("phase-original")
    );
    assert!(phase
        .sources
        .iter()
        .any(|cut| cut.stream_id == "session:carrier-session" && cut.expected_revision == 1));
}

#[test]
fn applied_phase_program_advance_rejects_the_original_transaction_cut() {
    let fixture = Fixture::new();
    let proof = fixture.review("program-fence-original", "source");
    let program = fixture.program();
    let snapshot =
        with_authenticated_applied_review(&fixture.store, &program, &proof, |identity| {
            proof_input_snapshot_for_phase(
                &fixture.store,
                &program,
                Some(&fixture.goal()),
                &proof,
                Some(&proof.input_set.as_ref().unwrap().acceptance_policy_digest),
                Default::default(),
                Some(identity),
            )
        })
        .unwrap();
    assert!(snapshot
        .sources
        .iter()
        .any(|cut| cut.stream_id == "agentic-program:carrier-program"
            && cut.expected_revision == program.revision));
    let mut legal_cuts = snapshot.sources.clone();
    legal_cuts.push(crate::ExpectedStreamRevision {
        stream_id: "phase-cut-positive-test".into(),
        expected_revision: 0,
    });
    fixture
        .store
        .append_transaction(crate::runtime_event_store::AppendTransactionRequest {
            transaction_id: "phase-with-current-program".into(),
            expected_streams: legal_cuts,
            events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: "phase-cut-positive-test".into(),
                    scope: crate::RuntimeEventScope::Program,
                    kind: "test.conclusion".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                },
                idempotency_key: Some("legal-publication".into()),
                schema_version: 1,
            }],
        })
        .unwrap();
    assert_eq!(
        fixture
            .store
            .stream_revision("phase-cut-positive-test")
            .unwrap(),
        1
    );
    let advanced = fixture
        .service()
        .apply(&fixture.envelope(
            "concurrent-required-work",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "new team".into(),
                mission: "new required work".into(),
                objective: None,
            }),
        ))
        .unwrap();
    assert_eq!(advanced.status, AgentActionStatus::Applied);
    let mut expected_streams = snapshot.sources;
    expected_streams.push(crate::ExpectedStreamRevision {
        stream_id: "phase-conclusion-test".into(),
        expected_revision: 0,
    });
    let outcome =
        fixture
            .store
            .append_transaction(crate::runtime_event_store::AppendTransactionRequest {
                transaction_id: "phase-after-stale-program".into(),
                expected_streams,
                events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: "phase-conclusion-test".into(),
                        scope: crate::RuntimeEventScope::Program,
                        kind: "test.conclusion".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    },
                    idempotency_key: Some("never-committed".into()),
                    schema_version: 1,
                }],
            });
    let Err(crate::RuntimeEventStoreError::StaleRevision {
        stream_id,
        expected,
        actual,
    }) = outcome
    else {
        panic!("the original Program read cut must reject publication: {outcome:?}");
    };
    assert_eq!(stream_id, "agentic-program:carrier-program");
    assert_eq!(expected, program.revision);
    assert_eq!(actual, program.revision + 1);
    let output_revision = fixture
        .store
        .stream_revision("phase-conclusion-test")
        .unwrap();
    assert_eq!(output_revision, 0);
    eprintln!("CAS_REJECT Program stream={stream_id} expected={expected} actual={actual} output_revision={output_revision}");
}
