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

    fn review(&self, id: &str, condition: &str) -> ObjectiveReviewVerification {
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
    let outcome =
        fixture
            .store
            .append_transaction(crate::runtime_event_store::AppendTransactionRequest {
                transaction_id: "conclusion-after-stale-goal".into(),
                expected_streams: snapshot.sources,
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
    assert!(outcome.is_err());
    assert_eq!(fixture.store.stream_revision("conclusion-test").unwrap(), 0);
}
