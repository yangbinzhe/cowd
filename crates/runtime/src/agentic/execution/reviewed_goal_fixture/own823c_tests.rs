use super::*;
use harness_contract::agent_action::{
    AgentActionStatus, ObjectiveCompleteRequestInput, ObjectiveReviewDecision, ObjectiveReviewInput,
};
use harness_contract::goal::{AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract};

async fn root_delivery_review_case(file: bool) {
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    let services = ReviewedFixtureServices(services);
    let _cleanup = RealFixtureCleanup {
        services: services.0.clone(),
        host: host.clone(),
    };
    _cleanup.protect(async {

    host.services.set(Arc::downgrade(&services.0)).unwrap();
    install_real_process_fixture(&services, &host).await;
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let spec = services
        .task_runtime_port()
        .bind_task_spec(
            "session-dispatch",
            Some(PermissionMode::ReadOnly),
            harness_contract::task::TaskSpec::new("Deliver existing reviewed content"),
        )
        .unwrap();
    services
        .task_aggregate_service()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "task-root-dispatch".into(),
            mission_id: services.mission_runtime().default_mission_id().into(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::User,
            origin_session_id: "session-dispatch".into(),
            origin_turn_id: "turn-dispatch".into(),
            root_task_id: "task-root-dispatch".into(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
            mission_assigned_by: "test".into(),
            spec,
            evidence_refs: Vec::new(),
        })
        .unwrap();
    let body: &[u8] = if file {
        b"nonempty_lines=3\n"
    } else {
        "The requested substantive answer. 正文\n".as_bytes()
    };
    std::fs::write(services.workspace_root().join("review-effect.txt"), body).unwrap();
    let digest = crate::context_evidence::read_sha256(body);
    let content = services
        .publish_authorized_content(body, &digest, "text/plain", "session-dispatch")
        .await
        .unwrap();
    let actions = services.agent_action_service();
    assert!(actions.project_if_exists("program-dispatch").unwrap().is_none());
    let (_, runtime) =
        root_reads_generic_results(&services.0, host.clone(), &content.selector, false, false).await;
    let selector = if file {
        let publication = serde_json::json!({"source":"file","path":"review-effect.txt",
            "sha256":digest,"media_type":"text/plain"});
        root_review_input_tool(&runtime, "c4-root-publication", "artifact_publish", &publication.to_string()).await;
        assert!(actions.project_if_exists("program-dispatch").unwrap().is_none());
        let history = services.session_history_reader().unwrap();
        let receipt = history
        .domain_events_by_kind_blocking("session-dispatch", "tool.invocation.completed", 0, 256)
        .unwrap()
        .into_iter()
        .find(|event| event.payload["tool_call_id"] == "c4-root-publication")
        .unwrap();
        let evidence = receipt.payload["full_output_ref"].as_str().unwrap();
        let access = services
        .session_evidence_access("session-dispatch", evidence.strip_prefix("tool://").unwrap())
        .await
        .unwrap()
        .unwrap();
        let raw = services.artifact_store().resolve(&access.retrieval_selector).unwrap();
        let published: serde_json::Value = serde_json::from_slice(
        &services.artifact_store().read(&raw, &raw.visibility_scope, None).await.unwrap(),
    )
    .unwrap();
        published["content_ref"].as_str().unwrap().to_owned()
    } else {
        content.selector.clone()
    };
    let artifact = services.artifact_store().resolve(&selector).unwrap();
    assert_eq!(services.artifact_store().read(&artifact, &artifact.visibility_scope, None).await.unwrap(), body);
    let mut root_actor = root("root-binding", AgentAction::ArtifactCommit(ArtifactCommitInput {
        content_ref: selector.clone(), kind: "result".into(), title: "Requested result".into(), relates_to: Vec::new(),
    })).actor;
    root_actor.root_execution_id = Some("root-agentic-execution".into());
    root_actor.resource_scopes = vec!["workspace:.".into()];
    let envelope = |id: &str, action| AgentActionEnvelope {
        action_id: id.into(), actor: root_actor.clone(), expected_revision: None, action,
    };
    let inspected = services.submit_agent_action(&envelope("c4-registration-guidance", AgentAction::StateInspect(
        harness_contract::agent_action::StateInspectInput {
            query: None, wait_for_workers: false, scope_ref: None, after_revision: None,
            page_cursor: None, entry_ref: None,
        },
    ))).await.unwrap();
    assert_eq!(inspected.status, AgentActionStatus::Applied);
    assert!(inspected.actionable.iter().any(|hint| hint.contains("tool_search") && hint.contains("artifact_commit")));
    let registered = services.submit_agent_action(&envelope("c4-register-root", AgentAction::ArtifactCommit(
        ArtifactCommitInput { content_ref: selector.clone(), kind: "result".into(), title: "Requested result".into(), relates_to: Vec::new() },
    ))).await.unwrap();
    assert_eq!(registered.status, AgentActionStatus::Applied);
    let result_ref = registered.changed_refs[0].clone();
    let registered_program = actions.project("program-dispatch").unwrap();
    assert_eq!(registered_program.required_team_count, 0);
    assert!(registered_program.teams.is_empty() && registered_program.tasks.is_empty());
    assert_eq!(registered_program.artifacts[&result_ref].content_ref, selector);
    services.goal_store().create(GoalContract {
        input_contract: None,
        id: "goal:root-agentic-execution".into(), session_id: "session-dispatch".into(),
        objective: "dispatch objective".into(), criteria: vec![AcceptanceCriterion {
            id: "reviewed-delivery".into(), statement: "Deliver the requested existing content with independent verification".into(),
            statement_ref: None, source_refs: Vec::new(), required_evidence: vec!["execution_graph:root-agentic-execution".into()],
            status: AcceptanceStatus::Open, waiver: None,
        }], constraints: Vec::new(), phase: "execution".into(), evidence_refs: Vec::new(), unresolved: Vec::new(), blockers: Vec::new(),
        scope: harness_contract::goal::GoalScope::UserObjective,
        user_intent_criterion_id: Some("reviewed-delivery".into()), source_intent_ref: Some("session_message:dispatch".into()),
        execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
            objective_id: root_actor.objective_id.clone(), session_id: root_actor.session_id.clone(), turn_id: root_actor.turn_id.clone(),
            root_execution_id: "root-agentic-execution".into(), agentic_program_id: root_actor.program_id.clone(),
        }), spec_revision: 1, spec_digest: "c4-root-delivery".into(), review_refs: Vec::new(), waiting: None,
        participation_requirement: None, obligations: Vec::new(), recovery: None, terminal: None,
        completion: GoalCompletion::Open, revision: 1, user_sequence: 1, reviews: Vec::new(),
    }).unwrap();
    let completion = || envelope("c4-completion-request", AgentAction::ObjectiveCompleteRequest(
        ObjectiveCompleteRequestInput {
            result_refs: vec![result_ref.clone()], evidence_refs: vec![selector.clone()], unresolved: Vec::new(),
            blocked: false, blocked_reason: None, human_review_required: false, recommended_next_actions: Vec::new(),
        },
    ));
    let supervisor = crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(services.goal_store()));
    if file {
        let requested = services.submit_agent_action(&completion()).await.unwrap();
        assert_eq!(requested.status, AgentActionStatus::Applied);
        crate::agentic::supervision::reconcile_completion_request(&actions, &supervisor, "program-dispatch").unwrap();
    } else {
        assert!(actions.project("program-dispatch").unwrap().completion_request.is_none());
    }
    assert_eq!(services.goal_store().get("goal:root-agentic-execution").unwrap().unwrap().completion, GoalCompletion::Open);
    let review_input = || ObjectiveReviewInput {
        criterion_ref: "reviewed-delivery".into(), decision: ObjectiveReviewDecision::Satisfied,
        result_refs: vec![result_ref.clone()], evidence_refs: vec![selector.clone()], reason_ref: selector.clone(), reason: String::new(),
    };
    let before_self_review = services.goal_store().get("goal:root-agentic-execution").unwrap().unwrap();
    let self_review = services.submit_agent_action(&envelope("c4-self-review", AgentAction::ObjectiveReview(review_input()))).await;
    assert!(self_review.is_err() || self_review.unwrap().status == AgentActionStatus::Rejected);
    assert_eq!(services.goal_store().get("goal:root-agentic-execution").unwrap().unwrap(), before_self_review);
    let reviewer = start_r1_scoped_goal_reviewer(&services, &root_actor, &selector, "c4-review-only", file).await;
    assert_ne!(reviewer.actor.actor_id, root_actor.actor_id);
    assert!(reviewer.actor.execution_id.is_some());
    let reviewed = services.submit_agent_action(&AgentActionEnvelope {
        action_id: "c4-distinct-review".into(), actor: reviewer.actor.clone(), expected_revision: None,
        action: AgentAction::ObjectiveReview(review_input()),
    }).await.unwrap();
    assert_eq!(reviewed.status, AgentActionStatus::Applied);
    let reviewed_goal = services.goal_store().get("goal:root-agentic-execution").unwrap().unwrap();
    let proof = reviewed_goal.reviews.last().unwrap().verification.as_ref().unwrap();
    assert!(!proof.reads.is_empty());
    assert_eq!(Some(proof.reviewer_execution_id.as_str()), reviewer.actor.execution_id.as_deref());
    reviewer.finish(&services).await;
    let reviewed_program = actions.project("program-dispatch").unwrap();
    let reviewed_goal = services.goal_store().get("goal:root-agentic-execution").unwrap().unwrap();
    assert!(crate::agentic::current_reviews_satisfy_the_goal(&reviewed_goal));
    assert!(crate::agentic::uncovered_delivered_result_refs(&reviewed_program, &reviewed_goal).is_empty());
    let mut final_request = completion();
    final_request.action_id = "c4-completion-after-review".into();
    assert_eq!(services.submit_agent_action(&final_request).await.unwrap().status, AgentActionStatus::Applied);
    crate::agentic::supervision::reconcile_completion_request(&actions, &supervisor, "program-dispatch").unwrap();
    let goal = services.goal_store().get("goal:root-agentic-execution").unwrap().unwrap();
    assert_eq!(goal.completion, GoalCompletion::Satisfied);
    assert!(goal.terminal.is_some());
    let program = actions.project("program-dispatch").unwrap();
    assert_eq!(program.required_team_count, 0);
    assert_eq!(program.teams.len(), 1);
    assert_eq!(program.tasks.len(), 1);
    let control = program.tasks.values().next().unwrap();
    assert_eq!(control.purpose, harness_contract::agent_action::TaskPurpose::Exploration);
    assert_eq!(control.status, crate::AgenticTaskStatus::Withdrawn);
    assert!(control.active_attempts.is_empty() && control.artifact_refs.is_empty());
    assert_eq!(program.artifacts.len(), 1);
    assert!(program.objective_verdict.is_some());
    services.shutdown_execution().await;
    }, || {}).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_file_registration_distinct_review_goal_cas_and_retirement() {
    root_delivery_review_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_body_registration_distinct_review_goal_cas_and_retirement() {
    root_delivery_review_case(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_goal_cas_precedes_original_physical_retirement_without_accepting_early() {
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    exercise_reviewed_goal_delivery_with_drain(
        services,
        host,
        false,
        false,
        DelegatedEffectCase::None,
        true,
        PhysicalDrainTiming::AfterGoal,
    )
    .await;
}
