use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
    ArtifactCommitInput, MessagePublishInput, ObjectiveCompleteRequestInput, TaskAttemptFailInput,
    TaskClaimInput, TaskPublishInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput,
    TaskSupersedeInput, TeamCreateInput,
};
use harness_contract::goal::{
    AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract,
    ObjectiveEvidenceRequirement, ObjectiveObligation, ObjectiveObligationState, ObjectiveTerminal,
    ObjectiveTerminalKind,
};

use super::*;

fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            objective_id: "objective-1".to_string(),
            program_id: "program-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_execution_id: Some("root-execution-1".to_string()),
            required_team_count: 1,
            objective_summary: "test objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: "root-1".to_string(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        },
        expected_revision: None,
        action,
    }
}

fn managed(
    action_id: &str,
    team_id: &str,
    agent_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            objective_id: "objective-1".to_string(),
            program_id: "program-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_execution_id: Some("root-execution-1".to_string()),
            required_team_count: 1,
            objective_summary: "test objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: agent_id.to_string(),
            kind: AgentActorKind::Agent,
            execution_id: Some(format!("execution:{agent_id}")),
            team_id: Some(team_id.to_string()),
            agent_id: Some(agent_id.to_string()),
        },
        expected_revision: None,
        action,
    }
}

fn supervisor(action_id: &str, execution_id: &str, action: AgentAction) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.actor_id = "runtime.program-supervisor".to_string();
    envelope.actor.kind = AgentActorKind::Supervisor;
    envelope.actor.execution_id = Some(execution_id.to_string());
    envelope
}

#[test]
fn topic_observations_cross_teams_and_resume_from_durable_execution_cursor() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
    let service = AgentActionService::new(Arc::clone(&store));
    let team_a = service
        .apply(&root(
            "topic-team-a",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "publish evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team a")
        .changed_refs[0]
        .clone();
    let team_b = service
        .apply(&root(
            "topic-team-b",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Synthesis".to_string(),
                mission: "consume cross-Team evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team b")
        .changed_refs[0]
        .clone();
    let invite = |action_id: &str, team_ref: &str, role: &str| {
        service
            .apply(&root(
                action_id,
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team_ref.to_string(),
                    role: role.to_string(),
                    mission: "observe committed topic deltas".to_string(),
                    required_capabilities: vec!["read".to_string()],
                }),
            ))
            .expect("invite")
            .changed_refs[0]
            .clone()
    };
    let author = invite("topic-author", &team_a, "Author");
    let peer = invite("topic-peer", &team_a, "Peer");
    let cross_team_peer = invite("topic-cross-peer", &team_b, "Synthesizer");
    service
        .apply(&managed(
            "program-broadcast",
            &team_a,
            &author,
            AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: "topic:program-1".to_string(),
                summary: Some("public cross-Team finding".to_string()),
                content_ref: None,
                refs: vec!["artifact://finding".to_string()],
            }),
        ))
        .expect("Program broadcast");
    service
        .apply(&managed(
            "team-a-message",
            &team_a,
            &author,
            AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: format!("topic:{team_a}"),
                summary: Some("Team-only implementation note".to_string()),
                content_ref: None,
                refs: Vec::new(),
            }),
        ))
        .expect("Team message");

    let cross_page = service
        .topic_observations(
            "program-1",
            &cross_team_peer,
            "execution:cross-team-peer",
            16,
            48 * 1024,
        )
        .expect("cross-Team observations")
        .expect("Program message");
    assert_eq!(cross_page.entries.len(), 1);
    assert_eq!(
        cross_page.entries[0].entry.summary.as_deref(),
        Some("public cross-Team finding")
    );
    service
        .acknowledge_topic_observations(AgenticTopicObservationAck {
            program_id: "program-1".to_string(),
            execution_id: "execution:cross-team-peer".to_string(),
            through_revision: cross_page.to_revision,
            expected_cursor_revision: cross_page.cursor_revision,
        })
        .expect("acknowledge cross-Team page");

    let restarted = AgentActionService::new(store);
    assert!(restarted
        .topic_observations(
            "program-1",
            &cross_team_peer,
            "execution:cross-team-peer",
            16,
            48 * 1024,
        )
        .expect("restart query")
        .is_none());
    let own_team_page = restarted
        .topic_observations("program-1", &peer, "execution:own-team-peer", 16, 48 * 1024)
        .expect("own-Team observations")
        .expect("Program and own-Team messages");
    assert_eq!(own_team_page.entries.len(), 2);
    assert!(own_team_page
        .entries
        .iter()
        .any(|entry| entry.entry.summary.as_deref() == Some("Team-only implementation note")));
}

#[test]
fn complete_vertical_chain_is_durable_and_idempotent() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
    let service = AgentActionService::new(Arc::clone(&store));
    let goals = Arc::new(crate::execution_core::goal::GoalStore::new(store));
    goals
        .create(GoalContract {
            id: "goal:root-execution-1".to_string(),
            session_id: "session-1".to_string(),
            objective: "test objective".to_string(),
            criteria: vec![AcceptanceCriterion {
                id: "terminal_synthesis".to_string(),
                statement: "produce one durable terminal synthesis".to_string(),
                required_evidence: vec!["execution_graph:root-execution-1".to_string()],
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: Vec::new(),
            phase: "execution".to_string(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            obligations: Vec::new(),
            program_ref: Some("root-execution-1".to_string()),
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        })
        .expect("goal");

    let team_receipt = service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "Investigate evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team");
    let team_id = team_receipt.changed_refs[0].clone();
    let duplicate = service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research".to_string(),
                mission: "Investigate evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("duplicate");
    assert_eq!(duplicate.revision, team_receipt.revision);

    let agent_receipt = service
        .apply(&root(
            "invite-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_id.clone(),
                role: "researcher".to_string(),
                mission: "produce evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("agent");
    let agent_id = agent_receipt.changed_refs[0].clone();
    let reviewer_receipt = service
        .apply(&root(
            "invite-reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_id.clone(),
                role: "reviewer".to_string(),
                mission: "verify evidence independently".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("reviewer");
    let reviewer_id = reviewer_receipt.changed_refs[0].clone();
    let task_receipt = service
        .apply(&root(
            "publish-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_id.clone(),
                title: "Research".to_string(),
                objective: "Read source".to_string(),
                acceptance: "artifact and evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
            }),
        ))
        .expect("task");
    let task_id = task_receipt.changed_refs[0].clone();
    service
        .apply(&managed(
            "claim-task",
            &team_id,
            &agent_id,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_id.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    let artifact_receipt = service
        .apply(&managed(
            "commit-artifact",
            &team_id,
            &agent_id,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: "artifact://abc".to_string(),
                kind: "research".to_string(),
                title: "Findings".to_string(),
                relates_to: vec![task_id.clone()],
            }),
        ))
        .expect("artifact");
    let artifact_ref = artifact_receipt.changed_refs[0].clone();
    service
        .apply(&managed(
            "submit-task",
            &team_id,
            &agent_id,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: task_id.clone(),
                artifact_refs: vec![artifact_ref.clone()],
                evidence_refs: vec!["tool://source-observation".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("submit");
    let submitted = service.project("program-1").expect("submitted projection");
    assert_eq!(
        submitted.tasks[&task_id].evidence_refs,
        vec![
            "artifact://abc".to_string(),
            "tool://source-observation".to_string(),
        ],
        "Runtime attaches committed artifact content without making the model duplicate it"
    );
    service
        .apply(&managed(
            "review-task",
            &team_id,
            &reviewer_id,
            AgentAction::TaskReview(TaskReviewInput {
                task_ref: task_id.clone(),
                decision: TaskReviewDecision::Accept,
                reason: "verified".to_string(),
                evidence_refs: vec!["tool://independent-inspection".to_string()],
            }),
        ))
        .expect("review");
    let delegated_completion = service
        .apply(&managed(
            "delegated-complete",
            &team_id,
            &reviewer_id,
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                final_artifact_ref: artifact_ref.clone(),
                evidence_refs: vec!["artifact://abc".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("delegated completion rejection");
    assert_eq!(delegated_completion.status, AgentActionStatus::Rejected);
    assert_eq!(
        delegated_completion.error.expect("error").code,
        "objective_completion_not_delegated"
    );
    let complete = service
        .apply(&root(
            "complete",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                final_artifact_ref: artifact_ref,
                evidence_refs: vec!["artifact://abc".to_string()],
                unresolved: Vec::new(),
            }),
        ))
        .expect("complete");
    assert_eq!(complete.status, AgentActionStatus::Applied, "{complete:?}");
    assert_eq!(
        service.project("program-1").expect("projection").status,
        super::super::program::AgenticProgramStatus::CompletionRequested
    );
    let pending = service.project("program-1").expect("projection");
    assert!(pending.objective_verdict.is_none());
    let objective_supervisor =
        crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(&goals));
    let request_revision = pending
        .completion_request
        .as_ref()
        .expect("completion request")
        .program_revision;
    let mut foreign_terminal = goals
        .get("goal:root-execution-1")
        .expect("goal read")
        .expect("goal");
    foreign_terminal.completion = GoalCompletion::Satisfied;
    foreign_terminal.evidence_refs = vec![
        "artifact://abc".to_string(),
        "execution_graph:root-execution-1".to_string(),
    ];
    foreign_terminal.terminal = Some(ObjectiveTerminal {
        kind: ObjectiveTerminalKind::Satisfied,
        terminal_fence: "foreign-presentation-terminal".to_string(),
        authority_revision: request_revision,
        reason: "fault injection: presentation raced Agentic supervision".to_string(),
        evidence_refs: foreign_terminal.evidence_refs.clone(),
        diagnostics: Vec::new(),
        committed_at_ms: 1,
    });
    assert!(matches!(
        service.bind_objective_verdict("program-1", &foreign_terminal),
        Err(AgentActionServiceError::Corrupt(message))
            if message == "objective_verdict_binding_mismatch"
    ));
    assert_eq!(
        service.project("program-1").expect("projection").status,
        crate::AgenticProgramStatus::CompletionRequested,
        "a foreign terminal fence must never verify the Program"
    );
    let accepted_task = pending.tasks.values().next().expect("accepted task");
    objective_supervisor
        .reconcile(
            "goal:root-execution-1",
            request_revision,
            &format!("agentic-objective:program-1:request:{request_revision}"),
            vec![ObjectiveObligation {
                obligation_id: format!("agentic-task:{}", accepted_task.task_id),
                required: true,
                success_predicate: accepted_task.acceptance.clone(),
                producer: Default::default(),
                evidence_requirement: ObjectiveEvidenceRequirement {
                    required_artifact_kinds: Vec::new(),
                    independent_verifier_required: true,
                    reread_required: false,
                },
                state: ObjectiveObligationState::Satisfied,
                artifact_refs: accepted_task.artifact_refs.clone(),
                evidence_refs: accepted_task.evidence_refs.clone(),
                reread_receipts: Vec::new(),
                verifier_decision: Some("accepted_by:independent-reviewer".to_string()),
                diagnostic_code: None,
            }],
            vec![
                "artifact://abc".to_string(),
                "execution_graph:root-execution-1".to_string(),
            ],
            Vec::new(),
            false,
            "fault injection: Goal terminal committed before Program verdict binding",
        )
        .expect("Objective terminal");
    assert_eq!(
        service.project("program-1").expect("projection").status,
        crate::AgenticProgramStatus::CompletionRequested,
        "Goal terminal alone must not mutate Program projection"
    );
    let verified = crate::agentic::supervision::reconcile_completion_request(
        &service,
        &objective_supervisor,
        "program-1",
    )
    .expect("Objective supervision")
    .expect("completion request");
    assert_eq!(verified.status, crate::AgenticProgramStatus::Verified);
    assert!(verified.objective_verdict.is_some());
    assert_eq!(
        goals
            .get("goal:root-execution-1")
            .expect("goal projection")
            .expect("goal")
            .completion,
        GoalCompletion::Satisfied
    );
    let replayed = crate::agentic::supervision::reconcile_completion_request(
        &service,
        &objective_supervisor,
        "program-1",
    )
    .expect("replay Objective supervision")
    .expect("verified Program");
    assert_eq!(replayed.revision, verified.revision);
    assert_eq!(replayed.objective_verdict, verified.objective_verdict);
    let terminal_mutation = service
        .apply(&root(
            "late-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Late".to_string(),
                mission: "must not reopen verified truth".to_string(),
                objective: None,
            }),
        ))
        .expect("terminal rejection");
    assert_eq!(terminal_mutation.status, AgentActionStatus::Rejected);
    assert_eq!(
        terminal_mutation.error.expect("error").code,
        "program_terminal"
    );
}

#[tokio::test]
async fn production_artifact_authority_closes_submit_review_and_completion_chain() {
    let temporary = tempfile::tempdir().expect("artifact root");
    let artifacts = Arc::new(crate::ArtifactStore::sqlite_default(temporary.path()));
    let content = artifacts
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: "session:session-1".to_string(),
                expected_bytes: None,
                original_name: Some("verified-report.md".to_string()),
            },
            b"verified production evidence",
        )
        .await
        .expect("durable content");
    let service = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open_in_memory().expect("event store"),
    ))
    .with_artifact_store(artifacts);
    let team = service
        .apply(&root(
            "durable-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Delivery".to_string(),
                mission: "produce and independently verify evidence".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let worker = service
        .apply(&root(
            "durable-worker",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "implementer".to_string(),
                mission: "produce the report".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("worker")
        .changed_refs[0]
        .clone();
    let reviewer = service
        .apply(&root(
            "durable-reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "reviewer".to_string(),
                mission: "verify the report".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("reviewer")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "durable-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Verified report".to_string(),
                objective: "produce a grounded report".to_string(),
                acceptance: "durable report and independent review".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    service
        .apply(&managed(
            "durable-claim",
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    let artifact = service
        .apply(&managed(
            "durable-commit",
            &team,
            &worker,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "final_report".to_string(),
                title: "Verified report".to_string(),
                relates_to: vec![task.clone()],
            }),
        ))
        .expect("commit")
        .changed_refs[0]
        .clone();
    let durable_submit = service
        .apply(&managed(
            "durable-submit",
            &team,
            &worker,
            AgentAction::TaskSubmit(TaskSubmitInput {
                task_ref: task.clone(),
                artifact_refs: vec![artifact.clone()],
                evidence_refs: vec![content.selector.clone()],
                unresolved: vec![
                    "Known limitation disclosed in the reviewed artifact; not an acceptance blocker"
                        .to_string(),
                ],
            }),
        ))
        .expect("submit");
    assert_eq!(
        durable_submit.status,
        AgentActionStatus::Applied,
        "{durable_submit:?}"
    );
    assert_eq!(
        service
            .apply(&managed(
                "durable-review",
                &team,
                &reviewer,
                AgentAction::TaskReview(TaskReviewInput {
                    task_ref: task,
                    decision: TaskReviewDecision::Accept,
                    reason: "content resolved and acceptance was met".to_string(),
                    evidence_refs: vec![content.selector.clone()],
                }),
            ))
            .expect("review")
            .status,
        AgentActionStatus::Applied
    );
    assert_eq!(
        service
            .apply(&root(
                "blocked-objective-complete",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    final_artifact_ref: artifact.clone(),
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec!["Objective-level delivery blocker".to_string()],
                }),
            ))
            .expect("objective blocker rejection")
            .status,
        AgentActionStatus::Rejected,
        "objective-level blockers remain authoritative even after Task acceptance"
    );
    assert_eq!(
        service
            .apply(&root(
                "durable-complete",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    final_artifact_ref: artifact,
                    evidence_refs: vec![content.selector],
                    unresolved: Vec::new(),
                }),
            ))
            .expect("completion")
            .status,
        AgentActionStatus::Applied,
        "an independent Task accept verdict is authoritative; disclosed limitations must not be re-litigated by the Objective supervisor"
    );
}

#[test]
fn completion_cannot_bypass_real_artifact_and_task_review() {
    let service = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open_in_memory().expect("event store"),
    ));
    service
        .apply(&root(
            "create-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Team".to_string(),
                mission: "Work".to_string(),
                objective: None,
            }),
        ))
        .expect("team");
    let rejected = service
        .apply(&root(
            "complete",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                final_artifact_ref: "artifact:missing".to_string(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
            }),
        ))
        .expect("rejection");
    assert_eq!(rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        rejected.error.expect("error").code,
        "objective_not_verifiable"
    );
}

#[test]
fn task_claim_requires_a_roster_agent_and_expired_lease_is_reclaimable() {
    let service = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open_in_memory().expect("event store"),
    ));
    let team = service
        .apply(&root(
            "lease-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Lease Team".to_string(),
                mission: "recover abandoned work".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "lease-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "own recoverable work".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "lease-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Recover".to_string(),
                objective: "complete even after a worker crash".to_string(),
                acceptance: "durable result".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let root_rejection = service
        .apply(&root(
            "root-cannot-claim",
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("root rejection");
    assert_eq!(
        root_rejection.error.expect("error").code,
        "actor_cannot_execute_task"
    );

    let claimant = managed(
        "expired-reclaim",
        &team,
        &agent,
        AgentAction::TaskClaim(TaskClaimInput {
            task_ref: task.clone(),
            reason: Some("previous claimant lease expired".to_string()),
        }),
    );
    let mut projection = service.project("program-1").expect("projection");
    let task_projection = projection.tasks.get_mut(&task).expect("task projection");
    task_projection.status = AgenticTaskStatus::Claimed;
    task_projection.claimant = Some("agent:abandoned".to_string());
    task_projection.claim_execution_id = Some("execution:abandoned".to_string());
    task_projection.lease_expires_at_ms = Some(10);
    assert!(validate_transition(&projection, &claimant, 11).is_none());

    let expired_submit = managed(
        "expired-submit",
        &team,
        &agent,
        AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: task.clone(),
            artifact_refs: vec!["artifact:any".to_string()],
            evidence_refs: vec!["artifact://any".to_string()],
            unresolved: Vec::new(),
        }),
    );
    let task_projection = projection.tasks.get_mut(&task).expect("task projection");
    task_projection.claimant = Some(agent);
    task_projection.claim_execution_id = expired_submit.actor.execution_id.clone();
    assert_eq!(
        validate_transition(&projection, &expired_submit, 11),
        Some(("task_claim_expired", task))
    );
}

#[test]
fn failed_physical_attempts_stop_auto_retry_without_blocking_program_replan() {
    let service = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open_in_memory().expect("event store"),
    ));
    let team = service
        .apply(&root(
            "failure-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Failure recovery".to_string(),
                mission: "bound retries".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "failure-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "worker".to_string(),
                mission: "attempt work".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "failure-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Bounded attempt".to_string(),
                objective: "never loop forever".to_string(),
                acceptance: "durable completion or blocker".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let execution_id = format!("execution:{agent}");
    for attempt in 1..=3 {
        assert_eq!(
            service
                .apply(&managed(
                    &format!("failure-claim-{attempt}"),
                    &team,
                    &agent,
                    AgentAction::TaskClaim(TaskClaimInput {
                        task_ref: task.clone(),
                        reason: None,
                    }),
                ))
                .expect("claim")
                .status,
            AgentActionStatus::Applied
        );
        assert_eq!(
            service
                .apply(&supervisor(
                    &format!("failure-settle-{attempt}"),
                    &execution_id,
                    AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                        task_ref: task.clone(),
                        execution_id: execution_id.clone(),
                        mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                        reason: format!("provider failure {attempt}"),
                        retryable: true,
                    }),
                ))
                .expect("settle")
                .status,
            AgentActionStatus::Applied
        );
    }
    let projection = service.project("program-1").expect("projection");
    let task = projection.tasks.get(&task).expect("task");
    assert_eq!(task.failed_attempts, 3);
    assert_eq!(task.status, AgenticTaskStatus::Published);
    assert_eq!(task.last_failure.as_deref(), Some("provider failure 3"));
    assert_eq!(projection.status, crate::AgenticProgramStatus::Open);
    assert!(projection.unresolved.is_empty());
    let replacement_agent = service
        .apply(&root(
            "failure-replacement-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "recovery worker".to_string(),
                mission: "take over after the bounded automatic retry window".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("replacement agent")
        .changed_refs[0]
        .clone();
    assert_eq!(
        service
            .apply(&managed(
                "failure-reassigned-claim",
                &team,
                &replacement_agent,
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task.task_id.clone(),
                    reason: Some("reassign after repeated provider failures".to_string()),
                }),
            ))
            .expect("reassigned claim")
            .status,
        AgentActionStatus::Applied
    );
}

#[test]
fn failed_review_attempts_stop_auto_retry_and_return_work_to_market() {
    let service = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open_in_memory().expect("event store"),
    ));
    let team = service
        .apply(&root(
            "review-failure-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Independent review".to_string(),
                mission: "review submitted work".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let task = service
        .apply(&root(
            "review-failure-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Review retry".to_string(),
                objective: "prove review recovery".to_string(),
                acceptance: "bounded independent review".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    // The execution worker reports review failure only after a real
    // submission. Set that already-durable precondition directly so this
    // reducer test stays focused on the review attempt state machine.
    let mut projection = service.project("program-1").expect("projection");
    projection.tasks.get_mut(&task).expect("task").status = AgenticTaskStatus::Submitted;

    for attempt in 1..=3 {
        let input = TaskAttemptFailInput {
            task_ref: task.clone(),
            execution_id: format!("review-execution-{attempt}"),
            mode: harness_contract::agent_action::AgentAttemptMode::Review,
            reason: format!("review provider failure {attempt}"),
            retryable: true,
        };
        assert_eq!(
            validate_transition(
                &projection,
                &supervisor(
                    &format!("review-failure-{attempt}"),
                    &input.execution_id,
                    AgentAction::TaskAttemptFail(input.clone()),
                ),
                now_ms()
            ),
            None
        );
        crate::agentic::work_market::apply_task_attempt_fail(&mut projection, &input);
    }
    let task = projection.tasks.get(&task).expect("task");
    assert_eq!(task.review_generation, 3);
    assert_eq!(task.failed_review_attempts, 3);
    assert_eq!(task.status, AgenticTaskStatus::Published);
    assert_eq!(
        task.last_failure.as_deref(),
        Some("review provider failure 3")
    );
}

#[test]
fn roster_agent_can_expand_own_team_but_not_mutate_another_team() {
    let service = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open_in_memory().expect("event store"),
    ));
    let first_team = service
        .apply(&root(
            "delegation-team-a",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Team A".to_string(),
                mission: "own scope".to_string(),
                objective: None,
            }),
        ))
        .expect("first team")
        .changed_refs[0]
        .clone();
    let second_team = service
        .apply(&root(
            "delegation-team-b",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Team B".to_string(),
                mission: "separate scope".to_string(),
                objective: None,
            }),
        ))
        .expect("second team")
        .changed_refs[0]
        .clone();
    let agent = service
        .apply(&root(
            "delegation-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: first_team.clone(),
                role: "lead".to_string(),
                mission: "organize local work".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let local = service
        .apply(&managed(
            "delegation-local-invite",
            &first_team,
            &agent,
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: first_team.clone(),
                role: "peer".to_string(),
                mission: "help locally".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("local invite");
    assert_eq!(local.status, AgentActionStatus::Applied);
    let cross_team = service
        .apply(&managed(
            "delegation-cross-invite",
            &first_team,
            &agent,
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: second_team,
                role: "outsider".to_string(),
                mission: "mutate other team".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("cross-team observation");
    assert_eq!(cross_team.status, AgentActionStatus::Rejected);
    assert_eq!(
        cross_team.error.expect("error").code,
        "cross_team_roster_mutation_not_delegated"
    );
}

#[test]
fn concurrent_actions_serialize_without_stale_revision_loss() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
    let service = AgentActionService::new(store);
    let workers = (0..24)
        .map(|index| {
            let service = service.clone();
            std::thread::spawn(move || {
                service
                    .apply(&root(
                        &format!("team-{index}"),
                        AgentAction::TeamCreate(TeamCreateInput {
                            name: format!("Team {index}"),
                            mission: "independent workstream".to_string(),
                            objective: None,
                        }),
                    ))
                    .expect("concurrent action")
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        assert_eq!(
            worker.join().expect("worker").status,
            AgentActionStatus::Applied
        );
    }
    let projection = service.project("program-1").expect("projection");
    assert_eq!(projection.teams.len(), 24);
    assert_eq!(projection.revision, 25);
}

#[test]
fn program_projection_recovers_exactly_after_process_reopen() {
    let directory = tempfile::tempdir().expect("temporary event store");
    let path = directory.path().join("agentic-events.sqlite3");
    let team_id;
    {
        let service = AgentActionService::new(Arc::new(
            RuntimeEventStore::try_open(&path).expect("event store"),
        ));
        team_id = service
            .apply(&root(
                "recover-team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Recovery Team".to_string(),
                    mission: "survive a Runtime restart".to_string(),
                    objective: None,
                }),
            ))
            .expect("commit team")
            .changed_refs[0]
            .clone();
        service
            .apply(&root(
                "recover-agent",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team_id.clone(),
                    role: "recovery owner".to_string(),
                    mission: "resume durable work".to_string(),
                    required_capabilities: vec!["read".to_string()],
                }),
            ))
            .expect("commit member");
    }

    let reopened = AgentActionService::new(Arc::new(
        RuntimeEventStore::try_open(&path).expect("reopen event store"),
    ));
    let projection = reopened.project("program-1").expect("replay program");
    assert_eq!(projection.teams[&team_id].name, "Recovery Team");
    assert_eq!(projection.teams[&team_id].member_ids.len(), 1);
    assert_eq!(projection.revision, 3);

    let duplicate = reopened
        .apply(&root(
            "recover-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery Team".to_string(),
                mission: "survive a Runtime restart".to_string(),
                objective: None,
            }),
        ))
        .expect("idempotent replay after reopen");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, projection.revision);
}

#[test]
fn failed_task_supersede_is_cas_idempotent_recoverable_and_not_fake_completion() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
    let service = AgentActionService::new(Arc::clone(&store));
    let team = service
        .apply(&root(
            "supersede-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery team".to_string(),
                mission: "replace a disproved approach without losing its audit trail".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let worker = service
        .apply(&root(
            "supersede-worker",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "investigator".to_string(),
                mission: "run falsifiable work".to_string(),
                required_capabilities: vec!["read".to_string()],
            }),
        ))
        .expect("worker")
        .changed_refs[0]
        .clone();
    let source = service
        .apply(&root(
            "supersede-source",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Disproved approach".to_string(),
                objective: "test the original hypothesis".to_string(),
                acceptance: "reproducible evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
            }),
        ))
        .expect("source task")
        .changed_refs[0]
        .clone();
    let publish_successor = |action_id: &str, title: &str, depends_on: Vec<String>| {
        service
            .apply(&root(
                action_id,
                AgentAction::TaskPublish(TaskPublishInput {
                    team_ref: team.clone(),
                    title: title.to_string(),
                    objective: "replace the failed hypothesis with bounded evidence".to_string(),
                    acceptance: "independently reviewable artifact".to_string(),
                    required_capabilities: vec!["read".to_string()],
                    depends_on,
                }),
            ))
            .expect("successor task")
            .changed_refs[0]
            .clone()
    };
    let part_a = publish_successor("supersede-part-a", "Replacement A", Vec::new());
    let part_b = publish_successor("supersede-part-b", "Replacement B", Vec::new());
    let cyclic = publish_successor(
        "supersede-cyclic",
        "Invalid replacement",
        vec![source.clone()],
    );
    let supersede_action = |replacement_task_refs: Vec<String>| {
        AgentAction::TaskSupersede(TaskSupersedeInput {
            task_ref: source.clone(),
            replacement_task_refs,
            reason: "the provider attempt disproved the original approach; split the work"
                .to_string(),
            evidence_refs: vec!["tool://failure-receipt".to_string()],
        })
    };

    let fresh_bypass = service
        .apply(&root(
            "supersede-fresh-bypass",
            supersede_action(vec![part_a.clone(), part_b.clone()]),
        ))
        .expect("fresh task rejection");
    assert_eq!(fresh_bypass.status, AgentActionStatus::Rejected);
    assert_eq!(
        fresh_bypass.error.expect("error").code,
        "task_has_no_failed_or_challenged_attempt"
    );

    service
        .apply(&managed(
            "supersede-claim",
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: source.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    service
        .apply(&supervisor(
            "supersede-attempt-failed",
            &format!("execution:{worker}"),
            AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                task_ref: source.clone(),
                execution_id: format!("execution:{worker}"),
                mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                reason: "provider returned a reproducible counterexample".to_string(),
                retryable: true,
            }),
        ))
        .expect("attempt failure");

    let cycle_rejected = service
        .apply(&root(
            "supersede-cycle",
            supersede_action(vec![cyclic.clone()]),
        ))
        .expect("cycle rejection");
    assert_eq!(cycle_rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        cycle_rejected.error.expect("error").code,
        "replacement_depends_on_superseded_task"
    );
    let agent_rejected = service
        .apply(&managed(
            "supersede-by-worker",
            &team,
            &worker,
            supersede_action(vec![part_a.clone(), part_b.clone()]),
        ))
        .expect("actor rejection");
    assert_eq!(agent_rejected.status, AgentActionStatus::Rejected);
    assert_eq!(
        agent_rejected.error.expect("error").code,
        "task_supersede_not_delegated"
    );

    let current_revision = service.project("program-1").expect("projection").revision;
    let mut stale = root(
        "supersede-stale",
        supersede_action(vec![part_a.clone(), part_b.clone()]),
    );
    stale.expected_revision = Some(current_revision.saturating_sub(1));
    let stale = service.apply(&stale).expect("stale rejection");
    assert_eq!(stale.status, AgentActionStatus::Rejected);
    assert_eq!(stale.error.expect("error").code, "stale_revision");

    let mut supersede = root(
        "supersede-commit",
        supersede_action(vec![part_a.clone(), part_b.clone()]),
    );
    supersede.expected_revision = Some(current_revision);
    let applied = service.apply(&supersede).expect("supersede");
    assert_eq!(applied.status, AgentActionStatus::Applied);
    assert_eq!(applied.changed_refs, vec![source.clone()]);
    let duplicate = service.apply(&supersede).expect("idempotent replay");
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.revision, applied.revision);
    let audit_event = store
        .list_stream("agentic-program:program-1")
        .expect("audit stream")
        .into_iter()
        .find(|event| {
            event
                .payload
                .pointer("/envelope/action_id")
                .and_then(serde_json::Value::as_str)
                == Some("supersede-commit")
        })
        .expect("supersede audit event");
    assert!(audit_event
        .refs
        .iter()
        .any(|reference| reference.kind == "replacement_task" && reference.id == part_a));
    assert!(audit_event.refs.iter().any(|reference| {
        reference.kind == "evidence" && reference.id == "tool://failure-receipt"
    }));

    let restarted = AgentActionService::new(store);
    let recovered = restarted
        .project("program-1")
        .expect("recovered projection");
    let retired = &recovered.tasks[&source];
    assert_eq!(retired.status, AgenticTaskStatus::Superseded);
    assert_eq!(
        retired.replacement_task_refs,
        vec![part_a.clone(), part_b.clone()]
    );
    assert_eq!(retired.failed_attempts, 1);
    assert_eq!(
        retired.supersede_evidence_refs,
        vec!["tool://failure-receipt"]
    );
    assert_eq!(retired.superseded_by.as_deref(), Some("root-1"));

    let reclaim = restarted
        .apply(&managed(
            "supersede-reclaim",
            &team,
            &worker,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: source.clone(),
                reason: None,
            }),
        ))
        .expect("retired source rejection");
    assert_eq!(reclaim.status, AgentActionStatus::Rejected);
    assert_eq!(reclaim.error.expect("error").code, "task_not_claimable");

    let final_artifact_ref = "artifact:successor-final".to_string();
    let mut incomplete_projection = recovered.clone();
    incomplete_projection.artifacts.insert(
        final_artifact_ref.clone(),
        super::super::program::AgenticArtifactProjection {
            artifact_ref: final_artifact_ref.clone(),
            content_ref: "artifact://accepted-evidence".to_string(),
            kind: "final".to_string(),
            title: "Premature replacement synthesis".to_string(),
            relates_to: vec![part_a.clone(), part_b.clone()],
            committed_by: "root-1".to_string(),
        },
    );
    assert!(
        super::super::supervision::completion_gap(
            &incomplete_projection,
            &ObjectiveCompleteRequestInput {
                final_artifact_ref: final_artifact_ref.clone(),
                evidence_refs: vec!["tool://failure-receipt".to_string()],
                unresolved: Vec::new(),
            },
        )
        .is_some_and(|gap| gap.starts_with("tasks_not_accepted:")),
        "retiring the failed source must not waive unfinished successor Tasks"
    );

    let mut supervisor_projection = recovered;
    for successor in [&part_a, &part_b, &cyclic] {
        let task = supervisor_projection
            .tasks
            .get_mut(successor)
            .expect("successor");
        task.status = AgenticTaskStatus::Accepted;
        task.claimant = Some("agent:author".to_string());
        task.reviewed_by = Some("agent:reviewer".to_string());
        task.artifact_refs = vec![final_artifact_ref.clone()];
        task.evidence_refs = vec!["tool://accepted-evidence".to_string()];
    }
    supervisor_projection.artifacts.insert(
        final_artifact_ref.clone(),
        super::super::program::AgenticArtifactProjection {
            artifact_ref: final_artifact_ref.clone(),
            content_ref: "artifact://accepted-evidence".to_string(),
            kind: "final".to_string(),
            title: "Replacement synthesis".to_string(),
            relates_to: vec![part_a, part_b, cyclic],
            committed_by: "agent:author".to_string(),
        },
    );
    assert_eq!(
        super::super::supervision::completion_gap(
            &supervisor_projection,
            &ObjectiveCompleteRequestInput {
                final_artifact_ref,
                evidence_refs: vec!["tool://accepted-evidence".to_string()],
                unresolved: Vec::new(),
            },
        ),
        None,
        "ObjectiveSupervisor must require accepted active successors while retaining, not accepting, the superseded source"
    );
}
