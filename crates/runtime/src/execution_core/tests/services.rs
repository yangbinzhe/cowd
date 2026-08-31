use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentEvaluationContract, AgentExecutorPolicy, AgentModelPolicy,
    AgentOutputContract, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
    CognitiveReadScope, CognitiveWriteMode, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    RevisionSelector,
};
use harness_contract::context::ChildExecutionBudgetReservation;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
};
use harness_contract::mission::ScheduleTrigger;
use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
};
use harness_contract::team::{
    RoleCardinalityPolicy, TeamInstantiationRequest, TeamRoleCardinalityOverride,
    TeamSelectionMode, TeamTemplateDefinitionId, TeamTemplateSelector,
};
use session::SessionRecord;

struct ReadinessOnlyEvolutionEvalRunner;

#[async_trait::async_trait]
impl crate::EvolutionEvalRunner for ReadinessOnlyEvolutionEvalRunner {
    async fn evaluate(
        &self,
        _candidate: &crate::EvolutionGovernanceCandidate,
    ) -> Result<crate::EvolutionComparisonReportV2, String> {
        Err("readiness-only test runner must not execute evaluation".to_string())
    }
}

fn approved_evolution_proposal(services: &RuntimeServices, suffix: &str) -> String {
    let signal = services
        .record_evolution_signal(crate::EvolutionSignal::eval_failure(
            format!("episode-baseline-{suffix}"),
            vec![harness_contract::reality::EvidenceRef::observed(
                "test",
                format!("episode-baseline-{suffix}"),
            )],
        ))
        .expect("evolution signal");
    let proposal = services
        .create_evolution_lifecycle(vec![signal.signal_id])
        .expect("evolution proposal")
        .proposal;
    let principal = crate::security::test_human_interactive_principal();
    let digest = services
        .evolution_proposal_decision_digest(&proposal.proposal_id, "approved")
        .expect("proposal decision digest");
    let lease = crate::security::test_verified_decision_lease(
        &format!("evolution-proposal:{}", proposal.proposal_id),
        "proposal.decision.approved",
        &format!("evolution.proposal:{}", proposal.proposal_id),
        &digest,
    );
    services
        .decide_evolution_proposal(&principal, &lease, &proposal.proposal_id, "approved")
        .expect("proposal approval");
    proposal.proposal_id
}

fn episode_baseline_signature() -> harness_contract::evolution::CollaborationSemanticSignature {
    harness_contract::evolution::CollaborationSemanticSignature {
        normalizer_revision: 1,
        workstream_shapes: Vec::new(),
        dependency_shapes: Vec::new(),
        required_capability_ids: vec!["read".to_string()],
        required_skill_ids: Vec::new(),
        required_tool_capabilities: Vec::new(),
        acceptance_kinds: vec!["evidence".to_string()],
        result_field_shapes: vec!["summary".to_string()],
    }
    .normalized()
}

fn append_episode_baseline_fixture(
    services: &RuntimeServices,
    program_id: &str,
    turn_ref_hash: &str,
) -> String {
    let episode_id = harness_contract::evolution::CollaborationExperienceEpisode::deterministic_id(
        program_id, 1,
    );
    let episode = harness_contract::evolution::CollaborationExperienceEpisode {
        schema_version: harness_contract::evolution::COLLABORATION_EXPERIENCE_SCHEMA_VERSION,
        episode_id: episode_id.clone(),
        session_ref_hash: "sha256:session".to_string(),
        turn_ref_hash: turn_ref_hash.to_string(),
        program_id: program_id.to_string(),
        program_revision: 1,
        intent_digest: "sha256:intent".to_string(),
        binding_digest: "sha256:binding".to_string(),
        capacity_profile_digest: "sha256:capacity".to_string(),
        approval_policy_digest: "sha256:approval".to_string(),
        semantic_signature: episode_baseline_signature(),
        outcome: harness_contract::evolution::CollaborationExperienceOutcome::Completed,
        evidence_refs: vec![format!("evidence:sha256:{program_id}")],
        coverage: harness_contract::evolution::CollaborationEvidenceCoverage {
            required_obligation_count: 1,
            satisfied_obligation_count: 1,
            coverage_basis_points: 10_000,
            reusable: true,
        },
        latency_ms: 1,
        resource_summary: harness_contract::evolution::CollaborationResourceSummary {
            parallel_demand: 1,
            context_reservation_tokens: 1,
            output_reservation_tokens: 1,
        },
        completed_at_ms: 1,
    };
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: format!("evolution:experience:{episode_id}"),
            scope: crate::RuntimeEventScope::Evolution,
            kind: "evolution.collaboration_experience.recorded.v1".to_string(),
            status: Some("eligible".to_string()),
            actor: Some("test".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"episode": episode}),
        })
        .expect("episode fixture");
    episode_id
}

#[test]
fn builder_rejects_a_clean_but_unaddressable_build_identity() {
    let invalid = harness_contract::outcome::RuntimeBuildIdentity::new(
        env!("CARGO_PKG_VERSION"),
        "unknown",
        false,
    );
    let result = RuntimeServices::builder("non-empty-home", "non-empty-workspace")
        .runtime_build_identity(invalid)
        .build();
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("clean Runtime identity requires a full Git object ID"),
    };
    assert!(
        matches!(error, RuntimeServicesError::Invariant(message) if message.contains("Git SHA"))
    );
}

#[test]
fn evolution_evaluation_single_flight_rejects_without_waiting() {
    let active = Arc::new(Mutex::new(BTreeSet::new()));
    let first =
        EvolutionEvaluationFlight::try_acquire(Arc::clone(&active), "candidate").expect("first");
    assert!(matches!(
        EvolutionEvaluationFlight::try_acquire(Arc::clone(&active), "candidate"),
        Err(RuntimeServicesError::Invariant(message))
            if message == "evolution_evaluation_in_progress"
    ));
    drop(first);
    EvolutionEvaluationFlight::try_acquire(active, "candidate").expect("released");
}

#[test]
fn in_memory_services_reclaim_their_filesystem_state() {
    let root = {
        let services = RuntimeServices::in_memory().expect("in-memory runtime services");
        let root = services
            ._ephemeral_root
            .as_ref()
            .expect("in-memory services own a temporary root")
            .path()
            .to_path_buf();
        assert!(root.exists());
        root
    };

    assert!(
        !root.exists(),
        "dropping the final RuntimeServices owner must remove {root:?}"
    );
}

#[test]
fn episode_baseline_cannot_bypass_an_active_stable_release() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let published = services
        .agent_runtime()
        .catalog()
        .all()
        .into_iter()
        .next()
        .expect("bootstrap catalog has an active Stable Agent");
    let proposal_id = approved_evolution_proposal(&services, "published-baseline");
    let signature_digest = episode_baseline_signature().digest();
    let episode_ids = vec![
        "experience:one".to_string(),
        "experience:two".to_string(),
        "experience:three".to_string(),
    ];
    let aggregate_digest = harness_contract::evolution::collaboration_episode_set_digest(
        &signature_digest,
        &episode_ids,
    );
    let error = services
        .register_evolution_candidate(crate::EvolutionCandidateIntent {
            candidate_id: "candidate-episode-stable-bypass".to_string(),
            proposal_id,
            subject: crate::EvolutionCandidateSubject::AgentDefinition {
                revision_ref: published.definition_ref,
            },
            evaluation_baseline: crate::EvolutionEvaluationBaseline::EpisodeSet {
                semantic_signature_digest: signature_digest,
                episode_ids,
                aggregate_digest,
            },
            source_evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test",
                "episode-stable-bypass",
            )],
            canary_policy: Default::default(),
        })
        .expect_err("an EpisodeSet cannot replace an existing Stable baseline");
    assert!(matches!(
        error,
        RuntimeServicesError::Invariant(message)
            if message.contains("forbidden when an active Stable baseline exists")
    ));
}

#[test]
fn episode_baseline_requires_three_distinct_durable_turns() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let proposal_id = approved_evolution_proposal(&services, "duplicate-turns");
    let episode_ids = ["one", "two", "three"]
        .into_iter()
        .map(|suffix| {
            append_episode_baseline_fixture(
                &services,
                &format!("program-{suffix}"),
                "sha256:the-same-turn",
            )
        })
        .collect::<Vec<_>>();
    let signature_digest = episode_baseline_signature().digest();
    let aggregate_digest = harness_contract::evolution::collaboration_episode_set_digest(
        &signature_digest,
        &episode_ids,
    );
    let definition_id = AgentDefinitionId::new(
        DefinitionScope::Workspace,
        "cowd/episode-without-published-baseline",
    )
    .expect("definition id");
    let revision_ref = harness_contract::agent::AgentDefinitionRevisionRef::new(definition_id, 1)
        .expect("revision ref");
    let error = services
        .register_evolution_candidate(crate::EvolutionCandidateIntent {
            candidate_id: "candidate-episode-duplicate-turns".to_string(),
            proposal_id,
            subject: crate::EvolutionCandidateSubject::AgentDefinition { revision_ref },
            evaluation_baseline: crate::EvolutionEvaluationBaseline::EpisodeSet {
                semantic_signature_digest: signature_digest,
                episode_ids,
                aggregate_digest,
            },
            source_evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test",
                "episode-duplicate-turns",
            )],
            canary_policy: Default::default(),
        })
        .expect_err("three episode ids from one Turn are not a reusable baseline");
    assert!(matches!(
        error,
        RuntimeServicesError::Invariant(message)
            if message.contains("eligible episodes from three distinct Turns")
    ));
}

#[test]
fn startup_recovers_task_outbox_without_mutating_mission_membership() {
    let root = tempfile::tempdir().expect("runtime root");
    let home = root.path().join("home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let first = RuntimeServices::builder(&home, &workspace)
        .build()
        .expect("first runtime");
    publish_team_test_policy(&first, "session-startup-recovery");
    let task_spec = first
        .task_runtime_port()
        .bind_task_spec(
            "session-startup-recovery",
            None,
            harness_contract::task::TaskSpec::new("recover committed task side effects"),
        )
        .expect("bind startup recovery Task policy");
    let mission_id = first.mission_runtime().default_mission_id().to_string();
    first
        .task_aggregate_service()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "task-startup-recovery".to_string(),
            mission_id: mission_id.clone(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::User,
            origin_session_id: "session-startup-recovery".to_string(),
            origin_turn_id: "turn-startup-recovery".to_string(),
            root_task_id: "task-startup-recovery".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: task_spec,
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test_fixture",
                "test://task/startup-recovery",
            )],
        })
        .expect("commit Task without running its Runtime port");
    assert!(first
        .event_reader()
        .list_stream("task:task-startup-recovery")
        .expect("task event stream")
        .is_empty());
    assert_eq!(
        first
            .task_aggregate_service()
            .pending_outbox(None, 10)
            .expect("pending outbox")
            .len(),
        1
    );
    drop(first);

    let recovered = RuntimeServices::builder(&home, &workspace)
        .build()
        .expect("recovered runtime");
    assert_eq!(
        recovered
            .task_aggregate_service()
            .pending_outbox(None, 10)
            .expect("drained outbox")
            .len(),
        0,
        "startup recovery must mark the projected Task evidence outbox as drained"
    );
    assert_eq!(
        recovered
            .event_reader()
            .list_stream("task:task-startup-recovery")
            .expect("projected Task event")
            .len(),
        1
    );
    let recovered_task = recovered
        .task_aggregate_service()
        .get("task-startup-recovery")
        .expect("Task lookup")
        .expect("Task survives restart");
    assert_eq!(recovered_task.mission_id, mission_id);
    assert_eq!(recovered_task.origin_session_id, "session-startup-recovery");
}

#[tokio::test]
async fn maintenance_supervisor_serializes_owner_tasks_and_drains_them() {
    let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
    let order = Arc::new(Mutex::new(Vec::new()));
    let first_order = Arc::clone(&order);
    assert!(
        supervisor
            .submit("session-a".to_string(), async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                first_order
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(1);
            })
            .await
    );
    let second_order = Arc::clone(&order);
    assert!(
        supervisor
            .submit("session-a".to_string(), async move {
                second_order
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(2);
            })
            .await
    );

    assert_eq!(supervisor.tracked_task_count(), 1);
    supervisor.shutdown_and_drain().await;
    assert_eq!(supervisor.tracked_task_count(), 0);
    assert_eq!(
        *order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![1, 2]
    );
}

#[tokio::test]
async fn maintenance_supervisor_serializes_concurrent_submissions_per_owner() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
    let start = Arc::new(tokio::sync::Barrier::new(9));
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let mut submitters = Vec::new();
    for _ in 0..8 {
        let supervisor = Arc::clone(&supervisor);
        let start = Arc::clone(&start);
        let active = Arc::clone(&active);
        let maximum = Arc::clone(&maximum);
        let completed = Arc::clone(&completed);
        submitters.push(tokio::spawn(async move {
            start.wait().await;
            assert!(
                supervisor
                    .submit("session-a".to_string(), async move {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        maximum.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        completed.fetch_add(1, Ordering::SeqCst);
                    })
                    .await
            );
        }));
    }
    start.wait().await;
    for submitter in submitters {
        submitter.await.expect("submitter joins");
    }
    supervisor.shutdown_and_drain().await;
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert_eq!(completed.load(Ordering::SeqCst), 8);
    assert_eq!(supervisor.tracked_task_count(), 0);
}

#[tokio::test]
async fn maintenance_supervisor_backpressures_instead_of_growing_owner_queue() {
    let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let release_work = Arc::clone(&release);
    assert!(
        supervisor
            .submit("session-bounded".to_string(), async move {
                release_work.notified().await;
            })
            .await
    );
    for _ in 0..MAX_QUEUED_MAINTENANCE_PER_OWNER {
        assert!(
            supervisor
                .submit("session-bounded".to_string(), async {})
                .await
        );
    }

    let overflow = {
        let supervisor = Arc::clone(&supervisor);
        tokio::spawn(async move {
            supervisor
                .submit("session-bounded".to_string(), async {})
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !overflow.is_finished(),
        "the first overflow item must wait for bounded capacity"
    );

    release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(1), overflow)
        .await
        .expect("capacity is released")
        .expect("overflow submitter joins"));
    supervisor.shutdown_and_drain().await;
    assert_eq!(supervisor.tracked_task_count(), 0);
}

#[tokio::test]
async fn maintenance_supervisor_rejects_work_after_shutdown_starts() {
    let supervisor = Arc::new(RuntimeMaintenanceSupervisor::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let release_work = Arc::clone(&release);
    assert!(
        supervisor
            .submit("session-a".to_string(), async move {
                release_work.notified().await;
            })
            .await
    );

    let draining = {
        let supervisor = Arc::clone(&supervisor);
        tokio::spawn(async move { supervisor.shutdown_and_drain().await })
    };
    tokio::task::yield_now().await;
    let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let executed_work = Arc::clone(&executed);
    assert!(
        !supervisor
            .submit("session-b".to_string(), async move {
                executed_work.store(true, std::sync::atomic::Ordering::SeqCst);
            })
            .await
    );
    release.notify_waiters();
    draining.await.expect("shutdown joins");
    assert!(!executed.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn maintenance_supervisor_contains_panics_and_reclaims_owner() {
    let supervisor = RuntimeMaintenanceSupervisor::new();
    assert!(
        supervisor
            .submit("session-a".to_string(), async move {
                panic!("maintenance failure");
            })
            .await
    );
    supervisor.shutdown_and_drain().await;
    assert_eq!(supervisor.tracked_task_count(), 0);
}

#[tokio::test]
async fn maintenance_supervisor_reclaims_idle_owner_without_shutdown() {
    let supervisor = RuntimeMaintenanceSupervisor::new();
    let completed = Arc::new(tokio::sync::Notify::new());
    let completed_work = Arc::clone(&completed);
    assert!(
        supervisor
            .submit("session-a".to_string(), async move {
                completed_work.notify_one();
            })
            .await
    );
    completed.notified().await;
    tokio::time::timeout(Duration::from_millis(100), async {
        while supervisor.tracked_task_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("idle owner is reclaimed");
    supervisor.shutdown_and_drain().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_supervisor_never_strands_immediate_owner_completion() {
    let supervisor = RuntimeMaintenanceSupervisor::new();
    for index in 0..128 {
        assert!(
            supervisor
                .submit(format!("immediate-{index}"), async {})
                .await
        );
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while supervisor.tracked_task_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every immediate owner is reaped");
    supervisor.shutdown_and_drain().await;
}

#[tokio::test]
async fn maintenance_supervisor_aborts_timed_out_work() {
    let supervisor = RuntimeMaintenanceSupervisor::with_shutdown_timeout(Duration::from_millis(10));
    assert!(
        supervisor
            .submit("session-a".to_string(), std::future::pending())
            .await
    );
    tokio::time::timeout(Duration::from_millis(100), supervisor.shutdown_and_drain())
        .await
        .expect("bounded shutdown");
    assert_eq!(supervisor.tracked_task_count(), 0);
}

#[test]
fn task_terminal_observation_is_idempotent_without_becoming_a_task_writer() {
    let services = RuntimeServices::in_memory().expect("in-memory runtime services");
    publish_team_test_policy(&services, "session-completion-1");
    let task_spec = services
        .task_runtime_port()
        .bind_task_spec(
            "session-completion-1",
            None,
            harness_contract::task::TaskSpec::new("observe assignment completion"),
        )
        .expect("bind observed Task policy");
    services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "task-completion-1".to_string(),
            mission_id: services
                .task_runtime_port()
                .workspace_default_mission_id()
                .to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::User,
            origin_session_id: "session-completion-1".to_string(),
            origin_turn_id: "turn-completion-1".to_string(),
            root_task_id: "task-completion-1".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: task_spec,
            evidence_refs: Vec::new(),
        })
        .expect("create observed Task");
    let first = services
        .task_runtime_port()
        .record_assignment_terminal_observation(
            "task-completion-1",
            "completed",
            "runtime-event://source",
            "correlation-1",
        )
        .expect("first observation");
    let replay = services
        .task_runtime_port()
        .record_assignment_terminal_observation(
            "task-completion-1",
            "completed",
            "runtime-event://source",
            "correlation-1",
        )
        .expect("idempotent observation replay");
    assert_eq!(first.event_id, replay.event_id);
    assert_eq!(first.commit_cursor, replay.commit_cursor);
    assert_eq!(first.scope, RuntimeEventScope::Relation);
    assert_eq!(
        first.kind,
        "application.assignment.task_terminal_observed.v1"
    );
    assert_eq!(
        services
            .event_store
            .list_stream("task-observation:task-completion-1")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn concurrent_task_terminal_observation_replays_the_committed_receipt() {
    let services =
        std::sync::Arc::new(RuntimeServices::in_memory().expect("in-memory runtime services"));
    publish_team_test_policy(&services, "session-completion-race");
    let task_spec = services
        .task_runtime_port()
        .bind_task_spec(
            "session-completion-race",
            None,
            harness_contract::task::TaskSpec::new("observe one assignment completion concurrently"),
        )
        .expect("bind concurrently observed Task policy");
    services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "task-completion-race".to_string(),
            mission_id: services
                .task_runtime_port()
                .workspace_default_mission_id()
                .to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::User,
            origin_session_id: "session-completion-race".to_string(),
            origin_turn_id: "turn-completion-race".to_string(),
            root_task_id: "task-completion-race".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: task_spec,
            evidence_refs: Vec::new(),
        })
        .expect("create concurrently observed Task");
    let workers = (0..16)
        .map(|_| {
            let services = std::sync::Arc::clone(&services);
            std::thread::spawn(move || {
                services
                    .task_runtime_port()
                    .record_assignment_terminal_observation(
                        "task-completion-race",
                        "completed",
                        "runtime-event://source-race",
                        "correlation-race",
                    )
            })
        })
        .collect::<Vec<_>>();

    let receipts = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker join").expect("observation"))
        .collect::<Vec<_>>();
    let first_event_id = receipts[0].event_id.clone();
    assert!(receipts
        .iter()
        .all(|receipt| receipt.event_id == first_event_id));
    assert_eq!(
        services
            .event_store
            .list_stream("task-observation:task-completion-race")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn runtime_skill_catalog_is_available_to_delegated_execution_services() {
    let services = RuntimeServices::in_memory().expect("in-memory runtime services");
    let profile = SkillCapabilityProfile {
        skill_id: "delegated-review".to_string(),
        name: "Delegated Review".to_string(),
        version: Some("1.0.0".to_string()),
        source_root: "skill://delegated-review".to_string(),
        package_fingerprint: "sha256:delegated-review".to_string(),
        kind: SkillKind::Workflow,
        lifecycle_status: SkillLifecycleStatus::UsablePrompt,
        adapters: vec![SkillAdapterKind::PromptOnly],
        risk_level: SkillRiskLevel::Low,
        entrypoints: Vec::new(),
        inspection_summary: vec!["review source changes".to_string()],
        structured_dependencies: Vec::new(),
    };
    services.replace_skill_catalog(crate::RuntimeSkillCatalog::new(
        vec![profile],
        vec![crate::RuntimeSkillPromptAsset {
            skill_id: "delegated-review".to_string(),
            version: Some("1.0.0".to_string()),
            content: "Review evidence before returning.".to_string(),
            source_ref: "skill://delegated-review/SKILL.md".to_string(),
            tool_refs: Vec::new(),
        }],
    ));

    let catalog = services.skill_catalog();
    assert_eq!(catalog.profiles()[0].skill_id, "delegated-review");
    assert_eq!(
        catalog.prompt_assets()[0].source_ref,
        "skill://delegated-review/SKILL.md"
    );
}

struct TestExecutionHost;

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for TestExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some("ok".to_string()),
            error: None,
            evidence_ref: format!("evidence:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
        }
    }
}

struct ServiceScopedBackend {
    calls: Arc<AtomicUsize>,
}

struct CompletedAgentBackend;

struct ParallelTrackingAgentBackend {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl crate::AgentRuntimeBackend for CompletedAgentBackend {
    fn kind(&self) -> crate::AgentBackendKind {
        crate::AgentBackendKind::InProcess
    }

    fn capabilities(&self) -> crate::AgentBackendCapabilities {
        crate::AgentBackendCapabilities::in_process()
    }

    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: crate::AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let mut evidence_refs = packet.evidence_refs.clone();
        evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed(
                "tool",
                format!("materialized:{}", packet.node_id()),
            ),
            "a".repeat(64),
            1,
            "application/json",
            "artifact://art_runtime_services_packet",
            format!("session:{}", packet.session_id()),
        ));
        let mut evidence_obligations = packet
            .required_acceptance
            .evidence_obligations
            .iter()
            .collect::<Vec<_>>();
        // Canonical ToolHost receipts are causally ordered: a committed
        // write necessarily precedes its exact verification read. The
        // test backend must preserve that invariant instead of inheriting
        // an incidental lexical obligation order.
        evidence_obligations.sort_by_key(|obligation| match obligation.kind {
            harness_contract::context::EvidenceObligationKind::WriteEffect => 0,
            harness_contract::context::EvidenceObligationKind::VerifyAfterWrite => 1,
            _ => 2,
        });
        let observed_evidence = evidence_obligations
                .into_iter()
                .enumerate()
                .map(|(index, obligation)| {
                    let mut target = obligation.target.clone();
                    if let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
                        &mut target
                    {
                        if scope.coverage
                            == harness_contract::context::EvidenceCoverageKind::ScopedContent
                        {
                            scope.coverage =
                                harness_contract::context::EvidenceCoverageKind::ExactContent;
                        }
                        if matches!(
                            scope.coverage,
                            harness_contract::context::EvidenceCoverageKind::ExactContent
                                | harness_contract::context::EvidenceCoverageKind::WriteEffect
                        ) && scope.path.observed_revision_or_digest.is_none()
                        {
                            scope.path.observed_revision_or_digest = Some("a".repeat(64));
                        }
                    }
                    harness_contract::context::ObservedEvidence {
                        obligation_id: obligation.obligation_id.clone(),
                        target,
                        observed_at_sequence: u64::try_from(index + 1).unwrap_or(u64::MAX),
                        tool_name: "test_runtime_evidence".to_string(),
                        provenance:
                            harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
                        evidence_ref: None,
                        // This backend is the explicit observation-authority
                        // test double for a completed in-process Provider
                        // turn. Semantic obligations therefore carry the same
                        // typed attestation production would mint after a
                        // matching request receives a valid response.
                        model_observation: (obligation.observation_requirement
                            == harness_contract::context::EvidenceObservationRequirement::ProviderModel)
                            .then(|| harness_contract::context::ProviderModelObservationAttestation {
                                provider_invocation_id: format!(
                                    "test-provider:{}:{}",
                                    packet.node_id(),
                                    index + 1
                                ),
                                obligation_ids: vec![obligation.obligation_id.clone()],
                                raw_ref: harness_contract::context::EvidenceRef::observed(
                                    "tool",
                                    format!("test-provider-raw:{}:{}", packet.node_id(), index + 1),
                                ),
                                model_receipt_sha256: format!("sha256:{}", "c".repeat(64)),
                                raw_tokens: 1,
                                receipt_tokens: 1,
                                omitted_tokens: 0,
                                complete: true,
                                provider_request_sequence: u64::try_from(index + 2)
                                    .unwrap_or(u64::MAX),
                                provider_attempt: 1,
                                model: selection.model.clone(),
                            }),
                        workspace_prior_state: None,
                    }
                })
                .collect::<Vec<_>>();
        let runtime_change_receipts = packet
            .acceptance
            .iter()
            .any(|criterion| matches!(criterion.as_str(), "implementation" | "mitigation"))
            .then(|| {
                vec![harness_contract::agent::AgentChangeReceipt {
                    path: packet
                        .resource_scopes
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "fixture.txt".to_string()),
                    before_sha256: Some("b".repeat(64)),
                    after_sha256: "a".repeat(64),
                    write_sequence: 1,
                }]
            })
            .unwrap_or_default();
        let changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect();
        Ok(AgentReturnPacket {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            session_id: packet.session_id().to_string(),
            mission_id: packet.mission_id().to_string(),
            team_id: packet.team_id().map(ToString::to_string),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: serde_json::json!({
                "summary": "verified agent result",
                "evidence": "materialized durable tool evidence",
                "completed": "verified"
            })
            .to_string(),
            answer_candidate: None,
            observed_acceptance: harness_contract::context::ObservedAcceptance {
                satisfied_criteria: packet.acceptance.clone(),
                observed_evidence,
                unresolved_obligation_ids: Vec::new(),
            },
            acceptance_evaluation: None,
            acceptance: packet.acceptance,
            evidence_refs,
            changes,
            runtime_change_receipts,
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 5,
            output_tokens: 3,
            cached_tokens: 0,
            model: selection.model,
            provider: selection.provider,
            tool_calls: 1,
            duplicate_tool_calls: 0,
            max_tool_concurrency_observed: 1,
            parallel_tool_batches: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: Vec::new(),
            failure: None,
        })
    }
}

#[async_trait::async_trait]
impl crate::AgentRuntimeBackend for ParallelTrackingAgentBackend {
    fn kind(&self) -> crate::AgentBackendKind {
        crate::AgentBackendKind::InProcess
    }

    fn capabilities(&self) -> crate::AgentBackendCapabilities {
        crate::AgentBackendCapabilities::in_process()
    }

    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: crate::AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let mut evidence_refs = packet.evidence_refs.clone();
        evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed(
                "tool",
                format!("materialized:{}", packet.node_id()),
            ),
            "a".repeat(64),
            1,
            "application/json",
            "artifact://art_runtime_services_shared",
            format!("session:{}", packet.session_id()),
        ));
        Ok(AgentReturnPacket {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            session_id: packet.session_id().to_string(),
            mission_id: packet.mission_id().to_string(),
            team_id: packet.team_id().map(ToString::to_string),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status: AgentTerminalStatus::Completed,
            outcome: serde_json::json!({
                "summary": "parallel agent result",
                "evidence": "materialized durable tool evidence",
                "completed": "verified"
            })
            .to_string(),
            answer_candidate: None,
            observed_acceptance: harness_contract::context::ObservedAcceptance {
                satisfied_criteria: packet.acceptance.clone(),
                observed_evidence: Vec::new(),
                unresolved_obligation_ids: Vec::new(),
            },
            acceptance_evaluation: None,
            acceptance: packet.acceptance,
            evidence_refs,
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 5,
            output_tokens: 3,
            cached_tokens: 0,
            model: selection.model,
            provider: selection.provider,
            tool_calls: 1,
            duplicate_tool_calls: 0,
            max_tool_concurrency_observed: 1,
            parallel_tool_batches: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: Vec::new(),
            failure: None,
        })
    }
}

#[async_trait::async_trait]
impl super::super::graph::ScopedNodeBackend for ServiceScopedBackend {
    async fn execute(
        &self,
        ticket: &super::super::graph::NodeExecutionTicket,
    ) -> Result<super::super::graph::NodeExecutionOutcome, super::super::graph::NodeExecutorError>
    {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(super::super::graph::NodeExecutionOutcome::new(
            harness_contract::execution_graph::ExecutionNodeResult {
                status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                result_ref: Some(format!("service-result:{}", ticket.node_id)),
                summary: Some("service backend completed".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            },
        ))
    }
}

struct ServiceScopedResolver {
    payload_ref: String,
    backend: Arc<ServiceScopedBackend>,
}

impl super::super::graph::executors::ScopedNodeBackendResolver for ServiceScopedResolver {
    fn resolve(
        &self,
        ticket: &super::super::graph::NodeExecutionTicket,
    ) -> Option<Arc<dyn super::super::graph::ScopedNodeBackend>> {
        (ticket.payload_ref == self.payload_ref)
            .then(|| Arc::clone(&self.backend) as Arc<dyn super::super::graph::ScopedNodeBackend>)
    }
}

#[test]
fn workspace_instances_isolate_provider_tool_and_registry_ownership() {
    let left = RuntimeServices::in_memory().unwrap();
    let right = RuntimeServices::in_memory().unwrap();
    assert_ne!(left.workspace_key(), right.workspace_key());
    assert!(!Arc::ptr_eq(
        left.provider_registry(),
        right.provider_registry()
    ));
    assert!(!Arc::ptr_eq(
        left.executor_registry(),
        right.executor_registry()
    ));
    assert_eq!(
        left.executor_registry().available_kinds(),
        right.executor_registry().available_kinds()
    );
    assert!(left
        .executor_registry()
        .available_kinds()
        .contains("inline_model"));
    assert!(left
        .executor_registry()
        .available_kinds()
        .contains("tool_batch"));
    assert!(left
        .executor_registry()
        .available_kinds()
        .contains("session_dispatch"));
    assert!(left
        .executor_registry()
        .available_kinds()
        .contains("cross_plane_connector"));
    assert!(!Arc::ptr_eq(
        left.cross_plane_connector_executor(),
        right.cross_plane_connector_executor()
    ));
    assert!(!Arc::ptr_eq(left.scope_locks(), right.scope_locks()));
    assert!(!Arc::ptr_eq(
        left.worktree_leases(),
        right.worktree_leases()
    ));
}

#[test]
fn definition_catalog_refresh_only_exposes_active_stable_revisions() {
    let temp = tempfile::tempdir().expect("temporary root");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
        .build()
        .expect("runtime services");
    let definition_id =
        AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/reviewer").expect("definition id");
    let instructions = "# Reviewer\n\nReview evidence.\n";
    let digest = format!("{:x}", Sha256::digest(instructions.as_bytes()));
    let stored = services
        .definition_registry()
        .agents()
        .store_revision(
            AgentDefinitionManifest {
                api_version: "cowd.agent/v1".to_string(),
                definition_id: definition_id.clone(),
                revision: 1,
                name: "Reviewer".to_string(),
                description: "Reviews implementation evidence".to_string(),
                lifecycle: RevisionLifecycle::Published,
                executor: AgentExecutorPolicy::CowdNative,
                model_policy: AgentModelPolicy {
                    profile: "coding".to_string(),
                    allowed_models: vec!["test-model".to_string()],
                    fallback_allowed: true,
                },
                cognitive_policy: AgentCognitivePolicy {
                    context_profile: "team".to_string(),
                    read_scopes: vec![CognitiveReadScope::Session],
                    write_mode: CognitiveWriteMode::CandidateOnly,
                    team_working_state_visible: true,
                },
                capability_contract: AgentCapabilityContract {
                    capability_ceiling: vec![AgentCapability::Read],
                    skill_refs: Vec::new(),
                    approval_required_for: Vec::new(),
                },
                output_contract: AgentOutputContract::reviewable(),
                evaluation: AgentEvaluationContract::single_release_gate("review", "evidence"),
                instructions_digest: digest,
            },
            instructions,
        )
        .expect("stored revision");
    services
        .definition_registry()
        .agents()
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: stored.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/reviewer-v1".to_string(),
            },
            content_digest: stored.revision.content_digest.clone(),
        })
        .expect("active stable");
    services
        .refresh_definition_catalog()
        .expect("catalog refresh");
    let entry = services
        .agent_runtime()
        .catalog()
        .get(definition_id.as_str())
        .expect("runnable entry");
    assert_eq!(entry.definition_ref.revision, 1);
    assert_eq!(entry.capabilities, vec!["read"]);

    services
        .definition_registry()
        .agents()
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: stored.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Stopped,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/reviewer-v1".to_string(),
            },
            content_digest: stored.revision.content_digest,
        })
        .expect("stopped stable");
    services
        .refresh_definition_catalog()
        .expect("catalog refresh");
    assert!(services
        .agent_runtime()
        .catalog()
        .get(definition_id.as_str())
        .is_none());
}

#[test]
fn active_canary_routes_new_bindings_and_stop_reverts_to_stable() {
    let temp = tempfile::tempdir().expect("temporary root");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
        .evolution_eval_runner(Arc::new(ReadinessOnlyEvolutionEvalRunner))
        .build()
        .expect("runtime services");
    let definition_id =
        AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/canary").expect("definition id");
    let instructions = "# Canary\n\nReturn evidence-backed review output.\n";
    let evaluation = AgentEvaluationContract {
        scenario_refs: vec!["canary/review".to_string()],
        metrics: vec![
            harness_contract::agent::EvaluationMetricSpec::release_gate(
                "canary/review",
                "contract",
                true,
                true,
            ),
            harness_contract::agent::EvaluationMetricSpec::release_gate(
                "canary/review",
                "evidence",
                true,
                false,
            ),
        ],
    };
    let manifest = |revision| AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: definition_id.clone(),
        revision,
        name: format!("Canary {revision}"),
        description: "Canary routing fixture".to_string(),
        lifecycle: RevisionLifecycle::Published,
        executor: AgentExecutorPolicy::CowdNative,
        model_policy: AgentModelPolicy {
            profile: "test".to_string(),
            allowed_models: Vec::new(),
            fallback_allowed: true,
        },
        cognitive_policy: AgentCognitivePolicy {
            context_profile: "sub_agent".to_string(),
            read_scopes: vec![CognitiveReadScope::Session],
            write_mode: CognitiveWriteMode::CandidateOnly,
            team_working_state_visible: false,
        },
        capability_contract: AgentCapabilityContract {
            capability_ceiling: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            approval_required_for: Vec::new(),
        },
        output_contract: AgentOutputContract::reviewable(),
        evaluation: evaluation.clone(),
        instructions_digest: format!("{:x}", Sha256::digest(instructions.as_bytes())),
    };
    let baseline = services
        .definition_registry()
        .agents()
        .store_revision(manifest(1), instructions)
        .expect("baseline revision");
    let candidate_revision = services
        .definition_registry()
        .agents()
        .store_revision(manifest(2), instructions)
        .expect("candidate revision");
    services
        .definition_registry()
        .agents()
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: baseline.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/canary-baseline".to_string(),
            },
            content_digest: baseline.revision.content_digest.clone(),
        })
        .expect("baseline stable");
    let signal = services
        .record_evolution_signal(crate::EvolutionSignal::eval_failure(
            "canary-fixture",
            vec![harness_contract::reality::EvidenceRef::observed(
                "agent_run",
                "baseline",
            )],
        ))
        .expect("signal");
    let proposal = services
        .create_evolution_lifecycle(vec![signal.signal_id])
        .expect("proposal")
        .proposal;
    let principal = crate::security::test_human_interactive_principal();
    let digest = services
        .evolution_proposal_decision_digest(&proposal.proposal_id, "approved")
        .expect("proposal digest");
    let proposal_lease = crate::security::test_verified_decision_lease(
        &format!("evolution-proposal:{}", proposal.proposal_id),
        "proposal.decision.approved",
        &format!("evolution.proposal:{}", proposal.proposal_id),
        &digest,
    );
    services
        .decide_evolution_proposal(
            &principal,
            &proposal_lease,
            &proposal.proposal_id,
            "approved",
        )
        .expect("proposal approved");
    let candidate = services
        .register_evolution_candidate(crate::EvolutionCandidateIntent {
            candidate_id: "candidate-canary-v2".to_string(),
            proposal_id: proposal.proposal_id,
            subject: crate::EvolutionCandidateSubject::AgentDefinition {
                revision_ref: candidate_revision.revision.revision_ref.clone(),
            },
            evaluation_baseline: crate::EvolutionEvaluationBaseline::PublishedRevision {
                subject_ref: format!("agent-definition:{}", definition_id.as_str()),
                revision: 1,
                content_digest: baseline.revision.content_digest.clone(),
            },
            source_evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "agent_run",
                "baseline",
            )],
            canary_policy: crate::CanaryRolloutPolicy {
                traffic_basis_points: 10_000,
                minimum_samples: 1,
                minimum_duration_ms: 1,
                maximum_duration_ms: 60_000,
            },
        })
        .expect("candidate registered");
    services
        .evolution_governance
        .record_comparison(crate::EvolutionComparisonReportV2 {
            report_id: "canary-fixture-report".to_string(),
            candidate_id: candidate.candidate_id.clone(),
            evaluation_contract_digest: candidate.evaluation_contract_digest(),
            evaluation_policy_digest: candidate.evaluation_policy_floor.digest(),
            evaluation_scenario_digest: candidate.evaluation_scenario_digest.clone(),
            subject_ref: candidate.subject.subject_ref(),
            environment_fingerprint: "sha256:test-environment".to_string(),
            stopping_reason:
                harness_contract::evaluation::EvaluationStoppingReason::FixedSamplesCompleted,
            executed_sample_count: 10,
            dimensions: vec![
                crate::EvolutionComparisonDimension {
                    metric_id: "evidence".to_string(),
                    direction: crate::EvaluationDirection::HigherIsBetter,
                    baseline: 1.0,
                    candidate: 1.0,
                    non_inferiority_margin: 0.0,
                    sample_count: 10,
                    minimum_samples: 10,
                    confidence: 1.0,
                    minimum_confidence: 0.9,
                    minimum_improvement: 0.01,
                    superiority_confidence: 1.0,
                    minimum_superiority_confidence: 0.9,
                    hard_gate: true,
                    protected: true,
                    target_improvement: false,
                },
                crate::EvolutionComparisonDimension {
                    metric_id: "contract".to_string(),
                    direction: crate::EvaluationDirection::HigherIsBetter,
                    baseline: 1.0,
                    // The immutable contract marks this as a target-improvement
                    // metric, so equality is intentionally not eligible for a
                    // Canary review.
                    candidate: 1.01,
                    non_inferiority_margin: 0.0,
                    sample_count: 10,
                    minimum_samples: 10,
                    confidence: 1.0,
                    minimum_confidence: 0.9,
                    minimum_improvement: 0.01,
                    superiority_confidence: 1.0,
                    minimum_superiority_confidence: 0.9,
                    hard_gate: true,
                    protected: true,
                    target_improvement: true,
                },
            ],
            source_run_refs: vec!["eval:paired".to_string()],
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "evaluation",
                "paired",
            )],
            created_at_ms: 1,
        })
        .expect("eligible comparison");
    let review = services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("canary review");
    let lease = crate::security::test_verified_decision_lease(
        &review.review_id,
        review.action_key(),
        review.subject.scope_ref(),
        review.evidence_digest(),
    );
    services
        .decide_evolution_release_review(
            &principal,
            &lease,
            &review.review_id,
            crate::ReleaseChangeReviewDecision::Approve,
            "approve canary fixture".to_string(),
        )
        .expect("canary approved");

    let mut request = AgentBindingRequest::new(
        definition_id.clone(),
        RevisionSelector::LatestApprovedStable,
        "instance:canary-a",
        "session:canary",
        "task:canary",
    );
    request.granted_capabilities = vec![AgentCapability::Read];
    let routed = services
        .compile_agent_binding(request)
        .expect("canary binding");
    assert_eq!(routed.snapshot.definition_ref.revision, 2);
    assert_eq!(
        routed
            .snapshot
            .release
            .as_ref()
            .map(|release| release.channel),
        Some(ReleaseChannel::Canary)
    );

    let stop = services
        .request_evolution_release_change(crate::ReleaseChangeRequest {
            request_id: "stop-canary-fixture".to_string(),
            subject: candidate.subject.clone(),
            action: crate::ReleaseChangeAction::StopCanary,
            selector: None,
            candidate_id: Some(candidate.candidate_id.clone()),
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "incident", "fixture",
            )],
        })
        .expect("stop review");
    let stop_lease = crate::security::test_verified_decision_lease(
        &stop.review_id,
        stop.action_key(),
        stop.subject.scope_ref(),
        stop.evidence_digest(),
    );
    services
        .decide_evolution_release_review(
            &principal,
            &stop_lease,
            &stop.review_id,
            crate::ReleaseChangeReviewDecision::Approve,
            "stop canary fixture".to_string(),
        )
        .expect("stopped canary");

    let mut request = AgentBindingRequest::new(
        definition_id,
        RevisionSelector::LatestApprovedStable,
        "instance:canary-b",
        "session:canary",
        "task:stable",
    );
    request.granted_capabilities = vec![AgentCapability::Read];
    let stable = services
        .compile_agent_binding(request)
        .expect("stable binding");
    assert_eq!(stable.snapshot.definition_ref.revision, 1);
    assert!(stable.snapshot.release.is_none());
}

#[test]
fn explicit_toml_import_is_runtime_owned_and_never_enters_runnable_catalog() {
    let temp = tempfile::tempdir().expect("temporary root");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
        .build()
        .expect("runtime services");
    let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "external/reviewer")
        .expect("definition id");

    let receipt = services
        .import_agent_toml_draft(crate::agent::definition::ExplicitTomlAgentImport {
            definition_id: definition_id.clone(),
            revision: 1,
            source_label: "manual:/tmp/external-reviewer.toml".to_string(),
            toml: "name = 'External reviewer'\nmodel = 'review-model'\n".to_string(),
        })
        .expect("runtime import");

    assert_eq!(receipt.revision_ref.definition_id, definition_id);
    assert_eq!(receipt.revision_ref.revision, 1);
    assert!(!receipt.content_digest.is_empty());
    services
        .refresh_definition_catalog()
        .expect("catalog refresh");
    assert!(services
        .agent_runtime()
        .catalog()
        .get("workspace/external/reviewer")
        .is_none());
}

#[test]
fn builder_rejects_partial_session_port_sets() {
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
    let mut builder = RuntimeServices::builder(temp.path(), temp.path().join("partial"));
    builder.session_query_port = Some(ports);
    let result = builder.build();

    assert!(matches!(
        result,
        Err(RuntimeServicesError::IncompleteSessionPorts)
    ));
}

#[test]
fn workspace_builders_isolate_provider_tool_host_and_session_router() {
    let temp = tempfile::tempdir().unwrap();
    let left_provider = Arc::new(crate::ProviderRegistry::empty());
    let right_provider = Arc::new(crate::ProviderRegistry::empty());
    let left_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
    let right_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
    let left_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let right_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    std::fs::create_dir_all(temp.path().join("left")).unwrap();
    std::fs::create_dir_all(temp.path().join("right")).unwrap();

    let left_ports =
        crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&left_store));
    let right_ports =
        crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&right_store));
    let left = RuntimeServices::builder(temp.path(), temp.path().join("left"))
        .provider_registry(Arc::clone(&left_provider))
        .tool_execution_host(Arc::clone(&left_tool))
        .session_ports(
            left_ports.clone(),
            left_ports.clone(),
            left_ports.clone(),
            left_ports,
        )
        .build()
        .unwrap();
    let right = RuntimeServices::builder(temp.path(), temp.path().join("right"))
        .provider_registry(Arc::clone(&right_provider))
        .tool_execution_host(Arc::clone(&right_tool))
        .session_ports(
            right_ports.clone(),
            right_ports.clone(),
            right_ports.clone(),
            right_ports,
        )
        .build()
        .unwrap();

    assert!(Arc::ptr_eq(left.provider_registry(), &left_provider));
    assert!(Arc::ptr_eq(right.provider_registry(), &right_provider));
    assert!(!Arc::ptr_eq(
        left.provider_registry(),
        right.provider_registry()
    ));
    assert!(Arc::ptr_eq(left.tool_execution_host().unwrap(), &left_tool));
    assert!(Arc::ptr_eq(
        right.tool_execution_host().unwrap(),
        &right_tool
    ));
    assert!(!Arc::ptr_eq(
        left.tool_execution_host().unwrap(),
        right.tool_execution_host().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        left.session_input_router().unwrap(),
        right.session_input_router().unwrap()
    ));
    assert!(left.session_query_port().is_some());
    assert!(left.session_ingress_port().is_some());
    assert!(left.session_journal_port().is_some());
    assert!(left.session_application_port().is_some());
    assert!(right.session_query_port().is_some());
    assert!(right.session_ingress_port().is_some());
    assert!(right.session_journal_port().is_some());
    assert!(right.session_application_port().is_some());
}

#[tokio::test]
async fn due_schedule_submits_one_durable_handoff_graph_and_never_duplicates_it() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let timestamp = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&SessionRecord {
            session_id: "scheduled-target".to_string(),
            platform: "test".to_string(),
            chat_id: "scheduled-chat".to_string(),
            user_id: None,
            model: None,
            created_at: timestamp.clone(),
            last_activity: timestamp,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let services = RuntimeServices::in_memory().unwrap();
    services
        .install_test_session_store(Arc::clone(&store))
        .unwrap();
    services.publish_session_execution_policy(
        "scheduled-target",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Supervised,
                7,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    services
        .mission_runtime()
        .create_mission(
            "schedule-mission",
            "scheduled test mission",
            vec![harness_contract::reality::EvidenceRef::observed(
                "test",
                "schedule-mission",
            )],
        )
        .unwrap();
    let due_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    services
        .mission_schedules()
        .create(
            crate::CreateMissionScheduleRequest {
                mission_id: "schedule-mission".to_string(),
                target_session_id: "scheduled-target".to_string(),
                objective: "check the durable schedule path".to_string(),
                trigger: ScheduleTrigger::At { at_ms: due_at_ms },
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                priority: 64,
            },
            due_at_ms,
        )
        .unwrap();

    let first = services
        .dispatch_due_mission_schedules(due_at_ms)
        .await
        .unwrap();
    assert_eq!(first.tick.claimed.len(), 1);
    assert_eq!(first.submitted.len(), 1);
    assert!(first.failed.is_empty());
    let binding = first.submitted[0]
        .target_policy_binding
        .as_ref()
        .expect("target Session policy binding");
    assert_eq!(binding.policy_revision, 7);
    assert_eq!(
        binding.sandbox_posture,
        harness_contract::policy::SandboxPosture::WorkspaceWriteSandbox
    );
    assert_eq!(
        binding.permission_ceiling,
        harness_contract::policy::PermissionMode::ReadOnly
    );
    let graph_id = first.submitted[0]
        .graph_id
        .clone()
        .expect("stable graph id");
    services
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .unwrap();
    let graph = services.graph_state_store().load(&graph_id).unwrap();
    assert!(graph
        .node_statuses
        .values()
        .all(|status| *status == ExecutionNodeStatus::WaitingExternal));

    let target_outbox = store
        .claim_session_runtime_outbox("schedule-test", due_at_ms.saturating_add(5_000), 1_000, 8)
        .await
        .unwrap();
    assert_eq!(target_outbox.len(), 1);
    assert_eq!(target_outbox[0].session_id, "scheduled-target");

    services
        .execution_supervisor()
        .command_graph(
            &graph_id,
            ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "test terminal Mission fire cleanup".to_string(),
            },
        )
        .await
        .unwrap();

    let second = services
        .dispatch_due_mission_schedules(due_at_ms.saturating_add(1))
        .await
        .unwrap();
    assert!(second.tick.claimed.is_empty());
    assert!(second.submitted.is_empty());
    assert!(second.failed.is_empty());
    assert_eq!(services.mission_schedules().active_fire_count(), 0);
    let terminal = services
        .mission_schedules()
        .fire_by_id(&first.submitted[0].fire_id)
        .unwrap()
        .expect("durable terminal fire");
    assert_eq!(
        terminal.status,
        harness_contract::mission::MissionScheduleFireStatus::Cancelled
    );
}

#[tokio::test]
async fn same_workspace_services_coordinate_with_persistent_resources() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(temp.path().join("other")).unwrap();
    let first = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let second = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let isolated = RuntimeServices::builder(temp.path(), temp.path().join("other"))
        .build()
        .unwrap();

    assert!(!Arc::ptr_eq(first.scope_locks(), second.scope_locks()));
    assert!(!Arc::ptr_eq(
        first.worktree_leases(),
        second.worktree_leases()
    ));
    assert!(!Arc::ptr_eq(first.scope_locks(), isolated.scope_locks()));
    assert!(!Arc::ptr_eq(
        first.worktree_leases(),
        isolated.worktree_leases()
    ));

    let held = first
        .scope_locks()
        .acquire(
            [super::super::graph::ScopeLockRequest {
                scope: super::super::graph::ScopedResource::workspace(first.workspace_key())
                    .unwrap(),
                mode: super::super::graph::ScopeLockMode::Write,
            }],
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        second
            .scope_locks()
            .acquire(
                [super::super::graph::ScopeLockRequest {
                    scope: super::super::graph::ScopedResource::workspace(second.workspace_key(),)
                        .unwrap(),
                    mode: super::super::graph::ScopeLockMode::Write,
                }],
                Some(std::time::Duration::from_millis(25)),
            )
            .await,
        Err(super::super::graph::ScopeLockError::TimedOut { .. })
    ));
    drop(held);
}

#[test]
fn canonical_workspace_identity_shares_resources_across_home_and_symlink_aliases() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let alias = temp.path().join("workspace-alias");
    std::fs::create_dir_all(&workspace).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&workspace, &alias).unwrap();
    #[cfg(not(unix))]
    std::fs::create_dir_all(&alias).unwrap();

    let first = RuntimeServices::builder(temp.path().join("home-a"), &workspace)
        .build()
        .unwrap();
    let second = RuntimeServices::builder(temp.path().join("home-b"), &alias)
        .build()
        .unwrap();

    #[cfg(unix)]
    {
        assert_eq!(first.workspace_key(), second.workspace_key());
        assert!(!Arc::ptr_eq(first.scope_locks(), second.scope_locks()));
        assert!(!Arc::ptr_eq(
            first.worktree_leases(),
            second.worktree_leases()
        ));
    }
}

#[tokio::test]
async fn recovery_marker_blocks_runner_start_run_and_command_entries() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let importer = crate::upgrade::LegacyExecutionImporter::new(
        Arc::clone(services.event_store()),
        services.workspace_key(),
        services.workspace_root(),
        "0.9.472",
    );
    assert!(importer
        .import_receipt_file(temp.path().join("missing-receipt.json"))
        .is_err());

    let graph = harness_contract::execution_graph::ExecutionGraph::new("blocked");
    let graph_id = graph.id.clone();
    assert!(matches!(
        services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await,
        Err(super::super::graph::ExecutionRunnerError::MutationBlocked(
            _
        ))
    ));
    assert!(matches!(
        services
            .execution_supervisor()
            .notify_graph(&graph_id)
            .await,
        Ok(())
    ));
    assert!(matches!(
        services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await,
        Err(super::super::graph::ExecutionRunnerError::Driver(_))
    ));
    assert!(matches!(
        services.recover_execution_graphs_on_startup().await,
        Err(RuntimeServicesError::UpgradeRecoveryRequired)
    ));
    assert!(matches!(
        services
            .execution_supervisor()
            .command_graph(
                &graph_id,
                harness_contract::execution_graph::ExecutionGraphCommand::Advance {
                    expected_revision: 0,
                },
            )
            .await,
        Err(super::super::graph::ExecutionRunnerError::MutationBlocked(
            _
        ))
    ));
}

#[tokio::test]
async fn startup_recovery_rehydrates_and_advances_persistent_execution_graphs() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let cowd_home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&cowd_home).unwrap();

    let graph_id = {
        let services = RuntimeServices::builder(&cowd_home, &workspace)
            .build()
            .unwrap();
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new(
            "startup recovery production path",
        );
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "payload:startup-recovery",
        );
        node.id = "startup-node".to_string();
        node.idempotency_key = "idempotency:startup-node".to_string();
        graph.nodes.push(node);
        let graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        let graph = services
            .commit_service()
            .transition_node(
                &graph,
                "startup-node",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let graph = services
            .commit_service()
            .transition_node(
                &graph,
                "startup-node",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        graph.id
    };

    let restarted = RuntimeServices::builder(&cowd_home, &workspace)
        .build()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    restarted
        .tool_batch_executor()
        .install_resolver(Arc::new(ServiceScopedResolver {
            payload_ref: "payload:startup-recovery".to_string(),
            backend: Arc::new(ServiceScopedBackend {
                calls: Arc::clone(&calls),
            }),
        }));

    let report = restarted
        .recover_execution_graphs_on_startup()
        .await
        .expect("startup recovery");
    assert_eq!(report.examined_graphs, 1);
    assert_eq!(report.recovered_graphs, 1);
    assert_eq!(report.notified_graphs, 1);
    assert!(report.errors.is_empty());
    restarted
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .expect("recovered graph reaches quiescence");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let graph = restarted.graph_state_store().load(&graph_id).unwrap();
    assert_eq!(
        graph.node_statuses["startup-node"],
        ExecutionNodeStatus::Completed
    );
}

#[tokio::test]
async fn canonical_agent_task_flows_through_runner_and_commits_once() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                name: "test".into(),
                base_url: "https://example.test/v1".into(),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
        .build()
        .unwrap();
    publish_team_test_policy(&services, "agent-runtime-session");
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(CompletedAgentBackend));

    let mut graph = ExecutionGraph::new("agent graph integration");
    graph.id = "agent-runtime-graph".into();
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: "agent-runtime-session".to_string(),
        turn_id: "agent-runtime-turn".to_string(),
        root_task_id: "agent-runtime-task".to_string(),
        task_id: "agent-runtime-task".to_string(),
        generation: 1,
    });
    let intent = AgentTaskIntent {
        selected_agent_id: Some("builtin/cowd/direct".to_string()),
        definition_ref: Some(
            harness_contract::agent::AgentDefinitionRevisionRef::new(
                harness_contract::agent::AgentDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/direct",
                )
                .expect("builtin definition id"),
                1,
            )
            .expect("builtin definition revision"),
        ),
        granted_capabilities: vec![harness_contract::agent::AgentCapability::Read],
        principal_id: "test".to_string(),
        source_turn_id: "agent-runtime-turn".to_string(),
        run_id: "agent-runtime-run".into(),
        task_id: "agent-runtime-task".into(),
        root_task_id: "agent-runtime-task".into(),
        parent_task_id: None,
        session_id: "agent-runtime-session".into(),
        mission_id: services.mission_runtime().default_mission_id().to_string(),
        team_id: None,
        graph_id: graph.id.clone(),
        node_id: "agent-runtime-node".into(),
        attempt: 1,
        expected_graph_revision: 0,
        objective: "complete one graph-owned agent task".into(),
        team_role_identity: None,
        required_acceptance: harness_contract::context::RequiredAcceptance {
            criteria: vec!["completed".into()],
            evidence_obligations: Vec::new(),
        },
        output_acceptance: Vec::new(),
        requires_managed_collaboration_escalation: false,
        acceptance: vec!["completed".into()],
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: Vec::new(),
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "fast".into(),
        budget_lease: ChildExecutionBudgetReservation::single(
            "agent-runtime-budget",
            "agent-runtime-agent",
            "agent",
            1000,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        managed_invocation: None,
        idempotency_key: "agent-runtime-idempotency".into(),
    };
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
        serde_json::to_string(&intent).unwrap(),
    );
    node.id = intent.node_id.clone();
    node.idempotency_key = intent.idempotency_key.clone();
    node.acceptance.criteria = intent.acceptance.clone();
    graph.nodes.push(node);

    let graph = services.compile_graph_agent_intents(graph).unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();

    let (_, report) = services
        .execution_supervisor()
        .submit_and_wait(
            graph,
            harness_contract::execution_graph::ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("run graph");
    assert_eq!(report.completed, 1);
    let graph = services.graph_state_store().load(&report.graph_id).unwrap();
    assert_eq!(
        graph.node_statuses.get(packet.node_id()),
        Some(&ExecutionNodeStatus::Completed)
    );
    let agent = services
        .agent_runtime()
        .get(packet.agent_id())
        .expect("agent projection");
    assert_eq!(
        agent.status,
        harness_contract::agent::AgentStatus::Completed
    );
    let binding = agent.binding.expect("prepared Agent Binding is durable");
    assert_eq!(
        binding.definition_ref.definition_id.as_str(),
        "builtin/cowd/direct"
    );
    assert_eq!(binding.data_lease.session_id, packet.session_id());
    assert_eq!(binding.data_lease.task_id, packet.task_id());
    assert_eq!(services.agent_runtime().events(packet.agent_id()).len(), 3);
}

#[tokio::test]
async fn one_definition_can_drive_eight_isolated_runtime_instances() {
    let temp = tempfile::tempdir().expect("temporary root");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                name: "test".into(),
                base_url: "https://example.test/v1".into(),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .provider_registry(Arc::new(
            crate::ProviderRegistry::new(providers).expect("provider"),
        ))
        .build()
        .expect("runtime services");
    publish_team_test_policy(&services, "binding-session");
    let root_spec = services
        .task_runtime_port()
        .bind_task_spec(
            "binding-session",
            Some(harness_contract::policy::PermissionMode::ReadOnly),
            harness_contract::task::TaskSpec::new("coordinate evidence reads"),
        )
        .expect("bind root Task policy");
    services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "binding-root-task".to_string(),
            mission_id: services.mission_runtime().default_mission_id().to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::System,
            origin_session_id: "binding-session".to_string(),
            origin_turn_id: "binding-turn".to_string(),
            root_task_id: "binding-root-task".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
            mission_assigned_by: "runtime.test".to_string(),
            spec: root_spec,
            evidence_refs: Vec::new(),
        })
        .expect("create root Task");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(ParallelTrackingAgentBackend {
            active: Arc::clone(&active),
            max_active: Arc::clone(&max_active),
        }));

    let mut graph = ExecutionGraph::new("eight independent evidence reads");
    graph.id = "binding-eight-instances".to_string();
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: "binding-session".to_string(),
        turn_id: "binding-turn".to_string(),
        root_task_id: "binding-root-task".to_string(),
        task_id: "binding-root-task".to_string(),
        generation: 1,
    });
    for index in 0..8_u8 {
        let agent_id = format!("researcher-slot-{index}");
        let node_id = format!("binding-agent-node-{index}");
        let intent = AgentTaskIntent {
            selected_agent_id: Some("builtin/cowd/direct".to_string()),
            definition_ref: None,
            granted_capabilities: Vec::new(),
            principal_id: "test".to_string(),
            source_turn_id: format!("binding-turn-{index}"),
            run_id: format!("binding-run-{index}"),
            task_id: format!("binding-task-{index}"),
            root_task_id: "binding-root-task".to_string(),
            parent_task_id: Some("binding-root-task".to_string()),
            session_id: "binding-session".to_string(),
            mission_id: services.mission_runtime().default_mission_id().to_string(),
            // This is a fan-out of independent root-level Agent work;
            // it intentionally is not a Team binding and therefore must
            // not claim a Team id without a frozen typed role identity.
            team_id: None,
            graph_id: graph.id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            expected_graph_revision: 0,
            objective: format!("research isolated domain {index}"),
            team_role_identity: None,
            required_acceptance: harness_contract::context::RequiredAcceptance {
                criteria: vec!["evidence".to_string()],
                evidence_obligations: Vec::new(),
            },
            output_acceptance: vec![harness_contract::team::TeamAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                    scopes: vec![format!("read:binding-domain-{index}")],
                },
            }],
            requires_managed_collaboration_escalation: false,
            acceptance: vec!["evidence".to_string()],
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: vec![format!("read:binding-domain-{index}")],
            allowed_tools: vec!["read_file".to_string()],
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "fast".to_string(),
            budget_lease: ChildExecutionBudgetReservation::single(
                format!("binding-budget-{index}"),
                agent_id,
                "agent",
                2_000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            idempotency_key: format!("binding-agent-{index}"),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            crate::execution_core::graph::executors::AgentTaskExecutor::KIND,
            serde_json::to_string(&intent).expect("intent"),
        );
        node.id = node_id;
        node.idempotency_key = intent.idempotency_key;
        node.acceptance.criteria = intent.acceptance;
        graph.nodes.push(node);
    }

    let graph = services
        .compile_graph_agent_intents(graph)
        .expect("bind graph");
    let packets = graph
        .nodes
        .iter()
        .map(|node| {
            serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                .expect("canonical AgentTaskPacket")
        })
        .collect::<Vec<_>>();
    assert!(packets.iter().all(|packet| {
        let typed_acceptance_matches_lease = packet.output_acceptance.iter().any(|requirement| {
            matches!(
                &requirement.check,
                harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { scopes }
                    if scopes == &packet.resource_scopes
            )
        });
        typed_acceptance_matches_lease && packet.constraints.is_empty()
    }));
    let agent_ids = packets
        .iter()
        .map(|packet| packet.agent_id().to_string())
        .collect::<Vec<_>>();

    let (_, report) = services
        .execution_supervisor()
        .submit_and_wait(
            graph,
            harness_contract::execution_graph::ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("run graph");
    assert_eq!(report.completed, 8, "parallel agent report: {report:?}");
    assert!(
        services.agent_runtime().list().is_empty(),
        "terminal Agent projections must leave bounded hot state"
    );
    let snapshots = agent_ids
        .into_iter()
        .map(|agent_id| {
            services
                .agent_runtime()
                .get(&agent_id)
                .expect("durable terminal Agent projection")
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 8);
    let bindings = snapshots
        .iter()
        .map(|snapshot| snapshot.binding.as_ref().expect("durable binding"))
        .collect::<Vec<_>>();
    assert!(bindings.iter().all(|binding| {
        binding.definition_ref.definition_id.as_str() == "builtin/cowd/direct"
            && binding.definition_ref.revision == 1
            && binding.data_lease.team_id.is_none()
    }));
    let instances = bindings
        .iter()
        .map(|binding| binding.instance.instance_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(instances.len(), 8);
    assert!(max_active.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn policy_drain_tracks_and_terminalizes_an_admitted_pre_graph_task() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    publish_team_test_policy(&services, "policy-drain-session");
    let spec = services
        .task_runtime_port()
        .bind_task_spec(
            "policy-drain-session",
            Some(harness_contract::policy::PermissionMode::ReadOnly),
            harness_contract::task::TaskSpec::new("scheduled work awaiting graph submission"),
        )
        .expect("bound policy");
    services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "policy-drain-pre-graph-task".to_string(),
            mission_id: services.mission_runtime().default_mission_id().to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::Schedule,
            origin_session_id: "mission-schedule:test".to_string(),
            origin_turn_id: "schedule-turn:test".to_string(),
            root_task_id: "policy-drain-pre-graph-task".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
            mission_assigned_by: "runtime.test".to_string(),
            spec,
            evidence_refs: Vec::new(),
        })
        .expect("admitted Task");

    assert_eq!(
        services
            .active_tasks_for_session_policy_revision("policy-drain-session", 1)
            .await
            .expect("active old revision"),
        vec![("policy-drain-pre-graph-task".to_string(), 1)]
    );
    assert_eq!(
        services
            .cancel_attempts_for_session_policy_revision(
                "policy-drain-session",
                1,
                "policy transition timeout",
            )
            .await
            .expect("exact cancellation"),
        1
    );
    assert!(services
        .active_tasks_for_session_policy_revision("policy-drain-session", 1)
        .await
        .expect("drained")
        .is_empty());
    assert_eq!(
        services
            .task_aggregate_service()
            .get("policy-drain-pre-graph-task")
            .expect("task read")
            .expect("task")
            .status,
        harness_contract::task::TaskStatus::Cancelled
    );
}

#[tokio::test]
async fn policy_drain_terminalizes_graph_and_its_owning_task_from_one_snapshot() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    publish_team_test_policy(&services, "policy-drain-graph-session");
    let spec = services
        .task_runtime_port()
        .bind_task_spec(
            "policy-drain-graph-session",
            Some(harness_contract::policy::PermissionMode::ReadOnly),
            harness_contract::task::TaskSpec::new("background graph under old policy"),
        )
        .expect("bound policy");
    let task = services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "policy-drain-graph-task".to_string(),
            mission_id: services.mission_runtime().default_mission_id().to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::Schedule,
            origin_session_id: "mission-schedule:graph-test".to_string(),
            origin_turn_id: "schedule-turn:graph-test".to_string(),
            root_task_id: "policy-drain-graph-task".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
            mission_assigned_by: "runtime.test".to_string(),
            spec,
            evidence_refs: Vec::new(),
        })
        .expect("admitted Task")
        .aggregate;
    let mut graph = ExecutionGraph::new("background graph under old policy").with_lineage(
        harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "policy-drain-graph-session".to_string(),
            turn_id: "schedule-turn:graph-test".to_string(),
            root_task_id: task.task_id.clone(),
            task_id: task.task_id.clone(),
            generation: 1,
        },
    );
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::ToolBatch,
        "tool_batch",
        "payload:policy-drain",
    );
    node.id = "policy-drain-node".to_string();
    node.idempotency_key = "policy-drain-node".to_string();
    graph.nodes.push(node);
    let graph = services
        .commit_service()
        .register_graph(graph)
        .expect("register background graph")
        .graph;
    services
        .task_runtime_port()
        .link_existing_graph(
            &task.task_id,
            &graph.id,
            graph.revision,
            vec![harness_contract::reality::EvidenceRef::observed(
                "execution_graph",
                graph.id.clone(),
            )],
        )
        .expect("link graph to Task");

    assert_eq!(
        services
            .cancel_attempts_for_session_policy_revision(
                "policy-drain-graph-session",
                1,
                "policy transition timeout",
            )
            .await
            .expect("exact cancellation"),
        2
    );
    let cancelled_graph = services
        .graph_state_store()
        .load(&graph.id)
        .expect("cancelled graph");
    assert!(cancelled_graph
        .node_statuses
        .values()
        .all(|status| *status == ExecutionNodeStatus::Cancelled));
    assert_eq!(
        services
            .task_aggregate_service()
            .get(&task.task_id)
            .expect("task read")
            .expect("task")
            .status,
        harness_contract::task::TaskStatus::Cancelled
    );
}

#[tokio::test]
async fn session_cancellation_terminalizes_descendants_of_an_already_terminal_root() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let lineage = harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: "session-cancel-lineage".to_string(),
        turn_id: "turn-cancel-lineage".to_string(),
        root_task_id: "task-cancel-lineage".to_string(),
        task_id: "task-cancel-lineage".to_string(),
        generation: 1,
    };
    let mut parent = ExecutionGraph::new("cancelled Session root").with_lineage(lineage.clone());
    parent.id = "session-cancel-root".to_string();
    let mut parent_node =
        ExecutionNodeSpec::new(ExecutionNodeKind::Subgraph, "team_subgraph", "{}");
    parent_node.id = "team-node".to_string();
    parent_node.idempotency_key = "team-node".to_string();
    parent.nodes.push(parent_node);
    let parent = services
        .commit_service()
        .register_graph(parent)
        .expect("register parent")
        .graph;

    let mut child = ExecutionGraph::new("running Team child").with_lineage(lineage);
    child.id = "session-cancel-child".to_string();
    child.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: parent.id.clone(),
        node_id: "team-node".to_string(),
    });
    let mut child_node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
    child_node.id = "researcher".to_string();
    child_node.idempotency_key = "researcher".to_string();
    child.nodes.push(child_node);
    let child = services
        .commit_service()
        .register_graph(child)
        .expect("register child")
        .graph;

    services
        .execution_supervisor()
        .command_graph(
            &parent.id,
            ExecutionGraphCommand::Cancel {
                expected_revision: parent.revision,
                reason: "root already terminal".to_string(),
            },
        )
        .await
        .expect("cancel root");
    assert!(services
        .graph_state_store()
        .load(&child.id)
        .expect("child before propagation")
        .node_statuses
        .values()
        .any(|status| !status.is_terminal()));

    let cancelled = services
        .cancel_execution_tree(&parent.id, "user cancelled Session")
        .await
        .expect("cancel execution tree");
    assert_eq!(cancelled, vec![child.id.clone()]);
    assert!(services
        .graph_state_store()
        .load(&child.id)
        .expect("child after propagation")
        .node_statuses
        .values()
        .all(|status| *status == ExecutionNodeStatus::Cancelled));
}

#[tokio::test]
async fn team_runtime_compiles_parallel_agents_and_emits_one_verified_terminal_result() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(workspace.join("crates")).unwrap();
    std::fs::write(workspace.join("crates/runtime"), "fixture before\n").unwrap();
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                name: "test".into(),
                base_url: "https://example.test/v1".into(),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
        .build()
        .unwrap();
    publish_team_test_policy(&services, "team-runtime-session");
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(CompletedAgentBackend));

    let projection = services
        .team_runtime()
        .instantiate(team_request(
            "team-runtime-integration",
            "team-runtime-session",
            "cowd/execute-review",
            "independently analyse and review the runtime boundary",
            "fast",
            services.mission_runtime().default_mission_id(),
        ))
        .await
        .expect("team execution");

    assert_eq!(projection.status, "completed");
    assert_eq!(projection.tasks.len(), 2);
    let terminal = projection.terminal_result.expect("one terminal result");
    assert!(
            terminal.result_ref.starts_with("delivery-envelope: "),
            "a backend without an explicit validated AnswerCandidate must use the mechanical delivery envelope: {terminal:?}"
        );
    assert!(!terminal.evidence_refs.is_empty());
    let graph = services
        .graph_state_store()
        .load(&projection.graph_id)
        .expect("canonical graph");
    assert!(
        graph
            .node_statuses
            .values()
            .all(|status| *status == ExecutionNodeStatus::Completed),
        "deterministic Team backend must terminalize every node: statuses={:?}; results={:?}",
        graph.node_statuses,
        graph.node_results
    );
    let team_bindings = graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .map(|node| {
            serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                .expect("canonical AgentTaskPacket")
                .binding
                .expect("Team graph payload contains exact Binding")
        })
        .collect::<Vec<_>>();
    assert_eq!(team_bindings.len(), 2);
    assert_eq!(
        team_bindings
            .iter()
            .map(|binding| binding.definition_ref.definition_id.as_str())
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["builtin/cowd/direct", "builtin/cowd/execute"])
    );
    assert!(team_bindings.iter().all(|binding| {
        binding.data_lease.team_id.as_deref() == Some("team-runtime-integration")
    }));
    assert!(services.team_runtime().projection_json()["teams"]
        .as_array()
        .is_some_and(|teams| teams.len() == 1));
    let binding = crate::team_binding::load_binding(services.event_store(), &projection.graph_id)
        .expect("binding read")
        .expect("normal Team admission persists its frozen Binding");
    assert_eq!(binding.roles.len(), 2);
    assert!(
        crate::team_binding::has_ready_marker(services.event_store(), &projection.graph_id)
            .expect("ready marker read"),
        "normal Team admission closes the exact link set with a Ready marker"
    );
}

#[tokio::test]
async fn team_admission_recovers_incomplete_task_links_before_drive() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                name: "test".into(),
                base_url: "https://example.test/v1".into(),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
        .build()
        .unwrap();
    publish_team_test_policy(&services, "team-crash-session");
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(CompletedAgentBackend));
    let request = team_request(
        "team-crash-recovery",
        "team-crash-session",
        "cowd/execute-review",
        "recover the exact link set after a crash between graph registration and Task admission",
        "fast",
        services.mission_runtime().default_mission_id(),
    );
    let mut instantiated = services
        .team_runtime()
        .plan(request.clone())
        .expect("team plan");
    services
        .team_runtime()
        .ensure_root_task(&request)
        .expect("root task exists before the crash window");
    assert!(
        services
            .task_runtime_port()
            .get(&request.lineage.root_task_id)
            .expect("root lookup")
            .is_some(),
        "root task must be durable before the crash window"
    );
    services
        .team_runtime()
        .bind_instantiated_task_policies(&mut instantiated)
        .expect("freeze inherited Task policy before durable Preparing marker");
    let registered = services
        .commit_service()
        .register_graph(instantiated.graph.clone())
        .expect("graph registered in crash window");
    crate::team_binding::persist_preparing_with_task_commands(
        services.event_store(),
        &registered.graph.id,
        instantiated
            .binding
            .as_ref()
            .expect("compiled Team Binding"),
        &instantiated.task_commands,
    )
    .expect("preparing marker persisted");
    assert!(
        !crate::team_binding::has_ready_marker(services.event_store(), &registered.graph.id)
            .expect("ready marker read"),
        "crash window has Preparing but no Ready marker"
    );
    assert_eq!(
        services
            .team_runtime()
            .reconcile_preparing_bindings_on_startup(256)
            .expect("startup reconciliation closes the frozen Task link set"),
        1,
        "recovery must repair exactly this unready Team before any graph is driven"
    );

    let projection = services
        .team_runtime()
        .instantiate_or_resume(request.clone())
        .await
        .expect("resume reconciles links and executes once");
    assert_eq!(projection.status, "unavailable");
    assert_eq!(projection.tasks.len(), 2);
    assert!(
        crate::team_binding::has_ready_marker(services.event_store(), &registered.graph.id)
            .expect("ready marker read"),
        "Ready marker must close the exact link set"
    );
    let binding = crate::team_binding::load_binding(services.event_store(), &registered.graph.id)
        .expect("binding read")
        .expect("binding persisted");
    assert_eq!(binding.roles.len(), 2);

    let again = services
        .team_runtime()
        .instantiate_or_resume(request)
        .await
        .expect("second resume is idempotent");
    assert_eq!(again.tasks.len(), 2);
}

#[tokio::test]
async fn team_admission_recovers_crash_after_the_first_task_link() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                name: "test".into(),
                base_url: "https://example.test/v1".into(),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
        .build()
        .unwrap();
    publish_team_test_policy(&services, "team-crash-second-link-session");
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(CompletedAgentBackend));
    let request = team_request(
        "team-crash-second-link",
        "team-crash-second-link-session",
        "cowd/execute-review",
        "recover a crash that happened after the first Task link",
        "fast",
        services.mission_runtime().default_mission_id(),
    );
    let mut instantiated = services
        .team_runtime()
        .plan(request.clone())
        .expect("team plan");
    assert_eq!(instantiated.task_commands.len(), 2);
    services
        .team_runtime()
        .ensure_root_task(&request)
        .expect("root task exists before the crash window");
    assert!(
        services
            .task_runtime_port()
            .get(&request.lineage.root_task_id)
            .expect("root lookup")
            .is_some(),
        "root task must be durable before the crash window"
    );
    services
        .team_runtime()
        .bind_instantiated_task_policies(&mut instantiated)
        .expect("freeze inherited Task policy before durable Preparing marker");
    let registered = services
        .commit_service()
        .register_graph(instantiated.graph.clone())
        .expect("graph registered");
    crate::team_binding::persist_preparing_with_task_commands(
        services.event_store(),
        &registered.graph.id,
        instantiated
            .binding
            .as_ref()
            .expect("compiled Team Binding"),
        &instantiated.task_commands,
    )
    .expect("preparing marker persisted");
    // Simulate a crash after exactly the first Task link was committed.
    let first = instantiated.task_commands[0].clone();
    assert_eq!(
        first.parent_task_id.as_deref(),
        Some(request.lineage.root_task_id.as_str()),
        "first command parent is the root task"
    );
    let bound_spec = services
        .task_runtime_port()
        .bind_inherited_task_spec(
            request.lineage.root_task_id.as_str(),
            instantiated.task_permission_ceiling,
            first.spec.clone(),
        )
        .expect("bind inherited task policy");
    let mut bound_first = first.clone();
    bound_first.spec = bound_spec;
    services
        .task_aggregate_service()
        .create(bound_first)
        .expect("first Task committed in the crash window");
    services
        .task_runtime_port()
        .link_existing_graph(
            &first.task_id,
            &registered.graph.id,
            registered.graph.revision,
            vec![harness_contract::reality::EvidenceRef::observed(
                "execution_graph",
                format!(
                    "execution-graph://{}?revision={}",
                    registered.graph.id, registered.graph.revision
                ),
            )],
        )
        .expect("first link committed");

    let projection = services
        .team_runtime()
        .instantiate_or_resume(request)
        .await
        .expect("resume completes the exact link set");
    assert_eq!(projection.status, "unavailable");
    assert_eq!(projection.tasks.len(), 2);
    assert!(
        crate::team_binding::has_ready_marker(services.event_store(), &registered.graph.id)
            .expect("ready marker read"),
        "resume must close the remaining link and mark Ready"
    );
    let linked = services
        .task_aggregate_service()
        .for_graphs(&[registered.graph.id.clone()])
        .expect("durable Task link set");
    assert_eq!(
        linked.len(),
        2,
        "final link set must be exact: no duplicate, no missing link"
    );
}

#[tokio::test]
async fn same_team_ingress_claim_never_creates_a_second_root() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::in_memory().unwrap();
    publish_team_test_policy(&services, "team-cas-session");
    let request = team_request(
        "team-cas-root",
        "team-cas-session",
        "cowd/execute-review",
        "claim exactly one Team root",
        "fast",
        services.mission_runtime().default_mission_id(),
    );
    services
        .team_runtime()
        .admit(request.clone())
        .await
        .expect("first admission claims the root");
    let second = services
        .team_runtime()
        .admit(request)
        .await
        .expect_err("same ingress+team tuple must not claim a second root");
    assert!(second.contains("already claimed"));
}

#[tokio::test]
async fn fanout_team_uses_runner_parallelism_without_a_team_scheduler() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                name: "test".into(),
                base_url: "https://example.test/v1".into(),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
        .build()
        .unwrap();
    publish_team_test_policy(&services, "team-runtime-session");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(ParallelTrackingAgentBackend {
            active: Arc::clone(&active),
            max_active: Arc::clone(&max_active),
        }));
    let projection = services
        .team_runtime()
        .instantiate(TeamInstantiationRequest {
            cardinality_overrides: vec![TeamRoleCardinalityOverride {
                role_id: "researcher".to_string(),
                cardinality: RoleCardinalityPolicy::Fixed { count: 3 },
            }],
            focus_partition_plans: vec![harness_contract::team::FocusPartitionPlan {
                role_id: "researcher".to_string(),
                shared_baseline: vec!["compare the same architecture constraints".to_string()],
                slots: vec![
                    harness_contract::team::FocusPartitionSlot {
                        focus_id: "architecture-a".to_string(),
                        boundary: "only architecture-a".to_string(),
                        evidence_responsibility: "source evidence for architecture-a".to_string(),
                        capability_cropped_refs: vec!["read:architecture-a".to_string()],
                        scope_hash: harness_contract::team::focus_scope_hash(
                            "researcher",
                            "only architecture-a",
                            &["read:architecture-a".to_string()],
                        ),
                        overlap_budget_bp: 0,
                        novelty_target_bp: 2_500,
                        output_contract: vec!["findings".to_string(), "evidence".to_string()],
                        output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
                    },
                    harness_contract::team::FocusPartitionSlot {
                        focus_id: "architecture-b".to_string(),
                        boundary: "only architecture-b".to_string(),
                        evidence_responsibility: "source evidence for architecture-b".to_string(),
                        capability_cropped_refs: vec!["read:architecture-b".to_string()],
                        scope_hash: harness_contract::team::focus_scope_hash(
                            "researcher",
                            "only architecture-b",
                            &["read:architecture-b".to_string()],
                        ),
                        overlap_budget_bp: 0,
                        novelty_target_bp: 2_500,
                        output_contract: vec!["findings".to_string(), "evidence".to_string()],
                        output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
                    },
                    harness_contract::team::FocusPartitionSlot {
                        focus_id: "architecture-c".to_string(),
                        boundary: "only architecture-c".to_string(),
                        evidence_responsibility: "source evidence for architecture-c".to_string(),
                        capability_cropped_refs: vec!["read:architecture-c".to_string()],
                        scope_hash: harness_contract::team::focus_scope_hash(
                            "researcher",
                            "only architecture-c",
                            &["read:architecture-c".to_string()],
                        ),
                        overlap_budget_bp: 0,
                        novelty_target_bp: 2_500,
                        output_contract: vec!["findings".to_string(), "evidence".to_string()],
                        output_acceptance: vec!["findings".to_string(), "evidence".to_string()],
                    },
                ],
            }],
            resource_scopes: vec![
                "read:architecture-a".to_string(),
                "read:architecture-b".to_string(),
                "read:architecture-c".to_string(),
            ],
            ..team_request(
                "team-runtime-fanout",
                "team-runtime-session",
                "cowd/parallel-research-synthesis",
                "compare three independent architecture choices",
                "fast",
                services.mission_runtime().default_mission_id(),
            )
        })
        .await
        .expect("fanout team execution");
    assert_eq!(projection.status, "unavailable");
    assert!(max_active.load(Ordering::SeqCst) >= 2);
    assert!(max_active.load(Ordering::SeqCst) <= 3);
}

#[test]
fn ephemeral_team_snapshot_compiles_without_catalog_publication() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    let mut request = team_request(
        "ephemeral-template",
        "ephemeral-session",
        "cowd/parallel-research-synthesis",
        "independently assess the bounded evidence",
        "test-model",
        services.mission_runtime().default_mission_id(),
    );
    let snapshot = crate::orchestration::compile_ephemeral_team_template_snapshot(
        serde_json::json!({
            "template_id": "cowd/ephemeral-independent-assessment",
            "name": "独立证据评估团队",
            "team_display_name": "独立评估",
            "roles": [{
                "role_id": "evidence_assessor",
                "display_name": "证据评估师",
                "responsibility": "独立检查已授权证据并报告不确定性",
                "agent_definition_ref": "builtin/cowd/explore@1",
                "grant_ceiling": ["read"],
                "fixed_count": 1,
                "acceptance": ["summary", "evidence"],
                "behavior": [{"kind": "reacquire_evidence", "required": true}]
            }],
            "result_fields": ["summary", "evidence"],
            "evidence_required": true,
            "instructions": "# 独立评估\n\n只使用已授权证据，清楚列出不确定性。"
        }),
        &request.lineage,
        harness_contract::policy::PermissionMode::ReadOnly,
        "session-policy:ephemeral-session:1".to_string(),
        u64::MAX,
        &services,
    )
    .expect("custom snapshot compiles without catalog publication");
    snapshot.validate().expect("snapshot is self-consistent");
    let ephemeral_id = snapshot.revision.revision_ref.template_id.clone();
    request.template_selector = TeamTemplateSelector::Ephemeral {
        snapshot: Box::new(snapshot),
    };
    let planned = services
        .team_runtime()
        .plan(request)
        .expect("ephemeral Team compiles without a published catalog revision");
    assert_eq!(planned.template_ref.template_id, ephemeral_id);
    assert!(services
        .definition_registry()
        .resolve_team(&ephemeral_id, RevisionSelector::LatestApprovedStable)
        .is_err());
}

fn team_request(
    team_id: &str,
    session_id: &str,
    template_id: &str,
    objective: &str,
    model_lease: &str,
    mission_id: &str,
) -> TeamInstantiationRequest {
    TeamInstantiationRequest {
        request_id: format!("test-request-{team_id}"),
        team_id: team_id.to_string(),
        mission_id: mission_id.to_string(),
        lineage: harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: session_id.to_string(),
            turn_id: format!("turn-{team_id}"),
            root_task_id: format!("task-root-{team_id}"),
            task_id: format!("task-root-{team_id}"),
            generation: 1,
        },
        parent_execution: None,
        selection_mode: TeamSelectionMode::Explicit,
        strategy_binding: None,
        template_selector: TeamTemplateSelector::LatestStable {
            template_id: TeamTemplateDefinitionId::new(DefinitionScope::Builtin, template_id)
                .expect("builtin Team template id"),
        },
        objective: objective.to_string(),
        acceptance: vec!["summary".to_string(), "evidence".to_string()],
        risk: None,
        role_binding_overrides: Vec::new(),
        display_name: None,
        role_display_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        requires_managed_collaboration_escalation: false,
        permission_ceiling: if template_id == "cowd/execute-review" {
            harness_contract::policy::PermissionMode::WorkspaceWrite
        } else {
            harness_contract::policy::PermissionMode::ReadOnly
        },
        model_lease: model_lease.to_string(),
        execution_budget: harness_contract::context::ParentExecutionBudget::new(
            format!("service-team-budget:{team_id}"),
            65_536,
            u64::MAX,
            32,
            1,
        ),
        deadline_at_ms: u64::MAX,
        managed_invocation: None,
        resource_scopes: vec![if template_id == "cowd/execute-review" {
            "write:crates/runtime".to_string()
        } else {
            "read:crates/runtime".to_string()
        }],
        allow_whole_workspace_scope: false,
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
        upstream_result_context: Vec::new(),
        execution_capacity: None,
    }
}

fn publish_team_test_policy(services: &RuntimeServices, session_id: &str) {
    services.publish_session_execution_policy(
        session_id,
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Supervised,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
}

#[test]
fn services_builder_imports_and_retires_unbound_legacy_team_state() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let legacy_path = temp
        .path()
        .join("agents")
        .join("team-runtime")
        .join("state.json");
    std::fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(
        &legacy_path,
        r#"{"runs":{"legacy":{"snapshot":{"team_id":"legacy","status":"running"}}}}"#,
    )
    .unwrap();

    let services = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    assert!(!legacy_path.exists());
    let imported = services
        .event_store()
        .all_events(20)
        .unwrap()
        .into_iter()
        .find(|event| event.kind == "team.legacy_imported")
        .expect("legacy team audit event");
    assert_eq!(imported.status.as_deref(), Some("blocked"));
    assert_eq!(imported.payload["team_id"], "legacy");
    assert_eq!(imported.payload["disposition"], "blocked_unbound");
}

#[test]
fn runtime_timeline_position_never_skips_events_inside_one_transaction() {
    let store =
        Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("runtime event store"));
    store
        .append_transaction(crate::AppendTransactionRequest {
            transaction_id: "timeline-transaction".to_string(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: "timeline-session".to_string(),
                expected_revision: 0,
            }],
            events: vec![
                crate::RuntimeEventInput {
                    stream_id: "timeline-session".to_string(),
                    scope: crate::RuntimeEventScope::SessionInput,
                    kind: "timeline.first".to_string(),
                    status: None,
                    actor: None,
                    refs: Vec::new(),
                    payload: serde_json::Value::Null,
                }
                .into(),
                crate::RuntimeEventInput {
                    stream_id: "timeline-session".to_string(),
                    scope: crate::RuntimeEventScope::SessionInput,
                    kind: "timeline.second".to_string(),
                    status: None,
                    actor: None,
                    refs: Vec::new(),
                    payload: serde_json::Value::Null,
                }
                .into(),
            ],
        })
        .expect("transaction commits");
    let reader = super::RuntimeEventReader { store };

    let first = reader
        .session_timeline_events("timeline-session", None, 1)
        .expect("first page");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].transaction_index, 0);
    let second = reader
        .session_timeline_events(
            "timeline-session",
            Some((first[0].commit_cursor, first[0].transaction_index)),
            1,
        )
        .expect("second page");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].transaction_index, 1);
    assert_eq!(second[0].kind, "timeline.second");
}

#[test]
fn cancellation_receipt_is_durable_idempotent_and_conflict_checked() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let requested = harness_contract::turn::CancellationReceipt {
        cancellation_id: "cancel-1".to_string(),
        session_id: "session-1".to_string(),
        turn_id: "turn-1".to_string(),
        execution_id: "execution-1".to_string(),
        actor_id: "user-1".to_string(),
        cause: harness_contract::turn::CancellationCause::UserRequested,
        reason: Some("stop".to_string()),
        requested_at_ms: 10,
        effective_at_ms: None,
        status: harness_contract::turn::CancellationStatus::Requested,
        journal_sequence: 0,
        projection_revision: 0,
    };
    let intent = services
        .commit_cancellation_receipt(requested.clone())
        .unwrap();
    assert_eq!(
        intent.status,
        harness_contract::turn::CancellationStatus::Requested
    );

    // Simulate a process dying after the durable intent but before it
    // records the winner. Reopening the services must recover that intent
    // and permit exactly one final transition.
    drop(services);
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    assert_eq!(
        services.cancellation_receipt("cancel-1").unwrap(),
        Some(intent.clone())
    );
    let mut receipt = requested;
    receipt.effective_at_ms = Some(11);
    receipt.status = harness_contract::turn::CancellationStatus::Cancelled;
    let first = services
        .commit_cancellation_receipt(receipt.clone())
        .unwrap();
    let duplicate = services
        .commit_cancellation_receipt(receipt.clone())
        .unwrap();
    assert_eq!(first, duplicate);
    let mut concurrent_finalizer = receipt.clone();
    concurrent_finalizer.effective_at_ms = Some(99);
    concurrent_finalizer.status = harness_contract::turn::CancellationStatus::AlreadyTerminal;
    assert_eq!(
        services
            .commit_cancellation_receipt(concurrent_finalizer)
            .unwrap(),
        first,
        "the first durable finalizer owns status and effective timestamp"
    );
    assert!(first.journal_sequence > intent.journal_sequence);
    assert_eq!(first.projection_revision, 2);

    let mut conflicting = receipt;
    conflicting.reason = Some("different".to_string());
    assert!(services.commit_cancellation_receipt(conflicting).is_err());
}

#[test]
fn concurrent_cancellation_finalizers_converge_on_one_durable_winner() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::builder(temp.path(), &workspace)
        .build()
        .unwrap();
    services.record_live_execution(
        "cancel-race-session",
        "cancel-race-execution".to_string(),
        "cancel-race-turn".to_string(),
    );
    let requested = harness_contract::turn::CancellationReceipt {
        cancellation_id: "cancel-race".to_string(),
        session_id: "cancel-race-session".to_string(),
        turn_id: "cancel-race-turn".to_string(),
        execution_id: "cancel-race-execution".to_string(),
        actor_id: "principal:local-human".to_string(),
        cause: harness_contract::turn::CancellationCause::UserRequested,
        reason: Some("user_requested".to_string()),
        requested_at_ms: 100,
        effective_at_ms: None,
        status: harness_contract::turn::CancellationStatus::Requested,
        journal_sequence: 0,
        projection_revision: 0,
    };
    let request_barrier = std::sync::Arc::new(std::sync::Barrier::new(5));
    let mut request_workers = Vec::new();
    for _ in 0..4 {
        let services = std::sync::Arc::clone(&services);
        let barrier = std::sync::Arc::clone(&request_barrier);
        let requested = requested.clone();
        request_workers.push(std::thread::spawn(move || {
            barrier.wait();
            services.commit_cancellation_receipt(requested).unwrap()
        }));
    }
    request_barrier.wait();
    let requested_results = request_workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert!(requested_results.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(
        services
            .event_store
            .list_stream("cancellation:cancel-race")
            .unwrap()
            .len(),
        1,
        "concurrent identical intents commit once"
    );
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let services = std::sync::Arc::clone(&services);
        let barrier = std::sync::Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            services
                .resolve_requested_cancellation("cancel-race")
                .unwrap()
                .unwrap()
        }));
    }
    barrier.wait();
    let first = workers.remove(0).join().unwrap();
    let second = workers.remove(0).join().unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first.status,
        harness_contract::turn::CancellationStatus::Cancelled
    );
    assert_eq!(
        services
            .event_store
            .list_stream("cancellation:cancel-race")
            .unwrap()
            .len(),
        2,
        "one Requested and one final winner are the complete saga"
    );
}
