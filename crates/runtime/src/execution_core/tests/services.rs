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
use session::SessionRecord;

#[path = "paired_skill.rs"]
mod paired_skill;

struct ReadinessOnlyEvolutionEvalRunner;

struct TestRuntimeBackends {
    events: Arc<crate::RuntimeEventStore>,
    artifacts: Arc<crate::ArtifactStore>,
    tasks: Arc<crate::TaskAggregateService>,
}

impl TestRuntimeBackends {
    fn new(root: &std::path::Path) -> Self {
        Self {
            events: Arc::new(crate::RuntimeEventStore::for_test()),
            artifacts: Arc::new(crate::ArtifactStore::for_test_default(
                root.join("test-artifacts"),
            )),
            tasks: Arc::new(crate::TaskAggregateService::for_test()),
        }
    }

    fn builder(
        &self,
        cowd_home: impl Into<std::path::PathBuf>,
        workspace: impl Into<std::path::PathBuf>,
    ) -> RuntimeServicesBuilder {
        RuntimeServices::builder(cowd_home, workspace)
            .runtime_event_store(Arc::clone(&self.events))
            .artifact_store(Arc::clone(&self.artifacts))
            .task_aggregate_service(Arc::clone(&self.tasks))
    }
}

#[test]
fn evolution_projector_health_reports_the_effective_background_switch() {
    // A deliberately disabled Evolution background worker has no lane, so its
    // health must say "not enabled" instead of presenting an absent worker as
    // a broken projection. The switch is the only honest source of that
    // distinction, and it is exactly what the isolation profile sets.
    for evolution_enabled in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let backends = TestRuntimeBackends::new(root.path());
        let services = backends
            .builder(root.path().join("home"), &workspace)
            .background_work(crate::BackgroundWorkConfig {
                evolution: evolution_enabled,
                growth: evolution_enabled,
            })
            .build()
            .unwrap();
        let health = services.evolution_projector_health().unwrap();
        assert_eq!(
            health.enabled, evolution_enabled,
            "projector health must report the effective switch"
        );
        if !evolution_enabled {
            assert!(
                !health.worker_running,
                "a disabled projector registers no lane and therefore no worker"
            );
        }
    }
}

#[test]
fn background_evolution_configuration_controls_real_registration_without_removing_governance() {
    for production_default in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let backends = TestRuntimeBackends::new(root.path());
        let builder = backends.builder(root.path().join("home"), &workspace);
        // No setting in the first branch: exercise the production builder's
        // actual default and common assembly, not a copied expected bool.
        let builder = if production_default {
            builder
        } else {
            builder.background_work(crate::BackgroundWorkConfig {
                evolution: false,
                growth: false,
            })
        };
        let services = builder.build().unwrap();
        let ids = services
            .event_reactor
            .health()
            .unwrap()
            .lanes
            .into_iter()
            .map(|lane| lane.projection_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ids.contains("projector:evolution-case:v2"),
            production_default
        );
        assert_eq!(
            ids.contains("projector:agentic-collaboration-experience:v1"),
            production_default
        );
        assert_eq!(
            services.evolution_analyst.read().unwrap().is_some(),
            production_default
        );
        // Historical management/query owners are retained independently of
        // automatic lane activation and do not require constructing an analyst.
        assert!(services
            .evolution_analysis("absent-case")
            .unwrap()
            .is_none());
        assert!(services.evolution_cases(1).unwrap().is_empty());
        assert!(
            !ids.is_empty(),
            "ordinary recovery/read-model owners remain registered"
        );
        let mut observation = crate::EvolutionSignal::low_novelty_tool_loop(
            "runtime",
            "background-control-observation",
            vec![harness_contract::reality::EvidenceRef::observed(
                "runtime_event",
                "background-control-source",
            )],
        );
        observation.severity = crate::EvolutionSignalSeverity::Critical;
        let recorded = services
            .record_evolution_signal(observation.clone())
            .unwrap();
        assert_eq!(
            recorded, observation,
            "raw source survives independent of policy"
        );
        assert_eq!(
            services.evolution_signal(&recorded.signal_id).unwrap(),
            Some(recorded.clone())
        );
        assert_eq!(
            services.record_evolution_signal(recorded.clone()).unwrap(),
            recorded
        );
        assert_eq!(
            services.evolution_cases(10).unwrap().len(),
            usize::from(production_default)
        );
        let mut conflicting = recorded.clone();
        conflicting.summary = "changed payload under the original identity".into();
        assert!(services
            .record_evolution_signal(conflicting)
            .unwrap_err()
            .to_string()
            .contains("signal idempotency conflict"));
        if production_default {
            let precondition_executor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            precondition_executor.block_on(async {
                assert!(services
                    .drain_evolution_background_physical(tokio::time::Instant::now())
                    .await
                    .unwrap_err()
                    .contains("stopped"));
            });
        }
        services.stop_evolution_background_admission();
        let cases_before = services.evolution_cases(10).unwrap();
        let mut after_stop = observation;
        after_stop.signal_id = "evo-signal-background-after-stop".into();
        after_stop.source.session_id = Some("background-after-stop".into());
        services
            .record_evolution_signal(after_stop.clone())
            .unwrap();
        assert_eq!(
            services.evolution_signal(&after_stop.signal_id).unwrap(),
            Some(after_stop)
        );
        assert_eq!(
            services.evolution_cases(10).unwrap(),
            cases_before,
            "stopped direct observation cannot bypass automatic lane admission"
        );
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        executor.block_on(async {
            if !production_default {
                let error = services
                    .analyze_evolution_case("absent-case", "not-a-provider")
                    .await
                    .unwrap_err();
                assert!(
                    error.to_string().contains("evolution_background_disabled"),
                    "{error}"
                );
                services
                    .drain_evolution_background_physical(
                        tokio::time::Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .unwrap();
            } else {
                let reports = services
                    .drain_evolution_background_physical(
                        tokio::time::Instant::now() + Duration::from_secs(2),
                    )
                    .await
                    .unwrap();
                let mut projection_ids = reports.iter().map(|report| report.projection_id.as_str()).collect::<Vec<_>>();
                projection_ids.sort_unstable();
                assert_eq!(projection_ids, [crate::evolution::collaboration_experience::PROJECTOR_ID, crate::evolution::projector::PROJECTOR_ID]);
                assert!(reports.iter().all(|report| report.last_error.is_none()));
                let remaining = services
                    .event_reactor
                    .health()
                    .unwrap()
                    .lanes
                    .into_iter()
                    .map(|lane| lane.projection_id)
                    .collect::<BTreeSet<_>>();
                let expected = ids
                    .iter()
                    .filter(|id| {
                        id.as_str() != "projector:evolution-case:v2"
                            && id.as_str() != "projector:agentic-collaboration-experience:v1"
                    })
                    .cloned()
                    .collect::<BTreeSet<_>>();
                assert_eq!(remaining, expected, "stop only this background domain");
            }
            let growth_before = services.background_work().growth;
            services.resume_evolution_background_admission().unwrap();
            services.resume_evolution_background_admission().unwrap();
            assert_eq!(
                services.background_work().growth,
                growth_before,
                "Evolution control cannot alter Growth policy"
            );
            let resumed = services.event_reactor_health().unwrap();
            assert_eq!(
                resumed
                    .lanes
                    .iter()
                    .filter(|lane| matches!(
                        lane.projection_id.as_str(),
                        "projector:evolution-case:v2"
                            | "projector:agentic-collaboration-experience:v1"
                    ))
                    .count(),
                2,
                "repeated on must leave exactly the original two lanes"
            );
            services.evolution_signal_projector.run_once(128).unwrap();
            assert_eq!(
                services.evolution_cases(10).unwrap(),
                cases_before,
                "resume must not bootstrap paused raw signals into new Cases"
            );
            let mut new_observation = recorded.clone();
            new_observation.signal_id = "evo-signal-background-after-resume".into();
            new_observation.source.session_id = Some("background-after-resume".into());
            services.record_evolution_signal(new_observation).unwrap();
            assert_eq!(
                services.evolution_cases(10).unwrap().len(),
                cases_before.len() + 1,
                "current observation remains eligible after resume"
            );
            services.stop_evolution_background_admission();
            services
                .drain_evolution_background_physical(
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
                .unwrap();
            services.shutdown_maintenance().await;
            let report = services.shutdown_execution().await;
            assert_eq!(report.forced_aborts, 0);
            assert_eq!(report.remaining_keys, 0);
            assert!(report.errors.is_empty(), "{report:?}");
        });
        drop(services);
    }
}

fn publish_agent_test_policy(services: &RuntimeServices, session_id: &str) {
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
            context_reservation_tokens: Some(1),
            output_reservation_tokens: Some(1),
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

fn append_episode_pattern_fixture(
    services: &RuntimeServices,
    episode_ids: &[String],
    turn_refs: &[String],
) {
    let signature = episode_baseline_signature();
    let signature_digest = signature.digest();
    let pattern_id = harness_contract::evolution::CollaborationSemanticPattern::deterministic_id(
        &signature_digest,
    );
    let pattern = harness_contract::evolution::CollaborationSemanticPattern {
        schema_version: harness_contract::evolution::COLLABORATION_PATTERN_SCHEMA_VERSION,
        pattern_id: pattern_id.clone(),
        pattern_revision: 1,
        signature_digest,
        semantic_suggestion: harness_contract::evolution::SemanticCollaborationSuggestion {
            required_capability_ids: signature.required_capability_ids.clone(),
            required_skill_ids: signature.required_skill_ids.clone(),
            required_tool_capabilities: signature.required_tool_capabilities.clone(),
            dependency_shapes: signature.dependency_shapes.clone(),
            acceptance_kinds: signature.acceptance_kinds.clone(),
            result_field_shapes: signature.result_field_shapes.clone(),
        },
        semantic_signature: signature,
        evidence_summary: harness_contract::evolution::PatternEvidenceSummary {
            eligible_episode_count: episode_ids.len() as u32,
            distinct_turn_count: turn_refs.len() as u32,
            evidence_ref_count: episode_ids.len() as u32,
            coverage_basis_points: 10_000,
        },
        lifecycle: harness_contract::evolution::SemanticPatternLifecycle::Advisory,
        qualifying_episode_ids: episode_ids.to_vec(),
        distinct_turn_ref_hashes: turn_refs.to_vec(),
        support_count: episode_ids.len() as u32,
        latest_completed_at_ms: 1,
    };
    assert!(pattern.is_actionable());
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: format!("evolution:pattern:{pattern_id}"),
            scope: crate::RuntimeEventScope::Evolution,
            kind: "evolution.collaboration_pattern.projected.v1".to_string(),
            status: Some("projected".to_string()),
            actor: Some("test".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"pattern": pattern}),
        })
        .expect("pattern fixture");
}

fn evaluation_digest(value: &impl serde::Serialize) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("evaluation fixture serializes"))
    )
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
fn builder_rejects_conflicting_process_command_manifests_before_runtime_recovery() {
    let temp = tempfile::tempdir().expect("temporary roots");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let first = crate::ProcessJsonlSpec::new("command:worker", "worker-a", Vec::new());
    let second = crate::ProcessJsonlSpec::new("command:worker", "worker-b", Vec::new());

    let result = RuntimeServices::test_builder(temp.path().join("home"), &workspace)
        .process_jsonl_commands([first, second])
        .build();
    let error = match result {
        Ok(_) => panic!("one command reference cannot select two executable manifests"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        RuntimeServicesError::AgentRuntime(message)
            if message.contains("already registered with a different digest")
    ));
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
                revision_ref: published.definition_ref.clone(),
            },
            evaluation_baseline: crate::EvolutionEvaluationBaseline::EpisodeSet {
                semantic_signature_digest: signature_digest,
                episode_ids,
                aggregate_digest,
                executable_baseline_ref: published.definition_ref,
                executable_baseline_content_digest: "sha256:baseline".to_string(),
                replay_manifest_ref: "replay:stable-bypass".to_string(),
                replay_manifest_digest: "sha256:replay".to_string(),
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
            subject: crate::EvolutionCandidateSubject::AgentDefinition {
                revision_ref: revision_ref.clone(),
            },
            evaluation_baseline: crate::EvolutionEvaluationBaseline::EpisodeSet {
                semantic_signature_digest: signature_digest,
                episode_ids,
                aggregate_digest,
                executable_baseline_ref: revision_ref,
                executable_baseline_content_digest: "sha256:baseline".to_string(),
                replay_manifest_ref: "replay:duplicate-turns".to_string(),
                replay_manifest_digest: "sha256:replay".to_string(),
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

#[tokio::test]
async fn generalized_episode_set_executable_baseline() {
    episode_set_pair(false).await;
}

#[tokio::test]
async fn paired_skill_executes_native_writes_and_reads_in_distinct_runtime_leases() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    episode_set_pair(true).await;
}

async fn episode_set_pair(real_skill: bool) {
    let temp = tempfile::tempdir().expect("temporary root");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let local = if real_skill {
        Some(paired_skill::Fixture::new(workspace.clone()).await)
    } else {
        None
    };
    let providers = crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "test".into(),
            crate::config::ProviderConfig {
                reasoning_effort: None,
                name: "test".into(),
                base_url: local.as_ref().map_or_else(
                    || "https://example.test/v1".into(),
                    |fixture| fixture.url.clone(),
                ),
                api_key: "test".into(),
                models: vec!["fast".into()],
                protocol: Some("responses".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    };
    let mut builder = RuntimeServices::test_builder(temp.path().join("home"), &workspace)
        .provider_registry(Arc::new(
            crate::ProviderRegistry::new_with_context_windows(
                providers,
                std::collections::BTreeMap::from([("fast".into(), 128_000)]),
            )
            .expect("providers"),
        ))
        .evolution_eval_runner(Arc::new(ReadinessOnlyEvolutionEvalRunner));
    if let Some(local) = &local {
        builder = builder.tool_execution_host(local.host.clone());
    }
    let services = builder.build().expect("runtime services");
    let captured_packets = Arc::new(Mutex::new(Vec::new()));
    if let Some(local) = &local {
        services.replace_skill_catalog(local.catalog());
        local.install_sessions(&services).await;
    }
    if !real_skill {
        let selector_slot = Arc::new(Mutex::new(None));
        if let Ok(written) = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/json".into(),
                    visibility_scope: "session:episode-session".into(),
                    expected_bytes: None,
                    original_name: None,
                },
                b"{}",
            )
            .await
        {
            *selector_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(written);
        }
        services
            .agent_runtime()
            .register_observation_authority_backend(Arc::new(CapturingAgentBackend {
                packets: Arc::clone(&captured_packets),
                retrieval_artifact: Some(selector_slot),
            }));
    }

    let definition_id = AgentDefinitionId::new(
        DefinitionScope::Workspace,
        "cowd/episode-executable-baseline",
    )
    .expect("definition id");
    let scenario_ref = "episode/replay";
    let instructions = "# Evaluated Agent\n\nReturn evidence-backed output.\n";
    let manifest = |revision| AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: definition_id.clone(),
        revision,
        name: format!("Evaluated Agent {revision}"),
        description: "EpisodeSet executable baseline fixture".to_string(),
        lifecycle: RevisionLifecycle::Published,
        executor: AgentExecutorPolicy::CowdNative,
        model_policy: AgentModelPolicy {
            profile: "test".to_string(),
            allowed_models: vec!["fast".to_string()],
            fallback_allowed: false,
        },
        cognitive_policy: AgentCognitivePolicy {
            context_profile: "sub_agent".to_string(),
            read_scopes: vec![CognitiveReadScope::Session],
            write_mode: CognitiveWriteMode::CandidateOnly,
        },
        capability_contract: AgentCapabilityContract {
            capability_ceiling: if real_skill {
                vec![AgentCapability::Read, AgentCapability::Write]
            } else {
                vec![AgentCapability::Read]
            },
            skill_refs: if real_skill {
                vec!["paired-evidence".into(), "paired-verification".into()]
            } else {
                Vec::new()
            },
            approval_required_for: Vec::new(),
        },
        output_contract: AgentOutputContract::reviewable(),
        evaluation: AgentEvaluationContract::single_release_gate(scenario_ref, "evidence"),
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

    let turn_refs = vec![
        "sha256:turn-one".to_string(),
        "sha256:turn-two".to_string(),
        "sha256:turn-three".to_string(),
    ];
    let episode_ids = turn_refs
        .iter()
        .enumerate()
        .map(|(index, turn_ref)| {
            append_episode_baseline_fixture(
                &services,
                &format!("program-executable-{index}"),
                turn_ref,
            )
        })
        .collect::<Vec<_>>();
    append_episode_pattern_fixture(&services, &episode_ids, &turn_refs);
    let signature_digest = episode_baseline_signature().digest();
    let aggregate_digest = harness_contract::evolution::collaboration_episode_set_digest(
        &signature_digest,
        &episode_ids,
    );

    let acceptance = if real_skill {
        vec!["isolated write".to_string(), "isolated reread".to_string()]
    } else {
        vec!["completed".to_string()]
    };
    use harness_contract::evaluation::{
        EvaluationAcceptanceCheck, EvaluationAcceptanceRequirement,
    };
    let acceptance_checks = if real_skill {
        vec![
            EvaluationAcceptanceRequirement {
                criterion: acceptance[0].clone(),
                check: EvaluationAcceptanceCheck::IsolatedWorkspaceChange {
                    field: harness_contract::agent::StructuredOutputField::Implementation,
                },
            },
            EvaluationAcceptanceRequirement {
                criterion: acceptance[1].clone(),
                check: EvaluationAcceptanceCheck::IsolatedWorkspaceRead,
            },
        ]
    } else {
        vec![EvaluationAcceptanceRequirement {
            criterion: acceptance[0].clone(),
            check: EvaluationAcceptanceCheck::Output {
                check: harness_contract::agent::OutputAcceptanceCheck::StructuredArtifact {
                    name: "summary".into(),
                },
            },
        }]
    };
    let objective = if real_skill { "Activate paired-evidence to write proof.txt inside your Runtime-issued evaluation_output_scope, then use paired-verification to read that exact produced file and verify the actual content. Return reviewable evidence." } else { "execute the frozen paired evaluation workload" }.to_string();
    let permission = if real_skill {
        harness_contract::policy::PermissionMode::WorkspaceWrite
    } else {
        harness_contract::policy::PermissionMode::ReadOnly
    };
    let allowed_tools = if real_skill {
        vec!["edit".to_string(), "read_file".to_string()]
    } else {
        vec![]
    };
    let allowed_skills = if real_skill {
        vec![
            "paired-evidence".to_string(),
            "paired-verification".to_string(),
        ]
    } else {
        vec![]
    };
    let environment_fingerprint = evaluation_digest(&serde_json::json!({
        "provider": "test",
        "model": "fast",
        "permission_ceiling": permission,
        "allowed_tools": allowed_tools,
        "allowed_skills": allowed_skills,
        "resource_scopes": Vec::<String>::new(),
    }));
    let replay = harness_contract::evaluation::FrozenEvaluationReplayManifest {
        manifest_ref: "replay:episode-executable".to_string(),
        source_episode_set_digest: aggregate_digest.clone(),
        input_digest: evaluation_digest(&objective),
        attachment_refs: Vec::new(),
        environment_fingerprint,
        rubric_digest: evaluation_digest(&(&acceptance, &acceptance_checks)),
    };
    let scenario = harness_contract::evaluation::EvaluationScenarioSpec {
        scenario_ref: scenario_ref.to_string(),
        objective,
        acceptance,
        acceptance_checks,
        allowed_tools,
        allowed_skills,
        resource_scopes: Vec::new(),
        permission_ceiling: permission,
        model_lease: "fast".to_string(),
        replay_manifest: Some(replay.clone()),
    };
    scenario.validate().expect("valid replay scenario");

    let proposal_id = approved_evolution_proposal(&services, "executable-pair");
    let candidate = services
        .register_evolution_candidate(crate::EvolutionCandidateIntent {
            candidate_id: "candidate-episode-executable".to_string(),
            proposal_id,
            subject: crate::EvolutionCandidateSubject::AgentDefinition {
                revision_ref: candidate_revision.revision.revision_ref.clone(),
            },
            evaluation_baseline: crate::EvolutionEvaluationBaseline::EpisodeSet {
                semantic_signature_digest: signature_digest,
                episode_ids,
                aggregate_digest,
                executable_baseline_ref: baseline.revision.revision_ref.clone(),
                executable_baseline_content_digest: baseline.revision.content_digest.clone(),
                replay_manifest_ref: replay.manifest_ref,
                replay_manifest_digest: scenario.executable_replay_digest(),
            },
            source_evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test",
                "episode-executable-pair",
            )],
            canary_policy: Default::default(),
        })
        .expect("candidate registered without a Stable release");
    for side in ["baseline", "candidate"] {
        publish_agent_test_policy(
            &services,
            &format!("evolution-eval:{}:{side}:0", candidate.candidate_id),
        );
    }
    let (baseline_observation, candidate_observation) = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        services.execute_evolution_agent_scenario(&candidate.candidate_id, &scenario, 0),
    )
    .await
    .expect("paired original Runner deadline")
    .expect("both revisions execute from the frozen EpisodeSet replay");
    assert_eq!(baseline_observation.definition_revision, 1);
    assert_eq!(candidate_observation.definition_revision, 2);
    if !(baseline_observation.succeeded && candidate_observation.succeeded) {
        if let Some(local) = &local {
            eprintln!("{}", local.diagnostics());
        }
    }
    assert!(
        baseline_observation.succeeded && candidate_observation.succeeded,
        "baseline={baseline_observation:?}; candidate={candidate_observation:?}; returns={:?}",
        ["baseline", "candidate"].map(|side| services.agent_runtime().terminal_return(&format!(
            "instance:evolution-eval:{}:{}:{side}:{}:0",
            candidate.candidate_id,
            scenario.scenario_ref,
            if side == "baseline" { 1 } else { 2 }
        )))
    );
    assert_ne!(baseline_observation.run_ref, candidate_observation.run_ref);
    assert_eq!(
        baseline_observation.environment_fingerprint,
        candidate_observation.environment_fingerprint
    );
    if let Some(local) = local {
        let repeated = services
            .execute_evolution_agent_scenario(&candidate.candidate_id, &scenario, 0)
            .await
            .expect("same sample reattaches without renewing its budget");
        assert_eq!(repeated.0, baseline_observation);
        assert_eq!(repeated.1, candidate_observation);
        assert!(repeated.0.succeeded && repeated.1.succeeded);
        local.verify(&services).await;
        assert!(services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
        return;
    }
    let packets = captured_packets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let [_first_packets_964, _second_packets_964] = packets.as_slice() else { panic!("expected both original packets exactly once") };
    assert_ne!(_first_packets_964.session_id(), packets[1].session_id());
    let isolated_output = |packet: &AgentTaskPacket| {
        packet
            .resource_scopes
            .iter()
            .find(|scope| scope.starts_with("write:.cowd/evaluation/"))
            .cloned()
            .expect("Runtime-issued isolated output lease")
    };
    assert_ne!(isolated_output(&packets[0]), isolated_output(&packets[1]));
}

#[test]
fn generalized_paired_write_skill_isolation() {
    let scenario = |resource_scopes: Vec<String>, allowed_skills: Vec<String>| {
        harness_contract::evaluation::EvaluationScenarioSpec {
            scenario_ref: "episode/isolation".to_string(),
            objective: "exercise a paired workload".to_string(),
            acceptance: vec!["reviewable evidence".to_string()],
            acceptance_checks: vec![
                harness_contract::evaluation::EvaluationAcceptanceRequirement {
                    criterion: "reviewable evidence".into(),
                    check: harness_contract::evaluation::EvaluationAcceptanceCheck::Output {
                        check: harness_contract::agent::OutputAcceptanceCheck::StructuredArtifact {
                            name: "evidence".into(),
                        },
                    },
                },
            ],
            allowed_tools: Vec::new(),
            allowed_skills,
            resource_scopes,
            permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
            model_lease: "fast".to_string(),
            replay_manifest: None,
        }
    };
    let catalog = crate::RuntimeSkillCatalog::new(Vec::new());
    let shared_write = validate_evolution_scenario_isolation(
        &scenario(vec!["write:shared-output".to_string()], Vec::new()),
        None,
        &catalog,
    )
    .expect_err("caller-provided shared writes must be rejected before execution");
    assert!(matches!(
        shared_write,
        RuntimeServicesError::Invariant(message)
            if message.contains("cannot share writable input scopes")
    ));

    let missing_skill = validate_evolution_scenario_isolation(
        &scenario(Vec::new(), vec!["skill:not-installed".to_string()]),
        None,
        &catalog,
    )
    .expect_err("an unavailable Skill must fail before provider admission");
    assert!(matches!(
        missing_skill,
        RuntimeServicesError::Invariant(message)
            if message.contains("unavailable Skills")
    ));

    validate_evolution_scenario_isolation(&scenario(Vec::new(), Vec::new()), None, &catalog)
        .expect("read-only inputs with Runtime-issued output scopes are isolated");
    for check in [
        harness_contract::evaluation::EvaluationAcceptanceCheck::IsolatedWorkspaceRead,
        harness_contract::evaluation::EvaluationAcceptanceCheck::IsolatedWorkspaceChange {
            field: harness_contract::agent::StructuredOutputField::Implementation,
        },
    ] {
        let mut missing_tool = scenario(Vec::new(), Vec::new());
        missing_tool.acceptance_checks[0].check = check;
        assert!(
            validate_evolution_scenario_isolation(&missing_tool, None, &catalog)
                .unwrap_err()
                .to_string()
                .contains("has no authorized file")
        );
    }
}

#[test]
fn startup_recovers_task_outbox_without_mutating_mission_membership() {
    let root = tempfile::tempdir().expect("runtime root");
    let home = root.path().join("home");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let backends = TestRuntimeBackends::new(root.path());
    let first = backends
        .builder(&home, &workspace)
        .build()
        .expect("first runtime");
    publish_agent_test_policy(&first, "session-startup-recovery");
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

    let recovered = backends
        .builder(&home, &workspace)
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
async fn evolution_maintenance_drain_keeps_work_after_waiter_drop_without_stopping_memory_owner() {
    let supervisor = RuntimeMaintenanceSupervisor::new();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let memory_release = Arc::new(tokio::sync::Notify::new());
    let returned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<()>();
    supervisor
        .try_submit("evolution:analysis:controlled".into(), {
            let entered = entered.clone();
            let release = release.clone();
            let returned = returned.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                returned.store(true, Ordering::SeqCst);
                let _ = result_tx.send(());
            }
        })
        .unwrap();
    supervisor
        .try_submit("memory:ordinary".into(), {
            let release = memory_release.clone();
            async move {
                release.notified().await;
            }
        })
        .unwrap();
    drop(result_rx);
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(supervisor.pending_for_owner_prefix("evolution:") > 0);
    assert!(supervisor
        .drain_owner_prefix("evolution:", tokio::time::Instant::now())
        .await
        .is_err());
    assert!(!returned.load(Ordering::SeqCst));
    release.notify_one();
    supervisor
        .drain_owner_prefix(
            "evolution:",
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(returned.load(Ordering::SeqCst));
    assert_eq!(supervisor.pending_for_owner_prefix("evolution:"), 0);
    assert_eq!(supervisor.pending_for_owner_prefix("memory:"), 1);
    memory_release.notify_one();
    supervisor.shutdown_and_drain().await;
    assert_eq!(supervisor.pending_for_owner_prefix("memory:"), 0);
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
    publish_agent_test_policy(&services, "session-completion-1");
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
    publish_agent_test_policy(&services, "session-completion-race");
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

#[tokio::test]
async fn runtime_skill_catalog_is_available_to_delegated_execution_services() {
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
    services.replace_skill_catalog(crate::skill::test_support::catalog(
        vec![profile],
        vec![crate::RuntimeSkillPromptAsset {
            skill_id: "delegated-review".to_string(),
            version: Some("1.0.0".to_string()),
            content: "Review evidence before returning.".to_string(),
            pin: crate::skill::test_support::pin(
                "delegated-review",
                Some("1.0.0"),
                "Review evidence before returning.",
                &[],
            ),
            source_ref: "skill://delegated-review/SKILL.md".to_string(),
            tool_refs: Vec::new(),
        }],
    ));

    let catalog = services.skill_catalog();
    assert_eq!(catalog.profiles().unwrap()[0].skill_id, "delegated-review");
    let asset = catalog
        .instruction_source()
        .unwrap()
        .load_instruction(
            &crate::SkillInvocation {
                skill_id: "delegated-review".into(),
                skill_version: Some("1.0.0".into()),
                adapter: SkillAdapterKind::PromptOnly,
                entrypoint: None,
            },
            &crate::RuntimeSkillUsageContext {
                workspace_identity: "workspace".into(),
                workload_fingerprint: "workload".into(),
                config_revision: "config".into(),
                evaluation_environment: "test".into(),
                execution_id: "execution".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
                observed_at_ms: 1,
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(asset.source_ref, "skill://delegated-review/SKILL.md");
}

struct TestExecutionHost;

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for TestExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        crate::RuntimeToolExecutionOutcome {
            pending_agent_batch: None,
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

struct CompletedAgentBackend {
    /// The artifact this backend cites. A synthetic selector with a made-up
    /// digest is rejected by the typed-selector and identity checks, which is
    /// correct: the fixture must cite an artifact it actually wrote.
    retrieval_artifact: Option<Arc<Mutex<Option<harness_contract::context::ArtifactRef>>>>,
}

impl Default for CompletedAgentBackend {
    fn default() -> Self {
        Self {
            retrieval_artifact: None,
        }
    }
}

struct CapturingAgentBackend {
    packets: Arc<Mutex<Vec<AgentTaskPacket>>>,
    retrieval_artifact: Option<Arc<Mutex<Option<harness_contract::context::ArtifactRef>>>>,
}

#[async_trait::async_trait]
impl crate::AgentRuntimeBackend for CapturingAgentBackend {
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
        self.packets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(packet.clone());
        CompletedAgentBackend {
            retrieval_artifact: self.retrieval_artifact.clone(),
        }
        .execute(packet, selection)
        .await
    }
}

struct ParallelTrackingAgentBackend {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    retrieval_artifact: Option<Arc<Mutex<Option<harness_contract::context::ArtifactRef>>>>,
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
        let artifact = self.retrieval_artifact.as_ref().and_then(|slot| {
            slot.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        let mut evidence_refs = packet.evidence_refs.clone();
        evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed(
                "tool",
                format!("materialized:{}", packet.node_id()),
            ),
            artifact
                .as_ref()
                .map_or_else(|| "a".repeat(64), |written| written.sha256.clone()),
            artifact.as_ref().map_or(1, |written| written.bytes),
            artifact
                .as_ref()
                .map_or_else(|| "application/json".to_string(), |written| written.media_type.clone()),
            artifact.as_ref().map_or_else(
                || "artifact://art_runtime_services_packet".to_string(),
                |written| written.selector.clone(),
            ),
            artifact.as_ref().map_or_else(
                || format!("session:{}", packet.session_id()),
                |written| written.visibility_scope.clone(),
            ),
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
                    bytes: None,
                    reread_sequence: None,
                    reread_evidence_ref: None,
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
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
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
        let artifact = self.retrieval_artifact.as_ref().and_then(|slot| {
            slot.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        });
        let mut evidence_refs = packet.evidence_refs.clone();
        evidence_refs.push(harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed(
                "tool",
                format!("materialized:{}", packet.node_id()),
            ),
            artifact
                .as_ref()
                .map_or_else(|| "a".repeat(64), |written| written.sha256.clone()),
            artifact.as_ref().map_or(1, |written| written.bytes),
            artifact
                .as_ref()
                .map_or_else(|| "application/json".to_string(), |written| written.media_type.clone()),
            artifact.as_ref().map_or_else(
                || "artifact://art_runtime_services_shared".to_string(),
                |written| written.selector.clone(),
            ),
            artifact.as_ref().map_or_else(
                || format!("session:{}", packet.session_id()),
                |written| written.visibility_scope.clone(),
            ),
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
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
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
                wait_class: None,
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
    let services = RuntimeServices::test_builder(temp.path().join("home"), &workspace)
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
    let services = RuntimeServices::test_builder(temp.path().join("home"), &workspace)
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
    let services = RuntimeServices::test_builder(temp.path().join("home"), &workspace)
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
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let store = Arc::new(crate::test_support::session_store());
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
    let mut builder = RuntimeServices::builder(temp.path(), temp.path().join("partial"));
    builder.session_query_port = Some(ports);
    let result = builder.build();

    assert!(matches!(
        result,
        Err(RuntimeServicesError::IncompleteSessionPorts)
    ));
}

#[tokio::test]
async fn session_integrated_runtime_holds_all_autonomous_producers_until_recovery_release() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("producer-gate");
    std::fs::create_dir_all(&workspace).unwrap();
    let store = Arc::new(crate::test_support::session_store());
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .session_ports(ports.clone(), ports.clone(), ports.clone(), ports)
        .build()
        .expect("session-integrated Runtime");
    // Bind this graph's conversation-scoped executor. Release wakes only graphs
    // whose turn-scoped executors are bound; an unbound graph is deliberately
    // held for its Session host instead of being woken into `unavailable`.
    services
        .tool_batch_executor()
        .install_resolver(Arc::new(ServiceScopedResolver {
            payload_ref: "{}".to_string(),
            backend: Arc::new(ServiceScopedBackend {
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        }));
    assert!(matches!(
        services
            .execution_supervisor()
            .admit_owned("must-wait-for-recovery", Box::pin(async { Ok(()) }))
            .await,
        Err(crate::execution_core::ExecutionRunnerError::SupervisorUnavailable(_))
    ));
    let mut graph = ExecutionGraph::new("recovery-gated graph");
    graph.id = "recovery-gated-graph".to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    graph.nodes.push(ExecutionNodeSpec::new(
        ExecutionNodeKind::ToolBatch,
        "tool_batch",
        "{}",
    ));
    let receipt = services
        .execution_supervisor()
        .submit(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .expect("durably admit graph behind recovery gate");
    let gated = services
        .graph_state_store()
        .load(&receipt.graph_id)
        .expect("gated graph");
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert_eq!(
        services
            .graph_state_store()
            .load(&receipt.graph_id)
            .expect("still gated")
            .revision,
        gated.revision,
        "a submitted graph must not start before ordered recovery releases it"
    );

    assert!(
        services
            .event_reactor_health()
            .expect("reactor health")
            .lanes
            .iter()
            .all(|lane| !lane.worker_running),
        "event reactor lanes must remain closed before ordered recovery"
    );
    assert!(
        !services.approval_queue().deadline_scheduler_running(),
        "expired approval deadlines must not advance graphs before ordered recovery"
    );

    services
        .release_recovered_producers()
        .await
        .expect("release recovered producers");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if services
                .event_reactor_health()
                .expect("reactor health")
                .lanes
                .iter()
                .all(|lane| lane.worker_running)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all reactor lanes start after release");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if services
                .graph_state_store()
                .load(&receipt.graph_id)
                .expect("released graph")
                .revision
                > gated.revision
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("deferred graph starts exactly after release");
    assert!(services.approval_queue().deadline_scheduler_running());
    services
        .execution_supervisor()
        .admit_owned("released-owned-work", Box::pin(async { Ok(()) }))
        .await
        .expect("owned work starts only after release");

    services
        .release_recovered_producers()
        .await
        .expect("release is idempotent");
    assert!(services.approval_queue().deadline_scheduler_running());
}

#[test]
fn workspace_builders_isolate_provider_tool_host_and_session_router() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let left_provider = Arc::new(crate::ProviderRegistry::empty());
    let right_provider = Arc::new(crate::ProviderRegistry::empty());
    let left_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
    let right_tool: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(TestExecutionHost);
    let left_store = Arc::new(crate::test_support::session_store());
    let right_store = Arc::new(crate::test_support::session_store());
    std::fs::create_dir_all(temp.path().join("left")).unwrap();
    std::fs::create_dir_all(temp.path().join("right")).unwrap();

    let left_ports =
        crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&left_store));
    let right_ports =
        crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&right_store));
    let left = RuntimeServices::test_builder(temp.path(), temp.path().join("left"))
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
    let right = RuntimeServices::test_builder(temp.path(), temp.path().join("right"))
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
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let store = Arc::new(crate::test_support::session_store());
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
    assert!(matches!(first.tick.claimed.as_slice(), [fire] if fire.mission_id == "schedule-mission" && fire.target_session_id == "scheduled-target" && fire.due_at_ms == due_at_ms));
    assert!(matches!(first.submitted.as_slice(), [submission]
        if submission.target_policy_binding.as_ref().is_some_and(|binding| binding.policy_revision == 7)));
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
        .claim_session_runtime_outbox(
            "schedule-test",
            due_at_ms.saturating_add(5_000),
            1_000,
            8,
            &[],
        )
        .await
        .unwrap();
    assert!(matches!(target_outbox.as_slice(), [item] if item.session_id == "scheduled-target"));
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
    let first = RuntimeServices::test_builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let second = RuntimeServices::test_builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let isolated = RuntimeServices::test_builder(temp.path(), temp.path().join("other"))
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

    let first = RuntimeServices::test_builder(temp.path().join("home-a"), &workspace)
        .build()
        .unwrap();
    let second = RuntimeServices::test_builder(temp.path().join("home-b"), &alias)
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
    let services = RuntimeServices::test_builder(temp.path(), &workspace)
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
#[ignore = "needs a durably bound running node; startup recovery only resumes bindings that were really started"]
async fn startup_recovery_rehydrates_and_advances_persistent_execution_graphs() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let cowd_home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&cowd_home).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());

    let graph_id = {
        let services = backends.builder(&cowd_home, &workspace).build().unwrap();
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

    let restarted = backends.builder(&cowd_home, &workspace).build().unwrap();
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
#[ignore = "needs a durably bound running node; startup recovery only resumes bindings that were really started"]
async fn startup_recovery_defers_turn_scoped_graph_until_exact_resolver_is_bound() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let cowd_home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&cowd_home).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());
    let graph_id = {
        let services = backends.builder(&cowd_home, &workspace).build().unwrap();
        let mut graph = ExecutionGraph::new("defer until Session binding");
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "payload:late-session-binding",
        );
        node.id = "late-session-node".to_string();
        node.idempotency_key = "idempotency:late-session-node".to_string();
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
                "late-session-node",
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        services
            .commit_service()
            .transition_node(
                &graph,
                "late-session-node",
                ExecutionNodeStatus::Running,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph
            .id
    };

    let restarted = backends.builder(&cowd_home, &workspace).build().unwrap();
    let report = restarted
        .recover_execution_graphs_on_startup()
        .await
        .expect("startup recovery");
    assert_eq!(report.recovered_graphs, 1);
    assert_eq!(report.notified_graphs, 0);
    assert_eq!(report.deferred_graphs, 1);
    assert!(report.records.iter().any(|record| {
        record.graph_id == graph_id && record.action == "deferred_turn_scoped_executor_binding"
    }));
    assert_eq!(
        restarted
            .graph_state_store()
            .load(&graph_id)
            .unwrap()
            .node_statuses["late-session-node"],
        ExecutionNodeStatus::Ready
    );

    let calls = Arc::new(AtomicUsize::new(0));
    restarted
        .tool_batch_executor()
        .install_resolver(Arc::new(ServiceScopedResolver {
            payload_ref: "payload:late-session-binding".to_string(),
            backend: Arc::new(ServiceScopedBackend {
                calls: Arc::clone(&calls),
            }),
        }));
    restarted
        .execution_supervisor()
        .notify_graph(&graph_id)
        .await
        .expect("Session-bound recovery wake");
    restarted
        .execution_supervisor()
        .wait_for_quiescence(&graph_id)
        .await
        .expect("rebound graph completes");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "needs a durably bound running node; startup recovery only resumes bindings that were really started"]
async fn startup_recovery_quarantines_one_failed_session_without_blocking_healthy_graphs() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let cowd_home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&cowd_home).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());

    let (healthy_graph_id, quarantined_graph_id, quarantined_revision) = {
        let services = backends.builder(&cowd_home, &workspace).build().unwrap();
        let mut persisted = Vec::new();
        for (graph_id, session_id, node_id) in [
            ("healthy-graph", "healthy-session", "healthy-node"),
            ("quarantined-graph", "failed-session", "quarantined-node"),
        ] {
            let mut graph = harness_contract::execution_graph::ExecutionGraph::new(
                "partitioned startup recovery",
            );
            graph.id = graph_id.to_string();
            crate::test_support::attach_execution_graph_lineage(&mut graph);
            graph.lineage.as_mut().unwrap().session_id = session_id.to_string();
            let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
                harness_contract::execution_graph::ExecutionNodeKind::ToolBatch,
                "tool_batch",
                "payload:partitioned-recovery",
            );
            node.id = node_id.to_string();
            node.idempotency_key = format!("idempotency:{node_id}");
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
                    node_id,
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
                    node_id,
                    ExecutionNodeStatus::Running,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            persisted.push((graph.id, graph.revision));
        }
        (
            persisted[0].0.clone(),
            persisted[1].0.clone(),
            persisted[1].1,
        )
    };

    let restarted = backends.builder(&cowd_home, &workspace).build().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    restarted
        .tool_batch_executor()
        .install_resolver(Arc::new(ServiceScopedResolver {
            payload_ref: "payload:partitioned-recovery".to_string(),
            backend: Arc::new(ServiceScopedBackend {
                calls: Arc::clone(&calls),
            }),
        }));
    let excluded_sessions = BTreeSet::from(["failed-session".to_string()]);

    let report = restarted
        .recover_execution_graphs_on_startup_excluding_sessions(&excluded_sessions)
        .await
        .expect("partitioned startup recovery");

    assert_eq!(report.examined_graphs, 2);
    assert_eq!(report.deferred_graphs, 1);
    assert_eq!(report.recovered_graphs, 1);
    assert_eq!(report.notified_graphs, 1);
    assert!(report.errors.is_empty());
    assert!(report.records.iter().any(|record| {
        record.graph_id == quarantined_graph_id && record.action == "deferred_session_hydration"
    }));
    restarted
        .execution_supervisor()
        .wait_for_quiescence(&healthy_graph_id)
        .await
        .expect("healthy Session graph reaches quiescence");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let quarantined = restarted
        .graph_state_store()
        .load(&quarantined_graph_id)
        .unwrap();
    assert_eq!(quarantined.revision, quarantined_revision);
    assert_eq!(
        quarantined.node_statuses["quarantined-node"],
        ExecutionNodeStatus::Running
    );
}

#[tokio::test]
#[ignore = "a graph record without lineage cannot be written; the store refuses it before recovery can observe it"]
async fn startup_recovery_rejects_missing_lineage_before_any_durable_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let cowd_home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&cowd_home).unwrap();
    let services = RuntimeServices::test_builder(&cowd_home, &workspace)
        .build()
        .unwrap();
    let mut graph = ExecutionGraph::new("corrupt unscoped recovery graph");
    graph.id = "missing-lineage-recovery".to_string();
    graph.revision = 1;
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::ToolBatch,
        "tool_batch",
        "payload:must-not-run",
    );
    node.id = "unscoped-node".to_string();
    graph.nodes.push(node);
    graph
        .node_statuses
        .insert("unscoped-node".to_string(), ExecutionNodeStatus::Ready);
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: graph.id.clone(),
            scope: crate::RuntimeEventScope::ExecutionGraph,
            kind: "execution_graph.planned".to_string(),
            status: Some("ready".to_string()),
            actor: Some("corruption-fixture".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({"event": "planned", "graph": graph}),
        })
        .unwrap();
    let before_revision = services
        .event_store()
        .stream_revision("missing-lineage-recovery")
        .unwrap();

    let report = services
        .recover_execution_graphs_on_startup()
        .await
        .expect("invalid lineage is reported, not executed");

    assert!(matches!(report.errors.as_slice(), [error] if error.graph_id == "missing-lineage-recovery"));
    assert!(matches!(report.records.as_slice(), [record] if record.action == "invalid_lineage"));
    assert_eq!(report.notified_graphs, 0);
    assert_eq!(
        services
            .event_store()
            .stream_revision("missing-lineage-recovery")
            .unwrap(),
        before_revision,
        "lineage validation must run before any recovery mutation"
    );
}

#[tokio::test]
async fn startup_recovery_does_not_resolve_handoff_for_an_excluded_session() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let cowd_home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&cowd_home).unwrap();
    let services = RuntimeServices::test_builder(&cowd_home, &workspace)
        .build()
        .unwrap();
    services
        .install_test_session_store(Arc::new(crate::test_support::session_store()))
        .unwrap();
    let handoff = harness_contract::turn::SessionHandoff {
        handoff_id: "excluded-handoff".to_string(),
        source_session_id: "failed-session".to_string(),
        target_session_id: "target-session".to_string(),
        objective: "return a durable result".to_string(),
        acceptance: Vec::new(),
        scope: Vec::new(),
        context_lens: Vec::new(),
        evidence_refs: Vec::new(),
        context_budget_lease: None,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        deadline_at_ms: None,
        priority: 128,
        correlation_id: "excluded-correlation".to_string(),
        result_contract: "result".to_string(),
        task_route_hint: None,
    };
    let command = harness_contract::turn::SessionDispatchCommand {
        command_id: "excluded-command".to_string(),
        action: harness_contract::turn::SessionDispatchAction::Enqueue,
        handoff: handoff.clone(),
        expected_target_revision: 0,
    };
    let mut source = ExecutionGraph::new("excluded handoff source");
    source.id = "excluded-handoff-source".to_string();
    crate::test_support::attach_execution_graph_lineage(&mut source);
    source.lineage.as_mut().unwrap().session_id = "failed-session".to_string();
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::SessionDispatch,
        crate::SESSION_DISPATCH_EXECUTOR,
        format!(
            "session_handoff:{}",
            serde_json::to_string(&command).unwrap()
        ),
    );
    node.id = "excluded-handoff-node".to_string();
    node.idempotency_key = "excluded-handoff-node".to_string();
    source.nodes.push(node);
    let mut source = services.commit_service().register_graph(source).unwrap().graph;
    for status in [
        ExecutionNodeStatus::Ready,
        ExecutionNodeStatus::Running,
        ExecutionNodeStatus::WaitingExternal,
    ] {
        source = services
            .commit_service()
            .transition_node(&source, "excluded-handoff-node", status, None, Vec::new())
            .unwrap()
            .graph;
    }
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: "session-handoff-target:excluded-request".to_string(),
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "session.handoff.accepted.v1".to_string(),
            status: Some("accepted".to_string()),
            actor: Some("handoff-recovery-fixture".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({
                "handoff": handoff,
                "request_id": "excluded-request",
                "receipt": {
                    "command_id": "excluded-command",
                    "source_node_id": "excluded-handoff-node",
                    "target_session_id": "target-session",
                    "target_turn_id": "target-turn",
                    "accepted_revision": 1,
                    "status": "accepted"
                },
                "source_graph_id": "excluded-handoff-source",
                "source_node_id": "excluded-handoff-node"
            }),
        })
        .unwrap();
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: "session-handoff-correlation:excluded-correlation".to_string(),
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "session.handoff.result.v1".to_string(),
            status: Some("completed".to_string()),
            actor: Some("handoff-recovery-fixture".to_string()),
            refs: Vec::new(),
            payload: serde_json::json!({
                "correlation_id": "excluded-correlation",
                "source_session_id": "failed-session",
                "target_session_id": "target-session",
                "result_ref": "artifact:target-result",
                "evidence_refs": [],
                "unresolved": [],
                "conflict_refs": [],
                "input_tokens": 1,
                "output_tokens": 1
            }),
        })
        .unwrap();
    let before_revision = services
        .event_store()
        .stream_revision("excluded-handoff-source")
        .unwrap();

    let report = services
        .recover_execution_graphs_on_startup_excluding_sessions(&BTreeSet::from([
            "failed-session".to_string()
        ]))
        .await
        .expect("partitioned handoff recovery");

    assert_eq!(report.resolved_handoff_results, 0);
    assert_eq!(report.deferred_graphs, 1);
    assert!(report.errors.is_empty());
    let source = services
        .graph_state_store()
        .load("excluded-handoff-source")
        .unwrap();
    assert_eq!(source.revision, before_revision);
    assert_eq!(
        source.node_statuses["excluded-handoff-node"],
        ExecutionNodeStatus::WaitingExternal
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
                reasoning_effort: None,
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
    let services = RuntimeServices::test_builder(temp.path(), &workspace)
        .provider_registry(Arc::new(crate::ProviderRegistry::new(providers).unwrap()))
        .build()
        .unwrap();
    publish_agent_test_policy(&services, "agent-runtime-session");
    let selector_slot = Arc::new(Mutex::new(None));
    let written = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/json".into(),
                visibility_scope: "session:agent-runtime-session".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"{}",
        )
        .await
        .expect("registered observation artifact");
    *selector_slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(written);
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(CompletedAgentBackend {
            retrieval_artifact: Some(Arc::clone(&selector_slot)),
        }));

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
        required_acceptance: harness_contract::context::RequiredAcceptance {
            criteria: vec!["completed".into()],
            evidence_obligations: Vec::new(),
        },
        output_acceptance: Vec::new(),
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
        agentic_binding: None,
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
    let events = services.agent_runtime().events(packet.agent_id());
    assert_eq!(events.iter().map(|event| event.kind.as_str()).collect::<Vec<_>>(), ["agent.prepared", "agent.running", "agent.terminal"]);
    assert!(events.iter().all(|event| event.agent_id == packet.agent_id()));
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
                reasoning_effort: None,
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
    let services = RuntimeServices::test_builder(temp.path(), &workspace)
        .provider_registry(Arc::new(
            crate::ProviderRegistry::new(providers).expect("provider"),
        ))
        .build()
        .expect("runtime services");
    publish_agent_test_policy(&services, "binding-session");
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
    let selector_slot = Arc::new(Mutex::new(None));
    let shared_artifact = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/json".into(),
                visibility_scope: "session:binding-session".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"{}",
        )
        .await
        .expect("registered observation artifact");
    *selector_slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) =
        Some(shared_artifact);
    services
        .agent_runtime()
        .register_observation_authority_backend(Arc::new(ParallelTrackingAgentBackend {
            active: Arc::clone(&active),
            max_active: Arc::clone(&max_active),
            retrieval_artifact: Some(Arc::clone(&selector_slot)),
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
            required_acceptance: harness_contract::context::RequiredAcceptance {
                criteria: vec!["evidence".to_string()],
                evidence_obligations: Vec::new(),
            },
            output_acceptance: vec![harness_contract::agent::OutputAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence {
                    scopes: vec![format!("read:binding-domain-{index}")],
                },
            }],
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
            agentic_binding: None,
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
                harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence { scopes }
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
        .iter()
        .map(|agent_id| {
            services
                .agent_runtime()
                .get(agent_id)
                .expect("durable terminal Agent projection")
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshots.iter().map(|snapshot| &snapshot.agent_id).collect::<Vec<_>>(), agent_ids.iter().collect::<Vec<_>>());
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
    assert_eq!(instances.len(), bindings.len(), "each original agent must have a distinct instance");
    assert!(max_active.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn policy_drain_tracks_and_terminalizes_an_admitted_pre_graph_task() {
    let services = RuntimeServices::in_memory().expect("runtime services");
    publish_agent_test_policy(&services, "policy-drain-session");
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
    publish_agent_test_policy(&services, "policy-drain-graph-session");
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
    let mut parent_node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
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

#[test]
fn r1_bounded_history_session_timeline_scopes_and_commit_ties() {
    let store = Arc::new(crate::RuntimeEventStore::for_test());
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
    for sequence in 0..250 {
        store
            .append(crate::RuntimeEventInput {
                stream_id: "foreign-session".into(),
                scope: crate::RuntimeEventScope::SessionInput,
                kind: "foreign.event".into(),
                status: None,
                actor: None,
                refs: Vec::new(),
                payload: serde_json::json!({"sequence":sequence}),
            })
            .unwrap();
    }
    let reader = super::RuntimeEventReader { store };

    let first = reader
        .session_timeline_events("timeline-session", None, 1)
        .expect("first page").events;
    assert!(matches!(first.as_slice(), [item] if item.transaction_index == 0));
    assert_eq!(first[0].transaction_index, 0);
    let second = reader
        .session_timeline_events(
            "timeline-session",
            Some((first[0].commit_cursor, first[0].transaction_index)),
            1,
        )
        .expect("second page").events;
    assert!(matches!(second.as_slice(), [item] if item.transaction_index == 1));
    assert_eq!(second[0].transaction_index, 1);
    assert_eq!(second[0].kind, "timeline.second");
    assert!(reader
        .session_timeline_events(
            "timeline-session",
            Some((second[0].commit_cursor, second[0].transaction_index)),
            1
        )
        .unwrap()
        .events.is_empty());
    assert!(reader
        .session_timeline_events("timeline-session", None, 0)
        .unwrap()
        .events.is_empty());
    assert!(reader
        .session_timeline_events("timeline-session", Some((u64::MAX, 0)), 1)
        .is_err());
    assert_eq!(
        reader
            .session_timeline_events("foreign-session", None, usize::MAX)
            .unwrap()
            .events.len(),
        100
    );
}

#[test]
fn cancellation_receipt_is_durable_idempotent_and_conflict_checked() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());
    let services = backends.builder(temp.path(), &workspace).build().unwrap();
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
    let services = backends.builder(temp.path(), &workspace).build().unwrap();
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
    let services = RuntimeServices::test_builder(temp.path(), &workspace)
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

#[test]
fn committed_cancellation_replays_live_finalization_after_crash() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());
    let services = backends.builder(temp.path(), &workspace).build().unwrap();
    services.record_live_execution(
        "cancel-recovery-session",
        "cancel-recovery-execution".to_string(),
        "cancel-recovery-turn".to_string(),
    );
    let requested = services
        .commit_cancellation_receipt(harness_contract::turn::CancellationReceipt {
            cancellation_id: "cancel-recovery".to_string(),
            session_id: "cancel-recovery-session".to_string(),
            turn_id: "cancel-recovery-turn".to_string(),
            execution_id: "cancel-recovery-execution".to_string(),
            actor_id: "principal:local-human".to_string(),
            cause: harness_contract::turn::CancellationCause::UserRequested,
            reason: Some("recover committed cancellation".to_string()),
            requested_at_ms: 100,
            effective_at_ms: None,
            status: harness_contract::turn::CancellationStatus::Requested,
            journal_sequence: 0,
            projection_revision: 0,
        })
        .unwrap();
    assert_eq!(
        services
            .claim_live_terminal_fence(
                &requested.execution_id,
                "cancellation:cancel-recovery".to_string(),
                harness_contract::projection::ExecutionLiveStatus::Cancelled,
                requested.requested_at_ms,
            )
            .unwrap(),
        crate::execution_live::TerminalFenceClaim::Claimed
    );
    let mut committed = requested;
    committed.status = harness_contract::turn::CancellationStatus::Cancelled;
    committed.effective_at_ms = Some(101);
    let committed = services.commit_cancellation_receipt(committed).unwrap();

    // Simulate process loss after the canonical receipt commits but before
    // its derived live projection is finalized.
    drop(services);
    let recovered = backends.builder(temp.path(), &workspace).build().unwrap();
    assert_eq!(
        recovered
            .resolve_requested_cancellation("cancel-recovery")
            .unwrap(),
        Some(committed)
    );
    let live = recovered
        .execution_live("cancel-recovery-execution")
        .expect("committed cancellation must rebuild its live projection");
    assert_eq!(
        live.status,
        harness_contract::projection::ExecutionLiveStatus::Cancelled
    );
    assert_eq!(
        live.terminal_ref.as_deref(),
        Some("cancellation:cancel-recovery")
    );
    drop(recovered);

    let second_restart = backends.builder(temp.path(), &workspace).build().unwrap();
    assert_eq!(
        second_restart
            .execution_live("cancel-recovery-execution")
            .expect("finalized cancellation checkpoint remains recoverable")
            .status,
        harness_contract::projection::ExecutionLiveStatus::Cancelled
    );
}

#[test]
fn prepared_session_terminal_defers_cancellation_until_terminal_winner_commits() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), &workspace)
        .build()
        .unwrap();
    let execution_id = "completion-wins-execution";
    services.record_live_execution(
        "completion-wins-session",
        execution_id.to_string(),
        "completion-wins-turn".to_string(),
    );
    assert_eq!(
        services
            .claim_live_terminal_fence(
                execution_id,
                "completion-wins-terminal".to_string(),
                harness_contract::projection::ExecutionLiveStatus::Complete,
                7,
            )
            .unwrap(),
        crate::execution_live::TerminalFenceClaim::Claimed
    );
    services
        .commit_cancellation_receipt(harness_contract::turn::CancellationReceipt {
            cancellation_id: "completion-wins-cancellation".to_string(),
            session_id: "completion-wins-session".to_string(),
            turn_id: "completion-wins-turn".to_string(),
            execution_id: execution_id.to_string(),
            actor_id: "principal:local-human".to_string(),
            cause: harness_contract::turn::CancellationCause::UserRequested,
            reason: Some("raced terminal delivery".to_string()),
            requested_at_ms: 200,
            effective_at_ms: None,
            status: harness_contract::turn::CancellationStatus::Requested,
            journal_sequence: 0,
            projection_revision: 0,
        })
        .unwrap();
    assert!(services
        .resolve_requested_cancellation("completion-wins-cancellation")
        .unwrap()
        .is_none());
    assert!(!services
        .execution_live(execution_id)
        .unwrap()
        .status
        .is_terminal());

    assert_eq!(
        services
            .finalize_live_terminal_fence(
                execution_id,
                "completion-wins-terminal",
                harness_contract::projection::ExecutionLiveStatus::Complete,
                7,
            )
            .unwrap(),
        crate::execution_live::TerminalFenceClaim::Claimed
    );
    let cancellation = services
        .resolve_requested_cancellation("completion-wins-cancellation")
        .unwrap()
        .unwrap();
    assert_eq!(
        cancellation.status,
        harness_contract::turn::CancellationStatus::AlreadyTerminal
    );
    let live = services.execution_live(execution_id).unwrap();
    assert_eq!(
        live.status,
        harness_contract::projection::ExecutionLiveStatus::Complete
    );
    assert_eq!(
        live.terminal_ref.as_deref(),
        Some("completion-wins-terminal")
    );
}

async fn mixed_lease_start_442(
    services: &RuntimeServices,
    completed_effect: bool,
    successor_status: ExecutionNodeStatus,
    calls: Arc<AtomicUsize>,
) -> (
    ExecutionGraph,
    crate::execution_core::graph::ExecutionNodeBinding,
) {
    use crate::execution_core::graph::{
        ExecutionNodeBinding, NodeExecutor, ScopeLockMode, ScopeLockRequest, ScopedResource,
    };
    services
        .model_step_executor()
        .install_resolver(Arc::new(ServiceScopedResolver {
            payload_ref: "mixed-startup-442".into(),
            backend: Arc::new(ServiceScopedBackend { calls }),
        }));
    let mut graph = ExecutionGraph::new("mixed terminal lease startup reconciliation");
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    for node_id in ["mixed-old-442", "mixed-next-442"] {
        let mut spec = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "mixed-startup-442",
        );
        spec.id = node_id.into();
        spec.idempotency_key = format!("{node_id}:1");
        graph.nodes.push(spec);
    }
    graph
        .edges
        .push(harness_contract::execution_graph::ExecutionEdge {
            from: "mixed-old-442".into(),
            to: "mixed-next-442".into(),
            kind: harness_contract::execution_graph::ExecutionEdgeKind::DependsOn,
        });
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let graph = services
        .commit_service()
        .transition_node(
            &graph,
            "mixed-old-442",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let resource = services
        .resource_manager()
        .acquire(
            crate::execution_core::graph::ExecutionResourceKind::Provider,
            None,
        )
        .await
        .unwrap();
    let scope = services
        .scope_locks()
        .try_acquire([ScopeLockRequest {
            scope: ScopedResource::Resource {
                namespace: "442".into(),
                key: "mixed-old".into(),
            },
            mode: ScopeLockMode::Read,
        }])
        .unwrap()
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: "inline_model".into(),
        ticket_idempotency_key: "mixed-old-442:1".into(),
        attempt: 1,
        resource_lease_refs: vec![resource.projection_reference()],
        scope_lease_ref: Some(scope.projection_reference()),
        worktree_lease_ref: None,
    };
    let key = binding.lease_closure_key(&graph, "mixed-old-442");
    let admitted = crate::execution_core::graph::NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: "mixed-old-442".into(),
        executor_kind: "inline_model".into(),
        service_class: graph.service_class,
        attempt: 1,
        idempotency_key: binding.ticket_idempotency_key.clone(),
        payload_ref: "mixed-startup-442".into(),
    };
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == admitted.node_id)
        .unwrap();
    let fence = services
        .scope_locks()
        .acquire_binding_owner(key.rsplit(':').next().unwrap(), || {
            services
                .commit_service()
                .validate_unadmitted_binding_start(&graph, node, &admitted)
                .map_err(|error| error.to_string())
        })
        .unwrap();
    services.model_step_executor().prepare_original_binding(&admitted).unwrap();
    let graph = services
        .commit_service()
        .bind_and_start_nodes(
            &graph,
            std::collections::BTreeMap::from([("mixed-old-442".into(), binding.clone())]),
            vec![admitted.clone()],
        )
        .unwrap()
        .graph;
    let (ticket, _) = services
        .graph_state_store()
        .binding_start_ticket(&graph, "mixed-old-442", &binding)
        .unwrap();
    assert_eq!(ticket, admitted);
    let graph = if completed_effect {
        let outcome = services
            .model_step_executor()
            .poll_or_await(&ticket)
            .await
            .unwrap();
        services
            .commit_service()
            .transition_execution_outcome(
                &graph,
                &ticket,
                &outcome,
                &std::collections::BTreeMap::new(),
            )
            .unwrap()
            .graph
    } else {
        let result = harness_contract::execution_graph::ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: None,
            wait_class: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        };
        services
            .commit_service()
            .transition_node(
                &graph,
                "mixed-old-442",
                ExecutionNodeStatus::Completed,
                Some(result),
                Vec::new(),
            )
            .unwrap()
            .graph
    };
    let mut graph = services
        .commit_service()
        .transition_node(
            &graph,
            "mixed-next-442",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    if successor_status == ExecutionNodeStatus::Paused {
        graph = services
            .commit_service()
            .transition_node(
                &graph,
                "mixed-next-442",
                ExecutionNodeStatus::Paused,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
    }
    scope.try_release().unwrap();
    drop(resource);
    drop(fence);
    (graph, binding)
}

#[tokio::test]
async fn r1_442_mixed_startup_reconciles_old_lease_before_ready_or_paused_successor() {
    for successor_status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Paused] {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let home = temp.path().join("home");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        let backends = TestRuntimeBackends::new(temp.path());
        let calls = Arc::new(AtomicUsize::new(0));
        let original = backends.builder(&home, &workspace).build().unwrap();
        let (graph, binding) =
            mixed_lease_start_442(&original, true, successor_status, Arc::clone(&calls)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let key = binding.lease_closure_key(&graph, "mixed-old-442");
        assert!(backends
            .events
            .event_by_idempotency_key(&graph.id, &key)
            .unwrap()
            .is_none());
        drop(original);
        let restarted = backends.builder(&home, &workspace).build().unwrap();
        restarted
            .model_step_executor()
            .install_resolver(Arc::new(ServiceScopedResolver {
                payload_ref: "mixed-startup-442".into(),
                backend: Arc::new(ServiceScopedBackend {
                    calls: Arc::clone(&calls),
                }),
            }));
        let report = restarted
            .recover_execution_graphs_on_startup()
            .await
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let closure = backends
            .events
            .event_by_idempotency_key(&graph.id, &key)
            .unwrap()
            .unwrap();
        let recovered = restarted.graph_state_store().load(&graph.id).unwrap();
        let position = restarted
            .graph_state_store()
            .binding_start_position(&recovered, "mixed-old-442", &binding)
            .unwrap()
            .unwrap();
        assert!(restarted
            .graph_state_store()
            .binding_lease_closure(&recovered, "mixed-old-442", &binding, position,)
            .unwrap());
        if successor_status == ExecutionNodeStatus::Ready {
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                loop {
                    let current = restarted.graph_state_store().load(&graph.id).unwrap();
                    if current.node_statuses["mixed-next-442"] == ExecutionNodeStatus::Completed {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let current = restarted.graph_state_store().load(&graph.id).unwrap();
            let binding = restarted
                .graph_state_store()
                .current_binding(&current, "mixed-next-442", 1)
                .unwrap();
            let (ticket, _) = restarted
                .graph_state_store()
                .binding_start_ticket(&current, "mixed-next-442", &binding)
                .unwrap();
            let intent = backends
                .events
                .event_by_idempotency_key(
                    &crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(&ticket),
                    &format!("{}:intent", ticket.idempotency_key),
                )
                .unwrap()
                .unwrap();
            assert!(intent.commit_cursor > closure.commit_cursor);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        } else {
            assert_eq!(
                recovered.node_statuses["mixed-next-442"],
                ExecutionNodeStatus::Paused
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let repeated = restarted
                .recover_execution_graphs_on_startup()
                .await
                .unwrap();
            assert!(repeated.errors.is_empty(), "{:?}", repeated.errors);
            assert_eq!(
                restarted
                    .graph_state_store()
                    .load(&graph.id)
                    .unwrap()
                    .revision,
                recovered.revision
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn r1_442_mixed_startup_unknown_old_operation_blocks_notify_and_preserves_graph() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());
    let calls = Arc::new(AtomicUsize::new(0));
    let original = backends.builder(&home, &workspace).build().unwrap();
    let (graph, binding) = mixed_lease_start_442(
        &original,
        false,
        ExecutionNodeStatus::Ready,
        Arc::clone(&calls),
    )
    .await;
    let key = binding.lease_closure_key(&graph, "mixed-old-442");
    drop(original);
    let restarted = backends.builder(&home, &workspace).build().unwrap();
    restarted
        .model_step_executor()
        .install_resolver(Arc::new(ServiceScopedResolver {
            payload_ref: "mixed-startup-442".into(),
            backend: Arc::new(ServiceScopedBackend {
                calls: Arc::clone(&calls),
            }),
        }));
    for _ in 0..2 {
        let report = restarted
            .recover_execution_graphs_on_startup()
            .await
            .unwrap();
        assert!(report.errors.iter().any(|error| error.graph_id == graph.id));
        assert_eq!(report.notified_graphs, 0);
        assert_eq!(
            restarted
                .graph_state_store()
                .load(&graph.id)
                .unwrap()
                .revision,
            graph.revision
        );
        assert!(backends
            .events
            .event_by_idempotency_key(&graph.id, &key)
            .unwrap()
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

async fn terminal_lease_start_423(
    services: &RuntimeServices,
) -> (
    ExecutionGraph,
    crate::execution_core::graph::ExecutionNodeBinding,
    crate::execution_core::graph::resources::BindingOwnerFence,
) {
    use crate::execution_core::graph::{
        ExecutionNodeBinding, ScopeLockMode, ScopeLockRequest, ScopedResource,
    };
    let mut graph = ExecutionGraph::new("423 already terminal startup lease reconciliation");
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    let mut spec =
        ExecutionNodeSpec::new(ExecutionNodeKind::Materialize, "materialize", "payload:423");
    spec.id = "startup-lease-423".into();
    spec.idempotency_key = "startup-lease-423:1".into();
    graph.nodes.push(spec);
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let graph = services
        .commit_service()
        .transition_node(
            &graph,
            "startup-lease-423",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let resource = services
        .resource_manager()
        .acquire(
            crate::execution_core::graph::ExecutionResourceKind::Tool,
            None,
        )
        .await
        .unwrap();
    let scope = services
        .scope_locks()
        .try_acquire([ScopeLockRequest {
            scope: ScopedResource::Resource {
                namespace: "423".into(),
                key: "startup-reader".into(),
            },
            mode: ScopeLockMode::Read,
        }])
        .unwrap()
        .unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: "materialize".into(),
        ticket_idempotency_key: "startup-lease-423:1".into(),
        attempt: 1,
        resource_lease_refs: vec![resource.projection_reference()],
        scope_lease_ref: Some(scope.projection_reference()),
        worktree_lease_ref: None,
    };
    let key = binding.lease_closure_key(&graph, "startup-lease-423");
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == "startup-lease-423")
        .unwrap();
    let admitted = crate::execution_core::graph::NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node.id.clone(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: binding.attempt,
        idempotency_key: binding.ticket_idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    let fence = services
        .scope_locks()
        .acquire_binding_owner(key.rsplit(':').next().unwrap(), || {
            services
                .commit_service()
                .validate_unadmitted_binding_start(&graph, node, &admitted)
                .map_err(|error| error.to_string())
        })
        .unwrap();
    let graph = services
        .commit_service()
        .bind_and_start_node(&graph, "startup-lease-423", binding.clone())
        .unwrap()
        .graph;
    let graph = services
        .commit_service()
        .transition_node(
            &graph,
            "startup-lease-423",
            ExecutionNodeStatus::Completed,
            Some(harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }),
            Vec::new(),
        )
        .unwrap()
        .graph;
    scope.try_release().unwrap();
    drop(resource);
    (graph, binding, fence)
}

#[tokio::test]
async fn r1_423b_startup_blocks_unknown_operation_without_releasing_or_rewriting_terminal() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());
    let original = backends.builder(&home, &workspace).build().unwrap();
    let (graph, binding, fence) = terminal_lease_start_423(&original).await;
    let before = graph.revision;
    let key = binding.lease_closure_key(&graph, "startup-lease-423");
    drop(fence);
    drop(original);
    let restarted = backends.builder(&home, &workspace).build().unwrap();
    assert_eq!(restarted.scope_locks().active_lease_count().unwrap(), 0);
    let report = restarted
        .recover_execution_graphs_on_startup()
        .await
        .unwrap();
    assert!(report.errors.iter().any(|error| error.graph_id == graph.id));
    assert!(report
        .records
        .iter()
        .any(|record| record.graph_id == graph.id
            && record.action == "terminal_lease_reconciliation"));
    let after = restarted.graph_state_store().load(&graph.id).unwrap();
    assert_eq!(after.revision, before);
    assert_eq!(after.node_statuses, graph.node_statuses);
    assert_eq!(after.node_results, graph.node_results);
    assert!(backends
        .events
        .event_by_idempotency_key(&graph.id, &key)
        .unwrap()
        .is_none());
    restarted
        .recover_execution_graphs_on_startup()
        .await
        .unwrap();
    assert_eq!(
        restarted
            .graph_state_store()
            .load(&graph.id)
            .unwrap()
            .revision,
        after.revision
    );
}

#[tokio::test]
async fn r1_423_startup_preserves_excluded_session_and_live_owner() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    let backends = TestRuntimeBackends::new(temp.path());
    let original = backends.builder(&home, &workspace).build().unwrap();
    let (graph, binding, fence) = terminal_lease_start_423(&original).await;
    let restarted = backends.builder(&home, &workspace).build().unwrap();
    let session = graph.lineage.as_ref().unwrap().session_id.clone();
    let excluded = std::collections::BTreeSet::from([session]);
    restarted
        .recover_execution_graphs_on_startup_excluding_sessions(&excluded)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .graph_state_store()
            .load(&graph.id)
            .unwrap()
            .revision,
        graph.revision
    );
    let report = restarted
        .recover_execution_graphs_on_startup()
        .await
        .unwrap();
    assert!(report.errors.iter().any(|error| error.graph_id == graph.id));
    let key = binding.lease_closure_key(&graph, "startup-lease-423");
    assert!(backends
        .events
        .event_by_idempotency_key(&graph.id, &key)
        .unwrap()
        .is_none());
    assert_eq!(
        restarted
            .graph_state_store()
            .load(&graph.id)
            .unwrap()
            .revision,
        graph.revision
    );
    drop(fence);
}

#[tokio::test]
async fn u5_timeline_external_body_is_bound_unobserved_and_pages_without_loss() {
    let services = RuntimeServices::in_memory().unwrap();
    let session = "u5-large-external-body";
    let mut graph = ExecutionGraph::new("u5 oversized graph");
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    graph.lineage.as_mut().unwrap().session_id = session.into();
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "x".repeat(1_100_000),
    );
    node.id = "u5-node".into();
    node.idempotency_key = node.id.clone();
    graph.nodes.push(node);
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let original = services
        .event_store()
        .latest_for_stream(&graph.id)
        .unwrap()
        .unwrap();
    assert!(original.payload["bytes"].as_u64().unwrap() > 1_048_576);
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: session.into(),
            scope: RuntimeEventScope::SessionInput,
            kind: "u5.normal".into(),
            status: None,
            actor: None,
            refs: vec![],
            payload: serde_json::json!({"value": "ordinary"}),
        })
        .unwrap();

    let reader = services.event_reader();
    let first = reader.session_timeline_events(session, None, 1).unwrap();
    assert_eq!(first.events.len(), 1);
    assert!(first.has_more);
    let event = &first.events[0];
    let descriptor = &event.payload["_timeline_body"];
    assert_eq!(descriptor["status"], "unobserved");
    assert_eq!(descriptor["session_id"], session);
    assert_eq!(descriptor["event_id"], original.event_id);
    assert_eq!(descriptor["stream_id"], original.stream_id);
    assert_eq!(descriptor["commit_cursor"], original.commit_cursor);
    assert_eq!(descriptor["transaction_index"], original.transaction_index);
    assert_eq!(descriptor["bytes"], original.payload["bytes"]);
    assert_eq!(descriptor["sha256"], original.payload["sha256"]);
    assert_eq!(descriptor["digest_status"], "declared_unverified");
    assert_eq!(descriptor["origin_status"], "unverified");
    assert_eq!(descriptor["body_read_continuation"], "authenticated_page_if_supported");
    assert_eq!(descriptor["content_envelope"], original.payload);
    assert!(serde_json::to_vec(event).unwrap().len() < 1_048_576);
    let total = original.payload["bytes"].as_u64().unwrap();
    let mut restored = Vec::new();
    let mut offset = 0;
    while offset < total {
        let page = reader.session_timeline_body_page(session, &original.event_id, original.commit_cursor, original.transaction_index, offset, 65_536).unwrap();
        restored.extend(page["bytes"].as_array().unwrap().iter().map(|value| value.as_u64().unwrap() as u8));
        offset = page["next_offset"].as_u64().unwrap();
    }
    let value: serde_json::Value = serde_json::from_slice(&restored).unwrap();
    assert_eq!(value, services.event_store().resolve_graph_record(&original).unwrap());
    assert!(reader.session_timeline_body_page("u5-foreign", &original.event_id, original.commit_cursor, original.transaction_index, 0, 1024).is_err());
    assert!(reader.session_timeline_body_page(session, "wrong-event", original.commit_cursor, original.transaction_index, 0, 1024).is_err());
    let second = reader
        .session_timeline_events(
            session,
            Some((event.commit_cursor, event.transaction_index)),
            1,
        )
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].kind, "u5.normal");
    assert_eq!(second.events[0].payload["value"], "ordinary");
    assert!(!second.has_more);
    assert!(reader
        .session_timeline_events("u5-foreign", None, 10)
        .unwrap()
        .events
        .is_empty());
    assert!(RuntimeEventStore::timeline_unobserved_body(original.clone(), "u5-foreign").is_err());
    let mut tampered = original.clone();
    tampered.payload["body"]["artifact"]["sha256"] = serde_json::json!("sha256:bad");
    assert!(RuntimeEventStore::timeline_unobserved_body(tampered, session).is_err());
    let mut observation = original.clone();
    observation.scope = RuntimeEventScope::Goal;
    observation.kind = "goal.observation".into();
    let mut observation_envelope = original.payload.clone();
    observation_envelope
        .as_object_mut()
        .unwrap()
        .remove("_runtime_activity_binding");
    observation.payload = serde_json::json!({"observation_schema": 2, "session_id": session,
        "goal_source": {"stream_id": original.stream_id, "sequence": 8, "goal_revision": 3, "spec_digest": "digest"},
        "classifier": {"kind": "observed"}, "future_field": {"preserved": true}, "observation_body": observation_envelope});
    let observed =
        RuntimeEventStore::timeline_unobserved_body(observation.clone(), session).unwrap();
    assert_eq!(observed.payload["_timeline_body"]["status"], "unobserved");
    assert_eq!(
        observed.payload["_timeline_body"]["content_envelope"],
        observation.payload["observation_body"]
    );
    let mut expected_outer = observation.payload.clone();
    expected_outer.as_object_mut().unwrap().remove("observation_body");
    assert_eq!(observed.payload["_timeline_body"]["observation_outer"], expected_outer);
    observation.payload["goal_source"]["stream_id"] = serde_json::json!("wrong");
    assert!(RuntimeEventStore::timeline_unobserved_body(observation, session).is_err());

    // The ordinary authenticated content resolver still verifies the original
    // envelope, session, origin, full length and digest when actually read.
    let origin = crate::PreparedArtifactOrigin::EventBody {
        stream_id: original.stream_id.clone(),
        idempotency_key: original.idempotency_key.clone().unwrap(),
        kind: original.kind.clone(),
    };
    let scope = format!("session:{session}");
    assert!(services
        .event_store()
        .resolve_content(&scope, &original.payload, &origin)
        .is_ok());
    assert!(services
        .event_store()
        .resolve_content("session:u5-foreign", &original.payload, &origin)
        .is_err());
    let wrong_origin = crate::PreparedArtifactOrigin::EventBody {
        stream_id: "other-stream".into(),
        idempotency_key: original.idempotency_key.clone().unwrap(),
        kind: original.kind.clone(),
    };
    assert!(services
        .event_store()
        .resolve_content(&scope, &original.payload, &wrong_origin)
        .is_err());
    let mut missing = original.payload.clone();
    missing["body"]["artifact"]["selector"] =
        serde_json::json!(format!("artifact://art_{}", "0".repeat(64)));
    assert!(services
        .event_store()
        .resolve_content(&scope, &missing, &origin)
        .is_err());
    let mut incomplete = original.payload.clone();
    incomplete["bytes"] = serde_json::json!(original.payload["bytes"].as_u64().unwrap() - 1);
    assert!(services
        .event_store()
        .resolve_content(&scope, &incomplete, &origin)
        .is_err());
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
}

#[tokio::test]
async fn r453_timeline_expanded_bodies_continue_without_loss_or_foreign_records() {
    let services = RuntimeServices::in_memory().unwrap();
    let mut expected = Vec::new();
    let body = "R453_ORIGINAL_PRIVATE_ROW".repeat(8_000);
    for index in 0..2 {
        let mut graph = ExecutionGraph::new(format!("r453 original graph {index}"));
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        graph.lineage.as_mut().unwrap().session_id = "r453-timeline".into();
        for node in 0..3 {
            let mut spec = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                body.clone(),
            );
            spec.id = format!("r453-node-{index}-{node}");
            spec.idempotency_key = spec.id.clone();
            graph.nodes.push(spec);
        }
        let graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        let record = services
            .event_store()
            .latest_for_stream(&graph.id)
            .unwrap()
            .unwrap();
        assert_eq!(record.payload["body"]["kind"], "artifact_ref");
        let bytes = record.payload["bytes"].as_u64().unwrap();
        assert!(bytes > 524_288 && bytes < 786_432);
        expected.push(record.event_id);
    }
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: "r453-foreign".into(),
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "r453.foreign".into(),
            status: None,
            actor: None,
            refs: vec![],
            payload: serde_json::json!({"not_this_session":true}),
        })
        .unwrap();
    let reader = services.event_reader();
    let result = (|| -> Result<(), String> {
        let first = reader.session_timeline_events("r453-timeline", None, 100)?;
        assert_eq!(
            first.events.len(),
            1,
            "byte short page, not count short end"
        );
        assert!(first.has_more);
        assert_eq!(first.events[0].event_id, expected[0]);
        assert_eq!(
            first.events[0].payload["graph"]["nodes"][0]["payload_ref"],
            body
        );
        let last = first.events.last().unwrap();
        let second = reader.session_timeline_events(
            "r453-timeline",
            Some((last.commit_cursor, last.transaction_index)),
            100,
        )?;
        assert!(matches!(second.events.as_slice(), [event] if event.event_id == expected[1]));
        assert!(!second.has_more);
        assert_eq!(
            second.events[0].payload["graph"]["nodes"][0]["payload_ref"],
            body
        );
        for page in [&first, &second] {
            assert!(
                page.events
                    .iter()
                    .map(|e| serde_json::to_vec(e).unwrap().len())
                    .sum::<usize>()
                    <= 1_048_576
            );
        }
        let last = second.events.last().unwrap();
        let end = reader.session_timeline_events(
            "r453-timeline",
            Some((last.commit_cursor, last.transaction_index)),
            100,
        )?;
        assert!(end.events.is_empty() && !end.has_more);
        assert_eq!(
            first
                .events
                .into_iter()
                .chain(second.events)
                .map(|event| event.event_id)
                .collect::<Vec<_>>(),
            expected
        );
        assert!(
            reader
                .session_timeline_events("r453-other", None, 100)?
                .events
                .is_empty()
        );
        Ok(())
    })();
    services.shutdown_execution().await;
    services.shutdown_maintenance().await;
    result.expect(
        "each complete original body fits; cumulative size must yield a continuation, not 413",
    );
}

#[test]
fn r453_timeline_byte_short_transaction_resumes_by_exact_position_and_rejects_single_oversize() {
    let store = Arc::new(crate::RuntimeEventStore::for_test());
    let receipt =
        store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: "r453-tied-transaction".into(),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: "r453-tied".into(),
                    expected_revision: 0,
                }],
                events: (0..2)
                    .map(|index| {
                        crate::RuntimeEventInput {
            stream_id:"r453-tied".into(),scope:crate::RuntimeEventScope::SessionInput,
            kind:format!("r453.tied.{index}"),status:None,actor:None,refs:vec![],
            payload:serde_json::json!({"original":"z".repeat(600_000),"index":index}),
        }.into()
                    })
                    .collect(),
            })
            .unwrap();
    let reader = super::RuntimeEventReader {
        store: store.clone(),
    };
    let first = reader
        .session_timeline_events("r453-tied", None, 100)
        .unwrap();
    assert!(matches!(first.events.as_slice(), [event] if event.event_id == receipt.event_ids[0]));
    assert!(first.has_more);
    assert_eq!(
        first.complete_through_cursor,
        receipt.commit_cursor - 1,
        "integer replay cannot skip an unreturned event in the same commit"
    );
    let second = reader
        .session_timeline_events("r453-tied", Some((receipt.commit_cursor, 0)), 100)
        .unwrap();
    assert!(matches!(second.events.as_slice(), [event] if event.event_id == receipt.event_ids[1]));
    assert!(!second.has_more);
    assert_eq!(second.complete_through_cursor, receipt.commit_cursor);
    store
        .append(crate::RuntimeEventInput {
            stream_id: "r453-oversize".into(),
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "r453.oversize".into(),
            status: None,
            actor: None,
            refs: vec![crate::RuntimeEventRef { kind: "evidence".into(), id: "q".repeat(1_048_576) }],
            payload: serde_json::json!({"small_body":true}),
        })
        .unwrap();
    let normal = store.append(crate::RuntimeEventInput {
        stream_id: "r453-oversize".into(), scope: crate::RuntimeEventScope::SessionInput,
        kind: "r453.after".into(), status: None, actor: None, refs: vec![],
        payload: serde_json::json!({"value": "normal"}),
    }).unwrap();
    let descriptor = reader.session_timeline_events("r453-oversize", None, 100).unwrap();
    assert!(descriptor.events.is_empty());
    let first = descriptor.descriptor.as_ref().expect("separate descriptor");
    let payload = first.projection_payload("r453-oversize").unwrap();
    assert_eq!(payload["descriptor_type"], "timeline_projection_only");
    assert_eq!(payload["body_observation"], "unobserved");
    assert_eq!(payload["body_continuation"], "authenticated_event_page_if_supported");
    assert_eq!(payload["history_completeness"], "unknown");
    assert_eq!(payload["digest_status"], "unknown");
    assert!(payload["sha256"].is_null());
    assert_eq!(payload["content_digest_status"], "unknown");
    assert_eq!(payload["original_kind"], "r453.oversize");
    assert!(first.encoded_bytes > 1_048_576);
    assert!(serde_json::to_vec(&payload).unwrap().len() < 4096);
    assert!(descriptor.has_more);
    let next = reader.session_timeline_events("r453-oversize", Some((first.commit_cursor, first.transaction_index)), 100).unwrap();
    assert_eq!(next.events.len(), 1);
    assert_eq!(next.events[0].event_id, normal.event_id);
    assert_eq!(next.events[0].payload["value"], "normal");
    assert!(!next.has_more);
    assert!(reader.session_timeline_events("foreign", None, 100).unwrap().events.is_empty());
    assert!(store.timeline_oversize_descriptor("foreign", None, first.commit_cursor, &first.event_id).unwrap().is_none());
    assert!(store.timeline_oversize_descriptor("r453-oversize", None, first.commit_cursor, "wrong-event").is_err());
}

#[test]
fn timeline_descriptor_declared_digest_and_missing_digest_are_explicit() {
    let make = |digest| crate::TimelineOversizeDescriptor {
        event_id: "event-a".into(), stream_id: "session-a".into(), sequence: 1,
        original_scope: crate::RuntimeEventScope::SessionInput,
        original_kind: "legacy.kind".into(), created_at_ms: 1, commit_cursor: 2,
        transaction_index: 3, encoded_bytes: 1_048_577, digest, content_bytes: Some(42),
    };
    let sha = format!("sha256:{}", "a".repeat(64));
    let observed = make(Some(sha.clone())).projection_payload("session-a").unwrap();
    assert_eq!(observed["declared_content_sha256"], sha);
    assert_eq!(observed["content_digest_status"], "declared");
    assert_eq!(observed["declared_content_bytes"], 42);
    assert_eq!(observed["body_observation"], "unobserved");
    for digest in [None, Some("invalid".into())] {
        let unknown = make(digest).projection_payload("session-a").unwrap();
        assert_eq!(unknown["content_digest_status"], "unknown");
        assert!(unknown["declared_content_sha256"].is_null());
    }
}

#[test]
fn timeline_oversize_descriptor_resumes_inside_same_commit() {
    let store = Arc::new(crate::RuntimeEventStore::for_test());
    let receipt = store.append_transaction(crate::AppendTransactionRequest {
        transaction_id: "descriptor-tied".into(),
        expected_streams: vec![crate::ExpectedStreamRevision { stream_id: "descriptor-session".into(), expected_revision: 0 }],
        events: [serde_json::json!({"big": "x".repeat(1_048_576)}), serde_json::json!({"small": true})]
            .into_iter().enumerate().map(|(index, payload)| crate::RuntimeEventInput {
                stream_id: "descriptor-session".into(), scope: crate::RuntimeEventScope::SessionInput,
                kind: format!("descriptor.{index}"), status: None, actor: None, refs: vec![], payload,
            }.into()).collect(),
    }).unwrap();
    let reader = super::RuntimeEventReader { store };
    let first = reader.session_timeline_events("descriptor-session", None, 100).unwrap();
    assert!(first.events.is_empty());
    assert_eq!(first.descriptor.as_ref().unwrap().event_id, receipt.event_ids[0]);
    assert_eq!(first.complete_through_cursor, receipt.commit_cursor - 1);
    assert!(first.has_more);
    let second = reader.session_timeline_events("descriptor-session", Some((receipt.commit_cursor, 0)), 100).unwrap();
    assert_eq!(second.events[0].event_id, receipt.event_ids[1]);
    assert_eq!(second.complete_through_cursor, receipt.commit_cursor);
    assert!(!second.has_more);
}

#[test]
fn timeline_same_event_bounded_range_preserves_public_projection_and_binding() {
    use sha2::{Digest, Sha256};
    let store = Arc::new(crate::RuntimeEventStore::for_test());
    let evidence = "中".repeat(400_000);
    let receipt = store.append_transaction(crate::AppendTransactionRequest {
        transaction_id: "private-transaction-marker".into(),
        expected_streams: vec![crate::ExpectedStreamRevision { stream_id: "range-stream".into(), expected_revision: 0 }],
        events: vec![crate::RuntimeTransactionEventInput {
            event: crate::RuntimeEventInput {
                stream_id: "range-stream".into(), scope: crate::RuntimeEventScope::SessionInput,
                kind: "range.original".into(), status: Some("observed-status".into()),
                actor: Some("private-actor-marker".into()),
                refs: vec![crate::RuntimeEventRef { kind: "session".into(), id: "range-owner".into() },
                    crate::RuntimeEventRef { kind: "evidence".into(), id: evidence.clone() }],
                payload: serde_json::json!({"body":"原文", "counter":7}),
            },
            idempotency_key: Some("private-idempotency-marker".into()), schema_version: 1,
        }],
    }).unwrap();
    let reader = super::RuntimeEventReader { store: store.clone() };
    fn request<'a>(session_id: &'a str, event_id: &'a str, commit_cursor: u64, transaction_index: u32, offset: u64, limit: usize) -> crate::TimelineEventRangeRequest<'a> {
        crate::TimelineEventRangeRequest { session_id, event_id, commit_cursor, transaction_index, offset, limit }
    }
    let id = &receipt.event_ids[0];
    let mut cursor = 0;
    let mut bytes = Vec::new();
    let mut identity = None;
    loop {
        let page = reader.session_timeline_event_range(&request("range-owner", id, receipt.commit_cursor, 0, cursor, 64 * 1024)).unwrap().unwrap();
        assert_eq!(page.session_id, "range-owner");
        assert_eq!(&page.event_id, id);
        assert_eq!((page.commit_cursor, page.transaction_index), (receipt.commit_cursor, 0));
        assert_eq!(page.offset, cursor);
        assert!(page.bytes.len() <= 64 * 1024 && !page.bytes.is_empty());
        assert_eq!(page.digest_basis, "server_computed_public_projection_bytes");
        assert_eq!(page.format, "timeline_public_projection_json_v1");
        let stable = (page.total_bytes, page.projection_sha256.clone());
        if let Some(expected) = &identity { assert_eq!(expected, &stable); } else { identity = Some(stable); }
        bytes.extend_from_slice(&page.bytes);
        match page.next_offset { Some(next) => { assert!(next > cursor); cursor = next; }, None => break }
    }
    let (total, digest) = identity.unwrap();
    assert!(total > 1_048_576);
    assert_eq!(total, bytes.len() as u64);
    assert_eq!(digest, format!("sha256:{:x}", Sha256::digest(&bytes)));
    let projection: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(projection["event_id"], *id);
    assert_eq!(projection["stream_id"], "range-stream");
    assert_eq!(projection["kind"], "range.original");
    assert_eq!(projection["refs"][1]["id"], evidence);
    assert_eq!(projection["payload"]["body"], "原文");
    assert_eq!(projection["commit_cursor"], receipt.commit_cursor);
    assert_eq!(projection["transaction_index"], 0);
    for key in ["actor", "idempotency_key", "transaction_id", "schema_version"] { assert!(projection.get(key).is_none()); }
    let wire = String::from_utf8(bytes).unwrap();
    for secret in ["private-actor-marker", "private-idempotency-marker", "private-transaction-marker"] { assert!(!wire.contains(secret)); }
    for invalid in [request("foreign", id, receipt.commit_cursor, 0, 0, 1),
        request("range-owner", "wrong-event", receipt.commit_cursor, 0, 0, 1),
        request("range-owner", id, receipt.commit_cursor, 1, 0, 1),
        request("range-owner", id, receipt.commit_cursor + 1, 0, 0, 1)] {
        assert!(reader.session_timeline_event_range(&invalid).unwrap().is_none());
    }
    for invalid in [request("range-owner", id, receipt.commit_cursor, 0, 0, 0),
        request("range-owner", id, receipt.commit_cursor, 0, 0, 64 * 1024 + 1),
        request("range-owner", id, receipt.commit_cursor, 0, total + 1, 1)] {
        assert!(reader.session_timeline_event_range(&invalid).is_err());
    }
    let final_page = reader.session_timeline_event_range(&request("range-owner", id, receipt.commit_cursor, 0, total, 1)).unwrap().unwrap();
    assert!(final_page.bytes.is_empty() && final_page.next_offset.is_none());
    assert_eq!(final_page.projection_sha256, digest);
}

/// A Session query port that answers with one durable input claim.
///
/// The owner recovery under test reuses the claim and the control the
/// original ingress installed, and asks the durable Session whether that
/// claim is still the live one. This port answers exactly that question and
/// nothing else.
struct A8SessionQueryPort {
    records: std::sync::Mutex<Vec<crate::RuntimeSessionInputRecord>>,
    admission: std::sync::Mutex<Option<crate::RuntimeSessionInputAdmission>>,
    returned_record: std::sync::Mutex<Option<crate::RuntimeSessionInputRecord>>,
}

impl A8SessionQueryPort {
    fn new(records: Vec<crate::RuntimeSessionInputRecord>) -> Arc<Self> {
        Arc::new(Self { records: std::sync::Mutex::new(records), admission: std::sync::Mutex::new(None), returned_record: std::sync::Mutex::new(None) })
    }

    fn set(&self, records: Vec<crate::RuntimeSessionInputRecord>) {
        *self.records.lock().unwrap() = records;
    }

    fn set_admission(&self, admission: Option<crate::RuntimeSessionInputAdmission>) {
        *self.admission.lock().unwrap() = admission;
    }
}

#[async_trait::async_trait]
impl crate::SessionRuntimeQueryPort for A8SessionQueryPort {
    async fn cancellation_authorization_by_id(
        &self,
        _request: &crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
    ) -> Result<Option<session::SessionDomainEvent>, session::SessionError> {
        Ok(None)
    }

    fn history_reader(&self) -> Option<Arc<session::SessionHistoryReader>> {
        None
    }

    async fn session_record(
        &self,
        _session_id: &str,
    ) -> Result<Option<crate::RuntimeSessionRecord>, session::SessionError> {
        Ok(None)
    }

    async fn runtime_input(
        &self,
        request_id: &str,
    ) -> Result<Option<crate::RuntimeSessionInputRecord>, session::SessionError> {
        if let Some(record) = self.returned_record.lock().unwrap().clone() {
            return Ok(Some(record));
        }
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.request_id == request_id)
            .cloned())
    }

    async fn runtime_input_by_input_id(
        &self,
        _input_id: &str,
    ) -> Result<Option<crate::RuntimeSessionInputRecord>, session::SessionError> {
        Ok(None)
    }

    async fn pending_session_input_applications(
        &self,
        _session_id: &str,
        _session_generation: u64,
        _after: Option<(usize, String)>,
        _limit: usize,
    ) -> Result<Vec<crate::RuntimeSessionInputRecord>, session::SessionError> {
        Ok(Vec::new())
    }

    async fn input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::RuntimeSessionInputAdmission>, session::SessionError> {
        Ok(self.admission.lock().unwrap().as_ref()
            .filter(|admission| admission.session_id == session_id).cloned())
    }

    async fn evidence_access(
        &self,
        _session_id: &str,
        _evidence_id: &str,
    ) -> Result<Option<harness_contract::context::EvidenceAccessRef>, session::SessionError> {
        Ok(None)
    }

    async fn session_evidence_receipt_ids(
        &self,
        _session_id: &str,
        _limit: usize,
    ) -> Result<Vec<String>, session::SessionError> {
        Ok(Vec::new())
    }
}

fn a8_original_ingress() -> crate::TurnIngressRef {
    crate::TurnIngressRef {
        request_id: "request-a8".into(),
        turn_id: "turn-a8".into(),
        message_id: "message-a8".into(),
        session_id: "session-a8".into(),
        primary_task_id: "task-a8".into(),
        root_task_id: "task-a8".into(),
        session_generation: 4,
        input_sequence: 9,
        claim_owner: "session-a8".into(),
        claim_token: "claim-token-a8".into(),
        claim_revision: 2,
        handoff_id: None,
    }
}

/// The durable input claim of the original root turn, still live.
fn a8_live_input_record(ingress: &crate::TurnIngressRef) -> crate::RuntimeSessionInputRecord {
    crate::RuntimeSessionInputRecord {
        input_id: "input-a8".into(),
        request_id: ingress.request_id.clone(),
        turn_id: ingress.turn_id.clone(),
        message_id: ingress.message_id.clone(),
        session_id: ingress.session_id.clone(),
        sequence: ingress.input_sequence as usize,
        session_generation: ingress.session_generation,
        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
        target_turn_id: Some(ingress.turn_id.clone()),
        classification_json: None,
        task_route_hint: None,
        status: crate::RuntimeSessionInputStatus::Running,
        runtime_commit_cursor: None,
        attempts: 1,
        next_attempt_at_ms: 0,
        claim_owner: Some(ingress.claim_owner.clone()),
        claim_token: Some(ingress.claim_token.clone()),
        claim_fence_epoch: Some(ingress.claim_revision),
        claim_expires_at_ms: Some(crate::tool_invocation::now_ms() + 600_000),
        failure_class: None,
        last_error: None,
        revision: ingress.claim_revision,
        created_at_ms: 0,
        updated_at_ms: 0,
        terminal_at_ms: None,
        runtime_options_json: None,
        application_receipt: None,
    }
}

/// The root graph ends and its bus lease is retired, but the original Session
/// turn still owes its members an owner. The owner is recovered from the same
/// claim and the same original turn control, never from a new one.
#[tokio::test]
async fn a8_original_session_owner_survives_the_root_bus_lease() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let ingress = a8_original_ingress();
    let execution_id = crate::session_ingress_graph_id(
        &ingress.session_id,
        &ingress.request_id,
        &ingress.turn_id,
    );
    let control = services
        .create_turn_cancellation_control(&execution_id, &ingress)
        .expect("original turn control");
    services.live_execution_store.record_queued(
        &ingress.session_id,
        execution_id.clone(),
        ingress.turn_id.clone(),
    );
    let lease = services
        .bind_active_execution_bus(execution_id.clone(), crate::CowdEventBus::new())
        .expect("root execution bus");
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let fence = crate::SessionExecutionFence::from_claim(
        Arc::clone(&port) as Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress.request_id.clone(),
        ingress.session_id.clone(),
        ingress.session_generation,
        ingress.input_sequence as usize,
        ingress.claim_owner.clone(),
        ingress.claim_token.clone(),
    )
    .expect("durable execution fence");
    let claim = lease
        .install_session_fence(
            &ingress,
            fence,
            Arc::clone(&control),
            crate::SessionInputStream::new(ingress.session_id.clone()),
            Arc::new(crate::conversation::OriginalTurnInputControl::default()),
            crate::permissions::PermissionPolicy::new(crate::permissions::PermissionMode::ReadOnly),
        )
        .expect("original Session fence");
    // The root graph ends: its active execution bus lease is retired.
    drop(lease);

    let recovered = services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .expect("the original Session owner is recovered from the original fence");
    // The recovered owner carries the very claim the ingress installed, so its
    // claim token is not lost and its commit_wins identity is unchanged.
    assert!(Arc::ptr_eq(&recovered.claim, &claim));
    assert_eq!(recovered.claim.session_id(), ingress.session_id);
    assert_eq!(recovered.claim.turn_id(), ingress.turn_id);

    // Another root execution never inherits that owner.
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            "session-ingress-graph:00000000000000000000000000000000",
            "agentic-graph:a8-member",
        )
        .await
        .is_none());
    // Another turn of that Session never inherits it either.
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            "turn-other",
            &execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .is_none());
    // A Session without an original fence has no owner to recover.
    assert!(services
        .recovered_original_conclusion_owner(
            "session-other",
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .is_none());

    // A finished original turn keeps no recoverable owner: the Session no
    // longer holds that turn's live claim.
    port.set(vec![crate::RuntimeSessionInputRecord {
        status: crate::RuntimeSessionInputStatus::Completed,
        claim_expires_at_ms: None,
        ..a8_live_input_record(&ingress)
    }]);
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .is_none());

    // The original turn's own cancellation still refuses the recovery.
    port.set(vec![a8_live_input_record(&ingress)]);
    control.signal_operational();
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .is_none());
}

/// Install one original Session fence on a fresh root execution bus.
fn a8_install_session_fence(
    services: &RuntimeServices,
    port: &Arc<A8SessionQueryPort>,
    ingress: &crate::TurnIngressRef,
) -> Arc<crate::execution_core::services::OriginalSessionClaim> {
    let execution_id = crate::session_ingress_graph_id(
        &ingress.session_id,
        &ingress.request_id,
        &ingress.turn_id,
    );
    let control = services
        .create_turn_cancellation_control(&execution_id, ingress)
        .expect("original turn control");
    services.live_execution_store.record_queued(
        &ingress.session_id,
        execution_id.clone(),
        ingress.turn_id.clone(),
    );
    let lease = services
        .bind_active_execution_bus(execution_id.clone(), crate::CowdEventBus::new())
        .expect("root execution bus");
    let fence = crate::SessionExecutionFence::from_claim(
        Arc::clone(port) as Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress.request_id.clone(),
        ingress.session_id.clone(),
        ingress.session_generation,
        ingress.input_sequence as usize,
        ingress.claim_owner.clone(),
        ingress.claim_token.clone(),
    )
    .expect("durable execution fence");
    let claim = lease
        .install_session_fence(
            ingress,
            fence,
            Arc::clone(&control),
            crate::SessionInputStream::new(ingress.session_id.clone()),
            Arc::new(crate::conversation::OriginalTurnInputControl::default()),
            crate::permissions::PermissionPolicy::new(crate::permissions::PermissionMode::ReadOnly),
        )
        .expect("original Session fence");
    // The root graph ends: its active execution bus lease is retired.
    drop(lease);
    claim
}

/// The retained original Session owner is released by identity.
///
/// The next turn for the same Session replaces the retained entry. A release
/// that belongs to the previous turn must not remove the new turn's owner.
#[tokio::test]
async fn a8_retained_owner_release_is_identity_qualified() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let first = a8_original_ingress();
    let second = crate::TurnIngressRef {
        request_id: "request-b8".into(),
        turn_id: "turn-b8".into(),
        message_id: "message-b8".into(),
        session_generation: 5,
        input_sequence: 10,
        claim_token: "claim-token-b8".into(),
        claim_revision: 3,
        ..first.clone()
    };
    let port = A8SessionQueryPort::new(vec![
        a8_live_input_record(&first),
        a8_live_input_record(&second),
    ]);
    let first_claim = a8_install_session_fence(&services, &port, &first);
    let second_claim = a8_install_session_fence(&services, &port, &second);
    let second_execution_id = crate::session_ingress_graph_id(
        &second.session_id,
        &second.request_id,
        &second.turn_id,
    );

    // The previous turn releases its owner. The retained entry now belongs to
    // the new turn, so that owner must survive.
    services.release_original_session_owner(&first.session_id, &first_claim);
    assert!(services
        .recovered_original_conclusion_owner(
            &second.session_id,
            &second.turn_id,
            &second_execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .is_some());

    // The new turn releases its own owner, and that owner is gone.
    services.release_original_session_owner(&second.session_id, &second_claim);
    assert!(services
        .recovered_original_conclusion_owner(
            &second.session_id,
            &second.turn_id,
            &second_execution_id,
            "agentic-graph:a8-member",
        )
        .await
        .is_none());
}

/// Write one Program action into the Runtime's durable store with the
/// production Program event shape.
fn a8_append_program_action(
    services: &RuntimeServices,
    program_id: &str,
    envelope: &harness_contract::agent_action::AgentActionEnvelope,
    entity_ref: Option<&str>,
) {
    let mut payload = serde_json::json!({ "envelope": envelope });
    if let Some(entity_ref) = entity_ref {
        payload["entity_ref"] = serde_json::json!(entity_ref);
    }
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: format!("agentic-program:{program_id}"),
            scope: crate::RuntimeEventScope::Program,
            kind: "agentic.action_applied".into(),
            status: Some("applied".into()),
            actor: Some(envelope.actor.actor_id.clone()),
            refs: vec![],
            payload,
        })
        .expect("durable Program action");
}

/// The Program owner identity of one Session turn.
fn a8_program_actor(
    program_id: &str,
    objective_id: &str,
    session_id: &str,
    turn_id: &str,
    root_execution_id: &str,
) -> harness_contract::agent_action::AgentActorBinding {
    harness_contract::agent_action::AgentActorBinding {
        original_execution: None,
        objective_id: objective_id.into(),
        program_id: program_id.into(),
        session_id: session_id.into(),
        turn_id: turn_id.into(),
        root_execution_id: Some(root_execution_id.into()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "a8 owner recovery fixture".into(),
        model_lease: "test".into(),
        permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
        resource_scopes: vec![],
        actor_id: format!("root:{session_id}"),
        kind: harness_contract::agent_action::AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    }
}

/// Write one Program with its member attempts into the Runtime's durable
/// store, and read it back through the production projection.
///
/// The Program writer inside the action service is broken at the baseline
/// (123 baseline failures share its "new root action requires original
/// physical execution" refusal), so this fixture writes the same durable
/// events that writer writes and lets the production reducer and the
/// production read path build the projection. Every member is registered by
/// the production dispatch event, and every settled member by the
/// production attempt-fail event.
///
/// Returns the Program id, which is the one the Session turn derives.
fn a8_program_with_members(
    services: &RuntimeServices,
    session_id: &str,
    turn_id: &str,
    root_execution_id: &str,
    members: &[(&str, bool)],
) -> String {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentAttemptMode, AgentInviteInput,
        TaskAttemptDispatchInput, TaskAttemptFailInput, TeamCreateInput,
    };
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let actor = a8_program_actor(&program_id, &objective_id, session_id, turn_id, root_execution_id);
    let envelope = |action_id: &str, action: AgentAction| AgentActionEnvelope {
        action_id: action_id.into(),
        actor: actor.clone(),
        expected_revision: None,
        action,
    };
    if !services.event_store().list_stream(&format!("agentic-program:{program_id}"))
        .unwrap().iter().any(|event| event.kind == "agentic.program_opened") {
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: format!("agentic-program:{program_id}"),
            scope: crate::RuntimeEventScope::Program,
            kind: "agentic.program_opened".into(),
            status: Some("open".into()),
            actor: Some(actor.actor_id.clone()),
            refs: vec![],
            payload: serde_json::json!({
                "program_id": program_id,
                "objective_id": objective_id,
                "session_id": session_id,
                "turn_id": turn_id,
                "root_execution_id": root_execution_id,
                "required_team_count": 0,
                "team_count_contract_version": 1,
                "objective_summary": "a8 owner recovery fixture",
                "model_lease": "test",
                "resource_scopes": [],
            }),
        })
        .expect("durable Program opening");
    }
    a8_append_program_action(
        services,
        &program_id,
        &envelope(
            "a8-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Delivery".into(),
                mission: "deliver independent evidence".into(),
                objective: None,
            }),
        ),
        Some("a8-team"),
    );
    a8_append_program_action(
        services,
        &program_id,
        &envelope(
            "a8-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: "a8-team".into(),
                role: "worker".into(),
                mission: "work the Task".into(),
                required_capabilities: vec!["read".into()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: vec![],
                execution_requirements: vec![],
            }),
        ),
        Some("a8-agent"),
    );
    a8_append_program_action(
        services,
        &program_id,
        &envelope(
            "a8-task",
            AgentAction::TaskPublish(
                serde_json::from_value(serde_json::json!({
                    "team_ref": "a8-team",
                    "title": "Evidence",
                    "objective": "collect cited evidence",
                    "acceptance": "independent cited evidence",
                }))
                .expect("task input"),
            ),
        ),
        Some("a8-task"),
    );
    for (index, (member, unsettled)) in members.iter().enumerate() {
        // The first live member holds the Task claim, so that the claim
        // branch of the per-member fact is exercised as well.
        let mode = if index == 0 {
            AgentAttemptMode::Execute
        } else {
            AgentAttemptMode::Review
        };
        let mut supervisor = actor.clone();
        supervisor.kind = harness_contract::agent_action::AgentActorKind::Supervisor;
        supervisor.actor_id = "runtime.program-supervisor".into();
        supervisor.execution_id = Some((*member).into());
        supervisor.agent_id = None;
        supervisor.team_id = None;
        let dispatch = AgentActionEnvelope {
            action_id: format!("runtime-attempt-dispatch:{program_id}:a8-task:{member}"),
            actor: supervisor,
            expected_revision: None,
            action: AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                task_ref: "a8-task".into(),
                execution_id: (*member).into(),
                agent_ref: "a8-agent".into(),
                membership_id: "membership:a8-agent:a8-team".into(),
                mode,
                generation: 0,
            }),
        };
        a8_append_program_action(services, &program_id, &dispatch, None);
        if !unsettled {
            let mut supervisor = actor.clone();
            supervisor.kind = harness_contract::agent_action::AgentActorKind::Supervisor;
            supervisor.actor_id = "runtime.program-supervisor".into();
            supervisor.execution_id = Some((*member).into());
            supervisor.agent_id = None;
            supervisor.team_id = None;
            let fail = AgentActionEnvelope {
                action_id: format!("runtime-attempt-fail:{program_id}:a8-task:{member}"),
                actor: supervisor,
                expected_revision: None,
                action: AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                    task_ref: "a8-task".into(),
                    execution_id: (*member).into(),
                    mode,
                    reason: "a8 member settled".into(),
                    retryable: false,
                }),
            };
            a8_append_program_action(services, &program_id, &fail, None);
        }
    }
    program_id
}

/// Settle every member attempt of the fixture Program.
fn a8_settle_member_attempts(
    services: &RuntimeServices,
    session_id: &str,
    turn_id: &str,
    root_execution_id: &str,
    members: &[&str],
) {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, TaskAttemptFailInput,
    };
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let actor = a8_program_actor(&program_id, &objective_id, session_id, turn_id, root_execution_id);
    for member in members {
        // Re-dispatch is valid here only when every retained attempt names the same identity.
        let mut original_dispatch = None;
        for event in services.event_store().list_stream(&format!("agentic-program:{program_id}"))
            .expect("member Program journal").iter().filter(|event| event.kind == "agentic.action_applied")
        {
            let envelope: AgentActionEnvelope = serde_json::from_value(event.payload["envelope"].clone())
                .expect("Program action envelope");
            if let AgentAction::TaskAttemptDispatch(input) = envelope.action {
                if input.execution_id != *member { continue; }
                assert_eq!(envelope.actor.execution_id.as_deref(), Some(*member));
                assert_eq!(envelope.actor.session_id, session_id);
                assert_eq!(envelope.actor.turn_id, turn_id);
                assert_eq!(envelope.actor.program_id, program_id);
                assert_eq!(envelope.actor.root_execution_id.as_deref(), Some(root_execution_id));
                let identity = (input.task_ref, input.mode, input.generation, input.agent_ref, input.membership_id);
                if let Some(previous) = &original_dispatch {
                    assert_eq!(previous, &identity, "ambiguous original member attempt");
                }
                original_dispatch = Some(identity);
            }
        }
        let (task_ref, mode, _, _, _) = original_dispatch.expect("member original dispatch");
        let mut supervisor = actor.clone();
        supervisor.kind = harness_contract::agent_action::AgentActorKind::Supervisor;
        supervisor.actor_id = "runtime.program-supervisor".into();
        supervisor.execution_id = Some((*member).into());
        supervisor.agent_id = None;
        supervisor.team_id = None;
        let fail = AgentActionEnvelope {
            action_id: format!("runtime-attempt-fail:{program_id}:a8-task:{member}"),
            actor: supervisor,
            expected_revision: None,
            action: AgentAction::TaskAttemptFail(TaskAttemptFailInput {
                task_ref,
                execution_id: (*member).into(),
                mode,
                reason: "a8 member settled".into(),
                retryable: false,
            }),
        };
        a8_append_program_action(services, &program_id, &fail, None);
    }
}

/// A store backend whose one Program journal read can be made to fail.
///
/// The recovery gate has to tell "this turn registered no member work" apart
/// from "this turn's journal could not be read", and only the second state
/// keeps a retained owner. The ephemeral backend never fails a read, so this
/// wrapper is the only way to reach that state in-process. Every other read
/// is forwarded untouched.
struct A8JournalFaultBackend {
    inner: crate::runtime_event_store::EphemeralRuntimeEventStore,
    failed_stream: std::sync::Mutex<Option<String>>,
}

impl std::fmt::Debug for A8JournalFaultBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("A8JournalFaultBackend").finish_non_exhaustive()
    }
}

impl A8JournalFaultBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: crate::runtime_event_store::EphemeralRuntimeEventStore::new(),
            failed_stream: std::sync::Mutex::new(None),
        })
    }

    /// Fail every read of one stream until the fault is cleared.
    fn fail_stream(&self, stream_id: &str) {
        *self.failed_stream.lock().unwrap() = Some(stream_id.to_string());
    }

    fn clear_fault(&self) {
        *self.failed_stream.lock().unwrap() = None;
    }
}

impl crate::RuntimeEventStoreBackend for A8JournalFaultBackend {
    fn list_stream(&self, stream_id: &str) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        if self.failed_stream.lock().unwrap().as_deref() == Some(stream_id) {
            return Err(format!("injected Program journal read failure for {stream_id}"));
        }
        self.inner.list_stream(stream_id)
    }

fn recent_event_lines(&self, stream_id: &str, references: &[crate::RuntimeEventRef], through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String> {
        self.inner.recent_event_lines(stream_id, references, through_cursor, max_events, max_bytes)
    }
    fn append(&self, input: crate::RuntimeEventInput) -> Result<crate::DurableRuntimeEvent, String> {
        self.inner.append(input)
    }
    fn append_transaction(&self, request: crate::AppendTransactionRequest) -> crate::RuntimeEventStoreResult<crate::AppendTransactionReceipt> {
        self.inner.append_transaction(request)
    }
    fn append_transaction_with_terminal(&self, request: crate::AppendTransactionRequest, terminal: crate::SessionTerminalInput) -> crate::RuntimeEventStoreResult<crate::AppendTransactionReceipt> {
        self.inner.append_transaction_with_terminal(request, terminal)
    }
    fn consume_verified_decision_lease(&self, lease_id: &str, principal_id: &str, review_id: &str, action: &str, scope: &str, evidence_digest: &str, credential_epoch: u64, consumed_at_ms: u64) -> crate::RuntimeEventStoreResult<()> {
        self.inner.consume_verified_decision_lease(lease_id, principal_id, review_id, action, scope, evidence_digest, credential_epoch, consumed_at_ms)
    }
    fn append_transaction_with_verified_decision_lease(&self, request: crate::AppendTransactionRequest, lease: &crate::VerifiedDecisionLease) -> crate::RuntimeEventStoreResult<crate::AppendTransactionReceipt> {
        self.inner.append_transaction_with_verified_decision_lease(request, lease)
    }
    fn append_batch_if_revision(&self, stream_id: String, expected_revision: u64, transaction_id: String, events: Vec<crate::RuntimeTransactionEventInput>) -> crate::RuntimeEventStoreResult<crate::AppendTransactionReceipt> {
        self.inner.append_batch_if_revision(stream_id, expected_revision, transaction_id, events)
    }
    fn events_after_cursor(&self, cursor: u64, max_commits: usize) -> crate::RuntimeEventStoreResult<Vec<crate::CommittedEventBatch>> {
        self.inner.events_after_cursor(cursor, max_commits)
    }
    fn projection_scan_page(&self, cursor: u64, interest: &crate::RuntimeProjectionInterest, max_commits: usize, max_events: usize, max_bytes: usize) -> crate::RuntimeEventStoreResult<crate::RuntimeProjectionScanPage> {
        self.inner.projection_scan_page(cursor, interest, max_commits, max_events, max_bytes)
    }
    fn projection_checkpoint(&self, projection_id: &str) -> crate::RuntimeEventStoreResult<Option<crate::RuntimeProjectionCheckpoint>> {
        self.inner.projection_checkpoint(projection_id)
    }
    fn projection_checkpoints_with_prefix(&self, prefix: &str) -> crate::RuntimeEventStoreResult<Vec<crate::RuntimeProjectionCheckpoint>> {
        self.inner.projection_checkpoints_with_prefix(prefix)
    }
    fn put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, payload: &serde_json::Value, updated_at_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeProjectionCheckpoint> {
        self.inner.put_projection_checkpoint(projection_id, source_cursor, payload, updated_at_ms)
    }
    fn compare_and_put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeProjectionCheckpoint> {
        self.inner.compare_and_put_projection_checkpoint(projection_id, source_cursor, expected_revision, payload, updated_at_ms)
    }
    fn compare_and_repair_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeProjectionCheckpoint> {
        self.inner.compare_and_repair_projection_checkpoint(projection_id, source_cursor, expected_revision, payload, updated_at_ms)
    }
    fn delete_projection_checkpoint(&self, projection_id: &str) -> crate::RuntimeEventStoreResult<bool> {
        self.inner.delete_projection_checkpoint(projection_id)
    }
    fn event_by_idempotency_key(&self, stream_id: &str, idempotency_key: &str) -> crate::RuntimeEventStoreResult<Option<crate::RuntimeEventRecord>> {
        self.inner.event_by_idempotency_key(stream_id, idempotency_key)
    }
    fn stream_revision(&self, stream_id: &str) -> crate::RuntimeEventStoreResult<u64> {
        self.inner.stream_revision(stream_id)
    }
    fn position_page(&self, scope: crate::RuntimeEventScope, identity: &str, kind: Option<&str>, after: Option<(u64, u32)>, through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.position_page(scope, identity, kind, after, through_cursor, max_events, max_bytes)
    }
    fn list_stream_after(&self, stream_id: &str, after_sequence: u64, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_stream_after(stream_id, after_sequence, through_sequence, max_events, max_bytes)
    }
    fn list_stream_before(&self, stream_id: &str, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_stream_before(stream_id, through_sequence, max_events, max_bytes)
    }
    fn count_stream_kinds(&self, stream_id: &str, kinds: &[&str], through_sequence: u64) -> Result<u64, String> {
        self.inner.count_stream_kinds(stream_id, kinds, through_sequence)
    }
    fn list_stream_page_desc(&self, stream_id: &str, limit: usize, offset: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_stream_page_desc(stream_id, limit, offset)
    }
    fn stream_event_count(&self, stream_id: &str) -> Result<usize, String> {
        self.inner.stream_event_count(stream_id)
    }
    fn execution_events_for_session(&self, session_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.execution_events_for_session(session_id, after_position, limit)
    }
    fn events_for_root_execution(&self, root_execution_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.events_for_root_execution(root_execution_id, after_position, limit)
    }
    fn events_for_root_execution_kind(&self, root_execution_id: &str, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.events_for_root_execution_kind(root_execution_id, kind, after_position, limit)
    }
    fn events_for_activity(&self, activity_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.events_for_activity(activity_id, after_position, limit)
    }
    fn list_scope(&self, scope: crate::RuntimeEventScope, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_scope(scope, limit)
    }
    fn list_scope_page_asc(&self, scope: crate::RuntimeEventScope, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_scope_page_asc(scope, after_position, limit)
    }
    fn list_scope_stream_prefix_page_asc(&self, scope: crate::RuntimeEventScope, stream_prefix: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_scope_stream_prefix_page_asc(scope, stream_prefix, after_position, limit)
    }
    fn list_scope_kind_page_asc(&self, scope: crate::RuntimeEventScope, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.list_scope_kind_page_asc(scope, kind, after_position, limit)
    }
    fn stream_ids_for_scope(&self, scope: crate::RuntimeEventScope) -> crate::RuntimeEventStoreResult<Vec<String>> {
        self.inner.stream_ids_for_scope(scope)
    }
    fn stream_ids_for_scope_kind_at_sequence(&self, scope: crate::RuntimeEventScope, kind: &str, sequence: u64) -> crate::RuntimeEventStoreResult<Vec<String>> {
        self.inner.stream_ids_for_scope_kind_at_sequence(scope, kind, sequence)
    }
    fn stream_ids_for_scope_kind_at_sequence_page(&self, scope: crate::RuntimeEventScope, kind: &str, sequence: u64, after: Option<(u64, String)>, limit: usize) -> crate::RuntimeEventStoreResult<Vec<(String, u64)>> {
        self.inner.stream_ids_for_scope_kind_at_sequence_page(scope, kind, sequence, after, limit)
    }
    fn latest_stream_statuses_for_scope_kind_at_sequence(&self, scope: crate::RuntimeEventScope, kind: &str, sequence: u64) -> crate::RuntimeEventStoreResult<Vec<(String, Option<String>)>> {
        self.inner.latest_stream_statuses_for_scope_kind_at_sequence(scope, kind, sequence)
    }
    fn all_events(&self, limit: usize) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.inner.all_events(limit)
    }
    fn latest_for_stream(&self, stream_id: &str) -> Result<Option<crate::DurableRuntimeEvent>, String> {
        self.inner.latest_for_stream(stream_id)
    }
    fn latest_for_stream_kind(&self, stream_id: &str, kind: &str) -> Result<Option<crate::DurableRuntimeEvent>, String> {
        self.inner.latest_for_stream_kind(stream_id, kind)
    }
    fn enqueue_session_terminal(&self, terminal_id: &str, message_id: &str, session_id: &str, commit_cursor: u64, payload_ref: &str) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxRecord> {
        self.inner.enqueue_session_terminal(terminal_id, message_id, session_id, commit_cursor, payload_ref)
    }
    fn claim_session_terminals(&self, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize) -> crate::RuntimeEventStoreResult<Vec<crate::RuntimeSessionOutboxRecord>> {
        self.inner.claim_session_terminals(worker_id, now_ms, lease_ms, limit)
    }
    fn session_terminal(&self, terminal_id: &str) -> crate::RuntimeEventStoreResult<Option<crate::RuntimeSessionOutboxRecord>> {
        self.inner.session_terminal(terminal_id)
    }
    fn has_unsettled_session_terminals(&self, session_id: &str) -> crate::RuntimeEventStoreResult<bool> {
        self.inner.has_unsettled_session_terminals(session_id)
    }
    fn session_terminal_for_execution(&self, session_id: &str, execution_id: &str, turn_id: &str) -> crate::RuntimeEventStoreResult<Option<crate::RuntimeSessionOutboxRecord>> {
        self.inner.session_terminal_for_execution(session_id, execution_id, turn_id)
    }
    fn materialized_session_terminals_after(&self, session_id: &str, after_commit_cursor: u64, limit: usize) -> crate::RuntimeEventStoreResult<Vec<crate::RuntimeSessionOutboxRecord>> {
        self.inner.materialized_session_terminals_after(session_id, after_commit_cursor, limit)
    }
    fn session_terminal_health(&self) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxHealth> {
        self.inner.session_terminal_health()
    }
    fn blocked_session_terminals(&self, limit: usize) -> crate::RuntimeEventStoreResult<Vec<crate::RuntimeSessionOutboxRecord>> {
        self.inner.blocked_session_terminals(limit)
    }
    fn retry_session_terminal(&self, terminal_id: &str, actor: &str, reason: &str, now_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxRecord> {
        self.inner.retry_session_terminal(terminal_id, actor, reason, now_ms)
    }
    fn adopt_session_terminal_fence(&self, request: &crate::RuntimeSessionTerminalFenceAdoption) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxRecord> {
        self.inner.adopt_session_terminal_fence(request)
    }
    fn ack_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxRecord> {
        self.inner.ack_session_terminal(terminal_id, worker_id, expected_revision, now_ms)
    }
    fn suppress_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, reason: &str, now_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxRecord> {
        self.inner.suppress_session_terminal(terminal_id, worker_id, expected_revision, reason, now_ms)
    }
    fn fail_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, class: crate::RuntimeSessionOutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64) -> crate::RuntimeEventStoreResult<crate::RuntimeSessionOutboxRecord> {
        self.inner.fail_session_terminal(terminal_id, worker_id, expected_revision, class, error, retry_at_ms, max_attempts, now_ms)
    }
}

/// Build Runtime services whose Program journal read can be failed on demand.
fn a8_wc_faulting_services(
    root: &std::path::Path,
    backend: &Arc<A8JournalFaultBackend>,
) -> Arc<RuntimeServices> {
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    RuntimeServices::builder(root, workspace)
        .non_durable_backends_for_testing(root)
        .runtime_event_store(Arc::new(crate::RuntimeEventStore::from_backend(Arc::clone(
            backend,
        )
            as Arc<dyn crate::RuntimeEventStoreBackend>)))
        .build()
        .expect("runtime services")
}

/// The Program journal stream of one Session turn.
fn a8_wc_program_stream(session_id: &str, turn_id: &str) -> String {
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    format!(
        "agentic-program:{}",
        harness_contract::agent_action::program_id_for_objective(&objective_id)
    )
}

/// The root graph ended and its bus lease is retired, and the root ingress
/// execution reached its terminal: the durable Session claim is gone.
fn a8_wc_terminal_root_claim(
    services: &RuntimeServices,
    port: &Arc<A8SessionQueryPort>,
    ingress: &crate::TurnIngressRef,
) -> (Arc<crate::execution_core::services::OriginalSessionClaim>, String) {
    let execution_id = crate::session_ingress_graph_id(
        &ingress.session_id,
        &ingress.request_id,
        &ingress.turn_id,
    );
    let claim = a8_install_session_fence(services, port, ingress);
    port.set(vec![crate::RuntimeSessionInputRecord {
        status: crate::RuntimeSessionInputStatus::Completed,
        claim_expires_at_ms: None,
        ..a8_live_input_record(ingress)
    }]);
    (claim, execution_id)
}



/// A member that is still running recovers the original Session owner after
/// the root graph ended and the root ingress claim is already terminal.
///
/// The recovery returns the very claim the ingress installed, so the claim
/// token and the commit_wins identity are not lost.
#[tokio::test]
async fn a8_wc_an_unsettled_member_recovers_the_original_owner_after_the_root_claim_ended() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let (claim, execution_id) = a8_wc_terminal_root_claim(&services, &port, &ingress);
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[("agentic-graph:live-member", true)],
    );

    let recovered = services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:live-member",
        )
        .await
        .expect("the unsettled member recovers the original Session owner");
    // The recovered owner is the one the ingress installed: same claim, same
    // claim token, same original turn control. Nothing is minted here.
    assert!(Arc::ptr_eq(&recovered.claim, &claim));
    assert_eq!(recovered.claim.session_id(), ingress.session_id);
    assert_eq!(recovered.claim.turn_id(), ingress.turn_id);
}

/// A member with no unsettled work of its own is still refused, in the very
/// shape where the root ingress claim is already terminal.
///
/// The refusal wording of an ownerless delegation is unchanged by the
/// recovery: it is pinned here so a later change cannot rewrite what an
/// ownerless delegation says. A member whose attempt already settled, and a
/// member that was never dispatched, own no live work and get no owner.
#[tokio::test]
async fn a8_wc_a_member_without_unsettled_work_is_still_refused() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let (_claim, execution_id) = a8_wc_terminal_root_claim(&services, &port, &ingress);
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[
            ("agentic-graph:settled-member", false),
            ("agentic-graph:live-member", true),
        ],
    );

    // The member whose attempt already settled owns no live work, so it gets
    // no owner.
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:settled-member",
        )
        .await
        .is_none());
    // A member that was never registered owns no live work either.
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:absent-member",
        )
        .await
        .is_none());
    // Another Session turn never inherits this owner, settled or not.
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            "turn-other",
            &execution_id,
            "agentic-graph:live-member",
        )
        .await
        .is_none());
    // A turn whose durable Program journal cannot be read has no fact to
    // recover from. This is a different turn of the same Session: its own
    // owner is retained, its claim is terminal, and there is simply no
    // attempt record behind it. An unreadable fact is not permission.
    let journal_free = crate::TurnIngressRef {
        request_id: "request-a8-nojournal".into(),
        turn_id: "turn-a8-nojournal".into(),
        message_id: "message-a8-nojournal".into(),
        session_generation: 4,
        input_sequence: 11,
        claim_token: "claim-token-a8-nojournal".into(),
        claim_revision: 2,
        ..ingress.clone()
    };
    let journal_free_execution = crate::session_ingress_graph_id(
        &journal_free.session_id,
        &journal_free.request_id,
        &journal_free.turn_id,
    );
    let journal_free_claim = a8_install_session_fence(&services, &port, &journal_free);
    port.set(vec![
        crate::RuntimeSessionInputRecord {
            status: crate::RuntimeSessionInputStatus::Completed,
            claim_expires_at_ms: None,
            ..a8_live_input_record(&ingress)
        },
        crate::RuntimeSessionInputRecord {
            status: crate::RuntimeSessionInputStatus::Completed,
            claim_expires_at_ms: None,
            ..a8_live_input_record(&journal_free)
        },
    ]);
    assert!(
        !services.session_has_unsettled_attempts(
            &journal_free.session_id,
            &journal_free.turn_id
        ),
        "this turn has no readable Program journal at all"
    );
    assert!(services
        .recovered_original_conclusion_owner(
            &journal_free.session_id,
            &journal_free.turn_id,
            &journal_free_execution,
            "agentic-graph:live-member",
        )
        .await
        .is_none());
    // The write side moves the same way, in the same refusal: with no
    // readable journal nobody in the turn is unsettled, so the release that
    // refusal ran already removed the retained owner. The explicit release
    // is therefore a no-op, and the window stays shut.
    assert!(!services.release_original_session_owner_after_terminal(
        &journal_free.session_id,
        &journal_free_claim
    ));
    assert!(services
        .recovered_original_conclusion_owner(
            &journal_free.session_id,
            &journal_free.turn_id,
            &journal_free_execution,
            "agentic-graph:live-member",
        )
        .await
        .is_none());

    // The ownerless delegation keeps its own refusal, word for word.
    let source = include_str!("../../agentic/action_invocation.rs");
    assert!(
        source.contains("\"unknown: original delegated Session owner unavailable\""),
        "the ownerless delegation refusal text must not change"
    );
}

/// A large batch gives every live member an owner, and leaks none.
///
/// No live member of a batch is left ownerless, and no member whose attempt
/// already settled receives one. The recovery window of the Session stays
/// open while any member is still unsettled, and the first member that asks
/// after the last settlement finds it closed.
#[tokio::test]
async fn a8_wc_a_large_batch_keeps_every_live_member_an_owner_and_leaks_none() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let (claim, execution_id) = a8_wc_terminal_root_claim(&services, &port, &ingress);
    let live: Vec<String> = (0..6).map(|index| format!("agentic-graph:live-{index}")).collect();
    let settled: Vec<String> = (0..2)
        .map(|index| format!("agentic-graph:settled-{index}"))
        .collect();
    let mut members: Vec<(String, bool)> = live
        .iter()
        .map(|member| (member.clone(), true))
        .chain(settled.iter().map(|member| (member.clone(), false)))
        .collect();
    members.sort_by(|left, right| right.0.cmp(&left.0));
    let members: Vec<(&str, bool)> = members
        .iter()
        .map(|(member, unsettled)| (member.as_str(), *unsettled))
        .collect();
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &members,
    );

    // Every live member of the batch recovers the original owner.
    for member in &live {
        assert!(
            services
                .recovered_original_conclusion_owner(
                    &ingress.session_id,
                    &ingress.turn_id,
                    &execution_id,
                    member,
                )
                .await
                .is_some(),
            "the live member {member} must keep an owner"
        );
    }
    // No settled member receives one.
    for member in &settled {
        assert!(
            services
                .recovered_original_conclusion_owner(
                    &ingress.session_id,
                    &ingress.turn_id,
                    &execution_id,
                    member,
                )
                .await
                .is_none(),
            "the settled member {member} must not receive an owner"
        );
    }

    // The recovery window of the Session does not close while a member is
    // still unsettled, so the write side cannot take the owner away from a
    // member the read side still serves.
    assert!(!services.release_original_session_owner_after_terminal(
        &ingress.session_id,
        &claim
    ));
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            &live[0],
        )
        .await
        .is_some());

    // With the last member settled, the window closes with the turn, and the
    // next member that asks about it finds nothing to recover.
    let live_refs: Vec<&str> = live.iter().map(String::as_str).collect();
    a8_settle_member_attempts(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &live_refs,
    );
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            &live[0],
        )
        .await
        .is_none());
    // That refusal already closed the window: nobody in the turn is
    // unsettled any more, so the release it ran removed the retained owner.
    // The explicit release is therefore a no-op, and the window stays shut.
    assert!(!services.release_original_session_owner_after_terminal(
        &ingress.session_id,
        &claim
    ));
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            &live[0],
        )
        .await
        .is_none());
}

/// Once the recovery window has closed, no later attempt registration can
/// reopen it.
///
/// The window boundary is the attempt settlement. A member dispatched after
/// the turn's members have all settled belongs to a turn that is over, and
/// the retained owner for it was already removed: the entry itself is the
/// boundary, so a fresh dispatch record grants nothing.
#[tokio::test]
async fn a8_wc_a_closed_window_admits_no_later_attempt_registration() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let (claim, execution_id) = a8_wc_terminal_root_claim(&services, &port, &ingress);
    // Every member of the turn settles before the turn's terminal close, so
    // the window closes with the turn itself.
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[("agentic-graph:settled-member", false)],
    );
    assert!(services.release_original_session_owner_after_terminal(
        &ingress.session_id,
        &claim
    ));

    // A later attempt registration for the same turn is still recorded
    // durably, and it changes nothing about the closed window.
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[("agentic-graph:late-member", true)],
    );
    assert!(
        services.session_has_unsettled_attempts(&ingress.session_id, &ingress.turn_id),
        "the fixture records the later attempt as unsettled"
    );
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:late-member",
        )
        .await
        .is_none());
    // The removal stays identity qualified: a release that belongs to the
    // previous turn cannot remove a later turn's retained owner.
    services.release_original_session_owner(&ingress.session_id, &claim);
}

/// A Program journal that cannot be read is no fact at all.
///
/// The read side refuses the member, and the write side keeps the retained
/// owner rather than removing an owner it cannot reason about. A turn with no
/// Program journal at all is the opposite case: that one removes.
#[tokio::test]
async fn a8_wc_an_unreadable_program_journal_keeps_the_owner_and_refuses_the_member() {
    let temp = tempfile::tempdir().unwrap();
    let backend = A8JournalFaultBackend::new();
    let services = a8_wc_faulting_services(temp.path(), &backend);
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let (claim, execution_id) = a8_wc_terminal_root_claim(&services, &port, &ingress);
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[("agentic-graph:live-member", true)],
    );
    let program_stream = a8_wc_program_stream(&ingress.session_id, &ingress.turn_id);

    // The journal is readable, so the live member has its owner.
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:live-member",
        )
        .await
        .is_some());

    // The journal read now fails. That is no fact: the member is refused.
    backend.fail_stream(&program_stream);
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:live-member",
        )
        .await
        .is_none());
    // The write side does not remove an owner it cannot reason about.
    assert!(!services.release_original_session_owner_after_terminal(
        &ingress.session_id,
        &claim
    ));
    // The retained owner is still there: once the journal reads again, the
    // same member is served again.
    backend.clear_fault();
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:live-member",
        )
        .await
        .is_some());
    // A turn with no Program journal at all is the other state: it removes.
    let empty = crate::TurnIngressRef {
        request_id: "request-a8-noprogram".into(),
        turn_id: "turn-a8-noprogram".into(),
        message_id: "message-a8-noprogram".into(),
        session_generation: 4,
        input_sequence: 12,
        claim_token: "claim-token-a8-noprogram".into(),
        claim_revision: 2,
        ..ingress.clone()
    };
    let empty_execution = crate::session_ingress_graph_id(
        &empty.session_id,
        &empty.request_id,
        &empty.turn_id,
    );
    let empty_claim = a8_install_session_fence(&services, &port, &empty);
    assert!(
        !services.session_has_unsettled_attempts(&empty.session_id, &empty.turn_id),
        "a turn with no Program journal has no member work"
    );
    assert!(services.release_original_session_owner_after_terminal(
        &empty.session_id,
        &empty_claim
    ));
    assert!(services
        .recovered_original_conclusion_owner(
            &empty.session_id,
            &empty.turn_id,
            &empty_execution,
            "agentic-graph:live-member",
        )
        .await
        .is_none());
}

/// The read side and the write side fold one member's attempt the same way.
///
/// A member that was dispatched, settled and dispatched again is unsettled:
/// the last record wins. Both predicates read that same fold, so neither can
/// call that member settled while the other calls it live.
#[tokio::test]
async fn a8_wc_both_sides_fold_one_members_attempt_the_same_way() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build()
        .expect("runtime services");
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let (claim, execution_id) = a8_wc_terminal_root_claim(&services, &port, &ingress);
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[("agentic-graph:retried-member", false)],
    );
    // The same physical execution is dispatched again after its settlement.
    // The member is running again, and both sides have to see that.
    a8_program_with_members(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &[("agentic-graph:retried-member", true)],
    );

    assert!(
        services.session_has_unsettled_attempts(&ingress.session_id, &ingress.turn_id),
        "the re-dispatched member is unsettled for the write side too"
    );
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:retried-member",
        )
        .await
        .is_some());
    // So the release keeps the owner: it cannot take away what the read side
    // still serves.
    assert!(!services.release_original_session_owner_after_terminal(
        &ingress.session_id,
        &claim
    ));
    // And once that member settles for good, both sides agree it is over.
    a8_settle_member_attempts(
        &services,
        &ingress.session_id,
        &ingress.turn_id,
        &execution_id,
        &["agentic-graph:retried-member"],
    );
    assert!(
        !services.session_has_unsettled_attempts(&ingress.session_id, &ingress.turn_id),
        "the settled member is over for both sides"
    );
    assert!(services
        .recovered_original_conclusion_owner(
            &ingress.session_id,
            &ingress.turn_id,
            &execution_id,
            "agentic-graph:retried-member",
        )
        .await
        .is_none());
}

#[test]
fn a8_attempt_fold_rejects_unreadable_and_foreign_facts() {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorKind, AgentAttemptMode,
        MemberAttemptFactError, MemberAttemptFold, TaskAttemptDispatchInput,
        TaskAttemptFailInput, TaskClaimInput, TaskReleaseInput,
        OriginalAgentExecution, MemberAttemptObservation,
    };
    let session = "a8-fold-session";
    let turn = "a8-fold-turn";
    let objective = harness_contract::agent_action::root_objective_id(session, turn);
    let program = harness_contract::agent_action::program_id_for_objective(&objective);
    let mut actor = a8_program_actor(&program, &objective, session, turn, "a8-root");
    actor.kind = AgentActorKind::Supervisor;
    actor.execution_id = Some("a8-member".into());
    let dispatch = AgentActionEnvelope {
        action_id: "a8-dispatch".into(), actor: actor.clone(), expected_revision: None,
        action: AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
            task_ref: "task".into(), execution_id: "a8-member".into(),
            agent_ref: "agent".into(), membership_id: "membership".into(),
            mode: AgentAttemptMode::Execute, generation: 1,
        }),
    };
    let settled = AgentActionEnvelope {
        action_id: "a8-settle".into(), actor: actor.clone(), expected_revision: None,
        action: AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref: "task".into(), execution_id: "a8-member".into(),
            mode: AgentAttemptMode::Execute, reason: "settled".into(), retryable: false,
        }),
    };
    let mut fold = MemberAttemptFold::default();
    let event = |envelope: &AgentActionEnvelope| serde_json::json!({"envelope": envelope});
    let mut root = actor.clone();
    root.kind = AgentActorKind::Root;
    root.execution_id = None;
    let root_action = AgentActionEnvelope {
        action_id: "a8-root-action".into(), actor: root, expected_revision: None,
        action: AgentAction::TaskClaim(TaskClaimInput { task_ref: "task".into(), reason: None }),
    };
    fold.observe_action_applied(&event(&root_action), session, turn, &program, None).unwrap();
    assert!(!fold.any_unsettled());
    let mut physical_root = root_action.actor.clone();
    physical_root.actor_id = format!("root:{session}");
    physical_root.original_execution = Some(OriginalAgentExecution {
        graph_id: "a8-root".into(), node_id: "node".into(), attempt: 1,
    });
    let root_release = AgentActionEnvelope {
        action_id: "a8-root-release".into(), actor: physical_root,
        expected_revision: None,
        action: AgentAction::TaskRelease(TaskReleaseInput {
            task_ref: "task".into(), reason: "release".into(), expected_claim_generation: None,
        }),
    };
    assert_eq!(fold.observe_action_applied(&event(&root_release), session, turn, &program, Some("a8-root")),
               Ok(MemberAttemptObservation::RootNonMember));
    assert_eq!(fold.observe_action_applied(&event(&root_release), session, turn, &program, Some("other")),
               Err(MemberAttemptFactError::UnprovenRoot));
    fold.observe_action_applied(&event(&dispatch), session, turn, &program, None).unwrap();
    assert!(fold.is_unsettled("a8-member"));
    fold.observe_action_applied(&event(&settled), session, turn, &program, None).unwrap();
    assert!(!fold.any_unsettled());
    fold.observe_action_applied(&event(&dispatch), session, turn, &program, None).unwrap();
    assert!(fold.is_unsettled("a8-member"), "the last dispatch reopens the attempt");
    assert_eq!(fold.observe_action_applied(&serde_json::json!({}), session, turn, &program, None),
               Err(MemberAttemptFactError::MalformedEnvelope));
    let mut bad = settled.clone();
    bad.actor.execution_id = None;
    assert_eq!(fold.observe_action_applied(&event(&bad), session, turn, &program, None),
               Err(MemberAttemptFactError::MissingExecution));
    bad.actor.execution_id = Some("other".into());
    assert_eq!(fold.observe_action_applied(&event(&bad), session, turn, &program, None),
               Err(MemberAttemptFactError::MismatchedExecution));
    bad.actor.session_id = "other-session".into();
    assert_eq!(fold.observe_action_applied(&event(&bad), session, turn, &program, None),
               Err(MemberAttemptFactError::ForeignLineage));
    let mut wrong_task = settled.clone();
    if let AgentAction::TaskAttemptFail(input) = &mut wrong_task.action {
        input.task_ref = "other-task".into();
    }
    assert_eq!(fold.observe_action_applied(&event(&wrong_task), session, turn, &program, None),
               Err(MemberAttemptFactError::AmbiguousAttempt));
    let mut wrong_generation = dispatch.clone();
    if let AgentAction::TaskAttemptDispatch(input) = &mut wrong_generation.action {
        input.generation += 1;
    }
    assert_eq!(fold.observe_action_applied(&event(&wrong_generation), session, turn, &program, None),
               Err(MemberAttemptFactError::AmbiguousAttempt));
    assert!(fold.is_unsettled("a8-member"), "invalid facts cannot close the attempt");
}

#[tokio::test]
async fn a8_attempt_fence_distinguishes_refusal_from_unavailable_record() {
    use crate::session_execution::{SessionExecutionFenceConjunction, SessionExecutionFenceError};
    let ingress = a8_original_ingress();
    let mut record = a8_live_input_record(&ingress);
    record.claim_token = Some("another-token".into());
    let port = A8SessionQueryPort::new(vec![record]);
    let fence = crate::SessionExecutionFence::from_claim(
        Arc::clone(&port) as Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress.request_id.clone(), ingress.session_id.clone(), ingress.session_generation,
        ingress.input_sequence as usize, ingress.claim_owner.clone(), ingress.claim_token.clone(),
    ).unwrap();
    let phase = crate::SessionExecutionFencePhase::ProviderRequest;
    let refusal = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(&refusal, SessionExecutionFenceError::Refused { phase: actual, verdict, .. }
        if *actual == phase && verdict.failed == Some(SessionExecutionFenceConjunction::ClaimToken)));
    assert!(refusal.to_string().contains("Session execution fence rejected ProviderRequest"));
    let fence = fence.with_claim_fence_epoch(ingress.claim_revision);
    let mut wrong_request = a8_live_input_record(&ingress);
    wrong_request.request_id = "another-request".into();
    assert_eq!(fence.classify(&wrong_request).failed,
               Some(SessionExecutionFenceConjunction::RequestIdentity));
    *port.returned_record.lock().unwrap() = Some(wrong_request);
    let refusal = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(refusal, SessionExecutionFenceError::Refused { verdict, .. }
        if verdict.failed == Some(SessionExecutionFenceConjunction::RequestIdentity)));
    *port.returned_record.lock().unwrap() = None;
    port.set(vec![a8_live_input_record(&ingress)]);
    port.set_admission(Some(crate::RuntimeSessionInputAdmission {
        session_id: ingress.session_id.clone(), generation: ingress.session_generation, open: true,
    }));
    assert_eq!(fence.verify_attempt(phase, "a8-member").await.unwrap().claim_fence_epoch,
               ingress.claim_revision);
    port.set_admission(Some(crate::RuntimeSessionInputAdmission {
        session_id: ingress.session_id.clone(), generation: ingress.session_generation, open: false,
    }));
    let closed = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(closed, SessionExecutionFenceError::Refused { verdict, .. }
        if verdict.failed == Some(SessionExecutionFenceConjunction::ClaimStatus)));
    port.set_admission(Some(crate::RuntimeSessionInputAdmission {
        session_id: ingress.session_id.clone(), generation: ingress.session_generation + 1, open: true,
    }));
    let advanced = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(advanced, SessionExecutionFenceError::Refused { verdict, .. }
        if verdict.failed == Some(SessionExecutionFenceConjunction::Generation)));
    let mut wrong_epoch = a8_live_input_record(&ingress);
    wrong_epoch.claim_fence_epoch = Some(ingress.claim_revision + 1);
    port.set(vec![wrong_epoch]);
    let refusal = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(&refusal, SessionExecutionFenceError::Refused { verdict, .. }
        if verdict.failed == Some(SessionExecutionFenceConjunction::ClaimFenceEpoch)));
    let mut released = a8_live_input_record(&ingress);
    released.status = crate::RuntimeSessionInputStatus::Completed;
    released.claim_owner = None;
    released.claim_token = None;
    released.claim_fence_epoch = None;
    released.claim_expires_at_ms = None;
    port.set(vec![released]);
    let refusal = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(&refusal, SessionExecutionFenceError::Refused { verdict, .. }
        if verdict.failed == Some(SessionExecutionFenceConjunction::ClaimOwner)));
    port.set(vec![]);
    let unavailable = fence.verify_attempt(phase, "a8-member").await.unwrap_err();
    assert!(matches!(unavailable, SessionExecutionFenceError::Unavailable { phase: actual, .. }
        if actual == phase));
}

#[tokio::test]
async fn a8_synthesis_waits_for_original_members_and_keeps_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace).build().unwrap();
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    port.set_admission(Some(crate::RuntimeSessionInputAdmission {
        session_id: ingress.session_id.clone(), generation: ingress.session_generation, open: true,
    }));
    let _claim = a8_install_session_fence(&services, &port, &ingress);
    let graph = crate::session_ingress_graph_id(&ingress.session_id, &ingress.request_id, &ingress.turn_id);
    a8_program_with_members(&services, &ingress.session_id, &ingress.turn_id, &graph,
        &[("agentic-graph:pending-member", true)]);
    let copy = Arc::clone(&services);
    let captured = ingress.clone();
    let captured_graph = graph.clone();
    let mut waiter = tokio::spawn(async move {
        copy.wait_for_original_members_before_synthesis(&captured_graph, &captured.session_id, &captured.turn_id).await
    });
    assert!(tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter).await.is_err());
    a8_settle_member_attempts(&services, &ingress.session_id, &ingress.turn_id, &graph,
        &["agentic-graph:pending-member"]);
    assert!(tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await.unwrap().unwrap().unwrap());
    let mut expired = a8_live_input_record(&ingress);
    expired.status = crate::RuntimeSessionInputStatus::Completed;
    port.set(vec![expired]);
    assert!(services.wait_for_original_members_before_synthesis(&graph, &ingress.session_id, &ingress.turn_id).await.is_err());
    port.set(vec![a8_live_input_record(&ingress)]);
    a8_program_with_members(&services, &ingress.session_id, &ingress.turn_id, &graph,
        &[("agentic-graph:pending-member", true)]);
    let copy = Arc::clone(&services);
    let captured = ingress.clone();
    let captured_graph = graph.clone();
    let mut waiter = tokio::spawn(async move {
        copy.wait_for_original_members_before_synthesis(&captured_graph, &captured.session_id, &captured.turn_id).await
    });
    assert!(tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiter).await.is_err());
    let control = services.original_session_owners.lock().unwrap()
        .get(&ingress.session_id).unwrap().root_control.clone();
    control.signal_operational();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await.unwrap().unwrap().unwrap_err();
    assert_eq!(error, "terminal_presentation_cancelled");
}

#[test]
fn a8_member_journal_requires_one_original_opening_after_diagnostics() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace).build().unwrap();
    let ingress = a8_original_ingress();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let _claim = a8_install_session_fence(&services, &port, &ingress);
    let graph = crate::session_ingress_graph_id(&ingress.session_id, &ingress.request_id, &ingress.turn_id);
    let stream = a8_wc_program_stream(&ingress.session_id, &ingress.turn_id);
    services.event_store().append(crate::RuntimeEventInput {
        stream_id: stream.clone(), scope: crate::RuntimeEventScope::Program,
        kind: "agentic.tool_contract".into(), status: None, actor: None, refs: vec![], payload: serde_json::json!({}),
    }).unwrap();
    assert!(services.session_has_unsettled_attempts(&ingress.session_id, &ingress.turn_id));
    assert!(!services.member_attempt_is_unsettled(&ingress.session_id, &ingress.turn_id, "agentic-graph:pending-member"));
    a8_program_with_members(&services, &ingress.session_id, &ingress.turn_id, &graph,
        &[("agentic-graph:pending-member", true)]);
    let events = services.event_store().list_stream(&stream).unwrap();
    assert!(services.member_attempt_fold(&events, &ingress.session_id, &ingress.turn_id).unwrap().any_unsettled());
    let mut foreign = events.clone();
    foreign.iter_mut().find(|event| event.kind == "agentic.program_opened").unwrap().payload["root_execution_id"] = serde_json::json!("foreign");
    assert!(services.member_attempt_fold(&foreign, &ingress.session_id, &ingress.turn_id).is_err());
    let opening = events.iter().find(|event| event.kind == "agentic.program_opened").unwrap();
    services.event_store().append(crate::RuntimeEventInput {
        stream_id: stream, scope: opening.scope, kind: opening.kind.clone(), status: opening.status.clone(),
        actor: opening.actor.clone(), refs: opening.refs.clone(), payload: opening.payload.clone(),
    }).unwrap();
    assert!(services.session_has_unsettled_attempts(&ingress.session_id, &ingress.turn_id));
    assert!(!services.member_attempt_is_unsettled(&ingress.session_id, &ingress.turn_id, "agentic-graph:pending-member"));
}

#[test]
fn a8_corrupt_attempt_fact_refuses_member_and_keeps_owner() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace)
        .build().expect("runtime services");
    let ingress = a8_original_ingress();
    services.event_store().append(crate::RuntimeEventInput {
        stream_id: a8_wc_program_stream(&ingress.session_id, &ingress.turn_id),
        scope: crate::RuntimeEventScope::Program,
        kind: "agentic.action_applied".into(), status: Some("applied".into()),
        actor: Some("runtime.program-supervisor".into()), refs: vec![],
        payload: serde_json::json!({"envelope": {"action": "unreadable"}}),
    }).expect("append corrupt applied action");
    assert!(!services.member_attempt_is_unsettled(
        &ingress.session_id, &ingress.turn_id, "agentic-graph:member"));
    assert!(services.session_has_unsettled_attempts(&ingress.session_id, &ingress.turn_id),
            "unreadable facts cannot justify removing the original owner");
}

#[test]
fn a8_original_owner_rejects_a_fence_from_another_claim() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::test_builder(temp.path(), workspace).build().unwrap();
    let ingress = a8_original_ingress();
    let execution = crate::session_ingress_graph_id(&ingress.session_id, &ingress.request_id, &ingress.turn_id);
    let control = services.create_turn_cancellation_control(&execution, &ingress).unwrap();
    services.live_execution_store.record_queued(&ingress.session_id, execution.clone(), ingress.turn_id.clone());
    let lease = services.bind_active_execution_bus(execution, crate::CowdEventBus::new()).unwrap();
    let port = A8SessionQueryPort::new(vec![a8_live_input_record(&ingress)]);
    let make_fence = |token: String| crate::SessionExecutionFence::from_claim(
        Arc::clone(&port) as Arc<dyn crate::SessionRuntimeQueryPort>,
        ingress.request_id.clone(), ingress.session_id.clone(), ingress.session_generation,
        ingress.input_sequence as usize, ingress.claim_owner.clone(), token,
    ).unwrap();
    let input = || crate::SessionInputStream::new(ingress.session_id.clone());
    let requested = || Arc::new(crate::conversation::OriginalTurnInputControl::default());
    let policy = || crate::permissions::PermissionPolicy::new(crate::permissions::PermissionMode::ReadOnly);
    assert!(lease.install_session_fence(&ingress, make_fence("foreign-token".into()), Arc::clone(&control),
        input(), requested(), policy()).is_err());
    assert!(lease.install_session_fence(&ingress, make_fence(ingress.claim_token.clone()), control,
        input(), requested(), policy()).is_ok(), "a refused foreign fence must not occupy the owner");
}
