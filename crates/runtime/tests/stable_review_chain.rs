#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use std::{
    fs,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use evolution_test_support::HumanAuthority;
use harness_contract::{
    agent::{
        AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
        AgentDefinitionManifest, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
        CognitiveReadScope, CognitiveWriteMode, DefinitionScope, ReleaseAssignment,
        ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
        RevisionSelector,
    },
    evaluation::{EvaluationContract, EvaluationMetricSpec},
    reality::EvidenceRef,
    security::{
        DecisionLeaseClaims, PrincipalAssurance, PrincipalClaims, PrincipalKind,
        SignedDecisionLease, SignedPrincipalEnvelope,
    },
};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use runtime::{
    agent::definition::{AgentDefinitionStore, RegisteredAgentDefinitionLayout},
    CanaryObservationReport, CanaryRolloutPolicy, DecisionLeaseExpectation, EvaluationDirection,
    EvolutionCandidateIntent, EvolutionCandidateSubject, EvolutionComparisonDimension,
    EvolutionComparisonReportV2, EvolutionEvalRunner, PrincipalVerifier, ReleaseChangeReview,
    ReleaseChangeReviewDecision, RuntimeServices, VerifiedDecisionLease, VerifiedPrincipal,
};
use sha2::{Digest, Sha256};

const INSTRUCTIONS: &str = "# Stable fixture\n\nReturn evidence-backed conclusions.\n";

struct EligibleRunner;

#[async_trait]
impl EvolutionEvalRunner for EligibleRunner {
    async fn evaluate(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<EvolutionComparisonReportV2, String> {
        Ok(EvolutionComparisonReportV2 {
            report_id: format!("report:{}", candidate.candidate_id),
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
                EvolutionComparisonDimension {
                    metric_id: "evidence".to_string(),
                    direction: EvaluationDirection::HigherIsBetter,
                    baseline: 1.0,
                    candidate: 1.0,
                    non_inferiority_margin: 0.0,
                    sample_count: 10,
                    minimum_samples: 10,
                    confidence: 0.99,
                    minimum_confidence: 0.9,
                    minimum_improvement: 0.01,
                    superiority_confidence: 0.99,
                    minimum_superiority_confidence: 0.9,
                    hard_gate: true,
                    protected: true,
                    target_improvement: false,
                },
                EvolutionComparisonDimension {
                    metric_id: "contract".to_string(),
                    direction: EvaluationDirection::HigherIsBetter,
                    baseline: 0.8,
                    candidate: 1.0,
                    non_inferiority_margin: 0.0,
                    sample_count: 10,
                    minimum_samples: 10,
                    confidence: 0.99,
                    minimum_confidence: 0.9,
                    minimum_improvement: 0.01,
                    superiority_confidence: 0.99,
                    minimum_superiority_confidence: 0.9,
                    hard_gate: true,
                    protected: true,
                    target_improvement: true,
                },
            ],
            source_run_refs: vec!["agent-run:paired".to_string()],
            evidence_refs: vec![EvidenceRef::observed("evaluation", "paired")],
            created_at_ms: 1,
        })
    }
}

fn contract() -> EvaluationContract {
    EvaluationContract {
        scenario_refs: vec!["evolution/stable".to_string()],
        metrics: vec![
            EvaluationMetricSpec::release_gate("evolution/stable", "evidence", true, false),
            EvaluationMetricSpec::release_gate("evolution/stable", "contract", true, true),
        ],
    }
}

fn manifest(id: AgentDefinitionId, revision: u64) -> AgentDefinitionManifest {
    AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: id,
        revision,
        name: format!("Stable fixture {revision}"),
        description: "Stable review fixture".to_string(),
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
        evaluation: contract(),
        instructions_digest: format!("{:x}", Sha256::digest(INSTRUCTIONS.as_bytes())),
    }
}

fn services() -> (tempfile::TempDir, Arc<RuntimeServices>, AgentDefinitionId) {
    let root = tempfile::tempdir().expect("temporary root");
    let home = root.path().join("home");
    let workspace = root.path().join("workspace");
    let builtin = root.path().join("builtin");
    fs::create_dir_all(&workspace).expect("workspace");
    let store = AgentDefinitionStore::new(
        RegisteredAgentDefinitionLayout::new(&builtin, home.join("definitions"), &workspace)
            .expect("layout"),
    );
    let id = AgentDefinitionId::new(DefinitionScope::Workspace, "evolution/stable")
        .expect("definition id");
    let baseline = store
        .store_revision(manifest(id.clone(), 1), INSTRUCTIONS)
        .expect("baseline");
    store
        .store_revision(manifest(id.clone(), 2), INSTRUCTIONS)
        .expect("candidate");
    store
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: baseline.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval:baseline".to_string(),
            },
            content_digest: baseline.revision.content_digest,
        })
        .expect("baseline stable");
    let services = RuntimeServices::builder(&home, &workspace)
        .builtin_definitions_root(&builtin)
        .evolution_eval_runner(Arc::new(EligibleRunner))
        .build()
        .expect("services");
    (root, services, id)
}

fn human_lease(review: &ReleaseChangeReview) -> (VerifiedPrincipal, VerifiedDecisionLease) {
    let key = Ed25519KeyPair::from_pkcs8(
        Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .expect("key material")
            .as_ref(),
    )
    .expect("key");
    let key_id = "stable-test-authority";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    let claims = PrincipalClaims {
        principal_id: "human:stable-operator".to_string(),
        tenant_id: "tenant:test".to_string(),
        grant_id: "grant:stable-operator".to_string(),
        kind: PrincipalKind::Human,
        scopes: vec!["workspace".to_string()],
        capabilities: vec!["evolution.release.manage".to_string()],
        assurance: PrincipalAssurance::HumanInteractive,
        issuer: key_id.to_string(),
        issued_at_ms: now,
        expires_at_ms: Some(now + 60_000),
        credential_fingerprint: "fixture".to_string(),
        credential_epoch: 1,
        profile_revision: 1,
        app_profiles: std::collections::BTreeMap::new(),
    };
    let verifier =
        PrincipalVerifier::from_base64(key_id, &BASE64.encode(key.public_key().as_ref()))
            .expect("verifier");
    let payload = serde_json::to_vec(&claims).expect("principal payload");
    let principal = verifier
        .verify(&SignedPrincipalEnvelope {
            key_id: key_id.to_string(),
            claims: claims.clone(),
            signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
        })
        .expect("principal");
    let lease_claims = DecisionLeaseClaims {
        lease_id: format!("lease:{}", review.review_id),
        principal_id: claims.principal_id,
        review_id: review.review_id.clone(),
        action: review.action_key().to_string(),
        scope: review.subject.scope_ref(),
        evidence_digest: review.evidence_digest(),
        issuer: key_id.to_string(),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
        credential_epoch: 1,
    };
    let payload = serde_json::to_vec(&lease_claims).expect("lease payload");
    let expected = DecisionLeaseExpectation::new(
        &lease_claims.review_id,
        &lease_claims.action,
        &lease_claims.scope,
        &lease_claims.evidence_digest,
    );
    let lease = verifier
        .verify_decision_lease(
            &SignedDecisionLease {
                key_id: key_id.to_string(),
                claims: lease_claims,
                signature_base64: BASE64.encode(key.sign(&payload).as_ref()),
            },
            &principal,
            &expected,
        )
        .expect("lease");
    (principal, lease)
}

#[tokio::test]
async fn stable_requires_approved_canary_and_eligible_observation_before_human_promotion() {
    let (_root, services, definition_id) = services();
    let signal = services
        .record_evolution_signal(runtime::EvolutionSignal::eval_failure(
            "stable-review",
            vec![EvidenceRef::observed("agent_run", "baseline")],
        ))
        .expect("signal");
    let proposal = services
        .create_evolution_lifecycle(vec![signal.signal_id])
        .expect("proposal")
        .proposal;
    let authority = HumanAuthority::new();
    let proposal_digest = services
        .evolution_proposal_decision_digest(&proposal.proposal_id, "approved")
        .expect("proposal digest");
    let proposal_lease = authority.lease_for_expectation(DecisionLeaseExpectation::new(
        format!("evolution-proposal:{}", proposal.proposal_id),
        "proposal.decision.approved",
        format!("evolution.proposal:{}", proposal.proposal_id),
        proposal_digest,
    ));
    services
        .decide_evolution_proposal(
            authority.principal(),
            &proposal_lease,
            &proposal.proposal_id,
            "approved",
        )
        .expect("proposal approved");
    let candidate = services
        .register_evolution_candidate(EvolutionCandidateIntent {
            candidate_id: "candidate-stable-v2".to_string(),
            proposal_id: proposal.proposal_id,
            subject: EvolutionCandidateSubject::AgentDefinition {
                revision_ref: harness_contract::agent::AgentDefinitionRevisionRef::new(
                    definition_id.clone(),
                    2,
                )
                .expect("revision"),
            },
            baseline_revision: 1,
            source_evidence_refs: vec![EvidenceRef::observed("agent_run", "baseline")],
            canary_policy: CanaryRolloutPolicy {
                traffic_basis_points: 10_000,
                minimum_samples: 1,
                minimum_duration_ms: 1,
                maximum_duration_ms: 60_000,
            },
        })
        .expect("candidate");
    services
        .evaluate_evolution_candidate(&candidate.candidate_id)
        .await
        .expect("evaluation");
    assert!(
        services
            .request_evolution_stable_review(&candidate.candidate_id)
            .is_err(),
        "candidate cannot manufacture a Stable review without Canary evidence"
    );
    let canary_review = services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("canary review");
    let (principal, canary_lease) = human_lease(&canary_review);
    let canary = services
        .decide_evolution_release_review(
            &principal,
            &canary_lease,
            &canary_review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve canary".to_string(),
        )
        .expect("canary decision")
        .expect("canary assignment");
    services
        .record_evolution_canary_observation(CanaryObservationReport {
            report_id: "canary-observation:stable".to_string(),
            candidate_id: candidate.candidate_id.clone(),
            canary_assignment_id: canary.assignment_id,
            generation: canary.generation,
            source_run_refs: vec!["agent-run:canary-1".to_string()],
            evidence_refs: vec![EvidenceRef::observed("canary_evaluation", "canary-1")],
            sample_count: 1,
            minimum_samples: 1,
            observed_duration_ms: 1,
            minimum_duration_ms: 1,
            hard_gates_passed: true,
            protected_dimensions_noninferior: true,
            created_at_ms: 2,
        })
        .expect("durable Canary observation");
    let stable_review = services
        .request_evolution_stable_review(&candidate.candidate_id)
        .expect("only Runtime observation gate creates stable review");
    assert_eq!(
        stable_review.prior_canary_approval_ref,
        Some(canary.approval_ref)
    );
    assert!(stable_review.observation_report_digest.is_some());
    let (_, stable_lease) = human_lease(&stable_review);
    services
        .decide_evolution_release_review(
            &principal,
            &stable_lease,
            &stable_review.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve stable".to_string(),
        )
        .expect("stable decision");
    let resolved = services
        .definition_registry()
        .resolve_agent(&definition_id, RevisionSelector::LatestApprovedStable)
        .expect("stable resolution");
    assert_eq!(resolved.revision.revision_ref.revision, 2);
}
