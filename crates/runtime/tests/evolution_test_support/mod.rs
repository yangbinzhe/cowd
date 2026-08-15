#![allow(clippy::expect_used, clippy::unwrap_used, dead_code)]

use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use harness_contract::{
    agent::{
        AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
        AgentDefinitionManifest, AgentDefinitionRevisionRef, AgentExecutorPolicy, AgentModelPolicy,
        AgentOutputContract, CognitiveReadScope, CognitiveWriteMode, DefinitionScope,
        ReleaseAssignment, ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel,
        RevisionLifecycle,
    },
    evaluation::EvaluationContract,
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
    CanaryObservationReport, CanaryRolloutPolicy, DecisionLeaseExpectation,
    EvolutionCandidateIntent, EvolutionCandidateSubject, EvolutionComparisonDimension,
    EvolutionComparisonReportV2, EvolutionEvalRunner, ReleaseChangeReview, RuntimeServices,
    VerifiedDecisionLease, VerifiedPrincipal,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

pub const CANDIDATE_ID: &str = "workspace/cowd/evolution-agent-candidate";

pub struct EvolutionFixture {
    _root: TempDir,
    pub services: Arc<RuntimeServices>,
    pub definition_id: AgentDefinitionId,
}

pub fn fixture() -> EvolutionFixture {
    let root = tempfile::tempdir().expect("temporary Runtime root");
    let workspace = root.path().join("workspace");
    fs::create_dir_all(&workspace).expect("workspace root");
    let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/evolution-agent")
        .expect("fixture definition id");
    for revision in 1..=3 {
        write_published_agent_revision(&workspace, &definition_id, revision);
    }
    let storage = storage::StorageRegistry::default_for_config_home(root.path())
        .with_workspace(&workspace)
        .expect("storage registry");
    let definition_store = AgentDefinitionStore::new(
        RegisteredAgentDefinitionLayout::from_storage_layout(
            &storage.layout,
            root.path().join("runtime").join("builtin-definitions"),
            &workspace,
        )
        .expect("definition layout"),
    );
    let baseline_ref =
        AgentDefinitionRevisionRef::new(definition_id.clone(), 1).expect("baseline revision ref");
    let baseline = definition_store
        .read_revision(&baseline_ref)
        .expect("baseline revision");
    definition_store
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: baseline.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "fixture:baseline-stable".to_string(),
            },
            content_digest: baseline.revision.content_digest,
        })
        .expect("baseline stable assignment");
    let services = RuntimeServices::builder(root.path(), &workspace)
        .evolution_eval_runner(Arc::new(EligibleEvalRunner))
        .build()
        .expect("Runtime services");
    EvolutionFixture {
        _root: root,
        services,
        definition_id,
    }
}

pub async fn register_and_evaluate(
    fixture: &EvolutionFixture,
    candidate_id: &str,
    baseline_revision: u64,
    candidate_revision: u64,
) -> runtime::EvolutionGovernanceCandidate {
    try_register_and_evaluate(fixture, candidate_id, baseline_revision, candidate_revision)
        .await
        .expect("register and evaluate candidate")
}

pub async fn try_register_and_evaluate(
    fixture: &EvolutionFixture,
    candidate_id: &str,
    baseline_revision: u64,
    candidate_revision: u64,
) -> Result<runtime::EvolutionGovernanceCandidate, String> {
    let candidate_ref =
        AgentDefinitionRevisionRef::new(fixture.definition_id.clone(), candidate_revision)
            .map_err(|error| error.to_string())?;
    let signal = fixture
        .services
        .record_evolution_signal(runtime::EvolutionSignal::eval_failure(
            format!("{candidate_id}:baseline"),
            vec![EvidenceRef::observed(
                "agent_run",
                format!("{candidate_id}:baseline"),
            )],
        ))
        .map_err(|error| error.to_string())?;
    let proposal = fixture
        .services
        .create_evolution_lifecycle(vec![signal.signal_id])
        .map_err(|error| error.to_string())?
        .proposal;
    let authority = HumanAuthority::new();
    let digest = fixture
        .services
        .evolution_proposal_decision_digest(&proposal.proposal_id, "approved")
        .map_err(|error| error.to_string())?;
    let lease = authority.lease_for_expectation(DecisionLeaseExpectation::new(
        format!("evolution-proposal:{}", proposal.proposal_id),
        "proposal.decision.approved",
        format!("evolution.proposal:{}", proposal.proposal_id),
        digest,
    ));
    fixture
        .services
        .decide_evolution_proposal(
            authority.principal(),
            &lease,
            &proposal.proposal_id,
            "approved",
        )
        .map_err(|error| error.to_string())?;
    fixture
        .services
        .register_evolution_candidate(EvolutionCandidateIntent {
            candidate_id: candidate_id.to_string(),
            proposal_id: proposal.proposal_id,
            subject: EvolutionCandidateSubject::AgentDefinition {
                revision_ref: candidate_ref,
            },
            baseline_revision,
            source_evidence_refs: vec![EvidenceRef::observed(
                "agent_run",
                format!("{candidate_id}:baseline"),
            )],
            canary_policy: CanaryRolloutPolicy {
                traffic_basis_points: 10_000,
                minimum_samples: 2,
                minimum_duration_ms: 1,
                maximum_duration_ms: 60_000,
            },
        })
        .map_err(|error| error.to_string())?;
    fixture
        .services
        .evaluate_evolution_candidate(candidate_id)
        .await
        .map_err(|error| error.to_string())
}

pub fn qualified_observation(
    candidate_id: &str,
    assignment: &runtime::EvolutionReleaseAssignment,
) -> CanaryObservationReport {
    CanaryObservationReport {
        report_id: format!("observation:{candidate_id}:{}", assignment.generation),
        candidate_id: candidate_id.to_string(),
        canary_assignment_id: assignment.assignment_id.clone(),
        generation: assignment.generation,
        source_run_refs: vec![format!("agent-run:{candidate_id}:1")],
        evidence_refs: vec![EvidenceRef::observed(
            "canary_evaluation",
            format!("{candidate_id}:canary"),
        )],
        sample_count: 2,
        minimum_samples: 2,
        observed_duration_ms: 1,
        minimum_duration_ms: 1,
        hard_gates_passed: true,
        protected_dimensions_noninferior: true,
        created_at_ms: now_ms(),
    }
}

pub struct HumanAuthority {
    key_id: String,
    key_pair: Ed25519KeyPair,
    verifier: runtime::security::PrincipalVerifier,
    principal: VerifiedPrincipal,
}

impl HumanAuthority {
    pub fn new() -> Self {
        let key_pair = Ed25519KeyPair::from_pkcs8(
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
                .expect("Ed25519 key material")
                .as_ref(),
        )
        .expect("Ed25519 key pair");
        let key_id = "evolution-test-authority".to_string();
        let verifier = runtime::security::PrincipalVerifier::from_base64(
            key_id.clone(),
            &BASE64.encode(key_pair.public_key().as_ref()),
        )
        .expect("principal verifier");
        let claims = PrincipalClaims {
            principal_id: "human-evolution-operator".to_string(),
            tenant_id: "tenant:test".to_string(),
            grant_id: "grant:evolution-operator".to_string(),
            kind: PrincipalKind::Human,
            scopes: vec!["workspace/cowd/evolution-agent".to_string()],
            capabilities: vec![
                "approval.respond".to_string(),
                "evolution.release.manage".to_string(),
            ],
            assurance: PrincipalAssurance::HumanInteractive,
            issuer: key_id.clone(),
            issued_at_ms: now_ms(),
            expires_at_ms: Some(now_ms().saturating_add(60_000)),
            credential_fingerprint: "test-human-evolution-operator".to_string(),
            credential_epoch: 1,
            profile_revision: 1,
            app_profiles: std::collections::BTreeMap::new(),
        };
        let principal = verifier
            .verify(&SignedPrincipalEnvelope {
                key_id: key_id.clone(),
                signature_base64: sign_json(&key_pair, &claims),
                claims,
            })
            .expect("verified human principal");
        Self {
            key_id,
            key_pair,
            verifier,
            principal,
        }
    }

    pub fn principal(&self) -> &VerifiedPrincipal {
        &self.principal
    }

    pub fn lease_for(&self, review: &ReleaseChangeReview) -> VerifiedDecisionLease {
        let expectation = DecisionLeaseExpectation::new(
            review.review_id.clone(),
            review.action_key(),
            review.subject.scope_ref(),
            review.evidence_digest(),
        );
        self.lease_for_expectation(expectation)
    }

    pub fn lease_for_expectation(
        &self,
        expectation: DecisionLeaseExpectation,
    ) -> VerifiedDecisionLease {
        let claims = DecisionLeaseClaims {
            lease_id: format!("lease:{}:{}", expectation.review_id, uuid::Uuid::new_v4()),
            principal_id: self.principal.claims().principal_id.clone(),
            review_id: expectation.review_id.clone(),
            action: expectation.action.clone(),
            scope: expectation.scope.clone(),
            evidence_digest: expectation.evidence_digest.clone(),
            issuer: self.key_id.clone(),
            issued_at_ms: now_ms(),
            expires_at_ms: now_ms().saturating_add(60_000),
            credential_epoch: self.principal.credential_epoch(),
        };
        self.verifier
            .verify_decision_lease(
                &SignedDecisionLease {
                    key_id: self.key_id.clone(),
                    signature_base64: sign_json(&self.key_pair, &claims),
                    claims,
                },
                &self.principal,
                &expectation,
            )
            .expect("verified decision lease")
    }
}

struct EligibleEvalRunner;

#[async_trait]
impl EvolutionEvalRunner for EligibleEvalRunner {
    async fn evaluate(
        &self,
        candidate: &runtime::EvolutionGovernanceCandidate,
    ) -> Result<EvolutionComparisonReportV2, String> {
        let dimensions = candidate
            .evaluation_contract
            .metrics
            .iter()
            .map(|metric| {
                let (baseline, candidate_value) = match metric.direction {
                    harness_contract::evaluation::EvaluationMetricDirection::HigherIsBetter => {
                        (0.80, 0.95)
                    }
                    harness_contract::evaluation::EvaluationMetricDirection::LowerIsBetter => {
                        (10.0, 8.0)
                    }
                };
                EvolutionComparisonDimension {
                    metric_id: metric.metric_id.clone(),
                    direction: metric.direction,
                    baseline,
                    candidate: candidate_value,
                    non_inferiority_margin: metric.non_inferiority_margin(),
                    sample_count: metric.minimum_samples,
                    minimum_samples: metric.minimum_samples,
                    confidence: 1.0,
                    minimum_confidence: metric.minimum_confidence(),
                    minimum_improvement: metric.minimum_improvement(),
                    superiority_confidence: 1.0,
                    minimum_superiority_confidence: metric.minimum_superiority_confidence(),
                    hard_gate: metric.hard_gate,
                    protected: metric.protected,
                    target_improvement: metric.target_improvement,
                }
            })
            .collect();
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
            executed_sample_count: candidate
                .evaluation_contract
                .metrics
                .iter()
                .map(|metric| metric.minimum_samples)
                .max()
                .unwrap_or_default(),
            dimensions,
            source_run_refs: vec![format!("paired-run:{}", candidate.candidate_id)],
            evidence_refs: vec![EvidenceRef::observed(
                "paired_evaluation",
                candidate.candidate_id.clone(),
            )],
            created_at_ms: now_ms(),
        })
    }
}

fn write_published_agent_revision(
    workspace: &Path,
    definition_id: &AgentDefinitionId,
    revision: u64,
) {
    let instructions = format!(
        "# Evolution Agent revision {revision}\n\nProduce bounded evidence and preserve release constraints.\n"
    );
    let manifest = AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: definition_id.clone(),
        revision,
        name: "Evolution Agent".to_string(),
        description: "Fixture definition for end-to-end evolution governance tests.".to_string(),
        lifecycle: RevisionLifecycle::Published,
        executor: AgentExecutorPolicy::CowdNative,
        model_policy: AgentModelPolicy {
            profile: "default".to_string(),
            allowed_models: Vec::new(),
            fallback_allowed: true,
        },
        cognitive_policy: AgentCognitivePolicy {
            context_profile: "default".to_string(),
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
        evaluation: EvaluationContract::single_release_gate(
            "evolution/fixture-workload",
            "task_success",
        ),
        instructions_digest: format!("{:x}", Sha256::digest(instructions.as_bytes())),
    };
    let revision_dir = workspace
        .join(".cowd/definitions/agents")
        .join(
            definition_id
                .as_str()
                .strip_prefix("workspace/")
                .expect("scope prefix"),
        )
        .join("revisions")
        .join(revision.to_string());
    fs::create_dir_all(&revision_dir).expect("definition revision directory");
    fs::write(
        revision_dir.join("agent.yaml"),
        serde_yaml::to_string(&manifest).expect("manifest yaml"),
    )
    .expect("write manifest");
    fs::write(revision_dir.join("AGENT.md"), instructions).expect("write instructions");
}

fn sign_json<T: serde::Serialize>(key_pair: &Ed25519KeyPair, value: &T) -> String {
    let payload = serde_json::to_vec(value).expect("signed payload");
    BASE64.encode(key_pair.sign(&payload).as_ref())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
