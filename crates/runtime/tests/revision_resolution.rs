#![allow(clippy::expect_used, clippy::unwrap_used)]

mod evolution_test_support;

use evolution_test_support::{
    fixture, qualified_observation, register_and_evaluate, HumanAuthority, CANDIDATE_ID,
};
use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
    CognitiveReadScope, CognitiveWriteMode, DefaultPointer, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    RevisionSelector,
};
use harness_contract::evaluation::EvaluationContract;
use runtime::agent::definition::{
    AgentDefinitionResolver, AgentDefinitionStore, ScopedDefinitionLayout,
};
use runtime::{ReleaseChangeAction, ReleaseChangeRequest, ReleaseChangeReviewDecision};
use sha2::{Digest, Sha256};

fn store(root: &tempfile::TempDir) -> AgentDefinitionStore<ScopedDefinitionLayout> {
    AgentDefinitionStore::new(ScopedDefinitionLayout::new(
        root.path().join("builtin"),
        root.path().join("user"),
        root.path().join("workspace"),
    ))
}

fn manifest(id: AgentDefinitionId, revision: u64) -> AgentDefinitionManifest {
    let instructions = format!("# Researcher {revision}\n");
    AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: id,
        revision,
        name: "Researcher".to_string(),
        description: "Resolves a qualified revision.".to_string(),
        lifecycle: RevisionLifecycle::Published,
        executor: AgentExecutorPolicy::CowdNative,
        model_policy: AgentModelPolicy {
            profile: "default".to_string(),
            allowed_models: Vec::new(),
            fallback_allowed: false,
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
        evaluation: EvaluationContract::single_release_gate("quality/research", "task_success"),
        instructions_digest: format!("{:x}", Sha256::digest(instructions.as_bytes())),
    }
}

fn publish(
    store: &AgentDefinitionStore<ScopedDefinitionLayout>,
    id: AgentDefinitionId,
    revision: u64,
) {
    let instructions = format!("# Researcher {revision}\n");
    let stored = store
        .store_revision(manifest(id, revision), &instructions)
        .unwrap();
    store
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: stored.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: format!("approval:r{revision}"),
            },
            content_digest: stored.revision.content_digest,
        })
        .unwrap();
}

#[test]
fn latest_stable_and_manual_exact_pointer_resolve_only_the_qualified_approved_revision() {
    let root = tempfile::tempdir().unwrap();
    let store = store(&root);
    let id = AgentDefinitionId::new(DefinitionScope::Workspace, "quality/researcher").unwrap();
    publish(&store, id.clone(), 1);
    publish(&store, id.clone(), 2);
    let resolver = AgentDefinitionResolver::new(&store);
    assert_eq!(
        resolver
            .resolve(&id, RevisionSelector::LatestApprovedStable)
            .unwrap()
            .revision
            .revision_ref
            .revision,
        2
    );
    store
        .set_default_pointer(&DefaultPointer {
            scope: DefinitionScope::Workspace,
            definition_id: id.clone(),
            selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval:pin-r1".to_string(),
            },
        })
        .unwrap();
    assert_eq!(
        resolver
            .resolve_default(&id)
            .unwrap()
            .revision
            .revision_ref
            .revision,
        1
    );
    assert!(store
        .set_default_pointer(&DefaultPointer::latest(
            DefinitionScope::Workspace,
            id,
            ReleaseAuthorization::HumanApproval {
                approval_ref: "approval:overwrite-pin".to_string()
            }
        ))
        .is_err());
}

#[tokio::test]
async fn pin_and_rollback_are_both_applied_only_through_typed_runtime_reviews() {
    let fixture = fixture();
    let authority = HumanAuthority::new();
    let subject = runtime::EvolutionCandidateSubject::AgentDefinition {
        revision_ref: harness_contract::agent::AgentDefinitionRevisionRef::new(
            fixture.definition_id.clone(),
            1,
        )
        .expect("initial stable revision"),
    };
    let initial = fixture
        .services
        .request_evolution_release_change(ReleaseChangeRequest {
            request_id: "publish-revision-one".to_string(),
            subject: subject.clone(),
            action: ReleaseChangeAction::PublishInitialStable,
            selector: None,
            candidate_id: None,
            evidence_refs: vec!["audit:publish-r1".to_string()],
        })
        .expect("initial stable review");
    fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&initial),
            &initial.review_id,
            ReleaseChangeReviewDecision::Approve,
            "publish first stable revision".to_string(),
        )
        .expect("initial stable decision");

    let pin = fixture
        .services
        .request_evolution_release_change(ReleaseChangeRequest {
            request_id: "pin-revision-one".to_string(),
            subject: subject.clone(),
            action: ReleaseChangeAction::SetDefaultExact,
            selector: Some(RevisionSelector::ExactApprovedRevision { revision: 1 }),
            candidate_id: None,
            evidence_refs: vec!["operator:pin-r1".to_string()],
        })
        .expect("exact pointer review");
    fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&pin),
            &pin.review_id,
            ReleaseChangeReviewDecision::Approve,
            "keep revision one pinned".to_string(),
        )
        .expect("pin decision");

    let candidate = register_and_evaluate(&fixture, CANDIDATE_ID, 1, 2).await;
    let canary = fixture
        .services
        .request_evolution_canary_review(&candidate.candidate_id)
        .expect("canary review");
    let canary_assignment = fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&canary),
            &canary.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve candidate canary".to_string(),
        )
        .expect("canary decision")
        .expect("canary assignment");
    fixture
        .services
        .record_evolution_canary_observation(qualified_observation(
            &candidate.candidate_id,
            &canary_assignment,
        ))
        .expect("canary observation");
    let stable = fixture
        .services
        .request_evolution_stable_review(&candidate.candidate_id)
        .expect("stable review");
    fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&stable),
            &stable.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve candidate stable".to_string(),
        )
        .expect("stable decision");
    assert_eq!(
        fixture
            .services
            .definition_registry()
            .resolve_agent(&fixture.definition_id, RevisionSelector::DefaultPointer)
            .expect("manually pinned default")
            .revision
            .revision_ref
            .revision,
        1,
        "a later Stable release cannot override an explicit human pin"
    );

    let rollback = fixture
        .services
        .request_evolution_release_change(ReleaseChangeRequest {
            request_id: "rollback-to-revision-one".to_string(),
            subject,
            action: ReleaseChangeAction::Rollback,
            selector: Some(RevisionSelector::ExactApprovedRevision { revision: 1 }),
            candidate_id: None,
            evidence_refs: vec!["incident:rollback-r1".to_string()],
        })
        .expect("rollback review");
    fixture
        .services
        .decide_evolution_release_review(
            authority.principal(),
            &authority.lease_for(&rollback),
            &rollback.review_id,
            ReleaseChangeReviewDecision::Approve,
            "approve rollback through typed review".to_string(),
        )
        .expect("rollback decision");
    assert_eq!(
        fixture
            .services
            .definition_registry()
            .resolve_agent(&fixture.definition_id, RevisionSelector::DefaultPointer)
            .expect("resolved rollback target")
            .revision
            .revision_ref
            .revision,
        1
    );
}
