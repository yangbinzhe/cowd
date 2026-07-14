#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
    CognitiveReadScope, CognitiveWriteMode, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    RevisionSelector,
};
use harness_contract::evaluation::EvaluationContract;
use runtime::agent::definition::{
    AgentDefinitionResolver, AgentDefinitionStore, ScopedDefinitionLayout,
};
use sha2::{Digest, Sha256};

fn manifest(
    id: AgentDefinitionId,
    revision: u64,
    lifecycle: RevisionLifecycle,
) -> AgentDefinitionManifest {
    let body = format!("# R{revision}\n");
    AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: id,
        revision,
        name: format!("R{revision}"),
        description: "Eligibility fixture.".to_string(),
        lifecycle,
        executor: AgentExecutorPolicy::CowdNative,
        model_policy: AgentModelPolicy {
            profile: "default".to_string(),
            allowed_models: Vec::new(),
            fallback_allowed: false,
        },
        cognitive_policy: AgentCognitivePolicy {
            context_profile: "default".to_string(),
            read_scopes: vec![CognitiveReadScope::Session],
            write_mode: CognitiveWriteMode::None,
            team_working_state_visible: false,
        },
        capability_contract: AgentCapabilityContract {
            capability_ceiling: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            approval_required_for: Vec::new(),
        },
        output_contract: AgentOutputContract::reviewable(),
        evaluation: EvaluationContract::single_release_gate("quality/eligibility", "success"),
        instructions_digest: format!("{:x}", Sha256::digest(body.as_bytes())),
    }
}

#[test]
fn only_published_active_human_stable_revisions_are_resolvable() {
    let root = tempfile::tempdir().unwrap();
    let store = AgentDefinitionStore::new(ScopedDefinitionLayout::new(
        root.path().join("builtin"),
        root.path().join("user"),
        root.path().join("workspace"),
    ));
    let id = AgentDefinitionId::new(DefinitionScope::Workspace, "quality/eligibility").unwrap();
    for (revision, lifecycle) in [
        (1, RevisionLifecycle::Draft),
        (2, RevisionLifecycle::Published),
        (3, RevisionLifecycle::Published),
        (4, RevisionLifecycle::Published),
    ] {
        let body = format!("# R{revision}\n");
        let stored = store
            .store_revision(manifest(id.clone(), revision, lifecycle), &body)
            .unwrap();
        let (status, authorization) = match revision {
            2 => (
                ReleaseAssignmentStatus::Stopped,
                ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval:stopped".to_string(),
                },
            ),
            3 => (
                ReleaseAssignmentStatus::Active,
                ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval:active".to_string(),
                },
            ),
            4 => (
                ReleaseAssignmentStatus::Active,
                ReleaseAuthorization::ReleaseAuthorityAttestation {
                    attestation_ref: "release:invalid-workspace".to_string(),
                },
            ),
            _ => continue,
        };
        if revision != 4 {
            store
                .record_release_assignment(&ReleaseAssignment {
                    scope: DefinitionScope::Workspace,
                    revision_ref: stored.revision.revision_ref,
                    channel: ReleaseChannel::Stable,
                    status,
                    authorization,
                    content_digest: stored.revision.content_digest,
                })
                .unwrap();
        } else {
            assert!(ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref,
                channel: ReleaseChannel::Stable,
                status,
                authorization,
                content_digest: stored.revision.content_digest
            }
            .validate()
            .is_err());
        }
    }
    let resolver = AgentDefinitionResolver::new(&store);
    assert_eq!(
        resolver
            .resolve(&id, RevisionSelector::LatestApprovedStable)
            .unwrap()
            .revision
            .revision_ref
            .revision,
        3
    );
    for revision in [1, 2, 4] {
        assert!(
            resolver
                .resolve(&id, RevisionSelector::ExactApprovedRevision { revision })
                .is_err(),
            "revision {revision} must not produce a new binding"
        );
    }
}
