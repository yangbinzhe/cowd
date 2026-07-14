#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
    CognitiveReadScope, CognitiveWriteMode, DefaultPointer, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    RevisionSelector,
};
use harness_contract::evaluation::EvaluationContract;
use runtime::agent::definition::{
    AgentDefinitionResolver, AgentDefinitionStore, DefinitionStorageLayout, ScopedDefinitionLayout,
};
use sha2::{Digest, Sha256};

fn manifest(scope: DefinitionScope, local_name: &str, revision: u64) -> AgentDefinitionManifest {
    let markdown = "# Scoped Researcher\n\nReturn only evidence-backed findings.\n";
    AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: AgentDefinitionId::new(scope, local_name).expect("qualified definition"),
        revision,
        name: format!("{scope:?} Researcher"),
        description: "scope isolation fixture".to_string(),
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
        evaluation: EvaluationContract::single_release_gate("v0/scoped-registry", "task_success"),
        instructions_digest: format!("{:x}", Sha256::digest(markdown.as_bytes())),
    }
}

fn release(
    stored: &runtime::agent::definition::StoredAgentDefinitionRevision,
) -> ReleaseAssignment {
    ReleaseAssignment {
        scope: stored.revision.revision_ref.definition_id.scope(),
        revision_ref: stored.revision.revision_ref.clone(),
        channel: ReleaseChannel::Stable,
        status: ReleaseAssignmentStatus::Active,
        authorization: ReleaseAuthorization::HumanApproval {
            approval_ref: format!(
                "approval:{}",
                stored.revision.revision_ref.definition_id.as_str()
            ),
        },
        content_digest: stored.revision.content_digest.clone(),
    }
}

#[test]
fn qualified_user_and_workspace_definitions_are_isolated_without_name_shadowing() {
    let root = tempfile::tempdir().expect("temporary registry");
    let layout = ScopedDefinitionLayout::new(
        root.path().join("builtin"),
        root.path().join("user"),
        root.path().join("workspace"),
    );
    let store = AgentDefinitionStore::new(layout.clone());
    let user_id = AgentDefinitionId::new(DefinitionScope::User, "supply/researcher")
        .expect("user definition");
    let workspace_id = AgentDefinitionId::new(DefinitionScope::Workspace, "supply/researcher")
        .expect("workspace definition");
    let markdown = "# Scoped Researcher\n\nReturn only evidence-backed findings.\n";

    let user = store
        .store_revision(
            manifest(DefinitionScope::User, "supply/researcher", 1),
            markdown,
        )
        .expect("user revision");
    let workspace = store
        .store_revision(
            manifest(DefinitionScope::Workspace, "supply/researcher", 1),
            markdown,
        )
        .expect("workspace revision");
    store
        .record_release_assignment(&release(&user))
        .expect("user release");
    store
        .record_release_assignment(&release(&workspace))
        .expect("workspace release");
    store
        .set_default_pointer(&DefaultPointer::latest(
            DefinitionScope::User,
            user_id.clone(),
            ReleaseAuthorization::HumanApproval {
                approval_ref: "approval:user-pointer".to_string(),
            },
        ))
        .expect("user pointer");
    store
        .set_default_pointer(&DefaultPointer::latest(
            DefinitionScope::Workspace,
            workspace_id.clone(),
            ReleaseAuthorization::HumanApproval {
                approval_ref: "approval:workspace-pointer".to_string(),
            },
        ))
        .expect("workspace pointer");

    assert_ne!(
        layout
            .root_for_scope(DefinitionScope::User)
            .expect("user root"),
        layout
            .root_for_scope(DefinitionScope::Workspace)
            .expect("workspace root")
    );
    assert_ne!(user.revision.revision_ref, workspace.revision.revision_ref);

    let resolver = AgentDefinitionResolver::new(&store);
    let resolved_user = resolver
        .resolve(&user_id, RevisionSelector::LatestApprovedStable)
        .expect("user resolution");
    let resolved_workspace = resolver
        .resolve(&workspace_id, RevisionSelector::LatestApprovedStable)
        .expect("workspace resolution");
    assert_eq!(resolved_user.revision.revision_ref.definition_id, user_id);
    assert_eq!(
        resolved_workspace.revision.revision_ref.definition_id,
        workspace_id
    );
}
