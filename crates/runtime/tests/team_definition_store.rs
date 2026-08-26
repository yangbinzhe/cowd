#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionLifecycle, RevisionSelector,
};
use harness_contract::team::{
    RoleBehaviorFacet, RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract,
    TeamRoleDefinition, TeamRoleTaskContract, TeamTemplateDefinitionId, TeamTemplateManifest,
    TeamTopologyContract,
};
use runtime::team_definition::{
    ScopedTeamTemplateLayout, TeamDefinitionStoreError, TeamTemplateDefinitionStore,
};
use sha2::{Digest, Sha256};

const MARKDOWN: &str = "# Review Team\n\nReview bounded evidence.\n";

fn store(root: &tempfile::TempDir) -> TeamTemplateDefinitionStore<ScopedTeamTemplateLayout> {
    TeamTemplateDefinitionStore::new(ScopedTeamTemplateLayout::new(
        root.path().join("builtin"),
        root.path().join("user"),
        root.path().join("workspace"),
    ))
}

fn manifest(revision: u64, name: &str) -> TeamTemplateManifest {
    TeamTemplateManifest {
        api_version: "cowd.team/v1".to_string(),
        template_id: TeamTemplateDefinitionId::new(DefinitionScope::Workspace, "quality/review")
            .unwrap(),
        revision,
        name: name.to_string(),
        display: None,
        lifecycle: RevisionLifecycle::Draft,
        topology: TeamTopologyContract {
            protocol_ref: "team/review@1".to_string(),
            require_synthesis: true,
            require_review: true,
        },
        role_aliases: std::collections::BTreeMap::new(),
        roles: vec![TeamRoleDefinition {
            role_id: "reviewer".to_string(),
            display_name: None,
            responsibility: "Inspect evidence.".to_string(),
            agent_definition_id: AgentDefinitionId::new(
                DefinitionScope::Workspace,
                "quality/researcher",
            )
            .unwrap(),
            agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
            cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            partition: RolePartitionPolicy::Single,
            behavior: vec![RoleBehaviorFacet::TerminalCandidate { required: true }],
            grant_ceiling: vec![AgentCapability::Read],
            task_contract: TeamRoleTaskContract {
                contract_ref: "task/review@1".to_string(),
                acceptance: vec!["evidence".to_string()],
                allowed_tool_contract_refs: Vec::new(),
                allowed_skill_refs: Vec::new(),
                dataflow: Default::default(),
            },
        }],
        dependencies: Vec::new(),
        result_contract: TeamResultContract {
            required_fields: vec!["summary".to_string(), "evidence".to_string()],
            evidence_required: true,
            synthesis_required: true,
        },
        evaluation: harness_contract::team::TeamEvaluationContract::single_release_gate(
            "quality/review",
            "team_success",
        ),
        instructions_digest: format!("{:x}", Sha256::digest(MARKDOWN.as_bytes())),
    }
}

#[test]
fn team_revision_integrity_rejects_semantic_overwrite_and_reopens_with_same_digest() {
    let root = tempfile::tempdir().unwrap();
    let initial = store(&root)
        .store_revision(manifest(1, "Evidence review"), MARKDOWN)
        .unwrap();
    let repeated = store(&root)
        .store_revision(manifest(1, "Evidence review"), MARKDOWN)
        .unwrap();
    assert_eq!(initial, repeated);
    assert!(matches!(
        store(&root).store_revision(manifest(1, "Changed review semantics"), MARKDOWN),
        Err(TeamDefinitionStoreError::RevisionConflict { .. })
    ));
    let reopened = store(&root)
        .read_revision(&initial.revision.revision_ref)
        .unwrap();
    assert_eq!(
        reopened.revision.content_digest,
        initial.revision.content_digest
    );
    assert_eq!(reopened.team_markdown, MARKDOWN);
}
