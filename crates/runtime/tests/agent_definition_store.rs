#![allow(clippy::expect_used, clippy::unwrap_used)]

use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
    CognitiveReadScope, CognitiveWriteMode, DefinitionScope, RevisionLifecycle,
};
use harness_contract::evaluation::EvaluationContract;
use runtime::agent::definition::{
    AgentDefinitionStore, DefinitionStoreError, ScopedDefinitionLayout,
};
use sha2::{Digest, Sha256};

const MARKDOWN: &str = "# Researcher\n\nProduce evidence only.\n";

fn store(root: &tempfile::TempDir) -> AgentDefinitionStore<ScopedDefinitionLayout> {
    AgentDefinitionStore::new(ScopedDefinitionLayout::new(
        root.path().join("builtin"),
        root.path().join("user"),
        root.path().join("workspace"),
    ))
}

fn manifest(revision: u64, description: &str) -> AgentDefinitionManifest {
    AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: AgentDefinitionId::new(DefinitionScope::Workspace, "quality/researcher")
            .unwrap(),
        revision,
        name: "Researcher".to_string(),
        description: description.to_string(),
        lifecycle: RevisionLifecycle::Draft,
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
        instructions_digest: format!("{:x}", Sha256::digest(MARKDOWN.as_bytes())),
    }
}

#[test]
fn immutable_revision_is_idempotent_only_for_identical_content_and_survives_store_reopen() {
    let root = tempfile::tempdir().unwrap();
    let first = store(&root)
        .store_revision(manifest(1, "Original evidence contract."), MARKDOWN)
        .expect("write immutable revision");
    let same = store(&root)
        .store_revision(manifest(1, "Original evidence contract."), MARKDOWN)
        .expect("identical write is idempotent");
    assert_eq!(same, first);

    let conflict = store(&root).store_revision(manifest(1, "Changed semantics."), MARKDOWN);
    assert!(matches!(
        conflict,
        Err(DefinitionStoreError::RevisionConflict { .. })
    ));

    let reopened = store(&root)
        .read_revision(&first.revision.revision_ref)
        .expect("verified persisted revision");
    assert_eq!(
        reopened.revision.content_digest,
        first.revision.content_digest
    );
    assert_eq!(reopened.agent_markdown, MARKDOWN);
}
