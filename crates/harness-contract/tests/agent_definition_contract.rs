use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
    CognitiveReadScope, CognitiveWriteMode, DefaultPointer, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    RevisionSelector,
};
use harness_contract::evaluation::EvaluationContract;

fn manifest(scope: DefinitionScope) -> AgentDefinitionManifest {
    AgentDefinitionManifest {
        api_version: "cowd.agent/v1".to_string(),
        definition_id: AgentDefinitionId::new(scope, "quality/researcher").expect("id"),
        revision: 1,
        name: "Quality researcher".to_string(),
        description: "Produces bounded evidence for a reviewed task.".to_string(),
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
        instructions_digest: "a".repeat(64),
    }
}

#[test]
fn qualified_identity_and_release_authority_keep_lifecycle_and_routing_orthogonal() {
    let workspace = manifest(DefinitionScope::Workspace);
    workspace.validate().expect("valid executable definition");

    let release = ReleaseAssignment {
        scope: DefinitionScope::Workspace,
        revision_ref: workspace.revision_ref(),
        channel: ReleaseChannel::Stable,
        status: ReleaseAssignmentStatus::Active,
        authorization: ReleaseAuthorization::HumanApproval {
            approval_ref: "approval:quality-researcher-v1".to_string(),
        },
        content_digest: "b".repeat(64),
    };
    release
        .validate()
        .expect("workspace stable requires human approval");
    assert!(release.is_active_approved_stable());

    let pointer = DefaultPointer::latest(
        DefinitionScope::Workspace,
        workspace.definition_id.clone(),
        ReleaseAuthorization::HumanApproval {
            approval_ref: "approval:quality-researcher-default".to_string(),
        },
    );
    pointer
        .validate()
        .expect("default selector is independently valid");
    assert!(matches!(
        pointer.selector,
        RevisionSelector::LatestApprovedStable
    ));

    let mut revoked = workspace.clone();
    revoked.lifecycle = RevisionLifecycle::Revoked;
    assert!(!revoked.lifecycle.can_create_new_binding());
    assert!(
        release.is_active_approved_stable(),
        "release history does not rewrite revision lifecycle"
    );

    let invalid_builtin = ReleaseAssignment {
        scope: DefinitionScope::Builtin,
        revision_ref: manifest(DefinitionScope::Builtin).revision_ref(),
        channel: ReleaseChannel::Stable,
        status: ReleaseAssignmentStatus::Active,
        authorization: ReleaseAuthorization::HumanApproval {
            approval_ref: "approval:cannot-authorize-builtin".to_string(),
        },
        content_digest: "c".repeat(64),
    };
    assert!(
        invalid_builtin.validate().is_err(),
        "builtin trust cannot be forged as a human release"
    );
}
