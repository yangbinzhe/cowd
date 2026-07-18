//! Runtime-owned registry for durable Agent and Team Definition assets.
//!
//! This is the composition boundary between the generic storage registry and
//! executable Definition resolvers. It deliberately has no current-directory
//! discovery, no name shadowing, and no Gateway dependency.

use std::path::PathBuf;

use harness_contract::agent::{
    AgentDefinitionId, AgentDefinitionRevisionRef, DefaultPointer, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionSelector,
};
use harness_contract::team::{
    TeamRoleDefinition, TeamRoleDependency, TeamTemplateDefinitionId, TeamTemplateRevisionRef,
    TeamTopologyContract,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::agent::definition::{
    bootstrap_builtin_agents, AgentDefinitionResolver, AgentDefinitionStore, BuiltinAgentTrust,
    DefinitionStoreError, ExplicitTomlAgentImport, RegisteredAgentDefinitionLayout,
    ResolvedAgentDefinition,
};
use crate::team_definition::{
    bootstrap_builtin_teams, BuiltinTeamTrust, ExactAgentRevisionResolver,
    RegisteredTeamTemplateLayout, ResolvedTeamTemplate, TeamDefaultPointer,
    TeamDefinitionStoreError, TeamReleaseAssignment, TeamTemplateDefinitionResolver,
    TeamTemplateDefinitionStore,
};
use crate::{
    AgentCatalogEntry, EvolutionCandidateSubject, EvolutionReleaseAssignment, ReleaseChangeAction,
};

/// Composition failures for the two Definition domains.
#[derive(Debug, Error)]
pub enum DefinitionRegistryError {
    #[error(transparent)]
    Agent(#[from] DefinitionStoreError),
    #[error(transparent)]
    Team(#[from] TeamDefinitionStoreError),
}

/// Read-only projection of a runnable Team Template revision. It deliberately
/// contains no mutable TeamWorkingState or execution graph data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTeamTemplateCatalogEntry {
    pub revision_ref: TeamTemplateRevisionRef,
    pub name: String,
    pub topology: TeamTopologyContract,
    pub role_count: usize,
    #[serde(default)]
    pub roles: Vec<TeamRoleDefinition>,
    #[serde(default)]
    pub dependencies: Vec<TeamRoleDependency>,
    pub result_fields: Vec<String>,
}

/// Receipt for an explicitly imported Agent Definition. A draft receipt is
/// intentionally not a resolver result: draft revisions are not runnable and
/// must not be confused with an approved Binding candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinitionDraftReceipt {
    pub revision_ref: AgentDefinitionRevisionRef,
    pub content_digest: String,
}

/// Sole Runtime composition root for Agent and Team Definition stores.
///
/// Both domains share one registered definitions root but retain separate
/// `agents/` and `teams/` subtrees inside their stores. All IDs are qualified
/// by scope, so a workspace definition can never shadow a user or builtin
/// definition by local name alone.
#[derive(Debug)]
pub struct RuntimeDefinitionRegistry {
    agents: AgentDefinitionStore<RegisteredAgentDefinitionLayout>,
    teams: TeamTemplateDefinitionStore<RegisteredTeamTemplateLayout>,
    builtin_agent_trust: BuiltinAgentTrust,
    builtin_team_trust: BuiltinTeamTrust,
}

impl RuntimeDefinitionRegistry {
    /// Create the registry from a registered user storage layout and explicit
    /// builtin/workspace roots. `builtin_definitions_root` is the verified
    /// release bundle's definitions root, never a user-configurable path.
    pub fn from_storage_layout(
        storage: &storage::StorageLayout,
        builtin_definitions_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<Self, DefinitionRegistryError> {
        let builtin_definitions_root = builtin_definitions_root.into();
        let workspace_root = workspace_root.into();
        let agents =
            AgentDefinitionStore::new(RegisteredAgentDefinitionLayout::from_storage_layout(
                storage,
                builtin_definitions_root.clone(),
                workspace_root.clone(),
            )?);
        let teams =
            TeamTemplateDefinitionStore::new(RegisteredTeamTemplateLayout::from_storage_layout(
                storage,
                builtin_definitions_root,
                workspace_root,
            )?);
        let builtin_agent_trust = bootstrap_builtin_agents(&agents)?;
        let builtin_team_trust = bootstrap_builtin_teams(&teams)?;
        Ok(Self {
            agents,
            teams,
            builtin_agent_trust,
            builtin_team_trust,
        })
    }

    #[must_use]
    pub(crate) fn agents(&self) -> &AgentDefinitionStore<RegisteredAgentDefinitionLayout> {
        &self.agents
    }

    #[must_use]
    pub(crate) fn teams(&self) -> &TeamTemplateDefinitionStore<RegisteredTeamTemplateLayout> {
        &self.teams
    }

    #[must_use]
    pub fn agent_resolver(&self) -> AgentDefinitionResolver<'_, RegisteredAgentDefinitionLayout> {
        AgentDefinitionResolver::new(self.agents())
    }

    #[must_use]
    pub fn team_resolver(
        &self,
    ) -> TeamTemplateDefinitionResolver<'_, RegisteredTeamTemplateLayout> {
        TeamTemplateDefinitionResolver::new(self.teams())
    }

    pub fn resolve_agent(
        &self,
        definition_id: &AgentDefinitionId,
        selector: RevisionSelector,
    ) -> Result<ResolvedAgentDefinition, DefinitionRegistryError> {
        let resolved = self
            .agent_resolver()
            .resolve(definition_id, selector)
            .map_err(DefinitionRegistryError::from)?;
        if resolved.revision.revision_ref.definition_id.scope()
            == harness_contract::agent::DefinitionScope::Builtin
        {
            self.builtin_agent_trust
                .verify(
                    &resolved.revision.revision_ref,
                    &resolved.revision.content_digest,
                )
                .map_err(DefinitionRegistryError::from)?;
        }
        Ok(resolved)
    }

    /// Resolve an immutable Agent revision that has already been selected by
    /// the Runtime evolution ledger for a Canary Binding. This bypasses the
    /// Stable-only resolver *only* after governance has supplied the exact
    /// approved Canary assignment; callers cannot discover arbitrary Drafts
    /// through this method because it is crate-visible.
    pub(crate) fn resolve_agent_canary(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
    ) -> Result<ResolvedAgentDefinition, DefinitionRegistryError> {
        let stored = self.agents().read_revision(revision_ref)?;
        if !stored.revision.manifest.lifecycle.can_create_new_binding() {
            return Err(DefinitionRegistryError::Agent(
                DefinitionStoreError::UnresolvablePointer(
                    revision_ref.definition_id.clone(),
                    "Canary assignment must reference a published Definition revision".to_string(),
                ),
            ));
        }
        Ok(ResolvedAgentDefinition {
            revision: stored.revision,
            agent_markdown: stored.agent_markdown,
            selected_by: RevisionSelector::ExactApprovedRevision {
                revision: revision_ref.revision,
            },
        })
    }

    pub fn resolve_team(
        &self,
        template_id: &TeamTemplateDefinitionId,
        selector: RevisionSelector,
    ) -> Result<ResolvedTeamTemplate, DefinitionRegistryError> {
        let resolved = self
            .team_resolver()
            .resolve(template_id, selector, self)
            .map_err(DefinitionRegistryError::from)?;
        if resolved.revision.revision_ref.template_id.scope()
            == harness_contract::agent::DefinitionScope::Builtin
        {
            self.builtin_team_trust
                .verify(
                    &resolved.revision.revision_ref,
                    &resolved.revision.content_digest,
                )
                .map_err(DefinitionRegistryError::from)?;
        }
        Ok(resolved)
    }

    /// Resolve an immutable Team revision that has already been selected by
    /// the Runtime evolution ledger for a Canary instantiation. Like the
    /// Agent equivalent, this is crate-visible so it cannot become a generic
    /// bypass around Stable/default resolution.
    pub(crate) fn resolve_team_canary(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
    ) -> Result<ResolvedTeamTemplate, DefinitionRegistryError> {
        let stored = self.teams().read_revision(revision_ref)?;
        if !stored.revision.manifest.lifecycle.can_create_new_binding() {
            return Err(DefinitionRegistryError::Team(
                TeamDefinitionStoreError::UnresolvablePointer(
                    revision_ref.template_id.clone(),
                    "Canary assignment must reference a published Team Template revision"
                        .to_string(),
                ),
            ));
        }
        for role in &stored.revision.manifest.roles {
            let RevisionSelector::ExactApprovedRevision { revision } = &role.agent_selector else {
                return Err(DefinitionRegistryError::Team(
                    TeamDefinitionStoreError::UnresolvablePointer(
                        revision_ref.template_id.clone(),
                        "Canary Team Template role must pin an approved Agent revision".to_string(),
                    ),
                ));
            };
            self.agents
                .ensure_eligible_revision(&role.agent_definition_id, *revision)?;
        }
        Ok(ResolvedTeamTemplate {
            revision: stored.revision,
            team_markdown: stored.team_markdown,
            selected_by: RevisionSelector::ExactApprovedRevision {
                revision: revision_ref.revision,
            },
        })
    }

    /// Rebuild the Runtime's runnable Agent catalog from immutable Definition
    /// revisions. Draft, revoked, stopped, quarantined, and corrupted
    /// Definitions never appear in this projection.
    pub fn runnable_agent_catalog(
        &self,
    ) -> Result<Vec<AgentCatalogEntry>, DefinitionRegistryError> {
        let mut entries = Vec::new();
        for definition_id in self.agents().list_definition_ids()? {
            let resolved =
                match self.resolve_agent(&definition_id, RevisionSelector::LatestApprovedStable) {
                    Ok(resolved) => resolved,
                    Err(DefinitionRegistryError::Agent(
                        DefinitionStoreError::UnresolvablePointer(_, _),
                    )) => continue,
                    Err(error) => return Err(error),
                };
            entries.push(agent_catalog_entry(&resolved.revision));
        }
        entries.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        Ok(entries)
    }

    /// Rebuild the runnable Team Template projection from the same exact
    /// Definition resolver used at instantiation time.
    pub fn runnable_team_catalog(
        &self,
    ) -> Result<Vec<RuntimeTeamTemplateCatalogEntry>, DefinitionRegistryError> {
        let mut entries = Vec::new();
        for template_id in self.teams().list_template_ids()? {
            let resolved =
                match self.resolve_team(&template_id, RevisionSelector::LatestApprovedStable) {
                    Ok(resolved) => resolved,
                    Err(DefinitionRegistryError::Team(
                        TeamDefinitionStoreError::UnresolvablePointer(_, _),
                    )) => continue,
                    Err(error) => return Err(error),
                };
            let manifest = &resolved.revision.manifest;
            entries.push(RuntimeTeamTemplateCatalogEntry {
                revision_ref: resolved.revision.revision_ref,
                name: manifest.name.clone(),
                topology: manifest.topology.clone(),
                role_count: manifest.roles.len(),
                roles: manifest.roles.clone(),
                dependencies: manifest.dependencies.clone(),
                result_fields: manifest.result_contract.required_fields.clone(),
            });
        }
        entries.sort_by(|left, right| {
            left.revision_ref
                .template_id
                .as_str()
                .cmp(right.revision_ref.template_id.as_str())
        });
        Ok(entries)
    }

    /// Explicitly import one caller-selected external TOML Definition as a
    /// local Draft. The adapter cannot discover external roots or set release
    /// state, so imports never become runnable without the Runtime release
    /// command and a human decision.
    pub(crate) fn import_agent_toml_draft(
        &self,
        import: ExplicitTomlAgentImport,
    ) -> Result<AgentDefinitionDraftReceipt, DefinitionRegistryError> {
        let stored = self.agents().import_draft(import.into_draft()?)?;
        Ok(AgentDefinitionDraftReceipt {
            revision_ref: stored.revision.revision_ref,
            content_digest: stored.revision.content_digest,
        })
    }

    /// Materialize an already-authorized Runtime evolution release event into
    /// the immutable Definition stores. The event ledger remains the source
    /// of authorization; this idempotent projection can be replayed after a
    /// process crash without granting a new release decision.
    pub(crate) fn materialize_evolution_release(
        &self,
        assignment: &EvolutionReleaseAssignment,
    ) -> Result<(), DefinitionRegistryError> {
        match &assignment.subject {
            EvolutionCandidateSubject::AgentDefinition { revision_ref } => {
                let stored = self.agents.read_revision(revision_ref)?;
                let authorization = ReleaseAuthorization::HumanApproval {
                    approval_ref: assignment.approval_ref.clone(),
                };
                match assignment.action {
                    ReleaseChangeAction::PromoteCanary => {
                        self.agents.record_release_assignment(&ReleaseAssignment {
                            scope: revision_ref.definition_id.scope(),
                            revision_ref: revision_ref.clone(),
                            channel: ReleaseChannel::Canary,
                            status: ReleaseAssignmentStatus::Active,
                            authorization,
                            content_digest: stored.revision.content_digest,
                        })?
                    }
                    ReleaseChangeAction::PromoteStable
                    | ReleaseChangeAction::PublishInitialStable => {
                        self.agents.record_release_assignment(&ReleaseAssignment {
                            scope: revision_ref.definition_id.scope(),
                            revision_ref: revision_ref.clone(),
                            channel: ReleaseChannel::Stable,
                            status: ReleaseAssignmentStatus::Active,
                            authorization: authorization.clone(),
                            content_digest: stored.revision.content_digest,
                        })?;
                        if let Err(error) =
                            self.agents.set_default_pointer(&DefaultPointer::latest(
                                revision_ref.definition_id.scope(),
                                revision_ref.definition_id.clone(),
                                authorization,
                            ))
                        {
                            if !matches!(error, DefinitionStoreError::ManualPinProtected) {
                                return Err(error.into());
                            }
                        }
                    }
                    ReleaseChangeAction::StopCanary => {
                        self.agents.record_release_assignment(&ReleaseAssignment {
                            scope: revision_ref.definition_id.scope(),
                            revision_ref: revision_ref.clone(),
                            channel: ReleaseChannel::Canary,
                            status: ReleaseAssignmentStatus::Stopped,
                            authorization,
                            content_digest: stored.revision.content_digest,
                        })?
                    }
                    ReleaseChangeAction::SetDefaultLatest => {
                        self.agents.set_default_pointer(&DefaultPointer::latest(
                            revision_ref.definition_id.scope(),
                            revision_ref.definition_id.clone(),
                            authorization,
                        ))?
                    }
                    ReleaseChangeAction::SetDefaultExact | ReleaseChangeAction::Rollback => {
                        self.agents.set_default_pointer(&DefaultPointer {
                            scope: revision_ref.definition_id.scope(),
                            definition_id: revision_ref.definition_id.clone(),
                            selector: assignment.selector.clone().ok_or_else(|| {
                                DefinitionRegistryError::Agent(
                                    DefinitionStoreError::UnresolvablePointer(
                                        revision_ref.definition_id.clone(),
                                        "release assignment requires an exact selector".to_string(),
                                    ),
                                )
                            })?,
                            authorization,
                        })?
                    }
                }
            }
            EvolutionCandidateSubject::TeamTemplate { revision_ref } => {
                let stored = self.teams.read_revision(revision_ref)?;
                let authorization = ReleaseAuthorization::HumanApproval {
                    approval_ref: assignment.approval_ref.clone(),
                };
                match assignment.action {
                    ReleaseChangeAction::PromoteCanary => {
                        self.teams
                            .record_release_assignment(&TeamReleaseAssignment {
                                scope: revision_ref.template_id.scope(),
                                revision_ref: revision_ref.clone(),
                                channel: ReleaseChannel::Canary,
                                status: ReleaseAssignmentStatus::Active,
                                authorization,
                                content_digest: stored.revision.content_digest,
                            })?
                    }
                    ReleaseChangeAction::PromoteStable
                    | ReleaseChangeAction::PublishInitialStable => {
                        self.teams
                            .record_release_assignment(&TeamReleaseAssignment {
                                scope: revision_ref.template_id.scope(),
                                revision_ref: revision_ref.clone(),
                                channel: ReleaseChannel::Stable,
                                status: ReleaseAssignmentStatus::Active,
                                authorization: authorization.clone(),
                                content_digest: stored.revision.content_digest,
                            })?;
                        if let Err(error) =
                            self.teams.set_default_pointer(&TeamDefaultPointer::latest(
                                revision_ref.template_id.scope(),
                                revision_ref.template_id.clone(),
                                authorization,
                            ))
                        {
                            if !matches!(error, TeamDefinitionStoreError::ManualPinProtected) {
                                return Err(error.into());
                            }
                        }
                    }
                    ReleaseChangeAction::StopCanary => {
                        self.teams
                            .record_release_assignment(&TeamReleaseAssignment {
                                scope: revision_ref.template_id.scope(),
                                revision_ref: revision_ref.clone(),
                                channel: ReleaseChannel::Canary,
                                status: ReleaseAssignmentStatus::Stopped,
                                authorization,
                                content_digest: stored.revision.content_digest,
                            })?
                    }
                    ReleaseChangeAction::SetDefaultLatest => {
                        self.teams.set_default_pointer(&TeamDefaultPointer::latest(
                            revision_ref.template_id.scope(),
                            revision_ref.template_id.clone(),
                            authorization,
                        ))?
                    }
                    ReleaseChangeAction::SetDefaultExact | ReleaseChangeAction::Rollback => {
                        self.teams.set_default_pointer(&TeamDefaultPointer {
                            scope: revision_ref.template_id.scope(),
                            template_id: revision_ref.template_id.clone(),
                            selector: assignment.selector.clone().ok_or_else(|| {
                                DefinitionRegistryError::Team(
                                    TeamDefinitionStoreError::UnresolvablePointer(
                                        revision_ref.template_id.clone(),
                                        "release assignment requires an exact selector".to_string(),
                                    ),
                                )
                            })?,
                            authorization,
                        })?
                    }
                }
            }
        }
        Ok(())
    }
}

fn agent_catalog_entry(
    revision: &harness_contract::agent::AgentDefinitionRevision,
) -> AgentCatalogEntry {
    let manifest = &revision.manifest;
    AgentCatalogEntry {
        definition_ref: AgentDefinitionRevisionRef {
            definition_id: manifest.definition_id.clone(),
            revision: manifest.revision,
        },
        agent_id: manifest.definition_id.as_str().to_string(),
        name: manifest.name.clone(),
        description: manifest.description.clone(),
        capabilities: manifest
            .capability_contract
            .capability_ceiling
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect(),
        scope: manifest.definition_id.scope(),
        evaluation: manifest.evaluation.clone(),
    }
}

impl ExactAgentRevisionResolver for RuntimeDefinitionRegistry {
    fn ensure_exact_approved_revision(
        &self,
        definition_id: &AgentDefinitionId,
        revision: u64,
    ) -> Result<(), String> {
        self.agents
            .ensure_eligible_revision(definition_id, revision)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::agent::{
        AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionManifest,
        AgentEvaluationContract, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
        CognitiveReadScope, CognitiveWriteMode, DefinitionScope, ReleaseAssignment,
        ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    };
    use harness_contract::team::{
        RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract, TeamRoleDefinition,
        TeamRoleTaskContract, TeamTemplateManifest, TeamTopologyContract,
    };

    use super::*;
    use crate::team_definition::TeamReleaseAssignment;

    fn digest(value: &str) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    fn registry() -> (tempfile::TempDir, RuntimeDefinitionRegistry) {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let storage =
            storage::StorageLayout::default_for_config_home(temporary.path().join("user"));
        let registry = RuntimeDefinitionRegistry::from_storage_layout(
            &storage,
            temporary.path().join("bundle/definitions"),
            temporary.path().join("workspace"),
        )
        .expect("registry");
        (temporary, registry)
    }

    fn publish_reviewer(registry: &RuntimeDefinitionRegistry) -> AgentDefinitionId {
        let instructions = "# Reviewer\n\nReview evidence.\n";
        let definition_id = AgentDefinitionId::new(DefinitionScope::Workspace, "cowd/reviewer")
            .expect("definition id");
        let stored = registry
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
                        skill_refs: vec![],
                        approval_required_for: vec![],
                    },
                    output_contract: AgentOutputContract::reviewable(),
                    evaluation: AgentEvaluationContract::single_release_gate("review", "evidence"),
                    instructions_digest: digest(instructions),
                },
                instructions,
            )
            .expect("stored agent");
        registry
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/reviewer-v1".to_string(),
                },
                content_digest: stored.revision.content_digest,
            })
            .expect("agent release");
        definition_id
    }

    fn publish_team(
        registry: &RuntimeDefinitionRegistry,
        reviewer: AgentDefinitionId,
    ) -> TeamTemplateDefinitionId {
        let instructions = "# Review team\n\nReview and synthesize.\n";
        let template_id =
            TeamTemplateDefinitionId::new(DefinitionScope::Workspace, "cowd/review-team")
                .expect("team id");
        let stored = registry
            .teams()
            .store_revision(
                TeamTemplateManifest {
                    api_version: "cowd.team/v1".to_string(),
                    template_id: template_id.clone(),
                    revision: 1,
                    name: "Review team".to_string(),
                    lifecycle: RevisionLifecycle::Published,
                    topology: TeamTopologyContract {
                        protocol_ref: "review_fix@1".to_string(),
                        require_synthesis: true,
                        require_review: true,
                    },
                    roles: vec![TeamRoleDefinition {
                        role_id: "reviewer".to_string(),
                        responsibility: "Review implementation evidence".to_string(),
                        agent_definition_id: reviewer,
                        agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                        cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                        partition: RolePartitionPolicy::Single,
                        grant_ceiling: vec![AgentCapability::Read],
                        task_contract: TeamRoleTaskContract {
                            contract_ref: "task/review@1".to_string(),
                            acceptance: vec!["evidence".to_string()],
                        },
                    }],
                    dependencies: vec![],
                    result_contract: TeamResultContract {
                        required_fields: vec!["summary".to_string(), "evidence".to_string()],
                        evidence_required: true,
                        synthesis_required: true,
                    },
                    evaluation: harness_contract::team::TeamEvaluationContract::single_release_gate(
                        "team/review",
                        "team_interoperability",
                    ),
                    instructions_digest: digest(instructions),
                },
                instructions,
            )
            .expect("stored team");
        registry
            .teams()
            .record_release_assignment(&TeamReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/team-v1".to_string(),
                },
                content_digest: stored.revision.content_digest,
            })
            .expect("team release");
        template_id
    }

    #[test]
    fn agent_and_team_revisions_use_separate_subtrees_under_one_registered_root() {
        let (temporary, registry) = registry();
        let reviewer = publish_reviewer(&registry);
        let template = publish_team(&registry, reviewer.clone());

        assert!(temporary
            .path()
            .join("workspace/.cowd/definitions/agents/cowd/reviewer/revisions/1/agent.yaml")
            .is_file());
        assert!(temporary
            .path()
            .join("workspace/.cowd/definitions/teams/cowd/review-team/revisions/1/team.yaml")
            .is_file());
        assert!(registry
            .resolve_agent(&reviewer, RevisionSelector::LatestApprovedStable)
            .is_ok());
        assert!(registry
            .resolve_team(&template, RevisionSelector::LatestApprovedStable)
            .is_ok());
    }

    #[test]
    fn runnable_catalog_exposes_exact_definition_and_removes_stopped_release() {
        let (_temporary, registry) = registry();
        let reviewer = publish_reviewer(&registry);
        let catalog = registry.runnable_agent_catalog().expect("catalog");
        assert_eq!(catalog.len(), 4);
        let reviewer_entry = catalog
            .iter()
            .find(|entry| entry.agent_id == reviewer.as_str())
            .expect("workspace reviewer in catalog");
        assert_eq!(reviewer_entry.definition_ref.definition_id, reviewer);
        assert_eq!(reviewer_entry.definition_ref.revision, 1);
        assert_eq!(reviewer_entry.capabilities, vec!["read"]);
        assert_eq!(reviewer_entry.evaluation.scenario_refs, vec!["review"]);

        let stored = registry
            .agents()
            .read_revision(&reviewer_entry.definition_ref)
            .expect("stored reviewer");
        registry
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
            .expect("stopped release");

        let catalog_after_stop = registry
            .runnable_agent_catalog()
            .expect("catalog after stop");
        assert_eq!(catalog_after_stop.len(), 3);
        assert!(catalog_after_stop
            .iter()
            .all(|entry| entry.agent_id != reviewer.as_str()));
    }

    #[test]
    fn fresh_registry_resolves_embedded_agent_and_team_primitives() {
        let (_temporary, registry) = registry();
        let execute = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute")
            .expect("builtin agent id");
        let team = TeamTemplateDefinitionId::new(DefinitionScope::Builtin, "cowd/execute-review")
            .expect("builtin team id");
        assert!(registry
            .resolve_agent(&execute, RevisionSelector::LatestApprovedStable)
            .is_ok());
        assert!(registry
            .resolve_team(&team, RevisionSelector::LatestApprovedStable)
            .is_ok());
        let teams = registry.runnable_team_catalog().expect("team catalog");
        assert!(
            teams.len() >= 8,
            "builtin Team catalog must expose all standard templates"
        );
        let execute_review = teams
            .iter()
            .find(|entry| entry.revision_ref.template_id == team)
            .expect("execute-review builtin Team template");
        assert_eq!(execute_review.revision_ref.revision, 2);
    }

    #[test]
    fn team_resolution_fails_after_its_exact_agent_release_is_stopped() {
        let (_temporary, registry) = registry();
        let reviewer = publish_reviewer(&registry);
        let template = publish_team(&registry, reviewer.clone());
        let stored = registry
            .agents()
            .read_revision(
                &harness_contract::agent::AgentDefinitionRevisionRef::new(reviewer, 1)
                    .expect("revision ref"),
            )
            .expect("stored reviewer");
        registry
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
            .expect("stopped release");

        assert!(registry
            .resolve_team(&template, RevisionSelector::LatestApprovedStable)
            .is_err());
    }
}
