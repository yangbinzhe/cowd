use harness_contract::agent::{AgentDefinitionId, RevisionSelector};
use harness_contract::team::{TeamTemplateDefinitionId, TeamTemplateRevision};

use super::store::{
    StoredTeamTemplateRevision, TeamDefaultPointer, TeamDefinitionStoreError,
    TeamTemplateDefinitionStore, TeamTemplateStorageLayout,
};

/// Runtime composition supplies this narrow bridge to the Agent Definition
/// resolver.  It prevents a Team revision from becoming runnable when a role
/// pins an agent revision that has since been revoked, stopped, or superseded.
pub trait ExactAgentRevisionResolver: Send + Sync {
    fn ensure_exact_approved_revision(
        &self,
        definition_id: &AgentDefinitionId,
        revision: u64,
    ) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTeamTemplate {
    pub revision: TeamTemplateRevision,
    pub team_markdown: String,
    pub selected_by: RevisionSelector,
}

#[derive(Debug)]
pub struct TeamTemplateDefinitionResolver<'a, L> {
    store: &'a TeamTemplateDefinitionStore<L>,
}

impl<'a, L> TeamTemplateDefinitionResolver<'a, L>
where
    L: TeamTemplateStorageLayout,
{
    #[must_use]
    pub fn new(store: &'a TeamTemplateDefinitionStore<L>) -> Self {
        Self { store }
    }

    pub fn resolve(
        &self,
        template_id: &TeamTemplateDefinitionId,
        selector: RevisionSelector,
        agents: &dyn ExactAgentRevisionResolver,
    ) -> Result<ResolvedTeamTemplate, TeamDefinitionStoreError> {
        let selected_by = selector.clone();
        let stored = match selector {
            RevisionSelector::LatestApprovedStable => {
                self.store.latest_eligible_revision(template_id)?
            }
            RevisionSelector::ExactApprovedRevision { revision } => {
                self.store.ensure_eligible_revision(template_id, revision)?
            }
            RevisionSelector::DefaultPointer => {
                self.resolve_pointer(self.store.default_pointer(template_id)?)?
            }
        };
        validate_role_agent_bindings(&stored, agents)?;
        Ok(ResolvedTeamTemplate {
            revision: stored.revision,
            team_markdown: stored.team_markdown,
            selected_by,
        })
    }

    pub fn resolve_default(
        &self,
        template_id: &TeamTemplateDefinitionId,
        agents: &dyn ExactAgentRevisionResolver,
    ) -> Result<ResolvedTeamTemplate, TeamDefinitionStoreError> {
        self.resolve(template_id, RevisionSelector::DefaultPointer, agents)
    }

    fn resolve_pointer(
        &self,
        pointer: TeamDefaultPointer,
    ) -> Result<StoredTeamTemplateRevision, TeamDefinitionStoreError> {
        match pointer.selector {
            RevisionSelector::LatestApprovedStable => {
                self.store.latest_eligible_revision(&pointer.template_id)
            }
            RevisionSelector::ExactApprovedRevision { revision } => self
                .store
                .ensure_eligible_revision(&pointer.template_id, revision),
            RevisionSelector::DefaultPointer => Err(TeamDefinitionStoreError::UnresolvablePointer(
                pointer.template_id,
                "a default pointer cannot recursively target another default pointer".to_string(),
            )),
        }
    }
}

fn validate_role_agent_bindings(
    stored: &StoredTeamTemplateRevision,
    agents: &dyn ExactAgentRevisionResolver,
) -> Result<(), TeamDefinitionStoreError> {
    for role in &stored.revision.manifest.roles {
        let RevisionSelector::ExactApprovedRevision { revision } = role.agent_selector else {
            return Err(TeamDefinitionStoreError::UnresolvablePointer(
                stored.revision.revision_ref.template_id.clone(),
                format!(
                    "role `{}` does not pin an exact approved agent revision",
                    role.role_id
                ),
            ));
        };
        agents
            .ensure_exact_approved_revision(&role.agent_definition_id, revision)
            .map_err(|reason| {
                TeamDefinitionStoreError::UnresolvablePointer(
                    stored.revision.revision_ref.template_id.clone(),
                    format!(
                        "role `{}` agent binding is not eligible: {reason}",
                        role.role_id
                    ),
                )
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use harness_contract::agent::{
        DefinitionScope, ReleaseAssignmentStatus, ReleaseChannel, RevisionLifecycle,
    };
    use tempfile::TempDir;

    use super::super::store::tests_support::{manifest, markdown, release_authorization, store};
    use super::super::{TeamDefaultPointer, TeamReleaseAssignment};
    use super::*;

    #[derive(Default)]
    struct EligibleAgents(BTreeSet<(String, u64)>);

    impl ExactAgentRevisionResolver for EligibleAgents {
        fn ensure_exact_approved_revision(
            &self,
            definition_id: &AgentDefinitionId,
            revision: u64,
        ) -> Result<(), String> {
            self.0
                .contains(&(definition_id.as_str().to_string(), revision))
                .then_some(())
                .ok_or_else(|| "agent revision is not an active approved stable".to_string())
        }
    }

    fn publish(
        store: &TeamTemplateDefinitionStore<super::super::store::ScopedTeamTemplateLayout>,
        revision: u64,
    ) -> TeamTemplateDefinitionId {
        publish_scoped(store, DefinitionScope::Workspace, revision)
    }

    fn publish_scoped(
        store: &TeamTemplateDefinitionStore<super::super::store::ScopedTeamTemplateLayout>,
        scope: DefinitionScope,
        revision: u64,
    ) -> TeamTemplateDefinitionId {
        let stored = store
            .store_revision(
                manifest(scope, revision, RevisionLifecycle::Published),
                markdown(),
            )
            .unwrap();
        store
            .record_release_assignment(&TeamReleaseAssignment {
                scope,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: release_authorization(scope),
                content_digest: stored.revision.content_digest,
            })
            .unwrap();
        stored.revision.revision_ref.template_id
    }

    fn agents() -> EligibleAgents {
        let mut agents = EligibleAgents::default();
        agents.0.insert(("workspace/cowd/reviewer".to_string(), 1));
        agents
    }

    fn builtin_agents() -> EligibleAgents {
        let mut agents = EligibleAgents::default();
        agents
            .0
            .insert(("builtin/cowd/reviewer".to_string(), 1));
        agents
    }

    #[test]
    fn latest_selects_largest_eligible_stable_and_manual_pin_survives() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let id = publish(&store, 1);
        publish(&store, 2);
        let resolver = TeamTemplateDefinitionResolver::new(&store);
        assert_eq!(
            resolver
                .resolve(&id, RevisionSelector::LatestApprovedStable, &agents())
                .unwrap()
                .revision
                .revision_ref
                .revision,
            2
        );
        store
            .set_default_pointer(&TeamDefaultPointer {
                scope: DefinitionScope::Workspace,
                template_id: id.clone(),
                selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                authorization: release_authorization(DefinitionScope::Workspace),
            })
            .unwrap();
        assert_eq!(
            resolver
                .resolve_default(&id, &agents())
                .unwrap()
                .revision
                .revision_ref
                .revision,
            1
        );
        assert!(matches!(
            store.set_default_pointer(&TeamDefaultPointer::latest(
                DefinitionScope::Workspace,
                id,
                release_authorization(DefinitionScope::Workspace)
            )),
            Err(TeamDefinitionStoreError::ManualPinProtected)
        ));
    }

    #[test]
    fn latest_approved_stable_default_pointer_resolves_to_latest_eligible_revision() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let id = publish(&store, 1);
        publish(&store, 2);
        let resolver = TeamTemplateDefinitionResolver::new(&store);
        store
            .set_default_pointer(&TeamDefaultPointer::latest(
                DefinitionScope::Workspace,
                id.clone(),
                release_authorization(DefinitionScope::Workspace),
            ))
            .unwrap();
        assert_eq!(
            resolver
                .resolve_default(&id, &agents())
                .unwrap()
                .revision
                .revision_ref
                .revision,
            2,
            "LatestApprovedStable default pointer must resolve to the latest eligible revision"
        );
    }

    #[test]
    fn bootstrapped_builtin_team_resolves_latest_approved_stable_through_default_pointer() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let id = publish_scoped(&store, DefinitionScope::Builtin, 1);
        publish_scoped(&store, DefinitionScope::Builtin, 2);
        let resolver = TeamTemplateDefinitionResolver::new(&store);
        store
            .set_default_pointer(&TeamDefaultPointer::latest(
                DefinitionScope::Builtin,
                id.clone(),
                release_authorization(DefinitionScope::Builtin),
            ))
            .unwrap();
        let pointer = store.default_pointer(&id).unwrap();
        assert_eq!(
            pointer.selector,
            RevisionSelector::LatestApprovedStable,
            "default pointer selector must be LatestApprovedStable"
        );
        assert_eq!(
            resolver
                .resolve(&id, RevisionSelector::LatestApprovedStable, &builtin_agents())
                .unwrap()
                .revision
                .revision_ref
                .revision,
            2,
            "direct LatestApprovedStable resolution must find the latest eligible revision"
        );
        let resolved = resolver
            .resolve_default(&id, &builtin_agents())
            .unwrap();
        assert_eq!(
            resolved.revision.revision_ref.revision,
            2,
            "LatestApprovedStable default pointer must resolve to the latest eligible revision"
        );
        assert_eq!(
            resolved.selected_by,
            RevisionSelector::DefaultPointer,
            "selected_by reflects the top-level selector passed to resolve"
        );
    }

    #[test]
    fn draft_stopped_or_agent_ineligible_team_never_resolves() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let draft = store
            .store_revision(
                manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        let resolver = TeamTemplateDefinitionResolver::new(&store);
        assert!(matches!(
            resolver.resolve(
                &draft.revision.revision_ref.template_id,
                RevisionSelector::ExactApprovedRevision { revision: 1 },
                &agents()
            ),
            Err(TeamDefinitionStoreError::UnresolvablePointer(_, _))
        ));
        let id = publish(&store, 2);
        assert!(matches!(
            resolver.resolve(
                &id,
                RevisionSelector::LatestApprovedStable,
                &EligibleAgents::default()
            ),
            Err(TeamDefinitionStoreError::UnresolvablePointer(_, _))
        ));
    }
}
