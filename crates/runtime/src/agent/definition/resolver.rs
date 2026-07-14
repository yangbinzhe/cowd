use harness_contract::agent::{
    AgentDefinitionId, AgentDefinitionRevision, DefaultPointer, RevisionSelector,
};

use super::store::{
    AgentDefinitionStore, DefinitionStorageLayout, DefinitionStoreError,
    StoredAgentDefinitionRevision,
};

/// A fully verified, runnable Definition resolution.  It carries the exact
/// revision rather than an unqualified agent name, preventing scope shadowing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAgentDefinition {
    pub revision: AgentDefinitionRevision,
    pub agent_markdown: String,
    pub selected_by: RevisionSelector,
}

/// Resolves only explicit qualified Definition IDs and only Stable assignments
/// that satisfy the scope's approval/attestation truth rule.
#[derive(Debug)]
pub struct AgentDefinitionResolver<'a, L> {
    store: &'a AgentDefinitionStore<L>,
}

impl<'a, L> AgentDefinitionResolver<'a, L>
where
    L: DefinitionStorageLayout,
{
    #[must_use]
    pub fn new(store: &'a AgentDefinitionStore<L>) -> Self {
        Self { store }
    }

    pub fn resolve(
        &self,
        definition_id: &AgentDefinitionId,
        selector: RevisionSelector,
    ) -> Result<ResolvedAgentDefinition, DefinitionStoreError> {
        let selected_by = selector.clone();
        let stored = match selector {
            RevisionSelector::LatestApprovedStable => {
                self.store.latest_eligible_revision(definition_id)?
            }
            RevisionSelector::ExactApprovedRevision { revision } => self
                .store
                .ensure_eligible_revision(definition_id, revision)?,
            RevisionSelector::DefaultPointer => {
                let pointer = self.store.default_pointer(definition_id)?;
                self.resolve_pointer(pointer)?
            }
        };
        Ok(resolved(stored, selected_by))
    }

    pub fn resolve_default(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<ResolvedAgentDefinition, DefinitionStoreError> {
        self.resolve(definition_id, RevisionSelector::DefaultPointer)
    }

    fn resolve_pointer(
        &self,
        pointer: DefaultPointer,
    ) -> Result<StoredAgentDefinitionRevision, DefinitionStoreError> {
        match pointer.selector {
            RevisionSelector::LatestApprovedStable => {
                self.store.latest_eligible_revision(&pointer.definition_id)
            }
            RevisionSelector::ExactApprovedRevision { revision } => self
                .store
                .ensure_eligible_revision(&pointer.definition_id, revision),
            RevisionSelector::DefaultPointer => Err(DefinitionStoreError::UnresolvablePointer(
                pointer.definition_id,
                "a default pointer cannot recursively target another default pointer".to_string(),
            )),
        }
    }
}

fn resolved(
    stored: StoredAgentDefinitionRevision,
    selected_by: RevisionSelector,
) -> ResolvedAgentDefinition {
    ResolvedAgentDefinition {
        revision: stored.revision,
        agent_markdown: stored.agent_markdown,
        selected_by,
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::agent::{
        AgentDefinitionId, DefaultPointer, DefinitionScope, ReleaseAssignment,
        ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
        RevisionSelector,
    };
    use tempfile::TempDir;

    use super::super::store::tests_support::{manifest, markdown, store};
    use super::*;

    fn publish(
        store: &AgentDefinitionStore<super::super::store::ScopedDefinitionLayout>,
        revision: u64,
    ) -> AgentDefinitionId {
        let stored = store
            .store_revision(
                manifest(
                    DefinitionScope::User,
                    revision,
                    RevisionLifecycle::Published,
                ),
                markdown(),
            )
            .unwrap();
        store
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::User,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: format!("approval/{revision}"),
                },
                content_digest: stored.revision.content_digest,
            })
            .unwrap();
        stored.revision.revision_ref.definition_id
    }

    #[test]
    fn latest_selects_largest_active_approved_stable_and_exact_pin_survives_newer_stable() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let id = publish(&store, 1);
        publish(&store, 2);
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
                scope: DefinitionScope::User,
                definition_id: id.clone(),
                selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/pin-1".to_string(),
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
        assert!(matches!(
            store.set_default_pointer(&DefaultPointer::latest(
                DefinitionScope::User,
                id,
                ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/latest".to_string(),
                },
            )),
            Err(DefinitionStoreError::ManualPinProtected)
        ));
    }

    #[test]
    fn draft_and_unreleased_revisions_never_resolve() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::User, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        let resolver = AgentDefinitionResolver::new(&store);
        assert!(matches!(
            resolver.resolve(
                &stored.revision.revision_ref.definition_id,
                RevisionSelector::ExactApprovedRevision { revision: 1 }
            ),
            Err(DefinitionStoreError::UnresolvablePointer(_, _))
        ));
    }

    #[test]
    fn stopped_release_is_removed_from_latest_resolution() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let id = publish(&store, 1);
        let stored = store
            .read_revision(
                &harness_contract::agent::AgentDefinitionRevisionRef::new(id.clone(), 1).unwrap(),
            )
            .unwrap();
        store
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::User,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Stopped,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/stopped".to_string(),
                },
                content_digest: stored.revision.content_digest,
            })
            .unwrap();
        let resolver = AgentDefinitionResolver::new(&store);
        assert!(matches!(
            resolver.resolve(&id, RevisionSelector::LatestApprovedStable),
            Err(DefinitionStoreError::UnresolvablePointer(_, _))
        ));
    }
}
