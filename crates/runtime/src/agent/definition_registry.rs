//! Runtime-owned registry for durable Agent Definition assets.
//!
//! This is the composition boundary between the generic storage registry and
//! executable Definition resolvers. It deliberately has no current-directory
//! discovery, no name shadowing, and no Gateway dependency.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::RwLock,
};

use harness_contract::agent::{
    AgentDefinitionId, AgentDefinitionRevisionRef, DefaultPointer, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionSelector,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::agent::definition::{
    bootstrap_builtin_agents, AgentDefinitionResolver, AgentDefinitionStore, BuiltinAgentTrust,
    DefinitionStoreError, ExplicitTomlAgentImport, RegisteredAgentDefinitionLayout,
    ResolvedAgentDefinition,
};
use crate::{
    AgentCatalogEntry, EvolutionCandidateSubject, EvolutionReleaseAssignment, ReleaseChangeAction,
};

/// Agent Definition composition failures.
#[derive(Debug, Error)]
pub enum DefinitionRegistryError {
    #[error(transparent)]
    Agent(#[from] DefinitionStoreError),
}

/// Agent catalog entry paired with the frozen content digest of its exact
/// published revision. Binding compilers consume this once and never re-read
/// a mutable latest definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenAgentCatalogEntry {
    pub entry: AgentCatalogEntry,
    pub content_digest: String,
}

/// Receipt for an explicitly imported Agent Definition. A draft receipt is
/// intentionally not a resolver result: draft revisions are not runnable and
/// must not be confused with an approved Binding candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDefinitionDraftReceipt {
    pub revision_ref: AgentDefinitionRevisionRef,
    pub content_digest: String,
}

/// Sole Runtime composition root for Agent Definition storage and resolution.
#[derive(Debug)]
pub struct RuntimeDefinitionRegistry {
    agents: AgentDefinitionStore<RegisteredAgentDefinitionLayout>,
    builtin_agent_trust: BuiltinAgentTrust,
    exact_agent_cache: RwLock<HashMap<(String, u64), ResolvedAgentDefinition>>,
}

impl RuntimeDefinitionRegistry {
    /// Production composition from resolved storage endpoints. Definition
    /// stores receive only their registered roots; they never derive config or
    /// workspace paths themselves.
    pub fn from_storage_registry(
        storage: &storage::StorageRegistry,
        builtin_definitions_root: impl Into<PathBuf>,
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, DefinitionRegistryError> {
        let agents =
            AgentDefinitionStore::new(RegisteredAgentDefinitionLayout::from_storage_registry(
                storage,
                builtin_definitions_root,
                workspace_root.as_ref(),
            )?);
        let builtin_agent_trust = bootstrap_builtin_agents(&agents)?;
        Ok(Self {
            agents,
            builtin_agent_trust,
            exact_agent_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Create the registry from a registered user storage layout and explicit
    /// builtin/workspace roots. `builtin_definitions_root` is the verified
    /// release bundle's definitions root, never a user-configurable path.
    pub fn from_storage_layout(
        storage: &storage::StorageLayout,
        builtin_definitions_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<Self, DefinitionRegistryError> {
        let agents =
            AgentDefinitionStore::new(RegisteredAgentDefinitionLayout::from_storage_layout(
                storage,
                builtin_definitions_root,
                workspace_root,
            )?);
        let builtin_agent_trust = bootstrap_builtin_agents(&agents)?;
        Ok(Self {
            agents,
            builtin_agent_trust,
            exact_agent_cache: RwLock::new(HashMap::new()),
        })
    }

    #[must_use]
    pub(crate) fn agents(&self) -> &AgentDefinitionStore<RegisteredAgentDefinitionLayout> {
        &self.agents
    }

    #[must_use]
    pub fn agent_resolver(&self) -> AgentDefinitionResolver<'_, RegisteredAgentDefinitionLayout> {
        AgentDefinitionResolver::new(self.agents())
    }

    pub fn resolve_agent(
        &self,
        definition_id: &AgentDefinitionId,
        selector: RevisionSelector,
    ) -> Result<ResolvedAgentDefinition, DefinitionRegistryError> {
        let exact_key = match &selector {
            RevisionSelector::ExactApprovedRevision { revision } => {
                Some((definition_id.as_str().to_string(), *revision))
            }
            RevisionSelector::LatestApprovedStable | RevisionSelector::DefaultPointer => None,
        };
        if let Some(cached) = exact_key.as_ref().and_then(|key| {
            self.exact_agent_cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .cloned()
        }) {
            return Ok(cached);
        }
        let resolved = self
            .agent_resolver()
            .resolve(definition_id, selector.clone())
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
        if let Some(key) = exact_key {
            self.exact_agent_cache
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key, resolved.clone());
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

    /// Rebuild the Runtime's runnable Agent catalog from immutable Definition
    /// revisions. Draft, revoked, stopped, quarantined, and corrupted
    /// Definitions never appear in this projection.
    pub fn runnable_agent_catalog(
        &self,
    ) -> Result<Vec<AgentCatalogEntry>, DefinitionRegistryError> {
        Ok(self
            .runnable_agent_catalog_frozen()?
            .into_iter()
            .map(|frozen| frozen.entry)
            .collect())
    }

    /// Rebuild the runnable Agent catalog with the frozen revision digest.
    /// The registry resolves each exact published revision exactly once per
    /// catalog build; Binding compilers use this carrier instead of re-reading
    /// or re-scanning Definition files.
    pub fn runnable_agent_catalog_frozen(
        &self,
    ) -> Result<Vec<FrozenAgentCatalogEntry>, DefinitionRegistryError> {
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
            entries.push(FrozenAgentCatalogEntry {
                entry: agent_catalog_entry(&resolved.revision),
                content_digest: resolved.revision.content_digest.clone(),
            });
        }
        entries.sort_by(|left, right| left.entry.agent_id.cmp(&right.entry.agent_id));
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
        // Eligibility and default pointers are mutable projections. Clear the
        // exact cache before applying a release transition so partial durable
        // success can never leave a stale runnable definition in memory.
        self.invalidate_definition_caches();
        let EvolutionCandidateSubject::AgentDefinition { revision_ref } = &assignment.subject;
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
            ReleaseChangeAction::PromoteStable => {
                self.agents.record_release_assignment(&ReleaseAssignment {
                    scope: revision_ref.definition_id.scope(),
                    revision_ref: revision_ref.clone(),
                    channel: ReleaseChannel::Stable,
                    status: ReleaseAssignmentStatus::Active,
                    authorization: authorization.clone(),
                    content_digest: stored.revision.content_digest,
                })?;
                if let Err(error) = self.agents.set_default_pointer(&DefaultPointer::latest(
                    revision_ref.definition_id.scope(),
                    revision_ref.definition_id.clone(),
                    authorization,
                )) {
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
                        DefinitionRegistryError::Agent(DefinitionStoreError::UnresolvablePointer(
                            revision_ref.definition_id.clone(),
                            "release assignment requires an exact selector".to_string(),
                        ))
                    })?,
                    authorization,
                })?
            }
        }
        Ok(())
    }

    fn invalidate_definition_caches(&self) {
        self.exact_agent_cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
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
        skill_refs: manifest.capability_contract.skill_refs.clone(),
        scope: manifest.definition_id.scope(),
        evaluation: manifest.evaluation.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent::{
        AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionManifest,
        AgentEvaluationContract, AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract,
        CognitiveReadScope, CognitiveWriteMode, DefinitionScope, ReleaseAssignment,
        ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    };

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

    #[test]
    fn runnable_catalog_exposes_exact_definition_and_removes_stopped_release() {
        let (_temporary, registry) = registry();
        let reviewer = publish_reviewer(&registry);
        let catalog = registry.runnable_agent_catalog().expect("catalog");
        let catalog_len = catalog.len();
        assert!(
            catalog
                .iter()
                .any(|entry| entry.agent_id == "builtin/cowd/autonomous"),
            "the cross-effect builtin must remain independently runnable"
        );
        let reviewer_entry = catalog
            .iter()
            .find(|entry| entry.agent_id == reviewer.as_str())
            .expect("workspace reviewer in catalog");
        assert_eq!(reviewer_entry.definition_ref.definition_id, reviewer);
        assert_eq!(reviewer_entry.definition_ref.revision, 1);
        assert_eq!(reviewer_entry.capabilities, vec!["read"]);
        assert_eq!(reviewer_entry.evaluation.scenario_refs, vec!["review"]);
        let frozen = registry
            .runnable_agent_catalog_frozen()
            .expect("frozen catalog");
        let frozen_reviewer = frozen
            .iter()
            .find(|frozen| frozen.entry.agent_id == reviewer.as_str())
            .expect("frozen reviewer entry");
        let stored = registry
            .agents()
            .read_revision(&reviewer_entry.definition_ref)
            .expect("stored reviewer");
        assert_eq!(
            frozen_reviewer.content_digest,
            stored.revision.content_digest
        );

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
        assert_eq!(catalog_after_stop.len(), catalog_len - 1);
        assert!(catalog_after_stop
            .iter()
            .all(|entry| entry.agent_id != reviewer.as_str()));
    }

    #[test]
    fn exact_approved_revisions_are_cached_and_release_changes_invalidate_the_cache() {
        let (_temporary, registry) = registry();
        let reviewer = publish_reviewer(&registry);

        registry
            .resolve_agent(
                &reviewer,
                RevisionSelector::ExactApprovedRevision { revision: 1 },
            )
            .expect("exact Agent revision");
        assert_eq!(
            registry
                .exact_agent_cache
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
        registry.invalidate_definition_caches();

        assert!(registry
            .exact_agent_cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }
}
