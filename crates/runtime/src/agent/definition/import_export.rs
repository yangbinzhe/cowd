//! Explicit import and export operations for Agent Definition assets.

use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentEvaluationContract, AgentExecutorPolicy, AgentModelPolicy,
    AgentOutputContract, CognitiveReadScope, CognitiveWriteMode, DefinitionScope,
    RevisionLifecycle,
};
use serde::Deserialize;

use super::store::{AgentDefinitionStore, DefinitionStoreError, StoredAgentDefinitionRevision};
use super::validation::normalize_agent_markdown;

/// A third-party import.  Imports never carry release state and always enter
/// the local store as `Draft`, even if the source manifest claims otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftAgentDefinitionImport {
    pub manifest: AgentDefinitionManifest,
    pub agent_markdown: String,
    pub source_label: String,
}

impl DraftAgentDefinitionImport {
    pub fn validate(&self) -> Result<(), DefinitionStoreError> {
        if self.source_label.trim().is_empty() {
            return Err(DefinitionStoreError::InvalidImport(
                "source_label cannot be blank".to_string(),
            ));
        }
        Ok(())
    }

    /// Remove source lifecycle intent before a persisted import.  Release
    /// assignments and default pointers are deliberately absent from this type.
    #[must_use]
    pub fn into_draft_manifest(mut self) -> AgentDefinitionManifest {
        self.manifest.lifecycle = RevisionLifecycle::Draft;
        self.manifest.instructions_digest = super::validation::digest_hex(
            normalize_agent_markdown(&self.agent_markdown).as_bytes(),
        );
        self.manifest
    }
}

/// Exported revision content.  Releases and pointers remain local authority
/// state and are never smuggled through an asset export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDefinitionExport {
    pub manifest: AgentDefinitionManifest,
    pub agent_markdown: String,
    pub content_digest: String,
}

/// Explicit third-party TOML import adapter. It accepts one caller-selected
/// source document; it never traverses `.codex`, `.claude`, ancestor folders,
/// or a home directory. Imported material receives a new qualified local ID
/// and conservative read-only capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplicitTomlAgentImport {
    pub definition_id: AgentDefinitionId,
    pub revision: u64,
    pub source_label: String,
    pub toml: String,
}

#[derive(Debug, Deserialize)]
struct ExternalTomlAgent {
    name: Option<String>,
    description: Option<String>,
    model: Option<String>,
    instructions: Option<String>,
}

impl ExplicitTomlAgentImport {
    pub fn into_draft(self) -> Result<DraftAgentDefinitionImport, DefinitionStoreError> {
        if matches!(self.definition_id.scope(), DefinitionScope::Builtin) {
            return Err(DefinitionStoreError::InvalidImport(
                "third-party imports cannot claim the builtin definition scope".to_string(),
            ));
        }
        if self.revision == 0 {
            return Err(DefinitionStoreError::InvalidImport(
                "import revision must be greater than zero".to_string(),
            ));
        }
        let parsed: ExternalTomlAgent = toml::from_str(&self.toml)
            .map_err(|error| DefinitionStoreError::InvalidImport(error.to_string()))?;
        let name = parsed.name.unwrap_or_else(|| {
            self.definition_id
                .as_str()
                .rsplit('/')
                .next()
                .unwrap_or("imported-agent")
                .to_string()
        });
        let description = parsed
            .description
            .unwrap_or_else(|| format!("Explicitly imported from {}", self.source_label));
        let instructions = parsed.instructions.unwrap_or_else(|| {
            format!(
                "# {name}\n\nImported from `{}`. Follow the current task contract and request additional capability explicitly.\n",
                self.source_label
            )
        });
        let mut allowed_models = parsed.model.into_iter().collect::<Vec<_>>();
        allowed_models.sort();
        allowed_models.dedup();
        Ok(DraftAgentDefinitionImport {
            manifest: AgentDefinitionManifest {
                api_version: "cowd.agent/v1".to_string(),
                definition_id: self.definition_id,
                revision: self.revision,
                name,
                description,
                lifecycle: RevisionLifecycle::Draft,
                executor: AgentExecutorPolicy::CowdNative,
                model_policy: AgentModelPolicy {
                    profile: "default".to_string(),
                    allowed_models,
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
                evaluation: AgentEvaluationContract::single_release_gate(
                    "import/manual-review",
                    "evidence",
                ),
                instructions_digest: String::new(),
            },
            agent_markdown: instructions,
            source_label: self.source_label,
        })
    }
}

impl<L> AgentDefinitionStore<L>
where
    L: super::store::DefinitionStorageLayout,
{
    pub fn import_draft(
        &self,
        import: DraftAgentDefinitionImport,
    ) -> Result<StoredAgentDefinitionRevision, DefinitionStoreError> {
        import.validate()?;
        let markdown = import.agent_markdown.clone();
        self.store_revision(import.into_draft_manifest(), &markdown)
    }

    pub fn export_revision(
        &self,
        revision_ref: &harness_contract::agent::AgentDefinitionRevisionRef,
    ) -> Result<AgentDefinitionExport, DefinitionStoreError> {
        let stored = self.read_revision(revision_ref)?;
        Ok(AgentDefinitionExport {
            manifest: stored.revision.manifest,
            agent_markdown: stored.agent_markdown,
            content_digest: stored.revision.content_digest,
        })
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::agent::{DefinitionScope, RevisionLifecycle};
    use tempfile::TempDir;

    use super::super::store::tests_support::{manifest, markdown, store};
    use super::*;

    #[test]
    fn third_party_import_is_always_draft_and_never_creates_release_state() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .import_draft(DraftAgentDefinitionImport {
                manifest: manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Published),
                agent_markdown: markdown().to_string(),
                source_label: "external-skill-bundle".to_string(),
            })
            .unwrap();
        assert_eq!(stored.revision.manifest.lifecycle, RevisionLifecycle::Draft);
        assert!(store
            .release_assignments(&stored.revision.revision_ref.definition_id)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn explicit_toml_import_never_scans_or_imports_as_builtin_or_published() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let imported = ExplicitTomlAgentImport {
            definition_id: AgentDefinitionId::new(DefinitionScope::Workspace, "external/reviewer")
                .unwrap(),
            revision: 1,
            source_label: "manual:/opt/agents/reviewer.toml".to_string(),
            toml: "name = 'External reviewer'\ndescription = 'Review source'\nmodel = 'fast'\n"
                .to_string(),
        };
        let stored = store.import_draft(imported.into_draft().unwrap()).unwrap();
        assert_eq!(stored.revision.manifest.lifecycle, RevisionLifecycle::Draft);
        assert_eq!(
            stored
                .revision
                .manifest
                .capability_contract
                .capability_ceiling,
            vec![AgentCapability::Read]
        );
        assert_eq!(
            stored.revision.manifest.model_policy.allowed_models,
            vec!["fast"]
        );
        assert!(store
            .release_assignments(&stored.revision.revision_ref.definition_id)
            .unwrap()
            .is_empty());
        assert!(ExplicitTomlAgentImport {
            definition_id: AgentDefinitionId::new(DefinitionScope::Builtin, "forbidden").unwrap(),
            revision: 1,
            source_label: "manual".to_string(),
            toml: "name = 'forbidden'".to_string(),
        }
        .into_draft()
        .is_err());
    }
}
