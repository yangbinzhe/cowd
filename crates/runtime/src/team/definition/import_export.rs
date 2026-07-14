//! Explicit import and export operations for Team Template assets.

use harness_contract::agent::RevisionLifecycle;
use harness_contract::team::{TeamTemplateManifest, TeamTemplateRevisionRef};

use super::store::{
    StoredTeamTemplateRevision, TeamDefinitionStoreError, TeamTemplateDefinitionStore,
    TeamTemplateStorageLayout,
};
use super::validation::{digest_hex, normalize_team_markdown};

/// A third-party template import.  Source release and pointer state is never
/// trusted; every imported revision becomes a local Draft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DraftTeamTemplateImport {
    pub manifest: TeamTemplateManifest,
    pub team_markdown: String,
    pub source_label: String,
}

impl DraftTeamTemplateImport {
    pub fn validate(&self) -> Result<(), TeamDefinitionStoreError> {
        if self.source_label.trim().is_empty() || self.source_label.contains('\0') {
            return Err(TeamDefinitionStoreError::InvalidImport(
                "source_label must be non-empty and cannot contain NUL bytes".to_string(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn into_draft_manifest(mut self) -> TeamTemplateManifest {
        self.manifest.lifecycle = RevisionLifecycle::Draft;
        self.manifest.instructions_digest =
            digest_hex(normalize_team_markdown(&self.team_markdown).as_bytes());
        self.manifest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamTemplateExport {
    pub manifest: TeamTemplateManifest,
    pub team_markdown: String,
    pub content_digest: String,
}

impl<L> TeamTemplateDefinitionStore<L>
where
    L: TeamTemplateStorageLayout,
{
    pub fn import_draft(
        &self,
        import: DraftTeamTemplateImport,
    ) -> Result<StoredTeamTemplateRevision, TeamDefinitionStoreError> {
        import.validate()?;
        let markdown = import.team_markdown.clone();
        self.store_revision(import.into_draft_manifest(), &markdown)
    }

    pub fn export_revision(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
    ) -> Result<TeamTemplateExport, TeamDefinitionStoreError> {
        let stored = self.read_revision(revision_ref)?;
        Ok(TeamTemplateExport {
            manifest: stored.revision.manifest,
            team_markdown: stored.team_markdown,
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
    fn third_party_import_is_always_draft_without_release_state() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .import_draft(DraftTeamTemplateImport {
                manifest: manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Published),
                team_markdown: markdown().to_string(),
                source_label: "external-template-pack".to_string(),
            })
            .unwrap();
        assert_eq!(stored.revision.manifest.lifecycle, RevisionLifecycle::Draft);
        assert!(store
            .release_assignments(&stored.revision.revision_ref.template_id)
            .unwrap()
            .is_empty());
    }
}
