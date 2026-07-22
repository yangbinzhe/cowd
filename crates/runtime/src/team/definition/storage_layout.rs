//! Runtime composition adapter for registered Team Template storage roots.
//!
//! Team assets follow the same explicit three-scope boundary as Agent
//! Definitions. User scope is sourced from the storage registry, builtin
//! scope is supplied by the verified installation bundle, and workspace scope
//! is bound to the concrete workspace rather than the process working
//! directory.

use std::path::PathBuf;

use harness_contract::agent::DefinitionScope;
use storage::{StorageDomainId, StorageRegistry, StorageScope};

use super::store::{TeamDefinitionStoreError, TeamTemplateStorageLayout};

const DEFINITIONS_DOMAIN: &str = "definitions";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredTeamTemplateLayout {
    builtin_root: PathBuf,
    user_root: PathBuf,
    workspace_root: PathBuf,
}

impl RegisteredTeamTemplateLayout {
    pub fn from_storage_registry(
        registry: &StorageRegistry,
        builtin_root: impl Into<PathBuf>,
        workspace_root: impl AsRef<std::path::Path>,
    ) -> Result<Self, TeamDefinitionStoreError> {
        let user_root = registry
            .endpoint(&StorageDomainId::Definitions)
            .map_err(|_| TeamDefinitionStoreError::UnregisteredStorageRoot {
                domain: DEFINITIONS_DOMAIN.to_string(),
            })?
            .as_handle()
            .path;
        let scope = StorageScope::workspace_for_root(workspace_root.as_ref());
        let workspace_definition_root = registry
            .endpoint_in_scope(&StorageDomainId::Definitions, &scope)
            .map_err(|_| TeamDefinitionStoreError::UnregisteredStorageRoot {
                domain: "definitions@workspace".to_string(),
            })?
            .as_handle()
            .path;
        Self::from_registered_roots(builtin_root, user_root, workspace_definition_root)
    }

    pub fn from_storage_layout(
        layout: &storage::StorageLayout,
        builtin_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<Self, TeamDefinitionStoreError> {
        let user_root = layout
            .directory_path(DEFINITIONS_DOMAIN)
            .ok_or_else(|| TeamDefinitionStoreError::UnregisteredStorageRoot {
                domain: DEFINITIONS_DOMAIN.to_string(),
            })?
            .to_path_buf();
        Self::new(builtin_root, user_root, workspace_root)
    }

    fn from_registered_roots(
        builtin_root: impl Into<PathBuf>,
        user_root: impl Into<PathBuf>,
        workspace_definition_root: impl Into<PathBuf>,
    ) -> Result<Self, TeamDefinitionStoreError> {
        Ok(Self {
            builtin_root: require_non_empty_root("builtin", builtin_root.into())?,
            user_root: require_non_empty_root("user", user_root.into())?,
            workspace_root: require_non_empty_root("workspace", workspace_definition_root.into())?,
        })
    }

    pub fn new(
        builtin_root: impl Into<PathBuf>,
        user_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<Self, TeamDefinitionStoreError> {
        Self::from_registered_roots(
            builtin_root,
            user_root,
            workspace_root.into().join(".cowd").join("definitions"),
        )
    }

    #[must_use]
    pub fn workspace_definition_root(&self) -> PathBuf {
        self.workspace_root.clone()
    }
}

impl TeamTemplateStorageLayout for RegisteredTeamTemplateLayout {
    fn root_for_scope(&self, scope: DefinitionScope) -> Result<PathBuf, TeamDefinitionStoreError> {
        Ok(match scope {
            DefinitionScope::Builtin => self.builtin_root.clone(),
            DefinitionScope::User => self.user_root.clone(),
            DefinitionScope::Workspace => self.workspace_definition_root(),
        })
    }
}

fn require_non_empty_root(
    scope: &'static str,
    root: PathBuf,
) -> Result<PathBuf, TeamDefinitionStoreError> {
    if root.as_os_str().is_empty() {
        return Err(TeamDefinitionStoreError::InvalidStorageRoot {
            scope: scope.to_string(),
            reason: "root cannot be empty".to_string(),
        });
    }
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_roots_are_registered_and_explicit() {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let config_home = temporary.path().join("config");
        let workspace = temporary.path().join("workspace");
        let registered = RegisteredTeamTemplateLayout::from_storage_layout(
            &StorageLayout::default_for_config_home(&config_home),
            temporary.path().join("release-bundle/teams"),
            &workspace,
        )
        .expect("registered layout");

        assert_eq!(
            registered.root_for_scope(DefinitionScope::Builtin).unwrap(),
            temporary.path().join("release-bundle/teams")
        );
        assert_eq!(
            registered.root_for_scope(DefinitionScope::User).unwrap(),
            config_home.join("definitions")
        );
        assert_eq!(
            registered
                .root_for_scope(DefinitionScope::Workspace)
                .unwrap(),
            workspace.join(".cowd/definitions")
        );
    }

    #[test]
    fn missing_user_directory_registration_fails_closed() {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let mut layout = StorageLayout::default_for_config_home(temporary.path());
        layout.directories.remove(DEFINITIONS_DOMAIN);
        assert!(matches!(
            RegisteredTeamTemplateLayout::from_storage_layout(
                &layout,
                temporary.path().join("builtin"),
                temporary.path().join("workspace"),
            ),
            Err(TeamDefinitionStoreError::UnregisteredStorageRoot { .. })
        ));
    }
}
