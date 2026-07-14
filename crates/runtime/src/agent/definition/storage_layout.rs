//! Runtime composition adapter for registered Agent Definition storage roots.
//!
//! Definition persistence never discovers a configuration home from the
//! process environment. The composition root supplies every scope explicitly:
//! user scope comes from the registered `storage::StorageLayout`, workspace
//! scope comes from the bound workspace, and builtin scope comes from the
//! verified installation bundle selected by the caller.

use std::path::{Path, PathBuf};

use harness_contract::agent::DefinitionScope;
use storage::StorageLayout;

use super::store::{DefinitionStorageLayout, DefinitionStoreError};

const DEFINITIONS_DOMAIN: &str = "definitions";

/// Registered roots for the three non-shadowing Agent Definition scopes.
///
/// The builtin root is deliberately not derived from a user-configurable
/// storage directory: the runtime composition root must supply the verified
/// release bundle root. Workspace definitions remain under the workspace's
/// own control plane and cannot overwrite user or builtin assets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredAgentDefinitionLayout {
    builtin_root: PathBuf,
    user_root: PathBuf,
    workspace_root: PathBuf,
}

impl RegisteredAgentDefinitionLayout {
    /// Compose roots from registered user storage plus explicit installation
    /// and workspace boundaries.
    pub fn from_storage_layout(
        layout: &StorageLayout,
        builtin_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<Self, DefinitionStoreError> {
        let user_root = layout
            .directory_path(DEFINITIONS_DOMAIN)
            .ok_or_else(|| DefinitionStoreError::UnregisteredStorageRoot {
                domain: DEFINITIONS_DOMAIN.to_string(),
            })?
            .to_path_buf();
        Self::new(builtin_root, user_root, workspace_root)
    }

    /// Construct from already registered roots. `workspace_root` is the
    /// workspace itself, not an arbitrary definitions directory.
    pub fn new(
        builtin_root: impl Into<PathBuf>,
        user_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<Self, DefinitionStoreError> {
        let builtin_root = require_non_empty_root("builtin", builtin_root.into())?;
        let user_root = require_non_empty_root("user", user_root.into())?;
        let workspace_root = require_non_empty_root("workspace", workspace_root.into())?;
        Ok(Self {
            builtin_root,
            user_root,
            workspace_root,
        })
    }

    #[must_use]
    pub fn builtin_root(&self) -> &Path {
        &self.builtin_root
    }

    #[must_use]
    pub fn user_root(&self) -> &Path {
        &self.user_root
    }

    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    #[must_use]
    pub fn workspace_definition_root(&self) -> PathBuf {
        self.workspace_root.join(".cowd").join("definitions")
    }
}

impl DefinitionStorageLayout for RegisteredAgentDefinitionLayout {
    fn root_for_scope(&self, scope: DefinitionScope) -> Result<PathBuf, DefinitionStoreError> {
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
) -> Result<PathBuf, DefinitionStoreError> {
    if root.as_os_str().is_empty() {
        return Err(DefinitionStoreError::InvalidStorageRoot {
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
    fn scope_roots_are_explicit_and_do_not_shadow_each_other() {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let config_home = temporary.path().join("config");
        let workspace = temporary.path().join("workspace");
        let layout = StorageLayout::default_for_config_home(&config_home);
        let registered = RegisteredAgentDefinitionLayout::from_storage_layout(
            &layout,
            temporary.path().join("release-bundle/agents"),
            &workspace,
        )
        .expect("registered layout");

        assert_eq!(
            registered.root_for_scope(DefinitionScope::Builtin).unwrap(),
            temporary.path().join("release-bundle/agents")
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
    fn missing_registered_user_root_fails_closed() {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let mut layout = StorageLayout::default_for_config_home(temporary.path());
        layout.directories.remove(DEFINITIONS_DOMAIN);

        assert!(matches!(
            RegisteredAgentDefinitionLayout::from_storage_layout(
                &layout,
                temporary.path().join("builtin"),
                temporary.path().join("workspace"),
            ),
            Err(DefinitionStoreError::UnregisteredStorageRoot { .. })
        ));
    }

    #[test]
    fn empty_scope_root_is_rejected_before_storage_is_touched() {
        assert!(matches!(
            RegisteredAgentDefinitionLayout::new("", "/user", "/workspace"),
            Err(DefinitionStoreError::InvalidStorageRoot { .. })
        ));
    }
}
