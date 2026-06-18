use std::path::{Path, PathBuf};

use runtime::{ExternalResourceRef, SqliteResourceDirectory};

use super::{ConnectorService, ServiceEnvelope};

impl ConnectorService {
    pub(crate) fn resource_list(&self) -> ServiceEnvelope {
        self.envelope("resource_list")
    }

    pub(crate) fn resource_revalidate(&self) -> ServiceEnvelope {
        self.envelope("resource_revalidate")
    }

    pub(crate) fn resource_promote_memory(&self) -> ServiceEnvelope {
        self.envelope("resource_promote_memory")
    }

    pub(crate) fn resource_directory(
        &self,
        workspace_root: impl AsRef<Path>,
    ) -> rusqlite::Result<SqliteResourceDirectory> {
        let handle = self.resource_directory_handle(workspace_root);
        if let Some(parent) = handle.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    error.kind(),
                    format!("failed to create resource directory parent: {error}"),
                )))
            })?;
        }
        SqliteResourceDirectory::open_storage_handle(&handle)
    }

    pub(crate) fn resource_directory_handle(
        &self,
        workspace_root: impl AsRef<Path>,
    ) -> storage::StorageHandle {
        let config_home = workspace_root.as_ref().join(".cowd");
        storage::StorageRegistry::default_for_config_home(config_home)
            .sqlite_handle("resource_directory")
            .cloned()
            .unwrap_or_else(|_| {
                storage::StorageHandle::sqlite(
                    "resource_directory",
                    self.resource_directory_path(workspace_root),
                    "connector",
                    "workspace_scoped_storage_handle_since_0.9.315",
                )
            })
    }

    pub(crate) fn resource_directory_path(&self, workspace_root: impl AsRef<Path>) -> PathBuf {
        workspace_root
            .as_ref()
            .join(".cowd")
            .join("storage")
            .join("resource-directory.sqlite")
    }

    pub(crate) fn list_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
        offset: usize,
        query: Option<&str>,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        let directory = self.resource_directory(workspace_root)?;
        query
            .map(|value| directory.search(value, limit))
            .unwrap_or_else(|| directory.list_page(limit, offset))
    }

    pub(crate) fn recent_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        limit: usize,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.list_recent(limit)
    }

    pub(crate) fn search_resources(
        &self,
        workspace_root: impl AsRef<Path>,
        query: &str,
        limit: usize,
    ) -> rusqlite::Result<Vec<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?
            .search(query, limit)
    }

    pub(crate) fn get_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
    ) -> rusqlite::Result<Option<ExternalResourceRef>> {
        self.resource_directory(workspace_root)?.get(reference)
    }

    pub(crate) fn upsert_resource(
        &self,
        workspace_root: impl AsRef<Path>,
        resource: &ExternalResourceRef,
    ) -> rusqlite::Result<()> {
        self.resource_directory(workspace_root)?
            .upsert(resource)
            .map(|_| ())
    }

    pub(crate) fn mark_resource_state(
        &self,
        workspace_root: impl AsRef<Path>,
        reference: &str,
        desired_state: &str,
    ) -> rusqlite::Result<(bool, Option<ExternalResourceRef>, Option<String>)> {
        let directory = self.resource_directory(workspace_root)?;
        let changed = match desired_state {
            "indexed" => directory.mark_indexed(reference)?,
            "stale" => directory.mark_stale(reference)?,
            other => return Ok((false, None, Some(format!("unsupported state: {other}")))),
        };
        let resource = directory.get(reference)?;
        Ok((changed, resource, None))
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.resource_list(),
            self.resource_revalidate(),
            self.resource_promote_memory(),
        ]
    }
}
