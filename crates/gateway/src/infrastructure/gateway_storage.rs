use std::path::{Path, PathBuf};

use crate::task_kernel::TaskKernel;
use storage::StorageDomainId;

pub(crate) struct GatewayStorage;

impl GatewayStorage {
    pub(crate) fn registry(config_home: impl AsRef<Path>) -> storage::StorageRegistry {
        storage::StorageRegistry::default_for_config_home(config_home)
    }

    pub(crate) fn task_db_path(config_home: impl AsRef<Path>) -> PathBuf {
        let layout = storage::StorageLayout::default_for_config_home(config_home);
        layout
            .sqlite_path("tasks")
            .map(Path::to_path_buf)
            .unwrap_or_else(|| layout.root.join("tasks.sqlite"))
    }

    pub(crate) fn session_db_path(config_home: impl AsRef<Path>) -> PathBuf {
        let layout = storage::StorageLayout::default_for_config_home(config_home);
        layout
            .sqlite_path("session")
            .map(Path::to_path_buf)
            .unwrap_or_else(|| layout.root.join("session.sqlite"))
    }

    pub(crate) fn open_task_kernel(config_home: impl AsRef<Path>) -> Result<TaskKernel, String> {
        let registry = Self::registry(config_home);
        let endpoint = registry
            .endpoint(&StorageDomainId::Tasks)
            .map_err(|error| error.to_string())?;
        TaskKernel::open_storage_handle(&endpoint.as_handle())
    }

    pub(crate) fn open_unified_session_store(
        config_home: impl AsRef<Path>,
    ) -> Result<memory::UnifiedSessionStore, Box<dyn std::error::Error>> {
        let registry = Self::registry(config_home);
        let handle = registry.endpoint(&StorageDomainId::Session)?.as_handle();
        if let Some(parent) = handle.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        memory::UnifiedSessionStore::open_sqlite_storage_handle(&handle).map_err(|error| {
            let message = format!(
                "failed to open unified session store at {:?}: {error}",
                handle.path
            );
            Box::<dyn std::error::Error>::from(message)
        })
    }
}
