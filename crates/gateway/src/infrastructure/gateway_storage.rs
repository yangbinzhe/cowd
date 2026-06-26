use std::path::{Path, PathBuf};

use crate::task_kernel::TaskKernel;

pub(crate) struct GatewayStorage;

impl GatewayStorage {
    pub(crate) fn layout(config_home: impl AsRef<Path>) -> storage::StorageLayout {
        storage::StorageLayout::default_for_config_home(config_home)
    }

    pub(crate) fn registry(config_home: impl AsRef<Path>) -> storage::StorageRegistry {
        storage::StorageRegistry::default_for_config_home(config_home)
    }

    pub(crate) fn task_db_path(config_home: impl AsRef<Path>) -> PathBuf {
        let config_home = config_home.as_ref();
        Self::layout(config_home)
            .sqlite_path("tasks")
            .map(Path::to_path_buf)
            .unwrap_or_else(|| config_home.join("tasks.db"))
    }

    pub(crate) fn session_db_path(config_home: impl AsRef<Path>) -> PathBuf {
        let config_home = config_home.as_ref();
        Self::layout(config_home)
            .sqlite_path("session")
            .map(Path::to_path_buf)
            .unwrap_or_else(|| config_home.join("sessions.db"))
    }

    pub(crate) fn open_task_kernel(config_home: impl AsRef<Path>) -> Result<TaskKernel, String> {
        let registry = Self::registry(config_home);
        let handle = registry
            .sqlite_handle("tasks")
            .map_err(|error| error.to_string())?;
        TaskKernel::open_storage_handle(handle)
    }

    pub(crate) fn open_unified_session_store(
        config_home: impl AsRef<Path>,
    ) -> Result<memory::UnifiedSessionStore, Box<dyn std::error::Error>> {
        let registry = Self::registry(config_home);
        let handle = registry.sqlite_handle("session")?;
        if let Some(parent) = handle.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        memory::UnifiedSessionStore::open_storage_handle(handle).map_err(|error| {
            let message = format!(
                "failed to open unified session store at {:?}: {error}",
                handle.path
            );
            Box::<dyn std::error::Error>::from(message)
        })
    }
}
