//! PostgreSQL-only storage maintenance commands.
//!
//! Cowd has one database owner. These commands never inspect, import, create,
//! or fall back to a second database.

use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use crate::selected_storage::SelectedStorageTopology;

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let [command] = args else {
        return Err("usage: cowd storage plan | upgrade | verify | status | cleanup".to_string());
    };
    let context = PostgresMaintenanceContext::load()?;
    match command.as_str() {
        "plan" => context.plan(),
        "upgrade" => context.upgrade(),
        "verify" => context.verify(),
        "status" => context.status(),
        "cleanup" => context.cleanup(),
        _ => Err("usage: cowd storage plan | upgrade | verify | status | cleanup".to_string()),
    }
}

struct PostgresMaintenanceContext {
    config_home: PathBuf,
    workspace_root: PathBuf,
    runtime_config: runtime::RuntimeConfig,
}

impl PostgresMaintenanceContext {
    fn load() -> Result<Self, String> {
        let config_home = runtime::cowd_dirs::config_home_dir();
        let workspace_root = std::env::current_dir().map_err(stringify)?;
        let loaded = runtime::ConfigLoader::new(&workspace_root, &config_home)
            .load_with_diagnostics()
            .map_err(|error| format!("failed to load runtime configuration: {error}"))?;
        Ok(Self {
            config_home,
            workspace_root,
            runtime_config: loaded.config,
        })
    }

    fn postgres(&self) -> Result<&runtime::PostgresTopologyConfig, String> {
        self.runtime_config
            .storage()
            .postgres
            .as_ref()
            .ok_or_else(|| {
                "storage.postgres is required; PostgreSQL is the only supported database"
                    .to_string()
            })
    }

    fn plan(&self) -> Result<(), String> {
        let postgres = self.postgres()?;
        print_json(&serde_json::json!({
            "operation": "postgres_maintenance_plan",
            "backend": "postgres",
            "logical_identity": postgres.logical_identity,
            "secret_ref": postgres.secret_ref,
            "workspace": self.workspace_root,
            "commands": ["upgrade", "verify"],
            "fallback": serde_json::Value::Null,
            "historical_import": false,
        }))
    }

    fn upgrade(&self) -> Result<(), String> {
        self.postgres()?;
        ensure_gateway_stopped()?;
        let topology = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        print_json(&serde_json::json!({
            "operation": "postgres_schema_upgrade",
            "backend": topology.backend_label(),
            "logical_identity": topology.postgres_executor.logical_identity(),
            "gateway_stopped": true,
            "cowd_version": env!("CARGO_PKG_VERSION"),
            "status": "completed",
        }))
    }

    fn verify(&self) -> Result<(), String> {
        self.postgres()?;
        ensure_gateway_stopped()?;
        let topology = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        print_json(&serde_json::json!({
            "operation": "postgres_readiness_verify",
            "backend": topology.backend_label(),
            "logical_identity": topology.postgres_executor.logical_identity(),
            "migration_catalogs": "verified",
            "status": "ready",
        }))
    }

    fn status(&self) -> Result<(), String> {
        let postgres = self.postgres()?;
        print_json(&serde_json::json!({
            "configured_backend": "postgres",
            "logical_identity": postgres.logical_identity,
            "secret_ref": postgres.secret_ref,
            "historical_import": false,
            "fallback": serde_json::Value::Null,
        }))
    }

    fn cleanup(&self) -> Result<(), String> {
        let artifact_dir = std::env::var_os("COWD_BASH_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| self.config_home.join("storage").join("bash-artifacts"));
        let removed = cleanup_bash_artifacts(&artifact_dir, Duration::from_secs(7 * 24 * 3600))?;
        print_json(&serde_json::json!({
            "operation": "storage_cleanup",
            "artifact_dir": artifact_dir,
            "removed_files": removed,
            "ttl_days": 7,
        }))
    }
}

fn ensure_gateway_stopped() -> Result<(), String> {
    let pid_path = crate::server::pid_file();
    let Ok(value) = fs::read_to_string(&pid_path) else {
        return Ok(());
    };
    let pid = value.trim();
    if pid.is_empty() {
        return Ok(());
    }
    if PathBuf::from("/proc").join(pid).exists() {
        return Err(format!(
            "Gateway pid {pid} is still running at {}; stop it before PostgreSQL maintenance",
            pid_path.display()
        ));
    }
    Ok(())
}

fn cleanup_bash_artifacts(dir: &Path, max_age: Duration) -> Result<usize, String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(0);
    };
    let cutoff = SystemTime::now()
        .checked_sub(max_age)
        .ok_or_else(|| "invalid cleanup age".to_string())?;
    let mut removed = 0usize;
    for entry in entries {
        let entry = entry.map_err(stringify)?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let metadata = entry.metadata().map_err(stringify)?;
        if metadata.modified().map_err(stringify)? < cutoff {
            fs::remove_file(&path).map_err(stringify)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn print_json(value: &serde_json::Value) -> Result<(), String> {
    let output = serde_json::to_string_pretty(value).map_err(stringify)?;
    println!("{output}");
    Ok(())
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_keeps_fresh_regular_files() {
        let root = tempfile::tempdir().expect("tempdir");
        fs::write(root.path().join("fresh"), "keep").expect("fresh");
        assert_eq!(
            cleanup_bash_artifacts(root.path(), Duration::from_secs(24 * 3600)).expect("cleanup"),
            0
        );
        assert!(root.path().join("fresh").exists());
    }
}
