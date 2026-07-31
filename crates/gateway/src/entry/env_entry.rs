use std::{collections::HashMap, env};

use model_protocol::model_registry::ModelResolver;
use runtime::{ConfigLoader, PermissionMode, ResolvedPermissionMode};

use crate::{cli, DEFAULT_MODEL_ALIAS};

pub(crate) fn resolve_model_alias_with_config(model: &str) -> String {
    let trimmed = model.trim();
    let config_aliases = config_aliases_for_current_dir();
    let resolver = ModelResolver::new(config_aliases);
    resolver.resolve(trimmed)
}

fn config_aliases_for_current_dir() -> HashMap<String, String> {
    let cwd = match env::current_dir() {
        Ok(cwd) => cwd,
        Err(_) => return HashMap::new(),
    };
    let loader = ConfigLoader::default_for(&cwd);
    match loader.load() {
        Ok(config) => config.aliases().clone().into_iter().collect(),
        Err(_) => HashMap::new(),
    }
}

pub(crate) fn parse_permission_mode_arg(value: &str) -> Result<PermissionMode, String> {
    cli::normalize_permission_mode(value)
        .ok_or_else(|| {
            format!(
                "unsupported permission mode '{value}'. Use read-only, workspace-write, or danger-full-access."
            )
        })
        .map(cli::permission_mode_from_label)
}

pub(crate) fn default_permission_mode() -> PermissionMode {
    env::var("COWD_PERMISSION_MODE")
        .ok()
        .as_deref()
        .and_then(cli::normalize_permission_mode)
        .map(cli::permission_mode_from_label)
        .or_else(config_permission_mode_for_current_dir)
        .unwrap_or(PermissionMode::WorkspaceWrite)
}

fn config_permission_mode_for_current_dir() -> Option<PermissionMode> {
    let cwd = env::current_dir().ok()?;
    let loader = ConfigLoader::default_for(&cwd);
    loader
        .load()
        .ok()?
        .permission_mode()
        .map(permission_mode_from_resolved)
}

fn permission_mode_from_resolved(mode: ResolvedPermissionMode) -> PermissionMode {
    cli::permission_mode_from_resolved(mode)
}

fn config_model_for_current_dir() -> Option<String> {
    let cwd = env::current_dir().ok()?;
    let loader = ConfigLoader::default_for(&cwd);
    loader.load().ok()?.resolved_model()
}

pub(crate) fn resolve_tui_model(cli_model: String) -> String {
    if cli_model != DEFAULT_MODEL_ALIAS {
        return resolve_model_alias_with_config(&cli_model);
    }
    if let Some(config_model) = config_model_for_current_dir() {
        return config_model;
    }
    if let Some(env_model) = env::var("COWD_MODEL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return resolve_model_alias_with_config(&env_model);
    }
    cli_model
}
