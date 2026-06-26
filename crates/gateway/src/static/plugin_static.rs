use plugins::{PluginError, PluginLoadFailure, PluginManager, PluginSummary};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginsCommandResult {
    pub(crate) message: String,
    pub(crate) reload_runtime: bool,
}

pub(crate) fn handle_plugins_slash_command(
    action: Option<&str>,
    target: Option<&str>,
    manager: &mut PluginManager,
) -> Result<PluginsCommandResult, PluginError> {
    match action {
        None | Some("list") => {
            let report = manager.installed_plugin_registry_report()?;
            Ok(PluginsCommandResult {
                message: render_plugins_report_with_failures(&report.summaries(), report.failures()),
                reload_runtime: false,
            })
        }
        Some("install") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins install <path>".to_string(),
                    reload_runtime: false,
                });
            };
            let install = manager.install(target)?;
            let plugin = manager
                .list_installed_plugins()?
                .into_iter()
                .find(|plugin| plugin.metadata.id == install.plugin_id);
            Ok(PluginsCommandResult {
                message: render_plugin_install_report(&install.plugin_id, plugin.as_ref()),
                reload_runtime: true,
            })
        }
        Some("enable") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins enable <name>".to_string(),
                    reload_runtime: false,
                });
            };
            let plugin = resolve_plugin_target(manager, target)?;
            manager.enable(&plugin.metadata.id)?;
            Ok(PluginsCommandResult {
                message: format!(
                    "Plugins\n  Result           enabled {}\n  Name             {}\n  Version          {}\n  Status           enabled",
                    plugin.metadata.id, plugin.metadata.name, plugin.metadata.version
                ),
                reload_runtime: true,
            })
        }
        Some("disable") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins disable <name>".to_string(),
                    reload_runtime: false,
                });
            };
            let plugin = resolve_plugin_target(manager, target)?;
            manager.disable(&plugin.metadata.id)?;
            Ok(PluginsCommandResult {
                message: format!(
                    "Plugins\n  Result           disabled {}\n  Name             {}\n  Version          {}\n  Status           disabled",
                    plugin.metadata.id, plugin.metadata.name, plugin.metadata.version
                ),
                reload_runtime: true,
            })
        }
        Some("uninstall") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins uninstall <plugin-id>".to_string(),
                    reload_runtime: false,
                });
            };
            manager.uninstall(target)?;
            Ok(PluginsCommandResult {
                message: format!("Plugins\n  Result           uninstalled {target}"),
                reload_runtime: true,
            })
        }
        Some("update") => {
            let Some(target) = target else {
                return Ok(PluginsCommandResult {
                    message: "Usage: /plugins update <plugin-id>".to_string(),
                    reload_runtime: false,
                });
            };
            let update = manager.update(target)?;
            let plugin = manager
                .list_installed_plugins()?
                .into_iter()
                .find(|plugin| plugin.metadata.id == update.plugin_id);
            Ok(PluginsCommandResult {
                message: format!(
                    "Plugins\n  Result           updated {}\n  Name             {}\n  Old version      {}\n  New version      {}\n  Status           {}",
                    update.plugin_id,
                    plugin
                        .as_ref()
                        .map_or_else(|| update.plugin_id.clone(), |plugin| plugin.metadata.name.clone()),
                    update.old_version,
                    update.new_version,
                    plugin
                        .as_ref()
                        .map_or("unknown", |plugin| if plugin.enabled { "enabled" } else { "disabled" }),
                ),
                reload_runtime: true,
            })
        }
        Some(other) => Ok(PluginsCommandResult {
            message: format!(
                "Unknown /plugins action '{other}'. Use list, install, enable, disable, uninstall, or update."
            ),
            reload_runtime: false,
        }),
    }
}

fn render_plugins_report_with_failures(
    plugins: &[PluginSummary],
    failures: &[PluginLoadFailure],
) -> String {
    let mut lines = vec!["Plugins".to_string()];
    if plugins.is_empty() {
        lines.push("  No plugins installed.".to_string());
    } else {
        for plugin in plugins {
            let enabled = if plugin.enabled {
                "enabled"
            } else {
                "disabled"
            };
            lines.push(format!(
                "  {name:<20} v{version:<10} {enabled}",
                name = plugin.metadata.name,
                version = plugin.metadata.version,
            ));
        }
    }
    if !failures.is_empty() {
        lines.push(String::new());
        lines.push("Warnings:".to_string());
        for failure in failures {
            lines.push(format!(
                "  Failed to load {} plugin from `{}`",
                failure.kind,
                failure.plugin_root.display()
            ));
            lines.push(format!("      Error: {}", failure.error()));
        }
    }
    lines.join("\n")
}

fn render_plugin_install_report(plugin_id: &str, plugin: Option<&PluginSummary>) -> String {
    let name = plugin.map_or(plugin_id, |plugin| plugin.metadata.name.as_str());
    let version = plugin.map_or("unknown", |plugin| plugin.metadata.version.as_str());
    let enabled = plugin.is_some_and(|plugin| plugin.enabled);
    format!(
        "Plugins\n  Result           installed {plugin_id}\n  Name             {name}\n  Version          {version}\n  Status           {}",
        if enabled { "enabled" } else { "disabled" }
    )
}

fn resolve_plugin_target(
    manager: &PluginManager,
    target: &str,
) -> Result<PluginSummary, PluginError> {
    let mut matches = manager
        .list_installed_plugins()?
        .into_iter()
        .filter(|plugin| plugin.metadata.id == target || plugin.metadata.name == target)
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(PluginError::NotFound(format!(
            "plugin `{target}` is not installed or discoverable"
        ))),
        _ => Err(PluginError::InvalidManifest(format!(
            "plugin name `{target}` is ambiguous; use the full plugin id"
        ))),
    }
}
