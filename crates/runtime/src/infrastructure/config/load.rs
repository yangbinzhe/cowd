//! Layered configuration loading and precedence assembly.

use super::*;

/// Discovers config files and merges them into a [`RuntimeConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigLoader {
    cwd: PathBuf,
    config_home: PathBuf,
}

impl ConfigLoader {
    #[must_use]
    pub fn new(cwd: impl Into<PathBuf>, config_home: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            config_home: config_home.into(),
        }
    }

    #[must_use]
    pub fn default_for(cwd: impl Into<PathBuf>) -> Self {
        let cwd = cwd.into();
        let config_home = default_config_home();
        Self { cwd, config_home }
    }

    #[must_use]
    pub fn config_home(&self) -> &Path {
        &self.config_home
    }

    #[must_use]
    pub fn discover(&self) -> Vec<ConfigEntry> {
        let cc_user_dir = &self.config_home;
        let entry = |source, path: PathBuf| ConfigEntry {
            exists: path.exists(),
            source,
            path,
        };

        vec![
            // ── User-level: ~/.cc paths ──────────────────────────────────────
            entry(ConfigSource::User, cc_user_dir.join("config.yaml")),
            entry(ConfigSource::User, cc_user_dir.join("config.yml")),
            // ── Project-level: .cowd/ paths ──────────────────────────────────
            entry(
                ConfigSource::Project,
                self.cwd.join(".cowd").join("config.yaml"),
            ),
            entry(
                ConfigSource::Project,
                self.cwd.join(".cowd").join("config.yml"),
            ),
            // ── Local overrides: highest priority ────────────────────────────
            entry(
                ConfigSource::Local,
                self.cwd.join(".cowd").join("config.local.yaml"),
            ),
            entry(
                ConfigSource::Local,
                self.cwd.join(".cowd").join("config.local.yml"),
            ),
        ]
    }

    pub fn load(&self) -> Result<RuntimeConfig, ConfigError> {
        self.load_with_diagnostics().map(|result| result.config)
    }

    pub fn load_with_diagnostics(&self) -> Result<ConfigLoadResult, ConfigError> {
        let mut merged = BTreeMap::new();
        let mut loaded_entries = Vec::new();
        let mut mcp_servers = BTreeMap::new();
        let mut all_warnings = Vec::new();

        for entry in self.discover() {
            crate::config_validate::check_unsupported_format(&entry.path)?;
            let parsed_opt = read_optional_yaml_object(&entry.path)?;
            let Some(parsed) = parsed_opt else {
                continue;
            };
            // Validate schema
            {
                let validation = crate::config_validate::validate_config_file(
                    &parsed.object,
                    &parsed.source,
                    &entry.path,
                );
                if !validation.is_ok() {
                    let errors = validation
                        .errors
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join("\n");
                    return Err(ConfigError::Parse(errors));
                }
                all_warnings.extend(validation.warnings);
                validate_optional_hooks_config(&parsed.object, &entry.path)?;
            }
            merge_mcp_servers(&mut mcp_servers, entry.source, &parsed.object, &entry.path)?;
            deep_merge_objects(&mut merged, &parsed.object);
            loaded_entries.push(entry);
        }

        // Apply environment variable overrides (CC_* prefix) after file configs.
        let env_overrides = collect_env_overrides();
        deep_merge_objects(&mut merged, &env_overrides);

        // Inject config file `env:` section into the process environment.
        inject_config_env(&merged);
        // P13: network domain policy is configurable while remaining env-first.
        // Illegal mode values are fail-closed: the process refuses to start
        // instead of silently widening network access.
        inject_network_domain_env(&merged)?;

        let mut diagnostics = all_warnings
            .iter()
            .map(|warning| {
                tracing::warn!("{warning}");
                ConfigDiagnostic {
                    severity: ConfigDiagnosticSeverity::Warning,
                    code: "config_validation_warning".to_string(),
                    message: warning.to_string(),
                }
            })
            .collect::<Vec<_>>();

        let merged_value = JsonValue::Object(merged.clone());

        let feature_config = RuntimeFeatureConfig {
            workspace: parse_optional_workspace(&merged_value)?,
            hooks: parse_optional_hooks_config(&merged_value)?,
            plugins: parse_optional_plugin_config(&merged_value)?,
            mcp: McpConfigCollection {
                servers: mcp_servers,
            },
            oauth: parse_optional_oauth_config(&merged_value, "merged settings.oauth")?,
            model: parse_optional_model(&merged_value),
            routing_mode: parse_routing_mode(&merged_value)?,
            aliases: parse_optional_aliases(&merged_value)?,
            model_context_windows: parse_optional_model_context_windows(&merged_value)?,
            permission_mode: parse_optional_permission_mode(&merged_value)?,
            permission_rules: parse_optional_permission_rules(&merged_value)?,
            approval: parse_optional_approval_config(&merged_value)?,
            sandbox: parse_optional_sandbox_config(&merged_value)?,
            fallbacks: parse_fallbacks(&merged_value, &mut diagnostics),
            providers: parse_optional_providers_config(&merged_value)?,
            trusted_roots: parse_optional_trusted_roots(&merged_value)?,
            memory: parse_optional_memory_config(&merged_value)?,
            context_budget: parse_optional_context_budget_config(&merged_value)?,
            compression: parse_optional_compression_config(&merged_value)?,
            session_history: parse_optional_session_history_config(&merged_value)?,
            gateway: parse_optional_gateway_config(&merged_value)?,
            apps: parse_optional_apps_config(&merged_value)?,
            storage: parse_optional_storage_config(&merged_value)?,
            gate_auto_fix: parse_optional_gate_auto_fix_config(&merged_value)?,
            runtime_control: parse_optional_runtime_control_config(&merged_value)?,
            hot_state: parse_optional_hot_state_config(&merged_value)?,
            provider_resources: parse_optional_provider_resource_config(&merged_value)?,
        };

        Ok(ConfigLoadResult {
            config: RuntimeConfig {
                merged,
                loaded_entries,
                feature_config,
            },
            diagnostics,
        })
    }
}
