#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::tool::{
    ToolDefinition as KernelToolDefinition, ToolDescriptorHealth, ToolDescriptorRef,
    ToolEffectKind, ToolEffectResolverSpec, ToolPermissionMode as KernelToolPermissionMode,
};
use plugins::PluginTool;
use serde_json::Value;

use crate::permissions::PermissionMode;

/// Serializes tests that temporarily mutate process-wide state such as `PATH`
/// or the current working directory. Production code never takes this lock.
#[cfg(test)]
pub(crate) fn test_process_environment_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

// Re-exports from split modules
pub use tool_specs::{
    bootstrap_builtin_tool_ids, builtin_effect_resolver_spec, mvp_tool_specs, ToolFamily, ToolSpec,
    ToolTier,
};
pub(crate) use tool_specs::{normalize_tool_name, permission_mode_from_plugin};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolManifestEntry {
    pub name: String,
    pub source: ToolSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSource {
    Base,
    Conditional,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolRegistry {
    entries: Vec<ToolManifestEntry>,
}

impl ToolRegistry {
    #[must_use]
    pub fn new(entries: Vec<ToolManifestEntry>) -> Self {
        Self { entries }
    }

    #[must_use]
    pub fn entries(&self) -> &[ToolManifestEntry] {
        &self.entries
    }
}

#[derive(Debug, Clone)]
pub struct ToolCatalog {
    plugin_tools: Vec<PluginTool>,
    entries: Vec<ToolCatalogEntry>,
    entry_order: Vec<usize>,
    canonical_index: BTreeMap<String, usize>,
    identity_index: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
    pub required_permission: PermissionMode,
    pub effect_resolver: ToolEffectResolverSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ToolCatalogSource {
    Builtin,
    Runtime,
    Plugin,
}

#[derive(Debug, Clone)]
struct ToolCatalogEntry {
    definition: ToolDefinition,
    raw_description: Option<String>,
    search_description: String,
    permission: PermissionMode,
    effect_resolver: ToolEffectResolverSpec,
    source: ToolCatalogSource,
    model_exposed: bool,
    plugin_index: Option<usize>,
}

impl ToolCatalogSource {
    const fn label(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Runtime => "runtime",
            Self::Plugin => "plugin",
        }
    }
}

fn insert_catalog_entry(
    entries: &mut Vec<ToolCatalogEntry>,
    canonical_index: &mut BTreeMap<String, usize>,
    identity_index: &mut BTreeMap<String, usize>,
    entry: ToolCatalogEntry,
) -> Result<(), String> {
    let name = entry.definition.name.clone();
    let identity = tool_name_identity(&name);
    if let Some(existing_index) = identity_index.get(&identity) {
        let existing = &entries[*existing_index].definition.name;
        return Err(format!(
            "{} tool `{name}` conflicts with canonical tool identity `{existing}`",
            entry.source.label(),
        ));
    }
    let index = entries.len();
    canonical_index.insert(name, index);
    identity_index.insert(identity, index);
    entries.push(entry);
    Ok(())
}

fn tool_name_identity(value: &str) -> String {
    normalize_tool_name(value)
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect()
}

fn is_canonical_tool_name(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('_')
        && !value.ends_with('_')
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

/// Attach the tool's input contract to its description.
///
/// The schema states what a tool accepts and the description states why one uses
/// it; neither translates into the other, and until now nothing did. A model had
/// to resolve a `oneOf`, chase a `$ref` into `$defs`, or invent a reference format
/// to work out what to send - one live run needed seventeen rejected calls and
/// eleven invented input shapes to discover it. Deriving the missing half from
/// the schema is done once, here, for every source of definitions: built-in,
/// runtime-injected, and plugin.
fn model_facing_description(description: Option<&str>, schema: &Value) -> Option<String> {
    let summary = harness_contract::tool_input_contract::input_contract_summary(schema)?;
    let contract = format!("Input contract (from the schema):\n{summary}");
    match description {
        Some(description) if !description.is_empty() => Some(format!("{description}\n{contract}")),
        _ => Some(contract),
    }
}

impl ToolCatalog {
    #[must_use]
    pub fn builtin() -> Self {
        Self::from_parts(Vec::new(), Vec::new()).expect("the builtin catalog is valid")
    }

    pub fn with_plugin_tools(plugin_tools: Vec<PluginTool>) -> Result<Self, String> {
        Self::from_parts(plugin_tools, Vec::new())
    }

    fn from_parts(
        plugin_tools: Vec<PluginTool>,
        runtime_tools: Vec<RuntimeToolDefinition>,
    ) -> Result<Self, String> {
        let mut catalog = Self {
            plugin_tools,
            entries: Vec::new(),
            entry_order: Vec::new(),
            canonical_index: BTreeMap::new(),
            identity_index: BTreeMap::new(),
        };
        catalog.rebuild_index(&runtime_tools)?;
        Ok(catalog)
    }

    /// Build the one immutable generation consumed by every catalog query.
    ///
    /// Runtime definitions are consumed only while building this generation.
    /// The plugin implementation vector remains for execution; reads never
    /// rescan either source.
    fn rebuild_index(&mut self, runtime_tools: &[RuntimeToolDefinition]) -> Result<(), String> {
        let mut entries = Vec::new();
        let mut canonical_index = BTreeMap::new();
        let mut identity_index = BTreeMap::new();

        for spec in mvp_tool_specs() {
            let name = spec.name.to_string();
            let model_description =
                model_facing_description(Some(&spec.description), &spec.input_schema);
            let search_description = model_description
                .clone()
                .unwrap_or_else(|| spec.description.to_string());
            let entry = ToolCatalogEntry {
                definition: ToolDefinition {
                    name: name.clone(),
                    description: model_description,
                    input_schema: spec.input_schema,
                },
                raw_description: Some(spec.description.to_string()),
                search_description,
                permission: spec.required_permission,
                effect_resolver: builtin_effect_resolver_spec(&name),
                source: ToolCatalogSource::Builtin,
                model_exposed: true,
                plugin_index: None,
            };
            insert_catalog_entry(
                &mut entries,
                &mut canonical_index,
                &mut identity_index,
                entry,
            )?;
        }

        for (plugin_index, tool) in self.plugin_tools.iter().enumerate() {
            let definition = tool.definition();
            let name = definition.name.clone();
            if !is_canonical_tool_name(&name) {
                return Err(format!(
                    "plugin tool `{name}` must use one lowercase snake_case identity"
                ));
            }
            if let Some(existing) = canonical_index.get(&name) {
                let source = entries[*existing].source;
                if source == ToolCatalogSource::Builtin {
                    return Err(format!(
                        "plugin tool `{name}` conflicts with a built-in tool name"
                    ));
                }
                return Err(format!("duplicate plugin tool name `{name}`"));
            }
            let model_description = model_facing_description(
                definition.description.as_deref(),
                &definition.input_schema,
            );
            let search_description = model_description
                .clone()
                .or_else(|| definition.description.clone())
                .unwrap_or_default();
            let permission = permission_mode_from_plugin(tool.required_permission())?;
            let entry = ToolCatalogEntry {
                definition: ToolDefinition {
                    name: name.clone(),
                    description: model_description,
                    input_schema: definition.input_schema.clone(),
                },
                raw_description: definition.description.clone(),
                search_description,
                permission,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "plugin.unknown".to_string(),
                    resolver_version: 1,
                },
                source: ToolCatalogSource::Plugin,
                model_exposed: true,
                plugin_index: Some(plugin_index),
            };
            insert_catalog_entry(
                &mut entries,
                &mut canonical_index,
                &mut identity_index,
                entry,
            )?;
        }

        for tool in runtime_tools {
            let name = tool.name.clone();
            if !is_canonical_tool_name(&name) {
                return Err(format!(
                    "runtime tool `{name}` must use one lowercase snake_case identity"
                ));
            }
            if canonical_index.contains_key(&name) {
                return Err(format!(
                    "runtime tool `{name}` conflicts with an existing tool name"
                ));
            }
            let model_description =
                model_facing_description(tool.description.as_deref(), &tool.input_schema);
            let search_description = model_description.clone().unwrap_or_default();
            let resolver = crate::tool_orchestrator::resolve_registered_tool_effect(
                &tool.effect_resolver,
                &tool.name,
                &Value::Object(serde_json::Map::new()),
                kernel_permission_mode(tool.required_permission),
            );
            if resolver.effect_kind == ToolEffectKind::Unknown {
                return Err(format!(
                    "runtime tool `{}` declares unsupported effect resolver `{}`",
                    tool.name, tool.effect_resolver.resolver_id
                ));
            }
            let entry = ToolCatalogEntry {
                definition: ToolDefinition {
                    name: name.clone(),
                    description: model_description,
                    input_schema: tool.input_schema.clone(),
                },
                raw_description: tool.description.clone(),
                search_description,
                permission: tool.required_permission,
                effect_resolver: tool.effect_resolver.clone(),
                source: ToolCatalogSource::Runtime,
                model_exposed: !name.starts_with("mcp__"),
                plugin_index: None,
            };
            insert_catalog_entry(
                &mut entries,
                &mut canonical_index,
                &mut identity_index,
                entry,
            )?;
        }

        self.entries = entries;
        self.entry_order = [
            ToolCatalogSource::Builtin,
            ToolCatalogSource::Runtime,
            ToolCatalogSource::Plugin,
        ]
        .into_iter()
        .flat_map(|source| {
            self.entries
                .iter()
                .enumerate()
                .filter(move |(_, entry)| entry.source == source)
                .map(|(index, _)| index)
        })
        .collect();
        self.canonical_index = canonical_index;
        self.identity_index = identity_index;
        Ok(())
    }

    pub fn with_runtime_tools(
        self,
        runtime_tools: Vec<RuntimeToolDefinition>,
    ) -> Result<Self, String> {
        Self::from_parts(self.plugin_tools, runtime_tools)
    }

    /// Append runtime-provided tools without replacing definitions already
    /// installed by the core Runtime composition.
    pub fn extend_runtime_tools(
        self,
        additional_tools: Vec<RuntimeToolDefinition>,
    ) -> Result<Self, String> {
        let mut runtime_tools = self.runtime_definitions();
        runtime_tools.extend(additional_tools);
        Self::from_parts(self.plugin_tools, runtime_tools)
    }

    fn runtime_definitions(&self) -> Vec<RuntimeToolDefinition> {
        self.entries
            .iter()
            .filter(|entry| entry.source == ToolCatalogSource::Runtime)
            .map(|entry| RuntimeToolDefinition {
                name: entry.definition.name.clone(),
                description: entry.raw_description.clone(),
                input_schema: entry.definition.input_schema.clone(),
                required_permission: entry.permission,
                effect_resolver: entry.effect_resolver.clone(),
            })
            .collect()
    }

    pub fn normalize_allowed_tools(
        &self,
        values: &[String],
    ) -> Result<Option<BTreeSet<String>>, String> {
        if values.is_empty() {
            return Ok(None);
        }

        let canonical_names = self
            .entries
            .iter()
            .map(|entry| entry.definition.name.clone())
            .collect::<Vec<_>>();
        let mut allowed = BTreeSet::new();
        for value in values {
            for token in value
                .split(|ch: char| ch == ',' || ch.is_whitespace())
                .filter(|token| !token.is_empty())
            {
                let canonical = self.canonical_name(token).ok_or_else(|| {
                    format!(
                        "unsupported tool in --allowedTools: {token} (expected one of: {})",
                        canonical_names.join(", ")
                    )
                })?;
                allowed.insert(canonical);
            }
        }

        Ok(Some(allowed))
    }

    #[must_use]
    pub fn definitions(&self, allowed_tools: Option<&BTreeSet<String>>) -> Vec<ToolDefinition> {
        self.entry_order
            .iter()
            .filter_map(|index| self.entries.get(*index))
            .filter(|entry| entry.model_exposed)
            .filter(|entry| {
                allowed_tools.is_none_or(|allowed| allowed.contains(entry.definition.name.as_str()))
            })
            .map(|entry| entry.definition.clone())
            .collect()
    }

    #[must_use]
    pub fn kernel_definitions(
        &self,
        allowed_tools: Option<&BTreeSet<String>>,
    ) -> Vec<KernelToolDefinition> {
        self.entry_order
            .iter()
            .filter_map(|index| self.entries.get(*index))
            .filter(|entry| {
                allowed_tools.is_none_or(|allowed| allowed.contains(entry.definition.name.as_str()))
            })
            .map(|entry| KernelToolDefinition {
                name: entry.definition.name.clone(),
                description: entry.definition.description.clone(),
                input_schema: entry.definition.input_schema.clone(),
                required_permission: kernel_permission_mode(entry.permission),
            })
            .collect()
    }

    pub fn permission_specs(
        &self,
        allowed_tools: Option<&BTreeSet<String>>,
    ) -> Result<Vec<(String, PermissionMode)>, String> {
        Ok(self
            .entry_order
            .iter()
            .filter_map(|index| self.entries.get(*index))
            .filter(|entry| {
                allowed_tools.is_none_or(|allowed| allowed.contains(entry.definition.name.as_str()))
            })
            .map(|entry| (entry.definition.name.clone(), entry.permission))
            .collect())
    }

    #[must_use]
    pub fn required_permission(&self, name: &str) -> Option<KernelToolPermissionMode> {
        let name = self.canonical_name(name)?;
        self.entries
            .get(*self.canonical_index.get(&name)?)
            .map(|entry| kernel_permission_mode(entry.permission))
    }

    #[must_use]
    pub fn effect_resolver(&self, name: &str) -> harness_contract::tool::ToolEffectResolverSpec {
        let Some(name) = self.canonical_name(name) else {
            return harness_contract::tool::ToolEffectResolverSpec {
                resolver_id: "unregistered.unknown".to_string(),
                resolver_version: 1,
            };
        };
        self.canonical_index
            .get(&name)
            .and_then(|index| self.entries.get(*index))
            .map(|entry| entry.effect_resolver.clone())
            .unwrap_or(harness_contract::tool::ToolEffectResolverSpec {
                resolver_id: "unregistered.unknown".to_string(),
                resolver_version: 1,
            })
    }

    #[must_use]
    pub fn has_runtime_tool(&self, name: &str) -> bool {
        self.canonical_name(name).is_some_and(|name| {
            self.canonical_index
                .get(&name)
                .and_then(|index| self.entries.get(*index))
                .is_some_and(|entry| entry.source == ToolCatalogSource::Runtime)
        })
    }

    #[must_use]
    pub(crate) fn search_ids(&self, query: &str, max_results: usize) -> Vec<String> {
        search_tool_specs(query, max_results.max(1), &self.searchable_tool_specs())
    }

    #[must_use]
    pub(crate) fn descriptor_ref(&self, name: &str) -> Option<ToolDescriptorRef> {
        let name = self.canonical_name(name)?;
        let entry = self.entries.get(*self.canonical_index.get(&name)?)?;
        let source = entry.source.label();
        Some(ToolDescriptorRef {
            canonical_id: entry.definition.name.clone(),
            display_name: entry.definition.name.clone(),
            source: source.to_string(),
            schema_hash: value_hash(&entry.definition.input_schema),
            required_permission: kernel_permission_mode(entry.permission),
            permission_source: format!("{source}_manifest"),
            health: ToolDescriptorHealth::Healthy,
        })
    }

    pub(crate) fn execute_plugin(&self, name: &str, input: &Value) -> Result<String, String> {
        let name = self
            .canonical_name(name)
            .ok_or_else(|| format!("unsupported tool: {name}"))?;
        let entry = self.entries.get(
            *self
                .canonical_index
                .get(&name)
                .ok_or_else(|| format!("unsupported tool: {name}"))?,
        );
        let plugin_index = entry
            .and_then(|entry| entry.plugin_index)
            .ok_or_else(|| format!("unsupported tool: {name}"))?;
        self.plugin_tools
            .get(plugin_index)
            .ok_or_else(|| format!("unsupported tool: {name}"))?
            .execute(input)
            .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub fn execute(&self, name: &str, input: &Value) -> Result<String, String> {
        if mvp_tool_specs().iter().any(|spec| spec.name == name) {
            let host = ToolHost::builtin("tools-catalog-test", std::env::current_dir().unwrap());
            return executor::execute_with_lease(&host.pin_snapshot(), name, input);
        }
        self.execute_plugin(name, input)
    }

    #[cfg(test)]
    pub(crate) fn searchable_tool_specs_for_test(&self) -> Vec<SearchableToolSpec> {
        self.searchable_tool_specs()
    }

    fn searchable_tool_specs(&self) -> Vec<SearchableToolSpec> {
        self.entry_order
            .iter()
            .filter_map(|index| self.entries.get(*index))
            .filter(|entry| entry.model_exposed)
            .map(|entry| SearchableToolSpec {
                name: entry.definition.name.clone(),
                description: entry.search_description.clone(),
            })
            .collect()
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.canonical_name(name).is_some()
    }

    /// Resolve every accepted spelling to the one catalog-owned tool id.
    ///
    /// Exact ids always win. Case, separator, and CamelCase variants are
    /// accepted only when they identify exactly one registered tool. A small
    /// explicit alias set covers the established CLI shorthand. Registration
    /// rejects identity collisions, so authorization, exposure, and execution
    /// never resolve the same provider token differently.
    #[must_use]
    pub fn canonical_name(&self, requested: &str) -> Option<String> {
        if self.canonical_index.contains_key(requested) {
            return Some(requested.to_string());
        }
        self.identity_index
            .get(&tool_name_identity(requested))
            .and_then(|index| self.entries.get(*index))
            .map(|entry| entry.definition.name.clone())
    }

    #[must_use]
    pub fn tool_ids(&self) -> BTreeSet<String> {
        self.canonical_index.keys().cloned().collect()
    }

    #[must_use]
    pub(crate) fn definition(&self, name: &str) -> Option<&ToolDefinition> {
        let name = self.canonical_name(name)?;
        self.canonical_index
            .get(&name)
            .and_then(|index| self.entries.get(*index))
            .map(|entry| &entry.definition)
    }
}

fn kernel_permission_mode(permission: PermissionMode) -> KernelToolPermissionMode {
    match permission {
        PermissionMode::ReadOnly => KernelToolPermissionMode::ReadOnly,
        PermissionMode::WorkspaceWrite => KernelToolPermissionMode::WorkspaceWrite,
        PermissionMode::DangerFullAccess => KernelToolPermissionMode::DangerFullAccess,
    }
}

fn value_hash(value: &Value) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serde_json::to_string(value)
        .unwrap_or_default()
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
#[path = "execution/executor.rs"]
pub mod executor;
pub use executor::orchestrate::{requested_sub_call_tools, SubCallAuthorization};
pub(crate) use executor::*;
#[path = "host.rs"]
pub mod host;
pub use host::{OrchestratedRead, ToolHost, ToolHostError, ToolHostLease, ToolHostSnapshot};
#[path = "execution/background_jobs.rs"]
pub mod background_jobs;
#[path = "execution/bash.rs"]
pub mod bash;
#[path = "state/checkpoint.rs"]
pub mod checkpoint;
#[path = "filesystem/file_ops.rs"]
pub mod file_ops;
#[path = "policy/lane_events.rs"]
pub mod lane_events;
#[path = "policy/lane_policy.rs"]
pub mod lane_policy;
#[path = "state/lsp_client.rs"]
pub mod lsp_client;
#[path = "state/mutation_plan.rs"]
pub mod mutation_plan;
#[path = "policy/network_policy.rs"]
pub mod network_policy;
#[path = "filesystem/path_policy.rs"]
pub mod path_policy;
#[path = "filesystem/pdf_extract.rs"]
pub mod pdf_extract;
#[path = "policy/permissions.rs"]
pub mod permissions;
#[path = "execution/sandbox_exec.rs"]
pub mod sandbox_exec;
pub use sandbox_exec::{CompletedToolExecution, PreparedToolExecution, ToolExecutionCancellation};
#[path = "search/mod.rs"]
pub(crate) mod search;
#[path = "policy/stale_branch.rs"]
pub mod stale_branch;
#[path = "state/tool_cache.rs"]
pub mod tool_cache;
#[path = "state/tool_orchestrator.rs"]
pub mod tool_orchestrator;
#[path = "registry/tool_specs.rs"]
pub mod tool_specs;

#[cfg(test)]
mod input_contract_tests {
    use super::*;
    use harness_contract::tool_input_contract::admissible_field_names;

    #[test]
    fn bounded_opaque_capabilities_are_owned_send_and_cancellation_is_shared() {
        fn assert_send<Value: Send>() {}
        fn assert_shared<Value: Send + Sync + Clone>() {}
        assert_send::<PreparedToolExecution>();
        assert_send::<CompletedToolExecution>();
        assert_shared::<ToolExecutionCancellation>();
        let cancellation = ToolExecutionCancellation::new();
        let shared = cancellation.clone();
        shared.cancel();
    }

    /// Every model-facing definition must restate its own required fields. Until
    /// it did, a tool could be perfectly callable and still be uncallable in
    /// practice: one live run needed seventeen rejected `artifact_publish` calls
    /// and eleven invented input shapes because nothing said what `source` could be.
    #[test]
    fn every_model_facing_definition_restates_its_own_requirements() {
        let catalog = ToolCatalog::builtin();
        let definitions = catalog.definitions(None);
        assert!(
            definitions.len() > 20,
            "expected the builtin surface, got {}",
            definitions.len()
        );
        for definition in &definitions {
            let description = definition.description.clone().unwrap_or_default();
            let required = definition
                .input_schema
                .get("required")
                .and_then(Value::as_array)
                .map(|required| {
                    required
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let variant_required = definition
                .input_schema
                .get("oneOf")
                .and_then(Value::as_array)
                .map(|variants| {
                    variants
                        .iter()
                        .flat_map(|variant| {
                            variant
                                .get("required")
                                .and_then(Value::as_array)
                                .map(|required| {
                                    required
                                        .iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for field in required.into_iter().chain(variant_required) {
                assert!(
                    description.contains(&field),
                    "a tool requires a field its description never names"
                );
            }
        }
    }

    #[test]
    fn a_tagged_tool_names_the_legal_values_of_its_discriminator() {
        let schema = serde_json::json!({
            "type": "object",
            "oneOf": [
                {"type": "object", "properties": {"source": {"const": "file"}}, "required": ["source"]},
                {"type": "object", "properties": {"source": {"const": "message_block"}}, "required": ["source"]}
            ]
        });
        let description = model_facing_description(Some("publish something"), &schema)
            .expect("a tagged schema is summarizable");
        assert!(
            description.contains("\"file\" | \"message_block\""),
            "{description}"
        );
    }

    /// The tools array is part of the prompt prefix. A description that varies
    /// between two requests would fork that prefix on every turn, which is
    /// precisely what the cache work exists to prevent.
    #[test]
    fn the_same_catalog_renders_the_same_descriptions_twice() {
        let catalog = ToolCatalog::builtin();
        let first = catalog.definitions(None);
        let second = catalog.definitions(None);
        assert_eq!(first.len(), second.len());
        for (left, right) in first.iter().zip(second.iter()) {
            assert_eq!(left.name, right.name);
            assert_eq!(
                left.description, right.description,
                "`{}` is not stable",
                left.name
            );
        }
    }

    /// The descriptor fingerprint hashes the description, so it has to hash the
    /// description the model is actually shown. Hashing the raw one would let a
    /// changed contract keep a stale identity.
    #[test]
    fn the_kernel_definitions_show_the_model_the_same_contract() {
        let catalog = ToolCatalog::builtin();
        let shown: std::collections::BTreeMap<String, Option<String>> = catalog
            .definitions(None)
            .into_iter()
            .map(|definition| (definition.name, definition.description))
            .collect();
        for definition in catalog.kernel_definitions(None) {
            assert_eq!(
                shown.get(&definition.name).cloned().flatten(),
                definition.description,
                "a tool is fingerprinted with a different description than it shows"
            );
        }
    }

    /// `tool_search` answers with names and descriptions, so it is a second
    /// model-facing surface and has to carry the same contract as the tools array.
    #[test]
    fn tool_search_results_carry_the_contract_too() {
        let catalog = ToolCatalog::builtin();
        let specs = catalog.searchable_tool_specs_for_test();
        let submitted = specs
            .iter()
            .find(|spec| spec.name == "bash")
            .expect("bash is searchable");
        assert!(
            submitted
                .description
                .contains("Input contract (from the schema):"),
            "tool_search would advertise a description without the contract"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcp::McpService;
    use std::sync::Arc;

    #[test]
    fn runtime_tool_registration_rejects_unknown_effect_resolvers() {
        let error = ToolCatalog::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "unclassified_runtime_tool".to_string(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.unknown".to_string(),
                    resolver_version: 1,
                },
            }])
            .expect_err("unknown effects must fail during registration");

        assert!(error.contains("unsupported effect resolver"));
        assert!(error.contains("unclassified_runtime_tool"));
    }

    #[test]
    fn catalog_resolves_every_registered_spelling_to_one_canonical_id() {
        let catalog = ToolCatalog::builtin();
        for canonical in catalog.tool_ids() {
            for requested in [
                canonical.to_ascii_uppercase(),
                canonical.replace('_', "-"),
                tool_name_identity(&canonical),
            ] {
                assert_eq!(
                    catalog.canonical_name(&requested).as_deref(),
                    Some(canonical.as_str()),
                    "registered tool `{canonical}` must resolve provider spelling `{requested}`"
                );
            }
        }
        for (requested, canonical) in [
            ("web_search", "web_search"),
            ("web-search", "web_search"),
            ("websearch", "web_search"),
            ("tool_search", "tool_search"),
            ("tool-search", "tool_search"),
            ("GLOB-SEARCH", "glob_search"),
        ] {
            assert_eq!(
                catalog.canonical_name(requested).as_deref(),
                Some(canonical),
                "{requested}"
            );
        }
        assert_eq!(catalog.canonical_name("shell-command"), None);
    }

    #[test]
    fn every_catalog_query_consumes_the_same_canonical_generation() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"subject": {"type": "string"}},
            "required": ["subject"],
        });
        let catalog = ToolCatalog::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "index_probe_tool".into(),
                description: Some("indexed probe".into()),
                input_schema: schema.clone(),
                required_permission: PermissionMode::WorkspaceWrite,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.readonly".into(),
                    resolver_version: 7,
                },
            }])
            .unwrap();

        assert_eq!(
            catalog.canonical_name("INDEX-PROBE-TOOL").as_deref(),
            Some("index_probe_tool")
        );
        assert_eq!(
            catalog.definition("index_probe_tool").unwrap().input_schema,
            schema
        );
        assert_eq!(
            catalog
                .definitions(None)
                .into_iter()
                .find(|definition| definition.name == "index_probe_tool")
                .unwrap()
                .input_schema,
            schema
        );
        assert_eq!(
            catalog
                .kernel_definitions(None)
                .into_iter()
                .find(|definition| definition.name == "index_probe_tool")
                .unwrap()
                .required_permission,
            KernelToolPermissionMode::WorkspaceWrite
        );
        assert_eq!(
            catalog.required_permission("index_probe_tool"),
            Some(KernelToolPermissionMode::WorkspaceWrite)
        );
        assert_eq!(
            catalog
                .permission_specs(None)
                .unwrap()
                .into_iter()
                .find(|(name, _)| name == "index_probe_tool"),
            Some((
                "index_probe_tool".to_string(),
                PermissionMode::WorkspaceWrite
            ))
        );
        assert_eq!(
            catalog.effect_resolver("index_probe_tool"),
            ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".into(),
                resolver_version: 7,
            }
        );
        let descriptor = catalog.descriptor_ref("index_probe_tool").unwrap();
        assert_eq!(descriptor.schema_hash, value_hash(&schema));
        assert_eq!(
            descriptor.required_permission,
            KernelToolPermissionMode::WorkspaceWrite
        );
        assert_eq!(descriptor.source, "runtime");
        assert_eq!(
            catalog.entries.len(),
            catalog.canonical_index.len(),
            "the ordered generation contains exactly one publishable entry per name"
        );
        assert_eq!(
            catalog.entries.len(),
            catalog.identity_index.len(),
            "the alias index contains exactly one publishable entry per normalized identity"
        );

        let description = catalog
            .definition("index_probe_tool")
            .unwrap()
            .description
            .clone();
        let extended = catalog
            .clone()
            .extend_runtime_tools(vec![RuntimeToolDefinition {
                name: "second_index_probe".into(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.readonly".into(),
                    resolver_version: 1,
                },
            }])
            .unwrap();
        assert_eq!(
            extended.definition("index_probe_tool").unwrap().description,
            description,
            "extending a runtime generation must not re-render an existing model contract",
        );
    }

    #[test]
    fn canonical_identity_collision_is_rejected_before_a_generation_is_published() {
        let error = ToolCatalog::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "websearch".into(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.readonly".into(),
                    resolver_version: 1,
                },
            }])
            .expect_err("a normalized collision must not create a second publishable identity");

        assert!(error.contains("canonical tool identity"), "{error}");
        assert!(error.contains("web_search"), "{error}");
    }

    #[test]
    fn plugin_and_runtime_entries_share_the_index_with_stable_exposure_order() {
        let plugin = PluginTool::new(
            "plugin-index",
            "Index plugin",
            plugins::PluginToolDefinition {
                name: "plugin_index_tool".into(),
                description: Some("plugin indexed tool".into()),
                input_schema: serde_json::json!({"type": "object"}),
            },
            "/bin/true",
            Vec::new(),
            plugins::PluginToolPermission::WorkspaceWrite,
            None,
        );
        let catalog = ToolCatalog::with_plugin_tools(vec![plugin])
            .unwrap()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_index_tool".into(),
                description: Some("runtime indexed tool".into()),
                input_schema: serde_json::json!({"type": "object"}),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.readonly".into(),
                    resolver_version: 1,
                },
            }])
            .unwrap();

        assert_eq!(
            catalog.required_permission("plugin-index-tool"),
            Some(KernelToolPermissionMode::WorkspaceWrite)
        );
        assert_eq!(
            catalog.required_permission("runtime-index-tool"),
            Some(KernelToolPermissionMode::ReadOnly)
        );
        let definitions = catalog
            .definitions(None)
            .into_iter()
            .map(|definition| definition.name)
            .collect::<Vec<_>>();
        let runtime = definitions
            .iter()
            .position(|name| name == "runtime_index_tool")
            .unwrap();
        let plugin = definitions
            .iter()
            .position(|name| name == "plugin_index_tool")
            .unwrap();
        assert!(
            runtime < plugin,
            "runtime definitions retain precedence before plugin definitions"
        );
        assert_eq!(catalog.entry_order.len(), catalog.entries.len());
    }

    #[test]
    fn runtime_tool_registration_rejects_builtin_identity_collisions() {
        let error = ToolCatalog::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "web_search".to_string(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
                required_permission: PermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "network.read".to_string(),
                    resolver_version: 1,
                },
            }])
            .expect_err("builtin identity must not acquire a second authorization contract");
        assert!(error.contains("existing tool name"));
        assert!(error.contains("web_search"));
    }

    #[test]
    fn extending_runtime_tools_retains_existing_core_definitions() {
        let definition = |name: &str| RuntimeToolDefinition {
            name: name.to_string(),
            description: None,
            input_schema: serde_json::json!({"type": "object"}),
            required_permission: PermissionMode::ReadOnly,
            effect_resolver: ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".to_string(),
                resolver_version: 1,
            },
        };
        let catalog = ToolCatalog::builtin()
            .with_runtime_tools(vec![definition("team_create")])
            .expect("core runtime tool")
            .extend_runtime_tools(vec![definition("mcp__alpha__echo")])
            .expect("MCP runtime tool");

        assert_eq!(
            catalog.required_permission("team_create"),
            Some(KernelToolPermissionMode::ReadOnly)
        );
        assert_eq!(
            catalog.required_permission("mcp__alpha__echo"),
            Some(KernelToolPermissionMode::ReadOnly)
        );
    }

    /// PLAN B-4: externally discovered MCP tools are reachable through
    /// `mcp(action=call)` (and orchestration), but they must NOT enter the
    /// provider tool array - activating one used to rewrite that array and
    /// invalidate the whole prompt cache from the tools onward.
    #[test]
    fn externally_discovered_mcp_tools_stay_out_of_the_provider_definitions() {
        let definition = |name: &str| RuntimeToolDefinition {
            name: name.to_string(),
            description: None,
            input_schema: serde_json::json!({ "type": "object" }),
            required_permission: PermissionMode::ReadOnly,
            effect_resolver: ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".to_string(),
                resolver_version: 1,
            },
        };
        let catalog = ToolCatalog::builtin()
            .with_runtime_tools(vec![definition("team_create")])
            .expect("core runtime tool")
            .extend_runtime_tools(vec![definition("mcp__alpha__echo")])
            .expect("MCP runtime tool");

        let names = catalog
            .definitions(None)
            .into_iter()
            .map(|definition| definition.name)
            .collect::<BTreeSet<_>>();
        assert!(
            names.contains("team_create"),
            "core runtime tools stay in the provider array"
        );
        assert!(
            !names.contains("mcp__alpha__echo"),
            "an MCP tool must not enter the provider tool array"
        );
        // It stays resolvable, so `mcp(action=call)` and orchestration reach it.
        assert_eq!(
            catalog.canonical_name("mcp__alpha__echo").as_deref(),
            Some("mcp__alpha__echo")
        );
        assert!(catalog.required_permission("mcp__alpha__echo").is_some());
    }

    /// PLAN B-4, at scale: the existing guard proves ONE MCP tool stays out of
    /// the provider array. A bigger server (or a hot-reconnected one) is the
    /// case that actually matters, because the array sits inside cache
    /// breakpoint 2 - if a single MCP schema ever re-entered it, every message
    /// after it would be re-sent for the rest of the session. This pins the
    /// invariant byte-for-byte at a size no single server will approach.
    #[test]
    fn adding_any_number_of_mcp_tools_never_changes_the_provider_tool_array() {
        let definition = |name: &str| RuntimeToolDefinition {
            name: name.to_string(),
            description: None,
            input_schema: serde_json::json!({ "type": "object" }),
            required_permission: PermissionMode::ReadOnly,
            effect_resolver: ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".to_string(),
                resolver_version: 1,
            },
        };
        let base = ToolCatalog::builtin()
            .with_runtime_tools(vec![definition("team_create")])
            .expect("core runtime tool");
        let discovered = (0..256usize)
            .map(|index| definition(&format!("mcp__server{index}__tool")))
            .collect();
        let with_mcp = ToolCatalog::builtin()
            .with_runtime_tools(vec![definition("team_create")])
            .expect("core runtime tool")
            .extend_runtime_tools(discovered)
            .expect("MCP runtime tools");

        // Render the two things that actually reach the wire: ordered names and
        // each input schema. Comparing these, not the structs, keeps the test
        // honest about bytes and not about incidental fields.
        let array = |catalog: &ToolCatalog| -> Vec<String> {
            catalog
                .definitions(None)
                .into_iter()
                .map(|entry| format!("{}:{:?}", entry.name, entry.input_schema))
                .collect()
        };
        assert_eq!(
            array(&base),
            array(&with_mcp),
            "256 MCP tools must not move a single byte of the provider tool array"
        );
        // They stay resolvable, which is the whole point of keeping them in the
        // registry while keeping them out of the prompt.
        assert_eq!(
            with_mcp.canonical_name("mcp__server255__tool").as_deref(),
            Some("mcp__server255__tool")
        );
        assert!(with_mcp.required_permission("mcp__server0__tool").is_some());
    }

    #[derive(Debug)]
    struct FakeMcpService {
        name: &'static str,
    }

    impl McpService for FakeMcpService {
        fn list_servers(&self) -> Result<Vec<mcp::McpServerProjection>, mcp::McpServiceError> {
            Ok(vec![self.server(self.name)?])
        }

        fn server(&self, name: &str) -> Result<mcp::McpServerProjection, mcp::McpServiceError> {
            Ok(mcp::McpServerProjection {
                name: name.to_string(),
                transport: mcp::McpTransportKind::ManagedProxy,
                enabled: true,
                status: "ready".to_string(),
                auth_state: None,
            })
        }

        fn health(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
            Ok(serde_json::json!({ "name": self.name }))
        }

        fn reload_config(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
            Ok(serde_json::json!({ "ok": true }))
        }

        fn list_tools(
            &self,
            _server: Option<&str>,
        ) -> Result<Vec<mcp::McpToolProjection>, mcp::McpServiceError> {
            Ok(Vec::new())
        }

        fn list_resources(
            &self,
            _server: Option<&str>,
        ) -> Result<Vec<mcp::McpResourceProjection>, mcp::McpServiceError> {
            Ok(Vec::new())
        }

        fn read_resource(
            &self,
            server: &str,
            uri: &str,
        ) -> Result<mcp::McpResourceProjection, mcp::McpServiceError> {
            Ok(mcp::McpResourceProjection {
                server: server.to_string(),
                uri: uri.to_string(),
                name: None,
                mime_type: None,
                content: None,
            })
        }

        fn call_tool(
            &self,
            request: mcp::McpToolCallRequest,
        ) -> Result<mcp::McpToolCallReceipt, mcp::McpServiceError> {
            Ok(mcp::McpToolCallReceipt {
                server: request.server,
                tool: request.tool,
                ok: true,
                output: serde_json::json!({ "name": self.name }),
            })
        }
    }

    #[test]
    fn mcp_service_is_replaced_by_snapshot_without_changing_pinned_request() {
        let host = ToolHost::new(
            "workspace",
            "/tmp/workspace",
            ToolHostSnapshot::new(
                Arc::new(ToolCatalog::builtin()),
                Arc::new(lsp_client::LspRegistry::new()),
                Some(Arc::new(FakeMcpService { name: "first" })),
            ),
        );
        let pinned = host.pin_snapshot();
        host.replace_snapshot(ToolHostSnapshot::new(
            Arc::new(ToolCatalog::builtin()),
            Arc::new(lsp_client::LspRegistry::new()),
            Some(Arc::new(FakeMcpService { name: "second" })),
        ));
        assert_eq!(
            pinned.snapshot().mcp.as_ref().unwrap().health().unwrap()["name"],
            "first"
        );
        assert_eq!(
            host.pin_snapshot()
                .snapshot()
                .mcp
                .as_ref()
                .unwrap()
                .health()
                .unwrap()["name"],
            "second"
        );
    }
}
