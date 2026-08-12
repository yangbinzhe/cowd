use std::collections::BTreeMap;
use std::path::Path;

use crate::config::ConfigError;
use crate::json::JsonValue;

/// Diagnostic emitted when a config file contains a suspect field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDiagnostic {
    pub path: String,
    pub field: String,
    pub line: Option<usize>,
    pub kind: DiagnosticKind,
}

/// Classification of the diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiagnosticKind {
    UnknownKey {
        suggestion: Option<String>,
    },
    WrongType {
        expected: &'static str,
        got: &'static str,
    },
    Deprecated {
        replacement: &'static str,
    },
}

impl std::fmt::Display for ConfigDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let location = self
            .line
            .map_or_else(String::new, |line| format!(" (line {line})"));
        match &self.kind {
            DiagnosticKind::UnknownKey { suggestion: None } => {
                write!(f, "{}: unknown key \"{}\"{location}", self.path, self.field)
            }
            DiagnosticKind::UnknownKey {
                suggestion: Some(hint),
            } => {
                write!(
                    f,
                    "{}: unknown key \"{}\"{location}. Did you mean \"{}\"?",
                    self.path, self.field, hint
                )
            }
            DiagnosticKind::WrongType { expected, got } => {
                write!(
                    f,
                    "{}: field \"{}\" must be {expected}, got {got}{location}",
                    self.path, self.field
                )
            }
            DiagnosticKind::Deprecated { replacement } => {
                write!(
                    f,
                    "{}: field \"{}\" is deprecated{location}. Use \"{replacement}\" instead",
                    self.path, self.field
                )
            }
        }
    }
}

/// Result of validating a single config file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationResult {
    pub errors: Vec<ConfigDiagnostic>,
    pub warnings: Vec<ConfigDiagnostic>,
}

impl ValidationResult {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    fn merge(&mut self, other: Self) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }
}

// ---- known-key schema ----

/// Expected type for a config field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldType {
    String,
    Bool,
    Object,
    StringArray,
    Number,
    #[allow(dead_code)]
    ObjectOrObjectArray,
}

impl FieldType {
    fn label(self) -> &'static str {
        match self {
            Self::String => "a string",
            Self::Bool => "a boolean",
            Self::Object => "an object",
            Self::StringArray => "an array of strings",
            Self::Number => "a number",
            Self::ObjectOrObjectArray => "an object or an array of objects",
        }
    }

    fn matches(self, value: &JsonValue) -> bool {
        match self {
            Self::String => value.as_str().is_some(),
            Self::Bool => value.as_bool().is_some(),
            Self::Object => value.as_object().is_some(),
            Self::StringArray => value
                .as_array()
                .is_some_and(|arr| arr.iter().all(|v| v.as_str().is_some())),
            Self::Number => value.as_i64().is_some(),
            Self::ObjectOrObjectArray => {
                value.as_object().is_some()
                    || value
                        .as_array()
                        .is_some_and(|arr| arr.iter().all(|v| v.as_object().is_some()))
            }
        }
    }
}

fn json_type_label(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "a boolean",
        JsonValue::Number(_) => "a number",
        JsonValue::String(_) => "a string",
        JsonValue::Array(_) => "an array",
        JsonValue::Object(_) => "an object",
    }
}

struct FieldSpec {
    name: &'static str,
    expected: FieldType,
}

struct DeprecatedField {
    name: &'static str,
    replacement: &'static str,
}

const TOP_LEVEL_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "$schema",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "model",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "workspace",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "hooks",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "permissions",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "approval",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "permissionMode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "permission_mode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "mcpServers",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "mcp_servers",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "mcp",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "oauth",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "enabledPlugins",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "enabled_plugins",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "plugins",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "sandbox",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "env",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "aliases",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "fallbacks",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "trustedRoots",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "trusted_roots",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "compression",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "providers",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "memory",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "gateway",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "platforms",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "runtime",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "context_budget",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "model_context_windows",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "apps",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "storage",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "gateAutoFix",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "network",
        expected: FieldType::Object,
    },
];

const HOOKS_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "pre_tool_use",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "PreToolUse",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "post_tool_use",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "PostToolUse",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "post_tool_use_failure",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "PostToolUseFailure",
        expected: FieldType::StringArray,
    },
];

const PERMISSIONS_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "defaultMode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "default_mode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "allow",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "deny",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "ask",
        expected: FieldType::StringArray,
    },
];

const PLUGINS_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "enabled",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "externalDirectories",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "external_directories",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "external_dirs",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "installRoot",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "install_root",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "registryPath",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "registry_path",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "bundledRoot",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "bundled_root",
        expected: FieldType::String,
    },
];

const SANDBOX_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "enabled",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "namespaceRestrictions",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "namespace_restrictions",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "networkIsolation",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "network_isolation",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "isolate_network",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "filesystemMode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "filesystem_mode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "workspaceRoot",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "workspace_root",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "allowedMounts",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "allowed_mounts",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "allowed_dirs",
        expected: FieldType::StringArray,
    },
];

const OAUTH_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "clientId",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "client_id",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "authorizeUrl",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "authorize_url",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "tokenUrl",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "token_url",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "callbackPort",
        expected: FieldType::Number,
    },
    FieldSpec {
        name: "callback_port",
        expected: FieldType::Number,
    },
    FieldSpec {
        name: "manualRedirectUrl",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "manual_redirect_url",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "scopes",
        expected: FieldType::StringArray,
    },
];

const COMPRESSION_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "circuitBreaker",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "circuit_breaker",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "micro",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "session",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "deep",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "smart",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "fast",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "cheap",
        expected: FieldType::Object,
    },
];

const MCP_FIELDS: &[FieldSpec] = &[FieldSpec {
    name: "servers",
    expected: FieldType::Object,
}];

const NETWORK_FIELDS: &[FieldSpec] = &[FieldSpec {
    name: "domain",
    expected: FieldType::Object,
}];

const NETWORK_DOMAIN_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "mode",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "allow",
        expected: FieldType::StringArray,
    },
    FieldSpec {
        name: "block",
        expected: FieldType::StringArray,
    },
];

const APPROVAL_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "profile",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "low_risk_timeout",
        expected: FieldType::String,
    },
];

const RUNTIME_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "scenario",
        expected: FieldType::String,
    },
    FieldSpec {
        name: "resources",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "hot_state",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "control",
        expected: FieldType::Object,
    },
];

const RUNTIME_CONTROL_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "enabled",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "agent",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "task",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "context",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "memory",
        expected: FieldType::Object,
    },
    FieldSpec {
        name: "observability",
        expected: FieldType::Object,
    },
];

const RUNTIME_CONTROL_AGENT_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "enabled",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "max_parallel_agents",
        expected: FieldType::Number,
    },
    FieldSpec {
        name: "review_on_conflict",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "require_positive_lift",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "min_collaboration_score",
        expected: FieldType::Number,
    },
];

const RUNTIME_CONTROL_TASK_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "auto_phase_for_yolo",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "review_after_each_phase",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "max_failures_before_review",
        expected: FieldType::Number,
    },
];

const RUNTIME_CONTROL_CONTEXT_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "preserve_stable_head",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "yolo_budget_tokens",
        expected: FieldType::Number,
    },
    FieldSpec {
        name: "collaboration_budget_tokens",
        expected: FieldType::Number,
    },
    FieldSpec {
        name: "review_budget_tokens",
        expected: FieldType::Number,
    },
    FieldSpec {
        name: "degrade_on_pressure_bp",
        expected: FieldType::Number,
    },
];

const RUNTIME_CONTROL_MEMORY_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "emit_pulses_from_execution_graph",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "review_conflicts",
        expected: FieldType::Bool,
    },
    FieldSpec {
        name: "max_candidates_per_turn",
        expected: FieldType::Number,
    },
];

const DEPRECATED_FIELDS: &[DeprecatedField] = &[
    DeprecatedField {
        name: "permissionMode",
        replacement: "permissions.default_mode",
    },
    DeprecatedField {
        name: "enabledPlugins",
        replacement: "plugins.enabled",
    },
];

// ---- line-number resolution ----

/// Find the 1-based line number where a JSON key first appears in the raw source.
fn find_key_line(source: &str, key: &str) -> Option<usize> {
    // Search for `"key"` followed by optional whitespace and a colon.
    let needle = format!("\"{key}\"");
    let mut search_start = 0;
    while let Some(offset) = source[search_start..].find(&needle) {
        let absolute = search_start + offset;
        let after = absolute + needle.len();
        // Verify the next non-whitespace char is `:` to confirm this is a key, not a value.
        if source[after..].chars().find(|ch| !ch.is_ascii_whitespace()) == Some(':') {
            return Some(source[..absolute].chars().filter(|&ch| ch == '\n').count() + 1);
        }
        search_start = after;
    }
    None
}

// ---- core validation ----

fn validate_object_keys(
    object: &BTreeMap<String, JsonValue>,
    known_fields: &[FieldSpec],
    prefix: &str,
    source: &str,
    path_display: &str,
) -> ValidationResult {
    let mut result = ValidationResult {
        errors: Vec::new(),
        warnings: Vec::new(),
    };

    let known_names: Vec<&str> = known_fields.iter().map(|f| f.name).collect();

    for (key, value) in object {
        let field_path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        if let Some(spec) = known_fields.iter().find(|f| f.name == key) {
            // Type check — null values are acceptable for any field (explicitly unset)
            if !matches!(value, JsonValue::Null) && !spec.expected.matches(value) {
                result.errors.push(ConfigDiagnostic {
                    path: path_display.to_string(),
                    field: field_path,
                    line: find_key_line(source, key),
                    kind: DiagnosticKind::WrongType {
                        expected: spec.expected.label(),
                        got: json_type_label(value),
                    },
                });
            }
        } else if DEPRECATED_FIELDS.iter().any(|d| d.name == key) {
            // Deprecated key — handled separately, not an unknown-key error.
        } else {
            // Unknown key — warn but don't reject the config.
            let suggestion = suggest_field(key, &known_names);
            result.warnings.push(ConfigDiagnostic {
                path: path_display.to_string(),
                field: field_path,
                line: find_key_line(source, key),
                kind: DiagnosticKind::UnknownKey { suggestion },
            });
        }
    }

    result
}

fn suggest_field(input: &str, candidates: &[&str]) -> Option<String> {
    let input_lower = input.to_ascii_lowercase();
    candidates
        .iter()
        .filter_map(|candidate| {
            let distance = simple_edit_distance(&input_lower, &candidate.to_ascii_lowercase());
            (distance <= 3).then_some((distance, *candidate))
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, name)| name.to_string())
}

fn simple_edit_distance(left: &str, right: &str) -> usize {
    if left.is_empty() {
        return right.len();
    }
    if right.is_empty() {
        return left.len();
    }
    let right_chars: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right_chars.len()).collect();
    let mut current = vec![0; right_chars.len() + 1];

    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right_chars.iter().enumerate() {
            let cost = usize::from(left_char != *right_char);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + cost);
        }
        previous.clone_from(&current);
    }

    previous[right_chars.len()]
}

/// Validate a parsed config file's keys and types against the known schema.
///
/// Returns diagnostics (errors and deprecation warnings) without blocking the load.
pub fn validate_config_file(
    object: &BTreeMap<String, JsonValue>,
    source: &str,
    file_path: &Path,
) -> ValidationResult {
    let path_display = file_path.display().to_string();
    let mut result = validate_object_keys(object, TOP_LEVEL_FIELDS, "", source, &path_display);

    // Check deprecated fields.
    for deprecated in DEPRECATED_FIELDS {
        if object.contains_key(deprecated.name) {
            result.warnings.push(ConfigDiagnostic {
                path: path_display.clone(),
                field: deprecated.name.to_string(),
                line: find_key_line(source, deprecated.name),
                kind: DiagnosticKind::Deprecated {
                    replacement: deprecated.replacement,
                },
            });
        }
    }

    // Validate known nested objects.
    if let Some(hooks) = object.get("hooks").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            hooks,
            HOOKS_FIELDS,
            "hooks",
            source,
            &path_display,
        ));
    }
    if let Some(permissions) = object.get("permissions").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            permissions,
            PERMISSIONS_FIELDS,
            "permissions",
            source,
            &path_display,
        ));
    }
    if let Some(plugins) = object.get("plugins").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            plugins,
            PLUGINS_FIELDS,
            "plugins",
            source,
            &path_display,
        ));
    }
    if let Some(sandbox) = object.get("sandbox").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            sandbox,
            SANDBOX_FIELDS,
            "sandbox",
            source,
            &path_display,
        ));
    }
    if let Some(oauth) = object.get("oauth").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            oauth,
            OAUTH_FIELDS,
            "oauth",
            source,
            &path_display,
        ));
    }
    if let Some(compression) = object.get("compression").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            compression,
            COMPRESSION_FIELDS,
            "compression",
            source,
            &path_display,
        ));
    }
    if let Some(mcp) = object.get("mcp").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            mcp,
            MCP_FIELDS,
            "mcp",
            source,
            &path_display,
        ));
    }
    if let Some(network) = object.get("network").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            network,
            NETWORK_FIELDS,
            "network",
            source,
            &path_display,
        ));
        if let Some(domain) = network.get("domain").and_then(JsonValue::as_object) {
            result.merge(validate_object_keys(
                domain,
                NETWORK_DOMAIN_FIELDS,
                "network.domain",
                source,
                &path_display,
            ));
            if let Some(mode) = domain.get("mode").and_then(JsonValue::as_str) {
                if !matches!(
                    mode.trim().to_ascii_lowercase().as_str(),
                    "allow" | "ask" | "deny"
                ) {
                    result.errors.push(ConfigDiagnostic {
                        path: path_display.clone(),
                        field: "network.domain.mode".to_string(),
                        line: find_key_line(source, "mode"),
                        kind: DiagnosticKind::WrongType {
                            expected: "one of allow|ask|deny",
                            got: "an invalid mode string",
                        },
                    });
                }
            }
        }
    }
    if let Some(approval) = object.get("approval").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            approval,
            APPROVAL_FIELDS,
            "approval",
            source,
            &path_display,
        ));
    }
    if let Some(runtime) = object.get("runtime").and_then(JsonValue::as_object) {
        result.merge(validate_object_keys(
            runtime,
            RUNTIME_FIELDS,
            "runtime",
            source,
            &path_display,
        ));
        if let Some(control) = runtime.get("control").and_then(JsonValue::as_object) {
            result.merge(validate_object_keys(
                control,
                RUNTIME_CONTROL_FIELDS,
                "runtime.control",
                source,
                &path_display,
            ));
            if let Some(agent) = control.get("agent").and_then(JsonValue::as_object) {
                result.merge(validate_object_keys(
                    agent,
                    RUNTIME_CONTROL_AGENT_FIELDS,
                    "runtime.control.agent",
                    source,
                    &path_display,
                ));
            }
            if let Some(task) = control.get("task").and_then(JsonValue::as_object) {
                result.merge(validate_object_keys(
                    task,
                    RUNTIME_CONTROL_TASK_FIELDS,
                    "runtime.control.task",
                    source,
                    &path_display,
                ));
            }
            if let Some(context) = control.get("context").and_then(JsonValue::as_object) {
                result.merge(validate_object_keys(
                    context,
                    RUNTIME_CONTROL_CONTEXT_FIELDS,
                    "runtime.control.context",
                    source,
                    &path_display,
                ));
            }
            if let Some(memory) = control.get("memory").and_then(JsonValue::as_object) {
                result.merge(validate_object_keys(
                    memory,
                    RUNTIME_CONTROL_MEMORY_FIELDS,
                    "runtime.control.memory",
                    source,
                    &path_display,
                ));
            }
        }
    }

    result
}

/// Check whether a file path uses an unsupported config format (e.g. TOML).
pub fn check_unsupported_format(file_path: &Path) -> Result<(), ConfigError> {
    if let Some(ext) = file_path.extension().and_then(|e| e.to_str()) {
        if ext.eq_ignore_ascii_case("toml") {
            return Err(ConfigError::Parse(format!(
                "{}: TOML config files are not supported. Use YAML or JSON instead",
                file_path.display()
            )));
        }
    }
    Ok(())
}

/// Format all diagnostics into a human-readable report.
#[must_use]
pub fn format_diagnostics(result: &ValidationResult) -> String {
    let mut lines = Vec::new();
    for warning in &result.warnings {
        lines.push(format!("warning: {warning}"));
    }
    for error in &result.errors {
        lines.push(format!("error: {error}"));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_path() -> PathBuf {
        PathBuf::from("/test/config.yaml")
    }

    #[test]
    fn detects_unknown_top_level_key() {
        // given
        let source = r#"{"model": "opus", "unknownField": true}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "unknownField");
        assert!(matches!(
            result.warnings[0].kind,
            DiagnosticKind::UnknownKey { .. }
        ));
    }

    #[test]
    fn detects_wrong_type_for_model() {
        // given
        let source = r#"{"model": 123}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].field, "model");
        assert!(matches!(
            result.errors[0].kind,
            DiagnosticKind::WrongType {
                expected: "a string",
                got: "a number"
            }
        ));
    }

    #[test]
    fn detects_deprecated_permission_mode() {
        // given
        let source = r#"{"permissionMode": "plan"}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "permissionMode");
        assert!(matches!(
            result.warnings[0].kind,
            DiagnosticKind::Deprecated {
                replacement: "permissions.default_mode"
            }
        ));
    }

    #[test]
    fn detects_deprecated_enabled_plugins() {
        // given
        let source = r#"{"enabledPlugins": {"tool-guard@builtin": true}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "enabledPlugins");
        assert!(matches!(
            result.warnings[0].kind,
            DiagnosticKind::Deprecated {
                replacement: "plugins.enabled"
            }
        ));
    }

    #[test]
    fn accepts_runtime_control_schema() {
        let source = r#"{
          "runtime": {
            "scenario": "coding",
            "control": {
              "enabled": true,
              "agent": {"enabled": true, "max_parallel_agents": 4, "min_collaboration_score": 50},
              "task": {"auto_phase_for_yolo": true, "max_failures_before_review": 2},
              "context": {"preserve_stable_head": true, "yolo_budget_tokens": 12000},
              "memory": {"emit_pulses_from_execution_graph": true, "max_candidates_per_turn": 8}
            }
          }
        }"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        let result = validate_config_file(object, source, &test_path());

        assert!(result.errors.is_empty(), "{:?}", result.errors);
        assert!(result.warnings.is_empty(), "{:?}", result.warnings);
    }

    #[test]
    fn reports_line_number_for_unknown_key() {
        // given
        let source = "{\n  \"model\": \"opus\",\n  \"badKey\": true\n}";
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].line, Some(3));
        assert_eq!(result.warnings[0].field, "badKey");
    }

    #[test]
    fn reports_line_number_for_wrong_type() {
        // given
        let source = "{\n  \"model\": 42\n}";
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].line, Some(2));
    }

    #[test]
    fn validates_nested_hooks_keys() {
        // given
        let source = r#"{"hooks": {"PreToolUse": ["cmd"], "BadHook": ["x"]}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "hooks.BadHook");
    }

    #[test]
    fn validates_nested_permissions_keys() {
        // given
        let source = r#"{"permissions": {"allow": ["Read"], "denyAll": true}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "permissions.denyAll");
    }

    #[test]
    fn accepts_only_the_top_level_approval_contract() {
        let source = r#"{
            "approval": {"profile": "balanced", "low_risk_timeout": "auto_approve_once"},
            "permissions": {"approval": {"profile": "autonomous"}}
        }"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        let result = validate_config_file(object, source, &test_path());

        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "permissions.approval");
    }

    #[test]
    fn validates_nested_sandbox_keys() {
        // given
        let source = r#"{"sandbox": {"enabled": true, "containerMode": "strict"}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "sandbox.containerMode");
    }

    #[test]
    fn validates_nested_plugins_keys() {
        // given
        let source = r#"{"plugins": {"install_root": "/tmp", "autoUpdate": true}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "plugins.autoUpdate");
    }

    #[test]
    fn validates_nested_oauth_keys() {
        // given
        let source = r#"{"oauth": {"client_id": "abc", "secret": "hidden"}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].field, "oauth.secret");
    }

    #[test]
    fn valid_config_produces_no_diagnostics() {
        // given
        let source = r#"{
  "model": "opus",
  "hooks": {"PreToolUse": ["guard"]},
  "permissions": {"default_mode": "plan", "allow": ["Read"]},
  "mcpServers": {},
  "sandbox": {"enabled": false}
}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert!(result.is_ok());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn suggests_close_field_name() {
        // given
        let source = r#"{"modle": "opus"}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.warnings.len(), 1);
        match &result.warnings[0].kind {
            DiagnosticKind::UnknownKey {
                suggestion: Some(s),
            } => assert_eq!(s, "model"),
            other => panic!("expected suggestion, got {other:?}"),
        }
    }

    #[test]
    fn format_diagnostics_includes_all_entries() {
        // given
        let source = r#"{"permissionMode": "plan", "badKey": 1}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");
        let result = validate_config_file(object, source, &test_path());

        // when
        let output = format_diagnostics(&result);

        // then
        assert!(output.contains("warning:"));
        assert!(output.contains("badKey"));
        assert!(output.contains("permissionMode"));
    }

    #[test]
    fn check_unsupported_format_rejects_toml() {
        // given
        let path = PathBuf::from("/home/.cowd/settings.toml");

        // when
        let result = check_unsupported_format(&path);

        // then
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("TOML"));
        assert!(message.contains("settings.toml"));
    }

    #[test]
    fn check_unsupported_format_allows_json() {
        // given
        let path = PathBuf::from("/home/.cowd/config.yaml");

        // when / then
        assert!(check_unsupported_format(&path).is_ok());
    }

    #[test]
    fn wrong_type_in_nested_sandbox_field() {
        // given
        let source = r#"{"sandbox": {"enabled": "yes"}}"#;
        let parsed = JsonValue::parse(source).expect("valid json");
        let object = parsed.as_object().expect("object");

        // when
        let result = validate_config_file(object, source, &test_path());

        // then
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].field, "sandbox.enabled");
        assert!(matches!(
            result.errors[0].kind,
            DiagnosticKind::WrongType {
                expected: "a boolean",
                got: "a string"
            }
        ));
    }

    #[test]
    fn display_format_unknown_key_with_line() {
        // given
        let diag = ConfigDiagnostic {
            path: "/test/config.yaml".to_string(),
            field: "badKey".to_string(),
            line: Some(5),
            kind: DiagnosticKind::UnknownKey { suggestion: None },
        };

        // when
        let output = diag.to_string();

        // then
        assert_eq!(
            output,
            r#"/test/config.yaml: unknown key "badKey" (line 5)"#
        );
    }

    #[test]
    fn display_format_wrong_type_with_line() {
        // given
        let diag = ConfigDiagnostic {
            path: "/test/config.yaml".to_string(),
            field: "model".to_string(),
            line: Some(2),
            kind: DiagnosticKind::WrongType {
                expected: "a string",
                got: "a number",
            },
        };

        // when
        let output = diag.to_string();

        // then
        assert_eq!(
            output,
            r#"/test/config.yaml: field "model" must be a string, got a number (line 2)"#
        );
    }

    #[test]
    fn display_format_deprecated_with_line() {
        // given
        let diag = ConfigDiagnostic {
            path: "/test/config.yaml".to_string(),
            field: "permissionMode".to_string(),
            line: Some(3),
            kind: DiagnosticKind::Deprecated {
                replacement: "permissions.default_mode",
            },
        };

        // when
        let output = diag.to_string();

        // then
        assert_eq!(
            output,
            r#"/test/config.yaml: field "permissionMode" is deprecated (line 3). Use "permissions.default_mode" instead"#
        );
    }

    #[test]
    fn validates_network_domain_mode_enum() {
        let valid_source = r#"{"network":{"domain":{"mode":"deny","allow":["docs.rs"],"block":["evil.example"]}}}"#;
        let valid = JsonValue::parse(valid_source).expect("valid json");
        let valid_result = validate_config_file(
            valid.as_object().expect("object"),
            valid_source,
            &test_path(),
        );
        assert!(valid_result.errors.is_empty());
        assert!(valid_result.warnings.is_empty());

        let invalid_source = r#"{"network":{"domain":{"mode":"denny"}}}"#;
        let invalid = JsonValue::parse(invalid_source).expect("invalid-mode json");
        let invalid_result = validate_config_file(
            invalid.as_object().expect("object"),
            invalid_source,
            &test_path(),
        );
        assert_eq!(invalid_result.errors.len(), 1);
        assert_eq!(invalid_result.errors[0].field, "network.domain.mode");
        assert!(matches!(
            invalid_result.errors[0].kind,
            DiagnosticKind::WrongType {
                expected: "one of allow|ask|deny",
                ..
            }
        ));
    }
}
