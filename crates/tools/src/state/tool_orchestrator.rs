//! Intrinsic tool-effect preflight.
//!
//! This module describes implementation effects only. Runtime owns policy,
//! approval, concurrency, timeout and result-budget decisions.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
use harness_contract::tool::{
    ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolEffectResolverSpec,
    ToolIdempotency, ToolPermissionMode,
};
use serde_json::Value;

/// Derive the effective effect from the concrete tool input.
#[must_use]
pub fn resolve_registered_tool_effect(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    catalog_permission: ToolPermissionMode,
) -> ToolEffectDescriptor {
    let mut properties = resolve_effect_properties(resolver, tool_id, input);
    properties.required_permission =
        stricter_permission(properties.required_permission, catalog_permission);

    let descriptor_hash = descriptor_hash(tool_id, input, &properties);
    ToolEffectDescriptor {
        tool_id: tool_id.to_string(),
        descriptor_hash,
        effect_kind: properties.effect_kind,
        idempotency: properties.idempotency,
        scopes: properties.scopes,
        required_permission: properties.required_permission,
        approval_class: properties.approval_class,
        uses_network: properties.uses_network,
        spawns_process: properties.spawns_process,
        mutates_packages: properties.mutates_packages,
        mutates_system: properties.mutates_system,
    }
}

#[derive(Debug)]
struct EffectProperties {
    effect_kind: ToolEffectKind,
    idempotency: ToolIdempotency,
    scopes: Vec<PermissionScope>,
    required_permission: ToolPermissionMode,
    approval_class: ToolApprovalClass,
    uses_network: bool,
    spawns_process: bool,
    mutates_packages: bool,
    mutates_system: bool,
}

fn resolve_effect_properties(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
) -> EffectProperties {
    let target = resource_target(input);
    let read_scope = || {
        scope(
            PermissionResource::Tool,
            PermissionOperation::Read,
            target.clone(),
        )
    };
    let write_scope = || {
        scope(
            PermissionResource::Tool,
            PermissionOperation::Write,
            target.clone(),
        )
    };

    match resolver.resolver_id.as_str() {
        "builtin.command" => {
            let command = input
                .get("command")
                .or_else(|| input.get("code"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            command_effect(command)
        }
        "builtin.readonly" | "runtime.readonly" => EffectProperties {
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![read_scope()],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
        "builtin.readonly_process" => EffectProperties {
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![read_scope()],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
        "builtin.workspace_write" | "runtime.state_write" => EffectProperties {
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![write_scope()],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
        "builtin.network" => EffectProperties {
            effect_kind: ToolEffectKind::Network,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![scope(
                PermissionResource::Network,
                PermissionOperation::Call,
                target,
            )],
            required_permission: ToolPermissionMode::DangerFullAccess,
            approval_class: ToolApprovalClass::Policy,
            uses_network: true,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
        "runtime.external_read" => EffectProperties {
            effect_kind: ToolEffectKind::Network,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope(
                PermissionResource::Network,
                PermissionOperation::Call,
                target,
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: true,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
        "runtime.external_write" => EffectProperties {
            effect_kind: ToolEffectKind::Network,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![scope(
                PermissionResource::Network,
                PermissionOperation::Call,
                target,
            )],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::User,
            uses_network: true,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
        "runtime.external_danger" => EffectProperties {
            effect_kind: ToolEffectKind::Network,
            idempotency: ToolIdempotency::NonIdempotent,
            scopes: vec![scope(
                PermissionResource::Network,
                PermissionOperation::Call,
                target,
            )],
            required_permission: ToolPermissionMode::DangerFullAccess,
            approval_class: ToolApprovalClass::User,
            uses_network: true,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
        "builtin.process" => EffectProperties {
            effect_kind: ToolEffectKind::Process,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Execute,
                target,
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
        "builtin.external_unknown" => EffectProperties {
            effect_kind: ToolEffectKind::Unknown,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Call,
                target,
            )],
            required_permission: ToolPermissionMode::DangerFullAccess,
            approval_class: ToolApprovalClass::User,
            uses_network: true,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
        _ => conservative_unknown(tool_id_target(tool_id, input)),
    }
}

fn command_effect(command: &str) -> EffectProperties {
    let normalized = command.to_ascii_lowercase();
    let mut uses_network =
        contains_word(&normalized, &["curl", "wget", "ssh", "scp", "rsync", "nc"]);
    let mutates_packages = contains_sequence(
        &normalized,
        &[
            "cargo add",
            "cargo install",
            "npm install",
            "pnpm add",
            "yarn add",
            "pip install",
            "apt install",
            "dnf install",
            "brew install",
        ],
    );
    let mutates_system = contains_word(
        &normalized,
        &["sudo", "systemctl", "mount", "umount", "reboot", "shutdown"],
    );
    let destructive = contains_word(&normalized, &["rm", "kill"])
        || contains_sequence(
            &normalized,
            &[
                "rm -",
                "git reset --hard",
                "git clean -",
                "drop table",
                "truncate table",
                "mkfs",
                "dd if=",
            ],
        );
    let writes = destructive
        || mutates_packages
        || normalized.contains(" >")
        || normalized.contains(">>")
        || contains_word(
            &normalized,
            &["mv", "cp", "touch", "mkdir", "chmod", "chown", "sed"],
        );

    if mutates_packages {
        uses_network = true;
    }
    let effect_kind = if destructive {
        ToolEffectKind::Destructive
    } else if mutates_system {
        ToolEffectKind::System
    } else if mutates_packages {
        ToolEffectKind::Package
    } else if writes {
        ToolEffectKind::Write
    } else if uses_network {
        ToolEffectKind::Network
    } else {
        ToolEffectKind::Process
    };
    let operation = if writes {
        PermissionOperation::Write
    } else {
        PermissionOperation::Execute
    };

    EffectProperties {
        effect_kind,
        idempotency: if writes || uses_network {
            ToolIdempotency::Unknown
        } else {
            ToolIdempotency::Idempotent
        },
        scopes: vec![scope(
            if uses_network {
                PermissionResource::Network
            } else {
                PermissionResource::Shell
            },
            operation,
            None,
        )],
        required_permission: if writes || uses_network {
            ToolPermissionMode::DangerFullAccess
        } else {
            ToolPermissionMode::ReadOnly
        },
        approval_class: if destructive || mutates_system {
            ToolApprovalClass::Administrator
        } else if writes || uses_network || mutates_packages {
            ToolApprovalClass::User
        } else {
            ToolApprovalClass::Policy
        },
        uses_network,
        spawns_process: true,
        mutates_packages,
        mutates_system,
    }
}

fn conservative_unknown(target: Option<String>) -> EffectProperties {
    EffectProperties {
        effect_kind: ToolEffectKind::Unknown,
        idempotency: ToolIdempotency::Unknown,
        scopes: vec![scope(
            PermissionResource::Tool,
            PermissionOperation::Call,
            target,
        )],
        required_permission: ToolPermissionMode::DangerFullAccess,
        approval_class: ToolApprovalClass::User,
        uses_network: true,
        spawns_process: true,
        mutates_packages: false,
        mutates_system: false,
    }
}

fn scope(
    resource: PermissionResource,
    operation: PermissionOperation,
    target: Option<String>,
) -> PermissionScope {
    PermissionScope {
        resource,
        operation,
        target,
    }
}

fn resource_target(input: &Value) -> Option<String> {
    ["path", "url", "server", "uri", "target"]
        .into_iter()
        .find_map(|key| input.get(key).and_then(Value::as_str).map(str::to_string))
}

fn tool_id_target(name: &str, input: &Value) -> Option<String> {
    resource_target(input).or_else(|| Some(name.to_string()))
}

fn stricter_permission(left: ToolPermissionMode, right: ToolPermissionMode) -> ToolPermissionMode {
    if permission_rank(left) >= permission_rank(right) {
        left
    } else {
        right
    }
}

const fn permission_rank(mode: ToolPermissionMode) -> u8 {
    match mode {
        ToolPermissionMode::ReadOnly => 0,
        ToolPermissionMode::WorkspaceWrite => 1,
        ToolPermissionMode::DangerFullAccess => 2,
    }
}

fn descriptor_hash(tool_id: &str, input: &Value, properties: &EffectProperties) -> String {
    let mut hasher = DefaultHasher::new();
    tool_id.hash(&mut hasher);
    canonical_json(input).hash(&mut hasher);
    format!("{:?}", properties).hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn canonical_json(input: &Value) -> String {
    match input {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let fields = keys
                .into_iter()
                .map(|key| format!("{key}:{}", canonical_json(&map[key])))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{fields}}}")
        }
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        _ => input.to_string(),
    }
}

fn contains_word(command: &str, words: &[&str]) -> bool {
    command
        .split(|character: char| {
            !(character.is_ascii_alphanumeric() || character == '_' || character == '-')
        })
        .any(|token| words.contains(&token))
}

fn contains_sequence(command: &str, sequences: &[&str]) -> bool {
    sequences.iter().any(|sequence| command.contains(sequence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bash_effect_escalates_with_effective_command() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        let read = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({"command": "git status"}),
            ToolPermissionMode::ReadOnly,
        );
        let destructive = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({"command": "rm -rf target"}),
            ToolPermissionMode::ReadOnly,
        );
        assert_eq!(read.effect_kind, ToolEffectKind::Process);
        assert_eq!(destructive.effect_kind, ToolEffectKind::Destructive);
        assert_ne!(read.descriptor_hash, destructive.descriptor_hash);
        assert_eq!(
            destructive.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
    }

    #[test]
    fn unknown_tool_is_conservative() {
        let descriptor = resolve_registered_tool_effect(
            &ToolEffectResolverSpec {
                resolver_id: "plugin.unknown".to_string(),
                resolver_version: 1,
            },
            "plugin_tool",
            &json!({}),
            ToolPermissionMode::ReadOnly,
        );
        assert_eq!(descriptor.effect_kind, ToolEffectKind::Unknown);
        assert_eq!(
            descriptor.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
        assert!(descriptor.uses_network);
        assert!(descriptor.spawns_process);
    }
}
