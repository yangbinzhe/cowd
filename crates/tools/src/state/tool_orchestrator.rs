//! Intrinsic tool-effect preflight.
//!
//! This module describes implementation effects only. Runtime owns policy,
//! approval, concurrency, timeout and result-budget decisions.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use harness_contract::policy::{
    DataClassification, EffectAssessment, EffectBlastRadius, EffectExternality, EffectNovelty,
    EffectReversibility, PermissionOperation, PermissionResource, PermissionScope,
};
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
    let assessment = effect_assessment(&properties, input);
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
        assessment,
    }
}

fn effect_assessment(properties: &EffectProperties, input: &Value) -> EffectAssessment {
    let externality = if properties.mutates_system
        || matches!(
            properties.effect_kind,
            ToolEffectKind::System | ToolEffectKind::Package | ToolEffectKind::Destructive
        ) {
        EffectExternality::System
    } else if properties.uses_network
        && properties.required_permission != ToolPermissionMode::ReadOnly
    {
        EffectExternality::ExternalMutation
    } else if properties.uses_network {
        EffectExternality::NetworkRead
    } else if matches!(
        properties.effect_kind,
        ToolEffectKind::Write | ToolEffectKind::Process
    ) {
        EffectExternality::Workspace
    } else {
        EffectExternality::Internal
    };
    let reversibility = match properties.effect_kind {
        ToolEffectKind::Read | ToolEffectKind::Network => EffectReversibility::Reversible,
        ToolEffectKind::Write | ToolEffectKind::Process => EffectReversibility::Compensatable,
        ToolEffectKind::Destructive | ToolEffectKind::System => EffectReversibility::Irreversible,
        ToolEffectKind::Package | ToolEffectKind::Unknown => EffectReversibility::Unknown,
    };
    let blast_radius = match externality {
        EffectExternality::Internal => EffectBlastRadius::Item,
        EffectExternality::Workspace => EffectBlastRadius::Workspace,
        EffectExternality::NetworkRead => EffectBlastRadius::Item,
        EffectExternality::ExternalMutation => EffectBlastRadius::ExternalAccount,
        EffectExternality::System => EffectBlastRadius::System,
        EffectExternality::Unknown => EffectBlastRadius::Unknown,
    };
    EffectAssessment {
        reversibility,
        externality,
        data_sensitivity: data_classification(properties, input),
        novelty: match properties.effect_kind {
            ToolEffectKind::Package => EffectNovelty::NewCapability,
            ToolEffectKind::Process => EffectNovelty::NewTarget,
            ToolEffectKind::Unknown => EffectNovelty::Unknown,
            _ => EffectNovelty::Routine,
        },
        blast_radius,
    }
}

fn data_classification(properties: &EffectProperties, input: &Value) -> DataClassification {
    if input
        .get("contains_secrets")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return DataClassification::Secret;
    }
    if let Some(classification) = input.get("data_classification").and_then(Value::as_str) {
        return match classification.trim().to_ascii_lowercase().as_str() {
            "public" => DataClassification::Public,
            "confidential" => DataClassification::Confidential,
            "secret" => DataClassification::Secret,
            _ => DataClassification::Internal,
        };
    }
    let target = resource_target(input)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if target == ".env"
        || target.ends_with("/.env")
        || target.contains("/.ssh/")
        || target.contains("credentials")
        || target.contains("secrets.")
    {
        return DataClassification::Secret;
    }
    if properties.uses_network && properties.required_permission == ToolPermissionMode::ReadOnly {
        DataClassification::Public
    } else {
        DataClassification::Internal
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
            let properties = command_effect(command);
            properties
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
        "runtime.orchestration" => {
            let operation = input
                .get("operation")
                .and_then(Value::as_str)
                .unwrap_or("inspect");
            if operation == "inspect" {
                EffectProperties {
                    effect_kind: ToolEffectKind::Read,
                    idempotency: ToolIdempotency::Idempotent,
                    scopes: vec![read_scope()],
                    required_permission: ToolPermissionMode::ReadOnly,
                    approval_class: ToolApprovalClass::None,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                }
            } else {
                EffectProperties {
                    effect_kind: ToolEffectKind::Write,
                    idempotency: ToolIdempotency::IdempotentWithKey,
                    scopes: vec![write_scope()],
                    required_permission: ToolPermissionMode::WorkspaceWrite,
                    approval_class: ToolApprovalClass::Policy,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                }
            }
        }
        "runtime.team_board" => {
            let operation = input
                .get("operation")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if operation == "publish" {
                EffectProperties {
                    effect_kind: ToolEffectKind::Write,
                    idempotency: ToolIdempotency::IdempotentWithKey,
                    scopes: vec![write_scope()],
                    required_permission: ToolPermissionMode::WorkspaceWrite,
                    approval_class: ToolApprovalClass::Policy,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                }
            } else {
                EffectProperties {
                    effect_kind: ToolEffectKind::Read,
                    idempotency: ToolIdempotency::Idempotent,
                    scopes: vec![read_scope()],
                    required_permission: ToolPermissionMode::ReadOnly,
                    approval_class: ToolApprovalClass::None,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                }
            }
        }
        "builtin.network" => EffectProperties {
            effect_kind: ToolEffectKind::Network,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![scope(
                PermissionResource::Network,
                PermissionOperation::Call,
                target,
            )],
            // Network transport is an effect dimension, not an authority
            // level. The catalog contract raises mutating or remote-trigger
            // tools to WorkspaceWrite/DangerFullAccess while read-only web
            // search and fetch remain usable under a read lease.
            required_permission: ToolPermissionMode::ReadOnly,
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
    let read_only = !destructive
        && !mutates_system
        && !mutates_packages
        && !writes
        && !uses_network
        && is_known_read_only_command(&normalized);
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
    } else if read_only {
        ToolEffectKind::Read
    } else {
        // `bash` is a registered process capability even when the command is
        // outside the deliberately small static classifier. Keep it behind
        // the strongest permission and approval boundary, but give the
        // governed planner a concrete process scope so an approved command
        // can execute instead of being rejected before policy evaluation.
        ToolEffectKind::Process
    };
    let operation = if read_only {
        PermissionOperation::Read
    } else if writes {
        PermissionOperation::Write
    } else {
        PermissionOperation::Execute
    };

    EffectProperties {
        effect_kind,
        idempotency: if read_only {
            ToolIdempotency::Idempotent
        } else if writes || uses_network {
            ToolIdempotency::Unknown
        } else {
            ToolIdempotency::Unknown
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
        required_permission: if read_only {
            ToolPermissionMode::ReadOnly
        } else if writes || uses_network {
            ToolPermissionMode::DangerFullAccess
        } else {
            ToolPermissionMode::DangerFullAccess
        },
        approval_class: if read_only {
            ToolApprovalClass::None
        } else if destructive || mutates_system {
            ToolApprovalClass::Administrator
        } else if writes || uses_network || mutates_packages {
            ToolApprovalClass::User
        } else {
            ToolApprovalClass::User
        },
        uses_network,
        spawns_process: true,
        mutates_packages,
        mutates_system,
    }
}

/// Recognize the deliberately small shell subset that is safe to represent as
/// a read effect. Process creation remains visible through `spawns_process`;
/// commands outside this set retain governed Process authority and never
/// inherit read-only authority.
fn is_known_read_only_command(command: &str) -> bool {
    if command.trim().is_empty() || command.contains("$(") || command.contains('`') {
        return false;
    }
    // Discarding stdout/stderr to /dev/null is read-only; any other
    // redirection writes a file and must stay governed.
    if has_file_redirection(command) {
        return false;
    }

    let Some(words) = shlex::split(command) else {
        return false;
    };
    let mut segment = Vec::new();
    for word in words {
        if matches!(word.as_str(), ";" | "|" | "&&" | "||") {
            if segment.is_empty() || !is_known_read_only_words(&segment) {
                return false;
            }
            segment.clear();
        } else {
            segment.push(word);
        }
    }
    !segment.is_empty() && is_known_read_only_words(&segment)
}

fn has_file_redirection(command: &str) -> bool {
    let Some(words) = shlex::split(command) else {
        return command.contains('>');
    };
    words.iter().any(|word| {
        if !word.contains('>') {
            return false;
        }
        let normalized = word.replace(' ', "");
        !matches!(
            normalized.as_str(),
            ">/dev/null" | "2>/dev/null" | "1>/dev/null" | "2>&-" | ">&-"
        )
    })
}

fn is_known_read_only_words(words: &[String]) -> bool {
    let Some(command) = words.first().map(String::as_str) else {
        return false;
    };
    match command {
        "cd" | "pwd" | "ls" | "cat" | "head" | "tail" | "grep" | "rg" | "egrep" | "fgrep"
        | "wc" | "stat" | "file" | "which" | "whereis" | "basename" | "dirname" | "realpath"
        | "readlink" | "date" | "uname" | "whoami" | "id" | "groups" | "df" | "du" | "free"
        | "uptime" | "hostname" | "env" | "printenv" | "seq" | "sort" | "uniq" | "cut"
        | "paste" | "tr" | "awk" | "jq" | "yq" | "sha256sum" | "md5sum" | "sha1sum" | "xxd"
        | "od" | "strings" | "find" | "tree" | "diff" | "cmp" | "comm" | "test" | "true"
        | "false" => true,
        "printf" | "echo" => words.iter().skip(1).all(|word| !word.contains('$')),
        "git" => {
            matches!(
                words.get(1).map(String::as_str),
                Some(
                    "status"
                        | "diff"
                        | "log"
                        | "show"
                        | "rev-parse"
                        | "ls-files"
                        | "ls-tree"
                        | "describe"
                )
            ) || (words.get(1).map(String::as_str) == Some("remote")
                && words
                    .iter()
                    .skip(2)
                    .all(|argument| matches!(argument.as_str(), "-v" | "--verbose")))
        }
        _ => false,
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
    let mut effect_input = input.clone();
    if let Value::Object(map) = &mut effect_input {
        map.remove("dangerouslyDisableSandbox");
        map.remove("isolateNetwork");
        map.remove("workspaceAccess");
    }
    canonical_json(&effect_input).hash(&mut hasher);
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
        assert_eq!(read.effect_kind, ToolEffectKind::Read);
        assert_eq!(read.approval_class, ToolApprovalClass::None);
        assert!(read.spawns_process);
        assert_eq!(destructive.effect_kind, ToolEffectKind::Destructive);
        assert_ne!(read.descriptor_hash, destructive.descriptor_hash);
        assert_eq!(
            destructive.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
    }

    #[test]
    fn sandbox_fields_do_not_escalate_effect_classification() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        let effect = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({
                "command": "git status",
                "dangerouslyDisableSandbox": true
            }),
            ToolPermissionMode::ReadOnly,
        );
        // The execution boundary is owned by the Runtime SandboxPosture, not
        // by model-supplied fields. Sandbox fields must not change approval
        // classification; the gateway overwrites them before execution.
        assert_eq!(effect.effect_kind, ToolEffectKind::Read);
        assert_eq!(effect.required_permission, ToolPermissionMode::ReadOnly);
        assert_ne!(effect.approval_class, ToolApprovalClass::User);
    }

    #[test]
    fn combined_read_only_bash_with_dev_null_discard_stays_unapproved() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        for command in [
            "ls -la && echo --- && find . -maxdepth 3 -type d 2>/dev/null | head -50",
            "pwd && ls -la && sha256sum README.md && stat --format=%s README.md",
            "find /home -maxdepth 3 -name '*.rs' 2>/dev/null | wc -l",
        ] {
            let effect = resolve_registered_tool_effect(
                &resolver,
                "bash",
                &json!({"command": command}),
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(
                effect.effect_kind,
                ToolEffectKind::Read,
                "command should stay read-only: {command}"
            );
            assert_eq!(effect.approval_class, ToolApprovalClass::None);
        }
    }

    #[test]
    fn file_redirection_keeps_write_classification_even_with_read_commands() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        let effect = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({"command": "ls -la > listing.txt"}),
            ToolPermissionMode::ReadOnly,
        );
        assert_eq!(effect.effect_kind, ToolEffectKind::Write);
        assert_eq!(effect.approval_class, ToolApprovalClass::User);
    }

    #[test]
    fn chained_git_inspection_is_read_only_but_unclassified_commands_remain_governed_processes() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        let inspection = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({
                "command": "cd /workspace && git diff --stat HEAD && git log --oneline -15 && git remote -v"
            }),
            ToolPermissionMode::ReadOnly,
        );
        let unknown = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({"command": "python -c \"print('x')\""}),
            ToolPermissionMode::ReadOnly,
        );

        assert_eq!(inspection.effect_kind, ToolEffectKind::Read);
        assert_eq!(inspection.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(inspection.approval_class, ToolApprovalClass::None);
        assert_eq!(unknown.effect_kind, ToolEffectKind::Process);
        assert_eq!(
            unknown.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
        assert_eq!(unknown.approval_class, ToolApprovalClass::User);
    }

    #[test]
    fn shell_inspection_with_dev_null_discard_stays_read_only() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        let effect = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({
                "command": "ls -la docs/ && ls -la .cowd/ 2>/dev/null && cat .cowd-todos.json 2>/dev/null | head -40"
            }),
            ToolPermissionMode::ReadOnly,
        );

        assert_eq!(effect.effect_kind, ToolEffectKind::Read);
        assert_eq!(effect.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(effect.approval_class, ToolApprovalClass::None);
    }

    #[test]
    fn quoted_grep_pattern_does_not_turn_a_read_pipeline_into_an_unknown_effect() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        let effect = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({
                "command": "cd /workspace && grep -n 'struct\\|enum\\|impl ' src/lib.rs | head -20"
            }),
            ToolPermissionMode::ReadOnly,
        );

        assert_eq!(effect.effect_kind, ToolEffectKind::Read);
        assert_eq!(effect.approval_class, ToolApprovalClass::None);
    }

    #[test]
    fn readonly_system_inspection_commands_have_concrete_effects() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.command".to_string(),
            resolver_version: 1,
        };
        for command in [
            "date +%Y",
            "date +%Y && printf '\\n'",
            "uname -a",
            "whoami",
            "df -h",
        ] {
            let effect = resolve_registered_tool_effect(
                &resolver,
                "bash",
                &json!({ "command": command }),
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(
                effect.effect_kind,
                ToolEffectKind::Read,
                "`{command}` should remain a governed read-only process",
            );
            assert_eq!(effect.approval_class, ToolApprovalClass::None);
        }
        let environment_dump = resolve_registered_tool_effect(
            &resolver,
            "bash",
            &json!({ "command": "env" }),
            ToolPermissionMode::ReadOnly,
        );
        // `env` stays read-only: the bash executor only ever exposes the
        // allowlisted sandbox environment, and the shell policy masks
        // secrets before launch.
        assert_eq!(environment_dump.effect_kind, ToolEffectKind::Read);
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

    #[test]
    fn runtime_orchestration_effect_depends_on_typed_operation() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "runtime.orchestration".to_string(),
            resolver_version: 1,
        };
        let inspect = resolve_registered_tool_effect(
            &resolver,
            "runtime_orchestrate",
            &json!({"intent": "inspect", "operation": "inspect"}),
            ToolPermissionMode::ReadOnly,
        );
        let propose = resolve_registered_tool_effect(
            &resolver,
            "runtime_orchestrate",
            &json!({"intent": "propose", "operation": "propose"}),
            ToolPermissionMode::ReadOnly,
        );

        assert_eq!(inspect.effect_kind, ToolEffectKind::Read);
        assert_eq!(inspect.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(propose.effect_kind, ToolEffectKind::Write);
        assert_eq!(
            propose.required_permission,
            ToolPermissionMode::WorkspaceWrite
        );
        assert_eq!(propose.idempotency, ToolIdempotency::IdempotentWithKey);
    }

    #[test]
    fn network_transport_preserves_the_catalog_permission_floor() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.network".to_string(),
            resolver_version: 1,
        };
        let search = resolve_registered_tool_effect(
            &resolver,
            "web_search",
            &json!({"query": "rust stable"}),
            ToolPermissionMode::ReadOnly,
        );
        let remote_trigger = resolve_registered_tool_effect(
            &resolver,
            "remote_trigger",
            &json!({"url": "https://example.com/hook"}),
            ToolPermissionMode::DangerFullAccess,
        );
        assert_eq!(search.required_permission, ToolPermissionMode::ReadOnly);
        assert!(search.uses_network);
        assert_eq!(
            remote_trigger.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
    }

    #[test]
    fn effect_assessment_marks_explicit_and_sensitive_path_data() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.readonly".to_string(),
            resolver_version: 1,
        };
        let environment = resolve_registered_tool_effect(
            &resolver,
            "read_file",
            &json!({"path": ".env"}),
            ToolPermissionMode::ReadOnly,
        );
        let confidential = resolve_registered_tool_effect(
            &resolver,
            "read_file",
            &json!({"path": "docs/design.md", "data_classification": "confidential"}),
            ToolPermissionMode::ReadOnly,
        );
        assert_eq!(
            environment.assessment.data_sensitivity,
            DataClassification::Secret
        );
        assert_eq!(
            confidential.assessment.data_sensitivity,
            DataClassification::Confidential
        );
    }
}
