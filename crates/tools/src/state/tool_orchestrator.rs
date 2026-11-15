//! Intrinsic tool-effect preflight.
//!
//! This module describes implementation effects only. Runtime owns policy,
//! approval, concurrency, timeout and result-budget decisions.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use harness_contract::goal::{ProofEffectPurpose, ProofEffectScopePurpose};
use harness_contract::policy::{
    DataClassification, EffectAssessment, EffectBlastRadius, EffectExternality, EffectNovelty,
    EffectReversibility, PermissionOperation, PermissionResource, PermissionScope,
};
use harness_contract::tool::{
    ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolEffectResolverSpec,
    ToolIdempotency, ToolPermissionMode,
};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolEffectResolution {
    pub descriptor: ToolEffectDescriptor,
    pub scope_purposes: Vec<ProofEffectScopePurpose>,
}

struct ResolvedEffectProperties {
    properties: EffectProperties,
    scope_purposes: Vec<ProofEffectScopePurpose>,
}

impl ResolvedEffectProperties {
    fn ordinary(properties: EffectProperties) -> Self {
        let scope_purposes = properties
            .scopes
            .iter()
            .cloned()
            .map(|declared_scope| {
                let purpose = if declared_scope.operation == PermissionOperation::Read {
                    ProofEffectPurpose::InputRead
                } else {
                    ProofEffectPurpose::BusinessEffect
                };
                ProofEffectScopePurpose {
                    declared_scope,
                    purpose,
                }
            })
            .collect();
        Self {
            properties,
            scope_purposes,
        }
    }
}

impl std::ops::Deref for ResolvedEffectProperties {
    type Target = EffectProperties;

    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}

impl std::ops::DerefMut for ResolvedEffectProperties {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.properties
    }
}

/// Effect for an action-aware merged tool: a query action stays ReadOnly, any
/// other action takes the family's elevated permission.
fn action_effect(
    input: &Value,
    readonly_actions: &[&str],
    elevated_permission: ToolPermissionMode,
    elevated_kind: ToolEffectKind,
) -> EffectProperties {
    let action = input
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if readonly_actions.contains(&action) {
        EffectProperties {
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Read,
                resource_target(input),
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        }
    } else {
        EffectProperties {
            effect_kind: elevated_kind,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Write,
                resource_target(input),
            )],
            required_permission: elevated_permission,
            approval_class: ToolApprovalClass::Policy,
            uses_network: matches!(elevated_kind, ToolEffectKind::Network),
            spawns_process: matches!(elevated_kind, ToolEffectKind::Process),
            mutates_packages: false,
            mutates_system: false,
        }
    }
}

/// Derive the effective effect from the concrete tool input.
#[must_use]
pub fn resolve_registered_tool_effect(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    catalog_permission: ToolPermissionMode,
) -> ToolEffectDescriptor {
    resolve_registered_tool_effect_resolution(resolver, tool_id, input, catalog_permission)
        .descriptor
}

#[must_use]
pub fn resolve_registered_tool_effect_resolution(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    catalog_permission: ToolPermissionMode,
) -> ToolEffectResolution {
    let mut properties = if resolver.resolver_id == "builtin.orchestrate" {
        ResolvedEffectProperties::ordinary(orchestration_properties(
            catalog_permission,
            resource_target(input),
        ))
    } else {
        resolve_effect_properties(resolver, tool_id, input)
    };
    if !resolver.resolver_id.starts_with("runtime.action_") {
        properties.required_permission =
            stricter_permission(properties.required_permission, catalog_permission);
    }

    let mut assessment = effect_assessment(&properties, input);
    if data_only_compute(resolver, tool_id, input, &properties) {
        assessment.novelty = EffectNovelty::Routine;
    }
    finalize_effect(resolver, tool_id, input, properties, assessment, &[])
}

fn finalize_effect(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    resolved: ResolvedEffectProperties,
    assessment: EffectAssessment,
    child_fingerprints: &[String],
) -> ToolEffectResolution {
    let ResolvedEffectProperties {
        mut properties,
        scope_purposes,
    } = resolved;
    properties
        .scopes
        .sort_by_key(|scope| serde_json::to_string(scope).unwrap_or_default());
    properties.scopes.dedup();
    let descriptor_hash = descriptor_hash(
        resolver,
        tool_id,
        input,
        &properties,
        &assessment,
        child_fingerprints,
    );
    let descriptor = ToolEffectDescriptor {
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
    };
    let scope_purposes = descriptor
        .scopes
        .iter()
        .map(|declared_scope| {
            let purpose = scope_purposes
                .iter()
                .filter(|entry| &entry.declared_scope == declared_scope)
                .map(|entry| entry.purpose)
                .max_by_key(|purpose| match purpose {
                    ProofEffectPurpose::BusinessEffect => 2,
                    ProofEffectPurpose::InputRead => 1,
                    ProofEffectPurpose::AutoCheckpointMaintenance => 0,
                })
                .expect("every effect scope has a producer purpose");
            ProofEffectScopePurpose {
                declared_scope: declared_scope.clone(),
                purpose,
            }
        })
        .collect();
    ToolEffectResolution {
        descriptor,
        scope_purposes,
    }
}

pub(crate) fn resolve_lsp_tool_effect_resolution(
    resolver: &ToolEffectResolverSpec,
    input: &Value,
    catalog_permission: ToolPermissionMode,
    roots: Option<Vec<Option<String>>>,
) -> ToolEffectResolution {
    let mut properties = resolve_base_effect_properties(resolver, "lsp", input);
    if input.get("action").and_then(Value::as_str) == Some("diagnostics") {
        properties.spawns_process = false;
    } else if let Some(roots) = roots.filter(|roots| !roots.is_empty()) {
        properties.scopes.extend(
            roots
                .into_iter()
                .map(|root| scope(PermissionResource::File, PermissionOperation::Read, root)),
        );
    } else {
        properties = conservative_unknown(None);
    }
    properties.required_permission =
        stricter_permission(properties.required_permission, catalog_permission);
    let assessment = effect_assessment(&properties, input);
    finalize_effect(
        resolver,
        "lsp",
        input,
        ResolvedEffectProperties::ordinary(properties),
        assessment,
        &[],
    )
}

pub(crate) fn resolve_orchestration_child_effect_resolution(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    catalog_permission: ToolPermissionMode,
    references: &[Vec<String>],
) -> Result<ToolEffectResolution, String> {
    if resolver.resolver_id == "runtime.action_edit" && resolver.resolver_version != 3 {
        return Err("edit resolver version cannot execute current checkpoint semantics".into());
    }
    if references.is_empty() {
        return Ok(resolve_registered_tool_effect_resolution(
            resolver,
            tool_id,
            input,
            catalog_permission,
        ));
    }
    let compute_data_only = catalog_permission == ToolPermissionMode::ReadOnly
        && data_only_compute(
            resolver, tool_id, input, &resolve_effect_properties(resolver, tool_id, input),
        );
    let data_only =
        references
            .iter()
            .all(|path| match (tool_id, path.first().map(String::as_str)) {
                (_, Some("contains_secrets" | "data_classification")) => true,
                (
                    "edit",
                    Some(
                        "content" | "old_string" | "new_string" | "new_source" | "replace_all"
                        | "expected_hashes" | "cell_id" | "cell_type" | "edit_mode",
                    ),
                ) => true,
                ("edit", Some("edits")) => path
                    .get(2)
                    .is_some_and(|field| matches!(field.as_str(), "old_string" | "new_string")),
                ("web_fetch", Some("prompt")) | ("web_search", Some("query")) => true,
                ("read_file", Some("offset" | "limit" | "complete")) => true,
                ("checkpoint", Some("label")) | ("background", Some("tail")) => true,
                ("bash" | "powershell", Some("description")) => true,
                ("execute_code", Some("input")) if compute_data_only => true,
                _ => false,
            });
    let mut properties = if data_only {
        resolve_effect_properties(resolver, tool_id, input)
    } else {
        match (tool_id, resolver.resolver_id.as_str()) {
            ("edit", "runtime.action_edit") => {
                let mut properties = action_effect(input, &[], ToolPermissionMode::WorkspaceWrite, ToolEffectKind::Write);
                properties.scopes = vec![
                    scope(PermissionResource::File, PermissionOperation::Read, None),
                    scope(PermissionResource::File, PermissionOperation::Write, None),
                    scope(PermissionResource::File, PermissionOperation::Write, Some(".cowd/checkpoints".into())),
                ];
                let mut resolved = ResolvedEffectProperties::ordinary(properties);
                if matches!(input.get("action").and_then(Value::as_str), Some("write" | "replace" | "patch")) {
                    resolved.scope_purposes[2].purpose = ProofEffectPurpose::AutoCheckpointMaintenance;
                }
                resolved
            }
            ("read_file" | "glob_search" | "grep_search" | "workspace_snapshot", "builtin.readonly") => {
                let mut properties = resolve_effect_properties(resolver, tool_id, &Value::Null);
                properties.scopes.retain(|scope| scope.resource != PermissionResource::File);
                properties.scope_purposes.retain(|entry| entry.declared_scope.resource != PermissionResource::File);
                let declared_scope = scope(PermissionResource::File, PermissionOperation::Read, None);
                properties.scopes.push(declared_scope.clone());
                properties.scope_purposes.push(ProofEffectScopePurpose {
                    declared_scope, purpose: ProofEffectPurpose::InputRead,
                });
                properties
            }
            // The compute child's own declared file set is part of its
            // contract, so it is resolved from the child's real input. A null
            // input hid that declaration, which made the child unprovable
            // and dropped every sibling effect with it - planning then
            // described less than what the program may do.
            ("execute_code", "builtin.sandboxed_command") => {
                resolve_effect_properties(resolver, tool_id, input)
            }
            _ => return Err(format!("tool_orchestrate cannot prove a bounded effect envelope for `{tool_id}` reference paths {references:?}")),
        }
    };
    properties.required_permission =
        stricter_permission(properties.required_permission, catalog_permission);
    properties.idempotency = ToolIdempotency::Unknown;
    let mut assessment = effect_assessment(&properties, input);
    if compute_data_only {
        assessment.novelty = EffectNovelty::Routine;
    }
    // A dynamic reference can hide a sensitive target or override an explicit
    // classification. Only references confined to known non-sensitive data
    // fields of a proven read-only effect retain the normal classification.
    let sensitivity_dynamic = references.iter().any(|path| {
        matches!(path.first().map(String::as_str), Some("contains_secrets" | "data_classification"))
    });
    // A bounded read-only compute child keeps the normal classification: it
    // observes exactly its declared files inside the read-only sandbox. The
    // sensitive-target rules in `data_classification` still apply unchanged,
    // so a `.env` or credentials input stays Secret.
    let bounded_child = is_bounded_read_only_compute(
        properties.effect_kind,
        properties.required_permission,
        properties.uses_network,
        properties.spawns_process,
        properties.mutates_packages,
        properties.mutates_system,
        &properties.scopes,
        false,
    );
    if !data_only || sensitivity_dynamic || !(is_read_only_research_effect(&properties) || bounded_child)
    {
        assessment.data_sensitivity = DataClassification::Secret;
    }
    if !data_only {
        assessment.novelty = EffectNovelty::NewTarget;
        assessment.blast_radius = EffectBlastRadius::Workspace;
    }
    Ok(finalize_effect(
        resolver,
        tool_id,
        input,
        properties,
        assessment,
        &[],
    ))
}

pub(crate) fn resolve_composite_tool_effect_resolution(
    resolver: &ToolEffectResolverSpec,
    input: &Value,
    floor: ToolPermissionMode,
    children: &[ToolEffectResolution],
    child_fingerprints: &[String],
    unresolved: bool,
) -> ToolEffectResolution {
    let mut properties = if unresolved {
        conservative_unknown(None)
    } else {
        orchestration_properties(floor, None)
    };
    if !unresolved && !children.is_empty() {
        properties.scopes.clear();
    }
    let mut properties = ResolvedEffectProperties::ordinary(properties);
    let mut assessment = effect_assessment(&properties, input);
    if unresolved {
        assessment = EffectAssessment::default();
        assessment.data_sensitivity = DataClassification::Secret;
    }
    for resolution in children {
        properties
            .scope_purposes
            .extend(resolution.scope_purposes.iter().cloned());
        let child = &resolution.descriptor;
        properties.scopes.extend(child.scopes.iter().cloned());
        properties.effect_kind = join_ranked(
            properties.effect_kind,
            child.effect_kind,
            &[
                ToolEffectKind::Read,
                ToolEffectKind::Network,
                ToolEffectKind::Write,
                ToolEffectKind::Process,
                ToolEffectKind::Package,
                ToolEffectKind::Destructive,
                ToolEffectKind::System,
                ToolEffectKind::Unknown,
            ],
        );
        properties.required_permission =
            stricter_permission(properties.required_permission, child.required_permission);
        properties.approval_class = join_ranked(
            properties.approval_class,
            child.approval_class,
            &[
                ToolApprovalClass::None,
                ToolApprovalClass::Policy,
                ToolApprovalClass::User,
                ToolApprovalClass::Administrator,
            ],
        );
        let child_idempotency = if child.idempotency == ToolIdempotency::IdempotentWithKey {
            ToolIdempotency::Unknown
        } else {
            child.idempotency
        };
        properties.idempotency = join_ranked(
            properties.idempotency,
            child_idempotency,
            &[
                ToolIdempotency::Idempotent,
                ToolIdempotency::IdempotentWithKey,
                ToolIdempotency::Unknown,
                ToolIdempotency::NonIdempotent,
            ],
        );
        properties.uses_network |= child.uses_network;
        properties.spawns_process |= child.spawns_process;
        properties.mutates_packages |= child.mutates_packages;
        properties.mutates_system |= child.mutates_system;
        assessment.reversibility = join_ranked(
            assessment.reversibility,
            child.assessment.reversibility,
            &[
                EffectReversibility::Reversible,
                EffectReversibility::Compensatable,
                EffectReversibility::Irreversible,
                EffectReversibility::Unknown,
            ],
        );
        assessment.externality = join_ranked(
            assessment.externality,
            child.assessment.externality,
            &[
                EffectExternality::Internal,
                EffectExternality::Workspace,
                EffectExternality::NetworkRead,
                EffectExternality::ExternalMutation,
                EffectExternality::System,
                EffectExternality::Unknown,
            ],
        );
        assessment.data_sensitivity = join_ranked(
            assessment.data_sensitivity,
            child.assessment.data_sensitivity,
            &[
                DataClassification::Public,
                DataClassification::Internal,
                DataClassification::Confidential,
                DataClassification::Secret,
            ],
        );
        assessment.novelty = join_ranked(
            assessment.novelty,
            child.assessment.novelty,
            &[
                EffectNovelty::Routine,
                EffectNovelty::NewTarget,
                EffectNovelty::NewCapability,
                EffectNovelty::Unknown,
            ],
        );
        assessment.blast_radius = join_ranked(
            assessment.blast_radius,
            child.assessment.blast_radius,
            &[
                EffectBlastRadius::Item,
                EffectBlastRadius::Workspace,
                EffectBlastRadius::ExternalAccount,
                EffectBlastRadius::System,
                EffectBlastRadius::Unbounded,
                EffectBlastRadius::Unknown,
            ],
        );
    }
    finalize_effect(
        resolver,
        "tool_orchestrate",
        input,
        properties,
        assessment,
        child_fingerprints,
    )
}

fn join_ranked<Ranked: Copy + PartialEq>(left: Ranked, right: Ranked, order: &[Ranked]) -> Ranked {
    if order.iter().position(|value| *value == left)
        >= order.iter().position(|value| *value == right)
    {
        left
    } else {
        right
    }
}

fn effect_assessment(properties: &EffectProperties, input: &Value) -> EffectAssessment {
    let internal_control = harness_contract::tool::is_internal_control_effect(
        properties.effect_kind, properties.idempotency, properties.required_permission,
        properties.approval_class, &properties.scopes, properties.uses_network,
        properties.spawns_process, properties.mutates_packages, properties.mutates_system,
    );
    let externality = if internal_control {
        EffectExternality::Internal
    } else if properties.mutates_system
        || matches!(
            properties.effect_kind,
            ToolEffectKind::System | ToolEffectKind::Package | ToolEffectKind::Destructive
        )
    {
        EffectExternality::System
    } else if properties.uses_network
        && properties.required_permission != ToolPermissionMode::ReadOnly
    {
        EffectExternality::ExternalMutation
    } else if properties.uses_network {
        EffectExternality::NetworkRead
    } else if properties
        .scopes
        .iter()
        .any(|scope| scope.resource == PermissionResource::File)
        || matches!(
            properties.effect_kind,
            ToolEffectKind::Write | ToolEffectKind::Process
        )
    {
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

/// Whether this effect is a plain read-only research effect: it reads workspace
/// content, writes nothing, touches no network and spawns no process.
///
/// `read_file` with an explicit path and `glob_search` with a recursive pattern
/// both read workspace content, yet only the latter used to be classified
/// Secret - purely because its declared scope carries no target. That
/// inconsistency is what forced `danger-full-access` onto a read-only research
/// tool and made a read-only session unable to enumerate the very corpus the
/// scenario declares. Sensitivity is a property of what an effect does, not of
/// whether its scope happens to name a target.
/// Whether this effect is a **bounded read-only compute**.
///
/// A read-only ceiling does not forbid a process: it forbids *unbounded*
/// authority. This predicate is the single contract statement for the narrow
/// case the product actually supports - a real child process that
///
///   * declares its own effect as read-only,
///   * uses no network, mutates no package and no system state,
///   * may observe exactly the declared file scopes (each one an explicit,
///     non-root target) and nothing else,
///   * carries no Shell scope (an arbitrary command line is not bounded), and
///   * is executed inside the kernel-enforced, network-off, read-only sandbox.
///
/// Anything outside that envelope - an unbounded file scope, the workspace
/// root, a Shell scope, a write or control scope, network access - is NOT a
/// bounded read-only compute and keeps the conservative Secret classification
/// and the permission it implies.
fn is_bounded_read_only_compute(
    effect_kind: ToolEffectKind,
    required_permission: ToolPermissionMode,
    uses_network: bool,
    spawns_process: bool,
    mutates_packages: bool,
    mutates_system: bool,
    scopes: &[PermissionScope],
    composite: bool,
) -> bool {
    if effect_kind != ToolEffectKind::Process
        || !spawns_process
        || uses_network
        || mutates_packages
        || mutates_system
        || required_permission != ToolPermissionMode::ReadOnly
    {
        return false;
    }
    scopes.iter().all(|scope| match scope.resource {
        PermissionResource::File => {
            scope.operation == PermissionOperation::Read
                && scope
                    .target
                    .as_deref()
                    .map(str::trim)
                    .filter(|target| !target.is_empty())
                    .is_some_and(|target| composite || target != ".")
        }
        // The invocation itself is what the lease authorizes; it grants no
        // authority over any file, network or command line by itself.
        PermissionResource::Tool => scope.operation == PermissionOperation::Call
            || (composite && scope.operation == PermissionOperation::Read),
        // A process that may run an arbitrary command line is not bounded, and
        // neither is a tool that writes or controls the workspace.
        PermissionResource::Shell => false,
        PermissionResource::Network => false,
        _ => false,
    })
}

/// The public contract form of [`is_bounded_read_only_compute`], evaluated on
/// the resolved effect descriptor. Runtime owns the permission decision, so the
/// predicate lives with the tool contract it describes.
pub fn effect_is_bounded_read_only_compute(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> bool {
    is_bounded_read_only_compute(
        descriptor.effect_kind,
        descriptor.required_permission,
        descriptor.uses_network,
        descriptor.spawns_process,
        descriptor.mutates_packages,
        descriptor.mutates_system,
        &descriptor.scopes,
        descriptor.tool_id == "tool_orchestrate",
    )
}

fn data_only_compute(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    properties: &EffectProperties,
) -> bool {
    tool_id == "execute_code"
        && resolver.resolver_id == "builtin.sandboxed_command"
        && input.get("input_files").and_then(Value::as_array).is_some_and(Vec::is_empty)
        && input.get("dangerouslyDisableSandbox").is_none_or(|v| v == false)
        && input.get("isolateNetwork").is_none_or(|v| v == true)
        && input.get("workspaceAccess").is_none_or(|v| v == "read-only")
        && is_bounded_read_only_compute(
            properties.effect_kind,
            properties.required_permission,
            properties.uses_network,
            properties.spawns_process,
            properties.mutates_packages,
            properties.mutates_system,
            &properties.scopes,
            false,
        )
}

fn is_read_only_research_effect(properties: &EffectProperties) -> bool {
    properties.required_permission == ToolPermissionMode::ReadOnly
        && !properties.uses_network
        && !properties.spawns_process
        && !properties.mutates_packages
        && !properties.mutates_system
        && matches!(properties.effect_kind, ToolEffectKind::Read)
        && !properties.scopes.iter().any(|scope| {
            scope.resource == PermissionResource::File
                && matches!(
                    scope.operation,
                    PermissionOperation::Write | PermissionOperation::Control
                )
        })
}

fn data_classification(properties: &EffectProperties, input: &Value) -> DataClassification {
    // An unbounded read scope is only Secret when the effect is not a plain
    // read-only research effect. Everything below still applies unchanged, so
    // an explicit `.env` / `.ssh` / credentials target, an explicit
    // `contains_secrets` or `data_classification` input, and any non-read-only
    // posture still classify as before.
    if !is_read_only_research_effect(properties)
        && properties.scopes.iter().any(|scope| {
            scope.resource == PermissionResource::File
                && scope.operation == PermissionOperation::Read
                && scope.target.is_none()
        })
    {
        return DataClassification::Secret;
    }
    if input
        .get("contains_secrets")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return DataClassification::Secret;
    }
    if properties
        .scopes
        .iter()
        .filter_map(|scope| scope.target.as_deref())
        .any(|target| {
            let target = target.to_ascii_lowercase().replace('\\', "/");
            target == ".env"
                || target.ends_with("/.env")
                || target.contains("/.ssh/")
                || target.starts_with(".ssh/")
                || target.contains("credentials")
                || target.contains("secrets.")
        })
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
    if properties.uses_network && properties.required_permission == ToolPermissionMode::ReadOnly {
        DataClassification::Public
    } else {
        DataClassification::Internal
    }
}

#[derive(Debug, Clone)]
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
) -> ResolvedEffectProperties {
    let mut properties = ResolvedEffectProperties::ordinary(resolve_base_effect_properties(
        resolver, tool_id, input,
    ));
    if resolver.resolver_id == crate::builtin_effect_resolver_spec(tool_id).resolver_id {
        match crate::executor::orchestrate::original_static_file_scopes(tool_id, input) {
            Ok(scopes) => {
                properties
                    .scopes
                    .extend(scopes.iter().map(|entry| entry.declared_scope.clone()));
                properties.scope_purposes.extend(scopes);
            }
            Err(_) => return ResolvedEffectProperties::ordinary(conservative_unknown(None)),
        }
        if tool_id == "vision_analyze" {
            properties.spawns_process = false;
        }
        if tool_id == "checkpoint" && input.get("action").and_then(Value::as_str) == Some("create")
        {
            properties.idempotency = ToolIdempotency::NonIdempotent;
        }
        if tool_id == "skill"
            && matches!(
                input.get("action").and_then(Value::as_str),
                Some("install_plan" | "install_commit")
            )
            && input
                .get("source")
                .and_then(Value::as_str)
                .is_some_and(|source| {
                    source.trim().starts_with("https://github.com/")
                        || source.trim().starts_with("github://")
                })
        {
            properties.uses_network = true;
            let declared_scope =
                scope(PermissionResource::Network, PermissionOperation::Call, None);
            properties.scopes.push(declared_scope.clone());
            properties.scope_purposes.push(ProofEffectScopePurpose {
                declared_scope,
                purpose: ProofEffectPurpose::BusinessEffect,
            });
        }
        if properties.scopes.iter().any(|scope| {
            scope.resource == PermissionResource::File
                && scope.operation == PermissionOperation::Write
        }) && properties.effect_kind == ToolEffectKind::Read
        {
            properties.effect_kind = ToolEffectKind::Write;
            properties.idempotency = ToolIdempotency::Unknown;
        }
    }
    properties
}

fn resolve_base_effect_properties(
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
    let control_scope = || {
        scope(
            PermissionResource::Tool,
            PermissionOperation::Control,
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
        "builtin.sandboxed_command" => EffectProperties {
            // The implementation is still a real child process and remains
            // visible to capacity accounting, but its authority is bounded
            // to the workspace sandbox rather than arbitrary host command
            // execution. Code contents cannot opt out of this posture.
            //
            // The effect declares NO unbounded scope. The only file content the
            // kernel-hardened, network-off, read-only sandbox can observe is the
            // finite `input_files` set the caller declared, so the file scopes
            // are derived from that input by `original_static_file_scopes`.
            // Declaring `Shell:Execute(None)` and `File:Read(None)` here is what
            // used to classify this read-only tool as touching Secret data and
            // demand a whole-workspace lease that no read-only ceiling could
            // certify, which made the bounded reduction objective unreachable
            // regardless of model behaviour. Process-ness stays declared through
            // `effect_kind` and `spawns_process`.
            effect_kind: ToolEffectKind::Process,
            idempotency: ToolIdempotency::Unknown,
            // The tool invocation itself is the capability the lease
            // authorizes; the file scopes the process may observe are added
            // from the request's declared  (and only from there).
            // An empty scope set would mean "no invocation at all", which is
            // not what this tool is, and it would leave the descriptor
            // without any scope to certify.
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Call,
                None,
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
        "builtin.readonly" if tool_id == "read_file" => EffectProperties {
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope(
                PermissionResource::File,
                PermissionOperation::Read,
                input
                    .get("path")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
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
        "runtime.agent_action" | "runtime.private_note" => {
            if resolver.resolver_version != 2 {
                return conservative_unknown(tool_id_target(tool_id, input));
            }
            if tool_id == harness_contract::agent_action::STATE_INSPECT_TOOL_ID {
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
                // Agent Actions mutate only Runtime-owned collaboration
                // facts. They neither grant nor perform workspace/external
                // effects; those remain separately authorized ToolHost calls.
                EffectProperties {
                    effect_kind: ToolEffectKind::Write,
                    idempotency: ToolIdempotency::IdempotentWithKey,
                    scopes: vec![control_scope()],
                    required_permission: ToolPermissionMode::ReadOnly,
                    approval_class: ToolApprovalClass::None,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                }
            }
        }
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
        // Action-aware merged families: the permission follows the `action`
        // field, so one tool can be safely ReadOnly for its query actions and
        // elevated for its mutating ones. The catalog still declares the
        // STRICTEST action, which is what the orchestration escalation path uses.
        "runtime.action_skill" => action_effect(
            input,
            &["install_plan", "status"],
            ToolPermissionMode::DangerFullAccess,
            ToolEffectKind::Unknown,
        ),
        "runtime.action_checkpoint" => action_effect(
            input,
            &["list", "diff"],
            ToolPermissionMode::WorkspaceWrite,
            ToolEffectKind::Write,
        ),
        "runtime.action_mcp" => action_effect(
            input,
            &["list", "read"],
            ToolPermissionMode::DangerFullAccess,
            ToolEffectKind::Network,
        ),
        "runtime.action_edit" => {
            if resolver.resolver_version != 3 {
                return conservative_unknown(tool_id_target(tool_id, input));
            }
            let mut properties = action_effect(
                input,
                &["preview"],
                ToolPermissionMode::WorkspaceWrite,
                ToolEffectKind::Write,
            );
            let action = input
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let operation = if action == "preview" {
                PermissionOperation::Read
            } else {
                PermissionOperation::Write
            };
            let mut targets = match action {
                "preview" | "patch" => input
                    .get("edits")
                    .and_then(Value::as_array)
                    .map(|edits| {
                        edits
                            .iter()
                            .map(|edit| {
                                edit.get("path").and_then(Value::as_str).map(str::to_string)
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_else(|| vec![None]),
                "notebook" => vec![input
                    .get("notebook_path")
                    .and_then(Value::as_str)
                    .map(str::to_string)],
                _ => vec![input
                    .get("path")
                    .and_then(Value::as_str)
                    .map(str::to_string)],
            };
            if targets.is_empty() {
                targets.push(None);
            }
            properties.scopes = targets
                .into_iter()
                .map(|target| scope(PermissionResource::File, operation.clone(), target))
                .collect();
            properties
        }
        "runtime.action_background" => action_effect(
            input,
            &["status"],
            ToolPermissionMode::WorkspaceWrite,
            ToolEffectKind::Process,
        ),
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

/// Effect properties for an orchestration tool, derived from the strictest
/// sub-call permission the caller escalated to.
fn orchestration_properties(
    permission: ToolPermissionMode,
    target: Option<String>,
) -> EffectProperties {
    match permission {
        ToolPermissionMode::ReadOnly => EffectProperties {
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Read,
                target,
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
        ToolPermissionMode::WorkspaceWrite => EffectProperties {
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Write,
                target,
            )],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        },
        ToolPermissionMode::DangerFullAccess => EffectProperties {
            effect_kind: ToolEffectKind::Destructive,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![scope(
                PermissionResource::Tool,
                PermissionOperation::Control,
                target,
            )],
            required_permission: ToolPermissionMode::DangerFullAccess,
            approval_class: ToolApprovalClass::User,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
        },
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

pub(crate) fn resource_target(input: &Value) -> Option<String> {
    ["path", "url", "server", "uri", "target"]
        .into_iter()
        .find_map(|key| input.get(key).and_then(Value::as_str).map(str::to_string))
}

fn tool_id_target(name: &str, input: &Value) -> Option<String> {
    resource_target(input).or_else(|| Some(name.to_string()))
}

pub(crate) fn stricter_permission(
    left: ToolPermissionMode,
    right: ToolPermissionMode,
) -> ToolPermissionMode {
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

fn descriptor_hash(
    resolver: &ToolEffectResolverSpec,
    tool_id: &str,
    input: &Value,
    properties: &EffectProperties,
    assessment: &EffectAssessment,
    child_fingerprints: &[String],
) -> String {
    let mut hasher = DefaultHasher::new();
    "tools.effect.v2".hash(&mut hasher);
    resolver.resolver_id.hash(&mut hasher);
    resolver.resolver_version.hash(&mut hasher);
    tool_id.hash(&mut hasher);
    let mut effect_input = input.clone();
    if let Value::Object(map) = &mut effect_input {
        map.remove("dangerouslyDisableSandbox");
        map.remove("isolateNetwork");
        map.remove("workspaceAccess");
    }
    canonical_json(&effect_input).hash(&mut hasher);
    format!("{:?}", properties).hash(&mut hasher);
    serde_json::to_string(assessment)
        .unwrap_or_default()
        .hash(&mut hasher);
    child_fingerprints.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn canonical_json(input: &Value) -> String {
    match input {
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let fields = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap_or_default(),
                        canonical_json(&map[key])
                    )
                })
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

    #[test]
    fn fix2_compute_composite_classification_and_boundaries() {
        let host = crate::ToolHost::builtin("fix2", &std::env::current_dir().unwrap());
        let lease = host.pin_snapshot();
        let program = |read: Value, compute_input: Value| json!({"steps":[
            {"id":"r","op":"call","tool":"read_file","input":read},
            {"id":"c","op":"compute","language":"python","code":"print(_input)","input":compute_input}
        ]});
        for data in [json!([1, 2]), json!({"value":"$r"})] {
            let effect = lease.describe_effect("tool_orchestrate", &program(json!({"path":"src/a"}), data));
            assert!(effect_is_bounded_read_only_compute(&effect), "{effect:?}");
            assert_eq!(effect.assessment.novelty, EffectNovelty::Routine);
            assert_eq!(effect.assessment.data_sensitivity, DataClassification::Internal);
        }
        let secret = lease.describe_effect("tool_orchestrate", &program(json!({"path":".env"}), json!("$r")));
        assert_eq!(secret.assessment.data_sensitivity, DataClassification::Secret);
        let dynamic_path = lease.describe_effect("tool_orchestrate", &program(json!({"path":"$previous.path"}), json!("$r")));
        assert!(!effect_is_bounded_read_only_compute(&dynamic_path));
        assert_eq!(dynamic_path.assessment.data_sensitivity, DataClassification::Secret);
        let write = lease.describe_effect("tool_orchestrate", &json!({"steps":[
            {"id":"w","op":"call","tool":"edit","input":{"action":"write","path":"out","content":"x"}},
            {"id":"c","op":"compute","language":"python","code":"print(1)","input":1}
        ]}));
        assert_eq!(write.required_permission, ToolPermissionMode::WorkspaceWrite);
        assert!(!effect_is_bounded_read_only_compute(&write));

        let resolver = crate::builtin_effect_resolver_spec("execute_code");
        for input in [
            json!({"input":"$r","input_files":["src/a"]}),
            json!({"input":"$r","input_files":[],"workspaceAccess":"write"}),
            json!({"input":"$r","input_files":[],"isolateNetwork":false}),
            json!({"input":"$r","input_files":[],"dangerouslyDisableSandbox":true}),
            json!({"input_files":["$r.path"]}),
        ] {
            let path = if input.get("input").is_some() { "input" } else { "input_files" };
            let effect = resolve_orchestration_child_effect_resolution(&resolver, "execute_code", &input, ToolPermissionMode::ReadOnly, &[vec![path.into()]]).unwrap().descriptor;
            assert_eq!(effect.assessment.data_sensitivity, DataClassification::Secret, "{input}");
            assert_ne!(effect.assessment.novelty, EffectNovelty::Routine, "{input}");
        }
    }
    use serde_json::json;

    fn assert_complete_purposes(resolution: &ToolEffectResolution) {
        harness_contract::goal::ProofEffectScope {
            version: 2,
            descriptor_hash: resolution.descriptor.descriptor_hash.clone(),
            complete: false,
            targets: Vec::new(),
            scope_purposes: resolution.scope_purposes.clone(),
        }
        .validate_purpose_mapping(&resolution.descriptor)
        .unwrap();
    }

    #[test]
    fn typed_purposes_preserve_frozen_original_descriptor_hash_inputs() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "runtime.action_edit".into(),
            resolver_version: 3,
        };
        let input = json!({"action":"write","path":"a","content":"x"});
        let properties = EffectProperties {
            effect_kind: ToolEffectKind::Write,
            idempotency: ToolIdempotency::IdempotentWithKey,
            scopes: vec![
                scope(
                    PermissionResource::File,
                    PermissionOperation::Read,
                    Some("a".into()),
                ),
                scope(
                    PermissionResource::File,
                    PermissionOperation::Write,
                    Some(".cowd/checkpoints".into()),
                ),
                scope(
                    PermissionResource::File,
                    PermissionOperation::Write,
                    Some("a".into()),
                ),
            ],
            required_permission: ToolPermissionMode::WorkspaceWrite,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        };
        assert_eq!(format!("{properties:?}"), "EffectProperties { effect_kind: Write, idempotency: IdempotentWithKey, scopes: [PermissionScope { resource: File, operation: Read, target: Some(\"a\") }, PermissionScope { resource: File, operation: Write, target: Some(\".cowd/checkpoints\") }, PermissionScope { resource: File, operation: Write, target: Some(\"a\") }], required_permission: WorkspaceWrite, approval_class: Policy, uses_network: false, spawns_process: false, mutates_packages: false, mutates_system: false }");
        let assessment = EffectAssessment {
            reversibility: EffectReversibility::Compensatable,
            externality: EffectExternality::Workspace,
            data_sensitivity: DataClassification::Internal,
            novelty: EffectNovelty::Routine,
            blast_radius: EffectBlastRadius::Workspace,
        };
        let expected = ToolEffectDescriptor {
            tool_id: "edit".into(),
            descriptor_hash: "437196b5cd33b6fe".into(),
            effect_kind: properties.effect_kind,
            idempotency: properties.idempotency,
            scopes: properties.scopes.clone(),
            required_permission: properties.required_permission,
            approval_class: properties.approval_class,
            uses_network: properties.uses_network,
            spawns_process: properties.spawns_process,
            mutates_packages: properties.mutates_packages,
            mutates_system: properties.mutates_system,
            assessment,
        };
        let resolution = resolve_registered_tool_effect_resolution(
            &resolver,
            "edit",
            &input,
            ToolPermissionMode::WorkspaceWrite,
        );
        assert_eq!(
            descriptor_hash(
                &resolver,
                "edit",
                &input,
                &properties,
                &expected.assessment,
                &[]
            ),
            expected.descriptor_hash
        );
        assert_eq!(resolution.descriptor, expected);
        assert_complete_purposes(&resolution);
        assert_eq!(
            resolution
                .scope_purposes
                .iter()
                .map(|entry| entry.purpose)
                .collect::<Vec<_>>(),
            vec![
                ProofEffectPurpose::InputRead,
                ProofEffectPurpose::AutoCheckpointMaintenance,
                ProofEffectPurpose::BusinessEffect,
            ]
        );
        let mut altered = resolution.clone();
        altered.scope_purposes[1].purpose = ProofEffectPurpose::BusinessEffect;
        assert_eq!(altered.descriptor, expected);
        assert_ne!(altered.scope_purposes, resolution.scope_purposes);
    }

    #[test]
    fn scope_purposes_cover_static_dynamic_composite_lsp_and_unknown_producers() {
        let edit_resolver = crate::builtin_effect_resolver_spec("edit");
        for input in [
            json!({"action":"write","path":"out","content":"literal"}),
            json!({"action":"replace","path":"out","old_string":"old","new_string":"new"}),
            json!({"action":"patch","edits":[{"path":"out"},{"path":"out"}]}),
        ] {
            let resolution = resolve_registered_tool_effect_resolution(
                &edit_resolver,
                "edit",
                &input,
                ToolPermissionMode::WorkspaceWrite,
            );
            assert_complete_purposes(&resolution);
            assert_eq!(
                resolution
                    .scope_purposes
                    .iter()
                    .filter(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance)
                    .count(),
                1
            );
            let data_only = resolve_orchestration_child_effect_resolution(
                &edit_resolver,
                "edit",
                &input,
                ToolPermissionMode::WorkspaceWrite,
                &[vec!["content".into()]],
            )
            .unwrap();
            assert_eq!(resolution.scope_purposes, data_only.scope_purposes);
            assert_complete_purposes(&data_only);
        }
        for (tool, input, permission) in [
            (
                "checkpoint",
                json!({"action":"create","paths":["src"]}),
                ToolPermissionMode::WorkspaceWrite,
            ),
            (
                "checkpoint",
                json!({"action":"restore","id":"checkpoint-one"}),
                ToolPermissionMode::WorkspaceWrite,
            ),
            (
                "checkpoint",
                json!({"action":"diff","id":"checkpoint-one"}),
                ToolPermissionMode::ReadOnly,
            ),
            (
                "checkpoint",
                json!({"action":"list"}),
                ToolPermissionMode::ReadOnly,
            ),
            (
                "edit",
                json!({"action":"notebook","notebook_path":"book.ipynb"}),
                ToolPermissionMode::WorkspaceWrite,
            ),
            (
                "read_file",
                json!({"path":".cowd/checkpoints"}),
                ToolPermissionMode::ReadOnly,
            ),
            (
                "edit",
                json!({"action":"invalid"}),
                ToolPermissionMode::WorkspaceWrite,
            ),
            (
                "unregistered",
                json!({}),
                ToolPermissionMode::DangerFullAccess,
            ),
        ] {
            let resolution = resolve_registered_tool_effect_resolution(
                &crate::builtin_effect_resolver_spec(tool),
                tool,
                &input,
                permission,
            );
            assert_complete_purposes(&resolution);
            assert!(
                !resolution
                    .scope_purposes
                    .iter()
                    .any(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance),
                "{tool}: {input}"
            );
        }
        let dynamic = resolve_orchestration_child_effect_resolution(
            &edit_resolver,
            "edit",
            &json!({"action":"write","path":"$source.path","content":"literal"}),
            ToolPermissionMode::WorkspaceWrite,
            &[vec!["path".into()]],
        )
        .unwrap();
        assert_complete_purposes(&dynamic);
        assert!(dynamic
            .scope_purposes
            .iter()
            .any(|entry| entry.declared_scope.target.is_none()
                && entry.purpose == ProofEffectPurpose::InputRead));
        assert!(dynamic
            .scope_purposes
            .iter()
            .any(|entry| entry.declared_scope.target.is_none()
                && entry.purpose == ProofEffectPurpose::BusinessEffect));
        assert_eq!(dynamic.descriptor.idempotency, ToolIdempotency::Unknown);
        let broad_read = resolve_orchestration_child_effect_resolution(
            &crate::builtin_effect_resolver_spec("read_file"),
            "read_file",
            &json!({"path":"$source.path"}),
            ToolPermissionMode::ReadOnly,
            &[vec!["path".into()]],
        )
        .unwrap();
        assert_complete_purposes(&broad_read);
        assert!(broad_read
            .scope_purposes
            .iter()
            .all(|entry| entry.purpose == ProofEffectPurpose::InputRead));
        for roots in [
            None,
            Some(vec![None]),
            Some(vec![Some("/registered/root".into())]),
        ] {
            let lsp = resolve_lsp_tool_effect_resolution(
                &crate::builtin_effect_resolver_spec("lsp"),
                &json!({"action":"definition","path":"src/a.rs"}),
                ToolPermissionMode::ReadOnly,
                roots.clone(),
            );
            assert_complete_purposes(&lsp);
            assert!(!lsp
                .scope_purposes
                .iter()
                .any(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance));
            if roots.is_none() {
                assert_eq!(lsp.descriptor.effect_kind, ToolEffectKind::Unknown);
            }
        }
        let maintenance = resolve_registered_tool_effect_resolution(
            &edit_resolver,
            "edit",
            &json!({"action":"write","path":"out","content":"literal"}),
            ToolPermissionMode::WorkspaceWrite,
        );
        let business = resolve_registered_tool_effect_resolution(
            &edit_resolver,
            "edit",
            &json!({"action":"write","path":".cowd/checkpoints","content":"explicit-business"}),
            ToolPermissionMode::WorkspaceWrite,
        );
        let composite_resolver = crate::builtin_effect_resolver_spec("tool_orchestrate");
        let joined = resolve_composite_tool_effect_resolution(
            &composite_resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[maintenance.clone(), business.clone()],
            &["original-child-fingerprint".into()],
            false,
        );
        let reversed = resolve_composite_tool_effect_resolution(
            &composite_resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[business, maintenance],
            &["original-child-fingerprint".into()],
            false,
        );
        assert_complete_purposes(&joined);
        assert_eq!(joined, reversed);
        assert!(joined
            .scope_purposes
            .iter()
            .any(
                |entry| entry.declared_scope.target.as_deref() == Some(".cowd/checkpoints")
                    && entry.declared_scope.operation == PermissionOperation::Write
                    && entry.purpose == ProofEffectPurpose::BusinessEffect
            ));
        assert!(joined
            .scope_purposes
            .iter()
            .any(
                |entry| entry.declared_scope.target.as_deref() == Some(".cowd/checkpoints")
                    && entry.declared_scope.operation == PermissionOperation::Read
                    && entry.purpose == ProofEffectPurpose::InputRead
            ));
        let unresolved = resolve_composite_tool_effect_resolution(
            &composite_resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[dynamic],
            &[],
            true,
        );
        assert_complete_purposes(&unresolved);
        assert_eq!(unresolved.descriptor.effect_kind, ToolEffectKind::Unknown);
    }

    #[test]
    fn r1_composite_effect_edit_leaf_declares_file_scope() {
        let resolver = crate::builtin_effect_resolver_spec("edit");
        assert_eq!(resolver.resolver_version, 3);
        for (input, operation, targets) in [
            (
                json!({"action":"write","path":"a","content":"x"}),
                PermissionOperation::Write,
                vec!["a"],
            ),
            (
                json!({"action":"replace","path":"b","old_string":"x","new_string":"y"}),
                PermissionOperation::Write,
                vec!["b"],
            ),
            (
                json!({"action":"preview","edits":[{"path":"b","old_string":"x","new_string":"y"},{"path":"a","old_string":"x","new_string":"y"}]}),
                PermissionOperation::Read,
                vec!["a", "b"],
            ),
            (
                json!({"action":"patch","edits":[{"path":"b","old_string":"x","new_string":"y"},{"path":"a","old_string":"x","new_string":"y"},{"path":"a","old_string":"y","new_string":"z"}]}),
                PermissionOperation::Write,
                vec!["a", "b"],
            ),
            (
                json!({"action":"notebook","notebook_path":"n.ipynb","new_source":"x"}),
                PermissionOperation::Write,
                vec!["n.ipynb"],
            ),
        ] {
            let effect = resolve_registered_tool_effect(
                &resolver,
                "edit",
                &input,
                ToolPermissionMode::WorkspaceWrite,
            );
            let mut expected = targets
                .iter()
                .map(|target| {
                    scope(
                        PermissionResource::File,
                        operation.clone(),
                        Some((*target).into()),
                    )
                })
                .collect::<Vec<_>>();
            match input["action"].as_str().unwrap() {
                "write" | "replace" | "patch" => {
                    for target in &targets {
                        expected.push(scope(
                            PermissionResource::File,
                            PermissionOperation::Read,
                            Some((*target).into()),
                        ));
                    }
                    expected.push(scope(
                        PermissionResource::File,
                        PermissionOperation::Write,
                        Some(".cowd/checkpoints".into()),
                    ));
                }
                "notebook" => expected.push(scope(
                    PermissionResource::File,
                    PermissionOperation::Read,
                    Some("n.ipynb".into()),
                )),
                _ => {}
            }
            expected.sort_by_key(|scope| serde_json::to_string(scope).unwrap());
            assert_eq!(effect.scopes, expected);
            if operation == PermissionOperation::Read {
                assert_eq!(effect.required_permission, ToolPermissionMode::ReadOnly);
                assert_eq!(effect.approval_class, ToolApprovalClass::None);
                assert_eq!(effect.idempotency, ToolIdempotency::Idempotent);
            } else {
                assert_eq!(
                    effect.required_permission,
                    ToolPermissionMode::WorkspaceWrite
                );
                assert_eq!(effect.approval_class, ToolApprovalClass::Policy);
                assert_eq!(effect.idempotency, ToolIdempotency::IdempotentWithKey);
            }
        }
        let secret = resolve_registered_tool_effect(
            &resolver,
            "edit",
            &json!({"action":"preview","edits":[{"path":".env"}]}),
            ToolPermissionMode::WorkspaceWrite,
        );
        assert_eq!(
            secret.assessment.data_sensitivity,
            DataClassification::Secret
        );
        let unrelated = resolve_registered_tool_effect(
            &crate::builtin_effect_resolver_spec("checkpoint"),
            "checkpoint",
            &json!({"action":"list","path":"a"}),
            ToolPermissionMode::WorkspaceWrite,
        );
        assert!(unrelated.scopes.contains(&scope(
            PermissionResource::Tool,
            PermissionOperation::Read,
            Some("a".into()),
        )));
        assert!(unrelated.scopes.contains(&scope(
            PermissionResource::File,
            PermissionOperation::Read,
            Some(".cowd/checkpoints".into()),
        )));
        let read = resolve_registered_tool_effect(
            &crate::builtin_effect_resolver_spec("read_file"),
            "read_file",
            &json!({"path":"a"}),
            ToolPermissionMode::ReadOnly,
        );
        assert_eq!(
            read.scopes,
            vec![scope(
                PermissionResource::File,
                PermissionOperation::Read,
                Some("a".into())
            )]
        );
    }

    #[test]
    fn r1_685_explicit_directory_roots_do_not_become_unknown_secret_scopes() {
        for root in [".", "./", "././"] {
            for (tool, input) in [
                ("workspace_snapshot", json!({"roots":[root]})),
                ("glob_search", json!({"path":root,"pattern":"*.rs"})),
                ("grep_search", json!({"path":root,"pattern":"needle"})),
                ("glob_search", json!({"patterns":[{"path":root,"pattern":"*.rs"}]})),
                ("grep_search", json!({"searches":[{"path":root,"pattern":"needle"}]})),
            ] {
                let effect = resolve_registered_tool_effect(
                    &crate::builtin_effect_resolver_spec(tool), tool, &input,
                    ToolPermissionMode::ReadOnly,
                );
                assert_eq!(effect.assessment.data_sensitivity, DataClassification::Internal, "{tool}: {root}");
                let files = effect.scopes.iter().filter(|scope| scope.resource == PermissionResource::File).collect::<Vec<_>>();
                assert_eq!(files.len(), 1, "{tool}: {root}");
                assert_eq!(files[0].target.as_deref(), Some("."), "{tool}: {root}");
                assert_eq!(files[0].operation, PermissionOperation::Read);
                assert_eq!(effect.required_permission, ToolPermissionMode::ReadOnly);
            }
        }
    }

    #[test]
    fn r1_685_directory_root_fix_preserves_unknown_and_secret_classification() {
        // (1) Still gated, and must stay so: an explicit secret-bearing target,
        // an explicit secret declaration, or a glob pattern that escapes the
        // workspace. Escaping patterns are refused outright, so the effect falls
        // back to the conservative unknown envelope, which demands
        // danger-full-access - a stronger gate than the Secret classification
        // it replaces, and one a read-only ceiling cannot satisfy either.
        for (tool, input) in [
            ("workspace_snapshot", json!({"roots":[".env"]})),
            ("workspace_snapshot", json!({"roots":["."],"contains_secrets":true})),
            ("workspace_snapshot", json!({"roots":["."],"data_classification":"secret"})),
        ] {
            let effect = resolve_registered_tool_effect(
                &crate::builtin_effect_resolver_spec(tool), tool, &input,
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(effect.assessment.data_sensitivity, DataClassification::Secret, "{tool}: {input}");
        }
        for (tool, input) in [
            ("glob_search", json!({"path":".","pattern":"../*.rs"})),
            ("glob_search", json!({"path":".","pattern":"/other/*.rs"})),
        ] {
            let effect = resolve_registered_tool_effect(
                &crate::builtin_effect_resolver_spec(tool), tool, &input,
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(
                effect.required_permission,
                ToolPermissionMode::DangerFullAccess,
                "an escaping glob pattern must not be certifiable under a read-only ceiling: {tool}: {input}"
            );
        }
        // (2) No longer Secret: a plain read-only research effect whose read
        // scope happens to carry no target. `read_file` with an explicit path
        // and `glob_search` with a recursive pattern both read workspace
        // content, and classifying only the latter as Secret is what forced
        // danger-full-access onto read-only research tools. Sensitivity follows
        // what the effect does, not whether its scope names a target.
        for (tool, input) in [
            ("workspace_snapshot", json!({})),
            ("workspace_snapshot", json!({"roots":[null]})),
            ("workspace_snapshot", json!({"roots":[""]})),
            ("workspace_snapshot", json!({"roots":[" "]})),
            ("workspace_snapshot", json!({"roots":[".",null]})),
            ("glob_search", json!({"pattern":"*.rs"})),
            ("glob_search", json!({"path":".","pattern":"nested/*.rs"})),
            ("grep_search", json!({"pattern":"needle"})),
        ] {
            let effect = resolve_registered_tool_effect(
                &crate::builtin_effect_resolver_spec(tool), tool, &input,
                ToolPermissionMode::ReadOnly,
            );
            assert_ne!(
                effect.assessment.data_sensitivity,
                DataClassification::Secret,
                "a read-only research effect must not be classified Secret: {tool}: {input}"
            );
        }
        assert!(crate::executor::orchestrate::original_static_file_scopes(
            "edit", &json!({"action":"write","path":".","content":"never"}),
        ).is_err());
    }

    #[test]
    fn static_filesystem_frontier_follows_original_input_owners() {
        let file_scopes = |tool: &str, input: Value| {
            resolve_registered_tool_effect(
                &crate::builtin_effect_resolver_spec(tool),
                tool,
                &input,
                ToolPermissionMode::ReadOnly,
            )
            .scopes
            .into_iter()
            .filter(|scope| scope.resource == PermissionResource::File)
            .map(|scope| (scope.operation, scope.target))
            .collect::<Vec<_>>()
        };
        for (tool, input, targets) in [
            (
                "glob_search",
                json!({"path":"src","pattern":"*.rs"}),
                vec![Some("src")],
            ),
            (
                "glob_search",
                json!({"patterns":[{"path":"src","pattern":"*.rs"},{"path":"tests","pattern":"*.rs"}]}),
                vec![Some("src"), Some("tests")],
            ),
            // Escaping patterns are refused, so the effect becomes the
            // conservative unknown envelope, which declares no File scope at
            // all and demands danger-full-access.
            (
                "glob_search",
                json!({"path":"src","pattern":"../*.rs"}),
                Vec::new(),
            ),
            (
                "glob_search",
                json!({"path":"src","pattern":"/other/*.rs"}),
                Vec::new(),
            ),
            (
                "glob_search",
                json!({"path":"src","pattern":"nested/*.rs"}),
                vec![Some(".")],
            ),
            (
                "glob_search",
                json!({"path":"crates","pattern":"*/src/*.rs"}),
                vec![Some(".")],
            ),
            (
                "glob_search",
                json!({"path":"crates/runtime","pattern":"**/*.rs"}),
                vec![Some(".")],
            ),
            (
                "glob_search",
                json!({"patterns":[{"path":"src","pattern":"**/*.rs"},{"path":"tests","pattern":"nested/*.rs"}]}),
                vec![Some(".")],
            ),
            (
                "glob_search",
                json!({"path":"src","pattern":"{..,nested}/*.rs"}),
                vec![None],
            ),
            (
                "glob_search",
                json!({"path":" ","pattern":"*.rs"}),
                vec![Some(".")],
            ),
            (
                "glob_search",
                json!({"pattern":"crates"}),
                vec![Some(".")],
            ),
            (
                "glob_search",
                json!({"patterns":[{"pattern":"*.rs"},{"path":null,"pattern":"**/*.rs"},{"path":"","pattern":"*.rs"},{"path":42,"pattern":"*.rs"}]}),
                vec![Some("."), None],
            ),
            (
                "grep_search",
                json!({"path":"src","pattern":"needle"}),
                vec![Some("src")],
            ),
            (
                "grep_search",
                json!({"pattern":"needle"}),
                vec![Some(".")],
            ),
            (
                "grep_search",
                json!({"searches":[{"pattern":"a"},{"path":null,"pattern":"b"},{"path":" ","pattern":"c"},{"path":42,"pattern":"d"}]}),
                vec![Some("."), None],
            ),
            (
                "grep_search",
                json!({"path":" src ","pattern":"needle"}),
                vec![Some(" src ")],
            ),
            (
                "grep_search",
                json!({"searches":[{"path":"src","pattern":"a"},{"path":"tests","pattern":"b"}]}),
                vec![Some("src"), Some("tests")],
            ),
            (
                "grep_search",
                json!({"path":"src","pattern":"fn","language":"rust"}),
                vec![Some("src")],
            ),
            (
                "workspace_snapshot",
                json!({"roots":["src","tests"],"path":"decoy"}),
                vec![Some("src"), Some("tests")],
            ),
            ("workspace_snapshot", json!({"path":"decoy"}), vec![Some(".")]),
            ("workspace_snapshot", json!({"roots":null}), vec![Some(".")]),
            ("workspace_snapshot", json!({"roots":"bad"}), vec![None]),
            (
                "vision_analyze",
                json!({"image_path":"image.png","path":"decoy"}),
                vec![Some("image.png")],
            ),
            ("read_file", json!({"image_path":"decoy"}), vec![None]),
            (
                "ask",
                json!({"message":"files","attachments":["src/a.rs","tests/a.rs"]}),
                vec![Some("src/a.rs"), Some("tests/a.rs")],
            ),
        ] {
            assert_eq!(
                file_scopes(tool, input),
                targets
                    .into_iter()
                    .map(|target| (PermissionOperation::Read, target.map(str::to_string)))
                    .collect::<Vec<_>>(),
                "{tool}"
            );
        }
        for tool in [
            "tool_search",
            "tool_cache_stats",
            "structured_output",
            "current_time",
            "get_context_remaining",
            "sleep",
            "background",
        ] {
            assert!(
                file_scopes(tool, json!({"path":"not-a-file-effect","action":"status"})).is_empty(),
                "{tool}"
            );
        }
        assert!(file_scopes(
            "mcp",
            json!({"action":"read","uri":"file:///remote","path":"decoy"})
        )
        .is_empty());
    }

    #[test]
    fn checkpoint_storage_and_restore_do_not_masquerade_as_exact_input_paths() {
        for (action, expected) in [
            (
                "list",
                vec![(PermissionOperation::Read, Some(".cowd/checkpoints"))],
            ),
            ("diff", vec![(PermissionOperation::Read, None)]),
            (
                "create",
                vec![
                    (PermissionOperation::Read, Some("src")),
                    (PermissionOperation::Write, Some(".cowd/checkpoints")),
                ],
            ),
            (
                "restore",
                vec![
                    (PermissionOperation::Read, None),
                    (PermissionOperation::Write, None),
                ],
            ),
        ] {
            let input =
                json!({"action":action,"id":"checkpoint-example","paths":["src"],"path":"decoy"});
            let effect = resolve_registered_tool_effect(
                &crate::builtin_effect_resolver_spec("checkpoint"),
                "checkpoint",
                &input,
                ToolPermissionMode::WorkspaceWrite,
            );
            let actual = effect
                .scopes
                .iter()
                .filter(|scope| scope.resource == PermissionResource::File)
                .map(|scope| (scope.operation.clone(), scope.target.as_deref()))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{action}");
            assert!(effect
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::Tool));
            assert_eq!(effect.uses_network, false);
            assert_eq!(effect.spawns_process, false);
            assert_eq!(
                effect.required_permission,
                if matches!(action, "list" | "diff") {
                    ToolPermissionMode::ReadOnly
                } else {
                    ToolPermissionMode::WorkspaceWrite
                }
            );
            if action == "create" {
                assert_eq!(effect.idempotency, ToolIdempotency::NonIdempotent);
            }
            if action == "restore" {
                assert_eq!(
                    effect.assessment.data_sensitivity,
                    DataClassification::Secret
                );
                assert_eq!(effect.assessment.blast_radius, EffectBlastRadius::Workspace);
            }
        }
        let escaped = resolve_registered_tool_effect(
            &crate::builtin_effect_resolver_spec("checkpoint"),
            "checkpoint",
            &json!({"action":"restore","id":"../escape"}),
            ToolPermissionMode::WorkspaceWrite,
        );
        assert_eq!(escaped.effect_kind, ToolEffectKind::Unknown);
        let control = resolve_registered_tool_effect(
            &ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".into(),
                resolver_version: 2,
            },
            "grep_search",
            &json!({"path":"src"}),
            ToolPermissionMode::ReadOnly,
        );
        assert!(control
            .scopes
            .iter()
            .all(|scope| scope.resource == PermissionResource::Tool));
    }

    #[test]
    fn composite_safety_joins_are_independent_and_hash_every_final_property() {
        let resolver = crate::builtin_effect_resolver_spec("tool_orchestrate");
        let child_resolver = ToolEffectResolverSpec {
            resolver_id: "runtime.external_danger".into(),
            resolver_version: 7,
        };
        let mut child = resolve_registered_tool_effect_resolution(
            &child_resolver,
            "external",
            &json!({"url":"https://example.invalid","contains_secrets":true}),
            ToolPermissionMode::ReadOnly,
        );
        child.descriptor.approval_class = ToolApprovalClass::Administrator;
        child.descriptor.mutates_packages = true;
        child.descriptor.mutates_system = true;
        child.descriptor.assessment.reversibility = EffectReversibility::Irreversible;
        child.descriptor.assessment.novelty = EffectNovelty::Unknown;
        child.descriptor.assessment.blast_radius = EffectBlastRadius::Unbounded;
        let joined = resolve_composite_tool_effect_resolution(
            &resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[child.clone()],
            &[child.descriptor.descriptor_hash.clone()],
            false,
        )
        .descriptor;
        assert_eq!(joined.approval_class, ToolApprovalClass::Administrator);
        assert_eq!(joined.idempotency, ToolIdempotency::NonIdempotent);
        assert_eq!(
            joined.required_permission,
            ToolPermissionMode::DangerFullAccess
        );
        assert!(
            joined.uses_network
                && joined.spawns_process
                && joined.mutates_packages
                && joined.mutates_system
        );
        assert_eq!(joined.assessment, child.descriptor.assessment);
        let original_hash = joined.descriptor_hash;
        child.descriptor.assessment.blast_radius = EffectBlastRadius::System;
        let changed = resolve_composite_tool_effect_resolution(
            &resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[child.clone()],
            &[child.descriptor.descriptor_hash.clone()],
            false,
        )
        .descriptor;
        assert_ne!(original_hash, changed.descriptor_hash);
        child.descriptor.assessment.blast_radius = EffectBlastRadius::Unbounded;
        child.descriptor.approval_class = ToolApprovalClass::User;
        assert_ne!(
            original_hash,
            resolve_composite_tool_effect_resolution(
                &resolver,
                &json!({}),
                ToolPermissionMode::ReadOnly,
                &[child.clone()],
                &[child.descriptor.descriptor_hash.clone()],
                false
            )
            .descriptor
            .descriptor_hash
        );
        child.descriptor.idempotency = ToolIdempotency::IdempotentWithKey;
        assert_eq!(
            resolve_composite_tool_effect_resolution(
                &resolver,
                &json!({}),
                ToolPermissionMode::ReadOnly,
                &[child],
                &[],
                false
            )
            .descriptor
            .idempotency,
            ToolIdempotency::Unknown
        );
    }

    #[test]
    fn scoped_edit_effects_are_input_exact_and_version_fenced() {
        let resolver = crate::builtin_effect_resolver_spec("edit");
        assert_eq!(resolver.resolver_version, 3);
        for (input, targets) in [
            (
                json!({"action":"write","path":"src/a","content":"changed"}),
                vec!["src/a"],
            ),
            (
                json!({"action":"replace","path":"src/a","old_string":"before","new_string":"after"}),
                vec!["src/a"],
            ),
            (
                json!({"action":"patch","edits":[
                    {"path":"src/b","old_string":"before","new_string":"after"},
                    {"path":"src/a","old_string":"before","new_string":"after"}
                ]}),
                vec!["src/a", "src/b"],
            ),
        ] {
            let effect = resolve_registered_tool_effect(
                &resolver,
                "edit",
                &input,
                ToolPermissionMode::WorkspaceWrite,
            );
            let reads = effect
                .scopes
                .iter()
                .filter(|scope| {
                    scope.resource == PermissionResource::File
                        && scope.operation == PermissionOperation::Read
                })
                .map(|scope| scope.target.as_deref().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(reads, targets);
            for target in targets {
                assert!(effect.scopes.contains(&scope(
                    PermissionResource::File,
                    PermissionOperation::Write,
                    Some(target.into())
                )));
            }
            assert!(effect.scopes.contains(&scope(
                PermissionResource::File,
                PermissionOperation::Write,
                Some(".cowd/checkpoints".into())
            )));
            assert_eq!(
                effect.required_permission,
                ToolPermissionMode::WorkspaceWrite
            );
            assert_eq!(
                effect.assessment.data_sensitivity,
                DataClassification::Internal
            );
            assert!(
                !effect.uses_network
                    && !effect.spawns_process
                    && !effect.mutates_packages
                    && !effect.mutates_system
            );
        }
        for target in [".env", ".ssh/id_ed25519", "credentials.json", "secrets.txt"] {
            let input = json!({"action":"write","path":target,"content":"fixture","data_classification":"internal"});
            assert_eq!(
                resolve_registered_tool_effect(
                    &resolver,
                    "edit",
                    &input,
                    ToolPermissionMode::WorkspaceWrite
                )
                .assessment
                .data_sensitivity,
                DataClassification::Secret
            );
        }
        assert_eq!(resolve_registered_tool_effect(&resolver, "edit",
            &json!({"action":"write","path":"src/a","content":"fixture","contains_secrets":true}),
            ToolPermissionMode::WorkspaceWrite).assessment.data_sensitivity, DataClassification::Secret);
        for input in [
            json!({}),
            json!({"action":"write"}),
            json!({"action":"write","path":""}),
            json!({"action":"patch","edits":[]}),
            json!({"action":"patch","edits":[{"path":"src/a"},{}]}),
            json!({"action":"unknown","path":"src/a"}),
        ] {
            assert_eq!(
                resolve_registered_tool_effect(
                    &resolver,
                    "edit",
                    &input,
                    ToolPermissionMode::WorkspaceWrite
                )
                .effect_kind,
                ToolEffectKind::Unknown
            );
        }
        let dynamic_input = json!({"action":"write","path":"$read.file.path","content":"changed"});
        let references = vec![vec!["path".to_string()]];
        let dynamic = resolve_orchestration_child_effect_resolution(
            &resolver,
            "edit",
            &dynamic_input,
            ToolPermissionMode::WorkspaceWrite,
            &references,
        )
        .unwrap()
        .descriptor;
        assert!(dynamic.scopes.contains(&scope(
            PermissionResource::File,
            PermissionOperation::Read,
            None
        )));
        assert!(dynamic.scopes.contains(&scope(
            PermissionResource::File,
            PermissionOperation::Write,
            None
        )));
        assert!(dynamic.scopes.contains(&scope(
            PermissionResource::File,
            PermissionOperation::Write,
            Some(".cowd/checkpoints".into())
        )));
        assert_eq!(
            dynamic.assessment.data_sensitivity,
            DataClassification::Secret
        );
        for version in [0, 1, 2, 4, u32::MAX] {
            let obsolete = ToolEffectResolverSpec {
                resolver_id: resolver.resolver_id.clone(),
                resolver_version: version,
            };
            assert_eq!(
                resolve_registered_tool_effect(
                    &obsolete,
                    "edit",
                    &json!({"action":"write","path":"src/a","content":"changed"}),
                    ToolPermissionMode::WorkspaceWrite
                )
                .effect_kind,
                ToolEffectKind::Unknown
            );
            assert!(resolve_orchestration_child_effect_resolution(
                &obsolete,
                "edit",
                &dynamic_input,
                ToolPermissionMode::WorkspaceWrite,
                &references
            )
            .is_err());
        }
        let current = resolve_registered_tool_effect(
            &resolver,
            "edit",
            &json!({"action":"write","path":"out/a","content":"captured-v2-edit"}),
            ToolPermissionMode::WorkspaceWrite,
        );
        assert_ne!(current.descriptor_hash, "a556282fb072b286");
    }

    #[test]
    fn effect_hash_binds_resolver_semantics_child_fingerprints_and_canonical_input() {
        let mut resolver = crate::builtin_effect_resolver_spec("edit");
        let first = json!({"action":"write","path":"a","content":"x"});
        let reordered: Value =
            serde_json::from_str(r#"{"content":"x","path":"a","action":"write"}"#).unwrap();
        let effect = resolve_registered_tool_effect(
            &resolver,
            "edit",
            &first,
            ToolPermissionMode::WorkspaceWrite,
        );
        assert_eq!(
            effect.descriptor_hash,
            resolve_registered_tool_effect(
                &resolver,
                "edit",
                &reordered,
                ToolPermissionMode::WorkspaceWrite
            )
            .descriptor_hash
        );
        resolver.resolver_version += 1;
        assert_ne!(
            effect.descriptor_hash,
            resolve_registered_tool_effect(
                &resolver,
                "edit",
                &first,
                ToolPermissionMode::WorkspaceWrite
            )
            .descriptor_hash
        );
        let effect = resolve_registered_tool_effect_resolution(
            &crate::builtin_effect_resolver_spec("edit"),
            "edit",
            &first,
            ToolPermissionMode::WorkspaceWrite,
        );
        let resolver = crate::builtin_effect_resolver_spec("tool_orchestrate");
        let first = resolve_composite_tool_effect_resolution(
            &resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[effect.clone()],
            &["child-v1".into()],
            false,
        )
        .descriptor;
        let second = resolve_composite_tool_effect_resolution(
            &resolver,
            &json!({}),
            ToolPermissionMode::ReadOnly,
            &[effect],
            &["child-v2".into()],
            false,
        )
        .descriptor;
        assert_ne!(first.descriptor_hash, second.descriptor_hash);
        assert_ne!(
            canonical_json(&json!({"a:b":1})),
            canonical_json(&json!({"a":{"b":1}}))
        );
    }

    #[test]
    fn r1_typed_control_resolver_version_and_effect_contract() {
        for family in ["runtime.agent_action", "runtime.private_note"] {
            let resolver = ToolEffectResolverSpec {
                resolver_id: family.into(),
                resolver_version: 2,
            };
            let input = serde_json::json!({"action":"claim","task_ref":"task:one"});
            let effect = resolve_registered_tool_effect(
                &resolver,
                "task_claim",
                &input,
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(effect.effect_kind, ToolEffectKind::Write);
            assert_eq!(effect.idempotency, ToolIdempotency::IdempotentWithKey);
            assert_eq!(effect.assessment.externality, EffectExternality::Internal);
            assert_eq!(effect.assessment.blast_radius, EffectBlastRadius::Item);
            assert_eq!(
                effect.assessment.reversibility,
                EffectReversibility::Compensatable
            );
            assert_eq!(effect.assessment.novelty, EffectNovelty::Routine);
            assert_eq!(
                effect,
                resolve_registered_tool_effect(
                    &resolver,
                    "task_claim",
                    &serde_json::json!({"task_ref":"task:one","action":"claim"}),
                    ToolPermissionMode::ReadOnly
                )
            );
            for version in [0, 1, 3, u32::MAX] {
                let old = resolve_registered_tool_effect(
                    &ToolEffectResolverSpec {
                        resolver_id: family.into(),
                        resolver_version: version,
                    },
                    "task_claim",
                    &input,
                    ToolPermissionMode::ReadOnly,
                );
                assert_eq!(old.effect_kind, ToolEffectKind::Unknown);
                assert_eq!(
                    old.required_permission,
                    ToolPermissionMode::DangerFullAccess
                );
                assert_ne!(old.descriptor_hash, effect.descriptor_hash);
            }
            for floor in [
                ToolPermissionMode::WorkspaceWrite,
                ToolPermissionMode::DangerFullAccess,
            ] {
                let raised = resolve_registered_tool_effect(&resolver, "task_claim", &input, floor);
                assert_eq!(raised.required_permission, floor);
                assert_ne!(raised.assessment.externality, EffectExternality::Internal);
            }
            for sensitivity in ["public", "internal", "confidential", "secret"] {
                let marked = resolve_registered_tool_effect(
                    &resolver,
                    "private_note",
                    &serde_json::json!({"data_classification":sensitivity}),
                    ToolPermissionMode::ReadOnly,
                );
                assert_eq!(marked.assessment.externality, EffectExternality::Internal);
                assert_eq!(
                    marked.assessment.data_sensitivity,
                    match sensitivity {
                        "public" => DataClassification::Public,
                        "confidential" => DataClassification::Confidential,
                        "secret" => DataClassification::Secret,
                        _ => DataClassification::Internal,
                    }
                );
            }
            let secret = resolve_registered_tool_effect(
                &resolver,
                "private_note",
                &serde_json::json!({"contains_secrets":true}),
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(
                secret.assessment.data_sensitivity,
                DataClassification::Secret
            );
            let base = resolve_base_effect_properties(&resolver, "task_claim", &input);
            let mut multiple = base.clone();
            multiple.scopes.push(scope(
                PermissionResource::Tool,
                PermissionOperation::Control,
                Some("second".into()),
            ));
            assert_eq!(
                effect_assessment(&multiple, &input).externality,
                EffectExternality::Internal
            );
            let mut variants = Vec::new();
            let mut empty = base.clone();
            empty.scopes.clear();
            variants.push(empty);
            for operation in [
                PermissionOperation::Read,
                PermissionOperation::Write,
                PermissionOperation::Call,
                PermissionOperation::Execute,
            ] {
                let mut changed = base.clone();
                changed.scopes[0].operation = operation;
                variants.push(changed);
            }
            for resource in [
                PermissionResource::File,
                PermissionResource::Network,
                PermissionResource::Shell,
                PermissionResource::Provider,
            ] {
                let mut changed = base.clone();
                changed
                    .scopes
                    .push(scope(resource, PermissionOperation::Read, None));
                variants.push(changed);
            }
            for idempotency in [
                ToolIdempotency::Idempotent,
                ToolIdempotency::NonIdempotent,
                ToolIdempotency::Unknown,
            ] {
                let mut changed = base.clone();
                changed.idempotency = idempotency;
                variants.push(changed);
            }
            for approval in [
                ToolApprovalClass::Policy,
                ToolApprovalClass::User,
                ToolApprovalClass::Administrator,
            ] {
                let mut changed = base.clone();
                changed.approval_class = approval;
                variants.push(changed);
            }
            for flag in 0..4 {
                let mut changed = base.clone();
                match flag {
                    0 => changed.uses_network = true,
                    1 => changed.spawns_process = true,
                    2 => changed.mutates_packages = true,
                    _ => changed.mutates_system = true,
                }
                variants.push(changed);
            }
            for changed in variants {
                assert_ne!(
                    effect_assessment(&changed, &input).externality,
                    EffectExternality::Internal,
                    "{changed:?}"
                );
            }
            let read = resolve_registered_tool_effect(
                &resolver,
                harness_contract::agent_action::STATE_INSPECT_TOOL_ID,
                &input,
                ToolPermissionMode::ReadOnly,
            );
            assert_eq!(read.effect_kind, ToolEffectKind::Read);
            let definition = crate::RuntimeToolDefinition {
                name: "snapshot_probe_tool".into(),
                description: Some("frozen control registration".into()),
                input_schema: serde_json::json!({"type":"object"}),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: ToolEffectResolverSpec {
                    resolver_id: "runtime.readonly".into(),
                    resolver_version: 2,
                },
            };
            let mut historical = definition.clone();
            historical.effect_resolver.resolver_version = 1;
            let old_catalog = crate::ToolCatalog::builtin()
                .with_runtime_tools(vec![historical])
                .unwrap();
            let new_catalog = crate::ToolCatalog::builtin()
                .with_runtime_tools(vec![definition])
                .unwrap();
            let old_snapshot = crate::ToolHostSnapshot::new(
                std::sync::Arc::new(old_catalog),
                std::sync::Arc::new(crate::lsp_client::LspRegistry::default()),
                None,
            );
            let new_snapshot = crate::ToolHostSnapshot::new(
                std::sync::Arc::new(new_catalog),
                std::sync::Arc::new(crate::lsp_client::LspRegistry::default()),
                None,
            );
            assert_ne!(
                old_snapshot.descriptor_set_hash,
                new_snapshot.descriptor_set_hash
            );
        }
    }

    #[test]
    fn private_note_is_an_idempotent_runtime_write_without_workspace_authority() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "runtime.private_note".into(),
            resolver_version: 2,
        };
        let effect = resolve_registered_tool_effect(
            &resolver,
            "private_note",
            &json!({"kind":"hypothesis","title":"note","content":"maybe"}),
            ToolPermissionMode::ReadOnly,
        );
        assert_eq!(effect.effect_kind, ToolEffectKind::Write);
        assert_eq!(effect.idempotency, ToolIdempotency::IdempotentWithKey);
        assert_eq!(effect.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(effect.approval_class, ToolApprovalClass::None);
        assert!(!effect.uses_network && !effect.spawns_process && !effect.mutates_system);
    }

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
    fn execute_code_is_a_workspace_bounded_sandbox_process() {
        let resolver = ToolEffectResolverSpec {
            resolver_id: "builtin.sandboxed_command".to_string(),
            resolver_version: 1,
        };
        let descriptor = resolve_registered_tool_effect(
            &resolver,
            "execute_code",
            &json!({
                "language": "python",
                "code": "print(sum(range(10)))",
                "input_files": ["crates/tools/src/lib.rs"]
            }),
            ToolPermissionMode::ReadOnly,
        );

        assert_eq!(descriptor.effect_kind, ToolEffectKind::Process);
        assert_eq!(descriptor.required_permission, ToolPermissionMode::ReadOnly);
        assert_eq!(descriptor.approval_class, ToolApprovalClass::Policy);
        assert!(descriptor.spawns_process);
        assert!(!descriptor.uses_network);
        assert!(!descriptor.mutates_packages);
        assert!(!descriptor.mutates_system);
        assert_eq!(
            descriptor.assessment.externality,
            EffectExternality::Workspace
        );
        assert_eq!(
            descriptor.assessment.blast_radius,
            EffectBlastRadius::Workspace
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
