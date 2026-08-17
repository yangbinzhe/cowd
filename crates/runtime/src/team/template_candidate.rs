//! AI-authored Team template candidates.
//!
//! The model drafts a structured team template; this module compiles it into
//! a validated `TeamTemplateManifest`, clips every role's grant ceiling to the
//! caller's permission ceiling, produces an audit preview, and publishes it as
//! an immutable User-scope revision. Display names never participate in
//! behavior; permission and acceptance contracts are the only execution facts.

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionLifecycle, RevisionSelector,
};
use harness_contract::policy::PermissionMode;
use harness_contract::team::definition::{RoleDisplayName, TeamTemplateDisplay};
use harness_contract::team::{
    RoleCardinalityPolicy, RolePartitionPolicy, TeamEvaluationContract, TeamResultContract,
    TeamRoleDefinition, TeamRoleDependency, TeamRoleTaskContract, TeamTemplateDefinitionId,
    TeamTemplateManifest, TeamTopologyContract,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::RuntimeDefinitionRegistry;

const AI_TEMPLATE_PROTOCOL: &str = "ai-authored@1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamTemplateProposal {
    pub template_id: String,
    pub name: String,
    #[serde(default)]
    pub team_display_name: Option<String>,
    #[serde(default)]
    pub role_display_names: Vec<RoleDisplayName>,
    pub roles: Vec<ProposedRole>,
    #[serde(default)]
    pub dependencies: Vec<ProposedDependency>,
    #[serde(default)]
    pub result_fields: Vec<String>,
    #[serde(default)]
    pub evidence_required: bool,
    pub instructions: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedRole {
    pub role_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub responsibility: String,
    pub agent_definition_ref: String,
    #[serde(default)]
    pub grant_ceiling: Vec<String>,
    #[serde(default)]
    pub fixed_count: Option<u32>,
    #[serde(default)]
    pub min_count: Option<u32>,
    #[serde(default)]
    pub max_count: Option<u32>,
    #[serde(default)]
    pub acceptance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedDependency {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone)]
pub struct TemplateCandidate {
    pub manifest: TeamTemplateManifest,
    pub digest: String,
    pub preview: serde_json::Value,
}

/// Normalizes common model-authoring shortcuts in a `template_proposal` JSON
/// value so the strict contract only sees canonical shapes. Returns audit
/// notes describing every structural default/normalization that was applied.
///
/// The function is deliberately total: malformed model input produces a
/// descriptive `Err` instead of a panic, and wrapped shapes (a JSON string
/// containing the proposal, or a single-element array wrapping it) are
/// unwrapped before validation.
pub(crate) fn normalize_template_proposal(
    value: &mut serde_json::Value,
) -> Result<Vec<String>, String> {
    let mut notes = Vec::new();
    if let Some(encoded) = value.as_str() {
        let decoded: serde_json::Value = serde_json::from_str(encoded).map_err(|error| {
            format!("template_proposal is a JSON string that failed to parse: {error}")
        })?;
        notes.push("unwrapped string-encoded template_proposal".to_string());
        *value = decoded;
    } else if let Some(items) = value.as_array() {
        if items.len() != 1 {
            return Err(format!(
                "template_proposal array must contain exactly one template object, got {} elements",
                items.len()
            ));
        }
        let mut inner = items[0].clone();
        if let Some(encoded) = inner.as_str() {
            inner = serde_json::from_str(encoded).map_err(|error| {
                format!("template_proposal array element failed to parse as JSON: {error}")
            })?;
        }
        notes.push("unwrapped single-element template_proposal array".to_string());
        *value = inner;
    }
    if !value.is_object() {
        return Err(format!(
            "template_proposal must be a JSON object, got {}",
            json_type_name(value)
        ));
    }
    for key in ["template_id", "name", "team_display_name"] {
        if let Some(raw) = value.get(key).cloned() {
            if !raw.is_string() {
                value[key] = serde_json::json!(raw.to_string());
                notes.push(format!("stringified non-string `{key}`"));
            }
        }
    }
    if value.get("instructions").is_none() {
        value["instructions"] =
            serde_json::json!("# 协作研讨\n\n分工调研、对抗质询并收敛为统一结论。\n");
        notes.push("defaulted missing instructions".to_string());
    }
    let Some(roles_value) = value.get("roles").cloned() else {
        return Err("template_proposal is missing required field `roles`".to_string());
    };
    let role_items = match roles_value {
        serde_json::Value::Array(items) => items,
        serde_json::Value::Object(map) if map.contains_key("role_id") => {
            vec![serde_json::Value::Object(map)]
        }
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(role_id, mut role)| {
                if role.is_object() && role.get("role_id").is_none() {
                    role["role_id"] = serde_json::json!(role_id);
                }
                role
            })
            .collect::<Vec<_>>(),
        other => {
            return Err(format!(
                "roles must be an array or object, got {}",
                json_type_name(&other)
            ))
        }
    };
    if role_items.is_empty() {
        return Err("roles must contain at least one role".to_string());
    }
    let mut normalized_roles = Vec::with_capacity(role_items.len());
    for mut role in role_items {
        normalize_proposed_role(&mut role, &mut notes)?;
        normalized_roles.push(role);
    }
    value["roles"] = serde_json::json!(normalized_roles);
    if let Some(displays) = value.get("role_display_names").cloned() {
        let roles = value["roles"].as_array().cloned().unwrap_or_default();
        let normalized = match displays {
            serde_json::Value::Object(map) => map
                .into_iter()
                .map(|(role_id, display_name)| {
                    let display_name = if display_name.is_string() {
                        display_name
                    } else {
                        serde_json::json!(display_name.to_string())
                    };
                    serde_json::json!({ "role_id": role_id, "display_name": display_name })
                })
                .collect::<Vec<_>>(),
            serde_json::Value::Array(items) => {
                let mut out = Vec::new();
                for (index, item) in items.into_iter().enumerate() {
                    match item {
                        serde_json::Value::String(display_name) => {
                            let role_id = roles
                                .get(index)
                                .and_then(|role| role.get("role_id"))
                                .and_then(serde_json::Value::as_str)
                                .ok_or_else(|| {
                                    format!("role_display_names[{index}] has no matching role_id")
                                })?;
                            out.push(serde_json::json!({
                                "role_id": role_id,
                                "display_name": display_name
                            }));
                        }
                        serde_json::Value::Object(mut map) => {
                            if map.get("role_id").is_none() {
                                let role_id = roles
                                    .get(index)
                                    .and_then(|role| role.get("role_id"))
                                    .and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| {
                                        format!("role_display_names[{index}] has no matching role_id")
                                    })?;
                                map.insert("role_id".to_string(), serde_json::json!(role_id));
                            }
                            if map.get("display_name").is_none() {
                                return Err(format!(
                                    "role_display_names[{index}] is missing display_name"
                                ));
                            }
                            out.push(serde_json::Value::Object(map));
                        }
                        other => {
                            return Err(format!(
                                "role_display_names items must be strings or objects, got {}",
                                json_type_name(&other)
                            ))
                        }
                    }
                }
                out
            }
            other => {
                return Err(format!(
                    "role_display_names must be an object or array, got {}",
                    json_type_name(&other)
                ))
            }
        };
        value["role_display_names"] = serde_json::json!(normalized);
    }
    if let Some(fields) = value.get("result_fields").cloned() {
        let normalized = match fields {
            serde_json::Value::String(raw) => serde_json::json!([raw]),
            serde_json::Value::Object(map) => {
                serde_json::json!(map.keys().cloned().collect::<Vec<_>>())
            }
            serde_json::Value::Array(_) => fields,
            other => {
                return Err(format!(
                    "result_fields must be a string, array, or object, got {}",
                    json_type_name(&other)
                ))
            }
        };
        value["result_fields"] = normalized;
    }
    normalize_dependencies(value, &mut notes)?;
    Ok(notes)
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(enabled) => *enabled,
        serde_json::Value::String(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "true" | "yes" | "1" | "enabled" | "on"
        ),
        serde_json::Value::Number(number) => {
            number.as_u64().map(|number| number != 0).unwrap_or(true)
        }
        _ => true,
    }
}

const KNOWN_CAPABILITIES: [&str; 5] = ["read", "search", "write", "test", "network"];

fn revision_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse::<u64>().ok()))
}

fn capability_names_from_string(raw: &str) -> Result<Vec<String>, String> {
    let mut names = Vec::new();
    for token in raw.split(|character: char| !character.is_ascii_alphanumeric()) {
        let lower = token.to_ascii_lowercase();
        if KNOWN_CAPABILITIES.contains(&lower.as_str()) && !names.contains(&lower) {
            names.push(lower);
        }
    }
    if names.is_empty() {
        return Err(format!(
            "grant_ceiling `{raw}` contains no known capability (read|search|write|test|network)"
        ));
    }
    Ok(names)
}

fn normalize_ref_string(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("agent_definition_ref is empty".to_string());
    }
    let (path, revision) = match raw.split_once('@') {
        Some((path, revision)) => (
            path,
            revision
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("agent_definition_ref `{raw}` has an invalid revision"))?,
        ),
        None => (raw, 1),
    };
    let path = match path.split('/').count() {
        1 => format!("builtin/cowd/{path}"),
        2 if path.starts_with("cowd/") => format!("builtin/{path}"),
        _ => path.to_string(),
    };
    Ok(format!("{path}@{revision}"))
}

fn normalize_agent_definition_ref(
    fields: &mut serde_json::Map<String, serde_json::Value>,
    ceiling_names: &[String],
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let role_id = fields
        .get("role_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("<role>");
    let Some(value) = fields.get("agent_definition_ref").cloned() else {
        let default_ref = if ceiling_names
            .iter()
            .any(|name| matches!(name.as_str(), "write" | "test"))
        {
            "builtin/cowd/execute@1"
        } else if ceiling_names.iter().any(|name| name == "network") {
            "builtin/cowd/explore@2"
        } else {
            "builtin/cowd/explore@1"
        };
        notes.push(format!(
            "role `{role_id}`: defaulted agent_definition_ref to `{default_ref}`"
        ));
        fields.insert(
            "agent_definition_ref".to_string(),
            serde_json::json!(default_ref),
        );
        return Ok(());
    };
    if value.is_null() {
        // An explicit `null` is the model saying "no preference", exactly
        // like an absent field: use the safe builtin default.
        fields.remove("agent_definition_ref");
        return normalize_agent_definition_ref(fields, ceiling_names, notes);
    }
    let normalized = match value {
        serde_json::Value::String(raw) => normalize_ref_string(&raw)?,
        serde_json::Value::Object(reference) => {
            let definition =
                if let Some(value) = reference.get("definition").or_else(|| reference.get("name")) {
                    value.as_str().map(str::to_string).ok_or_else(|| {
                        format!(
                            "role `{role_id}` agent_definition_ref definition must be a string"
                        )
                    })?
                } else if reference.len() == 1 {
                    reference.keys().next().cloned().unwrap_or_default()
                } else {
                    return Err(format!(
                        "role `{role_id}` agent_definition_ref object requires `definition` (or a single key)"
                    ));
                };
            let revision = reference
                .get("revision")
                .and_then(revision_u64)
                .or_else(|| {
                    reference
                        .values()
                        .find(|value| value.as_u64().is_some() || value.as_str().is_some())
                        .and_then(revision_u64)
                })
                .unwrap_or(1);
            format!("{}@{}", definition, revision)
        }
        serde_json::Value::Array(items) => match items.as_slice() {
            [serde_json::Value::String(definition)] => format!("{definition}@1"),
            [serde_json::Value::String(definition), revision] => {
                let revision = revision_u64(revision).ok_or_else(|| {
                    format!(
                        "role `{role_id}` agent_definition_ref revision must be a non-negative integer"
                    )
                })?;
                format!("{definition}@{revision}")
            }
            _ => {
                return Err(format!(
                    "role `{role_id}` agent_definition_ref array must be [\"definition\"] or [\"definition\", revision]"
                ))
            }
        },
        other => {
            return Err(format!(
                "role `{role_id}` agent_definition_ref must be a string, object, or array, got {}",
                json_type_name(&other)
            ))
        }
    };
    let normalized = normalize_ref_string(&normalized)?;
    fields.insert(
        "agent_definition_ref".to_string(),
        serde_json::json!(normalized),
    );
    Ok(())
}

fn normalize_proposed_role(
    role: &mut serde_json::Value,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let Some(fields) = role.as_object_mut() else {
        return Err(format!("role must be an object, got {}", json_type_name(role)));
    };
    let role_id = fields
        .get("role_id")
        .and_then(serde_json::Value::as_str)
        .filter(|role_id| !role_id.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| "every proposed role needs a non-empty role_id".to_string())?;
    if fields.get("display_name").is_none() {
        fields.insert("display_name".to_string(), serde_json::json!(role_id));
        notes.push(format!("role `{role_id}`: defaulted display_name to role_id"));
    }
    if fields.get("responsibility").is_none() {
        let display_name = fields
            .get("display_name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&role_id);
        fields.insert(
            "responsibility".to_string(),
            serde_json::json!(format!("执行 {display_name} 的职责并产出可验证证据")),
        );
        notes.push(format!("role `{role_id}`: defaulted missing responsibility"));
    }
    let mut ceiling_names: Vec<String> = Vec::new();
    match fields.get("grant_ceiling").cloned() {
        None => {}
        Some(serde_json::Value::String(raw)) => {
            ceiling_names = capability_names_from_string(&raw)?;
            fields.insert("grant_ceiling".to_string(), serde_json::json!(ceiling_names));
        }
        Some(serde_json::Value::Object(ceiling)) => {
            let normalized = ceiling
                .into_iter()
                .filter(|(_, enabled)| truthy(enabled))
                .map(|(capability, _)| serde_json::json!(capability))
                .collect::<Vec<_>>();
            fields.insert("grant_ceiling".to_string(), serde_json::json!(normalized));
        }
        Some(serde_json::Value::Array(items)) => {
            let mut names = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::String(name) => names.push(name),
                    serde_json::Value::Object(map) => {
                        for (capability, enabled) in map {
                            if truthy(&enabled) {
                                names.push(capability);
                            }
                        }
                    }
                    _ => {
                        return Err(format!(
                            "role `{role_id}` grant_ceiling array items must be strings or objects"
                        ))
                    }
                }
            }
            let mut seen = std::collections::BTreeSet::new();
            for name in names {
                let lower = name.to_ascii_lowercase();
                if !KNOWN_CAPABILITIES.contains(&lower.as_str()) {
                    return Err(format!(
                        "role `{role_id}` grant_ceiling contains unknown capability `{name}` (read|search|write|test|network)"
                    ));
                }
                if seen.insert(lower.clone()) {
                    ceiling_names.push(lower);
                }
            }
            fields.insert("grant_ceiling".to_string(), serde_json::json!(ceiling_names));
        }
        Some(other) => {
            return Err(format!(
                "role `{role_id}` grant_ceiling must be a string, array, or object, got {}",
                json_type_name(&other)
            ))
        }
    }
    if let Some(cardinality) = fields.get("cardinality").cloned() {
        let count = revision_u64(&cardinality).ok_or_else(|| {
            format!("role `{role_id}` cardinality must be a positive integer")
        })?;
        if count == 0 {
            return Err(format!("role `{role_id}` cardinality must be > 0"));
        }
        fields.insert("fixed_count".to_string(), serde_json::json!(count));
        notes.push(format!(
            "role `{role_id}`: normalized cardinality {count} to fixed_count"
        ));
    }
    normalize_agent_definition_ref(fields, &ceiling_names, notes)?;
    if let Some(acceptance) = fields.get("acceptance").cloned() {
        let normalized = match acceptance {
            serde_json::Value::String(raw) => serde_json::json!([raw]),
            serde_json::Value::Object(map) => {
                serde_json::json!(map.keys().cloned().collect::<Vec<_>>())
            }
            serde_json::Value::Array(_) => acceptance,
            serde_json::Value::Number(number) => serde_json::json!([number.to_string()]),
            other => {
                return Err(format!(
                    "role `{role_id}` acceptance must be a string, array, or object, got {}",
                    json_type_name(&other)
                ))
            }
        };
        fields.insert("acceptance".to_string(), normalized);
    }
    Ok(())
}

fn resolve_member_roles(
    member: &str,
    role_ids: &[String],
    groups: &std::collections::BTreeMap<String, Vec<String>>,
    seen: &mut std::collections::BTreeSet<String>,
) -> Result<Vec<String>, String> {
    if role_ids.iter().any(|role_id| role_id == member) {
        return Ok(vec![member.to_string()]);
    }
    let Some(members) = groups.get(member) else {
        return Err(format!(
            "dependency member `{member}` is neither a role_id nor a dependency group"
        ));
    };
    if !seen.insert(member.to_string()) {
        return Err(format!("dependency group cycle detected at `{member}`"));
    }
    let mut resolved = Vec::new();
    for nested in members {
        resolved.extend(resolve_member_roles(nested, role_ids, groups, seen)?);
    }
    seen.remove(member);
    Ok(resolved)
}

fn resolve_consumer_roles(
    label: &str,
    role_ids: &[String],
    team_of: &dyn Fn(&str) -> Option<String>,
) -> Vec<String> {
    if role_ids.iter().any(|role_id| role_id == label) {
        return vec![label.to_string()];
    }
    let team_hint = label.strip_suffix("_team").unwrap_or(label);
    let by_team = role_ids
        .iter()
        .filter(|role_id| team_of(role_id).as_deref() == Some(team_hint))
        .cloned()
        .collect::<Vec<_>>();
    if !by_team.is_empty() {
        return by_team;
    }
    let by_substring = role_ids
        .iter()
        .filter(|role_id| role_id.contains(label))
        .cloned()
        .collect::<Vec<_>>();
    if !by_substring.is_empty() {
        return by_substring;
    }
    role_ids
        .iter()
        .filter(|role_id| {
            team_of(role_id)
                .map(|team| team.contains(label))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn normalize_dependencies(
    value: &mut serde_json::Value,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let Some(dependencies) = value.get("dependencies").cloned() else {
        return Ok(());
    };
    let roles = value
        .get("roles")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let role_ids = roles
        .iter()
        .filter_map(|role| {
            role.get("role_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let team_of = |role_id: &str| -> Option<String> {
        roles
            .iter()
            .find(|role| {
                role.get("role_id").and_then(serde_json::Value::as_str) == Some(role_id)
            })
            .and_then(|role| {
                role.get("team")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
    };
    let mut pair_edges = Vec::new();
    let mut groups = std::collections::BTreeMap::new();
    let members_to_vec = |members: serde_json::Value, label: &str| -> Result<Vec<String>, String> {
        match members {
            serde_json::Value::String(raw) => Ok(vec![raw]),
            serde_json::Value::Array(items) => items
                .into_iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        format!("dependency group `{label}` members must be strings")
                    })
                })
                .collect::<Result<Vec<_>, _>>(),
            other => Err(format!(
                "dependency group `{label}` must map to a string or array of strings, got {}",
                json_type_name(&other)
            )),
        }
    };
    match dependencies {
        serde_json::Value::Array(items) => {
            for item in items {
                match item {
                    serde_json::Value::Object(mut map) => {
                        let from = map.remove("from").or_else(|| map.remove("source"));
                        let to = map.remove("to").or_else(|| map.remove("target"));
                        match (from, to) {
                            (
                                Some(serde_json::Value::String(from)),
                                Some(serde_json::Value::String(to)),
                            ) => pair_edges.push((from, to)),
                            (Some(from), Some(to)) => {
                                return Err(format!(
                                    "dependency from/to must be strings, got {}/{}",
                                    json_type_name(&from),
                                    json_type_name(&to)
                                ))
                            }
                            (None, None) => {
                                for (label, members) in map {
                                    groups.insert(
                                        label.clone(),
                                        members_to_vec(members, &label)?,
                                    );
                                }
                            }
                            _ => {
                                return Err(
                                    "dependency object requires from and to (or source and target), or a single group key"
                                        .to_string(),
                                )
                            }
                        }
                    }
                    serde_json::Value::Array(pair) if pair.len() == 2 => {
                        let mut iter = pair.into_iter();
                        let from = iter.next().unwrap_or_default();
                        let to = iter.next().unwrap_or_default();
                        let from = from.as_str().ok_or_else(|| {
                            "dependency pair first element must be a string".to_string()
                        })?;
                        let to = to.as_str().ok_or_else(|| {
                            "dependency pair second element must be a string".to_string()
                        })?;
                        pair_edges.push((from.to_string(), to.to_string()));
                    }
                    serde_json::Value::String(raw) => {
                        let (from, to) = raw
                            .split_once("->")
                            .or_else(|| raw.split_once(':'))
                            .ok_or_else(|| {
                                format!(
                                    "dependency string `{raw}` must use `from->to` or `from:to`"
                                )
                            })?;
                        pair_edges.push((from.trim().to_string(), to.trim().to_string()));
                    }
                    other => {
                        return Err(format!(
                            "dependency items must be objects, pairs, or strings, got {}",
                            json_type_name(&other)
                        ))
                    }
                }
            }
        }
        serde_json::Value::Object(map) => {
            for (label, members) in map {
                groups.insert(label.clone(), members_to_vec(members, &label)?);
            }
        }
        other => {
            return Err(format!(
                "dependencies must be an array or object, got {}",
                json_type_name(&other)
            ))
        }
    }
    let mut group_edges = Vec::new();
    for (label, members) in &groups {
        let is_role_label = role_ids.iter().any(|role_id| role_id == label);
        let all_members_are_roles = members
            .iter()
            .all(|member| role_ids.iter().any(|role_id| role_id == member));
        if is_role_label && all_members_are_roles {
            for member in members {
                group_edges.push((label.clone(), member.clone()));
            }
        } else if !all_members_are_roles {
            let consumers = resolve_consumer_roles(label, &role_ids, &team_of);
            if consumers.is_empty() {
                return Err(format!(
                    "dependency group `{label}` does not resolve to any role_id or team"
                ));
            }
            let mut sources = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            for member in members {
                sources.extend(resolve_member_roles(
                    member, &role_ids, &groups, &mut seen,
                )?);
            }
            for source in sources {
                for consumer in &consumers {
                    if &source != consumer {
                        group_edges.push((source.clone(), consumer.clone()));
                    }
                }
            }
        }
    }
    if !group_edges.is_empty() {
        notes.push("normalized object/group-shaped dependencies into role-level edges".to_string());
    }
    let mut edges = pair_edges;
    edges.extend(group_edges);
    let mut canonical = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (from, to) in edges {
        if !role_ids.iter().any(|role_id| role_id == &from) {
            return Err(format!("dependency source `{from}` is not a role_id"));
        }
        if !role_ids.iter().any(|role_id| role_id == &to) {
            return Err(format!("dependency target `{to}` is not a role_id"));
        }
        if from != to && seen.insert((from.clone(), to.clone())) {
            canonical.push(ProposedDependency { from, to });
        }
    }
    value["dependencies"] = serde_json::json!(canonical);
    Ok(())
}

fn capability_from_name(name: &str) -> Option<AgentCapability> {
    match name.to_ascii_lowercase().as_str() {
        "read" => Some(AgentCapability::Read),
        "search" => Some(AgentCapability::Search),
        "write" => Some(AgentCapability::Write),
        "test" => Some(AgentCapability::Test),
        "network" => Some(AgentCapability::Network),
        _ => None,
    }
}

fn ceiling_allows(ceiling: PermissionMode, capability: AgentCapability) -> bool {
    match capability {
        AgentCapability::Read | AgentCapability::Search => true,
        AgentCapability::Write | AgentCapability::Test => {
            ceiling.permits(PermissionMode::WorkspaceWrite)
                || ceiling.permits(PermissionMode::DangerFullAccess)
        }
        AgentCapability::Network => ceiling.permits(PermissionMode::DangerFullAccess),
        _ => false,
    }
}

fn cardinality(role: &ProposedRole) -> Result<RoleCardinalityPolicy, String> {
    if let Some(count) = role.fixed_count {
        if count == 0 {
            return Err(format!("role `{}` fixed_count must be > 0", role.role_id));
        }
        let count = u16::try_from(count)
            .map_err(|_| format!("role `{}` fixed_count exceeds u16", role.role_id))?;
        return Ok(RoleCardinalityPolicy::Fixed { count });
    }
    let min = u16::try_from(role.min_count.unwrap_or(1))
        .map_err(|_| format!("role `{}` min_count exceeds u16", role.role_id))?;
    let max = u16::try_from(role.max_count.unwrap_or(1))
        .map_err(|_| format!("role `{}` max_count exceeds u16", role.role_id))?
        .max(min);
    if min == 0 || max == 0 {
        return Err(format!(
            "role `{}` cardinality must be positive",
            role.role_id
        ));
    }
    if min == max {
        return Ok(RoleCardinalityPolicy::Fixed { count: min });
    }
    Ok(RoleCardinalityPolicy::Adaptive {
        min,
        target: min.max(1),
        max,
    })
}

fn parse_agent_ref(value: &str) -> Result<(AgentDefinitionId, u64), String> {
    let (path, revision) = match value.split_once('@') {
        Some((path, revision)) => (
            path,
            revision
                .parse::<u64>()
                .map_err(|_| format!("agent_definition_ref `{value}` has an invalid revision"))?,
        ),
        None => (value, 1),
    };
    let (scope, local_id) = match path.split_once('/') {
        Some(("builtin", local)) => (DefinitionScope::Builtin, local),
        Some(("user", local)) => (DefinitionScope::User, local),
        Some(("workspace", local)) => (DefinitionScope::Workspace, local),
        _ => {
            return Err(format!(
                "agent_definition_ref `{value}` must be builtin/<id> or user/<id>"
            ))
        }
    };
    AgentDefinitionId::new(scope, local_id)
        .map(|definition_id| (definition_id, revision))
        .map_err(|error| format!("invalid agent_definition_ref `{value}`: {error}"))
}

/// Team instructions are stored as normalized TEAM.md text: CRLF/CR are
/// folded to LF and a trailing newline is guaranteed. The manifest digest
/// must be computed over that same normalized text, otherwise
/// `store_revision` rejects AI-authored proposals containing `\r\n` with an
/// `instructions_digest` mismatch.
pub(crate) fn normalized_team_instructions(instructions: &str) -> String {
    let normalized = instructions.replace("\r\n", "\n").replace('\r', "\n");
    if normalized.ends_with('\n') {
        normalized
    } else {
        format!("{normalized}\n")
    }
}

pub struct TemplateCandidateCompiler;

impl TemplateCandidateCompiler {
    pub fn compile(
        registry: &RuntimeDefinitionRegistry,
        proposal: &TeamTemplateProposal,
        ceiling: PermissionMode,
    ) -> Result<TemplateCandidate, String> {
        // AI proposals may carry the full id including a scope prefix
        // (`workspace/biz-tech-...`) or the model-facing `cowd/...` alias.
        // Publish is always Workspace-scoped, so normalize the local id and
        // never produce a doubled `workspace/workspace/...` path.
        let mut local_id = proposal.template_id.trim();
        for prefix in ["cowd/", "workspace/", "user/", "builtin/"] {
            while let Some(stripped) = local_id.strip_prefix(prefix) {
                local_id = stripped;
            }
        }
        let template_id = TeamTemplateDefinitionId::new(DefinitionScope::Workspace, local_id)
            .map_err(|error| format!("invalid template_id: {error}"))?;
        let mut role_ids = std::collections::BTreeSet::new();
        let mut roles = Vec::with_capacity(proposal.roles.len());
        let mut clipped_capabilities = Vec::new();
        let mut defaulted_agent_refs = Vec::new();
        for role in &proposal.roles {
            if role.role_id.trim().is_empty() {
                return Err("every proposed role needs a non-empty role_id".to_string());
            }
            if !role_ids.insert(role.role_id.as_str()) {
                return Err(format!("duplicate role_id `{}`", role.role_id));
            }
            let (mut definition_id, mut revision) = parse_agent_ref(&role.agent_definition_ref)?;
            // The Definition must exist in the registry; AI cannot invent one.
            if registry
                .resolve_agent(
                    &definition_id,
                    RevisionSelector::ExactApprovedRevision { revision },
                )
                .is_err()
            {
                // Bounded auto-repair for a nonexistent/invented Agent
                // Definition: bind the safe builtin matching the requested
                // capability profile (same rule as a missing ref) and record
                // the exact substitution in the audit preview. Grant ceilings
                // are still clipped to the caller's permission ceiling below.
                let default_ref = if role
                    .grant_ceiling
                    .iter()
                    .any(|name| matches!(name.as_str(), "write" | "test"))
                {
                    "builtin/cowd/execute@1"
                } else if role.grant_ceiling.iter().any(|name| name == "network") {
                    "builtin/cowd/explore@2"
                } else {
                    "builtin/cowd/explore@1"
                };
                let (default_id, default_revision) = parse_agent_ref(default_ref)?;
                registry
                    .resolve_agent(
                        &default_id,
                        RevisionSelector::ExactApprovedRevision {
                            revision: default_revision,
                        },
                    )
                    .map_err(|error| {
                        format!(
                            "role `{}` references unknown Agent Definition `{}` and the safe builtin fallback `{default_ref}` is unavailable: {error}",
                            role.role_id, role.agent_definition_ref
                        )
                    })?;
                defaulted_agent_refs.push(format!(
                    "{}: {} -> {default_ref}",
                    role.role_id, role.agent_definition_ref
                ));
                definition_id = default_id;
                revision = default_revision;
            }
            let mut grant_ceiling = Vec::new();
            for name in &role.grant_ceiling {
                let capability = capability_from_name(name).ok_or_else(|| {
                    format!(
                        "role `{}` uses unknown capability `{name}` (read|search|write|test|network)",
                        role.role_id
                    )
                })?;
                if !ceiling_allows(ceiling, capability) {
                    // Bounded auto-repair: clip the over-ceiling capability and
                    // record it in the preview so the audit trail shows the
                    // exact compensation applied.
                    clipped_capabilities.push(format!("{}:{}", role.role_id, name));
                    continue;
                }
                grant_ceiling.push(capability);
            }
            if grant_ceiling.is_empty() {
                grant_ceiling.push(AgentCapability::Read);
            }
            grant_ceiling.sort_by_key(|capability| format!("{capability:?}"));
            grant_ceiling.dedup();
            let cardinality = cardinality(role)?;
            let partition = if cardinality.max() == 1 {
                RolePartitionPolicy::Single
            } else {
                RolePartitionPolicy::ByFocus {
                    partition_key: role.role_id.clone(),
                }
            };
            roles.push(TeamRoleDefinition {
                role_id: role.role_id.clone(),
                display_name: role.display_name.clone(),
                responsibility: role.responsibility.clone(),
                agent_definition_id: definition_id,
                agent_selector: RevisionSelector::ExactApprovedRevision { revision },
                cardinality,
                partition,
                grant_ceiling,
                task_contract: TeamRoleTaskContract {
                    contract_ref: format!("ai/team-role/{}@1", role.role_id),
                    acceptance: if role.acceptance.is_empty() {
                        vec!["evidence".to_string()]
                    } else {
                        role.acceptance.clone()
                    },
                },
            });
        }
        let dependencies = proposal
            .dependencies
            .iter()
            .map(|dependency| TeamRoleDependency {
                from_role_id: dependency.from.clone(),
                to_role_id: dependency.to.clone(),
            })
            .collect::<Vec<_>>();
        let result_fields = if proposal.result_fields.is_empty() {
            vec!["summary".to_string(), "evidence".to_string()]
        } else {
            proposal.result_fields.clone()
        };
        let manifest = TeamTemplateManifest {
            api_version: "cowd.team/v1".to_string(),
            template_id,
            revision: 1,
            name: proposal.name.clone(),
            display: Some(TeamTemplateDisplay {
                team_display_name: proposal.team_display_name.clone(),
                role_display_names: proposal.role_display_names.clone(),
            }),
            lifecycle: RevisionLifecycle::Draft,
            topology: TeamTopologyContract {
                protocol_ref: AI_TEMPLATE_PROTOCOL.to_string(),
                require_synthesis: true,
                require_review: dependencies.iter().any(|dependency| {
                    dependency.to_role_id.contains("review")
                        || dependency.to_role_id.contains("critic")
                }),
            },
            roles,
            dependencies,
            result_contract: TeamResultContract {
                required_fields: result_fields.clone(),
                evidence_required: proposal.evidence_required
                    || result_fields.contains(&"evidence".to_string()),
                synthesis_required: true,
            },
            evaluation: TeamEvaluationContract::single_release_gate(
                format!(
                    "ai/{}@1",
                    proposal
                        .template_id
                        .trim()
                        .strip_prefix("cowd/")
                        .unwrap_or(proposal.template_id.trim())
                ),
                "team_interoperability",
            ),
            instructions_digest: format!(
                "{:x}",
                Sha256::digest(normalized_team_instructions(&proposal.instructions).as_bytes())
            ),
        };
        manifest
            .validate()
            .map_err(|error| format!("proposed template is invalid: {error}"))?;
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_string(&manifest).map_err(|error| error.to_string())?)
        );
        let preview = json!({
            "template_id": manifest.template_id.as_str(),
            "revision": manifest.revision,
            "name": manifest.name,
            "team_display_name": manifest.display.as_ref().and_then(|display| display.team_display_name.clone()),
            "digest": digest,
            "roles": manifest.roles.iter().map(|role| json!({
                "role_id": role.role_id,
                "display_name": role.display_name,
                "responsibility": role.responsibility,
                "grant_ceiling": role.grant_ceiling.iter().map(|capability| format!("{capability:?}").to_ascii_lowercase()).collect::<Vec<_>>(),
                "cardinality": format!("{:?}", role.cardinality),
                "acceptance": role.task_contract.acceptance,
            })).collect::<Vec<_>>(),
            "dependencies": manifest.dependencies.iter().map(|dependency| json!({
                "from": dependency.from_role_id,
                "to": dependency.to_role_id,
            })).collect::<Vec<_>>(),
            "result_fields": manifest.result_contract.required_fields,
            "clipped_capabilities": clipped_capabilities,
            "defaulted_agent_refs": defaulted_agent_refs,
            "risk_notes": {
                "requires_write": manifest.roles.iter().any(|role| role.grant_ceiling.contains(&AgentCapability::Write)),
                "requires_network": manifest.roles.iter().any(|role| role.grant_ceiling.contains(&AgentCapability::Network)),
            },
        });
        Ok(TemplateCandidate {
            manifest,
            digest,
            preview,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RuntimeDefinitionRegistry, RuntimeEventInput, RuntimeEventScope, RuntimeServices,
        SubmitGlobalApprovalRequest,
    };
    use harness_contract::agent::AgentExecutorPolicy;
    use harness_contract::agent::{
        AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionManifest,
        AgentEvaluationContract, AgentModelPolicy, AgentOutputContract, CognitiveReadScope,
        CognitiveWriteMode, ReleaseAssignment, ReleaseAssignmentStatus, ReleaseAuthorization,
        ReleaseChannel,
    };
    use harness_contract::core::TaskRisk;
    use harness_contract::policy::{
        ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand,
        ApprovalDomain, ApprovalGrantScope, ApprovalSource, ApprovalSourceKind,
        ApprovalTimeoutPolicy,
    };

    fn digest(value: &str) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    fn registry() -> (tempfile::TempDir, RuntimeDefinitionRegistry) {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let storage =
            storage::StorageLayout::default_for_config_home(temporary.path().join("user"));
        let registry = RuntimeDefinitionRegistry::from_storage_layout(
            &storage,
            temporary.path().join("bundle/definitions"),
            temporary.path().join("workspace"),
        )
        .expect("registry");
        (temporary, registry)
    }

    fn publish_agent(registry: &RuntimeDefinitionRegistry, local_id: &str) {
        let instructions = format!("# {local_id}\n\nBounded agent.\n");
        let definition_id =
            AgentDefinitionId::new(DefinitionScope::Workspace, local_id).expect("definition id");
        let stored = registry
            .agents()
            .store_revision(
                AgentDefinitionManifest {
                    api_version: "cowd.agent/v1".to_string(),
                    definition_id: definition_id.clone(),
                    revision: 1,
                    name: local_id.to_string(),
                    description: "Bounded agent".to_string(),
                    lifecycle: RevisionLifecycle::Published,
                    executor: AgentExecutorPolicy::CowdNative,
                    model_policy: AgentModelPolicy {
                        profile: "coding".to_string(),
                        allowed_models: vec!["test-model".to_string()],
                        fallback_allowed: true,
                    },
                    cognitive_policy: AgentCognitivePolicy {
                        context_profile: "team".to_string(),
                        read_scopes: vec![CognitiveReadScope::Session],
                        write_mode: CognitiveWriteMode::CandidateOnly,
                        team_working_state_visible: true,
                    },
                    capability_contract: AgentCapabilityContract {
                        capability_ceiling: vec![AgentCapability::Read],
                        skill_refs: vec![],
                        approval_required_for: vec![],
                    },
                    output_contract: AgentOutputContract::reviewable(),
                    evaluation: AgentEvaluationContract::single_release_gate(local_id, "evidence"),
                    instructions_digest: digest(&instructions),
                },
                &instructions,
            )
            .expect("stored agent");
        registry
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: format!("approval/{local_id}-v1"),
                },
                content_digest: stored.revision.content_digest,
            })
            .expect("agent release");
    }

    fn business_tech_proposal() -> TeamTemplateProposal {
        TeamTemplateProposal {
            template_id: "cowd/business-tech-deliberation".to_string(),
            name: "业务/技术双团队研讨".to_string(),
            team_display_name: Some("业务技术研讨".to_string()),
            role_display_names: vec![
                RoleDisplayName {
                    role_id: "business_expert".to_string(),
                    display_name: "供应链专家".to_string(),
                },
                RoleDisplayName {
                    role_id: "cto".to_string(),
                    display_name: "CTO".to_string(),
                },
            ],
            roles: vec![
                ProposedRole {
                    role_id: "business_expert".to_string(),
                    display_name: Some("供应链专家".to_string()),
                    responsibility: "分析供应制造与订单履行约束".to_string(),
                    agent_definition_ref: "workspace/cowd/explore@1".to_string(),
                    grant_ceiling: vec!["read".to_string(), "search".to_string()],
                    fixed_count: Some(2),
                    min_count: None,
                    max_count: None,
                    acceptance: vec!["findings".to_string(), "evidence".to_string()],
                },
                ProposedRole {
                    role_id: "cto".to_string(),
                    display_name: Some("CTO".to_string()),
                    responsibility: "裁定技术方案并汇总".to_string(),
                    agent_definition_ref: "workspace/cowd/direct@1".to_string(),
                    grant_ceiling: vec!["read".to_string()],
                    fixed_count: Some(1),
                    min_count: None,
                    max_count: None,
                    acceptance: vec!["summary".to_string(), "evidence".to_string()],
                },
            ],
            dependencies: vec![ProposedDependency {
                from: "business_expert".to_string(),
                to: "cto".to_string(),
            }],
            result_fields: vec!["summary".to_string(), "evidence".to_string()],
            evidence_required: true,
            instructions: "# 民主集中式研讨\n\n业务专家先产出证据，CTO 汇总并裁决。\n".to_string(),
        }
    }

    #[test]
    fn compiles_and_clips_a_business_tech_template() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &business_tech_proposal(),
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        assert_eq!(candidate.manifest.roles.len(), 2);
        assert_eq!(
            candidate
                .manifest
                .display
                .as_ref()
                .unwrap()
                .team_display_name
                .as_deref(),
            Some("业务技术研讨")
        );
        assert_eq!(
            candidate.manifest.roles[0].display_name.as_deref(),
            Some("供应链专家")
        );
        assert!(candidate
            .manifest
            .roles
            .iter()
            .all(|role| !role.grant_ceiling.contains(&AgentCapability::Write)));
        assert_eq!(candidate.preview["digest"], candidate.digest);
        assert!(candidate.manifest.validate().is_ok());
    }

    #[test]
    fn clips_over_ceiling_grants_and_defaults_unknown_definitions() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let mut proposal = business_tech_proposal();
        proposal.roles[0].agent_definition_ref =
            "workspace/cowd/not-a-real-definition@1".to_string();
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &proposal,
            PermissionMode::ReadOnly,
        )
        .expect("unknown definition falls back to a safe builtin");
        assert_eq!(
            candidate.manifest.roles[0]
                .agent_definition_id
                .as_str(),
            "builtin/cowd/explore"
        );
        assert!(candidate
            .preview
            .get("defaulted_agent_refs")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|defaulted| defaulted.len() == 1));
        proposal = business_tech_proposal();
        proposal.roles[0].grant_ceiling = vec!["write".to_string()];
        let candidate =
            TemplateCandidateCompiler::compile(&registry, &proposal, PermissionMode::ReadOnly)
                .expect("over-ceiling grant is clipped, not rejected");
        assert!(candidate
            .manifest
            .roles
            .iter()
            .all(|role| !role.grant_ceiling.contains(&AgentCapability::Write)));
        assert!(candidate
            .preview
            .get("clipped_capabilities")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|clipped| clipped.len() == 1));
    }

    #[test]
    fn publishes_to_the_user_template_catalog() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &business_tech_proposal(),
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        let stored = registry
            .teams()
            .store_revision(candidate.manifest, &business_tech_proposal().instructions)
            .expect("publish");
        let reloaded = registry
            .teams()
            .read_revision(&stored.revision.revision_ref)
            .expect("reload");
        assert_eq!(reloaded.revision.manifest.name, "业务/技术双团队研讨");
        assert_eq!(
            reloaded
                .revision
                .manifest
                .display
                .as_ref()
                .unwrap()
                .team_display_name
                .as_deref(),
            Some("业务技术研讨")
        );
    }

    #[test]
    fn approval_gated_publish_roundtrip() {
        let (_temp, registry) = registry();
        let services = RuntimeServices::in_memory().expect("services");
        // The publish target registry only carries builtin Agents, so the
        // proposal must reference builtin definitions for the runnable
        // catalog to resolve its role bindings.
        let mut proposal = business_tech_proposal();
        proposal.roles[0].agent_definition_ref = "builtin/cowd/explore@1".to_string();
        proposal.roles[1].agent_definition_ref = "builtin/cowd/direct@1".to_string();
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &proposal,
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        let approval_id = "template-approval:test-roundtrip";
        services
            .event_store()
            .append(RuntimeEventInput {
                stream_id: format!("definition-template-candidate:{approval_id}"),
                scope: RuntimeEventScope::Mission,
                kind: "definition.template.candidate.v1".to_string(),
                status: Some("pending_approval".to_string()),
                actor: None,
                refs: vec![],
                payload: serde_json::json!({
                    "approval_id": approval_id,
                    "manifest": candidate.manifest,
                    "instructions": crate::team_template_candidate::normalized_team_instructions(
                        &proposal.instructions,
                    ),
                    "digest": candidate.digest,
                    "preview": candidate.preview,
                }),
            })
            .expect("candidate event");
        assert!(services
            .publish_approved_template_candidate(approval_id)
            .is_err());
        let context = ApprovalContext {
            principal_id: "session:s".to_string(),
            profile_id: "template-publish".to_string(),
            approval_profile: None,
            workspace_key: "w".to_string(),
            session_id: Some("s".to_string()),
            turn_id: None,
            task_id: None,
            capability: "definition.template.publish".to_string(),
            invocation_id: None,
            execution_id: None,
            strategy_decision_ref: None,
            source_surface: None,
            resource_targets: vec![],
            effect: None,
            explicit_ask: true,
            effective_sandbox_posture: None,
            policy_revision: 0,
            requested_sandbox_posture: None,
        };
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some("s".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        services
            .approval_queue()
            .submit_scoped(
                approval_id,
                SubmitGlobalApprovalRequest {
                    source,
                    context,
                    action: "definition.template.publish".to_string(),
                    summary: "publish test template".to_string(),
                    risk: TaskRisk::Low,
                    domain: ApprovalDomain::System,
                    blocks_execution: false,
                    evidence_refs: vec![],
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )
            .expect("submit");
        services
            .approval_queue()
            .decide_internal(ApprovalDecisionCommand {
                approval_id: approval_id.to_string(),
                approved: true,
                skip: false,
                reason: "test".to_string(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::Policy,
                    actor_id: "test".to_string(),
                },
                evidence_refs: vec![],
            })
            .expect("decide");
        let published = services
            .publish_approved_template_candidate(approval_id)
            .expect("publish after approval");
        assert!(published.get("content_digest").is_some());
        let stored = services
            .definition_registry()
            .teams()
            .read_revision(&candidate.manifest.revision_ref())
            .expect("reload published template");
        assert_eq!(
            stored
                .revision
                .manifest
                .display
                .as_ref()
                .unwrap()
                .team_display_name
                .as_deref(),
            Some("业务技术研讨")
        );
        let catalog = services
            .definition_registry()
            .runnable_team_catalog()
            .expect("runnable team catalog");
        assert!(
            catalog
                .iter()
                .any(|entry| entry.name == "业务/技术双团队研讨"),
            "published template must be runnable: {:?}",
            catalog
                .iter()
                .map(|entry| entry.revision_ref.template_id.as_str().to_string())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn normalize_template_proposal_accepts_map_shaped_roles() {
        let mut value = serde_json::json!({
            "template_id": "cowd/test",
            "name": "测试",
            "roles": {
                "business_expert": {
                    "responsibility": "业务分析",
                    "agent_definition_ref": {
                        "definition": "workspace/cowd/explore",
                        "revision": 1
                    },
                    "grant_ceiling": {"read": true, "write": false},
                    "acceptance": "findings"
                },
                "cto": {
                    "responsibility": "技术裁决",
                    "agent_definition_ref": {"workspace/cowd/direct": 1}
                }
            },
            "role_display_names": {
                "business_expert": "供应链专家",
                "cto": "CTO"
            },
            "instructions": "# 测试\n"
        });
        let notes = normalize_template_proposal(&mut value).expect("normalized");
        assert!(!notes.is_empty(), "defaulted display names should be recorded");
        let roles = value["roles"].as_array().expect("roles array");
        assert_eq!(roles.len(), 2);
        assert!(roles.iter().any(|role| {
            role["role_id"] == "business_expert"
                && role["responsibility"] == "业务分析"
                && role["agent_definition_ref"] == "workspace/cowd/explore@1"
                && role["grant_ceiling"] == serde_json::json!(["read"])
                && role["acceptance"] == serde_json::json!(["findings"])
        }));
        assert!(roles.iter().any(|role| role["role_id"] == "cto"
            && role["agent_definition_ref"] == "workspace/cowd/direct@1"));
        let displays = value["role_display_names"]
            .as_array()
            .expect("display names array");
        assert!(displays.iter().any(|item| {
            item["role_id"] == "business_expert" && item["display_name"] == "供应链专家"
        }));
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let proposal: TeamTemplateProposal =
            serde_json::from_value(value).expect("normalized proposal parses");
        assert_eq!(proposal.roles.len(), 2);
    }

    #[test]
    fn normalize_template_proposal_accepts_wrapped_json_string_and_array() {
        let payload = serde_json::json!({
            "template_id": "cowd/biz-tech-dual-team-deliberation",
            "name": "业务/技术双团队民主集中研讨",
            "team_display_name": "业务-技术双团队研讨组（民主集中制）",
            "roles": [
                {
                    "role_id": "biz_manufacturing_expert",
                    "display_name": "制造领域业务专家(1)",
                    "team": "business",
                    "cardinality": 1,
                    "grant_ceiling": "workspace-read",
                    "responsibility": "从制造现实出发评估约束",
                    "acceptance": "交付制造约束清单"
                },
                {
                    "role_id": "cto_supply_manufacturing",
                    "display_name": "高级供应制造领域CTO",
                    "team": "technical",
                    "cardinality": 1,
                    "grant_ceiling": "workspace-read-write",
                    "responsibility": "总体架构决策"
                },
                {
                    "role_id": "convergence_arbiter",
                    "display_name": "集中收敛主持人/仲裁",
                    "team": "convergence",
                    "cardinality": 1,
                    "grant_ceiling": "workspace-read-write",
                    "responsibility": "主持收敛"
                }
            ],
            "dependencies": {
                "business_team": ["biz_manufacturing_expert"],
                "technical_team": ["cto_supply_manufacturing"],
                "convergence": ["business_team", "technical_team"]
            },
            "result_fields": ["summary", "evidence"],
            "instructions": "# 研讨\n"
        });
        let encoded = serde_json::to_string(&payload).expect("encode");
        let mut value = serde_json::json!([encoded]);
        let notes =
            normalize_template_proposal(&mut value).expect("normalize array-wrapped string");
        assert!(
            notes
                .iter()
                .any(|note| note.contains("single-element template_proposal array"))
        );
        assert!(value.is_object());
        let roles = value["roles"].as_array().expect("roles array");
        assert_eq!(roles.len(), 3);
        assert_eq!(roles[0]["grant_ceiling"], serde_json::json!(["read"]));
        assert_eq!(
            roles[1]["grant_ceiling"],
            serde_json::json!(["read", "write"])
        );
        assert_eq!(roles[1]["fixed_count"], serde_json::json!(1));
        assert_eq!(
            roles[0]["agent_definition_ref"],
            serde_json::json!("builtin/cowd/explore@1")
        );
        assert_eq!(
            roles[1]["agent_definition_ref"],
            serde_json::json!("builtin/cowd/execute@1")
        );
        let dependencies = value["dependencies"].as_array().expect("dependencies array");
        assert_eq!(dependencies.len(), 2);
        assert!(dependencies.iter().any(|dep| {
            dep["from"] == "biz_manufacturing_expert" && dep["to"] == "convergence_arbiter"
        }));
        assert!(dependencies.iter().any(|dep| {
            dep["from"] == "cto_supply_manufacturing" && dep["to"] == "convergence_arbiter"
        }));
        let mut value = serde_json::json!(encoded);
        normalize_template_proposal(&mut value).expect("normalize raw JSON string");
        assert!(value.is_object());
    }

    #[test]
    fn normalize_template_proposal_rejects_non_object_without_panicking() {
        let mut value = serde_json::json!(["a", "b"]);
        let error =
            normalize_template_proposal(&mut value).expect_err("two-element array must error");
        assert!(error.contains("exactly one template object"));
        let mut value = serde_json::json!(42);
        let error = normalize_template_proposal(&mut value).expect_err("number must error");
        assert!(error.contains("must be a JSON object"));
    }

    #[test]
    fn normalize_template_proposal_rejects_unknown_grant_ceiling() {
        let mut value = serde_json::json!({
            "template_id": "cowd/test",
            "name": "测试",
            "roles": [{
                "role_id": "r",
                "responsibility": "x",
                "grant_ceiling": "full-access"
            }]
        });
        let error =
            normalize_template_proposal(&mut value).expect_err("unknown ceiling must error");
        assert!(error.contains("grant_ceiling"));
    }

    #[test]
    fn normalize_dependencies_supports_role_keyed_upstream_edges() {
        let mut value = serde_json::json!({
            "template_id": "cowd/test",
            "name": "测试",
            "roles": [
                {"role_id": "implementer", "responsibility": "x"},
                {"role_id": "reviewer", "responsibility": "y"}
            ],
            "dependencies": {"implementer": ["reviewer"]}
        });
        normalize_template_proposal(&mut value).expect("normalize");
        let dependencies = value["dependencies"].as_array().expect("dependencies array");
        assert_eq!(dependencies.len(), 1);
        assert_eq!(dependencies[0]["from"], "implementer");
        assert_eq!(dependencies[0]["to"], "reviewer");
    }

    #[test]
    fn compiles_normalized_model_payload_with_defaulted_agent_refs() {
        let (_temp, registry) = registry();
        let mut value = serde_json::json!({
            "template_id": "cowd/biz-tech-dual-team-deliberation",
            "name": "业务/技术双团队民主集中研讨",
            "team_display_name": "业务-技术双团队研讨组",
            "roles": [
                {
                    "role_id": "biz_expert",
                    "display_name": "供应链专家",
                    "grant_ceiling": "workspace-read",
                    "responsibility": "分析供应制造约束",
                    "acceptance": "findings"
                },
                {
                    "role_id": "cto",
                    "display_name": "CTO",
                    "grant_ceiling": "workspace-read-write",
                    "responsibility": "技术裁决"
                },
                {
                    "role_id": "convergence_arbiter",
                    "display_name": "集中收敛主持人",
                    "team": "convergence",
                    "grant_ceiling": "workspace-read-write",
                    "responsibility": "汇总裁决"
                }
            ],
            "dependencies": {
                "business_team": ["biz_expert"],
                "technical_team": ["cto"],
                "convergence": ["business_team", "technical_team"]
            },
            "result_fields": ["summary", "evidence"],
            "instructions": "# 研讨\n"
        });
        normalize_template_proposal(&mut value).expect("normalize");
        let proposal: TeamTemplateProposal = serde_json::from_value(value).expect("parse");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &proposal,
            PermissionMode::DangerFullAccess,
        )
        .expect("compile with builtin defaults");
        assert_eq!(candidate.manifest.roles.len(), 3);
        assert!(candidate.manifest.dependencies.iter().any(|dependency| {
            dependency.from_role_id == "biz_expert"
                && dependency.to_role_id == "convergence_arbiter"
        }));
        assert!(candidate.manifest.dependencies.iter().any(|dependency| {
            dependency.from_role_id == "cto" && dependency.to_role_id == "convergence_arbiter"
        }));
        assert!(candidate
            .manifest
            .roles
            .iter()
            .any(|role| role.grant_ceiling.contains(&AgentCapability::Write)));
    }

    #[test]
    fn crlf_instructions_publish_with_a_matching_manifest_digest() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let mut proposal = business_tech_proposal();
        proposal.instructions = "第一行\r\n第二行\r\n".to_string();
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &proposal,
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        // store_revision normalizes CRLF before hashing; the manifest digest
        // must match that normalized text, not the raw proposal bytes.
        let stored = registry
            .teams()
            .store_revision(candidate.manifest, &proposal.instructions)
            .expect("CRLF instructions must publish");
        assert_eq!(stored.revision.revision_ref.revision, 1);
    }

    #[test]
    fn normalize_dependencies_accepts_group_objects_inside_arrays() {
        let mut value = serde_json::json!({
            "template_id": "cowd/test",
            "name": "测试",
            "roles": [
                {"role_id": "business_expert", "responsibility": "x"},
                {"role_id": "cto", "responsibility": "y"},
                {
                    "role_id": "convergence_arbiter",
                    "team": "convergence",
                    "responsibility": "z"
                }
            ],
            "dependencies": [
                {"business_team": ["business_expert"]},
                {"technical_team": ["cto"]},
                {"convergence": ["business_team", "technical_team"]}
            ]
        });
        normalize_template_proposal(&mut value).expect("normalize");
        let dependencies = value["dependencies"].as_array().expect("dependencies array");
        assert_eq!(dependencies.len(), 2);
        assert!(dependencies.iter().any(|dependency| {
            dependency["from"] == "business_expert"
                && dependency["to"] == "convergence_arbiter"
        }));
        assert!(dependencies.iter().any(|dependency| {
            dependency["from"] == "cto" && dependency["to"] == "convergence_arbiter"
        }));
    }

    #[test]
    fn unknown_agent_definition_refs_fall_back_to_safe_builtins_with_audit() {
        let (_temp, registry) = registry();
        let mut proposal = business_tech_proposal();
        proposal.roles[0].agent_definition_ref = "builtin/cowd/researcher@1".to_string();
        proposal.roles[1].agent_definition_ref = "builtin/cowd/cto@1".to_string();
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &proposal,
            PermissionMode::ReadOnly,
        )
        .expect("unknown agent refs fall back to safe builtins");
        let defaulted = candidate.preview["defaulted_agent_refs"]
            .as_array()
            .expect("defaulted agent refs audit");
        assert_eq!(defaulted.len(), 2);
        assert_eq!(
            candidate.manifest.roles[0]
                .agent_definition_id
                .as_str(),
            "builtin/cowd/explore"
        );
        assert_eq!(
            candidate.manifest.roles[1]
                .agent_definition_id
                .as_str(),
            "builtin/cowd/explore"
        );
    }

    #[test]
    fn workspace_prefixed_template_id_and_null_agent_ref_are_normalized() {
        let mut value = serde_json::json!({
            "template_id": "workspace/biz-tech-dual-team-deliberation",
            "name": "测试",
            "roles": [{
                "role_id": "cto",
                "responsibility": "x",
                "grant_ceiling": "workspace-read",
                "agent_definition_ref": null
            }]
        });
        normalize_template_proposal(&mut value).expect("normalize");
        assert_eq!(
            value["roles"][0]["agent_definition_ref"],
            "builtin/cowd/explore@1"
        );
        let (_temp, registry) = registry();
        let proposal: TeamTemplateProposal =
            serde_json::from_value(value).expect("parses after normalization");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &proposal,
            PermissionMode::ReadOnly,
        )
        .expect("compile with workspace-prefixed id and null agent ref");
        assert_eq!(
            candidate.manifest.template_id.as_str(),
            "workspace/biz-tech-dual-team-deliberation"
        );
    }
}
