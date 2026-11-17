//! Same-call grep windows reuse the original acquisition and nested delivery owners.
use super::{read_sha256, ReadBodySeal, ReadExtent};
use serde_json::{json, Value};

pub(super) fn acquisitions(
    request: &crate::RuntimeToolExecutionRequest,
    scope: &harness_contract::goal::ProofEffectScope,
    child: &tools::OrchestratedRead,
) -> Option<Vec<Value>> {
    let windows = child.grep_windows()?;
    let output: Value = serde_json::from_str(child.output()).ok()?;
    if output["mode"] != "content"
        || output["scanComplete"] != true
        || output["truncated"] != false
        || !output["omissions"].as_array()?.is_empty()
        || !output["next_request"].is_null()
        || !output["continuationCursor"].is_null()
    {
        return None;
    }
    let rows = output["content"].as_str()?.split('\n').collect::<Vec<_>>();
    let mut result = Vec::new();
    for (window_index, window) in windows.iter().enumerate() {
        let count = window.body().split('\n').count();
        if rows
            .get(window.output_row()..window.output_row().checked_add(count)?)?
            .join("\n")
            != window.presentation()
        {
            return None;
        }
        let body = ReadBodySeal {
            schema_version: 1,
            source_ref: window.path().into(),
            source_sha256: window.source_sha256().into(),
            source_bytes: window.source_bytes(),
            output_sha256: read_sha256(child.output().as_bytes()),
            output_bytes: child.output().len() as u64,
            extent: ReadExtent::FileLines {
                start_line: window.start_line(),
                line_count: count as u64,
                total_lines: window.total_lines(),
                ends_with_newline: window.ends_with_newline(),
                body_sha256: read_sha256(window.body().as_bytes()),
                body_bytes: window.body().len() as u64,
                source_omitted: window.start_line() != 1 || count as u64 != window.total_lines(),
            },
        };
        let record = json!({"schema_version":1,"parent_tool_name":request.tool_name,
            "parent_tool_use_id":request.tool_use_id,"parent_idempotency_key":request.idempotency_key,
            "parent_descriptor_hash":request.authorization.as_ref()?.descriptor_hash,
            "step_index":child.step_index(),"step_id":child.step_id(),"position":child.position(),
            "batch_index":child.grep_batch_index(),"window_index":window_index,
            "tool_name":"grep_search","input":child.input(),"input_sha256":read_sha256(&serde_json::to_vec(child.input()).ok()?),
            "effect":child.effect(),"body":body,"prefixes":[],
            "relative_path":window.relative_path(),"physical_identity":window.physical_identity(),
            "directory_ancestry":window.directory_ancestry(),
            "output_row":window.output_row(),"presentation":window.presentation()});
        let input: Value = serde_json::from_str(&request.input).ok()?;
        if !grep_original_input_matches(&input, &record) {
            return None;
        }
        // An unprovable file does not discard another independently admitted file.
        if grep_read_target(scope, &record).is_some()
            || grep_read_target_for_parent(scope, &record, &input, &request.compiled_invocation.as_ref()?.effect).is_some()
            || grep_read_target_for_composite(scope, &record, &input, &request.compiled_invocation.as_ref()?.effect).is_some() {
            result.push(record);
        }
    }
    Some(result)
}

// Resolve only original literal call locations. Dynamic/summary outputs remain unknown.
fn original_call<'a>(input: &'a Value, child: &Value) -> Option<&'a Value> {
    if child["parent_tool_name"] == "grep_search" {
        if child["step_index"] != 0 || child["position"] != 0 || child["step_id"] != "" {
            return None;
        }
        return Some(input);
    }
    if child["parent_tool_name"] != "tool_orchestrate" {
        return None;
    }
    let steps = input["steps"].as_array()?;
    let step = steps.get(child["step_index"].as_u64()? as usize)?;
    if step["id"] != child["step_id"] || steps.iter().filter(|s| s["id"] == step["id"]).count() != 1
    {
        return None;
    }
    let call = match step["op"].as_str()? {
        "call" if child["position"] == 0 => step,
        "batch" => step["calls"]
            .as_array()?
            .get(child["position"].as_u64()? as usize)?,
        _ => return None,
    };
    if call["tool"] != "grep_search" {
        return None;
    }
    call.get("input")
}
pub(crate) fn grep_original_input_matches(input: &Value, child: &Value) -> bool {
    let Some(call) = original_call(input, child) else {
        return false;
    };
    let actual = if let Some(index) = child["batch_index"].as_u64() {
        call["searches"]
            .as_array()
            .and_then(|items| items.get(index as usize))
    } else if call.get("searches").is_none() {
        Some(call)
    } else {
        None
    };
    actual == Some(&child["input"])
        && child["input_sha256"]
            == read_sha256(&serde_json::to_vec(&child["input"]).unwrap_or_default())
}

/// Prove the File portion of one original direct grep batch without changing
/// the completeness claim of the parent effect scope.
pub(crate) fn direct_grep_file_domain(
    input: &Value, parent: &harness_contract::tool::ToolEffectDescriptor,
    scope: &harness_contract::goal::ProofEffectScope, child: &Value,
) -> bool {
    if child["parent_tool_name"] != "grep_search" || child["step_index"] != 0
        || child["position"] != 0 || child["step_id"] != "" || scope.version != 2
        || scope.descriptor_hash != parent.descriptor_hash || scope.validate_purpose_mapping(parent).is_err()
        || !grep_original_input_matches(input, child) { return false; }
    let Some(index) = child["batch_index"].as_u64().and_then(|v| usize::try_from(v).ok()) else { return false; };
    let Ok(effect) = serde_json::from_value::<harness_contract::tool::ToolEffectDescriptor>(child["effect"].clone()) else { return false; };
    if !tools::host::original_grep_batch_member_scopes(input, parent, index, &child["input"], &effect) { return false; }
    original_read_file_domain(parent, scope)
}

/// Validate the original File/Read domain without widening the mixed scope.
pub(crate) fn original_read_file_domain(
    parent: &harness_contract::tool::ToolEffectDescriptor,
    scope: &harness_contract::goal::ProofEffectScope,
) -> bool {
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    if parent.effect_kind != harness_contract::tool::ToolEffectKind::Read
        || parent.uses_network || parent.spawns_process || parent.mutates_packages || parent.mutates_system
        || scope.version != 2 || scope.descriptor_hash != parent.descriptor_hash
        || scope.validate_purpose_mapping(parent).is_err()
        || parent.scopes.iter().any(|item| item.operation != PermissionOperation::Read
            || !matches!(item.resource, PermissionResource::File | PermissionResource::Tool)) {
        return false;
    }
    let files = parent.scopes.iter().filter(|s| s.resource == PermissionResource::File && s.operation == PermissionOperation::Read).collect::<Vec<_>>();
    !files.is_empty() && files.len() == scope.targets.len()
        && files.iter().all(|s| scope.targets.iter().filter(|target| &target.declared_scope == *s).count() == 1)
        && scope.targets.iter().all(|target| {
            use harness_contract::context::{EvidenceCoverageKind, WorkspaceAccessMode, WorkspaceObjectKind};
            files.contains(&&target.declared_scope)
                && target.access_mode == WorkspaceAccessMode::Read
                && target.coverage == EvidenceCoverageKind::ScopedContent
                && !target.path.workspace_id.is_empty() && !target.path.repository_id.is_empty()
                && !target.path.workspace_relative_path.is_empty()
                && match target.path.object_kind {
                    WorkspaceObjectKind::File => target.exact && target.alias_checked && target.physical_identity.as_deref().is_some_and(|id| !id.is_empty()),
                    WorkspaceObjectKind::Directory => !target.exact && !target.alias_checked && target.physical_identity.is_none(),
                }
        })
}

pub(crate) fn grep_read_target_for_parent(
    scope: &harness_contract::goal::ProofEffectScope, child: &Value, input: &Value,
    parent: &harness_contract::tool::ToolEffectDescriptor,
) -> Option<harness_contract::goal::ProofEffectTarget> {
    if !direct_grep_file_domain(input, parent, scope, child) { return None; }
    grep_read_target_checked(scope, child, true)
}

pub(crate) fn grep_read_target_for_composite(
    scope: &harness_contract::goal::ProofEffectScope, child: &Value, input: &Value,
    parent: &harness_contract::tool::ToolEffectDescriptor,
) -> Option<harness_contract::goal::ProofEffectTarget> {
    if parent.tool_id != "tool_orchestrate" || child["parent_tool_name"] != "tool_orchestrate"
        || !original_read_file_domain(parent, scope) || !grep_original_input_matches(input, child) {
        return None;
    }
    let effect: harness_contract::tool::ToolEffectDescriptor = serde_json::from_value(child["effect"].clone()).ok()?;
    if !effect.scopes.iter().all(|item| parent.scopes.contains(item)) { return None; }
    grep_read_target_checked(scope, child, true)
}

pub(crate) fn grep_read_target(
    scope: &harness_contract::goal::ProofEffectScope,
    child: &Value,
) -> Option<harness_contract::goal::ProofEffectTarget> {
    grep_read_target_checked(scope, child, false)
}

fn grep_read_target_checked(
    scope: &harness_contract::goal::ProofEffectScope,
    child: &Value, verified_file_domain: bool,
) -> Option<harness_contract::goal::ProofEffectTarget> {
    use harness_contract::{
        context::{EvidenceCoverageKind, WorkspaceAccessMode, WorkspaceObjectKind},
        policy::{PermissionOperation, PermissionResource},
    };
    let relative = child["relative_path"].as_str()?;
    let path = std::path::Path::new(relative);
    if relative.is_empty()
        || !path
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
        || (!scope.complete && !verified_file_domain)
        || child["physical_identity"]
            .as_str()?
            .strip_prefix("unix-file:")?
            .is_empty()
    {
        return None;
    }
    let effect: harness_contract::tool::ToolEffectDescriptor =
        serde_json::from_value(child["effect"].clone()).ok()?;
    if effect.tool_id != "grep_search"
        || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
        || effect.uses_network
        || effect.spawns_process
        || effect.mutates_packages
        || effect.mutates_system
    {
        return None;
    }
    let mut matches = scope.targets.iter().filter(|target| {
        effect.scopes.contains(&target.declared_scope)
            && target.declared_scope.resource == PermissionResource::File
            && target.declared_scope.operation == PermissionOperation::Read
            && target.access_mode == WorkspaceAccessMode::Read
            && target.coverage == EvidenceCoverageKind::ScopedContent
            && match target.path.object_kind {
                WorkspaceObjectKind::File => target.path.workspace_relative_path == relative
                    && target.exact && target.alias_checked
                    && target.physical_identity.as_deref() == child["physical_identity"].as_str(),
                // This is a private same-call openat2 ancestry witness from the
                // original authorized lease, not an admitted directory file id.
                WorkspaceObjectKind::Directory => {
                    let ancestry = &child["directory_ancestry"];
                    target.physical_identity.is_none() && !target.exact && !target.alias_checked
                        && ancestry.as_array().is_some_and(|parts| parts.len() == 3)
                        && ancestry[0].as_str() == Some(target.path.workspace_relative_path.as_str())
                        && [1, 2].into_iter().all(|index| ancestry[index].as_str()
                            .is_some_and(|identity| identity.strip_prefix("unix-directory:").is_some_and(|v| !v.is_empty())))
                        && (target.path.workspace_relative_path == "."
                            || path.starts_with(&target.path.workspace_relative_path))
                }
            }
    });
    let original = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    let mut target = original.clone();
    let suffix = if original.path.workspace_relative_path == "." {
        path
    } else {
        path.strip_prefix(&original.path.workspace_relative_path)
            .ok()?
    };
    let repository_path = if original.path.object_kind == WorkspaceObjectKind::File {
        original.path.repository_relative_path.clone()
    } else if original.path.repository_relative_path == "." {
        suffix.to_str()?.to_string()
    } else {
        std::path::Path::new(&original.path.repository_relative_path)
            .join(suffix)
            .to_str()?
            .to_string()
    };
    target.path.workspace_relative_path = relative.into();
    target.path.repository_relative_path = repository_path;
    target.path.object_kind = WorkspaceObjectKind::File;
    target.path.observed_revision_or_digest = Some(child["body"]["source_sha256"].as_str()?.into());
    target.physical_identity = Some(child["physical_identity"].as_str()?.into());
    target.exact = true;
    target.alias_checked = true;
    target.coverage = EvidenceCoverageKind::ScopedContent;
    Some(target)
}

pub(super) fn presented_extent(
    parent_input: &str,
    presentation: &str,
    child: &Value,
) -> Option<Value> {
    let input: Value = serde_json::from_str(parent_input).ok()?;
    if !grep_original_input_matches(&input, child) {
        return None;
    }
    let tool = child["parent_tool_name"].as_str()?;
    let prefix = format!("Tool `{tool}` completed. Evidence: tool://");
    let tail = presentation.strip_prefix(&prefix)?;
    let (_, mut body) = tail.split_once(". ")?;
    if body.starts_with("JSON object with ") {
        body = body.split_once(". ")?.1;
    }
    let value: Value = serde_json::from_str(body).ok()?;
    let mut output = &value;
    if tool == "tool_orchestrate" {
        let step = &input["steps"][child["step_index"].as_u64()? as usize];
        let id = step["id"].as_str()?;
        let selected = input.get("return").and_then(Value::as_str);
        if selected.is_some_and(|selected| selected != format!("${id}")) {
            return None;
        }
        let partial = value["kind"] == "orchestration_partial_receipt";
        output = if selected.is_some() || (partial && input["steps"].as_array()?.len() == 1) {
            &value["result"]
        } else {
            &value["result"][id]
        };
        if step["op"] == "batch" {
            let position = child["position"].as_u64()? as usize;
            let calls = step["calls"].as_array()?;
            let alias = |i: usize, call: &Value| {
                call["as"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("{}[{i}]", call["tool"].as_str().unwrap_or("")))
            };
            let key = alias(position, calls.get(position)?);
            if calls
                .iter()
                .enumerate()
                .filter(|(i, call)| alias(*i, call) == key)
                .count()
                != 1
            {
                return None;
            }
            output = &output[&key];
        }
        if partial || step["on_error"] == "collect" {
            if output["ok"] != true || (partial && output["source_tool"] != "grep_search") {
                return None;
            }
            output = &output["output"];
        }
    }
    if let Some(index) = child["batch_index"].as_u64() {
        let mut members = output["results"]
            .as_array()?
            .iter()
            .filter(|m| m["index"] == index);
        let member = members.next()?;
        if members.next().is_some() || member["status"] != "success" {
            return None;
        }
        output = &member["output"];
    }
    if output["mode"] != "content" || output["content_omitted"] == true {
        return None;
    }
    let content = output["content"].as_str()?;
    let extent = &child["body"]["extent"];
    let count = usize::try_from(extent["line_count"].as_u64()?).ok()?;
    let start = usize::try_from(child["output_row"].as_u64()?).ok()?;
    if count == 0 || extent["kind"] != "file_lines" {
        return None;
    }
    let rows = content.split('\n').collect::<Vec<_>>();
    if rows.get(start..start.checked_add(count)?)?.join("\n") != child["presentation"].as_str()? {
        return None;
    }
    Some(extent.clone())
}
