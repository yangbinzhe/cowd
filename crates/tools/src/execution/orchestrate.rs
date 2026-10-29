//! Declarative orchestration (`tool_orchestrate`).
//!
//! One model call can express a small bounded program over the workspace:
//! sequential calls, a `map` over a result list, `filter`/`collect`
//! aggregation, a bounded `repeat`, a `when` branch, and a `compute` step that
//! runs `execute_code` on data. Data flows between steps through `$`-references;
//! no general expression language is needed, which keeps it auditable and
//! friendly to weaker models.
//!
//! Safety boundary: every sub-call is restricted to
//! `is_allowed_orchestration_tool` (read-only query tools plus workspace
//! writes) AND, at the runtime/gateway, to the caller's own allow-list before
//! delegation. Write steps run in declaration order under the per-call scope
//! lock, and the call's effect escalates to the strictest sub-call, so a write
//! orchestration requires write authorization. `compute` reuses the existing
//! kernel-hardened, read-only, network-off `execute_code` sandbox.

use std::collections::BTreeMap;

use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::executor::{execute_with_lease, is_allowed_orchestration_tool};
use crate::ToolHostLease;

/// Upper bounds keep one orchestration call bounded in calls and output.
const MAX_STEPS: usize = 50;
const MAX_MAP_ITEMS: usize = 200;
const MAX_REPEAT: usize = 25;
const MAX_TOTAL_CALLS: usize = 400;

pub(crate) fn original_static_file_scopes(
    tool_id: &str,
    input: &Value,
) -> Result<Vec<harness_contract::goal::ProofEffectScopePurpose>, String> {
    use harness_contract::goal::{ProofEffectPurpose, ProofEffectScopePurpose};
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use std::path::{Component, Path};

    let mut scopes = Vec::new();
    let mut add =
        |operation: &PermissionOperation, target: Option<String>, purpose: ProofEffectPurpose| {
            scopes.push(ProofEffectScopePurpose {
                declared_scope: PermissionScope {
                    resource: PermissionResource::File,
                    operation: operation.clone(),
                    target,
                },
                purpose,
            });
        };
    let read = &PermissionOperation::Read;
    let write = &PermissionOperation::Write;
    let path = |value: Option<&Value>| {
        value.and_then(Value::as_str).and_then(|value| {
            (!value.trim().is_empty()
                && Path::new(value)
                    .components()
                    .any(|component| component != Component::CurDir))
            .then(|| value.to_string())
        })
    };
    let directory_path = |value: Option<&Value>| {
        path(value).or_else(|| {
            value.and_then(Value::as_str).and_then(|value| {
                (!value.trim().is_empty()
                    && Path::new(value)
                        .components()
                        .all(|component| component == Component::CurDir))
                .then(|| ".".to_string())
            })
        })
    };
    match tool_id {
        "glob_search" | "grep_search" => {
            let batch_key = if tool_id == "glob_search" {
                "patterns"
            } else {
                "searches"
            };
            let requests = input
                .get(batch_key)
                .and_then(Value::as_array)
                .map(|requests| requests.iter().collect::<Vec<_>>())
                .unwrap_or_else(|| vec![input]);
            for request in requests {
                let target = if tool_id == "glob_search"
                    && request
                        .get("pattern")
                        .and_then(Value::as_str)
                        .is_none_or(|pattern| {
                            pattern.contains('/') || pattern.contains('\\') || pattern == ".."
                        }) {
                    None
                } else {
                    directory_path(request.get("path"))
                };
                add(read, target, ProofEffectPurpose::InputRead);
            }
        }
        "workspace_snapshot" => {
            if let Some(roots) = input.get("roots").and_then(Value::as_array) {
                for root in roots {
                    add(
                        read,
                        directory_path(Some(root)),
                        ProofEffectPurpose::InputRead,
                    );
                }
            } else {
                add(read, None, ProofEffectPurpose::InputRead);
            }
        }
        "checkpoint" | "checkpoint_list" | "checkpoint_diff" => {
            let action = match tool_id {
                "checkpoint_list" => "list",
                "checkpoint_diff" => "diff",
                _ => input
                    .get("action")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            };
            if matches!(action, "diff" | "restore") {
                if let Some(id) = input.get("id").and_then(Value::as_str) {
                    if Path::new(id)
                        .components()
                        .any(|component| !matches!(component, Component::Normal(_)))
                    {
                        return Err("checkpoint id is not a bounded storage child".into());
                    }
                }
            }
            match action {
                "list" => add(
                    read,
                    Some(".cowd/checkpoints".into()),
                    ProofEffectPurpose::InputRead,
                ),
                "diff" => add(read, None, ProofEffectPurpose::InputRead),
                "create" => {
                    if let Some(paths) = input
                        .get("paths")
                        .and_then(Value::as_array)
                        .filter(|paths| !paths.is_empty())
                    {
                        for target in paths {
                            add(read, path(Some(target)), ProofEffectPurpose::InputRead);
                        }
                    } else {
                        add(read, None, ProofEffectPurpose::InputRead);
                    }
                    add(
                        write,
                        Some(".cowd/checkpoints".into()),
                        ProofEffectPurpose::BusinessEffect,
                    );
                }
                "restore" => {
                    add(read, None, ProofEffectPurpose::InputRead);
                    add(write, None, ProofEffectPurpose::BusinessEffect);
                }
                _ => return Err("checkpoint action has no proven filesystem envelope".into()),
            }
        }
        "edit" => {
            let action = input
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default();
            match action {
                "write" | "replace" | "patch" => {
                    if action == "patch" {
                        let edits = input
                            .get("edits")
                            .and_then(Value::as_array)
                            .filter(|edits| !edits.is_empty())
                            .ok_or("edit patch has no proven file targets")?;
                        for edit in edits {
                            let target = path(edit.get("path"))
                                .ok_or("edit patch has an invalid file target")?;
                            add(read, Some(target), ProofEffectPurpose::InputRead);
                        }
                    } else {
                        let target =
                            path(input.get("path")).ok_or("edit has no proven file target")?;
                        add(read, Some(target), ProofEffectPurpose::InputRead);
                    }
                    add(
                        write,
                        Some(".cowd/checkpoints".into()),
                        ProofEffectPurpose::AutoCheckpointMaintenance,
                    );
                }
                "notebook" => add(
                    read,
                    path(input.get("notebook_path")),
                    ProofEffectPurpose::InputRead,
                ),
                "preview" => {}
                _ => return Err("edit action has no proven filesystem envelope".into()),
            }
        }
        "vision_analyze" => add(
            read,
            path(input.get("image_path")),
            ProofEffectPurpose::InputRead,
        ),
        "lsp" if input.get("action").and_then(Value::as_str) == Some("diagnostics") => {}
        "lsp" => return Err("LSP registered server root is not a proven lease workspace".into()),
        "ask" if input.get("message").is_some() => {
            if let Some(attachments) = input.get("attachments").and_then(Value::as_array) {
                for attachment in attachments {
                    add(read, path(Some(attachment)), ProofEffectPurpose::InputRead);
                }
            }
        }
        "todo_write" => {
            let target = super::todo_store_path(Path::new("."));
            let target = target.to_string_lossy().into_owned();
            add(read, Some(target.clone()), ProofEffectPurpose::InputRead);
            add(write, Some(target), ProofEffectPurpose::BusinessEffect);
        }
        "config" => {
            if let Some(spec) = input
                .get("setting")
                .and_then(Value::as_str)
                .and_then(|setting| super::supported_config_setting(setting.trim()))
            {
                let target = super::config_file_for_scope(spec.scope, Path::new("."))?;
                let target =
                    if matches!(spec.scope, super::ConfigScope::Global) && target.is_relative() {
                        std::env::current_dir()
                            .map_err(|error| error.to_string())?
                            .join(target)
                    } else {
                        target
                    }
                    .to_string_lossy()
                    .into_owned();
                add(read, Some(target.clone()), ProofEffectPurpose::InputRead);
                if input.get("value").is_some_and(|value| !value.is_null()) {
                    add(write, Some(target), ProofEffectPurpose::BusinessEffect);
                }
            }
        }
        "plan_mode" => {
            for target in [
                super::config_file_for_scope(super::ConfigScope::Settings, Path::new("."))?,
                super::plan_mode_state_file(Path::new("."))?,
            ] {
                let target = target.to_string_lossy().into_owned();
                add(read, Some(target.clone()), ProofEffectPurpose::InputRead);
                add(write, Some(target), ProofEffectPurpose::BusinessEffect);
            }
        }
        "skill" => {
            let action = input
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let store =
                skill::ManagedSkillStore::default_for_user().map_err(|error| error.to_string())?;
            let store_root = if store.root().is_relative() {
                std::env::current_dir()
                    .map_err(|error| error.to_string())?
                    .join(store.root())
            } else {
                store.root().to_path_buf()
            }
            .to_string_lossy()
            .into_owned();
            add(
                read,
                Some(store_root.clone()),
                ProofEffectPurpose::InputRead,
            );
            if action == "install_commit" {
                add(write, Some(store_root), ProofEffectPurpose::BusinessEffect);
            }
            if matches!(action, "install_plan" | "install_commit") {
                let source = input
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim();
                if !source.starts_with("https://github.com/") && !source.starts_with("github://") {
                    add(
                        read,
                        path(input.get("source")),
                        ProofEffectPurpose::InputRead,
                    );
                }
                let temporary = std::env::temp_dir();
                let temporary = if temporary.is_relative() {
                    std::env::current_dir()
                        .map_err(|error| error.to_string())?
                        .join(temporary)
                } else {
                    temporary
                }
                .to_string_lossy()
                .into_owned();
                add(read, Some(temporary.clone()), ProofEffectPurpose::InputRead);
                add(write, Some(temporary), ProofEffectPurpose::BusinessEffect);
            }
        }
        _ => {}
    }
    Ok(scopes)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OrchestrateInput {
    steps: Vec<Step>,
    #[serde(default, rename = "return")]
    ret: Option<String>,
    #[serde(default)]
    max_concurrency: Option<usize>,
}

/// batch/map failure policy. `fail` (default) aborts the whole program on the
/// first sub-call error; `collect` returns a per-item receipt and continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OnError {
    Fail,
    Collect,
}

/// Per-item receipt for a `collect` batch/map entry: preserves the status and
/// duration the batch engine already measured, so a caller can read the
/// successful items without losing the failed ones.
fn batch_item_value(result: &crate::executor::BatchToolItemOutput) -> Value {
    serde_json::json!({
        "ok": result.output.is_some(),
        "status": result.status,
        "durationMs": result.duration_ms,
        "output": result.output,
        "error": result.error,
    })
}

#[derive(Debug, Deserialize)]
struct Step {
    id: String,
    #[serde(flatten)]
    kind: StepKind,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum StepKind {
    Call {
        tool: String,
        #[serde(default)]
        input: Value,
    },
    Batch {
        calls: Vec<BatchCall>,
        #[serde(default)]
        on_error: Option<OnError>,
    },
    Map {
        over: String,
        tool: String,
        #[serde(default)]
        input: Value,
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        on_error: Option<OnError>,
    },
    Filter {
        over: String,
        field: String,
        #[serde(default)]
        contains: Option<String>,
        #[serde(default)]
        equals: Option<Value>,
    },
    Collect {
        over: String,
        agg: CollectAgg,
        #[serde(default)]
        field: Option<String>,
        #[serde(default)]
        separator: Option<String>,
    },
    Compute {
        language: String,
        code: String,
        #[serde(default)]
        input: Value,
    },
    When {
        #[serde(rename = "if")]
        condition: Condition,
        tool: String,
        #[serde(default)]
        input: Value,
    },
    Repeat {
        max: usize,
        #[serde(rename = "while")]
        condition: Condition,
        tool: String,
        #[serde(default)]
        input: Value,
    },
}

#[derive(Debug, Deserialize)]
struct BatchCall {
    #[serde(default)]
    tool: String,
    #[serde(default)]
    input: Value,
    #[serde(default, rename = "as")]
    alias: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CollectAgg {
    Count,
    First,
    Last,
    Join,
    Sum,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Condition {
    #[serde(rename = "ref")]
    reference: String,
    #[serde(default)]
    present: Option<bool>,
    #[serde(default)]
    equals: Option<Value>,
    #[serde(default)]
    contains: Option<String>,
    #[serde(default)]
    non_empty: Option<bool>,
}

type State = BTreeMap<String, Value>;

pub(crate) fn run_orchestrate(lease: &ToolHostLease, input: &Value) -> Result<String, String> {
    let (parsed, calls) = parse_preflight(input)?;
    lease.orchestration_child_effects(&calls)?;
    let mut state: State = BTreeMap::new();
    let mut total_calls = 0usize;
    let mut calls_trace: Vec<Value> = Vec::new();
    for (step_index, step) in parsed.steps.iter().enumerate() {
        let before = total_calls;
        let started = std::time::Instant::now();
        let (op, tool) = step_descriptor(&step.kind);
        match evaluate(
            lease,
            step_index,
            &step.id,
            &step.kind,
            &state,
            &mut total_calls,
            parsed.max_concurrency,
        ) {
            Ok(value) => {
                calls_trace.push(step_trace(
                    &step.id,
                    op,
                    tool,
                    total_calls - before,
                    "ok",
                    None,
                    started,
                ));
                state.insert(step.id.clone(), value);
            }
            Err(error) => {
                calls_trace.push(step_trace(
                    &step.id,
                    op,
                    tool,
                    total_calls - before,
                    "error",
                    Some(&error),
                    started,
                ));
                return Err(format!(
                    "tool_orchestrate step `{}` ({op}) failed: {error}; trace: {}",
                    step.id,
                    serde_json::to_string(&calls_trace).unwrap_or_default()
                ));
            }
        }
    }
    let result = match parsed.ret.as_deref() {
        Some(reference) => resolve_ref(reference, &state, None)?,
        None => Value::Object(state.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
    };
    serde_json::to_string_pretty(&json!({
        "calls": calls_trace,
        "steps": state,
        "result": result
    }))
    .map_err(|error| error.to_string())
}

/// The dispatch a step performs, for the model-visible evidence trace.
fn step_descriptor(kind: &StepKind) -> (&'static str, Option<&str>) {
    match kind {
        StepKind::Call { tool, .. } => ("call", Some(tool)),
        StepKind::Batch { .. } => ("batch", None),
        StepKind::Map { tool, .. } => ("map", Some(tool)),
        StepKind::Filter { .. } => ("filter", None),
        StepKind::Collect { .. } => ("collect", None),
        StepKind::Compute { .. } => ("compute", Some("execute_code")),
        StepKind::When { tool, .. } => ("when", Some(tool)),
        StepKind::Repeat { tool, .. } => ("repeat", Some(tool)),
    }
}

fn step_trace(
    step_id: &str,
    op: &str,
    tool: Option<&str>,
    calls: usize,
    status: &str,
    error: Option<&str>,
    started: std::time::Instant,
) -> Value {
    let mut object = Map::new();
    object.insert("stepId".to_string(), json!(step_id));
    object.insert("op".to_string(), json!(op));
    if let Some(tool) = tool {
        object.insert("tool".to_string(), json!(tool));
    }
    object.insert("calls".to_string(), json!(calls));
    object.insert("status".to_string(), json!(status));
    if let Some(error) = error {
        object.insert("error".to_string(), json!(error));
    }
    object.insert(
        "durationMs".to_string(),
        json!(started.elapsed().as_millis() as u64),
    );
    Value::Object(object)
}

/// What a composite (orchestration) tool wants to dispatch, so the
/// authorization owner can fail closed *before* any delegation happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubCallAuthorization {
    /// The tool is not a composite orchestrator; no extra check applies.
    NotOrchestration,
    /// The tool will dispatch exactly these tools; each must be authorized.
    Requires(Vec<String>),
    /// The composite input cannot be trusted; the caller must reject it.
    Invalid(String),
}

/// Extract the literal tool names an orchestration input would dispatch.
///
/// Reuses the same `OrchestrateInput`/`StepKind` parser the evaluator uses, so
/// the authorization set can never drift from what actually runs. `compute`
/// requires `execute_code`. A `tool` value containing `$` is rejected: tool
/// names are literals, never reference-resolved. Returns `NotOrchestration`
/// for every non-orchestration tool.
pub fn requested_sub_call_tools(tool_name: &str, input: &Value) -> SubCallAuthorization {
    if tool_name != "tool_orchestrate" {
        return SubCallAuthorization::NotOrchestration;
    }
    let calls = match effect_preflight(input) {
        Ok(calls) => calls,
        Err(error) => return SubCallAuthorization::Invalid(error),
    };
    let mut names = calls.into_iter().map(|call| call.tool).collect::<Vec<_>>();
    names.sort();
    names.dedup();
    SubCallAuthorization::Requires(names)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreflightCall {
    pub step_id: String,
    pub operation: &'static str,
    pub tool: String,
    pub input: Value,
    pub references: Vec<Vec<String>>,
    pub maximum_calls: usize,
}

pub(crate) fn effect_preflight(input: &Value) -> Result<Vec<PreflightCall>, String> {
    parse_preflight(input).map(|(_, calls)| calls)
}

fn parse_preflight(input: &Value) -> Result<(OrchestrateInput, Vec<PreflightCall>), String> {
    let parsed: OrchestrateInput = serde_json::from_value(input.clone())
        .map_err(|error| format!("tool_orchestrate input is invalid: {error}"))?;
    if parsed.steps.len() > MAX_STEPS {
        return Err(format!(
            "tool_orchestrate supports at most {MAX_STEPS} steps"
        ));
    }
    let mut calls = Vec::new();
    let mut declared_calls = 0usize;
    for step in &parsed.steps {
        let (operation, _) = step_descriptor(&step.kind);
        let mut append = |tool: &str, input: &Value, maximum_calls: usize, compute: bool| {
            if tool.contains('$') || !is_allowed_orchestration_tool(tool) {
                return Err(format!(
                    "tool_orchestrate cannot dispatch `{tool}`: expected an allowed literal tool"
                ));
            }
            let mut references = Vec::new();
            let value = if compute { &input["input"] } else { input };
            let mut path = if compute {
                vec!["input".to_string()]
            } else {
                Vec::new()
            };
            transform_value(value, &mut path, &mut |reference, path| {
                references.push(path.to_vec());
                Ok(Value::String(reference.to_string()))
            })?;
            calls.push(PreflightCall {
                step_id: step.id.clone(),
                operation,
                tool: tool.to_string(),
                input: input.clone(),
                references,
                maximum_calls,
            });
            Ok::<(), String>(())
        };
        match &step.kind {
            StepKind::Call { tool, input } | StepKind::When { tool, input, .. } => {
                append(tool, input, 1, false)?;
                if matches!(&step.kind, StepKind::Call { .. }) {
                    declared_calls += 1;
                }
            }
            StepKind::Batch { calls, .. } => {
                if calls.len() > MAX_MAP_ITEMS {
                    return Err(format!("batch exceeds the limit of {MAX_MAP_ITEMS}"));
                }
                for call in calls {
                    append(&call.tool, &call.input, 1, false)?;
                }
                declared_calls += calls.len();
            }
            StepKind::Map { tool, input, .. } => {
                append(tool, input, MAX_MAP_ITEMS, false)?;
            }
            StepKind::Repeat {
                tool, input, max, ..
            } => {
                if *max > MAX_REPEAT {
                    return Err(format!("repeat exceeds the limit of {MAX_REPEAT}"));
                }
                append(tool, input, *max, false)?;
            }
            StepKind::Compute {
                language,
                code,
                input,
            } => {
                if !matches!(
                    language.trim().to_ascii_lowercase().as_str(),
                    "python" | "py" | "javascript" | "js" | "node"
                ) {
                    return Err(format!(
                        "compute supports python or javascript, got `{language}`"
                    ));
                }
                append(
                    "execute_code",
                    &json!({"language": language, "code": code, "input": input, "input_files": []}),
                    1,
                    true,
                )?;
                declared_calls += 1;
            }
            StepKind::Filter { .. } | StepKind::Collect { .. } => {}
        }
        if declared_calls > MAX_TOTAL_CALLS {
            return Err(format!(
                "tool_orchestrate exceeded the total sub-call limit of {MAX_TOTAL_CALLS}"
            ));
        }
    }
    Ok((parsed, calls))
}

fn evaluate(
    lease: &ToolHostLease,
    step_index: usize,
    step_id: &str,
    kind: &StepKind,
    state: &State,
    total_calls: &mut usize,
    max_concurrency: Option<usize>,
) -> Result<Value, String> {
    match kind {
        StepKind::Call { tool, input } => {
            let resolved = resolve_value(input, state, None)?;
            call_tool(lease, step_index, step_id, 0, tool, &resolved, total_calls)
        }
        StepKind::Batch { calls, on_error } => {
            if calls.len() > MAX_MAP_ITEMS {
                return Err(format!(
                    "batch has {} calls, over the limit of {MAX_MAP_ITEMS}",
                    calls.len()
                ));
            }
            if *total_calls + calls.len() > MAX_TOTAL_CALLS {
                return Err(format!(
                    "tool_orchestrate exceeded the total sub-call limit of {MAX_TOTAL_CALLS}"
                ));
            }
            let mut prepared = Vec::with_capacity(calls.len());
            for (position, call) in calls.iter().enumerate() {
                prepared.push((
                    call.alias.clone(),
                    call.tool.clone(),
                    resolve_value(&call.input, state, None)?,
                    position,
                ));
            }
            let items = prepared.clone();
            let results = crate::executor::run_ordered_batch(prepared, max_concurrency, |call| {
                call_tool_uncounted(lease, step_index, step_id, call.3, &call.1, &call.2)
            });
            *total_calls += items.len();
            let collect = matches!(on_error, Some(OnError::Collect));
            let mut object = Map::new();
            for (index, result) in results.into_iter().enumerate() {
                let (alias, tool, _, _) = &items[index];
                let key = alias.clone().unwrap_or_else(|| format!("{tool}[{index}]"));
                if collect {
                    object.insert(key, batch_item_value(&result));
                    continue;
                }
                match result.output {
                    Some(value) => {
                        object.insert(key, value);
                    }
                    None => {
                        return Err(result
                            .error
                            .unwrap_or_else(|| format!("batch call `{tool}` failed")))
                    }
                }
            }
            Ok(Value::Object(object))
        }
        StepKind::Map {
            over,
            tool,
            input,
            field,
            on_error,
        } => {
            let list = resolve_ref(over, state, None)?;
            let items = list
                .as_array()
                .ok_or_else(|| format!("map.over `{over}` must resolve to an array"))?
                .clone();
            if items.len() > MAX_MAP_ITEMS {
                return Err(format!(
                    "map.over has {} items, over the limit of {MAX_MAP_ITEMS}",
                    items.len()
                ));
            }
            if *total_calls + items.len() > MAX_TOTAL_CALLS {
                return Err(format!(
                    "tool_orchestrate exceeded the total sub-call limit of {MAX_TOTAL_CALLS}"
                ));
            }
            // Bounded parallel fan-out, matching `read_many`/`grep_many`.
            let indexed = items.iter().cloned().enumerate().collect::<Vec<_>>();
            let results =
                crate::executor::run_ordered_batch(indexed, max_concurrency, |(position, item)| {
                    let resolved = resolve_value(input, state, Some(&item))?;
                    call_tool_uncounted(lease, step_index, step_id, position, tool, &resolved)
                });
            *total_calls += items.len();
            let collect = matches!(on_error, Some(OnError::Collect));
            let mut outputs = Vec::with_capacity(results.len());
            for result in results {
                if collect {
                    outputs.push(batch_item_value(&result));
                    continue;
                }
                match result.output {
                    Some(value) => outputs.push(select_field(value, field.as_deref())),
                    None => {
                        return Err(result
                            .error
                            .unwrap_or_else(|| "map sub-call failed".to_string()))
                    }
                }
            }
            Ok(Value::Array(outputs))
        }
        StepKind::Filter {
            over,
            field,
            contains,
            equals,
        } => {
            let list = resolve_ref(over, state, None)?;
            let items = list
                .as_array()
                .ok_or_else(|| format!("filter.over `{over}` must resolve to an array"))?;
            let kept = items
                .iter()
                .filter(|item| {
                    let candidate = item.get(field);
                    if let Some(needle) = contains {
                        candidate
                            .and_then(Value::as_str)
                            .is_some_and(|text| text.contains(needle))
                    } else if let Some(expected) = equals {
                        candidate == Some(expected)
                    } else {
                        candidate.is_some_and(|value| !value.is_null())
                    }
                })
                .cloned()
                .collect::<Vec<_>>();
            Ok(Value::Array(kept))
        }
        StepKind::Collect {
            over,
            agg,
            field,
            separator,
        } => {
            let list = resolve_ref(over, state, None)?;
            let items = list
                .as_array()
                .ok_or_else(|| format!("collect.over `{over}` must resolve to an array"))?;
            collect(items, *agg, field.as_deref(), separator.as_deref())
        }
        StepKind::Compute {
            language,
            code,
            input,
        } => {
            let resolved = resolve_value(input, state, None)?;
            bump(total_calls)?;
            run_compute(lease, language, code, &resolved)
        }
        StepKind::When {
            condition,
            tool,
            input,
        } => {
            if eval_condition(condition, state, None)? {
                let resolved = resolve_value(input, state, None)?;
                call_tool(lease, step_index, step_id, 0, tool, &resolved, total_calls)
            } else {
                Ok(Value::Null)
            }
        }
        StepKind::Repeat {
            max,
            condition,
            tool,
            input,
        } => {
            if *max > MAX_REPEAT {
                return Err(format!(
                    "repeat.max {max} exceeds the limit of {MAX_REPEAT}"
                ));
            }
            let mut last = Value::Null;
            for position in 0..*max {
                if !eval_condition(condition, state, None)? {
                    break;
                }
                let resolved = resolve_value(input, state, None)?;
                last = call_tool(
                    lease,
                    step_index,
                    step_id,
                    position,
                    tool,
                    &resolved,
                    total_calls,
                )?;
            }
            Ok(last)
        }
    }
}

fn bump(total_calls: &mut usize) -> Result<(), String> {
    *total_calls += 1;
    if *total_calls > MAX_TOTAL_CALLS {
        return Err(format!(
            "tool_orchestrate exceeded the total sub-call limit of {MAX_TOTAL_CALLS}"
        ));
    }
    Ok(())
}

fn call_tool(
    lease: &ToolHostLease,
    step_index: usize,
    step_id: &str,
    position: usize,
    tool: &str,
    input: &Value,
    total_calls: &mut usize,
) -> Result<Value, String> {
    bump(total_calls)?;
    call_tool_uncounted(lease, step_index, step_id, position, tool, input)
}

/// Dispatch one allowed sub-call without touching the shared call counter, so
/// the `map` step can fan out in parallel and count once.
fn call_tool_uncounted(
    lease: &ToolHostLease,
    step_index: usize,
    step_id: &str,
    position: usize,
    tool: &str,
    input: &Value,
) -> Result<Value, String> {
    if !is_allowed_orchestration_tool(tool) {
        return Err(format!(
            "tool_orchestrate cannot dispatch `{tool}`: not an allowed orchestration tool"
        ));
    }
    let output = execute_with_lease(lease, tool, input)?;
    if tool == "read_file" {
        lease.record_orchestrated_read(step_index, step_id, position, input, &output)?;
    }
    Ok(serde_json::from_str(&output).unwrap_or(Value::String(output)))
}

fn run_compute(
    lease: &ToolHostLease,
    language: &str,
    code: &str,
    input: &Value,
) -> Result<Value, String> {
    // Data crosses into the sandbox as a base64 JSON literal, never by string
    // interpolation into executable source, so tool output cannot inject code.
    let data = serde_json::to_string(input).map_err(|error| error.to_string())?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(data.as_bytes());
    let source = match language.trim().to_ascii_lowercase().as_str() {
        "python" | "py" => format!(
            "import base64, json\n_input = json.loads(base64.b64decode(\"{encoded}\").decode())\n{code}"
        ),
        "javascript" | "js" | "node" => format!(
            "const _input = JSON.parse(Buffer.from(\"{encoded}\", \"base64\").toString())\n{code}"
        ),
        other => {
            return Err(format!(
                "compute supports python or javascript, got `{other}`"
            ))
        }
    };
    let result = crate::sandbox_exec::execute_code_in_workspace(
        if matches!(
            language.trim().to_ascii_lowercase().as_str(),
            "python" | "py"
        ) {
            "python"
        } else {
            "javascript"
        },
        &source,
        None,
        lease.workspace_root(),
        &[],
    );
    if result.exit_code != 0 {
        return Err(format!(
            "compute failed (exit {}): {}",
            result.exit_code,
            result.stderr.trim()
        ));
    }
    Ok(serde_json::from_str(result.stdout.trim()).unwrap_or(Value::String(result.stdout)))
}

fn collect(
    items: &[Value],
    agg: CollectAgg,
    field: Option<&str>,
    separator: Option<&str>,
) -> Result<Value, String> {
    match agg {
        CollectAgg::Count => Ok(json!(items.len())),
        CollectAgg::First => Ok(select_field(
            items.first().cloned().unwrap_or(Value::Null),
            field,
        )),
        CollectAgg::Last => Ok(select_field(
            items.last().cloned().unwrap_or(Value::Null),
            field,
        )),
        CollectAgg::Join => {
            // The published contract (`tool_orchestrate` documentation) says the
            // default separator is a single space; the code must not silently do
            // otherwise.
            let separator = separator.unwrap_or(" ");
            let parts = items
                .iter()
                .map(|item| {
                    select_field(item.clone(), field)
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| item.to_string())
                })
                .collect::<Vec<_>>();
            Ok(Value::String(parts.join(separator)))
        }
        CollectAgg::Sum => {
            let mut total = 0.0f64;
            for item in items {
                let value = select_field(item.clone(), field);
                match value.as_f64() {
                    Some(number) => total += number,
                    None => return Err(format!("collect(sum) expected a number, got `{value}`")),
                }
            }
            Ok(json!(total))
        }
    }
}

fn select_field(value: Value, field: Option<&str>) -> Value {
    match field {
        Some(field) => value.get(field).cloned().unwrap_or(Value::Null),
        None => value,
    }
}

fn eval_condition(
    condition: &Condition,
    state: &State,
    item: Option<&Value>,
) -> Result<bool, String> {
    let resolved = resolve_ref(&condition.reference, state, item);
    let value = match resolved {
        Ok(value) => Some(value),
        Err(_) => None,
    };
    if let Some(present) = condition.present {
        let is_present = value.as_ref().is_some_and(|value| !value.is_null());
        return Ok(is_present == present);
    }
    let Some(value) = value else {
        return Ok(false);
    };
    if let Some(expected) = &condition.equals {
        return Ok(&value == expected);
    }
    if let Some(needle) = &condition.contains {
        return Ok(value
            .as_str()
            .is_some_and(|text| text.contains(needle.as_str())));
    }
    if let Some(non_empty) = condition.non_empty {
        let empty = value.is_null()
            || value.as_str().is_some_and(str::is_empty)
            || value.as_array().is_some_and(Vec::is_empty);
        return Ok(non_empty == !empty);
    }
    Ok(!value.is_null())
}

fn resolve_ref(expr: &str, state: &State, item: Option<&Value>) -> Result<Value, String> {
    let path = expr
        .strip_prefix('$')
        .ok_or_else(|| format!("`{expr}` is not a $reference"))?;
    let mut segments = path.split('.');
    let head = segments.next().unwrap_or_default();
    let mut current = if head == "item" {
        item.cloned()
            .ok_or_else(|| "$item is only available inside a map step".to_string())?
    } else {
        state
            .get(head)
            .cloned()
            .ok_or_else(|| format!("unknown step reference `${head}`"))?
    };
    for segment in segments {
        current = current
            .get(segment)
            .cloned()
            .ok_or_else(|| format!("reference `{expr}` has no field `{segment}`"))?;
    }
    Ok(current)
}

fn resolve_value(value: &Value, state: &State, item: Option<&Value>) -> Result<Value, String> {
    transform_value(value, &mut Vec::new(), &mut |reference, _| {
        resolve_ref(reference, state, item)
    })
}

fn transform_value(
    value: &Value,
    path: &mut Vec<String>,
    reference: &mut impl FnMut(&str, &[String]) -> Result<Value, String>,
) -> Result<Value, String> {
    match value {
        Value::String(text) if is_reference(text) => reference(text, path),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                path.push(index.to_string());
                let result = transform_value(entry, path, reference);
                path.pop();
                result
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(entries) => entries
            .iter()
            .map(|(key, entry)| {
                path.push(key.clone());
                let result = transform_value(entry, path, reference);
                path.pop();
                Ok((key.clone(), result?))
            })
            .collect::<Result<Map<String, Value>, String>>()
            .map(Value::Object),
        other => Ok(other.clone()),
    }
}

fn is_reference(text: &str) -> bool {
    let Some(rest) = text.strip_prefix('$') else {
        return false;
    };
    !rest.is_empty()
        && rest
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
        && !rest.starts_with('.')
        && !rest.ends_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_uses_explicit_empty_project_input_and_cannot_import_live_files() {
        let root =
            std::env::temp_dir().join(format!("cowd-bounded-compute-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("live_only_module.py"), "secret=42").unwrap();
        let lease = crate::ToolHost::builtin("bounded-compute", &root).pin_snapshot();
        let program = json!({"steps":[{"id":"sum","op":"compute","language":"python","code":"print(sum(_input))","input":[2,3]}]});
        let preflight = effect_preflight(&program).unwrap();
        assert_eq!(preflight[0].input["input_files"], json!([]));
        assert_eq!(
            run_compute(&lease, "python", "print(sum(_input))", &json!([2, 3])).unwrap(),
            json!(5)
        );
        assert!(run_compute(&lease, "python", "import live_only_module", &json!(null)).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn static_scope_purposes_are_assigned_only_by_the_original_producer() {
        use harness_contract::goal::ProofEffectPurpose;
        use harness_contract::policy::PermissionOperation;

        for action in ["write", "replace", "patch"] {
            let input = json!({"action":action,"path":"src/a","edits":[{"path":"src/a"}]});
            let scopes = original_static_file_scopes("edit", &input).unwrap();
            assert!(scopes.iter().any(|entry| entry.declared_scope.operation
                == PermissionOperation::Read
                && entry.declared_scope.target.as_deref() == Some("src/a")
                && entry.purpose == ProofEffectPurpose::InputRead));
            assert_eq!(
                scopes
                    .iter()
                    .filter(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance)
                    .count(),
                1
            );
        }
        for (tool, input) in [
            ("checkpoint", json!({"action":"create","paths":["src/a"]})),
            ("checkpoint", json!({"action":"restore","id":"snapshot"})),
            ("checkpoint", json!({"action":"list"})),
            (
                "edit",
                json!({"action":"notebook","notebook_path":".cowd/checkpoints"}),
            ),
            (
                "grep_search",
                json!({"path":".cowd/checkpoints","pattern":"needle"}),
            ),
        ] {
            let scopes = original_static_file_scopes(tool, &input).unwrap();
            assert!(!scopes
                .iter()
                .any(|entry| entry.purpose == ProofEffectPurpose::AutoCheckpointMaintenance));
            assert!(scopes.iter().all(|entry| entry.purpose
                == if entry.declared_scope.operation == PermissionOperation::Read {
                    ProofEffectPurpose::InputRead
                } else {
                    ProofEffectPurpose::BusinessEffect
                }));
        }
        for input in [
            json!({"action":"write"}),
            json!({"action":"patch","edits":[]}),
            json!({"action":"unknown"}),
        ] {
            assert!(original_static_file_scopes("edit", &input).is_err());
        }
    }

    fn state_with(id: &str, value: Value) -> State {
        let mut state = State::new();
        state.insert(id.to_string(), value);
        state
    }

    #[test]
    fn resolves_nested_references_and_item() {
        let state = state_with("m", json!({"matches": [{"path": "a.rs"}]}));
        assert_eq!(
            resolve_ref("$m.matches", &state, None).unwrap(),
            json!([{"path": "a.rs"}])
        );
        assert_eq!(
            resolve_ref("$item.path", &state, Some(&json!({"path": "b.rs"}))).unwrap(),
            json!("b.rs")
        );
        assert!(resolve_ref("$missing", &state, None).is_err());
    }

    #[test]
    fn resolves_references_inside_object_inputs() {
        let state = state_with("m", json!({"count": 2}));
        let resolved =
            resolve_value(&json!({"pattern": "x", "limit": "$m.count"}), &state, None).unwrap();
        assert_eq!(resolved, json!({"pattern": "x", "limit": 2}));
    }

    #[test]
    fn collects_count_first_last_join_sum() {
        let items = vec![json!({"name": "a", "n": 1}), json!({"name": "b", "n": 2})];
        assert_eq!(
            collect(&items, CollectAgg::Count, None, None).unwrap(),
            json!(2)
        );
        assert_eq!(
            collect(&items, CollectAgg::Last, Some("name"), None).unwrap(),
            json!("b")
        );
        assert_eq!(
            collect(&items, CollectAgg::Join, Some("name"), Some(",")).unwrap(),
            json!("a,b")
        );
        // The published contract says the default separator is a single space;
        // this pins it so the code cannot drift from the documentation again.
        assert_eq!(
            collect(&items, CollectAgg::Join, Some("name"), None).unwrap(),
            json!("a b")
        );
        assert_eq!(
            collect(&items, CollectAgg::Sum, Some("n"), None).unwrap(),
            json!(3.0)
        );
    }

    #[test]
    fn conditions_cover_present_equals_contains_and_non_empty() {
        let state = state_with("m", json!({"ok": true, "text": "needle", "empty": ""}));
        let present = Condition {
            reference: "$m.ok".to_string(),
            present: Some(true),
            equals: None,
            contains: None,
            non_empty: None,
        };
        assert!(eval_condition(&present, &state, None).unwrap());
        let absent = Condition {
            reference: "$m.missing".to_string(),
            present: Some(false),
            equals: None,
            contains: None,
            non_empty: None,
        };
        assert!(eval_condition(&absent, &state, None).unwrap());
        let contains = Condition {
            reference: "$m.text".to_string(),
            present: None,
            equals: None,
            contains: Some("need".to_string()),
            non_empty: None,
        };
        assert!(eval_condition(&contains, &state, None).unwrap());
        let empty = Condition {
            reference: "$m.empty".to_string(),
            present: None,
            equals: None,
            contains: None,
            non_empty: Some(false),
        };
        assert!(eval_condition(&empty, &state, None).unwrap());
    }

    #[test]
    fn references_do_not_swallow_literal_dollar_strings() {
        // A shell variable is not a `$reference`: it is not a bare identifier path.
        assert!(!is_reference("$(date)"));
        assert!(!is_reference("$HOME/x"));
        assert!(is_reference("$item.path"));
        assert!(is_reference("$m"));
    }

    #[test]
    fn extracts_literal_sub_calls_and_maps_compute_to_execute_code() {
        let input = json!({
            "steps": [
                {"id": "g", "op": "call", "tool": "glob_search", "input": {}},
                {"id": "r", "op": "map", "over": "$g.filenames", "tool": "read_file", "input": {}},
                {"id": "c", "op": "compute", "language": "python", "code": "print(1)", "input": []},
                {"id": "b", "op": "batch", "calls": [{"tool": "tool_cache_stats", "input": {}}, {"tool": "workspace_snapshot", "input": {}}]},
                {"id": "w", "op": "when", "if": {"ref": "$x", "present": true}, "tool": "grep_search", "input": {}}
            ]
        });
        match requested_sub_call_tools("tool_orchestrate", &input) {
            SubCallAuthorization::Requires(names) => assert_eq!(
                names,
                vec![
                    "execute_code",
                    "glob_search",
                    "grep_search",
                    "read_file",
                    "tool_cache_stats",
                    "workspace_snapshot"
                ]
            ),
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(
            requested_sub_call_tools("read_file", &json!({})),
            SubCallAuthorization::NotOrchestration
        );
    }

    #[test]
    fn rejects_reference_tool_names_and_unparseable_input() {
        let referenced = json!({
            "steps": [{"id": "x", "op": "call", "tool": "$item.name", "input": {}}]
        });
        assert!(matches!(
            requested_sub_call_tools("tool_orchestrate", &referenced),
            SubCallAuthorization::Invalid(_)
        ));
        assert!(matches!(
            requested_sub_call_tools("tool_orchestrate", &json!({"bad": true})),
            SubCallAuthorization::Invalid(_)
        ));
    }

    #[test]
    fn original_file_executors_and_their_static_descriptors_agree() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let root = std::env::temp_dir().join(format!(
            "cowd-original-file-owners-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join("tests")).unwrap();
        std::fs::write(root.join("src/a.rs"), "fn needle() {}\n").unwrap();
        std::fs::write(root.join("tests/b.rs"), "fn needle_test() {}\n").unwrap();
        std::fs::write(root.join("image.png"), [1u8, 2, 3]).unwrap();
        let lease = crate::ToolHost::builtin("original-file-owners", &root).pin_snapshot();
        for (tool, input, target, observed) in [
            (
                "glob_search",
                json!({"path":"src","pattern":"*.rs"}),
                "src",
                "a.rs",
            ),
            (
                "glob_search",
                json!({"patterns":[{"path":"src","pattern":"*.rs"},{"path":"tests","pattern":"*.rs"}]}),
                "tests",
                "b.rs",
            ),
            (
                "grep_search",
                json!({"path":"src","pattern":"needle"}),
                "src",
                "needle",
            ),
            (
                "grep_search",
                json!({"searches":[{"path":"src","pattern":"needle"},{"path":"tests","pattern":"needle"}]}),
                "tests",
                "needle_test",
            ),
            (
                "grep_search",
                json!({"path":"src","pattern":"needle","language":"rust"}),
                "src",
                "needle",
            ),
            (
                "workspace_snapshot",
                json!({"roots":["src","tests"],"include_files":true}),
                "src",
                "a.rs",
            ),
            (
                "read_file",
                json!({"path":"src/a.rs"}),
                "src/a.rs",
                "needle",
            ),
            (
                "vision_analyze",
                json!({"image_path":"image.png","prompt":"describe"}),
                "image.png",
                "image_base64",
            ),
            (
                "ask",
                json!({"message":"attachment","attachments":["src/a.rs"]}),
                "src/a.rs",
                "src/a.rs",
            ),
        ] {
            let effect = lease.describe_effect(tool, &input);
            assert!(
                effect
                    .scopes
                    .iter()
                    .any(|scope| scope.resource == PermissionResource::File
                        && scope.operation == PermissionOperation::Read
                        && scope.target.as_deref() == Some(target)),
                "{tool}"
            );
            assert!(effect
                .scopes
                .iter()
                .filter(|scope| scope.resource == PermissionResource::File)
                .all(|scope| scope.operation == PermissionOperation::Read));
            let output = execute_with_lease(&lease, tool, &input).unwrap();
            assert!(output.contains(observed), "{tool}: {output}");
            assert_eq!(
                lease.describe_effect(tool, &input).descriptor_hash,
                effect.descriptor_hash
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn original_checkpoint_create_diff_restore_and_control_storage_match_the_envelope() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let root = std::env::temp_dir().join(format!(
            "cowd-original-checkpoint-owners-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::write(root.join("src/a.txt"), "before").unwrap();
        std::fs::write(root.join("outside.txt"), "original").unwrap();
        let lease = crate::ToolHost::builtin("original-checkpoint-owners", &root).pin_snapshot();
        let create = json!({"action":"create","label":"source-owner","paths":["src"]});
        let created: Value =
            serde_json::from_str(&execute_with_lease(&lease, "checkpoint", &create).unwrap())
                .unwrap();
        let id = created["id"].as_str().unwrap();
        assert!(root
            .join(".cowd/checkpoints")
            .join(id)
            .join("src/a.txt")
            .is_file());
        assert!(!root
            .join(".cowd/checkpoints")
            .join(id)
            .join("outside.txt")
            .exists());
        for (operation, target) in [
            (PermissionOperation::Read, "src"),
            (PermissionOperation::Write, ".cowd/checkpoints"),
        ] {
            assert!(lease
                .describe_effect("checkpoint", &create)
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File
                    && scope.operation == operation
                    && scope.target.as_deref() == Some(target)));
        }
        std::fs::write(root.join("src/a.txt"), "after").unwrap();
        std::fs::write(root.join("src/new.txt"), "remove on restore").unwrap();
        std::fs::write(root.join("outside.txt"), "retained").unwrap();
        let diff = json!({"action":"diff","id":id});
        let difference: Value =
            serde_json::from_str(&execute_with_lease(&lease, "checkpoint", &diff).unwrap())
                .unwrap();
        assert_eq!(difference["changedFiles"], json!(["src/a.txt"]));
        assert_eq!(difference["addedFiles"], json!(["src/new.txt"]));
        let list = json!({"action":"list"});
        assert!(execute_with_lease(&lease, "checkpoint", &list)
            .unwrap()
            .contains(id));
        for input in [&diff, &list] {
            assert!(lease
                .describe_effect("checkpoint", input)
                .scopes
                .iter()
                .filter(|scope| scope.resource == PermissionResource::File)
                .all(|scope| scope.operation == PermissionOperation::Read));
        }
        let restore = json!({"action":"restore","id":id});
        let effect = lease.describe_effect("checkpoint", &restore);
        for operation in [PermissionOperation::Read, PermissionOperation::Write] {
            assert!(effect
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File
                    && scope.operation == operation
                    && scope.target.is_none()));
        }
        assert!(effect
            .scopes
            .iter()
            .any(|scope| scope.resource == PermissionResource::Tool));
        execute_with_lease(&lease, "checkpoint", &restore).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("src/a.txt")).unwrap(),
            "before"
        );
        assert!(!root.join("src/new.txt").exists());
        assert_eq!(
            std::fs::read_to_string(root.join("outside.txt")).unwrap(),
            "retained"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn original_settings_and_todo_paths_are_derived_from_the_executable_owners() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        let root = std::env::temp_dir().join(format!(
            "cowd-original-settings-owners-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let lease = crate::ToolHost::builtin("original-settings-owners", &root).pin_snapshot();
        let read = lease.describe_effect("config", &json!({"setting":"model"}));
        assert!(read
            .scopes
            .iter()
            .filter(|scope| scope.resource == PermissionResource::File)
            .all(|scope| scope.operation == PermissionOperation::Read));
        execute_with_lease(
            &lease,
            "config",
            &json!({"setting":"model","value":"fixture-model"}),
        )
        .unwrap();
        assert!(
            std::fs::read_to_string(root.join(".cowd/config.local.yaml"))
                .unwrap()
                .contains("fixture-model")
        );
        let plan = json!({"action":"enter"});
        let effect = lease.describe_effect("plan_mode", &plan);
        for target in [
            "./.cowd/config.local.yaml",
            "./.cowd/tool-state/plan-mode.json",
        ] {
            for operation in [PermissionOperation::Read, PermissionOperation::Write] {
                assert!(effect
                    .scopes
                    .iter()
                    .any(|scope| scope.resource == PermissionResource::File
                        && scope.operation == operation
                        && scope.target.as_deref() == Some(target)));
            }
        }
        execute_with_lease(&lease, "plan_mode", &plan).unwrap();
        assert!(root.join(".cowd/tool-state/plan-mode.json").is_file());
        execute_with_lease(&lease, "plan_mode", &json!({"action":"exit"})).unwrap();
        assert!(!root.join(".cowd/tool-state/plan-mode.json").exists());
        let target = super::super::todo_store_path(std::path::Path::new("."))
            .to_string_lossy()
            .into_owned();
        let todo = lease.describe_effect("todo_write", &json!({"path":"decoy","todos":[]}));
        for operation in [PermissionOperation::Read, PermissionOperation::Write] {
            assert!(todo
                .scopes
                .iter()
                .any(|scope| scope.resource == PermissionResource::File
                    && scope.operation == operation
                    && scope.target.as_deref() == Some(target.as_str())));
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn shared_preflight_preserves_all_original_operations_occurrences_and_reference_rules() {
        let program = json!({"steps":[
            {"id":"first","op":"call","tool":"edit","input":{"action":"write","path":"a","content":"$prior.text"},"depends_on":["ignored"]},
            {"id":"batch","op":"batch","on_error":"collect","calls":[
                {"tool":"edit","as":"one","input":{"action":"write","path":"b","content":"literal"}},
                {"tool":"edit","as":"two","input":{"action":"write","path":"c","content":"literal"}}
            ]},
            {"id":"map","op":"map","over":"$prior.list","tool":"read_file","input":{"path":"$item.path"},"field":"file"},
            {"id":"filter","op":"filter","over":"$map","field":"x","equals":true},
            {"id":"collect","op":"collect","over":"$filter","agg":"count"},
            {"id":"compute","op":"compute","language":"python","code":"$this_is_literal_source","input":{"nested":["$collect"]}},
            {"id":"when","op":"when","if":{"ref":"$collect","equals":0},"tool":"web_search","input":{"query":"$prior.query"}},
            {"id":"repeat","op":"repeat","max":0,"while":{"ref":"$collect","equals":0},"tool":"edit","input":{"action":"write","path":"never","content":"literal"}}
        ],"return":"$collect","max_concurrency":2});
        let calls = effect_preflight(&program).unwrap();
        assert_eq!(calls.len(), 7);
        assert_eq!(
            calls.iter().map(|call| call.operation).collect::<Vec<_>>(),
            vec!["call", "batch", "batch", "map", "compute", "when", "repeat"]
        );
        assert_eq!(calls[0].references, vec![vec!["content".to_string()]]);
        assert_eq!(calls[1].input["path"], "b");
        assert_eq!(calls[2].input["path"], "c");
        assert_eq!(calls[3].references, vec![vec!["path".to_string()]]);
        assert_eq!(
            calls[4].references,
            vec![vec![
                "input".to_string(),
                "nested".to_string(),
                "0".to_string()
            ]]
        );
        assert_eq!(calls[6].maximum_calls, 0);
        assert_eq!(
            requested_sub_call_tools("tool_orchestrate", &program),
            SubCallAuthorization::Requires(vec![
                "edit".into(),
                "execute_code".into(),
                "read_file".into(),
                "web_search".into()
            ])
        );
        let constants = json!({"steps":[{"id":"literal","op":"call","tool":"edit","input":{"action":"write","path":"$HOME/x","content":["$(date)", "$$item", {"$item.key":"literal"}]}}]});
        assert!(effect_preflight(&constants).unwrap()[0]
            .references
            .is_empty());
    }

    #[test]
    fn shared_preflight_checks_declared_bounds_and_original_dispatch_vocabulary() {
        let call = json!({"id":"read","op":"call","tool":"read_file","input":{"path":"a"}});
        assert!(effect_preflight(&json!({"steps":vec![call.clone(); MAX_STEPS]})).is_ok());
        assert!(effect_preflight(&json!({"steps":vec![call; MAX_STEPS + 1]})).is_err());
        let entry = json!({"tool":"read_file","input":{"path":"a"}});
        for count in [MAX_MAP_ITEMS, MAX_MAP_ITEMS + 1] {
            let result = effect_preflight(
                &json!({"steps":[{"id":"batch","op":"batch","calls":vec![entry.clone();count]}]}),
            );
            assert_eq!(result.is_ok(), count == MAX_MAP_ITEMS);
        }
        let batch = json!({"id":"batch","op":"batch","calls":vec![entry;MAX_MAP_ITEMS]});
        assert!(effect_preflight(&json!({"steps":[batch.clone(), batch.clone()]})).is_ok());
        assert!(effect_preflight(&json!({"steps":[batch.clone(), batch.clone(), batch]})).is_err());
        for max in [MAX_REPEAT, MAX_REPEAT + 1] {
            let result = effect_preflight(
                &json!({"steps":[{"id":"repeat","op":"repeat","max":max,"while":{"ref":"$r","present":true},"tool":"read_file","input":{"path":"a"}}]}),
            );
            assert_eq!(result.is_ok(), max == MAX_REPEAT);
        }
        for tool in crate::mvp_tool_specs() {
            let program =
                json!({"steps":[{"id":"literal","op":"call","tool":tool.name,"input":{}}]});
            assert_eq!(
                effect_preflight(&program).is_ok(),
                tool.name != "tool_orchestrate",
                "{}",
                tool.name
            );
        }
        for program in [
            json!({"steps":[],"invented":true}),
            json!({"steps":[{"id":"bad","op":"unknown"}]}),
            json!({"steps":[{"id":"bad","op":"compute","language":"bash","code":"pwd"}]}),
            json!({"steps":[{"id":"bad","op":"call","tool":"web-fetch","input":{}}]}),
            json!({"steps":[{"id":"bad","op":"call","tool":"tool_orchestrate","input":{"steps":[]}}]}),
        ] {
            assert!(effect_preflight(&program).is_err());
        }
    }

    #[test]
    fn dynamic_cardinality_keeps_original_runtime_limits_before_dispatch() {
        let lease = crate::ToolHost::builtin("map-bounds", std::env::temp_dir()).pin_snapshot();
        let mut state = State::new();
        state.insert("items".into(), json!(vec!["a"; MAX_MAP_ITEMS + 1]));
        let map: StepKind = serde_json::from_value(
            json!({"op":"map","over":"$items","tool":"read_file","input":{"path":"$item"}}),
        )
        .unwrap();
        let mut calls = 0;
        assert!(evaluate(&lease, 0, "map", &map, &state, &mut calls, None)
            .unwrap_err()
            .contains("over the limit"));
        assert_eq!(calls, 0);
        state.insert("items".into(), json!(["a", "b"]));
        calls = MAX_TOTAL_CALLS - 1;
        assert!(evaluate(&lease, 0, "map", &map, &state, &mut calls, None)
            .unwrap_err()
            .contains("total sub-call limit"));
        assert_eq!(calls, MAX_TOTAL_CALLS - 1);
        calls = MAX_TOTAL_CALLS;
        assert!(bump(&mut calls).is_err());
        let conditional = json!({"id":"r","op":"repeat","max":MAX_REPEAT,"while":{"ref":"$items","present":false},"tool":"read_file","input":{"path":"a"}});
        assert!(effect_preflight(&json!({"steps":vec![conditional; MAX_STEPS]})).is_ok());
    }
}
