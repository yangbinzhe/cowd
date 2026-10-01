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
    let parsed: OrchestrateInput = crate::executor::from_value(input)?;
    if parsed.steps.len() > MAX_STEPS {
        return Err(format!(
            "tool_orchestrate supports at most {MAX_STEPS} steps; got {}",
            parsed.steps.len()
        ));
    }
    let mut state: State = BTreeMap::new();
    let mut total_calls = 0usize;
    let mut calls_trace: Vec<Value> = Vec::new();
    for step in &parsed.steps {
        let before = total_calls;
        let started = std::time::Instant::now();
        let (op, tool) = step_descriptor(&step.kind);
        match evaluate(
            lease,
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
    let parsed: OrchestrateInput = match serde_json::from_value(input.clone()) {
        Ok(parsed) => parsed,
        Err(error) => {
            return SubCallAuthorization::Invalid(format!(
                "tool_orchestrate input is invalid: {error}"
            ))
        }
    };
    let mut names = Vec::new();
    for step in &parsed.steps {
        match &step.kind {
            StepKind::Call { tool, .. }
            | StepKind::Map { tool, .. }
            | StepKind::When { tool, .. }
            | StepKind::Repeat { tool, .. } => names.push(tool.clone()),
            StepKind::Batch { calls, .. } => {
                names.extend(calls.iter().map(|call| call.tool.clone()));
            }
            StepKind::Compute { .. } => names.push("execute_code".to_string()),
            StepKind::Filter { .. } | StepKind::Collect { .. } => {}
        }
    }
    if let Some(bad) = names.iter().find(|name| name.contains('$')) {
        return SubCallAuthorization::Invalid(format!(
            "orchestrated tool name `{bad}` must be a literal, not a $reference"
        ));
    }
    names.sort();
    names.dedup();
    SubCallAuthorization::Requires(names)
}

fn evaluate(
    lease: &ToolHostLease,
    kind: &StepKind,
    state: &State,
    total_calls: &mut usize,
    max_concurrency: Option<usize>,
) -> Result<Value, String> {
    match kind {
        StepKind::Call { tool, input } => {
            let resolved = resolve_value(input, state, None)?;
            call_tool(lease, tool, &resolved, total_calls)
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
            for call in calls {
                prepared.push((
                    call.alias.clone(),
                    call.tool.clone(),
                    resolve_value(&call.input, state, None)?,
                ));
            }
            let items = prepared.clone();
            let results = crate::executor::run_ordered_batch(prepared, max_concurrency, |call| {
                call_tool_uncounted(lease, &call.1, &call.2)
            });
            *total_calls += items.len();
            let collect = matches!(on_error, Some(OnError::Collect));
            let mut object = Map::new();
            for (index, result) in results.into_iter().enumerate() {
                let (alias, tool, _) = &items[index];
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
            let results = crate::executor::run_ordered_batch(items.clone(), max_concurrency, |item| {
                let resolved = resolve_value(input, state, Some(&item))?;
                call_tool_uncounted(lease, tool, &resolved)
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
                call_tool(lease, tool, &resolved, total_calls)
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
            for _ in 0..*max {
                if !eval_condition(condition, state, None)? {
                    break;
                }
                let resolved = resolve_value(input, state, None)?;
                last = call_tool(lease, tool, &resolved, total_calls)?;
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
    tool: &str,
    input: &Value,
    total_calls: &mut usize,
) -> Result<Value, String> {
    bump(total_calls)?;
    call_tool_uncounted(lease, tool, input)
}

/// Dispatch one allowed sub-call without touching the shared call counter, so
/// the `map` step can fan out in parallel and count once.
fn call_tool_uncounted(lease: &ToolHostLease, tool: &str, input: &Value) -> Result<Value, String> {
    if !is_allowed_orchestration_tool(tool) {
        return Err(format!(
            "tool_orchestrate cannot dispatch `{tool}`: not an allowed orchestration tool"
        ));
    }
    let output = execute_with_lease(lease, tool, input)?;
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
        language,
        &source,
        None,
        lease.workspace_root(),
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
    match value {
        Value::String(text) if is_reference(text) => resolve_ref(text, state, item),
        Value::Array(items) => items
            .iter()
            .map(|entry| resolve_value(entry, state, item))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(entries) => entries
            .iter()
            .map(|(key, entry)| Ok((key.clone(), resolve_value(entry, state, item)?)))
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
}
