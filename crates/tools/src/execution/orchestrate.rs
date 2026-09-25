//! Declarative read-only orchestration (`tool_orchestrate`).
//!
//! One model call can express a small bounded program over the workspace:
//! sequential calls, a `map` over a result list, `filter`/`collect`
//! aggregation, a bounded `repeat`, a `when` branch, and a `compute` step that
//! runs `execute_code` on data. Data flows between steps through `$`-references;
//! no general expression language is needed, which keeps it auditable and
//! friendly to weaker models.
//!
//! Safety boundary: every sub-call is restricted to the curated read-only batch
//! allow-list (`is_allowed_readonly_batch_tool`). `tool_orchestrate` therefore
//! cannot widen the model's authority: it only composes tools the model can
//! already call, and never mutates the workspace. `compute` reuses the existing
//! kernel-hardened, read-only, network-off `execute_code` sandbox.

use std::collections::BTreeMap;

use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::executor::{execute_with_lease, is_allowed_readonly_batch_tool};
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
    Map {
        over: String,
        tool: String,
        #[serde(default)]
        input: Value,
        #[serde(default)]
        field: Option<String>,
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
    for step in &parsed.steps {
        let value = evaluate(lease, &step.kind, &state, &mut total_calls)?;
        state.insert(step.id.clone(), value);
    }
    let result = match parsed.ret.as_deref() {
        Some(reference) => resolve_ref(reference, &state, None)?,
        None => Value::Object(state.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
    };
    serde_json::to_string_pretty(&json!({ "steps": state, "result": result }))
        .map_err(|error| error.to_string())
}

fn evaluate(
    lease: &ToolHostLease,
    kind: &StepKind,
    state: &State,
    total_calls: &mut usize,
) -> Result<Value, String> {
    match kind {
        StepKind::Call { tool, input } => {
            let resolved = resolve_value(input, state, None)?;
            call_tool(lease, tool, &resolved, total_calls)
        }
        StepKind::Map {
            over,
            tool,
            input,
            field,
        } => {
            let list = resolve_ref(over, state, None)?;
            let items = list
                .as_array()
                .ok_or_else(|| format!("map.over `{over}` must resolve to an array"))?;
            if items.len() > MAX_MAP_ITEMS {
                return Err(format!(
                    "map.over has {} items, over the limit of {MAX_MAP_ITEMS}",
                    items.len()
                ));
            }
            let mut outputs = Vec::with_capacity(items.len());
            for item in items {
                let resolved = resolve_value(input, state, Some(item))?;
                let output = call_tool(lease, tool, &resolved, total_calls)?;
                outputs.push(select_field(output, field.as_deref()));
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
    if !is_allowed_readonly_batch_tool(tool) {
        return Err(format!(
            "tool_orchestrate is read-only: `{tool}` is not an allowed query tool"
        ));
    }
    bump(total_calls)?;
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
            let separator = separator.unwrap_or("\n");
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
}
