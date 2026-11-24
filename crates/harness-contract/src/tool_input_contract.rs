//! One deterministic translation from a tool's JSON schema to the compact form a
//! model can actually read.
//!
//! A schema is the single source of truth for what a tool accepts, but it is not
//! written for model eyes: an internally-tagged enum spreads one tool across a
//! `oneOf` of variants, a field's legal values may hide behind a `$ref` into
//! `$defs`, and a bare `string` says nothing about the Runtime reference it is
//! meant to carry. One live run spent seventeen rejected `artifact_publish`
//! calls and eleven invented input shapes discovering that the hard way, because
//! nothing said what `source` could be.
//!
//! This module derives the missing half from the schema itself, so no tool name
//! or field name is ever written down twice, and adding a variant or a tool
//! needs no edit here.

use serde_json::{Map, Value};

/// A short correction only when this exact schema proves the statement.
#[must_use]
pub fn input_shape_correction(tool: &str, reason: &str, schema: &Value) -> Option<&'static str> {
    let reason = reason.split_once("pinned schema: ").map_or(reason, |(_, bare)| bare);
    // Local level 9 (Qwen3.8-Flash-Next, 2026-11-24): task_submit refused twice for
    // `$: unexpected field `reason``. Read before the closed-schema guards below, which
    // describe task_review's shape and would otherwise reject a task_submit schema.
    if tool == "task_submit" && reason == "$: unexpected field `reason`" {
        return Some(
            "task_submit takes no `reason`; a submission is the deliverable plus its evidence_refs.",
        );
    }
    // Local level 9 (Qwen3.8-Flash-Next, 2026-11-24): state_inspect refused once for
    // `$: unexpected field `limit`` - StateInspectInput pages with `page_cursor` only.
    if tool == "state_inspect" && reason == "$: unexpected field `limit`" {
        return Some(
            "state_inspect has no `limit`; it pages with `page_cursor` taken from the previous result.",
        );
    }
    if tool == "membership_update" && reason == "$: unexpected field `reason`"
        && ["$ref", "oneOf", "anyOf", "allOf"].iter().all(|key| schema.get(*key).is_none())
        && schema["additionalProperties"] == false && schema["properties"].get("reason").is_none()
        && schema["properties"].get("reason_ref").is_some()
    {
        return Some("membership_update does not accept `reason`; its schema lists `reason_ref`. Do not automatically convert reason text; supply a value that meets that field's contract if needed.");
    }
    if tool == "tool_orchestrate" && reason == "$: unexpected field `id`"
        && ["$ref", "oneOf", "anyOf", "allOf"].iter().all(|key| schema.get(*key).is_none())
        && schema["additionalProperties"] == false
        && schema["properties"].get("id").is_none()
        && schema["properties"]["steps"]["type"] == "array"
        && schema["properties"]["steps"]["items"]["type"] == "object"
        && ["$ref", "oneOf", "anyOf", "allOf"].iter()
            .all(|key| schema["properties"]["steps"]["items"].get(*key).is_none())
        && schema["properties"]["steps"]["items"]["properties"]["id"]["type"] == "string"
        && schema["properties"]["steps"]["items"]["required"].as_array()
            .is_some_and(|fields| fields.iter().any(|field| field == "id"))
    {
        return Some("tool_orchestrate has no top-level `id`; each step requires its own string `id` in `steps`. Choose the step IDs explicitly.");
    }
    if tool != "task_review"
        || ["$ref", "oneOf", "anyOf", "allOf"].iter().any(|key| schema.get(*key).is_some())
        || !schema["required"].as_array().is_some_and(|fields| fields.iter().any(|field| field == "reason"))
        || schema["properties"]["reason"]["type"] != "string"
    {
        return None;
    }
    match reason {
        "$: missing required field `reason`" if tool == "task_review" => Some("task_review requires a string `reason`; supply your own reason text."),
        // Local level 9 (Qwen3.8-Flash-Next, 2026-11-24): task_submit refused twice for
        // `$: unexpected field `reason`` - handled above, before the closed-schema guards.
        "$: unexpected field `reason_ref`"
            if schema["additionalProperties"] == false && schema["properties"].get("reason_ref").is_none() =>
            Some("task_review requires a string `reason`; `reason_ref` is not a task_review field. Supply your own reason text."),
        _ => None,
    }
}

/// Report other known unexpected root fields without modifying the caller's input.
#[must_use]
pub fn orchestrate_extra_root_fields(tool: &str, reason: &str, schema: &Value, input: &Value) -> Option<String> {
    if tool != "tool_orchestrate" || reason != "$: unexpected field `id`"
        || input_shape_correction(tool, reason, schema).is_none()
        || schema.get("patternProperties").is_some()
    {
        return None;
    }
    let properties = schema.get("properties")?.as_object()?;
    let object = input.as_object()?;
    let extras: Vec<_> = ["op", "item"].into_iter()
        .filter(|field| object.contains_key(*field) && !properties.contains_key(*field))
        .map(|field| format!("`{field}`"))
        .collect();
    if extras.is_empty() { return None; }
    Some(format!("Also unsupported at the top level of this schema: {}. Choose the intended step structure explicitly and validate the complete input.", extras.join(", ")))
}

/// A `$ref` chain longer than this is treated as absent rather than followed, so
/// a self-referential schema cannot spin here.
const MAX_REF_DEPTH: usize = 5;

/// Field names carrying this suffix are Runtime references. The schema types them
/// as a bare `string` because a reference is whatever object it names, so the
/// only honest guidance is where the value comes from - never a made-up format.
const REF_SUFFIX: &str = "_ref";

/// Longest description fragment carried into one contract line. Long enough for
/// the operative sentence, short enough that a wide struct cannot flood the
/// prompt. A truncation is always announced.
const DESCRIPTION_MAX_CHARS: usize = 200;

const REF_HINT: &str = "canonical Runtime ref; the receipt that returned the object \
                         carries the exact value; verify one with `read_ref`";

/// Longest example rendered for one tool. Past this an example stops being
/// something a model copies and becomes something it skims.
const EXAMPLE_MAX_CHARS: usize = 600;

/// The key that marks an example that could not be rendered as a copyable
/// instance.
///
/// An example that quietly omits a required field is worse than no example: a
/// model copies it, the copy is refused for the missing field, and the round
/// teaches nothing. A degraded example says so by name and lists the fields
/// instead of showing a half-call.
pub const DEGRADED_EXAMPLE_KEY: &str = "__example_degraded__";

/// Why an example was degraded rather than rendered.
pub const DEGRADED_REASON: &str =
    "the whole instance would not fit the example budget; every field is named below instead";

/// One field of a degraded example: where it is, what it holds, and whether the
/// schema demands it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegradedField {
    /// Dotted path from the tool's input root.
    pub name: String,
    /// The field's shape in words (`string`, `array of string`, `const ...`).
    pub type_name: String,
    /// Whether the schema requires this field.
    pub required: bool,
}

/// A named degradation: the example could not be rendered whole, so it is
/// replaced by the field list a model can still read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegradedExample {
    /// Why the instance was not rendered.
    pub reason: String,
    /// Every field the instance would have carried.
    pub fields: Vec<DegradedField>,
}

/// How one tool's example was rendered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExampleRender {
    /// A complete instance a model can copy.
    Complete(Value),
    /// The instance could not be shown whole; its fields are named instead.
    Degraded(DegradedExample),
}

/// Beyond this many `oneOf` variants only the first is examplified and the rest
/// are pointed at the contract lines: seven examples is seven that nobody reads.
const MAX_EXAMPLIFIED_VARIANTS: usize = 4;

/// A value a model recognizes as a placeholder to replace, never as content to
/// repeat.
fn placeholder(field: &str) -> Value {
    Value::String(format!("<{field}>"))
}

/// Minimal instance a model can copy, derived only from `schema`.
///
/// The contract lines say what a tool accepts. They do not say what a correct
/// call *looks like*, and a model that has to synthesize one from a field list
/// eventually drops the wrapper while keeping the content: one live run emitted
/// `{"actions": "[{\"input\": {...}}]"}` four times - every mission and
/// acceptance was present, the `{tool, input}` envelope never was - and being
/// told "each entry must be {tool, input}" did not fix it.
///
/// Both codex and opencode solve this the same way for their patch tools: a
/// complete literal example in the contract. This generates one from the schema
/// so it cannot drift from the contract it illustrates.
///
/// Nothing here guesses meaning. Every value is a recognizable placeholder, an
/// enum's first legal value, or an empty container of the declared type - so the
/// example is always shape-correct and field-complete, and never mistaken for
/// content.
#[must_use]
pub fn input_example(schema: &Value) -> Option<Value> {
    match render_example(schema) {
        ExampleRender::Complete(value) => {
            // A schema that declares nothing to fill in has nothing to
            // exemplify; an example of `{}` is noise.
            let is_empty = value.as_object().is_some_and(Map::is_empty);
            (!is_empty).then_some(value)
        }
        ExampleRender::Degraded(degraded) => Some(degraded_marker(&degraded)),
    }
}

/// Render one tool's example, naming the degradation when the instance cannot
/// be shown whole.
///
/// The budget used to end the rendering silently: a field the example meant to
/// carry was simply left out, and a model that copied the result was refused
/// for the field the framework had dropped. A degradation is the honest
/// alternative - the instance is not shown, and every field is named instead.
#[must_use]
pub fn render_example(schema: &Value) -> ExampleRender {
    let defs = schema.get("$defs").and_then(Value::as_object);
    let mut state = RenderState {
        rendered: 0,
        lost: Vec::new(),
        path_form: false,
    };
    let example = example_for(schema, defs, 0, &mut state, "value");
    if state.lost.is_empty() {
        return ExampleRender::Complete(
            example.unwrap_or_else(|| Value::Object(Map::new())),
        );
    }
    ExampleRender::Degraded(DegradedExample {
        reason: DEGRADED_REASON.to_string(),
        fields: degraded_fields(schema, defs),
    })
}

/// The legal form of one field, derived from the same schema the whole example
/// comes from.
///
/// The field the model got wrong is the one it needs to see. An optional or
/// chosen field never appears in a whole-tool example, so a refusal that shows
/// only the whole example teaches the model to send the same call again.
#[must_use]
pub fn example_for_path(schema: &Value, path: &str) -> Option<Value> {
    let defs = schema.get("$defs").and_then(Value::as_object);
    let segments = path_segments(path);
    let node = schema_at_path(schema, defs, &segments)?;
    let field_name = segments.last().map(String::as_str).unwrap_or("value");
    let mut state = RenderState {
        rendered: 0,
        lost: Vec::new(),
        path_form: true,
    };
    example_for(node, defs, 0, &mut state, field_name)
}

/// The degraded form of one example, if that is what it is.
#[must_use]
pub fn degraded_example(value: &Value) -> Option<DegradedExample> {
    let marker = value.get(DEGRADED_EXAMPLE_KEY)?;
    let fields = marker
        .get("fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(|field| {
                    Some(DegradedField {
                        name: field.get("name")?.as_str()?.to_string(),
                        type_name: field.get("type")?.as_str()?.to_string(),
                        required: field
                            .get("required")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Some(DegradedExample {
        reason: marker
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| DEGRADED_REASON.to_string()),
        fields,
    })
}

/// The model-facing text of a degraded example.
#[must_use]
pub fn degraded_example_text(degraded: &DegradedExample) -> String {
    let fields = degraded
        .fields
        .iter()
        .map(|field| {
            let requirement = if field.required { "required" } else { "optional" };
            format!("{}: {} ({requirement})", field.name, field.type_name)
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!("Example degraded - {}. Fields: {fields}.", degraded.reason)
}

/// The floor of real instances a tool surface's examples must carry.
///
/// The floor counts **instances only**. A tool whose example degraded is not
/// covered by an instance - it is covered by a field list - so it never widens
/// this window. The window is the pre-degradation rule, unchanged.
pub const INSTANCE_COVERAGE_FLOOR: usize = 20;

/// How many degradations the instance count may carry.
///
/// The degraded branch is a bounded side branch, not a second window: four
/// degradations per instance is the most a surface may show before the
/// degradation stops being an edge case.
pub const DEGRADED_BRANCH_RATIO: usize = 4;

/// How much of a tool surface's examples were real instances, and how much
/// degraded into a field list.
///
/// The two counts are kept apart on purpose. Folding them together is what
/// let a surface lose real instances and still pass the coverage window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExampleCoverage {
    /// Tools whose example is a real instance.
    pub instances: usize,
    /// Tools whose example degraded into a named field list.
    pub degraded: usize,
}

impl ExampleCoverage {
    /// Whether the instance window holds.
    ///
    /// Degradations never enter this test: the window says how much of the
    /// surface is covered by examples a model can copy.
    #[must_use]
    pub fn instance_window_holds(self) -> bool {
        self.instances > INSTANCE_COVERAGE_FLOOR
    }

    /// Whether the degraded branch stays inside its own bound.
    #[must_use]
    pub fn degraded_branch_holds(self) -> bool {
        self.degraded.saturating_mul(DEGRADED_BRANCH_RATIO) <= self.instances
    }
}

/// The schema node a field really means.
///
/// A derived schema answers with a `$ref` into the definitions, and a choice of
/// named variants answers with a `$ref` inside its first branch. A repair that
/// reads the node as it was written therefore reads **no type and no members**
/// on exactly the fields that need repairing: the one-shot deliverable block
/// is a named struct, and its quote-corrupted form is what the shape gate
/// refused over and over.
#[must_use]
pub fn resolve_schema_node<'a>(root: &'a Value, node: &'a Value) -> &'a Value {
    let defs = root.get("$defs").and_then(Value::as_object);
    let mut current = follow_ref(node, defs);
    for _ in 0..MAX_REF_DEPTH {
        let mut next = None;
        for keyword in ["oneOf", "anyOf"] {
            if let Some(variants) = current.get(keyword).and_then(Value::as_array) {
                if let Some(first) = variants.first() {
                    let resolved = follow_ref(first, defs);
                    if !std::ptr::eq(resolved, current) {
                        next = Some(resolved);
                    }
                }
            }
        }
        match next {
            Some(resolved) => current = resolved,
            None => break,
        }
    }
    current
}

/// The legal spelling of one literal, when the value names it in another case.
///
/// A schema that closes a field to a fixed set of values is the authority on
/// what those values are. When the model's value differs from a legal member
/// **only in case**, and exactly one member matches that way, the member is the
/// value the model meant: the shape is the same shape, spelled the way the
/// contract spells it.
///
/// Two members that differ only in case are two members, not one - then the
/// value names nothing and nothing is repaired.
#[must_use]
pub fn normalize_schema_literal(schema_node: &Value, value: &Value) -> Option<Value> {
    let serde_json::Value::String(text) = value else {
        return None;
    };
    let members: Vec<&Value> = match schema_node.get("enum").and_then(Value::as_array) {
        Some(values) => values.iter().collect(),
        None => match schema_node.get("const") {
            Some(one) => vec![one],
            None => return None,
        },
    };
    let matching = members
        .iter()
        .filter(|member| {
            member
                .as_str()
                .is_some_and(|member| member.eq_ignore_ascii_case(text))
        })
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [only] if only.as_str() != Some(text.as_str()) => Some((**only).clone()),
        _ => None,
    }
}

/// How much of the example budget is spent, and which fields the budget cost.
struct RenderState {
    rendered: usize,
    lost: Vec<String>,
    /// The example is for one violated path (not the whole call).
    path_form: bool,
}

fn degraded_marker(degraded: &DegradedExample) -> Value {
    let fields = degraded
        .fields
        .iter()
        .map(|field| {
            serde_json::json!({
                "name": field.name,
                "type": field.type_name,
                "required": field.required,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        DEGRADED_EXAMPLE_KEY: {
            "reason": degraded.reason,
            "fields": fields,
        }
    })
}

fn example_for(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
    depth: usize,
    state: &mut RenderState,
    field_name: &str,
) -> Option<Value> {
    if depth > MAX_REF_DEPTH || state.rendered > EXAMPLE_MAX_CHARS {
        return None;
    }
    // The module's existing resolver, reused rather than reimplemented: a second
    // `$ref` walker is a second thing that can drift.
    let schema = follow_ref(schema, defs);
    if let Some(one) = schema.get("const") {
        return Some(one.clone());
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        // The first legal value is the one a model can copy without choosing.
        return values.first().cloned();
    }
    // A tagged enum is the case this function exists for: the model's difficulty
    // is picking the variant, so each variant gets its own example.
    for keyword in ["oneOf", "anyOf"] {
        let Some(variants) = schema.get(keyword).and_then(Value::as_array) else {
            continue;
        };
        // ONE valid instance, never a list of them. An array of examples is not
        // itself a legal input, and a model that copies it is refused for a shape
        // the framework handed it - which is the exact loop this ends. The other
        // variants are already named by `input_contract_summary`, which lists
        // every variant's required fields; the example's job is to show one shape
        // that validates, not to enumerate the choice.
        for variant in variants.iter().take(MAX_EXAMPLIFIED_VARIANTS) {
            let resolved = follow_ref(variant, defs);
            if let Some(example) = example_for(resolved, defs, depth + 1, state, field_name) {
                return Some(example);
            }
        }
    }
    // `Option<T>` is emitted as `"type": ["string", "null"]`, not as a bare
    // string. Reading only the string form sent every optional field down the
    // object branch, which produced `{}` - an example that fails its own schema.
    // Take the first non-null member, the same reading `validate_schema_value`
    // uses.
    let declared_type = match schema.get("type") {
        Some(Value::String(one)) => Some(one.clone()),
        Some(Value::Array(choices)) => choices
            .iter()
            .filter_map(Value::as_str)
            .find(|choice| *choice != "null")
            .map(str::to_string),
        _ => None,
    };
    match declared_type.as_deref() {
        Some("string") => {
            let example = placeholder(field_name);
            state.rendered += example.as_str().map_or(0, str::len);
            Some(example)
        }
        Some("integer") | Some("number") => Some(Value::from(0)),
        Some("boolean") => Some(Value::Bool(true)),
        Some("array") => match schema.get("items") {
            // One element shows the element's shape; an empty array shows none.
            Some(items) => {
                match example_for(items, defs, depth + 1, state, field_name) {
                    Some(item) => Some(Value::Array(vec![item])),
                    None => Some(Value::Array(Vec::new())),
                }
            }
            None => Some(Value::Array(Vec::new())),
        },
        _ => Some(object_example(schema, defs, depth, state)),
    }
}

fn object_example(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
    depth: usize,
    state: &mut RenderState,
) -> Value {
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if properties.is_empty() {
        return Value::Object(Map::new());
    }
    let required = required_names(schema);
    // A tool with nothing mandatory has nothing an example must show. Filling in
    // every optional field instead would teach a model to send what it may omit -
    // `state_inspect` would be exemplified with all six of its optional filters,
    // when the common correct call is `{}`.
    // A violated nested path whose object carries a default on every field has no
    // mandatory field, and `{}` would be shown for it: the model cannot repair a type
    // violation (live: `block_index: "0"` against `integer`) against an empty object.
    // The path form therefore exemplifies the non-nullable fields instead.
    let exemplify_non_nullable = required.is_empty() && state.path_form;
    if required.is_empty() && !exemplify_non_nullable {
        return Value::Object(Map::new());
    }
    let mut example = Map::new();
    for (field, field_schema) in &properties {
        // Required fields are the ones whose absence is a refusal. An optional
        // field in the example would teach a model to send what it may omit -
        // unless it is the discriminator: a tagged enum's `source`/`kind` is
        // pinned to one constant and is semantically mandatory even when the
        // `required` list omits it, and an example without it shows the model a
        // variant it cannot actually send.
        let is_discriminator = field_schema.get("const").is_some();
        let non_nullable = !matches!(field_schema.get("type"), Some(Value::Array(choices)) if choices.iter().any(|choice| choice == "null"));
        if !required.iter().any(|name| name == field) && !is_discriminator && !(exemplify_non_nullable && non_nullable) {
            continue;
        }
        match example_for(field_schema, defs, depth + 1, state, field) {
            Some(value) => {
                example.insert(field.clone(), value);
            }
            None => {
                // The instance would be partial, and a partial instance is a
                // refusal waiting to happen. The loss is recorded so the whole
                // example degrades into a field list instead.
                state.lost.push(field.clone());
            }
        }
    }
    Value::Object(example)
}

/// Every field the instance would have carried, with its shape.
fn degraded_fields(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
) -> Vec<DegradedField> {
    let mut fields = Vec::new();
    collect_degraded_fields(schema, defs, "", 0, &mut fields);
    fields
}

fn collect_degraded_fields(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
    prefix: &str,
    depth: usize,
    out: &mut Vec<DegradedField>,
) {
    if depth > MAX_REF_DEPTH {
        return;
    }
    let schema = follow_ref(schema, defs);
    // A choice: the instance would have shown the first variant, so the field
    // list names that variant's fields.
    for keyword in ["oneOf", "anyOf"] {
        if let Some(variants) = schema.get(keyword).and_then(Value::as_array) {
            if let Some(first) = variants.first() {
                collect_degraded_fields(follow_ref(first, defs), defs, prefix, depth + 1, out);
            }
            return;
        }
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    let required = required_names(schema);
    for (field, field_schema) in properties {
        let is_discriminator = field_schema.get("const").is_some();
        let is_required = required.iter().any(|name| name == field);
        if !is_required && !is_discriminator {
            continue;
        }
        let path = if prefix.is_empty() {
            field.clone()
        } else {
            format!("{prefix}.{field}")
        };
        out.push(DegradedField {
            name: path.clone(),
            type_name: type_name_of(field_schema, defs),
            required: is_required,
        });
        // A nested object's own required fields travel with their parent.
        let resolved = follow_ref(field_schema, defs);
        if resolved.get("properties").is_some() {
            collect_degraded_fields(resolved, defs, &path, depth + 1, out);
        } else if let Some(items) = resolved.get("items") {
            if follow_ref(items, defs).get("properties").is_some() {
                collect_degraded_fields(items, defs, &path, depth + 1, out);
            }
        }
    }
}

/// A field's shape in the fewest words a model can act on.
fn type_name_of(schema: &Value, defs: Option<&Map<String, Value>>) -> String {
    let schema = follow_ref(schema, defs);
    if let Some(one) = schema.get("const") {
        return format!("const {one}");
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        let listed = values
            .iter()
            .take(4)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(" | ");
        return format!("enum {listed}");
    }
    for keyword in ["oneOf", "anyOf"] {
        if let Some(variants) = schema.get(keyword).and_then(Value::as_array) {
            let tags = variants
                .iter()
                .take(4)
                .filter_map(|variant| {
                    let resolved = follow_ref(variant, defs);
                    let properties = resolved.get("properties").and_then(Value::as_object)?;
                    discriminator(properties, defs).map(|(_, value)| value)
                })
                .collect::<Vec<_>>();
            return if tags.is_empty() {
                "one of".to_string()
            } else {
                format!("one of {}", tags.join(" | "))
            };
        }
    }
    match schema.get("type") {
        Some(Value::String(one)) if one == "array" => match schema.get("items") {
            Some(items) => format!("array of {}", type_name_of(items, defs)),
            None => "array".to_string(),
        },
        Some(Value::String(one)) => one.clone(),
        Some(Value::Array(choices)) => choices
            .iter()
            .filter_map(Value::as_str)
            .find(|choice| *choice != "null")
            .unwrap_or("value")
            .to_string(),
        _ => {
            if schema.get("properties").is_some() {
                "object".to_string()
            } else {
                "value".to_string()
            }
        }
    }
}

/// The field names a JSON path names, in order.
///
/// The schema validator writes its paths as `$.deliverable` or
/// `$.entries[0].input`; the index of an array item says which item, not which
/// field, so it is dropped.
fn path_segments(path: &str) -> Vec<String> {
    path.trim_start_matches('$')
        .split('.')
        .filter_map(|part| {
            let name = part
                .chars()
                .take_while(|character| character.is_alphanumeric() || *character == '_')
                .collect::<String>();
            (!name.is_empty()).then_some(name)
        })
        .collect()
}

/// The schema node one path names, if the path names one.
fn schema_at_path<'a>(
    schema: &'a Value,
    defs: Option<&'a Map<String, Value>>,
    segments: &[String],
) -> Option<&'a Value> {
    let mut current = schema;
    for segment in segments {
        let resolved = follow_ref(current, defs);
        let mut next: Option<&'a Value> = None;
        // A choice: the segment belongs to the variant that declares it.
        for keyword in ["oneOf", "anyOf"] {
            let Some(variants) = resolved.get(keyword).and_then(Value::as_array) else {
                continue;
            };
            for variant in variants {
                let variant = follow_ref(variant, defs);
                if let Some(property) = variant
                    .get("properties")
                    .and_then(Value::as_object)
                    .and_then(|properties| properties.get(segment))
                {
                    next = Some(property);
                    break;
                }
            }
            if next.is_some() {
                break;
            }
        }
        if next.is_none() {
            next = resolved
                .get("properties")
                .and_then(Value::as_object)
                .and_then(|properties| properties.get(segment));
        }
        if next.is_none() {
            // An array's element schema carries the fields of its items.
            next = resolved.get("items").and_then(|items| {
                follow_ref(items, defs)
                    .get("properties")
                    .and_then(Value::as_object)
                    .and_then(|properties| properties.get(segment))
            });
        }
        current = next?;
    }
    Some(current)
}

/// Compact statement of what one tool accepts, derived only from `schema`.
///
/// Returns `None` when the schema declares nothing a model has to be told, so a
/// caller can leave the original description untouched.
#[must_use]
pub fn input_contract_summary(schema: &Value) -> Option<String> {
    let defs = schema.get("$defs").and_then(Value::as_object);
    let mut lines: Vec<String> = Vec::new();
    collect_object(schema, defs, &mut lines, 0);
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

fn collect_object(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
    out: &mut Vec<String>,
    depth: usize,
) {
    if depth > 2 {
        return;
    }
    let Some(obj) = schema.as_object() else {
        return;
    };
    // A tagged enum is the case the whole module exists for: list the
    // discriminator once, then each variant's own required set.
    let variants = ["oneOf", "anyOf"]
        .iter()
        .find_map(|key| obj.get(*key).and_then(Value::as_array));
    if let Some(variants) = variants {
        let mut tags: Vec<(String, String)> = Vec::new();
        for variant in variants {
            let resolved = follow_ref(variant, defs);
            let Some(variant) = resolved.as_object() else {
                continue;
            };
            let properties = variant.get("properties").and_then(Value::as_object);
            let required = required_of(variant);
            let Some(properties) = properties else { continue };
            let Some((tag, value)) = discriminator(properties, defs) else {
                continue;
            };
            let tag = tag.as_str();
            let value = value.as_str();
            tags.push((tag.to_string(), value.to_string()));
            // The discriminator is declared once above the variants; repeating it
            // inside each variant only makes the block longer.
            let fields = required
                .iter()
                .filter(|field| ***field != *tag)
                .map(|field| field_line(field, properties.get(*field), true, defs))
                .collect::<Vec<_>>()
                .join(", ");
            // The legal values are the one thing a model cannot guess, so they are
            // worth reporting even for a variant that adds no further
            // requirement - and reporting them must not depend on one existing.
            if !fields.is_empty() {
                out.push(format!("  {tag}=\"{value}\": {fields}"));
            }
        }
        if tags.is_empty() {
            collect_fields(obj, defs, out, depth);
            return;
        }
        let (tag, _) = &tags[0];
        let values = tags
            .iter()
            .map(|(_, value)| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(" | ");
        out.insert(0, format!("{tag}: string {values} (required, discriminator)"));
        return;
    }
    collect_fields(obj, defs, out, depth);
}

fn collect_fields(
    obj: &Map<String, Value>,
    defs: Option<&Map<String, Value>>,
    out: &mut Vec<String>,
    _depth: usize,
) {
    let properties = obj.get("properties").and_then(Value::as_object);
    let required = required_of(obj);
    if properties.is_none() && required.is_empty() {
        return;
    }
    for field in &required {
        out.push(field_line(
            field,
            properties.and_then(|props| props.get(*field)),
            true,
            defs,
        ));
    }
    let Some(properties) = properties else { return };
    for (field, schema) in properties {
        if required.iter().any(|required| required == field) {
            continue;
        }
        let resolved = follow_ref(schema, defs);
        if !states_a_legal_input(&resolved) {
            continue;
        }
        out.push(field_line(field, Some(schema), false, defs));
    }
}

fn field_line(
    field: &str,
    schema: Option<&Value>,
    required: bool,
    defs: Option<&Map<String, Value>>,
) -> String {
    let empty = Value::Null;
    let resolved = schema.map(|schema| follow_ref(schema, defs)).unwrap_or(&empty);
    let mut line = format!("{}: {}", field, type_of(resolved));
    let mut constraints = Vec::new();
    match resolved.get("const") {
        Some(Value::String(value)) => constraints.push(format!("= \"{value}\"")),
        Some(value) => constraints.push(format!("= {value}")),
        None => {
            if let Some(values) = resolved.get("enum").and_then(Value::as_array) {
                let values = values
                    .iter()
                    .map(|value| match value {
                        Value::String(value) => format!("\"{value}\""),
                        value => value.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                constraints.push(format!("= {values}"));
            }
        }
    }
    for (key, label) in [
        ("minLength", "min length"),
        ("maxLength", "max length"),
        ("minimum", "min"),
        ("maximum", "max"),
        ("minItems", "min items"),
        ("format", "format"),
        ("pattern", "pattern"),
    ] {
        if let Some(value) = resolved.get(key) {
            // `format` and `pattern` are words, not JSON values; quoting them
            // would read as a literal the model has to type.
            let rendered = match (key, value.as_str()) {
                ("format", Some(word)) | ("pattern", Some(word)) => word.to_string(),
                _ => value.to_string(),
            };
            constraints.push(format!("{label} {rendered}"));
        }
    }
    if field.ends_with(REF_SUFFIX) && type_of(resolved) == "string" && resolved.get("const").is_none() && resolved.get("enum").is_none() {
        constraints.push(REF_HINT.to_string());
    }
    if !constraints.is_empty() {
        line.push(' ');
        line.push_str(&constraints.join(", "));
    }
    if required {
        line.push_str(" (required)");
    } else {
        line.push_str(" (optional)");
    }
    // The schema's own description is the only place that says what the field
    // MEANS, and therefore what the model has to do to obtain a legal value.
    // Dropping it left the model with types and constraints alone: one live run
    // omitted a required `reason_ref` twice because its description ("a durable
    // reference to the review body itself") never reached it, and another
    // published a sha256 that was the digest of the empty string although the
    // description said it must come from a real read. schemars carries the
    // struct's doc comments here, so this is already-written guidance, not new
    // prompt text.
    if let Some(summary) = description_summary(resolved.get("description")) {
        line.push_str(" — ");
        line.push_str(&summary);
    }
    line
}

/// A field description reduced to what fits one line.
///
/// The first sentence carries the operative statement in almost every doc
/// comment. Everything after the cap is dropped, and the drop is announced with
/// an ellipsis so a silent truncation is never mistaken for the whole rule.
fn description_summary(description: Option<&Value>) -> Option<String> {
    let raw = description.and_then(Value::as_str)?.trim();
    if raw.is_empty() {
        return None;
    }
    let flattened = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let first_sentence = match flattened.find(". ") {
        Some(index) if index + 2 <= DESCRIPTION_MAX_CHARS => &flattened[..index + 1],
        _ => &flattened[..flattened.len().min(DESCRIPTION_MAX_CHARS)],
    };
    let mut summary = first_sentence.to_string();
    if summary.len() < flattened.len() {
        summary.push('…');
    }
    Some(summary)
}

/// A field's shape in the fewest words. A schema omits `type` exactly when the
/// field is an object or a choice, which is the case where a bare "value" would
/// tell the model nothing.
fn type_of(schema: &Value) -> String {
    if let Some(kind) = schema.get("type").and_then(Value::as_str) {
        return kind.to_string();
    }
    if schema.get("properties").is_some() {
        return "object".to_string();
    }
    if schema.get("oneOf").is_some_and(Value::is_array) {
        return "one of".to_string();
    }
    if schema.get("anyOf").is_some_and(Value::is_array) {
        return "one of".to_string();
    }
    "value".to_string()
}

/// A field is worth listing when the schema says something a model could get
/// wrong: a fixed value, a closed set, or a bound. Everything else is a type the
/// model already reads off the schema itself.
fn states_a_legal_input(schema: &Value) -> bool {
    [
        "const",
        "enum",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "minItems",
        "format",
        "pattern",
    ]
    .iter()
    .any(|key| schema.get(key).is_some())
        // A description is a statement of what the field is for, which is the
        // only guidance an optional field may carry. Requiring a numeric
        // constraint here silently dropped every such field from the contract:
        // `objective_complete_request` has seven fields and rendered zero
        // characters, so the model was told nothing about its input at all.
        || schema
            .get("description")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty())
}

fn required_of(obj: &Map<String, Value>) -> Vec<&str> {
    obj.get("required")
        .and_then(Value::as_array)
        .map(|required| required.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

/// The names a schema requires, owned so a caller can keep them alongside the
/// schema it read them from.
fn required_names(schema: &Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The variant key whose `const` tells the variants apart: the tag of an
/// internally-tagged enum.
fn discriminator<'a>(
    properties: &'a Map<String, Value>,
    defs: Option<&Map<String, Value>>,
) -> Option<(String, String)> {
    properties.iter().find_map(|(field, schema)| {
        let resolved = follow_ref(schema, defs);
        let value = resolved.get("const")?.as_str()?;
        Some((field.clone(), value.to_string()))
    })
}

fn follow_ref<'a>(schema: &'a Value, defs: Option<&'a Map<String, Value>>) -> &'a Value {
    let mut current = schema;
    for _ in 0..MAX_REF_DEPTH {
        let Some(target) = current.get("$ref").and_then(Value::as_str) else {
            break;
        };
        let Some(name) = target.rsplit('/').next() else {
            break;
        };
        let Some(resolved) = defs.and_then(|defs| defs.get(name)) else {
            break;
        };
        current = resolved;
    }
    current
}

/// Every field name the schema admits.
///
/// A tagged schema has no top-level property set - each variant declares its own -
/// so reading the top level answers `[]` even though the tool accepts plenty. The
/// rejection message therefore has to list the union, or the model is told what is
/// missing and nothing it may send.
#[must_use]
pub fn admissible_field_names(schema: &Value) -> Vec<String> {
    let defs = schema.get("$defs").and_then(Value::as_object);
    let mut names: Vec<String> = Vec::new();
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        names.extend(properties.keys().cloned());
    }
    for key in ["oneOf", "anyOf"] {
        let Some(variants) = schema.get(key).and_then(Value::as_array) else {
            continue;
        };
        for variant in variants {
            let resolved = follow_ref(variant, defs);
            if let Some(properties) = resolved.get("properties").and_then(Value::as_object) {
                names.extend(properties.keys().cloned());
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

#[cfg(test)]
mod tests {
    #[test]
    fn gb2_correction_requires_the_exact_closed_schema_and_error() {
        let schema = serde_json::json!({"type":"object", "required":["reason"],
            "properties":{"reason":{"type":"string"}}, "additionalProperties":false});
        let hint = super::input_shape_correction;
        assert!(hint("task_review", "$: missing required field `reason`", &schema).is_some());
        assert!(hint("task_review", "$: unexpected field `reason_ref`", &schema).is_some());
        assert!(hint("other", "$: unexpected field `reason_ref`", &schema).is_none());
        assert!(hint("task_review", "$.other: unexpected field `reason_ref`", &schema).is_none());
        // The runtime composes `tool.call.rejected_before_dispatch` reasons from the
        // production validator text, which carries the tool prefix; live_one_team_execution
        // (0.10.49) refused task_review three times for `reason_ref` with no hint because
        // the lookup below only ever saw the bare spelling.
        assert!(hint(
            "task_review",
            "tool `task_review` input violates its pinned schema: $: unexpected field `reason_ref`",
            &schema
        )
        .is_some());
        assert!(hint(
            "task_review",
            "tool `task_review` input violates its pinned schema: $: missing required field `reason`",
            &schema
        )
        .is_some());
        let submit = serde_json::json!({"type":"object", "properties":{"task_ref":{"type":"string"}},
            "additionalProperties":false});
        assert!(hint("task_submit", "$: unexpected field `reason`", &submit).is_some());
        assert!(hint("other", "$: unexpected field `reason`", &submit).is_none());
        assert!(hint("state_inspect", "$: unexpected field `limit`", &submit).is_some());
        assert!(hint("state_inspect", "$: unexpected field `other`", &submit).is_none());
        let mut open = schema.clone();
        open["additionalProperties"] = true.into();
        assert!(hint("task_review", "$: unexpected field `reason_ref`", &open).is_none());
        let mut branched = schema.clone();
        branched["oneOf"] = serde_json::json!([{}]);
        assert!(hint("task_review", "$: missing required field `reason`", &branched).is_none());
        let shape = serde_json::json!({"type":"object", "additionalProperties":false,
            "properties":{"steps":{"type":"array","items":{"type":"object",
                "required":["id"],"properties":{"id":{"type":"string"}}}}}});
        assert!(hint("tool_orchestrate", "$: unexpected field `id`", &shape).is_some());
        let mut open = shape.clone();
        open["additionalProperties"] = true.into();
        assert!(hint("tool_orchestrate", "$: unexpected field `id`", &open).is_none());
    }

    /// A degradation never widens the instance-coverage window, and the
    /// degraded branch is bounded on its own.
    ///
    /// This is the symmetric negative control of the coverage gate: the
    /// positive control says every tool's example is a real instance or a
    /// named degradation; this one says a surface full of degradations cannot
    /// pass the window that counts instances.
    #[test]
    fn a_degradation_never_widens_the_instance_window() {
        // Twenty instances and forty degradations: neither the window nor the
        // degraded branch may hold.
        let widened = ExampleCoverage {
            instances: 20,
            degraded: 40,
        };
        assert!(
            !widened.instance_window_holds(),
            "a degradation must not count into the instance-coverage window"
        );
        assert!(
            !widened.degraded_branch_holds(),
            "the degraded branch is bounded on its own"
        );
        // The honest shape: the instances carry the window, and the
        // degradations stay inside the side branch.
        let honest = ExampleCoverage {
            instances: 21,
            degraded: 4,
        };
        assert!(honest.instance_window_holds());
        assert!(honest.degraded_branch_holds());
        // A surface that lost its instances cannot be carried by degradations.
        let lost = ExampleCoverage {
            instances: 3,
            degraded: 40,
        };
        assert!(!lost.instance_window_holds());
    }

    /// A field's schema node resolves to the shape it really means.
    ///
    /// The derived schema answers with a `$ref` into the definitions, and a
    /// choice of named variants answers with a `$ref` inside its first branch.
    /// A repair that reads the node as written reads no type at all on exactly
    /// the fields that need repairing.
    #[test]
    fn a_field_schema_resolves_to_the_shape_it_means() {
        let root = serde_json::json!({
            "$defs": {
                "Deliverable": {"type": "object", "required": ["block_index"]},
                "Decision": {"type": "string", "enum": ["accept", "rework"]},
            },
            "type": "object",
            "properties": {
                "deliverable": {"$ref": "#/$defs/Deliverable"},
                "decision": {"$ref": "#/$defs/Decision"},
                "optional": {"anyOf": [{"$ref": "#/$defs/Deliverable"}, {"type": "null"}]},
            },
        });
        let properties = root
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .expect("properties");
        let deliverable = resolve_schema_node(&root, &properties["deliverable"]);
        assert_eq!(deliverable.get("type").and_then(serde_json::Value::as_str), Some("object"));
        let decision = resolve_schema_node(&root, &properties["decision"]);
        assert_eq!(
            decision.get("enum").and_then(serde_json::Value::as_array).map(Vec::len),
            Some(2)
        );
        let optional = resolve_schema_node(&root, &properties["optional"]);
        assert_eq!(optional.get("type").and_then(serde_json::Value::as_str), Some("object"));
    }

    /// A literal that differs from a legal member only in case is put back into
    /// the contract's spelling - but only when exactly one member matches.
    ///
    /// Two members that differ only in case are two members: the value names
    /// neither, and nothing is repaired.
    #[test]
    fn a_literal_is_repaired_only_when_one_member_matches() {
        let schema = serde_json::json!({"enum": ["accept", "challenge", "rework"]});
        assert_eq!(
            normalize_schema_literal(&schema, &serde_json::json!("Accept")),
            Some(serde_json::json!("accept"))
        );
        // A value that names no member is not repaired.
        assert_eq!(
            normalize_schema_literal(&schema, &serde_json::json!("nonsense")),
            None
        );
        // Two members that differ only in case: nothing is repaired.
        let ambiguous = serde_json::json!({"enum": ["Accept", "accept"]});
        assert_eq!(
            normalize_schema_literal(&ambiguous, &serde_json::json!("ACCEPT")),
            None
        );
        // A const is a member of one.
        let fixed = serde_json::json!({"const": "task_review"});
        assert_eq!(
            normalize_schema_literal(&fixed, &serde_json::json!("Task_Review")),
            Some(serde_json::json!("task_review"))
        );
        // A value that already holds is not touched.
        assert_eq!(
            normalize_schema_literal(&schema, &serde_json::json!("accept")),
            None
        );
    }

    /// A required field the budget cost is never dropped silently.
    ///
    /// The budget ends the rendering, and a field the example meant to carry
    /// used to disappear with it: a model that copied the example was refused
    /// for the field the framework had dropped. The example may therefore be a
    /// named degradation - but it may never be a partial instance.
    ///
    /// This control is written against the stable surface only, so it runs on
    /// the tree that predates the degradation as well: there it is red.
    #[test]
    fn a_required_field_the_budget_cost_is_never_dropped_silently() {
        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();
        for index in 0..120 {
            let name = format!("field_{index}");
            properties.insert(name.clone(), serde_json::json!({"type": "string"}));
            required.push(serde_json::Value::String(name));
        }
        let schema = serde_json::json!({
            "type": "object",
            "properties": properties,
            "required": required,
        });
        let example = input_example(&schema).expect("a schema with required fields has an example");
        if let Some(degraded) = example.get("__example_degraded__") {
            let named = degraded
                .get("fields")
                .and_then(serde_json::Value::as_array)
                .map(|fields| {
                    fields
                        .iter()
                        .filter_map(|field| field.get("name").and_then(serde_json::Value::as_str))
                        .collect::<std::collections::BTreeSet<_>>()
                })
                .unwrap_or_default();
            for index in 0..120 {
                let name = format!("field_{index}");
                assert!(
                    named.contains(name.as_str()),
                    "the degradation must name the required field {name}: {example}"
                );
            }
            return;
        }
        let instance = example
            .as_object()
            .expect("an instance that is not degraded is an object");
        for index in 0..120 {
            let name = format!("field_{index}");
            assert!(
                instance.contains_key(&name),
                "the example dropped the required field {name}: {example}"
            );
        }
    }

    #[test]
    fn the_contract_carries_what_each_field_means() {
        // The summary used to render types and constraints only, dropping the
        // schema's own descriptions. One live run omitted a required `reason_ref`
        // twice because its description ("a durable reference to the review body
        // itself") never reached the model, and another published a sha256 that
        // was the digest of the empty string although the description said it
        // must come from a real read.
        let schema = serde_json::to_value(schemars::schema_for!(
            crate::agent_action::ObjectiveReviewInput
        ))
        .expect("schema");
        let summary = input_contract_summary(&schema).expect("summary");
        assert!(
            summary.contains("review body itself"),
            "reason_ref's meaning must reach the model: {summary}"
        );
        assert!(
            summary.contains("returned by state_inspect"),
            "criterion_ref's origin must reach the model: {summary}"
        );
        assert!(
            summary.contains("result_refs: array (optional)"),
            "an optional field with a description must appear: {summary}"
        );
        assert!(
            summary.contains("evidence_refs: array (optional)"),
            "evidence_refs must appear: {summary}"
        );
    }

    #[test]
    fn a_tool_with_no_constraints_still_gets_a_contract() {
        // `objective_complete_request` has seven fields and used to render zero
        // characters, so the model was told nothing about its input.
        let schema = serde_json::to_value(schemars::schema_for!(
            crate::agent_action::ObjectiveCompleteRequestInput
        ))
        .expect("schema");
        let summary = input_contract_summary(&schema).expect("summary");
        assert!(
            !summary.trim().is_empty(),
            "a tool whose fields carry only descriptions must still have a contract"
        );
        assert!(
            summary.contains("result_refs"),
            "its fields must be named: {summary}"
        );
    }

    #[test]
    fn a_long_description_is_truncated_with_an_announced_ellipsis() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "body": {
                    "type": "string",
                    "description": "x".repeat(600).as_str()
                }
            },
            "required": ["body"]
        });
        let summary = input_contract_summary(&schema).expect("summary");
        assert!(
            summary.contains('…'),
            "a truncation must be announced, never silent: {summary}"
        );
        assert!(
            summary.len() < 600,
            "the cap must bound the line: {} chars",
            summary.len()
        );
    }
    use super::*;
    use serde_json::json;

    #[test]
    fn a_tagged_enum_names_its_discriminator_and_each_variants_requirements() {
        let schema = json!({
            "type": "object",
            "oneOf": [
                {"type": "object", "additionalProperties": false,
                 "properties": {"source": {"const": "file"}, "path": {"type": "string"}, "sha256": {"type": "string"}},
                 "required": ["source", "path", "sha256"]},
                {"type": "object", "additionalProperties": false,
                 "properties": {"source": {"const": "message_block"}, "message_id": {"type": "string"}, "block_index": {"type": "integer", "minimum": 0}},
                 "required": ["source", "message_id", "block_index"]}
            ]
        });
        let summary = input_contract_summary(&schema).expect("a tagged enum is summarizable");
        assert!(
            summary.contains("source: string \"file\" | \"message_block\" (required, discriminator)"),
            "{summary}"
        );
        assert!(summary.contains("source=\"file\": path: string (required), sha256: string (required)"), "{summary}");
        assert!(summary.contains("source=\"message_block\": message_id: string (required), block_index: integer min 0 (required)"), "{summary}");
    }

    /// The legal values of a field are what the model cannot guess, and they are
    /// exactly what a `$ref` into `$defs` hides. One live run failed a
    /// `task_review` because `decision` is such a field and the description
    /// never named `accept`, `challenge` or `rework`.
    #[test]
    fn a_ref_into_defs_still_yields_the_legal_values() {
        let schema = json!({
            "type": "object",
            "$defs": {"TaskReviewDecision": {"enum": ["accept", "challenge", "rework"], "type": "string"}},
            "properties": {"task_ref": {"type": "string"}, "decision": {"$ref": "#/$defs/TaskReviewDecision"}, "reason": {"type": "string"}},
            "required": ["task_ref", "decision", "reason"]
        });
        let summary = input_contract_summary(&schema).expect("required fields are summarizable");
        assert!(
            summary.contains("decision: string = \"accept\" | \"challenge\" | \"rework\" (required)"),
            "{summary}"
        );
    }

    /// A reference is whatever object it names, so the schema types it as a bare
    /// `string`. The model must not have to invent a format: say where the value
    /// comes from instead of guessing one.
    #[test]
    fn a_bare_reference_field_is_told_where_its_value_comes_from() {
        let schema = json!({
            "type": "object",
            "properties": {"task_ref": {"type": "string"}},
            "required": ["task_ref"]
        });
        let summary = input_contract_summary(&schema).expect("a required field is summarizable");
        assert!(summary.contains("task_ref: string canonical Runtime ref"), "{summary}");
        assert!(summary.contains("verify one with `read_ref`"), "{summary}");
    }

    /// A numeric bound is a legal-input fact the same way an enum is, and it is
    /// invisible in prose.
    #[test]
    fn bounds_are_stated_with_the_field() {
        let schema = json!({
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 16}, "query": {"type": "string", "minLength": 1}},
            "required": []
        });
        let summary = input_contract_summary(&schema).expect("optional enums are summarizable");
        assert!(summary.contains("limit: integer min 1, max 16 (optional)"), "{summary}");
    }

    /// The summary is what a model is told, so it must be byte-identical on every
    /// request. Anything that varies - a clock, a map iteration order - would
    /// fork the prompt prefix for no reason.
    #[test]
    fn the_summary_is_deterministic_and_names_no_tool() {
        let schema = json!({
            "type": "object",
            "properties": {"b": {"type": "string"}, "a": {"type": "string"}, "c": {"enum": ["x", "y"]}},
            "required": ["a"]
        });
        let first = input_contract_summary(&schema).expect("summarizable");
        let second = input_contract_summary(&schema).expect("summarizable");
        assert_eq!(first, second);
        for tool in ["artifact_publish", "task_review", "read_ref"] {
            assert!(
                !first.contains(tool),
                "the summary must derive from the schema, not name tools: {first}"
            );
        }
    }

    /// A rejection that names what is missing but nothing that is allowed leaves
    /// the model guessing, which is exactly how one live run produced eleven
    /// invented `artifact_publish` inputs.
    #[test]
    fn a_tagged_schema_still_lists_every_field_it_admits() {
        let schema = json!({
            "type": "object",
            "oneOf": [
                {"type": "object", "properties": {"source": {"const": "file"}, "path": {"type": "string"}}, "required": ["source", "path"]},
                {"type": "object", "properties": {"source": {"const": "artifact"}, "content_ref": {"type": "string"}}, "required": ["source", "content_ref"]}
            ]
        });
        assert_eq!(
            admissible_field_names(&schema),
            vec!["content_ref", "path", "source"],
            "the union across variants is what the tool accepts"
        );
        assert!(
            admissible_field_names(&json!({"type": "object", "properties": {"a": {}, "b": {}}}))
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                == vec!["a", "b"]
        );
    }

    #[test]
    fn a_schema_with_nothing_to_say_produces_no_summary() {
        assert!(input_contract_summary(&json!({"type": "object"})).is_none());
        assert!(input_contract_summary(&json!({"type": "string"})).is_none());
    }

    /// The example exists so a model can copy it, so it has to be a valid
    /// instance of the very schema it illustrates. An example that fails its own
    /// schema is worse than no example: the model copies it and is refused.
    #[test]
    fn the_example_is_a_valid_instance_of_its_own_schema() {
        let cases = [
            // Every JSON scalar type.
            json!({"type":"object","properties":{"s":{"type":"string"},"i":{"type":"integer"},"b":{"type":"boolean"}},"required":["s","i","b"]}),
            // An enum takes its first legal value.
            json!({"type":"object","properties":{"source":{"type":"string","enum":["file","message_block"]}},"required":["source"]}),
            // An array shows one element, not none.
            json!({"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}},"required":["tags"]}),
            // A nested object is filled in too, or the wrapper is still missing.
            json!({"type":"object","properties":{"input":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}},"required":["input"]}),
        ];
        for schema in cases {
            let example = input_example(&schema).expect("an example");
            let errors = minimal_schema_errors(&schema, &example, "$");
            assert_eq!(errors, Vec::<String>::new(), "example {example} vs schema {schema}");
        }
    }

    /// The invariants that make an example copyable: required fields present,
    /// declared types honored, enum values legal. The authoritative check - the
    /// same `validate_input` Runtime uses - lives with the gateway's tools, where
    /// every real tool schema is exercised; this one keeps the unit local.
    fn minimal_schema_errors(schema: &Value, value: &Value, path: &str) -> Vec<String> {
        let mut errors = Vec::new();
        if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
            let matches = variants
                .iter()
                .filter(|variant| minimal_schema_errors(variant, value, path).is_empty())
                .count();
            if matches != 1 {
                errors.push(format!(
                    "{path}: expected exactly one variant, matched {matches}"
                ));
            }
        }
        if let Some(expected) = schema.get("const") {
            if value != expected {
                errors.push(format!("{path}: not the required constant"));
            }
        }
        if let Some(expected) = schema.get("type") {
            let choices: Vec<&str> = match expected {
                Value::String(one) => vec![one.as_str()],
                Value::Array(many) => many.iter().filter_map(Value::as_str).collect(),
                _ => Vec::new(),
            };
            if !choices.is_empty() {
                let ok = choices.iter().any(|choice| match *choice {
                    "string" => value.is_string(),
                    "integer" => value.as_i64().is_some(),
                    "number" => value.as_f64().is_some(),
                    "boolean" => value.is_boolean(),
                    "array" => value.is_array(),
                    "object" => value.is_object(),
                    "null" => value.is_null(),
                    _ => true,
                });
                if !ok {
                    errors.push(format!("{path}: expected one of {choices:?}"));
                }
            }
        }
        if let Some(values) = schema.get("enum").and_then(Value::as_array) {
            if !values.contains(value) {
                errors.push(format!("{path}: not an allowed value"));
            }
        }
        if let (Some(object), Some(required)) = (
            value.as_object(),
            schema.get("required").and_then(Value::as_array),
        ) {
            for field in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(field) {
                    errors.push(format!("{path}: missing required `{field}`"));
                }
            }
        }
        if let (Some(object), Some(properties)) = (
            value.as_object(),
            schema.get("properties").and_then(Value::as_object),
        ) {
            for (field, field_value) in object {
                if let Some(field_schema) = properties.get(field) {
                    errors.extend(minimal_schema_errors(
                        field_schema,
                        field_value,
                        &format!("{path}.{field}"),
                    ));
                }
            }
        }
        if let (Some(array), Some(items)) = (
            value.as_array(),
            schema.get("items"),
        ) {
            for (index, item) in array.iter().enumerate() {
                errors.extend(minimal_schema_errors(items, item, &format!("{path}[{index}]")));
            }
        }
        errors
    }

    /// The case that matters most: the entry wrapper a model keeps dropping.
    #[test]
    fn the_batch_entry_wrapper_is_exemplified() {
        let schema = json!({
            "type": "object",
            "properties": {
                "actions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "tool": {"type": "string"},
                            "input": {"type": "object"}
                        },
                        "required": ["tool", "input"]
                    }
                }
            },
            "required": ["actions"]
        });
        let example = input_example(&schema).expect("an example");
        let actions = example["actions"].as_array().expect("actions array");
        assert_eq!(actions.len(), 1);
        assert!(actions[0].get("tool").is_some(), "the wrapper must be shown: {example}");
        assert!(actions[0].get("input").is_some(), "the payload must be shown: {example}");
    }

    /// An optional field must not appear: an example that shows one teaches a
    /// model to send what it is allowed to omit.
    #[test]
    fn only_required_fields_are_exemplified() {
        let schema = json!({
            "type":"object",
            "properties":{"need":{"type":"string"},"optional":{"type":"string"}},
            "required":["need"]
        });
        let example = input_example(&schema).expect("an example");
        assert!(example.get("need").is_some(), "{example}");
        assert!(example.get("optional").is_none(), "{example}");
    }

    /// A tagged enum is examplified with ONE valid instance, never a list.
    ///
    /// An array of examples is not itself a legal input, so a model that copies
    /// it is refused for a shape the framework handed it. The example's job is to
    /// show one shape that validates; `input_contract_summary` already names
    /// every variant's required fields, so the choice is not lost.
    #[test]
    fn a_tagged_enum_is_exemplified_with_one_valid_instance() {
        let schema = json!({
            "oneOf": [
                {"type":"object","properties":{"source":{"const":"file"},"path":{"type":"string"}},"required":["path"]},
                {"type":"object","properties":{"source":{"const":"message_block"},"block_index":{"type":"integer"}},"required":["block_index"]}
            ]
        });
        let example = input_example(&schema).expect("an example");
        assert!(example.is_object(), "one instance, not a list of them: {example}");
        assert_eq!(example["source"], "file");
        assert_eq!(example["path"], "<path>");
        // The instance validates against the schema that produced it.
        let errors = minimal_schema_errors(&schema, &example, "$");
        assert_eq!(errors, Vec::<String>::new(), "{errors:?}");

        let mut wrong_tag = example.clone();
        wrong_tag["source"] = json!("message_block");
        assert_eq!(
            minimal_schema_errors(&schema["oneOf"][0], &wrong_tag, "$"),
            vec!["$.source: not the required constant".to_string()]
        );
        assert_eq!(
            minimal_schema_errors(&schema, &wrong_tag, "$"),
            vec!["$: expected exactly one variant, matched 0".to_string()]
        );

        let mut missing_path = example.clone();
        missing_path.as_object_mut().expect("object").remove("path");
        assert_eq!(
            minimal_schema_errors(&schema["oneOf"][0], &missing_path, "$"),
            vec!["$: missing required `path`".to_string()]
        );
        assert_eq!(
            minimal_schema_errors(&schema, &missing_path, "$"),
            vec!["$: expected exactly one variant, matched 0".to_string()]
        );

        let overlapping_schema = json!({
            "oneOf": [schema["oneOf"][0].clone(), schema["oneOf"][0].clone()]
        });
        assert_eq!(
            minimal_schema_errors(&overlapping_schema, &example, "$"),
            vec!["$: expected exactly one variant, matched 2".to_string()]
        );
    }

    /// A schema with nothing to fill in has nothing to exemplify: an example of
    /// `{}` is noise a model learns to skip.
    #[test]
    fn a_schema_with_nothing_to_fill_in_has_no_example() {
        assert!(input_example(&json!({"type":"object"})).is_none());
        assert!(input_example(&json!({"type":"object","properties":{},"required":[]})).is_none());
    }

    /// Placeholders must be recognizable as placeholders, so a model replaces
    /// them instead of repeating them as content.
    #[test]
    fn placeholders_are_recognizable() {
        let schema = json!({"type":"object","properties":{"team_ref":{"type":"string"}},"required":["team_ref"]});
        let example = input_example(&schema).expect("an example");
        assert_eq!(example["team_ref"], "<team_ref>");
    }

}
