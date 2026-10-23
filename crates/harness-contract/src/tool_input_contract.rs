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
    let defs = schema.get("$defs").and_then(Value::as_object);
    let mut rendered = 0_usize;
    let example = example_for(schema, defs, 0, &mut rendered, "value");
    // A schema that declares nothing to fill in has nothing to exemplify; an
    // example of `{}` is noise.
    let is_empty = example.as_object().is_some_and(Map::is_empty);
    (!is_empty).then_some(example)
}

fn example_for(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
    depth: usize,
    rendered: &mut usize,
    field_name: &str,
) -> Value {
    if depth > MAX_REF_DEPTH || *rendered > EXAMPLE_MAX_CHARS {
        return Value::Null;
    }
    // The module's existing resolver, reused rather than reimplemented: a second
    // `$ref` walker is a second thing that can drift.
    let schema = follow_ref(schema, defs);
    if let Some(one) = schema.get("const") {
        return one.clone();
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        // The first legal value is the one a model can copy without choosing.
        return values.first().cloned().unwrap_or(Value::Null);
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
            let example = example_for(resolved, defs, depth + 1, rendered, field_name);
            if !example.is_null() {
                return example;
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
            *rendered += example.as_str().map_or(0, str::len);
            example
        }
        Some("integer") | Some("number") => Value::from(0),
        Some("boolean") => Value::Bool(true),
        Some("array") => match schema.get("items") {
            // One element shows the element's shape; an empty array shows none.
            Some(items) => {
                let item = example_for(items, defs, depth + 1, rendered, field_name);
                if item.is_null() {
                    Value::Array(Vec::new())
                } else {
                    Value::Array(vec![item])
                }
            }
            None => Value::Array(Vec::new()),
        },
        _ => object_example(schema, defs, depth, rendered),
    }
}

fn object_example(
    schema: &Value,
    defs: Option<&Map<String, Value>>,
    depth: usize,
    rendered: &mut usize,
) -> Value {
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if properties.is_empty() {
        return Value::Object(Map::new());
    }
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_default();
    // A tool with nothing mandatory has nothing an example must show. Filling in
    // every optional field instead would teach a model to send what it may omit -
    // `state_inspect` would be exemplified with all six of its optional filters,
    // when the common correct call is `{}`.
    if required.is_empty() {
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
        if !required.iter().any(|name| name == field) && !is_discriminator {
            continue;
        }
        let value = example_for(field_schema, defs, depth + 1, rendered, field);
        if !value.is_null() {
            example.insert(field.clone(), value);
        }
    }
    Value::Object(example)
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
