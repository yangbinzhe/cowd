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

const REF_HINT: &str = "canonical Runtime ref; the receipt that returned the object \
                         carries the exact value; verify one with `read_ref`";

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
    line
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
}
