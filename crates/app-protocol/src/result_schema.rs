//! Explicit JSON Schema Draft 7 subset for APP structured results.
//!
//! The subset is deliberately small. A schema either validates completely
//! within these keywords or the manifest is rejected before installation.

use serde_json::Value;

use crate::digest::canonical_digest_v1;
use crate::{ProtocolValidationError, Sha256Digest};

const MAX_SCHEMA_BYTES: usize = 64 * 1024;
const MAX_SCHEMA_DEPTH: usize = 64;

pub fn canonical_result_schema_bytes(schema: &Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(schema).map_err(|error| format!("result schema is not JSON: {error}"))
}

pub fn canonical_result_schema_digest(
    schema: &Value,
) -> Result<Sha256Digest, ProtocolValidationError> {
    canonical_digest_v1("cowd.app.result-schema.v1", schema)
}

pub fn validate_result_schema(schema: &Value) -> Result<(), String> {
    let bytes = canonical_result_schema_bytes(schema)?;
    if bytes.is_empty() || bytes.len() > MAX_SCHEMA_BYTES {
        return Err(format!(
            "result schema must contain 1..={MAX_SCHEMA_BYTES} bytes"
        ));
    }
    validate_schema_node(schema, schema, "$", 0)
}

pub fn validate_result_value(schema: &Value, value: &Value) -> Result<(), String> {
    validate_schema_node(schema, schema, "$", 0)?;
    validate_value_node(schema, schema, value, "$", 0)
}

fn validate_schema_node(
    root: &Value,
    schema: &Value,
    path: &str,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(format!("{path}: result schema exceeds the depth limit"));
    }
    let object = schema
        .as_object()
        .ok_or_else(|| format!("{path}: result schema node must be an object"))?;
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "$schema"
                | "$id"
                | "title"
                | "description"
                | "type"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "minItems"
                | "maxItems"
                | "minLength"
                | "maxLength"
                | "minimum"
                | "maximum"
                | "enum"
                | "const"
                | "allOf"
                | "anyOf"
                | "oneOf"
                | "$defs"
                | "definitions"
                | "$ref"
        ) {
            return Err(format!("{path}: unsupported result schema keyword `{key}`"));
        }
    }
    if let Some(dialect) = object.get("$schema") {
        let dialect = dialect
            .as_str()
            .ok_or_else(|| format!("{path}.$schema: must be a string"))?;
        if !matches!(
            dialect,
            "http://json-schema.org/draft-07/schema#" | "https://json-schema.org/draft-07/schema#"
        ) {
            return Err(format!(
                "{path}.$schema: unsupported result schema dialect `{dialect}`"
            ));
        }
    }
    for keyword in ["$id", "title", "description"] {
        if object
            .get(keyword)
            .is_some_and(|value| value.as_str().is_none())
        {
            return Err(format!("{path}.{keyword}: must be a string"));
        }
    }

    if let Some(reference) = object.get("$ref") {
        let reference = reference
            .as_str()
            .ok_or_else(|| format!("{path}.$ref: must be a string"))?;
        if !reference.starts_with('#') {
            return Err(format!("{path}.$ref: external references are unsupported"));
        }
        if root.pointer(&reference[1..]).is_none() {
            return Err(format!(
                "{path}.$ref: unresolved local reference `{reference}`"
            ));
        }
    }

    if let Some(expected) = object.get("type") {
        match expected {
            Value::String(kind) if is_supported_type(kind) => {}
            Value::Array(kinds)
                if !kinds.is_empty()
                    && kinds
                        .iter()
                        .all(|kind| kind.as_str().is_some_and(is_supported_type)) => {}
            _ => return Err(format!("{path}.type: unsupported type declaration")),
        }
    }

    if let Some(properties) = object.get("properties") {
        let properties = properties
            .as_object()
            .ok_or_else(|| format!("{path}.properties: must be an object"))?;
        for (name, property) in properties {
            validate_schema_node(
                root,
                property,
                &format!("{path}.properties.{name}"),
                depth + 1,
            )?;
        }
    }
    if let Some(required) = object.get("required") {
        let required = required
            .as_array()
            .ok_or_else(|| format!("{path}.required: must be an array"))?;
        let mut names = std::collections::BTreeSet::new();
        for name in required {
            let name = name
                .as_str()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| format!("{path}.required: names must be non-empty strings"))?;
            if !names.insert(name) {
                return Err(format!("{path}.required: duplicate field `{name}`"));
            }
        }
    }
    if let Some(additional) = object.get("additionalProperties") {
        if !additional.is_boolean() {
            validate_schema_node(
                root,
                additional,
                &format!("{path}.additionalProperties"),
                depth + 1,
            )?;
        }
    }
    if let Some(items) = object.get("items") {
        if items.is_array() {
            return Err(format!("{path}.items: tuple item schemas are unsupported"));
        }
        validate_schema_node(root, items, &format!("{path}.items"), depth + 1)?;
    }
    for keyword in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = object.get(keyword) {
            let branches = branches
                .as_array()
                .filter(|branches| !branches.is_empty())
                .ok_or_else(|| format!("{path}.{keyword}: must be a non-empty array"))?;
            for (index, branch) in branches.iter().enumerate() {
                validate_schema_node(
                    root,
                    branch,
                    &format!("{path}.{keyword}[{index}]"),
                    depth + 1,
                )?;
            }
        }
    }
    for keyword in ["$defs", "definitions"] {
        if let Some(definitions) = object.get(keyword) {
            let definitions = definitions
                .as_object()
                .ok_or_else(|| format!("{path}.{keyword}: must be an object"))?;
            for (name, definition) in definitions {
                validate_schema_node(
                    root,
                    definition,
                    &format!("{path}.{keyword}.{name}"),
                    depth + 1,
                )?;
            }
        }
    }
    for keyword in ["minItems", "maxItems", "minLength", "maxLength"] {
        if object
            .get(keyword)
            .is_some_and(|value| value.as_u64().is_none())
        {
            return Err(format!("{path}.{keyword}: must be a nonnegative integer"));
        }
    }
    validate_bound_pair(object, "minItems", "maxItems", path)?;
    validate_bound_pair(object, "minLength", "maxLength", path)?;
    for keyword in ["minimum", "maximum"] {
        if object
            .get(keyword)
            .is_some_and(|value| value.as_f64().is_none())
        {
            return Err(format!("{path}.{keyword}: must be a number"));
        }
    }
    if let (Some(minimum), Some(maximum)) = (
        object.get("minimum").and_then(Value::as_f64),
        object.get("maximum").and_then(Value::as_f64),
    ) {
        if minimum > maximum {
            return Err(format!("{path}: minimum exceeds maximum"));
        }
    }
    if let Some(values) = object.get("enum") {
        if values.as_array().is_none_or(Vec::is_empty) {
            return Err(format!("{path}.enum: must be a non-empty array"));
        }
    }
    Ok(())
}

fn validate_value_node(
    root: &Value,
    schema: &Value,
    value: &Value,
    path: &str,
    depth: usize,
) -> Result<(), String> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(format!(
            "{path}: result value exceeds the schema depth limit"
        ));
    }
    let object = schema
        .as_object()
        .ok_or_else(|| format!("{path}: schema node is not an object"))?;
    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        let target = root
            .pointer(&reference[1..])
            .ok_or_else(|| format!("{path}: unresolved schema reference `{reference}`"))?;
        return validate_value_node(root, target, value, path, depth + 1);
    }
    for keyword in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = object.get(keyword).and_then(Value::as_array) {
            let matches = branches
                .iter()
                .filter(|branch| validate_value_node(root, branch, value, path, depth + 1).is_ok())
                .count();
            let valid = if keyword == "oneOf" {
                matches == 1
            } else if keyword == "anyOf" {
                matches >= 1
            } else {
                matches == branches.len()
            };
            if !valid {
                return Err(format!("{path}: value does not satisfy `{keyword}`"));
            }
        }
    }
    if object
        .get("const")
        .is_some_and(|expected| expected != value)
    {
        return Err(format!("{path}: value does not match `const`"));
    }
    if object
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|allowed| !allowed.contains(value))
    {
        return Err(format!("{path}: value is not in `enum`"));
    }
    if let Some(expected) = object.get("type") {
        let matches = match expected {
            Value::String(kind) => value_matches_type(value, kind),
            Value::Array(kinds) => kinds
                .iter()
                .filter_map(Value::as_str)
                .any(|kind| value_matches_type(value, kind)),
            _ => false,
        };
        if !matches {
            return Err(format!("{path}: value has the wrong JSON type"));
        }
    }
    if let Some(value) = value.as_object() {
        if let Some(required) = object.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !value.contains_key(name) {
                    return Err(format!("{path}: missing required field `{name}`"));
                }
            }
        }
        let properties = object.get("properties").and_then(Value::as_object);
        for (name, child) in value {
            if let Some(property) = properties.and_then(|properties| properties.get(name)) {
                validate_value_node(root, property, child, &format!("{path}.{name}"), depth + 1)?;
            } else if object.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(format!("{path}: unexpected field `{name}`"));
            } else if let Some(additional) = object
                .get("additionalProperties")
                .filter(|additional| additional.is_object())
            {
                validate_value_node(
                    root,
                    additional,
                    child,
                    &format!("{path}.{name}"),
                    depth + 1,
                )?;
            }
        }
    }
    if let (Some(items), Some(values)) = (object.get("items"), value.as_array()) {
        validate_count(object, values.len(), path, "Items")?;
        for (index, item) in values.iter().enumerate() {
            validate_value_node(root, items, item, &format!("{path}[{index}]"), depth + 1)?;
        }
    } else if let Some(values) = value.as_array() {
        validate_count(object, values.len(), path, "Items")?;
    }
    if let Some(text) = value.as_str() {
        validate_count(object, text.chars().count(), path, "Length")?;
    }
    if let Some(number) = value.as_f64() {
        if object
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
        {
            return Err(format!("{path}: value is below `minimum`"));
        }
        if object
            .get("maximum")
            .and_then(Value::as_f64)
            .is_some_and(|maximum| number > maximum)
        {
            return Err(format!("{path}: value exceeds `maximum`"));
        }
    }
    Ok(())
}

fn validate_bound_pair(
    object: &serde_json::Map<String, Value>,
    minimum: &str,
    maximum: &str,
    path: &str,
) -> Result<(), String> {
    if let (Some(min), Some(max)) = (
        object.get(minimum).and_then(Value::as_u64),
        object.get(maximum).and_then(Value::as_u64),
    ) {
        if min > max {
            return Err(format!("{path}: `{minimum}` exceeds `{maximum}`"));
        }
    }
    Ok(())
}

fn validate_count(
    schema: &serde_json::Map<String, Value>,
    count: usize,
    path: &str,
    suffix: &str,
) -> Result<(), String> {
    let minimum = format!("min{suffix}");
    if schema
        .get(&minimum)
        .and_then(Value::as_u64)
        .is_some_and(|minimum| count < minimum as usize)
    {
        return Err(format!("{path}: value is shorter than `{minimum}`"));
    }
    let maximum = format!("max{suffix}");
    if schema
        .get(&maximum)
        .and_then(Value::as_u64)
        .is_some_and(|maximum| count > maximum as usize)
    {
        return Err(format!("{path}: value exceeds `{maximum}`"));
    }
    Ok(())
}

fn is_supported_type(kind: &str) -> bool {
    matches!(
        kind,
        "null" | "boolean" | "object" | "array" | "number" | "integer" | "string"
    )
}

fn value_matches_type(value: &Value, kind: &str) -> bool {
    match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "number" => value.is_number(),
        "integer" => value
            .as_number()
            .is_some_and(|number| number.is_i64() || number.is_u64()),
        "string" => value.is_string(),
        _ => false,
    }
}
