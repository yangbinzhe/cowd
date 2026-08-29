//! Shared strict parsing and validation helpers.

use super::*;

/// Convert a snake_case string to camelCase.
pub(super) fn to_camel_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize = false;
    for c in s.chars() {
        if c == '_' {
            capitalize = true;
        } else if capitalize {
            result.push(c.to_ascii_uppercase());
            capitalize = false;
        } else {
            result.push(c);
        }
    }
    result
}

/// Look up a config value, supporting both snake_case (preferred) and camelCase (deprecated).
/// If found via camelCase only, emits a deprecation warning.
pub(super) fn optional_string_dual<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<&'a str>, ConfigError> {
    // Try snake_case first.
    if let Some(_value) = object.get(snake_key) {
        return optional_string(object, snake_key, ctx);
    }

    // Convert snake_case to camelCase and try.
    let camel_key = to_camel_case(snake_key);
    if let Some(_value) = object.get(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_string(object, &camel_key, ctx);
    }

    Ok(None)
}

/// Look up a boolean config value, supporting both snake_case (preferred) and camelCase (deprecated).
pub(super) fn optional_bool_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<bool>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_bool(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_bool(object, &camel_key, ctx);
    }
    Ok(None)
}

/// Look up any config value by key, supporting snake_case (preferred), camelCase,
/// and PascalCase (deprecated). Emits a deprecation warning when a non-snake_case
/// key is used.
pub(super) fn find_key_dual<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Option<&'a JsonValue> {
    // Try snake_case first.
    if let Some(value) = object.get(snake_key) {
        return Some(value);
    }
    // Try camelCase (lowercase first letter).
    let camel_key = to_camel_case(snake_key);
    if let Some(value) = object.get(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return Some(value);
    }
    // Try PascalCase (uppercase first letter).
    let pascal_key = {
        let mut chars = camel_key.chars();
        match chars.next() {
            Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
            None => return None,
        }
    };
    if let Some(value) = object.get(&pascal_key) {
        tracing::warn!(
            "config key '{pascal_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return Some(value);
    }
    None
}

/// Look up a string array config value, supporting both snake_case (preferred)
/// and camelCase/PascalCase (deprecated).
pub(super) fn optional_string_array_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<Vec<String>>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_string_array(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_string_array(object, &camel_key, ctx);
    }
    let pascal_key = {
        let mut chars = camel_key.chars();
        match chars.next() {
            Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
            None => return Ok(None),
        }
    };
    if object.contains_key(&pascal_key) {
        tracing::warn!(
            "config key '{pascal_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_string_array(object, &pascal_key, ctx);
    }
    Ok(None)
}

/// Look up a u32 config value, supporting both snake_case (preferred) and camelCase (deprecated).
pub(super) fn optional_u32_dual(
    object: &BTreeMap<String, JsonValue>,
    snake_key: &str,
    ctx: &str,
) -> Result<Option<u32>, ConfigError> {
    if object.contains_key(snake_key) {
        return optional_u32(object, snake_key, ctx);
    }
    let camel_key = to_camel_case(snake_key);
    if object.contains_key(&camel_key) {
        tracing::warn!(
            "config key '{camel_key}' is deprecated, use '{snake_key}' instead (in {ctx})"
        );
        return optional_u32(object, &camel_key, ctx);
    }
    Ok(None)
}

pub(super) fn deep_merge_objects(
    target: &mut BTreeMap<String, JsonValue>,
    source: &BTreeMap<String, JsonValue>,
) {
    for (key, value) in source {
        match (target.get_mut(key), value) {
            (Some(JsonValue::Object(existing)), JsonValue::Object(incoming)) => {
                deep_merge_objects(existing, incoming);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}
