//! Legacy JSONL import gates.
//!
//! SQLite is the runtime source of truth. JSONL readers are kept only for
//! explicit, operator-triggered import/recovery paths.

const LEGACY_JSONL_IMPORT_ENV: &str = "COWD_ENABLE_LEGACY_JSONL_SESSION_IMPORT";

pub(crate) fn legacy_jsonl_session_import_enabled() -> bool {
    legacy_jsonl_session_import_enabled_from(std::env::var(LEGACY_JSONL_IMPORT_ENV).ok().as_deref())
}

fn legacy_jsonl_session_import_enabled_from(value: Option<&str>) -> bool {
    value
        .map(|value| {
            let value = value.trim();
            value == "1" || value.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_jsonl_import_gate_defaults_to_disabled() {
        assert!(!legacy_jsonl_session_import_enabled_from(None));
        assert!(!legacy_jsonl_session_import_enabled_from(Some("")));
        assert!(!legacy_jsonl_session_import_enabled_from(Some("0")));
        assert!(!legacy_jsonl_session_import_enabled_from(Some("false")));
    }

    #[test]
    fn legacy_jsonl_import_gate_accepts_explicit_enable_values() {
        assert!(legacy_jsonl_session_import_enabled_from(Some("1")));
        assert!(legacy_jsonl_session_import_enabled_from(Some("true")));
        assert!(legacy_jsonl_session_import_enabled_from(Some(" TRUE ")));
    }
}
