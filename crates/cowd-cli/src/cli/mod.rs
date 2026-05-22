pub fn is_help_flag(value: &str) -> bool {
    value == "--help" || value == "-h"
}

pub fn normalize_permission_mode(mode: &str) -> Option<&'static str> {
    match mode {
        "read-only" | "readonly" | "read_only" => Some("read-only"),
        "workspace-write" | "workspacewrite" | "workspace_write" => Some("workspace-write"),
        "danger-full-access" | "dangerfull" | "dangerFullAccess" | "danger_full_access" => Some(
            "danger-full-access",
        ),
        "prompt" => Some("prompt"),
        _ => None,
    }
}

pub fn resolve_model_alias(model: &str) -> &str {
    match model {
        "opus" => "claude-opus-4-6",
        "sonnet" => "claude-sonnet-4-6",
        "haiku" => "claude-haiku-4-5-20251213",
        _ => model,
    }
}

pub fn permission_mode_from_label(mode: &str) -> runtime::PermissionMode {
    match mode {
        "read-only" => runtime::PermissionMode::ReadOnly,
        "workspace-write" => runtime::PermissionMode::WorkspaceWrite,
        "danger-full-access" => runtime::PermissionMode::DangerFullAccess,
        other => panic!("unsupported permission mode label: {other}"),
    }
}

pub fn permission_mode_from_resolved(mode: runtime::ResolvedPermissionMode) -> runtime::PermissionMode {
    match mode {
        runtime::ResolvedPermissionMode::ReadOnly => runtime::PermissionMode::ReadOnly,
        runtime::ResolvedPermissionMode::WorkspaceWrite => runtime::PermissionMode::WorkspaceWrite,
        runtime::ResolvedPermissionMode::DangerFullAccess => runtime::PermissionMode::DangerFullAccess,
    }
}

pub fn max_tokens_for_model(model: &str) -> u32 {
    if model.contains("opus") {
        32_000
    } else {
        64_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_help_flag_true() {
        assert!(is_help_flag("--help"));
        assert!(is_help_flag("-h"));
    }

    #[test]
    fn is_help_flag_false() {
        assert!(!is_help_flag("--model"));
    }

    #[test]
    fn normalize_permission_read_only() {
        assert_eq!(normalize_permission_mode("read-only"), Some("read-only"));
        assert_eq!(normalize_permission_mode("readonly"), Some("read-only"));
    }

    #[test]
    fn normalize_permission_unknown() {
        assert_eq!(normalize_permission_mode("solo"), None);
    }

    #[test]
    fn model_alias_sonnet() {
        assert_eq!(resolve_model_alias("sonnet"), "claude-sonnet-4-6");
    }

    #[test]
    fn model_alias_unknown_passthrough() {
        assert_eq!(resolve_model_alias("qwen-turbo"), "qwen-turbo");
    }

    #[test]
    fn permission_mode_label_read_only() {
        use runtime::PermissionMode;
        assert_eq!(
            permission_mode_from_label("read-only"),
            PermissionMode::ReadOnly
        );
    }

    #[test]
    fn max_tokens_opus() {
        assert_eq!(max_tokens_for_model("claude-opus-4-6"), 32_000);
    }

    #[test]
    fn max_tokens_other() {
        assert_eq!(max_tokens_for_model("claude-sonnet-4-6"), 64_000);
    }
}
