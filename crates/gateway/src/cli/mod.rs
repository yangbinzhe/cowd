const PROMPT_PERMISSION_MODE: &str = "prompt";

pub fn is_help_flag(value: &str) -> bool {
    value == "--help" || value == "-h"
}

pub fn normalize_permission_mode(mode: &str) -> Option<&'static str> {
    match mode {
        "read-only" | "readonly" | "read_only" => Some("read-only"),
        "workspace-write" | "workspacewrite" | "workspace_write" => Some("workspace-write"),
        "danger-full-access" | "dangerfull" | "dangerFullAccess" | "danger_full_access" => {
            Some("danger-full-access")
        }
        value if value == PROMPT_PERMISSION_MODE => Some(PROMPT_PERMISSION_MODE),
        _ => None,
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

pub fn permission_mode_from_resolved(
    mode: runtime::ResolvedPermissionMode,
) -> runtime::PermissionMode {
    match mode {
        runtime::ResolvedPermissionMode::ReadOnly => runtime::PermissionMode::ReadOnly,
        runtime::ResolvedPermissionMode::WorkspaceWrite => runtime::PermissionMode::WorkspaceWrite,
        runtime::ResolvedPermissionMode::DangerFullAccess => {
            runtime::PermissionMode::DangerFullAccess
        }
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
    fn permission_mode_label_read_only() {
        use runtime::PermissionMode;
        assert_eq!(
            permission_mode_from_label("read-only"),
            PermissionMode::ReadOnly
        );
    }
}
