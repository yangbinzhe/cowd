use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::execute_tool_for_test as execute_tool;
use crate::lane_events::LaneEventName;
use crate::permissions::PermissionMode;
use crate::{mvp_tool_specs, permission_mode_from_plugin, ToolCatalog};
use serde_json::json;

fn env_lock() -> &'static Mutex<()> {
    crate::test_process_environment_lock()
}

fn temp_path(name: &str) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("cowd-tools-{unique}-{name}"))
}

fn make_tree_writable_for_test(root: &Path) {
    if !root.exists() {
        return;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(root).expect("tree metadata");
        let mode = if metadata.is_dir() { 0o700 } else { 0o600 };
        fs::set_permissions(root, fs::Permissions::from_mode(mode)).expect("tree permissions");
    }
    if root.is_dir() {
        for entry in fs::read_dir(root).expect("tree entries") {
            make_tree_writable_for_test(&entry.expect("tree entry").path());
        }
    }
}

fn execute_in_workspace(
    root: &Path,
    name: &str,
    input: &serde_json::Value,
) -> Result<String, String> {
    let host = crate::ToolHost::builtin("tools-test-workspace", root);
    super::execute_with_lease(&host.pin_snapshot(), name, input)
}

// Test shards intentionally share this module scope and its fixtures.
include!("dispatch.rs");
include!("builtin.rs");
