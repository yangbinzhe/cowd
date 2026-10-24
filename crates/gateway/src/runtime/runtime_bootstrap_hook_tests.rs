use super::*;

fn write_plugin(root: &std::path::Path, name: &str, messages: [&str; 3]) {
    std::fs::create_dir_all(root.join(".cowd/plugins")).unwrap();
    std::fs::create_dir_all(root.join("hooks")).unwrap();
    for (script, message) in ["pre.sh", "post.sh", "failure.sh"]
        .into_iter()
        .zip(messages)
    {
        std::fs::write(
            root.join("hooks").join(script),
            format!("#!/bin/sh\nprintf '%s' '{message}'\n"),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            root.join("hooks").join(script),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
    }
    std::fs::write(
        root.join(".cowd/plugins/plugin.json"),
        serde_json::to_vec(&serde_json::json!({
            "name":name, "version":"1.0.0", "description":"local synthetic hook owner transfer",
            "hooks":{"PreToolUse":["./hooks/pre.sh"], "PostToolUse":["./hooks/post.sh"],
                     "PostToolUseFailure":["./hooks/failure.sh"]}
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn enabled_plugin_hooks_reach_the_original_runtime_owner_for_all_three_events() {
    let root = tempfile::tempdir().unwrap();
    let first = root
        .path()
        .join("first ' source $(printf wrong) with spaces");
    let second = root.path().join("second source with spaces");
    write_plugin(
        &first,
        "first",
        ["first pre", "first post", "first failure"],
    );
    write_plugin(
        &second,
        "second",
        ["second pre", "second post", "second failure"],
    );
    let mut manager = PluginManager::new(PluginManagerConfig::new(root.path().join("config")));
    // Local synthetic sources only; no external package or model installation.
    manager.install(first.to_str().unwrap()).unwrap();
    manager.install(second.to_str().unwrap()).unwrap();
    for disabled in [false, true] {
        if disabled {
            let registry = manager.plugin_registry().unwrap();
            let second_id = registry
                .plugins()
                .iter()
                .find(|plugin| plugin.metadata().name == "second")
                .unwrap()
                .metadata()
                .id
                .clone();
            manager.disable(&second_id).unwrap();
        }
        let registry = manager.plugin_registry().unwrap();
        let config = runtime_hook_config_from_plugin_hooks(registry.aggregated_hooks().unwrap());
        let runner = runtime::HookRunner::new(config);
        let results = [
            runner.run_pre_tool_use("read_file", "{}"),
            runner.run_post_tool_use("read_file", "{}", "original body", false),
            runner.run_post_tool_use_failure("read_file", "{}", "original failure"),
        ];
        for (result, (first, second)) in results.into_iter().zip([
            ("first pre", "second pre"),
            ("first post", "second post"),
            ("first failure", "second failure"),
        ]) {
            assert!(!result.is_failed() && !result.is_denied() && !result.is_cancelled());
            let expected = if disabled {
                vec![first]
            } else {
                vec![first, second]
            };
            assert_eq!(result.messages(), expected);
        }
        assert!(runner.execution_scope().is_closed());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn plugin_file_adapter_preserves_actual_non_executable_shell_script() {
    use std::{
        io::Write,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::PermissionsExt,
        },
    };
    let name = std::ffi::CString::new("cowd-owned-plugin-script").unwrap();
    // Anonymous kernel memory gives a real POSIX mode oracle. This mount
    // ignores chmod; no disk file is created outside the authorized Datas tree.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    assert!(
        fd >= 0,
        "owned anonymous script: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: memfd_create returned a new owned descriptor; File closes it.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.set_permissions(std::fs::Permissions::from_mode(0o644))
        .unwrap();
    file.write_all(b"printf 'nonexec-shell-original'\n")
        .unwrap();
    assert_eq!(file.metadata().unwrap().permissions().mode() & 0o777, 0o644);
    let path = format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd());
    let runner = runtime::HookRunner::new(runtime_hook_config_from_plugin_hooks(PluginHooks {
        pre_tool_use: vec![path],
        ..PluginHooks::default()
    }));
    let result = runner.run_pre_tool_use("read_file", "{}");
    assert!(!result.is_failed() && !result.is_denied() && !result.is_cancelled());
    assert_eq!(result.messages(), &["nonexec-shell-original"]);
    assert!(runner.execution_scope().is_closed());
    drop(file);
}
