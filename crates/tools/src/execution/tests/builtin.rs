// Interactive and process-oriented builtin adapter coverage.
#[test]
fn sleep_waits_and_reports_duration() {
    let started = std::time::Instant::now();
    let result = execute_tool("sleep", &json!({"duration_ms": 20})).expect("Sleep should succeed");
    let elapsed = started.elapsed();
    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["duration_ms"], 20);
    assert!(output["message"]
        .as_str()
        .expect("message")
        .contains("Slept for 20ms"));
    assert!(elapsed >= Duration::from_millis(15));
}

#[test]
fn given_excessive_duration_when_sleep_then_rejects_with_error() {
    let result = execute_tool("sleep", &json!({"duration_ms": 999_999_999_u64}));
    let error = result.expect_err("excessive sleep should fail");
    assert!(error.contains("exceeds maximum allowed sleep"));
}

#[test]
fn given_zero_duration_when_sleep_then_succeeds() {
    let result =
        execute_tool("sleep", &json!({"duration_ms": 0})).expect("0ms sleep should succeed");
    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["duration_ms"], 0);
}

#[test]
fn brief_returns_sent_message_and_attachment_metadata() {
    let attachment = std::env::temp_dir().join(format!(
        "cowd-brief-{}.png",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::write(&attachment, b"png-data").expect("write attachment");
    let root = attachment.parent().expect("attachment parent");

    let result = execute_in_workspace(
        root,
        "ask",
        &json!({
            "message": "hello user",
            "attachments": [attachment.display().to_string()],
            "status": "normal"
        }),
    )
    .expect("SendUserMessage should succeed");

    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["message"], "hello user");
    assert!(output["sentAt"].as_str().is_some());
    assert_eq!(output["attachments"][0]["isImage"], true);
    let _ = std::fs::remove_file(attachment);
}

#[test]
fn config_reads_and_writes_supported_values() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = std::env::temp_dir().join(format!(
        "cowd-config-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let home = root.join("home");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(home.join(".cowd")).expect("home dir");
    std::fs::create_dir_all(cwd.join(".cowd")).expect("cwd dir");
    std::fs::write(
        home.join(".cowd").join("config.yaml"),
        r#"{"verbose":false}"#,
    )
    .expect("write global config");

    let original_home = std::env::var("HOME").ok();
    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_var("HOME", &home);
    std::env::remove_var("COWD_CONFIG_HOME");
    std::env::set_current_dir(&cwd).expect("set cwd");

    let get = execute_tool("config", &json!({"setting": "verbose"})).expect("get config");
    let get_output: serde_json::Value = serde_json::from_str(&get).expect("json");
    assert_eq!(get_output["value"], false);

    let set = execute_tool(
        "config",
        &json!({"setting": "permissions.default_mode", "value": "read-only"}),
    )
    .expect("set config");
    let set_output: serde_json::Value = serde_json::from_str(&set).expect("json");
    assert_eq!(set_output["operation"], "set");
    assert_eq!(set_output["newValue"], "read-only");

    let invalid = execute_tool(
        "config",
        &json!({"setting": "permissions.default_mode", "value": "bogus"}),
    )
    .expect_err("invalid config value should error");
    assert!(invalid.contains("Invalid value"));

    let unknown =
        execute_tool("config", &json!({"setting": "nope"})).expect("unknown setting result");
    let unknown_output: serde_json::Value = serde_json::from_str(&unknown).expect("json");
    assert_eq!(unknown_output["success"], false);

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    match original_home {
        Some(value) => std::env::set_var("HOME", value),
        None => std::env::remove_var("HOME"),
    }
    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn enter_and_exit_plan_mode_round_trip_existing_local_override() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = std::env::temp_dir().join(format!(
        "cowd-plan-mode-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let home = root.join("home");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(home.join(".cowd")).expect("home dir");
    std::fs::create_dir_all(cwd.join(".cowd")).expect("cwd dir");
    std::fs::write(
        cwd.join(".cowd").join("config.local.yaml"),
        r#"{"permissions":{"default_mode":"workspace-write"}}"#,
    )
    .expect("write local config");

    let original_home = std::env::var("HOME").ok();
    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_var("HOME", &home);
    std::env::remove_var("COWD_CONFIG_HOME");
    std::env::set_current_dir(&cwd).expect("set cwd");

    let enter = execute_tool("plan_mode", &json!({"action": "enter"})).expect("enter plan mode");
    let enter_output: serde_json::Value = serde_json::from_str(&enter).expect("json");
    assert_eq!(enter_output["changed"], true);
    assert_eq!(enter_output["managed"], true);
    assert_eq!(enter_output["previousLocalMode"], "workspace-write");
    assert_eq!(enter_output["currentLocalMode"], "read-only");

    let local_settings = std::fs::read_to_string(cwd.join(".cowd").join("config.local.yaml"))
        .expect("local config after enter");
    assert!(local_settings.contains(r#""default_mode": "read-only""#));
    let state =
        std::fs::read_to_string(cwd.join(".cowd").join("tool-state").join("plan-mode.json"))
            .expect("plan mode state");
    assert!(state.contains(r#""hadLocalOverride": true"#));
    assert!(state.contains(r#""previousLocalMode": "workspace-write""#));

    let exit = execute_tool("plan_mode", &json!({"action": "exit"})).expect("exit plan mode");
    let exit_output: serde_json::Value = serde_json::from_str(&exit).expect("json");
    assert_eq!(exit_output["changed"], true);
    assert_eq!(exit_output["managed"], false);
    assert_eq!(exit_output["previousLocalMode"], "workspace-write");
    assert_eq!(exit_output["currentLocalMode"], "workspace-write");

    let local_settings = std::fs::read_to_string(cwd.join(".cowd").join("config.local.yaml"))
        .expect("local settings after exit");
    assert!(local_settings.contains(r#""default_mode": "workspace-write""#));
    assert!(!cwd
        .join(".cowd")
        .join("tool-state")
        .join("plan-mode.json")
        .exists());

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    match original_home {
        Some(value) => std::env::set_var("HOME", value),
        None => std::env::remove_var("HOME"),
    }
    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn exit_plan_mode_clears_override_when_enter_created_it_from_empty_local_state() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let root = std::env::temp_dir().join(format!(
        "cowd-plan-mode-empty-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    let home = root.join("home");
    let cwd = root.join("cwd");
    std::fs::create_dir_all(home.join(".cowd")).expect("home dir");
    std::fs::create_dir_all(cwd.join(".cowd")).expect("cwd dir");

    let original_home = std::env::var("HOME").ok();
    let original_config_home = std::env::var("COWD_CONFIG_HOME").ok();
    let original_dir = std::env::current_dir().expect("cwd");
    std::env::set_var("HOME", &home);
    std::env::remove_var("COWD_CONFIG_HOME");
    std::env::set_current_dir(&cwd).expect("set cwd");

    let enter = execute_tool("plan_mode", &json!({"action": "enter"})).expect("enter plan mode");
    let enter_output: serde_json::Value = serde_json::from_str(&enter).expect("json");
    assert_eq!(enter_output["previousLocalMode"], serde_json::Value::Null);
    assert_eq!(enter_output["currentLocalMode"], "read-only");

    let exit = execute_tool("plan_mode", &json!({"action": "exit"})).expect("exit plan mode");
    let exit_output: serde_json::Value = serde_json::from_str(&exit).expect("json");
    assert_eq!(exit_output["changed"], true);
    assert_eq!(exit_output["currentLocalMode"], serde_json::Value::Null);

    let local_settings = std::fs::read_to_string(cwd.join(".cowd").join("config.local.yaml"))
        .expect("local config after exit");
    let local_settings_json: serde_json::Value =
        serde_json::from_str(&local_settings).expect("valid config json");
    assert_eq!(
        local_settings_json.get("permissions"),
        None,
        "permissions override should be removed on exit"
    );
    assert!(!cwd
        .join(".cowd")
        .join("tool-state")
        .join("plan-mode.json")
        .exists());

    std::env::set_current_dir(&original_dir).expect("restore cwd");
    match original_home {
        Some(value) => std::env::set_var("HOME", value),
        None => std::env::remove_var("HOME"),
    }
    match original_config_home {
        Some(value) => std::env::set_var("COWD_CONFIG_HOME", value),
        None => std::env::remove_var("COWD_CONFIG_HOME"),
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn structured_output_echoes_input_payload() {
    let result = execute_tool(
        "structured_output",
        &json!({"ok": true, "items": [1, 2, 3]}),
    )
    .expect("StructuredOutput should succeed");
    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["data"], "Structured output provided successfully");
    assert_eq!(output["structured_output"]["ok"], true);
    assert_eq!(output["structured_output"]["items"][1], 2);
}

#[test]
fn given_empty_payload_when_structured_output_then_rejects_with_error() {
    let result = execute_tool("structured_output", &json!({}));
    let error = result.expect_err("empty payload should fail");
    assert!(error.contains("must not be empty"));
}

#[test]
fn powershell_runs_via_stub_shell() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!(
            "cowd-pwsh-bin-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
    std::fs::create_dir_all(&dir).expect("create dir");
    let script = dir.join("pwsh");
    std::fs::write(
        &script,
        r#"#!/bin/sh
while [ "$1" != "-Command" ] && [ $# -gt 0 ]; do shift; done
shift
printf 'pwsh:%s' "$1"
"#,
    )
    .expect("write script");
    std::process::Command::new("/bin/chmod")
        .arg("+x")
        .arg(&script)
        .status()
        .expect("chmod");
    let original_path = std::env::var("PATH").unwrap_or_default();
    std::env::set_var("PATH", format!("{}:{}", dir.display(), original_path));

    let result = execute_tool(
        "bash",
        &json!({"shell": "powershell", "command": "Write-Output hello", "timeout_ms": 1000}),
    )
    .expect("PowerShell should succeed");

    let background = execute_tool(
        "bash",
        &json!({"shell": "powershell", "command": "Write-Output hello", "run_in_background": true}),
    )
    .expect("background powershell execution is a model capability");
    let background_output: serde_json::Value = serde_json::from_str(&background).expect("json");
    assert!(background_output["backgroundTaskId"]
        .as_str()
        .is_some_and(|id| id.starts_with("bg-")));

    std::env::set_var("PATH", original_path);
    let _ = std::fs::remove_dir_all(dir);

    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["stdout"], "pwsh:Write-Output hello");
    assert!(output["stderr"].as_str().expect("stderr").is_empty());
}

#[test]
fn powershell_errors_when_shell_is_missing() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let original_path = std::env::var("PATH").unwrap_or_default();
    let empty_dir = std::env::temp_dir().join(format!(
        "cowd-empty-bin-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&empty_dir).expect("create empty dir");
    std::env::set_var("PATH", empty_dir.display().to_string());

    let err = execute_tool("bash", &json!({"shell": "powershell", "command": "Write-Output hello"}))
        .expect_err("PowerShell should fail when shell is missing");

    std::env::set_var("PATH", original_path);
    let _ = std::fs::remove_dir_all(empty_dir);

    assert!(err.contains("PowerShell executable not found"));
}

#[test]
fn builtin_bash_executes_inside_the_pinned_tool_host() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let registry = ToolCatalog::builtin();
    let result = registry
        .execute("bash", &json!({ "command": "printf 'ok'" }))
        .expect("bash should execute after Runtime authorization");
    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["stdout"], "ok");
}

/// PLAN B2: a windowed read must not pay a full file read to fingerprint.
///
/// The old `file_fingerprint` hashed every byte before the read flight, so a
/// cache *hit* still cost `O(file size)`; a multi-file batch therefore paid one
/// full read per file per round. A windowed read now fingerprints
/// `len + mtime + sha256(head 8K) + sha256(tail 8K)`. Only `complete` (EOF)
/// reads keep the full content hash, because an EOF receipt must be bound to
/// the exact bytes it claims to have read end to end.
#[test]
fn windowed_fingerprint_is_bounded_but_detects_edge_and_length_changes() {
    let dir = std::env::temp_dir().join(format!("cowd-fingerprint-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("big.rs");
    let mut body = String::from("fn head() {}\n");
    body.push_str(&"// filler line to make the file large\n".repeat(20_000));
    body.push_str("fn tail() {}\n");
    std::fs::write(&path, &body).expect("write big file");
    let fp = super::super::executor::file_fingerprint;

    let windowed = fp(&path, false);
    let exact = fp(&path, true);
    assert_ne!(windowed, exact, "exact reads must keep a distinct fingerprint");
    assert!(windowed.starts_with("file:"), "shape changed: {windowed}");
    assert_eq!(windowed, fp(&path, false), "an unchanged file must fingerprint stably");

    let mut edited = body.clone();
    edited.replace_range(0..5, "fn HEAD");
    std::fs::write(&path, &edited).expect("write edited head");
    assert_ne!(windowed, fp(&path, false), "a head edit must invalidate");

    let mut tail_edited = body.clone();
    let cut = tail_edited.len() - 6;
    tail_edited.replace_range(cut.., "TAIL()\n");
    std::fs::write(&path, &tail_edited).expect("write edited tail");
    assert_ne!(windowed, fp(&path, false), "a tail edit must invalidate");

    // A rewrite always changes mtime, which is part of the fingerprint: any
    // in-place rewrite invalidates the cache even when the bytes match.
    let other = dir.join("len.rs");
    std::fs::write(&other, &body).expect("write baseline");
    let baseline = fp(&other, false);
    let mut shorter = body.clone();
    shorter.truncate(shorter.len() - 4_096);
    std::fs::write(&other, &shorter).expect("write shorter");
    assert_ne!(baseline, fp(&other, false), "a length change must invalidate");

    // A file smaller than one fingerprint edge must hash exactly: head+tail
    // covers the whole body, so there is no middle-of-file blind spot.
    let small = dir.join("small.rs");
    std::fs::write(&small, "fn small() {}\n").expect("write small");
    assert_eq!(fp(&small, false), fp(&small, true), "small file must hash exactly");

    assert!(fp(&dir.join("nope.rs"), false).starts_with("missing:"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// PLAN E2: the orchestration vocabulary is the whole built-in catalog.
#[test]
fn orchestrate_can_dispatch_the_whole_builtin_catalog_but_never_itself() {
    let dispatchable = super::super::executor::is_allowed_orchestration_tool;
    let catalog: Vec<String> = crate::mvp_tool_specs()
        .into_iter()
        .map(|spec| spec.name.to_string())
        .collect();

    assert_eq!(catalog.len(), 28, "the catalog size is the contract");
    let catalog: Vec<String> = catalog
        .into_iter()
        .filter(|name| name != "tool_orchestrate")
        .collect();
    assert_eq!(catalog.len(), 27, "the catalog minus the orchestrator itself");
    for name in &catalog {
        assert!(dispatchable(name), "`{name}` is in the catalog, so it must be composable");
    }
    assert!(!dispatchable("tool_orchestrate"), "orchestration must never nest");
    assert!(!dispatchable("not_a_tool"), "an unknown name must fail closed");
}
