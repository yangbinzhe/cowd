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
        "send_user_message",
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

    let enter = execute_tool("enter_plan_mode", &json!({})).expect("enter plan mode");
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

    let exit = execute_tool("exit_plan_mode", &json!({})).expect("exit plan mode");
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

    let enter = execute_tool("enter_plan_mode", &json!({})).expect("enter plan mode");
    let enter_output: serde_json::Value = serde_json::from_str(&enter).expect("json");
    assert_eq!(enter_output["previousLocalMode"], serde_json::Value::Null);
    assert_eq!(enter_output["currentLocalMode"], "read-only");

    let exit = execute_tool("exit_plan_mode", &json!({})).expect("exit plan mode");
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
fn repl_executes_python_code() {
    let _guard = env_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let result = execute_tool(
        "repl",
        &json!({"language": "python", "code": "print(1 + 1)", "timeout_ms": 5_000}),
    )
    .expect("REPL should succeed");
    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["language"], "python");
    assert_eq!(output["exitCode"], 0);
    assert!(output["stdout"].as_str().expect("stdout").contains('2'));
}

#[test]
fn given_empty_code_when_repl_then_rejects_with_error() {
    let result = execute_tool("repl", &json!({"language": "python", "code": "   "}));

    let error = result.expect_err("empty REPL code should fail");
    assert!(error.contains("code must not be empty"));
}

#[test]
fn given_unsupported_language_when_repl_then_rejects_with_error() {
    let result = execute_tool("repl", &json!({"language": "ruby", "code": "puts 1"}));

    let error = result.expect_err("unsupported REPL language should fail");
    assert!(error.contains("unsupported REPL language: ruby"));
}

#[test]
fn given_timeout_ms_when_repl_blocks_then_returns_timeout_error() {
    let _guard = crate::test_process_environment_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let result = execute_tool(
        "repl",
        &json!({
            "language": "python",
            "code": "import time\ntime.sleep(1)",
            "timeout_ms": 10
        }),
    );

    let error = result.expect_err("timed out REPL execution should fail");
    assert!(error.contains("REPL execution exceeded timeout of 10 ms"));
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
        "power_shell",
        &json!({"command": "Write-Output hello", "timeout_ms": 1000}),
    )
    .expect("PowerShell should succeed");

    let background = execute_tool(
        "power_shell",
        &json!({"command": "Write-Output hello", "run_in_background": true}),
    )
    .expect_err("PID-only background execution is not a model capability (S-03)");

    std::env::set_var("PATH", original_path);
    let _ = std::fs::remove_dir_all(dir);

    let output: serde_json::Value = serde_json::from_str(&result).expect("json");
    assert_eq!(output["stdout"], "pwsh:Write-Output hello");
    assert!(output["stderr"].as_str().expect("stderr").is_empty());
    assert!(background.contains("S-03"));
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

    let err = execute_tool("power_shell", &json!({"command": "Write-Output hello"}))
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
