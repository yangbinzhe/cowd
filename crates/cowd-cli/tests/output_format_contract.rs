use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn help_emits_json_when_requested() {
    let root = unique_temp_dir("help-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let parsed = assert_json_command(&root, &["--output-format", "json", "help"]);
    assert_eq!(parsed["kind"], "help");
    let message = parsed["message"].as_str().expect("help text");
    assert!(message.contains("Core commands:"));
    assert!(message.contains("cowd gateway start|stop|restart|status|doctor|logs|repair|open"));
    assert!(message.contains("cowd skills list|show|validate"));
    assert!(message.contains("Advanced local tools:"));
}

#[test]
fn version_emits_json_when_requested() {
    let root = unique_temp_dir("version-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let parsed = assert_json_command(&root, &["--output-format", "json", "version"]);
    assert_eq!(parsed["kind"], "version");
    assert_eq!(parsed["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn status_and_sandbox_emit_json_when_requested() {
    let root = unique_temp_dir("status-sandbox-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let status = assert_json_command(&root, &["--output-format", "json", "status"]);
    assert_eq!(status["kind"], "status");
    assert!(status["workspace"]["cwd"].as_str().is_some());

    let sandbox = assert_json_command(&root, &["--output-format", "json", "sandbox"]);
    assert_eq!(sandbox["kind"], "sandbox");
    assert!(sandbox["filesystem_mode"].as_str().is_some());
}

#[test]
fn inventory_commands_emit_structured_json_when_requested() {
    let root = unique_temp_dir("inventory-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let isolated_home = root.join("home");
    let isolated_config = root.join("config-home");
    let isolated_codex = root.join("codex-home");
    fs::create_dir_all(&isolated_home).expect("isolated home should exist");

    let agents = assert_json_command_with_env(
        &root,
        &["--output-format", "json", "agents"],
        &[
            ("HOME", isolated_home.to_str().expect("utf8 home")),
            (
                "COWD_CONFIG_HOME",
                isolated_config.to_str().expect("utf8 config home"),
            ),
            (
                "CODEX_HOME",
                isolated_codex.to_str().expect("utf8 codex home"),
            ),
        ],
    );
    assert_eq!(agents["kind"], "agents");
    assert_eq!(agents["action"], "list");
    assert_eq!(agents["count"], 0);
    assert_eq!(agents["summary"]["active"], 0);
    assert!(agents["agents"]
        .as_array()
        .expect("agents array")
        .is_empty());

    let mcp = assert_json_command(&root, &["--output-format", "json", "mcp"]);
    assert_eq!(mcp["kind"], "mcp");
    assert_eq!(mcp["action"], "list");

    let skills = assert_json_command(&root, &["--output-format", "json", "skills"]);
    assert_eq!(skills["kind"], "skills");
    assert_eq!(skills["action"], "list");
}

#[test]
fn agents_command_emits_structured_agent_entries_when_requested() {
    let root = unique_temp_dir("agents-json-populated");
    let workspace = root.join("workspace");
    let project_agents = workspace.join(".codex").join("agents");
    let home = root.join("home");
    let user_agents = home.join(".codex").join("agents");
    let isolated_config = root.join("config-home");
    let isolated_codex = root.join("codex-home");
    fs::create_dir_all(&workspace).expect("workspace should exist");
    write_agent(
        &project_agents,
        "planner",
        "Project planner",
        "gpt-5.4",
        "medium",
    );
    write_agent(
        &project_agents,
        "verifier",
        "Verification agent",
        "gpt-5.4-mini",
        "high",
    );
    write_agent(
        &user_agents,
        "planner",
        "User planner",
        "gpt-5.4-mini",
        "high",
    );

    let parsed = assert_json_command_with_env(
        &workspace,
        &["--output-format", "json", "agents"],
        &[
            ("HOME", home.to_str().expect("utf8 home")),
            (
                "COWD_CONFIG_HOME",
                isolated_config.to_str().expect("utf8 config home"),
            ),
            (
                "CODEX_HOME",
                isolated_codex.to_str().expect("utf8 codex home"),
            ),
        ],
    );

    assert_eq!(parsed["kind"], "agents");
    assert_eq!(parsed["action"], "list");
    assert_eq!(parsed["count"], 3);
    assert_eq!(parsed["summary"]["active"], 2);
    assert_eq!(parsed["summary"]["shadowed"], 1);
    assert_eq!(parsed["agents"][0]["name"], "planner");
    assert_eq!(parsed["agents"][0]["source"]["id"], "project_cowd");
    assert_eq!(parsed["agents"][0]["active"], true);
    assert_eq!(parsed["agents"][1]["name"], "verifier");
    assert_eq!(parsed["agents"][2]["name"], "planner");
    assert_eq!(parsed["agents"][2]["active"], false);
    assert_eq!(parsed["agents"][2]["shadowed_by"]["id"], "project_cowd");
}

#[test]
fn bootstrap_and_system_prompt_emit_json_when_requested() {
    let root = unique_temp_dir("bootstrap-system-prompt-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let plan = assert_json_command(&root, &["--output-format", "json", "bootstrap-plan"]);
    assert_eq!(plan["kind"], "bootstrap-plan");
    assert!(plan["phases"].as_array().expect("phases").len() > 1);

    let prompt = assert_json_command(&root, &["--output-format", "json", "system-prompt"]);
    assert_eq!(prompt["kind"], "system-prompt");
    assert!(prompt["message"]
        .as_str()
        .expect("prompt text")
        .contains("interactive agent"));
}

#[test]
fn dump_manifests_and_init_emit_json_when_requested() {
    let root = unique_temp_dir("manifest-init-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let upstream = write_upstream_fixture(&root);
    let manifests = assert_json_command(
        &root,
        &[
            "--output-format",
            "json",
            "dump-manifests",
            "--manifests-dir",
            upstream.to_str().expect("utf8 upstream"),
        ],
    );
    assert_eq!(manifests["kind"], "dump-manifests");
    assert_eq!(manifests["commands"], 1);
    assert_eq!(manifests["tools"], 1);

    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).expect("workspace should exist");
    let init = assert_json_command(&workspace, &["--output-format", "json", "init"]);
    assert_eq!(init["kind"], "init");
    assert!(workspace.join("CLAUDE.md").exists());
}

#[test]
fn doctor_and_resume_status_emit_json_when_requested() {
    let root = unique_temp_dir("doctor-resume-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let doctor = assert_json_command(&root, &["--output-format", "json", "doctor"]);
    assert_eq!(doctor["kind"], "doctor");
    assert!(doctor["message"].is_string());
    let summary = doctor["summary"].as_object().expect("doctor summary");
    assert!(summary["ok"].as_u64().is_some());
    assert!(summary["warnings"].as_u64().is_some());
    assert!(summary["failures"].as_u64().is_some());

    let checks = doctor["checks"].as_array().expect("doctor checks");
    assert_eq!(checks.len(), 7);
    let check_names = checks
        .iter()
        .map(|check| {
            assert!(check["status"].as_str().is_some());
            assert!(check["summary"].as_str().is_some());
            assert!(check["details"].is_array());
            check["name"].as_str().expect("doctor check name")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        check_names,
        vec![
            "auth",
            "config",
            "install source",
            "workspace",
            "sandbox",
            "system",
            "enterprise readiness"
        ]
    );
    let readiness = checks
        .iter()
        .find(|check| check["name"] == "enterprise readiness")
        .expect("enterprise readiness check");
    let components = readiness["components"]
        .as_object()
        .expect("readiness components");
    for component in ["webui", "tui", "session", "memory", "provider"] {
        assert!(
            components
                .get(component)
                .and_then(|value| value.get("status"))
                .and_then(|value| value.as_str())
                .is_some(),
            "missing readiness component {component}"
        );
    }

    let install_source = checks
        .iter()
        .find(|check| check["name"] == "install source")
        .expect("install source check");
    assert_eq!(
        install_source["official_repo"],
        "https://github.com/ultraworkers/cowd"
    );
    assert_eq!(install_source["deprecated_install"], "cargo install cowd");

    let workspace = checks
        .iter()
        .find(|check| check["name"] == "workspace")
        .expect("workspace check");
    assert!(workspace["cwd"].as_str().is_some());
    assert!(workspace["in_git_repo"].is_boolean());

    let sandbox = checks
        .iter()
        .find(|check| check["name"] == "sandbox")
        .expect("sandbox check");
    assert!(sandbox["filesystem_mode"].as_str().is_some());
    assert!(sandbox["enabled"].is_boolean());
    assert!(sandbox["fallback_reason"].is_null() || sandbox["fallback_reason"].is_string());

    let output = run_cowd(
        &root,
        &[
            "--output-format",
            "json",
            "--resume",
            "resume-json",
            "/status",
        ],
        &[],
    );
    assert_resume_slash_removed(&output);
}

#[test]
fn resumed_inventory_commands_are_tui_only() {
    let root = unique_temp_dir("resume-inventory-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let mcp = run_cowd(
        &root,
        &[
            "--output-format",
            "json",
            "--resume",
            "resume-inventory-json",
            "/mcp",
        ],
        &[],
    );
    assert_resume_slash_removed(&mcp);

    let skills = run_cowd(
        &root,
        &[
            "--output-format",
            "json",
            "--resume",
            "resume-inventory-json",
            "/skills",
        ],
        &[],
    );
    assert_resume_slash_removed(&skills);
}

#[test]
fn resumed_version_and_init_commands_are_tui_only() {
    let root = unique_temp_dir("resume-version-init-json");
    fs::create_dir_all(&root).expect("temp dir should exist");

    let version = run_cowd(
        &root,
        &[
            "--output-format",
            "json",
            "--resume",
            "resume-version-init-json",
            "/version",
        ],
        &[],
    );
    assert_resume_slash_removed(&version);

    let init = run_cowd(
        &root,
        &[
            "--output-format",
            "json",
            "--resume",
            "resume-version-init-json",
            "/init",
        ],
        &[],
    );
    assert_resume_slash_removed(&init);
}

fn assert_json_command(current_dir: &Path, args: &[&str]) -> Value {
    assert_json_command_with_env(current_dir, args, &[])
}

fn assert_json_command_with_env(current_dir: &Path, args: &[&str], envs: &[(&str, &str)]) -> Value {
    let output = run_cowd(current_dir, args, envs);
    assert!(
        output.status.success(),
        "stdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout should be valid json")
}

fn assert_resume_slash_removed(output: &Output) {
    assert!(
        !output.status.success(),
        "expected resume slash command to fail\nstdout:\n{}\n\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("was removed from the CLI surface"),
        "{stderr}"
    );
    assert!(
        stderr.contains("run slash commands inside the TUI"),
        "{stderr}"
    );
}

fn run_cowd(current_dir: &Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    // Pre-create minimal config to skip bootstrap when COWD_CONFIG_HOME is set
    for (key, value) in envs {
        if *key == "COWD_CONFIG_HOME" {
            let config_dir = Path::new(value);
            let _ = fs::create_dir_all(config_dir);
            let _ = fs::write(
                config_dir.join("config.yaml"),
                "model: \"sonnet\"\n\
                providers:\n  anthropic:\n    base_url: \"https://api.anthropic.com/v1\"\n    \
                api_key: \"test-key\"\n    models: [\"sonnet\"]\n    protocol: \"anthropic\"\n\
                permissions:\n  defaultMode: \"acceptEdits\"\n  allow: []\n  deny: []\n  ask: []\n\
                memory:\n  enabled: false\n\
                gateway:\n  enabled: false\n",
            );
            break;
        }
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_cowd"));
    command.current_dir(current_dir).args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("cowd should launch")
}

fn write_upstream_fixture(root: &Path) -> PathBuf {
    let upstream = root.join("cowd-code");
    let src = upstream.join("src");
    let entrypoints = src.join("entrypoints");
    fs::create_dir_all(&entrypoints).expect("upstream entrypoints dir should exist");
    fs::write(
        src.join("commands.ts"),
        "import FooCommand from './commands/foo'\n",
    )
    .expect("commands fixture should write");
    fs::write(
        src.join("tools.ts"),
        "import ReadTool from './tools/read'\n",
    )
    .expect("tools fixture should write");
    fs::write(
        entrypoints.join("cli.tsx"),
        "if (args[0] === '--version') {}\nstartupProfiler()\n",
    )
    .expect("cli fixture should write");
    upstream
}

fn write_agent(root: &Path, name: &str, description: &str, model: &str, reasoning: &str) {
    fs::create_dir_all(root).expect("agent root should exist");
    fs::write(
        root.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\ndescription = \"{description}\"\nmodel = \"{model}\"\nmodel_reasoning_effort = \"{reasoning}\"\n"
        ),
    )
    .expect("agent fixture should write");
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_millis();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "cowd-output-format-{label}-{}-{millis}-{counter}",
        std::process::id()
    ))
}
