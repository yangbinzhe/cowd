use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PROTECTED_ENV: &[&str] = &[
    "HOME",
    "COWD_CONFIG_HOME",
    "COWD_GATEWAY_URL",
    "COWD_API_TOKEN",
    "COWD_DISABLE_DAEMON_AUTOSTART",
    "COWD_SESSION_ID",
    "TERM",
    "PATH",
    "LANG",
];

#[derive(Clone)]
pub struct TuiLaunchConfig {
    pub name: String,
    pub cowd_bin: PathBuf,
    pub workspace: PathBuf,
    pub config_home: PathBuf,
    pub home_dir: PathBuf,
    pub gateway_url: String,
    pub api_token: String,
    pub session_id: String,
    pub width: u16,
    pub height: u16,
    pub extra_env: BTreeMap<String, String>,
}

impl TuiLaunchConfig {
    pub fn from_env(name: &str) -> Result<Self> {
        let nonce = timestamp_millis();
        let root = std::env::temp_dir().join(format!(
            "cowd-interactive-{}-{}-{nonce}",
            sanitize(name),
            std::process::id()
        ));
        Ok(Self {
            name: name.to_string(),
            cowd_bin: std::env::var_os("COWD_BIN")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("cowd")),
            workspace: std::env::current_dir().context("resolve interactive workspace")?,
            config_home: root.join("config"),
            home_dir: root.join("home"),
            gateway_url: std::env::var("COWD_GATEWAY_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8642".to_string()),
            api_token: std::env::var("COWD_API_TOKEN").unwrap_or_default(),
            session_id: format!("interactive-{}-{nonce}", sanitize(name)),
            width: 120,
            height: 40,
            extra_env: BTreeMap::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactEntry {
    sequence: u64,
    captured_at_ms: u128,
    kind: String,
    step_id: String,
    viewport: Option<Viewport>,
    path: String,
    acceptance_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Viewport {
    pub width: u16,
    pub height: u16,
}

pub struct TuiSession {
    session: String,
    tmux_label: String,
    runtime_session_id: String,
    artifact_dir: PathBuf,
    width: Cell<u16>,
    height: Cell<u16>,
    sequence: Cell<u64>,
    artifacts: RefCell<Vec<ArtifactEntry>>,
    closed: bool,
}

impl TuiSession {
    pub fn new(config: TuiLaunchConfig) -> Result<Self> {
        validate_launch_config(&config)?;
        std::fs::create_dir_all(&config.workspace)?;
        std::fs::create_dir_all(&config.config_home)?;
        std::fs::create_dir_all(&config.home_dir)?;
        let artifact_dir = std::env::var_os("COWD_INTERACTIVE_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| config.config_home.join("artifacts"))
            .join(sanitize(&config.name));
        std::fs::create_dir_all(artifact_dir.join("panes"))?;
        std::fs::create_dir_all(artifact_dir.join("history"))?;
        std::fs::create_dir_all(artifact_dir.join("sidecars"))?;

        let session = owned_session_name(&config.name);
        let tmux_label = std::env::var("COWD_INTERACTIVE_TMUX_LABEL")
            .unwrap_or_else(|_| format!("cowd-it-{}", sanitize(&session)));
        let mut command = vec![
            "env".to_string(),
            "-i".to_string(),
            format!("HOME={}", config.home_dir.display()),
            format!("COWD_CONFIG_HOME={}", config.config_home.display()),
            format!("COWD_GATEWAY_URL={}", config.gateway_url),
            format!("COWD_API_TOKEN={}", config.api_token),
            "COWD_DISABLE_DAEMON_AUTOSTART=1".to_string(),
            format!("COWD_SESSION_ID={}", config.session_id),
            "TERM=xterm-256color".to_string(),
            format!(
                "PATH={}",
                std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_string())
            ),
            format!(
                "LANG={}",
                std::env::var("LANG").unwrap_or_else(|_| "C.UTF-8".to_string())
            ),
        ];
        for (key, value) in &config.extra_env {
            command.push(format!("{key}={value}"));
        }
        command.push(config.cowd_bin.display().to_string());
        command.push("--session".to_string());
        command.push(config.session_id.clone());
        let shell_command = command
            .iter()
            .map(|part| shell_quote(part))
            .collect::<Vec<_>>()
            .join(" ");
        let args = vec![
            "new-session".to_string(),
            "-d".to_string(),
            "-s".to_string(),
            session.clone(),
            "-c".to_string(),
            config.workspace.display().to_string(),
            "-x".to_string(),
            config.width.to_string(),
            "-y".to_string(),
            config.height.to_string(),
            "--".to_string(),
            "sh".to_string(),
        ];

        let output = tmux_output(&tmux_label, &args)?;
        ensure_success("tmux new-session", &output)?;
        let output = tmux_output(
            &tmux_label,
            &[
                "set-option".to_string(),
                "-t".to_string(),
                session.clone(),
                "remain-on-exit".to_string(),
                "on".to_string(),
            ],
        )?;
        ensure_success("tmux remain-on-exit", &output)?;
        let output = tmux_output(
            &tmux_label,
            &[
                "respawn-pane".to_string(),
                "-k".to_string(),
                "-t".to_string(),
                session.clone(),
                "-c".to_string(),
                config.workspace.display().to_string(),
                "--".to_string(),
                shell_command,
            ],
        )?;
        ensure_success("tmux respawn isolated TUI", &output)?;

        let instance = Self {
            session,
            tmux_label,
            runtime_session_id: config.session_id.clone(),
            artifact_dir,
            width: Cell::new(config.width),
            height: Cell::new(config.height),
            sequence: Cell::new(0),
            artifacts: RefCell::new(Vec::new()),
            closed: false,
        };
        instance.write_scenario_manifest(&config)?;
        Ok(instance)
    }

    pub fn send(&self, text: &str) -> Result<()> {
        self.run_tmux(
            "send literal keys",
            &["send-keys", "-t", &self.session, "-l", text],
        )
    }

    pub fn enter(&self) -> Result<()> {
        self.run_tmux("send Enter", &["send-keys", "-t", &self.session, "Enter"])
    }

    pub fn send_key(&self, key: &str) -> Result<()> {
        self.run_tmux("send key", &["send-keys", "-t", &self.session, key])
    }

    pub fn send_ctrl(&self, ch: char) -> Result<()> {
        self.send_key(&format!("C-{ch}"))
    }

    pub fn send_alt(&self, key: &str) -> Result<()> {
        self.send_key(&format!("M-{key}"))
    }

    #[allow(dead_code)]
    pub fn send_shift_enter(&self) -> Result<()> {
        self.send_key("S-Enter")
    }

    pub fn resize(&self, width: u16, height: u16) -> Result<()> {
        self.run_tmux(
            "resize window",
            &[
                "resize-window",
                "-t",
                &self.session,
                "-x",
                &width.to_string(),
                "-y",
                &height.to_string(),
            ],
        )?;
        let actual = self.actual_viewport()?;
        if actual.width != width || actual.height != height {
            return Err(anyhow!(
                "tmux resize mismatch: requested {width}x{height}, observed {}x{}",
                actual.width,
                actual.height
            ));
        }
        self.width.set(actual.width);
        self.height.set(actual.height);
        std::thread::sleep(Duration::from_millis(100));
        self.write_sidecar(
            "resize",
            &[],
            json!({
                "viewport": {"width": actual.width, "height": actual.height},
                "assertions": [],
                "status": "captured_not_asserted"
            }),
        )?;
        Ok(())
    }

    /// Capture only the current visible viewport. Layout assertions must use
    /// this method so content left in scrollback cannot produce a false pass.
    pub fn capture(&self) -> Result<String> {
        self.capture_step("viewport", &[])
    }

    pub fn capture_step(&self, step_id: &str, acceptance_ids: &[&str]) -> Result<String> {
        let content = self.capture_viewport_raw()?;
        let viewport = self.actual_viewport()?;
        self.width.set(viewport.width);
        self.height.set(viewport.height);
        let sequence = self.next_sequence();
        let timestamp = timestamp_millis();
        let safe_step = sanitize(step_id);
        let path = self.artifact_dir.join("panes").join(format!(
            "{sequence:04}-{timestamp}-{safe_step}-{}x{}.txt",
            viewport.width, viewport.height
        ));
        std::fs::write(&path, &content)?;
        self.record_artifact(ArtifactEntry {
            sequence,
            captured_at_ms: timestamp,
            kind: "viewport".to_string(),
            step_id: step_id.to_string(),
            viewport: Some(viewport),
            path: relative_artifact_path(&self.artifact_dir, &path),
            acceptance_ids: acceptance_ids.iter().map(|id| (*id).to_string()).collect(),
        })?;
        Ok(content)
    }

    pub fn capture_history(&self, step_id: &str) -> Result<String> {
        let output = tmux_output(
            &self.tmux_label,
            &[
                "capture-pane".to_string(),
                "-t".to_string(),
                self.session.clone(),
                "-p".to_string(),
                "-S".to_string(),
                "-".to_string(),
            ],
        )?;
        ensure_success("capture diagnostic history", &output)?;
        let content = String::from_utf8_lossy(&output.stdout).to_string();
        let sequence = self.next_sequence();
        let timestamp = timestamp_millis();
        let path = self.artifact_dir.join("history").join(format!(
            "{sequence:04}-{timestamp}-{}.txt",
            sanitize(step_id)
        ));
        std::fs::write(&path, &content)?;
        self.record_artifact(ArtifactEntry {
            sequence,
            captured_at_ms: timestamp,
            kind: "history_diagnostic".to_string(),
            step_id: step_id.to_string(),
            viewport: None,
            path: relative_artifact_path(&self.artifact_dir, &path),
            acceptance_ids: Vec::new(),
        })?;
        Ok(content)
    }

    pub fn write_sidecar(
        &self,
        step_id: &str,
        acceptance_ids: &[&str],
        facts: Value,
    ) -> Result<PathBuf> {
        let sequence = self.next_sequence();
        let timestamp = timestamp_millis();
        let path = self.artifact_dir.join("sidecars").join(format!(
            "{sequence:04}-{timestamp}-{}.json",
            sanitize(step_id)
        ));
        let request_id = facts.get("request_id").cloned().unwrap_or(Value::Null);
        let method = facts.get("method").cloned().unwrap_or(Value::Null);
        let request_path = facts.get("path").cloned().unwrap_or(Value::Null);
        let receipt_id = facts.get("receipt_id").cloned().unwrap_or(Value::Null);
        let receipt_status = facts.get("receipt_status").cloned().unwrap_or(Value::Null);
        let replayed = facts.get("replayed").cloned().unwrap_or(Value::Null);
        let revision_before = facts.get("revision_before").cloned().unwrap_or(Value::Null);
        let revision_after = facts.get("revision_after").cloned().unwrap_or(Value::Null);
        let view_epoch = facts.get("view_epoch").cloned().unwrap_or(Value::Null);
        let cursor = facts.get("cursor").cloned().unwrap_or(Value::Null);
        let base_cursor = facts.get("base_cursor").cloned().unwrap_or(Value::Null);
        let target_cursor = facts.get("target_cursor").cloned().unwrap_or(Value::Null);
        let document = json!({
            "schema_version": 1,
            "scenario": self.session,
            "step_id": step_id,
            "captured_at_ms": timestamp,
            "runtime_session_id": self.runtime_session_id,
            "viewport": {
                "width": self.width.get(),
                "height": self.height.get()
            },
            "acceptance_ids": acceptance_ids,
            "request_id": request_id,
            "method": method,
            "path": request_path,
            "receipt_id": receipt_id,
            "receipt_status": receipt_status,
            "replayed": replayed,
            "revision_before": revision_before,
            "revision_after": revision_after,
            "view_epoch": view_epoch,
            "cursor": cursor,
            "base_cursor": base_cursor,
            "target_cursor": target_cursor,
            "facts": facts
        });
        std::fs::write(&path, serde_json::to_vec_pretty(&document)?)?;
        self.record_artifact(ArtifactEntry {
            sequence,
            captured_at_ms: timestamp,
            kind: "sidecar".to_string(),
            step_id: step_id.to_string(),
            viewport: Some(Viewport {
                width: self.width.get(),
                height: self.height.get(),
            }),
            path: relative_artifact_path(&self.artifact_dir, &path),
            acceptance_ids: acceptance_ids.iter().map(|id| (*id).to_string()).collect(),
        })?;
        Ok(path)
    }

    pub fn wait_for(&self, expected: &str, secs: u64) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(secs);
        while start.elapsed() < timeout {
            if self.capture_viewport_raw()?.contains(expected) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let _ = self.capture_history("wait-timeout");
        Err(anyhow!(
            "Timeout waiting for '{expected}' in current viewport"
        ))
    }

    pub fn wait_until_ready(&self, secs: u64) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_secs(secs);
        while start.elapsed() < timeout {
            let capture = self.capture_viewport_raw()?;
            if capture_is_healthy(&capture) && capture.trim().len() > 80 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        let _ = self.capture_history("startup-timeout");
        Err(anyhow!("Timeout waiting for rendered TUI"))
    }

    pub fn assert_healthy_capture(&self, min_len: usize) -> Result<String> {
        let capture = self.capture()?;
        if !capture_is_healthy(&capture) {
            let _ = self.capture_history("unhealthy");
            return Err(anyhow!("TUI viewport contains startup/runtime failure"));
        }
        if capture.trim().len() < min_len {
            return Err(anyhow!(
                "TUI viewport too short ({} chars)",
                capture.trim().len()
            ));
        }
        Ok(capture)
    }

    pub fn screenshot(&self, path: &str) -> Result<()> {
        let content = self.capture_step("screenshot", &[])?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.send("/quit")?;
        self.enter()?;
        let exit = self.wait_for_exit(Duration::from_secs(5))?;
        let exit_path = self.artifact_dir.join("process-exit.json");
        std::fs::write(
            &exit_path,
            serde_json::to_vec_pretty(&json!({
                "runtime_session_id": self.runtime_session_id,
                "captured_at_ms": timestamp_millis(),
                "natural_exit": true,
                "exit_code": exit
            }))?,
        )?;
        self.record_artifact(ArtifactEntry {
            sequence: self.next_sequence(),
            captured_at_ms: timestamp_millis(),
            kind: "process_exit".to_string(),
            step_id: "natural-exit".to_string(),
            viewport: Some(Viewport {
                width: self.width.get(),
                height: self.height.get(),
            }),
            path: relative_artifact_path(&self.artifact_dir, &exit_path),
            acceptance_ids: Vec::new(),
        })?;
        self.run_tmux("kill owned session", &["kill-session", "-t", &self.session])?;
        self.closed = true;
        if exit != 0 {
            return Err(anyhow!("TUI exited with status {exit}"));
        }
        Ok(())
    }

    fn wait_for_exit(&self, timeout: Duration) -> Result<i32> {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let output = tmux_output(
                &self.tmux_label,
                &[
                    "display-message".to_string(),
                    "-p".to_string(),
                    "-t".to_string(),
                    self.session.clone(),
                    "#{pane_dead} #{pane_dead_status}".to_string(),
                ],
            )?;
            ensure_success("read pane exit status", &output)?;
            let status = String::from_utf8_lossy(&output.stdout);
            let mut parts = status.split_whitespace();
            if parts.next() == Some("1") {
                return parts
                    .next()
                    .unwrap_or("1")
                    .parse::<i32>()
                    .map_err(|error| anyhow!("invalid pane exit status: {error}"));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = self.capture_history("exit-timeout");
        Err(anyhow!("TUI did not exit naturally after /quit"))
    }

    fn capture_viewport_raw(&self) -> Result<String> {
        let output = tmux_output(
            &self.tmux_label,
            &[
                "capture-pane".to_string(),
                "-t".to_string(),
                self.session.clone(),
                "-p".to_string(),
            ],
        )?;
        ensure_success("capture current viewport", &output)?;
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn actual_viewport(&self) -> Result<Viewport> {
        let output = tmux_output(
            &self.tmux_label,
            &[
                "display-message".to_string(),
                "-p".to_string(),
                "-t".to_string(),
                self.session.clone(),
                "#{window_width} #{window_height}".to_string(),
            ],
        )?;
        ensure_success("read tmux viewport", &output)?;
        let rendered = String::from_utf8_lossy(&output.stdout);
        let mut values = rendered.split_whitespace();
        let width = values
            .next()
            .ok_or_else(|| anyhow!("tmux viewport width missing"))?
            .parse::<u16>()
            .context("parse tmux viewport width")?;
        let height = values
            .next()
            .ok_or_else(|| anyhow!("tmux viewport height missing"))?
            .parse::<u16>()
            .context("parse tmux viewport height")?;
        Ok(Viewport { width, height })
    }

    fn write_scenario_manifest(&self, config: &TuiLaunchConfig) -> Result<()> {
        let manifest = json!({
            "schema_version": 1,
            "scenario": config.name,
            "runtime_session_id": config.session_id,
            "workspace": config.workspace,
            "config_home": config.config_home,
            "home_dir": config.home_dir,
            "gateway_url": config.gateway_url,
            "viewport": {"width": config.width, "height": config.height},
            "environment_mode": "env-i-explicit",
            "api_token_present": !config.api_token.is_empty()
        });
        let path = self.artifact_dir.join("scenario.json");
        std::fs::write(&path, serde_json::to_vec_pretty(&manifest)?)?;
        self.record_artifact(ArtifactEntry {
            sequence: self.next_sequence(),
            captured_at_ms: timestamp_millis(),
            kind: "scenario_manifest".to_string(),
            step_id: "launch".to_string(),
            viewport: Some(Viewport {
                width: config.width,
                height: config.height,
            }),
            path: relative_artifact_path(&self.artifact_dir, &path),
            acceptance_ids: Vec::new(),
        })
    }

    fn run_tmux(&self, operation: &str, args: &[&str]) -> Result<()> {
        let output = tmux_output(
            &self.tmux_label,
            &args
                .iter()
                .map(|arg| (*arg).to_string())
                .collect::<Vec<_>>(),
        )?;
        ensure_success(operation, &output)
    }

    fn next_sequence(&self) -> u64 {
        let next = self.sequence.get().saturating_add(1);
        self.sequence.set(next);
        next
    }

    fn record_artifact(&self, entry: ArtifactEntry) -> Result<()> {
        self.artifacts.borrow_mut().push(entry);
        self.write_index()
    }

    fn write_index(&self) -> Result<()> {
        std::fs::write(
            self.artifact_dir.join("index.json"),
            serde_json::to_vec_pretty(&json!({
                "schema_version": 1,
                "scenario": self.session,
                "runtime_session_id": self.runtime_session_id,
                "artifacts": &*self.artifacts.borrow()
            }))?,
        )?;
        Ok(())
    }
}

fn validate_launch_config(config: &TuiLaunchConfig) -> Result<()> {
    if config.name.trim().is_empty()
        || config.gateway_url.trim().is_empty()
        || config.session_id.trim().is_empty()
        || config.width < 40
        || config.height < 12
    {
        return Err(anyhow!("TUI launch config is incomplete"));
    }
    let protected = PROTECTED_ENV.iter().copied().collect::<BTreeSet<_>>();
    let overlaps = config
        .extra_env
        .keys()
        .filter(|key| protected.contains(key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !overlaps.is_empty() {
        return Err(anyhow!(
            "extra_env cannot override protected launch fields: {}",
            overlaps.join(",")
        ));
    }
    Ok(())
}

fn tmux_output(label: &str, args: &[String]) -> Result<Output> {
    Command::new("tmux")
        .arg("-L")
        .arg(label)
        .args(args)
        .output()
        .context("execute tmux")
}

fn ensure_success(operation: &str, output: &Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "{operation} failed (status={}): {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn owned_session_name(name: &str) -> String {
    format!(
        "cowd-it-{}-{}-{}",
        sanitize(name),
        std::process::id(),
        timestamp_millis()
    )
}

fn sanitize(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if sanitized.is_empty() {
        "scenario".to_string()
    } else {
        sanitized
    }
}

fn timestamp_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn relative_artifact_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || "-_./:=,".contains(character))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn capture_is_healthy(capture: &str) -> bool {
    let lower = capture.to_lowercase();
    !lower.contains("panic")
        && !lower.contains("backtrace")
        && !lower.contains("thread '")
        && !lower.contains("failed to initialize terminal")
        && !lower.contains("run cowd --help")
}

impl Drop for TuiSession {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let _ = tmux_output(
            &self.tmux_label,
            &[
                "kill-session".to_string(),
                "-t".to_string(),
                self.session.clone(),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protected_environment_cannot_be_overridden() {
        let mut config = TuiLaunchConfig::from_env("protected-env").expect("config");
        config
            .extra_env
            .insert("COWD_GATEWAY_URL".to_string(), "forged".to_string());
        assert!(validate_launch_config(&config).is_err());
    }

    #[test]
    fn shell_quoting_does_not_expose_argument_boundaries() {
        assert_eq!(shell_quote("plain/path"), "plain/path");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
    }
}
