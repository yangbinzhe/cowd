use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_PATH: &str = "/usr/local/bin:/usr/bin:/bin";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct OwnedRoot {
    path: PathBuf,
    failed: bool,
}

impl OwnedRoot {
    fn create() -> Result<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        for _ in 0..32 {
            let sequence = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "cowd-it-server-{}-{stamp}-{sequence}",
                std::process::id()
            ));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        failed: false,
                    })
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(anyhow!(
            "Unable to allocate an exclusive interactive server root"
        ))
    }

    // Only the leaf double currently has an independently known resource set.
    // Real Gateway cleanup cannot call this until its descendant/pipe owner
    // supplies that confirmation. Direct Child::wait alone is insufficient.
    #[cfg(test)]
    fn remove_after_confirmed_completion(&mut self) -> Result<()> {
        if self.failed || std::thread::panicking() {
            return Err(anyhow!("Failed interactive evidence must be retained"));
        }
        std::fs::remove_dir_all(&self.path).context("Remove confirmed interactive server root")
    }
}

impl Drop for OwnedRoot {
    fn drop(&mut self) {
        // A default drop, including startup errors and unwinding, is never
        // authorization to destroy logs. The generated path contains no secret.
        if self.path.exists() {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            if let Ok(mut marker) = options.open(self.path.join("retained.txt")) {
                use std::io::Write;
                let _ = marker.write_all(b"Retained: normal completion and full resource return have not both been confirmed.\n");
            }
            eprintln!("Interactive evidence retained at {}", self.path.display());
        }
    }
}

#[derive(Deserialize)]
struct HealthSnapshot {
    status: String,
    gateway: String,
    api_router: String,
    process: ProcessSnapshot,
}

#[derive(Deserialize)]
struct ProcessSnapshot {
    pid: u32,
    address: String,
    pid_file: PathBuf,
    addr_file: PathBuf,
    discovery_warning: Option<String>,
}

fn owned_health(body: &[u8], pid: u32, base_url: &str, config_home: &Path) -> Result<bool> {
    let health: HealthSnapshot =
        serde_json::from_slice(body).context("Invalid interactive gateway healthz JSON")?;
    let pid_file = config_home.join("run/cowd-serve.pid");
    if health.gateway != "gateway-runtime-host"
        || health.api_router != "gateway-api-router"
        || health.process.pid != pid
        || health.process.address != base_url
        || health.process.pid_file != pid_file
        || health.process.addr_file != pid_file.with_extension("addr")
        || health.process.discovery_warning.is_some()
    {
        return Err(anyhow!("healthz does not identify the owned gateway child"));
    }
    Ok(matches!(health.status.as_str(), "healthy" | "degraded"))
}

fn private_config(port: u16, identity: &str, model: &str, apps_dir: &Path) -> serde_json::Value {
    // JSON is YAML-compatible; ConfigLoader discovers config.yaml, not config.json.
    // Config parsing flattens platform extras: host/port belong on the row.
    serde_json::json!({
        "model": model,
        "storage": {"backend": "postgres", "postgres": {
            "logicalIdentity": identity,
            "secretRef": "env:COWD_TEST_POSTGRES_URL"
        }},
        "gateway": {"enabled": true, "platforms": [{
            "platformType": "api_server", "enabled": true,
            "host": "127.0.0.1", "port": port
        }]},
        "apps": {"directories": [apps_dir]}
    })
}

pub struct ServerProcess {
    proc: Option<Child>,
    base_url: String,
    root: Option<OwnedRoot>,
    config_home: PathBuf,
    home_dir: PathBuf,
    workspace: PathBuf,
    runtime_dir: PathBuf,
    cowd_bin: PathBuf,
    postgres_url: String,
}

impl ServerProcess {
    pub fn start() -> Result<Self> {
        let postgres_url = std::env::var("COWD_TEST_POSTGRES_URL")
            .ok().filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("NotStartedError: COWD_TEST_POSTGRES_URL is required; no user database fallback"))?;
        let model = std::env::var("COWD_INTERACTIVE_MODEL")
            .ok().filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("NotStartedError: COWD_INTERACTIVE_MODEL must explicitly select the manual test model"))?;
        let requested_bin = std::env::var_os("COWD_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("cowd"));
        let cowd_bin = if requested_bin.components().count() > 1 {
            requested_bin
                .canonicalize()
                .context("Resolve COWD_BIN before changing workspace")?
        } else {
            std::env::split_paths(CHILD_PATH).map(|dir| dir.join(&requested_bin))
                .find(|path| path.is_file())
                .ok_or_else(|| anyhow!("NotStartedError: cowd is absent from the explicit child PATH; set COWD_BIN"))?
        };
        Self::start_with_inputs(postgres_url, model, cowd_bin)
    }

    fn start_with_inputs(postgres_url: String, model: String, cowd_bin: PathBuf) -> Result<Self> {
        let root = OwnedRoot::create()?;
        let retained_path = root.path.clone();
        let result = (|| {
            let mut srv = Self {
                proc: None,
                base_url: String::new(),
                config_home: root.path.join("config"),
                home_dir: root.path.join("home"),
                workspace: root.path.join("workspace"),
                runtime_dir: root.path.join("run"),
                cowd_bin,
                postgres_url,
                root: Some(root),
            };
            for path in [
                &srv.config_home,
                &srv.home_dir,
                &srv.workspace,
                &srv.runtime_dir,
            ] {
                std::fs::create_dir(path)?;
            }
            let apps_dir = srv.workspace.join("apps");
            std::fs::create_dir(&apps_dir)?;
            let reservation = TcpListener::bind(("127.0.0.1", 0))?;
            let port = reservation.local_addr()?.port();
            srv.base_url = format!("http://127.0.0.1:{port}");
            let identity = srv
                .root
                .as_ref()
                .unwrap()
                .path
                .file_name()
                .unwrap()
                .to_string_lossy();
            std::fs::write(
                srv.config_home.join("config.yaml"),
                serde_json::to_vec_pretty(&private_config(port, &identity, &model, &apps_dir))?,
            )?;
            let stdout = std::fs::File::create(srv.config_home.join("gateway.stdout"))?;
            let stderr = std::fs::File::create(srv.config_home.join("gateway.stderr"))?;
            let mut command = Command::new(&srv.cowd_bin);
            command
                .args(["gateway", "run"])
                .env_clear()
                .env("PATH", CHILD_PATH)
                .env("HOME", &srv.home_dir)
                .env("COWD_CONFIG_HOME", &srv.config_home)
                .env("XDG_RUNTIME_DIR", &srv.runtime_dir)
                .env("COWD_TEST_POSTGRES_URL", &srv.postgres_url)
                .current_dir(&srv.workspace)
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(stderr);
            // No inherited listener support exists in this CLI. Release immediately
            // before spawn; a collision must fail child/health identity admission.
            drop(reservation);
            srv.proc = Some(command.spawn().context("Spawn owned cowd gateway run")?);
            let deadline = Instant::now() + Duration::from_secs(15);
            while Instant::now() < deadline {
                let child = srv.proc.as_mut().unwrap();
                if let Some(status) = child.try_wait()? {
                    return Err(anyhow!("Owned gateway exited before readiness: {status}"));
                }
                let pid = child.id();
                let probe = Command::new("curl")
                    .args([
                        "-q",
                        "--noproxy",
                        "*",
                        "-fsS",
                        "--connect-timeout",
                        "1",
                        "--max-time",
                        "2",
                        &format!("{}/healthz", srv.base_url),
                    ])
                    .env_clear()
                    .env("PATH", CHILD_PATH)
                    .env("HOME", &srv.home_dir)
                    .current_dir(&srv.workspace)
                    .stdin(Stdio::null())
                    .output()
                    .context("Probe owned gateway healthz")?;
                if let Some(status) = srv.proc.as_mut().unwrap().try_wait()? {
                    return Err(anyhow!("Owned gateway exited during readiness: {status}"));
                }
                if probe.status.success()
                    && owned_health(&probe.stdout, pid, &srv.base_url, &srv.config_home)?
                {
                    let pid_file = srv.config_home.join("run/cowd-serve.pid");
                    if std::fs::read_to_string(&pid_file)?.trim() != pid.to_string()
                        || std::fs::read_to_string(pid_file.with_extension("addr"))?.trim()
                            != srv.base_url
                    {
                        return Err(anyhow!(
                            "Owned gateway process metadata does not match healthz"
                        ));
                    }
                    if let Some(status) = srv.proc.as_mut().unwrap().try_wait()? {
                        return Err(anyhow!(
                            "Owned gateway exited after health validation: {status}"
                        ));
                    }
                    return Ok(srv);
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(anyhow!(
                "Owned gateway did not provide acceptable local health within 15s"
            ))
        })();
        result.with_context(|| {
            format!(
                "Interactive startup failed; private evidence retained at {}",
                retained_path.display()
            )
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn configure_tui(&self, config: &mut crate::tui::TuiLaunchConfig) {
        config.gateway_url = self.base_url.clone();
        config.config_home = self.config_home.clone();
        config.home_dir = self.home_dir.clone();
        config.workspace = self.workspace.clone();
        config.cowd_bin = self.cowd_bin.clone();
        config.api_token.clear();
        config
            .extra_env
            .insert("COWD_TEST_POSTGRES_URL".into(), self.postgres_url.clone());
        config.extra_env.insert(
            "XDG_RUNTIME_DIR".into(),
            self.runtime_dir.display().to_string(),
        );
    }

    pub fn close(&mut self) -> Result<()> {
        self.close_child_with(|child| {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            child.wait()?;
            Ok(())
        })
    }

    fn close_child_with(&mut self, stop: impl FnOnce(&mut Child) -> Result<()>) -> Result<()> {
        if let Some(child) = self.proc.as_mut() {
            if let Err(error) = stop(child) {
                if let Some(root) = self.root.as_mut() {
                    root.failed = true;
                }
                return Err(error.context("Interactive close failed; retain private root and logs"));
            }
            self.proc = None;
        }
        // Reaping this Child does not certify Gateway descendants or pipes.
        // Keep the root even on Ok; the known-resource owner must confirm it.
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{owned_health, private_config};
    use std::path::Path;

    #[test]
    fn private_config_uses_existing_env_secret_and_flat_platform_fields() {
        let config = private_config(
            43127,
            "owned-test",
            "explicit-model",
            Path::new("/private/apps"),
        );
        assert_eq!(config["storage"]["backend"], "postgres");
        assert_eq!(
            config["storage"]["postgres"],
            serde_json::json!({
                "logicalIdentity": "owned-test", "secretRef": "env:COWD_TEST_POSTGRES_URL"
            })
        );
        assert_eq!(
            config["gateway"]["platforms"],
            serde_json::json!([{
                "platformType": "api_server", "enabled": true, "host": "127.0.0.1", "port": 43127
            }])
        );
        assert_eq!(config["model"], "explicit-model");
        assert_eq!(
            config["apps"]["directories"],
            serde_json::json!(["/private/apps"])
        );
    }

    #[test]
    fn health_requires_owned_pid_address_paths_and_gateway_identity() {
        let config_home = Path::new("/private/config");
        let base_url = "http://127.0.0.1:43127";
        let valid = serde_json::json!({
            "status": "healthy", "gateway": "gateway-runtime-host", "api_router": "gateway-api-router",
            "process": {"pid": 42, "address": base_url,
                "pid_file": "/private/config/run/cowd-serve.pid",
                "addr_file": "/private/config/run/cowd-serve.addr", "discovery_warning": null}
        });
        let check = |value: &serde_json::Value| {
            owned_health(
                &serde_json::to_vec(value).unwrap(),
                42,
                base_url,
                config_home,
            )
        };
        assert!(check(&valid).unwrap());
        for (field, value) in [
            ("pid", serde_json::json!(43)),
            ("address", serde_json::json!("http://127.0.0.1:8642")),
            ("pid_file", serde_json::json!("/foreign/cowd-serve.pid")),
            ("addr_file", serde_json::json!("/foreign/cowd-serve.addr")),
            ("discovery_warning", serde_json::json!("foreign discovery")),
        ] {
            let mut foreign = valid.clone();
            foreign["process"][field] = value;
            assert!(check(&foreign).is_err(), "{field}");
        }
        let mut wrong_gateway = valid.clone();
        wrong_gateway["gateway"] = serde_json::json!("foreign");
        assert!(check(&wrong_gateway).is_err());
        let mut degraded = valid.clone();
        degraded["status"] = serde_json::json!("degraded");
        assert!(check(&degraded).unwrap());
        for status in ["unhealthy", "unknown", "starting", "error", ""] {
            let mut rejected = valid.clone();
            rejected["status"] = serde_json::json!(status);
            assert!(!check(&rejected).unwrap(), "{status}");
        }
        assert!(owned_health(b"not JSON", 42, base_url, config_home).is_err());
        assert!(owned_health(b"{}", 42, base_url, config_home).is_err());
    }

    // Only parent-controlled execution invokes these local Python doubles.
    // The trace path is embedded in the private script, never passed via env.
    #[cfg(unix)]
    fn run_child_double(mode: &str) {
        use std::os::unix::fs::PermissionsExt;
        let mut fixture = super::OwnedRoot::create().unwrap();
        let executable = fixture.path.join("gateway-double");
        let trace = fixture.path.join("trace.json");
        let script = r#"#!/usr/bin/python3
import http.server, json, os, pathlib, signal, sys
trace = pathlib.Path(TRACE_PATH)
mode = DOUBLE_MODE
config_home = pathlib.Path(os.environ["COWD_CONFIG_HOME"])
root = config_home.parent
pid = os.getpid()
trace.write_text(json.dumps({"root": str(root), "pid": pid, "validated": False}))
assert sys.argv[1:] == ["gateway", "run"]
assert pathlib.Path(os.environ["HOME"]) == root / "home"
assert pathlib.Path.cwd() == root / "workspace"
assert pathlib.Path(os.environ["XDG_RUNTIME_DIR"]) == root / "run"
assert os.environ["PATH"] == "/usr/local/bin:/usr/bin:/bin"
assert os.environ["COWD_TEST_POSTGRES_URL"] == "postgresql://command-double.invalid/unused"
assert not any(key in os.environ for key in ("COWD_API_TOKEN", "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY", "COWD_GATEWAY_URL", "COWD_BASH_ARTIFACT_DIR", "COWD_DISABLE_DAEMON_AUTOSTART"))
config = json.loads((config_home / "config.yaml").read_text())
assert config["model"] == "explicit-command-double-model"
assert config["storage"]["backend"] == "postgres"
assert config["storage"]["postgres"]["secretRef"] == "env:COWD_TEST_POSTGRES_URL"
assert config["apps"]["directories"] == [str(root / "workspace" / "apps")]
assert list((root / "workspace" / "apps").iterdir()) == []
platform, = config["gateway"]["platforms"]
assert platform["platformType"] == "api_server" and platform["enabled"] is True
assert platform["host"] == "127.0.0.1" and 0 < platform["port"] < 65536
trace.write_text(json.dumps({"root": str(root), "pid": pid, "validated": True}))
sys.stdout.write("owned gateway stdout evidence\n")
sys.stdout.flush()
sys.stderr.write("owned gateway stderr evidence\n")
sys.stderr.flush()
if mode == "exit":
    sys.exit(23)
address = "http://127.0.0.1:" + str(platform["port"])
pid_file = config_home / "run" / "cowd-serve.pid"
addr_file = pid_file.with_suffix(".addr")
health = {"status": "degraded" if mode in ("degraded", "foreign") else "healthy",
    "gateway": "gateway-runtime-host", "api_router": "gateway-api-router",
    "process": {"pid": pid + 1 if mode == "foreign" else pid, "address": address,
        "pid_file": str(pid_file), "addr_file": str(addr_file), "discovery_warning": None}}
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/healthz":
            self.send_error(404)
            return
        body = json.dumps(health).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
signal.alarm(25)
server = http.server.HTTPServer(("127.0.0.1", platform["port"]), Handler)
pid_file.parent.mkdir(exist_ok=True)
pid_file.write_text(str(pid))
addr_file.write_text(address)
server.serve_forever()
"#
        .replace("TRACE_PATH", &serde_json::to_string(&trace).unwrap())
        .replace("DOUBLE_MODE", &serde_json::to_string(mode).unwrap());
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        // Explicit inputs exercise the exact same core called by start(), with
        // no process-environment mutation or real database/model access.
        let result = super::ServerProcess::start_with_inputs(
            "postgresql://command-double.invalid/unused".into(),
            "explicit-command-double-model".into(),
            executable,
        );
        let observed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&trace).unwrap()).unwrap();
        assert_eq!(observed["validated"], true, "{mode}: child contract checks");
        let root = Path::new(observed["root"].as_str().unwrap());
        let pid = observed["pid"].as_u64().unwrap().to_string();
        let child_reaped = || {
            !std::process::Command::new("/bin/kill")
                .args(["-0", &pid])
                .output()
                .unwrap()
                .status
                .success()
        };
        if matches!(mode, "exit" | "foreign") {
            let error = format!("{:#}", result.err().expect("double must fail closed"));
            assert!(
                if mode == "exit" {
                    error.contains("exited")
                } else {
                    error.contains("does not identify")
                },
                "{mode}: {error}"
            );
        } else {
            let mut server = result.expect("owned healthy/degraded local child must start");
            let mut config = crate::tui::TuiLaunchConfig::from_env("command-double").unwrap();
            let session_id = config.session_id.clone();
            server.configure_tui(&mut config);
            assert_eq!(config.gateway_url, server.base_url());
            assert_eq!(config.config_home, server.config_home);
            assert_eq!(config.home_dir, server.home_dir);
            assert_eq!(config.workspace, server.workspace);
            assert_eq!(config.session_id, session_id);
            assert!(config.api_token.is_empty());
            if mode == "panic" {
                let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _server = server;
                    panic!("controlled interactive scenario panic");
                }));
                assert!(panicked.is_err());
            } else if mode == "close-error" {
                assert!(server
                    .close_child_with(|_| Err(anyhow::anyhow!(
                        "controlled close failure before reaping"
                    )))
                    .is_err());
                assert!(root.is_dir());
                assert_eq!(
                    std::fs::read(root.join("config/gateway.stderr")).unwrap(),
                    b"owned gateway stderr evidence\n"
                );
                server.close().unwrap();
                assert!(child_reaped());
                // A later successful direct reap cannot erase the earlier failure.
                let mut failed_root = server.root.take().unwrap();
                assert!(failed_root.remove_after_confirmed_completion().is_err());
            } else {
                server.close().unwrap();
                server.close().unwrap();
                assert!(server.proc.is_none());
                assert!(server.root.is_some());
                assert!(root.is_dir(), "successful close alone is not cleanup proof");
                assert!(child_reaped());
                // This script has exactly one known process and file-backed
                // stdout/stderr, no descendants or cancellable pipes. Only its
                // owner can now confirm successful scenario + complete return.
                let mut confirmed = server.root.take().unwrap();
                confirmed.remove_after_confirmed_completion().unwrap();
            }
        }
        assert!(child_reaped(), "{mode}: owned child was not reaped");
        if matches!(mode, "exit" | "foreign" | "panic" | "close-error") {
            assert!(root.is_dir());
            assert_eq!(
                std::fs::metadata(root).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::read(root.join("config/gateway.stdout")).unwrap(),
                b"owned gateway stdout evidence\n"
            );
            assert_eq!(
                std::fs::read(root.join("config/gateway.stderr")).unwrap(),
                b"owned gateway stderr evidence\n"
            );
            assert!(root.join("retained.txt").is_file());
        } else {
            assert!(!root.exists());
        }
        fixture.remove_after_confirmed_completion().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn owned_child_healthy_and_degraded_start_close_cleanup() {
        run_child_double("healthy");
        run_child_double("degraded");
    }

    #[cfg(unix)]
    #[test]
    fn early_child_exit_preserves_private_root_and_logs() {
        run_child_double("exit");
    }

    #[cfg(unix)]
    #[test]
    fn foreign_health_pid_reaps_child_and_preserves_private_logs() {
        run_child_double("foreign");
    }

    #[cfg(unix)]
    #[test]
    fn close_failure_preserves_logs_even_after_later_direct_reaping() {
        run_child_double("close-error");
    }

    #[cfg(unix)]
    #[test]
    fn scenario_panic_preserves_private_root_and_logs() {
        run_child_double("panic");
    }

    #[cfg(unix)]
    #[test]
    fn spawn_failure_preserves_private_root_and_opened_logs() {
        let mut fixture = super::OwnedRoot::create().unwrap();
        let error = super::ServerProcess::start_with_inputs(
            "postgresql://command-double.invalid/unused".into(),
            "explicit-command-double-model".into(),
            fixture.path.join("missing-executable"),
        )
        .err()
        .expect("missing executable must fail");
        let message = error.to_string();
        let root = Path::new(
            message
                .strip_prefix("Interactive startup failed; private evidence retained at ")
                .expect("safe failure evidence locator"),
        );
        assert!(root.is_dir());
        assert!(root.join("config/gateway.stdout").is_file());
        assert!(root.join("config/gateway.stderr").is_file());
        assert!(root.join("retained.txt").is_file());
        fixture.remove_after_confirmed_completion().unwrap();
    }
}
