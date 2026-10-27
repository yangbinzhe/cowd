use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHILD_PATH: &str = "/usr/local/bin:/usr/bin:/bin";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Deserialize)]
struct LaunchIdentity {
    nonce: String,
    pid: u32,
    starttime: u64,
    image_sha256: String,
}

fn process_starttime(pid: u32) -> Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    // comm can contain spaces and parentheses; field 22 is offset 19 after comm.
    stat.rsplit_once(") ")
        .and_then(|(_, rest)| rest.split_whitespace().nth(19))
        .ok_or_else(|| anyhow!("Missing owned process starttime"))?
        .parse()
        .context("Invalid owned process starttime")
}

fn wait_bounded(child: &mut Child, budget: Duration) -> Result<Option<std::process::ExitStatus>> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn hash_image(path: &Path) -> Result<String> {
    let input = File::open(path).context("Open launch image for hashing")?;
    if !input.metadata()?.is_file() {
        return Err(anyhow!("Launch image is not a regular file"));
    }
    let mut command = Command::new("/usr/bin/sha256sum")
        .env_clear()
        .stdin(input)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    if wait_bounded(&mut command, Duration::from_secs(5))?.is_none() {
        let _ = command.kill();
        let _ = wait_bounded(&mut command, Duration::from_secs(1));
        return Err(anyhow!("Launch image hash timed out"));
    }
    let output = command.wait_with_output()?;
    let digest = String::from_utf8(output.stdout)?;
    let digest = digest.split_whitespace().next().unwrap_or("");
    let result = format!("sha256:{digest}");
    if !output.status.success() || !valid_digest(&result) {
        return Err(anyhow!("Launch image hash failed"));
    }
    Ok(result)
}

fn valid_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn private_directory(path: &Path) -> Result<File> {
    // Linux O_DIRECTORY | O_NOFOLLOW. Keep the actual directory pinned through close.
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x10000 | 0x20000)
        .open(path)?;
    let metadata = directory.metadata()?;
    if metadata.uid() != std::fs::metadata("/proc/self")?.uid()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(anyhow!("Runtime directory must be owned and actually 0700"));
    }
    Ok(directory)
}

fn receipt_identity_and_report(bytes: &[u8], expected: &LaunchIdentity) -> Result<()> {
    let envelope: serde_json::Value = serde_json::from_slice(bytes)?;
    if envelope["schema_version"].as_u64() != Some(1) {
        return Err(anyhow!("Unknown shutdown receipt schema"));
    }
    let identity: LaunchIdentity = serde_json::from_value(envelope["identity"].clone())?;
    if identity.nonce != expected.nonce
        || identity.pid != expected.pid
        || identity.starttime != expected.starttime
        || identity.image_sha256 != expected.image_sha256
    {
        return Err(anyhow!("Shutdown receipt does not match this launch"));
    }
    // The producer hashes SELECTED config fields, not the entire configuration.
    if !envelope["identity"]["config_sha256"]
        .as_str()
        .is_some_and(valid_digest)
    {
        return Err(anyhow!("Missing selected-config digest"));
    }
    let report = &envelope["report"];
    let strings = |value: &serde_json::Value| -> Result<usize> {
        let values = value
            .as_array()
            .ok_or_else(|| anyhow!("Missing report string array"))?;
        if values.iter().any(|v| !v.is_string()) {
            return Err(anyhow!("Invalid report string array"));
        }
        Ok(values.len())
    };
    let number = |value: &serde_json::Value| {
        value
            .as_u64()
            .ok_or_else(|| anyhow!("Missing numeric shutdown counter"))
    };
    strings(&report["phases"])?;
    if strings(&report["failures"])? != 0 {
        return Err(anyhow!("Gateway reports shutdown failures"));
    }
    let started = report["execution_started"]
        .as_bool()
        .ok_or_else(|| anyhow!("Missing execution_started"))?;
    for key in ["execution", "active_turns", "memory"] {
        if report.get(key).is_none() {
            return Err(anyhow!("Missing owner report field: {key}"));
        }
    }
    let execution = &report["execution"];
    if started && execution.is_null() {
        return Err(anyhow!("Started execution has no shutdown report"));
    }
    if !execution.is_null() {
        for key in [
            "accepted",
            "completed",
            "failed",
            "preserved_graphs",
            "cancelled_graphs",
            "forced_aborts",
            "remaining_keys",
        ] {
            number(&execution[key])?;
        }
        if number(&execution["forced_aborts"])? != 0
            || number(&execution["remaining_keys"])? != 0
            || strings(&execution["errors"])? != 0
        {
            return Err(anyhow!("Execution did not drain naturally"));
        }
        for owner in execution["owners"]
            .as_object()
            .ok_or_else(|| anyhow!("Missing execution owners"))?
            .values()
        {
            for key in ["submitted", "active", "completed", "failed", "aborted"] {
                number(&owner[key])?;
            }
            if number(&owner["active"])? != 0 {
                return Err(anyhow!("Execution owner remains active"));
            }
        }
    }
    let active = &report["active_turns"];
    if !active.is_null() {
        number(&active["cancelled"])?;
        number(&active["drained"])?;
        if strings(&active["remaining_turn_ids"])? != 0 {
            return Err(anyhow!("Active turns remain"));
        }
    }
    let memory = &report["memory"];
    if !memory.is_null() {
        number(&memory["joined_tasks"])?;
        memory["watcher_joined"]
            .as_bool()
            .ok_or_else(|| anyhow!("Missing Memory watcher outcome"))?;
        if number(&memory["forced_aborts"])? != 0 || strings(&memory["errors"])? != 0 {
            return Err(anyhow!("Memory shutdown incomplete"));
        }
    }
    let tasks = &report["tasks"];
    for key in ["joined", "panicked", "forced_aborts", "rejected"] {
        number(&tasks[key])?;
    }
    if number(&tasks["panicked"])? != 0 || number(&tasks["forced_aborts"])? != 0 {
        return Err(anyhow!("Gateway tasks did not close naturally"));
    }
    Ok(())
}

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
        "runtime": {"background": {"evolution": false, "growth": false}},
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
    nonce: String,
    launch: Option<LaunchIdentity>,
    runtime_handle: Option<File>,
    closed: bool,
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
        let image = cowd_bin.clone();
        Self::start_with_image(postgres_url, model, cowd_bin, image)
    }

    fn start_with_image(
        postgres_url: String,
        model: String,
        cowd_bin: PathBuf,
        expected_image: PathBuf,
    ) -> Result<Self> {
        let root = OwnedRoot::create()?;
        Self::start_epoch(
            postgres_url,
            model,
            cowd_bin,
            expected_image,
            root,
            None,
            None,
        )
    }

    // A retained epoch preserves the complete operator file, logical database
    // identity, workspace and HOME. It is reachable only after original F2 close.
    fn start_epoch(
        postgres_url: String,
        model: String,
        cowd_bin: PathBuf,
        expected_image: PathBuf,
        root: OwnedRoot,
        retained_port: Option<u16>,
        initial_operator_token: Option<&str>,
    ) -> Result<Self> {
        let retained_path = root.path.clone();
        let result = (|| {
            let nonce = {
                let mut bytes = [0u8; 32];
                File::open("/dev/urandom")?.read_exact(&mut bytes)?;
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            };
            let mut srv = Self {
                proc: None,
                base_url: String::new(),
                config_home: root.path.join("config"),
                home_dir: root.path.join("home"),
                workspace: root.path.join("workspace"),
                runtime_dir: root.path.join(format!("run-{nonce}")),
                cowd_bin,
                postgres_url,
                root: Some(root),
                nonce,
                launch: None,
                runtime_handle: None,
                closed: false,
            };
            for path in [&srv.config_home, &srv.home_dir, &srv.workspace] {
                if retained_port.is_some() {
                    if !path.is_dir() {
                        return Err(anyhow!(
                            "Retained epoch is missing an original owned directory"
                        ));
                    }
                } else {
                    std::fs::create_dir(path)?;
                }
            }
            std::fs::create_dir(&srv.runtime_dir)?;
            std::fs::set_permissions(&srv.runtime_dir, std::fs::Permissions::from_mode(0o700))?;
            srv.runtime_handle = match private_directory(&srv.runtime_dir) {
                Ok(directory) => Some(directory),
                Err(_) => {
                    // Only this launch's private runtime subdirectory may leave Datas.
                    let parent = Path::new("/run/user/1000");
                    let _parent = private_directory(parent)?;
                    srv.runtime_dir = parent.join(format!("cowd-it-{}", srv.nonce));
                    let mut builder = std::fs::DirBuilder::new();
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700).create(&srv.runtime_dir)?;
                    Some(private_directory(&srv.runtime_dir)?)
                }
            };
            // Only runtime evidence uses the approved private fallback. General
            // TMP/workspace remain on Datas even when it cannot enforce modes.
            std::fs::write(
                srv.root.as_ref().unwrap().path.join("runtime-location.txt"),
                srv.runtime_dir.as_os_str().as_encoded_bytes(),
            )?;
            std::fs::write(
                srv.root
                    .as_ref()
                    .unwrap()
                    .path
                    .join(format!("runtime-location-{}.txt", srv.nonce)),
                srv.runtime_dir.as_os_str().as_encoded_bytes(),
            )?;
            let apps_dir = srv.workspace.join("apps");
            if retained_port.is_none() {
                std::fs::create_dir(&apps_dir)?;
            } else if !apps_dir.is_dir() {
                return Err(anyhow!("Retained epoch lost its original APP directory"));
            }
            let reservation = TcpListener::bind(("127.0.0.1", retained_port.unwrap_or(0)))?;
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
            if retained_port.is_none() {
                let mut config = private_config(port, &identity, &model, &apps_dir);
                if let Some(token) = initial_operator_token {
                    config["gateway"]["platforms"][0]["auth_token"] = token.into();
                    // Same inert, declared completions shape as the original
                    // Gateway test_provider_registry; initialization is real,
                    // no account/environment inference or live model service.
                    config["providers"]["r5-no-provider"] = serde_json::json!({
                        "baseUrl":"http://127.0.0.1:9/v1", "apiKey":"r5-synthetic-only",
                        "models":[model], "protocol":"completions"
                    });
                }
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(srv.config_home.join("config.yaml"))?;
                if initial_operator_token.is_some()
                    && file.metadata()?.permissions().mode() & 0o777 != 0o600
                {
                    return Err(anyhow!(
                        "Synthetic operator configuration must actually be private0600"
                    ));
                }
                file.write_all(&serde_json::to_vec_pretty(&config)?)?;
            } else if !srv.config_home.join("config.yaml").is_file() {
                return Err(anyhow!(
                    "Retained epoch lost its persisted operator configuration"
                ));
            }
            let log_file = |name: &str| -> Result<File> {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .custom_flags(0x20000)
                    .open(srv.runtime_dir.join(name))?;
                if file.metadata()?.permissions().mode() & 0o777 != 0o600 {
                    return Err(anyhow!("Private runtime log is not actually0600"));
                }
                Ok(file)
            };
            let stdout = log_file("gateway.stdout")?;
            let stderr = log_file("gateway.stderr")?;
            let expected_image_sha256 = hash_image(&expected_image)?;
            let owned_command = |args: &[&str]| {
                let mut command = Command::new(&srv.cowd_bin);
                command
                    .args(args)
                    .env_clear()
                    .env("PATH", CHILD_PATH)
                    .env("HOME", &srv.home_dir)
                    .env("COWD_CONFIG_HOME", &srv.config_home)
                    .env("XDG_RUNTIME_DIR", &srv.runtime_dir)
                    .env("COWD_TEST_SHUTDOWN_RECEIPT_NONCE", &srv.nonce)
                    .env("COWD_TEST_POSTGRES_URL", &srv.postgres_url)
                    .current_dir(&srv.workspace)
                    .stdin(Stdio::null());
                command
            };
            if initial_operator_token.is_some() {
                // Only the explicitly owned real-process specialty fixture
                // bootstraps its new isolated database, before Gateway starts.
                // Runtime readiness must still verify the original catalogs.
                let mut maintenance = owned_command(&["storage", "upgrade"])
                    .stdout(log_file("storage-upgrade.stdout")?)
                    .stderr(log_file("storage-upgrade.stderr")?)
                    .spawn()?;
                let identity = serde_json::json!({"pid":maintenance.id(),
                    "starttime":process_starttime(maintenance.id())?,
                    "image_sha256":expected_image_sha256,"operation":"storage upgrade"});
                use std::io::Write;
                log_file("storage-upgrade.identity.json")?
                    .write_all(&serde_json::to_vec(&identity)?)?;
                let status = wait_bounded(&mut maintenance, Duration::from_secs(30))?;
                if status.is_none() {
                    let _ = maintenance.kill();
                    let reaped = wait_bounded(&mut maintenance, Duration::from_secs(5))?.is_some();
                    return Err(anyhow!(
                        "Owned storage preparation timed out; reaped={reaped}"
                    ));
                }
                if !status.unwrap().success() {
                    return Err(anyhow!(
                        "Original storage upgrade failed; preserve private startup logs"
                    ));
                }
                let mut bytes = Vec::new();
                File::open(srv.runtime_dir.join("storage-upgrade.stdout"))?
                    .take(64 * 1024 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > 64 * 1024 {
                    return Err(anyhow!("Storage preparation report exceeded bound"));
                }
                let report: serde_json::Value = serde_json::from_slice(&bytes)?;
                if report["operation"] != "postgres_schema_upgrade"
                    || report["status"] != "completed"
                    || report["gateway_stopped"] != true
                {
                    return Err(anyhow!(
                        "Original storage preparation did not confirm its exact operation"
                    ));
                }
            }
            let mut command = owned_command(&["gateway", "run"]);
            command.stdout(stdout).stderr(stderr);
            // No inherited listener support exists in this CLI. Release immediately
            // before spawn; a collision must fail child/health identity admission.
            drop(reservation);
            srv.proc = Some(command.spawn().context("Spawn owned cowd gateway run")?);
            let pid = srv.proc.as_ref().unwrap().id();
            let starttime = process_starttime(pid)?;
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
                    let image_sha256 = hash_image(Path::new(&format!("/proc/{pid}/exe")))?;
                    if image_sha256 != expected_image_sha256 || process_starttime(pid)? != starttime
                    {
                        return Err(anyhow!(
                            "Owned child startup image/starttime differs from admitted launch"
                        ));
                    }
                    srv.launch = Some(LaunchIdentity {
                        nonce: srv.nonce.clone(),
                        pid,
                        starttime,
                        image_sha256,
                    });
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
                "Interactive startup failed; evidence locator retained at {}",
                retained_path.display()
            )
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Restart this owned test instance, never a user's daemon. A failed or
    /// unconfirmed original shutdown cannot transfer the root to a new epoch.
    pub fn restart(&mut self) -> Result<()> {
        self.restart_with_image(self.cowd_bin.clone())
    }

    fn restart_with_image(&mut self, expected_image: PathBuf) -> Result<()> {
        self.close()?;
        let port = self
            .base_url
            .strip_prefix("http://127.0.0.1:")
            .ok_or_else(|| anyhow!("Owned restart address is not loopback"))?
            .parse::<u16>()?;
        let root = self
            .root
            .take()
            .ok_or_else(|| anyhow!("Owned root was already transferred"))?;
        let next = Self::start_epoch(
            self.postgres_url.clone(),
            String::new(),
            self.cowd_bin.clone(),
            expected_image,
            root,
            Some(port),
            None,
        )?;
        *self = next;
        Ok(())
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
        if self.closed {
            return Ok(());
        }
        // Original coordinator has serial 10/30/30/10/10-second owner drains,
        // plus other owners. Do not preempt those with a ten-second root kill.
        // This is a consumer waiting bound, not a claim that every owner closes.
        let result = self.finish_close(Duration::from_secs(120));
        if result.is_err() {
            if let Some(root) = self.root.as_mut() {
                root.failed = true;
            }
        } else {
            self.closed = true;
        }
        result.context("Interactive close unconfirmed; retain private report/root/logs")
    }

    // A failed stop never removes the original evidence or certifies owner closure.
    #[cfg(test)]
    fn close_child_with(&mut self, stop: impl FnOnce(&mut Child) -> Result<()>) -> Result<()> {
        if let Some(child) = self.proc.as_mut() {
            if let Err(error) = stop(child) {
                if let Some(root) = self.root.as_mut() {
                    root.failed = true;
                }
                return Err(error.context("Interactive close failed; retain private root and logs"));
            }
        }
        self.close()
    }

    fn finish_close(&mut self, budget: Duration) -> Result<()> {
        if let Some(child) = self.proc.as_mut() {
            if child.try_wait()?.is_none() {
                if let Some(launch) = &self.launch {
                    if child.id() != launch.pid
                        || process_starttime(child.id())? != launch.starttime
                    {
                        return Err(anyhow!("Owned child identity changed before SIGTERM"));
                    }
                }
                // Linux kill targets this still-owned, unreaped Child; no process search.
                unsafe extern "C" {
                    fn kill(pid: i32, signal: i32) -> i32;
                }
                let sent = unsafe { kill(child.id() as i32, 15) };
                if sent != 0 && child.try_wait()?.is_none() {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            let status = wait_bounded(child, budget)?;
            if status.is_none() {
                let _ = child.kill();
                let reaped = wait_bounded(child, Duration::from_millis(250))?.is_some();
                return Err(anyhow!(
                    "Forced shutdown, root reaped={reaped}; owner closure remains unconfirmed"
                ));
            }
            if !status.unwrap().success() {
                return Err(anyhow!("Owned gateway exited unsuccessfully"));
            }
        }
        let launch = self
            .launch
            .as_ref()
            .ok_or_else(|| anyhow!("No authenticated startup identity"))?;
        let directory = self
            .runtime_handle
            .as_ref()
            .ok_or_else(|| anyhow!("No pinned receipt directory"))?;
        let directory_metadata = directory.metadata()?;
        if directory_metadata.uid() != std::fs::metadata("/proc/self")?.uid()
            || directory_metadata.permissions().mode() & 0o777 != 0o700
        {
            return Err(anyhow!(
                "Pinned runtime directory lost private ownership/mode"
            ));
        }
        // O_NOFOLLOW | O_NONBLOCK: even a malformed FIFO cannot hang close/Drop.
        // Resolve the leaf relative to the startup-pinned directory FD.
        let path = format!(
            "/proc/self/fd/{}/shutdown-{}.v1.json",
            directory.as_raw_fd(),
            launch.nonce
        );
        let mut receipt = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(0x20000 | 0x800)
            .open(path)?;
        let metadata = receipt.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != std::fs::metadata("/proc/self")?.uid()
            || metadata.permissions().mode() & 0o777 != 0o600
            || metadata.len() > 2 * 1024 * 1024
        {
            return Err(anyhow!(
                "Receipt must be private owned0600 regular bounded file"
            ));
        }
        let mut bytes = Vec::new();
        (&mut receipt)
            .take(2 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 2 * 1024 * 1024 {
            return Err(anyhow!("Receipt grew beyond bound"));
        }
        receipt_identity_and_report(&bytes, launch)?;
        self.proc = None;
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !self.closed {
            // Drop is bounded independently of the explicit close budget.
            let result = self.finish_close(Duration::from_millis(250));
            if result.is_err() || std::thread::panicking() {
                if let Some(root) = self.root.as_mut() {
                    root.failed = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{owned_health, private_config};
    use std::path::Path;

    #[test]
    fn receipt_requires_exact_launch_and_complete_original_owner_counters() {
        use serde_json::json;
        let identity = super::LaunchIdentity {
            nonce: "a".repeat(64),
            pid: 123,
            starttime: 456,
            image_sha256: format!("sha256:{}", "b".repeat(64)),
        };
        // Read-only consumer fixture, not an actual Gateway shutdown original.
        let baseline = json!({"schema_version":1,
            "identity":{"nonce":identity.nonce,"pid":123,"starttime":456,
                "image_sha256":identity.image_sha256,"config_sha256":format!("sha256:{}", "c".repeat(64))},
            "report":{"phases":["test-owner-drain"],"failures":[],"execution_started":true,
                "execution":{"accepted":1,"completed":1,"failed":0,"preserved_graphs":0,
                    "cancelled_graphs":0,"forced_aborts":0,"remaining_keys":0,"errors":[],
                    "owners":{"test":{"submitted":1,"active":0,"completed":1,"failed":0,"aborted":0}}},
                "active_turns":{"cancelled":1,"drained":1,"remaining_turn_ids":[]},
                "memory":{"joined_tasks":1,"forced_aborts":0,"watcher_joined":true,"errors":[]},
                "tasks":{"joined":1,"panicked":0,"forced_aborts":0,"rejected":0}}});
        let accepted = |value: &serde_json::Value| {
            super::receipt_identity_and_report(&serde_json::to_vec(value).unwrap(), &identity)
                .is_ok()
        };
        assert!(accepted(&baseline));
        for (pointer, bad) in [
            ("/schema_version", json!(2)),
            ("/identity/nonce", json!("f".repeat(64))),
            ("/identity/pid", json!(124)),
            ("/identity/starttime", json!(457)),
            (
                "/identity/image_sha256",
                json!(format!("sha256:{}", "d".repeat(64))),
            ),
            ("/identity/config_sha256", json!("not-a-selected-digest")),
            ("/report/execution_started", json!(null)),
            ("/report/execution", json!(null)),
            ("/report/failures", json!(["failed"])),
            ("/report/execution/remaining_keys", json!(1)),
            ("/report/execution/forced_aborts", json!(1)),
            ("/report/execution/errors", json!(["error"])),
            ("/report/execution/owners/test/active", json!(1)),
            ("/report/active_turns/remaining_turn_ids", json!(["turn"])),
            ("/report/memory/forced_aborts", json!(1)),
            ("/report/memory/errors", json!(["error"])),
            ("/report/tasks/panicked", json!(1)),
            ("/report/tasks/forced_aborts", json!(1)),
            ("/report/tasks/joined", json!("0")),
            ("/report/tasks/rejected", json!(null)),
        ] {
            let mut changed = baseline.clone();
            *changed.pointer_mut(pointer).unwrap() = bad;
            assert!(!accepted(&changed), "must reject {pointer}");
        }
        for pointer in [
            "/identity",
            "/report",
            "/report/execution",
            "/report/execution/owners/test",
            "/report/active_turns",
            "/report/memory",
            "/report/tasks",
        ] {
            let object = baseline.pointer(pointer).unwrap().as_object().unwrap();
            for key in object.keys() {
                let mut missing = baseline.clone();
                missing
                    .pointer_mut(pointer)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove(key);
                assert!(!accepted(&missing), "missing {pointer}/{key}");
            }
        }
        let mut unstarted = baseline.clone();
        unstarted["report"]["execution_started"] = json!(false);
        for key in ["execution", "active_turns", "memory"] {
            unstarted["report"][key] = json!(null);
        }
        assert!(
            accepted(&unstarted),
            "explicit unstarted/disabled options remain legal"
        );
        // Historical failed work is not an active owner or a shutdown failure.
        let mut historic = baseline.clone();
        historic["report"]["execution"]["failed"] = json!(1);
        historic["report"]["execution"]["preserved_graphs"] = json!(1);
        assert!(accepted(&historic));
    }

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
import hashlib, http.server, json, os, pathlib, signal, sys
trace = pathlib.Path(TRACE_PATH)
mode = DOUBLE_MODE
config_home = pathlib.Path(os.environ["COWD_CONFIG_HOME"])
root = config_home.parent
pid = os.getpid()
trace.write_text(json.dumps({"root": str(root), "runtime": os.environ["XDG_RUNTIME_DIR"], "pid": pid, "validated": False}))
assert sys.argv[1:] == ["gateway", "run"]
assert pathlib.Path(os.environ["HOME"]) == root / "home"
assert pathlib.Path.cwd() == root / "workspace"
runtime_dir = pathlib.Path(os.environ["XDG_RUNTIME_DIR"])
assert runtime_dir.stat().st_mode & 0o777 == 0o700
nonce = os.environ["COWD_TEST_SHUTDOWN_RECEIPT_NONCE"]
assert len(nonce) == 64 and all(c in "0123456789abcdef" for c in nonce)
starttime = int(pathlib.Path("/proc/self/stat").read_text().rsplit(") ", 1)[1].split()[19])
image = "sha256:" + hashlib.sha256(pathlib.Path("/proc/self/exe").read_bytes()).hexdigest()
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
trace.write_text(json.dumps({"root": str(root), "runtime": str(runtime_dir), "pid": pid, "validated": True}))
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
def shutdown(signum, frame):
    if mode not in ("missing-receipt", "restart-missing-receipt"):
        envelope = {"schema_version": 1, "identity": {"nonce": nonce, "pid": pid,
            "starttime": starttime, "image_sha256": image, "config_sha256": "sha256:" + "0" * 64},
            "report": {"phases": ["local-double-drained"], "failures": [], "execution_started": False,
                "execution": None, "active_turns": None, "memory": None,
                "tasks": {"joined": 0, "panicked": 0, "forced_aborts": 0, "rejected": 0}}}
        if mode == "wrong-nonce":
            envelope["identity"]["nonce"] = "f" * 64
        path = runtime_dir / ("shutdown-" + nonce + ".v1.json")
        with open(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
            json.dump(envelope, output)
        if mode == "wide-receipt":
            path.chmod(0o644)
        if mode == "symlink-receipt":
            saved = path.with_suffix(".original")
            path.rename(saved)
            path.symlink_to(saved)
        if mode == "fifo-receipt":
            path.unlink()
            os.mkfifo(path, 0o600)
    sys.exit(0)
signal.signal(signal.SIGTERM, signal.SIG_IGN if mode == "drop-stubborn" else shutdown)
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
        // The command double is a script; independently bind its known interpreter
        // image. Production start_with_inputs always binds the actual cowd ELF.
        let result = super::ServerProcess::start_with_image(
            "postgresql://command-double.invalid/unused".into(),
            "explicit-command-double-model".into(),
            executable,
            "/usr/bin/python3".into(),
        );
        let observed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&trace).unwrap()).unwrap();
        assert_eq!(observed["validated"], true, "{mode}: child contract checks");
        let root = Path::new(observed["root"].as_str().unwrap());
        let runtime = Path::new(observed["runtime"].as_str().unwrap());
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
            if matches!(mode, "panic" | "drop-stubborn") {
                let started = std::time::Instant::now();
                let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _server = server;
                    panic!("controlled interactive scenario panic");
                }));
                assert!(panicked.is_err());
                if mode == "drop-stubborn" {
                    assert!(
                        started.elapsed() < std::time::Duration::from_secs(3),
                        "Drop must not wait for the explicit close budget"
                    );
                }
            } else if mode == "close-error" {
                assert!(server
                    .close_child_with(|_| Err(anyhow::anyhow!(
                        "controlled close failure before reaping"
                    )))
                    .is_err());
                assert!(root.is_dir());
                assert_eq!(
                    std::fs::read(runtime.join("gateway.stderr")).unwrap(),
                    b"owned gateway stderr evidence\n"
                );
                server.close().unwrap();
                assert!(child_reaped());
                // A later successful direct reap cannot erase the earlier failure.
                let mut failed_root = server.root.take().unwrap();
                assert!(failed_root.remove_after_confirmed_completion().is_err());
            } else if mode == "restart-missing-receipt" {
                let first = server.launch.clone().unwrap();
                assert!(server
                    .restart_with_image("/usr/bin/python3".into())
                    .is_err());
                assert!(
                    server.root.is_some(),
                    "failed original closure cannot transfer the root"
                );
                assert_eq!(server.launch.as_ref().unwrap().pid, first.pid);
                assert_eq!(server.nonce, first.nonce);
                let unchanged: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&trace).unwrap()).unwrap();
                assert_eq!(
                    unchanged["pid"], first.pid,
                    "no second launch on missing receipt"
                );
            } else if matches!(
                mode,
                "missing-receipt"
                    | "wrong-nonce"
                    | "wide-receipt"
                    | "symlink-receipt"
                    | "fifo-receipt"
            ) {
                assert!(
                    server.close().is_err(),
                    "root exit alone must not certify closure"
                );
                assert!(child_reaped());
                assert!(
                    server.close().is_err(),
                    "failed closure cannot turn into default success"
                );
            } else {
                if mode == "restart" {
                    let first = server.launch.clone().unwrap();
                    let original_config =
                        std::fs::read(server.config_home.join("config.yaml")).unwrap();
                    let original_workspace = server.workspace.clone();
                    std::fs::write(
                        original_workspace.join("retained-business.txt"),
                        b"independent-original",
                    )
                    .unwrap();
                    server
                        .restart_with_image("/usr/bin/python3".into())
                        .unwrap();
                    let second = server.launch.as_ref().unwrap();
                    assert_ne!(first.pid, second.pid);
                    assert_ne!(first.nonce, second.nonce);
                    let second_pid = second.pid;
                    assert!(!Path::new(&format!("/proc/{}", first.pid)).exists());
                    assert_eq!(server.root.as_ref().unwrap().path, root);
                    assert_eq!(server.workspace, original_workspace);
                    assert_eq!(
                        std::fs::read(server.config_home.join("config.yaml")).unwrap(),
                        original_config
                    );
                    assert_eq!(
                        std::fs::read(original_workspace.join("retained-business.txt")).unwrap(),
                        b"independent-original"
                    );
                    assert_ne!(server.runtime_dir, runtime);
                    assert!(runtime
                        .join(format!("shutdown-{}.v1.json", first.nonce))
                        .is_file());
                    let second_runtime = server.runtime_dir.clone();
                    server.close().unwrap();
                    assert!(!Path::new(&format!("/proc/{second_pid}")).exists());
                    if !second_runtime.starts_with(root) {
                        std::fs::remove_dir_all(second_runtime).unwrap();
                    }
                }
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
                if !runtime.starts_with(&confirmed.path) {
                    // Only the controlled leaf's known fully returned runtime.
                    std::fs::remove_dir_all(runtime).unwrap();
                }
                confirmed.remove_after_confirmed_completion().unwrap();
            }
        }
        assert!(child_reaped(), "{mode}: owned child was not reaped");
        if matches!(
            mode,
            "exit"
                | "foreign"
                | "panic"
                | "drop-stubborn"
                | "close-error"
                | "missing-receipt"
                | "restart-missing-receipt"
                | "wrong-nonce"
                | "wide-receipt"
                | "symlink-receipt"
                | "fifo-receipt"
        ) {
            assert!(root.is_dir());
            assert_eq!(
                std::fs::metadata(runtime).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::read(runtime.join("gateway.stdout")).unwrap(),
                b"owned gateway stdout evidence\n"
            );
            assert_eq!(
                std::fs::read(runtime.join("gateway.stderr")).unwrap(),
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
    fn retained_root_epoch_does_not_rewrite_config_and_preserves_original_receipt() {
        // This proves only the consumer's two leaf-double lifecycle, not real
        // Gateway/background recovery. The real two-PID gate below is separate.
        run_child_double("restart");
        run_child_double("restart-missing-receipt");
    }

    // Original public broker protocol, using a fresh synthetic credential for
    // this owned process only. No signer, authority-file edits or fake grants.
    fn background_broker_request(
        server: &super::ServerProcess,
        token: &str,
        mut request: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;
        let mut stream = UnixStream::connect(server.config_home.join("auth-broker/broker.sock"))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        stream.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
        request["credential"] = token.into();
        stream.write_all(&serde_json::to_vec(&request)?)?;
        stream.write_all(b"\n")?;
        let mut body = Vec::new();
        let mut chunk = [0u8; 2048];
        loop {
            let left = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or_else(|| anyhow::anyhow!("Owned broker response deadline"))?;
            stream.set_read_timeout(Some(left))?;
            let n = stream.read(&mut chunk)?;
            body.extend_from_slice(&chunk[..n]);
            anyhow::ensure!(body.len() <= 65536, "Owned broker response exceeded bound");
            if n == 0 || body.contains(&b'\n') {
                break;
            }
        }
        let value: serde_json::Value = serde_json::from_slice(&body)?;
        anyhow::ensure!(
            value["kind"] != "error",
            "Original broker refused test management request"
        );
        Ok(value)
    }

    fn background_http_command(
        server: &super::ServerProcess,
        token: &str,
        domain: &str,
        mode: &str,
    ) -> anyhow::Result<(u16, serde_json::Value)> {
        use std::io::{Read, Write};
        use std::net::TcpStream;
        let address = server
            .base_url
            .strip_prefix("http://")
            .ok_or_else(|| anyhow::anyhow!("Owned test address must be HTTP"))?;
        let mut stream =
            TcpStream::connect_timeout(&address.parse()?, std::time::Duration::from_secs(2))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(2)))?;
        let body = serde_json::to_vec(&serde_json::json!({"command":domain,"args":{"mode":mode}}))?;
        let header = format!("POST /api/slash/dispatch HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nAuthorization: Bearer {token}\r\nx-cowd-surface-id: legacy_gateway\r\nx-cowd-requested-capabilities: runtime.maintenance.manage\r\nContent-Length: {}\r\n\r\n", body.len());
        stream.write_all(header.as_bytes())?;
        stream.write_all(&body)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut response = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let left = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or_else(|| anyhow::anyhow!("Owned HTTP response deadline"))?;
            stream.set_read_timeout(Some(left))?;
            let n = stream.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            response.extend_from_slice(&buffer[..n]);
            anyhow::ensure!(
                response.len() <= 128 * 1024,
                "Owned HTTP response exceeded bound"
            );
        }
        let cut = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| anyhow::anyhow!("Owned HTTP response lacks headers"))?;
        let headers = std::str::from_utf8(&response[..cut])?;
        anyhow::ensure!(
            !headers.to_ascii_lowercase().contains("transfer-encoding:"),
            "Test consumer requires bounded Content-Length JSON"
        );
        let status = headers
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .ok_or_else(|| anyhow::anyhow!("Missing HTTP status"))?
            .parse()?;
        Ok((status, serde_json::from_slice(&response[cut + 4..])?))
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "manual real Gateway/isolated PostgreSQL gate; run explicitly --ignored --exact, never counted as passed while ignored"]
    fn background_off_survives_two_real_gateway_epochs_in_the_same_root() -> anyhow::Result<()> {
        use std::io::{Read, Write};
        use std::os::unix::fs::OpenOptionsExt;
        // The controller must supply a new private database and a source-bound
        // real cowd. No model is invoked and no account configuration is read.
        let postgres = std::env::var("COWD_TEST_POSTGRES_URL")
            .map_err(|_| anyhow::anyhow!("NotStarted: new isolated PostgreSQL URL required"))?;
        let image = std::path::PathBuf::from(
            std::env::var_os("COWD_BIN")
                .ok_or_else(|| anyhow::anyhow!("NotStarted: exact current COWD_BIN required"))?,
        )
        .canonicalize()?;
        let root_path =
            std::path::PathBuf::from(std::env::var_os("COWD_R5_OWNED_ROOT").ok_or_else(|| {
                anyhow::anyhow!("NotStarted: pre-registered private COWD_R5_OWNED_ROOT required")
            })?);
        anyhow::ensure!(
            root_path.is_absolute() && root_path.symlink_metadata()?.is_dir(),
            "R5 root must be an absolute original directory"
        );
        super::private_directory(&root_path)?;
        anyhow::ensure!(
            std::fs::read_dir(&root_path)?.next().is_none(),
            "R5 pre-registered root must be fresh and empty"
        );
        let root = super::OwnedRoot {
            path: root_path,
            failed: false,
        };
        // The broker and synthetic credential cannot live on a filesystem
        // which merely pretends chmod succeeded. Use the pre-registered private TMPDIR.
        super::private_directory(&root.path)?;
        let root_path = root.path.clone();
        let token = {
            let mut bytes = [0u8; 32];
            std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let mut server = super::ServerProcess::start_epoch(
            postgres,
            "isolation-no-provider".into(),
            image.clone(),
            image,
            root,
            None,
            Some(&token),
        )?;
        let (status, _) = background_http_command(&server, &token, "growth", "status")?;
        assert_eq!(
            status, 403,
            "ordinary initial operator must not gain maintenance from a requested header"
        );
        let original = background_broker_request(
            &server,
            &token,
            serde_json::json!({"kind":"get_human_entitlements"}),
        )?;
        assert_eq!(original["kind"], "human_entitlements");
        let before = &original["entitlement"];
        let preview = background_broker_request(
            &server,
            &token,
            serde_json::json!({
                "kind":"preview_human_entitlements", "core_profile_id":"core_manager", "app_profiles":before["app_profiles"]
            }),
        )?;
        assert_eq!(preview["kind"], "human_entitlement_preview");
        let elevated = background_broker_request(
            &server,
            &token,
            serde_json::json!({
                "kind":"set_human_entitlements", "core_profile_id":"core_manager", "app_profiles":before["app_profiles"],
                "expected_credential_epoch":before["credential_epoch"], "expected_profile_revision":before["profile_revision"],
                "confirmation_digest":preview["confirmation_digest"]
            }),
        )?;
        assert_eq!(elevated["kind"], "human_entitlements");
        let mut snapshots = Vec::new();
        for domain in ["evolution", "growth"] {
            let (status, initial) = background_http_command(&server, &token, domain, "status")?;
            assert_eq!(status, 200);
            assert_eq!(initial["data"]["effective"], "off");
            let (status, on) = background_http_command(&server, &token, domain, "on")?;
            assert_eq!(status, 200);
            assert_eq!(on["data"]["effective"], "on");
            snapshots.push(on);
        }
        for domain in ["evolution", "growth"] {
            let (status, off) = background_http_command(&server, &token, domain, "off")?;
            assert_eq!(status, 200);
            assert_eq!(off["data"]["effective"], "off");
            assert_eq!(off["data"]["durable_recovery"], "settled");
            snapshots.push(off);
        }
        let first = server.launch.clone().unwrap();
        let old_runtime = server.runtime_dir.clone();
        let config_before = std::fs::read(server.config_home.join("config.yaml"))?;
        let config_digest = super::hash_image(&server.config_home.join("config.yaml"))?;
        let same_url = server.base_url.clone();
        server.restart()?;
        let second = server.launch.clone().unwrap();
        assert_ne!(first.pid, second.pid);
        assert_ne!(first.nonce, second.nonce);
        assert_ne!(old_runtime, server.runtime_dir);
        assert!(!std::path::Path::new(&format!("/proc/{}", first.pid)).exists());
        assert_eq!(server.root.as_ref().unwrap().path, root_path);
        assert_eq!(server.base_url, same_url);
        assert!(
            std::fs::read(server.config_home.join("config.yaml"))? == config_before,
            "retained operator config must be byte-identical; contents are private"
        );
        assert_eq!(
            super::hash_image(&server.config_home.join("config.yaml"))?,
            config_digest
        );
        for domain in ["evolution", "growth"] {
            let (status, cold) = background_http_command(&server, &token, domain, "status")?;
            assert_eq!(status, 200);
            assert_eq!(cold["data"]["effective"], "off");
            assert_eq!(cold["data"]["desired"][domain], false);
            assert_eq!(cold["data"]["durable_recovery"], "settled");
            snapshots.push(cold);
        }
        let second_runtime = server.runtime_dir.clone();
        server.close()?;
        assert!(!std::path::Path::new(&format!("/proc/{}", second.pid)).exists());
        assert!(std::net::TcpListener::bind(same_url.strip_prefix("http://").unwrap()).is_ok());
        let receipt_paths = [
            old_runtime.join(format!("shutdown-{}.v1.json", first.nonce)),
            second_runtime.join(format!("shutdown-{}.v1.json", second.nonce)),
        ];
        assert!(receipt_paths.iter().all(|path| path.is_file()));
        let report = serde_json::json!({"scope":"normal_two_epoch_background_control_only",
            "root":root_path,"config_sha256":config_digest,"pid1":first.pid,"starttime1":first.starttime,
            "pid2":second.pid,"starttime2":second.starttime,"nonce1":first.nonce,"nonce2":second.nonce,
            "image_sha256":second.image_sha256,"receipts":receipt_paths,"management":snapshots,
            "whole_G5_crash_pending_proven":false,"R6_foreground_ACK_proven":false,"model_calls":0});
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root_path.join("two-epoch-background-report.json"))?;
        output.write_all(&serde_json::to_vec_pretty(&report)?)?;
        Ok(())
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
    fn root_exit_without_matching_receipt_never_means_owner_closure() {
        run_child_double("missing-receipt");
        run_child_double("wrong-nonce");
        run_child_double("wide-receipt");
        run_child_double("symlink-receipt");
        run_child_double("fifo-receipt");
    }

    #[cfg(unix)]
    #[test]
    fn drop_is_bounded_and_keeps_logs_when_child_ignores_term() {
        run_child_double("drop-stubborn");
    }

    #[cfg(unix)]
    #[test]
    fn scenario_panic_preserves_private_root_and_logs() {
        run_child_double("panic");
    }

    #[cfg(unix)]
    #[test]
    fn spawn_failure_preserves_private_root_and_opened_logs() {
        use std::os::unix::fs::PermissionsExt;
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
                .strip_prefix("Interactive startup failed; evidence locator retained at ")
                .expect("safe failure evidence locator"),
        );
        assert!(root.is_dir());
        let runtime = std::fs::read_to_string(root.join("runtime-location.txt")).unwrap();
        let runtime = Path::new(&runtime);
        assert!(runtime.join("gateway.stdout").is_file());
        assert!(runtime.join("gateway.stderr").is_file());
        assert_eq!(
            std::fs::metadata(runtime).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert!(root.join("retained.txt").is_file());
        fixture.remove_after_confirmed_completion().unwrap();
    }
}
