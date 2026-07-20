use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use reqwest::Client;
use runtime::{GatewayConfig, JsonValue};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 180_000;
const MAX_OUTPUT_BYTES: u64 = 512 * 1024;
const TOKEN_EXPIRY_SAFETY_SECS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LarkCliToolMode {
    Read,
    Write,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LarkCliToolRequest {
    pub args: Vec<String>,
    #[serde(default)]
    pub brand: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct LarkAccount {
    brand: String,
    app_id: String,
    app_secret: String,
    open_base_url: String,
}

#[derive(Debug, Clone)]
struct CachedTenantToken {
    token: String,
    expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct TenantTokenResponse {
    #[serde(default)]
    code: i64,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    tenant_access_token: String,
    #[serde(default)]
    expire: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliRisk {
    Read,
    Write,
    HighRiskWrite,
}

static TENANT_TOKEN_CACHE: OnceLock<Mutex<HashMap<String, CachedTenantToken>>> = OnceLock::new();

pub(crate) fn execute_lark_cli_tool(
    gateway: &GatewayConfig,
    request: LarkCliToolRequest,
    mode: LarkCliToolMode,
) -> Result<String, String> {
    validate_args(&request.args)?;
    let cli_path = lark_cli_path()?;
    let risk = inspect_cli_risk(&cli_path, &request.args)?;
    match (mode, risk) {
        (LarkCliToolMode::Read, CliRisk::Read)
        | (LarkCliToolMode::Write, CliRisk::Write | CliRisk::HighRiskWrite) => {}
        (LarkCliToolMode::Read, CliRisk::Write | CliRisk::HighRiskWrite) => {
            return Err(
                "lark_cli_read rejected a mutating command; use lark_cli_write so Cowd can apply its approval policy"
                    .to_string(),
            );
        }
        (LarkCliToolMode::Write, CliRisk::Read) => {
            return Err(
                "lark_cli_write rejected a read-only command; use lark_cli_read to avoid unnecessary approval"
                    .to_string(),
            );
        }
    }

    let account = resolve_account(gateway, request.brand.as_deref())?;
    let tenant_token = tenant_token(&account)?;
    let private_root = PrivateRuntimeRoot::create("execute")?;
    let environment = cli_environment(&account, &tenant_token, private_root.path());
    let timeout_ms = request
        .timeout_ms
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(1_000, MAX_TIMEOUT_MS);
    let output = run_process(
        &cli_path,
        &request.args,
        &environment,
        Duration::from_millis(timeout_ms),
        private_root.path(),
    )?;
    let stdout = redact_secrets(
        &output.stdout,
        &[&account.app_secret, &tenant_token, &account.app_id],
    );
    let stderr = redact_secrets(
        &output.stderr,
        &[&account.app_secret, &tenant_token, &account.app_id],
    );
    if !output.success {
        return Err(format!(
            "lark-cli exited with code {}: {}",
            output.exit_code.unwrap_or(-1),
            if stderr.trim().is_empty() {
                stdout.trim()
            } else {
                stderr.trim()
            }
        ));
    }
    if let Ok(value) = serde_json::from_str::<Value>(&stdout) {
        if value.get("ok").and_then(Value::as_bool) == Some(false) {
            return Err(format!("lark-cli returned an error envelope: {value}"));
        }
        return serde_json::to_string_pretty(&serde_json::json!({
            "status": "ok",
            "identity": "bot",
            "brand": account.brand,
            "risk": cli_risk_label(risk),
            "result": value,
            "stderr": if stderr.trim().is_empty() { Value::Null } else { Value::String(stderr) },
        }))
        .map_err(|error| error.to_string());
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "status": "ok",
        "identity": "bot",
        "brand": account.brand,
        "risk": cli_risk_label(risk),
        "output": stdout,
        "stderr": if stderr.trim().is_empty() { Value::Null } else { Value::String(stderr) },
    }))
    .map_err(|error| error.to_string())
}

fn resolve_account(
    gateway: &GatewayConfig,
    requested_brand: Option<&str>,
) -> Result<LarkAccount, String> {
    let requested = requested_brand
        .unwrap_or("auto")
        .trim()
        .to_ascii_lowercase();
    if !matches!(requested.as_str(), "auto" | "feishu" | "lark") {
        return Err("brand must be auto, feishu, or lark".to_string());
    }
    let platform = gateway
        .platforms
        .iter()
        .filter(|platform| platform.enabled)
        .find(|platform| {
            let brand = platform_brand(&platform.platform_type);
            brand.is_some_and(|brand| requested == "auto" || brand == requested)
                && string_value(&platform.extra, "app_id").is_some_and(|value| !value.is_empty())
                && string_value(&platform.extra, "app_secret")
                    .is_some_and(|value| !value.is_empty())
        })
        .ok_or_else(|| {
            if requested == "auto" {
                "no enabled Feishu/Lark platform with app credentials is configured".to_string()
            } else {
                format!("no enabled {requested} platform is configured")
            }
        })?;
    let brand = platform_brand(&platform.platform_type)
        .expect("filtered platform must have a supported brand")
        .to_string();
    let app_id = string_value(&platform.extra, "app_id")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{brand} platform config is missing app_id"))?;
    let app_secret = string_value(&platform.extra, "app_secret")
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{brand} platform config is missing app_secret"))?;
    let default_base = if brand == "lark" {
        "https://open.larksuite.com"
    } else {
        "https://open.feishu.cn"
    };
    let open_base_url = string_value(&platform.extra, "base_url")
        .filter(|value| value.starts_with("https://"))
        .unwrap_or_else(|| default_base.to_string())
        .trim_end_matches('/')
        .to_string();
    Ok(LarkAccount {
        brand,
        app_id,
        app_secret,
        open_base_url,
    })
}

fn platform_brand(platform_type: &str) -> Option<&'static str> {
    match platform_type.trim().to_ascii_lowercase().as_str() {
        "feishu" => Some("feishu"),
        "lark" | "larksuite" => Some("lark"),
        _ => None,
    }
}

fn string_value(values: &BTreeMap<String, JsonValue>, key: &str) -> Option<String> {
    values
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .map(str::to_string)
}

fn tenant_token(account: &LarkAccount) -> Result<String, String> {
    let key = token_cache_key(account);
    let cache = TENANT_TOKEN_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(token) = cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&key)
        .filter(|token| token.expires_at > Instant::now())
        .map(|token| token.token.clone())
    {
        return Ok(token);
    }
    // Gateway tools execute behind a synchronous ToolExecutor boundary that
    // may itself be called from Tokio. reqwest::blocking owns an internal
    // Runtime and panics when dropped there, so keep the async client/runtime
    // entirely on a dedicated OS thread.
    let endpoint = format!(
        "{}/open-apis/auth/v3/tenant_access_token/internal",
        account.open_base_url
    );
    let app_id = account.app_id.clone();
    let app_secret = account.app_secret.clone();
    let (status, response) = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("build Lark token runtime: {error}"))?;
        runtime.block_on(async move {
            let response = Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .map_err(|error| format!("build Lark token client: {error}"))?
                .post(endpoint)
                .json(&serde_json::json!({
                    "app_id": app_id,
                    "app_secret": app_secret,
                }))
                .send()
                .await
                .map_err(|error| format!("request Lark tenant token: {error}"))?;
            let status = response.status().as_u16();
            let response = response
                .json::<TenantTokenResponse>()
                .await
                .map_err(|error| {
                    format!("decode Lark tenant token response ({status}): {error}")
                })?;
            Ok::<_, String>((status, response))
        })
    })
    .join()
    .map_err(|_| "Lark token worker panicked".to_string())??;
    if !(200..300).contains(&status)
        || response.code != 0
        || response.tenant_access_token.trim().is_empty()
    {
        return Err(format!(
            "Lark tenant token rejected: http_status={status}, code={}, message={}",
            response.code,
            response.msg.chars().take(240).collect::<String>()
        ));
    }
    let usable_secs = response
        .expire
        .saturating_sub(TOKEN_EXPIRY_SAFETY_SECS)
        .max(30);
    let token = response.tenant_access_token;
    cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(
            key,
            CachedTenantToken {
                token: token.clone(),
                expires_at: Instant::now() + Duration::from_secs(usable_secs),
            },
        );
    Ok(token)
}

fn token_cache_key(account: &LarkAccount) -> String {
    let mut digest = Sha256::new();
    digest.update(account.app_secret.as_bytes());
    format!(
        "{}\0{}\0{:x}",
        account.brand,
        account.app_id,
        digest.finalize()
    )
}

fn inspect_cli_risk(cli_path: &Path, args: &[String]) -> Result<CliRisk, String> {
    match args.first().map(String::as_str) {
        Some("api") => {
            let method = args
                .get(1)
                .ok_or_else(|| "lark-cli api requires an HTTP method".to_string())?
                .to_ascii_uppercase();
            return Ok(if matches!(method.as_str(), "GET" | "HEAD" | "OPTIONS") {
                CliRisk::Read
            } else {
                CliRisk::Write
            });
        }
        Some("schema" | "skills" | "whoami" | "doctor") => return Ok(CliRisk::Read),
        Some("auth" | "config" | "profile" | "update") => {
            return Err("credential and CLI lifecycle commands are owned by Cowd and cannot be invoked through a Skill".to_string());
        }
        Some(_) => {}
        None => return Err("lark-cli args must not be empty".to_string()),
    }
    let private_root = PrivateRuntimeRoot::create("risk")?;
    let mut help_args = args.to_vec();
    if !help_args
        .iter()
        .any(|arg| matches!(arg.as_str(), "-h" | "--help"))
    {
        help_args.push("--help".to_string());
    }
    let output = run_process(
        cli_path,
        &help_args,
        &base_cli_environment(private_root.path()),
        Duration::from_secs(10),
        private_root.path(),
    )?;
    let help = format!("{}\n{}", output.stdout, output.stderr);
    parse_risk_from_help(&help).ok_or_else(|| {
        "unable to verify lark-cli command risk from official help; command rejected closed"
            .to_string()
    })
}

fn parse_risk_from_help(help: &str) -> Option<CliRisk> {
    help.lines().find_map(|line| {
        let risk = line.trim().strip_prefix("Risk:")?.trim();
        match risk {
            "read" => Some(CliRisk::Read),
            "write" => Some(CliRisk::Write),
            "high-risk-write" => Some(CliRisk::HighRiskWrite),
            _ => None,
        }
    })
}

fn validate_args(args: &[String]) -> Result<(), String> {
    if args.is_empty() || args.len() > 96 {
        return Err("lark-cli args must contain between 1 and 96 entries".to_string());
    }
    let mut total = 0usize;
    for (index, arg) in args.iter().enumerate() {
        total = total.saturating_add(arg.len());
        if arg.contains('\0') || arg == "--" {
            return Err("lark-cli args contain a forbidden delimiter".to_string());
        }
        if arg == "--profile" || arg.starts_with("--profile=") {
            return Err("profile selection is owned by Cowd configuration".to_string());
        }
        if [
            "--app-id",
            "--app-secret",
            "--tenant-access-token",
            "--user-access-token",
            "--access-token",
        ]
        .iter()
        .any(|flag| arg == flag || arg.starts_with(&format!("{flag}=")))
        {
            return Err("credential arguments are owned by Cowd configuration".to_string());
        }
        if arg == "--as" && args.get(index + 1).map(String::as_str) != Some("bot") {
            return Err("Cowd Lark Skill tools currently support only bot identity".to_string());
        }
        if arg.starts_with("--as=") && arg != "--as=bot" {
            return Err("Cowd Lark Skill tools currently support only bot identity".to_string());
        }
    }
    if total > 64 * 1024 {
        return Err("lark-cli args exceed the 64 KiB limit".to_string());
    }
    Ok(())
}

fn lark_cli_path() -> Result<PathBuf, String> {
    let path = runtime::cowd_dirs::user_tools_dir()
        .join("node_modules")
        .join(".bin")
        .join(if cfg!(windows) {
            "lark-cli.cmd"
        } else {
            "lark-cli"
        });
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!(
            "Cowd-owned lark-cli is not installed at {}",
            path.display()
        ))
    }
}

fn cli_environment(account: &LarkAccount, token: &str, root: &Path) -> BTreeMap<String, String> {
    let mut environment = base_cli_environment(root);
    environment.insert("LARKSUITE_CLI_APP_ID".to_string(), account.app_id.clone());
    environment.insert(
        "LARKSUITE_CLI_TENANT_ACCESS_TOKEN".to_string(),
        token.to_string(),
    );
    environment.insert("LARKSUITE_CLI_BRAND".to_string(), account.brand.clone());
    environment.insert("LARKSUITE_CLI_DEFAULT_AS".to_string(), "bot".to_string());
    environment.insert("LARKSUITE_CLI_STRICT_MODE".to_string(), "bot".to_string());
    environment
}

fn base_cli_environment(root: &Path) -> BTreeMap<String, String> {
    let tool_bin = runtime::cowd_dirs::user_tools_dir()
        .join("node_modules")
        .join(".bin");
    BTreeMap::from([
        (
            "PATH".to_string(),
            format!("{}:/usr/bin:/bin", tool_bin.display()),
        ),
        ("HOME".to_string(), root.join("home").display().to_string()),
        ("TMPDIR".to_string(), root.join("tmp").display().to_string()),
        (
            "LARKSUITE_CLI_CONFIG_DIR".to_string(),
            root.join("config").display().to_string(),
        ),
        (
            "LARKSUITE_CLI_NO_UPDATE_NOTIFIER".to_string(),
            "1".to_string(),
        ),
        (
            "LARKSUITE_CLI_NO_SKILLS_NOTIFIER".to_string(),
            "1".to_string(),
        ),
        ("LANG".to_string(), "C.UTF-8".to_string()),
    ])
}

struct ProcessOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run_process(
    program: &Path,
    args: &[String],
    environment: &BTreeMap<String, String>,
    timeout: Duration,
    private_root: &Path,
) -> Result<ProcessOutput, String> {
    let stdout_path = private_root.join("stdout");
    let stderr_path = private_root.join("stderr");
    let stdout = File::create(&stdout_path).map_err(|error| error.to_string())?;
    let stderr = File::create(&stderr_path).map_err(|error| error.to_string())?;
    let mut child = Command::new(program)
        .args(args)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| format!("start Cowd-owned lark-cli: {error}"))?;
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("wait for lark-cli: {error}"))?
        {
            break status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "lark-cli exceeded the {} ms timeout",
                timeout.as_millis()
            ));
        }
        thread::sleep(Duration::from_millis(20));
    };
    Ok(ProcessOutput {
        success: status.success(),
        exit_code: status.code(),
        stdout: read_bounded(&stdout_path)?,
        stderr: read_bounded(&stderr_path)?,
    })
}

fn read_bounded(path: &Path) -> Result<String, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.take(MAX_OUTPUT_BYTES)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    let truncated = fs::metadata(path)
        .map(|metadata| metadata.len() > MAX_OUTPUT_BYTES)
        .unwrap_or(false);
    let mut output = String::from_utf8_lossy(&bytes).to_string();
    if truncated {
        output.push_str("\n…[lark-cli output truncated by Cowd]");
    }
    Ok(output)
}

fn redact_secrets(value: &str, secrets: &[&str]) -> String {
    secrets
        .iter()
        .filter(|secret| !secret.is_empty())
        .fold(value.to_string(), |output, secret| {
            output.replace(secret, "<redacted>")
        })
}

fn cli_risk_label(risk: CliRisk) -> &'static str {
    match risk {
        CliRisk::Read => "read",
        CliRisk::Write => "write",
        CliRisk::HighRiskWrite => "high-risk-write",
    }
}

struct PrivateRuntimeRoot {
    path: PathBuf,
}

impl PrivateRuntimeRoot {
    fn create(label: &str) -> Result<Self, String> {
        let path = std::env::temp_dir().join(format!(
            "cowd-lark-cli-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(path.join("home")).map_err(|error| error.to_string())?;
        fs::create_dir_all(path.join("tmp")).map_err(|error| error.to_string())?;
        fs::create_dir_all(path.join("config")).map_err(|error| error.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .map_err(|error| error.to_string())?;
        }
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for PrivateRuntimeRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime::{
        GatewayCapacityConfig, GatewayConfig, GatewayPlatformConfig, SessionResetPolicy,
    };

    #[test]
    fn risk_parser_and_raw_api_classification_fail_closed() {
        assert_eq!(parse_risk_from_help("Risk: read\n"), Some(CliRisk::Read));
        assert_eq!(
            parse_risk_from_help("Risk: high-risk-write\n"),
            Some(CliRisk::HighRiskWrite)
        );
        assert_eq!(parse_risk_from_help("Risk: mystery\n"), None);
        assert!(validate_args(&["api".to_string(), "GET".to_string()]).is_ok());
        assert!(validate_args(&["base".to_string(), "--".to_string()]).is_err());
        assert!(validate_args(&["im".to_string(), "--as=user".to_string()]).is_err());
        assert!(validate_args(&[
            "im".to_string(),
            "+chat-list".to_string(),
            "--app-secret=forbidden".to_string()
        ])
        .is_err());
    }

    #[test]
    fn configured_account_supports_feishu_and_never_places_secret_in_child_env() {
        let gateway = GatewayConfig {
            enabled: true,
            webui_dir: None,
            platforms: vec![GatewayPlatformConfig {
                platform_type: "feishu".to_string(),
                enabled: true,
                extra: BTreeMap::from([
                    (
                        "app_id".to_string(),
                        JsonValue::String("cli_test".to_string()),
                    ),
                    (
                        "app_secret".to_string(),
                        JsonValue::String("permanent-secret".to_string()),
                    ),
                ]),
            }],
            session_reset: SessionResetPolicy::default(),
            capacity: GatewayCapacityConfig::default(),
        };
        let account = resolve_account(&gateway, Some("feishu")).expect("account");
        let root = PrivateRuntimeRoot::create("environment-test").expect("private root");
        let environment = cli_environment(&account, "short-token", root.path());

        assert_eq!(
            environment["LARKSUITE_CLI_TENANT_ACCESS_TOKEN"],
            "short-token"
        );
        assert!(!environment.contains_key("LARKSUITE_CLI_APP_SECRET"));
        assert!(!environment
            .values()
            .any(|value| value == "permanent-secret"));
    }

    #[test]
    fn output_redaction_covers_permanent_and_short_lived_credentials() {
        assert_eq!(
            redact_secrets(
                "id=cli_test token=short-token secret=permanent-secret",
                &["cli_test", "short-token", "permanent-secret"]
            ),
            "id=<redacted> token=<redacted> secret=<redacted>"
        );
    }

    #[tokio::test]
    async fn live_configured_bot_executes_official_cli_without_forwarding_app_secret() {
        if std::env::var_os("COWD_LIVE_LARK_CLI_TOOL_TEST").is_none() {
            return;
        }
        let workspace_root = std::env::current_dir().expect("workspace root");
        let config = runtime::ConfigLoader::default_for(&workspace_root)
            .load()
            .expect("active Cowd configuration");
        let rejected = execute_lark_cli_tool(
            config.gateway(),
            LarkCliToolRequest {
                args: vec![
                    "im".to_string(),
                    "+chat-create".to_string(),
                    "--help".to_string(),
                ],
                brand: None,
                timeout_ms: Some(10_000),
            },
            LarkCliToolMode::Read,
        )
        .expect_err("read tool must reject an official CLI write command");
        assert!(rejected.contains("mutating command"));
        let output = execute_lark_cli_tool(
            config.gateway(),
            LarkCliToolRequest {
                args: vec![
                    "im".to_string(),
                    "+chat-list".to_string(),
                    "--as".to_string(),
                    "bot".to_string(),
                ],
                brand: None,
                timeout_ms: Some(30_000),
            },
            LarkCliToolMode::Read,
        )
        .expect("configured Cowd bot should execute an official read command");
        let value: Value = serde_json::from_str(&output).expect("structured tool result");
        assert_eq!(value["status"], "ok");
        assert_eq!(value["identity"], "bot");
        assert_eq!(value["risk"], "read");
        assert!(!output.contains("tenant_access_token"));
        assert!(!output.contains("app_secret"));
    }
}
