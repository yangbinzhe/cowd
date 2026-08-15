//! Live, product-neutral APP administration through the Gateway boundary.

use std::{path::PathBuf, process::ExitCode, time::Duration};

use cowd_app_protocol::{
    app_operation_catalog_digest_v1, AppCatalogEntryV1, AppCatalogV1, AppLifecycleStateV1,
    AppManifestV1, OperationDescriptorV1, ProtocolValidate,
};
use reqwest::blocking::{Client, RequestBuilder};
use serde::Deserialize;

const DEFAULT_GATEWAY_URL: &str = "http://127.0.0.1:8642";
const EXIT_USAGE: u8 = 64;
const EXIT_UNAVAILABLE: u8 = 69;
const EXIT_CONTRACT: u8 = 70;

pub(super) fn entry(args: &[String]) -> ExitCode {
    match run(args, &GatewayAppsClient::from_environment()) {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("apps command failed [{}]: {}", error.code(), error);
            ExitCode::from(error.exit_code())
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum AppsCliError {
    #[error("{0}")]
    Usage(&'static str),
    #[error("Gateway is unavailable: {0}")]
    Unavailable(String),
    #[error("Gateway rejected the request with HTTP {status}: {code}: {detail}")]
    Rejected {
        status: u16,
        code: String,
        detail: String,
    },
    #[error("Gateway returned an incompatible APP contract: {0}")]
    Incompatible(String),
    #[error("APP health check failed: {0}")]
    Unhealthy(String),
}

impl AppsCliError {
    const fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) => EXIT_USAGE,
            Self::Unavailable(_) => EXIT_UNAVAILABLE,
            Self::Rejected { .. } | Self::Incompatible(_) | Self::Unhealthy(_) => EXIT_CONTRACT,
        }
    }

    const fn code(&self) -> &'static str {
        match self {
            Self::Usage(_) => "usage",
            Self::Unavailable(_) => "gateway_unavailable",
            Self::Rejected { .. } => "gateway_rejected",
            Self::Incompatible(_) => "app_incompatible",
            Self::Unhealthy(_) => "app_unhealthy",
        }
    }
}

trait AppsApi {
    fn get(&self, path: &str) -> Result<serde_json::Value, AppsCliError>;
    fn post(&self, path: &str) -> Result<serde_json::Value, AppsCliError>;
}

struct GatewayAppsClient {
    base_url: String,
    token: Option<String>,
    client: Result<Client, String>,
}

impl GatewayAppsClient {
    fn from_environment() -> Self {
        let base_url = std::env::var("COWD_GATEWAY_URL")
            .unwrap_or_else(|_| DEFAULT_GATEWAY_URL.to_owned())
            .trim_end_matches('/')
            .to_owned();
        let token = default_auth_token();
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|error| error.to_string());
        Self {
            base_url,
            token,
            client,
        }
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        let request = request.header("x-cowd-surface-id", "cli");
        match self
            .token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            Some(token) => request.bearer_auth(token.trim()),
            None => request,
        }
    }

    fn execute(&self, request: RequestBuilder) -> Result<serde_json::Value, AppsCliError> {
        let response = self
            .authorize(request)
            .send()
            .map_err(|error| AppsCliError::Unavailable(error.to_string()))?;
        let status = response.status();
        let value = response
            .json::<serde_json::Value>()
            .map_err(|error| AppsCliError::Incompatible(error.to_string()))?;
        if status.is_success() {
            return Ok(value);
        }
        let code = value["error"]["code"]
            .as_str()
            .unwrap_or("gateway_error")
            .to_owned();
        let detail = value["error"]["detail"]
            .as_str()
            .unwrap_or("Gateway returned no typed error detail")
            .to_owned();
        Err(AppsCliError::Rejected {
            status: status.as_u16(),
            code,
            detail,
        })
    }
}

impl AppsApi for GatewayAppsClient {
    fn get(&self, path: &str) -> Result<serde_json::Value, AppsCliError> {
        let client = self
            .client
            .as_ref()
            .map_err(|error| AppsCliError::Unavailable(error.clone()))?;
        self.execute(client.get(format!("{}{}", self.base_url, path)))
    }

    fn post(&self, path: &str) -> Result<serde_json::Value, AppsCliError> {
        let client = self
            .client
            .as_ref()
            .map_err(|error| AppsCliError::Unavailable(error.clone()))?;
        self.execute(client.post(format!("{}{}", self.base_url, path)))
    }
}

fn run(args: &[String], api: &dyn AppsApi) -> Result<String, AppsCliError> {
    let (args, json) = parse_output(args)?;
    let action = args.first().map(String::as_str).unwrap_or("list");
    match (action, args.get(1..).unwrap_or_default()) {
        ("list", []) => render_catalog(load_catalog(api)?, json),
        ("status", [app_id]) => render_status(find_app(&load_catalog(api)?, app_id)?, json),
        ("doctor", []) => doctor(api, None, json),
        ("doctor", [app_id]) => doctor(api, Some(app_id), json),
        ("logs", [app_id]) => render_passthrough(
            api.get(&format!("/api/apps/{}/logs", encode(app_id)))?,
            json,
        ),
        ("restart", [app_id]) => render_passthrough(
            api.post(&format!("/api/apps/{}/restart", encode(app_id)))?,
            json,
        ),
        _ => Err(AppsCliError::Usage(
            "usage: cowd apps list|status <id>|doctor [id]|logs <id>|restart <id> [--output-format text|json]",
        )),
    }
}

fn parse_output(args: &[String]) -> Result<(Vec<String>, bool), AppsCliError> {
    let mut result = Vec::new();
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--output-format" => {
                let value = args
                    .get(index + 1)
                    .ok_or(AppsCliError::Usage("--output-format requires text or json"))?;
                json = parse_output_value(value)?;
                index += 2;
            }
            value if value.starts_with("--output-format=") => {
                json = parse_output_value(&value["--output-format=".len()..])?;
                index += 1;
            }
            value => {
                result.push(value.to_owned());
                index += 1;
            }
        }
    }
    Ok((result, json))
}

fn parse_output_value(value: &str) -> Result<bool, AppsCliError> {
    match value {
        "text" => Ok(false),
        "json" => Ok(true),
        _ => Err(AppsCliError::Usage("--output-format requires text or json")),
    }
}

fn load_catalog(api: &dyn AppsApi) -> Result<AppCatalogV1, AppsCliError> {
    let catalog: AppCatalogV1 = serde_json::from_value(api.get("/api/apps")?)
        .map_err(|error| AppsCliError::Incompatible(error.to_string()))?;
    catalog
        .validate()
        .map_err(|error| AppsCliError::Incompatible(error.to_string()))?;
    Ok(catalog)
}

fn find_app<'a>(
    catalog: &'a AppCatalogV1,
    app_id: &str,
) -> Result<&'a AppCatalogEntryV1, AppsCliError> {
    catalog
        .apps
        .iter()
        .find(|app| app.app_id.0 == app_id)
        .ok_or_else(|| AppsCliError::Unhealthy(format!("APP `{app_id}` is not mounted")))
}

fn doctor(
    api: &dyn AppsApi,
    selected: Option<&String>,
    json: bool,
) -> Result<String, AppsCliError> {
    let catalog = load_catalog(api)?;
    let entries = match selected {
        Some(app_id) => vec![find_app(&catalog, app_id)?],
        None => catalog.apps.iter().collect(),
    };
    let mut checked = Vec::with_capacity(entries.len());
    for entry in entries {
        if matches!(
            entry.lifecycle.state,
            AppLifecycleStateV1::Failed
                | AppLifecycleStateV1::Invalid
                | AppLifecycleStateV1::CircuitOpen
                | AppLifecycleStateV1::ProtocolIncompatible
        ) {
            return Err(AppsCliError::Unhealthy(format!(
                "APP `{}` is {:?}",
                entry.app_id.0, entry.lifecycle.state
            )));
        }
        let detail: AppDetailV1 =
            serde_json::from_value(api.get(&format!("/api/apps/{}", encode(&entry.app_id.0)))?)
                .map_err(|error| AppsCliError::Incompatible(error.to_string()))?;
        detail.validate(entry)?;
        checked.push(serde_json::json!({
            "app_id": entry.app_id,
            "generation": entry.generation,
            "status": "compatible"
        }));
    }
    let result = serde_json::json!({
        "schema_version": 1,
        "ready": true,
        "checked": checked.len(),
        "apps": checked,
    });
    if json {
        pretty(&result)
    } else {
        Ok(format!(
            "APP doctor\n  Status           ready\n  Checked          {}",
            checked.len()
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppDetailV1 {
    schema_version: u16,
    entry: AppCatalogEntryV1,
    manifest: AppManifestV1,
    operations: Vec<OperationDescriptorV1>,
}

impl AppDetailV1 {
    fn validate(&self, expected: &AppCatalogEntryV1) -> Result<(), AppsCliError> {
        self.entry
            .validate()
            .and_then(|()| self.manifest.validate())
            .map_err(|error| AppsCliError::Incompatible(error.to_string()))?;
        let digest = app_operation_catalog_digest_v1(&self.manifest.app_id, &self.operations)
            .map_err(|error| AppsCliError::Incompatible(error.to_string()))?;
        if self.schema_version != 1
            || &self.entry != expected
            || self.entry.app_id != self.manifest.app_id
            || self.entry.artifact_version != self.manifest.artifact_version
            || digest != self.manifest.operation_catalog_digest
        {
            return Err(AppsCliError::Incompatible(
                "detail identity or signed operation digest mismatch".to_owned(),
            ));
        }
        Ok(())
    }
}

fn render_catalog(catalog: AppCatalogV1, json: bool) -> Result<String, AppsCliError> {
    if json {
        return pretty(&catalog);
    }
    let mut output = format!("Applications\n  Count            {}", catalog.apps.len());
    for app in catalog.apps {
        output.push_str(&format!(
            "\n  - {} ({}, {:?})",
            app.app_id.0, app.artifact_version, app.lifecycle.state
        ));
    }
    Ok(output)
}

fn render_status(entry: &AppCatalogEntryV1, json: bool) -> Result<String, AppsCliError> {
    if json {
        return pretty(entry);
    }
    Ok(format!(
        "Application {}\n  Version          {}\n  Generation       {}\n  Lifecycle        {:?}\n  Activation       {:?}",
        entry.app_id.0,
        entry.artifact_version,
        entry.generation.0,
        entry.lifecycle.state,
        entry.activation
    ))
}

fn render_passthrough(value: serde_json::Value, _json: bool) -> Result<String, AppsCliError> {
    pretty(&value)
}

fn pretty(value: &impl serde::Serialize) -> Result<String, AppsCliError> {
    serde_json::to_string_pretty(value)
        .map_err(|error| AppsCliError::Incompatible(error.to_string()))
}

fn encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn default_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
        .or_else(|| {
            let config_path: PathBuf = runtime::cowd_dirs::config_home_dir().join("config.yaml");
            let config = std::fs::read_to_string(config_path).ok()?;
            config.lines().find_map(|line| {
                let token = line.trim().strip_prefix("token:")?.trim().trim_matches('"');
                (!token.is_empty()).then(|| token.to_owned())
            })
        })
        .map(|token| token.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct FakeApi {
        responses: BTreeMap<String, serde_json::Value>,
    }

    impl AppsApi for FakeApi {
        fn get(&self, path: &str) -> Result<serde_json::Value, AppsCliError> {
            self.responses
                .get(&format!("GET {path}"))
                .cloned()
                .ok_or_else(|| AppsCliError::Unavailable(path.to_owned()))
        }

        fn post(&self, path: &str) -> Result<serde_json::Value, AppsCliError> {
            self.responses
                .get(&format!("POST {path}"))
                .cloned()
                .ok_or_else(|| AppsCliError::Unavailable(path.to_owned()))
        }
    }

    fn catalog(states: &[(&str, &str)]) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "protocol_revision": cowd_app_protocol::PROTOCOL_REVISION_V1,
            "protocol_digest": format!("sha256:{}", "1".repeat(64)),
            "catalog_generation": format!("sha256:{}", "2".repeat(64)),
            "apps": states.iter().map(|(id, state)| serde_json::json!({
                "app_id": id,
                "display_name": id,
                "artifact_version": "1.0.0",
                "generation": format!("sha256:{}", "3".repeat(64)),
                "required": false,
                "activation": "lazy",
                "lifecycle": {"state": state, "retryable": *state == "failed" || *state == "circuit_open"},
                "compatibility": {
                    "status": if *state == "protocol_incompatible" { "protocol_incompatible" } else { "compatible" },
                    "gateway_supported_minimum": cowd_app_protocol::PROTOCOL_REVISION_V1,
                    "gateway_supported_maximum": cowd_app_protocol::PROTOCOL_REVISION_V1,
                    "app_required_minimum": cowd_app_protocol::PROTOCOL_REVISION_V1,
                    "app_required_maximum": cowd_app_protocol::PROTOCOL_REVISION_V1
                },
                "web_surface": {"available": false, "bridge_revision": 1},
                "effective_capabilities": [format!("{id}.read")],
                "effective_authorization_profile": "default"
            })).collect::<Vec<_>>()
        })
    }

    #[test]
    fn list_handles_zero_and_many_apps_without_product_assumptions() {
        for states in [
            vec![],
            vec![("reference-app", "mounted"), ("audit-app", "ready")],
        ] {
            let api = FakeApi {
                responses: BTreeMap::from([("GET /api/apps".to_owned(), catalog(&states))]),
            };
            let output = run(&["list".to_owned()], &api).expect("list");
            assert!(output.contains(&format!("Count            {}", states.len())));
        }
    }

    #[test]
    fn doctor_fails_closed_for_failed_circuit_and_incompatible_catalogs() {
        for state in ["failed", "circuit_open", "protocol_incompatible"] {
            let api = FakeApi {
                responses: BTreeMap::from([(
                    "GET /api/apps".to_owned(),
                    catalog(&[("reference-app", state)]),
                )]),
            };
            let error = run(&["doctor".to_owned()], &api).expect_err("unhealthy APP");
            assert!(matches!(&error, AppsCliError::Unhealthy(_)));
            assert_eq!(error.exit_code(), EXIT_CONTRACT);
        }
        let api = FakeApi {
            responses: BTreeMap::from([("GET /api/apps".to_owned(), serde_json::json!({}))]),
        };
        let error = run(&["doctor".to_owned()], &api).expect_err("incompatible catalog");
        assert!(matches!(&error, AppsCliError::Incompatible(_)));
        assert_eq!(error.exit_code(), EXIT_CONTRACT);
    }

    #[test]
    fn logs_and_restart_use_only_generic_encoded_routes() {
        let api = FakeApi {
            responses: BTreeMap::from([
                (
                    "GET /api/apps/reference-app/logs".to_owned(),
                    serde_json::json!({"schema_version":1,"app_id":"reference-app"}),
                ),
                (
                    "POST /api/apps/reference-app/restart".to_owned(),
                    serde_json::json!({"schema_version":1,"app_id":"reference-app"}),
                ),
            ]),
        };
        assert!(run(&["logs".to_owned(), "reference-app".to_owned()], &api).is_ok());
        assert!(run(&["restart".to_owned(), "reference-app".to_owned()], &api).is_ok());
    }
}
