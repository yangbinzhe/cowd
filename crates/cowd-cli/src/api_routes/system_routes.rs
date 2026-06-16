use std::{fs, sync::Arc};

use axum::{
    extract::State as AxumState,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use runtime::{ConfigLoader, JsonValue};
use serde::Deserialize;

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/tools", get(tools_handler))
        .route(
            "/api/config",
            get(config_handler).put(update_config_handler),
        )
        .route("/api/config/providers", get(config_providers_handler))
        .route("/api/commands", get(commands_handler))
        .route("/api/commands/history", get(commands_history_handler))
        .route("/api/commands/execute", post(commands_execute_handler))
}

#[derive(Deserialize)]
struct UpdateConfigRequest {
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct ExecuteCommandRequest {
    command: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
}

async fn tools_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let tools: Vec<serde_json::Value> = state
        .tool_registry
        .definitions(None)
        .iter()
        .map(|tool| {
            serde_json::json!({
                "name": tool.name,
                "description": tool.description,
                "enabled": true,
            })
        })
        .collect();
    Json(serde_json::json!({ "tools": tools, "count": tools.len() }))
}

async fn config_handler(AxumState(state): AxumState<Arc<AppState>>) -> impl IntoResponse {
    let mut config = load_runtime_config_json(&state).unwrap_or_else(|error| {
        serde_json::json!({
            "error": error,
            "model": "unknown",
            "version": env!("CARGO_PKG_VERSION"),
        })
    });
    redact_config_secrets(&mut config);
    Json(config)
}

async fn update_config_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<UpdateConfigRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(model) = body
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "model is required for config update",
        ));
    };
    let providers = load_runtime_config(&state)?.providers().clone();
    if !providers.is_empty() && providers.resolve_full(model).is_none() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("model `{model}` is not declared by any configured provider"),
        ));
    }

    let path = state.config_home.join("config.yaml");
    fs::create_dir_all(&state.config_home)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let mut value = if path.exists() {
        let raw = fs::read_to_string(&path)
            .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        serde_yaml::from_str::<serde_yaml::Value>(&raw)
            .map_err(|error| api_error(StatusCode::BAD_REQUEST, error.to_string()))?
    } else {
        serde_yaml::Value::Mapping(Default::default())
    };
    let mapping = value.as_mapping_mut().ok_or_else(|| {
        api_error(
            StatusCode::BAD_REQUEST,
            "config root must be a mapping before it can be updated",
        )
    })?;
    mapping.insert(
        serde_yaml::Value::String("model".to_string()),
        serde_yaml::Value::String(model.to_string()),
    );
    let rendered = serde_yaml::to_string(&value)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    fs::write(&path, rendered)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "changed": true,
        "config_path": path.display().to_string(),
        "config": redacted_config_json(load_runtime_config_json(&state).unwrap_or_else(|_| serde_json::json!({"model": model}))),
    })))
}

async fn config_providers_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_config = load_runtime_config(&state)?;
    let providers = runtime_config.providers();
    let configured_model = runtime_config.model().map(str::to_string);
    let configured_model_provider = configured_model
        .as_deref()
        .and_then(|model| providers.resolve_full(model))
        .map(|provider| provider.name.clone());
    let mut provider_rows = providers
        .providers
        .values()
        .map(|provider| {
            serde_json::json!({
                "name": provider.name,
                "base_url": provider.base_url,
                "protocol": provider.protocol,
                "models": provider.models,
                "model_count": provider.models.len(),
                "credential_present": !provider.api_key.trim().is_empty(),
            })
        })
        .collect::<Vec<_>>();
    provider_rows.sort_by(|left, right| {
        left["name"]
            .as_str()
            .unwrap_or("")
            .cmp(right["name"].as_str().unwrap_or(""))
    });
    let selected_model = configured_model.clone();
    let models = provider_rows
        .iter()
        .flat_map(|provider| {
            let provider_name = provider["name"].as_str().unwrap_or("").to_string();
            let selected_model = selected_model.clone();
            provider["models"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(move |model| {
                    model.as_str().map(|id| {
                        serde_json::json!({
                            "id": id,
                            "name": id,
                            "provider": provider_name,
                            "selected": selected_model.as_deref() == Some(id),
                        })
                    })
                })
        })
        .collect::<Vec<_>>();

    Ok(Json(serde_json::json!({
        "providers": provider_rows,
        "models": models,
        "provider_count": providers.providers.len(),
        "provider_model_count": models.len(),
        "configured_model": configured_model,
        "configured_model_provider": configured_model_provider,
        "configured_model_resolved": configured_model.is_none() || configured_model_provider.is_some(),
    })))
}

async fn commands_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "commands": command_registry(),
    }))
}

async fn commands_history_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<serde_json::Value> {
    let path = state.config_home.join("command_history.jsonl");
    let entries = fs::read_to_string(path)
        .ok()
        .map(|raw| {
            raw.lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Json(serde_json::json!({ "history": entries, "total": entries.len() }))
}

async fn commands_execute_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(body): Json<ExecuteCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let command = normalize_command(&body.command);
    let Some(definition) = command_registry()
        .into_iter()
        .find(|item| item["name"].as_str() == Some(command.as_str()))
    else {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("unknown command `{}`", body.command),
        ));
    };
    let receipt = serde_json::json!({
        "ok": true,
        "command": command,
        "args": body.args.unwrap_or_else(|| serde_json::json!({})),
        "action": definition["action"].clone(),
        "target": definition["target"].clone(),
        "executed_at_ms": chrono::Utc::now().timestamp_millis(),
    });
    append_command_history(&state, &receipt);
    Ok(Json(receipt))
}

fn command_registry() -> Vec<serde_json::Value> {
    vec![
        command(
            "/status",
            "Show runtime, session, memory, and gateway status.",
            "open",
            "/runtime",
        ),
        command(
            "/model",
            "Open model selector or switch the current session model.",
            "open",
            "model-modal",
        ),
        command(
            "/workspace",
            "Open workspace browser and file controls.",
            "open",
            "workspace-panel",
        ),
        command(
            "/memory",
            "Open memory search, facts, and packet tools.",
            "open",
            "/memory",
        ),
        command(
            "/context",
            "Open context packet and evidence tools.",
            "open",
            "/context",
        ),
        command(
            "/skills",
            "Open skills catalog and run console.",
            "open",
            "/skills",
        ),
        command(
            "/agents",
            "Open tasks and agent work graph.",
            "open",
            "/agents",
        ),
        command(
            "/gateway",
            "Open channel, connector, and cross-plane controls.",
            "open",
            "/gateway",
        ),
        command(
            "/settings",
            "Open settings and provider/profile controls.",
            "open",
            "/settings",
        ),
        command(
            "/clear",
            "Clear the local composer input.",
            "client",
            "composer",
        ),
        command(
            "/compact",
            "Compact the current session.",
            "api",
            "/api/sessions/:id/compact",
        ),
    ]
}

fn command(name: &str, description: &str, action: &str, target: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "description": description,
        "action": action,
        "target": target,
        "surface": ["webui", "tui"],
    })
}

fn normalize_command(command: &str) -> String {
    let trimmed = command.trim();
    let first = trimmed.split_whitespace().next().unwrap_or(trimmed);
    if first.starts_with('/') {
        first.to_string()
    } else {
        format!("/{first}")
    }
}

fn append_command_history(state: &AppState, receipt: &serde_json::Value) {
    if fs::create_dir_all(&state.config_home).is_err() {
        return;
    }
    let path = state.config_home.join("command_history.jsonl");
    let Ok(line) = serde_json::to_string(receipt) else {
        return;
    };
    let mut options = fs::OpenOptions::new();
    let _ = options
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| {
            use std::io::Write;
            writeln!(file, "{line}")
        });
}

fn load_runtime_config(
    state: &AppState,
) -> Result<runtime::RuntimeConfig, (StatusCode, Json<ErrorResponse>)> {
    ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

fn load_runtime_config_json(state: &AppState) -> Result<serde_json::Value, String> {
    let runtime_config = ConfigLoader::new(&state.workspace_root, &state.config_home)
        .load()
        .map_err(|error| error.to_string())?;
    Ok(json_value_to_serde(&runtime_config.as_json()))
}

fn redacted_config_json(mut value: serde_json::Value) -> serde_json::Value {
    redact_config_secrets(&mut value);
    value
}

fn redact_config_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                let normalized = key.to_ascii_lowercase();
                if normalized.contains("api_key")
                    || normalized == "token"
                    || normalized.ends_with("_token")
                    || normalized == "secret"
                    || normalized.ends_with("_secret")
                    || normalized == "password"
                {
                    *item = serde_json::Value::String("[redacted]".to_string());
                } else {
                    redact_config_secrets(item);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_config_secrets(item);
            }
        }
        _ => {}
    }
}

fn json_value_to_serde(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(value) => serde_json::Value::Bool(*value),
        JsonValue::Number(value) => serde_json::Value::Number((*value).into()),
        JsonValue::String(value) => serde_json::Value::String(value.clone()),
        JsonValue::Array(values) => {
            serde_json::Value::Array(values.iter().map(json_value_to_serde).collect())
        }
        JsonValue::Object(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), json_value_to_serde(value)))
                .collect(),
        ),
    }
}
