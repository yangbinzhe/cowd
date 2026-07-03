use super::*;

#[derive(Debug, Clone, Serialize)]
pub(super) struct McpServerReadiness {
    pub(super) name: String,
    transport: String,
    enabled: bool,
    status: &'static str,
    configured: bool,
    missing_required: Vec<String>,
    diagnostics: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    probe: Option<McpServerProbe>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct McpServerProbe {
    requested: bool,
    mode: &'static str,
    status: &'static str,
    timeout_ms: u64,
    diagnostics: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct McpServerQuery {
    #[serde(default)]
    probe: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

pub(super) fn configured_mcp_servers(
    config: Option<&serde_json::Value>,
) -> Vec<McpServerReadiness> {
    let Some(servers) = config
        .and_then(|value| value.get("mcpServers"))
        .and_then(serde_json::Value::as_object)
    else {
        return Vec::new();
    };

    let mut items = servers
        .iter()
        .map(|(name, value)| mcp_server_readiness_from_value(name, value))
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left.name.cmp(&right.name));
    items
}

fn mcp_server_readiness_from_value(name: &str, value: &serde_json::Value) -> McpServerReadiness {
    let transport = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| infer_mcp_transport(value).to_string());
    let enabled = value
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let missing_required = missing_mcp_required_fields(&transport, value);
    let configured = missing_required.is_empty();
    let status = if !enabled {
        "disabled"
    } else if configured {
        "ready"
    } else {
        "degraded"
    };
    let diagnostics = if configured {
        vec![
            "MCP server declared; live discovery is evaluated outside control-plane snapshot"
                .to_string(),
        ]
    } else {
        vec![format!(
            "missing required fields: {}",
            missing_required.join(", ")
        )]
    };
    McpServerReadiness {
        name: name.to_string(),
        transport,
        enabled,
        status,
        configured,
        missing_required,
        diagnostics,
        probe: None,
    }
}

fn infer_mcp_transport(value: &serde_json::Value) -> &'static str {
    if value.get("command").is_some() {
        "stdio"
    } else if value.get("url").is_some() {
        "http"
    } else if value.get("name").is_some() {
        "sdk"
    } else {
        "unknown"
    }
}

fn missing_mcp_required_fields(transport: &str, value: &serde_json::Value) -> Vec<String> {
    let required: &[&str] = match transport {
        "stdio" => &["command"],
        "http" | "sse" | "ws" | "claudeai-proxy" => &["url"],
        "sdk" => &["name"],
        _ => &["type"],
    };
    required
        .iter()
        .filter(|field| !has_non_empty(value, field))
        .map(|field| (*field).to_string())
        .collect()
}

fn has_non_empty(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .is_some_and(|item| !item.is_empty())
}

pub(super) fn account_from_mcp_server(server: &McpServerReadiness) -> ProviderAccount {
    let health = match server.status {
        "ready" => ConnectorHealth::ready(),
        "disabled" => ConnectorHealth::disabled("MCP server is disabled"),
        "degraded" => ConnectorHealth::degraded(format!(
            "missing required fields: {}",
            server.missing_required.join(", ")
        )),
        other => ConnectorHealth::degraded(format!("MCP server status is {other}")),
    };
    ProviderAccount::mcp_server(server.name.clone(), server.transport.clone(), health)
}

pub(super) async fn connector_summary_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    Json(serde_json::json!({
        "kind": "connector_summary",
        "summary": snapshot.summary(),
        "generated_at": snapshot.generated_at,
    }))
}

pub(super) async fn connector_accounts_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    let total = snapshot.accounts.len();
    Json(serde_json::json!({
        "kind": "connector_accounts",
        "accounts": snapshot.accounts,
        "total": total,
    }))
}

pub(super) async fn connector_capabilities_handler(
    AxumState(state): AxumState<Arc<AppState>>,
) -> impl IntoResponse {
    let snapshot = connector_snapshot(&state);
    let total = snapshot.capabilities.len();
    Json(serde_json::json!({
        "kind": "connector_capabilities",
        "capabilities": snapshot.capabilities,
        "total": total,
    }))
}

pub(super) async fn mcp_servers_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<McpServerQuery>,
) -> impl IntoResponse {
    let config = state.runtime_config_json_snapshot();
    let mut servers = configured_mcp_servers(config.as_ref());
    let timeout_ms = query.timeout_ms.unwrap_or(300).clamp(50, 2_000);
    if query.probe {
        apply_mcp_probe_results(&mut servers, config.as_ref(), timeout_ms).await;
    }
    let ready = servers
        .iter()
        .filter(|server| server.status == "ready")
        .count();
    let degraded = servers
        .iter()
        .filter(|server| server.status == "degraded")
        .count();
    let disabled = servers
        .iter()
        .filter(|server| server.status == "disabled")
        .count();
    Json(serde_json::json!({
        "kind": "connector_mcp_servers",
        "probe": {
            "requested": query.probe,
            "timeout_ms": timeout_ms,
            "policy": "bounded_http_only",
        },
        "summary": {
            "total": servers.len(),
            "ready": ready,
            "degraded": degraded,
            "disabled": disabled,
        },
        "servers": servers,
    }))
}

async fn apply_mcp_probe_results(
    servers: &mut [McpServerReadiness],
    config: Option<&serde_json::Value>,
    timeout_ms: u64,
) {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            for server in servers {
                server.probe = Some(McpServerProbe {
                    requested: true,
                    mode: "client_init",
                    status: "error",
                    timeout_ms,
                    diagnostics: vec![error.to_string()],
                });
            }
            return;
        }
    };

    for server in servers {
        server.probe = Some(
            probe_mcp_server(&client, config, server, timeout_ms)
                .await
                .unwrap_or_else(|diagnostic| McpServerProbe {
                    requested: true,
                    mode: "bounded",
                    status: "error",
                    timeout_ms,
                    diagnostics: vec![diagnostic],
                }),
        );
    }
}

async fn probe_mcp_server(
    client: &reqwest::Client,
    config: Option<&serde_json::Value>,
    server: &McpServerReadiness,
    timeout_ms: u64,
) -> Result<McpServerProbe, String> {
    if !server.enabled {
        return Ok(McpServerProbe {
            requested: true,
            mode: "skipped",
            status: "disabled",
            timeout_ms,
            diagnostics: vec!["server disabled".to_string()],
        });
    }
    if !server.configured {
        return Ok(McpServerProbe {
            requested: true,
            mode: "skipped",
            status: "degraded",
            timeout_ms,
            diagnostics: vec![format!(
                "missing required fields: {}",
                server.missing_required.join(", ")
            )],
        });
    }

    match server.transport.as_str() {
        "http" | "sse" | "ws" | "claudeai-proxy" => {
            let Some(url) = mcp_server_config_value(config, &server.name)
                .and_then(|value| value.get("url"))
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|url| !url.is_empty())
            else {
                return Ok(McpServerProbe {
                    requested: true,
                    mode: "bounded_http",
                    status: "degraded",
                    timeout_ms,
                    diagnostics: vec!["url missing".to_string()],
                });
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                client.get(url).send(),
            )
            .await;
            match result {
                Ok(Ok(response)) => {
                    let status = response.status();
                    Ok(McpServerProbe {
                        requested: true,
                        mode: "bounded_http",
                        status: if status.is_server_error() {
                            "degraded"
                        } else {
                            "reachable"
                        },
                        timeout_ms,
                        diagnostics: vec![format!("http_status: {}", status.as_u16())],
                    })
                }
                Ok(Err(error)) => Ok(McpServerProbe {
                    requested: true,
                    mode: "bounded_http",
                    status: "unreachable",
                    timeout_ms,
                    diagnostics: vec![error.to_string()],
                }),
                Err(_) => Ok(McpServerProbe {
                    requested: true,
                    mode: "bounded_http",
                    status: "timeout",
                    timeout_ms,
                    diagnostics: vec!["probe timed out".to_string()],
                }),
            }
        }
        "stdio" | "sdk" => Ok(McpServerProbe {
            requested: true,
            mode: "config_only",
            status: "declared",
            timeout_ms,
            diagnostics: vec![
                "live process discovery is intentionally not started from control-plane probe"
                    .to_string(),
            ],
        }),
        other => Ok(McpServerProbe {
            requested: true,
            mode: "skipped",
            status: "unsupported",
            timeout_ms,
            diagnostics: vec![format!("unsupported transport: {other}")],
        }),
    }
}

fn mcp_server_config_value<'a>(
    config: Option<&'a serde_json::Value>,
    name: &str,
) -> Option<&'a serde_json::Value> {
    config
        .and_then(|value| value.get("mcpServers"))
        .and_then(serde_json::Value::as_object)
        .and_then(|servers| servers.get(name))
}
