use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;

use super::{connector_routes, AppState, ErrorResponse};
use memory::store::session::SessionListOptions;
use memory::RuntimeEvent;
use runtime::{init_global_providers, AgentControlPolicy, RuntimeConfig};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/runtime/timeline", get(get_runtime_timeline))
        .route(
            "/api/runtime/config/effective",
            get(get_runtime_effective_config),
        )
        .route(
            "/api/runtime/providers/reload",
            post(reload_runtime_providers),
        )
        .route("/api/runtime/status", get(get_runtime_status))
        .route("/api/runtime/snapshot", get(get_runtime_snapshot))
        .route("/api/runtime/control-plane", get(get_runtime_control_plane))
        .route(
            "/api/runtime/sessions/:id/attach",
            post(attach_runtime_session),
        )
        .route(
            "/api/runtime/sessions/:id/detach",
            post(detach_runtime_session),
        )
        .route(
            "/api/runtime/sessions/:id/lifecycle",
            get(get_runtime_session_lifecycle),
        )
        .route(
            "/api/runtime/sessions/:id/replay",
            get(replay_runtime_session),
        )
        .route(
            "/api/runtime/session-leases",
            get(get_runtime_session_leases),
        )
        .route(
            "/api/runtime/session-leases/acquire",
            post(acquire_runtime_session_lease),
        )
        .route(
            "/api/runtime/session-leases/release",
            post(release_runtime_session_lease),
        )
}

#[derive(Deserialize)]
pub(super) struct RuntimeTimelineParams {
    session_id: String,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct RuntimeSessionLeaseAcquireRequest {
    session_id: String,
    owner: String,
    #[serde(default)]
    mode: Option<String>,
}

#[derive(Deserialize)]
struct RuntimeSessionLeaseReleaseRequest {
    session_id: String,
    owner: String,
}

#[derive(Deserialize)]
struct RuntimeSessionAttachRequest {
    actor_id: String,
    surface: String,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Deserialize)]
struct RuntimeSessionDetachRequest {
    actor_id: String,
}

#[derive(Deserialize)]
struct RuntimeSessionReplayParams {
    #[serde(default)]
    from_sequence: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

pub(super) async fn get_runtime_status(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.status_value()),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

pub(super) async fn get_runtime_snapshot(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.snapshot_value().await),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn attach_runtime_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<RuntimeSessionAttachRequest>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(
            runtime
                .attach_session_value(&id, &body.actor_id, &body.surface, body.role.as_deref())
                .await,
        ),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn detach_runtime_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<RuntimeSessionDetachRequest>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.detach_session_value(&id, &body.actor_id).await),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn get_runtime_session_lifecycle(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(runtime.lifecycle_snapshot_value(Some(&id)).await),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

async fn replay_runtime_session(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<RuntimeSessionReplayParams>,
) -> Json<Value> {
    match state.services.runtime.as_ref() {
        Some(runtime) => Json(
            runtime
                .replay_session_value(
                    &id,
                    params.from_sequence.unwrap_or(0),
                    params.limit.unwrap_or(100),
                )
                .await,
        ),
        None => Json(serde_json::json!({
            "ok": false,
            "error": "runtime service unavailable",
        })),
    }
}

pub(super) async fn get_runtime_timeline(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(params): Query<RuntimeTimelineParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let from_seq = params.from_seq.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).min(500);
    let agent_policy = load_agent_control_policy(&state);
    let page = state
        .services
        .session
        .stored_timeline_runtime_page(&params.session_id, from_seq, limit)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("failed to load runtime timeline: {e}"),
                }),
            )
        })?;

    let Some(page) = page else {
        return Ok(Json(serde_json::json!({
            "session_id": params.session_id,
            "events": [],
            "total": 0,
            "from_seq": from_seq,
            "next_seq": null,
            "limit": limit,
            "has_more": false,
            "degraded": true,
            "degraded_reason": "session store not available",
            "workgraph_summary": empty_workgraph_summary(),
            "health_summary": degraded_health_summary("session store not available"),
            "value_loop": degraded_value_loop_summary("session store not available"),
            "agent_value": degraded_agent_value_summary(&agent_policy, "session store not available"),
        })));
    };

    let workgraph_summary = workgraph_summary(&page.events);
    let health_summary = health_summary(&page.events, false, None);
    let value_loop = value_loop_summary(&page.events, false, None);
    let agent_value = agent_value_summary(&page.events, &agent_policy, false, None);

    Ok(Json(serde_json::json!({
        "session_id": params.session_id,
        "events": page.events,
        "total": page.total,
        "from_seq": from_seq,
        "next_seq": page.next_seq,
        "limit": limit,
        "has_more": page.has_more,
        "degraded": false,
        "degraded_reason": null,
        "workgraph_summary": workgraph_summary,
        "health_summary": health_summary,
        "value_loop": value_loop,
        "agent_value": agent_value,
    })))
}

fn load_agent_control_policy(state: &AppState) -> AgentControlPolicy {
    state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .map(|config| config.runtime_control().policy.agent.clone())
        .unwrap_or_else(|error| {
            tracing::warn!(
                target: "cowd.runtime.agent_value",
                error = %error,
                "failed to load agent control policy; using defaults"
            );
            AgentControlPolicy::default()
        })
}

pub(super) async fn get_runtime_effective_config(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let (source, runtime_config, warnings) = match state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
    {
        Ok(config) => {
            let source = if config.loaded_entries().is_empty() {
                "default"
            } else {
                "config"
            };
            (source, config, Vec::<String>::new())
        }
        Err(error) => (
            "default",
            RuntimeConfig::empty(),
            vec![format!("failed to load runtime config: {error}")],
        ),
    };
    let control = runtime_config.runtime_control();
    Json(serde_json::json!({
        "source": source,
        "workspace_root": state.workspace_root,
        "profile_id": state.profile_id,
        "scenario": control.scenario.as_str(),
        "control_policy": control.policy,
        "warnings": warnings,
    }))
}

pub(super) async fn reload_runtime_providers(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let loaded = state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home);
    match loaded {
        Ok(runtime_config) => {
            let source = if runtime_config.loaded_entries().is_empty() {
                "default"
            } else {
                "config"
            };
            let providers = runtime_config.providers().clone();
            let provider_count = providers.providers.len();
            let provider_model_count: usize = providers
                .providers
                .values()
                .map(|provider| provider.models.len())
                .sum();
            let configured_model = runtime_config.model().map(str::to_string);
            let configured_model_provider = configured_model
                .as_deref()
                .and_then(|model| providers.resolve_full(model))
                .map(|provider| provider.name.clone());
            let configured_model_resolved =
                configured_model.is_none() || configured_model_provider.is_some();
            let mut provider_names: Vec<String> = providers.providers.keys().cloned().collect();
            provider_names.sort();

            init_global_providers(providers);

            tracing::info!(
                target: "cowd.runtime.provider",
                applied = true,
                source,
                provider_count,
                provider_model_count,
                configured_model = configured_model.as_deref().unwrap_or(""),
                configured_model_provider = configured_model_provider.as_deref().unwrap_or(""),
                configured_model_resolved,
                "runtime providers reloaded"
            );

            Json(serde_json::json!({
                "kind": "runtime_provider_reload",
                "status": if provider_count == 0 { "unconfigured" } else if configured_model_resolved { "applied" } else { "attention" },
                "applied": true,
                "source": source,
                "provider_count": provider_count,
                "provider_model_count": provider_model_count,
                "provider_names": provider_names,
                "configured_model": configured_model,
                "configured_model_provider": configured_model_provider,
                "configured_model_resolved": configured_model_resolved,
                "warnings": if provider_count == 0 {
                    serde_json::json!(["no runtime providers are configured"])
                } else if !configured_model_resolved {
                    serde_json::json!(["configured default model is not declared by any provider"])
                } else {
                    serde_json::json!([])
                }
            }))
        }
        Err(error) => {
            let message = format!("failed to load runtime config: {error}");
            tracing::warn!(
                target: "cowd.runtime.provider",
                applied = false,
                error = %error,
                "runtime providers reload skipped"
            );
            Json(serde_json::json!({
                "kind": "runtime_provider_reload",
                "status": "failed",
                "applied": false,
                "source": "error",
                "provider_count": 0,
                "provider_model_count": 0,
                "provider_names": [],
                "configured_model": null,
                "configured_model_provider": null,
                "configured_model_resolved": false,
                "warnings": [message]
            }))
        }
    }
}

async fn get_runtime_session_leases(AxumState(state): AxumState<Arc<AppState>>) -> Json<Value> {
    Json(session_lease_projection(&state).await)
}

async fn acquire_runtime_session_lease(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<RuntimeSessionLeaseAcquireRequest>,
) -> Json<Value> {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return Json(serde_json::json!({
            "ok": false,
            "error": "session lease registry is not attached",
        }));
    };
    let mode = request.mode.as_deref().unwrap_or("collaborative");
    Json(
        registry
            .acquire(&request.session_id, &request.owner, mode)
            .await,
    )
}

async fn release_runtime_session_lease(
    AxumState(state): AxumState<Arc<AppState>>,
    Json(request): Json<RuntimeSessionLeaseReleaseRequest>,
) -> Json<Value> {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return Json(serde_json::json!({
            "ok": false,
            "error": "session lease registry is not attached",
        }));
    };
    Json(registry.release(&request.session_id, &request.owner).await)
}

pub(super) async fn get_runtime_control_plane(
    AxumState(state): AxumState<Arc<AppState>>,
) -> Json<Value> {
    let started_at = Instant::now();
    let (config_source, runtime_config, config_warnings) = match state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
    {
        Ok(config) => {
            let source = if config.loaded_entries().is_empty() {
                "default"
            } else {
                "config"
            };
            (source, config, Vec::<String>::new())
        }
        Err(error) => (
            "default",
            RuntimeConfig::empty(),
            vec![format!("failed to load runtime config: {error}")],
        ),
    };
    let control = runtime_config.runtime_control();
    let providers = runtime_config.providers();
    let provider_count = providers.providers.len();
    let provider_configured = provider_count > 0;
    let provider_model_count: usize = providers
        .providers
        .values()
        .map(|provider| provider.models.len())
        .sum();
    let mut provider_names: Vec<String> = providers.providers.keys().cloned().collect();
    provider_names.sort();
    let configured_model = runtime_config.model().map(str::to_string);
    let configured_model_provider = configured_model
        .as_deref()
        .and_then(|model| providers.resolve_full(model))
        .map(|provider| provider.name.clone());
    let configured_model_resolved =
        configured_model.is_none() || configured_model_provider.is_some();
    let provider_status = if !provider_configured {
        "unconfigured"
    } else if configured_model_resolved {
        "available"
    } else {
        "degraded"
    };
    let active_session_ids = state.services.session.list_active_session_ids();
    let active_session_count = active_session_ids.len();
    let durable_session_store = state.services.session.has_unified_store();
    let stored_session_count = match state
        .services
        .session
        .list_stored_sessions_page(&SessionListOptions {
            sort: "last_activity",
            order: "desc",
            limit: 1,
            offset: 0,
            ..Default::default()
        })
        .await
    {
        Ok(Some(page)) => Some(page.total),
        Ok(None) => None,
        Err(error) => {
            tracing::warn!(
                target: "cowd.runtime.control_plane",
                error = %error,
                "runtime control plane stored session count unavailable"
            );
            None
        }
    };
    let task_records = state.services.task.list_records().unwrap_or_default();
    let open_tasks = task_records
        .iter()
        .filter(|task| matches!(task.status.as_str(), "pending" | "running" | "reviewing"))
        .count();

    let memory = if let Some(memory_manager) = state.services.memory.manager() {
        serde_json::json!({
            "status": "available",
            "enabled": true,
            "search_mode": memory_manager.search_mode_label(),
            "vector_index_count": memory_manager.vector_index_count(),
            "ctx_health": format!("{:?}", memory_manager.ctx_health()),
        })
    } else {
        serde_json::json!({
            "status": "unavailable",
            "enabled": false,
            "reason": "memory manager not attached"
        })
    };

    let connector_snapshot = connector_routes::connector_snapshot(&state);
    let connector_summary = connector_snapshot.summary();
    let connector_ready = !connector_summary.degraded;
    let mut degraded_reasons = config_warnings.clone();
    if !durable_session_store {
        degraded_reasons.push("session store not available".to_string());
    }
    degraded_reasons.extend(
        connector_summary
            .degraded_reasons
            .iter()
            .map(|reason| format!("connector runtime: {reason}")),
    );
    let degraded = !degraded_reasons.is_empty();
    let status = if degraded {
        "degraded"
    } else if state.services.memory.manager().is_none()
        || !control.policy.enabled
        || !provider_configured
        || !configured_model_resolved
    {
        "attention"
    } else {
        "healthy"
    };

    let task_status_counts = task_records
        .iter()
        .fold(serde_json::Map::new(), |mut acc, task| {
            let key = task.status.as_str().to_string();
            let next = acc.get(&key).and_then(Value::as_u64).unwrap_or(0) + 1;
            acc.insert(key, Value::from(next));
            acc
        });
    let session_lease_projection = session_lease_projection(&state).await;
    let memory_attached = state.services.memory.manager().is_some();
    let component_count = 10usize;
    let degraded_component_count =
        usize::from(!durable_session_store) * 2 + usize::from(!connector_ready);
    let attention_component_count = usize::from(!memory_attached)
        + usize::from(!control.policy.enabled)
        + usize::from(!provider_configured)
        + usize::from(!configured_model_resolved);
    let capability_count = 11usize + connector_summary.capability_count;
    let generated_at_ms = now_ms();
    let elapsed_ms = started_at.elapsed().as_millis() as u64;
    let performance_status = if elapsed_ms <= 50 {
        "healthy"
    } else if elapsed_ms <= 200 {
        "attention"
    } else {
        "degraded"
    };
    let observability_ready = control.policy.observability.emit_events
        && (control.policy.observability.webui || control.policy.observability.tui);
    let readiness_checks = vec![
        serde_json::json!({
            "id": "session.sqlite_source_of_truth",
            "label": "Session DB source of truth",
            "required": true,
            "status": if durable_session_store { "ready" } else { "blocked" },
            "reason": if durable_session_store { "SQLite session store is attached" } else { "session store is not attached" },
        }),
        serde_json::json!({
            "id": "memory.manager",
            "label": "Memory manager",
            "required": true,
            "status": if memory_attached { "ready" } else { "blocked" },
            "reason": if memory_attached { "memory manager is attached" } else { "memory manager is not attached" },
        }),
        serde_json::json!({
            "id": "context.durable_history",
            "label": "Durable context history",
            "required": true,
            "status": if durable_session_store { "ready" } else { "blocked" },
            "reason": if durable_session_store { "context envelopes can persist through SQLite" } else { "context history cannot persist without the session store" },
        }),
        serde_json::json!({
            "id": "task.kernel",
            "label": "Task kernel",
            "required": true,
            "status": "ready",
            "reason": "task kernel is available",
        }),
        serde_json::json!({
            "id": "permission.control",
            "label": "Permission control",
            "required": true,
            "status": "ready",
            "reason": "approval and cross-plane control APIs are registered",
        }),
        serde_json::json!({
            "id": "channel.plane",
            "label": "Channel plane",
            "required": true,
            "status": "ready",
            "reason": "channel and cross-plane adapters are registered",
        }),
        serde_json::json!({
            "id": "connector.contract",
            "label": "Connector runtime contract",
            "required": true,
            "status": if connector_ready { "ready" } else { "blocked" },
            "reason": if connector_ready {
                format!("{} connector capabilities are declared", connector_summary.capability_count)
            } else {
                format!("connector runtime is degraded: {}", connector_summary.degraded_reasons.join("; "))
            },
        }),
        serde_json::json!({
            "id": "observability.control",
            "label": "Runtime observability",
            "required": true,
            "status": if observability_ready { "ready" } else { "blocked" },
            "reason": if observability_ready { "runtime events and operator surfaces are enabled" } else { "runtime observability is disabled by policy" },
        }),
        serde_json::json!({
            "id": "provider.registry",
            "label": "Provider registry",
            "required": true,
            "status": if provider_configured { "ready" } else { "blocked" },
            "reason": if provider_configured { format!("{provider_count} providers expose {provider_model_count} models") } else { "no runtime providers are configured".to_string() },
        }),
        serde_json::json!({
            "id": "provider.model_routing",
            "label": "Configured model routing",
            "required": true,
            "status": if configured_model_resolved { "ready" } else { "blocked" },
            "reason": match (configured_model.as_deref(), configured_model_provider.as_deref()) {
                (Some(model), Some(provider)) => format!("configured model {model} resolves to provider {provider}"),
                (Some(model), None) => format!("configured model {model} is not declared by any provider"),
                (None, _) => "no configured default model; runtime model override can be resolved at request time".to_string(),
            },
        }),
        serde_json::json!({
            "id": "performance.control_plane",
            "label": "Control-plane latency",
            "required": true,
            "status": if performance_status == "degraded" { "blocked" } else { "ready" },
            "reason": format!("control-plane inspection completed in {elapsed_ms}ms"),
        }),
        serde_json::json!({
            "id": "agent.policy",
            "label": "Agent policy",
            "required": false,
            "status": if control.policy.agent.enabled { "ready" } else { "attention" },
            "reason": if control.policy.agent.enabled { "agent complexity policy is enabled" } else { "agent complexity policy is disabled" },
        }),
        serde_json::json!({
            "id": "runtime.policy",
            "label": "Runtime control policy",
            "required": false,
            "status": if control.policy.enabled { "ready" } else { "attention" },
            "reason": if control.policy.enabled { "runtime control policy is enabled" } else { "runtime control policy is disabled" },
        }),
    ];
    let required_check_count = readiness_checks
        .iter()
        .filter(|check| check["required"].as_bool().unwrap_or(false))
        .count();
    let ready_required_count = readiness_checks
        .iter()
        .filter(|check| {
            check["required"].as_bool().unwrap_or(false)
                && check["status"].as_str() == Some("ready")
        })
        .count();
    let blocked_checks: Vec<Value> = readiness_checks
        .iter()
        .filter(|check| {
            check["required"].as_bool().unwrap_or(false)
                && check["status"].as_str() != Some("ready")
        })
        .cloned()
        .collect();
    let blocked_required_count = blocked_checks.len();
    let readiness_score = if required_check_count == 0 {
        100
    } else {
        ((ready_required_count * 100) / required_check_count) as u64
    };
    let production_ready = blocked_required_count == 0;
    let mut next_actions = Vec::new();
    if !durable_session_store {
        next_actions.push("attach SQLite session store before enterprise runtime use".to_string());
    }
    if !memory_attached {
        next_actions.push("attach memory manager before production AI workloads".to_string());
    }
    if !observability_ready {
        next_actions
            .push("enable runtime observability for WebUI or TUI control surfaces".to_string());
    }
    if performance_status == "degraded" {
        next_actions.push("profile runtime control-plane latency before rollout".to_string());
    }
    if !provider_configured {
        next_actions
            .push("configure at least one runtime provider before model execution".to_string());
    }
    if !configured_model_resolved {
        next_actions
            .push("align configured default model with a declared provider model".to_string());
    }

    tracing::info!(
        target: "cowd.runtime.control_plane",
        status,
        performance_status,
        elapsed_ms,
        production_ready,
        readiness_score,
        blocked_required_count,
        degraded,
        durable_session_store,
        memory_attached,
        provider_configured,
        provider_count,
        provider_model_count,
        configured_model_resolved,
        active_sessions = active_session_count,
        stored_sessions = stored_session_count.unwrap_or(0),
        open_tasks,
        component_count,
        degraded_component_count,
        attention_component_count,
        capability_count,
        "runtime control plane inspected"
    );

    Json(serde_json::json!({
        "kind": "runtime_control_plane",
        "version": env!("CARGO_PKG_VERSION"),
        "status": status,
        "degraded": degraded,
        "degraded_reasons": degraded_reasons,
        "workspace_root": state.workspace_root,
        "config_home": state.config_home,
        "profile_id": state.profile_id,
        "config": {
            "source": config_source,
            "scenario": control.scenario.as_str(),
            "warnings": config_warnings,
        },
        "diagnostics": {
            "generated_at_ms": generated_at_ms,
            "component_count": component_count,
            "degraded_component_count": degraded_component_count,
            "attention_component_count": attention_component_count,
            "capability_count": capability_count,
            "durable_session_store": durable_session_store,
            "memory_attached": memory_attached,
            "active_sessions": active_session_count,
            "stored_sessions": stored_session_count,
            "open_tasks": open_tasks,
            "elapsed_ms": elapsed_ms,
            "performance_status": performance_status,
            "production_ready": production_ready,
            "readiness_score": readiness_score,
            "required_check_count": required_check_count,
            "ready_required_count": ready_required_count,
            "blocked_required_count": blocked_required_count,
            "provider_configured": provider_configured,
            "provider_count": provider_count,
            "provider_model_count": provider_model_count,
            "configured_model_resolved": configured_model_resolved,
            "connector_account_count": connector_summary.account_count,
            "connector_capability_count": connector_summary.capability_count,
            "connector_resource_count": connector_summary.resource_count,
        },
        "readiness": {
            "production_ready": production_ready,
            "score": readiness_score,
            "required_total": required_check_count,
            "required_ready": ready_required_count,
            "required_blocked": blocked_required_count,
            "checks": readiness_checks,
            "blocked": blocked_checks,
        },
        "components": {
            "session": {
                "status": if durable_session_store { "available" } else { "degraded" },
                "durable_store": durable_session_store,
                "active_count": active_session_ids.len(),
                "active_session_ids": active_session_ids,
                "event_bus": true,
                "source_of_truth": if durable_session_store { "sqlite" } else { "unavailable" },
                "leases": session_lease_projection,
            },
            "memory": memory,
            "context": {
                "status": if durable_session_store { "available" } else { "degraded" },
                "durable_history": durable_session_store,
                "stable_head": control.policy.context.preserve_stable_head,
                "degrade_on_pressure_bp": control.policy.context.degrade_on_pressure_bp,
            },
            "agent": {
                "status": if control.policy.agent.enabled { "available" } else { "disabled" },
                "mode_control": control.policy.agent.enabled,
                "max_parallel_agents": control.policy.agent.max_parallel_agents,
                "review_on_conflict": control.policy.agent.review_on_conflict,
                "require_positive_lift": control.policy.agent.require_positive_lift,
            },
            "task": {
                "status": "available",
                "total": task_records.len(),
                "open": open_tasks,
                "status_counts": task_status_counts,
                "auto_phase_for_yolo": control.policy.task.auto_phase_for_yolo,
            },
            "permissions": {
                "status": "available",
                "auth_required": state.auth_token.is_some(),
                "approval_gate": state.services.approval.is_configured(),
                "cross_plane_api": true,
                "review_critical_actions": control.policy.permission.review_critical_actions,
            },
            "provider": {
                "status": provider_status,
                "source": config_source,
                "configured": provider_configured,
                "provider_count": provider_count,
                "model_count": provider_model_count,
                "provider_names": provider_names,
                "configured_model": configured_model,
                "configured_model_provider": configured_model_provider,
                "configured_model_resolved": configured_model_resolved,
            },
            "channels": {
                "status": "available",
                "adapters": [
                    {
                        "id": "wechat-ilink",
                        "kind": "personal_wechat",
                        "capabilities": ["qr_login", "account_list"]
                    },
                    {
                        "id": "cross-plane",
                        "kind": "permission_control",
                        "capabilities": ["identity_binding", "grant", "audit", "policy_simulation"]
                    }
                ]
            },
            "connectors": {
                "status": if connector_ready { "available" } else { "degraded" },
                "accounts": connector_snapshot.accounts,
                "capabilities": connector_snapshot.capabilities,
                "resources": connector_snapshot.resources,
                "summary": connector_summary,
            },
            "observability": {
                "status": "available",
                "emit_events": control.policy.observability.emit_events,
                "explain": control.policy.observability.explain,
                "webui": control.policy.observability.webui,
                "tui": control.policy.observability.tui,
                "debug_reasons": control.policy.observability.debug_reasons,
            }
        },
        "capabilities": [
            "session.sqlite_source_of_truth",
            "context.envelope_history",
            "runtime.timeline",
            "runtime.effective_config",
            "memory.manager",
            "agent.complexity_policy",
            "task.phase_control",
            "permission.cross_plane",
            "connector.runtime_contract",
            "channel.wechat_ilink_qr",
            "provider.registry",
            "provider.model_routing"
        ],
        "next_actions": next_actions
    }))
}

async fn session_lease_projection(state: &AppState) -> Value {
    let Some(registry) = state.session_lease_registry.as_ref() else {
        return serde_json::json!({
            "kind": "runtime_session_leases",
            "status": "unavailable",
            "attached": false,
            "leases": [],
            "total": 0,
        });
    };
    let leases = registry.list().await;
    serde_json::json!({
        "kind": "runtime_session_leases",
        "status": "available",
        "attached": true,
        "total": leases.len(),
        "leases": leases,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn workgraph_summary(events: &[RuntimeEvent]) -> Value {
    let graph_events: Vec<&RuntimeEvent> = events
        .iter()
        .filter(|event| {
            event.kind == "agent.workgraph.reviewed" || event.kind == "agent.workgraph.planned"
        })
        .collect();
    let Some(latest) = graph_events.last() else {
        return empty_workgraph_summary();
    };

    let payload = &latest.payload;
    let graph = payload.get("graph").unwrap_or(&Value::Null);
    let scorecard = payload.get("scorecard").unwrap_or(&Value::Null);
    let value_verdict = payload.get("value_verdict").unwrap_or(&Value::Null);
    let agent_tasks = graph
        .get("nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.get("kind").and_then(Value::as_str),
                        Some("AgentTask") | Some("agent_task")
                    )
                })
                .count()
        })
        .unwrap_or(0);
    let memory_candidates = payload
        .get("maintenance_candidates")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let graph_id = graph
        .get("graph_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            latest
                .refs
                .iter()
                .find(|reference| reference.ref_type == "workgraph")
                .map(|reference| reference.id.clone())
        });
    let board_id = payload
        .get("board_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            graph
                .get("board_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            latest
                .refs
                .iter()
                .find(|reference| reference.ref_type == "collaboration_board")
                .map(|reference| reference.id.clone())
        });

    serde_json::json!({
        "count": graph_events.len(),
        "latest": {
            "sequence": latest.sequence,
            "kind": latest.kind,
            "status": graph
                .get("status")
                .and_then(Value::as_str)
                .or(latest.status.as_deref())
                .unwrap_or("n/a"),
            "graph_id": graph_id,
            "board_id": board_id,
            "completion_rate": scorecard.get("completion_rate").and_then(Value::as_f64),
            "synthesis_lift": scorecard.get("synthesis_lift").and_then(Value::as_f64),
            "complementarity_score": scorecard
                .get("complementarity_score")
                .and_then(Value::as_f64),
            "value_verdict": value_verdict,
        },
        "agent_tasks": agent_tasks,
        "memory_candidates": memory_candidates,
        "conflicts": scorecard
            .get("conflict_count")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

fn empty_workgraph_summary() -> Value {
    serde_json::json!({
        "count": 0,
        "latest": null,
        "agent_tasks": 0,
        "memory_candidates": 0,
        "conflicts": 0,
    })
}

fn agent_value_summary(
    events: &[RuntimeEvent],
    policy: &AgentControlPolicy,
    degraded: bool,
    degraded_reason: Option<&str>,
) -> Value {
    let latest = events.iter().rev().find(|event| {
        event.kind == "agent.workgraph.reviewed" || event.kind == "agent.workgraph.planned"
    });
    let mut reasons: Vec<String> = degraded_reason.map(str::to_string).into_iter().collect();

    if !policy.enabled {
        reasons.push("agent policy is disabled".to_string());
        return serde_json::json!({
            "status": "disabled",
            "recommendation": "single_agent_or_manual_review",
            "policy": agent_policy_json(policy),
            "latest": null,
            "policy_passed": false,
            "reasons": reasons,
        });
    }

    let Some(event) = latest else {
        reasons.push("no agent workgraph evidence in selected timeline".to_string());
        return serde_json::json!({
            "status": if degraded { "degraded" } else { "unproven" },
            "recommendation": "collect_workgraph_review",
            "policy": agent_policy_json(policy),
            "latest": null,
            "policy_passed": false,
            "reasons": reasons,
        });
    };

    let payload = &event.payload;
    let scorecard = payload.get("scorecard").unwrap_or(&Value::Null);
    let verdict = payload.get("value_verdict").unwrap_or(&Value::Null);
    let graph = payload.get("graph").unwrap_or(&Value::Null);
    let value_score = verdict
        .get("value_score")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u16;
    let positive_lift = verdict
        .get("positive_lift")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let continue_multi_agent = verdict
        .get("continue_multi_agent")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let completion_rate = scorecard
        .get("completion_rate")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let synthesis_lift = scorecard
        .get("synthesis_lift")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let complementarity_score = scorecard
        .get("complementarity_score")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let conflict_count = scorecard
        .get("conflict_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let agent_tasks = graph
        .get("nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .filter(|node| {
                    matches!(
                        node.get("kind").and_then(Value::as_str),
                        Some("AgentTask") | Some("agent_task")
                    )
                })
                .count()
        })
        .unwrap_or(0);
    let policy_score_passed = value_score >= policy.min_collaboration_score;
    let lift_passed = !policy.require_positive_lift || positive_lift;
    let conflict_review_required = policy.review_on_conflict && conflict_count > 0;
    let event_failed = runtime_event_failed(event);
    let event_degraded = runtime_event_degraded(event) || degraded;
    let policy_passed = policy_score_passed
        && lift_passed
        && !event_failed
        && !event_degraded
        && !conflict_review_required;

    if !policy_score_passed {
        reasons.push(format!(
            "value score {value_score} is below policy threshold {}",
            policy.min_collaboration_score
        ));
    }
    if !lift_passed {
        reasons.push("positive lift is required by policy but was not proven".to_string());
    }
    if conflict_review_required {
        reasons.push(format!("{conflict_count} conflict(s) require review"));
    }
    if event_failed {
        reasons.push("latest workgraph event failed".to_string());
    }
    if event_degraded {
        reasons.push("latest workgraph evidence is degraded".to_string());
    }
    for reason in verdict
        .get("reasons")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        if !reasons.iter().any(|existing| existing == reason) {
            reasons.push(reason.to_string());
        }
    }
    if reasons.is_empty() {
        reasons.push("multi-agent collaboration clears policy threshold".to_string());
    }

    let status = if event_failed || event_degraded {
        "degraded"
    } else if conflict_review_required {
        "review_required"
    } else if policy_passed {
        "proven"
    } else {
        "insufficient"
    };
    let recommendation = if status == "proven" && continue_multi_agent {
        "continue_multi_agent"
    } else if conflict_review_required {
        "review_conflicts"
    } else if status == "insufficient" {
        "prefer_single_agent_or_review_only"
    } else if status == "degraded" {
        "repair_workgraph_evidence"
    } else {
        "collect_more_collaboration_evidence"
    };

    serde_json::json!({
        "status": status,
        "recommendation": recommendation,
        "policy": agent_policy_json(policy),
        "policy_passed": policy_passed,
        "latest": {
            "sequence": event.sequence,
            "kind": event.kind,
            "status": event.status,
            "value_score": value_score,
            "positive_lift": positive_lift,
            "continue_multi_agent": continue_multi_agent,
            "completion_rate": completion_rate,
            "synthesis_lift": synthesis_lift,
            "complementarity_score": complementarity_score,
            "conflict_count": conflict_count,
            "agent_tasks": agent_tasks,
        },
        "reasons": reasons,
    })
}

fn degraded_agent_value_summary(policy: &AgentControlPolicy, reason: &str) -> Value {
    agent_value_summary(&[], policy, true, Some(reason))
}

fn agent_policy_json(policy: &AgentControlPolicy) -> Value {
    serde_json::json!({
        "enabled": policy.enabled,
        "max_parallel_agents": policy.max_parallel_agents,
        "review_on_conflict": policy.review_on_conflict,
        "require_positive_lift": policy.require_positive_lift,
        "min_collaboration_score": policy.min_collaboration_score,
    })
}

fn health_summary(events: &[RuntimeEvent], degraded: bool, degraded_reason: Option<&str>) -> Value {
    let mut score: i64 = if degraded { 35 } else { 100 };
    let mut failed_events = 0usize;
    let mut degraded_events = 0usize;
    let mut open_tasks = 0i64;
    let mut positive_agent_lift = false;
    let mut latest_policy = Value::Null;
    let mut latest_value_score: Option<u64> = None;
    let mut reasons: Vec<String> = Vec::new();
    let mut scope_counts = serde_json::Map::new();

    if let Some(reason) = degraded_reason {
        reasons.push(reason.to_string());
    }

    for event in events {
        let scope = serde_json::to_value(event.scope)
            .ok()
            .and_then(|value| value.as_str().map(ToString::to_string))
            .unwrap_or_else(|| "unknown".to_string());
        let next = scope_counts
            .get(&scope)
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        scope_counts.insert(scope, Value::from(next));

        if matches!(event.status.as_deref(), Some("failed") | Some("error")) {
            failed_events += 1;
        }
        if matches!(event.status.as_deref(), Some("degraded"))
            || event.payload.get("parse_error").is_some()
        {
            degraded_events += 1;
        }

        match event.kind.as_str() {
            "task.started" => open_tasks += 1,
            "task.completed" | "task.cancelled" | "task.blocked" => {
                open_tasks = open_tasks.saturating_sub(1);
            }
            "runtime.policy.decided" => {
                latest_policy = serde_json::json!({
                    "sequence": event.sequence,
                    "agent_mode": event.payload.get("agent_mode").cloned().unwrap_or(Value::Null),
                    "requires_review": event
                        .payload
                        .get("requires_review")
                        .cloned()
                        .unwrap_or(Value::Null),
                    "complexity": event.payload.get("complexity").cloned().unwrap_or(Value::Null),
                });
            }
            "agent.workgraph.reviewed" => {
                if let Some(verdict) = event.payload.get("value_verdict") {
                    positive_agent_lift |= verdict
                        .get("positive_lift")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    latest_value_score = verdict.get("value_score").and_then(Value::as_u64);
                }
            }
            _ => {}
        }
    }

    if failed_events > 0 {
        score -= (failed_events as i64 * 18).min(45);
        reasons.push(format!("{failed_events} failed runtime event(s)"));
    }
    if degraded_events > 0 {
        score -= (degraded_events as i64 * 12).min(36);
        reasons.push(format!("{degraded_events} degraded runtime event(s)"));
    }
    if open_tasks > 0 {
        score -= (open_tasks * 4).min(16);
        reasons.push(format!("{open_tasks} open task(s)"));
    }
    if let Some(value_score) = latest_value_score {
        if value_score < 50 {
            score -= 10;
            reasons.push("latest agent collaboration value below threshold".to_string());
        } else if positive_agent_lift {
            score = (score + 3).min(100);
        }
    }
    if events.is_empty() && !degraded {
        score = 80;
        reasons.push("no runtime events in selected page".to_string());
    }

    let score = score.clamp(0, 100) as u64;
    let status = if degraded || degraded_events > 0 {
        "degraded"
    } else if failed_events > 0 || open_tasks > 0 || score < 85 {
        "attention"
    } else {
        "healthy"
    };

    if reasons.is_empty() {
        reasons.push("runtime event spine is coherent".to_string());
    }

    serde_json::json!({
        "status": status,
        "score": score,
        "event_count": events.len(),
        "failed_events": failed_events,
        "degraded_events": degraded_events,
        "open_tasks": open_tasks,
        "positive_agent_lift": positive_agent_lift,
        "latest_policy": latest_policy,
        "latest_value_score": latest_value_score,
        "reasons": reasons,
        "scope_counts": scope_counts,
    })
}

fn degraded_health_summary(reason: &str) -> Value {
    health_summary(&[], true, Some(reason))
}

#[derive(Debug, Clone, Copy)]
struct ValueLoopStageSpec {
    id: &'static str,
    label: &'static str,
    required: bool,
}

#[derive(Debug, Clone)]
struct ValueLoopStageState {
    spec: ValueLoopStageSpec,
    event_count: usize,
    failed_count: usize,
    degraded_count: usize,
    latest_sequence: Option<usize>,
    latest_kind: Option<String>,
}

const VALUE_LOOP_STAGES: [ValueLoopStageSpec; 8] = [
    ValueLoopStageSpec {
        id: "intake",
        label: "Intake",
        required: true,
    },
    ValueLoopStageSpec {
        id: "context",
        label: "Context",
        required: true,
    },
    ValueLoopStageSpec {
        id: "memory",
        label: "Memory",
        required: true,
    },
    ValueLoopStageSpec {
        id: "governance",
        label: "Governance",
        required: true,
    },
    ValueLoopStageSpec {
        id: "task",
        label: "Task",
        required: true,
    },
    ValueLoopStageSpec {
        id: "execution",
        label: "Execution",
        required: true,
    },
    ValueLoopStageSpec {
        id: "agent",
        label: "Agent",
        required: true,
    },
    ValueLoopStageSpec {
        id: "channel",
        label: "Channel",
        required: false,
    },
];

fn value_loop_summary(
    events: &[RuntimeEvent],
    degraded: bool,
    degraded_reason: Option<&str>,
) -> Value {
    let mut stages: Vec<ValueLoopStageState> = VALUE_LOOP_STAGES
        .iter()
        .copied()
        .map(|spec| ValueLoopStageState {
            spec,
            event_count: 0,
            failed_count: 0,
            degraded_count: 0,
            latest_sequence: None,
            latest_kind: None,
        })
        .collect();
    let mut failed_events = 0usize;
    let mut degraded_events = 0usize;
    let mut open_tasks = 0i64;
    let mut positive_agent_lift = false;
    let mut latest_value_score: Option<u64> = None;
    let mut reasons: Vec<String> = Vec::new();

    if let Some(reason) = degraded_reason {
        reasons.push(reason.to_string());
    }

    for event in events {
        let stage_id = value_loop_stage_id(event);
        if let Some(stage) = stages.iter_mut().find(|stage| stage.spec.id == stage_id) {
            stage.event_count += 1;
            stage.latest_sequence = Some(event.sequence);
            stage.latest_kind = Some(event.kind.clone());
            if runtime_event_failed(event) {
                stage.failed_count += 1;
            }
            if runtime_event_degraded(event) {
                stage.degraded_count += 1;
            }
        }

        if runtime_event_failed(event) {
            failed_events += 1;
        }
        if runtime_event_degraded(event) {
            degraded_events += 1;
        }
        match event.kind.as_str() {
            "task.started" => open_tasks += 1,
            "task.completed" | "task.cancelled" | "task.blocked" => {
                open_tasks = open_tasks.saturating_sub(1);
            }
            "agent.workgraph.reviewed" => {
                if let Some(verdict) = event.payload.get("value_verdict") {
                    positive_agent_lift |= verdict
                        .get("positive_lift")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    latest_value_score = verdict.get("value_score").and_then(Value::as_u64);
                }
            }
            _ => {}
        }
    }

    let required_total = stages.iter().filter(|stage| stage.spec.required).count();
    let required_observed = stages
        .iter()
        .filter(|stage| stage.spec.required && stage.event_count > 0)
        .count();
    let missing_required: Vec<Value> = stages
        .iter()
        .filter(|stage| stage.spec.required && stage.event_count == 0)
        .map(|stage| {
            serde_json::json!({
                "id": stage.spec.id,
                "label": stage.spec.label,
                "next_action": value_loop_next_action(stage.spec.id),
            })
        })
        .collect();
    let missing_required_count = missing_required.len();
    let mut score = if required_total == 0 {
        100i64
    } else {
        ((required_observed * 100) / required_total) as i64
    };

    if degraded {
        score -= 35;
    }
    if failed_events > 0 {
        score -= (failed_events as i64 * 15).min(45);
        reasons.push(format!("{failed_events} failed event(s) in value loop"));
    }
    if degraded_events > 0 {
        score -= (degraded_events as i64 * 10).min(30);
        reasons.push(format!("{degraded_events} degraded event(s) in value loop"));
    }
    if open_tasks > 0 {
        score -= (open_tasks * 5).min(20);
        reasons.push(format!(
            "{open_tasks} open task(s) still need review or completion"
        ));
    }
    if missing_required_count > 0 {
        reasons.push(format!(
            "{missing_required_count} required stage(s) missing from selected timeline"
        ));
    }
    if let Some(value_score) = latest_value_score {
        if value_score < 50 {
            score -= 10;
            reasons.push("latest multi-agent value score is below threshold".to_string());
        } else if positive_agent_lift {
            score = (score + 3).min(100);
        }
    }
    if events.is_empty() && !degraded {
        score = 0;
        reasons.push("no runtime events available for value-loop assessment".to_string());
    }

    let score = score.clamp(0, 100) as u64;
    let status = if degraded || failed_events > 0 || degraded_events > 0 {
        "degraded"
    } else if missing_required_count > 0 || open_tasks > 0 || score < 90 {
        "incomplete"
    } else {
        "complete"
    };
    if reasons.is_empty() {
        reasons
            .push("runtime value loop has all required stages and no blocking defects".to_string());
    }

    let stages_json: Vec<Value> = stages
        .into_iter()
        .map(|stage| {
            let status = if stage.failed_count > 0 {
                "failed"
            } else if stage.degraded_count > 0 {
                "degraded"
            } else if stage.event_count > 0 {
                "observed"
            } else if stage.spec.required {
                "missing"
            } else {
                "optional"
            };
            serde_json::json!({
                "id": stage.spec.id,
                "label": stage.spec.label,
                "required": stage.spec.required,
                "status": status,
                "event_count": stage.event_count,
                "failed_count": stage.failed_count,
                "degraded_count": stage.degraded_count,
                "latest_sequence": stage.latest_sequence,
                "latest_kind": stage.latest_kind,
            })
        })
        .collect();

    serde_json::json!({
        "status": status,
        "score": score,
        "event_count": events.len(),
        "required_total": required_total,
        "required_observed": required_observed,
        "missing_required_count": missing_required_count,
        "missing_required": missing_required,
        "failed_events": failed_events,
        "degraded_events": degraded_events,
        "open_tasks": open_tasks,
        "positive_agent_lift": positive_agent_lift,
        "latest_value_score": latest_value_score,
        "stages": stages_json,
        "reasons": reasons,
        "next_actions": value_loop_next_actions(&missing_required_count, open_tasks, failed_events, degraded_events),
    })
}

fn degraded_value_loop_summary(reason: &str) -> Value {
    value_loop_summary(&[], true, Some(reason))
}

fn value_loop_stage_id(event: &RuntimeEvent) -> &'static str {
    if is_channel_event(event) {
        return "channel";
    }
    match event.scope {
        memory::RuntimeEventScope::Session
        | memory::RuntimeEventScope::Message
        | memory::RuntimeEventScope::Turn => "intake",
        memory::RuntimeEventScope::Context => "context",
        memory::RuntimeEventScope::Memory => "memory",
        memory::RuntimeEventScope::Policy | memory::RuntimeEventScope::Approval => "governance",
        memory::RuntimeEventScope::Task => "task",
        memory::RuntimeEventScope::Tool | memory::RuntimeEventScope::Scheduler => "execution",
        memory::RuntimeEventScope::Agent | memory::RuntimeEventScope::Workgraph => "agent",
    }
}

fn is_channel_event(event: &RuntimeEvent) -> bool {
    event.kind.starts_with("channel.")
        || event.kind.starts_with("platform.")
        || event.kind.starts_with("cross_plane.")
        || event.refs.iter().any(|reference| {
            matches!(
                reference.ref_type.as_str(),
                "channel" | "platform" | "feishu" | "wechat" | "wecom" | "email"
            )
        })
}

fn runtime_event_failed(event: &RuntimeEvent) -> bool {
    matches!(
        event.status.as_deref(),
        Some("failed") | Some("error") | Some("denied")
    ) || event.kind.ends_with(".failed")
        || event.kind.ends_with(".error")
}

fn runtime_event_degraded(event: &RuntimeEvent) -> bool {
    matches!(event.status.as_deref(), Some("degraded"))
        || event.payload.get("parse_error").is_some()
        || event
            .payload
            .get("degraded")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

fn value_loop_next_action(stage_id: &str) -> &'static str {
    match stage_id {
        "intake" => "persist at least one session, turn, or message event",
        "context" => "build and persist a context envelope for this run",
        "memory" => "record memory recall, write, pulse, or maintenance evidence",
        "governance" => "record runtime policy, approval, or permission decision",
        "task" => "bind execution to a task lifecycle event",
        "execution" => "record tool, scheduler, or channel execution evidence",
        "agent" => "record agent collaboration, workgraph, or single-agent decision evidence",
        _ => "record runtime evidence for this stage",
    }
}

fn value_loop_next_actions(
    missing_required_count: &usize,
    open_tasks: i64,
    failed_events: usize,
    degraded_events: usize,
) -> Vec<String> {
    let mut actions = Vec::new();
    if *missing_required_count > 0 {
        actions.push(
            "complete missing required runtime stages before claiming closed-loop execution"
                .to_string(),
        );
    }
    if open_tasks > 0 {
        actions
            .push("complete, cancel, or explicitly block open task lifecycle records".to_string());
    }
    if failed_events > 0 {
        actions.push("inspect failed events and append recovery or rollback evidence".to_string());
    }
    if degraded_events > 0 {
        actions.push(
            "resolve degraded runtime evidence before promoting the session as healthy".to_string(),
        );
    }
    if actions.is_empty() {
        actions.push("no blocking action required for the selected runtime timeline".to_string());
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(sequence: usize, scope: memory::RuntimeEventScope, kind: &str) -> RuntimeEvent {
        RuntimeEvent::new(
            "value-loop-session",
            sequence,
            scope,
            kind,
            serde_json::json!({}),
            sequence as u64,
        )
    }

    fn reviewed_workgraph_event(
        sequence: usize,
        value_score: u16,
        positive_lift: bool,
    ) -> RuntimeEvent {
        let mut event = RuntimeEvent::new(
            "agent-value-session",
            sequence,
            memory::RuntimeEventScope::Workgraph,
            "agent.workgraph.reviewed",
            serde_json::json!({
                "graph": {
                    "graph_id": "graph-agent-value",
                    "nodes": [
                        {"kind": "AgentTask", "node_id": "worker-1"},
                        {"kind": "AgentTask", "node_id": "worker-2"},
                        {"kind": "Synthesis", "node_id": "synthesis"}
                    ]
                },
                "scorecard": {
                    "completion_rate": 1.0,
                    "synthesis_lift": if positive_lift { 1.25 } else { 1.0 },
                    "complementarity_score": if positive_lift { 0.75 } else { 0.0 },
                    "conflict_count": 0
                },
                "value_verdict": {
                    "positive_lift": positive_lift,
                    "continue_multi_agent": positive_lift,
                    "value_score": value_score,
                    "reasons": if positive_lift {
                        vec!["positive_multi_agent_lift"]
                    } else {
                        vec!["no_synthesis_lift", "no_complementarity"]
                    }
                }
            }),
            sequence as u64,
        );
        event.status = Some("completed".to_string());
        event
    }

    #[test]
    fn value_loop_summary_marks_complete_closed_loop() {
        let mut workgraph = event(
            6,
            memory::RuntimeEventScope::Workgraph,
            "agent.workgraph.reviewed",
        );
        workgraph.payload = serde_json::json!({
            "value_verdict": {
                "positive_lift": true,
                "value_score": 76
            }
        });
        let events = vec![
            event(0, memory::RuntimeEventScope::Message, "message.received"),
            event(
                1,
                memory::RuntimeEventScope::Context,
                "context.envelope.built",
            ),
            event(
                2,
                memory::RuntimeEventScope::Memory,
                "memory.recall.completed",
            ),
            event(
                3,
                memory::RuntimeEventScope::Policy,
                "runtime.policy.decided",
            ),
            event(4, memory::RuntimeEventScope::Task, "task.started"),
            event(5, memory::RuntimeEventScope::Tool, "tool.completed"),
            workgraph,
            event(7, memory::RuntimeEventScope::Task, "task.completed"),
        ];

        let summary = value_loop_summary(&events, false, None);

        assert_eq!(summary["status"], "complete");
        assert_eq!(summary["score"], 100);
        assert_eq!(summary["required_total"], 7);
        assert_eq!(summary["required_observed"], 7);
        assert_eq!(summary["missing_required_count"], 0);
        assert_eq!(summary["open_tasks"], 0);
        assert_eq!(summary["positive_agent_lift"], true);
        assert_eq!(
            summary["next_actions"][0],
            "no blocking action required for the selected runtime timeline"
        );
    }

    #[test]
    fn value_loop_summary_surfaces_missing_and_degraded_stages() {
        let mut failed_tool = event(2, memory::RuntimeEventScope::Tool, "tool.failed");
        failed_tool.status = Some("failed".to_string());
        let mut degraded_memory = event(
            1,
            memory::RuntimeEventScope::Memory,
            "memory.recall.completed",
        );
        degraded_memory.status = Some("degraded".to_string());
        let events = vec![
            event(0, memory::RuntimeEventScope::Message, "message.received"),
            degraded_memory,
            failed_tool,
        ];

        let summary = value_loop_summary(&events, false, None);

        assert_eq!(summary["status"], "degraded");
        assert_eq!(summary["failed_events"], 1);
        assert_eq!(summary["degraded_events"], 1);
        assert_eq!(summary["missing_required_count"], 4);
        assert!(summary["score"].as_u64().unwrap() < 50);
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action
                .as_str()
                .unwrap()
                .contains("complete missing required runtime stages")));
    }

    #[test]
    fn value_loop_summary_tracks_optional_channel_stage() {
        let mut channel_event = event(
            0,
            memory::RuntimeEventScope::Tool,
            "channel.feishu.message.sent",
        );
        channel_event.refs = vec![memory::RuntimeRef {
            ref_type: "feishu".to_string(),
            id: "chat-1".to_string(),
            label: Some("Feishu".to_string()),
        }];

        let summary = value_loop_summary(&[channel_event], false, None);
        let channel = summary["stages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|stage| stage["id"] == "channel")
            .unwrap();

        assert_eq!(channel["required"], false);
        assert_eq!(channel["status"], "observed");
        assert_eq!(channel["event_count"], 1);
    }

    #[test]
    fn agent_value_summary_proves_multi_agent_lift_against_policy() {
        let policy = AgentControlPolicy {
            min_collaboration_score: 70,
            ..AgentControlPolicy::default()
        };
        let event = reviewed_workgraph_event(4, 76, true);

        let summary = agent_value_summary(&[event], &policy, false, None);

        assert_eq!(summary["status"], "proven");
        assert_eq!(summary["recommendation"], "continue_multi_agent");
        assert_eq!(summary["policy_passed"], true);
        assert_eq!(summary["latest"]["agent_tasks"], 2);
        assert_eq!(summary["latest"]["value_score"], 76);
        assert_eq!(summary["latest"]["positive_lift"], true);
    }

    #[test]
    fn agent_value_summary_rejects_low_value_or_missing_lift() {
        let policy = AgentControlPolicy {
            min_collaboration_score: 70,
            require_positive_lift: true,
            ..AgentControlPolicy::default()
        };
        let event = reviewed_workgraph_event(4, 48, false);

        let summary = agent_value_summary(&[event], &policy, false, None);

        assert_eq!(summary["status"], "insufficient");
        assert_eq!(
            summary["recommendation"],
            "prefer_single_agent_or_review_only"
        );
        assert_eq!(summary["policy_passed"], false);
        assert!(summary["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason.as_str().unwrap().contains("below policy threshold")));
        assert!(summary["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason.as_str().unwrap() == "no_synthesis_lift"));
    }

    #[test]
    fn agent_value_summary_requires_review_for_unresolved_conflict() {
        let policy = AgentControlPolicy::default();
        let mut event = reviewed_workgraph_event(4, 82, false);
        event.payload["scorecard"]["conflict_count"] = serde_json::json!(2);
        event.payload["value_verdict"]["reasons"] = serde_json::json!(["excessive_conflict"]);

        let summary = agent_value_summary(&[event], &policy, false, None);

        assert_eq!(summary["status"], "review_required");
        assert_eq!(summary["recommendation"], "review_conflicts");
        assert_eq!(summary["policy_passed"], false);
        assert_eq!(summary["latest"]["conflict_count"], 2);
    }
}
