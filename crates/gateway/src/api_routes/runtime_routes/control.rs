use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

mod agent_value;
mod execution_graph;
mod health;
mod value_loop;

pub(in crate::api_routes) use agent_value::*;
pub(in crate::api_routes) use execution_graph::*;
pub(in crate::api_routes) use health::*;
pub(in crate::api_routes) use value_loop::*;

pub(in crate::api_routes) async fn get_runtime_control_plane(
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
    let registry = model_protocol::model_registry::ModelRegistry::load()
        .unwrap_or_else(|_| model_protocol::model_registry::ModelRegistry::empty());
    let provider_catalog = provider::ProviderCatalog::from_input(provider::ProviderCatalogInput {
        providers,
        registry: &registry,
        configured_model: runtime_config.model(),
        aliases: runtime_config.aliases(),
        config_source,
        extra_sources: Vec::new(),
        transforms: Vec::new(),
        warnings: config_warnings.clone(),
    });
    let catalog_generation = provider_catalog.generation.clone();
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
                "catalog_generation": catalog_generation,
                "catalog_updated": generated_at_ms,
                "catalog": provider_catalog,
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

pub(in crate::api_routes) async fn session_lease_projection(state: &AppState) -> Value {
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
