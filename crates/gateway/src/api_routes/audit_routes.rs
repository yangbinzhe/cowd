use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Extension, Query, State as AxumState},
    response::IntoResponse,
    routing::get,
    Json, Router,
};

use super::{AppState, AuthenticatedPrincipal};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new().route("/api/audit/export", get(audit_export_handler))
}

async fn audit_export_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .min(500);
    let offset = params
        .get("offset")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let source = params.get("source").map(String::as_str).unwrap_or("all");
    let include_approval = matches!(source, "all" | "approval");
    let include_memory = matches!(source, "all" | "memory");

    let (approval, approval_total) = if include_approval {
        let approval = state
            .services
            .approval
            .history(limit + offset, 0, &principal.0)
            .await;
        let items = approval.as_array().cloned().unwrap_or_default();
        let total = items.len();
        (items, total)
    } else {
        (Vec::new(), 0)
    };
    let memory = if include_memory {
        match state.services.memory.manager() {
            Some(manager) => manager.audit_entries(limit + offset).unwrap_or_default(),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    let memory_total = memory.len();

    let mut records = Vec::new();
    for entry in &approval {
        records.push(serde_json::json!({
            "source": "approval",
            "timestamp": entry.get("resolved_at").and_then(|value| value.as_str()).unwrap_or_default(),
            "id": entry.get("id").and_then(|value| value.as_str()).unwrap_or_default(),
            "summary": entry.get("command").and_then(|value| value.as_str()).unwrap_or_default(),
            "record": entry,
        }));
    }
    for entry in &memory {
        records.push(serde_json::json!({
            "source": "memory",
            "timestamp": entry.timestamp,
            "id": entry.entry_id,
            "summary": entry.summary,
            "record": entry,
        }));
    }
    records.sort_by(|a, b| {
        b.get("timestamp")
            .and_then(|v| v.as_str())
            .cmp(&a.get("timestamp").and_then(|v| v.as_str()))
    });
    let total = records.len();
    let records = records
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    Json(serde_json::json!({
        "kind": "audit_export",
        "generated_at": chrono::Utc::now(),
        "source": source,
        "limit": limit,
        "offset": offset,
        "total": total,
        "totals": {
            "approval": approval_total,
            "memory": memory_total,
        },
        "records": records,
        "approval": approval,
        "memory": memory,
    }))
}
