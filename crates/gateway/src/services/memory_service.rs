use std::sync::Arc;

use memory::types::{MemoryEntry, MemoryId};
use memory::{MemoryContextPacket, MemoryKernel, MemoryTurnContext, RotAlert};

use super::{GatewayMemoryManager, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct MemoryService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    manager: Option<Arc<GatewayMemoryManager>>,
}

impl MemoryService {
    pub(crate) fn new() -> Self {
        Self {
            label: "memory",
            owner: "0.9.368 GatewayServices",
            manager: None,
        }
    }

    pub(crate) fn with_manager(manager: Option<Arc<GatewayMemoryManager>>) -> Self {
        Self {
            manager,
            ..Self::new()
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        ServiceEnvelope {
            service: self.label,
            operation,
            status: if self.manager.is_some() {
                "service_ready"
            } else {
                "service_boundary_ready"
            },
            owner: self.owner,
            boundary_status: "reviewed_0.9.307",
        }
    }

    pub(crate) fn manager(&self) -> Option<Arc<GatewayMemoryManager>> {
        self.manager.clone()
    }

    pub(crate) fn is_available(&self) -> bool {
        self.manager.is_some()
    }

    pub(crate) async fn status_projection(&self) -> serde_json::Value {
        if let Some(mgr) = self.manager() {
            let layers = mgr.list_layers().await;
            let kernel = MemoryKernel::new(Arc::clone(&mgr));
            let kernel_ctx = MemoryTurnContext::new("api-memory-status", "api");
            let kernel_health = kernel
                .health(&kernel_ctx)
                .await
                .map(memory_kernel_health_json)
                .unwrap_or_else(|error| {
                    serde_json::json!({
                        "degraded": true,
                        "degraded_reasons": [format!("health failed: {error}")],
                        "orientation_pressure": 0.0,
                        "conflict_pressure": 0.0,
                        "stale_pressure": 0.0,
                        "evidence_coverage": 0.0,
                        "link_coverage": 0.0,
                        "background_lag_ms": null,
                    })
                });
            let vector_count = mgr.vector_index_count();
            let total_entries: usize = layers
                .iter()
                .filter_map(|layer| layer.get("entry_count").and_then(|value| value.as_u64()))
                .map(|count| count as usize)
                .sum();
            serde_json::json!({
                "enabled": true,
                "status": "ready",
                "degraded": false,
                "degraded_reason": null,
                "layers": layers,
                "total_entries": total_entries,
                "vector_count": vector_count,
                "session_store": true,
                "context_health": context_health_json(mgr.ctx_health()),
                "kernel_health": kernel_health,
                "runtime": kernel.runtime_snapshot().await.ok(),
                "performance": mgr.performance_report(),
            })
        } else {
            serde_json::json!({
                "enabled": false,
                "status": "disabled",
                "degraded": false,
                "degraded_reason": "memory not configured",
                "layers": empty_memory_layers_json(),
                "total_entries": 0,
                "vector_count": 0,
                "session_store": false,
                "context_health": {
                    "level": "unavailable",
                    "message": "memory not configured",
                },
                "kernel_health": {
                    "degraded": true,
                    "degraded_reasons": ["memory not configured"],
                    "orientation_pressure": 0.0,
                    "conflict_pressure": 0.0,
                    "stale_pressure": 0.0,
                    "evidence_coverage": 0.0,
                    "link_coverage": 0.0,
                    "background_lag_ms": null,
                },
                "message": "memory not configured"
            })
        }
    }

    pub(crate) async fn remember_entry(&self, entry: MemoryEntry) -> Result<(), String> {
        self.remember_entry_with_context(entry, "api-memory-create", "api")
            .await
    }

    pub(crate) async fn remember_entry_with_context(
        &self,
        entry: MemoryEntry,
        session_id: &str,
        source: &str,
    ) -> Result<(), String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        let memory_ctx = MemoryTurnContext::new(session_id, source);
        kernel
            .remember(&memory_ctx, entry)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn list_all_entries(&self) -> Result<Vec<MemoryEntry>, String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        mgr.list_all_entries()
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn update_entry(
        &self,
        id: &str,
        content: Option<String>,
        tags: Option<Vec<String>>,
        priority: Option<memory::types::Priority>,
    ) -> Result<(), String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        mgr.update_entry(id, content, tags, priority)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn archive_entry(&self, memory_id: MemoryId) -> Result<(), String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        let memory_ctx = MemoryTurnContext::new("api-memory-delete", "api");
        kernel
            .archive(&memory_ctx, memory_id, "archived by API delete request")
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn packet_projection(
        &self,
        query: String,
        max_items: usize,
        max_tokens: u64,
    ) -> serde_json::Value {
        let Some(mgr) = self.manager() else {
            return serde_json::json!({
                "enabled": false,
                "query": query,
                "packet": null,
                "degraded": true,
                "degraded_reason": "memory not configured",
            });
        };

        let query_for_packet = query.clone();
        let packet_result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            rt.block_on(async move {
                let kernel = MemoryKernel::new(mgr);
                let ctx = MemoryTurnContext::new("api-memory-packet", "api");
                kernel
                    .context_packet(&ctx, &query_for_packet, &[], max_items, max_tokens)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result);

        match packet_result {
            Ok(packet) => serde_json::json!({
                "enabled": true,
                "query": query,
                "packet": packet,
                "degraded": false,
                "degraded_reason": null,
            }),
            Err(error) => serde_json::json!({
                "enabled": true,
                "query": query,
                "packet": null,
                "degraded": true,
                "degraded_reason": error,
            }),
        }
    }

    pub(crate) async fn context_packet(
        &self,
        session_id: String,
        source: &'static str,
        query: String,
        max_items: usize,
        max_tokens: u64,
    ) -> Result<MemoryContextPacket, String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            rt.block_on(async move {
                let kernel = MemoryKernel::new(mgr);
                let ctx = MemoryTurnContext::new(session_id, source);
                kernel
                    .context_packet(&ctx, &query, &[], max_items, max_tokens)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .await
        .map_err(|error| error.to_string())?
    }

    pub(crate) async fn links_projection(&self) -> serde_json::Value {
        let Some(mgr) = self.manager() else {
            return serde_json::json!({
                "enabled": false,
                "links": [],
                "degraded": true,
                "degraded_reason": "memory not configured",
            });
        };
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        match kernel.links().await {
            Ok(links) => serde_json::json!({
                "enabled": true,
                "links": links,
                "total": links.len(),
                "degraded": false,
                "degraded_reason": null,
            }),
            Err(error) => serde_json::json!({
                "enabled": true,
                "links": [],
                "total": 0,
                "degraded": true,
                "degraded_reason": error.to_string(),
            }),
        }
    }

    pub(crate) async fn runtime_projection(&self) -> serde_json::Value {
        let Some(mgr) = self.manager() else {
            return serde_json::json!({
                "enabled": false,
                "runtime": null,
                "degraded": true,
                "degraded_reason": "memory not configured",
            });
        };
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        match kernel.runtime_snapshot().await {
            Ok(runtime) => serde_json::json!({
                "enabled": true,
                "runtime": runtime,
                "degraded": false,
                "degraded_reason": null,
            }),
            Err(error) => serde_json::json!({
                "enabled": true,
                "runtime": null,
                "degraded": true,
                "degraded_reason": error.to_string(),
            }),
        }
    }

    pub(crate) async fn clusters_projection(&self, limit: usize) -> serde_json::Value {
        let Some(mgr) = self.manager() else {
            return serde_json::json!({
                "enabled": false,
                "clusters": [],
                "degraded": true,
                "degraded_reason": "memory not configured",
            });
        };
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        match kernel.clusters(limit.min(200)).await {
            Ok(clusters) => serde_json::json!({
                "enabled": true,
                "clusters": clusters,
                "total": clusters.len(),
                "degraded": false,
                "degraded_reason": null,
            }),
            Err(error) => serde_json::json!({
                "enabled": true,
                "clusters": [],
                "total": 0,
                "degraded": true,
                "degraded_reason": error.to_string(),
            }),
        }
    }

    pub(crate) async fn lifecycle_projection(
        &self,
        memory_id: MemoryId,
        id: String,
    ) -> Result<serde_json::Value, String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        let events = kernel
            .lifecycle_events(memory_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({
            "enabled": true,
            "id": id,
            "events": events,
        }))
    }
}

fn context_health_json(alert: RotAlert) -> serde_json::Value {
    match alert {
        RotAlert::None => serde_json::json!({
            "level": "healthy",
            "message": null,
        }),
        RotAlert::Warning(message) => serde_json::json!({
            "level": "warning",
            "message": message,
        }),
        RotAlert::Critical(message) => serde_json::json!({
            "level": "critical",
            "message": message,
        }),
    }
}

fn memory_kernel_health_json(health: memory::MemoryHealth) -> serde_json::Value {
    let degraded_reasons: Vec<String> = health
        .degraded
        .iter()
        .map(|reason| format!("{reason:?}"))
        .collect();
    serde_json::json!({
        "degraded": health.is_degraded(),
        "degraded_reasons": degraded_reasons,
        "orientation_pressure": health.orientation_pressure,
        "conflict_pressure": health.conflict_pressure,
        "stale_pressure": health.stale_pressure,
        "evidence_coverage": health.evidence_coverage,
        "link_coverage": health.link_coverage,
        "background_lag_ms": health.background_lag_ms,
    })
}

fn empty_memory_layers_json() -> Vec<serde_json::Value> {
    ["L0", "L1", "L2", "L3", "L4"]
        .into_iter()
        .map(|layer| serde_json::json!({ "layer": layer, "entry_count": 0 }))
        .collect()
}
