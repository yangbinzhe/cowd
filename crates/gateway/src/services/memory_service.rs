use std::sync::Arc;

use harness_contract::{
    knowledge::{KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace},
    reality::RealityCapabilityStatus,
};
use memory::types::{MemoryEntry, MemoryId, MemoryLayer};
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
            owner: "0.9.380 GatewayServices",
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
            let capabilities = memory_capabilities_json(true, vector_count);
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
                "capabilities": capabilities,
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
                "capabilities": memory_capabilities_json(false, 0),
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
                    .context_packet_preview(&ctx, &query_for_packet, &[], max_items, max_tokens)
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

    pub(crate) async fn context_packet_preview(
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
                    .context_packet_preview(&ctx, &query, &[], max_items, max_tokens)
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

    pub(crate) async fn knowledge_projection(&self) -> serde_json::Value {
        let Some(_mgr) = self.manager() else {
            return serde_json::json!({
                "enabled": false,
                "kind": "memory.knowledge_fabric",
                "capability_status": RealityCapabilityStatus::Disabled.as_str(),
                "projection_mode": "unavailable",
                "durable": false,
                "degraded": true,
                "degraded_reason": "memory not configured",
                "projection": null,
            });
        };
        let entries = match self.list_all_entries().await {
            Ok(entries) => entries,
            Err(error) => {
                return serde_json::json!({
                    "enabled": true,
                    "kind": "memory.knowledge_fabric",
                    "capability_status": RealityCapabilityStatus::Degraded.as_str(),
                    "projection_mode": "synthetic_from_memory_entries",
                    "durable": false,
                    "degraded": true,
                    "degraded_reason": error,
                    "projection": null,
                });
            }
        };
        let import_candidate_count = entries
            .iter()
            .filter(|entry| is_knowledge_memory_entry(entry))
            .count();
        let durable_fabric =
            memory::durable_knowledge_fabric_for_config_home(runtime::cowd_dirs::config_home_dir());
        let (capability_status, projection_mode, durable, degraded, degraded_reason, projection) =
            match durable_fabric {
                Ok(fabric) => (
                    RealityCapabilityStatus::EnabledAndWired.as_str(),
                    "durable_knowledge_store",
                    true,
                    false,
                    serde_json::Value::Null,
                    fabric.projection(),
                ),
                Err(error) => (
                    RealityCapabilityStatus::Degraded.as_str(),
                    "durable_knowledge_store_unavailable",
                    false,
                    true,
                    serde_json::json!(error.to_string()),
                    serde_json::Value::Null,
                ),
            };
        serde_json::json!({
            "enabled": true,
            "kind": "memory.knowledge_fabric",
            "capability_status": capability_status,
            "projection_mode": projection_mode,
            "durable": durable,
            "degraded": degraded,
            "degraded_reason": degraded_reason,
            "source": "knowledge.sqlite",
            "import_candidate_count": import_candidate_count,
            "projection": projection,
        })
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

fn is_knowledge_memory_entry(entry: &MemoryEntry) -> bool {
    matches!(
        entry.layer,
        MemoryLayer::L2 | MemoryLayer::L3 | MemoryLayer::L4
    ) || entry.tags.iter().any(|tag| {
        matches!(
            tag.as_str(),
            "knowledge"
                | "knowledge_base"
                | "policy"
                | "standard"
                | "procedure"
                | "architecture"
                | "domain"
                | "default"
                | "global"
        )
    })
}

fn knowledge_namespace_for_entry(entry: &MemoryEntry) -> KnowledgeNamespace {
    if entry
        .tags
        .iter()
        .any(|tag| tag == "global" || tag == "shared")
    {
        KnowledgeNamespace::SharedLibrary("global".to_string())
    } else {
        match &entry.scope {
            memory::MemoryScope::Project(project) => KnowledgeNamespace::Project(project.clone()),
            memory::MemoryScope::Global => KnowledgeNamespace::SharedLibrary("global".to_string()),
            memory::MemoryScope::Session(session) => KnowledgeNamespace::Corpus(session.clone()),
            memory::MemoryScope::Task(task) => KnowledgeNamespace::Corpus(task.clone()),
            memory::MemoryScope::Agent(agent) => KnowledgeNamespace::Corpus(agent.clone()),
        }
    }
}

fn knowledge_activation_policy_for_entry(entry: &MemoryEntry) -> KnowledgeActivationPolicy {
    if entry
        .tags
        .iter()
        .any(|tag| tag == "default" || tag == "global")
    {
        KnowledgeActivationPolicy::DefaultForDomain
    } else if matches!(entry.layer, MemoryLayer::L2) {
        KnowledgeActivationPolicy::DefaultForProjectGroup
    } else {
        KnowledgeActivationPolicy::OnDemand
    }
}

fn knowledge_governance_for_entry(entry: &MemoryEntry) -> KnowledgeGovernanceLevel {
    let text = format!("{} {}", entry.title, entry.content).to_ascii_lowercase();
    if text.contains("must not") || text.contains("禁止") || text.contains("不得") {
        KnowledgeGovernanceLevel::Blocking
    } else if text.contains("must")
        || text.contains("required")
        || text.contains("必须")
        || text.contains("应该")
    {
        KnowledgeGovernanceLevel::Required
    } else {
        KnowledgeGovernanceLevel::Advisory
    }
}

fn memory_capabilities_json(enabled: bool, vector_count: usize) -> serde_json::Value {
    if !enabled {
        return serde_json::json!({
            "vector_semantic": capability_probe_json(
                RealityCapabilityStatus::Disabled,
                "memory manager is not configured"
            ),
            "aaak_index": capability_probe_json(
                RealityCapabilityStatus::Disabled,
                "memory manager is not configured"
            ),
            "knowledge_fabric": capability_probe_json(
                RealityCapabilityStatus::Disabled,
                "memory manager is not configured"
            ),
            "context_envelope": capability_probe_json(
                RealityCapabilityStatus::Disabled,
                "memory manager is not configured"
            ),
        });
    }

    serde_json::json!({
        "vector_semantic": capability_probe_json(
            RealityCapabilityStatus::EnabledAndWired,
            format!(
                "vector index stores {vector_count} embeddings and participates in MemoryKernel recall as a first-class source when embeddings are available"
            )
        ),
        "aaak_index": capability_probe_json(
            RealityCapabilityStatus::EnabledAndWired,
            "AAAK compact navigation is exposed as a recall source and omission pointer for deep context recovery"
        ),
        "knowledge_fabric": capability_probe_json(
            RealityCapabilityStatus::EnabledAndWired,
            "KnowledgeFabric uses durable storage/knowledge.sqlite and feeds activation evidence through runtime context assembly"
        ),
        "context_envelope": capability_probe_json(
            RealityCapabilityStatus::EnabledAndWired,
            "ContextEnvelope is the single runtime prompt assembly boundary and is persisted for evidence and replay"
        ),
    })
}

fn capability_probe_json(
    status: RealityCapabilityStatus,
    reason: impl Into<String>,
) -> serde_json::Value {
    serde_json::json!({
        "status": status.as_str(),
        "reason": reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use memory::types::{AgentVisibility, MemoryCategory, MemorySource, Priority};

    fn memory_entry(
        layer: MemoryLayer,
        tags: Vec<String>,
        scope: memory::MemoryScope,
        content: &str,
    ) -> MemoryEntry {
        MemoryEntry {
            id: uuid::Uuid::new_v4(),
            layer,
            category: MemoryCategory::Reference,
            priority: Priority::High,
            source: MemorySource::Import,
            title: "Knowledge fixture".to_string(),
            content: content.to_string(),
            embedding: None,
            tags,
            relations: Vec::new(),
            confidence: 0.9,
            access_count: 0,
            staleness: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            last_accessed_at: None,
            scope,
            session_id: None,
            source_agent: Some("gateway-test".to_string()),
            visibility: AgentVisibility::Shared,
        }
    }

    #[test]
    fn knowledge_helpers_keep_global_and_project_scopes_distinct() {
        let project = memory_entry(
            MemoryLayer::L3,
            vec!["knowledge".to_string()],
            memory::MemoryScope::Project("cowd".to_string()),
            "must retain evidence",
        );
        let global = memory_entry(
            MemoryLayer::L0,
            vec!["global".to_string(), "default".to_string()],
            memory::MemoryScope::Global,
            "must follow user principle",
        );

        assert!(is_knowledge_memory_entry(&project));
        assert_eq!(
            knowledge_namespace_for_entry(&project),
            KnowledgeNamespace::Project("cowd".to_string())
        );
        assert_eq!(
            knowledge_namespace_for_entry(&global),
            KnowledgeNamespace::SharedLibrary("global".to_string())
        );
        assert_eq!(
            knowledge_activation_policy_for_entry(&global),
            KnowledgeActivationPolicy::DefaultForDomain
        );
        assert_eq!(
            knowledge_governance_for_entry(&project),
            KnowledgeGovernanceLevel::Required
        );
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
