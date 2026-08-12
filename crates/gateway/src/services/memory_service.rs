use std::{path::Path, sync::Arc};

use harness_contract::{
    knowledge::{KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace},
    reality::RealityCapabilityStatus,
};
use memory::types::{MemoryEntry, MemoryId, MemoryLayer};
use memory::{
    MemoryContextPacket, MemoryInformationState, MemoryKernel, MemoryState, MemoryTurnContext,
    RotAlert,
};

use super::{GatewayMemoryManager, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct MemoryService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
    manager: Option<Arc<GatewayMemoryManager>>,
    knowledge: Option<memory::KnowledgeFabric>,
}

impl MemoryService {
    pub(crate) fn new() -> Self {
        Self {
            label: "memory",
            owner: "0.9.380 GatewayServices",
            manager: None,
            knowledge: None,
        }
    }

    pub(crate) fn with_manager(manager: Option<Arc<GatewayMemoryManager>>) -> Self {
        Self {
            manager,
            ..Self::new()
        }
    }

    pub(crate) fn with_manager_and_knowledge(
        manager: Option<Arc<GatewayMemoryManager>>,
        knowledge: memory::KnowledgeFabric,
    ) -> Self {
        Self {
            manager,
            knowledge: Some(knowledge),
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

    pub(crate) async fn run_automatic_governance(
        &self,
        policy: &memory::GovernanceConfig,
        mode: memory::AutomaticGovernanceMode,
        resolver: Option<&dyn memory::SemanticGovernanceResolver>,
    ) -> Result<memory::AutomaticGovernanceReport, memory::MemoryError> {
        let manager = self
            .manager()
            .ok_or_else(|| memory::MemoryError::CapabilityUnavailable {
                capability: "memory_governance".to_string(),
                details: "memory manager is not configured".to_string(),
            })?;
        memory::run_automatic_governance_with_resolver(
            manager,
            self.knowledge.as_ref(),
            policy,
            mode,
            resolver,
        )
        .await
    }

    pub(crate) async fn status_projection(&self) -> serde_json::Value {
        if let Some(mgr) = self.manager() {
            let kernel = MemoryKernel::new(Arc::clone(&mgr));
            let layers = layer_summaries(&mgr).await;
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
            let kernel_degraded = kernel_health
                .get("degraded")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let kernel_degraded_reason = kernel_health
                .get("degraded_reasons")
                .and_then(serde_json::Value::as_array)
                .and_then(|reasons| reasons.first())
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned);
            let vector_count = mgr.vector_index_count();
            let scope_migrations = mgr
                .legacy_scope_migration_reports()
                .await
                .map(|reports| {
                    serde_json::json!({
                        "held_count": reports.len(),
                        "reports": reports,
                    })
                })
                .unwrap_or_else(|error| {
                    serde_json::json!({
                        "held_count": null,
                        "reports": [],
                        "error": error.to_string(),
                    })
                });
            let search_mode = mgr.search_mode_label();
            let semantic_supported = mgr.embedding_capability().supports_semantic();
            let (automatic_governance, automatic_governance_error) =
                match memory::last_automatic_governance_report(mgr.as_ref()).await {
                    Ok(report) => (report, None),
                    Err(error) => (None, Some(error.to_string())),
                };
            let automatic_governance_run = mgr.automatic_governance_run_status();
            let governance_review_queue_durable = mgr.maintenance_queue_is_durable();
            let capabilities =
                memory_capabilities_json(true, vector_count, search_mode, semantic_supported);
            let total_entries: usize = layers
                .iter()
                .filter_map(|layer| layer.get("entry_count").and_then(|value| value.as_u64()))
                .map(|count| count as usize)
                .sum();
            serde_json::json!({
                "enabled": true,
                "status": if kernel_degraded { "degraded" } else { "ready" },
                "degraded": kernel_degraded,
                "degraded_reason": kernel_degraded_reason,
                "layers": layers,
                "total_entries": total_entries,
                "vector_count": vector_count,
                "capabilities": capabilities,
                "session_store": true,
                "context_health": context_health_json(mgr.ctx_health()),
                "kernel_health": kernel_health,
                "scope_migration": scope_migrations,
                "runtime": {
                    "total_entries": layers.iter()
                        .filter_map(|layer| layer.get("retained_count").and_then(serde_json::Value::as_u64))
                        .sum::<u64>(),
                    "active_entries": total_entries,
                    "detail_route": "/api/memory/runtime",
                },
                "performance": mgr.performance_report(),
                "automatic_governance": automatic_governance,
                "automatic_governance_error": automatic_governance_error,
                "automatic_governance_run": automatic_governance_run,
                "governance_review_queue": {
                    "durable": governance_review_queue_durable,
                    "status": if governance_review_queue_durable { "durable" } else { "process_local" },
                    "warning": if governance_review_queue_durable {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(
                            "maintenance review candidates are process-local for the selected backend".to_string()
                        )
                    },
                },
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
                "capabilities": memory_capabilities_json(false, 0, "unavailable", false),
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

    pub(crate) async fn tagged_candidates(
        &self,
        query: memory::TaggedLookup,
    ) -> Result<Vec<MemoryEntry>, String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        mgr.tagged_candidates(query)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn layer_summaries(&self) -> Vec<serde_json::Value> {
        let Some(mgr) = self.manager() else {
            return empty_memory_layers_json();
        };
        layer_summaries(&mgr).await
    }

    pub(crate) async fn layer_projection(
        &self,
        layer: MemoryLayer,
        include_archived: bool,
    ) -> Result<serde_json::Value, String> {
        let mgr = self
            .manager()
            .ok_or_else(|| "memory not configured".to_string())?;
        let entries = mgr
            .list_layer_full_entries(layer)
            .await
            .map_err(|error| error.to_string())?;
        let kernel = MemoryKernel::new(Arc::clone(&mgr));
        let view = kernel
            .layer_view(layer, MemoryInformationState::Orientation)
            .await
            .map_err(|error| error.to_string())?;
        let states = view
            .atoms
            .into_iter()
            .map(|atom| (atom.id, atom.state))
            .collect::<std::collections::HashMap<_, _>>();
        let archived_count = states
            .values()
            .filter(|state| is_inactive_memory_state(**state))
            .count();
        let entries = entries
            .into_iter()
            .filter_map(|entry| {
                let state = states
                    .get(&entry.id)
                    .copied()
                    .unwrap_or(MemoryState::Active);
                if !include_archived && is_inactive_memory_state(state) {
                    return None;
                }
                let mut value = serde_json::to_value(entry).ok()?;
                value.as_object_mut()?.insert(
                    "lifecycle_state".to_string(),
                    serde_json::json!(format!("{state:?}").to_ascii_lowercase()),
                );
                Some(value)
            })
            .collect::<Vec<_>>();
        Ok(serde_json::json!({
            "enabled": true,
            "layer": format!("{layer:?}"),
            "entries": entries,
            "archived_count": archived_count,
            "include_archived": include_archived,
        }))
    }

    /// L0 identity projection (P9): durable role/language guidance plus
    /// provenance facts for direct inspection.
    pub(crate) async fn identity_projection(&self) -> serde_json::Value {
        let Some(mgr) = self.manager() else {
            return serde_json::json!({
                "status": "unavailable",
                "entries": [],
                "role": null,
                "language": null,
            });
        };
        let entries = mgr
            .list_layer_full_entries(MemoryLayer::L0)
            .await
            .unwrap_or_default();
        let identity_entries = entries
            .into_iter()
            .filter(|entry| matches!(entry.title.as_str(), "assistant-role" | "response-language"))
            .map(|entry| {
                serde_json::json!({
                    "title": entry.title,
                    "content": entry.content,
                    "layer": format!("{:?}", entry.layer).to_ascii_lowercase(),
                    "created_at_ms": entry.created_at.timestamp_millis().max(0) as u64,
                    "updated_at_ms": entry.updated_at.timestamp_millis().max(0) as u64,
                    "source": format!("{:?}", entry.source),
                })
            })
            .collect::<Vec<_>>();
        let role = identity_entries
            .iter()
            .find(|entry| entry["title"] == "assistant-role")
            .and_then(|entry| entry["content"].as_str())
            .map(ToOwned::to_owned);
        let language = identity_entries
            .iter()
            .find(|entry| entry["title"] == "response-language")
            .and_then(|entry| entry["content"].as_str())
            .map(ToOwned::to_owned);
        serde_json::json!({
            "status": if identity_entries.is_empty() { "missing" } else { "present" },
            "entries": identity_entries,
            "role": role,
            "language": language,
        })
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
            .map_err(|error| error.to_string())?;
        if let Err(error) = mgr.evict_vector_entry(&memory_id) {
            tracing::warn!(
                %error,
                %memory_id,
                "archived memory vector eviction degraded; lifecycle filtering remains authoritative"
            );
        }
        if let Some(knowledge) = self.knowledge.as_ref() {
            knowledge
                .quarantine_source(&format!("memory:{memory_id}"))
                .map_err(|error| {
                    format!(
                        "memory archived but derived knowledge quarantine failed for {memory_id}: {error}"
                    )
                })?;
        }
        Ok(())
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

        let kernel = MemoryKernel::new(mgr);
        let ctx = MemoryTurnContext::new("api-memory-packet", "api");
        let packet_result = kernel
            .context_packet_preview(&ctx, &query, &[], max_items, max_tokens)
            .await
            .map_err(|error| error.to_string());

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
        let kernel = MemoryKernel::new(mgr);
        let ctx = MemoryTurnContext::new(session_id, source);
        kernel
            .context_packet(&ctx, &query, &[], max_items, max_tokens)
            .await
            .map_err(|error| error.to_string())
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
        let kernel = MemoryKernel::new(mgr);
        let ctx = MemoryTurnContext::new(session_id, source);
        kernel
            .context_packet_preview(&ctx, &query, &[], max_items, max_tokens)
            .await
            .map_err(|error| error.to_string())
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

    pub(crate) async fn knowledge_projection(&self, _config_home: &Path) -> serde_json::Value {
        let Some(mgr) = self.manager() else {
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
        let entries = MemoryKernel::new(Arc::clone(&mgr))
            .filter_active_entries(entries)
            .await;
        let import_candidate_count = entries
            .iter()
            .filter(|entry| is_knowledge_memory_entry(entry))
            .count();
        let durable_fabric = self.knowledge.as_ref();
        let (capability_status, projection_mode, durable, degraded, degraded_reason, projection) =
            match durable_fabric {
                Some(fabric) => (
                    RealityCapabilityStatus::EnabledAndWired.as_str(),
                    "durable_knowledge_store",
                    true,
                    false,
                    serde_json::Value::Null,
                    fabric.projection(),
                ),
                None => (
                    RealityCapabilityStatus::Degraded.as_str(),
                    "durable_knowledge_store_unavailable",
                    false,
                    true,
                    serde_json::json!("selected Knowledge store is unavailable"),
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
            "source": "selected_storage_topology",
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
        match kernel.clusters(limit.min(501)).await {
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

async fn layer_summaries(mgr: &Arc<GatewayMemoryManager>) -> Vec<serde_json::Value> {
    mgr.list_layers().await
}

fn is_inactive_memory_state(state: MemoryState) -> bool {
    matches!(state, MemoryState::Archived | MemoryState::Superseded)
}

fn is_knowledge_memory_entry(entry: &MemoryEntry) -> bool {
    if is_runtime_memory_noise_entry(entry) {
        return false;
    }

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

fn is_runtime_memory_noise_entry(entry: &MemoryEntry) -> bool {
    matches!(
        entry.category,
        memory::types::MemoryCategory::UserPreference
            | memory::types::MemoryCategory::CompressedSummary
    ) || entry.tags.iter().any(|tag| {
        matches!(
            tag.as_str(),
            "runtime"
                | "session-checkpoint"
                | "semantic-checkpoint"
                | "tool-usage"
                | "usage-feedback"
        )
    }) || runtime_memory_noise_text(&format!("{} {}", entry.title, entry.content))
}

fn runtime_memory_noise_text(text: &str) -> bool {
    let normalized = text
        .to_lowercase()
        .replace('：', ":")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    [
        "user preference:",
        "session critical context checkpoint",
        "session pending work checkpoint",
        "session preferences checkpoint",
        "session tool evidence checkpoint",
        "frequent tool usage:",
        "usage_feedback:selected_count",
        "active memory lacks explicit orientation evidence",
        "用户偏好:",
        "会话 checkpoint",
        "会话检查点",
        "工具使用频率",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
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
            memory::MemoryScope::AgentDefinitionLineage(agent) => {
                KnowledgeNamespace::Corpus(agent.clone())
            }
            memory::MemoryScope::AgentInstance(agent) => KnowledgeNamespace::Corpus(agent.clone()),
            memory::MemoryScope::TeamRun(team) => KnowledgeNamespace::Corpus(team.clone()),
            memory::MemoryScope::LegacyUnresolvedAgent(agent) => {
                KnowledgeNamespace::Corpus(format!("held-{agent}"))
            }
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

fn memory_capabilities_json(
    enabled: bool,
    vector_count: usize,
    search_mode: &str,
    semantic_supported: bool,
) -> serde_json::Value {
    if !enabled {
        return serde_json::json!({
            "vector_semantic": capability_probe_json(
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

    let vector_status = if semantic_supported {
        RealityCapabilityStatus::EnabledAndWired
    } else {
        RealityCapabilityStatus::ConfiguredButUnwired
    };
    let vector_reason = if semantic_supported {
        format!(
            "semantic embedding backend is configured; search_mode={search_mode}; vector index currently stores {vector_count} embeddings"
        )
    } else {
        format!(
            "semantic embedding backend is not configured; search_mode={search_mode}; recall is using keyword sources while vector_count={vector_count}"
        )
    };

    serde_json::json!({
        "vector_semantic": capability_probe_json(vector_status, vector_reason),
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
        "background_extraction": health.background_extraction,
    })
}

fn empty_memory_layers_json() -> Vec<serde_json::Value> {
    ["L0", "L1", "L2", "L3", "L4"]
        .into_iter()
        .map(|layer| serde_json::json!({ "layer": layer, "entry_count": 0 }))
        .collect()
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

    #[test]
    fn knowledge_helpers_exclude_runtime_memory_noise() {
        let mut preference = memory_entry(
            MemoryLayer::L3,
            vec!["global".to_string(), "default".to_string()],
            memory::MemoryScope::Global,
            "不要无限展开读取上下文",
        );
        preference.category = MemoryCategory::UserPreference;
        preference.title = "User preference: 不要无限展开".to_string();

        let checkpoint = memory_entry(
            MemoryLayer::L3,
            vec!["semantic-checkpoint".to_string()],
            memory::MemoryScope::Session("s1".to_string()),
            "Session critical context checkpoint",
        );

        let tool_usage = memory_entry(
            MemoryLayer::L2,
            vec!["tool-usage".to_string()],
            memory::MemoryScope::Project("cowd".to_string()),
            "Frequent tool usage: rg selected_count=10",
        );

        let knowledge = memory_entry(
            MemoryLayer::L3,
            vec!["knowledge".to_string()],
            memory::MemoryScope::Project("cowd".to_string()),
            "必须保留证据链",
        );

        assert!(!is_knowledge_memory_entry(&preference));
        assert!(!is_knowledge_memory_entry(&checkpoint));
        assert!(!is_knowledge_memory_entry(&tool_usage));
        assert!(is_knowledge_memory_entry(&knowledge));
    }
}
