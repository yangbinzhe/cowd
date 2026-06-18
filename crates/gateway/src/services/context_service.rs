use std::{collections::HashMap, path::Path};

use matrix_core::MatrixEvidencePacket;
use memory::store::session::SessionEvent;
use runtime::{
    AgentContextLease, AgentReturnRequirement, ContextAuthority, ContextEnvelope,
    ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextOmission, ContextProfile,
    ContextRole, ContextSourceKind, ContextVisibility, ExternalResourceRef,
};

use super::{ContextService, GatewayServices, RuntimeContextBoundary};

#[derive(Debug, Clone)]
pub(crate) enum ContextServiceError {
    BadRequest(String),
    NotFound(String),
    StoreUnavailable(String),
    Internal(String),
}

impl ContextServiceError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::BadRequest(message)
            | Self::NotFound(message)
            | Self::StoreUnavailable(message)
            | Self::Internal(message) => message.clone(),
        }
    }
}

impl ContextService {
    pub(crate) fn structured_evidence_item(&self, packet: &MatrixEvidencePacket) -> ContextItem {
        let mut item = ContextItem::new(
            format!("structured-evidence:{}", packet.packet_id),
            ContextSourceKind::ToolTrace,
            ContextRole::Evidence,
            format!(
                "Structured evidence packet {}: {}. Confidence {:.2}.",
                packet.packet_id, packet.problem_statement, packet.confidence
            ),
        );
        item.evidence = packet
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect();
        item.score = packet.confidence;
        item
    }

    pub(crate) fn workspace_evidence_preview(
        &self,
        root: &Path,
        reference: &str,
        relative: &str,
    ) -> serde_json::Value {
        const MAX_BYTES: u64 = 256 * 1024;
        const PREVIEW_BYTES: usize = 4096;

        let path = root.join(relative);
        let Ok(canonical_root) = root.canonicalize() else {
            return workspace_file_unavailable(reference, "workspace root unavailable");
        };
        let Ok(canonical_path) = path.canonicalize() else {
            return workspace_file_unavailable(reference, "file unavailable");
        };
        if !canonical_path.starts_with(&canonical_root) {
            return workspace_file_unavailable(reference, "file is outside workspace");
        }
        let Ok(metadata) = std::fs::metadata(&canonical_path) else {
            return workspace_file_unavailable(reference, "file metadata unavailable");
        };
        if !metadata.is_file() {
            return workspace_file_unavailable(reference, "path is not a file");
        }
        if metadata.len() > MAX_BYTES {
            return serde_json::json!({
                "ref": reference,
                "kind": "workspace_file",
                "available": true,
                "truncated": true,
                "size_bytes": metadata.len(),
                "reason": "file exceeds preview limit",
            });
        }
        let preview = std::fs::read_to_string(&canonical_path)
            .map(|content| content.chars().take(PREVIEW_BYTES).collect::<String>())
            .unwrap_or_default();
        serde_json::json!({
            "ref": reference,
            "kind": "workspace_file",
            "available": true,
            "path": relative,
            "size_bytes": metadata.len(),
            "preview": preview,
            "truncated": metadata.len() as usize > PREVIEW_BYTES,
        })
    }
}

impl GatewayServices {
    pub(crate) async fn context_history(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
        include_envelopes: bool,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some((total, stored_events)) = self
            .session
            .stored_events_by_type_page(session_id, "ContextEnvelope", from_seq, limit)
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!("failed to load context timeline: {error}"))
            })?
        else {
            return Err(ContextServiceError::StoreUnavailable(
                "session store not available".to_string(),
            ));
        };

        let envelope_events: Vec<serde_json::Value> = stored_events
            .into_iter()
            .map(context_envelope_event_json)
            .collect();
        let summaries: Vec<serde_json::Value> = envelope_events
            .iter()
            .map(context_envelope_summary_json)
            .collect();
        let next_seq = envelope_events
            .last()
            .and_then(|event| event["sequence"].as_u64())
            .map(|sequence| sequence as usize + 1);
        let has_more = envelope_events.len() < total;
        let envelopes = if include_envelopes {
            envelope_events
        } else {
            Vec::new()
        };

        tracing::info!(
            session_id = session_id,
            include_envelopes = include_envelopes,
            total = total,
            from_seq = from_seq,
            limit = limit,
            "context history loaded"
        );

        Ok(serde_json::json!({
            "session_id": session_id,
            "envelopes": envelopes,
            "summaries": summaries,
            "include_envelopes": include_envelopes,
            "total": total,
            "from_seq": from_seq,
            "next_seq": next_seq,
            "limit": limit,
            "has_more": has_more,
        }))
    }

    pub(crate) async fn context_envelope(
        &self,
        envelope_id: &str,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some(event) = self
            .session
            .context_event_by_envelope_id(envelope_id)
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!("failed to load context envelope: {error}"))
            })?
        else {
            return Err(ContextServiceError::NotFound(format!(
                "context envelope {envelope_id} not found"
            )));
        };

        tracing::info!(
            envelope_id = envelope_id,
            session_id = event.session_id.as_str(),
            sequence = event.sequence,
            "context envelope loaded"
        );

        Ok(serde_json::json!({
            "enabled": true,
            "source": "history",
            "context": context_envelope_event_json(event),
        }))
    }

    pub(crate) async fn context_recommendation_stats(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some((total, stored_events)) = self
            .session
            .stored_events_by_type_page(session_id, "ContextRecommendationAction", from_seq, limit)
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!(
                    "failed to load context recommendation stats: {error}"
                ))
            })?
        else {
            return Err(ContextServiceError::StoreUnavailable(
                "session store not available".to_string(),
            ));
        };

        let event_count = stored_events.len();
        let mut grouped: HashMap<String, serde_json::Value> = HashMap::new();
        for event in stored_events {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
                .unwrap_or_else(|_| serde_json::json!({}));
            let Some(recommendation) = payload
                .get("recommendation")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let action = payload
                .get("action")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("acknowledged");
            let entry = grouped
                .entry(recommendation.to_string())
                .or_insert_with(|| {
                    serde_json::json!({
                        "recommendation": recommendation,
                        "count": 0_u64,
                        "actions": {},
                        "latest_envelope_id": null,
                        "latest_created_at_ms": 0_u64,
                    })
                });
            let count = entry["count"].as_u64().unwrap_or(0) + 1;
            entry["count"] = serde_json::json!(count);
            let action_count = entry["actions"][action].as_u64().unwrap_or(0) + 1;
            entry["actions"][action] = serde_json::json!(action_count);
            if event.created_at_ms >= entry["latest_created_at_ms"].as_u64().unwrap_or(0) {
                entry["latest_created_at_ms"] = serde_json::json!(event.created_at_ms);
                entry["latest_envelope_id"] = payload
                    .get("envelope_id")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
            }
        }

        let mut recommendations: Vec<serde_json::Value> = grouped.into_values().collect();
        recommendations.sort_by(|left, right| {
            right["count"]
                .as_u64()
                .cmp(&left["count"].as_u64())
                .then_with(|| {
                    left["recommendation"]
                        .as_str()
                        .cmp(&right["recommendation"].as_str())
                })
        });

        Ok(serde_json::json!({
            "session_id": session_id,
            "recommendations": recommendations,
            "total": total,
            "from_seq": from_seq,
            "limit": limit,
            "has_more": event_count < total,
        }))
    }

    pub(crate) async fn record_context_recommendation_action(
        &self,
        session_id: &str,
        envelope_id: String,
        recommendation: String,
        action: String,
        note: Option<String>,
    ) -> Result<serde_json::Value, ContextServiceError> {
        if envelope_id.trim().is_empty() || recommendation.trim().is_empty() {
            return Err(ContextServiceError::BadRequest(
                "envelope_id and recommendation are required".to_string(),
            ));
        }
        let action = if action.trim().is_empty() {
            "acknowledged".to_string()
        } else {
            action
        };
        let payload = serde_json::json!({
            "type": "ContextRecommendationAction",
            "session_id": session_id,
            "envelope_id": envelope_id,
            "recommendation": recommendation,
            "action": action,
            "note": note,
        });
        self.session
            .append_timeline_event(session_id, "ContextRecommendationAction", payload.clone())
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!(
                    "failed to record context recommendation action: {error}"
                ))
            })?;

        Ok(serde_json::json!({
            "ok": true,
            "session_id": session_id,
            "event": payload,
        }))
    }

    pub(crate) async fn resolve_evidence_ref(
        &self,
        state: &crate::api_routes::AppState,
        reference: &str,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, ContextServiceError> {
        if let Some(path) = reference.strip_prefix("workspace://changed-file/") {
            Ok(self
                .context
                .workspace_evidence_preview(&state.workspace_root, reference, path))
        } else if let Some(symbol) = reference.strip_prefix("workspace://symbol/") {
            Ok(serde_json::json!({
                "ref": reference,
                "kind": "workspace_symbol",
                "available": true,
                "symbol": symbol,
            }))
        } else if let Some(session_ref) = reference.strip_prefix("session://") {
            Ok(self.resolve_session_evidence(reference, session_ref).await)
        } else if reference.starts_with("tool://") {
            Ok(self.resolve_tool_evidence(reference, session_id).await)
        } else if reference.starts_with("service://") || reference.starts_with("mcp://") {
            Ok(self.resolve_resource_evidence(&state.workspace_root, reference))
        } else if reference.starts_with("agent://") {
            Ok(serde_json::json!({
                "ref": reference,
                "kind": "agent",
                "available": false,
                "reason": "agent evidence payload drilldown is not persisted yet",
            }))
        } else {
            Err(ContextServiceError::BadRequest(format!(
                "unsupported evidence ref: {reference}"
            )))
        }
    }

    fn resolve_resource_evidence(
        &self,
        workspace_root: &Path,
        reference: &str,
    ) -> serde_json::Value {
        if !self
            .connector
            .resource_directory_path(workspace_root)
            .exists()
        {
            return serde_json::json!({
                "ref": reference,
                "kind": "resource",
                "available": false,
                "reason": "resource directory is not initialized",
            });
        }
        match self.connector.get_resource(workspace_root, reference) {
            Ok(Some(resource)) => serde_json::json!({
                "ref": reference,
                "kind": "resource",
                "available": true,
                "resource": resource,
                "body": null,
                "reason": "resource evidence resolves metadata only; fetch/read must go through connector capability",
                "body_policy": if resource.provider == "feishu" { "metadata_only" } else { "not_persisted" },
                "retrieval_capability": resource_retrieval_capability(&resource),
                "next_actions": resource_next_actions(&resource),
            }),
            Ok(None) => serde_json::json!({
                "ref": reference,
                "kind": "resource",
                "available": false,
                "reason": "resource ref not found",
            }),
            Err(error) => serde_json::json!({
                "ref": reference,
                "kind": "resource",
                "available": false,
                "reason": format!("resource lookup failed: {error}"),
            }),
        }
    }

    async fn resolve_session_evidence(
        &self,
        reference: &str,
        session_ref: &str,
    ) -> serde_json::Value {
        let session_id = session_ref.split('/').next().unwrap_or_default();
        if session_id.is_empty() {
            return serde_json::json!({
                "ref": reference,
                "kind": "session",
                "available": false,
                "reason": "missing session id",
            });
        }
        match self.session.stored_session(session_id).await {
            Ok(Some(session)) => serde_json::json!({
                "ref": reference,
                "kind": "session",
                "available": true,
                "session": {
                    "session_id": session.session_id,
                    "platform": session.platform,
                    "model": session.model,
                    "created_at": session.created_at,
                    "last_activity": session.last_activity,
                    "message_count": session.message_count,
                    "status": session.status,
                },
            }),
            Ok(None) => serde_json::json!({
                "ref": reference,
                "kind": "session",
                "available": false,
                "reason": "session not found",
            }),
            Err(error) => serde_json::json!({
                "ref": reference,
                "kind": "session",
                "available": false,
                "reason": format!("session lookup failed: {error}"),
            }),
        }
    }

    async fn resolve_tool_evidence(
        &self,
        reference: &str,
        session_id: Option<&str>,
    ) -> serde_json::Value {
        let Some(session_id) = session_id else {
            return serde_json::json!({
                "ref": reference,
                "kind": "tool",
                "available": false,
                "reason": "session_id is required for tool evidence",
            });
        };
        let tool_id = reference
            .strip_prefix("tool://")
            .and_then(|tail| tail.split('/').next())
            .unwrap_or_default();
        let Some((_, events)) = self
            .session
            .stored_events_page(session_id, 0, 500)
            .await
            .ok()
            .flatten()
        else {
            return serde_json::json!({
                "ref": reference,
                "kind": "tool",
                "available": false,
                "reason": "session events unavailable",
            });
        };
        let matches = events
            .into_iter()
            .filter_map(|event| {
                let payload = serde_json::from_str::<serde_json::Value>(&event.event_json).ok()?;
                let id_matches = payload
                    .get("id")
                    .and_then(|value| value.as_str())
                    .is_some_and(|id| id == tool_id)
                    || payload
                        .get("tool_use_id")
                        .and_then(|value| value.as_str())
                        .is_some_and(|id| id == tool_id);
                id_matches.then(|| {
                    serde_json::json!({
                        "type": event.event_type,
                        "sequence": event.sequence,
                        "created_at_ms": event.created_at_ms,
                        "payload": payload,
                    })
                })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "ref": reference,
            "kind": "tool",
            "available": !matches.is_empty(),
            "session_id": session_id,
            "events": matches,
        })
    }
}

fn workspace_file_unavailable(reference: &str, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "ref": reference,
        "kind": "workspace_file",
        "available": false,
        "reason": reason,
    })
}

fn context_envelope_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    let envelope = payload
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let envelope_id = payload
        .get("envelope_id")
        .cloned()
        .or_else(|| envelope.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    let run_id = payload
        .get("run_id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "envelope_id": envelope_id,
        "run_id": run_id,
        "envelope": envelope,
    })
}

fn context_envelope_summary_json(event: &serde_json::Value) -> serde_json::Value {
    let envelope = event
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let diagnostics = envelope
        .get("diagnostics")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::json!({
        "session_id": event.get("session_id").cloned().unwrap_or(serde_json::Value::Null),
        "sequence": event.get("sequence").cloned().unwrap_or(serde_json::Value::Null),
        "created_at_ms": event.get("created_at_ms").cloned().unwrap_or(serde_json::Value::Null),
        "envelope_id": event.get("envelope_id").cloned().unwrap_or_else(|| envelope.get("id").cloned().unwrap_or(serde_json::Value::Null)),
        "run_id": event.get("run_id").cloned().unwrap_or(serde_json::Value::Null),
        "profile": envelope.get("profile").cloned().unwrap_or(serde_json::Value::Null),
        "intent": envelope.get("intent").cloned().unwrap_or(serde_json::Value::Null),
        "pressure_bp": diagnostics.get("pressure_bp").cloned().unwrap_or(serde_json::Value::Null),
        "selected_count": envelope.get("selected").and_then(|value| value.as_array()).map(|items| items.len()).unwrap_or(0),
        "omitted_count": envelope.get("omitted").and_then(|value| value.as_array()).map(|items| items.len()).unwrap_or(0),
    })
}

fn resource_retrieval_capability(resource: &ExternalResourceRef) -> Option<String> {
    if resource.provider != "feishu" {
        return None;
    }
    let operation = match resource.resource_type.as_str() {
        "drive" => "drive.metadata",
        "wiki" => "wiki.node_readonly",
        "docx" => "docx.read",
        other => other,
    };
    Some(format!("service.feishu.{operation}"))
}

fn resource_next_actions(resource: &ExternalResourceRef) -> Vec<&'static str> {
    if resource.provider == "feishu" {
        vec![
            "review_metadata_and_permissions",
            "request_or_use_feishu_read_scope",
            "fetch_body_through_connector_before_context_injection",
        ]
    } else {
        vec!["review_metadata", "fetch_body_through_connector_if_needed"]
    }
}

impl GatewayServices {
    pub(crate) async fn current_context_projection(
        &self,
        state: &crate::api_routes::AppState,
        params: HashMap<String, String>,
    ) -> serde_json::Value {
        let session_id = params
            .get("session_id")
            .cloned()
            .or_else(|| state.list_active_session_ids().into_iter().next())
            .unwrap_or_else(|| "api-context".to_string());
        let query = params.get("q").cloned().unwrap_or_default();
        let profile = params
            .get("profile")
            .and_then(|value| parse_context_profile(value))
            .unwrap_or(ContextProfile::MainTurn);

        if let Some(runtime_entry) = state.active_runtime(&session_id) {
            if let Ok(runtime) = runtime_entry.try_lock() {
                if let Some(envelope) = runtime.last_context_envelope() {
                    return context_projection_json("runtime", envelope, &params);
                }
            } else {
                tracing::debug!(
                    %session_id,
                    "runtime context envelope skipped because active runtime is busy"
                );
            }
        }

        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = RuntimeContextBoundary::mode_for_profile(profile);
        let mut dynamic_items = Vec::new();
        let mut omitted_items = Vec::new();
        let mut degraded = Vec::new();

        match self
            .memory
            .context_packet(session_id.clone(), "api", query.clone(), 12, 2_000)
            .await
        {
            Ok(packet) => {
                for item in packet.selected {
                    let mut context_item = ContextItem::new(
                        item.atom.id.to_string(),
                        ContextSourceKind::Memory,
                        match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        },
                        format!(
                            "{}\nreason: {}\nevidence: {}",
                            item.atom.title,
                            item.reason,
                            item.atom.evidence_pointer.as_deref().unwrap_or("")
                        ),
                    );
                    context_item.authority = ContextAuthority::Session;
                    context_item.visibility = ContextVisibility::Private;
                    context_item.score = item.atom.confidence;
                    dynamic_items.push(context_item);
                }
                for omitted in packet.omitted {
                    omitted_items.push(ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    });
                }
            }
            Err(_) => degraded.push(ContextSourceKind::Memory),
        }

        dynamic_items.extend(self.resource_context_items(&state.workspace_root, &query));

        let mut envelope = RuntimeContextBoundary::build_envelope(ContextEnvelopeRequest {
            profile,
            runtime_header: RuntimeContextBoundary::runtime_header(&identity, profile),
            identity,
            intent: query,
            stable_head: vec!["cowd-context-runtime:v0.8.13".to_string()],
            dynamic_items,
            omitted: omitted_items,
            total_budget_tokens: 8_000,
        });
        envelope.diagnostics.degraded_sources = degraded;
        context_projection_json("synthetic", envelope, &params)
    }

    fn resource_context_items(&self, workspace_root: &Path, query: &str) -> Vec<ContextItem> {
        if !self
            .connector
            .resource_directory_path(workspace_root)
            .exists()
        {
            return Vec::new();
        }
        let resources = if query.trim().is_empty() {
            self.connector.recent_resources(workspace_root, 5)
        } else {
            self.connector.search_resources(workspace_root, query, 5)
        }
        .unwrap_or_default();

        resources.into_iter().map(resource_context_item).collect()
    }
}

fn context_projection_json(
    source: &'static str,
    envelope: ContextEnvelope,
    params: &HashMap<String, String>,
) -> serde_json::Value {
    let lean_probe = RuntimeContextBoundary::lean_probe(&envelope);
    let policy_decision = RuntimeContextBoundary::policy_decision(&lean_probe);
    let mode_coverage = RuntimeContextBoundary::mode_coverage_report(
        envelope.identity.session_id.clone(),
        envelope.intent.clone(),
        envelope.assembled.stable_head.clone(),
        envelope.selected.clone(),
        envelope.budget.total_tokens,
    );
    let cache_stability = RuntimeContextBoundary::cache_stability_report(&envelope, &envelope);
    let snapshot = RuntimeContextBoundary::snapshot(&envelope);
    let budget_explanation = RuntimeContextBoundary::budget_explanation(&envelope);
    let agent_view = context_agent_view_from_params(params, &envelope);

    serde_json::json!({
        "enabled": true,
        "source": source,
        "envelope": envelope,
        "lean_probe": lean_probe,
        "policy_decision": policy_decision,
        "cache_stability": cache_stability,
        "mode_coverage": mode_coverage,
        "snapshot": snapshot,
        "budget_explanation": budget_explanation,
        "agent_view": agent_view,
    })
}

fn context_agent_view_from_params(
    params: &HashMap<String, String>,
    envelope: &ContextEnvelope,
) -> Option<runtime::AgentContextView> {
    let agent_id = params
        .get("agent_id")
        .or_else(|| params.get("agent"))
        .map(String::as_str)?
        .trim();
    if agent_id.is_empty() {
        return None;
    }
    let allowed_sources = params
        .get("agent_sources")
        .map(|raw| {
            raw.split(',')
                .filter_map(parse_context_source_kind)
                .collect::<Vec<_>>()
        })
        .filter(|sources| !sources.is_empty())
        .unwrap_or_else(|| {
            vec![
                ContextSourceKind::Task,
                ContextSourceKind::Workspace,
                ContextSourceKind::Memory,
                ContextSourceKind::AgentPeer,
            ]
        });
    Some(RuntimeContextBoundary::agent_context_view(
        envelope,
        AgentContextLease {
            parent_session_id: envelope.identity.session_id.clone(),
            parent_agent_id: envelope.identity.agent_id.clone(),
            child_agent_id: agent_id.to_string(),
            task_contract: params
                .get("agent_task")
                .cloned()
                .unwrap_or_else(|| envelope.intent.clone()),
            allowed_sources,
            max_tokens: params
                .get("agent_budget")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(4_000),
            required_return: vec![
                AgentReturnRequirement::ResultSummary,
                AgentReturnRequirement::Evidence,
                AgentReturnRequirement::Conflicts,
            ],
        },
    ))
}

fn parse_context_source_kind(value: &str) -> Option<ContextSourceKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "stablehead" | "stable_head" => Some(ContextSourceKind::StableHead),
        "runtimeheader" | "runtime_header" => Some(ContextSourceKind::RuntimeHeader),
        "conversation" => Some(ContextSourceKind::Conversation),
        "memory" => Some(ContextSourceKind::Memory),
        "task" => Some(ContextSourceKind::Task),
        "tooltrace" | "tool_trace" => Some(ContextSourceKind::ToolTrace),
        "workspace" => Some(ContextSourceKind::Workspace),
        "agentpeer" | "agent_peer" => Some(ContextSourceKind::AgentPeer),
        "handoff" => Some(ContextSourceKind::Handoff),
        _ => None,
    }
}

fn parse_context_profile(value: &str) -> Option<ContextProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mainturn" | "main" => Some(ContextProfile::MainTurn),
        "sologoal" | "solo" => Some(ContextProfile::SoloGoal),
        "yologoal" | "yolo" => Some(ContextProfile::YoloGoal),
        "subagent" | "sub_agent" => Some(ContextProfile::SubAgent),
        "collaboration" => Some(ContextProfile::Collaboration),
        "review" => Some(ContextProfile::Review),
        "resume" => Some(ContextProfile::Resume),
        "cron" => Some(ContextProfile::Cron),
        _ => None,
    }
}

fn resource_context_item(resource: ExternalResourceRef) -> ContextItem {
    let mut content = format!(
        "resource: {}\nref: {}\nprovider: {}\ntype: {}\nindexed_state: {}",
        resource.title,
        resource.reference,
        resource.provider,
        resource.resource_type,
        resource.indexed_state
    );
    if matches!(resource.indexed_state.as_str(), "stale" | "degraded") {
        content.push_str(
            "\nwarning: resource metadata may be stale or degraded; resolve evidence before relying on details",
        );
    }
    if resource.provider == "feishu" {
        content.push_str(
            "\nbody_policy: metadata_only\nretrieval: use an authorized Feishu read capability before injecting body content",
        );
    }
    let mut item = ContextItem::new(
        resource.reference.clone(),
        ContextSourceKind::Workspace,
        ContextRole::Evidence,
        content,
    );
    item.authority = ContextAuthority::Derived;
    item.visibility = ContextVisibility::Shared;
    item.score = if resource.indexed_state == "stale" {
        0.45
    } else {
        0.7
    };
    item.evidence = vec![resource.reference];
    item
}
