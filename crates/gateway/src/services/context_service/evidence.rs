use super::*;

impl ContextService {
    pub(crate) async fn resolve_evidence_ref(
        &self,
        session: &SessionService,
        connector: &ConnectorService,
        workspace_root: &Path,
        reference: &str,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, ContextServiceError> {
        if let Some(path) = reference.strip_prefix("workspace://changed-file/") {
            Ok(self.workspace_evidence_preview(workspace_root, reference, path))
        } else if let Some(symbol) = reference.strip_prefix("workspace://symbol/") {
            Ok(serde_json::json!({
                "ref": reference,
                "kind": "workspace_symbol",
                "available": true,
                "symbol": symbol,
            }))
        } else if let Some(session_ref) = reference.strip_prefix("session://") {
            Ok(self
                .resolve_session_evidence(session, reference, session_ref)
                .await)
        } else if reference.starts_with("tool://") {
            Ok(self
                .resolve_tool_evidence(session, reference, session_id)
                .await)
        } else if let Some(knowledge_ref) = reference.strip_prefix("knowledge://") {
            Ok(serde_json::json!({
                "ref": reference,
                "kind": "knowledge",
                "available": true,
                "knowledge_ref": knowledge_ref,
                "reason": "knowledge evidence is derived from memory knowledge fabric projection; inspect /api/memory/knowledge for canon and pack details",
                "projection_api": "/api/memory/knowledge",
            }))
        } else if reference.starts_with("service://") || reference.starts_with("mcp://") {
            Ok(self.resolve_resource_evidence(connector, workspace_root, reference))
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
        connector: &ConnectorService,
        workspace_root: &Path,
        reference: &str,
    ) -> serde_json::Value {
        if !connector.resource_directory_path(workspace_root).exists() {
            return serde_json::json!({
                "ref": reference,
                "kind": "resource",
                "available": false,
                "reason": "resource directory is not initialized",
            });
        }
        match connector.get_resource(workspace_root, reference) {
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
        session: &SessionService,
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
        match session.stored_session(session_id).await {
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
        session: &SessionService,
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
        if let Ok(Some((_, raw_events))) = session
            .stored_events_by_type_page(session_id, "ToolObservationRaw", 0, 1000)
            .await
        {
            if let Some(raw_match) = raw_events.into_iter().find_map(|event| {
                let payload = serde_json::from_str::<serde_json::Value>(&event.event_json).ok()?;
                let evidence_matches = payload
                    .get("evidence_id")
                    .and_then(|value| value.as_str())
                    .is_some_and(|id| id == tool_id);
                evidence_matches.then(|| {
                    serde_json::json!({
                        "type": event.event_type,
                        "sequence": event.sequence,
                        "created_at_ms": event.created_at_ms,
                        "payload": payload,
                    })
                })
            }) {
                return serde_json::json!({
                    "ref": reference,
                    "kind": "tool",
                    "available": true,
                    "session_id": session_id,
                    "events": [raw_match],
                });
            }
        }
        let Some((_, events)) = session
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
                let id_matches = id_matches
                    || payload
                        .get("evidence_id")
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

pub(super) fn workspace_file_unavailable(reference: &str, reason: &str) -> serde_json::Value {
    serde_json::json!({
        "ref": reference,
        "kind": "workspace_file",
        "available": false,
        "reason": reason,
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
