use super::*;
use harness_contract::context::{EvidenceAccessRef, EvidenceAuditProjection};
use sha2::{Digest, Sha256};

const DEFAULT_EVIDENCE_SNIPPET_BYTES: usize = 4 * 1024;
const MAX_EVIDENCE_SNIPPET_BYTES: usize = 16 * 1024;

impl ContextService {
    pub(crate) async fn resolve_evidence_ref(
        &self,
        session: &SessionService,
        connector: &ConnectorService,
        workspace_root: &Path,
        reference: &str,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, ContextServiceError> {
        self.resolve_evidence_ref_with_snippet(
            session,
            connector,
            workspace_root,
            reference,
            session_id,
            None,
        )
        .await
    }

    pub(crate) async fn resolve_evidence_ref_with_snippet(
        &self,
        session: &SessionService,
        connector: &ConnectorService,
        workspace_root: &Path,
        reference: &str,
        session_id: Option<&str>,
        snippet_bytes: Option<usize>,
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
                .resolve_tool_evidence(
                    session,
                    reference,
                    session_id,
                    snippet_bytes
                        .unwrap_or(DEFAULT_EVIDENCE_SNIPPET_BYTES)
                        .min(MAX_EVIDENCE_SNIPPET_BYTES),
                )
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
        snippet_bytes: usize,
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
        if let Some(projection) = self
            .find_evidence_audit_projection(session, session_id, tool_id)
            .await
        {
            return self
                .resolve_audit_projection(session, session_id, projection, snippet_bytes)
                .await;
        }
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
                    let raw = payload.get("raw").and_then(serde_json::Value::as_str);
                    serde_json::json!({
                        "type": event.event_type,
                        "sequence": event.sequence,
                        "created_at_ms": event.created_at_ms,
                        "metadata": raw_evidence_metadata(&payload),
                        "snippet": raw.map(|raw| byte_safe_snippet(raw, snippet_bytes)),
                        "snippet_truncated": raw.is_some_and(|raw| raw.len() > snippet_bytes),
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

    pub(crate) async fn evidence_audit_projections(
        &self,
        session: &SessionService,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some((total, events)) = session
            .stored_events_by_type_page(session_id, "ContextTurnReport", from_sequence, limit)
            .await
            .map_err(|error| ContextServiceError::StoreUnavailable(error.to_string()))?
        else {
            return Err(ContextServiceError::StoreUnavailable(
                "session store is unavailable".to_string(),
            ));
        };
        let mut projections = Vec::new();
        for event in events {
            let Ok(payload) = serde_json::from_str::<serde_json::Value>(&event.event_json) else {
                continue;
            };
            let report = payload.get("report").unwrap_or(&payload);
            let turn_id = report
                .get("turn_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            let Some(items) = report
                .get("audit_projections")
                .and_then(serde_json::Value::as_array)
            else {
                continue;
            };
            for item in items {
                let Ok(projection) =
                    serde_json::from_value::<EvidenceAuditProjection>(item.clone())
                else {
                    continue;
                };
                if projection_visible_to_session(&projection, session_id) {
                    projections.push(serde_json::json!({
                        "sequence": event.sequence,
                        "created_at_ms": event.created_at_ms,
                        "turn_id": turn_id,
                        "projection": projection,
                    }));
                }
            }
        }
        Ok(serde_json::json!({
            "kind": "context.evidence_audit_projections",
            "session_id": session_id,
            "total_context_reports": total,
            "from_sequence": from_sequence,
            "limit": limit,
            "projection_count": projections.len(),
            "projections": projections,
        }))
    }

    async fn find_evidence_audit_projection(
        &self,
        session: &SessionService,
        session_id: &str,
        evidence_id: &str,
    ) -> Option<EvidenceAuditProjection> {
        let (_, events) = session
            .stored_events_by_type_page(session_id, "ContextTurnReport", 0, 1000)
            .await
            .ok()??;
        events.into_iter().rev().find_map(|event| {
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json).ok()?;
            let report = payload.get("report").unwrap_or(&payload);
            report
                .get("audit_projections")?
                .as_array()?
                .iter()
                .filter_map(|item| serde_json::from_value(item.clone()).ok())
                .find(|projection: &EvidenceAuditProjection| {
                    projection.evidence_ref.id() == evidence_id
                        && projection_visible_to_session(projection, session_id)
                })
        })
    }

    async fn resolve_audit_projection(
        &self,
        session: &SessionService,
        session_id: &str,
        projection: EvidenceAuditProjection,
        snippet_bytes: usize,
    ) -> serde_json::Value {
        let Some(access) = projection.access.as_ref() else {
            return serde_json::json!({
                "ref": format!("tool://{}", projection.evidence_ref.id()),
                "kind": "tool",
                "available": false,
                "projection": projection,
                "reason": "raw evidence has no durable access receipt",
            });
        };
        let Some(sequence) = validated_session_event_selector(access, session_id) else {
            return serde_json::json!({
                "ref": format!("tool://{}", projection.evidence_ref.id()),
                "kind": "tool",
                "available": false,
                "projection": projection,
                "reason": "evidence access scope or retrieval selector is invalid for this session",
            });
        };
        let event = session
            .stored_events_page(session_id, sequence, 1)
            .await
            .ok()
            .flatten()
            .and_then(|(_, events)| events.into_iter().find(|event| event.sequence == sequence));
        let Some(event) = event else {
            return serde_json::json!({
                "ref": format!("tool://{}", projection.evidence_ref.id()),
                "kind": "tool",
                "available": false,
                "projection": projection,
                "reason": "durable evidence event is unavailable",
            });
        };
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&event.event_json) else {
            return serde_json::json!({
                "ref": format!("tool://{}", projection.evidence_ref.id()),
                "kind": "tool",
                "available": false,
                "projection": projection,
                "reason": "durable evidence event payload is invalid",
            });
        };
        let raw = payload.get("raw").and_then(serde_json::Value::as_str);
        let verified = raw.is_some_and(|raw| evidence_payload_matches(raw, access));
        serde_json::json!({
            "ref": format!("tool://{}", projection.evidence_ref.id()),
            "kind": "tool",
            "available": verified,
            "verified": verified,
            "session_id": session_id,
            "projection": projection,
            "event": {
                "type": event.event_type,
                "sequence": event.sequence,
                "created_at_ms": event.created_at_ms,
                "metadata": raw_evidence_metadata(&payload),
                "snippet": raw.filter(|_| verified).map(|raw| byte_safe_snippet(raw, snippet_bytes)),
                "snippet_truncated": raw.is_some_and(|raw| verified && raw.len() > snippet_bytes),
            },
            "reason": (!verified).then_some("durable evidence hash or byte count does not match"),
        })
    }
}

fn projection_visible_to_session(projection: &EvidenceAuditProjection, session_id: &str) -> bool {
    projection.access.as_ref().is_none_or(|access| {
        access.is_durable() && access.visibility_scope == format!("session:{session_id}")
    })
}

fn validated_session_event_selector(access: &EvidenceAccessRef, session_id: &str) -> Option<usize> {
    if !access.is_durable() || access.visibility_scope != format!("session:{session_id}") {
        return None;
    }
    let selector = access.retrieval_selector.strip_prefix("session-event://")?;
    let (selector_session, sequence) = selector.rsplit_once('/')?;
    (selector_session == session_id)
        .then(|| sequence.parse::<usize>().ok())
        .flatten()
}

fn evidence_payload_matches(raw: &str, access: &EvidenceAccessRef) -> bool {
    access.bytes == raw.len() as u64
        && access.sha256 == format!("sha256:{:x}", Sha256::digest(raw.as_bytes()))
}

fn byte_safe_snippet(raw: &str, max_bytes: usize) -> &str {
    if raw.len() <= max_bytes {
        return raw;
    }
    let mut end = max_bytes;
    while !raw.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    &raw[..end]
}

fn raw_evidence_metadata(payload: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "evidence_id": payload.get("evidence_id"),
        "tool_call_id": payload.get("tool_call_id"),
        "tool_name": payload.get("tool_name"),
        "input_hash": payload.get("input_hash"),
        "is_error": payload.get("is_error"),
        "duration_ms": payload.get("duration_ms"),
        "line_count": payload.get("line_count"),
        "byte_count": payload.get("byte_count"),
        "content_hash": payload.get("content_hash"),
    })
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

#[cfg(test)]
mod tests {
    use harness_contract::{
        context::{EvidenceAccessRef, EvidenceAuditProjection, EvidenceContentKind},
        core::EvidenceRef,
    };
    use sha2::Digest;

    use super::{
        byte_safe_snippet, evidence_payload_matches, projection_visible_to_session,
        validated_session_event_selector,
    };

    fn projection(session_id: &str, raw: &str) -> EvidenceAuditProjection {
        let evidence_ref = EvidenceRef::new("tool", "evidence-1");
        EvidenceAuditProjection {
            evidence_ref: evidence_ref.clone(),
            content_kind: EvidenceContentKind::Text,
            raw_tokens: 4,
            receipt_tokens: 2,
            omitted_tokens: 2,
            raw_available: true,
            access: Some(EvidenceAccessRef::durable(
                evidence_ref,
                format!("sha256:{:x}", sha2::Sha256::digest(raw.as_bytes())),
                raw.len() as u64,
                "text/plain",
                format!("session-event://{session_id}/7"),
                format!("session:{session_id}"),
            )),
        }
    }

    #[test]
    fn durable_evidence_access_is_scoped_to_its_session() {
        let projection = projection("session-a", "complete raw evidence");
        assert!(projection_visible_to_session(&projection, "session-a"));
        assert!(!projection_visible_to_session(&projection, "session-b"));
        let access = projection.access.as_ref().expect("durable access");
        assert_eq!(
            validated_session_event_selector(access, "session-a"),
            Some(7)
        );
        assert_eq!(validated_session_event_selector(access, "session-b"), None);
    }

    #[test]
    fn raw_payload_must_match_durable_hash_and_byte_count() {
        let projection = projection("session-a", "complete raw evidence");
        let access = projection.access.as_ref().expect("durable access");
        assert!(evidence_payload_matches("complete raw evidence", access));
        assert!(!evidence_payload_matches("changed raw evidence", access));
    }

    #[test]
    fn snippets_respect_utf8_boundaries() {
        assert_eq!(byte_safe_snippet("工具结果", 4), "工");
    }
}
