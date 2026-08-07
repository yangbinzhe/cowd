use std::{collections::HashMap, path::Path};

use connector::ExternalResourceRef;
use harness_contract::context::ArtifactRef;
use matrix_core::MatrixEvidencePacket;
use runtime::{
    AgentContextLease, AgentReturnRequirement, ContextAuthority, ContextEnvelope,
    ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextOmission, ContextProfile,
    ContextRole, ContextSourceKind, ContextVisibility,
};
use session::SessionEvent;

use super::{
    ConnectorService, ContextService, MemoryService, RuntimeContextBoundary, SessionService,
};

mod evidence;
mod history;
mod projection;

use evidence::*;

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
            ContextSourceKind::Matrix,
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

impl ContextService {
    pub(crate) async fn record_context_recommendation_action(
        &self,
        session: &SessionService,
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
        let event =
            crate::services::session_service::ContextSessionJournalEvent::recommendation_action(
                session_id,
                envelope_id,
                recommendation,
                action,
                note,
            );
        let payload = serde_json::to_value(&event).map_err(|error| {
            ContextServiceError::Internal(format!(
                "failed to serialize context recommendation action: {error}"
            ))
        })?;
        session
            .append_context_event(&event)
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
}
