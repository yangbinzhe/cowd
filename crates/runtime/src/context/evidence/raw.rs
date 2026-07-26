//! Canonical raw-evidence durability boundary.

use async_trait::async_trait;
use harness_contract::context::{
    ArtifactRef, ArtifactWriteDescriptor, EvidenceAccessRef, EvidenceDurability,
};
use harness_contract::core::EvidenceRef;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEvidenceWrite {
    pub evidence_ref: EvidenceRef,
    pub session_id: String,
    pub media_type: String,
    pub visibility_scope: String,
    pub payload: String,
    /// Source-specific metadata is projected with the canonical payload but
    /// never changes the durable receipt identity.
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEvidenceRead {
    pub access: EvidenceAccessRef,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RawEvidenceError {
    #[error("raw evidence persistence failed: {0}")]
    Persistence(String),
    #[error("raw evidence retrieval failed: {0}")]
    Retrieval(String),
    #[error("invalid raw evidence durability receipt: {0}")]
    InvalidReceipt(String),
}

#[async_trait]
pub trait RawEvidenceStore: Send + Sync {
    async fn persist(&self, write: RawEvidenceWrite)
        -> Result<EvidenceAccessRef, RawEvidenceError>;
    async fn retrieve(&self, access: &EvidenceAccessRef) -> Result<Vec<u8>, RawEvidenceError>;
}

/// Runtime production adapter. Artifact bytes are durable before the bounded
/// Session receipt is appended; Session events never carry raw output.
#[derive(Clone)]
pub struct SessionPortRawEvidenceStore {
    journal: Arc<dyn crate::SessionRuntimeJournalPort>,
    artifacts: Arc<crate::ArtifactStore>,
}

impl SessionPortRawEvidenceStore {
    #[must_use]
    pub fn new(
        journal: Arc<dyn crate::SessionRuntimeJournalPort>,
        artifacts: Arc<crate::ArtifactStore>,
    ) -> Self {
        Self { journal, artifacts }
    }

    /// Publish a bounded Session receipt for bytes a Tool already wrote
    /// through the selected Artifact sink.
    pub async fn persist_artifact(
        &self,
        evidence_ref: EvidenceRef,
        session_id: String,
        artifact: ArtifactRef,
        metadata: Value,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        if !artifact.is_durable() {
            return Err(RawEvidenceError::InvalidReceipt(
                "staged tool artifact is not durable".to_string(),
            ));
        }
        let expected_scope = format!("session:{session_id}");
        if artifact.visibility_scope != expected_scope {
            return Err(RawEvidenceError::InvalidReceipt(format!(
                "staged tool artifact scope `{}` does not match `{expected_scope}`",
                artifact.visibility_scope
            )));
        }
        // A zero-length authorized range validates selector, metadata, hash,
        // size, media type, and visibility without materializing the payload.
        self.artifacts
            .read(&artifact, &expected_scope, Some(0..0))
            .await
            .map_err(|error| RawEvidenceError::InvalidReceipt(error.to_string()))?;
        self.publish_artifact_receipt(evidence_ref, session_id, artifact, metadata, false)
            .await
    }

    async fn publish_artifact_receipt(
        &self,
        evidence_ref: EvidenceRef,
        session_id: String,
        artifact: ArtifactRef,
        metadata: Value,
        delete_on_append_failure: bool,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        let pin_owner = format!("evidence:{session_id}:{}", evidence_ref.id());
        self.artifacts
            .pin(
                &artifact,
                &pin_owner,
                crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            )
            .map_err(|error| RawEvidenceError::Persistence(error.to_string()))?;
        let mut payload = metadata.as_object().cloned().unwrap_or_else(Map::new);
        payload.insert("type".to_string(), Value::String("RawEvidence".to_string()));
        payload.insert(
            "evidence_id".to_string(),
            Value::String(evidence_ref.id().to_string()),
        );
        payload.insert("session_id".to_string(), Value::String(session_id.clone()));
        payload.insert(
            "media_type".to_string(),
            Value::String(artifact.media_type.clone()),
        );
        payload.insert(
            "visibility_scope".to_string(),
            Value::String(artifact.visibility_scope.clone()),
        );
        payload.insert("byte_count".to_string(), Value::from(artifact.bytes));
        payload.insert(
            "content_hash".to_string(),
            Value::String(artifact.sha256.clone()),
        );
        payload.insert(
            "artifact_selector".to_string(),
            Value::String(artifact.selector.clone()),
        );
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default();
        let mut event = crate::RuntimeSessionEvent::new(
            session_id.clone(),
            0,
            crate::RuntimeSessionEventKind::EvidenceRawPersisted,
            Value::Object(payload),
            created_at_ms,
        );
        event.refs.push(crate::RuntimeSessionEventRef {
            ref_type: "evidence".to_string(),
            id: evidence_ref.id().to_string(),
            label: Some(evidence_ref.0.ref_type.clone()),
        });
        if let Err(error) = self.journal.append_event(&event).await {
            let _ = self.artifacts.unpin(&artifact, &pin_owner);
            if delete_on_append_failure {
                let _ = self.artifacts.delete(&artifact, &artifact.visibility_scope);
            }
            return Err(RawEvidenceError::Persistence(error.to_string()));
        }
        Ok(EvidenceAccessRef::durable(
            evidence_ref,
            artifact.sha256,
            artifact.bytes,
            artifact.media_type,
            artifact.selector,
            artifact.visibility_scope,
        ))
    }
}

#[async_trait]
impl RawEvidenceStore for SessionPortRawEvidenceStore {
    async fn persist(
        &self,
        write: RawEvidenceWrite,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        let artifact = self
            .artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: write.media_type.clone(),
                    visibility_scope: write.visibility_scope.clone(),
                    expected_bytes: Some(write.payload.len() as u64),
                    original_name: Some(format!("{}.raw", write.evidence_ref.id())),
                },
                write.payload.as_bytes(),
            )
            .await
            .map_err(|error| RawEvidenceError::Persistence(error.to_string()))?;
        self.publish_artifact_receipt(
            write.evidence_ref,
            write.session_id,
            artifact,
            write.metadata,
            true,
        )
        .await
    }

    async fn retrieve(&self, access: &EvidenceAccessRef) -> Result<Vec<u8>, RawEvidenceError> {
        let artifact = ArtifactRef::durable(
            access.retrieval_selector.clone(),
            access.sha256.clone(),
            access.bytes,
            access.media_type.clone(),
            access.visibility_scope.clone(),
        );
        self.artifacts
            .read(&artifact, &access.visibility_scope, None)
            .await
            .map_err(|error| RawEvidenceError::Retrieval(error.to_string()))
    }
}

pub struct RawEvidenceFacade<S> {
    store: S,
}

impl<S> RawEvidenceFacade<S>
where
    S: RawEvidenceStore,
{
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn persist(
        &self,
        write: RawEvidenceWrite,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        let expected_ref = write.evidence_ref.clone();
        let expected_hash = sha256(write.payload.as_bytes());
        let expected_bytes = write.payload.len() as u64;
        let expected_media_type = write.media_type.clone();
        let expected_visibility_scope = write.visibility_scope.clone();
        let access = self.store.persist(write).await?;
        validate_access(
            &access,
            &expected_ref,
            &expected_hash,
            expected_bytes,
            Some(&expected_media_type),
            Some(&expected_visibility_scope),
        )?;
        Ok(access)
    }

    pub async fn retrieve(
        &self,
        access: &EvidenceAccessRef,
    ) -> Result<RawEvidenceRead, RawEvidenceError> {
        if !access.is_durable() {
            return Err(RawEvidenceError::Retrieval(
                "evidence does not have a durable receipt".to_string(),
            ));
        }
        let payload = self.store.retrieve(access).await?;
        validate_access(
            access,
            &access.evidence_ref,
            &sha256(&payload),
            payload.len() as u64,
            None,
            None,
        )?;
        Ok(RawEvidenceRead {
            access: access.clone(),
            payload,
        })
    }
}

fn validate_access(
    access: &EvidenceAccessRef,
    expected_ref: &EvidenceRef,
    expected_hash: &str,
    expected_bytes: u64,
    expected_media_type: Option<&str>,
    expected_visibility_scope: Option<&str>,
) -> Result<(), RawEvidenceError> {
    if access.durability != EvidenceDurability::Durable {
        return Err(RawEvidenceError::InvalidReceipt(
            "store did not confirm durable persistence".to_string(),
        ));
    }
    if &access.evidence_ref != expected_ref {
        return Err(RawEvidenceError::InvalidReceipt(
            "canonical evidence reference changed during persistence".to_string(),
        ));
    }
    if access.sha256 != expected_hash || access.bytes != expected_bytes {
        return Err(RawEvidenceError::InvalidReceipt(
            "payload hash or byte count does not match the durable receipt".to_string(),
        ));
    }
    if expected_media_type.is_some_and(|expected| access.media_type != expected)
        || expected_visibility_scope.is_some_and(|expected| access.visibility_scope != expected)
    {
        return Err(RawEvidenceError::InvalidReceipt(
            "media type or visibility scope changed during persistence".to_string(),
        ));
    }
    if access.retrieval_selector.trim().is_empty() {
        return Err(RawEvidenceError::InvalidReceipt(
            "durable receipt has no retrieval selector".to_string(),
        ));
    }
    Ok(())
}

fn sha256(payload: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoStore {
        corrupt_receipt: bool,
    }

    #[async_trait]
    impl RawEvidenceStore for EchoStore {
        async fn persist(
            &self,
            write: RawEvidenceWrite,
        ) -> Result<EvidenceAccessRef, RawEvidenceError> {
            let hash = if self.corrupt_receipt {
                "sha256:wrong".to_string()
            } else {
                sha256(write.payload.as_bytes())
            };
            Ok(EvidenceAccessRef::durable(
                write.evidence_ref,
                hash,
                write.payload.len() as u64,
                write.media_type,
                "artifact://art_echo",
                write.visibility_scope,
            ))
        }

        async fn retrieve(&self, _access: &EvidenceAccessRef) -> Result<Vec<u8>, RawEvidenceError> {
            Ok(b"full raw output".to_vec())
        }
    }

    fn write() -> RawEvidenceWrite {
        RawEvidenceWrite {
            evidence_ref: EvidenceRef::new("tool", "raw-1"),
            session_id: "s1".to_string(),
            media_type: "text/plain".to_string(),
            visibility_scope: "session:s1".to_string(),
            payload: "full raw output".to_string(),
            metadata: Value::Null,
        }
    }

    #[tokio::test]
    async fn persistence_returns_only_verified_durable_access() {
        let facade = RawEvidenceFacade::new(EchoStore {
            corrupt_receipt: false,
        });
        let access = facade.persist(write()).await.unwrap();
        assert!(access.is_durable());
        let read = facade.retrieve(&access).await.unwrap();
        assert_eq!(read.payload, b"full raw output");
    }

    #[tokio::test]
    async fn corrupt_persistence_receipt_is_rejected() {
        let facade = RawEvidenceFacade::new(EchoStore {
            corrupt_receipt: true,
        });
        assert!(matches!(
            facade.persist(write()).await,
            Err(RawEvidenceError::InvalidReceipt(_))
        ));
    }

    #[tokio::test]
    async fn session_store_adapter_roundtrips_verified_raw_payload() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&session::SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "raw-evidence".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                tempfile::tempdir().unwrap().keep(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        );
        let facade = RawEvidenceFacade::new(SessionPortRawEvidenceStore::new(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
            artifacts,
        ));
        let access = facade.persist(write()).await.expect("durable write");
        assert!(access.is_durable());
        assert!(access.retrieval_selector.starts_with("artifact://"));
        let read = facade.retrieve(&access).await.expect("durable read");
        assert_eq!(read.payload, b"full raw output");
    }

    #[tokio::test]
    async fn session_scoped_raw_evidence_cannot_be_read_through_a_sibling_scope() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&session::SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "raw-evidence-isolation".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                tempfile::tempdir().unwrap().keep(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        );
        let facade = RawEvidenceFacade::new(SessionPortRawEvidenceStore::new(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
            artifacts,
        ));
        let access = facade.persist(write()).await.expect("durable write");
        let mut sibling_access = access;
        sibling_access.visibility_scope = "session:sibling".to_string();

        assert!(matches!(
            facade.retrieve(&sibling_access).await,
            Err(RawEvidenceError::Retrieval(_))
        ));
    }

    #[tokio::test]
    async fn native_staged_artifact_publishes_receipt_without_rematerializing_payload() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&session::SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "staged-evidence".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                tempfile::tempdir().unwrap().keep(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        );
        let artifact = artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "text/plain".to_string(),
                    visibility_scope: "session:s1".to_string(),
                    expected_bytes: Some(18),
                    original_name: Some("native-tool.raw".to_string()),
                },
                b"native staged body",
            )
            .await
            .unwrap();
        let adapter = SessionPortRawEvidenceStore::new(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
            Arc::clone(&artifacts),
        );
        let access = adapter
            .persist_artifact(
                EvidenceRef::new("tool", "native-staged"),
                "s1".to_string(),
                artifact.clone(),
                serde_json::json!({"native_staged_artifact": true}),
            )
            .await
            .expect("publish staged receipt");

        assert_eq!(access.retrieval_selector, artifact.selector);
        assert_eq!(access.sha256, artifact.sha256);
        assert_eq!(access.bytes, artifact.bytes);
        let page = store.timeline_events_page("s1", 0, 100).await.unwrap();
        let receipt = page
            .events
            .iter()
            .find(|event| event.kind == "evidence.raw.persisted")
            .expect("bounded Session receipt");
        assert!(receipt.payload.get("payload").is_none());
        assert_eq!(
            receipt
                .payload
                .get("native_staged_artifact")
                .and_then(Value::as_bool),
            Some(true)
        );
    }
}
