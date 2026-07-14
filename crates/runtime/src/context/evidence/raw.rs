//! Canonical raw-evidence durability boundary.

use async_trait::async_trait;
use harness_contract::context::{EvidenceAccessRef, EvidenceDurability};
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
    pub payload: Vec<u8>,
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

/// Session-domain adapter used by the Runtime production path. Session events
/// remain the only durable carrier; the facade owns receipt construction and
/// validation so no caller can publish a dangling raw reference.
#[derive(Clone)]
pub struct SessionStoreRawEvidenceStore {
    store: Arc<memory::UnifiedSessionStore>,
}

impl SessionStoreRawEvidenceStore {
    #[must_use]
    pub fn new(store: Arc<memory::UnifiedSessionStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl RawEvidenceStore for SessionStoreRawEvidenceStore {
    async fn persist(
        &self,
        write: RawEvidenceWrite,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        let mut payload = write.metadata.as_object().cloned().unwrap_or_else(Map::new);
        payload.insert("type".to_string(), Value::String("RawEvidence".to_string()));
        payload.insert(
            "evidence_id".to_string(),
            Value::String(write.evidence_ref.id().to_string()),
        );
        payload.insert(
            "session_id".to_string(),
            Value::String(write.session_id.clone()),
        );
        payload.insert(
            "media_type".to_string(),
            Value::String(write.media_type.clone()),
        );
        payload.insert(
            "visibility_scope".to_string(),
            Value::String(write.visibility_scope.clone()),
        );
        payload.insert(
            "byte_count".to_string(),
            Value::from(write.payload.len() as u64),
        );
        payload.insert(
            "content_hash".to_string(),
            Value::String(sha256(&write.payload)),
        );
        payload.insert(
            "raw".to_string(),
            Value::String(String::from_utf8_lossy(&write.payload).into_owned()),
        );
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or_default();
        let mut event = memory::SessionDomainEvent::new(
            write.session_id.clone(),
            0,
            memory::SessionDomainScope::Tool,
            "evidence.raw.persisted",
            Value::Object(payload),
            created_at_ms,
        );
        event.refs.push(memory::SessionDomainRef {
            ref_type: "evidence".to_string(),
            id: write.evidence_ref.id().to_string(),
            label: Some(write.evidence_ref.0.ref_type.clone()),
        });
        let persisted = self
            .store
            .append_session_domain_event_allocating_sequence(&event)
            .await
            .map_err(|error| RawEvidenceError::Persistence(error.to_string()))?;
        Ok(EvidenceAccessRef::durable(
            write.evidence_ref,
            sha256(&write.payload),
            write.payload.len() as u64,
            write.media_type,
            format!(
                "session-event://{}/{}",
                write.session_id, persisted.sequence
            ),
            write.visibility_scope,
        ))
    }

    async fn retrieve(&self, access: &EvidenceAccessRef) -> Result<Vec<u8>, RawEvidenceError> {
        let selector = access
            .retrieval_selector
            .strip_prefix("session-event://")
            .ok_or_else(|| {
                RawEvidenceError::Retrieval("unsupported evidence selector".to_string())
            })?;
        let (session_id, sequence) = selector
            .rsplit_once('/')
            .ok_or_else(|| RawEvidenceError::Retrieval("invalid evidence selector".to_string()))?;
        if access.visibility_scope != format!("session:{session_id}") {
            return Err(RawEvidenceError::Retrieval(
                "evidence selector and visibility scope disagree".to_string(),
            ));
        }
        let sequence = sequence
            .parse::<usize>()
            .map_err(|_| RawEvidenceError::Retrieval("invalid evidence sequence".to_string()))?;
        let event = self
            .store
            .get_events(session_id, sequence)
            .await
            .map_err(|error| RawEvidenceError::Retrieval(error.to_string()))?
            .into_iter()
            .find(|event| event.sequence == sequence)
            .ok_or_else(|| RawEvidenceError::Retrieval("evidence event is missing".to_string()))?;
        let event = memory::SessionDomainEvent::from_session_event(&event)
            .map_err(|error| RawEvidenceError::Retrieval(error.to_string()))?;
        let raw = event
            .payload
            .get("raw")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                RawEvidenceError::Retrieval("evidence payload has no raw content".to_string())
            })?;
        if event.payload.get("evidence_id").and_then(Value::as_str)
            != Some(access.evidence_ref.id())
            || event
                .payload
                .get("visibility_scope")
                .and_then(Value::as_str)
                != Some(access.visibility_scope.as_str())
        {
            return Err(RawEvidenceError::Retrieval(
                "evidence selector does not resolve the referenced durable payload".to_string(),
            ));
        }
        Ok(raw.as_bytes().to_vec())
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
        let expected_hash = sha256(&write.payload);
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
                sha256(&write.payload)
            };
            Ok(EvidenceAccessRef::durable(
                write.evidence_ref,
                hash,
                write.payload.len() as u64,
                write.media_type,
                "session-event://s1/1",
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
            payload: b"full raw output".to_vec(),
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
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&memory::SessionRecord {
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
        let facade = RawEvidenceFacade::new(SessionStoreRawEvidenceStore::new(store));
        let access = facade.persist(write()).await.expect("durable write");
        assert!(access.is_durable());
        assert!(access.retrieval_selector.starts_with("session-event://s1/"));
        let read = facade.retrieve(&access).await.expect("durable read");
        assert_eq!(read.payload, b"full raw output");
    }
}
