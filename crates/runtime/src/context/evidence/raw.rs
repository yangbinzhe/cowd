//! Canonical raw-evidence durability boundary.

use async_trait::async_trait;
use harness_contract::context::{EvidenceAccessRef, EvidenceDurability};
use harness_contract::core::EvidenceRef;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEvidenceWrite {
    pub evidence_ref: EvidenceRef,
    pub session_id: String,
    pub media_type: String,
    pub visibility_scope: String,
    pub payload: Vec<u8>,
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
}
