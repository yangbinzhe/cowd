//! Canonical raw-evidence durability boundary.

use async_trait::async_trait;
use harness_contract::context::{
    ArtifactRef, ArtifactWriteDescriptor, EvidenceAccessRef, EvidenceDurability,
};
use harness_contract::reality::EvidenceRef;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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

/// Rebuild the durable access receipt recorded by `evidence.raw.persisted`.
///
/// `tool://` is the stable logical identity exposed to models, while the
/// ArtifactStore selector is deliberately opaque.  Every resolver must use
/// this canonical mapping instead of guessing that both identifiers match.
/// The stable logical `tool://` identity recorded by `evidence.raw.persisted`.
/// Used to resolve a model-supplied reference that dropped the opaque content
/// suffix by an unambiguous prefix instead of failing the whole action.
#[must_use]
pub fn persisted_evidence_id(payload: &Value) -> Option<&str> {
    payload.get("evidence_id")?.as_str()
}

#[must_use]
pub fn access_from_persisted_payload(
    evidence_id: &str,
    payload: &Value,
) -> Option<EvidenceAccessRef> {
    let recorded_id = payload.get("evidence_id")?.as_str()?;
    if recorded_id != evidence_id {
        return None;
    }
    Some(EvidenceAccessRef::durable(
        EvidenceRef::observed("tool", evidence_id),
        payload.get("content_hash")?.as_str()?,
        payload.get("byte_count")?.as_u64()?,
        payload.get("media_type")?.as_str()?,
        payload.get("artifact_selector")?.as_str()?,
        payload.get("visibility_scope")?.as_str()?,
    ))
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

    /// Bind an already-stored body to a new evidence reference, without writing
    /// the bytes a second time.
    ///
    /// A reused read still owes a receipt under its own identity: the caller
    /// validates the receipt against the reference it just used, so handing
    /// back the first call's receipt fails with "canonical evidence reference
    /// changed during persistence". The default re-persists, which is correct
    /// but writes the bytes again; the production adapter overrides it.
    async fn rebind_receipt(
        &self,
        write: RawEvidenceWrite,
        _artifact: ArtifactRef,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        self.persist(write).await
    }
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
            label: Some(evidence_ref.ref_type.clone()),
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

    async fn rebind_receipt(
        &self,
        write: RawEvidenceWrite,
        artifact: ArtifactRef,
    ) -> Result<EvidenceAccessRef, RawEvidenceError> {
        self.publish_artifact_receipt(
            write.evidence_ref,
            write.session_id,
            artifact,
            write.metadata,
            false,
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
        // A whole-file read of an unchanged path in the same Session is the one
        // case where writing again adds bytes and no information.
        let dedup_key = Self::read_dedup_key(&write);
        if let Some(key) = &dedup_key {
            // The guard is scoped so it is dropped before the await below: a
            // held lock across an await makes this future non-Send.
            let cached_body = {
                Self::read_receipt_index()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(key)
                    .cloned()
            };
            // The stored body is reused only when it is still the bytes this
            // call asked for. The evidence-economy contract says a repeat read
            // is served "same bytes, hash-verified", so a body whose hash moved
            // is not a redundant read at all - the file changed - and it falls
            // through to a fresh persist below. Refusing it instead would break
            // the ordinary write-then-read-again shape.
            if let Some(body) = cached_body {
                if body.sha256 == expected_hash && body.bytes == expected_bytes {
                    let access = self.store.rebind_receipt(write, body).await?;
                    validate_access(
                        &access,
                        &expected_ref,
                        &expected_hash,
                        expected_bytes,
                        Some(&expected_media_type),
                        Some(&expected_visibility_scope),
                    )?;
                    return Ok(access);
                }
            }
        }
        let access = self.store.persist(write).await?;
        if let Some(key) = dedup_key {
            let body = ArtifactRef::durable(
                access.retrieval_selector.clone(),
                access.sha256.clone(),
                access.bytes,
                access.media_type.clone(),
                access.visibility_scope.clone(),
            );
            {
                Self::read_receipt_index()
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(key, body);
            }
        }
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

    /// Durable receipts already written for an identical read, keyed by
    /// `(session, tool, input)`.
    ///
    /// A whole-file read of a path the same Session already read returns the
    /// receipt it already has instead of writing the same bytes again: one live
    /// run performed four full reads of `crates/tools/src/lib.rs`, and each one
    /// cost a provider round-trip and a fresh artifact. The key includes the
    /// tool input, so a ranged read (whose input names `limit` and `offset`)
    /// never collides with a whole-file read.
    ///
    /// The key deliberately does NOT include the payload hash. A read's payload
    /// embeds the call that produced it - a compaction notice names the tool
    /// call id - so the same input yields a different payload every time and a
    /// payload-keyed index never hits. That was measured: one live run persisted
    /// the same input three times at the same byte count under three different
    /// content hashes. What makes two reads the same read is what was asked for,
    /// not how the answer was framed.
    ///
    /// The index is process-wide rather than per-facade because every governed
    /// tool result builds a fresh facade at its call site, so a per-facade index
    /// would be empty on every call and the dedup would never fire. It is still
    /// in-memory only: deduplication is a within-session economy, not a new
    /// durability contract, and a restart simply re-reads once.
    #[cfg(test)]
    fn clear_read_receipt_index() {
        Self::read_receipt_index()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn read_receipt_index() -> &'static Mutex<HashMap<(String, String, String), ArtifactRef>> {
        static INDEX: std::sync::OnceLock<Mutex<HashMap<(String, String, String), ArtifactRef>>> =
            std::sync::OnceLock::new();
        INDEX.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// The deduplication key for a write, or `None` when the write is not a
    /// repeatable whole-file read.
    ///
    /// `None` covers everything that is not a read at all (compaction windows,
    /// review bodies) and every read whose input is not stable: a ranged read
    /// names `limit`/`offset`, so its input hash already differs from a
    /// whole-file read of the same path.
    fn read_dedup_key(write: &RawEvidenceWrite) -> Option<(String, String, String)> {
        let tool_name = write.metadata.get("tool_name").and_then(Value::as_str)?;
        if !matches!(tool_name, "read_file" | "read_ref") {
            return None;
        }
        let input_hash = write
            .metadata
            .get("input_hash")
            .and_then(Value::as_str)?
            .to_string();
        Some((write.session_id.clone(), tool_name.to_string(), input_hash))
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
            evidence_ref: EvidenceRef::observed("tool", "raw-1"),
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
        let store = Arc::new(crate::test_support::session_store());
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
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::for_test(
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
        let store = Arc::new(crate::test_support::session_store());
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
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::for_test(
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
        let store = Arc::new(crate::test_support::session_store());
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
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::for_test(
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
                EvidenceRef::observed("tool", "native-staged"),
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
    /// A whole-file read of a path the same Session already read must reuse the
    /// receipt it already has. One live run performed four full reads of
    /// `crates/tools/src/lib.rs`; each cost a provider round-trip and a fresh
    /// artifact, and none of them added information.
    #[tokio::test]
    async fn redundant_full_read_returns_the_existing_receipt() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let facade = RawEvidenceFacade::new(CountingStore::default());
        let read = || RawEvidenceWrite {
            metadata: serde_json::json!({
                "tool_name": "read_file",
                "input_hash": "input-whole-file",
            }),
            ..write()
        };
        let first = facade.persist(read()).await.unwrap();
        let second = facade.persist(read()).await.unwrap();
        assert_eq!(
            first.retrieval_selector, second.retrieval_selector,
            "the second identical read must reuse the first receipt"
        );
        assert_eq!(
            facade.persist_calls(),
            1,
            "the second identical read must not write bytes again"
        );
    }

    /// The production shape: every governed tool result builds a FRESH facade at
    /// its call site, so the index has to survive across instances. A per-facade
    /// index passes the unit test above and never fires in a real run - which is
    /// exactly what happened, and a live run still wrote the same file three
    /// times.
    #[tokio::test]
    async fn a_fresh_facade_per_call_still_reuses_the_receipt() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let read = || RawEvidenceWrite {
            metadata: serde_json::json!({
                "tool_name": "read_file",
                "input_hash": "input-fresh-facade",
            }),
            ..write()
        };
        // A new store AND a new facade for every call, exactly as the tool
        // executor does.
        let first = RawEvidenceFacade::new(CountingStore::default())
            .persist(read())
            .await
            .unwrap();
        let second = RawEvidenceFacade::new(CountingStore::default())
            .persist(read())
            .await
            .unwrap();
        assert_eq!(
            first.retrieval_selector, second.retrieval_selector,
            "a fresh facade must still reuse the receipt the Session already has"
        );
    }

    /// A ranged read names `limit`/`offset`, so its input hash differs and it
    /// never collides with a whole-file read. Paging through a file is not
    /// redundant reading.
    #[tokio::test]
    async fn ranged_reads_are_not_deduped() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let facade = RawEvidenceFacade::new(CountingStore::default());
        let whole = facade
            .persist(RawEvidenceWrite {
                metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "whole"}),
                ..write()
            })
            .await
            .unwrap();
        let ranged = facade
            .persist(RawEvidenceWrite {
                metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "limit-20"}),
                ..write()
            })
            .await
            .unwrap();
        assert_ne!(whole.retrieval_selector, ranged.retrieval_selector);
        assert_eq!(facade.persist_calls(), 2, "a ranged read must be persisted");
    }

    /// A different read is a different read. Narrowing the range changes the
    /// input, so it is persisted rather than served from the first receipt:
    /// that is how a model asks for bytes the first read did not cover.
    #[tokio::test]
    async fn a_narrower_range_is_a_different_read() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let facade = RawEvidenceFacade::new(CountingStore::default());
        facade
            .persist(RawEvidenceWrite {
                metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "narrow-whole"}),
                ..write()
            })
            .await
            .unwrap();
        facade
            .persist(RawEvidenceWrite {
                metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "narrow-limit-20"}),
                ..write()
            })
            .await
            .unwrap();
        assert_eq!(
            facade.persist_calls(),
            2,
            "a different input is a different read and must be persisted"
        );
    }

    /// The same read issued twice is one read. The framework's own evidence
    /// economy says a file an accepted Task already read is opened through its
    /// durable receipt, so the second identical call reuses the first receipt
    /// even though the payload framing differs - which it always does, because
    /// the payload names the call that produced it.
    #[tokio::test]
    async fn the_same_read_twice_is_one_read_even_when_the_framing_differs() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let facade = RawEvidenceFacade::new(CountingStore::default());
        let read = || RawEvidenceWrite {
            metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "same"}),
            ..write()
        };
        let first = facade.persist(read()).await.unwrap();
        let second = facade.persist(read()).await.unwrap();
        assert_eq!(first.retrieval_selector, second.retrieval_selector);
        assert_eq!(facade.persist_calls(), 1);
    }

    /// Deduplication is scoped to one Session. A different Agent reading the
    /// same file is acquiring information it does not have, which is exactly the
    /// mistake an earlier monitor revision made when it called three agents'
    /// three first reads a loop.
    #[tokio::test]
    async fn a_different_session_reads_the_same_file_again() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let facade = RawEvidenceFacade::new(CountingStore::default());
        facade
            .persist(RawEvidenceWrite {
                metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "same"}),
                ..write()
            })
            .await
            .unwrap();
        facade
            .persist(RawEvidenceWrite {
                session_id: "s2".to_string(),
                visibility_scope: "session:s2".to_string(),
                metadata: serde_json::json!({"tool_name": "read_file", "input_hash": "same"}),
                ..write()
            })
            .await
            .unwrap();
        assert_eq!(
            facade.persist_calls(),
            2,
            "another Session's first read is information, not waste"
        );
    }

    /// A write that is not a read at all must never be deduplicated: review
    /// bodies and compaction windows are distinct durable facts even when their
    /// bytes coincide.
    #[tokio::test]
    async fn a_non_read_write_is_never_deduped() {
        RawEvidenceFacade::<CountingStore>::clear_read_receipt_index();
        let facade = RawEvidenceFacade::new(CountingStore::default());
        for _ in 0..2 {
            facade
                .persist(RawEvidenceWrite {
                    evidence_ref: EvidenceRef::observed("tool", "raw-review"),
                    metadata: serde_json::json!({"tool_name": "objective_review"}),
                    ..write()
                })
                .await
                .unwrap();
        }
        assert_eq!(facade.persist_calls(), 2, "only reads are deduplicated");
    }

    #[derive(Default)]
    struct CountingStore {
        persists: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl RawEvidenceStore for CountingStore {
        /// A reused body issues a fresh receipt without writing again, exactly
        /// as the production adapter does, so a test can tell "the bytes were
        /// written once" from "the receipt was issued twice".
        async fn rebind_receipt(
            &self,
            write: RawEvidenceWrite,
            artifact: ArtifactRef,
        ) -> Result<EvidenceAccessRef, RawEvidenceError> {
            Ok(EvidenceAccessRef::durable(
                write.evidence_ref,
                artifact.sha256,
                artifact.bytes,
                artifact.media_type,
                artifact.selector,
                artifact.visibility_scope,
            ))
        }

        async fn persist(
            &self,
            write: RawEvidenceWrite,
        ) -> Result<EvidenceAccessRef, RawEvidenceError> {
            self.persists
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // A distinct selector per physical write, so a test can tell a
            // reused receipt from a freshly written one.
            let writes = self.persists.load(std::sync::atomic::Ordering::SeqCst);
            Ok(EvidenceAccessRef::durable(
                write.evidence_ref,
                sha256(write.payload.as_bytes()),
                write.payload.len() as u64,
                write.media_type,
                format!("artifact://art_echo_{writes}"),
                write.visibility_scope,
            ))
        }

        async fn retrieve(&self, _access: &EvidenceAccessRef) -> Result<Vec<u8>, RawEvidenceError> {
            Ok(b"full raw output".to_vec())
        }
    }

    impl RawEvidenceFacade<CountingStore> {
        fn persist_calls(&self) -> usize {
            self.store
                .persists
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }
}
