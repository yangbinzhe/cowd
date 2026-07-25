//! Canonical raw-evidence durability boundary.

use async_trait::async_trait;
use harness_contract::context::{
    ArtifactRef, ArtifactWriteDescriptor, EvidenceAccessRef, EvidenceDurability,
};
use harness_contract::core::EvidenceRef;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
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
pub struct SessionStoreRawEvidenceStore {
    store: Arc<memory::UnifiedSessionStore>,
    artifacts: Arc<crate::ArtifactStore>,
}

impl SessionStoreRawEvidenceStore {
    #[must_use]
    pub fn new(
        store: Arc<memory::UnifiedSessionStore>,
        artifacts: Arc<crate::ArtifactStore>,
    ) -> Self {
        Self { store, artifacts }
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
        let mut event = memory::SessionDomainEvent::new(
            session_id.clone(),
            0,
            memory::SessionDomainScope::Tool,
            "evidence.raw.persisted",
            Value::Object(payload),
            created_at_ms,
        );
        event.refs.push(memory::SessionDomainRef {
            ref_type: "evidence".to_string(),
            id: evidence_ref.id().to_string(),
            label: Some(evidence_ref.0.ref_type.clone()),
        });
        if let Err(error) = self
            .store
            .append_session_domain_event_allocating_sequence(&event)
            .await
        {
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
impl RawEvidenceStore for SessionStoreRawEvidenceStore {
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawEvidenceMigrationOptions {
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub resume_after_session: Option<String>,
    #[serde(default = "default_raw_migration_session_limit")]
    pub session_limit: usize,
}

impl Default for RawEvidenceMigrationOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            resume_after_session: None,
            session_limit: default_raw_migration_session_limit(),
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RawEvidenceMigrationReport {
    pub dry_run: bool,
    pub sessions_scanned: usize,
    pub legacy_raw_events: usize,
    pub migrated_raw_events: usize,
    pub legacy_context_reports: usize,
    pub migrated_context_reports: usize,
    pub already_current: usize,
    pub failed: usize,
    pub next_session_cursor: Option<String>,
    pub complete: bool,
    pub failures: Vec<String>,
}

/// Converts legacy Session-inline raw payloads into the selected Artifact
/// plane and appends corrected context projections. Original Session events
/// remain immutable audit history but no read path depends on them afterward.
pub async fn migrate_legacy_raw_evidence(
    sessions: Arc<memory::UnifiedSessionStore>,
    artifacts: Arc<crate::ArtifactStore>,
    options: RawEvidenceMigrationOptions,
) -> RawEvidenceMigrationReport {
    let mut report = RawEvidenceMigrationReport {
        dry_run: options.dry_run,
        ..RawEvidenceMigrationReport::default()
    };
    let mut records = match sessions.list_sessions().await {
        Ok(records) => records,
        Err(error) => {
            report.failed = 1;
            report.failures.push(format!("list sessions: {error}"));
            return report;
        }
    };
    records.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    let mut remaining_after_limit = false;
    for record in records {
        if options
            .resume_after_session
            .as_deref()
            .is_some_and(|resume| record.session_id.as_str() <= resume)
        {
            continue;
        }
        if report.sessions_scanned >= options.session_limit.max(1) {
            remaining_after_limit = true;
            break;
        }
        report.sessions_scanned += 1;
        report.next_session_cursor = Some(record.session_id.clone());
        match migrate_session_raw_evidence(
            &sessions,
            &artifacts,
            &record.session_id,
            options.dry_run,
        )
        .await
        {
            Ok(session_report) => {
                report.legacy_raw_events += session_report.legacy_raw_events;
                report.migrated_raw_events += session_report.migrated_raw_events;
                report.legacy_context_reports += session_report.legacy_context_reports;
                report.migrated_context_reports += session_report.migrated_context_reports;
                report.already_current += session_report.already_current;
            }
            Err(error) => {
                report.failed += 1;
                report
                    .failures
                    .push(format!("{}: {error}", record.session_id));
            }
        }
    }
    report.complete = !remaining_after_limit && report.failed == 0;
    report
}

#[derive(Debug, Default)]
struct SessionRawMigrationReport {
    legacy_raw_events: usize,
    migrated_raw_events: usize,
    legacy_context_reports: usize,
    migrated_context_reports: usize,
    already_current: usize,
}

async fn migrate_session_raw_evidence(
    sessions: &memory::UnifiedSessionStore,
    artifacts: &crate::ArtifactStore,
    session_id: &str,
    dry_run: bool,
) -> Result<SessionRawMigrationReport, String> {
    let events = load_session_timeline(sessions, session_id).await?;
    let mut report = SessionRawMigrationReport::default();
    let mut access_by_evidence = HashMap::<String, EvidenceAccessRef>::new();
    let mut legacy_evidence_ids = HashSet::<String>::new();
    let mut corrected_context_sequences = HashSet::<usize>::new();

    for event in &events {
        if event.kind == "evidence.raw.persisted" {
            if let Some(access) = access_from_artifact_event(&event.payload) {
                access_by_evidence.insert(access.evidence_ref.id().to_string(), access);
                report.already_current += 1;
            }
        } else if event.kind == "context.turn_report" {
            let report_value = event.payload.get("report").unwrap_or(&event.payload);
            if let Some(sequence) = report_value
                .get("artifact_migration")
                .and_then(|value| value.get("source_sequence"))
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
            {
                corrected_context_sequences.insert(sequence);
            }
        }
    }

    for event in &events {
        if event.kind != "evidence.raw.persisted"
            || event.payload.get("artifact_selector").is_some()
        {
            continue;
        }
        let Some(raw) = event.payload.get("raw").and_then(serde_json::Value::as_str) else {
            continue;
        };
        report.legacy_raw_events += 1;
        let evidence_id = event
            .payload
            .get("evidence_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("legacy raw event {} has no evidence_id", event.sequence))?;
        let expected_hash = event
            .payload
            .get("content_hash")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| sha256(raw.as_bytes()));
        let expected_bytes = event
            .payload
            .get("byte_count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(raw.len() as u64);
        if expected_hash != sha256(raw.as_bytes()) || expected_bytes != raw.len() as u64 {
            return Err(format!(
                "legacy raw event {} failed hash/size verification",
                event.sequence
            ));
        }
        legacy_evidence_ids.insert(evidence_id.to_string());
        if access_by_evidence.contains_key(evidence_id) {
            report.already_current += 1;
            continue;
        }
        if dry_run {
            continue;
        }
        let media_type = event
            .payload
            .get("media_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("text/plain")
            .to_string();
        let visibility_scope = event
            .payload
            .get("visibility_scope")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("private")
            .to_string();
        let artifact = artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: media_type.clone(),
                    visibility_scope: visibility_scope.clone(),
                    expected_bytes: Some(expected_bytes),
                    original_name: Some(format!("{evidence_id}.raw")),
                },
                raw.as_bytes(),
            )
            .await
            .map_err(|error| format!("publish raw artifact: {error}"))?;
        let evidence_ref = EvidenceRef::new("tool", evidence_id);
        let pin_owner = format!("evidence:{session_id}:{evidence_id}");
        artifacts
            .pin(
                &artifact,
                &pin_owner,
                crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            )
            .map_err(|error| format!("pin migrated raw artifact: {error}"))?;
        let mut payload = event.payload.as_object().cloned().unwrap_or_else(Map::new);
        payload.remove("raw");
        payload.insert(
            "artifact_selector".to_string(),
            Value::String(artifact.selector.clone()),
        );
        payload.insert(
            "content_hash".to_string(),
            Value::String(artifact.sha256.clone()),
        );
        payload.insert("byte_count".to_string(), Value::from(artifact.bytes));
        payload.insert(
            "migrated_from_sequence".to_string(),
            Value::from(event.sequence as u64),
        );
        let mut migrated = memory::SessionDomainEvent::new(
            session_id,
            0,
            memory::SessionDomainScope::Tool,
            "evidence.raw.persisted",
            Value::Object(payload),
            now_ms(),
        );
        migrated.refs.push(memory::SessionDomainRef {
            ref_type: "evidence".to_string(),
            id: evidence_id.to_string(),
            label: Some(evidence_ref.0.ref_type.clone()),
        });
        if let Err(error) = sessions
            .append_session_domain_event_allocating_sequence(&migrated)
            .await
        {
            let _ = artifacts.unpin(&artifact, &pin_owner);
            let _ = artifacts.delete(&artifact, &visibility_scope);
            return Err(format!("append migrated raw receipt: {error}"));
        }
        access_by_evidence.insert(
            evidence_id.to_string(),
            EvidenceAccessRef::durable(
                evidence_ref,
                artifact.sha256,
                artifact.bytes,
                media_type,
                artifact.selector,
                visibility_scope,
            ),
        );
        report.migrated_raw_events += 1;
    }

    for event in &events {
        if event.kind != "context.turn_report"
            || corrected_context_sequences.contains(&event.sequence)
        {
            continue;
        }
        let mut payload = event.payload.clone();
        let report_value = if payload.get("report").is_some() {
            let Some(report) = payload.get_mut("report") else {
                continue;
            };
            report
        } else {
            &mut payload
        };
        let Some(projections) = report_value
            .get_mut("audit_projections")
            .and_then(serde_json::Value::as_array_mut)
        else {
            continue;
        };
        let mut changed = false;
        for projection in projections {
            let Some(access) = projection
                .get_mut("access")
                .and_then(serde_json::Value::as_object_mut)
            else {
                continue;
            };
            let is_legacy = access
                .get("retrieval_selector")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|selector| selector.starts_with("session-event://"));
            if !is_legacy {
                continue;
            }
            let evidence_id = projection
                .get("evidence_ref")
                .and_then(|reference| reference.get("0"))
                .and_then(|inner| inner.get("id"))
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    projection
                        .get("evidence_ref")
                        .and_then(|reference| reference.get("id"))
                        .and_then(serde_json::Value::as_str)
                })
                .ok_or_else(|| {
                    format!(
                        "legacy context report {} has an invalid evidence ref",
                        event.sequence
                    )
                })?;
            if dry_run && legacy_evidence_ids.contains(evidence_id) {
                changed = true;
                continue;
            }
            let durable = access_by_evidence.get(evidence_id).ok_or_else(|| {
                format!(
                    "legacy context report {} cannot resolve raw evidence {evidence_id}",
                    event.sequence
                )
            })?;
            let Some(access_slot) = projection.get_mut("access") else {
                continue;
            };
            *access_slot = serde_json::to_value(durable)
                .map_err(|error| format!("encode migrated evidence access: {error}"))?;
            changed = true;
        }
        if !changed {
            continue;
        }
        report.legacy_context_reports += 1;
        if dry_run {
            continue;
        }
        report_value["artifact_migration"] = serde_json::json!({
            "source_sequence": event.sequence,
            "migrated_at_ms": now_ms(),
        });
        sessions
            .append_session_domain_event_allocating_sequence(&memory::SessionDomainEvent::new(
                session_id,
                0,
                memory::SessionDomainScope::Context,
                "context.turn_report",
                payload,
                now_ms(),
            ))
            .await
            .map_err(|error| format!("append migrated context projection: {error}"))?;
        report.migrated_context_reports += 1;
    }
    Ok(report)
}

async fn load_session_timeline(
    sessions: &memory::UnifiedSessionStore,
    session_id: &str,
) -> Result<Vec<memory::SessionDomainEvent>, String> {
    let mut events = Vec::new();
    let mut cursor = 0_usize;
    loop {
        let page = sessions
            .timeline_events_page(session_id, cursor, 1_000)
            .await
            .map_err(|error| format!("load session timeline: {error}"))?;
        let next = page.next_seq;
        events.extend(page.events);
        if !page.has_more {
            break;
        }
        cursor = next.ok_or_else(|| "timeline page has no resume cursor".to_string())?;
    }
    Ok(events)
}

fn access_from_artifact_event(payload: &Value) -> Option<EvidenceAccessRef> {
    let evidence_id = payload.get("evidence_id")?.as_str()?;
    let selector = payload.get("artifact_selector")?.as_str()?;
    let hash = payload.get("content_hash")?.as_str()?;
    let bytes = payload.get("byte_count")?.as_u64()?;
    let media_type = payload.get("media_type")?.as_str()?;
    let scope = payload.get("visibility_scope")?.as_str()?;
    Some(EvidenceAccessRef::durable(
        EvidenceRef::new("tool", evidence_id),
        hash,
        bytes,
        media_type,
        selector,
        scope,
    ))
}

const fn default_raw_migration_session_limit() -> usize {
    10_000
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
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
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                tempfile::tempdir().unwrap().keep(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        );
        let facade = RawEvidenceFacade::new(SessionStoreRawEvidenceStore::new(store, artifacts));
        let access = facade.persist(write()).await.expect("durable write");
        assert!(access.is_durable());
        assert!(access.retrieval_selector.starts_with("artifact://"));
        let read = facade.retrieve(&access).await.expect("durable read");
        assert_eq!(read.payload, b"full raw output");
    }

    #[tokio::test]
    async fn session_scoped_raw_evidence_cannot_be_read_through_a_sibling_scope() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&memory::SessionRecord {
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
        let facade = RawEvidenceFacade::new(SessionStoreRawEvidenceStore::new(store, artifacts));
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
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&memory::SessionRecord {
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
        let adapter = SessionStoreRawEvidenceStore::new(Arc::clone(&store), Arc::clone(&artifacts));
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

    #[tokio::test]
    async fn legacy_inline_raw_and_context_projection_migrate_once() {
        use harness_contract::context::{EvidenceAuditProjection, EvidenceContentKind};

        let sessions = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        sessions
            .create_session(&memory::SessionRecord {
                session_id: "legacy-session".to_string(),
                platform: "test".to_string(),
                chat_id: "legacy-raw".to_string(),
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
        let raw = "legacy complete raw output";
        let hash = sha256(raw.as_bytes());
        let raw_event = sessions
            .append_session_domain_event_allocating_sequence(&memory::SessionDomainEvent::new(
                "legacy-session",
                0,
                memory::SessionDomainScope::Tool,
                "evidence.raw.persisted",
                serde_json::json!({
                    "type": "RawEvidence",
                    "evidence_id": "legacy-evidence",
                    "raw": raw,
                    "content_hash": hash,
                    "byte_count": raw.len(),
                    "media_type": "text/plain",
                    "visibility_scope": "session:legacy-session"
                }),
                1,
            ))
            .await
            .unwrap();
        let evidence_ref = EvidenceRef::new("tool", "legacy-evidence");
        let projection = EvidenceAuditProjection {
            evidence_ref: evidence_ref.clone(),
            content_kind: EvidenceContentKind::Text,
            raw_tokens: 8,
            receipt_tokens: 2,
            omitted_tokens: 6,
            raw_available: true,
            access: Some(EvidenceAccessRef::durable(
                evidence_ref,
                hash,
                raw.len() as u64,
                "text/plain",
                format!(
                    "{}legacy-session/{}",
                    ["session", "-event://"].concat(),
                    raw_event.sequence
                ),
                "session:legacy-session",
            )),
        };
        sessions
            .append_session_domain_event_allocating_sequence(&memory::SessionDomainEvent::new(
                "legacy-session",
                0,
                memory::SessionDomainScope::Context,
                "context.turn_report",
                serde_json::json!({
                    "type": "ContextTurnReport",
                    "report": {
                        "turn_id": "legacy-turn",
                        "audit_projections": [projection]
                    }
                }),
                2,
            ))
            .await
            .unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                tempfile::tempdir().unwrap().keep(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        );

        let dry_run = migrate_legacy_raw_evidence(
            Arc::clone(&sessions),
            Arc::clone(&artifacts),
            RawEvidenceMigrationOptions {
                dry_run: true,
                ..RawEvidenceMigrationOptions::default()
            },
        )
        .await;
        assert!(dry_run.complete, "{:?}", dry_run.failures);
        assert_eq!(dry_run.legacy_raw_events, 1);
        assert_eq!(dry_run.legacy_context_reports, 1);
        assert_eq!(artifacts.stats().unwrap().artifacts, 0);

        let applied = migrate_legacy_raw_evidence(
            Arc::clone(&sessions),
            Arc::clone(&artifacts),
            RawEvidenceMigrationOptions::default(),
        )
        .await;
        assert!(applied.complete, "{:?}", applied.failures);
        assert_eq!(applied.migrated_raw_events, 1);
        assert_eq!(applied.migrated_context_reports, 1);
        let events = load_session_timeline(&sessions, "legacy-session")
            .await
            .unwrap();
        let corrected = events
            .iter()
            .rev()
            .find(|event| {
                event.kind == "context.turn_report"
                    && event.payload["report"]["artifact_migration"].is_object()
            })
            .expect("corrected context report");
        let access: EvidenceAccessRef = serde_json::from_value(
            corrected.payload["report"]["audit_projections"][0]["access"].clone(),
        )
        .unwrap();
        assert!(access.retrieval_selector.starts_with("artifact://"));
        assert_eq!(
            artifacts
                .read(
                    &ArtifactRef::durable(
                        access.retrieval_selector,
                        access.sha256,
                        access.bytes,
                        access.media_type,
                        access.visibility_scope.clone(),
                    ),
                    &access.visibility_scope,
                    None,
                )
                .await
                .unwrap(),
            raw.as_bytes()
        );

        let repeated = migrate_legacy_raw_evidence(
            Arc::clone(&sessions),
            artifacts,
            RawEvidenceMigrationOptions::default(),
        )
        .await;
        assert!(repeated.complete);
        assert_eq!(repeated.migrated_raw_events, 0);
        assert_eq!(repeated.migrated_context_reports, 0);
    }
}
