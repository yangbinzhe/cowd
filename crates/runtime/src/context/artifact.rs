//! Unified, backend-neutral artifact durability plane.
//!
//! Resource attachments, raw tool output, and evidence receipts share this
//! physical object layer. Domain metadata remains owned by its respective
//! facade; callers never receive a host path or adapter-specific key.

use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use harness_contract::context::{ArtifactRef, ArtifactWriteDescriptor};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[path = "artifact_catalog.rs"]
mod catalog;
pub use catalog::{
    ArtifactCatalogPage, ArtifactCatalogQuery, ArtifactCatalogSnapshot, ArtifactDirectoryPage,
};

const ARTIFACT_SELECTOR_PREFIX: &str = "artifact://";
pub const ARTIFACT_PREPARED_PIN_OWNER: &str = "cowd:private-prepared:v1";
pub const ARTIFACT_PERMANENT_PIN_UNTIL_MS: u64 = i64::MAX as u64;
pub const ARTIFACT_STAGING_PIN_TTL_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactStoreConfig {
    pub compact_threshold_bytes: u64,
    pub max_object_bytes: u64,
    pub total_quota_bytes: u64,
    pub gc_high_water_bytes: u64,
    pub gc_low_water_bytes: u64,
    pub orphan_grace_ms: u64,
}

impl Default for ArtifactStoreConfig {
    fn default() -> Self {
        Self {
            compact_threshold_bytes: 256 * 1024,
            max_object_bytes: 512 * 1024 * 1024,
            total_quota_bytes: 20 * 1024 * 1024 * 1024,
            gc_high_water_bytes: 18 * 1024 * 1024 * 1024,
            gc_low_water_bytes: 16 * 1024 * 1024 * 1024,
            orphan_grace_ms: 24 * 60 * 60 * 1_000,
        }
    }
}

impl ArtifactStoreConfig {
    pub fn validate(self) -> Result<Self, ArtifactError> {
        if self.compact_threshold_bytes == 0
            || self.max_object_bytes < self.compact_threshold_bytes
            || self.total_quota_bytes < self.max_object_bytes
            || self.gc_low_water_bytes > self.gc_high_water_bytes
            || self.gc_high_water_bytes > self.total_quota_bytes
        {
            return Err(ArtifactError::InvalidConfig);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtifactObjectTier {
    Compact,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "origin_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparedArtifactOrigin {
    EventBody {
        stream_id: String,
        idempotency_key: String,
        kind: String,
    },
    ToolEffectClosure {
        stream_id: String,
        idempotency_key: String,
    },
    CheckpointBody {
        projection_id: String,
        source_cursor: u64,
    },
    TerminalBody {
        terminal_id: String,
        session_id: String,
        execution_id: String,
        turn_id: String,
        event_stream: String,
        event_key: String,
    },
}

impl PreparedArtifactOrigin {
    pub fn validate(&self) -> Result<(), String> {
        let fields: Vec<&str> = match self {
            Self::EventBody {
                stream_id,
                idempotency_key,
                kind,
            } => vec![stream_id, idempotency_key, kind],
            Self::ToolEffectClosure {
                stream_id,
                idempotency_key,
            } => {
                // The closure of a process-class effect is published for one
                // exact receipt of its own key family: the mutation receipt or
                // the read receipt. Both are committed by this service, and
                // the origin still binds the artifact to that exact receipt.
                if !stream_id.starts_with("execution-effect:")
                    || stream_id
                        .strip_prefix("execution-effect:")
                        .is_none_or(|key| {
                            key.is_empty()
                                || (idempotency_key != &format!("{key}:receipt")
                                    && idempotency_key != &format!("{key}:read-receipt"))
                        })
                {
                    return Err("invalid process closure primary identity".into());
                }
                vec![stream_id, idempotency_key]
            }
            Self::CheckpointBody {
                projection_id,
                source_cursor,
            } => {
                i64::try_from(*source_cursor).map_err(|_| "prepared cursor overflow")?;
                if !projection_id.starts_with("goal-projection:")
                    && crate::RuntimeEventStore::program_read_model_checkpoint_schema(projection_id)
                        .is_none()
                    && !projection_id.starts_with("projection-history:")
                {
                    return Err("unsupported prepared checkpoint owner".into());
                }
                vec![projection_id]
            }
            Self::TerminalBody {
                terminal_id,
                session_id,
                execution_id,
                turn_id,
                event_stream,
                event_key,
            } => vec![
                terminal_id,
                session_id,
                execution_id,
                turn_id,
                event_stream,
                event_key,
            ],
        };
        if fields.iter().any(|field| {
            field.trim().is_empty() || field.len() > 1024 || field.chars().any(char::is_control)
        }) {
            return Err("invalid bounded prepared origin".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRecordClass {
    Live,
    PrivatePrepared,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReservation {
    pub token: String,
    pub sha256: String,
    pub generation: String,
    pub publication_id: String,
    pub state: ArtifactReservationState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactReservationState {
    Active,
    Consumed,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedArtifactCandidate {
    pub record: ArtifactRecord,
    pub generation: String,
}

#[derive(Debug, Clone, Default)]
pub struct PreparedArtifactPage {
    pub candidates: Vec<PreparedArtifactCandidate>,
    pub last_examined_id: Option<String>,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedArtifactRetirement {
    Removed,
    Missing,
    Retained,
}

pub(crate) struct VerifiedProcessClosureRead {
    pub(crate) body: Vec<u8>,
    pub(crate) metadata_bytes: usize,
    pub(crate) reads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
    pub record_class: ArtifactRecordClass,
    pub prepared_origin: Option<PreparedArtifactOrigin>,
    pub artifact_id: String,
    pub sha256: String,
    pub bytes: u64,
    pub media_type: String,
    pub visibility_scope: String,
    pub tier: ArtifactObjectTier,
    pub created_at_ms: u64,
    pub last_access_at_ms: u64,
}

impl ArtifactRecord {
    /// The physical content identity comes only from the metadata owner.
    #[must_use]
    pub fn content_reference(&self) -> ArtifactRef {
        ArtifactRef {
            selector: format!("{ARTIFACT_SELECTOR_PREFIX}{}", self.artifact_id),
            sha256: self.sha256.clone(),
            bytes: self.bytes,
            media_type: self.media_type.clone(),
            durability: harness_contract::context::EvidenceDurability::Durable,
            visibility_scope: self.visibility_scope.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactObjectRecord {
    pub generation: String,
    pub sha256: String,
    pub bytes: u64,
    pub tier: ArtifactObjectTier,
    pub compact_body: Option<Vec<u8>>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactStoreStats {
    pub objects: u64,
    pub artifacts: u64,
    pub physical_bytes: u64,
    pub compact_bytes: u64,
    pub blob_bytes: u64,
    pub pins: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArtifactGcReport {
    pub examined: u64,
    pub removed_objects: u64,
    pub reclaimed_bytes: u64,
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact configuration is invalid")]
    InvalidConfig,
    #[error("artifact write exceeds the configured object limit")]
    ObjectTooLarge,
    #[error("artifact storage quota is exhausted")]
    QuotaExceeded,
    #[error("artifact selector is invalid")]
    InvalidSelector,
    #[error("artifact does not exist")]
    NotFound,
    #[error("artifact visibility scope is not authorized")]
    Unauthorized,
    #[error("artifact writer has already finished or aborted")]
    WriterClosed,
    #[error("artifact I/O failed: {0}")]
    Io(String),
    #[error("artifact metadata failed: {0}")]
    Metadata(String),
    #[error("artifact blocking task failed: {0}")]
    Blocking(String),
}

impl From<std::io::Error> for ArtifactError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

fn retain_primary_error(
    primary: ArtifactError,
    phase: &str,
    secondary: impl std::fmt::Display,
) -> ArtifactError {
    tracing::error!(phase, primary = %primary, secondary = %secondary, "artifact compensation unconfirmed");
    match primary {
        ArtifactError::Metadata(message) => {
            ArtifactError::Metadata(format!("{message}; {phase}: {secondary}"))
        }
        ArtifactError::Io(message) => ArtifactError::Io(format!("{message}; {phase}: {secondary}")),
        ArtifactError::Blocking(message) => {
            ArtifactError::Blocking(format!("{message}; {phase}: {secondary}"))
        }
        original => original,
    }
}

struct StagingOwner {
    path: PathBuf,
    armed: bool,
}

impl StagingOwner {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn cleanup(&mut self) -> Result<(), ArtifactError> {
        if !self.armed {
            return Ok(());
        }
        match fs::remove_file(&self.path) {
            Ok(()) => self.armed = false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => self.armed = false,
            Err(error) => {
                tracing::error!(phase = "staging_unlink", path = %self.path.display(), cleanup_error = %error, "cleanup_unconfirmed");
                return Err(ArtifactError::Io(format!(
                    "cleanup_unconfirmed at {}: {error}",
                    self.path.display()
                )));
            }
        }
        Ok(())
    }

    fn settle<T>(&mut self, result: Result<T, ArtifactError>) -> Result<T, ArtifactError> {
        match (result, self.cleanup()) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Ok(())) => Err(primary),
            (Err(primary), Err(cleanup)) => Err(retain_primary_error(
                primary,
                "cleanup_unconfirmed",
                cleanup,
            )),
        }
    }
}

impl Drop for StagingOwner {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::error!(phase = "staging_drop", path = %self.path.display(), cleanup_error = %error, "cleanup_unconfirmed");
        }
    }
}

/// Adapter contract for the selected compact tier and artifact catalogue.
///
/// Durable PostgreSQL implementations live outside Runtime. The process-local
/// implementation below is available only through explicitly test-scoped
/// constructors. Blob bytes always remain behind
/// the selected `StorageDomainId::Blobs` endpoint.
pub trait ArtifactMetadataRepository: Send + Sync {
    fn reserve_attempt(
        &self,
        object: &ArtifactObjectRecord,
        quota: u64,
        token: &str,
        publication_id: &str,
    ) -> Result<ArtifactObjectRecord, String>;
    fn reservation(&self, token: &str) -> Result<Option<ArtifactReservation>, String>;
    fn publish_attempt(
        &self,
        record: &ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
    ) -> Result<(), String>;
    fn release_attempt(&self, token: &str, sha256: &str, generation: &str) -> Result<bool, String>;
    fn active_attempts_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ArtifactReservation>, String>;
    fn prepared_high_water(&self) -> Result<Option<String>, String>;
    fn prepared_page(
        &self,
        after: Option<&str>,
        through: &str,
        limit: usize,
    ) -> Result<PreparedArtifactPage, String>;
    fn retire_prepared(
        &self,
        candidate: &PreparedArtifactCandidate,
    ) -> Result<PreparedArtifactRetirement, String>;
    fn adopt_live(&self, expected: &ArtifactRecord) -> Result<(), String>;
    fn prepared_readable(
        &self,
        expected: &ArtifactRecord,
        origin: &PreparedArtifactOrigin,
    ) -> Result<bool, String>;
    fn process_closure_record(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, String> {
        let _ = artifact_id;
        Err("bounded process closure metadata read is unsupported by this adapter".into())
    }
    fn process_closure_object(&self, sha256: &str) -> Result<Option<ArtifactObjectRecord>, String> {
        let _ = sha256;
        Err("bounded process closure object read is unsupported by this adapter".into())
    }
    fn publish_process_closure_attempt(
        &self,
        record: &ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
        authority: &crate::ProcessClosurePublicationAuthority,
    ) -> Result<(), String> {
        let _ = (record, generation, token, pin, authority);
        Err("process closure publication is unsupported by this adapter".into())
    }
    fn claim_object_deletion(&self, sha256: &str, generation: &str) -> Result<bool, String>;
    fn finish_object_deletion(&self, sha256: &str, generation: &str) -> Result<(), String>;
    fn object(&self, sha256: &str) -> Result<Option<ArtifactObjectRecord>, String>;
    fn checkpoint_object_metadata(
        &self, sha256: &str,
    ) -> Result<Option<(ArtifactObjectRecord, String)>, String> {
        let _ = sha256;
        Err("prepared checkpoint metadata reuse is unsupported by this adapter".into())
    }
    fn record(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, String>;
    fn catalog_page(&self, query: ArtifactCatalogQuery) -> Result<ArtifactCatalogPage, String>;
    fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(), String>;
    fn remove_record(&self, artifact_id: &str) -> Result<(), String>;
    fn unreferenced_objects_before(
        &self,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<ArtifactObjectRecord>, String>;
    fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String>;
    fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String>;
    fn is_pinned(&self, artifact_id: &str, at_ms: u64) -> Result<bool, String>;
    fn stats(&self, at_ms: u64) -> Result<ArtifactStoreStats, String>;
}

#[async_trait]
pub trait ArtifactReadPort: Send + Sync {
    async fn read_artifact(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>, ArtifactError>;
}

pub trait ArtifactMetadataPort: Send + Sync {
    fn artifact_stats(&self) -> Result<ArtifactStoreStats, ArtifactError>;
}

pub trait ArtifactGcPort: Send + Sync {
    fn pin_artifact(
        &self,
        artifact: &ArtifactRef,
        owner: &str,
        until_ms: u64,
    ) -> Result<(), ArtifactError>;
    fn unpin_artifact(&self, artifact: &ArtifactRef, owner: &str) -> Result<(), ArtifactError>;
    fn delete_artifact(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
    ) -> Result<(), ArtifactError>;
    fn collect_artifact_garbage(&self, limit: usize) -> Result<ArtifactGcReport, ArtifactError>;
}

#[derive(Clone)]
pub struct ArtifactStore {
    inner: Arc<ArtifactStoreInner>,
    prepared_observation: bool,
}

struct ArtifactStoreInner {
    blob_root: PathBuf,
    staging_root: PathBuf,
    repository: Arc<dyn ArtifactMetadataRepository>,
    config: ArtifactStoreConfig,
    #[cfg(test)]
    checkpoint_body_reads: std::sync::atomic::AtomicUsize,
    /// Test-only input byte accounting at the actual read hash sites.
    #[cfg(test)]
    verified_read_hash_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    staging_fault: Mutex<Option<StagingFault>>,
    #[cfg(test)]
    public_read_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

pub(crate) struct PreparedCheckpointGuard {
    owner: Arc<ArtifactStoreInner>,
    record: ArtifactRecord,
    object: ArtifactObjectRecord,
    revision: String,
    blob_identity: Option<String>,
}

impl ArtifactStore {
    fn checkpoint_metadata_snapshot(
        &self, artifact: &ArtifactRef, scope: &str, origin: &PreparedArtifactOrigin,
    ) -> Result<(ArtifactRecord, ArtifactObjectRecord, String, Option<String>), ArtifactError> {
        origin.validate().map_err(ArtifactError::Metadata)?;
        if !matches!(origin, PreparedArtifactOrigin::CheckpointBody { projection_id, .. }
            if projection_id.starts_with("projection-history:"))
            || artifact.visibility_scope != scope || scope == "public"
        {
            return Err(ArtifactError::Unauthorized);
        }
        let record = self.inner.repository.record(parse_selector(&artifact.selector)?)
            .map_err(ArtifactError::Metadata)?.ok_or(ArtifactError::NotFound)?;
        validate_ref(artifact, &record)?;
        if record.record_class != ArtifactRecordClass::PrivatePrepared
            || record.prepared_origin.as_ref() != Some(origin)
            || record.visibility_scope != scope
            || !self.inner.repository.prepared_readable(&record, origin).map_err(ArtifactError::Metadata)?
        {
            return Err(ArtifactError::Unauthorized);
        }
        let (object, revision) = self.inner.repository.checkpoint_object_metadata(&record.sha256)
            .map_err(ArtifactError::Metadata)?.ok_or(ArtifactError::NotFound)?;
        if object.compact_body.is_some() || object.sha256 != record.sha256
            || object.bytes != record.bytes || object.tier != record.tier
            || object.bytes > self.inner.config.max_object_bytes
            || object.generation.is_empty() || object.generation.len() > 128
            || revision.is_empty() || revision.len() > 128
        {
            return Err(ArtifactError::Metadata("prepared checkpoint metadata identity mismatch".into()));
        }
        let blob_identity = if object.tier == ArtifactObjectTier::Blob {
            let metadata = fs::symlink_metadata(self.generation_path(&object.sha256, &object.generation))?;
            if !metadata.file_type().is_file() || metadata.len() != object.bytes {
                return Err(ArtifactError::Metadata("prepared checkpoint blob is missing or changed".into()));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                Some(format!("{}:{}:{}:{}:{}:{}:{}", metadata.len(), metadata.dev(), metadata.ino(),
                    metadata.mtime(), metadata.mtime_nsec(), metadata.ctime(), metadata.ctime_nsec()))
            }
            #[cfg(not(unix))]
            return Err(ArtifactError::Metadata("prepared checkpoint blob identity is unsupported".into()));
        } else { None };
        let bytes = prepared_metadata_bytes(&record).map_err(ArtifactError::Metadata)?
            + serde_json::to_vec(&(&object.generation, &object.sha256,
                object.bytes, if object.tier == ArtifactObjectTier::Blob { "blob" } else { "compact" },
                object.created_at_ms, &revision, &blob_identity))
                .map_err(|error| ArtifactError::Metadata(error.to_string()))?.len();
        if bytes > 16 * 1024 {
            return Err(ArtifactError::Metadata("prepared checkpoint metadata exceeds reserved bound".into()));
        }
        Ok((record, object, revision, blob_identity))
    }

    pub(crate) fn checkpoint_guard_after_write(
        &self, artifact: &ArtifactRef, scope: &str, origin: &PreparedArtifactOrigin,
    ) -> Result<PreparedCheckpointGuard, ArtifactError> {
        let (record, object, revision, blob_identity) = self.checkpoint_metadata_snapshot(artifact, scope, origin)?;
        Ok(PreparedCheckpointGuard { owner: self.inner.clone(), record, object, revision, blob_identity })
    }

    pub(crate) fn validate_checkpoint_guard(
        &self, guard: PreparedCheckpointGuard, artifact: &ArtifactRef,
        scope: &str, origin: &PreparedArtifactOrigin,
    ) -> Result<(), ArtifactError> {
        if !Arc::ptr_eq(&self.inner, &guard.owner) {
            return Err(ArtifactError::Unauthorized);
        }
        let (record, object, revision, blob_identity) = self.checkpoint_metadata_snapshot(artifact, scope, origin)?;
        if !same_publication(&record, &guard.record) || object != guard.object
            || revision != guard.revision || blob_identity != guard.blob_identity
        {
            return Err(ArtifactError::Metadata("prepared checkpoint witness changed before publication".into()));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_body_read_count(&self) -> usize {
        self.inner.checkpoint_body_reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
struct StagingFault {
    stage: &'static str,
    path: Option<PathBuf>,
    hits: usize,
    observed: Vec<u8>,
}

#[cfg(test)]
mod own865_checkpoint_guard_tests {
    use super::*;

    #[test]
    fn checkpoint_guard_rejects_missing_compact_generation_and_changed_blob() {
        for compact in [true, false] {
            for mutation in 0..3 {
                let directory = tempfile::tempdir().unwrap();
                let repository = Arc::new(EphemeralArtifactRepository::default());
                let store = ArtifactStore::new(directory.path(), repository.clone(),
                    ArtifactStoreConfig { compact_threshold_bytes: if compact { 1024 * 1024 } else { 1 },
                        ..Default::default() }).unwrap();
                let origin = PreparedArtifactOrigin::CheckpointBody {
                    projection_id: "projection-history:guard".into(), source_cursor: 334,
                };
                let artifact = store.write_pre_pinned_blocking("session:guard", "guard-body",
                    &vec![b'x'; 242788], &origin).unwrap();
                let guard = store.checkpoint_guard_after_write(&artifact, "session:guard", &origin).unwrap();
                if compact {
                    let mut state = repository.state.lock().unwrap();
                    let object = state.objects.get_mut(&artifact.sha256).unwrap();
                    match mutation {
                        0 => object.compact_body = None,
                        1 => object.generation.push_str("-replaced"),
                        _ => object.bytes += 1,
                    }
                } else {
                    let path = store.generation_path(&guard.object.sha256, &guard.object.generation);
                    match mutation {
                        0 => fs::remove_file(path).unwrap(),
                        1 => fs::write(path, b"truncated").unwrap(),
                        _ => {
                            fs::remove_file(&path).unwrap();
                            fs::write(path, vec![b'y'; 242788]).unwrap();
                        }
                    }
                }
                assert!(store.validate_checkpoint_guard(guard, &artifact, "session:guard", &origin).is_err());
            }
        }
    }
}

impl std::fmt::Debug for ArtifactStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ArtifactStore")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl ArtifactStore {
    #[must_use]
    pub fn for_test_default(blob_root: impl Into<PathBuf>) -> Self {
        let blob_root = blob_root.into();
        let repository = Arc::new(EphemeralArtifactRepository::default());
        Self::from_validated(blob_root, repository, ArtifactStoreConfig::default())
    }

    pub fn for_test(
        blob_root: impl Into<PathBuf>,
        config: ArtifactStoreConfig,
    ) -> Result<Self, ArtifactError> {
        let blob_root = blob_root.into();
        let repository = Arc::new(EphemeralArtifactRepository::default());
        Self::new(blob_root, repository, config)
    }

    pub fn new(
        blob_root: impl Into<PathBuf>,
        repository: Arc<dyn ArtifactMetadataRepository>,
        config: ArtifactStoreConfig,
    ) -> Result<Self, ArtifactError> {
        let blob_root = blob_root.into();
        let config = config.validate()?;
        Ok(Self::from_validated(blob_root, repository, config))
    }

    fn from_validated(
        blob_root: PathBuf,
        repository: Arc<dyn ArtifactMetadataRepository>,
        config: ArtifactStoreConfig,
    ) -> Self {
        Self {
            prepared_observation: false,
            inner: Arc::new(ArtifactStoreInner {
                staging_root: blob_root.join("staging"),
                blob_root,
                repository,
                config,
                #[cfg(test)]
                checkpoint_body_reads: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(test)]
                verified_read_hash_bytes: std::sync::atomic::AtomicU64::new(0),
                #[cfg(test)]
                staging_fault: Mutex::new(None),
                #[cfg(test)]
                public_read_hook: Mutex::new(None),
            }),
        }
    }

    #[must_use]
    pub fn config(&self) -> ArtifactStoreConfig {
        self.inner.config
    }

    /// A private read view over the same catalogue and blob generations. Only
    /// the event-store observation binding selects this view; it grants no new
    /// publication/adoption rights and does not replace metadata validation.
    pub(crate) fn verified_observation_view(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            prepared_observation: true,
        }
    }

    pub(crate) fn is_verified_observation(&self) -> bool {
        self.prepared_observation
    }

    #[cfg(test)]
    fn bind_staging_fault(&self, path: &Path) {
        if let Some(fault) = self.inner.staging_fault.lock().unwrap().as_mut() {
            if fault.path.is_none() {
                assert_eq!(path.parent(), Some(self.inner.staging_root.as_path()));
                fault.path = Some(path.to_path_buf());
            }
        }
    }

    #[cfg(test)]
    fn inject_staging_fault(&self, stage: &str, path: &Path, file: Option<&mut fs::File>) {
        let mut slot = self.inner.staging_fault.lock().unwrap();
        let Some(fault) = slot.as_mut().filter(|fault| {
            fault.stage == stage && fault.path.as_deref() == Some(path) && fault.hits == 0
        }) else {
            return;
        };
        fault.hits += 1;
        match stage {
            "write" => {
                let file = file.unwrap();
                file.write_all(b"partial staged bytes").unwrap();
                file.flush().unwrap();
                fault.observed = fs::read(path).unwrap();
                *file = fs::File::open(path).unwrap();
            }
            "sync" => {
                let file = file.unwrap();
                file.flush().unwrap();
                fault.observed = fs::read(path).unwrap();
                #[cfg(unix)]
                {
                    let (socket, peer) = std::os::unix::net::UnixStream::pair().unwrap();
                    let descriptor: std::os::fd::OwnedFd = socket.into();
                    *file = fs::File::from(descriptor);
                    drop(peer);
                }
                #[cfg(not(unix))]
                panic!("this OS error-path fixture requires owned Unix descriptors");
            }
            "compact_read" => {
                fault.observed = fs::read(path).unwrap();
                #[cfg(unix)]
                {
                    // A permission-only fault is ineffective on filesystems that
                    // ignore chmod. This dangling relative link fails the actual
                    // read and remains removable by the original staging owner.
                    fs::remove_file(path).unwrap();
                    std::os::unix::fs::symlink("missing-compact-body", path).unwrap();
                }
                #[cfg(not(unix))]
                panic!("this OS error-path fixture requires Unix symlinks");
            }
            "source_read" => *file.unwrap() = fs::File::open(path.parent().unwrap()).unwrap(),
            "source_growth" => {
                let oversized = vec![b'x'; self.inner.config.max_object_bytes as usize + 1];
                fs::write(path, &oversized).unwrap();
                fault.observed = oversized;
                *file.unwrap() = fs::File::open(path).unwrap();
            }
            other => panic!("unsupported staging fault {other}"),
        }
    }

    /// Resolve a durable `ArtifactRef` from a public selector without reading
    /// its body. Used by runtime-owned tools such as `evidence_retrieve`.
    pub fn resolve(&self, selector: &str) -> Result<ArtifactRef, ArtifactError> {
        let id = parse_selector(selector)?;
        let record = self
            .inner
            .repository
            .record(id)
            .map_err(ArtifactError::Metadata)?
            .ok_or(ArtifactError::NotFound)?;
        if record.record_class == ArtifactRecordClass::PrivatePrepared {
            return Err(ArtifactError::Unauthorized);
        }
        Ok(record.content_reference())
    }

    pub async fn begin(
        &self,
        descriptor: ArtifactWriteDescriptor,
    ) -> Result<Box<dyn ArtifactWriteSink>, ArtifactError> {
        self.begin_with_identity(descriptor, None).await
    }

    async fn begin_with_identity(
        &self,
        descriptor: ArtifactWriteDescriptor,
        artifact_id: Option<String>,
    ) -> Result<Box<dyn ArtifactWriteSink>, ArtifactError> {
        if artifact_id.as_deref().is_some_and(|artifact_id| {
            artifact_id.is_empty()
                || artifact_id.len() > 128
                || !artifact_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        }) {
            return Err(ArtifactError::InvalidSelector);
        }
        if descriptor
            .expected_bytes
            .is_some_and(|bytes| bytes > self.inner.config.max_object_bytes)
        {
            return Err(ArtifactError::ObjectTooLarge);
        }
        tokio::fs::create_dir_all(&self.inner.staging_root).await?;
        let staging_path = self
            .inner
            .staging_root
            .join(format!("{}.part", Uuid::new_v4().simple()));
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging_path)
            .await?;
        #[cfg(test)]
        self.bind_staging_fault(&staging_path);
        Ok(Box::new(LocalArtifactWriter {
            store: self.clone(),
            descriptor,
            staging: Some(StagingOwner::new(staging_path)),
            file: Some(file),
            hasher: Sha256::new(),
            bytes: 0,
            artifact_id,
            closed: false,
        }))
    }

    pub async fn write_bytes(
        &self,
        descriptor: ArtifactWriteDescriptor,
        payload: &[u8],
    ) -> Result<ArtifactRef, ArtifactError> {
        let mut writer = self.begin(descriptor).await?;
        writer.write_chunk(payload).await?;
        writer.finish().await
    }

    pub async fn write_bytes_with_identity(
        &self,
        descriptor: ArtifactWriteDescriptor,
        payload: &[u8],
        artifact_id: impl Into<String>,
    ) -> Result<ArtifactRef, ArtifactError> {
        let mut writer = self
            .begin_with_identity(descriptor, Some(artifact_id.into()))
            .await?;
        writer.write_chunk(payload).await?;
        writer.finish().await
    }

    pub fn write_path_blocking(
        &self,
        descriptor: ArtifactWriteDescriptor,
        path: &Path,
    ) -> Result<ArtifactRef, ArtifactError> {
        if !path.is_file() {
            return Err(ArtifactError::Io(format!(
                "artifact source is not a file: {}",
                path.display()
            )));
        }
        let size = fs::metadata(path)?.len();
        if size > self.inner.config.max_object_bytes {
            return Err(ArtifactError::ObjectTooLarge);
        }
        fs::create_dir_all(&self.inner.staging_root)?;
        let staging_path = self
            .inner
            .staging_root
            .join(format!("{}.part", Uuid::new_v4().simple()));
        let mut input = fs::File::open(path)?;
        let mut output = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging_path)?;
        let mut staging = StagingOwner::new(staging_path);
        #[cfg(test)]
        {
            self.bind_staging_fault(&staging.path);
            self.inject_staging_fault("source_read", &staging.path, Some(&mut input));
            self.inject_staging_fault("source_growth", &staging.path, Some(&mut input));
        }
        let mut hasher = Sha256::new();
        let mut copied = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        let copied_result = (|| -> Result<(), ArtifactError> {
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                copied = copied.saturating_add(read as u64);
                if copied > self.inner.config.max_object_bytes {
                    return Err(ArtifactError::ObjectTooLarge);
                }
                hasher.update(&buffer[..read]);
                #[cfg(test)]
                self.inject_staging_fault("write", &staging.path, Some(&mut output));
                output.write_all(&buffer[..read])?;
            }
            #[cfg(test)]
            self.inject_staging_fault("sync", &staging.path, Some(&mut output));
            output.sync_all()?;
            Ok(())
        })();
        drop(output);
        if let Err(error) = copied_result {
            return staging.settle(Err(error));
        }
        self.publish_staged(
            descriptor,
            staging,
            copied,
            format!("sha256:{:x}", hasher.finalize()),
        )
    }

    pub async fn read(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>, ArtifactError> {
        let store = self.clone();
        let artifact = artifact.clone();
        let authorized_scope = authorized_scope.to_string();
        tokio::task::spawn_blocking(move || {
            store.read_blocking(&artifact, &authorized_scope, range)
        })
        .await
        .map_err(|error| ArtifactError::Blocking(error.to_string()))?
    }

    /// Peak temporary storage for a public range verification. Compact objects
    /// are bounded by the configured compact threshold; blob hashing streams.
    #[cfg(test)]
    pub(crate) fn set_public_read_hook(&self, hook: Arc<dyn Fn() + Send + Sync>) {
        *self.inner.public_read_hook.lock().unwrap() = Some(hook);
    }

    pub(crate) fn public_read_scratch_bytes(&self) -> u64 {
        self.inner
            .config
            .compact_threshold_bytes
            .saturating_add(64 * 1024)
    }

    pub(crate) fn read_public_range_blocking(
        &self,
        artifact: &ArtifactRef,
        scope: &str,
        range: Range<u64>,
    ) -> Result<Vec<u8>, ArtifactError> {
        let current = self.resolve(&artifact.selector)?;
        if current != *artifact
            || (scope != current.visibility_scope && current.visibility_scope != "public")
        {
            return Err(ArtifactError::Unauthorized);
        }
        #[cfg(test)]
        if let Some(hook) = self.inner.public_read_hook.lock().unwrap().clone() {
            hook();
        }
        let bytes = self.read_blocking(artifact, scope, Some(range))?;
        // A deleted/replaced record must not escape as a successful read.
        if self.resolve(&artifact.selector)? != *artifact {
            return Err(ArtifactError::Unauthorized);
        }
        Ok(bytes)
    }

    pub(crate) fn read_session_content_by_digest_blocking(
        &self,
        session_id: &str,
        sha256: &str,
        byte_count: u64,
    ) -> Result<Vec<u8>, ArtifactError> {
        if session_id.trim().is_empty() {
            return Err(ArtifactError::Unauthorized);
        }
        let digest = sha256.strip_prefix("sha256:").unwrap_or(sha256);
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ArtifactError::Metadata("invalid content SHA256".into()));
        }
        if byte_count > self.inner.config.max_object_bytes {
            return Err(ArtifactError::ObjectTooLarge);
        }
        let digest = digest.to_ascii_lowercase();
        let scope = format!("session:{session_id}");
        let mut snapshot: Option<ArtifactCatalogSnapshot> = None;
        let mut after_id: Option<String> = None;
        let mut last_error = ArtifactError::NotFound;
        loop {
            let page = self
                .inner
                .repository
                .catalog_page(ArtifactCatalogQuery {
                    scopes: vec![scope.clone()],
                    query: digest.clone(),
                    after_id: after_id.clone(),
                    snapshot: snapshot.clone(),
                    limit: 128,
                })
                .map_err(ArtifactError::Metadata)?;
            if snapshot.as_ref().is_some_and(|original| {
                original.fence != page.snapshot.fence
                    || original.revisions != page.snapshot.revisions
            }) {
                return Err(ArtifactError::Metadata(
                    "Artifact catalog snapshot changed".into(),
                ));
            }
            snapshot = Some(page.snapshot);
            for record in page.records {
                if record.visibility_scope != scope
                    || record
                        .sha256
                        .strip_prefix("sha256:")
                        .unwrap_or(&record.sha256)
                        != digest
                    || record.bytes != byte_count
                {
                    continue;
                }
                match self.read_blocking(&record.content_reference(), &scope, None) {
                    Ok(bytes) => {
                        if bytes.len() as u64 == byte_count
                            && format!("{:x}", Sha256::digest(&bytes)) == digest
                        {
                            return Ok(bytes);
                        }
                        last_error = ArtifactError::Metadata(
                            "Artifact content does not match its SHA256/bytes".into(),
                        );
                    }
                    Err(error) => last_error = error,
                }
            }
            let Some(next_id) = page.next_id else {
                return Err(last_error);
            };
            if next_id.is_empty() || after_id.as_ref().is_some_and(|after| next_id <= *after) {
                return Err(ArtifactError::Metadata(
                    "Artifact catalog did not advance".into(),
                ));
            }
            after_id = Some(next_id);
        }
    }

    pub fn read_blocking(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>, ArtifactError> {
        self.read_verified_blocking(artifact, authorized_scope, range, None, false, true)
            .map(|read| read.body)
    }

    pub(crate) fn read_prepared_blocking(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        origin: &PreparedArtifactOrigin,
    ) -> Result<Vec<u8>, ArtifactError> {
        origin.validate().map_err(ArtifactError::Metadata)?;
        self.read_verified_blocking(artifact, authorized_scope, None, Some(origin), false, true)
            .map(|read| read.body)
    }

    pub(crate) fn read_prepared_observation_blocking(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        origin: &PreparedArtifactOrigin,
    ) -> Result<Vec<u8>, ArtifactError> {
        origin.validate().map_err(ArtifactError::Metadata)?;
        self.read_verified_blocking(artifact, authorized_scope, None, Some(origin), false, false)
            .map(|read| read.body)
    }

    pub(crate) fn read_process_closure_blocking(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        origin: &PreparedArtifactOrigin,
    ) -> Result<VerifiedProcessClosureRead, ArtifactError> {
        origin.validate().map_err(ArtifactError::Metadata)?;
        if !matches!(origin, PreparedArtifactOrigin::ToolEffectClosure { .. })
            || artifact.media_type != "application/vnd.cowd.process-closure+json"
            || artifact.bytes > 32768
            || authorized_scope == "public"
        {
            return Err(ArtifactError::Unauthorized);
        }
        self.read_verified_blocking(artifact, authorized_scope, None, Some(origin), true, true)
    }

    fn read_verified_blocking(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        range: Option<Range<u64>>,
        origin: Option<&PreparedArtifactOrigin>,
        process_closure: bool,
        track_access: bool,
    ) -> Result<VerifiedProcessClosureRead, ArtifactError> {
        let id = parse_selector(&artifact.selector)?;
        let mut reads = 0;
        let mut metadata_bytes = 0;
        reads += 1;
        let record = if process_closure {
            self.inner.repository.process_closure_record(id)
        } else {
            self.inner.repository.record(id)
        }
        .map_err(ArtifactError::Metadata)?
        .ok_or(ArtifactError::NotFound)?;
        if process_closure {
            metadata_bytes +=
                closure_record_metadata_bytes(&record).map_err(ArtifactError::Metadata)?;
        }
        if record.visibility_scope != authorized_scope && record.visibility_scope != "public" {
            return Err(ArtifactError::Unauthorized);
        }
        validate_ref(artifact, &record)?;
        let reserved = matches!(
            record.prepared_origin,
            Some(PreparedArtifactOrigin::ToolEffectClosure { .. })
        );
        if process_closure {
            if !reserved
                || record.record_class != ArtifactRecordClass::PrivatePrepared
                || record.prepared_origin.as_ref() != origin
                || record.visibility_scope != authorized_scope
                || record.bytes > 32768
                || record.media_type != "application/vnd.cowd.process-closure+json"
            {
                return Err(ArtifactError::Unauthorized);
            }
        } else if reserved
            || matches!(
                origin,
                Some(PreparedArtifactOrigin::ToolEffectClosure { .. })
            )
        {
            return Err(ArtifactError::Unauthorized);
        }
        if let Some(origin) = origin {
            if process_closure
                || !(record.record_class == ArtifactRecordClass::Live
                    && record.prepared_origin.is_none())
            {
                reads += 1;
                let readable = self
                    .inner
                    .repository
                    .prepared_readable(&record, origin)
                    .map_err(ArtifactError::Metadata)?;
                metadata_bytes += serde_json::to_vec(&readable)
                    .map_err(|error| ArtifactError::Metadata(error.to_string()))?
                    .len();
                if !readable {
                    return Err(ArtifactError::Metadata(
                        "prepared origin or protection mismatch".into(),
                    ));
                }
            }
        }
        reads += 1;
        #[cfg(test)]
        if matches!(origin, Some(PreparedArtifactOrigin::CheckpointBody { .. })) {
            self.inner.checkpoint_body_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let object = if process_closure {
            self.inner.repository.process_closure_object(&record.sha256)
        } else {
            self.inner.repository.object(&record.sha256)
        }
        .map_err(ArtifactError::Metadata)?
        .ok_or(ArtifactError::NotFound)?;
        if object.bytes != record.bytes
            || object.bytes > self.inner.config.max_object_bytes
            || (process_closure
                && (object.bytes > 32768
                    || object
                        .compact_body
                        .as_ref()
                        .is_some_and(|body| body.len() > 32768)))
        {
            return Err(ArtifactError::Metadata(
                "artifact object byte count mismatch".into(),
            ));
        }
        if process_closure {
            metadata_bytes += serde_json::to_vec(&serde_json::json!({
                "sha256":object.sha256, "bytes":object.bytes, "generation":object.generation,
                "tier":match object.tier { ArtifactObjectTier::Compact => "compact", ArtifactObjectTier::Blob => "blob" },
                "created_at_ms":object.created_at_ms,
            })).map_err(|error| ArtifactError::Metadata(error.to_string()))?.len();
        }
        let requested_range = normalize_range(range.clone(), object.bytes)?;
        let full_read =
            range.is_none() || record.record_class == ArtifactRecordClass::PrivatePrepared;
        let range = if full_read {
            0..object.bytes
        } else {
            requested_range.clone()
        };
        let payload = match object.tier {
            ArtifactObjectTier::Compact => {
                let body = object.compact_body.ok_or_else(|| {
                    ArtifactError::Metadata("compact artifact body is missing".to_string())
                })?;
                if body.len() as u64 != object.bytes {
                    return Err(ArtifactError::Metadata(
                        "compact artifact length mismatch".into(),
                    ));
                }
                #[cfg(test)]
                self.inner
                    .verified_read_hash_bytes
                    .fetch_add(body.len() as u64, std::sync::atomic::Ordering::Relaxed);
                if format!("sha256:{:x}", Sha256::digest(&body)) != record.sha256 {
                    return Err(ArtifactError::Metadata(
                        "artifact content digest mismatch".into(),
                    ));
                }
                body[range.start as usize..range.end as usize].to_vec()
            }
            ArtifactObjectTier::Blob => {
                let mut file =
                    fs::File::open(self.generation_path(&object.sha256, &object.generation))?;
                if file.metadata()?.len() != object.bytes {
                    return Err(ArtifactError::Metadata(
                        "blob artifact length mismatch".into(),
                    ));
                }
                // Hash and collect the selected range in one physical pass.
                // No full-blob Vec and no hash/read time-of-check gap.
                let mut digest = Sha256::new();
                let mut scratch = [0_u8; 64 * 1024];
                let mut body = Vec::with_capacity((range.end - range.start) as usize);
                let mut position = 0_u64;
                loop {
                    let count = file.read(&mut scratch)?;
                    if count == 0 {
                        break;
                    }
                    digest.update(&scratch[..count]);
                    #[cfg(test)]
                    self.inner
                        .verified_read_hash_bytes
                        .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
                    let next = position.checked_add(count as u64).ok_or_else(|| {
                        ArtifactError::Metadata("artifact byte range overflow".into())
                    })?;
                    let start = position.max(range.start);
                    let end = next.min(range.end);
                    if start < end {
                        body.extend_from_slice(
                            &scratch[(start - position) as usize..(end - position) as usize],
                        );
                    }
                    position = next;
                }
                if position != object.bytes
                    || body.len() as u64 != range.end - range.start
                    || format!("sha256:{:x}", digest.finalize()) != record.sha256
                {
                    return Err(ArtifactError::Metadata(
                        "artifact content digest mismatch".into(),
                    ));
                }
                body
            }
        };
        if process_closure {
            reads += 1;
            let readable = self
                .inner
                .repository
                .prepared_readable(&record, origin.ok_or(ArtifactError::Unauthorized)?)
                .map_err(ArtifactError::Metadata)?;
            metadata_bytes += serde_json::to_vec(&readable)
                .map_err(|error| ArtifactError::Metadata(error.to_string()))?
                .len();
            if !readable {
                return Err(ArtifactError::Unauthorized);
            }
            return Ok(VerifiedProcessClosureRead {
                body: payload,
                metadata_bytes,
                reads,
            });
        }
        if origin.is_none() && record.record_class == ArtifactRecordClass::PrivatePrepared {
            self.inner
                .repository
                .adopt_live(&record)
                .map_err(ArtifactError::Metadata)?;
            return Ok(VerifiedProcessClosureRead {
                body: payload[requested_range.start as usize..requested_range.end as usize]
                    .to_vec(),
                metadata_bytes,
                reads,
            });
        }
        if track_access {
            self.inner
                .repository
                .touch(id, now_ms())
                .map_err(ArtifactError::Metadata)?;
        }
        Ok(VerifiedProcessClosureRead {
            body: payload,
            metadata_bytes,
            reads,
        })
    }

    pub fn delete(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
    ) -> Result<(), ArtifactError> {
        let id = parse_selector(&artifact.selector)?;
        let record = self
            .inner
            .repository
            .record(id)
            .map_err(ArtifactError::Metadata)?
            .ok_or(ArtifactError::NotFound)?;
        if record.visibility_scope != authorized_scope {
            return Err(ArtifactError::Unauthorized);
        }
        if self
            .inner
            .repository
            .is_pinned(id, now_ms())
            .map_err(ArtifactError::Metadata)?
        {
            return Err(ArtifactError::Metadata(
                "artifact is pinned by an active receipt".to_string(),
            ));
        }
        self.inner
            .repository
            .remove_record(id)
            .map_err(ArtifactError::Metadata)
    }

    pub fn pin(
        &self,
        artifact: &ArtifactRef,
        owner: &str,
        until_ms: u64,
    ) -> Result<(), ArtifactError> {
        let id = parse_selector(&artifact.selector)?;
        if owner.trim().is_empty() || owner == ARTIFACT_PREPARED_PIN_OWNER {
            return Err(ArtifactError::InvalidSelector);
        }
        let record = self
            .inner
            .repository
            .record(id)
            .map_err(ArtifactError::Metadata)?
            .ok_or(ArtifactError::NotFound)?;
        validate_ref(artifact, &record)?;
        self.inner
            .repository
            .pin(id, owner, until_ms)
            .map_err(ArtifactError::Metadata)
    }

    pub fn unpin(&self, artifact: &ArtifactRef, owner: &str) -> Result<(), ArtifactError> {
        if owner == ARTIFACT_PREPARED_PIN_OWNER {
            return Err(ArtifactError::Unauthorized);
        }
        let id = parse_selector(&artifact.selector)?;
        self.inner
            .repository
            .unpin(id, owner)
            .map_err(ArtifactError::Metadata)
    }

    pub fn stats(&self) -> Result<ArtifactStoreStats, ArtifactError> {
        self.inner
            .repository
            .stats(now_ms())
            .map_err(ArtifactError::Metadata)
    }

    pub fn collect_garbage(&self, limit: usize) -> Result<ArtifactGcReport, ArtifactError> {
        let stats = self.stats()?;
        if stats.physical_bytes <= self.inner.config.gc_high_water_bytes {
            return Ok(ArtifactGcReport::default());
        }
        let before = now_ms().saturating_sub(self.inner.config.orphan_grace_ms);
        self.collect_unreferenced(
            before,
            limit,
            Some(
                stats
                    .physical_bytes
                    .saturating_sub(self.inner.config.gc_low_water_bytes),
            ),
        )
    }

    pub fn reconcile_quiesced_orphans(
        &mut self,
        all_publishers_quiesced: bool,
        limit: usize,
    ) -> Result<ArtifactGcReport, ArtifactError> {
        self.require_quiesced_exclusive(all_publishers_quiesced)?;
        let before = now_ms().saturating_sub(self.inner.config.orphan_grace_ms);
        self.collect_unreferenced(before, limit, None)
    }

    pub(crate) fn require_quiesced_exclusive(
        &mut self,
        quiesced: bool,
    ) -> Result<(), ArtifactError> {
        if !quiesced || Arc::get_mut(&mut self.inner).is_none() {
            return Err(ArtifactError::Metadata(
                "orphan reconciliation requires explicit quiescence and exclusive local ownership"
                    .into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn repository(&self) -> &dyn ArtifactMetadataRepository {
        self.inner.repository.as_ref()
    }

    fn collect_unreferenced(
        &self,
        before: u64,
        limit: usize,
        reclaim_target: Option<u64>,
    ) -> Result<ArtifactGcReport, ArtifactError> {
        let candidates = self
            .inner
            .repository
            .unreferenced_objects_before(before, limit)
            .map_err(ArtifactError::Metadata)?;
        let mut report = ArtifactGcReport::default();
        for object in candidates {
            report.examined = report.examined.saturating_add(1);
            if !self
                .inner
                .repository
                .claim_object_deletion(&object.sha256, &object.generation)
                .map_err(ArtifactError::Metadata)?
            {
                continue;
            }
            if object.tier == ArtifactObjectTier::Blob {
                let path = self.generation_path(&object.sha256, &object.generation);
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                if let Some(parent) = path.parent() {
                    match fs::File::open(parent) {
                        Ok(directory) => directory.sync_all()?,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            self.inner
                .repository
                .finish_object_deletion(&object.sha256, &object.generation)
                .map_err(ArtifactError::Metadata)?;
            report.removed_objects = report.removed_objects.saturating_add(1);
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(object.bytes);
            if reclaim_target.is_some_and(|target| report.reclaimed_bytes >= target) {
                break;
            }
        }
        Ok(report)
    }

    pub(crate) fn write_pre_pinned_blocking(
        &self,
        scope: &str,
        identity: &str,
        payload: &[u8],
        origin: &PreparedArtifactOrigin,
    ) -> Result<ArtifactRef, ArtifactError> {
        self.write_pre_pinned_with_media_blocking(
            scope,
            identity,
            "application/json",
            payload,
            origin,
        )
    }

    pub(crate) fn write_pre_pinned_with_media_blocking(
        &self,
        scope: &str,
        identity: &str,
        media_type: &str,
        payload: &[u8],
        origin: &PreparedArtifactOrigin,
    ) -> Result<ArtifactRef, ArtifactError> {
        if matches!(origin, PreparedArtifactOrigin::ToolEffectClosure { .. }) {
            return Err(ArtifactError::Unauthorized);
        }
        self.write_prepared_with_authority_blocking(
            scope, identity, media_type, payload, origin, None,
        )
    }

    pub fn write_process_closure_blocking(
        &self,
        scope: &str,
        identity: &str,
        payload: &[u8],
        origin: &PreparedArtifactOrigin,
        authority: &crate::ProcessClosurePublicationAuthority,
    ) -> Result<ArtifactRef, ArtifactError> {
        if !matches!(origin, PreparedArtifactOrigin::ToolEffectClosure { .. })
            || payload.len() > 32768
        {
            return Err(ArtifactError::Unauthorized);
        }
        self.write_prepared_with_authority_blocking(
            scope,
            identity,
            "application/vnd.cowd.process-closure+json",
            payload,
            origin,
            Some(authority),
        )
    }

    fn write_prepared_with_authority_blocking(
        &self,
        scope: &str,
        identity: &str,
        media_type: &str,
        payload: &[u8],
        origin: &PreparedArtifactOrigin,
        authority: Option<&crate::ProcessClosurePublicationAuthority>,
    ) -> Result<ArtifactRef, ArtifactError> {
        origin.validate().map_err(ArtifactError::Metadata)?;
        if scope.trim().is_empty() || scope == "public" || identity.trim().is_empty() {
            return Err(ArtifactError::Unauthorized);
        }
        if scope.len() > 1024 || identity.len() > 1024 || media_type.len() > 256 {
            return Err(ArtifactError::Metadata(
                "prepared publication metadata exceeds bounded identity".into(),
            ));
        }
        if payload.len() as u64 > self.inner.config.max_object_bytes {
            return Err(ArtifactError::ObjectTooLarge);
        }
        let sha256 = format!("sha256:{:x}", Sha256::digest(payload));
        let artifact_id = format!(
            "art_{:x}",
            Sha256::digest(
                serde_json::to_vec(&(scope, identity, media_type, &sha256, origin))
                    .map_err(|_| ArtifactError::InvalidSelector)?
            )
        );
        if let Some(existing) = self
            .inner
            .repository
            .record(&artifact_id)
            .map_err(ArtifactError::Metadata)?
        {
            if existing.sha256 != sha256
                || existing.bytes != payload.len() as u64
                || existing.media_type != media_type
                || existing.visibility_scope != scope
                || existing.prepared_origin.as_ref() != Some(origin)
            {
                return Err(ArtifactError::Metadata(
                    "existing prepared publication identity conflict".into(),
                ));
            }
            if let Some(authority) = authority {
                if !authority.authorizes(&existing) {
                    return Err(ArtifactError::Unauthorized);
                }
            }
            let reference = existing.content_reference();
            let bytes = if authority.is_some() {
                self.read_process_closure_blocking(&reference, scope, origin)?
                    .body
            } else {
                self.read_prepared_blocking(&reference, scope, origin)?
            };
            if bytes != payload {
                return Err(ArtifactError::Metadata(
                    "existing prepared publication bytes conflict".into(),
                ));
            }
            return Ok(reference);
        }
        fs::create_dir_all(&self.inner.staging_root)?;
        let staging_path = self
            .inner
            .staging_root
            .join(format!("{}.part", Uuid::new_v4().simple()));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging_path)?;
        let mut staging = StagingOwner::new(staging_path);
        #[cfg(test)]
        {
            self.bind_staging_fault(&staging.path);
            self.inject_staging_fault("write", &staging.path, Some(&mut file));
        }
        let written = (|| -> Result<(), ArtifactError> {
            file.write_all(payload)?;
            #[cfg(test)]
            self.inject_staging_fault("sync", &staging.path, Some(&mut file));
            file.sync_all()?;
            Ok(())
        })();
        drop(file);
        if let Err(error) = written {
            return staging.settle(Err(error));
        }
        self.publish_staged_with_identity(
            ArtifactWriteDescriptor {
                media_type: media_type.into(),
                visibility_scope: scope.into(),
                expected_bytes: Some(payload.len() as u64),
                original_name: None,
            },
            staging,
            payload.len() as u64,
            sha256,
            artifact_id,
            Some(origin),
            authority,
        )
    }

    fn publish_staged(
        &self,
        descriptor: ArtifactWriteDescriptor,
        staging: StagingOwner,
        bytes: u64,
        sha256: String,
    ) -> Result<ArtifactRef, ArtifactError> {
        self.publish_staged_with_identity(
            descriptor,
            staging,
            bytes,
            sha256,
            format!("art_{}", Uuid::new_v4().simple()),
            None,
            None,
        )
    }

    fn publish_staged_with_identity(
        &self,
        descriptor: ArtifactWriteDescriptor,
        mut staging: StagingOwner,
        bytes: u64,
        sha256: String,
        artifact_id: String,
        origin: Option<&PreparedArtifactOrigin>,
        authority: Option<&crate::ProcessClosurePublicationAuthority>,
    ) -> Result<ArtifactRef, ArtifactError> {
        let staging_path = staging.path.clone();
        let result = (|| -> Result<ArtifactRef, ArtifactError> {
            if bytes > self.inner.config.max_object_bytes {
                return Err(ArtifactError::ObjectTooLarge);
            }
            let now = now_ms();
            let tier = if bytes <= self.inner.config.compact_threshold_bytes {
                ArtifactObjectTier::Compact
            } else {
                ArtifactObjectTier::Blob
            };
            #[cfg(test)]
            self.inject_staging_fault("compact_read", &staging_path, None);
            let compact_body = if tier == ArtifactObjectTier::Compact {
                Some(fs::read(&staging_path)?)
            } else {
                None
            };
            let token = Uuid::new_v4().simple().to_string();
            let reservation = ArtifactObjectRecord {
                generation: Uuid::new_v4().simple().to_string(),
                sha256: sha256.clone(),
                bytes,
                tier,
                compact_body,
                created_at_ms: now,
            };
            let reserved = match self.inner.repository.reserve_attempt(
                &reservation,
                self.inner.config.total_quota_bytes,
                &token,
                &artifact_id,
            ) {
                Ok(object) => object,
                Err(error) => {
                    let primary = ArtifactError::Metadata(error);
                    let compensation =
                        self.inner
                            .repository
                            .reservation(&token)
                            .and_then(|attempt| {
                                if let Some(attempt) = attempt.filter(|attempt| {
                                    attempt.state == ArtifactReservationState::Active
                                }) {
                                    self.inner.repository.release_attempt(
                                        &token,
                                        &attempt.sha256,
                                        &attempt.generation,
                                    )?;
                                }
                                Ok(())
                            });
                    return Err(match compensation {
                        Ok(()) => primary,
                        Err(error) => retain_primary_error(
                            primary,
                            "reservation_compensation_unconfirmed",
                            error,
                        ),
                    });
                }
            };
            let publish = || -> Result<ArtifactRef, ArtifactError> {
                if reserved.tier == ArtifactObjectTier::Blob {
                    let target = self.generation_path(&sha256, &reserved.generation);
                    if let Some(parent) = target.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::rename(&staging_path, &target)?;
                    let mut parent = target.parent();
                    while let Some(directory) = parent {
                        fs::File::open(directory)?.sync_all()?;
                        if directory == self.inner.blob_root {
                            break;
                        }
                        parent = directory.parent();
                    }
                } else {
                    fs::remove_file(&staging_path)?;
                }
                let record = ArtifactRecord {
                    record_class: if origin.is_some() {
                        ArtifactRecordClass::PrivatePrepared
                    } else {
                        ArtifactRecordClass::Live
                    },
                    prepared_origin: origin.cloned(),
                    artifact_id: artifact_id.clone(),
                    sha256: sha256.clone(),
                    bytes,
                    media_type: descriptor.media_type,
                    visibility_scope: descriptor.visibility_scope,
                    tier: reserved.tier.clone(),
                    created_at_ms: now,
                    last_access_at_ms: now,
                };
                let pin =
                    origin.map(|_| (ARTIFACT_PREPARED_PIN_OWNER, ARTIFACT_PERMANENT_PIN_UNTIL_MS));
                if matches!(
                    origin,
                    Some(PreparedArtifactOrigin::ToolEffectClosure { .. })
                ) {
                    let authority = authority.ok_or(ArtifactError::Unauthorized)?;
                    if !authority.authorizes(&record) {
                        return Err(ArtifactError::Unauthorized);
                    }
                    self.inner.repository.publish_process_closure_attempt(
                        &record,
                        &reserved.generation,
                        &token,
                        pin,
                        authority,
                    )
                } else {
                    if authority.is_some() {
                        return Err(ArtifactError::Unauthorized);
                    }
                    self.inner.repository.publish_attempt(
                        &record,
                        &reserved.generation,
                        &token,
                        pin,
                    )
                }
                .map_err(ArtifactError::Metadata)?;
                Ok(record.content_reference())
            };
            match publish() {
                Ok(reference) => Ok(reference),
                Err(primary) => {
                    let compensation =
                        self.inner
                            .repository
                            .reservation(&token)
                            .and_then(|attempt| {
                                if let Some(attempt) = attempt.filter(|attempt| {
                                    attempt.state == ArtifactReservationState::Active
                                }) {
                                    if attempt.sha256 != sha256
                                        || attempt.generation != reserved.generation
                                    {
                                        return Err(
                                            "reservation identity changed during compensation"
                                                .into(),
                                        );
                                    }
                                    self.inner.repository.release_attempt(
                                        &token,
                                        &sha256,
                                        &reserved.generation,
                                    )?;
                                }
                                Ok(())
                            });
                    Err(match compensation {
                        Ok(()) => primary,
                        Err(error) => retain_primary_error(
                            primary,
                            "publication_compensation_unconfirmed",
                            error,
                        ),
                    })
                }
            }
        })();
        staging.settle(result)
    }

    fn generation_path(&self, sha256: &str, generation: &str) -> PathBuf {
        self.blob_path(sha256).join(generation)
    }

    fn blob_path(&self, sha256: &str) -> PathBuf {
        let digest = sha256.strip_prefix("sha256:").unwrap_or(sha256);
        self.inner
            .blob_root
            .join("objects")
            .join(&digest[..digest.len().min(2)])
            .join(digest)
    }
}

#[async_trait]
impl ArtifactReadPort for ArtifactStore {
    async fn read_artifact(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>, ArtifactError> {
        self.read(artifact, authorized_scope, range).await
    }
}

impl ArtifactMetadataPort for ArtifactStore {
    fn artifact_stats(&self) -> Result<ArtifactStoreStats, ArtifactError> {
        self.stats()
    }
}

impl ArtifactGcPort for ArtifactStore {
    fn pin_artifact(
        &self,
        artifact: &ArtifactRef,
        owner: &str,
        until_ms: u64,
    ) -> Result<(), ArtifactError> {
        self.pin(artifact, owner, until_ms)
    }

    fn unpin_artifact(&self, artifact: &ArtifactRef, owner: &str) -> Result<(), ArtifactError> {
        self.unpin(artifact, owner)
    }

    fn delete_artifact(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
    ) -> Result<(), ArtifactError> {
        self.delete(artifact, authorized_scope)
    }

    fn collect_artifact_garbage(&self, limit: usize) -> Result<ArtifactGcReport, ArtifactError> {
        self.collect_garbage(limit)
    }
}

#[async_trait]
pub trait ArtifactWriteSink: Send {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactError>;
    async fn finish(&mut self) -> Result<ArtifactRef, ArtifactError>;
    async fn abort(&mut self) -> Result<(), ArtifactError>;
}

struct LocalArtifactWriter {
    store: ArtifactStore,
    descriptor: ArtifactWriteDescriptor,
    staging: Option<StagingOwner>,
    file: Option<tokio::fs::File>,
    hasher: Sha256,
    bytes: u64,
    artifact_id: Option<String>,
    closed: bool,
}

impl Drop for LocalArtifactWriter {
    fn drop(&mut self) {
        self.file.take();
        self.staging.take();
    }
}

impl LocalArtifactWriter {
    fn fail(&mut self, primary: ArtifactError) -> ArtifactError {
        self.closed = true;
        self.file.take();
        match self.staging.as_mut() {
            Some(staging) => staging.settle::<()>(Err(primary)).unwrap_err(),
            None => primary,
        }
    }
}

#[async_trait]
impl ArtifactWriteSink for LocalArtifactWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactError> {
        if self.closed {
            return Err(ArtifactError::WriterClosed);
        }
        let next_bytes = self.bytes.saturating_add(chunk.len() as u64);
        if next_bytes > self.store.inner.config.max_object_bytes {
            return Err(self.fail(ArtifactError::ObjectTooLarge));
        }
        self.closed = true;
        let written = self
            .file
            .as_mut()
            .ok_or(ArtifactError::WriterClosed)?
            .write_all(chunk)
            .await;
        if let Err(error) = written {
            return Err(self.fail(error.into()));
        }
        self.bytes = next_bytes;
        self.hasher.update(chunk);
        self.closed = false;
        Ok(())
    }

    async fn finish(&mut self) -> Result<ArtifactRef, ArtifactError> {
        if self.closed {
            return Err(ArtifactError::WriterClosed);
        }
        self.closed = true;
        let mut file = self.file.take().ok_or(ArtifactError::WriterClosed)?;
        let flushed = match file.flush().await {
            Ok(()) => file.sync_all().await,
            Err(error) => Err(error),
        };
        drop(file);
        if let Err(error) = flushed {
            return Err(self.fail(error.into()));
        }
        let descriptor = self.descriptor.clone();
        let staging = self.staging.take().ok_or(ArtifactError::WriterClosed)?;
        let bytes = self.bytes;
        let sha256 = format!("sha256:{:x}", self.hasher.clone().finalize());
        let store = self.store.clone();
        let artifact_id = self.artifact_id.take();
        tokio::task::spawn_blocking(move || match artifact_id {
            Some(artifact_id) => store.publish_staged_with_identity(
                descriptor,
                staging,
                bytes,
                sha256,
                artifact_id,
                None,
                None,
            ),
            None => store.publish_staged(descriptor, staging, bytes, sha256),
        })
        .await
        .map_err(|error| ArtifactError::Blocking(error.to_string()))?
    }

    async fn abort(&mut self) -> Result<(), ArtifactError> {
        self.file.take();
        self.closed = true;
        match self.staging.as_mut() {
            Some(staging) => staging.cleanup(),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Default)]
pub struct EphemeralArtifactRepository {
    state: Mutex<EphemeralArtifactState>,
}

#[derive(Debug, Default)]
struct EphemeralArtifactState {
    objects: HashMap<String, ArtifactObjectRecord>,
    reservations: HashMap<String, u64>,
    deleting: std::collections::BTreeSet<String>,
    ready: std::collections::BTreeSet<String>,
    records: std::collections::BTreeMap<String, ArtifactRecord>,
    attempts: std::collections::BTreeMap<String, ArtifactReservation>,
    catalog_sequence: u64,
    creation_sequence: HashMap<String, u64>,
    catalog_invalidations: std::collections::BTreeMap<String, i64>,
    pins: HashMap<(String, String), u64>,
}

impl ArtifactMetadataRepository for EphemeralArtifactRepository {
    fn reserve_attempt(
        &self,
        object: &ArtifactObjectRecord,
        quota: u64,
        token: &str,
        publication_id: &str,
    ) -> Result<ArtifactObjectRecord, String> {
        validate_attempt_identity(token, publication_id)?;
        let mut state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        if let Some(attempt) = state.attempts.get(token) {
            if attempt.sha256 != object.sha256
                || attempt.publication_id != publication_id
                || attempt.state != ArtifactReservationState::Active
            {
                return Err("artifact attempt conflict or finalized".into());
            }
            return state
                .objects
                .get(&attempt.sha256)
                .filter(|object| object.generation == attempt.generation)
                .cloned()
                .ok_or("artifact attempt generation missing".into());
        }
        if state.deleting.contains(&object.sha256) {
            return Err("artifact generation is being collected".into());
        }
        let reserved = if let Some(existing) = state.objects.get(&object.sha256) {
            if existing.bytes != object.bytes {
                return Err("artifact object conflict".into());
            }
            existing.clone()
        } else {
            let used = state.objects.values().try_fold(0u64, |total, object| {
                total
                    .checked_add(object.bytes)
                    .ok_or("artifact quota overflow")
            })?;
            if object.bytes > quota.saturating_sub(used) {
                return Err("artifact quota exhausted".into());
            }
            object.clone()
        };
        let count = state
            .reservations
            .get(&object.sha256)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or("artifact reservations overflow")?;
        state
            .objects
            .entry(object.sha256.clone())
            .or_insert_with(|| reserved.clone());
        state.reservations.insert(object.sha256.clone(), count);
        state.attempts.insert(
            token.into(),
            ArtifactReservation {
                token: token.into(),
                sha256: object.sha256.clone(),
                generation: reserved.generation.clone(),
                publication_id: publication_id.into(),
                state: ArtifactReservationState::Active,
            },
        );
        Ok(reserved)
    }

    fn reservation(&self, token: &str) -> Result<Option<ArtifactReservation>, String> {
        Ok(self
            .state
            .lock()
            .map_err(|_| "artifact lock poisoned")?
            .attempts
            .get(token)
            .cloned())
    }

    fn publish_attempt(
        &self,
        record: &ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
    ) -> Result<(), String> {
        if matches!(
            record.prepared_origin,
            Some(PreparedArtifactOrigin::ToolEffectClosure { .. })
        ) {
            return Err("reserved process closure publication".into());
        }
        self.publish_attempt_inner(record, generation, token, pin)
    }

    fn process_closure_record(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, String> {
        let state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        state
            .records
            .get(artifact_id)
            .map(|record| {
                closure_record_metadata_bytes(record)?;
                Ok(record.clone())
            })
            .transpose()
    }

    fn process_closure_object(&self, sha256: &str) -> Result<Option<ArtifactObjectRecord>, String> {
        let state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        state
            .objects
            .get(sha256)
            .map(|object| {
                if object.bytes > 32768
                    || object.generation.len() > 128
                    || object
                        .compact_body
                        .as_ref()
                        .is_some_and(|body| body.len() > 32768)
                {
                    return Err("process closure object bounds exceeded".into());
                }
                Ok(object.clone())
            })
            .transpose()
    }

    fn publish_process_closure_attempt(
        &self,
        record: &ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
        authority: &crate::ProcessClosurePublicationAuthority,
    ) -> Result<(), String> {
        if !matches!(
            record.prepared_origin,
            Some(PreparedArtifactOrigin::ToolEffectClosure { .. })
        ) || !authority.authorizes(record)
        {
            return Err("process closure publication authority mismatch".into());
        }
        self.publish_attempt_inner(record, generation, token, pin)
    }

    fn release_attempt(&self, token: &str, sha256: &str, generation: &str) -> Result<bool, String> {
        let mut state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        let Some(attempt) = state.attempts.get(token) else {
            return Ok(false);
        };
        if attempt.sha256 != sha256 || attempt.generation != generation {
            return Err("artifact release identity conflict".into());
        }
        if attempt.state != ArtifactReservationState::Active {
            return Ok(false);
        }
        if !state
            .objects
            .get(sha256)
            .is_some_and(|object| object.generation == generation)
        {
            return Err("artifact release generation missing".into());
        }
        let count = state
            .reservations
            .get(sha256)
            .copied()
            .unwrap_or(0)
            .checked_sub(1)
            .ok_or("artifact reservation underflow")?;
        state.reservations.insert(sha256.into(), count);
        state
            .attempts
            .get_mut(token)
            .ok_or("artifact attempt missing")?
            .state = ArtifactReservationState::Released;
        if count == 0 {
            state.ready.insert(sha256.into());
        }
        Ok(true)
    }

    fn active_attempts_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ArtifactReservation>, String> {
        let state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        Ok(state
            .attempts
            .range(after.unwrap_or("").to_string()..)
            .filter(|(token, attempt)| {
                after.is_none_or(|after| token.as_str() > after)
                    && attempt.state == ArtifactReservationState::Active
            })
            .take(limit.min(100))
            .map(|(_, attempt)| attempt.clone())
            .collect())
    }

    fn prepared_high_water(&self) -> Result<Option<String>, String> {
        Ok(self
            .state
            .lock()
            .map_err(|_| "artifact lock poisoned")?
            .records
            .iter()
            .rev()
            .find(|(_, record)| record.record_class == ArtifactRecordClass::PrivatePrepared)
            .map(|(identity, _)| identity.clone()))
    }

    fn prepared_page(
        &self,
        after: Option<&str>,
        through: &str,
        limit: usize,
    ) -> Result<PreparedArtifactPage, String> {
        if after.is_some_and(|after| after >= through) {
            return Ok(PreparedArtifactPage {
                complete: true,
                ..PreparedArtifactPage::default()
            });
        }
        let state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        let mut page = PreparedArtifactPage::default();
        let mut bytes = 0usize;
        for (identity, record) in state
            .records
            .range(after.unwrap_or("").to_string()..=through.to_string())
        {
            if after.is_some_and(|after| identity.as_str() <= after)
                || record.record_class != ArtifactRecordClass::PrivatePrepared
            {
                continue;
            }
            if page.candidates.len() >= limit.clamp(1, 100) {
                return Ok(page);
            }
            let encoded = prepared_metadata_bytes(record)?;
            if bytes + encoded > 1024 * 1024 {
                if page.candidates.is_empty() {
                    return Err("prepared metadata exceeds page budget".into());
                }
                return Ok(page);
            }
            bytes += encoded;
            let object = state
                .objects
                .get(&record.sha256)
                .ok_or("prepared object missing")?;
            page.candidates.push(PreparedArtifactCandidate {
                record: record.clone(),
                generation: object.generation.clone(),
            });
            page.last_examined_id = Some(identity.clone());
        }
        page.complete = true;
        Ok(page)
    }

    fn retire_prepared(
        &self,
        candidate: &PreparedArtifactCandidate,
    ) -> Result<PreparedArtifactRetirement, String> {
        let mut state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        let expected = &candidate.record;
        let Some(record) = state.records.get(&expected.artifact_id) else {
            return Ok(PreparedArtifactRetirement::Missing);
        };
        if expected.record_class != ArtifactRecordClass::PrivatePrepared
            || record.record_class != ArtifactRecordClass::PrivatePrepared
            || !same_publication(record, expected)
            || record
                .prepared_origin
                .as_ref()
                .is_none_or(|origin| origin.validate().is_err())
            || !state
                .objects
                .get(&record.sha256)
                .is_some_and(|object| object.generation == candidate.generation)
            || state.pins.get(&(
                record.artifact_id.clone(),
                ARTIFACT_PREPARED_PIN_OWNER.into(),
            )) != Some(&ARTIFACT_PERMANENT_PIN_UNTIL_MS)
            || state.pins.keys().any(|(identity, owner)| {
                identity == &record.artifact_id && owner != ARTIFACT_PREPARED_PIN_OWNER
            })
        {
            return Ok(PreparedArtifactRetirement::Retained);
        }
        let scope = record.visibility_scope.clone();
        state.records.remove(&expected.artifact_id);
        state.creation_sequence.remove(&expected.artifact_id);
        state.pins.remove(&(
            expected.artifact_id.clone(),
            ARTIFACT_PREPARED_PIN_OWNER.into(),
        ));
        *state.catalog_invalidations.entry(scope).or_default() += 1;
        Ok(PreparedArtifactRetirement::Removed)
    }

    fn adopt_live(&self, expected: &ArtifactRecord) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        let record = state
            .records
            .get_mut(&expected.artifact_id)
            .ok_or("artifact adoption missing")?;
        if !same_publication(record, expected) {
            return Err("artifact adoption identity changed".into());
        }
        if record.record_class == ArtifactRecordClass::Live {
            return Ok(());
        }
        record.record_class = ArtifactRecordClass::Live;
        let scope = record.visibility_scope.clone();
        *state.catalog_invalidations.entry(scope).or_default() += 1;
        Ok(())
    }

    fn prepared_readable(
        &self,
        expected: &ArtifactRecord,
        origin: &PreparedArtifactOrigin,
    ) -> Result<bool, String> {
        origin.validate()?;
        let state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        Ok(state
            .records
            .get(&expected.artifact_id)
            .is_some_and(|record| {
                same_publication(record, expected)
                    && (!matches!(origin, PreparedArtifactOrigin::ToolEffectClosure { .. })
                        || (record.record_class == ArtifactRecordClass::PrivatePrepared
                            && expected.record_class == ArtifactRecordClass::PrivatePrepared))
                    && record.prepared_origin.as_ref() == Some(origin)
                    && !state.deleting.contains(&record.sha256)
                    && state.ready.contains(&record.sha256)
                    && (record.record_class == ArtifactRecordClass::Live
                        || state.pins.get(&(
                            record.artifact_id.clone(),
                            ARTIFACT_PREPARED_PIN_OWNER.into(),
                        )) == Some(&ARTIFACT_PERMANENT_PIN_UNTIL_MS))
            }))
    }
    fn claim_object_deletion(&self, sha256: &str, generation: &str) -> Result<bool, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "artifact lock poisoned".to_string())?;
        if !state
            .objects
            .get(sha256)
            .is_some_and(|object| object.generation == generation)
            || state.reservations.get(sha256).copied().unwrap_or_default() != 0
            || !state.ready.contains(sha256)
            || state.records.values().any(|record| record.sha256 == sha256)
        {
            return Ok(false);
        }
        state.deleting.insert(sha256.into());
        Ok(true)
    }

    fn finish_object_deletion(&self, sha256: &str, generation: &str) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "artifact lock poisoned".to_string())?;
        if !state
            .objects
            .get(sha256)
            .is_some_and(|object| object.generation == generation)
        {
            return Ok(());
        }
        if !state.deleting.contains(sha256)
            || state.records.values().any(|record| record.sha256 == sha256)
        {
            return Err("artifact deletion is not claimed".into());
        }
        state.objects.remove(sha256);
        state.ready.remove(sha256);
        state.deleting.remove(sha256);
        state.reservations.remove(sha256);
        Ok(())
    }

    fn object(&self, sha256: &str) -> Result<Option<ArtifactObjectRecord>, String> {
        Ok(self
            .state
            .lock()
            .map_err(|error| error.to_string())?
            .objects
            .get(sha256)
            .cloned())
    }

    fn checkpoint_object_metadata(
        &self, sha256: &str,
    ) -> Result<Option<(ArtifactObjectRecord, String)>, String> {
        let state = self.state.lock().map_err(|error| error.to_string())?;
        if state.objects.get(sha256).is_some_and(|object| object.tier == ArtifactObjectTier::Compact
            && object.compact_body.as_ref().is_none_or(|body| body.len() as u64 != object.bytes))
        {
            return Err("prepared compact checkpoint body is missing".into());
        }
        Ok(state.objects.get(sha256).map(|object| (
            ArtifactObjectRecord {
                generation: object.generation.clone(), sha256: object.sha256.clone(),
                bytes: object.bytes, tier: object.tier.clone(), compact_body: None,
                created_at_ms: object.created_at_ms,
            },
            object.generation.clone(),
        )))
    }

    fn catalog_page(&self, query: ArtifactCatalogQuery) -> Result<ArtifactCatalogPage, String> {
        let state = self.state.lock().map_err(|error| error.to_string())?;
        let revisions = query
            .scopes
            .iter()
            .map(|scope| {
                (
                    scope.clone(),
                    *state.catalog_invalidations.get(scope).unwrap_or(&0),
                )
            })
            .collect();
        let snapshot = query.snapshot.unwrap_or(ArtifactCatalogSnapshot {
            fence: format!("ephemeral:{}", state.catalog_sequence),
            revisions,
        });
        let current = query
            .scopes
            .iter()
            .map(|scope| {
                (
                    scope.clone(),
                    *state.catalog_invalidations.get(scope).unwrap_or(&0),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        if snapshot.revisions != current {
            return Err("Artifact catalog source changed; start a fresh discovery".into());
        }
        let through = snapshot
            .fence
            .strip_prefix("ephemeral:")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|sequence| *sequence <= state.catalog_sequence)
            .ok_or("invalid Artifact catalog snapshot")?;
        let needle = query.query.to_lowercase();
        let limit = query.limit.clamp(1, 128);
        let mut records = state
            .records
            .values()
            .filter(|record| {
                record.record_class == ArtifactRecordClass::Live
                    && query.scopes.contains(&record.visibility_scope)
                    && state
                        .creation_sequence
                        .get(&record.artifact_id)
                        .is_some_and(|sequence| *sequence <= through)
                    && query
                        .after_id
                        .as_ref()
                        .is_none_or(|after| record.artifact_id > *after)
                    && (needle.is_empty()
                        || format!(
                            "{} {} {}",
                            record.artifact_id, record.sha256, record.media_type
                        )
                        .to_lowercase()
                        .contains(&needle))
            })
            .take(limit + 1)
            .cloned()
            .collect::<Vec<_>>();
        let has_more = records.len() > limit;
        records.truncate(limit);
        let next_id = has_more.then(|| records.last().expect("nonempty page").artifact_id.clone());
        Ok(ArtifactCatalogPage {
            records,
            snapshot,
            next_id,
        })
    }

    fn record(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, String> {
        Ok(self
            .state
            .lock()
            .map_err(|error| error.to_string())?
            .records
            .get(artifact_id)
            .cloned())
    }

    fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(), String> {
        if let Some(record) = self
            .state
            .lock()
            .map_err(|error| error.to_string())?
            .records
            .get_mut(artifact_id)
        {
            record.last_access_at_ms = at_ms;
        }
        Ok(())
    }

    fn remove_record(&self, artifact_id: &str) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        if state
            .records
            .get(artifact_id)
            .is_some_and(|record| record.record_class == ArtifactRecordClass::PrivatePrepared)
        {
            return Err("private prepared retirement requires exact roots".into());
        }
        if state
            .pins
            .iter()
            .any(|((identity, _), until)| identity == artifact_id && *until > now_ms())
        {
            return Err("artifact is pinned".into());
        }
        if let Some(record) = state.records.remove(artifact_id) {
            *state
                .catalog_invalidations
                .entry(record.visibility_scope)
                .or_default() += 1;
            state.creation_sequence.remove(artifact_id);
        }
        state.pins.retain(|(id, _), _| id != artifact_id);
        Ok(())
    }

    fn unreferenced_objects_before(
        &self,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<ArtifactObjectRecord>, String> {
        let state = self.state.lock().map_err(|error| error.to_string())?;
        let mut objects = state
            .objects
            .values()
            .filter(|object| {
                object.created_at_ms <= before_ms
                    && state.ready.contains(&object.sha256)
                    && state
                        .reservations
                        .get(&object.sha256)
                        .copied()
                        .unwrap_or_default()
                        == 0
                    && !state
                        .records
                        .values()
                        .any(|record| record.sha256 == object.sha256)
            })
            .take(limit.min(100))
            .map(|object| ArtifactObjectRecord {
                generation: object.generation.clone(),
                sha256: object.sha256.clone(),
                bytes: object.bytes,
                tier: object.tier.clone(),
                compact_body: None,
                created_at_ms: object.created_at_ms,
            })
            .collect::<Vec<_>>();
        objects.sort_by_key(|object| object.created_at_ms);
        objects.truncate(limit.min(100));
        Ok(objects)
    }

    fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        if owner == ARTIFACT_PREPARED_PIN_OWNER {
            return Err("reserved prepared protection".into());
        }
        if !state.records.contains_key(artifact_id) {
            return Err("artifact record does not exist".to_string());
        }
        let record = state
            .records
            .get_mut(artifact_id)
            .ok_or("artifact missing")?;
        let newly_live = record.record_class == ArtifactRecordClass::PrivatePrepared;
        record.record_class = ArtifactRecordClass::Live;
        let scope = record.visibility_scope.clone();
        if newly_live {
            *state.catalog_invalidations.entry(scope).or_default() += 1;
        }
        state
            .pins
            .entry((artifact_id.to_string(), owner.to_string()))
            .and_modify(|current| *current = (*current).max(until_ms))
            .or_insert(until_ms);
        Ok(())
    }

    fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String> {
        if owner == ARTIFACT_PREPARED_PIN_OWNER {
            return Err("reserved prepared protection".into());
        }
        self.state
            .lock()
            .map_err(|error| error.to_string())?
            .pins
            .remove(&(artifact_id.to_string(), owner.to_string()));
        Ok(())
    }

    fn is_pinned(&self, artifact_id: &str, at_ms: u64) -> Result<bool, String> {
        Ok(self
            .state
            .lock()
            .map_err(|error| error.to_string())?
            .pins
            .iter()
            .any(|((id, _), until_ms)| id == artifact_id && *until_ms > at_ms))
    }

    fn stats(&self, at_ms: u64) -> Result<ArtifactStoreStats, String> {
        let state = self.state.lock().map_err(|error| error.to_string())?;
        let physical_bytes = state.objects.values().map(|object| object.bytes).sum();
        let compact_bytes = state
            .objects
            .values()
            .filter(|object| object.tier == ArtifactObjectTier::Compact)
            .map(|object| object.bytes)
            .sum();
        let blob_bytes = state
            .objects
            .values()
            .filter(|object| object.tier == ArtifactObjectTier::Blob)
            .map(|object| object.bytes)
            .sum();
        Ok(ArtifactStoreStats {
            objects: state.objects.len() as u64,
            artifacts: state.records.len() as u64,
            physical_bytes,
            compact_bytes,
            blob_bytes,
            pins: state
                .pins
                .values()
                .filter(|until_ms| **until_ms > at_ms)
                .count() as u64,
        })
    }
}

#[cfg(test)]
impl EphemeralArtifactRepository {
    fn put_record(&self, record: &ArtifactRecord) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        if state.deleting.contains(&record.sha256) {
            return Err("artifact generation is being collected".into());
        }
        if !state.objects.contains_key(&record.sha256) {
            return Err("artifact object does not exist".to_string());
        }
        if state.records.contains_key(&record.artifact_id) {
            return Err("artifact record already exists".to_string());
        }
        state.catalog_sequence += 1;
        let sequence = state.catalog_sequence;
        state
            .creation_sequence
            .insert(record.artifact_id.clone(), sequence);
        state
            .records
            .insert(record.artifact_id.clone(), record.clone());
        Ok(())
    }
}

fn validate_attempt_identity(token: &str, publication_id: &str) -> Result<(), String> {
    if token.is_empty()
        || token.len() > 128
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || publication_id.is_empty()
        || publication_id.len() > 128
    {
        return Err("invalid bounded artifact attempt".into());
    }
    Ok(())
}

fn same_publication(existing: &ArtifactRecord, expected: &ArtifactRecord) -> bool {
    existing.content_reference() == expected.content_reference()
        && existing.prepared_origin == expected.prepared_origin
}

fn prepared_metadata_bytes(record: &ArtifactRecord) -> Result<usize, String> {
    Ok(
        serde_json::to_vec(&(record.content_reference(), &record.prepared_origin))
            .map_err(|error| error.to_string())?
            .len()
            + 256,
    )
}

fn publish_ephemeral_record(
    state: &mut EphemeralArtifactState,
    record: &ArtifactRecord,
    generation: &str,
    pin: Option<(&str, u64)>,
) -> Result<(), String> {
    let object = state
        .objects
        .get(&record.sha256)
        .ok_or("artifact reservation missing")?;
    if object.generation != generation
        || object.bytes != record.bytes
        || object.tier != record.tier
        || state.deleting.contains(&record.sha256)
    {
        return Err("artifact generation conflict".into());
    }
    if record.record_class == ArtifactRecordClass::PrivatePrepared {
        record
            .prepared_origin
            .as_ref()
            .ok_or("prepared origin missing")?
            .validate()?;
        if pin != Some((ARTIFACT_PREPARED_PIN_OWNER, ARTIFACT_PERMANENT_PIN_UNTIL_MS)) {
            return Err("prepared protection missing".into());
        }
    }
    let count = state
        .reservations
        .get(&record.sha256)
        .copied()
        .unwrap_or(0)
        .checked_sub(1)
        .ok_or("artifact reservation underflow")?;
    if let Some(existing) = state.records.get(&record.artifact_id) {
        if !same_publication(existing, record) {
            return Err("artifact publication identity conflict".into());
        }
    } else {
        let sequence = state
            .catalog_sequence
            .checked_add(1)
            .ok_or("artifact sequence overflow")?;
        state.catalog_sequence = sequence;
        state
            .creation_sequence
            .insert(record.artifact_id.clone(), sequence);
        state
            .records
            .insert(record.artifact_id.clone(), record.clone());
    }
    if let Some((owner, until)) = pin {
        if owner != ARTIFACT_PREPARED_PIN_OWNER
            || state.records[&record.artifact_id].record_class
                == ArtifactRecordClass::PrivatePrepared
        {
            let current = state
                .pins
                .entry((record.artifact_id.clone(), owner.into()))
                .or_default();
            *current = (*current).max(until);
        }
    }
    state.ready.insert(record.sha256.clone());
    state.reservations.insert(record.sha256.clone(), count);
    Ok(())
}

fn parse_selector(selector: &str) -> Result<&str, ArtifactError> {
    selector
        .strip_prefix(ARTIFACT_SELECTOR_PREFIX)
        .filter(|id| !id.is_empty() && !id.contains('/'))
        .ok_or(ArtifactError::InvalidSelector)
}

fn validate_ref(reference: &ArtifactRef, record: &ArtifactRecord) -> Result<(), ArtifactError> {
    if !reference.is_durable()
        || reference.sha256 != record.sha256
        || reference.bytes != record.bytes
        || reference.media_type != record.media_type
        || reference.visibility_scope != record.visibility_scope
    {
        return Err(ArtifactError::Metadata(
            "artifact reference does not match durable metadata".to_string(),
        ));
    }
    Ok(())
}

fn normalize_range(requested: Option<Range<u64>>, bytes: u64) -> Result<Range<u64>, ArtifactError> {
    let range = requested.unwrap_or(0..bytes);
    if range.start > range.end || range.end > bytes {
        return Err(ArtifactError::Io("artifact range is invalid".to_string()));
    }
    Ok(range)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    fn r1_artifacts(
        label: &str,
        compact: u64,
    ) -> (
        super::ArtifactStore,
        std::sync::Arc<super::EphemeralArtifactRepository>,
    ) {
        let base = std::env::var_os("COWD_TEST_FIXTURE_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        assert!(base.is_absolute());
        assert!(!base
            .components()
            .any(|component| component == std::path::Component::ParentDir));
        let metadata = std::fs::symlink_metadata(&base).unwrap();
        assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
        assert_eq!(std::fs::canonicalize(&base).unwrap(), base);
        assert!(
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        );
        let root = base.join(format!("{label}-{}", uuid::Uuid::new_v4()));
        assert_eq!(root.parent(), Some(base.as_path()));
        std::fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let repository = std::sync::Arc::new(super::EphemeralArtifactRepository::default());
        let store = super::ArtifactStore::new(
            root,
            repository.clone(),
            super::ArtifactStoreConfig {
                compact_threshold_bytes: compact,
                max_object_bytes: 1024 * 1024,
                total_quota_bytes: 2 * 1024 * 1024,
                gc_high_water_bytes: 1,
                gc_low_water_bytes: 0,
                orphan_grace_ms: 0,
            },
        )
        .unwrap();
        (store, repository)
    }

    #[test]
    fn prepared_observation_preserves_verified_reads_without_touch_or_adoption() {
        use super::*;
        for compact in [8, 400_000] {
            let root = tempfile::tempdir().unwrap();
            let repository = Arc::new(EphemeralArtifactRepository::default());
            let store = ArtifactStore::new(
                root.path(),
                repository.clone(),
                ArtifactStoreConfig {
                    compact_threshold_bytes: compact,
                    ..ArtifactStoreConfig::default()
                },
            )
            .unwrap();
            let origin = PreparedArtifactOrigin::EventBody {
                stream_id: "graph:observation".into(),
                idempotency_key: "original".into(),
                kind: "execution_graph.planned".into(),
            };
            let body = vec![b'x'; 300_000];
            let artifact = store
                .write_pre_pinned_blocking("session:owner", "body", &body, &origin)
                .unwrap();
            let id = parse_selector(&artifact.selector).unwrap();
            repository.touch(id, 1).unwrap();
            assert_eq!(
                store
                    .read_prepared_blocking(&artifact, "session:owner", &origin)
                    .unwrap(),
                body
            );
            assert!(repository.record(id).unwrap().unwrap().last_access_at_ms > 1);
            repository.touch(id, 2).unwrap();
            let observation = store.verified_observation_view();
            assert_eq!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                    .unwrap(),
                body
            );
            let record = repository.record(id).unwrap().unwrap();
            assert_eq!(record.last_access_at_ms, 2);
            assert_eq!(record.record_class, ArtifactRecordClass::PrivatePrepared);
            assert!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:foreign", &origin)
                    .is_err()
            );
            let foreign = PreparedArtifactOrigin::EventBody {
                stream_id: "graph:foreign".into(),
                idempotency_key: "original".into(),
                kind: "execution_graph.planned".into(),
            };
            assert!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:owner", &foreign)
                    .is_err()
            );
            let mut changed = artifact.clone();
            changed.sha256 = format!("sha256:{}", "0".repeat(64));
            assert!(
                observation
                    .read_prepared_observation_blocking(&changed, "session:owner", &origin)
                    .is_err()
            );
            changed = artifact.clone();
            changed.bytes += 1;
            assert!(
                observation
                    .read_prepared_observation_blocking(&changed, "session:owner", &origin)
                    .is_err()
            );
            changed = artifact.clone();
            changed.selector = format!("artifact://art_{}", "0".repeat(64));
            assert!(
                observation
                    .read_prepared_observation_blocking(&changed, "session:owner", &origin)
                    .is_err()
            );
            // Mutate only this real repository's test fixture, not a no-op
            // repository implementation or a new production mutation port.
            let protection = (id.to_owned(), ARTIFACT_PREPARED_PIN_OWNER.to_owned());
            let saved_pin = repository
                .state
                .lock()
                .unwrap()
                .pins
                .remove(&protection)
                .unwrap();
            assert!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                    .is_err()
            );
            repository
                .state
                .lock()
                .unwrap()
                .pins
                .insert(protection, saved_pin);
            repository
                .state
                .lock()
                .unwrap()
                .ready
                .remove(&artifact.sha256);
            assert!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                    .is_err()
            );
            repository
                .state
                .lock()
                .unwrap()
                .ready
                .insert(artifact.sha256.clone());
            repository
                .state
                .lock()
                .unwrap()
                .deleting
                .insert(artifact.sha256.clone());
            assert!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                    .is_err()
            );
            repository
                .state
                .lock()
                .unwrap()
                .deleting
                .remove(&artifact.sha256);
            assert_eq!(
                observation
                    .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                    .unwrap(),
                body
            );
            if compact == 8 {
                let object = repository.object(&artifact.sha256).unwrap().unwrap();
                let path = store.generation_path(&object.sha256, &object.generation);
                fs::write(&path, vec![b'y'; body.len()]).unwrap();
                assert!(
                    observation
                        .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                        .unwrap_err()
                        .to_string()
                        .contains("digest")
                );
                fs::remove_file(path).unwrap();
                assert!(
                    observation
                        .read_prepared_observation_blocking(&artifact, "session:owner", &origin)
                        .is_err()
                );
            }
            assert_eq!(repository.record(id).unwrap().unwrap().last_access_at_ms, 2);
        }
    }

    #[test]
    fn bounded_public_ranges_verify_the_entire_source_and_never_adopt_private_prepared() {
        for compact in [8, 1024] {
            let root = tempfile::tempdir().unwrap();
            let repository = std::sync::Arc::new(super::EphemeralArtifactRepository::default());
            let store = super::ArtifactStore::new(
                root.path(),
                repository.clone(),
                super::ArtifactStoreConfig {
                    compact_threshold_bytes: compact,
                    ..super::ArtifactStoreConfig::default()
                },
            )
            .unwrap();
            let artifact = r1_test_live(
                &store,
                "session:range",
                "source",
                b"original immutable content",
            );
            assert_eq!(
                store
                    .read_public_range_blocking(&artifact, "session:range", 0..4)
                    .unwrap(),
                b"orig"
            );
            assert!(store
                .read_public_range_blocking(&artifact, "session:foreign", 0..4)
                .is_err());
            let id = super::parse_selector(&artifact.selector)
                .unwrap()
                .to_owned();
            repository
                .state
                .lock()
                .unwrap()
                .records
                .get_mut(&id)
                .unwrap()
                .record_class = super::ArtifactRecordClass::PrivatePrepared;
            assert!(store
                .read_public_range_blocking(&artifact, "session:range", 0..4)
                .is_err());
            assert_eq!(
                repository.state.lock().unwrap().records[&id].record_class,
                super::ArtifactRecordClass::PrivatePrepared
            );
            repository
                .state
                .lock()
                .unwrap()
                .records
                .get_mut(&id)
                .unwrap()
                .record_class = super::ArtifactRecordClass::Live;
            let object = repository.state.lock().unwrap().objects[&artifact.sha256].clone();
            // Corruption outside the requested range must still be detected.
            let corrupt = b"original immutable CONTENT";
            assert_eq!(corrupt.len() as u64, artifact.bytes);
            if compact == 8 {
                std::fs::write(
                    store.generation_path(&artifact.sha256, &object.generation),
                    corrupt,
                )
                .unwrap();
            } else {
                repository
                    .state
                    .lock()
                    .unwrap()
                    .objects
                    .get_mut(&artifact.sha256)
                    .unwrap()
                    .compact_body = Some(corrupt.to_vec());
            }
            assert!(store
                .read_public_range_blocking(&artifact, "session:range", 0..4)
                .is_err());
        }
    }

    #[test]
    fn r1_content_gc_publish_pin_delete_race() {
        use super::ArtifactMetadataRepository;
        for iteration in 0..16 {
            let (store, repository) = r1_artifacts("pin-delete", 8);
            let artifact = r1_test_live(
                &store,
                "session:race",
                "prepare",
                b"actual durable race bytes",
            );
            store.unpin(&artifact, "prepare").unwrap();
            let gate = std::sync::Arc::new(std::sync::Barrier::new(3));
            let pin = {
                let gate = gate.clone();
                let store = store.clone();
                let artifact = artifact.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    store.pin(&artifact, "live", super::ARTIFACT_PERMANENT_PIN_UNTIL_MS)
                })
            };
            let delete = {
                let gate = gate.clone();
                let store = store.clone();
                let artifact = artifact.clone();
                std::thread::spawn(move || {
                    gate.wait();
                    store.delete(&artifact, "session:race")
                })
            };
            gate.wait();
            let pinned = pin.join().unwrap().is_ok();
            let deleted = delete.join().unwrap().is_ok();
            assert_ne!(pinned, deleted, "pin/delete must serialize: {iteration}");
            if pinned {
                assert_eq!(
                    store
                        .read_blocking(&artifact, "session:race", None)
                        .unwrap(),
                    b"actual durable race bytes"
                );
            } else {
                assert!(repository
                    .record(super::parse_selector(&artifact.selector).unwrap())
                    .unwrap()
                    .is_none());
            }
        }
    }

    #[test]
    fn r1_content_gc_ref_scope_and_actual_bytes_are_verified() {
        staging_error_path_matrix();
        staging_cleanup_failure_preserves_primary_and_exact_owner();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async_staging_failures_close_the_writer());
        for compact in [8, 1024] {
            let (store, repository) = r1_artifacts("content-verification", compact);
            let artifact = r1_test_live(
                &store,
                "session:verified",
                "prepare",
                b"actual immutable bytes",
            );
            assert_eq!(
                store
                    .read_blocking(&artifact, "session:verified", None)
                    .unwrap(),
                b"actual immutable bytes"
            );
            assert_eq!(
                store
                    .read_blocking(&artifact, "session:verified", Some(0..1))
                    .unwrap(),
                b"a"
            );
            assert!(store
                .read_blocking(&artifact, "session:foreign", None)
                .is_err());
            let mut forged = artifact.clone();
            forged.bytes += 1;
            assert!(store
                .read_blocking(&forged, "session:verified", None)
                .is_err());
            forged = artifact.clone();
            forged.media_type = "text/plain".into();
            assert!(store
                .read_blocking(&forged, "session:verified", None)
                .is_err());
            let object = repository.state.lock().unwrap().objects[&artifact.sha256].clone();
            if compact == 8 {
                std::fs::write(
                    store.generation_path(&artifact.sha256, &object.generation),
                    vec![b'x'; artifact.bytes as usize],
                )
                .unwrap();
            } else {
                repository
                    .state
                    .lock()
                    .unwrap()
                    .objects
                    .get_mut(&artifact.sha256)
                    .unwrap()
                    .compact_body = Some(vec![b'x'; artifact.bytes as usize]);
            }
            assert!(matches!(
                store.read_blocking(&artifact, "session:verified", None),
                Err(super::ArtifactError::Metadata(_))
            ));
            assert!(matches!(
                store.read_blocking(&artifact, "session:verified", Some(0..1)),
                Err(super::ArtifactError::Metadata(_))
            ));
        }
    }

    fn staging_error_path_matrix() {
        for compact in [8, 1024] {
            for entry in ["prepared", "path"] {
                for stage in [
                    "write",
                    "sync",
                    "compact_read",
                    "source_read",
                    "source_growth",
                ] {
                    if (stage == "compact_read" && compact == 8)
                        || (stage.starts_with("source_") && entry != "path")
                    {
                        continue;
                    }
                    let (store, repository) = r1_artifacts("staging-real-io", compact);
                    let baseline = r1_test_live(
                        &store,
                        "session:staging",
                        "audit-pin",
                        b"unchanged committed baseline",
                    );
                    fs::create_dir_all(&store.inner.staging_root).unwrap();
                    let sentinel = store.inner.staging_root.join("other-run-sentinel");
                    fs::write(&sentinel, b"not this staging owner").unwrap();
                    let source = store.inner.blob_root.join("source-input");
                    let payload = b"complete source bytes for a real publication";
                    fs::write(&source, payload).unwrap();
                    let staging_before = fs::read_dir(&store.inner.staging_root)
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .collect::<std::collections::BTreeSet<_>>();
                    let before = {
                        let state = repository.state.lock().unwrap();
                        (
                            state.objects.clone(),
                            state.attempts.clone(),
                            state.records.clone(),
                            state.pins.clone(),
                            state.reservations.clone(),
                        )
                    };
                    *store.inner.staging_fault.lock().unwrap() = Some(StagingFault {
                        stage,
                        path: None,
                        hits: 0,
                        observed: Vec::new(),
                    });
                    let foreign = store.inner.staging_root.join("different-path");
                    store.inject_staging_fault(stage, &foreign, None);
                    let origin = PreparedArtifactOrigin::EventBody {
                        stream_id: "graph:staging".into(),
                        idempotency_key: format!("{entry}-{stage}"),
                        kind: "execution_graph.planned".into(),
                    };
                    let publish = || {
                        if entry == "prepared" {
                            store.write_pre_pinned_blocking(
                                "session:staging",
                                "retry-same-origin",
                                payload,
                                &origin,
                            )
                        } else {
                            store.write_path_blocking(
                                ArtifactWriteDescriptor {
                                    media_type: "text/plain".into(),
                                    visibility_scope: "session:staging".into(),
                                    expected_bytes: Some(payload.len() as u64),
                                    original_name: None,
                                },
                                &source,
                            )
                        }
                    };
                    let error = publish().expect_err(
                        "actual OS fault or actual stream overflow must reject publication",
                    );
                    let fault = store.inner.staging_fault.lock().unwrap().take().unwrap();
                    assert_eq!(fault.hits, 1, "{entry}/{stage}/{compact}");
                    let path = fault.path.unwrap();
                    assert!(
                        !path.exists(),
                        "owned staging survived {entry}/{stage}: {error}"
                    );
                    if stage == "source_growth" {
                        assert!(matches!(error, ArtifactError::ObjectTooLarge));
                        assert!(fault.observed.len() as u64 > store.config().max_object_bytes);
                    } else {
                        assert!(
                            matches!(error, ArtifactError::Io(_)),
                            "{entry}/{stage}: {error}"
                        );
                    }
                    if stage == "write" {
                        assert_eq!(fault.observed, b"partial staged bytes");
                    }
                    if stage == "sync" || stage == "compact_read" {
                        assert_eq!(fault.observed, payload);
                    }
                    let after = {
                        let state = repository.state.lock().unwrap();
                        (
                            state.objects.clone(),
                            state.attempts.clone(),
                            state.records.clone(),
                            state.pins.clone(),
                            state.reservations.clone(),
                        )
                    };
                    assert_eq!(before, after, "pre-reservation fault changed metadata");
                    assert_eq!(fs::read(&sentinel).unwrap(), b"not this staging owner");
                    let staging_after = fs::read_dir(&store.inner.staging_root)
                        .unwrap()
                        .map(|entry| entry.unwrap().path())
                        .collect::<std::collections::BTreeSet<_>>();
                    assert_eq!(staging_after, staging_before);
                    assert_eq!(
                        store
                            .read_blocking(&baseline, "session:staging", None)
                            .unwrap(),
                        b"unchanged committed baseline"
                    );
                    let retried = publish().expect("same origin recovers after the one-shot fault");
                    assert_eq!(
                        store
                            .read_blocking(&retried, "session:staging", None)
                            .unwrap(),
                        payload
                    );
                    if entry == "prepared" {
                        assert_eq!(publish().unwrap(), retried);
                    }
                    assert_eq!(store.collect_garbage(100).unwrap().removed_objects, 0);
                }
            }
        }
    }

    fn staging_cleanup_failure_preserves_primary_and_exact_owner() {
        let (store, _) = r1_artifacts("staging-unlink-fault", 8);
        fs::create_dir_all(&store.inner.staging_root).unwrap();
        let path = store.inner.staging_root.join("owned.part");
        let quarantine = store.inner.staging_root.join("owned.quarantined");
        let sentinel = store.inner.staging_root.join("foreign.part");
        fs::write(&path, b"actual partial publication").unwrap();
        fs::write(&sentinel, b"foreign bytes").unwrap();
        let mut staging = StagingOwner::new(path.clone());
        fs::rename(&path, &quarantine).unwrap();
        fs::create_dir(&path).unwrap();
        let unknown = ArtifactError::Metadata("publication commit outcome unknown".into());
        let error = staging.settle::<()>(Err(unknown)).unwrap_err();
        assert!(
            matches!(&error, ArtifactError::Metadata(message) if message.contains("commit outcome unknown") && message.contains("cleanup_unconfirmed"))
        );
        assert!(path.is_dir() && quarantine.is_file() && staging.armed);
        assert_eq!(fs::read(&sentinel).unwrap(), b"foreign bytes");
        assert!(matches!(
            staging.settle::<()>(Err(ArtifactError::ObjectTooLarge)),
            Err(ArtifactError::ObjectTooLarge)
        ));
        fs::remove_dir(&path).unwrap();
        fs::rename(&quarantine, &path).unwrap();
        staging.cleanup().unwrap();
        assert!(!path.exists() && !quarantine.exists() && !staging.armed);
        staging.cleanup().unwrap();
        assert_eq!(fs::read(&sentinel).unwrap(), b"foreign bytes");
        for primary in [
            ArtifactError::Metadata("reserve commit outcome unknown".into()),
            ArtifactError::Io("real write error".into()),
            ArtifactError::Blocking("real join error".into()),
        ] {
            let primary_kind = std::mem::discriminant(&primary);
            let primary_message = primary.to_string();
            let retained =
                retain_primary_error(primary, "release_unconfirmed", "secondary metadata fault");
            assert_eq!(primary_kind, std::mem::discriminant(&retained));
            assert!(retained.to_string().contains(&primary_message));
        }
    }

    async fn async_staging_failures_close_the_writer() {
        for stage in ["write", "flush", "sync", "overflow"] {
            let (store, repository) = r1_artifacts("async-staging-real-io", 8);
            fs::create_dir_all(&store.inner.staging_root).unwrap();
            let path = store.inner.staging_root.join("async-owned.part");
            fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            fs::write(&path, b"partial bytes before real async error").unwrap();
            let file = if stage == "sync" {
                #[cfg(unix)]
                {
                    let (socket, peer) = std::os::unix::net::UnixStream::pair().unwrap();
                    let descriptor: std::os::fd::OwnedFd = socket.into();
                    drop(peer);
                    fs::File::from(descriptor)
                }
                #[cfg(not(unix))]
                panic!("this OS error-path fixture requires Unix");
            } else {
                fs::File::open(&path).unwrap()
            };
            let mut file = tokio::fs::File::from_std(file);
            file.set_max_buf_size(1);
            let mut writer = LocalArtifactWriter {
                store: store.clone(),
                descriptor: ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:async-fault".into(),
                    expected_bytes: None,
                    original_name: None,
                },
                staging: Some(StagingOwner::new(path.clone())),
                file: Some(file),
                hasher: Sha256::new(),
                bytes: 0,
                artifact_id: None,
                closed: false,
            };
            let error = match stage {
                "write" => writer.write_chunk(b"more than one byte").await.unwrap_err(),
                "flush" => {
                    writer.file.as_mut().unwrap().write_all(b"x").await.unwrap();
                    writer.finish().await.unwrap_err()
                }
                "sync" => writer.finish().await.unwrap_err(),
                "overflow" => writer
                    .write_chunk(&vec![b'x'; store.config().max_object_bytes as usize + 1])
                    .await
                    .unwrap_err(),
                _ => unreachable!(),
            };
            assert!(
                matches!(error, ArtifactError::Io(_) | ArtifactError::ObjectTooLarge),
                "{stage}: {error}"
            );
            assert!(matches!(
                writer.finish().await,
                Err(ArtifactError::WriterClosed)
            ));
            assert!(matches!(
                writer.write_chunk(b"must not reopen").await,
                Err(ArtifactError::WriterClosed)
            ));
            assert!(!path.exists(), "{stage}: staging not removed");
            writer.abort().await.unwrap();
            let state = repository.state.lock().unwrap();
            assert!(
                state.objects.is_empty()
                    && state.records.is_empty()
                    && state.attempts.is_empty()
                    && state.pins.is_empty()
            );
        }
    }

    #[test]
    fn quiesced_reconciliation_preserves_audit_pins_and_unknown_publications() {
        use super::*;
        let (store, repository) = r1_artifacts("quiesced-reconciliation", 8);
        let audit = r1_test_live(
            &store,
            "session:audit",
            "current-audit-fact",
            b"immutable accepted policy fact",
        );
        let orphan = r1_test_live(
            &store,
            "session:audit",
            "discarded-content",
            b"known discarded content",
        );
        store.unpin(&orphan, "discarded-content").unwrap();
        store.delete(&orphan, "session:audit").unwrap();
        let generation = repository
            .object(&orphan.sha256)
            .unwrap()
            .unwrap()
            .generation;
        assert!(repository
            .claim_object_deletion(&orphan.sha256, &generation)
            .unwrap());
        let unknown = ArtifactObjectRecord {
            sha256: format!("sha256:{}", "a".repeat(64)),
            bytes: 3,
            tier: ArtifactObjectTier::Compact,
            compact_body: Some(b"???".to_vec()),
            created_at_ms: 0,
            generation: "unknown-current-attempt".into(),
        };
        {
            let mut state = repository.state.lock().unwrap();
            state
                .objects
                .insert(unknown.sha256.clone(), unknown.clone());
            state.reservations.insert(unknown.sha256.clone(), 1);
        }
        let root = store.inner.blob_root.clone();
        let config = store.config();
        drop(store);
        let mut reopened = ArtifactStore::new(root, repository.clone(), config).unwrap();
        assert!(reopened.reconcile_quiesced_orphans(false, 100).is_err());
        let active_owner = reopened.clone();
        assert!(reopened.reconcile_quiesced_orphans(true, 100).is_err());
        drop(active_owner);
        let report = reopened.reconcile_quiesced_orphans(true, 100).unwrap();
        assert_eq!(report.removed_objects, 1);
        assert!(repository.object(&orphan.sha256).unwrap().is_none());
        assert_eq!(repository.object(&unknown.sha256).unwrap(), Some(unknown));
        assert_eq!(
            reopened
                .read_blocking(&audit, "session:audit", None)
                .unwrap(),
            b"immutable accepted policy fact"
        );
        assert_eq!(
            reopened
                .reconcile_quiesced_orphans(true, 100)
                .unwrap()
                .removed_objects,
            0
        );
    }
    use super::*;

    #[tokio::test]
    async fn session_digest_reader_exact_filters_beyond_sixteen_catalog_pages() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = Arc::new(EphemeralArtifactRepository::default());
        let store = ArtifactStore::new(
            temporary.path(),
            repository.clone(),
            ArtifactStoreConfig::default(),
        )
        .unwrap();
        let descriptor = |media_type: String| ArtifactWriteDescriptor {
            media_type,
            visibility_scope: "session:digest-pages".into(),
            expected_bytes: None,
            original_name: None,
        };
        let body = serde_json::json!({
            "status": "published",
            "media_type": "正文".repeat(10_000)
        })
        .to_string();
        let artifact = store
            .write_bytes(descriptor("application/json".into()), body.as_bytes())
            .await
            .unwrap();
        let noise = store
            .write_bytes(descriptor(artifact.sha256.clone()), b"not the target")
            .await
            .unwrap();
        let mut target = repository
            .record(parse_selector(&artifact.selector).unwrap())
            .unwrap()
            .unwrap();
        repository.remove_record(&target.artifact_id).unwrap();
        target.artifact_id = "art_ffffffffffffffffffffffffffffffff".into();
        repository.put_record(&target).unwrap();
        let template = repository
            .record(parse_selector(&noise.selector).unwrap())
            .unwrap()
            .unwrap();
        for sequence in 1..=17 * 128 {
            let mut record = template.clone();
            record.artifact_id = format!("art_{sequence:032x}");
            repository.put_record(&record).unwrap();
        }
        let bytes = store
            .read_session_content_by_digest_blocking(
                "digest-pages",
                &artifact.sha256,
                artifact.bytes,
            )
            .unwrap();
        assert_eq!(bytes, body.as_bytes());
        assert!(body.len() > 16 * 1024 && body.len() < 256 * 1024);
        assert!(matches!(
            store.read_session_content_by_digest_blocking(
                "digest-pages",
                &artifact.sha256,
                artifact.bytes + 1
            ),
            Err(ArtifactError::NotFound)
        ));
    }

    #[tokio::test]
    async fn session_digest_reader_preserves_scope_duplicates_and_original_tiers() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(
            temporary.path(),
            ArtifactStoreConfig {
                compact_threshold_bytes: 8,
                ..ArtifactStoreConfig::default()
            },
        )
        .unwrap();
        for body in [
            b"tiny".as_slice(),
            b"original blob content".as_slice(),
            b"".as_slice(),
        ] {
            let descriptor = |scope: &str| ArtifactWriteDescriptor {
                media_type: "application/octet-stream".into(),
                visibility_scope: scope.into(),
                expected_bytes: None,
                original_name: None,
            };
            let foreign = store
                .write_bytes(descriptor("session:foreign"), body)
                .await
                .unwrap();
            store.write_bytes(descriptor("public"), body).await.unwrap();
            assert!(matches!(
                store.read_session_content_by_digest_blocking(
                    "local",
                    &foreign.sha256,
                    foreign.bytes
                ),
                Err(ArtifactError::NotFound)
            ));
            let first = store
                .write_bytes(descriptor("session:local"), body)
                .await
                .unwrap();
            let second = store
                .write_bytes(descriptor("session:local"), body)
                .await
                .unwrap();
            assert_ne!(first.selector, second.selector);
            store.delete(&first, "session:local").unwrap();
            store
                .pin(
                    &second,
                    "evidence:local:original",
                    ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                )
                .unwrap();
            assert!(store.delete(&second, "session:local").is_err());
            store.collect_garbage(128).unwrap();
            assert_eq!(
                store
                    .read_session_content_by_digest_blocking("local", &second.sha256, second.bytes)
                    .unwrap(),
                body
            );
            store.unpin(&second, "evidence:local:original").unwrap();
            store.delete(&second, "session:local").unwrap();
            assert!(matches!(
                store.read_session_content_by_digest_blocking(
                    "local",
                    &second.sha256,
                    second.bytes
                ),
                Err(ArtifactError::NotFound)
            ));
        }
        assert!(matches!(
            store.read_session_content_by_digest_blocking(" ", &"a".repeat(64), 0),
            Err(ArtifactError::Unauthorized)
        ));
        for digest in [
            "a".repeat(16),
            format!("sha256:sha256:{}", "a".repeat(64)),
            "g".repeat(64),
        ] {
            assert!(matches!(
                store.read_session_content_by_digest_blocking("local", &digest, 0),
                Err(ArtifactError::Metadata(_))
            ));
        }
    }

    #[tokio::test]
    async fn session_digest_reader_rehashes_compact_and_blob_payloads() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = Arc::new(EphemeralArtifactRepository::default());
        let store = ArtifactStore::new(
            temporary.path(),
            repository.clone(),
            ArtifactStoreConfig {
                compact_threshold_bytes: 8,
                ..ArtifactStoreConfig::default()
            },
        )
        .unwrap();
        for body in [b"tiny".as_slice(), b"original blob content".as_slice()] {
            let artifact = store
                .write_bytes(
                    ArtifactWriteDescriptor {
                        media_type: "application/octet-stream".into(),
                        visibility_scope: "session:corruption".into(),
                        expected_bytes: None,
                        original_name: None,
                    },
                    body,
                )
                .await
                .unwrap();
            if body.len() <= 8 {
                repository
                    .state
                    .lock()
                    .unwrap()
                    .objects
                    .get_mut(&artifact.sha256)
                    .unwrap()
                    .compact_body = Some(vec![b'x'; body.len()]);
            } else {
                fs::write(
                    store.generation_path(
                        &artifact.sha256,
                        &repository
                            .object(&artifact.sha256)
                            .unwrap()
                            .unwrap()
                            .generation,
                    ),
                    vec![b'x'; body.len()],
                )
                .unwrap();
            }
            assert!(matches!(
                store.read_session_content_by_digest_blocking(
                    "corruption",
                    &artifact.sha256,
                    artifact.bytes
                ),
                Err(ArtifactError::Metadata(_))
            ));
            if body.len() > 8 {
                fs::remove_file(
                    store.generation_path(
                        &artifact.sha256,
                        &repository
                            .object(&artifact.sha256)
                            .unwrap()
                            .unwrap()
                            .generation,
                    ),
                )
                .unwrap();
                assert!(matches!(
                    store.read_session_content_by_digest_blocking(
                        "corruption",
                        &artifact.sha256,
                        artifact.bytes
                    ),
                    Err(ArtifactError::Io(_))
                ));
            }
        }
    }

    #[tokio::test]
    async fn session_digest_reader_checks_actual_length_and_catalog_invalidation() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = Arc::new(EphemeralArtifactRepository::default());
        let store = ArtifactStore::new(
            temporary.path(),
            repository.clone(),
            ArtifactStoreConfig::default(),
        )
        .unwrap();
        let artifact = store
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:source-cut".into(),
                    expected_bytes: None,
                    original_name: None,
                },
                b"immutable original",
            )
            .await
            .unwrap();
        let mut record = repository
            .record(parse_selector(&artifact.selector).unwrap())
            .unwrap()
            .unwrap();
        record.bytes += 1;
        assert!(repository.put_record(&record).is_err());
        repository.remove_record(&record.artifact_id).unwrap();
        repository.put_record(&record).unwrap();
        assert!(matches!(
            store.read_session_content_by_digest_blocking(
                "source-cut",
                &record.sha256,
                record.bytes
            ),
            Err(ArtifactError::Metadata(_))
        ));
        assert!(matches!(
            store.read_session_content_by_digest_blocking(
                "source-cut",
                &artifact.sha256,
                store.config().max_object_bytes + 1
            ),
            Err(ArtifactError::ObjectTooLarge)
        ));
        let request = ArtifactCatalogQuery {
            scopes: vec!["session:source-cut".into()],
            query: artifact.sha256.clone(),
            after_id: None,
            snapshot: None,
            limit: 1,
        };
        let page = repository.catalog_page(request.clone()).unwrap();
        repository.remove_record(&record.artifact_id).unwrap();
        assert!(repository
            .catalog_page(ArtifactCatalogQuery {
                snapshot: Some(page.snapshot),
                ..request
            })
            .unwrap_err()
            .contains("source changed"));
        assert!(matches!(
            store.read_session_content_by_digest_blocking(
                "source-cut",
                &artifact.sha256,
                artifact.bytes
            ),
            Err(ArtifactError::NotFound)
        ));
    }

    #[tokio::test]
    async fn compact_and_blob_share_selector_contract_and_support_ranges() {
        let temporary = tempfile::tempdir().unwrap();
        let config = ArtifactStoreConfig {
            compact_threshold_bytes: 8,
            max_object_bytes: 1_024,
            total_quota_bytes: 4_096,
            gc_high_water_bytes: 3_500,
            gc_low_water_bytes: 3_000,
            orphan_grace_ms: 0,
        };
        let store = ArtifactStore::for_test(temporary.path(), config).expect("artifact store");
        let descriptor = |scope: &str| ArtifactWriteDescriptor {
            media_type: "application/octet-stream".to_string(),
            visibility_scope: scope.to_string(),
            expected_bytes: None,
            original_name: None,
        };
        let compact = store
            .write_bytes(descriptor("session:s1"), b"12345678")
            .await
            .unwrap();
        let blob = store
            .write_bytes(descriptor("session:s1"), b"0123456789")
            .await
            .unwrap();
        let hash_bytes = || {
            store
                .inner
                .verified_read_hash_bytes
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        assert_eq!(hash_bytes(), 0);
        for (artifact, expected) in [(&compact, &b"12345678"[..]), (&blob, &b"0123456789"[..])] {
            let before = hash_bytes();
            let read = store
                .read_verified_blocking(artifact, "session:s1", None, None, false, true)
                .unwrap();
            assert_eq!(read.body, expected);
            assert_eq!(hash_bytes() - before, expected.len() as u64);
            eprintln!("verified_artifact_cost tier_full bytes={} metadata_reads={} hashed_bytes={} returned_bytes={}",
                artifact.bytes, read.reads, hash_bytes()-before, read.body.len());
        }
        assert!(compact.selector.starts_with("artifact://"));
        assert!(blob.selector.starts_with("artifact://"));
        assert_eq!(
            store.read(&blob, "session:s1", Some(2..6)).await.unwrap(),
            b"2345"
        );
        assert!(matches!(
            store.read(&compact, "session:s2", None).await,
            Err(ArtifactError::Unauthorized)
        ));
        assert_eq!(
            hash_bytes(),
            28,
            "8+10 full bytes and10 for the partial Blob read; foreign scope hashes nothing"
        );
        let stats = store.stats().unwrap();
        assert_eq!(stats.objects, 2);
        assert_eq!(stats.compact_bytes, 8);
        assert_eq!(stats.blob_bytes, 10);
    }

    #[tokio::test]
    async fn resolve_returns_durable_ref_and_rejects_unknown_or_invalid() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(temporary.path(), ArtifactStoreConfig::default())
            .expect("artifact store");
        let written = store
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "text/plain".to_string(),
                    visibility_scope: "public".to_string(),
                    expected_bytes: None,
                    original_name: None,
                },
                b"hello",
            )
            .await
            .expect("write");
        let resolved = store.resolve(&written.selector).expect("resolve");
        assert_eq!(resolved.selector, written.selector);
        assert_eq!(resolved.sha256, written.sha256);
        assert_eq!(resolved.bytes, 5);
        assert_eq!(resolved.media_type, "text/plain");
        assert_eq!(
            store.read(&resolved, "public", None).await.expect("read"),
            b"hello"
        );
        assert!(matches!(
            store.resolve("artifact://missing"),
            Err(ArtifactError::NotFound)
        ));
        assert!(store.resolve("not-a-selector").is_err());
    }

    #[tokio::test]
    async fn duplicate_content_deduplicates_physical_bytes_and_abort_is_invisible() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(temporary.path(), ArtifactStoreConfig::default())
            .expect("artifact store");
        let descriptor = ArtifactWriteDescriptor {
            media_type: "text/plain".to_string(),
            visibility_scope: "session:s1".to_string(),
            expected_bytes: None,
            original_name: None,
        };
        let first = store
            .write_bytes(descriptor.clone(), b"same")
            .await
            .unwrap();
        let second = store
            .write_bytes(descriptor.clone(), b"same")
            .await
            .unwrap();
        assert_ne!(first.selector, second.selector);
        assert_eq!(store.stats().unwrap().objects, 1);

        let mut writer = store.begin(descriptor).await.unwrap();
        writer.write_chunk(b"never published").await.unwrap();
        writer.abort().await.unwrap();
        assert_eq!(store.stats().unwrap().artifacts, 2);
    }

    #[tokio::test]
    async fn dropping_an_unfinished_writer_removes_staging_bytes() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(temporary.path(), ArtifactStoreConfig::default())
            .expect("artifact store");
        let staging_root = temporary.path().join("staging");
        let mut writer = store
            .begin(ArtifactWriteDescriptor {
                media_type: "text/plain".to_string(),
                visibility_scope: "session:drop".to_string(),
                expected_bytes: None,
                original_name: None,
            })
            .await
            .unwrap();
        writer.write_chunk(b"cancelled request").await.unwrap();
        assert_eq!(fs::read_dir(&staging_root).unwrap().count(), 1);

        drop(writer);

        assert_eq!(fs::read_dir(staging_root).unwrap().count(), 0);
        assert_eq!(store.stats().unwrap().artifacts, 0);
    }

    #[tokio::test]
    async fn streaming_writer_handles_one_hundred_megabytes_without_whole_object_buffering() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(
            temporary.path(),
            ArtifactStoreConfig {
                compact_threshold_bytes: 256 * 1024,
                max_object_bytes: 128 * 1024 * 1024,
                total_quota_bytes: 256 * 1024 * 1024,
                gc_high_water_bytes: 220 * 1024 * 1024,
                gc_low_water_bytes: 200 * 1024 * 1024,
                orphan_grace_ms: 0,
            },
        )
        .expect("artifact store");
        let mut writer = store
            .begin(ArtifactWriteDescriptor {
                media_type: "application/octet-stream".to_string(),
                visibility_scope: "session:large".to_string(),
                expected_bytes: Some(100 * 1024 * 1024),
                original_name: Some("large.bin".to_string()),
            })
            .await
            .unwrap();
        let chunk = vec![0x5a; 1024 * 1024];
        for _ in 0..100 {
            writer.write_chunk(&chunk).await.unwrap();
        }
        let artifact = writer.finish().await.unwrap();
        assert_eq!(artifact.bytes, 100 * 1024 * 1024);
        assert_eq!(
            store
                .read(
                    &artifact,
                    "session:large",
                    Some((50 * 1024 * 1024)..(50 * 1024 * 1024 + 32)),
                )
                .await
                .unwrap(),
            vec![0x5a; 32]
        );
        assert_eq!(store.stats().unwrap().blob_bytes, artifact.bytes);
    }

    #[tokio::test]
    async fn quota_pin_and_gc_enforce_lifecycle_contract() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(
            temporary.path(),
            ArtifactStoreConfig {
                compact_threshold_bytes: 4,
                max_object_bytes: 20,
                total_quota_bytes: 40,
                gc_high_water_bytes: 10,
                gc_low_water_bytes: 0,
                orphan_grace_ms: 0,
            },
        )
        .expect("artifact store");
        let descriptor = ArtifactWriteDescriptor {
            media_type: "application/octet-stream".to_string(),
            visibility_scope: "session:gc".to_string(),
            expected_bytes: None,
            original_name: None,
        };
        let artifact = store
            .write_bytes(descriptor.clone(), b"twelve-bytes")
            .await
            .unwrap();
        store
            .pin(&artifact, "receipt:active", ARTIFACT_PERMANENT_PIN_UNTIL_MS)
            .unwrap();
        assert!(matches!(
            store.delete(&artifact, "session:gc"),
            Err(ArtifactError::Metadata(_))
        ));
        store.unpin(&artifact, "receipt:active").unwrap();
        store.delete(&artifact, "session:gc").unwrap();
        let report = store.collect_garbage(10).unwrap();
        assert_eq!(report.removed_objects, 1);
        assert_eq!(store.stats().unwrap().physical_bytes, 0);

        let _first = store
            .write_bytes(descriptor.clone(), &[1; 20])
            .await
            .unwrap();
        let _second = store
            .write_bytes(descriptor.clone(), &[2; 20])
            .await
            .unwrap();
        assert!(matches!(
            store.write_bytes(descriptor.clone(), b"x").await,
            Err(ArtifactError::QuotaExceeded)
        ));
        assert!(matches!(
            store.write_bytes(descriptor, &[3; 21]).await,
            Err(ArtifactError::ObjectTooLarge)
        ));
    }

    #[tokio::test]
    async fn eighty_concurrent_range_reads_are_stable_and_scoped() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test(
            temporary.path(),
            ArtifactStoreConfig {
                compact_threshold_bytes: 8,
                max_object_bytes: 2 * 1024 * 1024,
                total_quota_bytes: 4 * 1024 * 1024,
                gc_high_water_bytes: 3 * 1024 * 1024,
                gc_low_water_bytes: 2 * 1024 * 1024,
                orphan_grace_ms: 0,
            },
        )
        .expect("artifact store");
        let payload = vec![0x7b; 1024 * 1024];
        let artifact = store
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "application/octet-stream".to_string(),
                    visibility_scope: "team:parallel".to_string(),
                    expected_bytes: Some(payload.len() as u64),
                    original_name: None,
                },
                &payload,
            )
            .await
            .unwrap();
        let mut readers = Vec::new();
        for index in 0..80_u64 {
            let store = store.clone();
            let artifact = artifact.clone();
            readers.push(tokio::spawn(async move {
                let start = index * 128;
                store
                    .read(&artifact, "team:parallel", Some(start..start + 128))
                    .await
            }));
        }
        for reader in readers {
            assert_eq!(reader.await.unwrap().unwrap(), vec![0x7b; 128]);
        }
        let hashed = store
            .inner
            .verified_read_hash_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(hashed, 80 * 1024 * 1024);
        eprintln!("verified_artifact_cost large_blob_pages=80 source_bytes=1048576 hashed_bytes={hashed} returned_bytes=10240");
        assert!(matches!(
            store.read(&artifact, "team:other", None).await,
            Err(ArtifactError::Unauthorized)
        ));
        assert_eq!(
            store
                .inner
                .verified_read_hash_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            hashed
        );
    }
    fn r1_test_live(
        store: &super::ArtifactStore,
        scope: &str,
        owner: &str,
        body: &[u8],
    ) -> super::ArtifactRef {
        let source = store
            .inner
            .staging_root
            .join(format!("live-source-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&store.inner.staging_root).unwrap();
        std::fs::write(&source, body).unwrap();
        let artifact = store
            .write_path_blocking(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "application/json".into(),
                    visibility_scope: scope.into(),
                    expected_bytes: Some(body.len() as u64),
                    original_name: None,
                },
                &source,
            )
            .unwrap();
        store
            .pin(&artifact, owner, super::ARTIFACT_PERMANENT_PIN_UNTIL_MS)
            .unwrap();
        artifact
    }

    #[test]
    fn r1_content_gc_public_adoption_preserves_cross_owner_roots() {
        use super::*;
        for adoption in ["read", "pin"] {
            let (mut store, repository) = r1_artifacts("public-adoption", 8);
            let origin = PreparedArtifactOrigin::EventBody {
                stream_id: "graph:discarded".into(),
                idempotency_key: adoption.into(),
                kind: "execution_graph.planned".into(),
            };
            let artifact = store
                .write_pre_pinned_blocking(
                    "session:owner",
                    adoption,
                    b"private complete content",
                    &origin,
                )
                .unwrap();
            let candidate = repository
                .prepared_page(
                    None,
                    &repository.prepared_high_water().unwrap().unwrap(),
                    100,
                )
                .unwrap()
                .candidates
                .remove(0);
            assert!(store.resolve(&artifact.selector).is_err());
            assert!(repository
                .catalog_page(ArtifactCatalogQuery {
                    scopes: vec!["session:owner".into()],
                    query: String::new(),
                    after_id: None,
                    snapshot: None,
                    limit: 100
                })
                .unwrap()
                .records
                .is_empty());
            assert!(store
                .read_blocking(&artifact, "session:foreign", None)
                .is_err());
            assert!(store.unpin(&artifact, ARTIFACT_PREPARED_PIN_OWNER).is_err());
            assert!(store
                .pin(
                    &artifact,
                    ARTIFACT_PREPARED_PIN_OWNER,
                    ARTIFACT_PERMANENT_PIN_UNTIL_MS
                )
                .is_err());
            assert!(store.delete(&artifact, "session:owner").is_err());
            assert_eq!(
                store
                    .read_prepared_blocking(&artifact, "session:owner", &origin)
                    .unwrap(),
                b"private complete content"
            );
            assert_eq!(
                repository
                    .record(parse_selector(&artifact.selector).unwrap())
                    .unwrap()
                    .unwrap()
                    .record_class,
                ArtifactRecordClass::PrivatePrepared
            );
            let mut foreign = origin.clone();
            if let PreparedArtifactOrigin::EventBody { stream_id, .. } = &mut foreign {
                *stream_id = "graph:other".into();
            }
            assert!(store
                .read_prepared_blocking(&artifact, "session:owner", &foreign)
                .is_err());
            if adoption == "read" {
                assert_eq!(
                    store
                        .read_blocking(&artifact, "session:owner", Some(0..1))
                        .unwrap(),
                    b"p"
                );
            } else {
                store
                    .pin(&artifact, adoption, ARTIFACT_PERMANENT_PIN_UNTIL_MS)
                    .unwrap();
                store.unpin(&artifact, adoption).unwrap();
            }
            assert_eq!(
                repository
                    .record(parse_selector(&artifact.selector).unwrap())
                    .unwrap()
                    .unwrap()
                    .record_class,
                ArtifactRecordClass::Live
            );
            assert_eq!(
                repository.retire_prepared(&candidate).unwrap(),
                PreparedArtifactRetirement::Retained
            );
            assert_eq!(store.resolve(&artifact.selector).unwrap(), artifact);
            assert!(store.delete(&artifact, "session:owner").is_err());
            assert_eq!(
                store
                    .reconcile_quiesced_orphans(true, 100)
                    .unwrap()
                    .removed_objects,
                0
            );
        }
    }

    #[test]
    fn r1_content_gc_retirement_is_atomic_and_attempts_are_generation_exact() {
        use super::*;
        let (mut store, repository) = r1_artifacts("retire-attempts", 8);
        let origin = PreparedArtifactOrigin::EventBody {
            stream_id: "graph:attempt".into(),
            idempotency_key: "attempt".into(),
            kind: "execution_graph.planned".into(),
        };
        let artifact = store
            .write_pre_pinned_blocking(
                "session:attempt",
                "attempt",
                b"reserved generation content",
                &origin,
            )
            .unwrap();
        let candidate = repository
            .prepared_page(
                None,
                &repository.prepared_high_water().unwrap().unwrap(),
                100,
            )
            .unwrap()
            .candidates
            .remove(0);
        repository.state.lock().unwrap().pins.insert(
            (candidate.record.artifact_id.clone(), "other-owner".into()),
            1,
        );
        assert_eq!(
            repository.retire_prepared(&candidate).unwrap(),
            PreparedArtifactRetirement::Retained
        );
        assert!(repository
            .is_pinned(&candidate.record.artifact_id, 0)
            .unwrap());
        repository
            .state
            .lock()
            .unwrap()
            .pins
            .remove(&(candidate.record.artifact_id.clone(), "other-owner".into()));
        let mut stale = candidate.clone();
        stale.generation = "stale".into();
        assert_eq!(
            repository.retire_prepared(&stale).unwrap(),
            PreparedArtifactRetirement::Retained
        );
        assert_eq!(
            repository.retire_prepared(&candidate).unwrap(),
            PreparedArtifactRetirement::Removed
        );
        assert_eq!(
            repository.retire_prepared(&candidate).unwrap(),
            PreparedArtifactRetirement::Missing
        );
        assert_eq!(
            store
                .reconcile_quiesced_orphans(true, 100)
                .unwrap()
                .removed_objects,
            1
        );
        assert!(repository.object(&artifact.sha256).unwrap().is_none());
        let object = ArtifactObjectRecord {
            generation: "reserve-only".into(),
            sha256: format!("sha256:{}", "e".repeat(64)),
            bytes: 3,
            tier: ArtifactObjectTier::Blob,
            compact_body: None,
            created_at_ms: 0,
        };
        repository
            .reserve_attempt(
                &object,
                store.config().total_quota_bytes,
                "first",
                "first-publication",
            )
            .unwrap();
        repository
            .reserve_attempt(
                &object,
                store.config().total_quota_bytes,
                "second",
                "second-publication",
            )
            .unwrap();
        assert!(repository
            .release_attempt("first", &object.sha256, "stale")
            .is_err());
        assert!(repository
            .release_attempt("first", &object.sha256, &object.generation)
            .unwrap());
        assert!(!repository
            .release_attempt("first", &object.sha256, &object.generation)
            .unwrap());
        assert!(!repository
            .claim_object_deletion(&object.sha256, &object.generation)
            .unwrap());
        assert!(repository
            .release_attempt("second", &object.sha256, &object.generation)
            .unwrap());
        assert!(repository
            .reserve_attempt(
                &object,
                store.config().total_quota_bytes,
                "first",
                "first-publication"
            )
            .is_err());
        assert_eq!(
            store
                .reconcile_quiesced_orphans(true, 100)
                .unwrap()
                .removed_objects,
            1
        );
    }

    #[test]
    fn r1_content_gc_exact_prepare_reuses_publication_without_new_attempts() {
        use super::*;
        let (store, repository) = r1_artifacts("exact-prepare", 8);
        let origin = PreparedArtifactOrigin::EventBody {
            stream_id: "graph:exact".into(),
            idempotency_key: "exact".into(),
            kind: "execution_graph.planned".into(),
        };
        let first = store
            .write_pre_pinned_blocking(
                "session:exact",
                "exact",
                b"same durable publication",
                &origin,
            )
            .unwrap();
        let generation = repository
            .object(&first.sha256)
            .unwrap()
            .unwrap()
            .generation;
        let attempts = repository.state.lock().unwrap().attempts.len();
        for _ in 0..8 {
            assert_eq!(
                store
                    .write_pre_pinned_blocking(
                        "session:exact",
                        "exact",
                        b"same durable publication",
                        &origin
                    )
                    .unwrap(),
                first
            );
            assert_eq!(
                repository
                    .object(&first.sha256)
                    .unwrap()
                    .unwrap()
                    .generation,
                generation
            );
            assert_eq!(repository.state.lock().unwrap().attempts.len(), attempts);
        }
        let second = store
            .write_pre_pinned_blocking(
                "session:exact",
                "exact",
                b"changed durable publication",
                &origin,
            )
            .unwrap();
        assert_ne!(first.selector, second.selector);
        repository.state.lock().unwrap().pins.remove(&(
            parse_selector(&first.selector).unwrap().into(),
            ARTIFACT_PREPARED_PIN_OWNER.into(),
        ));
        assert!(store
            .write_pre_pinned_blocking(
                "session:exact",
                "exact",
                b"same durable publication",
                &origin
            )
            .is_err());
        assert_eq!(
            repository.state.lock().unwrap().attempts.len(),
            attempts + 1
        );
    }

    #[test]
    fn r1_content_gc_closed_checkpoint_roots_preserve_current_and_retire_replaced_bodies() {
        use super::*;
        use crate::RuntimeEventStoreBackend;
        let (store, repository) = r1_artifacts("checkpoint-root-contract", 8);
        let backend = Arc::new(crate::runtime_event_store::EphemeralRuntimeEventStore::new());
        let projections = [
            "goal-projection:root-contract",
            "runtime:agentic-read-model:v1:root-contract",
            "projection-history:root-contract",
        ];
        let artifacts = Arc::new(store);
        let events = crate::RuntimeEventStore::from_backend(backend.clone());
        events.bind_artifact_store(artifacts.clone()).unwrap();
        let mut old = Vec::new();
        for projection in projections {
            let origin = PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection.into(),
                source_cursor: 1,
            };
            let body = events
                .prepare_content(
                    "session:checkpoint",
                    projection,
                    &serde_json::json!({"padding":format!("old {projection} ").repeat(2048)}),
                    &origin,
                )
                .unwrap();
            old.push(
                serde_json::from_value::<ArtifactRef>(body["body"]["artifact"].clone()).unwrap(),
            );
            let payload = if projection.starts_with("runtime:") {
                serde_json::json!({"schema_version":2,"session_id":"checkpoint","body":body})
            } else {
                serde_json::json!({"schema":if projection.starts_with("projection-history:") {3} else {2},"scope":"session:checkpoint","body":body})
            };
            backend
                .put_projection_checkpoint(projection, 1, &payload, 0)
                .unwrap();
        }
        let view = events.quiesced_content_roots();
        drop(events);
        let mut store = Arc::try_unwrap(artifacts).ok().unwrap();
        let first = view
            .reconcile_prepared_batch(&mut store, true, "ephemeral:checkpoint", "checkpoint-run-1")
            .unwrap();
        assert_eq!(first.referenced_checkpoint, 3);
        assert_eq!(first.retired_records, 0);
        let artifacts = Arc::new(store);
        let events = crate::RuntimeEventStore::from_backend(backend.clone());
        events.bind_artifact_store(artifacts.clone()).unwrap();
        for projection in projections {
            let origin = PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection.into(),
                source_cursor: 2,
            };
            let body = events
                .prepare_content(
                    "session:checkpoint",
                    projection,
                    &serde_json::json!({"padding":format!("new {projection} ").repeat(2048)}),
                    &origin,
                )
                .unwrap();
            let payload = if projection.starts_with("runtime:") {
                serde_json::json!({"schema_version":2,"session_id":"checkpoint","body":body})
            } else {
                serde_json::json!({"schema":if projection.starts_with("projection-history:") {3} else {2},"scope":"session:checkpoint","body":body})
            };
            backend
                .put_projection_checkpoint(projection, 2, &payload, 0)
                .unwrap();
        }
        drop(events);
        let mut store = Arc::try_unwrap(artifacts).ok().unwrap();
        let report = view
            .reconcile_prepared_batch(&mut store, true, "ephemeral:checkpoint", "checkpoint-run-2")
            .unwrap();
        assert_eq!(report.referenced_checkpoint, 3);
        assert_eq!(report.retired_records, 3);
        assert_eq!(report.blocked, 0);
        assert!(report.eligible_prepared_reconciled);
        for reference in old {
            assert!(repository
                .record(parse_selector(&reference.selector).unwrap())
                .unwrap()
                .is_none());
        }
        assert_eq!(
            store
                .reconcile_quiesced_orphans(true, 100)
                .unwrap()
                .removed_objects,
            3
        );
        let current = backend
            .projection_checkpoint(projections[0])
            .unwrap()
            .unwrap();
        backend
            .compare_and_repair_projection_checkpoint(
                projections[0],
                current.source_cursor,
                current.revision,
                &serde_json::json!({"schema":999}),
                0,
            )
            .unwrap();
        let blocked = view
            .reconcile_prepared_batch(&mut store, true, "ephemeral:checkpoint", "checkpoint-run-3")
            .unwrap();
        assert_eq!(blocked.blocked, 1);
        assert!(!blocked.eligible_prepared_reconciled);
    }
}

impl EphemeralArtifactRepository {
    fn publish_attempt_inner(
        &self,
        record: &ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "artifact lock poisoned")?;
        let attempt = state
            .attempts
            .get(token)
            .ok_or("artifact attempt missing")?;
        if attempt.sha256 != record.sha256
            || attempt.generation != generation
            || attempt.publication_id != record.artifact_id
            || attempt.state == ArtifactReservationState::Released
        {
            return Err("artifact attempt identity conflict".into());
        }
        if attempt.state == ArtifactReservationState::Consumed {
            return state
                .records
                .get(&record.artifact_id)
                .filter(|existing| same_publication(existing, record))
                .map(|_| ())
                .ok_or("consumed artifact publication missing or changed".into());
        }
        publish_ephemeral_record(&mut state, record, generation, pin)?;
        state
            .attempts
            .get_mut(token)
            .ok_or("artifact attempt missing")?
            .state = ArtifactReservationState::Consumed;
        Ok(())
    }
}

fn closure_record_metadata_bytes(record: &ArtifactRecord) -> Result<usize, String> {
    if record.artifact_id.len() > 1024
        || record.sha256.len() > 80
        || record.media_type.len() > 256
        || record.visibility_scope.len() > 1024
        || record
            .prepared_origin
            .as_ref()
            .is_none_or(|origin| origin.validate().is_err())
    {
        return Err("process closure metadata bounds exceeded".into());
    }
    let bytes = serde_json::to_vec(&serde_json::json!({
        "record_class":record.record_class, "prepared_origin":record.prepared_origin,
        "artifact_id":record.artifact_id, "sha256":record.sha256, "bytes":record.bytes,
        "media_type":record.media_type, "visibility_scope":record.visibility_scope,
        "tier":match record.tier { ArtifactObjectTier::Compact => "compact", ArtifactObjectTier::Blob => "blob" },
        "created_at_ms":record.created_at_ms, "last_access_at_ms":record.last_access_at_ms,
    })).map_err(|error| error.to_string())?.len();
    if bytes > 32768 {
        return Err("process closure metadata size exceeded".into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod process_closure_storage_428_tests {
    use super::*;

    fn record(body: &[u8]) -> ArtifactRecord {
        ArtifactRecord {
            record_class: ArtifactRecordClass::PrivatePrepared,
            prepared_origin: Some(PreparedArtifactOrigin::ToolEffectClosure {
                stream_id: "execution-effect:closure-428".into(),
                idempotency_key: "closure-428:receipt".into(),
            }),
            artifact_id: "art_closure_428".into(),
            sha256: format!("sha256:{:x}", Sha256::digest(body)),
            bytes: body.len() as u64,
            media_type: "application/vnd.cowd.process-closure+json".into(),
            visibility_scope: "session:closure-428".into(),
            tier: ArtifactObjectTier::Compact,
            created_at_ms: 1,
            last_access_at_ms: 1,
        }
    }

    #[test]
    fn r1_428_generic_metadata_rejects_reserved_origin_even_when_consumed() {
        let repository = EphemeralArtifactRepository::default();
        let record = record(b"{}");
        {
            let mut state = repository.state.lock().unwrap();
            state
                .records
                .insert(record.artifact_id.clone(), record.clone());
            state.attempts.insert(
                "consumed".into(),
                ArtifactReservation {
                    token: "consumed".into(),
                    sha256: record.sha256.clone(),
                    generation: "generation".into(),
                    publication_id: record.artifact_id.clone(),
                    state: ArtifactReservationState::Consumed,
                },
            );
        }
        let error = repository
            .publish_attempt(
                &record,
                "generation",
                "consumed",
                Some((ARTIFACT_PREPARED_PIN_OWNER, ARTIFACT_PERMANENT_PIN_UNTIL_MS)),
            )
            .unwrap_err();
        assert!(error.contains("reserved"));
    }

    #[test]
    fn r1_428_generic_prepared_cannot_publish_reserved_closure() {
        let temporary = tempfile::tempdir().unwrap();
        let store =
            ArtifactStore::for_test(temporary.path(), ArtifactStoreConfig::default()).unwrap();
        let record = record(b"{}");
        assert!(store
            .write_pre_pinned_with_media_blocking(
                &record.visibility_scope,
                "closure-428",
                &record.media_type,
                b"{}",
                record.prepared_origin.as_ref().unwrap(),
            )
            .is_err());
    }

    #[test]
    fn r1_428_strict_read_protection_hash_bound_and_meter() {
        let temporary = tempfile::tempdir().unwrap();
        let repository = Arc::new(EphemeralArtifactRepository::default());
        let store = ArtifactStore::new(
            temporary.path(),
            repository.clone(),
            ArtifactStoreConfig::default(),
        )
        .unwrap();
        let body = b"{\"version\":1}";
        let record = record(body);
        let reference = record.content_reference();
        let origin = record.prepared_origin.as_ref().unwrap();
        {
            let mut state = repository.state.lock().unwrap();
            state
                .records
                .insert(record.artifact_id.clone(), record.clone());
            state.objects.insert(
                record.sha256.clone(),
                ArtifactObjectRecord {
                    generation: "generation".into(),
                    sha256: record.sha256.clone(),
                    bytes: record.bytes,
                    tier: ArtifactObjectTier::Compact,
                    compact_body: Some(body.to_vec()),
                    created_at_ms: 1,
                },
            );
            state.ready.insert(record.sha256.clone());
            state.pins.insert(
                (
                    record.artifact_id.clone(),
                    ARTIFACT_PREPARED_PIN_OWNER.into(),
                ),
                ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            );
        }
        let read = store
            .read_process_closure_blocking(&reference, &record.visibility_scope, origin)
            .unwrap();
        assert_eq!(read.body, body);
        assert_eq!(read.reads, 4);
        assert!(read.metadata_bytes > 0 && read.metadata_bytes < 32768);
        assert!(store
            .read_prepared_blocking(&reference, &record.visibility_scope, origin)
            .is_err());
        assert!(store
            .read_blocking(&reference, &record.visibility_scope, None)
            .is_err());
        let mut wrong_origin = origin.clone();
        if let PreparedArtifactOrigin::ToolEffectClosure {
            stream_id,
            idempotency_key,
        } = &mut wrong_origin
        {
            *stream_id = "execution-effect:other".into();
            *idempotency_key = "other:receipt".into();
        }
        assert!(store
            .read_process_closure_blocking(&reference, &record.visibility_scope, &wrong_origin)
            .is_err());
        {
            let mut state = repository.state.lock().unwrap();
            state.pins.remove(&(
                record.artifact_id.clone(),
                ARTIFACT_PREPARED_PIN_OWNER.into(),
            ));
        }
        assert!(store
            .read_process_closure_blocking(&reference, &record.visibility_scope, origin)
            .is_err());
        {
            let mut state = repository.state.lock().unwrap();
            state.pins.insert(
                (
                    record.artifact_id.clone(),
                    ARTIFACT_PREPARED_PIN_OWNER.into(),
                ),
                ARTIFACT_PERMANENT_PIN_UNTIL_MS,
            );
            state.objects.get_mut(&record.sha256).unwrap().compact_body =
                Some(vec![b'x'; body.len()]);
        }
        assert!(store
            .read_process_closure_blocking(&reference, &record.visibility_scope, origin)
            .is_err());
        {
            let mut state = repository.state.lock().unwrap();
            state.objects.get_mut(&record.sha256).unwrap().compact_body = Some(vec![0; 32769]);
        }
        assert!(repository.process_closure_object(&record.sha256).is_err());
        {
            let mut state = repository.state.lock().unwrap();
            state
                .records
                .get_mut(&record.artifact_id)
                .unwrap()
                .record_class = ArtifactRecordClass::Live;
        }
        assert!(store
            .read_process_closure_blocking(&reference, &record.visibility_scope, origin)
            .is_err());
    }
}

#[cfg(test)]
mod structured_artifact_identity_tests {
    use super::*;

    #[tokio::test]
    async fn same_structured_artifact_identity_is_idempotent_and_conflicts_on_changed_bytes() {
        let root = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test_default(root.path());
        let descriptor = ArtifactWriteDescriptor {
            media_type: "application/json".into(),
            visibility_scope: "session:structured-identity".into(),
            expected_bytes: Some(2),
            original_name: Some("input.json".into()),
        };
        let first = store
            .write_bytes_with_identity(descriptor.clone(), br#"{}"#, "task_identity_input")
            .await
            .unwrap();
        let replay = store
            .write_bytes_with_identity(descriptor.clone(), br#"{}"#, "task_identity_input")
            .await
            .unwrap();
        assert_eq!(first, replay);

        assert!(store
            .write_bytes_with_identity(descriptor, br#"[]"#, "task_identity_input")
            .await
            .is_err());
    }
}
