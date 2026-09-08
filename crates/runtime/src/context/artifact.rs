//! Unified, backend-neutral artifact durability plane.
//!
//! Resource attachments, raw tool output, and evidence receipts share this
//! physical object layer. Domain metadata remains owned by its respective
//! facade; callers never receive a host path or adapter-specific key.

use std::{
    collections::HashMap,
    fs,
    io::{Read, Seek, SeekFrom, Write},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRecord {
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

/// Adapter contract for the selected compact tier and artifact catalogue.
///
/// Durable PostgreSQL implementations live outside Runtime. The process-local
/// implementation below is available only through explicitly test-scoped
/// constructors. Blob bytes always remain behind
/// the selected `StorageDomainId::Blobs` endpoint.
pub trait ArtifactMetadataRepository: Send + Sync {
    fn put_object(&self, object: &ArtifactObjectRecord) -> Result<bool, String>;
    fn object(&self, sha256: &str) -> Result<Option<ArtifactObjectRecord>, String>;
    fn put_record(&self, record: &ArtifactRecord) -> Result<(), String>;
    fn record(&self, artifact_id: &str) -> Result<Option<ArtifactRecord>, String>;
    fn catalog_page(&self, query: ArtifactCatalogQuery) -> Result<ArtifactCatalogPage, String>;
    fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(), String>;
    fn remove_record(&self, artifact_id: &str) -> Result<(), String>;
    fn unreferenced_objects_before(
        &self,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<ArtifactObjectRecord>, String>;
    fn remove_object(&self, sha256: &str) -> Result<(), String>;
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
}

struct ArtifactStoreInner {
    blob_root: PathBuf,
    staging_root: PathBuf,
    repository: Arc<dyn ArtifactMetadataRepository>,
    config: ArtifactStoreConfig,
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
            inner: Arc::new(ArtifactStoreInner {
                staging_root: blob_root.join("staging"),
                blob_root,
                repository,
                config,
            }),
        }
    }

    #[must_use]
    pub fn config(&self) -> ArtifactStoreConfig {
        self.inner.config
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
        Ok(record.content_reference())
    }

    pub async fn begin(
        &self,
        descriptor: ArtifactWriteDescriptor,
    ) -> Result<Box<dyn ArtifactWriteSink>, ArtifactError> {
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
        Ok(Box::new(LocalArtifactWriter {
            store: self.clone(),
            descriptor,
            staging_path,
            file: Some(file),
            hasher: Sha256::new(),
            bytes: 0,
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
        let mut hasher = Sha256::new();
        let mut copied = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            copied = copied.saturating_add(read as u64);
            if copied > self.inner.config.max_object_bytes {
                let _ = fs::remove_file(&staging_path);
                return Err(ArtifactError::ObjectTooLarge);
            }
            hasher.update(&buffer[..read]);
            output.write_all(&buffer[..read])?;
        }
        output.sync_all()?;
        drop(output);
        self.publish_staged(
            descriptor,
            staging_path,
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

    pub fn read_blocking(
        &self,
        artifact: &ArtifactRef,
        authorized_scope: &str,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>, ArtifactError> {
        let id = parse_selector(&artifact.selector)?;
        let record = self
            .inner
            .repository
            .record(id)
            .map_err(ArtifactError::Metadata)?
            .ok_or(ArtifactError::NotFound)?;
        if record.visibility_scope != authorized_scope && record.visibility_scope != "public" {
            return Err(ArtifactError::Unauthorized);
        }
        validate_ref(artifact, &record)?;
        let object = self
            .inner
            .repository
            .object(&record.sha256)
            .map_err(ArtifactError::Metadata)?
            .ok_or(ArtifactError::NotFound)?;
        let range = normalize_range(range, object.bytes)?;
        let payload = match object.tier {
            ArtifactObjectTier::Compact => {
                let body = object.compact_body.ok_or_else(|| {
                    ArtifactError::Metadata("compact artifact body is missing".to_string())
                })?;
                body[range.start as usize..range.end as usize].to_vec()
            }
            ArtifactObjectTier::Blob => {
                let mut file = fs::File::open(self.blob_path(&object.sha256))?;
                file.seek(SeekFrom::Start(range.start))?;
                let mut body = vec![0_u8; (range.end - range.start) as usize];
                file.read_exact(&mut body)?;
                body
            }
        };
        self.inner
            .repository
            .touch(id, now_ms())
            .map_err(ArtifactError::Metadata)?;
        Ok(payload)
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
        self.inner
            .repository
            .pin(id, owner, until_ms)
            .map_err(ArtifactError::Metadata)
    }

    pub fn unpin(&self, artifact: &ArtifactRef, owner: &str) -> Result<(), ArtifactError> {
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
        let candidates = self
            .inner
            .repository
            .unreferenced_objects_before(before, limit)
            .map_err(ArtifactError::Metadata)?;
        let mut report = ArtifactGcReport::default();
        for object in candidates {
            report.examined = report.examined.saturating_add(1);
            if object.tier == ArtifactObjectTier::Blob {
                match fs::remove_file(self.blob_path(&object.sha256)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            self.inner
                .repository
                .remove_object(&object.sha256)
                .map_err(ArtifactError::Metadata)?;
            report.removed_objects = report.removed_objects.saturating_add(1);
            report.reclaimed_bytes = report.reclaimed_bytes.saturating_add(object.bytes);
            if stats.physical_bytes.saturating_sub(report.reclaimed_bytes)
                <= self.inner.config.gc_low_water_bytes
            {
                break;
            }
        }
        Ok(report)
    }

    fn publish_staged(
        &self,
        descriptor: ArtifactWriteDescriptor,
        staging_path: PathBuf,
        bytes: u64,
        sha256: String,
    ) -> Result<ArtifactRef, ArtifactError> {
        if bytes > self.inner.config.max_object_bytes {
            let _ = fs::remove_file(staging_path);
            return Err(ArtifactError::ObjectTooLarge);
        }
        let existing = self
            .inner
            .repository
            .object(&sha256)
            .map_err(ArtifactError::Metadata)?;
        let stats = self.stats()?;
        if existing.is_none()
            && stats.physical_bytes.saturating_add(bytes) > self.inner.config.total_quota_bytes
        {
            let _ = fs::remove_file(staging_path);
            return Err(ArtifactError::QuotaExceeded);
        }
        let now = now_ms();
        let selected_tier = if bytes <= self.inner.config.compact_threshold_bytes {
            ArtifactObjectTier::Compact
        } else {
            ArtifactObjectTier::Blob
        };
        let tier = existing
            .as_ref()
            .map_or_else(|| selected_tier.clone(), |object| object.tier.clone());
        if let Some(object) = existing.as_ref() {
            if object.bytes != bytes {
                let _ = fs::remove_file(&staging_path);
                return Err(ArtifactError::Metadata(format!(
                    "artifact object `{sha256}` has conflicting byte counts"
                )));
            }
            if object.tier == ArtifactObjectTier::Compact && object.compact_body.is_none() {
                let _ = fs::remove_file(&staging_path);
                return Err(ArtifactError::Metadata(format!(
                    "compact artifact object `{sha256}` has no body"
                )));
            }
        }
        if tier == ArtifactObjectTier::Blob {
            // PostgreSQL metadata can outlive a process-local blob directory.
            // A repeated content-addressed write must repair a missing local
            // object instead of trusting the global hash row and publishing a
            // dangling ArtifactRef. Production multi-instance deployments use
            // one shared configured blob root; this branch also makes restart
            // and root migration self-healing.
            let target = self.blob_path(&sha256);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            if !target.exists() {
                fs::rename(&staging_path, &target)?;
            }
        }
        if existing.is_none() {
            let compact_body = if tier == ArtifactObjectTier::Compact {
                Some(fs::read(&staging_path)?)
            } else {
                None
            };
            self.inner
                .repository
                .put_object(&ArtifactObjectRecord {
                    sha256: sha256.clone(),
                    bytes,
                    tier: tier.clone(),
                    compact_body,
                    created_at_ms: now,
                })
                .map_err(ArtifactError::Metadata)?;
        }
        let _ = fs::remove_file(&staging_path);
        let artifact_id = format!("art_{}", Uuid::new_v4().simple());
        self.inner
            .repository
            .put_record(&ArtifactRecord {
                artifact_id: artifact_id.clone(),
                sha256: sha256.clone(),
                bytes,
                media_type: descriptor.media_type.clone(),
                visibility_scope: descriptor.visibility_scope.clone(),
                tier,
                created_at_ms: now,
                last_access_at_ms: now,
            })
            .map_err(ArtifactError::Metadata)?;
        Ok(ArtifactRef::durable(
            format!("{ARTIFACT_SELECTOR_PREFIX}{artifact_id}"),
            sha256,
            bytes,
            descriptor.media_type,
            descriptor.visibility_scope,
        ))
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
    staging_path: PathBuf,
    file: Option<tokio::fs::File>,
    hasher: Sha256,
    bytes: u64,
    closed: bool,
}

impl Drop for LocalArtifactWriter {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.staging_path);
    }
}

#[async_trait]
impl ArtifactWriteSink for LocalArtifactWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), ArtifactError> {
        if self.closed {
            return Err(ArtifactError::WriterClosed);
        }
        self.bytes = self.bytes.saturating_add(chunk.len() as u64);
        if self.bytes > self.store.inner.config.max_object_bytes {
            self.abort().await?;
            return Err(ArtifactError::ObjectTooLarge);
        }
        self.hasher.update(chunk);
        self.file
            .as_mut()
            .ok_or(ArtifactError::WriterClosed)?
            .write_all(chunk)
            .await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<ArtifactRef, ArtifactError> {
        if self.closed {
            return Err(ArtifactError::WriterClosed);
        }
        let mut file = self.file.take().ok_or(ArtifactError::WriterClosed)?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        self.closed = true;
        let descriptor = self.descriptor.clone();
        let staging = self.staging_path.clone();
        let bytes = self.bytes;
        let sha256 = format!("sha256:{:x}", self.hasher.clone().finalize());
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            store.publish_staged(descriptor, staging, bytes, sha256)
        })
        .await
        .map_err(|error| ArtifactError::Blocking(error.to_string()))?
    }

    async fn abort(&mut self) -> Result<(), ArtifactError> {
        if self.closed {
            return Ok(());
        }
        self.file.take();
        self.closed = true;
        match tokio::fs::remove_file(&self.staging_path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
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
    records: HashMap<String, ArtifactRecord>,
    catalog_sequence: u64,
    creation_sequence: HashMap<String, u64>,
    catalog_invalidations: std::collections::BTreeMap<String, i64>,
    pins: HashMap<(String, String), u64>,
}

impl ArtifactMetadataRepository for EphemeralArtifactRepository {
    fn put_object(&self, object: &ArtifactObjectRecord) -> Result<bool, String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        if state.objects.contains_key(&object.sha256) {
            return Ok(false);
        }
        state.objects.insert(object.sha256.clone(), object.clone());
        Ok(true)
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

    fn put_record(&self, record: &ArtifactRecord) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
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
        let mut records = state
            .records
            .values()
            .filter(|record| {
                query.scopes.contains(&record.visibility_scope)
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
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|a, b| a.artifact_id.cmp(&b.artifact_id));
        let limit = query.limit.clamp(1, 128);
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
                    && !state
                        .records
                        .values()
                        .any(|record| record.sha256 == object.sha256)
            })
            .cloned()
            .collect::<Vec<_>>();
        objects.sort_by_key(|object| object.created_at_ms);
        objects.truncate(limit);
        Ok(objects)
    }

    fn remove_object(&self, sha256: &str) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        if !state.records.values().any(|record| record.sha256 == sha256) {
            state.objects.remove(sha256);
        }
        Ok(())
    }

    fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|error| error.to_string())?;
        if !state.records.contains_key(artifact_id) {
            return Err("artifact record does not exist".to_string());
        }
        state
            .pins
            .insert((artifact_id.to_string(), owner.to_string()), until_ms);
        Ok(())
    }

    fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String> {
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
    use super::*;

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
        assert!(matches!(
            store.read(&artifact, "team:other", None).await,
            Err(ArtifactError::Unauthorized)
        ));
    }
}
