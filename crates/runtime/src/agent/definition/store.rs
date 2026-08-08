use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent::{
    AgentDefinitionId, AgentDefinitionManifest, AgentDefinitionRevision,
    AgentDefinitionRevisionRef, DefaultPointer, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseChannel, RevisionLifecycle, RevisionSelector, ValidationError,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

use super::validation::{
    build_revision, ensure_same_revision_ref, manifest_yaml, verify_read_revision,
    INSTRUCTIONS_FILE_NAME, MANIFEST_FILE_NAME,
};

const AGENTS_DIRECTORY: &str = "agents";
const REVISIONS_DIRECTORY: &str = "revisions";
const RELEASES_DIRECTORY: &str = "release-assignments";
const POINTER_FILE_NAME: &str = "default-pointer.json";

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Supplies the registered root for each explicit Definition scope.
///
/// Runtime composition owns the decision of whether the roots live in a
/// workspace, user profile, installation bundle, database projection, or
/// another registered storage layout.  This definition domain never computes a
/// config-home path itself.
pub trait DefinitionStorageLayout: Send + Sync + fmt::Debug {
    fn root_for_scope(&self, scope: DefinitionScope) -> Result<PathBuf, DefinitionStoreError>;
}

/// A simple path adapter suitable for tests and the future storage registry
/// composition root.  Each scope is explicit, so an identical local name
/// cannot shadow another scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedDefinitionLayout {
    builtin_root: PathBuf,
    user_root: PathBuf,
    workspace_root: PathBuf,
}

impl ScopedDefinitionLayout {
    #[must_use]
    pub fn new(
        builtin_root: impl Into<PathBuf>,
        user_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            builtin_root: builtin_root.into(),
            user_root: user_root.into(),
            workspace_root: workspace_root.into(),
        }
    }
}

impl DefinitionStorageLayout for ScopedDefinitionLayout {
    fn root_for_scope(&self, scope: DefinitionScope) -> Result<PathBuf, DefinitionStoreError> {
        Ok(match scope {
            DefinitionScope::Builtin => self.builtin_root.clone(),
            DefinitionScope::User => self.user_root.clone(),
            DefinitionScope::Workspace => self.workspace_root.clone(),
        })
    }
}

#[derive(Debug, Error)]
pub enum DefinitionStoreError {
    #[error("definition storage domain `{domain}` is not registered in StorageLayout")]
    UnregisteredStorageRoot { domain: String },
    #[error("definition storage root for `{scope}` is invalid: {reason}")]
    InvalidStorageRoot { scope: String, reason: String },
    #[error("definition storage I/O failed at `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("definition manifest serialization failed: {0}")]
    Serialize(String),
    #[error("definition manifest deserialization failed: {0}")]
    Deserialize(String),
    #[error("definition contract is invalid: {0}")]
    Contract(ValidationError),
    #[error("AGENT.md is invalid: {0}")]
    InvalidAgentMarkdown(String),
    #[error("definition import is invalid: {0}")]
    InvalidImport(String),
    #[error(
        "definition `{}` revision {} was not found",
        .definition_id.as_str(),
        .revision
    )]
    RevisionNotFound {
        definition_id: AgentDefinitionId,
        revision: u64,
    },
    #[error("definition revision `{}` revision {} already exists with different content", .revision.definition_id.as_str(), .revision.revision)]
    RevisionConflict {
        revision: AgentDefinitionRevisionRef,
    },
    #[error("definition revision `{}` revision {} is corrupt: {reason}", .revision.definition_id.as_str(), .revision.revision)]
    CorruptRevision {
        revision: AgentDefinitionRevisionRef,
        reason: String,
    },
    #[error("digest mismatch for {subject}: expected {expected}, got {actual}")]
    DigestMismatch {
        subject: String,
        expected: String,
        actual: String,
    },
    #[error("release assignment does not match the stored revision content")]
    ReleaseDigestMismatch,
    #[error("release assignment `{}` revision {} channel {channel:?} already exists with different content", .revision.definition_id.as_str(), .revision.revision)]
    ReleaseAssignmentConflict {
        revision: AgentDefinitionRevisionRef,
        channel: ReleaseChannel,
    },
    #[error("default pointer has a manual exact pin and cannot be overwritten by latest")]
    ManualPinProtected,
    #[error("default pointer for `{}` does not exist", .0.as_str())]
    DefaultPointerNotFound(AgentDefinitionId),
    #[error("default pointer for `{}` cannot resolve: {}", .0.as_str(), .1)]
    UnresolvablePointer(AgentDefinitionId, String),
}

impl DefinitionStoreError {
    pub(crate) fn serialize(error: serde_yaml::Error) -> Self {
        Self::Serialize(error.to_string())
    }

    pub(crate) fn deserialize(error: serde_yaml::Error) -> Self {
        Self::Deserialize(error.to_string())
    }

    pub(crate) fn contract(error: ValidationError) -> Self {
        Self::Contract(error)
    }

    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAgentDefinitionRevision {
    pub revision: AgentDefinitionRevision,
    pub agent_markdown: String,
}

/// Immutable Agent Definition revisions plus release and pointer projections.
#[derive(Debug)]
pub struct AgentDefinitionStore<L> {
    layout: L,
}

impl<L> AgentDefinitionStore<L>
where
    L: DefinitionStorageLayout,
{
    #[must_use]
    pub fn new(layout: L) -> Self {
        Self { layout }
    }

    #[must_use]
    pub fn layout(&self) -> &L {
        &self.layout
    }

    /// Persist one immutable revision. A semantically identical re-write is
    /// idempotent even when a newer serializer materializes fields that an
    /// older manifest omitted through serde defaults. Any contract or Markdown
    /// change under the same `(qualified id, revision)` is rejected.
    pub fn store_revision(
        &self,
        manifest: AgentDefinitionManifest,
        agent_markdown: &str,
    ) -> Result<StoredAgentDefinitionRevision, DefinitionStoreError> {
        let (revision, normalized_markdown) = build_revision(manifest, agent_markdown)?;
        let revision_dir = self.revision_dir(&revision.revision_ref)?;
        if revision_dir.exists() {
            let existing = self.read_revision(&revision.revision_ref)?;
            if existing.revision.manifest == revision.manifest
                && existing.agent_markdown == normalized_markdown
            {
                return Ok(existing);
            }
            return Err(DefinitionStoreError::RevisionConflict {
                revision: revision.revision_ref,
            });
        }

        let parent =
            revision_dir
                .parent()
                .ok_or_else(|| DefinitionStoreError::CorruptRevision {
                    revision: revision.revision_ref.clone(),
                    reason: "revision path has no parent".to_string(),
                })?;
        create_dir_all(parent)?;
        let staging = unique_staging_dir(parent)?;
        let persisted = (|| {
            create_dir_all(&staging)?;
            let manifest = manifest_yaml(&revision.manifest)?;
            write_new_file(&staging.join(MANIFEST_FILE_NAME), manifest.as_bytes())?;
            write_new_file(
                &staging.join(INSTRUCTIONS_FILE_NAME),
                normalized_markdown.as_bytes(),
            )?;
            sync_directory(&staging)?;
            fs::rename(&staging, &revision_dir)
                .map_err(|error| DefinitionStoreError::io(&revision_dir, error))?;
            sync_directory(parent)?;
            Ok(StoredAgentDefinitionRevision {
                revision,
                agent_markdown: normalized_markdown,
            })
        })();
        if persisted.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        persisted
    }

    pub fn read_revision(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
    ) -> Result<StoredAgentDefinitionRevision, DefinitionStoreError> {
        let directory = self.revision_dir(revision_ref)?;
        let manifest_path = directory.join(MANIFEST_FILE_NAME);
        let instructions_path = directory.join(INSTRUCTIONS_FILE_NAME);
        if !manifest_path.is_file() || !instructions_path.is_file() {
            if !directory.exists() {
                return Err(DefinitionStoreError::RevisionNotFound {
                    definition_id: revision_ref.definition_id.clone(),
                    revision: revision_ref.revision,
                });
            }
            return Err(DefinitionStoreError::CorruptRevision {
                revision: revision_ref.clone(),
                reason: format!(
                    "expected both `{MANIFEST_FILE_NAME}` and `{INSTRUCTIONS_FILE_NAME}`"
                ),
            });
        }
        let manifest_bytes = read_file(&manifest_path)?;
        let instruction_bytes = read_file(&instructions_path)?;
        let (revision, agent_markdown) = verify_read_revision(&manifest_bytes, &instruction_bytes)?;
        ensure_same_revision_ref(revision_ref, &revision.revision_ref)?;
        Ok(StoredAgentDefinitionRevision {
            revision,
            agent_markdown,
        })
    }

    pub fn list_revisions(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<Vec<StoredAgentDefinitionRevision>, DefinitionStoreError> {
        let root = self
            .definition_dir(definition_id)?
            .join(REVISIONS_DIRECTORY);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let entries =
            fs::read_dir(&root).map_err(|error| DefinitionStoreError::io(&root, error))?;
        let mut revisions = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| DefinitionStoreError::io(&root, error))?;
            if !entry
                .file_type()
                .map_err(|error| DefinitionStoreError::io(entry.path(), error))?
                .is_dir()
            {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(number) = name.parse::<u64>() else {
                continue;
            };
            if number == 0 {
                continue;
            }
            let revision_ref = AgentDefinitionRevisionRef::new(definition_id.clone(), number)
                .map_err(DefinitionStoreError::contract)?;
            revisions.push(self.read_revision(&revision_ref)?);
        }
        revisions.sort_by_key(|stored| stored.revision.revision_ref.revision);
        Ok(revisions)
    }

    /// Enumerate scope-qualified Definition identities from registered roots.
    ///
    /// This is a Store-owned projection, not a Gateway directory discovery
    /// mechanism. Every discovered manifest is parsed and validated, and its
    /// declared scope must equal the root being traversed. A corrupt or
    /// misplaced manifest fails closed instead of being silently omitted.
    pub fn list_definition_ids(&self) -> Result<Vec<AgentDefinitionId>, DefinitionStoreError> {
        let mut ids = std::collections::BTreeSet::new();
        for scope in [
            DefinitionScope::Builtin,
            DefinitionScope::User,
            DefinitionScope::Workspace,
        ] {
            let root = self.layout.root_for_scope(scope)?.join(AGENTS_DIRECTORY);
            let mut manifests = Vec::new();
            collect_manifest_files(&root, MANIFEST_FILE_NAME, &mut manifests)?;
            for manifest_path in manifests {
                let manifest: AgentDefinitionManifest =
                    serde_yaml::from_slice(&read_file(&manifest_path)?)
                        .map_err(DefinitionStoreError::deserialize)?;
                manifest
                    .validate()
                    .map_err(DefinitionStoreError::contract)?;
                if manifest.definition_id.scope() != scope {
                    return Err(DefinitionStoreError::InvalidImport(format!(
                        "manifest `{}` declares scope `{}` under `{}` root",
                        manifest_path.display(),
                        manifest.definition_id.scope().as_str(),
                        scope.as_str(),
                    )));
                }
                ids.insert(manifest.definition_id.as_str().to_string());
            }
        }
        ids.into_iter()
            .map(|id| {
                AgentDefinitionId::try_from(id.as_str()).map_err(DefinitionStoreError::contract)
            })
            .collect()
    }

    /// Save the current release-assignment projection.  Assignment content is
    /// idempotent and cannot be silently replaced under the same revision and
    /// channel.  A future event-store adapter can replay its authoritative
    /// lifecycle into this projection without changing resolver semantics.
    pub fn record_release_assignment(
        &self,
        assignment: &ReleaseAssignment,
    ) -> Result<(), DefinitionStoreError> {
        assignment
            .validate()
            .map_err(DefinitionStoreError::contract)?;
        let stored = self.read_revision(&assignment.revision_ref)?;
        if stored.revision.content_digest != assignment.content_digest {
            return Err(DefinitionStoreError::ReleaseDigestMismatch);
        }
        let path = self.release_assignment_path(&assignment.revision_ref, assignment.channel)?;
        if path.exists() {
            let existing: ReleaseAssignment = self.read_json(&path)?;
            if existing == *assignment {
                return Ok(());
            }
            if existing.scope != assignment.scope
                || existing.revision_ref != assignment.revision_ref
                || existing.channel != assignment.channel
                || existing.content_digest != assignment.content_digest
            {
                return Err(DefinitionStoreError::ReleaseAssignmentConflict {
                    revision: assignment.revision_ref.clone(),
                    channel: assignment.channel,
                });
            }
            // A release assignment is a mutable state projection, unlike a
            // Definition revision.  Its immutable identity remains bound to
            // the exact revision and digest above; status/approval changes are
            // atomically replaced here until a scoped event-store adapter owns
            // the append-only history.
            return self.write_json_replace(&path, assignment);
        }
        self.write_json_immutable(&path, assignment, || {
            DefinitionStoreError::ReleaseAssignmentConflict {
                revision: assignment.revision_ref.clone(),
                channel: assignment.channel,
            }
        })
    }

    pub fn release_assignments(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<Vec<ReleaseAssignment>, DefinitionStoreError> {
        let root = self.definition_dir(definition_id)?.join(RELEASES_DIRECTORY);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut assignments = Vec::new();
        collect_json_files(&root, &mut assignments)?;
        assignments.retain(|assignment: &ReleaseAssignment| {
            assignment.revision_ref.definition_id == *definition_id
        });
        for assignment in &assignments {
            assignment
                .validate()
                .map_err(DefinitionStoreError::contract)?;
        }
        assignments.sort_by(|left, right| {
            left.revision_ref
                .revision
                .cmp(&right.revision_ref.revision)
                .then_with(|| channel_order(left.channel).cmp(&channel_order(right.channel)))
        });
        Ok(assignments)
    }

    /// Persist the default selection rule.  A manual exact pin is authoritative
    /// and a later `LatestApprovedStable` request cannot erase it.
    pub fn set_default_pointer(
        &self,
        pointer: &DefaultPointer,
    ) -> Result<(), DefinitionStoreError> {
        pointer.validate().map_err(DefinitionStoreError::contract)?;
        if let RevisionSelector::ExactApprovedRevision { revision } = pointer.selector {
            self.ensure_eligible_revision(&pointer.definition_id, revision)?;
        }
        let path = self.pointer_path(&pointer.definition_id)?;
        if path.exists() {
            let existing: DefaultPointer = self.read_json(&path)?;
            existing
                .validate()
                .map_err(DefinitionStoreError::contract)?;
            if matches!(
                (&existing.selector, &pointer.selector),
                (
                    RevisionSelector::ExactApprovedRevision { .. },
                    RevisionSelector::LatestApprovedStable
                )
            ) {
                return Err(DefinitionStoreError::ManualPinProtected);
            }
            if existing == *pointer {
                return Ok(());
            }
        }
        self.write_json_replace(&path, pointer)
    }

    pub fn default_pointer(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<DefaultPointer, DefinitionStoreError> {
        let path = self.pointer_path(definition_id)?;
        if !path.is_file() {
            return Err(DefinitionStoreError::DefaultPointerNotFound(
                definition_id.clone(),
            ));
        }
        let pointer: DefaultPointer = self.read_json(&path)?;
        pointer.validate().map_err(DefinitionStoreError::contract)?;
        if pointer.definition_id != *definition_id {
            return Err(DefinitionStoreError::UnresolvablePointer(
                definition_id.clone(),
                "pointer definition_id does not match its path".to_string(),
            ));
        }
        Ok(pointer)
    }

    pub(crate) fn ensure_eligible_revision(
        &self,
        definition_id: &AgentDefinitionId,
        revision: u64,
    ) -> Result<StoredAgentDefinitionRevision, DefinitionStoreError> {
        let revision_ref = AgentDefinitionRevisionRef::new(definition_id.clone(), revision)
            .map_err(DefinitionStoreError::contract)?;
        let stored = self.read_revision(&revision_ref)?;
        if stored.revision.manifest.lifecycle != RevisionLifecycle::Published {
            return Err(DefinitionStoreError::UnresolvablePointer(
                definition_id.clone(),
                format!("revision {revision} is not published"),
            ));
        }
        let eligible = self
            .release_assignments(definition_id)?
            .into_iter()
            .any(|assignment| {
                assignment.revision_ref == revision_ref
                    && assignment.content_digest == stored.revision.content_digest
                    && assignment_is_eligible(&assignment)
            });
        if !eligible {
            return Err(DefinitionStoreError::UnresolvablePointer(
                definition_id.clone(),
                format!("revision {revision} has no active eligible stable release"),
            ));
        }
        Ok(stored)
    }

    pub(crate) fn latest_eligible_revision(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<StoredAgentDefinitionRevision, DefinitionStoreError> {
        let mut candidates = self
            .release_assignments(definition_id)?
            .into_iter()
            .filter(assignment_is_eligible)
            .collect::<Vec<_>>();
        candidates.sort_by_key(|assignment| assignment.revision_ref.revision);
        let assignment = candidates.pop().ok_or_else(|| {
            DefinitionStoreError::UnresolvablePointer(
                definition_id.clone(),
                "no active eligible stable release exists".to_string(),
            )
        })?;
        self.ensure_eligible_revision(definition_id, assignment.revision_ref.revision)
    }

    fn definition_dir(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<PathBuf, DefinitionStoreError> {
        let mut directory = self
            .layout
            .root_for_scope(definition_id.scope())?
            .join(AGENTS_DIRECTORY);
        for segment in definition_id.as_str().split('/').skip(1) {
            if segment.is_empty() || Path::new(segment).components().count() != 1 {
                return Err(DefinitionStoreError::InvalidImport(format!(
                    "unsafe qualified definition id `{}`",
                    definition_id.as_str()
                )));
            }
            directory.push(segment);
        }
        Ok(directory)
    }

    fn revision_dir(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
    ) -> Result<PathBuf, DefinitionStoreError> {
        Ok(self
            .definition_dir(&revision_ref.definition_id)?
            .join(REVISIONS_DIRECTORY)
            .join(revision_ref.revision.to_string()))
    }

    fn release_assignment_path(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
        channel: ReleaseChannel,
    ) -> Result<PathBuf, DefinitionStoreError> {
        Ok(self
            .definition_dir(&revision_ref.definition_id)?
            .join(RELEASES_DIRECTORY)
            .join(revision_ref.revision.to_string())
            .join(format!("{}.json", channel_name(channel))))
    }

    fn pointer_path(
        &self,
        definition_id: &AgentDefinitionId,
    ) -> Result<PathBuf, DefinitionStoreError> {
        Ok(self.definition_dir(definition_id)?.join(POINTER_FILE_NAME))
    }

    fn write_json_immutable<T, F>(
        &self,
        path: &Path,
        value: &T,
        on_conflict: F,
    ) -> Result<(), DefinitionStoreError>
    where
        T: Serialize + DeserializeOwned + PartialEq,
        F: FnOnce() -> DefinitionStoreError,
    {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| DefinitionStoreError::Serialize(error.to_string()))?;
        if path.exists() {
            let existing: T = self.read_json(path)?;
            if existing == *value {
                return Ok(());
            }
            return Err(on_conflict());
        }
        write_atomic(path, &bytes)
    }

    fn write_json_replace<T: Serialize>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), DefinitionStoreError> {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| DefinitionStoreError::Serialize(error.to_string()))?;
        write_atomic(path, &bytes)
    }

    fn read_json<T: DeserializeOwned>(&self, path: &Path) -> Result<T, DefinitionStoreError> {
        let bytes = read_file(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| DefinitionStoreError::Deserialize(error.to_string()))
    }
}

pub(crate) fn assignment_is_eligible(assignment: &ReleaseAssignment) -> bool {
    if assignment.channel != ReleaseChannel::Stable
        || assignment.status != ReleaseAssignmentStatus::Active
    {
        return false;
    }
    match assignment.scope {
        DefinitionScope::Builtin => assignment.is_active_stable(),
        DefinitionScope::User | DefinitionScope::Workspace => {
            assignment.is_active_approved_stable()
        }
    }
}

fn channel_name(channel: ReleaseChannel) -> &'static str {
    match channel {
        ReleaseChannel::Shadow => "shadow",
        ReleaseChannel::Canary => "canary",
        ReleaseChannel::Stable => "stable",
    }
}

fn channel_order(channel: ReleaseChannel) -> u8 {
    match channel {
        ReleaseChannel::Shadow => 0,
        ReleaseChannel::Canary => 1,
        ReleaseChannel::Stable => 2,
    }
}

fn create_dir_all(path: &Path) -> Result<(), DefinitionStoreError> {
    fs::create_dir_all(path).map_err(|error| DefinitionStoreError::io(path, error))
}

fn read_file(path: &Path) -> Result<Vec<u8>, DefinitionStoreError> {
    fs::read(path).map_err(|error| DefinitionStoreError::io(path, error))
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), DefinitionStoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| DefinitionStoreError::io(path, error))?;
    file.write_all(bytes)
        .map_err(|error| DefinitionStoreError::io(path, error))?;
    file.sync_all()
        .map_err(|error| DefinitionStoreError::io(path, error))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), DefinitionStoreError> {
    let parent = path.parent().ok_or_else(|| {
        DefinitionStoreError::InvalidImport(format!("path `{}` has no parent", path.display()))
    })?;
    create_dir_all(parent)?;
    let staging = unique_staging_file(parent, path.file_name().and_then(|value| value.to_str()))?;
    let result = (|| {
        write_new_file(&staging, bytes)?;
        fs::rename(&staging, path).map_err(|error| DefinitionStoreError::io(path, error))?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

fn unique_staging_dir(parent: &Path) -> Result<PathBuf, DefinitionStoreError> {
    for _ in 0..32 {
        let candidate = parent.join(staging_name("revision"));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(DefinitionStoreError::io(&candidate, error)),
        }
    }
    Err(DefinitionStoreError::InvalidImport(
        "could not allocate an atomic revision staging directory".to_string(),
    ))
}

fn unique_staging_file(parent: &Path, stem: Option<&str>) -> Result<PathBuf, DefinitionStoreError> {
    for _ in 0..32 {
        let candidate = parent.join(format!(
            ".{}.{}",
            stem.unwrap_or("record"),
            staging_name("tmp")
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(DefinitionStoreError::InvalidImport(
        "could not allocate an atomic write staging file".to_string(),
    ))
}

fn staging_name(kind: &str) -> String {
    let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!(".{kind}-{}-{nanos}-{sequence}", std::process::id())
}

fn sync_directory(path: &Path) -> Result<(), DefinitionStoreError> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| DefinitionStoreError::io(path, error))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn collect_json_files<T: DeserializeOwned>(
    root: &Path,
    output: &mut Vec<T>,
) -> Result<(), DefinitionStoreError> {
    let entries = fs::read_dir(root).map_err(|error| DefinitionStoreError::io(root, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| DefinitionStoreError::io(root, error))?;
        let kind = entry
            .file_type()
            .map_err(|error| DefinitionStoreError::io(entry.path(), error))?;
        if kind.is_dir() {
            collect_json_files(&entry.path(), output)?;
        } else if kind.is_file() && entry.path().extension().is_some_and(|ext| ext == "json") {
            let bytes = read_file(&entry.path())?;
            output.push(
                serde_json::from_slice(&bytes)
                    .map_err(|error| DefinitionStoreError::Deserialize(error.to_string()))?,
            );
        }
    }
    Ok(())
}

fn collect_manifest_files(
    root: &Path,
    file_name: &str,
    output: &mut Vec<PathBuf>,
) -> Result<(), DefinitionStoreError> {
    if !root.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(root).map_err(|error| DefinitionStoreError::io(root, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| DefinitionStoreError::io(root, error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| DefinitionStoreError::io(&path, error))?;
        if file_type.is_symlink() {
            return Err(DefinitionStoreError::InvalidImport(format!(
                "definition storage must not contain symlink `{}`",
                path.display()
            )));
        }
        if file_type.is_dir() {
            collect_manifest_files(&path, file_name, output)?;
        } else if file_type.is_file()
            && path.file_name().and_then(|value| value.to_str()) == Some(file_name)
        {
            output.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests_support {
    use harness_contract::agent::{
        AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
        AgentDefinitionManifest, AgentEvaluationContract, AgentExecutorPolicy, AgentModelPolicy,
        AgentOutputContract, CognitiveReadScope, CognitiveWriteMode, DefinitionScope,
        RevisionLifecycle,
    };
    use tempfile::TempDir;

    use super::{AgentDefinitionStore, ScopedDefinitionLayout};

    pub fn markdown() -> &'static str {
        "# Reviewer\n\nReview implementation evidence.\n"
    }

    pub fn manifest(
        scope: DefinitionScope,
        revision: u64,
        lifecycle: RevisionLifecycle,
    ) -> AgentDefinitionManifest {
        AgentDefinitionManifest {
            api_version: "cowd.agent/v1".to_string(),
            definition_id: AgentDefinitionId::new(scope, "reviewer").unwrap(),
            revision,
            name: "Reviewer".to_string(),
            description: "Reviews implementation evidence".to_string(),
            lifecycle,
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "coding-balanced".to_string(),
                allowed_models: vec!["gpt-5".to_string()],
                fallback_allowed: true,
            },
            cognitive_policy: AgentCognitivePolicy {
                context_profile: "sub-agent".to_string(),
                read_scopes: vec![CognitiveReadScope::Session],
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
            },
            capability_contract: AgentCapabilityContract {
                capability_ceiling: vec![AgentCapability::Read],
                skill_refs: vec![],
                approval_required_for: vec![],
            },
            output_contract: AgentOutputContract::reviewable(),
            evaluation: AgentEvaluationContract::single_release_gate("review", "evidence"),
            instructions_digest: super::super::validation::digest_hex(markdown().as_bytes()),
        }
    }

    pub fn store(temp: &TempDir) -> AgentDefinitionStore<ScopedDefinitionLayout> {
        AgentDefinitionStore::new(ScopedDefinitionLayout::new(
            temp.path().join("builtin-definitions"),
            temp.path().join("user-definitions"),
            temp.path().join("workspace-definitions"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use harness_contract::agent::{ReleaseAuthorization, RevisionLifecycle};
    use tempfile::TempDir;

    use super::*;

    use super::tests_support::{manifest, markdown, store};

    #[test]
    fn revision_is_atomic_immutable_and_idempotent() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        let duplicate = store
            .store_revision(
                manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        assert_eq!(stored, duplicate);

        let mut conflicting = manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft);
        conflicting.description = "Different definition".to_string();
        assert!(matches!(
            store.store_revision(conflicting, markdown()),
            Err(DefinitionStoreError::RevisionConflict { .. })
        ));
    }

    #[test]
    fn revision_is_idempotent_across_defaulted_manifest_schema_growth() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let manifest = manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft);
        let stored = store.store_revision(manifest.clone(), markdown()).unwrap();
        let revision_dir = store.revision_dir(&stored.revision.revision_ref).unwrap();
        let manifest_path = revision_dir.join(MANIFEST_FILE_NAME);
        let legacy_yaml = fs::read_to_string(&manifest_path)
            .unwrap()
            .lines()
            .filter(|line| {
                !line.contains("minimum_improvement_micros")
                    && !line.contains("minimum_superiority_confidence_basis_points")
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(&manifest_path, legacy_yaml).unwrap();

        let legacy = store.read_revision(&stored.revision.revision_ref).unwrap();
        assert_eq!(legacy.revision.manifest, manifest);
        assert_ne!(
            legacy.revision.content_digest,
            stored.revision.content_digest
        );

        let duplicate = store.store_revision(manifest, markdown()).unwrap();
        assert_eq!(duplicate, legacy);
    }

    #[test]
    fn stored_revision_detects_missing_or_tampered_content() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::User, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        let revision_dir = store.revision_dir(&stored.revision.revision_ref).unwrap();
        fs::write(revision_dir.join(INSTRUCTIONS_FILE_NAME), "# Tampered\n").unwrap();
        assert!(matches!(
            store.read_revision(&stored.revision.revision_ref),
            Err(DefinitionStoreError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn release_and_pointer_are_explicitly_scoped() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::User, 1, RevisionLifecycle::Published),
                markdown(),
            )
            .unwrap();
        let assignment = ReleaseAssignment {
            scope: DefinitionScope::User,
            revision_ref: stored.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/user-release-1".to_string(),
            },
            content_digest: stored.revision.content_digest.clone(),
        };
        store.record_release_assignment(&assignment).unwrap();
        let pointer = DefaultPointer::latest(
            DefinitionScope::User,
            stored.revision.revision_ref.definition_id.clone(),
            ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/default-1".to_string(),
            },
        );
        store.set_default_pointer(&pointer).unwrap();
        assert_eq!(
            store.default_pointer(&pointer.definition_id).unwrap(),
            pointer
        );
    }

    #[test]
    fn release_assignment_cannot_name_a_different_revision_digest() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::User, 1, RevisionLifecycle::Published),
                markdown(),
            )
            .unwrap();
        let result = store.record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::User,
            revision_ref: stored.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "approval/wrong-digest".to_string(),
            },
            content_digest: "0".repeat(64),
        });
        assert!(matches!(
            result,
            Err(DefinitionStoreError::ReleaseDigestMismatch)
        ));
    }
}
