use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent::{
    DefinitionScope, ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel,
    RevisionLifecycle, RevisionSelector, ValidationError,
};
use harness_contract::team::{
    TeamTemplateDefinitionId, TeamTemplateManifest, TeamTemplateRevision, TeamTemplateRevisionRef,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::validation::{
    build_revision, ensure_same_revision_ref, manifest_yaml, verify_read_revision,
    INSTRUCTIONS_FILE_NAME, MANIFEST_FILE_NAME,
};

const TEAMS_DIRECTORY: &str = "teams";
const REVISIONS_DIRECTORY: &str = "revisions";
const RELEASES_DIRECTORY: &str = "release-assignments";
const POINTER_FILE_NAME: &str = "default-pointer.json";
const INTEGRITY_FILE_NAME: &str = "revision.json";

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Supplies the registered root for one explicit Team Definition scope.
pub trait TeamTemplateStorageLayout: Send + Sync + fmt::Debug {
    fn root_for_scope(&self, scope: DefinitionScope) -> Result<PathBuf, TeamDefinitionStoreError>;
}

/// Simple test and composition-root adapter.  Scope is explicit, so a local
/// team name cannot shadow an identically named user or builtin asset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedTeamTemplateLayout {
    builtin_root: PathBuf,
    user_root: PathBuf,
    workspace_root: PathBuf,
}

impl ScopedTeamTemplateLayout {
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

impl TeamTemplateStorageLayout for ScopedTeamTemplateLayout {
    fn root_for_scope(&self, scope: DefinitionScope) -> Result<PathBuf, TeamDefinitionStoreError> {
        Ok(match scope {
            DefinitionScope::Builtin => self.builtin_root.clone(),
            DefinitionScope::User => self.user_root.clone(),
            DefinitionScope::Workspace => self.workspace_root.clone(),
        })
    }
}

#[derive(Debug, Error)]
pub enum TeamDefinitionStoreError {
    #[error("team definition storage domain `{domain}` is not registered in StorageLayout")]
    UnregisteredStorageRoot { domain: String },
    #[error("team definition storage root for `{scope}` is invalid: {reason}")]
    InvalidStorageRoot { scope: String, reason: String },
    #[error("team definition storage I/O failed at `{path}`: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("team definition manifest serialization failed: {0}")]
    Serialize(String),
    #[error("team definition manifest deserialization failed: {0}")]
    Deserialize(String),
    #[error("team definition contract is invalid: {0}")]
    Contract(ValidationError),
    #[error("TEAM.md is invalid: {0}")]
    InvalidTeamMarkdown(String),
    #[error("team definition import is invalid: {0}")]
    InvalidImport(String),
    #[error("team definition `{}` revision {} was not found", .template_id.as_str(), .revision)]
    RevisionNotFound {
        template_id: TeamTemplateDefinitionId,
        revision: u64,
    },
    #[error("team definition revision `{}` revision {} already exists with different content", .revision.template_id.as_str(), .revision.revision)]
    RevisionConflict { revision: TeamTemplateRevisionRef },
    #[error("team definition revision `{}` revision {} is corrupt: {reason}", .revision.template_id.as_str(), .revision.revision)]
    CorruptRevision {
        revision: TeamTemplateRevisionRef,
        reason: String,
    },
    #[error("digest mismatch for {subject}: expected {expected}, got {actual}")]
    DigestMismatch {
        subject: String,
        expected: String,
        actual: String,
    },
    #[error("team release assignment does not match stored revision content")]
    ReleaseDigestMismatch,
    #[error("team release assignment `{}` revision {} channel {channel:?} already exists with different content", .revision.template_id.as_str(), .revision.revision)]
    ReleaseAssignmentConflict {
        revision: TeamTemplateRevisionRef,
        channel: ReleaseChannel,
    },
    #[error("team default pointer has a manual exact pin and cannot be overwritten by latest")]
    ManualPinProtected,
    #[error("team default pointer for `{}` does not exist", .0.as_str())]
    DefaultPointerNotFound(TeamTemplateDefinitionId),
    #[error("team default pointer for `{}` cannot resolve: {}", .0.as_str(), .1)]
    UnresolvablePointer(TeamTemplateDefinitionId, String),
}

impl TeamDefinitionStoreError {
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

/// Team-specific release projection.  The Agent `ReleaseAssignment` cannot be
/// reused because it embeds an `AgentDefinitionRevisionRef`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamReleaseAssignment {
    pub scope: DefinitionScope,
    pub revision_ref: TeamTemplateRevisionRef,
    pub channel: ReleaseChannel,
    pub status: ReleaseAssignmentStatus,
    pub authorization: ReleaseAuthorization,
    pub content_digest: String,
}

impl TeamReleaseAssignment {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.revision_ref.validate()?;
        if self.scope != self.revision_ref.template_id.scope() {
            return Err(ValidationError::InvalidContract {
                message: "team release assignment scope must match template scope".to_string(),
            });
        }
        validate_release_authorization(self.scope, &self.authorization)?;
        validate_sha256("content_digest", &self.content_digest)
    }

    #[must_use]
    pub fn is_active_stable(&self) -> bool {
        self.channel == ReleaseChannel::Stable && self.status == ReleaseAssignmentStatus::Active
    }
}

/// Team-specific pointer.  The Agent `DefaultPointer` is intentionally not
/// reused because its identity is an `AgentDefinitionId`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamDefaultPointer {
    pub scope: DefinitionScope,
    pub template_id: TeamTemplateDefinitionId,
    pub selector: RevisionSelector,
    pub authorization: ReleaseAuthorization,
}

impl TeamDefaultPointer {
    #[must_use]
    pub fn latest(
        scope: DefinitionScope,
        template_id: TeamTemplateDefinitionId,
        authorization: ReleaseAuthorization,
    ) -> Self {
        Self {
            scope,
            template_id,
            selector: RevisionSelector::LatestApprovedStable,
            authorization,
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.scope != self.template_id.scope() {
            return Err(ValidationError::InvalidContract {
                message: "team default pointer scope must match template scope".to_string(),
            });
        }
        self.selector.validate()?;
        if matches!(self.selector, RevisionSelector::DefaultPointer) {
            return Err(ValidationError::InvalidContract {
                message: "a team default pointer cannot recursively target another default pointer"
                    .to_string(),
            });
        }
        validate_release_authorization(self.scope, &self.authorization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTeamTemplateRevision {
    pub revision: TeamTemplateRevision,
    pub team_markdown: String,
}

/// Persisted independently from `team.yaml` so a syntactically valid manifest
/// edit cannot silently rewrite the revision content digest on read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RevisionIntegrity {
    revision_ref: TeamTemplateRevisionRef,
    content_digest: String,
}

/// Immutable Team Template revisions plus release and default-pointer
/// projections.  Release projections are deliberately separate from revision
/// files so a status transition never mutates the asset itself.
#[derive(Debug)]
pub struct TeamTemplateDefinitionStore<L> {
    layout: L,
}

impl<L> TeamTemplateDefinitionStore<L>
where
    L: TeamTemplateStorageLayout,
{
    #[must_use]
    pub fn new(layout: L) -> Self {
        Self { layout }
    }

    #[must_use]
    pub fn layout(&self) -> &L {
        &self.layout
    }

    pub fn store_revision(
        &self,
        manifest: TeamTemplateManifest,
        team_markdown: &str,
    ) -> Result<StoredTeamTemplateRevision, TeamDefinitionStoreError> {
        let (revision, normalized_markdown) = build_revision(manifest, team_markdown)?;
        let revision_dir = self.revision_dir(&revision.revision_ref)?;
        if revision_dir.exists() {
            let existing = self.read_revision(&revision.revision_ref)?;
            if existing.revision.manifest == revision.manifest
                && existing.team_markdown == normalized_markdown
            {
                return Ok(existing);
            }
            return Err(TeamDefinitionStoreError::RevisionConflict {
                revision: revision.revision_ref,
            });
        }

        let parent =
            revision_dir
                .parent()
                .ok_or_else(|| TeamDefinitionStoreError::CorruptRevision {
                    revision: revision.revision_ref.clone(),
                    reason: "revision path has no parent".to_string(),
                })?;
        create_dir_all(parent)?;
        let staging = unique_staging_dir(parent)?;
        let persisted = (|| {
            let manifest_yaml = manifest_yaml(&revision.manifest)?;
            write_new_file(&staging.join(MANIFEST_FILE_NAME), manifest_yaml.as_bytes())?;
            write_new_file(
                &staging.join(INSTRUCTIONS_FILE_NAME),
                normalized_markdown.as_bytes(),
            )?;
            let integrity = serde_json::to_vec_pretty(&RevisionIntegrity {
                revision_ref: revision.revision_ref.clone(),
                content_digest: revision.content_digest.clone(),
            })
            .map_err(|error| TeamDefinitionStoreError::Serialize(error.to_string()))?;
            write_new_file(&staging.join(INTEGRITY_FILE_NAME), &integrity)?;
            sync_directory(&staging)?;
            fs::rename(&staging, &revision_dir)
                .map_err(|error| TeamDefinitionStoreError::io(&revision_dir, error))?;
            sync_directory(parent)?;
            Ok(StoredTeamTemplateRevision {
                revision,
                team_markdown: normalized_markdown,
            })
        })();
        if persisted.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        persisted
    }

    pub fn read_revision(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
    ) -> Result<StoredTeamTemplateRevision, TeamDefinitionStoreError> {
        let directory = self.revision_dir(revision_ref)?;
        let manifest_path = directory.join(MANIFEST_FILE_NAME);
        let instructions_path = directory.join(INSTRUCTIONS_FILE_NAME);
        let integrity_path = directory.join(INTEGRITY_FILE_NAME);
        if !manifest_path.is_file() || !instructions_path.is_file() || !integrity_path.is_file() {
            if !directory.exists() {
                return Err(TeamDefinitionStoreError::RevisionNotFound {
                    template_id: revision_ref.template_id.clone(),
                    revision: revision_ref.revision,
                });
            }
            return Err(TeamDefinitionStoreError::CorruptRevision {
                revision: revision_ref.clone(),
                reason: format!(
                    "expected `{MANIFEST_FILE_NAME}`, `{INSTRUCTIONS_FILE_NAME}`, and `{INTEGRITY_FILE_NAME}`"
                ),
            });
        }
        let manifest_bytes = read_file(&manifest_path)?;
        let instructions_bytes = read_file(&instructions_path)?;
        let integrity: RevisionIntegrity = self.read_json(&integrity_path)?;
        let (revision, team_markdown) = verify_read_revision(&manifest_bytes, &instructions_bytes)?;
        ensure_same_revision_ref(revision_ref, &revision.revision_ref)?;
        if integrity.revision_ref != revision.revision_ref {
            return Err(TeamDefinitionStoreError::CorruptRevision {
                revision: revision_ref.clone(),
                reason: "integrity record revision reference does not match team manifest"
                    .to_string(),
            });
        }
        if integrity.content_digest != revision.content_digest {
            return Err(TeamDefinitionStoreError::DigestMismatch {
                subject: "revision.content_digest".to_string(),
                expected: integrity.content_digest,
                actual: revision.content_digest,
            });
        }
        Ok(StoredTeamTemplateRevision {
            revision,
            team_markdown,
        })
    }

    pub fn list_revisions(
        &self,
        template_id: &TeamTemplateDefinitionId,
    ) -> Result<Vec<StoredTeamTemplateRevision>, TeamDefinitionStoreError> {
        let root = self.template_dir(template_id)?.join(REVISIONS_DIRECTORY);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut revisions = Vec::new();
        for entry in
            fs::read_dir(&root).map_err(|error| TeamDefinitionStoreError::io(&root, error))?
        {
            let entry = entry.map_err(|error| TeamDefinitionStoreError::io(&root, error))?;
            if !entry
                .file_type()
                .map_err(|error| TeamDefinitionStoreError::io(entry.path(), error))?
                .is_dir()
            {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(revision) = name.parse::<u64>() else {
                continue;
            };
            if revision == 0 {
                continue;
            }
            let revision_ref = TeamTemplateRevisionRef::new(template_id.clone(), revision)
                .map_err(TeamDefinitionStoreError::contract)?;
            revisions.push(self.read_revision(&revision_ref)?);
        }
        revisions.sort_by_key(|stored| stored.revision.revision_ref.revision);
        Ok(revisions)
    }

    /// Enumerate registered, scope-qualified Team Template identities. The
    /// store validates every manifest and rejects scope confusion or symlink
    /// traversal so callers never recreate Gateway's legacy shadow scan.
    pub fn list_template_ids(
        &self,
    ) -> Result<Vec<TeamTemplateDefinitionId>, TeamDefinitionStoreError> {
        let mut ids = std::collections::BTreeSet::new();
        for scope in [
            DefinitionScope::Builtin,
            DefinitionScope::User,
            DefinitionScope::Workspace,
        ] {
            let root = self.layout.root_for_scope(scope)?.join(TEAMS_DIRECTORY);
            let mut manifests = Vec::new();
            collect_manifest_files(&root, MANIFEST_FILE_NAME, &mut manifests)?;
            for manifest_path in manifests {
                let manifest: TeamTemplateManifest =
                    serde_yaml::from_slice(&read_file(&manifest_path)?)
                        .map_err(TeamDefinitionStoreError::deserialize)?;
                manifest
                    .validate()
                    .map_err(TeamDefinitionStoreError::contract)?;
                if manifest.template_id.scope() != scope {
                    return Err(TeamDefinitionStoreError::InvalidImport(format!(
                        "manifest `{}` declares scope `{}` under `{}` root",
                        manifest_path.display(),
                        manifest.template_id.scope().as_str(),
                        scope.as_str(),
                    )));
                }
                ids.insert(manifest.template_id.as_str().to_string());
            }
        }
        ids.into_iter()
            .map(|id| {
                TeamTemplateDefinitionId::try_from(id.as_str())
                    .map_err(TeamDefinitionStoreError::contract)
            })
            .collect()
    }

    pub fn record_release_assignment(
        &self,
        assignment: &TeamReleaseAssignment,
    ) -> Result<(), TeamDefinitionStoreError> {
        assignment
            .validate()
            .map_err(TeamDefinitionStoreError::contract)?;
        let stored = self.read_revision(&assignment.revision_ref)?;
        if stored.revision.content_digest != assignment.content_digest {
            return Err(TeamDefinitionStoreError::ReleaseDigestMismatch);
        }
        let path = self.release_assignment_path(&assignment.revision_ref, assignment.channel)?;
        if path.exists() {
            let existing: TeamReleaseAssignment = self.read_json(&path)?;
            if existing == *assignment {
                return Ok(());
            }
            if existing.scope != assignment.scope
                || existing.revision_ref != assignment.revision_ref
                || existing.channel != assignment.channel
                || existing.content_digest != assignment.content_digest
            {
                return Err(TeamDefinitionStoreError::ReleaseAssignmentConflict {
                    revision: assignment.revision_ref.clone(),
                    channel: assignment.channel,
                });
            }
            return self.write_json_replace(&path, assignment);
        }
        self.write_json_immutable(&path, assignment, || {
            TeamDefinitionStoreError::ReleaseAssignmentConflict {
                revision: assignment.revision_ref.clone(),
                channel: assignment.channel,
            }
        })
    }

    pub fn release_assignments(
        &self,
        template_id: &TeamTemplateDefinitionId,
    ) -> Result<Vec<TeamReleaseAssignment>, TeamDefinitionStoreError> {
        let root = self.template_dir(template_id)?.join(RELEASES_DIRECTORY);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let mut assignments = Vec::new();
        collect_json_files(&root, &mut assignments)?;
        assignments.retain(|assignment: &TeamReleaseAssignment| {
            assignment.revision_ref.template_id == *template_id
        });
        for assignment in &assignments {
            assignment
                .validate()
                .map_err(TeamDefinitionStoreError::contract)?;
        }
        assignments.sort_by_key(|assignment| assignment.revision_ref.revision);
        Ok(assignments)
    }

    pub fn set_default_pointer(
        &self,
        pointer: &TeamDefaultPointer,
    ) -> Result<(), TeamDefinitionStoreError> {
        pointer
            .validate()
            .map_err(TeamDefinitionStoreError::contract)?;
        if let RevisionSelector::ExactApprovedRevision { revision } = pointer.selector {
            self.ensure_eligible_revision(&pointer.template_id, revision)?;
        }
        let path = self.pointer_path(&pointer.template_id)?;
        if path.exists() {
            let existing: TeamDefaultPointer = self.read_json(&path)?;
            existing
                .validate()
                .map_err(TeamDefinitionStoreError::contract)?;
            if matches!(
                (&existing.selector, &pointer.selector),
                (
                    RevisionSelector::ExactApprovedRevision { .. },
                    RevisionSelector::LatestApprovedStable
                )
            ) {
                return Err(TeamDefinitionStoreError::ManualPinProtected);
            }
            if existing == *pointer {
                return Ok(());
            }
        }
        self.write_json_replace(&path, pointer)
    }

    pub fn default_pointer(
        &self,
        template_id: &TeamTemplateDefinitionId,
    ) -> Result<TeamDefaultPointer, TeamDefinitionStoreError> {
        let path = self.pointer_path(template_id)?;
        if !path.is_file() {
            return Err(TeamDefinitionStoreError::DefaultPointerNotFound(
                template_id.clone(),
            ));
        }
        let pointer: TeamDefaultPointer = self.read_json(&path)?;
        pointer
            .validate()
            .map_err(TeamDefinitionStoreError::contract)?;
        if pointer.template_id != *template_id {
            return Err(TeamDefinitionStoreError::UnresolvablePointer(
                template_id.clone(),
                "pointer template_id does not match its path".to_string(),
            ));
        }
        Ok(pointer)
    }

    pub(crate) fn ensure_eligible_revision(
        &self,
        template_id: &TeamTemplateDefinitionId,
        revision: u64,
    ) -> Result<StoredTeamTemplateRevision, TeamDefinitionStoreError> {
        let revision_ref = TeamTemplateRevisionRef::new(template_id.clone(), revision)
            .map_err(TeamDefinitionStoreError::contract)?;
        let stored = self.read_revision(&revision_ref)?;
        if stored.revision.manifest.lifecycle != RevisionLifecycle::Published {
            return Err(TeamDefinitionStoreError::UnresolvablePointer(
                template_id.clone(),
                format!("revision {revision} is not published"),
            ));
        }
        let eligible = self
            .release_assignments(template_id)?
            .iter()
            .any(|assignment| {
                assignment.revision_ref == revision_ref
                    && assignment.content_digest == stored.revision.content_digest
                    && assignment_is_eligible(assignment)
            });
        if !eligible {
            return Err(TeamDefinitionStoreError::UnresolvablePointer(
                template_id.clone(),
                format!("revision {revision} has no active eligible stable release"),
            ));
        }
        Ok(stored)
    }

    pub(crate) fn latest_eligible_revision(
        &self,
        template_id: &TeamTemplateDefinitionId,
    ) -> Result<StoredTeamTemplateRevision, TeamDefinitionStoreError> {
        let revision = self
            .release_assignments(template_id)?
            .into_iter()
            .filter(assignment_is_eligible)
            .max_by_key(|assignment| assignment.revision_ref.revision)
            .ok_or_else(|| {
                TeamDefinitionStoreError::UnresolvablePointer(
                    template_id.clone(),
                    "no active eligible stable release exists".to_string(),
                )
            })?;
        self.ensure_eligible_revision(template_id, revision.revision_ref.revision)
    }

    fn template_dir(
        &self,
        template_id: &TeamTemplateDefinitionId,
    ) -> Result<PathBuf, TeamDefinitionStoreError> {
        let mut directory = self
            .layout
            .root_for_scope(template_id.scope())?
            .join(TEAMS_DIRECTORY);
        for segment in template_id.as_str().split('/').skip(1) {
            if segment.is_empty() || Path::new(segment).components().count() != 1 {
                return Err(TeamDefinitionStoreError::InvalidImport(format!(
                    "unsafe qualified team template id `{}`",
                    template_id.as_str()
                )));
            }
            directory.push(segment);
        }
        Ok(directory)
    }

    fn revision_dir(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
    ) -> Result<PathBuf, TeamDefinitionStoreError> {
        Ok(self
            .template_dir(&revision_ref.template_id)?
            .join(REVISIONS_DIRECTORY)
            .join(revision_ref.revision.to_string()))
    }

    fn release_assignment_path(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
        channel: ReleaseChannel,
    ) -> Result<PathBuf, TeamDefinitionStoreError> {
        Ok(self
            .template_dir(&revision_ref.template_id)?
            .join(RELEASES_DIRECTORY)
            .join(revision_ref.revision.to_string())
            .join(format!("{}.json", channel_name(channel))))
    }

    fn pointer_path(
        &self,
        template_id: &TeamTemplateDefinitionId,
    ) -> Result<PathBuf, TeamDefinitionStoreError> {
        Ok(self.template_dir(template_id)?.join(POINTER_FILE_NAME))
    }

    fn write_json_immutable<T, F>(
        &self,
        path: &Path,
        value: &T,
        on_conflict: F,
    ) -> Result<(), TeamDefinitionStoreError>
    where
        T: Serialize + DeserializeOwned + PartialEq,
        F: FnOnce() -> TeamDefinitionStoreError,
    {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| TeamDefinitionStoreError::Serialize(error.to_string()))?;
        if path.exists() {
            let existing: T = self.read_json(path)?;
            return if existing == *value {
                Ok(())
            } else {
                Err(on_conflict())
            };
        }
        write_atomic(path, &bytes)
    }

    fn write_json_replace<T: Serialize>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), TeamDefinitionStoreError> {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| TeamDefinitionStoreError::Serialize(error.to_string()))?;
        write_atomic(path, &bytes)
    }

    fn read_json<T: DeserializeOwned>(&self, path: &Path) -> Result<T, TeamDefinitionStoreError> {
        let bytes = read_file(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| TeamDefinitionStoreError::Deserialize(error.to_string()))
    }
}

fn assignment_is_eligible(assignment: &TeamReleaseAssignment) -> bool {
    if !assignment.is_active_stable() {
        return false;
    }
    match assignment.scope {
        DefinitionScope::Builtin => matches!(
            assignment.authorization,
            ReleaseAuthorization::ReleaseAuthorityAttestation { .. }
        ),
        DefinitionScope::User | DefinitionScope::Workspace => matches!(
            assignment.authorization,
            ReleaseAuthorization::HumanApproval { .. }
        ),
    }
}

fn validate_release_authorization(
    scope: DefinitionScope,
    authorization: &ReleaseAuthorization,
) -> Result<(), ValidationError> {
    let reference = match authorization {
        ReleaseAuthorization::HumanApproval { approval_ref } => approval_ref,
        ReleaseAuthorization::ReleaseAuthorityAttestation { attestation_ref } => attestation_ref,
    };
    if reference.trim().is_empty() || reference.contains('\0') {
        return Err(ValidationError::InvalidReference {
            field: "release_authorization".to_string(),
            value: reference.clone(),
            reason: "must be a non-empty reference without NUL bytes".to_string(),
        });
    }
    let valid = match scope {
        DefinitionScope::Builtin => matches!(
            authorization,
            ReleaseAuthorization::ReleaseAuthorityAttestation { .. }
        ),
        DefinitionScope::User | DefinitionScope::Workspace => {
            matches!(authorization, ReleaseAuthorization::HumanApproval { .. })
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ValidationError::InvalidContract {
            message: "builtin releases require release-authority attestation; user and workspace releases require human approval".to_string(),
        })
    }
}

fn validate_sha256(field: &str, value: &str) -> Result<(), ValidationError> {
    let valid = value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(ValidationError::InvalidReference {
            field: field.to_string(),
            value: value.to_string(),
            reason: "must be a 64-character SHA-256 hex digest".to_string(),
        })
    }
}

fn channel_name(channel: ReleaseChannel) -> &'static str {
    match channel {
        ReleaseChannel::Shadow => "shadow",
        ReleaseChannel::Canary => "canary",
        ReleaseChannel::Stable => "stable",
    }
}

fn create_dir_all(path: &Path) -> Result<(), TeamDefinitionStoreError> {
    fs::create_dir_all(path).map_err(|error| TeamDefinitionStoreError::io(path, error))
}

fn read_file(path: &Path) -> Result<Vec<u8>, TeamDefinitionStoreError> {
    fs::read(path).map_err(|error| TeamDefinitionStoreError::io(path, error))
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), TeamDefinitionStoreError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| TeamDefinitionStoreError::io(path, error))?;
    file.write_all(bytes)
        .map_err(|error| TeamDefinitionStoreError::io(path, error))?;
    file.sync_all()
        .map_err(|error| TeamDefinitionStoreError::io(path, error))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), TeamDefinitionStoreError> {
    let parent = path.parent().ok_or_else(|| {
        TeamDefinitionStoreError::InvalidImport(format!("path `{}` has no parent", path.display()))
    })?;
    create_dir_all(parent)?;
    let staging = unique_staging_file(parent, path.file_name().and_then(|value| value.to_str()))?;
    let result = (|| {
        write_new_file(&staging, bytes)?;
        fs::rename(&staging, path).map_err(|error| TeamDefinitionStoreError::io(path, error))?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

fn unique_staging_dir(parent: &Path) -> Result<PathBuf, TeamDefinitionStoreError> {
    for _ in 0..32 {
        let candidate = parent.join(staging_name("revision"));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(TeamDefinitionStoreError::io(&candidate, error)),
        }
    }
    Err(TeamDefinitionStoreError::InvalidImport(
        "could not allocate revision staging directory".to_string(),
    ))
}

fn unique_staging_file(
    parent: &Path,
    stem: Option<&str>,
) -> Result<PathBuf, TeamDefinitionStoreError> {
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
    Err(TeamDefinitionStoreError::InvalidImport(
        "could not allocate atomic staging file".to_string(),
    ))
}

fn staging_name(kind: &str) -> String {
    let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!(".{kind}-{}-{nanos}-{sequence}", std::process::id())
}

fn sync_directory(path: &Path) -> Result<(), TeamDefinitionStoreError> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| TeamDefinitionStoreError::io(path, error))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn collect_manifest_files(
    root: &Path,
    file_name: &str,
    output: &mut Vec<PathBuf>,
) -> Result<(), TeamDefinitionStoreError> {
    if !root.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(root).map_err(|error| TeamDefinitionStoreError::io(root, error))?;
    for entry in entries {
        let entry = entry.map_err(|error| TeamDefinitionStoreError::io(root, error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| TeamDefinitionStoreError::io(&path, error))?;
        if file_type.is_symlink() {
            return Err(TeamDefinitionStoreError::InvalidImport(format!(
                "team definition storage must not contain symlink `{}`",
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

fn collect_json_files<T: DeserializeOwned>(
    root: &Path,
    output: &mut Vec<T>,
) -> Result<(), TeamDefinitionStoreError> {
    for entry in fs::read_dir(root).map_err(|error| TeamDefinitionStoreError::io(root, error))? {
        let entry = entry.map_err(|error| TeamDefinitionStoreError::io(root, error))?;
        let kind = entry
            .file_type()
            .map_err(|error| TeamDefinitionStoreError::io(entry.path(), error))?;
        if kind.is_dir() {
            collect_json_files(&entry.path(), output)?;
        } else if kind.is_file()
            && entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
        {
            let bytes = read_file(&entry.path())?;
            output.push(
                serde_json::from_slice(&bytes)
                    .map_err(|error| TeamDefinitionStoreError::Deserialize(error.to_string()))?,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests_support {
    use harness_contract::agent::{
        AgentCapability, AgentDefinitionId, DefinitionScope, ReleaseAuthorization,
        RevisionLifecycle, RevisionSelector,
    };
    use harness_contract::team::{
        RoleBehaviorFacet, RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract,
        TeamRoleDefinition, TeamRoleTaskContract, TeamTemplateDefinitionId, TeamTemplateManifest,
        TeamTopologyContract,
    };
    use tempfile::TempDir;

    use super::{ScopedTeamTemplateLayout, TeamTemplateDefinitionStore};
    use crate::team_definition::validation::digest_hex;

    pub fn markdown() -> &'static str {
        "# Team\n\nCoordinate implementation and review.\n"
    }

    pub fn manifest(
        scope: DefinitionScope,
        revision: u64,
        lifecycle: RevisionLifecycle,
    ) -> TeamTemplateManifest {
        TeamTemplateManifest {
            api_version: "cowd.team/v1".to_string(),
            template_id: TeamTemplateDefinitionId::new(scope, "cowd/implementation-review")
                .unwrap(),
            revision,
            name: "Implementation review".to_string(),
            display: None,
            lifecycle,
            topology: TeamTopologyContract {
                protocol_ref: "team/implementation-review@1".to_string(),
                require_synthesis: true,
                require_review: true,
            },
            role_aliases: std::collections::BTreeMap::new(),
            roles: vec![TeamRoleDefinition {
                role_id: "reviewer".to_string(),
                display_name: None,
                responsibility: "Review implementation evidence".to_string(),
                agent_definition_id: AgentDefinitionId::new(scope, "cowd/reviewer").unwrap(),
                agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                behavior: vec![RoleBehaviorFacet::TerminalCandidate { required: true }],
                grant_ceiling: vec![AgentCapability::Read, AgentCapability::Search],
                task_contract: TeamRoleTaskContract {
                    contract_ref: "task/review@1".to_string(),
                    acceptance: vec!["evidence".to_string()],
                    dataflow: Default::default(),
                },
            }],
            dependencies: vec![],
            result_contract: TeamResultContract {
                required_fields: vec!["summary".to_string(), "evidence".to_string()],
                evidence_required: true,
                synthesis_required: true,
            },
            evaluation: harness_contract::team::TeamEvaluationContract::single_release_gate(
                "team/implementation-review",
                "team_interoperability",
            ),
            instructions_digest: digest_hex(markdown().as_bytes()),
        }
    }

    pub fn release_authorization(scope: DefinitionScope) -> ReleaseAuthorization {
        match scope {
            DefinitionScope::Builtin => ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: "release/cowd-v1".to_string(),
            },
            DefinitionScope::User | DefinitionScope::Workspace => {
                ReleaseAuthorization::HumanApproval {
                    approval_ref: "approval/team-v1".to_string(),
                }
            }
        }
    }

    pub fn store(temp: &TempDir) -> TeamTemplateDefinitionStore<ScopedTeamTemplateLayout> {
        TeamTemplateDefinitionStore::new(ScopedTeamTemplateLayout::new(
            temp.path().join("builtin"),
            temp.path().join("user"),
            temp.path().join("workspace"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::agent::{
        DefinitionScope, ReleaseAssignmentStatus, ReleaseChannel, RevisionLifecycle,
    };

    use super::tests_support::{manifest, markdown, release_authorization, store};
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn revision_is_immutable_and_idempotent_only_for_same_content() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let initial = manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft);
        store.store_revision(initial.clone(), markdown()).unwrap();
        store.store_revision(initial, markdown()).unwrap();
        let mut changed = manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft);
        changed.name = "Changed implementation review".to_string();
        assert!(matches!(
            store.store_revision(changed, markdown()),
            Err(TeamDefinitionStoreError::RevisionConflict { .. })
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
        fs::write(&manifest_path, legacy_yaml.as_bytes()).unwrap();
        let legacy_digest = crate::team_definition::validation::content_digest(
            legacy_yaml.as_bytes(),
            markdown().as_bytes(),
        );
        let integrity = RevisionIntegrity {
            revision_ref: stored.revision.revision_ref.clone(),
            content_digest: legacy_digest,
        };
        fs::write(
            revision_dir.join(INTEGRITY_FILE_NAME),
            serde_json::to_vec_pretty(&integrity).unwrap(),
        )
        .unwrap();

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
    fn digest_or_file_tampering_makes_a_revision_unreadable() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        let root = store
            .layout()
            .root_for_scope(DefinitionScope::Workspace)
            .unwrap();
        let file = root.join("teams/cowd/implementation-review/revisions/1/TEAM.md");
        fs::write(file, "# Tampered\n").unwrap();
        assert!(matches!(
            store.read_revision(&stored.revision.revision_ref),
            Err(TeamDefinitionStoreError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn syntactically_valid_manifest_tampering_is_detected_by_revision_integrity() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let stored = store
            .store_revision(
                manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft),
                markdown(),
            )
            .unwrap();
        let root = store
            .layout()
            .root_for_scope(DefinitionScope::Workspace)
            .unwrap();
        let file = root.join("teams/cowd/implementation-review/revisions/1/team.yaml");
        let mut changed = manifest(DefinitionScope::Workspace, 1, RevisionLifecycle::Draft);
        changed.name = "Tampered display name".to_string();
        fs::write(file, serde_yaml::to_string(&changed).unwrap()).unwrap();
        assert!(matches!(
            store.read_revision(&stored.revision.revision_ref),
            Err(TeamDefinitionStoreError::DigestMismatch { .. })
        ));
    }

    #[test]
    fn user_scope_requires_human_approval_and_builtin_requires_release_attestation() {
        let temp = TempDir::new().unwrap();
        let store = store(&temp);
        let user = store
            .store_revision(
                manifest(DefinitionScope::User, 1, RevisionLifecycle::Published),
                markdown(),
            )
            .unwrap();
        let mut invalid = TeamReleaseAssignment {
            scope: DefinitionScope::User,
            revision_ref: user.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: "release/nope".to_string(),
            },
            content_digest: user.revision.content_digest.clone(),
        };
        assert!(matches!(
            store.record_release_assignment(&invalid),
            Err(TeamDefinitionStoreError::Contract(_))
        ));
        invalid.authorization = release_authorization(DefinitionScope::User);
        store.record_release_assignment(&invalid).unwrap();
        let builtin = store
            .store_revision(
                manifest(DefinitionScope::Builtin, 1, RevisionLifecycle::Published),
                markdown(),
            )
            .unwrap();
        let invalid_builtin = TeamReleaseAssignment {
            scope: DefinitionScope::Builtin,
            revision_ref: builtin.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: release_authorization(DefinitionScope::User),
            content_digest: builtin.revision.content_digest,
        };
        assert!(matches!(
            store.record_release_assignment(&invalid_builtin),
            Err(TeamDefinitionStoreError::Contract(_))
        ));
    }
}
