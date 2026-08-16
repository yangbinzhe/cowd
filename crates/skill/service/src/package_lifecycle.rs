//! Governed Skill acquisition and immutable lifecycle management.
//!
//! A Skill is inert instructional content. Installing one never executes its
//! scripts, installs dependencies, starts a process, or grants a capability.
//! Executable extensions remain owned by the Plugin/MCP/managed-process
//! control planes.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use harness_contract::skill::{SkillAdapterKind, SkillInspectionReport};
use reqwest::blocking::{Client, Response};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::{Builder as TempBuilder, TempDir};
use thiserror::Error;

use crate::skill_manifest::{get_skill_description, get_skill_name, parse_skill_file};
use crate::skill_security::{scan_skill_content, SecurityFinding, SecurityStatus};
use crate::{inspect_skill_package, stable_skill_id};

pub const SKILL_STORE_SCHEMA_VERSION: u32 = 1;
pub const MAX_SKILL_ARCHIVE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SKILL_EXTRACTED_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_SKILL_FILES: usize = 512;
pub const MAX_SKILL_DEPTH: usize = 16;
pub const MAX_SKILL_FILE_BYTES: u64 = 16 * 1024 * 1024;

const TREE_DIGEST_DOMAIN: &[u8] = b"cowd.skill.package-tree.v1\0";
const SCANNER_VERSION: &str = "cowd.skill.security.v2";

#[derive(Debug, Error)]
pub enum SkillLifecycleError {
    #[error("invalid Skill source: {0}")]
    InvalidSource(String),
    #[error("Skill package is invalid: {0}")]
    InvalidPackage(String),
    #[error("Skill package is blocked: {0}")]
    Blocked(String),
    #[error("Skill package changed after review: expected {expected}, found {actual}")]
    DigestMismatch { expected: String, actual: String },
    #[error("managed Skill not found: {0}")]
    NotFound(String),
    #[error("remote Skill acquisition failed: {0}")]
    Remote(String),
    #[error("Skill lifecycle I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Skill lifecycle serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillSourceKindV1 {
    LocalDirectory,
    LocalMarkdown,
    GithubArchive,
    UploadedArchive,
    Generated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillSourceIdentityV1 {
    pub kind: SkillSourceKindV1,
    /// Credential-free source locator retained for provenance.
    pub locator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_ref: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillPackageClassV1 {
    Prompt,
    Workflow,
    ExecutableExtension,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillPackageFileV1 {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
    pub executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillInstallPlanV1 {
    pub schema_version: u32,
    pub skill_id: String,
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    pub source: SkillSourceIdentityV1,
    pub package_class: SkillPackageClassV1,
    pub package_digest: String,
    pub manifest_digest: String,
    pub total_bytes: u64,
    pub files: Vec<SkillPackageFileV1>,
    pub inspection: SkillInspectionReport,
    pub security_status: SecurityStatus,
    pub security_findings: Vec<SecurityFinding>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub installable: bool,
    pub scanner_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillInstallReceiptV1 {
    pub schema_version: u32,
    pub install_id: String,
    pub skill_id: String,
    pub name: String,
    pub package_digest: String,
    pub manifest_digest: String,
    pub plan_digest: String,
    pub security_findings_digest: String,
    pub package_class: SkillPackageClassV1,
    pub file_count: usize,
    pub total_bytes: u64,
    pub source: SkillSourceIdentityV1,
    pub scanner_version: String,
    pub security_status: SecurityStatus,
    pub warnings_accepted: bool,
    pub actor: String,
    pub installed_at_unix_ms: u64,
    pub previous_revision: Option<String>,
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedSkillActivePointerV1 {
    pub schema_version: u32,
    pub skill_id: String,
    pub revision: String,
    pub package_digest: String,
    pub receipt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedSkillEntryV1 {
    pub pointer: ManagedSkillActivePointerV1,
    pub skill_dir: PathBuf,
    pub package_root: PathBuf,
    pub prompt_path: PathBuf,
    pub receipt_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillLifecycleStatusV1 {
    pub schema_version: u32,
    pub skill_id: String,
    pub active: Option<ManagedSkillActivePointerV1>,
    pub revisions: Vec<String>,
    pub receipts: Vec<SkillInstallReceiptV1>,
}

#[derive(Debug, Clone)]
pub struct ManagedSkillStore {
    root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SkillLifecycle {
    store: ManagedSkillStore,
    client: Client,
}

#[derive(Debug)]
struct CollectedPackage {
    source: SkillSourceIdentityV1,
    files: Vec<CollectedFile>,
    plan: SkillInstallPlanV1,
}

#[derive(Debug)]
struct CollectedFile {
    relative: String,
    bytes: Vec<u8>,
    executable: bool,
}

#[derive(Debug)]
struct AcquiredPackage {
    root: PathBuf,
    source: SkillSourceIdentityV1,
    _temporary: Option<TempDir>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GithubSource {
    owner: String,
    repository: String,
    requested_ref: String,
    subpath: PathBuf,
}

#[derive(Debug, Deserialize)]
struct GithubCommitResponse {
    sha: String,
}

impl ManagedSkillStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn default_for_user() -> Result<Self, SkillLifecycleError> {
        Ok(Self::new(default_managed_skill_store_root()?))
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn status(&self, skill_id: &str) -> Result<SkillLifecycleStatusV1, SkillLifecycleError> {
        let skill_id = validate_skill_id(skill_id)?;
        let skill_dir = self.root.join(&skill_id);
        if !skill_dir.is_dir() {
            return Err(SkillLifecycleError::NotFound(skill_id));
        }
        let active = read_active_pointer(&skill_dir).transpose()?;
        let mut revisions = list_names(skill_dir.join("revisions"), true)?;
        for revision in &mut revisions {
            *revision = normalize_digest(revision)?;
        }
        revisions.sort();
        let mut receipts = Vec::new();
        let receipt_dir = skill_dir.join("receipts");
        if receipt_dir.is_dir() {
            for name in list_names(&receipt_dir, false)? {
                if !name.ends_with(".json") {
                    continue;
                }
                let path = receipt_dir.join(name);
                let receipt = read_json_bounded::<SkillInstallReceiptV1>(&path, 256 * 1024)?;
                if receipt.skill_id != skill_id {
                    return Err(SkillLifecycleError::InvalidPackage(format!(
                        "receipt {} belongs to a different Skill",
                        path.display()
                    )));
                }
                receipts.push(receipt);
            }
        }
        receipts.sort_by(|left, right| {
            left.installed_at_unix_ms
                .cmp(&right.installed_at_unix_ms)
                .then_with(|| left.install_id.cmp(&right.install_id))
        });
        Ok(SkillLifecycleStatusV1 {
            schema_version: SKILL_STORE_SCHEMA_VERSION,
            skill_id,
            active,
            revisions,
            receipts,
        })
    }

    pub fn rollback(
        &self,
        skill_id: &str,
        revision: &str,
        actor: &str,
    ) -> Result<SkillInstallReceiptV1, SkillLifecycleError> {
        let skill_id = validate_skill_id(skill_id)?;
        let digest = normalize_digest(revision)?;
        let _lock = self.lock()?;
        let skill_dir = self.root.join(&skill_id);
        let package_root = skill_dir.join("revisions").join(digest_hex(&digest)?);
        if !package_root.is_dir() {
            return Err(SkillLifecycleError::NotFound(format!(
                "{skill_id} revision {digest}"
            )));
        }
        let source = SkillSourceIdentityV1 {
            kind: SkillSourceKindV1::Generated,
            locator: format!("managed:{skill_id}@{digest}"),
            requested_ref: Some(digest.clone()),
            resolved_ref: Some(digest.clone()),
        };
        let collected = collect_package(&package_root, source)?;
        if collected.plan.package_digest != digest {
            return Err(SkillLifecycleError::DigestMismatch {
                expected: digest,
                actual: collected.plan.package_digest,
            });
        }
        if collected.plan.skill_id != skill_id {
            return Err(SkillLifecycleError::InvalidPackage(
                "revision manifest Skill id does not match its managed store owner".to_string(),
            ));
        }
        if !collected.plan.blockers.is_empty() {
            return Err(SkillLifecycleError::Blocked(
                collected.plan.blockers.join("; "),
            ));
        }
        let prior_receipt =
            find_verified_receipt_for_revision(&skill_dir, &skill_id, &digest, &collected.plan)?;
        let prior_package = collect_package(&package_root, prior_receipt.source.clone())?;
        if !prior_package.plan.warnings.is_empty() && !prior_receipt.warnings_accepted {
            return Err(SkillLifecycleError::Blocked(
                "the retained revision has warnings without evidence of explicit acceptance"
                    .to_string(),
            ));
        }
        if prior_receipt.plan_digest != plan_evidence_digest(&prior_package.plan)? {
            return Err(SkillLifecycleError::InvalidPackage(
                "retained revision install plan evidence does not match its receipt".to_string(),
            ));
        }
        self.publish(
            &prior_package,
            !prior_package.plan.warnings.is_empty(),
            actor,
        )
    }

    pub fn deactivate(
        &self,
        skill_id: &str,
        actor: &str,
    ) -> Result<Option<ManagedSkillActivePointerV1>, SkillLifecycleError> {
        let skill_id = validate_skill_id(skill_id)?;
        let _lock = self.lock()?;
        let skill_dir = self.root.join(&skill_id);
        let Some(pointer) = read_active_pointer(&skill_dir).transpose()? else {
            return Ok(None);
        };
        let inactive_dir = skill_dir.join("inactive");
        create_private_dir_all(&inactive_dir)?;
        let target = inactive_dir.join(format!(
            "{}-{}-{}.json",
            unix_ms(),
            sanitize_actor(actor),
            uuid::Uuid::new_v4()
        ));
        fs::rename(skill_dir.join("active.json"), &target)?;
        sync_dir(&inactive_dir)?;
        sync_dir(&skill_dir)?;
        Ok(Some(pointer))
    }

    fn commit(
        &self,
        package: &CollectedPackage,
        allow_warnings: bool,
        actor: &str,
    ) -> Result<SkillInstallReceiptV1, SkillLifecycleError> {
        if !package.plan.blockers.is_empty() {
            return Err(SkillLifecycleError::Blocked(
                package.plan.blockers.join("; "),
            ));
        }
        if !allow_warnings && !package.plan.warnings.is_empty() {
            return Err(SkillLifecycleError::Blocked(format!(
                "review required before accepting warnings: {}",
                package.plan.warnings.join("; ")
            )));
        }
        let _lock = self.lock()?;
        self.publish(package, allow_warnings, actor)
    }

    fn publish(
        &self,
        package: &CollectedPackage,
        warnings_accepted: bool,
        actor: &str,
    ) -> Result<SkillInstallReceiptV1, SkillLifecycleError> {
        create_private_dir_all(&self.root)?;
        let skill_dir = self.root.join(&package.plan.skill_id);
        let revisions_dir = skill_dir.join("revisions");
        let receipts_dir = skill_dir.join("receipts");
        let staging_dir = self.root.join(".staging");
        for path in [&skill_dir, &revisions_dir, &receipts_dir, &staging_dir] {
            create_private_dir_all(path)?;
        }

        let digest_hex = digest_hex(&package.plan.package_digest)?;
        let revision_dir = revisions_dir.join(digest_hex);
        if revision_dir.exists() {
            let existing = collect_package(&revision_dir, package.source.clone())?;
            if existing.plan.package_digest != package.plan.package_digest {
                return Err(SkillLifecycleError::InvalidPackage(format!(
                    "immutable revision path {} contains different bytes",
                    revision_dir.display()
                )));
            }
            make_revision_read_only(&revision_dir)?;
            sync_tree(&revision_dir)?;
        } else {
            let staging = TempBuilder::new()
                .prefix("revision-")
                .tempdir_in(&staging_dir)?;
            let staged_package = staging.path().join("package");
            create_private_dir_all(&staged_package)?;
            write_collected_files(&staged_package, &package.files)?;
            let staged = collect_package(&staged_package, package.source.clone())?;
            if staged.plan.package_digest != package.plan.package_digest {
                return Err(SkillLifecycleError::DigestMismatch {
                    expected: package.plan.package_digest.clone(),
                    actual: staged.plan.package_digest,
                });
            }
            sync_tree(&staged_package)?;
            fs::rename(&staged_package, &revision_dir)?;
            make_revision_read_only(&revision_dir)?;
            sync_tree(&revision_dir)?;
            sync_dir(&revisions_dir)?;
        }

        let previous_revision = read_active_pointer(&skill_dir)
            .transpose()?
            .map(|pointer| pointer.revision);
        let install_id = uuid::Uuid::new_v4().to_string();
        let receipt = SkillInstallReceiptV1 {
            schema_version: SKILL_STORE_SCHEMA_VERSION,
            install_id: install_id.clone(),
            skill_id: package.plan.skill_id.clone(),
            name: package.plan.name.clone(),
            package_digest: package.plan.package_digest.clone(),
            manifest_digest: package.plan.manifest_digest.clone(),
            plan_digest: plan_evidence_digest(&package.plan)?,
            security_findings_digest: canonical_json_digest(&package.plan.security_findings)?,
            package_class: package.plan.package_class,
            file_count: package.plan.files.len(),
            total_bytes: package.plan.total_bytes,
            source: package.source.clone(),
            scanner_version: package.plan.scanner_version.clone(),
            security_status: package.plan.security_status,
            warnings_accepted,
            actor: sanitize_actor(actor),
            installed_at_unix_ms: unix_ms(),
            previous_revision,
            revision: package.plan.package_digest.clone(),
        };
        let receipt_path = receipts_dir.join(format!("{install_id}.json"));
        write_new_json(&receipt_path, &receipt)?;
        let pointer = ManagedSkillActivePointerV1 {
            schema_version: SKILL_STORE_SCHEMA_VERSION,
            skill_id: package.plan.skill_id.clone(),
            revision: package.plan.package_digest.clone(),
            package_digest: package.plan.package_digest.clone(),
            receipt_id: install_id,
        };
        write_active_pointer(&skill_dir, &pointer)?;
        Ok(receipt)
    }

    fn lock(&self) -> Result<File, SkillLifecycleError> {
        create_private_dir_all(&self.root)?;
        let path = self.root.join(".lifecycle.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        FileExt::lock_exclusive(&file)?;
        Ok(file)
    }
}

impl SkillLifecycle {
    pub fn default_for_user() -> Result<Self, SkillLifecycleError> {
        Self::new(ManagedSkillStore::default_for_user()?)
    }

    pub fn new(store: ManagedSkillStore) -> Result<Self, SkillLifecycleError> {
        let client = Client::builder()
            .user_agent("cowd-skill-lifecycle/1")
            .timeout(Duration::from_secs(30))
            // Both endpoints below are fixed Cowd trust boundaries. Do not
            // follow a repository-controlled redirect to a different host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| SkillLifecycleError::Remote(error.to_string()))?;
        Ok(Self { store, client })
    }

    #[must_use]
    pub fn store(&self) -> &ManagedSkillStore {
        &self.store
    }

    pub fn plan(
        &self,
        source: &str,
        cwd: &Path,
    ) -> Result<SkillInstallPlanV1, SkillLifecycleError> {
        let acquired = self.acquire(source, cwd)?;
        Ok(collect_package(&acquired.root, acquired.source)?.plan)
    }

    pub fn commit(
        &self,
        source: &str,
        cwd: &Path,
        expected_digest: &str,
        allow_warnings: bool,
        actor: &str,
    ) -> Result<SkillInstallReceiptV1, SkillLifecycleError> {
        let expected_digest = normalize_digest(expected_digest)?;
        let acquired = self.acquire(source, cwd)?;
        let package = collect_package(&acquired.root, acquired.source)?;
        if package.plan.package_digest != expected_digest {
            return Err(SkillLifecycleError::DigestMismatch {
                expected: expected_digest,
                actual: package.plan.package_digest,
            });
        }
        self.store.commit(&package, allow_warnings, actor)
    }

    pub fn plan_directory(
        &self,
        root: &Path,
        source: SkillSourceIdentityV1,
    ) -> Result<SkillInstallPlanV1, SkillLifecycleError> {
        Ok(collect_package(root, source)?.plan)
    }

    pub fn commit_directory(
        &self,
        root: &Path,
        source: SkillSourceIdentityV1,
        expected_digest: &str,
        allow_warnings: bool,
        actor: &str,
    ) -> Result<SkillInstallReceiptV1, SkillLifecycleError> {
        let expected_digest = normalize_digest(expected_digest)?;
        let package = collect_package(root, source)?;
        if package.plan.package_digest != expected_digest {
            return Err(SkillLifecycleError::DigestMismatch {
                expected: expected_digest,
                actual: package.plan.package_digest,
            });
        }
        self.store.commit(&package, allow_warnings, actor)
    }

    fn acquire(&self, source: &str, cwd: &Path) -> Result<AcquiredPackage, SkillLifecycleError> {
        if let Some(github) = parse_github_source(source)? {
            return self.acquire_github(&github);
        }
        acquire_local(source, cwd)
    }

    fn acquire_github(
        &self,
        source: &GithubSource,
    ) -> Result<AcquiredPackage, SkillLifecycleError> {
        let commit = if is_github_commit_sha(&source.requested_ref) {
            source.requested_ref.to_ascii_lowercase()
        } else {
            let mut commit_url = reqwest::Url::parse(&format!(
                "https://api.github.com/repos/{}/{}/commits/",
                source.owner, source.repository
            ))
            .map_err(|error| SkillLifecycleError::InvalidSource(error.to_string()))?;
            commit_url
                .path_segments_mut()
                .map_err(|()| {
                    SkillLifecycleError::InvalidSource(
                        "GitHub commit endpoint cannot accept path segments".to_string(),
                    )
                })?
                .push(&source.requested_ref);
            let mut request = self.client.get(commit_url);
            if let Some(token) = github_token() {
                request = request.bearer_auth(token);
            }
            let response = checked_response(request.send(), "resolve GitHub revision")?;
            let commit = response
                .json::<GithubCommitResponse>()
                .map_err(|error| SkillLifecycleError::Remote(error.to_string()))?
                .sha;
            if !is_github_commit_sha(&commit) {
                return Err(SkillLifecycleError::Remote(
                    "GitHub returned an invalid commit SHA".to_string(),
                ));
            }
            commit.to_ascii_lowercase()
        };
        let archive_url = format!(
            "https://codeload.github.com/{}/{}/zip/{}",
            source.owner, source.repository, commit
        );
        let mut request = self.client.get(archive_url);
        if let Some(token) = github_token() {
            request = request.bearer_auth(token);
        }
        let response = checked_response(request.send(), "download GitHub archive")?;
        let archive = bounded_response_bytes(response, MAX_SKILL_ARCHIVE_BYTES)?;
        let temporary = TempBuilder::new().prefix("cowd-skill-github-").tempdir()?;
        let package_root = temporary.path().join("package");
        create_private_dir_all(&package_root)?;
        extract_github_zip(&archive, &source.subpath, &package_root)?;
        if !package_root.join("SKILL.md").is_file() {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "GitHub source does not contain SKILL.md at {}",
                source.subpath.display()
            )));
        }
        Ok(AcquiredPackage {
            root: package_root,
            source: SkillSourceIdentityV1 {
                kind: SkillSourceKindV1::GithubArchive,
                locator: format!(
                    "https://github.com/{}/{}{}",
                    source.owner,
                    source.repository,
                    if source.subpath.as_os_str().is_empty() {
                        String::new()
                    } else {
                        format!(
                            "/tree/{}/{}",
                            source.requested_ref,
                            source.subpath.display()
                        )
                    }
                ),
                requested_ref: Some(source.requested_ref.clone()),
                resolved_ref: Some(commit),
            },
            _temporary: Some(temporary),
        })
    }
}

pub fn default_managed_skill_store_root() -> std::io::Result<PathBuf> {
    let root = std::env::var("COWD_CONFIG_HOME")
        .or_else(|_| std::env::var("CC_CONFIG_HOME"))
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".cowd")))
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::NotFound, error))?;
    Ok(root.join("skill-store").join("v1"))
}

pub fn list_managed_skill_entries(
    store_root: &Path,
) -> Result<Vec<ManagedSkillEntryV1>, SkillLifecycleError> {
    if !store_root.is_dir() {
        return Ok(Vec::new());
    }
    reject_symlink(store_root)?;
    let mut entries = Vec::new();
    for entry in fs::read_dir(store_root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let skill_dir = entry.path();
        let Some(pointer) = read_active_pointer(&skill_dir).transpose()? else {
            continue;
        };
        if entry.file_name().to_string_lossy() != pointer.skill_id {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "active pointer Skill id does not match {}",
                skill_dir.display()
            )));
        }
        let package_root = skill_dir
            .join("revisions")
            .join(digest_hex(&pointer.revision)?);
        let prompt_path = package_root.join("SKILL.md");
        let receipt_path = skill_dir
            .join("receipts")
            .join(format!("{}.json", pointer.receipt_id));
        if !prompt_path.is_file() || !receipt_path.is_file() {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "active pointer for {} references missing immutable evidence",
                pointer.skill_id
            )));
        }
        reject_symlink(&prompt_path)?;
        let receipt = read_json_bounded::<SkillInstallReceiptV1>(&receipt_path, 256 * 1024)?;
        if receipt.skill_id != pointer.skill_id
            || receipt.revision != pointer.revision
            || receipt.package_digest != pointer.package_digest
        {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "active pointer for {} does not match its receipt",
                pointer.skill_id
            )));
        }
        let verified = collect_package(&package_root, receipt.source.clone())?;
        if !receipt_matches_plan(&receipt, &verified.plan)?
            || receipt.revision != pointer.revision
            || receipt.plan_digest != plan_evidence_digest(&verified.plan)?
            || !verified.plan.blockers.is_empty()
            || (!verified.plan.warnings.is_empty() && !receipt.warnings_accepted)
        {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "active revision for {} failed content, manifest, or security verification",
                pointer.skill_id
            )));
        }
        entries.push(ManagedSkillEntryV1 {
            pointer,
            skill_dir,
            package_root,
            prompt_path,
            receipt_path,
        });
    }
    entries.sort_by(|left, right| left.pointer.skill_id.cmp(&right.pointer.skill_id));
    Ok(entries)
}

fn find_verified_receipt_for_revision(
    skill_dir: &Path,
    skill_id: &str,
    revision: &str,
    plan: &SkillInstallPlanV1,
) -> Result<SkillInstallReceiptV1, SkillLifecycleError> {
    let receipt_dir = skill_dir.join("receipts");
    if receipt_dir.is_dir() {
        for name in list_names(&receipt_dir, false)? {
            if !name.ends_with(".json") {
                continue;
            }
            let receipt =
                read_json_bounded::<SkillInstallReceiptV1>(&receipt_dir.join(name), 256 * 1024)?;
            if receipt.skill_id == skill_id
                && receipt.revision == revision
                && receipt_matches_plan(&receipt, plan)?
            {
                return Ok(receipt);
            }
        }
    }
    Err(SkillLifecycleError::InvalidPackage(format!(
        "revision {revision} has no matching durable install receipt"
    )))
}

fn receipt_matches_plan(
    receipt: &SkillInstallReceiptV1,
    plan: &SkillInstallPlanV1,
) -> Result<bool, SkillLifecycleError> {
    Ok(receipt.skill_id == plan.skill_id
        && receipt.name == plan.name
        && receipt.package_digest == plan.package_digest
        && receipt.manifest_digest == plan.manifest_digest
        && receipt.package_class == plan.package_class
        && receipt.file_count == plan.files.len()
        && receipt.total_bytes == plan.total_bytes
        && receipt.scanner_version == plan.scanner_version
        && receipt.security_status == plan.security_status
        && receipt.security_findings_digest == canonical_json_digest(&plan.security_findings)?)
}

fn acquire_local(source: &str, cwd: &Path) -> Result<AcquiredPackage, SkillLifecycleError> {
    let candidate = PathBuf::from(source);
    let path = if candidate.is_absolute() {
        candidate
    } else {
        cwd.join(candidate)
    };
    let source_metadata = fs::symlink_metadata(&path)
        .map_err(|error| SkillLifecycleError::InvalidSource(error.to_string()))?;
    if source_metadata.file_type().is_symlink() {
        return Err(SkillLifecycleError::InvalidSource(format!(
            "local Skill source must not be a symbolic link: {}",
            path.display()
        )));
    }
    let canonical = path
        .canonicalize()
        .map_err(|error| SkillLifecycleError::InvalidSource(error.to_string()))?;
    reject_symlink(&canonical)?;
    if canonical.is_dir() {
        if !canonical.join("SKILL.md").is_file() {
            return Err(SkillLifecycleError::InvalidSource(format!(
                "{} must contain SKILL.md",
                canonical.display()
            )));
        }
        let temporary = TempBuilder::new().prefix("cowd-skill-local-").tempdir()?;
        let root = temporary.path().join("package");
        create_private_dir_all(&root)?;
        snapshot_local_directory(&canonical, &root)?;
        return Ok(AcquiredPackage {
            root,
            source: SkillSourceIdentityV1 {
                kind: SkillSourceKindV1::LocalDirectory,
                locator: canonical.display().to_string(),
                requested_ref: None,
                resolved_ref: None,
            },
            _temporary: Some(temporary),
        });
    }
    if canonical.is_file()
        && canonical
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
    {
        let temporary = TempBuilder::new()
            .prefix("cowd-skill-markdown-")
            .tempdir()?;
        let root = temporary.path().join("package");
        create_private_dir_all(&root)?;
        let bytes = read_stable_regular_file(&canonical, &source_metadata, "SKILL.md")?;
        write_collected_files(
            &root,
            &[CollectedFile {
                relative: "SKILL.md".to_string(),
                executable: is_executable_resource("SKILL.md", &bytes),
                bytes,
            }],
        )?;
        return Ok(AcquiredPackage {
            root,
            source: SkillSourceIdentityV1 {
                kind: SkillSourceKindV1::LocalMarkdown,
                locator: canonical.display().to_string(),
                requested_ref: None,
                resolved_ref: None,
            },
            _temporary: Some(temporary),
        });
    }
    Err(SkillLifecycleError::InvalidSource(format!(
        "{} must be a directory with SKILL.md, a Markdown file, or a supported GitHub URL",
        canonical.display()
    )))
}

fn snapshot_local_directory(source: &Path, target: &Path) -> Result<(), SkillLifecycleError> {
    let mut files = Vec::new();
    let mut casefold = BTreeSet::new();
    let mut total_bytes = 0u64;
    collect_package_files(
        source,
        source,
        0,
        &mut files,
        &mut casefold,
        &mut total_bytes,
    )?;
    files.sort_by(|left, right| left.relative.as_bytes().cmp(right.relative.as_bytes()));
    write_collected_files(target, &files)
}

fn collect_package(
    root: &Path,
    source: SkillSourceIdentityV1,
) -> Result<CollectedPackage, SkillLifecycleError> {
    if !root.is_dir() {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "{} is not a directory",
            root.display()
        )));
    }
    reject_symlink(root)?;
    let mut files = Vec::new();
    let mut casefold = BTreeSet::new();
    let mut total_bytes = 0u64;
    collect_package_files(root, root, 0, &mut files, &mut casefold, &mut total_bytes)?;
    files.sort_by(|left, right| left.relative.as_bytes().cmp(right.relative.as_bytes()));
    if files.is_empty() {
        return Err(SkillLifecycleError::InvalidPackage(
            "package contains no files".to_string(),
        ));
    }
    let prompt = files
        .iter()
        .find(|file| file.relative == "SKILL.md")
        .ok_or_else(|| SkillLifecycleError::InvalidPackage("SKILL.md is required".to_string()))?;
    let _prompt_text = std::str::from_utf8(&prompt.bytes).map_err(|_| {
        SkillLifecycleError::InvalidPackage("SKILL.md must be UTF-8 text".to_string())
    })?;
    let parsed = parse_skill_file(&root.join("SKILL.md"))
        .map_err(|error| SkillLifecycleError::InvalidPackage(error.to_string()))?;
    let manifest = parsed.manifest.as_ref();
    let name = get_skill_name(&parsed)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SkillLifecycleError::InvalidPackage(
                "SKILL.md must declare a non-empty name in YAML frontmatter".to_string(),
            )
        })?
        .to_string();
    let skill_id = validate_skill_id(&stable_skill_id(&name))?;
    let description = get_skill_description(&parsed)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SkillLifecycleError::InvalidPackage(
                "SKILL.md must declare a non-empty description in YAML frontmatter".to_string(),
            )
        })?
        .to_string();
    let package_digest = tree_digest(&files);
    let manifest_value = match manifest {
        Some(value) => serde_json::to_value(value)?,
        None => serde_json::json!({
            "name": name.clone(),
            "description": description.clone(),
        }),
    };
    let manifest_digest = canonical_json_digest(&manifest_value)?;
    let inspection = inspect_skill_package(root)?;
    let package_class = package_class(&inspection);
    let mut blockers = inspection.blocked_reasons.clone();
    if package_class == SkillPackageClassV1::ExecutableExtension {
        blockers.push(
            "package declares an MCP server or sidecar; install it through the governed Plugin/MCP lifecycle, not as a Skill"
                .to_string(),
        );
    }
    let (security_status, security_findings) = scan_all_text_files(&files, &name);
    let mut security_findings = security_findings;
    let findings_exceeded = security_findings.len() > 512;
    security_findings.truncate(512);
    if security_status == SecurityStatus::Danger {
        blockers.push("security scan contains high or critical findings".to_string());
    }
    if findings_exceeded {
        blockers.push("security scan finding count exceeds the bounded review limit".to_string());
    }
    let mut warnings = Vec::new();
    if security_status == SecurityStatus::Warning {
        warnings
            .push("security scan contains findings that require explicit acceptance".to_string());
    }
    if files.iter().any(|file| file.executable) {
        warnings.push(
            "package contains script or executable resources; Cowd stores them inert and execution still requires the ordinary governed tool path"
                .to_string(),
        );
    }
    if matches!(source.kind, SkillSourceKindV1::GithubArchive)
        && manifest
            .and_then(|value| value.license.as_deref())
            .is_none()
        && !files.iter().any(|file| {
            let lower = file.relative.to_ascii_lowercase();
            lower == "license" || lower.starts_with("license.") || lower == "copying"
        })
    {
        warnings.push(
            "remote package does not declare or include a license; provenance is recorded but legal permission must be reviewed"
                .to_string(),
        );
    }
    warnings.sort();
    warnings.dedup();
    blockers.sort();
    blockers.dedup();
    let file_inventory = files
        .iter()
        .map(|file| SkillPackageFileV1 {
            path: file.relative.clone(),
            bytes: u64::try_from(file.bytes.len()).unwrap_or(u64::MAX),
            sha256: format!("sha256:{:x}", Sha256::digest(&file.bytes)),
            executable: file.executable,
        })
        .collect::<Vec<_>>();
    let plan = SkillInstallPlanV1 {
        schema_version: SKILL_STORE_SCHEMA_VERSION,
        skill_id,
        name,
        description,
        version: manifest.and_then(|value| value.version.clone()),
        author: manifest.and_then(|value| value.author.clone()),
        license: manifest.and_then(|value| value.license.clone()),
        source: source.clone(),
        package_class,
        package_digest,
        manifest_digest,
        total_bytes,
        files: file_inventory,
        inspection,
        security_status,
        security_findings,
        installable: blockers.is_empty(),
        blockers,
        warnings,
        scanner_version: SCANNER_VERSION.to_string(),
    };
    Ok(CollectedPackage {
        source,
        files,
        plan,
    })
}

fn collect_package_files(
    root: &Path,
    dir: &Path,
    depth: usize,
    files: &mut Vec<CollectedFile>,
    casefold: &mut BTreeSet<String>,
    total_bytes: &mut u64,
) -> Result<(), SkillLifecycleError> {
    if depth > MAX_SKILL_DEPTH {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "package depth exceeds {MAX_SKILL_DEPTH}"
        )));
    }
    let mut entries = fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "symbolic links are not allowed: {}",
                path.display()
            )));
        }
        let relative_path = path.strip_prefix(root).map_err(|_| {
            SkillLifecycleError::InvalidPackage("package path escaped its root".to_string())
        })?;
        let relative = normalized_relative_path(relative_path)?;
        let folded = relative.to_lowercase();
        if !casefold.insert(folded) {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "case-folding path collision: {relative}"
            )));
        }
        if metadata.is_dir() {
            if matches!(
                entry.file_name().to_string_lossy().as_ref(),
                ".git" | "node_modules" | "target"
            ) {
                return Err(SkillLifecycleError::InvalidPackage(format!(
                    "dependency, VCS, and build-output directories must not be shipped: {relative}"
                )));
            }
            collect_package_files(root, &path, depth + 1, files, casefold, total_bytes)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "only regular files and directories are allowed: {relative}"
            )));
        }
        reject_hard_link(&metadata, &relative)?;
        let bytes = read_stable_regular_file(&path, &metadata, &relative)?;
        let byte_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        *total_bytes = total_bytes.saturating_add(byte_len);
        if *total_bytes > MAX_SKILL_EXTRACTED_BYTES {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "package exceeds {MAX_SKILL_EXTRACTED_BYTES} bytes"
            )));
        }
        if files.len() >= MAX_SKILL_FILES {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "package exceeds {MAX_SKILL_FILES} files"
            )));
        }
        // ZIP archives do not reliably preserve executable mode. Classify the
        // resource from its bounded bytes/path so a remote script cannot lose
        // its review warning during extraction, while an ordinary Markdown
        // file on a 0777-mounted filesystem is not mislabeled executable.
        let executable = is_executable_resource(&relative, &bytes);
        files.push(CollectedFile {
            relative,
            bytes,
            executable,
        });
    }
    Ok(())
}

fn read_stable_regular_file(
    path: &Path,
    observed: &fs::Metadata,
    display_path: &str,
) -> Result<Vec<u8>, SkillLifecycleError> {
    if !observed.is_file() || observed.file_type().is_symlink() {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "only regular files are allowed: {display_path}"
        )));
    }
    reject_hard_link(observed, display_path)?;
    if observed.len() > MAX_SKILL_FILE_BYTES {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "file exceeds {MAX_SKILL_FILE_BYTES} bytes: {display_path}"
        )));
    }
    let mut input = File::open(path)?;
    let opened = input.metadata()?;
    if !same_file_state(observed, &opened) {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "file changed while the package was being inspected: {display_path}"
        )));
    }
    let mut bytes = Vec::new();
    (&mut input)
        .take(MAX_SKILL_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_SKILL_FILE_BYTES {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "file exceeds {MAX_SKILL_FILE_BYTES} bytes: {display_path}"
        )));
    }
    let after = input.metadata()?;
    if !same_file_state(&opened, &after) {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "file changed while the package was being inspected: {display_path}"
        )));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn same_file_state(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_file_state(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len() && left.modified().ok() == right.modified().ok()
}

fn package_class(inspection: &SkillInspectionReport) -> SkillPackageClassV1 {
    if inspection.recommended_adapters.iter().any(|adapter| {
        matches!(
            adapter,
            SkillAdapterKind::McpServer | SkillAdapterKind::SidecarService
        )
    }) {
        SkillPackageClassV1::ExecutableExtension
    } else if inspection
        .recommended_adapters
        .iter()
        .any(|adapter| !matches!(adapter, SkillAdapterKind::PromptOnly))
    {
        SkillPackageClassV1::Workflow
    } else {
        SkillPackageClassV1::Prompt
    }
}

fn scan_all_text_files(
    files: &[CollectedFile],
    skill_name: &str,
) -> (SecurityStatus, Vec<SecurityFinding>) {
    let mut findings = Vec::new();
    for file in files {
        let Ok(content) = std::str::from_utf8(&file.bytes) else {
            continue;
        };
        if content.contains('\0') {
            continue;
        }
        let scan = scan_skill_content(content, skill_name);
        findings.extend(scan.findings.into_iter().map(|mut finding| {
            finding.location = Some(match finding.location {
                Some(location) => format!("{}:{location}", file.relative),
                None => file.relative.clone(),
            });
            finding
        }));
    }
    let status = if findings.iter().any(|finding| {
        matches!(
            finding.severity,
            crate::Severity::High | crate::Severity::Critical
        )
    }) {
        SecurityStatus::Danger
    } else if findings.is_empty() {
        SecurityStatus::Safe
    } else {
        SecurityStatus::Warning
    };
    (status, findings)
}

fn tree_digest(files: &[CollectedFile]) -> String {
    let mut digest = Sha256::new();
    digest.update(TREE_DIGEST_DOMAIN);
    for file in files {
        digest.update((file.relative.len() as u64).to_be_bytes());
        digest.update(file.relative.as_bytes());
        digest.update((file.bytes.len() as u64).to_be_bytes());
        digest.update(&file.bytes);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn canonical_json_digest<T: Serialize>(value: &T) -> Result<String, SkillLifecycleError> {
    let bytes = serde_json::to_vec(value)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn plan_evidence_digest(plan: &SkillInstallPlanV1) -> Result<String, SkillLifecycleError> {
    let mut value = serde_json::to_value(plan)?;
    // The inspection root is an acquisition-time filesystem location. It is
    // intentionally excluded so the same reviewed plan can be reverified
    // after bytes move into their immutable content-addressed revision.
    if let Some(inspection) = value
        .pointer_mut("/inspection")
        .and_then(serde_json::Value::as_object_mut)
    {
        inspection.remove("source_root");
    }
    canonical_json_digest(&value)
}

fn write_collected_files(root: &Path, files: &[CollectedFile]) -> Result<(), SkillLifecycleError> {
    for file in files {
        let target = root.join(&file.relative);
        if let Some(parent) = target.parent() {
            create_private_dir_all(parent)?;
        }
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&target)?;
        output.write_all(&file.bytes)?;
        output.sync_all()?;
        #[cfg(unix)]
        if file.executable {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

fn parse_github_source(source: &str) -> Result<Option<GithubSource>, SkillLifecycleError> {
    let source = source.trim();
    if !(source.starts_with("github://")
        || source.starts_with("https://")
        || source.starts_with("http://"))
    {
        return Ok(None);
    }
    let url = reqwest::Url::parse(source)
        .map_err(|error| SkillLifecycleError::InvalidSource(error.to_string()))?;
    if url.scheme() != "github" && url.host_str() != Some("github.com") {
        return Ok(None);
    }
    let (owner, parts) = if url.scheme() == "github" {
        let owner = url.host_str().unwrap_or_default().to_string();
        let parts = url
            .path_segments()
            .into_iter()
            .flatten()
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        (owner, parts)
    } else {
        if url.host_str() != Some("github.com") || url.username() != "" || url.password().is_some()
        {
            return Err(SkillLifecycleError::InvalidSource(
                "GitHub source must not contain credentials or a non-GitHub host".to_string(),
            ));
        }
        let mut parts = url
            .path_segments()
            .into_iter()
            .flatten()
            .filter(|part| !part.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>();
        if parts.is_empty() {
            return Err(SkillLifecycleError::InvalidSource(
                "GitHub source must contain owner and repository".to_string(),
            ));
        }
        (parts.remove(0), parts)
    };
    if owner.is_empty() || parts.is_empty() {
        return Err(SkillLifecycleError::InvalidSource(
            "GitHub source must contain owner and repository".to_string(),
        ));
    }
    validate_github_atom(&owner, "owner")?;
    let repository = parts[0].trim_end_matches(".git").to_string();
    validate_github_atom(&repository, "repository")?;
    let mut requested_ref = url
        .query_pairs()
        .find_map(|(key, value)| (key == "ref").then(|| value.into_owned()))
        .unwrap_or_else(|| "HEAD".to_string());
    let mut subpath = PathBuf::new();
    if url.scheme() == "github" {
        for part in parts.iter().skip(1) {
            subpath.push(part);
        }
    } else if parts
        .get(1)
        .is_some_and(|part| part == "tree" || part == "blob")
    {
        requested_ref = parts.get(2).cloned().unwrap_or_else(|| "HEAD".to_string());
        for part in parts.iter().skip(3) {
            subpath.push(part);
        }
        if parts.get(1).is_some_and(|part| part == "blob") {
            subpath.pop();
        }
    } else {
        for part in parts.iter().skip(1) {
            subpath.push(part);
        }
    }
    validate_github_ref(&requested_ref)?;
    validate_relative_path(&subpath)?;
    Ok(Some(GithubSource {
        owner,
        repository,
        requested_ref,
        subpath,
    }))
}

fn extract_github_zip(
    archive: &[u8],
    subpath: &Path,
    destination: &Path,
) -> Result<(), SkillLifecycleError> {
    let mut archive = zip::ZipArchive::new(Cursor::new(archive))
        .map_err(|error| SkillLifecycleError::InvalidPackage(error.to_string()))?;
    let mut count = 0usize;
    let mut total = 0u64;
    let mut seen = BTreeSet::new();
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| SkillLifecycleError::InvalidPackage(error.to_string()))?;
        let Some(enclosed) = entry.enclosed_name() else {
            return Err(SkillLifecycleError::InvalidPackage(
                "GitHub archive contains an unsafe path".to_string(),
            ));
        };
        let mut components = enclosed.components();
        let _repository_prefix = components.next();
        let repository_relative = components.as_path();
        let Ok(selected) = repository_relative.strip_prefix(subpath) else {
            continue;
        };
        if selected.as_os_str().is_empty() || entry.is_dir() {
            continue;
        }
        if entry
            .unix_mode()
            .is_some_and(|mode| mode & 0o170_000 == 0o120_000)
        {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "GitHub archive contains a symbolic link: {}",
                enclosed.display()
            )));
        }
        validate_relative_path(selected)?;
        let relative = normalized_relative_path(selected)?;
        if !seen.insert(relative.to_lowercase()) {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "GitHub archive contains a path collision: {relative}"
            )));
        }
        count = count.saturating_add(1);
        total = total.saturating_add(entry.size());
        if count > MAX_SKILL_FILES
            || total > MAX_SKILL_EXTRACTED_BYTES
            || entry.size() > MAX_SKILL_FILE_BYTES
        {
            return Err(SkillLifecycleError::InvalidPackage(
                "GitHub Skill exceeds package resource limits".to_string(),
            ));
        }
        let target = destination.join(&relative);
        if let Some(parent) = target.parent() {
            create_private_dir_all(parent)?;
        }
        let mut bytes = Vec::with_capacity(usize::try_from(entry.size()).unwrap_or_default());
        entry
            .by_ref()
            .take(MAX_SKILL_FILE_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != entry.size() {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "GitHub archive file size mismatch: {relative}"
            )));
        }
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(target)?;
        output.write_all(&bytes)?;
    }
    Ok(())
}

fn checked_response(
    response: Result<Response, reqwest::Error>,
    operation: &str,
) -> Result<Response, SkillLifecycleError> {
    let response = response.map_err(|error| SkillLifecycleError::Remote(error.to_string()))?;
    if !response.status().is_success() {
        if operation == "resolve GitHub revision"
            && response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Err(SkillLifecycleError::Remote(
                "GitHub revision lookup was refused or rate-limited; provide GITHUB_TOKEN/GH_TOKEN or use an exact 40-character commit SHA"
                    .to_string(),
            ));
        }
        return Err(SkillLifecycleError::Remote(format!(
            "{operation} returned HTTP {}",
            response.status()
        )));
    }
    Ok(response)
}

fn bounded_response_bytes(
    mut response: Response,
    max_bytes: usize,
) -> Result<Vec<u8>, SkillLifecycleError> {
    if response
        .content_length()
        .is_some_and(|length| length > u64::try_from(max_bytes).unwrap_or(u64::MAX))
    {
        return Err(SkillLifecycleError::Remote(format!(
            "remote archive exceeds {max_bytes} bytes"
        )));
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(
            u64::try_from(max_bytes)
                .unwrap_or(u64::MAX)
                .saturating_add(1),
        )
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(SkillLifecycleError::Remote(format!(
            "remote archive exceeds {max_bytes} bytes"
        )));
    }
    Ok(bytes)
}

fn github_token() -> Option<String> {
    std::env::var("GITHUB_TOKEN")
        .or_else(|_| std::env::var("GH_TOKEN"))
        .ok()
        .filter(|token| !token.trim().is_empty())
}

fn read_active_pointer(
    skill_dir: &Path,
) -> Option<Result<ManagedSkillActivePointerV1, SkillLifecycleError>> {
    let path = skill_dir.join("active.json");
    path.is_file().then(|| {
        reject_symlink(&path)?;
        let pointer = read_json_bounded::<ManagedSkillActivePointerV1>(&path, 64 * 1024)?;
        if pointer.schema_version != SKILL_STORE_SCHEMA_VERSION
            || pointer.skill_id != validate_skill_id(&pointer.skill_id)?
            || normalize_digest(&pointer.revision)? != pointer.package_digest
        {
            return Err(SkillLifecycleError::InvalidPackage(format!(
                "invalid active pointer {}",
                path.display()
            )));
        }
        Ok(pointer)
    })
}

fn write_active_pointer(
    skill_dir: &Path,
    pointer: &ManagedSkillActivePointerV1,
) -> Result<(), SkillLifecycleError> {
    let mut temporary = TempBuilder::new()
        .prefix("active-")
        .tempfile_in(skill_dir)?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), pointer)?;
    temporary.as_file_mut().write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    set_private_file_mode(temporary.path())?;
    let (file, temporary_path) = temporary.keep().map_err(|error| error.error)?;
    drop(file);
    fs::rename(temporary_path, skill_dir.join("active.json"))?;
    sync_dir(skill_dir)?;
    Ok(())
}

fn write_new_json<T: Serialize>(path: &Path, value: &T) -> Result<(), SkillLifecycleError> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    set_private_file_mode(path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn read_json_bounded<T: for<'de> Deserialize<'de>>(
    path: &Path,
    max_bytes: u64,
) -> Result<T, SkillLifecycleError> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > max_bytes {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "metadata file exceeds {max_bytes} bytes: {}",
            path.display()
        )));
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn list_names(
    path: impl AsRef<Path>,
    directories: bool,
) -> Result<Vec<String>, SkillLifecycleError> {
    let path = path.as_ref();
    if !path.is_dir() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() || file_type.is_dir() != directories {
            continue;
        }
        names.push(entry.file_name().to_string_lossy().to_string());
    }
    Ok(names)
}

fn normalized_relative_path(path: &Path) -> Result<String, SkillLifecycleError> {
    validate_relative_path(path)?;
    path.to_str()
        .map(|value| value.replace('\\', "/"))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            SkillLifecycleError::InvalidPackage(
                "Skill paths must be non-empty UTF-8 relative paths".to_string(),
            )
        })
}

fn validate_relative_path(path: &Path) -> Result<(), SkillLifecycleError> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "unsafe relative path: {}",
            path.display()
        )));
    }
    if path.components().count() > MAX_SKILL_DEPTH {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "path exceeds {MAX_SKILL_DEPTH} components: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_skill_id(value: &str) -> Result<String, SkillLifecycleError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 64
        || value.starts_with('-')
        || value.ends_with('-')
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
    {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "invalid normalized Skill id: {value}"
        )));
    }
    Ok(value.to_string())
}

fn validate_github_atom(value: &str, field: &str) -> Result<(), SkillLifecycleError> {
    if value.is_empty()
        || value.len() > 100
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(SkillLifecycleError::InvalidSource(format!(
            "invalid GitHub {field}"
        )));
    }
    Ok(())
}

fn validate_github_ref(value: &str) -> Result<(), SkillLifecycleError> {
    let invalid = value.is_empty()
        || value.len() > 256
        || value.starts_with('/')
        || value.starts_with('.')
        || value.ends_with('/')
        || value.ends_with('.')
        || value.ends_with(".lock")
        || value.contains("..")
        || value.contains("@{")
        || value.contains("//")
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || value
            .bytes()
            .any(|byte| matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\'));
    if invalid {
        return Err(SkillLifecycleError::InvalidSource(
            "GitHub ref is not a safe canonical Git reference".to_string(),
        ));
    }
    Ok(())
}

fn is_github_commit_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn normalize_digest(value: &str) -> Result<String, SkillLifecycleError> {
    let value = value.trim().to_ascii_lowercase();
    let hex = value.strip_prefix("sha256:").unwrap_or(&value);
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SkillLifecycleError::InvalidSource(
            "digest must be sha256:<64 lowercase hex>".to_string(),
        ));
    }
    Ok(format!("sha256:{hex}"))
}

fn digest_hex(digest: &str) -> Result<&str, SkillLifecycleError> {
    let normalized = normalize_digest(digest)?;
    // The caller only needs a path component during this stack frame. Returning
    // from the original input avoids allocating another owned path component.
    if digest.starts_with("sha256:") && digest == normalized {
        return Ok(&digest["sha256:".len()..]);
    }
    Err(SkillLifecycleError::InvalidSource(
        "stored digest must use canonical sha256:<lowercase hex> form".to_string(),
    ))
}

fn sanitize_actor(actor: &str) -> String {
    let value = actor.trim();
    if value.is_empty() {
        return "unknown".to_string();
    }
    value
        .chars()
        .take(128)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

fn reject_symlink(path: &Path) -> Result<(), SkillLifecycleError> {
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "symbolic link is not allowed: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn reject_hard_link(metadata: &fs::Metadata, relative: &str) -> Result<(), SkillLifecycleError> {
    use std::os::unix::fs::MetadataExt;
    if metadata.nlink() != 1 {
        return Err(SkillLifecycleError::InvalidPackage(format!(
            "hard links are not allowed: {relative}"
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn reject_hard_link(_metadata: &fs::Metadata, _relative: &str) -> Result<(), SkillLifecycleError> {
    Ok(())
}

fn is_executable_resource(relative: &str, bytes: &[u8]) -> bool {
    if bytes.starts_with(b"#!") || bytes.starts_with(b"\x7fELF") || bytes.starts_with(b"MZ") {
        return true;
    }
    Path::new(relative)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "sh" | "bash" | "zsh" | "fish" | "py" | "rb" | "pl" | "ps1" | "exe" | "bin"
            )
        })
}

fn create_private_dir_all(path: &Path) -> Result<(), SkillLifecycleError> {
    fs::create_dir_all(path)?;
    reject_symlink(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file_mode(path: &Path) -> Result<(), SkillLifecycleError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn make_revision_read_only(root: &Path) -> Result<(), SkillLifecycleError> {
    let mut directories = Vec::new();
    for entry in walk_directory(root)? {
        let metadata = fs::symlink_metadata(&entry)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.is_dir() {
                directories.push(entry);
            } else {
                let mode = if metadata.permissions().mode() & 0o111 != 0 {
                    0o555
                } else {
                    0o444
                };
                fs::set_permissions(&entry, fs::Permissions::from_mode(mode))?;
            }
        }
        #[cfg(not(unix))]
        if metadata.is_file() {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&entry, permissions)?;
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in directories {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o555))?;
        }
    }
    Ok(())
}

fn walk_directory(root: &Path) -> Result<Vec<PathBuf>, SkillLifecycleError> {
    let mut result = vec![root.to_path_buf()];
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                pending.push(path.clone());
            }
            result.push(path);
        }
    }
    Ok(result)
}

fn sync_tree(root: &Path) -> Result<(), SkillLifecycleError> {
    let mut directories = Vec::new();
    for path in walk_directory(root)? {
        if path.is_file() {
            File::open(&path)?.sync_all()?;
        } else if path.is_dir() {
            directories.push(path);
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_dir(&directory)?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), SkillLifecycleError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zip::write::SimpleFileOptions;

    fn package(root: &Path, name: &str, body: &str) {
        fs::create_dir_all(root).expect("package root");
        fs::write(
            root.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: test skill\nversion: 1.0.0\nlicense: MIT\n---\n{body}\n"),
        )
        .expect("skill");
    }

    #[test]
    fn plan_commit_replace_rollback_and_deactivate_are_evidence_bound() {
        let temp = tempfile::tempdir().expect("temp");
        let source = temp.path().join("source");
        package(&source, "demo-skill", "First revision");
        let lifecycle = SkillLifecycle::new(ManagedSkillStore::new(temp.path().join("store")))
            .expect("lifecycle");
        let first = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("first plan");
        let receipt = lifecycle
            .commit(
                source.to_str().expect("source"),
                temp.path(),
                &first.package_digest,
                false,
                "human:test",
            )
            .expect("first commit");
        assert_eq!(receipt.revision, first.package_digest);
        let entries = list_managed_skill_entries(lifecycle.store().root()).expect("entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].pointer.skill_id, "demo-skill");

        package(&source, "demo-skill", "Second revision");
        let second = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("second plan");
        lifecycle
            .commit(
                source.to_str().expect("source"),
                temp.path(),
                &second.package_digest,
                false,
                "human:test",
            )
            .expect("second commit");
        let rollback = lifecycle
            .store()
            .rollback("demo-skill", &first.package_digest, "human:test")
            .expect("rollback");
        assert_eq!(rollback.revision, first.package_digest);
        assert_eq!(rollback.previous_revision, Some(second.package_digest));
        assert!(lifecycle
            .store()
            .deactivate("demo-skill", "human:test")
            .expect("deactivate")
            .is_some());
        assert!(list_managed_skill_entries(lifecycle.store().root())
            .expect("entries")
            .is_empty());
        assert_eq!(
            lifecycle
                .store()
                .status("demo-skill")
                .expect("status")
                .revisions
                .len(),
            2
        );
    }

    #[test]
    fn commit_rejects_toctou_and_dangerous_package() {
        let temp = tempfile::tempdir().expect("temp");
        let source = temp.path().join("source");
        package(&source, "danger", "Safe first");
        let lifecycle = SkillLifecycle::new(ManagedSkillStore::new(temp.path().join("store")))
            .expect("lifecycle");
        let plan = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("plan");
        fs::write(
            source.join("SKILL.md"),
            "---\nname: danger\ndescription: bad\n---\nrm -rf /home\n",
        )
        .expect("mutate");
        assert!(matches!(
            lifecycle.commit(
                source.to_str().expect("source"),
                temp.path(),
                &plan.package_digest,
                true,
                "model:test"
            ),
            Err(SkillLifecycleError::DigestMismatch { .. })
        ));
        let dangerous = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("danger plan");
        assert!(!dangerous.installable);
        assert_eq!(dangerous.security_status, SecurityStatus::Danger);
        assert!(matches!(
            lifecycle.commit(
                source.to_str().expect("source"),
                temp.path(),
                &dangerous.package_digest,
                true,
                "model:test"
            ),
            Err(SkillLifecycleError::Blocked(_))
        ));
    }

    #[test]
    fn package_limits_and_links_fail_closed() {
        let temp = tempfile::tempdir().expect("temp");
        let source = temp.path().join("source");
        package(&source, "linked", "safe");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("SKILL.md", source.join("alias.md")).expect("symlink");
            let lifecycle = SkillLifecycle::new(ManagedSkillStore::new(temp.path().join("store")))
                .expect("lifecycle");
            assert!(matches!(
                lifecycle.plan(source.to_str().expect("source"), temp.path()),
                Err(SkillLifecycleError::InvalidPackage(_))
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn local_source_root_symlink_fails_closed() {
        let temp = tempfile::tempdir().expect("temp");
        let source = temp.path().join("source");
        package(&source, "linked-root", "safe");
        let linked = temp.path().join("linked-source");
        std::os::unix::fs::symlink(&source, &linked).expect("source symlink");
        let lifecycle = SkillLifecycle::new(ManagedSkillStore::new(temp.path().join("store")))
            .expect("lifecycle");

        assert!(matches!(
            lifecycle.plan(linked.to_str().expect("source"), temp.path()),
            Err(SkillLifecycleError::InvalidSource(_))
        ));
    }

    #[test]
    fn executable_resources_require_review_and_remain_evidence_bound() {
        let temp = tempfile::tempdir().expect("temp");
        let source = temp.path().join("source");
        package(
            &source,
            "workflow",
            "Use scripts/run.sh through the governed shell tool.",
        );
        fs::create_dir_all(source.join("scripts")).expect("scripts");
        fs::write(source.join("scripts/run.sh"), "#!/bin/sh\nprintf ok\n").expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                source.join("scripts/run.sh"),
                fs::Permissions::from_mode(0o755),
            )
            .expect("mode");
        }
        let lifecycle = SkillLifecycle::new(ManagedSkillStore::new(temp.path().join("store")))
            .expect("lifecycle");
        let plan = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("plan");
        #[cfg(unix)]
        assert!(!plan.warnings.is_empty());
        #[cfg(unix)]
        assert!(matches!(
            lifecycle.commit(
                source.to_str().expect("source"),
                temp.path(),
                &plan.package_digest,
                false,
                "human:test"
            ),
            Err(SkillLifecycleError::Blocked(_))
        ));
        lifecycle
            .commit(
                source.to_str().expect("source"),
                temp.path(),
                &plan.package_digest,
                true,
                "human:test",
            )
            .expect("reviewed commit");
        assert_eq!(
            list_managed_skill_entries(lifecycle.store().root())
                .expect("verified entry")
                .len(),
            1
        );
    }

    #[test]
    fn executable_extensions_and_tampered_receipts_fail_closed() {
        let temp = tempfile::tempdir().expect("temp");
        let source = temp.path().join("source");
        package(&source, "extension", "Do not start a sidecar as a Skill.");
        fs::write(source.join("mcp.json"), "{}\n").expect("mcp descriptor");
        let lifecycle = SkillLifecycle::new(ManagedSkillStore::new(temp.path().join("store")))
            .expect("lifecycle");
        let plan = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("plan");
        assert_eq!(plan.package_class, SkillPackageClassV1::ExecutableExtension);
        assert!(!plan.installable);

        fs::remove_file(source.join("mcp.json")).expect("remove descriptor");
        let safe = lifecycle
            .plan(source.to_str().expect("source"), temp.path())
            .expect("safe plan");
        let receipt = lifecycle
            .commit(
                source.to_str().expect("source"),
                temp.path(),
                &safe.package_digest,
                false,
                "human:test",
            )
            .expect("commit");
        let receipt_path = lifecycle
            .store()
            .root()
            .join("extension/receipts")
            .join(format!("{}.json", receipt.install_id));
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&receipt_path).expect("receipt")).expect("json");
        value["total_bytes"] = serde_json::json!(receipt.total_bytes + 1);
        fs::write(
            &receipt_path,
            serde_json::to_vec_pretty(&value).expect("serialize"),
        )
        .expect("tamper receipt");
        assert!(matches!(
            list_managed_skill_entries(lifecycle.store().root()),
            Err(SkillLifecycleError::InvalidPackage(_))
        ));
    }

    #[test]
    fn parses_github_sources_without_credentials() {
        let source = parse_github_source(
            "https://github.com/openai/skills/tree/main/skills/docs?ignored=value",
        )
        .expect("parse")
        .expect("github");
        assert_eq!(source.owner, "openai");
        assert_eq!(source.repository, "skills");
        assert_eq!(source.requested_ref, "main");
        assert_eq!(source.subpath, PathBuf::from("skills/docs"));
        let slash_ref =
            parse_github_source("github://openai/skills/skills/docs?ref=feature/review")
                .expect("parse slash ref")
                .expect("github");
        assert_eq!(slash_ref.requested_ref, "feature/review");
        assert!(
            parse_github_source("github://openai/skills/skills/docs?ref=bad%0Aref")
                .expect_err("control character ref")
                .to_string()
                .contains("safe canonical")
        );
        assert!(parse_github_source("https://user:secret@github.com/a/b")
            .expect_err("credentials")
            .to_string()
            .contains("credentials"));
    }

    #[test]
    fn github_selection_ignores_unselected_links_but_rejects_selected_links() {
        fn archive(link: &str) -> Vec<u8> {
            let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
            writer
                .add_symlink(link, "target", SimpleFileOptions::default())
                .expect("link");
            writer
                .start_file(
                    "repository/selected/SKILL.md",
                    SimpleFileOptions::default().unix_permissions(0o644),
                )
                .expect("file");
            writer
                .write_all(b"---\nname: selected\ndescription: selected package\n---\n")
                .expect("content");
            writer.finish().expect("zip").into_inner()
        }

        let safe = tempfile::tempdir().expect("safe");
        extract_github_zip(
            &archive("repository/elsewhere/link"),
            Path::new("selected"),
            safe.path(),
        )
        .expect("unselected link must not poison the selected subtree");
        assert!(safe.path().join("SKILL.md").is_file());

        let blocked = tempfile::tempdir().expect("blocked");
        assert!(matches!(
            extract_github_zip(
                &archive("repository/selected/link"),
                Path::new("selected"),
                blocked.path()
            ),
            Err(SkillLifecycleError::InvalidPackage(message)) if message.contains("symbolic link")
        ));
    }
}
