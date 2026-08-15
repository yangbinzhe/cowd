//! Canonical workspace/repository path and evidence-scope identities.
//!
//! Model-visible paths are aliases. Runtime resolves them once into this
//! identity before authorization, receipt comparison or acceptance checks.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use harness_contract::context::{
    EvidenceCoverageKind, EvidenceObligation, EvidenceObligationKind, EvidenceTargetIdentity,
    ObservedAcceptance, ObservedEvidence, RequiredAcceptance, WorkspaceAccessMode,
    WorkspaceObjectKind, WorkspacePathIdentity, WorkspaceScopeIdentity,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkspacePathIdentityError {
    #[error("workspace root is unavailable: {0}")]
    WorkspaceUnavailable(String),
    #[error("workspace path is invalid or escapes the workspace: {0}")]
    InvalidPath(String),
    #[error("workspace path does not exist: {0}")]
    NotFound(String),
    #[error("workspace path is ambiguous across repositories: {path}; candidates={candidates:?}")]
    Ambiguous {
        path: String,
        candidates: Vec<String>,
    },
    #[error("evidence scope is invalid: {0}")]
    InvalidScope(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepositoryBinding {
    repository_id: String,
    workspace_prefix: String,
}

/// Immutable repository map for one execution workspace.
#[derive(Debug, Clone)]
pub struct WorkspacePathIdentityResolver {
    workspace_root: PathBuf,
    workspace_id: String,
    repositories: Vec<RepositoryBinding>,
}

impl WorkspacePathIdentityResolver {
    pub fn discover(workspace_root: &Path) -> Result<Self, WorkspacePathIdentityError> {
        let workspace_root = workspace_root.canonicalize().map_err(|_| {
            WorkspacePathIdentityError::WorkspaceUnavailable(workspace_root.display().to_string())
        })?;
        let workspace_id = format!(
            "workspace:{:x}",
            Sha256::digest(workspace_root.to_string_lossy().as_bytes())
        );
        let mut repositories = Vec::new();
        if is_repository_root(&workspace_root) {
            repositories.push(RepositoryBinding {
                repository_id: repository_id(&workspace_root),
                workspace_prefix: String::new(),
            });
        } else if let Ok(entries) = std::fs::read_dir(&workspace_root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() || !is_repository_root(&path) {
                    continue;
                }
                let prefix = entry.file_name().to_string_lossy().into_owned();
                if prefix.starts_with('.') {
                    continue;
                }
                repositories.push(RepositoryBinding {
                    repository_id: repository_id(&path),
                    workspace_prefix: prefix,
                });
            }
        }
        if repositories.is_empty() {
            repositories.push(RepositoryBinding {
                repository_id: repository_id(&workspace_root),
                workspace_prefix: String::new(),
            });
        }
        repositories.sort_by(|left, right| {
            left.workspace_prefix
                .cmp(&right.workspace_prefix)
                .then_with(|| left.repository_id.cmp(&right.repository_id))
        });
        repositories.dedup();
        Ok(Self {
            workspace_root,
            workspace_id,
            repositories,
        })
    }

    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    pub fn resolve_existing(
        &self,
        path: &str,
    ) -> Result<WorkspacePathIdentity, WorkspacePathIdentityError> {
        if matches!(path.trim(), "." | "./") {
            if self.repositories.len() != 1 || !self.repositories[0].workspace_prefix.is_empty() {
                return Err(WorkspacePathIdentityError::Ambiguous {
                    path: path.to_string(),
                    candidates: self
                        .repositories
                        .iter()
                        .map(|repository| repository.workspace_prefix.clone())
                        .collect(),
                });
            }
            return Ok(WorkspacePathIdentity {
                workspace_id: self.workspace_id.clone(),
                repository_id: self.repositories[0].repository_id.clone(),
                workspace_relative_path: ".".to_string(),
                repository_relative_path: ".".to_string(),
                object_kind: WorkspaceObjectKind::Directory,
                observed_revision_or_digest: None,
            });
        }
        let relative = self.workspace_relative_input(path)?;
        let direct = self.workspace_root.join(&relative);
        if direct.exists() {
            return self.identity_for_existing(&relative);
        }

        let mut candidates = self
            .repositories
            .iter()
            .filter(|repository| !repository.workspace_prefix.is_empty())
            .filter_map(|repository| {
                let candidate = Path::new(&repository.workspace_prefix).join(&relative);
                self.workspace_root
                    .join(&candidate)
                    .exists()
                    .then_some(candidate)
            })
            .map(|candidate| path_to_slash(&candidate))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        match candidates.len() {
            0 => Err(WorkspacePathIdentityError::NotFound(path.to_string())),
            1 => self.identity_for_existing(Path::new(&candidates.remove(0))),
            _ => Err(WorkspacePathIdentityError::Ambiguous {
                path: path.to_string(),
                candidates,
            }),
        }
    }

    /// Resolve an authorization target that may not exist yet. Missing paths
    /// require an explicit repository prefix whenever the workspace contains
    /// multiple repositories; Runtime never chooses the first repository.
    pub fn resolve_planned_file(
        &self,
        path: &str,
    ) -> Result<WorkspacePathIdentity, WorkspacePathIdentityError> {
        if let Ok(identity) = self.resolve_existing(path) {
            return Ok(identity);
        }
        let relative = self.workspace_relative_input(path)?;
        let repository = self
            .repository_for_relative(&relative)
            .or_else(|| (self.repositories.len() == 1).then(|| &self.repositories[0]));
        let Some(repository) = repository else {
            return Err(WorkspacePathIdentityError::Ambiguous {
                path: path.to_string(),
                candidates: self
                    .repositories
                    .iter()
                    .map(|candidate| candidate.workspace_prefix.clone())
                    .collect(),
            });
        };
        let relative = if !repository.workspace_prefix.is_empty()
            && self.repository_for_relative(&relative).is_none()
        {
            Path::new(&repository.workspace_prefix).join(relative)
        } else {
            relative
        };
        let planned = self.workspace_root.join(&relative);
        let mut ancestor = planned.as_path();
        let canonical_ancestor = loop {
            match ancestor.canonicalize() {
                Ok(canonical) => break canonical,
                Err(_) => {
                    ancestor = ancestor
                        .parent()
                        .ok_or_else(|| WorkspacePathIdentityError::InvalidPath(path.to_string()))?;
                }
            }
        };
        if canonical_ancestor != ancestor {
            return Err(WorkspacePathIdentityError::InvalidPath(path.to_string()));
        }
        let workspace_relative_path = path_to_slash(&relative);
        let repository_relative_path = strip_repository_prefix(&relative, repository);
        Ok(WorkspacePathIdentity {
            workspace_id: self.workspace_id.clone(),
            repository_id: repository.repository_id.clone(),
            workspace_relative_path,
            repository_relative_path,
            object_kind: WorkspaceObjectKind::File,
            observed_revision_or_digest: None,
        })
    }

    pub fn compile_obligation(
        &self,
        raw_scope: &str,
    ) -> Result<EvidenceObligation, WorkspacePathIdentityError> {
        let raw_scope = raw_scope.trim();
        if raw_scope == "network:*" {
            return Ok(EvidenceObligation {
                obligation_id: obligation_id(raw_scope),
                kind: EvidenceObligationKind::NetworkEvidence,
                target: EvidenceTargetIdentity::Network {
                    endpoint: "*".to_string(),
                },
            });
        }
        let (prefix, path) = raw_scope
            .split_once(':')
            .ok_or_else(|| WorkspacePathIdentityError::InvalidScope(raw_scope.to_string()))?;
        if matches!(path.trim(), "." | "./") {
            return Err(WorkspacePathIdentityError::InvalidScope(format!(
                "{raw_scope}; workspace root alias requires one explicit bound scope"
            )));
        }
        let (access_mode, kind, requested_coverage, allow_missing) = match prefix {
            "read" => (
                WorkspaceAccessMode::Read,
                EvidenceObligationKind::ContentRead,
                None,
                false,
            ),
            "list" => (
                WorkspaceAccessMode::Read,
                EvidenceObligationKind::DirectoryListing,
                Some(EvidenceCoverageKind::DirectoryListing),
                false,
            ),
            "recursive" => (
                WorkspaceAccessMode::Read,
                EvidenceObligationKind::RecursiveScan,
                Some(EvidenceCoverageKind::RecursiveContent),
                false,
            ),
            "glob" => (
                WorkspaceAccessMode::Read,
                EvidenceObligationKind::GlobDiscovery,
                Some(EvidenceCoverageKind::GlobDiscovery),
                false,
            ),
            "write" => (
                WorkspaceAccessMode::Write,
                EvidenceObligationKind::WriteEffect,
                Some(EvidenceCoverageKind::WriteEffect),
                true,
            ),
            "verify_after_write" => (
                WorkspaceAccessMode::Read,
                EvidenceObligationKind::VerifyAfterWrite,
                Some(EvidenceCoverageKind::ExactContent),
                false,
            ),
            "verify_upstream_change" => (
                WorkspaceAccessMode::Read,
                EvidenceObligationKind::VerifyUpstreamChange,
                Some(EvidenceCoverageKind::ExactContent),
                false,
            ),
            _ => {
                return Err(WorkspacePathIdentityError::InvalidScope(
                    raw_scope.to_string(),
                ))
            }
        };
        let identity = if allow_missing {
            self.resolve_planned_file(path)?
        } else {
            self.resolve_existing(path)?
        };
        let coverage = requested_coverage.unwrap_or_else(|| {
            if identity.object_kind == WorkspaceObjectKind::File {
                EvidenceCoverageKind::ExactContent
            } else {
                EvidenceCoverageKind::ScopedContent
            }
        });
        Ok(EvidenceObligation {
            obligation_id: obligation_id(&format!(
                "{}:{}:{:?}:{:?}",
                identity.workspace_id, identity.workspace_relative_path, access_mode, coverage
            )),
            kind,
            target: EvidenceTargetIdentity::Workspace {
                scope: WorkspaceScopeIdentity {
                    access_mode,
                    path: identity,
                    coverage,
                },
            },
        })
    }

    /// Compile an external/raw scope into a typed requirement without losing
    /// ambiguity or unavailability. Callers may display the error state, but
    /// must never silently drop it or choose a repository.
    #[must_use]
    pub fn compile_obligation_or_unresolved(&self, raw_scope: &str) -> EvidenceObligation {
        match self.compile_obligation(raw_scope) {
            Ok(obligation) => obligation,
            Err(WorkspacePathIdentityError::Ambiguous { path, candidates }) => EvidenceObligation {
                obligation_id: obligation_id(raw_scope),
                kind: obligation_kind(raw_scope),
                target: EvidenceTargetIdentity::AmbiguousWorkspace {
                    display_alias: path,
                    candidates,
                },
            },
            Err(error) => EvidenceObligation {
                obligation_id: obligation_id(raw_scope),
                kind: obligation_kind(raw_scope),
                target: EvidenceTargetIdentity::UnavailableWorkspace {
                    display_alias: raw_scope.to_string(),
                    reason: error.to_string(),
                },
            },
        }
    }

    #[must_use]
    pub fn compile_required_acceptance(
        &self,
        criteria: &[String],
        raw_scopes: &[String],
    ) -> RequiredAcceptance {
        RequiredAcceptance {
            criteria: criteria.to_vec(),
            evidence_obligations: raw_scopes
                .iter()
                .map(|scope| self.compile_obligation_or_unresolved(scope))
                .collect(),
        }
    }

    pub fn observe_scope(
        &self,
        raw_scope: &str,
        observed_revision_or_digest: Option<&str>,
    ) -> Result<ObservedEvidence, WorkspacePathIdentityError> {
        let obligation = self.compile_obligation(raw_scope)?;
        let mut target = obligation.target;
        if let EvidenceTargetIdentity::Workspace { scope } = &mut target {
            scope.path.observed_revision_or_digest =
                observed_revision_or_digest.map(str::to_string);
        }
        Ok(ObservedEvidence {
            obligation_id: obligation.obligation_id,
            target,
            observed_at_sequence: 0,
            tool_name: String::new(),
            provenance: harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
            evidence_ref: None,
            workspace_prior_state: None,
        })
    }

    pub fn observe_tool_scope(
        &self,
        tool_name: &str,
        raw_scope: &str,
        observed_revision_or_digest: Option<&str>,
        observed_at_sequence: u64,
    ) -> Result<ObservedEvidence, WorkspacePathIdentityError> {
        let mut observed = self.observe_scope(raw_scope, observed_revision_or_digest)?;
        observed.observed_at_sequence = observed_at_sequence;
        observed.tool_name = tool_name.to_string();
        if let EvidenceTargetIdentity::Workspace { scope } = &mut observed.target {
            let (kind, coverage) = match tool_name {
                "read_file" => (
                    EvidenceObligationKind::ContentRead,
                    EvidenceCoverageKind::ExactContent,
                ),
                "list_directory" | "workspace_snapshot" => (
                    EvidenceObligationKind::DirectoryListing,
                    EvidenceCoverageKind::DirectoryListing,
                ),
                "glob_search" => (
                    EvidenceObligationKind::GlobDiscovery,
                    EvidenceCoverageKind::GlobDiscovery,
                ),
                "grep_search" => (
                    EvidenceObligationKind::RecursiveScan,
                    EvidenceCoverageKind::RecursiveContent,
                ),
                _ if scope.access_mode == WorkspaceAccessMode::Write => (
                    EvidenceObligationKind::WriteEffect,
                    EvidenceCoverageKind::WriteEffect,
                ),
                _ => (
                    EvidenceObligationKind::ContentRead,
                    EvidenceCoverageKind::ExactContent,
                ),
            };
            scope.coverage = coverage;
            observed.obligation_id = obligation_id(&format!(
                "observed:{kind:?}:{}:{}",
                scope.path.workspace_id, scope.path.workspace_relative_path
            ));
        }
        Ok(observed)
    }

    /// Convert the canonical path emitted by a successfully completed
    /// ToolHost filesystem adapter into a snapshot identity without touching
    /// the object again. The repository map and lexical workspace boundary
    /// are frozen by this resolver; execution-time path/symlink enforcement
    /// remains the ToolHost's responsibility.
    pub fn observe_trusted_tool_output_file(
        &self,
        tool_name: &str,
        access_mode: WorkspaceAccessMode,
        path: &str,
        digest: &str,
        observed_at_sequence: u64,
    ) -> Result<ObservedEvidence, WorkspacePathIdentityError> {
        if digest.trim().is_empty() || observed_at_sequence == 0 {
            return Err(WorkspacePathIdentityError::InvalidScope(path.to_string()));
        }
        self.observe_trusted_tool_output_scope(
            tool_name,
            access_mode,
            path,
            WorkspaceObjectKind::File,
            if access_mode == WorkspaceAccessMode::Write {
                EvidenceCoverageKind::WriteEffect
            } else {
                EvidenceCoverageKind::ExactContent
            },
            Some(digest),
            observed_at_sequence,
        )
    }

    /// Snapshot a canonical scope emitted by a successful ToolHost adapter.
    /// Resolution uses only the frozen repository map and lexical workspace
    /// boundary; mutable filesystem state cannot rewrite the receipt.
    pub fn observe_trusted_tool_output_scope(
        &self,
        tool_name: &str,
        access_mode: WorkspaceAccessMode,
        path: &str,
        object_kind: WorkspaceObjectKind,
        coverage: EvidenceCoverageKind,
        digest: Option<&str>,
        observed_at_sequence: u64,
    ) -> Result<ObservedEvidence, WorkspacePathIdentityError> {
        if observed_at_sequence == 0
            || matches!(
                coverage,
                EvidenceCoverageKind::ExactContent | EvidenceCoverageKind::WriteEffect
            ) && digest.is_none_or(str::is_empty)
        {
            return Err(WorkspacePathIdentityError::InvalidScope(path.to_string()));
        }
        let relative = self.workspace_relative_input(path)?;
        if (relative.as_os_str().is_empty() || relative == Path::new("."))
            && object_kind == WorkspaceObjectKind::Directory
            && self.repositories.len() == 1
            && self.repositories[0].workspace_prefix.is_empty()
        {
            let identity = WorkspacePathIdentity {
                workspace_id: self.workspace_id.clone(),
                repository_id: self.repositories[0].repository_id.clone(),
                workspace_relative_path: ".".to_string(),
                repository_relative_path: ".".to_string(),
                object_kind,
                observed_revision_or_digest: digest.map(str::to_string),
            };
            return Ok(observed_from_identity(
                tool_name,
                access_mode,
                coverage,
                identity,
                observed_at_sequence,
            ));
        }
        if relative.as_os_str().is_empty() || relative == Path::new(".") {
            return Err(WorkspacePathIdentityError::InvalidPath(path.to_string()));
        }
        let repository = self.repository_for_relative(&relative).ok_or_else(|| {
            WorkspacePathIdentityError::Ambiguous {
                path: path.to_string(),
                candidates: self
                    .repositories
                    .iter()
                    .map(|candidate| candidate.workspace_prefix.clone())
                    .collect(),
            }
        })?;
        let identity = WorkspacePathIdentity {
            workspace_id: self.workspace_id.clone(),
            repository_id: repository.repository_id.clone(),
            workspace_relative_path: path_to_slash(&relative),
            repository_relative_path: strip_repository_prefix(&relative, repository),
            object_kind,
            observed_revision_or_digest: digest.map(str::to_string),
        };
        Ok(observed_from_identity(
            tool_name,
            access_mode,
            coverage,
            identity,
            observed_at_sequence,
        ))
    }

    fn workspace_relative_input(&self, path: &str) -> Result<PathBuf, WorkspacePathIdentityError> {
        let input = Path::new(path.trim());
        let relative = if input.is_absolute() {
            input
                .strip_prefix(&self.workspace_root)
                .map_err(|_| WorkspacePathIdentityError::InvalidPath(path.to_string()))?
        } else {
            input
        };
        normalize_relative(relative)
            .ok_or_else(|| WorkspacePathIdentityError::InvalidPath(path.to_string()))
    }

    fn identity_for_existing(
        &self,
        relative: &Path,
    ) -> Result<WorkspacePathIdentity, WorkspacePathIdentityError> {
        let full = self.workspace_root.join(relative);
        let metadata = full
            .symlink_metadata()
            .map_err(|_| WorkspacePathIdentityError::NotFound(relative.display().to_string()))?;
        let canonical = full
            .canonicalize()
            .map_err(|_| WorkspacePathIdentityError::NotFound(relative.display().to_string()))?;
        if metadata.file_type().is_symlink() || canonical != full {
            return Err(WorkspacePathIdentityError::InvalidPath(
                relative.display().to_string(),
            ));
        }
        let repository = self.repository_for_relative(relative).ok_or_else(|| {
            WorkspacePathIdentityError::InvalidPath(relative.display().to_string())
        })?;
        Ok(WorkspacePathIdentity {
            workspace_id: self.workspace_id.clone(),
            repository_id: repository.repository_id.clone(),
            workspace_relative_path: path_to_slash(relative),
            repository_relative_path: strip_repository_prefix(relative, repository),
            object_kind: if metadata.is_dir() {
                WorkspaceObjectKind::Directory
            } else {
                WorkspaceObjectKind::File
            },
            observed_revision_or_digest: None,
        })
    }

    fn repository_for_relative(&self, relative: &Path) -> Option<&RepositoryBinding> {
        let rendered = path_to_slash(relative);
        self.repositories
            .iter()
            .filter(|repository| {
                repository.workspace_prefix.is_empty()
                    || rendered == repository.workspace_prefix
                    || rendered
                        .strip_prefix(&repository.workspace_prefix)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            })
            .max_by_key(|repository| repository.workspace_prefix.len())
    }
}

fn observed_from_identity(
    tool_name: &str,
    access_mode: WorkspaceAccessMode,
    coverage: EvidenceCoverageKind,
    identity: WorkspacePathIdentity,
    observed_at_sequence: u64,
) -> ObservedEvidence {
    let kind = match coverage {
        EvidenceCoverageKind::DirectoryListing => EvidenceObligationKind::DirectoryListing,
        EvidenceCoverageKind::RecursiveContent => EvidenceObligationKind::RecursiveScan,
        EvidenceCoverageKind::GlobDiscovery => EvidenceObligationKind::GlobDiscovery,
        EvidenceCoverageKind::WriteEffect => EvidenceObligationKind::WriteEffect,
        EvidenceCoverageKind::ExactContent | EvidenceCoverageKind::ScopedContent => {
            EvidenceObligationKind::ContentRead
        }
    };
    ObservedEvidence {
        obligation_id: obligation_id(&format!(
            "observed:{kind:?}:{}:{}",
            identity.workspace_id, identity.workspace_relative_path
        )),
        target: EvidenceTargetIdentity::Workspace {
            scope: WorkspaceScopeIdentity {
                access_mode,
                path: identity,
                coverage,
            },
        },
        observed_at_sequence,
        tool_name: tool_name.to_string(),
        provenance: harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
        evidence_ref: None,
        workspace_prior_state: None,
    }
}

/// Human-facing projection of an observed typed fact. This is never parsed
/// back for authorization or acceptance in new executions.
#[must_use]
pub fn observed_scope_key(observed: &ObservedEvidence) -> String {
    match &observed.target {
        EvidenceTargetIdentity::Network { endpoint } => format!("network:{endpoint}"),
        EvidenceTargetIdentity::AmbiguousWorkspace { display_alias, .. }
        | EvidenceTargetIdentity::UnavailableWorkspace { display_alias, .. } => {
            display_alias.clone()
        }
        EvidenceTargetIdentity::Workspace { scope } => {
            let prefix = match (scope.access_mode, scope.coverage) {
                (WorkspaceAccessMode::Write, _) => "write",
                (_, EvidenceCoverageKind::DirectoryListing) => "list",
                (_, EvidenceCoverageKind::RecursiveContent) => "recursive",
                (_, EvidenceCoverageKind::GlobDiscovery) => "glob",
                _ => "read",
            };
            format!("{prefix}:{}", scope.path.workspace_relative_path)
        }
    }
}

/// Human-facing scope projection for a typed requirement. This is a one-way
/// display/control hint; callers never parse it back into acceptance truth.
#[must_use]
pub fn obligation_scope_key(obligation: &EvidenceObligation) -> String {
    match &obligation.target {
        EvidenceTargetIdentity::Network { endpoint } => format!("network:{endpoint}"),
        EvidenceTargetIdentity::AmbiguousWorkspace { display_alias, .. }
        | EvidenceTargetIdentity::UnavailableWorkspace { display_alias, .. } => {
            display_alias.clone()
        }
        EvidenceTargetIdentity::Workspace { scope } => {
            let prefix = match obligation.kind {
                EvidenceObligationKind::ContentRead => "read",
                EvidenceObligationKind::DirectoryListing => "list",
                EvidenceObligationKind::RecursiveScan => "recursive",
                EvidenceObligationKind::GlobDiscovery => "glob",
                EvidenceObligationKind::WriteEffect => "write",
                EvidenceObligationKind::VerifyAfterWrite => "verify_after_write",
                EvidenceObligationKind::VerifyUpstreamChange => "verify_upstream_change",
                EvidenceObligationKind::NetworkEvidence => "network",
            };
            format!("{prefix}:{}", scope.path.workspace_relative_path)
        }
    }
}

/// Stable fact fingerprint used for novelty. Causal sequence, replay
/// provenance and retrieval location are deliberately excluded: the same
/// object/coverage/digest is one fact, while a changed digest is new evidence.
#[must_use]
pub fn observed_evidence_fingerprint(observed: &ObservedEvidence) -> String {
    let encoded = serde_json::to_vec(&observed.target).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(encoded))
}

/// Return only facts that were freshly executed in this wave and were not
/// already present in prior Runtime observations. Sequence and replay metadata
/// deliberately do not create novelty; a changed target digest does.
#[must_use]
pub fn fresh_novel_observed_evidence_fingerprints(
    prior: &[ObservedEvidence],
    current: &[ObservedEvidence],
) -> BTreeSet<String> {
    let prior = prior
        .iter()
        .map(observed_evidence_fingerprint)
        .collect::<BTreeSet<_>>();
    current
        .iter()
        .filter(|evidence| {
            evidence.provenance
                == harness_contract::context::ObservedEvidenceProvenance::FreshExecution
        })
        .map(observed_evidence_fingerprint)
        .filter(|fingerprint| !prior.contains(fingerprint))
        .collect()
}

#[must_use]
pub fn evaluate_observed_acceptance(
    required: &RequiredAcceptance,
    satisfied_criteria: Vec<String>,
    observed_evidence: Vec<ObservedEvidence>,
) -> ObservedAcceptance {
    let unresolved_obligation_ids = required
        .evidence_obligations
        .iter()
        .filter(|obligation| {
            !observed_evidence.iter().any(|observed| {
                if obligation.kind == EvidenceObligationKind::VerifyAfterWrite {
                    observed_evidence_satisfies(obligation, observed)
                        && observed_evidence.iter().any(|write| {
                            write.observed_at_sequence > 0
                                && write.observed_at_sequence < observed.observed_at_sequence
                                && observed_write_matches_read(write, observed)
                        })
                } else {
                    observed_evidence_satisfies(obligation, observed)
                }
            })
        })
        .map(|obligation| obligation.obligation_id.clone())
        .collect();
    ObservedAcceptance {
        satisfied_criteria,
        observed_evidence,
        unresolved_obligation_ids,
    }
}

fn observed_write_matches_read(write: &ObservedEvidence, read: &ObservedEvidence) -> bool {
    match (&write.target, &read.target) {
        (
            EvidenceTargetIdentity::Workspace { scope: write },
            EvidenceTargetIdentity::Workspace { scope: read },
        ) => {
            write.access_mode == WorkspaceAccessMode::Write
                && write.coverage == EvidenceCoverageKind::WriteEffect
                && read.access_mode == WorkspaceAccessMode::Read
                && read.coverage == EvidenceCoverageKind::ExactContent
                && write.path.workspace_id == read.path.workspace_id
                && write.path.repository_id == read.path.repository_id
                && write.path.workspace_relative_path == read.path.workspace_relative_path
                && write.path.observed_revision_or_digest.is_some()
                && write.path.observed_revision_or_digest == read.path.observed_revision_or_digest
        }
        _ => false,
    }
}

#[must_use]
pub fn observed_evidence_satisfies(
    required: &EvidenceObligation,
    observed: &ObservedEvidence,
) -> bool {
    // Typed identity alone is not an execution receipt. Sequence and tool
    // provenance are minted only at the Runtime-owned ToolHost boundary; a
    // deserialized/model-authored target with default metadata cannot satisfy
    // an obligation. Exact content and writes additionally require the
    // immutable digest captured by that boundary.
    if observed.observed_at_sequence == 0 || observed.tool_name.trim().is_empty() {
        return false;
    }
    if matches!(
        &observed.target,
        EvidenceTargetIdentity::Workspace { scope }
            if matches!(
                scope.coverage,
                EvidenceCoverageKind::ExactContent | EvidenceCoverageKind::WriteEffect
            ) && scope.path.observed_revision_or_digest.is_none()
    ) {
        return false;
    }
    match (&required.target, &observed.target) {
        (
            EvidenceTargetIdentity::Workspace { scope: required },
            EvidenceTargetIdentity::Workspace { scope: observed },
        ) => observed_scope_satisfies(required, observed),
        (
            EvidenceTargetIdentity::Network { endpoint: required },
            EvidenceTargetIdentity::Network { endpoint: observed },
        ) => required == "*" || required == observed,
        _ => false,
    }
}

/// Directional coverage check for acceptance. It intentionally does not use a
/// symmetric prefix rule.
#[must_use]
pub fn observed_scope_satisfies(
    required: &WorkspaceScopeIdentity,
    observed: &WorkspaceScopeIdentity,
) -> bool {
    if required.access_mode != observed.access_mode
        || required.path.workspace_id != observed.path.workspace_id
        || required.path.repository_id != observed.path.repository_id
        || required
            .path
            .observed_revision_or_digest
            .as_ref()
            .is_some_and(|required_digest| {
                observed.path.observed_revision_or_digest.as_ref() != Some(required_digest)
            })
    {
        return false;
    }
    let required_path = &required.path.workspace_relative_path;
    let observed_path = &observed.path.workspace_relative_path;
    match required.coverage {
        EvidenceCoverageKind::ExactContent => {
            observed.coverage == EvidenceCoverageKind::ExactContent
                && required_path == observed_path
        }
        EvidenceCoverageKind::ScopedContent => {
            matches!(
                observed.coverage,
                EvidenceCoverageKind::ExactContent | EvidenceCoverageKind::RecursiveContent
            ) && path_contains(required_path, observed_path)
        }
        EvidenceCoverageKind::DirectoryListing => {
            observed.coverage == EvidenceCoverageKind::DirectoryListing
                && required_path == observed_path
        }
        EvidenceCoverageKind::RecursiveContent => {
            observed.coverage == EvidenceCoverageKind::RecursiveContent
                && path_contains(observed_path, required_path)
        }
        EvidenceCoverageKind::GlobDiscovery => {
            observed.coverage == EvidenceCoverageKind::GlobDiscovery
                && required_path == observed_path
        }
        EvidenceCoverageKind::WriteEffect => {
            observed.coverage == EvidenceCoverageKind::WriteEffect
                && path_contains(required_path, observed_path)
        }
    }
}

fn is_repository_root(path: &Path) -> bool {
    path.join(".git").exists()
        || path.join("Cargo.toml").is_file()
        || path.join("package.json").is_file()
}

fn repository_id(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("workspace")
        .to_string()
}

fn normalize_relative(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    (!normalized.as_os_str().is_empty()).then_some(normalized)
}

fn strip_repository_prefix(path: &Path, repository: &RepositoryBinding) -> String {
    if repository.workspace_prefix.is_empty() {
        return path_to_slash(path);
    }
    let relative = path
        .strip_prefix(&repository.workspace_prefix)
        .map(path_to_slash)
        .unwrap_or_else(|_| path_to_slash(path));
    if relative.is_empty() {
        ".".to_string()
    } else {
        relative
    }
}

fn path_to_slash(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn path_contains(parent: &str, child: &str) -> bool {
    parent == "."
        || parent == child
        || child
            .strip_prefix(parent)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn obligation_id(value: &str) -> String {
    format!("evidence-obligation:{:x}", Sha256::digest(value.as_bytes()))
}

fn obligation_kind(raw_scope: &str) -> EvidenceObligationKind {
    match raw_scope.split_once(':').map(|(prefix, _)| prefix) {
        Some("list") => EvidenceObligationKind::DirectoryListing,
        Some("recursive") => EvidenceObligationKind::RecursiveScan,
        Some("glob") => EvidenceObligationKind::GlobDiscovery,
        Some("write") => EvidenceObligationKind::WriteEffect,
        Some("verify_after_write") => EvidenceObligationKind::VerifyAfterWrite,
        Some("verify_upstream_change") => EvidenceObligationKind::VerifyUpstreamChange,
        Some("network") => EvidenceObligationKind::NetworkEvidence,
        _ => EvidenceObligationKind::ContentRead,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository(root: &Path, name: &str, relative: &str) {
        let repository = root.join(name);
        std::fs::create_dir_all(repository.join(".git")).unwrap();
        let file = repository.join(relative);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, name).unwrap();
    }

    #[test]
    fn repo_relative_and_workspace_relative_paths_share_one_identity() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "crates/runtime/src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let repo_relative = resolver
            .resolve_existing("crates/runtime/src/lib.rs")
            .unwrap();
        let workspace_relative = resolver
            .resolve_existing("cowd/crates/runtime/src/lib.rs")
            .unwrap();
        assert_eq!(repo_relative, workspace_relative);
        assert_eq!(
            repo_relative.repository_relative_path,
            "crates/runtime/src/lib.rs"
        );
    }

    #[test]
    fn planned_repo_relative_path_is_bound_under_the_only_nested_repository() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let planned = resolver.resolve_planned_file("evidence/report.md").unwrap();
        assert_eq!(planned.workspace_relative_path, "cowd/evidence/report.md");
        assert_eq!(planned.repository_relative_path, "evidence/report.md");
    }

    #[test]
    fn duplicate_repo_relative_path_is_ambiguous_not_first_match() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "crates/shared/src/lib.rs");
        repository(root.path(), "other", "crates/shared/src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        assert!(matches!(
            resolver.resolve_existing("crates/shared/src/lib.rs"),
            Err(WorkspacePathIdentityError::Ambiguous { candidates, .. }) if candidates.len() == 2
        ));
        assert!(matches!(
            resolver
                .compile_obligation_or_unresolved("read:crates/shared/src/lib.rs")
                .target,
            EvidenceTargetIdentity::AmbiguousWorkspace { candidates, .. }
                if candidates.len() == 2
        ));
    }

    #[test]
    fn directory_listing_never_satisfies_exact_content() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "crates/runtime/src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let file = resolver
            .resolve_existing("cowd/crates/runtime/src/lib.rs")
            .unwrap();
        let required = WorkspaceScopeIdentity {
            access_mode: WorkspaceAccessMode::Read,
            path: file.clone(),
            coverage: EvidenceCoverageKind::ExactContent,
        };
        let observed = WorkspaceScopeIdentity {
            access_mode: WorkspaceAccessMode::Read,
            path: file,
            coverage: EvidenceCoverageKind::DirectoryListing,
        };
        assert!(!observed_scope_satisfies(&required, &observed));
    }

    #[test]
    fn receipt_identity_survives_file_removal() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let required = resolver.compile_obligation("read:cowd/src/lib.rs").unwrap();
        let observed = resolver
            .observe_tool_scope(
                "read_file",
                "read:cowd/src/lib.rs",
                Some("sha256:before"),
                1,
            )
            .unwrap();
        std::fs::remove_file(root.path().join("cowd/src/lib.rs")).unwrap();
        assert!(observed_evidence_satisfies(&required, &observed));
    }

    #[test]
    fn typed_target_without_runtime_attestation_metadata_is_not_a_receipt() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let required = resolver.compile_obligation("read:cowd/src/lib.rs").unwrap();
        let observed = resolver
            .observe_scope("read:cowd/src/lib.rs", Some("sha256:content"))
            .unwrap();
        assert!(!observed_evidence_satisfies(&required, &observed));
    }

    #[test]
    fn post_write_verification_requires_a_later_exact_read() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let required = resolver.compile_required_acceptance(
            &["verified".to_string()],
            &["verify_after_write:cowd/src/lib.rs".to_string()],
        );
        let read_before = resolver
            .observe_tool_scope("read_file", "read:cowd/src/lib.rs", Some("before"), 1)
            .unwrap();
        let write = resolver
            .observe_tool_scope("write_file", "write:cowd/src/lib.rs", Some("after"), 2)
            .unwrap();
        let early = evaluate_observed_acceptance(
            &required,
            Vec::new(),
            vec![read_before.clone(), write.clone()],
        );
        assert_eq!(early.unresolved_obligation_ids.len(), 1);
        let wrong_read = resolver
            .observe_tool_scope("read_file", "read:cowd/src/lib.rs", Some("changed"), 3)
            .unwrap();
        let changed_after_write =
            evaluate_observed_acceptance(&required, Vec::new(), vec![write.clone(), wrong_read]);
        assert_eq!(changed_after_write.unresolved_obligation_ids.len(), 1);
        let read_after = resolver
            .observe_tool_scope("read_file", "read:cowd/src/lib.rs", Some("after"), 3)
            .unwrap();
        let complete = evaluate_observed_acceptance(
            &required,
            vec!["verified".to_string()],
            vec![read_before, write, read_after],
        );
        assert!(complete.unresolved_obligation_ids.is_empty());
    }

    #[test]
    fn novelty_is_digest_stable_and_excludes_retained_replay() {
        let root = tempfile::tempdir().unwrap();
        repository(root.path(), "cowd", "src/lib.rs");
        let resolver = WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let prior = resolver
            .observe_tool_scope("read_file", "read:cowd/src/lib.rs", Some("digest-a"), 1)
            .unwrap();
        let same_fact_new_sequence = resolver
            .observe_tool_scope("read_file", "read:cowd/src/lib.rs", Some("digest-a"), 2)
            .unwrap();
        let changed_digest = resolver
            .observe_tool_scope("read_file", "read:cowd/src/lib.rs", Some("digest-b"), 3)
            .unwrap();
        let mut retained_changed_digest = changed_digest.clone();
        retained_changed_digest.observed_at_sequence = 4;
        retained_changed_digest.provenance =
            harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;

        assert!(fresh_novel_observed_evidence_fingerprints(
            std::slice::from_ref(&prior),
            std::slice::from_ref(&same_fact_new_sequence),
        )
        .is_empty());
        assert_eq!(
            fresh_novel_observed_evidence_fingerprints(
                std::slice::from_ref(&prior),
                std::slice::from_ref(&changed_digest),
            ),
            BTreeSet::from([observed_evidence_fingerprint(&changed_digest)])
        );
        assert!(fresh_novel_observed_evidence_fingerprints(
            std::slice::from_ref(&prior),
            std::slice::from_ref(&retained_changed_digest),
        )
        .is_empty());
        assert_eq!(
            observed_evidence_fingerprint(&changed_digest),
            observed_evidence_fingerprint(&retained_changed_digest),
            "replay provenance and wave sequence must not manufacture novelty"
        );
    }
}
