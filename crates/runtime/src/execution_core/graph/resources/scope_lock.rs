use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Notify;
use uuid::Uuid;

use fs2::FileExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeLockMode {
    Read,
    Write,
}

/// Hierarchical lock scope. Paths are normalized lexical paths, not
/// canonicalized filesystem paths, so acquiring a lock never touches disk.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopedResource {
    Workspace { workspace_id: String },
    File { workspace_id: String, path: String },
    Resource { namespace: String, key: String },
}

impl ScopedResource {
    pub fn workspace(workspace_id: impl Into<String>) -> Result<Self, ScopeLockError> {
        let workspace_id = nonempty("workspace_id", workspace_id.into())?;
        Ok(Self::Workspace { workspace_id })
    }

    pub fn file(
        workspace_id: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<Self, ScopeLockError> {
        let workspace_id = nonempty("workspace_id", workspace_id.into())?;
        let path = normalize_relative_path(path.as_ref())?;
        Ok(Self::File { workspace_id, path })
    }

    pub fn resource(
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Result<Self, ScopeLockError> {
        Ok(Self::Resource {
            namespace: nonempty("namespace", namespace.into())?,
            key: nonempty("key", key.into())?,
        })
    }

    fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Workspace { workspace_id: left },
                Self::Workspace {
                    workspace_id: right,
                },
            ) => left == right,
            (
                Self::Workspace { workspace_id: left },
                Self::File {
                    workspace_id: right,
                    ..
                },
            )
            | (
                Self::File {
                    workspace_id: right,
                    ..
                },
                Self::Workspace { workspace_id: left },
            ) => left == right,
            (
                Self::File {
                    workspace_id: left_workspace,
                    path: left,
                },
                Self::File {
                    workspace_id: right_workspace,
                    path: right,
                },
            ) => {
                left_workspace == right_workspace
                    && (path_is_prefix(left, right) || path_is_prefix(right, left))
            }
            (
                Self::Resource {
                    namespace: left_namespace,
                    key: left_key,
                },
                Self::Resource {
                    namespace: right_namespace,
                    key: right_key,
                },
            ) => left_namespace == right_namespace && left_key == right_key,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeLockRequest {
    pub scope: ScopedResource,
    pub mode: ScopeLockMode,
}

#[derive(Debug, Error)]
pub enum ScopeLockError {
    #[error("{0} must not be empty")]
    EmptyField(&'static str),
    #[error("scope path must be relative and must not escape its workspace: {0}")]
    InvalidPath(String),
    #[error("duplicate scope requested with incompatible modes: {0:?}")]
    DuplicateScope(ScopedResource),
    #[error("timed out waiting for scoped resources after {waited_ms} ms")]
    TimedOut { waited_ms: u64 },
    #[error("scope lock manager lock is poisoned")]
    Poisoned,
    #[error("scope lock waiter registration was lost before acquisition")]
    RegistrationLost,
    #[error("scope lock I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: Arc<std::io::Error>,
    },
}

#[derive(Clone, Debug)]
struct ActiveLock {
    requests: Vec<ScopeLockRequest>,
}

#[derive(Debug)]
struct PendingLock {
    id: Uuid,
    requests: Vec<ScopeLockRequest>,
}

#[derive(Debug, Default)]
struct State {
    active: HashMap<Uuid, ActiveLock>,
    pending: VecDeque<PendingLock>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    changed: Notify,
    lock_root: Option<PathBuf>,
}

/// Atomic, conflict-aware acquisition for one or more hierarchical scopes.
#[derive(Clone, Debug, Default)]
pub struct ScopeLockManager {
    shared: Arc<Shared>,
}

impl ScopeLockManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn persistent(lock_root: impl Into<PathBuf>) -> Result<Self, ScopeLockError> {
        let lock_root = lock_root.into();
        fs::create_dir_all(&lock_root).map_err(|source| scope_io(&lock_root, source))?;
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State::default()),
                changed: Notify::new(),
                lock_root: Some(lock_root),
            }),
        })
    }

    pub async fn acquire(
        &self,
        requests: impl IntoIterator<Item = ScopeLockRequest>,
        timeout: Option<Duration>,
    ) -> Result<ScopeLockLease, ScopeLockError> {
        let requests = normalize_requests(requests)?;
        let id = Uuid::new_v4();
        {
            let mut guard = self
                .shared
                .state
                .lock()
                .map_err(|_| ScopeLockError::Poisoned)?;
            guard.pending.push_back(PendingLock {
                id,
                requests: requests.clone(),
            });
        }
        let mut registration = PendingRegistration {
            shared: Arc::clone(&self.shared),
            id,
            active: true,
        };
        let started = Instant::now();

        loop {
            let notified = self.shared.changed.notified();
            let acquired = {
                let mut guard = self
                    .shared
                    .state
                    .lock()
                    .map_err(|_| ScopeLockError::Poisoned)?;
                let Some(pending_index) = guard.pending.iter().position(|pending| pending.id == id)
                else {
                    return Err(ScopeLockError::RegistrationLost);
                };
                let blocked_by_earlier_conflict = guard
                    .pending
                    .iter()
                    .take(pending_index)
                    .any(|pending| requests_conflict(&requests, &pending.requests));
                let has_conflict = guard
                    .active
                    .values()
                    .any(|active| requests_conflict(&requests, &active.requests));
                if !blocked_by_earlier_conflict && !has_conflict {
                    if let Some(process_locks) =
                        try_process_locks(self.shared.lock_root.as_deref(), &requests)?
                    {
                        guard.pending.remove(pending_index);
                        guard.active.insert(
                            id,
                            ActiveLock {
                                requests: requests.clone(),
                            },
                        );
                        Some(process_locks)
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            if let Some(process_locks) = acquired {
                registration.active = false;
                return Ok(ScopeLockLease {
                    shared: Arc::clone(&self.shared),
                    id,
                    requests,
                    process_locks,
                    released: false,
                });
            }

            if let Some(limit) = timeout {
                let Some(remaining) = limit.checked_sub(started.elapsed()) else {
                    return Err(ScopeLockError::TimedOut {
                        waited_ms: duration_millis(limit),
                    });
                };
                let wake = async {
                    tokio::select! {
                        () = notified => {},
                        () = tokio::time::sleep(Duration::from_millis(5)) => {},
                    }
                };
                if tokio::time::timeout(remaining, wake).await.is_err() {
                    return Err(ScopeLockError::TimedOut {
                        waited_ms: duration_millis(limit),
                    });
                }
            } else {
                tokio::select! {
                    () = notified => {},
                    () = tokio::time::sleep(Duration::from_millis(5)) => {},
                }
            }
        }
    }

    pub fn active_lease_count(&self) -> Result<usize, ScopeLockError> {
        self.shared
            .state
            .lock()
            .map(|state| state.active.len())
            .map_err(|_| ScopeLockError::Poisoned)
    }
}

pub struct ScopeLockLease {
    shared: Arc<Shared>,
    id: Uuid,
    requests: Vec<ScopeLockRequest>,
    process_locks: Vec<File>,
    released: bool,
}

impl ScopeLockLease {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn requests(&self) -> &[ScopeLockRequest] {
        &self.requests
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            guard.active.remove(&self.id);
        }
        for lock in self.process_locks.drain(..).rev() {
            let _ = FileExt::unlock(&lock);
        }
        self.released = true;
        self.shared.changed.notify_waiters();
    }
}

fn try_process_locks(
    lock_root: Option<&Path>,
    requests: &[ScopeLockRequest],
) -> Result<Option<Vec<File>>, ScopeLockError> {
    let Some(lock_root) = lock_root else {
        return Ok(Some(Vec::new()));
    };
    let mut targets = HashMap::<String, ScopeLockMode>::new();
    for request in requests {
        let key = match &request.scope {
            ScopedResource::Workspace { workspace_id }
            | ScopedResource::File { workspace_id, .. } => format!("workspace:{workspace_id}"),
            ScopedResource::Resource { namespace, key } => {
                format!("resource:{namespace}:{key}")
            }
        };
        targets
            .entry(key)
            .and_modify(|mode| {
                if request.mode == ScopeLockMode::Write {
                    *mode = ScopeLockMode::Write;
                }
            })
            .or_insert(request.mode);
    }
    let mut targets = targets.into_iter().collect::<Vec<_>>();
    targets.sort_by(|left, right| left.0.cmp(&right.0));
    let mut locks = Vec::with_capacity(targets.len());
    for (key, mode) in targets {
        let path = lock_root.join(format!("{:016x}.lock", stable_key_hash(&key)));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| scope_io(&path, source))?;
        let acquired = match mode {
            ScopeLockMode::Read => FileExt::try_lock_shared(&file),
            ScopeLockMode::Write => FileExt::try_lock_exclusive(&file),
        };
        match acquired {
            Ok(()) => locks.push(file),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                for lock in locks.iter().rev() {
                    let _ = FileExt::unlock(lock);
                }
                return Ok(None);
            }
            Err(source) => return Err(scope_io(&path, source)),
        }
    }
    Ok(Some(locks))
}

fn stable_key_hash(value: &str) -> u64 {
    model_protocol::fingerprint::stable_hash_bytes(value.as_bytes())
}

fn scope_io(path: &Path, source: std::io::Error) -> ScopeLockError {
    ScopeLockError::Io {
        path: path.to_path_buf(),
        source: Arc::new(source),
    }
}

impl Drop for ScopeLockLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

struct PendingRegistration {
    shared: Arc<Shared>,
    id: Uuid,
    active: bool,
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut guard) = self.shared.state.lock() {
            guard.pending.retain(|pending| pending.id != self.id);
        }
        self.shared.changed.notify_waiters();
    }
}

fn requests_conflict(left: &[ScopeLockRequest], right: &[ScopeLockRequest]) -> bool {
    left.iter().any(|left| {
        right.iter().any(|right| {
            left.scope.overlaps(&right.scope)
                && (left.mode == ScopeLockMode::Write || right.mode == ScopeLockMode::Write)
        })
    })
}

fn normalize_requests(
    requests: impl IntoIterator<Item = ScopeLockRequest>,
) -> Result<Vec<ScopeLockRequest>, ScopeLockError> {
    let mut normalized = Vec::<ScopeLockRequest>::new();
    for request in requests {
        if let Some(existing) = normalized.iter().find(|item| item.scope == request.scope) {
            if existing.mode != request.mode {
                return Err(ScopeLockError::DuplicateScope(request.scope));
            }
            continue;
        }
        normalized.push(request);
    }
    normalized
        .sort_by(|left, right| format!("{:?}", left.scope).cmp(&format!("{:?}", right.scope)));
    Ok(normalized)
}

fn nonempty(field: &'static str, value: String) -> Result<String, ScopeLockError> {
    if value.trim().is_empty() {
        Err(ScopeLockError::EmptyField(field))
    } else {
        Ok(value)
    }
}

fn normalize_relative_path(path: &Path) -> Result<String, ScopeLockError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(ScopeLockError::InvalidPath(path.display().to_string()));
    }
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ScopeLockError::InvalidPath(path.display().to_string()));
            }
        }
    }
    if parts.is_empty() {
        return Err(ScopeLockError::InvalidPath(path.display().to_string()));
    }
    Ok(parts.join("/"))
}

fn path_is_prefix(prefix: &str, path: &str) -> bool {
    prefix == path
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(scope: ScopedResource, mode: ScopeLockMode) -> ScopeLockRequest {
        ScopeLockRequest { scope, mode }
    }

    #[tokio::test]
    async fn same_file_write_is_serialized() {
        let manager = ScopeLockManager::new();
        let scope = ScopedResource::file("workspace", "src/lib.rs").unwrap();
        let first = manager
            .acquire([request(scope.clone(), ScopeLockMode::Write)], None)
            .await
            .unwrap();
        assert!(matches!(
            manager
                .acquire(
                    [request(scope, ScopeLockMode::Write)],
                    Some(Duration::from_millis(10)),
                )
                .await,
            Err(ScopeLockError::TimedOut { .. })
        ));
        drop(first);
    }

    #[tokio::test]
    async fn unrelated_files_run_in_parallel() {
        let manager = ScopeLockManager::new();
        let _left = manager
            .acquire(
                [request(
                    ScopedResource::file("workspace", "src/lib.rs").unwrap(),
                    ScopeLockMode::Write,
                )],
                None,
            )
            .await
            .unwrap();
        let right = manager
            .acquire(
                [request(
                    ScopedResource::file("workspace", "tests/e2e.rs").unwrap(),
                    ScopeLockMode::Write,
                )],
                Some(Duration::from_millis(10)),
            )
            .await;
        assert!(right.is_ok());
    }

    #[tokio::test]
    async fn unrelated_scope_can_bypass_blocked_waiter() {
        let manager = ScopeLockManager::new();
        let occupied = ScopedResource::file("workspace", "src/lib.rs").unwrap();
        let _active = manager
            .acquire([request(occupied.clone(), ScopeLockMode::Write)], None)
            .await
            .unwrap();

        let blocked_manager = manager.clone();
        let blocked = tokio::spawn(async move {
            blocked_manager
                .acquire([request(occupied, ScopeLockMode::Write)], None)
                .await
        });
        tokio::task::yield_now().await;

        let independent = manager
            .acquire(
                [request(
                    ScopedResource::file("workspace", "tests/e2e.rs").unwrap(),
                    ScopeLockMode::Write,
                )],
                Some(Duration::from_millis(20)),
            )
            .await;
        assert!(independent.is_ok());
        blocked.abort();
    }

    #[tokio::test]
    async fn workspace_write_conflicts_with_descendant_file() {
        let manager = ScopeLockManager::new();
        let _workspace = manager
            .acquire(
                [request(
                    ScopedResource::workspace("workspace").unwrap(),
                    ScopeLockMode::Write,
                )],
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            manager
                .acquire(
                    [request(
                        ScopedResource::file("workspace", "README.md").unwrap(),
                        ScopeLockMode::Read,
                    )],
                    Some(Duration::from_millis(10)),
                )
                .await,
            Err(ScopeLockError::TimedOut { .. })
        ));
    }

    #[tokio::test]
    async fn independently_opened_persistent_managers_hold_cross_process_scope_until_drop() {
        let temp = tempfile::tempdir().unwrap();
        let first = ScopeLockManager::persistent(temp.path()).unwrap();
        let second = ScopeLockManager::persistent(temp.path()).unwrap();
        let held = first
            .acquire(
                [request(
                    ScopedResource::file("workspace", "src/lib.rs").unwrap(),
                    ScopeLockMode::Write,
                )],
                None,
            )
            .await
            .unwrap();

        assert!(matches!(
            second
                .acquire(
                    [request(
                        ScopedResource::workspace("workspace").unwrap(),
                        ScopeLockMode::Read,
                    )],
                    Some(Duration::from_millis(25)),
                )
                .await,
            Err(ScopeLockError::TimedOut { .. })
        ));
        drop(held);
        assert!(second
            .acquire(
                [request(
                    ScopedResource::workspace("workspace").unwrap(),
                    ScopeLockMode::Read,
                )],
                Some(Duration::from_millis(50)),
            )
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn persistent_scope_locks_isolate_workspaces() {
        let temp = tempfile::tempdir().unwrap();
        let first = ScopeLockManager::persistent(temp.path()).unwrap();
        let second = ScopeLockManager::persistent(temp.path()).unwrap();
        let _held = first
            .acquire(
                [request(
                    ScopedResource::workspace("left").unwrap(),
                    ScopeLockMode::Write,
                )],
                None,
            )
            .await
            .unwrap();
        assert!(second
            .acquire(
                [request(
                    ScopedResource::workspace("right").unwrap(),
                    ScopeLockMode::Write,
                )],
                Some(Duration::from_millis(25)),
            )
            .await
            .is_ok());
    }

    #[test]
    fn escaping_paths_are_rejected() {
        assert!(matches!(
            ScopedResource::file("workspace", "../secret"),
            Err(ScopeLockError::InvalidPath(_))
        ));
    }
}
