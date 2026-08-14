use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
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
    Workspace {
        workspace_id: String,
    },
    WorkspaceObject {
        identity: harness_contract::context::WorkspacePathIdentity,
    },
    Resource {
        namespace: String,
        key: String,
    },
}

impl ScopedResource {
    pub fn workspace(workspace_id: impl Into<String>) -> Result<Self, ScopeLockError> {
        let workspace_id = nonempty("workspace_id", workspace_id.into())?;
        Ok(Self::Workspace { workspace_id })
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

    #[must_use]
    pub fn workspace_object(identity: harness_contract::context::WorkspacePathIdentity) -> Self {
        Self::WorkspaceObject { identity }
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
                Self::WorkspaceObject { identity: left },
                Self::WorkspaceObject { identity: right },
            ) => {
                left.workspace_id == right.workspace_id
                    && left.repository_id == right.repository_id
                    && (path_is_prefix(
                        &left.repository_relative_path,
                        &right.repository_relative_path,
                    ) || path_is_prefix(
                        &right.repository_relative_path,
                        &left.repository_relative_path,
                    ))
            }
            (Self::Workspace { workspace_id }, Self::WorkspaceObject { identity })
            | (Self::WorkspaceObject { identity }, Self::Workspace { workspace_id }) => {
                workspace_id == &identity.workspace_id
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
        match &request.scope {
            ScopedResource::Workspace { workspace_id } => {
                // Workspace-wide read blocks object writers but coexists with
                // object readers; workspace-wide write blocks every object.
                merge_process_target(
                    &mut targets,
                    format!("workspace-all:{workspace_id}"),
                    request.mode,
                );
                if request.mode == ScopeLockMode::Read {
                    merge_process_target(
                        &mut targets,
                        format!("workspace-writers:{workspace_id}"),
                        ScopeLockMode::Write,
                    );
                }
            }
            ScopedResource::WorkspaceObject { identity } => {
                merge_process_target(
                    &mut targets,
                    format!("workspace-all:{}", identity.workspace_id),
                    ScopeLockMode::Read,
                );
                if request.mode == ScopeLockMode::Write {
                    merge_process_target(
                        &mut targets,
                        format!("workspace-writers:{}", identity.workspace_id),
                        ScopeLockMode::Read,
                    );
                }
                // The process-shared tier uses a conservative repository
                // zone. It preserves parent/descendant exclusion without
                // collapsing unrelated top-level zones or repositories into
                // one workspace mutex. The in-process tier above remains
                // exact and fully hierarchical.
                let zone = identity
                    .repository_relative_path
                    .split('/')
                    .next()
                    .filter(|part| !part.is_empty())
                    .unwrap_or(".");
                merge_process_target(
                    &mut targets,
                    format!(
                        "workspace-object:{}:{}:{zone}",
                        identity.workspace_id, identity.repository_id
                    ),
                    request.mode,
                );
            }
            ScopedResource::Resource { namespace, key } => {
                merge_process_target(
                    &mut targets,
                    format!("resource:{namespace}:{key}"),
                    request.mode,
                );
            }
        }
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

fn merge_process_target(
    targets: &mut HashMap<String, ScopeLockMode>,
    key: String,
    requested: ScopeLockMode,
) {
    targets
        .entry(key)
        .and_modify(|mode| {
            if requested == ScopeLockMode::Write {
                *mode = ScopeLockMode::Write;
            }
        })
        .or_insert(requested);
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
        if let Some(existing) = normalized
            .iter_mut()
            .find(|item| item.scope == request.scope)
        {
            // One atomic execution node may contain role-local read and write
            // leases for the same path. The container lock must acquire the
            // strongest mode once; child role permissions remain unchanged.
            if request.mode == ScopeLockMode::Write {
                existing.mode = ScopeLockMode::Write;
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

    fn workspace_object(path: &str) -> ScopedResource {
        workspace_object_in("repo", path)
    }

    fn workspace_object_in(repository_id: &str, path: &str) -> ScopedResource {
        ScopedResource::workspace_object(harness_contract::context::WorkspacePathIdentity {
            workspace_id: "workspace".to_string(),
            repository_id: repository_id.to_string(),
            workspace_relative_path: path.to_string(),
            repository_relative_path: path.to_string(),
            object_kind: harness_contract::context::WorkspaceObjectKind::File,
            observed_revision_or_digest: None,
        })
    }

    #[tokio::test]
    async fn duplicate_read_and_write_scope_acquires_one_strongest_lock() {
        let manager = ScopeLockManager::new();
        let scope = workspace_object("evidence/report.html");
        let lease = manager
            .acquire(
                [
                    request(scope.clone(), ScopeLockMode::Read),
                    request(scope, ScopeLockMode::Write),
                ],
                None,
            )
            .await
            .expect("read and write aggregate to one write lock");
        assert_eq!(lease.requests().len(), 1);
        assert_eq!(lease.requests()[0].mode, ScopeLockMode::Write);
    }

    #[tokio::test]
    async fn same_file_write_is_serialized() {
        let manager = ScopeLockManager::new();
        let scope = workspace_object("src/lib.rs");
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
                    workspace_object("src/lib.rs"),
                    ScopeLockMode::Write,
                )],
                None,
            )
            .await
            .unwrap();
        let right = manager
            .acquire(
                [request(
                    workspace_object("tests/e2e.rs"),
                    ScopeLockMode::Write,
                )],
                Some(Duration::from_millis(10)),
            )
            .await;
        assert!(right.is_ok());
    }

    #[tokio::test]
    async fn parent_and_descendant_aliases_conflict_but_other_repositories_do_not() {
        let manager = ScopeLockManager::new();
        let _parent = manager
            .acquire(
                [request(workspace_object("src"), ScopeLockMode::Write)],
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            manager
                .acquire(
                    [request(workspace_object("src/lib.rs"), ScopeLockMode::Read,)],
                    Some(Duration::from_millis(10)),
                )
                .await,
            Err(ScopeLockError::TimedOut { .. })
        ));
        assert!(manager
            .acquire(
                [request(
                    workspace_object_in("other-repo", "src/lib.rs"),
                    ScopeLockMode::Write,
                )],
                Some(Duration::from_millis(10)),
            )
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn unrelated_scope_can_bypass_blocked_waiter() {
        let manager = ScopeLockManager::new();
        let occupied = workspace_object("src/lib.rs");
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
                    workspace_object("tests/e2e.rs"),
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
                    [request(workspace_object("README.md"), ScopeLockMode::Read,)],
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
                    workspace_object("src/lib.rs"),
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

    #[tokio::test]
    async fn persistent_managers_parallelize_unrelated_workspace_zones() {
        let temp = tempfile::tempdir().unwrap();
        let first = ScopeLockManager::persistent(temp.path()).unwrap();
        let second = ScopeLockManager::persistent(temp.path()).unwrap();
        let _held = first
            .acquire(
                [request(
                    workspace_object("src/lib.rs"),
                    ScopeLockMode::Write,
                )],
                None,
            )
            .await
            .unwrap();

        assert!(second
            .acquire(
                [request(
                    workspace_object("tests/e2e.rs"),
                    ScopeLockMode::Write,
                )],
                Some(Duration::from_millis(25)),
            )
            .await
            .is_ok());
        assert!(matches!(
            second
                .acquire(
                    [request(
                        workspace_object("src/child.rs"),
                        ScopeLockMode::Read,
                    )],
                    Some(Duration::from_millis(25)),
                )
                .await,
            Err(ScopeLockError::TimedOut { .. })
        ));
    }
}
