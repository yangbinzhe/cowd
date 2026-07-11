use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use fs2::FileExt;

const STORE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeOwnership {
    /// The path pre-existed or is controlled by the user. It is never deleted.
    UserManaged,
    /// Runtime provisioned the path. Reclaim still releases metadata only;
    /// cleanup belongs to a separately authorized maintenance operation.
    RuntimeManaged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorktreeLeaseStatus {
    Active,
    Released,
    Reclaimed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeLeaseRequest {
    pub workspace_id: String,
    pub task_id: String,
    pub owner_id: String,
    pub path: PathBuf,
    pub ownership: WorktreeOwnership,
    pub ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeLeaseRecord {
    pub lease_id: Uuid,
    pub workspace_id: String,
    pub task_id: String,
    pub owner_id: String,
    pub path: PathBuf,
    pub ownership: WorktreeOwnership,
    pub status: WorktreeLeaseStatus,
    pub created_at_ms: u64,
    pub heartbeat_at_ms: u64,
    pub expires_at_ms: u64,
    pub released_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReclaimedWorktreeLease {
    pub record: WorktreeLeaseRecord,
    /// Always true: reclaiming a lease never removes the worktree path.
    pub path_preserved: bool,
}

#[derive(Debug, Error)]
pub enum WorktreeLeaseError {
    #[error("{0} must not be empty")]
    EmptyField(&'static str),
    #[error("worktree lease TTL must be greater than zero")]
    InvalidTtl,
    #[error("worktree path must be absolute: {0}")]
    RelativePath(PathBuf),
    #[error("worktree path is already leased by {lease_id}: {path}")]
    PathAlreadyLeased { path: PathBuf, lease_id: Uuid },
    #[error("worktree lease not found: {0}")]
    NotFound(Uuid),
    #[error("worktree lease is not active: {0}")]
    NotActive(Uuid),
    #[error("worktree lease owner mismatch for {lease_id}")]
    OwnerMismatch { lease_id: Uuid },
    #[error("worktree lease store schema {found} is unsupported (expected {expected})")]
    UnsupportedSchema { found: u32, expected: u32 },
    #[error("worktree lease store lock is poisoned")]
    Poisoned,
    #[error("worktree lease store is invalid: {0}")]
    Corrupt(String),
    #[error("worktree lease I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct DurableStore {
    schema_version: u32,
    leases: Vec<WorktreeLeaseRecord>,
}

#[derive(Debug, Default)]
struct State {
    leases: HashMap<Uuid, WorktreeLeaseRecord>,
}

#[derive(Debug)]
struct Shared {
    state_path: PathBuf,
    lock_path: PathBuf,
    state: Mutex<State>,
}

/// Durable worktree occupancy records. This manager does not invoke Git and
/// never deletes, prunes, or mutates a worktree directory.
#[derive(Clone, Debug)]
pub struct WorktreeLeaseManager {
    shared: Arc<Shared>,
}

impl WorktreeLeaseManager {
    pub fn open(state_path: impl Into<PathBuf>) -> Result<Self, WorktreeLeaseError> {
        let state_path = state_path.into();
        let lock_path = state_path.with_extension("lock");
        let state = with_store_lock(&lock_path, || load_state(&state_path))?;
        Ok(Self {
            shared: Arc::new(Shared {
                state_path,
                lock_path,
                state: Mutex::new(state),
            }),
        })
    }

    pub fn acquire(
        &self,
        request: WorktreeLeaseRequest,
    ) -> Result<WorktreeLease, WorktreeLeaseError> {
        validate_request(&request)?;
        let now = now_ms();
        let expires_at_ms = now.saturating_add(duration_millis(request.ttl));
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| WorktreeLeaseError::Poisoned)?;
        let record = with_store_lock(&self.shared.lock_path, || {
            *guard = load_state(&self.shared.state_path)?;
            for record in guard.leases.values_mut() {
                if record.status == WorktreeLeaseStatus::Active && record.expires_at_ms <= now {
                    record.status = WorktreeLeaseStatus::Reclaimed;
                    record.released_at_ms = Some(now);
                }
            }
            if let Some(existing) = guard.leases.values().find(|record| {
                record.status == WorktreeLeaseStatus::Active
                    && same_path(&record.path, &request.path)
            }) {
                return Err(WorktreeLeaseError::PathAlreadyLeased {
                    path: request.path.clone(),
                    lease_id: existing.lease_id,
                });
            }
            let record = WorktreeLeaseRecord {
                lease_id: Uuid::new_v4(),
                workspace_id: request.workspace_id.clone(),
                task_id: request.task_id.clone(),
                owner_id: request.owner_id.clone(),
                path: request.path.clone(),
                ownership: request.ownership,
                status: WorktreeLeaseStatus::Active,
                created_at_ms: now,
                heartbeat_at_ms: now,
                expires_at_ms,
                released_at_ms: None,
            };
            guard.leases.insert(record.lease_id, record.clone());
            persist_state(&self.shared.state_path, &guard)?;
            Ok(record)
        })?;
        Ok(WorktreeLease {
            manager: self.clone(),
            record,
        })
    }

    pub fn heartbeat(
        &self,
        lease_id: Uuid,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<WorktreeLeaseRecord, WorktreeLeaseError> {
        if ttl.is_zero() {
            return Err(WorktreeLeaseError::InvalidTtl);
        }
        let now = now_ms();
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| WorktreeLeaseError::Poisoned)?;
        with_store_lock(&self.shared.lock_path, || {
            *guard = load_state(&self.shared.state_path)?;
            let record = guard
                .leases
                .get_mut(&lease_id)
                .ok_or(WorktreeLeaseError::NotFound(lease_id))?;
            ensure_active_owner(record, owner_id)?;
            record.heartbeat_at_ms = now;
            record.expires_at_ms = now.saturating_add(duration_millis(ttl));
            let updated = record.clone();
            persist_state(&self.shared.state_path, &guard)?;
            Ok(updated)
        })
    }

    pub fn release(
        &self,
        lease_id: Uuid,
        owner_id: &str,
    ) -> Result<WorktreeLeaseRecord, WorktreeLeaseError> {
        let now = now_ms();
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| WorktreeLeaseError::Poisoned)?;
        with_store_lock(&self.shared.lock_path, || {
            *guard = load_state(&self.shared.state_path)?;
            let record = guard
                .leases
                .get_mut(&lease_id)
                .ok_or(WorktreeLeaseError::NotFound(lease_id))?;
            ensure_active_owner(record, owner_id)?;
            record.status = WorktreeLeaseStatus::Released;
            record.released_at_ms = Some(now);
            let updated = record.clone();
            persist_state(&self.shared.state_path, &guard)?;
            Ok(updated)
        })
    }

    /// Reclaims only expired lease ownership. The corresponding filesystem
    /// path is preserved for both user- and runtime-managed worktrees.
    pub fn reclaim_expired(
        &self,
        at_ms: u64,
    ) -> Result<Vec<ReclaimedWorktreeLease>, WorktreeLeaseError> {
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| WorktreeLeaseError::Poisoned)?;
        with_store_lock(&self.shared.lock_path, || {
            *guard = load_state(&self.shared.state_path)?;
            let mut reclaimed = Vec::new();
            for record in guard.leases.values_mut() {
                if record.status == WorktreeLeaseStatus::Active && record.expires_at_ms <= at_ms {
                    record.status = WorktreeLeaseStatus::Reclaimed;
                    record.released_at_ms = Some(at_ms);
                    reclaimed.push(ReclaimedWorktreeLease {
                        record: record.clone(),
                        path_preserved: true,
                    });
                }
            }
            if !reclaimed.is_empty() {
                persist_state(&self.shared.state_path, &guard)?;
            }
            reclaimed.sort_by_key(|item| item.record.created_at_ms);
            Ok(reclaimed)
        })
    }

    pub fn record(
        &self,
        lease_id: Uuid,
    ) -> Result<Option<WorktreeLeaseRecord>, WorktreeLeaseError> {
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| WorktreeLeaseError::Poisoned)?;
        with_store_lock(&self.shared.lock_path, || {
            *guard = load_state(&self.shared.state_path)?;
            Ok(guard.leases.get(&lease_id).cloned())
        })
    }

    pub fn active_records(&self) -> Result<Vec<WorktreeLeaseRecord>, WorktreeLeaseError> {
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| WorktreeLeaseError::Poisoned)?;
        with_store_lock(&self.shared.lock_path, || {
            *guard = load_state(&self.shared.state_path)?;
            let mut records = guard
                .leases
                .values()
                .filter(|record| record.status == WorktreeLeaseStatus::Active)
                .cloned()
                .collect::<Vec<_>>();
            records.sort_by_key(|record| record.created_at_ms);
            Ok(records)
        })
    }
}

/// A durable lease handle. Dropping it does not release the lease: a process
/// crash must leave the record available for TTL-based recovery.
#[derive(Clone, Debug)]
pub struct WorktreeLease {
    manager: WorktreeLeaseManager,
    record: WorktreeLeaseRecord,
}

impl WorktreeLease {
    pub fn record(&self) -> &WorktreeLeaseRecord {
        &self.record
    }

    pub fn heartbeat(&mut self, ttl: Duration) -> Result<(), WorktreeLeaseError> {
        self.record = self
            .manager
            .heartbeat(self.record.lease_id, &self.record.owner_id, ttl)?;
        Ok(())
    }

    pub fn release(self) -> Result<WorktreeLeaseRecord, WorktreeLeaseError> {
        self.manager
            .release(self.record.lease_id, &self.record.owner_id)
    }
}

fn validate_request(request: &WorktreeLeaseRequest) -> Result<(), WorktreeLeaseError> {
    for (field, value) in [
        ("workspace_id", request.workspace_id.as_str()),
        ("task_id", request.task_id.as_str()),
        ("owner_id", request.owner_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(WorktreeLeaseError::EmptyField(field));
        }
    }
    if request.ttl.is_zero() {
        return Err(WorktreeLeaseError::InvalidTtl);
    }
    if !request.path.is_absolute() {
        return Err(WorktreeLeaseError::RelativePath(request.path.clone()));
    }
    Ok(())
}

fn ensure_active_owner(
    record: &WorktreeLeaseRecord,
    owner_id: &str,
) -> Result<(), WorktreeLeaseError> {
    if record.status != WorktreeLeaseStatus::Active {
        return Err(WorktreeLeaseError::NotActive(record.lease_id));
    }
    if record.owner_id != owner_id {
        return Err(WorktreeLeaseError::OwnerMismatch {
            lease_id: record.lease_id,
        });
    }
    Ok(())
}

fn load_state(path: &Path) -> Result<State, WorktreeLeaseError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(State::default()),
        Err(source) => {
            return Err(WorktreeLeaseError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let store: DurableStore = serde_json::from_slice(&bytes)
        .map_err(|error| WorktreeLeaseError::Corrupt(error.to_string()))?;
    if store.schema_version != STORE_SCHEMA_VERSION {
        return Err(WorktreeLeaseError::UnsupportedSchema {
            found: store.schema_version,
            expected: STORE_SCHEMA_VERSION,
        });
    }
    let mut leases = HashMap::new();
    for record in store.leases {
        if leases.insert(record.lease_id, record).is_some() {
            return Err(WorktreeLeaseError::Corrupt(
                "duplicate lease id in durable store".to_string(),
            ));
        }
    }
    Ok(State { leases })
}

fn with_store_lock<T>(
    lock_path: &Path,
    operation: impl FnOnce() -> Result<T, WorktreeLeaseError>,
) -> Result<T, WorktreeLeaseError> {
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(|source| WorktreeLeaseError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|source| WorktreeLeaseError::Io {
            path: lock_path.to_path_buf(),
            source,
        })?;
    lock.lock_exclusive()
        .map_err(|source| WorktreeLeaseError::Io {
            path: lock_path.to_path_buf(),
            source,
        })?;
    let result = operation();
    FileExt::unlock(&lock).map_err(|source| WorktreeLeaseError::Io {
        path: lock_path.to_path_buf(),
        source,
    })?;
    result
}

fn persist_state(path: &Path, state: &State) -> Result<(), WorktreeLeaseError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| WorktreeLeaseError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let mut leases = state.leases.values().cloned().collect::<Vec<_>>();
    leases.sort_by_key(|record| record.created_at_ms);
    let bytes = serde_json::to_vec_pretty(&DurableStore {
        schema_version: STORE_SCHEMA_VERSION,
        leases,
    })
    .map_err(|error| WorktreeLeaseError::Corrupt(error.to_string()))?;
    let temporary = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
    let temporary_file = fs::File::create(&temporary).map_err(|source| WorktreeLeaseError::Io {
        path: temporary.clone(),
        source,
    })?;
    {
        use std::io::Write;
        let mut writer = std::io::BufWriter::new(&temporary_file);
        writer
            .write_all(&bytes)
            .and_then(|_| writer.flush())
            .map_err(|source| WorktreeLeaseError::Io {
                path: temporary.clone(),
                source,
            })?;
    }
    temporary_file
        .sync_all()
        .map_err(|source| WorktreeLeaseError::Io {
            path: temporary.clone(),
            source,
        })?;
    fs::rename(&temporary, path).map_err(|source| WorktreeLeaseError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| WorktreeLeaseError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
    }
    Ok(())
}

fn same_path(left: &Path, right: &Path) -> bool {
    lexical_path(left) == lexical_path(right)
}

fn lexical_path(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                result.pop();
            }
            other => result.push(other.as_os_str()),
        }
    }
    result
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(path: PathBuf) -> WorktreeLeaseRequest {
        WorktreeLeaseRequest {
            workspace_id: "workspace".to_string(),
            task_id: "task".to_string(),
            owner_id: "runner".to_string(),
            path,
            ownership: WorktreeOwnership::UserManaged,
            ttl: Duration::from_secs(60),
        }
    }

    #[test]
    fn durable_records_survive_restart_and_reclaim_preserves_path() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("leases.json");
        let user_worktree = temp.path().join("user-worktree");
        fs::create_dir(&user_worktree).unwrap();

        let manager = WorktreeLeaseManager::open(&state_path).unwrap();
        let lease = manager.acquire(request(user_worktree.clone())).unwrap();
        let lease_id = lease.record().lease_id;
        let expiry = lease.record().expires_at_ms;
        drop((lease, manager));

        let restarted = WorktreeLeaseManager::open(&state_path).unwrap();
        assert_eq!(restarted.active_records().unwrap().len(), 1);
        let reclaimed = restarted.reclaim_expired(expiry).unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].record.lease_id, lease_id);
        assert!(reclaimed[0].path_preserved);
        assert!(user_worktree.exists());
    }

    #[test]
    fn active_path_cannot_be_leased_twice() {
        let temp = tempfile::tempdir().unwrap();
        let manager = WorktreeLeaseManager::open(temp.path().join("leases.json")).unwrap();
        let path = temp.path().join("worktree");
        let _first = manager.acquire(request(path.clone())).unwrap();
        assert!(matches!(
            manager.acquire(request(path)),
            Err(WorktreeLeaseError::PathAlreadyLeased { .. })
        ));
    }

    #[test]
    fn independently_opened_managers_reload_under_process_lock() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("leases.json");
        let first = WorktreeLeaseManager::open(&state_path).unwrap();
        let second = WorktreeLeaseManager::open(&state_path).unwrap();
        let path = temp.path().join("worktree");

        let lease = first.acquire(request(path.clone())).unwrap();
        assert!(matches!(
            second.acquire(request(path)),
            Err(WorktreeLeaseError::PathAlreadyLeased { .. })
        ));
        assert_eq!(second.active_records().unwrap().len(), 1);

        lease.release().unwrap();
        assert!(second.active_records().unwrap().is_empty());
    }

    #[test]
    fn drop_does_not_erase_crash_recovery_record() {
        let temp = tempfile::tempdir().unwrap();
        let manager = WorktreeLeaseManager::open(temp.path().join("leases.json")).unwrap();
        let lease = manager
            .acquire(request(temp.path().join("worktree")))
            .unwrap();
        let id = lease.record().lease_id;
        drop(lease);
        assert_eq!(
            manager.record(id).unwrap().unwrap().status,
            WorktreeLeaseStatus::Active
        );
    }

    #[test]
    fn acquire_reclaims_expired_record_before_conflict_check() {
        let temp = tempfile::tempdir().unwrap();
        let state_path = temp.path().join("leases.json");
        let path = temp.path().join("worktree");
        let first = WorktreeLeaseManager::open(&state_path).unwrap();
        let mut expiring = request(path.clone());
        expiring.ttl = Duration::from_millis(1);
        let old = first.acquire(expiring).unwrap();
        std::thread::sleep(Duration::from_millis(5));

        let second = WorktreeLeaseManager::open(&state_path).unwrap();
        let replacement = second.acquire(request(path)).unwrap();

        assert_ne!(old.record().lease_id, replacement.record().lease_id);
        assert_eq!(
            second
                .record(old.record().lease_id)
                .unwrap()
                .unwrap()
                .status,
            WorktreeLeaseStatus::Reclaimed
        );
    }
}
