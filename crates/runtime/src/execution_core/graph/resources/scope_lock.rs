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

#[derive(Debug)]
struct Shared {
    owner_generation: Uuid,
    state: Mutex<State>,
    changed: Notify,
    lock_root: Option<PathBuf>,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            owner_generation: Uuid::new_v4(),
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            lock_root: None,
        }
    }
}

/// Atomic, conflict-aware acquisition for one or more hierarchical scopes.
#[derive(Clone, Debug, Default)]
pub struct ScopeLockManager {
    shared: Arc<Shared>,
}

pub(crate) struct BindingOwnerFence {
    file: File,
    key: String,
    recovery: bool,
    released: bool,
    failed: bool,
}

pub(crate) struct ReleasedBindingLeases<'owner> {
    owner: &'owner BindingOwnerFence,
    closure: super::super::events::ExecutionBindingLeaseClosure,
    sources: Vec<crate::ExpectedStreamRevision>,
}

impl ReleasedBindingLeases<'_> {
    pub(crate) fn sources(&self) -> &[crate::ExpectedStreamRevision] {
        &self.sources
    }
    pub(crate) fn closure(&self) -> &super::super::events::ExecutionBindingLeaseClosure {
        &self.closure
    }

    pub(crate) fn authentic_for(
        &self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
    ) -> bool {
        self.owner.released
            && !self.owner.failed
            && self.owner.key
                == super::super::events::ExecutionBindingLeaseClosure::fence_key(
                    graph,
                    &self.closure.node_id,
                    &self.closure.binding,
                )
            && self.owner.file.metadata().is_ok()
    }
}

impl BindingOwnerFence {
    fn matches(
        &self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
        node: &str,
        binding: &super::super::events::ExecutionNodeBinding,
    ) -> bool {
        self.key
            == super::super::events::ExecutionBindingLeaseClosure::fence_key(graph, node, binding)
    }

    fn proof<'owner>(
        &'owner self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
        node: &str,
        binding: &super::super::events::ExecutionNodeBinding,
        position: (u64, u64, u32),
    ) -> ReleasedBindingLeases<'owner> {
        ReleasedBindingLeases {
            owner: self,
            closure: super::super::events::ExecutionBindingLeaseClosure {
                schema: 1,
                graph_id: graph.id.clone(),
                node_id: node.into(),
                binding: binding.clone(),
                start_sequence: position.0,
                start_cursor: position.1,
                start_transaction_index: position.2,
                no_dispatch: None,
                source: if self.recovery {
                    "recovery_release"
                } else {
                    "normal_release"
                }
                .into(),
            },
            sources: Vec::new(),
        }
    }

    pub(crate) fn release_original<'owner>(
        &'owner mut self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
        node: &str,
        binding: &super::super::events::ExecutionNodeBinding,
        position: (u64, u64, u32),
        resource: &mut Option<super::ExecutionResourceLease>,
        scope: &mut Option<ScopeLockLease>,
        worktree: &mut Option<super::WorktreeLease>,
        worktrees: &super::WorktreeLeaseManager,
    ) -> Result<ReleasedBindingLeases<'owner>, String> {
        if self.recovery || self.failed || !self.matches(graph, node, binding) {
            return Err("original release has no exact original owner capability".into());
        }
        if !self.released {
            if binding.resource_lease_refs
                != resource
                    .as_ref()
                    .map(|lease| vec![lease.projection_reference()])
                    .unwrap_or_default()
                || binding.scope_lease_ref
                    != scope.as_ref().map(ScopeLockLease::projection_reference)
                || binding.worktree_lease_ref
                    != worktree
                        .as_ref()
                        .map(|lease| lease.record().lease_id.to_string())
            {
                return Err("original release handles do not match durable binding".into());
            }
            if let Some(lease) = worktree.as_ref() {
                let record = lease.record();
                if record.task_id != graph.id || record.owner_id != node {
                    return Err("original worktree handle identity mismatch".into());
                }
                if let Err(error) = worktrees.release(record.lease_id, &record.owner_id) {
                    self.failed = true;
                    return Err(error.to_string());
                }
                drop(worktree.take());
            }
            drop(resource.take());
            if let Some(lease) = scope.take() {
                if let Err(error) = lease.try_release() {
                    self.failed = true;
                    return Err(error.to_string());
                }
            }
            self.released = true;
        }
        Ok(self.proof(graph, node, binding, position))
    }

    pub(crate) fn release_recovered<'owner>(
        &'owner mut self,
        graph: &harness_contract::execution_graph::ExecutionGraph,
        node: &str,
        binding: &super::super::events::ExecutionNodeBinding,
        position: (u64, u64, u32),
        physical: &super::super::commit_service::BindingRecoveryQuiescence,
        worktrees: &super::WorktreeLeaseManager,
        workspace: &str,
    ) -> Result<ReleasedBindingLeases<'owner>, String> {
        if !self.recovery
            || self.failed
            || !self.matches(graph, node, binding)
            || !physical.matches(graph, node, binding)
        {
            return Err("recovered release lacks exact authenticated quiescence capability".into());
        }
        if let Some(identity) = &binding.worktree_lease_ref {
            let identity = Uuid::parse_str(identity).map_err(|error| error.to_string())?;
            let record = worktrees
                .record(identity)
                .map_err(|error| error.to_string())?
                .ok_or("original worktree lease missing")?;
            if record.workspace_id != workspace
                || record.task_id != graph.id
                || record.owner_id != node
            {
                return Err("recovered worktree exact owner mismatch".into());
            }
            match record.status {
                super::WorktreeLeaseStatus::Active => {
                    worktrees
                        .release(identity, node)
                        .map_err(|error| error.to_string())?;
                }
                super::WorktreeLeaseStatus::Released => {}
                _ => return Err("TTL reclaim is not authenticated release".into()),
            }
        }
        self.released = true;
        let mut released = self.proof(graph, node, binding, position);
        released.sources = physical.sources().to_vec();
        Ok(released)
    }
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
                owner_generation: Uuid::new_v4(),
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
                    #[cfg(test)]
                    fail_release: false,
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

    pub fn try_acquire(
        &self,
        requests: impl IntoIterator<Item = ScopeLockRequest>,
    ) -> Result<Option<ScopeLockLease>, ScopeLockError> {
        let requests = normalize_requests(requests)?;
        let mut guard = self
            .shared
            .state
            .lock()
            .map_err(|_| ScopeLockError::Poisoned)?;
        if guard
            .pending
            .iter()
            .any(|pending| requests_conflict(&requests, &pending.requests))
            || guard
                .active
                .values()
                .any(|active| requests_conflict(&requests, &active.requests))
        {
            return Ok(None);
        }
        let Some(process_locks) = try_process_locks(self.shared.lock_root.as_deref(), &requests)?
        else {
            return Ok(None);
        };
        let id = Uuid::new_v4();
        guard.active.insert(
            id,
            ActiveLock {
                requests: requests.clone(),
            },
        );
        Ok(Some(ScopeLockLease {
            shared: Arc::clone(&self.shared),
            id,
            requests,
            process_locks,
            released: false,
            #[cfg(test)]
            fail_release: false,
        }))
    }

    pub(crate) fn lease_is_active(&self, reference: &str) -> Result<Option<bool>, ScopeLockError> {
        let Some((generation, identity)) = reference
            .strip_prefix("scope-lease:")
            .and_then(|value| value.split_once(':'))
        else {
            return Ok(None);
        };
        if generation != self.shared.owner_generation.to_string() {
            return Ok(None);
        }
        let Ok(identity) = Uuid::parse_str(identity) else {
            return Ok(None);
        };
        let guard = self
            .shared
            .state
            .lock()
            .map_err(|_| ScopeLockError::Poisoned)?;
        Ok(Some(guard.active.contains_key(&identity)))
    }

    fn binding_owner_path(&self, key: &str) -> Result<PathBuf, ScopeLockError> {
        if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ScopeLockError::InvalidPath(
                "invalid binding owner identity".into(),
            ));
        }
        let root = self.shared.lock_root.as_ref().ok_or_else(|| {
            ScopeLockError::InvalidPath(
                "binding recovery requires persistent scope lock root".into(),
            )
        })?;
        Ok(root.join("binding-owners-v1").join(format!("{key}.lock")))
    }

    pub(crate) fn acquire_binding_owner(
        &self,
        key: &str,
        validate_unadmitted: impl FnOnce() -> Result<(), String>,
    ) -> Result<BindingOwnerFence, ScopeLockError> {
        use std::io::{Read, Seek, SeekFrom, Write};
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let path = self.binding_owner_path(key)?;
        let directory = path.parent().unwrap();
        fs::create_dir_all(directory).map_err(|error| scope_io(directory, error))?;
        File::open(directory.parent().unwrap())
            .and_then(|file| file.sync_all())
            .map_err(|error| scope_io(directory, error))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|error| scope_io(&path, error))?;
        FileExt::try_lock_exclusive(&file).map_err(|error| scope_io(&path, error))?;
        let metadata = file.metadata().map_err(|error| scope_io(&path, error))?;
        let current = fs::symlink_metadata(&path).map_err(|error| scope_io(&path, error))?;
        if !metadata.is_file()
            || !current.is_file()
            || metadata.dev() != current.dev()
            || metadata.ino() != current.ino()
        {
            return Err(ScopeLockError::InvalidPath(
                "binding owner inode changed".into(),
            ));
        }
        let mut marker = String::new();
        Read::by_ref(&mut file)
            .take(128)
            .read_to_string(&mut marker)
            .map_err(|error| scope_io(&path, error))?;
        let expected = format!("binding-owner-v1:{key}\n");
        if !expected.starts_with(marker.as_str()) {
            return Err(ScopeLockError::InvalidPath(
                "unsupported or incomplete binding owner marker".into(),
            ));
        }
        validate_unadmitted().map_err(ScopeLockError::InvalidPath)?;
        if marker != expected {
            file.seek(SeekFrom::Start(0))
                .and_then(|_| file.write_all(expected.as_bytes()))
                .map_err(|error| scope_io(&path, error))?;
        }
        file.sync_all().map_err(|error| scope_io(&path, error))?;
        File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(|error| scope_io(directory, error))?;
        Ok(BindingOwnerFence {
            file,
            key: key.into(),
            recovery: false,
            released: false,
            failed: false,
        })
    }

    pub(crate) fn try_recover_binding_owner(
        &self,
        key: &str,
    ) -> Result<Option<BindingOwnerFence>, ScopeLockError> {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        let path = self.binding_owner_path(key)?;
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(scope_io(&path, error)),
        };
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(scope_io(&path, error)),
        }
        let mut marker = String::new();
        file.by_ref()
            .take(128)
            .read_to_string(&mut marker)
            .map_err(|error| scope_io(&path, error))?;
        if marker != format!("binding-owner-v1:{key}\n") {
            return Err(ScopeLockError::InvalidPath(
                "unsupported or incomplete binding owner marker".into(),
            ));
        }
        Ok(Some(BindingOwnerFence {
            file,
            key: key.into(),
            recovery: true,
            released: false,
            failed: false,
        }))
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
    #[cfg(test)]
    fail_release: bool,
}

impl ScopeLockLease {
    pub(crate) fn projection_reference(&self) -> String {
        format!("scope-lease:{}:{}", self.shared.owner_generation, self.id)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn requests(&self) -> &[ScopeLockRequest] {
        &self.requests
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    #[cfg(test)]
    pub(crate) fn fail_release_for_test(&mut self) {
        self.fail_release = true;
    }

    pub(crate) fn try_release(mut self) -> Result<(), ScopeLockError> {
        self.release_checked()
    }

    fn release_inner(&mut self) {
        let _ = self.release_checked();
    }

    fn release_checked(&mut self) -> Result<(), ScopeLockError> {
        if self.released {
            return Ok(());
        }
        let mut release_error = None;
        #[cfg(test)]
        if self.fail_release {
            release_error = Some(scope_io(
                Path::new("scope-lease"),
                std::io::Error::other("injected explicit release failure"),
            ));
        }
        for lock in self.process_locks.drain(..).rev() {
            if let Err(error) = FileExt::unlock(&lock) {
                release_error = Some(scope_io(Path::new("scope-lease"), error));
            }
            drop(lock);
        }
        {
            let mut guard = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            guard.active.remove(&self.id);
        }
        self.released = true;
        self.shared.changed.notify_waiters();
        release_error.map_or(Ok(()), Err)
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
        {
            let mut guard = self
                .shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
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

    #[test]
    fn r1_478_unadmitted_owner_retry_keeps_inode_and_runs_checker_under_lock() {
        use std::os::unix::fs::MetadataExt;
        let directory = tempfile::tempdir().unwrap();
        let manager = ScopeLockManager::persistent(directory.path()).unwrap();
        let peer = ScopeLockManager::persistent(directory.path()).unwrap();
        let key = "a".repeat(64);
        let first = manager.acquire_binding_owner(&key, || Ok(())).unwrap();
        let path = manager.binding_owner_path(&key).unwrap();
        let inode = fs::metadata(&path).unwrap().ino();
        let checks = std::cell::Cell::new(0);
        assert!(peer
            .acquire_binding_owner(&key, || {
                checks.set(checks.get() + 1);
                Ok(())
            })
            .is_err());
        assert_eq!(checks.get(), 0);
        assert!(peer.try_recover_binding_owner(&key).unwrap().is_none());
        drop(first);
        let retried = peer
            .acquire_binding_owner(&key, || {
                checks.set(checks.get() + 1);
                assert!(manager.try_recover_binding_owner(&key).unwrap().is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(checks.get(), 1);
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            format!("binding-owner-v1:{key}\n")
        );
        assert!(!retried.recovery);
        drop(retried);
        assert!(
            manager
                .try_recover_binding_owner(&key)
                .unwrap()
                .unwrap()
                .recovery
        );
    }

    #[test]
    fn r1_478_canonical_rejection_preserves_marker_and_releases_same_inode_lock() {
        use std::os::unix::fs::MetadataExt;
        let directory = tempfile::tempdir().unwrap();
        let manager = ScopeLockManager::persistent(directory.path()).unwrap();
        let key = "b".repeat(64);
        drop(manager.acquire_binding_owner(&key, || Ok(())).unwrap());
        let path = manager.binding_owner_path(&key).unwrap();
        let inode = fs::metadata(&path).unwrap().ino();
        let marker = fs::read(&path).unwrap();
        let result = manager
            .acquire_binding_owner(&key, || Err("canonical attempt already admitted".into()));
        assert!(matches!(result, Err(ScopeLockError::InvalidPath(reason))
            if reason == "canonical attempt already admitted"));
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(fs::read(&path).unwrap(), marker);
        assert!(manager.try_recover_binding_owner(&key).unwrap().is_some());
    }

    #[test]
    fn r1_478_empty_marker_requires_admission_check_and_missing_recovery_never_creates() {
        use std::os::unix::fs::MetadataExt;
        let directory = tempfile::tempdir().unwrap();
        let manager = ScopeLockManager::persistent(directory.path()).unwrap();
        let key = "c".repeat(64);
        let path = manager.binding_owner_path(&key).unwrap();
        assert!(manager.try_recover_binding_owner(&key).unwrap().is_none());
        assert!(!path.exists());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        File::create(&path).unwrap();
        let inode = fs::metadata(&path).unwrap().ino();
        assert!(manager.try_recover_binding_owner(&key).is_err());
        assert!(manager
            .acquire_binding_owner(&key, || Err("source unavailable".into()))
            .is_err());
        assert!(fs::read(&path).unwrap().is_empty());
        drop(manager.acquire_binding_owner(&key, || Ok(())).unwrap());
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            format!("binding-owner-v1:{key}\n")
        );
        let interrupted = format!("binding-owner-v1:{}", &key[..8]);
        fs::write(&path, &interrupted).unwrap();
        assert!(manager.try_recover_binding_owner(&key).is_err());
        assert!(manager
            .acquire_binding_owner(&key, || Err("source unavailable".into()))
            .is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), interrupted);
        drop(manager.acquire_binding_owner(&key, || Ok(())).unwrap());
        assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            format!("binding-owner-v1:{key}\n")
        );
        fs::write(&path, "corrupt marker").unwrap();
        let checks = std::cell::Cell::new(0);
        assert!(manager
            .acquire_binding_owner(&key, || {
                checks.set(checks.get() + 1);
                Ok(())
            })
            .is_err());
        assert_eq!(checks.get(), 0);
        assert_eq!(fs::read_to_string(&path).unwrap(), "corrupt marker");
    }

    #[test]
    fn r1_478_owner_admission_rejects_symlink_without_touching_target() {
        let directory = tempfile::tempdir().unwrap();
        let manager = ScopeLockManager::persistent(directory.path()).unwrap();
        let key = "d".repeat(64);
        let path = manager.binding_owner_path(&key).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let target = directory.path().join("unrelated");
        fs::write(&target, "untouched").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let checks = std::cell::Cell::new(0);
        assert!(manager
            .acquire_binding_owner(&key, || {
                checks.set(checks.get() + 1);
                Ok(())
            })
            .is_err());
        assert_eq!(checks.get(), 0);
        assert_eq!(fs::read_to_string(&target).unwrap(), "untouched");
    }

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
    #[test]
    fn try_acquire_normalizes_conflicts_and_releases_at_scope_exit() {
        let manager = ScopeLockManager::new();
        let scope = workspace_object("proof/source.py");
        {
            let lease = manager
                .try_acquire([
                    request(scope.clone(), ScopeLockMode::Read),
                    request(scope.clone(), ScopeLockMode::Write),
                ])
                .unwrap()
                .unwrap();
            assert_eq!(lease.requests().len(), 1);
            assert_eq!(lease.requests()[0].mode, ScopeLockMode::Write);
            assert!(manager
                .try_acquire([request(scope.clone(), ScopeLockMode::Read)])
                .unwrap()
                .is_none());
            assert_eq!(manager.active_lease_count().unwrap(), 1);
        }
        assert_eq!(manager.active_lease_count().unwrap(), 0);
        assert!(manager
            .try_acquire([request(scope, ScopeLockMode::Write)])
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn try_acquire_respects_original_pending_writer_fairness_without_waiting() {
        let manager = ScopeLockManager::new();
        let scope = workspace_object("proof/source.py");
        let reader = manager
            .try_acquire([request(scope.clone(), ScopeLockMode::Read)])
            .unwrap()
            .unwrap();
        let waiter_manager = manager.clone();
        let waiter_scope = scope.clone();
        let waiter = tokio::spawn(async move {
            waiter_manager
                .acquire([request(waiter_scope, ScopeLockMode::Write)], None)
                .await
        });
        for _iteration in 0..1000 {
            if !manager.shared.state.lock().unwrap().pending.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(manager.shared.state.lock().unwrap().pending.len(), 1);
        let started = Instant::now();
        assert!(manager
            .try_acquire([request(scope.clone(), ScopeLockMode::Read)])
            .unwrap()
            .is_none());
        assert!(started.elapsed() < Duration::from_secs(1));
        let independent = manager
            .try_acquire([request(
                workspace_object("other/source.py"),
                ScopeLockMode::Write,
            )])
            .unwrap()
            .unwrap();
        drop(independent);
        drop(reader);
        let writer = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(manager
            .try_acquire([request(scope, ScopeLockMode::Read)])
            .unwrap()
            .is_none());
        drop(writer);
        assert_eq!(manager.active_lease_count().unwrap(), 0);
    }

    #[test]
    fn try_acquire_preserves_persistent_process_lock_exclusion() {
        let directory = tempfile::tempdir().unwrap();
        let original = ScopeLockManager::persistent(directory.path()).unwrap();
        let peer = ScopeLockManager::persistent(directory.path()).unwrap();
        let scope = workspace_object("proof/source.py");
        let writer = original
            .try_acquire([request(scope.clone(), ScopeLockMode::Write)])
            .unwrap()
            .unwrap();
        assert!(peer
            .try_acquire([request(scope.clone(), ScopeLockMode::Read)])
            .unwrap()
            .is_none());
        assert_eq!(peer.active_lease_count().unwrap(), 0);
        drop(writer);
        let reader = peer
            .try_acquire([request(scope.clone(), ScopeLockMode::Read)])
            .unwrap()
            .unwrap();
        assert!(original
            .try_acquire([request(scope.clone(), ScopeLockMode::Write)])
            .unwrap()
            .is_none());
        drop(reader);
        assert!(original
            .try_acquire([request(scope, ScopeLockMode::Write)])
            .unwrap()
            .is_some());
    }
}
