use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use managed_worker_launcher::{proc_identity, read_boot_id, sha256_file, WorkerIdentityV1};

use crate::{ManagedWorkerError, ManagedWorkerResult, WorkerRuntimeDir};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeRecoveryReport {
    pub inspected: usize,
    pub terminated: usize,
    pub cleaned: usize,
}

pub fn recover_runtime_root(
    root: impl AsRef<Path>,
    current_generation: &str,
    current_gateway_instance: &str,
) -> ManagedWorkerResult<RuntimeRecoveryReport> {
    let root = root.as_ref();
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RuntimeRecoveryReport::default())
        }
        Err(error) => return Err(ManagedWorkerError::io(root, error)),
    };
    let mut report = RuntimeRecoveryReport::default();
    for entry in entries {
        let entry = entry.map_err(|error| ManagedWorkerError::io(root, error))?;
        let path = entry.path();
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| ManagedWorkerError::io(&path, error))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            continue;
        }
        report.inspected += 1;
        let runtime = WorkerRuntimeDir::create(&path)?;
        let identity_path = runtime.identity_path();
        let identity = match read_identity(&identity_path) {
            Ok(identity) => identity,
            Err(ManagedWorkerError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                runtime.cleanup_ephemeral()?;
                report.cleaned += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        if identity.generation == current_generation
            && identity.gateway_instance == current_gateway_instance
        {
            continue;
        }
        if Path::new(&format!("/proc/{}", identity.pid)).exists() {
            verify_live_identity(&identity)?;
            kill_group(identity.pgid)?;
            report.terminated += 1;
        } else if process_group_members(identity.pgid) > 0 {
            verify_cgroup_membership(&identity)?;
            kill_group(identity.pgid)?;
            report.terminated += 1;
        }
        runtime.cleanup_ephemeral()?;
        remove_if_exists(&identity_path)?;
        if let Some(cgroup) = &identity.cgroup_path {
            let _ = fs::remove_dir(cgroup);
        }
        report.cleaned += 1;
    }
    Ok(report)
}

fn verify_cgroup_membership(identity: &WorkerIdentityV1) -> ManagedWorkerResult<()> {
    let cgroup = identity.cgroup_path.as_ref().ok_or_else(|| {
        ManagedWorkerError::RecoveryIsolation(format!(
            "leader pid {} is gone while process group {} remains and no cgroup proof exists",
            identity.pid, identity.pgid
        ))
    })?;
    if !managed_worker_launcher::is_cgroup2_mount(cgroup) {
        return Err(ManagedWorkerError::RecoveryIsolation(format!(
            "recorded cgroup is not on a cgroup v2 mount: {}",
            cgroup.display()
        )));
    }
    let admitted: std::collections::BTreeSet<u32> = fs::read_to_string(cgroup.join("cgroup.procs"))
        .map_err(|error| ManagedWorkerError::io(cgroup.join("cgroup.procs"), error))?
        .lines()
        .map(|value| value.parse::<u32>())
        .collect::<Result<_, _>>()
        .map_err(|error| ManagedWorkerError::RecoveryIsolation(error.to_string()))?;
    let observed = process_group_pids(identity.pgid);
    if !membership_covers_group(&admitted, &observed) {
        return Err(ManagedWorkerError::RecoveryIsolation(format!(
            "remaining process group {} is not wholly owned by recorded cgroup",
            identity.pgid
        )));
    }
    Ok(())
}

fn membership_covers_group(admitted: &std::collections::BTreeSet<u32>, observed: &[u32]) -> bool {
    !observed.is_empty() && observed.iter().all(|pid| admitted.contains(pid))
}

fn read_identity(path: &Path) -> ManagedWorkerResult<WorkerIdentityV1> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| ManagedWorkerError::io(path, error))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(ManagedWorkerError::RecoveryIsolation(format!(
            "identity is not a 0600 regular file: {}",
            path.display()
        )));
    }
    serde_json::from_slice(&fs::read(path).map_err(|error| ManagedWorkerError::io(path, error))?)
        .map_err(|error| ManagedWorkerError::RecoveryIsolation(error.to_string()))
}

fn verify_live_identity(identity: &WorkerIdentityV1) -> ManagedWorkerResult<()> {
    let observed_boot =
        read_boot_id().map_err(|error| ManagedWorkerError::RecoveryIsolation(error.to_string()))?;
    let observed_exe = fs::read_link(format!("/proc/{}/exe", identity.pid))
        .map_err(|error| ManagedWorkerError::io(format!("/proc/{}/exe", identity.pid), error))?;
    let (observed_pgid, observed_ticks) = proc_identity(identity.pid)
        .map_err(|error| ManagedWorkerError::RecoveryIsolation(error.to_string()))?;
    let observed_digest = sha256_file(&observed_exe)
        .map_err(|error| ManagedWorkerError::RecoveryIsolation(error.to_string()))?;
    if observed_boot != identity.boot_id
        || observed_exe != identity.target_path
        || observed_pgid != identity.pgid
        || observed_ticks != identity.proc_start_ticks
        || observed_digest != identity.target_sha256
    {
        return Err(ManagedWorkerError::RecoveryIsolation(format!(
            "pid {} does not match boot/program/digest/pgid/start identity; no signal sent",
            identity.pid
        )));
    }
    Ok(())
}

fn kill_group(pgid: u32) -> ManagedWorkerResult<()> {
    let status = std::process::Command::new("/bin/kill")
        .args(["-KILL", "--", &format!("-{pgid}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| ManagedWorkerError::Signal(error.to_string()))?;
    if !status.success() && process_group_members(pgid) > 0 {
        return Err(ManagedWorkerError::Signal(format!(
            "failed to kill recovered process group {pgid}"
        )));
    }
    for _ in 0..100 {
        if process_group_members(pgid) == 0 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err(ManagedWorkerError::RecoveryIsolation(format!(
        "recovered process group {pgid} did not disappear"
    )))
}

fn process_group_members(pgid: u32) -> usize {
    process_group_pids(pgid).len()
}

fn process_group_pids(pgid: u32) -> Vec<u32> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            proc_group_state(*pid).is_some_and(|(group, zombie)| group == pgid && !zombie)
        })
        .collect()
}

fn proc_group_state(pid: u32) -> Option<(u32, bool)> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.rfind(')')?;
    let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
    let state = *fields.first()?;
    let pgid = fields.get(2)?.parse().ok()?;
    Some((pgid, state == "Z"))
}

fn remove_if_exists(path: &PathBuf) -> ManagedWorkerResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ManagedWorkerError::io(path, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::{fs::OpenOptionsExt, process::CommandExt};
    use std::process::Stdio;

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "managed-worker-recovery-{label}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    fn write_identity(path: &Path, identity: &WorkerIdentityV1) {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .expect("identity fixture");
        serde_json::to_writer(&mut file, identity).expect("identity encode");
        file.flush().expect("identity flush");
    }

    #[test]
    fn pid_reuse_start_tick_mismatch_is_quarantined_without_signal() {
        let root = temp_root("pid-reuse");
        let generation = root.join("old-generation");
        let runtime = WorkerRuntimeDir::create(&generation).expect("runtime");
        let pid = std::process::id();
        let (pgid, ticks) = proc_identity(pid).expect("proc identity");
        let target = fs::canonicalize(std::env::current_exe().expect("test executable"))
            .expect("canonical test executable");
        let identity = WorkerIdentityV1 {
            schema_version: 1,
            pid,
            pgid,
            proc_start_ticks: ticks + 1,
            boot_id: read_boot_id().expect("boot id"),
            target_sha256: sha256_file(&target).expect("target digest"),
            target_path: target,
            generation: "old".into(),
            gateway_instance: "old-gateway".into(),
            launch_id: "old-launch".into(),
            launch_digest: "sha256:old".into(),
            cgroup_path: None,
        };
        write_identity(&runtime.identity_path(), &identity);
        assert!(matches!(
            recover_runtime_root(&root, "new", "new-gateway"),
            Err(ManagedWorkerError::RecoveryIsolation(message)) if message.contains("no signal sent")
        ));
        assert!(Path::new(&format!("/proc/{pid}")).exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn dead_identity_and_ephemeral_files_are_cleaned_idempotently() {
        let root = temp_root("dead");
        let generation = root.join("old-generation");
        let runtime = WorkerRuntimeDir::create(&generation).expect("runtime");
        fs::write(runtime.socket_path(), b"socket").expect("socket fixture");
        fs::write(runtime.credential_path(), b"credential").expect("credential fixture");
        let target = fs::canonicalize("/bin/true").expect("target");
        let identity = WorkerIdentityV1 {
            schema_version: 1,
            pid: u32::MAX,
            pgid: u32::MAX,
            proc_start_ticks: 1,
            boot_id: read_boot_id().expect("boot id"),
            target_sha256: sha256_file(&target).expect("digest"),
            target_path: target,
            generation: "old".into(),
            gateway_instance: "old-gateway".into(),
            launch_id: "dead-launch".into(),
            launch_digest: "sha256:dead".into(),
            cgroup_path: None,
        };
        write_identity(&runtime.identity_path(), &identity);
        let first = recover_runtime_root(&root, "new", "new-gateway").expect("first recovery");
        assert_eq!(first.cleaned, 1);
        assert!(!runtime.socket_path().exists());
        assert!(!runtime.credential_path().exists());
        assert!(!runtime.identity_path().exists());
        let second = recover_runtime_root(&root, "new", "new-gateway").expect("second recovery");
        assert_eq!(second.terminated, 0);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn exact_live_identity_is_recovered_and_reaped_without_touching_others() {
        let root = temp_root("exact-live");
        let generation = root.join("old-generation");
        let runtime = WorkerRuntimeDir::create(&generation).expect("runtime");
        let target = fs::canonicalize("/bin/sleep").expect("target");
        let mut command = std::process::Command::new(&target);
        command
            .arg("60")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().expect("worker fixture");
        let mut other_command = std::process::Command::new(&target);
        other_command
            .arg("60")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut other = other_command.spawn().expect("unrelated worker fixture");
        let pid = child.id();
        let (pgid, ticks) = proc_identity(pid).expect("proc identity");
        let identity = WorkerIdentityV1 {
            schema_version: 1,
            pid,
            pgid,
            proc_start_ticks: ticks,
            boot_id: read_boot_id().expect("boot id"),
            target_sha256: sha256_file(&target).expect("digest"),
            target_path: target,
            generation: "old".into(),
            gateway_instance: "old-gateway".into(),
            launch_id: "live-launch".into(),
            launch_digest: "sha256:live".into(),
            cgroup_path: None,
        };
        write_identity(&runtime.identity_path(), &identity);
        let report = recover_runtime_root(&root, "new", "new-gateway").expect("recover");
        assert_eq!(report.terminated, 1);
        assert!(other.try_wait().expect("other status").is_none());
        let status = child.wait().expect("reap fixture");
        assert!(!status.success());
        std::process::Command::new("/bin/kill")
            .args(["-KILL", "--", &format!("-{}", other.id())])
            .status()
            .expect("kill unrelated fixture");
        let _ = other.wait();
        assert!(!runtime.identity_path().exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn leaderless_cgroup_membership_parser_requires_every_descendant() {
        let admitted = std::collections::BTreeSet::from([101, 102, 103]);
        assert!(membership_covers_group(&admitted, &[102, 103]));
        assert!(!membership_covers_group(&admitted, &[]));
        assert!(!membership_covers_group(&admitted, &[102, 999]));
    }
}
