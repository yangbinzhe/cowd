//! PostgreSQL-only storage maintenance commands.
//!
//! Cowd has one database owner. These commands never inspect, import, create,
//! or fall back to a second database.

use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use sha2::{Digest, Sha256};

use crate::selected_storage::SelectedStorageTopology;

pub(crate) fn run(args: &[String]) -> Result<(), String> {
    let request = parse_request(args)?;
    let context = PostgresMaintenanceContext::load()?;
    match request {
        StorageRequest::Plan => context.plan(),
        StorageRequest::Upgrade => context.upgrade(),
        StorageRequest::Verify => context.verify(),
        StorageRequest::Status => context.status(),
        StorageRequest::Cleanup(CleanupRequest::BashTtl) => context.cleanup(),
        StorageRequest::Cleanup(CleanupRequest::QuiescedArtifacts { evidence_path }) => {
            context.cleanup_quiesced_artifacts(&evidence_path)
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CleanupRequest {
    BashTtl,
    QuiescedArtifacts { evidence_path: PathBuf },
}

#[derive(Debug, PartialEq, Eq)]
enum StorageRequest {
    Plan,
    Upgrade,
    Verify,
    Status,
    Cleanup(CleanupRequest),
}

fn parse_request(args: &[String]) -> Result<StorageRequest, String> {
    match args {
        [command] => match command.as_str() {
            "plan" => Ok(StorageRequest::Plan),
            "upgrade" => Ok(StorageRequest::Upgrade),
            "verify" => Ok(StorageRequest::Verify),
            "status" => Ok(StorageRequest::Status),
            "cleanup" => Ok(StorageRequest::Cleanup(CleanupRequest::BashTtl)),
            _ => Err(storage_usage()),
        },
        [command, flag, path]
            if command == "cleanup"
                && flag == "--quiescence-evidence"
                && Path::new(path).is_absolute() =>
        {
            Ok(StorageRequest::Cleanup(CleanupRequest::QuiescedArtifacts {
                evidence_path: path.into(),
            }))
        }
        _ => Err(storage_usage()),
    }
}

fn storage_usage() -> String {
    "usage: cowd storage plan | upgrade | verify | status | cleanup [--quiescence-evidence /absolute/file]".into()
}

const EVIDENCE_MAX_BYTES: u64 = 16 * 1024;
const QUIESCENCE_SCOPE: &str = "all_publishers_sharing_selected_artifact_repository_and_blobs";

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuiescenceEvidence {
    schema_version: u32,
    logical_identity: String,
    workspace: String,
    all_publishers_stopped: bool,
    all_inflight_drained: bool,
    all_prepared_candidates_discarded: bool,
    quiescence_scope: String,
    maintain_quiescence_until_command_exit: bool,
    issued_at_unix_ms: u64,
    expires_at_unix_ms: u64,
    evidence_ref: String,
}

struct ValidatedQuiescenceEvidence {
    declaration: QuiescenceEvidence,
    sha256: String,
}

impl QuiescenceEvidence {
    fn validate(&self, identity: &str, workspace: &Path, now: SystemTime) -> Result<(), String> {
        if self.schema_version != 1
            || self.logical_identity != identity
            || workspace.to_str() != Some(self.workspace.as_str())
            || !workspace.is_absolute()
            || !self.all_publishers_stopped
            || !self.all_inflight_drained
            || !self.all_prepared_candidates_discarded
            || !self.maintain_quiescence_until_command_exit
            || self.quiescence_scope != QUIESCENCE_SCOPE
        {
            return Err(
                "quiescence declaration identity, workspace or all-publisher scope mismatch".into(),
            );
        }
        if self.evidence_ref.trim().is_empty()
            || self.evidence_ref.len() > 256
            || !self
                .evidence_ref
                .bytes()
                .all(|byte| (0x20..=0x7e).contains(&byte))
        {
            return Err("quiescence evidence_ref must be a nonempty printable reference of at most 256 bytes".into());
        }
        let now_ms = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(stringify)?
            .as_millis();
        let ttl = self.expires_at_unix_ms.checked_sub(self.issued_at_unix_ms);
        if !ttl.is_some_and(|ttl| ttl > 0 && ttl <= 300_000)
            || now_ms < u128::from(self.issued_at_unix_ms)
            || now_ms >= u128::from(self.expires_at_unix_ms)
        {
            return Err("quiescence declaration is future, expired or exceeds the 300000ms admission window".into());
        }
        Ok(())
    }
}

fn load_quiescence_evidence(
    path: &Path,
    identity: &str,
    workspace: &Path,
    now: SystemTime,
) -> Result<ValidatedQuiescenceEvidence, String> {
    let mut file = open_evidence_file(path)?;
    let before = file.metadata().map_err(stringify)?;
    let bytes = read_stable_evidence(&mut file, &before)?;
    let declaration: QuiescenceEvidence = serde_json::from_slice(&bytes)
        .map_err(|_| "invalid closed quiescence declaration JSON".to_string())?;
    declaration.validate(identity, workspace, now)?;
    Ok(ValidatedQuiescenceEvidence {
        declaration,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    })
}

#[cfg(unix)]
fn open_evidence_file(path: &Path) -> Result<fs::File, String> {
    use std::ffi::CString;
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    };
    use std::path::Component;

    if !path.is_absolute() {
        return Err("quiescence declaration path must be absolute".into());
    }
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err("quiescence declaration requires an absolute Unix path".into());
    }
    let mut directory = fs::File::open("/").map_err(stringify)?;
    let mut components = components.peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err("quiescence declaration path contains an unsafe component".into());
        };
        let name = CString::new(name.as_bytes()).map_err(stringify)?;
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | if components.peek().is_some() {
                libc::O_DIRECTORY
            } else {
                libc::O_NONBLOCK
            };
        let descriptor = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if descriptor < 0 {
            return Err(format!(
                "cannot securely open quiescence declaration: {}",
                std::io::Error::last_os_error()
            ));
        }
        let opened = unsafe { fs::File::from_raw_fd(descriptor) };
        if components.peek().is_none() {
            return Ok(opened);
        }
        directory = opened;
    }
    Err("quiescence declaration path must name a regular file".into())
}

#[cfg(not(unix))]
fn open_evidence_file(_path: &Path) -> Result<fs::File, String> {
    Err("secure quiescence declaration admission is unavailable on this platform".into())
}

#[cfg(unix)]
fn read_stable_evidence(file: &mut fs::File, before: &fs::Metadata) -> Result<Vec<u8>, String> {
    use std::os::unix::fs::MetadataExt;

    let fingerprint = |metadata: &fs::Metadata| {
        (
            metadata.dev(),
            metadata.ino(),
            metadata.uid(),
            metadata.gid(),
            metadata.mode(),
            metadata.nlink(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    if !before.is_file()
        || before.uid() != unsafe { libc::geteuid() }
        || before.mode() & 0o7777 != 0o600
        || before.nlink() != 1
        || before.len() > EVIDENCE_MAX_BYTES
    {
        return Err(
            "quiescence declaration must be euid-owned regular 0600 nlink=1 and at most 16KiB"
                .into(),
        );
    }
    let mut bytes = Vec::new();
    (&mut *file)
        .take(EVIDENCE_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(stringify)?;
    let after = file.metadata().map_err(stringify)?;
    if bytes.len() as u64 != before.len()
        || bytes.len() as u64 > EVIDENCE_MAX_BYTES
        || fingerprint(before) != fingerprint(&after)
    {
        return Err("quiescence declaration changed while reading".into());
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_stable_evidence(_file: &mut fs::File, _before: &fs::Metadata) -> Result<Vec<u8>, String> {
    Err("secure quiescence declaration admission is unavailable on this platform".into())
}

pub(crate) fn execute_quiesced_artifact_cleanup(
    identity: &str,
    workspace: &Path,
    evidence_path: &Path,
    pid_path: &Path,
    compose: impl FnOnce() -> Result<SelectedStorageTopology, String>,
) -> Result<serde_json::Value, String> {
    let workspace = fs::canonicalize(workspace).map_err(stringify)?;
    let evidence =
        load_quiescence_evidence(evidence_path, identity, &workspace, SystemTime::now())?;
    ensure_gateway_stopped_for_artifact_cleanup(pid_path)?;
    let topology = compose()?;
    let actual_identity = topology.postgres_executor.logical_identity().to_string();
    evidence
        .declaration
        .validate(&actual_identity, &workspace, SystemTime::now())?;
    let roots = topology.quiesced_artifact_roots();
    let _namespace_guard = topology.artifact_maintenance_guard();
    let mut store = topology.into_exclusive_artifact_store()?;
    evidence
        .declaration
        .validate(&actual_identity, &workspace, SystemTime::now())?;
    ensure_gateway_stopped_for_artifact_cleanup(pid_path)?;
    let prepared = roots.reconcile_prepared_batch(&mut store, true, &actual_identity, &evidence.sha256).map_err(|error| {
        format!("prepared cleanup failed; partial metadata retirement may have occurred, no global rollback: {error}")
    })?;
    evidence
        .declaration
        .validate(&actual_identity, &workspace, SystemTime::now())?;
    ensure_gateway_stopped_for_artifact_cleanup(pid_path)?;
    let report = store.reconcile_quiesced_orphans(true, 100).map_err(|error| {
        format!("artifact cleanup failed; partial FS/PG deletion may have occurred, no global rollback: {error}")
    })?;
    Ok(serde_json::json!({
        "operation": "artifact_quiesced_cleanup",
        "scope": "closed_typed_prepared_roots_then_original_generation_gc",
        "quiescence_basis": "trusted_operator_attestation",
        "quiescence_limit": "not_automatic_global_stop_proof; operator_must_keep_all_shared_repository_and_blobs_publishers_stopped_and_drained_until_exit",
        "local_gateway_check": "passed_necessary_not_sufficient",
        "logical_identity": actual_identity,
        "canonical_workspace": workspace,
        "evidence_ref": evidence.declaration.evidence_ref,
        "evidence_sha256": evidence.sha256,
        "batch_limit": 100,
        "prepared": prepared,
        "examined": report.examined,
        "removed_objects": report.removed_objects,
        "reclaimed_bytes": report.reclaimed_bytes,
        "reclaimed_bytes_basis": "metadata_logical_bytes_not_measured_disk_reduction",
        "unknown_pins_policy": "preserve",
        "unknown_reservations_policy": "preserve",
        "unknown_pins_count": null,
        "unknown_reservations_count": null,
        "unknown_inventory": "legacy_aggregate_reservations_and_unowned_fs_not_enumerated",
        "unknown_disposition": "not_classified_not_enumerated_not_cleaned",
        "canonical_transaction_reconciliation": "closed_typed_exact_roots",
        "full_reconciliation_complete": false,
        "remaining_candidates": "unknown",
        "fs_pg_atomic_rollback": false,
    }))
}

#[cfg(unix)]
fn ensure_gateway_stopped_for_artifact_cleanup(pid_path: &Path) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(pid_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(format!("local gateway PID read failed: {error}")),
    };
    if !file.metadata().map_err(stringify)?.is_file() {
        return Err("local gateway PID must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(65).read_to_end(&mut bytes).map_err(stringify)?;
    if bytes.len() > 64 {
        return Err("local gateway PID is invalid".into());
    }
    let value = std::str::from_utf8(&bytes).map_err(stringify)?.trim();
    if value.is_empty() {
        return Ok(());
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("local gateway PID is invalid".into());
    }
    let pid: i32 = value
        .parse()
        .map_err(|_| "local gateway PID is invalid".to_string())?;
    if pid <= 0 {
        return Err("local gateway PID must be positive".into());
    }
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Err("local gateway PID is alive; stop it before artifact cleanup".into());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(format!(
            "local gateway PID liveness could not be disproved: {error}"
        ))
    }
}

#[cfg(not(unix))]
fn ensure_gateway_stopped_for_artifact_cleanup(_pid_path: &Path) -> Result<(), String> {
    Err("strict local gateway PID checking is unavailable on this platform".into())
}

struct PostgresMaintenanceContext {
    config_home: PathBuf,
    workspace_root: PathBuf,
    runtime_config: runtime::RuntimeConfig,
}

impl PostgresMaintenanceContext {
    fn load() -> Result<Self, String> {
        let config_home = runtime::cowd_dirs::config_home_dir();
        let workspace_root = std::env::current_dir().map_err(stringify)?;
        let loaded = runtime::ConfigLoader::new(&workspace_root, &config_home)
            .load_with_diagnostics()
            .map_err(|error| format!("failed to load runtime configuration: {error}"))?;
        Ok(Self {
            config_home,
            workspace_root,
            runtime_config: loaded.config,
        })
    }

    fn postgres(&self) -> Result<&runtime::PostgresTopologyConfig, String> {
        self.runtime_config
            .storage()
            .postgres
            .as_ref()
            .ok_or_else(|| {
                "storage.postgres is required; PostgreSQL is the only supported database"
                    .to_string()
            })
    }

    fn plan(&self) -> Result<(), String> {
        let postgres = self.postgres()?;
        print_json(&serde_json::json!({
            "operation": "postgres_maintenance_plan",
            "backend": "postgres",
            "logical_identity": postgres.logical_identity,
            "secret_ref": postgres.secret_ref,
            "workspace": self.workspace_root,
            "commands": ["upgrade", "verify"],
            "fallback": serde_json::Value::Null,
            "historical_import": false,
        }))
    }

    fn upgrade(&self) -> Result<(), String> {
        self.postgres()?;
        ensure_gateway_stopped()?;
        let topology = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        print_json(&serde_json::json!({
            "operation": "postgres_schema_upgrade",
            "backend": topology.backend_label(),
            "logical_identity": topology.postgres_executor.logical_identity(),
            "gateway_stopped": true,
            "cowd_version": env!("CARGO_PKG_VERSION"),
            "status": "completed",
        }))
    }

    fn verify(&self) -> Result<(), String> {
        self.postgres()?;
        ensure_gateway_stopped()?;
        let topology = SelectedStorageTopology::compose_for_maintenance(
            self.runtime_config.storage(),
            &self.config_home,
            &self.workspace_root,
        )?;
        print_json(&serde_json::json!({
            "operation": "postgres_readiness_verify",
            "backend": topology.backend_label(),
            "logical_identity": topology.postgres_executor.logical_identity(),
            "migration_catalogs": "verified",
            "status": "ready",
        }))
    }

    fn status(&self) -> Result<(), String> {
        let postgres = self.postgres()?;
        print_json(&serde_json::json!({
            "configured_backend": "postgres",
            "logical_identity": postgres.logical_identity,
            "secret_ref": postgres.secret_ref,
            "historical_import": false,
            "fallback": serde_json::Value::Null,
        }))
    }

    fn cleanup(&self) -> Result<(), String> {
        let artifact_dir = std::env::var_os("COWD_BASH_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| self.config_home.join("storage").join("bash-artifacts"));
        let removed = cleanup_bash_artifacts(&artifact_dir, Duration::from_secs(7 * 24 * 3600))?;
        print_json(&serde_json::json!({
            "operation": "storage_cleanup",
            "artifact_dir": artifact_dir,
            "removed_files": removed,
            "ttl_days": 7,
        }))
    }

    fn cleanup_quiesced_artifacts(&self, evidence_path: &Path) -> Result<(), String> {
        let identity = &self.postgres()?.logical_identity;
        let report = execute_quiesced_artifact_cleanup(
            identity,
            &self.workspace_root,
            evidence_path,
            &self.config_home.join("run").join("cowd-serve.pid"),
            || {
                SelectedStorageTopology::compose_for_runtime(
                    self.runtime_config.storage(),
                    &self.config_home,
                    &self.workspace_root,
                )
            },
        )?;
        print_json(&report)
    }
}

fn ensure_gateway_stopped() -> Result<(), String> {
    let pid_path = crate::server::pid_file();
    let Ok(value) = fs::read_to_string(&pid_path) else {
        return Ok(());
    };
    let pid = value.trim();
    if pid.is_empty() {
        return Ok(());
    }
    if PathBuf::from("/proc").join(pid).exists() {
        return Err(format!(
            "Gateway pid {pid} is still running at {}; stop it before PostgreSQL maintenance",
            pid_path.display()
        ));
    }
    Ok(())
}

fn cleanup_bash_artifacts(dir: &Path, max_age: Duration) -> Result<usize, String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(0);
    };
    let cutoff = SystemTime::now()
        .checked_sub(max_age)
        .ok_or_else(|| "invalid cleanup age".to_string())?;
    let mut removed = 0usize;
    for entry in entries {
        let entry = entry.map_err(stringify)?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let metadata = entry.metadata().map_err(stringify)?;
        if metadata.modified().map_err(stringify)? < cutoff {
            fs::remove_file(&path).map_err(stringify)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn print_json(value: &serde_json::Value) -> Result<(), String> {
    let output = serde_json::to_string_pretty(value).map_err(stringify)?;
    println!("{output}");
    Ok(())
}

fn stringify(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[cfg(unix)]
    pub(crate) fn posix_scratch() -> tempfile::TempDir {
        let root = std::env::var_os("COWD_R1_06_POSIX_SCRATCH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/run/user/1000/k16"));
        tempfile::Builder::new()
            .prefix("maintenance-caller-")
            .tempdir_in(root)
            .expect("existing runtime POSIX scratch, not the fuseblk source or plan tree")
    }

    #[cfg(unix)]
    pub(crate) fn evidence_json(
        workspace: &Path,
        identity: &str,
        now: SystemTime,
    ) -> serde_json::Value {
        let now_ms = u64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        serde_json::json!({
            "schema_version": 1,
            "logical_identity": identity,
            "workspace": fs::canonicalize(workspace).unwrap().to_str().unwrap(),
            "all_publishers_stopped": true,
            "all_inflight_drained": true,
            "all_prepared_candidates_discarded": true,
            "quiescence_scope": QUIESCENCE_SCOPE,
            "maintain_quiescence_until_command_exit": true,
            "issued_at_unix_ms": now_ms,
            "expires_at_unix_ms": now_ms + 300_000,
            "evidence_ref": "private-maintenance-test",
        })
    }

    #[cfg(unix)]
    pub(crate) fn write_evidence(path: &Path, bytes: &[u8]) {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .expect("private evidence");
        file.write_all(bytes).unwrap();
    }

    #[test]
    fn grammar_is_closed_before_configuration_loading() {
        let parse = |args: &[&str]| {
            parse_request(&args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>())
        };
        assert_eq!(
            parse(&["cleanup"]).unwrap(),
            StorageRequest::Cleanup(CleanupRequest::BashTtl)
        );
        assert_eq!(
            parse(&["cleanup", "--quiescence-evidence", "/private/evidence"]).unwrap(),
            StorageRequest::Cleanup(CleanupRequest::QuiescedArtifacts {
                evidence_path: "/private/evidence".into()
            })
        );
        for command in ["plan", "upgrade", "verify", "status"] {
            assert!(parse(&[command]).is_ok());
            assert!(parse(&[command, "--quiescence-evidence", "/private/evidence"]).is_err());
        }
        for args in [
            vec![],
            vec!["unknown"],
            vec!["cleanup", "--quiescence-evidence"],
            vec!["cleanup", "--quiescence-evidence", "relative"],
            vec!["cleanup", "--quiescence-evidence", ""],
            vec!["cleanup", "--other", "/private/evidence"],
            vec!["cleanup", "--quiescence-evidence=/private/evidence"],
            vec![
                "cleanup",
                "--quiescence-evidence",
                "/first",
                "--quiescence-evidence",
                "/second",
            ],
            vec!["cleanup", "--quiescence-evidence", "/first", "extra"],
        ] {
            let args = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
            assert_eq!(run(&args).unwrap_err(), storage_usage());
        }
    }

    #[cfg(unix)]
    #[test]
    fn declaration_validates_closed_scope_exact_context_and_time_boundaries() {
        let root = posix_scratch();
        let workspace = fs::canonicalize(root.path()).unwrap();
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        let valid = evidence_json(&workspace, "Identity", now);
        let decode =
            |value: serde_json::Value| serde_json::from_value::<QuiescenceEvidence>(value).unwrap();
        decode(valid.clone())
            .validate("Identity", &workspace, now)
            .unwrap();
        assert!(decode(valid.clone())
            .validate("identity", &workspace, now)
            .is_err());
        assert!(decode(valid.clone())
            .validate("Identity ", &workspace, now)
            .is_err());
        assert!(decode(valid.clone())
            .validate("Identity", Path::new("/other"), now)
            .is_err());
        assert!(decode(valid.clone())
            .validate("Identity", &workspace, SystemTime::UNIX_EPOCH)
            .is_err());
        assert!(decode(valid.clone())
            .validate("Identity", &workspace, now - Duration::from_millis(1))
            .is_err());
        decode(valid.clone())
            .validate("Identity", &workspace, now + Duration::from_millis(299_999))
            .unwrap();
        assert!(decode(valid.clone())
            .validate("Identity", &workspace, now + Duration::from_millis(300_000))
            .is_err());
        let mut changes = vec![
            ("schema_version", serde_json::json!(2)),
            ("logical_identity", serde_json::json!("Identity ")),
            (
                "workspace",
                serde_json::json!(format!("{}/.", workspace.display())),
            ),
            ("workspace", serde_json::json!("relative")),
            ("all_publishers_stopped", serde_json::json!(false)),
            ("all_inflight_drained", serde_json::json!(false)),
            (
                "all_prepared_candidates_discarded",
                serde_json::json!(false),
            ),
            (
                "maintain_quiescence_until_command_exit",
                serde_json::json!(false),
            ),
            ("quiescence_scope", serde_json::json!("this_workspace_only")),
            ("issued_at_unix_ms", serde_json::json!(1_000_001)),
            ("expires_at_unix_ms", serde_json::json!(1_000_000)),
            ("expires_at_unix_ms", serde_json::json!(999_999)),
            ("expires_at_unix_ms", serde_json::json!(1_300_001)),
            ("expires_at_unix_ms", serde_json::json!(u64::MAX)),
            ("evidence_ref", serde_json::json!("")),
            ("evidence_ref", serde_json::json!("   ")),
            ("evidence_ref", serde_json::json!("line\nbreak")),
            ("evidence_ref", serde_json::json!("非ASCII")),
            ("evidence_ref", serde_json::json!("x".repeat(257))),
        ];
        for (field, value) in changes.drain(..) {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(
                decode(invalid)
                    .validate("Identity", &workspace, now)
                    .is_err(),
                "{field}"
            );
        }
        for field in valid.as_object().unwrap().keys() {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<QuiescenceEvidence>(missing).is_err(),
                "{field}"
            );
        }
        for (field, value) in [
            ("unexpected", serde_json::json!(true)),
            ("all_inflight_drained", serde_json::json!("true")),
        ] {
            let mut invalid = valid.clone();
            invalid[field] = value;
            assert!(serde_json::from_value::<QuiescenceEvidence>(invalid).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn secure_reader_hashes_same_fd_and_rejects_invalid_json_without_composition() {
        let root = posix_scratch();
        let workspace = fs::canonicalize(root.path()).unwrap();
        let now = SystemTime::now();
        let valid = serde_json::to_vec(&evidence_json(&workspace, "reader", now)).unwrap();
        let path = root.path().join("evidence");
        write_evidence(&path, &valid);
        let loaded = load_quiescence_evidence(&path, "reader", &workspace, now).unwrap();
        assert_eq!(loaded.sha256, format!("{:x}", Sha256::digest(&valid)));
        let mut exact_limit = valid.clone();
        exact_limit.resize(EVIDENCE_MAX_BYTES as usize, b' ');
        fs::write(&path, &exact_limit).unwrap();
        assert!(load_quiescence_evidence(&path, "reader", &workspace, now).is_ok());
        exact_limit.push(b' ');
        fs::write(&path, &exact_limit).unwrap();
        assert!(load_quiescence_evidence(&path, "reader", &workspace, now).is_err());
        for bytes in [
            b"{}".to_vec(),
            b"null".to_vec(),
            vec![0xff],
            [valid.clone(), b" {}".to_vec()].concat(),
            [
                valid[..valid.len() - 1].to_vec(),
                b",\"schema_version\":1}".to_vec(),
            ]
            .concat(),
            [
                valid[..valid.len() - 1].to_vec(),
                b",\"delete_list\":[]}".to_vec(),
            ]
            .concat(),
            vec![b' '; EVIDENCE_MAX_BYTES as usize + 1],
        ] {
            fs::write(&path, &bytes).unwrap();
            let result = execute_quiesced_artifact_cleanup(
                "reader",
                &workspace,
                &path,
                &root.path().join("absent-pid"),
                || panic!("rejected evidence must not compose or delete"),
            );
            assert!(result.is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[cfg(unix)]
    #[test]
    fn secure_reader_rejects_permissions_links_special_files_and_unsafe_components() {
        use std::ffi::CString;
        use std::os::unix::{
            ffi::OsStrExt,
            fs::{symlink, MetadataExt, PermissionsExt},
        };

        let root = posix_scratch();
        let workspace = fs::canonicalize(root.path()).unwrap();
        let now = SystemTime::now();
        let path = root.path().join("evidence");
        write_evidence(
            &path,
            &serde_json::to_vec(&evidence_json(&workspace, "paths", now)).unwrap(),
        );
        let load = |path: &Path| load_quiescence_evidence(path, "paths", &workspace, now);
        for mode in [0o644, 0o400, 0o660, 0o1600, 0o2600, 0o4600] {
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            assert!(load(&path).is_err(), "{mode:o}");
            assert_eq!(fs::metadata(&path).unwrap().mode() & 0o7777, mode);
        }
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let linked = root.path().join("linked");
        symlink(&path, &linked).unwrap();
        assert!(load(&linked).is_err());
        let ancestor = root.path().join("ancestor");
        symlink(root.path(), &ancestor).unwrap();
        assert!(load(&ancestor.join("evidence")).is_err());
        let hardlink = root.path().join("hardlink");
        fs::hard_link(&path, &hardlink).unwrap();
        assert!(load(&path).is_err());
        fs::remove_file(hardlink).unwrap();
        let fifo = root.path().join("fifo");
        let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);
        assert!(load(&fifo).is_err());
        let socket_path = root.path().join("socket");
        let _socket = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        assert!(load(&socket_path).is_err());
        assert!(load(root.path()).is_err());
        assert!(load(Path::new("/dev/null")).is_err());
        assert!(load(Path::new("relative")).is_err());
        assert!(load(&root.path().join("../escape")).is_err());
        assert!(load(&root.path().join("missing")).is_err());
        assert!(load(&path).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn opened_fd_cannot_be_redirected_and_metadata_changes_fail_closed() {
        use std::os::unix::fs::PermissionsExt;

        let root = posix_scratch();
        let directory = root.path().join("directory");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("evidence");
        write_evidence(&path, b"original");
        let mut file = open_evidence_file(&path).unwrap();
        let before = file.metadata().unwrap();
        fs::rename(&directory, root.path().join("moved")).unwrap();
        fs::create_dir(&directory).unwrap();
        write_evidence(&path, b"replacement");
        assert_eq!(
            read_stable_evidence(&mut file, &before).unwrap(),
            b"original"
        );
        for same_size in [false, true] {
            let mut file = open_evidence_file(&path).unwrap();
            let before = file.metadata().unwrap();
            if same_size {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
            } else {
                fs::write(&path, b"changed length").unwrap();
            }
            assert!(read_stable_evidence(&mut file, &before).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn strict_local_pid_gate_rejects_invalid_alive_and_io_failures() {
        use std::os::unix::fs::symlink;

        let root = posix_scratch();
        let path = root.path().join("pid");
        ensure_gateway_stopped_for_artifact_cleanup(&path).unwrap();
        for value in ["", " \n"] {
            fs::write(&path, value).unwrap();
            ensure_gateway_stopped_for_artifact_cleanup(&path).unwrap();
        }
        for value in ["0", "-1", "+1", "../1", "abc", "1\n2", "2147483648"] {
            fs::write(&path, value).unwrap();
            assert!(ensure_gateway_stopped_for_artifact_cleanup(&path).is_err());
        }
        fs::write(&path, vec![b'1'; 65]).unwrap();
        assert!(ensure_gateway_stopped_for_artifact_cleanup(&path).is_err());
        fs::write(&path, [0xff]).unwrap();
        assert!(ensure_gateway_stopped_for_artifact_cleanup(&path).is_err());
        fs::write(&path, std::process::id().to_string()).unwrap();
        assert!(ensure_gateway_stopped_for_artifact_cleanup(&path)
            .unwrap_err()
            .contains("alive"));
        assert!(ensure_gateway_stopped_for_artifact_cleanup(root.path()).is_err());
        fs::remove_file(&path).unwrap();
        symlink(root.path().join("absent"), &path).unwrap();
        assert!(ensure_gateway_stopped_for_artifact_cleanup(&path).is_err());
        let mut child = std::process::Command::new("/bin/true").spawn().unwrap();
        let exited_pid = child.id();
        child.wait().unwrap();
        fs::remove_file(&path).unwrap();
        fs::write(&path, exited_pid.to_string()).unwrap();
        ensure_gateway_stopped_for_artifact_cleanup(&path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn live_local_gateway_blocks_before_composition() {
        let root = posix_scratch();
        let path = root.path().join("evidence");
        write_evidence(
            &path,
            &serde_json::to_vec(&evidence_json(root.path(), "stopped", SystemTime::now())).unwrap(),
        );
        let pid = root.path().join("pid");
        fs::write(&pid, std::process::id().to_string()).unwrap();
        assert!(
            execute_quiesced_artifact_cleanup("stopped", root.path(), &path, &pid, || panic!(
                "local gateway must stop before composing"
            ))
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires root/chown capability on private runtime POSIX scratch"]
    fn secure_reader_rejects_actual_foreign_owner_without_chown_repair() {
        use std::ffi::CString;
        use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};

        assert_eq!(
            unsafe { libc::geteuid() },
            0,
            "explicit capability prerequisite"
        );
        let root = posix_scratch();
        let path = root.path().join("foreign");
        write_evidence(&path, b"{}");
        let name = CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::chown(name.as_ptr(), 65534, !0) }, 0);
        let mut file = open_evidence_file(&path).unwrap();
        let before = file.metadata().unwrap();
        assert_eq!(before.uid(), 65534);
        assert!(read_stable_evidence(&mut file, &before).is_err());
        assert_eq!(fs::metadata(&path).unwrap().uid(), 65534);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires non-root euid and private runtime POSIX scratch"]
    fn strict_local_pid_gate_does_not_treat_permission_denied_as_absent() {
        use std::os::unix::fs::PermissionsExt;

        assert_ne!(
            unsafe { libc::geteuid() },
            0,
            "explicit unprivileged prerequisite"
        );
        let root = posix_scratch();
        let path = root.path().join("denied-pid");
        fs::write(&path, "123").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        assert!(ensure_gateway_stopped_for_artifact_cleanup(&path)
            .unwrap_err()
            .contains("read failed"));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, "1").unwrap();
        assert!(ensure_gateway_stopped_for_artifact_cleanup(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn bash_cleanup_removes_only_expired_files_without_postgres() {
        let root = posix_scratch();
        let old = root.path().join("old");
        fs::write(&old, b"expired").unwrap();
        fs::File::options()
            .write(true)
            .open(&old)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(SystemTime::UNIX_EPOCH))
            .unwrap();
        fs::write(root.path().join("fresh"), b"keep").unwrap();
        fs::create_dir(root.path().join("directory")).unwrap();
        assert_eq!(
            cleanup_bash_artifacts(root.path(), Duration::from_secs(7 * 24 * 3600)).unwrap(),
            1
        );
        assert!(!old.exists());
        assert!(root.path().join("fresh").exists());
        assert!(root.path().join("directory").is_dir());
    }

    #[test]
    fn cleanup_keeps_fresh_regular_files() {
        let root = tempfile::tempdir().expect("tempdir");
        fs::write(root.path().join("fresh"), "keep").expect("fresh");
        assert_eq!(
            cleanup_bash_artifacts(root.path(), Duration::from_secs(24 * 3600)).expect("cleanup"),
            0
        );
        assert!(root.path().join("fresh").exists());
    }
}
