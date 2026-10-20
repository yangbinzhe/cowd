use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use base64::Engine;
use harness_contract::context::{
    ArtifactRef, EvidenceTargetIdentity, ObservedEvidence, ObservedEvidenceProvenance,
    ToolExecutionAdmission, ToolExecutionBinding, ToolExecutionFact, ToolExecutionInput,
    ToolExecutionStopReason, ToolExecutionTerminal, ToolOutputDraft, ToolProcessClosure,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::host::ToolHostError;

const MAX_FILES: usize = 64;
const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BUNDLE_BYTES: usize = 17 * 1024 * 1024;
const MAX_BODY_BYTES: usize = 6 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 128 * 1024;
const CODE_FILE: &str = ".cowd-bounded-code";

#[cfg(test)]
thread_local! {
    static PREPARE_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(&str, &Path)>>> = std::cell::RefCell::new(None);
}

fn source_boundary(stage: &str, path: &Path) {
    #[cfg(test)]
    PREPARE_HOOK.with(|hook| {
        if let Some(callback) = hook.borrow_mut().as_mut() {
            callback(stage, path);
        }
    });
    let _ = (stage, path);
}

pub struct PreparedToolExecution {
    state: PreparedState,
}

pub struct CompletedToolExecution {
    state: CompletedState,
}

#[derive(Clone)]
pub struct ToolExecutionCancellation {
    state: Arc<AtomicBool>,
}

impl Default for ToolExecutionCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl ToolExecutionCancellation {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.state.store(true, Ordering::Release);
    }
}

struct PreparedState {
    binding: ToolExecutionBinding,
    snapshot: Snapshot,
}

struct CompletedState {
    admission: ToolExecutionAdmission,
    terminal: ToolExecutionTerminal,
    body: Vec<u8>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    capture_complete: bool,
    environment: Option<Value>,
}

struct Snapshot {
    scratch: OwnedScratch,
    source_root: PathBuf,
    inputs: Vec<ToolExecutionInput>,
    bundle: Vec<u8>,
    code_sha256: String,
    code: Vec<u8>,
    language: String,
    timeout: Duration,
    authorization_deadline: Option<Instant>,
    seals: BTreeMap<PathBuf, (u64, u64, u64, u64, i64, i64, i64, i64)>,
}

struct OwnedScratch {
    path: PathBuf,
    directory: File,
}

impl Drop for OwnedScratch {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&self.path, fs::Permissions::from_mode(0o700));
            for entry in walkdir::WalkDir::new(&self.path)
                .into_iter()
                .filter_map(Result::ok)
            {
                if entry.file_type().is_dir() {
                    let _ = fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o700));
                }
            }
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn failure(error: impl ToString) -> ToolHostError {
    ToolHostError::Execution(error.to_string())
}

fn bounded_json_bytes(
    value: &impl serde::Serialize,
    limit: usize,
) -> Result<Vec<u8>, ToolHostError> {
    struct BudgetWriter {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for BudgetWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("bounded_json_budget_exceeded"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BudgetWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(failure)?;
    Ok(writer.bytes)
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn artifact_matches(artifact: &ArtifactRef, bytes: &[u8], media_type: &str) -> bool {
    artifact.is_durable()
        && !artifact.selector.is_empty()
        && artifact.selector.len() <= 2048
        && !artifact.visibility_scope.is_empty()
        && artifact.media_type == media_type
        && artifact.bytes == bytes.len() as u64
        && artifact
            .sha256
            .strip_prefix("sha256:")
            .is_some_and(|original| original == digest(bytes))
}

pub(crate) fn prepare(
    workspace: &Path,
    input: &Value,
    binding: ToolExecutionBinding,
    root_capability: Option<&File>,
) -> Result<PreparedToolExecution, ToolHostError> {
    let (_, _, paths, _) = parse_input(input)?;
    let binding_bytes = bounded_json_bytes(&binding, MAX_METADATA_BYTES - 8192)?.len();
    let metadata_ceiling = paths.iter().try_fold(binding_bytes + 8192, |total, path| {
        let encoded = bounded_json_bytes(path, 8192)?;
        Ok::<usize, ToolHostError>(total.saturating_add(encoded.len() + 320))
    })?;
    if metadata_ceiling > MAX_METADATA_BYTES {
        return Err(failure("bounded_fact_metadata_limit"));
    }
    let input_digest = harness_contract::step::canonical_digest(
        harness_contract::context::TOOL_EXECUTION_INPUT_DOMAIN,
        input,
    )
    .map_err(failure)?;
    let input_sha256 = input_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| failure("bounded_input_digest_invalid"))?;
    if binding.call_key.is_empty()
        || binding.tool_use_id.is_empty()
        || binding.authority_sha256.len() != 64
        || !binding
            .authority_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || binding.descriptor_sha256.len() != 64
        || !binding
            .descriptor_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || binding.input_sha256 != input_sha256
        || bounded_json_bytes(&binding, MAX_METADATA_BYTES - 8192).is_err()
    {
        return Err(failure("bounded_process_binding_mismatch"));
    }
    Ok(PreparedToolExecution {
        state: PreparedState {
            binding,
            snapshot: prepare_snapshot(workspace, input, root_capability, &std::env::temp_dir())?,
        },
    })
}

impl PreparedToolExecution {
    pub(crate) fn restrict_deadline(mut self, deadline: Instant) -> Self {
        self.state.snapshot.authorization_deadline = Some(
            self.state
                .snapshot
                .authorization_deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
        self
    }
    pub fn binding(&self) -> &ToolExecutionBinding {
        &self.state.binding
    }

    pub fn workspace_root(&self) -> &Path {
        &self.state.snapshot.source_root
    }

    pub fn inputs(&self) -> &[ToolExecutionInput] {
        &self.state.snapshot.inputs
    }

    pub fn source_bundle_bytes(&self) -> &[u8] {
        &self.state.snapshot.bundle
    }

    pub fn admission(
        &self,
        source_bundle: &ArtifactRef,
    ) -> Result<ToolExecutionAdmission, ToolHostError> {
        if !artifact_matches(
            source_bundle,
            &self.state.snapshot.bundle,
            "application/octet-stream",
        ) {
            return Err(failure("bounded_source_archive_mismatch"));
        }
        let admission = ToolExecutionAdmission {
            version: 1,
            binding: self.state.binding.clone(),
            code_sha256: self.state.snapshot.code_sha256.clone(),
            inputs: self.state.snapshot.inputs.clone(),
            source_bundle: source_bundle.clone(),
        };
        if bounded_json_bytes(&admission, MAX_METADATA_BYTES - 8192).is_err() {
            return Err(failure("bounded_admission_metadata_limit"));
        }
        Ok(admission)
    }

    pub fn run(
        self,
        admission: ToolExecutionAdmission,
        cancellation: &ToolExecutionCancellation,
    ) -> CompletedToolExecution {
        let valid = self
            .admission(&admission.source_bundle)
            .is_ok_and(|expected| expected == admission);
        let environment = valid.then(|| admitted_environment(&self.state.snapshot));
        let result = if !valid {
            Err("bounded_admission_mismatch".to_string())
        } else if cancellation.state.load(Ordering::Acquire) {
            Err("bounded_cancelled_before_spawn".to_string())
        } else {
            run_snapshot(&self.state.snapshot, &admission, &cancellation.state)
        };
        let (mut terminal, stdout, stderr, mut capture_complete) = match result {
            Ok(observation) => {
                let terminal = terminal_from_observation(&observation);
                (
                    terminal,
                    observation.stdout().to_vec(),
                    observation.stderr().to_vec(),
                    observation.capture_complete(),
                )
            }
            Err(reason_code) => (
                ToolExecutionTerminal::NotStarted { reason_code },
                Vec::new(),
                Vec::new(),
                false,
            ),
        };
        let admission_digest = match serde_json::to_vec(&admission) {
            Ok(bytes) => Value::String(digest(&bytes)),
            Err(_) => {
                terminal = ToolExecutionTerminal::Unknown {
                    reason_code: "bounded_admission_encoding_failed".into(),
                };
                capture_complete = false;
                Value::Null
            }
        };
        let mut body_value = json!({
            "version": 1,
            "admission_sha256": admission_digest,
            "terminal": terminal,
            "stdout_base64": base64::engine::general_purpose::STANDARD.encode(&stdout),
            "stderr_base64": base64::engine::general_purpose::STANDARD.encode(&stderr),
            "stdout_sha256": digest(&stdout),
            "stderr_sha256": digest(&stderr),
            "stdout_bytes": stdout.len(),
            "stderr_bytes": stderr.len(),
            "capture_complete": capture_complete,
        });
        for (key, bytes) in [("stdout_utf8", &stdout), ("stderr_utf8", &stderr)] {
            if capture_complete && bytes.len() <= 131072 {
                if let Ok(text) = std::str::from_utf8(bytes) {
                    body_value[key] = Value::String(text.to_owned());
                }
            }
        }
        let body = body_value.to_string().into_bytes();
        CompletedToolExecution {
            state: CompletedState {
                admission,
                terminal,
                body,
                stdout,
                stderr,
                capture_complete,
                environment,
            },
        }
    }
}

impl CompletedToolExecution {
    pub fn admission(&self) -> &ToolExecutionAdmission {
        &self.state.admission
    }

    pub fn terminal(&self) -> &ToolExecutionTerminal {
        &self.state.terminal
    }

    pub fn body_bytes(&self) -> &[u8] {
        &self.state.body
    }

    pub fn into_output(self, body: ArtifactRef) -> Result<ToolOutputDraft, ToolHostError> {
        if self.state.body.len() > MAX_BODY_BYTES
            || !artifact_matches(&body, &self.state.body, "application/json")
        {
            return Err(failure("bounded_body_archive_mismatch"));
        }
        let fact = ToolExecutionFact {
            version: 1,
            admission: self.state.admission,
            terminal: self.state.terminal,
            body: body.clone(),
            stdout_sha256: digest(&self.state.stdout),
            stderr_sha256: digest(&self.state.stderr),
            stdout_bytes: self.state.stdout.len() as u64,
            stderr_bytes: self.state.stderr.len() as u64,
            capture_complete: self.state.capture_complete,
        };
        if bounded_json_bytes(&fact, MAX_METADATA_BYTES).is_err() {
            return Err(failure("bounded_fact_metadata_limit"));
        }
        let summary = summary(
            &self.state.stdout,
            &self.state.stderr,
            &fact.terminal,
            Some(&body.selector),
            self.state.environment.as_ref(),
        );
        Ok(ToolOutputDraft::ObservedArtifact {
            summary,
            artifact_ref: body,
            observations: vec![ObservedEvidence {
                obligation_id: String::new(),
                target: EvidenceTargetIdentity::ToolExecution { execution: fact },
                observed_at_sequence: 0,
                tool_name: "execute_code".into(),
                provenance: ObservedEvidenceProvenance::FreshExecution,
                evidence_ref: None,
                model_observation: None,
                workspace_prior_state: None,
            }],
        })
    }
}

fn terminal_from_observation(
    observation: &sandbox_launcher::BoundedSandboxObservation,
) -> ToolExecutionTerminal {
    if !observation.started() {
        let reason_code = observation
            .reason_code()
            .char_indices()
            .take_while(|(offset, character)| offset + character.len_utf8() <= 128)
            .map(|(_, character)| character)
            .collect();
        return if matches!(
            observation.reason_code(),
            "bounded_cancelled_before_spawn"
                | "bounded_launcher_start_failed"
                | "bounded_not_started"
                | "bounded_linux_required"
        ) {
            ToolExecutionTerminal::NotStarted { reason_code }
        } else {
            ToolExecutionTerminal::Unknown { reason_code }
        };
    }
    let Some(profile) = observation.applied_profile() else {
        return ToolExecutionTerminal::Unknown {
            reason_code: "bounded_missing_applied_profile".into(),
        };
    };
    let Some((namespace_inode, init_host_pid, init_start_ticks)) = observation.identity() else {
        return ToolExecutionTerminal::Unknown {
            reason_code: "bounded_missing_process_identity".into(),
        };
    };
    let stop_reason = match observation.stop_reason() {
        "command_exit" => ToolExecutionStopReason::CommandExit,
        "timeout" => ToolExecutionStopReason::Timeout,
        "cancelled" => ToolExecutionStopReason::Cancelled,
        "capture_limit" => ToolExecutionStopReason::CaptureLimit,
        _ => {
            return ToolExecutionTerminal::Unknown {
                reason_code: "bounded_invalid_stop_reason".into(),
            };
        }
    };
    if !observation.reason_code().is_empty()
        || !observation.all_children_reaped()
        || !observation.init_reaped()
        || !observation.status_channel_protected()
        || !profile.kernel_hardening_applied
        || profile.network_enabled
        || profile.workspace_access != sandbox_launcher::SandboxWorkspaceAccess::ReadOnly
        || observation.exit_code().is_some() == observation.signal().is_some()
    {
        return ToolExecutionTerminal::Unknown {
            reason_code: "bounded_unproven_settlement".into(),
        };
    }
    ToolExecutionTerminal::Settled {
        exit_code: observation.exit_code(),
        signal: observation.signal(),
        closure: ToolProcessClosure {
            backend: profile.backend.into(),
            namespace_inode,
            init_host_pid,
            init_start_ticks,
            all_children_reaped: true,
            init_reaped: true,
            kernel_hardening_applied: true,
            network_disabled: true,
            project_read_only: true,
            status_channel_protected: true,
        },
        stop_reason,
    }
}

fn parse_input(input: &Value) -> Result<(String, Vec<u8>, Vec<String>, Duration), ToolHostError> {
    let object = input
        .as_object()
        .ok_or_else(|| failure("bounded_input_object_required"))?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "language" | "code" | "input_files" | "timeout_ms"
        )
    }) {
        return Err(failure("bounded_unknown_input_field"));
    }
    let language = input
        .get("language")
        .and_then(Value::as_str)
        .ok_or_else(|| failure("bounded_language_required"))?;
    if !matches!(language, "python" | "javascript" | "bash") {
        return Err(failure("bounded_language_unsupported"));
    }
    let code = input
        .get("code")
        .and_then(Value::as_str)
        .ok_or_else(|| failure("bounded_code_required"))?;
    if code.len() > 65536 {
        return Err(failure("bounded_code_limit"));
    }
    let files = input
        .get("input_files")
        .and_then(Value::as_array)
        .ok_or_else(|| failure("bounded_input_files_required_replan"))?;
    if files.len() > MAX_FILES {
        return Err(failure("bounded_input_count_limit"));
    }
    let mut paths = BTreeSet::new();
    for value in files {
        let path = value
            .as_str()
            .ok_or_else(|| failure("bounded_input_path_required"))?;
        if path.is_empty()
            || path.len() > 1024
            || path.contains('\\')
            || path.contains('\0')
            || path
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || Path::new(path)
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
            || path.split('/').next() == Some(CODE_FILE)
            || !paths.insert(path.to_string())
        {
            return Err(failure("bounded_input_path_invalid"));
        }
    }
    let requested = match input.get("timeout_ms") {
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=120000).contains(value))
            .ok_or_else(|| failure("bounded_timeout_invalid"))?,
        None => 30000,
    };
    let configured = std::env::var("COWD_EXEC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.saturating_mul(1000))
        .unwrap_or(120000)
        .clamp(1, 120000);
    bounded_json_bytes(input, 131072)?;
    Ok((
        language.into(),
        code.as_bytes().to_vec(),
        paths.into_iter().collect(),
        Duration::from_millis(requested.min(configured)),
    ))
}

fn prepare_snapshot(
    workspace: &Path,
    input: &Value,
    root_capability: Option<&File>,
    temp_root: &Path,
) -> Result<Snapshot, ToolHostError> {
    let (language, code, paths, timeout) = parse_input(input)?;
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (workspace, language, code, paths, timeout, root_capability);
        Err(failure("bounded_linux_required"))
    }
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{openat2, Mode, OFlags, ResolveFlags};
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        let started = Instant::now();
        let root = workspace.canonicalize().map_err(failure)?;
        reject_system_overlap(&root)?;
        let root_file = if let Some(capability) = root_capability {
            capability.try_clone().map_err(failure)?
        } else {
            File::from(
                openat2(
                    rustix::fs::CWD,
                    root.as_path(),
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                    Mode::empty(),
                    ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
                )
                .map_err(failure)?,
            )
        };
        let root_identity = root_file.metadata().map_err(failure)?;
        let current_root = fs::symlink_metadata(&root).map_err(failure)?;
        if !current_root.is_dir()
            || current_root.dev() != root_identity.dev()
            || current_root.ino() != root_identity.ino()
        {
            return Err(failure("bounded_original_workspace_changed"));
        }
        let path_policy = crate::path_policy::WorkspacePathPolicy::new(&root);
        // Production points TMPDIR through a short-alias symlink (Unix socket
        // path bound); resolve it once so the scratch open below never
        // traverses a symlink and fails its own NO_SYMLINKS contract.
        let temp_root = temp_root.canonicalize().map_err(failure)?;
        let scratch_path = temp_root.join(format!("cowd-bounded-{}", uuid::Uuid::new_v4()));
        reject_system_overlap(&scratch_path)?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&scratch_path)
            .map_err(failure)?;
        let directory = File::from(
            openat2(
                rustix::fs::CWD,
                scratch_path.as_path(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
                ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
            )
            .map_err(failure)?,
        );
        let scratch = OwnedScratch {
            path: scratch_path,
            directory,
        };
        let mut inputs = Vec::with_capacity(paths.len());
        let mut payloads = Vec::with_capacity(paths.len());
        let mut total = 0_u64;
        for relative_path in paths {
            if started.elapsed() > Duration::from_secs(10) {
                return Err(failure("bounded_prepare_deadline"));
            }
            path_policy.resolve(&relative_path).map_err(failure)?;
            let source_fd = openat2(
                &root_file,
                relative_path.as_str(),
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
                Mode::empty(),
                ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
            )
            .map_err(failure)?;
            let mut source = File::from(source_fd);
            source_boundary("source_opened", &root.join(&relative_path));
            let before = source.metadata().map_err(failure)?;
            if !before.is_file() || before.nlink() != 1 || before.len() > MAX_FILE_BYTES {
                return Err(failure("bounded_source_not_single_regular_file"));
            }
            total = total
                .checked_add(before.len())
                .ok_or_else(|| failure("bounded_source_total_limit"))?;
            if total > MAX_TOTAL_BYTES {
                return Err(failure("bounded_source_total_limit"));
            }
            let mut bytes = Vec::with_capacity(before.len() as usize);
            Read::by_ref(&mut source)
                .take(MAX_FILE_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(failure)?;
            if bytes.len() as u64 != before.len() {
                return Err(failure("bounded_source_changed"));
            }
            source_boundary("source_read", &root.join(&relative_path));
            let after = source.metadata().map_err(failure)?;
            if source_identity(&before) != source_identity(&after) {
                return Err(failure("bounded_source_changed"));
            }
            source.seek(SeekFrom::Start(0)).map_err(failure)?;
            let mut reread = Vec::with_capacity(bytes.len());
            Read::by_ref(&mut source)
                .take(MAX_FILE_BYTES + 1)
                .read_to_end(&mut reread)
                .map_err(failure)?;
            if bytes != reread
                || source_identity(&before) != source_identity(&source.metadata().map_err(failure)?)
            {
                return Err(failure("bounded_source_changed"));
            }
            source_boundary("source_reread", &root.join(&relative_path));
            let current = File::from(
                openat2(
                    &root_file,
                    relative_path.as_str(),
                    OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
                    Mode::empty(),
                    ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
                )
                .map_err(failure)?,
            );
            if source_identity(&before) != source_identity(&current.metadata().map_err(failure)?) {
                return Err(failure("bounded_source_path_changed"));
            }
            let target = scratch.path.join(&relative_path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(failure)?;
            }
            let mut destination = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)
                .map_err(failure)?;
            destination.write_all(&bytes).map_err(failure)?;
            destination.sync_all().map_err(failure)?;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o400)).map_err(failure)?;
            inputs.push(ToolExecutionInput {
                relative_path,
                sha256: digest(&bytes),
                bytes: bytes.len() as u64,
                device: before.dev(),
                inode: before.ino(),
                mtime_ns: before
                    .mtime()
                    .checked_mul(1_000_000_000)
                    .and_then(|value| value.checked_add(before.mtime_nsec()))
                    .ok_or_else(|| failure("bounded_source_timestamp_out_of_range"))?,
                ctime_ns: before
                    .ctime()
                    .checked_mul(1_000_000_000)
                    .and_then(|value| value.checked_add(before.ctime_nsec()))
                    .ok_or_else(|| failure("bounded_source_timestamp_out_of_range"))?,
            });
            payloads.push(bytes);
        }
        source_boundary("root_check", &root);
        if source_identity(&root_identity)
            != source_identity(&fs::metadata(&root).map_err(failure)?)
        {
            return Err(failure("bounded_source_root_changed"));
        }
        let code_path = scratch.path.join(CODE_FILE);
        let mut code_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&code_path)
            .map_err(failure)?;
        code_file.write_all(&code).map_err(failure)?;
        code_file.sync_all().map_err(failure)?;
        fs::set_permissions(&code_path, fs::Permissions::from_mode(0o400)).map_err(failure)?;
        let code_sha256 = digest(&code);
        let manifest = serde_json::to_vec(&json!({
            "code_sha256": code_sha256,
            "inputs": inputs.iter().map(|item| json!({"path":item.relative_path,"sha256":item.sha256,"bytes":item.bytes})).collect::<Vec<_>>(),
        })).map_err(failure)?;
        let capacity = 8
            + 4
            + manifest.len()
            + 8
            + code.len()
            + payloads.iter().map(|bytes| 8 + bytes.len()).sum::<usize>();
        if capacity > MAX_BUNDLE_BYTES {
            return Err(failure("bounded_bundle_limit"));
        }
        let mut bundle = Vec::with_capacity(capacity);
        bundle.extend_from_slice(b"COWD-BP1");
        bundle.extend_from_slice(&(manifest.len() as u32).to_be_bytes());
        bundle.extend_from_slice(&manifest);
        bundle.extend_from_slice(&(code.len() as u64).to_be_bytes());
        bundle.extend_from_slice(&code);
        for bytes in payloads {
            bundle.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            bundle.extend_from_slice(&bytes);
        }
        for entry in walkdir::WalkDir::new(&scratch.path).into_iter() {
            let entry = entry.map_err(failure)?;
            if entry.file_type().is_dir() {
                fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o500))
                    .map_err(failure)?;
            }
        }
        let mut seals = BTreeMap::new();
        for entry in walkdir::WalkDir::new(&scratch.path).follow_links(false) {
            let entry = entry.map_err(failure)?;
            let relative = entry
                .path()
                .strip_prefix(&scratch.path)
                .map_err(failure)?
                .to_path_buf();
            seals.insert(
                relative,
                source_identity(&fs::symlink_metadata(entry.path()).map_err(failure)?),
            );
        }
        if started.elapsed() > Duration::from_secs(10) {
            return Err(failure("bounded_prepare_deadline"));
        }
        Ok(Snapshot {
            scratch,
            source_root: root,
            inputs,
            bundle,
            code_sha256,
            code,
            language,
            timeout,
            authorization_deadline: None,
            seals,
        })
    }
}

#[cfg(target_os = "linux")]
fn source_identity(metadata: &fs::Metadata) -> (u64, u64, u64, u64, i64, i64, i64, i64) {
    use std::os::unix::fs::MetadataExt;
    // Directory sizes are filesystem-defined and unstable on FUSE-backed mounts
    // (they flap between observations without any content change), so they carry
    // no integrity signal for directories; file sizes remain fully compared.
    let len = if metadata.is_dir() {
        0
    } else {
        metadata.len()
    };
    (
        metadata.dev(),
        metadata.ino(),
        metadata.nlink(),
        len,
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

fn reject_system_overlap(project: &Path) -> Result<(), ToolHostError> {
    let project = if project.exists() {
        project.canonicalize().map_err(failure)?
    } else {
        let parent = project
            .parent()
            .ok_or_else(|| failure("bounded_project_root_invalid"))?
            .canonicalize()
            .map_err(failure)?;
        parent.join(
            project
                .file_name()
                .ok_or_else(|| failure("bounded_project_root_invalid"))?,
        )
    };
    for name in ["/usr", "/bin", "/lib", "/lib64", "/etc"] {
        let root = Path::new(name);
        for system in [
            root.to_path_buf(),
            root.canonicalize().unwrap_or_else(|_| root.to_path_buf()),
        ] {
            if project.starts_with(&system) || system.starts_with(&project) {
                return Err(failure("bounded_project_system_root_overlap"));
            }
        }
    }
    Ok(())
}

fn validate_snapshot(snapshot: &Snapshot) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        for (path, expected) in &snapshot.seals {
            let current = fs::symlink_metadata(snapshot.scratch.path.join(path))
                .map_err(|_| "bounded_snapshot_changed")?;
            if &source_identity(&current) != expected {
                return Err("bounded_snapshot_changed".into());
            }
        }
        if source_identity(
            &snapshot
                .scratch
                .directory
                .metadata()
                .map_err(|_| "bounded_snapshot_changed")?,
        ) != source_identity(
            &fs::symlink_metadata(&snapshot.scratch.path)
                .map_err(|_| "bounded_snapshot_changed")?,
        ) {
            return Err("bounded_snapshot_root_changed".into());
        }
    }
    let mut expected = BTreeSet::from([CODE_FILE.to_string()]);
    for input in &snapshot.inputs {
        let bytes = read_snapshot_file(snapshot, &input.relative_path, input.bytes)?;
        if bytes.len() as u64 != input.bytes || digest(&bytes) != input.sha256 {
            return Err("bounded_snapshot_changed".into());
        }
        expected.insert(input.relative_path.clone());
    }
    if read_snapshot_file(snapshot, CODE_FILE, snapshot.code.len() as u64)? != snapshot.code {
        return Err("bounded_snapshot_changed".into());
    }
    for entry in walkdir::WalkDir::new(&snapshot.scratch.path).follow_links(false) {
        let entry = entry.map_err(|_| "bounded_snapshot_changed")?;
        if entry.file_type().is_symlink() {
            return Err("bounded_snapshot_changed".into());
        }
        if entry.file_type().is_file() {
            let relative = entry
                .path()
                .strip_prefix(&snapshot.scratch.path)
                .map_err(|_| "bounded_snapshot_changed")?
                .to_str()
                .ok_or("bounded_snapshot_changed")?
                .to_string();
            if !expected.remove(&relative) {
                return Err("bounded_snapshot_changed".into());
            }
        }
    }
    if !expected.is_empty() {
        return Err("bounded_snapshot_changed".into());
    }
    Ok(())
}

fn read_snapshot_file(snapshot: &Snapshot, path: &str, bytes: u64) -> Result<Vec<u8>, String> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (snapshot, path, bytes);
        Err("bounded_linux_required".into())
    }
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{openat2, Mode, OFlags, ResolveFlags};
        use std::os::unix::fs::MetadataExt;
        let descriptor = openat2(
            &snapshot.scratch.directory,
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
            ResolveFlags::BENEATH | ResolveFlags::NO_SYMLINKS | ResolveFlags::NO_MAGICLINKS,
        )
        .map_err(|_| "bounded_snapshot_changed")?;
        let mut file = File::from(descriptor);
        let before = file.metadata().map_err(|_| "bounded_snapshot_changed")?;
        if !before.is_file()
            || before.nlink() != 1
            || before.len() != bytes
            || bytes > MAX_FILE_BYTES
        {
            return Err("bounded_snapshot_changed".into());
        }
        let mut content = Vec::with_capacity(bytes as usize);
        Read::by_ref(&mut file)
            .take(bytes + 1)
            .read_to_end(&mut content)
            .map_err(|_| "bounded_snapshot_changed")?;
        if content.len() as u64 != bytes
            || source_identity(&before)
                != source_identity(&file.metadata().map_err(|_| "bounded_snapshot_changed")?)
        {
            return Err("bounded_snapshot_changed".into());
        }
        Ok(content)
    }
}

fn run_snapshot(
    snapshot: &Snapshot,
    admission: &ToolExecutionAdmission,
    cancellation: &AtomicBool,
) -> Result<sandbox_launcher::BoundedSandboxObservation, String> {
    let bytes = serde_json::to_vec(admission).map_err(|_| "bounded_admission_encoding")?;
    run_snapshot_digest(snapshot, Sha256::digest(&bytes).into(), cancellation)
}

fn run_snapshot_digest(
    snapshot: &Snapshot,
    identity: [u8; 32],
    cancellation: &AtomicBool,
) -> Result<sandbox_launcher::BoundedSandboxObservation, String> {
    validate_snapshot(snapshot)?;
    reject_system_overlap(&snapshot.source_root)
        .map_err(|_| "bounded_project_system_root_overlap")?;
    let mut spec = sandbox_launcher::SandboxLaunchSpec::workspace(&snapshot.scratch.path);
    spec.workspace_access = sandbox_launcher::SandboxWorkspaceAccess::ReadOnly;
    spec.network_enabled = false;
    spec.require_kernel_hardening = true;
    let command = match snapshot.language.as_str() {
        "python" => format!("exec python3 /project/{CODE_FILE}"),
        "javascript" => format!("exec node --jitless --max-old-space-size=64 /project/{CODE_FILE}"),
        "bash" => format!("exec bash /project/{CODE_FILE}"),
        _ => return Err("bounded_language_unsupported".into()),
    };
    let timeout = snapshot
        .authorization_deadline
        .map_or(snapshot.timeout, |deadline| {
            snapshot
                .timeout
                .min(deadline.saturating_duration_since(Instant::now()))
        });
    if timeout.is_zero() {
        return Err("bounded_authorization_expired".into());
    }
    let prepared = sandbox_launcher::prepare_bounded_command(spec, command, timeout, identity)
        .map_err(|_| "bounded_launcher_prepare_failed")?;
    validate_snapshot(snapshot)?;
    Ok(prepared.run(cancellation))
}

pub struct SandboxResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

pub fn execute_code_in_workspace(
    language: &str,
    code: &str,
    timeout_ms: Option<u64>,
    workspace: &Path,
    input_files: &[String],
) -> SandboxResult {
    let mut input = json!({"language":language,"code":code,"input_files":input_files});
    if let Some(timeout) = timeout_ms {
        input["timeout_ms"] = json!(timeout);
    }
    let result = prepare_snapshot(workspace, &input, None, &std::env::temp_dir())
        .map_err(|error| error.to_string())
        .and_then(|snapshot| {
            let identity = Sha256::digest(&snapshot.bundle).into();
            run_snapshot_digest(&snapshot, identity, &AtomicBool::new(false))
        });
    match result {
        Ok(result) => SandboxResult {
            stdout: String::from_utf8_lossy(result.stdout()).into_owned(),
            stderr: if result.reason_code().is_empty() {
                String::from_utf8_lossy(result.stderr()).into_owned()
            } else {
                format!(
                    "{}\n{}",
                    String::from_utf8_lossy(result.stderr()),
                    result.reason_code()
                )
            },
            exit_code: match result.stop_reason() {
                "timeout" => 124,
                "cancelled" => 125,
                "capture_limit" => 126,
                _ => result.exit_code().unwrap_or(1),
            },
        },
        Err(error) => SandboxResult {
            stdout: String::new(),
            stderr: error,
            exit_code: 1,
        },
    }
}

fn admitted_environment(snapshot: &Snapshot) -> Value {
    let mut paths = Vec::new();
    let mut encoded_bytes = 0usize;
    for input in &snapshot.inputs {
        let encoded = serde_json::to_string(&input.relative_path).unwrap_or_default();
        if paths.len() >= 4 || encoded_bytes + encoded.len() > 1024 {
            break;
        }
        encoded_bytes += encoded.len();
        paths.push(input.relative_path.as_str());
    }
    json!({
        "source": "validated_snapshot_contract_not_launch_attestation",
        "cwd": "/project",
        "workspace_access": "read_only_explicit_inputs",
        "network_enabled": false,
        "input_files_total": snapshot.inputs.len(),
        "input_files_omitted": snapshot.inputs.len() - paths.len(),
        "input_files": paths,
        "path_mapping": "Each admitted workspace-relative input is available as /project/<relative_path> only if execution starts; host absolute paths are not sandbox aliases.",
        "scratch": "/tmp is private scratch; no automatic file export or workspace writeback.",
        "next_call": "A new authorized call may explicitly include needed workspace-relative input_files. Persist computed output with an available authorized file tool; terminal and capture_complete, not this environment description, determine execution outcome."
    })
}

fn summary(
    stdout: &[u8],
    stderr: &[u8],
    terminal: &ToolExecutionTerminal,
    body: Option<&str>,
    environment: Option<&Value>,
) -> String {
    let stdout = String::from_utf8_lossy(&stdout[..stdout.len().min(2048)]);
    let stderr = String::from_utf8_lossy(&stderr[..stderr.len().min(2048)]);
    let mut summary =
        json!({"stdout":stdout,"stderr":stderr,"terminal":terminal,"body":body,"environment":environment}).to_string();
    if summary.len() > 16384 {
        let mut boundary = 16300;
        while !summary.is_char_boundary(boundary) {
            boundary -= 1;
        }
        summary.truncate(boundary);
        summary.push_str("\n[summary truncated; complete bytes are in the body artifact]");
    }
    summary
}

#[cfg(all(test, target_os = "linux"))]
mod bounded_tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    fn prepare(
        workspace: &Path,
        input: &Value,
        binding: ToolExecutionBinding,
    ) -> Result<PreparedToolExecution, ToolHostError> {
        super::prepare(workspace, input, binding, None)
    }

    struct Workspace(PathBuf);
    impl Workspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "cowd-bounded-source-tests-{}",
                uuid::Uuid::new_v4()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Workspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn binding(input: &Value) -> ToolExecutionBinding {
        ToolExecutionBinding {
            call_key: "real-copy-unit-call".into(),
            tool_use_id: "copy-test".into(),
            input_sha256: harness_contract::step::canonical_digest(
                harness_contract::context::TOOL_EXECUTION_INPUT_DOMAIN,
                input,
            )
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned(),
            descriptor_sha256: "d".repeat(64),
            authority_sha256: "a".repeat(64),
        }
    }
    fn prepared(workspace: &Path, input: &Value) -> PreparedToolExecution {
        prepare(workspace, input, binding(input)).unwrap()
    }
    fn source_ref(prepared: &PreparedToolExecution) -> ArtifactRef {
        ArtifactRef::durable(
            "artifact://source-unit",
            format!("sha256:{}", digest(prepared.source_bundle_bytes())),
            prepared.source_bundle_bytes().len() as u64,
            "application/octet-stream",
            "test",
        )
    }
    fn run(workspace: &Path, input: &Value) -> CompletedToolExecution {
        let prepared = prepared(workspace, input);
        let admission = prepared.admission(&source_ref(&prepared)).unwrap();
        prepared.run(admission, &ToolExecutionCancellation::new())
    }

    #[test]
    fn bounded_input_contract_rejects_missing_unknown_and_budget_escalation() {
        let workspace = Workspace::new();
        for input in [
            json!({"language":"python","code":"pass"}),
            json!({"language":"python","code":"pass","input_files":[],"invented":true}),
            json!({"language":"ruby","code":"puts 1","input_files":[]}),
            json!({"language":"python","code":"x".repeat(65537),"input_files":[]}),
            json!({"language":"python","code":"pass","input_files":["a","a"]}),
            json!({"language":"python","code":"pass","input_files":(0..65).map(|index|format!("file{index}")).collect::<Vec<_>>()}),
            json!({"language":"python","code":"pass","input_files":[],"timeout_ms":120001}),
            json!({"language":"python","code":"pass","input_files":[],"timeout_ms":0}),
            json!({"language":"python","code":"pass","input_files":[],"timeout_ms":"30"}),
        ] {
            assert!(
                prepare(&workspace.0, &input, binding(&input)).is_err(),
                "{input}"
            );
        }
        for path in [
            "",
            ".",
            "..",
            "a/../b",
            "/etc/passwd",
            "a//b",
            "./a",
            "a\\b",
            ".cowd-bounded-code",
            "a\0b",
        ] {
            let input = json!({"language":"python","code":"pass","input_files":[path]});
            assert!(
                prepare(&workspace.0, &input, binding(&input)).is_err(),
                "{path}"
            );
        }
        fs::create_dir(workspace.0.join(".cowd")).unwrap();
        fs::write(workspace.0.join(".cowd/secret"), "protected").unwrap();
        let control = json!({"language":"python","code":"pass","input_files":[".cowd/secret"]});
        assert!(prepare(&workspace.0, &control, binding(&control)).is_err());
        let json_overflow = json!({"language":"python","code":"\"".repeat(65536),"input_files":[]});
        assert!(parse_input(&json_overflow).is_err());
        let valid = json!({"language":"python","code":"pass","input_files":[]});
        let mut invalid_binding = binding(&valid);
        invalid_binding.input_sha256 = "f".repeat(64);
        assert!(prepare(&workspace.0, &valid, invalid_binding).is_err());
        for invalid_descriptor in [
            "d".repeat(16),
            "D".repeat(64),
            format!("sha256:{}", "d".repeat(64)),
        ] {
            let mut invalid_binding = binding(&valid);
            invalid_binding.descriptor_sha256 = invalid_descriptor;
            assert!(prepare(&workspace.0, &valid, invalid_binding).is_err());
        }
        assert!(reject_system_overlap(Path::new("/usr")).is_err());
        assert!(reject_system_overlap(Path::new("/")).is_err());
    }

    #[test]
    fn bounded_source_identity_ignores_filesystem_defined_directory_size() {
        let workspace = Workspace::new();
        let file = workspace.0.join("identity-file.txt");
        fs::write(&file, b"identity evidence").unwrap();
        let directory = fs::metadata(&workspace.0).unwrap();
        let regular = fs::metadata(&file).unwrap();
        let directory_identity = source_identity(&directory);
        let file_identity = source_identity(&regular);
        assert!(directory.is_dir());
        assert_eq!(
            directory_identity.3, 0,
            "directory size is filesystem-defined and flaps on FUSE mounts; it is no integrity signal"
        );
        assert_eq!(file_identity.3, regular.len(), "file size stays compared");
    }

    #[test]
    fn bounded_scratch_survives_symlinked_temp_root() {
        // Regression: production points TMPDIR through a short-alias symlink
        // (Unix socket path bound); scratch preparation resolves the alias once
        // instead of failing its own NO_SYMLINKS open, and repeated observations
        // of the scratch tree stay stable across filesystems.
        let workspace = Workspace::new();
        let base = std::env::temp_dir().join(format!("cowd-bounded-symlink-{}", uuid::Uuid::new_v4()));
        let real = base.join("real");
        let alias = base.join("alias");
        fs::create_dir_all(&real).unwrap();
        symlink(&real, &alias).unwrap();
        let input = json!({"language":"python","code":"print('symlinked-temp-ok')","input_files":[]});
        let snapshot = prepare_snapshot(&workspace.0, &input, None, &alias)
            .expect("prepare through symlinked temp root");
        let observation = run_snapshot_digest(
            &snapshot,
            Sha256::digest(&snapshot.bundle).into(),
            &AtomicBool::new(false),
        )
        .expect("run through symlinked temp root");
        let _ = fs::remove_dir_all(&base);
        assert!(observation.started(), "{:?}", observation.reason_code());
        assert_eq!(observation.exit_code(), Some(0));
    }

    #[test]
    fn bounded_input_canonical_digest_accepts_format_only_and_rejects_changed_values() {
        let workspace = Workspace::new();
        let original = r#"{"language":"python","code":"pass","input_files":[]}"#;
        let formatted =
            " { \n \"input_files\" : [ ], \"code\" : \"pass\", \"language\" : \"python\" } \n";
        let original_value = harness_contract::step::parse_strict_json(original).unwrap();
        let formatted_value = harness_contract::step::parse_strict_json(formatted).unwrap();
        assert_eq!(original_value, formatted_value);
        assert_ne!(original.as_bytes(), formatted.as_bytes());
        assert_ne!(digest(original.as_bytes()), digest(formatted.as_bytes()));
        let original_binding = binding(&original_value);
        assert_eq!(
            original_binding.input_sha256,
            binding(&formatted_value).input_sha256
        );
        assert!(prepare(&workspace.0, &original_value, original_binding.clone()).is_ok());
        assert!(prepare(&workspace.0, &formatted_value, original_binding.clone()).is_ok());
        let mut changed = formatted_value.clone();
        changed["code"] = json!("raise SystemExit(1)");
        assert!(prepare(&workspace.0, &changed, original_binding.clone()).is_err());
        for old_digest in [
            digest(original.as_bytes()),
            digest(formatted.as_bytes()),
            digest(&serde_json::to_vec(&original_value).unwrap()),
        ] {
            let mut old_binding = original_binding.clone();
            old_binding.input_sha256 = old_digest;
            assert!(prepare(&workspace.0, &original_value, old_binding).is_err());
        }
        assert!(harness_contract::step::parse_strict_json(
            r#"{"language":"python","code":"pass","code":"changed","input_files":[]}"#
        )
        .is_err());
        assert_eq!(
            original,
            r#"{"language":"python","code":"pass","input_files":[]}"#
        );
        assert!(formatted.starts_with(" { \n"));
    }

    #[test]
    fn bounded_workspace_getter_uses_prepared_canonical_root_not_model_fields() {
        let workspace = Workspace::new();
        let input = json!({"language":"python","code":"pass","input_files":[]});
        let alias = workspace.0.with_extension("alias");
        std::os::unix::fs::symlink(&workspace.0, &alias).unwrap();
        let prepared = prepared(&alias, &input);
        assert_eq!(
            prepared.workspace_root(),
            workspace.0.canonicalize().unwrap()
        );
        assert_ne!(prepared.workspace_root(), alias);
        fs::remove_file(alias).unwrap();
        assert_eq!(
            prepared.workspace_root(),
            workspace.0.canonicalize().unwrap()
        );
        for field in ["source_workspace_root", "workspace_root", "source_root"] {
            let mut untrusted = input.clone();
            untrusted[field] = json!("/unrelated-current-workspace");
            assert!(prepare(&workspace.0, &untrusted, binding(&untrusted)).is_err());
        }
    }

    #[test]
    fn bounded_source_copy_preserves_real_fd_bytes_bundle_and_old_version() {
        let workspace = Workspace::new();
        fs::create_dir(workspace.0.join("src")).unwrap();
        fs::write(workspace.0.join("src/data"), b"version-one\0\xff").unwrap();
        let input = json!({"language":"python","code":"print(open('src/data','rb').read().hex())","input_files":["src/data"]});
        let prepared = prepared(&workspace.0, &input);
        assert_eq!(
            prepared.workspace_root(),
            workspace.0.canonicalize().unwrap()
        );
        assert_eq!(prepared.inputs()[0].sha256, digest(b"version-one\0\xff"));
        assert_eq!(prepared.inputs()[0].bytes, 13);
        assert_eq!(&prepared.source_bundle_bytes()[..8], b"COWD-BP1");
        assert!(prepared
            .source_bundle_bytes()
            .windows(13)
            .any(|window| window == b"version-one\0\xff"));
        let mut wrong_ref = source_ref(&prepared);
        wrong_ref.bytes += 1;
        assert!(prepared.admission(&wrong_ref).is_err());
        fs::write(workspace.0.join("src/data"), "version-two").unwrap();
        let admission = prepared.admission(&source_ref(&prepared)).unwrap();
        let completed = prepared.run(admission, &ToolExecutionCancellation::new());
        assert!(
            matches!(
                completed.terminal(),
                ToolExecutionTerminal::Settled {
                    exit_code: Some(0),
                    ..
                }
            ),
            "{:?}",
            completed.terminal()
        );
        let body: Value = serde_json::from_slice(completed.body_bytes()).unwrap();
        let stdout = base64::engine::general_purpose::STANDARD
            .decode(body["stdout_base64"].as_str().unwrap())
            .unwrap();
        assert!(String::from_utf8(stdout)
            .unwrap()
            .contains("76657273696f6e2d6f6e6500ff"));
        assert_eq!(
            fs::read_to_string(workspace.0.join("src/data")).unwrap(),
            "version-two"
        );
    }

    #[test]
    fn bounded_source_rejects_aliases_special_files_and_size_limits() {
        let workspace = Workspace::new();
        fs::write(workspace.0.join("file"), "original").unwrap();
        fs::hard_link(workspace.0.join("file"), workspace.0.join("alias")).unwrap();
        symlink("file", workspace.0.join("link")).unwrap();
        fs::create_dir(workspace.0.join("directory")).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            workspace.0.join("fifo"),
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        for path in ["file", "alias", "link", "directory", "fifo"] {
            let input = json!({"language":"python","code":"pass","input_files":[path]});
            assert!(
                prepare(&workspace.0, &input, binding(&input)).is_err(),
                "{path}"
            );
        }
        let huge = File::create(workspace.0.join("huge")).unwrap();
        huge.set_len(MAX_FILE_BYTES + 1).unwrap();
        let input = json!({"language":"python","code":"pass","input_files":["huge"]});
        assert!(prepare(&workspace.0, &input, binding(&input)).is_err());
        let mut paths = Vec::new();
        for index in 0..9 {
            let name = format!("bounded-{index}");
            File::create(workspace.0.join(&name))
                .unwrap()
                .set_len(MAX_FILE_BYTES)
                .unwrap();
            paths.push(name);
        }
        let total = json!({"language":"python","code":"pass","input_files":paths});
        assert!(prepare(&workspace.0, &total, binding(&total)).is_err());
    }

    #[test]
    fn bounded_fd_barriers_reject_rename_nlink_inplace_and_root_swaps() {
        for attack in [
            "leaf-symlink",
            "parent-symlink",
            "rename",
            "nlink",
            "rewrite",
            "truncate",
            "grow",
            "root",
        ] {
            let container = Workspace::new();
            let root = container.0.join("workspace");
            fs::create_dir(&root).unwrap();
            fs::create_dir(root.join("src")).unwrap();
            fs::write(root.join("src/data"), "original").unwrap();
            let attack = attack.to_string();
            PREPARE_HOOK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move |stage, path| {
                    if stage == "source_opened" {
                        match attack.as_str() {
                            "leaf-symlink" => {
                                fs::rename(path, path.with_extension("old")).unwrap();
                                symlink("data.old", path).unwrap();
                            }
                            "parent-symlink" => {
                                let parent = path.parent().unwrap();
                                fs::rename(parent, parent.with_extension("old")).unwrap();
                                symlink("src.old", parent).unwrap();
                            }
                            "rename" => {
                                fs::rename(path, path.with_extension("old")).unwrap();
                                fs::write(path, "different").unwrap();
                            }
                            "nlink" => {
                                fs::hard_link(path, path.with_extension("alias")).unwrap();
                            }
                            _ => {}
                        }
                    }
                    if stage == "source_read" {
                        match attack.as_str() {
                            "rewrite" => fs::write(path, "modified").unwrap(),
                            "truncate" => fs::write(path, "x").unwrap(),
                            "grow" => fs::write(path, "a much longer replacement").unwrap(),
                            _ => {}
                        }
                    }
                    if stage == "root_check" && attack == "root" {
                        fs::rename(path, path.with_extension("old")).unwrap();
                        fs::create_dir(path).unwrap();
                    }
                }))
            });
            let input = json!({"language":"python","code":"pass","input_files":["src/data"]});
            let result = prepare(&root, &input, binding(&input));
            PREPARE_HOOK.with(|slot| *slot.borrow_mut() = None);
            assert!(result.is_err());
        }
    }

    #[test]
    fn bounded_prepared_scratch_tamper_and_wrong_admission_never_spawn() {
        let workspace = Workspace::new();
        fs::write(workspace.0.join("data"), "original").unwrap();
        let input = json!({"language":"python","code":"raise RuntimeError('must not spawn')","input_files":["data"]});
        let mut prepared = prepared(&workspace.0, &input);
        let admission = prepared.admission(&source_ref(&prepared)).unwrap();
        let file = prepared.state.snapshot.scratch.path.join("data");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&file, "tampered").unwrap();
        let completed = prepared.run(admission, &ToolExecutionCancellation::new());
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::NotStarted { .. }
        ));
        let identical = prepare(&workspace.0, &input, binding(&input)).unwrap();
        let identical_admission = identical.admission(&source_ref(&identical)).unwrap();
        let original = identical.state.snapshot.scratch.path.join("data");
        fs::set_permissions(
            &identical.state.snapshot.scratch.path,
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::rename(&original, original.with_extension("old")).unwrap();
        fs::write(&original, "original").unwrap();
        let completed = identical.run(identical_admission, &ToolExecutionCancellation::new());
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::NotStarted { .. }
        ));
        prepared = prepare(&workspace.0, &input, binding(&input)).unwrap();
        let mut admission = prepared.admission(&source_ref(&prepared)).unwrap();
        admission.binding.call_key = "different-call".into();
        let completed = prepared.run(admission, &ToolExecutionCancellation::new());
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::NotStarted { .. }
        ));
        let prepared = prepare(&workspace.0, &input, binding(&input)).unwrap();
        let admission = prepared.admission(&source_ref(&prepared)).unwrap();
        let cancellation = ToolExecutionCancellation::new();
        cancellation.cancel();
        let completed = prepared.run(admission, &cancellation);
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::NotStarted { .. }
        ));
    }

    #[test]
    fn bounded_completed_keeps_nonzero_and_binary_body_without_trusting_public_projection() {
        let workspace = Workspace::new();
        let input = json!({"language":"python","code":"import os; os.write(1,b'\\xff\\x00'); os.write(2,b'error'); raise SystemExit(7)","input_files":[]});
        let completed = run(&workspace.0, &input);
        assert!(
            matches!(
                completed.terminal(),
                ToolExecutionTerminal::Settled {
                    exit_code: Some(7),
                    ..
                }
            ),
            "{:?}",
            completed.terminal()
        );
        let body: Value = serde_json::from_slice(completed.body_bytes()).unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(body["stdout_base64"].as_str().unwrap())
                .unwrap(),
            b"\xff\0"
        );
        assert_eq!(body["stdout_bytes"], 2);
        assert!(body.get("stdout_utf8").is_none());
        assert_eq!(body["stderr_utf8"], "error");
        let artifact = ArtifactRef::durable(
            "artifact://actual-body-unit",
            format!("sha256:{}", digest(completed.body_bytes())),
            completed.body_bytes().len() as u64,
            "application/json",
            "test",
        );
        let output = completed.into_output(artifact.clone()).unwrap();
        assert_eq!(output.artifact_ref(), Some(&artifact));
        assert_eq!(output.observations().len(), 1);
        assert!(output.observations()[0].model_observation.is_none());
        assert!(output.observations()[0].workspace_prior_state.is_none());
        let EvidenceTargetIdentity::ToolExecution { execution } = &output.observations()[0].target
        else {
            panic!("dedicated nonFile target");
        };
        assert_eq!(execution.stdout_sha256, digest(b"\xff\0"));
        let completed = run(&workspace.0, &input);
        let mut wrong = artifact;
        wrong.sha256 = "f".repeat(64);
        assert!(completed.into_output(wrong).is_err());
    }

    #[test]
    fn bounded_omitted_import_fails_in_snapshot_instead_of_reading_live_project() {
        let workspace = Workspace::new();
        fs::write(workspace.0.join("omitted_dependency.py"), "secret = 42").unwrap();
        let input = json!({"language":"python","code":"import omitted_dependency; print(omitted_dependency.secret)","input_files":[]});
        let completed = run(&workspace.0, &input);
        assert!(
            matches!(completed.terminal(), ToolExecutionTerminal::Settled { exit_code: Some(code), .. } if *code != 0)
        );
        let body: Value = serde_json::from_slice(completed.body_bytes()).unwrap();
        let feedback = completed.state.environment.as_ref().unwrap();
        assert_eq!(feedback["input_files_total"], 0);
        assert_eq!(feedback["cwd"], "/project");
        assert_eq!(feedback["workspace_access"], "read_only_explicit_inputs");
        assert_eq!(feedback["network_enabled"], false);
        let stderr = base64::engine::general_purpose::STANDARD
            .decode(body["stderr_base64"].as_str().unwrap())
            .unwrap();
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("ModuleNotFoundError"));
    }

    #[test]
    fn bounded_environment_feedback_is_admitted_bounded_and_not_a_launch_claim() {
        let workspace = Workspace::new();
        for name in ["first", "second", "third", "fourth", "fifth"] {
            fs::write(workspace.0.join(name), name).unwrap();
        }
        let input = json!({"language":"python","code":"pass","input_files":["first","second","third","fourth","fifth"]});
        let prepared = prepared(&workspace.0, &input);
        let feedback = admitted_environment(&prepared.state.snapshot);
        assert_eq!(feedback["input_files_total"], 5);
        assert_eq!(feedback["input_files_omitted"], 1);
        assert_eq!(feedback["input_files"].as_array().unwrap().len(), 4);
        assert_eq!(
            feedback["source"],
            "validated_snapshot_contract_not_launch_attestation"
        );
        assert!(!feedback.to_string().contains(workspace.0.to_str().unwrap()));
        let mut admission = prepared.admission(&source_ref(&prepared)).unwrap();
        admission.binding.call_key = "forged".into();
        let completed = prepared.run(admission, &ToolExecutionCancellation::new());
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::NotStarted { .. }
        ));
        let body: Value = serde_json::from_slice(completed.body_bytes()).unwrap();
        assert!(completed.state.environment.is_none());
        assert!(body.get("environment").is_none());
        let summary: Value = serde_json::from_str(&super::summary(
            b"unchanged stdout",
            b"unchanged stderr",
            completed.terminal(),
            None,
            Some(&feedback),
        ))
        .unwrap();
        assert_eq!(summary["environment"], feedback);
        assert_eq!(summary["stdout"], "unchanged stdout");
        assert_eq!(summary["stderr"], "unchanged stderr");
        assert_eq!(summary["terminal"], body["terminal"]);
    }

    #[test]
    fn bounded_environment_feedback_reaches_model_text_without_changing_body_protocol() {
        let workspace = Workspace::new();
        fs::write(workspace.0.join("data"), "actual input").unwrap();
        let input =
            json!({"language":"python","code":"print(open('data').read())","input_files":["data"]});
        let completed = run(&workspace.0, &input);
        assert!(matches!(
            completed.terminal(),
            ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                ..
            }
        ));
        let raw: Value = serde_json::from_slice(completed.body_bytes()).unwrap();
        assert!(raw.get("environment").is_none());
        let artifact = ArtifactRef::durable(
            "artifact://feedback-body",
            format!("sha256:{}", digest(completed.body_bytes())),
            completed.body_bytes().len() as u64,
            "application/json",
            "test",
        );
        let output = completed.into_output(artifact.clone()).unwrap();
        let presented: Value = serde_json::from_str(output.model_text()).unwrap();
        assert_eq!(presented["stdout"], "actual input\n");
        assert_eq!(presented["terminal"], raw["terminal"]);
        assert_eq!(presented["body"], artifact.selector);
        assert_eq!(presented["environment"]["input_files"], json!(["data"]));
        assert_eq!(presented["environment"]["input_files_omitted"], 0);
        assert_eq!(output.artifact_ref(), Some(&artifact));
    }

    #[test]
    fn bounded_environment_feedback_accounts_for_escaped_and_long_paths() {
        let workspace = Workspace::new();
        let input = json!({"language":"python","code":"pass","input_files":[]});
        let mut prepared = prepared(&workspace.0, &input);
        prepared.state.snapshot.inputs = (0..MAX_FILES)
            .map(|index| ToolExecutionInput {
                relative_path: format!("{index}/{}", "\"\n".repeat(250)),
                sha256: "a".repeat(64),
                bytes: 0,
                device: 1,
                inode: 1,
                mtime_ns: 0,
                ctime_ns: 0,
            })
            .collect();
        let feedback = admitted_environment(&prepared.state.snapshot);
        assert_eq!(feedback["input_files_total"], MAX_FILES);
        let shown = feedback["input_files"].as_array().unwrap().len();
        assert_eq!(feedback["input_files_omitted"], MAX_FILES - shown);
        assert!(feedback.to_string().len() < 4096);
        assert_eq!(shown, 1);
    }

    #[test]
    fn bounded_utf8_body_text_is_complete_or_absent_never_a_prefix() {
        let workspace = Workspace::new();
        for size in [131072, 131073, 200000] {
            let input = json!({"language":"python","code":format!("import os; os.write(1,b'x'*{size})"),"input_files":[]});
            let completed = run(&workspace.0, &input);
            assert!(
                matches!(
                    completed.terminal(),
                    ToolExecutionTerminal::Settled {
                        exit_code: Some(0),
                        ..
                    }
                ),
                "{:?}",
                completed.terminal()
            );
            let body: Value = serde_json::from_slice(completed.body_bytes()).unwrap();
            assert_eq!(body["capture_complete"], true);
            let raw = base64::engine::general_purpose::STANDARD
                .decode(body["stdout_base64"].as_str().unwrap())
                .unwrap();
            assert_eq!(raw.len(), size);
            assert_eq!(body["stdout_sha256"], digest(&raw));
            if size <= 131072 {
                assert_eq!(body["stdout_utf8"].as_str().unwrap().as_bytes(), raw);
            } else {
                assert!(body.get("stdout_utf8").is_none());
            }
            assert!(completed.body_bytes().len() <= MAX_BODY_BYTES);
        }
    }
}

#[cfg(test)]
pub fn execute_code(language: &str, code: &str) -> SandboxResult {
    execute_code_with_timeout(language, code, None)
}

#[cfg(test)]
pub fn execute_code_with_timeout(
    language: &str,
    code: &str,
    timeout: Option<u64>,
) -> SandboxResult {
    let workspace =
        std::env::temp_dir().join(format!("cowd-bounded-test-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&workspace).expect("test workspace");
    let result = execute_code_in_workspace(language, code, timeout, &workspace, &[]);
    fs::remove_dir(&workspace).expect("remove workspace");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_executes_in_hardened_sandbox() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("bash", "echo hello");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
        assert!(result.stdout.contains("hello"));
    }

    #[test]
    fn python_executes_in_hardened_sandbox() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("python", "print('hello')");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
    }

    #[test]
    fn unsupported_language_fails() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_ne!(execute_code("brainfuck", "+.").exit_code, 0);
    }

    #[test]
    fn timeout_stops_child() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code_with_timeout("bash", "sleep 5", Some(100));
        assert_eq!(result.exit_code, 124);
    }

    #[test]
    fn protected_host_config_is_not_visible() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("bash", "test ! -e \"$HOME/.cowd\"");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sandboxed_code_cannot_reach_host_network_namespace() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("host listener");
        let port = listener.local_addr().expect("listener address").port();
        let result = execute_code(
            "python",
            &format!(
                "import socket, sys\ns=socket.socket()\ns.settimeout(1)\ntry:\n s.connect(('127.0.0.1', {port}))\nexcept OSError:\n sys.exit(0)\nsys.exit(9)"
            ),
        );
        assert_eq!(
            result.exit_code, 0,
            "sandbox unexpectedly reached a host-network listener: {}",
            result.stderr
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sandboxed_code_cannot_mutate_the_workspace() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let workspace =
            std::env::temp_dir().join(format!("cowd-execute-code-readonly-{}", std::process::id()));
        std::fs::create_dir_all(&workspace).expect("test workspace");
        let denied = workspace.join("denied.txt");
        let result = execute_code_in_workspace(
            "python",
            "from pathlib import Path; Path('denied.txt').write_text('escaped')",
            None,
            &workspace,
            &[],
        );
        assert_ne!(
            result.exit_code, 0,
            "workspace write unexpectedly succeeded"
        );
        assert!(!denied.exists());
        std::fs::remove_dir(&workspace).expect("remove empty test workspace");
    }

    #[test]
    fn large_output_does_not_deadlock_before_truncation() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("python", "print('x' * 3000000)");
        assert_eq!(result.exit_code, 126, "{}", result.stderr);
        assert!(result.stdout.len() <= 2 * 1024 * 1024);
    }
}
