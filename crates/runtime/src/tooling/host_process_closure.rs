//! Durable process-closure evidence for host-shell process effects.
//!
//! The host shell (`bash` / `powershell`) is deliberately **not** a sandbox
//! process: it owns no private PID namespace, it is not kernel hardened and it
//! is not network isolated. The only physical fact the host executor can
//! honestly prove is *quiescence*: the command process itself was reaped and
//! its process group holds no live member.
//!
//! This module turns that quiescence report into the durable
//! `EvidenceTargetIdentity::ToolExecution` evidence that the physical closure
//! witness consumes, and it never claims a security posture the host path
//! cannot prove. A missing or unprovable report becomes an explicit
//! `ToolExecutionTerminal::Unknown`, which the witness treats as *not*
//! closed (fail closed).

use crate::execution_core::graph::ExecutionCommitService;
use crate::RuntimeToolExecutionRequest;
use harness_contract::context::{
    ArtifactRef, EvidenceAccessRef, EvidenceTargetIdentity, ObservedEvidence,
    ObservedEvidenceProvenance, ToolExecutionAdmission, ToolExecutionFact,
    ToolExecutionStopReason, ToolExecutionTerminal, ToolProcessClosure,
};
use harness_contract::tool::ToolEffectDescriptor;
use sha2::{Digest, Sha256};

/// Backend identity of a host-shell process effect.
///
/// It is deliberately **not** the private bounded-process backend: a host
/// shell proves no hardening, no network isolation and no private namespace,
/// and this identity is what keeps the two capabilities apart.
pub(crate) const HOST_PROCESS_BACKEND: &str = "cowd-host-shell";

/// The host executor did not report any process closure fact at all.
pub(crate) const REASON_CLOSURE_ABSENT: &str = "host_process_closure_facts_absent";
/// The host executor reported a fact, but it does not prove quiescence.
pub(crate) const REASON_CLOSURE_NOT_REAPED: &str = "host_process_closure_not_reaped";
/// The host executor reported both an exit code and a signal.
pub(crate) const REASON_CLOSURE_EXIT_AMBIGUOUS: &str = "host_process_closure_exit_ambiguous";
/// The tool output could not be read as a host executor report.
pub(crate) const REASON_OUTPUT_UNREADABLE: &str = "host_process_output_unreadable";

/// Maximum admitted source archive size for a host process effect.
const MAX_SOURCE_BUNDLE_BYTES: usize = 1024 * 1024;
/// Maximum size of the durable closure document.
const MAX_BODY_BYTES: usize = 64 * 1024;

/// The quiescence report the host shell executor attaches to its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HostProcessClosure {
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub init_reaped: bool,
    pub all_children_reaped: bool,
}

/// The honest reading of one host process effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostProcessReport {
    /// The terminal the witness will read.
    pub terminal: ToolExecutionTerminal,
    pub stdout_sha256: String,
    pub stderr_sha256: String,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub capture_complete: bool,
}

#[derive(Debug, serde::Deserialize)]
struct RawHostClosure {
    #[serde(default, rename = "exitCode")]
    exit_code: Option<i32>,
    #[serde(default)]
    signal: Option<i32>,
    #[serde(default, rename = "initReaped")]
    init_reaped: bool,
    #[serde(default, rename = "allChildrenReaped")]
    all_children_reaped: bool,
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical_digest(domain: &str, value: &serde_json::Value) -> Result<String, String> {
    harness_contract::step::canonical_digest(domain, value)
        .map(|digest| {
            digest
                .strip_prefix("sha256:")
                .map(str::to_owned)
                .unwrap_or(digest)
        })
        .map_err(|error| error.to_string())
}

/// Mint the durable host-process closure evidence for a process-class effect.
///
/// The Runtime kernel is the only authority that may publish this fact: the
/// tool host adapter is rejected when it claims a process terminal of its
/// own. The fact carries the host executor's own quiescence report (exit code
/// or signal, the child reaped, the process group without a live member) and
/// never claims a hardening, network isolation or private namespace the host
/// path cannot prove.
///
/// Returns `false` when the effect does not need a host process terminal (the
/// caller then keeps its normal commit path).
pub(crate) async fn mint_host_process_evidence(
    service: &ExecutionCommitService,
    request: &crate::RuntimeToolExecutionRequest,
    descriptor: &ToolEffectDescriptor,
    outcome: &mut crate::RuntimeToolExecutionOutcome,
    admission: Option<&ToolExecutionAdmission>,
) -> Result<bool, String> {
    if !descriptor.spawns_process {
        return Ok(false);
    }
    // A read-class effect that owns a process tree keeps its own key family:
    // its admission is bound to its read intent and its receipt is the read
    // receipt, and the physical closure witness settles it against exactly
    // that family.
    let Some(admission) = admission else {
        // Without the admission published before the command ran there is
        // nothing that binds the executor's report to the authorized program,
        // so the kernel refuses to claim a terminal at all.
        return Ok(false);
    };
    if outcome.observed_evidence.iter().any(|observed| {
        matches!(
            observed.target,
            EvidenceTargetIdentity::ToolExecution { .. }
        )
    }) {
        return Err("tool host adapter may not publish a process terminal fact".into());
    }
    let Ok(artifacts) = service.bounded_process_artifacts() else {
        // Without the original ArtifactStore the closure cannot be archived,
        // and an unarchived closure is no closure: the caller keeps its normal
        // commit path and the witness keeps failing closed.
        return Ok(false);
    };
    let report = read_host_process_report(outcome.output.as_deref());
    let body_bytes = body_bytes(request, &report)?;
    let scope = format!(
        "session:{}",
        request
            .session_id
            .as_deref()
            .ok_or_else(|| "host process Session missing".to_string())?
    );
    let body = artifacts
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "application/json".into(),
                visibility_scope: scope,
                expected_bytes: Some(body_bytes.len() as u64),
                original_name: None,
            },
            &body_bytes,
        )
        .await
        .map_err(|error| format!("host process closure archive failed: {error}"))?;
    artifacts
        .pin(
            &body,
            &format!("execution-effect:{}", request.idempotency_key),
            i64::MAX as u64,
        )
        .map_err(|error| format!("host process closure pin failed: {error}"))?;
    let fact = build_host_process_fact(request, descriptor, &report, body, admission.clone())?;
    outcome
        .observed_evidence
        .push(host_process_observation(request, fact));
    Ok(true)
}

/// Read the honest closure report out of a host executor result.
///
/// The host executor serializes its whole result object, so the quiescence
/// facts live at the top level of the tool output under `processClosure`.
pub(crate) fn read_host_process_report(output: Option<&str>) -> HostProcessReport {
    let Some(output) = output else {
        return HostProcessReport {
            terminal: ToolExecutionTerminal::Unknown {
                reason_code: REASON_OUTPUT_UNREADABLE.into(),
            },
            stdout_sha256: hex_digest(&[]),
            stderr_sha256: hex_digest(&[]),
            stdout_bytes: 0,
            stderr_bytes: 0,
            capture_complete: false,
        };
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
        return HostProcessReport {
            terminal: ToolExecutionTerminal::Unknown {
                reason_code: REASON_OUTPUT_UNREADABLE.into(),
            },
            stdout_sha256: hex_digest(&[]),
            stderr_sha256: hex_digest(&[]),
            stdout_bytes: 0,
            stderr_bytes: 0,
            capture_complete: false,
        };
    };
    let stdout = value
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let stderr = value
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let capture_complete = value
        .get("returnTruncated")
        .and_then(serde_json::Value::as_bool)
        != Some(true);
    let closure = value
        .get("processClosure")
        .and_then(|slot| serde_json::from_value::<RawHostClosure>(slot.clone()).ok());
    let terminal = match closure {
        None => ToolExecutionTerminal::Unknown {
            reason_code: REASON_CLOSURE_ABSENT.into(),
        },
        Some(closure) => {
            if closure.exit_code.is_some() == closure.signal.is_some() {
                ToolExecutionTerminal::Unknown {
                    reason_code: REASON_CLOSURE_EXIT_AMBIGUOUS.into(),
                }
            } else if !closure.init_reaped || !closure.all_children_reaped {
                ToolExecutionTerminal::Unknown {
                    reason_code: REASON_CLOSURE_NOT_REAPED.into(),
                }
            } else {
                ToolExecutionTerminal::Settled {
                    exit_code: closure.exit_code,
                    signal: closure.signal,
                    closure: ToolProcessClosure {
                        backend: HOST_PROCESS_BACKEND.into(),
                        namespace_inode: 0,
                        init_host_pid: 0,
                        init_start_ticks: 0,
                        all_children_reaped: true,
                        init_reaped: true,
                        // The host shell proves no isolation of any kind. These
                        // flags stay in the evidence for audit; they are never
                        // a physical-closure criterion.
                        kernel_hardening_applied: false,
                        network_disabled: false,
                        project_read_only: false,
                        status_channel_protected: false,
                    },
                    stop_reason: if closure.exit_code.is_some() {
                        ToolExecutionStopReason::CommandExit
                    } else {
                        // The host executor reports the terminating signal but
                        // not its cause, and `Cancelled` is the only existing
                        // terminal reason that denotes a signal-terminated
                        // (non-exiting) process. The closure criterion never
                        // reads this field.
                        ToolExecutionStopReason::Cancelled
                    },
                }
            }
        }
    };
    HostProcessReport {
        terminal,
        stdout_sha256: hex_digest(stdout.as_bytes()),
        stderr_sha256: hex_digest(stderr.as_bytes()),
        stdout_bytes: stdout.len() as u64,
        stderr_bytes: stderr.len() as u64,
        capture_complete,
    }
}

/// The durable closure document that backs the host process evidence.
pub(crate) fn closure_document(
    request: &RuntimeToolExecutionRequest,
    report: &HostProcessReport,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "version": 1,
        "backend": HOST_PROCESS_BACKEND,
        "tool_name": request.tool_name,
        "tool_use_id": request.tool_use_id,
        "idempotency_key": request.idempotency_key,
        "terminal": report.terminal,
        "stdout_sha256": report.stdout_sha256,
        "stderr_sha256": report.stderr_sha256,
        "stdout_bytes": report.stdout_bytes,
        "stderr_bytes": report.stderr_bytes,
        "capture_complete": report.capture_complete,
    }))
    .unwrap_or_default()
}

/// Build the durable `ToolExecution` fact for one host process effect.
pub(crate) fn build_host_process_fact(
    request: &RuntimeToolExecutionRequest,
    descriptor: &ToolEffectDescriptor,
    report: &HostProcessReport,
    body: ArtifactRef,
    admission: ToolExecutionAdmission,
) -> Result<ToolExecutionFact, String> {
    let _ = descriptor;
    let fact = ToolExecutionFact {
        version: 1,
        admission,
        terminal: report.terminal.clone(),
        body,
        stdout_sha256: report.stdout_sha256.clone(),
        stderr_sha256: report.stderr_sha256.clone(),
        stdout_bytes: report.stdout_bytes,
        stderr_bytes: report.stderr_bytes,
        capture_complete: report.capture_complete,
    };
    validate_host_process_fact(&fact)?;
    if fact.admission.binding.call_key != request.idempotency_key
        || fact.admission.binding.tool_use_id != request.tool_use_id
    {
        return Err("host process fact admission is not this effect".into());
    }
    Ok(fact)
}

/// The digest of the authorized program text of a host process effect.
pub(crate) fn code_digest(request: &crate::RuntimeToolExecutionRequest) -> String {
    hex_digest(request.input.as_bytes())
}

/// The digest that binds a host process fact to the authority it ran under.
pub(crate) fn authority_digest(request: &crate::RuntimeToolExecutionRequest) -> Result<String, String> {
    let replay = crate::execution_core::graph::receipt_replay_contract(request)
        .map_err(|error: crate::execution_core::graph::ExecutionCommitError| error.to_string())?;
    canonical_digest(
        "cowd.host_process.authority.v1",
        &serde_json::json!({
            "replay": replay,
            "authorization": request.authorization,
            "parent_execution_attempt": request.parent_execution_attempt,
        }),
    )
}

/// A test-only admission for one host process effect.
///
/// Production always publishes the admission before the command runs; the
/// negative tests need the same object without the durable event, so they can
/// prove that a fact without a published admission still never closes.
#[cfg(test)]
pub(crate) fn test_admission(request: &crate::RuntimeToolExecutionRequest) -> ToolExecutionAdmission {
    ToolExecutionAdmission {
        version: 1,
        binding: harness_contract::context::ToolExecutionBinding {
            call_key: request.idempotency_key.clone(),
            tool_use_id: request.tool_use_id.clone(),
            input_sha256: "sha256:".to_string() + &"1".repeat(64),
            descriptor_sha256: "sha256:".to_string() + &"2".repeat(64),
            authority_sha256: "sha256:".to_string() + &"3".repeat(64),
        },
        code_sha256: "4".repeat(64),
        inputs: Vec::new(),
        source_bundle: ArtifactRef {
            sha256: format!("sha256:{}", "5".repeat(64)),
            bytes: 4,
            media_type: "application/octet-stream".into(),
            visibility_scope: format!("session:{}", request.session_id.as_deref().unwrap_or_default()),
            selector: "artifact://test-source".into(),
            durability: harness_contract::context::EvidenceDurability::Durable,
        },
    }
}

/// The honest shape of a published host process admission.
pub(crate) fn validate_host_process_admission(
    admission: &ToolExecutionAdmission,
) -> Result<(), String> {
    if admission.version != 1 {
        return Err("host process admission version is not supported".into());
    }
    if admission.binding.call_key.trim().is_empty()
        || admission.binding.tool_use_id.trim().is_empty()
        || admission.code_sha256.len() != 64
        || !admission.code_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !admission.inputs.is_empty()
    {
        return Err("host process admission does not bind one authorized program".into());
    }
    for digest in [
        &admission.binding.input_sha256,
        &admission.binding.descriptor_sha256,
        &admission.binding.authority_sha256,
    ] {
        let bare = digest.strip_prefix("sha256:").unwrap_or(digest.as_str());
        if bare.len() != 64 || !bare.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("host process admission digest is not canonical".into());
        }
    }
    Ok(())
}

/// The honest shape of a host process fact.
///
/// A host process fact must never look like the private bounded-process
/// capability: it may not claim a private namespace identity and it may not
/// claim any hardening, network isolation, read-only project or protected
/// status channel, because the host path proves none of them.
pub(crate) fn validate_host_process_fact(fact: &ToolExecutionFact) -> Result<(), String> {
    let invalid = |message: &str| message.to_string();
    if fact.version != 1 || fact.admission.version != 1 {
        return Err(invalid("host process fact version is not supported"));
    }
    if fact.admission.code_sha256.len() != 64
        || !fact
            .admission
            .code_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || fact.stdout_sha256.len() != 64
        || !fact
            .stdout_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || fact.stderr_sha256.len() != 64
        || !fact
            .stderr_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid("host process fact digest is not canonical"));
    }
    let ToolExecutionTerminal::Settled {
        exit_code,
        signal,
        closure,
        ..
    } = &fact.terminal
    else {
        return match &fact.terminal {
            ToolExecutionTerminal::Unknown { reason_code } if !reason_code.is_empty() => Ok(()),
            _ => Err(invalid(
                "host process fact terminal is not a settled or honest unknown",
            )),
        };
    };
    if exit_code.is_some() == signal.is_some() {
        return Err(invalid(
            "host process fact exit code and signal are not exclusive",
        ));
    }
    if !closure.all_children_reaped || !closure.init_reaped {
        return Err(invalid("host process fact does not prove quiescence"));
    }
    if closure.backend != HOST_PROCESS_BACKEND
        || closure.namespace_inode != 0
        || closure.init_host_pid != 0
        || closure.init_start_ticks != 0
        || closure.kernel_hardening_applied
        || closure.network_disabled
        || closure.project_read_only
        || closure.status_channel_protected
    {
        return Err(invalid(
            "host process fact claims a capability the host path cannot prove",
        ));
    }
    Ok(())
}

/// Build the durable observation that carries the host process fact.
pub(crate) fn host_process_observation(
    request: &RuntimeToolExecutionRequest,
    fact: ToolExecutionFact,
) -> ObservedEvidence {
    ObservedEvidence {
        obligation_id: String::new(),
        target: EvidenceTargetIdentity::ToolExecution { execution: fact },
        observed_at_sequence: request.observation_wave_sequence,
        tool_name: request.tool_name.clone(),
        provenance: ObservedEvidenceProvenance::FreshExecution,
        evidence_ref: Some(EvidenceAccessRef::unavailable(
            harness_contract::reality::EvidenceRef::observed(
                "tool_execution",
                format!("tool-effect:{}", request.idempotency_key),
            ),
            "application/vnd.cowd.tool-receipt+json",
            format!(
                "session:{}",
                request.session_id.as_deref().unwrap_or_default()
            ),
        )),
        model_observation: None,
        workspace_prior_state: None,
    }
}

/// The admitted source archive of a host process effect.
pub(crate) fn source_bundle_bytes(
    request: &RuntimeToolExecutionRequest,
) -> Result<Vec<u8>, String> {
    if request.input.len() > MAX_SOURCE_BUNDLE_BYTES {
        return Err("host process source bundle exceeds the admitted bound".to_string());
    }
    Ok(request.input.as_bytes().to_vec())
}

/// The bounded closure document of a host process effect.
pub(crate) fn body_bytes(
    request: &RuntimeToolExecutionRequest,
    report: &HostProcessReport,
) -> Result<Vec<u8>, String> {
    let bytes = closure_document(request, report);
    if bytes.len() > MAX_BODY_BYTES {
        return Err("host process closure document exceeds the admitted bound".to_string());
    }
    Ok(bytes)
}
