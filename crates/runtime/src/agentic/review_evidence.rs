mod coverage;
mod current_review;
mod identity;
pub(crate) use identity::review_result_identity;
mod policy;
pub(crate) mod read_source;
pub(crate) mod reason_code;
mod results;

// The rebuild record and its entry point belong to the review evidence domain,
// but a refusal travels outward as a reason string, so the runner half of the
// rebuild loop (T1b) consumes them through this module rather than reaching
// into `results`.
pub(crate) use results::{
    node_review_ref, pin_result_source_ticketed, ReviewEvidenceError, ReviewRepinTicket,
};
// The rebuild entry and its two refusal shapes are consumed only by tests until
// T1b wires them into dispatch. Keeping them unconditional trips
// #![deny(unused_imports)] in the production profile, so they are gated exactly
// like `coverage::evidence_contains` below; T1b's wiring lifts this gate back
// with no other edit.
#[cfg(test)]
pub(crate) use results::{repin_at_fresh_head, RepinRejection, ReviewSourceMovement};
// The record-path entries re-verify a proof without a live action, so they
// are the fixture and completion-lane entry points as well.
#[cfg(test)]
pub(crate) use coverage::evidence_contains;
pub(crate) use coverage::validate_review_coverage;
pub(crate) use current_review::{AuthenticatedHistoricalPolicyCall, AuthenticatedMemberApprovalEvidence, CurrentReviewContext, ReviewActionIdentity};
#[allow(unused_imports)]
pub(crate) use current_review::AuthenticatedMemberLeaseEvidence;
#[allow(unused_imports)]
pub(crate) use current_review::{authenticate_original_member_lease_transition, AuthenticatedMemberLeaseTransition};
#[allow(unused_imports)]
pub(crate) use policy::{
    completion_admission_context, completion_admission_context_from_prepared, effect_review_snapshot,
    proof_input_snapshot, proof_input_snapshot_for_action, root_self_review_policy,
    validate_applied_proof_inputs, validate_proof_inputs, validate_proof_inputs_for_action,
    CompletionAdmissionContext, ProofInputSnapshot, VerifiedConclusionProof,
    validate_applied_proof_for_conclusion,
};
pub(crate) use policy::{nested_parent_material, read_delivery_snapshot_for_scope};
#[cfg(test)]
pub(crate) use policy::{verify_nested_read_for_test, collect_actual_nested_from_original_host_for_test};
// Independent review reads existing Runtime receipts; it never mints effects.
use std::collections::{BTreeMap, BTreeSet};

use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::{AgentActorBinding, AgentActorKind};
use harness_contract::goal::{ObjectiveResultReadProof, ObjectiveReviewVerification};
use sha2::{Digest, Sha256};

use super::program::AgenticProgramProjection;
use crate::RuntimeServices;

/// The closing producer-metadata fence of an independent review.
///
/// Every published cut came out of a bounded read: the resolution paged each
/// stream up to that cut and validated its per-source sequence, so a stream that
/// grew afterwards added nothing this review can cite. Comparing the live head
/// for strict equality is the same defect the result package's own cut fence had
/// (030 T7): a busy Session could never settle, and this sentence is recognised
/// by neither review classifier, so the refusal landed untyped. The comparison now
/// goes through the one shared judgment. What still refuses is unchanged - growth
/// of a source the review judges, a head that moved backwards, and a cut above the
/// head - and the refusal wording is kept character for character.
fn verify_producer_metadata_cuts<'sources>(
    store: &crate::RuntimeEventStore,
    sources: impl Iterator<Item = &'sources crate::ExpectedStreamRevision>,
) -> Result<(), String> {
    for source in sources {
        let observed = store
            .stream_revision(&source.stream_id)
            .map_err(|error| error.to_string())?;
        if !results::result_cut_remains_consistent(
            &source.stream_id,
            source.expected_revision,
            observed,
        ) {
            return Err(
                "producer metadata changed during result resolution; retry current results".into(),
            );
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ResultContentReaders<'reader> {
    pub artifacts: Option<&'reader crate::ArtifactStore>,
    pub history: Option<&'reader session::SessionHistoryReader>,
    pub process_inputs: Option<&'reader ProcessInputFrame>,
}

#[derive(Clone)]
pub(crate) struct ProcessInputVerifier {
    scope_locks: std::sync::Arc<crate::execution_core::graph::resources::ScopeLockManager>,
    workspace_root: Option<std::path::PathBuf>,
}

pub(crate) struct ProcessInputFrame {
    verifier: Option<ProcessInputVerifier>,
    retained: std::sync::Mutex<BTreeMap<String, ProcessInputGuard>>,
    artifact_owner: String,
    artifact_pins: std::sync::Mutex<BTreeMap<String, ReviewArtifactPin>>,
}

struct ReviewArtifactPin {
    store: crate::ArtifactStore,
    artifact: harness_contract::context::ArtifactRef,
    owner: String,
}

impl Drop for ReviewArtifactPin {
    fn drop(&mut self) {
        let selector_hash = format!("{:x}", Sha256::digest(self.artifact.selector.as_bytes()));
        let owner_hash = format!("{:x}", Sha256::digest(self.owner.as_bytes()));
        match self.store.unpin(&self.artifact, &self.owner) {
            Ok(()) => tracing::info!(%selector_hash, %owner_hash,
                cleanup = "unpin_returned_ok", record_class = "not_rolled_back",
                "review artifact pin cleanup"),
            Err(_) => tracing::warn!(%selector_hash, %owner_hash,
                cleanup = "unpin_unconfirmed", record_class = "not_rolled_back",
                "review artifact pin cleanup failed"),
        }
    }
}

struct ProcessInputGuard {
    _lease: crate::execution_core::graph::resources::ScopeLockLease,
    _descriptors: Vec<std::fs::File>,
    checked_runs: BTreeSet<String>,
    protected_files: BTreeMap<String, (String, u64)>,
}

impl ProcessInputVerifier {
    pub(crate) fn new(
        scope_locks: std::sync::Arc<crate::execution_core::graph::resources::ScopeLockManager>,
    ) -> Self {
        Self { scope_locks, workspace_root: None }
    }

    pub(crate) fn with_workspace(scope_locks: std::sync::Arc<crate::execution_core::graph::resources::ScopeLockManager>, workspace_root: std::path::PathBuf) -> Self {
        Self { scope_locks, workspace_root: Some(workspace_root) }
    }

    pub(crate) fn frame(&self) -> ProcessInputFrame {
        ProcessInputFrame {
            verifier: Some(self.clone()),
            retained: std::sync::Mutex::new(BTreeMap::new()),
            artifact_owner: format!("review-proof:{}", uuid::Uuid::new_v4()),
            artifact_pins: std::sync::Mutex::new(BTreeMap::new()),
        }
    }
}

impl ProcessInputFrame {
    pub(crate) fn without_process_verifier() -> Self {
        Self {
            verifier: None,
            retained: std::sync::Mutex::new(BTreeMap::new()),
            artifact_owner: format!("review-proof:{}", uuid::Uuid::new_v4()),
            artifact_pins: std::sync::Mutex::new(BTreeMap::new()),
        }
    }
    fn retain_artifact(&self, store: &crate::ArtifactStore,
        artifact: &harness_contract::context::ArtifactRef) -> Result<(), String> {
        let mut pins = self.artifact_pins.lock().map_err(|error| error.to_string())?;
        if let Some(existing) = pins.get(&artifact.selector) {
            if existing.artifact != *artifact {
                return Err("review artifact identity changed while preparing".into());
            }
            return Ok(());
        }
        if let Err(error) = store.pin(artifact, &self.artifact_owner, crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS) {
            let selector_hash = format!("{:x}", Sha256::digest(artifact.selector.as_bytes()));
            let owner_hash = format!("{:x}", Sha256::digest(self.artifact_owner.as_bytes()));
            tracing::warn!(%selector_hash, %owner_hash, pin_result = "error_unclassified",
                cleanup = "not_attempted", record_class = "may_have_become_live",
                "review artifact pin was not confirmed");
            return Err(error.to_string());
        }
        pins.insert(artifact.selector.clone(), ReviewArtifactPin {
            store: store.clone(), artifact: artifact.clone(), owner: self.artifact_owner.clone(),
        });
        Ok(())
    }
    fn retains_exact_artifact(&self, artifact: &harness_contract::context::ArtifactRef) -> bool {
        self.artifact_pins.lock().ok().and_then(|pins| pins.get(&artifact.selector)
            .map(|pin| pin.artifact == *artifact)).unwrap_or(false)
    }
    pub(crate) fn retains_pinned_read(&self, selector: &str, digest: &str, bytes: u64) -> bool {
        self.artifact_pins.lock().ok().and_then(|pins| pins.get(selector).map(|pin|
            pin.artifact.selector == selector
                && pin.artifact.sha256.trim_start_matches("sha256:") == digest.trim_start_matches("sha256:")
                && pin.artifact.bytes == bytes)).unwrap_or(false)
    }
    pub(crate) fn has_original_file_workspace(&self) -> bool {
        self.verifier.as_ref().is_some_and(|verifier| verifier.workspace_root.is_some())
    }

    /// Retain the exact authenticated File acquisition under the existing
    /// workspace owner. This protects writers using the same scope manager;
    /// external writers are not represented by that lease.
    pub(crate) fn retain_original_file(&self, target: &harness_contract::goal::ProofEffectTarget, sha256: &str, bytes: u64) -> Result<(), String> {
        #[cfg(target_os = "linux")]
        {
            use std::io::Read;
            use std::os::unix::fs::MetadataExt;
            use sha2::{Digest, Sha256};
            use crate::execution_core::graph::resources::{ScopeLockMode, ScopeLockRequest, ScopedResource};
            let verifier = self.verifier.as_ref().ok_or("original File verifier unavailable")?;
            let workspace = verifier.workspace_root.as_deref().ok_or("original File workspace owner unavailable")?;
            let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(workspace).map_err(|error| error.to_string())?;
            let path = &target.path.workspace_relative_path;
            let canonical = resolver.resolve_existing(path).map_err(|error| error.to_string())?;
            if !target.exact || canonical.workspace_id != target.path.workspace_id
                || canonical.repository_id != target.path.repository_id
                || canonical.workspace_relative_path != *path
                || canonical.repository_relative_path != target.path.repository_relative_path
                || canonical.object_kind != target.path.object_kind {
                return Err("original File identity differs from workspace owner".into());
            }
            let mut retained = self.retained.lock().map_err(|_| "original File frame poisoned")?;
            let key = resolver.workspace_id().to_string();
            let lease = if retained.contains_key(&key) { None } else {
                Some(verifier.scope_locks.try_acquire([ScopeLockRequest {
                    scope: ScopedResource::workspace(&key).map_err(|error| error.to_string())?, mode: ScopeLockMode::Read,
                }]).map_err(|error| error.to_string())?.ok_or("original File scope busy")?)
            };
            let root = std::fs::File::from(rustix::fs::openat2(rustix::fs::CWD, workspace,
                rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(), rustix::fs::ResolveFlags::NO_SYMLINKS | rustix::fs::ResolveFlags::NO_MAGICLINKS)
                .map_err(|error| error.to_string())?);
            let open = || -> Result<std::fs::File, String> {
                rustix::fs::openat2(&root, path.as_str(), rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::CLOEXEC | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK,
                    rustix::fs::Mode::empty(), rustix::fs::ResolveFlags::BENEATH | rustix::fs::ResolveFlags::NO_SYMLINKS | rustix::fs::ResolveFlags::NO_MAGICLINKS)
                    .map(std::fs::File::from).map_err(|error| error.to_string())
            };
            let root_before = root.metadata().map_err(|error| error.to_string())?;
            let named_root = workspace.symlink_metadata().map_err(|error| error.to_string())?;
            if !root_before.is_dir() || named_root.file_type().is_symlink() || root_before.dev() != named_root.dev() || root_before.ino() != named_root.ino() {
                return Err("original File workspace changed".into());
            }
            let mut file = open()?;
            let before = file.metadata().map_err(|error| error.to_string())?;
            if !before.is_file() || before.nlink() != 1 || before.len() != bytes { return Err("original File extent changed".into()); }
            let mut hash = Sha256::new();
            let mut total = 0u64;
            let mut buffer = [0u8; 65536];
            loop { let count = file.read(&mut buffer).map_err(|error| error.to_string())?; if count == 0 { break; }
                total = total.checked_add(count as u64).ok_or("original File size overflow")?;
                if total > bytes { return Err("original File grew during validation".into()); }
                hash.update(&buffer[..count]); }
            let after = file.metadata().map_err(|error| error.to_string())?;
            let named = open()?.metadata().map_err(|error| error.to_string())?;
            let identity = |m: &std::fs::Metadata| (m.dev(), m.ino(), m.nlink(), m.len(), m.mtime(), m.mtime_nsec(), m.ctime(), m.ctime_nsec());
            let root_after = workspace.symlink_metadata().map_err(|error| error.to_string())?;
            if total != bytes || format!("{:x}", hash.finalize()) != sha256.trim_start_matches("sha256:")
                || identity(&before) != identity(&after) || identity(&after) != identity(&named)
                || root_after.file_type().is_symlink() || root_after.dev() != root_before.dev() || root_after.ino() != root_before.ino() {
                return Err("original File changed during validation".into());
            }
            if let Some(guard) = retained.get_mut(&key) {
                if guard.protected_files.get(path).is_some_and(|old| old.0.trim_start_matches("sha256:") != sha256.trim_start_matches("sha256:") || old.1 != bytes) {
                    return Err("original File identity conflicts with retained guard".into());
                }
                guard.protected_files.insert(path.clone(), (sha256.to_owned(), bytes));
                guard._descriptors.extend([file, root]);
            } else {
                retained.insert(key, ProcessInputGuard { _lease: lease.ok_or("original File scope lease missing")?, _descriptors: vec![file, root], checked_runs: BTreeSet::new(), protected_files: BTreeMap::from([(path.clone(), (sha256.to_owned(), bytes))]) });
            }
            Ok(())
        }
        #[cfg(not(target_os = "linux"))]
        { let _ = (target, sha256, bytes); Err("original File protected FD unsupported".into()) }
    }

    pub(crate) fn retains_process_input_run(&self, call_key: &str) -> bool {
        self.retained.lock().ok().is_some_and(|guards|
            guards.values().any(|guard| guard.checked_runs.contains(call_key)))
    }
    /// Only files actually checked by the original Process input owner are
    /// covered: its workspace read lease and open descriptors remain held.
    pub(crate) fn retains_exact_file(
        &self, target: &harness_contract::goal::ProofEffectTarget,
        digest: &str, bytes: Option<u64>,
    ) -> bool {
        self.retained.lock().ok().and_then(|guards| guards.get(&target.path.workspace_id)
            .and_then(|guard| guard.protected_files.get(&target.path.workspace_relative_path))
            .map(|(sha, size)| sha.trim_start_matches("sha256:") == digest.trim_start_matches("sha256:")
                && bytes.is_none_or(|bytes| bytes == *size))).unwrap_or(false)
    }
    fn validate(
        &self,
        store: &std::sync::Arc<crate::RuntimeEventStore>,
        run: &results::AuthenticatedProcessExecution,
    ) -> Result<(), String> {
        use crate::execution_core::graph::resources::{
            ScopeLockMode, ScopeLockRequest, ScopedResource,
        };
        let mut cuts = BTreeMap::new();
        let admitted = policy::verify_material_event(store, &run.admission, &mut cuts)?;
        let primary = policy::verify_material_event(store, &run.primary, &mut cuts)?;
        let root = admitted.payload["source_workspace_root"]
            .as_str()
            .filter(|root| !root.is_empty())
            .ok_or("bounded process protected source workspace missing")?;
        let workspace = std::path::Path::new(root);
        if !workspace.is_absolute()
            || workspace
                .canonicalize()
                .map_err(|error| error.to_string())?
                != workspace
        {
            return Err(
                "bounded process protected source workspace changed or is not canonical".into(),
            );
        }
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(workspace)
            .map_err(|error| error.to_string())?;
        let descriptor = results::receipt_effect_descriptor(&primary)?;
        let scope = results::require_original_effect_scope(
            &primary.payload["resolved_effect_scope"],
            &descriptor,
        )?;
        if scope
            .targets
            .iter()
            .any(|target| target.path.workspace_id != resolver.workspace_id())
        {
            return Err(
                "bounded process protected source workspace/scope namespace mismatch".into(),
            );
        }
        let mut retained = self
            .retained
            .lock()
            .map_err(|_| "bounded process input frame poisoned")?;
        let key = resolver.workspace_id().to_string();
        if let Some(guard) = retained.get_mut(&key) {
            let descriptors = policy::validate_current_process_inputs(workspace, run)?;
            for input in &run.fact.admission.inputs {
                let identity = (input.sha256.clone(), input.bytes);
                if guard.protected_files.insert(input.relative_path.clone(), identity.clone())
                    .is_some_and(|previous| previous != identity) {
                    return Err("bounded process protected File identity changed".into());
                }
            }
            if guard
                .checked_runs
                .insert(run.fact.admission.binding.call_key.clone())
            {
                guard._descriptors.extend(descriptors);
            }
            return Ok(());
        }
        let request = ScopeLockRequest {
            scope: ScopedResource::workspace(resolver.workspace_id())
                .map_err(|error| error.to_string())?,
            mode: ScopeLockMode::Read,
        };
        let lease = self
            .verifier
            .as_ref().ok_or("bounded process selected input verifier context missing")?
            .scope_locks
            .try_acquire([request])
            .map_err(|error| error.to_string())?
            .ok_or("effect_review_pending: bounded process input scope busy; retry")?;
        let descriptors = policy::validate_current_process_inputs(workspace, run)?;
        retained.insert(
            key,
            ProcessInputGuard {
                _lease: lease,
                _descriptors: descriptors,
                checked_runs: BTreeSet::from([run.fact.admission.binding.call_key.clone()]),
                protected_files: run.fact.admission.inputs.iter().map(|input|
                    (input.relative_path.clone(), (input.sha256.clone(), input.bytes))).collect(),
            },
        );
        Ok(())
    }
}

fn validate_selected_process_inputs(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    run: &results::AuthenticatedProcessExecution,
    readers: ResultContentReaders<'_>,
) -> Result<(), String> {
    readers
        .process_inputs
        .ok_or("bounded process selected input verifier context missing")?
        .validate(store, run)
}

#[cfg(test)]
tokio::task_local! {
    static PROOF_MATERIAL_CUT_BARRIER: std::cell::RefCell<Option<(
        String,
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    )>>;
}

#[cfg(test)]
pub(crate) async fn with_proof_material_cut_barrier<ReviewOutput>(
    source: String,
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
    review: impl std::future::Future<Output = ReviewOutput>,
) -> ReviewOutput {
    PROOF_MATERIAL_CUT_BARRIER
        .scope(
            std::cell::RefCell::new(Some((source, entered, release))),
            review,
        )
        .await
}

pub(crate) fn validate_original_effect_scope(
    value: &serde_json::Value,
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
) -> Result<harness_contract::goal::ProofEffectScope, String> {
    results::require_original_effect_scope(value, descriptor)
}

pub(crate) fn proof_value_digest(value: &impl serde::Serialize) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("serializable proof input"))
    )
}

pub(crate) fn required_review_conditions(
    goal: &harness_contract::goal::GoalContract,
) -> BTreeSet<String> {
    goal.user_intent_criterion_id
        .iter()
        .cloned()
        .chain(
            goal.obligations
                .iter()
                .filter(|obligation| obligation.required)
                .map(|obligation| obligation.obligation_id.clone()),
        )
        .chain(
            goal.criteria
                .iter()
                .filter(|criterion| {
                    criterion.status == harness_contract::goal::AcceptanceStatus::Satisfied
                })
                .map(|criterion| criterion.id.clone()),
        )
        .collect()
}

/// The reference that names one material receipt.
///
/// A receipt alias is only ever built from a genuine receipt. The delegated
/// receipt stream also carries the intent of every call, and that intent
/// carries its own logical sequence under the same payload key a receipt
/// uses for its observation sequence. A raw stream scan therefore read the
/// intent's sequence as a receipt selector, and the intent collided with the
/// read it was supposed to point at: the collector then refused the read as
/// "not an actual material receipt" even though the read had really
/// happened. Every other event keeps the plain event reference, so an intent
/// is never mistaken for a read.
pub(crate) fn receipt_alias(source: &str, event: &crate::DurableRuntimeEvent) -> String {
    if source.starts_with("execution-agent-receipts:")
        && event.kind == "execution.agent_tool.receipt"
    {
        return format!(
            "{}#{}",
            source,
            event.payload["sequence"].as_u64().unwrap_or_default()
        );
    }
    format!("runtime-event:{}", event.event_id)
}

pub(crate) fn bound_artifact_identities(
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<Vec<String>, String> {
    match identity::auxiliary_identity(program, reference)? {
        identity::MaterialIdentity::Publication(reference) => {
            let artifact = &program.artifacts[&reference];
            Ok(vec![reference, artifact.content_ref.clone()])
        }
        _ => Ok(Vec::new()),
    }
}

/// Bounded diagnostic only: this never changes source acceptance or proof cuts.
pub(crate) fn describe_source_advance(
    store: &crate::RuntimeEventStore,
    source: &str,
    after: u64,
) -> String {
    const MAX_ADVANCED_EVENTS: usize = 8;
    let head = match store.stream_revision(source) {
        Ok(head) => head,
        Err(_) => return "writer_advance=unknown head_read_failed".into(),
    };
    let events = match store.list_stream_after(
        source,
        after,
        head.max(after),
        MAX_ADVANCED_EVENTS,
        256 * 1024,
    ) {
        Ok(events) => events,
        Err(_) => return format!("writer_advance=unknown head={head} event_read_failed"),
    };
    #[cfg(test)]
    for event in &events {
        if matches!(
            event.kind.as_str(),
            "execution_graph.command_applied" | "execution_graph.recovered"
        ) {
            crate::agentic::execution::reviewed_goal_fixture::diagnostic_1120::record(
                "source_advance.original_event",
                serde_json::json!({"source":source,"after":after,"head":head,"record":event}),
            );
        }
    }
    let writers = events
        .iter()
        .map(|event| {
            format!(
                "#{}:{}:{}",
                event.sequence, event.kind, event.transaction_id
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("advanced={} head={head} [{writers}]", events.len())
}

#[cfg(test)]
mod bound_artifact_tests {
    use super::*;
    use crate::agentic::program::AgenticArtifactProjection;

    fn artifact(artifact_ref: &str, content_ref: &str) -> AgenticArtifactProjection {
        AgenticArtifactProjection {
            artifact_ref: artifact_ref.into(),
            content_ref: content_ref.into(),
            kind: "report".into(),
            title: String::new(),
            relates_to: Vec::new(),
            committed_by: "agent".into(),
            claim_execution_id: None,
            claim_generation: None,
        }
    }

    fn program(artifacts: Vec<AgenticArtifactProjection>) -> AgenticProgramProjection {
        let mut program = AgenticProgramProjection::empty("program", "objective");
        for artifact in artifacts {
            program
                .artifacts
                .insert(artifact.artifact_ref.clone(), artifact);
        }
        program
    }

    /// A logical artifact reference and the content it is bound to are TWO
    /// identities of one artifact. Both are returned, so neither namespace is
    /// dressed as the other.
    #[test]
    fn r1_a_logical_artifact_reference_binds_its_content() {
        let program = program(vec![artifact(
            "artifact:5efe90ba23d164d6672da34f",
            "artifact://art_99d6cafe58c24009872332b4880508a0",
        )]);
        assert_eq!(
            bound_artifact_identities(&program, "artifact:5efe90ba23d164d6672da34f")
                .expect("one artifact"),
            vec![
                "artifact:5efe90ba23d164d6672da34f".to_string(),
                "artifact://art_99d6cafe58c24009872332b4880508a0".to_string(),
            ]
        );
        // The dressed spelling of the same logical reference names the same
        // artifact, so the content is bound again and never invented.
        assert_eq!(
            bound_artifact_identities(&program, "artifact://5efe90ba23d164d6672da34f")
                .expect("one artifact"),
            vec![
                "artifact:5efe90ba23d164d6672da34f".to_string(),
                "artifact://art_99d6cafe58c24009872332b4880508a0".to_string(),
            ]
        );
        assert!(bound_artifact_identities(
            &program,
            "artifact://art_99d6cafe58c24009872332b4880508a0"
        )
        .expect("content does not select a publication")
        .is_empty());
    }

    #[test]
    fn r1_shared_content_is_auxiliary_but_not_an_unambiguous_business_result() {
        let program = program(vec![
            artifact(
                "artifact:5efe90ba23d164d6672da34f",
                "artifact://art_99d6cafe58c24009872332b4880508a0",
            ),
            artifact(
                "artifact:6cd2be959677023bc86cdfd4",
                "artifact://art_99d6cafe58c24009872332b4880508a0",
            ),
        ]);
        let reference = "artifact://art_99d6cafe58c24009872332b4880508a0";
        assert!(bound_artifact_identities(&program, reference)
            .unwrap()
            .is_empty());
        assert_eq!(
            identity::auxiliary_identity(&program, reference).unwrap(),
            identity::MaterialIdentity::Content(reference.into())
        );
        let error = review_result_identity(&program, reference)
            .expect_err("two logical identities over one content are ambiguous");
        assert!(error.contains("artifact identity is ambiguous"), "{error}");
        assert_eq!(
            review_result_identity(&program, "artifact:5efe90ba23d164d6672da34f").unwrap(),
            "artifact:5efe90ba23d164d6672da34f"
        );
    }

    #[test]
    fn r1_auxiliary_content_does_not_select_publication_work() {
        let program = program(vec![
            artifact("artifact:original", "artifact://shared"),
            artifact("artifact:stale", "artifact://shared"),
            artifact("artifact:fresh", "artifact://shared"),
        ]);
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let mut sources = BTreeMap::new();
        let (tasks, publications, _) = selected_work(
            &store,
            &program,
            None,
            &["artifact://shared".into()],
            &mut sources,
        )
        .unwrap();
        assert!(tasks.is_empty());
        assert!(publications.is_empty());
        let (_, publications, _) = selected_work(
            &store,
            &program,
            None,
            &["artifact:fresh".into()],
            &mut sources,
        )
        .unwrap();
        assert_eq!(publications, vec!["artifact:fresh".to_string()]);
        assert!(review_result_identity(&program, "artifact://shared").is_err());
    }

    #[test]
    fn r1_auxiliary_receipt_does_not_select_a_content_publication() {
        let program = program(vec![artifact("artifact:report", "tool://receipt")]);
        assert_eq!(
            identity::auxiliary_identity(&program, "tool://receipt").unwrap(),
            identity::MaterialIdentity::Receipt("tool://receipt".into())
        );
        assert!(bound_artifact_identities(&program, "tool://receipt")
            .unwrap()
            .is_empty());
    }

    /// A reference this Program does not own binds nothing: it is not this
    /// Program's artifact, so no content may be invented for it.
    #[test]
    fn r1_a_foreign_reference_binds_nothing() {
        let program = program(vec![artifact(
            "artifact:5efe90ba23d164d6672da34f",
            "artifact://art_99d6cafe58c24009872332b4880508a0",
        )]);
        assert!(
            bound_artifact_identities(&program, "artifact://art_somebody_elses_content")
                .expect("no ambiguity")
                .is_empty(),
            "a foreign content reference binds nothing"
        );
        assert!(
            bound_artifact_identities(&program, "artifact:000000000000000000000000")
                .expect("no ambiguity")
                .is_empty(),
            "an unknown logical reference binds nothing"
        );
        // A non-artifact reference (a tool receipt, a graph) is untouched.
        assert!(
            bound_artifact_identities(&program, "tool://raw-chatcmpl-tool-1")
                .expect("no ambiguity")
                .is_empty()
        );
    }
}

fn canonical_review_result_refs(
    program: &AgenticProgramProjection,
    references: &[String],
) -> Result<BTreeSet<String>, String> {
    references
        .iter()
        .map(|reference| review_result_identity(program, reference))
        .collect()
}

fn selected_work(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &AgenticProgramProjection,
    goal: Option<&harness_contract::goal::GoalContract>,
    references: &[String],
    publication_sources: &mut BTreeMap<String, u64>,
) -> Result<(Vec<String>, Vec<String>, String), results::ReviewEvidenceError> {
    let required = goal
        .into_iter()
        .flat_map(|goal| &goal.obligations)
        .filter(|obligation| obligation.required)
        .map(|obligation| obligation.obligation_id.as_str())
        .collect::<BTreeSet<_>>();
    let normalize = |reference: &str| identity::auxiliary_reference(program, reference);
    let mut artifacts = references
        .iter()
        .map(|reference| normalize(reference))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut task_artifacts = BTreeMap::new();
    for task in program.tasks.values() {
        task_artifacts.insert(
            task.task_id.clone(),
            canonical_review_result_refs(program, &task.artifact_refs),
        );
    }
    let mut pending = program
        .tasks
        .values()
        .filter(|task| {
            artifacts.contains(&task.task_id)
                || task_artifacts[&task.task_id]
                    .as_ref()
                    .is_ok_and(|references| references.iter().any(|reference| artifacts.contains(reference)))
                || task
                    .obligation_refs
                    .iter()
                    .any(|reference| required.contains(reference.as_str()))
        })
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let mut tasks = BTreeMap::new();
    let mut selected = BTreeMap::new();
    loop {
        while let Some(reference) = pending.pop() {
            if tasks.contains_key(&reference) {
                continue;
            }
            let task = program
                .tasks
                .get(&reference)
                .ok_or("proof dependency Task missing")?;
            pending.extend(task.depends_on.iter().cloned());
            if !task.replacement_task_refs.is_empty()
                || task.status == super::program::AgenticTaskStatus::Superseded
                || task.pending_retirement
                    == Some(super::program::AgenticTaskRetirement::Superseded)
            {
                if task.replacement_task_refs.is_empty()
                    || (task.status == super::program::AgenticTaskStatus::Superseded
                        && !super::work_market::task_dependency_satisfied(program, &reference))
                {
                    return Err("proof replacement dependency is not satisfied".into());
                }
                pending.extend(task.replacement_task_refs.iter().cloned());
            }
            artifacts.extend(
                task_artifacts[&task.task_id]
                    .as_ref()
                    .map_err(Clone::clone)?
                    .iter()
                    .cloned(),
            );
            let contract = semantic_task_contract(task);
            tasks.insert(reference, serde_json::json!({
                "team":contract["team_id"], "objective":contract["objective"], "acceptance":contract["acceptance"],
                "acceptance_checks":contract["acceptance_checks"], "criterion_source":contract["criterion_source"],
                "obligations":contract["obligation_refs"], "capabilities":contract["required_capabilities"],
                "execution_requirements":contract["execution_requirements"], "purpose":contract["purpose"],
                "dependencies":contract["depends_on"], "artifacts":contract["artifact_refs"],
                "unresolved":contract["unresolved"], "replacements":contract["replacement_task_refs"],
            }));
        }
        let mut pending_artifacts = std::mem::take(&mut artifacts)
            .into_iter()
            .collect::<Vec<_>>();
        while let Some(reference) = pending_artifacts.pop() {
            if selected.contains_key(&reference) {
                continue;
            }
            if let Some(artifact) = program.artifacts.get(&reference) {
                for dependency in &artifact.relates_to {
                    let normalized = normalize(dependency)?;
                    if program.artifacts.contains_key(&normalized) {
                        pending_artifacts.push(normalized);
                    }
                    if program.tasks.contains_key(dependency) {
                        if !task_artifacts[dependency]
                            .as_ref()
                            .map_err(Clone::clone)?
                            .contains(&artifact.artifact_ref)
                        {
                            results::authenticate_publication_task_link(
                                store,
                                program,
                                artifact,
                                dependency,
                                publication_sources,
                            )?;
                        }
                        if !tasks.contains_key(dependency) {
                            pending.push(dependency.clone());
                        }
                    }
                }
                selected.insert(
                    artifact.artifact_ref.clone(),
                    serde_json::to_value(artifact).map_err(|error| error.to_string())?,
                );
            }
        }
        if pending.is_empty() {
            break;
        }
    }
    let mut dependency_counts = BTreeMap::new();
    let mut dependents = BTreeMap::<String, Vec<String>>::new();
    for reference in tasks.keys() {
        let task = &program.tasks[reference];
        let dependencies = task
            .depends_on
            .iter()
            .chain(&task.replacement_task_refs)
            .cloned()
            .collect::<BTreeSet<_>>();
        dependency_counts.insert(reference.clone(), dependencies.len());
        for dependency in dependencies {
            if !tasks.contains_key(&dependency) {
                return Err("proof dependency closure is incomplete".into());
            }
            dependents
                .entry(dependency)
                .or_default()
                .push(reference.clone());
        }
    }
    let mut ready = dependency_counts
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(reference, _)| reference.clone())
        .collect::<Vec<_>>();
    let mut resolved_tasks = 0usize;
    while let Some(reference) = ready.pop() {
        resolved_tasks += 1;
        if let Some(references) = dependents.get(&reference) {
            for dependent in references {
                let count = dependency_counts
                    .get_mut(dependent)
                    .ok_or("proof dependency disappeared")?;
                *count -= 1;
                if *count == 0 {
                    ready.push(dependent.clone());
                }
            }
        }
    }
    if resolved_tasks != tasks.len() {
        return Err("proof dependency or replacement cycle".into());
    }
    let digest = proof_value_digest(&serde_json::json!({
        "domain":"cowd.proof.work.v2","program":program.program_id,"objective":program.objective_id,
        "continuation":program.continuation,"tasks":tasks,"artifacts":selected,
        "required_teams":program.required_team_count,"exact_teams":program.exact_team_count,
    }));
    Ok((
        tasks.into_keys().collect(),
        selected.into_keys().collect(),
        digest,
    ))
}

/// One semantic Task projection shared by complete and dependency-selected work.
/// Physical facts remain available to completion/effect/claim owners and source CAS.
fn semantic_task_contract(task: &super::program::AgenticTaskProjection) -> serde_json::Value {
    let mut value = serde_json::to_value(task).expect("serializable Task projection");
    let fields = value.as_object_mut().expect("Task object");
    for field in [
        "status",
        "claimant",
        "claim_generation",
        "claim_execution_id",
        "claimed_at_ms",
        "lease_expires_at_ms",
        "claim_renewal_streak",
        "active_attempts",
        "failed_attempts",
        "review_generation",
        "failed_review_attempts",
        // 033 S7: the two accounts are bookkeeping, not work - the manifest
        // pins semantic work only.
        "structural_execution_stalls",
        "structural_review_stalls",
        // 033 S8 (R102 rework): the review duty is a workflow duty, not
        // semantic work for the worker, so it stays out of both manifests -
        // and so do the two review measures, which count verdicts rather
        // than state work.
        "review_obligation",
        "reviews_applied",
        "reviews_accepted",
        "last_failure",
        "cancel_requested_by",
        "cancel_reason_ref",
        "cancel_evidence_refs",
        "pending_retirement",
        "reviewed_by",
    ] {
        fields.remove(field);
    }
    value
}

pub(crate) fn work_manifest_digest(program: &AgenticProgramProjection) -> String {
    // Hash semantic work, not lease renewal or retirement of its workers.
    let teams = program.teams.iter().map(|(id, team)| (id, serde_json::json!({
        "team_id":team.team_id,"name":team.name,"mission":team.mission,"objective":team.objective,
        "topic_ref":team.topic_ref,"created_by":team.created_by,"member_ids":team.member_ids,"task_ids":team.task_ids,
    }))).collect::<BTreeMap<_, _>>();
    let memberships = program.memberships.iter().map(|(id, member)| (id, serde_json::json!({
        "membership_id":member.membership_id,"agent_id":member.agent_id,"team_id":member.team_id,
        "delegation_ref":member.delegation_ref,"created_by":member.created_by,
    }))).collect::<BTreeMap<_, _>>();
    let tasks = program
        .tasks
        .iter()
        .map(|(id, task)| (id, semantic_task_contract(task)))
        .collect::<BTreeMap<_, _>>();
    let value = serde_json::json!({
        "program": program.program_id, "objective": program.objective_id,
        "continuation": program.continuation, "teams": teams,
        "memberships": memberships, "agents": program.agents,
        "tasks": tasks, "artifacts": program.artifacts,
        "topics": program.topics, "required_team_count": program.required_team_count,
    });
    format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
}

/// The refusal the model sees when an obligation's producer requirement is not
/// met. Pure so the acceptance can assert the model is told WHICH requirement
/// failed: the old text listed all four, so a Task with the right capability
/// and the wrong definition was told both were wrong.
pub(crate) fn unmet_obligation_error(
    obligation_id: &str,
    required: &harness_contract::goal::ObjectiveProducerContract,
) -> String {
    let mut unmet = Vec::new();
    if let Some(capability) = &required.capability_id {
        unmet.push(format!("capability `{capability}`"));
    }
    if let Some(definition) = &required.agent_definition_ref {
        unmet.push(format!("agent definition `{definition}`"));
    }
    if let Some(skill) = &required.skill_ref {
        unmet.push(format!("skill `{skill}`"));
    }
    if let Some(tool) = &required.tool_ref {
        unmet.push(format!("tool `{tool}`"));
    }
    let detail = if unmet.is_empty() {
        "a Runtime-bound producer with an actual tool receipt".to_string()
    } else {
        unmet.join(", ")
    };
    format!(
        "obligation {obligation_id} requires a producer with {detail}, and no delivered result was \
         produced by one. Publish a Task whose claimant satisfies that requirement, or record the \
         gap explicitly rather than asserting it is covered."
    )
}

/// The refusal the model sees when a review cites no current result. Pure so
/// the acceptance can assert the model is told which refs Runtime holds.
pub(crate) fn no_current_results_error(current: &[String]) -> String {
    let detail = if current.is_empty() {
        "no delivered result is accepted yet, so there is nothing to review; publish and accept a \
         Task first"
            .to_string()
    } else {
        format!(
            "the delivered result refs Runtime currently holds are: [{}]. Cite those (or the \
             `task:<id>` that produced them) in result_refs, then read each one with \
             evidence_retrieve before submitting the review",
            current.join(", ")
        )
    };
    format!("independent review has no current results: {detail}")
}

/// Compact only trusted ToolHost output, before the existing receipt owner
/// applies its size bound. The caller still receives the complete original page.
pub(crate) fn compact_read_receipt(output: &str) -> String {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
        return output.into();
    };
    if value["kind"] != "evidence_retrieve" || value["available"] != true {
        return output.into();
    }
    let Some(chunks) = value["chunks"]
        .as_array()
        .filter(|chunks| chunks.len() <= 16)
    else {
        return output.into();
    };
    let Some(chunks) = chunks
        .iter()
        .map(|chunk| {
            let content = chunk["content"].as_str()?;
            let bytes = crate::context_evidence::read_chunk_bytes(chunk, &value)?;
            Some(
                serde_json::json!({"index":chunk["index"], "characters":content.chars().count(),
            "body_bytes":bytes.len(), "content_sha256":format!("{:x}", Sha256::digest(&bytes))}),
            )
        })
        .collect::<Option<Vec<_>>>()
    else {
        return output.into();
    };
    serde_json::json!({"kind":value["kind"],"available":true,"evidence_ref":value["evidence_ref"],
        "sha256":value["sha256"],"bytes":value["bytes"],"total_chunks":value["total_chunks"],
        "receipt_format":"runtime_chunk_digests_v1", "chunks":chunks})
    .to_string()
}

/// Expand `task:<id>` entries in a review's result refs into that Task's
/// delivered artifacts, and pass everything else through unchanged.
///
/// A model that has just accepted three Tasks naturally reaches for the three
/// refs it was handed. Refusing that costs a review cycle and tells the model
/// nothing about what a "result" is, so the boundary resolves it instead.
/// Artifact refs are already the right shape and are returned as-is, and a task
/// with no artifacts contributes nothing rather than an invented ref.
fn resolve_result_refs(program: &AgenticProgramProjection, result_refs: &[String]) -> Vec<String> {
    let mut resolved = Vec::with_capacity(result_refs.len());
    for reference in result_refs {
        match program.tasks.get(reference) {
            Some(task) => resolved.extend(task.artifact_refs.iter().cloned()),
            None => resolved.push(reference.clone()),
        }
    }
    resolved.sort();
    resolved.dedup();
    resolved
}

pub(crate) struct IndependentReview {
    pub producers: Vec<String>,
    pub verification: ObjectiveReviewVerification,
    /// The required writes this review settled as failed, in the closure
    /// projection's entry shape. They travel BESIDE the proof: the proof's
    /// digests must not change, and a failure that is not named anywhere
    /// would be silence.
    pub required_write_failures: Vec<serde_json::Value>,
}

fn retain_producer_material_cuts(
    material: &BTreeMap<String, u64>,
    snapshot: &[crate::ExpectedStreamRevision],
) -> Result<BTreeMap<String, u64>, String> {
    let mut sources = BTreeMap::new();
    for source in snapshot {
        if sources.insert(source.stream_id.clone(), source.expected_revision)
            .is_some_and(|previous| previous != source.expected_revision)
        {
            return Err("producer snapshot has conflicting source cuts".into());
        }
    }
    for (source, revision) in material {
        if sources.get(source) != Some(revision) {
            return Err("producer material source cut changed during closure".into());
        }
    }
    Ok(sources)
}

struct ReadCoverage {
    proof: ObjectiveResultReadProof,
    total: Option<usize>,
    chunks: BTreeSet<usize>,
}

/// Whether a tool result counts as a physical read of a delivered result.
///
/// `read_ref` is the unified facade the primer names as the canonical reader
/// ("Read Runtime facts through one canonical reference with `read_ref`"), and
/// it resolves the same durable receipt as `evidence_retrieve` with the same
/// payload shape, so both count. `"read"` used to sit in this list and is not a
/// tool name at all, so that arm could never match - which left the facade the
/// primer advertises as the one reader that does not count.
fn counts_as_physical_read(tool_name: &str) -> bool {
    matches!(tool_name, "evidence_retrieve" | "read_ref")
}

/// Whether this read carries its own read-acquisition seal.
///
/// The seal is judged by its value, not by the presence of a key: the store
/// writes the seal into the read's receipt payload, and a key whose value is
/// json null carries no seal - that is the shape the truncated receipt of the
/// live run has. The decoder is used exactly as the seal path uses it, so
/// the seal's decoding and its authentication are unchanged by this fact;
/// it only says whether the seal is there.
fn read_carries_acquisition_seal(event_payload: &serde_json::Value) -> bool {
    let seal = &event_payload[crate::context_evidence::READ_ACQUISITION_FIELD];
    seal.is_object()
        && crate::context_evidence::decode_read_acquisition(seal).is_ok()
}

/// Whether two references name the same object under either spelling.
///
/// Runtime records a one-shot Task deliverable as `artifact:<24 hex>` - the
/// Program-logical key it repeats in the completion gap and in the citable-refs
/// projection - while `read_ref` resolves and echoes it back as
/// `artifact://<24 hex>`. The two are the same object, but a byte comparison of
/// the strings never matches, so a reviewer that read exactly the results it was
/// told to read was refused with `review_requires_current_physical_reads`.
/// `resolve_review_result` already normalises this spelling when it maps the
/// logical key to its selector; the coverage check must use the same rule.
fn same_reference_spelling(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    let normalize = |value: &str| {
        value
            .replacen("://", ":", 1)
            .trim_end_matches('/')
            .to_string()
    };
    normalize(left) == normalize(right)
}

impl ReadCoverage {
    fn observe_delivered(&mut self, read: &policy::VerifiedReadDelivery) -> Result<(), String> {
        if !read.body_delivered() {
            return Ok(());
        }
        let body = &read.material.acquisition.body;
        if body.source_sha256 != self.proof.sha256
            || body.source_bytes != self.proof.bytes
            || !(same_reference_spelling(&body.source_ref, &self.proof.content_ref)
                || same_reference_spelling(&body.source_ref, &self.proof.result_ref))
        {
            return Ok(());
        }
        let crate::context_evidence::ReadExtent::ArtifactChunks {
            total_chunks,
            chunks,
        } = &body.extent
        else {
            return Ok(());
        };
        let total =
            usize::try_from(*total_chunks).map_err(|_| "delivered result extent overflow")?;
        if self.total.is_some_and(|expected| expected != total)
            || (*total_chunks == 0 && self.proof.bytes != 0)
            || chunks.iter().any(|chunk| chunk.index >= *total_chunks)
        {
            return Err("delivered result pages disagree about the actual source extent".into());
        }
        self.total = Some(total);
        self.chunks
            .extend(chunks.iter().map(|chunk| chunk.index as usize));
        self.proof.receipt_refs.push(read.receipt_reference());
        Ok(())
    }

    #[cfg(test)]
    fn observe(&mut self, output: &str, receipt_ref: &str) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(output) else {
            return;
        };
        if value["kind"] != "evidence_retrieve"
            || value["available"] != true
            || value["sha256"].as_str() != Some(self.proof.sha256.as_str())
            || value["bytes"].as_u64() != Some(self.proof.bytes)
            || !value["evidence_ref"].as_str().is_some_and(|reference| {
                reference == self.proof.content_ref
                    || (same_reference_spelling(reference, &self.proof.result_ref)
                        && (reference.starts_with("tool://")
                            || reference.starts_with("artifact://")
                            || reference.starts_with("approval:v1:")))
            })
            || receipt_ref.is_empty()
        {
            return;
        }
        let Some(total) = value["total_chunks"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
        else {
            return;
        };
        let Some(chunks) = value["chunks"].as_array() else {
            return;
        };
        if self.total.is_some_and(|expected| expected != total) {
            return;
        }
        let indices = chunks
            .iter()
            .map(|chunk| {
                let index = usize::try_from(chunk["index"].as_u64()?).ok()?;
                let has_content = chunk["content"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty())
                    || (value["receipt_format"] == "runtime_chunk_digests_v1"
                        && chunk["characters"].as_u64().is_some_and(|count| count > 0)
                        && chunk["content_sha256"].as_str().is_some_and(|hash| {
                            hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                        }));
                (index < total && has_content).then_some(index)
            })
            .collect::<Option<Vec<_>>>();
        let Some(indices) = indices else {
            return;
        };
        if indices.is_empty() && self.proof.bytes != 0 {
            return;
        }
        self.total = Some(total);
        self.chunks.extend(indices);
        self.proof.receipt_refs.push(receipt_ref.into());
    }

    fn complete(&self) -> bool {
        self.total.is_some_and(|total| self.chunks.len() == total)
            && !self.proof.receipt_refs.is_empty()
    }
}

pub(crate) fn validate_explicit_process_evidence(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    verification: &harness_contract::goal::ObjectiveReviewVerification,
    explicit_evidence_refs: &[String],
) -> Result<(), results::ReviewEvidenceError> {
    let claims = verification
        .reads
        .iter()
        .flat_map(|read| &read.effect_observation_refs)
        .filter(|reference| is_process_body_receipt(reference))
        .cloned()
        .collect::<BTreeSet<_>>();
    if explicit_evidence_refs
        .iter()
        .filter(|reference| is_process_body_receipt(reference))
        .any(|reference| !claims.contains(reference))
    {
        return Err(
            "bounded process explicit receipt selection was omitted from this proof".into(),
        );
    }
    let Some(inputs) = verification.input_set.as_ref() else {
        return if claims.is_empty() {
            Ok(())
        } else {
            Err("bounded process selection inputs missing".into())
        };
    };
    let mut cuts = BTreeMap::new();
    let mut originals = Vec::new();
    let mut known_bodies = BTreeSet::new();
    for material in &inputs.material_events {
        let event = policy::verify_material_event(store, material, &mut cuts)?;
        if event.kind == "execution.effect.receipt" && results::has_process_fact(&event) {
            let run = results::authenticate_bounded_process(store, &event)?;
            known_bodies.insert(run.fact.body.selector);
        }
        originals.push(event);
    }
    let mut expected_bodies = known_bodies
        .iter()
        .filter(|body| {
            explicit_evidence_refs
                .iter()
                .any(|explicit| same_reference_spelling(explicit, body))
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut read_bodies = BTreeSet::new();
    let mut recovered = BTreeSet::new();
    for event in originals {
        if !matches!(
            event.kind.as_str(),
            crate::context_evidence::NATIVE_DELIVERY_KIND
                | crate::context_evidence::PROCESS_DELIVERY_KIND
        ) {
            continue;
        }
        let delivery = crate::context_evidence::decode_read_delivery(&event.payload)
            .map_err(str::to_string)?;
        let recipient = match &delivery.recipient {
            crate::context_evidence::ReadDeliveryRecipient::Root { owner, .. } => {
                format!("root-execution:{}", owner.binding.root_execution_id)
            }
            crate::context_evidence::ReadDeliveryRecipient::AgentTask { identity } => format!(
                "agent-run:{}",
                identity
                    .agent_run_id()
                    .ok_or("bounded body recipient run missing")?
            ),
        };
        if recipient != verification.reviewer_execution_id {
            continue;
        }
        let reference = match &delivery.recipient {
            crate::context_evidence::ReadDeliveryRecipient::Root { owner, .. } => {
                format!("tool://runtime-event:{}", owner.event_id)
            }
            crate::context_evidence::ReadDeliveryRecipient::AgentTask { .. } => {
                let index = store
                    .event_by_idempotency_key(
                        &delivery.acquisition.index_stream,
                        &delivery.acquisition.index_key,
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("bounded body original index missing")?;
                format!(
                    "tool://{}#{}",
                    index.stream_id,
                    index.payload["sequence"]
                        .as_u64()
                        .ok_or("bounded body original index sequence missing")?
                )
            }
        };
        let body = known_bodies
            .iter()
            .find(|body| same_reference_spelling(body, &delivery.acquisition.body.source_ref));
        if let Some(body) = body {
            if explicit_evidence_refs.contains(&reference) {
                expected_bodies.insert(body.clone());
            }
            if expected_bodies.contains(body) && claims.contains(&reference) {
                read_bodies.insert(body.clone());
            }
        }
        if !claims.contains(&reference) {
            continue;
        }
        if !explicit_evidence_refs.iter().any(|explicit| {
            explicit == &reference
                || same_reference_spelling(explicit, &delivery.acquisition.body.source_ref)
        }) {
            return Err(
                "bounded process selected body was not explicitly proposed by this review action"
                    .into(),
            );
        }
        recovered.insert(reference);
    }
    if recovered != claims || !expected_bodies.is_subset(&read_bodies) {
        return Err(
            "bounded process action selection lacks original body delivery materials".into(),
        );
    }
    Ok(())
}

pub(crate) fn validate_bounded_process_retirement(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    primary: &crate::DurableRuntimeEvent,
    artifacts: Option<&crate::ArtifactStore>,
) -> Result<Vec<crate::ExpectedStreamRevision>, String> {
    let run = results::authenticate_bounded_process(store, primary).map_err(|error| error.to_string())?;
    results::process_has_no_external_effects(&run)?;
    results::validate_process_archives(
        &run,
        ResultContentReaders {
            artifacts,
            history: None,
            process_inputs: None,
        },
    )?;
    let mut cuts = BTreeMap::new();
    results::retain_process_execution(store, &run, &mut cuts).map_err(|error| error.to_string())?;
    Ok(cuts
        .into_iter()
        .map(
            |(stream_id, expected_revision)| crate::ExpectedStreamRevision {
                stream_id,
                expected_revision,
            },
        )
        .collect())
}

fn process_body_receipt(read: &policy::VerifiedReadDelivery) -> String {
    format!("tool://{}", read.receipt_reference())
}

fn complete_process_body_refs(
    run: &results::AuthenticatedProcessExecution,
    deliveries: &policy::ReviewDeliverySnapshot,
) -> Result<Vec<String>, String> {
    let mut total = None;
    let mut chunks = BTreeMap::new();
    let mut references = BTreeSet::new();
    for read in &deliveries.reads {
        let body = &read.material.acquisition.body;
        if !read.body_delivered()
            || read.material.acquisition_cursor == 0
            || !same_reference_spelling(&body.source_ref, &run.fact.body.selector)
            || body.source_sha256 != run.fact.body.sha256
            || body.source_bytes != run.fact.body.bytes
        {
            continue;
        }
        let crate::context_evidence::ReadExtent::ArtifactChunks {
            total_chunks,
            chunks: pages,
        } = &body.extent
        else {
            continue;
        };
        if total.is_some_and(|old| old != *total_chunks) {
            return Err("bounded process body delivery extent conflict".into());
        }
        total = Some(*total_chunks);
        for chunk in pages {
            if chunk.index >= *total_chunks
                || chunks
                    .insert(chunk.index, (chunk.body_sha256.clone(), chunk.body_bytes))
                    .is_some_and(|previous| {
                        previous != (chunk.body_sha256.clone(), chunk.body_bytes)
                    })
            {
                return Err("bounded process body delivery chunk conflict".into());
            }
        }
        references.insert(process_body_receipt(read));
    }
    if !run.fact.capture_complete
        || total.is_none_or(|total| {
            (total == 0 && run.fact.body.bytes != 0)
                || chunks.len() as u64 != total
                || !chunks.keys().copied().eq(0..total)
        })
    {
        return Err(
            "review_requires_complete_process_body: independently read every original body chunk"
                .into(),
        );
    }
    Ok(references.into_iter().collect())
}

fn is_process_body_receipt(reference: &str) -> bool {
    reference.starts_with("tool://runtime-event:")
        || reference.starts_with("tool://execution-agent-receipts:")
}

fn validate_process_claims(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    runs: &[results::AuthenticatedProcessExecution],
    claimed: &[String],
    deliveries: &policy::ReviewDeliverySnapshot,
) -> Result<Vec<results::AuthenticatedProcessExecution>, results::ReviewEvidenceError> {
    let claims = claimed
        .iter()
        .filter(|reference| is_process_body_receipt(reference))
        .cloned()
        .collect::<BTreeSet<_>>();
    if claims.len()
        != claimed
            .iter()
            .filter(|reference| is_process_body_receipt(reference))
            .count()
    {
        return Err("bounded process selection repeats a body receipt".into());
    }
    let mut selected = Vec::new();
    let mut recovered = BTreeSet::new();
    for run in runs {
        let candidates = deliveries
            .reads
            .iter()
            .filter(|read| {
                same_reference_spelling(
                    &read.material.acquisition.body.source_ref,
                    &run.fact.body.selector,
                )
            })
            .map(process_body_receipt)
            .collect::<BTreeSet<_>>();
        if claims.is_disjoint(&candidates) {
            continue;
        }
        let primary = store
            .event_by_idempotency_key(
                &run.primary.stream_id,
                &format!("{}:receipt", run.fact.admission.binding.call_key),
            )
            .map_err(|error| error.to_string())?
            .ok_or("bounded process selected original terminal missing")?;
        let original = results::authenticate_bounded_process(store, &primary)?;
        results::process_has_no_external_effects(&original)?;
        if !matches!(
            original.fact.terminal,
            harness_contract::context::ToolExecutionTerminal::Settled { .. }
        ) {
            return Err("bounded process selected witness was not executed".into());
        }
        let complete = complete_process_body_refs(&original, deliveries)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if !complete.is_subset(&claims) {
            return Err("bounded process selected body omits an original chunk receipt".into());
        }
        recovered.extend(complete);
        selected.push(original);
    }
    if recovered != claims {
        return Err(
            "bounded process selected receipt is not in this result's authentic producer closure"
                .into(),
        );
    }
    Ok(selected)
}

fn select_process_witnesses(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    actor: &harness_contract::agent_action::AgentActorBinding,
    runs_by_result: &std::collections::BTreeMap<
        String,
        Vec<results::AuthenticatedProcessExecution>,
    >,
    explicit_evidence_refs: &[String],
    deliveries: &policy::ReviewDeliverySnapshot,
) -> Result<std::collections::BTreeMap<String, Vec<String>>, results::ReviewEvidenceError> {
    let mut selected: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (result, runs) in runs_by_result {
        let mut refs = BTreeSet::new();
        for run in runs {
            let requested = explicit_evidence_refs.iter().any(|reference| {
                same_reference_spelling(reference, &run.fact.body.selector)
                    || deliveries.reads.iter().any(|read| {
                        process_body_receipt(read) == *reference
                            && same_reference_spelling(
                                &read.material.acquisition.body.source_ref,
                                &run.fact.body.selector,
                            )
                    })
            });
            if !requested {
                continue;
            }
            let primary = store
                .event_by_idempotency_key(
                    &run.primary.stream_id,
                    &format!("{}:receipt", run.fact.admission.binding.call_key),
                )
                .map_err(|error| error.to_string())?
                .ok_or("selected bounded process original receipt missing")?;
            let original = results::authenticate_bounded_process(store, &primary)?;
            results::process_has_no_external_effects(&original)?;
            if !matches!(
                original.fact.terminal,
                harness_contract::context::ToolExecutionTerminal::Settled { .. }
            ) {
                return Err(
                    "review_requires_process_execution: NotStarted is not an executed witness"
                        .into(),
                );
            }
            if primary.payload["replay"]["session_id"] != actor.session_id {
                return Err("bounded process witness belongs to a foreign Session".into());
            }
            refs.extend(complete_process_body_refs(run, deliveries)?);
        }
        selected.insert(result.clone(), refs.into_iter().collect());
    }
    for reference in explicit_evidence_refs.iter().filter(|reference| {
        reference.starts_with("tool://runtime-event:")
            || reference.starts_with("tool://execution-agent-receipts:")
    }) {
        if !selected.values().any(|refs| refs.contains(reference)) {
            return Err(
                "explicit bounded body receipt is outside the original result producer closure"
                    .into(),
            );
        }
    }
    Ok(selected)
}

fn effect_observation_groups<'reads>(
    result: &results::ResolvedReviewResult,
    reads: &'reads [policy::DeliveredFileRead],
    claimed: Option<&BTreeSet<String>>,
) -> Result<Vec<&'reads policy::DeliveredFileRead>, String> {
    use harness_contract::context::WorkspaceObjectKind;
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    if result.kind != harness_contract::goal::GoalResultKind::ToolEffect {
        if claimed.is_some_and(|references| !references.is_empty()) {
            return Err("non-effect result cannot borrow effect observation references".into());
        }
        return Ok(Vec::new());
    }
    let cursor = result
        .effect_cursor
        .ok_or("effect result has no authenticated effect cursor")?;
    if result.unscoped_effect || result.effect_scopes.is_empty() {
        return Err(format!(
            "review_requires_effect_observation:{}; original effect targets are unscoped",
            result.reference
        ));
    }
    let mut selected = Vec::new();
    let mut targets = Vec::new();
    for scope in &result.effect_scopes {
        let matches = result
            .effect_targets
            .iter()
            .filter(|target| &target.declared_scope == scope)
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Err("effect observation lacks its original canonical target identity".into());
        }
        targets.extend(matches);
    }
    for target in targets {
        let scope = &target.declared_scope;
        let matches = |read: &&policy::DeliveredFileRead| {
            read.cursor > cursor
                && scope.resource == PermissionResource::File
                && read.scope.resource == scope.resource
                && read.scope.operation == PermissionOperation::Read
                && scope.target.is_some()
                && read.scope.target == scope.target
                && read.path.object_kind == WorkspaceObjectKind::File
                && target.path.object_kind == WorkspaceObjectKind::File
                && read.path.workspace_id == target.path.workspace_id
                && read.path.repository_id == target.path.repository_id
                && read.path.workspace_relative_path == target.path.workspace_relative_path
                && read.path.repository_relative_path == target.path.repository_relative_path
                && !read.receipt_refs.is_empty()
                && claimed.is_none_or(|references| read.receipt_refs.is_subset(references))
        };
        let mut matching = reads.iter().filter(matches);
        if let Some(references) = claimed {
            let groups = matching.collect::<Vec<_>>();
            if groups.is_empty() {
                return Err(format!(
                    "review_requires_effect_observation:{}; no complete strictly later reviewer witness for {:?}",
                    result.reference, scope.target
                ));
            }
            selected.extend(
                groups
                    .into_iter()
                    .filter(|read| !read.receipt_refs.is_disjoint(references)),
            );
        } else {
            selected.push(matching.next().ok_or_else(|| format!(
                "review_requires_effect_observation:{}; independently read every affected target after the original effect; target={:?} effect_cursor={cursor}", result.reference, scope.target
            ))?);
        }
    }
    Ok(selected)
}

fn effect_observation_refs(
    result: &results::ResolvedReviewResult,
    reads: &[policy::DeliveredFileRead],
    claimed: Option<&[String]>,
) -> Result<Vec<String>, String> {
    let claimed_set = claimed.map(|references| references.iter().cloned().collect::<BTreeSet<_>>());
    let groups = effect_observation_groups(result, reads, claimed_set.as_ref())?;
    let actual = groups
        .into_iter()
        .flat_map(|read| read.receipt_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
    if claimed_set
        .as_ref()
        .is_some_and(|references| references != &actual)
        || claimed.is_some_and(|references| references.len() != actual.len())
    {
        return Err(format!(
            "review_requires_effect_observation:{}; references do not form exact complete target witnesses",
            result.reference
        ));
    }
    Ok(actual.into_iter().collect())
}

fn directory_read_packet<'a>(
    packets: &'a [AgentTaskPacket],
    source: &str,
) -> Result<&'a AgentTaskPacket, String> {
    let mut matches = packets.iter().filter(|packet| source == format!(
        "execution-agent-receipts:{}:{}:{}", packet.graph_id(), packet.node_id(), packet.attempt));
    let packet = matches.next().ok_or("directory read original packet unavailable or ambiguous")?;
    let digest = proof_value_digest(packet);
    if matches.any(|other| proof_value_digest(other) != digest) {
        return Err("directory read original packet unavailable or ambiguous".into());
    }
    Ok(packet)
}

/// The review path reads through the durable-verified store, not the hot cache.
/// Event notification precedes the writer's cache publish, so a hot read can miss
/// a graph that exists; a failure produced by that window is timing, not a dead
/// end, and must not be counted as one. This is the read the store's own comment
/// prescribes for a consumer acknowledging a notification.
async fn physical_packet(
    services: &RuntimeServices,
    graph_id: &str,
    original: &harness_contract::agent_action::OriginalAgentExecution,
) -> Result<AgentTaskPacket, String> {
    let graph = services
        .graph_state_store()
        .load_current_async(graph_id.to_string())
        .await
        .map_err(|error| error.to_string())?;
    let mut packets = graph.nodes.iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .filter_map(|node| serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok()
            .filter(|packet| packet_identity_matches(packet.graph_id(), packet.run_id(), graph_id)
                && packet.node_id() == node.id));
    let mut packet = packets.next().ok_or_else(|| format!("review physical Agent packet missing:{graph_id}"))?;
    if packets.next().is_some() { return Err("review physical Agent packet ambiguous".into()); }
    if original.graph_id != graph.id || original.node_id != packet.node_id()
        || original.attempt == 0
        || graph.recovery_cursor.node_attempts.get(packet.node_id()) != Some(&original.attempt)
    {
        return Err("review original physical attempt is stale or foreign".into());
    }
    let binding = services.graph_state_store()
        .current_binding(&graph, packet.node_id(), original.attempt)
        .map_err(|error| error.to_string())?;
    let (ticket, _) = services.graph_state_store()
        .binding_start_ticket(&graph, packet.node_id(), &binding)
        .map_err(|error| error.to_string())?;
    crate::execution_core::graph::validate_node_ticket_start(services.event_store(), &ticket, None)
        .map_err(|error| error.to_string())?;
    packet.attempt = original.attempt;
    Ok(packet)
}

/// The identity check in the caller's namespace.
///
/// The caller names an execution id, and there are two binding paths:
/// `agent/in_process/model_loop.rs:196/367` bind the graph id, `:470` binds the physical run
/// id. Comparing a run id against `packet.graph_id()` can never hold, so a packet that was
/// present was reported missing for a run-bound actor - that was the origin of the 158-refusal
/// run. This normalises the namespace; it does not relax the identity check, and a packet from
/// a different graph still never matches.
fn packet_identity_matches(
    packet_graph_id: &str,
    packet_run_id: &str,
    caller_execution_id: &str,
) -> bool {
    if caller_execution_id.starts_with("run:") {
        packet_run_id == caller_execution_id
    } else {
        packet_graph_id == caller_execution_id
    }
}

/// The shared aggregate streams a review reads while other actors keep
/// appending to them: the reviewed Session stream, the physical lineage
/// stream of the reviewed graph, and the execution graph streams themselves.
///
/// All are append-only and every event in them is located by sequence, so an
/// append that lands while this review reads does not change the material it
/// already pinned. The execution graph streams grow on their own while the
/// review reads them - a sibling node migrating already advances the root
/// graph and its child graphs - and that growth moves nothing inside the
/// pinned window, because the window locates its material by sequence. The
/// prefixes are the ones the product already classifies by (`results.rs`
/// Session cuts, `execution_core/graph/commit_pipeline.rs` lineage stream ids,
/// and the product's own execution graph id forms `agentic-graph:` and
/// `session-ingress-graph:`); no new classification dimension is introduced.
fn source_grows_during_review(source: &str) -> bool {
    // A5: normalize before the prefix test. A delegated read reaches this
    // predicate in its run-id form (`run:agentic-graph:...`), because the
    // reviewer pins the un-normalized ticket graph id, while the store looks
    // that id up under the graph id alone. A rule that depends on every
    // caller remembering the normalization is not a rule.
    let source = crate::execution_core::graph::graph_id_for_lookup(source);
    source.starts_with("session:")
        || source.starts_with("execution-lineage:")
        || source.starts_with("agentic-graph:")
        || source.starts_with("session-ingress-graph:")
}

/// Whether re-pinning a source inside one review resolution may accept a head
/// that moved.
///
/// Only growth on a shared aggregate stream is admissible. A head that moved
/// backwards means the stream was truncated or replaced, so the material the
/// review already read may no longer be there; growth on any other source
/// means the object under review itself moved. Both still refuse - this
/// decides nothing about content, identity or authority.
pub(crate) fn repin_accepts_moved_head(source: &str, previous: u64, revision: u64) -> bool {
    revision > previous && source_grows_during_review(source)
}

/// Why a TaskReview-scoped physical attempt cannot validate this review.
///
/// Returns the specific reason, or `Ok(())` when every condition holds. The
/// three conditions used to collapse into one sentence - "review physical
/// attempt is stale or owns different results" - which named neither the
/// generation, the attempt, nor the refs, so a reviewer that had done everything
/// right could not act on it and cycled until Runtime settled the attempt.
///
/// The third condition is Task-review-only. An Objective review is
/// criterion-scoped and legitimately cites the delivered results of several
/// Tasks at once: with two or more accepted Tasks the criterion's result set
/// necessarily spans them, so applying the single-Task rule to an objective
/// review made every objective review of real multi-Task work impossible.
fn task_review_attempt_rejection(
    task: &crate::agentic::program::AgenticTaskProjection,
    graph_id: &str,
    attempt: u32,
    verified_node_attempt: u32,
    agent_id: &str,
    membership_id: &str,
    result_refs: &[String],
    condition_ref: Option<&str>,
) -> Result<(), String> {
    // Business generation belongs to the registered Program attempt. The
    // physical node attempt is a separate graph-ticket namespace.
    if attempt == 0 || attempt != verified_node_attempt {
        return Err("review physical node attempt is stale".into());
    }
    let active = task.active_attempts.get(graph_id).is_some_and(|candidate| {
        candidate.execution_id == graph_id
            && candidate.mode == harness_contract::agent_action::AgentAttemptMode::Review
            && candidate.generation == task.review_generation
            && candidate.agent_id == agent_id
            && candidate.membership_id == membership_id
    });
    if !active {
        return Err(format!(
            "review attempt is not the active Review attempt for Task {}: Runtime has no Review \
             attempt bound to this graph, agent and membership; re-read the \
             Task and let Runtime dispatch the current attempt",
            task.task_id
        ));
    }
    if condition_ref.is_none()
        && !result_refs
            .iter()
            .all(|reference| task.artifact_refs.contains(reference))
    {
        // Name the legal refs, not just the rule. Runtime reads `task.artifact_refs`
        // in this very condition, so it already knows the answer; the old message
        // named the Task and the rule and made the model guess the refs. Run one28
        // guessed eighteen times, each one refused with `state_derived: true` - a
        // cause the model could not have put in its own input, which is precisely
        // the case where the framework has to supply it.
        let legal = if task.artifact_refs.is_empty() {
            "this Task has no delivered artifact yet; submit or inspect it first".to_string()
        } else {
            task.artifact_refs.join(", ")
        };
        let shape = if task.artifact_refs.is_empty() {
            String::new()
        } else {
            format!(
                " A conforming call is {{\"task_ref\": \"{}\", \"decision\": \"accept\", \
                 \"reason\": \"...\", \"evidence_refs\": [{}]}}.",
                task.task_id,
                task.artifact_refs
                    .iter()
                    .map(|reference| format!("\"{reference}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        return Err(format!(
            "task review cites results outside Task {}: a Task review covers only that Task's \
             delivered artifacts. This Task's legal result refs are: {legal}.{shape} Cite those, \
             or settle the criterion with objective_review.",
            task.task_id
        ));
    }
    Ok(())
}

/// Whether a tool read belongs to the root actor itself.
///
/// The root's own reads are owned by the root execution and Runtime carries them
/// under whichever agentic-graph run happens to be executing the root's step at
/// that moment - one live run's 125 root tool calls were attributed to eight
/// different graphs, interleaved in time. Ownership, not the presence of a run
/// id, is therefore what identifies the root's own read: a delegated Agent's read
/// is not owned by the root execution and must stay invisible to it.
fn binding_is_owned_by_root(
    binding: &harness_contract::projection::RuntimeActivityBinding,
    root_execution_id: Option<&str>,
) -> bool {
    root_execution_id.is_some_and(|root| {
        binding
            .activity_id
            .starts_with(&format!("activity:execution:{root}:owner:"))
    })
}

impl RuntimeServices {
    async fn actual_review_material_output(
        &self,
        session: &str,
        event: &crate::DurableRuntimeEvent,
    ) -> Result<Option<String>, String> {
        if let Some(output) = event.payload["outcome"]["output"]
            .as_str()
            .or_else(|| event.payload["output"].as_str())
        {
            return Ok(Some(output.to_string()));
        }
        let Some(reference) = event.payload["full_output_ref"]
            .as_str()
            .and_then(|reference| reference.strip_prefix("tool://"))
        else {
            return Ok(None);
        };
        let access = self
            .session_evidence_access(session, reference)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("actual read material access missing")?;
        let artifact = self
            .artifact_store()
            .resolve(&access.retrieval_selector)
            .map_err(|error| error.to_string())?;
        if artifact.sha256 != access.sha256 || artifact.bytes != access.bytes {
            return Err("actual read material identity changed".into());
        }
        let bytes = self
            .artifact_store()
            .read(&artifact, &access.visibility_scope, None)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Some(
            String::from_utf8(bytes).map_err(|error| error.to_string())?,
        ))
    }

    pub(crate) async fn independent_result_review(
        &self,
        actor: &AgentActorBinding,
        program: &AgenticProgramProjection,
        result_refs: &[String],
        condition_ref: Option<&str>,
    ) -> Result<IndependentReview, String> {
        self.independent_result_review_with_evidence(
            actor,
            program,
            result_refs,
            condition_ref,
            &[],
            None,
        )
        .await
        .map_err(|error| error.to_string())
    }

    pub(crate) async fn independent_result_review_with_evidence(
        &self,
        actor: &AgentActorBinding,
        program: &AgenticProgramProjection,
        result_refs: &[String],
        condition_ref: Option<&str>,
        explicit_evidence_refs: &[String],
        current_review: Option<&CurrentReviewContext<'_>>,
    ) -> Result<IndependentReview, results::ReviewEvidenceError> {
        let process_verifier = ProcessInputVerifier::new(std::sync::Arc::clone(self.scope_locks()));
        let process_frame = process_verifier.frame();
        // A review names the delivered results it covers, and a model routinely
        // names the Task that produced them instead: `task:<id>` is work, not a
        // result, so the coverage map came back empty and the review was refused
        // with "independent review has no current results" while the work was in
        // fact delivered and accepted. The mapping is unambiguous - a Task's
        // `artifact_refs` are exactly its delivered results - so resolve it here
        // rather than making the model guess the spelling.
        let result_refs = resolve_result_refs(program, result_refs);
        let result_refs = result_refs.as_slice();
        let root = actor
            .root_execution_id
            .as_deref()
            .ok_or("review has no root execution")?;
        let goal = self.goal_store().get(&format!("goal:{root}"))?;
        let packet = if matches!(actor.kind, AgentActorKind::Agent | AgentActorKind::TeamLead) {
            let packet = physical_packet(
                self,
                actor
                    .execution_id
                    .as_deref()
                    .ok_or("review has no physical run")?,
                actor.original_execution.as_ref().ok_or("review original physical binding unobserved")?,
            )
            .await?;
            let scope = packet
                .agentic_binding
                .as_ref()
                .ok_or("review packet has no Program binding")?;
            if scope.program_id != actor.program_id
                || Some(scope.agent_id.as_str()) != actor.agent_id.as_deref()
                || Some(scope.team_id.as_str()) != actor.team_id.as_deref()
                || packet.session_id() != actor.session_id
            {
                return Err("review physical run does not match actor scope".into());
            }
            Some(packet)
        } else {
            None
        };
        if let Some(packet) = &packet {
            let scope = packet.agentic_binding.as_ref().unwrap();
            let binding = packet
                .binding
                .as_ref()
                .ok_or("review execution has no compiled binding")?;
            if let harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } =
                &scope.focus
            {
                let task = program
                    .tasks
                    .get(task_ref)
                    .ok_or("review Task disappeared")?;
                task_review_attempt_rejection(
                    task,
                    packet.graph_id(),
                    packet.attempt,
                    packet.attempt, // authenticated by physical_packet above
                    &scope.agent_id,
                    &scope.membership_id,
                    result_refs,
                    condition_ref,
                )?;
            }

            if actor.agent_id.as_deref() != Some(actor.actor_id.as_str())
                || !program.agent_is_active_in(&scope.agent_id, &scope.team_id)
                || !program
                    .membership_for(&scope.agent_id, &scope.team_id)
                    .is_some_and(|member| member.membership_id == scope.membership_id)
                || !crate::agent::binding::recompute_binding_digest(binding)
                    .is_ok_and(|digest| digest == binding.binding_digest)
            {
                return Err("review execution binding or membership is stale".into());
            }
        }
        let reviewer_execution_id = packet.as_ref().map_or_else(
            || format!("root-execution:{root}"),
            |packet| format!("agent-run:{}", packet.assignment.run_id),
        );
        let mut producers = BTreeSet::new();
        let mut producer_execution_ids = BTreeSet::new();
        let mut coverage = BTreeMap::new();
        let mut producer_packets = Vec::new();
        let mut result_kinds = BTreeSet::new();
        let mut resolved = Vec::new();
        for reference in canonical_review_result_refs(program, result_refs)? {
            resolved.push(results::resolve_result(self, actor, program, &reference).await?);
        }
        let policy = if actor.kind == AgentActorKind::Root && condition_ref.is_some() {
            match goal.as_ref() {
                Some(goal) => root_self_review_policy(self.event_store(), program, goal)?,
                None => None,
            }
        } else {
            None
        };
        let independence_required = policy.is_none();
        let effect_source_refs = resolved
            .iter()
            .flat_map(|result| &result.producers)
            .filter_map(|producer| producer.packet.as_ref())
            .flat_map(|packet| {
                (1..=packet.attempt.max(1)).map(move |attempt| {
                    format!(
                        "execution-agent-receipts:{}:{}:{}",
                        packet.graph_id(),
                        packet.node_id(),
                        attempt
                    )
                })
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let effects = if condition_ref.is_some()
            || !effect_source_refs.is_empty()
            || resolved
                .iter()
                .any(|result| result.kind == harness_contract::goal::GoalResultKind::ToolEffect)
        {
            Some(effect_review_snapshot(
                self.event_store(),
                program,
                goal.as_ref(),
                &effect_source_refs,
            )?)
        } else {
            None
        };

        verify_producer_metadata_cuts(
            self.event_store(),
            resolved
                .iter()
                .flat_map(|result| result.effect_sources.iter()),
        )?;
        for result in &resolved {
            for producer in &result.producers {
                if independence_required
                    && (producer.execution == reviewer_execution_id
                        || producer.actor == actor.actor_id)
                {
                    return Err(
                        "review producer and reviewer must be different physical executions".into(),
                    );
                }
                producers.insert(producer.actor.clone());
                producer_execution_ids.insert(producer.execution.clone());
                if let Some(packet) = &producer.packet {
                    producer_packets.push(packet.clone());
                }
            }
            result_kinds.extend(result.artifact_kinds.iter().cloned());
            coverage.insert(
                result.reference.clone(),
                ReadCoverage {
                    proof: ObjectiveResultReadProof {
                        result_kind: result.kind,
                        effect_observation_refs: vec![],
                        result_ref: result.reference.clone(),
                        content_ref: result.content.selector.clone(),
                        sha256: result.content.sha256.clone(),
                        bytes: result.content.bytes,
                        receipt_refs: vec![],
                    },
                    total: None,
                    chunks: BTreeSet::new(),
                },
            );
        }
        if let Some(obligation) = goal.as_ref().and_then(|goal| {
            goal.obligations
                .iter()
                .find(|obligation| Some(obligation.obligation_id.as_str()) == condition_ref)
        }) {
            if obligation
                .evidence_requirement
                .required_artifact_kinds
                .iter()
                .any(|kind| !result_kinds.contains(kind.as_str()))
            {
                return Err(
                    "obligation result kinds do not cover the original evidence requirement".into(),
                );
            }
            let required = &obligation.producer;
            let requires_bound_producer = required.capability_id.is_some()
                || required.agent_definition_ref.is_some()
                || required.skill_ref.is_some()
                || required.tool_ref.is_some();
            let mut matched = !requires_bound_producer;
            if required.agent_definition_ref.is_none() && required.skill_ref.is_none() {
                matched |= resolved
                    .iter()
                    .flat_map(|result| &result.producers)
                    .any(|producer| {
                        producer.packet.is_none()
                            && producer.tool.as_ref().is_some_and(|(name, effect)| {
                                required
                                    .tool_ref
                                    .as_ref()
                                    .is_none_or(|required| required == name)
                                    && required.capability_id.as_ref().is_none_or(|required| {
                                        match effect {
                                            harness_contract::tool::ToolEffectKind::Read => {
                                                required == "read"
                                            }
                                            harness_contract::tool::ToolEffectKind::Write => {
                                                required == "write"
                                            }
                                            _ => false,
                                        }
                                    })
                            })
                    });
            }

            for producer in &producer_packets {
                let Some(binding) = &producer.binding else {
                    continue;
                };
                if !crate::agent::binding::recompute_binding_digest(binding)
                    .is_ok_and(|digest| digest == binding.binding_digest)
                {
                    continue;
                }
                if required.capability_id.as_ref().is_some_and(|required| {
                    !binding
                        .effective_capabilities
                        .iter()
                        .any(|capability| capability.as_str() == required)
                }) || required
                    .agent_definition_ref
                    .as_ref()
                    .is_some_and(|required| {
                        required != binding.definition_ref.definition_id.as_str()
                            && required
                                != &format!(
                                    "{}@{}",
                                    binding.definition_ref.definition_id.as_str(),
                                    binding.definition_ref.revision
                                )
                    })
                    || required
                        .skill_ref
                        .as_ref()
                        .is_some_and(|required| !binding.skill_refs.contains(required))
                {
                    continue;
                }
                if let Some(tool) = &required.tool_ref {
                    if !binding.tool_contract_refs.contains(tool) {
                        continue;
                    }
                    let receipts = self
                        .commit_service()
                        .load_delegated_agent_tool_receipts(
                            producer.graph_id(),
                            producer.node_id(),
                            producer.attempt,
                        )
                        .map_err(|error| error.to_string())?;
                    if !receipts.iter().any(|receipt| {
                        receipt.outcome.tool_name == *tool
                            && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed
                    }) {
                        continue;
                    }
                }
                matched = true;
                break;
            }
            if !matched {
                // Name the requirement that was not met. The old message listed
                // all four requirements, so a model whose Task had the right
                // capability but the wrong definition was told both were wrong
                // and had no way to tell which.
                return Err(unmet_obligation_error(&obligation.obligation_id, required).into());
            }
        }
        if coverage.is_empty() {
            // Name what IS current. The old message said the review had no
            // current results and stopped; a model that had just named three
            // real receipts was told its evidence did not exist, with no list of
            // what Runtime considered current and no spelling to try.
            let current =
                crate::agentic::supervision::completion_ready_result_artifact_refs(program);
            return Err(no_current_results_error(&current).into());
        }
        let reviewer_packet = packet.clone();
        let delivery_snapshot = policy::review_delivery_snapshot(
            self.event_store(),
            program,
            reviewer_packet.as_ref(),
            None,
        )?;
        for read in &delivery_snapshot.reads {
            for state in coverage.values_mut() {
                state.observe_delivered(read)?;
            }
        }
        let complete_file_reads = delivery_snapshot.complete_file_reads()?;
        let runs_by_result = resolved
            .iter()
            .map(|result| (result.reference.clone(), result.process_runs.clone()))
            .collect::<BTreeMap<_, _>>();
        let process_selection = select_process_witnesses(
            self.event_store(),
            actor,
            &runs_by_result,
            explicit_evidence_refs,
            &delivery_snapshot,
        )?;
        for result in &resolved {
            let references = effect_observation_refs(result, &complete_file_reads, None)?;
            coverage
                .get_mut(&result.reference)
                .ok_or("review result coverage disappeared")?
                .proof
                .effect_observation_refs = references
                .into_iter()
                .chain(
                    process_selection
                        .get(&result.reference)
                        .into_iter()
                        .flatten()
                        .cloned(),
                )
                .collect();
        }
        let missing = coverage
            .values()
            .filter(|state| !state.complete())
            .map(|state| state.proof.result_ref.clone())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            // Review contract v2 (ruling 2026-11-24): reads the reviewer has not
            // repeated in this execution are reported for the reviewer to judge,
            // not refused - the submitted artifact bodies are already in front of
            // it, and live runs cycled on this refusal five times across three
            // agents while the work itself was complete.
            tracing::warn!(
                missing = %missing.join(","),
                "review_reads_not_repeated_in_this_execution_reported"
            );
        }
        let mut verification = ObjectiveReviewVerification {
            input_set: None,
            result_source_revisions: resolved
                .iter()
                .flat_map(|result| &result.result_sources)
                .map(|source| (source.stream_id.clone(), source.expected_revision))
                .collect(),
            independence_required,
            review_policy_digest: policy.map(|policy| policy.digest),
            effect_manifest_digest: effects.map(|effects| effects.digest),
            effect_source_refs,
            goal_spec_revision: goal.as_ref().map(|goal| goal.spec_revision),
            goal_spec_digest: goal.as_ref().map(|goal| goal.spec_digest.clone()),
            work_manifest_digest: String::new(),
            reviewer_execution_id,
            producer_execution_ids: producer_execution_ids.into_iter().collect(),
            current_review_effect_key: None,
            reads: coverage
                .into_values()
                .map(|mut state| {
                    state.proof.receipt_refs.sort();
                    state.proof.receipt_refs.dedup();
                    state.proof
                })
                .collect(),
        };
        let mut root_materials = resolved
            .iter()
            .flat_map(|result| &result.producers)
            .filter(|producer| producer.packet.is_none())
            .filter_map(|producer| {
                producer
                    .execution
                    .strip_prefix("root-execution:")
                    .map(str::to_string)
            })
            .collect::<BTreeSet<_>>();
        let mut primary_roots = root_materials.clone();
        if reviewer_packet.is_none() {
            root_materials.insert(root.to_string());
        }
        let mut packets = producer_packets;
        let mut primary_packets = packets
            .iter()
            .map(|packet| packet.graph_id().to_string())
            .collect::<BTreeSet<_>>();
        packets.extend(reviewer_packet);
        let initial_material_packets = packets
            .iter()
            .map(|packet| packet.graph_id().to_string())
            .collect::<BTreeSet<_>>();
        let initial_root_materials = root_materials.clone();
        let mut auxiliary_refs = packets
            .iter()
            .flat_map(|packet| packet.evidence_refs.iter())
            .map(|evidence| evidence.evidence_ref.id.clone())
            .filter(|reference| !reference.is_empty())
            .collect::<BTreeSet<_>>();
        let mut packet_digests = BTreeMap::new();
        let mut material_contents = resolved
            .iter()
            .map(|material| (material.reference.clone(), material.content.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut resolved_materials = material_contents.keys().cloned().collect::<BTreeSet<_>>();
        let mut process_bodies = runs_by_result
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let mut dependency_results = resolved;
        let mut actual_boundaries = Vec::new();
        let mut business_effect_targets = Vec::new();
        let mut primary_seed_refs = canonical_review_result_refs(program, result_refs)?
            .into_iter()
            .collect::<Vec<_>>();
        primary_seed_refs.extend(packets.iter().filter_map(|packet| {
            if !primary_packets.contains(packet.graph_id()) {
                return None;
            }
            let binding = packet.agentic_binding.as_ref()?;
            match &binding.focus {
                harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref }
                    if binding.program_id == program.program_id =>
                {
                    Some(task_ref.clone())
                }
                _ => None,
            }
        }));
        let mut publication_sources = BTreeMap::new();
        let (initial_primary_tasks, initial_primary_artifacts, _) = selected_work(
            self.event_store(),
            program,
            goal.as_ref(),
            &primary_seed_refs,
            &mut publication_sources,
        )?;
        let mut primary_refs = result_refs
            .iter()
            .map(|reference| review_result_identity(program, reference))
            .collect::<Result<BTreeSet<_>, _>>()?;
        primary_refs.extend(initial_primary_artifacts);
        let mut primary_tasks = initial_primary_tasks.into_iter().collect::<BTreeSet<_>>();
        let mut expanded_primary_tasks = BTreeSet::new();
        let mut expanded_primary_materials = BTreeSet::new();
        let mut expanded_packet_roles = BTreeMap::new();
        let mut pending_references = BTreeSet::new();
        let mut pending_metadata = BTreeSet::new();
        let mut inspected_metadata = BTreeMap::new();
        let material_discovery_cursor = self
            .event_store()
            .durable_commit_cursor()
            .map_err(|error| error.to_string())?;
        let selected_receipts = verification
            .reads
            .iter()
            .flat_map(|read| {
                read.receipt_refs
                    .iter()
                    .chain(&read.effect_observation_refs)
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let selected_receipts = selected_receipts
            .into_iter()
            .map(|reference| {
                reference
                    .strip_prefix("tool://")
                    .filter(|_| {
                        reference.starts_with("tool://runtime-event:")
                            || reference.starts_with("tool://execution-agent-receipts:")
                    })
                    .map(str::to_string)
                    .unwrap_or(reference)
            })
            .collect::<BTreeSet<_>>();
        let mut matched_receipts = BTreeSet::new();
        let mut matched_receipt_events: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut material_events = delivery_snapshot
            .reads
            .iter()
            .map(|read| harness_contract::goal::ProofInputEvent {
                stream_id: read.event.stream_id.clone(),
                sequence: read.event.sequence,
                event_id: read.event.event_id.clone(),
                payload_digest: proof_value_digest(&read.event.payload),
            })
            .collect::<Vec<_>>();
        let mut required_scopes = Vec::new();
        let required_scope_complete = true;
        let mut material_revisions = delivery_snapshot.sources;
        for (source, revision) in publication_sources {
            if material_revisions
                .insert(source, revision)
                .is_some_and(|previous| previous != revision)
            {
                return Err("publication source changed during original review discovery".into());
            }
        }
        let mut pending_sources = BTreeSet::from([format!("session:{}", actor.session_id)]);
        let mut selected_roots = BTreeMap::new();
        let mut selected_events = BTreeMap::new();
        let mut start_cursor = self
            .event_store()
            .list_stream_after(root, 0, 1, 1, 4 * 1024 * 1024)?
            .first()
            .map_or(0, |event| event.commit_cursor);
        loop {
            let previous_start_cursor = start_cursor;
            for mut material in std::mem::take(&mut dependency_results) {
                for run in &material.process_runs {
                    if !process_bodies
                        .iter()
                        .any(|original| original.primary == run.primary)
                    {
                        process_bodies.push(run.clone());
                    }
                }
                let identity = identity::auxiliary_reference(program, &material.reference)?;
                let primary = primary_refs.contains(&identity);
                if primary {
                    actual_boundaries.extend(material.input_boundaries.iter().cloned());
                    if let Some(cursor) = material.effect_cursor {
                        business_effect_targets.extend(
                            material
                                .effect_targets
                                .iter()
                                .cloned()
                                .map(|target| (target, cursor)),
                        );
                    }
                    expanded_primary_materials.insert(identity);
                }
                if primary {
                    let history = self.session_history_reader();
                    let acquired = policy::primary_producer_immutable_inputs(
                        self.event_store(),
                        program,
                        &material.producers,
                        ResultContentReaders {
                            artifacts: Some(self.artifact_store().as_ref()),
                            history: history.as_deref(),
                            process_inputs: Some(&process_frame),
                        },
                        None,
                    )?;
                    for (source, revision) in acquired.sources {
                        if material_revisions
                            .insert(source, revision)
                            .is_some_and(|previous| previous != revision)
                        {
                            return Err(
                                "primary acquisition source changed during discovery".into()
                            );
                        }
                    }
                    for event in acquired.material_events {
                        if !material_events.contains(&event) {
                            material_events.push(event);
                        }
                    }
                    for content in acquired.immutable_inputs {
                        if !material.primary_inputs.contains(&content) {
                            material.primary_inputs.push(content);
                        }
                    }
                    for input in &material.primary_inputs {
                        let Some(identity) = input.dependency_identity(program)?
                        else {
                            continue;
                        };
                        let metadata = results::resolve_result_metadata(
                            self.event_store(),
                            program,
                            &identity,
                            &input.content,
                            None,
                            ResultContentReaders {
                                artifacts: Some(self.artifact_store().as_ref()),
                                history: self.session_history_reader().as_deref(),
                                process_inputs: Some(&process_frame),
                            },
                        )?;
                        input.validate_dependency(program, &identity, &metadata)?;
                        for source in metadata.effect_sources.iter().chain(&metadata.result_sources) {
                            if material_revisions
                                .insert(source.stream_id.clone(), source.expected_revision)
                                .is_some_and(|previous| previous != source.expected_revision)
                            {
                                return Err("primary immutable input source changed during provenance discovery".into());
                            }
                        }
                        primary_refs.insert(identity.clone());
                        auxiliary_refs.insert(identity.clone());
                        pending_references.insert(identity);
                    }
                }
                if material.unscoped_effect {
                    return Err("recursive material has an unmodelled effect dependency".into());
                }
                pending_metadata.insert(material.reference.clone());
                for source in material
                    .result_sources
                    .iter()
                    .chain(&material.effect_sources)
                {
                    if material_revisions
                        .insert(source.stream_id.clone(), source.expected_revision)
                        .is_some_and(|revision| revision != source.expected_revision)
                    {
                        return Err("recursive proof material sources have different enumeration cuts; rebuild the whole review".into());
                    }
                }
                for source in &material.result_sources {
                    if verification
                        .result_source_revisions
                        .insert(source.stream_id.clone(), source.expected_revision)
                        .is_some_and(|revision| revision != source.expected_revision)
                    {
                        return Err(
                            "recursive approval material source changed; rebuild the whole review"
                                .into(),
                        );
                    }
                }
                for source in material.effect_sources {
                    if source.stream_id.starts_with("execution-agent-receipts:") {
                        verification.effect_source_refs.push(source.stream_id);
                    }
                }
                for producer in material.producers {
                    if primary
                        && independence_required
                        && (producer.execution == verification.reviewer_execution_id
                            || producer.actor == actor.actor_id)
                    {
                        return Err("derived material producer and reviewer must be different physical executions".into());
                    }
                    if primary {
                        producers.insert(producer.actor);
                        verification
                            .producer_execution_ids
                            .push(producer.execution.clone());
                    }
                    if let Some(packet) = producer.packet {
                        if primary {
                            primary_packets.insert(packet.graph_id().to_string());
                        }
                        let scope = packet
                            .agentic_binding
                            .as_ref()
                            .ok_or("recursive material producer has no Program Task binding")?;
                        match &scope.focus {
                            harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                                task_ref,
                            }
                            | harness_contract::agent::AgenticExecutionFocus::TaskReview {
                                task_ref,
                            } if scope.program_id == program.program_id => {
                                auxiliary_refs.insert(task_ref.clone());
                                if primary {
                                    primary_tasks.insert(task_ref.clone());
                                }
                            }
                            _ => {}
                        }
                        if let Some(previous) = packets
                            .iter()
                            .find(|previous| previous.graph_id() == packet.graph_id())
                        {
                            if proof_value_digest(previous) != proof_value_digest(&packet) {
                                return Err(
                                    "recursive material producer packet identity is ambiguous"
                                        .into(),
                                );
                            }
                        } else {
                            packets.push(packet);
                        }
                    } else if let Some(root) = producer.execution.strip_prefix("root-execution:") {
                        root_materials.insert(root.to_string());
                        if primary {
                            primary_roots.insert(root.to_string());
                        }
                    } else if !producer.execution.starts_with("approval-decision:") {
                        return Err(
                            "recursive material producer has no authenticated physical origin"
                                .into(),
                        );
                    }
                }
            }
            let mut pending_primary_tasks = primary_tasks
                .difference(&expanded_primary_tasks)
                .cloned()
                .collect::<Vec<_>>();
            while let Some(task_ref) = pending_primary_tasks.pop() {
                if !expanded_primary_tasks.insert(task_ref.clone()) {
                    continue;
                }
                let task = program
                    .tasks
                    .get(&task_ref)
                    .ok_or("primary influence Task missing")?;
                auxiliary_refs.insert(task_ref);
                for dependency in task.depends_on.iter().chain(&task.replacement_task_refs) {
                    primary_tasks.insert(dependency.clone());
                    pending_primary_tasks.push(dependency.clone());
                }
                for reference in &task.artifact_refs {
                    let identity = review_result_identity(program, reference)?;
                    primary_refs.insert(identity.clone());
                    pending_references.insert(identity);
                }
            }
            for packet in &packets {
                let digest = proof_value_digest(packet);
                let primary = primary_packets.contains(packet.graph_id());
                if let Some(previous) = packet_digests.get(packet.graph_id()) {
                    if previous != &digest {
                        return Err("recursive material packet changed".into());
                    }
                    if expanded_packet_roles.get(packet.graph_id()) == Some(&primary)
                        || expanded_packet_roles.get(packet.graph_id()) == Some(&true)
                    {
                        continue;
                    }
                }
                let graph = self
                    .graph_state_store()
                    .load(packet.graph_id())
                    .map_err(|error| error.to_string())?;
                // The material producer packet is attempt-specific. The node
                // payload is its immutable template; validate the exact original
                // NodesStarted ticket before binding the physical attempt to it.
                let original_binding = self.graph_state_store()
                    .current_binding(&graph, packet.node_id(), packet.attempt)
                    .map_err(|error| error.to_string())?;
                let (original_ticket, original_node) = self.graph_state_store()
                    .binding_start_ticket(&graph, packet.node_id(), &original_binding)
                    .map_err(|error| error.to_string())?;
                crate::execution_core::graph::validate_node_ticket_start(
                    self.event_store(), &original_ticket, None,
                ).map_err(|error| error.to_string())?;
                if !graph.nodes.iter().any(|node| node == &original_node) {
                    return Err("recursive material original node changed".into());
                }
                if packet.session_id() != actor.session_id
                    || packet.binding.as_ref().is_none_or(|binding| {
                        !crate::agent::binding::recompute_binding_digest(binding)
                            .is_ok_and(|digest| digest == binding.binding_digest)
                    })
                    || graph
                        .lineage
                        .as_ref()
                        .is_none_or(|lineage| lineage.session_id != actor.session_id)
                    || !graph.nodes.iter().any(|node| {
                        node.id == packet.node_id()
                            && serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
                                .is_ok_and(|stored| proof_value_digest(&stored) == digest)
                    })
                    || material_revisions
                        .insert(graph.id.clone(), graph.revision)
                        .is_some_and(|revision| revision != graph.revision)
                {
                    return Err(
                        "recursive material packet has no matching immutable graph origin".into(),
                    );
                }
                let genesis = self
                    .event_store()
                    .list_stream_after(packet.graph_id(), 0, 1, 1, 4 * 1024 * 1024)?
                    .into_iter()
                    .next()
                    .ok_or("recursive material graph genesis missing")?;
                if genesis.commit_cursor > material_discovery_cursor {
                    return Err("recursive material graph crosses the review discovery cut; rebuild the whole review".into());
                }
                start_cursor = start_cursor.min(genesis.commit_cursor);
                for attempt in 1..=packet.attempt.max(1).max(
                    graph
                        .recovery_cursor
                        .node_attempts
                        .get(packet.node_id())
                        .copied()
                        .unwrap_or_default(),
                ) {
                    pending_sources.insert(format!(
                        "execution-agent-receipts:{}:{}:{attempt}",
                        packet.graph_id(),
                        packet.node_id()
                    ));
                }
                for evidence in &packet.evidence_refs {
                    if evidence.evidence_ref.ref_type == "agent"
                        && !program
                            .artifacts
                            .contains_key(&identity::auxiliary_reference(
                                program,
                                &evidence.evidence_ref.id,
                            )?)
                    {
                        return Err(
                            "recursive Agent material has no authenticated publication producer"
                                .into(),
                        );
                    }
                    let content = self
                        .artifact_store()
                        .resolve(&evidence.retrieval_selector)
                        .map_err(|error| error.to_string())?;
                    if content.sha256 != evidence.sha256
                        || content.bytes != evidence.bytes
                        || content.media_type != evidence.media_type
                        || content.visibility_scope != evidence.visibility_scope
                        || (content.visibility_scope != "public"
                            && content.visibility_scope != format!("session:{}", actor.session_id)
                            && !packet.resource_scopes.contains(&content.visibility_scope))
                    {
                        return Err(
                            "recursive packet material does not match its immutable read grant"
                                .into(),
                        );
                    }
                    self.artifact_store()
                        .read(&content, &evidence.visibility_scope, None)
                        .await
                        .map_err(|error| error.to_string())?;
                    for reference in [&evidence.evidence_ref.id] {
                        auxiliary_refs.insert(reference.clone());
                        let identity = identity::auxiliary_reference(program, reference)?;
                        if program.artifacts.contains_key(&identity)
                            || reference.starts_with("tool://")
                            || reference.starts_with("approval:v1:")
                        {
                            if primary {
                                primary_refs.insert(identity.clone());
                            }
                            pending_references.insert(identity);
                        }
                    }
                    if evidence.evidence_ref.ref_type == "tool" {
                        let reference = if evidence.evidence_ref.id.starts_with("tool://") {
                            evidence.evidence_ref.id.clone()
                        } else {
                            format!("tool://{}", evidence.evidence_ref.id)
                        };
                        auxiliary_refs.insert(reference.clone());
                        if primary {
                            primary_refs.insert(reference.clone());
                        }
                        pending_references.insert(reference);
                    }
                    material_contents.insert(evidence.retrieval_selector.clone(), content);
                }
                packet_digests.insert(packet.graph_id().to_string(), digest);
                expanded_packet_roles.insert(packet.graph_id().to_string(), primary);
            }
            for material_root in &root_materials {
                let primary = primary_roots.contains(material_root);
                if selected_roots.get(material_root) != Some(&primary) {
                    selected_roots.insert(material_root.clone(), primary);
                    let genesis = self
                        .event_store()
                        .list_stream_after(material_root, 0, 1, 1, 4 * 1024 * 1024)?
                        .into_iter()
                        .next()
                        .ok_or("recursive root material graph genesis missing")?;
                    if genesis.commit_cursor > material_discovery_cursor {
                        return Err("recursive root material crosses the review discovery cut; rebuild the whole review".into());
                    }
                    start_cursor = start_cursor.min(genesis.commit_cursor);
                    pending_sources.insert(format!("session:{}", actor.session_id));
                }
            }
            if start_cursor < previous_start_cursor {
                pending_sources.extend(
                    material_revisions
                        .keys()
                        .filter(|source| {
                            source.starts_with("session:")
                                || source.starts_with("execution-agent-receipts:")
                        })
                        .cloned(),
                );
            }
            let dependencies = canonical_review_result_refs(program, result_refs)?
                .into_iter()
                .chain(auxiliary_refs.iter().cloned())
                .collect::<Vec<_>>();
            pending_metadata.extend(
                dependencies
                    .iter()
                    .map(|reference| identity::auxiliary_reference(program, reference))
                    .collect::<Result<Vec<_>, _>>()?,
            );
            while let Some(reference) = pending_metadata.pop_first() {
                let primary = primary_refs.contains(&reference);
                if inspected_metadata.get(&reference) == Some(&primary)
                    || inspected_metadata.get(&reference) == Some(&true)
                {
                    continue;
                }
                inspected_metadata.insert(reference.clone(), primary);
                let Some(artifact) = program.artifacts.get(&reference) else {
                    continue;
                };
                pending_references.insert(reference);
                for dependency in &artifact.relates_to {
                    let identity = identity::auxiliary_reference(program, dependency)?;
                    if primary {
                        primary_refs.insert(identity.clone());
                    }
                    if program.artifacts.contains_key(&identity) {
                        pending_metadata.insert(identity);
                    } else if program.tasks.contains_key(dependency) {
                        if primary {
                            primary_tasks.insert(dependency.clone());
                        }
                    } else if !program.tasks.contains_key(dependency) {
                        if dependency.starts_with("tool://")
                            || dependency.starts_with("approval:v1:")
                            || dependency.starts_with("artifact://")
                        {
                            pending_references.insert(identity);
                        } else {
                            return Err("recursive artifact dependency has no authenticated material identity".into());
                        }
                    }
                }
            }
            while let Some(reference) = pending_references.pop_first() {
                let identity = identity::auxiliary_reference(program, &reference)?;
                if !resolved_materials.insert(identity.clone())
                    && (!primary_refs.contains(&identity)
                        || expanded_primary_materials.contains(&identity))
                {
                    continue;
                }
                let material = results::resolve_material(self, actor, program, &identity).await?;
                material_contents.insert(identity, material.content.clone());
                dependency_results.push(material);
            }
            if !dependency_results.is_empty() {
                continue;
            }
            if !primary_tasks.is_subset(&expanded_primary_tasks) {
                continue;
            }
            let (_, artifacts, _) = selected_work(
                self.event_store(),
                program,
                goal.as_ref(),
                &dependencies,
                &mut material_revisions,
            )?;
            if artifacts
                .iter()
                .any(|reference| !resolved_materials.contains(reference))
            {
                pending_references.extend(artifacts);
                continue;
            }
            let Some(source) = pending_sources.pop_first() else {
                break;
            };
            let current_revision = self
                .event_store()
                .stream_revision(&source)
                .map_err(|error| error.to_string())?;
            let material_revision = *material_revisions
                .entry(source.clone())
                .or_insert(current_revision);
            if material_revision != current_revision {
                return Err(format!(
                    "recursive material enumeration cut changed: source={source} expected={material_revision} observed={current_revision} discovery_cursor={material_discovery_cursor}; rebuild the whole review"
                ).into());
            }
            let mut offset = 0;
            loop {
                let events = self
                    .event_store()
                    .list_stream_page_desc(&source, 64, offset)?;
                if self
                    .event_store()
                    .stream_revision(&source)
                    .map_err(|error| error.to_string())?
                    != material_revision
                {
                    return Err(
                        "actual read material source changed while paging; retry current inputs"
                            .into(),
                    );
                }
                if events.is_empty() {
                    break;
                }
                offset += events.len();
                let before_start = events
                    .iter()
                    .any(|event| event.commit_cursor < start_cursor);
                for event in events {
                    if event.commit_cursor < start_cursor {
                        continue;
                    }
                    let reference = receipt_alias(source.as_str(), &event);
                    let packet_read = source.starts_with("execution-agent-receipts:")
                        && event.kind == "execution.agent_tool.receipt"
                        && event.payload["effect_kind"] == "read"
                        && event.payload["outcome"]["status"] == "executed";
                    let root_read = event.kind == "tool.invocation.completed"
                        && event.payload["status"] == "completed"
                        && event.activity_binding().is_some_and(|binding| {
                            binding.session_id == actor.session_id
                                && binding.agent_run_id.is_none()
                                && root_materials.contains(&binding.root_execution_id)
                                && binding_is_owned_by_root(
                                    &binding,
                                    Some(&binding.root_execution_id),
                                )
                        })
                        && (event.payload["tool_name"]
                            .as_str()
                            .is_some_and(counts_as_physical_read)
                            || results::tool_effect_from_store(self.event_store(), &event)?
                                .is_some_and(|effect| {
                                    effect.effect_kind
                                        == harness_contract::tool::ToolEffectKind::Read
                                }));
                    if !packet_read && !root_read && !selected_receipts.contains(&reference) {
                        continue;
                    }
                    if event.commit_cursor > material_discovery_cursor {
                        return Err("actual material crosses the review discovery cut; rebuild the whole review".into());
                    }
                    if selected_receipts.contains(&reference) {
                        let matched = matched_receipt_events
                            .entry(reference.clone())
                            .or_default()
                            .insert(event.event_id.clone());
                        if matched && matched_receipt_events[&reference].len() > 1 {
                            return Err(
                                "v2 selected read matches more than one material receipt".into()
                            );
                        }
                        let actual_read = if source.starts_with("execution-agent-receipts:") {
                            event.kind == "execution.agent_tool.receipt"
                                && event.payload["outcome"]["status"] == "executed"
                                && event.payload["effect_kind"] == "read"
                        } else {
                            event.kind == "tool.invocation.completed"
                                && event.payload["status"] == "completed"
                        };
                        if !actual_read {
                            return Err("v2 selected read is not an actual material receipt".into());
                        }
                        matched_receipts.insert(reference);
                    }
                    let primary = packets.iter().any(|packet| {
                        primary_packets.contains(packet.graph_id())
                            && source.starts_with(&format!(
                                "execution-agent-receipts:{}:{}:",
                                packet.graph_id(),
                                packet.node_id()
                            ))
                    }) || event.activity_binding().is_some_and(|binding| {
                        binding.agent_run_id.is_none()
                            && primary_roots.contains(&binding.root_execution_id)
                            && binding_is_owned_by_root(&binding, Some(&binding.root_execution_id))
                    });
                    let event_key = (source.clone(), event.event_id.clone());
                    if selected_events.get(&event_key) == Some(&primary)
                        || selected_events.get(&event_key) == Some(&true)
                    {
                        continue;
                    }
                    if selected_events.insert(event_key, primary).is_none() {
                        material_events.push(harness_contract::goal::ProofInputEvent {
                            stream_id: source.clone(),
                            sequence: event.sequence,
                            event_id: event.event_id.clone(),
                            payload_digest: proof_value_digest(&event.payload),
                        });
                    }
                    let name = event.payload["tool_name"]
                        .as_str()
                        .or_else(|| event.payload["outcome"]["tool_name"].as_str())
                        .unwrap_or_default();
                    if root_read && name == "read_file" {
                        let (acquisition, index) = policy::root_file_acquisition(
                            self.event_store(),
                            &event,
                            &mut material_revisions,
                        )?;
                        for original in [&acquisition, &index] {
                            if original.commit_cursor > material_discovery_cursor {
                                return Err(
                                    "Root original acquisition crosses the material discovery cut"
                                        .into(),
                                );
                            }
                            let key = (original.stream_id.clone(), original.event_id.clone());
                            let previous = selected_events.get(&key).copied();
                            selected_events.insert(key, primary || previous.unwrap_or(false));
                            if previous.is_none() {
                                material_events.push(harness_contract::goal::ProofInputEvent {
                                    stream_id: original.stream_id.clone(),
                                    sequence: original.sequence,
                                    event_id: original.event_id.clone(),
                                    payload_digest: proof_value_digest(&original.payload),
                                });
                            }
                        }
                    }
                    let scope = if policy::goal_metadata_tool(name) {
                        None
                    } else {
                        policy::original_material_scope(
                            self.event_store(),
                            &event,
                            &mut material_revisions,
                        )?
                    };
                    if root_read
                        && name == "read_file"
                        && scope
                            .as_ref()
                            .is_none_or(|scope| !scope.complete || scope.targets.is_empty())
                    {
                        return Err(
                            "Root file dependency lacks its original complete physical scope"
                                .into(),
                        );
                    }
                    let mut immutable_material = scope.is_none()
                        && !counts_as_physical_read(name)
                        && !policy::goal_metadata_tool(name);
                    if counts_as_physical_read(name) {
                        let output = self
                            .actual_review_material_output(&actor.session_id, &event)
                            .await?;
                        if let Some(output) = output.and_then(|output| {
                            serde_json::from_str::<serde_json::Value>(&output).ok()
                        }) {
                            // G-d: the material judgment is about the material,
                            // not about the scope. This read has an authenticated
                            // material when it carries its own read-acquisition
                            // seal, or when its material output resolved as
                            // available and was authenticated against its
                            // original source identity below. A seal is judged by
                            // its value: a key whose value is json null carries
                            // no seal. The seal's decoding and authentication are
                            // unchanged, and the extent predicate further down is
                            // unchanged.
                            let read_carries_seal =
                                read_carries_acquisition_seal(&event.payload);
                            if read_carries_seal {
                                immutable_material = true;
                            }
                            if output["kind"] == "evidence_retrieve" && output["available"] == true
                            {
                                if let Some(selector) = output["evidence_ref"].as_str() {
                                    let session_events = results::result_source_events(
                                        self.event_store(),
                                        &format!("session:{}", actor.session_id),
                                        &mut material_revisions,
                                    )?;
                                    let original_primary = if !event.payload
                                        [crate::context_evidence::READ_ACQUISITION_FIELD]
                                        .is_null()
                                    {
                                        let seal = crate::context_evidence::decode_read_acquisition(
                                            &event.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                                        ).map_err(str::to_string)?;
                                        self.event_store()
                                            .event_by_idempotency_key(
                                                &seal.primary_stream,
                                                &seal.primary_key,
                                            )
                                            .map_err(|error| error.to_string())?
                                            .ok_or("immutable material original primary missing")?
                                    } else {
                                        let task =
                                            results::result_tool_plan(&session_events, &event)?
                                                .ok_or(
                                                "immutable material original governed plan missing",
                                            )?;
                                        results::original_tool_primary(
                                            self.event_store(),
                                            &event,
                                            task,
                                            &mut material_revisions,
                                        )?
                                    };
                                    let seal = crate::context_evidence::decode_read_acquisition(
                                        &original_primary.payload
                                            [crate::context_evidence::READ_ACQUISITION_FIELD],
                                    )
                                    .map_err(str::to_string)?;
                                    let index = self
                                        .event_store()
                                        .event_by_idempotency_key(
                                            &seal.index_stream,
                                            &seal.index_key,
                                        )
                                        .map_err(|error| error.to_string())?
                                        .ok_or("immutable material original index missing")?;
                                    let mut acquisition = policy::ActualInputSnapshot {
                                        file_dependencies: Vec::new(),
                                        immutable_inputs: Vec::new(),
                                        sources: BTreeMap::new(),
                                        material_events: Vec::new(),
                                        canonical_facts: Vec::new(),
                                    };
                                    let packet = packets.iter().find(|packet| {
                                        packet.graph_id() == seal.graph_id
                                            && packet.node_id() == seal.node_id
                                            && packet.attempt == seal.attempt
                                    });
                                    let input = policy::ActualImmutableInput::from_acquisition(
                                        self.event_store(),
                                        program,
                                        &original_primary,
                                        &index,
                                        &session_events,
                                        packet,
                                        ResultContentReaders {
                                            artifacts: Some(self.artifact_store().as_ref()),
                                            history: self.session_history_reader().as_deref(),
                                            process_inputs: Some(&process_frame),
                                        },
                                        &mut acquisition,
                                    )?;
                                    let review_ref =
                                        node_review_ref(&seal.graph_id, &seal.node_id);
                                    for original in [&original_primary, &index] {
                                        pin_result_source_ticketed(
                                            self.event_store(),
                                            &review_ref,
                                            seal.attempt,
                                            &original.stream_id,
                                            &mut acquisition.sources,
                                        )
                                        .map_err(results::ReviewEvidenceError::from_repin)?;
                                        acquisition.material_events.push(
                                            harness_contract::goal::ProofInputEvent {
                                                stream_id: original.stream_id.clone(),
                                                sequence: original.sequence,
                                                event_id: original.event_id.clone(),
                                                payload_digest: proof_value_digest(
                                                    &original.payload,
                                                ),
                                            },
                                        );
                                    }
                                    for material in acquisition.material_events {
                                        if !material_events.contains(&material) {
                                            material_events.push(material);
                                        }
                                    }
                                    for (source, revision) in acquisition.sources {
                                        if material_revisions
                                            .insert(source, revision)
                                            .is_some_and(|old| old != revision)
                                        {
                                            return Err("immutable material acquisition source changed during enumeration".into());
                                        }
                                    }
                                    let process_body = process_bodies.iter().find(|run| {
                                        input.reference == input.content.selector
                                            && run.fact.body == input.content
                                    });
                                    if let Some(run) = process_body {
                                        let original = self
                                            .event_store()
                                            .event_by_idempotency_key(
                                                &run.primary.stream_id,
                                                &format!(
                                                    "{}:receipt",
                                                    run.fact.admission.binding.call_key
                                                ),
                                            )
                                            .map_err(|error| error.to_string())?
                                            .ok_or(
                                                "bounded process original body primary missing",
                                            )?;
                                        let authenticated = results::authenticate_bounded_process(
                                            self.event_store(),
                                            &original,
                                        )?;
                                        if authenticated.fact != run.fact
                                            || authenticated.primary != run.primary
                                        {
                                            return Err(
                                                "bounded process original body identity changed"
                                                    .into(),
                                            );
                                        }
                                        results::process_has_no_external_effects(&authenticated)?;
                                        for material in results::retain_process_execution(
                                            self.event_store(),
                                            &authenticated,
                                            &mut material_revisions,
                                        )? {
                                            if !material_events.contains(&material) {
                                                material_events.push(material);
                                            }
                                        }
                                    }
                                    let dependency_identity = input.dependency_identity(program)?;
                                    let physical_body = dependency_identity.is_none();
                                    let identity = if physical_body {
                                        input.content.selector.clone()
                                    } else {
                                        dependency_identity.ok_or("immutable dependency identity missing")?
                                    };
                                    if primary && !physical_body {
                                        primary_refs.insert(identity.clone());
                                    }
                                    let material = if !physical_body {
                                        Some(
                                            results::resolve_material(
                                                self, actor, program, &identity,
                                            )
                                            .await?,
                                        )
                                    } else {
                                        None
                                    };
                                    if let Some(material) = &material {
                                        input.validate_dependency(program, &identity, material)?;
                                    }
                                    let content = if let Some(material) = &material {
                                        material.content.clone()
                                    } else if let Some(content) = material_contents.get(&identity) {
                                        content.clone()
                                    } else if selector.starts_with("artifact://") {
                                        let content = self
                                            .artifact_store()
                                            .resolve(selector)
                                            .map_err(|error| error.to_string())?;
                                        let session_scope = format!("session:{}", actor.session_id);
                                        let authorized_scope = if content.visibility_scope
                                            == session_scope
                                            || content.visibility_scope == "public"
                                        {
                                            session_scope.as_str()
                                        } else if packets.iter().any(|packet| {
                                            source.starts_with(&format!(
                                                "execution-agent-receipts:{}:{}:",
                                                packet.graph_id(),
                                                packet.node_id(),
                                            )) && packet
                                                .resource_scopes
                                                .contains(&content.visibility_scope)
                                        }) {
                                            content.visibility_scope.as_str()
                                        } else {
                                            return Err("actual immutable material is outside its original read grant".into());
                                        };
                                        self.artifact_store()
                                            .read(&content, authorized_scope, None)
                                            .await
                                            .map_err(|error| error.to_string())?;
                                        content
                                    } else {
                                        return Err("actual immutable material has no authenticated content identity".into());
                                    };
                                    if output["sha256"].as_str() != Some(content.sha256.as_str())
                                        || output["bytes"].as_u64() != Some(content.bytes)
                                    {
                                        return Err("immutable review material does not match its original source identity".into());
                                    }
                                    // The chain above authenticated this material
                                    // against its original source identity: the
                                    // original primary and its index were
                                    // resolved and the content matched. The
                                    // scope's targets are not a proxy for that.
                                    immutable_material = true;
                                    for source in material
                                        .iter()
                                        .flat_map(|material| &material.result_sources)
                                    {
                                        if verification
                                            .result_source_revisions
                                            .insert(
                                                source.stream_id.clone(),
                                                source.expected_revision,
                                            )
                                            .is_some_and(|revision| {
                                                revision != source.expected_revision
                                            })
                                        {
                                            return Err("immutable approval material source changed during enumeration".into());
                                        }
                                    }
                                    material_contents.insert(identity.clone(), content);
                                    if let Some(material) = material {
                                        resolved_materials.insert(identity.clone());
                                        dependency_results.push(material);
                                    }
                                    if !physical_body { auxiliary_refs.insert(identity); }
                                }
                            }
                        }
                    }
                    // An authenticated Program-directory read is metadata,
                    // not an artifact or a physical workspace acquisition.
                    let mut directory_metadata = false;
                    if !immutable_material && packet_read && name == "read_ref"
                        && event.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null()
                        && scope.as_ref().is_none_or(|scope| !scope.complete || scope.targets.is_empty()) {
                        let packet = directory_read_packet(&packets, &source)?;
                        let key = event.payload["idempotency_key"].as_str().ok_or("directory read original key missing")?;
                        let primary_stream = format!("execution-effect:{key}");
                        let mut authenticated = policy::ActualInputSnapshot {
                            file_dependencies: Vec::new(), immutable_inputs: Vec::new(),
                            sources: material_revisions.clone(), material_events: Vec::new(), canonical_facts: Vec::new(),
                        };
                        pin_result_source_ticketed(
                            self.event_store(),
                            &node_review_ref(packet.graph_id(), packet.node_id()),
                            packet.attempt,
                            &primary_stream,
                            &mut authenticated.sources,
                        )
                        .map_err(results::ReviewEvidenceError::from_repin)?;
                        let original = self.event_store().event_by_idempotency_key(&primary_stream, &format!("{key}:read-receipt"))
                            .map_err(|error| error.to_string())?.ok_or("directory read original primary missing")?;
                        if original.commit_cursor > material_discovery_cursor {
                            return Err("directory read crosses the review discovery cut".into());
                        }
                        policy::verify_actual_read_pair(&original, &event)?;
                        if !original.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null() {
                            return Err("directory read primary/index seal differs".into());
                        }
                        let input = policy::authenticate_unsealed_delegated_input(
                            self.event_store(), &original, &event, packet, &mut authenticated)?;
                        if crate::agentic::action_invocation::read_ref_directory_input(&input)?.is_some() {
                            let original_fact = harness_contract::goal::ProofInputEvent {
                                stream_id: original.stream_id.clone(), sequence: original.sequence,
                                event_id: original.event_id.clone(), payload_digest: proof_value_digest(&original.payload),
                            };
                            if !material_events.contains(&original_fact) { material_events.push(original_fact); }
                            for fact in authenticated.material_events {
                                if !material_events.contains(&fact) { material_events.push(fact); }
                            }
                            material_revisions = authenticated.sources;
                            directory_metadata = true;
                        }
                    }
                    if !directory_metadata && !immutable_material
                        && !policy::goal_metadata_tool(name)
                        && (packets.iter().any(|packet| {
                            !initial_material_packets.contains(packet.graph_id())
                                && source.starts_with(&format!(
                                    "execution-agent-receipts:{}:{}:",
                                    packet.graph_id(),
                                    packet.node_id()
                                ))
                        }) || event.activity_binding().is_some_and(|binding| {
                            root_materials.contains(&binding.root_execution_id)
                                && !initial_root_materials.contains(&binding.root_execution_id)
                        }))
                        && scope
                            .as_ref()
                            .is_none_or(|scope| !scope.complete || scope.targets.is_empty())
                    {
                        return Err(
                            "recursive physical material has no authenticated admission coverage"
                                .into(),
                        );
                    }
                    if !directory_metadata && !immutable_material
                        && counts_as_physical_read(name)
                        && scope
                            .as_ref()
                            .is_none_or(|scope| !scope.complete || scope.targets.is_empty())
                    {
                        return Err("proof auxiliary read has no authenticated immutable or physical material".into());
                    }
                }
                if before_start {
                    break;
                }
            }
            #[cfg(test)]
            if let Some((_, entered, release)) = PROOF_MATERIAL_CUT_BARRIER
                .try_with(|stored| {
                    let mut stored = stored.borrow_mut();
                    if stored
                        .as_ref()
                        .is_some_and(|(expected, _, _)| expected == &source)
                    {
                        stored.take()
                    } else {
                        None
                    }
                })
                .ok()
                .flatten()
            {
                entered.notify_one();
                release.notified().await;
            }
            if self
                .event_store()
                .stream_revision(&source)
                .map_err(|error| error.to_string())?
                != material_revision
            {
                return Err(
                    "actual read material enumeration cut changed; rebuild the whole review".into(),
                );
            }
        }
        verification.producer_execution_ids.sort();
        verification.producer_execution_ids.dedup();
        verification.effect_source_refs.sort();
        verification.effect_source_refs.dedup();
        if selected_receipts != matched_receipts {
            return Err("v2 selected physical read has no matching origin material event".into());
        }
        if verification
            .reads
            .iter()
            .any(|read| read.receipt_refs.is_empty())
        {
            return Err("v2 proof requires complete physical read receipts".into());
        }
        material_events.sort_by(|left, right| {
            (&left.stream_id, &left.event_id).cmp(&(&right.stream_id, &right.event_id))
        });
        material_events.dedup();
        let actual_history = self.session_history_reader();
        let actual = policy::actual_input_snapshot(
            self.event_store(),
            program,
            &actual_boundaries,
            ResultContentReaders {
                artifacts: Some(self.artifact_store().as_ref()),
                history: actual_history.as_deref(),
                process_inputs: Some(&process_frame),
            },
            None,
        )?;
        required_scopes = actual.required_scopes();
        for material in actual.material_events {
            if !material_events.contains(&material) {
                material_events.push(material);
            }
        }
        for (source, revision) in actual.sources {
            if let Some(previous) = material_revisions.insert(source.clone(), revision) {
                if previous != revision {
                    return Err(format!(
                        "actual input source moved after material discovery: source {source} \
                         moved from revision {previous} to {revision}"
                    ).into());
                }
            }
        }
        required_scopes.sort_by_key(proof_value_digest);
        if !packets.iter().all(|packet| {
            packet
                .required_acceptance
                .evidence_obligations
                .iter()
                .all(|obligation| match &obligation.target {
                    harness_contract::context::EvidenceTargetIdentity::Workspace { scope } => {
                        use harness_contract::context::{
                            EvidenceObligationKind, WorkspaceObjectKind,
                        };
                        let same_path =
                            |path: &harness_contract::context::WorkspacePathIdentity| {
                                path.workspace_id == scope.path.workspace_id
                                    && path.repository_id == scope.path.repository_id
                                    && path.workspace_relative_path
                                        == scope.path.workspace_relative_path
                                    && path.repository_relative_path
                                        == scope.path.repository_relative_path
                            };
                        scope.path.object_kind == WorkspaceObjectKind::File
                            && match obligation.kind {
                                EvidenceObligationKind::ContentRead
                                | EvidenceObligationKind::VerifyUpstreamChange => {
                                    required_scopes.iter().any(|target| same_path(&target.path))
                                        || complete_file_reads
                                            .iter()
                                            .any(|read| same_path(&read.path))
                                }
                                EvidenceObligationKind::WriteEffect => business_effect_targets
                                    .iter()
                                    .any(|(target, _)| same_path(&target.path)),
                                EvidenceObligationKind::VerifyAfterWrite => {
                                    business_effect_targets.iter().any(|(target, cursor)| {
                                        same_path(&target.path)
                                            && complete_file_reads.iter().any(|read| {
                                                same_path(&read.path) && read.cursor > *cursor
                                            })
                                    })
                                }
                                _ => false,
                            }
                    }
                    _ => false,
                })
        }) {
            return Err(
                "proof packet evidence obligation has no authenticated physical material coverage"
                    .into(),
            );
        }
        for reference in &auxiliary_refs {
            if reference.starts_with("approval:v1:") {
                let decision = results::resolve_material(self, actor, program, reference).await?;
                for source in decision.result_sources {
                    if verification
                        .result_source_revisions
                        .insert(source.stream_id, source.expected_revision)
                        .is_some_and(|previous| previous != source.expected_revision)
                    {
                        return Err(
                            "approval source changed during actual material collection".into()
                        );
                    }
                }
            }
        }
        let dependencies = canonical_review_result_refs(program, result_refs)?
            .into_iter()
            .chain(auxiliary_refs.iter().cloned())
            .collect::<Vec<_>>();
        let (task_refs, artifact_refs, _) = selected_work(
            self.event_store(),
            program,
            goal.as_ref(),
            &dependencies,
            &mut material_revisions,
        )?;
        verification.input_set = Some(harness_contract::goal::ProofInputSet {
            version: 2,
            condition_ref: condition_ref.map(str::to_string),
            result_refs: result_refs.to_vec(),
            auxiliary_refs: auxiliary_refs.into_iter().collect(),
            required_obligation_refs: goal
                .iter()
                .flat_map(|goal| &goal.obligations)
                .filter(|obligation| obligation.required)
                .map(|obligation| obligation.obligation_id.clone())
                .collect(),
            task_refs,
            artifact_refs,
            packet_digests,
            material_events,
            source_revisions: material_revisions,
            required_scopes,
            required_scope_complete,
            acceptance_policy_digest: String::new(),
            semantic_digest: String::new(),
        });
        let history = self.session_history_reader();
        let readers = ResultContentReaders {
            artifacts: Some(self.artifact_store().as_ref()),
            history: history.as_deref(),
            process_inputs: Some(&process_frame),
        };
        let snapshot = proof_input_snapshot_for_action(
            self.event_store(),
            program,
            goal.as_ref(),
            &verification,
            readers,
            current_review.map(ReviewActionIdentity::Live),
        )?;
        // The one place the entries are made: the review's own snapshot names
        // every required write that ran and failed. The snapshot's own fields
        // are moved into the proof below, so the entries are taken first.
        let required_write_failures = coverage::required_write_failure_entries(&snapshot);
        verification.work_manifest_digest = snapshot.work_digest;
        verification.effect_manifest_digest = Some(snapshot.effects.digest);
        // The identity of the review control call this proof was built
        // under. It travels with the proof as a CANDIDATE: every entry that
        // re-verifies the proof re-binds it against the durable evidence.
        verification.current_review_effect_key = snapshot.current_review_effect_key.clone();
        if !verification.independence_required {
            verification.review_policy_digest = Some(snapshot.policy_digest.clone());
        }
        let inputs = verification
            .input_set
            .as_mut()
            .expect("v2 inputs were constructed");
        inputs.semantic_digest = snapshot.semantic_digest;
        inputs.acceptance_policy_digest = snapshot.policy_digest;
        inputs.task_refs = snapshot.task_refs;
        inputs.artifact_refs = snapshot.artifact_refs;
        inputs.source_revisions = retain_producer_material_cuts(
            &inputs.source_revisions, &snapshot.sources,
        )?;
        Ok(IndependentReview {
            producers: producers.into_iter().collect(),
            verification,
            required_write_failures,
        })
    }

    /// True when the runtime handed this reviewer the submitted bodies for its
    /// exact review execution (a durable prefetch attestation), which satisfies
    /// the physical-read requirement without a separate tool read.
    fn review_body_prefetched(
        &self,
        actor: &harness_contract::agent_action::AgentActorBinding,
        program: &crate::AgenticProgramProjection,
    ) -> bool {
        let stream_id = format!("agentic-program:{}", program.program_id);
        let Ok(events) = self.event_store().list_stream(&stream_id) else {
            return false;
        };
        events.iter().any(|event| {
            event.kind == "agentic.review_body_prefetched"
                && event
                    .payload
                    .get("program_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(program.program_id.as_str())
                && event
                    .payload
                    .get("execution_id")
                    .and_then(serde_json::Value::as_str)
                    == actor.execution_id.as_deref()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_closure_retains_each_discovered_material_cut() {
        let material = BTreeMap::from([("effect:original".to_string(), 3)]);
        let source = |stream: &str, revision| crate::ExpectedStreamRevision {
            stream_id: stream.into(), expected_revision: revision,
        };
        let stable = retain_producer_material_cuts(
            &material, &[source("effect:original", 3), source("session:one", 7)],
        ).unwrap();
        assert_eq!(stable["effect:original"], 3);
        assert_eq!(stable["session:one"], 7);
        assert!(retain_producer_material_cuts(&material, &[source("session:one", 7)]).is_err());
        assert!(retain_producer_material_cuts(&material, &[source("effect:original", 4)]).is_err());
        assert!(retain_producer_material_cuts(
            &material, &[source("effect:original", 3), source("effect:original", 4)],
        ).is_err());
    }

    /// An intent and a read in the same delegated receipt stream carry the
    /// same logical sequence under the same payload key, so only a genuine
    /// receipt may name a receipt.
    ///
    /// The collector used to build a receipt alias for every event in the
    /// stream, so the intent's alias collided with the read's alias and the
    /// collector refused a read that had really happened.
    #[test]
    fn r1_receipt_alias_never_names_an_intent() {
        let source = "execution-agent-receipts:graph:node:agent:1";
        let event = |kind: &str, event_id: &str, sequence: u64, payload: serde_json::Value| {
            crate::DurableRuntimeEvent {
                event_id: event_id.into(),
                stream_id: source.into(),
                sequence,
                scope: crate::RuntimeEventScope::ExecutionNode,
                kind: kind.into(),
                status: None,
                actor: Some("governed_tool".into()),
                refs: Vec::new(),
                payload,
                created_at_ms: 0,
                commit_cursor: 0,
                transaction_id: "tx".into(),
                transaction_index: 0,
                schema_version: 1,
                idempotency_key: None,
            }
        };
        let read = event(
            "execution.agent_tool.receipt",
            "event-read",
            1,
            serde_json::json!({
                "sequence": 1,
                "effect_kind": "read",
                "outcome": {"status": "executed"},
            }),
        );
        let intent = event(
            "execution.agent_tool.intent",
            "event-intent",
            2,
            serde_json::json!({
                "sequence": 1,
                "effect_kind": "write",
                "tool_name": "task_review",
            }),
        );

        // The read keeps the receipt alias the producer emits.
        assert_eq!(receipt_alias(source, &read), format!("{source}#1"));
        // The intent never names a receipt, and never collides with the read.
        assert_ne!(receipt_alias(source, &intent), receipt_alias(source, &read));
        assert!(!receipt_alias(source, &intent).starts_with(source));
        assert_eq!(receipt_alias(source, &intent), "runtime-event:event-intent");
    }

    #[test]
    fn a_refusal_names_which_producer_requirement_was_not_met() {
        // V27-B2: the old text listed all four requirements, so a Task with
        // the right capability and the wrong definition was told both were
        // wrong. The model must be able to tell which one to fix.
        let required = harness_contract::goal::ObjectiveProducerContract {
            capability_id: Some("capability:reader".into()),
            agent_definition_ref: Some("definition:reviewer".into()),
            skill_ref: None,
            tool_ref: None,
        };
        let refusal = unmet_obligation_error("obligation:1", &required);
        assert!(
            refusal.contains("capability `capability:reader`"),
            "{refusal}"
        );
        assert!(
            refusal.contains("agent definition `definition:reviewer`"),
            "{refusal}"
        );
        // A requirement that was not set is not named as unmet.
        assert!(!refusal.contains("skill `"), "{refusal}");
        assert!(!refusal.contains("tool `"), "{refusal}");
        // And the way out is stated.
        assert!(refusal.contains("Publish a Task"), "{refusal}");
    }

    /// A reader that reports one revision per stream. The A2 assertions are
    /// about the pin, so the reader only has to answer heads.
    struct A2PinReader {
        revisions: std::collections::BTreeMap<String, u64>,
    }

    impl super::read_source::ReadAuthSource for A2PinReader {
        fn stream_revision(&mut self, stream: &str) -> Result<u64, String> {
            Ok(self.revisions.get(stream).copied().unwrap_or_default())
        }
        fn event_by_idempotency_key(
            &mut self,
            _stream: &str,
            _key: &str,
        ) -> Result<Option<crate::DurableRuntimeEvent>, String> {
            Ok(None)
        }
        fn list_stream_after(
            &mut self,
            _stream: &str,
            _after: u64,
            _through: u64,
            _limit: usize,
            _bytes: usize,
        ) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
            Ok(Vec::new())
        }
        fn transaction_stream_revision_exact(
            &mut self,
            _transaction: &str,
            _stream: &str,
        ) -> Result<Option<crate::CommittedStreamRevision>, String> {
            Ok(None)
        }
        fn resolve_graph_record(
            &mut self,
            _record: &crate::DurableRuntimeEvent,
        ) -> Result<serde_json::Value, String> {
            Ok(serde_json::Value::Null)
        }
    }

    /// A2f-P1: an execution graph head that grew while the review was reading
    /// is another actor advancing it, and the review goes on. The same pin
    /// path refuses a head that moved backwards.
    #[test]
    fn a2f_a_growing_execution_graph_head_is_admissible_and_a_backwards_head_is_not() {
        let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
        let mut cuts = std::collections::BTreeMap::new();
        reader.revisions.insert("agentic-graph:abc".to_string(), 4);
        assert_eq!(
            super::results::pin_result_source_with_reader(
                &mut reader,
                "agentic-graph:abc",
                &mut cuts
            )
            .expect("the first pin"),
            4
        );
        // The graph advanced while this review was reading it.
        reader.revisions.insert("agentic-graph:abc".to_string(), 5);
        assert_eq!(
            super::results::pin_result_source_with_reader(
                &mut reader,
                "agentic-graph:abc",
                &mut cuts
            )
            .expect("a growing execution graph head is admissible"),
            5
        );
        // A head that moved backwards is a truncated or replaced stream.
        reader.revisions.insert("agentic-graph:abc".to_string(), 4);
        let error = super::results::pin_result_source_with_reader(
            &mut reader,
            "agentic-graph:abc",
            &mut cuts,
        )
        .expect_err("a backwards head must still be refused");
        assert!(
            error.contains("result metadata source moved backwards during resolution"),
            "{error}"
        );
        assert_eq!(cuts.get("agentic-graph:abc"), Some(&5));
    }

    /// A2f-N2: a source that is not a shared aggregate stream still refuses a
    /// moved head, with the original wording.
    #[test]
    fn a2f_a_non_tolerated_source_head_that_moved_is_still_refused() {
        for source in [
            "agentic-program:abc",
            "goal:abc",
            "execution-effect:abc",
            "execution-agent-receipts:abc:node",
        ] {
            let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
            let mut cuts = std::collections::BTreeMap::new();
            reader.revisions.insert(source.to_string(), 4);
            super::results::pin_result_source_with_reader(&mut reader, source, &mut cuts)
                .expect("the first pin");
            reader.revisions.insert(source.to_string(), 5);
            let error = super::results::pin_result_source_with_reader(
                &mut reader,
                source,
                &mut cuts,
            )
            .expect_err("a moved head on a non-tolerated source must be refused");
            assert!(
                error.contains("result metadata source changed during resolution"),
                "{source}: {error}"
            );
            assert_eq!(cuts.get(source), Some(&4));
        }
    }

    /// T1a-N1: a refused source pin produces the named rebuild ticket, and the
    /// ticket's own reason is the sentence the pin has always produced. The
    /// source and the two revisions below are the shape the 029 explicit-team
    /// run died on: the receipts stream grew while one reviewer was resolving.
    #[test]
    fn t1a_a_refused_growth_produces_a_named_ticket_that_repins_at_the_fresh_head() {
        let receipts =
            "execution-agent-receipts:agentic-graph:one:agentic-graph:one:agent:1";
        let review = "agentic-graph:one/agent:1";
        let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
        let mut cuts = std::collections::BTreeMap::new();
        reader.revisions.insert(receipts.to_string(), 14);
        assert_eq!(
            super::results::pin_result_source_ticketed_with_reader(
                &mut reader, review, 1, receipts, &mut cuts,
            )
            .expect("the first pin reads the head as its window"),
            14
        );
        reader.revisions.insert(receipts.to_string(), 16);
        let rejection = super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 1, receipts, &mut cuts,
        )
        .expect_err("growth on a receipts stream is still refused");
        let ticket = rejection
            .ticket()
            .expect("a window refusal carries its ticket")
            .clone();
        assert_eq!(ticket.review_ref(), review);
        assert_eq!(ticket.attempt(), 1);
        assert_eq!(ticket.source(), receipts);
        assert_eq!(ticket.pinned_cut(), 14, "the cut this review pinned");
        assert_eq!(ticket.observed_head(), 16, "the head the store reported");
        assert_eq!(ticket.movement(), super::ReviewSourceMovement::ForwardNotAccepted);
        assert!(ticket.movement().rebuildable());
        assert_eq!(
            ticket.reason(),
            format!(
                "result metadata source changed during resolution; rebuild the review; source={receipts} expected=14 actual=16 phase=result_source_pin"
            ),
            "the ticket must not change the propagated wording"
        );
        assert_eq!(
            super::ReviewRepinTicket::from_refusal(review, 1, &ticket.reason()).as_ref(),
            Some(&ticket),
            "the reason round-trips back into the same ticket"
        );
        assert_eq!(cuts[receipts], 14, "a refusal leaves the window where it was");
        // The rebuild window comes from the store: the caller names only who it is.
        assert_eq!(
            super::results::repin_at_fresh_head_with_reader(
                &mut reader, review, 1, &ticket, &mut cuts,
            )
            .expect("the rebuild opens at the fresh head"),
            16
        );
        assert_eq!(cuts[receipts], 16);
        assert_eq!(
            super::results::pin_result_source_ticketed_with_reader(
                &mut reader, review, 1, receipts, &mut cuts,
            )
            .expect("a settled window lets the rebuilt review resolve"),
            16
        );
    }

    /// T1a-N2: a rebuilt window that moves again is refused again on the same
    /// lineage, and a rebuild refusal cannot be laundered into a fresh rebuild.
    #[test]
    fn t1a_a_rebuilt_window_that_moves_again_refuses_again_on_the_same_lineage() {
        let source = "execution-effect:abc";
        let review = "agentic-graph:two/agent:1";
        let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
        let mut cuts = std::collections::BTreeMap::new();
        reader.revisions.insert(source.to_string(), 14);
        super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 2, source, &mut cuts,
        )
        .expect("the first pin");
        reader.revisions.insert(source.to_string(), 16);
        let first_rejection = super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 2, source, &mut cuts,
        )
        .expect_err("the first growth refuses");
        let first = first_rejection.ticket().expect("window refusal").clone();
        assert_eq!(
            super::results::repin_at_fresh_head_with_reader(
                &mut reader, review, 2, &first, &mut cuts,
            )
            .expect("the rebuild opens"),
            16
        );
        // The window the rebuild just opened is what moved: refused again.
        reader.revisions.insert(source.to_string(), 18);
        let second_rejection = super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 2, source, &mut cuts,
        )
        .expect_err("a second growth refuses again");
        let second = second_rejection.ticket().expect("window refusal").clone();
        assert_eq!(
            second.lineage(),
            first.lineage(),
            "both refusals belong to one review attempt, which is what a bounded budget counts"
        );
        assert_eq!((second.pinned_cut(), second.observed_head()), (16, 18));
        let rebuild_refusal = super::RepinRejection::Window { refused: first.clone(), pinned: 999 };
        assert!(rebuild_refusal.reason().contains("movement=window-mismatch"));
        assert!(
            super::ReviewRepinTicket::from_refusal(review, 2, &rebuild_refusal.reason()).is_none(),
            "a rebuild refusal is a budget answer, not a fresh rebuild request"
        );
    }

    /// T1a-N3: a head that moved backwards keeps its original refusal and is not
    /// a rebuildable window. Re-reading at "whatever the head is now" would
    /// resolve the review against material nobody observed.
    #[test]
    fn t1a_a_backwards_head_is_refused_and_never_rebuildable() {
        let source = "agentic-program:abc";
        let review = "agentic-graph:three/agent:1";
        let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
        let mut cuts = std::collections::BTreeMap::new();
        reader.revisions.insert(source.to_string(), 5);
        super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 3, source, &mut cuts,
        )
        .expect("the first pin");
        reader.revisions.insert(source.to_string(), 3);
        let rejection = super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 3, source, &mut cuts,
        )
        .expect_err("a backwards head is refused");
        let ticket = rejection.ticket().expect("window refusal").clone();
        assert_eq!(ticket.movement(), super::ReviewSourceMovement::Backwards);
        assert!(!ticket.movement().rebuildable());
        assert_eq!(
            ticket.reason(),
            "result metadata source moved backwards during resolution; rebuild the review; source=agentic-program:abc expected=5 actual=3 phase=result_source_pin"
        );
        let refused = super::results::repin_at_fresh_head_with_reader(
            &mut reader, review, 3, &ticket, &mut cuts,
        )
        .expect_err("a backwards head does not open a rebuild window");
        assert_eq!(refused, super::RepinRejection::NotRebuildable(ticket.clone()));
        assert_eq!(refused.reason(), ticket.reason(), "the refusal wording survives untouched");
        assert_eq!(cuts[source], 5, "a refused rebuild leaves the window alone");
    }

    /// T1a-N4: the rebuild entry opens a window only for the review that was
    /// refused, only at the window it was refused on, and never below the head
    /// the refusal reported.
    #[test]
    fn t1a_the_rebuild_entry_refuses_a_foreign_review_a_stale_window_and_a_shrunken_head() {
        let source = "execution-agent-receipts:agentic-graph:four:agent:1";
        let review = "agentic-graph:four/agent:1";
        let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
        let mut cuts = std::collections::BTreeMap::new();
        reader.revisions.insert(source.to_string(), 14);
        super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 1, source, &mut cuts,
        )
        .expect("the first pin");
        reader.revisions.insert(source.to_string(), 16);
        let rejection = super::results::pin_result_source_ticketed_with_reader(
            &mut reader, review, 1, source, &mut cuts,
        )
        .expect_err("the growth refuses");
        let ticket = rejection.ticket().expect("window refusal").clone();
        let foreign = super::results::repin_at_fresh_head_with_reader(
            &mut reader, "agentic-graph:other/agent:1", 7, &ticket, &mut cuts,
        )
        .expect_err("another review cannot spend this refusal");
        assert!(foreign.reason().contains("movement=identity-mismatch"), "{}", foreign.reason());
        assert!(foreign.reason().contains("actual=agentic-graph:other/agent:1#7"));
        assert_eq!(cuts[source], 14);
        // A stale window: the entry this map pins is no longer the refused one.
        cuts.insert(source.to_string(), 15);
        let stale = super::results::repin_at_fresh_head_with_reader(
            &mut reader, review, 1, &ticket, &mut cuts,
        )
        .expect_err("a moved window is refused");
        assert!(stale.reason().contains("movement=window-mismatch"), "{}", stale.reason());
        assert!(stale.reason().contains("actual=15"));
        cuts.insert(source.to_string(), 14);
        // The stream shrank below the head the refusal reported.
        reader.revisions.insert(source.to_string(), 15);
        let behind = super::results::repin_at_fresh_head_with_reader(
            &mut reader, review, 1, &ticket, &mut cuts,
        )
        .expect_err("a truncated stream is not a rebuild window");
        assert!(
            behind.reason().contains("movement=head-behind-refused-head"),
            "{}",
            behind.reason()
        );
        assert!(behind.reason().contains("actual=15"));
        assert_eq!(cuts[source], 14, "none of the three rewrites the window");
    }

    /// T1a-N5: an accepted pin produces no ticket, so a shared aggregate that
    /// keeps growing never triggers a rebuild at all.
    #[test]
    fn t1a_an_accepted_pin_produces_no_ticket_and_a_shared_aggregate_never_needs_one() {
        for source in [
            "session:one",
            "agentic-graph:abc",
            "execution-lineage:abc",
            "session-ingress-graph:abc",
        ] {
            let review = "agentic-graph:five/agent:1";
            let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
            let mut cuts = std::collections::BTreeMap::new();
            reader.revisions.insert(source.to_string(), 4);
            assert_eq!(
                super::results::pin_result_source_ticketed_with_reader(
                    &mut reader, review, 5, source, &mut cuts,
                )
                .expect("the first pin"),
                4
            );
            reader.revisions.insert(source.to_string(), 5);
            assert_eq!(
                super::results::pin_result_source_ticketed_with_reader(
                    &mut reader, review, 5, source, &mut cuts,
                )
                .expect("growth on a shared aggregate is another actor appending"),
                5
            );
            assert_eq!(cuts[source], 5, "the pinned window moved with the aggregate");
        }
        assert!(super::ReviewRepinTicket::from_refusal("review", 1, "some other failure").is_none());
    }

    /// T1a-R1: the source shapes the field refusals actually used, replayed in
    /// memory. Each one refuses, names its own window, and rebuilds at the head
    /// the store reports - including the 028-era refusal, which is why the
    /// rebuild is not read as a 029 regression.
    #[test]
    fn t1a_the_field_refusal_shapes_rebuild_from_their_own_windows() {
        for (source, pinned, head) in [
            (
                "execution-agent-receipts:agentic-graph:33637f01:agentic-graph:33637f01:agent:1",
                14,
                16,
            ),
            (
                "execution-agent-receipts:agentic-graph:8c0a6fd5:agentic-graph:8c0a6fd5:agent:1",
                3,
                6,
            ),
            ("execution-effect:read-key", 4, 5),
            ("agentic-program:abc", 7, 8),
        ] {
            let review = "agentic-graph:replay/agent:1";
            let mut reader = A2PinReader { revisions: std::collections::BTreeMap::new() };
            let mut cuts = std::collections::BTreeMap::new();
            reader.revisions.insert(source.to_string(), pinned);
            super::results::pin_result_source_ticketed_with_reader(
                &mut reader, review, 1, source, &mut cuts,
            )
            .expect("the first pin");
            reader.revisions.insert(source.to_string(), head);
            let rejection = super::results::pin_result_source_ticketed_with_reader(
                &mut reader, review, 1, source, &mut cuts,
            )
            .expect_err("a moved head on these sources is refused");
            let ticket = rejection.ticket().expect("window refusal").clone();
            assert_eq!((ticket.pinned_cut(), ticket.observed_head()), (pinned, head), "{source}");
            assert_eq!(
                super::results::repin_at_fresh_head_with_reader(
                    &mut reader, review, 1, &ticket, &mut cuts,
                )
                .expect("the rebuild opens at the store's head"),
                head,
                "{source}"
            );
            assert_eq!(cuts[source], head);
        }
    }

    /// 030 T7 (e2 option B): the review's closing producer-metadata fence is
    /// self-consistency inside the published cut. The cuts below are produced by
    /// the real pinning and bounded paging path, not by hand.
    #[test]
    fn t7_the_producer_metadata_fence_settles_a_busy_session_and_still_refuses_drift() {
        let store = crate::RuntimeEventStore::for_test();
        let append = |store: &crate::RuntimeEventStore, stream: &str, scope| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: stream.into(),
                    scope,
                    kind: "test.t7-append".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({}),
                })
                .unwrap();
        };
        let session = "session:t7-v3";
        let mut cuts = std::collections::BTreeMap::new();
        for _ in 0..3 {
            append(&store, session, crate::RuntimeEventScope::Session);
        }
        assert_eq!(
            super::results::result_source_events(&store, session, &mut cuts)
                .expect("the real bounded read")
                .len(),
            3
        );
        let published = vec![crate::ExpectedStreamRevision {
            stream_id: session.into(),
            expected_revision: cuts[session],
        }];
        assert_eq!(published[0].expected_revision, 3);
        // The Session keeps appending while the reviewer is still resolving. The
        // fence used to refuse this with an unclassified sentence.
        for _ in 0..2 {
            append(&store, session, crate::RuntimeEventScope::Session);
        }
        super::verify_producer_metadata_cuts(&store, published.iter())
            .expect("growth above the published cut cites nothing this review read");
        assert_eq!(cuts[session], 3, "the fence does not chase the head");
        assert_eq!(
            published[0].expected_revision, 3,
            "the published cut stays published"
        );
        // A judged source moving is the object under review moving: still refused,
        // with the sentence unchanged.
        let judged = "execution-effect:t7-v3-key";
        let mut judged_cuts = std::collections::BTreeMap::new();
        append(&store, judged, crate::RuntimeEventScope::ExecutionNode);
        super::results::result_source_events(&store, judged, &mut judged_cuts).unwrap();
        append(&store, judged, crate::RuntimeEventScope::ExecutionNode);
        let drift = vec![crate::ExpectedStreamRevision {
            stream_id: judged.into(),
            expected_revision: judged_cuts[judged],
        }];
        assert_eq!(
            super::verify_producer_metadata_cuts(&store, drift.iter())
                .expect_err("a judged source that moved still refuses"),
            "producer metadata changed during result resolution; retry current results"
        );
        // A published cut above the durable head is a fabricated window or a
        // truncated stream, and refuses on the aggregate class too.
        let beyond = vec![crate::ExpectedStreamRevision {
            stream_id: session.into(),
            expected_revision: 9_999,
        }];
        assert!(
            super::verify_producer_metadata_cuts(&store, beyond.iter())
                .expect_err("a cut above the head must refuse")
                .contains("producer metadata changed during result resolution")
        );
    }

    /// 030 T7 v4 (option 甲): the same fence on a quiet stream, on a published cut
    /// that sits below the head, and on a published cut above the head.
    ///
    /// The middle case is the one this control first got backwards: a Session
    /// stream published at cut 0 whose head has since advanced to 1 is the same
    /// durable fact as "the aggregate grew above the published cut", which the
    /// sibling busy-Session control asserts must be admitted. A published cut
    /// above the durable head is the shape that cannot be a busy stream - a
    /// fabricated window or a truncated log - and that one still refuses with the
    /// untouched sentence. Nothing here re-pins: admitting growth must leave both
    /// the published value and the pinned cut exactly where they were.
    #[test]
    fn t7_the_producer_metadata_fence_passes_by_equality_and_treats_a_lower_published_cut_as_growth() {
        let store = crate::RuntimeEventStore::for_test();
        let quiet = "session:t7-v3-quiet";
        let mut cuts = std::collections::BTreeMap::new();
        store
            .append(crate::RuntimeEventInput {
                stream_id: quiet.into(),
                scope: crate::RuntimeEventScope::Session,
                kind: "test.t7-append".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({}),
            })
            .unwrap();
        super::results::result_source_events(&store, quiet, &mut cuts).unwrap();
        let published = vec![crate::ExpectedStreamRevision {
            stream_id: quiet.into(),
            expected_revision: cuts[quiet],
        }];
        super::verify_producer_metadata_cuts(&store, published.iter())
            .expect("an unchanged head satisfies the fence by equality");
        assert_eq!(cuts[quiet], 1);
        let below = vec![crate::ExpectedStreamRevision {
            stream_id: quiet.into(),
            expected_revision: 0,
        }];
        super::verify_producer_metadata_cuts(&store, below.iter())
            .expect("a head above a published Session cut is growth, not a replaced stream");
        assert_eq!(
            below[0].expected_revision, 0,
            "admitting growth never chases the head: the published value stays"
        );
        assert_eq!(cuts[quiet], 1, "admitting growth never re-pins");
        let beyond = vec![crate::ExpectedStreamRevision {
            stream_id: quiet.into(),
            expected_revision: 9_999,
        }];
        assert_eq!(
            super::verify_producer_metadata_cuts(&store, beyond.iter())
                .expect_err("a published cut above the durable head cannot be a quiet stream"),
            "producer metadata changed during result resolution; retry current results"
        );
    }

    /// A2f-P2: the paging path reads the same predicate, so the graph streams
    /// it pages are admissible there too. A delegated read reaches the
    /// predicate in its run-id form, because the reviewer pins the
    /// un-normalized ticket graph id while the store keys on the graph id.
    #[test]
    fn a2f_the_paging_path_reads_the_same_source_classes() {
        for source in [
            "agentic-graph:abc",
            "run:agentic-graph:abc",
            "session-ingress-graph:abc",
            "run:session-ingress-graph:abc",
            "session:abc",
            "execution-lineage:abc",
        ] {
            assert!(
                super::repin_accepts_moved_head(source, 4, 5),
                "{source} is a shared aggregate stream"
            );
        }
        // A head that did not grow is not a growth, and a source that is not a
        // shared aggregate stream is the object under review moving.
        assert!(!super::repin_accepts_moved_head("agentic-graph:abc", 5, 5));
        assert!(!super::repin_accepts_moved_head("agentic-graph:abc", 5, 4));
        for source in [
            "agentic-program:abc",
            "goal:abc",
            "execution-effect:abc",
            "execution-agent-receipts:abc:node",
        ] {
            assert!(
                !super::repin_accepts_moved_head(source, 4, 5),
                "{source} is not a shared aggregate stream"
            );
        }
    }

    /// G-d-N2: G-d does not widen which tools have to provide physical
    /// material.
    #[test]
    fn g_d_the_physical_read_coverage_is_unchanged() {
        assert!(counts_as_physical_read("read_ref"));
        assert!(counts_as_physical_read("evidence_retrieve"));
        assert!(!counts_as_physical_read("read"));
        assert!(!counts_as_physical_read("read_file"));
        assert!(!counts_as_physical_read("grep_search"));
        assert!(!counts_as_physical_read("state_inspect"));
    }

    /// A read-acquisition seal, in the shape the product writes it.
    fn a2_gd_seal() -> serde_json::Value {
        let step: harness_contract::step::StepIdentity = serde_json::from_value(
            serde_json::json!({
                "step_id": "read-step",
                "session_id": "read-session",
                "turn_id": "read-turn",
                "execution_id": "agentic-graph:read",
                "actor_ref": "read-agent",
                "run_ref": "read-run",
                "attempt": 1,
                "build_ref": "read-fixture",
            }),
        )
        .expect("step identity");
        let seal = crate::context_evidence::ReadAcquisitionSeal {
            schema_version: 1,
            primary_stream: "read-primary".into(),
            primary_key: "read-key:read-receipt".into(),
            index_stream: "execution-agent-receipts:agentic-graph:read:node:1".into(),
            index_key: "agent-tool-receipt:read-key".into(),
            session_id: "read-session".into(),
            invocation_id: "read-call".into(),
            tool_name: "read_file".into(),
            graph_id: "agentic-graph:read".into(),
            node_id: "node".into(),
            attempt: 1,
            step,
            origin: serde_json::json!({
                "origin_step_id": "read-step",
                "call_id": "read-call",
                "catalog_digest": "fixed",
                "source": {"kind": "provider", "attempt_id": "read-attempt"},
            }),
            input_sha256: "read-input".into(),
            body: crate::context_evidence::ReadBodySeal {
                schema_version: 1,
                source_ref: "workspace/read.txt".into(),
                source_sha256: "sha256:read-source".into(),
                source_bytes: 20,
                output_sha256: "sha256:read-output".into(),
                output_bytes: 4,
                extent: crate::context_evidence::ReadExtent::FileLines {
                    start_line: 2,
                    line_count: 1,
                    total_lines: 4,
                    ends_with_newline: true,
                    body_sha256: "sha256:read-output".into(),
                    body_bytes: 4,
                    source_omitted: true,
                },
            },
        };
        serde_json::to_value(seal).expect("seal value")
    }

    /// G-d-P1 and G-d-N1: a read carries a seal only when the seal's value is
    /// one. The truncated receipt of the live run has the key with a json null
    /// value, and that is no seal.
    #[test]
    fn g_d_a_read_carries_a_seal_only_when_its_value_is_one() {
        let sealed = serde_json::json!({
            "read_acquisition_v1": a2_gd_seal(),
        });
        assert!(read_carries_acquisition_seal(&sealed));
        let null_value =
            serde_json::json!({"read_acquisition_v1": serde_json::Value::Null});
        assert!(!read_carries_acquisition_seal(&null_value));
        let absent = serde_json::json!({"other": 1});
        assert!(!read_carries_acquisition_seal(&absent));
        let another_type = serde_json::json!({"read_acquisition_v1": "not a seal"});
        assert!(!read_carries_acquisition_seal(&another_type));
        let not_a_seal = serde_json::json!({"read_acquisition_v1": {"schema_version": 1}});
        assert!(!read_carries_acquisition_seal(&not_a_seal));
    }

    /// G-d-N1: the auxiliary read refusal keeps its own wording. G-d widens
    /// who has a material; it does not rewrite what a read without one says.
    #[test]
    fn g_d_the_auxiliary_read_refusal_text_is_unchanged() {
        let source = include_str!("review_evidence.rs");
        assert!(
            source.contains(
                "\"proof auxiliary read has no authenticated immutable or physical material\""
            ),
            "the auxiliary read refusal text must not change"
        );
    }

    #[test]
    fn a_refusal_lists_the_result_refs_runtime_currently_holds() {
        // V27-B1: a model that had just named three real receipts was told its
        // evidence did not exist. It must be told what IS current.
        let with_refs = no_current_results_error(&[
            "artifact://1111".to_string(),
            "artifact://2222".to_string(),
        ]);
        assert!(with_refs.contains("artifact://1111"), "{with_refs}");
        assert!(with_refs.contains("artifact://2222"), "{with_refs}");
        assert!(with_refs.contains("evidence_retrieve"), "{with_refs}");
        let empty = no_current_results_error(&[]);
        assert!(empty.contains("publish and accept a Task first"), "{empty}");
        assert!(!empty.contains("artifact://"), "{empty}");
    }

    fn coverage() -> ReadCoverage {
        ReadCoverage {
            proof: ObjectiveResultReadProof {
                result_kind: harness_contract::goal::GoalResultKind::Artifact,
                effect_observation_refs: vec![],
                result_ref: "result:1".into(),
                content_ref: "artifact://current".into(),
                sha256: "current-hash".into(),
                bytes: 3001,
                receipt_refs: vec![],
            },
            total: None,
            chunks: BTreeSet::new(),
        }
    }

    fn page(index: usize) -> serde_json::Value {
        serde_json::json!({"kind":"evidence_retrieve", "available":true,
            "evidence_ref":"artifact://current", "sha256":"current-hash", "bytes":3001,
            "total_chunks":3, "chunks":[{"index":index,"content":"actual page"}]})
    }

    /// A Task with two delivered artifacts and one active Review attempt at
    /// generation 1 bound to `graph` / `agent:1` / `membership:1`.
    fn reviewed_task() -> crate::agentic::program::AgenticTaskProjection {
        let mut task = crate::agentic::program::AgenticTaskProjection {
            obligations: Vec::new(),
            rework_directives: Vec::new(),
            unsettled_released_attempts: Vec::new(),
            terminal_note: None,
            task_id: "task".to_string(),
            team_id: "team".to_string(),
            title: "long task".to_string(),
            objective: "finish without an arbitrary wall timeout".to_string(),
            acceptance: "durable evidence".to_string(),
            acceptance_checks: Vec::new(),
            criterion_source: Default::default(),
            required_capabilities: Vec::new(),
            depends_on: Vec::new(),
            status: crate::agentic::program::AgenticTaskStatus::Published,
            claimant: None,
            claim_generation: 0,
            claim_execution_id: None,
            claimed_at_ms: None,
            lease_expires_at_ms: None,
            claim_renewal_streak: 0,
            active_attempts: Default::default(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            review_reason: None,
            reviewed_by: None,
            failed_attempts: 0,
            review_generation: 0,
            failed_review_attempts: 0,
            structural_execution_stalls: 0,
            structural_review_stalls: 0,
            reviews_applied: 0,
            review_obligation: None,
            reviews_accepted: 0,
            last_failure: None,
            replacement_task_refs: Vec::new(),
            supersede_evidence_refs: Vec::new(),
            superseded_reason: None,
            superseded_by: None,
            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
            cancel_requested_by: None,
            cancel_reason_ref: None,
            cancel_evidence_refs: Vec::new(),
            pending_retirement: None,
        };
        task.review_generation = 1;
        task.artifact_refs = vec!["artifact:a".to_string(), "artifact:b".to_string()];
        task.active_attempts.insert(
            "graph".to_string(),
            crate::agentic::program::AgenticTaskAttemptProjection {
                execution_id: "graph".to_string(),
                agent_id: "agent:1".to_string(),
                membership_id: "membership:1".to_string(),
                mode: harness_contract::agent_action::AgentAttemptMode::Review,
                generation: 1,
            },
        );
        task
    }

    #[test]
    fn an_objective_review_may_cite_results_across_tasks() {
        let task = reviewed_task();
        // The fix this pins: the single-Task artifact rule used to apply to every
        // review, so an objective review of real multi-Task work was refused with
        // "stale or owns different results" while generation, attempt and reads
        // were all correct. A criterion-scoped review legitimately cites the
        // delivered results of several Tasks.
        assert!(
            task_review_attempt_rejection(
                &task,
                "graph",
                2,
                2,
                "agent:1",
                "membership:1",
                &["artifact:a".to_string(), "artifact:z".to_string()],
                Some("user_intent"),
            )
            .is_ok(),
            "an Objective review is criterion-scoped, not Task-scoped"
        );
        // The same refs under a Task review are still refused, with a reason that
        // names the rule instead of collapsing three conditions into one sentence.
        let refusal = task_review_attempt_rejection(
            &task,
            "graph",
            2,
            2, // independently authenticated physical ticket
            "agent:1",
            "membership:1",
            &["artifact:a".to_string(), "artifact:z".to_string()],
            None,
        )
        .expect_err("a Task review covers only its own Task's artifacts");
        assert!(
            refusal.contains("covers only that Task's"),
            "the refusal must name the rule: {refusal}"
        );
        // The rule is not enough. Runtime reads `task.artifact_refs` in the same
        // condition that produced this refusal, so it already knows the legal set;
        // run one28 was told the rule eighteen times and guessed eighteen times.
        // The refusal has to hand over the answer.
        assert!(
            refusal.contains("artifact:a"),
            "the refusal must name this Task's legal artifact refs: {refusal}"
        );
        assert!(
            refusal.contains("\"evidence_refs\""),
            "the refusal must show a conforming input shape: {refusal}"
        );
    }

    #[test]
    fn a_stale_review_attempt_is_refused_with_its_own_reason() {
        let task = reviewed_task();
        let stale_generation = task_review_attempt_rejection(
            &task,
            "graph",
            5,
            2, // independently authenticated physical ticket
            "agent:1",
            "membership:1",
            &["artifact:a".to_string()],
            None,
        )
        .expect_err("generation 5 cannot review a Task at review generation 1");
        assert!(
            stale_generation.contains("physical node attempt is stale"),
            "the refusal must name the generation: {stale_generation}"
        );

        let mut old_owner = task.clone();
        old_owner.active_attempts.get_mut("graph").unwrap().generation = 0;
        assert!(task_review_attempt_rejection(
            &old_owner, "graph", 2, 2, "agent:1", "membership:1",
            &["artifact:a".to_string()], None,
        ).is_err(), "old business generation is not a physical attempt");

        let wrong_agent = task_review_attempt_rejection(
            &task,
            "graph",
            2,
            2, // independently authenticated physical ticket
            "agent:9",
            "membership:1",
            &["artifact:a".to_string()],
            None,
        )
        .expect_err("another agent's attempt is not this attempt");
        assert!(
            wrong_agent.contains("not the active Review attempt"),
            "the refusal must name the attempt: {wrong_agent}"
        );

        assert!(task_review_attempt_rejection(
            &task,
            "graph",
            2,
            2, // independently authenticated physical ticket
            "agent:1",
            "membership:1",
            &["artifact:a".to_string()],
            None,
        )
        .is_ok());
    }

    #[test]
    fn a_read_of_the_logical_key_counts_as_a_read_of_the_result() {
        // Runtime records a one-shot Task deliverable as `artifact:<24 hex>` and
        // repeats that spelling in the completion gap and the citable-refs
        // projection, while `read_ref` resolves and echoes it back as
        // `artifact://<24 hex>`. A byte comparison never matched, so a reviewer
        // that read exactly the results it was told to read was refused.
        assert!(same_reference_spelling(
            "artifact://4541b02b47dc2bca34a7f0f7",
            "artifact:4541b02b47dc2bca34a7f0f7"
        ));
        assert!(same_reference_spelling(
            "artifact:4541b02b47dc2bca34a7f0f7",
            "artifact://4541b02b47dc2bca34a7f0f7"
        ));
        assert!(same_reference_spelling(
            "tool://tool-raw-chatcmpl-tool-abc",
            "tool:tool-raw-chatcmpl-tool-abc"
        ));
        // Different objects stay different: the normalisation must not become a
        // wildcard that lets any read satisfy any result.
        assert!(!same_reference_spelling(
            "artifact://4541b02b47dc2bca34a7f0f7",
            "artifact:64506e38bc17f2a2ae97c06e"
        ));
        assert!(!same_reference_spelling(
            "tool://tool-raw-chatcmpl-tool-abc",
            "artifact://art_2387b74c32b348d9b3d2b17e69f8718a"
        ));
        // An exact match still passes, and a trailing slash is not a difference.
        assert!(same_reference_spelling("artifact://x", "artifact://x"));
        assert!(same_reference_spelling("artifact://x/", "artifact://x"));
    }

    #[test]
    fn the_roots_own_reads_count_whatever_graph_carried_them() {
        // Runtime attributes the root's tool calls to whichever agentic-graph run
        // is executing the root's step. One live run's 125 root tool calls were
        // spread over eight different graphs, interleaved in time, so a filter
        // keyed on the run id discarded every one of them and the root could
        // never satisfy its own read coverage.
        let root = "session-ingress-graph:root-1";
        let binding = |activity_id: &str| harness_contract::projection::RuntimeActivityBinding {
            activity_id: activity_id.to_string(),
            parent_activity_id: None,
            initiator_activity_id: None,
            agent_run_id: Some("run:agentic-graph:whatever".to_string()),
            agent_instance_id: None,
            root_execution_id: root.to_string(),
            root_task_id: "task:root:1".to_string(),
            task_id: "task:1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            node_id: None,
            team_run_id: None,
            skill_id: None,
            skill_revision: None,
            skill_activation_id: None,
            tool_contract_id: None,
            tool_call_id: None,
            approval_id: None,
            parallel_group_id: None,
            revision: 1,
            fence: 1,
            generation: 1,
        };
        for graph in ["aaa", "bbb", "ccc"] {
            assert!(
                binding_is_owned_by_root(
                    &binding(&format!(
                        "activity:execution:{root}:owner:run:agentic-graph:{graph}:tool:x"
                    )),
                    Some(root)
                ),
                "the root's own read counts under any graph: {graph}"
            );
        }
        // A delegated Agent's read is not owned by the root execution and must
        // stay invisible: independence is about who read it.
        assert!(
            !binding_is_owned_by_root(
                &binding("activity:execution:agentic-graph:delegated:node:agent"),
                Some(root)
            ),
            "another Agent's read must not count for the root"
        );
        // An actor with no root execution has nothing to match against.
        assert!(!binding_is_owned_by_root(
            &binding(&format!(
                "activity:execution:{root}:owner:run:agentic-graph:aaa"
            )),
            None
        ));
    }

    #[test]
    fn the_facade_the_primer_advertises_counts_as_a_physical_read() {
        // The primer names `read_ref` as THE canonical reader. It used to be
        // absent from this predicate (the list held `"read"`, which is not a
        // tool name), so a reviewer that followed the primer and then reviewed
        // was told its reads did not count. One live run cycled
        // `read_ref` -> `objective_review` -> `review_requires_current_physical_reads`
        // until the window expired; the identical read through
        // `evidence_retrieve` closed the coverage.
        assert!(counts_as_physical_read("read_ref"));
        assert!(counts_as_physical_read("evidence_retrieve"));
        // A capability label is not a tool name and must never satisfy the gate.
        assert!(!counts_as_physical_read("read"));
        // Ordinary workspace reads are not reads of a delivered result.
        assert!(!counts_as_physical_read("read_file"));
        assert!(!counts_as_physical_read("grep_search"));
        assert!(!counts_as_physical_read("state_inspect"));
    }

    #[test]
    fn historical_review_proofs_default_to_independent_verification() {
        let value = serde_json::json!({"goal_spec_revision":1,"goal_spec_digest":"spec",
            "work_manifest_digest":"work","reviewer_execution_id":"reviewer",
            "producer_execution_ids":["producer"],"reads":[{"result_ref":"result",
            "content_ref":"artifact://content","sha256":"hash","bytes":1,"receipt_refs":["read"]}]});
        let proof: ObjectiveReviewVerification = serde_json::from_value(value).unwrap();
        assert!(proof.independence_required);
        assert!(proof.review_policy_digest.is_none());
        assert_eq!(
            proof.reads[0].result_kind,
            harness_contract::goal::GoalResultKind::Artifact
        );
    }

    #[test]
    fn effect_observations_require_later_reads_of_every_actual_target() {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        let scope = |target: &str| {
            let mut scope =
                PermissionScope::new(PermissionResource::File, PermissionOperation::Read);
            scope.target = Some(target.into());
            scope
        };
        let results = vec![results::ResolvedReviewResult {
            origin: results::ResultOrigin::Strong,
            result_sources: vec![],
            reference: "result:1".into(),
            content: harness_contract::context::ArtifactRef::durable(
                "artifact://content",
                "hash",
                1,
                "text/plain",
                "public",
            ),
            kind: harness_contract::goal::GoalResultKind::ToolEffect,
            artifact_kinds: BTreeSet::new(),
            producers: vec![],
            effect_cursor: Some(10),
            effect_sources: Vec::new(),
            effect_scopes: vec![scope("a"), scope("b")],
            effect_targets: ["a", "b"]
                .into_iter()
                .map(|target| harness_contract::goal::ProofEffectTarget {
                    path: serde_json::from_value(serde_json::json!({
                        "workspace_id":"workspace", "repository_id":"repository",
                        "workspace_relative_path":target, "repository_relative_path":target,
                        "object_kind":"file"
                    }))
                    .unwrap(),
                    declared_scope: scope(target),
                    access_mode: harness_contract::context::WorkspaceAccessMode::Write,
                    coverage: harness_contract::context::EvidenceCoverageKind::WriteEffect,
                    exact: true,
                    physical_identity: Some(format!("test:{target}")),
                    alias_checked: true,
                })
                .collect(),
            unscoped_effect: false,
            primary_inputs: Vec::new(),
            input_boundaries: Vec::new(),
            process_runs: Vec::new(),
        }];
        let file = |target: &str, cursor, references: &[&str]| policy::DeliveredFileRead {
            path: serde_json::from_value(serde_json::json!({
                "workspace_id":"workspace", "repository_id":"repository",
                "workspace_relative_path":target, "repository_relative_path":target,
                "object_kind":"file"
            }))
            .unwrap(),
            scope: scope(target),
            cursor,
            receipt_refs: references
                .iter()
                .map(|reference| reference.to_string())
                .collect(),
        };
        let result = &results[0];
        let mut files = vec![
            file("a", 10, &["same-commit"]),
            file("other", 11, &["wrong-target"]),
        ];
        assert!(effect_observation_refs(result, &files, None).is_err());
        let mut foreign = file("a", 12, &["foreign-workspace"]);
        foreign.path.workspace_id = "another-workspace".into();
        files.push(foreign);
        assert!(effect_observation_refs(result, &files, None).is_err());
        files.push(file("a", 12, &["read-a"]));
        assert!(effect_observation_refs(result, &files, None).is_err());
        files.push(file("b", 13, &["read-b-page-0", "read-b-page-1"]));
        let expected = vec![
            "read-a".into(),
            "read-b-page-0".into(),
            "read-b-page-1".into(),
        ];
        assert_eq!(
            effect_observation_refs(result, &files, None).unwrap(),
            expected
        );
        assert!(effect_observation_refs(result, &files, Some(&expected)).is_ok());
        assert!(effect_observation_refs(
            result,
            &files,
            Some(&["read-a".into(), "read-b-page-0".into()])
        )
        .is_err());
        assert!(effect_observation_refs(
            result,
            &files,
            Some(&["read-a".into(), "wrong-target".into()])
        )
        .is_err());
        assert!(effect_observation_refs(
            result,
            &files,
            Some(&[
                "same-commit".into(),
                "read-b-page-0".into(),
                "read-b-page-1".into()
            ])
        )
        .is_err());
    }

    #[test]
    fn current_hash_pages_must_cover_every_chunk() {
        let mut read = coverage();
        read.observe(&page(2).to_string(), "receipt:2");
        read.observe(&page(0).to_string(), "receipt:0");
        read.observe(&page(0).to_string(), "receipt:duplicate");
        assert!(!read.complete());
        read.observe(&page(1).to_string(), "receipt:1");
        assert!(read.complete());
    }

    #[test]
    fn full_size_read_page_has_bounded_durable_coverage_without_truncation() {
        let mut page = page(0);
        page["total_chunks"] = 16.into();
        page["chunks"] = (0..16)
            .map(|index| serde_json::json!({"index":index,"content":"文".repeat(1500)}))
            .collect();
        let original = page.to_string();
        assert!(original.chars().count() > 16 * 1024);
        let compact = compact_read_receipt(&original);
        assert!(compact.len() < 4096);
        let mut read = coverage();
        read.observe(&compact, "receipt:large-page");
        assert!(read.complete());
        assert!(!compact.contains(&"文".repeat(100)));
    }

    #[test]
    fn metadata_denials_old_hashes_and_inconsistent_pages_are_not_reads() {
        for field in [
            "sha256",
            "evidence_ref",
            "bytes",
            "available",
            "kind",
            "chunks",
        ] {
            let mut read = coverage();
            let mut output = page(0);
            output[field] = serde_json::Value::Null;
            read.observe(&output.to_string(), "receipt:bad");
            assert!(read.chunks.is_empty(), "{field}");
            assert!(!read.complete());
        }
        let mut read = coverage();
        read.observe(&page(0).to_string(), "");
        assert!(read.chunks.is_empty());
        read.observe(&page(0).to_string(), "receipt:0");
        let mut inconsistent = page(1);
        inconsistent["total_chunks"] = 2.into();
        read.observe(&inconsistent.to_string(), "receipt:bad-total");
        assert_eq!(read.chunks, BTreeSet::from([0]));
        let mut invalid = page(3);
        invalid["chunks"][0]["content"] = "".into();
        read.observe(&invalid.to_string(), "receipt:empty");
        assert!(!read.complete());
    }

    /// The origin of the 158-refusal run: the review reported "packet missing" for a packet
    /// that was present, because the caller named the physical run id while the packet stored
    /// the bare graph id.
    #[test]
    fn the_identity_check_uses_the_namespace_the_caller_named() {
        let graph = "agentic-graph:bf5d8374a4c7";
        let run = "run:agentic-graph:bf5d8374a4c7";

        // The run-bound binding (`model_loop.rs:470`) names the run id.
        assert!(packet_identity_matches(graph, run, run));
        // The graph-bound binding (`model_loop.rs:196/367`) names the graph id.
        assert!(packet_identity_matches(graph, run, graph));

        // Normalising the namespace must not relax the identity check: a packet belonging to a
        // different graph still never matches, in either namespace. A packet's run id and graph
        // id name the same graph, so a different graph means both differ.
        assert!(!packet_identity_matches(
            "agentic-graph:other",
            "run:agentic-graph:other",
            run
        ));
        assert!(!packet_identity_matches(
            "agentic-graph:other",
            "run:agentic-graph:other",
            graph
        ));
        assert!(!packet_identity_matches(
            graph,
            run,
            "run:agentic-graph:bf5d8374a4c8"
        ));
        assert!(!packet_identity_matches(
            graph,
            run,
            "agentic-graph:bf5d8374a4c8"
        ));
    }

    /// A model that has just accepted three Tasks naturally names the three
    /// refs it was handed. Those are Tasks, not results, so the coverage map came
    /// back empty and the review was refused with "independent review has no
    /// current results" while the work was delivered and accepted. The Task's
    /// artifacts are exactly its delivered results, so the boundary resolves it.
    #[test]
    fn a_task_ref_resolves_to_the_tasks_delivered_artifacts() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        for (task_id, artifact) in [("task:a", "artifact:a"), ("task:b", "artifact:b")] {
            program.tasks.insert(
                task_id.to_string(),
                crate::AgenticTaskProjection {
                    obligations: Vec::new(),
                    rework_directives: Vec::new(),
                    unsettled_released_attempts: Vec::new(),
                    terminal_note: None,
                    task_id: task_id.to_string(),
                    team_id: "team:1".to_string(),
                    title: "work".to_string(),
                    objective: "objective".to_string(),
                    acceptance: "evidence".to_string(),
                    acceptance_checks: Vec::new(),
                    criterion_source: Default::default(),
                    required_capabilities: Vec::new(),
                    depends_on: Vec::new(),
                    status: crate::AgenticTaskStatus::Accepted,
                    claimant: Some("agent:worker".to_string()),
                    claim_generation: 1,
                    claim_execution_id: None,
                    claimed_at_ms: None,
                    lease_expires_at_ms: None,
                    claim_renewal_streak: 0,
                    active_attempts: Default::default(),
                    artifact_refs: vec![artifact.to_string()],
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    review_reason: None,
                    reviewed_by: None,
                    failed_attempts: 0,
                    review_generation: 1,
                    failed_review_attempts: 0,
                    structural_execution_stalls: 0,
                    structural_review_stalls: 0,
                    reviews_applied: 0,
                    review_obligation: None,
                    reviews_accepted: 0,
                    last_failure: None,
                    replacement_task_refs: Vec::new(),
                    supersede_evidence_refs: Vec::new(),
                    superseded_reason: None,
                    superseded_by: None,
                    obligation_refs: Vec::new(),
                    purpose: Default::default(),
                    execution_requirements: Vec::new(),
                    expertise_hints: Vec::new(),
                    cancel_requested_by: None,
                    cancel_reason_ref: None,
                    cancel_evidence_refs: Vec::new(),
                    pending_retirement: None,
                },
            );
        }
        assert_eq!(
            resolve_result_refs(&program, &["task:a".to_string(), "task:b".to_string()]),
            vec!["artifact:a".to_string(), "artifact:b".to_string()]
        );
    }

    /// Artifact refs are already the right shape and pass through untouched, so
    /// the coercion cannot invent or drop a result.
    #[test]
    fn artifact_refs_pass_through_unchanged() {
        let program = crate::AgenticProgramProjection::empty("program", "objective");
        assert_eq!(
            resolve_result_refs(
                &program,
                &["artifact://art_1".to_string(), "tool://receipt".to_string()]
            ),
            vec!["artifact://art_1".to_string(), "tool://receipt".to_string()]
        );
    }

    /// A Task with no artifacts contributes nothing rather than an invented ref.
    #[test]
    fn a_task_with_no_artifacts_contributes_nothing() {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.tasks.insert(
            "task:empty".to_string(),
            crate::AgenticTaskProjection {
                obligations: Vec::new(),
                rework_directives: Vec::new(),
                unsettled_released_attempts: Vec::new(),
                terminal_note: None,
                claim_renewal_streak: 0,
                task_id: "task:empty".to_string(),
                team_id: "team:1".to_string(),
                title: "work".to_string(),
                objective: "objective".to_string(),
                acceptance: "evidence".to_string(),
                acceptance_checks: Vec::new(),
                criterion_source: Default::default(),
                required_capabilities: Vec::new(),
                depends_on: Vec::new(),
                status: crate::AgenticTaskStatus::Published,
                claimant: None,
                claim_generation: 0,
                claim_execution_id: None,
                claimed_at_ms: None,
                lease_expires_at_ms: None,
                active_attempts: Default::default(),
                artifact_refs: Vec::new(),
                evidence_refs: Vec::new(),
                unresolved: Vec::new(),
                review_reason: None,
                reviewed_by: None,
                failed_attempts: 0,
                review_generation: 1,
                failed_review_attempts: 0,
                structural_execution_stalls: 0,
                structural_review_stalls: 0,
                reviews_applied: 0,
                review_obligation: None,
                reviews_accepted: 0,
                last_failure: None,
                replacement_task_refs: Vec::new(),
                supersede_evidence_refs: Vec::new(),
                superseded_reason: None,
                superseded_by: None,
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
                cancel_requested_by: None,
                cancel_reason_ref: None,
                cancel_evidence_refs: Vec::new(),
                pending_retirement: None,
            },
        );
        assert!(resolve_result_refs(&program, &["task:empty".to_string()]).is_empty());
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let mut sources = BTreeMap::new();
        program.artifacts = canonical_alias_program_565().artifacts;
        let task = program.tasks.get_mut("task:empty").unwrap();
        task.status = crate::AgenticTaskStatus::Submitted;
        task.artifact_refs = vec!["artifact://content-one".into()];
        let original = program.clone();
        let (tasks, artifacts, _) = selected_work(
            &store, &program, None, &["task:empty".into()], &mut sources,
        ).unwrap();
        assert_eq!(tasks, vec!["task:empty".to_string()]);
        assert_eq!(artifacts, vec!["artifact:one".to_string()]);
        let (tasks, artifacts, _) = selected_work(
            &store, &program, None, &["artifact://content-one".into()], &mut sources,
        ).unwrap();
        assert!(tasks.is_empty());
        assert!(artifacts.is_empty());
        let business = canonical_review_result_refs(
            &program, &["artifact://content-one".into()],
        ).unwrap().into_iter().collect::<Vec<_>>();
        let (tasks, artifacts, _) = selected_work(
            &store, &program, None, &business, &mut sources,
        ).unwrap();
        assert_eq!(tasks, vec!["task:empty".to_string()]);
        assert_eq!(artifacts, vec!["artifact:one".to_string()]);
        assert_eq!(program, original);
        let mut second = program.artifacts["artifact:one"].clone();
        second.artifact_ref = "artifact:two".into();
        program.artifacts.insert(second.artifact_ref.clone(), second);
        assert!(selected_work(
            &store, &program, None, &["task:empty".into()], &mut sources,
        ).unwrap_err().contains("ambiguous"));
        let (tasks, artifacts, _) = selected_work(
            &store, &program, None, &["artifact://content-one".into()], &mut sources,
        ).unwrap();
        assert!(tasks.is_empty());
        assert!(artifacts.is_empty());
        program.tasks.get_mut("task:empty").unwrap().artifact_refs = vec!["artifact:one".into()];
        let (tasks, artifacts, _) = selected_work(
            &store, &program, None, &["task:empty".into()], &mut sources,
        ).unwrap();
        assert_eq!(tasks, vec!["task:empty".to_string()]);
        assert_eq!(artifacts, vec!["artifact:one".to_string()]);
    }

    fn canonical_alias_program_565() -> crate::AgenticProgramProjection {
        let mut program = crate::AgenticProgramProjection::empty("program", "objective");
        program.artifacts.insert(
            "artifact:one".into(),
            crate::agentic::program::AgenticArtifactProjection {
                artifact_ref: "artifact:one".into(),
                content_ref: "artifact://content-one".into(),
                kind: "report".into(),
                title: "original".into(),
                relates_to: Vec::new(),
                committed_by: "producer".into(),
                claim_execution_id: None,
                claim_generation: None,
            },
        );
        program
    }

    #[test]
    fn r1_565_review_aliases_share_one_coverage_without_mutating_original_citations() {
        let program = canonical_alias_program_565();
        let references = vec![
            "artifact:one".into(),
            "artifact://one".into(),
            "artifact://content-one".into(),
            "artifact:one".into(),
        ];
        let original = references.clone();
        assert_eq!(
            canonical_review_result_refs(&program, &references).unwrap(),
            BTreeSet::from(["artifact:one".into()])
        );
        assert_eq!(references, original);
    }

    #[test]
    fn r1_565_review_identity_keeps_unrelated_tool_and_content_results() {
        let program = canonical_alias_program_565();
        let references = vec![
            "artifact:one".into(),
            "artifact://content-one".into(),
            "tool://original-receipt".into(),
            "artifact://other-content".into(),
        ];
        assert_eq!(
            canonical_review_result_refs(&program, &references).unwrap(),
            BTreeSet::from([
                "artifact:one".into(),
                "tool://original-receipt".into(),
                "artifact://other-content".into(),
            ])
        );
    }

    #[test]
    fn r1_565_review_identity_rejects_ambiguous_content_and_logical_spelling() {
        let program = canonical_alias_program_565();
        for (reference, artifact_ref) in [
            ("artifact://content-one", "artifact:two"),
            ("artifact://one", "artifact://one"),
        ] {
            let mut ambiguous = program.clone();
            let mut artifact = ambiguous.artifacts["artifact:one"].clone();
            artifact.artifact_ref = artifact_ref.into();
            ambiguous.artifacts.insert(artifact_ref.into(), artifact);
            assert!(
                canonical_review_result_refs(&ambiguous, &[reference.into()])
                    .unwrap_err()
                    .contains("ambiguous")
            );
            if artifact_ref == "artifact:two" {
                assert_eq!(
                    review_result_identity(&ambiguous, "artifact:one").unwrap(),
                    "artifact:one"
                );
            } else {
                assert!(review_result_identity(&ambiguous, "artifact:one")
                    .unwrap_err()
                    .contains("ambiguous"));
            }
        }
    }

    #[test]
    fn r1_566_unverified_task_relation_rejects_before_and_after_task_visit() {
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let mut program = canonical_alias_program_565();
        let task: crate::AgenticTaskProjection = serde_json::from_value(serde_json::json!({
            "task_id": "task:unproven", "team_id": "team:one", "title": "unproven",
            "objective": "unproven", "acceptance": "original independent evidence",
            "required_capabilities": [], "depends_on": [], "status": "published",
            "claim_generation": 0, "artifact_refs": [], "evidence_refs": [],
            "unresolved": [], "failed_attempts": 0, "review_generation": 1,
            "failed_review_attempts": 0
        }))
        .unwrap();
        program.tasks.insert(task.task_id.clone(), task);
        program
            .artifacts
            .get_mut("artifact:one")
            .unwrap()
            .relates_to = vec!["task:unproven".into()];
        let original = program.clone();
        for references in [
            vec!["artifact:one".into()],
            vec!["task:unproven".into(), "artifact:one".into()],
        ] {
            let mut cuts = BTreeMap::new();
            assert!(selected_work(&store, &program, None, &references, &mut cuts).is_err());
            assert_eq!(program, original);
            assert_eq!(
                program.tasks["task:unproven"].status,
                crate::AgenticTaskStatus::Published
            );
            assert!(program.tasks["task:unproven"].artifact_refs.is_empty());
        }
    }
    include!("review_evidence/work_contract_tests.rs");
}

#[cfg(test)]
pub(super) mod t1_frame_cleanup_tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::{Event, Metadata, Subscriber, span::{Attributes, Id, Record}};

    #[derive(Clone, Default)]
    pub(super) struct EventCapture(pub(super) Arc<Mutex<Vec<String>>>);
    struct Fields(String);
    impl tracing::field::Visit for Fields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{}={value:?};", field.name()));
        }
    }
    impl Subscriber for EventCapture {
        fn enabled(&self, _: &Metadata<'_>) -> bool { true }
        fn new_span(&self, _: &Attributes<'_>) -> Id { Id::from_u64(1) }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    #[test]
    fn t1_cleanup_does_not_reverse_original_artifact_adoption() {
        use crate::ArtifactMetadataRepository as _;
        let temp = tempfile::tempdir().unwrap();
        let repo = Arc::new(crate::EphemeralArtifactRepository::default());
        let store = crate::ArtifactStore::new(temp.path(), repo.clone(), crate::ArtifactStoreConfig::default()).unwrap();
        let origin = crate::PreparedArtifactOrigin::EventBody {
            stream_id: "graph:t1".into(), idempotency_key: "original".into(), kind: "execution_graph.planned".into(),
        };
        let artifact = store.write_pre_pinned_blocking("session:t1", "original", b"body", &origin).unwrap();
        let id = artifact.selector.strip_prefix("artifact://").unwrap();
        assert_eq!(repo.record(id).unwrap().unwrap().record_class, crate::ArtifactRecordClass::PrivatePrepared);
        let frame = ProcessInputFrame::without_process_verifier();
        let owner = frame.artifact_owner.clone();
        frame.retain_artifact(&store, &artifact).unwrap();
        assert_eq!(repo.record(id).unwrap().unwrap().record_class, crate::ArtifactRecordClass::Live);
        let captured = EventCapture::default();
        let events = Arc::clone(&captured.0);
        tracing::subscriber::with_default(captured, || drop(frame));
        assert!(!repo.fixture_review_exact_pin(id, &owner));
        assert!(repo.fixture_review_exact_pin(id, crate::ARTIFACT_PREPARED_PIN_OWNER));
        assert_eq!(repo.record(id).unwrap().unwrap().record_class, crate::ArtifactRecordClass::Live,
            "removing the review pin does not reverse adoption");
        assert!(events.lock().unwrap().join("\n").contains("not_rolled_back"));
    }

    #[tokio::test]
    async fn t1_frame_pin_failure_and_cleanup_report_exact_private_facts() {
        let temp = tempfile::tempdir().unwrap();
        let repo = Arc::new(crate::EphemeralArtifactRepository::default());
        let store = crate::ArtifactStore::new(temp.path(), repo.clone(), crate::ArtifactStoreConfig::default()).unwrap();
        let artifact = store.write_bytes(harness_contract::context::ArtifactWriteDescriptor {
            media_type: "text/plain".into(), visibility_scope: "session:t1".into(),
            expected_bytes: None, original_name: None,
        }, b"original").await.unwrap();
        let id = artifact.selector.strip_prefix("artifact://").unwrap();
        let other = "another-original-owner";
        store.pin(&artifact, other, crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS).unwrap();
        let captured = EventCapture::default();
        let events = Arc::clone(&captured.0);
        let selector_hash = format!("{:x}", Sha256::digest(artifact.selector.as_bytes()));

        let frame = ProcessInputFrame::without_process_verifier();
        let unknown_owner = frame.artifact_owner.clone();
        repo.fixture_review_pin_response_loss();
        tracing::subscriber::with_default(captured.clone(), || {
            let error = frame.retain_artifact(&store, &artifact).unwrap_err();
            assert!(error.contains("injected committed pin response loss"));
            assert!(!frame.retains_exact_artifact(&artifact));
            drop(frame);
        });
        assert!(repo.fixture_review_exact_pin(id, &unknown_owner), "a committed pin response loss must not be guessed away");
        assert!(repo.fixture_review_exact_pin(id, other));
        let unknown_hash = format!("{:x}", Sha256::digest(unknown_owner.as_bytes()));
        let logs = events.lock().unwrap().join("\n");
        assert!(logs.contains("error_unclassified") && logs.contains("not_attempted") && logs.contains("may_have_become_live"));
        assert!(logs.contains(&selector_hash) && logs.contains(&unknown_hash));
        assert!(!logs.contains(&artifact.selector) && !logs.contains(&unknown_owner) && !logs.contains("injected committed"));

        let frame = ProcessInputFrame::without_process_verifier();
        let failed_owner = frame.artifact_owner.clone();
        frame.retain_artifact(&store, &artifact).unwrap();
        repo.fixture_review_unpin_failure();
        tracing::subscriber::with_default(captured.clone(), || drop(frame));
        assert!(repo.fixture_review_exact_pin(id, &failed_owner), "failed unpin retains the original tuple");
        assert!(repo.fixture_review_exact_pin(id, other));
        let failed_hash = format!("{:x}", Sha256::digest(failed_owner.as_bytes()));
        let logs = events.lock().unwrap().join("\n");
        assert!(logs.contains("unpin_unconfirmed") && logs.contains("not_rolled_back") && logs.contains(&failed_hash));
        assert!(!logs.contains(&failed_owner) && !logs.contains("injected unpin failure"));

        let frame = ProcessInputFrame::without_process_verifier();
        let released_owner = frame.artifact_owner.clone();
        frame.retain_artifact(&store, &artifact).unwrap();
        tracing::subscriber::with_default(captured, || drop(frame));
        assert!(!repo.fixture_review_exact_pin(id, &released_owner));
        assert!(repo.fixture_review_exact_pin(id, other), "cleanup must not remove another owner");
        assert!(events.lock().unwrap().join("\n").contains("unpin_returned_ok"));
    }
}
