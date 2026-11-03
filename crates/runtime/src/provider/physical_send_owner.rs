//! The runtime side of the physical send authority.
//!
//! A provider HTTP request is only allowed to leave the process when two
//! facts hold: the caller holds an authenticated launcher identity, and a
//! durable receipt for that exact physical send is committed first. The
//! provider crate owns the gate (see `provider::physical_send`); this module
//! owns the durable side of it.
//!
//! The receipt is written in the runtime's own durable stream, with the
//! shared identity vocabulary (session, turn, root execution, call,
//! assessment, request id, owner and generation) so the fact can be
//! correlated with the run that produced it. The identity fields are never
//! taken from the caller of a tool: they come from the runtime's own
//! binding.

use std::sync::Arc;

pub use provider::{
    LauncherAttestation, LauncherAttestationVerifier, LauncherSigningKey, PhysicalSendAuthority,
    PhysicalSendConfirmation, PhysicalSendReceipt, VerifiedLauncherIdentity,
};

/// Re-exported so dependent crates do not need the provider crate directly.
pub use provider::{authorize_physical_send, body_digest, PhysicalSendRequest};

use crate::RuntimeEventStore;

/// The runtime identity that owns one physical send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSendIdentity {
    pub session_id: String,
    pub turn_ref: Option<String>,
    pub root_execution: Option<String>,
    pub execution_id: Option<String>,
    pub step_id: Option<String>,
    pub provider_attempt_id: Option<String>,
    pub graph_attempt: Option<u32>,
    pub request_attempt: Option<u32>,
    pub owner_revision: Option<u64>,
    pub owner_fence: Option<u64>,
    pub call_id: Option<String>,
    pub assessment_id: Option<String>,
    pub request_id: String,
    pub owner: String,
    pub generation: u64,
}

impl RuntimeSendIdentity {
    /// Bind a physical-send owner to the actual Runtime request and Step.
    /// Missing or conflicting graph ownership is not replaced by a global
    /// launcher identity: such a request cannot acquire this capability.
    pub(crate) fn for_provider_attempt(
        context: &crate::ProviderRequestEvidenceContext,
        step: &harness_contract::step::StepSnapshot,
        request_id: &str,
    ) -> Result<Self, String> {
        let graph = context.graph_owner.as_deref().ok_or("provider graph owner is missing")?;
        let activity = &graph.activity;
        activity.validate().map_err(str::to_string)?;
        if request_id.trim().is_empty()
            || context.execution_id.trim().is_empty()
            || context.attempt == 0
            || graph.attempt == 0
            || context.session_id != activity.session_id
            || context.turn_id != activity.turn_id
            || step.identity.session_id != context.session_id
            || step.identity.turn_id != context.turn_id
            || step.identity.execution_id.as_deref() != Some(context.execution_id.as_str())
            || step.identity.step_id.trim().is_empty()
            || step.provider_attempt.attempt_id.trim().is_empty()
            || step.provider_attempt.origin_step_id != step.identity.step_id
            || step.identity.attempt.is_some_and(|attempt| attempt != context.attempt)
        {
            return Err("provider request, Step and graph owner do not match".into());
        }
        Ok(Self {
            session_id: context.session_id.clone(),
            turn_ref: Some(context.turn_id.clone()),
            root_execution: Some(activity.root_execution_id.clone()),
            execution_id: Some(context.execution_id.clone()),
            step_id: Some(step.identity.step_id.clone()),
            provider_attempt_id: Some(step.provider_attempt.attempt_id.clone()),
            graph_attempt: Some(graph.attempt),
            request_attempt: Some(context.attempt),
            owner_revision: Some(activity.revision),
            owner_fence: Some(activity.fence),
            call_id: None,
            assessment_id: None,
            request_id: request_id.to_owned(),
            owner: activity.activity_id.clone(),
            generation: activity.generation,
        })
    }

    fn payload(&self, receipt: &PhysicalSendReceipt) -> serde_json::Value {
        serde_json::json!({
            "session_id": self.session_id,
            "turn_ref": self.turn_ref,
            "root_execution": self.root_execution,
            "execution_id": self.execution_id,
            "step_id": self.step_id,
            "provider_attempt_id": self.provider_attempt_id,
            "graph_attempt": self.graph_attempt,
            "request_attempt": self.request_attempt,
            "owner_revision": self.owner_revision,
            "owner_fence": self.owner_fence,
            "call_id": self.call_id,
            "assessment_id": self.assessment_id,
            "request_id": self.request_id,
            "owner": self.owner,
            "generation": self.generation,
            "send_id": receipt.send_id,
            "endpoint": receipt.endpoint,
            "method": receipt.method,
            "protocol": receipt.protocol,
            "model": receipt.model,
            "body_sha256": receipt.body_sha256,
            "candidate_head": receipt.candidate_head,
            "crates_sha256": receipt.crates_sha256,
            "gateway_sha256": receipt.gateway_sha256,
            "harness_sha256": receipt.harness_sha256,
            "launcher_pid": receipt.launcher_pid,
            "launcher_starttime": receipt.launcher_starttime,
            "launcher_nonce": receipt.nonce,
            "issued_at_ms": receipt.issued_at_ms,
            "expires_at_ms": receipt.expires_at_ms,
        })
    }
}

/// The durable owner of the physical send receipts.
pub struct RuntimePhysicalSendOwner {
    store: Arc<RuntimeEventStore>,
    identity: RuntimeSendIdentity,
}

impl RuntimePhysicalSendOwner {
    #[must_use]
    pub fn new(store: Arc<RuntimeEventStore>, identity: RuntimeSendIdentity) -> Self {
        Self { store, identity }
    }

    /// The durable stream that carries this owner's physical send receipts.
    #[must_use]
    pub fn stream_id(&self) -> String {
        format!("provider-physical-send:{}", self.identity.session_id)
    }

    /// The identity that owns the receipts of this owner.
    #[must_use]
    pub fn identity(&self) -> &RuntimeSendIdentity {
        &self.identity
    }
}

impl PhysicalSendConfirmation for RuntimePhysicalSendOwner {
    fn confirm<'a>(
        &'a self,
        receipt: &'a PhysicalSendReceipt,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<PhysicalSendReceipt, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let stream_id = self.stream_id();
            let payload = self.identity.payload(receipt);
            let store = Arc::clone(&self.store);
            let transaction_id = format!("{stream_id}:{}", receipt.send_id);
            store
                .with_stream_lock(&stream_id, || {
                    let expected_revision = store
                        .stream_revision(&stream_id)
                        .map_err(|error| error.to_string())?;
                    store
                        .append_transaction_locked(crate::AppendTransactionRequest {
                            transaction_id: transaction_id.clone(),
                            expected_streams: vec![crate::ExpectedStreamRevision {
                                stream_id: stream_id.clone(),
                                expected_revision,
                            }],
                            events: vec![crate::RuntimeTransactionEventInput {
                                event: crate::RuntimeEventInput {
                                    stream_id: stream_id.clone(),
                                    scope: crate::RuntimeEventScope::Tool,
                                    kind: "provider.physical_send.receipt".to_string(),
                                    status: Some("committed".to_string()),
                                    actor: Some(self.identity.owner.clone()),
                                    refs: vec![crate::RuntimeEventRef {
                                        kind: "physical_send".to_string(),
                                        id: receipt.send_id.clone(),
                                    }],
                                    payload: payload.clone(),
                                },
                                idempotency_key: Some(transaction_id.clone()),
                                schema_version: 1,
                            }],
                        })
                        .map_err(|error| error.to_string())
                })
                .map_err(|error| error.to_string())?;
            Ok(receipt.clone())
        })
    }
}

/// Build the send capability from an already verified launcher identity.
///
/// The identity must come from a verification that consumed the launcher's
/// attestation against a trusted key; this function never accepts a caller
/// supplied identity, and a missing owner keeps every send refused.
#[must_use]
pub fn build_physical_send_authority(
    identity: VerifiedLauncherIdentity,
    owner: Arc<RuntimePhysicalSendOwner>,
) -> Arc<PhysicalSendAuthority> {
    Arc::new(PhysicalSendAuthority::new(identity, owner))
}

/// The verified launcher fact and the existing Runtime event owner.  A new
/// confirmation owner is bound for every original provider request; no
/// session or request identity is installed globally.
pub struct RuntimePhysicalSendFactory {
    launcher: VerifiedLauncherIdentity,
    store: Arc<RuntimeEventStore>,
}

impl RuntimePhysicalSendFactory {
    pub(crate) fn new(launcher: VerifiedLauncherIdentity, store: Arc<RuntimeEventStore>) -> Self {
        Self { launcher, store }
    }

    pub(crate) fn for_provider_attempt(
        &self,
        context: &crate::ProviderRequestEvidenceContext,
        step: &harness_contract::step::StepSnapshot,
        request_id: &str,
    ) -> Result<Arc<PhysicalSendAuthority>, String> {
        let identity = RuntimeSendIdentity::for_provider_attempt(context, step, request_id)?;
        Ok(build_physical_send_authority(
            self.launcher.clone(),
            Arc::new(RuntimePhysicalSendOwner::new(Arc::clone(&self.store), identity)),
        ))
    }
}

/// One candidate that the central preregistered as attributable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreregisteredCandidate {
    pub id: String,
    pub head: String,
    pub crate_rust_aggregate_sha256: String,
    pub cowd_bin_sha256: String,
    pub harness_bin_sha256: String,
}

/// The candidate the central preregistration marks as current.
///
/// A preregistration without a current candidate cannot authorize any send:
/// the run would not be attributable.
pub fn preregistered_current(
    document: &serde_json::Value,
) -> Result<PreregisteredCandidate, String> {
    let current = document
        .get("current")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "no candidate is registered as current".to_string())?;
    let candidates = document
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "the preregistration has no candidates".to_string())?;
    let entry = candidates
        .iter()
        .find(|entry| entry.get("id").and_then(serde_json::Value::as_str) == Some(current))
        .ok_or_else(|| format!("the current candidate {current} is not registered"))?;
    let field = |name: &str| {
        entry
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| format!("the current candidate {current} has no {name}"))
    };
    Ok(PreregisteredCandidate {
        id: current.to_string(),
        head: field("head")?,
        crate_rust_aggregate_sha256: field("crate_rust_aggregate_sha256")?,
        cowd_bin_sha256: field("cowd_bin_sha256")?,
        harness_bin_sha256: field("harness_bin_sha256")?,
    })
}

/// The send authority declaration that the launcher hands over.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SendAuthorityDeclaration {
    /// The launcher's ephemeral public key. It is a carrier, not the root.
    pub public_key: String,
    /// The signed attestation over the images and the candidate.
    pub attestation: LauncherAttestation,
    /// The detached signature over the attestation bytes.
    pub signature: String,
}

/// What the declaration says about the candidate, next to what the central
/// preregistered. The actual identity is recorded for attribution; it is not a
/// precondition of the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendAuthorityAttribution {
    /// The candidate the declaration named.
    pub declared_candidate: String,
    /// The candidate the central preregistered as current, if any.
    pub registered_candidate: Option<String>,
    /// Whether the declaration named the registered candidate.
    pub registration_matches: bool,
}

/// Verify a send authority declaration against the central preregistration.
///
/// The signature must verify with the delivered key: a forged declaration is
/// refused. Whether the declared candidate is the registered one is recorded
/// as attribution, never used as a precondition of a run.
pub fn verify_send_authority(
    declaration: &SendAuthorityDeclaration,
    document: &serde_json::Value,
) -> Result<(VerifiedLauncherIdentity, SendAuthorityAttribution), String> {
    let expected = preregistered_current(document).ok();
    let attestation = &declaration.attestation;
    let attribution = SendAuthorityAttribution {
        declared_candidate: attestation.candidate_head.clone(),
        registered_candidate: expected.as_ref().map(|candidate| candidate.head.clone()),
        registration_matches: expected
            .as_ref()
            .is_some_and(|candidate| candidate.head == attestation.candidate_head),
    };
    let key_bytes = hex_decode(&declaration.public_key)?;
    let key: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| "the declared public key is not 32 bytes".to_string())?;
    let signature = hex_decode(&declaration.signature)?;
    let identity = LauncherAttestationVerifier::new(key, attestation.clone())
        .verify(attestation, &signature)
        .map_err(|error| format!("the send authority attestation is not authentic: {error}"))?;
    Ok((identity, attribution))
}

/// Verify a declaration once: a nonce that was already consumed is refused.
///
/// The launcher may only hand over one authority per run. A replayed
/// declaration must not create a second usable capability.
pub fn verify_send_authority_once(
    declaration: &SendAuthorityDeclaration,
    document: &serde_json::Value,
    consumed_nonces: &mut std::collections::BTreeSet<String>,
) -> Result<(VerifiedLauncherIdentity, SendAuthorityAttribution), String> {
    if consumed_nonces.contains(&declaration.attestation.nonce) {
        return Err("this send authority declaration was already consumed".to_string());
    }
    let identity = verify_send_authority(declaration, document)?;
    consumed_nonces.insert(declaration.attestation.nonce.clone());
    Ok(identity)
}

fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if value.len() % 2 != 0 {
        return Err("a hex value has an odd length".to_string());
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| "a hex value is malformed".to_string())
        })
        .collect()
}

/// The single assembly point for the provider send authority.
///
/// The owner hands one declaration over its kernel credentialed channel; the
/// runtime that received it installs the capability here and nowhere else.
/// Two bindings are required and they are independent:
///
/// * the **sender**: the peer that delivered the declaration must be the
///   process that started this runtime (its pid), read from the channel
///   credentials - a same-uid process is not evidence; and
/// * the **declaration**: the signature must verify, so a forged declaration
///   is refused.
///
/// Whether the declared candidate is the centrally preregistered one is
/// recorded as attribution; it is never a precondition of the run or of a
/// send, and no hash gates the start.
pub fn install_send_authority_from_declaration(
    declaration: &SendAuthorityDeclaration,
    peer_pid: u32,
    expected_peer_pid: u32,
    store: Arc<RuntimeEventStore>,
    identity: RuntimeSendIdentity,
    document: &serde_json::Value,
    consumed_nonces: &mut std::collections::BTreeSet<String>,
) -> Result<(Arc<PhysicalSendAuthority>, SendAuthorityAttribution), String> {
    if peer_pid == 0 || peer_pid != expected_peer_pid {
        return Err(
            "the send authority declaration did not come from the process that started this runtime"
                .to_string(),
        );
    }
    let (verified, attribution) =
        verify_send_authority_once(declaration, document, consumed_nonces)?;
    let owner = Arc::new(RuntimePhysicalSendOwner::new(store, identity));
    Ok((
        build_physical_send_authority(verified, owner),
        attribution,
    ))
}

#[cfg(test)]
mod request_identity_tests {
    use super::*;

    fn fixture() -> (crate::ProviderRequestEvidenceContext, harness_contract::step::StepSnapshot) {
        let activity = serde_json::from_value(serde_json::json!({
            "root_execution_id":"root", "session_id":"session", "turn_id":"turn",
            "root_task_id":"root-task", "task_id":"task", "activity_id":"activity",
            "revision":4, "fence":5, "generation":6
        })).unwrap();
        let context = crate::ProviderRequestEvidenceContext {
            session_id: "session".into(), execution_id: "execution".into(),
            turn_id: "turn".into(), request_sequence: 1,
            graph_owner: Some(Box::new(crate::provider_runtime_client::ProviderGraphOwner {
                activity, attempt: 7,
            })),
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt("model", 8192, 1024, 0, 0, 1),
            attempt: 2,
        };
        let step = serde_json::from_value(serde_json::json!({
            "contract_version":1,
            "identity":{"step_id":"step","session_id":"session","turn_id":"turn",
                "execution_id":"execution","attempt":2,"build_ref":"build"},
            "provider_attempt":{"attempt_id":"provider-attempt","origin_step_id":"step",
                "model_profile_digest":"profile","request_digest":"request"},
            "catalog_digest":"catalog","tools":[],"related_revisions":{},"context_refs":[],"expires_at_ms":1
        })).unwrap();
        (context, step)
    }

    #[test]
    fn request_identity_uses_original_step_and_owner_and_rejects_mismatches() {
        let (mut context, mut step) = fixture();
        let identity = RuntimeSendIdentity::for_provider_attempt(&context, &step, "request-id").unwrap();
        assert_eq!(identity.owner, "activity");
        assert_eq!((identity.generation, identity.owner_fence, identity.owner_revision), (6, Some(5), Some(4)));
        assert_eq!((identity.graph_attempt, identity.request_attempt), (Some(7), Some(2)));
        assert_eq!(identity.step_id.as_deref(), Some("step"));
        assert_eq!(identity.provider_attempt_id.as_deref(), Some("provider-attempt"));
        assert!(RuntimeSendIdentity::for_provider_attempt(&context, &step, "").is_err());
        step.provider_attempt.origin_step_id = "other".into();
        assert!(RuntimeSendIdentity::for_provider_attempt(&context, &step, "request-id").is_err());
        step.provider_attempt.origin_step_id = "step".into();
        context.graph_owner.as_mut().unwrap().activity.session_id = "other".into();
        assert!(RuntimeSendIdentity::for_provider_attempt(&context, &step, "request-id").is_err());
        context.graph_owner = None;
        assert!(RuntimeSendIdentity::for_provider_attempt(&context, &step, "request-id").is_err());
    }
}
