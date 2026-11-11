use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use harness_contract::step::{
    canonical_digest, CallOrigin, CallSource, CatalogAvailability, ProviderAttemptIdentity,
    StepIdentity, StepSnapshot, ToolBindingRef, STEP_CONTRACT_VERSION,
};

use crate::{ProviderToolDefinition, RuntimeError};

pub(crate) fn admitted_turn_id(
    execution: Option<&crate::CowdExecutionContext>,
    input_turn: Option<String>,
) -> Result<String, RuntimeError> {
    execution
        .map(|context| context.turn_id.clone())
        .or(input_turn)
        .ok_or_else(|| RuntimeError::new("production step has no admitted turn identity"))
}

#[cfg(test)]
mod turn_identity_tests {
    #[test]
    fn execution_owner_turn_is_not_replaced_by_supplemental_input_identity() {
        let execution = crate::CowdExecutionContext {
            execution_id: "graph".into(),
            session_id: "session".into(),
            turn_id: "admitted-turn".into(),
        };
        assert_eq!(
            super::admitted_turn_id(Some(&execution), Some("supplement-turn".into())).unwrap(),
            "admitted-turn"
        );
        assert_eq!(
            super::admitted_turn_id(None, Some("standalone-turn".into())).unwrap(),
            "standalone-turn"
        );
        assert!(super::admitted_turn_id(None, None).is_err());
    }
}

#[derive(Debug, Clone)]
pub struct BoundStep {
    provider_consumed: Arc<std::sync::atomic::AtomicBool>,
    snapshot: Arc<StepSnapshot>,
    lease: tools::ToolHostLease,
}

impl PartialEq for BoundStep {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot == other.snapshot
    }
}

impl Eq for BoundStep {}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StepCallBinding {
    pub origin: CallOrigin,
    pub snapshot: Arc<StepSnapshot>,
    #[serde(skip)]
    bound_step: Option<Arc<BoundStep>>,
}

impl PartialEq for StepCallBinding {
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin && self.snapshot == other.snapshot
    }
}

impl Eq for StepCallBinding {}

impl StepCallBinding {
    #[must_use]
    pub fn effect_idempotency_key(&self) -> String {
        canonical_digest(
            "step-effect-identity",
            &serde_json::json!([
                self.snapshot.identity.session_id,
                self.origin.origin_step_id,
                self.origin.call_id,
            ]),
        )
        .expect("fixed domain and UTF-8 string tuple always have canonical encoding")
    }

    pub(crate) fn prepare_invocation(
        &self,
        current: Option<tools::ToolHostLease>,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Result<harness_contract::tool::GovernedToolInvocation, RuntimeError> {
        let step = self.resolve(current)?;
        let mut invocation =
            step.lease()
                .prepare_governed_invocation(&self.origin.call_id, tool_name, input, &[]);
        invocation.origin = Some(self.origin.clone());
        Ok(invocation)
    }
    pub(crate) fn system(step: Arc<BoundStep>, parent_step_id: &str, call_id: &str) -> Self {
        Self {
            origin: CallOrigin {
                origin_step_id: step.snapshot.identity.step_id.clone(),
                call_id: call_id.to_string(),
                catalog_digest: step.snapshot.catalog_digest.clone(),
                source: CallSource::SystemDerived {
                    parent_step_id: parent_step_id.to_string(),
                },
            },
            snapshot: Arc::clone(&step.snapshot),
            bound_step: Some(step),
        }
    }
    #[must_use]
    pub fn provider(step: Arc<BoundStep>, call_id: &str) -> Self {
        Self {
            origin: step.provider_origin(call_id),
            snapshot: Arc::clone(&step.snapshot),
            bound_step: Some(step),
        }
    }

    #[must_use]
    pub fn persisted(origin: CallOrigin, snapshot: Arc<StepSnapshot>) -> Self {
        Self {
            origin,
            snapshot,
            bound_step: None,
        }
    }

    #[must_use]
    pub(crate) fn process(
        step: Arc<BoundStep>,
        worker_id: &str,
        sequence: u64,
        call_id: &str,
    ) -> Self {
        Self {
            origin: CallOrigin {
                origin_step_id: step.snapshot.identity.step_id.clone(),
                call_id: call_id.to_string(),
                catalog_digest: step.snapshot.catalog_digest.clone(),
                source: CallSource::Process {
                    worker_id: worker_id.to_string(),
                    sequence,
                },
            },
            snapshot: Arc::clone(&step.snapshot),
            bound_step: Some(step),
        }
    }

    pub fn resolve(
        &self,
        current: Option<tools::ToolHostLease>,
    ) -> Result<Arc<BoundStep>, RuntimeError> {
        if let Some(step) = &self.bound_step {
            return Ok(Arc::clone(step));
        }
        let lease = current.ok_or_else(|| RuntimeError::new("step_tool_host_unavailable"))?;
        BoundStep::restore(self.snapshot.as_ref().clone(), lease).map(Arc::new)
    }
}

impl BoundStep {
    pub(crate) fn with_parent(mut self, parent_step_id: &str) -> Self {
        Arc::make_mut(&mut self.snapshot)
            .context_refs
            .push(format!("step:{parent_step_id}"));
        self
    }
    pub fn bind(
        identity: StepIdentity,
        lease: tools::ToolHostLease,
        advertised: &[ProviderToolDefinition],
        model_profile: &serde_json::Value,
        request: &serde_json::Value,
        related_revisions: BTreeMap<String, String>,
        expires_at_ms: i64,
    ) -> Result<Self, RuntimeError> {
        if identity.step_id.is_empty()
            || identity.session_id.is_empty()
            || identity.turn_id.is_empty()
        {
            return Err(RuntimeError::new(
                "step identity requires runtime step/session/turn coordinates",
            ));
        }
        let registered = lease.snapshot().catalog.definitions(None);
        let mut seen = BTreeSet::new();
        let mut bindings = Vec::with_capacity(advertised.len());
        for definition in advertised {
            if !seen.insert(definition.name.clone()) {
                return Err(RuntimeError::new(format!(
                    "duplicate advertised tool `{}`",
                    definition.name
                )));
            }
            let registered = registered
                .iter()
                .find(|registered| registered.name == definition.name)
                .ok_or_else(|| {
                    RuntimeError::new(format!(
                        "advertised tool `{}` is absent from pinned catalog",
                        definition.name
                    ))
                })?;
            if registered.input_schema != definition.input_schema
                || registered.description != definition.description
            {
                return Err(RuntimeError::new(format!(
                    "advertised tool `{}` disagrees with pinned catalog",
                    definition.name
                )));
            }
            if let Some(reason) = lease.known_unavailability(&definition.name) {
                return Err(RuntimeError::new(format!(
                    "unavailable tool `{}` must not be advertised: {reason}",
                    definition.name
                )));
            }
            let availability = CatalogAvailability::Unknown;
            let resolver = lease.snapshot().catalog.effect_resolver(&definition.name);
            bindings.push(ToolBindingRef {
                tool_id: definition.name.clone(),
                semantic_revision: lease.implementation_revision().to_string(),
                schema_digest: digest("tool-schema", &definition.input_schema)?,
                examples_digest: digest(
                    "tool-presentation",
                    &serde_json::json!(definition.description),
                )?,
                effect_descriptor_digest: digest(
                    "tool-effect-resolver",
                    &serde_json::to_value(resolver)
                        .map_err(|error| RuntimeError::new(error.to_string()))?,
                )?,
                availability,
                availability_reason:
                    "registered_only; live environment and authorization checked at dispatch"
                        .to_string(),
                observed_at_ms: chrono::Utc::now().timestamp_millis(),
            });
        }
        bindings.sort_by(|left, right| left.tool_id.cmp(&right.tool_id));
        let catalog_digest = digest(
            "step-tool-catalog",
            &serde_json::to_value(&bindings)
                .map_err(|error| RuntimeError::new(error.to_string()))?,
        )?;
        let provider_attempt = ProviderAttemptIdentity {
            attempt_id: uuid::Uuid::new_v4().to_string(),
            origin_step_id: identity.step_id.clone(),
            model_profile_digest: digest("provider-profile", model_profile)?,
            request_digest: digest("runtime-request", request)?,
        };
        Ok(Self {
            provider_consumed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            snapshot: Arc::new(StepSnapshot {
                contract_version: STEP_CONTRACT_VERSION,
                identity,
                provider_attempt,
                catalog_digest,
                tools: bindings,
                related_revisions,
                context_refs: Vec::new(),
                expires_at_ms,
            }),
            lease,
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> &StepSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn lease(&self) -> &tools::ToolHostLease {
        &self.lease
    }

    pub fn validate_provider_request(
        &self,
        request: &crate::ApiRequest,
        advertised: &[ProviderToolDefinition],
    ) -> Result<(), RuntimeError> {
        if digest("runtime-request", &request_material(request))?
            != self.snapshot.provider_attempt.request_digest
            || advertised.len() != self.snapshot.tools.len()
        {
            return Err(RuntimeError::new(
                "provider_request_changed_after_step_binding",
            ));
        }
        let mut seen = BTreeSet::new();
        for definition in advertised {
            let binding = self
                .snapshot
                .tools
                .iter()
                .find(|binding| binding.tool_id == definition.name)
                .ok_or_else(|| RuntimeError::new("provider_tool_outside_bound_step"))?;
            if !seen.insert(definition.name.as_str())
                || binding.schema_digest != digest("tool-schema", &definition.input_schema)?
                || binding.examples_digest
                    != digest(
                        "tool-presentation",
                        &serde_json::json!(definition.description),
                    )?
            {
                return Err(RuntimeError::new(
                    "provider_tool_contract_changed_after_step_binding",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub(crate) fn consume_provider_attempt(&self) -> Result<(), RuntimeError> {
        self.provider_consumed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| {
                RuntimeError::preparation(
                    crate::ProviderPreparationFailure::Consumed,
                    "bound Provider attempt already consumed; bind a fresh Step",
                )
            })
    }

    pub fn provider_origin(&self, call_id: &str) -> CallOrigin {
        CallOrigin {
            origin_step_id: self.snapshot.identity.step_id.clone(),
            call_id: call_id.to_string(),
            catalog_digest: self.snapshot.catalog_digest.clone(),
            source: CallSource::Provider {
                attempt_id: self.snapshot.provider_attempt.attempt_id.clone(),
            },
        }
    }

    pub fn validate_call(
        &self,
        origin: &CallOrigin,
        session_id: &str,
        call_id: &str,
        tool_name: &str,
        input: &str,
        now_ms: i64,
    ) -> Result<serde_json::Value, RuntimeError> {
        if self.snapshot.contract_version != STEP_CONTRACT_VERSION
            || call_id.is_empty()
            || self.snapshot.identity.session_id != session_id
            || self.snapshot.identity.step_id != origin.origin_step_id
            || self.snapshot.catalog_digest != origin.catalog_digest
            || origin.call_id != call_id
        {
            return Err(RuntimeError::new("step_origin_mismatch"));
        }
        match &origin.source {
            CallSource::Provider { attempt_id }
                if attempt_id == &self.snapshot.provider_attempt.attempt_id => {}
            CallSource::Process {
                worker_id,
                sequence,
            } if !worker_id.is_empty()
                && *sequence > 0
                && self.snapshot.related_revisions.get("worker_run") == Some(worker_id) => {}
            CallSource::SystemDerived { parent_step_id }
                if self.snapshot.identity.execution_id.is_some()
                    && self
                        .snapshot
                        .context_refs
                        .contains(&format!("step:{parent_step_id}")) => {}
            _ => return Err(RuntimeError::new("step_origin_source_rejected")),
        }
        if now_ms >= self.snapshot.expires_at_ms {
            return Err(RuntimeError::new("step_origin_expired"));
        }
        if !self.snapshot.tools.iter().any(|tool| {
            tool.tool_id == tool_name && tool.availability != CatalogAvailability::Unavailable
        }) {
            return Err(RuntimeError::new("tool_outside_bound_step"));
        }
        // This is the production bound-call path. It deliberately does not
        // delegate to an executor's unbound validator (which could consult a
        // newer catalog). Reuse the existing example from this exact pinned
        // schema, after authenticating the origin and exposure above.
        let input_error = |error: String| {
            let example = self
                .lease
                .snapshot()
                .catalog
                .definitions(None)
                .into_iter()
                .find(|definition| definition.name == tool_name)
                .and_then(|definition| {
                    harness_contract::tool_input_contract::input_example(&definition.input_schema)
                });
            RuntimeError::new(match example {
                Some(example) => format!("{error}. Send it shaped like this: {example}"),
                None => error,
            })
        };
        let mut input = harness_contract::step::parse_strict_json(input)
            .map_err(|error| input_error(error.to_string()))?;
        // Only the authenticated, pinned batch schema can authorize decoding
        // this provider spelling; the original schema still validates the result.
        if tool_name == "agent_action_batch" {
            let array_in_schema = self.lease.snapshot().catalog.definitions(None).into_iter()
                .find(|definition| definition.name == tool_name)
                .and_then(|definition| definition.input_schema.get("properties")
                    .and_then(|properties| properties.get("actions"))
                    .and_then(|schema| schema.get("type"))
                    .and_then(serde_json::Value::as_str)
                    .map(|kind| kind == "array"))
                .unwrap_or(false);
            if array_in_schema {
                if let Some(serde_json::Value::String(encoded)) = input.get("actions") {
                    let decoded = harness_contract::step::parse_strict_json(encoded)
                        .map_err(|_| input_error("$.actions must contain a complete valid JSON array; no items were inferred or discarded".into()))?;
                    if !decoded.is_array() {
                        return Err(input_error("$.actions must contain a complete valid JSON array".into()));
                    }
                    input["actions"] = decoded;
                }
            }
        }
        self.lease
            .validate_input(tool_name, &input)
            .map_err(|error| input_error(error.to_string()))?;
        Ok(input)
    }

    pub fn restore(
        snapshot: StepSnapshot,
        lease: tools::ToolHostLease,
    ) -> Result<Self, RuntimeError> {
        if snapshot.contract_version != STEP_CONTRACT_VERSION
            || snapshot.identity.step_id.is_empty()
            || snapshot.identity.session_id.is_empty()
            || snapshot.identity.turn_id.is_empty()
            || snapshot.provider_attempt.attempt_id.is_empty()
            || snapshot.provider_attempt.origin_step_id != snapshot.identity.step_id
            || snapshot
                .tools
                .iter()
                .any(|tool| tool.semantic_revision != lease.implementation_revision())
        {
            return Err(RuntimeError::new("step_binding_stale"));
        }
        let bindings = serde_json::to_value(&snapshot.tools)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if digest("step-tool-catalog", &bindings)? != snapshot.catalog_digest {
            return Err(RuntimeError::new("step_catalog_digest_mismatch"));
        }
        let definitions = lease.snapshot().catalog.definitions(None);
        let mut seen = BTreeSet::new();
        for binding in &snapshot.tools {
            let definition = definitions
                .iter()
                .find(|definition| definition.name == binding.tool_id)
                .ok_or_else(|| RuntimeError::new("step_tool_contract_missing"))?;
            let resolver = lease.snapshot().catalog.effect_resolver(&binding.tool_id);
            if !seen.insert(&binding.tool_id)
                || binding.availability == CatalogAvailability::Unavailable
                || binding.schema_digest != digest("tool-schema", &definition.input_schema)?
                || binding.examples_digest
                    != digest(
                        "tool-presentation",
                        &serde_json::json!(definition.description),
                    )?
                || binding.effect_descriptor_digest
                    != digest(
                        "tool-effect-resolver",
                        &serde_json::to_value(resolver)
                            .map_err(|error| RuntimeError::new(error.to_string()))?,
                    )?
            {
                return Err(RuntimeError::new("step_tool_contract_mismatch"));
            }
        }
        Ok(Self {
            // Recovery restores tool/read attribution, never permission to
            // replay an uncertain physical Provider attempt.
            provider_consumed: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            snapshot: Arc::new(snapshot),
            lease,
        })
    }
}

fn digest(domain: &str, value: &serde_json::Value) -> Result<String, RuntimeError> {
    canonical_digest(domain, value).map_err(|error| RuntimeError::new(error.to_string()))
}

pub(crate) fn request_material(request: &crate::ApiRequest) -> serde_json::Value {
    serde_json::json!({
        "model": request.model,
        "reasoning_effort": request.reasoning_effort_override,
        "system": request.prompt.wire_system_text(),
        "cohort_user_prefix": request.prompt.cache_cohort_user_prefix_messages(),
        "user_prefix": request.prompt.immutable_user_prefix_messages(),
        "context": request.prompt.contextual_messages(),
        "history": crate::provider_runtime_client::convert_messages(request.messages.iter()),
        "max_output_tokens": request.budget.requested_output_tokens,
    })
}

pub(crate) fn bind_plan_calls(
    plan: &mut crate::GovernedToolPlan,
    calls: &[crate::conversation::ModelToolCall],
) {
    for task in &mut plan.tasks {
        task.step_binding = calls
            .iter()
            .find(|call| call.id == task.tool_call_id)
            .and_then(|call| call.binding.clone());
        task.invocation.origin = task
            .step_binding
            .as_ref()
            .map(|binding| binding.origin.clone());
    }
}

const PERSISTED_BATCH_LIMIT: usize = 1024 * 1024;

#[derive(Clone, Copy)]
enum PersistedBatchKind {
    Normal,
    BoundAgent,
}

impl PersistedBatchKind {
    fn envelope_kind(self) -> &'static str {
        match self {
            Self::Normal => "cowd_tool_batch_zlib_v1",
            Self::BoundAgent => "cowd_bound_agent_tool_zlib_v1",
        }
    }

    fn session_id(self, value: &serde_json::Value) -> Option<&str> {
        match self {
            Self::Normal => value["session_id"]
                .as_str()
                .filter(|session| !session.is_empty()),
            Self::BoundAgent => value["request"]["session_id"]
                .as_str()
                .filter(|session| !session.trim().is_empty()),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PackedToolBatch {
    kind: String,
    session_id: String,
    decoded_bytes: usize,
    sha256: String,
    body: String,
}

fn persisted_batch_error(reason: &str) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidData, reason))
}

pub(crate) fn encode_persisted_tool_batch(
    value: &serde_json::Value,
) -> Result<String, serde_json::Error> {
    encode_persisted_batch(value, PersistedBatchKind::Normal)
}

pub(crate) fn encode_persisted_bound_agent_tool(
    value: &serde_json::Value,
) -> Result<String, serde_json::Error> {
    encode_persisted_batch(value, PersistedBatchKind::BoundAgent)
}

fn encode_persisted_batch(
    value: &serde_json::Value,
    kind: PersistedBatchKind,
) -> Result<String, serde_json::Error> {
    use base64::Engine;
    use sha2::Digest;
    let session_id = kind
        .session_id(value)
        .ok_or_else(|| persisted_batch_error("tool batch Session missing"))?;
    let decoded = serde_json::to_vec(value)?;
    if decoded.len() > PERSISTED_BATCH_LIMIT {
        return Err(persisted_batch_error("tool batch decoded budget exceeded"));
    }
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut encoder, &decoded).map_err(serde_json::Error::io)?;
    let compressed = encoder.finish().map_err(serde_json::Error::io)?;
    let packed = PackedToolBatch {
        kind: kind.envelope_kind().into(),
        session_id: session_id.into(),
        decoded_bytes: decoded.len(),
        sha256: format!("sha256:{:x}", sha2::Sha256::digest(&decoded)),
        body: base64::engine::general_purpose::STANDARD.encode(&compressed),
    };
    let encoded = serde_json::to_string(&packed)?;
    if encoded
        .len()
        .checked_add(packed.body.len())
        .and_then(|bytes| bytes.checked_add(decoded.len()))
        .is_none_or(|bytes| bytes > PERSISTED_BATCH_LIMIT)
    {
        return Err(persisted_batch_error(
            "tool batch codec working-set budget exceeded",
        ));
    }
    Ok(encoded)
}

pub(crate) fn decode_persisted_tool_batch_value(
    payload: &str,
) -> Result<serde_json::Value, serde_json::Error> {
    decode_persisted_batch_value(payload, PersistedBatchKind::Normal)
}

#[cfg(feature = "test-fixtures")]
pub fn decode_persisted_tool_batch_for_test(
    payload: &str,
) -> Result<serde_json::Value, serde_json::Error> {
    decode_persisted_tool_batch_value(payload)
}

pub(crate) fn decode_persisted_bound_agent_tool_value(
    payload: &str,
) -> Result<serde_json::Value, serde_json::Error> {
    decode_persisted_batch_value(payload, PersistedBatchKind::BoundAgent)
}

fn decode_persisted_batch_value(
    payload: &str,
    kind: PersistedBatchKind,
) -> Result<serde_json::Value, serde_json::Error> {
    use base64::Engine;
    use sha2::Digest;
    if payload.len() > PERSISTED_BATCH_LIMIT {
        return Err(persisted_batch_error("tool batch encoded budget exceeded"));
    }
    let envelope = harness_contract::step::parse_strict_json(payload)
        .map_err(|_| persisted_batch_error("invalid tool batch envelope JSON"))?;
    let packed: PackedToolBatch = serde_json::from_value(envelope)?;
    if packed.kind != kind.envelope_kind()
        || packed.session_id.is_empty()
        || packed.decoded_bytes == 0
        || payload
            .len()
            .checked_add(packed.decoded_bytes)
            .and_then(|bytes| bytes.checked_add(packed.body.len()))
            .is_none_or(|bytes| bytes > PERSISTED_BATCH_LIMIT)
    {
        return Err(persisted_batch_error("tool batch codec extent invalid"));
    }
    let compressed = base64::engine::general_purpose::STANDARD
        .decode(&packed.body)
        .map_err(|_| persisted_batch_error("tool batch base64 invalid"))?;
    let mut decoded = vec![0; packed.decoded_bytes];
    let mut decoder = flate2::read::ZlibDecoder::new(compressed.as_slice());
    std::io::Read::read_exact(&mut decoder, &mut decoded)
        .map_err(|_| persisted_batch_error("tool batch compressed body invalid"))?;
    let mut trailing = [0u8; 1];
    let extra = std::io::Read::read(&mut decoder, &mut trailing)
        .map_err(|_| persisted_batch_error("tool batch compressed tail invalid"))?;
    if extra != 0
        || decoder.total_in() != compressed.len() as u64
        || format!("sha256:{:x}", sha2::Sha256::digest(&decoded)) != packed.sha256
    {
        return Err(persisted_batch_error("tool batch bytes/digest conflict"));
    }
    let text = std::str::from_utf8(&decoded)
        .map_err(|_| persisted_batch_error("tool batch UTF8 invalid"))?;
    let value = harness_contract::step::parse_strict_json(text)
        .map_err(|_| persisted_batch_error("tool batch body JSON invalid"))?;
    if kind.session_id(&value) != Some(packed.session_id.as_str()) {
        return Err(persisted_batch_error("tool batch Session conflict"));
    }
    Ok(value)
}

/// A redundant copy of the persisted batch payload inside one action payload
/// is replaced by a pointer to the single copy that event keeps.
const BATCH_PAYLOAD_REF_POINTER: &str = "__cowd_batch_payload_ref__";
/// A redundant copy of the Step snapshot that persisted batch payload carries
/// is replaced by a pointer to the snapshot the batch keeps.
const BATCH_STEP_SNAPSHOT_POINTER: &str = "__cowd_batch_step_snapshot__";

/// The Session the original member facts were admitted in.
fn member_facts_session_id(facts: &serde_json::Value) -> Option<&str> {
    facts.pointer("/outer_hook/session_id")
        .and_then(serde_json::Value::as_str)
        .filter(|session| !session.is_empty())
}

/// The persisted batch payload this member facts keep: the ticket's own
/// `payload_ref`. It is the one copy the event still carries inline.
fn member_facts_payload_ref(facts: &serde_json::Value) -> Option<&str> {
    facts.pointer("/ticket/payload_ref")
        .and_then(serde_json::Value::as_str)
        .filter(|payload| !payload.is_empty())
}

/// Does this facts value hold a pointer to the batch payload or to its Step
/// snapshot? The old inline shape holds neither and is returned unchanged.
fn holds_batch_pointer(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, child)| {
            (key == BATCH_PAYLOAD_REF_POINTER || key == BATCH_STEP_SNAPSHOT_POINTER)
                || holds_batch_pointer(child)
        }),
        serde_json::Value::Array(items) => items.iter().any(holds_batch_pointer),
        _ => false,
    }
}

/// Replace every copy of the persisted batch payload, and of the Step snapshot
/// it carries, that is byte-equal to the copy this event keeps.
///
/// Only an equal copy is replaced, so the same-source rebuild is byte
/// identical to the shape this used to inline.
fn replace_redundant_copies(
    value: &mut serde_json::Value,
    path: &[String],
    payload_ref: &str,
    snapshots: &serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                let Some(child) = map.get_mut(&key) else { continue };
                let mut child_path = path.to_vec();
                child_path.push(key.clone());
                let keeps_the_copy = path.len() == 1 && path.first().map(String::as_str) == Some("ticket");
                if key == "payload_ref"
                    && !keeps_the_copy
                    && child.as_str() == Some(payload_ref)
                {
                    *child = serde_json::json!({ BATCH_PAYLOAD_REF_POINTER: "ticket.payload_ref" });
                    continue;
                }
                if key == "snapshot" {
                    let mut carried: Option<String> = None;
                    for (step_id, snapshot) in snapshots.iter() {
                        if snapshot == &*child {
                            carried = Some(step_id.clone());
                            break;
                        }
                    }
                    if let Some(step_id) = carried {
                        *child = serde_json::json!({ BATCH_STEP_SNAPSHOT_POINTER: step_id });
                        continue;
                    }
                }
                replace_redundant_copies(child, &child_path, payload_ref, snapshots);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                replace_redundant_copies(item, path, payload_ref, snapshots);
            }
        }
        _ => {}
    }
}

/// Restore every pointer to the copy the event keeps.
fn restore_batch_pointers(
    value: &mut serde_json::Value,
    payload_ref: &str,
    snapshots: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    // A pointer is the whole object: the marker names the copy this event
    // keeps, and the rebuild replaces the pointer with that copy.
    if let Some(map) = value.as_object() {
        if map.len() == 1 {
            if let Some(target) = map.get(BATCH_PAYLOAD_REF_POINTER) {
                if target.as_str() != Some("ticket.payload_ref") {
                    return Err("original member facts missing: unknown batch payload pointer".into());
                }
                *value = serde_json::Value::String(payload_ref.to_string());
                return Ok(());
            }
            if let Some(step_id) = map.get(BATCH_STEP_SNAPSHOT_POINTER) {
                let step_id = step_id
                    .as_str()
                    .ok_or("original member facts missing: batch Step pointer unreadable")?;
                let snapshot = snapshots
                    .get(step_id)
                    .ok_or("original member facts missing: batch Step pointer unresolved")?;
                *value = snapshot.clone();
                return Ok(());
            }
        }
    }
    match value {
        serde_json::Value::Object(map) => {
            for (_, child) in map.iter_mut() {
                restore_batch_pointers(child, payload_ref, snapshots)?;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                restore_batch_pointers(item, payload_ref, snapshots)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Replace the redundant copies of the persisted batch payload, and of the
/// Step snapshot it carries, inside one member facts value.
///
/// The event keeps one copy of the batch payload (the ticket's own
/// `payload_ref`) and one copy of the Step snapshot (inside that batch). Every
/// other copy points at them, so the event no longer grows with the member's
/// batch payload. Nothing else changes: only an equal copy is replaced, so
/// the same-source rebuild is byte identical to the inline shape.
pub(crate) fn dedupe_member_admission_facts(
    facts: &mut serde_json::Value,
) -> Result<(), String> {
    let Some(payload_ref) = member_facts_payload_ref(facts).map(str::to_string) else { return Ok(()); };
    let batch = decode_persisted_tool_batch_value(&payload_ref)
        .map_err(|error| format!("original member facts missing: {error}"))?;
    let empty = serde_json::Map::new();
    let snapshots = batch
        .get("step_snapshots")
        .and_then(serde_json::Value::as_object)
        .unwrap_or(&empty);
    replace_redundant_copies(facts, &[], &payload_ref, snapshots);
    Ok(())
}

/// Rebuild the original member facts from the same-source batch payload.
///
/// The old inline shape is returned unchanged. The new pointer shape is
/// rebuilt from the batch payload the event keeps, so every reader sees the
/// byte identical facts it always saw. A pointer that cannot be resolved is
/// an explicit failure: a missing fact is never read as "no fact".
pub(crate) fn rebuild_member_admission_facts(
    facts: &serde_json::Value,
) -> Result<std::borrow::Cow<'_, serde_json::Value>, String> {
    if !holds_batch_pointer(facts) {
        return Ok(std::borrow::Cow::Borrowed(facts));
    }
    let payload_ref = member_facts_payload_ref(facts)
        .ok_or("original member facts missing: batch payload pointer unresolved")?;
    let batch = decode_persisted_tool_batch_value(payload_ref)
        .map_err(|error| format!("original member facts missing: {error}"))?;
    let session_id = member_facts_session_id(facts)
        .ok_or("original member facts missing: Session binding unavailable")?;
    if batch.get("session_id").and_then(serde_json::Value::as_str) != Some(session_id) {
        return Err("original member facts missing: batch payload Session differs".into());
    }
    let empty = serde_json::Map::new();
    let snapshots = batch
        .get("step_snapshots")
        .and_then(serde_json::Value::as_object)
        .unwrap_or(&empty);
    let mut rebuilt = facts.clone();
    restore_batch_pointers(&mut rebuilt, payload_ref, snapshots)?;
    Ok(std::borrow::Cow::Owned(rebuilt))
}

/// Rebuild the member admission facts inside one action payload.
///
/// The old inline shape is returned unchanged; the new pointer shape is
/// rebuilt from the batch payload the action event keeps, so every reader
/// sees the byte identical facts it always saw.
pub(crate) fn rebuild_member_admission_payload(
    payload: &serde_json::Value,
) -> Result<std::borrow::Cow<'_, serde_json::Value>, String> {
    let Some(facts) = payload.get("member_admission") else {
        return Ok(std::borrow::Cow::Borrowed(payload));
    };
    if !holds_batch_pointer(facts) {
        return Ok(std::borrow::Cow::Borrowed(payload));
    }
    let rebuilt = rebuild_member_admission_facts(facts)?;
    let mut payload = payload.clone();
    payload["member_admission"] = rebuilt.into_owned();
    Ok(std::borrow::Cow::Owned(payload))
}

#[cfg(test)]
mod persisted_payload_413_tests {
    use super::*;

    #[test]
    fn bounded_batch_codec_preserves_every_nested_origin_and_snapshot_field() {
        let value = serde_json::json!({"session_id":"session-413","calls":[{"origin":{"origin_step_id":"step-413","catalog_digest":"catalog","source":{"kind":"provider","attempt_id":"attempt-413"}},"input":"unchanged","depends_on":["original"]}],
            "step_snapshots":{"step-413":{"identity":{"attempt":"original","parent":"original","generation":3},"tools":[{"permissions":{"allow_write":false},"purpose":"business","schema":"complete schema ".repeat(1500)}],"catalog_digest":"catalog","related_revisions":{"goal":"7"}}},"continue_with_tool_batch":true});
        let packed = encode_persisted_tool_batch(&value).unwrap();
        assert!(packed.len() < serde_json::to_vec(&value).unwrap().len() / 2);
        assert_eq!(decode_persisted_tool_batch_value(&packed).unwrap(), value);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&packed).unwrap()["session_id"],
            "session-413"
        );
    }

    #[test]
    fn bounded_batch_codec_rejects_forged_extent_digest_owner_and_old_carrier() {
        let value = serde_json::json!({"session_id":"session-413","calls":[],"step_snapshots":{}});
        assert!(decode_persisted_tool_batch_value(&value.to_string()).is_err());
        let packed = encode_persisted_tool_batch(&value).unwrap();
        let original: serde_json::Value = serde_json::from_str(&packed).unwrap();
        for (field, replacement) in [
            ("decoded_bytes", serde_json::json!(usize::MAX)),
            ("sha256", serde_json::json!("sha256:wrong")),
            ("session_id", serde_json::json!("foreign")),
            ("body", serde_json::json!("!!!!")),
        ] {
            let mut changed = original.clone();
            changed[field] = replacement;
            assert!(
                decode_persisted_tool_batch_value(&changed.to_string()).is_err(),
                "{field}"
            );
        }
        let too_large = serde_json::json!({"session_id":"session-413","payload":"x".repeat(PERSISTED_BATCH_LIMIT)});
        assert!(encode_persisted_tool_batch(&too_large).is_err());
    }
}

/// A single member prepared from the exact bound outer call. This is only a
/// preparation, not policy approval or authority to execute the member.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct PreparedAgentBatchMember {
    pub(crate) outer_key: String,
    pub(crate) index: usize,
    pub(crate) tool: String,
    pub(crate) input: serde_json::Value,
    pub(crate) effect: harness_contract::tool::ToolEffectDescriptor,
    pub(crate) identity: String,
}

/// Coordinates of facts that the original member owner re-read and checked
/// against their own transaction receipt while holding the source locks.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct OriginalMemberEventRef {
    pub(crate) stream_id: String,
    pub(crate) sequence: u64,
    pub(crate) event_id: String,
    pub(crate) scope: crate::RuntimeEventScope,
    pub(crate) kind: String,
    pub(crate) schema_version: u32,
    pub(crate) transaction_id: String,
    pub(crate) transaction_index: u32,
    pub(crate) commit_cursor: u64,
    pub(crate) idempotency_key: Option<String>,
}

/// Resolve only original outer bytes and the original pinned Step. Never use a
/// current catalog to fill an unadvertised member or accept an input supplied
/// separately by the caller.
pub(crate) fn prepare_agent_batch_member(
    binding: &StepCallBinding,
    current: Option<tools::ToolHostLease>,
    session_id: &str,
    outer_invocation: &harness_contract::tool::GovernedToolInvocation,
    outer_input: &str,
    parent: &harness_contract::execution_graph::ExecutionParentBinding,
    attempt: u32,
    index: usize,
    now_ms: i64,
) -> Result<PreparedAgentBatchMember, RuntimeError> {
    if parent.execution_id.is_empty() || parent.node_id.is_empty() || attempt == 0 {
        return Err(RuntimeError::new("batch original physical owner missing"));
    }
    let step = binding.resolve(current)?;
    let outer = step.validate_call(&binding.origin, session_id, &binding.origin.call_id,
        "agent_action_batch", outer_input, now_ms)?;
    if outer_invocation.invocation_id != binding.origin.call_id
        || outer_invocation.origin.as_ref() != Some(&binding.origin)
        || outer_invocation.intent.tool_name != "agent_action_batch"
        || outer_invocation.intent.normalized_input != outer
        || outer_invocation.effect != step.lease().describe_effect("agent_action_batch", &outer)
    { return Err(RuntimeError::new("original batch invocation mismatch")); }
    let actions = outer.get("actions").and_then(serde_json::Value::as_array)
        .filter(|actions| !actions.is_empty() && actions.len() <= 25)
        .ok_or_else(|| RuntimeError::new("invalid original action batch"))?;
    let entry = actions.get(index).ok_or_else(|| RuntimeError::new("batch member index missing"))?;
    let tool = entry.get("tool").and_then(serde_json::Value::as_str)
        .filter(|tool| harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(tool))
        .ok_or_else(|| RuntimeError::new("batch member tool is not an Agent action"))?;
    let mut input = entry.get("input").cloned().unwrap_or_else(|| serde_json::json!({}));
    let object = input.as_object_mut().ok_or_else(|| RuntimeError::new("batch member input is not an object"))?;
    for name in ["reference", "evidence_ref"] {
        if let Some(raw) = object.get(name).and_then(serde_json::Value::as_str) {
            object.insert(name.into(), serde_json::Value::String(crate::normalize_bare_evidence_ref(raw)));
        }
    }
    for name in ["evidence_refs", "result_refs", "artifact_refs"] {
        if let Some(items) = object.get_mut(name).and_then(serde_json::Value::as_array_mut) {
            for item in items {
                if let Some(raw) = item.as_str() {
                    *item = serde_json::Value::String(crate::normalize_bare_evidence_ref(raw));
                }
            }
        }
    }
    let encoded = serde_json::to_string(&input).map_err(|error| RuntimeError::new(error.to_string()))?;
    let checked = step.validate_call(&binding.origin, session_id, &binding.origin.call_id,
        tool, &encoded, now_ms)?;
    let outer_key = binding.effect_idempotency_key();
    let identity = canonical_digest("agent-action-batch-member-v1", &serde_json::json!({
        "outer": outer_key, "index": index, "tool": tool, "input": checked,
        "step": binding.snapshot.identity.step_id, "catalog": binding.origin.catalog_digest,
        "session": session_id, "turn": binding.snapshot.identity.turn_id,
        "parent": parent, "attempt": attempt,
    })).map_err(|error| RuntimeError::new(error.to_string()))?;
    let effect = step.lease().describe_effect(tool, &checked);
    Ok(PreparedAgentBatchMember { outer_key, index, tool: tool.into(), input: checked, effect, identity })
}

/// Re-derive the effective input from the same original member after its
/// original policy owner has run PreToolUse once. This function validates
/// the original outer first; its result alone does not authenticate a hook.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_effective_agent_batch_member(
    binding: &StepCallBinding, current: Option<tools::ToolHostLease>, session_id: &str,
    outer_invocation: &harness_contract::tool::GovernedToolInvocation, outer_input: &str,
    parent: &harness_contract::execution_graph::ExecutionParentBinding, attempt: u32,
    index: usize, now_ms: i64, effective_input: &str,
) -> Result<(PreparedAgentBatchMember, PreparedAgentBatchMember), RuntimeError> {
    let original = prepare_agent_batch_member(binding, current.clone(), session_id, outer_invocation,
        outer_input, parent, attempt, index, now_ms)?;
    let step = binding.resolve(current)?;
    let mut input = harness_contract::step::parse_strict_json(effective_input)
        .map_err(|error| RuntimeError::new(error.to_string()))?;
    let object = input.as_object_mut().ok_or_else(|| RuntimeError::new("effective member input is not an object"))?;
    for name in ["reference", "evidence_ref"] {
        if let Some(raw) = object.get(name).and_then(serde_json::Value::as_str) {
            object.insert(name.into(), serde_json::Value::String(crate::normalize_bare_evidence_ref(raw)));
        }
    }
    for name in ["evidence_refs", "result_refs", "artifact_refs"] {
        if let Some(items) = object.get_mut(name).and_then(serde_json::Value::as_array_mut) {
            for item in items { if let Some(raw) = item.as_str() {
                *item = serde_json::Value::String(crate::normalize_bare_evidence_ref(raw));
            } }
        }
    }
    let encoded = serde_json::to_string(&input).map_err(|error| RuntimeError::new(error.to_string()))?;
    let checked = step.validate_call(&binding.origin, session_id, &binding.origin.call_id,
        &original.tool, &encoded, now_ms)?;
    let before_resolution = step.lease().describe_effect_resolution(&original.tool, &original.input);
    let after_resolution = step.lease().describe_effect_resolution(&original.tool, &checked);
    let mut before_invocation = step.lease().prepare_governed_invocation(
        &binding.origin.call_id, &original.tool, &original.input, &[]);
    let after_invocation = step.lease().prepare_governed_invocation(
        &binding.origin.call_id, &original.tool, &checked, &[]);
    // Input is the only allowed difference: the pinned owner independently
    // re-derives resource demand, dependency and catalog facts as well as
    // descriptor and proof scope purposes before preserving the plan.
    before_invocation.intent.normalized_input = after_invocation.intent.normalized_input.clone();
    if before_resolution != after_resolution || before_invocation != after_invocation
        || after_invocation.effect != original.effect {
        return Err(RuntimeError::new("effective member requires replan"));
    }
    let effect = after_resolution.descriptor;
    let identity = canonical_digest("agent-action-batch-member-v1", &serde_json::json!({
        "outer": original.outer_key, "index": index, "tool": original.tool, "input": checked,
        "step": binding.snapshot.identity.step_id, "catalog": binding.origin.catalog_digest,
        "session": session_id, "turn": binding.snapshot.identity.turn_id,
        "parent": parent, "attempt": attempt,
    })).map_err(|error| RuntimeError::new(error.to_string()))?;
    let effective = PreparedAgentBatchMember { outer_key: original.outer_key.clone(), index,
        tool: original.tool.clone(), input: checked, effect, identity };
    Ok((original, effective))
}

/// One independently issued member lease. Fields stay private so downstream
/// executors can only consume a policy-owner admission through its verifier.
#[derive(Debug)]
pub struct AgentBatchMemberAdmission {
    prepared: PreparedAgentBatchMember,
    index: usize,
    identity: String,
    authorization: harness_contract::tool::ToolExecutionAuthorization,
    claimed: Arc<std::sync::atomic::AtomicBool>,
    live_policy: crate::permissions::SessionExecutionPolicyControl,
    cancellation: crate::CancellationToken,
    fence: Option<crate::SessionExecutionFence>,
    original_session_claim: Option<Arc<crate::execution_core::services::OriginalSessionClaim>>,
    original_session_input: crate::session_input::SessionInputStream,
    original_turn: harness_contract::turn::TurnId,
    control_requested: Arc<crate::conversation::OriginalTurnInputControl>,
    original_events: Vec<crate::DurableRuntimeEvent>,
    original_event_store: Arc<crate::RuntimeEventStore>,
    original_ticket: Option<crate::execution_core::graph::NodeExecutionTicket>,
    commit_control: Arc<crate::conversation::MemberCommitControl>,
}

/// Process-local evidence from the original member owner. Not serializable or
/// constructible by callers; Session claim and checked commit remain owned by
/// the original action/transaction owner.
pub struct OriginalMemberCommitCapability {
    admission: AgentBatchMemberAdmission,
    original: crate::RuntimeToolExecutionRequest,
    pinned: tools::ToolHostLease,
}

/// A same-call, non-consuming candidate. Only the original Host or Process
/// producer can supply the private member Arc and the matching E1 authority.
/// Possession is not admission, a Fresh decision, or a receipt.
pub struct OriginalPreparedMemberCapability {
    member: crate::authorization_negotiator::PreparedMemberBinding,
    authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
    reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
    // Supplied only by the original effect/backend owner. It must represent
    // durable cross-process exclusivity, not a query or process-local lock.
    effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
}

pub enum OriginalPreparedMemberCancellation {
    Cancelled,
    Unknown(crate::tool_host::OriginalStopPublication),
}

impl OriginalPreparedMemberCapability {
    pub(crate) fn from_host(
        member: Arc<crate::conversation::OriginalHostPreparedMember>,
        authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<Self, String> {
        Self::from_binding(crate::authorization_negotiator::PreparedMemberBinding::from_host(member), authority, reservation, effect_fence)
    }

    pub(crate) fn from_process(
        member: Arc<crate::agent_in_process_worker::OriginalProcessPreparedMember>,
        authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<Self, String> {
        Self::from_binding(crate::authorization_negotiator::PreparedMemberBinding::from_process(member)?, authority, reservation, effect_fence)
    }

    fn from_binding(
        member: crate::authorization_negotiator::PreparedMemberBinding,
        authority: Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
        reservation: Arc<crate::runtime_event_store::OriginalMemberReservation>,
        effect_fence: Arc<crate::runtime_event_store::OriginalMemberEffectFence>,
    ) -> Result<Self, String> {
        // E1's equality compares the private Host/Process prepared Arc, not
        // the serialized fields of a prepared member.
        let same = authority.member().matches(&member) && match (member.host(), member.process()) {
            (Some(host), None) => Arc::ptr_eq(host.reservation(), &reservation),
            (None, Some(_)) => true,
            _ => false,
        };
        if !same {
            return Err("prepared member authority has a different original owner".into());
        }
        Ok(Self { member, authority, reservation, effect_fence })
    }

    pub(crate) fn prepared(&self) -> &PreparedAgentBatchMember { self.member.prepared() }

    pub fn normalized_member_input(&self) -> &serde_json::Value { &self.member.prepared().input }
    pub fn index(&self) -> usize { self.member.prepared().index }
    pub fn tool(&self) -> &str { &self.member.prepared().tool }
    /// The staged member lease's own ceiling. This is not an admission or
    /// proof of commit and never falls back to the outer batch authorization.
    pub fn member_permission_ceiling(&self) -> Result<harness_contract::policy::PermissionMode, String> {
        Ok(self.authority.staged_authorization_for_original_action()?.authorization.authorization_lease.ceiling)
    }
    pub(crate) fn original_reservation(&self) -> Arc<crate::runtime_event_store::OriginalMemberReservation> {
        Arc::clone(&self.reservation)
    }
    pub(crate) fn original_effect_fence(&self) -> Arc<crate::runtime_event_store::OriginalMemberEffectFence> {
        Arc::clone(&self.effect_fence)
    }
    pub fn coordinate(&self) -> crate::tool_host::OriginalBatchMemberCoordinate {
        let (request, ticket) = if let Some(host) = self.member.host() { (host.original_request(), host.ticket()) }
            else if let Some(process) = self.member.process() { (process.original_request(), process.ticket()) }
            else { unreachable!("private binding always has an original member") };
        crate::tool_host::OriginalBatchMemberCoordinate::from_original(request, ticket, self.member.prepared().index)
    }
    /// Used when the original member cannot reach its checked outcome. This
    /// consumes the candidate and never claims refusal or rollback.
    pub fn stop_before_outcome_unknown(self, detail: String) -> crate::tool_host::OriginalStopPublication {
        crate::tool_host::OriginalBatchMemberStop::publish_with_owners(self.coordinate(),
            crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(detail), Arc::clone(&self.reservation), Arc::clone(&self.effect_fence))
    }
    pub fn stop_before_outcome_error(self, error: crate::AgentActionServiceError) -> crate::tool_host::OriginalStopPublication {
        crate::tool_host::OriginalBatchMemberStop::publish_with_owners(self.coordinate(),
            crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeError(error), Arc::clone(&self.reservation), Arc::clone(&self.effect_fence))
    }
    /// Explicit pre-submit cancellation only. The original effect owner must
    /// first confirm durable cancellation; a failed or unknown result retains
    /// both original owners and is never treated as rollback.
    pub fn cancel_before_submit(self) -> OriginalPreparedMemberCancellation {
        // The stop producer asks the original backend for a winner or a
        // durable noncommit. The carrier stays mandatory until physical closure;
        // even a confirmed decision is not a standalone Cancelled acknowledgement.
        OriginalPreparedMemberCancellation::Unknown(
            crate::tool_host::OriginalBatchMemberStop::publish_with_owners(
                self.coordinate(),
                crate::tool_host::OriginalBatchMemberStopReason::BeforeOutcomeUnknown(
                    "original member cancelled before submit".into()),
                Arc::clone(&self.reservation), Arc::clone(&self.effect_fence)))
    }

    pub(crate) fn matches_action_envelope(&self, envelope: &harness_contract::agent_action::AgentActionEnvelope)
        -> Result<(), RuntimeError> {
        // The private candidate already carries the original Host or Process
        // producer. Both must match the same effective hook-derived member;
        // a missing Process hook is Unknown, never a Host authorization.
        let (original, claim, prepared) = if let Some(host) = self.member.host() {
            (host.original_request(), host.claim().as_ref(), host.prepared())
        } else if let Some(process) = self.member.process() {
            let effective = process.effective_prepared().ok_or_else(|| RuntimeError::new("original Process effective member unavailable"))?;
            (process.original_request(), process.claim().as_ref(), effective)
        } else {
            return Err(RuntimeError::new("original member owner unavailable"));
        };
        if prepared != self.member.prepared() { return Err(RuntimeError::new("original effective member differs")); }
        let parent = original.parent_execution.as_ref().ok_or_else(|| RuntimeError::new("original action parent unavailable"))?;
        let attempt = original.parent_execution_attempt.ok_or_else(|| RuntimeError::new("original action attempt unavailable"))?;
        let physical = envelope.actor.original_execution.as_ref().ok_or_else(|| RuntimeError::new("original action execution unavailable"))?;
        if envelope.actor.session_id != claim.session_id() || envelope.actor.turn_id != claim.turn_id()
            || physical.graph_id != parent.execution_id || physical.node_id != parent.node_id || physical.attempt != attempt
            || envelope.action_id != format!("{}:{}", original.idempotency_key, prepared.index) {
            return Err(RuntimeError::new("original member action identity mismatch"));
        }
        let mut input = prepared.input.clone();
        let object = input.as_object_mut().ok_or_else(|| RuntimeError::new("original member input is not an object"))?;
        let revision = match object.remove("expected_revision") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::Number(number)) => Some(number.as_u64().ok_or_else(|| RuntimeError::new("original member revision invalid"))?),
            _ => return Err(RuntimeError::new("original member revision invalid")),
        };
        if prepared.tool == "state_inspect" && (object.contains_key("entry_ref") || object.contains_key("scope_ref")) {
            object.remove("page_cursor"); object.remove("query");
        }
        let action: harness_contract::agent_action::AgentAction = serde_json::from_value(serde_json::json!({
            "kind": prepared.tool, "input": input,
        })).map_err(|error| RuntimeError::new(error.to_string()))?;
        if envelope.expected_revision != revision || envelope.action != action {
            return Err(RuntimeError::new("original member action payload mismatch"));
        }
        Ok(())
    }

    /// Acquire the original approval and lease owners before the complete
    /// ordered source set. Approval waits and all other awaits must finish
    /// before entering this synchronous decision. `append` must validate the
    /// original policy/claim/control and perform the checked action append;
    /// it must not acquire approval or lease locks again.
    pub(crate) fn commit_checked(
        self, services: &crate::RuntimeServices, additional_streams: &[String],
        append: impl FnOnce(&crate::runtime_event_store::HeldStreamLocks<'_>,
            &[crate::ExpectedStreamRevision],
            &crate::authorization_negotiator::PreparedMemberBinding,
            Arc<crate::authorization_negotiator::PreparedMemberAuthority>,
            &crate::runtime_event_store::OriginalMemberReservation,
            &crate::runtime_event_store::OriginalMemberEffectFence) -> Result<Arc<crate::OriginalCommittedMember>, crate::AgentActionServiceError>,
    ) -> Result<(Arc<crate::OriginalCommittedMember>, bool), crate::AgentActionServiceError> {
        if !Arc::ptr_eq(self.member.store(), services.event_store())
            || !self.authority.member().matches(&self.member) {
            return Err(crate::AgentActionServiceError::Corrupt("prepared member store or original owner changed".into()));
        }
        let outer = &self.member.prepared().outer_key;
        if outer.is_empty() { return Err(crate::AgentActionServiceError::Corrupt("prepared member outer missing".into())); }
        let mut streams = additional_streams.to_vec();
        streams.push(format!("execution-effect:{outer}"));
        streams.extend(self.authority.sources().map(|source| source.stream_id.clone()));
        streams.sort();
        streams.dedup();
        let (request, policy, ticket) = if let Some(host) = self.member.host() {
            (host.original_request(), host.original_policy().execution_policy_control(), host.ticket())
        } else if let Some(process) = self.member.process() {
            (process.original_request(), process.owner().policy().execution_policy_control(), process.ticket())
        } else {
            return Err(crate::AgentActionServiceError::Corrupt("prepared member owner missing".into()));
        };
        let session = request.session_id.as_deref().filter(|value| !value.is_empty())
            .ok_or_else(|| {
                crate::AgentActionServiceError::Corrupt("prepared member Session missing".into())
            })?;
        let revision = request.policy_revision;
        if revision == 0 { return Err(crate::AgentActionServiceError::Corrupt("prepared member policy missing".into())); }
        if request.idempotency_key != *outer { return Err(crate::AgentActionServiceError::Corrupt("prepared outer differs from request".into())); }
        streams.push(ticket.graph_id.clone());
        if let Some(parent) = &request.parent_execution { streams.push(parent.execution_id.clone()); }
        streams.sort();
        streams.dedup();
        let authority = Arc::clone(&self.authority);
        let result = authority.with_original_owners_for_checked_commit(services, &self.member, |_| {
            services.with_original_session_policy(session, &policy, |current| {
                if current.revision != revision {
                    return Err("prepared member policy changed".into());
                }
                services.event_store().with_authenticated_stream_locks(&streams, |held| {
                use sha2::Digest as _;
                let invocation = request.compiled_invocation.as_ref().ok_or_else(|| {
                    "original outer invocation missing"
                })?;
                let read = invocation.effect.effect_kind == harness_contract::tool::ToolEffectKind::Read;
                let effect_stream = format!("execution-effect:{outer}");
                let key = format!("{outer}:{}", if read { "read-intent" } else { "intent" });
                let intent = services.event_store().event_by_idempotency_key(&effect_stream, &key)
                    .map_err(|error| {
                        error.to_string()
                    })?.ok_or_else(|| {
                        "original outer intent unavailable"
                    })?;
                let receipt = services.event_store().original_transaction_receipt(&intent.transaction_id)
                    .map_err(|error| {
                        error.to_string()
                    })?.ok_or_else(|| {
                        "original outer intent receipt unavailable"
                    })?;
                let replay = crate::execution_core::graph::receipt_replay_contract(request)
                    .map_err(|error| {
                        error.to_string()
                    })?;
                if intent.stream_id != effect_stream || intent.sequence != 1
                    || intent.scope != crate::RuntimeEventScope::ExecutionNode
                    || intent.kind != if read { "execution.read.intent" } else { "execution.effect.intent" }
                    || intent.actor.as_deref() != Some("governed_tool") || intent.status.as_deref() != Some("inflight")
                    || intent.idempotency_key.as_deref() != Some(key.as_str()) || intent.schema_version != 1
                    || intent.payload["idempotency_key"] != request.idempotency_key
                    || intent.payload["tool_use_id"] != request.tool_use_id || intent.payload["tool_name"] != request.tool_name
                    || intent.payload["input_sha256"] != format!("sha256:{:x}", sha2::Sha256::digest(request.input.as_bytes()))
                    || intent.payload["effect"] != serde_json::to_value(&invocation.effect).map_err(|error| {
                        error.to_string()
                    })?
                    || intent.payload["replay"] != replay
                    || receipt.transaction_id != intent.transaction_id || receipt.commit_cursor != intent.commit_cursor
                    || receipt.event_ids.get(intent.transaction_index as usize) != Some(&intent.event_id)
                    || !receipt.stream_revisions.iter().any(|source| source.stream_id == effect_stream
                        && source.expected_revision == 0 && source.committed_revision == 1)
                {
                    return Err("original outer intent mismatch".into());
                }
                let mut sources = Vec::with_capacity(streams.len());
                for stream_id in &streams {
                    let expected_revision = services.event_store().stream_revision(stream_id)
                        .map_err(|error| {
                            error.to_string()
                        })?;
                    sources.push(crate::ExpectedStreamRevision { stream_id: stream_id.clone(), expected_revision });
                }
                if self.authority.sources().any(|expected| !sources.iter().any(|actual| actual == expected)) {
                    return Err("prepared authority source changed under lock".into());
                }
                let index = u64::try_from(self.member.prepared().index).map_err(|_| {
                    "original member index overflow"
                })?;
                if !self.reservation.matches_original(services.event_store(), outer, index, ticket) {
                    return Err("original member reservation changed before checked append".into());
                }
                // Preserve the action error, including an uncertain append,
                // without converting it into evidence of rollback.
                Ok(append(held, &sources, &self.member, Arc::clone(&self.authority), &self.reservation, &self.effect_fence))
                })
            }).map_err(crate::AgentActionServiceError::Corrupt)?
        });
        result
    }
}

impl AgentBatchMemberAdmission {
    /// The member input prepared by the original owner from the bound outer
    /// call and pinned schema. This value does not itself grant admission.
    pub fn normalized_member_input(&self) -> &serde_json::Value {
        &self.prepared.input
    }
    /// The original member lease ceiling, not the outer batch lease ceiling.
    pub fn member_permission_ceiling(&self) -> harness_contract::policy::PermissionMode {
        self.authorization.authorization_lease.ceiling
    }
    pub fn into_commit_capability(
        self, original: &crate::RuntimeToolExecutionRequest, pinned: tools::ToolHostLease,
    ) -> Result<OriginalMemberCommitCapability, RuntimeError> {
        if !self.claimed.load(std::sync::atomic::Ordering::Acquire) || self.original_ticket.is_none()
            || self.fence.is_none() || self.original_session_claim.is_none() || original.tool_name != "agent_action_batch"
        { return Err(RuntimeError::new("original verified member or Session fence unavailable")); }
        let binding = original.step_binding.as_ref()
            .ok_or_else(|| RuntimeError::new("original member Step unavailable"))?;
        let session = original.session_id.as_deref()
            .ok_or_else(|| RuntimeError::new("original member Session unavailable"))?;
        let invocation = original.compiled_invocation.as_ref()
            .ok_or_else(|| RuntimeError::new("original member invocation unavailable"))?;
        let parent = original.parent_execution.as_ref()
            .ok_or_else(|| RuntimeError::new("original member parent unavailable"))?;
        let attempt = original.parent_execution_attempt
            .ok_or_else(|| RuntimeError::new("original member attempt unavailable"))?;
        let member = prepare_agent_batch_member(
            binding, Some(pinned.clone()), session, invocation, &original.input,
            parent, attempt, self.index, chrono::Utc::now().timestamp_millis(),
        )?;
        if member.identity != self.identity || member.index != self.prepared.index
            || member.tool != self.prepared.tool || member.input != self.prepared.input
            || member.effect != self.prepared.effect || member.outer_key != self.prepared.outer_key
            || original.idempotency_key != member.outer_key
            || original.tool_use_id != binding.origin.call_id
            || self.cancellation.is_cancelled()
            || self.control_requested.load(std::sync::atomic::Ordering::Acquire)
            || self.live_policy.revision() != self.authorization.policy_revision
        {
            return Err(RuntimeError::new("original member changed before capability transfer"));
        }
        Ok(OriginalMemberCommitCapability { admission: self, original: original.clone(), pinned })
    }

    /// Original producer-prepared fields for a future locked verifier; this
    /// is evidence, not permission to execute or a transaction guard.
    pub(crate) fn original_preparation(&self) -> &PreparedAgentBatchMember { &self.prepared }
    pub(crate) fn member_authorization(&self) -> &harness_contract::tool::ToolExecutionAuthorization { &self.authorization }
    pub(crate) fn bind_original_ticket(&mut self, ticket: crate::execution_core::graph::NodeExecutionTicket) -> Result<(), RuntimeError> {
        if self.original_ticket.is_some() { return Err(RuntimeError::new("original member ticket already bound")); }
        self.original_ticket = Some(ticket); Ok(())
    }
    pub(crate) fn original_ticket(&self) -> Result<&crate::execution_core::graph::NodeExecutionTicket, RuntimeError> {
        self.original_ticket.as_ref().ok_or_else(|| RuntimeError::new("original member ticket unavailable"))
    }
    pub(crate) fn commit_control(&self) -> Arc<crate::conversation::MemberCommitControl> {
        Arc::clone(&self.commit_control)
    }

    /// Original writer evidence for the exact member lease. This is not a
    /// commit guard: a consumer must still authenticate and fence every source.
    pub(crate) fn original_member_events(&self) -> Result<Vec<&crate::DurableRuntimeEvent>, RuntimeError> {
        let lease_id = self.authorization.authorization_lease.lease_id.as_str();
        let relevant = self.original_events.iter().filter(|event| match event.kind.as_str() {
            "authorization.capability_assessed" => event.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str) == Some(lease_id),
            "authorization.lease_transition" => event.payload.pointer("/lease/lease_id").and_then(serde_json::Value::as_str) == Some(lease_id),
            "approval.grant_consumed" => true,
            _ => false,
        }).collect::<Vec<_>>();
        if !relevant.iter().any(|event| event.kind == "authorization.capability_assessed")
            || !relevant.iter().any(|event| event.kind == "authorization.lease_transition")
        { return Err(RuntimeError::new("original member assessment or lease event unavailable")); }
        Ok(relevant)
    }

    pub(crate) fn from_owner(
        member: &PreparedAgentBatchMember,
        authorization: harness_contract::tool::ToolExecutionAuthorization,
        live_policy: crate::permissions::SessionExecutionPolicyControl,
        cancellation: crate::CancellationToken,
        fence: Option<crate::SessionExecutionFence>,
        original_session_claim: Option<Arc<crate::execution_core::services::OriginalSessionClaim>>,
        original_session_input: crate::session_input::SessionInputStream,
        original_turn: harness_contract::turn::TurnId,
        control_requested: Arc<crate::conversation::OriginalTurnInputControl>,
        original_events: Vec<crate::DurableRuntimeEvent>,
        original_event_store: Arc<crate::RuntimeEventStore>,
    ) -> Result<Self, RuntimeError> {
        if authorization.tool_id != member.tool
            || authorization.request_id != member.identity
            || authorization.authorization_lease.idempotency_key != member.identity
            || authorization.policy_revision == 0
            || authorization.authorization_lease.policy_revision != authorization.policy_revision
        { return Err(RuntimeError::new("batch member policy result identity mismatch")); }
        let commit_control = cancellation.member_commit_control();
        control_requested.register(&commit_control);
        Ok(Self { prepared: member.clone(), index: member.index, identity: member.identity.clone(), authorization,
            claimed: Arc::new(std::sync::atomic::AtomicBool::new(false)), live_policy, cancellation, fence, original_session_claim, original_session_input, original_turn, control_requested, original_events, original_event_store,
            original_ticket: None, commit_control })
    }

    /// Verify against the exact outer request and live policy revision. The
    /// pinned ToolHost verifies the actual signed member lease and descriptor.
    pub async fn verify(
        &self,
        binding: &StepCallBinding,
        lease: tools::ToolHostLease,
        session_id: &str,
        invocation: &harness_contract::tool::GovernedToolInvocation,
        outer_input: &str,
        parent: &harness_contract::execution_graph::ExecutionParentBinding,
        attempt: u32,
        index: usize,
        actual_tool: &str,
        actual_input: &serde_json::Value,
        policy_revision: u64,
        now_ms: i64,
    ) -> Result<(), RuntimeError> {
        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) || self.live_policy.revision() != policy_revision {
            return Err(RuntimeError::new("batch member owner no longer live"));
        }
        if let Some(fence) = &self.fence {
            fence.verify(crate::SessionExecutionFencePhase::ToolExecution).await.map_err(RuntimeError::new)?;
        }
        let member = prepare_agent_batch_member(binding, Some(lease.clone()), session_id,
            invocation, outer_input, parent, attempt, index, now_ms)?;
        if self.index != index || self.identity != member.identity
            || member.tool != actual_tool || member.input != *actual_input
            || self.authorization.request_id != member.identity
            || self.authorization.authorization_lease.idempotency_key != member.identity
            || self.authorization.tool_id != member.tool
            || policy_revision == 0 || self.authorization.policy_revision != policy_revision
            || self.authorization.authorization_lease.policy_revision != policy_revision
        { return Err(RuntimeError::new("batch member admission mismatch")); }
        lease.validate_authorization(&self.authorization, &member.tool, &member.input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if self.cancellation.is_cancelled() || self.control_requested.load(std::sync::atomic::Ordering::Acquire) || self.live_policy.revision() != policy_revision {
            return Err(RuntimeError::new("batch member owner changed during verification"));
        }
        self.claimed.compare_exchange(false, true, std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire)
            .map_err(|_| RuntimeError::new("batch member admission already consumed"))?;
        Ok(())
    }
}

impl OriginalMemberCommitCapability {
    /// Match the actual semantic action against the exact normalized member
    /// input and the original physical attempt; no caller-provided scope or
    /// action identifier is accepted as a substitute for this binding.
    pub(crate) fn matches_action_envelope(
        &self, envelope: &harness_contract::agent_action::AgentActionEnvelope,
    ) -> Result<(), RuntimeError> {
        let claim = self.original_session_claim()?;
        let parent = self.original.parent_execution.as_ref().ok_or_else(|| RuntimeError::new("original action parent unavailable"))?;
        let attempt = self.original.parent_execution_attempt.ok_or_else(|| RuntimeError::new("original action attempt unavailable"))?;
        let physical = envelope.actor.original_execution.as_ref().ok_or_else(|| RuntimeError::new("original action execution unavailable"))?;
        if envelope.actor.session_id != claim.session_id() || envelope.actor.turn_id != claim.turn_id()
            || physical.graph_id != parent.execution_id || physical.node_id != parent.node_id || physical.attempt != attempt
            || envelope.action_id != format!("{}:{}", self.original.idempotency_key, self.admission.prepared.index)
        { return Err(RuntimeError::new("original member action identity mismatch")); }
        let mut input = self.admission.prepared.input.clone();
        let object = input.as_object_mut().ok_or_else(|| RuntimeError::new("original member input is not an object"))?;
        let revision = match object.remove("expected_revision") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::Number(number)) => Some(number.as_u64().ok_or_else(|| RuntimeError::new("original member revision invalid"))?),
            _ => return Err(RuntimeError::new("original member revision invalid")),
        };
        if self.admission.prepared.tool == "state_inspect" && (object.contains_key("entry_ref") || object.contains_key("scope_ref")) {
            object.remove("page_cursor"); object.remove("query");
        }
        let action: harness_contract::agent_action::AgentAction = serde_json::from_value(serde_json::json!({
            "kind": self.admission.prepared.tool, "input": input,
        })).map_err(|error| RuntimeError::new(error.to_string()))?;
        if envelope.expected_revision != revision || envelope.action != action {
            return Err(RuntimeError::new("original member action payload mismatch"));
        }
        Ok(())
    }

    /// Names only. Callers must union these with the Program, Goal and other
    /// original sources before acquiring the original sorted stream locks.
    pub(crate) fn original_session_claim(&self) -> Result<&crate::execution_core::services::OriginalSessionClaim, RuntimeError> {
        self.admission.original_session_claim.as_deref().ok_or_else(|| RuntimeError::new("original Session claim unavailable"))
    }

    pub(crate) fn source_streams(&self) -> Result<Vec<String>, RuntimeError> {
        let mut streams = BTreeSet::new();
        streams.insert(self.admission.original_ticket()?.graph_id.clone());
        // A member index is appended by the action transaction to the same
        // effect stream owned by the original outer call. Lock that exact
        // stream before reading its revision and before the checked append;
        // an absent index is not evidence of a fresh member.
        let outer_key = &self.admission.prepared.outer_key;
        if outer_key.is_empty() || outer_key != &self.original.idempotency_key {
            return Err(RuntimeError::new("original member outer effect identity mismatch"));
        }
        streams.insert(format!("execution-effect:{outer_key}"));
        for event in self.admission.original_member_events()? {
            if event.stream_id.is_empty() { return Err(RuntimeError::new("member evidence stream missing")); }
            streams.insert(event.stream_id.clone());
        }
        Ok(streams.into_iter().collect())
    }

    /// Recheck original call and exact writer events under the caller's full
    /// source lock set. This produces source cuts, not permission to append;
    /// the action owner must still fence Session claim and checked commit.
    fn validate_locked_sources(
        &self, store: &crate::RuntimeEventStore, now_ms: i64,
    ) -> Result<Vec<crate::ExpectedStreamRevision>, RuntimeError> {
        let ticket = self.admission.original_ticket()?;
        crate::execution_core::graph::validate_node_ticket_start(store, ticket, None)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        let mut matching = calls.iter().filter(|call| call.id == self.original.tool_use_id);
        let call = matching.next().ok_or_else(|| RuntimeError::new("original ticket call absent"))?;
        let binding = self.original.step_binding.as_ref().ok_or_else(|| RuntimeError::new("original Step absent"))?;
        if matching.next().is_some() || call.name != "agent_action_batch"
            || call.binding.as_ref() != Some(binding)
            || harness_contract::step::parse_strict_json(&call.input).ok()
                != harness_contract::step::parse_strict_json(&self.original.input).ok()
            || harness_contract::step::parse_strict_json(&call.input).is_err()
        { return Err(RuntimeError::new("original ticket call mismatch")); }
        // Transfer already authenticated the immutable original member and
        // pinned catalog. Only its time-sensitive Step and authorization need
        // checking at the commit decision; do not reparse the outer schema.
        if now_ms >= binding.snapshot.expires_at_ms { return Err(RuntimeError::new("step_origin_expired")); }
        let member = &self.admission.prepared;
        self.pinned.validate_authorization(&self.admission.authorization, &member.tool, &member.input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        if self.admission.cancellation.is_cancelled() || self.admission.control_requested.load(std::sync::atomic::Ordering::Acquire)
        { return Err(RuntimeError::new("original member owner is no longer live")); }
        // These are immutable events captured from the same producer store's
        // successful appends. Source revisions and controls are still fenced
        // below; a different store must enter its own authentication path.
        if !std::ptr::eq(self.admission.original_event_store.as_ref(), store) {
            return Err(RuntimeError::new("original member producer store mismatch"));
        }
        self.admission.original_member_events()?;
        self.source_streams()?.into_iter().map(|stream_id| {
            let expected_revision = store.stream_revision(&stream_id).map_err(|error| RuntimeError::new(error.to_string()))?;
            Ok(crate::ExpectedStreamRevision { stream_id, expected_revision })
        }).collect()
    }

    /// Acquire the same outer policy order as TaskRuntimePort, then acquire
    /// the complete union of source locks exactly once. The closure must use
    /// the transaction owner's *locked* checked+cancellable append; it must
    /// never reacquire these locks or await. Additional names add no authority;
    /// their current revisions are read only after acquiring the union of locks.
    /// Session claim remains an original backend check in the same transaction.
    pub(crate) fn with_locked_policy<T>(
        self, services: &crate::RuntimeServices, envelope: &harness_contract::agent_action::AgentActionEnvelope,
        additional_streams: &[String], now_ms: i64,
        append: impl FnOnce(&crate::runtime_event_store::HeldStreamLocks<'_>, &[crate::ExpectedStreamRevision], Arc<crate::conversation::MemberCommitControl>, &crate::execution_core::services::OriginalSessionClaim, usize,
            &PreparedAgentBatchMember, &harness_contract::tool::ToolExecutionAuthorization,
            &crate::execution_core::graph::NodeExecutionTicket, &[OriginalMemberEventRef]) -> Result<T, String>,
    ) -> Result<T, String> {
        self.matches_action_envelope(envelope).map_err(|error| error.to_string())?;
        let session_id = self.original.session_id.as_deref().ok_or("original Session absent")?;
        services.with_original_session_policy(session_id, &self.admission.live_policy, |policy| {
            if policy.revision != self.admission.authorization.policy_revision {
                return Err("original member policy changed".into());
            }
            let mut streams = self.source_streams().map_err(|error| error.to_string())?;
            streams.extend(additional_streams.iter().cloned());
            streams.sort();
            streams.dedup();
            services.event_store().with_authenticated_stream_locks(&streams, |held| {
                // The outer mutation owner writes the first intent before it
                // enters Gateway. An index must never create the effect stream
                // first: root intent provenance requires sequence one.
                let outer_key = &self.admission.prepared.outer_key;
                let effect_stream = format!("execution-effect:{outer_key}");
                let intent_key = format!("{outer_key}:intent");
                let intent = services.event_store().event_by_idempotency_key(&effect_stream, &intent_key)
                    .map_err(|error| error.to_string())?.ok_or("original outer effect intent unavailable")?;
                let receipt = services.event_store().original_transaction_receipt(&intent.transaction_id)
                    .map_err(|error| error.to_string())?.ok_or("original outer intent receipt unavailable")?;
                let effect = &self.original.compiled_invocation.as_ref().ok_or("original outer effect missing")?.effect;
                if intent.stream_id != effect_stream || intent.sequence != 1
                    || intent.kind != "execution.effect.intent" || intent.scope != crate::RuntimeEventScope::ExecutionNode
                    || intent.actor.as_deref() != Some("governed_tool") || intent.status.as_deref() != Some("inflight")
                    || intent.schema_version != 1 || intent.idempotency_key.as_deref() != Some(intent_key.as_str())
                    || intent.payload["idempotency_key"].as_str() != Some(outer_key.as_str())
                    || intent.payload["effect"] != serde_json::to_value(effect).map_err(|error| error.to_string())?
                    || receipt.transaction_id != intent.transaction_id || receipt.commit_cursor != intent.commit_cursor
                    || receipt.event_ids.get(intent.transaction_index as usize) != Some(&intent.event_id)
                { return Err("original outer effect intent identity mismatch".into()); }
                let validated = self.validate_locked_sources(services.event_store(), now_ms).map_err(|error| error.to_string())?;
                let mut sources = Vec::with_capacity(streams.len());
                for stream_id in &streams {
                    let expected_revision = services.event_store().stream_revision(stream_id)
                        .map_err(|error| error.to_string())?;
                    if validated.iter().any(|source| source.stream_id == *stream_id
                        && source.expected_revision != expected_revision)
                    {
                        return Err("original member source changed under lock".into());
                    }
                    sources.push(crate::ExpectedStreamRevision {
                        stream_id: stream_id.clone(), expected_revision,
                    });
                }
                let claim = self.original_session_claim().map_err(|error| error.to_string())?;
                let turn = &self.admission.original_turn;
                if turn.as_str() != claim.turn_id()
                    || self.admission.original_session_input.active_turn_id().as_ref() != Some(turn)
                { return Err("original Session active turn changed".into()); }
                let fence = claim.fence()?;
                let cursor = self.admission.original_session_input.highest_consumed_cursor(turn);
                if self.admission.original_session_input.active_turn_id().as_ref() != Some(turn) {
                    return Err("original Session active turn changed while reading cursor".into());
                }
                let consumed_input_sequence = match cursor {
                    Some(cursor) if cursor.generation == fence.session_generation => {
                        usize::try_from(cursor.sequence)
                            .map_err(|_| "original consumed input sequence overflow")?
                            .max(fence.input_sequence)
                    }
                    Some(_) => return Err("original consumed cursor generation mismatch".into()),
                    None => fence.input_sequence,
                };
                let mut original_events = self.admission.original_member_events()
                    .map_err(|error| error.to_string())?.into_iter().map(|event| OriginalMemberEventRef {
                        stream_id: event.stream_id.clone(), sequence: event.sequence, event_id: event.event_id.clone(),
                        scope: event.scope, kind: event.kind.clone(), schema_version: event.schema_version,
                        transaction_id: event.transaction_id.clone(), transaction_index: event.transaction_index,
                        commit_cursor: event.commit_cursor, idempotency_key: event.idempotency_key.clone(),
                    }).collect::<Vec<_>>();
                original_events.sort_by(|left, right| (&left.stream_id, left.sequence, &left.event_id)
                    .cmp(&(&right.stream_id, right.sequence, &right.event_id)));
                append(held, &sources, self.admission.commit_control(), claim, consumed_input_sequence, &self.admission.prepared,
                    &self.admission.authorization, self.admission.original_ticket().map_err(|error| error.to_string())?, &original_events)
            })
        })
    }

}

#[cfg(test)]
mod member_facts_dedup_tests {
    use super::*;

    /// The Step snapshot the persisted batch payload carries.
    fn a8_step_snapshot() -> serde_json::Value {
        serde_json::json!({
            "identity": {"attempt": 1, "parent": "original", "generation": 3, "step_id": "step-413"},
            "tools": [{"permissions": {"allow_write": false}, "purpose": "business",
                "schema": "complete schema ".repeat(1200)}],
            "catalog_digest": "catalog-413",
            "related_revisions": {"goal": "7"},
        })
    }

    /// The persisted batch payload one member ticket names.
    fn a8_batch(session_id: &str) -> String {
        encode_persisted_tool_batch(&serde_json::json!({
            "session_id": session_id,
            "step_snapshots": {"step-413": a8_step_snapshot()},
            "calls": [{"origin": {"origin_step_id": "step-413"}, "id": "call-413",
                "name": "state_inspect", "input": "{}", "depends_on": []}],
            "continue_with_tool_batch": false,
        }))
        .expect("tool batch")
    }

    /// The persisted batch payload of a batch with `members` members.
    fn a8_batch_with_members(session_id: &str, members: usize) -> String {
        let calls: Vec<serde_json::Value> = (0..members)
            .map(|index| serde_json::json!({
                "origin": {"origin_step_id": "step-413"},
                "id": format!("call-413-{index}"),
                "name": "state_inspect",
                "input": "member input ".repeat(256),
                "depends_on": [],
            }))
            .collect();
        encode_persisted_tool_batch(&serde_json::json!({
            "session_id": session_id,
            "step_snapshots": {"step-413": a8_step_snapshot()},
            "calls": calls,
            "continue_with_tool_batch": false,
        }))
        .expect("tool batch")
    }

    /// The original member facts in their inline shape.
    fn a8_member_facts(session_id: &str) -> (serde_json::Value, serde_json::Value) {
        a8_member_facts_for_batch(session_id, a8_batch(session_id))
    }

    /// The original member facts for one member of a named batch.
    fn a8_member_facts_for_batch(
        session_id: &str,
        batch: String,
    ) -> (serde_json::Value, serde_json::Value) {
        let snapshot = a8_step_snapshot();
        let facts = serde_json::json!({
            "prepared": {"outer_key": "outer-413", "index": 0, "tool": "state_inspect",
                "input": {}, "effect": {"kind": "read"}, "identity": "member-413"},
            "authorization": {"tool_id": "state_inspect", "request_id": "member-413"},
            "ticket": {"graph_id": "graph-413", "node_id": "node-413", "executor_kind": "tool_batch",
                "service_class": "foreground", "attempt": 1, "idempotency_key": "ticket-413",
                "payload_ref": batch},
            "original_events": [],
            "original_hook": {"owner": "host",
                "original_prepared": {"identity": "member-413"},
                "effective_prepared": {"identity": "member-413"},
                "hook_effective_input": "{}",
                "permission_context": {"override_decision": null},
                "outer_request": {"tool_name": "agent_action_batch",
                    "step_binding": {"snapshot": snapshot, "origin": {"call_id": "call-413"}}},
                "step": {"snapshot": snapshot, "origin": {"call_id": "call-413"}}},
            "outer_hook": {"ticket": {"graph_id": "graph-413", "node_id": "node-413",
                    "executor_kind": "tool_batch", "service_class": "foreground", "attempt": 1,
                    "idempotency_key": "ticket-413", "payload_ref": batch},
                "step": {"snapshot": snapshot, "origin": {"call_id": "call-413"}},
                "plan_id": "plan-413", "plan_revision": 1, "session_id": session_id,
                "parent": {"execution_id": "graph-413", "node_id": "node-413"}, "attempt": 1,
                "model_input": {}, "effective_input": {}, "invocation": {"intent": {"name": "state_inspect"}}},
        });
        (facts, snapshot)
    }

    /// The same-source rebuild gives a reader the byte identical facts, and the
    /// event no longer carries a second copy of the batch payload.
    #[test]
    fn member_admission_facts_dedupe_and_rebuild_are_byte_identical() {
        let (facts, _) = a8_member_facts("session-413");
        let inline = facts.clone();
        let mut deduped = facts.clone();
        dedupe_member_admission_facts(&mut deduped).expect("dedupe");
        assert!(
            serde_json::to_string(&deduped).unwrap().len()
                < serde_json::to_string(&inline).unwrap().len(),
            "the deduped facts must be smaller than the inline ones"
        );
        // The kept copy is still inline; every other copy points at it.
        assert!(deduped["ticket"]["payload_ref"].is_string());
        assert!(deduped["outer_hook"]["ticket"]["payload_ref"].is_object());
        assert!(deduped["original_hook"]["step"]["snapshot"].is_object());
        // The rebuild is byte identical to the inline shape.
        let rebuilt = rebuild_member_admission_facts(&deduped).expect("rebuild");
        assert_eq!(*rebuilt, inline);
        // The old inline shape reads back unchanged.
        let unchanged = rebuild_member_admission_facts(&inline).expect("rebuild inline");
        assert_eq!(*unchanged, inline);
    }

    /// A pointer that cannot be resolved is an explicit failure: a missing
    /// fact is never read as "no fact".
    #[test]
    fn member_admission_facts_rebuild_refuses_a_broken_pointer() {
        let (facts, _) = a8_member_facts("session-413");
        let mut deduped = facts.clone();
        dedupe_member_admission_facts(&mut deduped).expect("dedupe");

        // A pointer to a Step the batch does not carry is refused.
        let mut unknown_step = deduped.clone();
        unknown_step["original_hook"]["step"]["snapshot"] =
            serde_json::json!({ BATCH_STEP_SNAPSHOT_POINTER: "step-unknown" });
        let error = rebuild_member_admission_facts(&unknown_step).expect_err("unknown Step pointer");
        assert!(error.contains("original member facts missing"), "{error}");

        // A pointer to another Session's batch payload is refused.
        let (foreign, _) = a8_member_facts("session-other");
        let mut foreign_batch = deduped.clone();
        foreign_batch["ticket"]["payload_ref"] = foreign["ticket"]["payload_ref"].clone();
        let error = rebuild_member_admission_facts(&foreign_batch).expect_err("foreign Session");
        assert!(error.contains("original member facts missing"), "{error}");

        // A batch payload whose bytes do not match its digest is refused.
        let mut broken = deduped.clone();
        let mut envelope: serde_json::Value =
            serde_json::from_str(broken["ticket"]["payload_ref"].as_str().unwrap()).unwrap();
        envelope["sha256"] = serde_json::Value::String("sha256:0000".into());
        broken["ticket"]["payload_ref"] = serde_json::Value::String(envelope.to_string());
        let error = rebuild_member_admission_facts(&broken).expect_err("broken digest");
        assert!(error.contains("original member facts missing"), "{error}");
    }

    /// The action event one member of a batch writes, in the production
    /// payload shape.
    fn a8_action_event_payload(session_id: &str, members: usize) -> serde_json::Value {
        let (facts, _) = a8_member_facts_for_batch(
            session_id,
            a8_batch_with_members(session_id, members),
        );
        let mut facts = facts;
        dedupe_member_admission_facts(&mut facts).expect("dedupe");
        serde_json::json!({
            "envelope": {"action_id": "outer-413:0"},
            "entity_ref": "entity-413",
            "member_admission": facts,
        })
    }

    /// The core benefit of the same-source payload: one member's action event
    /// does not grow with the batch it belongs to.
    ///
    /// The batch payload the ticket names carries every member's call, so a
    /// bigger batch makes that one copy bigger. What must not happen is the
    /// event carrying a second copy of that payload, or a copy of the Step
    /// snapshot it carries: the event stays inside one fixed budget, and
    /// doubling the batch does not double it.
    #[test]
    fn agentic_action_event_size_is_almost_independent_of_the_batch_member_count() {
        let small = a8_action_event_payload("session-413", 4);
        let large = a8_action_event_payload("session-413", 8);
        let small_bytes = serde_json::to_vec(&small).expect("encode").len();
        let large_bytes = serde_json::to_vec(&large).expect("encode").len();
        // Doubling the batch does not double the event: the increment is far
        // below the linear growth of one member's facts per member.
        assert!(
            large_bytes < small_bytes + small_bytes / 4,
            "the action event must not grow with the batch member count: {small_bytes} -> {large_bytes}"
        );
        // The event stays inside one fixed budget whatever the batch holds.
        assert!(
            large_bytes < 64 * 1024,
            "one action event must stay inside the fixed budget: {large_bytes}"
        );
        // The bound is discriminating: the inline shape this patch replaced
        // carries the batch payload and its Step snapshot again, and is more
        // than double the deduped event.
        let (inline, _) = a8_member_facts_for_batch(
            "session-413",
            a8_batch_with_members("session-413", 8),
        );
        let inline_bytes = serde_json::to_vec(&serde_json::json!({
            "envelope": {"action_id": "outer-413:0"},
            "entity_ref": "entity-413",
            "member_admission": inline,
        }))
        .expect("encode")
        .len();
        assert!(
            inline_bytes > large_bytes * 2,
            "the inline shape must stay over twice the deduped event, or the bound proves nothing: {inline_bytes} vs {large_bytes}"
        );
    }

    /// A single event that is still over the page budget is not silently
    /// dropped, truncated or rewritten: the writer keeps every byte, and the
    /// reader's explicit failure (the durable page budget) is untouched.
    #[tokio::test]
    async fn an_oversized_action_event_is_written_without_silent_truncation() {
        let store = crate::RuntimeEventStore::for_test();
        let (facts, _) = a8_member_facts("session-413");
        let mut facts = facts;
        dedupe_member_admission_facts(&mut facts).expect("dedupe");
        let mut payload = serde_json::json!({
            "envelope": {"action_id": "outer-413:0"},
            "member_admission": facts,
        });
        payload["oversized"] = serde_json::Value::String("x".repeat(1024 * 1024 + 64));
        let stream_id = "agentic-program:program-oversized";
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: "tx-oversized".into(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: stream_id.into(),
                expected_revision: 0,
            }],
            events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream_id.into(),
                    scope: crate::RuntimeEventScope::Program,
                    kind: "agentic.action_applied".into(),
                    status: Some("applied".into()),
                    actor: Some("agent".into()),
                    refs: Vec::new(),
                    payload: payload.clone(),
                },
                idempotency_key: Some("action-oversized".into()),
                schema_version: 1,
            }],
        })
        .expect("append oversized action event");
        let stored = store
            .event_by_idempotency_key(stream_id, "action-oversized")
            .expect("read oversized action event")
            .expect("oversized action event present");
        // Every byte is kept: the writer neither drops nor rewrites the event.
        assert_eq!(stored.payload, payload);
    }

    /// The producer writes the deduped action event and the reader reads the
    /// byte identical facts back from the real durable event.
    #[tokio::test]
    async fn member_admission_action_event_reads_back_byte_identical_after_dedupe() {
        let store = crate::RuntimeEventStore::for_test();
        let (facts, _) = a8_member_facts("session-413");
        let inline = facts.clone();
        let mut deduped = facts.clone();
        dedupe_member_admission_facts(&mut deduped).expect("dedupe");
        let payload = serde_json::json!({
            "envelope": {"action_id": "outer-413:0"},
            "member_admission": deduped,
        });
        let stream_id = "agentic-program:program-413";
        store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: "tx-413".into(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: stream_id.into(),
                expected_revision: 0,
            }],
            events: vec![crate::runtime_event_store::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream_id.into(),
                    scope: crate::RuntimeEventScope::Program,
                    kind: "agentic.action_applied".into(),
                    status: Some("applied".into()),
                    actor: Some("agent".into()),
                    refs: Vec::new(),
                    payload: payload.clone(),
                },
                idempotency_key: Some("action-413".into()),
                schema_version: 1,
            }],
        })
        .expect("append action event");
        let stored = store
            .event_by_idempotency_key(stream_id, "action-413")
            .expect("read action event")
            .expect("action event present");
        // The durable event carries the deduped payload, and the reader
        // rebuilds the byte identical facts from it.
        assert!(stored.payload["member_admission"]["outer_hook"]["ticket"]["payload_ref"].is_object());
        let rebuilt = rebuild_member_admission_payload(&stored.payload).expect("rebuild payload");
        let rebuilt: &serde_json::Value = &rebuilt;
        assert_eq!(rebuilt["member_admission"], inline);
    }
}
