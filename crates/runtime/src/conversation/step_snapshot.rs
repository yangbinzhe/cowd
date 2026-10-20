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
        let input = harness_contract::step::parse_strict_json(input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
        self.lease
            .validate_input(tool_name, &input)
            .map_err(|error| RuntimeError::new(error.to_string()))?;
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
