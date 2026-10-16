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
    execution.map(|context| context.turn_id.clone()).or(input_turn)
        .ok_or_else(|| RuntimeError::new("production step has no admitted turn identity"))
}

#[cfg(test)]
mod turn_identity_tests {
    #[test]
    fn execution_owner_turn_is_not_replaced_by_supplemental_input_identity() {
        let execution = crate::CowdExecutionContext { execution_id: "graph".into(), session_id: "session".into(), turn_id: "admitted-turn".into() };
        assert_eq!(super::admitted_turn_id(Some(&execution), Some("supplement-turn".into())).unwrap(), "admitted-turn");
        assert_eq!(super::admitted_turn_id(None, Some("standalone-turn".into())).unwrap(), "standalone-turn");
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
