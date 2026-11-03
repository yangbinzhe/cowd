use super::*;
use harness_contract::context::{
    EvidenceTargetIdentity, LocalNetworkFact, LocalNetworkTerminal, ToolExecutionBinding,
};

fn invalid(reason: &str) -> ExecutionCommitError {
    ExecutionCommitError::InvalidCommand(reason.into())
}

fn digest(domain: &str, value: &serde_json::Value) -> Result<String, ExecutionCommitError> {
    harness_contract::step::canonical_digest(domain, value)
        .map_err(|error| invalid(&error.to_string()))?
        .strip_prefix("sha256:")
        .map(str::to_owned)
        .ok_or_else(|| invalid("local network digest encoding"))
}

fn facts(outcome: &crate::RuntimeToolExecutionOutcome) -> Vec<&LocalNetworkFact> {
    outcome
        .observed_evidence
        .iter()
        .filter_map(|observed| match &observed.target {
            EvidenceTargetIdentity::LocalNetwork { operation } => Some(operation),
            _ => None,
        })
        .collect()
}

fn binding(
    key: &str,
    call: &str,
    replay: &serde_json::Value,
    authorization: &serde_json::Value,
    parent_attempt: &serde_json::Value,
    effect: &ToolEffectDescriptor,
) -> Result<ToolExecutionBinding, ExecutionCommitError> {
    let grant: harness_contract::tool::ToolExecutionAuthorization =
        serde_json::from_value(authorization.clone())?;
    let lease = &grant.authorization_lease;
    let origin: harness_contract::step::CallOrigin = serde_json::from_value(replay["origin"].clone())?;
    let session = replay["session_id"].as_str().filter(|session| !session.is_empty())
        .ok_or_else(|| invalid("local network original Session missing"))?;
    let origin_valid = match &origin.source {
        harness_contract::step::CallSource::Provider { attempt_id } => !attempt_id.is_empty(),
        harness_contract::step::CallSource::Process { worker_id, sequence } => !worker_id.is_empty() && *sequence > 0,
        harness_contract::step::CallSource::SystemDerived { parent_step_id } => !parent_step_id.is_empty(),
        harness_contract::step::CallSource::OfflineTest => false,
    };
    let expected_key = harness_contract::step::canonical_digest(
        "step-effect-identity", &json!([session, origin.origin_step_id, origin.call_id]),
    ).map_err(|error| invalid(&error.to_string()))?;
    if key != expected_key
        || call.is_empty()
        || origin.call_id != call
        || origin.origin_step_id.is_empty()
        || origin.catalog_digest.is_empty()
        || !origin_valid
        || grant.request_id != call
        || lease.idempotency_key != call
        || replay["invocation"]["origin"] != replay["origin"]
        || replay.is_null()
        || replay["call_id"] != call
        || replay["origin"].is_null()
        || replay["session_id"].as_str().is_none_or(str::is_empty)
        || grant.policy_revision == 0
        || lease.policy_revision != grant.policy_revision
        || grant.tool_id != effect.tool_id
        || replay["invocation"]["effect"] != serde_json::to_value(effect)?
        || grant.descriptor_hash != effect.descriptor_hash
        || lease.effect_descriptor_hash != effect.descriptor_hash
        || lease.signature.is_empty()
        || lease.capability != grant.tool_id
        || !lease.permits(&effect.tool_id, effect.required_permission)
        || !effect.scopes.contains(&grant.scope)
        || !effect.scopes.iter().all(|scope| lease.scopes.contains(scope))
        || grant.timeout_lease.is_empty()
        || replay["invocation"]["invocation_id"] != call
        || replay["invocation"]["intent"]["tool_name"] != effect.tool_id
        || grant
            .idempotency_key
            .as_deref()
            .is_some_and(|original| original != call)
        || (effect.idempotency == harness_contract::tool::ToolIdempotency::IdempotentWithKey
            && grant.idempotency_key.as_deref().is_none_or(str::is_empty))
        || (!replay["parent_execution"].is_null()
            && parent_attempt.as_u64().is_none_or(|attempt| attempt == 0))
    {
        return Err(invalid(
            "local network original grant/Step/attempt mismatch",
        ));
    }
    let input = replay["payload_digest"]
        .as_str()
        .and_then(|value| value.strip_prefix("sha256:"))
        .filter(|value| value.len() == 64)
        .ok_or_else(|| invalid("local network original input digest missing"))?;
    Ok(ToolExecutionBinding {
        call_key: key.into(),
        tool_use_id: call.into(),
        input_sha256: input.into(),
        descriptor_sha256: ExecutionCommitService::bounded_process_descriptor_sha256(effect)?,
        authority_sha256: digest(
            "cowd.local-network.authority.v1",
            &json!({
                "replay":replay, "authorization":authorization, "parent_execution_attempt":parent_attempt,
            }),
        )?,
    })
}

pub(super) fn authorization(request: &crate::RuntimeToolExecutionRequest) -> serde_json::Value {
    if request
        .compiled_invocation
        .as_ref()
        .is_some_and(|invocation| {
            invocation.effect.uses_network && !invocation.effect.spawns_process
        })
    {
        serde_json::to_value(&request.authorization).unwrap_or(serde_json::Value::Null)
    } else {
        serde_json::Value::Null
    }
}

pub(super) fn receipt_payload(
    store: &crate::RuntimeEventStore,
    request: &crate::RuntimeToolExecutionRequest,
    mut payload: serde_json::Value,
) -> Result<serde_json::Value, ExecutionCommitError> {
    let grant = authorization(request);
    if !grant.is_null() {
        let typed: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(grant.clone())?;
        let issuer = original_issuer(store, &typed)?;
        payload["local_network_authorization_issuer"] = issuer_anchor(&issuer)?;
        payload["local_network_authorization"] = grant;
        payload["local_network_parent_attempt"] = json!(request.parent_execution_attempt);
    }
    Ok(payload)
}

pub(super) fn validate_outcome(
    request: &crate::RuntimeToolExecutionRequest,
    effect: &ToolEffectDescriptor,
    outcome: &crate::RuntimeToolExecutionOutcome,
) -> Result<(), ExecutionCommitError> {
    let observed = facts(outcome);
    if observed.is_empty() {
        return Ok(());
    }
    if !effect.uses_network
        || effect.spawns_process
        || observed.len() != 1
        || request
            .compiled_invocation
            .as_ref()
            .is_none_or(|invocation| invocation.effect != *effect)
        || request
            .authorization
            .as_ref()
            .is_none_or(|grant| grant.policy_revision != request.policy_revision)
        || !request.authorization.as_ref().is_some_and(|grant| {
            crate::AuthorizationNegotiator::verify_lease_signature(&grant.authorization_lease)
        })
        || observed[0].binding != ExecutionCommitService::local_network_binding(request)?
    {
        return Err(invalid(
            "local network original publication binding mismatch",
        ));
    }
    Ok(())
}

impl ExecutionCommitService {

    #[cfg(feature = "test-fixtures")]
    pub fn historical_network_receipt_fixture_for_test(&self) -> Result<(crate::DurableRuntimeEvent, ToolEffectDescriptor), ExecutionCommitError> {
        use crate::{RuntimeToolExecutionOutcome, RuntimeToolExecutionStatus};
        let (mut fixture, effect) = (|| {
    let input = json!({"url":"https://example.invalid","prompt":"read"});
    let effect = tools::ToolHost::builtin("801-runtime", std::env::temp_dir())
        .pin_snapshot()
        .describe_effect("web_fetch", &input);
    let permission = crate::PermissionMode::DangerFullAccess;
    let request = crate::AuthorizationRequest {
        principal_id:"session:801-session".into(), capability:effect.tool_id.clone(),
        input:input.to_string(), idempotency_key:"801-call".into(), effect:effect.clone(),
        parent_ceiling:permission, parent_lease_id:None, policy_revision:7,
        recovery_scope:"823n-fixture".into(), context:crate::PermissionContext::default(),
        safe_alternatives:Vec::new(),
    };
    let negotiator = crate::AuthorizationNegotiator::new();
    let mut first_call = request.clone();
    first_call.idempotency_key = "801-first-call".into();
    let _first = negotiator.assess_effective(&crate::PermissionPolicy::new(permission), &first_call);
    let evaluated = negotiator.assess_effective(&crate::PermissionPolicy::new(permission), &request);
    let signed = evaluated.assessment.lease.clone().expect("real policy must admit the exact public network-read fixture");
    assert!(crate::AuthorizationNegotiator::verify_lease_signature(&signed));
    let grant = serde_json::to_value(crate::ToolPolicy.authorize(
        &evaluated.effective, &evaluated.assessment, "801-call", signed, 30,
    ).unwrap().authorization).unwrap();
    let issuer_store = self.event_store.as_ref();
    for transition in negotiator.drain_transitions() {
        crate::authorization_negotiator::persist_authorization_transition(&issuer_store,
            &format!("authorization-lease:{}", transition.lease.lease_id), "conversation_runtime", &transition).unwrap();
    }
    let typed_grant = serde_json::from_value(grant.clone()).unwrap();
    let issuer = original_issuer(&issuer_store, &typed_grant).unwrap();
    let anchor = issuer_anchor(&issuer).unwrap();
    let effect_key = harness_contract::step::canonical_digest(
        "step-effect-identity", &json!(["801-session", "801-step", "801-call"]),
    ).unwrap();
    assert_ne!(effect_key, "801-call");
    let replay = json!({
        "session_id":"801-session", "call_id":"801-call",
        "parent_execution":{"execution_id":"801-owner","node_id":"801-node"},
        "origin":{"origin_step_id":"801-step","call_id":"801-call","catalog_digest":"801-catalog",
            "source":{"kind":"provider","attempt_id":"801-attempt"}},
        "payload_digest":harness_contract::step::canonical_digest("tool-effect-payload", &input).unwrap(),
        "invocation":{"invocation_id":"801-call","intent":{"tool_name":"web_fetch","normalized_input":null},"effect":effect,"origin":{"origin_step_id":"801-step","call_id":"801-call","catalog_digest":"801-catalog",
            "source":{"kind":"provider","attempt_id":"801-attempt"}}},
    });
    let original = binding(&effect_key, "801-call", &replay, &grant, &json!(3), &effect).unwrap();
    let fact = LocalNetworkFact {
        version: 1,
        binding: original,
        host_joined: true,
        coverage_complete: true,
        operations: vec![harness_contract::context::LocalNetworkOperation {
            ordinal: 1,
            input_sha256: "b".repeat(64),
            endpoint: "https://example.invalid".into(),
            terminal: LocalNetworkTerminal::BodyDone {
                status: 200,
                body_bytes: 2,
                body_sha256: "c".repeat(64),
            },
        }],
    };
    let outcome = RuntimeToolExecutionOutcome {
        tool_use_id: "801-call".into(),
        tool_name: "web_fetch".into(),
        status: RuntimeToolExecutionStatus::Executed,
        category: crate::ToolSafetyCategory::ReadOnly,
        output: Some("exact fixture output, not lifecycle authority".into()),
        error: None,
        evidence_ref: "801-test-source".into(),
        observed_evidence: vec![fact.into_observation("web_fetch".into(), 1)],
        pending_agent_batch: None,
    };
    (
        crate::DurableRuntimeEvent {
            event_id: "801-primary".into(),
            stream_id: format!("execution-effect:{effect_key}"),
            sequence: 1,
            scope: RuntimeEventScope::ExecutionNode,
            kind: "execution.read.receipt".into(),
            status: None,
            actor: Some("governed_tool".into()),
            refs: vec![],
            payload: json!({
                "idempotency_key":effect_key, "tool_use_id":"801-call", "tool_name":"web_fetch",
                "replay":replay, "effect":effect, "outcome":outcome,
                "local_network_authorization":grant, "local_network_authorization_issuer":anchor, "_fixture_issuer_record":issuer, "local_network_parent_attempt":3,
            }),
            created_at_ms: 1,
            commit_cursor: 3,
            transaction_id: "801-original".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: Some(format!("{effect_key}:read-receipt")),
        },
        effect,
    )
        })();
        fixture.payload.as_object_mut().unwrap().remove("_fixture_issuer_record");
        self.event_store.append_transaction(AppendTransactionRequest {
            transaction_id:fixture.transaction_id.clone(),expected_streams:vec![],
            events:vec![RuntimeTransactionEventInput {event:RuntimeEventInput {
                stream_id:fixture.stream_id.clone(),scope:fixture.scope,kind:fixture.kind.clone(),status:Some("completed".into()),
                actor:fixture.actor.clone(),refs:fixture.refs.clone(),payload:fixture.payload.clone(),
            },idempotency_key:fixture.idempotency_key.clone(),schema_version:1}],
        })?;
        let stored = self.event_store.event_by_idempotency_key(&fixture.stream_id, fixture.idempotency_key.as_deref().unwrap())?
            .ok_or_else(|| invalid("persisted network test primary missing"))?;
        Ok((stored,effect))
    }

    pub fn local_network_binding(
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<ToolExecutionBinding, ExecutionCommitError> {
        let effect = &request
            .compiled_invocation
            .as_ref()
            .ok_or_else(|| invalid("local network original invocation missing"))?
            .effect;
        let step = request.step_binding.as_ref().ok_or_else(|| invalid("local network original Step missing"))?;
        let source_authenticated = match &step.origin.source {
            harness_contract::step::CallSource::Provider { attempt_id } => attempt_id == &step.snapshot.provider_attempt.attempt_id,
            harness_contract::step::CallSource::Process { worker_id, sequence } => !worker_id.is_empty()
                && *sequence > 0 && step.snapshot.related_revisions.get("worker_run") == Some(worker_id),
            harness_contract::step::CallSource::SystemDerived { parent_step_id } => step.snapshot.identity.execution_id.is_some()
                && step.snapshot.context_refs.contains(&format!("step:{parent_step_id}")),
            harness_contract::step::CallSource::OfflineTest => false,
        };
        if !source_authenticated
            || step.snapshot.contract_version != harness_contract::step::STEP_CONTRACT_VERSION
            || step.effect_idempotency_key() != request.idempotency_key
            || step.origin.call_id != request.tool_use_id
            || request.session_id.as_deref() != Some(step.snapshot.identity.session_id.as_str())
            || step.origin.origin_step_id != step.snapshot.identity.step_id
            || step.origin.catalog_digest != step.snapshot.catalog_digest
            || request.compiled_invocation.as_ref().and_then(|invocation| invocation.origin.as_ref()) != Some(&step.origin)
            || !request.authorization.as_ref().is_some_and(|grant| crate::AuthorizationNegotiator::verify_lease_signature(&grant.authorization_lease)) {
            return Err(invalid("local network original Step/effect/grant authentication mismatch"));
        }
        if !effect.uses_network
            || effect.spawns_process
            || request.policy_revision == 0
            || request
                .authorization
                .as_ref()
                .is_none_or(|grant| grant.policy_revision != request.policy_revision)
        {
            return Err(invalid("local network original policy/effect mismatch"));
        }
        binding(
            &request.idempotency_key,
            &request.tool_use_id,
            &receipt_replay_contract(request)?,
            &authorization(request),
            &serde_json::to_value(request.parent_execution_attempt)?,
            effect,
        )
    }

    pub(crate) fn local_network_closed(
        outcome: &crate::RuntimeToolExecutionOutcome,
        effect: &ToolEffectDescriptor,
    ) -> bool {
        let observed = facts(outcome);
        if observed.is_empty() {
            return !effect.spawns_process && !effect.uses_network;
        }
        if effect.spawns_process
            || !effect.uses_network
            || observed.len() != 1
            || outcome.observed_evidence.iter().any(|observed| {
                matches!(
                    observed.target,
                    EvidenceTargetIdentity::ToolExecution { .. }
                )
            })
        {
            return false;
        }
        let fact = observed[0];
        fact.version == 1
            && fact.host_joined
            && fact.coverage_complete
            && !fact.operations.is_empty()
            && fact.operations.len() <= 400
            && fact
                .operations
                .iter()
                .enumerate()
                .all(|(position, operation)| {
                    operation.ordinal as usize == position + 1
                        && operation.input_sha256.len() == 64
                        && operation
                            .input_sha256
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit())
                        && !operation.endpoint.trim().is_empty()
                        && match &operation.terminal {
                            LocalNetworkTerminal::BodyDone {
                                status,
                                body_sha256,
                                ..
                            } => {
                                (100..=599).contains(status)
                                    && body_sha256.len() == 64
                                    && body_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
                            }
                            LocalNetworkTerminal::NotStarted { reason_code } => matches!(
                                reason_code.as_str(),
                                "network_policy_before_send" | "network_url_policy_before_send"
                            ),
                            LocalNetworkTerminal::Unknown { .. } => false,
                        }
                })
    }

    pub fn validate_stored_local_network_receipt(
        &self,
        receipt: &crate::DurableRuntimeEvent,
        effect: &ToolEffectDescriptor,
    ) -> Result<Option<crate::ExpectedStreamRevision>, ExecutionCommitError> {
        if facts(&serde_json::from_value(receipt.payload["outcome"].clone())?).is_empty() {
            return Ok(None);
        }
        let stream = receipt.payload["local_network_authorization_issuer"]["stream"].as_str()
            .ok_or_else(|| invalid("historical issuer source missing"))?;
        let head = self.event_store.stream_revision(stream)?;
        Self::validate_local_network_receipt(receipt, effect, &mut StoredTerminalScopeReader(&self.event_store))?;
        Ok(Some(crate::ExpectedStreamRevision {stream_id:stream.into(),expected_revision:head}))
    }

    pub(crate) fn validate_local_network_receipt(
        receipt: &crate::DurableRuntimeEvent,
        effect: &ToolEffectDescriptor,
        reader: &mut impl TerminalScopeReader,
    ) -> Result<(), ExecutionCommitError> {
        if !receipt.payload["outcome"]["observed_evidence"].as_array().is_some_and(|observations|
            observations.iter().any(|observed| observed["target"]["kind"] == "local_network"))
        {
            return Ok(());
        }
        let outcome: crate::RuntimeToolExecutionOutcome =
            serde_json::from_value(receipt.payload["outcome"].clone())?;
        let observed = facts(&outcome);
        if observed.is_empty() {
            return Ok(());
        }
        let receipt_key = receipt.idempotency_key.as_deref().ok_or_else(|| invalid("original primary key missing"))?;
        let original = reader.event(&receipt.stream_id, receipt_key)?.ok_or_else(|| invalid("original primary absent at source cut"))?;
        if serde_json::to_value(original.as_ref())? != serde_json::to_value(receipt)? {
            return Err(invalid("historical receipt differs from canonical original primary"));
        }
        let original_fence = reader.fence(&receipt.transaction_id, &receipt.stream_id)?.ok_or_else(|| invalid("original primary transaction fence missing"))?;
        if original_fence.stream_id != receipt.stream_id || original_fence.committed_revision != receipt.sequence {
            return Err(invalid("original primary transaction fence changed"));
        }
        let original_grant: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(receipt.payload["local_network_authorization"].clone())?;
        validate_issuer_from_reader(receipt, &original_grant, reader)?;
        if original_grant.authorization_lease.principal_id.starts_with("session:")
            && original_grant.authorization_lease.principal_id != format!("session:{}", receipt.payload["replay"]["session_id"].as_str().unwrap_or("")) {
            return Err(invalid("historical issuer Session/workspace principal mismatch"));
        }
        let key = receipt.payload["idempotency_key"]
            .as_str()
            .ok_or_else(|| invalid("local network receipt key missing"))?;
        let call = receipt.payload["tool_use_id"]
            .as_str()
            .or_else(|| receipt.payload["replay"]["call_id"].as_str())
            .ok_or_else(|| invalid("local network receipt call missing"))?;
        if !effect.uses_network
            || effect.spawns_process
            || observed.len() != 1
            || receipt.stream_id != format!("execution-effect:{key}")
            || receipt.schema_version != 1
            || receipt.scope != RuntimeEventScope::ExecutionNode
            || receipt.actor.as_deref() != Some("governed_tool")
            || !matches!(
                receipt.kind.as_str(),
                "execution.read.receipt" | "execution.effect.receipt"
            )
            || receipt.idempotency_key.as_deref()
                != Some(
                    format!(
                        "{key}:{}",
                        if receipt.kind == "execution.read.receipt" {
                            "read-receipt"
                        } else {
                            "receipt"
                        }
                    )
                    .as_str(),
                )
            || receipt.payload["effect"] != serde_json::to_value(effect)?
            || outcome.tool_use_id != call
            || receipt.payload["tool_name"] != outcome.tool_name
            || observed[0].binding
                != binding(
                    key,
                    call,
                    &receipt.payload["replay"],
                    &receipt.payload["local_network_authorization"],
                    &receipt.payload["local_network_parent_attempt"],
                    effect,
                )?
        {
            return Err(invalid("local network durable original binding mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "local_network_tests.rs"]
mod tests;

fn issuer_anchor(issuer: &crate::DurableRuntimeEvent) -> Result<serde_json::Value, ExecutionCommitError> {
    Ok(json!({"stream":issuer.stream_id,"key":issuer.idempotency_key,"event_id":issuer.event_id,
        "actor":issuer.actor,"scope":issuer.scope,"schema":issuer.schema_version,"status":issuer.status,
        "refs":issuer.refs,"sequence":issuer.sequence,"transaction_index":issuer.transaction_index,
        "transaction_id":issuer.transaction_id,"commit_cursor":issuer.commit_cursor,
        "payload_digest":digest("cowd.local-network.issuer.v1", &issuer.payload)?}))
}

pub(super) fn original_issuer(
    store: &crate::RuntimeEventStore,
    grant: &harness_contract::tool::ToolExecutionAuthorization,
) -> Result<crate::DurableRuntimeEvent, ExecutionCommitError> {
    let stream = format!("authorization-lease:{}", grant.authorization_lease.lease_id);
    let mut page = store.list_stream_after(&stream, 0, 1, 1, 128 * 1024).map_err(|error| invalid(&error))?;
    let issuer = page.pop().ok_or_else(|| invalid("original authorization Issued record missing"))?;
    crate::authorization_negotiator::validate_historical_authorization_issuer(&issuer, grant, u64::MAX).map_err(|error| invalid(&error))?;
    Ok(issuer)
}

fn validate_issuer_from_reader(
    receipt: &crate::DurableRuntimeEvent,
    grant: &harness_contract::tool::ToolExecutionAuthorization,
    reader: &mut impl TerminalScopeReader,
) -> Result<(), ExecutionCommitError> {
    let anchor = &receipt.payload["local_network_authorization_issuer"];
    let stream = anchor["stream"].as_str().ok_or_else(|| invalid("original issuer anchor missing"))?;
    let key = anchor["key"].as_str().ok_or_else(|| invalid("original issuer key missing"))?;
    if stream != format!("authorization-lease:{}", grant.authorization_lease.lease_id) {
        return Err(invalid("issuer stream does not bind the original lease"));
    }
    let issuer = reader.event(stream, key)?.ok_or_else(|| invalid("historical issuer unavailable at source cut"))?;
    if issuer_anchor(&issuer)? != *anchor {
        return Err(invalid("historical issuer anchor changed"));
    }
    crate::authorization_negotiator::validate_historical_authorization_issuer(&issuer, grant, receipt.commit_cursor).map_err(|error| invalid(&error))?;
    let fence = reader.fence(&issuer.transaction_id, stream)?.ok_or_else(|| invalid("original Issued transaction fence missing"))?;
    if fence.stream_id != stream || fence.expected_revision != 0 || fence.committed_revision != 1 {
        return Err(invalid("original Issued transaction fence changed"));
    }
    Ok(())
}
