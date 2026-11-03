use super::*;
use crate::{RuntimeToolExecutionOutcome, RuntimeToolExecutionStatus};

fn fixture() -> (crate::DurableRuntimeEvent, ToolEffectDescriptor) {
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
    let issuer_store = crate::RuntimeEventStore::for_test();
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
        pending_agent_batch: None,
        tool_use_id: "801-call".into(),
        tool_name: "web_fetch".into(),
        status: RuntimeToolExecutionStatus::Executed,
        category: crate::ToolSafetyCategory::ReadOnly,
        output: Some("exact fixture output, not lifecycle authority".into()),
        error: None,
        evidence_ref: "801-test-source".into(),
        observed_evidence: vec![fact.into_observation("web_fetch".into(), 1)],
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
}

#[test]
fn own823n_cold_and_live_reject_unknown_unjoined_unsupported_empty_and_process_substitution() {
    let (original, effect) = fixture();
    validate_receipt(&original, &effect).unwrap();
    let outcome: RuntimeToolExecutionOutcome =
        serde_json::from_value(original.payload["outcome"].clone()).unwrap();
    assert!(ExecutionCommitService::lease_recovery_process_closed(
        &outcome, &effect
    ));
    assert!(crate::execution_projection::settlement::process_closed(
        &outcome, &effect
    ));
    for fault in 0..7 {
        let mut changed = outcome.clone();
        let EvidenceTargetIdentity::LocalNetwork { operation } =
            &mut changed.observed_evidence[0].target
        else {
            unreachable!()
        };
        match fault {
            0 => operation.host_joined = false,
            1 => operation.coverage_complete = false,
            2 => operation.operations.clear(),
            3 => {
                operation.operations[0].terminal = LocalNetworkTerminal::Unknown {
                    reason_code: "timeout".into(),
                }
            }
            4 => operation.operations[0].ordinal = 2,
            5 => {
                operation.operations[0].terminal = LocalNetworkTerminal::NotStarted {
                    reason_code: "post_send_error".into(),
                }
            }
            6 => operation.version = 2,
            _ => unreachable!(),
        }
        assert!(
            !ExecutionCommitService::lease_recovery_process_closed(&changed, &effect),
            "{fault}"
        );
        assert!(
            !crate::execution_projection::settlement::process_closed(&changed, &effect),
            "{fault}"
        );
    }
    let mut missing = outcome.clone();
    missing.observed_evidence.clear();
    assert!(!ExecutionCommitService::lease_recovery_process_closed(&missing, &effect));
    assert!(!crate::execution_projection::settlement::process_closed(&missing, &effect));
    let mut duplicated = outcome.clone();
    duplicated.observed_evidence.push(outcome.observed_evidence[0].clone());
    assert!(!ExecutionCommitService::lease_recovery_process_closed(&duplicated, &effect));
    assert!(!crate::execution_projection::settlement::process_closed(&duplicated, &effect));
    let mut process = effect.clone();
    process.spawns_process = true;
    assert!(!ExecutionCommitService::lease_recovery_process_closed(
        &outcome, &process
    ));
    assert!(!crate::execution_projection::settlement::process_closed(
        &outcome, &process
    ));
}

#[test]
fn own823n_shared_original_validator_rejects_foreign_key_call_input_descriptor_step_attempt_and_policy(
) {
    let (original, effect) = fixture();
    for fault in 0..10 {
        let mut changed = original.clone();
        match fault {
            0 => changed.payload["idempotency_key"] = json!("foreign-key"),
            1 => changed.payload["tool_use_id"] = json!("foreign-call"),
            2 => {
                changed.payload["replay"]["payload_digest"] =
                    json!(format!("sha256:{}", "d".repeat(64)))
            }
            3 => changed.payload["replay"]["origin"]["origin_step_id"] = json!("foreign-step"),
            4 => changed.payload["replay"]["origin"]["catalog_digest"] = json!("foreign-run"),
            5 => changed.payload["local_network_parent_attempt"] = json!(4),
            6 => changed.payload["local_network_authorization"]["policy_revision"] = json!(8),
            7 => {
                changed.payload["replay"]["parent_execution"]["execution_id"] =
                    json!("foreign-owner")
            }
            8 => changed.payload["effect"]["descriptor_hash"] = json!("foreign-descriptor"),
            9 => {
                changed.payload["local_network_authorization"]["authorization_lease"]["signature"] =
                    json!("foreign-signature")
            }
            _ => unreachable!(),
        }
        assert!(
            validate_receipt(&changed, &effect).is_err(),
            "{fault}"
        );
    }
}

#[test]
fn own823n_atomic_primary_index_original_fact_and_raced_cut_cannot_publish_terminal() {
    let (mut primary, effect) = fixture();
    let store = crate::RuntimeEventStore::for_test();
    let prior_issuer: crate::DurableRuntimeEvent = serde_json::from_value(primary.payload["_fixture_issuer_record"].clone()).unwrap();
    let transition = serde_json::from_value(prior_issuer.payload).unwrap();
    crate::authorization_negotiator::persist_authorization_transition(&store, &prior_issuer.stream_id, "conversation_runtime", &transition).unwrap();
    let grant = serde_json::from_value(primary.payload["local_network_authorization"].clone()).unwrap();
    let issuer = original_issuer(&store, &grant).unwrap();
    primary.payload["local_network_authorization_issuer"] = issuer_anchor(&issuer).unwrap();
    primary.payload["_fixture_issuer_record"] = serde_json::to_value(issuer).unwrap();
    let key = primary.payload["idempotency_key"].as_str().unwrap().to_string();
    let index = "execution-agent-receipts:801-owner:801-node:3";
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "801-real-primary-index".into(),
            expected_streams: vec![
                ExpectedStreamRevision { stream_id: primary.stream_id.clone(), expected_revision: 0 },
                ExpectedStreamRevision { stream_id: index.into(), expected_revision: 0 },
            ],
            events: vec![
                RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: primary.stream_id.clone(),
                        scope: primary.scope,
                        kind: primary.kind.clone(),
                        status: None,
                        actor: primary.actor.clone(),
                        refs: vec![],
                        payload: primary.payload.clone(),
                    },
                    idempotency_key: primary.idempotency_key.clone(),
                    schema_version: 1,
                },
                RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: index.into(),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "execution.agent_tool.receipt".into(),
                        status: None,
                        actor: Some("governed_tool".into()),
                        refs: vec![],
                        payload: json!({
                            "idempotency_key":key, "outcome":primary.payload["outcome"],
                        }),
                    },
                    idempotency_key: Some(format!("agent-tool-receipt:{key}")),
                    schema_version: 1,
                },
            ],
        })
        .unwrap();
    let stored = store
        .event_by_idempotency_key(&primary.stream_id, &format!("{key}:read-receipt"))
        .unwrap()
        .unwrap();
    let indexed = store
        .event_by_idempotency_key(index, &format!("agent-tool-receipt:{key}"))
        .unwrap()
        .unwrap();
    assert_eq!(stored.transaction_id, indexed.transaction_id);
    assert_eq!(stored.commit_cursor, indexed.commit_cursor);
    assert!(stored.transaction_index < indexed.transaction_index);
    assert_eq!(stored.payload["outcome"], indexed.payload["outcome"]);
    validate_receipt(&stored, &effect).unwrap();
    store
        .append(RuntimeEventInput {
            stream_id: index.into(),
            scope: RuntimeEventScope::ExecutionNode,
            kind: "801.negative-only-race".into(),
            status: None,
            actor: None,
            refs: vec![],
            payload: json!({}),
        })
        .unwrap();
    let target = "801-no-terminal";
    let error = store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "801-raced-close".into(),
            expected_streams: vec![
                ExpectedStreamRevision {
                    stream_id: stored.stream_id,
                    expected_revision: 1
                },
                ExpectedStreamRevision {
                    stream_id: index.into(),
                    expected_revision: 1
                },
                ExpectedStreamRevision {
                    stream_id: target.into(),
                    expected_revision: 0
                },
            ],
            events: vec![RuntimeEventInput {
                stream_id: target.into(),
                scope: RuntimeEventScope::Session,
                kind: "801.negative-only-terminal".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: json!({}),
            }
            .into()],
        })
        .unwrap_err();
    assert!(matches!(error, crate::RuntimeEventStoreError::StaleRevision { ref stream_id, .. }
        if stream_id == index), "{error}");
    assert_eq!(store.stream_revision(target).unwrap(), 0);
}

fn recompute_untrusted_fact_authority(receipt: &mut crate::DurableRuntimeEvent) {
    let authority = digest("cowd.local-network.authority.v1", &json!({
        "replay":receipt.payload["replay"],
        "authorization":receipt.payload["local_network_authorization"],
        "parent_execution_attempt":receipt.payload["local_network_parent_attempt"],
    })).unwrap();
    let mut outcome: RuntimeToolExecutionOutcome = serde_json::from_value(receipt.payload["outcome"].clone()).unwrap();
    let EvidenceTargetIdentity::LocalNetwork { operation } = &mut outcome.observed_evidence[0].target else {unreachable!()};
    operation.binding.authority_sha256 = authority;
    receipt.payload["outcome"] = serde_json::to_value(outcome).unwrap();
}

#[test]
fn own823n_cold_signature_tamper_with_coherent_authority_reseal_is_not_original_authority() {
    let (original, effect) = fixture();
    validate_receipt(&original, &effect).unwrap();
    let mut changed = original.clone();
    changed.payload["local_network_authorization"]["authorization_lease"]["signature"] = json!(format!("sha256:{}", "f".repeat(64)));
    recompute_untrusted_fact_authority(&mut changed);
    let tampered: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(changed.payload["local_network_authorization"].clone()).unwrap();
    assert!(!tampered.authorization_lease.signature.is_empty());
    assert!(!crate::AuthorizationNegotiator::verify_lease_signature(&tampered.authorization_lease));
    assert_ne!(changed.payload["outcome"], original.payload["outcome"]);
    assert!(validate_receipt(&changed, &effect).is_err());
}

#[test]
fn own823n_cold_grant_scope_policy_origin_and_legacy_tamper_reseal_cannot_close() {
    let (original, effect) = fixture();
    for fault in 0..13 {
        let mut changed = original.clone();
        match fault {
            0 => changed.payload["local_network_authorization"]["request_id"] = json!("foreign"),
            1 => changed.payload["local_network_authorization"]["authorization_lease"]["idempotency_key"] = json!("foreign"),
            2 => changed.payload["local_network_authorization"]["idempotency_key"] = json!("foreign"),
            3 => changed.payload["local_network_authorization"]["scope"]["target"] = json!("foreign"),
            4 => changed.payload["local_network_authorization"]["authorization_lease"]["capability"] = json!("foreign"),
            5 => {
                changed.payload["local_network_authorization"]["policy_revision"] = json!(8);
                changed.payload["local_network_authorization"]["authorization_lease"]["policy_revision"] = json!(8);
            }
            6 => changed.payload["local_network_authorization"]["timeout_lease"] = json!(""),
            7 => changed.payload["replay"]["invocation"]["invocation_id"] = json!("foreign"),
            8 => changed.payload["replay"]["invocation"]["intent"]["tool_name"] = json!("foreign"),
            9 => changed.payload["local_network_authorization"] = serde_json::Value::Null,
            10 => changed.payload["replay"] = serde_json::Value::Null,
            11 => changed.payload["local_network_parent_attempt"] = serde_json::Value::Null,
            12 => {
                changed.payload["replay"]["origin"]["origin_step_id"] = json!("foreign-step");
                changed.payload["replay"]["invocation"]["origin"]["origin_step_id"] = json!("foreign-step");
            }
            _ => unreachable!(),
        }
        recompute_untrusted_fact_authority(&mut changed);
        assert!(validate_receipt(&changed, &effect).is_err(), "coherent reseal fault {fault}");
    }
}

#[test]
fn own823n_authentic_distinct_namespaces_reconstruct_and_each_original_tamper_fails() {
    let (original, effect) = fixture();
    let replay = &original.payload["replay"];
    let authorization = &original.payload["local_network_authorization"];
    let key = original.payload["idempotency_key"].as_str().unwrap();
    let grant: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(authorization.clone()).unwrap();
    assert!(crate::AuthorizationNegotiator::verify_lease_signature(&grant.authorization_lease));
    assert_eq!(grant.request_id, "801-call");
    assert_eq!(grant.authorization_lease.idempotency_key, "801-call");
    assert_ne!(key, grant.authorization_lease.idempotency_key);
    binding(key, "801-call", replay, authorization, &json!(3), &effect).unwrap();
    validate_receipt(&original, &effect).unwrap();
    for fault in 0..12 {
        let mut changed = original.clone();
        match fault {
            0 => changed.payload["local_network_authorization"]["request_id"] = json!("foreign"),
            1 => changed.payload["local_network_authorization"]["authorization_lease"]["idempotency_key"] = json!("foreign"),
            2 => changed.payload["local_network_authorization"]["idempotency_key"] = json!("foreign"),
            3 => changed.payload["idempotency_key"] = json!("801-call"),
            4 => changed.payload["replay"]["origin"]["call_id"] = json!("foreign"),
            5 => changed.payload["replay"]["session_id"] = json!("foreign"),
            6 => changed.payload["replay"]["origin"] = serde_json::Value::Null,
            7 => changed.payload["local_network_authorization"] = serde_json::Value::Null,
            8 => changed.payload["local_network_parent_attempt"] = serde_json::Value::Null,
            9 => changed.payload["replay"]["origin"]["source"] = json!({"kind":"offline_test"}),
            10 => changed.payload["replay"]["invocation"]["origin"]["catalog_digest"] = json!("foreign"),
            11 => changed.payload["local_network_authorization"]["authorization_lease"]["signature"] = json!("foreign"),
            _ => unreachable!(),
        }
        assert!(validate_receipt(&changed, &effect).is_err(), "fault {fault}");
    }
    for field in ["request_id", "idempotency_key"] {
        let mut changed = authorization.clone();
        changed[field] = json!("foreign");
        assert!(binding(key, "801-call", replay, &changed, &json!(3), &effect).is_err(), "grant {field}");
    }
    let mut lease_key = authorization.clone();
    lease_key["authorization_lease"]["idempotency_key"] = json!("foreign");
    assert!(binding(key, "801-call", replay, &lease_key, &json!(3), &effect).is_err());
    let mut legacy = original.clone();
    legacy.payload["outcome"]["observed_evidence"] = json!([]);
    validate_receipt(&legacy, &effect).unwrap();
    let legacy_outcome: RuntimeToolExecutionOutcome = serde_json::from_value(legacy.payload["outcome"].clone()).unwrap();
    assert!(!ExecutionCommitService::lease_recovery_process_closed(&legacy_outcome, &effect));
    assert!(!crate::execution_projection::settlement::process_closed(&legacy_outcome, &effect));
}

struct FixtureIssuerReader(Arc<crate::DurableRuntimeEvent>, Arc<crate::DurableRuntimeEvent>);
impl TerminalScopeReader for FixtureIssuerReader {
    fn event(&mut self, stream: &str, key: &str) -> Result<Option<Arc<crate::DurableRuntimeEvent>>, ExecutionCommitError> {
        Ok([&self.0,&self.1].into_iter().find(|record| record.stream_id == stream && record.idempotency_key.as_deref() == Some(key)).map(Arc::clone))
    }
    fn fence(&mut self, transaction: &str, stream: &str) -> Result<Option<crate::CommittedStreamRevision>, ExecutionCommitError> {
        Ok([&self.0,&self.1].into_iter().find(|record| record.transaction_id == transaction && record.stream_id == stream)
            .map(|record| crate::CommittedStreamRevision {stream_id:stream.into(),expected_revision:record.sequence-1,committed_revision:record.sequence}))
    }
}
fn validate_receipt(receipt: &crate::DurableRuntimeEvent, effect: &ToolEffectDescriptor) -> Result<(), ExecutionCommitError> {
    let issuer = serde_json::from_value(receipt.payload["_fixture_issuer_record"].clone())?;
    ExecutionCommitService::validate_local_network_receipt(receipt, effect, &mut FixtureIssuerReader(Arc::new(issuer),Arc::new(receipt.clone())))
}

#[test]
fn own823n_historical_issuer_allows_mutable_32_use_snapshot_but_not_signature_or_workspace_change() {
    let (original, effect) = fixture();
    let issuer: crate::DurableRuntimeEvent = serde_json::from_value(original.payload["_fixture_issuer_record"].clone()).unwrap();
    let mut grant: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(original.payload["local_network_authorization"].clone()).unwrap();
    grant.authorization_lease.remaining_uses = 1;
    grant.authorization_lease.idempotency_key = "later-call".into();
    crate::authorization_negotiator::validate_historical_authorization_issuer(&issuer, &grant, original.commit_cursor).unwrap();
    grant.authorization_lease.scopes[0].target = Some("another-workspace".into());
    assert!(crate::authorization_negotiator::validate_historical_authorization_issuer(&issuer, &grant, original.commit_cursor).is_err());
    for fault in 0..7 {
        let mut changed = issuer.clone();
        match fault {
            0 => changed.actor = Some("foreign-writer".into()),
            1 => changed.schema_version = 2,
            2 => changed.payload["kind"] = json!("consumed"),
            3 => changed.refs.clear(),
            4 => changed.sequence = 2,
            5 => changed.payload["lease"]["signature"] = json!("nonempty-foreign"),
            6 => changed.transaction_id = "foreign-transaction".into(),
            _ => unreachable!(),
        }
        let original_grant = serde_json::from_value(original.payload["local_network_authorization"].clone()).unwrap();
        assert!(crate::authorization_negotiator::validate_historical_authorization_issuer(&changed, &original_grant, original.commit_cursor).is_err(), "issuer {fault}");
    }
}
