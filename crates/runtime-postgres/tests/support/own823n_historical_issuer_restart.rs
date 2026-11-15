
use std::sync::Arc;
use runtime::{ExecutionCommitService, RuntimeEventStore, RuntimeEventInput, RuntimeEventScope};
use runtime_postgres::PostgresRuntimeEventStore;
use serde_json::{json,Value};
use storage::{PostgresConnectionConfig,PostgresExecutor,StaticSecretRefResolver};

use super::postgres_scope;

fn database(schema: &str) -> Arc<RuntimeEventStore> {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("isolated PostgreSQL required");
    let resolver = StaticSecretRefResolver::new([("own823n.pg".into(),url)]);
    let executor = PostgresExecutor::connect(PostgresConnectionConfig::new("own823n", "own823n.pg", "own823n-test"), &resolver).unwrap();
    let scoped = executor.scoped_namespace(schema).unwrap();
    Arc::new(PostgresRuntimeEventStore::new(scoped).unwrap().into_runtime_event_store())
}

fn reseal(receipt: &mut runtime::DurableRuntimeEvent) {
    let digest = harness_contract::step::canonical_digest("cowd.local-network.authority.v1", &json!({
        "replay":receipt.payload["replay"],"authorization":receipt.payload["local_network_authorization"],
        "parent_execution_attempt":receipt.payload["local_network_parent_attempt"],
    })).unwrap();
    receipt.payload["outcome"]["observed_evidence"][0]["target"]["operation"]["binding"]["authority_sha256"] = json!(digest.strip_prefix("sha256:").unwrap());
}

#[test]
#[ignore = "requires isolated PostgreSQL and two real child processes; no model or network execution"]
fn own823n_historical_issuer_cross_process_primitive() {
    let phase = std::env::var("OWN823N_PHASE").ok();
    if phase.is_none() {
        let fixture = postgres_scope::PostgresTestScope::new();
        let schema: String = fixture.reconnect().checkout_critical().unwrap().query_one("SELECT current_schema()", &[]).unwrap().get(0);
        let selector = format!("{}::own823n_historical_issuer_cross_process_primitive",
            module_path!().split_once("::").expect("existing test host submodule").1);
        for phase in ["write", "read"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact",selector.as_str(),"--ignored","--nocapture"])
                .env("OWN823N_PHASE",phase).env("OWN823N_SCHEMA",&schema).output().unwrap();
            if !output.status.success() {
                emit_child_failure_diagnostics(&output);
            }
            assert!(output.status.success(), "{phase} child failed: {}", output.status);
        }
        return;
    }
    let store = database(&std::env::var("OWN823N_SCHEMA").unwrap());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    if phase.as_deref() == Some("write") {
        let (receipt,effect) = service.historical_network_receipt_fixture_for_test().unwrap();
        let grant: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(receipt.payload["local_network_authorization"].clone()).unwrap();
        assert!(runtime::AuthorizationNegotiator::verify_lease_signature(&grant.authorization_lease));
        assert!(grant.authorization_lease.max_uses == 32 || grant.authorization_lease.max_uses == 64);
        assert_ne!(receipt.payload["idempotency_key"], grant.request_id);
        service.validate_stored_local_network_receipt(&receipt,&effect).unwrap().unwrap();
        return;
    }
    let key = harness_contract::step::canonical_digest("step-effect-identity", &json!(["801-session","801-step","801-call"])).unwrap();
    let receipt = store.event_by_idempotency_key(&format!("execution-effect:{key}"), &format!("{key}:read-receipt")).unwrap().unwrap();
    let effect = serde_json::from_value(receipt.payload["effect"].clone()).unwrap();
    let grant: harness_contract::tool::ToolExecutionAuthorization = serde_json::from_value(receipt.payload["local_network_authorization"].clone()).unwrap();
    assert!(!runtime::AuthorizationNegotiator::verify_lease_signature(&grant.authorization_lease), "reader must not share issuer process key");
    let before = store.durable_commit_cursor().unwrap();
    let issuer_cut = service.validate_stored_local_network_receipt(&receipt,&effect).unwrap().unwrap();
    assert_eq!(store.durable_commit_cursor().unwrap(),before,"cold validation must not reauthorize or redispatch");
    let anchor = &receipt.payload["local_network_authorization_issuer"];
    let issuer = store.event_by_idempotency_key(anchor["stream"].as_str().unwrap(),anchor["key"].as_str().unwrap()).unwrap().unwrap();
    let issued: harness_contract::policy::AuthorizationLeaseTransition = serde_json::from_value(issuer.payload).unwrap();
    assert_ne!(issued.lease.idempotency_key,grant.authorization_lease.idempotency_key,"fixture must exercise genuine multi-use execution_snapshot");
    assert_eq!(grant.authorization_lease.remaining_uses,1);
    for fault in 0..7 {
        let mut changed = receipt.clone();
        match fault {
            0 => changed.payload["local_network_authorization"]["authorization_lease"]["signature"] = json!("nonempty-foreign"),
            1 => changed.payload["local_network_authorization_issuer"] = Value::Null,
            2 => changed.payload["local_network_authorization_issuer"]["actor"] = json!("foreign"),
            3 => changed.payload["local_network_authorization"]["request_id"] = json!("foreign-call"),
            4 => changed.payload["local_network_authorization"]["authorization_lease"]["scopes"][0]["target"] = json!("another-workspace"),
            5 => changed.payload["local_network_parent_attempt"] = json!(4),
            6 => changed.payload["replay"]["session_id"] = json!("another-session"),
            _ => unreachable!(),
        }
        reseal(&mut changed);
        assert!(service.validate_stored_local_network_receipt(&changed,&effect).is_err(),"resealed cold tamper {fault}");
    }
    let mut foreign_effect = effect.clone();
    foreign_effect.uses_network = false;
    assert!(service.validate_stored_local_network_receipt(&receipt,&foreign_effect).is_err(), "canonical receipt rejects foreign effect");
    let mut absent = receipt.clone();
    absent.payload["local_network_authorization_issuer"]["key"] = json!("missing-original-issued");
    assert!(service.validate_stored_local_network_receipt(&absent,&effect).is_err());
    let terminal_stream = "own823n-terminal";
    let terminal_revision = store.stream_revision(terminal_stream).unwrap();
    assert_eq!(terminal_revision, 0, "fresh test terminal stream");
    store.append(RuntimeEventInput {stream_id:issuer_cut.stream_id.clone(),scope:RuntimeEventScope::Tool,
        kind:"own823n.test.source_cut_race".into(),status:None,actor:None,refs:vec![],payload:json!({})}).unwrap();
    let issuer_after = store.stream_revision(&issuer_cut.stream_id).unwrap();
    assert_eq!(issuer_after, issuer_cut.expected_revision + 1, "the original issuer source must actually advance");
    let result = store.append_transaction(runtime::AppendTransactionRequest {transaction_id:"own823n-raced-terminal".into(),
        expected_streams:vec![issuer_cut.clone(), runtime::ExpectedStreamRevision {
            stream_id:terminal_stream.into(), expected_revision:terminal_revision,
        }],events:vec![RuntimeEventInput {stream_id:terminal_stream.into(),scope:RuntimeEventScope::Session,
            kind:"own823n.test.no_false_terminal".into(),status:None,actor:None,refs:vec![],payload:json!({})}.into()]});
    assert!(matches!(result, Err(runtime::RuntimeEventStoreError::StaleRevision {
        ref stream_id, expected, actual,
    }) if stream_id == &issuer_cut.stream_id && expected == issuer_cut.expected_revision && actual == issuer_after),
        "must reject the advanced original issuer source, got {result:?}");
    assert_eq!(store.stream_revision(terminal_stream).unwrap(),terminal_revision);
}

pub(super) fn emit_child_failure_diagnostics(output: &std::process::Output) {
    use std::io::Write;
    let mut stderr = std::io::stderr().lock();
    let stdout_result = stderr.write_all(&output.stdout);
    let stderr_result = stderr.write_all(&output.stderr);
    if stdout_result.is_err() || stderr_result.is_err() {
        eprintln!("child diagnostic write failed: stdout={stdout_result:?}, stderr={stderr_result:?}");
    }
}
