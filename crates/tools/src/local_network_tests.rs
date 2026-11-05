use super::*;
use harness_contract::policy::{AuthorizationLease, AuthorizationLeaseStatus};
use std::io::{Read, Write};

fn origin() -> harness_contract::step::CallOrigin {
    harness_contract::step::CallOrigin { origin_step_id:"801-step".into(), call_id:"801-call".into(),
        catalog_digest:"801-catalog".into(), source:harness_contract::step::CallSource::Provider {attempt_id:"801-attempt".into()} }
}

fn fixture(
    input: &Value,
) -> (
    ToolHostLease,
    ToolExecutionAuthorization,
    ToolExecutionBinding,
) {
    let host = ToolHost::builtin("801-tools", std::env::temp_dir())
        .with_authorization_lease_verifier(Arc::new(|lease| lease.signature == "801-test-only"));
    let lease = host.pin_snapshot();
    let effect = lease.describe_effect("web_fetch", input);
    let authorization = ToolExecutionAuthorization {
        request_id: "801-call".into(),
        tool_id: effect.tool_id.clone(),
        descriptor_hash: effect.descriptor_hash.clone(),
        policy_revision: 1,
        scope: effect.scopes[0].clone(),
        timeout_lease: "801-timeout".into(),
        idempotency_key: Some("801-call".into()),
        authorization_lease: AuthorizationLease {
            lease_id: "801-lease".into(),
            principal_id: "801-test".into(),
            parent_lease_id: None,
            capability: effect.tool_id.clone(),
            scopes: effect.scopes.clone(),
            ceiling: effect.required_permission,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: "801-call".into(),
            policy_revision: 1,
            effect_descriptor_hash: effect.descriptor_hash.clone(),
            signature: "801-test-only".into(),
            status: AuthorizationLeaseStatus::Active,
        },
    };
    let hash = |domain, value: &Value| {
        harness_contract::step::canonical_digest(domain, value)
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
            .to_owned()
    };
    let binding = ToolExecutionBinding {
        call_key: harness_contract::step::canonical_digest("step-effect-identity", &serde_json::json!(["801-session", "801-step", "801-call"])).unwrap(),
        tool_use_id: "801-call".into(),
        input_sha256: hash("tool-effect-payload", input),
        descriptor_sha256: hash(
            harness_contract::context::TOOL_EXECUTION_DESCRIPTOR_DOMAIN,
            &serde_json::to_value(&effect).unwrap(),
        ),
        authority_sha256: "a".repeat(64),
    };
    (lease, authorization, binding)
}

#[test]
fn own823n_unknown_unsupported_and_retained_clone_are_not_positive_seals() {
    let input = serde_json::json!({"url":"https://example.invalid","prompt":"read"});
    let (mut lease, _, binding) = fixture(&input);
    let trace = Arc::new(std::sync::Mutex::new(LocalNetworkTrace::default()));
    lease.local_network_trace = Some(trace.clone());
    lease
        .begin_local_network_operation(&input, "https://example.invalid")
        .unwrap();
    lease
        .observe_local_network_dispatch("web_search", &serde_json::json!({"query":"test"}))
        .unwrap();
    assert!(seal_trace(binding.clone(), trace.clone()).is_err());
    drop(lease);
    let fact = seal_trace(binding, trace).unwrap();
    assert!(!fact.coverage_complete);
    assert!(matches!(
        fact.operations[0].terminal,
        LocalNetworkTerminal::Unknown { .. }
    ));
}

#[tokio::test]
async fn own823n_presend_refusal_and_foreign_binding_remain_distinct() {
    let input = serde_json::json!({
        "url":"https://example.invalid", "prompt":"read",
        "blocked_domains":["example.invalid"],
    });
    let (lease, authorization, binding) = fixture(&input);
    let mut foreign = binding.clone();
    foreign.call_key = "borrowed-key".into();
    assert!(lease
        .execute_joined_local_network(&authorization, "web_fetch", &input.to_string(), foreign, "801-session", &origin())
        .await
        .is_err());
    let (_, fact) = lease
        .execute_joined_local_network(&authorization, "web_fetch", &input.to_string(), binding, "801-session", &origin())
        .await
        .unwrap();
    assert!(fact.host_joined && fact.coverage_complete);
    assert_eq!(fact.operations.len(), 1);
    assert!(matches!(
        fact.operations[0].terminal,
        LocalNetworkTerminal::NotStarted { .. }
    ));
}

struct NetworkEnv(Option<std::ffi::OsString>);
impl Drop for NetworkEnv {
    fn drop(&mut self) {
        if let Some(value) = self.0.take() {
            std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", value);
        } else {
            std::env::remove_var("COWD_ALLOW_PRIVATE_NETWORK");
        }
    }
}

fn server(
    body: &'static str,
    declared: usize,
    delay: std::time::Duration,
) -> (String, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/801", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let worker = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut socket = loop {
            if let Ok((socket, _)) = listener.accept() {
                break socket;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "801 HTTP request never reached server"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut request = [0; 4096];
        socket.read(&mut request).unwrap();
        std::thread::sleep(delay);
        let _ = write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n{body}"
        );
    });
    (endpoint, worker)
}

#[tokio::test]
async fn own823n_real_body_done_body_error_and_timeout_cannot_share_positive_evidence() {
    let _guard = crate::test_process_environment_lock().lock().unwrap();
    let _environment = NetworkEnv(std::env::var_os("COWD_ALLOW_PRIVATE_NETWORK"));
    std::env::set_var("COWD_ALLOW_PRIVATE_NETWORK", "1");
    for mode in 0..3 {
        let (endpoint, worker) = server(
            "ok",
            if mode == 1 { 20 } else { 2 },
            if mode == 2 {
                std::time::Duration::from_millis(100)
            } else {
                std::time::Duration::ZERO
            },
        );
        let input = serde_json::json!({"url":endpoint,"prompt":"read"});
        let (lease, authorization, binding) = fixture(&input);
        let input = input.to_string();
        let original = origin();
        let call = lease.execute_joined_local_network(&authorization, "web_fetch", &input, binding, "801-session", &original);
        if mode == 2 {
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), call)
                    .await
                    .is_err()
            );
        } else if mode == 1 {
            assert!(call.await.is_err());
        } else {
            let (_, fact) = call.await.unwrap();
            assert!(fact.host_joined && fact.coverage_complete);
            assert!(matches!(
                &fact.operations[0].terminal,
                LocalNetworkTerminal::BodyDone {
                    status: 200,
                    body_bytes: 2,
                    ..
                }
            ));
        }
        worker.join().unwrap();
    }
}

#[tokio::test]
async fn own823n_distinct_step_effect_and_signed_call_namespaces_are_independently_checked() {
    let input = serde_json::json!({"url":"https://example.invalid","prompt":"read","blocked_domains":["example.invalid"]});
    let (lease, authorization, binding) = fixture(&input);
    assert_eq!(authorization.request_id, origin().call_id);
    assert_ne!(binding.call_key, authorization.authorization_lease.idempotency_key);
    let (_, fact) = lease.execute_joined_local_network(&authorization, "web_fetch", &input.to_string(), binding.clone(), "801-session", &origin()).await.unwrap();
    assert!(fact.host_joined && fact.coverage_complete);
    for fault in 0..10 {
        let mut grant = authorization.clone();
        let mut changed = binding.clone();
        let mut original = origin();
        let mut session = "801-session";
        match fault {
            0 => grant.request_id = "foreign-call".into(),
            1 => grant.authorization_lease.idempotency_key = "foreign-call".into(),
            2 => grant.idempotency_key = Some("foreign-call".into()),
            3 => changed.call_key = grant.request_id.clone(),
            4 => original.origin_step_id = "foreign-step".into(),
            5 => original.call_id = "foreign-call".into(),
            6 => session = "foreign-session",
            7 => grant.authorization_lease.signature = "foreign-signature".into(),
            8 => original.source = harness_contract::step::CallSource::OfflineTest,
            9 => changed.descriptor_sha256 = "f".repeat(64),
            _ => unreachable!(),
        }
        assert!(lease.execute_joined_local_network(&grant, "web_fetch", &input.to_string(), changed, session, &original).await.is_err(), "fault {fault}");
    }
    let mut optional = authorization.clone();
    optional.idempotency_key = None;
    let effect = lease.describe_effect("web_fetch", &input);
    if effect.idempotency != harness_contract::tool::ToolIdempotency::IdempotentWithKey {
        lease.execute_joined_local_network(&optional, "web_fetch", &input.to_string(), binding, "801-session", &origin()).await.unwrap();
    }
}

/// The joined path's acceptance set, in one place. Two tools are implemented and
/// the rest are not; the router and the executor both read this, so a tool can
/// never be routed into a path that cannot run it.
#[test]
fn the_joined_path_implements_exactly_two_tools() {
    for tool in ["web_fetch", "tool_orchestrate"] {
        assert!(joined_local_network_tool(tool), "{tool}");
    }
    for tool in ["web_search", "remote_trigger", "read_file", "bash", "execute_code", ""] {
        assert!(!joined_local_network_tool(tool), "{tool}");
    }
}

/// The property predicate is a necessary condition, never the routing decision:
/// a network read tool that the joined path does not implement must not be
/// accepted by the property alone.
#[test]
fn the_property_predicate_is_not_the_routing_decision() {
    for tool in ["web_search", "remote_trigger"] {
        let effect = ToolHost::builtin("r1-localnet-router", std::env::temp_dir())
            .pin_snapshot()
            .describe_effect(tool, &serde_json::json!({"query": "r1"}));
        assert!(effect.uses_network, "{tool} claims a network effect");
        assert!(!effect.spawns_process, "{tool} spawns no process");
        assert!(
            !joined_local_network_tool(tool),
            "{tool} satisfies the property but the joined path does not implement it"
        );
    }
}

/// Normalization happens before the routing decision: the gateway resolves the
/// canonical tool name first and routes on that, so a separator or case variant
/// of a joined tool still lands in the joined path and a variant of a
/// non-joined tool still does not. Without this, a future joined caller that
/// skips normalization would silently drift from the executor's acceptance set.
#[test]
fn a_spelling_variant_normalizes_before_the_routing_decision() {
    let lease = ToolHost::builtin("r1-localnet-variant", std::env::temp_dir()).pin_snapshot();
    let snapshot = lease.snapshot();
    for (requested, expected, joined) in [
        ("web_fetch", "web_fetch", true),
        ("web-fetch", "web_fetch", true),
        ("WEB-FETCH", "web_fetch", true),
        ("WebFetch", "web_fetch", true),
        ("tool_orchestrate", "tool_orchestrate", true),
        ("tool-orchestrate", "tool_orchestrate", true),
        ("web_search", "web_search", false),
        ("web-search", "web_search", false),
        ("websearch", "web_search", false),
        ("tool-search", "tool_search", false),
    ] {
        let canonical = snapshot
            .catalog
            .canonical_name(requested)
            .unwrap_or_else(|| panic!("{requested} must resolve"));
        assert_eq!(canonical, expected, "{requested}");
        assert_eq!(
            joined_local_network_tool(&canonical),
            joined,
            "{requested} resolves to {canonical}, whose joined membership must match"
        );
    }
}
