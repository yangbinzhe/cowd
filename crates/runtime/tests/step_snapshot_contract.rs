use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use harness_contract::policy::{AuthorizationLease, AuthorizationLeaseStatus};
use harness_contract::step::{CallSource, StepIdentity};
use harness_contract::tool::{ToolEffectDescriptor, ToolExecutionAuthorization};
use runtime::step_snapshot::{BoundStep, StepCallBinding};
use runtime::ProviderToolDefinition;
use serde_json::json;
use tools::{ToolCatalog, ToolHost, ToolHostLease, ToolHostSnapshot};

fn definitions(lease: &ToolHostLease, name: &str) -> Vec<ProviderToolDefinition> {
    lease
        .snapshot()
        .catalog
        .definitions(None)
        .into_iter()
        .filter(|definition| definition.name == name)
        .map(|definition| ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        })
        .collect()
}

fn step(lease: ToolHostLease, session: &str, name: &str, expires_at_ms: i64) -> BoundStep {
    let advertised = definitions(&lease, name);
    BoundStep::bind(
        StepIdentity {
            step_id: uuid::Uuid::new_v4().to_string(),
            session_id: session.to_string(),
            turn_id: format!("turn:{session}"),
            execution_id: Some(format!("execution:{session}")),
            actor_ref: Some("fixture".to_string()),
            run_ref: None,
            attempt: Some(1),
            epoch: Some("1".to_string()),
            goal_ref: None,
            program_ref: None,
            task_ref: None,
            build_ref: "isolated-test-fixture".to_string(),
        },
        lease,
        &advertised,
        &json!({"model":"fixture"}),
        &json!({"request":"fixture"}),
        BTreeMap::from([("policy".to_string(), "1".to_string())]),
        expires_at_ms,
    )
    .unwrap()
}

fn authorization(descriptor: &ToolEffectDescriptor, key: &str) -> ToolExecutionAuthorization {
    ToolExecutionAuthorization {
        request_id: "fixture-request".to_string(),
        tool_id: descriptor.tool_id.clone(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        policy_revision: 1,
        scope: descriptor.scopes[0].clone(),
        authorization_lease: AuthorizationLease {
            lease_id: "fixture-lease".to_string(),
            principal_id: "fixture-principal".to_string(),
            parent_lease_id: None,
            capability: descriptor.tool_id.clone(),
            scopes: descriptor.scopes.clone(),
            ceiling: descriptor.required_permission,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: key.to_string(),
            policy_revision: 1,
            effect_descriptor_hash: descriptor.descriptor_hash.clone(),
            signature: "isolated-test-signature".to_string(),
            status: AuthorizationLeaseStatus::Active,
        },
        timeout_lease: "fixture-timeout".to_string(),
        idempotency_key: Some(key.to_string()),
    }
}

#[test]
fn frozen_catalog_matches_advertised_schema_and_presentation() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let bound = step(host.pin_snapshot(), "session", "edit", i64::MAX);
    let mut advertised = definitions(bound.lease(), "edit");
    advertised[0].input_schema =
        json!({"type":"object","properties":{"different":{"type":"string"}}});
    assert!(BoundStep::bind(
        bound.snapshot().identity.clone(),
        host.pin_snapshot(),
        &advertised,
        &json!({}),
        &json!({}),
        BTreeMap::new(),
        i64::MAX
    )
    .is_err());
    let mut advertised = definitions(bound.lease(), "edit");
    advertised[0].description = Some("different contract".to_string());
    assert!(BoundStep::bind(
        bound.snapshot().identity.clone(),
        host.pin_snapshot(),
        &advertised,
        &json!({}),
        &json!({}),
        BTreeMap::new(),
        i64::MAX
    )
    .is_err());
}

#[test]
fn implementation_replacement_retains_inflight_handle_but_refuses_stale_restore() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let original = Arc::new(step(host.pin_snapshot(), "session", "edit", i64::MAX));
    let binding = StepCallBinding::provider(Arc::clone(&original), "call");
    let persisted: StepCallBinding =
        serde_json::from_value(serde_json::to_value(&binding).unwrap()).unwrap();
    assert_eq!(binding, persisted);
    host.replace_snapshot(ToolHostSnapshot::new(
        Arc::new(ToolCatalog::builtin()),
        Arc::new(tools::lsp_client::LspRegistry::new()),
        None,
    ));
    assert_ne!(
        original.lease().implementation_revision(),
        host.pin_snapshot().implementation_revision()
    );
    assert!(binding.resolve(Some(host.pin_snapshot())).is_ok());
    assert!(persisted.resolve(Some(host.pin_snapshot())).is_err());
    assert!(!directory.path().join("output.txt").exists());
}

#[test]
fn session_call_attempt_and_catalog_identity_are_all_checked_before_effect() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let bound = step(host.pin_snapshot(), "session-a", "edit", i64::MAX);
    let origin = bound.provider_origin("call-a");
    let input = r#"{"action":"write","path":"output.txt","content":"must not exist"}"#;
    bound
        .validate_call(&origin, "session-a", "call-a", "edit", input, 1)
        .unwrap();
    assert!(bound
        .validate_call(&origin, "session-b", "call-a", "edit", input, 1)
        .is_err());
    assert!(bound
        .validate_call(&origin, "session-a", "call-b", "edit", input, 1)
        .is_err());
    let mut forged = origin.clone();
    forged.call_id.clear();
    assert!(bound.validate_call(&forged, "session-a", "", "edit", input, 1).is_err());
    forged = origin.clone();
    forged.source = CallSource::Provider {
        attempt_id: "other-attempt".to_string(),
    };
    assert!(bound
        .validate_call(&forged, "session-a", "call-a", "edit", input, 1)
        .is_err());
    forged = origin.clone();
    forged.catalog_digest = "other-catalog".to_string();
    assert!(bound
        .validate_call(&forged, "session-a", "call-a", "edit", input, 1)
        .is_err());
    assert!(!directory.path().join("output.txt").exists());
}

#[test]
fn pinned_contract_does_not_freeze_live_authority() {
    let directory = tempfile::tempdir().unwrap();
    let live = Arc::new(AtomicBool::new(true));
    let verifier_live = Arc::clone(&live);
    let host = ToolHost::builtin("fixture", directory.path()).with_authorization_lease_verifier(
        Arc::new(move |lease| {
            verifier_live.load(Ordering::SeqCst) && lease.signature == "isolated-test-signature"
        }),
    );
    let bound = step(host.pin_snapshot(), "session", "edit", i64::MAX);
    let origin = bound.provider_origin("call");
    let input = bound
        .validate_call(
            &origin,
            "session",
            "call",
            "edit",
            r#"{"action":"write","path":"output.txt","content":"accepted"}"#,
            1,
        )
        .unwrap();
    let descriptor = bound.lease().describe_effect("edit", &input);
    let authorization = authorization(&descriptor, "effect-one");
    live.store(false, Ordering::SeqCst);
    assert!(bound
        .lease()
        .execute(&authorization, "edit", &input)
        .is_err());
    assert!(!directory.path().join("output.txt").exists());
    live.store(true, Ordering::SeqCst);
    bound
        .lease()
        .execute(&authorization, "edit", &input)
        .unwrap();
    live.store(false, Ordering::SeqCst);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("output.txt")).unwrap(),
        "accepted"
    );
}

#[test]
fn expired_or_offline_or_unfenced_system_origin_never_admits_new_effect() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let bound = step(host.pin_snapshot(), "session", "edit", 10);
    let mut origin = bound.provider_origin("call");
    let input = r#"{"action":"write","path":"output.txt","content":"forbidden"}"#;
    bound
        .validate_call(&origin, "session", "call", "edit", input, 1)
        .unwrap();
    assert!(bound
        .validate_call(&origin, "session", "call", "edit", input, 10)
        .is_err());
    origin.source = CallSource::OfflineTest;
    assert!(bound
        .validate_call(&origin, "session", "call", "edit", input, 1)
        .is_err());
    origin.source = CallSource::SystemDerived {
        parent_step_id: "unknown-parent".to_string(),
    };
    assert!(bound
        .validate_call(&origin, "session", "call", "edit", input, 1)
        .is_err());
    assert!(!directory.path().join("output.txt").exists());
}

#[test]
fn duplicate_keys_and_outside_catalog_calls_are_rejected_without_writes() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let bound = step(host.pin_snapshot(), "session", "edit", i64::MAX);
    let origin = bound.provider_origin("call");
    assert!(bound
        .validate_call(
            &origin,
            "session",
            "call",
            "edit",
            r#"{"action":"write","path":"a","path":"b","content":"forbidden"}"#,
            1
        )
        .is_err());
    assert!(bound
        .validate_call(
            &origin,
            "session",
            "call",
            "bash",
            r#"{"command":"false"}"#,
            1
        )
        .is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn canonical_digest_matches_independent_cross_language_goldens() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/step-digest-v1.json")).unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let input =
            harness_contract::step::parse_strict_json(fixture["json"].as_str().unwrap()).unwrap();
        let digest = harness_contract::step::canonical_digest("golden", &input).unwrap();
        assert_eq!(
            digest,
            fixture["digest"].as_str().unwrap(),
            "{}",
            fixture["name"]
        );
    }
}

#[test]
fn restored_binding_checks_actual_schema_even_when_catalog_digest_is_recomputed() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let bound = step(host.pin_snapshot(), "session", "edit", i64::MAX);
    BoundStep::restore(bound.snapshot().clone(), host.pin_snapshot()).unwrap();
    for field in ["schema", "presentation", "resolver", "duplicate"] {
        let mut snapshot = bound.snapshot().clone();
        match field {
            "schema" => snapshot.tools[0].schema_digest = "forged-schema".to_string(),
            "presentation" => snapshot.tools[0].examples_digest = "forged-presentation".to_string(),
            "resolver" => {
                snapshot.tools[0].effect_descriptor_digest = "forged-resolver".to_string()
            }
            "duplicate" => snapshot.tools.push(snapshot.tools[0].clone()),
            _ => unreachable!(),
        }
        snapshot.catalog_digest = harness_contract::step::canonical_digest(
            "step-tool-catalog",
            &serde_json::to_value(&snapshot.tools).unwrap(),
        )
        .unwrap();
        assert!(
            BoundStep::restore(snapshot, host.pin_snapshot()).is_err(),
            "{field}"
        );
    }
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn catalog_registration_is_unknown_readiness_not_a_live_authorization_grant() {
    let directory = tempfile::tempdir().unwrap();
    let host = ToolHost::builtin("fixture", directory.path());
    let bound = step(host.pin_snapshot(), "session", "read_file", i64::MAX);
    let tool = &bound.snapshot().tools[0];
    assert_eq!(
        tool.availability,
        harness_contract::step::CatalogAvailability::Unknown
    );
    assert!(tool.availability_reason.contains("registered_only"));
    assert!(tool.observed_at_ms > 0);
    let input = bound
        .validate_call(
            &bound.provider_origin("read"),
            "session",
            "read",
            "read_file",
            r#"{"path":"absent.txt"}"#,
            1,
        )
        .unwrap();
    let effect = bound.lease().describe_effect("read_file", &input);
    assert!(bound
        .lease()
        .execute(&authorization(&effect, "read"), "read_file", &input)
        .is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn missing_mcp_configuration_is_unavailable_without_a_network_probe() {
    let directory = tempfile::tempdir().unwrap();
    let catalog = ToolCatalog::builtin()
        .with_runtime_tools(vec![tools::RuntimeToolDefinition {
            name: "mcp_tool".into(),
            description: None,
            input_schema: json!({"type":"object","additionalProperties":true}),
            required_permission: harness_contract::policy::PermissionMode::ReadOnly,
            effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                resolver_id: "runtime.readonly".into(),
                resolver_version: 1,
            },
        }])
        .unwrap();
    let host = ToolHost::new(
        "fixture",
        directory.path(),
        ToolHostSnapshot::new(
            Arc::new(catalog),
            Arc::new(tools::lsp_client::LspRegistry::new()),
            None,
        ),
    );
    let lease = host.pin_snapshot();
    assert_eq!(
        lease
            .catalog_receipt()
            .descriptors
            .into_iter()
            .find(|descriptor| descriptor.canonical_id == "mcp_tool")
            .unwrap()
            .health,
        harness_contract::tool::ToolDescriptorHealth::Unavailable
    );
    let mut identity = step(lease.clone(), "session", "read_file", i64::MAX)
        .snapshot()
        .identity
        .clone();
    identity.step_id = "unavailable-mcp-step".into();
    assert!(BoundStep::bind(
        identity,
        lease.clone(),
        &definitions(&lease, "mcp_tool"),
        &json!({}),
        &json!({}),
        BTreeMap::new(),
        i64::MAX
    )
    .is_err());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn concurrent_sessions_keep_catalogs_scopes_and_physical_outputs_separate() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let execute = |directory: &std::path::Path,
                   session: &'static str,
                   tool_name: &'static str,
                   barrier: Arc<std::sync::Barrier>| {
        let host = ToolHost::builtin(session, directory).with_authorization_lease_verifier(
            Arc::new(|lease| lease.signature == "isolated-test-signature"),
        );
        let bound = step(host.pin_snapshot(), session, tool_name, i64::MAX);
        let opposite = if session == "first" {
            "second"
        } else {
            "first"
        };
        std::thread::spawn(move || {
            barrier.wait();
            let origin = bound.provider_origin("same-call-id");
            assert!(bound
                .validate_call(&origin, opposite, "same-call-id", tool_name, "{}", 1)
                .is_err());
            let input = json!({"action":"write", "path":"output.txt", "content":session});
            let input = bound
                .validate_call(
                    &origin,
                    session,
                    "same-call-id",
                    tool_name,
                    &input.to_string(),
                    1,
                )
                .unwrap();
            let effect = bound.lease().describe_effect(tool_name, &input);
            bound
                .lease()
                .execute(&authorization(&effect, session), tool_name, &input)
                .unwrap();
            (
                bound.snapshot().identity.session_id.clone(),
                bound.snapshot().catalog_digest.clone(),
            )
        })
    };
    let first_task = execute(first.path(), "first", "edit", Arc::clone(&barrier));
    let second_task = execute(second.path(), "second", "edit", barrier);
    let first_result = first_task.join().unwrap();
    let second_result = second_task.join().unwrap();
    assert_ne!(first_result, second_result);
    assert_eq!(
        std::fs::read_to_string(first.path().join("output.txt")).unwrap(),
        "first"
    );
    assert_eq!(
        std::fs::read_to_string(second.path().join("output.txt")).unwrap(),
        "second"
    );
    assert_eq!(std::fs::read_dir(first.path()).unwrap().count(), 1);
    assert_eq!(std::fs::read_dir(second.path()).unwrap().count(), 1);
}
