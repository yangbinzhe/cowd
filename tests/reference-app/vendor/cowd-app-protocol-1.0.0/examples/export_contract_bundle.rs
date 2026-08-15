#![allow(clippy::expect_used)]

use std::{collections::BTreeMap, env, fs, path::PathBuf};

use cowd_app_protocol::*;
use schemars::{schema_for, JsonSchema, Schema};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = output_directory()?;
    let schemas = output.join("schemas");
    let golden = output.join("golden");
    fs::create_dir_all(&schemas)?;
    fs::create_dir_all(&golden)?;

    let mut files = BTreeMap::<String, Vec<u8>>::new();
    add_schema::<AppManifestV1>(&mut files, "app-manifest.schema.json")?;
    add_schema::<AppHandshakeRequestV1>(&mut files, "handshake-request.schema.json")?;
    add_schema::<AppHandshakeV1>(&mut files, "handshake.schema.json")?;
    add_schema::<AppResultContractV1>(&mut files, "result-contract.schema.json")?;
    add_schema::<AppHealthV1>(&mut files, "health.schema.json")?;
    add_schema::<AppCatalogV1>(&mut files, "catalog.schema.json")?;
    add_schema::<CoreOperationCatalogV1>(&mut files, "core-operation-catalog.schema.json")?;
    add_schema::<OperationDescriptorV1>(&mut files, "operation.schema.json")?;
    add_schema::<AppInvocationEnvelopeV1>(&mut files, "invocation.schema.json")?;
    add_schema::<CoreBridgeInvocationV1>(&mut files, "core-bridge-invocation.schema.json")?;
    add_schema::<AppProviderResponseV1>(&mut files, "provider-response.schema.json")?;
    add_schema::<DurableReceiptV1>(&mut files, "receipt.schema.json")?;
    add_schema::<AppStreamFrameV1>(&mut files, "stream-frame.schema.json")?;
    add_schema::<AppErrorResponseV1>(&mut files, "error.schema.json")?;
    add_schema::<AppViewDocumentV1>(&mut files, "tui-view.schema.json")?;
    add_schema::<AppViewPatchV1>(&mut files, "tui-patch.schema.json")?;
    add_schema::<AppActionV1>(&mut files, "tui-action.schema.json")?;
    add_schema::<AppTuiViewOpenRequestV1>(&mut files, "tui-open-request.schema.json")?;
    add_schema::<AppTuiViewOpenResponseV1>(&mut files, "tui-open-response.schema.json")?;
    add_schema::<AppTuiViewActionResponseV1>(&mut files, "tui-action-response.schema.json")?;
    add_schema::<AppTuiViewStreamRequestV1>(&mut files, "tui-stream-request.schema.json")?;
    add_schema::<IframeBridgeMessageV1>(&mut files, "iframe-bridge.schema.json")?;
    add_schema::<IframeApiFrameV1>(&mut files, "iframe-api.schema.json")?;
    add_schema::<ApplicationExecutionOutcomeV1>(
        &mut files,
        "application-execution-outcome.schema.json",
    )?;
    add_schema::<ApplicationExecutionSummaryV1>(
        &mut files,
        "application-execution-summary.schema.json",
    )?;
    add_schema::<ApplicationExecutionSummaryIntentV1>(
        &mut files,
        "application-execution-summary-intent.schema.json",
    )?;
    add_schema::<ApplicationExecutionSummaryReceiptV1>(
        &mut files,
        "application-execution-summary-receipt.schema.json",
    )?;

    let fixtures = golden_fixtures();
    for (name, value) in fixtures {
        files.insert(format!("golden/{name}"), canonical_json(&value)?);
    }
    files.insert("openapi.json".to_owned(), canonical_json(&openapi())?);

    for (relative, bytes) in &files {
        let target = output.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(target, bytes)?;
    }

    let protocol_digest = digest_files(&files);
    let file_digests = files
        .iter()
        .map(|(path, bytes)| (path.clone(), digest_bytes(bytes)))
        .collect::<BTreeMap<_, _>>();
    let manifest = json!({
        "schema_version": 1,
        "protocol_revision": PROTOCOL_REVISION_V1,
        "protocol_digest": protocol_digest,
        "files": file_digests,
    });
    fs::write(
        output.join("contract-manifest.json"),
        canonical_json(&manifest)?,
    )?;
    Ok(())
}

fn output_directory() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    match (
        arguments.next().as_deref(),
        arguments.next(),
        arguments.next(),
    ) {
        (Some("--output"), Some(path), None) => Ok(PathBuf::from(path)),
        _ => Err("usage: export_contract_bundle --output <directory>".into()),
    }
}

fn add_schema<T: JsonSchema>(
    files: &mut BTreeMap<String, Vec<u8>>,
    name: &str,
) -> Result<(), serde_json::Error> {
    let schema: Schema = schema_for!(T);
    files.insert(format!("schemas/{name}"), canonical_json(&schema)?);
    Ok(())
}

fn canonical_json(value: &impl serde::Serialize) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn digest_files(files: &BTreeMap<String, Vec<u8>>) -> String {
    let mut digest = Sha256::new();
    for (path, bytes) in files {
        digest.update(path.as_bytes());
        digest.update([0]);
        digest.update(bytes);
        digest.update([0]);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn golden_fixtures() -> BTreeMap<&'static str, Value> {
    let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let principal = json!({
        "subject": "user:1",
        "tenant_id": "tenant:1",
        "workspace_id": "workspace:1",
        "delegation": "user",
        "grant_id": "grant:1",
        "authorization_profile_id": "operator",
        "authorization_revision": 7,
        "granted_capabilities": ["approval.respond", "reference-app.read", "reference-app.write"],
        "granted_scopes": ["workspace:read", "workspace:write"],
        "credential_epoch": 11,
        "expires_at_unix_ms": 4_000_000_000_000_u64
    });
    let execution = json!({
        "surface": "web",
        "session_id": "session:1",
        "turn_id": "turn:1",
        "task_id": "task:1"
    });
    let command_invocation = json!({
        "schema_version": 1,
        "operation_id": "reference.command.v1",
        "request_id": "request:1",
        "correlation_id": "correlation:1",
        "deadline_unix_ms": 4_000_000_000_000_u64,
        "idempotency_key": "idempotency:1",
        "expected_revision": "7",
        "call_chain": ["core:runtime"],
        "max_hops": 4,
        "input_schema_digest": digest,
        "principal": principal,
        "execution": execution,
        "payload": {"value": 1}
    });
    let query_invocation = json!({
        "schema_version": 1,
        "operation_id": "reference.query.v1",
        "request_id": "request:query:1",
        "correlation_id": "correlation:1",
        "deadline_unix_ms": 4_000_000_000_000_u64,
        "call_chain": ["surface:web"],
        "max_hops": 4,
        "input_schema_digest": digest,
        "principal": command_invocation["principal"].clone(),
        "execution": command_invocation["execution"].clone(),
        "payload": {"filter": "active"}
    });
    let mut core_invocation = command_invocation.clone();
    core_invocation["operation_id"] = json!("core.reference.command.v1");
    core_invocation["call_chain"] = json!(["app:reference-app"]);
    let mut fixtures = BTreeMap::from([
        (
            "handshake-success.json",
            json!({
                "schema_version": 1,
                "protocol_revision": 1,
                "app_id": "reference-app",
                "generation": digest,
                "artifact_version": "1.0.0",
                "worker_pid": 4242,
                "worker_nonce": "reference-worker-nonce",
                "operations": [],
                "operation_catalog_digest": digest,
                "capability_digest": digest,
                "authorization_profile_digest": digest
            }),
        ),
        (
            "query-success.json",
            json!({
                "schema_version": 1,
                "request_id": "request-query-1",
                "output_schema_digest": digest,
                "revision": "7",
                "payload": {"items": []}
            }),
        ),
        (
            "command-receipt.json",
            json!({
                "schema_version": 1,
                "request_id": "request-command-1",
                "receipt_id": "receipt-1",
                "idempotency_key": "command-key-1",
                "status": "completed",
                "result_revision": "8",
                "replayed": false,
                "payload_digest": digest,
                "payload": {"accepted": true}
            }),
        ),
        ("query-invocation.json", query_invocation),
        ("command-invocation.json", command_invocation.clone()),
        (
            "core-bridge-invocation.json",
            json!({
                "schema_version": 1,
                "originating_app_operation_id": "reference-app.command.v1",
                "invocation": core_invocation
            }),
        ),
        (
            "stream-open.json",
            json!({
                "kind": "open",
                "schema_version": 1,
                "subscription_id": "subscription-1",
                "sequence": 0,
                "schema_digest": digest
            }),
        ),
        (
            "error-cycle.json",
            json!({
                "schema_version": 1,
                "error": {
                    "code": "CALL_CYCLE_DETECTED",
                    "message": "synchronous authority cycle rejected",
                    "retryable": false,
                    "details": {},
                    "receipt_id": null
                }
            }),
        ),
        (
            "iframe-host-init.json",
            json!({
                "kind": "host_init",
                "schema_version": 1,
                "app_id": "reference-app",
                "frame_nonce": "frame-nonce-1",
                "message_id": "message-1",
                "protocol_digest": digest,
                "catalog_generation": digest
            }),
        ),
        (
            "application-execution-summary.json",
            json!({
                "schema_version": 1,
                "summary_id": "summary-1",
                "kind": "application_action",
                "status": "succeeded",
                "title": "Reference action completed",
                "summary": "The signed reference APP action completed.",
                "domain": "reference",
                "refs": [{"type": "action", "id": "action-1"}],
                "evidence_refs": ["evidence:reference:1"],
                "metric_refs": ["metric:reference:latency"],
                "counters": [{"name": "affected_rows", "value": 1}],
                "occurred_at_ms": 42
            }),
        ),
    ]);

    let manifest = reference_manifest();
    let app_operations = reference_app_operations();
    let catalog = reference_core_catalog(&manifest);
    fixtures
        .get_mut("handshake-success.json")
        .expect("handshake fixture")["operations"] =
        serde_json::to_value(&app_operations).expect("APP operations JSON");
    fixtures
        .get_mut("handshake-success.json")
        .expect("handshake fixture")["operation_catalog_digest"] =
        serde_json::to_value(&manifest.operation_catalog_digest)
            .expect("operation catalog digest JSON");
    fixtures
        .get_mut("handshake-success.json")
        .expect("handshake fixture")["capability_digest"] = serde_json::to_value(
        manifest_capability_digest_v1(&manifest).expect("reference capability digest"),
    )
    .expect("capability digest JSON");
    fixtures
        .get_mut("handshake-success.json")
        .expect("handshake fixture")["authorization_profile_digest"] = serde_json::to_value(
        manifest_authorization_profile_digest_v1(&manifest)
            .expect("reference authorization profile digest"),
    )
    .expect("authorization profile digest JSON");
    fixtures.insert(
        "app-manifest.json",
        serde_json::to_value(&manifest).expect("reference manifest JSON"),
    );
    fixtures.insert(
        "core-operation-catalog.json",
        serde_json::to_value(&catalog).expect("reference core catalog JSON"),
    );
    fixtures.insert(
        "manifest-digests.json",
        json!({
            "operation_catalog_digest": manifest.operation_catalog_digest,
            "capability_digest": manifest_capability_digest_v1(&manifest)
                .expect("reference capability digest"),
            "authorization_profile_digest": manifest_authorization_profile_digest_v1(&manifest)
                .expect("reference profile digest")
        }),
    );

    let mut requirement_tamper = serde_json::to_value(&manifest).expect("manifest tamper JSON");
    requirement_tamper["core_bridge_requirements"][0]["core_operation_id"] =
        json!("core.tampered.command.v1");
    fixtures.insert(
        "negative/manifest-requirement-tamper.json",
        requirement_tamper,
    );

    let mut unsorted_capabilities = manifest.clone();
    unsorted_capabilities.capabilities.reverse();
    unsorted_capabilities
        .bind_canonical_signed_digest()
        .expect("bind unsorted capability fixture");
    fixtures.insert(
        "negative/manifest-unsorted-capabilities.json",
        serde_json::to_value(unsorted_capabilities).expect("unsorted capability JSON"),
    );

    let mut no_default = manifest.clone();
    no_default.authorization_profiles[0].is_default = false;
    no_default
        .bind_canonical_signed_digest()
        .expect("bind no-default fixture");
    fixtures.insert(
        "negative/manifest-no-default-profile.json",
        serde_json::to_value(no_default).expect("no-default profile JSON"),
    );

    let mut duplicate_requirement = manifest.clone();
    let duplicate = duplicate_requirement.core_bridge_requirements[0].clone();
    duplicate_requirement
        .core_bridge_requirements
        .push(duplicate);
    duplicate_requirement
        .bind_canonical_signed_digest()
        .expect("bind duplicate requirement fixture");
    fixtures.insert(
        "negative/manifest-duplicate-requirement.json",
        serde_json::to_value(duplicate_requirement).expect("duplicate requirement JSON"),
    );

    let mut catalog_tamper = serde_json::to_value(&catalog).expect("catalog tamper JSON");
    catalog_tamper["operations"][0]["audit_classification"] = json!("tampered");
    fixtures.insert("negative/core-catalog-tamper.json", catalog_tamper);

    let mut unauthorized_catalog = catalog.clone();
    let mut unauthorized_operation = reference_operation();
    unauthorized_operation.operation_id = "core.unrequested.command.v1".to_owned();
    unauthorized_catalog.operations.push(unauthorized_operation);
    unauthorized_catalog
        .operations
        .sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    unauthorized_catalog
        .bind_canonical_catalog_digest()
        .expect("bind unauthorized catalog fixture");
    fixtures.insert(
        "negative/core-catalog-unauthorized.json",
        serde_json::to_value(unauthorized_catalog).expect("unauthorized catalog JSON"),
    );

    let mut remapped_catalog = catalog.clone();
    remapped_catalog.operations[0].operation_id = "core.remapped.command.v1".to_owned();
    remapped_catalog
        .bind_canonical_catalog_digest()
        .expect("bind remapped catalog fixture");
    fixtures.insert(
        "negative/core-catalog-remapped.json",
        serde_json::to_value(remapped_catalog).expect("remapped catalog JSON"),
    );

    let mut schema_mismatch_catalog = catalog.clone();
    schema_mismatch_catalog.operations[0].input_schema_digest = Sha256Digest(
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
    );
    schema_mismatch_catalog
        .bind_canonical_catalog_digest()
        .expect("bind schema mismatch catalog fixture");
    fixtures.insert(
        "negative/core-catalog-schema-mismatch.json",
        serde_json::to_value(schema_mismatch_catalog).expect("schema mismatch catalog JSON"),
    );

    let mut capability_mismatch_catalog = catalog.clone();
    capability_mismatch_catalog.operations[0].required_capabilities =
        vec!["reference-app.write".to_owned()];
    capability_mismatch_catalog
        .bind_canonical_catalog_digest()
        .expect("bind capability mismatch catalog fixture");
    fixtures.insert(
        "negative/core-catalog-capability-mismatch.json",
        serde_json::to_value(capability_mismatch_catalog)
            .expect("capability mismatch catalog JSON"),
    );

    let mut unsigned_app_capability_catalog = catalog.clone();
    unsigned_app_capability_catalog.operations[0].required_capabilities = vec![
        "approval.respond".to_owned(),
        "reference-app.read".to_owned(),
    ];
    unsigned_app_capability_catalog
        .bind_canonical_catalog_digest()
        .expect("bind unsigned APP capability catalog fixture");
    fixtures.insert(
        "negative/core-catalog-unsigned-app-capability.json",
        serde_json::to_value(unsigned_app_capability_catalog)
            .expect("unsigned APP capability catalog JSON"),
    );

    let mut empty_requirements = reference_operation();
    empty_requirements.required_capabilities.clear();
    fixtures.insert(
        "negative/operation-empty-required-capabilities.json",
        serde_json::to_value(empty_requirements).expect("empty operation requirements JSON"),
    );

    let mut duplicate_requirements = reference_operation();
    duplicate_requirements
        .required_capabilities
        .push("approval.respond".to_owned());
    fixtures.insert(
        "negative/operation-duplicate-required-capabilities.json",
        serde_json::to_value(duplicate_requirements)
            .expect("duplicate operation requirements JSON"),
    );

    let mut unsorted_requirements = reference_operation();
    unsorted_requirements
        .required_capabilities
        .push("reference-app.write".to_owned());
    unsorted_requirements.required_capabilities.reverse();
    fixtures.insert(
        "negative/operation-unsorted-required-capabilities.json",
        serde_json::to_value(unsorted_requirements).expect("unsorted operation requirements JSON"),
    );

    let mut legacy_requirement =
        serde_json::to_value(reference_operation()).expect("legacy operation requirement JSON");
    legacy_requirement
        .as_object_mut()
        .expect("operation descriptor object")
        .remove("required_capabilities");
    legacy_requirement["required_capability"] = json!("reference-app.write");
    fixtures.insert(
        "negative/operation-legacy-required-capability.json",
        legacy_requirement,
    );

    let mut cross_namespace_manifest = manifest.clone();
    cross_namespace_manifest.capabilities = vec!["approval.respond".to_owned()];
    cross_namespace_manifest.authorization_profiles[0].capabilities =
        vec!["approval.respond".to_owned()];
    cross_namespace_manifest.core_bridge_requirements[0].required_app_capabilities =
        vec!["approval.respond".to_owned()];
    cross_namespace_manifest
        .bind_canonical_signed_digest()
        .expect("bind cross-namespace manifest fixture");
    fixtures.insert(
        "negative/manifest-cross-namespace-capability.json",
        serde_json::to_value(cross_namespace_manifest).expect("cross-namespace manifest JSON"),
    );

    let mut cross_namespace_profile = manifest.clone();
    cross_namespace_profile.authorization_profiles[0]
        .capabilities
        .push("approval.respond".to_owned());
    cross_namespace_profile.authorization_profiles[0]
        .capabilities
        .sort();
    cross_namespace_profile
        .bind_canonical_signed_digest()
        .expect("bind cross-namespace profile fixture");
    fixtures.insert(
        "negative/manifest-cross-namespace-profile.json",
        serde_json::to_value(cross_namespace_profile).expect("cross-namespace profile JSON"),
    );

    let mut cross_namespace_surface = manifest.clone();
    cross_namespace_surface.authorization_profiles[0]
        .surface_capabilities
        .insert("web".to_owned(), vec!["approval.respond".to_owned()]);
    cross_namespace_surface
        .bind_canonical_signed_digest()
        .expect("bind cross-namespace surface fixture");
    fixtures.insert(
        "negative/manifest-cross-namespace-surface.json",
        serde_json::to_value(cross_namespace_surface).expect("cross-namespace surface JSON"),
    );

    let mut catalog_unknown = serde_json::to_value(&catalog).expect("catalog unknown JSON");
    catalog_unknown
        .as_object_mut()
        .expect("catalog object")
        .insert("system_operations".to_owned(), json!([]));
    fixtures.insert("negative/core-catalog-unknown.json", catalog_unknown);

    let mut missing_revision = command_invocation.clone();
    missing_revision["principal"]
        .as_object_mut()
        .expect("principal fixture")
        .remove("authorization_revision");
    fixtures.insert(
        "negative/missing-authorization-revision.json",
        missing_revision,
    );

    let mut duplicate_capability = command_invocation.clone();
    duplicate_capability["principal"]["granted_capabilities"] =
        json!(["reference-app.write", "reference-app.write"]);
    fixtures.insert("negative/duplicate-capability.json", duplicate_capability);

    let mut unsorted_scope = command_invocation.clone();
    unsorted_scope["principal"]["granted_scopes"] = json!(["workspace:write", "workspace:read"]);
    fixtures.insert("negative/unsorted-scope.json", unsorted_scope);

    let mut unknown_principal = command_invocation.clone();
    unknown_principal["principal"]["unverified_role"] = json!("admin");
    fixtures.insert("negative/unknown-principal-field.json", unknown_principal);

    let mut expired = command_invocation.clone();
    expired["principal"]["expires_at_unix_ms"] = json!(1_700_000_000_000_u64);
    fixtures.insert("negative/expired-grant.json", expired);

    let mut wrong_delegation = command_invocation.clone();
    wrong_delegation["principal"]["delegation"] = json!("service");
    fixtures.insert("negative/wrong-delegation.json", wrong_delegation);

    let mut missing_capability = command_invocation;
    missing_capability["principal"]["granted_capabilities"] =
        json!(["reference-app.read", "reference-app.write"]);
    fixtures.insert("negative/missing-capability.json", missing_capability);
    fixtures
}

fn reference_operation() -> OperationDescriptorV1 {
    let digest = Sha256Digest(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    );
    OperationDescriptorV1 {
        operation_id: "core.reference.command.v1".to_owned(),
        kind: OperationKindV1::Command,
        input_schema_digest: digest.clone(),
        output_schema_digest: digest,
        required_capabilities: vec!["approval.respond".to_owned()],
        delegation: OperationDelegationV1::User,
        tenant_scoped: true,
        workspace_scoped: true,
        read_only: false,
        idempotency: IdempotencySemanticsV1::Required,
        default_deadline_ms: 3_000,
        maximum_deadline_ms: 10_000,
        maximum_request_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        maximum_frame_bytes: 1_048_576,
        streaming: false,
        replay_window_seconds: None,
        degraded_read_allowed: false,
        audit_classification: "domain_write".to_owned(),
    }
}

fn reference_app_operations() -> Vec<OperationDescriptorV1> {
    let digest = Sha256Digest(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    );
    let mut operations = vec![
        OperationDescriptorV1 {
            operation_id: "reference-app.command.v1".to_owned(),
            kind: OperationKindV1::Command,
            input_schema_digest: digest.clone(),
            output_schema_digest: digest,
            required_capabilities: vec!["reference-app.write".to_owned()],
            delegation: OperationDelegationV1::User,
            tenant_scoped: true,
            workspace_scoped: true,
            read_only: false,
            idempotency: IdempotencySemanticsV1::Required,
            default_deadline_ms: 3_000,
            maximum_deadline_ms: 10_000,
            maximum_request_bytes: 65_536,
            maximum_response_bytes: 1_048_576,
            maximum_frame_bytes: 1_048_576,
            streaming: false,
            replay_window_seconds: None,
            degraded_read_allowed: false,
            audit_classification: "domain_write".to_owned(),
        },
        tui_operation(
            "reference-app.tui.main.action",
            OperationKindV1::Command,
            "reference-app.write",
            app_tui_view_action_request_schema_digest_v1()
                .expect("TUI action request schema digest"),
            app_tui_view_action_response_schema_digest_v1()
                .expect("TUI action response schema digest"),
        ),
        tui_operation(
            "reference-app.tui.main.open",
            OperationKindV1::Query,
            "reference-app.read",
            app_tui_view_open_request_schema_digest_v1().expect("TUI open request schema digest"),
            app_tui_view_open_response_schema_digest_v1().expect("TUI open response schema digest"),
        ),
        tui_operation(
            "reference-app.tui.main.stream",
            OperationKindV1::Subscribe,
            "reference-app.read",
            app_tui_view_stream_request_schema_digest_v1()
                .expect("TUI stream request schema digest"),
            app_tui_view_patch_schema_digest_v1().expect("TUI view patch schema digest"),
        ),
    ];
    operations.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    operations
}

fn tui_operation(
    operation_id: &str,
    kind: OperationKindV1,
    capability: &str,
    input_schema_digest: Sha256Digest,
    output_schema_digest: Sha256Digest,
) -> OperationDescriptorV1 {
    let (read_only, idempotency, streaming, replay_window_seconds) = match kind {
        OperationKindV1::Query => (true, IdempotencySemanticsV1::ReadOnly, false, None),
        OperationKindV1::Command => (false, IdempotencySemanticsV1::Required, false, None),
        OperationKindV1::Subscribe => (
            true,
            IdempotencySemanticsV1::SubscriptionCursor,
            true,
            Some(60),
        ),
        OperationKindV1::Export => (true, IdempotencySemanticsV1::ContentAddressed, true, None),
    };
    OperationDescriptorV1 {
        operation_id: operation_id.to_owned(),
        kind,
        input_schema_digest,
        output_schema_digest,
        required_capabilities: vec![capability.to_owned()],
        delegation: OperationDelegationV1::User,
        tenant_scoped: true,
        workspace_scoped: true,
        read_only,
        idempotency,
        default_deadline_ms: 3_000,
        maximum_deadline_ms: 30_000,
        maximum_request_bytes: 65_536,
        maximum_response_bytes: 1_048_576,
        maximum_frame_bytes: 1_048_576,
        streaming,
        replay_window_seconds,
        degraded_read_allowed: read_only,
        audit_classification: "tui_interaction".to_owned(),
    }
}

fn reference_manifest() -> AppManifestV1 {
    let digest = Sha256Digest(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
    );
    let app_id = AppId("reference-app".to_owned());
    let operation_catalog_digest =
        app_operation_catalog_digest_v1(&app_id, &reference_app_operations())
            .expect("reference APP operation catalog digest");
    let mut manifest = AppManifestV1 {
        schema_version: 1,
        app_id,
        display_name: "Reference APP".to_owned(),
        artifact_version: "1.0.0".to_owned(),
        required_protocol: ProtocolRangeV1::exact_v1(),
        executable: "bin/reference-worker".to_owned(),
        web_root: Some("webui".to_owned()),
        capabilities: vec![
            "reference-app.read".to_owned(),
            "reference-app.write".to_owned(),
        ],
        authorization_profiles: vec![AuthorizationProfileV1 {
            profile_id: "operator".to_owned(),
            display_name: "Operator".to_owned(),
            capabilities: vec![
                "reference-app.read".to_owned(),
                "reference-app.write".to_owned(),
            ],
            surface_capabilities: BTreeMap::new(),
            is_default: true,
        }],
        operation_catalog_digest,
        core_bridge_requirements: vec![CoreBridgeRequirementV1 {
            app_operation_id: "reference-app.command.v1".to_owned(),
            core_operation_id: "core.reference.command.v1".to_owned(),
            accepted_input_schema_digest: digest.clone(),
            accepted_output_schema_digest: digest.clone(),
            required_app_capabilities: vec!["reference-app.write".to_owned()],
            kind: OperationKindV1::Command,
            streaming: false,
        }],
        surfaces: AppSurfacesV1 {
            web: true,
            tui_view: true,
        },
        integrity: BundleIntegrityV1 {
            algorithm: IntegrityAlgorithmV1::Sha256,
            files: BTreeMap::from([("bin/reference-worker".to_owned(), digest.clone())]),
            manifest_digest: digest.clone(),
        },
        signature: BundleSignatureV1 {
            algorithm: SignatureAlgorithmV1::Ed25519,
            key_id: "release-key-1".to_owned(),
            signature: "base64url-signature".to_owned(),
            signed_digest: digest.clone(),
            expires_unix_ms: None,
            provenance_digest: Some(digest.clone()),
        },
        sandbox: SandboxProfileV1 {
            filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
            network: NetworkPolicyV1::Deny,
            max_processes: 8,
            max_open_files: 256,
            max_memory_bytes: 256 * 1024 * 1024,
            cpu_quota_millis_per_second: 1_000,
        },
        presentation: Some(AppPresentationV1 {
            result_shape_revision: 1,
            result_contracts: vec![AppResultContractV1 {
                contract_id: "reference-app.result.v1".to_owned(),
                schema_id: "cowd.reference.result.v1".to_owned(),
                schema_version: 1,
                schema_digest: digest.clone(),
                max_bytes: 256 * 1024,
            }],
            tui_views: vec![AppTuiViewDescriptorV1 {
                view_id: "main".to_owned(),
                open_operation_id: "reference-app.tui.main.open".to_owned(),
                action_operation_id: "reference-app.tui.main.action".to_owned(),
                stream_operation_id: "reference-app.tui.main.stream".to_owned(),
            }],
            core_navigation_kinds: vec!["reality.object".to_owned()],
        }),
    };
    manifest
        .bind_canonical_signed_digest()
        .expect("bind reference manifest");
    manifest
}

fn reference_core_catalog(manifest: &AppManifestV1) -> CoreOperationCatalogV1 {
    let mut catalog = CoreOperationCatalogV1 {
        schema_version: 1,
        protocol_revision: PROTOCOL_REVISION_V1,
        app_id: manifest.app_id.clone(),
        generation: GenerationId(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        ),
        catalog_digest: Sha256Digest(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        ),
        operations: vec![reference_operation()],
    };
    catalog
        .bind_canonical_catalog_digest()
        .expect("bind reference core catalog");
    catalog
}

fn openapi() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {"title": "Cowd APP Protocol", "version": "1"},
        "paths": {
            "/_cowd/v1/handshake": {"post": {"operationId": "appHandshake"}},
            "/_cowd/v1/health": {"get": {"operationId": "appHealth"}},
            "/_cowd/v1/operations": {"get": {"operationId": "appOperations"}},
            "/_cowd/v1/operations/{operation_id}/invoke": {"post": {
                "operationId": "appInvoke",
                "x-cowd-authorization-context": "gateway-verified",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/_cowd/v1/operations/{operation_id}/stream": {"post": {
                "operationId": "appStream",
                "x-cowd-authorization-context": "gateway-verified",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/_cowd/v1/tui/views/{view_id}/open": {"post": {
                "operationId": "appTuiViewOpen",
                "x-cowd-operation-selector": "signed-presentation.tui_views.open_operation_id",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/_cowd/v1/tui/views/{view_id}/actions/{action_id}": {"post": {
                "operationId": "appTuiViewAction",
                "x-cowd-operation-selector": "signed-presentation.tui_views.action_operation_id",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/_cowd/v1/tui/views/{view_id}/stream": {"post": {
                "operationId": "appTuiViewStream",
                "x-cowd-operation-selector": "signed-presentation.tui_views.stream_operation_id",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/_cowd/v1/shutdown": {"post": {"operationId": "appShutdown"}},
            "/_cowd/core/v1/operations": {"get": {
                "operationId": "coreOperations",
                "responses": {"200": {"description": "APP-scoped Core operation catalog", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CoreOperationCatalogV1"}}}}}
            }},
            "/_cowd/core/v1/operations/{operation_id}/invoke": {"post": {
                "operationId": "coreInvoke",
                "x-cowd-authorization-context": "gateway-verified",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CoreBridgeInvocationV1"}}}}
            }},
            "/_cowd/core/v1/operations/{operation_id}/stream": {"post": {
                "operationId": "coreStream",
                "x-cowd-authorization-context": "gateway-verified",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/CoreBridgeInvocationV1"}}}}
            }},
            "/api/apps": {"get": {"operationId": "appCatalog"}},
            "/api/apps/{app_id}": {"get": {"operationId": "appCatalogEntry"}}
        },
        "components": {"schemas": {
            "AppInvocationEnvelopeV1": {"$ref": "./schemas/invocation.schema.json"},
            "CoreBridgeInvocationV1": {"$ref": "./schemas/core-bridge-invocation.schema.json"},
            "CoreOperationCatalogV1": {"$ref": "./schemas/core-operation-catalog.schema.json"},
            "AppTuiViewOpenRequestV1": {"$ref": "./schemas/tui-open-request.schema.json"},
            "AppTuiViewOpenResponseV1": {"$ref": "./schemas/tui-open-response.schema.json"},
            "AppActionV1": {"$ref": "./schemas/tui-action.schema.json"},
            "AppTuiViewActionResponseV1": {"$ref": "./schemas/tui-action-response.schema.json"},
            "AppTuiViewStreamRequestV1": {"$ref": "./schemas/tui-stream-request.schema.json"},
            "AppViewPatchV1": {"$ref": "./schemas/tui-patch.schema.json"}
        }}
    })
}
