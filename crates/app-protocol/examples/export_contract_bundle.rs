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
    add_schema::<AppHealthV1>(&mut files, "health.schema.json")?;
    add_schema::<AppCatalogV1>(&mut files, "catalog.schema.json")?;
    add_schema::<OperationDescriptorV1>(&mut files, "operation.schema.json")?;
    add_schema::<AppInvocationEnvelopeV1>(&mut files, "invocation.schema.json")?;
    add_schema::<AppProviderResponseV1>(&mut files, "provider-response.schema.json")?;
    add_schema::<DurableReceiptV1>(&mut files, "receipt.schema.json")?;
    add_schema::<AppStreamFrameV1>(&mut files, "stream-frame.schema.json")?;
    add_schema::<AppErrorResponseV1>(&mut files, "error.schema.json")?;
    add_schema::<AppViewDocumentV1>(&mut files, "tui-view.schema.json")?;
    add_schema::<AppViewPatchV1>(&mut files, "tui-patch.schema.json")?;
    add_schema::<AppActionV1>(&mut files, "tui-action.schema.json")?;
    add_schema::<IframeBridgeMessageV1>(&mut files, "iframe-bridge.schema.json")?;
    add_schema::<IframeApiFrameV1>(&mut files, "iframe-api.schema.json")?;
    add_schema::<ApplicationExecutionOutcomeV1>(
        &mut files,
        "application-execution-outcome.schema.json",
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
        "granted_capabilities": ["app.reference.read", "app.reference.write"],
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
    ]);

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
        json!(["app.reference.write", "app.reference.write"]);
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
    missing_capability["principal"]["granted_capabilities"] = json!(["app.reference.read"]);
    fixtures.insert("negative/missing-capability.json", missing_capability);
    fixtures
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
            "/_cowd/v1/shutdown": {"post": {"operationId": "appShutdown"}},
            "/_cowd/core/v1/operations": {"get": {"operationId": "coreOperations"}},
            "/_cowd/core/v1/operations/{operation_id}/invoke": {"post": {
                "operationId": "coreInvoke",
                "x-cowd-authorization-context": "gateway-verified",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/_cowd/core/v1/operations/{operation_id}/stream": {"post": {
                "operationId": "coreStream",
                "x-cowd-authorization-context": "gateway-verified",
                "requestBody": {"required": true, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/AppInvocationEnvelopeV1"}}}}
            }},
            "/api/apps": {"get": {"operationId": "appCatalog"}},
            "/api/apps/{app_id}": {"get": {"operationId": "appCatalogEntry"}}
        },
        "components": {"schemas": {
            "AppInvocationEnvelopeV1": {"$ref": "./schemas/invocation.schema.json"}
        }}
    })
}
