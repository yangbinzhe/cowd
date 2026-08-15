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
    BTreeMap::from([
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
    ])
}

fn openapi() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {"title": "Cowd APP Protocol", "version": "1"},
        "paths": {
            "/_cowd/v1/handshake": {"post": {"operationId": "appHandshake"}},
            "/_cowd/v1/health": {"get": {"operationId": "appHealth"}},
            "/_cowd/v1/operations": {"get": {"operationId": "appOperations"}},
            "/_cowd/v1/operations/{operation_id}/invoke": {"post": {"operationId": "appInvoke"}},
            "/_cowd/v1/operations/{operation_id}/stream": {"post": {"operationId": "appStream"}},
            "/_cowd/v1/shutdown": {"post": {"operationId": "appShutdown"}},
            "/_cowd/core/v1/operations": {"get": {"operationId": "coreOperations"}},
            "/_cowd/core/v1/operations/{operation_id}/invoke": {"post": {"operationId": "coreInvoke"}},
            "/_cowd/core/v1/operations/{operation_id}/stream": {"post": {"operationId": "coreStream"}},
            "/api/apps": {"get": {"operationId": "appCatalog"}},
            "/api/apps/{app_id}": {"get": {"operationId": "appCatalogEntry"}}
        }
    })
}
