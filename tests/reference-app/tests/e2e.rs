#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fmt::Write as _;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cowd_app_protocol::{
    derive_channel_token_v1, format_bootstrap_authorization_v1, format_channel_authorization_v1,
    AppHandshakeRequestV1, AppHandshakeV1, AppInvocationEnvelopeV1, AppProviderResponseV1,
    AppStreamFrameV1, BootstrapSecretV1, ChannelPurposeV1, DurableReceiptV1, GenerationId,
    ProtocolValidate, APP_HANDSHAKE_PATH_V1, APP_HEALTH_PATH_V1, APP_OPERATIONS_PATH_V1,
    APP_SHUTDOWN_PATH_V1, HEADER_APP_GENERATION_V1, HEADER_APP_ID_V1, HEADER_AUTHORIZATION_V1,
    HEADER_DEADLINE_UNIX_MS_V1, HEADER_PROTOCOL_VERSION_V1, HEADER_REQUEST_ID_V1,
    PROTOCOL_REVISION_V1,
};
use cowd_reference_app::{
    discover_bundles, install_bundle, operations, package, validate_bundle, APP_ID,
    PROTOCOL_ARTIFACT_SHA256, PROTOCOL_SOURCE_COMMIT, PROTOCOL_WIRE_DIGEST,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::UnixStream;

const GENERATION: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SECRET_BYTES: [u8; 32] = [0x42; 32];

#[test]
fn package_is_deterministic_closed_signed_and_tamper_evident() {
    let temporary = TempDir::new().unwrap();
    let worker = temporary.path().join("worker");
    fs::write(&worker, b"deterministic-reference-worker").unwrap();
    fs::set_permissions(&worker, fs::Permissions::from_mode(0o755)).unwrap();
    let first = temporary.path().join("first");
    let second = temporary.path().join("second");
    let first_manifest = package(&worker, &first).unwrap();
    let second_manifest = package(&worker, &second).unwrap();
    assert_eq!(first_manifest, second_manifest);
    assert_eq!(
        fs::read(first.join("app.json")).unwrap(),
        fs::read(second.join("app.json")).unwrap()
    );
    assert_eq!(first_manifest.integrity.files.len(), 5);
    validate_bundle(&first).unwrap();
    let apps_root = temporary.path().join("apps");
    let installed = install_bundle(&first, &apps_root).unwrap();
    assert_eq!(installed, apps_root.join(APP_ID));
    let discovered = discover_bundles(&apps_root).unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].1.app_id.0, APP_ID);
    assert!(install_bundle(&first, &apps_root).is_err());

    let app_js = first.join("webui/app.js");
    fs::set_permissions(&app_js, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&app_js, b"tampered").unwrap();
    fs::set_permissions(&app_js, fs::Permissions::from_mode(0o444)).unwrap();
    assert!(validate_bundle(&first).is_err());
    fs::set_permissions(&app_js, fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(&app_js, include_bytes!("../webui/app.js")).unwrap();
    fs::set_permissions(&app_js, fs::Permissions::from_mode(0o444)).unwrap();
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(first.join("app.json")).unwrap()).unwrap();
    manifest["signature"]["signature"] = Value::String(URL_SAFE_NO_PAD.encode([0_u8; 64]));
    fs::set_permissions(first.join("app.json"), fs::Permissions::from_mode(0o644)).unwrap();
    fs::write(
        first.join("app.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    fs::set_permissions(first.join("app.json"), fs::Permissions::from_mode(0o444)).unwrap();
    assert!(validate_bundle(&first).is_err());
    fs::set_permissions(&first, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(first.join("extra"), b"not admitted").unwrap();
    fs::set_permissions(first.join("extra"), fs::Permissions::from_mode(0o444)).unwrap();
    fs::set_permissions(&first, fs::Permissions::from_mode(0o555)).unwrap();
    assert!(validate_bundle(&first).is_err());
    for bundle in [&first, &second, &installed] {
        fs::set_permissions(bundle, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(bundle.join("bin"), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(bundle.join("webui"), fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[test]
fn vendored_protocol_provenance_and_wire_contract_are_frozen() {
    assert_eq!(
        PROTOCOL_ARTIFACT_SHA256,
        "0151286b0871a854f4d76eed0c45c15c7c5ddcc81dfe9d1f3f3bf346a0891b28"
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("vendor/cowd-app-protocol-1.0.0");
    let vcs: Value =
        serde_json::from_slice(&fs::read(root.join(".cargo_vcs_info.json")).unwrap()).unwrap();
    assert_eq!(vcs["git"]["sha1"], PROTOCOL_SOURCE_COMMIT);
    let contract: Value = serde_json::from_slice(
        &fs::read(root.join("contracts/v1/contract-manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(contract["protocol_digest"], PROTOCOL_WIRE_DIGEST);
    let mut inventory = Vec::new();
    visit(&root, &root, &mut inventory);
    inventory.sort();
    let joined = inventory
        .into_iter()
        .fold(String::new(), |mut output, (path, digest)| {
            writeln!(output, "{digest}  {path}").unwrap();
            output
        });
    assert_eq!(
        format!("{:x}", Sha256::digest(joined)),
        "f2eedb345b51a6ab54a7328862831a058db7a3d795876ff36d882ea55c5d4d3e"
    );
}

fn visit(root: &Path, path: &Path, output: &mut Vec<(String, String)>) {
    for entry in fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            visit(root, &entry.path(), output);
        } else {
            let relative = entry
                .path()
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            output.push((
                relative,
                format!("{:x}", Sha256::digest(fs::read(entry.path()).unwrap())),
            ));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn uds_h2_worker_runs_complete_reference_lifecycle() {
    let temporary = TempDir::new().unwrap();
    let socket = temporary.path().join("worker.sock");
    let credential = temporary.path().join("bootstrap.secret");
    fs::write(&credential, URL_SAFE_NO_PAD.encode(SECRET_BYTES)).unwrap();
    fs::set_permissions(&credential, fs::Permissions::from_mode(0o600)).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_reference-app-worker"))
        .env("COWD_APP_ID", APP_ID)
        .env("COWD_APP_GENERATION", GENERATION)
        .env("COWD_APP_SOCKET", &socket)
        .env("COWD_APP_CREDENTIAL_FILE", &credential)
        .env("COWD_APP_DATA_DIR", temporary.path().join("data"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    for _ in 0..200 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(socket.exists());
    assert_eq!(
        fs::symlink_metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(!credential.exists());
    let worker_pid = child.id().unwrap();
    let secret = BootstrapSecretV1::from_bytes(&SECRET_BYTES).unwrap();
    let mut client = connect(&socket).await;
    let request = AppHandshakeRequestV1 {
        schema_version: 1,
        protocol_revision: PROTOCOL_REVISION_V1,
        app_id: cowd_app_protocol::AppId(APP_ID.to_owned()),
        generation: GenerationId(GENERATION.to_owned()),
        gateway_pid: std::process::id(),
        worker_pid,
    };
    let handshake_response = client
        .send_request(
            Request::builder()
                .method(Method::POST)
                .uri(format!("http://localhost{APP_HANDSHAKE_PATH_V1}"))
                .header(
                    HEADER_AUTHORIZATION_V1,
                    format_bootstrap_authorization_v1(&secret),
                )
                .body(Full::new(Bytes::from(
                    serde_json::to_vec(&request).unwrap(),
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(handshake_response.status(), StatusCode::OK);
    let handshake: AppHandshakeV1 = decode(handshake_response).await;
    handshake.validate().unwrap();
    assert_eq!(handshake.operations, operations());
    let token = derive_channel_token_v1(
        &secret,
        ChannelPurposeV1::WorkerChannel,
        &handshake.app_id,
        &handshake.generation,
        handshake.worker_pid,
        &handshake.worker_nonce,
    )
    .unwrap();
    let authorization = format_channel_authorization_v1(&token);
    assert_eq!(
        client
            .send_request(channel(
                Method::GET,
                APP_HEALTH_PATH_V1,
                &authorization,
                None
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let descriptors: Vec<cowd_app_protocol::OperationDescriptorV1> = decode(
        client
            .send_request(channel(
                Method::GET,
                APP_OPERATIONS_PATH_V1,
                &authorization,
                None,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(descriptors, operations());

    let echo = invocation(
        "reference.echo",
        "request-echo",
        None,
        json!({"message":"hello"}),
    );
    let echo_response: AppProviderResponseV1 = decode(
        client
            .send_request(channel(
                Method::POST,
                "/_cowd/v1/operations/reference.echo/invoke",
                &authorization,
                Some(serde_json::to_vec(&echo).unwrap()),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(echo_response.payload["echo"], echo.payload);
    let command = invocation(
        "reference.counter.increment",
        "request-command",
        Some("stable-key"),
        json!({}),
    );
    let first: DurableReceiptV1 = decode(
        client
            .send_request(channel(
                Method::POST,
                "/_cowd/v1/operations/reference.counter.increment/invoke",
                &authorization,
                Some(serde_json::to_vec(&command).unwrap()),
            ))
            .await
            .unwrap(),
    )
    .await;
    let replay: DurableReceiptV1 = decode(
        client
            .send_request(channel(
                Method::POST,
                "/_cowd/v1/operations/reference.counter.increment/invoke",
                &authorization,
                Some(serde_json::to_vec(&command).unwrap()),
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(first.payload["counter"], 1);
    assert!(!first.replayed);
    assert!(replay.replayed);
    assert_eq!(first.receipt_id, replay.receipt_id);
    let conflict = invocation(
        "reference.counter.increment",
        "request-command-conflict",
        Some("stable-key"),
        json!({"different":true}),
    );
    let conflict_response = client
        .send_request(channel(
            Method::POST,
            "/_cowd/v1/operations/reference.counter.increment/invoke",
            &authorization,
            Some(serde_json::to_vec(&conflict).unwrap()),
        ))
        .await
        .unwrap();
    assert_eq!(conflict_response.status(), StatusCode::CONFLICT);
    let fetched: DurableReceiptV1 = decode(
        client
            .send_request(channel(
                Method::GET,
                &format!("/_cowd/v1/receipts/{}", first.receipt_id),
                &authorization,
                None,
            ))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(fetched.receipt_id, first.receipt_id);

    for operation in ["reference.events", "reference.export"] {
        let envelope = invocation(operation, &format!("request-{operation}"), None, json!({}));
        let response = client
            .send_request(channel(
                Method::POST,
                &format!("/_cowd/v1/operations/{operation}/stream"),
                &authorization,
                Some(serde_json::to_vec(&envelope).unwrap()),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.collect().await.unwrap().to_bytes();
        let frames = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<AppStreamFrameV1>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(frames.len() >= 3);
        assert_eq!(frames[0].sequence(), 0);
        if operation == "reference.export" {
            assert!(matches!(frames[1], AppStreamFrameV1::Data { .. }));
            if let AppStreamFrameV1::Data { payload, .. } = &frames[1] {
                let artifact: cowd_app_protocol::AppArtifactRefV1 =
                    serde_json::from_value(payload.clone()).unwrap();
                let content = URL_SAFE_NO_PAD
                    .decode(artifact.metadata["data_base64url"].as_bytes())
                    .unwrap();
                assert_eq!(
                    artifact.content_digest.0,
                    format!("sha256:{:x}", Sha256::digest(content))
                );
                assert_eq!(artifact.row_count, 1);
            }
        }
        let subscription_id = frames[0].subscription_id().to_owned();
        assert_eq!(
            client
                .send_request(channel(
                    Method::DELETE,
                    &format!("/_cowd/v1/subscriptions/{subscription_id}"),
                    &authorization,
                    None
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
    }
    assert_eq!(
        client
            .send_request(channel(
                Method::POST,
                APP_SHUTDOWN_PATH_V1,
                &authorization,
                None
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert!(child.wait().await.unwrap().success());
    assert!(!socket.exists());
}

fn invocation(
    operation_id: &str,
    request_id: &str,
    idempotency: Option<&str>,
    payload: Value,
) -> AppInvocationEnvelopeV1 {
    let descriptor = operations()
        .into_iter()
        .find(|value| value.operation_id == operation_id)
        .unwrap();
    let mut value: Value = serde_json::from_str(include_str!(
        "../vendor/cowd-app-protocol-1.0.0/contracts/v1/golden/query-invocation.json"
    ))
    .unwrap();
    value["operation_id"] = Value::String(operation_id.to_owned());
    value["request_id"] = Value::String(request_id.to_owned());
    value["correlation_id"] = Value::String(format!("correlation-{request_id}"));
    value["input_schema_digest"] = Value::String(descriptor.input_schema_digest.0);
    value["principal"]["granted_capabilities"] = json!(descriptor.required_capabilities);
    value["payload"] = payload;
    if let Some(key) = idempotency {
        value["idempotency_key"] = Value::String(key.to_owned());
    }
    serde_json::from_value(value).unwrap()
}

async fn connect(socket: &Path) -> hyper::client::conn::http2::SendRequest<Full<Bytes>> {
    let stream = UnixStream::connect(socket).await.unwrap();
    let (sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _result = connection.await;
    });
    sender
}

fn channel(
    method: Method,
    path: &str,
    authorization: &str,
    body: Option<Vec<u8>>,
) -> Request<Full<Bytes>> {
    let deadline = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
        + 30_000;
    Request::builder()
        .method(method)
        .uri(format!("http://localhost{path}"))
        .header(HEADER_AUTHORIZATION_V1, authorization)
        .header(HEADER_PROTOCOL_VERSION_V1, "1")
        .header(HEADER_APP_ID_V1, APP_ID)
        .header(HEADER_APP_GENERATION_V1, GENERATION)
        .header(
            HEADER_REQUEST_ID_V1,
            format!("request-{}", uuid::Uuid::new_v4()),
        )
        .header(HEADER_DEADLINE_UNIX_MS_V1, deadline)
        .body(Full::new(Bytes::from(body.unwrap_or_default())))
        .unwrap()
}

async fn decode<T: DeserializeOwned>(response: hyper::Response<hyper::body::Incoming>) -> T {
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&response.collect().await.unwrap().to_bytes()).unwrap()
}
