use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cowd_app_protocol::{
    derive_channel_token_v1, verify_bootstrap_authorization_v1,
    verify_channel_token_authorization_v1, AppArtifactRefV1, AppErrorCodeV1, AppErrorDetailV1,
    AppErrorResponseV1, AppHandshakeRequestV1, AppHandshakeV1, AppHealthStatusV1, AppHealthV1,
    AppId, AppInvocationEnvelopeV1, AppProviderResponseV1, AppStreamAckV1, AppStreamFrameV1,
    BootstrapSecretV1, ChannelPurposeV1, ChannelTokenV1, DurableReceiptV1, GenerationId,
    HealthCheckV1, ProtocolValidate, ReceiptStatusV1, Sha256Digest, StreamEndReasonV1,
    APP_HANDSHAKE_PATH_V1, APP_HEALTH_PATH_V1, APP_OPERATIONS_PATH_V1, APP_SHUTDOWN_PATH_V1,
    ENV_APP_CREDENTIAL_FILE_V1, ENV_APP_DATA_DIR_V1, ENV_APP_GENERATION_V1, ENV_APP_ID_V1,
    ENV_APP_SOCKET_V1, HEADER_APP_GENERATION_V1, HEADER_APP_ID_V1, HEADER_AUTHORIZATION_V1,
    HEADER_DEADLINE_UNIX_MS_V1, HEADER_PROTOCOL_VERSION_V1, HEADER_REQUEST_ID_V1,
    PROTOCOL_REVISION_V1, STREAM_CONTENT_TYPE_V1, UNARY_CONTENT_TYPE_V1,
};
use cowd_reference_app::{manifest_digests, operations, APP_ID, ARTIFACT_VERSION};
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Bytes, Incoming};
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use zeroize::Zeroizing;

const MAX_BODY_BYTES: u64 = 64 * 1024;
const MAX_RECEIPTS: usize = 10_000;
const MAX_SUBSCRIPTIONS: usize = 64;

type ResponseBody = Full<Bytes>;

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error("configuration rejected: {0}")]
    Configuration(String),
    #[error("credential rejected: {0}")]
    Credential(String),
    #[error("transport failed: {0}")]
    Transport(String),
    #[error("storage failed: {0}")]
    Storage(String),
}

#[derive(Clone)]
struct Environment {
    app_id: AppId,
    generation: GenerationId,
    socket: PathBuf,
    credential: PathBuf,
    data_dir: PathBuf,
}

impl Environment {
    fn process() -> Result<Self, WorkerError> {
        let app_id = required_env(ENV_APP_ID_V1, None)?;
        if app_id != APP_ID {
            return Err(WorkerError::Configuration(
                "unexpected APP identity".to_owned(),
            ));
        }
        Ok(Self {
            app_id: AppId(app_id),
            generation: GenerationId(required_env(
                ENV_APP_GENERATION_V1,
                Some("COWD_WORKER_GENERATION"),
            )?),
            socket: PathBuf::from(required_env(ENV_APP_SOCKET_V1, Some("COWD_WORKER_SOCKET"))?),
            credential: PathBuf::from(required_env(
                ENV_APP_CREDENTIAL_FILE_V1,
                Some("COWD_WORKER_CREDENTIAL"),
            )?),
            data_dir: PathBuf::from(required_env(ENV_APP_DATA_DIR_V1, None)?),
        })
    }
}

fn required_env(primary: &str, fallback: Option<&str>) -> Result<String, WorkerError> {
    std::env::var(primary)
        .or_else(|_| fallback.map_or_else(|| Err(std::env::VarError::NotPresent), std::env::var))
        .map_err(|_| WorkerError::Configuration(format!("missing {primary}")))
}

struct Session {
    worker: ChannelTokenV1,
}

struct Bootstrap {
    app_id: AppId,
    generation: GenerationId,
    worker_pid: u32,
    gateway_pid: u32,
    secret: Mutex<Option<BootstrapSecretV1>>,
    session: Mutex<Option<Session>>,
}

impl Bootstrap {
    fn new(environment: &Environment) -> Result<Self, WorkerError> {
        Ok(Self {
            app_id: environment.app_id.clone(),
            generation: environment.generation.clone(),
            worker_pid: std::process::id(),
            gateway_pid: parent_pid()?,
            secret: Mutex::new(Some(read_credential(&environment.credential)?)),
            session: Mutex::new(None),
        })
    }

    fn handshake(
        &self,
        authorization: &str,
        request: &AppHandshakeRequestV1,
    ) -> Result<AppHandshakeV1, ()> {
        request.validate().map_err(|_| ())?;
        if request.app_id != self.app_id
            || request.generation != self.generation
            || request.worker_pid != self.worker_pid
            || request.gateway_pid != self.gateway_pid
        {
            return Err(());
        }
        let mut secret_slot = self
            .secret
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let secret = secret_slot.as_ref().ok_or(())?;
        verify_bootstrap_authorization_v1(secret, authorization).map_err(|_| ())?;
        let worker_nonce = format!(
            "{:x}",
            Sha256::digest(format!(
                "{}:{}:{}",
                self.app_id.0, self.generation.0, self.worker_pid
            ))
        );
        let worker = derive_channel_token_v1(
            secret,
            ChannelPurposeV1::WorkerChannel,
            &self.app_id,
            &self.generation,
            self.worker_pid,
            &worker_nonce,
        )
        .map_err(|_| ())?;
        let mut session = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if session.is_some() {
            return Err(());
        }
        *session = Some(Session { worker });
        let _consumed = secret_slot.take();
        let (capability_digest, authorization_profile_digest) =
            manifest_digests().map_err(|_| ())?;
        Ok(AppHandshakeV1 {
            schema_version: 1,
            protocol_revision: PROTOCOL_REVISION_V1,
            app_id: self.app_id.clone(),
            generation: self.generation.clone(),
            artifact_version: ARTIFACT_VERSION.to_owned(),
            worker_pid: self.worker_pid,
            worker_nonce,
            operations: operations(),
            capability_digest,
            authorization_profile_digest,
        })
    }

    fn authorize(&self, request: &Request<Incoming>) -> Result<(), ()> {
        require_header(request, HEADER_PROTOCOL_VERSION_V1, "1")?;
        require_header(request, HEADER_APP_ID_V1, &self.app_id.0)?;
        require_header(request, HEADER_APP_GENERATION_V1, &self.generation.0)?;
        let request_id = header(request, HEADER_REQUEST_ID_V1)?;
        if request_id.is_empty() || request_id.len() > 128 {
            return Err(());
        }
        let deadline = header(request, HEADER_DEADLINE_UNIX_MS_V1)?
            .parse::<u64>()
            .map_err(|_| ())?;
        let now = now_ms().map_err(|_| ())?;
        if deadline <= now || deadline > now.saturating_add(300_000) {
            return Err(());
        }
        let authorization = header(request, HEADER_AUTHORIZATION_V1)?;
        let session = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        verify_channel_token_authorization_v1(&session.as_ref().ok_or(())?.worker, authorization)
            .map_err(|_| ())
    }
}

fn header<'a>(request: &'a Request<Incoming>, name: &str) -> Result<&'a str, ()> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or(())
}

fn require_header(request: &Request<Incoming>, name: &str, expected: &str) -> Result<(), ()> {
    if header(request, name)? == expected {
        Ok(())
    } else {
        Err(())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableState {
    counter: u64,
    receipts: BTreeMap<String, DurableReceiptV1>,
    idempotency_inputs: BTreeMap<String, Sha256Digest>,
}

enum IncrementError {
    Conflict,
    Storage(WorkerError),
}

struct State {
    bootstrap: Bootstrap,
    durable: Mutex<DurableState>,
    durable_path: PathBuf,
    subscriptions: Mutex<BTreeSet<String>>,
    shutdown: watch::Sender<bool>,
}

impl State {
    fn load(
        environment: &Environment,
        bootstrap: Bootstrap,
        shutdown: watch::Sender<bool>,
    ) -> Result<Self, WorkerError> {
        fs::create_dir_all(&environment.data_dir)
            .map_err(|error| WorkerError::Storage(error.to_string()))?;
        let durable_path = environment.data_dir.join("reference-state.json");
        let durable = if durable_path.exists() {
            serde_json::from_slice(
                &fs::read(&durable_path)
                    .map_err(|error| WorkerError::Storage(error.to_string()))?,
            )
            .map_err(|error| WorkerError::Storage(error.to_string()))?
        } else {
            DurableState::default()
        };
        if durable.receipts.len() > MAX_RECEIPTS {
            return Err(WorkerError::Storage("receipt capacity exceeded".to_owned()));
        }
        Ok(Self {
            bootstrap,
            durable: Mutex::new(durable),
            durable_path,
            subscriptions: Mutex::new(BTreeSet::new()),
            shutdown,
        })
    }

    fn increment(
        &self,
        envelope: &AppInvocationEnvelopeV1,
    ) -> Result<DurableReceiptV1, IncrementError> {
        let key = envelope.idempotency_key.as_ref().ok_or_else(|| {
            IncrementError::Storage(WorkerError::Storage("idempotency key missing".to_owned()))
        })?;
        let input_digest = Sha256Digest(format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&envelope.payload).map_err(|error| {
                IncrementError::Storage(WorkerError::Storage(error.to_string()))
            })?)
        ));
        let mut state = self
            .durable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(receipt) = state.receipts.get(key) {
            if state.idempotency_inputs.get(key) != Some(&input_digest) {
                return Err(IncrementError::Conflict);
            }
            let mut replay = receipt.clone();
            replay.replayed = true;
            return Ok(replay);
        }
        if state.receipts.len() >= MAX_RECEIPTS {
            return Err(IncrementError::Storage(WorkerError::Storage(
                "receipt capacity exceeded".to_owned(),
            )));
        }
        state.counter = state.counter.checked_add(1).ok_or_else(|| {
            IncrementError::Storage(WorkerError::Storage("counter overflow".to_owned()))
        })?;
        let payload = json!({"counter": state.counter});
        let digest = Sha256Digest(format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&payload).map_err(|error| {
                IncrementError::Storage(WorkerError::Storage(error.to_string()))
            })?)
        ));
        let receipt = DurableReceiptV1 {
            schema_version: 1,
            request_id: envelope.request_id.clone(),
            receipt_id: format!("receipt-{:x}", Sha256::digest(key.as_bytes())),
            idempotency_key: key.clone(),
            status: ReceiptStatusV1::Completed,
            result_revision: Some(state.counter.to_string()),
            replayed: false,
            payload_digest: digest,
            payload,
        };
        state.receipts.insert(key.clone(), receipt.clone());
        state.idempotency_inputs.insert(key.clone(), input_digest);
        persist(&self.durable_path, &state).map_err(IncrementError::Storage)?;
        Ok(receipt)
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("reference APP worker failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), WorkerError> {
    let environment = Environment::process()?;
    if environment.socket.exists() {
        return Err(WorkerError::Transport("socket already exists".to_owned()));
    }
    if let Some(parent) = environment.socket.parent() {
        fs::create_dir_all(parent).map_err(|error| WorkerError::Transport(error.to_string()))?;
    }
    let bootstrap = Bootstrap::new(&environment)?;
    let listener = UnixListener::bind(&environment.socket)
        .map_err(|error| WorkerError::Transport(error.to_string()))?;
    fs::set_permissions(&environment.socket, fs::Permissions::from_mode(0o600))
        .map_err(|error| WorkerError::Transport(error.to_string()))?;
    let cleanup = SocketCleanup(environment.socket.clone());
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let state = Arc::new(State::load(&environment, bootstrap, shutdown)?);
    let signal = state.shutdown.clone();
    tokio::spawn(async move {
        if terminate_signal().await.is_ok() {
            let _changed = signal.send(true);
        }
    });
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => { if changed.is_err() || *shutdown_rx.borrow() { break; } }
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| WorkerError::Transport(error.to_string()))?;
                let shared = Arc::clone(&state);
                connections.spawn(async move {
                    let service = service_fn(move |request| handle(request, Arc::clone(&shared)));
                    let _served = http2::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(stream), service).await;
                });
            }
        }
    }
    drop(listener);
    tokio::time::sleep(Duration::from_millis(20)).await;
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    drop(cleanup);
    Ok(())
}

async fn handle(
    request: Request<Incoming>,
    state: Arc<State>,
) -> Result<Response<ResponseBody>, Infallible> {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let response = if method == Method::POST && path == APP_HANDSHAKE_PATH_V1 {
        handshake(request, &state).await
    } else if state.bootstrap.authorize(&request).is_err() {
        error(
            AppErrorCodeV1::Unauthenticated,
            "channel authentication failed",
        )
    } else if method == Method::GET && path == APP_HEALTH_PATH_V1 {
        health(&state)
    } else if method == Method::GET && path == APP_OPERATIONS_PATH_V1 {
        json_response(StatusCode::OK, &operations())
    } else if method == Method::POST && path == APP_SHUTDOWN_PATH_V1 {
        let _changed = state.shutdown.send(true);
        empty(StatusCode::NO_CONTENT)
    } else if method == Method::POST
        && path.starts_with("/_cowd/v1/operations/")
        && path.ends_with("/invoke")
    {
        invoke(request, &state, &path).await
    } else if method == Method::POST
        && path.starts_with("/_cowd/v1/operations/")
        && path.ends_with("/stream")
    {
        stream(request, &state, &path).await
    } else if method == Method::GET && path.starts_with("/_cowd/v1/receipts/") {
        receipt(&state, &path)
    } else if method == Method::POST
        && path.contains("/_cowd/v1/subscriptions/")
        && path.ends_with("/ack")
    {
        ack(request, &state, &path).await
    } else if method == Method::DELETE && path.starts_with("/_cowd/v1/subscriptions/") {
        cancel(&state, &path)
    } else {
        error(AppErrorCodeV1::AppNotFound, "route not found")
    };
    Ok(response)
}

async fn handshake(request: Request<Incoming>, state: &State) -> Response<ResponseBody> {
    let authorization = request
        .headers()
        .get(HEADER_AUTHORIZATION_V1)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let Some(authorization) = authorization else {
        return error(
            AppErrorCodeV1::Unauthenticated,
            "bootstrap authorization missing",
        );
    };
    let body = match bounded_body(request).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let parsed: AppHandshakeRequestV1 = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return error(AppErrorCodeV1::InvalidRequest, "invalid handshake"),
    };
    match state.bootstrap.handshake(&authorization, &parsed) {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(()) => error(AppErrorCodeV1::Unauthenticated, "handshake rejected"),
    }
}

fn health(state: &State) -> Response<ResponseBody> {
    json_response(
        StatusCode::OK,
        &AppHealthV1 {
            schema_version: 1,
            app_id: state.bootstrap.app_id.clone(),
            generation: state.bootstrap.generation.clone(),
            status: AppHealthStatusV1::Ready,
            checks: BTreeMap::from([(
                "durable_state".to_owned(),
                HealthCheckV1 {
                    healthy: true,
                    message: "ready".to_owned(),
                },
            )]),
        },
    )
}

async fn invoke(request: Request<Incoming>, state: &State, path: &str) -> Response<ResponseBody> {
    let operation_id = path
        .trim_start_matches("/_cowd/v1/operations/")
        .trim_end_matches("/invoke");
    let Some(descriptor) = operations()
        .into_iter()
        .find(|candidate| candidate.operation_id == operation_id)
    else {
        return error(AppErrorCodeV1::AppNotFound, "operation not found");
    };
    if descriptor.streaming {
        return error(
            AppErrorCodeV1::InvalidRequest,
            "streaming operation requires stream route",
        );
    }
    let body = match bounded_body(request).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let envelope: AppInvocationEnvelopeV1 = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return error(AppErrorCodeV1::InvalidRequest, "invalid invocation"),
    };
    if envelope
        .validate_at(now_ms().unwrap_or(u64::MAX), &descriptor)
        .is_err()
    {
        return error(
            AppErrorCodeV1::OperationNotGranted,
            "invocation validation failed",
        );
    }
    match operation_id {
        "reference.echo" => json_response(
            StatusCode::OK,
            &AppProviderResponseV1 {
                schema_version: 1,
                request_id: envelope.request_id,
                output_schema_digest: descriptor.output_schema_digest,
                revision: None,
                payload: json!({"echo":envelope.payload}),
            },
        ),
        "reference.counter.increment" => match state.increment(&envelope) {
            Ok(receipt) => json_response(StatusCode::OK, &receipt),
            Err(IncrementError::Conflict) => error(
                AppErrorCodeV1::IdempotencyConflict,
                "idempotency key is bound to different input",
            ),
            Err(IncrementError::Storage(_error)) => {
                error(AppErrorCodeV1::InternalError, "counter persistence failed")
            }
        },
        _ => error(AppErrorCodeV1::AppNotFound, "operation not found"),
    }
}

async fn stream(request: Request<Incoming>, state: &State, path: &str) -> Response<ResponseBody> {
    let operation_id = path
        .trim_start_matches("/_cowd/v1/operations/")
        .trim_end_matches("/stream");
    let Some(descriptor) = operations()
        .into_iter()
        .find(|candidate| candidate.operation_id == operation_id)
    else {
        return error(AppErrorCodeV1::AppNotFound, "operation not found");
    };
    if !descriptor.streaming {
        return error(
            AppErrorCodeV1::InvalidRequest,
            "unary operation requires invoke route",
        );
    }
    let body = match bounded_body(request).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let envelope: AppInvocationEnvelopeV1 = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return error(AppErrorCodeV1::InvalidRequest, "invalid invocation"),
    };
    if envelope
        .validate_at(now_ms().unwrap_or(u64::MAX), &descriptor)
        .is_err()
    {
        return error(
            AppErrorCodeV1::OperationNotGranted,
            "invocation validation failed",
        );
    }
    let subscription_id = format!(
        "subscription-{:x}",
        Sha256::digest(envelope.request_id.as_bytes())
    );
    {
        let mut subscriptions = state
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if subscriptions.len() >= MAX_SUBSCRIPTIONS {
            return error(
                AppErrorCodeV1::AppActivationOverloaded,
                "subscription capacity reached",
            );
        }
        subscriptions.insert(subscription_id.clone());
    }
    let mut frames = vec![AppStreamFrameV1::Open {
        schema_version: 1,
        subscription_id: subscription_id.clone(),
        sequence: 0,
        schema_digest: descriptor.output_schema_digest.clone(),
    }];
    if operation_id == "reference.events" {
        frames.extend(event_frames(&subscription_id));
    } else {
        frames.extend(export_frames(
            &subscription_id,
            descriptor.output_schema_digest,
        ));
    }
    ndjson_response(&frames)
}

fn event_frames(subscription_id: &str) -> Vec<AppStreamFrameV1> {
    let mut frames = Vec::new();
    for sequence in 1..=3 {
        frames.push(AppStreamFrameV1::Data {
            schema_version: 1,
            subscription_id: subscription_id.to_owned(),
            sequence,
            payload: json!({"event":sequence}),
        });
    }
    frames.push(AppStreamFrameV1::Checkpoint {
        schema_version: 1,
        subscription_id: subscription_id.to_owned(),
        sequence: 4,
        cursor: "cursor-3".to_owned(),
    });
    frames.push(AppStreamFrameV1::End {
        schema_version: 1,
        subscription_id: subscription_id.to_owned(),
        sequence: 5,
        reason: StreamEndReasonV1::Completed,
    });
    frames
}

fn export_frames(subscription_id: &str, schema_digest: Sha256Digest) -> Vec<AppStreamFrameV1> {
    let bytes = b"id,value\n1,reference\n";
    let now = now_ms().unwrap_or(1);
    let artifact = AppArtifactRefV1 {
        artifact_id: format!("artifact-{:x}", Sha256::digest(bytes)),
        schema_digest,
        content_digest: Sha256Digest(format!("sha256:{:x}", Sha256::digest(bytes))),
        row_count: 1,
        created_unix_ms: now,
        expires_unix_ms: now.saturating_add(60_000),
        media_type: "text/csv".to_owned(),
        metadata: BTreeMap::from([
            ("data_base64url".to_owned(), URL_SAFE_NO_PAD.encode(bytes)),
            ("inline".to_owned(), "true".to_owned()),
        ]),
    };
    vec![
        AppStreamFrameV1::Data {
            schema_version: 1,
            subscription_id: subscription_id.to_owned(),
            sequence: 1,
            payload: serde_json::to_value(artifact).unwrap_or(Value::Null),
        },
        AppStreamFrameV1::End {
            schema_version: 1,
            subscription_id: subscription_id.to_owned(),
            sequence: 2,
            reason: StreamEndReasonV1::Completed,
        },
    ]
}

fn receipt(state: &State, path: &str) -> Response<ResponseBody> {
    let receipt_id = path.trim_start_matches("/_cowd/v1/receipts/");
    let state = state
        .durable
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state
        .receipts
        .values()
        .find(|receipt| receipt.receipt_id == receipt_id)
        .map_or_else(
            || error(AppErrorCodeV1::ReceiptNotFound, "receipt not found"),
            |receipt| json_response(StatusCode::OK, receipt),
        )
}

async fn ack(request: Request<Incoming>, state: &State, path: &str) -> Response<ResponseBody> {
    let subscription_id = path
        .trim_start_matches("/_cowd/v1/subscriptions/")
        .trim_end_matches("/ack");
    if !state
        .subscriptions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(subscription_id)
    {
        return error(AppErrorCodeV1::AppNotFound, "subscription not found");
    }
    let body = match bounded_body(request).await {
        Ok(body) => body,
        Err(response) => return response,
    };
    let ack: AppStreamAckV1 = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return error(AppErrorCodeV1::InvalidRequest, "invalid acknowledgement"),
    };
    if ack.subscription_id != subscription_id || ack.validate().is_err() {
        return error(
            AppErrorCodeV1::InvalidRequest,
            "acknowledgement binding differs",
        );
    }
    empty(StatusCode::NO_CONTENT)
}

fn cancel(state: &State, path: &str) -> Response<ResponseBody> {
    let id = path.trim_start_matches("/_cowd/v1/subscriptions/");
    if state
        .subscriptions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(id)
    {
        empty(StatusCode::NO_CONTENT)
    } else {
        error(AppErrorCodeV1::AppNotFound, "subscription not found")
    }
}

async fn bounded_body(
    request: Request<Incoming>,
) -> std::result::Result<Bytes, Response<ResponseBody>> {
    if request
        .body()
        .size_hint()
        .upper()
        .is_some_and(|size| size > MAX_BODY_BYTES)
    {
        return Err(error(AppErrorCodeV1::RequestTooLarge, "request too large"));
    }
    let bytes = request
        .into_body()
        .collect()
        .await
        .map_err(|_| error(AppErrorCodeV1::InvalidRequest, "body read failed"))?
        .to_bytes();
    if bytes.len() as u64 > MAX_BODY_BYTES {
        Err(error(AppErrorCodeV1::RequestTooLarge, "request too large"))
    } else {
        Ok(bytes)
    }
}

fn persist(path: &Path, state: &DurableState) -> Result<(), WorkerError> {
    let parent = path
        .parent()
        .ok_or_else(|| WorkerError::Storage("state path has no parent".to_owned()))?;
    let temp = parent.join(format!(".reference-state-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(|error| WorkerError::Storage(error.to_string()))?;
    let bytes =
        serde_json::to_vec(state).map_err(|error| WorkerError::Storage(error.to_string()))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| WorkerError::Storage(error.to_string()))?;
    fs::rename(&temp, path).map_err(|error| WorkerError::Storage(error.to_string()))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| WorkerError::Storage(error.to_string()))
}

fn read_credential(path: &Path) -> Result<BootstrapSecretV1, WorkerError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| WorkerError::Credential(error.to_string()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != current_uid()?
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(WorkerError::Credential(
            "credential must be current-uid regular 0600 file".to_owned(),
        ));
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(0o400_000)
        .open(path)
        .map_err(|error| WorkerError::Credential(error.to_string()))?;
    let mut encoded = Zeroizing::new(String::new());
    file.take(256)
        .read_to_string(&mut encoded)
        .map_err(|error| WorkerError::Credential(error.to_string()))?;
    let secret = BootstrapSecretV1::parse_base64url(encoded.trim())
        .map_err(|_| WorkerError::Credential("invalid credential encoding".to_owned()))?;
    fs::remove_file(path).map_err(|error| WorkerError::Credential(error.to_string()))?;
    Ok(secret)
}

fn current_uid() -> Result<u32, WorkerError> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| WorkerError::Credential(error.to_string()))?;
    status
        .lines()
        .find_map(|line| {
            line.strip_prefix("Uid:")
                .and_then(|ids| ids.split_whitespace().next())
        })
        .and_then(|uid| uid.parse().ok())
        .ok_or_else(|| WorkerError::Credential("cannot determine uid".to_owned()))
}

fn parent_pid() -> Result<u32, WorkerError> {
    let stat = fs::read_to_string("/proc/self/stat")
        .map_err(|error| WorkerError::Configuration(error.to_string()))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| WorkerError::Configuration("cannot parse parent PID".to_owned()))?;
    stat[end + 1..]
        .split_whitespace()
        .nth(1)
        .and_then(|pid| pid.parse().ok())
        .filter(|pid| *pid != 0)
        .ok_or_else(|| WorkerError::Configuration("cannot parse parent PID".to_owned()))
}

fn now_ms() -> Result<u64, WorkerError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WorkerError::Configuration("clock before epoch".to_owned()))?
            .as_millis(),
    )
    .map_err(|_| WorkerError::Configuration("clock exceeds u64 milliseconds".to_owned()))
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response<ResponseBody> {
    serde_json::to_vec(value).map_or_else(
        |_| error(AppErrorCodeV1::InternalError, "response encoding failed"),
        |body| typed(status, UNARY_CONTENT_TYPE_V1, body),
    )
}

fn ndjson_response(frames: &[AppStreamFrameV1]) -> Response<ResponseBody> {
    let mut body = Vec::new();
    for frame in frames {
        let Ok(mut encoded) = serde_json::to_vec(frame) else {
            return error(AppErrorCodeV1::InternalError, "stream encoding failed");
        };
        body.append(&mut encoded);
        body.push(b'\n');
    }
    typed(StatusCode::OK, STREAM_CONTENT_TYPE_V1, body)
}

fn error(code: AppErrorCodeV1, message: &str) -> Response<ResponseBody> {
    let value = AppErrorResponseV1 {
        schema_version: 1,
        error: AppErrorDetailV1 {
            code,
            message: message.to_owned(),
            retryable: false,
            retry_after_ms: None,
            details: Value::Null,
            receipt_id: None,
        },
    };
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{\"schema_version\":1}".to_vec());
    typed(
        StatusCode::from_u16(code.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        UNARY_CONTENT_TYPE_V1,
        body,
    )
}

fn typed(status: StatusCode, content_type: &'static str, body: Vec<u8>) -> Response<ResponseBody> {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static(content_type),
    );
    response
}

fn empty(status: StatusCode) -> Response<ResponseBody> {
    let mut response = Response::new(Full::new(Bytes::new()));
    *response.status_mut() = status;
    response
}

async fn terminate_signal() -> Result<(), std::io::Error> {
    let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    signal.recv().await;
    Ok(())
}

struct SocketCleanup(PathBuf);
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _result = fs::remove_file(&self.0);
    }
}
