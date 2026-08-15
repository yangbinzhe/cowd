use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    fs,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use cowd_app_protocol::{
    verify_channel_token_authorization_v1, AppErrorCodeV1, AppErrorDetailV1, AppErrorResponseV1,
    AppId, AppInvocationEnvelopeV1, AppManifestV1, AppProviderResponseV1, ChannelTokenV1,
    DurableReceiptV1, GenerationId, OperationDescriptorV1, OperationKindV1, ProtocolValidate,
    ReceiptStatusV1, Sha256Digest, CORE_OPERATIONS_PATH_V1, HEADER_APP_GENERATION_V1,
    HEADER_APP_ID_V1, HEADER_AUTHORIZATION_V1, HEADER_CAUSATION_ID_V1, HEADER_CONTENT_TYPE_V1,
    HEADER_CORRELATION_ID_V1, HEADER_DEADLINE_UNIX_MS_V1, HEADER_PROTOCOL_VERSION_V1,
    HEADER_REQUEST_ID_V1, HEADER_SESSION_ID_V1, HEADER_TASK_ID_V1, HEADER_TENANT_ID_V1,
    HEADER_TURN_ID_V1, HEADER_WORKSPACE_ID_V1, PROTOCOL_REVISION_V1, UNARY_CONTENT_TYPE_V1,
};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, service::service_fn, Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use managed_worker_runtime::CancellationToken;
use matrix_repository::MatrixStore;
use runtime::{
    RuntimeEventInput, RuntimeEventScope, RuntimeEventStore, RuntimeTransactionEventInput,
};
use sha2::{Digest, Sha256};
use tokio::{net::UnixListener, task::JoinHandle};

use crate::services::{core_matrix_catalog, ContextService};

const CORE_INVOKE_PREFIX: &str = "/_cowd/core/v1/operations/";
const CORE_INVOKE_SUFFIX: &str = "/invoke";
const CORE_AUTHORITY: &str = "core:matrix";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoreBridgeRegistration {
    app_id: AppId,
    generation: GenerationId,
    pid: u32,
}

#[derive(Debug)]
struct CoreBridgeBinding {
    registration: CoreBridgeRegistration,
    uid: u32,
    manifest: Arc<AppManifestV1>,
    token: ChannelTokenV1,
}

#[derive(Debug, Clone)]
struct AuthorizedBinding {
    registration: CoreBridgeRegistration,
    manifest: Arc<AppManifestV1>,
}

#[derive(Debug, Default)]
pub(crate) struct CoreBridgeRegistry {
    bindings: RwLock<BTreeMap<AppId, Arc<CoreBridgeBinding>>>,
}

impl CoreBridgeRegistry {
    pub(crate) fn register(
        &self,
        app_id: AppId,
        generation: GenerationId,
        pid: u32,
        uid: u32,
        manifest: Arc<AppManifestV1>,
        token: ChannelTokenV1,
    ) -> CoreBridgeRegistration {
        let registration = CoreBridgeRegistration {
            app_id: app_id.clone(),
            generation,
            pid,
        };
        let binding = Arc::new(CoreBridgeBinding {
            registration: registration.clone(),
            uid,
            manifest,
            token,
        });
        self.bindings
            .write()
            .expect("CoreBridge registry lock poisoned")
            .insert(app_id, binding);
        registration
    }

    pub(crate) fn unregister(&self, registration: &CoreBridgeRegistration) {
        let mut bindings = self
            .bindings
            .write()
            .expect("CoreBridge registry lock poisoned");
        let matches = bindings.get(&registration.app_id).is_some_and(|binding| {
            binding.registration.generation == registration.generation
                && binding.registration.pid == registration.pid
        });
        if matches {
            bindings.remove(&registration.app_id);
        }
    }

    fn authorize(
        &self,
        app_id: &AppId,
        generation: &GenerationId,
        peer_pid: u32,
        peer_uid: u32,
        authorization: &str,
    ) -> Result<AuthorizedBinding, BridgeFailure> {
        let bindings = self
            .bindings
            .read()
            .map_err(|_| BridgeFailure::internal("CoreBridge registry lock poisoned"))?;
        let binding = bindings
            .get(app_id)
            .ok_or_else(|| BridgeFailure::unauthenticated("APP channel is not registered"))?;
        if binding.registration.generation != *generation
            || binding.registration.pid != peer_pid
            || binding.uid != peer_uid
        {
            return Err(BridgeFailure::unauthenticated(
                "APP channel peer or generation mismatch",
            ));
        }
        verify_channel_token_authorization_v1(&binding.token, authorization)
            .map_err(|_| BridgeFailure::unauthenticated("CoreBridge channel token is invalid"))?;
        Ok(AuthorizedBinding {
            registration: binding.registration.clone(),
            manifest: Arc::clone(&binding.manifest),
        })
    }
}

#[derive(Debug, Clone)]
enum CachedCommand {
    Running(Sha256Digest),
    Completed(Sha256Digest, DurableReceiptV1),
}

pub(crate) struct CoreBridgeServer {
    path: PathBuf,
    cancellation: CancellationToken,
    accept_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl CoreBridgeServer {
    pub(crate) async fn start(
        path: PathBuf,
        registry: Arc<CoreBridgeRegistry>,
        store: Arc<dyn MatrixStore>,
        event_store: Arc<RuntimeEventStore>,
    ) -> Result<Arc<Self>, String> {
        prepare_socket(&path)?;
        let listener = UnixListener::bind(&path)
            .map_err(|error| format!("bind CoreBridge socket {}: {error}", path.display()))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("secure CoreBridge socket {}: {error}", path.display()))?;
        let cancellation = CancellationToken::default();
        let task_cancel = cancellation.clone();
        let commands = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let accept_task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    () = task_cancel.cancelled() => break,
                    result = listener.accept() => match result {
                        Ok((stream, _)) => {
                            let peer = match stream.peer_cred() {
                                Ok(value) => value,
                                Err(error) => {
                                    tracing::warn!(%error, "CoreBridge rejected UDS peer without credentials");
                                    continue;
                                }
                            };
                            let Some(peer_pid) = peer.pid().and_then(|value| u32::try_from(value).ok()) else {
                                tracing::warn!("CoreBridge rejected UDS peer without PID");
                                continue;
                            };
                            let peer_uid = peer.uid();
                            let registry = Arc::clone(&registry);
                            let store = Arc::clone(&store);
                            let commands = Arc::clone(&commands);
                            let event_store = Arc::clone(&event_store);
                            connections.spawn(async move {
                                let service = service_fn(move |request| {
                                    handle_request(
                                        request,
                                        peer_pid,
                                        peer_uid,
                                        Arc::clone(&registry),
                                        Arc::clone(&store),
                                        Arc::clone(&commands),
                                        Arc::clone(&event_store),
                                    )
                                });
                                if let Err(error) = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                                    .serve_connection(TokioIo::new(stream), service)
                                    .await
                                {
                                    tracing::debug!(%error, "CoreBridge H2 connection closed");
                                }
                            });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "CoreBridge accept failed");
                            break;
                        }
                    },
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Arc::new(Self {
            path,
            cancellation,
            accept_task: tokio::sync::Mutex::new(Some(accept_task)),
        }))
    }

    pub(crate) async fn shutdown(&self) -> Result<(), String> {
        self.cancellation.cancel();
        if let Some(task) = self.accept_task.lock().await.take() {
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .map_err(|_| "CoreBridge server shutdown deadline elapsed".to_owned())?
                .map_err(|error| format!("CoreBridge server task failed: {error}"))?;
        }
        if self.path.exists() {
            fs::remove_file(&self.path).map_err(|error| {
                format!("remove CoreBridge socket {}: {error}", self.path.display())
            })?;
        }
        Ok(())
    }
}

async fn handle_request(
    request: Request<Incoming>,
    peer_pid: u32,
    peer_uid: u32,
    registry: Arc<CoreBridgeRegistry>,
    store: Arc<dyn MatrixStore>,
    commands: Arc<tokio::sync::Mutex<HashMap<String, CachedCommand>>>,
    event_store: Arc<RuntimeEventStore>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let result = route_request(
        request,
        peer_pid,
        peer_uid,
        &registry,
        store,
        commands,
        event_store,
    )
    .await;
    Ok(match result {
        Ok(response) => response,
        Err(error) => error.response(),
    })
}

async fn route_request(
    request: Request<Incoming>,
    peer_pid: u32,
    peer_uid: u32,
    registry: &CoreBridgeRegistry,
    store: Arc<dyn MatrixStore>,
    commands: Arc<tokio::sync::Mutex<HashMap<String, CachedCommand>>>,
    event_store: Arc<RuntimeEventStore>,
) -> Result<Response<Full<Bytes>>, BridgeFailure> {
    let app_id = AppId(required_header(&request, HEADER_APP_ID_V1)?.to_owned());
    app_id
        .validate_value()
        .map_err(|error| BridgeFailure::invalid(error.to_string()))?;
    let generation = GenerationId(required_header(&request, HEADER_APP_GENERATION_V1)?.to_owned());
    generation
        .validate_value()
        .map_err(|error| BridgeFailure::invalid(error.to_string()))?;
    let protocol = required_header(&request, HEADER_PROTOCOL_VERSION_V1)?;
    if protocol != PROTOCOL_REVISION_V1.to_string() {
        return Err(BridgeFailure::new(
            AppErrorCodeV1::ProtocolIncompatible,
            "CoreBridge protocol revision mismatch",
            false,
        ));
    }
    let authorization = required_header(&request, HEADER_AUTHORIZATION_V1)?;
    let binding = registry.authorize(&app_id, &generation, peer_pid, peer_uid, authorization)?;
    let catalog = core_matrix_catalog::projected_catalog(&binding.manifest, &generation)
        .map_err(|error| BridgeFailure::internal(error.to_string()))?;

    if request.method() == Method::GET && request.uri().path() == CORE_OPERATIONS_PATH_V1 {
        return json_response(StatusCode::OK, &catalog);
    }
    if request.method() != Method::POST {
        return Err(BridgeFailure::invalid("unsupported CoreBridge method"));
    }
    if required_header(&request, HEADER_CONTENT_TYPE_V1)? != UNARY_CONTENT_TYPE_V1 {
        return Err(BridgeFailure::invalid("invalid CoreBridge content type"));
    }
    let operation_id = parse_invoke_path(request.uri().path())?;
    let descriptor = catalog
        .operations
        .iter()
        .find(|operation| operation.operation_id == operation_id)
        .cloned()
        .ok_or_else(|| {
            BridgeFailure::new(
                AppErrorCodeV1::OperationNotGranted,
                "operation is outside the signed APP catalog",
                false,
            )
        })?;
    let header_snapshot = InvocationHeaders::from_request(&request)?;
    let body = Limited::new(
        request.into_body(),
        descriptor.maximum_request_bytes as usize,
    )
    .collect()
    .await
    .map_err(|_| {
        BridgeFailure::new(
            AppErrorCodeV1::RequestTooLarge,
            "CoreBridge request body exceeds operation limit",
            false,
        )
    })?
    .to_bytes();
    let mut envelope: AppInvocationEnvelopeV1 =
        serde_json::from_slice(&body).map_err(|error| BridgeFailure::invalid(error.to_string()))?;
    header_snapshot.validate(&envelope)?;
    let now = now_unix_ms();
    envelope
        .validate_at(now, &descriptor)
        .map_err(|error| validation_failure(error.to_string()))?;
    validate_manifest_profile(&binding.manifest, &envelope, &descriptor)?;
    envelope
        .append_authority(CORE_AUTHORITY.to_owned())
        .map_err(|error| validation_failure(error.to_string()))?;
    let timeout_ms = envelope.effective_deadline_unix_ms().saturating_sub(now);
    let request_id = envelope.request_id.clone();
    let payload = envelope.payload.clone();
    let operation_for_dispatch = operation_id.clone();

    if descriptor.kind == OperationKindV1::Command {
        let key = envelope.idempotency_key.clone().ok_or_else(|| {
            BridgeFailure::new(
                AppErrorCodeV1::IdempotencyConflict,
                "command idempotency key is missing",
                false,
            )
        })?;
        let digest = payload_digest(&binding.registration, &operation_id, &payload)?;
        let cache_key = format!("{}:{key}", binding.registration.app_id.0);
        {
            let mut cache = commands.lock().await;
            match cache.get(&cache_key) {
                Some(CachedCommand::Completed(existing, receipt)) if existing == &digest => {
                    let mut replayed = receipt.clone();
                    replayed.replayed = true;
                    return json_response(StatusCode::OK, &replayed);
                }
                Some(CachedCommand::Running(existing)) if existing == &digest => {
                    return Err(BridgeFailure::new(
                        AppErrorCodeV1::DependencyUnavailable,
                        "command outcome is not yet durable",
                        true,
                    ));
                }
                Some(_) => {
                    return Err(BridgeFailure::new(
                        AppErrorCodeV1::IdempotencyConflict,
                        "idempotency key was used with a different command payload",
                        false,
                    ));
                }
                None => {
                    if cache.len() >= 4096 {
                        let completed = cache.iter().find_map(|(key, value)| {
                            matches!(value, CachedCommand::Completed(_, _)).then(|| key.clone())
                        });
                        if let Some(completed) = completed {
                            cache.remove(&completed);
                        } else {
                            return Err(BridgeFailure::new(
                                AppErrorCodeV1::AppActivationOverloaded,
                                "CoreBridge command fence capacity is exhausted",
                                true,
                            ));
                        }
                    }
                    cache.insert(cache_key.clone(), CachedCommand::Running(digest.clone()));
                }
            }
        }
        match durable_command_state(&event_store, &binding.registration, &key, &digest)? {
            DurableCommandState::New { stream_id } => {
                append_command_intent(
                    &event_store,
                    &stream_id,
                    &binding.registration,
                    &operation_id,
                    &key,
                    &digest,
                    &envelope,
                )?;
            }
            DurableCommandState::Completed(mut receipt) => {
                receipt.replayed = true;
                commands
                    .lock()
                    .await
                    .insert(cache_key, CachedCommand::Completed(digest, receipt.clone()));
                return json_response(StatusCode::OK, &receipt);
            }
            DurableCommandState::Unknown => {
                return Err(BridgeFailure::new(
                    AppErrorCodeV1::DependencyUnavailable,
                    "durable command intent exists without a terminal receipt",
                    true,
                ));
            }
        }
        let output =
            dispatch_with_deadline(store, operation_for_dispatch, payload, timeout_ms).await?;
        let receipt = DurableReceiptV1 {
            schema_version: 1,
            request_id,
            receipt_id: format!("core-matrix:{key}"),
            idempotency_key: key.clone(),
            status: ReceiptStatusV1::Completed,
            result_revision: envelope.expected_revision,
            replayed: false,
            payload_digest: digest.clone(),
            payload: output,
        };
        receipt
            .validate()
            .map_err(|error| BridgeFailure::internal(error.to_string()))?;
        append_command_receipt(&event_store, &binding.registration, &key, &digest, &receipt)?;
        commands
            .lock()
            .await
            .insert(cache_key, CachedCommand::Completed(digest, receipt.clone()));
        json_response(StatusCode::OK, &receipt)
    } else {
        let output =
            dispatch_with_deadline(store, operation_for_dispatch, payload, timeout_ms).await?;
        let response = AppProviderResponseV1 {
            schema_version: 1,
            request_id,
            output_schema_digest: descriptor.output_schema_digest,
            revision: envelope.expected_revision,
            payload: output,
        };
        response
            .validate()
            .map_err(|error| BridgeFailure::internal(error.to_string()))?;
        json_response(StatusCode::OK, &response)
    }
}

#[derive(Debug)]
enum DurableCommandState {
    New { stream_id: String },
    Completed(DurableReceiptV1),
    Unknown,
}

fn durable_command_state(
    event_store: &RuntimeEventStore,
    registration: &CoreBridgeRegistration,
    key: &str,
    digest: &Sha256Digest,
) -> Result<DurableCommandState, BridgeFailure> {
    let stream_id = command_stream_id(registration, key);
    let intent = event_store
        .event_by_idempotency_key(&stream_id, "intent")
        .map_err(|error| {
            BridgeFailure::new(
                AppErrorCodeV1::DependencyUnavailable,
                error.to_string(),
                true,
            )
        })?;
    let Some(intent) = intent else {
        return Ok(DurableCommandState::New { stream_id });
    };
    if intent
        .payload
        .get("payload_digest")
        .and_then(serde_json::Value::as_str)
        != Some(digest.0.as_str())
    {
        return Err(BridgeFailure::new(
            AppErrorCodeV1::IdempotencyConflict,
            "durable idempotency key belongs to a different command payload",
            false,
        ));
    }
    let receipt = event_store
        .event_by_idempotency_key(&stream_id, "receipt")
        .map_err(|error| {
            BridgeFailure::new(
                AppErrorCodeV1::DependencyUnavailable,
                error.to_string(),
                true,
            )
        })?;
    receipt.map_or(Ok(DurableCommandState::Unknown), |event| {
        serde_json::from_value::<DurableReceiptV1>(event.payload)
            .map(DurableCommandState::Completed)
            .map_err(|error| {
                BridgeFailure::internal(format!("invalid durable CoreBridge receipt: {error}"))
            })
    })
}

fn append_command_intent(
    event_store: &RuntimeEventStore,
    stream_id: &str,
    registration: &CoreBridgeRegistration,
    operation_id: &str,
    key: &str,
    digest: &Sha256Digest,
    envelope: &AppInvocationEnvelopeV1,
) -> Result<(), BridgeFailure> {
    let revision = event_store.stream_revision(stream_id).map_err(|error| {
        BridgeFailure::new(
            AppErrorCodeV1::DependencyUnavailable,
            error.to_string(),
            true,
        )
    })?;
    let event = RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: stream_id.to_owned(),
            scope: RuntimeEventScope::CrossPlane,
            kind: "core_bridge.command_intent.v1".to_owned(),
            status: Some("accepted".to_owned()),
            actor: Some(format!("app:{}", registration.app_id.0)),
            refs: Vec::new(),
            payload: serde_json::json!({
                "app_id": registration.app_id,
                "generation": registration.generation,
                "worker_pid": registration.pid,
                "operation_id": operation_id,
                "idempotency_key": key,
                "payload_digest": digest.0,
                "request_id": envelope.request_id,
                "principal_subject": envelope.principal.subject,
                "workspace_id": envelope.principal.workspace_id,
            }),
        },
        idempotency_key: Some("intent".to_owned()),
        schema_version: 1,
    };
    event_store
        .append_batch_if_revision(
            stream_id.to_owned(),
            revision,
            format!("core-bridge-intent:{}", digest.0),
            vec![event],
        )
        .map(|_| ())
        .map_err(|error| {
            BridgeFailure::new(
                AppErrorCodeV1::DependencyUnavailable,
                error.to_string(),
                true,
            )
        })
}

fn append_command_receipt(
    event_store: &RuntimeEventStore,
    registration: &CoreBridgeRegistration,
    key: &str,
    digest: &Sha256Digest,
    receipt: &DurableReceiptV1,
) -> Result<(), BridgeFailure> {
    let stream_id = command_stream_id(registration, key);
    let revision = event_store.stream_revision(&stream_id).map_err(|error| {
        BridgeFailure::new(
            AppErrorCodeV1::DependencyUnavailable,
            error.to_string(),
            true,
        )
    })?;
    let payload = serde_json::to_value(receipt)
        .map_err(|error| BridgeFailure::internal(error.to_string()))?;
    event_store
        .append_batch_if_revision(
            stream_id.clone(),
            revision,
            format!("core-bridge-receipt:{}", digest.0),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id,
                    scope: RuntimeEventScope::CrossPlane,
                    kind: "core_bridge.command_receipt.v1".to_owned(),
                    status: Some("completed".to_owned()),
                    actor: Some("core:matrix".to_owned()),
                    refs: Vec::new(),
                    payload,
                },
                idempotency_key: Some("receipt".to_owned()),
                schema_version: 1,
            }],
        )
        .map(|_| ())
        .map_err(|error| {
            BridgeFailure::new(
                AppErrorCodeV1::DependencyUnavailable,
                error.to_string(),
                true,
            )
        })
}

fn command_stream_id(registration: &CoreBridgeRegistration, key: &str) -> String {
    let digest = Sha256::digest(format!("{}\0{key}", registration.app_id.0).as_bytes());
    format!("core-bridge-command:{digest:x}")
}

async fn dispatch_with_deadline(
    store: Arc<dyn MatrixStore>,
    operation_id: String,
    payload: serde_json::Value,
    timeout_ms: u64,
) -> Result<serde_json::Value, BridgeFailure> {
    if timeout_ms == 0 {
        return Err(BridgeFailure::deadline());
    }
    let task = tokio::task::spawn_blocking(move || {
        core_matrix_catalog::dispatch_operation(
            store.as_ref(),
            &ContextService::new(),
            &operation_id,
            &payload,
        )
    });
    tokio::time::timeout(Duration::from_millis(timeout_ms), task)
        .await
        .map_err(|_| BridgeFailure::deadline())?
        .map_err(|error| BridgeFailure::internal(error.to_string()))?
        .map_err(|error| match error.code {
            "not_found" | "validation_failed" => BridgeFailure::invalid(error.detail),
            "revision_conflict" => {
                BridgeFailure::new(AppErrorCodeV1::RevisionConflict, error.detail, false)
            }
            _ => BridgeFailure::new(AppErrorCodeV1::DependencyUnavailable, error.detail, true),
        })
}

fn validate_manifest_profile(
    manifest: &AppManifestV1,
    envelope: &AppInvocationEnvelopeV1,
    descriptor: &OperationDescriptorV1,
) -> Result<(), BridgeFailure> {
    let profile = manifest
        .authorization_profiles
        .iter()
        .find(|profile| profile.profile_id == envelope.principal.authorization_profile_id)
        .ok_or_else(|| {
            BridgeFailure::new(
                AppErrorCodeV1::OperationNotGranted,
                "authorization profile is outside the signed APP manifest",
                false,
            )
        })?;
    let mut allowed = profile.capabilities.clone();
    if let Some(surface) = profile
        .surface_capabilities
        .get(&envelope.execution.surface)
    {
        allowed.extend(surface.iter().cloned());
    }
    allowed.sort();
    allowed.dedup();
    if envelope
        .principal
        .granted_capabilities
        .iter()
        .any(|capability| allowed.binary_search(capability).is_err())
        || allowed
            .binary_search(&descriptor.required_capability)
            .is_err()
    {
        return Err(BridgeFailure::new(
            AppErrorCodeV1::OperationNotGranted,
            "principal capabilities exceed the signed APP authorization profile",
            false,
        ));
    }
    Ok(())
}

fn parse_invoke_path(path: &str) -> Result<String, BridgeFailure> {
    let operation = path
        .strip_prefix(CORE_INVOKE_PREFIX)
        .and_then(|value| value.strip_suffix(CORE_INVOKE_SUFFIX))
        .filter(|value| !value.is_empty() && !value.contains('/'))
        .ok_or_else(|| BridgeFailure::invalid("invalid CoreBridge invocation path"))?;
    Ok(operation.to_owned())
}

#[derive(Debug)]
struct InvocationHeaders {
    request_id: String,
    correlation_id: String,
    causation_id: Option<String>,
    deadline: u64,
    tenant_id: String,
    workspace_id: String,
    session_id: Option<String>,
    turn_id: Option<String>,
    task_id: Option<String>,
}

impl InvocationHeaders {
    fn from_request(request: &Request<Incoming>) -> Result<Self, BridgeFailure> {
        Ok(Self {
            request_id: required_header(request, HEADER_REQUEST_ID_V1)?.to_owned(),
            correlation_id: required_header(request, HEADER_CORRELATION_ID_V1)?.to_owned(),
            causation_id: optional_header(request, HEADER_CAUSATION_ID_V1)?,
            deadline: required_header(request, HEADER_DEADLINE_UNIX_MS_V1)?
                .parse()
                .map_err(|_| BridgeFailure::invalid("invalid deadline header"))?,
            tenant_id: required_header(request, HEADER_TENANT_ID_V1)?.to_owned(),
            workspace_id: required_header(request, HEADER_WORKSPACE_ID_V1)?.to_owned(),
            session_id: optional_header(request, HEADER_SESSION_ID_V1)?,
            turn_id: optional_header(request, HEADER_TURN_ID_V1)?,
            task_id: optional_header(request, HEADER_TASK_ID_V1)?,
        })
    }

    fn validate(&self, envelope: &AppInvocationEnvelopeV1) -> Result<(), BridgeFailure> {
        if self.request_id != envelope.request_id
            || self.correlation_id != envelope.correlation_id
            || self.causation_id != envelope.causation_id
            || self.deadline != envelope.deadline_unix_ms
            || self.tenant_id != envelope.principal.tenant_id
            || self.workspace_id != envelope.principal.workspace_id
            || self.session_id != envelope.execution.session_id
            || self.turn_id != envelope.execution.turn_id
            || self.task_id != envelope.execution.task_id
        {
            return Err(BridgeFailure::invalid(
                "CoreBridge headers do not match invocation envelope",
            ));
        }
        Ok(())
    }
}

fn required_header<'a>(
    request: &'a Request<Incoming>,
    name: &'static str,
) -> Result<&'a str, BridgeFailure> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| BridgeFailure::invalid(format!("missing or invalid header `{name}`")))
}

fn optional_header(
    request: &Request<Incoming>,
    name: &'static str,
) -> Result<Option<String>, BridgeFailure> {
    request
        .headers()
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| BridgeFailure::invalid(format!("invalid header `{name}`")))
        })
        .transpose()
}

fn payload_digest(
    registration: &CoreBridgeRegistration,
    operation_id: &str,
    payload: &serde_json::Value,
) -> Result<Sha256Digest, BridgeFailure> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "domain": "cowd.core-bridge.command/v1",
        "app_id": registration.app_id,
        "generation": registration.generation,
        "operation_id": operation_id,
        "payload": payload,
    }))
    .map_err(|error| BridgeFailure::internal(error.to_string()))?;
    Ok(Sha256Digest(format!("sha256:{:x}", Sha256::digest(bytes))))
}

fn validation_failure(detail: String) -> BridgeFailure {
    if detail.contains("CALL_CYCLE_DETECTED") {
        BridgeFailure::new(AppErrorCodeV1::CallCycleDetected, detail, false)
    } else if detail.contains("expired") {
        BridgeFailure::deadline()
    } else {
        BridgeFailure::invalid(detail)
    }
}

fn json_response<T: serde::Serialize>(
    status: StatusCode,
    value: &T,
) -> Result<Response<Full<Bytes>>, BridgeFailure> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| BridgeFailure::internal(error.to_string()))?;
    Response::builder()
        .status(status)
        .header(HEADER_CONTENT_TYPE_V1, UNARY_CONTENT_TYPE_V1)
        .body(Full::new(Bytes::from(bytes)))
        .map_err(|error| BridgeFailure::internal(error.to_string()))
}

#[derive(Debug)]
struct BridgeFailure {
    code: AppErrorCodeV1,
    detail: String,
    retryable: bool,
}

impl BridgeFailure {
    fn new(code: AppErrorCodeV1, detail: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            detail: detail.into(),
            retryable,
        }
    }

    fn invalid(detail: impl Into<String>) -> Self {
        Self::new(AppErrorCodeV1::InvalidRequest, detail, false)
    }

    fn unauthenticated(detail: impl Into<String>) -> Self {
        Self::new(AppErrorCodeV1::Unauthenticated, detail, false)
    }

    fn internal(detail: impl Into<String>) -> Self {
        Self::new(AppErrorCodeV1::InternalError, detail, false)
    }

    fn deadline() -> Self {
        Self::new(
            AppErrorCodeV1::DeadlineExceeded,
            "CoreBridge invocation deadline elapsed",
            false,
        )
    }

    fn response(self) -> Response<Full<Bytes>> {
        let status = StatusCode::from_u16(self.code.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let response = AppErrorResponseV1 {
            schema_version: 1,
            error: AppErrorDetailV1 {
                code: self.code,
                message: self.detail,
                retryable: self.retryable,
                retry_after_ms: None,
                details: serde_json::Value::Null,
                receipt_id: None,
            },
        };
        json_response(status, &response).unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::new()))
                .expect("static response")
        })
    }
}

fn prepare_socket(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "CoreBridge socket requires a parent directory".to_owned())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("create CoreBridge runtime directory: {error}"))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
        .map_err(|error| format!("secure CoreBridge runtime directory: {error}"))?;
    if path.exists() {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("inspect stale CoreBridge socket: {error}"))?;
        if !metadata.file_type().is_socket() {
            return Err("CoreBridge socket path exists and is not a socket".to_owned());
        }
        fs::remove_file(path)
            .map_err(|error| format!("remove stale CoreBridge socket: {error}"))?;
    }
    Ok(())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cowd_app_protocol::{
        derive_channel_token_v1, format_channel_authorization_v1, AppPresentationV1, AppSurfacesV1,
        AuthorizationProfileV1, BootstrapSecretV1, BundleIntegrityV1, BundleSignatureV1,
        ChannelPurposeV1, CoreBridgeRequirementV1, DelegationKindV1, ExecutionContextV1,
        FilesystemPolicyV1, IntegrityAlgorithmV1, NetworkPolicyV1, ProtocolRangeV1,
        SandboxProfileV1, SignatureAlgorithmV1,
    };
    use http_body_util::BodyExt;
    use managed_worker_runtime::{GenerationFence, ManagedH2Channel, PeerCredentialPolicy};
    use matrix_repository::MatrixSqliteRepository;

    use super::*;

    fn fixture_manifest(
        definition: &core_matrix_catalog::CoreMatrixOperationDefinition,
    ) -> AppManifestV1 {
        let placeholder = Sha256Digest(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        let mut manifest = AppManifestV1 {
            schema_version: 1,
            app_id: AppId("fixture".to_owned()),
            display_name: "Fixture".to_owned(),
            artifact_version: "1.0.0".to_owned(),
            required_protocol: ProtocolRangeV1::exact_v1(),
            executable: "bin/worker".to_owned(),
            web_root: None,
            capabilities: vec!["app.fixture.read".to_owned()],
            authorization_profiles: vec![AuthorizationProfileV1 {
                profile_id: "operator".to_owned(),
                display_name: "Operator".to_owned(),
                capabilities: vec!["app.fixture.read".to_owned()],
                surface_capabilities: BTreeMap::new(),
                is_default: true,
            }],
            core_bridge_requirements: vec![CoreBridgeRequirementV1 {
                app_operation_id: "fixture.health".to_owned(),
                core_operation_id: definition.descriptor.operation_id.clone(),
                accepted_input_schema_digest: definition.descriptor.input_schema_digest.clone(),
                accepted_output_schema_digest: definition.descriptor.output_schema_digest.clone(),
                required_app_capability: "app.fixture.read".to_owned(),
                kind: definition.descriptor.kind,
                streaming: false,
            }],
            surfaces: AppSurfacesV1 {
                web: false,
                tui_view: false,
            },
            integrity: BundleIntegrityV1 {
                algorithm: IntegrityAlgorithmV1::Sha256,
                files: BTreeMap::from([("bin/worker".to_owned(), placeholder.clone())]),
                manifest_digest: placeholder.clone(),
            },
            signature: BundleSignatureV1 {
                algorithm: SignatureAlgorithmV1::Ed25519,
                key_id: "fixture-key".to_owned(),
                signature: "AA".to_owned(),
                signed_digest: placeholder.clone(),
                expires_unix_ms: None,
                provenance_digest: Some(placeholder),
            },
            sandbox: SandboxProfileV1 {
                filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
                network: NetworkPolicyV1::Deny,
                max_processes: 8,
                max_open_files: 256,
                max_memory_bytes: 64 * 1024 * 1024,
                cpu_quota_millis_per_second: 1_000,
            },
            presentation: None::<AppPresentationV1>,
        };
        manifest
            .bind_canonical_signed_digest()
            .expect("manifest digest");
        manifest
    }

    fn base_request(
        method: Method,
        uri: &str,
        generation: &GenerationId,
        authorization: &str,
        body: Bytes,
    ) -> Request<Full<Bytes>> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(HEADER_APP_ID_V1, "fixture")
            .header(HEADER_APP_GENERATION_V1, &generation.0)
            .header(HEADER_PROTOCOL_VERSION_V1, PROTOCOL_REVISION_V1)
            .header(HEADER_AUTHORIZATION_V1, authorization)
            .body(Full::new(body))
            .expect("request")
    }

    #[tokio::test]
    async fn real_uds_h2_enforces_core_token_and_dispatches_catalog_and_query() {
        let definition = core_matrix_catalog::definitions()
            .expect("definitions")
            .into_iter()
            .find(|definition| definition.short_id == "health")
            .expect("health");
        let manifest = Arc::new(fixture_manifest(&definition));
        let generation = GenerationId(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        );
        let secret = BootstrapSecretV1::from_bytes(&[0x42; 32]).expect("secret");
        let core_token = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::CoreBridge,
            &manifest.app_id,
            &generation,
            std::process::id(),
            "worker-nonce",
        )
        .expect("core token");
        let worker_token = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &manifest.app_id,
            &generation,
            std::process::id(),
            "worker-nonce",
        )
        .expect("worker token");
        let authorization = format_channel_authorization_v1(&core_token);
        let wrong_authorization = format_channel_authorization_v1(&worker_token);
        let registry = Arc::new(CoreBridgeRegistry::default());
        registry.register(
            manifest.app_id.clone(),
            generation.clone(),
            std::process::id(),
            unsafe { libc::geteuid() },
            Arc::clone(&manifest),
            core_token,
        );
        let root = tempfile::tempdir().expect("tempdir");
        let socket = root.path().join("core.sock");
        let server = CoreBridgeServer::start(
            socket.clone(),
            Arc::clone(&registry),
            Arc::new(MatrixSqliteRepository::in_memory().expect("matrix")),
            Arc::new(RuntimeEventStore::open_in_memory().expect("events")),
        )
        .await
        .expect("server");
        let cancellation = CancellationToken::default();
        let channel = ManagedH2Channel::connect_verified(
            &socket,
            GenerationFence::new(generation.0.clone()).expect("fence"),
            &cancellation,
            tokio::time::Instant::now() + Duration::from_secs(2),
            PeerCredentialPolicy::CurrentUidAndExactPid {
                uid: unsafe { libc::geteuid() },
                pid: std::process::id(),
            },
        )
        .await
        .expect("channel");

        let response = channel
            .send(
                &generation.0,
                base_request(
                    Method::GET,
                    CORE_OPERATIONS_PATH_V1,
                    &generation,
                    &wrong_authorization,
                    Bytes::new(),
                ),
                Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("wrong-token response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = channel
            .send(
                &generation.0,
                base_request(
                    Method::GET,
                    CORE_OPERATIONS_PATH_V1,
                    &generation,
                    &authorization,
                    Bytes::new(),
                ),
                Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("catalog response");
        assert_eq!(response.status(), StatusCode::OK);
        let catalog: cowd_app_protocol::CoreOperationCatalogV1 = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("catalog");
        assert_eq!(catalog.operations.len(), 1);

        let now = now_unix_ms();
        let envelope = AppInvocationEnvelopeV1 {
            schema_version: 1,
            operation_id: definition.descriptor.operation_id.clone(),
            request_id: "request-1".to_owned(),
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            deadline_unix_ms: now + 5_000,
            idempotency_key: None,
            expected_revision: None,
            call_chain: vec!["app:fixture".to_owned()],
            max_hops: 4,
            input_schema_digest: definition.descriptor.input_schema_digest.clone(),
            principal: cowd_app_protocol::PrincipalContextV1 {
                subject: "fixture-worker".to_owned(),
                tenant_id: "deployment-tenant".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                delegation: DelegationKindV1::Service,
                grant_id: "grant-1".to_owned(),
                authorization_profile_id: "operator".to_owned(),
                authorization_revision: 1,
                granted_capabilities: vec!["app.fixture.read".to_owned()],
                granted_scopes: Vec::new(),
                credential_epoch: 1,
                expires_at_unix_ms: Some(now + 5_000),
            },
            execution: ExecutionContextV1 {
                surface: "worker".to_owned(),
                session_id: None,
                turn_id: None,
                task_id: None,
            },
            payload: serde_json::json!({}),
        };
        let mut request = base_request(
            Method::POST,
            &format!(
                "{CORE_INVOKE_PREFIX}{}{CORE_INVOKE_SUFFIX}",
                envelope.operation_id
            ),
            &generation,
            &authorization,
            Bytes::from(serde_json::to_vec(&envelope).expect("envelope")),
        );
        for (name, value) in [
            (HEADER_CONTENT_TYPE_V1, UNARY_CONTENT_TYPE_V1.to_owned()),
            (HEADER_REQUEST_ID_V1, envelope.request_id.clone()),
            (HEADER_CORRELATION_ID_V1, envelope.correlation_id.clone()),
            (
                HEADER_DEADLINE_UNIX_MS_V1,
                envelope.deadline_unix_ms.to_string(),
            ),
            (HEADER_TENANT_ID_V1, envelope.principal.tenant_id.clone()),
            (
                HEADER_WORKSPACE_ID_V1,
                envelope.principal.workspace_id.clone(),
            ),
        ] {
            request
                .headers_mut()
                .insert(name, value.parse().expect("header value"));
        }
        let response = channel
            .send(
                &generation.0,
                request,
                Duration::from_secs(2),
                &cancellation,
            )
            .await
            .expect("invoke response");
        assert_eq!(response.status(), StatusCode::OK);
        let response: AppProviderResponseV1 = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("provider response");
        assert_eq!(response.request_id, "request-1");
        assert_eq!(
            response.output_schema_digest,
            definition.descriptor.output_schema_digest
        );

        server.shutdown().await.expect("shutdown");
        assert!(!socket.exists());
    }

    #[test]
    fn durable_command_fence_replays_receipt_and_rejects_changed_payload() {
        let store = RuntimeEventStore::open_in_memory().expect("events");
        let registration = CoreBridgeRegistration {
            app_id: AppId("fixture".to_owned()),
            generation: GenerationId(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ),
            pid: 42,
        };
        let digest = Sha256Digest(
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        );
        let changed = Sha256Digest(
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
        );
        let envelope = AppInvocationEnvelopeV1 {
            schema_version: 1,
            operation_id: "core.matrix.metric.recompute".to_owned(),
            request_id: "request-1".to_owned(),
            correlation_id: "correlation-1".to_owned(),
            causation_id: None,
            deadline_unix_ms: now_unix_ms() + 1_000,
            idempotency_key: Some("idem-1".to_owned()),
            expected_revision: None,
            call_chain: vec!["app:fixture".to_owned()],
            max_hops: 4,
            input_schema_digest: digest.clone(),
            principal: cowd_app_protocol::PrincipalContextV1 {
                subject: "fixture-worker".to_owned(),
                tenant_id: "deployment-tenant".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                delegation: DelegationKindV1::Service,
                grant_id: "grant-1".to_owned(),
                authorization_profile_id: "operator".to_owned(),
                authorization_revision: 1,
                granted_capabilities: vec!["app.fixture.write".to_owned()],
                granted_scopes: Vec::new(),
                credential_epoch: 1,
                expires_at_unix_ms: None,
            },
            execution: ExecutionContextV1 {
                surface: "worker".to_owned(),
                session_id: None,
                turn_id: None,
                task_id: None,
            },
            payload: serde_json::json!({}),
        };
        let DurableCommandState::New { stream_id } =
            durable_command_state(&store, &registration, "idem-1", &digest).expect("new")
        else {
            panic!("expected new command");
        };
        append_command_intent(
            &store,
            &stream_id,
            &registration,
            &envelope.operation_id,
            "idem-1",
            &digest,
            &envelope,
        )
        .expect("intent");
        assert!(matches!(
            durable_command_state(&store, &registration, "idem-1", &digest).expect("unknown"),
            DurableCommandState::Unknown
        ));
        let conflict = durable_command_state(&store, &registration, "idem-1", &changed)
            .expect_err("changed payload rejected");
        assert_eq!(conflict.code, AppErrorCodeV1::IdempotencyConflict);
        let receipt = DurableReceiptV1 {
            schema_version: 1,
            request_id: "request-1".to_owned(),
            receipt_id: "core-matrix:idem-1".to_owned(),
            idempotency_key: "idem-1".to_owned(),
            status: ReceiptStatusV1::Completed,
            result_revision: None,
            replayed: false,
            payload_digest: digest.clone(),
            payload: serde_json::json!({"ok": true}),
        };
        append_command_receipt(&store, &registration, "idem-1", &digest, &receipt)
            .expect("receipt");
        let DurableCommandState::Completed(replayed) =
            durable_command_state(&store, &registration, "idem-1", &digest).expect("completed")
        else {
            panic!("expected completed command");
        };
        assert_eq!(replayed, receipt);
    }
}
