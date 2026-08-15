use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    fs,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use cowd_app_protocol::{
    verify_channel_token_authorization_v1, AppErrorCodeV1, AppErrorDetailV1, AppErrorResponseV1,
    AppId, AppInvocationEnvelopeV1, AppManifestV1, AppProviderResponseV1, ChannelTokenV1,
    CoreBridgeInvocationV1, DurableReceiptV1, GenerationId, OperationDescriptorV1, OperationKindV1,
    ProtocolValidate, ReceiptStatusV1, Sha256Digest, CORE_OPERATIONS_PATH_V1,
    HEADER_APP_GENERATION_V1, HEADER_APP_ID_V1, HEADER_AUTHORIZATION_V1, HEADER_CAUSATION_ID_V1,
    HEADER_CONTENT_TYPE_V1, HEADER_CORRELATION_ID_V1, HEADER_DEADLINE_UNIX_MS_V1,
    HEADER_PROTOCOL_VERSION_V1, HEADER_REQUEST_ID_V1, HEADER_SESSION_ID_V1, HEADER_TASK_ID_V1,
    HEADER_TENANT_ID_V1, HEADER_TURN_ID_V1, HEADER_WORKSPACE_ID_V1, PROTOCOL_REVISION_V1,
    UNARY_CONTENT_TYPE_V1,
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

use crate::{
    api_routes::AppState,
    services::{core_matrix_catalog, core_platform_operations, ContextService},
};

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

struct CoreBridgeRequestDependencies {
    registry: Arc<CoreBridgeRegistry>,
    store: Arc<dyn MatrixStore>,
    commands: Arc<tokio::sync::Mutex<HashMap<String, CachedCommand>>>,
    event_store: Arc<RuntimeEventStore>,
    app_state: Arc<OnceLock<Arc<AppState>>>,
}

pub(crate) struct CoreBridgeServer {
    path: PathBuf,
    cancellation: CancellationToken,
    accept_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    app_state: Arc<OnceLock<Arc<AppState>>>,
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
        let app_state = Arc::new(OnceLock::new());
        let dependencies = Arc::new(CoreBridgeRequestDependencies {
            registry,
            store,
            commands: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            event_store,
            app_state: Arc::clone(&app_state),
        });
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
                            let dependencies = Arc::clone(&dependencies);
                            connections.spawn(async move {
                                let service = service_fn(move |request| {
                                    handle_request(
                                        request,
                                        peer_pid,
                                        peer_uid,
                                        Arc::clone(&dependencies),
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
            app_state,
        }))
    }

    pub(crate) fn bind_app_state(&self, state: Arc<AppState>) -> Result<(), String> {
        self.app_state
            .set(state)
            .map_err(|_| "CoreBridge Gateway dependencies are already bound".to_owned())
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
    dependencies: Arc<CoreBridgeRequestDependencies>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let result = route_request(request, peer_pid, peer_uid, dependencies).await;
    Ok(match result {
        Ok(response) => response,
        Err(error) => error.response(),
    })
}

async fn route_request(
    request: Request<Incoming>,
    peer_pid: u32,
    peer_uid: u32,
    dependencies: Arc<CoreBridgeRequestDependencies>,
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
    let binding =
        dependencies
            .registry
            .authorize(&app_id, &generation, peer_pid, peer_uid, authorization)?;
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
    let mut bridge_invocation: CoreBridgeInvocationV1 =
        serde_json::from_slice(&body).map_err(|error| BridgeFailure::invalid(error.to_string()))?;
    header_snapshot.validate(&bridge_invocation.invocation)?;
    let now = now_unix_ms();
    bridge_invocation
        .validate_at_for_manifest(now, &descriptor, &binding.manifest)
        .map_err(|error| validation_failure(error.to_string()))?;
    validate_manifest_profile(
        &binding.manifest,
        &bridge_invocation.invocation,
        &descriptor,
        &bridge_invocation.originating_app_operation_id,
    )?;
    let originating_app_operation_id = bridge_invocation.originating_app_operation_id.clone();
    let envelope = &mut bridge_invocation.invocation;
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
        let digest = payload_digest(
            &binding.registration,
            &originating_app_operation_id,
            &operation_id,
            envelope.expected_revision.as_deref(),
            &payload,
        )?;
        let cache_key = format!("{}:{key}", binding.registration.app_id.0);
        {
            let mut cache = dependencies.commands.lock().await;
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
        match durable_command_state(
            &dependencies.event_store,
            &binding.registration,
            &key,
            &digest,
        )? {
            DurableCommandState::New { stream_id } => {
                append_command_intent(
                    &dependencies.event_store,
                    &stream_id,
                    &binding.registration,
                    &DurableCommandIntent {
                        originating_app_operation_id: &originating_app_operation_id,
                        operation_id: &operation_id,
                        key: &key,
                        digest: &digest,
                        envelope,
                    },
                )?;
            }
            DurableCommandState::Completed(mut receipt) => {
                receipt.replayed = true;
                dependencies
                    .commands
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
        let output = dispatch_with_deadline(
            Arc::clone(&dependencies.store),
            Arc::clone(&dependencies.app_state),
            binding.registration.app_id.0.clone(),
            envelope,
            operation_for_dispatch,
            payload,
            timeout_ms,
        )
        .await?;
        let receipt = DurableReceiptV1 {
            schema_version: 1,
            request_id,
            receipt_id: format!("core-matrix:{key}"),
            idempotency_key: key.clone(),
            status: ReceiptStatusV1::Completed,
            result_revision: envelope.expected_revision.clone(),
            replayed: false,
            payload_digest: digest.clone(),
            payload: output,
        };
        receipt
            .validate()
            .map_err(|error| BridgeFailure::internal(error.to_string()))?;
        append_command_receipt(
            &dependencies.event_store,
            &binding.registration,
            &key,
            &receipt,
        )?;
        dependencies
            .commands
            .lock()
            .await
            .insert(cache_key, CachedCommand::Completed(digest, receipt.clone()));
        json_response(StatusCode::OK, &receipt)
    } else {
        let output = dispatch_with_deadline(
            Arc::clone(&dependencies.store),
            Arc::clone(&dependencies.app_state),
            binding.registration.app_id.0.clone(),
            envelope,
            operation_for_dispatch,
            payload,
            timeout_ms,
        )
        .await?;
        let response = AppProviderResponseV1 {
            schema_version: 1,
            request_id,
            output_schema_digest: descriptor.output_schema_digest,
            revision: envelope.expected_revision.clone(),
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

struct DurableCommandIntent<'a> {
    originating_app_operation_id: &'a str,
    operation_id: &'a str,
    key: &'a str,
    digest: &'a Sha256Digest,
    envelope: &'a AppInvocationEnvelopeV1,
}

fn append_command_intent(
    event_store: &RuntimeEventStore,
    stream_id: &str,
    registration: &CoreBridgeRegistration,
    intent: &DurableCommandIntent<'_>,
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
                "originating_app_operation_id": intent.originating_app_operation_id,
                "operation_id": intent.operation_id,
                "idempotency_key": intent.key,
                "payload_digest": intent.digest.0,
                "request_id": intent.envelope.request_id,
                "principal_subject": intent.envelope.principal.subject,
                "workspace_id": intent.envelope.principal.workspace_id,
            }),
        },
        idempotency_key: Some("intent".to_owned()),
        schema_version: 1,
    };
    event_store
        .append_batch_if_revision(
            stream_id.to_owned(),
            revision,
            format!("core-bridge-intent:{stream_id}"),
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
            format!("core-bridge-receipt:{stream_id}"),
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
    app_state: Arc<OnceLock<Arc<AppState>>>,
    app_id: String,
    envelope: &AppInvocationEnvelopeV1,
    operation_id: String,
    payload: serde_json::Value,
    timeout_ms: u64,
) -> Result<serde_json::Value, BridgeFailure> {
    if timeout_ms == 0 {
        return Err(BridgeFailure::deadline());
    }
    if operation_id.starts_with("core.matrix.") {
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
    } else if core_platform_operations::supports(&operation_id) {
        let state = app_state.get().cloned().ok_or_else(|| {
            BridgeFailure::new(
                AppErrorCodeV1::DependencyUnavailable,
                "CoreBridge Gateway dependencies are not ready",
                true,
            )
        })?;
        tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            core_platform_operations::dispatch(&state, envelope, &app_id, &operation_id, &payload),
        )
        .await
        .map_err(|_| BridgeFailure::deadline())?
        .map_err(|detail| {
            if detail.contains("revision conflict") {
                BridgeFailure::new(AppErrorCodeV1::RevisionConflict, detail, false)
            } else if detail.contains("verified Gateway request")
                || detail.contains("signed APP producer")
            {
                BridgeFailure::new(AppErrorCodeV1::OperationNotGranted, detail, false)
            } else {
                BridgeFailure::invalid(detail)
            }
        })
    } else {
        Err(BridgeFailure::invalid(format!(
            "unknown Core operation `{operation_id}`"
        )))
    }
}

fn validate_manifest_profile(
    manifest: &AppManifestV1,
    envelope: &AppInvocationEnvelopeV1,
    descriptor: &OperationDescriptorV1,
    originating_app_operation_id: &str,
) -> Result<(), BridgeFailure> {
    core_matrix_catalog::validate_projected_capabilities(manifest, descriptor).map_err(|_| {
        BridgeFailure::new(
            AppErrorCodeV1::OperationNotGranted,
            "Core operation capabilities differ from Gateway authority",
            false,
        )
    })?;
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
    let app_capability_prefix = format!("{}.", manifest.app_id.0);
    let requirement = manifest
        .core_bridge_requirements
        .binary_search_by(|candidate| {
            (
                candidate.app_operation_id.as_str(),
                candidate.core_operation_id.as_str(),
            )
                .cmp(&(
                    originating_app_operation_id,
                    descriptor.operation_id.as_str(),
                ))
        })
        .ok()
        .map(|index| &manifest.core_bridge_requirements[index])
        .ok_or_else(|| {
            BridgeFailure::new(
                AppErrorCodeV1::OperationNotGranted,
                "originating APP operation has no exact signed Core edge",
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
    if allowed
        .iter()
        .any(|capability| !capability.starts_with(&app_capability_prefix))
    {
        return Err(BridgeFailure::new(
            AppErrorCodeV1::OperationNotGranted,
            "signed APP authorization profile contains a non-APP capability",
            false,
        ));
    }
    allowed.extend(
        descriptor
            .required_capabilities
            .iter()
            .filter(|capability| !capability.starts_with(&app_capability_prefix))
            .cloned(),
    );
    allowed.sort();
    allowed.dedup();
    if requirement
        .required_app_capabilities
        .iter()
        .any(|capability| allowed.binary_search(capability).is_err())
    {
        return Err(BridgeFailure::new(
            AppErrorCodeV1::OperationNotGranted,
            "selected APP profile does not grant every capability on the signed edge",
            false,
        ));
    }
    if envelope
        .principal
        .granted_capabilities
        .iter()
        .any(|capability| allowed.binary_search(capability).is_err())
        || descriptor.required_capabilities.iter().any(|capability| {
            envelope
                .principal
                .granted_capabilities
                .binary_search(capability)
                .is_err()
        })
        || requirement
            .required_app_capabilities
            .iter()
            .any(|capability| {
                envelope
                    .principal
                    .granted_capabilities
                    .binary_search(capability)
                    .is_err()
            })
    {
        return Err(BridgeFailure::new(
            AppErrorCodeV1::OperationNotGranted,
            "principal capabilities do not exactly satisfy the signed APP profile and Core authority",
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
    originating_app_operation_id: &str,
    operation_id: &str,
    expected_revision: Option<&str>,
    payload: &serde_json::Value,
) -> Result<Sha256Digest, BridgeFailure> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "domain": "cowd.core-bridge.command/v1",
        "app_id": registration.app_id,
        "generation": registration.generation,
        "originating_app_operation_id": originating_app_operation_id,
        "operation_id": operation_id,
        "expected_revision": expected_revision,
        "payload": payload,
    }))
    .map_err(|error| BridgeFailure::internal(error.to_string()))?;
    Ok(Sha256Digest(format!("sha256:{:x}", Sha256::digest(bytes))))
}

fn validation_failure(detail: String) -> BridgeFailure {
    if detail.contains("CALL_CYCLE_DETECTED") {
        BridgeFailure::new(AppErrorCodeV1::CallCycleDetected, detail, false)
    } else if detail.contains("capabilit")
        || detail.contains("signed Core edge")
        || detail.contains("core_bridge_edge")
    {
        BridgeFailure::new(AppErrorCodeV1::OperationNotGranted, detail, false)
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

    use cowd_app_host::catalog::{AdmittedApp, AppCatalogSnapshot, EffectiveAppPolicy};
    use cowd_app_protocol::{
        derive_channel_token_v1, format_channel_authorization_v1, AppPresentationV1,
        AppResultContractV1, AppSurfacesV1, AuthorizationProfileV1, BootstrapSecretV1,
        BundleIntegrityV1, BundleSignatureV1, ChannelPurposeV1, CoreBridgeRequirementV1,
        DelegationKindV1, ExecutionContextV1, FilesystemPolicyV1, IntegrityAlgorithmV1,
        NetworkPolicyV1, ProtocolRangeV1, SandboxProfileV1, SignatureAlgorithmV1,
    };
    use http_body_util::BodyExt;
    use managed_worker_runtime::{GenerationFence, ManagedH2Channel, PeerCredentialPolicy};
    use matrix_repository::MatrixSqliteRepository;
    use serde_json::Value;

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
            operation_catalog_digest: cowd_app_protocol::app_operation_catalog_digest_v1(
                &AppId("fixture".to_owned()),
                &[],
            )
            .expect("empty operation catalog digest"),
            capabilities: vec!["fixture.read".to_owned()],
            authorization_profiles: vec![AuthorizationProfileV1 {
                profile_id: "operator".to_owned(),
                display_name: "Operator".to_owned(),
                capabilities: vec!["fixture.read".to_owned()],
                surface_capabilities: BTreeMap::new(),
                is_default: true,
            }],
            core_bridge_requirements: vec![CoreBridgeRequirementV1 {
                app_operation_id: "fixture.health".to_owned(),
                core_operation_id: definition.descriptor.operation_id.clone(),
                accepted_input_schema_digest: definition.descriptor.input_schema_digest.clone(),
                accepted_output_schema_digest: definition.descriptor.output_schema_digest.clone(),
                required_app_capabilities: vec!["fixture.read".to_owned()],
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

    fn fixture_business_manifest(
        definitions: &[&core_matrix_catalog::CoreMatrixOperationDefinition],
    ) -> AppManifestV1 {
        let mut manifest = fixture_manifest(definitions[0]);
        manifest.capabilities = vec!["fixture.invoke".to_owned(), "fixture.review".to_owned()];
        manifest.authorization_profiles[0].capabilities = manifest.capabilities.clone();
        manifest.core_bridge_requirements = definitions
            .iter()
            .enumerate()
            .map(|(index, definition)| CoreBridgeRequirementV1 {
                app_operation_id: format!("fixture.operation.{index:02}"),
                core_operation_id: definition.descriptor.operation_id.clone(),
                accepted_input_schema_digest: definition.descriptor.input_schema_digest.clone(),
                accepted_output_schema_digest: definition.descriptor.output_schema_digest.clone(),
                required_app_capabilities: vec!["fixture.invoke".to_owned()],
                kind: definition.descriptor.kind,
                streaming: definition.descriptor.streaming,
            })
            .collect();
        manifest.core_bridge_requirements.sort_by(|left, right| {
            (&left.app_operation_id, &left.core_operation_id)
                .cmp(&(&right.app_operation_id, &right.core_operation_id))
        });
        manifest.presentation = Some(AppPresentationV1 {
            result_shape_revision: 1,
            result_contracts: vec![AppResultContractV1 {
                contract_id: "fixture.result".to_owned(),
                schema_id: "fixture.result.schema".to_owned(),
                schema_version: 1,
                schema_digest: Sha256Digest(
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                ),
                max_bytes: 64 * 1024,
            }],
            tui_views: Vec::new(),
            core_navigation_kinds: Vec::new(),
        });
        manifest
            .bind_canonical_signed_digest()
            .expect("business manifest digest");
        manifest
    }

    fn admitted_test_platform(
        mut manifest: AppManifestV1,
    ) -> (
        tempfile::TempDir,
        Arc<crate::app_platform::GatewayAppPlatform>,
        Arc<AppManifestV1>,
    ) {
        let root = tempfile::tempdir().expect("APP root");
        let bundle = root.path().join("fixture");
        let executable = bundle.join("bin/worker");
        manifest
            .bind_canonical_signed_digest()
            .expect("signed manifest digest");
        let generation = GenerationId(format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(&manifest).expect("manifest bytes"))
        ));
        let admitted_manifest = Arc::new(manifest.clone());
        let snapshot = AppCatalogSnapshot::from_admitted_for_tests(vec![AdmittedApp {
            manifest,
            bundle_root: bundle,
            executable,
            web_root: None,
            generation,
            policy: EffectiveAppPolicy::default(),
        }])
        .expect("admitted test snapshot");
        let platform = crate::app_platform::GatewayAppPlatform::for_test_catalog(snapshot);
        (root, platform, admitted_manifest)
    }

    fn fixture_envelope(
        descriptor: &OperationDescriptorV1,
        granted_capabilities: Vec<String>,
    ) -> AppInvocationEnvelopeV1 {
        let now = now_unix_ms();
        AppInvocationEnvelopeV1 {
            schema_version: 1,
            operation_id: descriptor.operation_id.clone(),
            request_id: "request-profile-validation".to_owned(),
            correlation_id: "correlation-profile-validation".to_owned(),
            causation_id: None,
            deadline_unix_ms: now + 5_000,
            idempotency_key: None,
            expected_revision: None,
            call_chain: vec!["app:fixture".to_owned()],
            max_hops: 4,
            input_schema_digest: descriptor.input_schema_digest.clone(),
            principal: cowd_app_protocol::PrincipalContextV1 {
                subject: "fixture-worker".to_owned(),
                tenant_id: "deployment-tenant".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                delegation: DelegationKindV1::Service,
                grant_id: "grant-profile-validation".to_owned(),
                authorization_profile_id: "operator".to_owned(),
                authorization_revision: 1,
                granted_capabilities,
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
        }
    }

    #[test]
    fn manifest_profile_requires_all_core_and_app_capabilities_and_rejects_excess() {
        let definition = core_matrix_catalog::definitions()
            .expect("definitions")
            .into_iter()
            .find(|definition| definition.short_id == "health")
            .expect("health");
        let manifest = fixture_manifest(&definition);
        let generation = GenerationId(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        );
        let catalog = core_matrix_catalog::projected_catalog(&manifest, &generation)
            .expect("projected catalog");
        let descriptor = &catalog.operations[0];

        let valid = fixture_envelope(
            descriptor,
            vec!["core.matrix.read".to_owned(), "fixture.read".to_owned()],
        );
        validate_manifest_profile(&manifest, &valid, descriptor, "fixture.health")
            .expect("Core and signed APP capabilities are jointly authorized");

        for (label, capabilities) in [
            ("missing Core capability", vec!["fixture.read".to_owned()]),
            (
                "missing APP capability",
                vec!["core.matrix.read".to_owned()],
            ),
            (
                "forged extra Core capability",
                vec![
                    "core.matrix.read".to_owned(),
                    "core.matrix.write".to_owned(),
                    "fixture.read".to_owned(),
                ],
            ),
            (
                "unsigned extra APP capability",
                vec![
                    "core.matrix.read".to_owned(),
                    "fixture.read".to_owned(),
                    "fixture.write".to_owned(),
                ],
            ),
        ] {
            let envelope = fixture_envelope(descriptor, capabilities);
            let failure =
                validate_manifest_profile(&manifest, &envelope, descriptor, "fixture.health")
                    .expect_err(label);
            assert_eq!(failure.code, AppErrorCodeV1::OperationNotGranted, "{label}");
        }
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

    fn bind_business_principal(
        state: &AppState,
        envelope: &AppInvocationEnvelopeV1,
        is_human: bool,
    ) {
        let verified = runtime::VerifiedPrincipal::from_test_claims(
            harness_contract::security::PrincipalClaims {
                principal_id: envelope.principal.subject.clone(),
                tenant_id: envelope.principal.tenant_id.clone(),
                grant_id: envelope.principal.grant_id.clone(),
                kind: if is_human {
                    harness_contract::security::PrincipalKind::Human
                } else {
                    harness_contract::security::PrincipalKind::Service
                },
                scopes: envelope.principal.granted_scopes.clone(),
                capabilities: envelope.principal.granted_capabilities.clone(),
                assurance: if is_human {
                    harness_contract::security::PrincipalAssurance::HumanInteractive
                } else {
                    harness_contract::security::PrincipalAssurance::Normal
                },
                issuer: "test.core-bridge".to_owned(),
                issued_at_ms: now_unix_ms(),
                expires_at_ms: envelope.principal.expires_at_unix_ms,
                credential_fingerprint: "business-fixture".to_owned(),
                credential_epoch: envelope.principal.credential_epoch,
                profile_revision: envelope.principal.authorization_revision,
                app_profiles: BTreeMap::new(),
            },
        );
        state
            .services
            .core_platform_bindings
            .bind_request_principal(
                &verified,
                &envelope.request_id,
                &envelope.principal.workspace_id,
                &envelope.execution.surface,
                "app:fixture".to_owned(),
            );
    }

    async fn send_uds_bridge_invocation(
        channel: &ManagedH2Channel,
        generation: &GenerationId,
        authorization: &str,
        cancellation: &CancellationToken,
        origin: String,
        envelope: &AppInvocationEnvelopeV1,
    ) -> (StatusCode, Bytes) {
        let bridge = CoreBridgeInvocationV1 {
            schema_version: 1,
            originating_app_operation_id: origin,
            invocation: envelope.clone(),
        };
        let mut request = base_request(
            Method::POST,
            &format!(
                "{CORE_INVOKE_PREFIX}{}{CORE_INVOKE_SUFFIX}",
                envelope.operation_id
            ),
            generation,
            authorization,
            Bytes::from(serde_json::to_vec(&bridge).expect("bridge request")),
        );
        let mut headers = vec![
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
        ];
        if let Some(session_id) = &envelope.execution.session_id {
            headers.push((HEADER_SESSION_ID_V1, session_id.clone()));
        }
        if let Some(turn_id) = &envelope.execution.turn_id {
            headers.push((HEADER_TURN_ID_V1, turn_id.clone()));
        }
        if let Some(task_id) = &envelope.execution.task_id {
            headers.push((HEADER_TASK_ID_V1, task_id.clone()));
        }
        for (name, value) in headers {
            request
                .headers_mut()
                .insert(name, value.parse().expect("header"));
        }
        let response = channel
            .send(
                &generation.0,
                request,
                Duration::from_secs(10),
                cancellation,
            )
            .await
            .expect("CoreBridge response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        (status, body)
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
                granted_capabilities: vec![
                    "core.matrix.read".to_owned(),
                    "fixture.read".to_owned(),
                ],
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
        let bridge_invocation = CoreBridgeInvocationV1 {
            schema_version: 1,
            originating_app_operation_id: "fixture.health".to_owned(),
            invocation: envelope.clone(),
        };
        let mut request = base_request(
            Method::POST,
            &format!(
                "{CORE_INVOKE_PREFIX}{}{CORE_INVOKE_SUFFIX}",
                envelope.operation_id
            ),
            &generation,
            &authorization,
            Bytes::from(serde_json::to_vec(&bridge_invocation).expect("bridge invocation")),
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

    #[tokio::test]
    async fn real_uds_h2_dispatches_all_fourteen_core_business_operations() {
        let authority = core_matrix_catalog::definitions().expect("definitions");
        let business_ids = core_platform_operations::BUSINESS_OPERATION_IDS
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let mut definitions = authority
            .iter()
            .filter(|definition| business_ids.contains(definition.descriptor.operation_id.as_str()))
            .collect::<Vec<_>>();
        definitions.sort_by(|left, right| {
            left.descriptor
                .operation_id
                .cmp(&right.descriptor.operation_id)
        });
        assert_eq!(definitions.len(), 14);
        let manifest = fixture_business_manifest(&definitions);
        let (_bundle, app_platform, manifest) = admitted_test_platform(manifest);
        let generation = GenerationId(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        );
        let secret = BootstrapSecretV1::from_bytes(&[0x43; 32]).expect("secret");
        let token = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::CoreBridge,
            &manifest.app_id,
            &generation,
            std::process::id(),
            "business-worker",
        )
        .expect("Core token");
        let authorization = format_channel_authorization_v1(&token);
        let registry = Arc::new(CoreBridgeRegistry::default());
        registry.register(
            manifest.app_id.clone(),
            generation.clone(),
            std::process::id(),
            unsafe { libc::geteuid() },
            Arc::clone(&manifest),
            token,
        );
        let root = tempfile::tempdir().expect("socket root");
        let socket = root.path().join("core-business.sock");
        let server = CoreBridgeServer::start(
            socket.clone(),
            Arc::clone(&registry),
            Arc::new(MatrixSqliteRepository::in_memory().expect("matrix")),
            Arc::new(RuntimeEventStore::open_in_memory().expect("events")),
        )
        .await
        .expect("server");
        let state = crate::api_routes::tests::test_state_with_app_platform(app_platform);
        crate::api_routes::tests::publish_test_session_policy(&state.services, "session-uds");
        server
            .bind_app_state(Arc::clone(&state))
            .expect("bind Gateway state");
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

        let task_id = format!("uds-goal-{}", uuid::Uuid::new_v4());
        let structured_task_id = format!("uds-structured-{}", uuid::Uuid::new_v4());
        let approval_id = format!("uds-approval-{}", uuid::Uuid::new_v4());
        let evidence = serde_json::to_value(matrix_core::MatrixEvidencePacket::new(
            "UDS business operation evidence",
        ))
        .expect("evidence");
        let fixtures = vec![
            (core_platform_operations::RUNTIME_START_GOAL_OPERATION_ID, serde_json::json!({"task_id":task_id,"mission":{"selector":"workspace_default"},"source_session_id":"session-uds","source_turn_id":"turn-uds","objective":"start a Core-owned goal","preemptive":false})),
            (core_platform_operations::RUNTIME_START_STRUCTURED_TASK_OPERATION_ID, serde_json::json!({"task_id":structured_task_id,"mission":{"selector":"workspace_default"},"source_session_id":"session-uds","source_turn_id":"turn-uds","objective":"start a Core-owned structured task","result_contract_id":"fixture.result","instruction":"return bounded JSON","input":{}})),
            (core_platform_operations::RUNTIME_CANCEL_STRUCTURED_TASK_OPERATION_ID, serde_json::json!({"task_id":structured_task_id})),
            (core_platform_operations::APPROVAL_SUBMIT_OPERATION_ID, serde_json::json!({"approval_id":approval_id,"app_id":"fixture","correlation_schema":"fixture.review.v1","decision_capability":"fixture.review","resource_ref":"resource://uds","review_ref":"review://uds","action":"approve","summary":"review UDS effect","risk":"low","evidence_refs":[],"timeout_policy":"pending"})),
            (core_platform_operations::APPROVAL_DECIDE_OPERATION_ID, serde_json::json!({"approval_id":approval_id,"app_id":"fixture","correlation_schema":"fixture.review.v1","review_ref":"review://uds","action":"approve","scope":"request","evidence_digest":"sha256:uds","approved":true,"decision":"approve","reason":"verified"})),
            (core_platform_operations::CROSS_PLANE_DISPATCH_OPERATION_ID, serde_json::json!({"mode":"dry_run","idempotency_key":"uds-cross","requested_capability":"message.send","risk":"low","data_classification":"internal","identity_trust":"verified","dispatch":{"platform":"fixture","operation":"send"}})),
            (core_platform_operations::CONNECTOR_SURFACE_DISPATCH_BATCH_OPERATION_ID, serde_json::json!({"deliveries":[{"surface":"fixture","recipient":"user-uds","thread":null,"text":"hello","idempotency_key":"uds-delivery","metadata":{}}]})),
            (core_platform_operations::WORK_CONTEXT_TASK_EXISTS_OPERATION_ID, serde_json::json!({"task_ref":format!("task://{task_id}")})),
            (core_platform_operations::WORK_CONTEXT_INSPECT_TASK_TERMINAL_OPERATION_ID, serde_json::json!({"task_ref":"task://missing","workflow_node_id":null})),
            (core_platform_operations::WORK_CONTEXT_RECORD_TASK_TERMINAL_OPERATION_ID, serde_json::json!({"task_ref":"task://missing","workflow_node_id":null,"correlation_id":"uds-correlation"})),
            (core_platform_operations::WORK_CONTEXT_STRUCTURED_EVIDENCE_ITEM_OPERATION_ID, serde_json::json!({"packet":evidence})),
            (core_platform_operations::WORK_CONTEXT_INSPECT_STRUCTURED_TASK_RESULT_OPERATION_ID, serde_json::json!({"task_id":structured_task_id})),
            (core_platform_operations::WORK_CONTEXT_APPEND_APPLICATION_EXECUTION_SUMMARY_OPERATION_ID, serde_json::json!({"schema_version":1,"session_id":"session-uds","summary":{"schema_version":1,"summary_id":"uds-summary","kind":"task","status":"succeeded","title":"UDS complete","summary":"All effects reached Core authority.","domain":"test","refs":[],"evidence_refs":[],"metric_refs":[],"counters":[],"occurred_at_ms":1}})),
            (core_platform_operations::PLATFORM_GOVERNANCE_SNAPSHOT_OPERATION_ID, serde_json::json!({})),
        ];
        assert_eq!(fixtures.len(), 14);
        let mut query_seed = None;
        let mut command_seed = None;
        let mut cancellation_seed = None;

        for (index, (operation_id, payload)) in fixtures.into_iter().enumerate() {
            let definition = definitions
                .iter()
                .find(|definition| definition.descriptor.operation_id == operation_id)
                .expect("operation definition");
            let origin = manifest
                .core_bridge_requirements
                .iter()
                .find(|edge| edge.core_operation_id == operation_id)
                .expect("signed edge")
                .app_operation_id
                .clone();
            let is_human = operation_id == core_platform_operations::APPROVAL_DECIDE_OPERATION_ID;
            let now = now_unix_ms();
            let mut capabilities = definition.descriptor.required_capabilities.clone();
            capabilities.push("fixture.invoke".to_owned());
            if matches!(
                operation_id,
                core_platform_operations::APPROVAL_SUBMIT_OPERATION_ID
                    | core_platform_operations::APPROVAL_DECIDE_OPERATION_ID
            ) {
                capabilities.push("fixture.review".to_owned());
            }
            capabilities.sort();
            capabilities.dedup();
            let request_id = format!("business-request-{index:02}");
            let idempotency_key =
                (definition.descriptor.kind == OperationKindV1::Command).then(|| {
                    if operation_id == core_platform_operations::CROSS_PLANE_DISPATCH_OPERATION_ID {
                        "uds-cross".to_owned()
                    } else {
                        format!("business-idempotency-{index:02}")
                    }
                });
            let envelope = AppInvocationEnvelopeV1 {
                schema_version: 1,
                operation_id: operation_id.to_owned(),
                request_id: request_id.clone(),
                correlation_id: format!("business-correlation-{index:02}"),
                causation_id: None,
                deadline_unix_ms: now + 30_000,
                idempotency_key,
                expected_revision: None,
                call_chain: vec!["app:fixture".to_owned()],
                max_hops: 4,
                input_schema_digest: definition.descriptor.input_schema_digest.clone(),
                principal: cowd_app_protocol::PrincipalContextV1 {
                    subject: if is_human {
                        "local-human".to_owned()
                    } else {
                        "fixture-service".to_owned()
                    },
                    tenant_id: "deployment-tenant".to_owned(),
                    workspace_id: "workspace-1".to_owned(),
                    delegation: if is_human {
                        DelegationKindV1::User
                    } else {
                        DelegationKindV1::Service
                    },
                    grant_id: format!("business-grant-{index:02}"),
                    authorization_profile_id: "operator".to_owned(),
                    authorization_revision: 1,
                    granted_capabilities: capabilities.clone(),
                    granted_scopes: Vec::new(),
                    credential_epoch: 1,
                    expires_at_unix_ms: Some(now + 30_000),
                },
                execution: ExecutionContextV1 {
                    surface: "worker".to_owned(),
                    session_id: Some("session-uds".to_owned()),
                    turn_id: Some("turn-uds".to_owned()),
                    task_id: None,
                },
                payload,
            };
            bind_business_principal(&state, &envelope, is_human);
            if operation_id == core_platform_operations::PLATFORM_GOVERNANCE_SNAPSHOT_OPERATION_ID {
                query_seed = Some((origin.clone(), envelope.clone()));
            }
            if operation_id == core_platform_operations::CROSS_PLANE_DISPATCH_OPERATION_ID {
                command_seed = Some((origin.clone(), envelope.clone()));
            }
            if operation_id == core_platform_operations::RUNTIME_CANCEL_STRUCTURED_TASK_OPERATION_ID
            {
                cancellation_seed = Some((origin.clone(), envelope.clone()));
            }
            let (status, body) = send_uds_bridge_invocation(
                &channel,
                &generation,
                &authorization,
                &cancellation,
                origin,
                &envelope,
            )
            .await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{operation_id} failed: {}",
                String::from_utf8_lossy(&body)
            );
        }

        let (query_origin, query) = query_seed.expect("query seed");
        let (command_origin, command) = command_seed.expect("command seed");
        let (cancellation_origin, mut revision_conflict) =
            cancellation_seed.expect("cancellation seed");

        revision_conflict.request_id = "negative-revision-conflict".to_owned();
        revision_conflict.correlation_id = "negative-revision-conflict".to_owned();
        revision_conflict.idempotency_key = Some("negative-revision-conflict".to_owned());
        revision_conflict.expected_revision = Some("18446744073709551615".to_owned());
        bind_business_principal(&state, &revision_conflict, false);
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            cancellation_origin,
            &revision_conflict,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "revision conflict failed: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(String::from_utf8_lossy(&body).contains("REVISION_CONFLICT"));

        let mut missing_capability = query.clone();
        missing_capability.request_id = "negative-missing-capability".to_owned();
        missing_capability.correlation_id = "negative-missing-capability".to_owned();
        missing_capability
            .principal
            .granted_capabilities
            .retain(|capability| capability == "fixture.invoke");
        bind_business_principal(&state, &missing_capability, false);
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            query_origin.clone(),
            &missing_capability,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(&body).contains("OPERATION_NOT_GRANTED"));

        let mut wrong_schema = query.clone();
        wrong_schema.request_id = "negative-wrong-schema".to_owned();
        wrong_schema.correlation_id = "negative-wrong-schema".to_owned();
        wrong_schema.input_schema_digest = Sha256Digest(
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        );
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            query_origin.clone(),
            &wrong_schema,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(String::from_utf8_lossy(&body).contains("INVALID_REQUEST"));

        let mut expired = query.clone();
        expired.request_id = "negative-expired".to_owned();
        expired.correlation_id = "negative-expired".to_owned();
        expired.deadline_unix_ms = now_unix_ms().saturating_sub(1);
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            query_origin.clone(),
            &expired,
        )
        .await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(String::from_utf8_lossy(&body).contains("DEADLINE_EXCEEDED"));

        let mut cycle = query.clone();
        cycle.request_id = "negative-cycle".to_owned();
        cycle.correlation_id = "negative-cycle".to_owned();
        cycle.call_chain.push(CORE_AUTHORITY.to_owned());
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            query_origin.clone(),
            &cycle,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(String::from_utf8_lossy(&body).contains("CALL_CYCLE_DETECTED"));

        let mut wrong_edge = query.clone();
        wrong_edge.request_id = "negative-wrong-edge".to_owned();
        wrong_edge.correlation_id = "negative-wrong-edge".to_owned();
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            command_origin.clone(),
            &wrong_edge,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(String::from_utf8_lossy(&body).contains("OPERATION_NOT_GRANTED"));

        let mut idempotency_conflict = command;
        idempotency_conflict.request_id = "negative-idempotency-conflict".to_owned();
        idempotency_conflict.correlation_id = "negative-idempotency-conflict".to_owned();
        idempotency_conflict.payload["requested_capability"] =
            Value::String("message.changed".to_owned());
        bind_business_principal(&state, &idempotency_conflict, false);
        let (status, body) = send_uds_bridge_invocation(
            &channel,
            &generation,
            &authorization,
            &cancellation,
            command_origin,
            &idempotency_conflict,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(String::from_utf8_lossy(&body).contains("IDEMPOTENCY_CONFLICT"));

        server.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn platform_dispatch_fails_closed_until_gateway_state_is_bound() {
        let definition = core_matrix_catalog::definitions()
            .expect("definitions")
            .into_iter()
            .find(|definition| {
                definition.descriptor.operation_id
                    == core_platform_operations::ACTION_PLAN_OPERATION_ID
            })
            .expect("action plan operation");
        let now = now_unix_ms();
        let envelope = AppInvocationEnvelopeV1 {
            schema_version: 1,
            operation_id: definition.descriptor.operation_id.clone(),
            request_id: "request-platform-before-bind".to_owned(),
            correlation_id: "correlation-platform-before-bind".to_owned(),
            causation_id: None,
            deadline_unix_ms: now + 5_000,
            idempotency_key: None,
            expected_revision: None,
            call_chain: vec!["app:fixture".to_owned()],
            max_hops: 4,
            input_schema_digest: definition.descriptor.input_schema_digest,
            principal: cowd_app_protocol::PrincipalContextV1 {
                subject: "fixture-worker".to_owned(),
                tenant_id: "deployment-tenant".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                delegation: DelegationKindV1::Service,
                grant_id: "grant-1".to_owned(),
                authorization_profile_id: "operator".to_owned(),
                authorization_revision: 1,
                granted_capabilities: vec![
                    "core.cross_plane.read".to_owned(),
                    "fixture.read".to_owned(),
                ],
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
        let failure = dispatch_with_deadline(
            Arc::new(MatrixSqliteRepository::in_memory().expect("matrix")),
            Arc::new(OnceLock::new()),
            "fixture".to_owned(),
            &envelope,
            envelope.operation_id.clone(),
            envelope.payload.clone(),
            1_000,
        )
        .await
        .expect_err("unbound Gateway state must fail closed");
        assert_eq!(failure.code, AppErrorCodeV1::DependencyUnavailable);
        assert!(failure.detail.contains("not ready"));
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
                granted_capabilities: vec![
                    "core.matrix.write".to_owned(),
                    "fixture.write".to_owned(),
                ],
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
            &DurableCommandIntent {
                originating_app_operation_id: "fixture.metric.recompute",
                operation_id: &envelope.operation_id,
                key: "idem-1",
                digest: &digest,
                envelope: &envelope,
            },
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
        append_command_receipt(&store, &registration, "idem-1", &receipt).expect("receipt");
        let DurableCommandState::Completed(replayed) =
            durable_command_state(&store, &registration, "idem-1", &digest).expect("completed")
        else {
            panic!("expected completed command");
        };
        assert_eq!(replayed, receipt);

        let DurableCommandState::New { stream_id } =
            durable_command_state(&store, &registration, "idem-2", &digest).expect("second new")
        else {
            panic!("expected second new command");
        };
        append_command_intent(
            &store,
            &stream_id,
            &registration,
            &DurableCommandIntent {
                originating_app_operation_id: "fixture.metric.recompute",
                operation_id: &envelope.operation_id,
                key: "idem-2",
                digest: &digest,
                envelope: &envelope,
            },
        )
        .expect("same payload on another idempotency key must not collide");
        let second_receipt = DurableReceiptV1 {
            request_id: "request-2".to_owned(),
            receipt_id: "core-matrix:idem-2".to_owned(),
            idempotency_key: "idem-2".to_owned(),
            ..receipt.clone()
        };
        append_command_receipt(&store, &registration, "idem-2", &second_receipt)
            .expect("second receipt must not collide");
        assert!(matches!(
            durable_command_state(&store, &registration, "idem-2", &digest)
                .expect("second completed"),
            DurableCommandState::Completed(completed) if completed == second_receipt
        ));
    }
}
