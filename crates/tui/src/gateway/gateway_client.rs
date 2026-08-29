use std::collections::BTreeMap;
use std::fmt;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

#[path = "client/domain.rs"]
mod domain;
#[path = "client/live.rs"]
mod live;
#[cfg(test)]
use live::{deliver_tui_live_envelope, deliver_tui_live_resync, refresh_tui_live_source_selector};

use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    events::CowdEventSender,
    protocol::{
        GatewayEventCorrelation, GatewaySessionEvent, SessionMessagesPage,
        SessionStreamConnectionState,
    },
    CowdEvent,
};
use cowd_app_protocol::{
    app_operation_catalog_digest_v1, app_tui_view_action_request_schema_digest_v1,
    app_tui_view_action_response_schema_digest_v1, app_tui_view_open_request_schema_digest_v1,
    app_tui_view_open_response_schema_digest_v1, app_tui_view_patch_schema_digest_v1,
    app_tui_view_stream_request_schema_digest_v1, AppCatalogEntryV1, AppCatalogV1, AppManifestV1,
    AppStreamFrameV1, AppTuiViewStreamRequestV1, OperationDescriptorV1, OperationKindV1,
    ProtocolValidate, MAX_STREAM_FRAME_BYTES,
};

use crate::app_surface_host::AppSurfaceEvent;

const GATEWAY_READY_RETRY_ATTEMPTS: usize = 20;
const GATEWAY_READY_RETRY_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_GATEWAY_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GATEWAY_SSE_IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const DEFAULT_GATEWAY_URL: &str = "http://127.0.0.1:8642";
const TUI_SURFACE_ID: &str = "tui";

fn authorize_tui_request(
    request: reqwest::RequestBuilder,
    auth_token: Option<&str>,
    observer_id: &str,
) -> reqwest::RequestBuilder {
    let request = request
        .header("x-cowd-surface-id", TUI_SURFACE_ID)
        .header("x-cowd-observer-id", observer_id);
    if let Some(token) = auth_token.filter(|token| !token.trim().is_empty()) {
        request.bearer_auth(token.trim())
    } else {
        request
    }
}

#[derive(Debug, Clone)]
pub struct GatewayApiClient {
    base_url: String,
    auth_token: Option<String>,
    observer_id: String,
    client: reqwest::Client,
    /// Long-lived streams cannot share the ordinary 15-second total request
    /// deadline. A per-read idle watchdog still detects missing heartbeats.
    sse_client: reqwest::Client,
    live: Arc<TuiLiveMultiplexer>,
    pending_cancellations: Arc<Mutex<BTreeMap<String, (String, u64)>>>,
}

#[derive(Debug)]
struct TuiLiveMultiplexer {
    transport: LiveTransportConfig,
    commands: Mutex<Option<mpsc::UnboundedSender<LiveCommand>>>,
}

#[derive(Debug, Clone)]
struct LiveTransportConfig {
    base_url: String,
    auth_token: Option<String>,
    observer_id: String,
    client: reqwest::Client,
    sse_client: reqwest::Client,
}

impl LiveTransportConfig {
    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        authorize_tui_request(request, self.auth_token.as_deref(), &self.observer_id)
    }
}

#[derive(Debug)]
enum LiveCommand {
    Add {
        subscriber_id: String,
        source: harness_contract::live::LiveSourceSelector,
        tx: mpsc::Sender<harness_contract::live::LiveEnvelope>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Remove {
        subscriber_id: String,
        source_key: String,
    },
}

struct TuiLiveLease {
    subscriber_id: String,
    source_key: String,
    commands: mpsc::UnboundedSender<LiveCommand>,
    rx: mpsc::Receiver<harness_contract::live::LiveEnvelope>,
}

impl TuiLiveLease {
    async fn recv(&mut self) -> Option<harness_contract::live::LiveEnvelope> {
        self.rx.recv().await
    }
}

impl Drop for TuiLiveLease {
    fn drop(&mut self) {
        let _ = self.commands.send(LiveCommand::Remove {
            subscriber_id: self.subscriber_id.clone(),
            source_key: self.source_key.clone(),
        });
    }
}

#[derive(Debug)]
enum LiveTransportEvent {
    Envelope(harness_contract::live::LiveEnvelope),
    Interrupted(String),
    Recreate(String),
}

#[derive(Debug)]
struct LiveSubscriber {
    selector: harness_contract::live::LiveSourceSelector,
    tx: mpsc::Sender<harness_contract::live::LiveEnvelope>,
}

#[derive(Debug)]
struct LiveSourceState {
    selector: harness_contract::live::LiveSourceSelector,
    subscribers: BTreeMap<String, LiveSubscriber>,
    pending_previews: BTreeMap<String, harness_contract::live::LiveEnvelope>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionStreamProgress {
    pub commit_cursor: Option<u64>,
    pub next_message_sequence: usize,
}

/// Sanitized failure delivered to an external APP terminal panel. Credentials
/// and host implementation details never cross this boundary.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AppTransportFailure {
    pub status: Option<u16>,
    pub body: Option<serde_json::Value>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub(crate) struct AppViewStreamRequest {
    pub app_id: String,
    pub view_id: String,
    pub request: AppTuiViewStreamRequestV1,
    pub session_id: String,
    pub authority_generation: u64,
}

/// Gateway-owned REST composition of the catalog projection, signed manifest,
/// and the live worker catalog. Semantic APP contracts remain shared protocol
/// types and are validated as one admission fact before the TUI uses them.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GatewayAppDetailResponseV1 {
    pub schema_version: u16,
    pub entry: AppCatalogEntryV1,
    pub manifest: AppManifestV1,
    pub operations: Vec<OperationDescriptorV1>,
}

impl GatewayAppDetailResponseV1 {
    pub(crate) fn validate_against_catalog_entry(
        &self,
        expected: &AppCatalogEntryV1,
    ) -> Result<(), GatewayApiError> {
        if self.schema_version != 1 {
            return Err(GatewayApiError::Contract(
                "invalid APP detail schema version".to_owned(),
            ));
        }
        self.entry
            .validate()
            .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        self.manifest
            .validate()
            .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        let catalog_digest =
            app_operation_catalog_digest_v1(&self.manifest.app_id, &self.operations)
                .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        if &self.entry != expected
            || self.entry.app_id != self.manifest.app_id
            || self.entry.artifact_version != self.manifest.artifact_version
            || catalog_digest != self.manifest.operation_catalog_digest
        {
            return Err(GatewayApiError::Contract(
                "APP detail identity, version, or operation catalog does not match the catalog"
                    .to_owned(),
            ));
        }
        validate_signed_tui_operations(&self.manifest, &self.operations)?;
        Ok(())
    }
}

fn validate_signed_tui_operations(
    manifest: &AppManifestV1,
    operations: &[OperationDescriptorV1],
) -> Result<(), GatewayApiError> {
    let presentation = manifest.presentation.as_ref().ok_or_else(|| {
        GatewayApiError::Contract("APP detail has no signed presentation".to_owned())
    })?;
    let expected = [
        (
            OperationKindV1::Query,
            false,
            app_tui_view_open_request_schema_digest_v1(),
            app_tui_view_open_response_schema_digest_v1(),
        ),
        (
            OperationKindV1::Command,
            false,
            app_tui_view_action_request_schema_digest_v1(),
            app_tui_view_action_response_schema_digest_v1(),
        ),
        (
            OperationKindV1::Subscribe,
            true,
            app_tui_view_stream_request_schema_digest_v1(),
            app_tui_view_patch_schema_digest_v1(),
        ),
    ];
    for view in &presentation.tui_views {
        for (operation_id, (kind, streaming, input, output)) in [
            &view.open_operation_id,
            &view.action_operation_id,
            &view.stream_operation_id,
        ]
        .into_iter()
        .zip(expected.iter())
        {
            let operation = operations
                .binary_search_by_key(&operation_id.as_str(), |candidate| {
                    candidate.operation_id.as_str()
                })
                .ok()
                .map(|index| &operations[index])
                .ok_or_else(|| {
                    GatewayApiError::Contract(format!(
                        "signed TUI operation `{operation_id}` is absent from the live catalog"
                    ))
                })?;
            let input = input
                .as_ref()
                .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
            let output = output
                .as_ref()
                .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
            if operation.kind != *kind
                || operation.streaming != *streaming
                || operation.input_schema_digest != *input
                || operation.output_schema_digest != *output
            {
                return Err(GatewayApiError::Contract(format!(
                    "signed TUI operation `{operation_id}` has incompatible role or schemas"
                )));
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub enum GatewayApiError {
    Http(reqwest::Error),
    Status(reqwest::StatusCode, String),
    /// The session SSE body already emitted and delivered the typed terminal
    /// revoke event. The runner must stop without synthesizing a duplicate.
    SessionAuthorizationRevoked(String),
    Contract(String),
    Url(String),
}

impl GatewayApiClient {
    #[must_use]
    pub fn observer_id(&self) -> &str {
        &self.observer_id
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        authorize_tui_request(request, self.auth_token.as_deref(), &self.observer_id)
    }

    pub fn new(
        base_url: impl Into<String>,
        auth_token: Option<String>,
    ) -> Result<Self, GatewayApiError> {
        let base_url = normalize_base_url(base_url.into())?;
        let client = reqwest::Client::builder()
            .connect_timeout(DEFAULT_GATEWAY_CONNECT_TIMEOUT)
            .timeout(DEFAULT_GATEWAY_REQUEST_TIMEOUT)
            .build()
            .map_err(GatewayApiError::Http)?;
        let sse_client = reqwest::Client::builder()
            .connect_timeout(DEFAULT_GATEWAY_CONNECT_TIMEOUT)
            .read_timeout(GATEWAY_SSE_IDLE_TIMEOUT)
            .build()
            .map_err(GatewayApiError::Http)?;
        let observer_id = std::env::var("COWD_TUI_OBSERVER_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("tui:{}", uuid::Uuid::new_v4()));
        let live = Arc::new(TuiLiveMultiplexer {
            transport: LiveTransportConfig {
                base_url: base_url.clone(),
                auth_token: auth_token.clone(),
                observer_id: observer_id.clone(),
                client: client.clone(),
                sse_client: sse_client.clone(),
            },
            commands: Mutex::new(None),
        });
        Ok(Self {
            base_url,
            auth_token,
            observer_id,
            client,
            sse_client,
            live,
            pending_cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Load and validate the Gateway-owned dynamic APP catalog.
    pub async fn app_catalog(&self) -> Result<AppCatalogV1, GatewayApiError> {
        let value = self
            .get_json(surface::gateway_api::paths::API_APPS.template())
            .await?;
        let catalog: AppCatalogV1 = serde_json::from_value(value)
            .map_err(|error| GatewayApiError::Contract(format!("invalid APP catalog: {error}")))?;
        catalog
            .validate()
            .map_err(|error| GatewayApiError::Contract(format!("invalid APP catalog: {error}")))?;
        Ok(catalog)
    }

    /// Load the signed and live contracts for one catalog generation.
    pub(crate) async fn app_detail(
        &self,
        entry: &AppCatalogEntryV1,
    ) -> Result<GatewayAppDetailResponseV1, GatewayApiError> {
        entry
            .validate()
            .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        let value = self
            .get_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_APPS_BY_APP_ID,
                &[(url_encode(&entry.app_id.0)).to_string()],
            ))
            .await?;
        let detail: GatewayAppDetailResponseV1 = serde_json::from_value(value)
            .map_err(|error| GatewayApiError::Contract(format!("invalid APP detail: {error}")))?;
        detail.validate_against_catalog_entry(entry)?;
        Ok(detail)
    }

    pub fn from_running_gateway(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        let base_url =
            std::env::var("COWD_GATEWAY_URL").unwrap_or_else(|_| DEFAULT_GATEWAY_URL.to_string());
        if !gateway_listener_reachable(&base_url) {
            return Ok(None);
        }
        Self::new(base_url, auth_token.or_else(default_auth_token)).map(Some)
    }

    pub fn from_running_gateway_with_retry(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        let auth_token = auth_token.or_else(default_auth_token);
        for attempt in 0..GATEWAY_READY_RETRY_ATTEMPTS {
            if let Some(client) = Self::from_running_gateway(auth_token.clone())? {
                return Ok(Some(client));
            }
            if attempt + 1 < GATEWAY_READY_RETRY_ATTEMPTS {
                std::thread::sleep(GATEWAY_READY_RETRY_DELAY);
            }
        }
        Ok(None)
    }

    pub fn ensure_running_with_retry(
        auth_token: Option<String>,
    ) -> Result<Option<Self>, GatewayApiError> {
        if let Some(client) = Self::from_running_gateway_with_retry(auth_token.clone())? {
            return Ok(Some(client));
        }

        if !cfg!(test) && std::env::var("COWD_DISABLE_DAEMON_AUTOSTART").is_err() {
            let exe =
                std::env::current_exe().map_err(|error| GatewayApiError::Url(error.to_string()))?;
            std::process::Command::new(exe)
                .arg("gateway")
                .arg("run")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        }

        Self::from_running_gateway_with_retry(auth_token)
    }

    pub async fn runtime_control_plane(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_CONTROL_PLANE.template())
            .await
    }

    pub async fn status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_STATUS.template())
            .await
    }

    pub async fn gateway_manifest(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_WEBUI_MANIFEST.template())
            .await
    }

    pub async fn slash_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_SLASH,
            &[],
            &format!("surface={}", url_encode(surface)),
        ))
        .await
    }

    pub async fn slash_resolve(
        &self,
        input: &str,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_SLASH_RESOLVE.template(),
            serde_json::json!({
                "input": input,
                "surface": surface,
                "context": {},
            }),
        )
        .await
    }

    pub async fn slash_dispatch(
        &self,
        command: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_SLASH_DISPATCH.template(),
            serde_json::json!({
                "command": command,
                "args": args,
            }),
        )
        .await
    }

    pub async fn runtime_snapshot(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_SNAPSHOT.template())
            .await
    }

    pub async fn list_sessions(&self) -> Result<serde_json::Value, GatewayApiError> {
        let mut offset = 0usize;
        let mut sessions = Vec::new();
        loop {
            let page = self
                .get_json(&crate::gateway_client_routes::route_with_query(
                    surface::gateway_api::paths::API_SESSIONS,
                    &[],
                    &format!("limit=200&offset={offset}&sort=updated_at&order=desc"),
                ))
                .await?;
            let page_sessions = page
                .get("sessions")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let total = page
                .get("total")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(page_sessions.len() as u64) as usize;
            let fetched = page_sessions.len();
            sessions.extend(page_sessions);
            offset = offset.saturating_add(fetched);
            if fetched == 0 || offset >= total {
                return Ok(serde_json::json!({
                    "sessions": sessions,
                    "total": total,
                    "offset": 0,
                    "limit": sessions.len(),
                    "sort": "updated_at",
                    "order": "desc",
                }));
            }
        }
    }

    pub async fn create_session(
        &self,
        model: Option<&str>,
        execution_policy_preset: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut body = serde_json::json!({ "model": model });
        if let Some(preset) = execution_policy_preset {
            if !preset.trim().is_empty() {
                body["execution_policy_preset"] = serde_json::Value::String(preset.to_string());
            }
        }
        self.post_json(surface::gateway_api::paths::API_SESSIONS.template(), body)
            .await
    }

    pub async fn rename_session(
        &self,
        session_id: &str,
        title: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .patch_json(
                &crate::gateway_client_routes::session::for_session(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID,
                    url_encode(session_id),
                ),
                serde_json::json!({ "title": title }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "rename session receipt")?;
        Ok(value)
    }

    pub async fn update_session_model(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .patch_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({ "model": model }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "update session receipt")?;
        Ok(value)
    }

    pub async fn delete_session(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SESSIONS_BY_ID,
            &[(url_encode(session_id)).to_string()],
        ))
        .await
    }

    pub async fn branch_session(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_BRANCH,
                &[(url_encode(session_id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn session_stats(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_STATS,
                &[(url_encode(session_id)).to_string()],
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session stats")?;
        Ok(value)
    }

    pub async fn session_input_projection(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_INPUT_PROJECTION,
                &[(url_encode(session_id)).to_string()],
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session input projection")?;
        Ok(value)
    }

    pub async fn cancel_session_input(
        &self,
        session_id: &str,
        input_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_INPUTS_BY_INPUT_ID_CANCEL,
                    &[
                        (url_encode(session_id)).to_string(),
                        (url_encode(input_id)).to_string(),
                    ],
                ),
                serde_json::json!({ "reason": reason }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "cancel session input receipt")?;
        Ok(value)
    }

    pub async fn turn_inbox(
        &self,
        session_id: &str,
        turn_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let suffix = turn_id
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("?turn_id={}", url_encode(value)))
            .unwrap_or_default();
        let value = self
            .get_json(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_TURN_INBOX,
                &[url_encode(session_id)],
                &suffix,
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "turn inbox")?;
        Ok(value)
    }

    pub async fn ensure_session(
        &self,
        session_id: &str,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_ENSURE,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({ "model": model }),
            )
            .await?,
            "ensure session",
            session_id,
        )
    }

    pub async fn session_messages(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<SessionMessagesPage, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_MESSAGES,
                &[(url_encode(session_id)).to_string()],
                &format!("from_seq={from_sequence}&limit={}", limit.min(500)),
            ))
            .await?;
        let page: SessionMessagesPage = serde_json::from_value(value).map_err(|error| {
            GatewayApiError::Contract(format!("invalid session message page: {error}"))
        })?;
        validate_session_messages_identity(session_id, &page)?;
        Ok(page)
    }

    pub async fn session_messages_offset(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<SessionMessagesPage, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_MESSAGES,
                &[(url_encode(session_id)).to_string()],
                &format!("offset={offset}&limit={}", limit.min(500)),
            ))
            .await?;
        let page: SessionMessagesPage = serde_json::from_value(value).map_err(|error| {
            GatewayApiError::Contract(format!("invalid session message page: {error}"))
        })?;
        validate_session_messages_identity(session_id, &page)?;
        Ok(page)
    }

    pub async fn hydrate_session_history(
        &self,
        session_id: &str,
        tx: CowdEventSender,
        next_sequence: Arc<AtomicUsize>,
        authority_generation: u64,
    ) {
        hydrate_session_history_with_retry(
            self,
            session_id,
            tx,
            next_sequence,
            authority_generation,
        )
        .await;
    }

    pub async fn session_history_index(
        &self,
        session_id: &str,
    ) -> Result<crate::protocol::SessionHistoryIndexProjection, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_HISTORY_INDEX,
                &[(url_encode(session_id)).to_string()],
                "metadata_limit=128&card_limit=64",
            ))
            .await?;
        let projection =
            serde_json::from_value::<crate::protocol::SessionHistoryIndexProjection>(value)
                .map_err(|error| {
                    GatewayApiError::Contract(format!("invalid session history index: {error}"))
                })?;
        if projection.session_id != session_id {
            return Err(GatewayApiError::Contract(format!(
                "requested session `{session_id}` but Gateway returned history index for `{}`",
                projection.session_id
            )));
        }
        Ok(projection)
    }

    pub async fn session_execution_index(
        &self,
        session_id: &str,
    ) -> Result<crate::protocol::SessionExecutionIndexProjection, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_EXECUTION,
                &[(url_encode(session_id)).to_string()],
            ))
            .await?;
        let index: crate::protocol::SessionExecutionIndexProjection = serde_json::from_value(value)
            .map_err(|error| {
                GatewayApiError::Contract(format!("invalid session execution index: {error}"))
            })?;
        if index.session_id != session_id {
            return Err(GatewayApiError::Contract(format!(
                "requested session `{session_id}` but Gateway returned execution index for `{}`",
                index.session_id
            )));
        }
        Ok(index)
    }

    pub async fn session_execution_policy(
        &self,
        session_id: &str,
    ) -> Result<harness_contract::policy::SessionExecutionPolicyResponse, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_EXECUTION_POLICY,
                &[(url_encode(session_id)).to_string()],
            ))
            .await?;
        let response = serde_json::from_value::<
            harness_contract::policy::SessionExecutionPolicyResponse,
        >(value)
        .map_err(|error| {
            GatewayApiError::Contract(format!(
                "invalid Session execution policy response: {error}"
            ))
        })?;
        if response.session_id != session_id || response.state.effective.revision == 0 {
            return Err(GatewayApiError::Contract(
                "invalid Session execution policy response".to_string(),
            ));
        }
        Ok(response)
    }

    pub async fn update_session_execution_policy(
        &self,
        session_id: &str,
        preset: &str,
        expected_revision: u64,
    ) -> Result<harness_contract::policy::SessionExecutionPolicyResponse, GatewayApiError> {
        let value = self
            .put_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_EXECUTION_POLICY,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({
                    "preset": preset,
                    "expected_revision": expected_revision,
                }),
            )
            .await?;
        serde_json::from_value(value).map_err(|error| {
            GatewayApiError::Contract(format!(
                "invalid updated Session execution policy response: {error}"
            ))
        })
    }

    pub async fn send_message(
        &self,
        session_id: &str,
        content: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.send_message_with_resources(session_id, content, &[], None)
            .await
    }

    pub async fn send_message_with_resources(
        &self,
        session_id: &str,
        content: &str,
        resource_ids: &[String],
        client_message_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_MESSAGES,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({
                    "content": content,
                    "resource_ids": resource_ids,
                    "client_message_id": client_message_id,
                    // The caller already supplies the surface-qualified stable
                    // identity (`tui:<uuid>`). Reuse it as the admission key so
                    // optimistic UI, durable message and idempotent retry all
                    // address one identity instead of `tui:tui:<uuid>`.
                    "idempotency_key": client_message_id,
                }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "message admission receipt")?;
        Ok(value)
    }

    pub async fn upload_resource_path(
        &self,
        path: &Path,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = tokio::fs::metadata(path)
            .await
            .map_err(|error| GatewayApiError::Url(error.to_string()))?
            .len();
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("resource.bin")
            .to_string();
        let body =
            reqwest::Body::wrap_stream(futures::stream::try_unfold(file, |mut file| async move {
                let mut chunk = vec![0_u8; 64 * 1024];
                let read = file.read(&mut chunk).await?;
                if read == 0 {
                    Ok::<_, std::io::Error>(None)
                } else {
                    chunk.truncate(read);
                    Ok(Some((chunk, file)))
                }
            }));
        let part = reqwest::multipart::Part::stream_with_length(body, bytes).file_name(file_name);
        let form = reqwest::multipart::Form::new()
            .text("source", "tui")
            .text("session_id", session_id.to_string())
            .part("file", part);
        let url = format!("{}/api/resources", self.base_url);
        let request = self.authorize(self.client.post(url).multipart(form));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        let text = response.text().await.map_err(GatewayApiError::Http)?;
        if !status.is_success() {
            return Err(gateway_status_error(status, text));
        }
        let value: serde_json::Value = serde_json::from_str(&text).map_err(|error| {
            GatewayApiError::Contract(format!("invalid resource receipt: {error}"))
        })?;
        validate_session_json_identity_at(
            session_id,
            &value,
            "resource upload receipt",
            &["/resource/session_id"],
        )?;
        Ok(value)
    }

    pub async fn workspace_overview(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_WORKSPACE.template())
            .await
    }

    pub async fn workspace_files(
        &self,
        dir: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match dir.map(str::trim).filter(|dir| !dir.is_empty()) {
            Some(dir) => crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_WORKSPACE_FILES,
                &[],
                &format!("dir={}", url_encode(dir)),
            ),
            None => surface::gateway_api::paths::API_WORKSPACE_FILES
                .template()
                .to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn workspace_files_recursive(
        &self,
        dir: Option<&str>,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut path = match dir.map(str::trim).filter(|dir| !dir.is_empty()) {
            Some(dir) => crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_WORKSPACE_FILES,
                &[],
                &format!("dir={}", url_encode(dir)),
            ),
            None => crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_WORKSPACE_FILES,
                &[],
                "",
            )
            .to_string(),
        };
        if !path.ends_with('?') && !path.ends_with('&') {
            path.push('&');
        }
        path.push_str("recursive=true&limit=");
        path.push_str(&limit.to_string());
        self.get_json(&path).await
    }

    pub async fn create_workspace_file(
        &self,
        path: &str,
        content: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_WORKSPACE_FILES.template(),
            serde_json::json!({ "path": path, "content": content }),
        )
        .await
    }

    pub async fn create_workspace_dir(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_WORKSPACE_DIRS.template(),
            serde_json::json!({ "path": path }),
        )
        .await
    }

    pub async fn delete_workspace_path(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_WORKSPACE_FILES,
            &[],
            &format!("path={}", url_encode(path)),
        ))
        .await
    }

    pub async fn rename_workspace_path(
        &self,
        path: &str,
        to: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_WORKSPACE_RENAME.template(),
            serde_json::json!({ "path": path, "to": to }),
        )
        .await
    }

    pub async fn workspace_meta(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_WORKSPACE_META,
            &[],
            &format!("path={}", url_encode(path)),
        ))
        .await
    }

    pub async fn download_workspace_path(&self, path: &str) -> Result<Vec<u8>, GatewayApiError> {
        self.get_bytes(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_WORKSPACE_DOWNLOAD,
            &[],
            &format!("path={}", url_encode(path)),
        ))
        .await
    }

    pub async fn workspace_file_preview(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = self
            .get_bytes(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_FILE_RAW,
                &[],
                &format!("path={}", url_encode(path)),
            ))
            .await?;
        let truncated = bytes.len() > max_bytes;
        let slice = if truncated {
            &bytes[..max_bytes]
        } else {
            bytes.as_slice()
        };
        let content = String::from_utf8_lossy(slice).to_string();
        Ok(serde_json::json!({
            "path": path,
            "content": content,
            "bytes": bytes.len(),
            "truncated": truncated,
        }))
    }

    pub async fn upload_workspace_file_path(
        &self,
        path: &Path,
        dir: Option<&str>,
        overwrite: bool,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let bytes = std::fs::read(path).map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("upload.bin")
            .to_string();
        let part = reqwest::multipart::Part::bytes(bytes).file_name(file_name);
        let form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("dir", dir.unwrap_or_default().to_string())
            .text("overwrite", overwrite.to_string());
        let url = format!("{}/api/upload", self.base_url);
        let request = self.authorize(self.client.post(url).multipart(form));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        let text = response.text().await.map_err(GatewayApiError::Http)?;
        if !status.is_success() {
            return Err(gateway_status_error(status, text));
        }
        serde_json::from_str(&text).map_err(|error| GatewayApiError::Url(error.to_string()))
    }

    pub async fn list_session_attachments(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_ATTACHMENTS,
                &[(url_encode(session_id)).to_string()],
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session attachment list")?;
        Ok(value)
    }

    pub async fn add_session_attachment(
        &self,
        session_id: &str,
        path: &str,
        kind: &str,
        label: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_ATTACHMENTS,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({
                    "path": path,
                    "kind": kind,
                    "label": label,
                }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "add session attachment receipt")?;
        Ok(value)
    }

    pub async fn delete_session_attachment(
        &self,
        session_id: &str,
        ref_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .delete_json(&crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_ATTACHMENTS_BY_REF_ID,
                &[
                    (url_encode(session_id)).to_string(),
                    (url_encode(ref_id)).to_string(),
                ],
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "delete session attachment receipt")?;
        Ok(value)
    }

    pub async fn cancel_session_turn(
        &self,
        session_id: &str,
        expected_execution_id: &str,
        expected_turn_id: &str,
        reason: &str,
    ) -> Result<harness_contract::turn::CancellationReceipt, GatewayApiError> {
        let target_key = format!("{session_id}\0{expected_execution_id}\0{expected_turn_id}");
        let (cancellation_id, requested_at_ms) = self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(target_key.clone())
            .or_insert_with(|| {
                (
                    format!("tui-cancel:{}", uuid::Uuid::new_v4()),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                )
            })
            .clone();
        let value = self
            .post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_CANCEL,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({
                    "reason": reason,
                    "cancellation_id": cancellation_id,
                    "requested_at_ms": requested_at_ms,
                    "expected_execution_id": expected_execution_id,
                    "expected_turn_id": expected_turn_id,
                }),
            )
            .await?;
        validate_session_json_identity(session_id, &value, "cancel session turn receipt")?;
        let receipt = serde_json::from_value(value).map_err(|error| {
            GatewayApiError::Contract(format!(
                "Gateway cancel session turn receipt is invalid: {error}"
            ))
        })?;
        self.pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&target_key);
        Ok(receipt)
    }

    pub async fn consume_session_live_source(
        &self,
        session_id: &str,
        tx: CowdEventSender,
        after_commit_cursor: Option<u64>,
        next_message_sequence: Arc<AtomicUsize>,
        authority_generation: u64,
    ) -> Result<SessionStreamProgress, GatewayApiError> {
        let mut source = self
            .live
            .subscribe(harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Session,
                id: session_id.to_string(),
                cursor: after_commit_cursor.unwrap_or_default(),
                revision: 0,
                detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            })
            .await?;
        tx.send_wait(session_scoped_event(
            session_id,
            authority_generation,
            CowdEvent::SessionStreamConnection {
                session_id: session_id.to_string(),
                state: SessionStreamConnectionState::Connected,
            },
        ))
        .await
        .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
        let mut latest_cursor = after_commit_cursor;
        while let Some(envelope) = source.recv().await {
            if envelope.source_kind != "session" || envelope.source_id != session_id {
                return Err(GatewayApiError::Contract(
                    "TUI live demultiplexer delivered a mismatched Session source".to_string(),
                ));
            }
            let candidate_cursor = envelope.source_cursor;
            if envelope.event == "source.authorization_revoked" {
                let reason = envelope
                    .payload
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Gateway revoked this Session source")
                    .to_string();
                tx.send_wait(session_scoped_event(
                    session_id,
                    authority_generation,
                    CowdEvent::SessionAuthorizationRevoked {
                        session_id: session_id.to_string(),
                        reason: reason.clone(),
                    },
                ))
                .await
                .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
                return Err(GatewayApiError::SessionAuthorizationRevoked(reason));
            }
            if envelope.source_health == harness_contract::live::SourceHealth::ResyncRequired {
                latest_cursor = Some(
                    latest_cursor
                        .unwrap_or_default()
                        .max(candidate_cursor.unwrap_or_default()),
                );
                let _ = tx.send(session_scoped_event(
                    session_id,
                    authority_generation,
                    CowdEvent::Warning {
                        message: format!(
                            "Gateway Session source requested recovery: {}; refreshing durable history and execution projection",
                            envelope
                                .payload
                                .get("reason")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("canonical resync required")
                        ),
                    },
                ));
                return Ok(SessionStreamProgress {
                    commit_cursor: latest_cursor,
                    next_message_sequence: next_message_sequence.load(Ordering::Acquire),
                });
            }
            let frame = match candidate_cursor {
                Some(cursor) => format!(
                    "id: {cursor}\nevent: {}\ndata: {}",
                    envelope.event, envelope.payload
                ),
                None => format!("event: {}\ndata: {}", envelope.event, envelope.payload),
            };
            match strict_gateway_sse_frame_to_cowd_event_for_session(&frame, session_id) {
                Ok(Some(event)) => {
                    deliver_session_stream_event_with_catchup(
                        self,
                        &tx,
                        session_id,
                        event,
                        &next_message_sequence,
                        authority_generation,
                    )
                    .await?;
                    latest_cursor = Some(
                        latest_cursor
                            .unwrap_or_default()
                            .max(candidate_cursor.unwrap_or_default()),
                    );
                }
                Ok(None) => {}
                Err(error) => return Err(GatewayApiError::Contract(error)),
            }
        }
        Ok(SessionStreamProgress {
            commit_cursor: latest_cursor,
            next_message_sequence: next_message_sequence.load(Ordering::Acquire),
        })
    }

    pub async fn attach_session(
        &self,
        session_id: &str,
        surface: &str,
        role: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_ATTACH,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({ "surface": surface, "role": role }),
            )
            .await?,
            "attach session",
            session_id,
        )
    }

    pub async fn detach_session(
        &self,
        session_id: &str,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_SESSIONS_BY_ID_DETACH,
                    &[(url_encode(session_id)).to_string()],
                ),
                serde_json::json!({ "surface": surface }),
            )
            .await?,
            "detach session",
            session_id,
        )
    }

    pub async fn lifecycle_snapshot(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        match session_id {
            Some(session_id) => {
                let value = self
                    .get_json(&crate::gateway_client_routes::render_route(
                        surface::gateway_api::paths::API_SESSIONS_BY_ID_LIFECYCLE,
                        &[(url_encode(session_id)).to_string()],
                    ))
                    .await?;
                validate_session_json_identity(session_id, &value, "session lifecycle snapshot")?;
                Ok(value)
            }
            None => {
                self.get_json(surface::gateway_api::paths::API_RUNTIME_SNAPSHOT.template())
                    .await
            }
        }
    }

    pub async fn replay_session(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let value = self
            .get_json(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_REPLAY,
                &[(url_encode(session_id)).to_string()],
                &format!("from_sequence={from_sequence}&limit={limit}"),
            ))
            .await?;
        validate_session_json_identity(session_id, &value, "session replay")?;
        Ok(value)
    }

    pub async fn cowd_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_CAPABILITIES.template())
            .await
    }

    pub async fn cowd_projection(
        &self,
        surface: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_COWD_PROJECTION,
            &[],
            &format!("surface={}", url_encode(surface)),
        ))
        .await
    }

    pub async fn cowd_surfaces(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_SURFACES.template())
            .await
    }

    pub async fn cowd_release_gate(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_RELEASE_GATE.template())
            .await
    }

    pub async fn gateway_capability_contract(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_GATEWAY_CAPABILITY_CONTRACT.template())
            .await
    }

    pub async fn gateway_openai_tools(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_GATEWAY_OPENAI_TOOLS.template())
            .await
    }

    pub async fn structured_sources(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_STRUCTURED_SOURCES.template())
            .await
    }

    pub async fn structured_facts(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_STRUCTURED_FACTS.template())
            .await
    }

    pub async fn structured_evidence(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_STRUCTURED_EVIDENCE.template())
            .await
    }

    pub async fn structured_watermarks(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_COWD_STRUCTURED_WATERMARKS.template())
            .await
    }

    pub async fn structured_ingest_plan(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_COWD_STRUCTURED_INGEST_PLAN.template(),
            input,
        )
        .await
    }

    pub async fn runtime_session_leases(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_SESSION_LEASES.template())
            .await
    }

    pub async fn acquire_runtime_session_lease(
        &self,
        session_id: &str,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                surface::gateway_api::paths::API_RUNTIME_SESSION_LEASES_ACQUIRE.template(),
                serde_json::json!({
                    "session_id": session_id,
                    "mode": mode,
                }),
            )
            .await?,
            "acquire runtime session lease",
            session_id,
        )
    }

    pub async fn release_runtime_session_lease(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        require_gateway_session_operation_ok(
            self.post_json(
                surface::gateway_api::paths::API_RUNTIME_SESSION_LEASES_RELEASE.template(),
                serde_json::json!({
                    "session_id": session_id,
                }),
            )
            .await?,
            "release runtime session lease",
            session_id,
        )
    }

    pub async fn runtime_effective_config(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_CONFIG_EFFECTIVE.template())
            .await
    }

    pub async fn config(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_CONFIG.template())
            .await
    }

    pub async fn config_providers(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_CONFIG_PROVIDERS.template())
            .await
    }

    pub async fn update_config_model(
        &self,
        model: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.put_json(
            surface::gateway_api::paths::API_CONFIG.template(),
            serde_json::json!({ "model": model }),
        )
        .await
    }

    pub async fn config_reload_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_CONFIG_RELOAD_STATUS.template())
            .await
    }

    pub async fn runtime_timeline(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_RUNTIME_TIMELINE,
            &[],
            &format!("session_id={}&limit={}", url_encode(session_id), limit),
        ))
        .await
    }

    pub async fn execution_projection(
        &self,
        execution_id: &str,
        full: bool,
    ) -> Result<harness_contract::projection::ExecutionProjection, GatewayApiError> {
        let scope = if full { "full" } else { "summary" };
        let value = self
            .get_json(&crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_RUNTIME_EXECUTIONS_BY_ID,
                &[(url_encode(execution_id)).to_string()],
                &format!("detail_scope={scope}"),
            ))
            .await?;
        let projection: harness_contract::projection::ExecutionProjection =
            serde_json::from_value(value)
                .map_err(|error| GatewayApiError::Contract(error.to_string()))?;
        crate::protocol::validate_execution_projection_schema(&projection)
            .map_err(GatewayApiError::Contract)?;
        validate_execution_projection_identity(execution_id, &projection.execution_id)?;
        Ok(projection)
    }

    pub async fn consume_execution_live_source(
        &self,
        execution_id: &str,
        after_cursor: u64,
        after_revision: u64,
        full: bool,
        generation: u64,
        tx: CowdEventSender,
    ) -> Result<(u64, u64), GatewayApiError> {
        let mut source = self
            .live
            .subscribe(harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Execution,
                id: execution_id.to_string(),
                cursor: after_cursor,
                revision: after_revision,
                detail_scope: if full {
                    harness_contract::projection::ProjectionDetailScope::Full
                } else {
                    harness_contract::projection::ProjectionDetailScope::Summary
                },
            })
            .await?;
        tx.send(CowdEvent::ExecutionProjectionConnection {
            generation,
            execution_id: execution_id.to_string(),
            state: crate::protocol::SessionStreamConnectionState::Connected,
        })
        .map_err(|_| {
            GatewayApiError::Url(
                "TUI execution projection consumer closed during connect".to_string(),
            )
        })?;

        let mut latest_cursor = after_cursor;
        let mut latest_revision = after_revision;
        while let Some(envelope) = source.recv().await {
            if envelope.source_kind != "execution" || envelope.source_id != execution_id {
                return Err(GatewayApiError::Contract(
                    "TUI live demultiplexer delivered a mismatched Execution source".to_string(),
                ));
            }
            if envelope.event == "source.authorization_revoked" {
                return Err(GatewayApiError::Status(
                    reqwest::StatusCode::FORBIDDEN,
                    envelope
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("execution source authorization revoked")
                        .to_string(),
                ));
            }
            if envelope.source_health == harness_contract::live::SourceHealth::ResyncRequired {
                let snapshot = self.execution_projection(execution_id, full).await?;
                latest_cursor = snapshot.cursor;
                latest_revision = snapshot.revision;
                tx.send_wait(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection: snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url(
                        "TUI execution projection consumer closed during resync".to_string(),
                    )
                })?;
                return Ok((latest_cursor, latest_revision));
            }
            latest_revision = match envelope.event.as_str() {
                "projection_snapshot" => envelope
                    .payload
                    .get("revision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(latest_revision),
                "projection_delta" => envelope
                    .payload
                    .get("target_revision")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(latest_revision),
                _ => latest_revision,
            };
            let frame = format!("event: {}\ndata: {}", envelope.event, envelope.payload);
            latest_cursor = self
                .apply_execution_projection_sse_frame(
                    &frame,
                    execution_id,
                    full,
                    generation,
                    latest_cursor,
                    &tx,
                )
                .await?;
        }
        Ok((latest_cursor, latest_revision))
    }

    pub async fn consume_mission_live_source(
        &self,
        mission_id: &str,
        tx: CowdEventSender,
    ) -> Result<(), GatewayApiError> {
        let mut source = self
            .live
            .subscribe(harness_contract::live::LiveSourceSelector {
                kind: harness_contract::live::LiveSourceKind::Mission,
                id: mission_id.to_string(),
                cursor: 0,
                revision: 0,
                detail_scope: harness_contract::projection::ProjectionDetailScope::Full,
            })
            .await?;
        let mut applied_cursor = None;
        let mut applied_revision = None;
        while let Some(envelope) = source.recv().await {
            if envelope.source_kind != "mission" || envelope.source_id != mission_id {
                return Err(GatewayApiError::Contract(
                    "TUI live demultiplexer delivered a mismatched Mission source".to_string(),
                ));
            }
            if envelope.event == "source.authorization_revoked" {
                return Err(GatewayApiError::Status(
                    reqwest::StatusCode::FORBIDDEN,
                    envelope
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("mission source authorization revoked")
                        .to_string(),
                ));
            }
            if envelope.source_health == harness_contract::live::SourceHealth::ResyncRequired {
                let snapshot = self.mission_control_snapshot().await?;
                applied_cursor = Some(snapshot.cursor);
                applied_revision = Some(snapshot.revision);
                tx.send_wait(CowdEvent::MissionProjectionSnapshot {
                    mission_id: mission_id.to_string(),
                    snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                })?;
                continue;
            }
            if envelope.event == "mission_snapshot" {
                let snapshot = serde_json::from_value::<
                    harness_contract::mission::MissionMaterializedSnapshot,
                >(envelope.payload)
                .map_err(|error| {
                    GatewayApiError::Contract(format!(
                        "Gateway Mission snapshot contract is invalid: {error}"
                    ))
                })?;
                applied_cursor = Some(snapshot.cursor);
                applied_revision = Some(snapshot.revision);
                tx.send_wait(CowdEvent::MissionProjectionSnapshot {
                    mission_id: mission_id.to_string(),
                    snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                })?;
            } else if envelope.event == "mission_delta" {
                let delta = serde_json::from_value::<
                    harness_contract::mission::MissionProjectionDelta,
                >(envelope.payload)
                .map_err(|error| {
                    GatewayApiError::Contract(format!(
                        "Gateway Mission delta contract is invalid: {error}"
                    ))
                })?;
                if applied_cursor != Some(delta.from_cursor)
                    || applied_revision != delta.from_revision
                {
                    let snapshot = self.mission_control_snapshot().await?;
                    applied_cursor = Some(snapshot.cursor);
                    applied_revision = Some(snapshot.revision);
                    tx.send_wait(CowdEvent::MissionProjectionSnapshot {
                        mission_id: mission_id.to_string(),
                        snapshot,
                    })
                    .await
                    .map_err(|_| {
                        GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                    })?;
                    continue;
                }
                applied_cursor = Some(delta.to_cursor);
                applied_revision = Some(delta.revision);
                tx.send_wait(CowdEvent::MissionProjectionDelta {
                    mission_id: mission_id.to_string(),
                    delta,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url("TUI Mission projection consumer closed".to_string())
                })?;
            }
        }
        Ok(())
    }

    async fn apply_execution_projection_sse_frame(
        &self,
        frame: &str,
        execution_id: &str,
        full: bool,
        generation: u64,
        latest_cursor: u64,
        tx: &CowdEventSender,
    ) -> Result<u64, GatewayApiError> {
        let event_name = gateway_sse_frame_event_name(frame);
        if event_name.is_none()
            && frame
                .lines()
                .all(|line| line.trim().is_empty() || line.trim_start().starts_with(':'))
        {
            return Ok(latest_cursor);
        }
        if gateway_sse_frame_projection_authorization_revoked(frame) {
            return Err(GatewayApiError::Status(
                reqwest::StatusCode::FORBIDDEN,
                "Gateway revoked the execution projection stream".to_string(),
            ));
        }
        let frame_cursor = gateway_sse_frame_commit_cursor(frame);
        match event_name {
            Some("projection_snapshot") => {
                let data = gateway_sse_frame_data(frame).ok_or_else(|| {
                    GatewayApiError::Contract(
                        "projection_snapshot frame has no typed payload".to_string(),
                    )
                })?;
                let snapshot: harness_contract::projection::ExecutionProjection =
                    serde_json::from_str(&data).map_err(|error| {
                        GatewayApiError::Contract(format!(
                            "projection_snapshot payload is invalid: {error}"
                        ))
                    })?;
                crate::protocol::validate_execution_projection_schema(&snapshot)
                    .map_err(GatewayApiError::Contract)?;
                if snapshot.execution_id != execution_id {
                    return Err(GatewayApiError::Contract(format!(
                        "projection_snapshot execution mismatch: expected {execution_id}, got {}",
                        snapshot.execution_id
                    )));
                }
                if frame_cursor.is_some_and(|cursor| cursor < snapshot.cursor) {
                    return Err(GatewayApiError::Contract(
                        "projection_snapshot frame id regressed behind its typed cursor"
                            .to_string(),
                    ));
                }
                let accepted_cursor = latest_cursor
                    .max(snapshot.cursor)
                    .max(frame_cursor.unwrap_or_default());
                tx.send_wait(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection: snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url(
                        "TUI execution projection consumer closed during snapshot".to_string(),
                    )
                })?;
                Ok(accepted_cursor)
            }
            Some("projection_delta") => {
                let data = gateway_sse_frame_data(frame).ok_or_else(|| {
                    GatewayApiError::Contract(
                        "projection_delta frame has no typed payload".to_string(),
                    )
                })?;
                let delta: harness_contract::projection::ProjectionDelta =
                    serde_json::from_str(&data).map_err(|error| {
                        GatewayApiError::Contract(format!(
                            "projection_delta payload is invalid: {error}"
                        ))
                    })?;
                crate::protocol::validate_projection_delta_schema(&delta)
                    .map_err(GatewayApiError::Contract)?;
                if delta.execution_id != execution_id {
                    return Err(GatewayApiError::Contract(format!(
                        "projection_delta execution mismatch: expected {execution_id}, got {}",
                        delta.execution_id
                    )));
                }
                if frame_cursor.is_some_and(|cursor| cursor < delta.target_cursor) {
                    return Err(GatewayApiError::Contract(
                        "projection_delta frame id regressed behind its target cursor".to_string(),
                    ));
                }
                let accepted_cursor = latest_cursor
                    .max(delta.target_cursor)
                    .max(frame_cursor.unwrap_or_default());
                tx.send_wait(CowdEvent::ExecutionProjectionDelta { generation, delta })
                    .await
                    .map_err(|_| {
                        GatewayApiError::Url(
                            "TUI execution projection consumer closed during delta".to_string(),
                        )
                    })?;
                Ok(accepted_cursor)
            }
            Some("projection_live") => {
                let data = gateway_sse_frame_data(frame).ok_or_else(|| {
                    GatewayApiError::Contract(
                        "projection_live frame has no typed payload".to_string(),
                    )
                })?;
                let update: harness_contract::projection::ExecutionLiveUpdate =
                    serde_json::from_str(&data).map_err(|error| {
                        GatewayApiError::Contract(format!(
                            "projection_live payload is invalid: {error}"
                        ))
                    })?;
                if update.schema_version
                    != harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION
                    || update.execution_id != execution_id
                {
                    return Err(GatewayApiError::Contract(
                        "projection_live schema or execution identity mismatch".to_string(),
                    ));
                }
                tx.send_wait(CowdEvent::ExecutionProjectionLive { generation, update })
                    .await
                    .map_err(|_| {
                        GatewayApiError::Url(
                            "TUI execution projection consumer closed during live update"
                                .to_string(),
                        )
                    })?;
                Ok(latest_cursor)
            }
            Some("projection_resync") => {
                // Fetch canonical state without terminating/restarting this
                // healthy SSE task. Generation gating drops a late snapshot
                // if the user selected a different execution meanwhile.
                let snapshot = self.execution_projection(execution_id, full).await?;
                let accepted_cursor = latest_cursor.max(snapshot.cursor);
                tx.send_wait(CowdEvent::ExecutionProjectionLoaded {
                    generation,
                    projection: snapshot,
                })
                .await
                .map_err(|_| {
                    GatewayApiError::Url(
                        "TUI execution projection consumer closed during resync".to_string(),
                    )
                })?;
                Ok(accepted_cursor)
            }
            Some("projection_error") => Err(GatewayApiError::Contract(
                "Gateway execution projection stream reported projection_error".to_string(),
            )),
            Some(other) => Err(GatewayApiError::Contract(format!(
                "Gateway execution projection stream emitted unknown event `{other}`"
            ))),
            None => Err(GatewayApiError::Contract(
                "Gateway execution projection frame has no event type".to_string(),
            )),
        }
    }

    pub async fn execute_projection_command(
        &self,
        execution_id: &str,
        request: &harness_contract::projection::ExecutionCommandRequest,
    ) -> Result<harness_contract::projection::ExecutionCommandReceipt, GatewayApiError> {
        let body = serde_json::to_value(request)
            .map_err(|error| GatewayApiError::Url(error.to_string()))?;
        let value = self
            .post_json(
                &crate::gateway_client_routes::render_route(
                    surface::gateway_api::paths::API_RUNTIME_EXECUTIONS_BY_ID_COMMANDS,
                    &[(url_encode(execution_id)).to_string()],
                ),
                body,
            )
            .await?;
        serde_json::from_value(value).map_err(|error| GatewayApiError::Url(error.to_string()))
    }

    pub async fn current_context(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match session_id {
            Some(id) if !id.trim().is_empty() => crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_CONTEXT_CURRENT,
                &[],
                &format!("session_id={}", url_encode(id)),
            ),
            _ => surface::gateway_api::paths::API_CONTEXT_CURRENT
                .template()
                .to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn memory_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MEMORY_STATUS.template())
            .await
    }

    pub async fn memory_maintenance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MEMORY_MAINTENANCE.template())
            .await
    }

    pub async fn run_memory_maintenance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_MEMORY_MAINTENANCE.template(),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn memory_knowledge_candidates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MEMORY_KNOWLEDGE_CANDIDATES.template())
            .await
    }

    pub async fn reality_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_REALITY_STATUS.template())
            .await
    }

    pub async fn reality_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_REALITY_CAPABILITIES.template())
            .await
    }

    pub async fn reality_flow(
        &self,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match session_id.map(str::trim).filter(|id| !id.is_empty()) {
            Some(id) => crate::gateway_client_routes::route_with_query(
                surface::gateway_api::paths::API_REALITY_FLOW,
                &[],
                &format!("session_id={}", url_encode(id)),
            ),
            None => surface::gateway_api::paths::API_REALITY_FLOW
                .template()
                .to_string(),
        };
        self.get_json(&path).await
    }

    pub async fn reality_boundaries(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_REALITY_BOUNDARIES.template())
            .await
    }

    pub async fn reality_governance(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_REALITY_GOVERNANCE.template())
            .await
    }

    pub async fn task_status(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_TASKS.template())
            .await
    }

    pub async fn session_task_focus(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SESSIONS_BY_ID_TASK_FOCUS,
            &[(url_encode(session_id)).to_string()],
        ))
        .await
    }

    pub async fn set_session_task_focus(
        &self,
        session_id: &str,
        task_id: &str,
        expected_revision: u64,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.put_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_TASK_FOCUS,
                &[(url_encode(session_id)).to_string()],
            ),
            serde_json::json!({
                "task_id": task_id,
                "expected_revision": expected_revision,
            }),
        )
        .await
    }

    pub async fn clear_session_task_focus(
        &self,
        session_id: &str,
        expected_revision: u64,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json_with_body(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_TASK_FOCUS,
                &[(url_encode(session_id)).to_string()],
            ),
            serde_json::json!({ "expected_revision": expected_revision }),
        )
        .await
    }

    pub async fn session_mission_focus(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SESSIONS_BY_ID_MISSION_FOCUS,
            &[(url_encode(session_id)).to_string()],
        ))
        .await
    }

    pub async fn set_session_mission_focus(
        &self,
        session_id: &str,
        mission_id: &str,
        expected_revision: u64,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.put_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_MISSION_FOCUS,
                &[(url_encode(session_id)).to_string()],
            ),
            serde_json::json!({
                "mission_id": mission_id,
                "expected_revision": expected_revision,
            }),
        )
        .await
    }

    pub async fn clear_session_mission_focus(
        &self,
        session_id: &str,
        expected_revision: u64,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.delete_json_with_body(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SESSIONS_BY_ID_MISSION_FOCUS,
                &[(url_encode(session_id)).to_string()],
            ),
            serde_json::json!({ "expected_revision": expected_revision }),
        )
        .await
    }

    pub async fn pending_approvals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_APPROVAL_PENDING.template())
            .await
    }

    pub async fn approval_history(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_APPROVAL_HISTORY,
            &[],
            "limit=200&offset=0",
        ))
        .await
    }

    pub async fn approval_grants(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_APPROVAL_GRANTS.template())
            .await
    }

    pub async fn revoke_approval_grant(
        &self,
        grant_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_APPROVAL_GRANTS_BY_ID_REVOKE,
                &[(url_encode(grant_id)).to_string()],
            ),
            serde_json::json!({ "reason": reason }),
        )
        .await
    }

    pub async fn approval_exact(
        &self,
        approval_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_APPROVAL_BY_ID,
            &[(url_encode(approval_id)).to_string()],
        ))
        .await
    }

    pub async fn mission_projection(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MISSION_PROJECTION.template())
            .await
    }

    pub async fn mission_control(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MISSION_CONTROL.template())
            .await
    }

    async fn mission_control_snapshot(
        &self,
    ) -> Result<harness_contract::mission::MissionMaterializedSnapshot, GatewayApiError> {
        let value = self.mission_control().await?;
        let snapshot = value.get("snapshot").cloned().unwrap_or(value);
        serde_json::from_value(snapshot).map_err(|error| {
            GatewayApiError::Contract(format!(
                "Gateway Mission materialized snapshot contract is invalid: {error}"
            ))
        })
    }

    pub async fn tick_mission_schedules(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_MISSION_SCHEDULES_TICK.template(),
            body,
        )
        .await
    }

    pub async fn apply_runtime_recovery(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_RUNTIME_EVENTS_RECOVER.template(),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn mission_session_detail(
        &self,
        session_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_MISSION_SESSIONS_BY_ID,
            &[(url_encode(session_id)).to_string()],
        ))
        .await
    }

    pub async fn mission_approvals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MISSION_APPROVALS.template())
            .await
    }

    pub async fn mission_relations(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MISSION_RELATIONS.template())
            .await
    }

    pub async fn submit_mission_approval(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_MISSION_APPROVALS.template(),
            body,
        )
        .await
    }

    pub async fn execute_mission_command(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_MISSION_CONTROL.template(),
            body,
        )
        .await
    }

    /// Read the Runtime-owned catalog of runnable Team template revisions.
    pub async fn team_templates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_TEAM_TEMPLATES.template())
            .await
    }

    /// Submit declarative Team intent. Gateway forwards it to Runtime, which
    /// resolves template/Agent revisions and constructs the graph.
    pub async fn instantiate_team_template(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TEAM_TEMPLATES_INSTANTIATE.template(),
            body,
        )
        .await
    }

    pub async fn team_working_state(
        &self,
        team_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_RUNTIME_TEAMS_BY_ID_WORKING_STATE,
            &[(url_encode(team_id)).to_string()],
        ))
        .await
    }

    pub async fn decide_mission_approval(
        &self,
        approval_id: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_MISSION_APPROVALS_BY_ID_DECISION,
                &[(url_encode(approval_id)).to_string()],
            ),
            body,
        )
        .await
    }

    pub async fn upsert_mission_proxy(
        &self,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_MISSION_PROXIES.template(),
            body,
        )
        .await
    }

    pub async fn runtime_agent_input(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_INPUT,
                &[(url_encode(agent_id)).to_string()],
            ),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn runtime_agent_interrupt(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_INTERRUPT,
                &[(url_encode(agent_id)).to_string()],
            ),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn runtime_agent_shutdown(
        &self,
        agent_id: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_RUNTIME_AGENTS_BY_ID_SHUTDOWN,
                &[(url_encode(agent_id)).to_string()],
            ),
            serde_json::json!({ "payload": payload }),
        )
        .await
    }

    pub async fn respond_approval(
        &self,
        id: &str,
        approved: bool,
        scope: Option<&str>,
        reason: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_APPROVAL_RESPOND.template(),
            serde_json::json!({
                "id": id,
                "approved": approved,
                "scope": scope.unwrap_or("once"),
                "reason": reason,
            }),
        )
        .await
    }

    pub async fn cancel_task(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_TASKS_BY_ID_CANCEL,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({
                "expected_revision": expected_revision,
                "note": "cancelled from TUI",
                "evidence_refs": [],
            }),
        )
        .await
    }

    pub async fn complete_task(
        &self,
        id: &str,
        expected_revision: u64,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_TASKS_BY_ID_COMPLETE,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({
                "expected_revision": expected_revision,
                "note": "completed from TUI",
                "evidence_refs": [],
            }),
        )
        .await
    }

    pub async fn cross_plane_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_CROSS_PLANE_SUMMARY.template())
            .await
    }

    pub async fn cross_plane_execution_receipt(
        &self,
        receipt_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_CROSS_PLANE_ACTION_EXECUTIONS_BY_ID,
            &[(url_encode(receipt_id)).to_string()],
        );
        let response: serde_json::Value = self.get_json(&path).await?;
        Ok(response
            .get("execution_receipt")
            .cloned()
            .unwrap_or(response))
    }

    pub async fn connector_accounts(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_CONNECTORS_ACCOUNTS.template())
            .await
    }

    pub async fn connector_capabilities(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_CONNECTORS_CAPABILITIES.template())
            .await
    }

    pub async fn connector_resources(
        &self,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let mut path = crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_CONNECTORS_RESOURCES,
            &[],
            &format!("limit={limit}&offset={offset}"),
        );
        if let Some(query) = query.map(str::trim).filter(|query| !query.is_empty()) {
            path.push_str("&q=");
            path.push_str(&url_encode(query));
        }
        self.get_json(&path).await
    }

    pub async fn message_connectors(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MESSAGE_CONNECTORS.template())
            .await
    }

    pub async fn message_connector_status(
        &self,
        name: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_MESSAGE_CONNECTORS_BY_NAME_STATUS,
            &[(url_encode(name)).to_string()],
        ))
        .await
    }

    pub async fn message_connector_repair(
        &self,
        name: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_MESSAGE_CONNECTORS_BY_NAME_REPAIR,
                &[(url_encode(name)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn message_endpoints(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MESSAGE_ENDPOINTS.template())
            .await
    }

    pub async fn message_routes(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MESSAGE_ROUTES.template())
            .await
    }

    pub async fn message_bindings(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_MESSAGE_BINDINGS.template())
            .await
    }

    pub async fn surface_registry(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_SURFACES.template())
            .await
    }

    pub async fn surface_health_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_SURFACES_HEALTH.template())
            .await
    }

    pub async fn surface_detail(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_routes(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_ROUTES,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_resources(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_RESOURCES,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_status(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_STATUS,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_health(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_HEALTH,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_health_check(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_HEALTH_CHECK,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_start(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_START,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_stop(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_STOP,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_restart(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_RESTART,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_repair(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_REPAIR,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_events(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_EVENTS,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_inbox(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_INBOX,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_outbox(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_OUTBOX,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_messages(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_MESSAGES,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn surface_archive_messages(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_MESSAGES_ARCHIVE,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({ "limit": limit }),
        )
        .await
    }

    pub async fn surface_purge_archived_events(
        &self,
        id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_MESSAGES_PURGE_ARCHIVED_EVENTS,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({ "limit": limit }),
        )
        .await
    }

    pub async fn surface_deliveries(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(
            &crate::gateway_client_routes::platform::for_platform_entity(
                surface::gateway_api::paths::API_SURFACES_BY_ID_DELIVERIES,
                url_encode(id),
            ),
        )
        .await
    }

    pub async fn surface_outbox_delivery(
        &self,
        id: &str,
        delivery_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SURFACES_BY_ID_OUTBOX_BY_DELIVERY_ID,
            &[
                (url_encode(id)).to_string(),
                (url_encode(delivery_id)).to_string(),
            ],
        ))
        .await
    }

    pub async fn surface_replay_inbox(
        &self,
        id: &str,
        message_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_INBOX_BY_MESSAGE_ID_REPLAY,
                &[
                    (url_encode(id)).to_string(),
                    (url_encode(message_id)).to_string(),
                ],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_retry_outbox(
        &self,
        id: &str,
        delivery_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_OUTBOX_BY_DELIVERY_ID_RETRY,
                &[
                    (url_encode(id)).to_string(),
                    (url_encode(delivery_id)).to_string(),
                ],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn surface_dead_letter_outbox(
        &self,
        id: &str,
        delivery_id: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_OUTBOX_BY_DELIVERY_ID_DEAD_LETTER,
                &[
                    (url_encode(id)).to_string(),
                    (url_encode(delivery_id)).to_string(),
                ],
            ),
            serde_json::json!({ "reason": reason }),
        )
        .await
    }

    pub async fn surface_send(
        &self,
        id: &str,
        recipient: &str,
        thread: Option<&str>,
        text: &str,
        metadata: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_SEND,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({
                "recipient": recipient,
                "thread": thread,
                "text": text,
                "metadata": metadata,
            }),
        )
        .await
    }

    pub async fn surface_action(
        &self,
        id: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_SURFACES_BY_ID_ACTION,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({
                "action": action,
                "payload": payload,
            }),
        )
        .await
    }

    pub async fn skill_runs(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_SKILLS_RUNS.template())
            .await
    }

    pub async fn skill_projection(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::route_with_query(
            surface::gateway_api::paths::API_SKILLS_PROJECTION,
            &[],
            "surface=tui",
        ))
        .await
    }

    pub async fn skill_run_detail(&self, id: &str) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_SKILLS_RUNS_BY_ID,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn skill_action(
        &self,
        id: &str,
        action: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let path = match action {
            "plan" => surface::gateway_api::paths::API_SKILLS_BY_ID_ACTIONS_PLAN,
            "run" => surface::gateway_api::paths::API_SKILLS_BY_ID_ACTIONS_RUN,
            "validate" => surface::gateway_api::paths::API_SKILLS_BY_ID_ACTIONS_VALIDATE,
            unsupported => {
                return Err(GatewayApiError::Contract(format!(
                    "unsupported skill action `{unsupported}`"
                )))
            }
        };
        self.post_json(
            &crate::gateway_client_routes::render_route(path, &[url_encode(id)]),
            payload,
        )
        .await
    }

    pub async fn harness_eval_latest_report(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_HARNESS_EVAL_REPORTS_LATEST.template())
            .await
    }

    pub async fn harness_eval_reports(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_HARNESS_EVAL_REPORTS.template())
            .await
    }

    pub async fn harness_eval_report(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_HARNESS_EVAL_REPORTS_BY_ID,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn harness_eval_report_artifacts(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_HARNESS_EVAL_REPORTS_BY_ID_ARTIFACTS,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn harness_eval_report_gate(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_HARNESS_EVAL_REPORTS_BY_ID_GATE,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn harness_eval_run_status(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_HARNESS_EVAL_RUNS_BY_ID,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn harness_eval_run_smoke(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.harness_eval_run(
            "quick",
            "low",
            false,
            "operator requested harness eval smoke",
        )
        .await
    }

    pub async fn harness_eval_run(
        &self,
        level: &str,
        budget: &str,
        allow_real_model: bool,
        objective: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_HARNESS_EVAL_RUNS.template(),
            serde_json::json!({
                "level": level,
                "budget": budget,
                "actor": "tui.gateway_panel",
                "objective": objective,
                "allow_real_model": allow_real_model
            }),
        )
        .await
    }

    pub async fn harness_eval_cancel_run(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_HARNESS_EVAL_RUNS_BY_ID_CANCEL,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_signals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_SIGNALS.template())
            .await
    }

    pub async fn evolution_overview(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_OVERVIEW.template())
            .await
    }

    pub async fn evolution_case_detail(
        &self,
        case_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_EVOLUTION_CASES_BY_ID,
            &[(url_encode(case_id)).to_string()],
        ))
        .await
    }

    pub async fn evolution_analyze_case(
        &self,
        case_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_EVOLUTION_CASES_BY_ID_ANALYZE,
                &[(url_encode(case_id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_diagnoses(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_DIAGNOSES.template())
            .await
    }

    pub async fn evolution_missions_summary(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_MISSIONS_SUMMARY.template())
            .await
    }

    pub async fn evolution_mission_detail(
        &self,
        mission_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_EVOLUTION_MISSIONS_BY_ID_DETAIL,
            &[(url_encode(mission_id)).to_string()],
        ))
        .await
    }

    pub async fn evolution_create_diagnosis(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_EVOLUTION_DIAGNOSES.template(),
            serde_json::json!({ "signal_ids": signal_ids }),
        )
        .await
    }

    pub async fn evolution_proposals(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_PROPOSALS.template())
            .await
    }

    pub async fn evolution_create_proposal(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_EVOLUTION_PROPOSALS.template(),
            serde_json::json!({ "signal_ids": signal_ids }),
        )
        .await
    }

    pub async fn evolution_skill_draft(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_EVOLUTION_PROPOSALS_BY_ID_SKILL_DRAFT,
            &[(url_encode(proposal_id)).to_string()],
        ))
        .await
    }

    pub async fn evolution_chain(
        &self,
        proposal_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_EVOLUTION_CHAIN_BY_ID,
            &[(url_encode(proposal_id)).to_string()],
        ))
        .await
    }

    pub async fn evolution_candidates(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_CANDIDATES.template())
            .await
    }

    pub async fn evolution_candidate_detail(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_EVOLUTION_CANDIDATES_BY_ID,
            &[(url_encode(candidate_id)).to_string()],
        ))
        .await
    }

    pub async fn evolution_create_candidate(
        &self,
        registration: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_EVOLUTION_CANDIDATES.template(),
            registration,
        )
        .await
    }

    pub async fn evolution_candidate_canary_review(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_EVOLUTION_CANDIDATES_BY_ID_REVIEWS_CANARY,
                &[(url_encode(candidate_id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    /// Ask Runtime's trusted evaluator to evaluate one immutable candidate.
    /// This endpoint never accepts a caller-supplied verdict or report.
    pub async fn evolution_candidate_evaluate(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_EVOLUTION_CANDIDATES_BY_ID_EVALUATE,
                &[(url_encode(candidate_id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_candidate_stable_review(
        &self,
        candidate_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_EVOLUTION_CANDIDATES_BY_ID_REVIEWS_STABLE,
                &[(url_encode(candidate_id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn evolution_reviews(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_REVIEWS.template())
            .await
    }

    pub async fn evolution_review_detail(
        &self,
        review_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_EVOLUTION_REVIEWS_BY_ID,
            &[(url_encode(review_id)).to_string()],
        ))
        .await
    }

    /// Queue pointer, rollback, or stop-Canary change through Runtime's
    /// typed review gate. TUI cannot mutate a release directly.
    pub async fn evolution_create_release_review(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_EVOLUTION_REVIEWS.template(),
            request,
        )
        .await
    }

    pub async fn evolution_review_decision(
        &self,
        review_id: &str,
        decision: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_EVOLUTION_REVIEWS_BY_ID_DECISION,
                &[(url_encode(review_id)).to_string()],
            ),
            serde_json::json!({ "decision": decision, "reason": reason }),
        )
        .await
    }

    /// Read Runtime's protected evaluation-policy floor. The terminal never
    /// computes a release verdict or keeps a policy cache of its own.
    pub async fn evolution_evaluation_policy(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_EVOLUTION_EVALUATION_POLICY.template())
            .await
    }

    pub async fn evolution_evaluation_policy_reviews(
        &self,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(
            surface::gateway_api::paths::API_EVOLUTION_EVALUATION_POLICY_REVIEWS.template(),
        )
        .await
    }

    pub async fn evolution_evaluation_policy_review_decision(
        &self,
        review_id: &str,
        decision: &str,
        reason: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_EVOLUTION_EVALUATION_POLICY_REVIEWS_BY_ID_DECISION,
                &[(url_encode(review_id)).to_string()],
            ),
            serde_json::json!({ "decision": decision, "reason": reason }),
        )
        .await
    }

    /// Runtime-owned Managed Agent projection. This is deliberately a single
    /// aggregate read so TUI cannot stitch a second scheduler state together.
    pub async fn managed_agents(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_RUNTIME_MANAGED_AGENTS.template())
            .await
    }

    pub async fn dispatch_managed_agents(
        &self,
        dispatcher_id: &str,
        limit: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_RUNTIME_MANAGED_AGENTS_DISPATCH.template(),
            serde_json::json!({ "dispatcher_id": dispatcher_id, "limit": limit }),
        )
        .await
    }

    pub async fn trigger_managed_agent(
        &self,
        managed_agent_id: &str,
        request_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_RUNTIME_MANAGED_AGENTS_BY_ID_TRIGGER,
                &[(url_encode(managed_agent_id)).to_string()],
            ),
            serde_json::json!({ "request_id": request_id }),
        )
        .await
    }

    pub async fn reset_managed_agent_health(
        &self,
        managed_agent_id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_RUNTIME_MANAGED_AGENTS_BY_ID_HEALTH_RESET,
                &[(url_encode(managed_agent_id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn connector_service_tools(
        &self,
        service: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_CONNECTORS_SERVICES_BY_SERVICE_ID_TOOLS,
            &[(url_encode(service)).to_string()],
        ))
        .await
    }

    pub async fn execute_connector_service(
        &self,
        service: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_CONNECTORS_SERVICES_BY_SERVICE_ID_EXECUTE,
                &[(url_encode(service)).to_string()],
            ),
            request,
        )
        .await
    }

    pub async fn revalidate_connector_resource(
        &self,
        reference: &str,
        state: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_CONNECTORS_RESOURCES_REVALIDATE.template(),
            serde_json::json!({
                "reference": reference,
                "state": state,
            }),
        )
        .await
    }

    pub async fn promote_connector_resource_to_memory(
        &self,
        reference: &str,
        session_id: Option<&str>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_CONNECTORS_RESOURCES_PROMOTE_MEMORY.template(),
            serde_json::json!({
                "reference": reference,
                "session_id": session_id,
            }),
        )
        .await
    }

    pub async fn preflight_cross_plane_action(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_CROSS_PLANE_ACTION_PREFLIGHT.template(),
            action,
        )
        .await
    }

    pub async fn execute_cross_plane_action(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_CROSS_PLANE_ACTION_EXECUTE.template(),
            request,
        )
        .await
    }

    pub async fn cross_plane_policy_simulate(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_CROSS_PLANE_POLICY_SIMULATE.template(),
            action,
        )
        .await
    }
}

fn take_gateway_sse_frame(buffer: &mut Vec<u8>) -> Result<Option<String>, GatewayApiError> {
    const MAX_GATEWAY_SSE_FRAME_BYTES: usize = 2 * 1024 * 1024;
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    let Some((index, delimiter_len)) = (match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf <= crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }) else {
        if buffer.len() > MAX_GATEWAY_SSE_FRAME_BYTES {
            return Err(GatewayApiError::Url(
                "Gateway SSE frame exceeded the 2 MiB transport limit".to_string(),
            ));
        }
        return Ok(None);
    };
    if index > MAX_GATEWAY_SSE_FRAME_BYTES {
        return Err(GatewayApiError::Url(
            "Gateway SSE frame exceeded the 2 MiB transport limit".to_string(),
        ));
    }
    let frame = buffer[..index].to_vec();
    buffer.drain(..index + delimiter_len);
    String::from_utf8(frame)
        .map(Some)
        .map_err(|_| GatewayApiError::Url("Gateway SSE stream emitted invalid UTF-8".to_string()))
}

fn app_method(method: &str) -> Result<reqwest::Method, AppTransportFailure> {
    let method = method.trim().to_ascii_uppercase();
    reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| AppTransportFailure {
        status: None,
        body: None,
        message: format!("APP request method is invalid: {method}"),
    })
}

fn validate_app_path(path: &str) -> Result<(), AppTransportFailure> {
    let path_without_query = path.split_once('?').map_or(path, |(path, _)| path);
    let percent_lower = path_without_query.to_ascii_lowercase();
    let traversal = path_without_query
        .split('/')
        .any(|segment| matches!(segment, "." | ".."))
        || percent_lower.contains("%2e");
    if path_without_query.starts_with(surface::gateway_api::API_PREFIX)
        && !path.contains("://")
        && !path.contains('\\')
        && !path.contains('\r')
        && !path.contains('\n')
        && !path.contains('#')
        && !traversal
    {
        return Ok(());
    }
    Err(AppTransportFailure {
        status: None,
        body: None,
        message: "APP request path must be a Gateway-local /api/ path".to_string(),
    })
}

fn validate_app_route_identifier(value: &str, maximum: usize) -> Result<(), AppTransportFailure> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(AppTransportFailure {
            status: None,
            body: None,
            message: "APP route identifier is invalid".to_string(),
        });
    }
    Ok(())
}

fn app_view_stream_path(app_id: &str, view_id: &str) -> String {
    crate::gateway_client_routes::render_route(
        surface::gateway_api::paths::API_APPS_BY_APP_ID_TUI_VIEWS_BY_VIEW_ID_STREAM,
        &[(app_id).to_string(), (view_id).to_string()],
    )
}

fn app_headers(
    headers: &BTreeMap<String, String>,
) -> Result<Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>, AppTransportFailure> {
    const RESERVED: &[&str] = &[
        "authorization",
        "cookie",
        "host",
        "content-length",
        "content-type",
        "accept",
        "x-cowd-surface-id",
        "x-cowd-observer-id",
    ];
    if headers.len() > 32 {
        return Err(AppTransportFailure {
            status: None,
            body: None,
            message: "APP request supplied too many headers".to_string(),
        });
    }
    headers
        .iter()
        .map(|(name, value)| {
            let normalized = name.trim().to_ascii_lowercase();
            if RESERVED.contains(&normalized.as_str()) {
                return Err(AppTransportFailure {
                    status: None,
                    body: None,
                    message: format!(
                        "APP request attempted to override reserved header {normalized}"
                    ),
                });
            }
            let name =
                reqwest::header::HeaderName::from_bytes(normalized.as_bytes()).map_err(|_| {
                    AppTransportFailure {
                        status: None,
                        body: None,
                        message: format!("APP request header is invalid: {normalized}"),
                    }
                })?;
            let value =
                reqwest::header::HeaderValue::from_str(value).map_err(|_| AppTransportFailure {
                    status: None,
                    body: None,
                    message: format!("APP request header value is invalid: {normalized}"),
                })?;
            Ok((name, value))
        })
        .collect()
}

fn decode_app_json_or_text(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).to_string()))
}

fn app_transport_failure(error: impl fmt::Display) -> AppTransportFailure {
    AppTransportFailure {
        status: None,
        body: None,
        message: error.to_string(),
    }
}

fn gateway_listener_reachable(base_url: &str) -> bool {
    let normalized = match normalize_base_url(base_url.to_string()) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let without_scheme = normalized
        .strip_prefix("http://")
        .or_else(|| normalized.strip_prefix("https://"))
        .unwrap_or(&normalized);
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let Ok(addrs) = host_port.to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok())
}

pub fn default_auth_token() -> Option<String> {
    std::env::var("COWD_API_TOKEN")
        .ok()
        .or_else(|| std::env::var("COWD_AUTH_TOKEN").ok())
        .or_else(|| {
            let home = std::env::var("HOME").ok()?;
            let config_path = std::path::PathBuf::from(home)
                .join(".cowd")
                .join("config.yaml");
            let config = std::fs::read_to_string(&config_path).ok()?;
            for line in config.lines() {
                let trimmed = line.trim();
                if trimmed.starts_with("token:") {
                    let token = trimmed.strip_prefix("token:")?.trim().trim_matches('"');
                    if !token.is_empty() {
                        return Some(token.to_string());
                    }
                }
            }
            None
        })
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
}

fn normalize_base_url(mut base_url: String) -> Result<String, GatewayApiError> {
    if base_url.trim().is_empty() {
        return Err(GatewayApiError::Url(
            "empty Gateway API base URL".to_string(),
        ));
    }
    base_url = base_url.trim().trim_end_matches('/').to_string();
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return Err(GatewayApiError::Url(format!(
            "Gateway API base URL must start with http:// or https://: {base_url}"
        )));
    }
    Ok(base_url)
}

pub(crate) fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{b:02X}").chars().collect(),
        })
        .collect()
}

fn gateway_status_error(status: reqwest::StatusCode, body: String) -> GatewayApiError {
    GatewayApiError::Status(status, body)
}

impl fmt::Display for GatewayApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(err) => write!(f, "Gateway API HTTP failed: {err}"),
            Self::Status(status, body) => {
                write!(f, "Gateway API returned {status}: {body}")
            }
            Self::SessionAuthorizationRevoked(reason) => {
                write!(f, "Gateway revoked session authorization: {reason}")
            }
            Self::Contract(err) => write!(f, "Gateway projection contract error: {err}"),
            Self::Url(err) => write!(f, "Gateway API URL error: {err}"),
        }
    }
}

impl std::error::Error for GatewayApiError {}

fn require_gateway_operation_ok(
    value: serde_json::Value,
    operation: &str,
) -> Result<serde_json::Value, GatewayApiError> {
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        return Ok(value);
    }
    let reason = value
        .get("error")
        .and_then(serde_json::Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or("Gateway returned an unsuccessful operation receipt");
    Err(GatewayApiError::Url(format!(
        "{operation} failed: {reason}"
    )))
}

fn require_gateway_session_operation_ok(
    value: serde_json::Value,
    operation: &str,
    requested_session_id: &str,
) -> Result<serde_json::Value, GatewayApiError> {
    let value = require_gateway_operation_ok(value, operation)?;
    validate_session_json_identity(
        requested_session_id,
        &value,
        &format!("{operation} receipt"),
    )?;
    Ok(value)
}

fn string_field(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn gateway_event_correlation(
    value: &serde_json::Value,
    session_id: Option<&str>,
    part_id: Option<String>,
) -> GatewayEventCorrelation {
    GatewayEventCorrelation {
        session_id: string_field(value, "session_id")
            .or_else(|| session_id.map(ToOwned::to_owned))
            .unwrap_or_default(),
        execution_id: string_field(value, "execution_id"),
        turn_id: string_field(value, "turn_id"),
        part_id: string_field(value, "part_id").or(part_id),
        model_step_id: string_field(value, "model_step_id"),
        item_id: string_field(value, "item_id"),
        segment_id: string_field(value, "segment_id"),
        tool_call_id: string_field(value, "tool_call_id"),
        causal_sequence: value
            .get("causal_sequence")
            .and_then(serde_json::Value::as_u64),
        delta_sequence: value
            .get("delta_sequence")
            .and_then(serde_json::Value::as_u64),
        causal_parent_ids: value
            .get("causal_parent_ids")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        message_id: string_field(value, "message_id"),
        terminal_id: string_field(value, "terminal_id"),
        commit_cursor: value
            .get("runtime_commit_cursor")
            .and_then(serde_json::Value::as_u64),
        replayed: value
            .get("replayed")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    }
}

fn execution_live_status(
    value: &serde_json::Value,
) -> Option<harness_contract::projection::ExecutionLiveStatus> {
    let normalized = value.as_str()?.trim().to_ascii_lowercase();
    use harness_contract::projection::ExecutionLiveStatus;
    match normalized.as_str() {
        "queued" => Some(ExecutionLiveStatus::Queued),
        "preparingcontext" | "preparing_context" => Some(ExecutionLiveStatus::PreparingContext),
        "callingmodel" | "calling_model" => Some(ExecutionLiveStatus::CallingModel),
        "thinking" => Some(ExecutionLiveStatus::Thinking),
        "callingtool" | "calling_tool" => Some(ExecutionLiveStatus::CallingTool),
        "waitingapproval" | "waiting_approval" => Some(ExecutionLiveStatus::WaitingApproval),
        "finalizing" => Some(ExecutionLiveStatus::Finalizing),
        "complete" | "completed" => Some(ExecutionLiveStatus::Complete),
        "cancelled" | "canceled" => Some(ExecutionLiveStatus::Cancelled),
        "error" | "failed" => Some(ExecutionLiveStatus::Error),
        _ => None,
    }
}

async fn hydrate_session_history_with_retry(
    client: &GatewayApiClient,
    session_id: &str,
    tx: CowdEventSender,
    next_sequence: Arc<AtomicUsize>,
    authority_generation: u64,
) {
    let mut retry_delay = Duration::from_millis(250);
    let mut attempt = 0u32;
    loop {
        let from_message_sequence = next_sequence.load(Ordering::Acquire);
        match hydrate_session_history_once(
            client,
            session_id,
            tx.clone(),
            from_message_sequence,
            &next_sequence,
            authority_generation,
        )
        .await
        {
            Ok(hydrated_to) => {
                next_sequence.fetch_max(hydrated_to, Ordering::AcqRel);
                return;
            }
            Err(error) => {
                attempt = attempt.saturating_add(1);
                if attempt == 1 || attempt % 12 == 0 {
                    let _ = tx.send(session_scoped_event(
                        session_id,
                        authority_generation,
                        CowdEvent::SessionHistoryHydrationFailed {
                            session_id: session_id.to_string(),
                            error: format!("{error}; retry attempt {attempt}"),
                        },
                    ));
                }
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
            }
        }
    }
}

fn session_scoped_event(
    session_id: &str,
    authority_generation: u64,
    event: CowdEvent,
) -> CowdEvent {
    CowdEvent::SessionScoped {
        session_id: session_id.to_string(),
        authority_generation,
        event: Box::new(event),
    }
}

async fn deliver_session_stream_event(
    tx: &CowdEventSender,
    session_id: &str,
    event: CowdEvent,
    authority_generation: u64,
) -> Result<(), GatewayApiError> {
    let event = session_scoped_event(session_id, authority_generation, event);
    // The shared live multiplexer has already applied the delivery-class
    // policy: ephemeral previews may be coalesced there, while durable and
    // reconstructable envelopes are lossless. Once an envelope enters a
    // session bridge, queue pressure must backpressure that bridge instead of
    // turning an otherwise healthy live source into a reconnect. In
    // particular, dropping TextDelta here makes every Surface see only the
    // terminal replay during event bursts.
    tx.send_wait(event)
        .await
        .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))
}

async fn deliver_session_stream_event_with_catchup(
    client: &GatewayApiClient,
    tx: &CowdEventSender,
    session_id: &str,
    event: CowdEvent,
    next_message_sequence: &Arc<AtomicUsize>,
    authority_generation: u64,
) -> Result<(), GatewayApiError> {
    if let CowdEvent::GatewaySession {
        event: GatewaySessionEvent::UserMessageCommitted { sequence, .. },
    } = &event
    {
        next_message_sequence.fetch_max(sequence.saturating_add(1), Ordering::AcqRel);
    }
    let terminal = matches!(
        &event,
        CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalCommitted { .. }
        }
    );
    deliver_session_stream_event(tx, session_id, event, authority_generation).await?;
    if terminal {
        let catchup_from = next_message_sequence.load(Ordering::Acquire);
        if let Err(error) = hydrate_session_history_once(
            client,
            session_id,
            tx.clone(),
            catchup_from,
            next_message_sequence.as_ref(),
            authority_generation,
        )
        .await
        {
            let _ = tx.send(session_scoped_event(
                session_id,
                authority_generation,
                CowdEvent::Warning {
                    message: format!(
                        "Terminal committed, but durable transcript catch-up failed: {error}"
                    ),
                },
            ));
        }
    }
    Ok(())
}

async fn hydrate_session_history_once(
    client: &GatewayApiClient,
    session_id: &str,
    tx: CowdEventSender,
    mut from_sequence: usize,
    accepted_sequence: &AtomicUsize,
    authority_generation: u64,
) -> Result<usize, GatewayApiError> {
    // Keep the initial TUI window large enough for local interactive search
    // across a substantial working history, while the body-free index and
    // explicit paging still bound sessions larger than this window.
    const HISTORY_WINDOW_CAP: usize = 10_000;
    let started = std::time::Instant::now();
    let hydration_kind = if from_sequence == 0 {
        crate::protocol::SessionHistoryHydrationKind::InitialWindow
    } else {
        crate::protocol::SessionHistoryHydrationKind::IncrementalCatchup
    };
    let mut message_count = 0usize;
    let mut page_count = 0usize;
    let mut oldest_offset = 0usize;
    let mut total_messages = 0usize;
    let mut has_older = false;

    if from_sequence == 0 {
        // Materialize the body-free index first, then fetch only the bounded
        // transcript tail. Older bodies remain available through explicit
        // paging and exact reads.
        let history_index = client.session_history_index(session_id).await?;
        total_messages = history_index.total_messages as usize;
        tx.send_wait(session_scoped_event(
            session_id,
            authority_generation,
            CowdEvent::SessionHistoryIndexLoaded {
                projection: history_index,
            },
        ))
        .await
        .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
        oldest_offset = total_messages.saturating_sub(HISTORY_WINDOW_CAP);
        has_older = oldest_offset > 0;
        let mut offset = oldest_offset;
        loop {
            let page = client
                .session_messages_offset(session_id, offset, 500)
                .await?;
            let loaded = page.messages.len();
            let next_sequence = page.next_seq;
            let page_has_more = page.has_more;
            total_messages = total_messages.max(page.total);
            message_count = message_count.saturating_add(loaded);
            page_count = page_count.saturating_add(1);
            tx.send_wait(session_scoped_event(
                session_id,
                authority_generation,
                CowdEvent::SessionHistoryPage { page },
            ))
            .await
            .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
            if let Some(next) = next_sequence {
                from_sequence = from_sequence.max(next);
                accepted_sequence.fetch_max(from_sequence, Ordering::AcqRel);
            }
            if !page_has_more {
                break;
            }
            if loaded == 0 {
                return Err(GatewayApiError::Url(
                    "Gateway returned a non-advancing history offset".to_string(),
                ));
            }
            offset = offset.saturating_add(loaded);
        }
    } else {
        // Reconnect hydration starts at the last accepted durable sequence and
        // fetches only messages committed while the Surface was away.
        loop {
            let page = client
                .session_messages(session_id, from_sequence, 500)
                .await?;
            let next_sequence = page.next_seq;
            let has_more = page.has_more;
            total_messages = total_messages.max(page.total);
            message_count = message_count.saturating_add(page.messages.len());
            page_count = page_count.saturating_add(1);
            tx.send_wait(session_scoped_event(
                session_id,
                authority_generation,
                CowdEvent::SessionHistoryCatchupPage { page },
            ))
            .await
            .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
            if let Some(next) = next_sequence {
                accepted_sequence.fetch_max(next, Ordering::AcqRel);
            }
            if !has_more {
                from_sequence = next_sequence.unwrap_or(from_sequence);
                break;
            }
            let Some(next_sequence) = next_sequence.filter(|next| *next > from_sequence) else {
                return Err(GatewayApiError::Url(
                    "Gateway returned a non-advancing history cursor".to_string(),
                ));
            };
            from_sequence = next_sequence;
        }
    }
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tx.send_wait(session_scoped_event(
        session_id,
        authority_generation,
        CowdEvent::SessionHistoryHydrated {
            session_id: session_id.to_string(),
            kind: hydration_kind,
            duration_ms,
            message_count,
            page_count,
            oldest_offset,
            total_messages,
            next_sequence: from_sequence,
            has_older,
        },
    ))
    .await
    .map_err(|_| GatewayApiError::Url("TUI event receiver closed".to_string()))?;
    tracing::info!(
        session_id,
        duration_ms,
        message_count,
        page_count,
        oldest_offset,
        total_messages,
        has_older,
        "TUI durable history hydration completed"
    );
    Ok(from_sequence)
}

pub(crate) fn gateway_sse_json_to_cowd_event_for_session(
    value: &serde_json::Value,
    session_id: Option<&str>,
) -> Option<CowdEvent> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("event_type"))
        .and_then(serde_json::Value::as_str)?;
    gateway_sse_json_to_conversation_event(value, session_id, event_type)
        .or_else(|| gateway_sse_json_to_session_control_event(value, session_id, event_type))
}

fn gateway_sse_json_to_conversation_event(
    value: &serde_json::Value,
    session_id: Option<&str>,
    event_type: &str,
) -> Option<CowdEvent> {
    match event_type {
        "UserMessageCommitted" | "user_message_committed" => {
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::UserMessageCommitted {
                    correlation,
                    content: value
                        .get("content")
                        .and_then(serde_json::Value::as_str)?
                        .to_string(),
                    sequence: value.get("sequence").and_then(serde_json::Value::as_u64)? as usize,
                    created_at_ms: value
                        .get("created_at_ms")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default(),
                },
            })
        }
        "TextDelta" | "text_delta" | "assistant_delta" => {
            let text = value
                .get("text")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, None);
            let start_bytes = value.get("start_bytes")?.as_u64()? as usize;
            let end_bytes = value.get("end_bytes")?.as_u64()? as usize;
            let stream_revision = value.get("stream_revision")?.as_u64()?;
            if end_bytes < start_bytes || end_bytes.saturating_sub(start_bytes) != text.len() {
                return None;
            }
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TextDelta {
                    correlation,
                    text,
                    start_bytes,
                    end_bytes,
                    stream_revision,
                },
            })
        }
        "TerminalDelivery" | "terminal_delivery" => {
            let delivery = serde_json::from_value::<harness_contract::live::TerminalDeliveryEvent>(
                value.get("delivery")?.clone(),
            )
            .ok()?;
            let correlation = match &delivery {
                harness_contract::live::TerminalDeliveryEvent::CancellationCommitted {
                    receipt,
                } => {
                    let mut correlation = gateway_event_correlation(
                        value,
                        session_id.or(Some(receipt.session_id.as_str())),
                        None,
                    );
                    if correlation.execution_id.is_none() && !receipt.execution_id.is_empty() {
                        correlation.execution_id = Some(receipt.execution_id.clone());
                    }
                    if correlation.turn_id.is_none() && !receipt.turn_id.is_empty() {
                        correlation.turn_id = Some(receipt.turn_id.clone());
                    }
                    correlation
                }
                _ => gateway_event_correlation(value, session_id, None),
            };
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TerminalDelivery {
                    correlation,
                    delivery,
                },
            })
        }
        "ReasoningSummaryDelta" | "reasoning_summary_delta" => {
            let summary = value
                .get("summary")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ReasoningSummaryDelta {
                    correlation,
                    summary,
                },
            })
        }
        "ModelStepStarted" | "model_step_started" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ModelStepStarted {
                correlation: gateway_event_correlation(value, session_id, None),
            },
        }),
        "ModelStepCompleted" | "model_step_completed" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ModelStepCompleted {
                correlation: gateway_event_correlation(value, session_id, None),
                status: value
                    .get("status")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("complete")
                    .to_string(),
            },
        }),
        "ItemStarted" | "item_started" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ItemStarted {
                correlation: gateway_event_correlation(value, session_id, None),
                kind: value
                    .get("kind")
                    .and_then(serde_json::Value::as_str)?
                    .to_string(),
            },
        }),
        "ItemCompleted" | "item_completed" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ItemCompleted {
                correlation: gateway_event_correlation(value, session_id, None),
                kind: value
                    .get("kind")
                    .and_then(serde_json::Value::as_str)?
                    .to_string(),
            },
        }),
        "ToolStart" | "tool_start" => {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.trim().is_empty())?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, Some(id.clone()));
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolStart {
                    correlation,
                    id,
                    name: value
                        .get("name")
                        .or_else(|| value.get("tool_name"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.trim().is_empty())?
                        .to_string(),
                    preview: value
                        .get("preview")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                },
            })
        }
        "ToolProgress" | "tool_progress" => {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.trim().is_empty())?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, Some(id.clone()));
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolProgress {
                    correlation,
                    id,
                    name: value
                        .get("name")
                        .or_else(|| value.get("tool_name"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.trim().is_empty())?
                        .to_string(),
                    progress: value
                        .get("progress")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                },
            })
        }
        "ToolComplete" | "tool_complete" => {
            let id = value
                .get("id")
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.trim().is_empty())?
                .to_string();
            let correlation = gateway_event_correlation(value, session_id, Some(id.clone()));
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ToolComplete {
                    correlation,
                    id,
                    name: value
                        .get("name")
                        .or_else(|| value.get("tool_name"))
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.trim().is_empty())?
                        .to_string(),
                    summary: value
                        .get("result_summary")
                        .or_else(|| value.get("summary"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    exit_code: value
                        .get("exit_code")
                        .and_then(serde_json::Value::as_i64)
                        .map(|code| code as i32),
                },
            })
        }
        _ => None,
    }
}

fn gateway_sse_json_to_session_control_event(
    value: &serde_json::Value,
    session_id: Option<&str>,
    event_type: &str,
) -> Option<CowdEvent> {
    match event_type {
        // A model loop completion is only rendering progress. The durable
        // SessionRuntimeBridge emits TerminalCommitted after the transcript
        // write succeeds; only that event is allowed to settle TUI state.
        "TurnComplete" | "turn_complete" => None,
        "TerminalCommitted" | "terminal_committed" => {
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TerminalCommitted {
                    correlation,
                    assistant_text: value
                        .get("assistant_text")
                        .or_else(|| value.get("response"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    sequence: value
                        .get("sequence")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok()),
                    iterations: value
                        .get("iterations")
                        .and_then(serde_json::Value::as_u64)
                        .map(|value| value as u32)
                        .unwrap_or_default(),
                    token_usage: value.get("token_usage").cloned(),
                },
            })
        }
        "TurnError" | "turn_error" => {
            let correlation = gateway_event_correlation(value, session_id, None);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::TurnError {
                    correlation,
                    error: value
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("Gateway turn error")
                        .to_string(),
                },
            })
        }
        "ExecutionPhase" | "execution_phase" => {
            let status = execution_live_status(value.get("status")?)?;
            let detail = value
                .get("detail")
                .and_then(serde_json::Value::as_str)
                .filter(|detail| !detail.trim().is_empty())
                .map(ToOwned::to_owned);
            Some(CowdEvent::GatewaySession {
                event: GatewaySessionEvent::ExecutionPhase {
                    correlation: gateway_event_correlation(value, session_id, None),
                    status,
                    detail,
                },
            })
        }
        "PermissionRevisionChanged" | "permission_revision_changed" => {
            Some(CowdEvent::PermissionRevisionChanged {
                permission_mode: value
                    .get("permission_mode")
                    .and_then(serde_json::Value::as_str)?
                    .to_string(),
                revision: value.get("revision").and_then(serde_json::Value::as_u64)?,
                applies_to_active_turn: value
                    .get("applies_to_active_turn")
                    .and_then(serde_json::Value::as_bool)?,
            })
        }
        "SessionInputReceived" | "session_input_received" => {
            if let Some(projection) = value.get("input_projection") {
                return Some(CowdEvent::SessionInputProjection {
                    projection: projection.clone(),
                });
            }
            let decision = value
                .get("receipt")
                .or_else(|| value.get("input"))
                .and_then(|receipt| receipt.get("decision"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("received");
            Some(CowdEvent::Warning {
                message: format!("Session input received: {decision}"),
            })
        }
        "SessionInputProjection" | "session_input_projection" => value
            .get("projection")
            .cloned()
            .map(|projection| CowdEvent::SessionInputProjection { projection }),
        "SessionInputDispositionChanged" | "session_input_disposition_changed" => value
            .get("receipt")
            .cloned()
            .map(|receipt| CowdEvent::SessionInputDispositionChanged { receipt }),
        "TurnInboxUpdated" | "turn_inbox_updated" => {
            // Older Gateway builds may publish an inbox without the adjacent
            // full projection. Treat its typed items as a bounded projection
            // instead of emitting a notice while leaving stale queue state on
            // screen.
            value.get("inbox").map(|inbox| CowdEvent::SessionInputProjection {
                projection: serde_json::json!({
                    "session_id": inbox.get("session_id").cloned().unwrap_or_default(),
                    "active_turn_id": inbox.get("turn_id").cloned().unwrap_or_default(),
                    "pending_count": inbox.get("pending_count").cloned().unwrap_or_default(),
                    "inputs": inbox.get("items").cloned().unwrap_or_else(|| serde_json::json!([])),
                }),
            })
        }
        "TurnInputCheckpointConsumed" | "turn_input_checkpoint_consumed" => {
            let checkpoint = value
                .get("checkpoint")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("checkpoint");
            let consumed = value
                .get("consumed")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            Some(CowdEvent::Warning {
                message: format!("Runtime consumed {consumed} input(s) at {checkpoint}"),
            })
        }
        "ContextEnvelope" | "context_envelope" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ContextEnvelope {
                correlation: gateway_event_correlation(value, session_id, None),
                envelope: value.get("envelope").cloned()?,
            },
        }),
        "TokenUsage" | "token_usage" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TokenUsage {
                correlation: gateway_event_correlation(value, session_id, None),
                input: value.get("input").and_then(serde_json::Value::as_u64)?,
                output: value.get("output").and_then(serde_json::Value::as_u64)?,
                cache_create: value
                    .get("cache_create")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
                cache_read: value
                    .get("cache_read")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or_default(),
            },
        }),
        "RunModelTelemetry" | "run_model_telemetry" => value
            .get("telemetry")
            .cloned()
            .and_then(|telemetry| serde_json::from_value(telemetry).ok())
            .map(|telemetry| CowdEvent::GatewaySession {
                event: GatewaySessionEvent::RunModelTelemetry {
                    correlation: gateway_event_correlation(value, session_id, None),
                    telemetry,
                },
            }),
        "ContextWindow" | "context_window" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ContextWindow {
                correlation: gateway_event_correlation(value, session_id, None),
                value: value.get("value").and_then(serde_json::Value::as_u64)?,
            },
        }),
        "ProviderAttempt" | "provider_attempt" => Some(CowdEvent::GatewaySession {
            event: GatewaySessionEvent::ProviderAttempt {
                correlation: gateway_event_correlation(value, session_id, None),
                model: value.get("model")?.as_str()?.to_string(),
                models_tried: value
                    .get("models_tried")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect(),
                context_window_tokens: value
                    .get("context_window_tokens")
                    .and_then(serde_json::Value::as_u64)?,
                context_window_source: value
                    .get("context_window_source")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                packed_input_tokens: value
                    .get("packed_input_tokens")
                    .and_then(serde_json::Value::as_u64)?,
            },
        }),
        "Warning" | "warning" => value
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(|message| CowdEvent::Warning {
                message: message.to_string(),
            }),
        "CompactionNotice" | "compaction_notice" => value
            .get("removed_count")
            .and_then(serde_json::Value::as_u64)
            .and_then(|count| usize::try_from(count).ok())
            .map(|removed_count| CowdEvent::CompactionNotice { removed_count }),
        "RuntimeEventEncodingError" => Some(CowdEvent::Warning {
            message: format!(
                "Gateway could not encode a Runtime event: {}",
                value
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown encoding failure")
            ),
        }),
        "RuntimePolicyDecision" | "runtime_policy_decision" => value
            .get("summary")
            .cloned()
            .and_then(|summary| serde_json::from_value(summary).ok())
            .map(|summary| CowdEvent::RuntimePolicyDecision { summary }),
        "ExecutionGraphSummary" | "execution_graph_summary" => value
            .get("summary")
            .cloned()
            .and_then(|summary| serde_json::from_value(summary).ok())
            .map(|summary| CowdEvent::ExecutionGraphSummary { summary }),
        _ => None,
    }
}

pub fn gateway_sse_json_to_cowd_event(value: &serde_json::Value) -> Option<CowdEvent> {
    gateway_sse_json_to_cowd_event_for_session(value, None)
}

pub fn gateway_sse_frame_to_cowd_event(frame: &str) -> Option<CowdEvent> {
    gateway_sse_frame_to_cowd_event_for_session(frame, "")
}

fn gateway_sse_frame_to_cowd_event_for_session(frame: &str, session_id: &str) -> Option<CowdEvent> {
    strict_gateway_sse_frame_to_cowd_event_for_session(frame, session_id)
        .ok()
        .flatten()
}

fn strict_gateway_sse_frame_to_cowd_event_for_session(
    frame: &str,
    session_id: &str,
) -> Result<Option<CowdEvent>, String> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() || data == "[DONE]" {
        return Ok(None);
    }
    let value = serde_json::from_str::<serde_json::Value>(&data)
        .map_err(|error| format!("invalid session event JSON: {error}"))?;
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .or_else(|| value.get("event_type"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "session event has no typed identity".to_string())?;
    let frame_cursor = gateway_sse_frame_commit_cursor(frame);
    validate_gateway_session_event_contract(&value, event_type, session_id, frame_cursor)?;
    if let Some(mut event) = gateway_sse_json_to_cowd_event_for_session(&value, Some(session_id)) {
        if let CowdEvent::GatewaySession {
            event: GatewaySessionEvent::TerminalCommitted { correlation, .. },
        } = &mut event
        {
            if correlation.commit_cursor.is_none() {
                correlation.commit_cursor = frame_cursor;
            }
        }
        return Ok(Some(event));
    }
    if matches!(
        event_type,
        "Connected"
            | "connected"
            | "TurnComplete"
            | "turn_complete"
            | "TurnStarted"
            | "turn_started"
            | "ToolExecuted"
            | "tool_executed"
            | "WriteAttemptsObserved"
            | "write_attempts_observed"
    ) {
        return Ok(None);
    }
    if matches!(
        event_type,
        "UserMessageCommitted"
            | "user_message_committed"
            | "TextDelta"
            | "text_delta"
            | "assistant_delta"
            | "TerminalDelivery"
            | "terminal_delivery"
            | "ReasoningSummaryDelta"
            | "reasoning_summary_delta"
            | "ModelStepStarted"
            | "model_step_started"
            | "ModelStepCompleted"
            | "model_step_completed"
            | "ItemStarted"
            | "item_started"
            | "ItemCompleted"
            | "item_completed"
            | "ToolStart"
            | "tool_start"
            | "ToolProgress"
            | "tool_progress"
            | "ToolComplete"
            | "tool_complete"
            | "TerminalCommitted"
            | "terminal_committed"
            | "TurnError"
            | "turn_error"
            | "ExecutionPhase"
            | "execution_phase"
            | "ContextEnvelope"
            | "context_envelope"
            | "TokenUsage"
            | "token_usage"
            | "RunModelTelemetry"
            | "run_model_telemetry"
            | "ContextWindow"
            | "context_window"
            | "ProviderAttempt"
            | "provider_attempt"
            | "Warning"
            | "warning"
            | "CompactionNotice"
            | "compaction_notice"
            | "RuntimePolicyDecision"
            | "runtime_policy_decision"
            | "PermissionRevisionChanged"
            | "permission_revision_changed"
            | "ExecutionGraphSummary"
            | "execution_graph_summary"
    ) {
        return Err(format!(
            "recognized session event `{event_type}` is missing or has invalid required fields"
        ));
    }
    Ok(Some(CowdEvent::Warning {
        message: format!(
            "Gateway session stream exposed an unsupported event type `{event_type}`; the event was not applied"
        ),
    }))
}

fn validate_gateway_session_event_contract(
    value: &serde_json::Value,
    event_type: &str,
    subscribed_session_id: &str,
    frame_cursor: Option<u64>,
) -> Result<(), String> {
    if value.get("session_id").is_some() {
        let explicit_session = string_field(value, "session_id").ok_or_else(|| {
            format!("`{event_type}` contains an empty or non-string `session_id`")
        })?;
        if !subscribed_session_id.trim().is_empty() && explicit_session != subscribed_session_id {
            return Err(format!(
                "`{event_type}` session `{explicit_session}` does not match subscribed session `{subscribed_session_id}`"
            ));
        }
    }
    let require_text = |field: &str| {
        string_field(value, field)
            .map(|_| ())
            .ok_or_else(|| format!("`{event_type}` requires non-empty `{field}`"))
    };
    let require_session = || {
        let event_session = string_field(value, "session_id")
            .or_else(|| {
                (!subscribed_session_id.trim().is_empty())
                    .then(|| subscribed_session_id.to_string())
            })
            .ok_or_else(|| format!("`{event_type}` requires non-empty `session_id`"))?;
        if !subscribed_session_id.trim().is_empty() && event_session != subscribed_session_id {
            return Err(format!(
                "`{event_type}` session `{event_session}` does not match subscribed session `{subscribed_session_id}`"
            ));
        }
        Ok::<(), String>(())
    };
    let require_execution = || {
        require_session()?;
        require_text("execution_id")?;
        require_text("turn_id")
    };
    let require_causal_item = || {
        require_execution()?;
        require_text("model_step_id")?;
        require_text("item_id")?;
        require_text("segment_id")?;
        value
            .get("causal_sequence")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("`{event_type}` requires integer `causal_sequence`"))?;
        value
            .get("delta_sequence")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("`{event_type}` requires integer `delta_sequence`"))?;
        Ok::<(), String>(())
    };
    match event_type {
        "UserMessageCommitted" | "user_message_committed" => {
            require_execution()?;
            require_text("message_id")?;
            value
                .get("sequence")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `sequence`"))?;
            require_text("content")
        }
        "TextDelta" | "text_delta" | "assistant_delta" => {
            require_causal_item()?;
            require_text("part_id")?;
            let text = value
                .get("text")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("`{event_type}` requires string `text`"))?;
            let start = value
                .get("start_bytes")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `start_bytes`"))?;
            let end = value
                .get("end_bytes")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `end_bytes`"))?;
            value
                .get("stream_revision")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("`{event_type}` requires integer `stream_revision`"))?;
            if end < start || end.saturating_sub(start) != text.len() as u64 {
                return Err(format!(
                    "`{event_type}` byte range does not match UTF-8 text length"
                ));
            }
            Ok(())
        }
        "TerminalDelivery" | "terminal_delivery" => {
            let delivery_value = value
                .get("delivery")
                .cloned()
                .ok_or_else(|| format!("`{event_type}` requires `delivery`"))?;
            let delivery = serde_json::from_value::<harness_contract::live::TerminalDeliveryEvent>(
                delivery_value,
            )
            .map_err(|error| format!("`{event_type}` has invalid delivery: {error}"))?;
            match delivery {
                harness_contract::live::TerminalDeliveryEvent::CancellationCommitted {
                    receipt,
                } => {
                    if receipt.session_id != subscribed_session_id
                        || receipt.cancellation_id.trim().is_empty()
                        || receipt.requested_at_ms == 0
                    {
                        return Err(format!(
                            "`{event_type}` has an invalid cancellation receipt identity"
                        ));
                    }
                    Ok(())
                }
                harness_contract::live::TerminalDeliveryEvent::TextDelta {
                    presentation_id,
                    attempt_id,
                    byte_start,
                    byte_end,
                    delta,
                } => {
                    require_execution()?;
                    if presentation_id.trim().is_empty()
                        || attempt_id.trim().is_empty()
                        || byte_end < byte_start
                        || byte_end.saturating_sub(byte_start) != delta.len() as u64
                    {
                        return Err(format!("`{event_type}` has an invalid presentation delta"));
                    }
                    Ok(())
                }
                harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                    presentation_id,
                    attempt_id,
                    envelope_id,
                    ..
                } => {
                    require_execution()?;
                    if presentation_id.trim().is_empty()
                        || attempt_id.trim().is_empty()
                        || envelope_id.trim().is_empty()
                    {
                        return Err(format!("`{event_type}` has an invalid presentation owner"));
                    }
                    Ok(())
                }
                harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                    presentation_id,
                    attempt_id,
                    ..
                }
                | harness_contract::live::TerminalDeliveryEvent::TerminalPresentationAborted {
                    presentation_id,
                    attempt_id,
                    ..
                }
                | harness_contract::live::TerminalDeliveryEvent::TerminalPresentationCommitted {
                    presentation_id,
                    attempt_id,
                    ..
                } => {
                    require_execution()?;
                    if presentation_id.trim().is_empty() || attempt_id.trim().is_empty() {
                        return Err(format!(
                            "`{event_type}` has an invalid presentation identity"
                        ));
                    }
                    Ok(())
                }
            }
        }
        "ReasoningSummaryDelta" | "reasoning_summary_delta" => {
            require_causal_item()?;
            require_text("part_id")?;
            value
                .get("summary")
                .or_else(|| value.get("content"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("`{event_type}` requires string `summary`"))
                .map(|_| ())
        }
        "ToolStart" | "tool_start" | "ToolProgress" | "tool_progress" | "ToolComplete"
        | "tool_complete" => {
            require_causal_item()?;
            require_text("part_id")?;
            require_text("tool_call_id")?;
            require_text("id")?;
            value
                .get("name")
                .or_else(|| value.get("tool_name"))
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| format!("`{event_type}` requires non-empty `name`"))?;
            Ok(())
        }
        "ModelStepStarted"
        | "model_step_started"
        | "ModelStepCompleted"
        | "model_step_completed" => {
            require_execution()?;
            require_text("model_step_id")
        }
        "ItemStarted" | "item_started" | "ItemCompleted" | "item_completed" => {
            require_causal_item()?;
            require_text("kind")
        }
        "TerminalCommitted" | "terminal_committed" => {
            require_execution()?;
            require_text("part_id")?;
            require_text("message_id")?;
            require_text("terminal_id")?;
            let payload_cursor = value
                .get("runtime_commit_cursor")
                .and_then(serde_json::Value::as_u64);
            if payload_cursor.or(frame_cursor).is_none() {
                return Err(format!(
                    "`{event_type}` requires an integer durable commit cursor"
                ));
            }
            value
                .get("assistant_text")
                .or_else(|| value.get("response"))
                .and_then(serde_json::Value::as_str)
                .filter(|response| !response.trim().is_empty())
                .ok_or_else(|| {
                    format!("`{event_type}` requires non-empty assistant terminal text")
                })?;
            Ok(())
        }
        "ExecutionPhase" | "execution_phase" | "TurnError" | "turn_error" => require_execution(),
        "PermissionRevisionChanged" | "permission_revision_changed" => {
            validate_permission_revision_event(value, event_type, &require_text)
        }
        "ProviderAttempt"
        | "provider_attempt"
        | "ContextEnvelope"
        | "context_envelope"
        | "ContextWindow"
        | "context_window"
        | "TokenUsage"
        | "token_usage"
        | "RunModelTelemetry"
        | "run_model_telemetry" => require_execution(),
        _ => Ok(()),
    }
}

fn validate_permission_revision_event(
    value: &serde_json::Value,
    event_type: &str,
    require_text: &impl Fn(&str) -> Result<(), String>,
) -> Result<(), String> {
    require_text("permission_mode")?;
    value
        .get("revision")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("`{event_type}` requires integer `revision`"))?;
    value
        .get("applies_to_active_turn")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| format!("`{event_type}` requires boolean `applies_to_active_turn`"))?;
    Ok(())
}

fn gateway_sse_frame_commit_cursor(frame: &str) -> Option<u64> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("id:"))
        .map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
}

fn validate_execution_projection_identity(
    requested_execution_id: &str,
    projected_execution_id: &str,
) -> Result<(), GatewayApiError> {
    if requested_execution_id == projected_execution_id {
        return Ok(());
    }
    Err(GatewayApiError::Contract(format!(
        "requested execution `{requested_execution_id}` but Gateway projected foreign execution `{projected_execution_id}`"
    )))
}

fn validate_session_json_identity(
    requested_session_id: &str,
    value: &serde_json::Value,
    contract: &str,
) -> Result<(), GatewayApiError> {
    validate_session_json_identity_at(requested_session_id, value, contract, &["/session_id"])
}

fn validate_session_json_identity_at(
    requested_session_id: &str,
    value: &serde_json::Value,
    contract: &str,
    pointers: &[&str],
) -> Result<(), GatewayApiError> {
    let projected_session_id = pointers
        .iter()
        .find_map(|pointer| {
            value
                .pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .filter(|session_id| !session_id.trim().is_empty())
        })
        .ok_or_else(|| {
            GatewayApiError::Contract(format!(
                "{contract} is missing a non-empty session identity at {}",
                pointers.join(" or ")
            ))
        })?;
    if projected_session_id != requested_session_id {
        return Err(GatewayApiError::Contract(format!(
            "requested session `{requested_session_id}` but Gateway returned {contract} for `{projected_session_id}`"
        )));
    }
    Ok(())
}

fn validate_session_messages_identity(
    requested_session_id: &str,
    page: &SessionMessagesPage,
) -> Result<(), GatewayApiError> {
    if page.session_id != requested_session_id {
        return Err(GatewayApiError::Contract(format!(
            "requested session `{requested_session_id}` but Gateway returned history for `{}`",
            page.session_id
        )));
    }
    if let Some(foreign) = page
        .messages
        .iter()
        .find(|message| message.session_id != requested_session_id)
    {
        return Err(GatewayApiError::Contract(format!(
            "history page for `{requested_session_id}` contains foreign message `{}` from session `{}`",
            foreign.id, foreign.session_id
        )));
    }
    Ok(())
}

#[cfg(test)]
fn gateway_sse_frame_resync_reason(frame: &str) -> Option<String> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    let value = serde_json::from_str::<serde_json::Value>(&data).ok()?;
    match value.get("type").and_then(serde_json::Value::as_str) {
        Some("session_stream_resync") => Some(
            value
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("transport resync")
                .to_string(),
        ),
        Some("RuntimeStreamLagged") => Some(format!(
            "runtime relay lag ({} events skipped)",
            value
                .get("skipped")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
        )),
        _ => None,
    }
}

#[cfg(test)]
fn validate_session_authorization_revoke_identity(
    frame: &str,
    subscribed_session_id: &str,
) -> Result<(), GatewayApiError> {
    let data = gateway_sse_frame_data(frame).ok_or_else(|| {
        GatewayApiError::Contract("session authorization revoke frame has no JSON data".to_string())
    })?;
    let value = serde_json::from_str::<serde_json::Value>(&data).map_err(|error| {
        GatewayApiError::Contract(format!(
            "invalid session authorization revoke JSON: {error}"
        ))
    })?;
    validate_gateway_session_event_contract(
        &value,
        "SessionAuthorizationRevoked",
        subscribed_session_id,
        gateway_sse_frame_commit_cursor(frame),
    )
    .map_err(GatewayApiError::Contract)
}

fn gateway_sse_frame_event_name(frame: &str) -> Option<&str> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("event:"))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn gateway_sse_frame_id(frame: &str) -> Option<&str> {
    frame.lines().find_map(|line| {
        line.strip_prefix("id:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
    })
}

fn gateway_sse_frame_data(frame: &str) -> Option<String> {
    let data = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!data.is_empty()).then_some(data)
}

#[cfg(test)]
fn gateway_sse_frame_projection_delta(
    frame: &str,
) -> Option<harness_contract::projection::ProjectionDelta> {
    (gateway_sse_frame_event_name(frame) == Some("projection_delta"))
        .then(|| gateway_sse_frame_data(frame))
        .flatten()
        .and_then(|data| serde_json::from_str(&data).ok())
}

fn gateway_sse_frame_projection_authorization_revoked(frame: &str) -> bool {
    gateway_sse_frame_event_name(frame) == Some("projection_authorization_revoked")
}

#[cfg(test)]
#[path = "client/tests.rs"]
mod tests;
