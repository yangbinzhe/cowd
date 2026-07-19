use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::client::conn::http2::SendRequest;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::{Method, Request, StatusCode, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use surface::{
    EdgeBootstrapRequest, EdgeBootstrapResponse, EdgeEventAck, EdgeEventEnvelope, SurfaceError,
    SurfaceFrame, EDGE_PROTOCOL_V2,
};
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc, Mutex};

use super::SurfaceMessageStore;

const AUTH_HEADER: &str = "x-cowd-edge-token";
const MAX_RESPONSE_BODY: usize = 2 * 1024 * 1024;
const MAX_STREAM_LINE: usize = 512 * 1024;
const STREAM_BUFFER: usize = 8;

#[derive(Clone)]
pub(super) struct EdgeH2Client {
    surface_id: Arc<str>,
    token: Arc<str>,
    sender: Arc<Mutex<SendRequest<Full<Bytes>>>>,
}

impl std::fmt::Debug for EdgeH2Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EdgeH2Client")
            .field("surface_id", &self.surface_id)
            .finish_non_exhaustive()
    }
}

impl EdgeH2Client {
    pub(super) async fn connect(
        socket: &std::path::Path,
        surface_id: &str,
        token: &str,
    ) -> Result<Self, SurfaceError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let stream = loop {
            match UnixStream::connect(socket).await {
                Ok(stream) => break stream,
                Err(error) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    tracing::trace!(surface = surface_id, error = %error, "waiting for edge socket");
                }
                Err(error) => {
                    return Err(SurfaceError::Invocation {
                        surface: surface_id.to_string(),
                        reason: format!("failed to connect managed edge UDS: {error}"),
                    });
                }
            }
        };
        let (sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                .await
                .map_err(|error| SurfaceError::Invocation {
                    surface: surface_id.to_string(),
                    reason: format!("managed edge H2 handshake failed: {error}"),
                })?;
        let owned_surface = surface_id.to_string();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(surface = %owned_surface, error = %error, "managed edge H2 connection closed");
            }
        });
        Ok(Self {
            surface_id: Arc::from(surface_id.to_string()),
            token: Arc::from(token.to_string()),
            sender: Arc::new(Mutex::new(sender)),
        })
    }

    pub(super) async fn bootstrap(
        &self,
        request: &EdgeBootstrapRequest,
    ) -> Result<EdgeBootstrapResponse, SurfaceError> {
        self.json_request(Method::POST, "/_cowd/edge/v2/handshake", request)
            .await
    }

    pub(super) async fn invoke(&self, frame: &SurfaceFrame) -> Result<SurfaceFrame, SurfaceError> {
        let (method, path) = frame_endpoint(frame);
        self.json_request(method, path, frame).await
    }

    pub(super) async fn invoke_stream(
        &self,
        frame: &SurfaceFrame,
    ) -> Result<mpsc::Receiver<Result<SurfaceFrame, SurfaceError>>, SurfaceError> {
        let (method, path) = frame_endpoint(frame);
        let body = serde_json::to_vec(frame).map_err(|error| SurfaceError::Invocation {
            surface: self.surface_id.to_string(),
            reason: format!("managed edge request encode failed: {error}"),
        })?;
        let request = self.request(method, path, Bytes::from(body))?;
        let response = self.send(request).await?;
        if !response.status().is_success() {
            return Err(self.response_error(response).await);
        }
        let surface = self.surface_id.clone();
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            let mut body = response.into_body();
            let mut buffered = Vec::new();
            while let Some(frame) = body.frame().await {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = tx
                            .send(Err(SurfaceError::Invocation {
                                surface: surface.to_string(),
                                reason: format!("managed edge stream read failed: {error}"),
                            }))
                            .await;
                        return;
                    }
                };
                let Ok(data) = frame.into_data() else {
                    continue;
                };
                buffered.extend_from_slice(&data);
                if buffered.len() > MAX_STREAM_LINE {
                    let _ = tx
                        .send(Err(SurfaceError::Invocation {
                            surface: surface.to_string(),
                            reason: "managed edge stream exceeded line limit".to_string(),
                        }))
                        .await;
                    return;
                }
                while let Some(position) = buffered.iter().position(|byte| *byte == b'\n') {
                    let line = buffered.drain(..=position).collect::<Vec<_>>();
                    let line = &line[..line.len().saturating_sub(1)];
                    if line.is_empty() {
                        continue;
                    }
                    let decoded = serde_json::from_slice::<SurfaceFrame>(line).map_err(|error| {
                        SurfaceError::Invocation {
                            surface: surface.to_string(),
                            reason: format!("managed edge stream decode failed: {error}"),
                        }
                    });
                    if tx.send(decoded).await.is_err() {
                        // 下游取消时立即丢弃 Incoming，HTTP/2 会重置该流。
                        return;
                    }
                }
            }
            if !buffered.is_empty() {
                let _ = tx
                    .send(Err(SurfaceError::Invocation {
                        surface: surface.to_string(),
                        reason: "managed edge stream ended with an incomplete frame".to_string(),
                    }))
                    .await;
            }
        });
        Ok(rx)
    }

    pub(super) fn spawn_event_stream(
        &self,
        events: Arc<Mutex<VecDeque<SurfaceFrame>>>,
        event_tx: broadcast::Sender<SurfaceFrame>,
        messages: Arc<SurfaceMessageStore>,
    ) {
        let client = self.clone();
        tokio::spawn(async move {
            if let Err(error) = client.consume_events(events, event_tx, messages).await {
                tracing::warn!(surface = %client.surface_id, error = %error, "managed edge event stream stopped");
            }
        });
    }

    async fn consume_events(
        &self,
        events: Arc<Mutex<VecDeque<SurfaceFrame>>>,
        event_tx: broadcast::Sender<SurfaceFrame>,
        messages: Arc<SurfaceMessageStore>,
    ) -> Result<(), SurfaceError> {
        let request = self.request(Method::GET, "/_cowd/edge/v2/events?after=0", Bytes::new())?;
        let response = self.send(request).await?;
        if response.status() != StatusCode::OK {
            return Err(self.response_error(response).await);
        }
        let mut body = response.into_body();
        let mut buffered = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| SurfaceError::Invocation {
                surface: self.surface_id.to_string(),
                reason: format!("managed edge event body failed: {error}"),
            })?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            buffered.extend_from_slice(&data);
            if buffered.len() > MAX_RESPONSE_BODY {
                return Err(SurfaceError::Invocation {
                    surface: self.surface_id.to_string(),
                    reason: "managed edge event exceeded line limit".to_string(),
                });
            }
            while let Some(position) = buffered.iter().position(|byte| *byte == b'\n') {
                let line = buffered.drain(..=position).collect::<Vec<_>>();
                let line = &line[..line.len().saturating_sub(1)];
                if line.is_empty() {
                    continue;
                }
                let envelope =
                    serde_json::from_slice::<EdgeEventEnvelope>(line).map_err(|error| {
                        SurfaceError::Invocation {
                            surface: self.surface_id.to_string(),
                            reason: format!("managed edge event decode failed: {error}"),
                        }
                    })?;
                messages
                    .persist_ingress_frame(&envelope.frame)
                    .map_err(|reason| SurfaceError::Invocation {
                        surface: self.surface_id.to_string(),
                        reason: format!("managed edge event durable persist failed: {reason}"),
                    })?;
                {
                    let mut snapshot = events.lock().await;
                    snapshot.push_back(envelope.frame.clone());
                    while snapshot.len() > 200 {
                        snapshot.pop_front();
                    }
                }
                let _ = event_tx.send(envelope.frame);
                let _: EdgeEventAck = self
                    .json_request(
                        Method::POST,
                        "/_cowd/edge/v2/events/ack",
                        &EdgeEventAck {
                            sequence: envelope.sequence,
                        },
                    )
                    .await?;
            }
        }
        Err(SurfaceError::Invocation {
            surface: self.surface_id.to_string(),
            reason: "managed edge event stream closed".to_string(),
        })
    }

    async fn json_request<T, R>(
        &self,
        method: Method,
        path: &str,
        payload: &T,
    ) -> Result<R, SurfaceError>
    where
        T: serde::Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
    {
        let body = serde_json::to_vec(payload).map_err(|error| SurfaceError::Invocation {
            surface: self.surface_id.to_string(),
            reason: format!("managed edge request encode failed: {error}"),
        })?;
        let request = self.request(method, path, Bytes::from(body))?;
        let response = self.send(request).await?;
        if !response.status().is_success() {
            return Err(self.response_error(response).await);
        }
        let collected = Limited::new(response.into_body(), MAX_RESPONSE_BODY)
            .collect()
            .await
            .map_err(|error| SurfaceError::Invocation {
                surface: self.surface_id.to_string(),
                reason: format!("managed edge response read failed or exceeded limit: {error}"),
            })?;
        let bytes = collected.to_bytes();
        serde_json::from_slice(&bytes).map_err(|error| SurfaceError::Invocation {
            surface: self.surface_id.to_string(),
            reason: format!("managed edge response decode failed: {error}"),
        })
    }

    fn request(
        &self,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> Result<Request<Full<Bytes>>, SurfaceError> {
        let token =
            HeaderValue::from_str(&self.token).map_err(|error| SurfaceError::Invocation {
                surface: self.surface_id.to_string(),
                reason: format!("invalid managed edge credential header: {error}"),
            })?;
        Request::builder()
            .method(method)
            .version(Version::HTTP_2)
            .uri(format!("http://cowd-edge{path}"))
            .header(AUTH_HEADER, token)
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(body))
            .map_err(|error| SurfaceError::Invocation {
                surface: self.surface_id.to_string(),
                reason: format!("managed edge request build failed: {error}"),
            })
    }

    async fn send(
        &self,
        request: Request<Full<Bytes>>,
    ) -> Result<hyper::Response<hyper::body::Incoming>, SurfaceError> {
        let response = {
            let mut sender = self.sender.lock().await;
            sender.send_request(request)
        };
        response.await.map_err(|error| SurfaceError::Invocation {
            surface: self.surface_id.to_string(),
            reason: format!("managed edge H2 request failed: {error}"),
        })
    }

    async fn response_error(
        &self,
        response: hyper::Response<hyper::body::Incoming>,
    ) -> SurfaceError {
        let status = response.status();
        let detail = response
            .into_body()
            .collect()
            .await
            .map(|body| String::from_utf8_lossy(&body.to_bytes()).into_owned())
            .unwrap_or_else(|error| error.to_string());
        SurfaceError::Invocation {
            surface: self.surface_id.to_string(),
            reason: format!("managed edge returned {status}: {detail}"),
        }
    }
}

fn frame_endpoint(frame: &SurfaceFrame) -> (Method, &'static str) {
    match frame {
        SurfaceFrame::Configure { .. } => (Method::POST, "/_cowd/edge/v2/configure"),
        SurfaceFrame::Connect { .. } => (Method::POST, "/_cowd/edge/v2/connect"),
        SurfaceFrame::Disconnect { .. } => (Method::POST, "/_cowd/edge/v2/disconnect"),
        SurfaceFrame::Health { .. } => (Method::GET, "/_cowd/edge/v2/health"),
        SurfaceFrame::Send { .. } => (Method::POST, "/_cowd/edge/v2/message/send"),
        SurfaceFrame::Action { action, .. } if action == "source.read_batch" => {
            (Method::POST, "/_cowd/edge/v2/source/read")
        }
        SurfaceFrame::Action { action, .. } if action == "source.schema_discovery" => {
            (Method::POST, "/_cowd/edge/v2/source/schema")
        }
        SurfaceFrame::Action { action, .. } if action == "source.incremental.run" => {
            (Method::POST, "/_cowd/edge/v2/source/incremental")
        }
        SurfaceFrame::Action { .. } => (Method::POST, "/_cowd/edge/v2/action"),
        _ => (Method::POST, "/_cowd/edge/v2/action"),
    }
}

pub(super) fn bootstrap_request(
    surface_id: &str,
    driver_profile: &str,
    capabilities: Vec<String>,
) -> EdgeBootstrapRequest {
    EdgeBootstrapRequest {
        protocol: EDGE_PROTOCOL_V2.to_string(),
        gateway_version: env!("CARGO_PKG_VERSION").to_string(),
        surface_id: surface_id.to_string(),
        driver_profile: driver_profile.to_string(),
        capabilities,
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Instant;

    use http_body_util::StreamBody;
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper::Response;
    use tokio::net::UnixListener;
    use tokio::sync::Notify;

    type FixtureBody = http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>;

    struct ActiveGuard(Arc<AtomicUsize>);

    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    async fn fixture_response(
        request: Request<Incoming>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    ) -> Result<Response<FixtureBody>, Infallible> {
        let path = request.uri().path().to_string();
        let bytes = request.into_body().collect().await.unwrap().to_bytes();
        if path.ends_with("/handshake") {
            let bootstrap: EdgeBootstrapRequest = serde_json::from_slice(&bytes).unwrap();
            let response = EdgeBootstrapResponse {
                protocol: EDGE_PROTOCOL_V2.to_string(),
                surface_id: bootstrap.surface_id,
                driver_profile: bootstrap.driver_profile,
                capabilities: bootstrap.capabilities,
                max_in_flight: 256,
            };
            return Ok(Response::new(
                Full::new(Bytes::from(serde_json::to_vec(&response).unwrap())).boxed_unsync(),
            ));
        }
        let active_now = active.fetch_add(1, Ordering::SeqCst) + 1;
        max_active.fetch_max(active_now, Ordering::SeqCst);
        let guard = ActiveGuard(active);
        let frame: SurfaceFrame = serde_json::from_slice(&bytes).unwrap();
        let (id, action, delay) = match frame {
            SurfaceFrame::Action {
                id,
                action,
                payload,
                ..
            } => (
                id,
                action,
                payload
                    .get("delay_ms")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            ),
            _ => ("fixture".to_string(), String::new(), 0),
        };
        if action == "source.incremental.run" {
            let frames =
                futures::stream::unfold((0usize, guard, id), |(index, guard, id)| async move {
                    if index >= 100 {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let frame = SurfaceFrame::Ok {
                        id: id.clone(),
                        payload: serde_json::json!({
                            "status": "ok",
                            "chunk_index": index,
                            "final_chunk": index == 99
                        }),
                    };
                    let mut bytes = serde_json::to_vec(&frame).unwrap();
                    bytes.push(b'\n');
                    Some((
                        Ok::<_, Infallible>(hyper::body::Frame::data(Bytes::from(bytes))),
                        (index + 1, guard, id),
                    ))
                });
            return Ok(Response::new(StreamBody::new(frames).boxed_unsync()));
        }
        let _guard = guard;
        tokio::time::sleep(Duration::from_millis(delay)).await;
        let response = SurfaceFrame::Ok {
            id,
            payload: serde_json::json!({"ok": true}),
        };
        Ok(Response::new(
            Full::new(Bytes::from(serde_json::to_vec(&response).unwrap())).boxed_unsync(),
        ))
    }

    async fn fixture() -> (
        EdgeH2Client,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let root = std::env::temp_dir().join(format!("cowd-h2-client-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("edge.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let server_active = active.clone();
        let server_max = max_active.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |request| {
                fixture_response(request, server_active.clone(), server_max.clone())
            });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
            let _ = std::fs::remove_dir_all(root);
        });
        let client = EdgeH2Client::connect(
            &socket,
            "fixture",
            "fixture-token-at-least-thirty-two-bytes",
        )
        .await
        .unwrap();
        client
            .bootstrap(&bootstrap_request(
                "fixture",
                "fixture",
                vec!["fixture.action".to_string()],
            ))
            .await
            .unwrap();
        (client, active, max_active, server)
    }

    fn delayed_action(id: usize, delay_ms: u64) -> SurfaceFrame {
        SurfaceFrame::Action {
            id: format!("fixture-{id}"),
            surface: "fixture".to_string(),
            action: "fixture.delay".to_string(),
            payload: serde_json::json!({"delay_ms": delay_ms}),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn managed_edge_h2_client_multiplexes_requests() {
        let (client, _, max_active, server) = fixture().await;
        let started = Instant::now();
        let mut tasks = Vec::new();
        for index in 0..64 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                client.invoke(&delayed_action(index, 50)).await.unwrap()
            }));
        }
        for task in tasks {
            assert!(matches!(task.await.unwrap(), SurfaceFrame::Ok { .. }));
        }
        assert!(max_active.load(Ordering::SeqCst) >= 8);
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_millis(500));
        eprintln!(
            "gateway_h2_64 elapsed_ms={} max_active={}",
            elapsed.as_micros() as f64 / 1_000.0,
            max_active.load(Ordering::SeqCst)
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_edge_cancellation_resets_handler_stream() {
        let (client, active, _, server) = fixture().await;
        let task = tokio::spawn(async move { client.invoke(&delayed_action(1, 5_000)).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("handler future remained active after H2 stream cancellation");
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_stream_is_bounded_and_downstream_drop_cancels_h2_body() {
        let (client, active, _, server) = fixture().await;
        let frame = SurfaceFrame::Action {
            id: "source-stream".to_string(),
            surface: "fixture".to_string(),
            action: "source.incremental.run".to_string(),
            payload: serde_json::Value::Null,
        };
        let mut chunks = client.invoke_stream(&frame).await.unwrap();
        assert!(matches!(
            chunks.recv().await.unwrap().unwrap(),
            SurfaceFrame::Ok { .. }
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(active.load(Ordering::SeqCst), 1);
        drop(chunks);
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("source H2 body remained active after downstream cancellation");
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_event_is_durable_before_gateway_sends_ack() {
        let root =
            std::env::temp_dir().join(format!("cowd-h2-durable-event-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("edge.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let store = Arc::new(SurfaceMessageStore::new(root.join("messages")));
        let acked = Arc::new(Notify::new());
        let persisted_before_ack = Arc::new(AtomicBool::new(false));
        let server_store = store.clone();
        let server_acked = acked.clone();
        let server_persisted = persisted_before_ack.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = service_fn(move |request: Request<Incoming>| {
                let store = server_store.clone();
                let acked = server_acked.clone();
                let persisted = server_persisted.clone();
                async move {
                    let path = request.uri().path().to_string();
                    let bytes = request.into_body().collect().await.unwrap().to_bytes();
                    if path.ends_with("/handshake") {
                        let bootstrap: EdgeBootstrapRequest =
                            serde_json::from_slice(&bytes).unwrap();
                        let response = EdgeBootstrapResponse {
                            protocol: EDGE_PROTOCOL_V2.to_string(),
                            surface_id: bootstrap.surface_id,
                            driver_profile: bootstrap.driver_profile,
                            capabilities: bootstrap.capabilities,
                            max_in_flight: 256,
                        };
                        return Ok::<_, Infallible>(Response::new(
                            Full::new(Bytes::from(serde_json::to_vec(&response).unwrap()))
                                .boxed_unsync(),
                        ));
                    }
                    if path.ends_with("/events") {
                        let envelope = EdgeEventEnvelope {
                            sequence: 1,
                            frame: SurfaceFrame::Event {
                                surface: "feishu".to_string(),
                                event: "message.received".to_string(),
                                payload: serde_json::json!({
                                    "session_id": "session-1",
                                    "message_id": "message-1",
                                    "text": "hello"
                                }),
                            },
                        };
                        let mut line = serde_json::to_vec(&envelope).unwrap();
                        line.push(b'\n');
                        let frames = futures::stream::once(async move {
                            Ok::<_, Infallible>(hyper::body::Frame::data(Bytes::from(line)))
                        })
                        .chain(futures::stream::pending());
                        return Ok(Response::new(StreamBody::new(frames).boxed_unsync()));
                    }
                    if path.ends_with("/events/ack") {
                        persisted.store(store.ingress_frame_count() == 1, Ordering::SeqCst);
                        acked.notify_one();
                        let ack: EdgeEventAck = serde_json::from_slice(&bytes).unwrap();
                        return Ok(Response::new(
                            Full::new(Bytes::from(serde_json::to_vec(&ack).unwrap()))
                                .boxed_unsync(),
                        ));
                    }
                    Ok(Response::new(Full::new(Bytes::new()).boxed_unsync()))
                }
            });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
        let client = EdgeH2Client::connect(
            &socket,
            "fixture",
            "fixture-token-at-least-thirty-two-bytes",
        )
        .await
        .unwrap();
        client
            .bootstrap(&bootstrap_request(
                "fixture",
                "fixture",
                vec!["message.receive".to_string()],
            ))
            .await
            .unwrap();
        let (event_tx, _event_rx) = broadcast::channel(8);
        client.spawn_event_stream(Arc::new(Mutex::new(VecDeque::new())), event_tx, store);

        tokio::time::timeout(Duration::from_secs(1), acked.notified())
            .await
            .expect("gateway did not ACK managed event");
        assert!(persisted_before_ack.load(Ordering::SeqCst));
        server.abort();
        let _ = std::fs::remove_dir_all(root);
    }
}
