use std::{path::Path, time::Duration};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::{body::Incoming, client::conn::http2::SendRequest};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{net::UnixStream, task::JoinHandle};

use crate::{CancellationToken, GenerationFence, ManagedWorkerError, ManagedWorkerResult};

/// One generation-bound, multiplexed HTTP/2 channel over a Unix socket.
#[derive(Debug)]
pub struct ManagedH2Channel {
    generation: GenerationFence,
    sender: SendRequest<Full<Bytes>>,
    driver: JoinHandle<()>,
}

impl ManagedH2Channel {
    pub async fn connect(
        socket: impl AsRef<Path>,
        generation: GenerationFence,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> ManagedWorkerResult<Self> {
        let socket = socket.as_ref();
        let connect = UnixStream::connect(socket);
        let stream = tokio::select! {
            () = cancellation.cancelled() => return Err(ManagedWorkerError::Cancelled),
            result = tokio::time::timeout(timeout, connect) => {
                result.map_err(|_| ManagedWorkerError::DeadlineExceeded(timeout))?
                    .map_err(|error| ManagedWorkerError::io(socket, error))?
            }
        };
        let handshake = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(TokioIo::new(stream));
        let (sender, connection) = tokio::select! {
            () = cancellation.cancelled() => return Err(ManagedWorkerError::Cancelled),
            result = tokio::time::timeout(timeout, handshake) => {
                result.map_err(|_| ManagedWorkerError::DeadlineExceeded(timeout))?
                    .map_err(|error| ManagedWorkerError::H2(error.to_string()))?
            }
        };
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "managed worker H2 connection closed");
            }
        });
        Ok(Self {
            generation,
            sender,
            driver,
        })
    }

    #[must_use]
    pub fn generation(&self) -> &GenerationFence {
        &self.generation
    }

    pub async fn send(
        &self,
        observed_generation: &str,
        request: Request<Full<Bytes>>,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> ManagedWorkerResult<Response<Incoming>> {
        self.generation.ensure(observed_generation)?;
        let mut sender = self.sender.clone();
        let send = async move {
            sender
                .ready()
                .await
                .map_err(|error| ManagedWorkerError::H2(error.to_string()))?;
            sender
                .send_request(request)
                .await
                .map_err(|error| ManagedWorkerError::H2(error.to_string()))
        };
        tokio::select! {
            () = cancellation.cancelled() => Err(ManagedWorkerError::Cancelled),
            result = tokio::time::timeout(timeout, send) => {
                result.map_err(|_| ManagedWorkerError::DeadlineExceeded(timeout))?
            }
        }
    }
}

impl Drop for ManagedH2Channel {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use hyper::service::service_fn;
    use std::sync::Arc;
    use tokio::net::UnixListener;

    fn socket_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "managed-worker-h2-{}-{}.sock",
            std::process::id(),
            rand::random::<u64>()
        ))
    }

    async fn channel() -> (
        ManagedH2Channel,
        JoinHandle<Result<(), hyper::Error>>,
        std::path::PathBuf,
    ) {
        let path = socket_path();
        let listener = UnixListener::bind(&path).expect("listener");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|request: Request<Incoming>| async move {
                        let body = request.into_body().collect().await.expect("request body");
                        Ok::<_, std::convert::Infallible>(Response::new(Full::new(body.to_bytes())))
                    }),
                )
                .await
        });
        let channel = ManagedH2Channel::connect(
            &path,
            GenerationFence::new("generation-1").expect("generation"),
            Duration::from_secs(1),
            &CancellationToken::default(),
        )
        .await
        .expect("connect");
        (channel, server, path)
    }

    #[tokio::test]
    async fn one_channel_multiplexes_concurrent_requests() {
        let (channel, server, path) = channel().await;
        let channel = Arc::new(channel);
        let mut requests = tokio::task::JoinSet::new();
        for index in 0..32_u8 {
            let request = Request::builder()
                .uri(format!("http://worker/request/{index}"))
                .body(Full::new(Bytes::from(vec![index; 16])))
                .expect("request");
            let channel = Arc::clone(&channel);
            requests.spawn(async move {
                channel
                    .send(
                        "generation-1",
                        request,
                        Duration::from_secs(1),
                        &CancellationToken::default(),
                    )
                    .await
                    .map(|response| (index, response))
            });
        }
        let mut observed = Vec::new();
        while let Some(response) = requests.join_next().await {
            let (index, response) = response.expect("request task").expect("response");
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            assert_eq!(body.as_ref(), vec![index; 16]);
            observed.push(index);
        }
        observed.sort_unstable();
        assert_eq!(observed, (0..32).collect::<Vec<_>>());
        drop(channel);
        server
            .await
            .expect("server task")
            .expect("serve multiplexed H2 requests");
        std::fs::remove_file(path).expect("socket cleanup");
    }

    #[tokio::test]
    async fn generation_and_cancellation_fence_requests() {
        let (channel, server, path) = channel().await;
        let request = || {
            Request::builder()
                .uri("http://worker/")
                .body(Full::new(Bytes::new()))
                .expect("request")
        };
        assert!(matches!(
            channel
                .send(
                    "generation-0",
                    request(),
                    Duration::from_secs(1),
                    &CancellationToken::default()
                )
                .await,
            Err(ManagedWorkerError::StaleGeneration { .. })
        ));
        let cancelled = CancellationToken::default();
        cancelled.cancel();
        assert!(matches!(
            channel
                .send(
                    "generation-1",
                    request(),
                    Duration::from_secs(1),
                    &cancelled
                )
                .await,
            Err(ManagedWorkerError::Cancelled)
        ));
        drop(channel);
        // Both requests were rejected before reaching the channel. Closing the
        // client without sending a stream may therefore end the server driver
        // with BrokenPipe; the contract under test is the client-side fence.
        let _ = server.await.expect("server task");
        std::fs::remove_file(path).expect("socket cleanup");
    }
}
