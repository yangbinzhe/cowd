use std::{path::Path, time::Duration};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::{body::Incoming, client::conn::http2::SendRequest};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{net::UnixStream, task::JoinHandle, time::Instant};

use crate::{CancellationToken, GenerationFence, ManagedWorkerError, ManagedWorkerResult};

/// Closed peer-identity policy evaluated on the same Unix stream used by H2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerCredentialPolicy {
    ExactPid(u32),
    CurrentUidAndExactPid { uid: u32, pid: u32 },
}

impl PeerCredentialPolicy {
    fn expected_pid(self) -> u32 {
        match self {
            Self::ExactPid(pid) | Self::CurrentUidAndExactPid { pid, .. } => pid,
        }
    }

    fn expected_uid(self) -> Option<u32> {
        match self {
            Self::ExactPid(_) => None,
            Self::CurrentUidAndExactPid { uid, .. } => Some(uid),
        }
    }
}

/// Immutable evidence captured before the connected Unix stream enters H2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentialReceipt {
    pid: u32,
    uid: u32,
    gid: u32,
}

impl PeerCredentialReceipt {
    #[must_use]
    pub fn pid(self) -> u32 {
        self.pid
    }

    #[must_use]
    pub fn uid(self) -> u32 {
        self.uid
    }

    #[must_use]
    pub fn gid(self) -> u32 {
        self.gid
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ManagedH2ConnectError {
    #[error("managed H2 connection was cancelled")]
    Cancelled,
    #[error("managed H2 connection deadline elapsed")]
    DeadlineExceeded,
    #[error("managed H2 Unix connection to {path} failed: {source}")]
    Connect {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("SO_PEERCRED is unavailable on the connected Unix stream: {source}")]
    CredentialUnavailable {
        #[source]
        source: std::io::Error,
    },
    #[error(
        "Unix peer credential mismatch: expected pid {expected_pid} uid {expected_uid:?}, observed pid {observed_pid:?} uid {observed_uid}"
    )]
    PeerMismatch {
        expected_pid: u32,
        expected_uid: Option<u32>,
        observed_pid: Option<u32>,
        observed_uid: u32,
    },
    #[error("peer credential policy requires a non-zero PID")]
    InvalidPolicy,
    #[error("managed-worker HTTP/2 handshake failed: {0}")]
    H2(String),
}

/// One generation-bound, multiplexed HTTP/2 channel over a Unix socket.
#[derive(Debug)]
pub struct ManagedH2Channel {
    generation: GenerationFence,
    peer: PeerCredentialReceipt,
    sender: SendRequest<Full<Bytes>>,
    driver: JoinHandle<()>,
}

impl ManagedH2Channel {
    /// Connect on Linux and verify `SO_PEERCRED` on the exact stream handed to H2.
    /// Non-Linux targets fail closed with `CredentialUnavailable`.
    pub async fn connect_verified(
        socket: impl AsRef<Path>,
        generation: GenerationFence,
        cancellation: &CancellationToken,
        deadline: Instant,
        policy: PeerCredentialPolicy,
    ) -> Result<Self, ManagedH2ConnectError> {
        if policy.expected_pid() == 0 {
            return Err(ManagedH2ConnectError::InvalidPolicy);
        }
        if cancellation.is_cancelled() {
            return Err(ManagedH2ConnectError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ManagedH2ConnectError::DeadlineExceeded);
        }
        let socket = socket.as_ref();
        let connect = UnixStream::connect(socket);
        let stream = tokio::select! {
            () = cancellation.cancelled() => return Err(ManagedH2ConnectError::Cancelled),
            result = tokio::time::timeout_at(deadline, connect) => {
                result.map_err(|_| ManagedH2ConnectError::DeadlineExceeded)?
                    .map_err(|source| ManagedH2ConnectError::Connect {
                        path: socket.to_path_buf(),
                        source,
                    })?
            }
        };
        let (observed_pid, observed_uid, observed_gid) = socket_peer_credentials(&stream)?;
        if observed_pid != Some(policy.expected_pid())
            || policy
                .expected_uid()
                .is_some_and(|expected| expected != observed_uid)
        {
            return Err(ManagedH2ConnectError::PeerMismatch {
                expected_pid: policy.expected_pid(),
                expected_uid: policy.expected_uid(),
                observed_pid,
                observed_uid,
            });
        }
        let peer = PeerCredentialReceipt {
            pid: policy.expected_pid(),
            uid: observed_uid,
            gid: observed_gid,
        };
        let handshake = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(TokioIo::new(stream));
        let (sender, connection) = tokio::select! {
            () = cancellation.cancelled() => return Err(ManagedH2ConnectError::Cancelled),
            result = tokio::time::timeout_at(deadline, handshake) => {
                result.map_err(|_| ManagedH2ConnectError::DeadlineExceeded)?
                    .map_err(|error| ManagedH2ConnectError::H2(error.to_string()))?
            }
        };
        let driver = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "managed worker H2 connection closed");
            }
        });
        Ok(Self {
            generation,
            peer,
            sender,
            driver,
        })
    }

    #[must_use]
    pub fn generation(&self) -> &GenerationFence {
        &self.generation
    }

    #[must_use]
    pub fn peer_credentials(&self) -> PeerCredentialReceipt {
        self.peer
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

#[cfg(target_os = "linux")]
fn socket_peer_credentials(
    stream: &UnixStream,
) -> Result<(Option<u32>, u32, u32), ManagedH2ConnectError> {
    let credentials = stream
        .peer_cred()
        .map_err(|source| ManagedH2ConnectError::CredentialUnavailable { source })?;
    Ok((
        credentials.pid().and_then(|pid| u32::try_from(pid).ok()),
        credentials.uid(),
        credentials.gid(),
    ))
}

#[cfg(not(target_os = "linux"))]
fn socket_peer_credentials(
    _stream: &UnixStream,
) -> Result<(Option<u32>, u32, u32), ManagedH2ConnectError> {
    Err(ManagedH2ConnectError::CredentialUnavailable {
        source: std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SO_PEERCRED PID verification requires Linux",
        ),
    })
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
    use std::{
        fs,
        os::unix::fs::MetadataExt,
        process::{Child, Command, Stdio},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
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
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = Arc::clone(&accepts);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            server_accepts.fetch_add(1, Ordering::AcqRel);
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
        let uid = fs::metadata(&path).expect("socket metadata").uid();
        let channel = ManagedH2Channel::connect_verified(
            &path,
            GenerationFence::new("generation-1").expect("generation"),
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(1),
            PeerCredentialPolicy::CurrentUidAndExactPid {
                uid,
                pid: std::process::id(),
            },
        )
        .await
        .expect("connect");
        assert_eq!(channel.peer_credentials().pid(), std::process::id());
        assert_eq!(channel.peer_credentials().uid(), uid);
        for _ in 0..100 {
            if accepts.load(Ordering::Acquire) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(accepts.load(Ordering::Acquire), 1);
        (channel, server, path)
    }

    async fn accepted_then_closed(path: &Path) -> JoinHandle<()> {
        let listener = UnixListener::bind(path).expect("listener");
        tokio::spawn(async move {
            let _ = listener.accept().await.expect("accept");
        })
    }

    #[tokio::test]
    async fn peer_policy_rejects_wrong_pid_and_uid_before_h2() {
        let pid_path = socket_path();
        let pid_server = accepted_then_closed(&pid_path).await;
        let error = ManagedH2Channel::connect_verified(
            &pid_path,
            GenerationFence::new("generation-1").expect("generation"),
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(1),
            PeerCredentialPolicy::ExactPid(std::process::id().saturating_add(1)),
        )
        .await
        .expect_err("wrong pid must fail closed");
        assert!(matches!(error, ManagedH2ConnectError::PeerMismatch { .. }));
        pid_server.await.expect("pid server");
        fs::remove_file(pid_path).expect("pid socket cleanup");

        let uid_path = socket_path();
        let uid_server = accepted_then_closed(&uid_path).await;
        let uid = fs::metadata(&uid_path).expect("socket metadata").uid();
        let error = ManagedH2Channel::connect_verified(
            &uid_path,
            GenerationFence::new("generation-1").expect("generation"),
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(1),
            PeerCredentialPolicy::CurrentUidAndExactPid {
                uid: uid.saturating_add(1),
                pid: std::process::id(),
            },
        )
        .await
        .expect_err("wrong uid must fail closed");
        assert!(matches!(error, ManagedH2ConnectError::PeerMismatch { .. }));
        uid_server.await.expect("uid server");
        fs::remove_file(uid_path).expect("uid socket cleanup");
    }

    #[tokio::test]
    async fn foreign_socket_server_is_rejected_before_h2_handshake() {
        let path = socket_path();
        let ready = path.with_extension("foreign-ready");
        let mut server = spawn_peer_server(&path, &ready);
        wait_ready(&ready).await;
        let result = ManagedH2Channel::connect_verified(
            &path,
            GenerationFence::new("generation-1").expect("generation"),
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(1),
            PeerCredentialPolicy::ExactPid(std::process::id()),
        )
        .await;
        assert!(matches!(
            result,
            Err(ManagedH2ConnectError::PeerMismatch { .. })
        ));
        server.kill().expect("stop foreign server");
        server.wait().expect("reap foreign server");
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(ready);
    }

    #[tokio::test]
    async fn cancelled_and_elapsed_connections_are_typed() {
        let cancelled = CancellationToken::default();
        cancelled.cancel();
        assert!(matches!(
            ManagedH2Channel::connect_verified(
                socket_path(),
                GenerationFence::new("generation-1").expect("generation"),
                &cancelled,
                Instant::now() + Duration::from_secs(1),
                PeerCredentialPolicy::ExactPid(std::process::id()),
            )
            .await,
            Err(ManagedH2ConnectError::Cancelled)
        ));
        assert!(matches!(
            ManagedH2Channel::connect_verified(
                socket_path(),
                GenerationFence::new("generation-1").expect("generation"),
                &CancellationToken::default(),
                Instant::now(),
                PeerCredentialPolicy::ExactPid(std::process::id()),
            )
            .await,
            Err(ManagedH2ConnectError::DeadlineExceeded)
        ));
    }

    fn spawn_peer_server(socket: &Path, ready: &Path) -> Child {
        Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "channel::tests::peer_server_entry",
                "--nocapture",
            ])
            .env("COWD_TEST_PEER_SOCKET", socket)
            .env("COWD_TEST_PEER_READY", ready)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn peer server")
    }

    async fn wait_ready(path: &Path) {
        for _ in 0..100 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("peer server did not become ready");
    }

    #[test]
    fn peer_server_entry() {
        let Some(socket) = std::env::var_os("COWD_TEST_PEER_SOCKET") else {
            return;
        };
        let ready = std::env::var_os("COWD_TEST_PEER_READY").expect("ready path");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("peer runtime");
        runtime.block_on(async move {
            let listener = UnixListener::bind(&socket).expect("peer listener");
            fs::write(&ready, b"ready").expect("ready marker");
            let (stream, _) = listener.accept().await.expect("peer accept");
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(|request: Request<Incoming>| async move {
                        let body = request.into_body().collect().await.expect("request body");
                        Ok::<_, std::convert::Infallible>(Response::new(Full::new(body.to_bytes())))
                    }),
                )
                .await;
        });
    }

    #[tokio::test]
    async fn receipt_remains_bound_to_the_original_peer_after_path_replacement() {
        let path = socket_path();
        let ready = path.with_extension("ready-1");
        let mut first = spawn_peer_server(&path, &ready);
        wait_ready(&ready).await;
        let first_pid = first.id();
        let channel = ManagedH2Channel::connect_verified(
            &path,
            GenerationFence::new("generation-1").expect("generation"),
            &CancellationToken::default(),
            Instant::now() + Duration::from_secs(2),
            PeerCredentialPolicy::ExactPid(first_pid),
        )
        .await
        .expect("verified child peer");
        let receipt = channel.peer_credentials();
        first.kill().expect("stop first peer");
        first.wait().expect("reap first peer");
        drop(channel);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&ready);

        let second_ready = path.with_extension("ready-2");
        let mut second = spawn_peer_server(&path, &second_ready);
        wait_ready(&second_ready).await;
        assert_ne!(second.id(), receipt.pid());
        assert_eq!(receipt.pid(), first_pid);
        second.kill().expect("stop replacement peer");
        second.wait().expect("reap replacement peer");
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(second_ready);
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
