use std::{collections::VecDeque, sync::Arc};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::Mutex,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSnapshot {
    pub bytes: Vec<u8>,
    pub dropped_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundedLogBuffer {
    inner: Arc<Mutex<LogState>>,
}

#[derive(Debug)]
struct LogState {
    bytes: VecDeque<u8>,
    capacity: usize,
    dropped_bytes: u64,
}

impl BoundedLogBuffer {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogState {
                bytes: VecDeque::with_capacity(capacity),
                capacity,
                dropped_bytes: 0,
            })),
        }
    }

    pub(crate) async fn drain<R>(&self, mut reader: R)
    where
        R: AsyncRead + Unpin,
    {
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) => return,
                Ok(read) => self.push(&chunk[..read]).await,
                Err(error) => {
                    tracing::warn!(%error, "managed worker log drain failed");
                    return;
                }
            }
        }
    }

    async fn push(&self, chunk: &[u8]) {
        let mut state = self.inner.lock().await;
        if state.capacity == 0 {
            state.dropped_bytes = state
                .dropped_bytes
                .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            return;
        }
        for byte in chunk {
            if state.bytes.len() == state.capacity {
                state.bytes.pop_front();
                state.dropped_bytes = state.dropped_bytes.saturating_add(1);
            }
            state.bytes.push_back(*byte);
        }
    }

    pub(crate) async fn snapshot(&self) -> LogSnapshot {
        let state = self.inner.lock().await;
        LogSnapshot {
            bytes: state.bytes.iter().copied().collect(),
            dropped_bytes: state.dropped_bytes,
        }
    }
}
