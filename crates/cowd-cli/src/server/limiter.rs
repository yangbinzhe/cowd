use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct ConnectionLimiter {
    inner: Arc<ConnectionLimiterInner>,
}

struct ConnectionLimiterInner {
    max_sse: AtomicUsize,
    active_sse: AtomicUsize,
    dropped_sse: AtomicUsize,
}

impl ConnectionLimiter {
    pub fn new(max_sse: usize) -> Self {
        Self {
            inner: Arc::new(ConnectionLimiterInner {
                max_sse: AtomicUsize::new(max_sse),
                active_sse: AtomicUsize::new(0),
                dropped_sse: AtomicUsize::new(0),
            }),
        }
    }

    pub fn try_acquire(&self) -> Result<ConnectionGuard, StatusCode> {
        let current = self.inner.active_sse.fetch_add(1, Ordering::Relaxed);
        if current >= self.inner.max_sse.load(Ordering::Relaxed) {
            self.inner.active_sse.fetch_sub(1, Ordering::Relaxed);
            self.inner.dropped_sse.fetch_add(1, Ordering::Relaxed);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        Ok(ConnectionGuard {
            inner: self.inner.clone(),
        })
    }

    pub fn active_count(&self) -> usize {
        self.inner.active_sse.load(Ordering::Relaxed)
    }

    pub fn max_connections(&self) -> usize {
        self.inner.max_sse.load(Ordering::Relaxed)
    }

    pub fn dropped_count(&self) -> usize {
        self.inner.dropped_sse.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            active_sse: self.active_count(),
            max_sse: self.max_connections(),
            dropped_sse: self.dropped_count(),
        }
    }
}

pub struct ConnectionGuard {
    inner: Arc<ConnectionLimiterInner>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.inner.active_sse.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectionStats {
    pub active_sse: usize,
    pub max_sse: usize,
    pub dropped_sse: usize,
}

pub async fn connection_stats_handler(
    axum::extract::State(state): axum::extract::State<crate::server::HttpAppStateRef>,
) -> Response {
    axum::Json(state.connection_limiter.stats()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_and_release() {
        let limiter = ConnectionLimiter::new(10);
        assert_eq!(limiter.active_count(), 0);
        {
            let _guard = limiter.try_acquire().unwrap();
            assert_eq!(limiter.active_count(), 1);
        }
        assert_eq!(limiter.active_count(), 0);
    }

    #[test]
    fn max_connections_enforced() {
        let limiter = ConnectionLimiter::new(2);
        let _g1 = limiter.try_acquire().unwrap();
        let _g2 = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_err());
        assert_eq!(limiter.dropped_count(), 1);
    }

    #[test]
    fn stats_correct() {
        let limiter = ConnectionLimiter::new(5);
        let _g1 = limiter.try_acquire().unwrap();
        let _g2 = limiter.try_acquire().unwrap();
        let stats = limiter.stats();
        assert_eq!(stats.active_sse, 2);
        assert_eq!(stats.max_sse, 5);
        assert_eq!(stats.dropped_sse, 0);
    }

    #[test]
    fn drop_after_dropped_does_not_double_count() {
        let limiter = ConnectionLimiter::new(2);
        let _g1 = limiter.try_acquire().unwrap();
        let _g2 = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_err());
        drop(_g1);
        drop(_g2);
        assert_eq!(limiter.active_count(), 0);
        assert_eq!(limiter.dropped_count(), 1);
    }
}
