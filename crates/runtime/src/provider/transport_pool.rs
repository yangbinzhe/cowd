use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

const DEFAULT_MAX_TRANSPORTS: usize = 8;
const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransportProfileFingerprint(pub u64);

impl TransportProfileFingerprint {
    #[must_use]
    pub fn from_proxy_config(config: &provider::ProxyConfig) -> Self {
        let value = (
            config.http_proxy.as_deref(),
            config.https_proxy.as_deref(),
            config.no_proxy.as_deref(),
            config.proxy_url.as_deref(),
        );
        Self(model_protocol::fingerprint::hash_serializable(&value))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderTransportPoolStats {
    pub entries: usize,
    pub checkouts: u64,
    pub hits: u64,
    pub builds: u64,
    pub evictions: u64,
    /// Number of model requests currently in flight across all transports.
    /// This is the real concurrency observed by the resource manager.
    pub in_flight: i64,
    /// Highest observed in-flight count (peak concurrency).
    pub peak_in_flight: u64,
}

#[derive(Clone)]
struct TransportEntry {
    client: reqwest::Client,
    last_checkout: u64,
    last_checkout_at: Instant,
}

/// Runtime-owned pool for request-stateless HTTP transports.
///
/// The mutex is also the single-flight build gate. Client construction is
/// synchronous and rare, so holding it here prevents duplicate pools during
/// concurrent cold starts without introducing an async lock into request code.
pub struct ProviderTransportPool {
    entries: Mutex<HashMap<TransportProfileFingerprint, TransportEntry>>,
    max_entries: usize,
    idle_ttl: Duration,
    sequence: AtomicU64,
    checkouts: AtomicU64,
    hits: AtomicU64,
    builds: AtomicU64,
    evictions: AtomicU64,
    in_flight: std::sync::atomic::AtomicI64,
    peak_in_flight: AtomicU64,
}

impl Default for ProviderTransportPool {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_TRANSPORTS)
    }
}

impl ProviderTransportPool {
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self::new_with_idle_ttl(max_entries, DEFAULT_IDLE_TTL)
    }

    #[must_use]
    pub fn new_with_idle_ttl(max_entries: usize, idle_ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries: max_entries.max(1),
            idle_ttl,
            sequence: AtomicU64::new(0),
            checkouts: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            builds: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            in_flight: std::sync::atomic::AtomicI64::new(0),
            peak_in_flight: AtomicU64::new(0),
        }
    }

    /// Mark one model request as in flight. The returned guard MUST be held
    /// for the lifetime of the provider request so `stats().in_flight`
    /// reflects true concurrency (P1 model pool multi-path concurrency).
    #[must_use]
    pub fn begin_request(&self) -> TransportRequestGuard<'_> {
        let previous = self.in_flight.fetch_add(1, Ordering::SeqCst);
        let current = previous.saturating_add(1).max(0) as u64;
        self.peak_in_flight.fetch_max(current, Ordering::SeqCst);
        TransportRequestGuard { pool: self }
    }

    pub fn checkout_default(
        &self,
    ) -> Result<(TransportProfileFingerprint, reqwest::Client), provider::ApiError> {
        self.checkout(&provider::ProxyConfig::from_env())
    }

    pub fn checkout(
        &self,
        config: &provider::ProxyConfig,
    ) -> Result<(TransportProfileFingerprint, reqwest::Client), provider::ApiError> {
        let started = Instant::now();
        let fingerprint = TransportProfileFingerprint::from_proxy_config(config);
        self.checkouts.fetch_add(1, Ordering::Relaxed);
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before_reclaim = entries.len();
        entries.retain(|_, entry| entry.last_checkout_at.elapsed() < self.idle_ttl);
        self.evictions.fetch_add(
            u64::try_from(before_reclaim.saturating_sub(entries.len())).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if let Some(entry) = entries.get_mut(&fingerprint) {
            entry.last_checkout = sequence;
            entry.last_checkout_at = Instant::now();
            self.hits.fetch_add(1, Ordering::Relaxed);
            crate::execution_core::performance::observe_duration(
                "transport_checkout_ms",
                started.elapsed(),
            );
            return Ok((fingerprint, entry.client.clone()));
        }
        let client = provider::build_http_client_with(config)?;
        if entries.len() >= self.max_entries {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_checkout)
                .map(|(key, _)| *key)
            {
                entries.remove(&oldest);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        entries.insert(
            fingerprint,
            TransportEntry {
                client: client.clone(),
                last_checkout: sequence,
                last_checkout_at: Instant::now(),
            },
        );
        self.builds.fetch_add(1, Ordering::Relaxed);
        crate::execution_core::performance::observe_duration(
            "transport_checkout_ms",
            started.elapsed(),
        );
        Ok((fingerprint, client))
    }

    #[must_use]
    pub fn stats(&self) -> ProviderTransportPoolStats {
        ProviderTransportPoolStats {
            entries: self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            checkouts: self.checkouts.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            builds: self.builds.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::SeqCst),
            peak_in_flight: self.peak_in_flight.load(Ordering::SeqCst),
        }
    }
}

/// RAII guard for one in-flight provider request.
pub struct TransportRequestGuard<'a> {
    pool: &'a ProviderTransportPool,
}

impl Drop for TransportRequestGuard<'_> {
    fn drop(&mut self) {
        self.pool.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::ProviderTransportPool;
    use std::time::Duration;

    #[test]
    fn repeated_checkout_reuses_transport() {
        let pool = ProviderTransportPool::new(2);
        let config = provider::ProxyConfig::default();
        pool.checkout(&config).expect("first transport");
        pool.checkout(&config).expect("shared transport");
        let stats = pool.stats();
        assert_eq!(stats.builds, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.entries, 1);
    }

    #[test]
    fn invalid_proxy_fails_closed() {
        let pool = ProviderTransportPool::new(2);
        let config = provider::ProxyConfig::from_proxy_url("not a valid proxy");
        assert!(pool.checkout(&config).is_err());
        assert_eq!(pool.stats().builds, 0);
    }

    #[test]
    fn idle_transports_are_reclaimed_on_checkout() {
        let pool = ProviderTransportPool::new_with_idle_ttl(2, Duration::ZERO);
        let config = provider::ProxyConfig::default();
        pool.checkout(&config).expect("first transport");
        pool.checkout(&config).expect("replacement transport");
        let stats = pool.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.builds, 2);
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn concurrent_request_guards_track_real_in_flight_concurrency() {
        let pool = ProviderTransportPool::new(2);
        assert_eq!(pool.stats().in_flight, 0);
        let guard_a = pool.begin_request();
        let guard_b = pool.begin_request();
        let stats = pool.stats();
        assert_eq!(stats.in_flight, 2);
        assert!(stats.peak_in_flight >= 2);
        drop(guard_a);
        assert_eq!(pool.stats().in_flight, 1);
        drop(guard_b);
        assert_eq!(pool.stats().in_flight, 0);
        assert!(pool.stats().peak_in_flight >= 2);
    }
}
