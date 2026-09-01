use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

const DEFAULT_MAX_TRANSPORTS: usize = 8;
const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_ACCOUNT_FAILURE_FENCE_TTL: Duration = Duration::from_secs(30);
const MAX_PREFIX_IDENTITIES: usize = 32;
const MAX_PREFIX_PREDECESSORS: usize = 8;
const DEFAULT_PROVIDER_CACHE_WARM_TTL: Duration = Duration::from_secs(5 * 60);
/// Serializing a long Provider request to discover a tiny shared prefix loses
/// concurrency without materially changing input cost. Only coordinate a
/// divergent DeepSeek cohort when at least half of the follower prompt is an
/// actual byte-identical prefix of the first request. Exact extensions always
/// remain coordinated regardless of total length.
const MIN_COMMON_PREFIX_COORDINATION_RATIO_BP: usize = 5_000;
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderWirePrefixObservation {
    pub cache_identity_sha256: String,
    pub predecessor_request_id: Option<String>,
    pub prompt_bytes: u64,
    pub reusable_prefix_bytes: u64,
    pub structural_reuse_ratio_bp: u32,
    pub exact_extension: bool,
    pub invalidation_reason: Option<String>,
    pub cold_leader: bool,
    /// This request serialized one DeepSeek common-prefix discovery after the
    /// first request had persisted. It is not another cold identity leader.
    pub common_prefix_leader: bool,
    pub warmup_bypassed_low_reuse: bool,
    pub waited_for_warmup: bool,
}

#[derive(Clone)]
struct CachedProviderPrompt {
    request_id: String,
    bytes: std::sync::Arc<[u8]>,
}

fn common_prefix_ratio_bp(first: &[u8], candidate: &[u8]) -> usize {
    if candidate.is_empty() {
        return 0;
    }
    let common = first
        .iter()
        .zip(candidate.iter())
        .take_while(|(left, right)| left == right)
        .count();
    common.saturating_mul(10_000) / candidate.len()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheWarmPhase {
    Cold,
    PrimingFirst,
    FirstRequestPersisted,
    PrimingCommonPrefix,
    Warm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheWarmAdmission {
    Leader(CacheWarmLeaderKind),
    Ready,
    BypassLowReuse,
    Wait,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheWarmLeaderKind {
    FirstRequest,
    CommonPrefix,
}

#[derive(Debug)]
struct CacheWarmState {
    phase: CacheWarmPhase,
    warmed_at: Option<Instant>,
    first_prompt: Option<std::sync::Arc<[u8]>>,
    waiters: usize,
}

#[derive(Debug)]
struct CacheWarmEntry {
    state: Mutex<CacheWarmState>,
    changed: tokio::sync::Notify,
}

pub(crate) struct ProviderCacheWarmupGuard {
    entry: std::sync::Arc<CacheWarmEntry>,
    leader: Option<CacheWarmLeaderKind>,
    finished: bool,
    pub waited: bool,
    pub bypassed_low_reuse: bool,
    persistence_barrier: Duration,
    requires_common_prefix_discovery: bool,
}

impl ProviderCacheWarmupGuard {
    #[cfg(test)]
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        self.leader.is_some()
    }

    #[must_use]
    pub const fn is_first_request_leader(&self) -> bool {
        matches!(self.leader, Some(CacheWarmLeaderKind::FirstRequest))
    }

    #[must_use]
    pub const fn is_common_prefix_leader(&self) -> bool {
        matches!(self.leader, Some(CacheWarmLeaderKind::CommonPrefix))
    }

    pub(crate) async fn finish_after_persistence_barrier(&mut self, success: bool) {
        if self.leader.is_some() && success && !self.persistence_barrier.is_zero() {
            tokio::time::sleep(self.persistence_barrier).await;
        }
        self.finish(success);
    }

    pub fn finish(&mut self, success: bool) {
        let Some(leader) = self.leader else {
            return;
        };
        if self.finished {
            return;
        }
        let mut state = self
            .entry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.phase = match (leader, success) {
            (CacheWarmLeaderKind::FirstRequest, true) => {
                if self.requires_common_prefix_discovery {
                    CacheWarmPhase::FirstRequestPersisted
                } else {
                    CacheWarmPhase::Warm
                }
            }
            (CacheWarmLeaderKind::FirstRequest, false) => {
                state.first_prompt = None;
                CacheWarmPhase::Cold
            }
            (CacheWarmLeaderKind::CommonPrefix, true) => CacheWarmPhase::Warm,
            (CacheWarmLeaderKind::CommonPrefix, false) => CacheWarmPhase::FirstRequestPersisted,
        };
        state.warmed_at = success.then(Instant::now);
        self.finished = true;
        drop(state);
        self.entry.changed.notify_waiters();
    }
}

impl Drop for ProviderCacheWarmupGuard {
    fn drop(&mut self) {
        self.finish(false);
    }
}

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
    /// Account-scoped failures currently suppressing sibling dispatches.
    pub account_failure_fences: usize,
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
    account_failure_fences: Mutex<HashMap<(u64, String), Instant>>,
    account_failure_fence_ttl: Duration,
    prompt_prefixes: Mutex<HashMap<String, VecDeque<CachedProviderPrompt>>>,
    cache_warm_entries: Mutex<HashMap<String, std::sync::Arc<CacheWarmEntry>>>,
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
        Self::new_with_ttls(max_entries, idle_ttl, DEFAULT_ACCOUNT_FAILURE_FENCE_TTL)
    }

    #[must_use]
    fn new_with_ttls(
        max_entries: usize,
        idle_ttl: Duration,
        account_failure_fence_ttl: Duration,
    ) -> Self {
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
            account_failure_fences: Mutex::new(HashMap::new()),
            account_failure_fence_ttl,
            prompt_prefixes: Mutex::new(HashMap::new()),
            cache_warm_entries: Mutex::new(HashMap::new()),
        }
    }

    /// Admit one cache cohort. Exactly one cold request primes an identity;
    /// followers wait without occupying an HTTP in-flight slot. Different
    /// identities never share this barrier.
    pub(crate) async fn acquire_cache_warmup(
        &self,
        cache_identity_sha256: &str,
        canonical_prompt: &[u8],
        persistence_barrier: Duration,
        requires_common_prefix_discovery: bool,
    ) -> ProviderCacheWarmupGuard {
        let entry = {
            let mut entries = self
                .cache_warm_entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !entries.contains_key(cache_identity_sha256)
                && entries.len() >= MAX_PREFIX_IDENTITIES
            {
                if let Some(evicted) = entries.keys().next().cloned() {
                    entries.remove(&evicted);
                }
            }
            std::sync::Arc::clone(
                entries
                    .entry(cache_identity_sha256.to_string())
                    .or_insert_with(|| {
                        std::sync::Arc::new(CacheWarmEntry {
                            state: Mutex::new(CacheWarmState {
                                phase: CacheWarmPhase::Cold,
                                warmed_at: None,
                                first_prompt: None,
                                waiters: 0,
                            }),
                            changed: tokio::sync::Notify::new(),
                        })
                    }),
            )
        };
        let mut waited = false;
        loop {
            // Register the wake before inspecting the phase. `notify_waiters`
            // does not retain a permit for a future that has not been polled;
            // without `enable`, a leader completing between the state check
            // and `.await` can strand a follower indefinitely.
            let notified = entry.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let admission = {
                let mut state = entry
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if matches!(
                    state.phase,
                    CacheWarmPhase::FirstRequestPersisted | CacheWarmPhase::Warm
                ) && state
                    .warmed_at
                    .is_some_and(|at| at.elapsed() >= DEFAULT_PROVIDER_CACHE_WARM_TTL)
                {
                    state.phase = CacheWarmPhase::Cold;
                    state.warmed_at = None;
                    state.first_prompt = None;
                }
                let exact_extension = state
                    .first_prompt
                    .as_deref()
                    .is_some_and(|first| canonical_prompt.starts_with(first));
                let coordinate_divergent_prefix =
                    state.first_prompt.as_deref().is_some_and(|first| {
                        common_prefix_ratio_bp(first, canonical_prompt)
                            >= MIN_COMMON_PREFIX_COORDINATION_RATIO_BP
                    });
                match state.phase {
                    CacheWarmPhase::Cold => {
                        state.phase = CacheWarmPhase::PrimingFirst;
                        state.first_prompt = Some(std::sync::Arc::from(canonical_prompt));
                        CacheWarmAdmission::Leader(CacheWarmLeaderKind::FirstRequest)
                    }
                    CacheWarmPhase::Warm => CacheWarmAdmission::Ready,
                    CacheWarmPhase::FirstRequestPersisted => {
                        if exact_extension {
                            // DeepSeek persists the full first request at its
                            // input boundary, so an exact extension can reuse
                            // it without another cold cohort request.
                            CacheWarmAdmission::Ready
                        } else if coordinate_divergent_prefix {
                            // A different suffix only causes DeepSeek to
                            // discover/persist the common prefix on this
                            // second request. Serialize exactly that request,
                            // then release the rest of the cohort.
                            state.phase = CacheWarmPhase::PrimingCommonPrefix;
                            CacheWarmAdmission::Leader(CacheWarmLeaderKind::CommonPrefix)
                        } else {
                            CacheWarmAdmission::BypassLowReuse
                        }
                    }
                    CacheWarmPhase::PrimingFirst | CacheWarmPhase::PrimingCommonPrefix => {
                        if exact_extension
                            || !requires_common_prefix_discovery
                            || coordinate_divergent_prefix
                        {
                            state.waiters = state.waiters.saturating_add(1);
                            CacheWarmAdmission::Wait
                        } else {
                            CacheWarmAdmission::BypassLowReuse
                        }
                    }
                }
            };
            match admission {
                CacheWarmAdmission::Leader(leader) => {
                    return ProviderCacheWarmupGuard {
                        entry: std::sync::Arc::clone(&entry),
                        leader: Some(leader),
                        finished: false,
                        waited,
                        bypassed_low_reuse: false,
                        persistence_barrier,
                        requires_common_prefix_discovery,
                    };
                }
                CacheWarmAdmission::Ready => {
                    return ProviderCacheWarmupGuard {
                        entry: std::sync::Arc::clone(&entry),
                        leader: None,
                        finished: true,
                        waited,
                        bypassed_low_reuse: false,
                        persistence_barrier,
                        requires_common_prefix_discovery,
                    };
                }
                CacheWarmAdmission::BypassLowReuse => {
                    return ProviderCacheWarmupGuard {
                        entry: std::sync::Arc::clone(&entry),
                        leader: None,
                        finished: true,
                        waited,
                        bypassed_low_reuse: true,
                        persistence_barrier,
                        requires_common_prefix_discovery,
                    };
                }
                CacheWarmAdmission::Wait => {
                    waited = true;
                    notified.await;
                    let mut state = entry
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.waiters = state.waiters.saturating_sub(1);
                }
            }
        }
    }

    /// Compare one exact canonical model-visible prompt against recent
    /// requests in the same cache-sensitive identity. This is structural
    /// evidence only; Provider usage remains the billing truth.
    pub(crate) fn observe_prompt_prefix(
        &self,
        cache_identity_sha256: String,
        request_id: String,
        prompt: Vec<u8>,
    ) -> ProviderWirePrefixObservation {
        let mut histories = self
            .prompt_prefixes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !histories.contains_key(&cache_identity_sha256)
            && histories.len() >= MAX_PREFIX_IDENTITIES
        {
            if let Some(evicted) = histories.keys().next().cloned() {
                histories.remove(&evicted);
            }
        }
        let history = histories.entry(cache_identity_sha256.clone()).or_default();
        let best = history
            .iter()
            .map(|candidate| {
                let lcp = candidate
                    .bytes
                    .iter()
                    .zip(prompt.iter())
                    .take_while(|(left, right)| left == right)
                    .count();
                (candidate, lcp)
            })
            .max_by_key(|(_, lcp)| *lcp);
        let predecessor_request_id = best.map(|(candidate, _)| candidate.request_id.clone());
        let reusable_prefix_bytes = best.map_or(0, |(_, lcp)| lcp);
        let exact_extension = best.is_some_and(|(candidate, lcp)| lcp == candidate.bytes.len());
        let structural_reuse_ratio_bp = if prompt.is_empty() {
            0
        } else {
            u32::try_from(reusable_prefix_bytes.saturating_mul(10_000) / prompt.len())
                .unwrap_or(10_000)
                .min(10_000)
        };
        let invalidation_reason = if best.is_none() {
            Some("cold_identity".to_string())
        } else if exact_extension {
            None
        } else if reusable_prefix_bytes == 0 {
            Some("no_common_model_visible_prefix".to_string())
        } else {
            Some("model_visible_prefix_diverged".to_string())
        };
        history.push_back(CachedProviderPrompt {
            request_id,
            bytes: prompt.into(),
        });
        while history.len() > MAX_PREFIX_PREDECESSORS {
            history.pop_front();
        }
        ProviderWirePrefixObservation {
            cache_identity_sha256,
            predecessor_request_id,
            prompt_bytes: history.back().map_or(0, |value| value.bytes.len()) as u64,
            reusable_prefix_bytes: reusable_prefix_bytes as u64,
            structural_reuse_ratio_bp,
            exact_extension,
            invalidation_reason,
            cold_leader: false,
            common_prefix_leader: false,
            warmup_bypassed_low_reuse: false,
            waited_for_warmup: false,
        }
    }

    /// Return whether a sibling Runtime consumer must avoid a known-unusable
    /// provider account before performing network I/O.
    pub(crate) fn provider_account_is_fenced(
        &self,
        registry_revision: u64,
        provider_account: &str,
    ) -> bool {
        let mut fences = self
            .account_failure_fences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fences.retain(|_, observed_at| observed_at.elapsed() < self.account_failure_fence_ttl);
        fences.contains_key(&(registry_revision, provider_account.to_string()))
    }

    /// Publish an account-scoped Provider fact for a short burst window.
    /// Request-scoped failures must never enter this shared fence.
    pub(crate) fn record_provider_failure(
        &self,
        registry_revision: u64,
        provider_account: &str,
        scope: model_protocol::provider_failure::ProviderFailureScope,
    ) -> bool {
        if scope != model_protocol::provider_failure::ProviderFailureScope::Account {
            return false;
        }
        let mut fences = self
            .account_failure_fences
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        fences.retain(|_, observed_at| observed_at.elapsed() < self.account_failure_fence_ttl);
        fences.insert(
            (registry_revision, provider_account.to_string()),
            Instant::now(),
        );
        true
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
        let account_failure_fences = {
            let mut fences = self
                .account_failure_fences
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            fences.retain(|_, observed_at| observed_at.elapsed() < self.account_failure_fence_ttl);
            fences.len()
        };
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
            account_failure_fences,
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
    use model_protocol::provider_failure::ProviderFailureScope;
    use std::sync::Arc;
    use std::time::Duration;

    async fn wait_for_cache_waiters(pool: &ProviderTransportPool, identity: &str, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let waiters = pool
                    .cache_warm_entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(identity)
                    .map_or(0, |entry| {
                        entry
                            .state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .waiters
                    });
                if waiters == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cache warmup followers register before the leader finishes");
    }

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

    #[test]
    fn prefix_oracle_reports_cold_then_exact_append_only_extension() {
        let pool = ProviderTransportPool::new(2);
        let cold = pool.observe_prompt_prefix(
            "identity-a".to_string(),
            "request-1".to_string(),
            b"stable-history".to_vec(),
        );
        assert_eq!(cold.invalidation_reason.as_deref(), Some("cold_identity"));
        assert_eq!(cold.structural_reuse_ratio_bp, 0);

        let warm = pool.observe_prompt_prefix(
            "identity-a".to_string(),
            "request-2".to_string(),
            b"stable-history-new-delta".to_vec(),
        );
        assert_eq!(warm.predecessor_request_id.as_deref(), Some("request-1"));
        assert_eq!(warm.reusable_prefix_bytes, 14);
        assert!(warm.exact_extension);
        assert!(warm.invalidation_reason.is_none());
    }

    #[test]
    fn prefix_oracle_does_not_mix_cache_sensitive_identities() {
        let pool = ProviderTransportPool::new(2);
        pool.observe_prompt_prefix(
            "identity-a".to_string(),
            "request-1".to_string(),
            b"same-prefix".to_vec(),
        );
        let other = pool.observe_prompt_prefix(
            "identity-b".to_string(),
            "request-2".to_string(),
            b"same-prefix-more".to_vec(),
        );
        assert_eq!(other.invalidation_reason.as_deref(), Some("cold_identity"));
        assert!(other.predecessor_request_id.is_none());
    }

    #[tokio::test]
    async fn cache_warmup_has_one_cold_leader_and_bounded_followers() {
        let pool = Arc::new(ProviderTransportPool::new(2));
        let mut leader = pool
            .acquire_cache_warmup("same-identity", b"stable-a", Duration::ZERO, false)
            .await;
        assert!(leader.is_leader());
        let followers = (0..100)
            .map(|_| {
                let pool = Arc::clone(&pool);
                tokio::spawn(async move {
                    pool.acquire_cache_warmup(
                        "same-identity",
                        b"stable-a-more",
                        Duration::ZERO,
                        false,
                    )
                    .await
                })
            })
            .collect::<Vec<_>>();
        wait_for_cache_waiters(&pool, "same-identity", 100).await;
        assert_eq!(pool.stats().in_flight, 0, "warmup wait is not HTTP traffic");
        leader.finish(true);
        for follower in followers {
            let admission = follower.await.expect("follower task");
            assert!(!admission.is_leader());
            assert!(admission.waited);
        }
    }

    #[tokio::test]
    async fn cancelled_cold_leader_promotes_exactly_one_waiter() {
        let pool = Arc::new(ProviderTransportPool::new(2));
        let leader = pool
            .acquire_cache_warmup("cancelled-identity", b"stable-a", Duration::ZERO, false)
            .await;
        let follower_pool = Arc::clone(&pool);
        let follower = tokio::spawn(async move {
            follower_pool
                .acquire_cache_warmup("cancelled-identity", b"stable-a", Duration::ZERO, false)
                .await
        });
        wait_for_cache_waiters(&pool, "cancelled-identity", 1).await;
        drop(leader);
        let mut promoted = follower.await.expect("promoted follower");
        assert!(promoted.is_leader());
        assert!(promoted.waited);
        promoted.finish(true);
    }

    #[tokio::test]
    async fn divergent_suffix_gets_one_common_prefix_leader_before_parallel_release() {
        let pool = Arc::new(ProviderTransportPool::new(4));
        let mut first = pool
            .acquire_cache_warmup(
                "shared-stable-cohort",
                b"stable|agent-a",
                Duration::ZERO,
                true,
            )
            .await;
        assert!(first.is_first_request_leader());
        assert!(!first.is_common_prefix_leader());
        first.finish(true);

        let mut common = pool
            .acquire_cache_warmup(
                "shared-stable-cohort",
                b"stable|agent-b",
                Duration::ZERO,
                true,
            )
            .await;
        assert!(!common.is_first_request_leader());
        assert!(common.is_common_prefix_leader());
        let follower_pool = Arc::clone(&pool);
        let follower = tokio::spawn(async move {
            follower_pool
                .acquire_cache_warmup(
                    "shared-stable-cohort",
                    b"stable|agent-c",
                    Duration::ZERO,
                    true,
                )
                .await
        });
        wait_for_cache_waiters(&pool, "shared-stable-cohort", 1).await;
        assert!(!follower.is_finished());
        common.finish(true);
        let released = follower.await.expect("common-prefix follower");
        assert!(!released.is_leader());
        assert!(released.waited);
    }

    #[tokio::test]
    async fn low_reuse_sibling_bypasses_warmup_without_serializing_provider_work() {
        let pool = ProviderTransportPool::new(4);
        let first_prompt = format!("shared|{}", "a".repeat(1_000));
        let sibling_prompt = format!("shared|{}", "b".repeat(1_000));
        let mut first = pool
            .acquire_cache_warmup(
                "low-reuse-cohort",
                first_prompt.as_bytes(),
                Duration::ZERO,
                true,
            )
            .await;
        assert!(first.is_first_request_leader());

        let sibling = tokio::time::timeout(
            Duration::from_millis(10),
            pool.acquire_cache_warmup(
                "low-reuse-cohort",
                sibling_prompt.as_bytes(),
                Duration::ZERO,
                true,
            ),
        )
        .await
        .expect("a low-reuse sibling must not wait for the first provider request");
        assert!(sibling.bypassed_low_reuse);
        assert!(!sibling.waited);
        assert!(!sibling.is_first_request_leader());
        assert!(!sibling.is_common_prefix_leader());

        first.finish(true);
        let later = pool
            .acquire_cache_warmup(
                "low-reuse-cohort",
                sibling_prompt.as_bytes(),
                Duration::ZERO,
                true,
            )
            .await;
        assert!(later.bypassed_low_reuse);
        assert!(!later.is_common_prefix_leader());
    }

    #[tokio::test]
    async fn distinct_cold_cohorts_are_admitted_concurrently() {
        let pool = ProviderTransportPool::new(8);
        let first = pool
            .acquire_cache_warmup("cohort-a", b"stable-a", Duration::ZERO, true)
            .await;
        let second = pool
            .acquire_cache_warmup("cohort-b", b"stable-b", Duration::ZERO, true)
            .await;
        assert!(first.is_leader());
        assert!(second.is_leader());
    }

    #[test]
    fn account_failure_fence_is_scoped_by_account_revision_and_failure_scope() {
        let pool = ProviderTransportPool::new(2);
        assert!(!pool.record_provider_failure(1, "token-plan", ProviderFailureScope::Request));
        assert!(!pool.provider_account_is_fenced(1, "token-plan"));

        assert!(pool.record_provider_failure(1, "token-plan", ProviderFailureScope::Account));
        assert!(pool.provider_account_is_fenced(1, "token-plan"));
        assert!(!pool.provider_account_is_fenced(1, "deepseek"));
        assert!(!pool.provider_account_is_fenced(2, "token-plan"));
        assert_eq!(pool.stats().account_failure_fences, 1);
    }

    #[test]
    fn account_failure_fence_expires_without_process_restart() {
        let pool = ProviderTransportPool::new_with_ttls(2, Duration::from_secs(60), Duration::ZERO);
        assert!(pool.record_provider_failure(1, "token-plan", ProviderFailureScope::Account));
        assert!(!pool.provider_account_is_fenced(1, "token-plan"));
        assert_eq!(pool.stats().account_failure_fences, 0);
    }
}
