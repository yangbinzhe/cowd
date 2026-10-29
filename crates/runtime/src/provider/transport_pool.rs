use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
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

/// Self-check: true when a warm request reuses less than half of its
/// model-visible prefix, the regime where prompt caching stops paying off.
/// Surfaced for observability so a prefix-stability regression is visible
/// without waiting for a cost report.
#[must_use]
pub(crate) fn low_structural_reuse(ratio_bp: u32) -> bool {
    ratio_bp < MIN_COMMON_PREFIX_COORDINATION_RATIO_BP as u32
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderWirePrefixObservation {
    pub cache_identity_sha256: String,
    pub predecessor_request_id: Option<String>,
    pub prompt_bytes: u64,
    pub reusable_prefix_bytes: u64,
    pub structural_reuse_ratio_bp: u32,
    pub exact_extension: bool,
    pub invalidation_reason: Option<String>,
    /// PLAN A2: WHICH segment moved when the prefix diverged. The canonical
    /// prompt carries only the messages, so this is either the stable leading
    /// cohort (expensive: everything after it is re-sent) or the append-only
    /// history (expected). A changed tool schema never shows up here - it
    /// changes the cache identity and is reported as `cold_identity`.
    // `String` rather than `&'static str` because this struct derives
    // Serialize/Deserialize and is persisted as wire evidence.
    pub divergence_segment: Option<String>,
    /// PLAN A1: the per-segment byte composition of this request's model-visible
    /// prompt. Observability only - it is never sent to the provider and never
    /// feeds a cache key, so a segment breakdown can be exported per turn without
    /// changing a single request byte.
    pub composition: Option<crate::PromptComposition>,
    pub cold_leader: bool,
    /// This request serialized one DeepSeek common-prefix discovery after the
    /// first request had persisted. It is not another cold identity leader.
    pub common_prefix_leader: bool,
    pub warmup_bypassed_low_reuse: bool,
    pub waited_for_warmup: bool,
    #[serde(default)]
    pub warmup_unobserved_reason: Option<String>,
    #[serde(default)]
    pub cache_retention_unavailable: Option<String>,
}

#[derive(Debug, Clone)]
struct CachedProviderPrompt {
    request_id: String,
    bytes: std::sync::Arc<CachedPromptBytes>,
}

// Optional optimization residency belongs to this original transport pool,
// independently of execution admission. Charges follow the actual payload Arc.
#[derive(Debug, Clone, Copy)]
struct CacheLimits {
    identities: usize,
    payload_bytes: usize,
    identity_bytes: usize,
    prompt_bytes: usize,
    waiters: usize,
    identity_waiters: usize,
    wait: Duration,
}
impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            identities: MAX_PREFIX_IDENTITIES,
            payload_bytes: 16 * 1024 * 1024,
            identity_bytes: 4 * 1024 * 1024,
            prompt_bytes: 1024 * 1024,
            waiters: 512,
            identity_waiters: 128,
            wait: Duration::from_secs(5),
        }
    }
}
#[derive(Debug)]
struct CacheByteBudget {
    used: AtomicUsize,
    limit: usize,
}
impl CacheByteBudget {
    fn new(limit: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }
    fn reserve(self: &std::sync::Arc<Self>, bytes: usize) -> Option<CacheByteReservation> {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|sum| *sum <= self.limit)
            })
            .ok()?;
        Some(CacheByteReservation {
            budget: self.clone(),
            bytes,
        })
    }
}
struct CacheByteReservation {
    budget: std::sync::Arc<CacheByteBudget>,
    bytes: usize,
}
impl Drop for CacheByteReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
struct CachedPromptBytes {
    bytes: Box<[u8]>,
    _identity: CacheByteReservation,
    _pool: CacheByteReservation,
}
impl std::ops::Deref for CachedPromptBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::fmt::Debug for CachedPromptBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPromptBytes")
            .field("len", &self.bytes.len())
            .finish()
    }
}
impl CachedPromptBytes {
    fn reserve(
        pool: &std::sync::Arc<CacheByteBudget>,
        entry: &CacheWarmEntry,
        bytes: &[u8],
    ) -> Option<std::sync::Arc<Self>> {
        let identity = entry.resident.reserve(bytes.len())?;
        let global = pool.reserve(bytes.len())?;
        Some(std::sync::Arc::new(Self {
            bytes: bytes.into(),
            _identity: identity,
            _pool: global,
        }))
    }
}
// Constructed immediately after the original registration and before await.
// Cancellation, timeout and normal wakes all use the same decrement path.
struct CacheWaiterRegistration {
    entry: std::sync::Arc<CacheWarmEntry>,
    global: std::sync::Arc<AtomicUsize>,
}
impl Drop for CacheWaiterRegistration {
    fn drop(&mut self) {
        let mut state = self
            .entry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.waiters = state
            .waiters
            .checked_sub(1)
            .expect("original cache waiter registered once");
        self.global.fetch_sub(1, Ordering::AcqRel);
    }
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
    BypassLimit(&'static str),
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
    first_prompt: Option<std::sync::Arc<CachedPromptBytes>>,
    history: VecDeque<CachedProviderPrompt>,
    waiters: usize,
}

#[derive(Debug)]
struct CacheWarmEntry {
    state: Mutex<CacheWarmState>,
    changed: tokio::sync::Notify,
    resident: std::sync::Arc<CacheByteBudget>,
}

pub(crate) struct ProviderCacheWarmupGuard {
    entry: Option<std::sync::Arc<CacheWarmEntry>>,
    leader: Option<CacheWarmLeaderKind>,
    finished: bool,
    pub waited: bool,
    pub bypassed_low_reuse: bool,
    pub unobserved_reason: Option<&'static str>,
    persistence_barrier: Duration,
    requires_common_prefix_discovery: bool,
}

impl ProviderCacheWarmupGuard {
    fn bypass(reason: &'static str, waited: bool) -> Self {
        Self {
            entry: None,
            leader: None,
            finished: true,
            waited,
            bypassed_low_reuse: false,
            unobserved_reason: Some(reason),
            persistence_barrier: Duration::ZERO,
            requires_common_prefix_discovery: false,
        }
    }

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
        let entry = self
            .entry
            .as_ref()
            .expect("original leader retains its cohort");
        let mut state = entry
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
        entry.changed.notify_waiters();
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
    cache_limits: CacheLimits,
    cache_resident: std::sync::Arc<CacheByteBudget>,
    cache_waiters: std::sync::Arc<AtomicUsize>,
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
            cache_limits: CacheLimits::default(),
            cache_resident: CacheByteBudget::new(CacheLimits::default().payload_bytes),
            cache_waiters: std::sync::Arc::new(AtomicUsize::new(0)),
            cache_warm_entries: Mutex::new(HashMap::new()),
        }
    }

    fn cache_entry(
        &self,
        identity: &str,
        additional_bytes: usize,
    ) -> Option<std::sync::Arc<CacheWarmEntry>> {
        if identity.len() > 256 {
            return None;
        }
        let mut entries = self
            .cache_warm_entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = entries.get(identity) {
            return Some(entry.clone());
        }
        while entries.len() >= self.cache_limits.identities
            || self
                .cache_resident
                .used
                .load(Ordering::Acquire)
                .saturating_add(additional_bytes)
                > self.cache_limits.payload_bytes
        {
            // A holder, including a waiter or a finished guard still in use,
            // pins this exact entry. Never split one active cohort in two.
            let victim = entries
                .iter()
                .filter(|(_, entry)| std::sync::Arc::strong_count(entry) == 1)
                .max_by_key(|(_, entry)| entry.resident.used.load(Ordering::Acquire))
                .map(|(key, _)| key.clone());
            if let Some(victim) = victim {
                entries.remove(&victim);
            } else if entries.len() >= self.cache_limits.identities {
                return None;
            } else {
                break;
            }
        }
        let entry = std::sync::Arc::new(CacheWarmEntry {
            state: Mutex::new(CacheWarmState {
                phase: CacheWarmPhase::Cold,
                warmed_at: None,
                first_prompt: None,
                history: VecDeque::new(),
                waiters: 0,
            }),
            changed: tokio::sync::Notify::new(),
            resident: CacheByteBudget::new(self.cache_limits.identity_bytes),
        });
        entries.insert(identity.to_owned(), entry.clone());
        Some(entry)
    }

    /// Coordinate optional cache warming without unbounded residency or wait.
    /// A limit bypass is unobserved optimization, never a successful warm fact.
    pub(crate) async fn acquire_cache_warmup(
        &self,
        cache_identity_sha256: &str,
        canonical_prompt: &[u8],
        persistence_barrier: Duration,
        requires_common_prefix_discovery: bool,
    ) -> ProviderCacheWarmupGuard {
        if canonical_prompt.len() > self.cache_limits.prompt_bytes {
            return ProviderCacheWarmupGuard::bypass("prompt_byte_limit", false);
        }
        let Some(entry) = self.cache_entry(cache_identity_sha256, canonical_prompt.len()) else {
            return ProviderCacheWarmupGuard::bypass("identity_capacity", false);
        };
        let deadline = tokio::time::Instant::now() + self.cache_limits.wait;
        let mut waited = false;
        loop {
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
                let exact = state
                    .first_prompt
                    .as_deref()
                    .is_some_and(|first| canonical_prompt.starts_with(first));
                let common = state.first_prompt.as_deref().is_some_and(|first| {
                    common_prefix_ratio_bp(first, canonical_prompt)
                        >= MIN_COMMON_PREFIX_COORDINATION_RATIO_BP
                });
                match state.phase {
                    CacheWarmPhase::Cold => match CachedPromptBytes::reserve(
                        &self.cache_resident,
                        &entry,
                        canonical_prompt,
                    ) {
                        Some(bytes) => {
                            state.first_prompt = Some(bytes);
                            state.phase = CacheWarmPhase::PrimingFirst;
                            CacheWarmAdmission::Leader(CacheWarmLeaderKind::FirstRequest)
                        }
                        None => CacheWarmAdmission::BypassLimit("resident_byte_capacity"),
                    },
                    CacheWarmPhase::Warm => CacheWarmAdmission::Ready,
                    CacheWarmPhase::FirstRequestPersisted => {
                        if exact {
                            CacheWarmAdmission::Ready
                        } else if common {
                            state.phase = CacheWarmPhase::PrimingCommonPrefix;
                            CacheWarmAdmission::Leader(CacheWarmLeaderKind::CommonPrefix)
                        } else {
                            CacheWarmAdmission::BypassLowReuse
                        }
                    }
                    CacheWarmPhase::PrimingFirst | CacheWarmPhase::PrimingCommonPrefix => {
                        if exact || !requires_common_prefix_discovery || common {
                            if state.waiters >= self.cache_limits.identity_waiters
                                || self
                                    .cache_waiters
                                    .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                                        count
                                            .checked_add(1)
                                            .filter(|sum| *sum <= self.cache_limits.waiters)
                                    })
                                    .is_err()
                            {
                                CacheWarmAdmission::BypassLimit("waiter_capacity")
                            } else {
                                state.waiters += 1;
                                CacheWarmAdmission::Wait
                            }
                        } else {
                            CacheWarmAdmission::BypassLowReuse
                        }
                    }
                }
            };
            match admission {
                CacheWarmAdmission::Wait => {
                    let registration = CacheWaiterRegistration {
                        entry: entry.clone(),
                        global: self.cache_waiters.clone(),
                    };
                    waited = true;
                    let wake = tokio::time::timeout_at(deadline, notified).await;
                    drop(registration);
                    if wake.is_err() {
                        return ProviderCacheWarmupGuard::bypass("wait_deadline", waited);
                    }
                }
                CacheWarmAdmission::BypassLimit(reason) => {
                    return ProviderCacheWarmupGuard::bypass(reason, waited)
                }
                other => {
                    return ProviderCacheWarmupGuard {
                        entry: Some(entry.clone()),
                        leader: match other {
                            CacheWarmAdmission::Leader(kind) => Some(kind),
                            _ => None,
                        },
                        finished: !matches!(other, CacheWarmAdmission::Leader(_)),
                        waited,
                        bypassed_low_reuse: matches!(other, CacheWarmAdmission::BypassLowReuse),
                        unobserved_reason: None,
                        persistence_barrier,
                        requires_common_prefix_discovery,
                    }
                }
            }
        }
    }

    /// Compare one exact canonical model-visible prompt against recent
    /// requests in the same cache-sensitive identity. This is structural
    /// evidence only; Provider usage remains the billing truth.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn observe_prompt_prefix(
        &self,
        cache_identity_sha256: String,
        request_id: String,
        prompt: Vec<u8>,
        stable_head_bytes: usize,
    ) -> ProviderWirePrefixObservation {
        let prompt_bytes = prompt.len();
        let unavailable = |reason: &str| ProviderWirePrefixObservation {
            cache_identity_sha256: cache_identity_sha256.clone(),
            prompt_bytes: prompt_bytes as u64,
            cache_retention_unavailable: Some(reason.into()),
            ..Default::default()
        };
        if prompt_bytes > self.cache_limits.prompt_bytes || request_id.len() > 256 {
            return unavailable("source_byte_limit");
        }
        let Some(entry) = self.cache_entry(&cache_identity_sha256, prompt_bytes) else {
            return unavailable("identity_capacity");
        };
        let mut state = entry
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let history = &mut state.history;
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
        let best_bytes = best.map(|(candidate, _)| candidate.bytes.clone());
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
        // PLAN A2: name the segment that moved, so a falling reuse ratio has a
        // cause instead of only a magnitude.
        let divergence_segment = if best.is_none() || exact_extension {
            None
        } else {
            let attribution = crate::provider_prefix_attribution::attribute_divergence(
                best_bytes.as_ref().map_or(&[][..], |bytes| &bytes[..]),
                &prompt,
                &crate::provider_prefix_attribution::canonical_prompt_layout(
                    stable_head_bytes.min(prompt.len()),
                    prompt.len(),
                ),
            );
            attribution.segment.map(|segment| segment.as_str())
        };
        if best.is_some() && low_structural_reuse(structural_reuse_ratio_bp) {
            tracing::debug!(
                cache_identity_sha256 = %cache_identity_sha256,
                structural_reuse_ratio_bp,
                invalidation_reason = invalidation_reason.as_deref().unwrap_or(""),
                "low provider prompt structural reuse"
            );
        }
        drop(best_bytes);
        // Charge before copying. Eviction of old history releases its bytes
        // only when all actual holders have dropped the payload.
        let mut retained = CachedPromptBytes::reserve(&self.cache_resident, &entry, &prompt);
        while retained.is_none() && !history.is_empty() {
            history.pop_front();
            retained = CachedPromptBytes::reserve(&self.cache_resident, &entry, &prompt);
        }
        let cache_retention_unavailable = if let Some(bytes) = retained {
            history.push_back(CachedProviderPrompt { request_id, bytes });
            while history.len() > MAX_PREFIX_PREDECESSORS {
                history.pop_front();
            }
            None
        } else {
            Some("resident_byte_capacity".into())
        };
        ProviderWirePrefixObservation {
            cache_identity_sha256,
            predecessor_request_id,
            prompt_bytes: prompt_bytes as u64,
            reusable_prefix_bytes: reusable_prefix_bytes as u64,
            structural_reuse_ratio_bp,
            exact_extension,
            invalidation_reason,
            divergence_segment: divergence_segment.map(str::to_owned),
            composition: None,
            cold_leader: false,
            common_prefix_leader: false,
            warmup_bypassed_low_reuse: false,
            waited_for_warmup: false,
            warmup_unobserved_reason: None,
            cache_retention_unavailable,
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

/// PLAN A2, wired: the transport's prefix observer names the segment that moved.
///
/// The real canonical prompt is the messages array encoded as `<len>:<json>\n`,
/// so this exercises exactly that shape against the real `observe_prompt_prefix`.
#[cfg(test)]
fn encode_messages(items: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for item in items {
        out.extend_from_slice(item.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(item.as_bytes());
        out.push(b'\n');
    }
    out
}

#[cfg(test)]
mod prefix_wiring_tests {
    use super::{ProviderTransportPool, encode_messages};

    /// A history that only GROWS is a pure append: reuse is total and there is no
    /// divergence to attribute. That is the healthy case.
    #[test]
    fn a_growing_history_is_a_pure_append_not_a_divergence() {
        let pool = ProviderTransportPool::new(4);
        let stable = 13_usize; // "10:system_msg\n"
        let first = encode_messages(&["system_msg", "history_msg"]);
        pool.observe_prompt_prefix(
            "sha256:id".to_string(),
            "r1".to_string(),
            first.clone(),
            stable,
        );

        let second = encode_messages(&["system_msg", "history_msg", "history_msg2"]);
        let observation =
            pool.observe_prompt_prefix("sha256:id".to_string(), "r2".to_string(), second, stable);

        assert!(
            observation.exact_extension,
            "the first prompt is a prefix of the second"
        );
        assert_eq!(
            observation.divergence_segment, None,
            "a pure append is not a divergence"
        );
        assert_eq!(observation.reusable_prefix_bytes as usize, first.len());
    }

    /// A REWRITTEN history is attributed to the message segment: expected, and
    /// still much cheaper than a moving stable head.
    #[test]
    fn a_rewritten_history_is_attributed_to_the_message_segment() {
        let pool = ProviderTransportPool::new(4);
        let first = encode_messages(&["system_msg", "history_msg"]);
        pool.observe_prompt_prefix("sha256:id".to_string(), "r1".to_string(), first, 13);

        let second = encode_messages(&["system_msg", "history_msgX"]);
        let observation =
            pool.observe_prompt_prefix("sha256:id".to_string(), "r2".to_string(), second, 13);

        assert_eq!(observation.divergence_segment.as_deref(), Some("messages"));
        assert!(!observation.exact_extension);
    }

    #[test]
    fn a_moving_stable_head_is_attributed_to_the_system_segment() {
        let pool = ProviderTransportPool::new(4);
        let first = encode_messages(&["system_msgA", "history_msg"]);
        pool.observe_prompt_prefix("sha256:id".to_string(), "r1".to_string(), first, 14);

        let second = encode_messages(&["system_msgB", "history_msg"]);
        let observation =
            pool.observe_prompt_prefix("sha256:id".to_string(), "r2".to_string(), second, 14);

        assert_eq!(
            observation.divergence_segment.as_deref(),
            Some("system"),
            "a moving stable head is the expensive case and must be named"
        );
    }

    #[test]
    fn a_cold_identity_has_no_divergence_to_attribute() {
        let pool = ProviderTransportPool::new(4);
        let observation = pool.observe_prompt_prefix(
            "sha256:brand-new".to_string(),
            "r1".to_string(),
            encode_messages(&["system_msg"]),
            13,
        );
        assert_eq!(observation.divergence_segment, None);
        assert_eq!(
            observation.invalidation_reason.as_deref(),
            Some("cold_identity")
        );
    }

    #[test]
    fn a_pure_append_has_no_divergence() {
        let pool = ProviderTransportPool::new(4);
        let first = encode_messages(&["system_msg", "history_msg"]);
        pool.observe_prompt_prefix("sha256:id".to_string(), "r1".to_string(), first.clone(), 13);

        let mut second = first.clone();
        second.extend_from_slice(b"13:history_msg2\n");
        let observation =
            pool.observe_prompt_prefix("sha256:id".to_string(), "r2".to_string(), second, 13);
        assert_eq!(observation.divergence_segment, None);
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

    fn small_cache(limits: super::CacheLimits) -> ProviderTransportPool {
        let mut pool = ProviderTransportPool::new(2);
        pool.cache_limits = limits;
        pool.cache_resident = super::CacheByteBudget::new(limits.payload_bytes);
        pool
    }

    #[tokio::test]
    async fn full_identity_table_cannot_evict_an_active_leader_or_split_its_waiters() {
        let pool = Arc::new(small_cache(super::CacheLimits {
            identities: 1,
            ..Default::default()
        }));
        let mut leader = pool
            .acquire_cache_warmup("a", b"prefix", Duration::ZERO, false)
            .await;
        let rejected = pool
            .acquire_cache_warmup("b", b"other", Duration::ZERO, false)
            .await;
        assert_eq!(rejected.unobserved_reason, Some("identity_capacity"));
        assert!(!rejected.is_leader());
        let original = pool
            .cache_warm_entries
            .lock()
            .unwrap()
            .get("a")
            .unwrap()
            .clone();
        let other = pool.clone();
        let follower = tokio::spawn(async move {
            other
                .acquire_cache_warmup("a", b"prefix-more", Duration::ZERO, false)
                .await
        });
        wait_for_cache_waiters(&pool, "a", 1).await;
        assert!(Arc::ptr_eq(
            &original,
            pool.cache_warm_entries.lock().unwrap().get("a").unwrap()
        ));
        assert_eq!(pool.cache_warm_entries.lock().unwrap().len(), 1);
        leader.finish(true);
        let follower = follower.await.unwrap();
        assert!(follower.waited);
        assert!(!follower.is_leader());
        drop(follower);
        drop(original);
        drop(leader);
        let next = pool
            .acquire_cache_warmup("b", b"other", Duration::ZERO, false)
            .await;
        assert!(next.is_leader());
    }

    #[tokio::test]
    async fn cancelled_and_expired_waiters_release_both_original_counts() {
        use std::sync::atomic::Ordering;
        let pool = Arc::new(small_cache(super::CacheLimits {
            waiters: 1,
            identity_waiters: 1,
            wait: Duration::from_millis(100),
            ..Default::default()
        }));
        let leader = pool
            .acquire_cache_warmup("a", b"first", Duration::ZERO, false)
            .await;
        let other = pool.clone();
        let follower = tokio::spawn(async move {
            other
                .acquire_cache_warmup("a", b"first-more", Duration::ZERO, false)
                .await
        });
        wait_for_cache_waiters(&pool, "a", 1).await;
        let rejected = pool
            .acquire_cache_warmup("a", b"first-tail", Duration::ZERO, false)
            .await;
        assert_eq!(rejected.unobserved_reason, Some("waiter_capacity"));
        assert!(!rejected.is_leader());
        follower.abort();
        assert!(matches!(follower.await,Err(error) if error.is_cancelled()));
        wait_for_cache_waiters(&pool, "a", 0).await;
        assert_eq!(pool.cache_waiters.load(Ordering::Acquire), 0);
        let expired = pool
            .acquire_cache_warmup("a", b"first-tail", Duration::ZERO, false)
            .await;
        assert_eq!(expired.unobserved_reason, Some("wait_deadline"));
        assert!(expired.waited);
        assert!(!expired.is_leader());
        wait_for_cache_waiters(&pool, "a", 0).await;
        assert_eq!(pool.cache_waiters.load(Ordering::Acquire), 0);
        assert!(leader.is_leader());
        drop(leader);
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn global_waiter_bound_is_shared_across_independent_cohorts() {
        use std::sync::atomic::Ordering;
        let pool = Arc::new(small_cache(super::CacheLimits {
            waiters: 1,
            identity_waiters: 2,
            ..Default::default()
        }));
        let leader_a = pool
            .acquire_cache_warmup("a", b"prefix", Duration::ZERO, false)
            .await;
        let mut leader_b = pool
            .acquire_cache_warmup("b", b"prefix", Duration::ZERO, false)
            .await;
        let other = pool.clone();
        let first = tokio::spawn(async move {
            other
                .acquire_cache_warmup("a", b"prefix-more", Duration::ZERO, false)
                .await
        });
        wait_for_cache_waiters(&pool, "a", 1).await;
        assert_eq!(pool.cache_waiters.load(Ordering::Acquire), 1);
        let refused = pool
            .acquire_cache_warmup("b", b"prefix-more", Duration::ZERO, false)
            .await;
        assert_eq!(refused.unobserved_reason, Some("waiter_capacity"));
        assert!(!refused.is_leader());
        wait_for_cache_waiters(&pool, "b", 0).await;
        first.abort();
        assert!(matches!(first.await,Err(error) if error.is_cancelled()));
        assert_eq!(pool.cache_waiters.load(Ordering::Acquire), 0);
        let other = pool.clone();
        let next = tokio::spawn(async move {
            other
                .acquire_cache_warmup("b", b"prefix-more", Duration::ZERO, false)
                .await
        });
        wait_for_cache_waiters(&pool, "b", 1).await;
        leader_b.finish(true);
        assert!(next.await.unwrap().waited);
        assert_eq!(pool.cache_waiters.load(Ordering::Acquire), 0);
        drop(leader_a);
    }

    #[tokio::test]
    async fn one_identity_cannot_consume_the_whole_pool_budget() {
        use std::sync::atomic::Ordering;
        let pool = small_cache(super::CacheLimits {
            payload_bytes: 64,
            identity_bytes: 8,
            prompt_bytes: 8,
            ..Default::default()
        });
        let first = pool
            .acquire_cache_warmup("a", b"1234", Duration::ZERO, false)
            .await;
        pool.observe_prompt_prefix("a".into(), "r1".into(), b"1234".to_vec(), 0);
        let later = pool.observe_prompt_prefix("a".into(), "r2".into(), b"1234".to_vec(), 0);
        assert_eq!(later.predecessor_request_id.as_deref(), Some("r1"));
        assert!(later.cache_retention_unavailable.is_none());
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 8);
        let entries = pool.cache_warm_entries.lock().unwrap();
        let state = entries.get("a").unwrap().state.lock().unwrap();
        assert_eq!(state.history.len(), 1);
        assert_eq!(state.history[0].request_id, "r2");
        drop(state);
        drop(entries);
        let independent = pool
            .acquire_cache_warmup("b", b"abcdefgh", Duration::ZERO, false)
            .await;
        assert!(independent.is_leader());
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 16);
        drop(first);
    }

    #[tokio::test]
    async fn payload_budget_survives_pool_eviction_until_the_actual_arc_is_dropped() {
        use std::sync::atomic::Ordering;
        let pool = small_cache(super::CacheLimits {
            payload_bytes: 8,
            identity_bytes: 8,
            prompt_bytes: 8,
            ..Default::default()
        });
        let leader = pool
            .acquire_cache_warmup("a", b"12345678", Duration::ZERO, false)
            .await;
        let bypass = pool
            .acquire_cache_warmup("b", b"1234", Duration::ZERO, false)
            .await;
        assert_eq!(bypass.unobserved_reason, Some("resident_byte_capacity"));
        assert!(!bypass.is_leader());
        let no_copy = pool.observe_prompt_prefix("a".into(), "r0".into(), b"12345678".to_vec(), 0);
        assert_eq!(
            no_copy.cache_retention_unavailable.as_deref(),
            Some("resident_byte_capacity")
        );
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 8);
        drop(leader);
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 0);
        let mut next = pool
            .acquire_cache_warmup("b", b"1234", Duration::ZERO, false)
            .await;
        assert!(next.is_leader());
        let observed = pool.observe_prompt_prefix("b".into(), "r1".into(), b"1234".to_vec(), 0);
        assert!(observed.cache_retention_unavailable.is_none());
        next.finish(true);
        drop(next);
        let retained = pool
            .cache_warm_entries
            .lock()
            .unwrap()
            .get("b")
            .unwrap()
            .state
            .lock()
            .unwrap()
            .history[0]
            .bytes
            .clone();
        let budget = pool.cache_resident.clone();
        assert_eq!(budget.used.load(Ordering::Acquire), 8);
        drop(pool);
        assert_eq!(budget.used.load(Ordering::Acquire), 4);
        drop(retained);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn prefix_bytes_and_metadata_are_bounded_and_inactive_payload_can_be_reclaimed() {
        use std::sync::atomic::Ordering;
        let pool = small_cache(super::CacheLimits {
            payload_bytes: 8,
            identity_bytes: 8,
            prompt_bytes: 8,
            ..Default::default()
        });
        pool.observe_prompt_prefix("a".into(), "r1".into(), b"12345678".to_vec(), 0);
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 8);
        let new = pool.observe_prompt_prefix("b".into(), "r2".into(), b"abcd".to_vec(), 0);
        assert!(new.cache_retention_unavailable.is_none());
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 4);
        let oversized =
            pool.observe_prompt_prefix("c".into(), "r3".into(), b"123456789".to_vec(), 0);
        assert_eq!(oversized.prompt_bytes, 9);
        assert_eq!(
            oversized.cache_retention_unavailable.as_deref(),
            Some("source_byte_limit")
        );
        let identity =
            pool.observe_prompt_prefix("x".repeat(257), "r4".into(), b"data".to_vec(), 0);
        assert_eq!(
            identity.cache_retention_unavailable.as_deref(),
            Some("identity_capacity")
        );
        let request = pool.observe_prompt_prefix("d".into(), "x".repeat(257), b"data".to_vec(), 0);
        assert_eq!(
            request.cache_retention_unavailable.as_deref(),
            Some("source_byte_limit")
        );
        assert_eq!(pool.cache_resident.used.load(Ordering::Acquire), 4);
        assert!(pool.cache_warm_entries.lock().unwrap().len() <= 2);
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
            0,
        );
        assert_eq!(cold.invalidation_reason.as_deref(), Some("cold_identity"));
        assert_eq!(cold.structural_reuse_ratio_bp, 0);

        let warm = pool.observe_prompt_prefix(
            "identity-a".to_string(),
            "request-2".to_string(),
            b"stable-history-new-delta".to_vec(),
            0,
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
            0,
        );
        let other = pool.observe_prompt_prefix(
            "identity-b".to_string(),
            "request-2".to_string(),
            b"same-prefix-more".to_vec(),
            0,
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
