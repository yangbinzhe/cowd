//! Circuit-breaker and recursion guard for the compression pipeline.
//!
//! Prevents runaway compression loops by:
//! 1. Tracking the current compression depth (recursion guard).
//! 2. Counting consecutive failures and tripping an open-circuit state when
//!    the threshold is exceeded (circuit-breaker).
//! 3. Enforcing a cooldown period before the circuit can be reset.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{compression::Result, error::MemoryError};

/// Maximum nesting depth before the recursion guard trips.
const DEFAULT_MAX_DEPTH: u32 = 4;
/// Default number of consecutive failures before the circuit opens.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default cooldown in seconds after the circuit trips.
const DEFAULT_COOLDOWN_SECS: u32 = 30;

// ─── CompressionGuard ────────────────────────────────────────────────────────

/// Circuit-breaker and recursion guard for the compression pipeline.
///
/// # Thread safety
/// All fields use atomic types or `Mutex`, so this type is `Send + Sync` and
/// can be shared freely across threads / tasks via `Arc`.
#[derive(Clone)]
pub struct CompressionGuard {
    // --- Recursion guard ---
    /// Current compression nesting depth.
    depth: Arc<AtomicU32>,
    /// Maximum allowed nesting depth.
    max_depth: u32,

    // --- Circuit-breaker ---
    /// True while a compression operation is in flight (single-flight guard).
    is_compressing: Arc<AtomicBool>,
    /// Consecutive failure count.
    failure_count: Arc<AtomicU32>,
    /// Number of consecutive failures that trip the breaker.
    max_retries: u32,
    /// How long the circuit stays open before an automatic reset is allowed.
    cooldown: Duration,
    /// Timestamp of the last time the circuit was tripped; `None` = closed.
    last_circuit_break: Arc<Mutex<Option<Instant>>>,
}

impl CompressionGuard {
    /// Create a guard with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Create a guard with a custom maximum depth.
    #[must_use]
    pub fn with_max_depth(max_depth: u32) -> Self {
        Self::builder().max_depth(max_depth).build()
    }

    /// Return a [`GuardBuilder`] for fine-grained configuration.
    #[must_use]
    pub fn builder() -> GuardBuilder {
        GuardBuilder::default()
    }

    // ─── Recursion guard ─────────────────────────────────────────────────────

    /// Enter a compression scope, incrementing the depth counter.
    ///
    /// Returns `Err` if the circuit is open **or** the maximum depth has been
    /// reached.  On success, returns a [`CompressionScope`] that decrements the
    /// counter on drop.
    pub fn enter(&self) -> Result<CompressionScope> {
        // Check circuit-breaker first.
        if self.is_circuit_open() {
            return Err(MemoryError::Compression(
                "compression circuit-breaker is open – too many consecutive failures".into(),
            ));
        }

        let prev = self.depth.fetch_add(1, Ordering::SeqCst);
        if prev >= self.max_depth {
            self.depth.fetch_sub(1, Ordering::SeqCst);
            return Err(MemoryError::Compression(format!(
                "recursion guard tripped at depth {prev}"
            )));
        }

        Ok(CompressionScope {
            depth: Arc::clone(&self.depth),
        })
    }

    /// Return `true` if the recursion depth is at the maximum.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.depth.load(Ordering::SeqCst) >= self.max_depth
    }

    // ─── Single-flight permit ────────────────────────────────────────────────

    /// Try to acquire an exclusive compression permit.
    ///
    /// Returns `Err` if another compression is already in progress or if the
    /// circuit-breaker is open.  The returned [`CompressionPermit`] releases
    /// the lock when dropped.
    pub fn try_acquire(&self) -> Result<CompressionPermit<'_>> {
        if self.is_circuit_open() {
            return Err(MemoryError::Compression(
                "compression circuit-breaker is open".into(),
            ));
        }

        // CAS: false → true
        self.is_compressing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| {
                MemoryError::Compression(
                    "another compression is already in progress".into(),
                )
            })?;

        Ok(CompressionPermit { guard: self })
    }

    // ─── Circuit-breaker ─────────────────────────────────────────────────────

    /// Report a successful compression run – resets the failure counter.
    pub fn report_success(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        // Also clear any lingering circuit-break timestamp.
        if let Ok(mut lock) = self.last_circuit_break.lock() {
            *lock = None;
        }
    }

    /// Report a failed compression run.
    ///
    /// If the failure count reaches `max_retries`, the circuit is tripped and a
    /// cooldown timestamp is recorded.
    pub fn report_failure(&self) {
        let prev = self.failure_count.fetch_add(1, Ordering::SeqCst);
        if prev + 1 >= self.max_retries {
            // Trip the circuit.
            if let Ok(mut lock) = self.last_circuit_break.lock() {
                *lock = Some(Instant::now());
            }
        }
    }

    /// Return `true` if the circuit is currently open (breaker tripped).
    ///
    /// The circuit stays open until the cooldown period has elapsed, after which
    /// [`reset`](Self::reset) must be called explicitly.
    #[must_use]
    pub fn is_circuit_open(&self) -> bool {
        let lock = match self.last_circuit_break.lock() {
            Ok(l) => l,
            Err(_) => return false,
        };
        if let Some(tripped_at) = *lock {
            tripped_at.elapsed() < self.cooldown
        } else {
            false
        }
    }

    /// Reset the circuit-breaker unconditionally.
    ///
    /// After calling this the guard accepts new operations even if the cooldown
    /// has not yet elapsed.  Also resets the failure counter.
    pub fn reset(&self) {
        self.failure_count.store(0, Ordering::SeqCst);
        if let Ok(mut lock) = self.last_circuit_break.lock() {
            *lock = None;
        }
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    fn release_permit(&self) {
        self.is_compressing.store(false, Ordering::SeqCst);
    }
}

impl Default for CompressionGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ─── GuardBuilder ─────────────────────────────────────────────────────────────

/// Builder for [`CompressionGuard`].
#[derive(Debug)]
pub struct GuardBuilder {
    max_depth: u32,
    max_retries: u32,
    cooldown_secs: u32,
}

impl Default for GuardBuilder {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_retries: DEFAULT_MAX_RETRIES,
            cooldown_secs: DEFAULT_COOLDOWN_SECS,
        }
    }
}

impl GuardBuilder {
    #[must_use] 
    pub fn max_depth(mut self, v: u32) -> Self {
        self.max_depth = v;
        self
    }

    #[must_use] 
    pub fn max_retries(mut self, v: u32) -> Self {
        self.max_retries = v;
        self
    }

    #[must_use] 
    pub fn cooldown_secs(mut self, v: u32) -> Self {
        self.cooldown_secs = v;
        self
    }

    #[must_use]
    pub fn build(self) -> CompressionGuard {
        CompressionGuard {
            depth: Arc::new(AtomicU32::new(0)),
            max_depth: self.max_depth,
            is_compressing: Arc::new(AtomicBool::new(false)),
            failure_count: Arc::new(AtomicU32::new(0)),
            max_retries: self.max_retries,
            cooldown: Duration::from_secs(u64::from(self.cooldown_secs)),
            last_circuit_break: Arc::new(Mutex::new(None)),
        }
    }
}

// ─── RAII types ───────────────────────────────────────────────────────────────

/// RAII scope guard that decrements the recursion depth on drop.
pub struct CompressionScope {
    depth: Arc<AtomicU32>,
}

impl Drop for CompressionScope {
    fn drop(&mut self) {
        self.depth.fetch_sub(1, Ordering::SeqCst);
    }
}

/// RAII permit that releases the single-flight lock on drop.
pub struct CompressionPermit<'a> {
    guard: &'a CompressionGuard,
}

impl Drop for CompressionPermit<'_> {
    fn drop(&mut self) {
        self.guard.release_permit();
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursion_guard_trips_at_max_depth() {
        let g = CompressionGuard::with_max_depth(2);
        let _s1 = g.enter().unwrap();
        let _s2 = g.enter().unwrap();
        assert!(g.enter().is_err());
    }

    #[test]
    fn recursion_depth_decrements_on_drop() {
        let g = CompressionGuard::with_max_depth(2);
        {
            let _s = g.enter().unwrap();
            assert_eq!(g.depth.load(Ordering::SeqCst), 1);
        }
        assert_eq!(g.depth.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn single_flight_blocks_concurrent_acquire() {
        let g = CompressionGuard::new();
        let _permit = g.try_acquire().unwrap();
        assert!(g.try_acquire().is_err());
    }

    #[test]
    fn permit_released_on_drop() {
        let g = CompressionGuard::new();
        {
            let _p = g.try_acquire().unwrap();
        }
        assert!(g.try_acquire().is_ok());
    }

    #[test]
    fn circuit_opens_after_max_retries() {
        let g = CompressionGuard::builder().max_retries(2).cooldown_secs(60).build();
        assert!(!g.is_circuit_open());
        g.report_failure();
        assert!(!g.is_circuit_open());
        g.report_failure();
        assert!(g.is_circuit_open());
    }

    #[test]
    fn reset_clears_circuit() {
        let g = CompressionGuard::builder().max_retries(1).cooldown_secs(60).build();
        g.report_failure();
        assert!(g.is_circuit_open());
        g.reset();
        assert!(!g.is_circuit_open());
        assert!(g.try_acquire().is_ok());
    }

    #[test]
    fn report_success_resets_failure_count() {
        let g = CompressionGuard::builder().max_retries(3).cooldown_secs(60).build();
        g.report_failure();
        g.report_failure();
        g.report_success();
        // Two more failures should not trip the circuit (count was reset).
        g.report_failure();
        g.report_failure();
        assert!(!g.is_circuit_open());
    }
}
