//! Process-wide live SQLite pool counter.
//!
//! Every crate that constructs an r2d2 SQLite pool registers a guard here.
//! Gateway doctor/cleanup can then prove that a PostgreSQL-mode process holds
//! zero live SQLite pools without scanning `/proc/<pid>/fd` or duplicating
//! construction knowledge across crates. The guard is RAII: dropping the
//! owner decrements the count, so a leaked pool can never be hidden.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static LIVE_SQLITE_POOLS: AtomicUsize = AtomicUsize::new(0);

/// RAII registration of one live SQLite pool. Keep it in the same struct that
/// owns the pool so the count follows the pool's actual lifetime.
#[derive(Debug)]
pub struct SqlitePoolGuard(Arc<()>);

impl SqlitePoolGuard {
    #[must_use]
    pub fn register() -> Self {
        LIVE_SQLITE_POOLS.fetch_add(1, Ordering::SeqCst);
        Self(Arc::new(()))
    }
}

impl Drop for SqlitePoolGuard {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) == 1 {
            LIVE_SQLITE_POOLS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Clone for SqlitePoolGuard {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// Number of SQLite pools currently alive in this process.
#[must_use]
pub fn live_sqlite_pool_count() -> usize {
    LIVE_SQLITE_POOLS.load(Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_increments_and_releases_the_shared_counter() {
        let before = live_sqlite_pool_count();
        let guard = SqlitePoolGuard::register();
        assert_eq!(live_sqlite_pool_count(), before + 1);
        drop(guard);
        assert_eq!(live_sqlite_pool_count(), before);
    }
}
