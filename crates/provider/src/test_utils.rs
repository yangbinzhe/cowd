//! Shared test utilities for the `api` crate.
//!
//! Provides a single global `env_lock()` to serialize all tests that mutate
//! process-wide environment variables (e.g. `COWD_CONFIG_HOME`). Without a
//! global lock, tests in different modules using independent `static Mutex`
//! instances can race on the same env var across threads.

use std::ffi::OsString;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Global lock that serializes *all* tests that mutate process-global
/// environment variables. Unlike module-local locks, tests in
/// `client.rs`, `providers/anthropic.rs`, and `providers/mod.rs` all
/// contend for this same lock — preventing `COWD_CONFIG_HOME` races.
pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Unique temporary directory for test-scoped config/credential storage.
/// Uses PID + nanosecond timestamp to avoid collisions between parallel
/// `cargo test` invocations on the same host.
pub(crate) fn temp_config_home() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "api-oauth-test-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ))
}

/// RAII guard that snapshot-restores a single environment variable.
///
/// Captures the original value on construction, applies the requested
/// override (set or remove), and restores the original on drop. This
/// leaves the process env untouched even when tests panic mid-assertion.
pub(crate) struct EnvVarGuard {
    key: &'static str,
    original: Option<OsString>,
}

impl EnvVarGuard {
    pub(crate) fn set(key: &'static str, value: Option<&str>) -> Self {
        let original = std::env::var_os(key);
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
        Self { key, original }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.original.take() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_var_guard_restores_on_drop() {
        let _lock = env_lock();
        std::env::set_var("__TEST_ENV_VAR", "before");
        {
            let _guard = EnvVarGuard::set("__TEST_ENV_VAR", Some("during"));
            assert_eq!(std::env::var("__TEST_ENV_VAR").unwrap(), "during");
        }
        assert_eq!(std::env::var("__TEST_ENV_VAR").unwrap(), "before");
        std::env::remove_var("__TEST_ENV_VAR");
    }

    #[test]
    fn env_var_guard_removes_when_value_is_none() {
        let _lock = env_lock();
        std::env::set_var("__TEST_ENV_VAR", "before");
        {
            let _guard = EnvVarGuard::set("__TEST_ENV_VAR", None);
            assert!(std::env::var("__TEST_ENV_VAR").is_err());
        }
        assert_eq!(std::env::var("__TEST_ENV_VAR").unwrap(), "before");
        std::env::remove_var("__TEST_ENV_VAR");
    }

    #[test]
    fn temp_config_home_is_unique() {
        let a = temp_config_home();
        let b = temp_config_home();
        assert_ne!(a, b);
    }
}
