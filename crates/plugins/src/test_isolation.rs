// Test isolation utilities for plugin tests
// ROADMAP #41: Stop ambient plugin state from skewing CLI regression checks

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Lock for test environment isolation
pub struct EnvLock {
    // Environment drops before the original mutex guard releases.
    environment: IsolatedEnvironment,
    _guard: std::sync::MutexGuard<'static, ()>,
}

struct IsolatedEnvironment {
    temp_home: PathBuf,
    previous: [(&'static str, Option<OsString>); 3],
}

impl EnvLock {
    /// Acquire environment lock for test isolation
    pub fn lock() -> Self {
        let guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            environment: IsolatedEnvironment::new(),
            _guard: guard,
        }
    }

    /// Get the temporary home directory for this test
    #[must_use]
    pub fn temp_home(&self) -> &PathBuf {
        &self.environment.temp_home
    }
}

impl IsolatedEnvironment {
    // Caller holds the original ENV_LOCK throughout construction and drop.
    fn new() -> Self {
        let previous =
            ["HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME"].map(|name| (name, env::var_os(name)));
        let temp_home = loop {
            let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = env::temp_dir().join(format!("plugin-test-{}-{count}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => break path,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create isolated plugin home {}: {error}", path.display()),
            }
        };
        let isolated = Self {
            temp_home,
            previous,
        };
        std::fs::create_dir_all(isolated.temp_home.join(".cowd/plugins/installed"))
            .expect("create isolated plugin directories");
        std::fs::create_dir_all(isolated.temp_home.join(".config"))
            .expect("create isolated config directory");
        env::set_var("HOME", &isolated.temp_home);
        env::set_var("XDG_CONFIG_HOME", isolated.temp_home.join(".config"));
        env::set_var("XDG_DATA_HOME", isolated.temp_home.join(".local/share"));
        isolated
    }
}

impl Drop for IsolatedEnvironment {
    fn drop(&mut self) {
        for (name, value) in &self.previous {
            match value {
                Some(value) => env::set_var(name, value),
                None => env::remove_var(name),
            }
        }
        // Only remove the directory exclusively created by this instance.
        std::fs::remove_dir_all(&self.temp_home).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_lock_creates_isolated_home() {
        let lock = EnvLock::lock();
        let home = env::var("HOME").unwrap();
        assert!(home.contains("plugin-test-"));
        assert_eq!(home, lock.temp_home().to_str().unwrap());
    }

    #[test]
    fn test_env_lock_creates_plugin_directories() {
        let lock = EnvLock::lock();
        let plugins_dir = lock.temp_home().join(".cowd/plugins/installed");
        assert!(plugins_dir.exists());
    }

    #[test]
    fn restores_present_absent_and_unwinding_environment() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        struct Restore([(&'static str, Option<OsString>); 3]);
        impl Drop for Restore {
            fn drop(&mut self) {
                for (name, value) in &self.0 {
                    match value {
                        Some(value) => env::set_var(name, value),
                        None => env::remove_var(name),
                    }
                }
            }
        }
        let names = ["HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME"];
        let _restore = Restore(names.map(|name| (name, env::var_os(name))));
        for present in [true, false] {
            for unwind in [false, true] {
                for name in names {
                    if present {
                        env::set_var(name, format!("original-{name}"));
                    } else {
                        env::remove_var(name);
                    }
                }
                let expected = names.map(env::var_os);
                let mut root = None;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let isolated = IsolatedEnvironment::new();
                    root = Some(isolated.temp_home.clone());
                    assert_eq!(
                        env::var_os("HOME"),
                        Some(isolated.temp_home.clone().into_os_string())
                    );
                    assert_eq!(
                        env::var_os("XDG_CONFIG_HOME"),
                        Some(isolated.temp_home.join(".config").into_os_string())
                    );
                    assert_eq!(
                        env::var_os("XDG_DATA_HOME"),
                        Some(isolated.temp_home.join(".local/share").into_os_string())
                    );
                    assert!(isolated.temp_home.join(".cowd/plugins/installed").is_dir());
                    if unwind {
                        panic!("exercise isolated environment unwind");
                    }
                }));
                assert_eq!(result.is_err(), unwind);
                assert_eq!(names.map(env::var_os), expected);
                assert!(!root.expect("isolated root created").exists());
            }
        }
    }
}
