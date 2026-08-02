use std::path::Path;
use std::time::{Duration, SystemTime};

pub(crate) fn init_logging(version: &str) -> bool {
    use tracing_appender::rolling;
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let log_dir = runtime::cowd_dirs::config_home_dir().join("logs");
    let _ = std::fs::create_dir_all(&log_dir);
    let retention = log_retention_config_from_env();
    let cleanup_summary = cleanup_cowd_logs(&log_dir, &retention).ok();

    let file_appender = rolling::daily(&log_dir, "cowd");
    let default_level = if cfg!(debug_assertions) {
        // Keep Cowd diagnostics detailed without enabling per-statement DEBUG
        // logging in database and network dependencies.
        "info,gateway=debug"
    } else {
        "warn,gateway=info"
    };
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let json_logs = std::env::var("COWD_LOG_FORMAT")
        .map(|value| value.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    let stderr_enabled = cfg!(debug_assertions) && std::env::var("COWD_LOG_STDERR").is_ok();

    let installed = if json_logs {
        let file_layer = fmt::layer()
            .json()
            .with_target(true)
            .with_thread_ids(true)
            .with_writer(file_appender);
        if stderr_enabled {
            let stderr_layer = fmt::layer().with_target(false).with_writer(std::io::stderr);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(file_layer)
                .with(stderr_layer)
                .try_init()
                .is_ok()
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(file_layer)
                .try_init()
                .is_ok()
        }
    } else {
        let file_layer = fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_writer(file_appender);
        if stderr_enabled {
            let stderr_layer = fmt::layer().with_target(false).with_writer(std::io::stderr);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(file_layer)
                .with(stderr_layer)
                .try_init()
                .is_ok()
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(file_layer)
                .try_init()
                .is_ok()
        }
    };

    tracing::info!(installed, "COWD v{} logging initialized", version);
    if let Some(summary) = cleanup_summary {
        tracing::info!(
            removed_files = summary.removed_files,
            removed_bytes = summary.removed_bytes,
            retained_bytes = summary.retained_bytes,
            retention_days = retention.retention_days,
            max_total_bytes = retention.max_total_bytes,
            json_logs,
            "log retention applied"
        );
    }

    installed
}

#[derive(Debug, Clone, Copy)]
struct LogRetentionConfig {
    retention_days: u64,
    max_total_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LogCleanupSummary {
    removed_files: usize,
    removed_bytes: u64,
    retained_bytes: u64,
}

fn log_retention_config_from_env() -> LogRetentionConfig {
    LogRetentionConfig {
        retention_days: read_u64_env("COWD_LOG_RETENTION_DAYS").unwrap_or(14),
        max_total_bytes: read_u64_env("COWD_LOG_MAX_BYTES").unwrap_or(256 * 1024 * 1024),
    }
}

fn read_u64_env(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn cleanup_cowd_logs(
    log_dir: &Path,
    config: &LogRetentionConfig,
) -> std::io::Result<LogCleanupSummary> {
    let mut summary = LogCleanupSummary::default();
    let mut retained = Vec::new();
    let now = SystemTime::now();
    let max_age = if config.retention_days == 0 {
        None
    } else {
        Some(Duration::from_secs(
            config.retention_days.saturating_mul(24 * 60 * 60),
        ))
    };

    for entry in std::fs::read_dir(log_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with("cowd.") || !path.is_file() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        let size = metadata.len();
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let expired = max_age
            .and_then(|age| {
                now.duration_since(modified)
                    .ok()
                    .map(|elapsed| elapsed > age)
            })
            .unwrap_or(false);
        if expired {
            if std::fs::remove_file(&path).is_ok() {
                summary.removed_files += 1;
                summary.removed_bytes = summary.removed_bytes.saturating_add(size);
            }
        } else {
            summary.retained_bytes = summary.retained_bytes.saturating_add(size);
            retained.push((modified, path, size));
        }
    }

    if config.max_total_bytes > 0 && summary.retained_bytes > config.max_total_bytes {
        retained.sort_by_key(|(modified, _, _)| *modified);
        for (_, path, size) in retained {
            if summary.retained_bytes <= config.max_total_bytes {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                summary.removed_files += 1;
                summary.removed_bytes = summary.removed_bytes.saturating_add(size);
                summary.retained_bytes = summary.retained_bytes.saturating_sub(size);
            }
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::{cleanup_cowd_logs, init_logging, LogRetentionConfig};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("cowd-logs-{nanos}-{}", std::process::id()))
    }

    #[test]
    fn cleanup_cowd_logs_limits_total_size_without_removing_unrelated_files() {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("create log fixture dir");
        fs::write(root.join("cowd.2026-01-01"), vec![b'a'; 80]).expect("write log a");
        fs::write(root.join("cowd.2026-01-02"), vec![b'b'; 80]).expect("write log b");
        fs::write(root.join("cowd.2026-01-03"), vec![b'c'; 80]).expect("write log c");
        fs::write(root.join("other.log"), vec![b'x'; 500]).expect("write unrelated log");

        let summary = cleanup_cowd_logs(
            &root,
            &LogRetentionConfig {
                retention_days: 0,
                max_total_bytes: 120,
            },
        )
        .expect("cleanup should run");

        assert!(summary.removed_files >= 1);
        assert!(summary.retained_bytes <= 120);
        assert!(root.join("other.log").exists());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn init_logging_is_idempotent_after_subscriber_is_set() {
        let first = init_logging("test");
        let second = init_logging("test");

        assert!(
            !(first && second),
            "global tracing subscriber must not be installed twice"
        );
    }
}
