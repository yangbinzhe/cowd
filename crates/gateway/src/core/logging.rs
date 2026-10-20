use std::path::Path;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tracing_subscriber::prelude::*;

static DIAGNOSTICS: OnceLock<Arc<BoundedDiagnostics>> = OnceLock::new();

pub(crate) struct LogFlushGuard;
impl Drop for LogFlushGuard {
    fn drop(&mut self) {
        flush_logging();
    }
}

pub(crate) fn flush_logging() {
    if let Some(diagnostics) = DIAGNOSTICS.get() {
        diagnostics.flush();
    }
}

struct DiagnosticState {
    counts: [u64; 8],
    last_flush: Instant,
    writer: Box<dyn Write + Send>,
}

struct BoundedDiagnostics {
    state: Mutex<DiagnosticState>,
    json: bool,
    stderr: bool,
}

impl BoundedDiagnostics {
    fn write_record(&self, state: &mut DiagnosticState, record: &serde_json::Value) {
        let serialized = if self.json {
            serde_json::to_string(record)
        } else {
            serde_json::to_string(record).map(|json| format!("cowd_diagnostic {json}"))
        };
        if let Ok(serialized) = serialized {
            let _ = writeln!(state.writer, "{serialized}");
            if self.stderr {
                let _ = writeln!(std::io::stderr().lock(), "{serialized}");
            }
        }
    }

    fn flush_state(&self, state: &mut DiagnosticState) {
        for bucket in 0..state.counts.len() {
            let count = std::mem::take(&mut state.counts[bucket]);
            if count > 0 {
                self.write_record(
                    state,
                    &serde_json::json!({"class":"normal_aggregate","bucket":bucket,"count":count}),
                );
            }
        }
        state.last_flush = Instant::now();
        let _ = state.writer.flush();
    }

    fn flush(&self) {
        if let Ok(mut state) = self.state.lock() {
            self.flush_state(&mut state);
        }
    }
}

#[derive(Clone)]
struct DiagnosticLayer(Arc<BoundedDiagnostics>);

#[derive(Default)]
struct SafeFields {
    values: serde_json::Map<String, serde_json::Value>,
    fact: bool,
}

impl SafeFields {
    fn observe(&mut self, field: &tracing::field::Field) {
        self.fact |= field.name().contains("security")
            || field.name().contains("error")
            || field.name().contains("failure");
    }
    fn correlation(&mut self, field: &tracing::field::Field, value: &[u8]) {
        self.observe(field);
        if self.values.len() < 16
            && (field.name().ends_with("_id")
                || matches!(field.name(), "phase" | "class" | "error_class"))
        {
            self.values.insert(
                format!("{}_sha256", field.name()),
                format!("{:x}", Sha256::digest(value)).into(),
            );
        }
    }
}

impl tracing::field::Visit for SafeFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.observe(field);
        if field.name().ends_with("_id") {
            self.correlation(field, format!("{value:?}").as_bytes());
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.correlation(field, value.as_bytes());
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.observe(field);
        if self.values.len() < 16
            && matches!(
                field.name(),
                "count"
                    | "attempt"
                    | "attempts"
                    | "elapsed_ms"
                    | "duration_ms"
                    | "sequence"
                    | "revision"
                    | "bytes"
                    | "events"
                    | "status_code"
            )
        {
            self.values.insert(field.name().into(), value.into());
        }
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.observe(field);
        if self.values.len() < 16
            && matches!(
                field.name(),
                "count"
                    | "elapsed_ms"
                    | "duration_ms"
                    | "sequence"
                    | "revision"
                    | "bytes"
                    | "status_code"
            )
        {
            self.values.insert(field.name().into(), value.into());
        }
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.observe(field);
        if self.values.len() < 16 {
            self.values.insert(field.name().into(), value.into());
        }
    }
}

impl<Subscriber: tracing::Subscriber> tracing_subscriber::Layer<Subscriber> for DiagnosticLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, Subscriber>,
    ) {
        let metadata = event.metadata();
        let target = metadata.target();
        let mut fields = SafeFields::default();
        event.record(&mut fields);
        let security = ["security", "audit", "auth", "policy", "permission"]
            .iter()
            .any(|part| target.contains(part));
        let immediate = fields.fact || security || *metadata.level() <= tracing::Level::WARN;
        let Ok(mut state) = self.0.state.lock() else {
            return;
        };
        if immediate {
            let time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |duration| duration.as_millis());
            self.0.write_record(&mut state, &serde_json::json!({"class":"unsampled_fact","time_ms":time.to_string(),"level":metadata.level().as_str(),"target":target,"event":metadata.name(),"line":metadata.line(),"fields":fields.values}));
        } else {
            let bucket = [
                "graph", "artifact", "storage", "provider", "tool", "session", "gateway",
            ]
            .iter()
            .position(|part| target.contains(part))
            .unwrap_or(7);
            state.counts[bucket] = state.counts[bucket].saturating_add(1);
            if state.counts.iter().sum::<u64>() >= 100
                || state.last_flush.elapsed() >= Duration::from_secs(1)
            {
                self.0.flush_state(&mut state);
            }
        }
    }
}

pub(crate) fn init_logging(_version: &str) -> bool {
    let log_dir = runtime::cowd_dirs::config_home_dir().join("logs");
    let _ = std::fs::create_dir_all(&log_dir);
    let _ = cleanup_cowd_logs(&log_dir, &log_retention_config_from_env());
    let writer = tracing_appender::rolling::daily(&log_dir, "cowd");
    let diagnostics = Arc::new(BoundedDiagnostics {
        state: Mutex::new(DiagnosticState {
            counts: [0; 8],
            last_flush: Instant::now(),
            writer: Box::new(writer),
        }),
        json: std::env::var("COWD_LOG_FORMAT")
            .is_ok_and(|value| value.eq_ignore_ascii_case("json")),
        stderr: cfg!(debug_assertions) && std::env::var("COWD_LOG_STDERR").is_ok(),
    });
    let installed = tracing_subscriber::registry()
        .with(DiagnosticLayer(Arc::clone(&diagnostics)))
        .try_init()
        .is_ok();
    if installed {
        let _ = DIAGNOSTICS.set(diagnostics);
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
    #[derive(Clone)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn diagnostic_capture(json: bool) -> (std::sync::Arc<super::BoundedDiagnostics>, Capture) {
        let capture = Capture(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())));
        let diagnostics = std::sync::Arc::new(super::BoundedDiagnostics {
            state: std::sync::Mutex::new(super::DiagnosticState {
                counts: [0; 8],
                last_flush: std::time::Instant::now(),
                writer: Box::new(capture.clone()),
            }),
            json,
            stderr: false,
        });
        (diagnostics, capture)
    }

    #[test]
    fn r1_secret_normal_aggregation_preserves_errors_and_security_facts() {
        use tracing_subscriber::prelude::*;
        let (diagnostics, capture) = diagnostic_capture(true);
        let subscriber =
            tracing_subscriber::registry().with(super::DiagnosticLayer(diagnostics.clone()));
        tracing::subscriber::with_default(subscriber, || {
            for identity in 0..10000 {
                tracing::info!(request_id = %identity, "normal operation");
            }
            for identity in 0..7 {
                tracing::error!(request_id = %identity, error="sensitive error", "failed");
            }
            for identity in 0..13 {
                tracing::info!(target:"cowd::security", request_id = %identity, "security fact");
            }
        });
        diagnostics.flush();
        let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        let rows: Vec<serde_json::Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let aggregated: u64 = rows.iter().filter_map(|row| row["count"].as_u64()).sum();
        assert_eq!(aggregated, 10000);
        assert_eq!(
            rows.iter()
                .filter(|row| row["class"] == "unsampled_fact")
                .count(),
            20
        );
        assert!(rows.len() <= 120);
        assert_eq!(diagnostics.state.lock().unwrap().counts.len(), 8);
    }

    #[test]
    fn r1_secret_ref_paths_and_shutdown_never_log_content_or_credentials() {
        use tracing_subscriber::prelude::*;
        for json in [true, false] {
            let (diagnostics, capture) = diagnostic_capture(json);
            let subscriber =
                tracing_subscriber::registry().with(super::DiagnosticLayer(diagnostics.clone()));
            let bait = "credential-body-password-token-https://secret.invalid/?key=private";
            tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!("request", authorization = bait, content = bait);
                let _entered = span.enter();
                tracing::error!(
                    request_id = bait,
                    error = bait,
                    body = bait,
                    password = 123456789u64,
                    "{bait}"
                );
                tracing::info!(target:"cowd::security", credential=bait, "{bait}");
                tracing::info!(request_id = bait, "{bait}");
            });
            diagnostics.flush();
            let output = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
            assert!(!output.contains(bait));
            assert!(!output.contains("123456789"));
            assert!(output.contains("unsampled_fact"));
            assert!(output.contains("normal_aggregate"));
            assert!(output.contains("request_id_sha256"));
            assert!(diagnostics
                .state
                .lock()
                .unwrap()
                .counts
                .iter()
                .all(|count| *count == 0));
        }
    }
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
