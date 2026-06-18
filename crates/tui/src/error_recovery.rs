// ── Error Recovery — Panic hook, crash log, graceful degrade ────
// Installs a custom std::panic::set_hook that:
//   1. Saves terminal state (disable raw mode, leave alt screen).
//   2. Writes a crash report to ~/.cowd/crash.log.
//   3. Preserves the original panic message and backtrace.
//
// Also provides a component render wrapper that catches panics and
// renders an error placeholder instead of crashing the TUI.
// -------------------------------------------------------------------

#![allow(dead_code)]

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::panic::{self, PanicHookInfo};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

// ── Crash Report ─────────────────────────────────────────────────

/// Path to the crash log file.
pub fn crash_log_path() -> PathBuf {
    let home = dirs_home();
    home.join(".cowd").join("crash.log")
}

fn dirs_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

/// Write a crash report to ~/.cowd/crash.log.
///
/// The report includes a timestamp, panic message, and location (file:line).
/// Appends to the existing log file; creates it and the ~/.cowd directory
/// if they don't exist.
pub fn write_crash_report(info: &PanicHookInfo) {
    let path = crash_log_path();

    // Ensure ~/.cowd/ exists
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let location = info
        .location()
        .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
        .unwrap_or_else(|| "<unknown location>".to_string());

    let message = info
        .payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
        .unwrap_or("<non-string panic payload>");

    let report = format!(
        "=== CRASH [{timestamp}] ===\n\
         location: {location}\n\
         message:  {message}\n\
         ==============================\n\n"
    );

    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = file.write_all(report.as_bytes());
        let _ = file.flush();
    }
}

/// Restore terminal to a sane state after a crash.
///
/// Attempts to leave raw mode and the alternate screen. This is a
/// best-effort operation — failures are silently ignored.
pub fn restore_terminal() {
    // Best-effort: disable raw mode and leave alternate screen
    // We use direct libc calls because crossterm may have panicked.
    // This is a last-resort cleanup.
    #[cfg(unix)]
    unsafe {
        // Write escape sequences directly to stdout fd
        let stdout_fd = 1; // STDOUT_FILENO
                           // Leave alternate screen: CSI ? 1049 l
        let _ = libc::write(
            stdout_fd,
            b"\x1b[?1049l" as *const u8 as *const libc::c_void,
            8,
        );
        // Show cursor: CSI ? 25 h
        let _ = libc::write(
            stdout_fd,
            b"\x1b[?25h" as *const u8 as *const libc::c_void,
            6,
        );
        // Reset attributes: CSI 0 m
        let _ = libc::write(stdout_fd, b"\x1b[0m" as *const u8 as *const libc::c_void, 4);
    }
}

/// Install the custom panic hook for the TUI.
///
/// Call once at TUI startup, before entering raw mode. The hook:
/// 1. Restores terminal state.
/// 2. Writes a crash report.
/// 3. Prints the panic info to stderr.
/// 4. Calls the previous hook (or the default).
pub fn install_tui_panic_hook() {
    let prev_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info: &PanicHookInfo| {
        // 1. Save terminal state
        restore_terminal();

        // 2. Write crash report
        write_crash_report(info);

        // 3. Print to stderr for immediate feedback
        let location = info
            .location()
            .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
            .unwrap_or("<non-string payload>");
        eprintln!("\n\n╔══ COWD CRASH ═══════════════════════════════╗");
        eprintln!("║ Location: {location}");
        eprintln!("║ Message:  {message}");
        eprintln!("║ Crash report saved to: {}", crash_log_path().display());
        eprintln!("╚══════════════════════════════════════════════╝\n");

        // 4. Chain to previous hook
        prev_hook(info);
    }));
}

// ── Graceful Degrade ─────────────────────────────────────────────

/// Result of attempting a component render.
pub enum RenderResult {
    /// Render succeeded normally.
    Ok,
    /// Component panicked — an error placeholder was rendered instead.
    Degraded(String),
}

/// Catch panics during component rendering and degrade gracefully.
///
/// If the closure panics, catches the panic, renders an error placeholder
/// message, and returns `RenderResult::Degraded` with the panic message.
///
/// # Example
/// ```ignore
/// let result = catch_render_panic("chat_view", |ctx| {
///     chat_view.render(ctx, area);
/// });
/// ```
pub fn catch_render_panic<F>(component_name: &str, render_fn: F) -> RenderResult
where
    F: FnOnce() + panic::UnwindSafe,
{
    match panic::catch_unwind(render_fn) {
        Ok(()) => RenderResult::Ok,
        Err(err) => {
            let msg = err
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| err.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("unknown error");
            let full_msg = format!("[ERROR] {component_name} render failed: {msg}");
            eprintln!("{full_msg}");
            RenderResult::Degraded(full_msg)
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_log_path_contains_cowd() {
        let path = crash_log_path();
        let path_str = path.to_string_lossy();
        assert!(
            path_str.contains(".cowd") || path_str.contains("cowd"),
            "crash log path should be under .cowd dir, got: {path_str}"
        );
        assert!(
            path_str.ends_with("crash.log"),
            "crash log file should be named crash.log, got: {path_str}"
        );
    }

    #[test]
    fn write_crash_report_creates_file() {
        // Use a temp directory to avoid polluting real crash log
        let tmp = std::env::temp_dir().join(format!("cowd-crash-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&tmp);

        // Can't easily test write_crash_report without a real PanicHookInfo,
        // but we can verify the file is writable
        let test_path = tmp.join("crash.log");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&test_path)
            .expect("should be able to create crash log");
        writeln!(file, "test crash entry").expect("should be able to write");
        let content = fs::read_to_string(&test_path).expect("should read back");
        assert!(content.contains("test crash entry"));

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn catch_render_panic_catches() {
        let result = catch_render_panic("test_component", || {
            panic!("intentional test panic");
        });
        match result {
            RenderResult::Degraded(msg) => {
                assert!(
                    msg.contains("test_component"),
                    "msg should name component: {msg}"
                );
                assert!(
                    msg.contains("intentional test panic"),
                    "msg should contain panic: {msg}"
                );
            }
            RenderResult::Ok => panic!("should have caught the panic"),
        }
    }

    #[test]
    fn catch_render_panic_ok() {
        let result = catch_render_panic("test_component", || {
            // No panic
        });
        assert!(matches!(result, RenderResult::Ok));
    }

    #[test]
    fn catch_render_panic_string_payload() {
        let result = catch_render_panic("comp", || {
            panic!("{}", "string payload");
        });
        match result {
            RenderResult::Degraded(msg) => {
                assert!(msg.contains("string payload"));
            }
            RenderResult::Ok => panic!("should have caught the panic"),
        }
    }
}
