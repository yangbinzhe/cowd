use std::io::Read;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use sandbox_launcher::{shell_command, SandboxLaunchSpec, SandboxWorkspaceAccess};

pub struct SandboxResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

fn config_timeout_secs() -> u64 {
    std::env::var("COWD_EXEC_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30)
}

fn config_max_output() -> usize {
    std::env::var("COWD_EXEC_MAX_OUTPUT_KIB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100)
        * 1024
}

#[cfg(test)]
pub fn execute_code(language: &str, code: &str) -> SandboxResult {
    execute_code_with_timeout(language, code, None)
}

#[cfg(test)]
pub fn execute_code_with_timeout(
    language: &str,
    code: &str,
    timeout_ms: Option<u64>,
) -> SandboxResult {
    let workspace = match std::env::current_dir().and_then(|path| path.canonicalize()) {
        Ok(workspace) => workspace,
        Err(error) => return failed(format!("resolve workspace: {error}")),
    };
    execute_code_in_workspace(language, code, timeout_ms, &workspace)
}

pub fn execute_code_in_workspace(
    language: &str,
    code: &str,
    timeout_ms: Option<u64>,
    workspace: &Path,
) -> SandboxResult {
    let command = match command_for(language, code) {
        Ok(command) => command,
        Err(error) => return failed(error),
    };
    let workspace = match workspace.canonicalize() {
        Ok(workspace) => workspace,
        Err(error) => return failed(format!("resolve workspace: {error}")),
    };
    let mut spec = SandboxLaunchSpec::workspace(&workspace);
    spec.working_directory = Some(workspace.clone());
    // Arbitrary code may inspect a Team's already-authorized workspace, but
    // all mutations must go through path-aware ToolHost contracts. This also
    // keeps a sandbox process compensatable without trying to infer paths
    // from source code.
    spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
    // `execute_code` is a data-analysis primitive, not an alternate network
    // client. Keep its registered no-network effect truthful at execution
    // time; callers that need network access must use governed network tools.
    spec.network_enabled = false;
    spec.require_kernel_hardening = true;
    let prepared = match shell_command(&command, &spec) {
        Ok(prepared) => prepared,
        Err(error) => return failed(format!("hardened sandbox unavailable: {error}")),
    };
    run_prepared(
        prepared.into_command(),
        timeout_ms.unwrap_or_else(|| config_timeout_secs().saturating_mul(1_000)),
        config_max_output(),
    )
}

fn run_prepared(
    mut command: std::process::Command,
    timeout_ms: u64,
    max_output: usize,
) -> SandboxResult {
    let mut child = match command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => return failed(format!("sandbox spawn failed: {error}")),
    };
    let stdout_reader = child.stdout.take().map(|mut stream| {
        std::thread::spawn(move || {
            let mut output = Vec::new();
            stream.read_to_end(&mut output).map(|_| output)
        })
    });
    let stderr_reader = child.stderr.take().map(|mut stream| {
        std::thread::spawn(move || {
            let mut output = Vec::new();
            stream.read_to_end(&mut output).map(|_| output)
        })
    });
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = collect_output(stdout_reader, "stdout");
                let mut stderr = collect_output(stderr_reader, "stderr");
                truncate_utf8(&mut stdout, max_output);
                truncate_utf8(&mut stderr, max_output);
                return SandboxResult {
                    stdout,
                    stderr,
                    exit_code: status.code().unwrap_or(1),
                };
            }
            Ok(None) if started.elapsed() >= Duration::from_millis(timeout_ms.max(1)) => {
                let _ = child.kill();
                let _ = child.wait();
                let mut stdout = collect_output(stdout_reader, "stdout");
                let mut stderr = collect_output(stderr_reader, "stderr");
                truncate_utf8(&mut stdout, max_output);
                truncate_utf8(&mut stderr, max_output);
                if !stderr.is_empty() {
                    stderr.push('\n');
                }
                stderr.push_str(&format!(
                    "sandbox execution exceeded timeout of {timeout_ms} ms"
                ));
                return SandboxResult {
                    stdout,
                    stderr,
                    exit_code: 124,
                };
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(error) => return failed(format!("sandbox wait failed: {error}")),
        }
    }
}

fn collect_output(
    reader: Option<std::thread::JoinHandle<std::io::Result<Vec<u8>>>>,
    stream: &str,
) -> String {
    match reader {
        Some(reader) => match reader.join() {
            Ok(Ok(output)) => String::from_utf8_lossy(&output).into_owned(),
            Ok(Err(error)) => format!("[{stream} read failed: {error}]"),
            Err(_) => format!("[{stream} reader panicked]"),
        },
        None => String::new(),
    }
}

fn command_for(language: &str, code: &str) -> Result<String, String> {
    let quoted = shell_quote(code);
    match language.trim().to_ascii_lowercase().as_str() {
        "python" | "py" => Ok(format!("exec python3 -c {quoted}")),
        "javascript" | "js" | "node" => Ok(format!("exec node -e {quoted}")),
        "bash" | "sh" | "shell" => Ok(format!("exec bash -lc {quoted}")),
        "ruby" | "rb" => Ok(format!("exec ruby -e {quoted}")),
        "lua" => Ok(format!("exec lua -e {quoted}")),
        "go" => Ok(format!(
            "printf %s {quoted} > /tmp/cowd-main.go && exec go run /tmp/cowd-main.go"
        )),
        "rust" | "rs" => {
            let source = format!("fn main() {{ {code} }}");
            Ok(format!(
                "printf %s {} > /tmp/cowd-main.rs && rustc /tmp/cowd-main.rs -o /tmp/cowd-main && exec /tmp/cowd-main",
                shell_quote(&source)
            ))
        }
        other => Err(format!("unsupported execution language: {other}")),
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let boundary = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    value.truncate(boundary);
    value.push_str(&format!(
        "\n... [output truncated at {}KiB]",
        max_bytes / 1024
    ));
}

fn failed(error: String) -> SandboxResult {
    SandboxResult {
        stdout: String::new(),
        stderr: error,
        exit_code: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shell_executes_in_hardened_sandbox() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("bash", "echo hello");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
        assert!(result.stdout.contains("hello"));
    }

    #[test]
    fn python_executes_in_hardened_sandbox() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("python", "print('hello')");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
    }

    #[test]
    fn unsupported_language_fails() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_ne!(execute_code("brainfuck", "+.").exit_code, 0);
    }

    #[test]
    fn timeout_stops_child() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code_with_timeout("bash", "sleep 5", Some(100));
        assert_eq!(result.exit_code, 124);
    }

    #[test]
    fn protected_host_config_is_not_visible() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("bash", "test ! -e \"$HOME/.cowd\"");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sandboxed_code_cannot_reach_host_network_namespace() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("host listener");
        let port = listener.local_addr().expect("listener address").port();
        let result = execute_code(
            "python",
            &format!(
                "import socket, sys\ns=socket.socket()\ns.settimeout(1)\ntry:\n s.connect(('127.0.0.1', {port}))\nexcept OSError:\n sys.exit(0)\nsys.exit(9)"
            ),
        );
        assert_eq!(
            result.exit_code, 0,
            "sandbox unexpectedly reached a host-network listener: {}",
            result.stderr
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sandboxed_code_cannot_mutate_the_workspace() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let workspace =
            std::env::temp_dir().join(format!("cowd-execute-code-readonly-{}", std::process::id()));
        std::fs::create_dir_all(&workspace).expect("test workspace");
        let denied = workspace.join("denied.txt");
        let result = execute_code_in_workspace(
            "python",
            "from pathlib import Path; Path('denied.txt').write_text('escaped')",
            None,
            &workspace,
        );
        assert_ne!(
            result.exit_code, 0,
            "workspace write unexpectedly succeeded"
        );
        assert!(!denied.exists());
        std::fs::remove_dir(&workspace).expect("remove empty test workspace");
    }

    #[test]
    fn large_output_does_not_deadlock_before_truncation() {
        let _guard = crate::test_process_environment_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = execute_code("python", "print('x' * 200000)");
        assert_eq!(result.exit_code, 0, "{}", result.stderr);
        assert!(result.stdout.contains("output truncated"));
    }
}
