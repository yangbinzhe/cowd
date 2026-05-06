use std::io::Read;
use std::process::{Command, Stdio};

pub struct SandboxResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

fn config_timeout_secs() -> u64 {
    std::env::var("COWD_EXEC_TIMEOUT_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(30)
}

fn config_max_output() -> usize {
    std::env::var("COWD_EXEC_MAX_OUTPUT_KIB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(100) * 1024
}

pub fn execute_code(language: &str, code: &str) -> SandboxResult {
    let (cmd, arg) = match language {
        "python" | "py" => ("python3", "-c"),
        "javascript" | "js" => ("node", "-e"),
        "bash" | "sh" => ("bash", "-c"),
        "ruby" | "rb" => ("ruby", "-e"),
        "lua" => ("lua", "-e"),
        _ => return SandboxResult { stdout: String::new(), stderr: format!("unsupported language: {language}"), exit_code: 1 },
    };
    let safe_code = if matches!(language, "bash" | "sh") {
        format!("set -euo pipefail; {code}")
    } else { code.to_string() };
    let mut child = match Command::new(cmd).arg(arg).arg(&safe_code).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn() {
        Ok(c) => c,
        Err(e) => return SandboxResult { stdout: String::new(), stderr: format!("spawn failed: {e}"), exit_code: 1 },
    };
    let pid = child.id();
    let timeout = config_timeout_secs();
    let max_output = config_max_output();
    let start = std::time::Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut stdout = String::new();
                let mut stderr = String::new();
                child.stdout.take().and_then(|mut p| p.read_to_string(&mut stdout).ok());
                child.stderr.take().and_then(|mut p| p.read_to_string(&mut stderr).ok());
                if stdout.len() > max_output {
                    stdout.truncate(max_output);
                    stdout.push_str(&format!("\n... [output truncated at {}KiB]", max_output / 1024));
                }
                return SandboxResult { stdout, stderr, exit_code: status.code().unwrap_or(1) };
            }
            Ok(None) => {
                if start.elapsed().as_secs() > timeout {
                    let mut buf = String::new();
                    child.stdout.take().and_then(|mut p| p.read_to_string(&mut buf).ok());
                    if buf.len() > max_output { buf.truncate(max_output); }
                    let _ = child.kill();
                    let _ = child.wait();
                    // Kill process group to clean up orphans
                    if pid > 0 { let _ = Command::new("kill").arg("-9").arg(pid.to_string()).output(); }
                    return SandboxResult { stdout: buf, stderr: format!("killed after {}s timeout", timeout), exit_code: 124 };
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => return SandboxResult { stdout: String::new(), stderr: format!("wait error: {e}"), exit_code: 1 },
        }
    }
}
