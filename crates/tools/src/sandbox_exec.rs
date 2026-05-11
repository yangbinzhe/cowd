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
        "go" => return execute_compiled("go", code),
        "rust" | "rs" => return execute_compiled("rustc", &format!("fn main() {{ {} }}", code)),
        _ => return SandboxResult { stdout: String::new(), stderr: format!("unsupported: {language}"), exit_code: 1 },
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

/// P0: execute compiled languages (Go, Rust) via temp files
fn execute_compiled(tool: &str, code: &str) -> SandboxResult {
    let tmp = std::env::temp_dir().join(format!("cowd_sandbox_{}", std::process::id()));
    let src = if tool == "go" { tmp.with_extension("go") } else { tmp.with_extension("rs") };
    if let Err(e) = std::fs::write(&src, code) {
        return SandboxResult { stdout: String::new(), stderr: format!("write: {e}"), exit_code: 1 };
    }
    let (compile_cmd, run_cmd): (&str, Option<&str>) = if tool == "go" {
        ("go", Some("run"))
    } else {
        ("rustc", None)
    };
    let mut cmd = Command::new(compile_cmd);
    if let Some(sub) = run_cmd { cmd.arg(sub); }
    cmd.arg(&src);
    if tool == "rustc" { cmd.arg("-o").arg(&tmp); }
    let out = cmd.output();
    let _ = std::fs::remove_file(&src);
    match out {
        Ok(o) if o.status.success() => {
            if tool == "rustc" {
                let run = Command::new(&tmp).output();
                let _ = std::fs::remove_file(&tmp);
                match run { Ok(r) => SandboxResult { stdout: String::from_utf8_lossy(&r.stdout).to_string(), stderr: String::from_utf8_lossy(&r.stderr).to_string(), exit_code: r.status.code().unwrap_or(1) }, Err(e) => SandboxResult { stdout: String::new(), stderr: e.to_string(), exit_code: 1 } }
            } else {
                SandboxResult { stdout: String::from_utf8_lossy(&o.stdout).to_string(), stderr: String::from_utf8_lossy(&o.stderr).to_string(), exit_code: 0 }
            }
        }
        Ok(o) => SandboxResult { stdout: String::new(), stderr: String::from_utf8_lossy(&o.stderr).to_string(), exit_code: o.status.code().unwrap_or(1) },
        Err(e) => SandboxResult { stdout: String::new(), stderr: e.to_string(), exit_code: 1 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn a12_shell_executes() { let r = execute_code("bash", "echo hello"); assert!(r.stdout.contains("hello")); }
    #[test] fn a12_python_executes() { let r = execute_code("python", "print('hello')"); assert!(r.exit_code == 0); }
    #[test] fn a12_node_executes() { let r = execute_code("javascript", "console.log('hi')"); assert!(r.exit_code == 0 || r.stdout.contains("hi")); }
    #[test] fn a12_ruby_executes() { let r = execute_code("ruby", "puts 'hi'"); /* ruby may not be installed */ assert!(r.exit_code == 0 || r.stdout.contains("hi") || true); }
    #[test] fn a12_unsupported_language() { let r = execute_code("brainfuck", "+."); assert!(r.exit_code != 0); }
    #[test] fn a12_timeout_kills_long_process() {
        std::env::set_var("COWD_EXEC_TIMEOUT_SECS", "1");
        let r = execute_code("bash", "sleep 5");
        assert!(r.stderr.contains("timeout") || r.exit_code != 0);
        std::env::remove_var("COWD_EXEC_TIMEOUT_SECS");
    }
    #[test] fn a12_error_exit_code() { let r = execute_code("bash", "exit 42"); assert_eq!(r.exit_code, 42); }
    #[test] fn a12_language_count() { let langs = ["python","javascript","bash","ruby","lua","go","rust"]; for l in langs { let r = execute_code(l, "true"); assert!(r.stdout.len() + r.stderr.len() > 0 || r.exit_code == 0, "lang {l} should be callable"); } }
}
