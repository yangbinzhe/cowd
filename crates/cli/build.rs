use std::{env, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=COWD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=COWD_BUILD_TARGET");

    let git_sha = env::var("COWD_GIT_SHA")
        .ok()
        .filter(|value| valid_git_sha(value))
        .or_else(|| git_output(["rev-parse", "--short=12", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=COWD_GIT_SHA={git_sha}");

    let target = env::var("COWD_BUILD_TARGET")
        .ok()
        .filter(|value| valid_metadata(value))
        .or_else(|| env::var("TARGET").ok())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=COWD_BUILD_TARGET={target}");

    // Linked worktrees keep HEAD and branch refs outside the checkout. Asking Git
    // for their real locations ensures that a new commit invalidates CLI metadata.
    watch_git_path("HEAD");
    watch_git_path("logs/HEAD");
    if let Some(branch) = git_output(["rev-parse", "--abbrev-ref", "HEAD"]) {
        if branch != "HEAD" {
            watch_git_path(&format!("refs/heads/{branch}"));
        }
    }
    watch_git_path("packed-refs");
}

fn valid_git_sha(value: &str) -> bool {
    (7..=40).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_metadata(value: &str) -> bool {
    !value.is_empty() && !value.contains(['\r', '\n'])
}

fn git_output<const N: usize>(args: [&str; N]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn watch_git_path(path: &str) {
    if let Some(actual_path) = git_output(["rev-parse", "--git-path", path]) {
        println!("cargo:rerun-if-changed={actual_path}");
    }
}

#[cfg(test)]
mod tests {
    use super::{valid_git_sha, valid_metadata};

    #[test]
    fn validates_release_metadata_without_accepting_control_characters() {
        assert!(valid_git_sha("0123456789ab"));
        assert!(!valid_git_sha("unknown"));
        assert!(!valid_git_sha("0123456\n"));
        assert!(valid_metadata("x86_64-unknown-linux-gnu"));
        assert!(!valid_metadata("target\nforged"));
    }
}
