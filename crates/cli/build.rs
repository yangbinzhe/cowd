use std::{env, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=COWD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=COWD_GIT_DIRTY");
    println!("cargo:rerun-if-env-changed=COWD_BUILD_TARGET");
    println!("cargo:rerun-if-env-changed=COWD_RELEASE_BUILD");

    let git_sha = git_output(["rev-parse", "HEAD"])
        .filter(|value| valid_full_git_sha(value))
        .or_else(|| {
            env::var("COWD_GIT_SHA")
                .ok()
                .filter(|value| valid_full_git_sha(value))
        })
        .unwrap_or_else(|| "unknown".to_string());
    let git_dirty = git_worktree_dirty()
        .or_else(|| {
            env::var("COWD_GIT_DIRTY")
                .ok()
                .and_then(|value| parse_bool(&value))
        })
        .unwrap_or(true);
    println!("cargo:rustc-env=COWD_GIT_SHA={git_sha}");
    println!("cargo:rustc-env=COWD_GIT_DIRTY={git_dirty}");

    let release_build = env::var("COWD_RELEASE_BUILD")
        .ok()
        .and_then(|value| parse_bool(&value))
        .unwrap_or(false);
    assert!(
        !release_build || env::var("PROFILE").as_deref() == Ok("release"),
        "COWD_RELEASE_BUILD requires Cargo's release profile"
    );
    assert!(
        !release_build || (!git_dirty && valid_full_git_sha(&git_sha)),
        "release build requires a clean checkout with a full Git SHA (sha={git_sha}, dirty={git_dirty})"
    );

    let target = env::var("COWD_BUILD_TARGET")
        .ok()
        .filter(|value| valid_metadata(value))
        .or_else(|| env::var("TARGET").ok())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=COWD_BUILD_TARGET={target}");

    // Linked worktrees keep HEAD and branch refs outside the checkout. Asking Git
    // for their real locations ensures that a new commit invalidates CLI metadata.
    watch_git_path("HEAD");
    watch_git_path("index");
    watch_git_path("logs/HEAD");
    if let Some(branch) = git_output(["rev-parse", "--abbrev-ref", "HEAD"]) {
        if branch != "HEAD" {
            watch_git_path(&format!("refs/heads/{branch}"));
        }
    }
    watch_git_path("packed-refs");
}

fn valid_full_git_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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

fn git_worktree_dirty() -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .output()
        .ok()?;
    output.status.success().then_some(!output.stdout.is_empty())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Some(true),
        "0" | "false" | "no" => Some(false),
        _ => None,
    }
}

fn watch_git_path(path: &str) {
    if let Some(actual_path) = git_output(["rev-parse", "--git-path", path]) {
        println!("cargo:rerun-if-changed={actual_path}");
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_bool, valid_full_git_sha, valid_metadata};

    #[test]
    fn validates_release_metadata_without_accepting_control_characters() {
        assert!(valid_full_git_sha(&"a".repeat(40)));
        assert!(valid_full_git_sha(&"b".repeat(64)));
        assert!(!valid_full_git_sha("0123456789ab"));
        assert!(!valid_full_git_sha("unknown"));
        assert!(!valid_full_git_sha("0123456\n"));
        assert!(valid_metadata("x86_64-unknown-linux-gnu"));
        assert!(!valid_metadata("target\nforged"));
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("unknown"), None);
    }
}
