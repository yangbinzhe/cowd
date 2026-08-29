use std::env;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=COWD_GIT_SHA");
    println!("cargo:rerun-if-env-changed=COWD_GIT_DIRTY");
    println!("cargo:rerun-if-env-changed=COWD_RELEASE_BUILD");

    // Prefer the checkout itself. Environment metadata is only a source
    // archive fallback and must still carry a full object ID.
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
        // Unknown source state is never allowed to masquerade as clean.
        .unwrap_or(true);

    println!("cargo:rustc-env=GIT_SHA={git_sha}");
    println!("cargo:rustc-env=GIT_DIRTY={git_dirty}");

    // A Cargo release profile is also used by performance tests and checks.
    // Only the explicit shipping path owns the clean-tree release gate.
    let release_build = env::var("COWD_RELEASE_BUILD")
        .ok()
        .and_then(|value| parse_bool(&value))
        .unwrap_or(false);
    if release_build && env::var("PROFILE").as_deref() != Ok("release") {
        return Err("COWD_RELEASE_BUILD requires Cargo's release profile".into());
    }
    if release_build && (git_dirty || !valid_full_git_sha(&git_sha)) {
        return Err(format!(
            "release build requires a clean checkout with a full Git SHA (sha={git_sha}, dirty={git_dirty})"
        )
        .into());
    }

    // TARGET is always set by Cargo during build
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TARGET={target}");

    // Build date from SOURCE_DATE_EPOCH (reproducible builds) or current UTC date.
    // Intentionally ignoring time component to keep output deterministic within a day.
    let build_date = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|epoch| epoch.parse::<i64>().ok())
        .map(|_ts| {
            // Use SOURCE_DATE_EPOCH to derive date via chrono if available;
            // for simplicity we just use the env var as a signal and fall back
            // to build-time env. In practice CI sets this via workflow.
            std::env::var("BUILD_DATE").unwrap_or_else(|_| "unknown".to_string())
        })
        .or_else(|| std::env::var("BUILD_DATE").ok())
        .unwrap_or_else(|| {
            // Fall back to current date via `date` command
            Command::new("date")
                .args(["+%Y-%m-%d"])
                .output()
                .ok()
                .and_then(|o| {
                    if o.status.success() {
                        String::from_utf8(o.stdout).ok()
                    } else {
                        None
                    }
                })
                .map_or_else(|| "unknown".to_string(), |s| s.trim().to_string())
        });
    println!("cargo:rustc-env=BUILD_DATE={build_date}");

    // Rerun if git state changes. Worktrees keep HEAD outside the workspace
    // `.git` path, so ask git for the actual files Cargo should watch.
    watch_git_path("HEAD");
    watch_git_path("index");
    watch_git_path("logs/HEAD");
    if let Some(branch) = git_output(["rev-parse", "--abbrev-ref", "HEAD"]) {
        if branch != "HEAD" {
            watch_git_path(&format!("refs/heads/{branch}"));
        }
    }
    watch_git_path("packed-refs");

    Ok(())
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

fn valid_full_git_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
