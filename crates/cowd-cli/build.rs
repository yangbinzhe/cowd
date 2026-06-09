use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const WEBUI_RUNTIME_FILES: &[&str] = &[
    "index.html",
    "style.css",
    "api.js",
    "boot.js",
    "commands.js",
    "messages.js",
    "panels.js",
    "sessions.js",
    "state.js",
    "sw.js",
    "ui.js",
    "workspace.js",
    "manifest.json",
];

fn main() {
    // Get git SHA (short hash)
    let git_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()
            } else {
                None
            }
        })
        .map_or_else(|| "unknown".to_string(), |s| s.trim().to_string());

    println!("cargo:rustc-env=GIT_SHA={git_sha}");

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
    watch_git_path("logs/HEAD");
    if let Some(branch) = git_output(["rev-parse", "--abbrev-ref", "HEAD"]) {
        if branch != "HEAD" {
            watch_git_path(&format!("refs/heads/{branch}"));
        }
    }
    watch_git_path("packed-refs");

    // Copy only runtime WebUI assets. Test/build dependencies such as
    // node_modules must never enter the generated CLI artifacts.
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let webui_dir = Path::new(&manifest_dir).join("../../webui");
    let static_dir = Path::new(&manifest_dir).join("static");

    if webui_dir.exists() {
        copy_webui_runtime_assets(&webui_dir, &static_dir);

        // Also copy webui/ → target/{profile}/webui/ so the binary can find it
        // relative to its own location at runtime.
        let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
        let target_dir = env::var("CARGO_TARGET_DIR")
            .map(|p| p.to_string())
            .unwrap_or_else(|_| {
                let manifest = std::path::Path::new(&manifest_dir);
                let workspace_root = manifest
                    .parent()
                    .and_then(|p| p.parent())
                    .unwrap_or(manifest);
                workspace_root.join("target").to_string_lossy().to_string()
            });
        let target_webui = Path::new(&target_dir).join(&profile).join("webui");
        copy_webui_runtime_assets(&webui_dir, &target_webui);
    }
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

fn copy_webui_runtime_assets(src: &Path, dst: &Path) {
    if let Err(error) = fs::remove_dir_all(dst) {
        if error.kind() != std::io::ErrorKind::NotFound {
            println!(
                "cargo:warning=failed to clear generated WebUI dir {}: {error}",
                dst.display()
            );
        }
    }
    if let Err(error) = fs::create_dir_all(dst) {
        println!(
            "cargo:warning=failed to create generated WebUI dir {}: {error}",
            dst.display()
        );
        return;
    }

    for file in WEBUI_RUNTIME_FILES {
        let source = src.join(file);
        let destination = dst.join(file);
        println!("cargo:rerun-if-changed={}", source.display());
        copy_file_if_exists(&source, &destination);
    }

    let assets = src.join("assets");
    println!("cargo:rerun-if-changed={}", assets.display());
    copy_dir_recursive(&assets, &dst.join("assets"));
}

fn copy_file_if_exists(src: &Path, dst: &Path) {
    if !src.is_file() {
        return;
    }
    if let Some(parent) = dst.parent() {
        if let Err(error) = fs::create_dir_all(parent) {
            println!(
                "cargo:warning=failed to create generated WebUI parent {}: {error}",
                parent.display()
            );
            return;
        }
    }
    if let Err(error) = fs::copy(src, dst) {
        println!(
            "cargo:warning=failed to copy WebUI asset {} -> {}: {error}",
            src.display(),
            dst.display()
        );
    }
}

fn copy_dir_recursive(src: &Path, dst: &Path) {
    if !src.is_dir() {
        return;
    }
    if let Err(error) = fs::create_dir_all(dst) {
        println!(
            "cargo:warning=failed to create generated WebUI asset dir {}: {error}",
            dst.display()
        );
        return;
    }
    let entries = match fs::read_dir(src) {
        Ok(entries) => entries,
        Err(error) => {
            println!(
                "cargo:warning=failed to read WebUI asset dir {}: {error}",
                src.display()
            );
            return;
        }
    };
    for entry in entries.flatten() {
        let src_path = entry.path();
        let dst_path: PathBuf = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path);
        } else {
            copy_file_if_exists(&src_path, &dst_path);
        }
    }
}
