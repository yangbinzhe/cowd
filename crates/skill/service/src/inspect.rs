//! Skill package inspection and internal capability profile generation.
//!
//! External skills stay open-ended. This module derives Cowd's internal
//! profile from package contents instead of requiring authors to maintain a
//! complex permission manifest.

use std::collections::BTreeSet;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillDetectedRuntime, SkillEntrypoint,
    SkillInspectionReport, SkillKind, SkillLifecycleStatus, SkillRiskLevel, SkillRiskSignal,
};

pub fn inspect_skill_package(root: &Path) -> std::io::Result<SkillInspectionReport> {
    let source_root = package_root(root);
    let mut files = Vec::new();
    collect_files(&source_root, &source_root, &mut files, 0)?;
    files.sort();

    let mut runtimes = BTreeSet::new();
    let mut adapters = BTreeSet::new();
    let mut entrypoints = Vec::new();
    let mut risk_signals = Vec::new();
    let mut blocked_reasons = Vec::new();

    for file in &files {
        detect_file(
            file,
            &mut runtimes,
            &mut adapters,
            &mut entrypoints,
            &mut risk_signals,
        );
    }

    if entrypoints.is_empty()
        && files
            .iter()
            .any(|file| file.eq_ignore_ascii_case("README.md"))
    {
        runtimes.insert(SkillDetectedRuntime::Markdown);
        adapters.insert(SkillAdapterKind::PromptOnly);
        entrypoints.push(SkillEntrypoint {
            runtime: SkillDetectedRuntime::Markdown,
            path: "README.md".to_string(),
            adapter: SkillAdapterKind::PromptOnly,
            command_hint: None,
        });
    }

    if files.iter().any(|file| file == "Dockerfile") {
        risk_signals.push(SkillRiskSignal {
            level: SkillRiskLevel::High,
            kind: "container_build".to_string(),
            evidence: "Dockerfile".to_string(),
        });
    }

    if files
        .iter()
        .any(|file| file == "node_modules" || file.starts_with("node_modules/"))
    {
        blocked_reasons.push("node_modules is not inspected as package source".to_string());
    }

    Ok(SkillInspectionReport {
        source_root: source_root.display().to_string(),
        detected_files: files,
        detected_runtimes: runtimes.into_iter().collect(),
        entrypoints,
        risk_signals,
        recommended_adapters: adapters.into_iter().collect(),
        blocked_reasons,
    })
}

pub fn profile_skill_package(
    root: &Path,
    name: &str,
    version: Option<String>,
) -> std::io::Result<SkillCapabilityProfile> {
    let source_root = package_root(root);
    let inspection = inspect_skill_package(&source_root)?;
    let risk_level = inspection
        .risk_signals
        .iter()
        .map(|signal| signal.level)
        .max()
        .unwrap_or(SkillRiskLevel::Low);
    let kind = infer_kind(&inspection);
    let lifecycle_status = if inspection.blocked_reasons.is_empty() {
        if inspection
            .recommended_adapters
            .iter()
            .any(|adapter| !matches!(adapter, SkillAdapterKind::PromptOnly))
        {
            SkillLifecycleStatus::UsableRuntime
        } else {
            SkillLifecycleStatus::UsablePrompt
        }
    } else {
        SkillLifecycleStatus::Blocked
    };

    Ok(SkillCapabilityProfile {
        skill_id: stable_skill_id(name),
        name: name.to_string(),
        version,
        source_root: source_root.display().to_string(),
        package_fingerprint: package_fingerprint(&source_root, &inspection.detected_files),
        kind,
        lifecycle_status,
        adapters: inspection.recommended_adapters.clone(),
        risk_level,
        entrypoints: inspection.entrypoints.clone(),
        inspection_summary: inspection_summary(&inspection),
    })
}

fn package_root(path: &Path) -> PathBuf {
    if path.is_file() {
        path.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else {
        path.to_path_buf()
    }
}

fn collect_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<String>,
    depth: usize,
) -> std::io::Result<()> {
    if depth > 4 || files.len() >= 512 {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(".git") || name == "target" || name == "node_modules" {
            continue;
        }
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .unwrap_or(path.as_path())
            .to_string_lossy()
            .replace('\\', "/");
        if path.is_dir() {
            files.push(relative.clone());
            collect_files(root, &path, files, depth + 1)?;
        } else if path.is_file() {
            files.push(relative);
        }
    }
    Ok(())
}

fn detect_file(
    file: &str,
    runtimes: &mut BTreeSet<SkillDetectedRuntime>,
    adapters: &mut BTreeSet<SkillAdapterKind>,
    entrypoints: &mut Vec<SkillEntrypoint>,
    risk_signals: &mut Vec<SkillRiskSignal>,
) {
    let lower = file.to_ascii_lowercase();
    match lower.as_str() {
        "skill.md" | "readme.md" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Markdown,
            file,
            SkillAdapterKind::PromptOnly,
            None,
        ),
        "package.json" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Node,
            file,
            SkillAdapterKind::SandboxExec,
            Some("npm test or npm run".to_string()),
        ),
        "pyproject.toml" | "requirements.txt" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Python,
            file,
            SkillAdapterKind::SandboxExec,
            Some("python module entrypoint".to_string()),
        ),
        "go.mod" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Go,
            file,
            SkillAdapterKind::SandboxExec,
            Some("go test or go run".to_string()),
        ),
        "cargo.toml" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Rust,
            file,
            SkillAdapterKind::SandboxExec,
            Some("cargo test or cargo run".to_string()),
        ),
        "index.html" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Browser,
            file,
            SkillAdapterKind::BrowserStatic,
            None,
        ),
        "mcp.json" => add_entrypoint(
            runtimes,
            adapters,
            entrypoints,
            SkillDetectedRuntime::Mcp,
            file,
            SkillAdapterKind::McpServer,
            None,
        ),
        "dockerfile" => {
            runtimes.insert(SkillDetectedRuntime::Docker);
            adapters.insert(SkillAdapterKind::SidecarService);
        }
        _ => {
            if lower.ends_with(".py") {
                add_entrypoint(
                    runtimes,
                    adapters,
                    entrypoints,
                    SkillDetectedRuntime::Python,
                    file,
                    SkillAdapterKind::SandboxExec,
                    Some(format!("python {file}")),
                );
            } else if lower.ends_with(".js") || lower.ends_with(".ts") {
                add_entrypoint(
                    runtimes,
                    adapters,
                    entrypoints,
                    SkillDetectedRuntime::Node,
                    file,
                    SkillAdapterKind::SandboxExec,
                    Some(format!("node {file}")),
                );
            } else if lower.ends_with(".sh") {
                add_entrypoint(
                    runtimes,
                    adapters,
                    entrypoints,
                    SkillDetectedRuntime::Shell,
                    file,
                    SkillAdapterKind::SandboxExec,
                    Some(format!("sh {file}")),
                );
            } else if lower.ends_with(".ipynb") {
                add_entrypoint(
                    runtimes,
                    adapters,
                    entrypoints,
                    SkillDetectedRuntime::Notebook,
                    file,
                    SkillAdapterKind::SandboxExec,
                    None,
                );
            }
        }
    }

    if lower.contains("secret") || lower.ends_with(".env") {
        risk_signals.push(SkillRiskSignal {
            level: SkillRiskLevel::High,
            kind: "secret_signal".to_string(),
            evidence: file.to_string(),
        });
    }
    if lower.contains("server") || lower.contains("sidecar") {
        risk_signals.push(SkillRiskSignal {
            level: SkillRiskLevel::Medium,
            kind: "long_running_signal".to_string(),
            evidence: file.to_string(),
        });
    }
}

fn add_entrypoint(
    runtimes: &mut BTreeSet<SkillDetectedRuntime>,
    adapters: &mut BTreeSet<SkillAdapterKind>,
    entrypoints: &mut Vec<SkillEntrypoint>,
    runtime: SkillDetectedRuntime,
    path: &str,
    adapter: SkillAdapterKind,
    command_hint: Option<String>,
) {
    runtimes.insert(runtime);
    adapters.insert(adapter);
    if !entrypoints.iter().any(|entry| entry.path == path) {
        entrypoints.push(SkillEntrypoint {
            runtime,
            path: path.to_string(),
            adapter,
            command_hint,
        });
    }
}

fn infer_kind(inspection: &SkillInspectionReport) -> SkillKind {
    if inspection
        .recommended_adapters
        .contains(&SkillAdapterKind::McpServer)
    {
        SkillKind::McpServer
    } else if inspection
        .recommended_adapters
        .contains(&SkillAdapterKind::SidecarService)
    {
        SkillKind::SidecarService
    } else if inspection
        .recommended_adapters
        .contains(&SkillAdapterKind::BrowserStatic)
    {
        SkillKind::BrowserStatic
    } else if inspection
        .recommended_adapters
        .contains(&SkillAdapterKind::SandboxExec)
    {
        SkillKind::RuntimePackage
    } else if inspection
        .recommended_adapters
        .contains(&SkillAdapterKind::PromptOnly)
    {
        SkillKind::Document
    } else {
        SkillKind::Unknown
    }
}

fn package_fingerprint(root: &Path, files: &[String]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    root.display().to_string().hash(&mut hasher);
    for file in files {
        file.hash(&mut hasher);
        if let Ok(metadata) = fs::metadata(root.join(file)) {
            metadata.len().hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

fn inspection_summary(inspection: &SkillInspectionReport) -> Vec<String> {
    vec![
        format!("files={}", inspection.detected_files.len()),
        format!("runtimes={}", inspection.detected_runtimes.len()),
        format!("entrypoints={}", inspection.entrypoints.len()),
        format!("risks={}", inspection.risk_signals.len()),
    ]
}

fn stable_skill_id(name: &str) -> String {
    name.trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_inspect_detects_open_package_entrypoints() {
        let root = std::env::temp_dir().join(format!("cowd-skill-inspect-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("scripts")).expect("create root");
        fs::write(root.join("SKILL.md"), "# Demo").expect("skill");
        fs::write(root.join("pyproject.toml"), "[project]\nname='demo'").expect("pyproject");
        fs::write(root.join("index.html"), "<main>demo</main>").expect("html");
        fs::write(root.join("scripts").join("check.py"), "print('ok')").expect("py");

        let report = inspect_skill_package(&root).expect("inspect");
        assert!(report
            .recommended_adapters
            .contains(&SkillAdapterKind::PromptOnly));
        assert!(report
            .recommended_adapters
            .contains(&SkillAdapterKind::SandboxExec));
        assert!(report
            .recommended_adapters
            .contains(&SkillAdapterKind::BrowserStatic));

        let profile =
            profile_skill_package(&root, "Demo Skill", Some("1.0.0".to_string())).expect("profile");
        assert_eq!(profile.skill_id, "demo-skill");
        assert_eq!(
            profile.lifecycle_status,
            SkillLifecycleStatus::UsableRuntime
        );
        assert_eq!(profile.risk_level, SkillRiskLevel::Low);

        fs::remove_dir_all(root).expect("cleanup");
    }
}
