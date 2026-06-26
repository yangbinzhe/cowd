use std::fs;
use std::path::{Path, PathBuf};

const GITIGNORE_COMMENT: &str = "# Cowd local artifacts";
const GITIGNORE_ENTRIES: [&str; 1] = [".cowd/config.local.yaml"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InitStatus {
    Created,
    Updated,
    Skipped,
}

impl InitStatus {
    #[must_use]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Skipped => "skipped (already exists)",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InitArtifact {
    pub(crate) name: &'static str,
    pub(crate) status: InitStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InitReport {
    pub(crate) project_root: PathBuf,
    pub(crate) artifacts: Vec<InitArtifact>,
}

impl InitReport {
    #[must_use]
    pub(crate) fn render(&self) -> String {
        let mut lines = vec![
            "Init".to_string(),
            format!("  Project          {}", self.project_root.display()),
        ];
        for artifact in &self.artifacts {
            lines.push(format!(
                "  {:<16} {}",
                artifact.name,
                artifact.status.label()
            ));
        }
        lines.push("  Next step        Review and tailor the generated guidance".to_string());
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
struct RepoDetection {
    rust_workspace: bool,
    rust_root: bool,
    python: bool,
    package_json: bool,
    typescript: bool,
    nextjs: bool,
    react: bool,
    vite: bool,
    nest: bool,
    src_dir: bool,
    tests_dir: bool,
    rust_dir: bool,
}

pub(crate) fn initialize_repo(cwd: &Path) -> Result<InitReport, Box<dyn std::error::Error>> {
    let mut artifacts = Vec::new();

    // User-level initialization (~/.cowd/): create all user-level directories
    {
        runtime::cowd_dirs::ensure_user_dirs()?;
        artifacts.push(InitArtifact {
            name: "~/.cowd/ (user-level)",
            status: InitStatus::Created,
        });
        // User-level CLAUDE.md (global default instructions)
        let user_claude_md = runtime::cowd_dirs::config_home_dir().join("CLAUDE.md");
        if !user_claude_md.exists() {
            let content = render_init_user_claude_md();
            fs::write(&user_claude_md, &content)?;
            artifacts.push(InitArtifact {
                name: "~/.cowd/CLAUDE.md",
                status: InitStatus::Created,
            });
        }
    }

    // Migration: if .claude directory exists but .cowd does not, copy contents
    let cowd_dir = cwd.join(".cowd");
    let claude_dir = cwd.join(".claude");
    if claude_dir.is_dir() && !cowd_dir.exists() {
        migrate_claude_to_cowd(&claude_dir, &cowd_dir)?;
        artifacts.push(InitArtifact {
            name: ".cowd/ (migrated from .claude)",
            status: InitStatus::Created,
        });
    } else {
        artifacts.push(InitArtifact {
            name: ".cowd/",
            status: ensure_dir(&cowd_dir)?,
        });
    }

    // .claude.json → config.local.yaml migration
    let claude_json = cwd.join(".claude.json");
    let project_local = cwd.join(".cowd").join("config.local.yaml");
    if claude_json.is_file() && !project_local.exists() {
        // Copy permissions.defaultMode from legacy .claude.json to config.local.yaml
        if let Ok(content) = fs::read_to_string(&claude_json) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(mode) = val
                    .get("permissions")
                    .and_then(|p| p.get("defaultMode"))
                    .and_then(|m| m.as_str())
                {
                    fs::write(
                        &project_local,
                        format!("permissions:\n  defaultMode: {mode}\n"),
                    )?;
                    artifacts.push(InitArtifact {
                        name: ".cowd/config.local.yaml (migrated from .claude.json)",
                        status: InitStatus::Created,
                    });
                }
            }
        }
    }

    let gitignore = cwd.join(".gitignore");
    artifacts.push(InitArtifact {
        name: ".gitignore",
        status: ensure_gitignore_entries(&gitignore)?,
    });

    // Project-level CLAUDE.md (instructions)
    let claude_md_path = cwd.join("CLAUDE.md");
    let legacy_cowd_md = cwd.join("COWD.md");
    // Migration: if legacy COWD.md exists, rename to CLAUDE.md
    if legacy_cowd_md.is_file() && !claude_md_path.exists() {
        fs::rename(&legacy_cowd_md, &claude_md_path)?;
        artifacts.push(InitArtifact {
            name: "CLAUDE.md (migrated from COWD.md)",
            status: InitStatus::Created,
        });
    } else if claude_md_path.is_file() {
        artifacts.push(InitArtifact {
            name: "CLAUDE.md",
            status: InitStatus::Skipped,
        });
    } else {
        let content = render_init_claude_md(cwd);
        fs::write(&claude_md_path, &content)?;
        artifacts.push(InitArtifact {
            name: "CLAUDE.md",
            status: InitStatus::Created,
        });
    }

    Ok(InitReport {
        project_root: cwd.to_path_buf(),
        artifacts,
    })
}

/// Recursively copy the `.claude` directory to `.cowd`, preserving structure.
fn migrate_claude_to_cowd(claude_dir: &Path, cowd_dir: &Path) -> Result<(), std::io::Error> {
    copy_dir_recursive(claude_dir, cowd_dir)
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), std::io::Error> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn ensure_dir(path: &Path) -> Result<InitStatus, std::io::Error> {
    if path.is_dir() {
        return Ok(InitStatus::Skipped);
    }
    fs::create_dir_all(path)?;
    Ok(InitStatus::Created)
}

fn write_file_if_missing(path: &Path, content: &str) -> Result<InitStatus, std::io::Error> {
    if path.exists() {
        return Ok(InitStatus::Skipped);
    }
    fs::write(path, content)?;
    Ok(InitStatus::Created)
}

fn ensure_gitignore_entries(path: &Path) -> Result<InitStatus, std::io::Error> {
    if !path.exists() {
        let mut lines = vec![GITIGNORE_COMMENT.to_string()];
        lines.extend(GITIGNORE_ENTRIES.iter().map(|entry| (*entry).to_string()));
        fs::write(path, format!("{}\n", lines.join("\n")))?;
        return Ok(InitStatus::Created);
    }

    let existing = fs::read_to_string(path)?;
    let mut lines = existing.lines().map(ToOwned::to_owned).collect::<Vec<_>>();
    let mut changed = false;

    if !lines.iter().any(|line| line == GITIGNORE_COMMENT) {
        lines.push(GITIGNORE_COMMENT.to_string());
        changed = true;
    }

    for entry in GITIGNORE_ENTRIES {
        if !lines.iter().any(|line| line == entry) {
            lines.push(entry.to_string());
            changed = true;
        }
    }

    if !changed {
        return Ok(InitStatus::Skipped);
    }

    fs::write(path, format!("{}\n", lines.join("\n")))?;
    Ok(InitStatus::Updated)
}

pub(crate) fn render_init_user_claude_md() -> String {
    format!(
        r#"# User-level Cowd Instructions

This file lives in ~/.cowd/CLAUDE.md and serves as the default instruction
file for all Cowd sessions.  It is loaded before any project-level
CLAUDE.md, so project-specific overrides can build on top of these
instructions.

## What to put here

- Your personal coding style preferences
- Default workflows and conventions
- Global tools or MCP server preferences
- Environment-specific settings (OS, editor, shell)

Project-level CLAUDE.md (in the project root) inherits from this file and can
override any settings here.
"#,
    )
}

pub(crate) fn render_init_claude_md(cwd: &Path) -> String {
    let detection = detect_repo(cwd);
    let mut lines = vec![
        "# CLAUDE.md".to_string(),
        String::new(),
        "This file provides guidance to Cowd (cowd.dev) when working with code in this repository."
            .to_string(),
        String::new(),
    ];

    let detected_languages = detected_languages(&detection);
    let detected_frameworks = detected_frameworks(&detection);
    lines.push("## Detected stack".to_string());
    if detected_languages.is_empty() {
        lines.push("- No specific language markers were detected yet; document the primary language and verification commands once the project structure settles.".to_string());
    } else {
        lines.push(format!("- Languages: {}.", detected_languages.join(", ")));
    }
    if detected_frameworks.is_empty() {
        lines.push("- Frameworks: none detected from the supported starter markers.".to_string());
    } else {
        lines.push(format!(
            "- Frameworks/tooling markers: {}.",
            detected_frameworks.join(", ")
        ));
    }
    lines.push(String::new());

    let verification_lines = verification_lines(cwd, &detection);
    if !verification_lines.is_empty() {
        lines.push("## Verification".to_string());
        lines.extend(verification_lines);
        lines.push(String::new());
    }

    let structure_lines = repository_shape_lines(&detection);
    if !structure_lines.is_empty() {
        lines.push("## Repository shape".to_string());
        lines.extend(structure_lines);
        lines.push(String::new());
    }

    let framework_lines = framework_notes(&detection);
    if !framework_lines.is_empty() {
        lines.push("## Framework notes".to_string());
        lines.extend(framework_lines);
        lines.push(String::new());
    }

    lines.push("## Working agreement".to_string());
    lines.push("- Prefer small, reviewable changes and keep generated bootstrap files aligned with actual repo workflows.".to_string());
    lines.push("- Use `config.yaml` for shared project config; reserve `config.local.yaml` for machine-local overrides.".to_string());
    lines.push("- Do not overwrite existing `CLAUDE.md` content automatically; update it intentionally when repo workflows change.".to_string());
    lines.push(String::new());

    lines.join("\n")
}

fn detect_repo(cwd: &Path) -> RepoDetection {
    let package_json_contents = fs::read_to_string(cwd.join("package.json"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    RepoDetection {
        rust_workspace: cwd.join("rust").join("Cargo.toml").is_file(),
        rust_root: cwd.join("Cargo.toml").is_file(),
        python: cwd.join("pyproject.toml").is_file()
            || cwd.join("requirements.txt").is_file()
            || cwd.join("setup.py").is_file(),
        package_json: cwd.join("package.json").is_file(),
        typescript: cwd.join("tsconfig.json").is_file()
            || package_json_contents.contains("typescript"),
        nextjs: package_json_contents.contains("\"next\""),
        react: package_json_contents.contains("\"react\""),
        vite: package_json_contents.contains("\"vite\""),
        nest: package_json_contents.contains("@nestjs"),
        src_dir: cwd.join("src").is_dir(),
        tests_dir: cwd.join("tests").is_dir(),
        rust_dir: cwd.join("rust").is_dir(),
    }
}

fn detected_languages(detection: &RepoDetection) -> Vec<&'static str> {
    let mut languages = Vec::new();
    if detection.rust_workspace || detection.rust_root {
        languages.push("Rust");
    }
    if detection.python {
        languages.push("Python");
    }
    if detection.typescript {
        languages.push("TypeScript");
    } else if detection.package_json {
        languages.push("JavaScript/Node.js");
    }
    languages
}

fn detected_frameworks(detection: &RepoDetection) -> Vec<&'static str> {
    let mut frameworks = Vec::new();
    if detection.nextjs {
        frameworks.push("Next.js");
    }
    if detection.react {
        frameworks.push("React");
    }
    if detection.vite {
        frameworks.push("Vite");
    }
    if detection.nest {
        frameworks.push("NestJS");
    }
    frameworks
}

fn verification_lines(cwd: &Path, detection: &RepoDetection) -> Vec<String> {
    let mut lines = Vec::new();
    if detection.rust_workspace {
        lines.push("- Run Rust verification from `rust/`: `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`".to_string());
    } else if detection.rust_root {
        lines.push("- Run Rust verification from the repo root: `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`".to_string());
    }
    if detection.python {
        if cwd.join("pyproject.toml").is_file() {
            lines.push("- Run the Python project checks declared in `pyproject.toml` (for example: `pytest`, `ruff check`, and `mypy` when configured).".to_string());
        } else {
            lines.push(
                "- Run the repo's Python test/lint commands before shipping changes.".to_string(),
            );
        }
    }
    if detection.package_json {
        lines.push("- Run the JavaScript/TypeScript checks declared by this repository before shipping changes.".to_string());
    }
    if detection.tests_dir && detection.src_dir {
        lines.push("- `src/` and `tests/` are both present; update both surfaces together when behavior changes.".to_string());
    }
    lines
}

fn repository_shape_lines(detection: &RepoDetection) -> Vec<String> {
    let mut lines = Vec::new();
    if detection.rust_dir {
        lines.push(
            "- `rust/` contains the Rust workspace and active CLI/runtime implementation."
                .to_string(),
        );
    }
    if detection.src_dir {
        lines.push("- `src/` contains source files that should stay consistent with generated guidance and tests.".to_string());
    }
    if detection.tests_dir {
        lines.push("- `tests/` contains validation surfaces that should be reviewed alongside code changes.".to_string());
    }
    lines
}

fn framework_notes(detection: &RepoDetection) -> Vec<String> {
    let mut lines = Vec::new();
    if detection.nextjs {
        lines.push("- Next.js detected: preserve routing/data-fetching conventions and verify production builds after changing app structure.".to_string());
    }
    if detection.react && !detection.nextjs {
        lines.push("- React detected: keep component behavior covered with focused tests and avoid unnecessary prop/API churn.".to_string());
    }
    if detection.vite {
        lines.push("- Vite detected: validate the production bundle after changing build-sensitive configuration or imports.".to_string());
    }
    if detection.nest {
        lines.push("- NestJS detected: keep module/provider boundaries explicit and verify controller/service wiring after refactors.".to_string());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::{initialize_repo, render_init_claude_md};
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("rusty-claude-init-{nanos}"))
    }

    #[test]
    fn initialize_repo_creates_expected_files_and_gitignore_entries() {
        let root = temp_dir();
        fs::create_dir_all(root.join("rust")).expect("create rust dir");
        fs::write(root.join("rust").join("Cargo.toml"), "[workspace]\n").expect("write cargo");

        let report = initialize_repo(&root).expect("init should succeed");
        let rendered = report.render();
        assert!(rendered.contains(".cowd/"));
        assert!(rendered.contains("CLAUDE.md"));
        assert!(rendered.contains("created"));
        assert!(rendered.contains(".gitignore       created"));
        assert!(rendered.contains("CLAUDE.md"));
        assert!(root.join(".cowd").is_dir());
        assert!(root.join("CLAUDE.md").is_file());
        let gitignore = fs::read_to_string(root.join(".gitignore")).expect("read gitignore");
        assert!(gitignore.contains(".cowd/config.local.yaml"));
        let claude_md = fs::read_to_string(root.join("CLAUDE.md")).expect("read claude md");
        assert!(claude_md.contains("Languages: Rust."));
        assert!(claude_md.contains("cargo clippy --workspace --all-targets -- -D warnings"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn initialize_repo_is_idempotent_and_preserves_existing_files() {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("CLAUDE.md"), "custom guidance\n").expect("write existing claude md");
        fs::write(root.join(".gitignore"), ".cowd/config.local.yaml\n").expect("write gitignore");

        let first = initialize_repo(&root).expect("first init should succeed");
        assert!(
            first.render().contains("CLAUDE.md")
                && first.render().contains("skipped (already exists)")
        );
        let second = initialize_repo(&root).expect("second init should succeed");
        let second_rendered = second.render();
        assert!(second_rendered.contains(".cowd/"));
        assert!(second_rendered.contains("CLAUDE.md"));
        assert!(second_rendered.contains("skipped (already exists)"));
        assert!(second_rendered.contains(".gitignore       skipped (already exists)"));
        assert!(second_rendered.contains("CLAUDE.md"));
        assert_eq!(
            fs::read_to_string(root.join("CLAUDE.md")).expect("read existing claude md"),
            "custom guidance\n"
        );
        let gitignore = fs::read_to_string(root.join(".gitignore")).expect("read gitignore");
        assert_eq!(gitignore.matches(".cowd/config.local.yaml").count(), 1);

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }

    #[test]
    fn render_init_template_mentions_detected_python_and_nextjs_markers() {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("create root");
        fs::write(root.join("pyproject.toml"), "[project]\nname = \"demo\"\n")
            .expect("write pyproject");
        fs::write(
            root.join("package.json"),
            r#"{"dependencies":{"next":"14.0.0","react":"18.0.0"},"devDependencies":{"typescript":"5.0.0"}}"#,
        )
        .expect("write package json");

        let rendered = render_init_claude_md(Path::new(&root));
        assert!(rendered.contains("Languages: Python, TypeScript."));
        assert!(rendered.contains("Frameworks/tooling markers: Next.js, React."));
        assert!(rendered.contains("pyproject.toml"));
        assert!(rendered.contains("Next.js detected"));

        fs::remove_dir_all(root).expect("cleanup temp dir");
    }
}
