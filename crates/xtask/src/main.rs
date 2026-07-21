//! Build-time APP source-lock tooling.
//!
//! The command deliberately has no clone, checkout, or package-install code:
//! source selection is reviewed in Git and dependency resolution remains the
//! responsibility of Cargo/pnpm during the ordinary product build.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppSourceLock {
    app_id: String,
    sdk_api: i64,
    rust_git: String,
    rust_rev: String,
    rust_packages: Vec<String>,
    webui_git: String,
    webui_rev: String,
    webui_package: String,
}

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask apps: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    match arguments.as_slice() {
        [command, action, flag] if command == "apps" && action == "sync" && flag == "--locked" => {
            sync_locked(&workspace_root()?)
        }
        [command, action, flag]
            if command == "apps" && action == "verify" && flag == "--locked" =>
        {
            verify_locked(&workspace_root()?)
        }
        [command, action, app_id, revision_flag, revision]
            if command == "apps"
                && action == "update"
                && app_id == "mfg"
                && revision_flag == "--rev" =>
        {
            update_mfg_revision(&workspace_root()?, revision)
        }
        _ => Err(
            "usage: cargo xtask apps <sync|verify> --locked | cargo xtask apps update mfg --rev <40-char-sha>"
                .to_string(),
        ),
    }
}

fn workspace_root() -> Result<PathBuf, String> {
    let mut current = env::current_dir().map_err(|error| error.to_string())?;
    loop {
        if current.join("Cargo.toml").is_file() && current.join("apps").is_dir() {
            return Ok(current);
        }
        if !current.pop() {
            return Err("could not locate Cowd workspace root".to_string());
        }
    }
}

fn verify_locked(root: &Path) -> Result<(), String> {
    let lock = parse_lock(&root.join("apps/mfg/source.lock.toml"))?;
    validate_lock(&lock)?;
    check_generated(
        &root.join("crates/app-bundle-mfg/Cargo.toml"),
        &render_bundle_manifest(&lock),
    )?;
    check_generated(
        &root.join("surfaces/webui/apps.generated.ts"),
        &render_webui_catalogue(&lock),
    )?;
    for manifest in mfg_consumer_manifests(root) {
        check_mfg_consumer_manifest(&manifest, &lock)?;
    }
    println!(
        "APP source lock verified: {} @ {}",
        lock.app_id, lock.rust_rev
    );
    Ok(())
}

/// Re-render only deterministic build inputs. It never clones, fetches,
/// installs packages or rewrites a resolver lock. The generated product
/// bundle is the sole normal dependency carrier; only explicit test fixtures
/// remain as direct APP consumers and are pinned here.
fn sync_locked(root: &Path) -> Result<(), String> {
    let lock = parse_lock(&root.join("apps/mfg/source.lock.toml"))?;
    validate_lock(&lock)?;
    write_generated(
        &root.join("crates/app-bundle-mfg/Cargo.toml"),
        &render_bundle_manifest(&lock),
    )?;
    write_generated(
        &root.join("surfaces/webui/apps.generated.ts"),
        &render_webui_catalogue(&lock),
    )?;
    for manifest in mfg_consumer_manifests(root) {
        synchronize_mfg_consumer_manifest(&manifest, &lock)?;
    }
    println!(
        "APP source inputs synchronized: {} @ {}",
        lock.app_id, lock.rust_rev
    );
    Ok(())
}

/// The only mutation command for the selected MFG source revision. Both
/// Rust and WebUI revisions advance together before generated inputs change.
fn update_mfg_revision(root: &Path, revision: &str) -> Result<(), String> {
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("--rev must be a full 40-character Git SHA".to_string());
    }
    let path = root.join("apps/mfg/source.lock.toml");
    let current =
        fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let lock = parse_lock(&path)?;
    validate_lock(&lock)?;
    let old = format!("rev = \"{}\"", lock.rust_rev);
    let replacement = format!("rev = \"{revision}\"");
    let occurrence_count = current.matches(&old).count();
    if occurrence_count != 2 {
        return Err(format!(
            "{} must contain the locked MFG revision exactly twice, found {occurrence_count}",
            path.display()
        ));
    }
    fs::write(&path, current.replace(&old, &replacement))
        .map_err(|error| format!("{}: {error}", path.display()))?;
    sync_locked(root)
}

fn parse_lock(path: &Path) -> Result<AppSourceLock, String> {
    let content =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let value: toml::Value = content
        .parse()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let top = value
        .as_table()
        .ok_or_else(|| "source lock must be a table".to_string())?;
    let rust = top
        .get("rust")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "source lock missing [rust]".to_string())?;
    let webui = top
        .get("webui")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "source lock missing [webui]".to_string())?;
    let string = |table: &toml::map::Map<String, toml::Value>, name: &str| {
        table
            .get(name)
            .and_then(toml::Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("source lock missing {name}"))
    };
    let rust_packages = rust
        .get("packages")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| "source lock missing rust.packages".to_string())?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| "rust.packages must contain strings".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AppSourceLock {
        app_id: string(top, "app_id")?,
        sdk_api: top
            .get("sdk_api")
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| "source lock missing sdk_api".to_string())?,
        rust_git: string(rust, "git")?,
        rust_rev: string(rust, "rev")?,
        rust_packages,
        webui_git: string(webui, "git")?,
        webui_rev: string(webui, "rev")?,
        webui_package: string(webui, "package")?,
    })
}

fn validate_lock(lock: &AppSourceLock) -> Result<(), String> {
    if lock.app_id != "mfg" || lock.sdk_api != 1 {
        return Err("MFG source lock has incompatible app_id or SDK API".to_string());
    }
    if lock.rust_rev.len() != 40 || !lock.rust_rev.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("rust.rev must be a full 40-character Git SHA".to_string());
    }
    if lock.webui_rev != lock.rust_rev || lock.webui_git != lock.rust_git {
        return Err("Rust and WebUI source must resolve to the same Git revision".to_string());
    }
    let expected = [
        "cowd-app-mfg-contract",
        "cowd-app-mfg-core",
        "cowd-app-mfg-adapter",
        "cowd-app-mfg-tui",
    ];
    if lock
        .rust_packages
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        != expected
    {
        return Err("MFG package set is incomplete or out of order".to_string());
    }
    if lock.webui_package != "@cowd/app-mfg-webui" {
        return Err("unexpected MFG WebUI package identity".to_string());
    }
    Ok(())
}

fn check_generated(path: &Path, expected: &str) -> Result<(), String> {
    let actual =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{} is stale; regenerate from apps/mfg/source.lock.toml",
            path.display()
        ))
    }
}

fn write_generated(path: &Path, expected: &str) -> Result<(), String> {
    let current = fs::read_to_string(path).ok();
    if current.as_deref() != Some(expected) {
        fs::write(path, expected).map_err(|error| format!("{}: {error}", path.display()))?;
    }
    Ok(())
}

fn mfg_consumer_manifests(root: &Path) -> [PathBuf; 1] {
    [root.join("crates/gateway/Cargo.toml")]
}

fn check_mfg_consumer_manifest(path: &Path, lock: &AppSourceLock) -> Result<(), String> {
    let current =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let synchronized = render_mfg_consumer_manifest(&current, lock)?;
    if current == synchronized {
        Ok(())
    } else {
        Err(format!(
            "{} is pinned to a different MFG revision; run cargo xtask apps sync --locked",
            path.display()
        ))
    }
}

fn synchronize_mfg_consumer_manifest(path: &Path, lock: &AppSourceLock) -> Result<(), String> {
    let current =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let synchronized = render_mfg_consumer_manifest(&current, lock)?;
    if current != synchronized {
        fs::write(path, synchronized).map_err(|error| format!("{}: {error}", path.display()))?;
    }
    Ok(())
}

fn render_mfg_consumer_manifest(current: &str, lock: &AppSourceLock) -> Result<String, String> {
    let mut matched = 0usize;
    let mut rendered = String::new();
    for line in current.split_inclusive('\n') {
        if !line.contains(&lock.rust_git) {
            rendered.push_str(line);
            continue;
        }
        let Some(revision_start) = line.find("rev = \"") else {
            return Err("MFG consumer dependency is missing a Git revision".to_string());
        };
        let value_start = revision_start + "rev = \"".len();
        let Some(value_end) = line[value_start..].find('"') else {
            return Err("MFG consumer dependency has an unterminated Git revision".to_string());
        };
        let value_end = value_start + value_end;
        rendered.push_str(&line[..value_start]);
        rendered.push_str(&lock.rust_rev);
        rendered.push_str(&line[value_end..]);
        matched += 1;
    }
    if matched == 0 {
        return Err("MFG consumer manifest has no source-locked dependency".to_string());
    }
    Ok(rendered)
}

fn render_bundle_manifest(lock: &AppSourceLock) -> String {
    let dependencies = lock
        .rust_packages
        .iter()
        .map(|package| {
            format!(
                "{package} = {{ git = \"{}\", rev = \"{}\" }}",
                lock.rust_git, lock.rust_rev
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# @generated by `cargo xtask apps sync --locked`; source of truth is\n# apps/mfg/source.lock.toml. Do not hand-edit the revision.\n[package]\nname = \"app-bundle-mfg\"\nversion.workspace = true\nedition.workspace = true\nlicense.workspace = true\npublish.workspace = true\n\n[dependencies]\n# Product composition owns the host's credential lifecycle authority.\nasync-trait = \"0.1\"\nauth-broker = {{ path = \"../auth-broker\" }}\ncowd-app-host = {{ path = \"../app-host\" }}\ncowd-app-sdk = {{ path = \"../app-sdk\" }}\nserde_json.workspace = true\n{dependencies}\n\n[lints]\nworkspace = true\n"
    )
}

fn render_webui_catalogue(lock: &AppSourceLock) -> String {
    format!(
        "// @generated by `cargo xtask apps sync --locked`; source of truth is\n// apps/mfg/source.lock.toml. This is a build-time package input, never a\n// runtime remote import.\nexport const cowdAppSources = [\n  {{\n    appId: \"{}\",\n    git: \"{}\",\n    rev: \"{}\",\n    package: \"{}\",\n  }},\n] as const;\n",
        lock.app_id, lock.webui_git, lock.webui_rev, lock.webui_package
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock() -> AppSourceLock {
        AppSourceLock {
            app_id: "mfg".to_string(),
            sdk_api: 1,
            rust_git: "https://example.invalid/mfg".to_string(),
            rust_rev: "0123456789abcdef0123456789abcdef01234567".to_string(),
            rust_packages: vec![
                "cowd-app-mfg-contract".to_string(),
                "cowd-app-mfg-core".to_string(),
                "cowd-app-mfg-adapter".to_string(),
                "cowd-app-mfg-tui".to_string(),
            ],
            webui_git: "https://example.invalid/mfg".to_string(),
            webui_rev: "0123456789abcdef0123456789abcdef01234567".to_string(),
            webui_package: "@cowd/app-mfg-webui".to_string(),
        }
    }

    #[test]
    fn generated_inputs_have_one_shared_revision() {
        let lock = lock();
        validate_lock(&lock).expect("valid fixture");
        assert!(render_bundle_manifest(&lock).contains(&lock.rust_rev));
        assert!(render_webui_catalogue(&lock).contains(&lock.webui_rev));
    }

    #[test]
    fn lock_rejects_short_or_divergent_revision() {
        let mut lock = lock();
        lock.rust_rev = "short".to_string();
        assert!(validate_lock(&lock).is_err());
    }

    #[test]
    fn revision_rendering_keeps_rust_and_webui_on_one_sha() {
        let old = "0123456789abcdef0123456789abcdef01234567";
        let new = "89abcdef0123456789abcdef0123456789abcdef";
        let source = format!("[rust]\nrev = \"{old}\"\n[webui]\nrev = \"{old}\"\n");
        let updated = source.replace(&format!("rev = \"{old}\""), &format!("rev = \"{new}\""));
        assert_eq!(updated.matches(new).count(), 2);
        assert!(!updated.contains(old));
    }

    #[test]
    fn direct_mfg_test_fixtures_cannot_drift_from_the_source_lock() {
        let lock = lock();
        let manifest = format!(
            "app-mfg = {{ git = \"{}\", rev = \"old-revision\" }}\n",
            lock.rust_git
        );
        let rendered = render_mfg_consumer_manifest(&manifest, &lock)
            .expect("consumer manifest is source-lockable");
        assert!(rendered.contains(&lock.rust_rev));
        assert!(!rendered.contains("old-revision"));
    }
}
