//! Build-time APP source-lock tooling.
//!
//! The command deliberately has no clone, checkout, package-install, or
//! runtime loading code.  A reviewed source lock selects static APP inputs;
//! Cargo then links those inputs into the ordinary Cowd product binary.

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppSourceLock {
    source_lock_path: PathBuf,
    app_id: String,
    feature: String,
    sdk_api: i64,
    rust_git: String,
    rust_rev: String,
    rust_packages: Vec<String>,
    rust_bundle_package: String,
    rust_bundle_crate: String,
    webui_git: String,
    webui_rev: String,
    webui_package: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppCatalogEntry {
    app_id: String,
    source_lock: PathBuf,
    feature: String,
    rust_bundle_package: String,
    rust_bundle_crate: String,
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
            if command == "apps" && action == "update" && revision_flag == "--rev" =>
        {
            update_app_revision(&workspace_root()?, app_id, revision)
        }
        _ => Err(
            "usage: cargo xtask apps <sync|verify> --locked | cargo xtask apps update <app-id> --rev <40-char-sha>"
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
    let locks = app_locks(root)?;
    check_generated(
        &root.join("crates/product-apps/Cargo.toml"),
        &render_product_manifest(&locks),
    )?;
    check_generated(
        &root.join("crates/product-apps/src/generated.rs"),
        &render_product_catalogue(&locks),
    )?;
    check_generated(
        &root.join("surfaces/webui/apps.generated.ts"),
        &render_webui_catalogue(&locks),
    )?;
    for manifest in direct_app_consumer_manifests(root) {
        check_consumer_manifest(&manifest, &locks)?;
    }
    println!("APP source locks verified: {}", locked_summary(&locks));
    Ok(())
}

/// Re-render only deterministic build inputs. It never clones, fetches,
/// installs packages or rewrites a resolver lock. The generated product
/// composer is the sole normal dependency carrier; direct APP fixtures stay
/// pinned to the same reviewed source lock.
fn sync_locked(root: &Path) -> Result<(), String> {
    let locks = app_locks(root)?;
    write_generated(
        &root.join("crates/product-apps/Cargo.toml"),
        &render_product_manifest(&locks),
    )?;
    write_generated(
        &root.join("crates/product-apps/src/generated.rs"),
        &render_product_catalogue(&locks),
    )?;
    write_generated(
        &root.join("surfaces/webui/apps.generated.ts"),
        &render_webui_catalogue(&locks),
    )?;
    for manifest in direct_app_consumer_manifests(root) {
        synchronize_consumer_manifest(&manifest, &locks)?;
    }
    println!("APP source inputs synchronized: {}", locked_summary(&locks));
    Ok(())
}

fn update_app_revision(root: &Path, app_id: &str, revision: &str) -> Result<(), String> {
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("--rev must be a full 40-character Git SHA".to_string());
    }
    let lock = app_locks(root)?
        .into_iter()
        .find(|lock| lock.app_id == app_id)
        .ok_or_else(|| format!("apps/catalog.toml has no APP named {app_id}"))?;
    let path = lock.source_lock_path.clone();
    let current =
        fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    validate_lock(&lock)?;
    if lock.app_id != app_id {
        return Err(format!(
            "{} declares app_id {}; expected {app_id}",
            path.display(),
            lock.app_id
        ));
    }
    let old = format!("rev = \"{}\"", lock.rust_rev);
    let replacement = format!("rev = \"{revision}\"");
    let occurrence_count = current.matches(&old).count();
    if occurrence_count != 2 {
        return Err(format!(
            "{} must contain the locked APP revision exactly twice, found {occurrence_count}",
            path.display()
        ));
    }
    fs::write(&path, current.replace(&old, &replacement))
        .map_err(|error| format!("{}: {error}", path.display()))?;
    sync_locked(root)
}

fn app_locks(root: &Path) -> Result<Vec<AppSourceLock>, String> {
    let apps_root = root.join("apps");
    let mut locks = Vec::new();
    for entry in parse_catalog(&apps_root)? {
        let path = apps_root.join(&entry.source_lock);
        let mut lock = parse_lock(&path)?;
        lock.feature = entry.feature;
        validate_lock(&lock)?;
        if lock.app_id != entry.app_id {
            return Err(format!(
                "{} declares app_id {}; catalog declares {}",
                path.display(),
                lock.app_id,
                entry.app_id
            ));
        }
        if lock.rust_bundle_package != entry.rust_bundle_package
            || lock.rust_bundle_crate != entry.rust_bundle_crate
            || lock.webui_package != entry.webui_package
        {
            return Err(format!(
                "{} bundle or WebUI identity differs from apps/catalog.toml",
                path.display()
            ));
        }
        locks.push(lock);
    }
    locks.sort_by(|left, right| left.app_id.cmp(&right.app_id));
    if locks.is_empty() {
        return Err("apps/ contains no source.lock.toml files".to_string());
    }
    let mut app_ids = BTreeSet::new();
    for lock in &locks {
        if !app_ids.insert(lock.app_id.as_str()) {
            return Err(format!("duplicate APP source lock for {}", lock.app_id));
        }
    }
    Ok(locks)
}

fn parse_catalog(apps_root: &Path) -> Result<Vec<AppCatalogEntry>, String> {
    let path = apps_root.join("catalog.toml");
    let content =
        fs::read_to_string(&path).map_err(|error| format!("{}: {error}", path.display()))?;
    let value: toml::Value = content
        .parse()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    let top = value
        .as_table()
        .ok_or_else(|| "APP catalog must be a table".to_string())?;
    if top.get("schema").and_then(toml::Value::as_integer) != Some(1) {
        return Err("APP catalog schema must equal 1".to_string());
    }
    let apps = top
        .get("apps")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| "APP catalog must contain [[apps]] entries".to_string())?;
    let string = |table: &toml::map::Map<String, toml::Value>, name: &str| {
        table
            .get(name)
            .and_then(toml::Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("APP catalog entry missing {name}"))
    };
    let mut entries = Vec::new();
    for value in apps {
        let table = value
            .as_table()
            .ok_or_else(|| "APP catalog entries must be tables".to_string())?;
        let app_id = string(table, "id")?;
        let source_lock = PathBuf::from(string(table, "source_lock")?);
        let feature = string(table, "feature")?;
        let rust_bundle_package = string(table, "rust_bundle_package")?;
        let rust_bundle_crate = string(table, "rust_bundle_crate")?;
        let webui_package = string(table, "webui_package")?;
        let valid_source_lock = !source_lock.is_absolute()
            && source_lock.ends_with("source.lock.toml")
            && !source_lock.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::RootDir
                )
            });
        if !valid_source_lock || feature != format!("app-{app_id}") {
            return Err("APP catalog has an invalid source_lock or feature name".to_string());
        }
        entries.push(AppCatalogEntry {
            app_id,
            source_lock,
            feature,
            rust_bundle_package,
            rust_bundle_crate,
            webui_package,
        });
    }
    if entries.is_empty() {
        return Err("APP catalog has no entries".to_string());
    }
    let mut ids = BTreeSet::new();
    let mut features = BTreeSet::new();
    let mut packages = BTreeSet::new();
    let mut crates = BTreeSet::new();
    for entry in &entries {
        if !ids.insert(entry.app_id.as_str())
            || !features.insert(entry.feature.as_str())
            || !packages.insert(entry.rust_bundle_package.as_str())
            || !crates.insert(entry.rust_bundle_crate.as_str())
        {
            return Err(
                "APP catalog contains duplicate app, feature, package, or crate identity"
                    .to_string(),
            );
        }
    }
    entries.sort_by(|left, right| left.app_id.cmp(&right.app_id));
    Ok(entries)
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
        source_lock_path: path.to_path_buf(),
        app_id: string(top, "app_id")?,
        feature: String::new(),
        sdk_api: top
            .get("sdk_api")
            .and_then(toml::Value::as_integer)
            .ok_or_else(|| "source lock missing sdk_api".to_string())?,
        rust_git: string(rust, "git")?,
        rust_rev: string(rust, "rev")?,
        rust_packages,
        rust_bundle_package: string(rust, "bundle_package")?,
        rust_bundle_crate: string(rust, "bundle_crate")?,
        webui_git: string(webui, "git")?,
        webui_rev: string(webui, "rev")?,
        webui_package: string(webui, "package")?,
    })
}

fn validate_lock(lock: &AppSourceLock) -> Result<(), String> {
    let valid_app_id = !lock.app_id.is_empty()
        && lock.app_id.len() <= 63
        && lock
            .app_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid_app_id || lock.sdk_api != 1 {
        return Err("APP source lock has incompatible app_id or SDK API".to_string());
    }
    if lock.rust_rev.len() != 40 || !lock.rust_rev.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("rust.rev must be a full 40-character Git SHA".to_string());
    }
    if lock.webui_rev != lock.rust_rev || lock.webui_git != lock.rust_git {
        return Err("Rust and WebUI source must resolve to the same Git revision".to_string());
    }
    if lock.rust_packages.is_empty()
        || lock
            .rust_packages
            .iter()
            .any(|package| package.trim().is_empty())
        || !lock
            .rust_packages
            .iter()
            .any(|package| package == &lock.rust_bundle_package)
    {
        return Err("rust.packages must include the non-empty bundle_package".to_string());
    }
    let unique_packages = lock.rust_packages.iter().collect::<BTreeSet<_>>();
    if unique_packages.len() != lock.rust_packages.len() {
        return Err("rust.packages must not contain duplicates".to_string());
    }
    let valid_bundle_crate = !lock.rust_bundle_crate.is_empty()
        && lock
            .rust_bundle_crate
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if !valid_bundle_crate || lock.webui_package.trim().is_empty() {
        return Err("APP bundle crate or WebUI package identity is invalid".to_string());
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
            "{} is stale; regenerate with cargo xtask apps sync --locked",
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

fn direct_app_consumer_manifests(root: &Path) -> [PathBuf; 1] {
    [root.join("crates/gateway/Cargo.toml")]
}

fn check_consumer_manifest(path: &Path, locks: &[AppSourceLock]) -> Result<(), String> {
    let current =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let synchronized = render_consumer_manifest(&current, locks)?;
    if current == synchronized {
        Ok(())
    } else {
        Err(format!(
            "{} is pinned to a different APP revision; run cargo xtask apps sync --locked",
            path.display()
        ))
    }
}

fn synchronize_consumer_manifest(path: &Path, locks: &[AppSourceLock]) -> Result<(), String> {
    let current =
        fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let synchronized = render_consumer_manifest(&current, locks)?;
    if current != synchronized {
        fs::write(path, synchronized).map_err(|error| format!("{}: {error}", path.display()))?;
    }
    Ok(())
}

fn render_consumer_manifest(current: &str, locks: &[AppSourceLock]) -> Result<String, String> {
    let mut rendered = String::new();
    let mut matched = 0usize;
    for line in current.split_inclusive('\n') {
        let matched_lock = locks.iter().find(|lock| {
            line.contains(&lock.rust_git)
                && lock
                    .rust_packages
                    .iter()
                    .any(|package| line.contains(package))
        });
        let Some(lock) = matched_lock else {
            rendered.push_str(line);
            continue;
        };
        let Some(revision_start) = line.find("rev = \"") else {
            return Err("APP consumer dependency is missing a Git revision".to_string());
        };
        let value_start = revision_start + "rev = \"".len();
        let Some(value_end) = line[value_start..].find('"') else {
            return Err("APP consumer dependency has an unterminated Git revision".to_string());
        };
        let value_end = value_start + value_end;
        rendered.push_str(&line[..value_start]);
        rendered.push_str(&lock.rust_rev);
        rendered.push_str(&line[value_end..]);
        matched += 1;
    }
    if matched == 0 {
        return Err("APP consumer manifest has no source-locked dependency".to_string());
    }
    Ok(rendered)
}

fn render_product_manifest(locks: &[AppSourceLock]) -> String {
    let dependencies = locks
        .iter()
        .map(|lock| {
            format!(
                "{} = {{ git = \"{}\", rev = \"{}\", optional = true }}",
                lock.rust_bundle_package, lock.rust_git, lock.rust_rev
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let features = locks
        .iter()
        .map(|lock| {
            format!(
                "{} = [\"dep:{}\"]",
                app_feature(lock),
                lock.rust_bundle_package
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let defaults = locks
        .iter()
        .map(app_feature)
        .map(|feature| format!("\"{feature}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "# @generated by `cargo xtask apps sync --locked`; source of truth is apps/catalog.toml.\n# Static product inputs only: do not hand-edit dependencies or features.\n[package]\nname = \"cowd-product-apps\"\nversion.workspace = true\nedition.workspace = true\nlicense.workspace = true\npublish.workspace = true\n\n[dependencies]\ncowd-app-host = {{ path = \"../app-host\" }}\ncowd-app-sdk = {{ path = \"../app-sdk\" }}\nstorage = {{ path = \"../storage\" }}\nthiserror = \"2\"\n{dependencies}\n\n[features]\ndefault = [{defaults}]\n{features}\n\n[lints]\nworkspace = true\n"
    )
}

fn render_product_catalogue(locks: &[AppSourceLock]) -> String {
    let registrations = locks
        .iter()
        .map(|lock| {
            format!(
                "        #[cfg(feature = \"{}\")]\n        {}::product(),",
                app_feature(lock),
                lock.rust_bundle_crate
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "// @generated by `cargo xtask apps sync --locked`; source of truth is apps/catalog.toml.\n// Each entry is a compile-time linked product bundle, never a runtime import.\n\nuse cowd_app_host::StaticAppProduct;\n\n#[must_use]\npub fn compiled_products() -> Vec<StaticAppProduct> {{\n    vec![\n{registrations}\n    ]\n}}\n"
    )
}

fn render_webui_catalogue(locks: &[AppSourceLock]) -> String {
    let entries = locks
        .iter()
        .map(|lock| {
            format!(
                "  {{\n    appId: \"{}\",\n    git: \"{}\",\n    rev: \"{}\",\n    package: \"{}\",\n  }},",
                lock.app_id, lock.webui_git, lock.webui_rev, lock.webui_package
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "// @generated by `cargo xtask apps sync --locked`; source of truth is apps/catalog.toml.\n// These are build-time package inputs, never runtime remote imports.\nexport const cowdAppSources = [\n{entries}\n] as const;\n"
    )
}

fn app_feature(lock: &AppSourceLock) -> String {
    lock.feature.clone()
}

fn locked_summary(locks: &[AppSourceLock]) -> String {
    locks
        .iter()
        .map(|lock| format!("{} @ {}", lock.app_id, lock.rust_rev))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temporary_apps_root(catalogue: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowd-xtask-catalogue-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("temporary apps root");
        std::fs::write(root.join("catalog.toml"), catalogue).expect("temporary catalogue");
        root
    }

    fn catalog_entry(id: &str, package_suffix: &str) -> String {
        format!(
            "[[apps]]\nid = \"{id}\"\nsource_lock = \"{id}/source.lock.toml\"\nfeature = \"app-{id}\"\nrust_bundle_package = \"cowd-app-{package_suffix}-bundle\"\nrust_bundle_crate = \"cowd_app_{package_suffix}_bundle\"\nwebui_package = \"@cowd/app-{package_suffix}-webui\"\n"
        )
    }

    fn source_lock(id: &str, package_suffix: &str) -> String {
        let revision = "0123456789abcdef0123456789abcdef01234567";
        format!(
            "schema = 1\napp_id = \"{id}\"\nsdk_api = 1\n\n[rust]\ngit = \"https://example.invalid/{package_suffix}\"\nrev = \"{revision}\"\npackages = [\"cowd-app-{package_suffix}-bundle\"]\nbundle_package = \"cowd-app-{package_suffix}-bundle\"\nbundle_crate = \"cowd_app_{package_suffix}_bundle\"\n\n[webui]\ngit = \"https://example.invalid/{package_suffix}\"\nrev = \"{revision}\"\npackage = \"@cowd/app-{package_suffix}-webui\"\n"
        )
    }

    fn temporary_workspace(catalogue: &str, locks: &[(&str, &str)]) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowd-xtask-workspace-{}-{nonce}",
            std::process::id()
        ));
        let apps_root = root.join("apps");
        std::fs::create_dir_all(&apps_root).expect("temporary apps root");
        std::fs::write(apps_root.join("catalog.toml"), catalogue).expect("temporary catalogue");
        for (relative_path, content) in locks {
            let path = apps_root.join(relative_path);
            std::fs::create_dir_all(path.parent().expect("lock parent"))
                .expect("temporary lock directory");
            std::fs::write(path, content).expect("temporary source lock");
        }
        root
    }

    fn lock() -> AppSourceLock {
        AppSourceLock {
            source_lock_path: PathBuf::from("fixture/source.lock.toml"),
            app_id: "fixture".to_string(),
            feature: "app-fixture".to_string(),
            sdk_api: 1,
            rust_git: "https://example.invalid/fixture".to_string(),
            rust_rev: "0123456789abcdef0123456789abcdef01234567".to_string(),
            rust_packages: vec![
                "cowd-app-fixture-contract".to_string(),
                "cowd-app-fixture-bundle".to_string(),
            ],
            rust_bundle_package: "cowd-app-fixture-bundle".to_string(),
            rust_bundle_crate: "cowd_app_fixture_bundle".to_string(),
            webui_git: "https://example.invalid/fixture".to_string(),
            webui_rev: "0123456789abcdef0123456789abcdef01234567".to_string(),
            webui_package: "@cowd/app-fixture-webui".to_string(),
        }
    }

    #[test]
    fn generated_inputs_share_one_revision_and_static_bundle_feature() {
        let lock = lock();
        validate_lock(&lock).expect("valid fixture");
        let manifest = render_product_manifest(&[lock.clone()]);
        assert!(manifest.contains(&lock.rust_rev));
        assert!(manifest.contains("app-fixture"));
        let catalogue = render_product_catalogue(&[lock.clone()]);
        assert!(catalogue.contains("cowd_app_fixture_bundle::product"));
        assert!(render_webui_catalogue(&[lock.clone()]).contains(&lock.webui_rev));
    }

    #[test]
    fn lock_rejects_short_revision_or_missing_bundle() {
        let mut fixture = lock();
        fixture.rust_rev = "short".to_string();
        assert!(validate_lock(&fixture).is_err());
        let mut fixture = lock();
        fixture.rust_packages.clear();
        assert!(validate_lock(&fixture).is_err());
    }

    #[test]
    fn direct_app_test_fixtures_cannot_drift_from_the_source_lock() {
        let lock = lock();
        let manifest = format!(
            "app-fixture = {{ package = \"cowd-app-fixture-contract\", git = \"{}\", rev = \"old-revision\" }}\n",
            lock.rust_git
        );
        let rendered = render_consumer_manifest(&manifest, &[lock.clone()])
            .expect("consumer manifest is source-lockable");
        assert!(rendered.contains(&lock.rust_rev));
        assert!(!rendered.contains("old-revision"));
    }

    #[test]
    fn catalog_supports_multiple_explicit_entries_in_stable_order() {
        let catalogue = format!(
            "schema = 1\n\n{}\n{}",
            catalog_entry("zeta", "zeta"),
            catalog_entry("alpha", "alpha")
        );
        let root = temporary_apps_root(&catalogue);
        let entries = parse_catalog(&root).expect("valid multi-APP catalogue");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.app_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        std::fs::remove_dir_all(root).expect("remove temporary catalogue");
    }

    #[test]
    fn source_locks_follow_explicit_catalogue_order_not_directory_discovery() {
        let catalogue = format!(
            "schema = 1\n\n{}\n{}",
            catalog_entry("zeta", "zeta"),
            catalog_entry("alpha", "alpha")
        );
        let root = temporary_workspace(
            &catalogue,
            &[
                ("zeta/source.lock.toml", &source_lock("zeta", "zeta")),
                ("alpha/source.lock.toml", &source_lock("alpha", "alpha")),
            ],
        );
        let locks = app_locks(&root).expect("catalogued source locks");
        assert_eq!(
            locks
                .iter()
                .map(|lock| lock.app_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        assert!(locks.iter().all(|lock| lock.feature.starts_with("app-")));
        std::fs::remove_dir_all(root).expect("remove temporary workspace");
    }

    #[test]
    fn catalog_rejects_duplicate_identity_and_escape_path() {
        let duplicate = format!(
            "schema = 1\n\n{}\n{}",
            catalog_entry("fixture", "first"),
            catalog_entry("fixture", "second")
        );
        let root = temporary_apps_root(&duplicate);
        assert!(parse_catalog(&root).is_err());
        std::fs::remove_dir_all(&root).expect("remove duplicate catalogue");

        let escape = "schema = 1\n\n[[apps]]\nid = \"fixture\"\nsource_lock = \"../fixture/source.lock.toml\"\nfeature = \"app-fixture\"\nrust_bundle_package = \"cowd-app-fixture-bundle\"\nrust_bundle_crate = \"cowd_app_fixture_bundle\"\nwebui_package = \"@cowd/app-fixture-webui\"\n";
        let root = temporary_apps_root(escape);
        assert!(parse_catalog(&root).is_err());
        std::fs::remove_dir_all(root).expect("remove escape catalogue");
    }
}
