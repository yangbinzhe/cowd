use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::collections::BTreeMap;

const INTERNAL_DISPATCH: &str = "__cowd_internal";

fn cowd_binary() -> &'static str {
    env!("CARGO_BIN_EXE_cowd")
}

fn temp_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "cowd-single-binary-{label}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create single-binary fixture");
    root
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn wait_for_broker(client: &auth_broker::BrokerClient, child: &mut Child) {
    for _ in 0..80 {
        if client.trust_metadata().is_ok() {
            return;
        }
        assert!(
            child.try_wait().expect("inspect broker startup").is_none(),
            "Cowd internal auth broker exited before becoming ready"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("Cowd internal auth broker never became ready");
}

fn workbench_catalog() -> auth_broker::AuthorizationCatalog {
    auth_broker::AuthorizationCatalog::from_app_descriptors([cowd_app_sdk::AppDescriptor {
        id: cowd_app_sdk::AppId::parse("workbench").expect("valid generic app id"),
        display_name: "Workbench".to_string(),
        sdk_api: cowd_app_sdk::SDK_API_VERSION,
        version: "test".to_string(),
        capabilities: Vec::new(),
        routes: Vec::new(),
        actions: Vec::new(),
        profile: Some(cowd_app_sdk::AppProfileDescriptor {
            catalog_revision: 1,
            capability_digest: "sha256:workbench-test-profile".to_string(),
            default_profile_id: "viewer".to_string(),
            profiles: vec![
                cowd_app_sdk::AppProfileVariant {
                    id: "viewer".to_string(),
                    capabilities: vec!["workbench.read".to_string()],
                },
                cowd_app_sdk::AppProfileVariant {
                    id: "manager".to_string(),
                    capabilities: vec![
                        "workbench.read".to_string(),
                        "workbench.manage".to_string(),
                    ],
                },
            ],
            surface_capabilities: BTreeMap::from([
                (
                    "backend".to_string(),
                    vec!["workbench.read".to_string(), "workbench.manage".to_string()],
                ),
                ("tui".to_string(), vec!["workbench.read".to_string()]),
            ]),
        }),
    }])
    .expect("generic descriptor catalogue")
}

fn spawn_auth_broker(
    authority_root: &Path,
    socket: &Path,
    catalog: &auth_broker::AuthorizationCatalog,
    credential: &str,
) -> Child {
    fs::create_dir_all(authority_root).expect("create authority root");
    let catalog_path = auth_broker::catalog_file(authority_root);
    auth_broker::write_catalog(&catalog_path, catalog).expect("write catalogue");
    let mut child = Command::new(cowd_binary())
        .args([
            INTERNAL_DISPATCH,
            "auth-broker",
            "--root",
            authority_root.to_str().expect("utf-8 authority root"),
            "--socket",
            socket.to_str().expect("utf-8 socket"),
            "--catalog",
            catalog_path.to_str().expect("utf-8 catalogue"),
            "--credential-stdin",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn Cowd auth broker role");
    writeln!(child.stdin.take().expect("broker stdin"), "{credential}").expect("enroll broker");
    child
}

fn run_auth_profile(args: &[&str], config_home: &Path, credential: &str) -> std::process::Output {
    let mut command = Command::new(cowd_binary());
    command
        .args(args)
        .env("COWD_CONFIG_HOME", config_home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("start auth profile command");
    writeln!(child.stdin.take().expect("profile stdin"), "{credential}")
        .expect("supply profile credential");
    child
        .wait_with_output()
        .expect("wait for auth profile command")
}

fn bwrap_available() -> bool {
    ["/usr/bin/bwrap", "/bin/bwrap"]
        .into_iter()
        .any(|path| Path::new(path).is_file())
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn collect_shell_sources(root: &Path, output: &mut String) {
    for entry in fs::read_dir(root).expect("read scripts directory") {
        let path = entry.expect("script entry").path();
        if path.is_dir() {
            collect_shell_sources(&path, output);
        } else if path.extension().and_then(|value| value.to_str()) == Some("sh") {
            output.push_str(&fs::read_to_string(path).expect("read shell source"));
        }
    }
}

#[test]
fn workspace_defines_only_one_cowd_executable_artifact() {
    let root = repository_root();
    let auth_manifest =
        fs::read_to_string(root.join("crates/auth-broker/Cargo.toml")).expect("auth manifest");
    let sandbox_manifest = fs::read_to_string(root.join("crates/sandbox-launcher/Cargo.toml"))
        .expect("sandbox manifest");
    let runtime_host = fs::read_to_string(root.join("crates/gateway/src/runtime_host/mod.rs"))
        .expect("runtime host");
    let cli_main = fs::read_to_string(root.join("crates/cli/src/main.rs")).expect("cli main");
    let installer =
        fs::read_to_string(root.join("scripts/release/install-debug-to-ai.sh")).expect("installer");
    let sandbox_launcher = fs::read_to_string(root.join("crates/sandbox-launcher/src/lib.rs"))
        .expect("sandbox launcher");
    let mut shell_sources = String::new();
    collect_shell_sources(&root.join("scripts"), &mut shell_sources);

    assert!(!auth_manifest.contains("[[bin]]"));
    assert!(!sandbox_manifest.contains("[[bin]]"));
    assert!(!root.join("crates/auth-broker/src/main.rs").exists());
    assert!(!root.join("crates/sandbox-launcher/src/main.rs").exists());
    assert!(!runtime_host.contains("COWD_AUTH_BROKER_BIN"));
    assert!(!runtime_host.contains("COWD_INTERNAL_PROCESS_BIN"));
    assert!(!runtime_host.contains("cowd-auth-broker"));
    assert!(runtime_host.contains(INTERNAL_DISPATCH));
    assert!(
        cli_main
            .find("register_cowd_process_host")
            .expect("Cowd process registration")
            < cli_main
                .find("dispatch_internal_process")
                .expect("internal process dispatch"),
        "Cowd must register its single-file process identity before role dispatch"
    );
    assert!(!shell_sources.contains("COWD_AUTH_BROKER_BIN"));
    assert!(!shell_sources.contains("COWD_SANDBOX_LAUNCHER"));
    assert!(!shell_sources.contains("COWD_INTERNAL_PROCESS_BIN"));
    assert!(!installer.contains("cp \"$SANDBOX_BIN\""));
    assert!(!installer.contains("cp \"$BIN\" \"$INSTALL_DIR/cowd\""));
    assert!(installer.contains("cowd binary version mismatch"));
    assert!(installer.contains(".cowd.install.XXXXXX"));
    assert!(installer.contains("mv -f \"$INSTALL_TMP\" \"$INSTALL_DIR/cowd\""));
    assert!(installer.contains("single_binary_multi_process"));
    assert!(!root
        .join("crates/gateway/src/entry/install_entry.rs")
        .exists());
    assert!(sandbox_launcher.contains("\"/proc/{}/exe\""));
    assert!(sandbox_launcher.contains("Command::new(\"/proc/self/exe\")"));
    assert!(sandbox_launcher.contains("requires Linux executable identity pinning"));
    assert!(runtime_host.contains("cowd_internal_process_command"));
}

#[test]
fn cowd_exposes_the_versioned_sandbox_process_protocol() {
    let output = Command::new(cowd_binary())
        .args([INTERNAL_DISPATCH, "sandbox-launcher", "--protocol-version"])
        .output()
        .expect("run Cowd sandbox protocol role");

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("sandbox-launcher/{}/kernel-v1", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn cowd_runs_the_auth_broker_as_an_internal_child_role() {
    let fixture = temp_root("auth");
    let authority_root = fixture.join("authority");
    let socket = fixture.join("broker.sock");
    let mut child = spawn_auth_broker(
        &authority_root,
        &socket,
        &auth_broker::AuthorizationCatalog::from_app_descriptors(Vec::new())
            .expect("generic core catalogue"),
        "single-binary-test-credential",
    );

    let client = auth_broker::BrokerClient::new(&socket);
    wait_for_broker(&client, &mut child);

    stop_child(&mut child);
    let _ = fs::remove_dir_all(&fixture);
}

#[test]
fn cowd_auth_profile_uses_a_descriptor_catalog_through_the_live_internal_broker() {
    let fixture = temp_root("generic-auth-profile");
    let config_home = fixture.join("config");
    let authority_root = config_home.join("auth-broker");
    let socket = auth_broker::BrokerClient::default_socket(&authority_root);
    let credential = "generic-profile-test-credential";
    let mut broker = spawn_auth_broker(&authority_root, &socket, &workbench_catalog(), credential);
    let client = auth_broker::BrokerClient::new(&socket);
    wait_for_broker(&client, &mut broker);

    let initial = client
        .human_entitlements(credential)
        .expect("read initial generic profile");
    assert_eq!(initial.app_profiles["workbench"], "viewer");
    let target_profiles = BTreeMap::from([("workbench".to_string(), "manager".to_string())]);
    let (_, confirmation) = client
        .preview_human_entitlements(credential, "core_manager", target_profiles.clone())
        .expect("preview generic profile");
    let expected_epoch = initial.credential_epoch.to_string();
    let expected_revision = initial.profile_revision.to_string();
    let set = run_auth_profile(
        &[
            "auth",
            "profile",
            "set",
            "--core-profile",
            "core_manager",
            "--apps",
            "workbench=manager",
            "--expected-epoch",
            &expected_epoch,
            "--expected-revision",
            &expected_revision,
            "--confirm",
            &confirmation,
        ],
        &config_home,
        credential,
    );
    assert!(
        set.status.success(),
        "profile set failed: stdout={} stderr={}",
        String::from_utf8_lossy(&set.stdout),
        String::from_utf8_lossy(&set.stderr)
    );
    assert!(String::from_utf8_lossy(&set.stderr)
        .contains("profile preview: core=core_manager apps=workbench=manager"));
    let updated: serde_json::Value = serde_json::from_slice(&set.stdout).expect("profile JSON");
    assert_eq!(updated["app_profiles"]["workbench"], "manager");

    let show = run_auth_profile(&["auth", "profile", "show"], &config_home, credential);
    assert!(show.status.success(), "profile show failed: {show:?}");
    let shown: serde_json::Value = serde_json::from_slice(&show.stdout).expect("shown profile");
    assert_eq!(shown["app_profiles"]["workbench"], "manager");

    let stale = run_auth_profile(
        &[
            "auth",
            "profile",
            "set",
            "--core-profile",
            "core_manager",
            "--apps",
            "workbench=manager",
            "--expected-epoch",
            &expected_epoch,
            "--expected-revision",
            &expected_revision,
            "--confirm",
            &confirmation,
        ],
        &config_home,
        credential,
    );
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stderr).contains("stale profile state"));
    assert!(client
        .set_human_entitlements(
            credential,
            initial.credential_epoch,
            initial.profile_revision,
            "core_manager",
            target_profiles,
            confirmation,
        )
        .is_err());
    assert!(client
        .authenticate_human_for_surface(
            credential,
            "tui",
            vec!["workbench.manage".to_string()],
            None,
        )
        .is_err());

    stop_child(&mut broker);
    let _ = fs::remove_dir_all(&fixture);
}

#[cfg(unix)]
#[test]
fn release_installer_replaces_a_running_cowd_atomically_and_cleans_legacy_helpers() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let fixture = temp_root("atomic-install");
    let install_dir = fixture.join("install");
    let authority_root = fixture.join("authority");
    let socket = fixture.join("broker.sock");
    fs::create_dir_all(&authority_root).expect("create authority root");
    let catalog_path = auth_broker::catalog_file(&authority_root);
    auth_broker::write_catalog(
        &catalog_path,
        &auth_broker::AuthorizationCatalog::from_app_descriptors(Vec::new())
            .expect("generic core catalogue"),
    )
    .expect("write catalogue");
    fs::create_dir_all(&install_dir).expect("create install directory");
    let installed = install_dir.join("cowd");
    fs::copy(cowd_binary(), &installed).expect("seed installed Cowd");
    fs::set_permissions(&installed, fs::Permissions::from_mode(0o755))
        .expect("make installed Cowd executable");
    fs::write(install_dir.join("cowd-auth-broker"), b"legacy").expect("seed legacy helper");
    fs::write(install_dir.join(".cowd-sandbox-launcher.prev-1"), b"legacy")
        .expect("seed legacy backup");
    let original_inode = fs::metadata(&installed).expect("old metadata").ino();

    let mut child = Command::new(&installed)
        .args([
            INTERNAL_DISPATCH,
            "auth-broker",
            "--root",
            authority_root.to_str().expect("utf-8 authority root"),
            "--socket",
            socket.to_str().expect("utf-8 socket"),
            "--catalog",
            catalog_path.to_str().expect("utf-8 catalogue"),
            "--credential-stdin",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start installed Cowd process");
    child
        .stdin
        .take()
        .expect("broker stdin")
        .write_all(b"atomic-install-test\n")
        .expect("enroll broker");
    let client = auth_broker::BrokerClient::new(&socket);
    for _ in 0..80 {
        if client.trust_metadata().is_ok() {
            break;
        }
        assert!(
            child.try_wait().expect("inspect broker startup").is_none(),
            "installed Cowd process exited before replacement"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    client
        .trust_metadata()
        .expect("installed Cowd broker must become ready");

    let installer = repository_root().join("scripts/release/install-debug-to-ai.sh");
    let output = Command::new("bash")
        .arg(installer)
        .arg("--print-path-only")
        .env("COWD_BIN", cowd_binary())
        .env("COWD_INSTALL_DIR", &install_dir)
        .env("COWD_AI_ROOT", fixture.join("ai"))
        .output()
        .expect("run release installer");

    assert!(output.status.success(), "{output:?}");
    assert_ne!(
        fs::metadata(&installed).expect("new metadata").ino(),
        original_inode,
        "installation must replace the directory entry instead of truncating the running file"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(
        fs::metadata(format!("/proc/{}/exe", child.id()))
            .expect("running process image")
            .ino(),
        original_inode,
        "the running process must remain pinned to its original executable inode"
    );
    assert!(
        child.try_wait().expect("inspect old process").is_none(),
        "the process using the old inode must remain alive until an explicit restart"
    );
    assert!(!install_dir.join("cowd-auth-broker").exists());
    assert!(!install_dir.join(".cowd-sandbox-launcher.prev-1").exists());
    assert!(fs::read_dir(&install_dir)
        .expect("read install directory")
        .all(|entry| !entry
            .expect("install entry")
            .file_name()
            .to_string_lossy()
            .starts_with(".cowd.install.")));

    stop_child(&mut child);
    let _ = fs::remove_dir_all(&fixture);
}

#[cfg(unix)]
#[test]
fn release_installer_rejects_binary_version_drift_without_touching_target() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = temp_root("version-drift");
    let install_dir = fixture.join("install");
    let candidate = fixture.join("cowd-candidate");
    fs::create_dir_all(&install_dir).expect("create install directory");
    fs::write(install_dir.join("cowd"), b"existing-cowd").expect("seed existing target");
    fs::write(
        &candidate,
        b"#!/bin/sh\nprintf 'Cowd\\n  Version          999.0.0\\n'\n",
    )
    .expect("write mismatched candidate");
    fs::set_permissions(&candidate, fs::Permissions::from_mode(0o755))
        .expect("make candidate executable");

    let output = Command::new("bash")
        .arg(repository_root().join("scripts/release/install-debug-to-ai.sh"))
        .arg("--print-path-only")
        .env("COWD_BIN", &candidate)
        .env("COWD_INSTALL_DIR", &install_dir)
        .env("COWD_AI_ROOT", fixture.join("ai"))
        .output()
        .expect("run release installer");

    assert!(!output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("binary version mismatch"));
    assert_eq!(
        fs::read(install_dir.join("cowd")).expect("read existing target"),
        b"existing-cowd"
    );
    assert!(fs::read_dir(&install_dir)
        .expect("read install directory")
        .all(|entry| !entry
            .expect("install entry")
            .file_name()
            .to_string_lossy()
            .starts_with(".cowd.install.")));

    let _ = fs::remove_dir_all(&fixture);
}

#[test]
fn cowd_runs_a_kernel_hardened_command_without_a_helper_binary() {
    if !bwrap_available() {
        return;
    }
    let workspace = temp_root("sandbox");
    let output = Command::new(cowd_binary())
        .args([
            INTERNAL_DISPATCH,
            "sandbox-launcher",
            workspace.to_str().expect("utf-8 workspace"),
            "printf single-binary-sandbox",
        ])
        .output()
        .expect("run Cowd sandbox role");

    let _ = fs::remove_dir_all(&workspace);
    assert!(
        output.status.success(),
        "sandbox role failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "single-binary-sandbox"
    );
}
