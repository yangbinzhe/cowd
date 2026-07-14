#![allow(clippy::expect_used, clippy::unwrap_used)]

use sandbox_launcher::{SandboxError, SandboxLaunchSpec};

/// The common Sidecar launcher rejects any mount that would expose a Gateway
/// control-plane root.  This is exercised through the actual launcher
/// contract used by Surface supervision, not by matching implementation text.
#[test]
fn sidecar_launcher_rejects_control_plane_mounts_and_control_environment() {
    let root = tempfile::tempdir().expect("temporary surface root");
    let workspace = root.path().join("workspace");
    let control = root.path().join("control");
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::create_dir_all(&control).expect("control root");

    let mut exposed_root = SandboxLaunchSpec::workspace(&workspace);
    exposed_root.readable_roots.push(control.clone());
    exposed_root.protect_root(&control);
    assert!(matches!(
        exposed_root.validate(),
        Err(SandboxError::ProtectedRootExposed { .. })
    ));

    let mut leaked_environment = SandboxLaunchSpec::workspace(&workspace);
    leaked_environment.environment.push((
        "COWD_CONFIG_HOME".to_string(),
        control.display().to_string(),
    ));
    assert!(matches!(
        leaked_environment.validate(),
        Err(SandboxError::DisallowedEnvironment(key)) if key == "COWD_CONFIG_HOME"
    ));
}
