#![allow(clippy::expect_used, clippy::unwrap_used)]

use sandbox_launcher::{shell_command, SandboxLaunchSpec, SandboxSecurityPosture};

/// The current rootless launcher may report `Restricted` when the host lacks
/// the kernel-hardening backend.  A caller that requires that stronger
/// posture must fail closed; it must never silently receive a naked command.
#[test]
fn rootless_launcher_does_not_downgrade_a_required_kernel_hardening_request() {
    let root = tempfile::tempdir().expect("temporary sandbox workspace");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("workspace");
    let mut spec = SandboxLaunchSpec::workspace(&workspace);
    spec.require_kernel_hardening = true;

    if let Ok(prepared) = shell_command("true", &spec) {
        assert_eq!(
            prepared.security_posture(),
            SandboxSecurityPosture::KernelHardened,
            "a successful required-hardening launch must prove the requested posture"
        )
    }
}
