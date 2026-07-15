use std::collections::BTreeMap;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use landlock::{
    Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr, RulesetCreatedAttr, ABI,
};
use seccompiler::{BpfProgram, SeccompAction, SeccompFilter};

use sandbox_launcher::{shell_command, SandboxLaunchSpec};

const LAUNCHER_PROTOCOL: &str =
    concat!("sandbox-launcher/", env!("CARGO_PKG_VERSION"), "/kernel-v1");

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--protocol-version") {
        println!("{LAUNCHER_PROTOCOL}");
        return ExitCode::SUCCESS;
    }
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("--inner") {
        return inner_main(args.collect());
    }
    let mut args = std::env::args().skip(1);
    let Some(workspace) = args.next() else {
        eprintln!("usage: cowd-sandbox-launcher <absolute-workspace> <shell-command>");
        return ExitCode::from(64);
    };
    let command = args.collect::<Vec<_>>().join(" ");
    if command.trim().is_empty() {
        eprintln!("shell command is required");
        return ExitCode::from(64);
    }
    let spec = SandboxLaunchSpec::workspace(workspace);
    match shell_command(&command, &spec) {
        Ok(prepared) => match prepared.into_command().status() {
            Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
            Err(error) => {
                eprintln!("failed to launch sandbox: {error}");
                ExitCode::from(1)
            }
        },
        Err(error) => {
            eprintln!("sandbox unavailable: {error}");
            ExitCode::from(1)
        }
    }
}

fn inner_main(args: Vec<String>) -> ExitCode {
    let mut workspace = None;
    let mut writable = Vec::new();
    let mut command = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--workspace" if index + 1 < args.len() => {
                workspace = Some(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            "--writable" if index + 1 < args.len() => {
                writable.push(PathBuf::from(&args[index + 1]));
                index += 2;
            }
            "--" if index + 1 < args.len() => {
                command = Some(args[index + 1..].join(" "));
                break;
            }
            _ => return ExitCode::from(64),
        }
    }
    let Some(workspace) = workspace else {
        eprintln!("sandbox inner launcher requires --workspace");
        return ExitCode::from(64);
    };
    let Some(command) = command else {
        eprintln!("sandbox inner launcher requires a command after --");
        return ExitCode::from(64);
    };
    writable.push(workspace);
    writable.push(PathBuf::from("/tmp"));
    writable.push(PathBuf::from("/dev"));
    if let Err(error) = install_landlock(&writable) {
        eprintln!("failed to install Landlock: {error}");
        return ExitCode::from(125);
    }
    if let Err(error) = install_seccomp() {
        eprintln!("failed to install seccomp: {error}");
        return ExitCode::from(125);
    }
    match Command::new("/bin/sh").arg("-c").arg(command).status() {
        Ok(status) => ExitCode::from(status.code().unwrap_or(1) as u8),
        Err(error) => {
            eprintln!("failed to execute hardened sandbox command: {error}");
            ExitCode::from(125)
        }
    }
}

fn install_landlock(writable: &[PathBuf]) -> Result<(), String> {
    let abi = ABI::V5;
    let access_all = AccessFs::from_all(abi);
    let access_read = AccessFs::from_read(abi);
    let mut ruleset = Ruleset::default()
        .handle_access(access_all)
        .map_err(|error| error.to_string())?
        .create()
        .map_err(|error| error.to_string())?
        .add_rule(PathBeneath::new(
            PathFd::new(Path::new("/")).map_err(|error| error.to_string())?,
            access_read,
        ))
        .map_err(|error| error.to_string())?;
    for root in writable.iter().filter(|root| root.exists()) {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(root).map_err(|error| error.to_string())?,
                access_all,
            ))
            .map_err(|error| error.to_string())?;
    }
    let status = ruleset.restrict_self().map_err(|error| error.to_string())?;
    let rendered = format!("{status:?}");
    if !rendered.contains("FullyEnforced") || !rendered.contains("no_new_privs: true") {
        return Err(format!("Landlock was not fully enforced: {rendered}"));
    }
    Ok(())
}

fn install_seccomp() -> Result<(), String> {
    let syscalls = [
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_keyctl,
        libc::SYS_open_by_handle_at,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_bpf,
        libc::SYS_kexec_load,
    ];
    let rules = syscalls
        .into_iter()
        .map(|syscall| (syscall, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    let filter: BpfProgram = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Trap,
        std::env::consts::ARCH
            .try_into()
            .map_err(|error| format!("unsupported seccomp architecture: {error}"))?,
    )
    .map_err(|error| error.to_string())?
    .try_into()
    .map_err(|error: seccompiler::BackendError| error.to_string())?;
    seccompiler::apply_filter(&filter).map_err(|error| error.to_string())
}
