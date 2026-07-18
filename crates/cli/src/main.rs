#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

fn main() -> std::process::ExitCode {
    sandbox_launcher::register_cowd_process_host();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(status) = cli::dispatch_internal_process(&args) {
        return status;
    }
    let first_arg = args.first().map(String::as_str);

    if matches!(first_arg, Some("auth")) {
        return auth_profile_entry(&args[1..]);
    }

    if should_open_tui(&args) || matches!(first_arg, Some("tui")) {
        return open_tui();
    }

    match first_arg {
        Some("gateway") => gateway::backend_entry(),
        _ => gateway::static_entry(),
    }
    std::process::ExitCode::SUCCESS
}

fn auth_profile_entry(args: &[String]) -> std::process::ExitCode {
    let result = match args {
        [profile, command, rest @ ..] if profile == "profile" && command == "show" => {
            auth_profile_show(rest)
        }
        [profile, command, rest @ ..] if profile == "profile" && command == "set" => {
            auth_profile_set(rest)
        }
        _ => Err(
            "usage: cowd auth profile show | cowd auth profile set --core <profile> --mfg <profile> --expected-epoch <n> --expected-revision <n> --confirm <digest>"
                .to_string(),
        ),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("auth profile failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn auth_profile_show(args: &[String]) -> Result<(), String> {
    if !args.is_empty() {
        return Err("profile show does not accept positional arguments".to_string());
    }
    let credential = read_credential_stdin()?;
    let client = auth_profile_client();
    let entitlement = client
        .human_entitlements(&credential)
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&entitlement).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn auth_profile_set(args: &[String]) -> Result<(), String> {
    let flags = parse_exact_flags(
        args,
        &[
            "--core",
            "--mfg",
            "--expected-epoch",
            "--expected-revision",
            "--confirm",
        ],
    )?;
    let core = flags["--core"].clone();
    let mfg = flags["--mfg"].clone();
    let expected_epoch = flags["--expected-epoch"]
        .parse::<u64>()
        .map_err(|_| "--expected-epoch must be an integer".to_string())?;
    let expected_revision = flags["--expected-revision"]
        .parse::<u64>()
        .map_err(|_| "--expected-revision must be an integer".to_string())?;
    let supplied_confirmation = flags["--confirm"].clone();
    let core_profile_id = parse_core_profile(&core)?;
    let mfg_profile_id = parse_mfg_profile(&mfg)?;
    let credential = read_credential_stdin()?;
    let client = auth_profile_client();
    let current = client
        .human_entitlements(&credential)
        .map_err(|error| error.to_string())?;
    if current.credential_epoch != expected_epoch || current.profile_revision != expected_revision {
        return Err(format!(
            "stale profile state: current epoch/revision is {}/{}",
            current.credential_epoch, current.profile_revision
        ));
    }
    let mut target = app_mfg_contract::core_profile_capabilities(core_profile_id)
        .iter()
        .map(|capability| (*capability).to_string())
        .chain(
            app_mfg_contract::mfg_profile_capabilities(mfg_profile_id)
                .iter()
                .map(|capability| capability.as_str().to_string()),
        )
        .collect::<Vec<_>>();
    target.sort();
    target.dedup();
    let expected_confirmation = auth_broker::entitlement_confirmation_digest(
        expected_epoch,
        expected_revision,
        core_profile_id,
        mfg_profile_id,
        &target,
    );
    let added = target
        .iter()
        .filter(|capability| !current.ceiling.contains(*capability))
        .cloned()
        .collect::<Vec<_>>();
    let removed = current
        .ceiling
        .iter()
        .filter(|capability| !target.contains(*capability))
        .cloned()
        .collect::<Vec<_>>();
    eprintln!(
        "profile diff: core={core} mfg={mfg} add={} remove={} confirmation={expected_confirmation}",
        added.join(","),
        removed.join(",")
    );
    if supplied_confirmation != expected_confirmation {
        return Err(
            "confirmation digest does not match the displayed complete capability diff".to_string(),
        );
    }
    let updated = client
        .set_human_entitlements(
            &credential,
            expected_epoch,
            expected_revision,
            core_profile_id,
            mfg_profile_id,
            supplied_confirmation,
        )
        .map_err(|error| error.to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(&updated).map_err(|error| error.to_string())?
    );
    Ok(())
}

fn auth_profile_client() -> auth_broker::BrokerClient {
    let config_home = std::env::var_os("COWD_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .map(|home| home.join(".cowd"))
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".cowd"));
    auth_broker::BrokerClient::new(auth_broker::BrokerClient::default_socket(
        config_home.join("auth-broker"),
    ))
}

fn read_credential_stdin() -> Result<String, String> {
    use std::io::BufRead;
    let mut credential = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut credential)
        .map_err(|error| error.to_string())?;
    let credential = credential
        .trim_end_matches(|character| character == '\r' || character == '\n')
        .to_string();
    if credential.trim().is_empty() {
        return Err("credential must be supplied on stdin".to_string());
    }
    Ok(credential)
}

fn parse_exact_flags(
    args: &[String],
    allowed: &[&'static str],
) -> Result<std::collections::BTreeMap<&'static str, String>, String> {
    let mut parsed = std::collections::BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        let name = args[index].as_str();
        let Some(&canonical) = allowed.iter().find(|allowed| **allowed == name) else {
            return Err(format!("unknown auth profile flag {name}"));
        };
        if parsed.contains_key(canonical) {
            return Err(format!("duplicate auth profile flag {canonical}"));
        }
        let value = args
            .get(index + 1)
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| format!("missing value for {canonical}"))?;
        parsed.insert(canonical, value.clone());
        index += 2;
    }
    for required in allowed {
        if !parsed.contains_key(*required) {
            return Err(format!("missing required flag {required}"));
        }
    }
    Ok(parsed)
}

fn parse_core_profile(value: &str) -> Result<app_mfg_contract::MfgCoreProfileId, String> {
    match value {
        "core_legacy_0_9_530" => Ok(app_mfg_contract::MfgCoreProfileId::CoreLegacy09530),
        "core_manager" => Ok(app_mfg_contract::MfgCoreProfileId::CoreManager),
        _ => Err(format!("unknown core profile {value}")),
    }
}

fn parse_mfg_profile(value: &str) -> Result<app_mfg_contract::MfgProfileId, String> {
    match value {
        "mfg_viewer" => Ok(app_mfg_contract::MfgProfileId::MfgViewer),
        "mfg_legacy_0_9_529" => Ok(app_mfg_contract::MfgProfileId::MfgLegacy09529),
        "mfg_operator" => Ok(app_mfg_contract::MfgProfileId::MfgOperator),
        "mfg_reviewer" => Ok(app_mfg_contract::MfgProfileId::MfgReviewer),
        "mfg_manager" => Ok(app_mfg_contract::MfgProfileId::MfgManager),
        _ => Err(format!("unknown MFG profile {value}")),
    }
}

#[cfg(feature = "tui-surface")]
fn open_tui() -> std::process::ExitCode {
    match tui::terminal_entry() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("TUI failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(not(feature = "tui-surface"))]
fn open_tui() -> std::process::ExitCode {
    eprintln!(
        "TUI surface is not built in this binary; rebuild cowd with `--features full` or install a full build."
    );
    std::process::ExitCode::from(2)
}

#[cfg(feature = "tui-surface")]
fn should_open_tui(args: &[String]) -> bool {
    let first_arg = args.first().map(String::as_str);
    if args.iter().any(|arg| arg.trim_start().starts_with('/')) {
        return false;
    }

    match first_arg {
        None => true,
        Some(
            "--resume"
            | "--session"
            | "--session-id"
            | "-s"
            | "--model"
            | "-m"
            | "--yolo"
            | "--dangerously-skip-permissions"
            | "--danger-full-access",
        ) => true,
        Some(arg)
            if arg.starts_with("--resume=")
                || arg.starts_with("--session=")
                || arg.starts_with("--session-id=")
                || arg.starts_with("--model=") =>
        {
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_profile_flags_accept_only_closed_profile_catalogs() {
        assert_eq!(
            parse_core_profile("core_manager").unwrap(),
            app_mfg_contract::MfgCoreProfileId::CoreManager
        );
        assert_eq!(
            parse_mfg_profile("mfg_reviewer").unwrap(),
            app_mfg_contract::MfgProfileId::MfgReviewer
        );
        assert!(parse_core_profile("arbitrary").is_err());
        assert!(parse_mfg_profile("mfg_arbitrary").is_err());
        let args = vec![
            "--core".to_string(),
            "core_manager".to_string(),
            "--mfg".to_string(),
            "mfg_manager".to_string(),
        ];
        assert!(parse_exact_flags(
            &args,
            &[
                "--core",
                "--mfg",
                "--expected-epoch",
                "--expected-revision",
                "--confirm"
            ]
        )
        .is_err());
        let complete = vec![
            "--core".to_string(),
            "core_manager".to_string(),
            "--mfg".to_string(),
            "mfg_manager".to_string(),
            "--expected-epoch".to_string(),
            "1".to_string(),
            "--expected-revision".to_string(),
            "1".to_string(),
            "--confirm".to_string(),
            "sha256:confirmation".to_string(),
        ];
        assert_eq!(
            parse_exact_flags(
                &complete,
                &[
                    "--core",
                    "--mfg",
                    "--expected-epoch",
                    "--expected-revision",
                    "--confirm"
                ]
            )
            .unwrap()["--mfg"],
            "mfg_manager"
        );
        let mut duplicate = complete.clone();
        duplicate.extend(["--mfg".to_string(), "mfg_viewer".to_string()]);
        assert!(parse_exact_flags(
            &duplicate,
            &[
                "--core",
                "--mfg",
                "--expected-epoch",
                "--expected-revision",
                "--confirm"
            ]
        )
        .is_err());
        let mut unknown = complete;
        unknown.extend(["--capability".to_string(), "mfg.read".to_string()]);
        assert!(parse_exact_flags(
            &unknown,
            &[
                "--core",
                "--mfg",
                "--expected-epoch",
                "--expected-revision",
                "--confirm"
            ]
        )
        .is_err());
    }

    #[test]
    fn non_auth_cli_routing_remains_additive() {
        assert!(!should_open_tui(&["gateway".to_string()]));
        assert!(!should_open_tui(&["/status".to_string()]));
        assert!(should_open_tui(&["--resume".to_string()]));
    }
}

#[cfg(not(feature = "tui-surface"))]
fn should_open_tui(args: &[String]) -> bool {
    let first_arg = args.first().map(String::as_str);
    if args.iter().any(|arg| arg.trim_start().starts_with('/')) {
        return false;
    }

    match first_arg {
        Some(
            "--resume"
            | "--session"
            | "--session-id"
            | "-s"
            | "--model"
            | "-m"
            | "--yolo"
            | "--dangerously-skip-permissions"
            | "--danger-full-access",
        ) => true,
        Some(arg)
            if arg.starts_with("--resume=")
                || arg.starts_with("--session=")
                || arg.starts_with("--session-id=")
                || arg.starts_with("--model=") =>
        {
            true
        }
        _ => false,
    }
}
