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
    if let Err(error) = runtime::cowd_dirs::prepend_user_tool_bins_to_path() {
        eprintln!("warning: failed to activate Cowd tool path: {error}");
    }
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(status) = cli::dispatch_internal_process(&args) {
        return status;
    }
    let first_arg = args.first().map(String::as_str);

    if matches!(first_arg, Some("auth")) {
        return auth_profile_entry(&args[1..]);
    }

    if matches!(first_arg, Some("storage")) {
        return gateway::storage_entry(&args[1..]);
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
            "usage: cowd auth profile show | cowd auth profile set --core-profile <id> --apps <app=profile[,app=profile]> --expected-epoch <n> --expected-revision <n> --confirm <digest>"
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
            "--core-profile",
            "--apps",
            "--expected-epoch",
            "--expected-revision",
            "--confirm",
        ],
    )?;
    let core = flags["--core-profile"].clone();
    let app_profiles = parse_app_profiles(&flags["--apps"])?;
    let expected_epoch = flags["--expected-epoch"]
        .parse::<u64>()
        .map_err(|_| "--expected-epoch must be an integer".to_string())?;
    let expected_revision = flags["--expected-revision"]
        .parse::<u64>()
        .map_err(|_| "--expected-revision must be an integer".to_string())?;
    let supplied_confirmation = flags["--confirm"].clone();
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
    // The broker is the single authority for an APP profile's actual
    // capability union. Preview and mutation therefore share one catalogue
    // and one conflict token; CLI never reconstructs product capabilities.
    let (preview, expected_confirmation) = client
        .preview_human_entitlements(&credential, core.clone(), app_profiles.clone())
        .map_err(|error| error.to_string())?;
    let added = preview
        .ceiling
        .iter()
        .filter(|capability| !current.ceiling.contains(*capability))
        .cloned()
        .collect::<Vec<_>>();
    let removed = current
        .ceiling
        .iter()
        .filter(|capability| !preview.ceiling.contains(*capability))
        .cloned()
        .collect::<Vec<_>>();
    eprintln!(
        "profile preview: core={core} apps={} add={} remove={} confirmation={expected_confirmation}",
        flags["--apps"],
        added.join(","),
        removed.join(","),
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
            core,
            app_profiles,
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

fn parse_app_profiles(value: &str) -> Result<std::collections::BTreeMap<String, String>, String> {
    let mut profiles = std::collections::BTreeMap::new();
    for entry in value.split(',') {
        let (app_id, profile_id) = entry
            .split_once('=')
            .ok_or_else(|| "--apps entries must use app=profile".to_string())?;
        if app_id.trim().is_empty()
            || profile_id.trim().is_empty()
            || profiles
                .insert(app_id.trim().to_string(), profile_id.trim().to_string())
                .is_some()
        {
            return Err("--apps must contain unique non-empty app=profile entries".to_string());
        }
    }
    if profiles.is_empty() {
        return Err("--apps must contain at least one app=profile entry".to_string());
    }
    Ok(profiles)
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
    fn auth_profile_flags_accept_generic_app_profile_selections() {
        let args = vec![
            "--core-profile".to_string(),
            "core_manager".to_string(),
            "--apps".to_string(),
            "workbench=manager,developer=developer".to_string(),
        ];
        assert!(parse_exact_flags(
            &args,
            &[
                "--core-profile",
                "--apps",
                "--expected-epoch",
                "--expected-revision",
                "--confirm"
            ]
        )
        .is_err());
        let complete = vec![
            "--core-profile".to_string(),
            "core_manager".to_string(),
            "--apps".to_string(),
            "workbench=manager,developer=developer".to_string(),
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
                    "--core-profile",
                    "--apps",
                    "--expected-epoch",
                    "--expected-revision",
                    "--confirm"
                ]
            )
            .unwrap()["--apps"],
            "workbench=manager,developer=developer"
        );
        assert_eq!(
            parse_app_profiles("workbench=manager,developer=developer").unwrap(),
            std::collections::BTreeMap::from([
                ("developer".to_string(), "developer".to_string()),
                ("workbench".to_string(), "manager".to_string()),
            ])
        );
        assert!(parse_app_profiles("workbench=viewer,workbench=manager").is_err());
        assert!(parse_app_profiles("missing-separator").is_err());
        let mut unknown = complete;
        unknown.extend(["--capability".to_string(), "app.read".to_string()]);
        assert!(parse_exact_flags(
            &unknown,
            &[
                "--core-profile",
                "--apps",
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
