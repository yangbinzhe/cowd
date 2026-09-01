use std::{process::Command, time::Duration};

use crate::server;

const USER_GATEWAY_SERVICE: &str = "cowd-gateway.service";

pub(crate) fn systemd_lifecycle_eligible(
    config_home_overridden: bool,
    direct_lifecycle_requested: bool,
    unit_load_state: Option<&str>,
) -> bool {
    !config_home_overridden
        && !direct_lifecycle_requested
        && unit_load_state.is_some_and(|state| state.trim() == "loaded")
}

pub(crate) fn user_gateway_service_is_loaded() -> bool {
    let config_home_overridden = std::env::var_os("COWD_CONFIG_HOME").is_some();
    let direct_lifecycle_requested = std::env::var("COWD_GATEWAY_DIRECT_LIFECYCLE")
        .is_ok_and(|value| matches!(value.trim(), "1" | "true" | "yes"));
    if config_home_overridden || direct_lifecycle_requested {
        return false;
    }
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            USER_GATEWAY_SERVICE,
            "--property=LoadState",
            "--value",
        ])
        .output();
    let load_state = output.ok().and_then(|output| {
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
    });
    systemd_lifecycle_eligible(false, false, load_state.as_deref())
}

pub(crate) fn run_user_gateway_service_action(
    action: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new("systemctl")
        .args(["--user", action, USER_GATEWAY_SERVICE])
        .output()
        .map_err(|error| format!("failed to invoke the user Gateway service: {error}"))?;
    if output.status.success() {
        return Ok(());
    }
    let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
    Err(format!(
        "user Gateway service `{action}` failed{}",
        (!detail.is_empty())
            .then(|| format!(": {detail}"))
            .unwrap_or_default()
    )
    .into())
}

pub(crate) fn wait_for_managed_gateway_start(
    timeout: Duration,
) -> Result<server::ServerInfo, Box<dyn std::error::Error>> {
    let readiness_client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let started = std::time::Instant::now();
    while started.elapsed() < timeout {
        if let Some(status) = server::get_server_status().map_err(|error| error.to_string())? {
            let readiness_url = format!("{}/readyz", status.address.trim_end_matches('/'));
            if readiness_client
                .get(readiness_url)
                .send()
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(status);
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err("managed Gateway service did not become ready before timeout".into())
}
