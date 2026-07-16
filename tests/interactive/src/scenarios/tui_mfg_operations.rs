use crate::reporter::TestRunner;
use crate::tui::{TuiLaunchConfig, TuiSession};
use anyhow::{anyhow, Context};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub fn has_scenario(name: &str) -> bool {
    matches!(name, "tui_mfg_operations" | "all")
}

pub fn run(runner: &mut TestRunner) -> anyhow::Result<()> {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let workspace = std::env::var_os("COWD_MFG_TEST_WORKSPACE")
        .map(PathBuf::from)
        .unwrap_or(std::env::current_dir()?);
    let isolated_root =
        std::env::temp_dir().join(format!("cowd-tui-mfg-{}-{nonce}", std::process::id()));
    let config = TuiLaunchConfig {
        name: "tui-mfg-operations".to_string(),
        cowd_bin: PathBuf::from(
            std::env::var_os("COWD_BIN").context("COWD_BIN is required for MFG PTY evidence")?,
        ),
        workspace,
        config_home: std::env::var_os("COWD_MFG_TEST_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| isolated_root.join("config")),
        home_dir: std::env::var_os("COWD_MFG_TEST_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| isolated_root.join("home")),
        gateway_url: std::env::var("COWD_GATEWAY_URL")
            .context("COWD_GATEWAY_URL is required for MFG PTY evidence")?,
        api_token: std::env::var("COWD_API_TOKEN").unwrap_or_default(),
        session_id: format!("tui-mfg-{nonce}"),
        width: 80,
        height: 24,
        extra_env: BTreeMap::new(),
    };
    let tui = TuiSession::new(config)?;
    tui.wait_until_ready(20)?;
    tui.send("/mfg")?;
    tui.enter()?;
    tui.wait_for("MFG Operations", 20)?;
    tui.wait_for("mfg.frontend.v1", 20)?;
    tui.wait_for("ReadOnly", 20)?;
    println!("\n── TUI MFG Operations (V543 read-only producer) ──");

    runner.run("V543 MFG contract and read-only shell visible", || {
        let capture = tui.capture_step("mfg-open-80x24", &[])?;
        if !capture.contains("MFG Operations")
            || !capture.contains("ReadOnly")
            || capture.contains("contract pending")
            || capture.contains("refreshed=never")
        {
            return Err(anyhow!("MFG read-only contract status is not visible"));
        }
        if !capture.contains("actions=0") || !capture.contains("mutations=0") {
            return Err(anyhow!(
                "V543 read-only shell did not prove zero actions and zero mutations"
            ));
        }
        Ok(())
    });

    tui.send_key("Right")?;
    tui.wait_for("tab=Incidents", 20)?;
    tui.wait_for("id=", 20)?;

    runner.run("V543 responsive 80-96-120 read layout", || {
        let compact = tui.capture_step("responsive-80x24-list", &[])?;
        if compact.contains("Backlinks") {
            return Err(anyhow!("80x24 must use the single-column MFG layout"));
        }
        tui.send_key("Enter")?;
        let compact_detail = tui.capture_step("responsive-80x24-detail", &[])?;
        if !compact_detail.contains("Detail") {
            return Err(anyhow!("80x24 Enter did not switch to detail"));
        }

        tui.resize(96, 30)?;
        let medium = tui.capture_step("responsive-96x30", &[])?;
        if !medium.contains("Detail") {
            return Err(anyhow!("96x30 did not expose the two-column detail"));
        }

        tui.resize(120, 40)?;
        let wide = tui.capture_step("responsive-120x40", &[])?;
        if !wide.contains("Backlinks") || !wide.contains("Recovery") {
            return Err(anyhow!("120x40 did not expose the third context column"));
        }
        Ok(())
    });

    runner.run("V543 responsive selection and focus survive resize", || {
        tui.send_key("Enter")?;
        tui.send_key("j")?;
        let before = tui.capture_step("selection-before-resize", &[])?;
        let selected = selected_object_id(&before)
            .ok_or_else(|| anyhow!("fixture has no stable selected MFG object"))?;
        let focus = mfg_header_fact(&before, "focus")
            .ok_or_else(|| anyhow!("MFG focus was not exposed in the evidence header"))?;
        let list_scroll = mfg_header_fact(&before, "list-scroll")
            .ok_or_else(|| anyhow!("MFG list scroll was not exposed in the evidence header"))?;
        tui.resize(80, 24)?;
        tui.resize(96, 30)?;
        let after = tui.capture_step("selection-after-resize", &[])?;
        if !after.contains(&format!("id={selected}")) {
            return Err(anyhow!(
                "selected object changed across resize: expected {selected}"
            ));
        }
        if mfg_header_fact(&after, "focus").as_deref() != Some(focus.as_str())
            || mfg_header_fact(&after, "list-scroll").as_deref() != Some(list_scroll.as_str())
        {
            return Err(anyhow!("MFG focus or list scroll changed across resize"));
        }
        Ok(())
    });

    runner.run("TUI read backlinks emit intent without operations", || {
        tui.resize(120, 40)?;
        let incident_context = tui.capture_step("backlink-incident-context", &[])?;
        assert_backlink(&tui, "e", "Evidence", &incident_context)?;
        assert_backlink(&tui, "s", "Surface", &incident_context)?;
        assert_backlink(&tui, "x", "Runtime", &incident_context)?;

        for _ in 0..3 {
            tui.send_key("Tab")?;
        }
        for _ in 0..4 {
            tui.send_key("Right")?;
        }
        tui.wait_for("tab=Reviews", 20)?;
        tui.wait_for("id=review", 20)?;
        let review_context = tui.capture_step("backlink-review-context", &[])?;
        assert_backlink(&tui, "p", "Approval", &review_context)?;
        tui.write_sidecar(
            "v543-read-only-boundary",
            &[],
            json!({
                "assertions": [
                    "contract_read_only",
                    "responsive_80_96_120",
                    "selection_preserved",
                    "backlink_intents"
                ],
                "status": "v543_read_only_producer_observed",
                "target_acceptance_ids": ["TUI-01", "TUI-02", "TUI-03", "TUI-04"],
                "deferred_acceptance_ids": ["TUI-01", "TUI-02", "TUI-03", "TUI-04", "TUI-05", "TUI-06", "TUI-07", "TUI-08"],
                "receipt": null,
                "cursor": null,
                "pending_mutation": null
            }),
        )?;
        Ok(())
    });

    tui.close()
}

fn assert_backlink(tui: &TuiSession, key: &str, label: &str, context: &str) -> anyhow::Result<()> {
    let target = backlink_target(context, label)
        .ok_or_else(|| anyhow!("fixture has no canonical {label} backlink target"))?;
    tui.send_key(key)?;
    std::thread::sleep(std::time::Duration::from_millis(150));
    let capture = tui.capture_step(&format!("backlink-{}", label.to_ascii_lowercase()), &[])?;
    if capture.contains(&format!("No {label} backlink"))
        || !capture.contains(&format!("{label} backlink"))
        || !capture.contains(&target)
    {
        return Err(anyhow!(
            "{label} backlink intent did not preserve canonical target {target}"
        ));
    }
    Ok(())
}

fn selected_object_id(capture: &str) -> Option<String> {
    capture
        .lines()
        .find(|line| line.contains('›'))
        .and_then(|line| line.split("id=").nth(1))
        .and_then(|value| value.split_whitespace().next())
        .map(|value| {
            value.trim_matches(|character: char| {
                !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.')
            })
        })
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn mfg_header_fact(capture: &str, key: &str) -> Option<String> {
    let marker = format!("{key}=");
    capture
        .lines()
        .find(|line| line.contains("selection-revision="))
        .and_then(|line| line.split(&marker).nth(1))
        .and_then(|value| value.split_whitespace().next())
        .map(|value| value.trim_end_matches('·').to_string())
}

fn backlink_target(capture: &str, label: &str) -> Option<String> {
    capture
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&format!("{label} · ")))
        .map(str::trim)
        .filter(|target| !target.is_empty())
        .map(str::to_string)
}
