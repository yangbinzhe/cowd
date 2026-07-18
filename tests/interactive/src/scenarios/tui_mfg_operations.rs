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
    tui.wait_for("Operational", 20)?;
    println!("\n── TUI MFG Operations (V544 governed actions producer) ──");

    runner.run(
        "V544 MFG operational contract and action inventory visible",
        || {
            let capture = tui.capture_step("mfg-open-80x24", &[])?;
            if !capture.contains("MFG Operations")
                || !capture.contains("Operational")
                || capture.contains("contract pending")
                || capture.contains("refreshed=never")
            {
                return Err(anyhow!("MFG operational contract status is not visible"));
            }
            let action_count = mfg_any_fact(&capture, "actions")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or_default();
            if action_count == 0 || !capture.contains("mutations=0") {
                return Err(anyhow!(
                "V544 operational shell did not expose governed actions with an idle mutation queue"
            ));
            }
            Ok(())
        },
    );

    tui.send_key("Right")?;
    tui.wait_for("tab=Incidents", 20)?;
    tui.wait_for("id=", 20)?;

    runner.run("V544 responsive 80-96-120 operational layout", || {
        let compact = tui.capture_step("responsive-80x24-list", &[])?;
        if compact.contains("Backlinks") {
            return Err(anyhow!("80x24 must use the single-column MFG layout"));
        }
        tui.send_key("Enter")?;
        let compact_detail = tui.capture_step("responsive-80x24-detail", &[])?;
        if !compact_detail.contains("Detail") {
            return Err(anyhow!("80x24 Enter did not switch to detail"));
        }

        tui.resize(96, 28)?;
        let medium = tui.capture_step("responsive-96x28", &[])?;
        if !medium.contains("Detail") {
            return Err(anyhow!("96x28 did not expose the two-column detail"));
        }

        tui.resize(120, 40)?;
        let wide = tui.capture_step("responsive-120x40", &[])?;
        if !wide.contains("Backlinks") || !wide.contains("Recovery") {
            return Err(anyhow!("120x40 did not expose the third context column"));
        }
        Ok(())
    });

    runner.run("V544 responsive selection and focus survive resize", || {
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
        tui.resize(96, 28)?;
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
        assert_backlink(&tui, "x", "Runtime", &incident_context)?;

        tui.send_key("BTab")?;
        for _ in 0..4 {
            tui.send_key("Right")?;
        }
        tui.wait_for("tab=Reviews", 20)?;
        tui.wait_for("id=review", 20)?;
        let review_context = tui.capture_step("backlink-review-context", &[])?;
        assert_backlink(&tui, "p", "Approval", &review_context)?;
        tui.send_key("Left")?;
        tui.wait_for("tab=Reports", 20)?;
        tui.wait_for("id=report", 20)?;
        let report_context = tui.capture_step("backlink-report-context", &[])?;
        assert_backlink(&tui, "s", "Surface", &report_context)?;
        Ok(())
    });

    runner.run("V544 high-risk cancel, receipt, and conflict sequence", || {
        tui.resize(120, 40)?;
        tui.send("/mfg action mfg.alert.escalate")?;
        tui.enter()?;
        tui.wait_for("AwaitingConfirmation", 20)?;
        let prepared = tui.capture_step("action-escalate-prepared", &[])?;
        let cancelled_key = mfg_any_fact(&prepared, "key")
            .ok_or_else(|| anyhow!("prepared action did not expose its idempotency key"))?;
        if !prepared.contains("target=mfg:alert-occurrence:")
            || !prepared.contains("revision=")
            || !prepared.contains("CONFIRM High")
        {
            return Err(anyhow!(
                "high-risk confirmation omitted target, revision, or impact"
            ));
        }
        tui.send("/mfg cancel")?;
        tui.enter()?;
        tui.wait_for("Cancelled", 20)?;
        let cancelled = tui.capture_step("action-escalate-cancelled", &[])?;
        if mfg_any_fact(&cancelled, "key").as_deref() != Some(cancelled_key.as_str())
            || !cancelled.contains("mutations=0")
            || cancelled.contains("receipt=")
        {
            return Err(anyhow!(
                "cancelled action sent a request, consumed its key, or created a receipt"
            ));
        }

        tui.send("/mfg action mfg.alert.escalate")?;
        tui.enter()?;
        tui.wait_for("AwaitingConfirmation", 20)?;
        let commit_prepared = tui.capture_step("action-escalate-confirmation", &[])?;
        let stale_revision = mfg_action_fact(&commit_prepared, "revision")
            .ok_or_else(|| anyhow!("confirmed action did not expose expected revision"))?;
        tui.send("/mfg confirm")?;
        tui.enter()?;
        tui.wait_for("Accepted", 20)?;
        tui.wait_for("receipt=", 20)?;
        let accepted = tui.capture_step("action-escalate-accepted", &[])?;
        if !accepted.contains("receipt=")
            || !accepted.contains("correlation=")
            || !accepted.contains("result-revision=")
        {
            return Err(anyhow!(
                "accepted action did not expose canonical receipt evidence"
            ));
        }
        let accepted_receipt = mfg_any_fact(&accepted, "receipt")
            .ok_or_else(|| anyhow!("accepted action receipt ID was not parseable"))?;
        let accepted_revision = mfg_any_fact(&accepted, "result-revision")
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("accepted action result revision was not parseable"))?;
        let accepted_receipt_status = mfg_receipt_status(&accepted)
            .ok_or_else(|| anyhow!("accepted action receipt status was not parseable"))?;
        if accepted_receipt_status != "completed" {
            return Err(anyhow!(
                "accepted action canonical receipt was not completed: {accepted_receipt_status}"
            ));
        }
        let expected_revision = stale_revision
            .parse::<u64>()
            .map_err(|_| anyhow!("prepared action expected revision was not numeric"))?;
        tui.resize(80, 24)?;
        let accepted_compact = tui.capture_step("action-accepted-80x24", &[])?;
        if !accepted_compact.contains("receipt=") || !accepted_compact.contains("Recovery") {
            return Err(anyhow!(
                "80x24 action view hid the canonical receipt or recovery section"
            ));
        }
        tui.resize(96, 28)?;
        let accepted_medium = tui.capture_step("action-accepted-96x28", &[])?;
        if !accepted_medium.contains("receipt=") || !accepted_medium.contains("Recovery") {
            return Err(anyhow!(
                "96x28 action view hid the canonical receipt or recovery section"
            ));
        }
        tui.resize(120, 40)?;

        tui.send(&format!(
            "/mfg action mfg.alert.resolve {{\"body\":{{\"command\":\"resolve\",\"expected_revision\":{stale_revision},\"reason\":\"PTY conflict proof\"}}}}"
        ))?;
        tui.enter()?;
        tui.wait_for("AwaitingConfirmation", 20)?;
        tui.send("/mfg confirm")?;
        tui.enter()?;
        tui.wait_for("Conflict", 20)?;
        let conflict = tui.capture_step("action-stale-revision-conflict", &[])?;
        if !conflict.contains("RevisionConflict")
            || !conflict.contains("retryable=false")
            || conflict.contains("result-revision=")
        {
            return Err(anyhow!(
                "stale revision did not stop at a non-overwriting typed conflict"
            ));
        }

        tui.write_sidecar(
            "v544-governed-action-boundary",
            &[],
            json!({
                "assertions": [
                    "contract_operational",
                    "responsive_80_96_120",
                    "selection_preserved",
                    "backlink_navigation",
                    "high_risk_cancel_no_request",
                    "accepted_receipt_visible",
                    "stale_revision_conflict_no_overwrite"
                ],
                "status": "v544_governed_action_producer_observed",
                "target_acceptance_ids": ["TUI-01", "TUI-02", "TUI-03", "TUI-04", "TUI-05"],
                "deferred_acceptance_ids": ["TUI-01", "TUI-02", "TUI-03", "TUI-04", "TUI-05", "TUI-06", "TUI-07", "TUI-08"],
                "method": "POST",
                "path": "/api/apps/mfg/focus/alerts/:id/command",
                "receipt_id": accepted_receipt,
                "receipt_status": accepted_receipt_status,
                "replayed": false,
                "revision_before": expected_revision,
                "revision_after": accepted_revision,
                "receipt": mfg_any_fact(&accepted, "receipt"),
                "cursor": null,
                "pending_mutation": mfg_any_fact(&conflict, "mutations")
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
    let identity = target
        .split(['/', ':'])
        .next_back()
        .unwrap_or(target.as_str())
        .split(['?', '#'])
        .next()
        .unwrap_or_default();
    let mut capture = String::new();
    for _ in 0..200 {
        capture = tui.capture_step(&format!("backlink-{}", label.to_ascii_lowercase()), &[])?;
        let resolved = if label == "Evidence" {
            capture.contains("focused_evidence_ref")
                && capture.contains(&identity)
                && capture.contains("\"evidence_backlink_resolved\": true")
        } else {
            capture
                .lines()
                .find(|line| line.contains("Resolved object:"))
                .is_some_and(|line| {
                    line.contains(identity)
                        && !line.to_ascii_lowercase().contains("loading")
                        && !line.to_ascii_lowercase().contains("unavailable")
                        && !line.to_ascii_lowercase().contains("failed")
                })
        };
        if resolved {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    if capture.contains(&format!("No {label} backlink"))
        || !capture.contains(&format!("{label} backlink"))
        || !capture.contains(&target)
        || (label == "Evidence"
            && (!capture.contains(&identity)
                || !capture.contains("\"evidence_backlink_resolved\": true")))
        || (label != "Evidence"
            && !capture.lines().any(|line| {
                line.contains("Resolved object:")
                    && line.contains(identity)
                    && !line.to_ascii_lowercase().contains("loading")
                    && !line.to_ascii_lowercase().contains("failed")
            }))
    {
        return Err(anyhow!(
            "{label} backlink did not focus its destination panel on canonical target {target}"
        ));
    }
    tui.send("/mfg")?;
    tui.enter()?;
    tui.wait_for("MFG Operations", 20)?;
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

fn mfg_any_fact(capture: &str, key: &str) -> Option<String> {
    let marker = format!("{key}=");
    capture
        .lines()
        .find_map(|line| line.split(&marker).nth(1))
        .and_then(|value| value.split_whitespace().next())
        .map(|value| {
            value
                .trim_end_matches('·')
                .trim_end_matches(',')
                .to_string()
        })
}

fn mfg_receipt_status(capture: &str) -> Option<String> {
    capture
        .lines()
        .find(|line| line.contains("receipt=") && line.contains("correlation="))
        .and_then(|line| line.split('·').nth(1))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
}

fn mfg_action_fact(capture: &str, key: &str) -> Option<String> {
    let marker = format!("{key}=");
    capture
        .lines()
        .find(|line| line.contains("target=") && line.contains(&marker))
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
