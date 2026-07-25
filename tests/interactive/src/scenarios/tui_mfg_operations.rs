use crate::reporter::TestRunner;
use crate::tui::{TuiLaunchConfig, TuiSession};
use anyhow::{anyhow, Context};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

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
    let gateway_url = std::env::var("COWD_GATEWAY_URL")
        .context("COWD_GATEWAY_URL is required for MFG PTY evidence")?;
    let api_token = std::env::var("COWD_API_TOKEN").unwrap_or_default();
    let fixture = seed_mfg_fixture(&gateway_url, &api_token, nonce)?;
    let state_artifact = std::env::var_os("COWD_INTERACTIVE_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| isolated_root.clone())
        .join("mfg-state.json");
    if let Some(parent) = state_artifact.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create MFG observer directory {}", parent.display()))?;
    }
    let extra_env = BTreeMap::from([(
        "COWD_TUI_MFG_STATE_ARTIFACT".to_string(),
        state_artifact.display().to_string(),
    )]);
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
        gateway_url,
        api_token,
        session_id: format!("tui-mfg-{nonce}"),
        width: 80,
        height: 24,
        extra_env,
    };
    let tui = TuiSession::new(config)?;
    tui.wait_until_ready(20)?;
    tui.send("/mfg")?;
    tui.enter()?;
    tui.wait_for("MFG Operations", 20)?;
    tui.wait_for("Operational", 20)?;
    wait_mfg_state(&state_artifact, "mounted MFG observer", |state| {
        state.pointer("/live/generation").is_some()
    })?;
    tui.write_sidecar(
        "mfg-production-fixture",
        &[],
        json!({
            "status": "seeded_through_authenticated_product_routes",
            "facts": fixture,
        }),
    )?;
    println!("\n── TUI MFG Operations (governed actions producer) ──");

    runner.run(
        "MFG operational contract and action inventory visible",
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
                    "operational shell did not expose governed actions with an idle mutation queue"
            ));
            }
            Ok(())
        },
    );

    tui.send_key("Right")?;
    tui.wait_for("tab=Incidents", 20)?;
    tui.wait_for("id=", 20)?;

    runner.run("responsive 80-96-120 operational layout", || {
        let compact = tui.capture_step("responsive-80x24-list", &[])?;
        if compact.contains("Backlinks") {
            return Err(anyhow!("80x24 must use the single-column MFG layout"));
        }
        tui.send_key("Enter")?;
        tui.wait_for("focus=Detail", 20)?;
        let compact_detail = tui.capture_step("responsive-80x24-detail", &[])?;
        if !compact_detail.contains("Detail") {
            return Err(anyhow!("80x24 Enter did not switch to detail"));
        }

        tui.resize(96, 28)?;
        tui.wait_for("focus=Detail", 20)?;
        let medium = tui.capture_step("responsive-96x28", &[])?;
        if !medium.contains("Detail") {
            return Err(anyhow!("96x28 did not expose the two-column detail"));
        }

        tui.resize(120, 40)?;
        tui.wait_for("Backlinks", 20)?;
        tui.wait_for("\"incident\": {", 20)?;
        let wide = tui.capture_step("responsive-120x40", &[])?;
        if !wide.contains("Backlinks") || !wide.contains("Recovery") {
            return Err(anyhow!("120x40 did not expose the third context column"));
        }
        if wide.contains("\"incident\": null") || wide.contains("\"room\": null") {
            return Err(anyhow!(
                "selected incident stayed on an unwired null detail projection"
            ));
        }
        Ok(())
    });

    runner.run("responsive selection and focus survive resize", || {
        tui.send_key("Enter")?;
        tui.wait_for("focus=List", 20)?;
        tui.send_key("j")?;
        std::thread::sleep(std::time::Duration::from_millis(150));
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

    runner.run("MFG P1 insights and live transport are visibly wired", || {
        tui.resize(120, 40)?;
        focus_mfg_tabs(&tui)?;
        for _ in 0..5 {
            tui.send_key("Right")?;
        }
        tui.wait_for("tab=Insights", 20)?;
        let state = wait_mfg_state(&state_artifact, "MFG P1 observer", |state| {
            state
                .get("p1_routes")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|routes| routes.len() >= 6)
                && state
                    .get("insights")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|items| {
                        items.iter().any(|item| {
                            item.get("id").and_then(serde_json::Value::as_str)
                                == Some("supply-risk-analyst")
                        })
                    })
                && state
                    .pointer("/live/cursor")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|cursor| !cursor.is_empty())
        })?;
        tui.wait_for("P1 attempted=", 20)?;
        let capture = tui.capture_step("mfg-p1-insights-live", &[])?;
        if capture.contains("not-connected")
            || capture.contains("route declared")
            || !capture.contains("live stream:")
        {
            return Err(anyhow!("MFG live transport state is not visibly wired"));
        }
        let documents = mfg_numeric_fact(&capture, "documents").unwrap_or_default();
        if documents == 0 {
            return Err(anyhow!("MFG P1 projection retained no visible document"));
        }
        let routes = state
            .get("p1_routes")
            .and_then(serde_json::Value::as_array)
            .context("MFG P1 observer omitted route documents")?;
        if !routes.iter().any(|route| {
            route.as_str() == Some("mfg.incident.skill_run.list")
        }) {
            return Err(anyhow!(
                "selected incident never wired its incident skill-run read into the P1 projection"
            ));
        }
        Ok(())
    });

    runner.run("TUI read backlinks emit intent without operations", || {
        tui.resize(120, 40)?;
        focus_mfg_tabs(&tui)?;
        for _ in 0..5 {
            tui.send_key("Left")?;
        }
        tui.wait_for("tab=Incidents", 20)?;
        tui.wait_for("Backlinks", 20)?;
        tui.wait_for("Evidence ·", 20)?;
        let incident_context = tui.capture_step("backlink-incident-context", &[])?;
        assert_backlink(
            &tui,
            "e",
            "Evidence",
            &format!(
                "evidence://matrix/{}",
                fixture_string(&fixture, "evidence_packet_id")?
            ),
            &incident_context,
        )?;
        assert_backlink(
            &tui,
            "x",
            "Runtime",
            &format!("task://{}", fixture_string(&fixture, "task_id")?),
            &incident_context,
        )?;

        focus_mfg_tabs(&tui)?;
        for _ in 0..4 {
            tui.send_key("Right")?;
        }
        tui.wait_for("tab=Reviews", 20)?;
        tui.wait_for("Approval ·", 20)?;
        let review_context = tui.capture_step("backlink-review-context", &[])?;
        assert_backlink(
            &tui,
            "p",
            "Approval",
            &format!("approval://{}", fixture_string(&fixture, "approval_id")?),
            &review_context,
        )?;
        tui.send_key("Left")?;
        tui.wait_for("tab=Reports", 20)?;
        tui.wait_for("Surface ·", 20)?;
        let report_context = tui.capture_step("backlink-report-context", &[])?;
        assert_backlink(
            &tui,
            "s",
            "Surface",
            &format!(
                "receipt://cross-plane/{}",
                fixture_string(&fixture, "surface_receipt_id")?
            ),
            &report_context,
        )?;
        Ok(())
    });

    runner.run("high-risk cancel, receipt, and conflict sequence", || {
        tui.resize(120, 40)?;
        let before_cancel_state = wait_mfg_state(&state_artifact, "initial MFG observer", |state| {
            state.get("receipts").is_some_and(serde_json::Value::is_array)
        })?;
        let receipts_before_cancel = mfg_receipt_ids(&before_cancel_state);
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
        let cancelled_state = wait_mfg_state(&state_artifact, "cancelled MFG intent", |state| {
            state.pointer("/latest_action/status").and_then(serde_json::Value::as_str)
                == Some("cancelled")
        })?;
        let cancelled = tui.capture_step("action-escalate-cancelled", &[])?;
        if mfg_any_fact(&cancelled, "key").as_deref() != Some(cancelled_key.as_str())
            || !cancelled.contains("mutations=0")
            || mfg_receipt_ids(&cancelled_state) != receipts_before_cancel
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
        let accepted_state = wait_mfg_state(&state_artifact, "completed MFG receipt", |state| {
            state
                .get("receipts")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|receipts| {
                    receipts.iter().any(|receipt| {
                        receipt.get("status").and_then(serde_json::Value::as_str)
                            == Some("completed")
                            && receipt
                                .get("id")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|id| !receipts_before_cancel.contains(id))
                    })
                })
        })?;
        let accepted = tui.capture_step("action-escalate-accepted", &[])?;
        if !accepted.contains("receipt=")
            || !accepted.contains("correlation=")
            || !accepted.contains("result-revision=")
        {
            return Err(anyhow!(
                "accepted action did not expose canonical receipt evidence"
            ));
        }
        let accepted_receipt_entry = accepted_state
            .get("receipts")
            .and_then(serde_json::Value::as_array)
            .and_then(|receipts| {
                receipts.iter().find(|receipt| {
                    receipt.get("status").and_then(serde_json::Value::as_str) == Some("completed")
                        && receipt
                            .get("id")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|id| !receipts_before_cancel.contains(id))
                })
            })
            .context("accepted action observer omitted its completed receipt")?;
        let accepted_receipt = accepted_receipt_entry
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("accepted action observer omitted receipt ID")?
            .to_string();
        let accepted_revision = accepted_receipt_entry
            .get("result_revision")
            .and_then(serde_json::Value::as_u64)
            .context("accepted action observer omitted result revision")?;
        let accepted_receipt_status = accepted_receipt_entry
            .get("status")
            .and_then(serde_json::Value::as_str)
            .context("accepted action observer omitted receipt status")?
            .to_string();
        if accepted_receipt_status != "completed" {
            return Err(anyhow!(
                "accepted action canonical receipt was not completed: {accepted_receipt_status}"
            ));
        }
        let expected_revision = stale_revision
            .parse::<u64>()
            .map_err(|_| anyhow!("prepared action expected revision was not numeric"))?;
        tui.resize(80, 24)?;
        tui.wait_for("focus=Actions", 20)?;
        tui.wait_for("Context & Actions", 20)?;
        let accepted_compact = tui.capture_step("action-accepted-80x24", &[])?;
        if !accepted_compact.contains("receipt=") || !accepted_compact.contains("Recovery") {
            return Err(anyhow!(
                "80x24 action view hid the canonical receipt or recovery section"
            ));
        }
        tui.resize(96, 28)?;
        tui.wait_for("focus=Actions", 20)?;
        tui.wait_for("Context & Actions", 20)?;
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
        let conflict_state = wait_mfg_state(&state_artifact, "typed revision conflict", |state| {
            state
                .pointer("/latest_action/error_code")
                .and_then(serde_json::Value::as_str)
                == Some("revision_conflict")
        })?;
        let conflict = tui.capture_step("action-stale-revision-conflict", &[])?;
        if !conflict.contains("RevisionConflict")
            || !conflict.contains("retryable=false")
            || conflict.contains("result-revision=")
            || mfg_receipt_ids(&conflict_state) != mfg_receipt_ids(&accepted_state)
        {
            return Err(anyhow!(
                "stale revision did not stop at a non-overwriting typed conflict"
            ));
        }

        tui.write_sidecar(
            "mfg-governed-action-boundary",
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
                "status": "governed_action_producer_observed",
                "target_acceptance_ids": ["TUI-01", "TUI-02", "TUI-03", "TUI-04", "TUI-05"],
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

fn seed_mfg_fixture(base_url: &str, token: &str, nonce: u128) -> anyhow::Result<serde_json::Value> {
    mfg_api_json(
        base_url,
        token,
        "POST",
        "/api/apps/mfg/domain/server-manufacturing/seed",
        Some(&format!("mfg-pty-domain-seed-{nonce}")),
        Some(&json!({})),
    )?;

    let rule = mfg_api_json(
        base_url,
        token,
        "POST",
        "/api/apps/mfg/focus/alert-rules",
        Some(&format!("mfg-pty-rule-{nonce}")),
        Some(&json!({
            "rule": {
                "owner_ref": "principal:local-human",
                "name": "MFG PTY critical alert fixture",
                "metric_refs": [],
                "entity_refs": [],
                "condition": {},
                "severity": "critical",
                "enabled": true
            }
        })),
    )?;
    let rule_id = rule
        .pointer("/rule/rule_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY alert-rule fixture omitted rule_id")?;
    let alerts = mfg_api_json(
        base_url,
        token,
        "GET",
        "/api/apps/mfg/focus/alerts?limit=100",
        None,
        None,
    )?;
    let alert_id = alerts
        .pointer("/items/0/occurrence_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY domain/rule fixture produced no alert occurrence")?;

    let incident = mfg_api_json(
        base_url,
        token,
        "POST",
        "/api/apps/mfg/incidents",
        Some(&format!("mfg-pty-incident-{nonce}")),
        Some(&json!({
            "request_id": format!("mfg-pty-incident-{nonce}"),
            "session_id": format!("mfg-pty-fixture-{nonce}"),
            "title": "MFG PTY production incident"
        })),
    )?;
    let incident_id = incident
        .pointer("/incident/incident_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY incident fixture omitted incident_id")?;
    let task_id = incident
        .pointer("/incident/task_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY incident fixture omitted task_id")?;
    let evidence_packet_id = incident
        .pointer("/incident/evidence_packet_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY incident fixture omitted evidence_packet_id")?;

    let grant_id = format!("mfg-pty-delivery-grant-{nonce}");
    mfg_api_json(
        base_url,
        token,
        "POST",
        "/api/cross-plane/grants",
        None,
        Some(&json!({
            "id": grant_id,
            "principal_id": "principal:local-human",
            "capability": "channel.feishu.send_text",
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "tui-mfg-production-acceptance",
            "approval_id": null
        })),
    )?;
    let profile = mfg_api_json(
        base_url,
        token,
        "POST",
        "/api/apps/mfg/cockpit/profiles/upsert",
        Some(&format!("mfg-pty-profile-{nonce}")),
        Some(&json!({
            "request_id": format!("mfg-pty-profile-{nonce}"),
            "profile": {
                "owner_ref": "principal:local-human",
                "display_name": "MFG PTY production cockpit",
                "cadence": "daily",
                "scope": {"kind": "personal"},
                "layout": {"columns": 12, "row_height": 72, "gap": 12},
                "global_filters": {},
                "widget_instances": [],
                "sharing_policy": {
                    "visibility": "private",
                    "viewer_refs": [],
                    "editor_refs": []
                }
            }
        })),
    )?;
    let profile_id = profile
        .pointer("/profile/profile_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY profile fixture omitted profile_id")?;
    let report = mfg_api_json(
        base_url,
        token,
        "POST",
        &format!("/api/apps/mfg/cockpit/profiles/{profile_id}/reports/generate"),
        Some(&format!("mfg-pty-report-{nonce}")),
        Some(&json!({
            "request_id": format!("mfg-pty-report-{nonce}"),
            "report": {
                "cadence": "daily",
                "note": "MFG PTY production acceptance"
            }
        })),
    )?;
    let report_id = report
        .pointer("/report/report_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY report fixture omitted report_id")?
        .to_string();
    let mut report_revision = report
        .pointer("/report/revision")
        .and_then(serde_json::Value::as_u64)
        .context("MFG PTY report fixture omitted revision")?;
    let mut surface_receipt_id = None;
    for attempt in 1..=3 {
        let delivery = mfg_api_json(
            base_url,
            token,
            "POST",
            &format!("/api/apps/mfg/cockpit/reports/{report_id}/deliver"),
            Some(&format!("mfg-pty-delivery-{nonce}-{attempt}")),
            Some(&json!({
                "mode": "commit",
                "expected_revision": report_revision,
                "channel": "feishu",
                "target_ref": format!("channel://feishu/user/mfg-pty-{attempt}"),
                "source_channel": "mfg.tui.production.acceptance"
            })),
        )?;
        if delivery
            .pointer("/cross_plane_execution_receipt/dispatch_status")
            .and_then(serde_json::Value::as_str)
            != Some("dispatch_failed")
        {
            return Err(anyhow!(
                "MFG PTY delivery fixture did not reach the real failing Surface executor: {delivery}"
            ));
        }
        surface_receipt_id = delivery
            .pointer("/cross_plane_execution_receipt/id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        report_revision = delivery
            .pointer("/report/revision")
            .and_then(serde_json::Value::as_u64)
            .context("MFG PTY delivery fixture omitted updated report revision")?;
    }
    let surface_receipt_id =
        surface_receipt_id.context("MFG PTY delivery fixture omitted cross-plane receipt")?;
    let review = mfg_api_json(
        base_url,
        token,
        "POST",
        &format!("/api/apps/mfg/cockpit/reports/{report_id}/reviews"),
        Some(&format!("mfg-pty-review-{nonce}")),
        Some(&json!({
            "expected_report_revision": report_revision,
            "reason": "MFG PTY production acceptance exhausted delivery",
            "evidence_refs": ["evidence://mfg-pty/production-acceptance"]
        })),
    )?;
    let review_id = review
        .pointer("/review/review_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY review fixture omitted review_id")?;
    let approval_id = review
        .pointer("/review/approval_id")
        .and_then(serde_json::Value::as_str)
        .context("MFG PTY review fixture omitted approval_id")?;

    Ok(json!({
        "rule_id": rule_id,
        "alert_id": alert_id,
        "incident_id": incident_id,
        "task_id": task_id,
        "evidence_packet_id": evidence_packet_id,
        "profile_id": profile_id,
        "report_id": report_id,
        "report_revision": report_revision,
        "surface_receipt_id": surface_receipt_id,
        "review_id": review_id,
        "approval_id": approval_id
    }))
}

fn mfg_api_json(
    base_url: &str,
    token: &str,
    method: &str,
    path: &str,
    idempotency_key: Option<&str>,
    body: Option<&serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let mut command = Command::new("curl");
    command
        .args(["--fail-with-body", "-sS", "-X", method])
        .arg(format!("{base_url}{path}"))
        .args(["-H", &format!("Authorization: Bearer {token}")]);
    if let Some(idempotency_key) = idempotency_key {
        command.args(["-H", &format!("idempotency-key: {idempotency_key}")]);
    }
    if let Some(body) = body {
        command
            .args(["-H", "content-type: application/json"])
            .arg("--data-binary")
            .arg(serde_json::to_string(body)?);
    }
    let output = command
        .output()
        .with_context(|| format!("execute MFG fixture request {method} {path}"))?;
    if !output.status.success() {
        return Err(anyhow!(
            "MFG fixture request {method} {path} failed ({}): stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("decode MFG fixture response {method} {path}"))
}

fn assert_backlink(
    tui: &TuiSession,
    key: &str,
    label: &str,
    expected_target: &str,
    context: &str,
) -> anyhow::Result<()> {
    let target = backlink_target(context, label)
        .ok_or_else(|| anyhow!("fixture has no canonical {label} backlink target"))?;
    if target != expected_target {
        return Err(anyhow!(
            "{label} backlink target mismatch: expected {expected_target}, observed {target}"
        ));
    }
    tui.send_key(key)?;
    let identity = target
        .split(['/', ':'])
        .next_back()
        .unwrap_or(target.as_str())
        .split(['?', '#'])
        .next()
        .unwrap_or_default();
    if label == "Evidence" {
        tui.wait_for("Focused evidence", 20)?;
        tui.wait_for("resolved", 20)?;
    } else {
        tui.wait_for("Resolved object:", 20)?;
    }
    let capture =
        tui.capture_step(&format!("backlink-{}", label.to_ascii_lowercase()), &[])?;
    let resolved = if label == "Evidence" {
        capture.contains("Focused evidence")
            && capture.contains("Resolution:")
            && capture.contains("resolved")
            && capture.contains(identity)
            && !capture.contains("Resolution failed")
    } else {
        resolved_object_summary(&capture).is_some_and(|summary| {
            let compact = summary
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            let lower = summary.to_ascii_lowercase();
            compact.contains(identity)
                && !lower.contains("loading canonical")
                && !lower.contains("resolution failed")
        })
    };
    if capture.contains(&format!("No {label} backlink")) || !resolved {
        return Err(anyhow!(
            "{label} backlink did not focus its destination panel on canonical target {target}"
        ));
    }
    tui.send("/mfg")?;
    tui.enter()?;
    tui.wait_for("MFG Operations", 20)?;
    Ok(())
}

fn focus_mfg_tabs(tui: &TuiSession) -> anyhow::Result<()> {
    for _ in 0..8 {
        if tui.capture()?.contains("focus=Tabs") {
            return Ok(());
        }
        tui.send_key("F6")?;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Err(anyhow!("MFG focus cycle never reached the tab strip"))
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

fn mfg_numeric_fact(capture: &str, key: &str) -> Option<usize> {
    let marker = format!("{key}=");
    capture.lines().find_map(|line| {
        let value = line.split(&marker).nth(1)?;
        let digits = value
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>();
        (!digits.is_empty())
            .then(|| digits.parse::<usize>().ok())
            .flatten()
    })
}

fn mfg_action_fact(capture: &str, key: &str) -> Option<String> {
    let marker = format!("{key}=");
    let mut after_target = false;
    for line in capture.lines() {
        if line.contains("target=") {
            after_target = true;
        }
        if after_target {
            if let Some(value) = line
                .split(&marker)
                .nth(1)
                .and_then(|value| value.split_whitespace().next())
            {
                return Some(value.trim_end_matches('·').to_string());
            }
            if line.contains("CONFIRM ") {
                break;
            }
        }
    }
    None
}

fn backlink_target(capture: &str, label: &str) -> Option<String> {
    let marker = format!("{label} ·");
    let right_column = capture.lines().filter_map(|line| {
        let (_, right) = line.rsplit_once("││")?;
        Some(right.trim_matches('│').trim().to_string())
    });
    let mut target = None;
    for fragment in right_column {
        if let Some(start) = fragment.strip_prefix(&marker) {
            target = Some(start.trim().to_string());
            continue;
        }
        let Some(current) = target.as_mut() else {
            continue;
        };
        if fragment.is_empty()
            || [
                "Backlinks",
                "Governed actions",
                "Governed action status",
                "Recovery",
                "Route projection",
                "Evidence ·",
                "Runtime ·",
                "Approval ·",
                "Surface ·",
            ]
            .iter()
            .any(|boundary| fragment.starts_with(boundary))
        {
            break;
        }
        current.push_str(fragment.trim());
    }
    target.filter(|target| !target.is_empty())
}

fn resolved_object_summary(capture: &str) -> Option<String> {
    let mut summary = None;
    for line in capture.lines() {
        let Some(cell) = rightmost_panel_cell(line) else {
            continue;
        };
        if let Some(start) = cell.strip_prefix("Resolved object:") {
            summary = Some(start.trim().to_string());
            continue;
        }
        let Some(current) = summary.as_mut() else {
            continue;
        };
        if cell.is_empty() {
            break;
        }
        current.push(' ');
        current.push_str(&cell);
    }
    summary.filter(|summary| !summary.trim().is_empty())
}

fn rightmost_panel_cell(line: &str) -> Option<String> {
    let boundaries = line
        .char_indices()
        .filter_map(|(index, character)| (character == '│').then_some(index))
        .collect::<Vec<_>>();
    let (start, end) = match boundaries.as_slice() {
        [.., start, end] => (*start, *end),
        _ => return None,
    };
    Some(line[start + '│'.len_utf8()..end].trim().to_string())
}

fn fixture_string<'a>(fixture: &'a serde_json::Value, key: &str) -> anyhow::Result<&'a str> {
    fixture
        .get(key)
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("MFG fixture omitted {key}"))
}

fn wait_mfg_state(
    path: &std::path::Path,
    label: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> anyhow::Result<serde_json::Value> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        if let Ok(body) = fs::read(path) {
            if let Ok(state) = serde_json::from_slice::<serde_json::Value>(&body) {
                if predicate(&state) {
                    return Ok(state);
                }
                last = Some(state);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Err(anyhow!(
        "timed out waiting for {label}; last observer state={}",
        last.unwrap_or(serde_json::Value::Null)
    ))
}

fn mfg_receipt_ids(state: &serde_json::Value) -> BTreeSet<String> {
    state
        .get("receipts")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|receipt| receipt.get("id").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backlink_parser_reconstructs_a_target_wrapped_inside_the_right_column() {
        let capture = concat!(
            "│ left ││ detail ││Runtime ·                   │\n",
            "│      ││        ││task://task-a60bc7ec89bb2f19│\n",
            "│      ││        ││64a5                        │\n",
            "│      ││        ││Evidence ·                  │\n",
        );
        assert_eq!(
            backlink_target(capture, "Runtime").as_deref(),
            Some("task://task-a60bc7ec89bb2f1964a5")
        );
    }

    #[test]
    fn receipt_identity_comparison_is_independent_of_terminal_wrapping() {
        let state = serde_json::json!({
            "receipts": [
                {"id": "receipt-1", "status": "completed", "result_revision": 2},
                {"id": "receipt-1", "status": "completed", "result_revision": 2}
            ]
        });
        assert_eq!(
            mfg_receipt_ids(&state),
            BTreeSet::from(["receipt-1".to_string()])
        );
    }

    #[test]
    fn resolved_object_parser_reconstructs_a_wrapped_sidebar_identity() {
        let capture = concat!(
            "│ left │ │Resolved object: task             │\n",
            "│ toast │ │task-c5390a98fffa12bbbbdb status  │\n",
            "│       │ │running phase implementation      │\n",
            "│       │ │                                  │\n",
        );
        assert_eq!(
            resolved_object_summary(capture).as_deref(),
            Some("task task-c5390a98fffa12bbbbdb status running phase implementation")
        );
    }
}
