use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::terminal_matrix::{
    render_terminal_capability_matrix_markdown, terminal_capability_matrix,
};

#[must_use]
pub fn terminal_gate_report(evidence_dir: PathBuf) -> Value {
    terminal_gate_report_with_report(evidence_dir, None)
}

#[must_use]
pub fn terminal_gate_report_with_report(
    evidence_dir: PathBuf,
    report_json: Option<PathBuf>,
) -> Value {
    let expected = (6..=12)
        .map(|version| format!("v{version}-evidence.md"))
        .collect::<Vec<_>>();
    let files = expected
        .iter()
        .map(|file| {
            let path = evidence_dir.join(file);
            serde_json::json!({
                "file": file,
                "path": path.display().to_string(),
                "present": path.exists(),
            })
        })
        .collect::<Vec<_>>();
    let missing = files
        .iter()
        .filter(|item| {
            !item
                .get("present")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        })
        .count();
    let matrix = terminal_capability_matrix(&evidence_dir);
    let failed_matrix_rows = matrix.iter().filter(|row| row.status != "passed").count();
    let report_checks = report_json
        .as_ref()
        .map(|path| terminal_report_checks(path))
        .unwrap_or_default();
    let failed_report_checks = report_checks
        .iter()
        .filter(|item| {
            item.get("required")
                .and_then(Value::as_bool)
                .unwrap_or(true)
                && item.get("status").and_then(Value::as_str) != Some("passed")
        })
        .count();
    let matrix_markdown = render_terminal_capability_matrix_markdown(&matrix);
    let matrix_path = evidence_dir.join("terminal-capability-matrix.md");
    let report_path = evidence_dir.join("terminal-gate-report.json");
    let _ = fs::create_dir_all(&evidence_dir);
    let _ = fs::write(&matrix_path, matrix_markdown);
    let report = serde_json::json!({
        "kind": "harness_eval.terminal_gate",
        "status": if missing == 0 && failed_matrix_rows == 0 && failed_report_checks == 0 { "passed" } else { "failed" },
        "evidence_dir": evidence_dir.display().to_string(),
        "report_json": report_json.as_ref().map(|path| path.display().to_string()),
        "expected": expected,
        "missing": missing,
        "files": files,
        "matrix_rows": matrix,
        "failed_matrix_rows": failed_matrix_rows,
        "report_checks": report_checks,
        "failed_report_checks": failed_report_checks,
        "matrix_path": matrix_path.display().to_string(),
        "report_path": report_path.display().to_string(),
    });
    if let Ok(text) = serde_json::to_string_pretty(&report) {
        let _ = fs::write(&report_path, text);
    }
    report
}

fn terminal_report_checks(report_path: &Path) -> Vec<Value> {
    let Ok(text) = fs::read_to_string(report_path) else {
        return vec![check(
            "report_json_readable",
            false,
            true,
            format!("cannot read {}", report_path.display()),
            "pass a readable harness eval report.json path",
        )];
    };
    let Ok(report) = serde_json::from_str::<Value>(&text) else {
        return vec![check(
            "report_json_parseable",
            false,
            true,
            format!("cannot parse {}", report_path.display()),
            "ensure report.json is valid JSON",
        )];
    };
    let level = report
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let provider_rounds = report
        .pointer("/execution_trace/provider_rounds")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let scenarios = report
        .pointer("/next_gen_harness_closure/scenarios")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let scenario = |id: &str| {
        scenarios
            .iter()
            .find(|item| item.get("scenario_id").and_then(Value::as_str) == Some(id))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let team = scenario("team_agent_execution_outcome");
    let cross = scenario("cross_session_dispatch");
    let recovery = scenario("conflict_recovery");
    let tool_batch = scenario("tool_batch_efficiency");
    let mut checks = vec![
        check(
            "report_json_readable",
            true,
            true,
            report_path.display().to_string(),
            "pass a readable harness eval report.json path",
        ),
        check(
            "next_gen_terminal_scenarios_present",
            !scenarios.is_empty(),
            true,
            format!("scenarios={}", scenarios.len()),
            "run full/deep harness eval so next_gen_harness_closure scenarios are emitted",
        ),
        check(
            "team_agent_terminal_count",
            u64_at(&team, "/terminal_evidence/agent_terminal_count") >= 2,
            true,
            format!(
                "agent_terminal_count={}",
                u64_at(&team, "/terminal_evidence/agent_terminal_count")
            ),
            "emit terminal_evidence.agent_terminal_count >= 2 for team_agent_execution_outcome",
        ),
        check(
            "team_agent_mailbox_completed",
            u64_at(&team, "/terminal_evidence/mailbox_completed_count") >= 1,
            true,
            format!(
                "mailbox_completed_count={}",
                u64_at(&team, "/terminal_evidence/mailbox_completed_count")
            ),
            "complete at least one agent/session mailbox command in terminal evidence",
        ),
        check(
            "team_agent_synthesis_receipt",
            str_at(&team, "/terminal_evidence/synthesis_receipt_id")
                .is_some_and(|value| !value.trim().is_empty()),
            true,
            format!(
                "synthesis_receipt_id={}",
                str_at(&team, "/terminal_evidence/synthesis_receipt_id").unwrap_or("-")
            ),
            "emit a synthesis receipt id for the team scenario",
        ),
        check(
            "cross_session_relation_count",
            u64_at(&cross, "/terminal_evidence/session_relation_count") >= 1,
            true,
            format!(
                "session_relation_count={}",
                u64_at(&cross, "/terminal_evidence/session_relation_count")
            ),
            "emit at least one session relation for cross-session dispatch",
        ),
        check(
            "cross_session_runtime_turn_result",
            u64_at(&cross, "/terminal_evidence/runtime_turn_result_count") >= 1,
            true,
            format!(
                "runtime_turn_result_count={}",
                u64_at(&cross, "/terminal_evidence/runtime_turn_result_count")
            ),
            "emit at least one runtime turn/session command result for cross-session dispatch",
        ),
        check(
            "conflict_recovery_verified",
            u64_at(&recovery, "/terminal_evidence/recovery_applied_count")
                + u64_at(&recovery, "/terminal_evidence/recovery_verified_count")
                >= 1,
            true,
            format!(
                "applied={}, verified={}",
                u64_at(&recovery, "/terminal_evidence/recovery_applied_count"),
                u64_at(&recovery, "/terminal_evidence/recovery_verified_count")
            ),
            "emit recovery applied or verified evidence for conflict recovery",
        ),
    ];
    if level != "quick" {
        let calls = u64_at(&tool_batch, "/terminal_evidence/tool_calls")
            .max(u64_at(&tool_batch, "/tool_calls"));
        checks.push(check(
            "tool_batch_real_tool_calls",
            calls >= 2,
            true,
            format!("tool_calls={calls}"),
            "full/deep terminal gate requires at least two real local tool calls",
        ));
    }
    if level == "deep" {
        checks.push(check(
            "deep_real_provider_round",
            provider_rounds >= 1,
            true,
            format!("provider_rounds={provider_rounds}"),
            "deep terminal gate requires a real provider round",
        ));
    }
    checks
}

fn check(
    name: impl Into<String>,
    passed: bool,
    required: bool,
    evidence: impl Into<String>,
    repair_hint: impl Into<String>,
) -> Value {
    serde_json::json!({
        "name": name.into(),
        "status": if passed { "passed" } else { "failed" },
        "required": required,
        "evidence": evidence.into(),
        "repair_hint": repair_hint.into(),
    })
}

fn u64_at(value: &Value, pointer: &str) -> u64 {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .unwrap_or_default()
}

fn str_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_gate_writes_matrix_and_report() {
        let root =
            std::env::temp_dir().join(format!("cowd-terminal-gate-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("root");
        for version in 6..=12 {
            fs::write(root.join(format!("v{version}-evidence.md")), "ok").expect("evidence");
        }

        let report = terminal_gate_report(root.clone());
        assert_eq!(report["status"], "passed");
        assert!(root.join("terminal-capability-matrix.md").exists());
        assert!(root.join("terminal-gate-report.json").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_gate_rejects_missing_terminal_report_evidence() {
        let root = seeded_evidence_dir();
        let report_path = root.join("report.json");
        fs::write(
            &report_path,
            serde_json::to_string(&serde_json::json!({
                "level": "full",
                "execution_trace": {"provider_rounds": 0},
                "next_gen_harness_closure": {
                    "scenarios": [
                        {"scenario_id": "team_agent_execution_outcome", "terminal_evidence": {}},
                        {"scenario_id": "cross_session_dispatch", "terminal_evidence": {}},
                        {"scenario_id": "conflict_recovery", "terminal_evidence": {}},
                        {"scenario_id": "tool_batch_efficiency", "tool_calls": 0, "terminal_evidence": {}}
                    ]
                }
            }))
            .expect("json"),
        )
        .expect("report");

        let report = terminal_gate_report_with_report(root.clone(), Some(report_path));
        assert_eq!(report["status"], "failed");
        assert!(report["report_checks"]
            .as_array()
            .expect("checks")
            .iter()
            .any(|item| item["name"] == "team_agent_terminal_count" && item["status"] == "failed"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_gate_accepts_structured_terminal_report_evidence() {
        let root = seeded_evidence_dir();
        let report_path = root.join("report.json");
        fs::write(
            &report_path,
            serde_json::to_string(&serde_json::json!({
                "level": "full",
                "execution_trace": {"provider_rounds": 0},
                "next_gen_harness_closure": {
                    "scenarios": [
                        {
                            "scenario_id": "team_agent_execution_outcome",
                            "terminal_evidence": {
                                "agent_terminal_count": 2,
                                "mailbox_completed_count": 1,
                                "synthesis_receipt_id": "synthesis:demo"
                            }
                        },
                        {
                            "scenario_id": "cross_session_dispatch",
                            "terminal_evidence": {
                                "session_relation_count": 1,
                                "runtime_turn_result_count": 1
                            }
                        },
                        {
                            "scenario_id": "conflict_recovery",
                            "terminal_evidence": {
                                "recovery_applied_count": 0,
                                "recovery_verified_count": 1
                            }
                        },
                        {
                            "scenario_id": "tool_batch_efficiency",
                            "tool_calls": 3,
                            "terminal_evidence": {"tool_calls": 3}
                        }
                    ]
                }
            }))
            .expect("json"),
        )
        .expect("report");

        let report = terminal_gate_report_with_report(root.clone(), Some(report_path));
        assert_eq!(report["status"], "passed");
        assert_eq!(report["failed_report_checks"], 0);
        let _ = fs::remove_dir_all(root);
    }

    fn seeded_evidence_dir() -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowd-terminal-gate-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("root");
        for version in 6..=12 {
            fs::write(root.join(format!("v{version}-evidence.md")), "ok").expect("evidence");
        }
        root
    }
}
