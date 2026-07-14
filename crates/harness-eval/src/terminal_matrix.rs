use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalCapabilityMatrixRow {
    pub capability: String,
    pub owner: String,
    pub source_entry: String,
    pub gateway_api: String,
    pub runtime_event: String,
    pub frontend_entry: String,
    pub eval_evidence: String,
    pub forbidden_scan: String,
    pub status: String,
}

#[must_use]
pub fn terminal_capability_matrix(evidence_dir: &Path) -> Vec<TerminalCapabilityMatrixRow> {
    let evidence = |version: u8| {
        let path = evidence_dir.join(format!("v{version}-evidence.md"));
        if path.exists() {
            path.display().to_string()
        } else {
            "missing".to_string()
        }
    };
    let rows = vec![
        row(
            "模型主动性",
            "Runtime",
            "capability_manifest + orchestration",
            "/api/runtime/control-plane",
            "runtime.orchestration",
            "WebUI/TUI capability surfaces",
            evidence(6),
        ),
        row(
            "Tool DAG 真实执行",
            "Runtime",
            "execution_core/tool_dag.rs",
            "/api/tools/*",
            "runtime.tool_dag.executed",
            "Tools page + TUI client",
            evidence(6),
        ),
        row(
            "多 Agent 协作",
            "Runtime",
            "agent/team modules",
            "/api/mission/control/teams",
            "agent outcome bridge",
            "Mission page + TUI",
            evidence(7),
        ),
        row(
            "跨 Session",
            "Runtime",
            "session_execution + mission_command_interpreter",
            "/api/mission/sessions/*",
            "cross_session.bridge",
            "Mission Control",
            evidence(8),
        ),
        row(
            "Reality / Memory",
            "Reality Core",
            "context/reality_decision.rs",
            "/api/reality/*",
            "reality.runtime_decision",
            "Reality/Memory pages + TUI",
            evidence(9),
        ),
        row(
            "Recovery",
            "Runtime",
            "recovery/runtime_event_replay.rs",
            "/api/runtime/recovery",
            "recovery_required",
            "Runtime page + TUI",
            evidence(8),
        ),
        row(
            "WebUI/TUI",
            "Surface",
            "AuditPage + gateway_client",
            "/api/harness-eval/* /api/evolution/*",
            "surface.control",
            "Audit page + TUI client",
            evidence(11),
        ),
        row(
            "Harness Eval",
            "Harness Eval",
            "runner/report_store/evolution",
            "/api/harness-eval/*",
            "harness_eval.execution_trace",
            "Audit page",
            evidence(10),
        ),
        row(
            "自我进化",
            "Runtime + Skill + Harness Eval",
            "runtime/evolution + skill_service",
            "/api/evolution/*",
            "evolution signal/proposal/sandbox",
            "Audit page + TUI client",
            evidence(12),
        ),
        row(
            "依赖边界",
            "Workspace",
            "Cargo graph",
            "service contracts",
            "forbidden scan",
            "Terminal gate",
            evidence(12),
        ),
    ];
    rows.into_iter()
        .map(|mut row| {
            row.status = if row.eval_evidence == "missing" {
                "failed".to_string()
            } else {
                "passed".to_string()
            };
            row
        })
        .collect()
}

#[must_use]
pub fn render_terminal_capability_matrix_markdown(rows: &[TerminalCapabilityMatrixRow]) -> String {
    let mut output = String::from(
        "# Terminal Capability Matrix\n\n| 终局能力 | owner | 源码入口 | Gateway/API | Runtime event | 前端入口 | 评测证据 | 禁止残留扫描 | 结论 |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n",
    );
    for row in rows {
        output.push_str(&format!(
            "| {} | {} | `{}` | `{}` | `{}` | {} | `{}` | {} | {} |\n",
            row.capability,
            row.owner,
            row.source_entry,
            row.gateway_api,
            row.runtime_event,
            row.frontend_entry,
            row.eval_evidence,
            row.forbidden_scan,
            row.status
        ));
    }
    output
}

fn row(
    capability: impl Into<String>,
    owner: impl Into<String>,
    source_entry: impl Into<String>,
    gateway_api: impl Into<String>,
    runtime_event: impl Into<String>,
    frontend_entry: impl Into<String>,
    eval_evidence: impl Into<String>,
) -> TerminalCapabilityMatrixRow {
    TerminalCapabilityMatrixRow {
        capability: capability.into(),
        owner: owner.into(),
        source_entry: source_entry.into(),
        gateway_api: gateway_api.into(),
        runtime_event: runtime_event.into(),
        frontend_entry: frontend_entry.into(),
        eval_evidence: eval_evidence.into(),
        forbidden_scan: "classified".to_string(),
        status: "pending".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_matrix_renders_required_capabilities() {
        let rows = terminal_capability_matrix(Path::new("/tmp/nonexistent-terminal-matrix"));
        assert_eq!(rows.len(), 10);
        assert!(rows.iter().any(|row| row.capability == "自我进化"));
        let markdown = render_terminal_capability_matrix_markdown(&rows);
        assert!(markdown.contains("终局能力"));
        assert!(markdown.contains("Tool DAG"));
    }
}
