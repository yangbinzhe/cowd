use serde_json::Value;
use std::{fs, path::PathBuf};

use crate::terminal_matrix::{
    render_terminal_capability_matrix_markdown, terminal_capability_matrix,
};

#[must_use]
pub fn terminal_gate_report(evidence_dir: PathBuf) -> Value {
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
    let matrix_markdown = render_terminal_capability_matrix_markdown(&matrix);
    let matrix_path = evidence_dir.join("terminal-capability-matrix.md");
    let report_path = evidence_dir.join("terminal-gate-report.json");
    let _ = fs::create_dir_all(&evidence_dir);
    let _ = fs::write(&matrix_path, matrix_markdown);
    let report = serde_json::json!({
        "kind": "harness_eval.terminal_gate",
        "status": if missing == 0 && failed_matrix_rows == 0 { "passed" } else { "failed" },
        "evidence_dir": evidence_dir.display().to_string(),
        "expected": expected,
        "missing": missing,
        "files": files,
        "matrix_rows": matrix,
        "failed_matrix_rows": failed_matrix_rows,
        "matrix_path": matrix_path.display().to_string(),
        "report_path": report_path.display().to_string(),
    });
    if let Ok(text) = serde_json::to_string_pretty(&report) {
        let _ = fs::write(&report_path, text);
    }
    report
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
}
