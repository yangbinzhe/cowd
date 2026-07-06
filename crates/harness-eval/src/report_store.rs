use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use crate::{
    evaluate_report_gate,
    report::{
        HarnessEvalReportDetail, HarnessEvalReportSummary, HarnessEvalRunRecord,
        HarnessEvalUsageSummary,
    },
    StableAiHealthReport,
};

const FULL_ANALYSIS_REPORT_TEMPLATE: &str =
    include_str!("../templates/full-analysis-report-template.md");
const FULL_ANALYSIS_REPORT_PROMPT: &str =
    include_str!("../templates/full-analysis-report-prompt.md");

#[derive(Debug, Clone)]
pub struct HarnessEvalReportStore {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessEvalStoredReport {
    pub summary: HarnessEvalReportSummary,
    pub json_path: PathBuf,
    pub markdown_path: PathBuf,
}

impl HarnessEvalReportStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn ensure_root(&self) -> Result<(), String> {
        fs::create_dir_all(&self.root).map_err(|error| error.to_string())
    }

    pub fn list_reports(&self) -> Result<Vec<HarnessEvalReportSummary>, String> {
        let mut reports = self
            .scan_report_files()?
            .into_iter()
            .map(|item| item.summary)
            .collect::<Vec<_>>();
        reports.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(reports)
    }

    pub fn latest_report(&self) -> Result<Option<HarnessEvalReportSummary>, String> {
        Ok(self.list_reports()?.into_iter().next())
    }

    pub fn get_report(&self, id: &str) -> Result<Option<HarnessEvalReportDetail>, String> {
        let Some(stored) = self
            .scan_report_files()?
            .into_iter()
            .find(|item| item.summary.id == id)
        else {
            return Ok(None);
        };
        let text = fs::read_to_string(&stored.json_path).map_err(|error| error.to_string())?;
        let report = serde_json::from_str::<Value>(&text).map_err(|error| error.to_string())?;
        let artifacts = stored
            .summary
            .result_package_dir
            .as_deref()
            .map(|dir| artifact_paths(Path::new(dir)))
            .unwrap_or_default();
        Ok(Some(HarnessEvalReportDetail {
            summary: stored.summary,
            report,
            artifacts,
        }))
    }

    pub fn write_report(
        &self,
        level: &str,
        report: &mut Value,
        stable_ai: &StableAiHealthReport,
    ) -> Result<HarnessEvalReportSummary, String> {
        self.ensure_root()?;
        let stamp = current_stamp();
        let base = format!("{stamp}-mission-harness-{level}");
        let run_dir = self.root.join("runs").join(&base);
        let evidence_dir = run_dir.join("evidence");
        let requests_dir = run_dir.join("requests");
        let responses_dir = run_dir.join("responses");
        let events_dir = run_dir.join("events");
        let run_evidence_dir = run_dir.join("run-evidence");
        let provider_rounds_dir = run_dir.join("provider-rounds");
        let tool_calls_dir = run_dir.join("tool-calls");
        let model_speed_dir = run_dir.join("model-speed");
        let quality_rubric_dir = run_dir.join("quality-rubric");
        fs::create_dir_all(&evidence_dir).map_err(|error| error.to_string())?;
        for dir in [
            &requests_dir,
            &responses_dir,
            &events_dir,
            &run_evidence_dir,
            &provider_rounds_dir,
            &tool_calls_dir,
            &model_speed_dir,
            &quality_rubric_dir,
        ] {
            fs::create_dir_all(dir).map_err(|error| error.to_string())?;
        }
        report["result_package_dir"] = Value::String(run_dir.display().to_string());
        if !report
            .get("evidence_manifest")
            .is_some_and(Value::is_object)
        {
            report["evidence_manifest"] = serde_json::json!({
                "kind": "harness_eval.evidence_manifest"
            });
        }
        report["evidence_manifest"]["report_id"] = Value::String(base.clone());
        report["evidence_manifest"]["result_package_dir"] =
            Value::String(run_dir.display().to_string());
        report["report_package"] = serde_json::json!({
            "status": "written",
            "root": run_dir.display().to_string(),
            "required_dirs": ["requests", "responses", "events", "run-evidence", "provider-rounds", "tool-calls", "model-speed", "quality-rubric", "evidence"],
            "summary": "summary.md",
            "full_report": "full-report.md",
            "full_analysis_report": "full-analysis-report.md",
            "full_analysis_template": "full-analysis-report-template.md",
            "full_analysis_prompt": "full-analysis-report-prompt.md",
            "analysis_context": "analysis-context.json",
            "evidence_manifest": "evidence/evidence-manifest.json",
            "quality_rubric": "quality-rubric/quality-rubric.json"
        });
        report["report_gate"] = serde_json::to_value(evaluate_report_gate(report))
            .map_err(|error| error.to_string())?;
        let json_path = run_dir.join("report.json");
        let md_path = run_dir.join("report.md");
        let markdown = render_markdown_report(report);
        fs::write(
            &json_path,
            serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(&md_path, &markdown).map_err(|error| error.to_string())?;
        fs::write(run_dir.join("summary.md"), render_summary_report(report))
            .map_err(|error| error.to_string())?;
        fs::write(run_dir.join("full-report.md"), &markdown).map_err(|error| error.to_string())?;
        fs::write(
            run_dir.join("full-analysis-report-template.md"),
            FULL_ANALYSIS_REPORT_TEMPLATE,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            run_dir.join("full-analysis-report-prompt.md"),
            FULL_ANALYSIS_REPORT_PROMPT,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            run_dir.join("full-analysis-report.md"),
            render_full_analysis_placeholder(report),
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            run_dir.join("analysis-context.json"),
            serde_json::to_string_pretty(&analysis_context(report))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            run_dir.join("execution-trace.json"),
            serde_json::to_string_pretty(report.get("execution_trace").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        write_provider_round_artifacts(report, &provider_rounds_dir)?;
        fs::write(
            events_dir.join("runtime-actions.json"),
            serde_json::to_string_pretty(
                report
                    .get("execution_trace")
                    .and_then(|trace| trace.get("runtime_action_log"))
                    .unwrap_or(&Value::Null),
            )
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            model_speed_dir.join("summary.json"),
            serde_json::to_string_pretty(
                report
                    .get("execution_trace")
                    .and_then(|trace| trace.get("total_usage"))
                    .unwrap_or(&Value::Null),
            )
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            quality_rubric_dir.join("quality-rubric.json"),
            serde_json::to_string_pretty(report.get("report_gate").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        write_tool_call_artifacts(
            report,
            &requests_dir,
            &responses_dir,
            &events_dir,
            &run_evidence_dir,
            &tool_calls_dir,
        )?;
        fs::write(
            evidence_dir.join("stable-ai-health.json"),
            serde_json::to_string_pretty(stable_ai).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            evidence_dir.join("reality-context-eval.json"),
            serde_json::to_string_pretty(
                report.get("reality_context_eval").unwrap_or(&Value::Null),
            )
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            evidence_dir.join("next-gen-harness-closure.json"),
            serde_json::to_string_pretty(
                report
                    .get("next_gen_harness_closure")
                    .unwrap_or(&Value::Null),
            )
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            evidence_dir.join("complex-scenarios.json"),
            serde_json::to_string_pretty(report.get("complex_scenarios").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            evidence_dir.join("real-tool-scenarios.json"),
            serde_json::to_string_pretty(report.get("real_tool_scenarios").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            evidence_dir.join("evidence-manifest.json"),
            serde_json::to_string_pretty(report.get("evidence_manifest").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::copy(&json_path, self.root.join(format!("{base}.json")))
            .map_err(|error| error.to_string())?;
        fs::copy(&md_path, self.root.join(format!("{base}.md")))
            .map_err(|error| error.to_string())?;
        write_stable_ai_health(&self.root, stable_ai)?;
        Ok(HarnessEvalReportSummary::from_report_json(
            base,
            json_path.display().to_string(),
            Some(md_path.display().to_string()),
            report,
        ))
    }

    pub fn append_run(&self, record: &HarnessEvalRunRecord) -> Result<(), String> {
        self.ensure_root()?;
        let path = self.root.join("runs.jsonl");
        let mut line = serde_json::to_string(record).map_err(|error| error.to_string())?;
        line.push('\n');
        let mut options = fs::OpenOptions::new();
        options.create(true).append(true);
        use std::io::Write;
        options
            .open(path)
            .and_then(|mut file| file.write_all(line.as_bytes()))
            .map_err(|error| error.to_string())
    }

    pub fn list_runs(&self) -> Result<Vec<HarnessEvalRunRecord>, String> {
        let path = self.root.join("runs.jsonl");
        if !path.exists() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        let mut records = text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str::<HarnessEvalRunRecord>(line).ok())
            .collect::<Vec<_>>();
        records.sort_by(|left, right| right.requested_at_ms.cmp(&left.requested_at_ms));
        Ok(records)
    }

    pub fn get_run(&self, id: &str) -> Result<Option<HarnessEvalRunRecord>, String> {
        Ok(self.list_runs()?.into_iter().find(|run| run.run_id == id))
    }

    fn scan_report_files(&self) -> Result<Vec<HarnessEvalStoredReport>, String> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut reports = Vec::new();
        collect_report_files(&self.root, &mut reports)?;
        reports.sort_by(|left, right| right.summary.created_at_ms.cmp(&left.summary.created_at_ms));
        Ok(reports)
    }
}

#[must_use]
pub fn default_report_root(config_home: impl AsRef<Path>) -> PathBuf {
    std::env::var_os("COWD_AI_HARNESS_REPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| config_home.as_ref().join("harness-eval").join("reports"))
}

#[must_use]
pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[must_use]
pub fn empty_usage(source: &str) -> HarnessEvalUsageSummary {
    HarnessEvalUsageSummary {
        usage_source: source.to_string(),
        ..HarnessEvalUsageSummary::default()
    }
}

fn collect_report_files(
    root: &Path,
    reports: &mut Vec<HarnessEvalStoredReport>,
) -> Result<(), String> {
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            collect_report_files(&path, reports)?;
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) != Some("report.json") {
            continue;
        }
        let text = fs::read_to_string(&path).map_err(|error| error.to_string())?;
        let report = serde_json::from_str::<Value>(&text).map_err(|error| error.to_string())?;
        let id = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            .unwrap_or("report")
            .to_string();
        let markdown = path.with_file_name("report.md");
        reports.push(HarnessEvalStoredReport {
            summary: HarnessEvalReportSummary::from_report_json(
                id,
                path.display().to_string(),
                markdown.exists().then(|| markdown.display().to_string()),
                &report,
            ),
            json_path: path,
            markdown_path: markdown,
        });
    }
    Ok(())
}

fn artifact_paths(root: &Path) -> Vec<String> {
    let mut artifacts = Vec::new();
    collect_artifacts(root, &mut artifacts);
    artifacts.sort();
    artifacts
}

fn collect_artifacts(root: &Path, artifacts: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_artifacts(&path, artifacts);
        } else {
            artifacts.push(path.display().to_string());
        }
    }
}

fn current_stamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("v{}-{seconds}", env!("CARGO_PKG_VERSION"))
}

fn render_markdown_report(report: &Value) -> String {
    let level = report
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let status = report
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let trace = report.get("execution_trace").unwrap_or(&Value::Null);
    let mut markdown =
        format!("# Mission Harness Evaluation Report\n\n- level: {level}\n- status: {status}\n");
    markdown.push_str(&format!(
        "- total_elapsed_ms: {}\n- provider_rounds: {}\n- runtime_actions: {}\n- tool_calls: {}\n- total_tokens: {}\n\n",
        trace.get("total_elapsed_ms").and_then(Value::as_u64).unwrap_or_default(),
        trace.get("provider_rounds").and_then(Value::as_u64).unwrap_or_default(),
        trace.get("runtime_actions").and_then(Value::as_u64).unwrap_or_default(),
        trace.get("tool_calls").and_then(Value::as_u64).unwrap_or_default(),
        trace.get("total_usage").and_then(|value| value.get("total_tokens")).and_then(Value::as_u64).unwrap_or_default(),
    ));
    markdown.push_str("| Capability | Status | Evidence |\n| --- | --- | --- |\n");
    for item in report
        .get("scenarios")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            item.get("capability")
                .and_then(Value::as_str)
                .unwrap_or("-"),
            item.get("status").and_then(Value::as_str).unwrap_or("-"),
            item.get("evidence")
                .and_then(Value::as_str)
                .unwrap_or("-")
                .replace('|', "\\|"),
        ));
    }
    if let Some(gate) = report.get("report_gate") {
        markdown.push_str("\n## Report Gate\n\n");
        markdown.push_str(&format!(
            "- status: {}\n- passed: {}\n- failed: {}\n\n",
            gate.get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            gate.get("passed")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            gate.get("failed")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        ));
        markdown.push_str("| Gate | Status | Evidence |\n| --- | --- | --- |\n");
        for item in gate
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            markdown.push_str(&format!(
                "| {} | {} | {} |\n",
                item.get("name").and_then(Value::as_str).unwrap_or("-"),
                item.get("status").and_then(Value::as_str).unwrap_or("-"),
                item.get("evidence")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
                    .replace('|', "\\|"),
            ));
        }
    }
    if let Some(reality) = report.get("reality_context_eval") {
        markdown.push_str("\n## Reality Context Eval\n\n");
        markdown.push_str(&format!(
            "- total: {}\n- passed: {}\n- failed: {}\n- selected_context_total: {}\n- omitted_context_total: {}\n- evidence_ref_total: {}\n- detail: `evidence/reality-context-eval.json`\n\n",
            reality.get("total").and_then(Value::as_u64).unwrap_or_default(),
            reality.get("passed").and_then(Value::as_u64).unwrap_or_default(),
            reality.get("failed").and_then(Value::as_u64).unwrap_or_default(),
            reality.get("selected_context_total").and_then(Value::as_u64).unwrap_or_default(),
            reality.get("omitted_context_total").and_then(Value::as_u64).unwrap_or_default(),
            reality.get("evidence_ref_total").and_then(Value::as_u64).unwrap_or_default(),
        ));
        markdown.push_str("| Scenario | Status | Selected | Omitted | Evidence |\n| --- | --- | ---: | ---: | ---: |\n");
        for scenario in reality
            .get("scenarios")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                scenario
                    .get("scenario_id")
                    .and_then(Value::as_str)
                    .unwrap_or("-"),
                scenario
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("-"),
                scenario
                    .get("selected_context_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                scenario
                    .get("omitted_context_count")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                scenario
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
            ));
        }
    }
    if let Some(next_gen) = report.get("next_gen_harness_closure") {
        markdown.push_str("\n## Next Gen Harness Closure\n\n");
        markdown.push_str(&format!(
            "- status: {}\n- total: {}\n- passed: {}\n- failed: {}\n- detail: `evidence/next-gen-harness-closure.json`\n\n",
            next_gen
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            next_gen
                .get("total")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            next_gen
                .get("passed")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            next_gen
                .get("failed")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        ));
        markdown.push_str("| Scenario | Status | Runtime Actions | Tool Calls | Evidence |\n| --- | --- | ---: | ---: | ---: |\n");
        for scenario in next_gen
            .get("scenarios")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
        {
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                scenario
                    .get("scenario_id")
                    .and_then(Value::as_str)
                    .unwrap_or("-"),
                scenario
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("-"),
                scenario
                    .get("runtime_actions")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
                scenario
                    .get("tool_calls")
                    .and_then(Value::as_u64)
                    .unwrap_or_default(),
                scenario
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
            ));
        }
    }
    if let Some(manifest) = report.get("evidence_manifest") {
        markdown.push_str("\n## Evidence Manifest\n\n");
        markdown.push_str(&format!(
            "- repo: `{}`\n- commit: `{}`\n- version: `{}`\n- command: `{}`\n- detail: `evidence/evidence-manifest.json`\n",
            manifest
                .get("repo")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            manifest
                .get("commit")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            manifest
                .get("version")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            manifest
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        ));
    }
    markdown
}

fn render_summary_report(report: &Value) -> String {
    let level = report
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let status = report
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let gate = report.get("report_gate").unwrap_or(&Value::Null);
    format!(
        "# Harness Eval Summary\n\n- level: {level}\n- status: {status}\n- gate: {}\n- failed_gates: {}\n- report: `full-report.md`\n- trace: `execution-trace.json`\n",
        gate.get("status").and_then(Value::as_str).unwrap_or("unknown"),
        gate.get("failed").and_then(Value::as_u64).unwrap_or_default(),
    )
}

fn render_full_analysis_placeholder(report: &Value) -> String {
    let level = report
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let status = report
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    format!(
        "# Mission Harness {level} Full Analysis Report\n\nStatus: `{status}`.\n\nThis file is the AI-reviewer output target. Generate the final analysis by reading `full-analysis-report-prompt.md`, `full-analysis-report-template.md`, `analysis-context.json`, `report.json`, and the `evidence/` directory. The evaluator intentionally stores full evidence in JSON artifacts and keeps this file as the canonical human report target.\n"
    )
}

fn analysis_context(report: &Value) -> Value {
    serde_json::json!({
        "kind": "harness_eval.analysis_context",
        "level": report.get("level").cloned().unwrap_or(Value::Null),
        "status": report.get("status").cloned().unwrap_or(Value::Null),
        "report_gate": report.get("report_gate").cloned().unwrap_or(Value::Null),
        "execution_trace": report.get("execution_trace").cloned().unwrap_or(Value::Null),
        "evidence_manifest": report.get("evidence_manifest").cloned().unwrap_or(Value::Null),
        "scenario_capabilities": report.get("scenarios").cloned().unwrap_or(Value::Null),
        "next_gen_harness_closure": report.get("next_gen_harness_closure").cloned().unwrap_or(Value::Null),
        "reality_context_eval": report.get("reality_context_eval").cloned().unwrap_or(Value::Null),
        "mission_runtime_collaboration": report.get("mission_runtime_collaboration").cloned().unwrap_or(Value::Null),
        "real_tool_scenarios": report.get("real_tool_scenarios").cloned().unwrap_or(Value::Null),
        "instructions": [
            "Use summaries in the human report and cite JSON artifact paths for details.",
            "Distinguish deterministic contract checks, real local tool execution, and real provider rounds.",
            "Do not claim a capability proven unless report_gate and scenario evidence support it."
        ]
    })
}

fn write_provider_round_artifacts(
    report: &Value,
    provider_rounds_dir: &Path,
) -> Result<(), String> {
    let Some(rounds) = report
        .get("execution_trace")
        .and_then(|trace| trace.get("rounds"))
        .and_then(Value::as_array)
    else {
        return Ok(());
    };
    for (index, round) in rounds.iter().enumerate() {
        fs::write(
            provider_rounds_dir.join(format!("{:03}-round.json", index + 1)),
            serde_json::to_string_pretty(round).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn write_tool_call_artifacts(
    report: &Value,
    requests_dir: &Path,
    responses_dir: &Path,
    events_dir: &Path,
    run_evidence_dir: &Path,
    tool_calls_dir: &Path,
) -> Result<(), String> {
    let Some(details) = report.get("tool_call_details").and_then(Value::as_array) else {
        return Ok(());
    };
    for detail in details {
        let index = detail
            .get("summary")
            .and_then(|summary| summary.get("call_index"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        fs::write(
            requests_dir.join(format!("tool-call-{index}.json")),
            serde_json::to_string_pretty(detail.get("input").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            responses_dir.join(format!("tool-call-{index}.json")),
            serde_json::to_string_pretty(&serde_json::json!({
                "output": detail.get("output").unwrap_or(&Value::Null),
                "error": detail.get("error").unwrap_or(&Value::Null)
            }))
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            events_dir.join(format!("tool-call-{index}.json")),
            serde_json::to_string_pretty(detail.get("summary").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            run_evidence_dir.join(format!("tool-call-{index}.json")),
            serde_json::to_string_pretty(detail).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            tool_calls_dir.join(format!("tool-call-{index}.json")),
            serde_json::to_string_pretty(detail).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn write_stable_ai_health(root: &Path, report: &StableAiHealthReport) -> Result<(), String> {
    let version = env!("CARGO_PKG_VERSION");
    fs::write(
        root.join(format!("stable-ai-health-report-v{version}.json")),
        serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::{run_eval, HarnessEvalRunnerOptions};

    #[test]
    fn report_store_writes_lists_and_reads_smoke_report() {
        let root =
            std::env::temp_dir().join(format!("cowd-harness-eval-store-{}", uuid::Uuid::new_v4()));
        let store = HarnessEvalReportStore::new(&root);
        let record = run_eval(
            &store,
            HarnessEvalRunnerOptions {
                level: crate::HarnessEvalLevel::Quick,
                provider: None,
                budget: Some("low".to_string()),
                allow_real_model: false,
            },
        )
        .expect("run eval");
        assert_eq!(record.status, "completed");
        let reports = store.list_reports().expect("reports");
        assert_eq!(reports.len(), 1);
        assert!(reports[0].total_elapsed_ms.is_some());
        let detail = store
            .get_report(&reports[0].id)
            .expect("detail")
            .expect("report exists");
        assert_eq!(detail.summary.id, reports[0].id);
        assert!(!detail.artifacts.is_empty());
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("summary.md")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("full-report.md")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("full-analysis-report-template.md")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("full-analysis-report-prompt.md")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("analysis-context.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("quality-rubric.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/reality-context-eval.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/next-gen-harness-closure.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/evidence-manifest.json")));
        let _ = fs::remove_dir_all(root);
    }
}
