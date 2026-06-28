use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use crate::{
    report::{
        HarnessEvalReportDetail, HarnessEvalReportSummary, HarnessEvalRunRecord,
        HarnessEvalUsageSummary,
    },
    StableAiHealthReport,
};

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
        fs::create_dir_all(&evidence_dir).map_err(|error| error.to_string())?;
        report["result_package_dir"] = Value::String(run_dir.display().to_string());
        let json_path = run_dir.join("report.json");
        let md_path = run_dir.join("report.md");
        fs::write(
            &json_path,
            serde_json::to_string_pretty(report).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(&md_path, render_markdown_report(report)).map_err(|error| error.to_string())?;
        fs::write(
            run_dir.join("execution-trace.json"),
            serde_json::to_string_pretty(report.get("execution_trace").unwrap_or(&Value::Null))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        fs::write(
            evidence_dir.join("stable-ai-health.json"),
            serde_json::to_string_pretty(stable_ai).map_err(|error| error.to_string())?,
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
    markdown
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
        let _ = fs::remove_dir_all(root);
    }
}
