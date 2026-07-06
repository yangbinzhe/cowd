use std::path::Path;

use harness_eval::{
    default_report_root, run_eval, HarnessEvalLevel, HarnessEvalReportStore, HarnessEvalRunRecord,
    HarnessEvalRunRequest, HarnessEvalRunnerOptions,
};
use serde_json::{json, Value};

use super::{service_envelope, HarnessEvalService, ServiceEnvelope};

#[derive(Debug)]
pub(crate) enum HarnessEvalServiceError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl HarnessEvalServiceError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::BadRequest(message) | Self::NotFound(message) | Self::Internal(message) => {
                message.clone()
            }
        }
    }
}

impl HarnessEvalService {
    pub(crate) fn new() -> Self {
        Self {
            label: "harness_eval",
            owner: "0.9.412 Harness Eval service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn reports(
        &self,
        config_home: &Path,
        config: Option<&Value>,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        let reports = store
            .list_reports()
            .map_err(HarnessEvalServiceError::Internal)?;
        Ok(json!({
            "kind": "harness_eval.reports",
            "envelope": self.envelope("reports"),
            "root": store.root().display().to_string(),
            "reports": reports,
            "count": reports.len(),
        }))
    }

    pub(crate) fn latest_report(
        &self,
        config_home: &Path,
        config: Option<&Value>,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        let latest = store
            .latest_report()
            .map_err(HarnessEvalServiceError::Internal)?;
        Ok(json!({
            "kind": "harness_eval.latest_report",
            "envelope": self.envelope("latest_report"),
            "root": store.root().display().to_string(),
            "report": latest,
            "status": latest.as_ref().map(|item| item.status.as_str()).unwrap_or("empty"),
        }))
    }

    pub(crate) fn report_detail(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        let detail = store
            .get_report(id)
            .map_err(HarnessEvalServiceError::Internal)?
            .ok_or_else(|| {
                HarnessEvalServiceError::NotFound("harness eval report not found".to_string())
            })?;
        Ok(json!({
            "kind": "harness_eval.report_detail",
            "envelope": self.envelope("report_detail"),
            "detail": detail,
        }))
    }

    pub(crate) fn report_artifacts(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<Value, HarnessEvalServiceError> {
        let detail = self.report_detail_model(config_home, config, id)?;
        let count = detail.artifacts.len();
        Ok(json!({
            "kind": "harness_eval.artifacts",
            "envelope": self.envelope("artifacts"),
            "report_id": id,
            "summary": detail.summary,
            "artifacts": detail.artifacts,
            "count": count,
        }))
    }

    pub(crate) fn report_gate(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<Value, HarnessEvalServiceError> {
        let detail = self.report_detail_model(config_home, config, id)?;
        let report_gate = detail
            .report
            .get("report_gate")
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({
            "kind": "harness_eval.report_gate",
            "envelope": self.envelope("report_gate"),
            "report_id": id,
            "summary": detail.summary,
            "report_gate": report_gate,
        }))
    }

    pub(crate) fn scenarios(&self) -> Value {
        json!({
            "kind": "harness_eval.scenarios",
            "envelope": self.envelope("scenarios"),
            "scenarios": harness_eval::stable_ai_scenario_matrix(),
            "next_gen_harness_closure": harness_eval::next_gen_harness_closure_specs(),
        })
    }

    pub(crate) fn runs(
        &self,
        config_home: &Path,
        config: Option<&Value>,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        let runs = store
            .list_runs()
            .map_err(HarnessEvalServiceError::Internal)?;
        Ok(json!({
            "kind": "harness_eval.runs",
            "envelope": self.envelope("runs"),
            "root": store.root().display().to_string(),
            "runs": runs,
            "count": runs.len(),
        }))
    }

    pub(crate) fn run_detail(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        let run = store
            .get_run(id)
            .map_err(HarnessEvalServiceError::Internal)?
            .ok_or_else(|| {
                HarnessEvalServiceError::NotFound("harness eval run not found".to_string())
            })?;
        Ok(json!({
            "kind": "harness_eval.run_detail",
            "envelope": self.envelope("run_detail"),
            "run": run,
        }))
    }

    pub(crate) fn start_run(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        request: HarnessEvalRunRequest,
    ) -> Result<Value, HarnessEvalServiceError> {
        let level = request.level;
        if level == HarnessEvalLevel::Deep && !request.allow_real_model {
            let store = self.store(config_home, config);
            let record = run_eval(
                &store,
                HarnessEvalRunnerOptions {
                    level,
                    provider: request.provider,
                    budget: request.budget,
                    allow_real_model: false,
                },
            )
            .map_err(HarnessEvalServiceError::Internal)?;
            return Ok(run_response(self.envelope("run_start"), record));
        }
        let store = self.store(config_home, config);
        let record = run_eval(
            &store,
            HarnessEvalRunnerOptions {
                level,
                provider: request.provider,
                budget: request.budget.or_else(|| Some("low".to_string())),
                allow_real_model: request.allow_real_model,
            },
        )
        .map_err(HarnessEvalServiceError::Internal)?;
        Ok(run_response(self.envelope("run_start"), record))
    }

    pub(crate) fn cancel_run(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        let Some(run) = store
            .get_run(id)
            .map_err(HarnessEvalServiceError::Internal)?
        else {
            return Err(HarnessEvalServiceError::NotFound(
                "harness eval run not found".to_string(),
            ));
        };
        Ok(json!({
            "kind": "harness_eval.run_cancel",
            "envelope": self.envelope("run_cancel"),
            "ok": false,
            "run_id": run.run_id,
            "status": run.status,
            "message": "no cancellable background harness eval task is active; Gateway smoke runs complete synchronously",
        }))
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.envelope("reports"),
            self.envelope("latest_report"),
            self.envelope("report_detail"),
            self.envelope("artifacts"),
            self.envelope("report_gate"),
            self.envelope("scenarios"),
            self.envelope("runs"),
            self.envelope("run_detail"),
            self.envelope("run_start"),
            self.envelope("run_cancel"),
        ]
    }

    fn store(&self, config_home: &Path, config: Option<&Value>) -> HarnessEvalReportStore {
        HarnessEvalReportStore::new(report_root(config_home, config))
    }

    fn report_detail_model(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<harness_eval::HarnessEvalReportDetail, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        store
            .get_report(id)
            .map_err(HarnessEvalServiceError::Internal)?
            .ok_or_else(|| {
                HarnessEvalServiceError::NotFound("harness eval report not found".to_string())
            })
    }
}

fn run_response(envelope: ServiceEnvelope, record: HarnessEvalRunRecord) -> Value {
    json!({
        "kind": "harness_eval.run",
        "envelope": envelope,
        "ok": record.status == "completed",
        "run": record,
    })
}

fn report_root(config_home: &Path, config: Option<&Value>) -> std::path::PathBuf {
    config
        .and_then(|value| value.get("eval_report_dir"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| default_report_root(config_home))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn harness_eval_service_runs_smoke_and_gates_deep() {
        let service = HarnessEvalService::new();
        let config_home = std::env::temp_dir().join(format!(
            "cowd-gateway-harness-eval-{}",
            uuid::Uuid::new_v4()
        ));
        let smoke = service
            .start_run(
                &config_home,
                None,
                HarnessEvalRunRequest {
                    level: HarnessEvalLevel::Quick,
                    provider: None,
                    budget: Some("low".to_string()),
                    allow_real_model: false,
                    actor: Some("test".to_string()),
                    objective: Some("smoke".to_string()),
                },
            )
            .expect("smoke");
        assert_eq!(smoke["run"]["status"], "completed");
        let reports = service.reports(&config_home, None).expect("reports");
        assert_eq!(reports["count"], 1);
        let gated = service
            .start_run(
                &config_home,
                None,
                HarnessEvalRunRequest {
                    level: HarnessEvalLevel::Deep,
                    provider: Some("configured".to_string()),
                    budget: Some("low".to_string()),
                    allow_real_model: false,
                    actor: None,
                    objective: None,
                },
            )
            .expect("gated");
        assert_eq!(gated["run"]["status"], "gated");
        let deep_real = service
            .start_run(
                &config_home,
                None,
                HarnessEvalRunRequest {
                    level: HarnessEvalLevel::Deep,
                    provider: Some("configured".to_string()),
                    budget: Some("full".to_string()),
                    allow_real_model: true,
                    actor: None,
                    objective: Some("deep real should be delegated to harness-eval".to_string()),
                },
            )
            .expect("deep real delegated");
        assert_eq!(deep_real["kind"], "harness_eval.run");
        assert_ne!(deep_real["run"]["status"], "gated");
        assert!(deep_real["run"]["report_path"].is_string());
        let _ = std::fs::remove_dir_all(config_home);
    }
}
