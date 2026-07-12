use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
};

use harness_eval::{
    default_report_root, now_ms, run_eval, run_eval_controlled, HarnessEvalLevel,
    HarnessEvalReportStore, HarnessEvalRunControl, HarnessEvalRunRecord, HarnessEvalRunRequest,
    HarnessEvalRunStatus, HarnessEvalRunnerOptions,
};
use serde_json::{json, Value};

use super::{service_envelope, ActiveHarnessEvalJob, HarnessEvalService, ServiceEnvelope};

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
            active_jobs: Arc::new(Mutex::new(Default::default())),
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
            "active_jobs": self.active_jobs_snapshot(),
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
            "active_job": self.active_job_snapshot(id),
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
        let requested_at_ms = now_ms();
        let run_id = format!("harness-eval-{}-{}", level.as_str(), uuid::Uuid::new_v4());
        let options = HarnessEvalRunnerOptions {
            level,
            provider: request.provider,
            budget: request.budget.or_else(|| Some("low".to_string())),
            allow_real_model: request.allow_real_model,
        };
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let record = pending_record(
            run_id.clone(),
            &options,
            requested_at_ms,
            HarnessEvalRunStatus::Running,
            "harness eval accepted by Gateway and running in background worker",
        );
        store
            .upsert_run(&record)
            .map_err(HarnessEvalServiceError::Internal)?;
        {
            let mut jobs = self
                .active_jobs
                .lock()
                .map_err(|error| HarnessEvalServiceError::Internal(error.to_string()))?;
            jobs.insert(
                run_id.clone(),
                ActiveHarnessEvalJob {
                    run_id: run_id.clone(),
                    level: level.as_str().to_string(),
                    requested_at_ms,
                    cancel_requested: Arc::clone(&cancel_requested),
                },
            );
        }
        let worker_store = store.clone();
        let worker_run_id = run_id.clone();
        let worker_jobs = Arc::clone(&self.active_jobs);
        thread::spawn(move || {
            let result = run_eval_controlled(
                &worker_store,
                options.clone(),
                HarnessEvalRunControl::with_run_id(worker_run_id.clone())
                    .with_cancel(Arc::clone(&cancel_requested)),
            );
            if let Err(error) = result {
                let failed = pending_record(
                    worker_run_id.clone(),
                    &options,
                    requested_at_ms,
                    HarnessEvalRunStatus::Failed,
                    format!("harness eval background worker failed: {error}"),
                )
                .finished(now_ms());
                let _ = worker_store.upsert_run(&failed);
            }
            if let Ok(mut jobs) = worker_jobs.lock() {
                jobs.remove(&worker_run_id);
            }
        });
        Ok(run_response(self.envelope("run_start"), record))
    }

    pub(crate) fn cancel_run(
        &self,
        config_home: &Path,
        config: Option<&Value>,
        id: &str,
    ) -> Result<Value, HarnessEvalServiceError> {
        let store = self.store(config_home, config);
        if let Some(job) = self.active_job(id) {
            job.cancel_requested.store(true, Ordering::SeqCst);
            let base = store
                .get_run(id)
                .map_err(HarnessEvalServiceError::Internal)?
                .unwrap_or_else(|| HarnessEvalRunRecord {
                    run_id: job.run_id.clone(),
                    level: job.level.clone(),
                    status: HarnessEvalRunStatus::Running.as_str().to_string(),
                    requested_at_ms: job.requested_at_ms,
                    finished_at_ms: None,
                    authorized_real_model: false,
                    provider: None,
                    budget: None,
                    report_id: None,
                    report_path: None,
                    result_package_dir: None,
                    total_elapsed_ms: None,
                    provider_rounds: 0,
                    tool_calls: 0,
                    total_tokens: 0,
                    scenario_count: 0,
                    message: "active harness eval job".to_string(),
                });
            let mut requested = base;
            requested.status = HarnessEvalRunStatus::CancelRequested.as_str().to_string();
            requested.message =
                "cancel requested; worker will stop at the next safe harness eval checkpoint"
                    .to_string();
            store
                .upsert_run(&requested)
                .map_err(HarnessEvalServiceError::Internal)?;
            return Ok(json!({
                "kind": "harness_eval.run_cancel",
                "envelope": self.envelope("run_cancel"),
                "ok": true,
                "run_id": requested.run_id,
                "status": requested.status,
                "active_job": self.active_job_snapshot(id),
                "message": requested.message,
            }));
        }
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
            "message": "no active background harness eval task is running for this id",
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
    fn active_job(&self, id: &str) -> Option<ActiveHarnessEvalJob> {
        self.active_jobs
            .lock()
            .ok()
            .and_then(|jobs| jobs.get(id).cloned())
    }

    fn active_job_snapshot(&self, id: &str) -> Value {
        self.active_job(id)
            .map(|job| {
            json!({
                "run_id": job.run_id,
                "level": job.level,
                "requested_at_ms": job.requested_at_ms,
                "status": if job.cancel_requested.load(Ordering::SeqCst) { "cancel_requested" } else { "running" },
                "cancel_requested": job.cancel_requested.load(Ordering::SeqCst),
            })
            })
            .unwrap_or(Value::Null)
    }

    fn active_jobs_snapshot(&self) -> Value {
        let jobs = self
            .active_jobs
            .lock()
            .map(|jobs| jobs.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        json!(jobs
            .into_iter()
            .map(|job| {
                json!({
                    "run_id": job.run_id,
                    "level": job.level,
                    "requested_at_ms": job.requested_at_ms,
                    "status": if job.cancel_requested.load(Ordering::SeqCst) { "cancel_requested" } else { "running" },
                    "cancel_requested": job.cancel_requested.load(Ordering::SeqCst),
                })
            })
            .collect::<Vec<_>>())
    }
}

fn pending_record(
    run_id: String,
    options: &HarnessEvalRunnerOptions,
    requested_at_ms: u128,
    status: HarnessEvalRunStatus,
    message: impl Into<String>,
) -> HarnessEvalRunRecord {
    HarnessEvalRunRecord {
        run_id,
        level: options.level.as_str().to_string(),
        status: status.as_str().to_string(),
        requested_at_ms,
        finished_at_ms: None,
        authorized_real_model: options.allow_real_model,
        provider: options.provider.clone(),
        budget: options.budget.clone(),
        report_id: None,
        report_path: None,
        result_package_dir: None,
        total_elapsed_ms: None,
        provider_rounds: 0,
        tool_calls: 0,
        total_tokens: 0,
        scenario_count: 0,
        message: message.into(),
    }
}

trait HarnessEvalRecordFinish {
    fn finished(self, finished_at_ms: u128) -> Self;
}

impl HarnessEvalRecordFinish for HarnessEvalRunRecord {
    fn finished(mut self, finished_at_ms: u128) -> Self {
        self.finished_at_ms = Some(finished_at_ms);
        self.total_elapsed_ms = Some(finished_at_ms.saturating_sub(self.requested_at_ms));
        self
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
        assert_eq!(smoke["run"]["status"], "running");
        let smoke_id = smoke["run"]["run_id"].as_str().expect("run id");
        let completed = wait_for_run_status(&service, &config_home, smoke_id, "completed");
        assert_eq!(completed["run"]["status"], "completed");
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
        assert_eq!(deep_real["run"]["status"], "running");
        let deep_id = deep_real["run"]["run_id"].as_str().expect("deep run id");
        let deep_done = wait_for_run_terminal(&service, &config_home, deep_id);
        assert!(deep_done["run"]["report_path"].is_string());
        let _ = std::fs::remove_dir_all(config_home);
    }

    fn wait_for_run_status(
        service: &HarnessEvalService,
        config_home: &Path,
        run_id: &str,
        expected: &str,
    ) -> Value {
        for _ in 0..40 {
            let detail = service.run_detail(config_home, None, run_id).expect("run");
            if detail["run"]["status"] == expected {
                return detail;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        service.run_detail(config_home, None, run_id).expect("run")
    }

    fn wait_for_run_terminal(
        service: &HarnessEvalService,
        config_home: &Path,
        run_id: &str,
    ) -> Value {
        for _ in 0..40 {
            let detail = service.run_detail(config_home, None, run_id).expect("run");
            let status = detail["run"]["status"].as_str().unwrap_or_default();
            if matches!(status, "completed" | "failed" | "cancelled" | "gated") {
                return detail;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        service.run_detail(config_home, None, run_id).expect("run")
    }
}
