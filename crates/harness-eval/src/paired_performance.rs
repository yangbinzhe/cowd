//! Public-API paired performance evaluation with distinct first-delta,
//! durability, and terminal measurements.

use std::{
    thread,
    time::{Duration, Instant},
};

use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::session_actor::SessionActor;

use crate::measurement::{
    paired_delta_confidence, start_first_delta_observer, validate_metric_definitions,
    FaultDriverPlan, MeasurementFault, METRIC_DEFINITIONS, RELIABLE_TAIL_MIN_SAMPLES,
};

const SEQUENTIAL_SEED: u64 = 20_260_725;
const WORKLOAD_PROMPT: &str = "只回答 7 乘以 8 的结果。不要调用工具，不要组队。";

#[derive(Debug, Clone)]
pub struct PairedPerformanceOptions {
    pub allow_real_model: bool,
    pub baseline_url: String,
    pub candidate_url: String,
    pub model: String,
    pub min_pairs: usize,
    pub pairs: usize,
    pub target_relative_ci_half_width_bp: u64,
    pub token: Option<String>,
    pub timeout: Duration,
    pub poll_interval: Duration,
}

impl PairedPerformanceOptions {
    pub fn validate(&self) -> Result<(), String> {
        if !self.allow_real_model {
            return Err("paired performance evaluation requires --allow-real-model".to_string());
        }
        if self.timeout.is_zero()
            || self.poll_interval.is_zero()
            || Instant::now().checked_add(self.timeout).is_none()
        {
            return Err(
                "paired performance requires finite positive timeout and polling windows".into(),
            );
        }
        if self.min_pairs < 5 {
            return Err(
                "paired performance evaluation requires at least five minimum pairs".to_string(),
            );
        }
        if self.pairs < self.min_pairs {
            return Err("paired performance maximum pairs must be >= minimum pairs".to_string());
        }
        if self.target_relative_ci_half_width_bp == 0
            || self.target_relative_ci_half_width_bp > 10_000
        {
            return Err(
                "paired performance CI half-width target must be in 1..=10000 basis points"
                    .to_string(),
            );
        }
        for (label, url) in [
            ("baseline", self.baseline_url.as_str()),
            ("candidate", self.candidate_url.as_str()),
        ] {
            if !reqwest::Url::parse(url).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
            }) {
                return Err(format!("{label} URL must be an http(s) URL"));
            }
        }
        if self.model.trim().is_empty() {
            return Err("paired performance evaluation requires a model".to_string());
        }
        Ok(())
    }
}

pub fn run_paired_performance(options: PairedPerformanceOptions) -> Result<Value, String> {
    options.validate()?;
    validate_metric_definitions()?;
    let mut evidence_manifest = crate::runner::build_evidence_manifest(
        &crate::HarnessEvalRunnerOptions {
            level: crate::HarnessEvalLevel::Deep,
            provider: Some(options.model.clone()),
            budget: None,
            allow_real_model: options.allow_real_model,
        },
        crate::report_store::now_ms(),
        0,
        0,
        "unavailable",
    );
    evidence_manifest["command"] = json!("harness-eval paired-performance --allow-real-model");
    evidence_manifest["token_usage"] = json!({"total_tokens":null,"source":"unavailable"});
    let client = build_client(&options)?;
    let baseline = EndpointRunner::new("baseline", &options.baseline_url, &options, &client);
    let candidate = EndpointRunner::new("candidate", &options.candidate_url, &options, &client);
    let warmup = json!({
        "baseline": baseline.run(usize::MAX),
        "candidate": candidate.run(usize::MAX),
    });
    if warmup["baseline"]["status"].as_str() != Some("passed")
        || warmup["candidate"]["status"].as_str() != Some("passed")
    {
        return Err(format!(
            "paired performance warmup failed: {}",
            summarize_json(&warmup)
        ));
    }
    let mut pairs = Vec::with_capacity(options.pairs);

    for pair_index in 0..options.pairs {
        // Alternate first endpoint instead of always giving the candidate the
        // warmer provider/network slot.
        let candidate_first = pair_index % 2 == 1;
        let (baseline_sample, candidate_sample) = if candidate_first {
            let candidate_sample = candidate.run(pair_index);
            let baseline_sample = baseline.run(pair_index);
            (baseline_sample, candidate_sample)
        } else {
            let baseline_sample = baseline.run(pair_index);
            let candidate_sample = candidate.run(pair_index);
            (baseline_sample, candidate_sample)
        };
        pairs.push(json!({
            "pair_index": pair_index,
            "order": if candidate_first { ["candidate", "baseline"] } else { ["baseline", "candidate"] },
            "baseline": baseline_sample,
            "candidate": candidate_sample,
        }));
        if sequential_confidence(&pairs, options.target_relative_ci_half_width_bp).is_some_and(
            |decision| {
                pair_index + 1 >= options.min_pairs
                    && decision["status"].as_str() == Some("converged")
            },
        ) {
            break;
        }
    }

    let sampling = sequential_confidence(&pairs, options.target_relative_ci_half_width_bp)
        .unwrap_or_else(|| json!({"status": "inconclusive", "reason": "missing paired metrics"}));
    let process_metrics = json!({
        "baseline": baseline.performance_snapshot(),
        "candidate": candidate.performance_snapshot(),
    });
    validate_reported_metric_ids(&process_metrics)?;
    let fault_driver = FaultDriverPlan {
        enabled: false,
        fault: MeasurementFault::StreamDisconnectBeforeFirstDelta,
        trigger_after_events: 1,
    };
    fault_driver.validate()?;
    let baseline_summary = summarize_samples(&pairs, "baseline");
    let candidate_summary = summarize_samples(&pairs, "candidate");
    let gate = compare(
        &baseline_summary,
        &candidate_summary,
        pairs.len(),
        options.poll_interval,
        &sampling,
    );

    let mut report = json!({
        "kind": "harness_eval.paired_public_api_performance",
        "status": gate["status"].as_str().unwrap_or("failed"),
        "measurement": {
            "first_surface_delta_definition": "milliseconds from public message admission to the first TextDelta observed on the public Surface stream",
            "actual_first_delta_definition": "provider-request to first Runtime TextDelta duration exported by the process performance snapshot",
            "first_durable_assistant_definition": "milliseconds from public message admission to the first durable assistant response visible through the public session API",
            "terminal_wall_definition": "milliseconds from public message admission to the terminal durable assistant response",
            "prompt": WORKLOAD_PROMPT,
            "acceptance": "assistant response contains 56",
            "all_samples_retained": true,
            "alternating_pair_order": true,
            "tail_percentile_min_samples": RELIABLE_TAIL_MIN_SAMPLES,
            "metric_definitions": METRIC_DEFINITIONS,
        },
        "workload_manifest": {
            "schema_version": 1,
            "prompt_sha256": format!("{:x}", Sha256::digest(WORKLOAD_PROMPT.as_bytes())),
            "model": options.model,
            "protocol": "gateway_session_api",
            "reasoning": "provider_profile",
            "seed": SEQUENTIAL_SEED,
            "transport_fingerprint": "public_http_sse_and_durable_projection",
            "storage_topology": "gateway_selected_storage",
            "feature_flags": [],
        },
        "fault_driver": fault_driver,
        "warmup": warmup,
        "environment": {
            "baseline_url": options.baseline_url,
            "candidate_url": options.candidate_url,
            "model": options.model,
            "pairs_required": options.pairs,
            "min_pairs": options.min_pairs,
            "max_pairs": options.pairs,
            "pairs_completed": pairs.len(),
            "target_relative_ci_half_width_bp": options.target_relative_ci_half_width_bp,
            "timeout_ms": options.timeout.as_millis(),
            "poll_interval_ms": options.poll_interval.as_millis(),
        },
        "pairs": pairs,
        "sampling": sampling,
        "process_metrics": process_metrics,
        "baseline": baseline_summary,
        "candidate": candidate_summary,
        "gate": gate,
        "evidence_manifest": evidence_manifest,
    });
    let readiness = crate::evaluate_report_gate(&report);
    report["status"] = json!(
        if report["gate"]["status"] == "passed" && readiness.status == "passed" {
            "passed"
        } else {
            "failed"
        }
    );
    report["report_gate"] = serde_json::to_value(readiness)
        .map_err(|_| "paired report gate serialization failed".to_string())?;
    Ok(report)
}

fn build_client(options: &PairedPerformanceOptions) -> Result<Client, String> {
    let mut builder =
        Client::builder().timeout(options.timeout.saturating_add(Duration::from_secs(15)));
    if let Some(token) = options
        .token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
    {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|error| format!("invalid paired performance bearer token: {error}"))?;
        headers.insert(AUTHORIZATION, value);
        builder = builder.default_headers(headers);
    }
    builder.build().map_err(|error| error.to_string())
}

struct EndpointRunner<'a> {
    label: &'a str,
    base_url: String,
    model: &'a str,
    timeout: Duration,
    poll_interval: Duration,
    client: &'a Client,
}

impl<'a> EndpointRunner<'a> {
    fn new(
        label: &'a str,
        base_url: &str,
        options: &'a PairedPerformanceOptions,
        client: &'a Client,
    ) -> Self {
        Self {
            label,
            base_url: base_url.trim_end_matches('/').to_string(),
            model: &options.model,
            timeout: options.timeout,
            poll_interval: options.poll_interval,
            client,
        }
    }

    fn run(&self, pair_index: usize) -> Value {
        let Some(deadline) = Instant::now().checked_add(self.timeout) else {
            return failed_sample(
                self.label,
                pair_index,
                "observation_window",
                "unrepresentable observation deadline".into(),
            );
        };
        let actor = SessionActor::create(
            self.client,
            &self.base_url,
            Some(self.model),
            "harness-eval-paired",
            deadline,
        );
        let Ok(mut actor) = actor else {
            return failed_sample(
                self.label,
                pair_index,
                "create_session",
                actor.err().unwrap_or_default(),
            );
        };
        let mut sample = self.run_attached(pair_index, &mut actor, deadline);
        let cleanup = actor.finish().map_or_else(
            |error| json!({"status":"failed","error":error}),
            |_| json!({"status":"passed"}),
        );
        let passed = paired_sample_passed(
            sample.get("status").and_then(Value::as_str) == Some("passed"),
            sample
                .get("first_surface_delta_ms")
                .is_some_and(Value::is_number),
            &cleanup,
        );
        sample["status"] = json!(if passed { "passed" } else { "failed" });
        sample["session_id"] = json!(actor.session_id());
        sample["session_actor_cleanup"] = cleanup;
        sample["session_actor_trace"] = json!(actor.drain_trace());
        sample
    }

    fn run_attached(
        &self,
        pair_index: usize,
        actor: &mut SessionActor<'_>,
        deadline: Instant,
    ) -> Value {
        let session_id = actor.session_id().to_string();

        let first_delta_observer =
            match start_first_delta_observer(self.client, &self.base_url, &session_id) {
                Ok(observer) => observer,
                Err(error) => {
                    return failed_sample(self.label, pair_index, "first_delta_observer", error);
                }
            };
        let started = Instant::now();
        let admission = actor.post_mutation(
            &format!("/api/sessions/{session_id}/messages"),
            json!({
                "content": WORKLOAD_PROMPT,
                "idempotency_key": format!("paired-performance-{}-{}-{}", self.label, pair_index, uuid::Uuid::new_v4()),
            }),
        );
        let admission = match admission {
            Ok(value) => value,
            Err(error) => return failed_sample(self.label, pair_index, "admit_message", error),
        };
        let Some(execution_id) = admission
            .pointer("/execution/graph_id")
            .or_else(|| admission.get("graph_id"))
            .or_else(|| admission.get("execution_graph_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return failed_sample(
                self.label,
                pair_index,
                "admit_message",
                "response lacks canonical execution graph id".to_string(),
            );
        };

        let mut first_surface_delta = None;
        let mut first_durable_assistant_ms = None;
        let mut message_observer = crate::live_scenario_observer::LiveScenarioObserver::default();
        loop {
            if let Err(error) = actor.maintain_receipt_authority() {
                return failed_sample(self.label, pair_index, "receipt_authority", error);
            }
            if first_surface_delta.is_none() {
                first_surface_delta = first_delta_observer.try_recv().ok();
            }
            if let Err(error) =
                self.drain_message_pages(&session_id, &mut message_observer, started, deadline)
            {
                return failed_sample(self.label, pair_index, "poll_messages", error);
            }
            if first_durable_assistant_ms.is_none()
                && message_observer
                    .assistant_message()
                    .is_some_and(|message| !message_text(&message).trim().is_empty())
            {
                first_durable_assistant_ms = Some(started.elapsed().as_millis() as u64);
            }
            let projection = match self
                .get_before(&format!("/api/runtime/executions/{execution_id}"), deadline)
            {
                Ok(projection) => Some(projection),
                Err(error) if error.starts_with("HTTP 404 ") => None,
                Err(error) => {
                    return failed_sample(
                        self.label,
                        pair_index,
                        "poll_execution_projection",
                        error,
                    );
                }
            };
            let terminal = projection.as_ref().is_some_and(|projection| {
                projection
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .is_some_and(|turn_id| {
                        crate::report::execution_observation_ready(
                            projection,
                            &execution_id,
                            &session_id,
                            turn_id,
                        )
                    })
            });
            if terminal {
                if first_surface_delta.is_none() {
                    first_surface_delta = first_delta_observer
                        .recv_timeout(
                            Duration::from_millis(500)
                                .min(deadline.saturating_duration_since(Instant::now())),
                        )
                        .ok();
                }
                let projection = projection.as_ref().unwrap();
                let turn_id = projection.get("turn_id").and_then(Value::as_str).unwrap();
                if let Err(error) =
                    self.drain_message_pages(&session_id, &mut message_observer, started, deadline)
                {
                    return failed_sample(self.label, pair_index, "terminal_messages", error);
                }
                let candidate = match actor.select_terminal_message(
                    &message_observer.assistant_message_ids(),
                    turn_id,
                    &execution_id,
                ) {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        return failed_sample(
                            self.label,
                            pair_index,
                            "terminal_receipt_selection",
                            error,
                        );
                    }
                };
                if let (Some(durable_ms), Some(message_id)) =
                    (first_durable_assistant_ms, candidate.as_ref())
                {
                    let delivery =
                        match actor.confirm_terminal_delivery(message_id, turn_id, &execution_id) {
                            Ok(delivery) => delivery,
                            Err(error) => {
                                return failed_sample(
                                    self.label,
                                    pair_index,
                                    "terminal_receipt",
                                    error,
                                );
                            }
                        };
                    let text = message_text(&json!({"blocks":delivery.blocks}));
                    let terminal_wall_ms = started.elapsed().as_millis() as u64;
                    let accepted = text.contains("56");
                    let delivery_bound = crate::report::delivery_observation_ready(
                        &json!({"execution_id":execution_id,"session_id":session_id,"turn_id":turn_id,"terminal_id":message_id,"delivery_observation":delivery.observation}),
                    );
                    let usage = self.usage_metrics(&session_id, projection, started, deadline);
                    let business_observation = usage
                        .as_ref()
                        .ok()
                        .and_then(|evidence| evidence.get("business_observation"))
                        .cloned()
                        .unwrap_or(Value::Null);
                    let business_passed = crate::report::business_observation_ready(
                        &json!({"execution_id":execution_id,"session_id":session_id,"turn_id":turn_id,"terminal_observation":bounded_execution_observation(projection),"business_observation":business_observation}),
                    );
                    let first_surface_delta_ms = first_surface_delta.map(|observed| {
                        u64::try_from(observed.observed_at.duration_since(started).as_millis())
                            .unwrap_or(u64::MAX)
                    });
                    let first_surface_delta_source_cursor =
                        first_surface_delta.and_then(|observed| observed.source_cursor);
                    return json!({
                        "endpoint": self.label,
                        "pair_index": pair_index,
                        "status": if accepted && business_passed && delivery_bound && first_surface_delta_ms.is_some() { "passed" } else { "failed" },
                        "session_id": session_id,
                        "execution_id": execution_id,
                        "turn_id": turn_id,
                        "terminal_id": message_id,
                        "terminal_observation": bounded_execution_observation(projection),
                        "delivery_observation": delivery.observation,
                        "business_observation": business_observation,
                        "metrics": usage.as_ref().ok().and_then(|evidence| evidence.get("metrics")).cloned().unwrap_or(Value::Null),
                        "usage_observation_integrity": usage.as_ref().ok().and_then(|evidence| evidence.get("integrity")).cloned().unwrap_or(Value::Null),
                        "usage_observation_error": usage.err(),
                        "first_surface_delta_ms": first_surface_delta_ms,
                        "first_surface_delta_source_cursor": first_surface_delta_source_cursor,
                        "first_durable_assistant_ms": durable_ms,
                        "terminal_wall_ms": terminal_wall_ms,
                        "measurement_error": if first_surface_delta_ms.is_none() {
                            Some("no visible TextDelta was observed; durable response was not substituted for TTFT")
                        } else {
                            None
                        },
                        "acceptance": {"passed": accepted, "required": "56"},
                        "response_summary": summarize(&text, 240),
                    });
                }
            }
            if Instant::now() >= deadline {
                return failed_sample(
                    self.label,
                    pair_index,
                    "wait_terminal",
                    format!("timed out after {}ms", self.timeout.as_millis()),
                );
            }
            thread::sleep(self.poll_interval);
        }
    }

    fn usage_metrics(
        &self,
        session_id: &str,
        projection: &Value,
        started: Instant,
        deadline: Instant,
    ) -> Result<Value, String> {
        let mut observer = crate::live_scenario_observer::LiveScenarioObserver::default();
        for _ in 0..crate::live_scenario_observer::MAX_DRAIN_PAGES_PER_PROBE {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| "provider evidence observation deadline exceeded".to_string())?;
            let path = observer.timeline_path(session_id);
            let response = self
                .client
                .get(format!("{}{}", self.base_url, path))
                .timeout(remaining)
                .send()
                .map_err(|_| "provider evidence observation transport failed".to_string())
                .and_then(response_json);
            if usage_page_drained(
                &mut observer,
                &path,
                &response,
                started.elapsed().as_millis() as u64,
            )? {
                let timeline = observer.timeline();
                let execution_id = projection
                    .get("execution_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "provider evidence lacks execution identity".to_string())?;
                return Ok(
                    json!({"metrics":crate::live_scenario_runner::scenario_metrics(
                    &timeline,
                    std::slice::from_ref(projection),
                    None,
                    started.elapsed(),
                ),"business_observation":crate::live_scenario_runner::root_business_observation(&timeline, execution_id),"integrity":observer.integrity_report()}),
                );
            }
        }
        Err("provider evidence pagination did not drain within the original page bound".into())
    }

    fn get(&self, path: &str) -> Result<Value, String> {
        self.client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .map_err(|error| error.to_string())
            .and_then(response_json)
    }

    fn performance_snapshot(&self) -> Value {
        self.get("/healthz")
            .ok()
            .and_then(|health| health.get("performance").cloned())
            .unwrap_or_else(|| json!({"status": "unavailable"}))
    }

    fn get_before(&self, path: &str, deadline: Instant) -> Result<Value, String> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err("paired observation deadline exceeded".into());
        }
        self.client
            .get(format!("{}{}", self.base_url, path))
            .timeout(remaining)
            .send()
            .map_err(|_| "paired observation transport failed".to_string())
            .and_then(response_json)
    }

    fn drain_message_pages(
        &self,
        session_id: &str,
        observer: &mut crate::live_scenario_observer::LiveScenarioObserver,
        started: Instant,
        deadline: Instant,
    ) -> Result<(), String> {
        for _ in 0..crate::live_scenario_observer::MAX_DRAIN_PAGES_PER_PROBE {
            let path = observer.message_path(session_id);
            let response = self.get_before(&path, deadline);
            let page = observer.observe_message_page(
                &path,
                &response,
                started.elapsed().as_millis() as u64,
            )?;
            response.as_ref().map_err(Clone::clone)?;
            if !page.has_more {
                return Ok(());
            }
        }
        Err("paired message pagination did not drain within the original page bound".into())
    }
}

fn usage_page_drained(
    observer: &mut crate::live_scenario_observer::LiveScenarioObserver,
    path: &str,
    response: &Result<Value, String>,
    elapsed_ms: u64,
) -> Result<bool, String> {
    let page = observer.observe_timeline_page(path, response, elapsed_ms)?;
    response.as_ref().map_err(Clone::clone)?;
    if !page.has_more
        && observer
            .integrity_report()
            .get("timeline_drained")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err("provider evidence timeline was not fully drained".into());
    }
    Ok(!page.has_more)
}

fn paired_sample_passed(
    answer_accepted: bool,
    observed_surface_delta: bool,
    cleanup: &Value,
) -> bool {
    answer_accepted && observed_surface_delta && cleanup["status"] == "passed"
}

fn validate_reported_metric_ids(process_metrics: &Value) -> Result<(), String> {
    let canonical = METRIC_DEFINITIONS
        .iter()
        .map(|metric| metric.id)
        .collect::<std::collections::BTreeSet<_>>();
    for endpoint in ["baseline", "candidate"] {
        let Some(samples) = process_metrics.get(endpoint).and_then(Value::as_array) else {
            continue;
        };
        for sample in samples {
            let Some(metric_id) = sample.get("metric_id").and_then(Value::as_str) else {
                return Err(format!(
                    "{endpoint} performance snapshot contains a metric without metric_id"
                ));
            };
            if !canonical.contains(metric_id) {
                return Err(format!(
                    "{endpoint} performance snapshot contains unknown metric `{metric_id}`"
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn bounded_execution_observation(value: &Value) -> Value {
    json!({
        "execution_id": value.get("execution_id"),
        "session_id": value.get("session_id"),
        "turn_id": value.get("turn_id"),
        "revision": value.get("revision"),
        "cursor": value.get("cursor"),
        "terminal_axes": value.get("terminal_axes"),
    })
}

fn response_json(response: reqwest::blocking::Response) -> Result<Value, String> {
    let status = response.status();
    let value = response
        .json::<Value>()
        .map_err(|error| error.to_string())?;
    if status.is_success() {
        Ok(value)
    } else {
        Err(format!("HTTP {status}: {}", summarize_json(&value)))
    }
}

fn message_text(message: &Value) -> String {
    message
        .get("content")
        .or_else(|| message.get("text"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            message
                .get("blocks")
                .and_then(Value::as_array)
                .map(|blocks| {
                    blocks
                        .iter()
                        .filter_map(|block| {
                            block
                                .get("text")
                                .or_else(|| block.get("content"))
                                .and_then(Value::as_str)
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
        })
        .unwrap_or_default()
}

fn failed_sample(endpoint: &str, pair_index: usize, stage: &str, reason: String) -> Value {
    json!({
        "endpoint": endpoint,
        "pair_index": pair_index,
        "status": "failed",
        "stage": stage,
        "error": reason,
    })
}

fn summarize_samples(pairs: &[Value], key: &str) -> Value {
    let samples = pairs
        .iter()
        .filter_map(|pair| pair.get(key))
        .collect::<Vec<_>>();
    let failed = samples
        .iter()
        .filter(|sample| sample.get("status").and_then(Value::as_str) != Some("passed"))
        .count();
    let first_surface_delta = values(&samples, "first_surface_delta_ms");
    let first_durable_assistant = values(&samples, "first_durable_assistant_ms");
    let terminal_wall = values(&samples, "terminal_wall_ms");
    json!({
        "sample_count": samples.len(),
        "failed_sample_count": failed,
        "first_surface_delta_ms": percentile_summary(first_surface_delta),
        "first_durable_assistant_ms": percentile_summary(first_durable_assistant),
        "terminal_wall_ms": percentile_summary(terminal_wall),
    })
}

fn values(samples: &[&Value], field: &str) -> Vec<u64> {
    samples
        .iter()
        .filter_map(|sample| sample.get(field).and_then(Value::as_u64))
        .collect()
}

fn percentile_summary(mut values: Vec<u64>) -> Value {
    values.sort_unstable();
    let tail_reliable = values.len() >= RELIABLE_TAIL_MIN_SAMPLES;
    json!({
        "samples": values,
        "p50": percentile(&values, 0.50),
        "p95": tail_reliable.then(|| percentile(&values, 0.95)).flatten(),
        "p95_reliable": tail_reliable,
        "tail_min_samples": RELIABLE_TAIL_MIN_SAMPLES,
    })
}

fn percentile(values: &[u64], quantile: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    // Nearest-rank percentile: rank = ceil(n * q), with one-based ranks.
    // The former `(n - 1) * q` interpolation-like expression selected the
    // maximum for p95 with 20 samples, silently turning the release p95 gate
    // into p100 while the report claimed nearest-rank semantics.
    let index = ((values.len() as f64 * quantile).ceil() as usize).saturating_sub(1);
    values.get(index).copied()
}

fn sequential_confidence(pairs: &[Value], target_half_width_bp: u64) -> Option<Value> {
    let paired_values = |field: &str| {
        pairs
            .iter()
            .map(|pair| {
                Some((
                    pair.pointer(&format!("/baseline/{field}"))?.as_u64()?,
                    pair.pointer(&format!("/candidate/{field}"))?.as_u64()?,
                ))
            })
            .collect::<Option<Vec<_>>>()
    };
    let ttft = paired_values("first_surface_delta_ms")?;
    let wall = paired_values("terminal_wall_ms")?;
    let (baseline_ttft, candidate_ttft): (Vec<_>, Vec<_>) = ttft.into_iter().unzip();
    let (baseline_wall, candidate_wall): (Vec<_>, Vec<_>) = wall.into_iter().unzip();
    let ttft_ci = paired_delta_confidence(&baseline_ttft, &candidate_ttft, SEQUENTIAL_SEED)?;
    let wall_ci = paired_delta_confidence(
        &baseline_wall,
        &candidate_wall,
        SEQUENTIAL_SEED ^ 0x9e37_79b9,
    )?;
    let converged = ttft_ci.relative_half_width_bp <= target_half_width_bp
        && wall_ci.relative_half_width_bp <= target_half_width_bp;
    Some(json!({
        "status": if converged { "converged" } else { "inconclusive" },
        "target_relative_ci_half_width_bp": target_half_width_bp,
        "first_surface_delta": ttft_ci,
        "terminal_wall": wall_ci,
    }))
}

fn compare(
    baseline: &Value,
    candidate: &Value,
    required_pairs: usize,
    poll_interval: Duration,
    sampling: &Value,
) -> Value {
    let failures = baseline
        .get("failed_sample_count")
        .and_then(Value::as_u64)
        .unwrap_or(u64::MAX)
        + candidate
            .get("failed_sample_count")
            .and_then(Value::as_u64)
            .unwrap_or(u64::MAX);
    let baseline_count = baseline
        .get("sample_count")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let candidate_count = candidate
        .get("sample_count")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let baseline_ttft = baseline
        .pointer("/first_surface_delta_ms/p50")
        .and_then(Value::as_u64);
    let candidate_ttft = candidate
        .pointer("/first_surface_delta_ms/p50")
        .and_then(Value::as_u64);
    let baseline_wall_summary = baseline.get("terminal_wall_ms").unwrap_or(&Value::Null);
    let candidate_wall_summary = candidate.get("terminal_wall_ms").unwrap_or(&Value::Null);
    let tail_reliable = baseline_wall_summary
        .get("p95_reliable")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && candidate_wall_summary
            .get("p95_reliable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let wall_percentile = if tail_reliable { "p95" } else { "p50" };
    let baseline_wall = baseline_wall_summary
        .get(wall_percentile)
        .and_then(Value::as_u64);
    let candidate_wall = candidate_wall_summary
        .get(wall_percentile)
        .and_then(Value::as_u64);
    let polling_precision_allowance_ms = u64::try_from(poll_interval.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(2);
    let ttft_limit_without_sampling_error = baseline_ttft
        .map(|value| (value as f64 * 0.05).ceil() as u64)
        .map(|value| value.max(100));
    let wall_limit_without_sampling_error = baseline_wall
        .map(|value| (value as f64 * 0.10).ceil() as u64)
        .map(|value| value.max(300));
    // The public endpoint is polled independently for baseline and candidate.
    // Each durable timestamp is therefore quantized by up to one poll period;
    // comparing two samples has a bounded two-period observation error. This
    // is measurement uncertainty, not extra Runtime budget, and is reported
    // separately instead of silently relaxing the performance target.
    let ttft_limit = ttft_limit_without_sampling_error
        .map(|limit| limit.saturating_add(polling_precision_allowance_ms));
    let wall_limit = wall_limit_without_sampling_error
        .map(|limit| limit.saturating_add(polling_precision_allowance_ms));
    let ttft_delta = baseline_ttft
        .zip(candidate_ttft)
        .map(|(base, current)| current.saturating_sub(base));
    let wall_delta = baseline_wall
        .zip(candidate_wall)
        .map(|(base, current)| current.saturating_sub(base));
    let passed = sampling.get("status").and_then(Value::as_str) == Some("converged")
        && failures == 0
        && baseline_count == required_pairs as u64
        && candidate_count == required_pairs as u64
        && ttft_delta
            .zip(ttft_limit)
            .is_some_and(|(delta, limit)| delta <= limit)
        && wall_delta
            .zip(wall_limit)
            .is_some_and(|(delta, limit)| delta <= limit);
    json!({
        "status": if passed { "passed" } else { "failed" },
        "requirements": {
            "pairs": required_pairs,
            "sequential_sampling": sampling,
            "p50_ttft_max_regression_ms": ttft_limit,
            "wall_max_regression_ms": wall_limit,
            "wall_gate_percentile": wall_percentile,
            "tail_percentile_reliable": tail_reliable,
            "p50_ttft_target_without_sampling_error_ms": ttft_limit_without_sampling_error,
            "wall_target_without_sampling_error_ms": wall_limit_without_sampling_error,
            "polling_precision_allowance_ms": polling_precision_allowance_ms,
            "formula": "actual TTFT p50 max(100ms, 5% baseline) + 2*poll_interval; wall uses p95 only with at least 20 samples per endpoint, otherwise p50, max(300ms, 10% baseline) + 2*poll_interval",
        },
        "observed": {
            "failed_samples": failures,
            "baseline_p50_ttft_ms": baseline_ttft,
            "candidate_p50_ttft_ms": candidate_ttft,
            "p50_ttft_regression_ms": ttft_delta,
            "baseline_wall_ms": baseline_wall,
            "candidate_wall_ms": candidate_wall,
            "wall_regression_ms": wall_delta,
        },
    })
}

pub(crate) fn paired_statistics_passed(report: &Value) -> bool {
    let Some(pairs) = report.get("pairs").and_then(Value::as_array) else {
        return false;
    };
    let Some(min_pairs) = report
        .pointer("/environment/min_pairs")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
    else {
        return false;
    };
    let Some(max_pairs) = report
        .pointer("/environment/max_pairs")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
    else {
        return false;
    };
    let Some(target) = report
        .pointer("/environment/target_relative_ci_half_width_bp")
        .and_then(Value::as_u64)
    else {
        return false;
    };
    let Some(poll_ms) = report
        .pointer("/environment/poll_interval_ms")
        .and_then(Value::as_u64)
    else {
        return false;
    };
    if min_pairs < 5
        || pairs.len() < min_pairs
        || pairs.len() > max_pairs
        || target == 0
        || target > 10_000
        || poll_ms == 0
    {
        return false;
    }
    let Some(sampling) = sequential_confidence(pairs, target) else {
        return false;
    };
    compare(
        &summarize_samples(pairs, "baseline"),
        &summarize_samples(pairs, "candidate"),
        pairs.len(),
        Duration::from_millis(poll_ms),
        &sampling,
    )["status"]
        == "passed"
}

fn summarize(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let summary = chars.by_ref().take(limit).collect::<String>();
    if chars.next().is_some() {
        format!("{summary}...")
    } else {
        summary
    }
}

fn summarize_json(value: &Value) -> String {
    summarize(&value.to_string(), 300)
}

#[cfg(test)]
mod tests {
    #[test]
    fn later_timeline_page_error_cannot_certify_a_successful_prefix() {
        let mut observer = crate::live_scenario_observer::LiveScenarioObserver::default();
        let first = Ok(json!({"events":[{
            "source":"runtime_lifecycle","stream_id":"session:test","sequence":0,"commit_cursor":1,"transaction_index":0,
            "kind":"context.provider_attempt_outcome","payload":{"request_id":"original","usage_status":"known","cache_dimensions_status":"known","terminal_status":"completed","usage":{"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}
        }],"next_cursor":"v2:1:1:0","has_more":true}));
        assert!(!usage_page_drained(&mut observer, "/timeline", &first, 1).unwrap());
        let prefix = crate::live_scenario_runner::scenario_metrics(
            &observer.timeline(),
            &[],
            None,
            Duration::from_secs(1),
        );
        assert_eq!(prefix["provider_usage_records"], 1);
        assert!(
            usage_page_drained(&mut observer, "/timeline", &Err("HTTP 503".into()), 2).is_err()
        );
        assert_eq!(observer.integrity_report()["timeline_drained"], false);
        assert!(usage_page_drained(
            &mut observer,
            "/timeline",
            &Ok(json!({"events":[],"has_more":false})),
            3
        )
        .unwrap());
    }

    #[test]
    fn message_candidates_come_from_the_original_drained_observer() {
        let mut observer = crate::live_scenario_observer::LiveScenarioObserver::default();
        observer.observe_message_page("/messages", &Ok(json!({
            "messages":[{"id":"user","sequence":0,"role":"user"},{"id":"answer","sequence":1,"role":"assistant"},{"message_id":"later","sequence":2,"role":"assistant"}],
            "next_seq":3,"has_more":false
        })), 1).unwrap();
        let actual = observer
            .assistant_message_ids()
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let expected = ["answer".to_string(), "later".to_string()]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual, expected);
    }

    fn authorized_options() -> super::PairedPerformanceOptions {
        super::PairedPerformanceOptions {
            allow_real_model: true,
            baseline_url: "http://127.0.0.1:1".into(),
            candidate_url: "http://127.0.0.1:2".into(),
            model: "fixture-model".into(),
            min_pairs: 5,
            pairs: 5,
            target_relative_ci_half_width_bp: 500,
            token: None,
            timeout: std::time::Duration::from_secs(1),
            poll_interval: std::time::Duration::from_millis(1),
        }
    }

    #[test]
    fn paired_requires_explicit_authorization_before_building_a_client() {
        for token in [None, Some("invalid\nheader".to_string())] {
            let mut options = authorized_options();
            options.allow_real_model = false;
            options.token = token;
            assert!(super::run_paired_performance(options)
                .unwrap_err()
                .contains("--allow-real-model"));
        }
    }

    #[test]
    fn authorized_paired_options_validate_without_starting_a_run() {
        let mut options = authorized_options();
        assert!(options.validate().is_ok());
        options.min_pairs = 4;
        assert!(options.validate().is_err());
        options.min_pairs = 5;
        options.pairs = 4;
        assert!(options.validate().is_err());
    }

    #[test]
    fn r1_integrated_provider_metric_ids_preserve_real_usage_and_unknowns() {
        assert!(super::paired_sample_passed(
            true,
            true,
            &serde_json::json!({"status":"passed"})
        ));
        assert!(!super::paired_sample_passed(
            false,
            true,
            &serde_json::json!({"status":"passed"})
        ));
        assert!(!super::paired_sample_passed(
            true,
            false,
            &serde_json::json!({"status":"passed"})
        ));
        for cleanup in [
            serde_json::json!({"status":"failed"}),
            serde_json::json!({"status":"unavailable"}),
            serde_json::json!({}),
        ] {
            assert!(!super::paired_sample_passed(true, true, &cleanup));
        }
        let provider_metrics = [
            "actual_first_delta_ms",
            "provider_stream_ms",
            "provider_service_ms",
            "provider_input_tokens",
            "provider_output_tokens",
            "provider_output_tokens_per_second",
        ]
        .map(|metric_id| serde_json::json!({"metric_id":metric_id,"count":1,"p50":7}));
        let real = serde_json::json!({"baseline":provider_metrics,"candidate":provider_metrics});
        assert!(super::validate_reported_metric_ids(&real).is_ok());
        let unknown = serde_json::json!({"baseline":[],"candidate":[{"metric_id":"estimated_provider_usage"}]});
        assert!(super::validate_reported_metric_ids(&unknown)
            .unwrap_err()
            .contains("unknown metric"));
        let missing = serde_json::json!({"baseline":[],"candidate":[{"count":0}]});
        assert!(super::validate_reported_metric_ids(&missing)
            .unwrap_err()
            .contains("without metric_id"));
        let unavailable = serde_json::json!({"baseline":{"status":"unavailable"},"candidate":{"status":"unavailable"}});
        assert!(super::validate_reported_metric_ids(&unavailable).is_ok());
        assert!(!unavailable["candidate"].is_array());
        for metric_id in [
            "provider_input_tokens",
            "provider_output_tokens",
            "provider_output_tokens_per_second",
        ] {
            let definition = crate::measurement::METRIC_DEFINITIONS
                .iter()
                .find(|metric| metric.id == metric_id)
                .unwrap();
            assert_eq!(definition.unit, "count");
        }
        assert!(crate::measurement::METRIC_DEFINITIONS
            .iter()
            .any(|metric| metric.id == "input_tokens"));
        assert!(crate::measurement::METRIC_DEFINITIONS
            .iter()
            .any(|metric| metric.id == "output_tokens"));
    }
    use super::*;

    #[test]
    fn percentile_summary_uses_all_samples_and_nearest_rank() {
        let summary = percentile_summary(vec![500, 100, 300, 200, 400]);
        assert_eq!(summary["samples"], json!([100, 200, 300, 400, 500]));
        assert_eq!(summary["p50"], 300);
        assert_eq!(summary["p95"], Value::Null);
        assert_eq!(summary["p95_reliable"], false);
    }

    #[test]
    fn nearest_rank_p95_of_twenty_samples_is_not_the_maximum() {
        let values = (1_u64..=20).collect::<Vec<_>>();

        assert_eq!(percentile(&values, 0.95), Some(19));
        assert_eq!(percentile(&values, 1.0), Some(20));
        let summary = percentile_summary(values);
        assert_eq!(summary["p95"], 19);
        assert_eq!(summary["p95_reliable"], true);
    }

    #[test]
    fn comparison_fails_when_any_sample_is_missing_or_failed() {
        let baseline = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "first_surface_delta_ms": {"p50": 1000},
            "terminal_wall_ms": {"p50": 2000, "p95": null, "p95_reliable": false},
        });
        let candidate = json!({
            "sample_count": 4,
            "failed_sample_count": 1,
            "first_surface_delta_ms": {"p50": 1000},
            "terminal_wall_ms": {"p50": 2000, "p95": null, "p95_reliable": false},
        });
        assert_eq!(
            compare(
                &baseline,
                &candidate,
                5,
                Duration::ZERO,
                &json!({"status":"converged"})
            )["status"],
            "failed"
        );
    }

    #[test]
    fn comparison_accepts_exactly_allowed_regression() {
        let baseline = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "first_surface_delta_ms": {"p50": 1000},
            "terminal_wall_ms": {"p50": 2000, "p95": null, "p95_reliable": false},
        });
        let candidate = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "first_surface_delta_ms": {"p50": 1050},
            "terminal_wall_ms": {"p50": 2200, "p95": null, "p95_reliable": false},
        });
        assert_eq!(
            compare(
                &baseline,
                &candidate,
                5,
                Duration::ZERO,
                &json!({"status":"converged"})
            )["status"],
            "passed"
        );
    }

    #[test]
    fn comparison_allows_only_the_declared_two_poll_observation_error() {
        let baseline = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "first_surface_delta_ms": {"p50": 1000},
            "terminal_wall_ms": {"p50": 2000, "p95": null, "p95_reliable": false},
        });
        let within = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "first_surface_delta_ms": {"p50": 1140},
            "terminal_wall_ms": {"p50": 2200, "p95": null, "p95_reliable": false},
        });
        let beyond = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "first_surface_delta_ms": {"p50": 1141},
            "terminal_wall_ms": {"p50": 2200, "p95": null, "p95_reliable": false},
        });
        let interval = Duration::from_millis(20);

        let sampling = json!({"status":"converged"});
        let allowed = compare(&baseline, &within, 5, interval, &sampling);
        assert_eq!(
            allowed["requirements"]["polling_precision_allowance_ms"],
            40
        );
        assert_eq!(allowed["status"], "passed");
        assert_eq!(
            compare(&baseline, &beyond, 5, interval, &sampling)["status"],
            "failed"
        );
    }
}
