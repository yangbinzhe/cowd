//! Public-API paired performance evaluation.
//!
//! This intentionally measures what a surface can observe: from message
//! admission to the first *durably materialized* assistant response. It does
//! not borrow Runtime internals or discard slow/failed samples. A historical
//! baseline and a candidate are started independently by the caller, then
//! exercised in alternating order to reduce simple time-drift bias.

use std::{
    thread,
    time::{Duration, Instant},
};

use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct PairedPerformanceOptions {
    pub baseline_url: String,
    pub candidate_url: String,
    pub model: String,
    pub pairs: usize,
    pub token: Option<String>,
    pub timeout: Duration,
    pub poll_interval: Duration,
}

impl PairedPerformanceOptions {
    pub fn validate(&self) -> Result<(), String> {
        if self.pairs < 5 {
            return Err("paired performance evaluation requires at least five pairs".to_string());
        }
        for (label, url) in [
            ("baseline", self.baseline_url.as_str()),
            ("candidate", self.candidate_url.as_str()),
        ] {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
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
    let client = build_client(&options)?;
    let baseline = EndpointRunner::new("baseline", &options.baseline_url, &options, &client);
    let candidate = EndpointRunner::new("candidate", &options.candidate_url, &options, &client);
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
    }

    let baseline_summary = summarize_samples(&pairs, "baseline");
    let candidate_summary = summarize_samples(&pairs, "candidate");
    let gate = compare(
        &baseline_summary,
        &candidate_summary,
        options.pairs,
        options.poll_interval,
    );

    Ok(json!({
        "kind": "harness_eval.paired_public_api_performance",
        "status": gate["status"].as_str().unwrap_or("failed"),
        "measurement": {
            "ttft_definition": "milliseconds from public message admission to the first durable assistant response visible through the public session API",
            "wall_definition": "milliseconds from public message admission to the terminal durable assistant response",
            "prompt": "只回答 7 乘以 8 的结果。不要调用工具，不要组队。",
            "acceptance": "assistant response contains 56",
            "all_samples_retained": true,
            "alternating_pair_order": true,
        },
        "environment": {
            "baseline_url": options.baseline_url,
            "candidate_url": options.candidate_url,
            "model": options.model,
            "pairs_required": options.pairs,
            "timeout_ms": options.timeout.as_millis(),
            "poll_interval_ms": options.poll_interval.as_millis(),
        },
        "pairs": pairs,
        "baseline": baseline_summary,
        "candidate": candidate_summary,
        "gate": gate,
    }))
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
        let session_response = self.post("/api/sessions", json!({"model": self.model}));
        let session_id = session_response
            .as_ref()
            .ok()
            .and_then(|value| extract_session_id(value))
            .map(ToString::to_string);
        let Some(session_id) = session_id else {
            return failed_sample(
                self.label,
                pair_index,
                "create_session",
                session_response
                    .err()
                    .unwrap_or_else(|| "response lacks session id".to_string()),
            );
        };

        let started = Instant::now();
        let admission = self.post(
            &format!("/api/sessions/{session_id}/messages"),
            json!({
                "content": "只回答 7 乘以 8 的结果。不要调用工具，不要组队。",
                "idempotency_key": format!("paired-performance-{}-{}-{}", self.label, pair_index, uuid::Uuid::new_v4()),
            }),
        );
        if let Err(error) = admission {
            return failed_sample(self.label, pair_index, "admit_message", error);
        }

        let deadline = started + self.timeout;
        loop {
            match self.get(&format!("/api/sessions/{session_id}/messages?limit=200")) {
                Ok(messages) => {
                    if let Some(text) = latest_assistant_text(&messages) {
                        let elapsed_ms = started.elapsed().as_millis() as u64;
                        let accepted = text.contains("56");
                        return json!({
                            "endpoint": self.label,
                            "pair_index": pair_index,
                            "status": if accepted { "passed" } else { "failed" },
                            "session_id": session_id,
                            "first_durable_response_ms": elapsed_ms,
                            "wall_ms": elapsed_ms,
                            "acceptance": {"passed": accepted, "required": "56"},
                            "response_summary": summarize(&text, 240),
                        });
                    }
                }
                Err(error) => return failed_sample(self.label, pair_index, "poll_messages", error),
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

    fn get(&self, path: &str) -> Result<Value, String> {
        self.client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .map_err(|error| error.to_string())
            .and_then(response_json)
    }

    fn post(&self, path: &str, body: Value) -> Result<Value, String> {
        self.client
            .post(format!("{}{}", self.base_url, path))
            .json(&body)
            .send()
            .map_err(|error| error.to_string())
            .and_then(response_json)
    }
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

fn extract_session_id(value: &Value) -> Option<&str> {
    value
        .get("id")
        .or_else(|| value.get("session_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn latest_assistant_text(value: &Value) -> Option<String> {
    let messages = value
        .as_array()
        .or_else(|| value.get("messages").and_then(Value::as_array))?;
    messages.iter().rev().find_map(|message| {
        (message.get("role").and_then(Value::as_str) == Some("assistant"))
            .then(|| message_text(message))
            .filter(|text| !text.trim().is_empty())
    })
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
    let ttft = values(&samples, "first_durable_response_ms");
    let wall = values(&samples, "wall_ms");
    json!({
        "sample_count": samples.len(),
        "failed_sample_count": failed,
        "ttft_ms": percentile_summary(ttft),
        "wall_ms": percentile_summary(wall),
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
    json!({
        "samples": values,
        "p50": percentile(&values, 0.50),
        "p95": percentile(&values, 0.95),
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

fn compare(
    baseline: &Value,
    candidate: &Value,
    required_pairs: usize,
    poll_interval: Duration,
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
    let baseline_ttft = baseline.pointer("/ttft_ms/p50").and_then(Value::as_u64);
    let candidate_ttft = candidate.pointer("/ttft_ms/p50").and_then(Value::as_u64);
    let baseline_wall = baseline.pointer("/wall_ms/p95").and_then(Value::as_u64);
    let candidate_wall = candidate.pointer("/wall_ms/p95").and_then(Value::as_u64);
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
    let passed = failures == 0
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
            "p50_ttft_max_regression_ms": ttft_limit,
            "p95_wall_max_regression_ms": wall_limit,
            "p50_ttft_target_without_sampling_error_ms": ttft_limit_without_sampling_error,
            "p95_wall_target_without_sampling_error_ms": wall_limit_without_sampling_error,
            "polling_precision_allowance_ms": polling_precision_allowance_ms,
            "formula": "TTFT max(100ms, 5% baseline) + 2*poll_interval; wall max(300ms, 10% baseline) + 2*poll_interval",
        },
        "observed": {
            "failed_samples": failures,
            "baseline_p50_ttft_ms": baseline_ttft,
            "candidate_p50_ttft_ms": candidate_ttft,
            "p50_ttft_regression_ms": ttft_delta,
            "baseline_p95_wall_ms": baseline_wall,
            "candidate_p95_wall_ms": candidate_wall,
            "p95_wall_regression_ms": wall_delta,
        },
    })
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
    use super::*;

    #[test]
    fn percentile_summary_uses_all_samples_and_nearest_rank() {
        let summary = percentile_summary(vec![500, 100, 300, 200, 400]);
        assert_eq!(summary["samples"], json!([100, 200, 300, 400, 500]));
        assert_eq!(summary["p50"], 300);
        assert_eq!(summary["p95"], 500);
    }

    #[test]
    fn nearest_rank_p95_of_twenty_samples_is_not_the_maximum() {
        let values = (1_u64..=20).collect::<Vec<_>>();

        assert_eq!(percentile(&values, 0.95), Some(19));
        assert_eq!(percentile(&values, 1.0), Some(20));
    }

    #[test]
    fn comparison_fails_when_any_sample_is_missing_or_failed() {
        let baseline = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "ttft_ms": {"p50": 1000},
            "wall_ms": {"p95": 2000},
        });
        let candidate = json!({
            "sample_count": 4,
            "failed_sample_count": 1,
            "ttft_ms": {"p50": 1000},
            "wall_ms": {"p95": 2000},
        });
        assert_eq!(
            compare(&baseline, &candidate, 5, Duration::ZERO)["status"],
            "failed"
        );
    }

    #[test]
    fn comparison_accepts_exactly_allowed_regression() {
        let baseline = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "ttft_ms": {"p50": 1000},
            "wall_ms": {"p95": 2000},
        });
        let candidate = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "ttft_ms": {"p50": 1050},
            "wall_ms": {"p95": 2200},
        });
        assert_eq!(
            compare(&baseline, &candidate, 5, Duration::ZERO)["status"],
            "passed"
        );
    }

    #[test]
    fn comparison_allows_only_the_declared_two_poll_observation_error() {
        let baseline = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "ttft_ms": {"p50": 1000},
            "wall_ms": {"p95": 2000},
        });
        let within = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "ttft_ms": {"p50": 1140},
            "wall_ms": {"p95": 2200},
        });
        let beyond = json!({
            "sample_count": 5,
            "failed_sample_count": 0,
            "ttft_ms": {"p50": 1141},
            "wall_ms": {"p95": 2200},
        });
        let interval = Duration::from_millis(20);

        let allowed = compare(&baseline, &within, 5, interval);
        assert_eq!(
            allowed["requirements"]["polling_precision_allowance_ms"],
            40
        );
        assert_eq!(allowed["status"], "passed");
        assert_eq!(compare(&baseline, &beyond, 5, interval)["status"], "failed");
    }
}
