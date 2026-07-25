use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const RELIABLE_TAIL_MIN_SAMPLES: usize = 20;
const BOOTSTRAP_RESAMPLES: usize = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MeasurementClock {
    ProcessMonotonic,
    SourceSequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MetricDefinition {
    pub id: &'static str,
    pub unit: &'static str,
    pub clock: MeasurementClock,
    pub producer: &'static str,
    pub consumer: &'static str,
}

pub(crate) const METRIC_DEFINITIONS: &[MetricDefinition] = &[
    duration_metric("gateway_accept_ms", "gateway ingress", "harness-eval"),
    duration_metric(
        "session_activation_queue_ms",
        "gateway session activation",
        "harness-eval",
    ),
    duration_metric("hydrate_ms", "runtime session activation", "harness-eval"),
    duration_metric(
        "runtime_lock_wait_ms",
        "gateway runtime guard",
        "harness-eval",
    ),
    duration_metric(
        "runtime_lock_hold_ms",
        "gateway runtime guard",
        "harness-eval",
    ),
    duration_metric("context_select_ms", "runtime context", "harness-eval"),
    duration_metric(
        "request_history_clone_ms",
        "runtime session/request",
        "harness-eval",
    ),
    duration_metric(
        "request_materialize_ms",
        "runtime provider adapter",
        "harness-eval",
    ),
    byte_metric("clone_bytes", "runtime session/request", "harness-eval"),
    duration_metric(
        "provider_admission_queue_ms",
        "runtime resource manager",
        "harness-eval",
    ),
    duration_metric(
        "transport_checkout_ms",
        "runtime provider transport pool",
        "harness-eval",
    ),
    duration_metric(
        "provider_stream_ms",
        "runtime provider stream",
        "harness-eval",
    ),
    duration_metric(
        "provider_producer_wait_ms",
        "runtime bounded provider event stream",
        "harness-eval",
    ),
    duration_metric(
        "provider_service_ms",
        "runtime provider boundary",
        "harness-eval",
    ),
    duration_metric(
        "actual_first_delta_ms",
        "runtime canonical TextDelta",
        "harness-eval",
    ),
    duration_metric(
        "first_surface_delta_ms",
        "gateway live projection",
        "harness-eval",
    ),
    duration_metric(
        "first_durable_assistant_ms",
        "session durable messages",
        "harness-eval",
    ),
    duration_metric(
        "terminal_wall_ms",
        "runtime terminal projection",
        "harness-eval",
    ),
    duration_metric("tool_prepare_ms", "runtime tooling", "harness-eval"),
    duration_metric("tool_queue_ms", "runtime resource manager", "harness-eval"),
    duration_metric("tool_run_ms", "runtime tooling", "harness-eval"),
    duration_metric("storage_queue_ms", "memory session store", "harness-eval"),
    duration_metric(
        "artifact_write_ms",
        "runtime artifact adapter",
        "harness-eval",
    ),
    duration_metric(
        "event_loop_lag_ms",
        "runtime/gateway timer probe",
        "harness-eval",
    ),
    duration_metric(
        "surface_projection_ms",
        "gateway live projection",
        "harness-eval",
    ),
    duration_metric(
        "markdown_parse_ms",
        "webui markdown renderer",
        "harness-eval",
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MeasurementFault {
    StreamDisconnectBeforeFirstDelta,
    StreamCursorGap,
    DurableProjectionDelay,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FaultDriverPlan {
    pub enabled: bool,
    pub fault: MeasurementFault,
    pub trigger_after_events: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FirstDeltaObservation {
    pub observed_at: Instant,
    pub source_cursor: Option<u64>,
}

impl FaultDriverPlan {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.enabled && self.trigger_after_events == 0 {
            return Err("enabled measurement fault must trigger after at least one event".into());
        }
        Ok(())
    }
}

const fn duration_metric(
    id: &'static str,
    producer: &'static str,
    consumer: &'static str,
) -> MetricDefinition {
    MetricDefinition {
        id,
        unit: "milliseconds",
        clock: MeasurementClock::ProcessMonotonic,
        producer,
        consumer,
    }
}

const fn byte_metric(
    id: &'static str,
    producer: &'static str,
    consumer: &'static str,
) -> MetricDefinition {
    MetricDefinition {
        id,
        unit: "bytes",
        clock: MeasurementClock::ProcessMonotonic,
        producer,
        consumer,
    }
}

pub(crate) fn validate_metric_definitions() -> Result<(), String> {
    let mut ids = BTreeSet::new();
    for metric in METRIC_DEFINITIONS {
        if !ids.insert(metric.id) {
            return Err(format!("duplicate measurement metric id: {}", metric.id));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PairedDeltaConfidence {
    pub samples: usize,
    pub median_delta_ms: i64,
    pub lower_ms: i64,
    pub upper_ms: i64,
    pub relative_half_width_bp: u64,
}

pub(crate) fn paired_delta_confidence(
    baseline: &[u64],
    candidate: &[u64],
    seed: u64,
) -> Option<PairedDeltaConfidence> {
    if baseline.is_empty() || baseline.len() != candidate.len() {
        return None;
    }
    let deltas = baseline
        .iter()
        .zip(candidate)
        .map(|(baseline, candidate)| {
            i64::try_from(*candidate)
                .unwrap_or(i64::MAX)
                .saturating_sub(i64::try_from(*baseline).unwrap_or(i64::MAX))
        })
        .collect::<Vec<_>>();
    let median_delta_ms = median_i64(deltas.clone());
    let baseline_median = median_u64(baseline.to_vec()).max(1);
    let mut state = seed.max(1);
    let mut estimates = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let mut resample = Vec::with_capacity(deltas.len());
        for _ in 0..deltas.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            resample.push(deltas[(state as usize) % deltas.len()]);
        }
        estimates.push(median_i64(resample));
    }
    estimates.sort_unstable();
    let lower_ms = estimates[(estimates.len() * 25 / 1_000).min(estimates.len() - 1)];
    let upper_ms = estimates[(estimates.len() * 975 / 1_000).min(estimates.len() - 1)];
    let half_width = upper_ms.saturating_sub(lower_ms).unsigned_abs() / 2;
    Some(PairedDeltaConfidence {
        samples: deltas.len(),
        median_delta_ms,
        lower_ms,
        upper_ms,
        relative_half_width_bp: half_width
            .saturating_mul(10_000)
            .saturating_div(baseline_median),
    })
}

fn median_i64(mut values: Vec<i64>) -> i64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_u64(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

/// Observe the first visible model TextDelta from the public multiplex stream.
///
/// A terminal response is deliberately not accepted as a substitute: doing so
/// would turn durable response latency back into a mislabeled TTFT sample.
pub(crate) fn start_first_delta_observer(
    client: &Client,
    base: &str,
    session_id: &str,
) -> Result<mpsc::Receiver<FirstDeltaObservation>, String> {
    let client = client.clone();
    let base = base.to_string();
    let session_id = session_id.to_string();
    let subscription_url = format!("{base}/api/runtime/live-subscriptions");
    let (ready_tx, ready_rx) = mpsc::sync_channel::<Result<(), String>>(1);
    let (first_tx, first_rx) = mpsc::sync_channel::<FirstDeltaObservation>(1);
    thread::spawn(move || {
        let surface_instance = format!("eval:{}", uuid::Uuid::new_v4());
        let subscription = match client
            .post(&subscription_url)
            .header("x-cowd-observer-id", &surface_instance)
            .json(&serde_json::json!({
                "surface_instance": surface_instance,
                "selector": {
                    "sources": [{
                        "kind": "session",
                        "id": session_id,
                        "cursor": 0,
                        "detail_scope": "summary"
                    }]
                }
            }))
            .send()
        {
            Ok(response) if response.status().is_success() => match response.json::<Value>() {
                Ok(value) => value,
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return;
                }
            },
            Ok(response) => {
                let _ = ready_tx.send(Err(format!(
                    "live subscription returned HTTP {}",
                    response.status()
                )));
                return;
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error.to_string()));
                return;
            }
        };
        let Some(stream_url) = subscription.get("stream_url").and_then(Value::as_str) else {
            let _ = ready_tx.send(Err("live subscription omitted stream_url".to_string()));
            return;
        };
        let response = match client.get(format!("{base}{stream_url}")).send() {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                let _ = ready_tx.send(Err(format!("SSE returned HTTP {}", response.status())));
                return;
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error.to_string()));
                return;
            }
        };
        let mut ready = false;
        let mut first_sent = false;
        for line in BufReader::new(response).lines() {
            let Ok(line) = line else {
                break;
            };
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let Ok(envelope) = serde_json::from_str::<Value>(data.trim()) else {
                continue;
            };
            let event_name = envelope.get("event").and_then(Value::as_str);
            let event = envelope.get("payload").cloned().unwrap_or(Value::Null);
            match event_name {
                Some("subscription.ready" | "session.connected") => {
                    if !ready {
                        ready = true;
                        let _ = ready_tx.send(Ok(()));
                    }
                }
                Some("TextDelta") if !first_sent && visible_text_delta(&event).is_some() => {
                    first_sent = true;
                    let source_cursor = envelope
                        .get("source_cursor")
                        .or_else(|| event.get("runtime_commit_cursor"))
                        .or_else(|| event.get("source_cursor"))
                        .or_else(|| event.get("cursor"))
                        .or_else(|| event.get("sequence"))
                        .and_then(Value::as_u64);
                    let _ = first_tx.send(FirstDeltaObservation {
                        observed_at: Instant::now(),
                        source_cursor,
                    });
                }
                Some("TerminalCommitted") => break,
                _ => {}
            }
        }
        if !ready {
            let _ = ready_tx.send(Err("SSE closed before the Connected event".to_string()));
        }
    });
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| "SSE Connected event timed out".to_string())??;
    Ok(first_rx)
}

fn visible_text_delta(event: &Value) -> Option<&str> {
    event
        .get("content")
        .or_else(|| event.get("text"))
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_metric_ids_are_unique() {
        validate_metric_definitions().unwrap();
        assert!(METRIC_DEFINITIONS
            .iter()
            .any(|metric| metric.id == "actual_first_delta_ms"));
        assert!(METRIC_DEFINITIONS
            .iter()
            .any(|metric| metric.id == "first_durable_assistant_ms"));
    }

    #[test]
    fn terminal_payload_is_not_a_text_delta() {
        let terminal = serde_json::json!({
            "type": "TerminalCommitted",
            "response": "complete"
        });
        assert_eq!(visible_text_delta(&terminal), None);
    }

    #[test]
    fn paired_confidence_is_deterministic_and_tight_for_stable_deltas() {
        let baseline = vec![1_000; 8];
        let candidate = vec![950; 8];
        let first = paired_delta_confidence(&baseline, &candidate, 42).unwrap();
        let second = paired_delta_confidence(&baseline, &candidate, 42).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.median_delta_ms, -50);
        assert_eq!(first.relative_half_width_bp, 0);
    }

    #[test]
    fn enabled_fault_driver_rejects_ambiguous_zero_event_trigger() {
        let plan = FaultDriverPlan {
            enabled: true,
            fault: MeasurementFault::StreamCursorGap,
            trigger_after_events: 0,
        };
        assert!(plan.validate().is_err());
    }
}
