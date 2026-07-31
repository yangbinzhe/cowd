//! Low-cardinality, always-on execution-path measurements.
//!
//! This registry deliberately accepts only static metric identifiers and keeps
//! a bounded sample window. Per-session and per-tool detail belongs in sampled
//! execution evidence, not in this process-wide aggregate.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Mutex, OnceLock},
    time::Duration,
};

use serde::{Deserialize, Serialize};

const SAMPLE_CAPACITY: usize = 512;
const TURN_TRACE_CAPACITY: usize = 1_024;

/// Correlated, bounded evidence for one Runtime turn. Optional phases are
/// filled by their owning layer; absent values mean "not observed", never
/// zero-duration work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnLatencyTrace {
    pub trace_id: String,
    pub session_id: String,
    pub turn_id: Option<String>,
    pub activation_ms: Option<u64>,
    pub context_ms: Option<u64>,
    pub provider_ms: Option<u64>,
    pub tool_ms: Option<u64>,
    pub commit_ms: Option<u64>,
    pub total_ms: u64,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PerformanceMetricSnapshot {
    pub metric_id: &'static str,
    pub unit: &'static str,
    pub samples: usize,
    pub p50: u64,
    pub p95: u64,
    pub max: u64,
    pub total: u64,
}

#[derive(Debug, Default)]
struct MetricSamples {
    unit: &'static str,
    values: VecDeque<u64>,
    total: u64,
}

#[derive(Debug, Default)]
struct PerformanceRegistry {
    metrics: Mutex<BTreeMap<&'static str, MetricSamples>>,
    turn_traces: Mutex<VecDeque<TurnLatencyTrace>>,
    pending_activation_ms: Mutex<BTreeMap<String, u64>>,
}

impl PerformanceRegistry {
    fn observe(&self, metric_id: &'static str, unit: &'static str, value: u64) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let samples = metrics.entry(metric_id).or_insert_with(|| MetricSamples {
            unit,
            values: VecDeque::with_capacity(SAMPLE_CAPACITY),
            total: 0,
        });
        debug_assert_eq!(samples.unit, unit);
        if samples.values.len() == SAMPLE_CAPACITY {
            samples.values.pop_front();
        }
        samples.values.push_back(value);
        samples.total = samples.total.saturating_add(value);
    }

    fn snapshot(&self) -> Vec<PerformanceMetricSnapshot> {
        self.metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(metric_id, samples)| {
                let mut values = samples.values.iter().copied().collect::<Vec<_>>();
                values.sort_unstable();
                PerformanceMetricSnapshot {
                    metric_id,
                    unit: samples.unit,
                    samples: values.len(),
                    p50: percentile(&values, 50),
                    p95: percentile(&values, 95),
                    max: values.last().copied().unwrap_or_default(),
                    total: samples.total,
                }
            })
            .collect()
    }

    fn record_session_activation(&self, session_id: String, activation_ms: u64) {
        self.pending_activation_ms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id, activation_ms);
    }

    fn record_turn_trace(&self, mut trace: TurnLatencyTrace) {
        if trace.activation_ms.is_none() {
            trace.activation_ms = self
                .pending_activation_ms
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&trace.session_id);
            if let Some(activation_ms) = trace.activation_ms {
                trace.total_ms = trace.total_ms.saturating_add(activation_ms);
            }
        }
        let mut traces = self
            .turn_traces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if traces.len() == TURN_TRACE_CAPACITY {
            traces.pop_front();
        }
        traces.push_back(trace);
    }

    fn turn_traces(&self, session_id: Option<&str>) -> Vec<TurnLatencyTrace> {
        self.turn_traces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|trace| session_id.is_none_or(|id| trace.session_id == id))
            .cloned()
            .collect()
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let rank = values
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1);
    values[rank.min(values.len() - 1)]
}

fn registry() -> &'static PerformanceRegistry {
    static REGISTRY: OnceLock<PerformanceRegistry> = OnceLock::new();
    REGISTRY.get_or_init(PerformanceRegistry::default)
}

pub fn observe_duration(metric_id: &'static str, duration: Duration) {
    registry().observe(
        metric_id,
        "milliseconds",
        duration.as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

pub fn observe_bytes(metric_id: &'static str, bytes: usize) {
    registry().observe(metric_id, "bytes", u64::try_from(bytes).unwrap_or(u64::MAX));
}

pub fn observe_count(metric_id: &'static str, count: u64) {
    registry().observe(metric_id, "count", count);
}

pub fn record_turn_latency_trace(trace: TurnLatencyTrace) {
    registry().record_turn_trace(trace);
}

pub fn record_session_activation_latency(session_id: impl Into<String>, duration: Duration) {
    registry().record_session_activation(
        session_id.into(),
        duration.as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

#[must_use]
pub fn turn_latency_traces(session_id: Option<&str>) -> Vec<TurnLatencyTrace> {
    registry().turn_traces(session_id)
}

#[must_use]
pub fn performance_snapshot() -> Vec<PerformanceMetricSnapshot> {
    registry().snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_is_bounded_and_low_cardinality() {
        let registry = PerformanceRegistry::default();
        for value in 0..(SAMPLE_CAPACITY + 10) {
            registry.observe("test_ms", "milliseconds", value as u64);
        }
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].samples, SAMPLE_CAPACITY);
        assert_eq!(snapshot[0].max, (SAMPLE_CAPACITY + 9) as u64);
    }

    #[test]
    fn turn_trace_registry_is_bounded_and_filterable() {
        let registry = PerformanceRegistry::default();
        for value in 0..(TURN_TRACE_CAPACITY + 2) {
            registry.record_turn_trace(TurnLatencyTrace {
                trace_id: format!("trace-{value}"),
                session_id: if value % 2 == 0 { "a" } else { "b" }.to_string(),
                total_ms: value as u64,
                ..TurnLatencyTrace::default()
            });
        }
        assert_eq!(registry.turn_traces(None).len(), TURN_TRACE_CAPACITY);
        assert_eq!(
            registry.turn_traces(Some("a")).len() + registry.turn_traces(Some("b")).len(),
            TURN_TRACE_CAPACITY
        );
    }

    #[test]
    fn first_turn_consumes_pending_session_activation_latency_once() {
        let registry = PerformanceRegistry::default();
        registry.record_session_activation("session-a".to_string(), 12);
        registry.record_turn_trace(TurnLatencyTrace {
            trace_id: "first".to_string(),
            session_id: "session-a".to_string(),
            total_ms: 5,
            ..TurnLatencyTrace::default()
        });
        registry.record_turn_trace(TurnLatencyTrace {
            trace_id: "second".to_string(),
            session_id: "session-a".to_string(),
            total_ms: 5,
            ..TurnLatencyTrace::default()
        });
        let traces = registry.turn_traces(Some("session-a"));
        assert_eq!(traces[0].activation_ms, Some(12));
        assert_eq!(traces[0].total_ms, 17);
        assert_eq!(traces[1].activation_ms, None);
    }
}
