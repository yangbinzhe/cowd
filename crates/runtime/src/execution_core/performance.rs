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

use serde::Serialize;

const SAMPLE_CAPACITY: usize = 512;

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
}
