//! Low-cardinality measurements for the terminal Surface hot path.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

const SAMPLE_CAPACITY: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuiPerformanceMetric {
    pub metric_id: &'static str,
    pub unit: &'static str,
    pub samples: usize,
    pub p50: u64,
    pub p95: u64,
    pub max: u64,
    pub total: u64,
}

#[derive(Debug, Default)]
struct Samples {
    unit: &'static str,
    values: VecDeque<u64>,
    total: u64,
}

#[derive(Debug, Default)]
struct Registry {
    metrics: Mutex<BTreeMap<&'static str, Samples>>,
    pending_input: Mutex<Option<Instant>>,
}

impl Registry {
    fn observe(&self, metric_id: &'static str, unit: &'static str, value: u64) {
        let mut metrics = self
            .metrics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let samples = metrics.entry(metric_id).or_insert_with(|| Samples {
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
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

pub(crate) fn observe_duration(metric_id: &'static str, duration: Duration) {
    registry().observe(
        metric_id,
        "milliseconds",
        duration.as_millis().min(u128::from(u64::MAX)) as u64,
    );
}

pub(crate) fn observe_count(metric_id: &'static str, count: usize) {
    registry().observe(metric_id, "count", u64::try_from(count).unwrap_or(u64::MAX));
}

pub(crate) fn note_input() {
    *registry()
        .pending_input
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Instant::now());
}

pub(crate) fn observe_input_frame() {
    let started = registry()
        .pending_input
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(started) = started {
        observe_duration("tui_input_to_frame_ms", started.elapsed());
    }
}

#[must_use]
pub fn performance_snapshot() -> Vec<TuiPerformanceMetric> {
    registry()
        .metrics
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|(metric_id, samples)| {
            let mut values = samples.values.iter().copied().collect::<Vec<_>>();
            values.sort_unstable();
            let percentile = |percent: usize| {
                if values.is_empty() {
                    return 0;
                }
                let rank = values
                    .len()
                    .saturating_mul(percent)
                    .div_ceil(100)
                    .saturating_sub(1);
                values[rank.min(values.len() - 1)]
            };
            TuiPerformanceMetric {
                metric_id,
                unit: samples.unit,
                samples: values.len(),
                p50: percentile(50),
                p95: percentile(95),
                max: values.last().copied().unwrap_or_default(),
                total: samples.total,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_bounded_and_keeps_static_metric_ids() {
        for value in 0..(SAMPLE_CAPACITY + 8) {
            registry().observe("tui_test_count", "count", value as u64);
        }
        let metric = performance_snapshot()
            .into_iter()
            .find(|metric| metric.metric_id == "tui_test_count")
            .expect("metric");
        assert_eq!(metric.samples, SAMPLE_CAPACITY);
        assert_eq!(metric.max, (SAMPLE_CAPACITY + 7) as u64);
    }
}
