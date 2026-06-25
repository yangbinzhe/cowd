//! Lightweight memory retrieval evaluation.
//!
//! This module is intentionally test/runtime friendly: it evaluates existing
//! memory search behavior without adding a new store, scheduler, or background
//! dependency.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{
    cognitive::CognitiveContextManager, error::MemoryError, types::SearchMemoriesRequest, MemoryId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEvalCase {
    pub id: String,
    pub query: String,
    pub expected_memory_id: MemoryId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEvalMiss {
    pub case_id: String,
    pub query: String,
    pub expected_memory_id: MemoryId,
    pub returned_ids: Vec<MemoryId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEvalReport {
    pub case_count: usize,
    pub top_k: usize,
    pub recall_at_k: f64,
    pub mrr: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub max_latency_ms: f64,
    pub total_latency_ms: f64,
    pub passed: bool,
    pub min_recall_at_k: f64,
    pub max_p95_latency_ms: f64,
    pub misses: Vec<MemoryEvalMiss>,
}

#[derive(Debug, Clone, Copy)]
pub struct MemoryEvalOptions {
    pub top_k: usize,
    pub min_recall_at_k: f64,
    pub max_p95_latency: Duration,
}

impl Default for MemoryEvalOptions {
    fn default() -> Self {
        Self {
            top_k: 5,
            min_recall_at_k: 0.95,
            max_p95_latency: Duration::from_millis(500),
        }
    }
}

pub async fn evaluate_retrieval(
    manager: &CognitiveContextManager,
    cases: &[MemoryEvalCase],
    options: MemoryEvalOptions,
) -> std::result::Result<MemoryEvalReport, MemoryError> {
    let top_k = options.top_k.max(1);
    let mut hits = 0usize;
    let mut reciprocal_rank_sum = 0.0f64;
    let mut latencies_ms = Vec::with_capacity(cases.len());
    let mut misses = Vec::new();
    let started = Instant::now();

    for case in cases {
        let query_started = Instant::now();
        let result = manager
            .search_memories(SearchMemoriesRequest {
                query: case.query.clone(),
                limit: top_k,
                with_snippets: false,
                with_keywords: false,
                ..Default::default()
            })
            .await?;
        latencies_ms.push(query_started.elapsed().as_secs_f64() * 1000.0);

        let returned_ids: Vec<MemoryId> = result.entries.iter().map(|entry| entry.id).collect();
        if let Some(rank) = returned_ids
            .iter()
            .position(|id| *id == case.expected_memory_id)
        {
            hits += 1;
            reciprocal_rank_sum += 1.0 / (rank + 1) as f64;
        } else {
            misses.push(MemoryEvalMiss {
                case_id: case.id.clone(),
                query: case.query.clone(),
                expected_memory_id: case.expected_memory_id,
                returned_ids,
            });
        }
    }

    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let case_count = cases.len();
    let recall_at_k = if case_count == 0 {
        1.0
    } else {
        hits as f64 / case_count as f64
    };
    let mrr = if case_count == 0 {
        1.0
    } else {
        reciprocal_rank_sum / case_count as f64
    };
    let p50_latency_ms = percentile(&latencies_ms, 50.0);
    let p95_latency_ms = percentile(&latencies_ms, 95.0);
    let max_latency_ms = latencies_ms.last().copied().unwrap_or(0.0);
    let total_latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    let max_p95_latency_ms = options.max_p95_latency.as_secs_f64() * 1000.0;
    let passed = recall_at_k >= options.min_recall_at_k && p95_latency_ms <= max_p95_latency_ms;

    Ok(MemoryEvalReport {
        case_count,
        top_k,
        recall_at_k,
        mrr,
        p50_latency_ms,
        p95_latency_ms,
        max_latency_ms,
        total_latency_ms,
        passed,
        min_recall_at_k: options.min_recall_at_k,
        max_p95_latency_ms,
        misses,
    })
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let clamped = percentile.clamp(0.0, 100.0);
    let rank = ((clamped / 100.0) * (sorted.len().saturating_sub(1)) as f64).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_handles_empty_and_bounds() {
        assert_eq!(percentile(&[], 95.0), 0.0);
        assert_eq!(percentile(&[1.0, 2.0, 3.0], 0.0), 1.0);
        assert_eq!(percentile(&[1.0, 2.0, 3.0], 95.0), 3.0);
        assert_eq!(percentile(&[1.0, 2.0, 3.0], 100.0), 3.0);
    }
}
