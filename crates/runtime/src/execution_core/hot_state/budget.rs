use std::fs;

use serde::{Deserialize, Serialize};

const DEFAULT_MEMORY_RATIO: f64 = 0.60;
const DEFAULT_RESERVE_RATIO: f64 = 0.20;
const DEFAULT_HIGH_WATERMARK: f64 = 0.90;
const DEFAULT_LOW_WATERMARK: f64 = 0.75;
const MIN_FALLBACK_MEMORY_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotStateMemoryConfig {
    pub ratio: f64,
    pub max_bytes: Option<u64>,
    pub reserve_ratio: f64,
    pub high_watermark: f64,
    pub low_watermark: f64,
}

impl Eq for HotStateMemoryConfig {}

impl Default for HotStateMemoryConfig {
    fn default() -> Self {
        Self {
            ratio: DEFAULT_MEMORY_RATIO,
            max_bytes: None,
            reserve_ratio: DEFAULT_RESERVE_RATIO,
            high_watermark: DEFAULT_HIGH_WATERMARK,
            low_watermark: DEFAULT_LOW_WATERMARK,
        }
    }
}

impl HotStateMemoryConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0 < self.ratio && self.ratio <= 0.90) {
            return Err("runtime.hot_state.memory.ratio must be in (0, 0.90]".to_string());
        }
        if !(0.10..=0.50).contains(&self.reserve_ratio) {
            return Err(
                "runtime.hot_state.memory.reserve_ratio must be in [0.10, 0.50]".to_string(),
            );
        }
        if !(0.0 < self.low_watermark
            && self.low_watermark < self.high_watermark
            && self.high_watermark <= 1.0)
        {
            return Err(
                "runtime.hot_state memory watermarks require 0 < low < high <= 1".to_string(),
            );
        }
        if self.max_bytes == Some(0) {
            return Err("runtime.hot_state.memory.max_bytes must be positive".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveCheckpointConfig {
    /// Minimum wall-clock distance between non-boundary durable checkpoints.
    pub min_interval_ms: u64,
    /// Force a checkpoint when this many live revisions accumulated even when
    /// the wall-clock interval has not elapsed.
    pub max_revision_gap: u64,
}

impl Eq for LiveCheckpointConfig {}

impl Default for LiveCheckpointConfig {
    fn default() -> Self {
        Self {
            min_interval_ms: 1_000,
            max_revision_gap: 32,
        }
    }
}

impl LiveCheckpointConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.min_interval_ms == 0 {
            return Err(
                "runtime.hot_state.live_checkpoint.min_interval_ms must be positive".to_string(),
            );
        }
        if self.max_revision_gap == 0 {
            return Err(
                "runtime.hot_state.live_checkpoint.max_revision_gap must be positive".to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotStateConfig {
    pub memory: HotStateMemoryConfig,
    /// Zero lets Runtime derive a power-of-two shard count from CPU
    /// parallelism. It is intentionally a topology hint, not a capacity cap.
    pub shards: usize,
    pub materializer_queue_capacity: usize,
    pub live_checkpoint: LiveCheckpointConfig,
}

impl Eq for HotStateConfig {}

impl Default for HotStateConfig {
    fn default() -> Self {
        Self {
            memory: HotStateMemoryConfig::default(),
            shards: 0,
            materializer_queue_capacity: 1024,
            live_checkpoint: LiveCheckpointConfig::default(),
        }
    }
}

impl HotStateConfig {
    pub fn validate(&self) -> Result<(), String> {
        self.memory.validate()?;
        self.live_checkpoint.validate()?;
        if self.materializer_queue_capacity == 0 {
            return Err(
                "runtime.hot_state.materializer_queue_capacity must be positive".to_string(),
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn resolved_shards(&self) -> usize {
        if self.shards > 0 {
            return self.shards.next_power_of_two().clamp(2, 256);
        }
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .saturating_mul(2)
            .next_power_of_two()
            .clamp(4, 256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotMemoryBudget {
    pub detected_available_bytes: u64,
    pub limit_bytes: u64,
    pub high_watermark_bytes: u64,
    pub low_watermark_bytes: u64,
}

impl HotMemoryBudget {
    #[must_use]
    pub fn resolve(config: &HotStateMemoryConfig) -> Self {
        let available = detect_available_memory_bytes().max(MIN_FALLBACK_MEMORY_BYTES);
        let after_reserve = ((available as f64) * (1.0 - config.reserve_ratio)).max(1.0) as u64;
        let ratio_limit = ((available as f64) * config.ratio).max(1.0) as u64;
        let mut limit = ratio_limit.min(after_reserve);
        if let Some(max_bytes) = config.max_bytes {
            limit = limit.min(max_bytes);
        }
        let limit = limit.max(1);
        Self {
            detected_available_bytes: available,
            limit_bytes: limit,
            high_watermark_bytes: ((limit as f64) * config.high_watermark) as u64,
            low_watermark_bytes: ((limit as f64) * config.low_watermark) as u64,
        }
    }
}

fn detect_available_memory_bytes() -> u64 {
    let cgroup_limit = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .into_iter()
    .filter_map(read_positive_bytes)
    .filter(|value| *value < (1_u64 << 60))
    .min();
    let host_available = fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| {
            contents.lines().find_map(|line| {
                let value = line.strip_prefix("MemAvailable:")?;
                value
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|kilobytes| kilobytes.saturating_mul(1024))
            })
        });
    match (cgroup_limit, host_available) {
        (Some(cgroup), Some(host)) => cgroup.min(host),
        (Some(cgroup), None) => cgroup,
        (None, Some(host)) => host,
        (None, None) => MIN_FALLBACK_MEMORY_BYTES,
    }
}

fn read_positive_bytes(path: &str) -> Option<u64> {
    let raw = fs::read_to_string(path).ok()?;
    let value = raw.trim().parse::<u64>().ok()?;
    (value > 0).then_some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_watermarks() {
        let config = HotStateMemoryConfig {
            low_watermark: 0.9,
            high_watermark: 0.8,
            ..HotStateMemoryConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn resolved_budget_obeys_explicit_ceiling() {
        let config = HotStateMemoryConfig {
            max_bytes: Some(64 * 1024 * 1024),
            ..HotStateMemoryConfig::default()
        };
        let budget = HotMemoryBudget::resolve(&config);
        assert!(budget.limit_bytes <= 64 * 1024 * 1024);
        assert!(budget.low_watermark_bytes < budget.high_watermark_bytes);
    }
}
