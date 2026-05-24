//! Provider Chain Implementation.
//!
//! Provides automatic failover and load balancing across multiple API providers.

use crate::error::ApiError;
use crate::types::{MessageRequest, MessageResponse};
use crate::ProviderClient;
use std::time::{Duration, Instant};

/// Provider health status.
#[derive(Debug, Clone)]
pub struct ProviderHealth {
    /// Provider name/identifier.
    pub name: String,
    /// Whether the provider is currently healthy.
    pub is_healthy: bool,
    /// Last check timestamp.
    pub last_check: Instant,
    /// Consecutive failures.
    pub failures: u32,
    /// Average response time in milliseconds.
    pub avg_response_time_ms: Option<f64>,
}

/// Configuration for a single provider in the chain.
#[derive(Debug, Clone)]
pub struct ChainProviderConfig {
    /// Provider name (for logging/debugging).
    pub name: String,
    /// The provider client.
    pub client: ProviderClient,
    /// Weight for load balancing (higher = more traffic).
    pub weight: u32,
    /// Whether this provider is enabled.
    pub enabled: bool,
    /// Failure threshold before marking unhealthy.
    pub failure_threshold: u32,
    /// Recovery threshold (successes needed to mark healthy again).
    pub recovery_threshold: u32,
    /// Timeout for this provider.
    pub timeout: Duration,
}

impl ChainProviderConfig {
    /// Create a new chain provider config.
    pub fn new(name: impl Into<String>, client: ProviderClient) -> Self {
        Self {
            name: name.into(),
            client,
            weight: 100,
            enabled: true,
            failure_threshold: 3,
            recovery_threshold: 2,
            timeout: Duration::from_secs(60),
        }
    }

    /// Set the weight for load balancing.
    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }

    /// Set the failure threshold.
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set the timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// A provider in the chain with its health status.
#[derive(Debug)]
struct ChainProvider {
    config: ChainProviderConfig,
    health: ProviderHealth,
    consecutive_successes: u32,
}

impl ChainProvider {
    fn new(config: ChainProviderConfig) -> Self {
        Self {
            health: ProviderHealth {
                name: config.name.clone(),
                is_healthy: true,
                last_check: Instant::now(),
                failures: 0,
                avg_response_time_ms: None,
            },
            consecutive_successes: 0,
            config,
        }
    }

    fn record_success(&mut self, response_time_ms: f64) {
        self.health.failures = 0;
        self.consecutive_successes += 1;

        // Update average response time
        let current_avg = self.health.avg_response_time_ms.unwrap_or(response_time_ms);
        let n = self.consecutive_successes as f64;
        self.health.avg_response_time_ms =
            Some((current_avg * (n - 1.0) + response_time_ms) / n);

        // Check if recovered
        if self.consecutive_successes >= self.config.recovery_threshold && !self.health.is_healthy
        {
            self.health.is_healthy = true;
        }

        self.health.last_check = Instant::now();
    }

    fn record_failure(&mut self) {
        self.health.failures += 1;
        self.consecutive_successes = 0;

        // Check if should mark unhealthy
        if self.health.failures >= self.config.failure_threshold && self.health.is_healthy {
            self.health.is_healthy = false;
        }

        self.health.last_check = Instant::now();
    }
}

/// Strategy for selecting providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionStrategy {
    /// Use providers in order (failover mode).
    Sequential,
    /// Random selection based on weights.
    WeightedRandom,
    /// Use the healthiest provider.
    Healthiest,
    /// Round-robin across healthy providers.
    RoundRobin,
}

impl Default for SelectionStrategy {
    fn default() -> Self {
        Self::Sequential
    }
}

/// Provider Chain configuration.
#[derive(Debug, Clone)]
pub struct ProviderChainConfig {
    /// Selection strategy.
    pub strategy: SelectionStrategy,
    /// Maximum retries per provider.
    pub max_retries: u32,
    /// Delay between retries.
    pub retry_delay: Duration,
    /// Enable automatic failover.
    pub auto_failover: bool,
}

impl Default for ProviderChainConfig {
    fn default() -> Self {
        Self {
            strategy: SelectionStrategy::Sequential,
            max_retries: 2,
            retry_delay: Duration::from_secs(1),
            auto_failover: true,
        }
    }
}

impl ProviderChainConfig {
    /// Set the selection strategy.
    pub fn with_strategy(mut self, strategy: SelectionStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set the maximum retries.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the retry delay.
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }
}

/// Provider Chain for automatic failover and load balancing.
pub struct ProviderChain {
    config: ProviderChainConfig,
    providers: Vec<ChainProvider>,
}

impl ProviderChain {
    /// Create a new provider chain.
    pub fn new(config: ProviderChainConfig) -> Self {
        Self {
            config,
            providers: Vec::new(),
        }
    }

    /// Add a provider to the chain.
    pub fn add_provider(&mut self, provider: ChainProviderConfig) -> &mut Self {
        self.providers.push(ChainProvider::new(provider));
        self
    }

    /// Add providers from a vector.
    pub fn with_providers(mut self, providers: Vec<ChainProviderConfig>) -> Self {
        for p in providers {
            self.providers.push(ChainProvider::new(p));
        }
        self
    }

    /// Get all healthy providers.
    fn healthy_providers(&self) -> Vec<&ChainProvider> {
        self.providers
            .iter()
            .filter(|p| p.config.enabled && p.health.is_healthy)
            .collect()
    }

    /// Send a message through the chain with failover.
    pub async fn send_message(
        &mut self,
        request: &MessageRequest,
    ) -> Result<MessageResponse, ApiError> {
        // Collect provider indices to avoid borrow issues
        let provider_indices: Vec<usize> = self
            .providers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.config.enabled && p.health.is_healthy)
            .map(|(i, _)| i)
            .collect();

        if provider_indices.is_empty() {
            return Err(ApiError::RetriesExhausted {
                attempts: 0,
                last_error: Box::new(ApiError::Auth(
                    "no available providers".to_string(),
                )),
            });
        }

        let mut last_error = None;

        // Process each provider by index
        for &provider_index in &provider_indices {
            // Get client reference outside of async block
            let client = {
                let provider = &self.providers[provider_index];
                // Clone the client to avoid borrow issues
                let client_ref = provider.config.client.clone();
                client_ref
            };

            for attempt in 0..=self.config.max_retries {
                if attempt > 0 {
                    tokio::time::sleep(self.config.retry_delay).await;
                }

                let start = Instant::now();
                match client.send_message(request).await {
                    Ok(response) => {
                        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                        // Record success
                        self.providers[provider_index].record_success(elapsed);
                        return Ok(response);
                    }
                    Err(e) => {
                        // Record failure
                        self.providers[provider_index].record_failure();

                        // Don't retry if this isn't a retryable error
                        if !is_retryable_error(&e) {
                            last_error = Some(e);
                            break;
                        }

                        last_error = Some(e);

                        // Check if provider is still available
                        if !self.providers[provider_index].config.enabled
                            || !self.providers[provider_index].health.is_healthy
                        {
                            break;
                        }
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ApiError::RetriesExhausted {
                attempts: self.config.max_retries + 1,
                last_error: Box::new(ApiError::Auth("all providers failed".to_string())),
            }
        }))
    }

    /// Get health status of all providers.
    pub fn get_health(&self) -> Vec<ProviderHealth> {
        self.providers.iter().map(|p| p.health.clone()).collect()
    }

    /// Get a specific provider's health.
    pub fn get_provider_health(&self, name: &str) -> Option<ProviderHealth> {
        self.providers
            .iter()
            .find(|p| p.config.name == name)
            .map(|p| p.health.clone())
    }

    /// Enable or disable a provider.
    pub fn set_provider_enabled(&mut self, name: &str, enabled: bool) -> bool {
        if let Some(provider) = self.providers.iter_mut().find(|p| p.config.name == name) {
            provider.config.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// Manually mark a provider as healthy/unhealthy.
    pub fn set_provider_health(&mut self, name: &str, is_healthy: bool) -> bool {
        if let Some(provider) = self.providers.iter_mut().find(|p| p.config.name == name) {
            provider.health.is_healthy = is_healthy;
            if is_healthy {
                provider.health.failures = 0;
                provider.consecutive_successes = provider.config.recovery_threshold;
            } else {
                provider.health.failures = provider.config.failure_threshold;
                provider.consecutive_successes = 0;
            }
            true
        } else {
            false
        }
    }

    /// Get the best (fastest healthy) provider.
    pub fn get_best_provider(&self) -> Option<&ChainProviderConfig> {
        self.healthy_providers()
            .into_iter()
            .min_by(|a, b| {
                let a_time = a.health.avg_response_time_ms.unwrap_or(f64::MAX);
                let b_time = b.health.avg_response_time_ms.unwrap_or(f64::MAX);
                a_time.partial_cmp(&b_time).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| &p.config)
    }
}

/// Check if an error is retryable.
fn is_retryable_error(error: &ApiError) -> bool {
    error.is_retryable()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_config_defaults() {
        let config = ProviderChainConfig::default();
        assert_eq!(config.strategy, SelectionStrategy::Sequential);
        assert_eq!(config.max_retries, 2);
        assert_eq!(config.retry_delay, Duration::from_secs(1));
        assert!(config.auto_failover);
    }

    #[test]
    fn test_chain_config_builder() {
        let config = ProviderChainConfig::default()
            .with_strategy(SelectionStrategy::WeightedRandom)
            .with_max_retries(3)
            .with_retry_delay(Duration::from_secs(2));

        assert_eq!(config.strategy, SelectionStrategy::WeightedRandom);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay, Duration::from_secs(2));
    }

    #[test]
    fn test_selection_strategy() {
        assert_eq!(SelectionStrategy::default(), SelectionStrategy::Sequential);
    }
}
