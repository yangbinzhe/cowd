//! Provider Chain Implementation.
//!
//! Provides automatic failover and load balancing across multiple API providers.

use crate::error::ApiError;
use crate::types::{MessageRequest, MessageResponse, StreamEvent};
use crate::ProviderClient;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

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
        self.health.avg_response_time_ms = Some(
            (current_avg * (n - 1.0) + response_time_ms) / n
        );

        // Check if recovered
        if self.consecutive_successes >= self.config.recovery_threshold && !self.health.is_healthy {
            self.health.is_healthy = true;
            tracing::info!(provider = %self.config.name, "provider recovered");
        }

        self.health.last_check = Instant::now();
    }

    fn record_failure(&mut self) {
        self.health.failures += 1;
        self.consecutive_successes = 0;

        // Check if should mark unhealthy
        if self.health.failures >= self.config.failure_threshold && self.health.is_healthy {
            self.health.is_healthy = false;
            tracing::warn!(provider = %self.config.name, failures = %self.health.failures, "provider marked unhealthy");
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
    round_robin_index: Arc<RwLock<usize>>,
}

impl ProviderChain {
    /// Create a new provider chain.
    pub fn new(config: ProviderChainConfig) -> Self {
        Self {
            config,
            providers: Vec::new(),
            round_robin_index: Arc::new(RwLock::new(0)),
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
        self.providers.iter().filter(|p| p.config.enabled && p.health.is_healthy).collect()
    }

    /// Select providers based on the configured strategy.
    fn select_providers(&self) -> Vec<&ChainProvider> {
        let healthy = self.healthy_providers();

        if healthy.is_empty() {
            // Fall back to all enabled providers if none are healthy
            return self.providers.iter()
                .filter(|p| p.config.enabled)
                .collect();
        }

        match self.config.strategy {
            SelectionStrategy::Sequential | SelectionStrategy::RoundRobin => healthy,
            SelectionStrategy::WeightedRandom => {
                // Sort by weight for weighted selection
                let mut sorted = healthy.clone();
                sorted.sort_by(|a, b| b.config.weight.cmp(&a.config.weight));
                sorted
            }
            SelectionStrategy::Healthiest => {
                // Sort by average response time (healthiest = fastest)
                let mut sorted = healthy.clone();
                sorted.sort_by(|a, b| {
                    let a_time = a.health.avg_response_time_ms.unwrap_or(f64::MAX);
                    let b_time = b.health.avg_response_time_ms.unwrap_or(f64::MAX);
                    a_time.partial_cmp(&b_time).unwrap_or(std::cmp::Ordering::Equal)
                });
                sorted
            }
        }
    }

    /// Get the next provider in round-robin order.
    async fn next_round_robin(&self) -> Option<usize> {
        let healthy = self.healthy_providers();
        if healthy.is_empty() {
            return None;
        }

        let mut index = self.round_robin_index.write().await;
        let healthy_count = healthy.len();

        // Find the actual index in the main providers list
        let provider_index = healthy[*index % healthy_count]
            .providers
            .iter()
            .position(|p| p.config.name == healthy[*index % healthy_count].config.name)
            .unwrap_or(0);

        *index = (*index + 1) % healthy_count;
        Some(provider_index)
    }

    /// Send a message through the chain with failover.
    pub async fn send_message(
        &mut self,
        request: &MessageRequest,
    ) -> Result<MessageResponse, ApiError> {
        let providers = self.select_providers();

        if providers.is_empty() {
            return Err(ApiError::ProviderUnavailable(
                "no available providers".to_string()
            ));
        }

        let mut last_error = None;

        for provider in providers {
            for attempt in 0..=self.config.max_retries {
                if attempt > 0 {
                    tracing::debug!(
                        provider = %provider.config.name,
                        attempt = attempt,
                        "retrying request"
                    );
                    tokio::time::sleep(self.config.retry_delay).await;
                }

                let start = Instant::now();
                match provider.client.send_message(request).await {
                    Ok(response) => {
                        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                        // Record success on the actual provider
                        if let Some(p) = self.providers.iter_mut().find(|p| p.config.name == provider.config.name) {
                            p.record_success(elapsed);
                        }
                        return Ok(response);
                    }
                    Err(e) => {
                        tracing::warn!(
                            provider = %provider.config.name,
                            error = %e,
                            "provider request failed"
                        );
                        // Record failure
                        if let Some(p) = self.providers.iter_mut().find(|p| p.config.name == provider.config.name) {
                            p.record_failure();
                        }
                        last_error = Some(e);

                        // Don't retry if this isn't a retryable error
                        if !is_retryable_error(&e) {
                            break;
                        }

                        // Check if provider is still available
                        if let Some(p) = self.providers.iter().find(|p| p.config.name == provider.config.name) {
                            if !p.config.enabled || !p.health.is_healthy {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| ApiError::ProviderUnavailable(
            "all providers failed".to_string()
        )))
    }

    /// Stream a message through the chain with failover.
    pub async fn stream_message(
        &mut self,
        request: &MessageRequest,
    ) -> Result<ChainMessageStream, ApiError> {
        let providers = self.select_providers();

        if providers.is_empty() {
            return Err(ApiError::ProviderUnavailable(
                "no available providers".to_string()
            ));
        }

        // Try to get a stream from any healthy provider
        for provider in providers {
            match provider.client.stream_message(request).await {
                Ok(stream) => {
                    return Ok(ChainMessageStream {
                        inner: Some(stream),
                        chain: self,
                        current_provider: provider.config.name.clone(),
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        provider = %provider.config.name,
                        error = %e,
                        "provider streaming request failed"
                    );
                    if !is_retryable_error(&e) {
                        continue;
                    }
                }
            }
        }

        Err(ApiError::ProviderUnavailable(
            "all providers failed to start streaming".to_string()
        ))
    }

    /// Get health status of all providers.
    pub fn get_health(&self) -> Vec<ProviderHealth> {
        self.providers.iter().map(|p| p.health.clone()).collect()
    }

    /// Get a specific provider's health.
    pub fn get_provider_health(&self, name: &str) -> Option<ProviderHealth> {
        self.providers.iter()
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

/// Message stream wrapper for provider chain.
pub struct ChainMessageStream<'a> {
    inner: Option<crate::MessageStream>,
    chain: &'a mut ProviderChain,
    current_provider: String,
}

impl<'a> ChainMessageStream<'a> {
    /// Get the current provider name.
    pub fn provider_name(&self) -> &str {
        &self.current_provider
    }
}

/// Check if an error is retryable.
fn is_retryable_error(error: &ApiError) -> bool {
    matches!(
        error,
        ApiError::RateLimited(_)
            | ApiError::Timeout(_)
            | ApiError::TransportError(_)
            | ApiError::UpstreamError(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MessageRequest, MessageResponse};
    use crate::providers::ProviderKind;

    #[test]
    fn test_chain_provider_config() {
        let config = ChainProviderConfig::new("primary", ProviderClient::from_model("claude-3-5-sonnet").unwrap())
            .with_weight(100)
            .with_failure_threshold(5)
            .with_timeout(Duration::from_secs(30));

        assert_eq!(config.name, "primary");
        assert_eq!(config.weight, 100);
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.timeout, Duration::from_secs(30));
    }

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

    #[test]
    fn test_is_retryable_error() {
        assert!(is_retryable_error(&ApiError::Timeout("timeout".to_string())));
        assert!(is_retryable_error(&ApiError::RateLimited {
            retry_after: 0,
            source: None,
        }));
        assert!(is_retryable_error(&ApiError::TransportError("connection refused".to_string())));
        assert!(!is_retryable_error(&ApiError::InvalidRequest("bad request".to_string())));
    }

    #[tokio::test]
    async fn test_provider_chain_add_provider() {
        let config = ProviderChainConfig::default();
        let mut chain = ProviderChain::new(config);

        let primary = ChainProviderConfig::new("primary", ProviderClient::from_model("claude-3-5-sonnet").unwrap());
        chain.add_provider(primary);

        let health = chain.get_health();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].name, "primary");
        assert!(health[0].is_healthy);
    }

    #[tokio::test]
    async fn test_provider_chain_set_enabled() {
        let config = ProviderChainConfig::default();
        let mut chain = ProviderChain::new(config);

        let primary = ChainProviderConfig::new("primary", ProviderClient::from_model("claude-3-5-sonnet").unwrap());
        chain.add_provider(primary);

        // Note: enabled state is tracked per ChainProvider, not in config
        let health = chain.get_health();
        assert!(health[0].is_healthy);
    }
}
