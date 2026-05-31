use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;



/// Retryable HTTP status codes that trigger a fallback switch.
const RETRYABLE_STATUSES: &[&str] = &["429", "500", "502", "503", "504"];

/// Provider fallback chain that automatically retries on retryable errors
/// across a primary model and its ordered list of fallback models.
pub struct FallbackChain {
    pub primary: String,
    pub fallbacks: Vec<String>,
    max_retries_per_model: u32,
    attempts: AtomicU32,
}

impl FallbackChain {
    pub fn new(primary: &str, fallbacks: &[String]) -> Self {
        Self {
            primary: primary.to_string(),
            fallbacks: fallbacks.to_vec(),
            max_retries_per_model: 8,
            attempts: AtomicU32::new(0),
        }
    }

    #[must_use]
    pub fn with_max_retries_per_model(mut self, max: u32) -> Self {
        self.max_retries_per_model = max;
        self
    }

    /// Build a fallback chain from the runtime config for the given model.
    ///
    /// The `fallbacks` list contains alternative model names. The primary is
    /// the requested model itself (not a config-driven primary).
    pub fn from_config(fallbacks: &[String], model: &str) -> Self {
        Self::new(model, fallbacks)
    }

    /// Iterates primary → fallbacks, calling `send_fn` for each model.
    /// On retryable errors (429/500/502/503/504), retries with exponential
    /// backoff up to `max_retries_per_model` times before switching to the
    /// next fallback. Non-retryable errors are returned immediately.
    ///
    /// Returns the first successful result, or an error when all models are
    /// exhausted.
    pub async fn try_send<F, Fut, T, E>(&self, mut send_fn: F) -> Result<T, E>
    where
        F: FnMut(&str) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut last_error: Option<E> = None;
        let models = std::iter::once(&self.primary).chain(self.fallbacks.iter());
        for model in models {
            for attempt in 0..self.max_retries_per_model {
                self.attempts.fetch_add(1, Ordering::Relaxed);
                match send_fn(model).await {
                    Ok(result) => {
                        let total = self.attempts.load(Ordering::Relaxed);
                        if total > 1 {
                            tracing::warn!(
                                model,
                                fallback_attempts = total,
                                "provider fallback succeeded"
                            );
                        }
                        return Ok(result);
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if is_retryable_status(&err_str) {
                            last_error = Some(e);
                            if attempt == self.max_retries_per_model - 1 {
                                tracing::warn!(
                                    model,
                                    attempt,
                                    "model exhausted retries, switching fallback"
                                );
                                break;
                            }
                            let backoff = Duration::from_millis(500 * 2u64.pow(attempt));
                            tokio::time::sleep(backoff).await;
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }
        Err(last_error.expect("all models exhausted without capturing an error"))
    }

    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts.load(Ordering::Relaxed)
    }
}

#[inline]
fn is_retryable_status(err_str: &str) -> bool {
    RETRYABLE_STATUSES
        .iter()
        .any(|code| err_str.contains(code))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn primary_success_no_fallback() {
        let chain = FallbackChain::new("primary", &["fallback-1".to_string()]);
        let result = chain
            .try_send(|model| {
                let m = model.to_string();
                async move { Ok::<_, String>(m) }
            })
            .await;
        assert_eq!(result, Ok("primary".to_string()));
        assert_eq!(chain.attempts(), 1);
    }

    #[tokio::test]
    async fn primary_429_switches_to_fallback() {
        let mut call_count = 0u32;
        let chain = FallbackChain::new("primary", &["fallback-1".to_string()])
            .with_max_retries_per_model(1); // no retries, switch immediately

        let result = chain
            .try_send(|model| {
                call_count += 1;
                let m = model.to_string();
                async move {
                    if m == "primary" {
                        Err("HTTP 429 Too Many Requests".to_string())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        assert_eq!(result, Ok("fallback-1".to_string()));
        assert_eq!(call_count, 2); // primary + fallback
        assert!(chain.attempts() >= 2);
    }

    #[tokio::test]
    async fn all_fallbacks_exhausted_returns_error() {
        let chain = FallbackChain::new("primary", &["fb1".to_string(), "fb2".to_string()])
            .with_max_retries_per_model(1);

        let result = chain
            .try_send(|model| {
                let m = model.to_string();
                async move { Err::<String, _>(format!("{m} returned 503")) }
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn non_retryable_error_returns_immediately() {
        let mut call_count = 0u32;
        let chain = FallbackChain::new("primary", &["fallback-1".to_string()]);

        let result = chain
            .try_send(|model| {
                call_count += 1;
                let _m = model.to_string();
                async move { Err::<String, _>("authentication error 401".to_string()) }
            })
            .await;
        assert!(result.is_err());
        // Should NOT try fallback for non-retryable error
        assert_eq!(call_count, 1);
    }

    #[tokio::test]
    async fn retries_with_backoff_inside_same_model() {
        use std::sync::atomic::AtomicU32;
        use std::sync::Arc;
        let calls = Arc::new(AtomicU32::new(0));
        let chain = FallbackChain::new("primary", &["fallback-1".to_string()])
            .with_max_retries_per_model(3);

        let calls_clone = Arc::clone(&calls);
        let result = chain
            .try_send(move |model| {
                let m = model.to_string();
                let c = Arc::clone(&calls_clone);
                async move {
                    if m == "primary" {
                        c.fetch_add(1, Ordering::Relaxed);
                        Err("HTTP 500 Internal Server Error".to_string())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        assert_eq!(result, Ok("fallback-1".to_string()));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn from_config_uses_fallbacks_list() {
        let fallbacks = vec!["fb1".to_string(), "fb2".to_string()];
        let chain = FallbackChain::from_config(&fallbacks, "primary");
        assert_eq!(chain.primary, "primary");
        assert_eq!(chain.fallbacks, vec!["fb1", "fb2"]);
    }

    #[tokio::test]
    async fn primary_fails_503_fallback_succeeds() {
        let chain = FallbackChain::new("primary", &["fb1".to_string()])
            .with_max_retries_per_model(1);

        let result = chain
            .try_send(|model| {
                let m = model.to_string();
                async move {
                    if m == "primary" {
                        Err("HTTP 503 Service Unavailable".to_string())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        assert_eq!(result, Ok("fb1".to_string()));
    }

    #[tokio::test]
    async fn primary_fails_504_fallback_succeeds() {
        let chain = FallbackChain::new("primary", &["fb1".to_string()])
            .with_max_retries_per_model(1);

        let result = chain
            .try_send(|model| {
                let m = model.to_string();
                async move {
                    if m == "primary" {
                        Err("HTTP 504 Gateway Timeout".to_string())
                    } else {
                        Ok(m)
                    }
                }
            })
            .await;
        assert_eq!(result, Ok("fb1".to_string()));
    }
}
