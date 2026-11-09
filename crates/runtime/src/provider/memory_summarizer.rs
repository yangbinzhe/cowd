//! Runtime adapter for Memory's model summarization port.

use std::sync::Arc;

use memory::compression::llm_summarizer::{LlmSummarizer, LlmSummarizerError};

use crate::{
    ProviderClientTemplateCache, ProviderRegistry, ProviderRuntimeClient, ProviderTransportPool,
};

type ResourceSource = Arc<
    dyn Fn() -> Option<(
            Arc<crate::execution_core::graph::ExecutionResourceManager>,
            Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
        )> + Send
        + Sync,
>;

/// Routes Memory summarization through Runtime's governed Provider client.
#[derive(Clone)]
pub struct RuntimeMemorySummarizer {
    client: ProviderRuntimeClient,
    model: String,
    max_tokens: u32,
    resource_source: Option<ResourceSource>,
}

impl RuntimeMemorySummarizer {
    pub fn new(
        registry: Arc<ProviderRegistry>,
        transport_pool: Arc<ProviderTransportPool>,
        template_cache: Arc<ProviderClientTemplateCache>,
        model: impl Into<String>,
        max_tokens: u32,
    ) -> Result<Self, String> {
        let model = model.into();
        let client = ProviderRuntimeClient::new_with_transport_and_template_cache(
            registry,
            transport_pool,
            template_cache,
            model.clone(),
            Vec::new(),
        )?;
        Ok(Self {
            client,
            model,
            max_tokens: max_tokens.max(1),
            resource_source: None,
        })
    }
    /// Read the embedding composition root's existing owner at call time.
    /// This callback carries no quota state and cannot admit or settle work.
    #[must_use]
    pub fn with_resource_source<F>(mut self, source: F) -> Self
    where
        F: Fn() -> Option<(
                Arc<crate::execution_core::graph::ExecutionResourceManager>,
                Arc<std::sync::RwLock<crate::ProviderResourceConfig>>,
            )> + Send
            + Sync
            + 'static,
    {
        self.resource_source = Some(Arc::new(source));
        self
    }
}

#[async_trait::async_trait]
impl LlmSummarizer for RuntimeMemorySummarizer {
    async fn summarize(&self, prompt: &str, content: &str) -> Result<String, LlmSummarizerError> {
        let (manager, config) = self
            .resource_source
            .as_ref()
            .and_then(|source| source())
            .ok_or_else(|| {
                LlmSummarizerError::Provider(
                    "Memory summarizer original Provider resource owner is unavailable".into(),
                )
            })?;
        let client = self.client.clone().with_control_resources(
            manager,
            config,
            crate::execution_core::graph::ExecutionServiceClass::Background,
            "memory-summarizer",
        );
        let completion = client
            .complete_control_analysis(&self.model, prompt, content, self.max_tokens)
            .await
            .map_err(LlmSummarizerError::Provider)?;
        non_empty_summary(completion.text)
    }
}

fn non_empty_summary(summary: String) -> Result<String, LlmSummarizerError> {
    if summary.trim().is_empty() {
        Err(LlmSummarizerError::EmptyResponse)
    } else {
        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use super::non_empty_summary;
    use memory::compression::llm_summarizer::LlmSummarizerError;

    #[tokio::test]
    async fn late_resource_binding_summarizes_through_the_original_capacity_one_owner() {
        use super::*;
        use crate::config::{ProviderConfig, ProvidersConfig};
        use crate::execution_core::graph::*;
        use std::collections::{BTreeMap, HashMap};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry = Arc::new(
            ProviderRegistry::new_with_context_windows(
                ProvidersConfig {
                    providers: HashMap::from([(
                        "memory-fixture".into(),
                        ProviderConfig {
                            reasoning_effort: None,
                            name: "memory-fixture".into(),
                            base_url: format!("http://{address}/v1"),
                            api_key: "fixture-only".into(),
                            models: vec!["memory-fixture".into()],
                            protocol: Some("completions".into()),
                            parallel_tool_calls: Default::default(),
                            early_tool_start: Default::default(),
                        },
                    )]),
                },
                BTreeMap::from([("memory-fixture".into(), 32768)]),
            )
            .unwrap(),
        );
        let one = crate::ProviderQuotaPolicy::new(1, 1, 1, 0);
        let config = crate::ProviderResourceConfig {
            global: one,
            fallback: one,
            ..Default::default()
        };
        let mut quotas = config.materialize(&registry.pin()).quotas;
        quotas.push((
            ExecutionResourceKind::ContextBytes,
            ResourceQuota::new(4 * 1024 * 1024, 4 * 1024 * 1024, 4 * 1024 * 1024).unwrap(),
        ));
        let manager = Arc::new(ExecutionResourceManager::new(quotas));
        let config = Arc::new(std::sync::RwLock::new(config));
        let source = Arc::new(std::sync::OnceLock::new());
        let reader = Arc::clone(&source);
        let summarizer = RuntimeMemorySummarizer::new(
            registry,
            Arc::new(ProviderTransportPool::default()),
            Arc::new(ProviderClientTemplateCache::default()),
            "memory-fixture",
            32,
        )
        .unwrap()
        .with_resource_source(move || reader.get().cloned());
        assert!(matches!(
            summarizer.summarize("system", "source").await,
            Err(LlmSummarizerError::Provider(_))
        ));
        assert_eq!(summarizer.client.wire_preparation_count(), 0);
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            0
        );
        assert!(source.set((Arc::clone(&manager), config)).is_ok());
        let observer = Arc::clone(&manager);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 2048];
            assert!(socket.read(&mut buffer).await.unwrap() > 0);
            assert_eq!(
                observer
                    .snapshot(&ExecutionResourceKind::Provider)
                    .unwrap()
                    .active_leases,
                1
            );
            assert!(
                observer
                    .snapshot(&ExecutionResourceKind::ContextBytes)
                    .unwrap()
                    .active_leases
                    > 0
            );
            let body = r#"{"id":"summary","model":"memory-fixture","choices":[{"message":{"role":"assistant","content":"verified summary"},"finish_reason":"stop"}]}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        });
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                summarizer.summarize("system", "source")
            )
            .await
            .unwrap()
            .unwrap(),
            "verified summary"
        );
        server.await.unwrap();
        assert_eq!(summarizer.client.wire_preparation_count(), 1);
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::Provider)
                .unwrap()
                .active_leases,
            0
        );
        assert_eq!(
            manager
                .snapshot(&ExecutionResourceKind::ContextBytes)
                .unwrap()
                .active_leases,
            0
        );
    }

    #[test]
    fn empty_provider_summary_preserves_memory_fallback_semantics() {
        assert!(matches!(
            non_empty_summary(" \n".to_string()),
            Err(LlmSummarizerError::EmptyResponse)
        ));
        assert_eq!(
            non_empty_summary("durable summary".to_string()).unwrap(),
            "durable summary"
        );
    }
}
