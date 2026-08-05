//! Runtime adapter for Memory's model summarization port.

use std::sync::Arc;

use memory::compression::llm_summarizer::{LlmSummarizer, LlmSummarizerError};

use crate::{
    ProviderClientTemplateCache, ProviderRegistry, ProviderRuntimeClient, ProviderTransportPool,
};

/// Routes Memory summarization through Runtime's governed Provider client.
#[derive(Clone)]
pub struct RuntimeMemorySummarizer {
    client: ProviderRuntimeClient,
    model: String,
    max_tokens: u32,
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
        })
    }
}

#[async_trait::async_trait]
impl LlmSummarizer for RuntimeMemorySummarizer {
    async fn summarize(&self, prompt: &str, content: &str) -> Result<String, LlmSummarizerError> {
        let completion = self
            .client
            .complete_control_analysis(
                &self.model,
                prompt.to_string(),
                content.to_string(),
                self.max_tokens,
            )
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
