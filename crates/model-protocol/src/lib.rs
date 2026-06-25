use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod model_registry;
pub mod oauth;
pub mod prompt_cache;
pub mod provider_config;
pub mod telemetry;
pub mod usage;

#[cfg(test)]
pub(crate) fn test_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("env lock poisoned")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelToolSpec {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelRequest {
    pub model: String,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ModelToolSpec>,
    pub stream: bool,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelResponse {
    pub id: String,
    pub model: String,
    pub content: String,
    pub usage: Option<ModelUsage>,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelStreamEvent {
    TextDelta {
        text: String,
    },
    ToolCall {
        id: String,
        name: String,
        input: Value,
    },
    Usage {
        usage: ModelUsage,
    },
    Done,
    Error {
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCapability {
    pub model: String,
    pub supports_streaming: bool,
    pub supports_tool_calls: bool,
    pub context_window_tokens: Option<u64>,
}

pub trait ModelProviderPort: Send + Sync {
    fn capabilities(&self) -> Vec<ModelCapability>;
}
