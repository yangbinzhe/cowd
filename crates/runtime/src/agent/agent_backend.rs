use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentExecutionBackendKind {
    InProcess,
    ProcessJsonl,
}

impl Default for AgentExecutionBackendKind {
    fn default() -> Self {
        Self::InProcess
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExecutionCommand {
    #[serde(rename = "agentId")]
    pub agent_id: String,
    pub command: AgentExecutionCommandKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentProcessJsonlSpec {
    pub command: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentExecutionCommandKind {
    Spawn,
    Input,
    Interrupt,
    Cancel,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExecutionEventEnvelope {
    #[serde(rename = "agentId")]
    pub agent_id: String,
    pub backend: AgentExecutionBackendKind,
    #[serde(rename = "eventType")]
    pub event_type: String,
    #[serde(rename = "emittedAt")]
    pub emitted_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExecutionCommandReceipt {
    #[serde(rename = "agentId")]
    pub agent_id: String,
    pub backend: AgentExecutionBackendKind,
    pub command: AgentExecutionCommandKind,
    pub status: String,
    pub message: String,
}
