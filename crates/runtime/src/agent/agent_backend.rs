use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentExecutionBackendKind {
    InProcess,
    ProcessJsonl,
    ManualMailbox,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBackendCapability {
    pub backend: AgentExecutionBackendKind,
    pub supports_input: bool,
    pub supports_interrupt: bool,
    pub supports_cancel: bool,
    pub supports_shutdown: bool,
    pub supports_status: bool,
    pub command_channel_attached: bool,
    pub mode: String,
}

impl Default for AgentExecutionBackendKind {
    fn default() -> Self {
        Self::InProcess
    }
}

impl AgentExecutionBackendKind {
    #[must_use]
    pub fn capability(self, command_channel_attached: bool) -> AgentBackendCapability {
        AgentBackendCapability {
            backend: self,
            supports_input: command_channel_attached,
            supports_interrupt: command_channel_attached,
            supports_cancel: true,
            supports_shutdown: command_channel_attached,
            supports_status: true,
            command_channel_attached,
            mode: match (self, command_channel_attached) {
                (Self::ProcessJsonl, true) => "process-jsonl-command-channel",
                (Self::ProcessJsonl, false) => "process-jsonl-observe-only",
                (Self::InProcess, true) => "in-process-command-channel",
                (Self::InProcess, false) => "in-process-one-shot",
                (Self::ManualMailbox, _) => "manual-mailbox",
            }
            .to_string(),
        }
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
