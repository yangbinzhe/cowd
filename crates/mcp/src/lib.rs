use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpServiceError {
    #[error("mcp server not found: {0}")]
    NotFound(String),
    #[error("mcp server unavailable: {0}")]
    Unavailable(String),
    #[error("mcp request failed: {0}")]
    Request(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportKind {
    Stdio,
    Sse,
    Http,
    WebSocket,
    Sdk,
    ManagedProxy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpServerProjection {
    pub name: String,
    pub transport: McpTransportKind,
    pub enabled: bool,
    pub status: String,
    pub auth_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolProjection {
    pub server: String,
    pub name: String,
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpResourceProjection {
    pub server: String,
    pub uri: String,
    pub name: Option<String>,
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolCallRequest {
    pub server: String,
    pub tool: String,
    pub input: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpToolCallReceipt {
    pub server: String,
    pub tool: String,
    pub ok: bool,
    pub output: serde_json::Value,
}

pub trait McpService: Send + Sync {
    fn list_servers(&self) -> Result<Vec<McpServerProjection>, McpServiceError>;
    fn server(&self, name: &str) -> Result<McpServerProjection, McpServiceError>;
    fn health(&self) -> Result<serde_json::Value, McpServiceError>;
    fn reload_config(&self) -> Result<serde_json::Value, McpServiceError>;
    fn list_tools(&self, server: Option<&str>) -> Result<Vec<McpToolProjection>, McpServiceError>;
    fn list_resources(
        &self,
        server: Option<&str>,
    ) -> Result<Vec<McpResourceProjection>, McpServiceError>;
    fn read_resource(
        &self,
        server: &str,
        uri: &str,
    ) -> Result<McpResourceProjection, McpServiceError>;
    fn call_tool(&self, request: McpToolCallRequest)
        -> Result<McpToolCallReceipt, McpServiceError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_projection_is_runtime_neutral() {
        let server = McpServerProjection {
            name: "filesystem".to_string(),
            transport: McpTransportKind::Stdio,
            enabled: true,
            status: "ready".to_string(),
            auth_state: None,
        };
        let value = serde_json::to_value(&server).unwrap();
        assert_eq!(value["transport"], "stdio");
        assert_eq!(value["name"], "filesystem");
    }
}
