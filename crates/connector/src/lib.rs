use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorAccount {
    pub id: String,
    pub provider: String,
    pub label: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorCapability {
    pub id: String,
    pub provider: String,
    pub operation: String,
    pub tool_name: Option<String>,
    pub risk: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorResourceRef {
    pub provider: String,
    pub resource_type: String,
    pub id: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorRequest {
    pub capability_id: String,
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorResponse {
    pub output: Value,
    pub receipts: Vec<Value>,
}
