use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecallRequest {
    pub session_id: String,
    pub agent_id: String,
    pub query: String,
    pub max_items: usize,
    pub max_tokens: u64,
}
