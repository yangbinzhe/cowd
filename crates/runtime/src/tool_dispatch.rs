use crate::session::ConversationMessage;
use crate::tool_orchestrator::{ToolSafetyCategory, ToolSafetyRegistry};
use std::collections::HashMap;

pub struct ToolRequest {
    pub tool_use_id: String,
    pub tool_name: String,
    pub input: String,
    /// Tool IDs that must complete before this tool can execute (wave orchestration).
    pub depends_on: Vec<String>,
}

pub struct ToolDispatchResult {
    pub tool_use_id: String,
    pub message: Result<ConversationMessage, String>,
    pub category: ToolSafetyCategory,
    pub duration_ms: u64,
}

pub fn categorize(requests: &[ToolRequest]) -> (Vec<usize>, Vec<usize>) {
    let registry = ToolSafetyRegistry::global();
    let mut read_only: Vec<usize> = Vec::new();
    let mut rest: Vec<usize> = Vec::new();
    for (i, req) in requests.iter().enumerate() {
        match registry.classify(&req.tool_name) {
            ToolSafetyCategory::ReadOnly => read_only.push(i),
            _ => rest.push(i),
        }
    }
    (read_only, rest)
}

pub fn reorder_in_original(
    mut results: HashMap<String, ToolDispatchResult>,
    ordered_ids: &[String],
) -> Vec<ToolDispatchResult> {
    let mut out = Vec::with_capacity(results.len());
    for id in ordered_ids {
        if let Some(r) = results.remove(id) {
            out.push(r);
        }
    }
    out
}
