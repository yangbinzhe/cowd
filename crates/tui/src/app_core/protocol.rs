use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeWorkGraphSummary {
    pub graph_id: Option<String>,
    pub board_id: Option<String>,
    pub status: String,
    pub agent_tasks: usize,
    pub memory_candidates: usize,
    pub conflicts: usize,
    pub completion_rate: Option<f32>,
    pub synthesis_lift: Option<f32>,
    pub complementarity_score: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePolicyDecisionSummary {
    pub level: String,
    pub score: u16,
    pub recommended_profile: String,
    pub agent_mode: String,
    pub requires_review: bool,
    pub signal_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CowdEvent {
    TextDelta {
        text: String,
    },
    ThinkingDelta {
        thinking: String,
    },
    ThinkingComplete,
    SignatureDelta {
        signature: String,
    },
    ToolStart {
        id: String,
        name: String,
        preview: String,
    },
    ToolProgress {
        id: String,
        name: String,
        progress: String,
    },
    ToolComplete {
        id: String,
        name: String,
        summary: String,
        exit_code: Option<i32>,
    },
    ToolExecuted {
        name: String,
        duration_ms: u64,
    },
    TurnStarted,
    TurnComplete {
        assistant_text: String,
        iterations: u32,
    },
    TurnError {
        error: String,
    },
    ContextWindow(u64),
    ContextEnvelope {
        envelope: Value,
    },
    RuntimePolicyDecision {
        summary: RuntimePolicyDecisionSummary,
    },
    WorkGraphSummary {
        summary: RuntimeWorkGraphSummary,
    },
    Warning {
        message: String,
    },
    TokenUsage {
        input: u64,
        output: u64,
        cache_create: u64,
        cache_read: u64,
    },
    CompactionNotice {
        removed_count: usize,
    },
    SessionCreated {
        id: String,
        name: String,
    },
    SessionDeleted {
        id: String,
    },
    SessionSwitched {
        id: String,
        name: String,
    },
    SessionList {
        sessions: Vec<(String, String, String)>,
    },
    MemoryEntry {
        layer: String,
        content: String,
        relevance: f64,
    },
    MemoryUpdate {
        entries: Vec<(String, String, f64)>,
        status: String,
    },
    MemoryStats {
        total_entries: usize,
        vector_count: usize,
        layers: Vec<String>,
    },
    MemoryExtracted {
        count: usize,
    },
    ApprovalRequested {
        tool: String,
    },
}
