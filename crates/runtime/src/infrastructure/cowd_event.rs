//! Unified event bus for all Cowd domain events.
//! Single entry point replacing runtime::bus::Event + TuiEvent.
//! Field names aligned with TuiEvent for zero-conversion migration.

use tokio::sync::broadcast;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

impl RuntimeWorkGraphSummary {
    #[must_use]
    pub fn from_review(
        graph: &crate::agent_workgraph::AgentWorkGraph,
        packet: &crate::agent_collaboration::CollaborationReviewPacket,
    ) -> Self {
        let agent_tasks = graph
            .nodes
            .iter()
            .filter(|node| node.kind == crate::agent_workgraph::WorkGraphNodeKind::AgentTask)
            .count();
        Self {
            graph_id: Some(graph.graph_id.clone()),
            board_id: graph
                .board_id
                .clone()
                .or_else(|| Some(packet.board_id.clone())),
            status: format!("{:?}", graph.status).to_lowercase(),
            agent_tasks,
            memory_candidates: packet.maintenance_candidates.len(),
            conflicts: packet.scorecard.conflict_count,
            completion_rate: Some(packet.scorecard.completion_rate),
            synthesis_lift: Some(packet.scorecard.synthesis_lift),
            complementarity_score: Some(packet.scorecard.complementarity_score),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimePolicyDecisionSummary {
    pub level: String,
    pub score: u16,
    pub recommended_profile: String,
    pub agent_mode: String,
    pub requires_review: bool,
    pub signal_count: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RunModelTelemetry {
    pub model: Option<String>,
    pub models_used: Vec<String>,
    pub first_token_latency_ms: Option<u64>,
    pub active_stream_duration_ms: Option<u64>,
    pub wall_duration_ms: u64,
    pub output_chars: u64,
    pub output_chunks: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_create_tokens: u64,
    pub cache_read_tokens: u64,
    pub total_tokens: u64,
    pub usage_source: String,
    pub chars_per_second: Option<f64>,
    pub tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CowdEvent {
    // Streaming — field names match TuiEvent
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
    // Tool lifecycle
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
    // Turn lifecycle
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
        envelope: crate::context_runtime::ContextEnvelope,
    },
    RuntimePolicyDecision {
        summary: RuntimePolicyDecisionSummary,
    },
    WorkGraphSummary {
        summary: RuntimeWorkGraphSummary,
    },
    // System
    Warning {
        message: String,
    },
    TokenUsage {
        input: u64,
        output: u64,
        cache_create: u64,
        cache_read: u64,
    },
    RunModelTelemetry {
        telemetry: RunModelTelemetry,
    },
    CompactionNotice {
        removed_count: usize,
    },
    // Session
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
    // Memory
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
    // Approval
    ApprovalRequested {
        tool: String,
    },
}

#[derive(Clone)]
pub struct CowdEventBus {
    tx: broadcast::Sender<CowdEvent>,
}

impl CowdEventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self { tx }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<CowdEvent> {
        self.tx.subscribe()
    }
    pub fn emit(&self, event: CowdEvent) {
        let _ = self.tx.send(event);
    }
}
