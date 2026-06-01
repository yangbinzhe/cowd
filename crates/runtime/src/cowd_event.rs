//! Unified event bus for all Cowd domain events.
//! Single entry point replacing runtime::bus::Event + TuiEvent.
//! Field names aligned with TuiEvent for zero-conversion migration.

use tokio::sync::broadcast;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CowdEvent {
    // Streaming — field names match TuiEvent
    TextDelta { text: String },
    ThinkingDelta { thinking: String },
    ThinkingComplete,
    SignatureDelta { signature: String },
    // Tool lifecycle
    ToolStart { id: String, name: String, preview: String },
    ToolProgress { id: String, name: String, progress: String },
    ToolComplete { id: String, name: String, summary: String, exit_code: Option<i32> },
    ToolExecuted { name: String, duration_ms: u64 },
    // Turn lifecycle
    TurnStarted,
    TurnComplete { assistant_text: String, iterations: u32 },
    TurnError { error: String },
    ContextWindow(u64),
    // System
    Warning { message: String },
    TokenUsage { input: u64, output: u64, cache_create: u64, cache_read: u64 },
    CompactionNotice { removed_count: usize },
    // Session
    SessionCreated { id: String, name: String },
    SessionDeleted { id: String },
    SessionSwitched { id: String, name: String },
    SessionList { sessions: Vec<(String, String, String)> },
    // Memory
    MemoryEntry { layer: String, content: String, relevance: f64 },
    MemoryUpdate { entries: Vec<(String, String, f64)>, status: String },
    MemoryStats { total_entries: usize, vector_count: usize, layers: Vec<String> },
    MemoryExtracted { count: usize },
    // Approval
    ApprovalRequested { tool: String },
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
