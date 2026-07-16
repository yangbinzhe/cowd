//! Unified event bus for all Cowd domain events.
//! Single entry point replacing runtime::bus::Event + TuiEvent.
//! Field names aligned with TuiEvent for zero-conversion migration.

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

/// Stable Runtime identity attached to every event produced while one
/// execution owns a conversation host.  Transport consumers may use this for
/// routing, but Runtime remains the only reducer of lifecycle facts.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CowdExecutionContext {
    pub execution_id: String,
    pub session_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuntimeExecutionGraphSummary {
    pub graph_id: Option<String>,
    pub board_id: Option<String>,
    pub status: String,
    pub agent_tasks: usize,
    pub child_executions: usize,
    pub memory_candidates: usize,
    pub conflicts: usize,
    pub completion_rate: Option<f32>,
    pub synthesis_lift: Option<f32>,
    pub complementarity_score: Option<f32>,
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
    pub wall_chars_per_second: Option<f64>,
    pub wall_tokens_per_second: Option<f64>,
    pub active_chars_per_second: Option<f64>,
    pub active_tokens_per_second: Option<f64>,
    pub chars_per_second: Option<f64>,
    pub tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CowdEvent {
    /// An execution-correlated Runtime event.  `CowdEventBus` adds this
    /// envelope while an ingress turn owns the host; producers continue to
    /// emit the domain event itself and never guess a session's latest turn.
    ExecutionScoped {
        context: CowdExecutionContext,
        event: Box<CowdEvent>,
    },
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
    /// Authoritative phase transitions emitted by Runtime's graph/conversation
    /// owner. Gateway and Surface transports may project this event but must
    /// not infer a phase from stream text or tool prose.
    ExecutionPhase {
        status: harness_contract::projection::ExecutionLiveStatus,
        detail: Option<String>,
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
    ExecutionGraphSummary {
        summary: RuntimeExecutionGraphSummary,
    },
    SessionInputReceived {
        receipt: harness_contract::turn::SessionInputReceipt,
    },
    SessionInputProjection {
        projection: harness_contract::turn::SessionInputProjection,
    },
    TurnInboxUpdated {
        inbox: harness_contract::turn::TurnInboxSnapshot,
    },
    TurnInputCheckpointConsumed {
        checkpoint: harness_contract::turn::TurnInputCheckpoint,
        consumed: Vec<harness_contract::turn::TurnInboxItem>,
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

impl CowdEvent {
    #[must_use]
    pub fn execution_context(&self) -> Option<&CowdExecutionContext> {
        match self {
            Self::ExecutionScoped { context, .. } => Some(context),
            _ => None,
        }
    }

    #[must_use]
    pub fn domain_event(&self) -> &CowdEvent {
        match self {
            Self::ExecutionScoped { event, .. } => event.domain_event(),
            event => event,
        }
    }

    fn is_execution_scoped(&self) -> bool {
        matches!(self, Self::ExecutionScoped { .. })
    }
}

#[derive(Clone)]
pub struct CowdEventBus {
    tx: broadcast::Sender<CowdEvent>,
    execution_context: Arc<Mutex<Option<CowdExecutionContext>>>,
}

/// Clears the bus execution context even when the owning turn future is
/// cancelled or dropped before its normal cleanup path runs.
pub struct CowdExecutionScope {
    execution_context: Arc<Mutex<Option<CowdExecutionContext>>>,
    context: CowdExecutionContext,
}

impl CowdEventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            tx,
            execution_context: Arc::new(Mutex::new(None)),
        }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<CowdEvent> {
        self.tx.subscribe()
    }

    #[must_use]
    pub fn enter_execution(&self, context: CowdExecutionContext) -> CowdExecutionScope {
        *self
            .execution_context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(context.clone());
        CowdExecutionScope {
            execution_context: Arc::clone(&self.execution_context),
            context,
        }
    }

    pub fn emit(&self, event: CowdEvent) {
        let event = if event.is_execution_scoped() {
            event
        } else if let Some(context) = self
            .execution_context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            CowdEvent::ExecutionScoped {
                context,
                event: Box::new(event),
            }
        } else {
            event
        };
        let _ = self.tx.send(event);
    }
}

impl Drop for CowdExecutionScope {
    fn drop(&mut self) {
        let mut current = self
            .execution_context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if current.as_ref() == Some(&self.context) {
            *current = None;
        }
    }
}
