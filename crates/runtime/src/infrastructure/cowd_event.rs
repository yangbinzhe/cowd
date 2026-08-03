//! Unified event bus for all Cowd domain events.
//! Single entry point replacing runtime::bus::Event + TuiEvent.
//! Field names aligned with TuiEvent for zero-conversion migration.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Runtime-owned relationship between a nested execution and the root
/// Session execution that surfaces are observing. This metadata is attached
/// only while a child event crosses into its parent's live stream; child
/// Runtime contracts and durable graph bindings remain independent.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CowdExecutionLineage {
    pub parent_execution_id: String,
    pub graph_id: String,
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
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

/// Runtime-owned identity for one causally ordered, user-visible model item.
/// Provider block indexes are transport hints only; this identity remains
/// stable across Gateway relays, Surface reconnects and durable replay.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CausalItemIdentity {
    pub model_step_id: String,
    pub item_id: String,
    pub segment_id: String,
    pub causal_sequence: u64,
    pub delta_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub causal_parent_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CausalItemKind {
    Text,
    PublicReasoning,
    ToolCall,
}

/// Public lifecycle phases for a delegated Agent execution.
///
/// This is a bounded projection contract, not a second Agent state machine.
/// The authoritative lifecycle remains in `AgentRuntime`; these values make
/// that lifecycle observable through the existing Session event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLifecyclePhase {
    Started,
    FirstOutput,
    Evaluating,
    Completed,
    Failed,
    Cancelled,
    Blocked,
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
    /// A nested Agent/Team event forwarded to the root Session event stream.
    /// The inner event retains its own execution identity, while the lineage
    /// proves why the root observer is authorized to consume it.
    RelatedExecution {
        lineage: CowdExecutionLineage,
        event: Box<CowdEvent>,
    },
    /// Causal item metadata is an orthogonal envelope. Domain reducers can
    /// still match the inner event, while Gateway and Surfaces receive the
    /// exact Runtime identity without guessing a fixed `part_id`.
    Causal {
        identity: CausalItemIdentity,
        event: Box<CowdEvent>,
    },
    ModelStepStarted {
        model_step_id: String,
    },
    ItemStarted {
        kind: CausalItemKind,
    },
    ItemCompleted {
        kind: CausalItemKind,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_input: Option<String>,
    },
    ModelStepCompleted {
        model_step_id: String,
        status: String,
    },
    /// A small, user-visible projection of one delegated Agent run.
    /// Team/graph/node ownership is carried by `RelatedExecution` lineage.
    AgentLifecycle {
        run_id: String,
        agent_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        role: Option<String>,
        phase: AgentLifecyclePhase,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    // Streaming — field names match TuiEvent
    TextDelta {
        text: String,
    },
    ReasoningSummaryDelta {
        summary: String,
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
    /// Request-local capacity and routing fact known before the first provider
    /// byte. This is distinct from billed usage, which remains unknown until a
    /// provider Usage event arrives.
    ProviderAttempt {
        model: String,
        models_tried: Vec<String>,
        context_window_tokens: u64,
        context_window_source: String,
        packed_input_tokens: u64,
    },
    /// Canonical write-capable tool targets observed by the graph owner. The
    /// live reducer reconciles this at finalization so preview parsing cannot
    /// undercount same-bytes writes or paths rejected before execution.
    WriteAttemptsObserved {
        paths: Vec<String>,
    },
    ContextEnvelope {
        envelope: crate::context_runtime::ContextEnvelope,
    },
    RuntimePolicyDecision {
        summary: RuntimePolicyDecisionSummary,
    },
    CapabilityAssessed {
        assessment: harness_contract::policy::CapabilityAssessment,
    },
    AuthorizationLeaseTransition {
        transition: harness_contract::policy::AuthorizationLeaseTransition,
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
        /// Stable identity of the approval gate request. Replayed live events
        /// must carry the same value so projections can deduplicate them.
        #[serde(default)]
        request_id: String,
        tool: String,
    },
    ApprovalResolved {
        request_id: String,
        status: harness_contract::policy::ApprovalStatus,
        scope: Option<harness_contract::policy::ApprovalGrantScope>,
        actor_id: Option<String>,
    },
}

impl CowdEvent {
    #[must_use]
    pub fn execution_context(&self) -> Option<&CowdExecutionContext> {
        match self {
            Self::ExecutionScoped { context, .. } => Some(context),
            Self::RelatedExecution { event, .. } | Self::Causal { event, .. } => {
                event.execution_context()
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn execution_lineage(&self) -> Option<&CowdExecutionLineage> {
        match self {
            Self::RelatedExecution { lineage, .. } => Some(lineage),
            Self::ExecutionScoped { event, .. } | Self::Causal { event, .. } => {
                event.execution_lineage()
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn causal_identity(&self) -> Option<&CausalItemIdentity> {
        match self {
            Self::ExecutionScoped { event, .. } | Self::RelatedExecution { event, .. } => {
                event.causal_identity()
            }
            Self::Causal { identity, .. } => Some(identity),
            _ => None,
        }
    }

    #[must_use]
    pub fn domain_event(&self) -> &CowdEvent {
        match self {
            Self::ExecutionScoped { event, .. }
            | Self::RelatedExecution { event, .. }
            | Self::Causal { event, .. } => event.domain_event(),
            event => event,
        }
    }

    fn is_execution_scoped(&self) -> bool {
        matches!(
            self,
            Self::ExecutionScoped { .. } | Self::RelatedExecution { .. }
        )
    }
}

#[derive(Clone)]
struct CowdEventForward {
    tx: broadcast::Sender<CowdEvent>,
    lineage: CowdExecutionLineage,
}

#[derive(Clone)]
pub struct CowdEventBus {
    tx: broadcast::Sender<CowdEvent>,
    forwards: Arc<Mutex<Vec<CowdEventForward>>>,
    execution_context: Arc<Mutex<Option<CowdExecutionContext>>>,
    model_step_sequence: Arc<AtomicU64>,
    causal_sequence: Arc<AtomicU64>,
    latest_model_step_id: Arc<Mutex<Option<String>>>,
    tool_identities: Arc<Mutex<HashMap<String, CausalItemIdentity>>>,
}

/// Clears the bus execution context even when the owning turn future is
/// cancelled or dropped before its normal cleanup path runs.
pub struct CowdExecutionScope {
    execution_context: Arc<Mutex<Option<CowdExecutionContext>>>,
    context: CowdExecutionContext,
}

impl Default for CowdEventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl CowdEventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            tx,
            forwards: Arc::new(Mutex::new(Vec::new())),
            execution_context: Arc::new(Mutex::new(None)),
            model_step_sequence: Arc::new(AtomicU64::new(0)),
            causal_sequence: Arc::new(AtomicU64::new(0)),
            latest_model_step_id: Arc::new(Mutex::new(None)),
            tool_identities: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<CowdEvent> {
        self.tx.subscribe()
    }

    #[must_use]
    pub fn current_execution_context(&self) -> Option<CowdExecutionContext> {
        self.execution_context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Forward this child bus into one root Session bus while preserving the
    /// child's execution identity. The target receives only an immutable
    /// relationship envelope, so concurrent child Runtime instances never
    /// share mutable execution context or causal counters.
    pub fn forward_to(&self, target: &Self, lineage: CowdExecutionLineage) {
        if self.tx.same_channel(&target.tx) {
            return;
        }
        let mut forwards = self
            .forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if forwards
            .iter()
            .any(|forward| forward.tx.same_channel(&target.tx) && forward.lineage == lineage)
        {
            return;
        }
        forwards.push(CowdEventForward {
            tx: target.tx.clone(),
            lineage,
        });
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
        let _ = self.tx.send(event.clone());
        let forwards = self
            .forwards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for forward in forwards {
            let _ = forward.tx.send(CowdEvent::RelatedExecution {
                lineage: forward.lineage,
                event: Box::new(event.clone()),
            });
        }
    }

    /// Allocate a stable step ID at the Runtime boundary. Provider adapters do
    /// not own this sequence because retries and fallback providers are still
    /// part of the same Runtime execution.
    #[must_use]
    pub fn next_model_step_id(&self) -> String {
        let sequence = self
            .model_step_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let execution_id = self
            .execution_context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|context| context.execution_id.as_str())
            .unwrap_or("unscoped")
            .to_string();
        let model_step_id = format!("{execution_id}:model-step:{sequence}");
        *self
            .latest_model_step_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(model_step_id.clone());
        model_step_id
    }

    #[must_use]
    pub fn next_causal_sequence(&self) -> u64 {
        self.causal_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    pub fn emit_causal(&self, identity: CausalItemIdentity, event: CowdEvent) {
        self.emit(CowdEvent::Causal {
            identity,
            event: Box::new(event),
        });
    }

    /// Publish visible text that does not originate from a Provider stream
    /// (for example, an already committed Team terminal) through the same
    /// typed item lifecycle as normal model output.
    pub fn emit_synthetic_text_item(&self, source: &str, text: &str) {
        let model_step_id = self.next_model_step_id();
        self.emit(CowdEvent::ModelStepStarted {
            model_step_id: model_step_id.clone(),
        });
        let item_id = format!("{model_step_id}:item:{source}");
        let mut identity = CausalItemIdentity {
            model_step_id: model_step_id.clone(),
            item_id: item_id.clone(),
            segment_id: format!("{item_id}:text:0"),
            causal_sequence: self.next_causal_sequence(),
            delta_sequence: 0,
            tool_call_id: None,
            causal_parent_ids: Vec::new(),
        };
        self.emit_causal(
            identity.clone(),
            CowdEvent::ItemStarted {
                kind: CausalItemKind::Text,
            },
        );
        identity.delta_sequence = 1;
        self.emit_causal(
            identity.clone(),
            CowdEvent::TextDelta {
                text: text.to_string(),
            },
        );
        identity.delta_sequence = 2;
        self.emit_causal(
            identity,
            CowdEvent::ItemCompleted {
                kind: CausalItemKind::Text,
                tool_name: None,
                tool_input: None,
            },
        );
        self.emit(CowdEvent::ModelStepCompleted {
            model_step_id,
            status: "completed".to_string(),
        });
    }

    /// Emit the single canonical start event for a governed tool invocation.
    /// The identity remains stable across progress and terminal events.
    pub fn emit_tool_started(&self, id: &str, name: &str, preview: &str) {
        self.emit_tool_started_with_dependencies(id, name, preview, &[]);
    }

    /// Emit a governed tool start together with the dependency edges already
    /// compiled by Runtime's canonical tool DAG.
    pub fn emit_tool_started_with_dependencies(
        &self,
        id: &str,
        name: &str,
        preview: &str,
        causal_parent_ids: &[String],
    ) {
        self.emit(CowdEvent::ExecutionPhase {
            status: harness_contract::projection::ExecutionLiveStatus::CallingTool,
            detail: Some(name.to_string()),
        });
        let identity = self.next_tool_identity(id, causal_parent_ids);
        self.emit_causal(
            identity,
            CowdEvent::ToolStart {
                id: id.to_string(),
                name: name.to_string(),
                preview: preview.to_string(),
            },
        );
    }

    pub fn emit_tool_completed(&self, id: &str, name: &str, summary: &str, exit_code: Option<i32>) {
        self.emit_tool_completed_with_dependencies(id, name, summary, exit_code, &[]);
    }

    pub fn emit_tool_completed_with_dependencies(
        &self,
        id: &str,
        name: &str,
        summary: &str,
        exit_code: Option<i32>,
        causal_parent_ids: &[String],
    ) {
        let identity = self.next_tool_identity(id, causal_parent_ids);
        self.emit_causal(
            identity,
            CowdEvent::ToolComplete {
                id: id.to_string(),
                name: name.to_string(),
                summary: summary.to_string(),
                exit_code,
            },
        );
    }

    fn next_tool_identity(
        &self,
        tool_call_id: &str,
        causal_parent_ids: &[String],
    ) -> CausalItemIdentity {
        let execution_id = self
            .execution_context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|context| context.execution_id.clone())
            .unwrap_or_else(|| "unscoped".to_string());
        let model_step_id = self
            .latest_model_step_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .filter(|step_id| step_id.starts_with(&format!("{execution_id}:model-step:")))
            .unwrap_or_else(|| format!("{execution_id}:model-step:unknown"));
        let key = format!("{execution_id}:{model_step_id}:{tool_call_id}");
        let mut identities = self
            .tool_identities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let identity = identities.entry(key).or_insert_with(|| CausalItemIdentity {
            model_step_id,
            item_id: tool_call_id.to_string(),
            segment_id: format!("{tool_call_id}:tool-execution:0"),
            causal_sequence: self.next_causal_sequence(),
            delta_sequence: 0,
            tool_call_id: Some(tool_call_id.to_string()),
            causal_parent_ids: causal_parent_ids.to_vec(),
        });
        if identity.causal_parent_ids.is_empty() && !causal_parent_ids.is_empty() {
            identity.causal_parent_ids = causal_parent_ids.to_vec();
        }
        identity.delta_sequence = identity.delta_sequence.saturating_add(1);
        identity.clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn unwrap_scoped_causal(
        event: CowdEvent,
    ) -> (CowdExecutionContext, CausalItemIdentity, CowdEvent) {
        let CowdEvent::ExecutionScoped { context, event } = event else {
            panic!("expected execution-scoped event");
        };
        let CowdEvent::Causal { identity, event } = *event else {
            panic!("expected causal event");
        };
        (context, identity, *event)
    }

    #[tokio::test]
    async fn governed_tool_lifecycle_preserves_identity_and_dependencies() {
        let bus = CowdEventBus::new();
        let mut events = bus.subscribe();
        let context = CowdExecutionContext {
            execution_id: "execution-a".to_string(),
            session_id: "session-a".to_string(),
            turn_id: "turn-a".to_string(),
        };
        let _scope = bus.enter_execution(context.clone());
        let model_step_id = bus.next_model_step_id();
        let dependencies = vec!["tool-a".to_string(), "tool-b".to_string()];

        bus.emit_tool_started_with_dependencies("tool-c", "read", "input", &dependencies);
        let phase = events.recv().await.expect("tool phase");
        assert!(matches!(
            phase.domain_event(),
            CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::CallingTool,
                ..
            }
        ));
        bus.emit_tool_completed_with_dependencies("tool-c", "read", "done", Some(0), &dependencies);

        let (start_context, start_identity, start_event) =
            unwrap_scoped_causal(events.recv().await.expect("tool start"));
        let (complete_context, complete_identity, complete_event) =
            unwrap_scoped_causal(events.recv().await.expect("tool complete"));

        assert_eq!(start_context, context);
        assert_eq!(complete_context, context);
        assert_eq!(start_identity.model_step_id, model_step_id);
        assert_eq!(start_identity.item_id, "tool-c");
        assert_eq!(start_identity.tool_call_id.as_deref(), Some("tool-c"));
        assert_eq!(start_identity.causal_parent_ids, dependencies);
        assert_eq!(
            start_identity.causal_sequence,
            complete_identity.causal_sequence
        );
        assert_eq!(
            complete_identity.delta_sequence,
            start_identity.delta_sequence + 1
        );
        assert_eq!(
            start_identity.causal_parent_ids,
            complete_identity.causal_parent_ids
        );
        assert!(matches!(start_event, CowdEvent::ToolStart { id, .. } if id == "tool-c"));
        assert!(matches!(
            complete_event,
            CowdEvent::ToolComplete { id, exit_code: Some(0), .. } if id == "tool-c"
        ));
    }

    #[tokio::test]
    async fn delegated_agent_lifecycle_keeps_child_identity_and_parent_lineage() {
        let parent = CowdEventBus::new();
        let child = CowdEventBus::new();
        let mut events = parent.subscribe();
        child.forward_to(
            &parent,
            CowdExecutionLineage {
                parent_execution_id: "root-execution".to_string(),
                graph_id: "team-graph".to_string(),
                node_id: "researcher:1".to_string(),
                team_id: Some("team-1".to_string()),
                agent_id: Some("agent-1".to_string()),
            },
        );
        let _scope = child.enter_execution(CowdExecutionContext {
            execution_id: "agent-run-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
        });

        child.emit(CowdEvent::AgentLifecycle {
            run_id: "agent-run-1".to_string(),
            agent_id: "agent-1".to_string(),
            role: Some("researcher".to_string()),
            phase: AgentLifecyclePhase::Started,
            status: "running".to_string(),
            summary: None,
        });

        let event = events.recv().await.expect("forwarded lifecycle");
        assert_eq!(
            event
                .execution_context()
                .map(|context| context.execution_id.as_str()),
            Some("agent-run-1")
        );
        assert_eq!(
            event
                .execution_lineage()
                .map(|lineage| lineage.parent_execution_id.as_str()),
            Some("root-execution")
        );
        assert!(matches!(
            event.domain_event(),
            CowdEvent::AgentLifecycle {
                phase: AgentLifecyclePhase::Started,
                role: Some(role),
                ..
            } if role == "researcher"
        ));
    }

    #[tokio::test]
    async fn repeated_provider_tool_id_does_not_merge_across_model_steps() {
        let bus = CowdEventBus::new();
        let mut events = bus.subscribe();
        let _scope = bus.enter_execution(CowdExecutionContext {
            execution_id: "execution-a".to_string(),
            session_id: "session-a".to_string(),
            turn_id: "turn-a".to_string(),
        });

        let first_step = bus.next_model_step_id();
        bus.emit_tool_started("provider-call-1", "read", "first");
        let _ = events.recv().await.expect("first phase");
        let (_, first_identity, _) =
            unwrap_scoped_causal(events.recv().await.expect("first start"));

        let second_step = bus.next_model_step_id();
        bus.emit_tool_started("provider-call-1", "read", "second");
        let _ = events.recv().await.expect("second phase");
        let (_, second_identity, _) =
            unwrap_scoped_causal(events.recv().await.expect("second start"));

        assert_eq!(first_identity.model_step_id, first_step);
        assert_eq!(second_identity.model_step_id, second_step);
        assert_ne!(
            first_identity.causal_sequence,
            second_identity.causal_sequence
        );
        assert_eq!(first_identity.delta_sequence, 1);
        assert_eq!(second_identity.delta_sequence, 1);
    }

    #[tokio::test]
    async fn child_bus_forwards_scoped_events_with_parent_lineage() {
        let parent = CowdEventBus::new();
        let child = CowdEventBus::new();
        let mut parent_events = parent.subscribe();
        let lineage = CowdExecutionLineage {
            parent_execution_id: "root-execution".to_string(),
            graph_id: "team-graph".to_string(),
            node_id: "researcher:1".to_string(),
            team_id: Some("team-run".to_string()),
            agent_id: Some("researcher".to_string()),
        };
        child.forward_to(&parent, lineage.clone());
        let child_context = CowdExecutionContext {
            execution_id: "agent-run".to_string(),
            session_id: "session-a".to_string(),
            turn_id: "turn-a".to_string(),
        };
        let _scope = child.enter_execution(child_context.clone());

        child.emit(CowdEvent::ExecutionPhase {
            status: harness_contract::projection::ExecutionLiveStatus::CallingModel,
            detail: Some("delegated research".to_string()),
        });

        let forwarded = parent_events.recv().await.expect("forwarded event");
        assert_eq!(forwarded.execution_context(), Some(&child_context));
        assert_eq!(forwarded.execution_lineage(), Some(&lineage));
        assert!(matches!(
            forwarded.domain_event(),
            CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::CallingModel,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn related_execution_preserves_causal_tool_identity() {
        let parent = CowdEventBus::new();
        let child = CowdEventBus::new();
        let mut parent_events = parent.subscribe();
        child.forward_to(
            &parent,
            CowdExecutionLineage {
                parent_execution_id: "root-execution".to_string(),
                graph_id: "team-graph".to_string(),
                node_id: "researcher:1".to_string(),
                team_id: Some("team-run".to_string()),
                agent_id: Some("researcher".to_string()),
            },
        );
        let _scope = child.enter_execution(CowdExecutionContext {
            execution_id: "agent-run".to_string(),
            session_id: "session-a".to_string(),
            turn_id: "turn-a".to_string(),
        });
        child.emit_tool_started("provider-call-1", "read_file", "README.md");
        let _phase = parent_events.recv().await.expect("tool phase");
        let forwarded = parent_events.recv().await.expect("tool start");

        assert_eq!(
            forwarded
                .causal_identity()
                .and_then(|identity| identity.tool_call_id.as_deref()),
            Some("provider-call-1")
        );
    }
}
