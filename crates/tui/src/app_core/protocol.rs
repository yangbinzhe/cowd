use serde::{Deserialize, Serialize};
use serde_json::Value;

use cowd_app_host::TuiAppEvent;

pub use harness_contract::projection::{
    ExecutionCommandReceipt, ExecutionCommandRequest, ExecutionLiveUpdate, ExecutionProjection,
    ProjectionDelta, SessionExecutionIndexProjection, SessionHistoryIndexProjection,
    SessionHistoryRecoveryState,
};

/// Stable identity carried by Gateway session events.
///
/// None of these fields are inferred from visible text. A Runtime event may be
/// transient, but its session/execution/turn ownership is still explicit.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayEventCorrelation {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_step_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_sequence: Option<u64>,
    #[serde(default)]
    pub causal_parent_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_cursor: Option<u64>,
    #[serde(default)]
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GatewaySessionEvent {
    UserMessageCommitted {
        correlation: GatewayEventCorrelation,
        content: String,
        sequence: usize,
        created_at_ms: u64,
    },
    TextDelta {
        correlation: GatewayEventCorrelation,
        text: String,
        start_bytes: usize,
        end_bytes: usize,
        stream_revision: u64,
    },
    ReasoningSummaryDelta {
        correlation: GatewayEventCorrelation,
        summary: String,
    },
    ModelStepStarted {
        correlation: GatewayEventCorrelation,
    },
    ModelStepCompleted {
        correlation: GatewayEventCorrelation,
        status: String,
    },
    ItemStarted {
        correlation: GatewayEventCorrelation,
        kind: String,
    },
    ItemCompleted {
        correlation: GatewayEventCorrelation,
        kind: String,
    },
    ToolStart {
        correlation: GatewayEventCorrelation,
        id: String,
        name: String,
        preview: String,
    },
    ToolProgress {
        correlation: GatewayEventCorrelation,
        id: String,
        name: String,
        progress: String,
    },
    ToolComplete {
        correlation: GatewayEventCorrelation,
        id: String,
        name: String,
        summary: String,
        exit_code: Option<i32>,
    },
    ExecutionPhase {
        correlation: GatewayEventCorrelation,
        status: harness_contract::projection::ExecutionLiveStatus,
        detail: Option<String>,
    },
    ProviderAttempt {
        correlation: GatewayEventCorrelation,
        model: String,
        models_tried: Vec<String>,
        context_window_tokens: u64,
        context_window_source: String,
        packed_input_tokens: u64,
    },
    ContextEnvelope {
        correlation: GatewayEventCorrelation,
        envelope: Value,
    },
    ContextWindow {
        correlation: GatewayEventCorrelation,
        value: u64,
    },
    TokenUsage {
        correlation: GatewayEventCorrelation,
        input: u64,
        output: u64,
        cache_create: u64,
        cache_read: u64,
    },
    RunModelTelemetry {
        correlation: GatewayEventCorrelation,
        telemetry: RunModelTelemetryProjection,
    },
    TerminalCommitted {
        correlation: GatewayEventCorrelation,
        assistant_text: String,
        sequence: Option<usize>,
        iterations: u32,
        token_usage: Option<Value>,
    },
    TurnError {
        correlation: GatewayEventCorrelation,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessageProjection {
    pub id: String,
    pub session_id: String,
    pub sequence: usize,
    pub role: String,
    #[serde(default)]
    pub blocks: Vec<Value>,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_usage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
}

impl SessionMessageProjection {
    #[must_use]
    pub fn visible_text(&self) -> String {
        self.blocks
            .iter()
            .filter_map(|block| {
                let block_type = block.get("type").and_then(Value::as_str);
                matches!(block_type, Some("text") | None)
                    .then(|| block.get("text").and_then(Value::as_str))
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("")
    }

    #[must_use]
    pub fn turn_id(&self) -> Option<&str> {
        self.blocks.iter().find_map(|block| {
            block
                .get("cowd_turn_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
        })
    }

    #[must_use]
    pub fn execution_id(&self) -> Option<&str> {
        self.blocks.iter().find_map(|block| {
            block
                .get("cowd_execution_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMessagesPage {
    pub session_id: String,
    #[serde(default)]
    pub messages: Vec<SessionMessageProjection>,
    pub total: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_seq: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_seq: Option<usize>,
    pub limit: usize,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionHistoryHydrationKind {
    InitialWindow,
    IncrementalCatchup,
}

/// Provider-observed model telemetry transported by Gateway.
///
/// This deliberately mirrors Runtime's public event payload without making
/// the presentation crate depend on Runtime internals.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunModelTelemetryProjection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub models_used: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_token_latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_stream_duration_ms: Option<u64>,
    #[serde(default)]
    pub wall_duration_ms: u64,
    #[serde(default)]
    pub output_chars: u64,
    #[serde(default)]
    pub output_chunks: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_create_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub usage_source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_chars_per_second: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_tokens_per_second: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_chars_per_second: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_tokens_per_second: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chars_per_second: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_per_second: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStreamConnectionState {
    Connecting,
    Connected,
    Reconnecting {
        attempt: u32,
        after_cursor: Option<u64>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionProjectionReducer {
    projection: Option<ExecutionProjection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionDeltaApply {
    Applied,
    ResyncRequired,
}

impl ExecutionProjectionReducer {
    pub fn install_snapshot(&mut self, projection: &ExecutionProjection) -> ProjectionDeltaApply {
        if validate_execution_projection_schema(projection).is_err() {
            self.projection = None;
            return ProjectionDeltaApply::ResyncRequired;
        }
        self.projection = Some(projection.clone());
        ProjectionDeltaApply::Applied
    }

    pub fn apply_delta(&mut self, delta: &ProjectionDelta) -> ProjectionDeltaApply {
        if validate_projection_delta_schema(delta).is_err() {
            return ProjectionDeltaApply::ResyncRequired;
        }
        let Some(current) = self.projection.as_ref() else {
            return ProjectionDeltaApply::ResyncRequired;
        };
        match harness_contract::projection::reduce_projection_delta(current, delta) {
            Ok(next) => {
                self.projection = Some(next);
                ProjectionDeltaApply::Applied
            }
            Err(_) => ProjectionDeltaApply::ResyncRequired,
        }
    }

    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.projection
            .as_ref()
            .map_or(0, |projection| projection.cursor)
    }

    #[must_use]
    pub const fn projection(&self) -> Option<&ExecutionProjection> {
        self.projection.as_ref()
    }
}

pub fn validate_execution_projection_schema(
    projection: &ExecutionProjection,
) -> Result<(), String> {
    if projection.schema_version
        != harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION
    {
        return Err(format!(
            "unsupported execution projection schema_version {}",
            projection.schema_version
        ));
    }
    if let Some(strategy) = projection.strategy.as_ref() {
        if strategy.schema_version
            != harness_contract::projection::STRATEGY_DECISION_PROJECTION_SCHEMA_VERSION
        {
            return Err(format!(
                "unsupported strategy projection schema_version {}",
                strategy.schema_version
            ));
        }
    }
    Ok(())
}

pub fn validate_projection_delta_schema(delta: &ProjectionDelta) -> Result<(), String> {
    if delta.schema_version != harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION
        || delta.reducer_version
            != harness_contract::projection::EXECUTION_PROJECTION_REDUCER_VERSION
    {
        return Err(format!(
            "unsupported execution projection delta schema/reducer {}/{}",
            delta.schema_version, delta.reducer_version
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Event emitted by one session SSE observer whose payload does not always
    /// carry correlation itself (usage/context/telemetry/warnings). Runner
    /// unwraps this only after routing it to the matching presentation state.
    SessionScoped {
        session_id: String,
        /// Monotonic authority epoch captured when the producer was started.
        /// A revoked/re-authorized session may reuse the same public ID, so
        /// session identity alone is not sufficient to reject late results.
        authority_generation: u64,
        event: Box<CowdEvent>,
    },
    GatewaySession {
        event: GatewaySessionEvent,
    },
    SessionHistoryPage {
        page: SessionMessagesPage,
    },
    /// Body-free activation/history navigation state loaded before transcript
    /// pages. This is the same typed Gateway contract consumed by WebUI.
    SessionHistoryIndexLoaded {
        projection: SessionHistoryIndexProjection,
    },
    /// Messages committed after the last accepted durable sequence. App may
    /// install these only while its visible window is at the durable tail;
    /// an operator browsing older history must keep a contiguous window and
    /// receive a "newer available" indication instead.
    SessionHistoryCatchupPage {
        page: SessionMessagesPage,
    },
    /// Completion facts for one bounded hydration pass. This is the typed
    /// owner for history performance/window telemetry; UI code must not infer
    /// these values from timeline length after eviction.
    SessionHistoryHydrated {
        session_id: String,
        kind: SessionHistoryHydrationKind,
        duration_ms: u64,
        message_count: usize,
        page_count: usize,
        oldest_offset: usize,
        total_messages: usize,
        next_sequence: usize,
        has_older: bool,
    },
    SessionHistoryOlderPage {
        page: SessionMessagesPage,
        oldest_offset: usize,
        has_older: bool,
    },
    SessionHistoryOlderFailed {
        session_id: String,
        error: String,
    },
    SessionHistoryNewerPage {
        page: SessionMessagesPage,
        window_end_offset: usize,
        has_newer: bool,
    },
    SessionHistoryLatestPage {
        page: SessionMessagesPage,
        oldest_offset: usize,
    },
    SessionHistoryHydrationFailed {
        session_id: String,
        error: String,
    },
    MessageAdmissionAccepted {
        session_id: String,
        client_message_id: String,
        submission_generation: u64,
    },
    MessageAdmissionFailed {
        session_id: String,
        client_message_id: String,
        submission_generation: u64,
        original_text: String,
        started_new_turn: bool,
        error: String,
    },
    SessionAuthorizationRevoked {
        session_id: String,
        reason: String,
    },
    SessionStreamConnection {
        session_id: String,
        state: SessionStreamConnectionState,
    },
    /// Transport state of the independently streamed canonical execution
    /// projection. Session prose may remain connected while this stream is
    /// stale, so it must never be folded into the session SSE state.
    ExecutionProjectionConnection {
        generation: u64,
        execution_id: String,
        state: SessionStreamConnectionState,
    },
    /// Canonical Mission/approval/team snapshot demultiplexed from the one
    /// physical Gateway live connection.
    MissionProjectionSnapshot {
        mission_id: String,
        snapshot: harness_contract::mission::MissionMaterializedSnapshot,
    },
    MissionProjectionDelta {
        mission_id: String,
        delta: harness_contract::mission::MissionProjectionDelta,
    },
    ReasoningSummaryDelta {
        summary: String,
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
    ResourcesCommitted {
        ids: Vec<String>,
    },
    ResourceUploaded {
        id: String,
        label: String,
        kind: String,
    },
    ResourceUploadFailed {
        path: String,
        error: String,
    },
    /// Runtime-owned SessionIngress queue snapshot. The payload remains a
    /// shared-contract JSON value at this boundary so TUI can evolve its
    /// presentation without creating a second queue authority.
    SessionInputProjection {
        projection: Value,
    },
    /// Runtime-owned result of applying one typed running-Turn input batch.
    SessionInputDispositionChanged {
        receipt: Value,
    },
    TurnError {
        error: String,
    },
    ContextWindow(u64),
    ProviderAttempt {
        model: String,
        models_tried: Vec<String>,
        context_window_tokens: u64,
        context_window_source: String,
        packed_input_tokens: u64,
    },
    ContextEnvelope {
        envelope: Value,
    },
    RuntimePolicyDecision {
        summary: RuntimePolicyDecisionSummary,
    },
    ExecutionGraphSummary {
        summary: RuntimeExecutionGraphSummary,
    },
    ExecutionProjectionDelta {
        generation: u64,
        delta: ProjectionDelta,
    },
    ExecutionProjectionLoaded {
        generation: u64,
        projection: ExecutionProjection,
    },
    ExecutionProjectionLive {
        generation: u64,
        update: ExecutionLiveUpdate,
    },
    /// A bounded background snapshot refresh failed.  It is separate from a
    /// display warning so the selected execution can release its one in-flight
    /// refresh slot without ever making the input/render loop await HTTP.
    ExecutionProjectionRefreshFailed {
        generation: u64,
        execution_id: String,
        message: String,
    },
    /// A stream established under an earlier credential or schema contract
    /// must never keep rendering its last full snapshot after Gateway rejects
    /// it.  The generation makes a delayed revoke harmless after a selection
    /// switch.
    ExecutionProjectionAccessRevoked {
        generation: u64,
        execution_id: String,
        message: String,
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
    RunModelTelemetry {
        telemetry: RunModelTelemetryProjection,
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
    ApprovalResolved {
        request_id: String,
        status: String,
        scope: Option<String>,
        actor_id: Option<String>,
    },
    /// Generic asynchronous result for a statically-linked APP terminal
    /// surface. The host routes it by panel id; the APP alone deserializes
    /// its domain contract and reduces the state.
    AppTui {
        panel_id: String,
        event: TuiAppEvent,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::ExecutionGraph;
    use harness_contract::projection::{
        ExecutionProjection, ProjectionCommandAvailability, ProjectionDetailScope,
        ProjectionOperation, ProjectionSourceHealth, EXECUTION_PROJECTION_REDUCER_VERSION,
        EXECUTION_PROJECTION_SCHEMA_VERSION,
    };

    fn snapshot() -> ExecutionProjection {
        ExecutionProjection {
            schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
            execution_id: "graph-a".to_string(),
            revision: 1,
            cursor: 10,
            detail_scope: ProjectionDetailScope::Summary,
            authorization_revision: 1,
            redaction_revision: "redaction-1".to_string(),
            session_id: None,
            mission_id: None,
            task_id: None,
            turn_id: None,
            strategy: None,
            graph: harness_contract::execution_graph::project_execution_graph(
                &ExecutionGraph::new("test"),
            ),
            child_executions: Vec::new(),
            activities: Vec::new(),
            activity_relations: Vec::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
            admissions: Vec::new(),
            outcomes: Vec::new(),
            interventions: Vec::new(),
            usage: Vec::new(),
            context: Vec::new(),
            evidence: Vec::new(),
            health: Vec::new(),
            recovery: Vec::new(),
            live: None,
            available_commands: Vec::<ProjectionCommandAvailability>::new(),
        }
    }

    #[test]
    fn projection_reducer_requires_a_contiguous_durable_cursor() {
        let projection = snapshot();
        let mut reducer = ExecutionProjectionReducer::default();
        reducer.install_snapshot(&projection);
        let delta = ProjectionDelta {
            schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION,
            reducer_version: EXECUTION_PROJECTION_REDUCER_VERSION,
            execution_id: "graph-a".to_string(),
            from_revision: 1,
            target_revision: 1,
            base_cursor: 10,
            target_cursor: 11,
            detail_scope: ProjectionDetailScope::Summary,
            authorization_revision: 1,
            redaction_revision: "redaction-1".to_string(),
            source_health: ProjectionSourceHealth::Fresh,
            operations: vec![ProjectionOperation::AdvanceCursor { cursor: 11 }],
            resync_reason: None,
        };
        assert_eq!(reducer.apply_delta(&delta), ProjectionDeltaApply::Applied);
        assert_eq!(reducer.cursor(), 11);
        assert_eq!(
            reducer.apply_delta(&delta),
            ProjectionDeltaApply::ResyncRequired
        );
        assert_eq!(reducer.cursor(), 11);
        assert_eq!(reducer.projection().map(|value| value.revision), Some(1));
    }

    #[test]
    fn projection_reducer_consumes_the_canonical_cross_surface_golden_corpus() {
        #[derive(Deserialize)]
        struct Corpus {
            initial: ExecutionProjection,
            delta: ProjectionDelta,
            expected: ExecutionProjection,
        }

        let corpus: Corpus = serde_json::from_str(include_str!(
            "../../../harness-contract/tests/fixtures/projection-v2/materialization.json"
        ))
        .expect("canonical projection v2 corpus");
        let mut reducer = ExecutionProjectionReducer::default();
        assert_eq!(
            reducer.install_snapshot(&corpus.initial),
            ProjectionDeltaApply::Applied
        );
        assert_eq!(
            reducer.apply_delta(&corpus.delta),
            ProjectionDeltaApply::Applied
        );
        assert_eq!(reducer.projection(), Some(&corpus.expected));
        assert_eq!(corpus.expected.admissions.len(), 1);
        assert_eq!(corpus.expected.outcomes.len(), 1);
        assert!(corpus.expected.evidence[0].payload.is_some());
    }

    #[test]
    fn projection_reducer_fails_closed_on_execution_delta_and_nested_strategy_versions() {
        let mut invalid_execution = snapshot();
        invalid_execution.schema_version = EXECUTION_PROJECTION_SCHEMA_VERSION + 1;
        let mut reducer = ExecutionProjectionReducer::default();
        assert_eq!(
            reducer.install_snapshot(&invalid_execution),
            ProjectionDeltaApply::ResyncRequired
        );

        let mut invalid_strategy = snapshot();
        invalid_strategy.strategy = Some(
            serde_json::from_value(serde_json::json!({
                "schema_version": 2,
                "id": "strategy-v2",
                "kind": "strategy_decision",
                "revision": 1,
                "evidence_refs": []
            }))
            .expect("future strategy wire remains deserializable for explicit rejection"),
        );
        assert_eq!(
            reducer.install_snapshot(&invalid_strategy),
            ProjectionDeltaApply::ResyncRequired
        );

        let projection = snapshot();
        assert_eq!(
            reducer.install_snapshot(&projection),
            ProjectionDeltaApply::Applied
        );
        assert_eq!(
            reducer.apply_delta(&ProjectionDelta {
                schema_version: EXECUTION_PROJECTION_SCHEMA_VERSION + 1,
                reducer_version: EXECUTION_PROJECTION_REDUCER_VERSION,
                execution_id: projection.execution_id,
                from_revision: projection.revision,
                target_revision: projection.revision,
                base_cursor: projection.cursor,
                target_cursor: projection.cursor,
                detail_scope: projection.detail_scope,
                authorization_revision: projection.authorization_revision,
                redaction_revision: projection.redaction_revision,
                source_health: ProjectionSourceHealth::Fresh,
                operations: Vec::new(),
                resync_reason: None,
            }),
            ProjectionDeltaApply::ResyncRequired
        );
    }
}
