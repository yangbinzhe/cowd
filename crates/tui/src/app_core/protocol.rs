use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

use crate::runtime_control_store::MfgOperationsSnapshot;

pub use harness_contract::projection::{
    ExecutionCommandReceipt, ExecutionCommandRequest, ExecutionProjection, ProjectionDelta,
};

/// TUI-side durable cursor guard. It deliberately does not infer graph or
/// session lifecycle from textual events: a gap requests a new canonical
/// snapshot from Gateway.
#[derive(Debug, Clone, Default)]
pub struct ExecutionProjectionReducer {
    execution_id: Option<String>,
    cursor: u64,
    seen_event_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionDeltaApply {
    Applied,
    ResyncRequired,
}

impl ExecutionProjectionReducer {
    pub fn install_snapshot(&mut self, projection: &ExecutionProjection) {
        self.execution_id = Some(projection.execution_id.clone());
        self.cursor = projection.cursor;
        self.seen_event_ids.clear();
    }

    pub fn apply_delta(&mut self, delta: &ProjectionDelta) -> ProjectionDeltaApply {
        if self.execution_id.as_deref() != Some(delta.execution_id.as_str())
            || self.cursor != delta.base_cursor
            || delta.target_cursor < delta.base_cursor
        {
            return ProjectionDeltaApply::ResyncRequired;
        }
        for event in &delta.events {
            if event.commit_cursor < delta.base_cursor || event.commit_cursor > delta.target_cursor
            {
                return ProjectionDeltaApply::ResyncRequired;
            }
            self.seen_event_ids.insert(event.event_id.clone());
        }
        self.cursor = delta.target_cursor;
        ProjectionDeltaApply::Applied
    }

    #[must_use]
    pub const fn cursor(&self) -> u64 {
        self.cursor
    }
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
    ResourcesCommitted {
        ids: Vec<String>,
    },
    /// Runtime-owned SessionIngress queue snapshot. The payload remains a
    /// shared-contract JSON value at this boundary so TUI can evolve its
    /// presentation without creating a second queue authority.
    SessionInputProjection {
        projection: Value,
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
    ExecutionGraphSummary {
        summary: RuntimeExecutionGraphSummary,
    },
    ExecutionProjectionDelta {
        delta: ProjectionDelta,
    },
    ExecutionProjectionLoaded {
        projection: ExecutionProjection,
    },
    RuntimeBacklinkResolved {
        target: String,
        object: Value,
    },
    RuntimeBacklinkFailed {
        target: String,
        message: String,
    },
    ApprovalBacklinkResolved {
        target: String,
        object: Value,
    },
    ApprovalBacklinkFailed {
        target: String,
        message: String,
    },
    SurfaceBacklinkResolved {
        target: String,
        receipt: Value,
    },
    SurfaceBacklinkFailed {
        target: String,
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
    MfgContract {
        generation: u64,
        contract: app_mfg_contract::MfgFrontendContractV1,
    },
    MfgSnapshot {
        generation: u64,
        snapshot: MfgOperationsSnapshot,
    },
    MfgReadFailed {
        generation: u64,
        section: String,
        error: app_mfg_contract::MfgApiErrorV1,
    },
    MfgActionAccepted {
        intent_id: String,
        response: app_mfg_contract::MfgMutationResponseV1,
    },
    MfgActionFailed {
        intent_id: String,
        error: app_mfg_contract::MfgApiErrorV1,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::ExecutionGraph;
    use harness_contract::projection::{
        ExecutionProjection, ProjectionCommandAvailability, ProjectionEvent, ProjectionEventKind,
    };

    fn snapshot() -> ExecutionProjection {
        ExecutionProjection {
            schema_version: 1,
            execution_id: "graph-a".to_string(),
            revision: 1,
            cursor: 10,
            session_id: None,
            mission_id: None,
            strategy: None,
            graph: harness_contract::execution_graph::project_execution_graph(
                &ExecutionGraph::new("test"),
            ),
            child_executions: Vec::new(),
            goals: Vec::new(),
            agents: Vec::new(),
            teams: Vec::new(),
            relations: Vec::new(),
            approvals: Vec::new(),
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
            schema_version: 1,
            execution_id: "graph-a".to_string(),
            base_cursor: 10,
            target_cursor: 11,
            events: vec![ProjectionEvent {
                commit_cursor: 11,
                transaction_index: 0,
                event_id: "event-11".to_string(),
                kind: ProjectionEventKind::CursorAdvanced,
                entity: None,
            }],
        };
        assert_eq!(reducer.apply_delta(&delta), ProjectionDeltaApply::Applied);
        assert_eq!(reducer.cursor(), 11);
        assert_eq!(
            reducer.apply_delta(&delta),
            ProjectionDeltaApply::ResyncRequired
        );
    }
}
