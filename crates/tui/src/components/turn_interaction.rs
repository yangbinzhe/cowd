//! TUI-owned presentation state for one Runtime execution.
//!
//! It deliberately consumes canonical ingress/projection facts and never
//! infers a lifecycle phase from text deltas, thinking, or tool output.

use harness_contract::projection::{ExecutionLiveStatus, ExecutionProjection};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TransportState {
    #[default]
    Idle,
    Submitting,
    Accepted,
    Reconnecting,
    Disconnected,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionViewState {
    pub execution_id: Option<String>,
    pub revision: Option<u64>,
    pub status: Option<ExecutionLiveStatus>,
    pub status_detail: Option<String>,
    pub terminal_ref: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PresentationState {
    pub stale: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnInteractionState {
    pub transport: TransportState,
    pub execution: ExecutionViewState,
    pub presentation: PresentationState,
}

impl TurnInteractionState {
    pub fn submit_started(&mut self) {
        self.transport = TransportState::Submitting;
        self.presentation.stale = false;
        self.execution = ExecutionViewState::default();
    }

    pub fn ingress_accepted(&mut self, execution_id: impl Into<String>) {
        self.transport = TransportState::Accepted;
        self.execution.execution_id = Some(execution_id.into());
    }

    pub fn projection_snapshot(&mut self, projection: &ExecutionProjection) {
        if let Some(existing_revision) = self.execution.revision {
            if projection.revision < existing_revision {
                return;
            }
        }
        self.execution.execution_id = Some(projection.execution_id.clone());
        self.execution.revision = Some(projection.revision);
        self.presentation.stale = false;
        if let Some(live) = &projection.live {
            self.execution.status = Some(live.status);
            self.execution.status_detail = live.status_detail.clone();
            self.execution.terminal_ref = live.terminal_ref.clone();
            self.transport = if live.status.is_terminal() {
                TransportState::Idle
            } else {
                TransportState::Accepted
            };
        }
    }

    pub fn reconnecting(&mut self) {
        if !matches!(self.transport, TransportState::Idle) {
            self.transport = TransportState::Reconnecting;
            self.presentation.stale = true;
        }
    }

    pub fn disconnected(&mut self) {
        if !matches!(self.transport, TransportState::Idle) {
            self.transport = TransportState::Disconnected;
            self.presentation.stale = true;
        }
    }

    pub fn terminal_observed(&mut self) {
        self.transport = TransportState::Idle;
        self.presentation.stale = false;
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        !matches!(self.transport, TransportState::Idle)
            && !self
                .execution
                .status
                .is_some_and(ExecutionLiveStatus::is_terminal)
    }

    #[must_use]
    pub fn label(&self) -> String {
        match self.transport {
            TransportState::Submitting => "Submitting…".to_string(),
            TransportState::Reconnecting => "Reconnecting; syncing Runtime state…".to_string(),
            TransportState::Disconnected => "Disconnected; Runtime state may continue".to_string(),
            TransportState::Accepted => self
                .execution
                .status
                .map(|status| format!("Runtime: {status:?}"))
                .or_else(|| {
                    self.execution
                        .execution_id
                        .as_ref()
                        .map(|_| "Accepted by Runtime".to_string())
                })
                .unwrap_or_else(|| "Accepted by Runtime".to_string()),
            TransportState::Idle => self
                .execution
                .status
                .filter(|status| status.is_terminal())
                .map(|status| format!("Runtime: {status:?}"))
                .unwrap_or_else(|| "Chat".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::ExecutionGraph;
    use harness_contract::projection::{ExecutionLiveState, ProjectionCommandAvailability};

    fn projection(revision: u64, status: ExecutionLiveStatus) -> ExecutionProjection {
        ExecutionProjection {
            schema_version: 1,
            execution_id: "execution-a".to_string(),
            revision,
            cursor: revision,
            session_id: Some("session-a".to_string()),
            mission_id: None,
            strategy: None,
            graph: harness_contract::execution_graph::project_execution_graph(
                &ExecutionGraph::new("g"),
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
            live: Some(ExecutionLiveState {
                revision,
                status,
                status_detail: Some("test".to_string()),
                turn_id: Some("turn-a".to_string()),
                started_at_ms: 1,
                updated_at_ms: revision,
                last_progress_at_ms: revision,
                context_usage: None,
                metrics: Default::default(),
                output_preview: None,
                terminal_ref: status.is_terminal().then(|| "terminal-a".to_string()),
                error: None,
            }),
            available_commands: Vec::<ProjectionCommandAvailability>::new(),
        }
    }

    #[test]
    fn projection_is_monotonic_and_terminal_is_authoritative() {
        let mut state = TurnInteractionState::default();
        state.submit_started();
        assert_eq!(state.transport, TransportState::Submitting);
        state.ingress_accepted("execution-a");
        state.projection_snapshot(&projection(4, ExecutionLiveStatus::CallingModel));
        assert!(state.is_active());
        state.projection_snapshot(&projection(3, ExecutionLiveStatus::Queued));
        assert_eq!(state.execution.revision, Some(4));
        state.projection_snapshot(&projection(5, ExecutionLiveStatus::Complete));
        assert!(!state.is_active());
        assert_eq!(state.execution.terminal_ref.as_deref(), Some("terminal-a"));
    }
}
