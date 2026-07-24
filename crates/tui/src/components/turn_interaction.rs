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
    pub started_at_ms: Option<u64>,
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

/// Every mutation of the active-turn presentation state.  Keeping this
/// explicit makes stream prose incapable of becoming a hidden lifecycle
/// transition and gives reconnect/resync paths a small, testable reducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnInteractionAction {
    SubmitStarted,
    IngressAccepted { execution_id: String },
    ProjectionSnapshot(ExecutionProjection),
    RevisionGap,
    ReconnectStarted,
    Disconnected,
    TerminalObserved,
    Reset,
}

impl TurnInteractionState {
    pub fn reduce(&mut self, action: TurnInteractionAction) {
        match action {
            TurnInteractionAction::SubmitStarted => self.submit_started(),
            TurnInteractionAction::IngressAccepted { execution_id } => {
                self.ingress_accepted(execution_id);
            }
            TurnInteractionAction::ProjectionSnapshot(projection) => {
                self.projection_snapshot(&projection);
            }
            TurnInteractionAction::RevisionGap => {
                if !matches!(self.transport, TransportState::Idle) {
                    self.presentation.stale = true;
                    self.transport = TransportState::Reconnecting;
                }
            }
            TurnInteractionAction::ReconnectStarted => self.reconnecting(),
            TurnInteractionAction::Disconnected => self.disconnected(),
            TurnInteractionAction::TerminalObserved => self.terminal_observed(),
            TurnInteractionAction::Reset => *self = Self::default(),
        }
    }

    pub fn submit_started(&mut self) {
        self.transport = TransportState::Submitting;
        self.presentation.stale = false;
        self.execution = ExecutionViewState::default();
    }

    pub fn ingress_accepted(&mut self, execution_id: impl Into<String>) {
        let execution_id = execution_id.into();
        // A newly admitted execution must never inherit the previous turn's
        // revision, live status or terminal reference while its first
        // projection snapshot is still loading.
        if self.execution.execution_id.as_deref() != Some(execution_id.as_str()) {
            self.execution = ExecutionViewState::default();
        }
        self.transport = TransportState::Accepted;
        self.execution.execution_id = Some(execution_id);
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
            self.execution.started_at_ms = Some(live.started_at_ms);
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

    /// Remove every display-derived fact for a projection that Gateway no
    /// longer authorizes.  Returning false makes a delayed revoke for an old
    /// selection harmless.
    pub fn clear_execution_if_matches(&mut self, execution_id: &str) -> bool {
        if self.execution.execution_id.as_deref() != Some(execution_id) {
            return false;
        }
        self.reduce(TurnInteractionAction::Reset);
        true
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
                .map(status_label)
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
                .map(status_label)
                .unwrap_or_else(|| "Chat".to_string()),
        }
    }
}

fn status_label(status: ExecutionLiveStatus) -> String {
    let label = match status {
        ExecutionLiveStatus::Queued => "Queued",
        ExecutionLiveStatus::PreparingContext => "Preparing context",
        ExecutionLiveStatus::CallingModel => "Thinking",
        ExecutionLiveStatus::Thinking => "Thinking",
        ExecutionLiveStatus::CallingTool => "Running tools",
        ExecutionLiveStatus::WaitingApproval => "Waiting for approval",
        ExecutionLiveStatus::Finalizing => "Finalizing",
        ExecutionLiveStatus::Complete => "Completed",
        ExecutionLiveStatus::Error => "Failed",
        ExecutionLiveStatus::Cancelled => "Cancelled",
    };
    format!("Runtime: {label}")
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
                output_preview_start_bytes: 0,
                output_bytes: 0,
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

    #[test]
    fn revision_gap_is_visible_until_a_projection_snapshot_repairs_it() {
        let mut state = TurnInteractionState::default();
        state.reduce(TurnInteractionAction::SubmitStarted);
        state.reduce(TurnInteractionAction::IngressAccepted {
            execution_id: "execution-a".to_string(),
        });
        state.reduce(TurnInteractionAction::RevisionGap);
        assert_eq!(state.transport, TransportState::Reconnecting);
        assert!(state.presentation.stale);

        state.reduce(TurnInteractionAction::ProjectionSnapshot(projection(
            8,
            ExecutionLiveStatus::CallingModel,
        )));
        assert_eq!(state.transport, TransportState::Accepted);
        assert!(!state.presentation.stale);
    }

    #[test]
    fn authorization_revoke_clears_all_execution_presentation_facts() {
        let mut state = TurnInteractionState::default();
        state.reduce(TurnInteractionAction::IngressAccepted {
            execution_id: "execution-a".to_string(),
        });
        state.reduce(TurnInteractionAction::ProjectionSnapshot(projection(
            8,
            ExecutionLiveStatus::CallingModel,
        )));

        assert!(state.clear_execution_if_matches("execution-a"));
        assert_eq!(state, TurnInteractionState::default());
        assert_eq!(state.label(), "Chat");
        assert!(!state.clear_execution_if_matches("execution-a"));
    }

    #[test]
    fn new_ingress_does_not_reuse_prior_execution_detail_while_loading() {
        let mut state = TurnInteractionState::default();
        state.reduce(TurnInteractionAction::ProjectionSnapshot(projection(
            8,
            ExecutionLiveStatus::CallingModel,
        )));
        state.reduce(TurnInteractionAction::IngressAccepted {
            execution_id: "execution-b".to_string(),
        });

        assert_eq!(state.execution.execution_id.as_deref(), Some("execution-b"));
        assert_eq!(state.execution.revision, None);
        assert_eq!(state.execution.status_detail, None);
        assert_eq!(state.execution.terminal_ref, None);
        assert_eq!(state.label(), "Accepted by Runtime");
    }
}
