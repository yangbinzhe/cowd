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
    pub active_root: Option<RootPresentationState>,
    pub root_closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootPresentationState {
    pub presentation_id: String,
    pub attempt_id: String,
    pub envelope_id: String,
    pub envelope_revision: u64,
    pub accepted_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresentationDeltaAdmission {
    Accepted,
    Duplicate,
    Gap,
    NotOwner,
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
        self.presentation = PresentationState::default();
        self.execution = ExecutionViewState::default();
    }

    pub fn ingress_accepted(&mut self, execution_id: impl Into<String>) {
        let execution_id = execution_id.into();
        // A newly admitted execution must never inherit the previous turn's
        // revision, live status or terminal reference while its first
        // projection snapshot is still loading.
        if self.execution.execution_id.as_deref() != Some(execution_id.as_str()) {
            self.execution = ExecutionViewState::default();
            self.presentation = PresentationState::default();
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
        self.presentation.active_root = None;
        self.presentation.root_closed = true;
    }

    /// Install one root answer owner. Repeated starts for the same attempt are
    /// idempotent and never reset the accepted byte cursor.
    pub fn begin_root_presentation(
        &mut self,
        presentation_id: String,
        attempt_id: String,
        envelope_id: String,
        envelope_revision: u64,
    ) -> bool {
        if let Some(active) = self.presentation.active_root.as_mut() {
            if active.presentation_id == presentation_id && active.attempt_id == attempt_id {
                if envelope_revision >= active.envelope_revision {
                    active.envelope_id = envelope_id;
                    active.envelope_revision = envelope_revision;
                }
                return false;
            }
        }
        self.presentation.active_root = Some(RootPresentationState {
            presentation_id,
            attempt_id,
            envelope_id,
            envelope_revision,
            accepted_bytes: 0,
        });
        self.presentation.root_closed = false;
        true
    }

    #[must_use]
    pub fn active_root_owner(&self) -> Option<(String, String)> {
        self.presentation
            .active_root
            .as_ref()
            .map(|active| (active.presentation_id.clone(), active.attempt_id.clone()))
    }

    pub fn admit_root_delta(
        &mut self,
        presentation_id: &str,
        attempt_id: &str,
        start_bytes: u64,
        end_bytes: u64,
    ) -> PresentationDeltaAdmission {
        let Some(active) = self.presentation.active_root.as_mut() else {
            return PresentationDeltaAdmission::NotOwner;
        };
        if active.presentation_id != presentation_id || active.attempt_id != attempt_id {
            return PresentationDeltaAdmission::NotOwner;
        }
        if end_bytes <= active.accepted_bytes {
            return PresentationDeltaAdmission::Duplicate;
        }
        if start_bytes != active.accepted_bytes || end_bytes < start_bytes {
            return PresentationDeltaAdmission::Gap;
        }
        active.accepted_bytes = end_bytes;
        PresentationDeltaAdmission::Accepted
    }

    pub fn end_root_presentation(&mut self, presentation_id: &str, attempt_id: &str) -> bool {
        let matches = self
            .presentation
            .active_root
            .as_ref()
            .is_some_and(|active| {
                active.presentation_id == presentation_id && active.attempt_id == attempt_id
            });
        if matches {
            self.presentation.active_root = None;
            self.presentation.root_closed = true;
        }
        matches
    }

    /// Reconcile a canonical snapshot that has no active root presentation.
    /// This is intentionally not `terminal_observed`: a dropped Abort may
    /// close only the presentation while the execution continues or retries.
    pub fn clear_root_presentation_from_snapshot(&mut self) -> bool {
        let changed = self.presentation.active_root.take().is_some();
        if !changed {
            return false;
        }
        self.presentation.root_closed = true;
        true
    }

    #[must_use]
    pub fn root_preview_closed(&self) -> bool {
        self.presentation.root_closed
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
            schema_version: harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION,
            execution_id: "execution-a".to_string(),
            revision,
            cursor: revision,
            detail_scope: harness_contract::projection::ProjectionDetailScope::Summary,
            authorization_revision: 1,
            redaction_revision: "redaction-1".to_string(),
            session_id: Some("session-a".to_string()),
            mission_id: None,
            task_id: None,
            turn_id: Some("turn-a".to_string()),
            strategy: None,
            graph: harness_contract::execution_graph::project_execution_graph(
                &ExecutionGraph::new("g"),
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
                latency: Default::default(),
                output_preview: None,
                output_preview_start_bytes: 0,
                output_bytes: 0,
                output_parts: Vec::new(),
                terminal_ref: status.is_terminal().then(|| "terminal-a".to_string()),
                error: None,
            }),
            delivery_envelope: None,
            terminal_presentation: None,
            cancellation_receipt: None,
            available_commands: Vec::<ProjectionCommandAvailability>::new(),
        }
    }

    #[test]
    fn root_presentation_owner_rejects_gaps_superseded_and_late_deltas() {
        let mut state = TurnInteractionState::default();
        assert!(state.begin_root_presentation(
            "presentation-1".to_string(),
            "attempt-1".to_string(),
            "envelope-1".to_string(),
            1,
        ));
        assert_eq!(
            state.admit_root_delta("presentation-1", "attempt-1", 0, 5),
            PresentationDeltaAdmission::Accepted
        );
        assert_eq!(
            state.admit_root_delta("presentation-1", "attempt-1", 0, 5),
            PresentationDeltaAdmission::Duplicate
        );
        assert_eq!(
            state.admit_root_delta("presentation-1", "attempt-1", 7, 9),
            PresentationDeltaAdmission::Gap
        );
        assert!(state.end_root_presentation("presentation-1", "attempt-1"));
        assert_eq!(
            state.admit_root_delta("presentation-1", "attempt-1", 5, 6),
            PresentationDeltaAdmission::NotOwner
        );
        assert!(state.root_preview_closed());
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
