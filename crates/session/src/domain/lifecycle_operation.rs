use serde::{Deserialize, Serialize};

use crate::{SessionError, SessionResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionCloseDisposition {
    Archive,
    Delete,
}

impl SessionCloseDisposition {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Archive => "archive",
            Self::Delete => "delete",
        }
    }

    pub fn parse(value: &str) -> SessionResult<Self> {
        match value {
            "archive" => Ok(Self::Archive),
            "delete" => Ok(Self::Delete),
            other => Err(SessionError::Store(format!(
                "unknown Session close disposition `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLifecyclePhase {
    Planned,
    AdmissionFenced,
    RuntimeDrained,
    TombstoneCommitted,
    Unloaded,
    Failed,
}

impl SessionLifecyclePhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::AdmissionFenced => "admission_fenced",
            Self::RuntimeDrained => "runtime_drained",
            Self::TombstoneCommitted => "tombstone_committed",
            Self::Unloaded => "unloaded",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Unloaded)
    }

    #[must_use]
    pub const fn is_stable(self) -> bool {
        !matches!(self, Self::Failed)
    }

    #[must_use]
    pub const fn successor(self) -> Option<Self> {
        match self {
            Self::Planned => Some(Self::AdmissionFenced),
            Self::AdmissionFenced => Some(Self::RuntimeDrained),
            Self::RuntimeDrained => Some(Self::TombstoneCommitted),
            Self::TombstoneCommitted => Some(Self::Unloaded),
            Self::Unloaded | Self::Failed => None,
        }
    }

    pub fn parse(value: &str) -> SessionResult<Self> {
        match value {
            "planned" => Ok(Self::Planned),
            "admission_fenced" => Ok(Self::AdmissionFenced),
            "runtime_drained" => Ok(Self::RuntimeDrained),
            "tombstone_committed" => Ok(Self::TombstoneCommitted),
            "unloaded" => Ok(Self::Unloaded),
            "failed" => Ok(Self::Failed),
            other => Err(SessionError::Store(format!(
                "unknown Session lifecycle phase `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleIntent {
    pub operation_id: String,
    pub session_id: String,
    pub disposition: SessionCloseDisposition,
    pub phase: SessionLifecyclePhase,
    /// Last completed non-failure phase. A failed operation resumes from this
    /// exact durable checkpoint rather than reconstructing progress from
    /// mutable Session state.
    pub last_stable_phase: SessionLifecyclePhase,
    pub expected_generation: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_error: Option<String>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecyclePlan {
    pub operation_id: String,
    pub session_id: String,
    pub disposition: SessionCloseDisposition,
    pub expected_generation: u64,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleTransition {
    pub operation_id: String,
    pub expected_revision: u64,
    pub expected_phase: SessionLifecyclePhase,
    pub next_phase: SessionLifecyclePhase,
    pub updated_at_ms: u64,
    pub error: Option<String>,
}

impl SessionLifecycleTransition {
    pub fn validate(&self, current: &SessionLifecycleIntent) -> SessionResult<()> {
        if self.operation_id != current.operation_id
            || self.expected_revision != current.revision
            || self.expected_phase != current.phase
        {
            return Err(SessionError::Store(format!(
                "stale Session lifecycle transition for `{}`",
                self.operation_id
            )));
        }
        let valid = if self.next_phase == SessionLifecyclePhase::Failed {
            current.phase.is_stable() && !current.phase.is_terminal() && self.error.is_some()
        } else if current.phase == SessionLifecyclePhase::Failed {
            self.next_phase == current.last_stable_phase && self.error.is_none()
        } else {
            current.phase.successor() == Some(self.next_phase) && self.error.is_none()
        };
        if !valid {
            return Err(SessionError::Store(format!(
                "invalid Session lifecycle transition {} -> {} for `{}`",
                current.phase.as_str(),
                self.next_phase.as_str(),
                self.operation_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionBranchActivationPhase {
    BranchCommitted,
    ActivationPending,
    Activated,
    Failed,
}

impl SessionBranchActivationPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BranchCommitted => "branch_committed",
            Self::ActivationPending => "activation_pending",
            Self::Activated => "activated",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> SessionResult<Self> {
        match value {
            "branch_committed" => Ok(Self::BranchCommitted),
            "activation_pending" => Ok(Self::ActivationPending),
            "activated" => Ok(Self::Activated),
            "failed" => Ok(Self::Failed),
            other => Err(SessionError::Store(format!(
                "unknown Session branch activation phase `{other}`"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionBranchActivation {
    pub operation_id: String,
    pub source_session_id: String,
    pub target_session_id: String,
    /// Immutable branch cutoff captured before the branch transaction.
    pub source_message_count: usize,
    pub phase: SessionBranchActivationPhase,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub last_error: Option<String>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionBranchActivationTransition {
    pub operation_id: String,
    pub expected_revision: u64,
    pub expected_phase: SessionBranchActivationPhase,
    pub next_phase: SessionBranchActivationPhase,
    pub updated_at_ms: u64,
    pub error: Option<String>,
}

impl SessionBranchActivationTransition {
    pub fn validate(&self, current: &SessionBranchActivation) -> SessionResult<()> {
        if self.operation_id != current.operation_id
            || self.expected_revision != current.revision
            || self.expected_phase != current.phase
        {
            return Err(SessionError::Store(format!(
                "stale Session branch activation transition for `{}`",
                self.operation_id
            )));
        }
        let valid = matches!(
            (current.phase, self.next_phase),
            (
                SessionBranchActivationPhase::BranchCommitted,
                SessionBranchActivationPhase::ActivationPending
            ) | (
                SessionBranchActivationPhase::ActivationPending,
                SessionBranchActivationPhase::Activated | SessionBranchActivationPhase::Failed
            ) | (
                SessionBranchActivationPhase::Failed,
                SessionBranchActivationPhase::ActivationPending
            )
        );
        if !valid {
            return Err(SessionError::Store(format!(
                "invalid Session branch activation transition {} -> {} for `{}`",
                current.phase.as_str(),
                self.next_phase.as_str(),
                self.operation_id
            )));
        }
        if self.next_phase == SessionBranchActivationPhase::Failed && self.error.is_none() {
            return Err(SessionError::Store(
                "failed Session branch activation requires an error".to_string(),
            ));
        }
        if self.next_phase != SessionBranchActivationPhase::Failed && self.error.is_some() {
            return Err(SessionError::Store(
                "successful Session branch activation transition cannot retain an error"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_and_branch_phases_roundtrip_without_aliases() {
        for phase in [
            SessionLifecyclePhase::Planned,
            SessionLifecyclePhase::AdmissionFenced,
            SessionLifecyclePhase::RuntimeDrained,
            SessionLifecyclePhase::TombstoneCommitted,
            SessionLifecyclePhase::Unloaded,
            SessionLifecyclePhase::Failed,
        ] {
            assert_eq!(
                SessionLifecyclePhase::parse(phase.as_str()).expect("known lifecycle phase"),
                phase
            );
        }
        for phase in [
            SessionBranchActivationPhase::BranchCommitted,
            SessionBranchActivationPhase::ActivationPending,
            SessionBranchActivationPhase::Activated,
            SessionBranchActivationPhase::Failed,
        ] {
            assert_eq!(
                SessionBranchActivationPhase::parse(phase.as_str())
                    .expect("known branch activation phase"),
                phase
            );
        }
        assert!(SessionLifecyclePhase::parse("complete").is_err());
        assert!(SessionBranchActivationPhase::parse("pending").is_err());
    }
}
