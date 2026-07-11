use harness_contract::agent::{AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentResultValidationError {
    BindingMismatch,
    MissingOutcome,
    MissingAcceptance,
    MissingEvidence,
}

impl std::fmt::Display for AgentResultValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::BindingMismatch => "agent return does not match the task graph binding",
            Self::MissingOutcome => "completed agent return has no outcome",
            Self::MissingAcceptance => "completed agent return omitted acceptance evaluation",
            Self::MissingEvidence => "completed agent return omitted required evidence",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for AgentResultValidationError {}

/// Pure validation before V3's `ExecutionCommitService` commits the graph/node
/// transition. This module never mutates graph state.
pub fn validate_agent_return(
    task: &AgentTaskPacket,
    returned: &AgentReturnPacket,
) -> Result<(), AgentResultValidationError> {
    if returned.run_id != task.run_id
        || returned.agent_id != task.agent_id
        || returned.task_id != task.task_id
        || returned.session_id != task.session_id
        || returned.graph_id != task.graph_id
        || returned.node_id != task.node_id
        || returned.attempt != task.attempt
        || returned.expected_graph_revision != task.expected_graph_revision
    {
        return Err(AgentResultValidationError::BindingMismatch);
    }
    if returned.status == AgentTerminalStatus::Completed && returned.outcome.trim().is_empty() {
        return Err(AgentResultValidationError::MissingOutcome);
    }
    if returned.status == AgentTerminalStatus::Completed
        && !task.acceptance.is_empty()
        && returned.acceptance.is_empty()
    {
        return Err(AgentResultValidationError::MissingAcceptance);
    }
    if returned.status == AgentTerminalStatus::Completed
        && !task.evidence_refs.is_empty()
        && returned.evidence_refs.is_empty()
    {
        return Err(AgentResultValidationError::MissingEvidence);
    }
    Ok(())
}
