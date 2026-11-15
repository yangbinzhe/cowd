//! Narrow test-only entry points for the Gateway real-PG coordination tests.
//!
//! These expose the production apply / dispatch / settlement entries under the
//! `test-fixtures` feature without widening production visibility. Actors passed
//! to `apply_for_test` must already carry a physical identity resolved through
//! the production `resolve_agent_action_actor_for_attempt`; this module does not
//! construct or authenticate identities.

use crate::agentic::action_service::AgentActionServiceError;
use crate::{AgentActionService, AgenticDispatchReceipt, RuntimeServices};
use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::{AgentActionEnvelope, AgentActionObservation};
use std::sync::Arc;

#[cfg(feature = "test-fixtures")]
impl AgentActionService {
    pub fn apply_for_test(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.apply(envelope)
    }

    /// Original Runtime supervisor entry for registering a coordination attempt
    /// before its graph exists (the missing-graph recovery case). This is the
    /// production owner call; it does not construct or authenticate identities.
    pub fn apply_runtime_supervisor_for_test(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<AgentActionObservation, AgentActionServiceError> {
        self.apply_runtime_supervisor(envelope)
    }
}

#[cfg(feature = "test-fixtures")]
impl RuntimeServices {
    pub async fn settle_abandoned_agentic_attempt_for_test(
        self: &Arc<Self>,
        packet: &AgentTaskPacket,
        reason: &str,
    ) -> Result<(), String> {
        self.settle_abandoned_agentic_attempt(packet, reason).await
    }

    pub async fn dispatch_ready_agentic_work_for_test(
        self: &Arc<Self>,
        program_id: &str,
    ) -> Result<Vec<AgenticDispatchReceipt>, String> {
        self.dispatch_ready_agentic_work(program_id).await
    }
}
