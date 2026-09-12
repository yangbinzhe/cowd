use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use harness_contract::agent::{AgentCapability, AgentTaskIntent, AgentTaskPacket};
use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, AGENT_ACTION_TOOL_IDS};
use harness_contract::context::{ChildExecutionBudgetReservation, RequiredAcceptance};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphCommand, ExecutionGraphLineage, ExecutionNodeKind,
    ExecutionNodeSpec, ExecutionNodeStatus, ExecutionParentBinding,
};
use harness_contract::policy::PermissionMode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::execution_core::graph::executors::AgentTaskExecutor;
use crate::{
    bind_agent_capability_to_host, delegated_tool_effect_is_bounded, resolve_agent_capability,
    AgentCapabilityRequest, AgentCatalogEntry, ResolvedAgentCapability, RuntimeServices,
    RuntimeSkillCatalog,
};

use super::program::{
    AgentMemberProjection, AgenticProgramProjection, AgenticTaskProjection, AgenticTaskStatus,
};

mod admission;
mod dispatch;
mod heartbeat;
mod helpers;
#[cfg(test)]
mod tests;

pub(crate) use heartbeat::start_agentic_claim_heartbeat;
#[cfg(test)]
use heartbeat::AgenticClaimHeartbeatGuard;

#[cfg(test)]
use admission::{effective_skill_grants, intersect_agentic_tools, AgenticToolHostSnapshot};
#[cfg(test)]
use dispatch::DispatchFlight;
#[cfg(test)]
use heartbeat::{
    agentic_claim_actor, agentic_claim_heartbeat_state, agentic_graph_is_terminal,
    AgenticClaimDriverState,
};
#[cfg(test)]
use helpers::{member_dispatch_rank, DispatchMode};

/// Trusted execution context inherited from the action caller. Model JSON
/// never controls these fields.
#[derive(Debug, Clone)]
pub struct AgenticDispatchContext {
    pub session_id: String,
    pub turn_id: String,
    pub model_lease: String,
    pub permission_ceiling: PermissionMode,
    pub resource_scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgenticDispatchReceipt {
    pub graph_id: String,
    pub task_ref: String,
    pub agent_ref: String,
    pub mode: String,
}
