//! Event-sourced collaboration semantics for one Team run.
//!
//! `ExecutionGraph` remains the exclusive owner of topology and node status.
//! This module stores only the information that collaborators need to share:
//! evidence references, terminal findings, blockers, and unresolved work. It
//! never copies a raw model trace or a full tool response into shared state.

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeResult, ExecutionNodeStatus,
};
use serde::{Deserialize, Serialize};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamWorkingStateKind {
    Finding,
    Evidence,
    Decision,
    Conflict,
    Unresolved,
    Blocker,
    UserIntervention,
    Artifact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkingStateEntry {
    pub entry_id: String,
    pub team_id: String,
    pub graph_id: String,
    pub node_id: String,
    pub producer_instance_id: String,
    pub kind: TeamWorkingStateKind,
    pub summary: String,
    pub refs: Vec<String>,
    pub boundary: String,
    pub confidence_milli: u16,
    pub graph_revision: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkingState {
    pub team_id: String,
    pub graph_id: String,
    pub graph_revision: u64,
    pub entries: Vec<TeamWorkingStateEntry>,
}

impl TeamWorkingState {
    #[must_use]
    pub fn from_events(
        team_id: impl Into<String>,
        graph_id: impl Into<String>,
        events: impl IntoIterator<Item = crate::DurableRuntimeEvent>,
    ) -> Self {
        let team_id = team_id.into();
        let graph_id = graph_id.into();
        let mut state = Self {
            team_id,
            graph_id,
            graph_revision: 0,
            entries: Vec::new(),
        };
        for event in events {
            if event.scope != RuntimeEventScope::Team
                || event.kind != "team.working_state.appended.v1"
            {
                continue;
            }
            let Ok(entry) = serde_json::from_value::<TeamWorkingStateEntry>(event.payload) else {
                continue;
            };
            if entry.team_id != state.team_id || entry.graph_id != state.graph_id {
                continue;
            }
            state.graph_revision = state.graph_revision.max(entry.graph_revision);
            if !state
                .entries
                .iter()
                .any(|existing| existing.entry_id == entry.entry_id)
            {
                state.entries.push(entry);
            }
        }
        state
            .entries
            .sort_by(|left, right| left.entry_id.cmp(&right.entry_id));
        state
    }
}

/// Convert one terminal Agent node transition into the team-local semantic
/// projection event that is committed in the same EventStore transaction.
pub(crate) fn terminal_working_state_event(
    graph: &ExecutionGraph,
    node_id: &str,
    status: ExecutionNodeStatus,
    result: Option<&ExecutionNodeResult>,
) -> Option<RuntimeTransactionEventInput> {
    if !status.is_terminal() {
        return None;
    }
    let node = graph.nodes.iter().find(|node| node.id == node_id)?;
    if node.kind != ExecutionNodeKind::AgentTask {
        return None;
    }
    let packet = serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok()?;
    let team_id = packet.team_id.as_deref()?.trim();
    if team_id.is_empty() {
        return None;
    }
    let binding = packet.binding.as_ref()?;
    let (kind, summary, confidence_milli) = match status {
        ExecutionNodeStatus::Completed => {
            let summary = result
                .and_then(|result| result.summary.as_deref())
                .map(str::trim)
                .filter(|summary| !summary.is_empty())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| {
                    format!("Role node `{node_id}` completed without a semantic summary.")
                });
            let kind = if result
                .and_then(|result| result.result_ref.as_deref())
                .is_some_and(|reference| reference.ends_with(":unresolved"))
            {
                TeamWorkingStateKind::Unresolved
            } else {
                TeamWorkingStateKind::Finding
            };
            (kind, summary, 1_000)
        }
        ExecutionNodeStatus::Failed
        | ExecutionNodeStatus::Blocked
        | ExecutionNodeStatus::Cancelled => (
            TeamWorkingStateKind::Blocker,
            result
                .and_then(|result| result.failure.as_ref())
                .map_or_else(
                    || format!("Role node `{node_id}` reached terminal status {status:?}."),
                    |failure| format!("Role node `{node_id}` blocked: {}", failure.message),
                ),
            0,
        ),
        _ => return None,
    };
    let mut refs = result
        .map(|result| {
            result
                .evidence_refs
                .iter()
                .map(|reference| {
                    format!(
                        "evidence:{}:{}",
                        reference.evidence_ref.0.ref_type, reference.evidence_ref.0.id
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(result_ref) = result.and_then(|result| result.result_ref.as_ref()) {
        refs.push(format!("result:{result_ref}"));
    }
    refs.sort();
    refs.dedup();
    let entry = TeamWorkingStateEntry {
        entry_id: format!("{}:{}:{}", graph.id, node_id, graph.revision),
        team_id: team_id.to_string(),
        graph_id: graph.id.clone(),
        node_id: node_id.to_string(),
        producer_instance_id: binding.instance.instance_id.clone(),
        kind,
        summary,
        refs,
        boundary: "runtime-terminal-projection; no raw chain-of-thought or raw tool output"
            .to_string(),
        confidence_milli,
        graph_revision: graph.revision,
    };
    Some(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: format!("team-working-state:{team_id}"),
            scope: RuntimeEventScope::Team,
            kind: "team.working_state.appended.v1".to_string(),
            status: Some("committed".to_string()),
            actor: Some("execution_commit_service".to_string()),
            refs: vec![
                RuntimeEventRef {
                    kind: "execution_graph".to_string(),
                    id: graph.id.clone(),
                },
                RuntimeEventRef {
                    kind: "execution_node".to_string(),
                    id: node_id.to_string(),
                },
                RuntimeEventRef {
                    kind: "agent_instance".to_string(),
                    id: binding.instance.instance_id.clone(),
                },
            ],
            payload: serde_json::to_value(entry).ok()?,
        },
        idempotency_key: Some(format!(
            "team-working-state:{}:{}:{}",
            graph.id, node_id, graph.revision
        )),
        schema_version: 1,
    })
}
