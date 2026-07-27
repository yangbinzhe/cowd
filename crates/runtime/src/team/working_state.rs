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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus_scope_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlap_budget_bp: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub novelty_target_bp: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub focus_resource_scopes: Vec<String>,
    /// Successful tool receipts materialized by this Agent. Unlike
    /// `focus_resource_scopes`, these scopes are observed, not planned.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub observed_resource_scopes: Vec<String>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusOverlapAssessment {
    pub observed: bool,
    pub maximum_overlap_bp: u16,
    pub allowed_overlap_bp: u16,
    pub exceeded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_focus_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_focus_id: Option<String>,
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

    #[must_use]
    pub fn focus_overlap_assessment(&self) -> FocusOverlapAssessment {
        let mut assessment = FocusOverlapAssessment::default();
        for (left_index, left) in self.entries.iter().enumerate() {
            for right in self.entries.iter().skip(left_index + 1) {
                if left.role_id.is_none()
                    || left.role_id != right.role_id
                    || left.focus_id.is_none()
                    || left.focus_id == right.focus_id
                    || left.observed_resource_scopes.is_empty()
                    || right.observed_resource_scopes.is_empty()
                {
                    continue;
                }
                let left_shared = left
                    .observed_resource_scopes
                    .iter()
                    .filter(|scope| {
                        right
                            .observed_resource_scopes
                            .iter()
                            .any(|other| resource_scopes_overlap(scope, other))
                    })
                    .count();
                let right_shared = right
                    .observed_resource_scopes
                    .iter()
                    .filter(|scope| {
                        left.observed_resource_scopes
                            .iter()
                            .any(|other| resource_scopes_overlap(scope, other))
                    })
                    .count();
                let shared = left_shared.max(right_shared);
                let union = left
                    .observed_resource_scopes
                    .len()
                    .saturating_add(right.observed_resource_scopes.len())
                    .saturating_sub(shared);
                let overlap_bp = if union == 0 {
                    0
                } else {
                    u16::try_from(shared.saturating_mul(10_000) / union).unwrap_or(10_000)
                };
                let allowed_overlap_bp = left
                    .overlap_budget_bp
                    .unwrap_or(0)
                    .min(right.overlap_budget_bp.unwrap_or(0));
                assessment.observed = true;
                if overlap_bp >= assessment.maximum_overlap_bp {
                    assessment.maximum_overlap_bp = overlap_bp;
                    assessment.allowed_overlap_bp = allowed_overlap_bp;
                    assessment.left_focus_id = left.focus_id.clone();
                    assessment.right_focus_id = right.focus_id.clone();
                }
                assessment.exceeded |= overlap_bp > allowed_overlap_bp;
            }
        }
        assessment
    }

    pub fn verify_completed_graph(&self, graph: &ExecutionGraph) -> Result<(), String> {
        if self.graph_id != graph.id {
            return Err("Team working state graph identity mismatch".to_string());
        }
        let completed_agents = graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .filter(|node| {
                graph.node_statuses.get(&node.id) == Some(&ExecutionNodeStatus::Completed)
            })
            .collect::<Vec<_>>();
        if completed_agents.is_empty() {
            return Err("Team graph has no completed Agent role slots".to_string());
        }
        for node in completed_agents {
            let entries = self
                .entries
                .iter()
                .filter(|entry| entry.node_id == node.id)
                .collect::<Vec<_>>();
            if entries.len() != 1 {
                return Err(format!(
                    "Team role slot `{}` has {} committed working-state entries",
                    node.id,
                    entries.len()
                ));
            }
            let entry = entries[0];
            if entry.role_id.as_deref().is_none_or(str::is_empty)
                || entry.focus_id.as_deref().is_none_or(str::is_empty)
                || entry.focus_scope_hash.as_deref().is_none_or(str::is_empty)
                || !entry.refs.iter().any(|reference| {
                    reference
                        .strip_prefix("evidence:")
                        .is_some_and(|reference| !reference.trim().is_empty())
                })
            {
                return Err(format!(
                    "Team role slot `{}` lacks role/focus/materialized evidence",
                    node.id
                ));
            }
        }
        let verify = graph
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::Verify)
            .ok_or_else(|| "Team graph has no Verify node".to_string())?;
        if graph.node_statuses.get(&verify.id) != Some(&ExecutionNodeStatus::Completed)
            || graph
                .node_results
                .get(&verify.id)
                .and_then(|result| result.result_ref.as_deref())
                .is_none_or(|reference| !reference.ends_with(":satisfied"))
        {
            return Err("Team result contract was not durably verified".to_string());
        }
        Ok(())
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
    let team_id = packet.team_id()?.trim();
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
    let refs = result
        .map(|result| {
            result
                .evidence_refs
                .iter()
                .filter(|reference| {
                    crate::agent_result_validator::is_materialized_durable_evidence(reference)
                })
                .map(|reference| {
                    format!(
                        "evidence:{}:{}",
                        reference.evidence_ref.ref_type, reference.evidence_ref.id
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut refs = refs;
    refs.sort();
    refs.dedup();
    if status == ExecutionNodeStatus::Completed && refs.is_empty() {
        return None;
    }
    let focus_boundary = packet_constraint(&packet, "focus_boundary:")
        .unwrap_or_else(|| "role-local semantic result".to_string());
    let entry = TeamWorkingStateEntry {
        entry_id: format!("{}:{}:{}", graph.id, node_id, graph.revision),
        team_id: team_id.to_string(),
        graph_id: graph.id.clone(),
        node_id: node_id.to_string(),
        producer_instance_id: binding.instance.instance_id.clone(),
        role_id: packet_constraint(&packet, "team_role:"),
        focus_id: packet_constraint(&packet, "focus_partition:"),
        focus_scope_hash: packet_constraint(&packet, "focus_scope_hash:"),
        overlap_budget_bp: packet_constraint(&packet, "focus_overlap_budget_bp:")
            .and_then(|value| value.parse::<u16>().ok()),
        novelty_target_bp: packet_constraint(&packet, "focus_novelty_target_bp:")
            .and_then(|value| value.parse::<u16>().ok()),
        focus_resource_scopes: packet
            .resource_scopes
            .iter()
            .filter(|scope| {
                scope.starts_with("read:")
                    || scope.starts_with("write:")
                    || scope.starts_with("workspace:")
            })
            .cloned()
            .collect(),
        observed_resource_scopes: result
            .map(|result| result.usage.runtime_observed_resource_scopes.clone())
            .unwrap_or_default(),
        kind,
        summary,
        refs,
        boundary: format!(
            "{focus_boundary}; runtime-terminal-projection; no raw chain-of-thought or raw tool output"
        ),
        confidence_milli,
        graph_revision: graph.revision,
    };
    let identity = &packet.assignment.execution_identity;
    let mut event_refs = vec![
        RuntimeEventRef {
            kind: "principal".to_string(),
            id: identity.principal_id().to_string(),
        },
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: identity.workspace_id().to_string(),
        },
        RuntimeEventRef {
            kind: "mission".to_string(),
            id: packet.mission_id().to_string(),
        },
        RuntimeEventRef {
            kind: "task".to_string(),
            id: packet.task_id().to_string(),
        },
        RuntimeEventRef {
            kind: "session".to_string(),
            id: packet.session_id().to_string(),
        },
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
        RuntimeEventRef {
            kind: "agent_run".to_string(),
            id: packet.run_id().to_string(),
        },
    ];
    if let Some(turn_id) = identity.turn_id() {
        event_refs.push(RuntimeEventRef {
            kind: "turn".to_string(),
            id: turn_id.to_string(),
        });
    }
    if let Some(team_run_id) = packet.team_id() {
        event_refs.push(RuntimeEventRef {
            kind: "team_run".to_string(),
            id: team_run_id.to_string(),
        });
    }
    event_refs.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.id.cmp(&right.id))
    });
    event_refs.dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    Some(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: format!("team-working-state:{team_id}"),
            scope: RuntimeEventScope::Team,
            kind: "team.working_state.appended.v1".to_string(),
            status: Some("committed".to_string()),
            actor: Some("execution_commit_service".to_string()),
            refs: event_refs,
            payload: serde_json::to_value(entry).ok()?,
        },
        idempotency_key: Some(format!(
            "team-working-state:{}:{}:{}",
            graph.id, node_id, graph.revision
        )),
        schema_version: 1,
    })
}

fn resource_scopes_overlap(left: &str, right: &str) -> bool {
    let normalize = |scope: &str| {
        let (_, path) = scope.split_once(':')?;
        let path = path.trim().replace('\\', "/");
        if path.starts_with('/') {
            return None;
        }
        let mut components = Vec::new();
        for component in path.split('/') {
            match component {
                "" | "." => {}
                ".." => return None,
                value if value.contains(':') => return None,
                value => components.push(value),
            }
        }
        (!components.is_empty()).then(|| components.join("/"))
    };
    let (Some(left), Some(right)) = (normalize(left), normalize(right)) else {
        return left == right;
    };
    let contains = |ancestor: &str, descendant: &str| {
        descendant == ancestor
            || descendant
                .strip_prefix(ancestor)
                .is_some_and(|suffix| suffix.starts_with('/'))
    };
    contains(&left, &right) || contains(&right, &left)
}

fn packet_constraint(packet: &AgentTaskPacket, prefix: &str) -> Option<String> {
    packet
        .constraints
        .iter()
        .find_map(|constraint| constraint.strip_prefix(prefix))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_overlap_treats_parent_child_and_read_write_scopes_as_shared() {
        assert!(resource_scopes_overlap(
            "read:crates",
            "read:crates/runtime"
        ));
        assert!(resource_scopes_overlap(
            "write:crates/runtime",
            "read:crates/runtime/src"
        ));
        assert!(!resource_scopes_overlap(
            "read:crates/runtime",
            "read:crates/gateway"
        ));
        assert!(resource_scopes_overlap(
            "read:crates/runtime",
            "read:crates//runtime"
        ));
        assert!(resource_scopes_overlap(
            "read:crates/runtime",
            "write:crates/./runtime/src"
        ));
    }
}
