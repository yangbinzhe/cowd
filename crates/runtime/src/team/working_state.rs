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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TeamWorkingStateVisibility {
    #[default]
    Team,
    Role,
    Private,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<String>,
    pub boundary: String,
    pub confidence_milli: u16,
    pub graph_revision: u64,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub source_generation: u64,
    #[serde(default)]
    pub visibility: TeamWorkingStateVisibility,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkingState {
    pub team_id: String,
    pub graph_id: String,
    pub graph_revision: u64,
    pub board_revision: u64,
    pub entries: Vec<TeamWorkingStateEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamWorkingStatePublishRequest {
    pub graph_id: String,
    pub node_id: String,
    pub expected_revision: u64,
    pub kind: TeamWorkingStateKind,
    pub summary: String,
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    #[serde(default)]
    pub visibility: TeamWorkingStateVisibility,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamWorkingStateReadRequest {
    pub graph_id: String,
    pub node_id: String,
    #[serde(default)]
    pub after_revision: Option<u64>,
    #[serde(default)]
    pub exact_revision: Option<u64>,
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
            board_revision: 0,
            entries: Vec::new(),
        };
        for event in events {
            if event.scope != RuntimeEventScope::Team
                || event.kind != "team.working_state.appended.v1"
            {
                continue;
            }
            let Ok(mut entry) = serde_json::from_value::<TeamWorkingStateEntry>(event.payload)
            else {
                continue;
            };
            if entry.team_id != state.team_id || entry.graph_id != state.graph_id {
                continue;
            }
            state.graph_revision = state.graph_revision.max(entry.graph_revision);
            state.board_revision = state.board_revision.max(event.sequence);
            entry.revision = event.sequence;
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
                // Scope overlap is a one-to-one set intersection. Using the
                // larger side lets several observed child paths all count
                // against one shared parent scope, producing impossible
                // values above 10_000 bp and blocking a valid read-only Team.
                let shared = left_shared.min(right_shared);
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
        let agent_slots = graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
            .collect::<Vec<_>>();
        if agent_slots.is_empty() {
            return Err("Team graph has no Agent role slots".to_string());
        }
        for node in agent_slots {
            let status = graph
                .node_statuses
                .get(&node.id)
                .copied()
                .ok_or_else(|| format!("Team role slot `{}` has no graph status", node.id))?;
            if !status.is_terminal() {
                return Err(format!(
                    "Team role slot `{}` is not terminal ({status:?})",
                    node.id
                ));
            }
            let entries = self
                .entries
                .iter()
                .filter(|entry| entry.node_id == node.id)
                .collect::<Vec<_>>();
            if entries.is_empty() {
                return Err(format!(
                    "Team role slot `{}` has no committed working-state entries",
                    node.id
                ));
            }
            let materialized = if status == ExecutionNodeStatus::Completed {
                entries.iter().any(|entry| {
                    entry
                        .role_id
                        .as_deref()
                        .is_some_and(|value| !value.is_empty())
                        && entry
                            .focus_id
                            .as_deref()
                            .is_some_and(|value| !value.is_empty())
                        && entry
                            .focus_scope_hash
                            .as_deref()
                            .is_some_and(|value| !value.is_empty())
                        && (entry.refs.iter().any(|reference| {
                            reference
                                .strip_prefix("evidence:")
                                .is_some_and(|reference| !reference.trim().is_empty())
                        }) || !entry.artifact_refs.is_empty())
                })
            } else {
                entries.iter().any(|entry| {
                    matches!(
                        entry.kind,
                        TeamWorkingStateKind::Blocker | TeamWorkingStateKind::Unresolved
                    ) && !entry.summary.trim().is_empty()
                })
            };
            if !materialized {
                return Err(format!(
                    "Team role slot `{}` lacks a materialized {:?} working-state entry",
                    node.id, status
                ));
            }
        }
        let verify = graph
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::Verify)
            .ok_or_else(|| "Team graph has no Verify node".to_string())?;
        let verify_status = graph
            .node_statuses
            .get(&verify.id)
            .copied()
            .ok_or_else(|| "Team Verify node has no graph status".to_string())?;
        if !verify_status.is_terminal() {
            return Err("Team result contract has no terminal verification verdict".to_string());
        }
        let verify_result = graph
            .node_results
            .get(&verify.id)
            .ok_or_else(|| "Team Verify node has no committed verdict".to_string())?;
        let has_satisfied_verdict = verify_status == ExecutionNodeStatus::Completed
            && verify_result
                .result_ref
                .as_deref()
                .is_some_and(|reference| reference.ends_with(":satisfied"));
        let has_unsatisfied_verdict = verify_status != ExecutionNodeStatus::Completed
            && (verify_result.failure.is_some()
                || verify_result
                    .result_ref
                    .as_deref()
                    .is_some_and(|reference| reference.ends_with(":not_satisfied")));
        if !has_satisfied_verdict && !has_unsatisfied_verdict {
            return Err("Team result contract has no durable verification verdict".to_string());
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
    // A terminal Team projection must be fenced to the role fragment frozen
    // with the Team binding.  Node ids and string constraints are not an
    // execution-time source of role semantics.
    let role = packet.team_role_assignment()?;
    let (kind, summary, confidence_milli) = match status {
        ExecutionNodeStatus::Completed => {
            let semantic_summary = result
                .and_then(|result| result.summary.as_deref())
                .map(str::trim)
                .filter(|summary| !summary.is_empty());
            let summary = semantic_summary.map_or_else(
                || format!("Role node `{node_id}` completed without a semantic summary."),
                ToOwned::to_owned,
            );
            let kind = if semantic_summary.is_none()
                || result
                    .and_then(|result| result.result_ref.as_deref())
                    .is_some_and(|reference| reference.ends_with(":unresolved"))
            {
                TeamWorkingStateKind::Unresolved
            } else {
                TeamWorkingStateKind::Finding
            };
            let confidence_milli = if kind == TeamWorkingStateKind::Finding {
                1_000
            } else {
                0
            };
            (kind, summary, confidence_milli)
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
    let entry = TeamWorkingStateEntry {
        entry_id: format!("{}:{}:{}", graph.id, node_id, graph.revision),
        team_id: team_id.to_string(),
        graph_id: graph.id.clone(),
        node_id: node_id.to_string(),
        producer_instance_id: binding.instance.instance_id.clone(),
        role_id: Some(role.identity.role_id.clone()),
        focus_id: Some(role.identity.focus_id.clone()),
        focus_scope_hash: Some(role.identity.focus_scope_hash.clone()),
        overlap_budget_bp: Some(role.identity.overlap_budget_bp),
        novelty_target_bp: Some(role.identity.novelty_target_bp),
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
        observed_resource_scopes: result.map_or_else(Vec::new, |result| {
            result
                .usage
                .observed_acceptance
                .observed_evidence
                .iter()
                .map(crate::path_identity::observed_scope_key)
                .collect()
        }),
        kind,
        summary,
        refs,
        artifact_refs: Vec::new(),
        boundary: format!(
            "{}; runtime-terminal-projection; no raw chain-of-thought or raw tool output",
            role.identity.focus_boundary
        ),
        confidence_milli,
        graph_revision: graph.revision,
        revision: 0,
        source_generation: graph
            .orchestration
            .as_ref()
            .map_or(graph.revision, |metadata| metadata.source_generation),
        visibility: TeamWorkingStateVisibility::Team,
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
    // A whole-workspace lease (`read:.` / `write:.`) normalizes to an empty
    // path; it overlaps every other scope in the same workspace.
    if left.is_empty() || right.is_empty() {
        return true;
    }
    let contains = |ancestor: &str, descendant: &str| {
        descendant == ancestor
            || descendant
                .strip_prefix(ancestor)
                .is_some_and(|suffix| suffix.starts_with('/'))
    };
    contains(&left, &right) || contains(&right, &left)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlap_entry(
        focus_id: &str,
        scopes: &[&str],
        overlap_budget_bp: u16,
    ) -> TeamWorkingStateEntry {
        TeamWorkingStateEntry {
            entry_id: format!("entry:{focus_id}"),
            team_id: "team".to_string(),
            graph_id: "graph".to_string(),
            node_id: format!("node:{focus_id}"),
            producer_instance_id: format!("agent:{focus_id}"),
            role_id: Some("researcher".to_string()),
            focus_id: Some(focus_id.to_string()),
            focus_scope_hash: None,
            overlap_budget_bp: Some(overlap_budget_bp),
            novelty_target_bp: None,
            focus_resource_scopes: vec!["read:Code/AICS".to_string()],
            observed_resource_scopes: scopes.iter().map(ToString::to_string).collect(),
            kind: TeamWorkingStateKind::Evidence,
            summary: "checked evidence".to_string(),
            refs: Vec::new(),
            artifact_refs: Vec::new(),
            boundary: "observed".to_string(),
            confidence_milli: 900,
            graph_revision: 1,
            revision: 1,
            source_generation: 1,
            visibility: TeamWorkingStateVisibility::Team,
        }
    }

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

    #[test]
    fn asymmetric_child_paths_cannot_exceed_the_overlap_scale() {
        let state = TeamWorkingState {
            team_id: "team".to_string(),
            graph_id: "graph".to_string(),
            graph_revision: 1,
            board_revision: 2,
            entries: vec![
                overlap_entry(
                    "architecture",
                    &["read:Code/AICS/pom.xml", "read:Code/AICS/README.md"],
                    10_000,
                ),
                overlap_entry("quality", &["read:Code/AICS"], 10_000),
            ],
        };

        let assessment = state.focus_overlap_assessment();
        assert!(assessment.observed);
        assert_eq!(assessment.maximum_overlap_bp, 5_000);
        assert_eq!(assessment.allowed_overlap_bp, 10_000);
        assert!(!assessment.exceeded);
    }

    #[test]
    fn failed_roles_and_unsatisfied_verify_are_durably_materialized() {
        let mut graph = ExecutionGraph::new("terminal failure");
        let mut agent = harness_contract::execution_graph::ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            "{}",
        );
        agent.id = "agent-failed".to_string();
        let mut verify = harness_contract::execution_graph::ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            "verify",
            "team:fixture",
        );
        verify.id = "verify".to_string();
        graph.nodes.extend([agent.clone(), verify.clone()]);
        graph
            .node_statuses
            .insert(agent.id.clone(), ExecutionNodeStatus::Failed);
        graph
            .node_statuses
            .insert(verify.id.clone(), ExecutionNodeStatus::Blocked);
        graph.node_results.insert(
            verify.id.clone(),
            ExecutionNodeResult {
                status: ExecutionNodeStatus::Blocked,
                result_ref: Some("verification:fixture:not_satisfied".to_string()),
                summary: Some("terminal branch failed".to_string()),
                evidence_refs: Vec::new(),
                failure: Some(harness_contract::execution_graph::ExecutionFailure {
                    kind: "team_delivery_unsatisfied".to_string(),
                    message: "terminal branch failed".to_string(),
                    retryable: false,
                    evidence_refs: Vec::new(),
                }),
                usage: Default::default(),
                finished_at_ms: 1,
            },
        );
        let mut entry = overlap_entry("failure", &[], 0);
        entry.graph_id = graph.id.clone();
        entry.node_id = agent.id;
        entry.kind = TeamWorkingStateKind::Blocker;
        entry.summary = "Role failed before producing evidence".to_string();
        let state = TeamWorkingState {
            team_id: "team".to_string(),
            graph_id: graph.id.clone(),
            graph_revision: 1,
            board_revision: 1,
            entries: vec![entry],
        };

        assert!(state.verify_completed_graph(&graph).is_ok());
    }
}
