use std::sync::Arc;

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind, ExecutionNodeStatus};
use harness_contract::team::{
    AgentDisplayIdentity, TeamLifecycleState, TeamRunResult, TeamTaskTrace,
};
use serde::{Deserialize, Serialize};

use crate::{AgentRuntime, ExecutionGraphStateStore, RuntimeEventStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProjection {
    pub team_id: String,
    pub session_id: String,
    pub graph_id: String,
    pub graph_revision: u64,
    /// Graph-derived lifecycle.  This is deliberately separate from the
    /// terminal delivery status below: a failed/blocked branch must not make
    /// a still-running Team appear partially complete.
    pub lifecycle: TeamLifecycleState,
    pub status: String,
    /// Exact node that currently determines a non-terminal wait, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_node_id: Option<String>,
    /// Typed wait/terminal status for `blocking_node_id`; UI never has to
    /// infer lifecycle from a free-form message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_node_status: Option<ExecutionNodeStatus>,
    /// Bounded canonical failure kind, if the blocking node has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_reason_kind: Option<String>,
    /// Human-facing team display name from the frozen Binding, when declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_label: Option<String>,
    pub tasks: Vec<TeamTaskTrace>,
    pub terminal_result: Option<TeamRunResult>,
    /// Frozen Agent Binding digests of every Team node. Surfaces use these as
    /// stable identity, never raw payload keys.
    #[serde(default)]
    pub binding_digests: Vec<String>,
    /// Compiled display identities. When a display snapshot is unavailable the
    /// label is the definition id with an explicit `unavailable-name`
    /// provenance, never a raw payload guess.
    #[serde(default)]
    pub agent_displays: Vec<AgentDisplayIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProjectionQuarantine {
    pub graph_id: String,
    pub reason: String,
    pub evidence_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProjectionCursor {
    pub commit_cursor: u64,
    pub graph_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProjectionPage {
    pub teams: Vec<TeamProjection>,
    pub next_cursor: Option<TeamProjectionCursor>,
}

/// Read-only Team facade. The graph and AgentRuntime are the sources of truth.
pub struct TeamProjectionReader {
    graphs: ExecutionGraphStateStore,
    binding_store: Option<Arc<RuntimeEventStore>>,
}

impl TeamProjectionReader {
    #[must_use]
    pub fn new(
        graphs: ExecutionGraphStateStore,
        _agents: Arc<AgentRuntime>,
        binding_store: Option<Arc<RuntimeEventStore>>,
    ) -> Self {
        Self {
            graphs,
            binding_store,
        }
    }

    pub fn project(&self, graph_id: &str) -> Result<TeamProjection, String> {
        let graph = self
            .graphs
            .load(graph_id)
            .map_err(|error| error.to_string())?;
        self.project_graph(graph)
    }

    pub fn list(&self) -> Result<Vec<TeamProjection>, String> {
        let mut projections = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_page(cursor, 256)?;
            projections.extend(page.teams);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        projections.sort_by(|left, right| left.graph_id.cmp(&right.graph_id));
        Ok(projections)
    }

    pub fn list_page(
        &self,
        after: Option<TeamProjectionCursor>,
        limit: usize,
    ) -> Result<TeamProjectionPage, String> {
        let limit = limit.clamp(1, 512);
        let graph_page = self
            .graphs
            .graph_ids_page(
                after.map(|cursor| (cursor.commit_cursor, cursor.graph_id)),
                limit,
            )
            .map_err(|error| error.to_string())?;
        let next_cursor = (graph_page.len() == limit).then(|| {
            let (graph_id, commit_cursor) = graph_page
                .last()
                .expect("a full graph page has a terminal cursor");
            TeamProjectionCursor {
                commit_cursor: *commit_cursor,
                graph_id: graph_id.clone(),
            }
        });
        let mut projections = Vec::new();
        for (graph_id, _) in graph_page {
            if self
                .graphs
                .team_projection_quarantine(&graph_id)
                .map_err(|error| error.to_string())?
                .is_some()
            {
                continue;
            }
            let graph = self
                .graphs
                .load(&graph_id)
                .map_err(|error| error.to_string())?;
            let declares_team = graph.nodes.iter().any(|node| {
                node.kind == ExecutionNodeKind::AgentTask
                    && serde_json::from_str::<serde_json::Value>(&node.payload_ref)
                        .ok()
                        .and_then(|value| value.get("team_id").cloned())
                        .and_then(|value| value.as_str().map(str::to_string))
                        .is_some_and(|team_id| !team_id.trim().is_empty())
            });
            match self.project_graph(graph) {
                Ok(projection) => projections.push(projection),
                // One historical or corrupt Team graph must not make every
                // healthy Team undiscoverable. Direct projection of that
                // graph still returns the parse error, while enumeration
                // quarantines it and keeps the remaining runtime usable.
                Err(error) if declares_team => {
                    let governance = self
                        .graphs
                        .quarantine_team_projection(&graph_id, &error)
                        .map_err(|governance_error| governance_error.to_string())?;
                    tracing::warn!(
                        graph_id,
                        evidence_id = governance["evidence_id"].as_str().unwrap_or_default(),
                        "quarantined invalid Team projection"
                    );
                }
                Err(_) => {}
            }
        }
        projections.sort_by(|left, right| left.graph_id.cmp(&right.graph_id));
        Ok(TeamProjectionPage {
            teams: projections,
            next_cursor,
        })
    }

    pub fn quarantined(&self) -> Result<Vec<TeamProjectionQuarantine>, String> {
        let mut quarantined = Vec::new();
        let mut cursor = None;
        loop {
            let graph_page = self
                .graphs
                .graph_ids_page(
                    cursor.take().map(|cursor: TeamProjectionCursor| {
                        (cursor.commit_cursor, cursor.graph_id)
                    }),
                    256,
                )
                .map_err(|error| error.to_string())?;
            for (graph_id, _) in &graph_page {
                let Some(value) = self
                    .graphs
                    .team_projection_quarantine(graph_id)
                    .map_err(|error| error.to_string())?
                else {
                    continue;
                };
                quarantined.push(TeamProjectionQuarantine {
                    graph_id: graph_id.clone(),
                    reason: value["reason"]
                        .as_str()
                        .unwrap_or("invalid Team graph")
                        .to_string(),
                    evidence_id: value["evidence_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                });
            }
            if graph_page.len() < 256 {
                break;
            }
            cursor = graph_page
                .last()
                .map(|(graph_id, commit_cursor)| TeamProjectionCursor {
                    commit_cursor: *commit_cursor,
                    graph_id: graph_id.clone(),
                });
        }
        quarantined.sort_by(|left, right| left.graph_id.cmp(&right.graph_id));
        Ok(quarantined)
    }

    fn project_graph(&self, graph: ExecutionGraph) -> Result<TeamProjection, String> {
        let mut tasks = Vec::new();
        let mut binding_digests = Vec::new();
        let mut agent_displays = Vec::new();
        let mut team_id = None;
        let mut session_id: Option<String> = None;
        for node in graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        {
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|error| format!("invalid team AgentTask packet {}: {error}", node.id))?;
            let packet_team = packet
                .team_id()
                .map(str::to_owned)
                .ok_or_else(|| format!("AgentTask {} is not bound to a team", node.id))?;
            if let Some(existing) = &team_id {
                if existing != &packet_team {
                    return Err(format!(
                        "graph {} contains multiple team identities",
                        graph.id
                    ));
                }
            } else {
                team_id = Some(packet_team);
            }
            if let Some(existing) = &session_id {
                if existing.as_str() != packet.session_id() {
                    return Err(format!(
                        "graph {} contains multiple team session identities",
                        graph.id
                    ));
                }
            } else {
                session_id = Some(packet.session_id().to_string());
            }
            let durable_result = graph.node_results.get(&node.id);
            let role = packet.team_role_assignment().ok_or_else(|| {
                format!(
                    "Team AgentTask {} lacks its frozen typed role assignment",
                    node.id
                )
            })?;
            tasks.push(TeamTaskTrace {
                task_id: packet.task_id().to_string(),
                role_id: role.identity.role_id.clone(),
                agent_id: packet.agent_id().to_string(),
                run_id: packet.run_id().to_string(),
                node_id: node.id.clone(),
                status: graph
                    .node_statuses
                    .get(&node.id)
                    .map(|status| format!("{status:?}").to_ascii_lowercase())
                    .unwrap_or_else(|| "planned".into()),
                result_ref: graph
                    .node_results
                    .get(&node.id)
                    .and_then(|result| result.result_ref.clone()),
                evidence_refs: durable_result
                    .map(|result| result.evidence_refs.clone())
                    .unwrap_or_default(),
                failure: durable_result.and_then(|result| {
                    result
                        .failure
                        .as_ref()
                        .map(|failure| failure.message.clone())
                }),
            });
            if let Some(binding) = packet.binding.as_ref() {
                binding_digests.push(binding.binding_digest.clone());
                let display = binding
                    .display
                    .clone()
                    .unwrap_or_else(|| AgentDisplayIdentity {
                        agent_id: packet.agent_id().to_string(),
                        role_id: packet.assignment.role_id.clone(),
                        role_display_name: None,
                        label: binding.definition_ref.definition_id.as_str().to_string(),
                        role_label: binding
                            .instance
                            .role_slot_id
                            .clone()
                            .unwrap_or_else(|| packet.assignment.role_id.clone()),
                        focus_label: None,
                        locale: "auto".to_string(),
                        provenance: "runtime.agent-binding:unavailable-name".to_string(),
                        digest: model_protocol::fingerprint::stable_hash_bytes(
                            binding.binding_digest.as_bytes(),
                        )
                        .to_string(),
                    });
                agent_displays.push(display);
            }
        }
        let team_id = team_id.ok_or_else(|| format!("graph {} has no team AgentTask", graph.id))?;
        let session_id =
            session_id.ok_or_else(|| format!("graph {} has no team session identity", graph.id))?;
        let final_node = graph
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::Synthesize);
        let terminal_result = final_node.and_then(|node| {
            graph.node_results.get(&node.id).and_then(|result| {
                result.result_ref.as_ref().map(|result_ref| TeamRunResult {
                    team_id: team_id.clone(),
                    graph_id: graph.id.clone(),
                    graph_revision: graph.revision,
                    result_ref: result_ref.clone(),
                    evidence_refs: result.evidence_refs.clone(),
                })
            })
        });
        binding_digests.sort();
        binding_digests.dedup();
        agent_displays.sort_by(|left, right| left.label.cmp(&right.label));
        let (lifecycle, status, blocking_node_id, blocking_node_status, blocking_reason_kind) =
            project_lifecycle(&graph);
        let display_label = self
            .binding_store
            .as_ref()
            .and_then(|store| {
                crate::team_binding::load_binding(store, &graph.id)
                    .ok()
                    .flatten()
            })
            .map(|binding| {
                binding
                    .display_identity
                    .team_display_name
                    .unwrap_or(binding.display_identity.label)
            });
        Ok(TeamProjection {
            team_id,
            session_id,
            graph_id: graph.id,
            graph_revision: graph.revision,
            lifecycle,
            status,
            blocking_node_id,
            blocking_node_status,
            blocking_reason_kind,
            display_label,
            tasks,
            terminal_result,
            binding_digests,
            agent_displays,
        })
    }
}

fn project_lifecycle(
    graph: &ExecutionGraph,
) -> (
    TeamLifecycleState,
    String,
    Option<String>,
    Option<ExecutionNodeStatus>,
    Option<String>,
) {
    let terminal = !graph.nodes.is_empty()
        && graph.nodes.iter().all(|node| {
            graph
                .node_statuses
                .get(&node.id)
                .copied()
                .unwrap_or(ExecutionNodeStatus::Planned)
                .is_terminal()
        });
    if terminal {
        let status = graph
            .delivery_envelope
            .as_ref()
            .map(|envelope| match envelope.delivery_status {
                harness_contract::outcome::DeliveryStatus::Satisfied => "completed",
                harness_contract::outcome::DeliveryStatus::Partial => "partial",
                harness_contract::outcome::DeliveryStatus::Unavailable => "unavailable",
                harness_contract::outcome::DeliveryStatus::Denied => "denied",
            })
            .unwrap_or_else(|| {
                if graph
                    .node_statuses
                    .values()
                    .any(|status| *status == ExecutionNodeStatus::Cancelled)
                {
                    "cancelled"
                } else if graph
                    .node_statuses
                    .values()
                    .any(|status| *status == ExecutionNodeStatus::Failed)
                {
                    "failed"
                } else if graph
                    .node_statuses
                    .values()
                    .any(|status| *status == ExecutionNodeStatus::Blocked)
                {
                    "blocked"
                } else {
                    "completed"
                }
            })
            .to_string();
        return (TeamLifecycleState::Terminal, status, None, None, None);
    }

    // The precedence is explicit and deterministic.  A wait is observable
    // even while independent branches continue running, but it never changes
    // terminal delivery semantics before the whole graph reaches terminal.
    let priorities = [
        (
            ExecutionNodeStatus::Paused,
            TeamLifecycleState::Paused,
            "paused",
        ),
        (
            ExecutionNodeStatus::WaitingInput,
            TeamLifecycleState::WaitingInput,
            "waiting_input",
        ),
        (
            ExecutionNodeStatus::WaitingApproval,
            TeamLifecycleState::WaitingApproval,
            "waiting_approval",
        ),
        (
            ExecutionNodeStatus::WaitingExternal,
            TeamLifecycleState::WaitingExternal,
            "waiting_external",
        ),
        (
            ExecutionNodeStatus::Blocked,
            TeamLifecycleState::WaitingDependency,
            "waiting_dependency",
        ),
    ];
    for (target_status, lifecycle, status) in priorities {
        if let Some(node) = graph
            .nodes
            .iter()
            .filter(|node| graph.node_statuses.get(&node.id) == Some(&target_status))
            .min_by(|left, right| left.id.cmp(&right.id))
        {
            let reason = graph
                .node_results
                .get(&node.id)
                .and_then(|result| result.failure.as_ref())
                .map(|failure| failure.kind.clone());
            return (
                lifecycle,
                status.to_string(),
                Some(node.id.clone()),
                Some(target_status),
                reason,
            );
        }
    }
    let all_not_started = graph.nodes.iter().all(|node| {
        matches!(
            graph
                .node_statuses
                .get(&node.id)
                .copied()
                .unwrap_or(ExecutionNodeStatus::Planned),
            ExecutionNodeStatus::Planned | ExecutionNodeStatus::Ready
        )
    });
    if all_not_started {
        (
            TeamLifecycleState::Preparing,
            "preparing".to_string(),
            None,
            None,
            None,
        )
    } else {
        (
            TeamLifecycleState::Running,
            "running".to_string(),
            None,
            None,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::agent::{AgentAssignment, AgentTaskIntent, AgentTaskPacket};
    use harness_contract::context::ChildExecutionBudgetReservation;
    use harness_contract::execution_graph::ExecutionNodeSpec;
    use harness_contract::policy::PermissionMode;
    use harness_contract::team::{TeamRoleAssignment, TeamRoleIdentity};
    use std::sync::Arc;

    fn frozen_role(role_id: &str) -> TeamRoleAssignment {
        TeamRoleAssignment {
            team_binding_id: "team-binding:projection-fixture".to_string(),
            team_binding_digest: "f".repeat(64),
            identity: TeamRoleIdentity {
                role_id: role_id.to_string(),
                slot: 1,
                focus_id: format!("{role_id}:focus"),
                focus_boundary: "fixture role-local boundary".to_string(),
                evidence_responsibility: "fixture evidence".to_string(),
                focus_scope_hash: "a".repeat(64),
                overlap_budget_bp: 0,
                novelty_target_bp: 0,
                output_acceptance: Vec::new(),
            },
            behavior: Vec::new(),
        }
    }

    fn packet(node_id: &str, run_id: &str) -> AgentTaskPacket {
        let intent = AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: None,
            granted_capabilities: Vec::new(),
            principal_id: "test".to_string(),
            source_turn_id: "turn-1".to_string(),
            run_id: run_id.to_string(),
            task_id: format!("task-{run_id}"),
            root_task_id: "root".to_string(),
            parent_task_id: None,
            session_id: "session-1".to_string(),
            mission_id: "mission-1".to_string(),
            team_id: Some("team-1".to_string()),
            graph_id: "team-graph:team-1".to_string(),
            node_id: node_id.to_string(),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "project".to_string(),
            team_role_identity: Some(frozen_role("implementer").identity),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: PermissionMode::ReadOnly,
            model_lease: "model".to_string(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "team",
                100,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            idempotency_key: format!("agent:{run_id}"),
        };
        let assignment = crate::test_support::agent_assignment(
            None,
            intent.run_id.as_str(),
            intent.run_id.as_str(),
            intent.task_id.as_str(),
            intent.session_id.as_str(),
            intent.mission_id.as_str(),
            intent.team_id.as_deref(),
            intent.graph_id.as_str(),
            intent.node_id.as_str(),
        );
        AgentTaskPacket {
            assignment: AgentAssignment {
                role_id: "implementer".to_string(),
                ..assignment
            },
            attempt: 1,
            expected_graph_revision: 1,
            objective: intent.objective,
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            team_role_identity: intent.team_role_identity.clone(),
            team_role: Some(frozen_role("implementer")),
            constraints: intent.constraints,
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: PermissionMode::ReadOnly,
            policy_revision: 1,
            model_lease: "model".to_string(),
            budget_lease: intent.budget_lease,
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: intent.idempotency_key,
        }
    }

    fn reader() -> TeamProjectionReader {
        let store = Arc::new(crate::RuntimeEventStore::try_open_in_memory().unwrap());
        let agents = Arc::new(crate::AgentRuntime::new(
            Arc::clone(&store),
            Arc::new(crate::ProviderRegistry::empty()),
        ));
        TeamProjectionReader::new(crate::ExecutionGraphStateStore::new(store), agents, None)
    }

    fn agent_node(id: &str, run_id: &str) -> ExecutionNodeSpec {
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&packet(id, run_id)).expect("packet"),
        );
        node.id = id.to_string();
        node
    }

    #[test]
    fn blocked_lane_with_running_sibling_is_nonterminal_not_partial() {
        let graph = {
            let mut graph = ExecutionGraph::new("blocked lane");
            graph.id = "team-graph:team-1".to_string();
            graph.nodes = vec![agent_node("n-a", "a"), agent_node("n-b", "b")];
            graph
                .node_statuses
                .insert("n-a".to_string(), ExecutionNodeStatus::Blocked);
            graph
                .node_statuses
                .insert("n-b".to_string(), ExecutionNodeStatus::Running);
            graph
        };
        let projection = reader()
            .project_graph(graph)
            .expect("projection with a blocked lane");
        assert_eq!(projection.status, "waiting_dependency");
    }

    #[test]
    fn failed_sibling_does_not_flip_status_while_other_lanes_run() {
        let graph = {
            let mut graph = ExecutionGraph::new("failed sibling");
            graph.id = "team-graph:team-1".to_string();
            graph.nodes = vec![agent_node("n-a", "a"), agent_node("n-b", "b")];
            graph
                .node_statuses
                .insert("n-a".to_string(), ExecutionNodeStatus::Failed);
            graph
                .node_statuses
                .insert("n-b".to_string(), ExecutionNodeStatus::Running);
            graph
        };
        let projection = reader()
            .project_graph(graph)
            .expect("projection with a failed sibling");
        assert_eq!(projection.status, "running");
        assert_eq!(projection.lifecycle, TeamLifecycleState::Running);
    }

    #[test]
    fn nonterminal_waits_are_typed_and_name_the_durable_blocking_node() {
        let mut graph = ExecutionGraph::new("waiting approval");
        graph.id = "team-graph:team-1".to_string();
        graph.nodes = vec![
            agent_node("n-running", "run"),
            agent_node("n-approval", "wait"),
        ];
        graph
            .node_statuses
            .insert("n-running".to_string(), ExecutionNodeStatus::Running);
        graph.node_statuses.insert(
            "n-approval".to_string(),
            ExecutionNodeStatus::WaitingApproval,
        );

        let projection = reader().project_graph(graph).expect("projection");
        assert_eq!(projection.lifecycle, TeamLifecycleState::WaitingApproval);
        assert_eq!(projection.status, "waiting_approval");
        assert_eq!(projection.blocking_node_id.as_deref(), Some("n-approval"));
        assert_eq!(
            projection.blocking_node_status,
            Some(ExecutionNodeStatus::WaitingApproval)
        );
        assert!(projection.blocking_reason_kind.is_none());
    }

    #[test]
    fn terminal_graph_without_envelope_maps_failed_and_completed_stably() {
        let failed = {
            let mut graph = ExecutionGraph::new("terminal failed");
            graph.id = "team-graph:team-1".to_string();
            graph.nodes = vec![agent_node("n-a", "a")];
            graph
                .node_statuses
                .insert("n-a".to_string(), ExecutionNodeStatus::Failed);
            graph
        };
        assert_eq!(
            reader().project_graph(failed).expect("failed").status,
            "failed"
        );
        assert_eq!(
            reader()
                .project_graph({
                    let mut graph = ExecutionGraph::new("cancelled");
                    graph.id = "team-graph:team-1".to_string();
                    graph.nodes = vec![agent_node("n-a", "a")];
                    graph
                        .node_statuses
                        .insert("n-a".to_string(), ExecutionNodeStatus::Cancelled);
                    graph
                })
                .expect("cancelled")
                .status,
            "cancelled"
        );

        let completed = {
            let mut graph = ExecutionGraph::new("terminal completed");
            graph.id = "team-graph:team-1".to_string();
            graph.nodes = vec![agent_node("n-a", "a")];
            graph
                .node_statuses
                .insert("n-a".to_string(), ExecutionNodeStatus::Completed);
            graph
        };
        assert_eq!(
            reader().project_graph(completed).expect("completed").status,
            "completed"
        );
    }

    #[test]
    fn projection_collects_binding_digests_when_packets_carry_bindings() {
        let mut packet = packet("n-a", "a");
        let binding = harness_contract::agent::AgentBindingSnapshot {
            binding_id: "binding:a".to_string(),
            definition_ref: harness_contract::agent::AgentDefinitionRevisionRef::new(
                harness_contract::agent::AgentDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/execute",
                )
                .unwrap(),
                1,
            )
            .unwrap(),
            definition_digest: "a".repeat(64),
            instructions: "Execute.".to_string(),
            instance: harness_contract::agent::AgentInstanceRef {
                instance_id: "instance:a".to_string(),
                role_slot_id: Some("implementer:1".to_string()),
            },
            executor: harness_contract::agent::AgentExecutorPolicy::CowdNative,
            model_policy: harness_contract::agent::AgentModelPolicy {
                profile: "coding".to_string(),
                allowed_models: vec!["test".to_string()],
                fallback_allowed: false,
            },
            effective_capabilities: vec![harness_contract::agent::AgentCapability::Read],
            skill_refs: Vec::new(),
            tool_contract_refs: Vec::new(),
            data_lease: harness_contract::agent::AgentDataLease {
                session_id: "session-1".to_string(),
                task_id: "task-a".to_string(),
                team_id: Some("team-1".to_string()),
                read_scopes: Vec::new(),
                write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
                fact_boundaries: Vec::new(),
                fact_refs: Vec::new(),
                matrix_snapshot_refs: Vec::new(),
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "b".repeat(64),
        };
        binding.validate().expect("valid binding");
        packet.binding = Some(binding);
        let graph = {
            let mut graph = ExecutionGraph::new("bound");
            graph.id = "team-graph:team-1".to_string();
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::AgentTask,
                "agent_task",
                serde_json::to_string(&packet).expect("packet"),
            );
            node.id = "n-a".to_string();
            graph.nodes = vec![node];
            graph
                .node_statuses
                .insert("n-a".to_string(), ExecutionNodeStatus::Completed);
            graph
        };
        let projection = reader().project_graph(graph).expect("projection");
        assert_eq!(projection.binding_digests, vec!["b".repeat(64)]);
        assert_eq!(projection.agent_displays.len(), 1);
        assert_eq!(
            projection.agent_displays[0].provenance,
            "runtime.agent-binding:unavailable-name"
        );
    }
}
