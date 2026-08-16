use std::sync::Arc;

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind, ExecutionNodeStatus};
use harness_contract::team::{TeamRunResult, TeamTaskTrace};
use serde::{Deserialize, Serialize};

use crate::{AgentRuntime, ExecutionGraphStateStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamProjection {
    pub team_id: String,
    pub session_id: String,
    pub graph_id: String,
    pub graph_revision: u64,
    pub status: String,
    pub tasks: Vec<TeamTaskTrace>,
    pub terminal_result: Option<TeamRunResult>,
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
}

impl TeamProjectionReader {
    #[must_use]
    pub fn new(graphs: ExecutionGraphStateStore, _agents: Arc<AgentRuntime>) -> Self {
        Self { graphs }
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
            tasks.push(TeamTaskTrace {
                task_id: packet.task_id().to_string(),
                role_id: node.id.rsplit(':').next().unwrap_or_default().to_string(),
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
        let status = if graph
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
            "partial"
        } else if graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
            && !graph.nodes.is_empty()
        {
            "completed"
        } else {
            "running"
        }
        .to_string();
        Ok(TeamProjection {
            team_id,
            session_id,
            graph_id: graph.id,
            graph_revision: graph.revision,
            status,
            tasks,
            terminal_result,
        })
    }
}
