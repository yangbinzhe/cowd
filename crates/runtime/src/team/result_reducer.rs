use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use harness_contract::agent::{AgentTaskPacket, AgentTerminalStatus};
use harness_contract::execution_graph::{
    ExecutionFailure, ExecutionNodeResult, ExecutionNodeStatus, ExecutionUsage,
};

use crate::execution_core::graph::executors::{SynthesizeBackend, SynthesizeBackendResolver};
use crate::execution_core::graph::{
    ExecutionGraphStateStore, NodeExecutionOutcome, NodeExecutionTicket,
};
use crate::execution_core::{
    ImmutableWorkKey, InFlightCoalescer, ModelWorkReducer, ModelWorkReductionInput,
};
use crate::AgentRuntime;

/// Reduces only durable AgentRuntime terminal packets already bound to the
/// graph. It returns an outcome to ExecutionCommitService and never mutates a
/// graph itself.
pub struct TeamResultReducer {
    state_store: ExecutionGraphStateStore,
    agents: Arc<AgentRuntime>,
    coalescer: Arc<InFlightCoalescer<ImmutableWorkKey, NodeExecutionOutcome, String>>,
}

impl TeamResultReducer {
    #[must_use]
    pub fn new(state_store: ExecutionGraphStateStore, agents: Arc<AgentRuntime>) -> Self {
        Self {
            state_store,
            agents,
            coalescer: Arc::new(InFlightCoalescer::default()),
        }
    }

    async fn synthesize_uncached(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let graph = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| error.to_string())?;
        let mut summaries = Vec::new();
        let mut evidence = Vec::new();
        let mut usage = ExecutionUsage::default();
        let mut blockers = Vec::new();
        let mut allows_unresolved = false;
        let terminal_agent_nodes = terminal_agent_node_ids(&graph);

        for node in graph.nodes.iter().filter(|node| {
            node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
        }) {
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| format!("team node {} is not an AgentTask packet", node.id))?;
            let required = node.work.as_ref().is_none_or(|work| work.required);
            allows_unresolved |= packet
                .constraints
                .iter()
                .any(|constraint| constraint == "protocol_allows_unresolved:true");
            let Some(returned) = self.agents.terminal_return(packet.agent_id()) else {
                if missing_optional_terminal_is_ignorable(&graph, node) {
                    continue;
                }
                return Err(format!(
                    "team binding missing terminal AgentRuntime result for {}",
                    packet.agent_id()
                ));
            };
            if returned.run_id != packet.run_id()
                || returned.graph_id != graph.id
                || returned.node_id != node.id
                || returned.attempt != packet.attempt
                || returned.expected_graph_revision != packet.expected_graph_revision
            {
                return Err(format!(
                    "team result binding mismatch for {}",
                    packet.agent_id()
                ));
            }
            usage.input_tokens = usage.input_tokens.saturating_add(returned.input_tokens);
            usage.output_tokens = usage.output_tokens.saturating_add(returned.output_tokens);
            usage.tool_calls = usage.tool_calls.saturating_add(returned.tool_calls);
            evidence.extend(returned.evidence_refs.clone());
            match returned.status {
                AgentTerminalStatus::Completed => {
                    if terminal_agent_nodes.contains(&node.id) {
                        summaries.push(ModelWorkReductionInput {
                            summary: render_terminal_outcome(&returned.outcome),
                            required,
                            evidence_refs: returned
                                .evidence_refs
                                .iter()
                                .map(|reference| {
                                    format!(
                                        "{}:{}",
                                        reference.evidence_ref.ref_type, reference.evidence_ref.id
                                    )
                                })
                                .collect(),
                        });
                    }
                }
                AgentTerminalStatus::Failed
                | AgentTerminalStatus::Cancelled
                | AgentTerminalStatus::Blocked => {
                    if required {
                        blockers.push(format!(
                            "{}: {}",
                            packet.agent_id(),
                            returned
                                .failure
                                .unwrap_or_else(|| "no terminal outcome".into())
                        ));
                    }
                }
            }
        }

        if summaries.is_empty() {
            return Err("team synthesis has no completed AgentRuntime results".into());
        }
        let (status, result_ref, failure) = if !blockers.is_empty() && !allows_unresolved {
            (
                ExecutionNodeStatus::Blocked,
                None,
                Some(ExecutionFailure {
                    kind: "team_agent_terminal_failure".into(),
                    message: blockers.join("; "),
                    retryable: false,
                    evidence_refs: evidence.clone(),
                }),
            )
        } else {
            let reduced = ModelWorkReducer::default().reduce(summaries);
            let mut final_answer = reduced.summary;
            if reduced.omitted_items > 0 {
                final_answer.push_str(&format!(
                    "\n\n{} oversized team result(s) remain available through durable evidence.",
                    reduced.omitted_items
                ));
            }
            if !blockers.is_empty() {
                final_answer.push_str("\n\n## Unresolved team role outcomes\n");
                for blocker in blockers {
                    final_answer.push_str("- ");
                    final_answer.push_str(&blocker);
                    final_answer.push('\n');
                }
            }
            (
                ExecutionNodeStatus::Completed,
                Some(format!(
                    "assistant_json:{}",
                    serde_json::to_string(&final_answer).map_err(|error| error.to_string())?
                )),
                None,
            )
        };
        let summary = failure
            .as_ref()
            .map(|failure| failure.message.clone())
            .or_else(|| {
                result_ref
                    .as_deref()
                    .and_then(|value| value.strip_prefix("assistant_json:"))
                    .and_then(|value| serde_json::from_str::<String>(value).ok())
            });
        Ok(NodeExecutionOutcome::new(ExecutionNodeResult {
            status,
            result_ref,
            summary,
            evidence_refs: evidence,
            failure,
            usage,
            finished_at_ms: crate::tool_invocation::now_ms(),
        }))
    }
}

fn missing_optional_terminal_is_ignorable(
    graph: &harness_contract::execution_graph::ExecutionGraph,
    node: &harness_contract::execution_graph::ExecutionNodeSpec,
) -> bool {
    node.work.as_ref().is_some_and(|work| !work.required)
        && graph
            .node_statuses
            .get(&node.id)
            .is_some_and(|status| status.is_terminal())
}

impl SynthesizeBackendResolver for TeamResultReducer {
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        ticket.payload_ref.starts_with("team:").then(|| {
            Arc::new(Self {
                state_store: self.state_store.clone(),
                agents: Arc::clone(&self.agents),
                coalescer: Arc::clone(&self.coalescer),
            }) as Arc<dyn SynthesizeBackend>
        })
    }
}

#[async_trait]
impl SynthesizeBackend for TeamResultReducer {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let graph_revision = self
            .state_store
            .load_async(ticket.graph_id.clone())
            .await
            .map_err(|error| error.to_string())?
            .revision;
        let key = ImmutableWorkKey {
            authority_scope: "runtime:team-reducer".to_string(),
            session_scope: ticket.graph_id.clone(),
            source_revision: format!("graph:{graph_revision}:attempt:{}", ticket.attempt),
            model_profile: ticket.executor_kind.clone(),
            prompt_contract: ticket.idempotency_key.clone(),
            evidence_digest: ticket.payload_ref.clone(),
        };
        self.coalescer
            .run(key, || self.synthesize_uncached(ticket))
            .await
            .map(|result| result.value)
    }
}

fn render_terminal_outcome(outcome: &str) -> String {
    let Ok(serde_json::Value::Object(mut fields)) =
        serde_json::from_str::<serde_json::Value>(outcome)
    else {
        return outcome.trim().to_string();
    };
    let mut sections = Vec::new();
    if let Some(summary) = fields.remove("summary") {
        append_rendered_field(&mut sections, None, summary);
    }
    for (field, heading) in [
        ("findings", "Findings"),
        ("plan", "Plan"),
        ("proposal", "Proposal"),
        ("critique", "Critique"),
        ("checkpoint", "Checkpoint"),
        ("implementation", "Implementation"),
        ("mitigation", "Mitigation"),
        ("review", "Review"),
        ("risks", "Risks"),
        ("unresolved", "Unresolved"),
        ("evidence", "Evidence"),
    ] {
        if let Some(value) = fields.remove(field) {
            append_rendered_field(&mut sections, Some(heading), value);
        }
    }
    for (field, value) in fields {
        let heading = field
            .split('_')
            .filter(|part| !part.is_empty())
            .map(|part| {
                let mut chars = part.chars();
                chars.next().map_or_else(String::new, |first| {
                    first.to_uppercase().collect::<String>() + chars.as_str()
                })
            })
            .collect::<Vec<_>>()
            .join(" ");
        append_rendered_field(&mut sections, Some(&heading), value);
    }
    if sections.is_empty() {
        outcome.trim().to_string()
    } else {
        sections.join("\n\n")
    }
}

fn append_rendered_field(
    sections: &mut Vec<String>,
    heading: Option<&str>,
    value: serde_json::Value,
) {
    let body = render_structured_value(&value);
    if body.trim().is_empty() {
        return;
    }
    sections.push(heading.map_or(body.clone(), |heading| format!("## {heading}\n{body}")));
}

fn render_structured_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.trim().to_string(),
        serde_json::Value::Array(values) => values
            .iter()
            .filter_map(|value| {
                let rendered = render_structured_value(value);
                (!rendered.is_empty()).then(|| format!("- {}", rendered.replace('\n', "\n  ")))
            })
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Object(fields) => fields
            .iter()
            .filter_map(|(key, value)| {
                let rendered = render_structured_value(value);
                (!rendered.is_empty()).then(|| {
                    if rendered.contains('\n') {
                        format!("- **{key}**:\n  {}", rendered.replace('\n', "\n  "))
                    } else {
                        format!("- **{key}**: {rendered}")
                    }
                })
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn terminal_agent_node_ids(
    graph: &harness_contract::execution_graph::ExecutionGraph,
) -> BTreeSet<String> {
    let agent_nodes = graph
        .nodes
        .iter()
        .filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask)
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    agent_nodes
        .iter()
        .filter(|node_id| {
            !graph.edges.iter().any(|edge| {
                edge.from == **node_id
                    && edge.kind == harness_contract::execution_graph::ExecutionEdgeKind::DependsOn
                    && agent_nodes.contains(edge.to.as_str())
            })
        })
        .map(|node_id| (*node_id).to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionNodeStatus, ExecutionWorkContract, ExecutionWorkRole,
    };

    use super::{
        missing_optional_terminal_is_ignorable, render_terminal_outcome, terminal_agent_node_ids,
    };

    #[test]
    fn only_topology_terminal_agent_publishes_the_team_answer() {
        let mut graph = ExecutionGraph::new("research");
        let mut researcher =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        researcher.id = "researcher".into();
        let mut synthesizer =
            ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        synthesizer.id = "synthesizer".into();
        graph.nodes.extend([researcher, synthesizer]);
        graph.edges.push(ExecutionEdge {
            from: "researcher".into(),
            to: "synthesizer".into(),
            kind: ExecutionEdgeKind::DependsOn,
        });

        assert_eq!(
            terminal_agent_node_ids(&graph),
            std::collections::BTreeSet::from(["synthesizer".to_string()])
        );
    }

    #[test]
    fn structured_terminal_outcome_becomes_user_facing_markdown() {
        let rendered = render_terminal_outcome(
            r#"{
                "summary":"Runtime, Memory, and Gateway have distinct canonical state boundaries.",
                "evidence":[
                    {"path":"crates/runtime/src/lib.rs","receipt":"tool://runtime"},
                    {"path":"crates/memory/src/lib.rs","receipt":"tool://memory"}
                ],
                "unresolved":["Verify commit-to-broadcast ordering."]
            }"#,
        );

        assert!(rendered
            .starts_with("Runtime, Memory, and Gateway have distinct canonical state boundaries."));
        assert!(rendered.contains("## Evidence"));
        assert!(rendered.contains("crates/runtime/src/lib.rs"));
        assert!(rendered.contains("crates/memory/src/lib.rs"));
        assert!(rendered.contains("## Unresolved"));
        assert!(!rendered.contains(r#""summary""#));
    }

    #[test]
    fn cancelled_optional_agent_does_not_block_team_reduction() {
        let mut graph = ExecutionGraph::new("quorum team");
        let mut optional = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        let mut work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        work.required = false;
        optional.work = Some(work);
        graph
            .node_statuses
            .insert(optional.id.clone(), ExecutionNodeStatus::Cancelled);

        assert!(missing_optional_terminal_is_ignorable(&graph, &optional));
    }
}
