//! Review retirement preserves business evidence only for the original, single
//! reviewer node. Source revision CAS still advances to the actual durable head.
use std::sync::Arc;

use harness_contract::agent::{AgentTaskPacket, AgenticExecutionFocus};
use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeStatus};

use crate::execution_core::graph::{ExecutionGraphEvent, ExecutionNodeBinding};
use crate::{AgenticProgramProjection, AgenticTaskStatus, RuntimeEventStore};

/// Parent continuations retain their original work and governed tool calls.
/// This does not waive their effect/material checks or the final source CAS.
pub(crate) fn graph_tail_preserves_work(
    store: &Arc<RuntimeEventStore>,
    source: &str,
    pinned: u64,
    current: u64,
    session_events: &[crate::DurableRuntimeEvent],
) -> Result<bool, String> {
    if pinned > current {
        return Err("graph cut moved backwards".into());
    }
    let records =
        crate::execution_core::goal::assessment::read_bounded_event_tail(store, source, 0, current)
            .map_err(|error| error.to_string())?;
    let mut graph = None;
    for record in records {
        if record.scope != crate::RuntimeEventScope::ExecutionGraph
            || record.schema_version != 2
            || record.actor.as_deref() != Some("execution_commit_service")
        {
            return Err("graph tail source identity changed".into());
        }
        let resolved = store.resolve_read_record(record)?;
        let event: ExecutionGraphEvent =
            serde_json::from_value(resolved.payload).map_err(|_| "graph tail body is invalid")?;
        if event.kind() != resolved.kind {
            return Err("graph tail kind changed".into());
        }
        let next = project_committed_graph_record(&event, graph.clone(), resolved.commit_cursor)?;
        if resolved.sequence > pinned {
            let Some(before) = graph.as_ref() else {
                return Ok(false);
            };
            if let Err(reason) =
                parent_transition_preserves_work(before, &next, &event, session_events)
            {
                return Err(format!(
                    "graph work changed: sequence={} kind={} reason={reason}",
                    resolved.sequence, resolved.kind
                ));
            }
        }
        graph = Some(next);
    }
    Ok(graph.is_some_and(|graph| graph.id == source && graph.revision == current))
}

fn parent_transition_preserves_work(
    before: &ExecutionGraph,
    after: &ExecutionGraph,
    event: &ExecutionGraphEvent,
    session_events: &[crate::DurableRuntimeEvent],
) -> Result<(), &'static str> {
    use harness_contract::execution_graph::{
        ExecutionNodeKind, ExecutionWorkContract, ExecutionWorkRole,
    };
    // Compare the complete immutable graph header, existing NodeSpecs, edges,
    // settled results and attempts. A semantic change cannot hide in a delta
    // carrying an ordinary lifecycle kind or a checkpoint.
    let mut normalized = after.clone();
    normalized.revision = before.revision;
    normalized.nodes = before.nodes.clone();
    normalized.edges = before.edges.clone();
    normalized.node_statuses = before.node_statuses.clone();
    normalized.node_results = before.node_results.clone();
    normalized.recovery_cursor = before.recovery_cursor.clone();
    if normalized != *before {
        return Err("immutable graph header");
    }
    if before
        .nodes
        .iter()
        .any(|node| after.nodes.iter().find(|next| next.id == node.id) != Some(node))
        || before.edges.iter().any(|edge| !after.edges.contains(edge))
        || before
            .node_results
            .iter()
            .any(|(id, result)| after.node_results.get(id) != Some(result))
        || before
            .recovery_cursor
            .node_attempts
            .iter()
            .any(|(id, attempt)| {
                *attempt > 0 && after.recovery_cursor.node_attempts.get(id) != Some(attempt)
            })
    {
        return Err("existing work, result or attempt changed");
    }
    let origin = match event {
        ExecutionGraphEvent::NodeTransitionedAndReplanned {
            node_id,
            added_node_ids,
            ..
        } => {
            let added = after
                .nodes
                .iter()
                .filter(|node| !before.nodes.iter().any(|old| old.id == node.id))
                .map(|node| node.id.clone())
                .collect::<Vec<_>>();
            if &added != added_node_ids {
                return Err("continuation node list");
            }
            before.nodes.iter().find(|node| &node.id == node_id)
        }
        ExecutionGraphEvent::NodesStarted { .. }
        | ExecutionGraphEvent::NodesTransitioned { .. }
        | ExecutionGraphEvent::NodeTransitioned { .. }
        | ExecutionGraphEvent::Recovered { .. } => None,
        ExecutionGraphEvent::Checkpoint { cause, .. }
            if matches!(
                cause.as_str(),
                "execution_graph.nodes_started"
                    | "execution_graph.nodes_transitioned"
                    | "execution_graph.node_transitioned"
                    | "execution_graph.node_transitioned_and_replanned"
                    | "execution_graph.recovered"
            ) =>
        {
            None
        }
        _ => return Err("unrecognised graph lifecycle or continuation kind"),
    };
    let mut predecessor = origin.map(|node| node.id.clone());
    let mut expected_edges = before.edges.clone();
    for node in after
        .nodes
        .iter()
        .filter(|node| !before.nodes.iter().any(|old| old.id == node.id))
    {
        let Some(origin) = origin else {
            return Err("new work outside original continuation");
        };
        let role = match node.kind {
            ExecutionNodeKind::InlineModel if node.payload_ref == origin.payload_ref => {
                ExecutionWorkRole::EvidenceAnalyze
            }
            ExecutionNodeKind::Synthesize if node.payload_ref == origin.payload_ref => {
                ExecutionWorkRole::Synthesize
            }
            ExecutionNodeKind::ToolBatch
                if continuation_calls_are_original(
                    &node.payload_ref,
                    &before.id,
                    session_events,
                ) =>
            {
                ExecutionWorkRole::Tool
            }
            _ => return Err("unrecognised graph lifecycle or continuation kind"),
        };
        if node.lease_ref.is_some()
            || node.acceptance != Default::default()
            || node.retry_policy != Default::default()
            || !node.resource_scopes.is_empty()
            || node.work != Some(ExecutionWorkContract::new(role))
            || node.idempotency_key != format!("{}:attempt", node.id)
            || node.executor_kind
                != match node.kind {
                    ExecutionNodeKind::InlineModel => "inline_model",
                    ExecutionNodeKind::Synthesize => {
                        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND
                    }
                    _ => "tool_batch",
                }
        {
            return Err("continuation work contract");
        }
        expected_edges.push(harness_contract::execution_graph::ExecutionEdge {
            from: predecessor.take().unwrap(),
            to: node.id.clone(),
            kind: harness_contract::execution_graph::ExecutionEdgeKind::DependsOn,
        });
        predecessor = Some(node.id.clone());
    }
    if after.edges == expected_edges {
        Ok(())
    } else {
        Err("continuation edges changed")
    }
}

fn continuation_calls_are_original(
    payload: &str,
    graph: &str,
    events: &[crate::DurableRuntimeEvent],
) -> bool {
    let Ok((calls, _)) = crate::host::decode_tool_batch(payload) else {
        return false;
    };
    !calls.is_empty()
        && calls.iter().all(|call| {
            let Some(binding) = &call.binding else {
                return false;
            };
            let Ok(input) = serde_json::from_str::<serde_json::Value>(&call.input) else {
                return false;
            };
            binding.snapshot.identity.execution_id.as_deref() == Some(graph)
                && events.iter().any(|event| {
                    event.actor.as_deref() == Some("conversation_runtime")
                        && event.stream_id
                            == format!("session:{}", binding.snapshot.identity.session_id)
                        && event.kind == "tool.execution_plan.created"
                        && event.payload["tasks"].as_array().is_some_and(|tasks| {
                            tasks.iter().any(|task| {
                                task["tool_call_id"] == call.id
                                && task["tool_name"] == call.name
                                && task["normalized_input"] == input
                                && serde_json::from_value::<crate::step_snapshot::StepCallBinding>(
                                    task["step_binding"].clone(),
                                )
                                .ok()
                                .as_ref()
                                    == Some(binding)
                            })
                        })
                })
        })
}

pub(crate) fn graph_tail_is_own_review_retirement(
    store: &Arc<RuntimeEventStore>,
    program: &AgenticProgramProjection,
    source: &str,
    pinned: u64,
    current: u64,
    packet: Option<&AgentTaskPacket>,
) -> Result<bool, String> {
    let Some(packet) = packet.filter(|packet| packet.graph_id() == source) else {
        return Ok(false);
    };
    if current.checked_sub(pinned) != Some(2) || packet.session_id() != program.session_id {
        return Ok(false);
    }
    let Some(scope) = packet.agentic_binding.as_ref() else {
        return Ok(false);
    };
    let task_ref = match &scope.focus {
        AgenticExecutionFocus::TaskExecute { task_ref }
        | AgenticExecutionFocus::TaskReview { task_ref } => task_ref,
        _ => return Ok(false),
    };
    if scope.program_id != program.program_id
        || program.tasks.get(task_ref).is_none_or(|task| {
            task.status != AgenticTaskStatus::Withdrawn
                || task
                    .cancel_requested_by
                    .as_ref()
                    .is_none_or(String::is_empty)
                || task.cancel_evidence_refs.is_empty()
        })
    {
        return Ok(false);
    }
    let records =
        crate::execution_core::goal::assessment::read_bounded_event_tail(store, source, 0, current)
            .map_err(|error| error.to_string())?;
    let mut before: Option<ExecutionGraph> = None;
    let mut tail = Vec::new();
    for record in records {
        if record.scope != crate::RuntimeEventScope::ExecutionGraph
            || record.schema_version != 2
            || record.actor.as_deref() != Some("execution_commit_service")
        {
            return Ok(false);
        }
        let resolved = store.resolve_read_record(record)?;
        let event: ExecutionGraphEvent = serde_json::from_value(resolved.payload.clone())
            .map_err(|_| "review retirement graph body is invalid")?;
        if event.kind() != resolved.kind {
            return Ok(false);
        }
        if resolved.sequence <= pinned {
            before = Some(project_committed_graph_record(
                &event,
                before,
                resolved.commit_cursor,
            )?);
        } else {
            tail.push((resolved, event));
        }
    }
    let Some(before) = before else {
        return Ok(false);
    };
    if before.id != source
        || before.revision != pinned
        || before.nodes.len() != 1
        || before.nodes[0].id != packet.node_id()
        || serde_json::from_str::<AgentTaskPacket>(&before.nodes[0].payload_ref)
            .ok()
            .as_ref()
            != Some(packet)
        || before.recovery_cursor.node_attempts.get(packet.node_id()) != Some(&packet.attempt)
        || before.node_statuses.get(packet.node_id()) != Some(&ExecutionNodeStatus::Running)
        || tail.len() != 2
    {
        return Ok(false);
    }
    let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(store));
    let binding = graphs
        .current_binding(&before, packet.node_id(), packet.attempt)
        .map_err(|error| error.to_string())?;
    let Some(start) = graphs
        .binding_start_position(&before, packet.node_id(), &binding)
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    if !retirement_delta_preserves_work(&before, &tail[0].1, &tail[1].1, packet.node_id())? {
        return Ok(false);
    }
    if !retirement_closes_original_binding(&tail[1].1, &before, packet.node_id(), &binding, start) {
        return Ok(false);
    }
    let key = binding.lease_closure_key(&before, packet.node_id());
    if tail[1].0.idempotency_key.as_deref() != Some(key.as_str())
        || tail[1].0.commit_cursor <= tail[0].0.commit_cursor
    {
        return Ok(false);
    }
    graphs
        .binding_lease_closure(&before, packet.node_id(), &binding, start)
        .map_err(|error| error.to_string())
}

/// Graph deltas are prepared before append. As in GraphStateStore, the
/// committed envelope supplies the recovery cursor after each projection.
fn project_committed_graph_record(
    event: &ExecutionGraphEvent,
    previous: Option<ExecutionGraph>,
    commit_cursor: u64,
) -> Result<ExecutionGraph, String> {
    let mut graph = event.project(previous)?;
    graph.recovery_cursor.commit_cursor = commit_cursor;
    Ok(graph)
}

fn retirement_closes_original_binding(
    event: &ExecutionGraphEvent,
    graph: &ExecutionGraph,
    node: &str,
    binding: &ExecutionNodeBinding,
    start: (u64, u64, u32),
) -> bool {
    match event {
        ExecutionGraphEvent::Recovered { lease_closures, .. } if lease_closures.len() == 1 => {
            let closure = &lease_closures[0];
            closure.matches(graph, node, binding, start) && closure.start_sequence <= graph.revision
        }
        _ => false,
    }
}

fn retirement_delta_preserves_work(
    before: &ExecutionGraph,
    command: &ExecutionGraphEvent,
    recovery: &ExecutionGraphEvent,
    node: &str,
) -> Result<bool, String> {
    if !matches!(command, ExecutionGraphEvent::CommandApplied { command, .. } if command == "cancel")
        || !matches!(recovery, ExecutionGraphEvent::Recovered { recovered_nodes, blocked_nodes, lease_closures, .. }
            if recovered_nodes.is_empty() && blocked_nodes.is_empty() && lease_closures.len() == 1)
    {
        return Ok(false);
    }
    let cancelled = command.project(Some(before.clone()))?;
    let mut expected = before.clone();
    expected.revision += 1;
    expected
        .node_statuses
        .insert(node.to_owned(), ExecutionNodeStatus::Cancelled);
    if cancelled != expected {
        return Ok(false);
    }
    let closed = recovery.project(Some(cancelled.clone()))?;
    expected.revision += 1;
    expected.recovery_cursor.commit_cursor = closed.recovery_cursor.commit_cursor;
    Ok(closed == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::execution_graph::{ExecutionNodeKind, ExecutionNodeSpec};

    #[test]
    fn r1_incomplete_graph_tail_cannot_grant_a_lifecycle_exemption() {
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        assert!(graph_tail_preserves_work(&store, "missing-graph", 1, 2, &[]).is_err());
    }

    fn parent_fixture() -> (ExecutionGraph, serde_json::Value) {
        let mut graph = ExecutionGraph::new("preserve user objective");
        graph.id = "parent".into();
        graph.revision = 1;
        let mut model = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "original-session-input",
        );
        model.id = "model".into();
        graph.nodes.push(model);
        let continuation = serde_json::json!({
            "id":"parent:2:model", "kind":"inline_model", "executor_kind":"inline_model",
            "payload_ref":"original-session-input", "idempotency_key":"parent:2:model:attempt",
            "lease_ref":null, "acceptance":{"criteria":[],"required_evidence":[],"minimum_score_basis_points":null},
            "retry_policy":{"max_attempts":1,"retryable_failure_kinds":[],"base_backoff_ms":500,"maximum_backoff_ms":30000}, "resource_scopes":[], "work":{"role":"evidence_analyze"}
        });
        let event = serde_json::json!({
            "event":"node_transitioned_and_replanned", "node_id":"model", "from":"running", "to":"completed",
            "result":{"status":"completed","result_ref":null,"evidence_refs":[],"failure":null,"usage":{"input_tokens":0,"output_tokens":0,"tool_calls":0,"duration_ms":0},"finished_at_ms":1},
            "reason":"ordinary model continuation", "added_node_ids":["parent:2:model"],
            "delta":{"revision":2,"recovery_cursor":{"commit_cursor":1,"node_attempts":{}},
                "added_nodes":[continuation], "added_edges":[{"from":"model","to":"parent:2:model","kind":"depends_on"}]}
        });
        (graph, event)
    }

    #[test]
    fn r1_parent_continuation_preserves_original_work() {
        let (before, value) = parent_fixture();
        let event: ExecutionGraphEvent = serde_json::from_value(value).unwrap();
        let after = project_committed_graph_record(&event, Some(before.clone()), 2).unwrap();
        assert!(parent_transition_preserves_work(&before, &after, &event, &[]).is_ok());
    }

    #[test]
    fn r1_foreign_replan_cannot_hide_payload_binding_or_required_work_changes() {
        let (before, value) = parent_fixture();
        let event: ExecutionGraphEvent = serde_json::from_value(value).unwrap();
        let after = project_committed_graph_record(&event, Some(before.clone()), 2).unwrap();
        for field in [
            "payload",
            "executor",
            "work",
            "binding",
            "objective",
            "unbound_tool",
        ] {
            let mut changed = after.clone();
            match field {
                "payload" => changed.nodes[0].payload_ref = "foreign input".into(),
                "executor" => changed.nodes[0].executor_kind = "foreign executor".into(),
                "work" => {
                    changed.nodes[1].work = Some(
                        harness_contract::execution_graph::ExecutionWorkContract::new(
                            harness_contract::execution_graph::ExecutionWorkRole::Verify,
                        ),
                    )
                }
                "binding" => changed
                    .recovery_cursor
                    .node_attempts
                    .insert("model".into(), 2)
                    .map(|_| ())
                    .unwrap_or(()),
                "objective" => changed.objective = "different user task".into(),
                "unbound_tool" => {
                    changed.nodes[1].kind = ExecutionNodeKind::ToolBatch;
                    changed.nodes[1].payload_ref = "{}".into();
                }
                _ => unreachable!(),
            }
            let mut pinned = before.clone();
            if field == "binding" {
                pinned
                    .recovery_cursor
                    .node_attempts
                    .insert("model".into(), 1);
            }
            assert!(
                parent_transition_preserves_work(&pinned, &changed, &event, &[]).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn r1_foreign_graph_command_and_checkpoint_stale_without_a_business_delta() {
        let (before, _) = parent_fixture();
        let mut after = before.clone();
        after.revision += 1;
        let command: ExecutionGraphEvent = serde_json::from_value(serde_json::json!({
            "event":"command_applied", "command":"cancel", "reason":"foreign caller",
            "delta":{"revision":2,"recovery_cursor":{"commit_cursor":1,"node_attempts":{}}}
        }))
        .unwrap();
        assert!(parent_transition_preserves_work(&before, &after, &command, &[]).is_err());
        let checkpoint = ExecutionGraphEvent::Checkpoint {
            cause: "execution_graph.command_applied".into(),
            graph: after.clone(),
            bindings: Default::default(),
            lease_closures: vec![],
        };
        assert!(parent_transition_preserves_work(&before, &after, &checkpoint, &[]).is_err());
    }

    #[test]
    fn r1_noncommand_semantic_change_and_unknown_checkpoint_stale() {
        let (before, _) = parent_fixture();
        let mut after = before.clone();
        after.revision += 1;
        let checkpoint = ExecutionGraphEvent::Checkpoint {
            cause: "unknown producer".into(),
            graph: after.clone(),
            bindings: Default::default(),
            lease_closures: vec![],
        };
        assert!(parent_transition_preserves_work(&before, &after, &checkpoint, &[]).is_err());
        let event: ExecutionGraphEvent = serde_json::from_value(serde_json::json!({
            "event":"nodes_transitioned", "node_ids":["model"],
            "delta":{"revision":2,"objective":"foreign business mutation", "recovery_cursor":{"commit_cursor":1,"node_attempts":{}}}
        })).unwrap();
        let after = event.project(Some(before.clone())).unwrap();
        assert!(parent_transition_preserves_work(&before, &after, &event, &[]).is_err());
        assert!(serde_json::from_value::<ExecutionGraphEvent>(
            serde_json::json!({"event":"unknown"})
        )
        .is_err());
    }

    #[test]
    fn r1_graph_retirement_preserves_work_and_refuses_foreign_mutations() {
        let mut graph = ExecutionGraph::new("independent verification");
        graph.id = "review-graph".into();
        graph.revision = 3;
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            "immutable-packet",
        );
        node.id = "review-node".into();
        graph.nodes.push(node);
        graph
            .node_statuses
            .insert("review-node".into(), ExecutionNodeStatus::Running);
        graph
            .recovery_cursor
            .node_attempts
            .insert("review-node".into(), 1);
        let command_json = serde_json::json!({
            "event":"command_applied", "command":"cancel", "reason":"retirement",
            "delta":{"revision":4,"node_status_updates":{"review-node":"cancelled"},
                "recovery_cursor":{"commit_cursor":3,"node_attempts":{"review-node":1}}}
        });
        // A real start delta holds the PREVIOUS commit cursor. Its envelope
        // assigns cursor 3. Replaying the delta alone gives the wrong source
        // frame and would reject the following legal cancellation.
        graph.revision = 2;
        graph.recovery_cursor.commit_cursor = 2;
        graph
            .node_statuses
            .insert("review-node".into(), ExecutionNodeStatus::Ready);
        let started: ExecutionGraphEvent = serde_json::from_value(serde_json::json!({
            "event":"nodes_started", "bindings":{},
            "delta":{"revision":3,"node_status_updates":{"review-node":"running"},
                "recovery_cursor":{"commit_cursor":2,"node_attempts":{"review-node":1}}}
        }))
        .unwrap();
        graph = project_committed_graph_record(&started, Some(graph), 3).unwrap();
        assert_eq!(graph.recovery_cursor.commit_cursor, 3);
        let recovery_json = serde_json::json!({
            "event":"recovered", "recovered_nodes":[], "blocked_nodes":[],
            "lease_closures":[{"schema":1,"graph_id":"review-graph","node_id":"review-node",
                "binding":{"executor_kind":"agent_task","ticket_idempotency_key":"original-ticket",
                    "attempt":1,"resource_lease_refs":[],"scope_lease_ref":null,"worktree_lease_ref":null},
                "start_sequence":3,"start_cursor":3,"start_transaction_index":0,"source":"normal_release"}],
            "delta":{"revision":5,"recovery_cursor":{"commit_cursor":4,"node_attempts":{"review-node":1}}}
        });
        let command: ExecutionGraphEvent = serde_json::from_value(command_json.clone()).unwrap();
        let recovery: ExecutionGraphEvent = serde_json::from_value(recovery_json.clone()).unwrap();
        assert!(
            retirement_delta_preserves_work(&graph, &command, &recovery, "review-node").unwrap()
        );
        for (field, value) in [
            (
                "objective",
                serde_json::json!("different business objective"),
            ),
            (
                "node_status_updates",
                serde_json::json!({"foreign-node":"cancelled"}),
            ),
            (
                "recovery_cursor",
                serde_json::json!({"commit_cursor":3,"node_attempts":{"review-node":2}}),
            ),
        ] {
            let mut mutated = command_json.clone();
            mutated["delta"][field] = value;
            let foreign: ExecutionGraphEvent = serde_json::from_value(mutated).unwrap();
            assert!(
                !retirement_delta_preserves_work(&graph, &foreign, &recovery, "review-node")
                    .unwrap(),
                "{field} remains business relevant"
            );
        }
        let mut foreign_command = command_json;
        foreign_command["command"] = serde_json::json!("resume");
        assert!(!retirement_delta_preserves_work(
            &graph,
            &serde_json::from_value(foreign_command).unwrap(),
            &recovery,
            "review-node"
        )
        .unwrap());
        let mut foreign_recovery = recovery_json;
        foreign_recovery["recovered_nodes"] = serde_json::json!(["foreign-node"]);
        assert!(!retirement_delta_preserves_work(
            &graph,
            &command,
            &serde_json::from_value(foreign_recovery).unwrap(),
            "review-node"
        )
        .unwrap());
    }

    #[test]
    fn r1_graph_retirement_requires_the_entire_original_binding() {
        let mut graph = ExecutionGraph::new("review");
        graph.id = "review-graph".into();
        graph.revision = 3;
        let original = serde_json::json!({
            "event":"recovered", "recovered_nodes":[], "blocked_nodes":[],
            "lease_closures":[{"schema":1,"graph_id":"review-graph","node_id":"review-node",
                "binding":{"executor_kind":"agent_task","ticket_idempotency_key":"original-ticket",
                    "attempt":1,"resource_lease_refs":["original-lease"],"scope_lease_ref":null,"worktree_lease_ref":null},
                "start_sequence":3,"start_cursor":3,"start_transaction_index":0,"source":"normal_release"}],
            "delta":{"revision":5,"recovery_cursor":{"commit_cursor":4,"node_attempts":{"review-node":1}}}
        });
        let binding: ExecutionNodeBinding =
            serde_json::from_value(original["lease_closures"][0]["binding"].clone()).unwrap();
        let event: ExecutionGraphEvent = serde_json::from_value(original.clone()).unwrap();
        assert!(retirement_closes_original_binding(
            &event,
            &graph,
            "review-node",
            &binding,
            (3, 3, 0)
        ));
        for (field, value) in [
            ("graph_id", serde_json::json!("foreign-graph")),
            ("node_id", serde_json::json!("foreign-node")),
            ("start_cursor", serde_json::json!(99)),
            ("source", serde_json::json!("unknown")),
        ] {
            let mut mutated = original.clone();
            mutated["lease_closures"][0][field] = value;
            assert!(
                !retirement_closes_original_binding(
                    &serde_json::from_value(mutated).unwrap(),
                    &graph,
                    "review-node",
                    &binding,
                    (3, 3, 0)
                ),
                "{field}"
            );
        }
        for (field, value) in [
            ("attempt", serde_json::json!(2)),
            (
                "ticket_idempotency_key",
                serde_json::json!("foreign-ticket"),
            ),
            ("resource_lease_refs", serde_json::json!(["foreign-lease"])),
            ("executor_kind", serde_json::json!("foreign-executor")),
        ] {
            let mut mutated = original.clone();
            mutated["lease_closures"][0]["binding"][field] = value;
            assert!(
                !retirement_closes_original_binding(
                    &serde_json::from_value(mutated).unwrap(),
                    &graph,
                    "review-node",
                    &binding,
                    (3, 3, 0)
                ),
                "{field}"
            );
        }
    }
}
