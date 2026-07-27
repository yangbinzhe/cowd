//! Canonical contracts for all stateful AI harness execution.

mod contract;
mod projection;
mod state;
mod validation;

pub use contract::*;
pub use projection::*;
pub use state::*;
pub use validation::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, kind: ExecutionNodeKind) -> ExecutionNodeSpec {
        let mut node = ExecutionNodeSpec::new(kind, "runtime", format!("payload:{id}"));
        node.id = id.to_string();
        node.idempotency_key = format!("idempotency:{id}");
        node
    }

    #[test]
    fn validates_and_batches_a_dependency_dag() {
        let mut graph = ExecutionGraph::new("answer with verified evidence");
        graph.nodes = vec![
            node("model", ExecutionNodeKind::InlineModel),
            node("verify", ExecutionNodeKind::Verify),
            node("synthesize", ExecutionNodeKind::Synthesize),
        ];
        graph.edges = vec![
            ExecutionEdge {
                from: "model".into(),
                to: "verify".into(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "verify".into(),
                to: "synthesize".into(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];

        let batches = validate_execution_graph(&graph).expect("valid graph");
        assert_eq!(
            batches,
            vec![vec!["model"], vec!["verify"], vec!["synthesize"]]
        );
        let projection = project_execution_graph(&graph);
        assert_eq!(projection.edges.len(), 2);
        assert_eq!(projection.edges[0].from, "model");
        assert_eq!(projection.edges[1].to, "synthesize");
    }

    #[test]
    fn rejects_dependency_cycles() {
        let mut graph = ExecutionGraph::new("cycle must fail");
        graph.nodes = vec![
            node("a", ExecutionNodeKind::InlineModel),
            node("b", ExecutionNodeKind::Verify),
        ];
        graph.edges = vec![
            ExecutionEdge {
                from: "a".into(),
                to: "b".into(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "b".into(),
                to: "a".into(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];

        assert_eq!(
            validate_execution_graph(&graph),
            Err(ExecutionGraphValidationError::Cycle)
        );
    }

    #[test]
    fn transition_is_revision_checked_and_pure() {
        let mut graph = ExecutionGraph::new("revision control");
        graph
            .nodes
            .push(node("model", ExecutionNodeKind::InlineModel));
        graph
            .node_statuses
            .insert("model".into(), ExecutionNodeStatus::Planned);

        let next = apply_node_transition(&graph, 0, "model", ExecutionNodeStatus::Ready, None)
            .expect("transition");
        assert_eq!(graph.revision, 0);
        assert_eq!(graph.node_statuses["model"], ExecutionNodeStatus::Planned);
        assert_eq!(next.revision, 1);
        assert_eq!(next.node_statuses["model"], ExecutionNodeStatus::Ready);
        assert!(matches!(
            apply_node_transition(&next, 0, "model", ExecutionNodeStatus::Running, None),
            Err(ExecutionTransitionError::StaleRevision { .. })
        ));
    }

    #[test]
    fn service_class_is_durable_projected_and_cannot_promote_a_child() {
        let mut graph = ExecutionGraph::new("managed background execution");
        graph.service_class = ExecutionServiceClass::Background;
        let encoded = serde_json::to_string(&graph).expect("encode graph");
        let decoded: ExecutionGraph = serde_json::from_str(&encoded).expect("decode graph");
        assert_eq!(decoded.service_class, ExecutionServiceClass::Background);
        assert_eq!(
            project_execution_graph(&decoded).service_class,
            ExecutionServiceClass::Background
        );
        assert_eq!(
            ExecutionServiceClass::Interactive.bounded_by(Some(ExecutionServiceClass::Background)),
            ExecutionServiceClass::Background
        );
        assert_eq!(
            ExecutionServiceClass::Maintenance.bounded_by(Some(ExecutionServiceClass::Foreground)),
            ExecutionServiceClass::Maintenance
        );
    }
}
