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
    fn projection_exposes_safe_node_inspection_without_private_payloads() {
        let mut graph = ExecutionGraph::new("inspect a governed execution");
        let mut inspect = node("inspect", ExecutionNodeKind::ToolBatch);
        inspect.acceptance.criteria = vec!["verified output".to_string()];
        inspect.resource_scopes = vec!["workspace:read".to_string()];
        graph.nodes.push(inspect);
        graph.node_results.insert(
            "inspect".to_string(),
            ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("result:inspect".to_string()),
                summary: Some("inspection complete".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: ExecutionUsage::default(),
                finished_at_ms: 1,
            },
        );

        let node = &project_execution_graph(&graph).nodes[0];
        assert_eq!(node.payload_ref, "execution-payload:inspect");
        assert_eq!(node.acceptance.criteria, vec!["verified output"]);
        assert_eq!(node.resource_scopes, vec!["workspace:read"]);
        assert_eq!(node.summary.as_deref(), Some("inspection complete"));
        assert_eq!(node.result_ref.as_deref(), Some("result:inspect"));
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

    #[test]
    fn rejects_invalid_quorum_and_optional_effect_owners() {
        let mut graph = ExecutionGraph::new("govern optional work");
        let mut optional = node("optional", ExecutionNodeKind::ToolBatch);
        let mut optional_work = ExecutionWorkContract::new(ExecutionWorkRole::Tool);
        optional_work.required = false;
        optional.work = Some(optional_work);
        optional.resource_scopes.push("write:workspace".to_string());
        graph.nodes.push(optional);
        assert_eq!(
            validate_execution_graph(&graph),
            Err(ExecutionGraphValidationError::OptionalEffectOwner {
                node_id: "optional".to_string()
            })
        );

        let mut graph = ExecutionGraph::new("optional read-only tool");
        let mut optional = node("optional", ExecutionNodeKind::ToolBatch);
        let mut optional_work = ExecutionWorkContract::new(ExecutionWorkRole::Tool);
        optional_work.required = false;
        optional.work = Some(optional_work);
        optional.resource_scopes.push("read:workspace".to_string());
        graph.nodes.push(optional);
        assert!(validate_execution_graph(&graph).is_ok());

        let mut graph = ExecutionGraph::new("validate quorum");
        let source = node("source", ExecutionNodeKind::AgentTask);
        let mut consumer = node("consumer", ExecutionNodeKind::AgentTask);
        let mut work = ExecutionWorkContract::new(ExecutionWorkRole::Synthesize);
        work.dependency = ExecutionDependencyPolicy::Quorum {
            minimum: 2,
            cancel_remaining: false,
        };
        consumer.work = Some(work);
        graph.nodes = vec![source, consumer];
        graph.edges.push(ExecutionEdge {
            from: "source".to_string(),
            to: "consumer".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        assert!(matches!(
            validate_execution_graph(&graph),
            Err(ExecutionGraphValidationError::InvalidDependencyPolicy { .. })
        ));
    }

    #[test]
    fn work_projection_reports_parallel_shape_and_expected_speedup() {
        let mut graph = ExecutionGraph::new("parallel evidence");
        let mut left = node("left", ExecutionNodeKind::AgentTask);
        let mut right = node("right", ExecutionNodeKind::AgentTask);
        let mut merge = node("merge", ExecutionNodeKind::Synthesize);
        for node in [&mut left, &mut right] {
            let mut work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
            work.expected_duration_ms = 1_000;
            node.work = Some(work);
        }
        let mut work = ExecutionWorkContract::new(ExecutionWorkRole::Synthesize);
        work.expected_duration_ms = 500;
        merge.work = Some(work);
        graph.nodes = vec![left, right, merge];
        graph.edges = vec![
            ExecutionEdge {
                from: "left".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "right".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];
        let projection = project_work_graph(&graph).expect("work projection");
        assert_eq!(projection.width, 2);
        assert_eq!(projection.depth, 2);
        assert_eq!(projection.expected_serial_ms, 2_500);
        assert_eq!(projection.expected_critical_path_ms, 1_500);
        assert_eq!(projection.expected_speedup_basis_points, Some(16_666));
    }

    #[test]
    fn work_projection_uses_quorum_path_instead_of_optional_tail() {
        let mut graph = ExecutionGraph::new("quorum projection");
        let mut fast = node("fast", ExecutionNodeKind::AgentTask);
        let mut slow = node("slow", ExecutionNodeKind::AgentTask);
        let mut merge = node("merge", ExecutionNodeKind::Synthesize);
        let mut fast_work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        fast_work.expected_duration_ms = 1_000;
        fast_work.required = false;
        fast.work = Some(fast_work);
        let mut slow_work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        slow_work.expected_duration_ms = 5_000;
        slow_work.required = false;
        slow.work = Some(slow_work);
        let mut merge_work = ExecutionWorkContract::new(ExecutionWorkRole::Synthesize);
        merge_work.expected_duration_ms = 100;
        merge_work.dependency = ExecutionDependencyPolicy::Quorum {
            minimum: 1,
            cancel_remaining: false,
        };
        merge.work = Some(merge_work);
        graph.nodes = vec![fast, slow, merge];
        graph.edges = vec![
            ExecutionEdge {
                from: "fast".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
            ExecutionEdge {
                from: "slow".to_string(),
                to: "merge".to_string(),
                kind: ExecutionEdgeKind::DependsOn,
            },
        ];

        let projection = project_work_graph(&graph).expect("work projection");
        assert_eq!(projection.expected_serial_ms, 6_100);
        assert_eq!(projection.expected_critical_path_ms, 1_100);
    }

    #[test]
    fn public_work_projection_omits_private_context_and_model_bindings() {
        let mut graph = ExecutionGraph::new("safe projection");
        let mut model = node("model", ExecutionNodeKind::InlineModel);
        let mut work = ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze);
        work.context_view_ref = Some("private-context-lease".to_string());
        work.model_profile = Some("private-provider-profile".to_string());
        work.reasoning_effort = Some("private-reasoning-policy".to_string());
        work.required_evidence_refs = vec!["private-evidence-selector".to_string()];
        model.work = Some(work);
        model.payload_ref = "private-model-prompt-and-binding".to_string();
        graph.nodes.push(model);

        let encoded = serde_json::to_string(&project_execution_graph(&graph)).expect("projection");
        assert!(!encoded.contains("private-context-lease"));
        assert!(!encoded.contains("private-provider-profile"));
        assert!(!encoded.contains("private-reasoning-policy"));
        assert!(!encoded.contains("private-evidence-selector"));
        assert!(!encoded.contains("private-model-prompt-and-binding"));
        assert!(encoded.contains("execution-payload:model"));
    }
}
