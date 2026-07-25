#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use harness_contract::agent::AgentTaskIntent;
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind,
};
use runtime::execution_core::{
    compile_debate, compile_incident, compile_jps, compile_review_fix, validate_protocol_graph,
    ProtocolCompileRequest, ProtocolId, ProtocolRef, ProtocolRegistry,
};

fn request(protocol: ProtocolId, graph_id: &str) -> ProtocolCompileRequest {
    let mut request = ProtocolCompileRequest::new(
        ProtocolRef::new(protocol, 1),
        graph_id,
        "protocol-session",
        "Produce a bounded, evidence-backed decision.",
    );
    request.context_refs = vec!["context:current-turn".to_string()];
    request.allowed_tools = vec!["read_file".to_string()];
    request.allowed_skills = vec!["repository_analysis".to_string()];
    request.model_lease = "analysis-model".to_string();
    request.budget_lease_id = "test-protocol-budget".to_string();
    request.budget_tokens = 4_096;
    request.budget_revision = 3;
    request.resource_scopes = vec!["read:crates/runtime".to_string()];
    request
}

fn packet(node: &harness_contract::execution_graph::ExecutionNodeSpec) -> AgentTaskIntent {
    serde_json::from_str(&node.payload_ref).expect("canonical AgentTaskIntent")
}

fn role_nodes<'a>(graph: &'a ExecutionGraph, role: &str) -> Vec<&'a str> {
    graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        .filter(|node| {
            packet(node)
                .constraints
                .contains(&format!("protocol_role:{role}"))
        })
        .map(|node| node.id.as_str())
        .collect()
}

fn has_dependency(graph: &ExecutionGraph, from: &str, to: &str) -> bool {
    graph
        .edges
        .iter()
        .any(|edge| edge.from == from && edge.to == to && edge.kind == ExecutionEdgeKind::DependsOn)
}

#[test]
fn registry_contains_only_valid_available_v1_protocols() {
    ProtocolRegistry::validate().expect("valid registry");
    let protocols = ProtocolRegistry::all();
    assert!(!protocols.is_empty());
    assert!(protocols
        .iter()
        .all(|spec| spec.version == 1 && spec.availability.is_available()));
}

#[test]
fn all_compilers_emit_binding_ready_task_intents_before_runtime_materializes_packets() {
    for (protocol, graph_id) in [
        (ProtocolId::Debate, "protocol-debate"),
        (ProtocolId::Jps, "protocol-jps"),
        (ProtocolId::ReviewFix, "protocol-review-fix"),
        (ProtocolId::Incident, "protocol-incident"),
    ] {
        let request = request(protocol, graph_id);
        let graph = ProtocolRegistry::compile(&request).expect("compiled protocol graph");
        let spec = ProtocolRegistry::spec(protocol);
        validate_execution_graph(&graph).expect("execution graph DAG");
        validate_protocol_graph(&spec, &request, &graph).expect("protocol graph contract");
        assert_eq!(graph.id, graph_id);
        assert_eq!(
            graph
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Verify)
                .count(),
            1
        );
        assert_eq!(
            graph
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::Synthesize)
                .count(),
            1
        );
        for node in graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        {
            let task = packet(node);
            assert_eq!(task.graph_id, graph.id);
            assert_eq!(task.node_id, node.id);
            assert_eq!(task.expected_graph_revision, 0);
            assert_eq!(task.budget_lease.max_tokens, 4_096);
            assert!(task.definition_ref.is_none());
            assert!(task.selected_agent_id.is_none());
            assert!(task.granted_capabilities.is_empty());
            assert_eq!(
                task.constraints
                    .iter()
                    .any(|constraint| constraint == "protocol_allows_unresolved:true"),
                spec.stop_policy.allows_unresolved,
                "protocol {} must make its partial-result policy explicit in every role packet",
                spec.protocol_ref(),
            );
        }
        assert!(graph.nodes.iter().all(|node| {
            matches!(
                node.kind,
                ExecutionNodeKind::AgentTask
                    | ExecutionNodeKind::Verify
                    | ExecutionNodeKind::Synthesize
            )
        }));
    }
}

#[test]
fn debate_cross_reviews_other_proposals_and_arbitrates_by_evidence() {
    let request = request(ProtocolId::Debate, "protocol-debate-cross-review");
    let graph = compile_debate(&request).expect("debate graph");
    let proposers = role_nodes(&graph, "proposer");
    let critics = role_nodes(&graph, "critic");
    assert_eq!(proposers.len(), 2);
    assert_eq!(critics.len(), 2);
    assert!(has_dependency(&graph, proposers[1], critics[0]));
    assert!(has_dependency(&graph, proposers[0], critics[1]));
    assert!(!has_dependency(&graph, proposers[0], critics[0]));
    assert!(!has_dependency(&graph, proposers[1], critics[1]));

    let arbiter = graph
        .nodes
        .iter()
        .find(|node| role_nodes(&graph, "arbiter").contains(&node.id.as_str()))
        .expect("arbiter node");
    assert!(packet(arbiter)
        .acceptance
        .contains(&"evidence_arbitration".to_string()));
    assert!(role_nodes(&graph, "repair").is_empty());
    assert!(has_dependency(
        &graph,
        arbiter.id.as_str(),
        "protocol-debate-cross-review:verify"
    ));
}

#[test]
fn debate_adds_one_repair_node_only_when_requested() {
    let mut request = request(ProtocolId::Debate, "protocol-debate-repair");
    request.enable_repair = true;
    let graph = compile_debate(&request).expect("debate graph");
    let repair = role_nodes(&graph, "repair");
    assert_eq!(repair.len(), 1);
    assert_eq!(
        graph
            .nodes
            .iter()
            .find(|node| node.id == repair[0])
            .expect("repair node")
            .retry_policy
            .max_attempts,
        1
    );
    assert!(has_dependency(
        &graph,
        repair[0],
        "protocol-debate-repair:verify"
    ));
}

#[test]
fn jps_fans_out_independent_evidence_lanes_then_synthesizes_once() {
    let mut request = request(ProtocolId::Jps, "protocol-jps-fanout");
    request.fanout = 3;
    let graph = compile_jps(&request).expect("jps graph");
    let solutions = role_nodes(&graph, "solution");
    let synthesis = role_nodes(&graph, "decision_synthesis");
    assert_eq!(solutions.len(), 3);
    assert_eq!(synthesis.len(), 1);
    assert!(role_nodes(&graph, "frame").is_empty());
    assert!(role_nodes(&graph, "evaluation").is_empty());
    assert!(role_nodes(&graph, "conflict_matrix").is_empty());
    assert!(solutions
        .iter()
        .all(|solution| has_dependency(&graph, solution, synthesis[0])));
    assert!(packet(
        graph
            .nodes
            .iter()
            .find(|node| node.id == synthesis[0])
            .expect("synthesis node")
    )
    .acceptance
    .contains(&"conflict_resolution".to_string()));
}

#[test]
fn review_fix_uses_independent_reviews_before_one_bounded_fix() {
    let request = request(ProtocolId::ReviewFix, "protocol-review-fix");
    let graph = compile_review_fix(&request).expect("review-fix graph");
    let implementation = role_nodes(&graph, "implement");
    let reviewers = role_nodes(&graph, "review");
    let fix = role_nodes(&graph, "fix");
    assert_eq!(implementation.len(), 1);
    assert_eq!(reviewers.len(), 2);
    assert_eq!(fix.len(), 1);
    assert!(reviewers
        .iter()
        .all(|review| has_dependency(&graph, implementation[0], review)));
    assert!(reviewers
        .iter()
        .all(|review| has_dependency(&graph, review, fix[0])));
    assert_eq!(
        graph
            .nodes
            .iter()
            .find(|node| node.id == fix[0])
            .expect("fix node")
            .retry_policy
            .max_attempts,
        1
    );
}

#[test]
fn incident_collects_parallel_evidence_and_reports_partial_findings() {
    let request = request(ProtocolId::Incident, "protocol-incident-parallel-evidence");
    let graph = compile_incident(&request).expect("incident graph");
    let triage = role_nodes(&graph, "triage");
    let hypotheses = role_nodes(&graph, "hypotheses");
    let report = role_nodes(&graph, "report");
    assert_eq!(triage.len(), 1);
    assert_eq!(hypotheses.len(), 1);
    assert_eq!(report.len(), 1);
    for evidence_role in ["evidence_logs", "evidence_code", "evidence_state"] {
        let evidence = role_nodes(&graph, evidence_role);
        assert_eq!(evidence.len(), 1);
        assert!(has_dependency(&graph, triage[0], evidence[0]));
        assert!(has_dependency(&graph, evidence[0], hypotheses[0]));
    }
    assert!(packet(
        graph
            .nodes
            .iter()
            .find(|node| node.id == report[0])
            .expect("report node")
    )
    .acceptance
    .contains(&"unresolved".to_string()));
}

#[test]
fn validation_rejects_a_debate_critic_that_reads_its_own_proposal() {
    let request = request(ProtocolId::Debate, "protocol-debate-invalid-cross-review");
    let mut graph = compile_debate(&request).expect("debate graph");
    let proposer = role_nodes(&graph, "proposer")[0];
    let critic = role_nodes(&graph, "critic")[0];
    graph
        .edges
        .push(harness_contract::execution_graph::ExecutionEdge {
            from: proposer.to_string(),
            to: critic.to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        });
    assert!(validate_protocol_graph(
        &ProtocolRegistry::spec(ProtocolId::Debate),
        &request,
        &graph
    )
    .is_err());
}
