#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use harness_contract::core::ExecutionPattern;
use harness_contract::execution_graph::{
    apply_node_transition, validate_execution_graph, ExecutionEdge, ExecutionEdgeKind,
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
};
use harness_contract::strategy::{
    decide_strategy, understand, StrategyExperienceRecord, StrategyExperienceStore, StrategyInput,
};
use runtime::eval_gate::{ScenarioCheck, ScenarioCheckKind, ScenarioSpec, ScenarioSuite};
use runtime::RuntimeAiKernel;

fn node(id: &str, kind: ExecutionNodeKind) -> ExecutionNodeSpec {
    let mut node = ExecutionNodeSpec::new(kind, "scenario-executor", format!("payload:{id}"));
    node.id = id.to_string();
    node.idempotency_key = format!("scenario:{id}");
    node
}

#[test]
fn deep_task_closure_links_strategy_execution_graph_memory_matrix_and_final_gate() {
    let kernel = RuntimeAiKernel::begin_turn(
        "deep-task-closure",
        "完整迁移 gateway runtime service 并保证 matrix evidence、memory pulse、测试回归和最终审查",
        runtime::context_runtime::ContextProfile::MainTurn,
        &[
            "project context".to_string(),
            "matrix evidence required".to_string(),
        ],
    );
    let trace = kernel.finalize(
        "已完成：inspect -> change -> verify，并附带 matrix evidence 与 regression report",
        0,
        0,
    );

    let quality = trace
        .execution_graph_quality
        .as_ref()
        .expect("complex closure should produce execution_graph quality");
    assert_eq!(
        trace.execution_decision.strategy.pattern,
        ExecutionPattern::Execute
    );
    assert!(
        trace.execution_graph.is_some(),
        "complex task should allocate execution_graph"
    );
    assert!(quality.is_dag, "execution_graph must be acyclic");
    assert!(
        quality.has_verify_node,
        "execution graph must include verification"
    );
    assert!(
        quality.has_synthesize_node,
        "execution_graph must include synthesis"
    );
    assert!(
        trace.regression_gate.allowed,
        "verified closure should pass regression gate"
    );
    assert!(
        !trace.verification_blocked,
        "evidence-backed closure should finalize"
    );
    assert!(
        !trace.growth_event.matrix_signals.is_empty(),
        "closure should emit matrix-compatible growth signal"
    );

    let spec = ScenarioSpec::new("deep_task_closure", "complex closure")
        .expect_mode(ExecutionPattern::Execute)
        .require(ScenarioCheck::bool(
            "execution_graph.present",
            ScenarioCheckKind::ExecutionGraphPresent,
            true,
            "runtime-harness-contract",
            "complex closure must allocate a execution_graph",
        ))
        .require(ScenarioCheck::bool(
            "execution_graph.quality",
            ScenarioCheckKind::ExecutionGraphQualityOk,
            true,
            "ai-execution_graph",
            "repair execution_graph DAG/review/synthesis requirements",
        ))
        .require(ScenarioCheck::min_count(
            "matrix.signal_count",
            ScenarioCheckKind::MatrixSignalCount,
            1,
            "ai-growth/matrix",
            "emit matrix-compatible evidence before final synthesis",
        ));
    let observation = runtime::eval_gate::ScenarioObservation {
        scenario_id: "deep_task_closure".to_string(),
        strategy_pattern: trace.execution_decision.strategy.pattern,
        verification_blocked: trace.verification_blocked,
        regression_allowed: trace.regression_gate.allowed,
        has_execution_graph: trace.execution_graph.is_some(),
        execution_graph_quality_ok: quality.is_dag
            && quality.has_verify_node
            && quality.has_synthesize_node,
        growth_has_blocker: trace.learning_record.has_blocker(),
        growth_signal_kinds: trace
            .learning_record
            .signals
            .iter()
            .map(|signal| format!("{:?}", signal.kind))
            .collect(),
        memory_candidate_count: trace.growth_event.memory_candidates.len(),
        matrix_signal_count: trace.growth_event.matrix_signals.len(),
        assistant_text: "verified closure".to_string(),
    };
    let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);
    assert_eq!(report.failed, 0, "{report:?}");
}

#[test]
fn failure_repair_scenario_blocks_finalization_and_exposes_repair_owner() {
    let kernel = RuntimeAiKernel::begin_turn(
        "deep-failure-repair",
        "修复复杂运行时问题并最终确认",
        runtime::context_runtime::ContextProfile::MainTurn,
        &[],
    );
    let trace = kernel.finalize("", 0, 0);

    assert!(trace.verification_blocked);
    assert!(!trace.regression_gate.allowed);
    assert!(trace.learning_record.has_blocker());
    assert!(
        trace
            .learning_record
            .next_strategy_hints
            .iter()
            .any(|hint| hint.contains("supporting evidence")),
        "blocked finalization should produce actionable repair hint"
    );
    assert!(
        !trace.growth_event.memory_candidates.is_empty(),
        "failure should become reviewable memory candidate"
    );
}

#[test]
fn multi_agent_merge_scenario_records_conflict_and_blocks_false_consensus() {
    let mut graph = ExecutionGraph::new("并行分析 provider fallback 是否应该默认开启");
    graph.nodes = vec![
        node("research-a", ExecutionNodeKind::AgentTask),
        node("research-b", ExecutionNodeKind::AgentTask),
        node("verify-conflict", ExecutionNodeKind::Verify),
        node("synthesize", ExecutionNodeKind::Synthesize),
    ];
    graph.edges = vec![
        ExecutionEdge {
            from: "research-a".into(),
            to: "verify-conflict".into(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: "research-b".into(),
            to: "verify-conflict".into(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: "verify-conflict".into(),
            to: "synthesize".into(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];

    let waves = validate_execution_graph(&graph).unwrap();
    assert_eq!(waves[0], vec!["research-a", "research-b"]);
    assert_eq!(waves[1], vec!["verify-conflict"]);
    assert_eq!(waves[2], vec!["synthesize"]);
}

#[test]
fn agent_parallel_research_scenario_keeps_independent_nodes_ready_for_merge() {
    let mut graph = ExecutionGraph::new("并行调研 provider、runtime、matrix 三条链路并合并审查");
    graph.revision = 1;
    graph.nodes = vec![
        node("provider-research", ExecutionNodeKind::AgentTask),
        node("runtime-research", ExecutionNodeKind::AgentTask),
        node("matrix-research", ExecutionNodeKind::AgentTask),
        node("merge-review", ExecutionNodeKind::Synthesize),
    ];
    graph.edges = ["provider-research", "runtime-research", "matrix-research"]
        .into_iter()
        .map(|from| ExecutionEdge {
            from: from.into(),
            to: "merge-review".into(),
            kind: ExecutionEdgeKind::DependsOn,
        })
        .collect();
    graph.node_statuses = graph
        .nodes
        .iter()
        .map(|node| (node.id.clone(), ExecutionNodeStatus::Planned))
        .collect();

    for id in ["provider-research", "runtime-research", "matrix-research"] {
        graph = apply_node_transition(&graph, graph.revision, id, ExecutionNodeStatus::Ready, None)
            .unwrap();
        graph = apply_node_transition(
            &graph,
            graph.revision,
            id,
            ExecutionNodeStatus::Running,
            None,
        )
        .unwrap();
        graph = apply_node_transition(
            &graph,
            graph.revision,
            id,
            ExecutionNodeStatus::Completed,
            None,
        )
        .unwrap();
    }
    assert_eq!(
        graph.node_statuses["merge-review"],
        ExecutionNodeStatus::Planned
    );
    assert!(graph
        .edges
        .iter()
        .all(|edge| graph.node_statuses[&edge.from] == ExecutionNodeStatus::Completed));
}

#[test]
fn agent_low_lift_downgrade_keeps_simple_work_out_of_multi_agent_path() {
    let prompt = "使用多 Agent 协同完成复杂架构分析";
    let understanding = understand(&StrategyInput::from_prompt(prompt));
    let mut store = StrategyExperienceStore::new();
    for idx in 0..3 {
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_pattern: harness_contract::core::ExecutionPattern::Collaborate,
            succeeded: idx == 0,
            verification_blocked: false,
            context_pressure: false,
            multi_agent_positive_lift: false,
            created_at_ms: idx,
        });
    }

    let adapted = decide_strategy(&store.enrich_input(StrategyInput::from_prompt(prompt)));
    assert_eq!(
        adapted.pattern,
        harness_contract::core::ExecutionPattern::Execute
    );
    assert!(adapted
        .reasons
        .iter()
        .any(|reason| reason.contains("low multi-agent lift")));
}
