use ai_eval::{ScenarioCheck, ScenarioCheckKind, ScenarioSpec, ScenarioSuite};
use ai_kernel::core::ExecutionMode;
use ai_kernel::strategy::{
    decide_strategy, understand, StrategyExperienceRecord, StrategyExperienceStore, StrategyInput,
};
use runtime::{
    AgentNodeStatus, AgentRole, AgentRunGraph, AgentTaskNode, ReviewVerdict, RuntimeAiKernel,
};

fn node(id: &str, role: AgentRole, deps: Vec<&str>) -> AgentTaskNode {
    AgentTaskNode {
        id: id.to_string(),
        role,
        title: id.to_string(),
        objective: format!("complete {id}"),
        depends_on: deps.into_iter().map(str::to_string).collect(),
        status: AgentNodeStatus::Pending,
        assigned_agent: None,
        result: None,
        error: None,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

#[test]
fn deep_task_closure_links_strategy_workgraph_memory_matrix_and_final_gate() {
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
        .workgraph_quality
        .as_ref()
        .expect("complex closure should produce workgraph quality");
    assert_eq!(trace.strategy.mode, ExecutionMode::PlanExecute);
    assert!(
        trace.workgraph.is_some(),
        "complex task should allocate workgraph"
    );
    assert!(quality.is_dag, "workgraph must be acyclic");
    assert!(quality.has_review_node, "workgraph must include review");
    assert!(
        quality.has_synthesis_node,
        "workgraph must include synthesis"
    );
    assert!(
        trace.regression_gate.allowed,
        "verified closure should pass regression gate"
    );
    assert!(
        !trace.finalization_blocked,
        "evidence-backed closure should finalize"
    );
    assert!(
        !trace.growth_event.matrix_signals.is_empty(),
        "closure should emit matrix-compatible growth signal"
    );

    let spec = ScenarioSpec::new("deep_task_closure", "complex closure")
        .expect_mode(ExecutionMode::PlanExecute)
        .require(ScenarioCheck::bool(
            "workgraph.present",
            ScenarioCheckKind::WorkgraphPresent,
            true,
            "runtime-ai-kernel",
            "complex closure must allocate a workgraph",
        ))
        .require(ScenarioCheck::bool(
            "workgraph.quality",
            ScenarioCheckKind::WorkgraphQualityOk,
            true,
            "ai-workgraph",
            "repair workgraph DAG/review/synthesis requirements",
        ))
        .require(ScenarioCheck::min_count(
            "matrix.signal_count",
            ScenarioCheckKind::MatrixSignalCount,
            1,
            "ai-growth/matrix",
            "emit matrix-compatible evidence before final synthesis",
        ));
    let observation = ai_eval::ScenarioObservation {
        scenario_id: "deep_task_closure".to_string(),
        strategy_mode: trace.strategy.mode,
        finalization_blocked: trace.finalization_blocked,
        regression_allowed: trace.regression_gate.allowed,
        has_workgraph: trace.workgraph.is_some(),
        workgraph_quality_ok: quality.is_dag
            && quality.has_review_node
            && quality.has_synthesis_node,
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

    assert!(trace.finalization_blocked);
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
    let mut graph = AgentRunGraph::new(
        "deep-agent-merge",
        "并行分析 provider fallback 是否应该默认开启",
    );
    graph
        .add_node(node("research-a", AgentRole::Researcher, vec![]))
        .unwrap();
    graph
        .add_node(node("research-b", AgentRole::Researcher, vec![]))
        .unwrap();
    graph
        .record_result("research-a", "fallback should retry 429")
        .unwrap();
    graph
        .record_result("research-b", "fallback should not retry auth errors")
        .unwrap();
    graph
        .add_evidence(
            "research-a",
            "provider_trace",
            "trace:429",
            "retryable throttling evidence",
        )
        .unwrap();
    graph
        .add_evidence(
            "research-b",
            "provider_trace",
            "trace:401",
            "non-retryable auth evidence",
        )
        .unwrap();
    graph
        .add_review(
            "research-a",
            "reviewer",
            ReviewVerdict::Accept,
            "429 evidence accepted",
        )
        .unwrap();
    graph
        .add_review(
            "research-b",
            "reviewer",
            ReviewVerdict::Challenge,
            "401 conflicts with blanket fallback",
        )
        .unwrap();
    let merge = graph.record_merge_decision(
        vec!["research-a".to_string(), "research-b".to_string()],
        "block blanket fallback; classify by provider error",
        vec!["retry policy differs between 429 and 401".to_string()],
    );

    assert_eq!(graph.evidence.len(), 2);
    assert_eq!(graph.reviews.len(), 2);
    assert_eq!(merge.conflicts.len(), 1);
    assert!(
        !merge.conflicts.is_empty(),
        "conflict must be surfaced instead of hidden by final answer"
    );
    assert_eq!(graph.status, AgentNodeStatus::Running);
    assert!(graph
        .merge_decisions
        .last()
        .unwrap()
        .decision
        .contains("classify by provider error"));
}

#[test]
fn agent_parallel_research_scenario_keeps_independent_nodes_ready_for_merge() {
    let mut graph = AgentRunGraph::new(
        "deep-agent-parallel",
        "并行调研 provider、runtime、matrix 三条链路并合并审查",
    );
    graph
        .add_node(node("provider-research", AgentRole::Researcher, vec![]))
        .unwrap();
    graph
        .add_node(node("runtime-research", AgentRole::Researcher, vec![]))
        .unwrap();
    graph
        .add_node(node("matrix-research", AgentRole::Researcher, vec![]))
        .unwrap();
    graph
        .add_node(node(
            "merge-review",
            AgentRole::Merger,
            vec!["provider-research", "runtime-research", "matrix-research"],
        ))
        .unwrap();

    let ready = graph
        .ready_nodes()
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    assert!(ready.contains(&"provider-research"));
    assert!(ready.contains(&"runtime-research"));
    assert!(ready.contains(&"matrix-research"));
    assert!(!ready.contains(&"merge-review"));

    for id in ["provider-research", "runtime-research", "matrix-research"] {
        graph
            .record_result(id, format!("{id} evidence ready"))
            .unwrap();
        graph
            .add_review(id, "reviewer", ReviewVerdict::Accept, "accepted")
            .unwrap();
    }

    let ready = graph
        .ready_nodes()
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ready, vec!["merge-review"]);
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
            selected_mode: ai_kernel::core::ExecutionMode::SupervisorSubagents,
            succeeded: idx == 0,
            verification_blocked: false,
            context_pressure: false,
            multi_agent_positive_lift: false,
            created_at_ms: idx,
        });
    }

    let adapted = decide_strategy(&store.enrich_input(StrategyInput::from_prompt(prompt)));
    assert_eq!(adapted.mode, ai_kernel::core::ExecutionMode::PlanExecute);
    assert!(adapted
        .reasons
        .iter()
        .any(|reason| reason.contains("low multi-agent lift")));
}
