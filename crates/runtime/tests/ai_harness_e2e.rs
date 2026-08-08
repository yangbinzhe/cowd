#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::pin::Pin;

use futures::Stream;
use harness_contract::core::{ExecutionPattern, TaskComplexity, TaskRisk};
use harness_contract::growth::{
    GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthSeverity, GrowthSignal,
    GrowthSignalKind, LearningRecord,
};
use harness_contract::strategy::{
    decide_strategy, understand, ExecutionCandidateKind, StrategyExperienceRecord,
    StrategyExperienceStore, StrategyInput,
};
use matrix_core::{MatrixEvidencePacket, MatrixQualityGateDecision};
use runtime::eval_gate::{
    ScenarioCheck, ScenarioCheckKind, ScenarioObservation, ScenarioSpec, ScenarioSuite,
};
use runtime::{
    ApiClient, ApiRequest, AssistantEvent, ContentBlock, ConversationRuntime, PermissionMode,
    PermissionPolicy, RuntimeAiKernel, RuntimeAiKernelTrace, RuntimeError, Session, SharedPrompter,
    StaticToolExecutor, TurnSummary,
};

#[derive(Debug)]
struct HarnessObservation {
    scenario_id: String,
    strategy_pattern: ExecutionPattern,
    verification_blocked: bool,
    regression_allowed: bool,
    has_execution_graph: bool,
    execution_graph_quality_ok: bool,
    growth_has_blocker: bool,
    growth_signal_kinds: Vec<String>,
    memory_candidate_count: usize,
    matrix_signal_count: usize,
    assistant_text: String,
}

impl HarnessObservation {
    fn from_trace(
        scenario_id: impl Into<String>,
        trace: &RuntimeAiKernelTrace,
        assistant_text: impl Into<String>,
    ) -> Self {
        let execution_graph_quality_ok = trace
            .execution_graph_quality
            .as_ref()
            .map(|quality| quality.is_dag && quality.has_verify_node && quality.has_synthesize_node)
            .unwrap_or(false);
        Self {
            scenario_id: scenario_id.into(),
            strategy_pattern: trace.execution_decision.strategy.pattern,
            verification_blocked: trace.verification_blocked,
            regression_allowed: trace.regression_gate.allowed,
            has_execution_graph: trace.execution_graph.is_some(),
            execution_graph_quality_ok,
            growth_has_blocker: trace.learning_record.has_blocker(),
            growth_signal_kinds: trace
                .learning_record
                .signals
                .iter()
                .map(|signal| format!("{:?}", signal.kind))
                .collect(),
            memory_candidate_count: trace.growth_event.memory_candidates.len(),
            matrix_signal_count: trace.growth_event.matrix_signals.len(),
            assistant_text: assistant_text.into(),
        }
    }

    fn from_summary(scenario_id: impl Into<String>, summary: &TurnSummary) -> Self {
        Self::from_trace(
            scenario_id,
            &summary.ai_kernel_trace,
            summary
                .assistant_messages
                .iter()
                .flat_map(|message| message.blocks.iter())
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(""),
        )
    }

    fn into_scenario_observation(self) -> ScenarioObservation {
        ScenarioObservation {
            scenario_id: self.scenario_id,
            strategy_pattern: self.strategy_pattern,
            verification_blocked: self.verification_blocked,
            regression_allowed: self.regression_allowed,
            has_execution_graph: self.has_execution_graph,
            execution_graph_quality_ok: self.execution_graph_quality_ok,
            growth_has_blocker: self.growth_has_blocker,
            growth_signal_kinds: self.growth_signal_kinds,
            memory_candidate_count: self.memory_candidate_count,
            matrix_signal_count: self.matrix_signal_count,
            assistant_text: self.assistant_text,
        }
    }
}

#[test]
fn simple_question_stays_direct_and_clean() {
    let spec = ScenarioSpec::new("simple_question", "explain this function")
        .expect_mode(ExecutionPattern::Direct)
        .require(ScenarioCheck::bool(
            "verification.verification_blocked",
            ScenarioCheckKind::FinalizationBlocked,
            false,
            "ai-verification/runtime-conversation",
            "simple successful answers must finalize without the gate",
        ))
        .require(ScenarioCheck::bool(
            "execution_graph.present",
            ScenarioCheckKind::ExecutionGraphPresent,
            false,
            "ai-strategy/runtime-harness-contract",
            "simple direct answers should not allocate execution_graph",
        ))
        .require(ScenarioCheck::bool(
            "growth.blocker",
            ScenarioCheckKind::GrowthBlocker,
            false,
            "ai-growth",
            "clean simple traces should not produce blocker signals",
        ))
        .require(ScenarioCheck::min_count(
            "matrix.signal_count",
            ScenarioCheckKind::MatrixSignalCount,
            1,
            "ai-growth",
            "runtime trace should expose matrix-compatible growth signals",
        ));
    let kernel = RuntimeAiKernel::begin_turn(
        "harness-simple",
        "explain this function",
        runtime::context_runtime::ContextProfile::MainTurn,
        &["system prompt".to_string()],
    );

    let trace = kernel.finalize("done", 0, 0);
    let observation = HarnessObservation::from_trace("simple_question", &trace, "done")
        .into_scenario_observation();
    let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);

    assert!(report.verdicts[0].passed, "{:?}", report.verdicts[0]);
    assert_eq!(trace.growth_event.memory_candidates.len(), 0);
}

#[test]
fn complex_task_builds_execution_execution_graph() {
    let spec = ScenarioSpec::new(
        "complex_execution_graph",
        "使用多 Agent 分别规划 runtime、gateway service crate 的复杂架构演进并综合审查",
    )
    .expect_mode(ExecutionPattern::Collaborate)
    .require(ScenarioCheck::bool(
        "execution_graph.present",
        ScenarioCheckKind::ExecutionGraphPresent,
        true,
        "ai-strategy/runtime-harness-contract",
        "complex tasks must allocate a execution_graph",
    ))
    .require(ScenarioCheck::bool(
        "execution_graph.quality",
        ScenarioCheckKind::ExecutionGraphQualityOk,
        true,
        "ai-execution_graph",
        "complex execution_graph must be DAG with review and synthesis nodes",
    ))
    .require(ScenarioCheck::bool(
        "regression.allowed",
        ScenarioCheckKind::RegressionAllowed,
        true,
        "harness-eval",
        "successful complex trace must pass regression gate",
    ));
    let kernel = RuntimeAiKernel::begin_turn(
        "harness-complex",
        "使用多 Agent 分别规划 runtime、gateway service crate 的复杂架构演进并综合审查",
        runtime::context_runtime::ContextProfile::MainTurn,
        &[],
    );

    let trace = kernel.finalize("planned", 0, 0);
    assert_eq!(
        trace.execution_decision.strategy.selected_candidate,
        ExecutionCandidateKind::Team,
        "the independent runtime and gateway architecture workstreams must use the governed Team topology"
    );
    let observation = HarnessObservation::from_trace("complex_execution_graph", &trace, "planned")
        .into_scenario_observation();
    let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);

    assert!(report.verdicts[0].passed, "{:?}", report.verdicts[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_answer_is_blocked_by_finalization_gate() {
    let spec = ScenarioSpec::new("empty_answer_gate", "answer this")
        .require(ScenarioCheck::bool(
            "verification.verification_blocked",
            ScenarioCheckKind::FinalizationBlocked,
            true,
            "ai-verification/runtime-conversation",
            "empty answers must be blocked by finalization gate",
        ))
        .require(ScenarioCheck::bool(
            "regression.allowed",
            ScenarioCheckKind::RegressionAllowed,
            false,
            "harness-eval",
            "blocked finalization must fail regression gate",
        ))
        .require(ScenarioCheck::bool(
            "growth.blocker",
            ScenarioCheckKind::GrowthBlocker,
            true,
            "ai-growth",
            "blocked finalization must become a growth blocker",
        ))
        .require(ScenarioCheck::min_count(
            "memory.candidate_count",
            ScenarioCheckKind::MemoryCandidateCount,
            1,
            "ai-growth/memory-pulse",
            "blocked finalization should create a reviewable memory candidate",
        ))
        .require(ScenarioCheck::text_contains(
            "assistant.gate_message",
            "Execution could not obtain a usable final answer",
            "runtime-conversation",
            "append limitation message when verification blocks finalization",
        ));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        EmptyApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system prompt".to_string()],
    )
    .without_memory();
    runtime.set_active_model("test-model");

    let services = runtime::RuntimeServices::in_memory().expect("runtime services");
    let (_runtime, summary) = runtime::submit_owned_conversation_turn(
        runtime,
        services,
        "answer this",
        &SharedPrompter::none(),
        harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-eval".to_string(),
            turn_id: "turn-eval".to_string(),
            root_task_id: "task-eval".to_string(),
            task_id: "task-eval".to_string(),
            generation: 1,
        },
    )
    .await;
    let summary = summary.expect("turn should complete with a gate message");
    let observation =
        HarnessObservation::from_summary("empty_answer_gate", &summary).into_scenario_observation();
    let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);

    assert!(report.verdicts[0].passed, "{:?}", report.verdicts[0]);
}

#[test]
fn untrusted_multi_agent_experience_cannot_downgrade_next_route() {
    let prompt = "使用多 Agent 协同完成复杂架构分析";
    let initial = decide_strategy(&StrategyInput::from_prompt(prompt));
    assert_eq!(initial.pattern, ExecutionPattern::Collaborate);

    let understanding = understand(&StrategyInput::from_prompt(prompt));
    let mut store = StrategyExperienceStore::new();
    for created_at_ms in 0..4 {
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_pattern: ExecutionPattern::Collaborate,
            selected_candidate: Some(harness_contract::strategy::ExecutionCandidateKind::Team),
            succeeded: created_at_ms == 0,
            verification_blocked: false,
            context_pressure: false,
            composite_execution: false,
            multi_agent_positive_lift: false,
            created_at_ms,
            actual_duration_ms: 120,
            actual_input_tokens: 10,
            actual_output_tokens: 5,
            actual_cached_tokens: 0,
            actual_coordination_cost_ms: 3,
            paired_calibration: Some(
                harness_contract::strategy::PairedStrategyCalibrationEvidence {
                    evaluation_ref: format!(
                        "harness_eval.auto_strategy_paired.v1:e2e-{created_at_ms}"
                    ),
                    corpus_sha256: "a".repeat(64),
                    workspace_revision: "test-revision".to_string(),
                    provider_account_ref: "test-provider".to_string(),
                    baseline_pattern: ExecutionPattern::Direct,
                    baseline_duration_ms: 100,
                    baseline_quality_score_bp: 8_000,
                    candidate_duration_ms: 120,
                    candidate_quality_score_bp: 8_000,
                    blind_judge_completed: true,
                    baseline_total_tokens: 15,
                    candidate_total_tokens: 15,
                    candidate_duplicate_tool_ratio_bp: 0,
                    admission_channel: None,
                    report_sha256: "b".repeat(64),
                    rubric_sha256: "c".repeat(64),
                    binary_sha256: "d".repeat(64),
                    frontend_workspace_revision: "test-frontend".to_string(),
                    model_revision: "test-model".to_string(),
                    judge_model_revision: "test-judge".to_string(),
                    invariant_fingerprint: "e".repeat(64),
                },
            ),
        });
    }

    let enriched = store.enrich_input(StrategyInput::from_prompt(prompt));
    let adapted = decide_strategy(&enriched);

    assert_eq!(adapted.pattern, ExecutionPattern::Collaborate);
    assert_eq!(
        enriched
            .experience
            .as_ref()
            .map(|summary| summary.multi_agent_lift_sample_count),
        Some(0)
    );
}

#[test]
fn matrix_quality_failure_becomes_growth_signal() {
    let spec = ScenarioSpec::new("matrix_quality_failure", "quality gate failure")
        .expect_mode(ExecutionPattern::Execute)
        .require(ScenarioCheck::growth_signal(
            "growth.matrix_quality_gate",
            "MatrixQualityGate",
            "ai-growth",
            "matrix quality gate failures must map into growth signals",
        ))
        .require(ScenarioCheck::min_count(
            "memory.candidate_count",
            ScenarioCheckKind::MemoryCandidateCount,
            1,
            "ai-growth/memory-pulse",
            "matrix quality failures should produce reviewable memory candidates",
        ))
        .require(ScenarioCheck::min_count(
            "matrix.signal_count",
            ScenarioCheckKind::MatrixSignalCount,
            1,
            "ai-growth/matrix",
            "matrix quality failures should produce matrix-compatible growth signals",
        ));
    let packet = MatrixEvidencePacket::new("AI harness evidence is incomplete");
    let gate = MatrixQualityGateDecision::for_evidence_packet(&packet);
    assert_eq!(gate.decision, "fail");

    let signal = GrowthSignal::from_matrix_quality_gate(
        gate.decision == "pass",
        (gate.score.clamp(0.0, 1.0) * 10_000.0).round() as u16,
        &gate.reasons,
    );
    assert_eq!(signal.kind, GrowthSignalKind::MatrixQualityGate);
    assert_eq!(signal.severity, GrowthSeverity::Blocker);

    let record = LearningRecord {
        id: "learning-record-matrix-quality-e2e".to_string(),
        policy: "growth-policy-test".to_string(),
        signals: vec![signal],
        next_strategy_hints: vec!["raise evidence quality before final synthesis".to_string()],
    };
    let event = GrowthEvent::from_input(GrowthEventInput {
        session_id: "harness-matrix-quality".to_string(),
        source_event_kind: "matrix.quality_gate".to_string(),
        strategy_pattern: ExecutionPattern::Execute,
        learning_record: record,
        evidence_refs: vec![GrowthEvidenceRef::new(
            "matrix_quality_gate",
            gate.gate_id,
            "matrix evidence quality gate failed",
        )],
    });

    assert!(event
        .signals
        .iter()
        .any(|item| item.kind == GrowthSignalKind::MatrixQualityGate));
    let observation = ScenarioObservation {
        scenario_id: "matrix_quality_failure".to_string(),
        strategy_pattern: event.strategy_pattern,
        verification_blocked: false,
        regression_allowed: false,
        has_execution_graph: false,
        execution_graph_quality_ok: false,
        growth_has_blocker: event
            .signals
            .iter()
            .any(|signal| signal.severity == GrowthSeverity::Blocker),
        growth_signal_kinds: event
            .signals
            .iter()
            .map(|signal| format!("{:?}", signal.kind))
            .collect(),
        memory_candidate_count: event.memory_candidates.len(),
        matrix_signal_count: event.matrix_signals.len(),
        assistant_text: String::new(),
    };
    let report = ScenarioSuite::new(vec![spec]).evaluate(&[observation]);

    assert!(report.verdicts[0].passed, "{:?}", report.verdicts[0]);
    assert!(event.confidence_bp >= 9000);
}

#[test]
fn underspecified_complex_trace_exposes_improvement_signal() {
    let record = LearningRecord::from_input(harness_contract::growth::GrowthInput {
        selected_pattern: ExecutionPattern::Direct,
        complexity: TaskComplexity::Complex,
        risk: TaskRisk::Medium,
        context_omitted: 0,
        tool_requires_checkpoint: false,
        tool_requires_human_confirm: false,
        verification_can_finalize: true,
        bench_passed: true,
    });

    assert!(record
        .signals
        .iter()
        .any(|signal| signal.kind == GrowthSignalKind::StrategyFit
            && signal.severity == GrowthSeverity::Improve));
    assert!(record
        .next_strategy_hints
        .iter()
        .any(|hint| hint.contains("execute")));
}

#[derive(Clone)]
struct EmptyApi;

impl ApiClient for EmptyApi {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
    }
}
