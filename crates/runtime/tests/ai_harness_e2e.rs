use std::pin::Pin;

use ai_core::{ExecutionMode, TaskComplexity, TaskRisk};
use ai_growth::{
    GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthSeverity, GrowthSignal,
    GrowthSignalKind, LearningRecord,
};
use ai_strategy::{
    decide_strategy, understand, StrategyExperienceRecord, StrategyExperienceStore, StrategyInput,
};
use futures::Stream;
use matrix_core::{MatrixEvidencePacket, MatrixQualityGateDecision};
use runtime::{
    ApiClient, ApiRequest, AssistantEvent, ContentBlock, ConversationRuntime, PermissionMode,
    PermissionPolicy, RuntimeAiKernel, RuntimeAiKernelTrace, RuntimeError, Session, SharedPrompter,
    StaticToolExecutor, TurnSummary,
};

#[derive(Debug)]
struct HarnessObservation {
    strategy_mode: ExecutionMode,
    finalization_blocked: bool,
    regression_allowed: bool,
    has_workgraph: bool,
    workgraph_quality_ok: bool,
    growth_has_blocker: bool,
    growth_signal_kinds: Vec<String>,
    memory_candidate_count: usize,
    matrix_signal_count: usize,
    assistant_text: String,
}

impl HarnessObservation {
    fn from_trace(trace: &RuntimeAiKernelTrace, assistant_text: impl Into<String>) -> Self {
        let workgraph_quality_ok = trace
            .workgraph_quality
            .as_ref()
            .map(|quality| quality.is_dag && quality.has_review_node && quality.has_synthesis_node)
            .unwrap_or(false);
        Self {
            strategy_mode: trace.strategy.mode,
            finalization_blocked: trace.finalization_blocked,
            regression_allowed: trace.regression_gate.allowed,
            has_workgraph: trace.workgraph.is_some(),
            workgraph_quality_ok,
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

    fn from_summary(summary: &TurnSummary) -> Self {
        Self::from_trace(
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

    fn has_growth_signal(&self, kind: &str) -> bool {
        self.growth_signal_kinds
            .iter()
            .any(|item| item == kind || item.eq_ignore_ascii_case(kind))
    }
}

#[test]
fn simple_question_stays_direct_and_clean() {
    let kernel = RuntimeAiKernel::begin_turn(
        "harness-simple",
        "explain this function",
        runtime::context_runtime::ContextProfile::MainTurn,
        &["system prompt".to_string()],
    );

    let trace = kernel.finalize("done", 0, 0);
    let observation = HarnessObservation::from_trace(&trace, "done");

    assert_eq!(observation.strategy_mode, ExecutionMode::DirectAnswer);
    assert!(!observation.finalization_blocked);
    assert!(observation.regression_allowed);
    assert!(!observation.has_workgraph);
    assert!(!observation.growth_has_blocker);
    assert_eq!(observation.memory_candidate_count, 0);
    assert!(observation.matrix_signal_count > 0);
    assert!(observation.has_growth_signal("StrategyFit"));
    assert!(observation.has_growth_signal("MatrixQualityGate"));
}

#[test]
fn complex_task_builds_plan_execute_workgraph() {
    let kernel = RuntimeAiKernel::begin_turn(
        "harness-complex",
        "全面规划 runtime gateway service crate 的复杂架构演进",
        runtime::context_runtime::ContextProfile::MainTurn,
        &[],
    );

    let trace = kernel.finalize("planned", 0, 0);
    let observation = HarnessObservation::from_trace(&trace, "planned");

    assert_eq!(observation.strategy_mode, ExecutionMode::PlanExecute);
    assert!(!observation.finalization_blocked);
    assert!(observation.regression_allowed);
    assert!(observation.has_workgraph);
    assert!(observation.workgraph_quality_ok);
    assert!(!observation.growth_has_blocker);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_answer_is_blocked_by_finalization_gate() {
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        EmptyApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system prompt".to_string()],
    )
    .without_memory();

    let summary = runtime
        .run_turn_async("answer this", &SharedPrompter::none())
        .await
        .expect("turn should complete with a gate message");
    let observation = HarnessObservation::from_summary(&summary);

    assert_eq!(observation.strategy_mode, ExecutionMode::DirectAnswer);
    assert!(observation.finalization_blocked);
    assert!(!observation.regression_allowed);
    assert!(observation.growth_has_blocker);
    assert!(observation.memory_candidate_count > 0);
    assert!(observation
        .assistant_text
        .contains("I cannot finalize this as a completed answer yet"));
}

#[test]
fn low_value_multi_agent_experience_downgrades_next_route() {
    let prompt = "使用多 Agent 协同完成复杂架构分析";
    let initial = decide_strategy(&StrategyInput::from_prompt(prompt));
    assert_eq!(initial.mode, ExecutionMode::SupervisorSubagents);

    let understanding = understand(&StrategyInput::from_prompt(prompt));
    let mut store = StrategyExperienceStore::new();
    for created_at_ms in 0..4 {
        store.record(StrategyExperienceRecord {
            domain: understanding.domain,
            complexity: understanding.complexity,
            risk: understanding.risk,
            selected_mode: ExecutionMode::SupervisorSubagents,
            succeeded: created_at_ms == 0,
            verification_blocked: false,
            context_pressure: false,
            multi_agent_positive_lift: false,
            created_at_ms,
        });
    }

    let enriched = store.enrich_input(StrategyInput::from_prompt(prompt));
    let adapted = decide_strategy(&enriched);

    assert_eq!(adapted.mode, ExecutionMode::PlanExecute);
    assert!(adapted
        .reasons
        .iter()
        .any(|reason| reason.contains("low multi-agent lift")));
}

#[test]
fn matrix_quality_failure_becomes_growth_signal() {
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
        strategy_mode: ExecutionMode::PlanExecute,
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
    assert!(!event.memory_candidates.is_empty());
    assert!(!event.matrix_signals.is_empty());
    assert!(event.confidence_bp >= 9000);
}

#[test]
fn underspecified_complex_trace_exposes_improvement_signal() {
    let record = LearningRecord::from_input(ai_growth::GrowthInput {
        selected_mode: ExecutionMode::DirectAnswer,
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
        .any(|hint| hint.contains("plan_execute")));
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
