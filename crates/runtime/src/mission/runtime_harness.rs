//! Runtime integration facade for the Cowd AI work kernel crates.

use crate::eval_gate::{
    score_case, score_report, BenchCaseKind, BenchCaseResult, CowdBenchCase, RegressionGate,
    RegressionGateVerdict, Trajectory,
};
use harness_contract::behavior::{decide_behavior_policy, BehaviorPolicyDecision};
use harness_contract::context::{
    ContextAlignmentReport, ContextAuthority, ContextBudget, ContextEpoch, ContextEpochBuilder,
    ContextIdentity, ContextItem, ContextMode, ContextRole, ContextSourceKind, PromptAssemblyPlan,
};
use harness_contract::core::{ExecutionModifier, ExecutionPattern};
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionEdgeKind, ExecutionGraph,
    ExecutionGraphQualityReport, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
};
use harness_contract::growth::{
    GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthInput, GrowthSeverity, GrowthSignal,
    GrowthSignalKind, LearningRecord,
};
use harness_contract::harness::{
    CowdNativeHarness, HarnessAdapter, HarnessTurnInput, HarnessTurnReceipt,
};
use harness_contract::policy::{
    agent_spec_policy_receipts, behavior_policy_receipt, governed_tool_policy_receipts,
    PolicyReceipt,
};
use harness_contract::strategy::{StrategyDecision, StrategyInput};
use harness_contract::tool::{GovernedToolPlanProjection, ToolApprovalClass, ToolEffectKind};
use harness_contract::verification::{
    Claim, ClaimKind, Evidence, EvidenceKind, VerificationLedger, VerificationReport,
};
use serde::{Deserialize, Serialize};

use crate::collaboration_template::{CollaborationDecision, CollaborationTemplateMatcher};
use crate::context_runtime::ContextProfile;
use crate::execution_core::{
    RuntimeCompileTarget, RuntimeExecutionDecision, StrategyDecisionEngine, StrategyResourceHealth,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeAiKernelTrace {
    pub execution_decision: RuntimeExecutionDecision,
    pub collaboration_decision: CollaborationDecision,
    pub context_epoch: ContextEpoch,
    pub context_envelope_id: Option<String>,
    pub context_alignment: Option<ContextAlignmentReport>,
    pub prompt_plan: PromptAssemblyPlan,
    pub governed_tool_plans: Vec<GovernedToolPlanProjection>,
    pub tool_receipt: Option<GovernedToolExecutionSummary>,
    pub verification_report: VerificationReport,
    pub verification_blocked: bool,
    pub trajectory: Trajectory,
    pub bench_result: BenchCaseResult,
    pub regression_gate: RegressionGateVerdict,
    pub learning_record: LearningRecord,
    pub growth_event: GrowthEvent,
    pub execution_graph: Option<ExecutionGraph>,
    pub execution_graph_quality: Option<ExecutionGraphQualityReport>,
    pub harness_receipt: HarnessTurnReceipt,
    pub policy_receipts: Vec<PolicyReceipt>,
    pub behavior_policy: BehaviorPolicyDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernedToolExecutionSummary {
    pub plan_ids: Vec<String>,
    pub completed_operations: usize,
    pub failed_operations: usize,
    pub checkpoint_created: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeAiKernel {
    user_input: String,
    execution_decision: RuntimeExecutionDecision,
    collaboration_decision: CollaborationDecision,
    context_epoch: ContextEpoch,
    governed_tool_plans: Vec<GovernedToolPlanProjection>,
    execution_graph: Option<ExecutionGraph>,
    verification: VerificationLedger,
    behavior_policy: BehaviorPolicyDecision,
    context_envelope_id: Option<String>,
    context_envelope_counts: Option<(usize, usize)>,
    checkpoint_created: bool,
}

impl RuntimeAiKernel {
    pub fn begin_turn(
        session_id: impl Into<String>,
        user_input: impl Into<String>,
        profile: ContextProfile,
        system_prompt: &[String],
    ) -> Self {
        let user_input = user_input.into();
        Self::begin_turn_with_strategy_input(
            session_id,
            user_input.clone(),
            profile,
            system_prompt,
            StrategyInput::from_prompt(user_input),
        )
    }

    pub fn begin_turn_with_strategy_input(
        session_id: impl Into<String>,
        user_input: impl Into<String>,
        profile: ContextProfile,
        system_prompt: &[String],
        strategy_input: StrategyInput,
    ) -> Self {
        let execution_decision = StrategyDecisionEngine.decide_with_input(
            strategy_input,
            Some(profile),
            StrategyResourceHealth::default(),
        );
        Self::begin_turn_with_execution_decision(
            session_id,
            user_input,
            profile,
            system_prompt,
            execution_decision,
        )
    }

    pub fn begin_turn_with_execution_decision(
        session_id: impl Into<String>,
        user_input: impl Into<String>,
        profile: ContextProfile,
        system_prompt: &[String],
        execution_decision: RuntimeExecutionDecision,
    ) -> Self {
        let session_id = session_id.into();
        let user_input = user_input.into();
        let strategy = &execution_decision.strategy;
        let collaboration_decision = CollaborationTemplateMatcher.decide(&user_input, strategy);
        let behavior_policy = decide_behavior_policy(&user_input, strategy);
        let context_epoch =
            build_context_epoch(&session_id, &user_input, profile, system_prompt, strategy);
        let execution_graph =
            build_initial_execution_graph(&user_input, strategy, execution_decision.compile_target);
        Self {
            user_input,
            execution_decision,
            collaboration_decision,
            context_epoch,
            governed_tool_plans: Vec::new(),
            execution_graph,
            verification: VerificationLedger::new(),
            behavior_policy,
            context_envelope_id: None,
            context_envelope_counts: None,
            checkpoint_created: false,
        }
    }

    pub fn strategy(&self) -> &StrategyDecision {
        &self.execution_decision.strategy
    }

    pub fn execution_decision(&self) -> &RuntimeExecutionDecision {
        &self.execution_decision
    }

    pub fn context_epoch(&self) -> &ContextEpoch {
        &self.context_epoch
    }

    pub fn record_governed_tool_plan(&mut self, plan: GovernedToolPlanProjection) {
        if plan.invocations.is_empty() {
            return;
        }
        let evidence_id = self.verification.add_evidence(Evidence::new(
            EvidenceKind::ToolResult,
            format!(
                "governed plan {} prepared {} tool invocations",
                plan.plan_id,
                plan.invocations.len()
            ),
        ));
        let claim_id = self.verification.add_claim(Claim::required(
            ClaimKind::DesignDecision,
            "tool execution used the Runtime governed plan",
        ));
        let _ = self.verification.support_claim(&claim_id, &evidence_id);
        self.governed_tool_plans.push(plan);
    }

    pub fn record_context_envelope(
        &mut self,
        envelope_id: impl Into<String>,
        selected_count: usize,
        omitted_count: usize,
    ) {
        self.context_envelope_id = Some(envelope_id.into());
        self.context_envelope_counts = Some((selected_count, omitted_count));
    }

    pub fn record_checkpoint_created(&mut self) {
        self.checkpoint_created = true;
    }

    /// A graph may reach a durable terminal Block without a usable model
    /// answer. Preserve the explanatory text for the surface, but never let
    /// that system-generated explanation satisfy the verification ledger or
    /// suppress the corresponding growth signal.
    pub fn record_terminal_blocked(&mut self, reason: impl Into<String>) {
        self.verification.add_claim(Claim::required(
            ClaimKind::Limitation,
            format!("runtime terminal blocked completion: {}", reason.into()),
        ));
        // Keep the required limitation pending. `NotRun` is an advisory
        // observation in the generic verification ledger, whereas a terminal
        // Block must make `can_finalize` false and feed the growth blocker.
    }

    pub fn finalize(
        mut self,
        assistant_text: &str,
        completed_tool_results: usize,
        failed_tool_results: usize,
    ) -> RuntimeAiKernelTrace {
        if !assistant_text.trim().is_empty() {
            let evidence_id = self.verification.add_evidence(Evidence::new(
                EvidenceKind::ToolResult,
                "assistant produced final response",
            ));
            let claim_id = self.verification.add_claim(Claim::required(
                ClaimKind::SourceFact,
                "assistant final response was produced",
            ));
            let _ = self.verification.support_claim(&claim_id, &evidence_id);
        } else {
            self.verification.add_claim(Claim::required(
                ClaimKind::Limitation,
                "assistant final response was empty",
            ));
        }

        let tool_receipt =
            (!self.governed_tool_plans.is_empty()).then(|| GovernedToolExecutionSummary {
                plan_ids: self
                    .governed_tool_plans
                    .iter()
                    .map(|plan| plan.plan_id.clone())
                    .collect(),
                completed_operations: completed_tool_results,
                failed_operations: failed_tool_results,
                checkpoint_created: self.checkpoint_created,
            });
        let verification_report = self.verification.report();
        let verification_blocked = !verification_report.can_finalize;
        let prompt_plan = self.context_epoch.prompt_assembly_plan();
        let context_alignment = self
            .context_envelope_id
            .as_ref()
            .zip(self.context_envelope_counts)
            .map(|(envelope_id, (selected_count, omitted_count))| {
                self.context_epoch.alignment_report(
                    envelope_id.clone(),
                    selected_count,
                    omitted_count,
                )
            });
        let mut bench_case = CowdBenchCase::new(
            bench_kind_for_mode(self.execution_decision.strategy.pattern),
            self.user_input.clone(),
            self.execution_decision.strategy.pattern,
        );
        bench_case.expected_modifiers = self.execution_decision.strategy.modifiers.clone();
        bench_case
            .required_checks
            .push("verification_report".to_string());
        bench_case
            .required_checks
            .extend(self.behavior_policy.eval_checks.clone());
        let trajectory = if verification_report.can_finalize {
            let mut trajectory = Trajectory::new(
                bench_case.id.clone(),
                self.execution_decision.strategy.pattern,
            )
            .pass("verification_report");
            trajectory.selected_modifiers = self.execution_decision.strategy.modifiers.clone();
            for check in &self.behavior_policy.eval_checks {
                trajectory = trajectory.pass(check.clone());
            }
            trajectory
        } else {
            let mut trajectory = Trajectory::new(
                bench_case.id.clone(),
                self.execution_decision.strategy.pattern,
            )
            .fail("verification_report");
            trajectory.selected_modifiers = self.execution_decision.strategy.modifiers.clone();
            trajectory
        };
        let bench_result = score_case(&bench_case, &trajectory);
        let bench_report = score_report(
            std::slice::from_ref(&bench_case),
            std::slice::from_ref(&trajectory),
        );
        let regression_gate = RegressionGate {
            min_average_score: 0.8,
            require_all_pass: true,
        }
        .evaluate(&bench_report);
        let tool_requires_checkpoint = self.governed_tool_plans.iter().any(|plan| {
            plan.invocations
                .iter()
                .any(|invocation| !matches!(invocation.effect.effect_kind, ToolEffectKind::Read))
        });
        let tool_requires_human_confirm = self.governed_tool_plans.iter().any(|plan| {
            plan.invocations.iter().any(|invocation| {
                matches!(
                    invocation.effect.approval_class,
                    ToolApprovalClass::User | ToolApprovalClass::Administrator
                ) || matches!(
                    invocation.effect.effect_kind,
                    ToolEffectKind::Destructive | ToolEffectKind::Unknown
                )
            })
        });
        let execution_graph_quality = self.execution_graph.as_ref().map(execution_graph_quality);
        let agent_spec = harness_contract::agent::AgentSpec::for_turn(
            &self.user_input,
            self.execution_decision.strategy.pattern,
            self.execution_decision.strategy.understanding.risk,
        );
        let mut policy_receipts = agent_spec_policy_receipts(&agent_spec);
        let governed_tool_plan_ids = self
            .governed_tool_plans
            .iter()
            .map(|plan| plan.plan_id.clone())
            .collect::<Vec<_>>();
        policy_receipts.extend(governed_tool_policy_receipts(
            &governed_tool_plan_ids,
            tool_requires_checkpoint,
            tool_requires_human_confirm,
        ));
        policy_receipts.push(behavior_policy_receipt(
            self.behavior_policy.enforcement.allow_execution,
            self.behavior_policy.enforcement.requires_scope_downgrade,
            self.behavior_policy.enforcement.requires_human_review,
            &self.behavior_policy.overengineering_risks,
        ));
        let mut learning_record = LearningRecord::from_input(GrowthInput {
            selected_pattern: self.execution_decision.strategy.pattern,
            complexity: self.execution_decision.strategy.understanding.complexity,
            risk: self.execution_decision.strategy.understanding.risk,
            context_omitted: self.context_epoch.omitted.len(),
            tool_requires_checkpoint,
            tool_requires_human_confirm,
            verification_can_finalize: verification_report.can_finalize,
            bench_passed: bench_result.passed,
        });
        learning_record
            .signals
            .push(GrowthSignal::from_matrix_quality_gate(
                regression_gate.allowed,
                (regression_gate.average_score.clamp(0.0, 1.0) * 10_000.0).round() as u16,
                &regression_gate.reasons,
            ));
        if let Some(alignment) = &context_alignment {
            if !alignment.aligned {
                learning_record.signals.push(GrowthSignal::new(
                    GrowthSignalKind::ContextPressure,
                    GrowthSeverity::Improve,
                    format!(
                        "context epoch and envelope diverged selected_delta={} omitted_delta={}",
                        alignment.selected_delta, alignment.omitted_delta
                    ),
                ));
                learning_record.next_strategy_hints.push(
                    "reconcile memory context packet projection with ai-context epoch".to_string(),
                );
            }
        }
        if let Some(quality) = &execution_graph_quality {
            if !quality.is_dag || !quality.has_verify_node || !quality.has_synthesize_node {
                learning_record.signals.push(GrowthSignal::new(
                    GrowthSignalKind::MultiAgentValue,
                    GrowthSeverity::Improve,
                    "execution graph quality missed dag/verify/synthesis requirements",
                ));
                learning_record
                    .next_strategy_hints
                    .push("repair execution graph before synthesizing complex tasks".to_string());
            }
        }
        if self.behavior_policy.has_overengineering_risk() {
            learning_record.signals.push(GrowthSignal::new(
                GrowthSignalKind::StrategyFit,
                GrowthSeverity::Improve,
                self.behavior_policy.overengineering_risks.join("; "),
            ));
            learning_record
                .next_strategy_hints
                .push("prefer minimal scope and reuse existing platform capabilities".to_string());
        }
        let mut evidence_refs = Vec::new();
        for plan in &self.governed_tool_plans {
            evidence_refs.push(GrowthEvidenceRef::new(
                "governed_tool_plan",
                plan.plan_id.clone(),
                "Runtime governed tool execution plan",
            ));
        }
        if let Some(graph) = &self.execution_graph {
            evidence_refs.push(GrowthEvidenceRef::new(
                "execution_graph",
                graph.id.clone(),
                "AI kernel execution graph",
            ));
        }
        let growth_event = GrowthEvent::from_input(GrowthEventInput {
            session_id: self.context_epoch.identity.session_id.clone(),
            source_event_kind: "runtime.harness_contract.trace".to_string(),
            strategy_pattern: self.execution_decision.strategy.pattern,
            learning_record: learning_record.clone(),
            evidence_refs,
        });
        let harness = CowdNativeHarness;
        let harness_receipt = harness
            .execute_turn(
                HarnessTurnInput {
                    agent_spec: agent_spec.clone(),
                    strategy: self.execution_decision.strategy.clone(),
                    context_epoch: self.context_epoch.clone(),
                    governed_tool_plans: self.governed_tool_plans.clone(),
                    policy_context: policy_receipts
                        .iter()
                        .map(|receipt| receipt.id.clone())
                        .collect(),
                },
                &verification_report,
                if assistant_text.trim().is_empty() {
                    "empty assistant output"
                } else {
                    "assistant output produced"
                },
            )
            .unwrap_or_else(|error| HarnessTurnReceipt {
                id: format!("harness-receipt-degraded-{}", uuid::Uuid::new_v4()),
                harness_id: "cowd-native".to_string(),
                agent_spec_id: agent_spec.id.clone(),
                strategy_pattern: self
                    .execution_decision
                    .strategy
                    .pattern
                    .as_str()
                    .to_string(),
                context_epoch_id: self.context_epoch.epoch_id.clone(),
                governed_tool_plan_ids: governed_tool_plan_ids.clone(),
                verification_can_finalize: verification_report.can_finalize,
                policy_receipts: Vec::new(),
                output_summary: format!("harness receipt degraded: {error}"),
            });
        RuntimeAiKernelTrace {
            execution_decision: self.execution_decision,
            collaboration_decision: self.collaboration_decision,
            context_epoch: self.context_epoch,
            context_envelope_id: self.context_envelope_id,
            context_alignment,
            prompt_plan,
            governed_tool_plans: self.governed_tool_plans,
            tool_receipt,
            verification_report,
            verification_blocked,
            trajectory,
            bench_result,
            regression_gate,
            learning_record,
            growth_event,
            execution_graph: self.execution_graph,
            execution_graph_quality,
            harness_receipt,
            policy_receipts,
            behavior_policy: self.behavior_policy,
        }
    }
}

fn build_context_epoch(
    session_id: &str,
    user_input: &str,
    profile: ContextProfile,
    system_prompt: &[String],
    strategy: &StrategyDecision,
) -> ContextEpoch {
    let identity = ContextIdentity {
        session_id: session_id.to_string(),
        task_id: None,
        agent_id: "primary".to_string(),
        mode: context_mode_for_profile(profile),
    };
    let mut builder = ContextEpochBuilder::new(identity, ContextBudget::new(16_000));
    for prompt in system_prompt {
        builder = builder.add_item(
            ContextItem::new(
                ContextSourceKind::StableHead,
                ContextAuthority::System,
                ContextRole::Instruction,
                prompt.clone(),
            )
            .with_score(1.0),
        );
    }
    builder = builder.add_item(
        ContextItem::new(
            ContextSourceKind::UserRequest,
            ContextAuthority::User,
            ContextRole::RecentTurn,
            user_input.to_string(),
        )
        .with_score(1.0),
    );
    builder = builder.add_item(
        ContextItem::new(
            ContextSourceKind::RuntimeHeader,
            ContextAuthority::Derived,
            ContextRole::TaskState,
            format!(
                "strategy_pattern={} complexity={:?} risk={:?}",
                strategy.pattern.as_str(),
                strategy.understanding.complexity,
                strategy.understanding.risk
            ),
        )
        .with_score(0.9),
    );
    builder
        .build()
        .unwrap_or_else(|_| harness_contract::context::ContextEpoch {
            epoch_id: format!("ctx-epoch-{}", uuid::Uuid::new_v4()),
            identity: ContextIdentity::main(session_id),
            budget: ContextBudget::new(1),
            selected: Vec::new(),
            omitted: Vec::new(),
            source_registry: Vec::new(),
            token_total: 0,
        })
}

fn build_initial_execution_graph(
    user_input: &str,
    strategy: &StrategyDecision,
    compile_target: RuntimeCompileTarget,
) -> Option<ExecutionGraph> {
    if compile_target == RuntimeCompileTarget::InlineModel
        && !strategy
            .modifiers
            .contains(&ExecutionModifier::WithVerifier)
    {
        return None;
    }
    let mut graph = ExecutionGraph::new(user_input.to_string());
    let (first_kind, first_label) = match compile_target {
        RuntimeCompileTarget::InlineModel => (ExecutionNodeKind::InlineModel, "respond"),
        RuntimeCompileTarget::EvidenceGraph => (ExecutionNodeKind::ToolBatch, "gather-evidence"),
        RuntimeCompileTarget::ExecutionGraph => (ExecutionNodeKind::ToolBatch, "execute"),
    };
    let mut first = ExecutionNodeSpec::new(first_kind, "runtime", first_label);
    first.id = first_label.to_string();
    first.idempotency_key = format!("{}:{first_label}", graph.id);
    let mut verify = ExecutionNodeSpec::new(
        ExecutionNodeKind::Verify,
        "runtime.verify",
        "verify-final-response",
    );
    verify.id = "verify".to_string();
    verify.idempotency_key = format!("{}:verify", graph.id);
    let mut synthesize = ExecutionNodeSpec::new(
        ExecutionNodeKind::Synthesize,
        "runtime.synthesize",
        "synthesize-final-response",
    );
    synthesize.id = "synthesize".to_string();
    synthesize.idempotency_key = format!("{}:synthesize", graph.id);
    graph.nodes = vec![first, verify, synthesize];
    for id in [first_label, "verify", "synthesize"] {
        graph
            .node_statuses
            .insert(id.to_string(), ExecutionNodeStatus::Planned);
    }
    graph.edges = vec![
        ExecutionEdge {
            from: first_label.to_string(),
            to: "verify".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        },
        ExecutionEdge {
            from: "verify".to_string(),
            to: "synthesize".to_string(),
            kind: ExecutionEdgeKind::DependsOn,
        },
    ];
    validate_execution_graph(&graph).ok()?;
    Some(graph)
}

fn execution_graph_quality(graph: &ExecutionGraph) -> ExecutionGraphQualityReport {
    let validation = validate_execution_graph(graph);
    ExecutionGraphQualityReport {
        node_count: graph.nodes.len(),
        edge_count: graph.edges.len(),
        ready_count: graph
            .node_statuses
            .values()
            .filter(|status| {
                matches!(
                    status,
                    ExecutionNodeStatus::Ready | ExecutionNodeStatus::Planned
                )
            })
            .count(),
        blocked_count: graph_nodes_in_status(graph, ExecutionNodeStatus::Blocked),
        failed_count: graph_nodes_in_status(graph, ExecutionNodeStatus::Failed),
        has_verify_node: graph
            .nodes
            .iter()
            .any(|node| node.kind == ExecutionNodeKind::Verify),
        has_synthesize_node: graph
            .nodes
            .iter()
            .any(|node| node.kind == ExecutionNodeKind::Synthesize),
        is_dag: validation.is_ok(),
        warnings: validation
            .err()
            .map(|error| vec![error.to_string()])
            .unwrap_or_default(),
    }
}

fn graph_nodes_in_status(graph: &ExecutionGraph, expected: ExecutionNodeStatus) -> usize {
    graph
        .node_statuses
        .values()
        .filter(|status| **status == expected)
        .count()
}

fn context_mode_for_profile(profile: ContextProfile) -> ContextMode {
    match profile {
        ContextProfile::SoloGoal | ContextProfile::YoloGoal => ContextMode::Goal,
        ContextProfile::SubAgent | ContextProfile::Collaboration => ContextMode::Agent,
        ContextProfile::Review | ContextProfile::DeepInvestigation => ContextMode::Review,
        ContextProfile::Resume => ContextMode::Resume,
        ContextProfile::MainTurn
        | ContextProfile::Cron
        | ContextProfile::SurfaceQuickReply
        | ContextProfile::SurfaceTaskIntake => ContextMode::MainTurn,
    }
}

fn bench_kind_for_mode(mode: ExecutionPattern) -> BenchCaseKind {
    match mode {
        ExecutionPattern::Direct => BenchCaseKind::SimpleAnswer,
        ExecutionPattern::Execute => BenchCaseKind::ArchitecturePlan,
        ExecutionPattern::Explore | ExecutionPattern::Collaborate => {
            BenchCaseKind::ExecutionGraphFanout
        }
        ExecutionPattern::Deliberate | ExecutionPattern::Supervise => {
            BenchCaseKind::VerificationGuard
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collaboration_template::CollaborationTemplateId;

    #[test]
    fn runtime_kernel_builds_trace_for_simple_turn() {
        let kernel = RuntimeAiKernel::begin_turn(
            "session-1",
            "explain this function",
            ContextProfile::MainTurn,
            &["system prompt".to_string()],
        );

        assert_eq!(kernel.strategy().pattern, ExecutionPattern::Direct);
        assert!(!kernel.context_epoch().selected.is_empty());
        let trace = kernel.finalize("done", 0, 0);
        assert!(trace.verification_report.can_finalize);
        assert!(!trace.verification_blocked);
        assert!(trace.bench_result.passed);
        assert!(trace.regression_gate.allowed);
        assert!(!trace.learning_record.has_blocker());
        assert_eq!(
            trace.growth_event.source_event_kind,
            "runtime.harness_contract.trace"
        );
        assert_eq!(
            trace.collaboration_decision.template_id,
            CollaborationTemplateId::DirectExecutor
        );
    }

    #[test]
    fn runtime_kernel_consumes_the_leased_decision_without_reclassifying() {
        let prompts = [
            ("解释这个函数", RuntimeCompileTarget::InlineModel, None),
            (
                "调研当前架构并并行汇总证据",
                RuntimeCompileTarget::EvidenceGraph,
                Some("gather-evidence"),
            ),
            (
                "实现这个明确的重构并验证",
                RuntimeCompileTarget::ExecutionGraph,
                Some("execute"),
            ),
            (
                "权衡两个架构方案并解决冲突",
                RuntimeCompileTarget::EvidenceGraph,
                Some("gather-evidence"),
            ),
            (
                "使用多 Agent 并行审查 runtime gateway memory",
                RuntimeCompileTarget::EvidenceGraph,
                Some("gather-evidence"),
            ),
            (
                "后台持续推进这项长期 mission 任务",
                RuntimeCompileTarget::EvidenceGraph,
                Some("gather-evidence"),
            ),
        ];

        for (prompt, target, expected_first_label) in prompts {
            let decision = StrategyDecisionEngine.decide(prompt, None);
            assert_eq!(decision.compile_target, target, "prompt: {prompt}");
            let lease_id = decision.lease.lease_id.clone();
            let kernel = RuntimeAiKernel::begin_turn_with_execution_decision(
                "session-strategy-lease",
                prompt,
                ContextProfile::MainTurn,
                &[],
                decision,
            );
            assert_eq!(kernel.execution_decision().lease.lease_id, lease_id);
            let trace = kernel.finalize("done", 0, 0);
            assert_eq!(trace.execution_decision.compile_target, target);
            match expected_first_label {
                Some(label) => assert_eq!(
                    trace
                        .execution_graph
                        .as_ref()
                        .and_then(|graph| graph.nodes.first())
                        .map(|node| node.id.as_str()),
                    Some(label),
                    "prompt: {prompt}"
                ),
                None => assert!(trace.execution_graph.is_none(), "prompt: {prompt}"),
            }
        }
    }

    #[test]
    fn runtime_kernel_records_governed_tool_plan() {
        let mut kernel = RuntimeAiKernel::begin_turn(
            "session-1",
            "modify one file",
            ContextProfile::MainTurn,
            &[],
        );
        kernel.record_governed_tool_plan(GovernedToolPlanProjection {
            contract_version: 1,
            plan_id: "plan-1".to_string(),
            revision: 1,
            catalog_revision: 7,
            invocations: vec![harness_contract::tool::GovernedToolInvocation {
                contract_version: 1,
                invocation_id: "tool-1".to_string(),
                intent: harness_contract::tool::ToolIntent {
                    invocation_id: "tool-1".to_string(),
                    tool_name: "apply_patch".to_string(),
                    normalized_input: serde_json::json!({"path": "src/lib.rs"}),
                },
                effect: harness_contract::tool::ToolEffectDescriptor {
                    tool_id: "apply_patch".to_string(),
                    descriptor_hash: "sha256:fixture".to_string(),
                    effect_kind: ToolEffectKind::Write,
                    idempotency: harness_contract::tool::ToolIdempotency::IdempotentWithKey,
                    scopes: Vec::new(),
                    required_permission: harness_contract::tool::ToolPermissionMode::WorkspaceWrite,
                    approval_class: ToolApprovalClass::Policy,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                },
                resource_demand: harness_contract::tool::ResourceDemand::default(),
                explicit_dependencies: Vec::new(),
                compiled_dependencies: Vec::new(),
                catalog_revision: 7,
                descriptor_set_hash: "sha256:catalog".to_string(),
                idempotency_key: "tool-1".to_string(),
            }],
            dependencies: Vec::new(),
        });

        let trace = kernel.finalize("changed", 1, 0);

        assert_eq!(trace.governed_tool_plans.len(), 1);
        assert!(trace.tool_receipt.is_some());
        assert!(!trace.learning_record.signals.is_empty());
        assert!(!trace.growth_event.evidence_refs.is_empty());
    }

    #[test]
    fn runtime_kernel_marks_empty_final_response_as_blocked() {
        let kernel =
            RuntimeAiKernel::begin_turn("session-1", "answer", ContextProfile::MainTurn, &[]);

        let trace = kernel.finalize("", 0, 0);

        assert!(trace.verification_blocked);
        assert!(!trace.regression_gate.allowed);
        assert!(trace.learning_record.has_blocker());
    }

    #[test]
    fn runtime_kernel_reports_execution_graph_quality_for_complex_turn() {
        let kernel = RuntimeAiKernel::begin_turn(
            "session-1",
            "全面规划 runtime gateway service crate 的复杂架构演进",
            ContextProfile::MainTurn,
            &[],
        );

        let trace = kernel.finalize("planned", 0, 0);
        let quality = trace
            .execution_graph_quality
            .expect("execution graph quality report");

        assert!(quality.is_dag);
        assert!(quality.has_verify_node);
        assert!(quality.has_synthesize_node);
        assert_eq!(
            trace.collaboration_decision.template_id,
            CollaborationTemplateId::LongRunningWorkstreams
        );
    }

    #[test]
    fn runtime_kernel_selects_debate_template_for_tradeoff_turn() {
        let kernel = RuntimeAiKernel::begin_turn(
            "session-1",
            "分析这个架构选择的利弊，是否应该拆 crate",
            ContextProfile::MainTurn,
            &[],
        );

        let trace = kernel.finalize("decided", 0, 0);

        assert_eq!(
            trace.collaboration_decision.template_id,
            CollaborationTemplateId::DebateCriticArbiter
        );
    }
}
