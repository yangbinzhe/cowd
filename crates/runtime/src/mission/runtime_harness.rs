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
use harness_contract::core::{ExecutionMode, StrategyDecorator};
use harness_contract::growth::{
    GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthInput, GrowthSeverity, GrowthSignal,
    GrowthSignalKind, LearningRecord,
};
use harness_contract::harness::{
    CowdNativeHarness, HarnessAdapter, HarnessTurnInput, HarnessTurnReceipt,
};
use harness_contract::policy::{
    agent_spec_policy_receipts, behavior_policy_receipt, tool_transaction_policy_receipts,
    PolicyReceipt,
};
use harness_contract::strategy::{decide_strategy, StrategyDecision, StrategyInput};
use harness_contract::tool::{
    ToolOperation, ToolRisk, ToolTransactionPlan, ToolTransactionPlanner, ToolTransactionReceipt,
};
use harness_contract::verification::{
    Claim, ClaimKind, Evidence, EvidenceKind, VerificationLedger, VerificationReport,
};
use harness_contract::workgraph::{WorkGraph, WorkGraphQualityReport, WorkNode, WorkNodeKind};
use serde::{Deserialize, Serialize};

use crate::collaboration_template::{CollaborationDecision, CollaborationTemplateMatcher};
use crate::context_runtime::ContextProfile;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeAiKernelTrace {
    pub strategy: StrategyDecision,
    pub collaboration_decision: CollaborationDecision,
    pub context_epoch: ContextEpoch,
    pub context_envelope_id: Option<String>,
    pub context_alignment: Option<ContextAlignmentReport>,
    pub prompt_plan: PromptAssemblyPlan,
    pub tool_transaction: Option<ToolTransactionPlan>,
    pub tool_receipt: Option<ToolTransactionReceipt>,
    pub verification_report: VerificationReport,
    pub finalization_blocked: bool,
    pub trajectory: Trajectory,
    pub bench_result: BenchCaseResult,
    pub regression_gate: RegressionGateVerdict,
    pub learning_record: LearningRecord,
    pub growth_event: GrowthEvent,
    pub workgraph: Option<WorkGraph>,
    pub workgraph_quality: Option<WorkGraphQualityReport>,
    pub harness_receipt: HarnessTurnReceipt,
    pub policy_receipts: Vec<PolicyReceipt>,
    pub behavior_policy: BehaviorPolicyDecision,
}

#[derive(Debug, Clone)]
pub struct RuntimeAiKernel {
    user_input: String,
    strategy: StrategyDecision,
    collaboration_decision: CollaborationDecision,
    context_epoch: ContextEpoch,
    tool_transaction: Option<ToolTransactionPlan>,
    workgraph: Option<WorkGraph>,
    verification: VerificationLedger,
    behavior_policy: BehaviorPolicyDecision,
    context_envelope_id: Option<String>,
    context_envelope_counts: Option<(usize, usize)>,
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
        let session_id = session_id.into();
        let user_input = user_input.into();
        let strategy = decide_strategy(&strategy_input);
        let collaboration_decision =
            CollaborationTemplateMatcher::default().decide(&user_input, &strategy);
        let behavior_policy = decide_behavior_policy(&user_input, &strategy);
        let context_epoch =
            build_context_epoch(&session_id, &user_input, profile, system_prompt, &strategy);
        let workgraph = build_initial_workgraph(&user_input, &strategy);
        Self {
            user_input,
            strategy,
            collaboration_decision,
            context_epoch,
            tool_transaction: None,
            workgraph,
            verification: VerificationLedger::new(),
            behavior_policy,
            context_envelope_id: None,
            context_envelope_counts: None,
        }
    }

    pub fn strategy(&self) -> &StrategyDecision {
        &self.strategy
    }

    pub fn context_epoch(&self) -> &ContextEpoch {
        &self.context_epoch
    }

    pub fn record_tool_requests(&mut self, requests: &[(String, String, String)]) {
        if requests.is_empty() {
            return;
        }
        let operations = requests
            .iter()
            .map(|(_, name, input)| operation_from_tool(name, input))
            .collect::<Vec<_>>();
        match ToolTransactionPlanner.plan(operations) {
            Ok(plan) => {
                let evidence_id = self.verification.add_evidence(Evidence::new(
                    EvidenceKind::ToolResult,
                    format!("planned {} tool transaction batches", plan.batches.len()),
                ));
                let claim_id = self.verification.add_claim(Claim::required(
                    ClaimKind::DesignDecision,
                    "tool execution was planned through transaction policy",
                ));
                let _ = self.verification.support_claim(&claim_id, &evidence_id);
                self.tool_transaction = Some(plan);
            }
            Err(error) => {
                let claim_id = self.verification.add_claim(Claim::required(
                    ClaimKind::Limitation,
                    format!("tool transaction planning failed: {error}"),
                ));
                let _ = self.verification.mark_not_run(&claim_id);
            }
        }
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

        let tool_receipt = self
            .tool_transaction
            .as_ref()
            .map(|plan| plan.receipt(completed_tool_results, failed_tool_results));
        let verification_report = self.verification.report();
        let finalization_blocked = !verification_report.can_finalize;
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
            bench_kind_for_mode(self.strategy.mode),
            self.user_input.clone(),
            self.strategy.mode,
        );
        bench_case
            .required_checks
            .push("verification_report".to_string());
        bench_case
            .required_checks
            .extend(self.behavior_policy.eval_checks.clone());
        let trajectory = if verification_report.can_finalize {
            let mut trajectory = Trajectory::new(bench_case.id.clone(), self.strategy.mode)
                .pass("verification_report");
            for check in &self.behavior_policy.eval_checks {
                trajectory = trajectory.pass(check.clone());
            }
            trajectory
        } else {
            Trajectory::new(bench_case.id.clone(), self.strategy.mode).fail("verification_report")
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
        let tool_requires_checkpoint = self
            .tool_transaction
            .as_ref()
            .map(|plan| plan.requires_checkpoint)
            .unwrap_or(false);
        let tool_requires_human_confirm = self
            .tool_transaction
            .as_ref()
            .map(|plan| plan.requires_human_confirm)
            .unwrap_or(false);
        let workgraph_quality = self
            .workgraph
            .as_ref()
            .map(harness_contract::workgraph::WorkGraph::quality_report);
        let agent_spec = harness_contract::agent::AgentSpec::for_turn(
            &self.user_input,
            self.strategy.mode,
            self.strategy.understanding.risk,
        );
        let mut policy_receipts = agent_spec_policy_receipts(&agent_spec);
        policy_receipts.extend(tool_transaction_policy_receipts(
            self.tool_transaction.as_ref().map(|plan| plan.id.as_str()),
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
            selected_mode: self.strategy.mode,
            complexity: self.strategy.understanding.complexity,
            risk: self.strategy.understanding.risk,
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
        if let Some(quality) = &workgraph_quality {
            if !quality.is_dag || !quality.has_review_node || !quality.has_synthesis_node {
                learning_record.signals.push(GrowthSignal::new(
                    GrowthSignalKind::MultiAgentValue,
                    GrowthSeverity::Improve,
                    "workgraph quality report missed dag/review/synthesis requirements",
                ));
                learning_record
                    .next_strategy_hints
                    .push("repair workgraph before synthesizing complex tasks".to_string());
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
        if let Some(plan) = &self.tool_transaction {
            evidence_refs.push(GrowthEvidenceRef::new(
                "tool_transaction",
                plan.id.clone(),
                "AI kernel tool transaction plan",
            ));
        }
        if let Some(graph) = &self.workgraph {
            evidence_refs.push(GrowthEvidenceRef::new(
                "workgraph",
                graph.id.clone(),
                "AI kernel workgraph",
            ));
        }
        let growth_event = GrowthEvent::from_input(GrowthEventInput {
            session_id: self.context_epoch.identity.session_id.clone(),
            source_event_kind: "runtime.harness_contract.trace".to_string(),
            strategy_mode: self.strategy.mode,
            learning_record: learning_record.clone(),
            evidence_refs,
        });
        let harness = CowdNativeHarness;
        let harness_receipt = harness
            .execute_turn(
                HarnessTurnInput {
                    agent_spec: agent_spec.clone(),
                    strategy: self.strategy.clone(),
                    context_epoch: self.context_epoch.clone(),
                    tool_plan: self.tool_transaction.clone(),
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
                strategy_mode: self.strategy.mode.as_str().to_string(),
                context_epoch_id: self.context_epoch.epoch_id.clone(),
                tool_transaction_id: self.tool_transaction.as_ref().map(|plan| plan.id.clone()),
                verification_can_finalize: verification_report.can_finalize,
                policy_receipts: Vec::new(),
                output_summary: format!("harness receipt degraded: {error}"),
            });
        RuntimeAiKernelTrace {
            strategy: self.strategy,
            collaboration_decision: self.collaboration_decision,
            context_epoch: self.context_epoch,
            context_envelope_id: self.context_envelope_id,
            context_alignment,
            prompt_plan,
            tool_transaction: self.tool_transaction,
            tool_receipt,
            verification_report,
            finalization_blocked,
            trajectory,
            bench_result,
            regression_gate,
            learning_record,
            growth_event,
            workgraph: self.workgraph,
            workgraph_quality,
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
                "strategy_mode={} complexity={:?} risk={:?}",
                strategy.mode.as_str(),
                strategy.understanding.complexity,
                strategy.understanding.risk
            ),
        )
        .with_score(0.9),
    );
    builder.build().unwrap_or_else(|_| {
        ContextEpochBuilder::new(ContextIdentity::main(session_id), ContextBudget::new(1))
            .add_item(ContextItem::new(
                ContextSourceKind::UserRequest,
                ContextAuthority::User,
                ContextRole::RecentTurn,
                user_input.to_string(),
            ))
            .build()
            .expect("fallback context epoch should build")
    })
}

fn build_initial_workgraph(user_input: &str, strategy: &StrategyDecision) -> Option<WorkGraph> {
    if !matches!(
        strategy.mode,
        ExecutionMode::PlanExecute
            | ExecutionMode::SupervisorSubagents
            | ExecutionMode::ParallelReadFanout
            | ExecutionMode::ParallelWorktree
    ) && !strategy
        .decorators
        .contains(&StrategyDecorator::WithVerifier)
    {
        return None;
    }
    let mut graph = WorkGraph::new(user_input.to_string());
    let plan = graph
        .add_node(WorkNode::new(
            WorkNodeKind::AgentTask,
            "plan",
            "understand and plan the task",
        ))
        .ok()?;
    let verify = graph
        .add_node(WorkNode::new(
            WorkNodeKind::Review,
            "verify",
            "verify the final response against evidence",
        ))
        .ok()?;
    let synthesize = graph
        .add_node(WorkNode::new(
            WorkNodeKind::Synthesis,
            "synthesize",
            "synthesize verified evidence into the final response",
        ))
        .ok()?;
    let _ = graph.add_edge(
        &plan,
        &verify,
        harness_contract::workgraph::WorkEdgeKind::DependsOn,
    );
    let _ = graph.add_edge(
        &verify,
        &synthesize,
        harness_contract::workgraph::WorkEdgeKind::DependsOn,
    );
    Some(graph)
}

fn operation_from_tool(name: &str, input: &str) -> ToolOperation {
    let lowered = name.to_ascii_lowercase();
    let path = extract_path(input);
    if lowered.contains("read")
        || lowered.contains("grep")
        || lowered.contains("glob")
        || lowered.contains("list")
        || lowered.contains("search")
    {
        ToolOperation::read(name, path)
    } else {
        ToolOperation::write(name, tool_risk_for(name, input), path)
    }
}

fn tool_risk_for(name: &str, input: &str) -> ToolRisk {
    let text = format!(
        "{} {}",
        name.to_ascii_lowercase(),
        input.to_ascii_lowercase()
    );
    if text.contains("reset --hard") || text.contains("force push") || text.contains("drop table") {
        ToolRisk::Critical
    } else if text.contains("delete") || text.contains("rm ") || text.contains("write") {
        ToolRisk::High
    } else if text.contains("edit") || text.contains("patch") {
        ToolRisk::Medium
    } else {
        ToolRisk::Low
    }
}

fn extract_path(input: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(input).ok()?;
    value
        .get("path")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            value
                .get("file")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

fn context_mode_for_profile(profile: ContextProfile) -> ContextMode {
    match profile {
        ContextProfile::SoloGoal | ContextProfile::YoloGoal => ContextMode::PlanExecute,
        ContextProfile::SubAgent | ContextProfile::Collaboration => ContextMode::SubAgent,
        ContextProfile::Review => ContextMode::Review,
        ContextProfile::Resume => ContextMode::Resume,
        ContextProfile::MainTurn | ContextProfile::Cron => ContextMode::MainTurn,
    }
}

fn bench_kind_for_mode(mode: ExecutionMode) -> BenchCaseKind {
    match mode {
        ExecutionMode::DirectAnswer => BenchCaseKind::SimpleAnswer,
        ExecutionMode::FastEdit => BenchCaseKind::FastEdit,
        ExecutionMode::PlanExecute => BenchCaseKind::ArchitecturePlan,
        ExecutionMode::ParallelReadFanout | ExecutionMode::SupervisorSubagents => {
            BenchCaseKind::WorkGraphFanout
        }
        ExecutionMode::RiskGate | ExecutionMode::HumanConfirm => BenchCaseKind::ToolTransaction,
        _ => BenchCaseKind::VerificationGuard,
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

        assert_eq!(kernel.strategy().mode, ExecutionMode::DirectAnswer);
        assert!(!kernel.context_epoch().selected.is_empty());
        let trace = kernel.finalize("done", 0, 0);
        assert!(trace.verification_report.can_finalize);
        assert!(!trace.finalization_blocked);
        assert!(trace.bench_result.passed);
        assert!(trace.regression_gate.allowed);
        assert!(!trace.learning_record.has_blocker());
        assert_eq!(
            trace.growth_event.source_event_kind,
            "runtime.harness_contract.trace"
        );
        assert_eq!(
            trace.collaboration_decision.template_id,
            CollaborationTemplateId::SingleExecutor
        );
    }

    #[test]
    fn runtime_kernel_records_tool_transaction_plan() {
        let mut kernel = RuntimeAiKernel::begin_turn(
            "session-1",
            "modify one file",
            ContextProfile::MainTurn,
            &[],
        );
        kernel.record_tool_requests(&[(
            "tool-1".to_string(),
            "apply_patch".to_string(),
            r#"{"path":"src/lib.rs"}"#.to_string(),
        )]);

        let trace = kernel.finalize("changed", 1, 0);

        assert!(trace.tool_transaction.is_some());
        assert!(trace.tool_receipt.is_some());
        assert!(!trace.learning_record.signals.is_empty());
        assert!(!trace.growth_event.evidence_refs.is_empty());
    }

    #[test]
    fn runtime_kernel_marks_empty_final_response_as_blocked() {
        let kernel =
            RuntimeAiKernel::begin_turn("session-1", "answer", ContextProfile::MainTurn, &[]);

        let trace = kernel.finalize("", 0, 0);

        assert!(trace.finalization_blocked);
        assert!(!trace.regression_gate.allowed);
        assert!(trace.learning_record.has_blocker());
    }

    #[test]
    fn runtime_kernel_reports_workgraph_quality_for_complex_turn() {
        let kernel = RuntimeAiKernel::begin_turn(
            "session-1",
            "全面规划 runtime gateway service crate 的复杂架构演进",
            ContextProfile::MainTurn,
            &[],
        );

        let trace = kernel.finalize("planned", 0, 0);
        let quality = trace.workgraph_quality.expect("workgraph quality report");

        assert!(quality.is_dag);
        assert!(quality.has_review_node);
        assert!(quality.has_synthesis_node);
        assert_eq!(
            trace.collaboration_decision.template_id,
            CollaborationTemplateId::LongRunningProject
        );
        assert!(trace.collaboration_decision.plan.agents.len() >= 3);
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
            CollaborationTemplateId::DebateConsensus
        );
        assert!(trace
            .collaboration_decision
            .plan
            .agents
            .iter()
            .any(|agent| agent.role_id == "skeptic"));
    }
}
