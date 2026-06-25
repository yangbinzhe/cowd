//! Strategy routing for Cowd AI work kernel.
//!
//! This crate owns deterministic task understanding and execution-mode
//! selection. It does not execute tools, assemble prompts, or mutate task
//! state; later layers consume its `StrategyDecision`.

use crate::core::{ExecutionMode, KernelCapability, StrategyDecorator, TaskComplexity, TaskRisk};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDomain {
    Review,
    Bugfix,
    Frontend,
    Backend,
    Docs,
    Release,
    Test,
    Research,
    Architecture,
    Explore,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskUnderstanding {
    pub domain: TaskDomain,
    pub complexity: TaskComplexity,
    pub risk: TaskRisk,
    pub requires_write: bool,
    pub requires_external_facts: bool,
    pub requests_parallelism: bool,
    pub requests_multi_agent: bool,
    pub requests_deep_plan: bool,
    pub likely_single_file: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyInput {
    pub prompt: String,
    pub workspace_available: bool,
    pub changed_files: usize,
    pub explicit_write: bool,
    pub experience: Option<StrategyExperienceSummary>,
}

impl StrategyInput {
    #[must_use]
    pub fn from_prompt(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            workspace_available: true,
            changed_files: 0,
            explicit_write: false,
            experience: None,
        }
    }

    #[must_use]
    pub const fn without_workspace(mut self) -> Self {
        self.workspace_available = false;
        self
    }

    #[must_use]
    pub const fn with_changed_files(mut self, changed_files: usize) -> Self {
        self.changed_files = changed_files;
        self
    }

    #[must_use]
    pub const fn with_explicit_write(mut self, explicit_write: bool) -> Self {
        self.explicit_write = explicit_write;
        self
    }

    #[must_use]
    pub fn with_experience(mut self, experience: StrategyExperienceSummary) -> Self {
        self.experience = Some(experience);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceSummary {
    pub sample_count: u32,
    pub success_rate_bp: u16,
    pub verification_block_rate_bp: u16,
    pub context_pressure_rate_bp: u16,
    pub multi_agent_lift_rate_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceRecord {
    pub domain: TaskDomain,
    pub complexity: TaskComplexity,
    pub risk: TaskRisk,
    pub selected_mode: ExecutionMode,
    pub succeeded: bool,
    pub verification_blocked: bool,
    pub context_pressure: bool,
    pub multi_agent_positive_lift: bool,
    pub created_at_ms: u64,
}

impl StrategyExperienceRecord {
    #[must_use]
    pub fn from_decision(
        decision: &StrategyDecision,
        succeeded: bool,
        verification_blocked: bool,
        context_pressure: bool,
        multi_agent_positive_lift: bool,
        created_at_ms: u64,
    ) -> Self {
        Self {
            domain: decision.understanding.domain,
            complexity: decision.understanding.complexity,
            risk: decision.understanding.risk,
            selected_mode: decision.mode,
            succeeded,
            verification_blocked,
            context_pressure,
            multi_agent_positive_lift,
            created_at_ms,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyExperienceStore {
    pub records: Vec<StrategyExperienceRecord>,
}

impl StrategyExperienceStore {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }
        let bytes = std::fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        std::fs::write(path, bytes)
    }

    pub fn load_or_default(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self::load(path).unwrap_or_default()
    }

    pub fn record(&mut self, record: StrategyExperienceRecord) {
        self.records.push(record);
    }

    #[must_use]
    pub fn summary_for(
        &self,
        understanding: &TaskUnderstanding,
    ) -> Option<StrategyExperienceSummary> {
        let comparable = self
            .records
            .iter()
            .filter(|record| {
                record.domain == understanding.domain
                    && record.complexity == understanding.complexity
                    && record.risk == understanding.risk
            })
            .collect::<Vec<_>>();
        if comparable.is_empty() {
            return None;
        }
        let sample_count = comparable.len() as u32;
        Some(StrategyExperienceSummary {
            sample_count,
            success_rate_bp: rate_bp(
                comparable.iter().filter(|record| record.succeeded).count(),
                comparable.len(),
            ),
            verification_block_rate_bp: rate_bp(
                comparable
                    .iter()
                    .filter(|record| record.verification_blocked)
                    .count(),
                comparable.len(),
            ),
            context_pressure_rate_bp: rate_bp(
                comparable
                    .iter()
                    .filter(|record| record.context_pressure)
                    .count(),
                comparable.len(),
            ),
            multi_agent_lift_rate_bp: rate_bp(
                comparable
                    .iter()
                    .filter(|record| record.multi_agent_positive_lift)
                    .count(),
                comparable.len(),
            ),
        })
    }

    #[must_use]
    pub fn enrich_input(&self, mut input: StrategyInput) -> StrategyInput {
        let understanding = understand(&input);
        input.experience = self.summary_for(&understanding);
        input
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyDecision {
    pub understanding: TaskUnderstanding,
    pub mode: ExecutionMode,
    pub decorators: Vec<StrategyDecorator>,
    pub confidence: u8,
    pub reasons: Vec<String>,
    pub required_capabilities: Vec<KernelCapability>,
    pub policy_version: String,
}

impl StrategyDecision {
    #[must_use]
    pub fn uses_decorator(&self, decorator: StrategyDecorator) -> bool {
        self.decorators.contains(&decorator)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyPolicy {
    pub enable_parallel_read_fanout: bool,
    pub enable_multi_agent: bool,
    pub require_verifier_for_complex: bool,
    pub require_guardrails_for_writes: bool,
}

impl Default for StrategyPolicy {
    fn default() -> Self {
        Self {
            enable_parallel_read_fanout: true,
            enable_multi_agent: true,
            require_verifier_for_complex: true,
            require_guardrails_for_writes: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct StrategyRouter {
    policy: StrategyPolicy,
}

impl StrategyRouter {
    #[must_use]
    pub fn new(policy: StrategyPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn decide(&self, input: &StrategyInput) -> StrategyDecision {
        let understanding = understand(input);
        let mut decorators = Vec::new();
        let mut reasons = Vec::new();

        if understanding.requires_external_facts {
            decorators.push(StrategyDecorator::WithExternalResearch);
            reasons.push("task asks for current or external facts".to_string());
        }

        if input.workspace_available
            && matches!(
                understanding.domain,
                TaskDomain::Backend
                    | TaskDomain::Frontend
                    | TaskDomain::Bugfix
                    | TaskDomain::Review
                    | TaskDomain::Architecture
            )
        {
            decorators.push(StrategyDecorator::WithSymbolGraph);
        }

        if understanding.requires_write && self.policy.require_guardrails_for_writes {
            decorators.push(StrategyDecorator::WithGuardrails);
        }

        if matches!(understanding.risk, TaskRisk::High | TaskRisk::Critical) {
            decorators.push(StrategyDecorator::WithCheckpoint);
            decorators.push(StrategyDecorator::WithReviewer);
        }

        if matches!(
            understanding.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        ) && self.policy.require_verifier_for_complex
        {
            decorators.push(StrategyDecorator::WithVerifier);
            decorators.push(StrategyDecorator::WithTrace);
        }

        if understanding.requests_multi_agent && self.policy.enable_multi_agent {
            decorators.push(StrategyDecorator::WithReflection);
            reasons.push("task explicitly benefits from multiple agents".to_string());
        }

        let mut mode = select_mode(&understanding, &self.policy, &mut reasons);
        if let Some(experience) = &input.experience {
            mode = adapt_mode_from_experience(mode, &understanding, experience, &mut reasons);
        }
        dedupe_decorators(&mut decorators);
        let mut confidence = confidence_for(&understanding, mode);
        if let Some(experience) = &input.experience {
            confidence =
                adapt_confidence_from_experience(confidence, mode, experience, &mut reasons);
            if experience.verification_block_rate_bp >= 3000
                && !decorators.contains(&StrategyDecorator::WithVerifier)
            {
                decorators.push(StrategyDecorator::WithVerifier);
                reasons.push("experience shows verification gaps for comparable tasks".to_string());
            }
        }
        let required_capabilities = required_capabilities_for(&understanding, mode, &decorators);

        StrategyDecision {
            understanding,
            mode,
            decorators,
            confidence,
            reasons,
            required_capabilities,
            policy_version: "strategy-router-v2".to_string(),
        }
    }
}

fn adapt_mode_from_experience(
    mode: ExecutionMode,
    understanding: &TaskUnderstanding,
    experience: &StrategyExperienceSummary,
    reasons: &mut Vec<String>,
) -> ExecutionMode {
    if experience.sample_count < 3 {
        return mode;
    }
    if mode == ExecutionMode::SupervisorSubagents
        && experience.multi_agent_lift_rate_bp < 4000
        && !matches!(understanding.risk, TaskRisk::Critical)
    {
        reasons.push("experience shows low multi-agent lift for comparable tasks".to_string());
        return ExecutionMode::PlanExecute;
    }
    if matches!(mode, ExecutionMode::DirectAnswer | ExecutionMode::FastEdit)
        && experience.verification_block_rate_bp >= 5000
        && matches!(
            understanding.complexity,
            TaskComplexity::Moderate | TaskComplexity::Complex | TaskComplexity::Strategic
        )
    {
        reasons.push(
            "experience shows frequent verification blocks; upgrading to plan-execute".to_string(),
        );
        return ExecutionMode::PlanExecute;
    }
    mode
}

fn adapt_confidence_from_experience(
    confidence: u8,
    mode: ExecutionMode,
    experience: &StrategyExperienceSummary,
    reasons: &mut Vec<String>,
) -> u8 {
    if experience.sample_count < 3 {
        return confidence;
    }
    if experience.success_rate_bp >= 8500 {
        reasons.push("experience shows high success rate for comparable routing".to_string());
        return confidence.saturating_add(5).min(95);
    }
    if experience.success_rate_bp <= 4500 || experience.context_pressure_rate_bp >= 6000 {
        reasons.push("experience shows degraded outcomes for comparable routing".to_string());
        return confidence.saturating_sub(match mode {
            ExecutionMode::HumanConfirm | ExecutionMode::RiskGate => 0,
            _ => 8,
        });
    }
    confidence
}

#[must_use]
pub fn decide_strategy(input: &StrategyInput) -> StrategyDecision {
    StrategyRouter::default().decide(input)
}

#[must_use]
pub fn understand(input: &StrategyInput) -> TaskUnderstanding {
    let normalized = normalize(&input.prompt);
    let domain = classify_domain(&normalized);
    let requires_write = input.explicit_write || contains_any(&normalized, WRITE_TERMS);
    let requires_external_facts = contains_any(&normalized, EXTERNAL_FACT_TERMS);
    let requests_parallelism = contains_any(&normalized, PARALLEL_TERMS);
    let requests_multi_agent = contains_any(&normalized, MULTI_AGENT_TERMS);
    let requests_deep_plan = contains_any(&normalized, DEEP_PLAN_TERMS);
    let likely_single_file = contains_any(&normalized, SINGLE_FILE_TERMS)
        || (requires_write
            && !requests_deep_plan
            && !requests_parallelism
            && !requests_multi_agent
            && input.changed_files <= 1);
    let risk = classify_risk(input, &normalized, requires_write);
    let complexity = classify_complexity(
        input,
        domain,
        requires_write,
        requires_external_facts,
        requests_parallelism,
        requests_multi_agent,
        requests_deep_plan,
        likely_single_file,
    );

    TaskUnderstanding {
        domain,
        complexity,
        risk,
        requires_write,
        requires_external_facts,
        requests_parallelism,
        requests_multi_agent,
        requests_deep_plan,
        likely_single_file,
    }
}

fn select_mode(
    understanding: &TaskUnderstanding,
    policy: &StrategyPolicy,
    reasons: &mut Vec<String>,
) -> ExecutionMode {
    if understanding.requests_multi_agent && policy.enable_multi_agent {
        return ExecutionMode::SupervisorSubagents;
    }
    if understanding.requests_parallelism && policy.enable_parallel_read_fanout {
        return ExecutionMode::ParallelReadFanout;
    }
    if matches!(understanding.risk, TaskRisk::Critical) {
        return ExecutionMode::HumanConfirm;
    }
    if matches!(understanding.risk, TaskRisk::High)
        && !matches!(
            understanding.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        )
        && !understanding.requests_deep_plan
    {
        return ExecutionMode::RiskGate;
    }
    if matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    ) || understanding.requests_deep_plan
    {
        return ExecutionMode::PlanExecute;
    }
    if understanding.requires_write && understanding.likely_single_file {
        reasons.push("bounded write can use fast edit path".to_string());
        return ExecutionMode::FastEdit;
    }
    if understanding.requires_external_facts {
        return ExecutionMode::ExploreThenAnswer;
    }
    if matches!(
        understanding.complexity,
        TaskComplexity::Trivial | TaskComplexity::Simple
    ) {
        reasons.push("low-risk simple task should avoid over-planning".to_string());
        return ExecutionMode::DirectAnswer;
    }
    ExecutionMode::ReActLoop
}

fn required_capabilities_for(
    understanding: &TaskUnderstanding,
    mode: ExecutionMode,
    decorators: &[StrategyDecorator],
) -> Vec<KernelCapability> {
    let mut capabilities = vec![
        KernelCapability::StrategyRouting,
        KernelCapability::ContextEpoch,
    ];
    if understanding.requires_write || decorators.contains(&StrategyDecorator::WithGuardrails) {
        capabilities.push(KernelCapability::ToolTransaction);
    }
    if matches!(
        mode,
        ExecutionMode::PlanExecute
            | ExecutionMode::SupervisorSubagents
            | ExecutionMode::ParallelReadFanout
            | ExecutionMode::ParallelWorktree
    ) {
        capabilities.push(KernelCapability::WorkGraph);
    }
    if decorators.contains(&StrategyDecorator::WithVerifier)
        || matches!(
            understanding.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        )
    {
        capabilities.push(KernelCapability::VerificationLedger);
    }
    capabilities.push(KernelCapability::Evaluation);
    capabilities.push(KernelCapability::GrowthLoop);
    capabilities.sort_by_key(|capability| format!("{capability:?}"));
    capabilities.dedup();
    capabilities
}

fn classify_domain(normalized: &str) -> TaskDomain {
    if contains_any(normalized, REVIEW_TERMS) {
        TaskDomain::Review
    } else if contains_any(normalized, BUGFIX_TERMS) {
        TaskDomain::Bugfix
    } else if contains_any(normalized, FRONTEND_TERMS) {
        TaskDomain::Frontend
    } else if contains_any(normalized, RELEASE_TERMS) {
        TaskDomain::Release
    } else if contains_any(normalized, TEST_TERMS) {
        TaskDomain::Test
    } else if contains_any(normalized, RESEARCH_TERMS) {
        TaskDomain::Research
    } else if contains_any(normalized, ARCHITECTURE_TERMS) {
        TaskDomain::Architecture
    } else if contains_any(normalized, DOCS_TERMS) {
        TaskDomain::Docs
    } else if contains_any(normalized, BACKEND_TERMS) {
        TaskDomain::Backend
    } else {
        TaskDomain::Explore
    }
}

fn classify_risk(input: &StrategyInput, normalized: &str, requires_write: bool) -> TaskRisk {
    if contains_any(normalized, CRITICAL_RISK_TERMS) {
        TaskRisk::Critical
    } else if contains_any(normalized, HIGH_RISK_TERMS) || input.changed_files > 20 {
        TaskRisk::High
    } else if requires_write || input.changed_files > 0 {
        TaskRisk::Medium
    } else {
        TaskRisk::Low
    }
}

fn classify_complexity(
    input: &StrategyInput,
    domain: TaskDomain,
    requires_write: bool,
    requires_external_facts: bool,
    requests_parallelism: bool,
    requests_multi_agent: bool,
    requests_deep_plan: bool,
    likely_single_file: bool,
) -> TaskComplexity {
    if requests_multi_agent || requests_deep_plan || contains_many_scopes(&input.prompt) {
        return TaskComplexity::Strategic;
    }
    if requests_parallelism
        || input.changed_files > 8
        || matches!(domain, TaskDomain::Architecture | TaskDomain::Release)
    {
        return TaskComplexity::Complex;
    }
    if requires_external_facts
        || input.changed_files > 2
        || matches!(
            domain,
            TaskDomain::Review | TaskDomain::Bugfix | TaskDomain::Backend
        )
    {
        return TaskComplexity::Moderate;
    }
    if requires_write && !likely_single_file {
        return TaskComplexity::Moderate;
    }
    if input.prompt.chars().count() <= 80 {
        TaskComplexity::Simple
    } else {
        TaskComplexity::Moderate
    }
}

fn confidence_for(understanding: &TaskUnderstanding, mode: ExecutionMode) -> u8 {
    match (understanding.complexity, mode) {
        (TaskComplexity::Simple | TaskComplexity::Trivial, ExecutionMode::DirectAnswer) => 88,
        (_, ExecutionMode::FastEdit) if understanding.likely_single_file => 84,
        (
            TaskComplexity::Strategic,
            ExecutionMode::PlanExecute | ExecutionMode::SupervisorSubagents,
        ) => 82,
        (_, ExecutionMode::RiskGate | ExecutionMode::HumanConfirm) => 80,
        _ => 72,
    }
}

fn contains_many_scopes(prompt: &str) -> bool {
    let normalized = normalize(prompt);
    let count = [
        "gateway", "runtime", "tui", "service", "crate", "agent", "context",
    ]
    .iter()
    .filter(|term| normalized.contains(**term))
    .count();
    count >= 3
}

fn dedupe_decorators(decorators: &mut Vec<StrategyDecorator>) {
    let mut seen = std::collections::HashSet::new();
    decorators.retain(|decorator| seen.insert(*decorator));
}

fn normalize(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

fn rate_bp(count: usize, total: usize) -> u16 {
    if total == 0 {
        return 0;
    }
    ((count as u32 * 10_000) / total as u32).min(10_000) as u16
}

const REVIEW_TERMS: &[&str] = &["review", "审查", "审计", "检查", "code review"];
const BUGFIX_TERMS: &[&str] = &["bug", "fix", "修复", "报错", "失败", "failure", "panic"];
const FRONTEND_TERMS: &[&str] = &["frontend", "ui", "页面", "样式", "tui", "webui", "react"];
const BACKEND_TERMS: &[&str] = &["backend", "runtime", "server", "后端", "api", "service"];
const DOCS_TERMS: &[&str] = &["docs", "文档", "方案", "report", "报告"];
const RELEASE_TERMS: &[&str] = &["release", "发布", "tag", "验收", "回归"];
const TEST_TERMS: &[&str] = &["test", "测试", "e2e", "验证", "cargo test"];
const RESEARCH_TERMS: &[&str] = &["research", "调研", "latest", "最新", "论文", "外部"];
const ARCHITECTURE_TERMS: &[&str] = &[
    "architecture",
    "架构",
    "重构",
    "内核",
    "crate",
    "harness",
    "系统设计",
];
const WRITE_TERMS: &[&str] = &[
    "implement",
    "实现",
    "修改",
    "重构",
    "新增",
    "删除",
    "rename",
    "extract",
    "迁移",
];
const EXTERNAL_FACT_TERMS: &[&str] = &[
    "latest", "最新", "today", "现在", "当前", "调研", "research", "web", "论文",
];
const PARALLEL_TERMS: &[&str] = &["parallel", "并行", "同时", "fanout", "多路"];
const MULTI_AGENT_TERMS: &[&str] = &["multi-agent", "多agent", "多 agent", "subagent", "协同"];
const DEEP_PLAN_TERMS: &[&str] = &[
    "全面",
    "完整",
    "彻底",
    "阶段",
    "规划",
    "演进",
    "沉浸式",
    "终极",
];
const SINGLE_FILE_TERMS: &[&str] = &["单文件", "one file", "small fix", "小修"];
const HIGH_RISK_TERMS: &[&str] = &[
    "删除",
    "迁移",
    "重构",
    "全局",
    "workspace",
    "schema",
    "database",
    "权限",
];
const CRITICAL_RISK_TERMS: &[&str] = &[
    "生产数据库",
    "drop table",
    "reset --hard",
    "force push",
    "密钥",
    "secret",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_simple_question_to_direct_answer() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释一下这个函数有什么用"));

        assert_eq!(decision.mode, ExecutionMode::DirectAnswer);
        assert!(decision.confidence >= 80);
        assert!(!decision.uses_decorator(StrategyDecorator::WithVerifier));
    }

    #[test]
    fn routes_bounded_write_to_fast_edit() {
        let decision = decide_strategy(
            &StrategyInput::from_prompt("修复这个单文件小问题")
                .with_explicit_write(true)
                .with_changed_files(1),
        );

        assert_eq!(decision.mode, ExecutionMode::FastEdit);
        assert!(decision.uses_decorator(StrategyDecorator::WithGuardrails));
    }

    #[test]
    fn routes_architecture_work_to_plan_execute() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "全面重构 runtime gateway service crate 的架构，做完整阶段规划",
        ));

        assert_eq!(decision.mode, ExecutionMode::PlanExecute);
        assert_eq!(decision.understanding.complexity, TaskComplexity::Strategic);
        assert!(decision.uses_decorator(StrategyDecorator::WithVerifier));
        assert_eq!(decision.policy_version, "strategy-router-v2");
        assert!(decision
            .required_capabilities
            .contains(&KernelCapability::WorkGraph));
        assert!(decision
            .required_capabilities
            .contains(&KernelCapability::VerificationLedger));
    }

    #[test]
    fn routes_parallel_research_to_parallel_fanout_with_external_research() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "并行调研最新 AI harness 实践并汇总",
        ));

        assert_eq!(decision.mode, ExecutionMode::ParallelReadFanout);
        assert!(decision.uses_decorator(StrategyDecorator::WithExternalResearch));
    }

    #[test]
    fn routes_multi_agent_request_to_supervisor() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "使用多 Agent 协同完成复杂架构分析",
        ));

        assert_eq!(decision.mode, ExecutionMode::SupervisorSubagents);
        assert!(decision.uses_decorator(StrategyDecorator::WithReflection));
    }

    #[test]
    fn critical_risk_requires_human_confirm() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "force push 并 reset --hard 清理所有内容",
        ));

        assert_eq!(decision.mode, ExecutionMode::HumanConfirm);
    }

    #[test]
    fn strategy_experience_can_downgrade_low_lift_multi_agent() {
        let decision = decide_strategy(
            &StrategyInput::from_prompt("使用多 Agent 协同完成复杂架构分析").with_experience(
                StrategyExperienceSummary {
                    sample_count: 5,
                    success_rate_bp: 5000,
                    verification_block_rate_bp: 0,
                    context_pressure_rate_bp: 0,
                    multi_agent_lift_rate_bp: 2000,
                },
            ),
        );

        assert_eq!(decision.mode, ExecutionMode::PlanExecute);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("low multi-agent lift")));
    }

    #[test]
    fn strategy_experience_store_summarizes_comparable_records() {
        let input = StrategyInput::from_prompt("使用多 Agent 协同完成复杂架构分析");
        let understanding = understand(&input);
        let mut store = StrategyExperienceStore::new();
        for index in 0..4 {
            store.record(StrategyExperienceRecord {
                domain: understanding.domain,
                complexity: understanding.complexity,
                risk: understanding.risk,
                selected_mode: ExecutionMode::SupervisorSubagents,
                succeeded: index < 3,
                verification_blocked: index == 3,
                context_pressure: index >= 2,
                multi_agent_positive_lift: index == 0,
                created_at_ms: index,
            });
        }

        let summary = store.summary_for(&understanding).expect("summary");

        assert_eq!(summary.sample_count, 4);
        assert_eq!(summary.success_rate_bp, 7500);
        assert_eq!(summary.verification_block_rate_bp, 2500);
        assert_eq!(summary.context_pressure_rate_bp, 5000);
        assert_eq!(summary.multi_agent_lift_rate_bp, 2500);
    }

    #[test]
    fn strategy_experience_store_persists_json() {
        let decision = decide_strategy(&StrategyInput::from_prompt("修复这个单文件小问题"));
        let mut store = StrategyExperienceStore::new();
        store.record(StrategyExperienceRecord::from_decision(
            &decision, true, false, false, true, 1,
        ));
        let path = std::env::temp_dir().join(format!(
            "cowd-strategy-experience-{}.json",
            std::process::id()
        ));

        store.save(&path).unwrap();
        let loaded = StrategyExperienceStore::load(&path).unwrap();
        let _ = std::fs::remove_file(path);

        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.records[0].selected_mode, decision.mode);
    }
}
