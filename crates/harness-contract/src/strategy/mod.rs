//! Strategy routing for Cowd AI work kernel.
//!
//! This crate owns deterministic task understanding and execution-mode
//! selection. It does not execute tools, assemble prompts, or mutate task
//! state; later layers consume its `StrategyDecision`.

use crate::core::{
    ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, KernelCapability, TaskComplexity,
    TaskRisk,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskDuration {
    Immediate,
    Short,
    Extended,
    LongRunning,
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
    pub requests_deliberation: bool,
    pub requests_background: bool,
    pub likely_single_file: bool,
    pub independent_workstreams: u8,
    pub uncertainty: u8,
    pub estimated_duration: TaskDuration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyProposal {
    pub pattern: ExecutionPattern,
    #[serde(default)]
    pub modifiers: Vec<ExecutionModifier>,
    pub template: Option<String>,
    pub confidence: u8,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDecisionSource {
    Deterministic,
    ModelValidated,
    ExperienceAdapted,
    ResourceAdapted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationLiftEstimate {
    pub expected_lift_bp: i16,
    pub coordination_cost_bp: u16,
    pub accepted: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyInput {
    pub prompt: String,
    pub workspace_available: bool,
    pub changed_files: usize,
    pub explicit_write: bool,
    #[serde(default)]
    pub risk_override: Option<TaskRisk>,
    pub experience: Option<StrategyExperienceSummary>,
    pub proposal: Option<StrategyProposal>,
    #[serde(default)]
    pub understanding: Option<TaskUnderstanding>,
}

impl StrategyInput {
    #[must_use]
    pub fn from_prompt(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            workspace_available: true,
            changed_files: 0,
            explicit_write: false,
            risk_override: None,
            experience: None,
            proposal: None,
            understanding: None,
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
    pub const fn with_risk_override(mut self, risk: TaskRisk) -> Self {
        self.risk_override = Some(risk);
        self
    }

    #[must_use]
    pub fn with_experience(mut self, experience: StrategyExperienceSummary) -> Self {
        self.experience = Some(experience);
        self
    }

    #[must_use]
    pub fn with_proposal(mut self, proposal: StrategyProposal) -> Self {
        self.proposal = Some(proposal);
        self
    }

    #[must_use]
    pub fn with_understanding(mut self, understanding: TaskUnderstanding) -> Self {
        self.understanding = Some(understanding);
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
    pub selected_pattern: ExecutionPattern,
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
            selected_pattern: decision.pattern,
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
        self.summary_for_pattern(understanding, None)
    }

    #[must_use]
    pub fn summary_for_pattern(
        &self,
        understanding: &TaskUnderstanding,
        selected_pattern: Option<ExecutionPattern>,
    ) -> Option<StrategyExperienceSummary> {
        let comparable = self
            .records
            .iter()
            .filter(|record| {
                record.domain == understanding.domain
                    && record.complexity == understanding.complexity
                    && record.risk == understanding.risk
                    && selected_pattern.is_none_or(|pattern| record.selected_pattern == pattern)
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
        let understanding = input
            .understanding
            .clone()
            .unwrap_or_else(|| understand(&input));
        input.understanding = Some(understanding.clone());
        let baseline_pattern = StrategyRouter::default().decide(&input).pattern;
        input.experience = self.summary_for_pattern(&understanding, Some(baseline_pattern));
        input
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyDecision {
    pub understanding: TaskUnderstanding,
    pub pattern: ExecutionPattern,
    pub modifiers: Vec<ExecutionModifier>,
    pub gates: Vec<ExecutionPolicyGate>,
    pub collaboration_lift: CollaborationLiftEstimate,
    pub source: StrategyDecisionSource,
    pub confidence: u8,
    pub reasons: Vec<String>,
    pub required_capabilities: Vec<KernelCapability>,
    pub policy_version: String,
}

impl StrategyDecision {
    #[must_use]
    pub fn uses_modifier(&self, modifier: ExecutionModifier) -> bool {
        self.modifiers.contains(&modifier)
    }

    #[must_use]
    pub fn uses_gate(&self, gate: ExecutionPolicyGate) -> bool {
        self.gates.contains(&gate)
    }

    pub fn retarget(
        &mut self,
        pattern: ExecutionPattern,
        reason: impl Into<String>,
    ) -> Result<(), String> {
        if !pattern_supports_required_gates(pattern, &self.understanding) {
            return Err(format!(
                "pattern `{}` cannot preserve the required policy gates",
                pattern.as_str()
            ));
        }
        self.pattern = pattern;
        normalize_modifiers(pattern, &mut self.modifiers);
        self.gates = policy_gates_for(pattern, &self.understanding);
        self.required_capabilities =
            required_capabilities_for(&self.understanding, pattern, &self.modifiers);
        self.source = StrategyDecisionSource::ResourceAdapted;
        self.reasons.push(reason.into());
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyPolicy {
    pub enable_parallel_evidence: bool,
    pub enable_multi_agent: bool,
    pub require_verifier_for_complex: bool,
    pub require_guardrails_for_writes: bool,
}

impl Default for StrategyPolicy {
    fn default() -> Self {
        Self {
            enable_parallel_evidence: true,
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
        let understanding = input
            .understanding
            .clone()
            .unwrap_or_else(|| understand(input));
        let mut modifiers = Vec::new();
        let mut reasons = Vec::new();

        if understanding.requires_external_facts {
            modifiers.push(ExecutionModifier::WithExternalResearch);
            reasons.push("task asks for current or external facts".to_string());
        }

        if understanding.requires_write && self.policy.require_guardrails_for_writes {
            modifiers.push(ExecutionModifier::WithGuardrails);
        }

        if matches!(understanding.risk, TaskRisk::High | TaskRisk::Critical) {
            modifiers.push(ExecutionModifier::WithCheckpoint);
            modifiers.push(ExecutionModifier::WithReviewer);
        }

        if matches!(
            understanding.complexity,
            TaskComplexity::Complex | TaskComplexity::Strategic
        ) && self.policy.require_verifier_for_complex
        {
            modifiers.push(ExecutionModifier::WithVerifier);
            modifiers.push(ExecutionModifier::WithTrace);
        }

        if understanding.requests_multi_agent && self.policy.enable_multi_agent {
            modifiers.push(ExecutionModifier::WithReviewer);
            reasons.push("task explicitly benefits from multiple agents".to_string());
        }

        if understanding.requests_parallelism {
            modifiers.push(ExecutionModifier::Parallel);
        }
        if understanding.requires_write && understanding.likely_single_file {
            modifiers.push(ExecutionModifier::BoundedChange);
        }
        if understanding.requests_background {
            modifiers.push(ExecutionModifier::Background);
        }

        let mut pattern = select_pattern(&understanding, &self.policy, &mut reasons);
        let mut source = StrategyDecisionSource::Deterministic;
        if let Some(proposal) = &input.proposal {
            if proposal_is_executable(proposal, &understanding) {
                pattern = proposal.pattern;
                modifiers.extend(proposal.modifiers.iter().copied());
                reasons.push(format!("validated model proposal: {}", proposal.rationale));
                source = StrategyDecisionSource::ModelValidated;
            } else {
                reasons.push("model proposal was rejected by contract policy".to_string());
            }
        }
        if let Some(experience) = &input.experience {
            let adapted =
                adapt_pattern_from_experience(pattern, &understanding, experience, &mut reasons);
            if adapted != pattern {
                source = StrategyDecisionSource::ExperienceAdapted;
                pattern = adapted;
            }
        }
        let collaboration_lift =
            estimate_collaboration_lift(&understanding, input.experience.as_ref());
        if pattern == ExecutionPattern::Collaborate && !collaboration_lift.accepted {
            reasons.push("collaboration lift gate rejected team execution".to_string());
            pattern = ExecutionPattern::Execute;
        }
        if !pattern_supports_required_gates(pattern, &understanding) {
            reasons.push(format!(
                "{} cannot represent the required policy gates; using execute",
                pattern.as_str()
            ));
            pattern = ExecutionPattern::Execute;
        }
        let mut confidence = confidence_for(&understanding, pattern);
        if let Some(experience) = &input.experience {
            confidence =
                adapt_confidence_from_experience(confidence, pattern, experience, &mut reasons);
            if experience.verification_block_rate_bp >= 3000
                && !modifiers.contains(&ExecutionModifier::WithVerifier)
                && pattern.supports_modifier(ExecutionModifier::WithVerifier)
            {
                modifiers.push(ExecutionModifier::WithVerifier);
                reasons.push("experience shows verification gaps for comparable tasks".to_string());
            }
        }
        normalize_modifiers(pattern, &mut modifiers);
        let gates = policy_gates_for(pattern, &understanding);
        let required_capabilities = required_capabilities_for(&understanding, pattern, &modifiers);

        StrategyDecision {
            understanding,
            pattern,
            modifiers,
            gates,
            collaboration_lift,
            source,
            confidence,
            reasons,
            required_capabilities,
            policy_version: "strategy-decision-v3".to_string(),
        }
    }
}

fn adapt_pattern_from_experience(
    pattern: ExecutionPattern,
    understanding: &TaskUnderstanding,
    experience: &StrategyExperienceSummary,
    reasons: &mut Vec<String>,
) -> ExecutionPattern {
    if experience.sample_count < 3 {
        return pattern;
    }
    if pattern == ExecutionPattern::Collaborate
        && experience.multi_agent_lift_rate_bp < 4000
        && !matches!(understanding.risk, TaskRisk::Critical)
    {
        reasons.push("experience shows low multi-agent lift for comparable tasks".to_string());
        return ExecutionPattern::Execute;
    }
    if matches!(
        pattern,
        ExecutionPattern::Direct | ExecutionPattern::Explore
    ) && experience.verification_block_rate_bp >= 5000
        && matches!(
            understanding.complexity,
            TaskComplexity::Moderate | TaskComplexity::Complex | TaskComplexity::Strategic
        )
    {
        reasons.push(
            "experience shows frequent verification blocks; upgrading to execute".to_string(),
        );
        return ExecutionPattern::Execute;
    }
    pattern
}

fn adapt_confidence_from_experience(
    confidence: u8,
    pattern: ExecutionPattern,
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
        return confidence.saturating_sub(match pattern {
            ExecutionPattern::Supervise => 0,
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
    // A user can mention tools only to rule them out for an otherwise direct
    // request. Do not turn an explicit prohibition into an evidence-seeking
    // strategy merely because the word "tool" occurs in the prompt.
    let requires_external_facts =
        contains_any(&normalized, EXTERNAL_FACT_TERMS) && !explicitly_forbids_tool_use(&normalized);
    let requests_parallelism = contains_any(&normalized, PARALLEL_TERMS);
    // A request may mention teams solely to prohibit them. Treating every
    // occurrence of "team" as an affirmative collaboration request turns a
    // user-selected single-agent execution mode into its opposite.
    let requests_multi_agent = contains_any(&normalized, MULTI_AGENT_TERMS)
        && !explicitly_forbids_collaboration(&normalized);
    let requests_deep_plan = contains_any(&normalized, DEEP_PLAN_TERMS);
    let requests_deliberation = contains_any(&normalized, DELIBERATION_TERMS);
    let requests_background = contains_any(&normalized, BACKGROUND_TERMS);
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
        requests_deliberation,
        requests_background,
        likely_single_file,
        independent_workstreams: independent_workstreams(&normalized),
        uncertainty: uncertainty_score(&normalized, requires_external_facts),
        estimated_duration: estimate_duration(
            complexity,
            requests_background,
            requests_multi_agent,
        ),
    }
}

fn explicitly_forbids_collaboration(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要组队",
            "不要团队",
            "不要启动团队",
            "不要启动协作",
            "不启动团队",
            "无需团队",
            "不需要团队",
            "单 agent",
            "单agent",
            "单人执行",
            "don't use team",
            "do not use team",
            "do not start a team",
            "single agent",
            "single-agent",
        ],
    )
}

/// Returns whether a prompt contains an unambiguous instruction not to invoke
/// tools for this request. Runtime consumers use the same interpretation when
/// selecting the provider tool schema set, so routing and actual exposure
/// cannot diverge.
#[must_use]
pub fn prompt_explicitly_forbids_tool_use(prompt: &str) -> bool {
    explicitly_forbids_tool_use(&normalize(prompt))
}

fn explicitly_forbids_tool_use(normalized: &str) -> bool {
    contains_any(
        normalized,
        &[
            "不要调用工具",
            "不要使用工具",
            "不调用工具",
            "不使用工具",
            "无需调用工具",
            "不需要调用工具",
            "无需使用工具",
            "不需要使用工具",
            "禁止调用工具",
            "禁止使用工具",
            "do not call tools",
            "don't call tools",
            "do not use tools",
            "don't use tools",
            "no tool calls",
            "without tools",
        ],
    )
}

fn select_pattern(
    understanding: &TaskUnderstanding,
    policy: &StrategyPolicy,
    reasons: &mut Vec<String>,
) -> ExecutionPattern {
    if understanding.risk == TaskRisk::Critical {
        reasons.push("critical risk requires governed execution".to_string());
        return ExecutionPattern::Execute;
    }
    if understanding.requests_background {
        return ExecutionPattern::Supervise;
    }
    if understanding.requests_deliberation {
        return ExecutionPattern::Deliberate;
    }
    if understanding.requests_multi_agent && policy.enable_multi_agent {
        return ExecutionPattern::Collaborate;
    }
    if understanding.requires_write {
        reasons.push("write work requires the governed execution graph".to_string());
        return ExecutionPattern::Execute;
    }
    if understanding.requests_parallelism && policy.enable_parallel_evidence {
        return ExecutionPattern::Explore;
    }
    if matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    ) || understanding.requests_deep_plan
    {
        return ExecutionPattern::Execute;
    }
    if understanding.requires_external_facts {
        return ExecutionPattern::Explore;
    }
    if matches!(
        understanding.complexity,
        TaskComplexity::Trivial | TaskComplexity::Simple
    ) {
        reasons.push("low-risk simple task should avoid over-planning".to_string());
        return ExecutionPattern::Direct;
    }
    ExecutionPattern::Explore
}

fn required_capabilities_for(
    understanding: &TaskUnderstanding,
    pattern: ExecutionPattern,
    modifiers: &[ExecutionModifier],
) -> Vec<KernelCapability> {
    let mut capabilities = vec![
        KernelCapability::StrategyRouting,
        KernelCapability::ContextEpoch,
    ];
    if understanding.requires_write || modifiers.contains(&ExecutionModifier::WithGuardrails) {
        capabilities.push(KernelCapability::ToolTransaction);
    }
    if matches!(
        pattern,
        ExecutionPattern::Explore
            | ExecutionPattern::Execute
            | ExecutionPattern::Deliberate
            | ExecutionPattern::Collaborate
            | ExecutionPattern::Supervise
    ) {
        capabilities.push(KernelCapability::ExecutionGraph);
    }
    if modifiers.contains(&ExecutionModifier::WithVerifier)
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
    if let Some(risk) = input.risk_override {
        risk
    } else if contains_any(normalized, CRITICAL_RISK_TERMS) {
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

fn confidence_for(understanding: &TaskUnderstanding, pattern: ExecutionPattern) -> u8 {
    match (understanding.complexity, pattern) {
        (TaskComplexity::Simple | TaskComplexity::Trivial, ExecutionPattern::Direct) => 88,
        (_, ExecutionPattern::Execute) if understanding.likely_single_file => 84,
        (TaskComplexity::Strategic, ExecutionPattern::Execute | ExecutionPattern::Collaborate) => {
            82
        }
        (_, ExecutionPattern::Supervise | ExecutionPattern::Deliberate) => 80,
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

fn normalize_modifiers(pattern: ExecutionPattern, modifiers: &mut Vec<ExecutionModifier>) {
    let mut seen = std::collections::HashSet::new();
    modifiers.retain(|modifier| pattern.supports_modifier(*modifier) && seen.insert(*modifier));
}

fn policy_gates_for(
    pattern: ExecutionPattern,
    understanding: &TaskUnderstanding,
) -> Vec<ExecutionPolicyGate> {
    ExecutionPolicyGate::ALL
        .iter()
        .copied()
        .filter(|gate| {
            gate.is_required_for(understanding.risk, understanding.requires_write)
                && pattern.supports_gate(*gate)
        })
        .collect()
}

fn pattern_supports_required_gates(
    pattern: ExecutionPattern,
    understanding: &TaskUnderstanding,
) -> bool {
    ExecutionPolicyGate::ALL.iter().copied().all(|gate| {
        !gate.is_required_for(understanding.risk, understanding.requires_write)
            || pattern.supports_gate(gate)
    })
}

fn proposal_is_executable(proposal: &StrategyProposal, understanding: &TaskUnderstanding) -> bool {
    if proposal.confidence < 40 {
        return false;
    }
    if understanding.risk == TaskRisk::Critical && proposal.pattern == ExecutionPattern::Direct {
        return false;
    }
    if understanding.requires_write && proposal.pattern == ExecutionPattern::Direct {
        return false;
    }
    proposal
        .modifiers
        .iter()
        .all(|modifier| proposal.pattern.supports_modifier(*modifier))
        && pattern_supports_required_gates(proposal.pattern, understanding)
}

fn estimate_collaboration_lift(
    understanding: &TaskUnderstanding,
    experience: Option<&StrategyExperienceSummary>,
) -> CollaborationLiftEstimate {
    let independence = i16::from(understanding.independent_workstreams) * 1_500;
    let verification = i16::from(matches!(
        understanding.complexity,
        TaskComplexity::Complex | TaskComplexity::Strategic
    )) * 1_500;
    let uncertainty = i16::from(understanding.uncertainty) * 100;
    let historical = experience
        .filter(|summary| summary.sample_count >= 3)
        .map_or(0, |summary| summary.multi_agent_lift_rate_bp as i16 - 5_000);
    let coordination_cost_bp = match understanding.complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => 4_500,
        TaskComplexity::Moderate => 3_000,
        TaskComplexity::Complex => 2_000,
        TaskComplexity::Strategic => 1_500,
    };
    let expected_lift_bp =
        independence + verification + uncertainty + historical - coordination_cost_bp as i16;
    CollaborationLiftEstimate {
        expected_lift_bp,
        coordination_cost_bp,
        accepted: understanding.requests_multi_agent && expected_lift_bp > 0,
        reasons: vec![format!(
            "{} independent workstreams; uncertainty {}; coordination cost {}bp",
            understanding.independent_workstreams, understanding.uncertainty, coordination_cost_bp
        )],
    }
}

fn independent_workstreams(normalized: &str) -> u8 {
    let domains = [
        "runtime", "gateway", "frontend", "tui", "webui", "memory", "matrix", "test",
    ]
    .iter()
    .filter(|term| normalized.contains(**term))
    .count();
    domains.clamp(1, 8) as u8
}

fn uncertainty_score(normalized: &str, external_facts: bool) -> u8 {
    let mut score = u8::from(external_facts) * 3;
    if contains_any(normalized, DELIBERATION_TERMS) {
        score = score.saturating_add(4);
    }
    if contains_any(
        normalized,
        &["未知", "不确定", "unknown", "hypothesis", "假设"],
    ) {
        score = score.saturating_add(3);
    }
    score.min(10)
}

fn estimate_duration(
    complexity: TaskComplexity,
    background: bool,
    multi_agent: bool,
) -> TaskDuration {
    if background {
        return TaskDuration::LongRunning;
    }
    match complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => TaskDuration::Immediate,
        TaskComplexity::Moderate if !multi_agent => TaskDuration::Short,
        TaskComplexity::Moderate | TaskComplexity::Complex => TaskDuration::Extended,
        TaskComplexity::Strategic => TaskDuration::LongRunning,
    }
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
    "latest",
    "最新",
    "today",
    "现在",
    "当前",
    "调研",
    "research",
    "web",
    "论文",
    // An explicit evidence/tool instruction is not merely a stylistic model
    // preference. It changes the acceptance contract: the answer must be
    // grounded in an observable capability result, so Direct must not hide
    // the tool catalog behind its minimal bootstrap set.
    "工具",
    "tool",
    "证据",
    "evidence",
    "读取",
    "read_file",
];
const PARALLEL_TERMS: &[&str] = &["parallel", "并行", "同时", "fanout", "多路"];
const MULTI_AGENT_TERMS: &[&str] = &[
    "multi-agent",
    "多agent",
    "多 agent",
    "subagent",
    "协同",
    "团队",
    "team",
];
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
const DELIBERATION_TERMS: &[&str] = &[
    "debate",
    "deliberate",
    "tradeoff",
    "权衡",
    "争议",
    "冲突方案",
    "对抗性审查",
];
const BACKGROUND_TERMS: &[&str] = &[
    "background",
    "后台",
    "长期运行",
    "持续监控",
    "overnight",
    "异步审查",
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

    fn proposal(pattern: ExecutionPattern, modifiers: Vec<ExecutionModifier>) -> StrategyProposal {
        StrategyProposal {
            pattern,
            modifiers,
            template: None,
            confidence: 90,
            rationale: "test proposal".to_string(),
        }
    }

    fn assert_contract_legal(decision: &StrategyDecision) {
        assert!(decision
            .modifiers
            .iter()
            .all(|modifier| decision.pattern.supports_modifier(*modifier)));
        assert!(decision
            .gates
            .iter()
            .all(|gate| decision.pattern.supports_gate(*gate)));
    }

    #[test]
    fn routes_simple_question_to_direct() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释一下这个函数有什么用"));

        assert_eq!(decision.pattern, ExecutionPattern::Direct);
        assert!(decision.confidence >= 80);
        assert!(!decision.uses_modifier(ExecutionModifier::WithVerifier));
    }

    #[test]
    fn explicit_tool_evidence_and_team_requests_do_not_fall_back_to_direct() {
        let tool = decide_strategy(&StrategyInput::from_prompt(
            "必须通过只读工具读取 Cargo.toml 并提供证据",
        ));
        assert_eq!(tool.pattern, ExecutionPattern::Explore);
        assert!(tool.understanding.requires_external_facts);

        let team = decide_strategy(&StrategyInput::from_prompt(
            "请实际启动协作团队，分别审查 runtime、memory、gateway 后综合结论",
        ));
        assert_eq!(team.pattern, ExecutionPattern::Collaborate);
        assert!(team.understanding.requests_multi_agent);
    }

    #[test]
    fn explicit_tool_prohibition_does_not_create_external_evidence_work() {
        let prompt = "只回答 7 乘以 8 的结果。不要调用工具，不要组队。";
        let decision = decide_strategy(&StrategyInput::from_prompt(prompt));

        assert!(prompt_explicitly_forbids_tool_use(prompt));
        assert_eq!(decision.pattern, ExecutionPattern::Direct);
        assert!(!decision.understanding.requires_external_facts);
        assert!(!decision.understanding.requests_multi_agent);
    }

    #[test]
    fn routes_bounded_write_to_execute_with_bounded_modifier() {
        let decision = decide_strategy(
            &StrategyInput::from_prompt("修复这个单文件小问题")
                .with_explicit_write(true)
                .with_changed_files(1),
        );

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
        assert!(decision.uses_modifier(ExecutionModifier::WithGuardrails));
        assert!(decision.uses_modifier(ExecutionModifier::BoundedChange));
    }

    #[test]
    fn routes_architecture_work_to_execute() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "全面重构 runtime gateway service crate 的架构，做完整阶段规划",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
        assert_eq!(decision.understanding.complexity, TaskComplexity::Strategic);
        assert!(decision.uses_modifier(ExecutionModifier::WithVerifier));
        assert_eq!(decision.policy_version, "strategy-decision-v3");
        assert!(decision
            .required_capabilities
            .contains(&KernelCapability::ExecutionGraph));
        assert!(decision
            .required_capabilities
            .contains(&KernelCapability::VerificationLedger));
    }

    #[test]
    fn routes_parallel_research_to_explore_with_parallel_modifier() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "并行调研最新 AI harness 实践并汇总",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Explore);
        assert!(decision.uses_modifier(ExecutionModifier::WithExternalResearch));
        assert!(decision.uses_modifier(ExecutionModifier::Parallel));
    }

    #[test]
    fn routes_multi_agent_request_to_collaborate() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "使用多 Agent 协同完成复杂架构分析",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Collaborate);
        assert!(decision.uses_modifier(ExecutionModifier::WithReviewer));
        assert_contract_legal(&decision);
    }

    #[test]
    fn negative_team_constraint_is_not_routed_as_collaboration() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "请单人执行这次架构审查，不要启动团队或多 Agent。",
        ));

        assert!(!decision.understanding.requests_multi_agent);
        assert_ne!(decision.pattern, ExecutionPattern::Collaborate);
    }

    #[test]
    fn rejects_model_proposal_with_unsupported_modifier() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释这个函数").with_proposal(
            proposal(
                ExecutionPattern::Execute,
                vec![ExecutionModifier::Background],
            ),
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Direct);
        assert_eq!(decision.source, StrategyDecisionSource::Deterministic);
        assert!(!decision.uses_modifier(ExecutionModifier::Background));
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason.contains("rejected by contract policy")));
        assert_contract_legal(&decision);
    }

    #[test]
    fn accepts_model_proposal_with_supported_modifier() {
        let decision = decide_strategy(&StrategyInput::from_prompt("解释这个函数").with_proposal(
            proposal(ExecutionPattern::Explore, vec![ExecutionModifier::Parallel]),
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Explore);
        assert_eq!(decision.source, StrategyDecisionSource::ModelValidated);
        assert!(decision.uses_modifier(ExecutionModifier::Parallel));
        assert_contract_legal(&decision);
    }

    #[test]
    fn six_patterns_generate_their_key_policy_gates() {
        use ExecutionPolicyGate::{Approval, Budget, Permission, Risk};

        let direct = decide_strategy(&StrategyInput::from_prompt("解释这个值"));
        let explore = decide_strategy(
            &StrategyInput::from_prompt("收集资料并更新记录")
                .with_explicit_write(true)
                .with_proposal(proposal(ExecutionPattern::Explore, Vec::new())),
        );
        let execute = decide_strategy(
            &StrategyInput::from_prompt("force push secret change").with_explicit_write(true),
        );
        let deliberate = decide_strategy(
            &StrategyInput::from_prompt("对两个方案做 tradeoff").with_changed_files(21),
        );
        let collaborate = decide_strategy(
            &StrategyInput::from_prompt(
                "使用多 Agent 协同分析 runtime gateway memory 的 secret 变更",
            )
            .with_explicit_write(true)
            .with_proposal(proposal(ExecutionPattern::Collaborate, Vec::new())),
        );
        let supervise = decide_strategy(
            &StrategyInput::from_prompt("后台持续监控 secret 变更")
                .with_explicit_write(true)
                .with_proposal(proposal(ExecutionPattern::Supervise, Vec::new())),
        );

        assert_eq!(direct.pattern, ExecutionPattern::Direct);
        assert_eq!(direct.gates, vec![Budget]);
        assert_eq!(explore.pattern, ExecutionPattern::Explore);
        assert_eq!(explore.gates, vec![Budget, Permission]);
        assert_eq!(execute.pattern, ExecutionPattern::Execute);
        assert_eq!(execute.gates, vec![Budget, Permission, Risk, Approval]);
        assert_eq!(deliberate.pattern, ExecutionPattern::Deliberate);
        assert_eq!(deliberate.gates, vec![Budget, Risk]);
        assert_eq!(collaborate.pattern, ExecutionPattern::Collaborate);
        assert_eq!(collaborate.gates, vec![Budget, Permission, Risk, Approval]);
        assert_eq!(supervise.pattern, ExecutionPattern::Supervise);
        assert_eq!(supervise.gates, vec![Budget, Permission, Risk, Approval]);

        for decision in [direct, explore, execute, deliberate, collaborate, supervise] {
            assert_contract_legal(&decision);
        }
    }

    #[test]
    fn same_input_is_stable_across_all_six_patterns() {
        let cases = [
            ("解释一下这个函数有什么用", ExecutionPattern::Direct),
            (
                "调研最新 AI harness 实践并汇总证据",
                ExecutionPattern::Explore,
            ),
            ("实现并修复这个单文件小问题", ExecutionPattern::Execute),
            (
                "权衡两个架构方案并解决冲突方案",
                ExecutionPattern::Deliberate,
            ),
            (
                "使用多 Agent 协同完成复杂架构分析",
                ExecutionPattern::Collaborate,
            ),
            ("后台持续监控这项长期运行任务", ExecutionPattern::Supervise),
        ];

        for (prompt, expected_pattern) in cases {
            let input = StrategyInput::from_prompt(prompt);
            let first = decide_strategy(&input);
            let second = decide_strategy(&input);
            let wire = serde_json::to_value(&first).expect("strategy decision wire payload");

            assert_eq!(first.pattern, expected_pattern, "prompt: {prompt}");
            assert_eq!(first, second, "prompt: {prompt}");
            assert_eq!(wire["pattern"], expected_pattern.as_str());
            assert!(wire.get("mode").is_none());
        }
    }

    #[test]
    fn critical_risk_requires_approval_gate() {
        let decision = decide_strategy(&StrategyInput::from_prompt(
            "force push 并 reset --hard 清理所有内容",
        ));

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
        assert!(decision.uses_gate(ExecutionPolicyGate::Risk));
        assert!(decision.uses_gate(ExecutionPolicyGate::Approval));
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

        assert_eq!(decision.pattern, ExecutionPattern::Execute);
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
                selected_pattern: ExecutionPattern::Collaborate,
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
        assert_eq!(loaded.records[0].selected_pattern, decision.pattern);
    }
}
