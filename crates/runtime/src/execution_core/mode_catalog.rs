use harness_contract::core::{ExecutionMode, StrategyDecorator};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeExecutionBinding {
    Direct,
    EvidencePlan,
    ToolDag,
    TeamTemplate,
    Deliberation,
    Reflexion,
    RiskGate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionModeSpec {
    pub mode: ExecutionMode,
    pub id: String,
    pub summary: String,
    pub suitable_for: Vec<String>,
    pub avoid_when: Vec<String>,
    pub default_templates: Vec<String>,
    pub required_runtime_capabilities: Vec<String>,
    pub decorators: Vec<StrategyDecorator>,
    pub requires_approval: bool,
    pub surface_visible: bool,
    pub execution_binding: RuntimeExecutionBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionModeCatalog {
    pub modes: Vec<RuntimeExecutionModeSpec>,
}

impl ExecutionModeCatalog {
    #[must_use]
    pub fn current() -> Self {
        use ExecutionMode::{
            BackgroundReview, DeliberationSearch, DirectAnswer, ExploreThenAnswer, FastEdit,
            HumanConfirm, ParallelReadFanout, ParallelWorktree, PlanExecute, ReActLoop,
            ReflexionRetry, RiskGate, SupervisorSubagents,
        };
        Self {
            modes: vec![
                spec(
                    DirectAnswer,
                    "Answer directly with no orchestration.",
                    &[
                        "simple question",
                        "known stable fact",
                        "low-risk clarification",
                    ],
                    &[
                        "multi-file evidence",
                        "write action",
                        "ambiguous architecture decision",
                    ],
                    &[],
                    &[],
                    &[],
                    false,
                    RuntimeExecutionBinding::Direct,
                ),
                spec(
                    FastEdit,
                    "Small bounded edit with compact validation.",
                    &["single-file edit", "small config/doc patch"],
                    &["cross-crate refactor", "unclear owner boundary"],
                    &["single_executor"],
                    &["tool_dag"],
                    &[StrategyDecorator::WithVerifier],
                    false,
                    RuntimeExecutionBinding::ToolDag,
                ),
                spec(
                    ExploreThenAnswer,
                    "Gather evidence first, then answer from checked facts.",
                    &[
                        "repository exploration",
                        "external/current facts",
                        "bug trace",
                    ],
                    &["already enough evidence", "destructive operation"],
                    &["fanout_research_synthesis"],
                    &["rewoo_evidence", "batch_readonly_evidence"],
                    &[
                        StrategyDecorator::WithExternalResearch,
                        StrategyDecorator::WithTrace,
                    ],
                    false,
                    RuntimeExecutionBinding::EvidencePlan,
                ),
                spec(
                    PlanExecute,
                    "Plan, execute, and verify a non-trivial implementation.",
                    &["feature implementation", "refactor", "bugfix with tests"],
                    &["pure discussion", "unapproved high-risk mutation"],
                    &["implementation_review_fix", "plan_execute_review"],
                    &["tool_dag", "verification"],
                    &[
                        StrategyDecorator::WithCheckpoint,
                        StrategyDecorator::WithVerifier,
                    ],
                    false,
                    RuntimeExecutionBinding::TeamTemplate,
                ),
                spec(
                    ReActLoop,
                    "Fallback exploratory loop when the task cannot yet be planned.",
                    &["unknown tool affordance", "initial probing"],
                    &["known parallel evidence", "repeated low-novelty loop"],
                    &["single_executor"],
                    &[],
                    &[StrategyDecorator::WithTrace],
                    false,
                    RuntimeExecutionBinding::Direct,
                ),
                spec(
                    DeliberationSearch,
                    "Explore competing options, critique, and merge a decision.",
                    &["architecture tradeoff", "what-if", "ambiguous decision"],
                    &["straightforward factual answer"],
                    &["debate_consensus"],
                    &["deliberation"],
                    &[
                        StrategyDecorator::WithReviewer,
                        StrategyDecorator::WithMatrixEvidence,
                    ],
                    false,
                    RuntimeExecutionBinding::Deliberation,
                ),
                spec(
                    ReflexionRetry,
                    "Reflect on failed or low-efficiency progress and retry with a new mode.",
                    &[
                        "repeated failed tool path",
                        "verification failed",
                        "user correction",
                    ],
                    &["first attempt with no evidence"],
                    &["implementation_review_fix"],
                    &["growth", "turn_supervisor"],
                    &[StrategyDecorator::WithReflection],
                    false,
                    RuntimeExecutionBinding::Reflexion,
                ),
                spec(
                    SupervisorSubagents,
                    "Use runtime-owned subagents or a team template.",
                    &[
                        "multi-domain investigation",
                        "implementation plus review",
                        "parallel analysis",
                    ],
                    &["simple direct answer", "no available backend"],
                    &["fanout_research_synthesis", "implementation_review_fix"],
                    &["team_runtime", "agent_lifecycle"],
                    &[
                        StrategyDecorator::WithReviewer,
                        StrategyDecorator::WithTrace,
                    ],
                    false,
                    RuntimeExecutionBinding::TeamTemplate,
                ),
                spec(
                    ParallelReadFanout,
                    "Batch independent read-only evidence and synthesize.",
                    &[
                        "README review",
                        "multi-file code audit",
                        "search and read fanout",
                    ],
                    &["write/destructive operations"],
                    &["fanout_research_synthesis"],
                    &["rewoo_evidence", "tool_dag"],
                    &[StrategyDecorator::WithTrace],
                    false,
                    RuntimeExecutionBinding::ToolDag,
                ),
                spec(
                    ParallelWorktree,
                    "Isolated parallel implementation lanes.",
                    &[
                        "competing implementations",
                        "large refactor with lane isolation",
                    ],
                    &["dirty worktree without isolation", "low-value small edit"],
                    &["long_running_project"],
                    &["worktree_isolation"],
                    &[
                        StrategyDecorator::WithWorktreeIsolation,
                        StrategyDecorator::WithCheckpoint,
                    ],
                    true,
                    RuntimeExecutionBinding::TeamTemplate,
                ),
                spec(
                    BackgroundReview,
                    "Run an asynchronous review or verification lane.",
                    &["long-running implementation", "post-change review"],
                    &["user waiting for immediate simple answer"],
                    &["plan_execute_review"],
                    &["verification", "surface_stage_reply"],
                    &[StrategyDecorator::WithReviewer],
                    false,
                    RuntimeExecutionBinding::TeamTemplate,
                ),
                spec(
                    RiskGate,
                    "Block or gate high-risk work until policy approves.",
                    &["destructive action", "credential/secret", "production risk"],
                    &["read-only investigation"],
                    &[],
                    &["approval"],
                    &[StrategyDecorator::WithGuardrails],
                    true,
                    RuntimeExecutionBinding::RiskGate,
                ),
                spec(
                    HumanConfirm,
                    "Ask the human for explicit approval or decision.",
                    &["irreversible action", "business-critical choice"],
                    &["low-risk routine task"],
                    &[],
                    &["approval"],
                    &[StrategyDecorator::WithGuardrails],
                    true,
                    RuntimeExecutionBinding::RiskGate,
                ),
            ],
        }
    }

    #[must_use]
    pub fn find(&self, mode: ExecutionMode) -> Option<&RuntimeExecutionModeSpec> {
        self.modes.iter().find(|spec| spec.mode == mode)
    }

    #[must_use]
    pub fn summary(&self) -> Value {
        json!({
            "execution_modes": self.modes.iter().map(|spec| json!({
                "id": spec.id,
                "summary": spec.summary,
                "binding": spec.execution_binding,
                "default_templates": spec.default_templates,
                "requires_approval": spec.requires_approval,
                "required_runtime_capabilities": spec.required_runtime_capabilities,
            })).collect::<Vec<_>>()
        })
    }
}

#[must_use]
pub fn execution_mode_catalog_response() -> Value {
    ExecutionModeCatalog::current().summary()
}

fn spec(
    mode: ExecutionMode,
    summary: &str,
    suitable_for: &[&str],
    avoid_when: &[&str],
    default_templates: &[&str],
    required_runtime_capabilities: &[&str],
    decorators: &[StrategyDecorator],
    requires_approval: bool,
    execution_binding: RuntimeExecutionBinding,
) -> RuntimeExecutionModeSpec {
    RuntimeExecutionModeSpec {
        mode,
        id: mode.as_str().to_string(),
        summary: summary.to_string(),
        suitable_for: suitable_for
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        avoid_when: avoid_when.iter().map(|item| (*item).to_string()).collect(),
        default_templates: default_templates
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        required_runtime_capabilities: required_runtime_capabilities
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        decorators: decorators.to_vec(),
        requires_approval,
        surface_visible: true,
        execution_binding,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_mode_catalog_covers_all_contract_modes() {
        let catalog = ExecutionModeCatalog::current();
        assert_eq!(catalog.modes.len(), 13);
        for mode in [
            ExecutionMode::DirectAnswer,
            ExecutionMode::FastEdit,
            ExecutionMode::ExploreThenAnswer,
            ExecutionMode::PlanExecute,
            ExecutionMode::ReActLoop,
            ExecutionMode::DeliberationSearch,
            ExecutionMode::ReflexionRetry,
            ExecutionMode::SupervisorSubagents,
            ExecutionMode::ParallelReadFanout,
            ExecutionMode::ParallelWorktree,
            ExecutionMode::BackgroundReview,
            ExecutionMode::RiskGate,
            ExecutionMode::HumanConfirm,
        ] {
            assert!(catalog.find(mode).is_some(), "{mode:?}");
        }
    }
}
