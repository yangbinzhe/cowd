use harness_contract::core::{ExecutionModifier, ExecutionPattern, ExecutionPolicyGate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeCompileTarget {
    InlineModel,
    EvidenceGraph,
    ExecutionGraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionPatternSpec {
    pub pattern: ExecutionPattern,
    pub id: String,
    pub summary: String,
    pub suitable_for: Vec<String>,
    pub avoid_when: Vec<String>,
    pub default_templates: Vec<String>,
    pub required_runtime_capabilities: Vec<String>,
    pub supported_modifiers: Vec<ExecutionModifier>,
    pub supported_gates: Vec<ExecutionPolicyGate>,
    pub compile_target: RuntimeCompileTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPatternCatalog {
    pub patterns: Vec<RuntimeExecutionPatternSpec>,
}

impl ExecutionPatternCatalog {
    #[must_use]
    pub fn current() -> Self {
        use ExecutionPattern::{Collaborate, Deliberate, Direct, Execute, Explore, Supervise};

        Self {
            patterns: vec![
                spec(
                    Direct,
                    "Answer directly from available context without orchestration overhead.",
                    &[
                        "simple question",
                        "stable known fact",
                        "low-risk clarification",
                    ],
                    &[
                        "external evidence required",
                        "workspace mutation",
                        "unresolved conflict",
                    ],
                    &[],
                    &["inline_model"],
                    RuntimeCompileTarget::InlineModel,
                ),
                spec(
                    Explore,
                    "Acquire, compare, and synthesize checked evidence.",
                    &[
                        "repository exploration",
                        "current facts",
                        "multi-file audit",
                    ],
                    &["irreversible mutation", "evidence already sufficient"],
                    &["cowd/parallel-research-synthesis"],
                    &["tool_dag", "evidence_ledger"],
                    RuntimeCompileTarget::EvidenceGraph,
                ),
                spec(
                    Execute,
                    "Plan, execute, verify, and synthesize a bounded change.",
                    &[
                        "implementation",
                        "refactor",
                        "bugfix",
                        "configuration change",
                    ],
                    &["pure factual answer", "unapproved critical mutation"],
                    &[
                        "single_executor",
                        "execute_review",
                        "implementation_review_fix",
                    ],
                    &["tool_dag", "agent_runtime", "verification"],
                    RuntimeCompileTarget::ExecutionGraph,
                ),
                spec(
                    Deliberate,
                    "Compare competing proposals and resolve material evidence conflicts.",
                    &["architecture tradeoff", "what-if", "conflicting evidence"],
                    &["straightforward factual answer", "no material uncertainty"],
                    &["debate@1", "jps@1"],
                    &["agent_runtime", "evidence_ledger", "verification"],
                    RuntimeCompileTarget::EvidenceGraph,
                ),
                spec(
                    Collaborate,
                    "Decompose independent domains across a governed agent team.",
                    &[
                        "multi-domain investigation",
                        "implementation plus review",
                        "parallel work",
                    ],
                    &[
                        "simple task",
                        "negative collaboration lift",
                        "no agent backend",
                    ],
                    &["cowd/parallel-research-synthesis", "implementation_review_fix"],
                    &["team_runtime", "agent_runtime", "evidence_ledger"],
                    RuntimeCompileTarget::EvidenceGraph,
                ),
                spec(
                    Supervise,
                    "Govern long-running or cross-session work through mission checkpoints.",
                    &[
                        "long-running project",
                        "background review",
                        "cross-session mission",
                    ],
                    &[
                        "immediate simple answer",
                        "unbounded objective without acceptance criteria",
                    ],
                    &["long_running_project", "incident_response"],
                    &["mission_runtime", "checkpoint", "recovery"],
                    RuntimeCompileTarget::EvidenceGraph,
                ),
            ],
        }
    }

    #[must_use]
    pub fn find(&self, pattern: ExecutionPattern) -> Option<&RuntimeExecutionPatternSpec> {
        self.patterns.iter().find(|spec| spec.pattern == pattern)
    }

    #[must_use]
    pub fn summary(&self) -> Value {
        json!({
            "execution_patterns": self.patterns.iter().map(|spec| json!({
                "id": spec.id,
                "summary": spec.summary,
                "compile_target": spec.compile_target,
                "default_templates": spec.default_templates,
                "required_runtime_capabilities": spec.required_runtime_capabilities,
                "supported_modifiers": spec.supported_modifiers,
                "supported_gates": spec.supported_gates,
            })).collect::<Vec<_>>()
        })
    }
}

#[must_use]
pub fn execution_pattern_catalog_response() -> Value {
    ExecutionPatternCatalog::current().summary()
}

#[allow(clippy::too_many_arguments)]
fn spec(
    pattern: ExecutionPattern,
    summary: &str,
    suitable_for: &[&str],
    avoid_when: &[&str],
    default_templates: &[&str],
    required_runtime_capabilities: &[&str],
    compile_target: RuntimeCompileTarget,
) -> RuntimeExecutionPatternSpec {
    RuntimeExecutionPatternSpec {
        pattern,
        id: pattern.as_str().to_string(),
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
        supported_modifiers: pattern.supported_modifiers().to_vec(),
        supported_gates: pattern.supported_gates().to_vec(),
        compile_target,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_covers_each_terminal_pattern_once() {
        let catalog = ExecutionPatternCatalog::current();
        assert_eq!(catalog.patterns.len(), 6);
        for pattern in [
            ExecutionPattern::Direct,
            ExecutionPattern::Explore,
            ExecutionPattern::Execute,
            ExecutionPattern::Deliberate,
            ExecutionPattern::Collaborate,
            ExecutionPattern::Supervise,
        ] {
            assert_eq!(
                catalog
                    .patterns
                    .iter()
                    .filter(|spec| spec.pattern == pattern)
                    .count(),
                1,
                "{pattern:?}"
            );
        }
    }
}
