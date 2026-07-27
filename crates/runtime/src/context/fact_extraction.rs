use fact_kernel::{
    Confidence, ExtractionMethod, FactCandidate, FactEvidenceId, FactExtractionBatch,
    FactExtractionTokenUsage, FactExtractionTrigger, FactScope, FactSource, SourceKind,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeFactExtractionTrigger {
    TurnEnd,
    SessionCompaction,
    Handoff,
    DeepInvestigation,
    Import,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeFactExtractionMode {
    Disabled,
    RuleOnly,
    ModelAssisted,
}

impl RuntimeFactExtractionMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::RuleOnly => "rule_only",
            Self::ModelAssisted => "model_assisted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFactExtractionInput {
    pub trigger: RuntimeFactExtractionTrigger,
    pub session_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub team_id: Option<String>,
    pub agent_id: Option<String>,
    pub source_text: String,
    pub evidence_refs: Vec<String>,
    pub token_budget: Option<u64>,
}

impl RuntimeFactExtractionInput {
    #[must_use]
    pub fn new(trigger: RuntimeFactExtractionTrigger, source_text: impl Into<String>) -> Self {
        Self {
            trigger,
            session_id: None,
            project_id: None,
            task_id: None,
            team_id: None,
            agent_id: None,
            source_text: source_text.into(),
            evidence_refs: Vec::new(),
            token_budget: None,
        }
    }

    #[must_use]
    pub fn with_session_id(mut self, session_id: Option<String>) -> Self {
        self.session_id = session_id;
        self
    }

    #[must_use]
    pub fn with_project_id(mut self, project_id: Option<String>) -> Self {
        self.project_id = project_id;
        self
    }

    #[must_use]
    pub fn with_task_id(mut self, task_id: Option<String>) -> Self {
        self.task_id = task_id;
        self
    }

    #[must_use]
    pub fn with_team_id(mut self, team_id: Option<String>) -> Self {
        self.team_id = team_id;
        self
    }

    #[must_use]
    pub fn with_agent_id(mut self, agent_id: Option<String>) -> Self {
        self.agent_id = agent_id;
        self
    }

    #[must_use]
    pub fn with_evidence_refs(mut self, evidence_refs: Vec<String>) -> Self {
        self.evidence_refs = evidence_refs;
        self
    }

    #[must_use]
    pub fn with_token_budget(mut self, token_budget: Option<u64>) -> Self {
        self.token_budget = token_budget;
        self
    }

    #[must_use]
    pub fn fact_scope(&self) -> FactScope {
        if let Some(task_id) = &self.task_id {
            FactScope::Task(task_id.clone())
        } else if let Some(session_id) = &self.session_id {
            FactScope::Session(session_id.clone())
        } else if let Some(project_id) = &self.project_id {
            FactScope::Project(project_id.clone())
        } else {
            FactScope::Global
        }
    }

    #[must_use]
    pub fn fact_trigger(&self) -> FactExtractionTrigger {
        match self.trigger {
            RuntimeFactExtractionTrigger::TurnEnd => FactExtractionTrigger::TurnEnd,
            RuntimeFactExtractionTrigger::SessionCompaction => {
                FactExtractionTrigger::SessionCompaction
            }
            RuntimeFactExtractionTrigger::Handoff => FactExtractionTrigger::Handoff,
            RuntimeFactExtractionTrigger::DeepInvestigation => {
                FactExtractionTrigger::DeepInvestigation
            }
            RuntimeFactExtractionTrigger::Import => FactExtractionTrigger::Import,
            RuntimeFactExtractionTrigger::Manual => FactExtractionTrigger::Manual,
        }
    }

    #[must_use]
    pub fn source_evidence(&self) -> Vec<FactEvidenceId> {
        self.evidence_refs
            .iter()
            .map(|reference| FactEvidenceId::from_string(reference.clone()))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFactExtractionPolicy {
    pub enabled: bool,
    pub allow_model_assisted: bool,
    pub provider_available: bool,
    pub sync_on_compaction: bool,
}

impl Default for RuntimeFactExtractionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_model_assisted: true,
            provider_available: false,
            sync_on_compaction: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFactExtractionDecision {
    pub trigger: RuntimeFactExtractionTrigger,
    pub mode: RuntimeFactExtractionMode,
    pub degraded: bool,
    pub reason: String,
}

impl RuntimeFactExtractionDecision {
    #[must_use]
    pub fn evidence_label(&self) -> String {
        format!(
            "mode={} degraded={} reason={}",
            self.mode.as_str(),
            self.degraded,
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactExtractionRuntimeEvent {
    pub trigger: RuntimeFactExtractionTrigger,
    pub mode: RuntimeFactExtractionMode,
    pub degraded: bool,
    pub reason: String,
    pub extractor_version: String,
    pub candidate_count: usize,
    pub source_evidence_count: usize,
    pub token_usage: FactExtractionTokenUsage,
}

impl FactExtractionRuntimeEvent {
    #[must_use]
    pub fn from_decision(
        decision: &RuntimeFactExtractionDecision,
        extractor_version: impl Into<String>,
        candidate_count: usize,
        source_evidence_count: usize,
        token_usage: FactExtractionTokenUsage,
    ) -> Self {
        Self {
            trigger: decision.trigger,
            mode: decision.mode,
            degraded: decision.degraded,
            reason: decision.reason.clone(),
            extractor_version: extractor_version.into(),
            candidate_count,
            source_evidence_count,
            token_usage,
        }
    }

    #[must_use]
    pub fn evidence_label(&self) -> String {
        format!(
            "mode={} degraded={} candidates={} evidence={} extractor={} reason={}",
            self.mode.as_str(),
            self.degraded,
            self.candidate_count,
            self.source_evidence_count,
            self.extractor_version,
            self.reason
        )
    }
}

pub trait RuntimeFactExtractor {
    fn extractor_version(&self) -> &'static str;
    fn extract(&self, input: &RuntimeFactExtractionInput) -> FactExtractionBatch;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RuleFactExtractor;

impl RuntimeFactExtractor for RuleFactExtractor {
    fn extractor_version(&self) -> &'static str {
        "runtime-rule-fact-extractor:v1"
    }

    fn extract(&self, input: &RuntimeFactExtractionInput) -> FactExtractionBatch {
        let evidence = input.source_evidence();
        let candidates = input
            .source_text
            .lines()
            .filter_map(extract_fact_line)
            .map(|statement| {
                FactCandidate::observed(
                    "memory.reference",
                    statement,
                    input.fact_scope(),
                    FactSource {
                        kind: SourceKind::Runtime,
                        id: input
                            .session_id
                            .clone()
                            .unwrap_or_else(|| "runtime-fact-extraction".to_string()),
                        label: Some("runtime rule fact extraction".to_string()),
                    },
                )
                .with_evidence(evidence.clone())
                .with_confidence(Confidence::from_basis_points(7_500))
                .with_method(ExtractionMethod::Rule, self.extractor_version())
                .with_tags(vec!["runtime-fact-extraction".to_string()])
            })
            .collect::<Vec<_>>();
        let token_usage = FactExtractionTokenUsage {
            input_tokens: approximate_tokens(&input.source_text),
            output_tokens: 0,
            total_tokens: approximate_tokens(&input.source_text),
        };

        FactExtractionBatch::new(input.fact_trigger(), candidates)
            .with_session_id(input.session_id.clone())
            .with_project_id(input.project_id.clone())
            .with_task_id(input.task_id.clone())
            .with_team_id(input.team_id.clone())
            .with_source_evidence(evidence)
            .with_token_usage(token_usage)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFactExtractionRequest {
    pub prompt_markdown: String,
    pub token_budget: Option<u64>,
    pub evidence_refs: Vec<String>,
    pub rule_candidate_count: usize,
}

impl ModelFactExtractionRequest {
    #[must_use]
    pub fn from_input_and_rule_batch(
        input: &RuntimeFactExtractionInput,
        rule_batch: &FactExtractionBatch,
    ) -> Self {
        Self {
            prompt_markdown: format!(
                "## Task\nExtract durable fact candidates from the runtime evidence. Return compact JSON only.\n\n## Scope\nsession: {}\nproject: {}\ntask: {}\nteam: {}\n\n## Evidence\n{}\n",
                input.session_id.as_deref().unwrap_or(""),
                input.project_id.as_deref().unwrap_or(""),
                input.task_id.as_deref().unwrap_or(""),
                input.team_id.as_deref().unwrap_or(""),
                input.source_text
            ),
            token_budget: input.token_budget,
            evidence_refs: input.evidence_refs.clone(),
            rule_candidate_count: rule_batch.candidates.len(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeFactExtractionScheduler {
    pub policy: RuntimeFactExtractionPolicy,
}

impl RuntimeFactExtractionScheduler {
    #[must_use]
    pub fn new(policy: RuntimeFactExtractionPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn decide(&self, trigger: RuntimeFactExtractionTrigger) -> RuntimeFactExtractionDecision {
        if !self.policy.enabled {
            return RuntimeFactExtractionDecision {
                trigger,
                mode: RuntimeFactExtractionMode::Disabled,
                degraded: true,
                reason: "runtime fact extraction is disabled".to_string(),
            };
        }

        if !self.policy.allow_model_assisted {
            return RuntimeFactExtractionDecision {
                trigger,
                mode: RuntimeFactExtractionMode::RuleOnly,
                degraded: false,
                reason: "policy requires deterministic rule extraction".to_string(),
            };
        }

        match trigger {
            RuntimeFactExtractionTrigger::SessionCompaction
            | RuntimeFactExtractionTrigger::Handoff
            | RuntimeFactExtractionTrigger::DeepInvestigation
            | RuntimeFactExtractionTrigger::Import
            | RuntimeFactExtractionTrigger::Manual
                if self.policy.provider_available =>
            {
                RuntimeFactExtractionDecision {
                    trigger,
                    mode: RuntimeFactExtractionMode::ModelAssisted,
                    degraded: false,
                    reason: "provider available for model-assisted fact extraction".to_string(),
                }
            }
            RuntimeFactExtractionTrigger::SessionCompaction if self.policy.sync_on_compaction => {
                RuntimeFactExtractionDecision {
                    trigger,
                    mode: RuntimeFactExtractionMode::RuleOnly,
                    degraded: true,
                    reason: "provider unavailable; session compaction keeps deterministic checkpoint extraction".to_string(),
                }
            }
            _ => RuntimeFactExtractionDecision {
                trigger,
                mode: RuntimeFactExtractionMode::RuleOnly,
                degraded: !self.policy.provider_available && self.policy.allow_model_assisted,
                reason: "foreground runtime keeps extraction bounded and deterministic".to_string(),
            },
        }
    }
}

fn extract_fact_line(line: &str) -> Option<String> {
    line.trim()
        .strip_prefix("FACT:")
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(str::to_string)
}

fn approximate_tokens(text: &str) -> u64 {
    text.len().div_ceil(4) as u64
}

#[cfg(test)]
mod tests {
    use fact_kernel::FactExtractionTrigger;

    use super::{
        FactExtractionRuntimeEvent, ModelFactExtractionRequest, RuleFactExtractor,
        RuntimeFactExtractionInput, RuntimeFactExtractionMode, RuntimeFactExtractionPolicy,
        RuntimeFactExtractionScheduler, RuntimeFactExtractionTrigger, RuntimeFactExtractor,
    };

    #[test]
    fn fact_extraction_scheduler_uses_rule_path_for_compaction_without_provider() {
        let decision = RuntimeFactExtractionScheduler::default()
            .decide(RuntimeFactExtractionTrigger::SessionCompaction);

        assert_eq!(decision.mode, RuntimeFactExtractionMode::RuleOnly);
        assert!(decision.degraded);
        assert!(decision.reason.contains("provider unavailable"));
    }

    #[test]
    fn fact_extraction_scheduler_allows_model_for_deep_investigation() {
        let scheduler = RuntimeFactExtractionScheduler::new(RuntimeFactExtractionPolicy {
            provider_available: true,
            ..RuntimeFactExtractionPolicy::default()
        });

        let decision = scheduler.decide(RuntimeFactExtractionTrigger::DeepInvestigation);

        assert_eq!(decision.mode, RuntimeFactExtractionMode::ModelAssisted);
        assert!(!decision.degraded);
    }

    #[test]
    fn fact_extraction_scheduler_allows_model_for_compaction_when_provider_is_available() {
        let scheduler = RuntimeFactExtractionScheduler::new(RuntimeFactExtractionPolicy {
            provider_available: true,
            ..RuntimeFactExtractionPolicy::default()
        });

        let decision = scheduler.decide(RuntimeFactExtractionTrigger::SessionCompaction);

        assert_eq!(decision.mode, RuntimeFactExtractionMode::ModelAssisted);
        assert!(!decision.degraded);
    }

    #[test]
    fn fact_extraction_scheduler_can_be_disabled_explicitly() {
        let scheduler = RuntimeFactExtractionScheduler::new(RuntimeFactExtractionPolicy {
            enabled: false,
            ..RuntimeFactExtractionPolicy::default()
        });

        let decision = scheduler.decide(RuntimeFactExtractionTrigger::TurnEnd);

        assert_eq!(decision.mode, RuntimeFactExtractionMode::Disabled);
        assert!(decision.degraded);
    }

    #[test]
    fn rule_fact_extractor_builds_fact_kernel_batch_with_scope_and_evidence() {
        let input = RuntimeFactExtractionInput::new(
            RuntimeFactExtractionTrigger::TurnEnd,
            "FACT: The user wants Chinese implementation reports.\nignore this line",
        )
        .with_session_id(Some("session-a".to_string()))
        .with_project_id(Some("project-a".to_string()))
        .with_task_id(Some("task-a".to_string()))
        .with_evidence_refs(vec!["session-message:session-a:0".to_string()]);

        let batch = RuleFactExtractor.extract(&input);

        assert_eq!(batch.trigger, FactExtractionTrigger::TurnEnd);
        assert_eq!(batch.candidates.len(), 1);
        assert_eq!(batch.candidates[0].scope.key(), "task:task-a");
        assert_eq!(
            batch.candidates[0].evidence[0].as_str(),
            "session-message:session-a:0"
        );
        assert_eq!(
            batch.candidates[0].extractor_version,
            "runtime-rule-fact-extractor:v1"
        );
    }

    #[test]
    fn model_fact_extraction_request_uses_markdown_not_xml() {
        let input = RuntimeFactExtractionInput::new(
            RuntimeFactExtractionTrigger::DeepInvestigation,
            "FACT: Runtime can request model-assisted extraction.",
        )
        .with_token_budget(Some(4_096));
        let rule_batch = RuleFactExtractor.extract(&input);

        let request = ModelFactExtractionRequest::from_input_and_rule_batch(&input, &rule_batch);

        assert!(request.prompt_markdown.starts_with("## Task"));
        assert!(!request.prompt_markdown.contains("<"));
        assert_eq!(request.token_budget, Some(4_096));
        assert_eq!(request.rule_candidate_count, 1);
    }

    #[test]
    fn fact_extraction_runtime_event_is_projection_ready() {
        let decision = RuntimeFactExtractionScheduler::new(RuntimeFactExtractionPolicy {
            provider_available: true,
            ..RuntimeFactExtractionPolicy::default()
        })
        .decide(RuntimeFactExtractionTrigger::Import);
        let event = FactExtractionRuntimeEvent::from_decision(
            &decision,
            "runtime-rule-fact-extractor:v1",
            2,
            3,
            fact_kernel::FactExtractionTokenUsage {
                input_tokens: 10,
                output_tokens: 4,
                total_tokens: 14,
            },
        );

        let json = serde_json::to_string(&event).expect("event serializes");
        assert!(json.contains("model_assisted"));
        assert!(event.evidence_label().contains("candidates=2"));
    }
}
