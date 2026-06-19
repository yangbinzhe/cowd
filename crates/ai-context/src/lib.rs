//! Context epoch and prompt assembly for Cowd AI work kernel.

use ai_core::{AiKernelError, AiKernelResult, KernelRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextMode {
    MainTurn,
    DirectAnswer,
    FastEdit,
    PlanExecute,
    SubAgent,
    Review,
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextIdentity {
    pub session_id: String,
    pub task_id: Option<String>,
    pub agent_id: String,
    pub mode: ContextMode,
}

impl ContextIdentity {
    #[must_use]
    pub fn main(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            task_id: None,
            agent_id: "primary".to_string(),
            mode: ContextMode::MainTurn,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceKind {
    StableHead,
    RuntimeHeader,
    UserRequest,
    Conversation,
    Memory,
    Task,
    ToolTrace,
    Workspace,
    AgentPeer,
    Handoff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextAuthority {
    System,
    User,
    Project,
    Session,
    Agent,
    Tool,
    Derived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextRole {
    Instruction,
    Identity,
    Orientation,
    Evidence,
    Warning,
    TaskState,
    RecentTurn,
    ToolSummary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub id: String,
    pub source: ContextSourceKind,
    pub authority: ContextAuthority,
    pub role: ContextRole,
    pub content: String,
    pub token_estimate: u64,
    pub score: f32,
    pub refs: Vec<KernelRef>,
}

impl ContextItem {
    #[must_use]
    pub fn new(
        source: ContextSourceKind,
        authority: ContextAuthority,
        role: ContextRole,
        content: impl Into<String>,
    ) -> Self {
        let content = content.into();
        Self {
            id: format!("ctx-item-{}", uuid::Uuid::new_v4()),
            source,
            authority,
            role,
            token_estimate: estimate_tokens(&content),
            content,
            score: 1.0,
            refs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_score(mut self, score: f32) -> Self {
        self.score = score.clamp(0.0, 1.0);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBudget {
    pub max_tokens: u64,
    pub stable_reserved: u64,
    pub runtime_reserved: u64,
}

impl ContextBudget {
    #[must_use]
    pub const fn new(max_tokens: u64) -> Self {
        Self {
            max_tokens,
            stable_reserved: 0,
            runtime_reserved: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextOmission {
    pub item_id: String,
    pub source: ContextSourceKind,
    pub reason: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEpoch {
    pub epoch_id: String,
    pub identity: ContextIdentity,
    pub budget: ContextBudget,
    pub selected: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    pub token_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextAlignmentReport {
    pub epoch_id: String,
    pub envelope_id: String,
    pub epoch_selected_count: usize,
    pub envelope_selected_count: usize,
    pub epoch_omitted_count: usize,
    pub envelope_omitted_count: usize,
    pub selected_delta: isize,
    pub omitted_delta: isize,
    pub aligned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptAssemblyPlan {
    pub epoch_id: String,
    pub sections: Vec<PromptSection>,
    pub token_total: u64,
    pub omissions: Vec<ContextOmission>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSection {
    pub source: ContextSourceKind,
    pub role: ContextRole,
    pub content: String,
    pub token_estimate: u64,
}

#[derive(Debug, Clone)]
pub struct ContextEpochBuilder {
    identity: ContextIdentity,
    budget: ContextBudget,
    items: Vec<ContextItem>,
}

impl ContextEpochBuilder {
    #[must_use]
    pub fn new(identity: ContextIdentity, budget: ContextBudget) -> Self {
        Self {
            identity,
            budget,
            items: Vec::new(),
        }
    }

    #[must_use]
    pub fn add_item(mut self, item: ContextItem) -> Self {
        self.items.push(item);
        self
    }

    pub fn build(mut self) -> AiKernelResult<ContextEpoch> {
        if self.budget.max_tokens == 0 {
            return Err(AiKernelError::InvalidInput(
                "context budget must be greater than zero".to_string(),
            ));
        }
        self.items.sort_by(compare_context_items);
        let mut selected = Vec::new();
        let mut omitted = Vec::new();
        let mut token_total = 0u64;
        for item in self.items {
            if token_total.saturating_add(item.token_estimate) <= self.budget.max_tokens {
                token_total = token_total.saturating_add(item.token_estimate);
                selected.push(item);
            } else {
                omitted.push(ContextOmission {
                    item_id: item.id,
                    source: item.source,
                    reason: "context budget exceeded".to_string(),
                    token_estimate: item.token_estimate,
                });
            }
        }
        Ok(ContextEpoch {
            epoch_id: format!("ctx-epoch-{}", uuid::Uuid::new_v4()),
            identity: self.identity,
            budget: self.budget,
            selected,
            omitted,
            token_total,
        })
    }
}

impl ContextEpoch {
    #[must_use]
    pub fn prompt_assembly_plan(&self) -> PromptAssemblyPlan {
        let sections = self
            .selected
            .iter()
            .map(|item| PromptSection {
                source: item.source,
                role: item.role,
                content: item.content.clone(),
                token_estimate: item.token_estimate,
            })
            .collect();
        PromptAssemblyPlan {
            epoch_id: self.epoch_id.clone(),
            sections,
            token_total: self.token_total,
            omissions: self.omitted.clone(),
        }
    }

    #[must_use]
    pub fn alignment_report(
        &self,
        envelope_id: impl Into<String>,
        envelope_selected_count: usize,
        envelope_omitted_count: usize,
    ) -> ContextAlignmentReport {
        let selected_delta = self.selected.len() as isize - envelope_selected_count as isize;
        let omitted_delta = self.omitted.len() as isize - envelope_omitted_count as isize;
        ContextAlignmentReport {
            epoch_id: self.epoch_id.clone(),
            envelope_id: envelope_id.into(),
            epoch_selected_count: self.selected.len(),
            envelope_selected_count,
            epoch_omitted_count: self.omitted.len(),
            envelope_omitted_count,
            selected_delta,
            omitted_delta,
            aligned: selected_delta == 0 && omitted_delta == 0,
        }
    }
}

fn compare_context_items(left: &ContextItem, right: &ContextItem) -> std::cmp::Ordering {
    source_priority(left.source)
        .cmp(&source_priority(right.source))
        .then_with(|| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.token_estimate.cmp(&right.token_estimate))
}

fn source_priority(source: ContextSourceKind) -> u8 {
    match source {
        ContextSourceKind::StableHead => 0,
        ContextSourceKind::RuntimeHeader => 1,
        ContextSourceKind::UserRequest => 2,
        ContextSourceKind::Task => 3,
        ContextSourceKind::Workspace => 4,
        ContextSourceKind::Memory => 5,
        ContextSourceKind::ToolTrace => 6,
        ContextSourceKind::Conversation => 7,
        ContextSourceKind::AgentPeer => 8,
        ContextSourceKind::Handoff => 9,
    }
}

fn estimate_tokens(content: &str) -> u64 {
    let chars = content.chars().count() as u64;
    chars.div_ceil(4).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(source: ContextSourceKind, content: &str, score: f32) -> ContextItem {
        ContextItem::new(
            source,
            ContextAuthority::Derived,
            ContextRole::Evidence,
            content,
        )
        .with_score(score)
    }

    #[test]
    fn epoch_keeps_stable_and_user_context_before_lower_priority_items() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(20))
            .add_item(item(
                ContextSourceKind::Memory,
                "remember this long memory",
                1.0,
            ))
            .add_item(item(ContextSourceKind::StableHead, "system", 0.1))
            .add_item(item(ContextSourceKind::UserRequest, "user asks", 0.5))
            .build()
            .unwrap();

        assert_eq!(epoch.selected[0].source, ContextSourceKind::StableHead);
        assert_eq!(epoch.selected[1].source, ContextSourceKind::UserRequest);
    }

    #[test]
    fn epoch_records_omissions_when_budget_is_exceeded() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::StableHead, "system", 1.0))
            .add_item(item(
                ContextSourceKind::Workspace,
                "this content is definitely too long for the tiny budget",
                1.0,
            ))
            .build()
            .unwrap();

        assert_eq!(epoch.selected.len(), 1);
        assert_eq!(epoch.omitted.len(), 1);
        assert_eq!(epoch.omitted[0].reason, "context budget exceeded");
    }

    #[test]
    fn prompt_assembly_plan_preserves_selected_items_and_omissions() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::StableHead, "system", 1.0))
            .add_item(item(
                ContextSourceKind::Memory,
                "too much memory content",
                1.0,
            ))
            .build()
            .unwrap();
        let plan = epoch.prompt_assembly_plan();

        assert_eq!(plan.epoch_id, epoch.epoch_id);
        assert_eq!(plan.sections.len(), epoch.selected.len());
        assert_eq!(plan.omissions.len(), epoch.omitted.len());
    }

    #[test]
    fn alignment_report_compares_epoch_with_envelope_counts() {
        let epoch = ContextEpochBuilder::new(ContextIdentity::main("s1"), ContextBudget::new(5))
            .add_item(item(ContextSourceKind::StableHead, "system", 1.0))
            .add_item(item(
                ContextSourceKind::Memory,
                "too much memory content",
                1.0,
            ))
            .build()
            .unwrap();

        let aligned =
            epoch.alignment_report("envelope-1", epoch.selected.len(), epoch.omitted.len());
        let drifted = epoch.alignment_report("envelope-2", 10, 0);

        assert!(aligned.aligned);
        assert!(!drifted.aligned);
        assert_eq!(drifted.envelope_id, "envelope-2");
    }
}
