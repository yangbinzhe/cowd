use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::signal::{EvolutionSignal, EvolutionSignalSeverity, EvolutionSignalType};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionProposalKind {
    PlanDraft,
    SkillDraft,
    TestScenario,
    ToolCapabilityRequest,
    ConnectorCapabilityRequest,
    MemoryGovernanceAdjustment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionProposalRisk {
    pub level: String,
    pub boundaries: Vec<String>,
    pub approval_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionProposal {
    pub proposal_id: String,
    pub kind: EvolutionProposalKind,
    pub problem_statement: String,
    pub current_evidence: Vec<String>,
    pub target_improvement: String,
    pub expected_benefit: String,
    pub risk: EvolutionProposalRisk,
    pub acceptance_gates: Vec<String>,
    pub rollback_strategy: String,
    pub source_signal_ids: Vec<String>,
    pub created_at_ms: u128,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionPlanDraft {
    pub proposal: EvolutionProposal,
    pub implementation_steps: Vec<String>,
    pub blocked_mainline_write: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionSkillDraft {
    pub skill_id: String,
    pub version: String,
    pub source_proposal_id: String,
    pub permissions_note: String,
    pub evidence_refs: Vec<String>,
    pub rollback_note: String,
    pub markdown: String,
}

impl EvolutionProposal {
    #[must_use]
    pub fn from_signals(signals: &[EvolutionSignal]) -> Self {
        let kind = proposal_kind(signals);
        let source_signal_ids = signals
            .iter()
            .map(|signal| signal.signal_id.clone())
            .collect::<Vec<_>>();
        let current_evidence = signals
            .iter()
            .flat_map(|signal| signal.evidence_refs.clone())
            .collect::<Vec<_>>();
        let problem_statement = signals
            .iter()
            .map(|signal| signal.summary.clone())
            .collect::<Vec<_>>()
            .join("; ");
        let critical = signals
            .iter()
            .any(|signal| signal.severity == EvolutionSignalSeverity::Critical);
        let risk = EvolutionProposalRisk {
            level: if critical { "high" } else { "medium" }.to_string(),
            boundaries: vec![
                "no_mainline_auto_write".to_string(),
                "human_approval_before_apply".to_string(),
                "sandbox_eval_required".to_string(),
            ],
            approval_required: true,
        };
        Self {
            proposal_id: format!("evo-proposal-{}", Uuid::new_v4()),
            kind,
            problem_statement,
            current_evidence,
            target_improvement: target_improvement(signals),
            expected_benefit: expected_benefit(signals),
            risk,
            acceptance_gates: vec![
                "source evidence exists".to_string(),
                "sandbox eval produces artifact".to_string(),
                "proposal does not mutate mainline automatically".to_string(),
                "human approval boundary is explicit".to_string(),
            ],
            rollback_strategy: "archive proposal and discard sandbox worktree/artifacts"
                .to_string(),
            source_signal_ids,
            created_at_ms: now_ms(),
            status: "draft".to_string(),
        }
    }

    #[must_use]
    pub fn to_plan_draft(&self) -> EvolutionPlanDraft {
        EvolutionPlanDraft {
            proposal: self.clone(),
            implementation_steps: vec![
                "collect source evidence".to_string(),
                "draft isolated candidate change or skill".to_string(),
                "run sandbox evaluation".to_string(),
                "compare baseline and candidate".to_string(),
                "request human approval before mainline application".to_string(),
            ],
            blocked_mainline_write: true,
        }
    }

    #[must_use]
    pub fn to_skill_draft(&self) -> EvolutionSkillDraft {
        let skill_id = format!("evolution-{}", slug(&self.problem_statement));
        let markdown = format!(
            "# {}\n\n## Purpose\n{}\n\n## Evidence\n{}\n\n## Operating Rules\n- Do not mutate mainline code without explicit approval.\n- Use sandbox evaluation before adoption.\n- Record rollback metadata.\n\n## Acceptance Gates\n{}\n",
            skill_id,
            self.target_improvement,
            self.current_evidence
                .iter()
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n"),
            self.acceptance_gates
                .iter()
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n")
        );
        EvolutionSkillDraft {
            skill_id,
            version: "0.1.0-draft".to_string(),
            source_proposal_id: self.proposal_id.clone(),
            permissions_note:
                "inherits agent/tool permissions; draft is inert until installed and approved"
                    .to_string(),
            evidence_refs: self.current_evidence.clone(),
            rollback_note: self.rollback_strategy.clone(),
            markdown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EvolutionProposalStore {
    path: PathBuf,
}

impl EvolutionProposalStore {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("proposals.jsonl"),
        }
    }

    pub fn append(&self, proposal: &EvolutionProposal) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| error.to_string())?;
        writeln!(
            file,
            "{}",
            serde_json::to_string(proposal).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())
    }

    pub fn list(&self) -> Result<Vec<EvolutionProposal>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|error| error.to_string())?;
        let mut proposals = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            proposals.push(
                serde_json::from_str::<EvolutionProposal>(&line)
                    .map_err(|error| error.to_string())?,
            );
        }
        proposals.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(proposals)
    }

    pub fn update_status(
        &self,
        proposal_id: &str,
        status: &str,
    ) -> Result<EvolutionProposal, String> {
        let mut proposals = self.list()?;
        let Some(proposal) = proposals
            .iter_mut()
            .find(|proposal| proposal.proposal_id == proposal_id)
        else {
            return Err("evolution proposal not found".to_string());
        };
        proposal.status = status.to_string();
        let updated = proposal.clone();
        proposals.sort_by(|left, right| left.created_at_ms.cmp(&right.created_at_ms));
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = fs::File::create(&self.path).map_err(|error| error.to_string())?;
        for proposal in &proposals {
            writeln!(
                file,
                "{}",
                serde_json::to_string(proposal).map_err(|error| error.to_string())?
            )
            .map_err(|error| error.to_string())?;
        }
        Ok(updated)
    }
}

fn proposal_kind(signals: &[EvolutionSignal]) -> EvolutionProposalKind {
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::MemoryNoise)
    {
        return EvolutionProposalKind::MemoryGovernanceAdjustment;
    }
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::MissingToolCapability)
    {
        return EvolutionProposalKind::ToolCapabilityRequest;
    }
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::EvalFailure)
    {
        return EvolutionProposalKind::TestScenario;
    }
    EvolutionProposalKind::PlanDraft
}

fn target_improvement(signals: &[EvolutionSignal]) -> String {
    let actions = signals
        .iter()
        .map(|signal| signal.suggested_action.clone())
        .collect::<Vec<_>>();
    format!(
        "Convert observed runtime gaps into governed improvements: {}",
        actions.join("; ")
    )
}

fn expected_benefit(signals: &[EvolutionSignal]) -> String {
    if signals
        .iter()
        .any(|signal| signal.signal_type == EvolutionSignalType::LowNoveltyToolLoop)
    {
        return "Reduce repeated tool calls by steering the model toward batch evidence, DAG execution, or delegation".to_string();
    }
    "Increase harness reliability without bypassing approval boundaries".to_string()
}

fn slug(input: &str) -> String {
    let mut output = input
        .chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else if ch.is_whitespace() || ch == '-' || ch == '_' {
                Some('-')
            } else {
                None
            }
        })
        .collect::<String>();
    while output.contains("--") {
        output = output.replace("--", "-");
    }
    output.trim_matches('-').chars().take(48).collect()
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evolution::signal::EvolutionSignal;

    #[test]
    fn planner_turns_memory_noise_into_governance_proposal_and_skill_draft() {
        let signal = EvolutionSignal::memory_noise(
            "runtime",
            "session-1",
            vec!["memory:packet:noise".to_string()],
        );
        let proposal = EvolutionProposal::from_signals(&[signal]);
        assert_eq!(
            proposal.kind,
            EvolutionProposalKind::MemoryGovernanceAdjustment
        );
        assert!(proposal.risk.approval_required);
        assert!(proposal
            .acceptance_gates
            .iter()
            .any(|gate| gate.contains("sandbox eval produces artifact")));

        let draft = proposal.to_skill_draft();
        assert_eq!(draft.source_proposal_id, proposal.proposal_id);
        assert!(draft.markdown.contains("Do not mutate mainline code"));
        assert!(draft.permissions_note.contains("inert"));
    }
}
