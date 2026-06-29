use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::candidate::{FactCandidate, FactCandidateId, FactCandidateRelation};
use crate::core::{FactId, FactRecord};
use crate::extraction::FactExtractionBatchId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactReviewDecisionKind {
    Promote,
    Hold,
    Reject,
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactConflict {
    pub candidate_id: FactCandidateId,
    pub existing_fact_id: FactId,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactReviewDecision {
    pub candidate: FactCandidate,
    pub decision: FactReviewDecisionKind,
    pub reason: String,
    pub promoted_fact: Option<FactRecord>,
    pub relations: Vec<FactCandidateRelation>,
}

impl FactReviewDecision {
    #[must_use]
    pub fn promote(
        candidate: FactCandidate,
        reason: impl Into<String>,
        promoted_fact: FactRecord,
    ) -> Self {
        Self {
            candidate,
            decision: FactReviewDecisionKind::Promote,
            reason: reason.into(),
            promoted_fact: Some(promoted_fact),
            relations: Vec::new(),
        }
    }

    #[must_use]
    pub fn hold(candidate: FactCandidate, reason: impl Into<String>) -> Self {
        Self {
            candidate,
            decision: FactReviewDecisionKind::Hold,
            reason: reason.into(),
            promoted_fact: None,
            relations: Vec::new(),
        }
    }

    #[must_use]
    pub fn reject(candidate: FactCandidate, reason: impl Into<String>) -> Self {
        Self {
            candidate,
            decision: FactReviewDecisionKind::Reject,
            reason: reason.into(),
            promoted_fact: None,
            relations: Vec::new(),
        }
    }

    #[must_use]
    pub fn conflict(
        candidate: FactCandidate,
        reason: impl Into<String>,
        relations: Vec<FactCandidateRelation>,
    ) -> Self {
        Self {
            candidate,
            decision: FactReviewDecisionKind::Conflict,
            reason: reason.into(),
            promoted_fact: None,
            relations,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactReviewReceipt {
    pub batch_id: FactExtractionBatchId,
    pub promoted: Vec<FactReviewDecision>,
    pub held: Vec<FactReviewDecision>,
    pub rejected: Vec<FactReviewDecision>,
    pub conflicts: Vec<FactConflict>,
    pub decisions: Vec<FactReviewDecision>,
    pub reviewed_at: DateTime<Utc>,
}

impl FactReviewReceipt {
    #[must_use]
    pub fn empty(batch_id: FactExtractionBatchId) -> Self {
        Self {
            batch_id,
            promoted: Vec::new(),
            held: Vec::new(),
            rejected: Vec::new(),
            conflicts: Vec::new(),
            decisions: Vec::new(),
            reviewed_at: Utc::now(),
        }
    }

    pub fn push_decision(&mut self, decision: FactReviewDecision) {
        match decision.decision {
            FactReviewDecisionKind::Promote => self.promoted.push(decision.clone()),
            FactReviewDecisionKind::Hold => self.held.push(decision.clone()),
            FactReviewDecisionKind::Reject => self.rejected.push(decision.clone()),
            FactReviewDecisionKind::Conflict => self.held.push(decision.clone()),
        }
        self.decisions.push(decision);
    }
}
