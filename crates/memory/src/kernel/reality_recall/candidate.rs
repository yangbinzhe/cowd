use harness_contract::reality::{RealityBoundary, RecallSelectionReason, RecallSourceKind};
use serde::{Deserialize, Serialize};

use crate::types::{MemoryEntry, MemoryId, MemoryLayer};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecallCandidateScores {
    pub relevance: f32,
    pub authority: f32,
    pub recency: f32,
    pub final_score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_similarity: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bm25_score: Option<f32>,
}

impl RecallCandidateScores {
    pub fn from_entry(entry: &MemoryEntry, relevance: f32) -> Self {
        let authority = entry.confidence.clamp(0.0, 1.0);
        let recency = (1.0 - entry.staleness).clamp(0.0, 1.0);
        let final_score = (relevance * 0.55 + authority * 0.30 + recency * 0.15).clamp(0.0, 1.0);
        Self {
            relevance,
            authority,
            recency,
            final_score,
            vector_similarity: None,
            bm25_score: None,
        }
    }

    pub fn with_vector_similarity(mut self, similarity: f32) -> Self {
        self.vector_similarity = Some(similarity);
        self.relevance = self.relevance.max(similarity.clamp(0.0, 1.0));
        self.final_score =
            (self.relevance * 0.55 + self.authority * 0.30 + self.recency * 0.15).clamp(0.0, 1.0);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecallCandidateEvidence {
    pub refs: Vec<String>,
    pub boundary: RealityBoundary,
}

impl RecallCandidateEvidence {
    pub fn memory(id: MemoryId) -> Self {
        Self {
            refs: vec![format!("memory:{id}")],
            boundary: RealityBoundary::Observed,
        }
    }

    pub fn external(refs: Vec<String>, boundary: RealityBoundary) -> Self {
        Self { refs, boundary }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecallCandidate {
    pub id: MemoryId,
    pub title: String,
    pub layer: MemoryLayer,
    pub content_preview: String,
    pub source: RecallSourceKind,
    pub scores: RecallCandidateScores,
    pub evidence: RecallCandidateEvidence,
    pub reason: RecallSelectionReason,
}

impl RecallCandidate {
    pub fn from_entry(entry: MemoryEntry, source: RecallSourceKind, relevance: f32) -> Self {
        let scores = RecallCandidateScores::from_entry(&entry, relevance);
        let reason = RecallSelectionReason::selected(
            source,
            scores.final_score,
            vec![
                format!("layer:{:?}", entry.layer),
                format!("source:{source:?}"),
            ],
        );
        let evidence = RecallCandidateEvidence::memory(entry.id);
        let content_preview = if entry.content.len() > 240 {
            format!("{}...", entry.content.chars().take(240).collect::<String>())
        } else {
            entry.content.clone()
        };
        Self {
            id: entry.id,
            title: entry.title,
            layer: entry.layer,
            content_preview,
            source,
            scores,
            evidence,
            reason,
        }
    }

    pub fn from_external(
        title: impl Into<String>,
        content_preview: impl Into<String>,
        layer: MemoryLayer,
        source: RecallSourceKind,
        relevance: f32,
        authority: f32,
        evidence_refs: Vec<String>,
        boundary: RealityBoundary,
    ) -> Self {
        let relevance = relevance.clamp(0.0, 1.0);
        let authority = authority.clamp(0.0, 1.0);
        let recency = 1.0;
        let final_score = (relevance * 0.55 + authority * 0.30 + recency * 0.15).clamp(0.0, 1.0);
        let scores = RecallCandidateScores {
            relevance,
            authority,
            recency,
            final_score,
            vector_similarity: None,
            bm25_score: None,
        };
        let mut reason = RecallSelectionReason::selected(
            source,
            scores.final_score,
            vec![
                format!("source:{source:?}"),
                format!("boundary:{boundary:?}"),
            ],
        );
        if !boundary.can_be_authoritative() {
            reason.omitted_reason = Some("non-authoritative reality boundary".to_string());
        }
        let content_preview = content_preview.into();
        Self {
            id: MemoryId::new_v4(),
            title: title.into(),
            layer,
            content_preview: if content_preview.len() > 240 {
                format!(
                    "{}...",
                    content_preview.chars().take(240).collect::<String>()
                )
            } else {
                content_preview
            },
            source,
            scores,
            evidence: RecallCandidateEvidence::external(evidence_refs, boundary),
            reason,
        }
    }

    pub fn with_vector_similarity(mut self, similarity: f32) -> Self {
        self.scores = self.scores.with_vector_similarity(similarity);
        self.reason.score = self.scores.final_score;
        self.reason
            .matched_by
            .push(format!("vector_similarity:{similarity:.3}"));
        self
    }
}
