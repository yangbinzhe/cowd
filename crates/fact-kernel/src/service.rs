use std::collections::BTreeMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::bridge::{decide_candidate_promotion, BridgeDecision};
use crate::candidate::{FactCandidate, FactCandidateRelation, FactCandidateRelationKind};
use crate::core::{EvidencePacket, FactId, FactRecord, Provenance};
use crate::extraction::FactExtractionBatch;
use crate::growth::{GrowthCandidate, PromotionDecision};
use crate::health::{FactHealthIssue, FactHealthIssueKind};
use crate::hypothesis::FactReality;
use crate::indexer::{FactIndex, FactSearchHit};
use crate::memory::RecallQuery;
use crate::review::{FactConflict, FactReviewDecision, FactReviewReceipt};
use crate::store::{FactStore, InMemoryFactStore};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionReceipt {
    pub decision: BridgeDecision,
    pub promoted_fact: Option<FactRecord>,
}

#[derive(Debug, Clone)]
pub struct FactKernelService<S = InMemoryFactStore> {
    store: S,
    index: FactIndex,
}

impl Default for FactKernelService<InMemoryFactStore> {
    fn default() -> Self {
        Self::new()
    }
}

impl FactKernelService<InMemoryFactStore> {
    #[must_use]
    pub fn new() -> Self {
        Self::with_store(InMemoryFactStore::new())
    }
}

impl<S> FactKernelService<S>
where
    S: FactStore,
{
    #[must_use]
    pub fn with_store(store: S) -> Self {
        let facts = store.list_facts();
        let mut index = FactIndex::new();
        index.rebuild(&facts);
        Self { store, index }
    }

    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn ingest_evidence(&mut self, evidence: EvidencePacket) -> EvidencePacket {
        self.store.insert_evidence(evidence)
    }

    pub fn upsert_fact(&mut self, fact: FactRecord) -> FactRecord {
        let stored = self.store.upsert_fact(fact);
        self.index.index_fact(stored.clone());
        stored
    }

    #[must_use]
    pub fn recall(&self, query: &RecallQuery) -> Vec<FactSearchHit> {
        self.index.search(query)
    }

    #[must_use]
    pub fn facts_by_type(&self, fact_type: &str, limit: usize) -> Vec<FactRecord> {
        self.index.by_type(fact_type, limit)
    }

    pub fn promote_candidate(&mut self, candidate: GrowthCandidate) -> PromotionReceipt {
        let decision = decide_candidate_promotion(candidate);
        let promoted_fact = if decision.decision == PromotionDecision::Promote {
            self.fact_from_candidate(&decision.candidate)
                .map(|fact| self.upsert_fact(fact))
        } else {
            None
        };

        PromotionReceipt {
            decision,
            promoted_fact,
        }
    }

    pub fn review_candidates(&mut self, batch: FactExtractionBatch) -> FactReviewReceipt {
        let mut receipt = FactReviewReceipt::empty(batch.batch_id);

        for candidate in batch.candidates {
            let decision = self.review_single_candidate(candidate);
            if let Some(conflict) = conflict_from_decision(&decision) {
                receipt.conflicts.push(conflict);
            }
            receipt.push_decision(decision);
        }

        receipt
    }

    #[must_use]
    pub fn evaluate_health(&self) -> Vec<FactHealthIssue> {
        let mut issues = Vec::new();
        let facts = self.store.list_facts();

        for fact in &facts {
            if fact.confidence.basis_points() < 5_000 {
                issues.push(FactHealthIssue {
                    fact_id: Some(fact.id.clone()),
                    kind: FactHealthIssueKind::LowConfidence,
                    detail: "fact confidence is below operational floor".to_string(),
                });
            }

            for evidence_id in &fact.evidence {
                if self.store.get_evidence(evidence_id).is_none() {
                    issues.push(FactHealthIssue {
                        fact_id: Some(fact.id.clone()),
                        kind: FactHealthIssueKind::MissingEvidence,
                        detail: format!("missing evidence {}", evidence_id.as_str()),
                    });
                }
            }
        }
        issues.extend(detect_matrix_conflicts(&facts));

        issues
    }

    fn fact_from_candidate(&self, candidate: &GrowthCandidate) -> Option<FactRecord> {
        match candidate {
            GrowthCandidate::Memory(memory) => {
                if memory.boundary.reality != FactReality::Observed {
                    return None;
                }
                let mut fact = FactRecord::new("memory", memory.summary.clone());
                fact.confidence = memory.confidence;
                fact.evidence = memory.evidence.clone();
                fact.provenance.push(Provenance {
                    source: memory.source.clone(),
                    observed_at: Utc::now(),
                    trace_id: None,
                });
                Some(fact)
            }
            GrowthCandidate::Matrix(matrix) => {
                if matrix.boundary.reality != FactReality::Observed {
                    return None;
                }
                let statement = format!("{} {} {}", matrix.entity, matrix.predicate, matrix.value);
                let mut fact = FactRecord::new(format!("matrix.{}", matrix.predicate), statement);
                fact.id = matrix.id.clone();
                fact.confidence = matrix.confidence;
                fact.evidence = matrix.evidence.clone();
                fact.provenance.push(Provenance {
                    source: matrix.source.clone(),
                    observed_at: Utc::now(),
                    trace_id: None,
                });
                Some(fact)
            }
            GrowthCandidate::PolicyLearning {
                summary,
                confidence,
            } => {
                let mut fact = FactRecord::new("policy_learning", summary.clone());
                fact.confidence = *confidence;
                Some(fact)
            }
        }
    }

    fn review_single_candidate(&mut self, candidate: FactCandidate) -> FactReviewDecision {
        if candidate.evidence.is_empty() {
            return FactReviewDecision::hold(candidate, "fact candidate has no evidence");
        }

        if candidate.reality != FactReality::Observed && candidate.reality != FactReality::Inferred
        {
            return FactReviewDecision::reject(
                candidate,
                "hypothetical or simulated candidate cannot be promoted",
            );
        }

        if let Some(existing) = self.find_duplicate_fact(&candidate) {
            let relations = vec![FactCandidateRelation {
                kind: FactCandidateRelationKind::Duplicates,
                target: existing.id.as_str().to_string(),
                reason: "candidate statement already exists in the same scope".to_string(),
            }];
            let mut decision = FactReviewDecision::hold(candidate, "duplicate fact candidate held");
            decision.relations = relations;
            return decision;
        }

        if let Some(existing) = self.find_conflicting_fact(&candidate) {
            return FactReviewDecision::conflict(
                candidate.clone(),
                "candidate conflicts with active fact in the same scope and type",
                vec![FactCandidateRelation {
                    kind: FactCandidateRelationKind::ConflictsWith,
                    target: existing.id.as_str().to_string(),
                    reason: "same fact type and scope but different statement".to_string(),
                }],
            );
        }

        let fact = self.fact_from_review_candidate(&candidate);
        let stored = self.upsert_fact(fact);
        FactReviewDecision::promote(
            candidate,
            "observed candidate has evidence and passed conflict review",
            stored,
        )
    }

    fn fact_from_review_candidate(&self, candidate: &FactCandidate) -> FactRecord {
        let mut fact = FactRecord::new(candidate.fact_type.clone(), candidate.statement.clone());
        fact.id = FactId::from_string(format!("candidate:{}", candidate.candidate_id.as_str()));
        fact.scope_key = Some(candidate.scope.key());
        fact.confidence = candidate.confidence;
        fact.evidence = candidate.evidence.clone();
        fact.provenance.push(Provenance {
            source: candidate.source.clone(),
            observed_at: Utc::now(),
            trace_id: Some(candidate.candidate_id.as_str().to_string()),
        });
        fact.relations = candidate
            .relations
            .iter()
            .map(|relation| {
                format!(
                    "{:?}:{}:{}",
                    relation.kind, relation.target, relation.reason
                )
            })
            .collect();
        fact
    }

    fn find_duplicate_fact(&self, candidate: &FactCandidate) -> Option<FactRecord> {
        let scope_key = candidate.scope.key();
        self.store.list_facts().into_iter().find(|fact| {
            fact.status == "active"
                && fact.fact_type == candidate.fact_type
                && fact.scope_key.as_deref() == Some(scope_key.as_str())
                && normalize_statement(&fact.statement) == normalize_statement(&candidate.statement)
        })
    }

    fn find_conflicting_fact(&self, candidate: &FactCandidate) -> Option<FactRecord> {
        if !fact_type_requires_unique_statement(&candidate.fact_type) {
            return None;
        }

        let scope_key = candidate.scope.key();
        self.store.list_facts().into_iter().find(|fact| {
            fact.status == "active"
                && fact.fact_type == candidate.fact_type
                && fact.scope_key.as_deref() == Some(scope_key.as_str())
                && normalize_statement(&fact.statement) != normalize_statement(&candidate.statement)
        })
    }
}

fn conflict_from_decision(decision: &FactReviewDecision) -> Option<FactConflict> {
    decision
        .relations
        .iter()
        .find(|relation| relation.kind == FactCandidateRelationKind::ConflictsWith)
        .map(|relation| FactConflict {
            candidate_id: decision.candidate.candidate_id.clone(),
            existing_fact_id: FactId::from_string(relation.target.clone()),
            reason: relation.reason.clone(),
        })
}

fn normalize_statement(statement: &str) -> String {
    statement
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn fact_type_requires_unique_statement(fact_type: &str) -> bool {
    fact_type.starts_with("matrix.")
}

fn detect_matrix_conflicts(facts: &[FactRecord]) -> Vec<FactHealthIssue> {
    let mut grouped: BTreeMap<(String, String), Vec<&FactRecord>> = BTreeMap::new();
    for fact in facts
        .iter()
        .filter(|fact| fact.fact_type.starts_with("matrix."))
    {
        if let Some((entity, predicate, _value)) = parse_matrix_statement(&fact.statement) {
            grouped
                .entry((entity.to_string(), predicate.to_string()))
                .or_default()
                .push(fact);
        }
    }

    let mut issues = Vec::new();
    for ((entity, predicate), facts) in grouped {
        let mut values = BTreeMap::new();
        for fact in facts {
            let Some((_entity, _predicate, value)) = parse_matrix_statement(&fact.statement) else {
                continue;
            };
            values
                .entry(value.to_string())
                .or_insert_with(Vec::new)
                .push(fact.id.clone());
        }
        if values.len() <= 1 {
            continue;
        }
        let detail = format!(
            "conflicting matrix values for {entity}.{predicate}: {}",
            values.keys().cloned().collect::<Vec<_>>().join(" vs ")
        );
        for ids in values.values() {
            for id in ids {
                issues.push(FactHealthIssue {
                    fact_id: Some(id.clone()),
                    kind: FactHealthIssueKind::Conflict,
                    detail: detail.clone(),
                });
            }
        }
    }
    issues
}

fn parse_matrix_statement(statement: &str) -> Option<(&str, &str, &str)> {
    let mut parts = statement.splitn(3, ' ');
    let entity = parts.next()?.trim();
    let predicate = parts.next()?.trim();
    let value = parts.next()?.trim();
    if entity.is_empty() || predicate.is_empty() || value.is_empty() {
        None
    } else {
        Some((entity, predicate, value))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::FactKernelService;
    use crate::candidate::{ExtractionMethod, FactCandidate, FactScope};
    use crate::core::{Confidence, EvidencePacket, FactId, FactRecord, FactSource, SourceKind};
    use crate::extraction::{FactExtractionBatch, FactExtractionTrigger};
    use crate::growth::{GrowthCandidate, PromotionDecision};
    use crate::health::FactHealthIssueKind;
    use crate::hypothesis::{FactReality, HypothesisBoundary};
    use crate::matrix::MatrixFact;
    use crate::memory::{MemoryCandidate, RecallQuery};
    use crate::review::FactReviewDecisionKind;

    fn source() -> FactSource {
        FactSource {
            kind: SourceKind::Runtime,
            id: "runtime-test".to_string(),
            label: None,
        }
    }

    #[test]
    fn stores_and_recalls_facts_by_statement_tokens() {
        let mut service = FactKernelService::new();
        let fact = FactRecord::new("memory", "user prefers concise progress updates");

        let stored = service.upsert_fact(fact);
        let hits = service.recall(&RecallQuery::new("concise updates"));

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].fact.id, stored.id);
    }

    #[test]
    fn promoted_observed_memory_candidate_becomes_fact() {
        let mut service = FactKernelService::new();
        let evidence = service.ingest_evidence(EvidencePacket::new(source(), json!({"turn": 1})));

        let receipt = service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
            summary: "user prefers direct execution".to_string(),
            source: source(),
            evidence: vec![evidence.id.clone()],
            confidence: Confidence::from_basis_points(8_100),
            boundary: HypothesisBoundary::observed(),
            tags: vec!["preference".to_string()],
        }));

        assert_eq!(receipt.decision.decision, PromotionDecision::Promote);
        assert!(receipt.promoted_fact.is_some());
        assert!(service.evaluate_health().is_empty());
    }

    #[test]
    fn promoted_observed_matrix_candidate_keeps_matrix_fact_id() {
        let mut service = FactKernelService::new();
        let evidence =
            service.ingest_evidence(EvidencePacket::new(source(), json!({"gate": true})));
        let matrix_id = FactId::from_string("matrix-fact-1");

        let receipt = service.promote_candidate(GrowthCandidate::Matrix(MatrixFact {
            id: matrix_id.clone(),
            entity: "system".to_string(),
            predicate: "passes_gate".to_string(),
            value: json!(true),
            source: source(),
            evidence: vec![evidence.id.clone()],
            confidence: Confidence::from_basis_points(8_500),
            boundary: HypothesisBoundary::observed(),
        }));

        assert_eq!(
            receipt.promoted_fact.as_ref().map(|fact| fact.id.clone()),
            Some(matrix_id)
        );
        assert_eq!(service.facts_by_type("matrix.passes_gate", 10).len(), 1);
    }

    #[test]
    fn hypothetical_candidate_is_rejected_and_not_stored() {
        let mut service = FactKernelService::new();

        let receipt = service.promote_candidate(GrowthCandidate::Memory(MemoryCandidate {
            summary: "simulated preference".to_string(),
            source: source(),
            evidence: vec![],
            confidence: Confidence::from_basis_points(9_000),
            boundary: HypothesisBoundary::hypothetical("scenario"),
            tags: vec![],
        }));

        assert_eq!(receipt.decision.decision, PromotionDecision::Reject);
        assert!(receipt.promoted_fact.is_none());
        assert!(service.recall(&RecallQuery::new("simulated")).is_empty());
    }

    #[test]
    fn health_reports_missing_evidence() {
        let mut service = FactKernelService::new();
        let mut fact = FactRecord::new("memory", "needs real evidence");
        fact.evidence.push(crate::core::EvidenceId::new());
        let stored = service.upsert_fact(fact);

        let issues = service.evaluate_health();

        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].fact_id, Some(stored.id));
        assert_eq!(issues[0].kind, FactHealthIssueKind::MissingEvidence);
    }

    #[test]
    fn health_reports_conflicting_matrix_values() {
        let mut service = FactKernelService::new();
        let evidence = service.ingest_evidence(EvidencePacket::new(source(), json!({"turn": 1})));

        for (id, value) in [
            ("matrix-pref-immersive", json!("immersive_then_review")),
            ("matrix-pref-pause", json!("pause_each_step")),
        ] {
            let receipt = service.promote_candidate(GrowthCandidate::Matrix(MatrixFact {
                id: FactId::from_string(id),
                entity: "user.workflow".to_string(),
                predicate: "prefers_flow".to_string(),
                value,
                source: source(),
                evidence: vec![evidence.id.clone()],
                confidence: Confidence::from_basis_points(8_500),
                boundary: HypothesisBoundary::observed(),
            }));
            assert_eq!(receipt.decision.decision, PromotionDecision::Promote);
        }

        let conflicts = service
            .evaluate_health()
            .into_iter()
            .filter(|issue| issue.kind == FactHealthIssueKind::Conflict)
            .collect::<Vec<_>>();

        assert_eq!(conflicts.len(), 2);
        assert!(conflicts
            .iter()
            .all(|issue| issue.detail.contains("user.workflow.prefers_flow")));
    }

    fn review_candidate(statement: &str) -> FactCandidate {
        FactCandidate::observed(
            "memory.preference",
            statement,
            FactScope::Task("task-a".to_string()),
            source(),
        )
        .with_method(ExtractionMethod::Checkpoint, "test-extractor:v1")
        .with_confidence(Confidence::from_basis_points(8_500))
    }

    fn matrix_review_candidate(statement: &str) -> FactCandidate {
        FactCandidate::observed(
            "matrix.prefers_flow",
            statement,
            FactScope::Task("task-a".to_string()),
            source(),
        )
        .with_method(ExtractionMethod::Checkpoint, "test-extractor:v1")
        .with_confidence(Confidence::from_basis_points(8_500))
    }

    #[test]
    fn review_holds_candidate_without_evidence() {
        let mut service = FactKernelService::new();
        let batch = FactExtractionBatch::new(
            FactExtractionTrigger::SessionCompaction,
            vec![review_candidate("user prefers Chinese reports")],
        );

        let receipt = service.review_candidates(batch);

        assert_eq!(receipt.promoted.len(), 0);
        assert_eq!(receipt.held.len(), 1);
        assert_eq!(receipt.held[0].decision, FactReviewDecisionKind::Hold);
        assert!(service
            .recall(&RecallQuery::new("Chinese reports"))
            .is_empty());
    }

    #[test]
    fn review_rejects_hypothetical_candidate_even_with_evidence() {
        let mut service = FactKernelService::new();
        let evidence = service.ingest_evidence(EvidencePacket::new(source(), json!({"turn": 1})));
        let candidate = review_candidate("hypothetical future preference")
            .with_evidence(vec![evidence.id])
            .with_reality(FactReality::Hypothetical);
        let batch = FactExtractionBatch::new(FactExtractionTrigger::Manual, vec![candidate]);

        let receipt = service.review_candidates(batch);

        assert_eq!(receipt.promoted.len(), 0);
        assert_eq!(receipt.rejected.len(), 1);
        assert_eq!(receipt.rejected[0].decision, FactReviewDecisionKind::Reject);
    }

    #[test]
    fn review_promotes_observed_candidate_with_evidence() {
        let mut service = FactKernelService::new();
        let evidence = service.ingest_evidence(EvidencePacket::new(source(), json!({"turn": 1})));
        let candidate =
            review_candidate("user prefers Chinese reports").with_evidence(vec![evidence.id]);
        let batch =
            FactExtractionBatch::new(FactExtractionTrigger::SessionCompaction, vec![candidate]);

        let receipt = service.review_candidates(batch);

        assert_eq!(receipt.promoted.len(), 1);
        assert_eq!(receipt.held.len(), 0);
        assert_eq!(
            receipt.promoted[0].decision,
            FactReviewDecisionKind::Promote
        );
        assert_eq!(
            service.recall(&RecallQuery::new("Chinese reports")).len(),
            1
        );
    }

    #[test]
    fn review_holds_conflicting_candidate_in_same_scope() {
        let mut service = FactKernelService::new();
        let first_evidence =
            service.ingest_evidence(EvidencePacket::new(source(), json!({"turn": 1})));
        let first = matrix_review_candidate("user.workflow prefers_flow immersive_then_review")
            .with_evidence(vec![first_evidence.id]);
        let first_receipt = service.review_candidates(FactExtractionBatch::new(
            FactExtractionTrigger::SessionCompaction,
            vec![first],
        ));
        assert_eq!(first_receipt.promoted.len(), 1);

        let second_evidence =
            service.ingest_evidence(EvidencePacket::new(source(), json!({"turn": 2})));
        let second = matrix_review_candidate("user.workflow prefers_flow pause_each_step")
            .with_evidence(vec![second_evidence.id]);
        let second_receipt = service.review_candidates(FactExtractionBatch::new(
            FactExtractionTrigger::SessionCompaction,
            vec![second],
        ));

        assert_eq!(second_receipt.promoted.len(), 0);
        assert_eq!(second_receipt.held.len(), 1);
        assert_eq!(second_receipt.conflicts.len(), 1);
        assert_eq!(
            second_receipt.decisions[0].decision,
            FactReviewDecisionKind::Conflict
        );
    }
}
