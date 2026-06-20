use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::bridge::{decide_candidate_promotion, BridgeDecision};
use crate::core::{EvidencePacket, FactRecord, Provenance};
use crate::growth::{GrowthCandidate, PromotionDecision};
use crate::health::{FactHealthIssue, FactHealthIssueKind};
use crate::hypothesis::FactReality;
use crate::indexer::{FactIndex, FactSearchHit};
use crate::memory::RecallQuery;
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
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::FactKernelService;
    use crate::core::{Confidence, EvidencePacket, FactId, FactRecord, FactSource, SourceKind};
    use crate::growth::{GrowthCandidate, PromotionDecision};
    use crate::health::FactHealthIssueKind;
    use crate::hypothesis::HypothesisBoundary;
    use crate::matrix::MatrixFact;
    use crate::memory::{MemoryCandidate, RecallQuery};

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
}
