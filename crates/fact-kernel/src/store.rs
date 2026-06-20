use std::collections::BTreeMap;

use crate::core::{EvidenceId, EvidencePacket, FactId, FactRecord};

pub trait FactStore {
    fn upsert_fact(&mut self, fact: FactRecord) -> FactRecord;
    fn get_fact(&self, id: &FactId) -> Option<&FactRecord>;
    fn list_facts(&self) -> Vec<FactRecord>;
    fn insert_evidence(&mut self, evidence: EvidencePacket) -> EvidencePacket;
    fn get_evidence(&self, id: &EvidenceId) -> Option<&EvidencePacket>;
    fn list_evidence(&self) -> Vec<EvidencePacket>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryFactStore {
    facts: BTreeMap<String, FactRecord>,
    evidence: BTreeMap<String, EvidencePacket>,
}

impl InMemoryFactStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn fact_count(&self) -> usize {
        self.facts.len()
    }

    #[must_use]
    pub fn evidence_count(&self) -> usize {
        self.evidence.len()
    }
}

impl FactStore for InMemoryFactStore {
    fn upsert_fact(&mut self, fact: FactRecord) -> FactRecord {
        self.facts
            .insert(fact.id.as_str().to_string(), fact.clone());
        fact
    }

    fn get_fact(&self, id: &FactId) -> Option<&FactRecord> {
        self.facts.get(id.as_str())
    }

    fn list_facts(&self) -> Vec<FactRecord> {
        self.facts.values().cloned().collect()
    }

    fn insert_evidence(&mut self, evidence: EvidencePacket) -> EvidencePacket {
        self.evidence
            .insert(evidence.id.as_str().to_string(), evidence.clone());
        evidence
    }

    fn get_evidence(&self, id: &EvidenceId) -> Option<&EvidencePacket> {
        self.evidence.get(id.as_str())
    }

    fn list_evidence(&self) -> Vec<EvidencePacket> {
        self.evidence.values().cloned().collect()
    }
}
