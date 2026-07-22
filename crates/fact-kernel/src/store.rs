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

    /// Build a purely semantic store from an already durable ledger snapshot.
    /// The returned store is an index/input cache only; callers must not treat
    /// it as a persistence fallback.
    #[must_use]
    pub fn from_records(
        facts: impl IntoIterator<Item = FactRecord>,
        evidence: impl IntoIterator<Item = EvidencePacket>,
    ) -> Self {
        Self {
            facts: facts
                .into_iter()
                .map(|fact| (fact.id.as_str().to_string(), fact))
                .collect(),
            evidence: evidence
                .into_iter()
                .map(|packet| (packet.id.as_str().to_string(), packet))
                .collect(),
        }
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
