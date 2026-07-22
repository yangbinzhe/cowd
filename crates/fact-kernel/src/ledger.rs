//! Backend-neutral durable Fact and Growth ledger contract.
//!
//! The Fact kernel keeps promotion and recall semantics pure.  This module
//! owns the explicit failure boundary for canonical facts, evidence, Growth
//! events, and promotion receipts so a storage failure can never be replaced
//! by an in-process map pretending to be durable.

use std::collections::BTreeSet;

use harness_contract::growth::GrowthEvent;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{EvidencePacket, FactRecord};

pub type FactLedgerResult<T> = Result<T, FactLedgerError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactLedgerError {
    pub message: String,
}

impl FactLedgerError {
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for FactLedgerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for FactLedgerError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrowthPromotionRecord {
    pub id: String,
    pub event_id: String,
    pub target: String,
    pub status: String,
    pub target_id: Option<String>,
    pub summary: String,
    pub error: Option<String>,
    pub created_at: String,
}

/// The portion of a Growth ingest owned solely by the Fact/Growth ledger.
/// Adapters must commit this as one transaction so an event can never be
/// reported durable while its evidence, promoted facts or fact receipts are
/// absent. Matrix and Memory own their own follow-up receipts.
#[derive(Debug, Clone)]
pub struct FactGrowthBatch {
    pub event: GrowthEvent,
    pub evidence: EvidencePacket,
    pub facts: Vec<FactRecord>,
    pub promotions: Vec<GrowthPromotionRecord>,
}

impl GrowthPromotionRecord {
    #[must_use]
    pub fn stable_id(
        event_id: &str,
        target: &str,
        target_id: Option<&str>,
        summary: &str,
    ) -> String {
        format!("{event_id}:{target}:{}", target_id.unwrap_or(summary))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FactLedgerSnapshot {
    pub facts: Vec<FactRecord>,
    pub evidence: Vec<EvidencePacket>,
    pub growth_events: Vec<GrowthEvent>,
    pub growth_promotions: Vec<GrowthPromotionRecord>,
}

impl FactLedgerSnapshot {
    pub fn validate(&self) -> FactLedgerResult<()> {
        ensure_unique(self.facts.iter().map(|fact| fact.id.as_str()), "fact id")?;
        ensure_unique(
            self.evidence.iter().map(|packet| packet.id.as_str()),
            "evidence id",
        )?;
        ensure_unique(
            self.growth_events.iter().map(|event| event.id.as_str()),
            "growth event id",
        )?;
        ensure_unique(
            self.growth_promotions
                .iter()
                .map(|record| record.id.as_str()),
            "growth promotion id",
        )?;
        Ok(())
    }

    pub fn canonical_digest(&self) -> FactLedgerResult<String> {
        self.validate()?;
        let mut stable = self.clone();
        stable
            .facts
            .sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        stable
            .evidence
            .sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        stable
            .growth_events
            .sort_by(|left, right| left.id.cmp(&right.id));
        stable
            .growth_promotions
            .sort_by(|left, right| left.id.cmp(&right.id));
        let bytes = serde_json::to_vec(&stable)
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

fn ensure_unique<'a>(
    values: impl IntoIterator<Item = &'a str>,
    label: &str,
) -> FactLedgerResult<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(FactLedgerError::backend(format!(
                "duplicate {label} `{value}` in Fact ledger snapshot"
            )));
        }
    }
    Ok(())
}

/// Canonical persistent owner for Fact/Growth data.  Implementations must be
/// idempotent by the supplied business IDs and must return an error on every
/// failed write.  There is intentionally no memory-only success implementation.
pub trait FactLedger: Send + Sync {
    fn upsert_fact(&self, fact: FactRecord) -> FactLedgerResult<FactRecord>;
    fn get_fact(&self, fact_id: &str) -> FactLedgerResult<Option<FactRecord>>;
    fn list_facts(&self) -> FactLedgerResult<Vec<FactRecord>>;
    fn upsert_evidence(&self, evidence: EvidencePacket) -> FactLedgerResult<EvidencePacket>;
    fn get_evidence(&self, evidence_id: &str) -> FactLedgerResult<Option<EvidencePacket>>;
    fn list_evidence(&self) -> FactLedgerResult<Vec<EvidencePacket>>;
    fn record_growth_event(&self, event: GrowthEvent) -> FactLedgerResult<()>;
    fn list_growth_events(&self) -> FactLedgerResult<Vec<GrowthEvent>>;
    fn record_growth_promotion(&self, record: GrowthPromotionRecord) -> FactLedgerResult<()>;
    fn list_growth_promotions(&self) -> FactLedgerResult<Vec<GrowthPromotionRecord>>;

    fn persist_growth_fact_batch(&self, batch: FactGrowthBatch) -> FactLedgerResult<()> {
        // Test-only/simple adapters may use the compositional fallback. Real
        // SQLite and PostgreSQL adapters override this with one transaction.
        self.record_growth_event(batch.event)?;
        self.upsert_evidence(batch.evidence)?;
        for fact in batch.facts {
            self.upsert_fact(fact)?;
        }
        for promotion in batch.promotions {
            self.record_growth_promotion(promotion)?;
        }
        Ok(())
    }

    fn export_snapshot(&self) -> FactLedgerResult<FactLedgerSnapshot> {
        let snapshot = FactLedgerSnapshot {
            facts: self.list_facts()?,
            evidence: self.list_evidence()?,
            growth_events: self.list_growth_events()?,
            growth_promotions: self.list_growth_promotions()?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn import_snapshot(&self, snapshot: &FactLedgerSnapshot) -> FactLedgerResult<()> {
        snapshot.validate()?;
        for fact in snapshot.facts.iter().cloned() {
            self.upsert_fact(fact)?;
        }
        for evidence in snapshot.evidence.iter().cloned() {
            self.upsert_evidence(evidence)?;
        }
        for event in snapshot.growth_events.iter().cloned() {
            self.record_growth_event(event)?;
        }
        for record in snapshot.growth_promotions.iter().cloned() {
            self.record_growth_promotion(record)?;
        }
        Ok(())
    }
}

/// Explicit unavailable implementation for composition failures.  It gives
/// callers a deterministic error instead of silently creating a second owner.
#[derive(Debug, Clone)]
pub struct UnavailableFactLedger {
    reason: String,
}

impl UnavailableFactLedger {
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    fn unavailable<T>(&self) -> FactLedgerResult<T> {
        Err(FactLedgerError::backend(format!(
            "fact/growth ledger is unavailable: {}",
            self.reason
        )))
    }
}

impl FactLedger for UnavailableFactLedger {
    fn upsert_fact(&self, _fact: FactRecord) -> FactLedgerResult<FactRecord> {
        self.unavailable()
    }

    fn get_fact(&self, _fact_id: &str) -> FactLedgerResult<Option<FactRecord>> {
        self.unavailable()
    }

    fn list_facts(&self) -> FactLedgerResult<Vec<FactRecord>> {
        self.unavailable()
    }

    fn upsert_evidence(&self, _evidence: EvidencePacket) -> FactLedgerResult<EvidencePacket> {
        self.unavailable()
    }

    fn get_evidence(&self, _evidence_id: &str) -> FactLedgerResult<Option<EvidencePacket>> {
        self.unavailable()
    }

    fn list_evidence(&self) -> FactLedgerResult<Vec<EvidencePacket>> {
        self.unavailable()
    }

    fn record_growth_event(&self, _event: GrowthEvent) -> FactLedgerResult<()> {
        self.unavailable()
    }

    fn list_growth_events(&self) -> FactLedgerResult<Vec<GrowthEvent>> {
        self.unavailable()
    }

    fn record_growth_promotion(&self, _record: GrowthPromotionRecord) -> FactLedgerResult<()> {
        self.unavailable()
    }

    fn list_growth_promotions(&self) -> FactLedgerResult<Vec<GrowthPromotionRecord>> {
        self.unavailable()
    }

    fn persist_growth_fact_batch(&self, _batch: FactGrowthBatch) -> FactLedgerResult<()> {
        self.unavailable()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::{EvidencePacket, FactId, FactRecord, FactSource, SourceKind};

    fn source() -> FactSource {
        FactSource {
            kind: SourceKind::Growth,
            id: "growth-test".to_string(),
            label: None,
        }
    }

    #[test]
    fn canonical_digest_ignores_record_order_and_rejects_duplicate_ids() {
        let mut first = FactRecord::new("policy", "always verify output");
        first.id = FactId::from_string("fact-1");
        first.created_at = Utc::now();
        let evidence = EvidencePacket::new(source(), serde_json::json!({"a": 1}));
        let ordered = FactLedgerSnapshot {
            facts: vec![first.clone()],
            evidence: vec![evidence.clone()],
            ..FactLedgerSnapshot::default()
        };
        let reordered = FactLedgerSnapshot {
            evidence: vec![evidence],
            facts: vec![first.clone()],
            ..FactLedgerSnapshot::default()
        };
        assert_eq!(
            ordered.canonical_digest().unwrap(),
            reordered.canonical_digest().unwrap()
        );
        let duplicate = FactLedgerSnapshot {
            facts: vec![first.clone(), first],
            ..FactLedgerSnapshot::default()
        };
        assert!(duplicate.canonical_digest().is_err());
    }

    #[test]
    fn unavailable_ledger_never_reports_success() {
        let ledger = UnavailableFactLedger::new("test outage");
        assert!(ledger.list_facts().is_err());
        assert!(ledger
            .upsert_fact(FactRecord::new("policy", "do not fabricate durability"))
            .is_err());
    }
}
