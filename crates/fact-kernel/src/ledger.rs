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

/// Storage-level Reality recall contract. Authorization and the result bound
/// are part of the query so an adapter can never implement recall by listing
/// the newest global rows and filtering them in Runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactRecallQuery {
    /// Exact Fact ids already granted by the immutable Binding lease.
    pub authorized_fact_ids: Vec<String>,
    /// Scope keys in the current workspace/task/session/team boundary.
    pub authorized_scope_keys: Vec<String>,
    /// Reality boundaries that may be recalled inside an authorized scope.
    pub authorized_boundaries: Vec<String>,
    /// Normalized, de-duplicated terms. A record matches when any term occurs
    /// in its statement; an empty list means no textual restriction.
    pub terms: Vec<String>,
    /// Maximum records returned by the storage adapter.
    pub limit: usize,
}

impl FactRecallQuery {
    #[must_use]
    pub fn new(
        authorized_fact_ids: Vec<String>,
        authorized_scope_keys: Vec<String>,
        authorized_boundaries: Vec<String>,
        query: &str,
        limit: usize,
    ) -> Self {
        Self {
            authorized_fact_ids: normalized_values(authorized_fact_ids),
            authorized_scope_keys: normalized_values(authorized_scope_keys),
            authorized_boundaries: normalized_values(authorized_boundaries),
            terms: normalized_terms(query),
            limit: limit.clamp(1, 65),
        }
    }

    #[must_use]
    pub fn is_authorized(&self) -> bool {
        !self.authorized_fact_ids.is_empty()
            || (!self.authorized_scope_keys.is_empty() && !self.authorized_boundaries.is_empty())
    }
}

/// First-page creation fence plus invalidations for the exact granted scopes/ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactCatalogSnapshot {
    pub fence: String,
    pub revisions: std::collections::BTreeMap<String, i64>,
}
#[derive(Debug, Clone)]
pub struct FactCatalogQuery {
    pub authorization: FactRecallQuery,
    /// A requested id restricts authorized rows; it never grants access.
    pub exact_id: Option<String>,
    pub after_id: Option<String>,
    pub snapshot: Option<FactCatalogSnapshot>,
}
#[derive(Debug, Clone)]
pub struct FactCatalogPage {
    pub records: Vec<FactRecord>,
    pub snapshot: FactCatalogSnapshot,
    pub next_id: Option<String>,
}
impl FactCatalogQuery {
    #[must_use]
    pub fn revision_keys(&self) -> Vec<String> {
        self.authorization
            .authorized_fact_ids
            .iter()
            .map(|id| format!("fact:{id}"))
            .chain(
                self.authorization
                    .authorized_scope_keys
                    .iter()
                    .map(|scope| format!("scope:{scope}")),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

fn normalized_values(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalized_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric() && !character.is_alphabetic())
        .map(str::trim)
        .filter(|term| term.chars().count() > 1)
        .map(str::to_lowercase)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

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
    /// Query only Binding-authorized candidates with a storage-enforced
    /// result bound and deterministic confidence/time/id ordering.
    fn recall_facts(&self, query: &FactRecallQuery) -> FactLedgerResult<Vec<FactRecord>>;
    fn catalog_page(&self, query: &FactCatalogQuery) -> FactLedgerResult<FactCatalogPage>;
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

    fn catalog_page(&self, _query: &FactCatalogQuery) -> FactLedgerResult<FactCatalogPage> {
        Err(FactLedgerError::backend(self.reason.clone()))
    }

    fn recall_facts(&self, _query: &FactRecallQuery) -> FactLedgerResult<Vec<FactRecord>> {
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

/// Explicit non-durable Fact port for pure selection/reducer tests.
///
/// This adapter is never a persistence or recovery acceptance target and is
/// absent from normal production builds. Database semantics must be verified
/// against the PostgreSQL adapter.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Default)]
pub struct EphemeralFactLedger {
    snapshot: std::sync::Mutex<FactLedgerSnapshot>,
    catalog: std::sync::Mutex<EphemeralFactCatalog>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Default)]
struct EphemeralFactCatalog {
    sequence: u64,
    created: std::collections::BTreeMap<String, u64>,
    revisions: std::collections::BTreeMap<String, i64>,
}

#[cfg(any(test, feature = "test-support"))]
impl EphemeralFactLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl FactLedger for EphemeralFactLedger {
    fn upsert_fact(&self, fact: FactRecord) -> FactLedgerResult<FactRecord> {
        // Both writers and directory readers take catalog before snapshot.
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        let mut state = self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        if let Some(existing) = state.facts.iter_mut().find(|item| item.id == fact.id) {
            if serde_json::to_value(&*existing)
                .map_err(|e| FactLedgerError::backend(e.to_string()))?
                != serde_json::to_value(&fact)
                    .map_err(|e| FactLedgerError::backend(e.to_string()))?
            {
                let mut keys = BTreeSet::from([format!("fact:{}", fact.id.as_str())]);
                for scope in [existing.scope_key.as_ref(), fact.scope_key.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    keys.insert(format!("scope:{scope}"));
                }
                for key in keys {
                    *catalog.revisions.entry(key).or_default() += 1;
                }
            }
            *existing = fact.clone();
        } else {
            catalog.sequence += 1;
            let sequence = catalog.sequence;
            catalog
                .created
                .insert(fact.id.as_str().to_owned(), sequence);
            state.facts.push(fact.clone());
        }
        Ok(fact)
    }

    fn catalog_page(&self, query: &FactCatalogQuery) -> FactLedgerResult<FactCatalogPage> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        let state = self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        let revisions = query
            .revision_keys()
            .into_iter()
            .map(|key| {
                let revision = catalog.revisions.get(&key).copied().unwrap_or_default();
                (key, revision)
            })
            .collect();
        let snapshot = query.snapshot.clone().unwrap_or(FactCatalogSnapshot {
            fence: format!("ephemeral:{}", catalog.sequence),
            revisions,
        });
        let current: std::collections::BTreeMap<_, _> = query
            .revision_keys()
            .into_iter()
            .map(|key| {
                let rev = catalog.revisions.get(&key).copied().unwrap_or_default();
                (key, rev)
            })
            .collect();
        if snapshot.revisions != current {
            return Err(FactLedgerError::backend(
                "Fact directory source changed; restart discovery",
            ));
        }
        let fence: u64 = snapshot
            .fence
            .strip_prefix("ephemeral:")
            .and_then(|v| v.parse().ok())
            .filter(|v| *v <= catalog.sequence)
            .ok_or_else(|| FactLedgerError::backend("invalid Fact snapshot"))?;
        let auth = &query.authorization;
        let mut records = state
            .facts
            .iter()
            .filter(|fact| {
                (auth
                    .authorized_fact_ids
                    .iter()
                    .any(|id| id == fact.id.as_str())
                    || (fact
                        .scope_key
                        .as_ref()
                        .is_some_and(|scope| auth.authorized_scope_keys.contains(scope))
                        && auth
                            .authorized_boundaries
                            .iter()
                            .any(|boundary| boundary == fact.boundary.as_str())))
                    && query
                        .exact_id
                        .as_ref()
                        .is_none_or(|id| id == fact.id.as_str())
                    && query
                        .after_id
                        .as_ref()
                        .is_none_or(|id| fact.id.as_str() > id.as_str())
                    && catalog
                        .created
                        .get(fact.id.as_str())
                        .is_some_and(|sequence| *sequence <= fence)
                    && (auth.terms.is_empty()
                        || auth
                            .terms
                            .iter()
                            .any(|term| fact.statement.to_lowercase().contains(term)))
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));
        let limit = auth.limit.clamp(1, 65);
        let more = records.len() > limit;
        records.truncate(limit);
        let next_id = more.then(|| records.last().unwrap().id.as_str().to_owned());
        Ok(FactCatalogPage {
            records,
            snapshot,
            next_id,
        })
    }

    fn get_fact(&self, fact_id: &str) -> FactLedgerResult<Option<FactRecord>> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .facts
            .iter()
            .find(|item| item.id.as_str() == fact_id)
            .cloned())
    }

    fn list_facts(&self) -> FactLedgerResult<Vec<FactRecord>> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .facts
            .clone())
    }

    fn recall_facts(&self, query: &FactRecallQuery) -> FactLedgerResult<Vec<FactRecord>> {
        if !query.is_authorized() {
            return Ok(Vec::new());
        }
        let mut facts = self
            .list_facts()?
            .into_iter()
            .filter(|fact| {
                query
                    .authorized_fact_ids
                    .iter()
                    .any(|id| id == fact.id.as_str())
                    || (fact
                        .scope_key
                        .as_ref()
                        .is_some_and(|scope| query.authorized_scope_keys.contains(scope))
                        && query
                            .authorized_boundaries
                            .iter()
                            .any(|boundary| boundary == fact.boundary.as_str()))
            })
            .filter(|fact| {
                query.terms.is_empty()
                    || query
                        .terms
                        .iter()
                        .any(|term| fact.statement.to_lowercase().contains(term))
            })
            .collect::<Vec<_>>();
        facts.sort_by(|left, right| {
            right
                .confidence
                .basis_points()
                .cmp(&left.confidence.basis_points())
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        facts.truncate(query.limit);
        Ok(facts)
    }

    fn upsert_evidence(&self, evidence: EvidencePacket) -> FactLedgerResult<EvidencePacket> {
        let mut state = self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        if let Some(existing) = state
            .evidence
            .iter_mut()
            .find(|item| item.id == evidence.id)
        {
            *existing = evidence.clone();
        } else {
            state.evidence.push(evidence.clone());
        }
        Ok(evidence)
    }

    fn get_evidence(&self, evidence_id: &str) -> FactLedgerResult<Option<EvidencePacket>> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .evidence
            .iter()
            .find(|item| item.id.as_str() == evidence_id)
            .cloned())
    }

    fn list_evidence(&self) -> FactLedgerResult<Vec<EvidencePacket>> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .evidence
            .clone())
    }

    fn record_growth_event(&self, event: GrowthEvent) -> FactLedgerResult<()> {
        let mut state = self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        if let Some(existing) = state
            .growth_events
            .iter_mut()
            .find(|item| item.id == event.id)
        {
            *existing = event;
        } else {
            state.growth_events.push(event);
        }
        Ok(())
    }

    fn list_growth_events(&self) -> FactLedgerResult<Vec<GrowthEvent>> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .growth_events
            .clone())
    }

    fn record_growth_promotion(&self, record: GrowthPromotionRecord) -> FactLedgerResult<()> {
        let mut state = self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?;
        if let Some(existing) = state
            .growth_promotions
            .iter_mut()
            .find(|item| item.id == record.id)
        {
            *existing = record;
        } else {
            state.growth_promotions.push(record);
        }
        Ok(())
    }

    fn list_growth_promotions(&self) -> FactLedgerResult<Vec<GrowthPromotionRecord>> {
        Ok(self
            .snapshot
            .lock()
            .map_err(|error| FactLedgerError::backend(error.to_string()))?
            .growth_promotions
            .clone())
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
