use std::path::Path;

use chrono::Utc;
use fact_kernel::{
    core::{EvidencePacket, FactSource, SourceKind},
    growth::GrowthCandidate,
    hypothesis::HypothesisBoundary,
    matrix::MatrixFact as KernelMatrixFact,
    memory::MemoryCandidate as KernelMemoryCandidate,
    Confidence, EvidenceId, FactGrowthBatch, FactId, FactKernelService, GrowthPromotionRecord,
    InMemoryFactStore,
};
use harness_contract::growth::{GrowthEvent, GrowthMatrixSignal, GrowthMemoryCandidate};
use matrix_core::{MatrixFact, MatrixFactInput};
use memory::{
    project_scope::MemoryScope,
    types::{AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority},
};
use serde::{Deserialize, Serialize};

use super::{GrowthService, MatrixService, MemoryService};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GrowthPromotionReceipt {
    pub(crate) target: String,
    pub(crate) status: String,
    pub(crate) target_id: Option<String>,
    pub(crate) summary: String,
    pub(crate) error: Option<String>,
}

impl From<GrowthPromotionRecord> for GrowthPromotionReceipt {
    fn from(record: GrowthPromotionRecord) -> Self {
        Self {
            target: record.target,
            status: record.status,
            target_id: record.target_id,
            summary: record.summary,
            error: record.error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GrowthIngestReceipt {
    pub(crate) event_id: String,
    pub(crate) durable: bool,
    pub(crate) promotions: Vec<GrowthPromotionReceipt>,
    pub(crate) fact_health_issues: Vec<fact_kernel::health::FactHealthIssue>,
    pub(crate) errors: Vec<String>,
}

impl GrowthService {
    pub(crate) fn fact_evidence(
        &self,
        evidence_id: &str,
    ) -> Result<Option<fact_kernel::EvidencePacket>, String> {
        self.ledger
            .get_evidence(evidence_id)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn recall_facts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<fact_kernel::FactSearchHit>, String> {
        let kernel = self.semantic_kernel()?;
        let mut recall_query = fact_kernel::memory::RecallQuery::new(query);
        recall_query.limit = limit.max(1);
        Ok(kernel.recall(&recall_query))
    }

    pub(crate) fn list_fact_records(&self) -> Result<Vec<fact_kernel::FactRecord>, String> {
        self.ledger.list_facts().map_err(|error| error.to_string())
    }

    pub(crate) async fn ingest_growth_event(
        &self,
        config_home: impl AsRef<Path>,
        memory: &MemoryService,
        matrix: &MatrixService,
        event: GrowthEvent,
    ) -> GrowthIngestReceipt {
        let mut errors = Vec::new();
        let mut promotions = match self.promote_event_to_fact_kernel(&event) {
            Ok(receipts) => receipts,
            Err(error) => {
                return GrowthIngestReceipt {
                    event_id: event.id,
                    durable: false,
                    promotions: Vec::new(),
                    fact_health_issues: Vec::new(),
                    errors: vec![error],
                };
            }
        };
        let fact_promotion_count = promotions.len();
        promotions.extend(self.promote_event_to_matrix(config_home.as_ref(), matrix, &event));
        promotions.extend(self.promote_event_to_memory(memory, &event).await);

        for promotion in promotions.iter().skip(fact_promotion_count) {
            if let Err(error) = self.persist_promotion(&event.id, promotion) {
                errors.push(error);
            }
        }

        let fact_health_issues = match self.semantic_kernel() {
            Ok(kernel) => kernel.evaluate_health(),
            Err(error) => {
                errors.push(error);
                Vec::new()
            }
        };

        GrowthIngestReceipt {
            event_id: event.id,
            durable: true,
            promotions,
            fact_health_issues,
            errors,
        }
    }

    pub(crate) async fn ingest_risk_gate_receipt(
        &self,
        config_home: impl AsRef<Path>,
        memory: &MemoryService,
        matrix: &MatrixService,
        session_id: impl Into<String>,
        receipt: &harness_contract::policy::RiskGateReceipt,
    ) -> serde_json::Value {
        let event = self.build_risk_gate_event(session_id, receipt);
        let ingest = self
            .ingest_growth_event(config_home, memory, matrix, event.clone())
            .await;
        serde_json::json!({
            "envelope": self.envelope("risk_gate_event"),
            "event": event,
            "ingest": ingest,
        })
    }

    pub(crate) fn durable_event_log(&self) -> Result<Vec<GrowthEvent>, String> {
        self.ledger
            .list_growth_events()
            .map_err(|error| error.to_string())
    }

    pub(crate) fn durable_promotion_log(&self) -> Result<Vec<GrowthPromotionReceipt>, String> {
        self.ledger
            .list_growth_promotions()
            .map(|records| {
                records
                    .into_iter()
                    .map(GrowthPromotionReceipt::from)
                    .collect()
            })
            .map_err(|error| error.to_string())
    }

    fn build_risk_gate_event(
        &self,
        session_id: impl Into<String>,
        receipt: &harness_contract::policy::RiskGateReceipt,
    ) -> GrowthEvent {
        let record = harness_contract::growth::LearningRecord::from_input(
            harness_contract::growth::GrowthInput {
                selected_pattern: if receipt.approval_required {
                    harness_contract::core::ExecutionPattern::Execute
                } else {
                    harness_contract::core::ExecutionPattern::Execute
                },
                complexity: harness_contract::core::TaskComplexity::Moderate,
                risk: if receipt.approval_required {
                    harness_contract::core::TaskRisk::High
                } else {
                    harness_contract::core::TaskRisk::Medium
                },
                context_omitted: 0,
                tool_requires_checkpoint: !matches!(
                    receipt.decision,
                    harness_contract::policy::PolicyDecisionKind::Allow
                ),
                tool_requires_human_confirm: receipt.approval_required,
                verification_can_finalize: !receipt.approval_required,
                bench_passed: true,
            },
        );
        GrowthEvent::from_input(harness_contract::growth::GrowthEventInput {
            session_id: session_id.into(),
            source_event_kind: "approval.risk_receipt".to_string(),
            strategy_pattern: if receipt.approval_required {
                harness_contract::core::ExecutionPattern::Execute
            } else {
                harness_contract::core::ExecutionPattern::Execute
            },
            learning_record: record,
            evidence_refs: vec![harness_contract::growth::GrowthEvidenceRef::new(
                "risk_gate_receipt",
                format!("risk:{}", receipt.issued_at.timestamp_millis()),
                format!(
                    "decision={:?} approval_required={}",
                    receipt.decision, receipt.approval_required
                ),
            )],
        })
    }

    fn persist_promotion(
        &self,
        event_id: &str,
        promotion: &GrowthPromotionReceipt,
    ) -> Result<(), String> {
        self.ledger
            .record_growth_promotion(GrowthPromotionRecord {
                id: GrowthPromotionRecord::stable_id(
                    event_id,
                    &promotion.target,
                    promotion.target_id.as_deref(),
                    &promotion.summary,
                ),
                event_id: event_id.to_string(),
                target: promotion.target.clone(),
                status: promotion.status.clone(),
                target_id: promotion.target_id.clone(),
                summary: promotion.summary.clone(),
                error: promotion.error.clone(),
                created_at: Utc::now().to_rfc3339(),
            })
            .map_err(|error| error.to_string())
    }

    fn promote_event_to_fact_kernel(
        &self,
        event: &GrowthEvent,
    ) -> Result<Vec<GrowthPromotionReceipt>, String> {
        let mut kernel = self.semantic_kernel()?;
        let source = growth_fact_source(event);
        let mut evidence = EvidencePacket::new(
            source.clone(),
            serde_json::json!({
                "event_id": event.id,
                "source_event_kind": event.source_event_kind,
                "signals": event.signals,
                "evidence_refs": event.evidence_refs,
            }),
        );
        evidence.id = EvidenceId::from_string(format!("growth:evidence:{}", event.id));
        kernel.ingest_evidence(evidence.clone());

        let mut receipts = Vec::new();
        let mut facts = Vec::new();
        for candidate in &event.memory_candidates {
            let (receipt, fact) = self.plan_fact_candidate(
                &mut kernel,
                "fact.memory",
                GrowthCandidate::Memory(kernel_memory_candidate(
                    event,
                    candidate,
                    &source,
                    &evidence.id,
                )),
                Some(format!("growth:{}:fact.memory:{}", event.id, candidate.id)),
            );
            receipts.push(receipt);
            facts.extend(fact);
        }
        for signal in &event.matrix_signals {
            let (receipt, fact) = self.plan_fact_candidate(
                &mut kernel,
                "fact.matrix",
                GrowthCandidate::Matrix(kernel_matrix_fact(event, signal, &source, &evidence.id)),
                None,
            );
            receipts.push(receipt);
            facts.extend(fact);
        }
        let promotion_records = receipts
            .iter()
            .map(|receipt| self.promotion_record(event, receipt))
            .collect();
        self.ledger
            .persist_growth_fact_batch(FactGrowthBatch {
                event: event.clone(),
                evidence,
                facts,
                promotions: promotion_records,
            })
            .map_err(|error| error.to_string())?;
        Ok(receipts)
    }

    fn semantic_kernel(&self) -> Result<FactKernelService<InMemoryFactStore>, String> {
        let snapshot = self
            .ledger
            .export_snapshot()
            .map_err(|error| error.to_string())?;
        Ok(FactKernelService::from_durable_records(
            snapshot.facts,
            snapshot.evidence,
        ))
    }

    fn plan_fact_candidate(
        &self,
        kernel: &mut FactKernelService<InMemoryFactStore>,
        target: &str,
        candidate: GrowthCandidate,
        deterministic_fact_id: Option<String>,
    ) -> (GrowthPromotionReceipt, Option<fact_kernel::FactRecord>) {
        let mut receipt = kernel.plan_candidate_promotion(candidate);
        let mut fact = receipt.promoted_fact.take();
        if let (Some(fact), Some(id)) = (&mut fact, deterministic_fact_id) {
            fact.id = FactId::from_string(id);
        }
        if let Some(fact) = &fact {
            kernel.upsert_fact(fact.clone());
        }
        (
            GrowthPromotionReceipt {
                target: target.to_string(),
                status: format!("{:?}", receipt.decision.decision).to_ascii_lowercase(),
                target_id: fact.as_ref().map(|fact| fact.id.as_str().to_string()),
                summary: receipt.decision.reason,
                error: None,
            },
            fact,
        )
    }

    fn promotion_record(
        &self,
        event: &GrowthEvent,
        promotion: &GrowthPromotionReceipt,
    ) -> GrowthPromotionRecord {
        GrowthPromotionRecord {
            id: GrowthPromotionRecord::stable_id(
                &event.id,
                &promotion.target,
                promotion.target_id.as_deref(),
                &promotion.summary,
            ),
            event_id: event.id.clone(),
            target: promotion.target.clone(),
            status: promotion.status.clone(),
            target_id: promotion.target_id.clone(),
            summary: promotion.summary.clone(),
            error: promotion.error.clone(),
            created_at: Utc::now().to_rfc3339(),
        }
    }

    fn promote_event_to_matrix(
        &self,
        config_home: &Path,
        matrix: &MatrixService,
        event: &GrowthEvent,
    ) -> Vec<GrowthPromotionReceipt> {
        event
            .matrix_signals
            .iter()
            .map(|signal| {
                let fact = matrix_fact_from_signal(event, signal);
                match matrix.ingest_fact(config_home, &fact) {
                    Ok(attention) => GrowthPromotionReceipt {
                        target: "matrix.fact".to_string(),
                        status: "promoted".to_string(),
                        target_id: Some(fact.fact_id),
                        summary: format!("attention={}", attention.attention_id),
                        error: None,
                    },
                    Err(error) => GrowthPromotionReceipt {
                        target: "matrix.fact".to_string(),
                        status: "held".to_string(),
                        target_id: Some(fact.fact_id),
                        summary: "matrix ingest failed".to_string(),
                        error: Some(error.to_string()),
                    },
                }
            })
            .collect()
    }

    async fn promote_event_to_memory(
        &self,
        memory: &MemoryService,
        event: &GrowthEvent,
    ) -> Vec<GrowthPromotionReceipt> {
        let mut receipts = Vec::new();
        for candidate in &event.memory_candidates {
            let entry = memory_entry_from_candidate(event, candidate);
            let decision = memory_promotion_decision(memory, event, candidate, &entry).await;
            match decision {
                MemoryPromotionDecision::Promote => {
                    let target_id = entry.id.to_string();
                    match memory
                        .remember_entry_with_context(entry, &event.session_id, "growth")
                        .await
                    {
                        Ok(()) => receipts.push(GrowthPromotionReceipt {
                            target: "memory.entry".to_string(),
                            status: "promoted".to_string(),
                            target_id: Some(target_id),
                            summary: candidate.summary.clone(),
                            error: None,
                        }),
                        Err(error) => receipts.push(GrowthPromotionReceipt {
                            target: "memory.entry".to_string(),
                            status: "held".to_string(),
                            target_id: Some(target_id),
                            summary: "memory promotion unavailable".to_string(),
                            error: Some(error),
                        }),
                    }
                }
                MemoryPromotionDecision::Refresh { existing_id } => {
                    let updated_content = entry.content.clone();
                    let updated_tags = merge_growth_tags(entry.tags.clone());
                    match memory
                        .update_entry(
                            &existing_id,
                            Some(updated_content),
                            Some(updated_tags),
                            Some(entry.priority),
                        )
                        .await
                    {
                        Ok(()) => receipts.push(GrowthPromotionReceipt {
                            target: "memory.entry".to_string(),
                            status: "refreshed".to_string(),
                            target_id: Some(existing_id),
                            summary: "existing stale growth memory refreshed".to_string(),
                            error: None,
                        }),
                        Err(error) => receipts.push(GrowthPromotionReceipt {
                            target: "memory.entry".to_string(),
                            status: "held".to_string(),
                            target_id: Some(existing_id),
                            summary: "memory refresh unavailable".to_string(),
                            error: Some(error),
                        }),
                    }
                }
                MemoryPromotionDecision::Duplicate { existing_id } => {
                    receipts.push(GrowthPromotionReceipt {
                        target: "memory.entry".to_string(),
                        status: "duplicate".to_string(),
                        target_id: Some(existing_id),
                        summary: "duplicate growth memory suppressed".to_string(),
                        error: None,
                    });
                }
                MemoryPromotionDecision::Conflict {
                    existing_id,
                    reason,
                } => {
                    receipts.push(GrowthPromotionReceipt {
                        target: "memory.entry".to_string(),
                        status: "conflict_held".to_string(),
                        target_id: Some(existing_id),
                        summary: reason,
                        error: None,
                    });
                }
                MemoryPromotionDecision::Hold { reason } => {
                    receipts.push(GrowthPromotionReceipt {
                        target: "memory.entry".to_string(),
                        status: "held".to_string(),
                        target_id: Some(entry.id.to_string()),
                        summary: reason,
                        error: None,
                    });
                }
            }
        }
        receipts
    }
}

fn growth_fact_source(event: &GrowthEvent) -> FactSource {
    FactSource {
        kind: SourceKind::Growth,
        id: event.id.clone(),
        label: Some(event.source_event_kind.clone()),
    }
}

enum MemoryPromotionDecision {
    Promote,
    Refresh { existing_id: String },
    Duplicate { existing_id: String },
    Conflict { existing_id: String, reason: String },
    Hold { reason: String },
}

async fn memory_promotion_decision(
    memory: &MemoryService,
    event: &GrowthEvent,
    candidate: &GrowthMemoryCandidate,
    entry: &MemoryEntry,
) -> MemoryPromotionDecision {
    const MIN_MEMORY_CONFIDENCE_BP: u16 = 7_000;
    const STALE_REFRESH_THRESHOLD: f32 = 0.80;

    if candidate.confidence_bp < MIN_MEMORY_CONFIDENCE_BP {
        return MemoryPromotionDecision::Hold {
            reason: format!(
                "memory candidate confidence {} below threshold {}",
                candidate.confidence_bp, MIN_MEMORY_CONFIDENCE_BP
            ),
        };
    }

    let existing_entries = match memory.list_all_entries().await {
        Ok(entries) => entries,
        Err(error) => {
            return MemoryPromotionDecision::Hold {
                reason: format!("memory governance unavailable: {error}"),
            };
        }
    };

    let scope_key = memory_scope_key(&entry.scope);
    let slot_key = growth_memory_slot_key(event, candidate, &entry.scope);
    let assertion_key = growth_memory_assertion_fingerprint(event, candidate);
    let candidate_text =
        normalize_memory_text(&format!("{} {}", candidate.summary, candidate.reason));

    for existing in existing_entries.iter().filter(|existing| {
        existing.source_agent.as_deref() == Some("growth-service")
            && memory_scope_key(&existing.scope) == scope_key
    }) {
        let existing_text = normalize_memory_text(&existing.content);
        let same_assertion = existing.tags.iter().any(|tag| tag == &assertion_key)
            || legacy_same_text(&existing_text, &candidate_text);
        let same_slot = existing.tags.iter().any(|tag| tag == &slot_key)
            || legacy_slot_key(existing)
                .as_ref()
                .is_some_and(|legacy| legacy == &growth_memory_legacy_key(event, candidate));

        if same_assertion {
            if existing.staleness >= STALE_REFRESH_THRESHOLD {
                return MemoryPromotionDecision::Refresh {
                    existing_id: existing.id.to_string(),
                };
            }
            return MemoryPromotionDecision::Duplicate {
                existing_id: existing.id.to_string(),
            };
        }

        if same_slot && deterministic_memory_contradiction(&existing_text, &candidate_text) {
            return MemoryPromotionDecision::Conflict {
                existing_id: existing.id.to_string(),
                reason: format!(
                    "growth memory slot `{slot_key}` already has a contradictory assertion"
                ),
            };
        }
    }

    MemoryPromotionDecision::Promote
}

fn merge_growth_tags(mut tags: Vec<String>) -> Vec<String> {
    for tag in ["growth", "growth-refreshed"] {
        if !tags.iter().any(|existing| existing == tag) {
            tags.push(tag.to_string());
        }
    }
    tags.sort();
    tags.dedup();
    tags
}

fn growth_memory_governance_tags(
    event: &GrowthEvent,
    candidate: &GrowthMemoryCandidate,
    scope: &MemoryScope,
) -> Vec<String> {
    vec![
        "growth".to_string(),
        growth_memory_slot_key(event, candidate, scope),
        growth_memory_assertion_fingerprint(event, candidate),
        event.source_event_kind.clone(),
        format!("{:?}", candidate.kind).to_ascii_lowercase(),
    ]
}

fn growth_memory_slot_key(
    event: &GrowthEvent,
    candidate: &GrowthMemoryCandidate,
    scope: &MemoryScope,
) -> String {
    format!(
        "growth-slot:{}:{}:{}:{}",
        event.source_event_kind,
        format!("{:?}", candidate.kind).to_ascii_lowercase(),
        format!("{:?}", event.strategy_pattern).to_ascii_lowercase(),
        memory_scope_key(scope)
    )
}

fn growth_memory_assertion_fingerprint(
    event: &GrowthEvent,
    candidate: &GrowthMemoryCandidate,
) -> String {
    let evidence = event
        .evidence_refs
        .iter()
        .map(|reference| {
            format!(
                "{}:{}:{}",
                normalize_memory_text(reference.kind()),
                normalize_memory_text(reference.reference()),
                normalize_memory_text(&reference.summary)
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    let basis = format!(
        "{}|{}|{}|{}|{}",
        normalize_memory_text(&candidate.summary),
        normalize_memory_text(&candidate.reason),
        normalize_memory_text(&event.source_event_kind),
        format!("{:?}", candidate.kind).to_ascii_lowercase(),
        evidence
    );
    format!("growth-assertion:{:016x}", stable_hash64(&basis))
}

fn growth_memory_legacy_key(event: &GrowthEvent, candidate: &GrowthMemoryCandidate) -> String {
    format!(
        "growth-key:{}:{}",
        event.source_event_kind,
        format!("{:?}", candidate.kind).to_ascii_lowercase()
    )
}

fn legacy_slot_key(entry: &MemoryEntry) -> Option<String> {
    entry
        .tags
        .iter()
        .find(|tag| tag.starts_with("growth-key:"))
        .cloned()
}

fn memory_scope_key(scope: &MemoryScope) -> String {
    match scope {
        MemoryScope::Global => "global".to_string(),
        MemoryScope::Project(value) => format!("project:{value}"),
        MemoryScope::Session(value) => format!("session:{value}"),
        MemoryScope::Task(value) => format!("task:{value}"),
        MemoryScope::AgentDefinitionLineage(value) => format!("agent_definition:{value}"),
        MemoryScope::AgentInstance(value) => format!("agent_instance:{value}"),
        MemoryScope::TeamRun(value) => format!("team_run:{value}"),
        MemoryScope::LegacyUnresolvedAgent(value) => format!("legacy_agent:{value}"),
    }
}

fn legacy_same_text(existing_text: &str, candidate_text: &str) -> bool {
    existing_text.contains(candidate_text) || candidate_text.contains(existing_text)
}

fn deterministic_memory_contradiction(left: &str, right: &str) -> bool {
    const PAIRS: [(&str, &str); 6] = [
        ("should", "should not"),
        ("required", "not required"),
        ("allow", "deny"),
        ("enabled", "disabled"),
        ("auto", "manual"),
        ("approval required", "approval not required"),
    ];
    PAIRS.iter().any(|(positive, negative)| {
        (left.contains(positive) && right.contains(negative))
            || (left.contains(negative) && right.contains(positive))
    })
}

fn stable_hash64(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn normalize_memory_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn kernel_memory_candidate(
    event: &GrowthEvent,
    candidate: &GrowthMemoryCandidate,
    source: &FactSource,
    evidence_id: &fact_kernel::EvidenceId,
) -> KernelMemoryCandidate {
    KernelMemoryCandidate {
        summary: candidate.summary.clone(),
        source: source.clone(),
        evidence: vec![evidence_id.clone()],
        confidence: Confidence::from_basis_points(candidate.confidence_bp),
        boundary: HypothesisBoundary::observed(),
        tags: growth_memory_governance_tags(
            event,
            candidate,
            &MemoryScope::Session(event.session_id.clone()),
        ),
    }
}

fn kernel_matrix_fact(
    event: &GrowthEvent,
    signal: &GrowthMatrixSignal,
    source: &FactSource,
    evidence_id: &fact_kernel::EvidenceId,
) -> KernelMatrixFact {
    KernelMatrixFact {
        id: FactId::from_string(format!("growth-matrix:{}:{}", event.id, signal.fact_type)),
        entity: format!("session:{}", event.session_id),
        predicate: signal.fact_type.clone(),
        value: serde_json::json!({
            "dimensions": signal.dimensions,
            "measures": signal.measures,
        }),
        source: source.clone(),
        evidence: vec![evidence_id.clone()],
        confidence: Confidence::from_basis_points(signal.confidence_bp),
        boundary: HypothesisBoundary::observed(),
    }
}

fn memory_entry_from_candidate(
    event: &GrowthEvent,
    candidate: &GrowthMemoryCandidate,
) -> MemoryEntry {
    let now = Utc::now();
    let scope = MemoryScope::Session(event.session_id.clone());
    MemoryEntry {
        id: uuid::Uuid::new_v4(),
        layer: MemoryLayer::L3,
        category: MemoryCategory::ProjectKnowledge,
        priority: if candidate.confidence_bp >= 9_000 {
            Priority::High
        } else {
            Priority::Normal
        },
        source: MemorySource::AutoExtracted,
        title: format!("Growth learning: {:?}", candidate.kind),
        content: format!(
            "{}\n\nReason: {}\nSource event: {}\nGrowth event: {}",
            candidate.summary, candidate.reason, event.source_event_kind, event.id
        ),
        embedding: None,
        tags: growth_memory_governance_tags(event, candidate, &scope),
        relations: Vec::new(),
        confidence: f32::from(candidate.confidence_bp) / 10_000.0,
        access_count: 0,
        staleness: 0.0,
        created_at: now,
        updated_at: now,
        last_accessed_at: None,
        scope,
        session_id: Some(event.session_id.clone()),
        source_agent: Some("growth-service".to_string()),
        visibility: AgentVisibility::Shared,
    }
}

fn matrix_fact_from_signal(event: &GrowthEvent, signal: &GrowthMatrixSignal) -> MatrixFact {
    MatrixFact::from_input(MatrixFactInput {
        fact_id: Some(format!("growth-matrix:{}:{}", event.id, signal.fact_type)),
        snapshot_id: Some(format!("growth:{}", event.id)),
        fact_type: signal.fact_type.clone(),
        entity_refs: vec![format!("session:{}", event.session_id)],
        metric_key: Some("ai_growth".to_string()),
        dimensions: signal.dimensions.clone(),
        measures: signal.measures.clone(),
        event_time: Some(Utc::now()),
        valid_from: None,
        valid_to: None,
        source_ref: Some(event.id.clone()),
        confidence: Some(f32::from(signal.confidence_bp) / 10_000.0),
        raw_hash: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use harness_contract::{
        core::{ExecutionPattern, TaskComplexity, TaskRisk},
        growth::{GrowthEvent, GrowthEventInput, GrowthEvidenceRef, GrowthInput, LearningRecord},
    };
    use memory::{
        config::{BudgetConfig, StoreConfig},
        CognitiveContextManager, MemoryConfig,
    };

    use super::*;

    fn test_memory_config(sqlite_path: &std::path::Path) -> MemoryConfig {
        MemoryConfig {
            store: StoreConfig {
                sqlite_path: sqlite_path.to_path_buf(),
                blob_dir: sqlite_path.parent().unwrap().join("blobs"),
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 8_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn sample_growth_event(session_id: &str) -> GrowthEvent {
        let record = LearningRecord::from_input(GrowthInput {
            selected_pattern: ExecutionPattern::Execute,
            complexity: TaskComplexity::Complex,
            risk: TaskRisk::Medium,
            context_omitted: 0,
            tool_requires_checkpoint: false,
            tool_requires_human_confirm: false,
            verification_can_finalize: false,
            bench_passed: false,
        });
        GrowthEvent::from_input(GrowthEventInput {
            session_id: session_id.to_string(),
            source_event_kind: "runtime.harness_contract.trace".to_string(),
            strategy_pattern: ExecutionPattern::Execute,
            learning_record: record,
            evidence_refs: vec![GrowthEvidenceRef::new(
                "runtime_trace",
                "trace-1",
                "blocked verification",
            )],
        })
    }

    #[test]
    fn growth_service_opens_a_single_canonical_fact_growth_ledger() {
        let config_home = std::env::temp_dir().join(format!(
            "cowd-growth-storage-registry-test-{}",
            uuid::Uuid::new_v4()
        ));
        let growth = GrowthService::new_for_config_home(&config_home);
        let path = storage::StorageRegistry::default_for_config_home(&config_home)
            .endpoint(&storage::StorageDomainId::Fact)
            .expect("growth endpoint")
            .as_handle()
            .path;
        assert!(path.ends_with("storage/fact.sqlite"));
        assert!(path.exists());
        assert!(growth.durable_event_log().is_ok());
        assert!(growth.durable_promotion_log().is_ok());
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn memory_promotion_governance_suppresses_duplicate_growth_candidates() {
        let (config_home, _manager, memory, matrix, growth) =
            growth_memory_test_services("duplicate").await;
        let event = single_candidate_event(
            "growth-governance-session",
            "approval required for risky tool execution",
            "approval required because risk gate blocked automatic execution",
            9_000,
        );

        let first = growth
            .ingest_growth_event(&config_home, &memory, &matrix, event.clone())
            .await;
        assert!(first
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "promoted"));
        let initial_growth_entries = growth_memory_entry_count(&memory).await;
        assert!(initial_growth_entries > 0);

        let second = growth
            .ingest_growth_event(&config_home, &memory, &matrix, event)
            .await;
        assert!(second
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "duplicate"));
        assert_eq!(
            growth_memory_entry_count(&memory).await,
            initial_growth_entries
        );
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn memory_promotion_governance_holds_low_confidence_candidates() {
        let (config_home, _manager, memory, matrix, growth) =
            growth_memory_test_services("low-confidence").await;
        let event = single_candidate_event(
            "growth-low-confidence-session",
            "weak signal should not enter long term memory",
            "insufficient evidence",
            6_999,
        );

        let receipt = growth
            .ingest_growth_event(&config_home, &memory, &matrix, event)
            .await;
        assert!(receipt.promotions.iter().any(|item| {
            item.target == "memory.entry"
                && item.status == "held"
                && item.summary.contains("below threshold")
        }));
        assert_eq!(growth_memory_entry_count(&memory).await, 0);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn memory_promotion_governance_holds_conflicting_growth_assertions() {
        let (config_home, _manager, memory, matrix, growth) =
            growth_memory_test_services("conflict").await;
        let first_event = single_candidate_event(
            "growth-conflict-session",
            "approval required for risky tool execution",
            "approval required before high risk mutation",
            9_000,
        );
        let second_event = single_candidate_event(
            "growth-conflict-session",
            "approval not required for risky tool execution",
            "approval not required before high risk mutation",
            9_000,
        );

        let first = growth
            .ingest_growth_event(&config_home, &memory, &matrix, first_event)
            .await;
        assert!(first
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "promoted"));
        let initial_growth_entries = growth_memory_entry_count(&memory).await;

        let second = growth
            .ingest_growth_event(&config_home, &memory, &matrix, second_event)
            .await;
        assert!(second
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "conflict_held"));
        assert_eq!(
            growth_memory_entry_count(&memory).await,
            initial_growth_entries
        );
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn memory_promotion_governance_allows_same_slot_non_conflicting_assertions() {
        let (config_home, _manager, memory, matrix, growth) =
            growth_memory_test_services("same-slot").await;
        let first_event = single_candidate_event(
            "growth-same-slot-session",
            "architecture guard should run before gateway boundary changes",
            "gateway boundary changes need architecture evidence",
            9_000,
        );
        let second_event = single_candidate_event(
            "growth-same-slot-session",
            "targeted growth tests should run after memory governance changes",
            "memory governance changes need targeted evidence",
            9_000,
        );

        let first = growth
            .ingest_growth_event(&config_home, &memory, &matrix, first_event)
            .await;
        let second = growth
            .ingest_growth_event(&config_home, &memory, &matrix, second_event)
            .await;
        assert!(first
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "promoted"));
        assert!(second
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "promoted"));
        assert_eq!(growth_memory_entry_count(&memory).await, 2);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn memory_promotion_governance_refreshes_stale_duplicates() {
        let (config_home, manager, memory, matrix, growth) =
            growth_memory_test_services("refresh").await;
        let event = single_candidate_event(
            "growth-refresh-session",
            "stale duplicate should refresh instead of duplicating",
            "same assertion has become stale",
            9_000,
        );

        let first = growth
            .ingest_growth_event(&config_home, &memory, &matrix, event.clone())
            .await;
        assert!(first
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "promoted"));
        let mut entries = memory.list_all_entries().await.expect("memory entries");
        let mut entry = entries
            .iter_mut()
            .find(|entry| entry.source_agent.as_deref() == Some("growth-service"))
            .expect("growth memory entry")
            .clone();
        entry.staleness = 0.90;
        manager
            .orchestrator()
            .update(&entry)
            .await
            .expect("stale entry update");
        let initial_growth_entries = growth_memory_entry_count(&memory).await;

        let second = growth
            .ingest_growth_event(&config_home, &memory, &matrix, event)
            .await;
        assert!(second
            .promotions
            .iter()
            .any(|item| item.target == "memory.entry" && item.status == "refreshed"));
        assert_eq!(
            growth_memory_entry_count(&memory).await,
            initial_growth_entries
        );
        let refreshed = memory
            .list_all_entries()
            .await
            .expect("refreshed memory entries")
            .into_iter()
            .find(|entry| entry.source_agent.as_deref() == Some("growth-service"))
            .expect("refreshed growth memory entry");
        assert!(refreshed.tags.iter().any(|tag| tag == "growth-refreshed"));
        let _ = std::fs::remove_dir_all(config_home);
    }

    async fn growth_memory_test_services(
        label: &str,
    ) -> (
        std::path::PathBuf,
        Arc<CognitiveContextManager>,
        MemoryService,
        MatrixService,
        GrowthService,
    ) {
        let config_home = std::env::temp_dir().join(format!(
            "cowd-growth-memory-governance-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(config_home.join("storage")).expect("storage dir");
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(
                &config_home.join("storage").join("memory.sqlite"),
            ))
            .await
            .expect("memory manager"),
        );
        let memory = MemoryService::with_manager(Some(Arc::clone(&manager)));
        let matrix = MatrixService::new();
        let growth = GrowthService::new_for_config_home(&config_home);
        (config_home, manager, memory, matrix, growth)
    }

    fn single_candidate_event(
        session_id: &str,
        summary: &str,
        reason: &str,
        confidence_bp: u16,
    ) -> GrowthEvent {
        let mut event = sample_growth_event(session_id);
        let mut candidate = event
            .memory_candidates
            .first()
            .expect("sample event should create a memory candidate")
            .clone();
        candidate.summary = summary.to_string();
        candidate.reason = reason.to_string();
        candidate.confidence_bp = confidence_bp;
        event.memory_candidates = vec![candidate];
        event
    }

    async fn growth_memory_entry_count(memory: &MemoryService) -> usize {
        memory
            .list_all_entries()
            .await
            .expect("memory entries")
            .iter()
            .filter(|entry| entry.source_agent.as_deref() == Some("growth-service"))
            .count()
    }
}
