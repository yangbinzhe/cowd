use std::path::Path;

use ai_kernel::growth::{GrowthEvent, GrowthMatrixSignal, GrowthMemoryCandidate};
use chrono::Utc;
use fact_kernel::{
    core::{EvidencePacket, FactSource, SourceKind},
    growth::GrowthCandidate,
    hypothesis::HypothesisBoundary,
    matrix::MatrixFact as KernelMatrixFact,
    memory::MemoryCandidate as KernelMemoryCandidate,
    Confidence, FactId,
};
use matrix_core::{MatrixFact, MatrixFactInput};
use memory::{
    project_scope::MemoryScope,
    types::{AgentVisibility, MemoryCategory, MemoryEntry, MemoryLayer, MemorySource, Priority},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use storage::{MigrationRunner, SqliteConnectionFactory, StorageMigrationSpec, StorageRegistry};

use super::{GrowthService, MatrixService, MemoryService};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GrowthPromotionReceipt {
    pub(crate) target: String,
    pub(crate) status: String,
    pub(crate) target_id: Option<String>,
    pub(crate) summary: String,
    pub(crate) error: Option<String>,
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
    pub(crate) async fn ingest_growth_event(
        &self,
        config_home: impl AsRef<Path>,
        memory: &MemoryService,
        matrix: &MatrixService,
        event: GrowthEvent,
    ) -> GrowthIngestReceipt {
        self.record_event(event.clone());
        let mut errors = Vec::new();
        let durable = match self.persist_event(config_home.as_ref(), &event) {
            Ok(()) => true,
            Err(error) => {
                errors.push(error);
                false
            }
        };

        let mut promotions = Vec::new();
        promotions.extend(self.promote_event_to_fact_kernel(&event));
        promotions.extend(self.promote_event_to_matrix(config_home.as_ref(), matrix, &event));
        promotions.extend(self.promote_event_to_memory(memory, &event).await);

        for promotion in &promotions {
            if let Err(error) = self.persist_promotion(config_home.as_ref(), &event.id, promotion) {
                errors.push(error);
            }
        }

        let fact_health_issues = self
            .fact_kernel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evaluate_health();

        GrowthIngestReceipt {
            event_id: event.id,
            durable,
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
        receipt: &ai_kernel::policy::RiskGateReceipt,
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

    pub(crate) fn durable_event_log(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<Vec<GrowthEvent>, String> {
        let conn = open_growth_store(config_home.as_ref())?;
        let mut statement = conn
            .prepare("SELECT payload FROM growth_events ORDER BY created_at DESC, event_id DESC")
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| {
                let payload: String = row.get(0)?;
                Ok(payload)
            })
            .map_err(|error| error.to_string())?;
        let mut events = Vec::new();
        for row in rows {
            let payload = row.map_err(|error| error.to_string())?;
            let event =
                serde_json::from_str::<GrowthEvent>(&payload).map_err(|error| error.to_string())?;
            events.push(event);
        }
        Ok(events)
    }

    pub(crate) fn durable_promotion_log(
        &self,
        config_home: impl AsRef<Path>,
    ) -> Result<Vec<GrowthPromotionReceipt>, String> {
        let conn = open_growth_store(config_home.as_ref())?;
        let mut statement = conn
            .prepare("SELECT payload FROM growth_promotions ORDER BY created_at DESC, id DESC")
            .map_err(|error| error.to_string())?;
        let rows = statement
            .query_map([], |row| {
                let payload: String = row.get(0)?;
                Ok(payload)
            })
            .map_err(|error| error.to_string())?;
        let mut promotions = Vec::new();
        for row in rows {
            let payload = row.map_err(|error| error.to_string())?;
            let promotion = serde_json::from_str::<GrowthPromotionReceipt>(&payload)
                .map_err(|error| error.to_string())?;
            promotions.push(promotion);
        }
        Ok(promotions)
    }

    fn build_risk_gate_event(
        &self,
        session_id: impl Into<String>,
        receipt: &ai_kernel::policy::RiskGateReceipt,
    ) -> GrowthEvent {
        let record =
            ai_kernel::growth::LearningRecord::from_input(ai_kernel::growth::GrowthInput {
                selected_mode: if receipt.approval_required {
                    ai_kernel::core::ExecutionMode::HumanConfirm
                } else {
                    ai_kernel::core::ExecutionMode::RiskGate
                },
                complexity: ai_kernel::core::TaskComplexity::Moderate,
                risk: if receipt.approval_required {
                    ai_kernel::core::TaskRisk::High
                } else {
                    ai_kernel::core::TaskRisk::Medium
                },
                context_omitted: 0,
                tool_requires_checkpoint: !matches!(
                    receipt.decision,
                    ai_kernel::policy::PolicyDecisionKind::Allow
                ),
                tool_requires_human_confirm: receipt.approval_required,
                verification_can_finalize: !receipt.approval_required,
                bench_passed: true,
            });
        GrowthEvent::from_input(ai_kernel::growth::GrowthEventInput {
            session_id: session_id.into(),
            source_event_kind: "approval.risk_receipt".to_string(),
            strategy_mode: if receipt.approval_required {
                ai_kernel::core::ExecutionMode::HumanConfirm
            } else {
                ai_kernel::core::ExecutionMode::RiskGate
            },
            learning_record: record,
            evidence_refs: vec![ai_kernel::growth::GrowthEvidenceRef::new(
                "risk_gate_receipt",
                format!("risk:{}", receipt.issued_at.timestamp_millis()),
                format!(
                    "decision={:?} approval_required={}",
                    receipt.decision, receipt.approval_required
                ),
            )],
        })
    }

    fn persist_event(&self, config_home: &Path, event: &GrowthEvent) -> Result<(), String> {
        let conn = open_growth_store(config_home)?;
        let payload = serde_json::to_string(event).map_err(|error| error.to_string())?;
        conn.execute(
            "INSERT OR REPLACE INTO growth_events(event_id, session_id, source_event_kind, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.id,
                event.session_id,
                event.source_event_kind,
                payload,
                Utc::now().to_rfc3339()
            ],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn persist_promotion(
        &self,
        config_home: &Path,
        event_id: &str,
        promotion: &GrowthPromotionReceipt,
    ) -> Result<(), String> {
        let conn = open_growth_store(config_home)?;
        let payload = serde_json::to_string(promotion).map_err(|error| error.to_string())?;
        let stable_id = format!(
            "{}:{}:{}",
            event_id,
            promotion.target,
            promotion
                .target_id
                .clone()
                .unwrap_or_else(|| promotion.summary.clone())
        );
        conn.execute(
            "INSERT OR REPLACE INTO growth_promotions(id, event_id, target, status, target_id, summary, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                stable_id,
                event_id,
                promotion.target,
                promotion.status,
                promotion.target_id,
                promotion.summary,
                payload,
                Utc::now().to_rfc3339()
            ],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn promote_event_to_fact_kernel(&self, event: &GrowthEvent) -> Vec<GrowthPromotionReceipt> {
        let mut kernel = self
            .fact_kernel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let source = growth_fact_source(event);
        let evidence = kernel.ingest_evidence(EvidencePacket::new(
            source.clone(),
            serde_json::json!({
                "event_id": event.id,
                "source_event_kind": event.source_event_kind,
                "signals": event.signals,
                "evidence_refs": event.evidence_refs,
            }),
        ));

        let mut receipts = Vec::new();
        for candidate in &event.memory_candidates {
            let receipt = kernel.promote_candidate(GrowthCandidate::Memory(
                kernel_memory_candidate(event, candidate, &source, &evidence.id),
            ));
            receipts.push(GrowthPromotionReceipt {
                target: "fact.memory".to_string(),
                status: format!("{:?}", receipt.decision.decision).to_ascii_lowercase(),
                target_id: receipt
                    .promoted_fact
                    .as_ref()
                    .map(|fact| fact.id.as_str().to_string()),
                summary: receipt.decision.reason,
                error: None,
            });
        }
        for signal in &event.matrix_signals {
            let receipt = kernel.promote_candidate(GrowthCandidate::Matrix(kernel_matrix_fact(
                event,
                signal,
                &source,
                &evidence.id,
            )));
            receipts.push(GrowthPromotionReceipt {
                target: "fact.matrix".to_string(),
                status: format!("{:?}", receipt.decision.decision).to_ascii_lowercase(),
                target_id: receipt
                    .promoted_fact
                    .as_ref()
                    .map(|fact| fact.id.as_str().to_string()),
                summary: receipt.decision.reason,
                error: None,
            });
        }
        receipts
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

fn open_growth_store(config_home: &Path) -> Result<Connection, String> {
    let registry = StorageRegistry::default_for_config_home(config_home);
    let handle = registry
        .sqlite_handle("growth")
        .map_err(|error| error.to_string())?;
    if let Some(parent) = handle.path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let conn = SqliteConnectionFactory::default()
        .open_handle(handle)
        .map_err(|error| error.to_string())?;
    let migration_reports =
        MigrationRunner::run_sqlite_domain(&conn, handle, &growth_storage_migrations())
            .map_err(|error| error.to_string())?;
    if let Some(failed) = migration_reports
        .iter()
        .find(|report| report.status == "failed")
    {
        return Err(format!(
            "growth storage migration {} failed: {}",
            failed.id,
            failed
                .error
                .clone()
                .unwrap_or_else(|| "unknown".to_string())
        ));
    }
    Ok(conn)
}

pub(crate) fn growth_storage_migrations() -> Vec<StorageMigrationSpec> {
    vec![StorageMigrationSpec {
        id: "growth.v1.init",
        domain: "growth",
        version: 1,
        description: "initialize growth durable event and promotion schema",
        statements: &[
            "CREATE TABLE IF NOT EXISTS growth_events (
                event_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                source_event_kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );",
            "CREATE TABLE IF NOT EXISTS growth_promotions (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                target TEXT NOT NULL,
                status TEXT NOT NULL,
                target_id TEXT,
                summary TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );",
        ],
    }]
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
        format!("{:?}", event.strategy_mode).to_ascii_lowercase(),
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
                normalize_memory_text(&reference.kind),
                normalize_memory_text(&reference.reference),
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
        MemoryScope::Agent(value) => format!("agent:{value}"),
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

#[allow(dead_code)]
fn event_exists(config_home: &Path, event_id: &str) -> Result<bool, String> {
    let conn = open_growth_store(config_home)?;
    let found = conn
        .query_row(
            "SELECT event_id FROM growth_events WHERE event_id = ?1",
            params![event_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| error.to_string())?
        .is_some();
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ai_kernel::{
        core::{ExecutionMode, TaskComplexity, TaskRisk},
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
            selected_mode: ExecutionMode::PlanExecute,
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
            source_event_kind: "runtime.ai_kernel.trace".to_string(),
            strategy_mode: ExecutionMode::PlanExecute,
            learning_record: record,
            evidence_refs: vec![GrowthEvidenceRef::new(
                "runtime_trace",
                "trace-1",
                "blocked verification",
            )],
        })
    }

    #[test]
    fn growth_store_is_opened_from_storage_registry() {
        let config_home = std::env::temp_dir().join(format!(
            "cowd-growth-storage-registry-test-{}",
            uuid::Uuid::new_v4()
        ));
        let conn = open_growth_store(&config_home).expect("growth store should open");
        let path = StorageRegistry::default_for_config_home(&config_home)
            .sqlite_handle("growth")
            .expect("growth handle")
            .path
            .clone();
        assert!(path.ends_with("storage/growth.sqlite"));
        assert!(path.exists());
        let table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('growth_events', 'growth_promotions')",
                [],
                |row| row.get(0),
            )
            .expect("table count");
        assert_eq!(table_count, 2);
        let migration_id: String = conn
            .query_row(
                "SELECT id FROM schema_migrations WHERE id = 'growth.v1.init'",
                [],
                |row| row.get(0),
            )
            .expect("growth migration should be recorded");
        assert_eq!(migration_id, "growth.v1.init");
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
        let growth = GrowthService::new();
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
