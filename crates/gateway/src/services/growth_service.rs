use std::path::{Path, PathBuf};

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
        receipts
    }
}

fn open_growth_store(config_home: &Path) -> Result<Connection, String> {
    let path = growth_store_path(config_home);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let conn = Connection::open(path).map_err(|error| error.to_string())?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS growth_events (
            event_id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            source_event_kind TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS growth_promotions (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            target TEXT NOT NULL,
            status TEXT NOT NULL,
            target_id TEXT,
            summary TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL
        );",
    )
    .map_err(|error| error.to_string())?;
    Ok(conn)
}

fn growth_store_path(config_home: &Path) -> PathBuf {
    config_home.join("storage").join("growth.sqlite")
}

fn growth_fact_source(event: &GrowthEvent) -> FactSource {
    FactSource {
        kind: SourceKind::Growth,
        id: event.id.clone(),
        label: Some(event.source_event_kind.clone()),
    }
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
        tags: vec![
            "growth".to_string(),
            event.source_event_kind.clone(),
            format!("{:?}", candidate.kind).to_ascii_lowercase(),
        ],
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
        tags: vec![
            "growth".to_string(),
            event.source_event_kind.clone(),
            format!("{:?}", candidate.kind).to_ascii_lowercase(),
        ],
        relations: Vec::new(),
        confidence: f32::from(candidate.confidence_bp) / 10_000.0,
        access_count: 0,
        staleness: 0.0,
        created_at: now,
        updated_at: now,
        last_accessed_at: None,
        scope: MemoryScope::Session(event.session_id.clone()),
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
