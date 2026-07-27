//! Runtime-owned conflict arbitration receipts.

use std::sync::Arc;

use harness_contract::reality::EvidenceRef;
use serde::{Deserialize, Serialize};

use crate::{
    MissionEvidenceBus, MissionEvidenceRef, RuntimeEventInput, RuntimeEventRef, RuntimeEventScope,
    RuntimeEventStore, RuntimeTransactionEventInput,
};

const CONFLICT_EVENT_KIND: &str = "runtime.conflict.resolved.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictSourceKind {
    AgentReturn,
    ExecutionGraph,
    SessionRelation,
    Tool,
    MemoryFact,
    Approval,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictDecisionKind {
    ContinueWithRecord,
    RequestReview,
    PauseAffectedScope,
    RequireApproval,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictResolutionRequest {
    pub source: ConflictSourceKind,
    pub severity: ConflictSeverity,
    pub summary: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub affected_scope: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictResolutionReceipt {
    pub conflict_id: String,
    pub source: ConflictSourceKind,
    pub severity: ConflictSeverity,
    pub decision: ConflictDecisionKind,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub affected_scope: Vec<String>,
    pub mission_evidence: MissionEvidenceRef,
    pub created_at_ms: u64,
}

#[derive(Debug)]
pub struct ConflictArbiter {
    evidence_bus: Arc<MissionEvidenceBus>,
    event_store: Arc<RuntimeEventStore>,
}

impl ConflictArbiter {
    #[must_use]
    pub fn new(evidence_bus: Arc<MissionEvidenceBus>, event_store: Arc<RuntimeEventStore>) -> Self {
        Self {
            evidence_bus,
            event_store,
        }
    }

    pub fn resolve(
        &self,
        request: ConflictResolutionRequest,
    ) -> Result<ConflictResolutionReceipt, String> {
        let created_at_ms = now_ms();
        let conflict_id = format!("conflict-{}", uuid::Uuid::new_v4());
        let evidence_id = format!("mission-evidence-{}", uuid::Uuid::new_v4());
        let decision = decision_for(request.severity);
        let mission_evidence = MissionEvidenceRef {
            evidence: EvidenceRef::conflict("runtime_conflict", evidence_id)
                .with_source("runtime.conflict_arbiter"),
            mission_id: Some("mission-control".to_string()),
            session_id: first_scope_with_prefix(&request.affected_scope, "session:")
                .unwrap_or_else(|| "mission-control".to_string()),
            team_id: first_scope_with_prefix(&request.affected_scope, "team:"),
            agent_id: first_scope_with_prefix(&request.affected_scope, "agent:"),
            kind: "conflict".to_string(),
            summary: request.summary.clone(),
            source_ref: Some(conflict_id.clone()),
            created_at_ms,
        };
        let receipt = ConflictResolutionReceipt {
            conflict_id: conflict_id.clone(),
            source: request.source,
            severity: request.severity,
            decision,
            summary: request.summary,
            evidence_refs: request.evidence_refs,
            affected_scope: request.affected_scope,
            mission_evidence: mission_evidence.clone(),
            created_at_ms,
        };
        self.evidence_bus.record_with_related_event(
            mission_evidence,
            conflict_event(&receipt)?,
            format!("runtime-conflict:{conflict_id}"),
        )?;
        Ok(receipt)
    }

    #[must_use]
    pub fn receipts(&self) -> Vec<ConflictResolutionReceipt> {
        self.event_store
            .all_events(10_000)
            .unwrap_or_default()
            .into_iter()
            .filter(|event| event.kind == CONFLICT_EVENT_KIND)
            .filter_map(|event| serde_json::from_value(event.payload).ok())
            .collect()
    }

    #[must_use]
    pub fn projection(&self) -> serde_json::Value {
        let receipts = self.receipts();
        serde_json::json!({
            "kind": "runtime.conflicts",
            "count": receipts.len(),
            "receipts": receipts,
        })
    }
}

fn decision_for(severity: ConflictSeverity) -> ConflictDecisionKind {
    match severity {
        ConflictSeverity::Low => ConflictDecisionKind::ContinueWithRecord,
        ConflictSeverity::Medium => ConflictDecisionKind::RequestReview,
        ConflictSeverity::High => ConflictDecisionKind::PauseAffectedScope,
        ConflictSeverity::Critical => ConflictDecisionKind::RequireApproval,
    }
}

fn conflict_event(
    receipt: &ConflictResolutionReceipt,
) -> Result<RuntimeTransactionEventInput, String> {
    let refs = receipt
        .affected_scope
        .iter()
        .filter_map(|scope| {
            let (kind, id) = scope.split_once(':')?;
            Some(RuntimeEventRef {
                kind: kind.to_string(),
                id: id.to_string(),
            })
        })
        .collect::<Vec<_>>();
    Ok(RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: format!("conflict:{}", receipt.conflict_id),
            scope: RuntimeEventScope::Mission,
            kind: CONFLICT_EVENT_KIND.to_string(),
            status: Some(format!("{:?}", receipt.decision).to_ascii_lowercase()),
            actor: Some("runtime.conflict_arbiter".to_string()),
            refs,
            payload: serde_json::to_value(receipt).map_err(|error| error.to_string())?,
        },
        idempotency_key: Some(format!("conflict:{}", receipt.conflict_id)),
        schema_version: 1,
    })
}

fn first_scope_with_prefix(scopes: &[String], prefix: &str) -> Option<String> {
    scopes
        .iter()
        .find_map(|scope| scope.strip_prefix(prefix).map(str::to_string))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arbiter() -> ConflictArbiter {
        let event_store =
            Arc::new(RuntimeEventStore::try_open_in_memory().expect("runtime event store"));
        let evidence_bus = Arc::new(MissionEvidenceBus::new(Arc::clone(&event_store)));
        ConflictArbiter::new(evidence_bus, event_store)
    }

    #[test]
    fn conflict_arbiter_commits_receipt_and_evidence_atomically() {
        let arbiter = arbiter();
        let receipt = arbiter
            .resolve(ConflictResolutionRequest {
                source: ConflictSourceKind::ExecutionGraph,
                severity: ConflictSeverity::High,
                summary: "downstream node blocked".to_string(),
                evidence_refs: vec!["execution_graph:test".to_string()],
                affected_scope: vec!["session:s1".to_string(), "team:t1".to_string()],
            })
            .unwrap();

        assert_eq!(receipt.decision, ConflictDecisionKind::PauseAffectedScope);
        assert_eq!(receipt.mission_evidence.kind, "conflict");
        assert!(arbiter
            .receipts()
            .iter()
            .any(|item| item.conflict_id == receipt.conflict_id));
    }
}
