//! Runtime-owned conflict arbitration receipts.

use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

use crate::{
    global_mission_evidence_bus, record_runtime_event, MissionEvidenceRef, RuntimeEventInput,
    RuntimeEventRef, RuntimeEventScope,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictSourceKind {
    AgentReturn,
    WorkGraph,
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

#[derive(Debug, Default)]
pub struct ConflictArbiter {
    receipts: Mutex<Vec<ConflictResolutionReceipt>>,
}

impl ConflictArbiter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn resolve(&self, request: ConflictResolutionRequest) -> ConflictResolutionReceipt {
        let created_at_ms = now_ms();
        let conflict_id = format!("conflict-{}", uuid::Uuid::new_v4());
        let decision = decision_for(request.severity);
        let mission_evidence = global_mission_evidence_bus().record(MissionEvidenceRef {
            evidence_id: String::new(),
            mission_id: Some("mission-control".to_string()),
            session_id: first_scope_with_prefix(&request.affected_scope, "session:")
                .unwrap_or_else(|| "mission-control".to_string()),
            team_id: first_scope_with_prefix(&request.affected_scope, "team:"),
            agent_id: first_scope_with_prefix(&request.affected_scope, "agent:"),
            kind: "conflict".to_string(),
            summary: request.summary.clone(),
            source_ref: Some(conflict_id.clone()),
            created_at_ms: 0,
        });
        let receipt = ConflictResolutionReceipt {
            conflict_id,
            source: request.source,
            severity: request.severity,
            decision,
            summary: request.summary,
            evidence_refs: request.evidence_refs,
            affected_scope: request.affected_scope,
            mission_evidence,
            created_at_ms,
        };
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(receipt.clone());
        record_conflict_event(&receipt);
        receipt
    }

    #[must_use]
    pub fn receipts(&self) -> Vec<ConflictResolutionReceipt> {
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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

pub fn global_conflict_arbiter() -> &'static ConflictArbiter {
    static ARBITER: OnceLock<ConflictArbiter> = OnceLock::new();
    ARBITER.get_or_init(ConflictArbiter::new)
}

fn decision_for(severity: ConflictSeverity) -> ConflictDecisionKind {
    match severity {
        ConflictSeverity::Low => ConflictDecisionKind::ContinueWithRecord,
        ConflictSeverity::Medium => ConflictDecisionKind::RequestReview,
        ConflictSeverity::High => ConflictDecisionKind::PauseAffectedScope,
        ConflictSeverity::Critical => ConflictDecisionKind::RequireApproval,
    }
}

fn record_conflict_event(receipt: &ConflictResolutionReceipt) {
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
    let _ = record_runtime_event(RuntimeEventInput {
        stream_id: format!("conflict:{}", receipt.conflict_id),
        scope: RuntimeEventScope::Mission,
        kind: "runtime.conflict.resolved".to_string(),
        status: Some(format!("{:?}", receipt.decision).to_ascii_lowercase()),
        actor: Some("conflict_arbiter".to_string()),
        refs,
        payload: serde_json::json!(receipt),
    });
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

    #[test]
    fn conflict_arbiter_records_receipt_event_and_evidence() {
        let receipt = global_conflict_arbiter().resolve(ConflictResolutionRequest {
            source: ConflictSourceKind::WorkGraph,
            severity: ConflictSeverity::High,
            summary: "downstream node blocked".to_string(),
            evidence_refs: vec!["workgraph:test".to_string()],
            affected_scope: vec!["session:s1".to_string(), "team:t1".to_string()],
        });

        assert_eq!(receipt.decision, ConflictDecisionKind::PauseAffectedScope);
        assert_eq!(receipt.mission_evidence.kind, "conflict");
        assert!(global_conflict_arbiter()
            .receipts()
            .iter()
            .any(|item| item.conflict_id == receipt.conflict_id));
    }
}
