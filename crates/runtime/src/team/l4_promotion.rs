//! Governed promotion of durable Team knowledge into Memory L4.
//!
//! Team execution writes collaboration semantics to `TeamWorkingState` only.
//! This service is the explicit second step that validates a candidate,
//! records its lifecycle, and asks memory to persist the promoted artifact.

use std::sync::Arc;

use memory::{L4PromotionCommand, MemoryScope, Priority};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum L4CandidateLifecycle {
    Proposed,
    Validated,
    Promoted,
    Rejected,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L4PromotionCandidate {
    pub candidate_id: String,
    pub team_id: String,
    pub graph_id: String,
    pub lineage_ref: String,
    pub title: String,
    pub content: String,
    pub scope: MemoryScope,
    pub source_evidence_refs: Vec<String>,
    pub priority: Priority,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L4PromotionReceipt {
    pub candidate_id: String,
    pub promotion_receipt: String,
    pub content_digest: String,
    pub memory_id: String,
    pub lifecycle: L4CandidateLifecycle,
}

#[derive(Clone)]
pub struct L4PromotionService {
    event_store: Arc<RuntimeEventStore>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
}

impl L4PromotionService {
    #[must_use]
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    ) -> Self {
        Self {
            event_store,
            memory_manager,
        }
    }

    pub fn propose(&self, candidate: &L4PromotionCandidate) -> Result<(), String> {
        validate_candidate(candidate)?;
        self.append_lifecycle(candidate, L4CandidateLifecycle::Proposed, None, None)
    }

    pub async fn validate_and_promote(
        &self,
        candidate: L4PromotionCandidate,
    ) -> Result<L4PromotionReceipt, String> {
        validate_candidate(&candidate)?;
        if let Some(receipt) = self.existing_promoted_receipt(&candidate)? {
            return Ok(receipt);
        }
        if self.has_terminal_rejection(&candidate)? {
            return Err(format!(
                "L4 candidate `{}` has a terminal rejected/superseded lifecycle and cannot be promoted",
                candidate.candidate_id
            ));
        }
        self.propose(&candidate)?;
        self.append_lifecycle(&candidate, L4CandidateLifecycle::Validated, None, None)?;
        let content_digest = format!("{:x}", Sha256::digest(candidate.content.as_bytes()));
        let promotion_receipt = format!(
            "l4-promotion:{}:{}",
            candidate.candidate_id,
            &content_digest[..16]
        );
        let manager = self
            .memory_manager
            .as_ref()
            .ok_or_else(|| "L4 promotion requires a configured Memory manager".to_string())?;
        let memory_id = manager
            .orchestrator()
            .promote_l4(L4PromotionCommand {
                candidate_id: candidate.candidate_id.clone(),
                promotion_receipt: promotion_receipt.clone(),
                lineage_ref: candidate.lineage_ref.clone(),
                source_evidence_refs: candidate.source_evidence_refs.clone(),
                scope: candidate.scope.clone(),
                title: candidate.title.clone(),
                content: candidate.content.clone(),
                priority: candidate.priority,
                tags: candidate.tags.clone(),
            })
            .await
            .map_err(|error| error.to_string())?;
        let receipt = L4PromotionReceipt {
            candidate_id: candidate.candidate_id.clone(),
            promotion_receipt,
            content_digest,
            memory_id: memory_id.to_string(),
            lifecycle: L4CandidateLifecycle::Promoted,
        };
        self.append_lifecycle(
            &candidate,
            L4CandidateLifecycle::Promoted,
            Some(&receipt),
            None,
        )?;
        Ok(receipt)
    }

    pub fn reject(&self, candidate: &L4PromotionCandidate, reason: &str) -> Result<(), String> {
        validate_candidate(candidate)?;
        if reason.trim().is_empty() {
            return Err("L4 rejection requires a reason".to_string());
        }
        self.append_lifecycle(
            candidate,
            L4CandidateLifecycle::Rejected,
            None,
            Some(reason),
        )
    }

    /// Read the durable lifecycle for one candidate without exposing the
    /// EventStore to callers. This is the inspectable proof that a promoted
    /// L4 entry passed the governed proposal/validation path.
    pub fn lifecycle(
        &self,
        candidate: &L4PromotionCandidate,
    ) -> Result<Vec<L4CandidateLifecycle>, String> {
        let stream_id = format!("team-l4-candidate:{}", candidate.team_id);
        self.event_store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|event| {
                event.kind == "team.l4_candidate.lifecycle.v1"
                    && event
                        .payload
                        .pointer("/candidate/candidate_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(candidate.candidate_id.as_str())
            })
            .map(|event| {
                serde_json::from_value(
                    event
                        .payload
                        .get("lifecycle")
                        .cloned()
                        .ok_or_else(|| "L4 lifecycle event lacks lifecycle state".to_string())?,
                )
                .map_err(|error| format!("decode L4 lifecycle state: {error}"))
            })
            .collect()
    }

    fn append_lifecycle(
        &self,
        candidate: &L4PromotionCandidate,
        lifecycle: L4CandidateLifecycle,
        receipt: Option<&L4PromotionReceipt>,
        reason: Option<&str>,
    ) -> Result<(), String> {
        let stream_id = format!("team-l4-candidate:{}", candidate.team_id);
        let key = format!("{}:{lifecycle:?}", candidate.candidate_id).to_ascii_lowercase();
        if self
            .event_store
            .event_by_idempotency_key(&stream_id, &key)
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Ok(());
        }
        let revision = self
            .event_store
            .stream_revision(&stream_id)
            .map_err(|error| error.to_string())?;
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("team-l4-candidate:{}:{key}", candidate.candidate_id),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::Team,
                        kind: "team.l4_candidate.lifecycle.v1".to_string(),
                        status: Some(format!("{lifecycle:?}").to_ascii_lowercase()),
                        actor: Some("runtime.l4_promotion_service".to_string()),
                        refs: vec![
                            RuntimeEventRef {
                                kind: "team".to_string(),
                                id: candidate.team_id.clone(),
                            },
                            RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: candidate.graph_id.clone(),
                            },
                        ],
                        payload: serde_json::json!({
                            "candidate": candidate,
                            "lifecycle": lifecycle,
                            "receipt": receipt,
                            "reason": reason,
                        }),
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn existing_promoted_receipt(
        &self,
        candidate: &L4PromotionCandidate,
    ) -> Result<Option<L4PromotionReceipt>, String> {
        let stream_id = format!("team-l4-candidate:{}", candidate.team_id);
        let events = self
            .event_store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?;
        for event in events.into_iter().rev() {
            if event.kind != "team.l4_candidate.lifecycle.v1" {
                continue;
            }
            let Some(event_candidate_id) = event
                .payload
                .pointer("/candidate/candidate_id")
                .and_then(serde_json::Value::as_str)
            else {
                continue;
            };
            if event_candidate_id != candidate.candidate_id {
                continue;
            }
            if event.payload.get("lifecycle")
                == Some(&serde_json::json!(L4CandidateLifecycle::Promoted))
            {
                return serde_json::from_value(event.payload.get("receipt").cloned().ok_or_else(
                    || "promoted L4 candidate is missing its durable receipt".to_string(),
                )?)
                .map(Some)
                .map_err(|error| format!("decode promoted L4 receipt: {error}"));
            }
        }
        Ok(None)
    }

    fn has_terminal_rejection(&self, candidate: &L4PromotionCandidate) -> Result<bool, String> {
        let stream_id = format!("team-l4-candidate:{}", candidate.team_id);
        let events = self
            .event_store
            .list_stream(&stream_id)
            .map_err(|error| error.to_string())?;
        Ok(events.into_iter().any(|event| {
            event.kind == "team.l4_candidate.lifecycle.v1"
                && event
                    .payload
                    .pointer("/candidate/candidate_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(candidate.candidate_id.as_str())
                && matches!(
                    event.payload.get("lifecycle"),
                    Some(value)
                        if value == &serde_json::json!(L4CandidateLifecycle::Rejected)
                            || value == &serde_json::json!(L4CandidateLifecycle::Superseded)
                )
        }))
    }
}

fn validate_candidate(candidate: &L4PromotionCandidate) -> Result<(), String> {
    for (field, value) in [
        ("candidate_id", &candidate.candidate_id),
        ("team_id", &candidate.team_id),
        ("graph_id", &candidate.graph_id),
        ("lineage_ref", &candidate.lineage_ref),
        ("title", &candidate.title),
        ("content", &candidate.content),
    ] {
        if value.trim().is_empty() {
            return Err(format!("L4 candidate {field} must not be empty"));
        }
    }
    if candidate.source_evidence_refs.is_empty()
        || candidate
            .source_evidence_refs
            .iter()
            .any(|reference| reference.trim().is_empty())
    {
        return Err("L4 candidate requires source evidence references".to_string());
    }
    if candidate.tags.iter().any(|tag| {
        let normalized = tag.to_ascii_lowercase();
        normalized.contains("progress")
            || normalized.contains("draft")
            || normalized.contains("heartbeat")
    }) {
        return Err(
            "L4 candidate cannot promote progress, draft, or heartbeat content".to_string(),
        );
    }
    Ok(())
}
