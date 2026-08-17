//! Governed promotion of terminal Runtime knowledge into Memory L4.
//!
//! Agent and Team execution emit immutable [`KnowledgeCandidate`] events.
//! This service is the only component allowed to validate those candidates,
//! route approvals, and write promoted content to Memory L4.

use std::collections::BTreeMap;
use std::sync::Arc;

use harness_contract::{
    core::TaskRisk,
    knowledge::{
        KnowledgeAuthority, KnowledgeCandidate, KnowledgeCandidateScope, KnowledgeCandidateState,
        KnowledgeNovelty,
    },
};
use memory::{L4PromotionCommand, MemoryScope, Priority};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{
    ApprovalQueue, ApprovalSource, ApprovalSourceKind, ApprovalTimeoutPolicy, GlobalApprovalStatus,
    RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore,
    SubmitGlobalApprovalRequest,
};
use harness_contract::policy::{
    ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand, ApprovalGrantScope,
};

pub type L4PromotionCandidate = KnowledgeCandidate;
pub type L4CandidateLifecycle = KnowledgeCandidateState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L4PromotionReceipt {
    pub candidate_id: String,
    pub promotion_receipt: String,
    pub content_digest: String,
    pub memory_id: String,
    pub lifecycle: KnowledgeCandidateState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCandidateProjection {
    pub candidate: KnowledgeCandidate,
    pub state: KnowledgeCandidateState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<L4PromotionReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone)]
pub struct L4PromotionService {
    event_store: Arc<RuntimeEventStore>,
    approval_queue: Arc<ApprovalQueue>,
    memory_manager: Option<Arc<memory::CognitiveContextManager>>,
    session_policy_lookup:
        Option<Arc<dyn Fn(&str) -> Option<harness_contract::policy::SessionExecutionPolicy> + Send + Sync>>,
}

impl L4PromotionService {
    #[must_use]
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        approval_queue: Arc<ApprovalQueue>,
        memory_manager: Option<Arc<memory::CognitiveContextManager>>,
        session_policy_lookup: Option<
            Arc<
                dyn Fn(
                        &str,
                    ) -> Option<harness_contract::policy::SessionExecutionPolicy>
                    + Send
                    + Sync,
            >,
        >,
    ) -> Self {
        Self {
            event_store,
            approval_queue,
            memory_manager,
            session_policy_lookup,
        }
    }

    /// Apply governance to one immutable candidate.
    ///
    /// Agent-private, low-risk observations can be promoted automatically.
    /// Team/workspace/global, conflicting, or high-risk candidates use the
    /// existing Runtime approval queue and remain pending until its durable
    /// decision is observed by the candidate projector.
    pub async fn govern(
        &self,
        mut candidate: KnowledgeCandidate,
    ) -> Result<KnowledgeCandidateProjection, String> {
        candidate.validate()?;
        if let Some(existing) = self.get(&candidate.candidate_id)? {
            if existing.state == KnowledgeCandidateState::Promoted
                || existing.state == KnowledgeCandidateState::RolledBack
                || existing.state == KnowledgeCandidateState::Rejected
                || existing.state == KnowledgeCandidateState::Superseded
            {
                return Ok(existing);
            }
            if existing.candidate != candidate {
                return Err(format!(
                    "knowledge candidate idempotency conflict: {}",
                    candidate.candidate_id
                ));
            }
        } else {
            candidate.novelty = self.classify_novelty(&candidate)?;
            self.append_state(
                &candidate,
                KnowledgeCandidateState::Proposed,
                None,
                None,
                None,
            )?;
        }

        let current = self
            .get(&candidate.candidate_id)?
            .ok_or_else(|| "candidate proposal was not persisted".to_string())?;
        candidate = current.candidate;
        if candidate.novelty == KnowledgeNovelty::Duplicate {
            self.append_state(
                &candidate,
                KnowledgeCandidateState::Superseded,
                None,
                None,
                Some("an identical promoted claim already exists"),
            )?;
            return self.require_projection(&candidate.candidate_id);
        }
        self.validate_authority(&candidate)?;
        self.append_state(
            &candidate,
            KnowledgeCandidateState::Validated,
            None,
            None,
            None,
        )?;

        if requires_approval(&candidate) {
            let approval_id = knowledge_approval_id(&candidate.candidate_id);
            let source = approval_source(&candidate);
            let source_session_id = source.session_id.clone();
            let action = "knowledge.promote_l4".to_string();
            let mut approval = self.approval_queue.submit_scoped(
                approval_id.clone(),
                SubmitGlobalApprovalRequest {
                    context: harness_contract::policy::ApprovalContext::owned(
                        &source,
                        &action,
                        candidate.scope.key(),
                    ),
                    source,
                    action,
                    summary: format!(
                        "Promote {} knowledge candidate `{}`: {}",
                        candidate.scope.key(),
                        candidate.candidate_id,
                        candidate.title
                    ),
                    risk: candidate.risk,
                    domain: harness_contract::policy::ApprovalDomain::Knowledge,
                    blocks_execution: false,
                    evidence_refs: candidate
                        .evidence_refs
                        .iter()
                        .map(|reference| format!("{}:{}", reference.ref_type, reference.id))
                        .collect(),
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )?;
            if approval.status == GlobalApprovalStatus::Pending {
                let decision = source_session_id
                    .as_deref()
                    .and_then(|session_id| {
                        self.session_policy_lookup
                            .as_ref()
                            .and_then(|lookup| lookup(session_id))
                    })
                    .map(|policy| {
                        crate::approval_router::ApprovalRouter::resolve(
                            policy.autonomy_profile,
                            harness_contract::policy::ApprovalDomain::Knowledge,
                            candidate.risk,
                            false,
                            false,
                        )
                    })
                    .unwrap_or(crate::approval_router::ApprovalDecision::Human);
                match decision {
                    crate::approval_router::ApprovalDecision::AutoApprove
                    | crate::approval_router::ApprovalDecision::StewardApprove => {
                        self.approval_queue.decide_internal(ApprovalDecisionCommand {
                            approval_id: approval_id.clone(),
                            approved: true,
                            skip: false,
                            reason: format!(
                                "approval router {decision:?} for knowledge promotion"
                            ),
                            scope: ApprovalGrantScope::Once,
                            actor: ApprovalDecisionActor {
                                kind: if decision
                                    == crate::approval_router::ApprovalDecision::StewardApprove
                                {
                                    ApprovalDecisionActorKind::StewardAgent
                                } else {
                                    ApprovalDecisionActorKind::Policy
                                },
                                actor_id: if decision
                                    == crate::approval_router::ApprovalDecision::StewardApprove
                                {
                                    "runtime-approval-steward".to_string()
                                } else {
                                    "approval-router-auto".to_string()
                                },
                            },
                            evidence_refs: vec![
                                "approval.router.auto".to_string(),
                                format!("approval.router.decision:{decision:?}"),
                            ],
                        })?;
                        approval = self
                            .approval_queue
                            .get(&approval_id)
                            .ok_or_else(|| "knowledge approval missing after router decision".to_string())?;
                    }
                    crate::approval_router::ApprovalDecision::ContinueAlternative => {
                        self.append_state(
                            &candidate,
                            KnowledgeCandidateState::Rejected,
                            Some(&approval_id),
                            None,
                            Some(
                                "approval router continued alternative for non-blocking knowledge promotion",
                            ),
                        )?;
                        return self.require_projection(&candidate.candidate_id);
                    }
                    crate::approval_router::ApprovalDecision::Human
                    | crate::approval_router::ApprovalDecision::Deny => {}
                }
            }
            match approval.status {
                GlobalApprovalStatus::Pending => {
                    self.append_state(
                        &candidate,
                        KnowledgeCandidateState::AwaitingApproval,
                        Some(&approval_id),
                        None,
                        None,
                    )?;
                    return self.require_projection(&candidate.candidate_id);
                }
                GlobalApprovalStatus::Denied
                | GlobalApprovalStatus::TimedOut
                | GlobalApprovalStatus::Cancelled
                | GlobalApprovalStatus::Superseded
                | GlobalApprovalStatus::Skipped => {
                    self.append_state(
                        &candidate,
                        KnowledgeCandidateState::Rejected,
                        Some(&approval_id),
                        None,
                        Some("knowledge promotion approval was denied or timed out"),
                    )?;
                    return self.require_projection(&candidate.candidate_id);
                }
                GlobalApprovalStatus::Approved => {
                    self.append_state(
                        &candidate,
                        KnowledgeCandidateState::Approved,
                        Some(&approval_id),
                        None,
                        None,
                    )?;
                }
            }
        }

        if self.memory_manager.is_none() {
            self.append_state(
                &candidate,
                KnowledgeCandidateState::Blocked,
                None,
                None,
                Some("L4 promotion requires a configured Memory manager"),
            )?;
            return self.require_projection(&candidate.candidate_id);
        }
        self.promote(candidate).await
    }

    /// Compatibility name for trusted Runtime tests and explicit private
    /// promotion callers. It still applies the complete governance policy.
    pub async fn validate_and_promote(
        &self,
        candidate: KnowledgeCandidate,
    ) -> Result<L4PromotionReceipt, String> {
        let projection = self.govern(candidate).await?;
        projection.receipt.ok_or_else(|| {
            format!(
                "knowledge candidate `{}` is {:?}, not promoted",
                projection.candidate.candidate_id, projection.state
            )
        })
    }

    pub fn lifecycle(
        &self,
        candidate: &KnowledgeCandidate,
    ) -> Result<Vec<KnowledgeCandidateState>, String> {
        self.event_store
            .list_stream(&candidate_stream_id(&candidate.candidate_id))?
            .into_iter()
            .filter(|event| event.kind == "knowledge.candidate.lifecycle.v1")
            .map(|event| {
                serde_json::from_value(
                    event
                        .payload
                        .get("state")
                        .cloned()
                        .ok_or_else(|| "candidate lifecycle event lacks state".to_string())?,
                )
                .map_err(|error| format!("decode candidate lifecycle: {error}"))
            })
            .collect()
    }

    pub fn get(&self, candidate_id: &str) -> Result<Option<KnowledgeCandidateProjection>, String> {
        self.event_store
            .list_stream(&candidate_stream_id(candidate_id))?
            .into_iter()
            .rev()
            .find(|event| event.kind == "knowledge.candidate.lifecycle.v1")
            .map(|event| {
                serde_json::from_value(event.payload)
                    .map_err(|error| format!("decode candidate projection: {error}"))
            })
            .transpose()
    }

    pub fn list(&self) -> Result<Vec<KnowledgeCandidateProjection>, String> {
        let mut latest = BTreeMap::<String, KnowledgeCandidateProjection>::new();
        // Complete scope replay is oldest-first, so later lifecycle records
        // replace earlier projections for the same candidate.
        for event in self.event_store.replay_scope_kind(
            RuntimeEventScope::Knowledge,
            "knowledge.candidate.lifecycle.v1",
        )? {
            let projection = serde_json::from_value::<KnowledgeCandidateProjection>(event.payload)
                .map_err(|error| format!("decode candidate projection: {error}"))?;
            latest.insert(projection.candidate.candidate_id.clone(), projection);
        }
        Ok(latest.into_values().collect())
    }

    pub async fn rollback(
        &self,
        candidate_id: &str,
        reason: &str,
    ) -> Result<KnowledgeCandidateProjection, String> {
        if reason.trim().is_empty() {
            return Err("knowledge rollback requires a reason".to_string());
        }
        let projection = self.require_projection(candidate_id)?;
        if projection.state == KnowledgeCandidateState::RolledBack {
            return Ok(projection);
        }
        if projection.state != KnowledgeCandidateState::Promoted {
            return Err("only a promoted knowledge candidate can be rolled back".to_string());
        }
        let receipt = projection
            .receipt
            .as_ref()
            .ok_or_else(|| "promoted candidate lacks its memory receipt".to_string())?;
        let manager = self
            .memory_manager
            .as_ref()
            .ok_or_else(|| "knowledge rollback requires a configured Memory manager".to_string())?;
        manager
            .delete_entry(&receipt.memory_id)
            .await
            .map_err(|error| error.to_string())?;
        self.append_state(
            &projection.candidate,
            KnowledgeCandidateState::RolledBack,
            projection.approval_id.as_deref(),
            projection.receipt.as_ref(),
            Some(reason),
        )?;
        self.require_projection(candidate_id)
    }

    async fn promote(
        &self,
        candidate: KnowledgeCandidate,
    ) -> Result<KnowledgeCandidateProjection, String> {
        if let Some(existing) = self.get(&candidate.candidate_id)? {
            if existing.state == KnowledgeCandidateState::Promoted {
                return Ok(existing);
            }
        }
        let content_digest = digest(&candidate.claim);
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
                lineage_ref: lineage_ref(&candidate),
                source_evidence_refs: candidate
                    .evidence_refs
                    .iter()
                    .map(|reference| format!("{}:{}", reference.ref_type, reference.id))
                    .collect(),
                scope: memory_scope(&candidate.scope),
                title: candidate.title.clone(),
                content: candidate.claim.clone(),
                priority: priority(candidate.risk),
                tags: candidate.tags.clone(),
            })
            .await
            .map_err(|error| error.to_string())?;
        let receipt = L4PromotionReceipt {
            candidate_id: candidate.candidate_id.clone(),
            promotion_receipt,
            content_digest,
            memory_id: memory_id.to_string(),
            lifecycle: KnowledgeCandidateState::Promoted,
        };
        let approval_id = self
            .get(&candidate.candidate_id)?
            .and_then(|projection| projection.approval_id);
        self.append_state(
            &candidate,
            KnowledgeCandidateState::Promoted,
            approval_id.as_deref(),
            Some(&receipt),
            None,
        )?;
        self.require_projection(&candidate.candidate_id)
    }

    fn classify_novelty(&self, candidate: &KnowledgeCandidate) -> Result<KnowledgeNovelty, String> {
        let title = normalize(&candidate.title);
        let claim = normalize(&candidate.claim);
        for existing in self.list()? {
            if existing.state != KnowledgeCandidateState::Promoted
                || existing.candidate.scope != candidate.scope
            {
                continue;
            }
            if normalize(&existing.candidate.claim) == claim {
                return Ok(KnowledgeNovelty::Duplicate);
            }
            if normalize(&existing.candidate.title) == title {
                return Ok(KnowledgeNovelty::Conflicts);
            }
        }
        Ok(candidate.novelty)
    }

    fn validate_authority(&self, candidate: &KnowledgeCandidate) -> Result<(), String> {
        if matches!(candidate.scope, KnowledgeCandidateScope::Team(_))
            && candidate.authority.rank() < KnowledgeAuthority::TeamSynthesis.rank()
        {
            return Err("team knowledge requires team-synthesis authority".to_string());
        }
        if matches!(
            candidate.scope,
            KnowledgeCandidateScope::Workspace(_) | KnowledgeCandidateScope::Global
        ) && candidate.authority.rank() < KnowledgeAuthority::WorkspaceVerified.rank()
        {
            return Err("workspace/global knowledge requires verified authority".to_string());
        }
        Ok(())
    }

    fn append_state(
        &self,
        candidate: &KnowledgeCandidate,
        state: KnowledgeCandidateState,
        approval_id: Option<&str>,
        receipt: Option<&L4PromotionReceipt>,
        reason: Option<&str>,
    ) -> Result<(), String> {
        let stream_id = candidate_stream_id(&candidate.candidate_id);
        let key = format!("{:?}", state).to_ascii_lowercase();
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
        let projection = KnowledgeCandidateProjection {
            candidate: candidate.clone(),
            state,
            approval_id: approval_id.map(str::to_owned),
            receipt: receipt.cloned(),
            reason: reason.map(str::to_owned),
        };
        self.event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!("knowledge-candidate:{}:{key}", candidate.candidate_id),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id,
                        scope: RuntimeEventScope::Knowledge,
                        kind: "knowledge.candidate.lifecycle.v1".to_string(),
                        status: Some(key.clone()),
                        actor: Some("runtime.l4_promotion_service".to_string()),
                        refs: candidate_event_refs(candidate),
                        payload: serde_json::to_value(projection)
                            .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn require_projection(
        &self,
        candidate_id: &str,
    ) -> Result<KnowledgeCandidateProjection, String> {
        self.get(candidate_id)?
            .ok_or_else(|| format!("knowledge candidate not found: {candidate_id}"))
    }
}

fn requires_approval(candidate: &KnowledgeCandidate) -> bool {
    if matches!(candidate.risk, TaskRisk::High | TaskRisk::Critical)
        || candidate.novelty == KnowledgeNovelty::Conflicts
    {
        return true;
    }
    let distinct_evidence = candidate
        .evidence_refs
        .iter()
        .map(|reference| format!("{}:{}", reference.ref_type, reference.id))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    match candidate.scope {
        KnowledgeCandidateScope::AgentPrivate(_) => false,
        KnowledgeCandidateScope::Team(_) => {
            candidate.risk != TaskRisk::Low
                || candidate.authority.rank() < KnowledgeAuthority::TeamSynthesis.rank()
                || distinct_evidence < 2
        }
        KnowledgeCandidateScope::Workspace(_) => {
            candidate.risk != TaskRisk::Low
                || candidate.authority.rank() < KnowledgeAuthority::WorkspaceVerified.rank()
                || distinct_evidence < 2
        }
        // Global promotion has the largest blast radius and remains the one
        // mandatory human boundary even when its evidence is strong.
        KnowledgeCandidateScope::Global => true,
    }
}

fn approval_source(candidate: &KnowledgeCandidate) -> ApprovalSource {
    let identity = &candidate.execution_identity;
    let kind = match candidate.scope {
        KnowledgeCandidateScope::AgentPrivate(_) => ApprovalSourceKind::Agent,
        KnowledgeCandidateScope::Team(_) => ApprovalSourceKind::Team,
        KnowledgeCandidateScope::Workspace(_) | KnowledgeCandidateScope::Global => {
            ApprovalSourceKind::Mission
        }
    };
    ApprovalSource {
        kind,
        session_id: identity.session_id().map(str::to_owned),
        agent_id: identity.agent_run_id().map(str::to_owned),
        team_id: identity.team_run_id().map(str::to_owned),
        mission_id: identity.mission_id().map(str::to_owned),
        resource_ref: Some(format!("knowledge-candidate:{}", candidate.candidate_id)),
        review_ref: None,
        application: None,
    }
}

fn candidate_event_refs(candidate: &KnowledgeCandidate) -> Vec<RuntimeEventRef> {
    let identity = &candidate.execution_identity;
    let mut refs = vec![
        RuntimeEventRef {
            kind: "principal".to_string(),
            id: identity.principal_id().to_string(),
        },
        RuntimeEventRef {
            kind: "workspace".to_string(),
            id: identity.workspace_id().to_string(),
        },
        RuntimeEventRef {
            kind: "knowledge_scope".to_string(),
            id: candidate.scope.key(),
        },
    ];
    for (kind, id) in [
        ("mission", identity.mission_id()),
        ("task", identity.task_id()),
        ("session", identity.session_id()),
        ("turn", identity.turn_id()),
        ("execution_graph", identity.graph_id()),
        ("team_run", identity.team_run_id()),
        ("agent_run", identity.agent_run_id()),
        ("execution_node", identity.node_id()),
    ] {
        if let Some(id) = id {
            refs.push(RuntimeEventRef {
                kind: kind.to_string(),
                id: id.to_string(),
            });
        }
    }
    refs
}

fn memory_scope(scope: &KnowledgeCandidateScope) -> MemoryScope {
    match scope {
        KnowledgeCandidateScope::AgentPrivate(id) => MemoryScope::AgentInstance(id.clone()),
        KnowledgeCandidateScope::Team(id) => MemoryScope::TeamRun(id.clone()),
        KnowledgeCandidateScope::Workspace(id) => MemoryScope::Project(id.clone()),
        KnowledgeCandidateScope::Global => MemoryScope::Global,
    }
}

fn priority(risk: TaskRisk) -> Priority {
    match risk {
        TaskRisk::Low => Priority::Normal,
        TaskRisk::Medium => Priority::High,
        TaskRisk::High | TaskRisk::Critical => Priority::Critical,
    }
}

fn lineage_ref(candidate: &KnowledgeCandidate) -> String {
    let parents = candidate.lineage.parent_candidate_ids.join(",");
    format!(
        "execution:{}:{}:{}:{}:parents={parents}",
        candidate.execution_identity.workspace_id(),
        candidate
            .execution_identity
            .mission_id()
            .unwrap_or("unbound"),
        candidate.execution_identity.task_id().unwrap_or("unbound"),
        candidate.execution_identity.graph_id().unwrap_or("unbound"),
    )
}

fn candidate_stream_id(candidate_id: &str) -> String {
    format!("knowledge-candidate:{candidate_id}")
}

fn knowledge_approval_id(candidate_id: &str) -> String {
    format!("knowledge-approval:{candidate_id}")
}

fn normalize(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
