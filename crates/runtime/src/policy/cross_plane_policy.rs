//! Cross-plane identity, grant, and approval policy primitives.
//!
//! This module is intentionally independent from channel adapters and service
//! SDKs. Channels and services submit a `CrossPlaneAction`; the policy engine
//! returns a stable decision that UI, audit, and runtime routing can consume.

use chrono::{DateTime, Utc};
use harness_contract::policy::{CrossPlaneRisk, DataClassification};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock, RwLock};
use uuid::Uuid;

const MAX_CROSS_PLANE_AUDIT_RECORDS: usize = 10_000;
const MAX_CROSS_PLANE_EXECUTION_RECEIPTS: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityTrust {
    Verified,
    Claimed,
    Observed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantType {
    SingleUse,
    Session,
    Task,
    TimeBound,
    Persistent,
    Inherited,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecisionKind {
    Allow,
    Deny,
    RequireSingleApproval,
    RequirePersistentGrant,
    RequireAdminApproval,
    Degrade,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneAction {
    pub actor_principal: String,
    #[serde(default)]
    pub actor_identity_ref: Option<String>,
    pub source_channel: Option<String>,
    pub session_id: Option<String>,
    pub requested_capability: String,
    pub provider_account: Option<String>,
    pub target_ref: Option<String>,
    pub resource_ref: Option<String>,
    pub risk: CrossPlaneRisk,
    pub data_classification: DataClassification,
    pub identity_trust: IdentityTrust,
}

impl CrossPlaneAction {
    #[must_use]
    pub fn new(
        actor_principal: impl Into<String>,
        requested_capability: impl Into<String>,
    ) -> Self {
        Self {
            actor_principal: actor_principal.into(),
            actor_identity_ref: None,
            source_channel: None,
            session_id: None,
            requested_capability: requested_capability.into(),
            provider_account: None,
            target_ref: None,
            resource_ref: None,
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            identity_trust: IdentityTrust::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneGrant {
    pub id: String,
    pub principal_id: String,
    pub capability: String,
    pub account_id: Option<String>,
    pub target_ref: Option<String>,
    pub resource_ref: Option<String>,
    pub source_channel: Option<String>,
    pub grant_type: GrantType,
    pub expires_at: Option<DateTime<Utc>>,
    pub remaining_uses: Option<u32>,
    pub created_by: String,
    pub approval_id: Option<String>,
}

impl CrossPlaneGrant {
    #[must_use]
    pub fn persistent(principal_id: impl Into<String>, capability: impl Into<String>) -> Self {
        Self {
            id: format!("grant-{}", Uuid::new_v4()),
            principal_id: principal_id.into(),
            capability: capability.into(),
            account_id: None,
            target_ref: None,
            resource_ref: None,
            source_channel: None,
            grant_type: GrantType::Persistent,
            expires_at: None,
            remaining_uses: None,
            created_by: "system".to_string(),
            approval_id: None,
        }
    }

    #[must_use]
    pub fn matches_action(&self, action: &CrossPlaneAction, now: DateTime<Utc>) -> bool {
        if self.principal_id != action.actor_principal {
            return false;
        }
        if self.capability != action.requested_capability {
            return false;
        }
        if self.expires_at.is_some_and(|expires| expires <= now) {
            return false;
        }
        if self.remaining_uses == Some(0) {
            return false;
        }
        if !optional_matches(&self.account_id, &action.provider_account) {
            return false;
        }
        if !optional_matches(&self.target_ref, &action.target_ref) {
            return false;
        }
        if !optional_matches(&self.resource_ref, &action.resource_ref) {
            return false;
        }
        if !optional_matches(&self.source_channel, &action.source_channel) {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneAuditRecord {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub action: CrossPlaneAction,
    pub decision: CrossPlanePolicyDecision,
    #[serde(default)]
    pub evidence: CrossPlaneDecisionEvidence,
    pub result: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneExecutionReceipt {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub idempotency_key: Option<String>,
    pub mode: String,
    pub status: String,
    pub dispatch_status: String,
    pub action: CrossPlaneAction,
    pub decision: CrossPlanePolicyDecision,
    pub blockers: Vec<String>,
    pub audit_record_id: Option<String>,
    #[serde(default)]
    pub execution_graph_id: Option<String>,
    #[serde(default)]
    pub dispatch_target: Option<CrossPlaneDispatchTarget>,
    #[serde(default)]
    pub dispatch_outcome: Option<CrossPlaneDispatchOutcome>,
}

impl CrossPlaneExecutionReceipt {
    #[must_use]
    pub fn new(
        idempotency_key: Option<String>,
        mode: impl Into<String>,
        status: impl Into<String>,
        dispatch_status: impl Into<String>,
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        blockers: Vec<String>,
        audit_record_id: Option<String>,
    ) -> Self {
        Self {
            id: format!("cpx-{}", Uuid::new_v4()),
            timestamp: Utc::now(),
            idempotency_key,
            mode: mode.into(),
            status: status.into(),
            dispatch_status: dispatch_status.into(),
            action,
            decision,
            blockers,
            audit_record_id,
            execution_graph_id: None,
            dispatch_target: None,
            dispatch_outcome: None,
        }
    }

    #[must_use]
    pub fn with_dispatch_target(
        mut self,
        dispatch_target: Option<CrossPlaneDispatchTarget>,
    ) -> Self {
        self.dispatch_target = dispatch_target;
        self
    }

    #[must_use]
    pub fn with_dispatch_outcome(
        mut self,
        dispatch_outcome: Option<CrossPlaneDispatchOutcome>,
    ) -> Self {
        self.dispatch_outcome = dispatch_outcome;
        self
    }

    #[must_use]
    pub fn with_execution_graph_id(mut self, execution_graph_id: Option<String>) -> Self {
        self.execution_graph_id = execution_graph_id;
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneDispatchTarget {
    pub platform: Option<String>,
    pub operation: Option<String>,
    pub target_ref: Option<String>,
    pub resource_ref: Option<String>,
    pub session_key: Option<String>,
    pub outbound_message: Option<CrossPlaneOutboundMessagePlan>,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneDispatchOutcome {
    pub attempted_at: DateTime<Utc>,
    pub platform: String,
    pub operation: String,
    pub session_key: String,
    pub status: String,
    pub error: Option<String>,
    pub provider_message_id: Option<String>,
}

impl CrossPlaneDispatchOutcome {
    #[must_use]
    pub fn delivery_uncertain(
        platform: impl Into<String>,
        operation: impl Into<String>,
        session_key: impl Into<String>,
    ) -> Self {
        Self {
            attempted_at: Utc::now(),
            platform: platform.into(),
            operation: operation.into(),
            session_key: session_key.into(),
            status: "delivery_uncertain".to_string(),
            error: Some("external delivery requires manual reconciliation".to_string()),
            provider_message_id: None,
        }
    }

    #[must_use]
    pub fn sent(
        platform: impl Into<String>,
        operation: impl Into<String>,
        session_key: impl Into<String>,
        provider_message_id: Option<String>,
    ) -> Self {
        Self {
            attempted_at: Utc::now(),
            platform: platform.into(),
            operation: operation.into(),
            session_key: session_key.into(),
            status: "sent".to_string(),
            error: None,
            provider_message_id,
        }
    }

    #[must_use]
    pub fn failed(
        platform: impl Into<String>,
        operation: impl Into<String>,
        session_key: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            attempted_at: Utc::now(),
            platform: platform.into(),
            operation: operation.into(),
            session_key: session_key.into(),
            status: "failed".to_string(),
            error: Some(error.into()),
            provider_message_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneOutboundMessagePlan {
    pub session_key: String,
    pub text: String,
    pub payload_kind: String,
    pub payload_ref: String,
    pub caption: Option<String>,
    pub file_name: Option<String>,
    pub reply_to: Option<String>,
    pub metadata: serde_json::Value,
}

impl CrossPlaneDispatchTarget {
    #[must_use]
    pub fn from_action(
        action: &CrossPlaneAction,
        target_platform: Option<&str>,
        operation: Option<&str>,
    ) -> Option<Self> {
        let operation = operation
            .map(str::to_string)
            .or_else(|| cross_plane_operation_from_capability(&action.requested_capability));
        if target_platform.is_none() && operation.is_none() {
            return None;
        }

        let platform = target_platform.map(str::to_string);
        let mut blockers = Vec::new();
        if platform.is_none() {
            blockers.push("dispatch:target_platform_missing".to_string());
        }
        let session_key = match (platform.as_deref(), action.target_ref.as_deref()) {
            (_, None) => {
                blockers.push("dispatch:target_ref_missing".to_string());
                None
            }
            (None, Some(_)) => None,
            (Some(platform), Some(target_ref)) => {
                match cross_plane_session_key_from_target_ref(platform, target_ref) {
                    Some(session_key) => Some(session_key),
                    None => {
                        blockers.push("dispatch:target_ref_invalid".to_string());
                        None
                    }
                }
            }
        };

        let payload = operation.as_deref().and_then(|operation| {
            cross_plane_outbound_payload_for_operation(operation, action, &mut blockers)
        });
        let outbound_message = session_key
            .as_ref()
            .zip(payload)
            .map(|(session_key, payload)| CrossPlaneOutboundMessagePlan {
                session_key: session_key.clone(),
                text: payload.text.clone(),
                payload_kind: payload.kind.to_string(),
                payload_ref: payload.payload_ref,
                caption: payload.caption,
                file_name: payload.file_name,
                reply_to: None,
                metadata: serde_json::json!({
                    "cross_plane": true,
                    "operation": operation,
                    "requested_capability": action.requested_capability,
                    "resource_ref": action.resource_ref,
                    "source_channel": action.source_channel,
                    "session_id": action.session_id,
                }),
            });
        let ready = blockers.is_empty() && outbound_message.is_some();

        Some(Self {
            platform,
            operation,
            target_ref: action.target_ref.clone(),
            resource_ref: action.resource_ref.clone(),
            session_key,
            outbound_message,
            ready,
            blockers,
        })
    }
}

struct CrossPlaneOutboundPayload {
    kind: &'static str,
    text: String,
    payload_ref: String,
    caption: Option<String>,
    file_name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneDecisionEvidence {
    pub policy_version: String,
    pub evaluated_at: Option<DateTime<Utc>>,
    pub resolved_identity: Option<CrossPlaneResolvedIdentity>,
    #[serde(default)]
    pub connector_context: Option<ConnectorDecisionEvidence>,
    pub active_grants_before: usize,
    pub matched_grant_id: Option<String>,
    pub consumed_grant_id: Option<String>,
    pub remaining_uses_after: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorDecisionEvidence {
    pub provider: String,
    pub plane: String,
    pub capability_id: String,
    #[serde(default)]
    pub provider_account: Option<String>,
    #[serde(default)]
    pub account_status: Option<String>,
    #[serde(default)]
    pub account_reason: Option<String>,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub missing_scopes: Vec<String>,
    pub supports_commit: bool,
    pub requires_approval: bool,
    pub risk: CrossPlaneRisk,
    pub data_classification: DataClassification,
    pub requested_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorActionContext {
    pub provider: String,
    pub plane: String,
    pub capability_id: String,
    pub provider_account: Option<String>,
    pub account_status: Option<String>,
    pub account_reason: Option<String>,
    pub resource_ref: Option<String>,
    pub required_scopes: Vec<String>,
    pub missing_scopes: Vec<String>,
    pub supports_commit: bool,
    pub requires_approval: bool,
    pub risk: CrossPlaneRisk,
    pub data_classification: DataClassification,
    pub requested_mode: String,
}

impl ConnectorActionContext {
    #[must_use]
    pub fn evidence(&self) -> ConnectorDecisionEvidence {
        ConnectorDecisionEvidence {
            provider: self.provider.clone(),
            plane: self.plane.clone(),
            capability_id: self.capability_id.clone(),
            provider_account: self.provider_account.clone(),
            account_status: self.account_status.clone(),
            account_reason: self.account_reason.clone(),
            resource_ref: self.resource_ref.clone(),
            required_scopes: self.required_scopes.clone(),
            missing_scopes: self.missing_scopes.clone(),
            supports_commit: self.supports_commit,
            requires_approval: self.requires_approval,
            risk: self.risk,
            data_classification: self.data_classification,
            requested_mode: self.requested_mode.clone(),
        }
    }

    #[must_use]
    pub fn requests_commit(&self) -> bool {
        matches!(
            self.requested_mode.trim().to_ascii_lowercase().as_str(),
            "commit" | "live" | "execute"
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneIdentityBinding {
    pub id: String,
    pub principal_id: String,
    pub identity_ref: String,
    pub trust: IdentityTrust,
    pub source: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneResolvedIdentity {
    pub input_ref: String,
    pub principal_id: String,
    pub trust: IdentityTrust,
    pub binding_id: String,
    pub matched_ref: String,
    pub match_kind: String,
}

impl CrossPlaneIdentityBinding {
    #[must_use]
    pub fn verified(principal_id: impl Into<String>, identity_ref: impl Into<String>) -> Self {
        Self {
            id: format!("idb-{}", Uuid::new_v4()),
            principal_id: principal_id.into(),
            identity_ref: identity_ref.into(),
            trust: IdentityTrust::Verified,
            source: "manual".to_string(),
            created_at: Utc::now(),
            expires_at: None,
        }
    }

    #[must_use]
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_none_or(|expires| expires > now)
    }
}

impl CrossPlaneAuditRecord {
    #[must_use]
    pub fn new(
        action: CrossPlaneAction,
        decision: CrossPlanePolicyDecision,
        result: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            id: format!("cpa-{}", Uuid::new_v4()),
            timestamp: Utc::now(),
            action,
            decision,
            evidence: CrossPlaneDecisionEvidence::default(),
            result: result.into(),
            summary: summary.into(),
        }
    }

    #[must_use]
    pub fn with_evidence(mut self, evidence: CrossPlaneDecisionEvidence) -> Self {
        self.evidence = evidence;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlanePolicyConfig {
    pub unknown_actor: PolicyDecisionKind,
    pub claimed_identity: PolicyDecisionKind,
    pub observed_identity: PolicyDecisionKind,
    pub confidential_data: PolicyDecisionKind,
    pub secret_data: PolicyDecisionKind,
    pub high_risk: PolicyDecisionKind,
    pub critical_risk: PolicyDecisionKind,
}

impl Default for CrossPlanePolicyConfig {
    fn default() -> Self {
        Self {
            unknown_actor: PolicyDecisionKind::Deny,
            claimed_identity: PolicyDecisionKind::RequireSingleApproval,
            observed_identity: PolicyDecisionKind::RequireSingleApproval,
            confidential_data: PolicyDecisionKind::RequireSingleApproval,
            secret_data: PolicyDecisionKind::RequireAdminApproval,
            high_risk: PolicyDecisionKind::RequireSingleApproval,
            critical_risk: PolicyDecisionKind::RequireAdminApproval,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlanePolicyDecision {
    pub decision: PolicyDecisionKind,
    pub reason: String,
    pub matched_grant: Option<CrossPlaneGrant>,
    pub required_approval: Option<GrantType>,
    pub degrade_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlanePolicyEngine {
    pub config: CrossPlanePolicyConfig,
    pub grants: Vec<CrossPlaneGrant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneSummary {
    pub verified_identities: usize,
    pub claimed_identities: usize,
    pub observed_identities: usize,
    pub active_grants: usize,
    pub audit_records: usize,
    pub allowed_actions: usize,
    pub denied_actions: usize,
    pub approval_required_actions: usize,
}

#[derive(Debug, Clone, Default)]
pub struct CrossPlaneControlPlane {
    inner: Arc<RwLock<CrossPlaneControlState>>,
}

#[derive(Debug, Clone, Default)]
struct CrossPlaneControlState {
    identities: Vec<CrossPlaneIdentityBinding>,
    grants: Vec<CrossPlaneGrant>,
    audit: Vec<CrossPlaneAuditRecord>,
    executions: Vec<CrossPlaneExecutionReceipt>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneControlSnapshot {
    pub identities: Vec<CrossPlaneIdentityBinding>,
    pub grants: Vec<CrossPlaneGrant>,
    pub audit: Vec<CrossPlaneAuditRecord>,
    #[serde(default)]
    pub executions: Vec<CrossPlaneExecutionReceipt>,
}

impl CrossPlaneControlPlane {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn list_grants(&self) -> Vec<CrossPlaneGrant> {
        self.inner
            .read()
            .map(|state| state.grants.clone())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn snapshot(&self) -> CrossPlaneControlSnapshot {
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        CrossPlaneControlSnapshot {
            identities: state.identities.clone(),
            grants: state.grants.clone(),
            audit: state.audit.clone(),
            executions: state.executions.clone(),
        }
    }

    pub fn replace_snapshot(&self, snapshot: CrossPlaneControlSnapshot) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        state.identities = snapshot.identities;
        state.grants = snapshot.grants;
        state.audit = snapshot.audit;
        state.executions = snapshot.executions;
    }

    #[must_use]
    pub fn list_identities(&self) -> Vec<CrossPlaneIdentityBinding> {
        self.inner
            .read()
            .map(|state| state.identities.clone())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn resolve_identity(
        &self,
        identity_ref: &str,
        now: DateTime<Utc>,
    ) -> Option<CrossPlaneResolvedIdentity> {
        let input_ref = identity_ref.trim();
        if input_ref.is_empty() {
            return None;
        }
        let input_normalized = normalize_identity_ref(input_ref);
        let input_contact_keys = identity_contact_keys(input_ref);
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        let active = state
            .identities
            .iter()
            .filter(|binding| binding.is_active(now))
            .collect::<Vec<_>>();

        if let Some(binding) = active
            .iter()
            .filter(|binding| normalize_identity_ref(&binding.identity_ref) == input_normalized)
            .max_by_key(|binding| identity_trust_rank(binding.trust))
        {
            return Some(CrossPlaneResolvedIdentity {
                input_ref: input_ref.to_string(),
                principal_id: binding.principal_id.clone(),
                trust: binding.trust,
                binding_id: binding.id.clone(),
                matched_ref: binding.identity_ref.clone(),
                match_kind: "exact_ref".to_string(),
            });
        }

        if input_contact_keys.is_empty() {
            return None;
        }
        active
            .iter()
            .filter_map(|binding| {
                let binding_keys = identity_contact_keys(&binding.identity_ref);
                let matched = input_contact_keys
                    .iter()
                    .any(|key| binding_keys.iter().any(|candidate| candidate == key));
                matched.then_some(binding)
            })
            .max_by_key(|binding| identity_trust_rank(binding.trust))
            .map(|binding| CrossPlaneResolvedIdentity {
                input_ref: input_ref.to_string(),
                principal_id: binding.principal_id.clone(),
                trust: binding.trust,
                binding_id: binding.id.clone(),
                matched_ref: binding.identity_ref.clone(),
                match_kind: "contact_key".to_string(),
            })
    }

    pub fn upsert_identity(
        &self,
        mut binding: CrossPlaneIdentityBinding,
    ) -> CrossPlaneIdentityBinding {
        if binding.id.trim().is_empty() {
            binding.id = format!("idb-{}", Uuid::new_v4());
        }
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        if let Some(existing) = state
            .identities
            .iter_mut()
            .find(|existing| existing.id == binding.id)
        {
            *existing = binding.clone();
        } else {
            state.identities.push(binding.clone());
        }
        binding
    }

    pub fn revoke_identity(&self, id: &str) -> bool {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let original_len = state.identities.len();
        state.identities.retain(|binding| binding.id != id);
        state.identities.len() != original_len
    }

    pub fn upsert_grant(&self, mut grant: CrossPlaneGrant) -> CrossPlaneGrant {
        if grant.id.trim().is_empty() {
            grant.id = format!("grant-{}", Uuid::new_v4());
        }
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        if let Some(existing) = state
            .grants
            .iter_mut()
            .find(|existing| existing.id == grant.id)
        {
            *existing = grant.clone();
        } else {
            state.grants.push(grant.clone());
        }
        grant
    }

    pub fn revoke_grant(&self, id: &str) -> bool {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let original_len = state.grants.len();
        state.grants.retain(|grant| grant.id != id);
        state.grants.len() != original_len
    }

    #[must_use]
    pub fn list_audit(&self, limit: usize, offset: usize) -> Vec<CrossPlaneAuditRecord> {
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        let mut records = state.audit.clone();
        records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        records.into_iter().skip(offset).take(limit).collect()
    }

    pub fn record_audit(&self, record: CrossPlaneAuditRecord) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        state.audit.push(record);
        let overflow = state
            .audit
            .len()
            .saturating_sub(MAX_CROSS_PLANE_AUDIT_RECORDS);
        if overflow > 0 {
            state.audit.drain(0..overflow);
        }
    }

    #[must_use]
    pub fn list_executions(&self, limit: usize, offset: usize) -> Vec<CrossPlaneExecutionReceipt> {
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        let mut records = state.executions.clone();
        records.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        records.into_iter().skip(offset).take(limit).collect()
    }

    #[must_use]
    pub fn find_execution_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Option<CrossPlaneExecutionReceipt> {
        let key = idempotency_key.trim();
        if key.is_empty() {
            return None;
        }
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        state
            .executions
            .iter()
            .rev()
            .find(|receipt| receipt.idempotency_key.as_deref() == Some(key))
            .cloned()
    }

    #[must_use]
    pub fn find_execution(&self, receipt_id: &str) -> Option<CrossPlaneExecutionReceipt> {
        let receipt_id = receipt_id.trim();
        if receipt_id.is_empty() {
            return None;
        }
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        state
            .executions
            .iter()
            .find(|receipt| receipt.id == receipt_id)
            .cloned()
    }

    pub fn record_execution(&self, receipt: CrossPlaneExecutionReceipt) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        state.executions.push(receipt);
        let overflow = state
            .executions
            .len()
            .saturating_sub(MAX_CROSS_PLANE_EXECUTION_RECEIPTS);
        if overflow > 0 {
            state.executions.drain(0..overflow);
        }
    }

    pub fn reconcile_execution(
        &self,
        idempotency_key: &str,
        outcome: CrossPlaneDispatchOutcome,
    ) -> Option<CrossPlaneExecutionReceipt> {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let receipt = state
            .executions
            .iter_mut()
            .rev()
            .find(|receipt| receipt.idempotency_key.as_deref() == Some(idempotency_key))?;
        match outcome.status.as_str() {
            "sent" => {
                receipt.status = "dispatched".into();
                receipt.dispatch_status = "sent".into();
                receipt.blockers.clear();
            }
            "failed" => {
                receipt.status = "blocked".into();
                receipt.dispatch_status = "dispatch_failed".into();
                receipt.blockers = outcome.error.clone().into_iter().collect();
            }
            _ => {
                receipt.status = "delivery_uncertain".into();
                receipt.dispatch_status = "blocked_reconciliation".into();
                receipt.blockers = vec!["delivery_uncertain:manual_reconciliation_required".into()];
            }
        }
        receipt.dispatch_outcome = Some(outcome);
        receipt.timestamp = Utc::now();
        Some(receipt.clone())
    }

    #[must_use]
    pub fn decide_and_audit(
        &self,
        action: CrossPlaneAction,
        now: DateTime<Utc>,
    ) -> CrossPlanePolicyDecision {
        self.decide_and_audit_with_action(action, now).1
    }

    #[must_use]
    pub fn decide_with_action(
        &self,
        mut action: CrossPlaneAction,
        now: DateTime<Utc>,
    ) -> (CrossPlaneAction, CrossPlanePolicyDecision) {
        self.resolve_action_identity(&mut action, now);
        let active_grants = self.active_grants(now);
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default())
            .with_grants(active_grants);
        let decision = engine.decide(&action, now);
        (action, decision)
    }

    #[must_use]
    pub fn decide_with_connector_context(
        &self,
        mut action: CrossPlaneAction,
        context: Option<ConnectorActionContext>,
        now: DateTime<Utc>,
    ) -> (
        CrossPlaneAction,
        CrossPlanePolicyDecision,
        CrossPlaneDecisionEvidence,
    ) {
        apply_connector_context_to_action(&mut action, context.as_ref());
        let resolved_identity = self.resolve_action_identity(&mut action, now);
        let active_grants = self.active_grants(now);
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default())
            .with_grants(active_grants.clone());
        let decision = engine.decide_with_connector_context(&action, context.as_ref(), now);
        let evidence = CrossPlaneDecisionEvidence {
            policy_version: "cross-plane.v2.connector".to_string(),
            evaluated_at: Some(now),
            resolved_identity,
            connector_context: context.as_ref().map(ConnectorActionContext::evidence),
            active_grants_before: active_grants.len(),
            matched_grant_id: decision
                .matched_grant
                .as_ref()
                .map(|grant| grant.id.clone()),
            consumed_grant_id: None,
            remaining_uses_after: None,
        };
        (action, decision, evidence)
    }

    #[must_use]
    pub fn decide_and_audit_with_action(
        &self,
        mut action: CrossPlaneAction,
        now: DateTime<Utc>,
    ) -> (CrossPlaneAction, CrossPlanePolicyDecision) {
        let resolved_identity = self.resolve_action_identity(&mut action, now);
        let active_grants = self.active_grants(now);
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default())
            .with_grants(active_grants.clone());
        let decision = engine.decide(&action, now);
        let consumed = self.consume_matched_grant_for_decision(&decision);
        let evidence = CrossPlaneDecisionEvidence {
            policy_version: "cross-plane.v1".to_string(),
            evaluated_at: Some(now),
            resolved_identity,
            connector_context: None,
            active_grants_before: active_grants.len(),
            matched_grant_id: decision
                .matched_grant
                .as_ref()
                .map(|grant| grant.id.clone()),
            consumed_grant_id: consumed
                .as_ref()
                .map(|(grant_id, _remaining)| grant_id.clone()),
            remaining_uses_after: consumed.as_ref().map(|(_grant_id, remaining)| *remaining),
        };
        self.record_audit(
            CrossPlaneAuditRecord::new(
                action.clone(),
                decision.clone(),
                format!("{:?}", decision.decision).to_lowercase(),
                decision.reason.clone(),
            )
            .with_evidence(evidence),
        );
        (action, decision)
    }

    #[must_use]
    pub fn decide_and_audit_with_connector_context(
        &self,
        action: CrossPlaneAction,
        context: Option<ConnectorActionContext>,
        now: DateTime<Utc>,
    ) -> (
        CrossPlaneAction,
        CrossPlanePolicyDecision,
        CrossPlaneDecisionEvidence,
    ) {
        let (action, decision, mut evidence) =
            self.decide_with_connector_context(action, context, now);
        let consumed = self.consume_matched_grant_for_decision(&decision);
        evidence.consumed_grant_id = consumed
            .as_ref()
            .map(|(grant_id, _remaining)| grant_id.clone());
        evidence.remaining_uses_after = consumed.as_ref().map(|(_grant_id, remaining)| *remaining);
        self.record_audit(
            CrossPlaneAuditRecord::new(
                action.clone(),
                decision.clone(),
                format!("{:?}", decision.decision).to_lowercase(),
                decision.reason.clone(),
            )
            .with_evidence(evidence.clone()),
        );
        (action, decision, evidence)
    }

    fn resolve_action_identity(
        &self,
        action: &mut CrossPlaneAction,
        now: DateTime<Utc>,
    ) -> Option<CrossPlaneResolvedIdentity> {
        let identity_ref = action.actor_identity_ref.as_deref()?;
        let resolved = self.resolve_identity(identity_ref, now)?;
        if action.actor_principal.trim().is_empty()
            || action.actor_principal == identity_ref
            || identity_trust_rank(resolved.trust) > identity_trust_rank(action.identity_trust)
        {
            action.actor_principal = resolved.principal_id.clone();
            action.identity_trust = resolved.trust;
        }
        Some(resolved)
    }

    fn active_grants(&self, now: DateTime<Utc>) -> Vec<CrossPlaneGrant> {
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        state
            .grants
            .iter()
            .filter(|grant| grant.expires_at.is_none_or(|expires| expires > now))
            .filter(|grant| grant.remaining_uses != Some(0))
            .cloned()
            .collect()
    }

    pub fn consume_matched_grant_for_decision(
        &self,
        decision: &CrossPlanePolicyDecision,
    ) -> Option<(String, u32)> {
        let grant = decision.matched_grant.as_ref()?;
        if grant.grant_type != GrantType::SingleUse {
            return None;
        }
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        let stored = state
            .grants
            .iter_mut()
            .find(|stored| stored.id == grant.id)?;
        let remaining = stored.remaining_uses.unwrap_or(1).saturating_sub(1);
        stored.remaining_uses = Some(remaining);
        Some((stored.id.clone(), remaining))
    }

    #[must_use]
    pub fn summary(&self, now: DateTime<Utc>) -> CrossPlaneSummary {
        let state = self.inner.read().unwrap_or_else(|err| err.into_inner());
        let active_identities = state
            .identities
            .iter()
            .filter(|binding| binding.is_active(now))
            .collect::<Vec<_>>();
        let verified_identities = active_identities
            .iter()
            .filter(|binding| binding.trust == IdentityTrust::Verified)
            .count();
        let claimed_identities = active_identities
            .iter()
            .filter(|binding| binding.trust == IdentityTrust::Claimed)
            .count();
        let observed_identities = active_identities
            .iter()
            .filter(|binding| binding.trust == IdentityTrust::Observed)
            .count();
        let active_grants = state
            .grants
            .iter()
            .filter(|grant| grant.expires_at.is_none_or(|expires| expires > now))
            .filter(|grant| grant.remaining_uses != Some(0))
            .count();
        let allowed_actions = state
            .audit
            .iter()
            .filter(|record| record.decision.decision == PolicyDecisionKind::Allow)
            .count();
        let denied_actions = state
            .audit
            .iter()
            .filter(|record| record.decision.decision == PolicyDecisionKind::Deny)
            .count();
        let approval_required_actions = state
            .audit
            .iter()
            .filter(|record| {
                matches!(
                    record.decision.decision,
                    PolicyDecisionKind::RequireSingleApproval
                        | PolicyDecisionKind::RequirePersistentGrant
                        | PolicyDecisionKind::RequireAdminApproval
                )
            })
            .count();
        CrossPlaneSummary {
            verified_identities,
            claimed_identities,
            observed_identities,
            active_grants,
            audit_records: state.audit.len(),
            allowed_actions,
            denied_actions,
            approval_required_actions,
        }
    }
}

impl CrossPlanePolicyEngine {
    #[must_use]
    pub fn new(config: CrossPlanePolicyConfig) -> Self {
        Self {
            config,
            grants: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_grants(mut self, grants: Vec<CrossPlaneGrant>) -> Self {
        self.grants = grants;
        self
    }

    #[must_use]
    pub fn decide(
        &self,
        action: &CrossPlaneAction,
        now: DateTime<Utc>,
    ) -> CrossPlanePolicyDecision {
        self.decide_with_connector_context(action, None, now)
    }

    #[must_use]
    pub fn decide_with_connector_context(
        &self,
        action: &CrossPlaneAction,
        context: Option<&ConnectorActionContext>,
        now: DateTime<Utc>,
    ) -> CrossPlanePolicyDecision {
        if let Some(decision) = self.connector_preflight_decision(action, context) {
            return decision;
        }

        if let Some(grant) = self
            .grants
            .iter()
            .find(|grant| grant.matches_action(action, now))
            .cloned()
        {
            return CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::Allow,
                reason: "matched_grant".to_string(),
                matched_grant: Some(grant),
                required_approval: None,
                degrade_to: None,
            };
        }

        match action.identity_trust {
            IdentityTrust::Unknown => {
                return self.decision(self.config.unknown_actor, "unknown_actor");
            }
            IdentityTrust::Observed => {
                return self.decision(
                    self.config.observed_identity,
                    "observed_identity_requires_binding",
                );
            }
            IdentityTrust::Claimed => {
                return self.decision(
                    self.config.claimed_identity,
                    "claimed_identity_requires_confirmation",
                );
            }
            IdentityTrust::Verified => {}
        }

        match action.data_classification {
            DataClassification::Secret => {
                return self.decision(
                    self.config.secret_data,
                    "secret_data_requires_admin_approval",
                );
            }
            DataClassification::Confidential => {
                return self.decision(
                    self.config.confidential_data,
                    "confidential_data_requires_approval",
                );
            }
            DataClassification::Public | DataClassification::Internal => {}
        }

        if context.is_some_and(|context| context.requires_approval) {
            return CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::RequireSingleApproval,
                reason: "connector_capability_requires_approval".to_string(),
                matched_grant: None,
                required_approval: Some(GrantType::SingleUse),
                degrade_to: None,
            };
        }

        match action.risk {
            CrossPlaneRisk::Critical => self.decision(
                self.config.critical_risk,
                "critical_risk_requires_admin_approval",
            ),
            CrossPlaneRisk::High => {
                self.decision(self.config.high_risk, "high_risk_requires_approval")
            }
            CrossPlaneRisk::Medium => CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::RequireSingleApproval,
                reason: "medium_risk_requires_approval_without_grant".to_string(),
                matched_grant: None,
                required_approval: Some(GrantType::SingleUse),
                degrade_to: None,
            },
            CrossPlaneRisk::Low => CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::Allow,
                reason: "low_risk_verified_actor".to_string(),
                matched_grant: None,
                required_approval: None,
                degrade_to: None,
            },
        }
    }

    fn connector_preflight_decision(
        &self,
        action: &CrossPlaneAction,
        context: Option<&ConnectorActionContext>,
    ) -> Option<CrossPlanePolicyDecision> {
        let context = context?;
        if context.capability_id != action.requested_capability {
            return Some(CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::Deny,
                reason: "connector_capability_mismatch".to_string(),
                matched_grant: None,
                required_approval: None,
                degrade_to: None,
            });
        }
        if !context.missing_scopes.is_empty() {
            return Some(CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::Deny,
                reason: format!(
                    "connector_missing_account_scope:{}",
                    context.missing_scopes.join(",")
                ),
                matched_grant: None,
                required_approval: None,
                degrade_to: None,
            });
        }
        match context.account_status.as_deref() {
            Some("disabled") => {
                return Some(CrossPlanePolicyDecision {
                    decision: PolicyDecisionKind::Deny,
                    reason: "connector_account_disabled".to_string(),
                    matched_grant: None,
                    required_approval: None,
                    degrade_to: None,
                });
            }
            Some("degraded" | "unknown") if context.requests_commit() => {
                return Some(CrossPlanePolicyDecision {
                    decision: PolicyDecisionKind::Deny,
                    reason: "connector_account_not_ready_for_commit".to_string(),
                    matched_grant: None,
                    required_approval: None,
                    degrade_to: None,
                });
            }
            _ => {}
        }
        if context.requests_commit() && !context.supports_commit {
            return Some(CrossPlanePolicyDecision {
                decision: PolicyDecisionKind::Deny,
                reason: "connector_capability_does_not_support_commit".to_string(),
                matched_grant: None,
                required_approval: None,
                degrade_to: None,
            });
        }
        None
    }

    fn decision(
        &self,
        decision: PolicyDecisionKind,
        reason: impl Into<String>,
    ) -> CrossPlanePolicyDecision {
        let required_approval = match decision {
            PolicyDecisionKind::RequireSingleApproval => Some(GrantType::SingleUse),
            PolicyDecisionKind::RequirePersistentGrant => Some(GrantType::Persistent),
            PolicyDecisionKind::RequireAdminApproval => Some(GrantType::Persistent),
            PolicyDecisionKind::Allow | PolicyDecisionKind::Deny | PolicyDecisionKind::Degrade => {
                None
            }
        };
        CrossPlanePolicyDecision {
            decision,
            reason: reason.into(),
            matched_grant: None,
            required_approval,
            degrade_to: None,
        }
    }
}

fn optional_matches(expected: &Option<String>, actual: &Option<String>) -> bool {
    expected
        .as_ref()
        .is_none_or(|expected| actual.as_ref().is_some_and(|actual| actual == expected))
}

fn apply_connector_context_to_action(
    action: &mut CrossPlaneAction,
    context: Option<&ConnectorActionContext>,
) {
    let Some(context) = context else {
        return;
    };
    action.risk = context.risk;
    action.data_classification = context.data_classification;
    if action.provider_account.is_none() {
        action.provider_account = context.provider_account.clone();
    }
    if action.resource_ref.is_none() {
        action.resource_ref = context.resource_ref.clone();
    }
}

fn normalize_identity_ref(identity_ref: &str) -> String {
    identity_ref.trim().to_ascii_lowercase()
}

fn identity_trust_rank(trust: IdentityTrust) -> u8 {
    match trust {
        IdentityTrust::Verified => 3,
        IdentityTrust::Claimed => 2,
        IdentityTrust::Observed => 1,
        IdentityTrust::Unknown => 0,
    }
}

fn identity_contact_keys(identity_ref: &str) -> Vec<String> {
    static EMAIL_RE: OnceLock<Option<Regex>> = OnceLock::new();
    let Some(email_re) = EMAIL_RE
        .get_or_init(|| Regex::new(r"(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}").ok())
        .as_ref()
    else {
        return Vec::new();
    };
    let mut keys = Vec::new();
    for matched in email_re.find_iter(identity_ref) {
        keys.push(format!("email:{}", matched.as_str().to_ascii_lowercase()));
    }

    let lower = identity_ref.to_ascii_lowercase();
    for part in lower.split(['?', '&', ';', ',', '|', ' ']) {
        let Some(value) = part
            .strip_prefix("phone=")
            .or_else(|| part.strip_prefix("mobile="))
            .or_else(|| part.strip_prefix("tel="))
            .or_else(|| part.strip_prefix("phone:"))
            .or_else(|| part.strip_prefix("mobile:"))
            .or_else(|| part.strip_prefix("tel:"))
        else {
            continue;
        };
        let mut normalized = String::new();
        for (idx, ch) in value.chars().enumerate() {
            if ch == '+' && idx == 0 {
                normalized.push(ch);
            } else if ch.is_ascii_digit() {
                normalized.push(ch);
            }
        }
        let digit_count = normalized.chars().filter(char::is_ascii_digit).count();
        if digit_count >= 7 {
            keys.push(format!("phone:{normalized}"));
        }
    }

    keys.sort();
    keys.dedup();
    keys
}

fn cross_plane_session_key_from_target_ref(platform: &str, target_ref: &str) -> Option<String> {
    let value = target_ref.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(rest) = value
        .strip_prefix("channel://")
        .or_else(|| value.strip_prefix("service://"))
    {
        let mut parts = rest.split('/').filter(|part| !part.is_empty());
        let target_platform = parts.next()?.to_ascii_lowercase();
        if target_platform != platform.to_ascii_lowercase() {
            return None;
        }
        let remaining = parts.collect::<Vec<_>>();
        return cross_plane_session_key_from_path_parts(platform, &remaining);
    }

    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case(platform) {
        let user_id = parts[1].trim();
        if user_id.is_empty() {
            return None;
        }
        return if parts.get(2).is_some_and(|thread| !thread.trim().is_empty()) {
            Some(format!("{platform}:{user_id}:{}", parts[2].trim()))
        } else {
            Some(format!("{platform}:{user_id}"))
        };
    }

    None
}

fn cross_plane_session_key_from_path_parts(platform: &str, parts: &[&str]) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let user_id = if matches!(parts[0], "user" | "chat" | "session") {
        parts.get(1).copied()
    } else {
        parts.first().copied()
    }?
    .trim();
    if user_id.is_empty() {
        return None;
    }
    let thread_id = parts
        .windows(2)
        .find(|window| matches!(window[0], "thread" | "topic"))
        .map(|window| window[1].trim())
        .filter(|thread| !thread.is_empty());
    Some(match thread_id {
        Some(thread_id) => format!("{platform}:{user_id}:{thread_id}"),
        None => format!("{platform}:{user_id}"),
    })
}

fn cross_plane_outbound_payload_for_operation(
    operation: &str,
    action: &CrossPlaneAction,
    blockers: &mut Vec<String>,
) -> Option<CrossPlaneOutboundPayload> {
    match operation {
        "send_text" => {
            let text = action
                .resource_ref
                .as_deref()
                .and_then(cross_plane_text_payload_from_resource_ref);
            if text.as_deref().is_none_or(str::is_empty) {
                blockers.push("dispatch:payload_text_missing".to_string());
            }
            text.map(|text| CrossPlaneOutboundPayload {
                kind: "text",
                payload_ref: text.clone(),
                text,
                caption: None,
                file_name: None,
            })
        }
        "send_image" | "send_file" => {
            let Some(resource_ref) = action.resource_ref.as_deref().map(str::trim) else {
                blockers.push("dispatch:resource_ref_missing".to_string());
                return None;
            };
            if resource_ref.is_empty() {
                blockers.push("dispatch:resource_ref_missing".to_string());
                None
            } else {
                let payload_ref = resource_ref
                    .strip_prefix("image://")
                    .or_else(|| resource_ref.strip_prefix("file://"))
                    .unwrap_or(resource_ref)
                    .to_string();
                let file_name = (operation == "send_file").then(|| {
                    std::path::Path::new(&payload_ref)
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("document")
                        .to_string()
                });
                Some(CrossPlaneOutboundPayload {
                    kind: if operation == "send_image" {
                        "image"
                    } else {
                        "file"
                    },
                    text: payload_ref.clone(),
                    payload_ref,
                    caption: None,
                    file_name,
                })
            }
        }
        _ => {
            blockers.push("dispatch:operation_not_dispatchable".to_string());
            None
        }
    }
}

fn cross_plane_text_payload_from_resource_ref(resource_ref: &str) -> Option<String> {
    let value = resource_ref.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(text) = value
        .strip_prefix("text://")
        .or_else(|| value.strip_prefix("text:"))
    {
        return Some(text.to_string());
    }
    if value.contains("://") {
        return None;
    }
    Some(value.to_string())
}

fn cross_plane_operation_from_capability(capability: &str) -> Option<String> {
    let lower = capability.trim().to_ascii_lowercase();
    for part in lower.split('.') {
        let operation = normalize_cross_plane_operation(part);
        if is_known_cross_plane_operation(operation) {
            return Some(operation.to_string());
        }
    }
    lower
        .rsplit('.')
        .next()
        .map(normalize_cross_plane_operation)
        .filter(|operation| !operation.is_empty())
        .map(str::to_string)
}

fn normalize_cross_plane_operation(operation: &str) -> &str {
    match operation.trim() {
        "send_file" | "send_document" => "send_file",
        other => other,
    }
}

fn is_known_cross_plane_operation(operation: &str) -> bool {
    matches!(
        operation,
        "send_text" | "send_image" | "send_file" | "callback" | "qr_login"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-07T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn unknown_actor_is_denied_by_default() {
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default());
        let action = CrossPlaneAction::new("wechat:unknown", "service.mock.docs.export");

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
        assert_eq!(decision.reason, "unknown_actor");
    }

    #[test]
    fn verified_low_risk_action_is_allowed_without_extra_grant() {
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default());
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::Low;

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(decision.reason, "low_risk_verified_actor");
    }

    #[test]
    fn connector_context_missing_scope_denies_before_grant() {
        let grant = CrossPlaneGrant::persistent("user:yi", "service.mock.docs.read");
        let engine =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).with_grants(vec![grant]);
        let mut action = CrossPlaneAction::new("user:yi", "service.mock.docs.read");
        action.identity_trust = IdentityTrust::Verified;
        let context = ConnectorActionContext {
            provider: "mock.docs".to_string(),
            plane: "service".to_string(),
            capability_id: "service.mock.docs.read".to_string(),
            provider_account: Some("mock-docs-main".to_string()),
            account_status: Some("ready".to_string()),
            account_reason: None,
            resource_ref: Some("service://mock.docs/document/doc-1".to_string()),
            required_scopes: vec!["document:read".to_string()],
            missing_scopes: vec!["document:read".to_string()],
            supports_commit: true,
            requires_approval: false,
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            requested_mode: "commit".to_string(),
        };

        let decision = engine.decide_with_connector_context(&action, Some(&context), now());

        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
        assert_eq!(
            decision.reason,
            "connector_missing_account_scope:document:read"
        );
        assert!(decision.matched_grant.is_none());
    }

    #[test]
    fn connector_context_disabled_account_denies_commit() {
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default());
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
        action.identity_trust = IdentityTrust::Verified;
        let context = ConnectorActionContext {
            provider: "mock.docs".to_string(),
            plane: "channel".to_string(),
            capability_id: "channel.chat.send_text".to_string(),
            provider_account: Some("mock-docs-main".to_string()),
            account_status: Some("disabled".to_string()),
            account_reason: Some("platform is disabled".to_string()),
            resource_ref: Some("text://hello".to_string()),
            required_scopes: Vec::new(),
            missing_scopes: Vec::new(),
            supports_commit: true,
            requires_approval: false,
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            requested_mode: "commit".to_string(),
        };

        let decision = engine.decide_with_connector_context(&action, Some(&context), now());

        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
        assert_eq!(decision.reason, "connector_account_disabled");
    }

    #[test]
    fn connector_requires_approval_can_be_allowed_by_matching_grant() {
        let grant = CrossPlaneGrant::persistent("user:yi", "mcp.github_com.tool.create_issue");
        let engine =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).with_grants(vec![grant]);
        let mut action = CrossPlaneAction::new("user:yi", "mcp.github_com.tool.create_issue");
        action.identity_trust = IdentityTrust::Verified;
        let context = ConnectorActionContext {
            provider: "mcp".to_string(),
            plane: "mcp".to_string(),
            capability_id: "mcp.github_com.tool.create_issue".to_string(),
            provider_account: Some("github.com".to_string()),
            account_status: Some("ready".to_string()),
            account_reason: None,
            resource_ref: None,
            required_scopes: Vec::new(),
            missing_scopes: Vec::new(),
            supports_commit: true,
            requires_approval: true,
            risk: CrossPlaneRisk::Medium,
            data_classification: DataClassification::Internal,
            requested_mode: "commit".to_string(),
        };

        let decision = engine.decide_with_connector_context(&action, Some(&context), now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(decision.reason, "matched_grant");
    }

    #[test]
    fn high_risk_requires_approval_without_matching_grant() {
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default());
        let mut action = CrossPlaneAction::new("user:yi", "service.drive.download");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::RequireSingleApproval);
        assert_eq!(decision.required_approval, Some(GrantType::SingleUse));
    }

    #[test]
    fn matching_grant_allows_high_risk_cross_channel_action() {
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.drive.download");
        grant.resource_ref = Some("service://drive/drive/file_1".to_string());
        grant.source_channel = Some("channel://wechat/chat/u1".to_string());

        let engine =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).with_grants(vec![grant]);
        let mut action = CrossPlaneAction::new("user:yi", "service.drive.download");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;
        action.resource_ref = Some("service://drive/drive/file_1".to_string());
        action.source_channel = Some("channel://wechat/chat/u1".to_string());

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(decision.reason, "matched_grant");
        assert!(decision.matched_grant.is_some());
    }

    #[test]
    fn control_plane_consumes_single_use_grant_and_audits_evidence() {
        let control = CrossPlaneControlPlane::new();
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.drive.download");
        grant.grant_type = GrantType::SingleUse;
        grant.remaining_uses = None;
        let grant_id = grant.id.clone();
        control.upsert_grant(grant);

        let mut action = CrossPlaneAction::new("user:yi", "service.drive.download");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;

        let first_decision = control.decide_and_audit(action.clone(), now());
        let second_decision = control.decide_and_audit(action, now());

        assert_eq!(first_decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(
            second_decision.decision,
            PolicyDecisionKind::RequireSingleApproval
        );
        assert_eq!(control.summary(now()).active_grants, 0);

        let audit = control.list_audit(10, 0);
        let first_record = audit
            .iter()
            .find(|record| record.evidence.consumed_grant_id.as_deref() == Some(&grant_id))
            .expect("single-use grant consumption should be auditable");
        assert_eq!(
            first_record.evidence.policy_version,
            "cross-plane.v1".to_string()
        );
        assert_eq!(
            first_record.evidence.matched_grant_id.as_deref(),
            Some(grant_id.as_str())
        );
        assert_eq!(first_record.evidence.remaining_uses_after, Some(0));
    }

    #[test]
    fn control_plane_resolves_cross_channel_identity_by_contact_key() {
        let control = CrossPlaneControlPlane::new();
        control.upsert_identity(CrossPlaneIdentityBinding::verified(
            "user:yi",
            "channel://chat/user/u1?email=Yi@Example.COM",
        ));
        control.upsert_identity(CrossPlaneIdentityBinding {
            id: "idb-observed".to_string(),
            principal_id: "user:other".to_string(),
            identity_ref: "channel://wechat/user/wx1?email=yi@example.com".to_string(),
            trust: IdentityTrust::Observed,
            source: "observed".to_string(),
            created_at: now(),
            expires_at: None,
        });

        let resolved = control
            .resolve_identity("channel://wechat/user/wx-new?email=yi@example.com", now())
            .expect("shared email should resolve to a principal");

        assert_eq!(resolved.principal_id, "user:yi");
        assert_eq!(resolved.trust, IdentityTrust::Verified);
        assert_eq!(resolved.match_kind, "contact_key");
    }

    #[test]
    fn control_plane_resolves_exact_ref_before_contact_key() {
        let control = CrossPlaneControlPlane::new();
        control.upsert_identity(CrossPlaneIdentityBinding::verified(
            "user:yi",
            "channel://chat/user/u1?email=yi@example.com",
        ));
        control.upsert_identity(CrossPlaneIdentityBinding {
            id: "idb-exact".to_string(),
            principal_id: "user:wechat".to_string(),
            identity_ref: "channel://wechat/user/wx1?email=yi@example.com".to_string(),
            trust: IdentityTrust::Claimed,
            source: "manual".to_string(),
            created_at: now(),
            expires_at: None,
        });

        let resolved = control
            .resolve_identity("channel://wechat/user/wx1?email=yi@example.com", now())
            .expect("exact ref should resolve");

        assert_eq!(resolved.principal_id, "user:wechat");
        assert_eq!(resolved.trust, IdentityTrust::Claimed);
        assert_eq!(resolved.match_kind, "exact_ref");
    }

    #[test]
    fn control_plane_decision_resolves_actor_identity_before_policy() {
        let control = CrossPlaneControlPlane::new();
        control.upsert_identity(CrossPlaneIdentityBinding::verified(
            "user:yi",
            "channel://chat/user/u1?email=yi@example.com",
        ));
        control.upsert_grant(CrossPlaneGrant::persistent(
            "user:yi",
            "service.drive.download",
        ));
        let mut action = CrossPlaneAction::new("", "service.drive.download");
        action.actor_identity_ref = Some("channel://wechat/user/wx1?email=yi@example.com".into());
        action.identity_trust = IdentityTrust::Unknown;
        action.risk = CrossPlaneRisk::High;

        let decision = control.decide_and_audit(action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(decision.reason, "matched_grant");
        let audit = control.list_audit(10, 0);
        assert_eq!(audit[0].action.actor_principal, "user:yi");
        assert_eq!(
            audit[0]
                .evidence
                .resolved_identity
                .as_ref()
                .map(|resolved| resolved.match_kind.as_str()),
            Some("contact_key")
        );
    }

    #[test]
    fn control_plane_pure_decision_does_not_consume_single_use_grant_or_audit() {
        let control = CrossPlaneControlPlane::new();
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.drive.download");
        grant.grant_type = GrantType::SingleUse;
        grant.remaining_uses = None;
        control.upsert_grant(grant);
        let mut action = CrossPlaneAction::new("user:yi", "service.drive.download");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;

        let (_action, decision) = control.decide_with_action(action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(control.summary(now()).active_grants, 1);
        assert!(control.list_audit(10, 0).is_empty());
    }

    #[test]
    fn control_plane_audits_connector_evidence_fields() {
        let control = CrossPlaneControlPlane::new();
        let mut action = CrossPlaneAction::new("user:yi", "service.mock.docs.read");
        action.identity_trust = IdentityTrust::Verified;
        let context = ConnectorActionContext {
            provider: "mock.docs".to_string(),
            plane: "service".to_string(),
            capability_id: "service.mock.docs.read".to_string(),
            provider_account: Some("mock-docs-main".to_string()),
            account_status: Some("ready".to_string()),
            account_reason: None,
            resource_ref: Some("service://mock.docs/document/doc-1".to_string()),
            required_scopes: vec!["document:read".to_string()],
            missing_scopes: Vec::new(),
            supports_commit: true,
            requires_approval: false,
            risk: CrossPlaneRisk::Low,
            data_classification: DataClassification::Internal,
            requested_mode: "commit".to_string(),
        };

        let (_action, decision, evidence) =
            control.decide_and_audit_with_connector_context(action, Some(context), now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(
            evidence
                .connector_context
                .as_ref()
                .map(|context| context.capability_id.as_str()),
            Some("service.mock.docs.read")
        );
        let audit = control.list_audit(10, 0);
        let connector = audit[0].evidence.connector_context.as_ref().unwrap();
        assert_eq!(
            connector.provider_account.as_deref(),
            Some("mock-docs-main")
        );
        assert_eq!(
            connector.resource_ref.as_deref(),
            Some("service://mock.docs/document/doc-1")
        );
        assert_eq!(connector.required_scopes, vec!["document:read".to_string()]);
        assert!(connector.missing_scopes.is_empty());
    }

    #[test]
    fn dispatch_target_builds_ready_text_plan_from_channel_ref() {
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
        action.source_channel = Some("channel://wechat/chat/source".to_string());
        action.session_id = Some("session-1".to_string());
        action.target_ref = Some("channel://chat/user/open-id/thread/chat-id".to_string());
        action.resource_ref = Some("text://hello runtime".to_string());

        let target = CrossPlaneDispatchTarget::from_action(&action, Some("chat"), None)
            .expect("dispatchable action should produce a target contract");

        assert!(target.ready);
        assert_eq!(target.platform.as_deref(), Some("chat"));
        assert_eq!(target.operation.as_deref(), Some("send_text"));
        assert_eq!(target.session_key.as_deref(), Some("chat:open-id:chat-id"));
        let outbound = target
            .outbound_message
            .as_ref()
            .expect("ready target should include outbound message");
        assert_eq!(outbound.session_key, "chat:open-id:chat-id");
        assert_eq!(outbound.text, "hello runtime");
        assert_eq!(outbound.payload_kind, "text");
        assert_eq!(outbound.payload_ref, "hello runtime");
        assert_eq!(
            outbound.metadata["requested_capability"],
            "channel.chat.send_text"
        );
    }

    #[test]
    fn dispatch_target_builds_ready_media_plan_without_text_smuggling() {
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_image");
        action.target_ref = Some("channel://chat/chat/open-chat".to_string());
        action.resource_ref = Some("image://https://example.test/diagram.png".to_string());

        let target = CrossPlaneDispatchTarget::from_action(&action, Some("chat"), None)
            .expect("dispatchable image action should produce a target contract");

        assert!(target.ready);
        assert_eq!(target.operation.as_deref(), Some("send_image"));
        let outbound = target.outbound_message.as_ref().unwrap();
        assert_eq!(outbound.payload_kind, "image");
        assert_eq!(outbound.payload_ref, "https://example.test/diagram.png");
        assert_eq!(outbound.text, "https://example.test/diagram.png");
        assert!(outbound.file_name.is_none());
    }

    #[test]
    fn dispatch_target_builds_ready_file_plan_with_file_name() {
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_file");
        action.target_ref = Some("channel://chat/chat/open-chat".to_string());
        action.resource_ref = Some("file:///tmp/runtime-report.pdf".to_string());

        let target = CrossPlaneDispatchTarget::from_action(&action, Some("chat"), None)
            .expect("dispatchable file action should produce a target contract");

        assert!(target.ready);
        let outbound = target.outbound_message.as_ref().unwrap();
        assert_eq!(outbound.payload_kind, "file");
        assert_eq!(outbound.payload_ref, "/tmp/runtime-report.pdf");
        assert_eq!(outbound.file_name.as_deref(), Some("runtime-report.pdf"));
    }

    #[test]
    fn dispatch_target_reports_missing_target_without_panicking() {
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
        action.resource_ref = Some("text://hello runtime".to_string());

        let target = CrossPlaneDispatchTarget::from_action(&action, Some("chat"), None)
            .expect("dispatchable capability still produces diagnostics");

        assert!(!target.ready);
        assert!(target
            .blockers
            .contains(&"dispatch:target_ref_missing".to_string()));
        assert!(target.outbound_message.is_none());
    }

    #[test]
    fn execution_receipt_roundtrips_typed_dispatch_target() {
        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
        action.identity_trust = IdentityTrust::Verified;
        action.target_ref = Some("chat:open-id".to_string());
        action.resource_ref = Some("text://persist me".to_string());
        let decision =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).decide(&action, now());
        let target = CrossPlaneDispatchTarget::from_action(&action, Some("chat"), None);
        let receipt = CrossPlaneExecutionReceipt::new(
            Some("idem-1".to_string()),
            "dry_run",
            "planned",
            "dry_run",
            action,
            decision,
            Vec::new(),
            Some("audit-1".to_string()),
        )
        .with_dispatch_target(target)
        .with_dispatch_outcome(Some(CrossPlaneDispatchOutcome::sent(
            "chat",
            "send_text",
            "chat:open-id",
            Some("om-test".to_string()),
        )));

        let text = serde_json::to_string(&receipt).unwrap();
        let decoded: CrossPlaneExecutionReceipt = serde_json::from_str(&text).unwrap();

        assert_eq!(
            decoded
                .dispatch_target
                .as_ref()
                .and_then(|target| target.session_key.as_deref()),
            Some("chat:open-id")
        );
        assert_eq!(
            decoded
                .dispatch_outcome
                .as_ref()
                .map(|outcome| outcome.status.as_str()),
            Some("sent")
        );
        assert_eq!(
            decoded
                .dispatch_outcome
                .as_ref()
                .and_then(|outcome| outcome.provider_message_id.as_deref()),
            Some("om-test")
        );
    }

    #[test]
    fn expired_grant_does_not_allow_action() {
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.mock.docs.export");
        grant.expires_at = Some(
            DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );

        let engine =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).with_grants(vec![grant]);
        let mut action = CrossPlaneAction::new("user:yi", "service.mock.docs.export");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::RequireSingleApproval);
    }

    #[test]
    fn control_plane_upserts_revokes_and_audits() {
        let control = CrossPlaneControlPlane::new();
        let binding = CrossPlaneIdentityBinding::verified("user:yi", "channel://chat/user/u1");
        let binding_id = binding.id.clone();
        let grant = CrossPlaneGrant::persistent("user:yi", "channel.chat.send_text");
        let grant_id = grant.id.clone();

        control.upsert_identity(binding);
        control.upsert_grant(grant);
        assert_eq!(control.list_identities().len(), 1);
        assert_eq!(control.list_grants().len(), 1);

        let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
        action.identity_trust = IdentityTrust::Verified;
        let decision = control.decide_and_audit(action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(control.list_audit(10, 0).len(), 1);
        assert_eq!(control.summary(now()).verified_identities, 1);
        assert_eq!(control.summary(now()).active_grants, 1);
        assert!(control.revoke_identity(&binding_id));
        assert!(control.revoke_grant(&grant_id));
        assert!(control.list_identities().is_empty());
        assert!(control.list_grants().is_empty());
    }

    #[test]
    fn control_plane_audit_is_bounded_to_recent_records() {
        let control = CrossPlaneControlPlane::new();
        for idx in 0..(MAX_CROSS_PLANE_AUDIT_RECORDS + 3) {
            let mut action = CrossPlaneAction::new("user:yi", "channel.chat.send_text");
            action.session_id = Some(format!("session-{idx}"));
            control.record_audit(CrossPlaneAuditRecord::new(
                action,
                CrossPlanePolicyDecision {
                    decision: PolicyDecisionKind::Allow,
                    reason: "test".to_string(),
                    matched_grant: None,
                    required_approval: None,
                    degrade_to: None,
                },
                "allow",
                "test",
            ));
        }

        let snapshot = control.snapshot();
        assert_eq!(snapshot.audit.len(), MAX_CROSS_PLANE_AUDIT_RECORDS);
        assert_eq!(
            snapshot.audit[0].action.session_id.as_deref(),
            Some("session-3")
        );
    }
}
