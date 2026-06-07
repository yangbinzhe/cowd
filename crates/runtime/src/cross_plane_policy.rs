//! Cross-plane identity, grant, and approval policy primitives.
//!
//! This module is intentionally independent from channel adapters and service
//! SDKs. Channels and services submit a `CrossPlaneAction`; the policy engine
//! returns a stable decision that UI, audit, and runtime routing can consume.

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use uuid::Uuid;

const MAX_CROSS_PLANE_AUDIT_RECORDS: usize = 10_000;

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
pub enum CrossPlaneRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataClassification {
    Public,
    Internal,
    Confidential,
    Secret,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneDecisionEvidence {
    pub policy_version: String,
    pub evaluated_at: Option<DateTime<Utc>>,
    pub resolved_identity: Option<CrossPlaneResolvedIdentity>,
    pub active_grants_before: usize,
    pub matched_grant_id: Option<String>,
    pub consumed_grant_id: Option<String>,
    pub remaining_uses_after: Option<u32>,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrossPlaneControlSnapshot {
    pub identities: Vec<CrossPlaneIdentityBinding>,
    pub grants: Vec<CrossPlaneGrant>,
    pub audit: Vec<CrossPlaneAuditRecord>,
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
        }
    }

    pub fn replace_snapshot(&self, snapshot: CrossPlaneControlSnapshot) {
        let mut state = self.inner.write().unwrap_or_else(|err| err.into_inner());
        state.identities = snapshot.identities;
        state.grants = snapshot.grants;
        state.audit = snapshot.audit;
    }

    pub fn load_from_path(&self, path: &Path) -> Result<(), String> {
        if !path.exists() {
            return Ok(());
        }
        let text = fs::read_to_string(path)
            .map_err(|err| format!("failed to read cross-plane state: {err}"))?;
        let snapshot = serde_json::from_str::<CrossPlaneControlSnapshot>(&text)
            .map_err(|err| format!("failed to parse cross-plane state: {err}"))?;
        self.replace_snapshot(snapshot);
        Ok(())
    }

    pub fn save_to_path(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create cross-plane state dir: {err}"))?;
        }
        let text = serde_json::to_string_pretty(&self.snapshot())
            .map_err(|err| format!("failed to encode cross-plane state: {err}"))?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("control-state.json");
        let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", Uuid::new_v4()));
        fs::write(&tmp_path, text)
            .map_err(|err| format!("failed to write cross-plane state temp file: {err}"))?;
        if let Err(err) = fs::rename(&tmp_path, path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(format!("failed to replace cross-plane state: {err}"));
        }
        Ok(())
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
    pub fn decide_and_audit(
        &self,
        action: CrossPlaneAction,
        now: DateTime<Utc>,
    ) -> CrossPlanePolicyDecision {
        self.decide_and_audit_with_action(action, now).1
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
        let consumed = self.consume_matched_grant_if_needed(&decision);
        let evidence = CrossPlaneDecisionEvidence {
            policy_version: "cross-plane.v1".to_string(),
            evaluated_at: Some(now),
            resolved_identity,
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

    fn consume_matched_grant_if_needed(
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
    static EMAIL_RE: OnceLock<Regex> = OnceLock::new();
    let email_re = EMAIL_RE.get_or_init(|| {
        Regex::new(r"(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}").expect("valid email regex")
    });
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
        let action = CrossPlaneAction::new("wechat:unknown", "service.feishu.docx.export");

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Deny);
        assert_eq!(decision.reason, "unknown_actor");
    }

    #[test]
    fn verified_low_risk_action_is_allowed_without_extra_grant() {
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default());
        let mut action = CrossPlaneAction::new("user:yi", "channel.feishu.send_text");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::Low;

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(decision.reason, "low_risk_verified_actor");
    }

    #[test]
    fn high_risk_requires_approval_without_matching_grant() {
        let engine = CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default());
        let mut action = CrossPlaneAction::new("user:yi", "service.feishu.drive.download");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::RequireSingleApproval);
        assert_eq!(decision.required_approval, Some(GrantType::SingleUse));
    }

    #[test]
    fn matching_grant_allows_high_risk_cross_channel_action() {
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.feishu.drive.download");
        grant.resource_ref = Some("service://feishu/drive/file_1".to_string());
        grant.source_channel = Some("channel://wechat/chat/u1".to_string());

        let engine =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).with_grants(vec![grant]);
        let mut action = CrossPlaneAction::new("user:yi", "service.feishu.drive.download");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;
        action.resource_ref = Some("service://feishu/drive/file_1".to_string());
        action.source_channel = Some("channel://wechat/chat/u1".to_string());

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::Allow);
        assert_eq!(decision.reason, "matched_grant");
        assert!(decision.matched_grant.is_some());
    }

    #[test]
    fn control_plane_consumes_single_use_grant_and_audits_evidence() {
        let control = CrossPlaneControlPlane::new();
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.feishu.drive.download");
        grant.grant_type = GrantType::SingleUse;
        grant.remaining_uses = None;
        let grant_id = grant.id.clone();
        control.upsert_grant(grant);

        let mut action = CrossPlaneAction::new("user:yi", "service.feishu.drive.download");
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
            "channel://feishu/user/u1?email=Yi@Example.COM",
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
            "channel://feishu/user/u1?email=yi@example.com",
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
            "channel://feishu/user/u1?email=yi@example.com",
        ));
        control.upsert_grant(CrossPlaneGrant::persistent(
            "user:yi",
            "service.feishu.drive.download",
        ));
        let mut action = CrossPlaneAction::new("", "service.feishu.drive.download");
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
    fn expired_grant_does_not_allow_action() {
        let mut grant = CrossPlaneGrant::persistent("user:yi", "service.feishu.docx.export");
        grant.expires_at = Some(
            DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );

        let engine =
            CrossPlanePolicyEngine::new(CrossPlanePolicyConfig::default()).with_grants(vec![grant]);
        let mut action = CrossPlaneAction::new("user:yi", "service.feishu.docx.export");
        action.identity_trust = IdentityTrust::Verified;
        action.risk = CrossPlaneRisk::High;

        let decision = engine.decide(&action, now());

        assert_eq!(decision.decision, PolicyDecisionKind::RequireSingleApproval);
    }

    #[test]
    fn control_plane_upserts_revokes_and_audits() {
        let control = CrossPlaneControlPlane::new();
        let binding = CrossPlaneIdentityBinding::verified("user:yi", "channel://feishu/user/u1");
        let binding_id = binding.id.clone();
        let grant = CrossPlaneGrant::persistent("user:yi", "channel.feishu.send_text");
        let grant_id = grant.id.clone();

        control.upsert_identity(binding);
        control.upsert_grant(grant);
        assert_eq!(control.list_identities().len(), 1);
        assert_eq!(control.list_grants().len(), 1);

        let mut action = CrossPlaneAction::new("user:yi", "channel.feishu.send_text");
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
    fn control_plane_snapshot_roundtrips_to_json_file() {
        let control = CrossPlaneControlPlane::new();
        control.upsert_identity(CrossPlaneIdentityBinding::verified(
            "user:yi",
            "channel://wechat/user/u1",
        ));
        control.upsert_grant(CrossPlaneGrant::persistent(
            "user:yi",
            "service.feishu.docx.read",
        ));
        let mut action = CrossPlaneAction::new("user:yi", "service.feishu.docx.read");
        action.identity_trust = IdentityTrust::Verified;
        let _ = control.decide_and_audit(action, now());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cross-plane.json");
        control.save_to_path(&path).unwrap();

        let restored = CrossPlaneControlPlane::new();
        restored.load_from_path(&path).unwrap();

        assert_eq!(restored.list_identities().len(), 1);
        assert_eq!(restored.list_grants().len(), 1);
        assert_eq!(restored.list_audit(10, 0).len(), 1);
    }

    #[test]
    fn control_plane_audit_is_bounded_to_recent_records() {
        let control = CrossPlaneControlPlane::new();
        for idx in 0..(MAX_CROSS_PLANE_AUDIT_RECORDS + 3) {
            let mut action = CrossPlaneAction::new("user:yi", "channel.feishu.send_text");
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

    #[test]
    fn control_plane_save_is_atomic_and_cleans_temp_files() {
        let control = CrossPlaneControlPlane::new();
        control.upsert_identity(CrossPlaneIdentityBinding::verified(
            "user:yi",
            "channel://wechat/user/u1",
        ));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control-state.json");

        control.save_to_path(&path).unwrap();

        assert!(path.exists());
        let temp_files = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .count();
        assert_eq!(temp_files, 0);

        let restored = CrossPlaneControlPlane::new();
        restored.load_from_path(&path).unwrap();
        assert_eq!(restored.list_identities().len(), 1);
    }
}
