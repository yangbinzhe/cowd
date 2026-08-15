//! Stable identity and decision-authorization wire contracts.
//!
//! These types intentionally carry claims only. They are not proof of an
//! authenticated identity; verification and privileged capabilities remain in
//! the runtime security boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Product-neutral capabilities exposed by Cowd's core human-manager profile.
///
/// The authorization catalogue always includes this stable core set when it
/// derives a Surface projection. APP capabilities remain catalogue-derived;
/// a Surface must not reconstruct or hard-code either set.
pub const CORE_HUMAN_CAPABILITIES: &[&str] = &[
    "approval.respond",
    "definition.manage",
    "definition.default.set",
    "definition.rollback",
    "evolution.candidate.register",
    "evolution.diagnosis.write",
    "evolution.analyze.run",
    "evolution.evaluate.run",
    "evolution.release.manage",
    "evolution.review.request",
    "evolution.signal.write",
    "mission.observe",
    "runtime.maintenance.manage",
    "skill.revision.manage",
    "runtime.outbox.retry",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Human,
    Service,
    Agent,
    Anonymous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalAssurance {
    None,
    Normal,
    HumanInteractive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalClaims {
    pub principal_id: String,
    /// Stable deployment authority identifier. It is signed by AuthBroker and
    /// is never accepted from an HTTP or APP payload.
    pub tenant_id: String,
    /// Unique signed authorization grant for this issued principal envelope.
    pub grant_id: String,
    pub kind: PrincipalKind,
    pub scopes: Vec<String>,
    pub capabilities: Vec<String>,
    pub assurance: PrincipalAssurance,
    pub issuer: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub credential_fingerprint: String,
    pub credential_epoch: u64,
    pub profile_revision: u64,
    /// Effective APP profile selections captured at issuance time.
    pub app_profiles: BTreeMap<String, String>,
}

const fn default_profile_revision() -> u64 {
    1
}

impl PrincipalClaims {
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            principal_id: "anonymous".to_string(),
            tenant_id: String::new(),
            grant_id: String::new(),
            kind: PrincipalKind::Anonymous,
            scopes: Vec::new(),
            capabilities: Vec::new(),
            assurance: PrincipalAssurance::None,
            issuer: "cowd.gateway".to_string(),
            issued_at_ms: 0,
            expires_at_ms: None,
            credential_fingerprint: "anonymous".to_string(),
            credential_epoch: 0,
            profile_revision: default_profile_revision(),
            app_profiles: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|item| item == capability)
    }

    #[must_use]
    pub fn is_human_interactive(&self) -> bool {
        self.kind == PrincipalKind::Human && self.assurance == PrincipalAssurance::HumanInteractive
    }

    #[must_use]
    pub fn app_profile(&self, app_id: &str) -> Option<&str> {
        self.app_profiles.get(app_id).map(String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionLeaseClaims {
    pub lease_id: String,
    pub principal_id: String,
    pub review_id: String,
    pub action: String,
    pub scope: String,
    pub evidence_digest: String,
    /// Authority that signed this lease.  Runtime verifies this against the
    /// pinned broker key rather than trusting a caller-supplied review claim.
    pub issuer: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub credential_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedPrincipalEnvelope {
    pub key_id: String,
    pub claims: PrincipalClaims,
    pub signature_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedDecisionLease {
    pub key_id: String,
    pub claims: DecisionLeaseClaims,
    pub signature_base64: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_claims_have_no_privileged_capabilities() {
        let claims = PrincipalClaims::anonymous();
        assert!(!claims.is_human_interactive());
        assert!(!claims.has_capability("approval.respond"));
    }
}
