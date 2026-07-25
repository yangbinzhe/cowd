//! Stable identity and decision-authorization wire contracts.
//!
//! These types intentionally carry claims only. They are not proof of an
//! authenticated identity; verification and privileged capabilities remain in
//! the runtime security boundary.

use serde::{Deserialize, Serialize};

/// Product-neutral capabilities exposed by Cowd's core human-manager profile.
///
/// Interactive Surfaces request only this stable core set during principal
/// issuance. APP capabilities remain catalogue-derived and must not be
/// reconstructed by a Surface.
pub const CORE_HUMAN_CAPABILITIES: &[&str] = &[
    "approval.respond",
    "definition.manage",
    "definition.default.set",
    "definition.rollback",
    "evolution.release.manage",
    "mission.observe",
    "runtime.maintenance.manage",
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
pub struct PrincipalClaims {
    pub principal_id: String,
    pub kind: PrincipalKind,
    pub scopes: Vec<String>,
    pub capabilities: Vec<String>,
    pub assurance: PrincipalAssurance,
    pub issuer: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub credential_fingerprint: String,
    pub credential_epoch: u64,
    #[serde(
        default = "default_profile_revision",
        skip_serializing_if = "is_default_profile_revision"
    )]
    pub profile_revision: u64,
}

const fn default_profile_revision() -> u64 {
    1
}

fn is_default_profile_revision(revision: &u64) -> bool {
    *revision == default_profile_revision()
}

impl PrincipalClaims {
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            principal_id: "anonymous".to_string(),
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
mod compatibility_tests {
    use super::*;

    #[test]
    fn profile_revision_preserves_v1_signed_claim_serialization() {
        let legacy = serde_json::json!({
            "principal_id": "legacy-human",
            "kind": "human",
            "scopes": ["gateway"],
            "capabilities": ["mfg.read"],
            "assurance": "human_interactive",
            "issuer": "cowd.local-auth-broker.v1",
            "issued_at_ms": 1,
            "expires_at_ms": null,
            "credential_fingerprint": "sha256:legacy",
            "credential_epoch": 1
        });
        let claims: PrincipalClaims = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(claims.profile_revision, 1);
        assert_eq!(serde_json::to_value(claims).unwrap(), legacy);
    }

    #[test]
    fn non_default_profile_revision_is_part_of_the_signed_claim() {
        let mut claims = PrincipalClaims::anonymous();
        claims.profile_revision = 7;
        assert_eq!(serde_json::to_value(claims).unwrap()["profile_revision"], 7);
    }
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
