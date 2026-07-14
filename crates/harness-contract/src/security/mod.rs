//! Stable identity and decision-authorization wire contracts.
//!
//! These types intentionally carry claims only. They are not proof of an
//! authenticated identity; verification and privileged capabilities remain in
//! the runtime security boundary.

use serde::{Deserialize, Serialize};

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
mod tests {
    use super::*;

    #[test]
    fn anonymous_claims_have_no_privileged_capabilities() {
        let claims = PrincipalClaims::anonymous();
        assert!(!claims.is_human_interactive());
        assert!(!claims.has_capability("approval.respond"));
    }
}
