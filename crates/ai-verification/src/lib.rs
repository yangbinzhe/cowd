//! Verification ledger for Cowd AI work kernel.

use ai_core::{AiKernelError, AiKernelResult, KernelRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimKind {
    CodeChanged,
    TestPassed,
    SourceFact,
    DesignDecision,
    Limitation,
    Inference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Command,
    Test,
    Diff,
    Source,
    ToolResult,
    UserInput,
    Inference,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Pending,
    Supported,
    Unsupported,
    Contradicted,
    NotRun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub id: String,
    pub kind: EvidenceKind,
    pub summary: String,
    pub refs: Vec<KernelRef>,
}

impl Evidence {
    #[must_use]
    pub fn new(kind: EvidenceKind, summary: impl Into<String>) -> Self {
        Self {
            id: format!("evidence-{}", uuid::Uuid::new_v4()),
            kind,
            summary: summary.into(),
            refs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_ref(mut self, reference: KernelRef) -> Self {
        self.refs.push(reference);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub id: String,
    pub kind: ClaimKind,
    pub statement: String,
    pub evidence_ids: Vec<String>,
    pub status: VerificationStatus,
    pub required: bool,
}

impl Claim {
    #[must_use]
    pub fn required(kind: ClaimKind, statement: impl Into<String>) -> Self {
        Self {
            id: format!("claim-{}", uuid::Uuid::new_v4()),
            kind,
            statement: statement.into(),
            evidence_ids: Vec::new(),
            status: VerificationStatus::Pending,
            required: true,
        }
    }

    #[must_use]
    pub fn optional(kind: ClaimKind, statement: impl Into<String>) -> Self {
        Self {
            required: false,
            ..Self::required(kind, statement)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub claim_count: usize,
    pub evidence_count: usize,
    pub unsupported_required_claims: Vec<Claim>,
    pub not_run_claims: Vec<Claim>,
    pub can_finalize: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationLedger {
    pub claims: Vec<Claim>,
    pub evidence: Vec<Evidence>,
}

impl VerificationLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_claim(&mut self, claim: Claim) -> String {
        let id = claim.id.clone();
        self.claims.push(claim);
        id
    }

    pub fn add_evidence(&mut self, evidence: Evidence) -> String {
        let id = evidence.id.clone();
        self.evidence.push(evidence);
        id
    }

    pub fn support_claim(&mut self, claim_id: &str, evidence_id: &str) -> AiKernelResult<()> {
        if !self
            .evidence
            .iter()
            .any(|evidence| evidence.id == evidence_id)
        {
            return Err(AiKernelError::InvalidInput(format!(
                "evidence {evidence_id} not found"
            )));
        }
        let claim = self
            .claims
            .iter_mut()
            .find(|claim| claim.id == claim_id)
            .ok_or_else(|| AiKernelError::InvalidInput(format!("claim {claim_id} not found")))?;
        if !claim.evidence_ids.iter().any(|id| id == evidence_id) {
            claim.evidence_ids.push(evidence_id.to_string());
        }
        claim.status = VerificationStatus::Supported;
        Ok(())
    }

    pub fn mark_not_run(&mut self, claim_id: &str) -> AiKernelResult<()> {
        let claim = self
            .claims
            .iter_mut()
            .find(|claim| claim.id == claim_id)
            .ok_or_else(|| AiKernelError::InvalidInput(format!("claim {claim_id} not found")))?;
        claim.status = VerificationStatus::NotRun;
        Ok(())
    }

    #[must_use]
    pub fn report(&self) -> VerificationReport {
        let unsupported_required_claims = self
            .claims
            .iter()
            .filter(|claim| {
                claim.required
                    && matches!(
                        claim.status,
                        VerificationStatus::Pending
                            | VerificationStatus::Unsupported
                            | VerificationStatus::Contradicted
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        let not_run_claims = self
            .claims
            .iter()
            .filter(|claim| matches!(claim.status, VerificationStatus::NotRun))
            .cloned()
            .collect::<Vec<_>>();
        VerificationReport {
            claim_count: self.claims.len(),
            evidence_count: self.evidence.len(),
            can_finalize: unsupported_required_claims.is_empty(),
            unsupported_required_claims,
            not_run_claims,
        }
    }

    pub fn assert_can_finalize(&self) -> AiKernelResult<VerificationReport> {
        let report = self.report();
        if report.can_finalize {
            Ok(report)
        } else {
            Err(AiKernelError::Degraded(format!(
                "{} required claims lack support",
                report.unsupported_required_claims.len()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_claim_without_evidence_blocks_finalize() {
        let mut ledger = VerificationLedger::new();
        ledger.add_claim(Claim::required(
            ClaimKind::CodeChanged,
            "gateway task kernel migrated",
        ));

        let report = ledger.report();
        assert!(!report.can_finalize);
        assert_eq!(report.unsupported_required_claims.len(), 1);
        assert!(ledger.assert_can_finalize().is_err());
    }

    #[test]
    fn supported_required_claim_allows_finalize() {
        let mut ledger = VerificationLedger::new();
        let claim = ledger.add_claim(Claim::required(
            ClaimKind::TestPassed,
            "ai-task tests passed",
        ));
        let evidence =
            ledger.add_evidence(Evidence::new(EvidenceKind::Test, "cargo test -p ai-task"));

        ledger.support_claim(&claim, &evidence).unwrap();

        let report = ledger.assert_can_finalize().unwrap();
        assert!(report.can_finalize);
        assert_eq!(report.evidence_count, 1);
    }

    #[test]
    fn not_run_claim_is_reported_but_not_treated_as_supported() {
        let mut ledger = VerificationLedger::new();
        let claim = ledger.add_claim(Claim::optional(
            ClaimKind::TestPassed,
            "workspace tests passed",
        ));
        ledger.mark_not_run(&claim).unwrap();

        let report = ledger.report();
        assert!(report.can_finalize);
        assert_eq!(report.not_run_claims.len(), 1);
    }
}
