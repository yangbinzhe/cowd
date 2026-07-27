//! Verification ledger for Cowd AI work kernel.

use crate::{
    core::{AiKernelError, AiKernelResult, KernelRef},
    reality::{ClaimSupportState, EvidenceCompleteness, EvidenceRef},
};
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
    Both,
    NotRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationSeverity {
    Clear,
    Advisory,
    Blocking,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Evidence {
    pub id: String,
    pub kind: EvidenceKind,
    pub summary: String,
    pub refs: Vec<EvidenceRef>,
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
    pub fn with_ref(mut self, reference: impl Into<EvidenceRef>) -> Self {
        self.refs.push(reference.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub id: String,
    pub kind: ClaimKind,
    pub statement: String,
    pub evidence_ids: Vec<String>,
    #[serde(default)]
    pub counter_evidence_ids: Vec<String>,
    #[serde(default)]
    pub support: ClaimSupportState,
    #[serde(default)]
    pub completeness: EvidenceCompleteness,
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
            counter_evidence_ids: Vec::new(),
            support: ClaimSupportState::Unknown,
            completeness: EvidenceCompleteness::None,
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
    pub severity: VerificationSeverity,
    pub blocking_reasons: Vec<String>,
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
        claim.support = claim.support.combine(ClaimSupportState::Supported);
        claim.completeness = EvidenceCompleteness::Partial;
        claim.status = status_from_support(claim.support);
        Ok(())
    }

    pub fn contradict_claim(&mut self, claim_id: &str, evidence_id: &str) -> AiKernelResult<()> {
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
        if !claim
            .counter_evidence_ids
            .iter()
            .any(|id| id == evidence_id)
        {
            claim.counter_evidence_ids.push(evidence_id.to_string());
        }
        claim.support = claim.support.combine(ClaimSupportState::Contradicted);
        claim.completeness = EvidenceCompleteness::Partial;
        claim.status = status_from_support(claim.support);
        Ok(())
    }

    pub fn set_completeness(
        &mut self,
        claim_id: &str,
        completeness: EvidenceCompleteness,
    ) -> AiKernelResult<()> {
        let claim = self
            .claims
            .iter_mut()
            .find(|claim| claim.id == claim_id)
            .ok_or_else(|| AiKernelError::InvalidInput(format!("claim {claim_id} not found")))?;
        claim.completeness = completeness;
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
                            | VerificationStatus::Both
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
        let mut blocking_reasons = unsupported_required_claims
            .iter()
            .map(|claim| format!("required claim lacks support: {}", claim.statement))
            .collect::<Vec<_>>();
        if self.claims.is_empty() {
            blocking_reasons.push("verification ledger has no claims".to_string());
        }
        let can_finalize = !self.claims.is_empty() && unsupported_required_claims.is_empty();
        let severity = if !can_finalize {
            VerificationSeverity::Blocking
        } else if !not_run_claims.is_empty() || self.evidence.is_empty() {
            VerificationSeverity::Advisory
        } else {
            VerificationSeverity::Clear
        };
        VerificationReport {
            claim_count: self.claims.len(),
            evidence_count: self.evidence.len(),
            can_finalize,
            unsupported_required_claims,
            not_run_claims,
            severity,
            blocking_reasons,
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

fn status_from_support(support: ClaimSupportState) -> VerificationStatus {
    match support {
        ClaimSupportState::Unknown => VerificationStatus::Pending,
        ClaimSupportState::Supported => VerificationStatus::Supported,
        ClaimSupportState::Contradicted => VerificationStatus::Contradicted,
        ClaimSupportState::Both => VerificationStatus::Both,
    }
}

impl From<KernelRef> for EvidenceRef {
    fn from(reference: KernelRef) -> Self {
        EvidenceRef::unknown(reference.ref_type, reference.id)
            .with_source(reference.label.unwrap_or_else(|| "ai_kernel".to_string()))
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
            "runtime task tests passed",
        ));
        let evidence = ledger.add_evidence(Evidence::new(
            EvidenceKind::Test,
            "cargo test -p runtime task",
        ));

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

    #[test]
    fn support_and_contradiction_are_order_independent() {
        for reverse in [false, true] {
            let mut ledger = VerificationLedger::new();
            let claim = ledger.add_claim(Claim::required(
                ClaimKind::SourceFact,
                "claim has mixed evidence",
            ));
            let support =
                ledger.add_evidence(Evidence::new(EvidenceKind::Source, "supporting source"));
            let contradiction =
                ledger.add_evidence(Evidence::new(EvidenceKind::Source, "counter source"));
            if reverse {
                ledger.contradict_claim(&claim, &contradiction).unwrap();
                ledger.support_claim(&claim, &support).unwrap();
            } else {
                ledger.support_claim(&claim, &support).unwrap();
                ledger.contradict_claim(&claim, &contradiction).unwrap();
            }
            assert_eq!(ledger.claims[0].support, ClaimSupportState::Both);
            assert_eq!(ledger.claims[0].status, VerificationStatus::Both);
            assert!(!ledger.report().can_finalize);
        }
    }

    #[test]
    fn empty_ledger_cannot_finalize() {
        let report = VerificationLedger::new().report();
        assert!(!report.can_finalize);
        assert!(report
            .blocking_reasons
            .iter()
            .any(|reason| reason.contains("no claims")));
    }
}
