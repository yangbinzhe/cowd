//! Conversion from runtime receipts to stable audit projections.

use harness_contract::context::{EvidenceAccessRef, EvidenceAuditProjection};

use super::ModelReceipt;

#[must_use]
pub fn audit_projection(
    receipt: &ModelReceipt,
    access: Option<&EvidenceAccessRef>,
) -> EvidenceAuditProjection {
    let durable_access = access.filter(|candidate| {
        candidate.is_durable() && candidate.evidence_ref == receipt.evidence_ref
    });
    EvidenceAuditProjection {
        evidence_ref: receipt.evidence_ref.clone(),
        content_kind: receipt.content_kind,
        raw_tokens: receipt.raw_tokens,
        receipt_tokens: receipt.receipt_tokens,
        omitted_tokens: receipt.omitted_tokens,
        raw_available: durable_access.is_some(),
        access: durable_access.cloned(),
    }
}

#[cfg(test)]
mod tests {
    use harness_contract::context::{EvidenceAccessRef, EvidenceContentKind};
    use harness_contract::reality::EvidenceRef;

    use super::*;

    fn receipt() -> ModelReceipt {
        ModelReceipt {
            evidence_ref: EvidenceRef::observed("tool", "raw-1"),
            content_kind: EvidenceContentKind::Text,
            summary: "receipt".to_string(),
            raw_tokens: 100,
            receipt_tokens: 10,
            omitted_tokens: 90,
            truncated: true,
        }
    }

    #[test]
    fn raw_is_available_only_after_matching_durable_receipt() {
        let receipt = receipt();
        assert!(!audit_projection(&receipt, None).raw_available);

        let access = EvidenceAccessRef::durable(
            receipt.evidence_ref.clone(),
            "sha256:raw",
            300,
            "text/plain",
            "artifact://art_projection_1",
            "session:s1",
        );
        let projection = audit_projection(&receipt, Some(&access));
        assert!(projection.raw_available);
        assert_eq!(projection.access, Some(access));
    }

    #[test]
    fn mismatched_durable_reference_is_not_exposed() {
        let receipt = receipt();
        let access = EvidenceAccessRef::durable(
            EvidenceRef::observed("tool", "other"),
            "sha256:raw",
            300,
            "text/plain",
            "artifact://art_projection_2",
            "session:s1",
        );
        assert!(!audit_projection(&receipt, Some(&access)).raw_available);
    }
}
