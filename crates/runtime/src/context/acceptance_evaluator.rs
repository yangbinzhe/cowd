//! Single acceptance evaluator for effect-derived acceptance.
//!
//! Host, Agent and Team verify/reducer consumers must read this evaluator's
//! verdict. There is exactly one evaluator; no second path mints a verdict.
//! The evaluator never scans the filesystem and never mutates facts.

use harness_contract::acceptance::{AcceptanceEvaluation, AcceptanceVerdict};
use harness_contract::context::{
    EvidenceObligation, ObservedAcceptance, ObservedEvidence, RequiredAcceptance,
};
use sha2::{Digest, Sha256};

use crate::path_identity::observed_evidence_collection_satisfies;
use crate::path_identity::WorkspacePathIdentityResolver;

/// The single acceptance evaluator.
#[derive(Debug, Clone, Copy, Default)]
pub struct AcceptanceEvaluator;

impl AcceptanceEvaluator {
    /// Bump this only when the canonical matching semantics change.  A
    /// consumer may reject an unknown revision, but it must never silently
    /// substitute a locally reimplemented matcher.
    pub const REVISION: u64 = 1;

    /// Preserve the frozen packet's legacy structured criteria only when the
    /// typed requirement carrier is genuinely absent.  This is a terminal
    /// producer migration aid, not a consumer-side recompiler.
    #[must_use]
    pub fn effective_required(
        required: &RequiredAcceptance,
        fallback_criteria: &[String],
    ) -> RequiredAcceptance {
        if required.is_empty() {
            RequiredAcceptance {
                criteria: fallback_criteria.to_vec(),
                evidence_obligations: Vec::new(),
            }
        } else {
            required.clone()
        }
    }

    /// Compile raw acceptance scopes into typed obligations without losing
    /// ambiguity or unavailability.
    pub fn derive_obligations(
        resolver: &WorkspacePathIdentityResolver,
        raw_scopes: &[String],
    ) -> Vec<EvidenceObligation> {
        raw_scopes
            .iter()
            .map(|scope| resolver.compile_obligation_or_unresolved(scope))
            .collect()
    }

    /// Decide whether one obligation is satisfied by Runtime-attested
    /// evidence. `verify_after_write` obligations derive per-committed
    /// descendant exact post-write reads; other kinds keep their existing
    /// directional coverage semantics.
    #[must_use]
    pub fn evaluate(obligation: &EvidenceObligation, evidence: &[ObservedEvidence]) -> bool {
        observed_evidence_collection_satisfies(obligation, evidence)
    }

    /// Evaluate the complete frozen acceptance contract.  Every Runtime
    /// terminal producer goes through this method; callers may contribute
    /// only deterministic structured-field criteria, never a second
    /// obligation matcher or filesystem observation.
    #[must_use]
    pub fn evaluate_required(
        required: &RequiredAcceptance,
        satisfied_criteria: Vec<String>,
        observed_evidence: Vec<ObservedEvidence>,
    ) -> ObservedAcceptance {
        Self::evaluate_terminal(required, satisfied_criteria, observed_evidence).0
    }

    /// Evaluate exactly one frozen acceptance contract and the complete
    /// receipt-derived observation set.  The returned digest pair is the
    /// durable identity consumed by graph dependencies and Team reduction;
    /// callers may display raw observations but cannot mint a second verdict.
    #[must_use]
    pub fn evaluate_terminal(
        required: &RequiredAcceptance,
        mut satisfied_criteria: Vec<String>,
        mut observed_evidence: Vec<ObservedEvidence>,
    ) -> (ObservedAcceptance, AcceptanceEvaluation) {
        satisfied_criteria.sort();
        satisfied_criteria.dedup();
        observed_evidence.sort_by(|left, right| {
            crate::path_identity::observed_evidence_fingerprint(left)
                .cmp(&crate::path_identity::observed_evidence_fingerprint(right))
        });
        observed_evidence.dedup_by(|left, right| {
            crate::path_identity::observed_evidence_fingerprint(left)
                == crate::path_identity::observed_evidence_fingerprint(right)
        });
        let unresolved_obligation_ids = required
            .evidence_obligations
            .iter()
            .filter(|obligation| !Self::evaluate(obligation, &observed_evidence))
            .map(|obligation| obligation.obligation_id.clone())
            .collect::<Vec<_>>();
        let observed = ObservedAcceptance {
            satisfied_criteria,
            observed_evidence,
            unresolved_obligation_ids,
        };
        let criteria_satisfied = required
            .criteria
            .iter()
            .all(|criterion| observed.satisfied_criteria.contains(criterion));
        let obligations_satisfied = observed.unresolved_obligation_ids.is_empty();
        let verdict = if required.is_empty() {
            AcceptanceVerdict::Satisfied
        } else if observed.is_empty() {
            // This is distinguishable from an evaluated, unsatisfied contract
            // through the durable evaluation envelope itself.  It means the
            // terminal producer had no Runtime-attested receipt snapshot.
            AcceptanceVerdict::Unresolved
        } else if criteria_satisfied && obligations_satisfied {
            AcceptanceVerdict::Satisfied
        } else {
            AcceptanceVerdict::Unsatisfied
        };
        let derived_obligations = required
            .evidence_obligations
            .iter()
            .map(|obligation| obligation.obligation_id.clone())
            .collect::<Vec<_>>();
        let evaluation = AcceptanceEvaluation {
            evaluator_revision: Self::REVISION,
            contract_digest: digest_json(required),
            receipt_set_digest: digest_json(&observed),
            derived_obligations,
            verdict,
        };
        (observed, evaluation)
    }
}

fn digest_json<T: serde::Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_evaluation_is_order_independent_and_durable() {
        let required = RequiredAcceptance {
            criteria: vec!["summary".to_string()],
            evidence_obligations: Vec::new(),
        };
        let (left_observed, left) = AcceptanceEvaluator::evaluate_terminal(
            &required,
            vec!["summary".to_string(), "summary".to_string()],
            Vec::new(),
        );
        let (right_observed, right) = AcceptanceEvaluator::evaluate_terminal(
            &required,
            vec!["summary".to_string()],
            Vec::new(),
        );
        assert_eq!(left_observed, right_observed);
        assert_eq!(left, right);
        assert_eq!(left.verdict, AcceptanceVerdict::Satisfied);
        assert_eq!(left.evaluator_revision, AcceptanceEvaluator::REVISION);
    }
}
