//! Single acceptance evaluator for effect-derived acceptance.
//!
//! Host, Agent and Team verify/reducer consumers must read this evaluator's
//! verdict. There is exactly one evaluator; no second path mints a verdict.
//! The evaluator never scans the filesystem and never mutates facts.

use harness_contract::context::{EvidenceObligation, ObservedEvidence};

use crate::path_identity::observed_evidence_collection_satisfies;
use crate::path_identity::WorkspacePathIdentityResolver;

/// The single acceptance evaluator.
#[derive(Debug, Clone, Copy, Default)]
pub struct AcceptanceEvaluator;

impl AcceptanceEvaluator {
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
}
