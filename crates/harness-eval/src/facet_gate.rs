//! PLAN Phase 4 / L1: a facet's acceptance must test the facet.
//!
//! # Why
//!
//! Both single-facet probes were declared failed by `ArchitectureQuality` - a
//! gate for a *multi-team architecture review* - even when the model had
//! answered correctly:
//!
//! * **arm E** (`live_minimal_literal_read`) answered with three verbatim source
//!   quotes in 3 rounds and was still `failed` on `architecture_quality`.
//! * **Facet B** (`live_minimal_compute_reduce`) answered 1125 files / 771040
//!   lines with cited receipts and kept the unknown unknown unresolved, again
//!   `failed` on `architecture_quality`.
//!
//! A gate that cannot pass a correct answer is not measuring anything: it turns
//! "the model did the job" into "the model failed" and makes every facet's
//! result unusable. That is the same class of defect as the wall-clock verdict -
//! the measurement, not the work.
//!
//! # What a facet acceptance is
//!
//! A facet tests ONE capability on ONE side. Its acceptance therefore states
//! exactly what that capability must have produced, and nothing else:
//!
//! * **evidence**: which durable tool receipts must exist (which paths were
//!   actually read), so "the model claims it read X" and "the model read X" are
//!   distinguishable;
//! * **answer**: what the final answer must contain, if anything;
//! * **`keep_unresolved`**: whether leaving something unresolved is acceptable.
//!   For a facet whose whole point is honest reporting, that must be true, or the
//!   gate punishes the behaviour it is trying to teach.
//!
//! It deliberately does NOT require teams, tasks, reviews or any collaboration
//! shape: a facet that needs those is not a facet, it is the medium scenario, and
//! the medium scenario has its own gate.

use serde_json::{json, Value};

/// What one capability facet must have produced to pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetAcceptance {
    /// Identifier of the facet, for the report.
    pub facet_id: String,
    /// Human statement of the single capability under test.
    pub capability: String,
    /// Paths that must have a durable read receipt. Empty means "not gated on
    /// reads".
    pub required_read_paths: Vec<String>,
    /// Substrings the final answer must contain. Empty means "not gated on
    /// wording".
    pub answer_contains: Vec<String>,
    /// Whether leaving part of the objective unresolved is acceptable.
    ///
    /// True for facets whose point is honest reporting. A gate that requires a
    /// complete answer from a task that cannot be completed teaches the model to
    /// invent the missing part.
    pub keep_unresolved_ok: bool,
}

/// The observable outcome of one facet run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FacetOutcome {
    /// Paths that actually have a durable read receipt.
    pub read_paths: Vec<String>,
    /// The final answer text.
    pub answer: String,
    /// Whether the model marked something unresolved.
    pub marked_unresolved: bool,
}

/// Result of gating one facet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetVerdict {
    /// Whether the facet passed.
    pub passed: bool,
    /// Per-requirement outcomes, so a failure says which requirement missed.
    pub requirements: Vec<FacetRequirement>,
}

/// One requirement and whether it held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRequirement {
    /// What was required.
    pub requirement: String,
    /// Whether it held.
    pub passed: bool,
    /// What was observed, when it did not.
    pub observed: String,
}

impl FacetVerdict {
    /// Machine-readable report payload.
    #[must_use]
    pub fn report(&self) -> Value {
        json!({
            "passed": self.passed,
            "requirements": self
                .requirements
                .iter()
                .map(|requirement| json!({
                    "requirement": requirement.requirement,
                    "passed": requirement.passed,
                    "observed": requirement.observed,
                }))
                .collect::<Vec<_>>(),
        })
    }
}

/// Gate one facet's outcome.
///
/// Pure: the same outcome and acceptance always produce the same verdict, which
/// is what makes a facet's result comparable across builds.
#[must_use]
pub fn evaluate_facet(acceptance: &FacetAcceptance, outcome: &FacetOutcome) -> FacetVerdict {
    let mut requirements = Vec::new();

    // (1) An answer must exist. A facet that produced nothing has not
    // demonstrated its capability, whatever the evidence says.
    requirements.push(FacetRequirement {
        requirement: "answer_present".to_string(),
        passed: !outcome.answer.trim().is_empty(),
        observed: format!("answer length {}", outcome.answer.trim().len()),
    });

    // (2) Required evidence must exist as durable receipts, not as claims.
    for path in &acceptance.required_read_paths {
        let found = outcome
            .read_paths
            .iter()
            .any(|read| read == path || read.ends_with(path));
        requirements.push(FacetRequirement {
            requirement: format!("read_receipt:{path}"),
            passed: found,
            observed: if found {
                format!("durable receipt for {path}")
            } else {
                format!("no durable receipt for {path} (have: {})", outcome.read_paths.join(", "))
            },
        });
    }

    // (3) The answer must contain what the facet asked for.
    for needle in &acceptance.answer_contains {
        let found = outcome.answer.contains(needle.as_str());
        requirements.push(FacetRequirement {
            requirement: format!("answer_contains:{needle}"),
            passed: found,
            observed: if found {
                "present".to_string()
            } else {
                "absent".to_string()
            },
        });
    }

    // (4) Unresolved is only a failure when the facet did not allow it.
    if !acceptance.keep_unresolved_ok && outcome.marked_unresolved {
        requirements.push(FacetRequirement {
            requirement: "no_unresolved".to_string(),
            passed: false,
            observed: "the answer marked something unresolved".to_string(),
        });
    }

    let passed = requirements.iter().all(|requirement| requirement.passed);
    FacetVerdict {
        passed,
        requirements,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acceptance() -> FacetAcceptance {
        FacetAcceptance {
            facet_id: "live_minimal_literal_read".to_string(),
            capability: "read a known file and quote it verbatim".to_string(),
            required_read_paths: vec![
                "crates/memory/src/lib.rs".to_string(),
                "crates/provider/src/lib.rs".to_string(),
            ],
            answer_contains: vec!["pub mod".to_string()],
            keep_unresolved_ok: true,
        }
    }

    fn outcome() -> FacetOutcome {
        FacetOutcome {
            read_paths: vec![
                "crates/memory/src/lib.rs".to_string(),
                "crates/provider/src/lib.rs".to_string(),
                "crates/harness-eval/src/lib.rs".to_string(),
            ],
            answer: "pub mod aaak_compression;".to_string(),
            marked_unresolved: false,
        }
    }

    /// The arm-E failure, fixed: a correct answer with durable receipts passes.
    #[test]
    fn a_correct_answer_with_durable_receipts_passes() {
        let verdict = evaluate_facet(&acceptance(), &outcome());
        assert!(
            verdict.passed,
            "a facet that produced its evidence must pass: {}",
            verdict.report()
        );
        // answer_present + 2 read receipts + 1 answer_contains, and no
        // unresolved requirement because this facet allows it.
        assert_eq!(verdict.requirements.len(), 4);
    }

    /// A claim without a receipt is not evidence. This is the distinction the
    /// whole gate exists to keep.
    #[test]
    fn a_claimed_read_without_a_receipt_fails() {
        let mut bad = outcome();
        bad.read_paths = vec!["crates/harness-eval/src/lib.rs".to_string()];
        let verdict = evaluate_facet(&acceptance(), &bad);
        assert!(!verdict.passed);
        let failed = verdict
            .requirements
            .iter()
            .find(|requirement| !requirement.passed)
            .expect("a failure");
        assert!(failed.requirement.starts_with("read_receipt:"));
        assert!(failed.observed.contains("no durable receipt"));
    }

    /// An empty answer is not a demonstrated capability.
    #[test]
    fn an_empty_answer_fails_even_with_perfect_evidence() {
        let mut bad = outcome();
        bad.answer = "   ".to_string();
        let verdict = evaluate_facet(&acceptance(), &bad);
        assert!(!verdict.passed);
        assert_eq!(
            verdict.requirements[0].requirement, "answer_present",
            "the missing answer must be the first thing reported"
        );
    }

    /// Honest reporting is rewarded, not punished, when the facet allows it.
    #[test]
    fn an_unknown_left_unresolved_is_acceptable_when_the_facet_allows_it() {
        let mut honest = outcome();
        honest.marked_unresolved = true;
        assert!(evaluate_facet(&acceptance(), &honest).passed);
    }

    /// And punished when it does not.
    #[test]
    fn an_unknown_left_unresolved_fails_when_the_facet_forbids_it() {
        let mut strict = acceptance();
        strict.keep_unresolved_ok = false;
        let mut honest = outcome();
        honest.marked_unresolved = true;
        let verdict = evaluate_facet(&strict, &honest);
        assert!(!verdict.passed);
        assert!(
            verdict
                .requirements
                .iter()
                .any(|requirement| requirement.requirement == "no_unresolved")
        );
    }

    /// A facet gate must not smuggle in collaboration requirements: those belong
    /// to the medium scenario, and putting them here is what broke both facets.
    #[test]
    fn a_facet_gate_has_no_collaboration_requirements() {
        let verdict = evaluate_facet(&acceptance(), &outcome());
        for requirement in &verdict.requirements {
            for forbidden in ["team", "task", "review", "cross_team", "topic", "agent"] {
                assert!(
                    !requirement.requirement.contains(forbidden),
                    "a facet gate must not require collaboration: {}",
                    requirement.requirement
                );
            }
        }
    }
}
