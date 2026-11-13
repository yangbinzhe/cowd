//! Layer 0 of the vocabulary registry: what each reference name means.
//!
//! A model that moves a legal name from one tool to its sibling is not making a
//! syntax mistake - it is answering a question the contract never answered. The
//! review-evidence domain and the objective review surface use two names for
//! two different roles (`result_refs` names the artifacts under review,
//! `evidence_refs` names the observations the reviewer actually read), and
//! nothing anywhere said so. One live run copied a legal name across the pair
//! and was refused for it.
//!
//! So the registry states, per family: the role the name plays, the forms it
//! may take, and the sibling it is most often confused with. The refusal
//! envelope reads this table to name the sibling instead of leaving the model
//! to guess which of two legal names belongs where.
//!
//! Layer 1 (the per-domain refusal-code tables) and layer 2 (the teaching
//! texts) live elsewhere: this table only says what a *name* means, never what
//! a refusal is called and never what a rule says.

/// One reference family: a field name, the role it plays, the forms it may
/// take, and the sibling it is confused with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefFamilySpec {
    /// The field name as the model writes it.
    pub family: &'static str,
    /// What the name is for, in one clause.
    pub role: &'static str,
    /// The durable forms the name may take.
    pub legal_forms: &'static [&'static str],
    /// The family this one is most often confused with.
    pub sibling: Option<&'static str>,
    /// The teaching sentence, naming the sibling and where it is legal.
    pub hint: &'static str,
}

/// Every reference family the model-facing contract names.
///
/// Only families whose legal form the contract actually states are listed: a
/// registry that guesses a form teaches a form the Runtime does not accept.
pub const REF_FAMILIES: &[RefFamilySpec] = &[
    RefFamilySpec {
        family: "evidence_refs",
        role: "the durable observations you actually read (tool:// receipts, artifact:// selectors)",
        legal_forms: &["tool://", "artifact://"],
        sibling: Some("result_refs"),
        hint: "`result_refs` names the results under review and is legal on objective_review and objective_complete_request; `evidence_refs` names only what you read.",
    },
    RefFamilySpec {
        family: "result_refs",
        role: "the result artifacts a review covers, or a completion delivers",
        legal_forms: &["artifact:<id>"],
        sibling: Some("evidence_refs"),
        hint: "`evidence_refs` names the observations you read; `result_refs` names the results under review. task_review has no `result_refs` field - the reviewed artifacts are already bound to the Task, so only `evidence_refs` belongs there.",
    },
    RefFamilySpec {
        family: "artifact_refs",
        role: "the artifacts a Task submission delivers",
        legal_forms: &["artifact:<id>"],
        sibling: Some("result_refs"),
        hint: "`artifact_refs` is what a task_submit delivers; `result_refs` is what a review covers.",
    },
    RefFamilySpec {
        family: "reason_ref",
        role: "a durable reference to the record itself (the review body, the withdrawal record)",
        legal_forms: &["artifact://"],
        sibling: Some("evidence_refs"),
        hint: "`reason_ref` carries no verification weight - the verdict, `reason`, `result_refs` and `evidence_refs` are what the gate reads.",
    },
    RefFamilySpec {
        family: "criterion_ref",
        role: "the Goal criterion or obligation under review",
        legal_forms: &["criterion://", "obligation://"],
        sibling: None,
        hint: "`criterion_ref` names the Goal criterion or obligation returned by state_inspect.",
    },
    RefFamilySpec {
        family: "content_ref",
        role: "the body this artifact binds (an artifact:// selector, or a block of this response)",
        legal_forms: &["artifact://", "current_message_block:<index>"],
        sibling: Some("evidence_refs"),
        hint: "`content_ref` carries the body itself; `evidence_refs` names what you read to judge it.",
    },
    RefFamilySpec {
        family: "source_refs",
        role: "the durable source material a criterion was built from",
        legal_forms: &[],
        sibling: Some("evidence_refs"),
        hint: "`source_refs` names the source material of a criterion change; `evidence_refs` names the observations you read.",
    },
];

/// The family a field name belongs to, if it is one.
#[must_use]
pub fn family_of_field(field: &str) -> Option<&'static RefFamilySpec> {
    REF_FAMILIES
        .iter()
        .find(|spec| spec.family == field)
}

/// The sibling-vocabulary hint for a field name, if the name is a family.
///
/// The hint is the teaching sentence that names the sibling: without it a
/// refusal says what was wrong and leaves the model to guess which of two
/// legal names belongs where.
#[must_use]
pub fn sibling_vocab_hint(field: &str) -> Option<&'static str> {
    family_of_field(field).map(|spec| spec.hint)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every family that names a sibling names one that is itself a family: a
    /// hint that points at a name nobody registered teaches a name the model
    /// cannot use.
    #[test]
    fn every_named_sibling_is_itself_a_registered_family() {
        for spec in REF_FAMILIES {
            let Some(sibling) = spec.sibling else {
                continue;
            };
            assert!(
                family_of_field(sibling).is_some(),
                "{} names sibling {sibling}, which is not a registered family",
                spec.family
            );
        }
    }

    /// The two names the live run confused are both registered, and each one
    /// points at the other.
    #[test]
    fn the_confused_pair_points_at_each_other() {
        assert_eq!(
            family_of_field("evidence_refs").and_then(|spec| spec.sibling),
            Some("result_refs")
        );
        assert_eq!(
            family_of_field("result_refs").and_then(|spec| spec.sibling),
            Some("evidence_refs")
        );
    }

    /// A hint names the sibling by its own name, so the model can search for
    /// it in the contract it was shown.
    #[test]
    fn a_hint_names_its_sibling_explicitly() {
        let hint = sibling_vocab_hint("result_refs").expect("result_refs is a family");
        assert!(hint.contains("evidence_refs"), "{hint}");
        let hint = sibling_vocab_hint("evidence_refs").expect("evidence_refs is a family");
        assert!(hint.contains("result_refs"), "{hint}");
    }

    /// A name nobody registered has no hint: a hint about a name the contract
    /// does not contain is noise.
    #[test]
    fn an_unregistered_name_has_no_hint() {
        assert!(sibling_vocab_hint("task_ref").is_none());
        assert!(sibling_vocab_hint("").is_none());
    }
}
