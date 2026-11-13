//! Layer 1 of the vocabulary registry: the refusal-code tables, and the
//! envelope that carries a refusal's teaching with it.
//!
//! A refusal that says only "this field is wrong" leaves the model to guess.
//! The envelope says four things, in this order: the code (from this layer, so
//! the counting is by code and not by wording), the path that was violated, the
//! legal form of that path (from the schema, so the model has something to
//! copy), and the sibling vocabulary hint (from layer 0, so a model that moved
//! a legal name from a sibling tool is told which name belongs where).
//!
//! The envelope adds no new judgement: the message it carries is the refusal
//! that was already going to be sent, and the code is a name for it.

use std::fmt;

use crate::agent_action::{
    rule_subjects, AgentAction, AgentActionRefusal, AgentActionValidationError,
};
use crate::reference_vocab::sibling_vocab_hint;
use crate::tool_input_contract::example_for_path;

/// The refusals this domain names.
///
/// A code is a name for a refusal shape, so the accounting can count by code
/// instead of by wording. Adding a code never changes a sentence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationRefusalCode {
    /// A field the schema requires was not sent.
    MissingField,
    /// A field the model did send carries nothing.
    EmptyValue,
    /// A list field repeats a value.
    DuplicateValue,
    /// A reference field does not carry its canonical form.
    InvalidCanonicalForm,
    /// A cross-field rule refused the call.
    RuleRejected,
}

impl ValidationRefusalCode {
    /// The stable name of this refusal class.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::MissingField => "missing_field",
            Self::EmptyValue => "empty_value",
            Self::DuplicateValue => "duplicate_value",
            Self::InvalidCanonicalForm => "invalid_canonical_form",
            Self::RuleRejected => "rule_rejected",
        }
    }
}

/// The code of one refusal.
#[must_use]
pub fn code_of(error: &AgentActionValidationError) -> ValidationRefusalCode {
    match error {
        AgentActionValidationError::Missing(_) => ValidationRefusalCode::MissingField,
        AgentActionValidationError::Empty(_, _) => ValidationRefusalCode::EmptyValue,
        AgentActionValidationError::Duplicate(_) => ValidationRefusalCode::DuplicateValue,
        AgentActionValidationError::Invalid(_) => ValidationRefusalCode::InvalidCanonicalForm,
        AgentActionValidationError::Rejected(_) => ValidationRefusalCode::RuleRejected,
        AgentActionValidationError::Enveloped(envelope) => envelope.code,
    }
}

/// The refusals the evidence-reference domain names.
///
/// A citation that names no receipt, or that names the legacy call-id form,
/// are two shapes of the same problem - the model wrote a reference that does
/// not pin one durable receipt. Each gets a code so the accounting counts the
/// shape instead of the wording, and so a repair can be aimed at the right
/// shape.
///
/// The third shape - a citation that more than one receipt answers to - is
/// **not** named here: it is the ambiguity, and the ambiguity has one class in
/// one ledger ([`crate::reference_ambiguity`]). A second name for it would be
/// a second form of the same refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceRefusalCode {
    /// The citation is the legacy call-id form, which pins no content.
    LegacyCallOnlyForm,
    /// The citation matches no recorded receipt under a repairable shape.
    UnrepairedCitation,
}

impl EvidenceRefusalCode {
    /// The stable name of this refusal class.
    #[must_use]
    pub fn code(self) -> &'static str {
        match self {
            Self::LegacyCallOnlyForm => "evidence_ref_legacy_form",
            Self::UnrepairedCitation => "evidence_ref_unrepairable",
        }
    }
}

/// A refusal that carries its teaching with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusalEnvelope {
    /// The refusal itself, unchanged.
    pub message: String,
    /// The refusal's judgement, kept whole under the teaching.
    pub refusal: AgentActionRefusal,
    /// The code from this layer.
    pub code: ValidationRefusalCode,
    /// The path that was violated. A rule that covers several fields names them
    /// all, separated by `|`.
    pub what_violated: String,
    /// The legal form of the violated path, rendered from the schema.
    pub complete_legal_example: Option<String>,
    /// The sibling-vocabulary hint from layer 0.
    pub sibling_vocab_hint: Option<&'static str>,
}

impl fmt::Display for RefusalEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}. code={} what_violated={}",
            self.message,
            self.code.code(),
            self.what_violated
        )?;
        if let Some(example) = &self.complete_legal_example {
            write!(formatter, ". Send it shaped like this: {example}")?;
        }
        if let Some(hint) = self.sibling_vocab_hint {
            write!(formatter, ". sibling_vocab_hint: {hint}")?;
        }
        Ok(())
    }
}

/// The envelope of one refusal, built from the action that was refused.
///
/// The legal form comes from the action's own schema, so it is the shape the
/// action actually accepts - not a hand-written sample that can drift from it.
#[must_use]
pub fn envelope_for(
    action: &AgentAction,
    error: &AgentActionValidationError,
) -> RefusalEnvelope {
    // An envelope that is already built is returned as it is: wrapping twice
    // would repeat the teaching in the same sentence.
    if let AgentActionValidationError::Enveloped(existing) = error {
        return (**existing).clone();
    }
    let message = error.to_string();
    let code = code_of(error);
    let what_violated = what_violated_of(error);
    let subject = what_violated
        .split('|')
        .map(str::trim)
        .find(|part| !part.is_empty())
        .unwrap_or_default();
    let complete_legal_example = if subject.is_empty() {
        None
    } else {
        action
            .input_schema()
            .and_then(|schema| example_for_path(&schema, subject))
            .and_then(|value| serde_json::to_string(&value).ok())
    };
    let sibling_hint = sibling_vocab_hint(subject);
    RefusalEnvelope {
        message,
        refusal: error.refusal(),
        code,
        what_violated,
        complete_legal_example,
        sibling_vocab_hint: sibling_hint,
    }
}

/// The path one refusal violated.
#[must_use]
pub fn what_violated_of(error: &AgentActionValidationError) -> String {
    match error {
        AgentActionValidationError::Missing(field)
        | AgentActionValidationError::Empty(field, _)
        | AgentActionValidationError::Duplicate(field)
        | AgentActionValidationError::Invalid(field) => (*field).to_string(),
        AgentActionValidationError::Rejected(rule) => rule_subjects(rule)
            .map(|subjects| subjects.join("|"))
            .unwrap_or_default(),
        AgentActionValidationError::Enveloped(envelope) => envelope.what_violated.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_action::{ObjectiveCompleteRequestInput, TaskReviewInput};

    fn objective_complete() -> AgentAction {
        AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
            result_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blocked: false,
            blocked_reason: None,
            human_review_required: false,
            recommended_next_actions: Vec::new(),
        })
    }

    /// A refusal for a missing field names the field, the code, and the legal
    /// form of that field - the form the model can copy, not the whole call.
    #[test]
    fn a_missing_field_carries_its_own_legal_form() {
        let action = objective_complete();
        let error = action.validate().expect_err("empty result_refs is refused");
        let envelope = envelope_for(&action, &error);
        assert_eq!(envelope.code, ValidationRefusalCode::RuleRejected);
        assert!(envelope.what_violated.contains("result_refs"));
        let example = envelope
            .complete_legal_example
            .expect("the refused field has a legal form");
        assert!(example.contains("result_refs"), "{example}");
    }

    /// The sibling hint names the family the model confused.
    #[test]
    fn a_refused_reference_family_carries_its_sibling_hint() {
        let action = objective_complete();
        let error = action.validate().expect_err("empty result_refs is refused");
        let envelope = envelope_for(&action, &error);
        let hint = envelope
            .sibling_vocab_hint
            .expect("result_refs is a registered family");
        assert!(hint.contains("evidence_refs"), "{hint}");
    }

    /// A refused field that is not a family carries no hint: a hint about a
    /// name the contract does not contain is noise.
    #[test]
    fn a_refused_field_outside_the_families_carries_no_hint() {
        let action = AgentAction::TaskReview(TaskReviewInput {
            task_ref: "task-1".to_string(),
            decision: crate::agent_action::TaskReviewDecision::Accept,
            reason: String::new(),
            operator_requirement: None,
            evidence_refs: Vec::new(),
            gaps: Vec::new(),
            directed_actions: Vec::new(),
        });
        let error = action.validate().expect_err("an empty reason is refused");
        let envelope = envelope_for(&action, &error);
        assert_eq!(envelope.code, ValidationRefusalCode::EmptyValue);
        assert_eq!(envelope.what_violated, "reason");
        assert!(envelope.sibling_vocab_hint.is_none());
    }

    /// The envelope reads as one teaching sentence: the refusal, the code, the
    /// path, the form, and the hint.
    #[test]
    fn the_envelope_reads_as_one_teaching_sentence() {
        let action = objective_complete();
        let error = action.validate().expect_err("empty result_refs is refused");
        let text = envelope_for(&action, &error).to_string();
        assert!(text.contains("code=rule_rejected"), "{text}");
        assert!(text.contains("what_violated=result_refs"), "{text}");
        assert!(text.contains("Send it shaped like this:"), "{text}");
        assert!(text.contains("sibling_vocab_hint:"), "{text}");
    }
}
