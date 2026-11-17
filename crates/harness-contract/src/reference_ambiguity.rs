//! The named refusal for a reference that more than one durable receipt answers to.
//!
//! A durable reference carries two things at once: the strong key that names
//! exactly one recorded receipt, and a content digest that several receipts can
//! legitimately share. When the strong key does not resolve, the content digest
//! is the only thing left to search on - and it can answer with several
//! receipts.
//!
//! That case has always been *reported*, in prose, by the hint path. It was
//! never *typed*: the refusal carried no code and no payload, so a reader
//! downstream had nothing to parse and no way to tell this refusal from any
//! other unresolvable reference. This module is the one typed shape for it, so
//! the hint path and every other reader name the same class.
//!
//! Two properties must hold for every user of this type:
//!
//!   * the prose a model reads is rendered *from* the typed payload, never
//!     written beside it - one source, so the two cannot drift apart;
//!   * the type names an ambiguity. It never picks a winner: resolving to one
//!     of several receipts is a different decision, and that decision belongs
//!     to whoever owns the strong key.

use serde_json::{json, Value};

/// The stable name of the refusal class.
pub const REFERENCE_AMBIGUOUS_CODE: &str = "reference_ambiguous";

/// A reference that more than one durable receipt answers to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AmbiguousReference {
    cited: String,
    candidates: Vec<String>,
}

impl AmbiguousReference {
    /// Record the citation and every receipt that answers to it.
    pub fn new(cited: impl Into<String>, candidates: Vec<String>) -> Self {
        Self {
            cited: cited.into(),
            candidates,
        }
    }

    /// The stable name of the refusal class.
    pub fn code(&self) -> &'static str {
        REFERENCE_AMBIGUOUS_CODE
    }

    /// The reference the submitter cited.
    pub fn cited(&self) -> &str {
        &self.cited
    }

    /// Every receipt that answers to the cited content.
    pub fn candidates(&self) -> &[String] {
        &self.candidates
    }

    /// How many receipts answer to it.
    pub fn candidate_count(&self) -> usize {
        self.candidates.len()
    }

    /// The machine-readable payload: the refusal a reader can parse.
    pub fn payload(&self) -> Value {
        self.payload_capped(usize::MAX)
    }

    /// The machine-readable payload, with the candidate list bounded.
    ///
    /// `candidate_count` always states how many receipts answer to the
    /// citation; `candidates` carries as many of them as a reader needs and
    /// `candidates_truncated` says plainly that the list is shorter than the
    /// count. A bounded list never pretends to be the whole set.
    pub fn payload_capped(&self, max: usize) -> Value {
        let shown = self.candidates.len().min(max);
        json!({
            "code": self.code(),
            "cited": self.cited,
            "candidate_count": self.candidate_count(),
            "candidates": &self.candidates[..shown],
            "candidates_truncated": shown < self.candidates.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ambiguous_reference_is_named_and_carries_its_candidates() {
        let ambiguity = AmbiguousReference::new(
            "tool-raw-call_00_abc",
            vec![
                "tool-raw-call_00_abc-1".to_string(),
                "tool-raw-call_00_abc-2".to_string(),
            ],
        );
        assert_eq!(ambiguity.code(), "reference_ambiguous");
        assert_eq!(ambiguity.candidate_count(), 2);
        assert_eq!(ambiguity.cited(), "tool-raw-call_00_abc");
        let payload = ambiguity.payload();
        assert_eq!(payload["code"], "reference_ambiguous");
        assert_eq!(payload["candidate_count"], 2);
        assert_eq!(payload["candidates"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn the_payload_is_the_only_source_of_the_candidate_list() {
        let ambiguity = AmbiguousReference::new("cited", vec!["a".to_string()]);
        let payload = ambiguity.payload();
        assert_eq!(payload["cited"], ambiguity.cited());
        let candidates = payload["candidates"]
            .as_array()
            .expect("candidates")
            .iter()
            .map(|value| value.as_str().expect("a candidate").to_string())
            .collect::<Vec<String>>();
        assert_eq!(candidates, ambiguity.candidates().to_vec());
    }
}
