//! The attempt ledger: one declaration of which counter is charged to which
//! book, and which bound governs it.
//!
//! 034 GB-3 (a9 half): the attempt accounting used to live in three places
//! at once - the runtime's own doc comments, the contract's teaching text
//! and the shape of the exhaustion note - and each of them could drift on
//! its own. A bound changed in the runtime left the contract teaching a
//! bound that no longer existed; a family renamed in the runtime left the
//! contract naming a book nobody could name any more. This module is the
//! one place the facts are stated, and the runtime mirrors it: the bounds,
//! the counter names and the family-to-book split are all checked against
//! the tree by `agentic::budget_ledger_mirror`.
//!
//! The books are spelled exactly as the runtime spells them in the
//! exhaustion note (`account=model`, `account=structural`), so the contract
//! and the note cannot disagree about the word for a book.
//!
//! Two books are declared here, and one is deliberately absent:
//!
//! - [`AttemptLedgerAccount::Model`] - a failure the model can fix by
//!   trying again. It is charged to the model's own attempt budget.
//! - [`AttemptLedgerAccount::Structural`] - a failure the model cannot fix
//!   by trying again (a delegated child's receipt that never settles, a
//!   read with no original physical extent, a composite read whose child
//!   acquisitions never completed). It is counted in its own column, so the
//!   model's budget is not spent on it, and the two columns share the
//!   exhaustion bound so a Task that only ever stalls still reaches a
//!   terminal instead of looping.
//!
//! The third book of the 034 design (provider cost, deadline window) has no
//! per-task counter in the tree today, so this table does not declare one,
//! and the rebuild/binding residual columns belong to the other half of
//! GB-3. A table that guessed their home would be a second story, not a
//! merge.
//!
//! **One rule for whoever adds a counter**: a new attempt counter is
//! declared here in the same change that adds it to the runtime. The mirror
//! fails when the two sides disagree.

use serde::{Deserialize, Serialize};

/// Which book an attempt count is charged to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptLedgerAccount {
    /// A failure the model can fix by trying again.
    Model,
    /// A failure the model cannot fix by trying again.
    Structural,
}

impl AttemptLedgerAccount {
    /// The word that goes into the exhaustion note.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Structural => "structural",
        }
    }

    /// Whether this book is the structural one.
    #[must_use]
    pub const fn is_structural(self) -> bool {
        matches!(self, Self::Structural)
    }
}

/// One attempt counter: where it is counted, which book it is charged to and
/// which bound governs its exhaustion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptLedgerColumn {
    /// The projection field that carries the count.
    pub counter: &'static str,
    /// The book the count is charged to.
    pub account: AttemptLedgerAccount,
    /// The bound that governs exhaustion. The model book and the structural
    /// book share one bound on purpose: exhaustion counts their sum, so a
    /// Task that only ever stalls still reaches a terminal.
    pub bound: &'static str,
    /// The bound's value.
    pub bound_value: u8,
}

/// Bounded physical failures per Task on the execution path.
pub const MAX_TASK_EXECUTION_ATTEMPTS: u8 = 3;
/// Bounded physical failures per Task on the review path.
pub const MAX_TASK_REVIEW_ATTEMPTS: u8 = 3;
/// Bounded review rounds per Task.
pub const MAX_TASK_REVIEW_ROUNDS: u8 = 3;

/// Every attempt counter the tree charges, with the book it is charged to.
pub const ATTEMPT_LEDGER_COLUMNS: &[AttemptLedgerColumn] = &[
    AttemptLedgerColumn {
        counter: "failed_attempts",
        account: AttemptLedgerAccount::Model,
        bound: "MAX_TASK_EXECUTION_ATTEMPTS",
        bound_value: MAX_TASK_EXECUTION_ATTEMPTS,
    },
    AttemptLedgerColumn {
        counter: "structural_execution_stalls",
        account: AttemptLedgerAccount::Structural,
        bound: "MAX_TASK_EXECUTION_ATTEMPTS",
        bound_value: MAX_TASK_EXECUTION_ATTEMPTS,
    },
    AttemptLedgerColumn {
        counter: "failed_review_attempts",
        account: AttemptLedgerAccount::Model,
        bound: "MAX_TASK_REVIEW_ATTEMPTS",
        bound_value: MAX_TASK_REVIEW_ATTEMPTS,
    },
    AttemptLedgerColumn {
        counter: "structural_review_stalls",
        account: AttemptLedgerAccount::Structural,
        bound: "MAX_TASK_REVIEW_ATTEMPTS",
        bound_value: MAX_TASK_REVIEW_ATTEMPTS,
    },
    AttemptLedgerColumn {
        counter: "review_generation",
        account: AttemptLedgerAccount::Model,
        bound: "MAX_TASK_REVIEW_ROUNDS",
        bound_value: MAX_TASK_REVIEW_ROUNDS,
    },
];

/// Every failure family the runtime can name, with the book it is charged to.
///
/// A family nobody has given a home would be a counter charged to the
/// wrong book, so an unclassified failure is charged to the model - the
/// conservative side of the ledger.
pub const ATTEMPT_FAILURE_FAMILIES: &[(&str, AttemptLedgerAccount)] = &[
    ("receipt_unsettled", AttemptLedgerAccount::Structural),
    (
        "physical_extent_unsupported",
        AttemptLedgerAccount::Structural,
    ),
    (
        "composite_acquisitions_incomplete",
        AttemptLedgerAccount::Structural,
    ),
    ("model_refusal", AttemptLedgerAccount::Model),
    ("unclassified", AttemptLedgerAccount::Model),
];

/// The book a failure family is charged to.
#[must_use]
pub fn account_of_family(family: &str) -> Option<AttemptLedgerAccount> {
    ATTEMPT_FAILURE_FAMILIES
        .iter()
        .find(|(name, _)| *name == family)
        .map(|(_, account)| *account)
}

/// The book a counter is charged to.
#[must_use]
pub fn account_of_counter(counter: &str) -> Option<AttemptLedgerAccount> {
    ATTEMPT_LEDGER_COLUMNS
        .iter()
        .find(|column| column.counter == counter)
        .map(|column| column.account)
}

/// The bound that governs a counter, with its value.
#[must_use]
pub fn bound_of_counter(counter: &str) -> Option<(&'static str, u8)> {
    ATTEMPT_LEDGER_COLUMNS
        .iter()
        .find(|column| column.counter == counter)
        .map(|column| (column.bound, column.bound_value))
}

/// The bound a named bound carries.
#[must_use]
pub fn bound_value(bound: &str) -> Option<u8> {
    match bound {
        "MAX_TASK_EXECUTION_ATTEMPTS" => Some(MAX_TASK_EXECUTION_ATTEMPTS),
        "MAX_TASK_REVIEW_ATTEMPTS" => Some(MAX_TASK_REVIEW_ATTEMPTS),
        "MAX_TASK_REVIEW_ROUNDS" => Some(MAX_TASK_REVIEW_ROUNDS),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_table_declares_every_counter_once() {
        let mut names: Vec<&str> = ATTEMPT_LEDGER_COLUMNS
            .iter()
            .map(|column| column.counter)
            .collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "the ledger table declares a counter twice");
    }

    #[test]
    fn every_column_names_a_declared_bound_and_its_value() {
        for column in ATTEMPT_LEDGER_COLUMNS {
            assert_eq!(
                bound_value(column.bound),
                Some(column.bound_value),
                "column {} names bound {} with a value the bound does not carry",
                column.counter,
                column.bound
            );
        }
    }

    #[test]
    fn both_books_are_charged_on_every_path() {
        let execution: Vec<&str> = ATTEMPT_LEDGER_COLUMNS
            .iter()
            .filter(|column| column.bound == "MAX_TASK_EXECUTION_ATTEMPTS")
            .map(|column| column.counter)
            .collect();
        assert_eq!(
            execution,
            vec!["failed_attempts", "structural_execution_stalls"],
            "the execution path must charge the model book and the structural book"
        );
        let review: Vec<&str> = ATTEMPT_LEDGER_COLUMNS
            .iter()
            .filter(|column| column.bound == "MAX_TASK_REVIEW_ATTEMPTS")
            .map(|column| column.counter)
            .collect();
        assert_eq!(
            review,
            vec!["failed_review_attempts", "structural_review_stalls"],
            "the review path must charge the model book and the structural book"
        );
    }

    #[test]
    fn the_books_are_spelled_as_the_exhaustion_note_spells_them() {
        assert_eq!(AttemptLedgerAccount::Model.as_str(), "model");
        assert_eq!(AttemptLedgerAccount::Structural.as_str(), "structural");
        assert!(AttemptLedgerAccount::Structural.is_structural());
        assert!(!AttemptLedgerAccount::Model.is_structural());
    }

    #[test]
    fn an_unclassified_failure_is_charged_to_the_model() {
        assert_eq!(
            account_of_family("unclassified"),
            Some(AttemptLedgerAccount::Model)
        );
        assert_eq!(
            account_of_family("receipt_unsettled"),
            Some(AttemptLedgerAccount::Structural)
        );
        assert_eq!(account_of_family("no_such_family"), None);
    }
}
