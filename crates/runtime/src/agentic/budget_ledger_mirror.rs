//! 034 GB-3 (a9 half): the mirror negative control between the contract's
//! attempt-ledger authority and the runtime that spends it.
//!
//! The contract declares the books, the counters and the bounds once
//! (`harness_contract::budget_ledger`). The runtime is the tree that charges
//! them. This module proves that the two still say the same thing:
//!
//! - every counter the ledger declares is carried by the runtime's Task
//!   projection, and every counter-shaped field the projection carries is
//!   declared;
//! - the runtime's bounds are the contract's bounds;
//! - every failure family the runtime can name is charged to the book the
//!   contract names.
//!
//! The family check is an exhaustive match on purpose: a new family in the
//! runtime breaks this file's build until the contract says where it is
//! charged. That is the point - a family nobody gave a home is a counter
//! that would be charged to the wrong book.

#[cfg(test)]
mod tests {
    use crate::agentic::program::AgenticTaskProjection;
    use crate::agentic::work_market::{
        AttemptFailureFamily, MAX_TASK_EXECUTION_ATTEMPTS, MAX_TASK_REVIEW_ATTEMPTS,
        MAX_TASK_REVIEW_ROUNDS,
    };
    use harness_contract::budget_ledger as ledger;

    /// The book the runtime charges a failure family to.
    ///
    /// The match is exhaustive without a wildcard arm on purpose: a new
    /// family in the runtime breaks this file's build until the contract
    /// says where it is charged.
    fn runtime_account(family: AttemptFailureFamily) -> ledger::AttemptLedgerAccount {
        match family {
            AttemptFailureFamily::ReceiptUnsettled
            | AttemptFailureFamily::PhysicalExtentUnsupported
            | AttemptFailureFamily::CompositeAcquisitionsIncomplete => {
                ledger::AttemptLedgerAccount::Structural
            }
            AttemptFailureFamily::ModelRefusal | AttemptFailureFamily::Unclassified => {
                ledger::AttemptLedgerAccount::Model
            }
        }
    }

    /// The runtime's Task projection as its published shape.
    fn projection_properties() -> Vec<String> {
        let schema = serde_json::to_value(schemars::schema_for!(AgenticTaskProjection))
            .expect("task projection schema");
        schema["properties"]
            .as_object()
            .expect("task projection properties")
            .keys()
            .cloned()
            .collect()
    }

    /// The numeric counters the projection carries.
    ///
    /// The type decides what a counter is: a field that holds a *list* of
    /// released attempts is the binding residual (the other half of GB-3),
    /// not a count, and the map of active attempts is not a count either.
    fn projection_numeric_counters() -> Vec<String> {
        let schema = serde_json::to_value(schemars::schema_for!(AgenticTaskProjection))
            .expect("task projection schema");
        let properties = schema["properties"]
            .as_object()
            .expect("task projection properties");
        properties
            .iter()
            .filter(|(_, value)| value["type"] == "integer")
            .map(|(name, _)| name.clone())
            .collect()
    }

    #[test]
    fn the_runtime_bounds_are_the_contract_ledger_bounds() {
        assert_eq!(
            MAX_TASK_EXECUTION_ATTEMPTS,
            ledger::MAX_TASK_EXECUTION_ATTEMPTS,
            "the execution bound drifted from the contract ledger"
        );
        assert_eq!(
            MAX_TASK_REVIEW_ATTEMPTS,
            ledger::MAX_TASK_REVIEW_ATTEMPTS,
            "the review bound drifted from the contract ledger"
        );
        assert_eq!(
            MAX_TASK_REVIEW_ROUNDS,
            ledger::MAX_TASK_REVIEW_ROUNDS,
            "the review-round bound drifted from the contract ledger"
        );
    }

    #[test]
    fn every_counter_the_ledger_declares_is_carried_by_the_runtime_projection() {
        let properties = projection_properties();
        for column in ledger::ATTEMPT_LEDGER_COLUMNS {
            assert!(
                properties.iter().any(|name| name == column.counter),
                "the contract ledger declares counter {} that the runtime projection does not carry",
                column.counter
            );
        }
    }

    #[test]
    fn every_counter_the_runtime_carries_is_declared_by_the_contract_ledger() {
        let mut runtime_counters: Vec<String> = projection_numeric_counters()
            .into_iter()
            .filter(|name| {
                // The shape of an attempt counter: a number that counts
                // attempts or stalls. A verdict counter (`reviews_applied`,
                // `reviews_accepted`) counts applied reviews, not failures,
                // and does not carry that shape - it belongs to the review
                // obligation book. A list of released attempts and the map
                // of active attempts are not numbers at all and are already
                // out of the surface by their type.
                name.ends_with("_attempts")
                    || name.ends_with("_stalls")
                    || name == "review_generation"
            })
            .collect();
        runtime_counters.sort();
        let mut declared: Vec<String> = ledger::ATTEMPT_LEDGER_COLUMNS
            .iter()
            .map(|column| column.counter.to_string())
            .collect();
        declared.sort();
        assert_eq!(
            runtime_counters, declared,
            "the runtime carries a counter the contract ledger does not declare"
        );
    }

    #[test]
    fn every_runtime_family_is_declared_in_the_contract_ledger() {
        let families = [
            AttemptFailureFamily::ReceiptUnsettled,
            AttemptFailureFamily::PhysicalExtentUnsupported,
            AttemptFailureFamily::CompositeAcquisitionsIncomplete,
            AttemptFailureFamily::ModelRefusal,
            AttemptFailureFamily::Unclassified,
        ];
        for family in families {
            let name = family.note_name();
            assert_eq!(
                ledger::account_of_family(name),
                Some(runtime_account(family)),
                "the runtime charges family {name} to a book the contract does not declare"
            );
        }
        assert_eq!(
            ledger::ATTEMPT_FAILURE_FAMILIES.len(),
            families.len(),
            "the contract declares a failure family the runtime cannot name"
        );
    }
}
