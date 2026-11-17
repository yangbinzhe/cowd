//! The named refusal codes of the review-evidence domain.
//!
//! Six refusal sentences leave this domain, and every one of them is counted
//! somewhere: the source-pin pair, the enumeration fence, the input-source merge,
//! the producer-metadata fence, and the effect-unknown wrapper. Counting them by
//! sentence shape is what makes the accounting fragile, because a wording change
//! silently renames a class. So every shape gets a stable code here, and the
//! ticket accounting can count by code instead.
//!
//! The sentences themselves do not change. A refusal whose wording changes
//! breaks every downstream classifier and every old control that pins the
//! wording, so a code is added *beside* a sentence, never inside it.

/// The six refusal shapes this domain produces or forwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReviewRefusalCode {
    /// The pinned source moved forward while the review was resolving it.
    ResultSourceChangedDuringResolution,
    /// The pinned source moved backwards: a truncated or replaced stream.
    ResultSourceMovedBackwardsDuringResolution,
    /// The result package's own closing cut fence refused.
    ResultMetadataChangedAfterEnumeration,
    /// The actual-input phase moved a source the package had already pinned.
    ActualInputSourceMovedDuringEnumeration,
    /// The review's closing producer-metadata fence refused.
    ProducerMetadataChangedDuringResolution,
    /// The physical effect itself is not knowable, so the R02 shape collects it.
    OriginalToolEffectUnknown,
}

impl ReviewRefusalCode {
    /// The stable name of this refusal class.
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::ResultSourceChangedDuringResolution => "result_source_changed_during_resolution",
            Self::ResultSourceMovedBackwardsDuringResolution => {
                "result_source_moved_backwards_during_resolution"
            }
            Self::ResultMetadataChangedAfterEnumeration => {
                "result_metadata_changed_after_enumeration"
            }
            Self::ActualInputSourceMovedDuringEnumeration => {
                "actual_input_source_moved_during_enumeration"
            }
            Self::ProducerMetadataChangedDuringResolution => {
                "producer_metadata_changed_during_resolution"
            }
            Self::OriginalToolEffectUnknown => "original_tool_effect_unknown",
        }
    }

    /// The sentence lead this code names.
    pub(crate) fn sentence_lead(self) -> &'static str {
        match self {
            Self::ResultSourceChangedDuringResolution => {
                "result metadata source changed during resolution"
            }
            Self::ResultSourceMovedBackwardsDuringResolution => {
                "result metadata source moved backwards during resolution"
            }
            Self::ResultMetadataChangedAfterEnumeration => {
                "result metadata changed after original effect enumeration"
            }
            Self::ActualInputSourceMovedDuringEnumeration => {
                "actual result input source moved during enumeration"
            }
            Self::ProducerMetadataChangedDuringResolution => {
                "producer metadata changed during result resolution"
            }
            Self::OriginalToolEffectUnknown => "original tool effect remains unknown",
        }
    }
}

/// Every refusal class this domain names, in a fixed order.
pub(crate) const REVIEW_REFUSAL_CODES: [ReviewRefusalCode; 6] = [
    ReviewRefusalCode::ResultSourceChangedDuringResolution,
    ReviewRefusalCode::ResultSourceMovedBackwardsDuringResolution,
    ReviewRefusalCode::ResultMetadataChangedAfterEnumeration,
    ReviewRefusalCode::ActualInputSourceMovedDuringEnumeration,
    ReviewRefusalCode::ProducerMetadataChangedDuringResolution,
    ReviewRefusalCode::OriginalToolEffectUnknown,
];

/// The named class of a produced refusal reason, if it is one this domain owns.
pub(crate) fn reason_code_of(reason: &str) -> Option<ReviewRefusalCode> {
    REVIEW_REFUSAL_CODES
        .into_iter()
        .find(|code| reason.contains(code.sentence_lead()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The six shapes each get their own stable name, and a refusal this domain
    /// does not own gets none.
    #[test]
    fn every_refusal_shape_has_its_own_stable_code() {
        for code in REVIEW_REFUSAL_CODES {
            let reason = format!("{}; some trailing detail", code.sentence_lead());
            assert_eq!(
                reason_code_of(&reason),
                Some(code),
                "{} must be named by its own code",
                code.code()
            );
        }
        let codes: std::collections::BTreeSet<&str> =
            REVIEW_REFUSAL_CODES.map(|code| code.code()).into();
        assert_eq!(codes.len(), REVIEW_REFUSAL_CODES.len(), "codes are distinct");
        assert_eq!(
            reason_code_of("some unrelated refusal"),
            None,
            "a refusal this domain does not own is not named here"
        );
    }

    /// The codes are stable names: changing one changes the accounting, so the
    /// set is pinned.
    #[test]
    fn the_refusal_code_names_are_pinned() {
        let names: Vec<&str> = REVIEW_REFUSAL_CODES
            .iter()
            .map(|code| code.code())
            .collect();
        assert_eq!(
            names,
            vec![
                "result_source_changed_during_resolution",
                "result_source_moved_backwards_during_resolution",
                "result_metadata_changed_after_enumeration",
                "actual_input_source_moved_during_enumeration",
                "producer_metadata_changed_during_resolution",
                "original_tool_effect_unknown",
            ]
        );
    }
}
