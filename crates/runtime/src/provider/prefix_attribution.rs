//! PLAN A2: attribute a provider prefix divergence to the segment that changed.
//!
//! # Why
//!
//! `transport_pool` already measures reuse as a longest-common-prefix byte count
//! and reports `structural_reuse_ratio_bp`, but a falling ratio could only be
//! described as "it dropped". The live runs made that concrete: the failing
//! scenario showed `exact_prefix_extension_count = 0` - not one round was a pure
//! append of the previous one - and nobody could say which of the six prompt
//! regions had moved.
//!
//! That matters because the six regions have completely different costs:
//!
//! | segment | what it is | cost of changing it |
//! |---|---|---|
//! | `System` | identity + runtime protocol | breaks breakpoint 1 and everything after |
//! | `Tools` | the tool schema array | breaks breakpoint 2 and every message |
//! | `Memory` | retrieved context packets | breaks the dynamic tail |
//! | `Skills` | activated skill bodies | breaks the dynamic tail |
//! | `Fence` | context governance fence | breaks the dynamic tail |
//! | `Messages` | the append-only history | expected to grow; only its head must be stable |
//!
//! # What this module does
//!
//! Given the segment boundaries of the previous and current request, it reports
//! the FIRST segment whose bytes differ. That is the only segment a caller needs
//! to fix: everything after it is downstream.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The six model-visible prompt regions, in serialization order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptSegment {
    /// Stable system head: identity contract and runtime context protocol.
    System,
    /// The tool schema array. Covered by cache breakpoint 2.
    Tools,
    /// Retrieved context packets (memory / knowledge / fact / matrix / task ...).
    Memory,
    /// Activated skill bodies.
    Skills,
    /// The context governance fence.
    Fence,
    /// The append-only conversation history.
    Messages,
}

impl PromptSegment {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Tools => "tools",
            Self::Memory => "memory",
            Self::Skills => "skills",
            Self::Fence => "fence",
            Self::Messages => "messages",
        }
    }

    /// Whether this segment is expected to be byte-stable across requests.
    ///
    /// `Messages` is the exception: it is append-only, so it must only ever
    /// GROW at the tail - its head is what has to stay stable.
    #[must_use]
    pub const fn must_be_stable(self) -> bool {
        !matches!(self, Self::Messages)
    }

    /// All segments in serialization order.
    pub const ALL: [Self; 6] = [
        Self::System,
        Self::Tools,
        Self::Memory,
        Self::Skills,
        Self::Fence,
        Self::Messages,
    ];
}

/// Where one segment sits in a serialized request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SegmentSpan {
    /// Byte offset where the segment starts.
    pub start: usize,
    /// Byte offset one past the segment's end.
    pub end: usize,
}

impl SegmentSpan {
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The result of attributing a prefix divergence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivergenceAttribution {
    /// The first segment whose bytes differ, if any.
    pub segment: Option<PromptSegment>,
    /// Byte offset of the first difference within the whole request.
    pub first_diff_offset: usize,
    /// Bytes that were reusable (the common prefix).
    pub reusable_bytes: usize,
    /// Total bytes of the current request.
    pub prompt_bytes: usize,
    /// Whether every stable segment was byte-identical (so the only change was
    /// the expected growth of the message tail).
    pub only_messages_grew: bool,
}

impl DivergenceAttribution {
    /// Reuse in basis points.
    #[must_use]
    pub fn reuse_bp(&self) -> u32 {
        if self.prompt_bytes == 0 {
            return 0;
        }
        u32::try_from(self.reusable_bytes.saturating_mul(10_000) / self.prompt_bytes)
            .unwrap_or(10_000)
            .min(10_000)
    }

    /// A one-line explanation suitable for a report.
    #[must_use]
    pub fn explain(&self) -> String {
        match self.segment {
            None => format!(
                "pure append: {} reusable of {} bytes ({}bp), only the message tail grew",
                self.reusable_bytes,
                self.prompt_bytes,
                self.reuse_bp()
            ),
            Some(segment) => format!(
                "diverged at segment `{}` at byte {}; {} reusable of {} bytes ({}bp){}",
                segment.as_str(),
                self.first_diff_offset,
                self.reusable_bytes,
                self.prompt_bytes,
                self.reuse_bp(),
                if segment.must_be_stable() {
                    " - this segment is expected to be byte-stable and is the one to fix"
                } else {
                    ""
                }
            ),
        }
    }
}

/// The layout of the canonical prompt the transport actually compares.
///
/// Code fact (`provider_cache_material`): the canonical prompt is the **messages
/// array** encoded as `<len>:<json>\n` per item. The tool schema array and a
/// provider's separate `system` field are NOT in it - they are folded into the
/// cache identity instead, so a tool change produces a *cold identity* rather
/// than a mid-prompt divergence. That leaves exactly two segments to attribute:
///
/// * `System` - the stable leading cohort. It is the first wire item for
///   OpenAI-compatible transports, or the separately-carried system bytes. If
///   this moves, every request after it is re-sent.
/// * `Messages` - the append-only history. It is supposed to grow; only its head
///   has to hold still.
pub fn canonical_prompt_layout(stable_head_bytes: usize, total_bytes: usize) -> PromptLayout {
    let boundary = stable_head_bytes.min(total_bytes);
    PromptLayout {
        spans: [
            (PromptSegment::System, SegmentSpan::new(0, boundary)),
            (PromptSegment::Messages, SegmentSpan::new(boundary, total_bytes)),
        ]
        .into_iter()
        .collect(),
    }
}

/// The segment layout of one serialized request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PromptLayout {
    spans: BTreeMap<PromptSegment, SegmentSpan>,
}

impl PromptLayout {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a segment's span. Spans must be given in serialization order and must
    /// not overlap; the builder is deliberately dumb so a caller cannot hide a
    /// mis-ordered layout.
    #[must_use]
    pub fn with_segment(mut self, segment: PromptSegment, span: SegmentSpan) -> Self {
        self.spans.insert(segment, span);
        self
    }

    /// Span of a segment, if present.
    #[must_use]
    pub fn span(&self, segment: PromptSegment) -> Option<SegmentSpan> {
        self.spans.get(&segment).copied()
    }

    /// Total bytes covered by the layout.
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.spans.values().map(|span| span.len()).sum()
    }
}

/// Attribute the first difference between `previous` and `current` to a segment.
///
/// `current_layout` describes where each segment sits in `current`. Segments that
/// are absent from the layout are treated as zero-length at the end, so a caller
/// that does not track a region still gets a usable answer for the ones it does.
#[must_use]
pub fn attribute_divergence(
    previous: &[u8],
    current: &[u8],
    current_layout: &PromptLayout,
) -> DivergenceAttribution {
    // Longest common prefix, byte by byte. This is exactly the quantity the
    // provider's prompt cache keys on.
    let reusable_bytes = previous
        .iter()
        .zip(current.iter())
        .take_while(|(left, right)| left == right)
        .count();

    // A cold identity has no predecessor, so there is nothing to attribute.
    let mut segment = None;
    let mut only_messages_grew = true;
    let pure_append = previous.is_empty() || reusable_bytes == previous.len();
    if previous.is_empty() || reusable_bytes == previous.len() {
        return DivergenceAttribution {
            segment: None,
            // For a cold identity there is no predecessor at all; for a pure
            // append nothing differs, so both report offset 0 with zero
            // attributed bytes.
            first_diff_offset: 0,
            reusable_bytes,
            prompt_bytes: current.len(),
            only_messages_grew: true,
        };
    }
    let _ = pure_append;
    // The first differing segment is the one that CONTAINS the first difference.
    for candidate in PromptSegment::ALL {
        let Some(span) = current_layout.span(candidate) else {
            continue;
        };
        if span.is_empty() {
            continue;
        }
        if reusable_bytes < span.end && reusable_bytes >= span.start {
            segment = Some(candidate);
            if candidate.must_be_stable() {
                only_messages_grew = false;
            }
            break;
        }
        // This segment ended at or before the divergence point, so it matched.
    }

    DivergenceAttribution {
        segment,
        first_diff_offset: reusable_bytes,
        reusable_bytes,
        prompt_bytes: current.len(),
        only_messages_grew,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Clean, separator-free bytes so each span is exactly what it names:
    // "sys" | "tool1" | "mem" | "first"
    fn clean_layout() -> PromptLayout {
        PromptLayout::new()
            .with_segment(PromptSegment::System, SegmentSpan::new(0, 3))
            .with_segment(PromptSegment::Tools, SegmentSpan::new(3, 8))
            .with_segment(PromptSegment::Memory, SegmentSpan::new(8, 11))
            .with_segment(PromptSegment::Messages, SegmentSpan::new(11, 16))
    }

    #[test]
    fn identical_prompts_are_a_pure_append() {
        let bytes = b"systool1memfirst";
        let attribution = attribute_divergence(bytes, bytes, &clean_layout());
        assert_eq!(attribution.segment, None);
        assert!(attribution.only_messages_grew);
        assert_eq!(attribution.reuse_bp(), 10_000);
        assert!(attribution.explain().contains("pure append"));
    }

    /// A growing message tail is the healthy case.
    #[test]
    fn only_the_message_tail_growing_is_not_divergence() {
        let previous = b"systool1memfirst";
        let current = b"systool1memfirstsecond";
        let layout = PromptLayout::new()
            .with_segment(PromptSegment::System, SegmentSpan::new(0, 3))
            .with_segment(PromptSegment::Tools, SegmentSpan::new(3, 8))
            .with_segment(PromptSegment::Memory, SegmentSpan::new(8, 11))
            .with_segment(PromptSegment::Messages, SegmentSpan::new(11, 21));
        let attribution = attribute_divergence(previous, current, &layout);
        // A purely appended history is not a divergence at all. Only a REWRITTEN
        // one is, and then it is attributed to Messages, the one segment allowed
        // to change.
        assert_eq!(attribution.segment, None);
        assert!(attribution.only_messages_grew);
    }

    /// A changed tool schema breaks breakpoint 2 and every message after it.
    #[test]
    fn a_changed_tool_schema_is_attributed_to_tools() {
        let previous = b"systool1memfirst";
        let current = b"systool2memfirst";
        let attribution = attribute_divergence(previous, current, &clean_layout());
        assert_eq!(attribution.segment, Some(PromptSegment::Tools));
        assert!(!attribution.only_messages_grew);
        let explained = attribution.explain();
        assert!(
            explained.contains("tools") && explained.contains("byte-stable"),
            "the tool segment must be named as the one to fix: {explained}"
        );
    }

    #[test]
    fn a_changed_memory_packet_is_attributed_to_memory() {
        // "sys" | "tool1" | "memA" | "first"
        let previous = b"systool1memAfirst";
        let current = b"systool1memBfirst";
        let layout = PromptLayout::new()
            .with_segment(PromptSegment::System, SegmentSpan::new(0, 3))
            .with_segment(PromptSegment::Tools, SegmentSpan::new(3, 8))
            .with_segment(PromptSegment::Memory, SegmentSpan::new(8, 12))
            .with_segment(PromptSegment::Messages, SegmentSpan::new(12, 17));
        assert_eq!(previous.len(), 17);
        assert_eq!(current.len(), 17);
        let attribution = attribute_divergence(previous, current, &layout);
        assert_eq!(attribution.segment, Some(PromptSegment::Memory));
        assert!(!attribution.only_messages_grew);
    }

    /// A changed system head is the most expensive possible divergence.
    #[test]
    fn a_changed_system_head_is_the_most_expensive() {
        let previous = b"syStool1memfirst";
        let current = b"syTtool1memfirst";
        let attribution = attribute_divergence(previous, current, &clean_layout());
        assert_eq!(attribution.segment, Some(PromptSegment::System));
    }

    /// An empty previous request is a cold identity, not a divergence.
    #[test]
    fn an_empty_previous_request_is_cold_not_diverged() {
        let current = b"systool1";
        let layout = PromptLayout::new()
            .with_segment(PromptSegment::System, SegmentSpan::new(0, 3))
            .with_segment(PromptSegment::Tools, SegmentSpan::new(3, 8));
        let attribution = attribute_divergence(b"", current, &layout);
        assert_eq!(attribution.reusable_bytes, 0);
        assert_eq!(attribution.first_diff_offset, 0);
        assert_eq!(attribution.segment, None, "no predecessor means cold, not diverged");
        assert_eq!(attribution.reuse_bp(), 0);
    }

    #[test]
    fn segments_are_ordered_and_mark_the_expected_stable_ones() {
        assert_eq!(PromptSegment::ALL[0], PromptSegment::System);
        assert_eq!(PromptSegment::ALL[5], PromptSegment::Messages);
        assert!(PromptSegment::System.must_be_stable());
        assert!(PromptSegment::Tools.must_be_stable());
        assert!(PromptSegment::Memory.must_be_stable());
        assert!(PromptSegment::Skills.must_be_stable());
        assert!(PromptSegment::Fence.must_be_stable());
        assert!(
            !PromptSegment::Messages.must_be_stable(),
            "history is append-only; only its head must hold still"
        );
    }
}

#[cfg(test)]
mod canonical_tests {
    use super::*;

    /// The real shape: a stable head plus a growing history. Reuse is high and no
    /// stable segment is blamed - which is what a healthy turn looks like.
    #[test]
    fn a_growing_history_behind_a_stable_head_is_not_divergence() {
        let mut previous = String::new();
        previous.push_str("11:system_msg\n");
        previous.push_str("12:history_msg\n");
        let mut current = previous.clone();
        current.push_str("13:history_msg2\n");
        let layout = canonical_prompt_layout(previous.len(), current.len());
        let attribution = attribute_divergence(previous.as_bytes(), current.as_bytes(), &layout);
        // A pure append is not a divergence at all: reuse is total and nothing
        // needs fixing, which is the healthy case.
        assert_eq!(attribution.segment, None);
        assert!(attribution.only_messages_grew);
        // Reuse is measured against the CURRENT prompt, which now includes the
        // appended message, so it is high but not total - and crucially there is
        // no segment to fix.
        assert!(attribution.reuse_bp() > 6_000, "{}", attribution.reuse_bp());
    }

    /// A moving stable head is the expensive failure: it invalidates everything
    /// after it, so it must be named.
    #[test]
    fn a_moving_stable_head_is_attributed_to_the_system_segment() {
        let previous = "12:system_msgA\n12:history_msg\n";
        let current = "12:system_msgB\n12:history_msg\n";
        let layout = canonical_prompt_layout(14, current.len());
        let attribution = attribute_divergence(previous.as_bytes(), current.as_bytes(), &layout);
        assert_eq!(attribution.segment, Some(PromptSegment::System));
        assert!(!attribution.only_messages_grew);
    }

    /// A changed tool schema does NOT reach the canonical prompt, so it can never
    /// be blamed here: it is a cold identity (a different cache key), which is
    /// the existing `cold_identity` invalidation reason.
    #[test]
    fn a_tool_schema_change_cannot_appear_as_a_prompt_divergence() {
        let previous = "12:system_msg\n12:history_msg\n";
        let current = "12:system_msg\n12:history_msg\n";
        let layout = canonical_prompt_layout(14, current.len());
        let attribution = attribute_divergence(previous.as_bytes(), current.as_bytes(), &layout);
        assert_ne!(attribution.segment, Some(PromptSegment::Tools));
        assert_eq!(attribution.segment, None);
    }
}
