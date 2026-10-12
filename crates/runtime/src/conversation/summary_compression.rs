use std::collections::BTreeSet;

use model_protocol::fingerprint::stable_hash_bytes;
use serde::{Deserialize, Serialize};

/// Payloads smaller than this are not worth stubbing: the stub itself plus the
/// lost locality would outweigh the savings. Error results are never stubbed.
pub const IDENTICAL_RESULT_STUB_MIN_CHARS: usize = 512;

/// Replace byte-identical duplicate tool-result payloads with a short reference
/// stub, and return how many were stubbed.
///
/// A model that cannot find what it already read reads it again, and each
/// re-read puts the whole payload back into its context. One live delegated
/// Agent issued the same `read_file` sixteen times in a row over three and a
/// half minutes, with a 145k-token context peak, because the thing it needed was
/// in that context and it could not locate it. Stubbing the repeats breaks that
/// loop without touching the durable evidence: the bytes stay keyed by
/// `tool_call` id and `evidence_retrieve` still returns them in full.
///
/// This is deliberately **not** a compression of live context in general. Only a
/// payload that is byte-identical to one already present in the same execution
/// is replaced, so a file that genuinely changed is never stubbed.
pub fn stub_duplicate_result_payloads(
    messages: &mut [crate::ConversationMessage],
) -> usize {
    use crate::ContentBlock;
    let mut first_seen: std::collections::HashMap<(String, String), String> =
        std::collections::HashMap::new();
    let mut stubbed = 0usize;
    for message in messages.iter_mut() {
        for block in message.blocks.iter_mut() {
            let ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error,
            } = block
            else {
                continue;
            };
            if *is_error || output.len() < IDENTICAL_RESULT_STUB_MIN_CHARS {
                continue;
            }
            let key = (tool_name.clone(), output.clone());
            match first_seen.get(&key) {
                Some(origin) => {
                    *output =
                        format!("[duplicate identical `{tool_name}` result omitted; see {origin}]");
                    stubbed = stubbed.saturating_add(1);
                }
                None => {
                    first_seen.insert(key, format!("tool_call:{tool_use_id}"));
                }
            }
        }
    }
    stubbed
}


const DEFAULT_MAX_CHARS: usize = 1_200;
const DEFAULT_MAX_LINES: usize = 24;
const DEFAULT_MAX_LINE_CHARS: usize = 160;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SummaryCompressionBudget {
    pub max_chars: usize,
    pub max_lines: usize,
    pub max_line_chars: usize,
}

impl Default for SummaryCompressionBudget {
    fn default() -> Self {
        Self {
            max_chars: DEFAULT_MAX_CHARS,
            max_lines: DEFAULT_MAX_LINES,
            max_line_chars: DEFAULT_MAX_LINE_CHARS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryCompressionResult {
    pub summary: String,
    pub original_chars: usize,
    pub compressed_chars: usize,
    pub original_lines: usize,
    pub compressed_lines: usize,
    pub removed_duplicate_lines: usize,
    pub omitted_lines: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryCompressionCheckpoint {
    pub checkpoint_id: String,
    pub summary: String,
    pub key_user_rules: Vec<String>,
    pub current_goal: Option<String>,
    pub pending_work: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub omitted_lines: usize,
    pub truncated: bool,
}

impl SummaryCompressionCheckpoint {
    #[must_use]
    pub fn from_result(
        session_id: &str,
        result: &SummaryCompressionResult,
        evidence_refs: Vec<String>,
    ) -> Self {
        let key_user_rules = result
            .summary
            .lines()
            .filter(|line| {
                contains_any_case_insensitive(
                    line,
                    &["rule", "principle", "must", "必须", "规则", "原则", "约定"],
                )
            })
            .map(str::to_string)
            .collect::<Vec<_>>();
        let current_goal = result
            .summary
            .lines()
            .find(|line| {
                contains_any_case_insensitive(
                    line,
                    &["current work", "current goal", "当前", "目标"],
                )
            })
            .map(str::to_string);
        let pending_work = result
            .summary
            .lines()
            .filter(|line| {
                contains_any_case_insensitive(line, &["pending", "todo", "未完成", "待办"])
            })
            .map(str::to_string)
            .collect::<Vec<_>>();
        Self {
            checkpoint_id: format!(
                "summary-checkpoint-{}",
                stable_hash_bytes(format!("{session_id}:{}", result.summary).as_bytes())
            ),
            summary: result.summary.clone(),
            key_user_rules,
            current_goal,
            pending_work,
            evidence_refs,
            omitted_lines: result.omitted_lines,
            truncated: result.truncated,
        }
    }
}

#[must_use]
pub fn compress_summary(
    summary: &str,
    budget: SummaryCompressionBudget,
) -> SummaryCompressionResult {
    let original_chars = summary.chars().count();
    let original_lines = summary.lines().count();

    let normalized = normalize_lines(summary, budget.max_line_chars);
    if normalized.lines.is_empty() || budget.max_chars == 0 || budget.max_lines == 0 {
        return SummaryCompressionResult {
            summary: String::new(),
            original_chars,
            compressed_chars: 0,
            original_lines,
            compressed_lines: 0,
            removed_duplicate_lines: normalized.removed_duplicate_lines,
            omitted_lines: normalized.lines.len(),
            truncated: original_chars > 0,
        };
    }

    let selected = select_line_indexes(&normalized.lines, budget);
    let mut compressed_lines = selected
        .iter()
        .map(|index| normalized.lines[*index].clone())
        .collect::<Vec<_>>();
    if compressed_lines.is_empty() {
        compressed_lines.push(truncate_line(&normalized.lines[0], budget.max_chars));
    }
    let omitted_lines = normalized
        .lines
        .len()
        .saturating_sub(compressed_lines.len());

    if omitted_lines > 0 {
        let omission_notice = omission_notice(omitted_lines);
        push_line_with_budget(&mut compressed_lines, omission_notice, budget);
    }

    let compressed_summary = compressed_lines.join("\n");

    SummaryCompressionResult {
        compressed_chars: compressed_summary.chars().count(),
        compressed_lines: compressed_lines.len(),
        removed_duplicate_lines: normalized.removed_duplicate_lines,
        omitted_lines,
        truncated: compressed_summary != summary.trim(),
        summary: compressed_summary,
        original_chars,
        original_lines,
    }
}

#[must_use]
pub fn compress_summary_text(summary: &str) -> String {
    compress_summary(summary, SummaryCompressionBudget::default()).summary
}

#[derive(Debug, Default)]
struct NormalizedSummary {
    lines: Vec<String>,
    removed_duplicate_lines: usize,
}

fn normalize_lines(summary: &str, max_line_chars: usize) -> NormalizedSummary {
    let mut seen = BTreeSet::new();
    let mut lines = Vec::new();
    let mut removed_duplicate_lines = 0;

    for raw_line in summary.lines() {
        let normalized = collapse_inline_whitespace(raw_line);
        if normalized.is_empty() {
            continue;
        }

        let truncated = truncate_line(&normalized, max_line_chars);
        let dedupe_key = dedupe_key(&truncated);
        if !seen.insert(dedupe_key) {
            removed_duplicate_lines += 1;
            continue;
        }

        lines.push(truncated);
    }

    NormalizedSummary {
        lines,
        removed_duplicate_lines,
    }
}

fn select_line_indexes(lines: &[String], budget: SummaryCompressionBudget) -> Vec<usize> {
    let mut selected = BTreeSet::<usize>::new();

    for priority in 0..=3 {
        for (index, line) in lines.iter().enumerate() {
            if selected.contains(&index) || line_priority(line) != priority {
                continue;
            }

            let candidate = selected
                .iter()
                .map(|selected_index| lines[*selected_index].as_str())
                .chain(std::iter::once(line.as_str()))
                .collect::<Vec<_>>();

            if candidate.len() > budget.max_lines {
                continue;
            }

            if joined_char_count(&candidate) > budget.max_chars {
                continue;
            }

            selected.insert(index);
        }
    }

    selected.into_iter().collect()
}

fn push_line_with_budget(lines: &mut Vec<String>, line: String, budget: SummaryCompressionBudget) {
    let candidate = lines
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(line.as_str()))
        .collect::<Vec<_>>();

    if candidate.len() <= budget.max_lines && joined_char_count(&candidate) <= budget.max_chars {
        lines.push(line);
    }
}

fn joined_char_count(lines: &[&str]) -> usize {
    lines.iter().map(|line| line.chars().count()).sum::<usize>() + lines.len().saturating_sub(1)
}

fn line_priority(line: &str) -> usize {
    if line == "Summary:" || line == "Conversation summary:" || is_core_detail(line) {
        0
    } else if is_section_header(line) {
        1
    } else if line.starts_with("- ") || line.starts_with("  - ") {
        2
    } else {
        3
    }
}

fn is_core_detail(line: &str) -> bool {
    [
        "- Scope:",
        "- Current work:",
        "- Pending work:",
        "- Key files referenced:",
        "- Tools mentioned:",
        "- Recent user requests:",
        "- Previously compacted context:",
        "- Newly compacted context:",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

fn is_section_header(line: &str) -> bool {
    line.ends_with(':')
}

fn omission_notice(omitted_lines: usize) -> String {
    format!("- … {omitted_lines} additional line(s) omitted.")
}

fn collapse_inline_whitespace(line: &str) -> String {
    line.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_line(line: &str, max_chars: usize) -> String {
    if max_chars == 0 || line.chars().count() <= max_chars {
        return line.to_string();
    }

    if max_chars == 1 {
        return "…".to_string();
    }

    let mut truncated = line
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn dedupe_key(line: &str) -> String {
    line.to_ascii_lowercase()
}

fn contains_any_case_insensitive(text: &str, needles: &[&str]) -> bool {
    let lower = text.to_ascii_lowercase();
    needles
        .iter()
        .any(|needle| lower.contains(&needle.to_ascii_lowercase()) || text.contains(needle))
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_repeat_in_a_later_round_is_stubbed_against_the_whole_transcript() {
        use super::stub_duplicate_result_payloads;
        use crate::{ContentBlock, ConversationMessage};
        // The caller seeds the dedup from the turn transcript, so a repeat that
        // arrives in its OWN round is still recognised. One live delegated Agent
        // re-read the same file sixteen times, each read in a separate batch; a
        // per-batch map saw no repeat at all and stubbed nothing.
        let big = "y".repeat(700);
        let mut transcript = vec![ConversationMessage::tool_result(
            "call-1",
            "read_file",
            big.clone(),
            false,
        )];
        // Round two returns the identical payload under a new tool_call id.
        transcript.extend([ConversationMessage::tool_result(
            "call-2",
            "read_file",
            big.clone(),
            false,
        )]);
        let stubbed = stub_duplicate_result_payloads(&mut transcript);
        assert_eq!(stubbed, 1, "the second round's identical payload must be stubbed");
        let second = match &transcript[1].blocks[0] {
            ContentBlock::ToolResult { output, .. } => output.clone(),
            _ => unreachable!(),
        };
        assert!(
            second.contains("[duplicate identical `read_file` result omitted; see tool_call:call-1]"),
            "the stub must point at the first occurrence: {second}"
        );
        // The first occurrence keeps its full payload: evidence is never lost.
        let first = match &transcript[0].blocks[0] {
            ContentBlock::ToolResult { output, .. } => output.clone(),
            _ => unreachable!(),
        };
        assert_eq!(first, big);
    }

    #[test]
    fn duplicate_result_payloads_are_stubbed_except_errors_and_small_payloads() {
        use super::stub_duplicate_result_payloads;
        use crate::{ContentBlock, ConversationMessage};
        let big = "x".repeat(600);
        let small = "tiny".to_string();
        let mut messages = vec![
            ConversationMessage::tool_result("call-1", "read_file", big.clone(), false),
            ConversationMessage::tool_result("call-2", "read_file", big.clone(), false),
            ConversationMessage::tool_result("call-3", "read_file", small.clone(), false),
            ConversationMessage::tool_result("call-4", "read_file", small.clone(), false),
            ConversationMessage::tool_result("call-5", "read_file", big.clone(), true),
        ];
        let stubbed = stub_duplicate_result_payloads(&mut messages);
        // Only the second large successful payload is a stub candidate: small
        // payloads are left alone and error payloads are never stubbed.
        assert_eq!(stubbed, 1);
        let outputs = messages
            .iter()
            .map(|message| match message.blocks.first() {
                Some(ContentBlock::ToolResult { output, .. }) => output.clone(),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(outputs[0], big);
        assert!(outputs[1].contains("duplicate identical"));
        assert_eq!(outputs[2], small);
        assert_eq!(outputs[3], small);
        assert_eq!(outputs[4], big);
    }

    use super::{
        SummaryCompressionBudget, SummaryCompressionCheckpoint, compress_summary,
        compress_summary_text,
    };

    #[test]
    fn collapses_whitespace_and_duplicate_lines() {
        // given
        let summary = "Conversation summary:\n\n- Scope:   compact   earlier   messages.\n- Scope: compact earlier messages.\n- Current work: update runtime module.\n";

        // when
        let result = compress_summary(summary, SummaryCompressionBudget::default());

        // then
        assert_eq!(result.removed_duplicate_lines, 1);
        assert!(
            result
                .summary
                .contains("- Scope: compact earlier messages.")
        );
        assert!(!result.summary.contains("  compact   earlier"));
    }

    #[test]
    fn keeps_core_lines_when_budget_is_tight() {
        // given
        let summary = [
            "Conversation summary:",
            "- Scope: 18 earlier messages compacted.",
            "- Current work: finish summary compression.",
            "- Key timeline:",
            "  - user: asked for a working implementation.",
            "  - assistant: inspected runtime compaction flow.",
            "  - tool: cargo check succeeded.",
        ]
        .join("\n");

        // when
        let result = compress_summary(
            &summary,
            SummaryCompressionBudget {
                max_chars: 120,
                max_lines: 3,
                max_line_chars: 80,
            },
        );

        // then
        assert!(result.summary.contains("Conversation summary:"));
        assert!(
            result
                .summary
                .contains("- Scope: 18 earlier messages compacted.")
        );
        assert!(
            result
                .summary
                .contains("- Current work: finish summary compression.")
        );
        assert!(result.omitted_lines > 0);
    }

    #[test]
    fn provides_a_default_text_only_helper() {
        // given
        let summary = "Summary:\n\nA short line.";

        // when
        let compressed = compress_summary_text(summary);

        // then
        assert_eq!(compressed, "Summary:\nA short line.");
    }

    #[test]
    fn summary_compression_checkpoint_preserves_rules_goal_and_pending_work() {
        let result = compress_summary(
            &[
                "Conversation summary:",
                "- Current work: finish runtime context governance.",
                "- User rule: do not lose important context.",
                "- Pending work: run regression tests.",
                "- Evidence: context-envelope-1.",
            ]
            .join("\n"),
            SummaryCompressionBudget::default(),
        );

        let checkpoint = SummaryCompressionCheckpoint::from_result(
            "session-a",
            &result,
            vec!["context-envelope-1".to_string()],
        );

        assert!(
            checkpoint
                .current_goal
                .as_deref()
                .unwrap_or_default()
                .contains("Current work")
        );
        assert_eq!(checkpoint.key_user_rules.len(), 1);
        assert_eq!(checkpoint.pending_work.len(), 1);
        assert_eq!(checkpoint.evidence_refs, vec!["context-envelope-1"]);
    }
}
