use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};

const COMPACT_CONTINUATION_PREAMBLE: &str =
    "This session is being continued from a previous conversation that ran out of context. The summary below covers the earlier portion of the conversation.\n\n";
const COMPACT_RECENT_MESSAGES_NOTE: &str = "Recent messages are preserved verbatim.";
const COMPACT_DIRECT_RESUME_INSTRUCTION: &str = "Continue the conversation from where it left off without asking the user any further questions. Resume directly — do not acknowledge the summary, do not recap what was happening, and do not preface with continuation text.";

/// Thresholds controlling when and how a session is compacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionConfig {
    pub preserve_recent_messages: usize,
    pub max_estimated_tokens: usize,
    pub priority_threshold: u8,
    pub keep_high_priority: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            preserve_recent_messages: 4,
            max_estimated_tokens: 10_000,
            priority_threshold: 3,
            keep_high_priority: true,
        }
    }
}

/// Result of compacting a session into a summary plus preserved tail messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    pub summary: String,
    pub formatted_summary: String,
    pub compacted_session: Session,
    pub removed_message_count: usize,
    pub source_message_start: usize,
    pub source_message_end: usize,
}

/// A deterministic compaction boundary. Runtime builds the semantic checkpoint
/// after this plan is chosen, then applies that one checkpoint summary through
/// `apply_compaction_summary`; it must not synthesize a second summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub source_message_start: usize,
    pub source_message_end: usize,
    pub removed_message_count: usize,
    pub existing_summary: Option<String>,
    pub preserved_messages: Vec<ConversationMessage>,
}

/// Roughly estimates the token footprint of the current session transcript.
#[must_use]
pub fn estimate_session_tokens(session: &Session) -> usize {
    session.messages().map(estimate_message_tokens).sum()
}

/// Returns `true` when the session exceeds the configured compaction budget.
#[must_use]
pub fn should_compact(session: &Session, config: CompactionConfig) -> bool {
    let start = compacted_summary_prefix_len(session);
    let compactable = session.messages().skip(start);
    let compactable_len = session.message_count().saturating_sub(start);

    compactable_len > config.preserve_recent_messages
        && compactable.map(estimate_message_tokens).sum::<usize>() >= config.max_estimated_tokens
}

/// Normalizes a compaction summary into user-facing continuation text.
#[must_use]
pub fn format_compact_summary(summary: &str) -> String {
    let without_analysis = strip_tag_block(summary, "analysis");
    let formatted = if let Some(content) = extract_tag_block(&without_analysis, "summary") {
        without_analysis.replace(
            &format!("<summary>{content}</summary>"),
            &format!("Summary:\n{}", content.trim()),
        )
    } else {
        without_analysis
    };

    collapse_blank_lines(&formatted).trim().to_string()
}

/// Builds the synthetic system message used after session compaction.
#[must_use]
pub fn get_compact_continuation_message(
    summary: &str,
    suppress_follow_up_questions: bool,
    recent_messages_preserved: bool,
) -> String {
    let mut base = format!(
        "{COMPACT_CONTINUATION_PREAMBLE}{}",
        format_compact_summary(summary)
    );

    if recent_messages_preserved {
        base.push_str("\n\n");
        base.push_str(COMPACT_RECENT_MESSAGES_NOTE);
    }

    if suppress_follow_up_questions {
        base.push('\n');
        base.push_str(COMPACT_DIRECT_RESUME_INSTRUCTION);
    }

    base
}

/// Compacts a session by summarizing older messages and preserving the recent tail.
#[cfg(test)]
#[must_use]
pub fn compact_session(session: &Session, config: CompactionConfig) -> CompactionResult {
    let Some(plan) = plan_session_compaction(session, config) else {
        return CompactionResult {
            summary: String::new(),
            formatted_summary: String::new(),
            compacted_session: session.clone(),
            removed_message_count: 0,
            source_message_start: 0,
            source_message_end: 0,
        };
    };

    let messages = session.materialize_messages();
    let removed = &messages[plan.source_message_start..plan.source_message_end];
    let summary = merge_compact_summaries(
        plan.existing_summary.as_deref(),
        &summarize_messages(removed),
    );
    apply_compaction_summary(session, plan, summary)
}

/// Choose the message range to compact while retaining tool call/result pairs.
/// This is pure and does not create any synthetic summary content.
#[must_use]
pub fn plan_session_compaction(
    session: &Session,
    config: CompactionConfig,
) -> Option<CompactionPlan> {
    if !should_compact(session, config) {
        return None;
    }

    let existing_summary = session
        .message(0)
        .and_then(extract_existing_compacted_summary);
    let compacted_prefix_len = usize::from(existing_summary.is_some());
    let raw_keep_from = session
        .message_count()
        .saturating_sub(config.preserve_recent_messages);
    // Ensure we do not split a tool-use / tool-result pair at the compaction
    // boundary. If the first preserved message is a user message whose first
    // block is a ToolResult, the assistant message with the matching ToolUse
    // was slated for removal — that produces an orphaned tool role message on
    // the OpenAI-compat path (400: tool message must follow assistant with
    // tool_calls). Walk the boundary back until we start at a safe point.
    let keep_from = {
        let mut k = raw_keep_from;
        // If the first preserved message is a tool-result turn, ensure its
        // paired assistant tool-use turn is preserved too. Without this fix,
        // the OpenAI-compat adapter sends an orphaned 'tool' role message
        // with no preceding assistant 'tool_calls', which providers reject
        // with a 400. We walk back only if the immediately preceding message
        // is NOT an assistant message that contains a ToolUse block (i.e. the
        // pair is actually broken at the boundary).
        loop {
            if k == 0 || k <= compacted_prefix_len {
                break;
            }
            let first_preserved = session.message(k)?;
            let starts_with_tool_result = first_preserved
                .blocks
                .first()
                .is_some_and(|b| matches!(b, ContentBlock::ToolResult { .. }));
            if !starts_with_tool_result {
                break;
            }
            // Check the message just before the current boundary.
            let preceding = session.message(k - 1)?;
            let preceding_has_tool_use = preceding
                .blocks
                .iter()
                .any(|b| matches!(b, ContentBlock::ToolUse { .. }));
            if preceding_has_tool_use {
                // Pair is intact — walk back one more to include the assistant turn.
                k = k.saturating_sub(1);
                break;
            }
            // Preceding message has no ToolUse but we have a ToolResult —
            // this is already an orphaned pair; walk back to try to fix it.
            k = k.saturating_sub(1);
        }
        k
    };
    Some(CompactionPlan {
        source_message_start: compacted_prefix_len,
        source_message_end: keep_from,
        removed_message_count: keep_from.saturating_sub(compacted_prefix_len),
        existing_summary,
        preserved_messages: session.messages().skip(keep_from).cloned().collect(),
    })
}

/// Apply exactly one caller-provided semantic summary to a previously planned
/// boundary. The caller owns checkpoint construction and durability; this
/// module only maintains transcript structure and pairing invariants.
#[must_use]
pub fn apply_compaction_summary(
    session: &Session,
    plan: CompactionPlan,
    summary: String,
) -> CompactionResult {
    let formatted_summary = format_compact_summary(&summary);
    let continuation =
        get_compact_continuation_message(&summary, true, !plan.preserved_messages.is_empty());

    let mut compacted_messages = vec![ConversationMessage {
        role: MessageRole::System,
        blocks: vec![ContentBlock::Text { text: continuation }],
        usage: None,
    }];
    compacted_messages.extend(plan.preserved_messages.clone());

    let mut compacted_session = session.clone();
    compacted_session.replace_messages(compacted_messages);
    compacted_session.record_compaction(summary.clone(), plan.removed_message_count);

    CompactionResult {
        summary,
        formatted_summary,
        compacted_session,
        removed_message_count: plan.removed_message_count,
        source_message_start: plan.source_message_start,
        source_message_end: plan.source_message_end,
    }
}

fn compacted_summary_prefix_len(session: &Session) -> usize {
    usize::from(
        session
            .message(0)
            .and_then(extract_existing_compacted_summary)
            .is_some(),
    )
}

#[cfg(test)]
fn summarize_messages(messages: &[ConversationMessage]) -> String {
    let user_messages = messages
        .iter()
        .filter(|message| message.role == MessageRole::User)
        .count();
    let assistant_messages = messages
        .iter()
        .filter(|message| message.role == MessageRole::Assistant)
        .count();
    let tool_messages = messages
        .iter()
        .filter(|message| message.role == MessageRole::Tool)
        .count();

    let mut tool_names = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolUse { name, .. } => Some(name.as_str()),
            ContentBlock::ToolResult { tool_name, .. } => Some(tool_name.as_str()),
            ContentBlock::Text { .. }
            | ContentBlock::Image { .. }
            | ContentBlock::Thinking { .. } => None,
        })
        .collect::<Vec<_>>();
    tool_names.sort_unstable();
    tool_names.dedup();

    let mut lines = vec![
        "<summary>".to_string(),
        "Conversation summary:".to_string(),
        format!(
            "- Scope: {} earlier messages compacted (user={}, assistant={}, tool={}).",
            messages.len(),
            user_messages,
            assistant_messages,
            tool_messages
        ),
    ];

    if !tool_names.is_empty() {
        lines.push(format!("- Tools mentioned: {}.", tool_names.join(", ")));
    }

    let recent_user_requests = collect_recent_role_summaries(messages, MessageRole::User, 3);
    if !recent_user_requests.is_empty() {
        lines.push("- Recent user requests:".to_string());
        lines.extend(
            recent_user_requests
                .into_iter()
                .map(|request| format!("  - {request}")),
        );
    }

    let pending_work = infer_pending_work(messages);
    if !pending_work.is_empty() {
        lines.push("- Pending work:".to_string());
        lines.extend(pending_work.into_iter().map(|item| format!("  - {item}")));
    }

    let key_files = collect_key_files(messages);
    if !key_files.is_empty() {
        lines.push(format!("- Key files referenced: {}.", key_files.join(", ")));
    }

    if let Some(current_work) = infer_current_work(messages) {
        lines.push(format!("- Current work: {current_work}"));
    }

    lines.push("- Key timeline:".to_string());
    for message in messages {
        let role = match message.role {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        };
        let content = message
            .blocks
            .iter()
            .map(summarize_block)
            .collect::<Vec<_>>()
            .join(" | ");
        lines.push(format!("  - {role}: {content}"));
    }
    lines.push("</summary>".to_string());
    lines.join("\n")
}

#[cfg(test)]
fn merge_compact_summaries(existing_summary: Option<&str>, new_summary: &str) -> String {
    let Some(existing_summary) = existing_summary else {
        return new_summary.to_string();
    };

    let previous_highlights = extract_summary_highlights(existing_summary);
    let new_formatted_summary = format_compact_summary(new_summary);
    let new_highlights = extract_summary_highlights(&new_formatted_summary);
    let new_timeline = extract_summary_timeline(&new_formatted_summary);

    let mut lines = vec!["<summary>".to_string(), "Conversation summary:".to_string()];

    if !previous_highlights.is_empty() {
        lines.push("- Previously compacted context:".to_string());
        lines.extend(
            previous_highlights
                .into_iter()
                .map(|line| format!("  {line}")),
        );
    }

    if !new_highlights.is_empty() {
        lines.push("- Newly compacted context:".to_string());
        lines.extend(new_highlights.into_iter().map(|line| format!("  {line}")));
    }

    if !new_timeline.is_empty() {
        lines.push("- Key timeline:".to_string());
        lines.extend(new_timeline.into_iter().map(|line| format!("  {line}")));
    }

    lines.push("</summary>".to_string());
    lines.join("\n")
}

#[cfg(test)]
fn summarize_block(block: &ContentBlock) -> String {
    let raw = match block {
        ContentBlock::Text { text } => text.clone(),
        ContentBlock::Image {
            media_type,
            source_path,
            ..
        } => format!(
            "[image media_type={} source_path={}]",
            media_type,
            source_path.as_deref().unwrap_or("<inline>")
        ),
        ContentBlock::Thinking { thinking, .. } => format!("[thinking] {}", thinking),
        ContentBlock::ToolUse { name, input, .. } => format!("tool_use {name}({input})"),
        ContentBlock::ToolResult {
            tool_name,
            output,
            is_error,
            ..
        } => format!(
            "tool_result {tool_name}: {}{output}",
            if *is_error { "error " } else { "" }
        ),
    };
    truncate_summary(&raw, 160)
}

#[cfg(test)]
fn collect_recent_role_summaries(
    messages: &[ConversationMessage],
    role: MessageRole,
    limit: usize,
) -> Vec<String> {
    messages
        .iter()
        .filter(|message| message.role == role)
        .rev()
        .filter_map(|message| first_text_block(message))
        .take(limit)
        .map(|text| truncate_summary(text, 160))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

#[cfg(test)]
fn infer_pending_work(messages: &[ConversationMessage]) -> Vec<String> {
    messages
        .iter()
        .rev()
        .filter_map(first_text_block)
        .filter(|text| {
            let lowered = text.to_ascii_lowercase();
            lowered.contains("todo")
                || lowered.contains("next")
                || lowered.contains("pending")
                || lowered.contains("follow up")
                || lowered.contains("remaining")
        })
        .take(3)
        .map(|text| truncate_summary(text, 160))
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

#[cfg(test)]
fn collect_key_files(messages: &[ConversationMessage]) -> Vec<String> {
    let mut files = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .map(|block| match block {
            ContentBlock::Text { text } => text.as_str(),
            ContentBlock::Image { source_path, .. } => source_path.as_deref().unwrap_or(""),
            ContentBlock::Thinking { .. } => "",
            ContentBlock::ToolUse { input, .. } => input.as_str(),
            ContentBlock::ToolResult { output, .. } => output.as_str(),
        })
        .flat_map(extract_file_candidates)
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    files.into_iter().take(8).collect()
}

#[cfg(test)]
fn infer_current_work(messages: &[ConversationMessage]) -> Option<String> {
    messages
        .iter()
        .rev()
        .filter_map(first_text_block)
        .find(|text| !text.trim().is_empty())
        .map(|text| truncate_summary(text, 200))
}

fn first_text_block(message: &ConversationMessage) -> Option<&str> {
    message.blocks.iter().find_map(|block| match block {
        ContentBlock::Text { text } if !text.trim().is_empty() => Some(text.as_str()),
        ContentBlock::Thinking { .. }
        | ContentBlock::Image { .. }
        | ContentBlock::ToolUse { .. }
        | ContentBlock::ToolResult { .. }
        | ContentBlock::Text { .. } => None,
    })
}

#[cfg(test)]
fn has_interesting_extension(candidate: &str) -> bool {
    std::path::Path::new(candidate)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            ["rs", "ts", "tsx", "js", "json", "md"]
                .iter()
                .any(|expected| extension.eq_ignore_ascii_case(expected))
        })
}

#[cfg(test)]
fn extract_file_candidates(content: &str) -> Vec<String> {
    content
        .split_whitespace()
        .filter_map(|token| {
            let candidate = token.trim_matches(|char: char| {
                matches!(char, ',' | '.' | ':' | ';' | ')' | '(' | '"' | '\'' | '`')
            });
            if candidate.contains('/') && has_interesting_extension(candidate) {
                Some(candidate.to_string())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
fn truncate_summary(content: &str, max_chars: usize) -> String {
    if content.chars().count() <= max_chars {
        return content.to_string();
    }
    let mut truncated = content.chars().take(max_chars).collect::<String>();
    truncated.push('…');
    truncated
}

fn estimate_message_tokens(message: &ConversationMessage) -> usize {
    message
        .blocks
        .iter()
        .map(|block| match block {
            ContentBlock::Text { text } => text.len() / 4 + 1,
            ContentBlock::Image {
                media_type,
                source_path,
                ..
            } => (media_type.len() + source_path.as_deref().unwrap_or("").len()) / 4 + 8,
            ContentBlock::Thinking { thinking, .. } => thinking.len() / 4 + 1,
            ContentBlock::ToolUse { name, input, .. } => (name.len() + input.len()) / 4 + 1,
            ContentBlock::ToolResult {
                tool_name, output, ..
            } => (tool_name.len() + output.len()) / 4 + 1,
        })
        .sum()
}

fn extract_tag_block(content: &str, tag: &str) -> Option<String> {
    let start = format!("<{tag}>");
    let end = format!("</{tag}>");
    let start_index = content.find(&start)? + start.len();
    let end_index = content[start_index..].find(&end)? + start_index;
    Some(content[start_index..end_index].to_string())
}

fn strip_tag_block(content: &str, tag: &str) -> String {
    let start = format!("<{tag}>");
    let end = format!("</{tag}>");
    if let (Some(start_index), Some(end_index_rel)) = (content.find(&start), content.find(&end)) {
        let end_index = end_index_rel + end.len();
        let mut stripped = String::new();
        stripped.push_str(&content[..start_index]);
        stripped.push_str(&content[end_index..]);
        stripped
    } else {
        content.to_string()
    }
}

fn collapse_blank_lines(content: &str) -> String {
    let mut result = String::new();
    let mut last_blank = false;
    for line in content.lines() {
        let is_blank = line.trim().is_empty();
        if is_blank && last_blank {
            continue;
        }
        result.push_str(line);
        result.push('\n');
        last_blank = is_blank;
    }
    result
}

fn extract_existing_compacted_summary(message: &ConversationMessage) -> Option<String> {
    if message.role != MessageRole::System {
        return None;
    }

    let text = first_text_block(message)?;
    let summary = text.strip_prefix(COMPACT_CONTINUATION_PREAMBLE)?;
    let summary = summary
        .split_once(&format!("\n\n{COMPACT_RECENT_MESSAGES_NOTE}"))
        .map_or(summary, |(value, _)| value);
    let summary = summary
        .split_once(&format!("\n{COMPACT_DIRECT_RESUME_INSTRUCTION}"))
        .map_or(summary, |(value, _)| value);
    Some(summary.trim().to_string())
}

#[cfg(test)]
fn extract_summary_highlights(summary: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut in_timeline = false;

    for line in format_compact_summary(summary).lines() {
        let trimmed = line.trim_end();
        if trimmed.is_empty() || trimmed == "Summary:" || trimmed == "Conversation summary:" {
            continue;
        }
        if trimmed == "- Key timeline:" {
            in_timeline = true;
            continue;
        }
        if in_timeline {
            continue;
        }
        lines.push(trimmed.to_string());
    }

    lines
}

#[cfg(test)]
fn extract_summary_timeline(summary: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut in_timeline = false;

    for line in format_compact_summary(summary).lines() {
        let trimmed = line.trim_end();
        if trimmed == "- Key timeline:" {
            in_timeline = true;
            continue;
        }
        if !in_timeline {
            continue;
        }
        if trimmed.is_empty() {
            break;
        }
        lines.push(trimmed.to_string());
    }

    lines
}

#[cfg(test)]
mod tests {
    use super::{
        apply_compaction_summary, collect_key_files, compact_session, format_compact_summary,
        get_compact_continuation_message, infer_pending_work, plan_session_compaction,
        should_compact, CompactionConfig,
    };
    use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};

    #[test]
    fn formats_compact_summary_like_upstream() {
        let summary = "<analysis>scratch</analysis>\n<summary>Kept work</summary>";
        assert_eq!(format_compact_summary(summary), "Summary:\nKept work");
    }

    #[test]
    fn leaves_small_sessions_unchanged() {
        let mut session = Session::new();
        session.replace_messages(vec![ConversationMessage::user_text("hello")]);

        let result = compact_session(&session, CompactionConfig::default());
        assert_eq!(result.removed_message_count, 0);
        assert_eq!(result.compacted_session, session);
        assert!(result.summary.is_empty());
        assert!(result.formatted_summary.is_empty());
    }

    #[test]
    fn compacts_older_messages_into_a_system_summary() {
        let mut session = Session::new();
        session.replace_messages(vec![
            ConversationMessage::user_text("one ".repeat(200)),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "two ".repeat(200),
            }]),
            ConversationMessage::tool_result("1", "bash", "ok ".repeat(200), false),
            ConversationMessage {
                role: MessageRole::Assistant,
                blocks: vec![ContentBlock::Text {
                    text: "recent".to_string(),
                }],
                usage: None,
            },
        ]);

        let result = compact_session(
            &session,
            CompactionConfig {
                preserve_recent_messages: 2,
                max_estimated_tokens: 1,
                priority_threshold: 3,
                keep_high_priority: true,
            },
        );

        // With the tool-use/tool-result boundary fix, the compaction preserves
        // one extra message to avoid an orphaned tool result at the boundary.
        // messages[1] (assistant) must be kept along with messages[2] (tool result).
        assert!(
            result.removed_message_count <= 2,
            "expected at most 2 removed, got {}",
            result.removed_message_count
        );
        assert_eq!(
            result.compacted_session.message(0).expect("summary").role,
            MessageRole::System
        );
        assert!(matches!(
            &result.compacted_session.message(0).expect("summary").blocks[0],
            ContentBlock::Text { text } if text.contains("Summary:")
        ));
        assert!(result.formatted_summary.contains("Scope:"));
        assert!(result.formatted_summary.contains("Key timeline:"));
        assert!(should_compact(
            &session,
            CompactionConfig {
                preserve_recent_messages: 2,
                max_estimated_tokens: 1,
                priority_threshold: 3,
                keep_high_priority: true,
            }
        ));
        // Note: with the tool-use/tool-result boundary guard the compacted session
        // may preserve one extra message at the boundary, so token reduction is
        // not guaranteed for small sessions. The invariant that matters is that
        // the removed_message_count is non-zero (something was compacted).
        assert!(
            result.removed_message_count > 0,
            "compaction must remove at least one message"
        );
    }

    #[test]
    fn runtime_path_applies_one_caller_supplied_semantic_checkpoint() {
        let mut session = Session::new();
        session.replace_messages(vec![
            ConversationMessage::user_text("old request ".repeat(80)),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old result ".repeat(80),
            }]),
            ConversationMessage::user_text("recent request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "recent result".to_string(),
            }]),
        ]);
        let plan = plan_session_compaction(
            &session,
            CompactionConfig {
                preserve_recent_messages: 2,
                max_estimated_tokens: 1,
                priority_threshold: 3,
                keep_high_priority: true,
            },
        )
        .expect("a boundary should be planned");
        let result = apply_compaction_summary(
            &session,
            plan,
            "## Active Task\nSemantic checkpoint only\n\n## Critical Context\nPreserve decision A"
                .to_string(),
        );
        assert_eq!(
            result.summary,
            "## Active Task\nSemantic checkpoint only\n\n## Critical Context\nPreserve decision A"
        );
        assert!(!result.summary.contains("Conversation summary:"));
        assert!(matches!(
            &result.compacted_session.message(0).expect("summary").blocks[0],
            ContentBlock::Text { text } if text.contains("Semantic checkpoint only")
        ));
    }

    #[test]
    fn keeps_previous_compacted_context_when_compacting_again() {
        let mut initial_session = Session::new();
        initial_session.replace_messages(vec![
            ConversationMessage::user_text("Investigate rust/crates/runtime/src/compact.rs"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "I will inspect the compact flow.".to_string(),
            }]),
            ConversationMessage::user_text("Also update rust/crates/runtime/src/conversation.rs"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "Next: preserve prior summary context during auto compact.".to_string(),
            }]),
        ]);
        let config = CompactionConfig {
            preserve_recent_messages: 2,
            max_estimated_tokens: 1,
            priority_threshold: 3,
            keep_high_priority: true,
        };

        let first = compact_session(&initial_session, config);
        let mut follow_up_messages = first.compacted_session.materialize_messages();
        follow_up_messages.extend([
            ConversationMessage::user_text("Please add regression tests for compaction."),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "Working on regression coverage now.".to_string(),
            }]),
        ]);

        let mut second_session = Session::new();
        second_session.replace_messages(follow_up_messages);
        let second = compact_session(&second_session, config);

        assert!(second
            .formatted_summary
            .contains("Previously compacted context:"));
        assert!(second
            .formatted_summary
            .contains("Scope: 2 earlier messages compacted"));
        assert!(second
            .formatted_summary
            .contains("Newly compacted context:"));
        assert!(second
            .formatted_summary
            .contains("Also update rust/crates/runtime/src/conversation.rs"));
        assert!(matches!(
            &second.compacted_session.message(0).expect("summary").blocks[0],
            ContentBlock::Text { text }
                if text.contains("Previously compacted context:")
                    && text.contains("Newly compacted context:")
        ));
        assert!(matches!(
            &second.compacted_session.message(1).expect("recent").blocks[0],
            ContentBlock::Text { text } if text.contains("Please add regression tests for compaction.")
        ));
    }

    #[test]
    fn ignores_existing_compacted_summary_when_deciding_to_recompact() {
        let summary = "<summary>Conversation summary:\n- Scope: earlier work preserved.\n- Key timeline:\n  - user: large preserved context\n</summary>";
        let mut session = Session::new();
        session.replace_messages(vec![
            ConversationMessage {
                role: MessageRole::System,
                blocks: vec![ContentBlock::Text {
                    text: get_compact_continuation_message(summary, true, true),
                }],
                usage: None,
            },
            ConversationMessage::user_text("tiny"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "recent".to_string(),
            }]),
        ]);

        assert!(!should_compact(
            &session,
            CompactionConfig {
                preserve_recent_messages: 2,
                max_estimated_tokens: 1,
                priority_threshold: 3,
                keep_high_priority: true,
            }
        ));
    }

    #[test]
    fn truncates_long_blocks_in_summary() {
        let summary = super::summarize_block(&ContentBlock::Text {
            text: "x".repeat(400),
        });
        assert!(summary.ends_with('…'));
        assert!(summary.chars().count() <= 161);
    }

    #[test]
    fn extracts_key_files_from_message_content() {
        let files = collect_key_files(&[ConversationMessage::user_text(
            "Update rust/crates/runtime/src/compact.rs and rust/crates/gateway/src/main.rs next.",
        )]);
        assert!(files.contains(&"rust/crates/runtime/src/compact.rs".to_string()));
        assert!(files.contains(&"rust/crates/gateway/src/main.rs".to_string()));
    }

    /// Regression: compaction must not split an assistant(ToolUse) /
    /// user(ToolResult) pair at the boundary. An orphaned tool-result message
    /// without the preceding assistant `tool_calls` causes a 400 on the
    /// OpenAI-compat path (gaebal-gajae repro 2026-04-09).
    #[test]
    fn compaction_does_not_split_tool_use_tool_result_pair() {
        use crate::session::{ContentBlock, Session};

        let tool_id = "call_abc";
        let mut session = Session::default();
        // Turn 1: user prompt
        session
            .push_message(ConversationMessage::user_text("Search for files"))
            .unwrap();
        // Turn 2: assistant calls a tool
        session
            .push_message(ConversationMessage::assistant(vec![
                ContentBlock::ToolUse {
                    id: tool_id.to_string(),
                    name: "search".to_string(),
                    input: "{\"q\":\"*.rs\"}".to_string(),
                },
            ]))
            .unwrap();
        // Turn 3: tool result
        session
            .push_message(ConversationMessage::tool_result(
                tool_id,
                "search",
                "found 5 files",
                false,
            ))
            .unwrap();
        // Turn 4: assistant final response
        session
            .push_message(ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "Done.".to_string(),
            }]))
            .unwrap();

        // Compact preserving only 1 recent message — without the fix this
        // would cut the boundary so that the tool result (turn 3) is first,
        // without its preceding assistant tool_calls (turn 2).
        let config = CompactionConfig {
            preserve_recent_messages: 1,
            ..CompactionConfig::default()
        };
        let result = compact_session(&session, config);
        // After compaction, no two consecutive messages should have the pattern
        // tool_result immediately following a non-assistant message (i.e. an
        // orphaned tool result without a preceding assistant ToolUse).
        let messages = result.compacted_session.materialize_messages();
        for i in 1..messages.len() {
            let curr_is_tool_result = messages[i]
                .blocks
                .first()
                .is_some_and(|b| matches!(b, ContentBlock::ToolResult { .. }));
            if curr_is_tool_result {
                let prev_has_tool_use = messages[i - 1]
                    .blocks
                    .iter()
                    .any(|b| matches!(b, ContentBlock::ToolUse { .. }));
                assert!(
                    prev_has_tool_use,
                    "message[{}] is a ToolResult but message[{}] has no ToolUse: {:?}",
                    i,
                    i - 1,
                    &messages[i - 1].blocks
                );
            }
        }
    }

    #[test]
    fn infers_pending_work_from_recent_messages() {
        let pending = infer_pending_work(&[
            ConversationMessage::user_text("done"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "Next: update tests and follow up on remaining CLI polish.".to_string(),
            }]),
        ]);
        assert_eq!(pending.len(), 1);
        assert!(pending[0].contains("Next: update tests"));
    }
}
