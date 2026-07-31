use std::ops::Range;
use std::sync::Arc;

use super::{ContentBlock, ConversationMessage};

const DEFAULT_CHUNK_MESSAGES: usize = 128;
const DEFAULT_CHUNK_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionHistoryConfig {
    pub chunk_messages: usize,
    pub chunk_bytes: usize,
    pub request_cache_entries: usize,
}

impl SessionHistoryConfig {
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            chunk_messages: self.chunk_messages.max(1),
            chunk_bytes: self.chunk_bytes.max(1),
            request_cache_entries: self.request_cache_entries.max(1),
        }
    }
}

impl Default for SessionHistoryConfig {
    fn default() -> Self {
        Self {
            chunk_messages: DEFAULT_CHUNK_MESSAGES,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            request_cache_entries: 16,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HistoryWeight {
    pub bytes: usize,
    pub tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryCursor {
    pub revision: u64,
    pub position: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryChunk {
    messages: Arc<[ConversationMessage]>,
    weight: HistoryWeight,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistorySlice {
    messages: Arc<[ConversationMessage]>,
    range: Range<usize>,
}

#[derive(Debug, Clone)]
pub struct HistoryView {
    slices: Vec<HistorySlice>,
    cursor: HistoryCursor,
    len: usize,
    weight: HistoryWeight,
}

impl PartialEq for HistoryView {
    fn eq(&self, other: &Self) -> bool {
        self.cursor == other.cursor
            && self.len == other.len
            && self.weight == other.weight
            && self.iter().eq(other.iter())
    }
}

impl Eq for HistoryView {}

impl HistoryView {
    #[must_use]
    pub fn empty(revision: u64, position: usize) -> Self {
        Self {
            slices: Vec::new(),
            cursor: HistoryCursor { revision, position },
            len: 0,
            weight: HistoryWeight::default(),
        }
    }

    #[must_use]
    pub const fn cursor(&self) -> HistoryCursor {
        self.cursor
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub const fn weight(&self) -> HistoryWeight {
        self.weight
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &ConversationMessage> {
        self.slices
            .iter()
            .flat_map(|slice| slice.messages[slice.range.clone()].iter())
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&ConversationMessage> {
        if index >= self.len {
            return None;
        }
        let mut remaining = index;
        for slice in &self.slices {
            let slice_len = slice.range.len();
            if remaining < slice_len {
                return slice.messages.get(slice.range.start + remaining);
            }
            remaining -= slice_len;
        }
        None
    }

    /// Explicitly materialize only this bounded view. Runtime hot paths should
    /// pass the view through to the provider adapter instead.
    #[must_use]
    pub fn materialize(&self) -> Vec<ConversationMessage> {
        self.iter().cloned().collect()
    }

    #[must_use]
    pub fn shared_segment_count_with(&self, other: &Self) -> usize {
        self.slices
            .iter()
            .filter(|left| {
                other
                    .slices
                    .iter()
                    .any(|right| Arc::ptr_eq(&left.messages, &right.messages))
            })
            .count()
    }
}

impl From<Vec<ConversationMessage>> for HistoryView {
    fn from(messages: Vec<ConversationMessage>) -> Self {
        let len = messages.len();
        let weight = messages.iter().fold(HistoryWeight::default(), add_weight);
        let messages: Arc<[ConversationMessage]> = messages.into();
        let slices = (!messages.is_empty())
            .then_some(HistorySlice {
                messages,
                range: 0..len,
            })
            .into_iter()
            .collect();
        Self {
            slices,
            cursor: HistoryCursor {
                revision: 0,
                position: 0,
            },
            len,
            weight,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionHistory {
    config: SessionHistoryConfig,
    chunks: Vec<HistoryChunk>,
    tail: Vec<ConversationMessage>,
    tail_weight: HistoryWeight,
    revision: u64,
    total_messages: usize,
    total_weight: HistoryWeight,
}

impl PartialEq for SessionHistory {
    fn eq(&self, other: &Self) -> bool {
        self.total_messages == other.total_messages
            && self.total_weight == other.total_weight
            && self.iter().eq(other.iter())
    }
}

impl Eq for SessionHistory {}

impl Default for SessionHistory {
    fn default() -> Self {
        Self::new(SessionHistoryConfig::default())
    }
}

impl SessionHistory {
    #[must_use]
    pub fn new(config: SessionHistoryConfig) -> Self {
        Self {
            config: config.normalized(),
            chunks: Vec::new(),
            tail: Vec::new(),
            tail_weight: HistoryWeight::default(),
            revision: 0,
            total_messages: 0,
            total_weight: HistoryWeight::default(),
        }
    }

    #[must_use]
    pub fn from_messages(messages: Vec<ConversationMessage>, config: SessionHistoryConfig) -> Self {
        let mut history = Self::new(config);
        history.extend(messages);
        history
    }

    #[must_use]
    pub const fn config(&self) -> SessionHistoryConfig {
        self.config
    }

    pub fn reconfigure(&mut self, config: SessionHistoryConfig) {
        let config = config.normalized();
        if self.config == config {
            return;
        }
        let messages = self.materialize();
        self.config = config;
        self.rebuild(messages);
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub const fn cursor(&self) -> HistoryCursor {
        HistoryCursor {
            revision: self.revision,
            position: self.total_messages,
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.total_messages
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.total_messages == 0
    }

    #[must_use]
    pub const fn weight(&self) -> HistoryWeight {
        self.total_weight
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &ConversationMessage> {
        self.chunks
            .iter()
            .flat_map(|chunk| chunk.messages.iter())
            .chain(self.tail.iter())
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&ConversationMessage> {
        if index >= self.total_messages {
            return None;
        }
        let mut remaining = index;
        for chunk in &self.chunks {
            if remaining < chunk.messages.len() {
                return chunk.messages.get(remaining);
            }
            remaining -= chunk.messages.len();
        }
        self.tail.get(remaining)
    }

    #[must_use]
    pub fn first(&self) -> Option<&ConversationMessage> {
        self.get(0)
    }

    #[must_use]
    pub fn last(&self) -> Option<&ConversationMessage> {
        self.tail
            .last()
            .or_else(|| self.chunks.last().and_then(|chunk| chunk.messages.last()))
    }

    pub fn append(&mut self, message: ConversationMessage) {
        let weight = message_weight(&message);
        self.tail.push(message);
        self.tail_weight = combine_weight(self.tail_weight, weight);
        self.total_weight = combine_weight(self.total_weight, weight);
        self.total_messages = self.total_messages.saturating_add(1);
        self.revision = self.revision.wrapping_add(1);
        self.seal_tail_if_needed();
    }

    pub fn pop(&mut self) -> Option<ConversationMessage> {
        if let Some(message) = self.tail.pop() {
            self.rebuild_weights();
            self.revision = self.revision.wrapping_add(1);
            return Some(message);
        }
        let chunk = self.chunks.pop()?;
        self.tail = chunk.messages.iter().cloned().collect();
        let message = self.tail.pop();
        self.rebuild_weights();
        self.revision = self.revision.wrapping_add(1);
        message
    }

    pub fn extend(&mut self, messages: impl IntoIterator<Item = ConversationMessage>) {
        for message in messages {
            self.append(message);
        }
    }

    pub fn truncate(&mut self, len: usize) {
        if len >= self.total_messages {
            return;
        }
        let sealed_len = self
            .chunks
            .iter()
            .map(|chunk| chunk.messages.len())
            .sum::<usize>();
        if len >= sealed_len {
            self.tail.truncate(len - sealed_len);
            self.rebuild_weights();
            self.revision = self.revision.wrapping_add(1);
            return;
        }
        let retained = self.iter().take(len).cloned().collect();
        self.rebuild(retained);
    }

    pub fn replace(&mut self, messages: Vec<ConversationMessage>) {
        self.rebuild(messages);
    }

    #[must_use]
    pub fn snapshot(&self) -> HistoryView {
        self.page(0, self.total_messages)
    }

    #[must_use]
    pub fn page(&self, position: usize, limit: usize) -> HistoryView {
        if limit == 0 || position >= self.total_messages {
            return HistoryView::empty(self.revision, position.min(self.total_messages));
        }
        let end = position.saturating_add(limit).min(self.total_messages);
        let mut slices = Vec::new();
        let mut segment_start = 0usize;
        let mut weight = HistoryWeight::default();

        for chunk in &self.chunks {
            let segment_end = segment_start + chunk.messages.len();
            if position < segment_end && end > segment_start {
                let local_start = position.saturating_sub(segment_start);
                let local_end = (end - segment_start).min(chunk.messages.len());
                let slice_weight = chunk.messages[local_start..local_end]
                    .iter()
                    .fold(HistoryWeight::default(), add_weight);
                weight = combine_weight(weight, slice_weight);
                slices.push(HistorySlice {
                    messages: Arc::clone(&chunk.messages),
                    range: local_start..local_end,
                });
            }
            segment_start = segment_end;
            if segment_start >= end {
                break;
            }
        }

        if end > segment_start && position < self.total_messages {
            let local_start = position.saturating_sub(segment_start);
            let local_end = (end - segment_start).min(self.tail.len());
            if local_start < local_end {
                let selected: Arc<[ConversationMessage]> =
                    self.tail[local_start..local_end].to_vec().into();
                weight = selected.iter().fold(weight, add_weight);
                let selected_len = selected.len();
                slices.push(HistorySlice {
                    messages: selected,
                    range: 0..selected_len,
                });
            }
        }

        HistoryView {
            slices,
            cursor: HistoryCursor {
                revision: self.revision,
                position,
            },
            len: end - position,
            weight,
        }
    }

    #[must_use]
    pub fn fork(&self) -> Self {
        let mut chunks = self.chunks.clone();
        if !self.tail.is_empty() {
            chunks.push(HistoryChunk {
                messages: self.tail.clone().into(),
                weight: self.tail_weight,
            });
        }
        Self {
            config: self.config,
            chunks,
            tail: Vec::new(),
            tail_weight: HistoryWeight::default(),
            revision: self.revision,
            total_messages: self.total_messages,
            total_weight: self.total_weight,
        }
    }

    #[must_use]
    pub fn materialize(&self) -> Vec<ConversationMessage> {
        self.iter().cloned().collect()
    }

    #[must_use]
    pub fn shared_chunk_count_with(&self, other: &Self) -> usize {
        self.chunks
            .iter()
            .filter(|left| {
                other
                    .chunks
                    .iter()
                    .any(|right| Arc::ptr_eq(&left.messages, &right.messages))
            })
            .count()
    }

    fn rebuild(&mut self, messages: Vec<ConversationMessage>) {
        let next_revision = self.revision.wrapping_add(1);
        self.chunks.clear();
        self.tail.clear();
        self.tail_weight = HistoryWeight::default();
        self.total_messages = 0;
        self.total_weight = HistoryWeight::default();
        for message in messages {
            let weight = message_weight(&message);
            self.tail.push(message);
            self.tail_weight = combine_weight(self.tail_weight, weight);
            self.total_weight = combine_weight(self.total_weight, weight);
            self.total_messages += 1;
            self.seal_tail_if_needed();
        }
        self.revision = next_revision;
    }

    fn seal_tail_if_needed(&mut self) {
        if self.tail.len() < self.config.chunk_messages
            && self.tail_weight.bytes < self.config.chunk_bytes
        {
            return;
        }
        let messages = std::mem::take(&mut self.tail);
        let weight = std::mem::take(&mut self.tail_weight);
        self.chunks.push(HistoryChunk {
            messages: messages.into(),
            weight,
        });
    }

    fn rebuild_weights(&mut self) {
        self.tail_weight = self.tail.iter().fold(HistoryWeight::default(), add_weight);
        self.total_messages = self
            .chunks
            .iter()
            .map(|chunk| chunk.messages.len())
            .sum::<usize>()
            + self.tail.len();
        self.total_weight = self
            .chunks
            .iter()
            .fold(HistoryWeight::default(), |total, chunk| {
                combine_weight(total, chunk.weight)
            });
        self.total_weight = combine_weight(self.total_weight, self.tail_weight);
    }
}

fn add_weight(total: HistoryWeight, message: &ConversationMessage) -> HistoryWeight {
    combine_weight(total, message_weight(message))
}

fn combine_weight(left: HistoryWeight, right: HistoryWeight) -> HistoryWeight {
    HistoryWeight {
        bytes: left.bytes.saturating_add(right.bytes),
        tokens: left.tokens.saturating_add(right.tokens),
    }
}

fn message_weight(message: &ConversationMessage) -> HistoryWeight {
    let mut bytes = 1usize;
    let mut tokens = 1u64;
    for block in &message.blocks {
        let (block_bytes, block_tokens) = match block {
            ContentBlock::Text { text } => text_weight(text),
            ContentBlock::ReasoningSummary { text } => text_weight(text),
            ContentBlock::Image {
                media_type,
                data,
                source_path,
            } => {
                let path_bytes = source_path.as_ref().map_or(0, String::len);
                (
                    media_type
                        .len()
                        .saturating_add(data.len())
                        .saturating_add(path_bytes),
                    (data.len() as u64)
                        .div_ceil(4)
                        .saturating_add((media_type.len() as u64).div_ceil(4))
                        .saturating_add((path_bytes as u64).div_ceil(4)),
                )
            }
            ContentBlock::Thinking {
                thinking,
                signature,
            } => {
                let signature_bytes = signature.as_ref().map_or(0, String::len);
                let (thinking_bytes, thinking_tokens) = text_weight(thinking);
                (
                    thinking_bytes.saturating_add(signature_bytes),
                    thinking_tokens.saturating_add((signature_bytes as u64).div_ceil(4)),
                )
            }
            ContentBlock::ToolUse { id, name, input } => {
                let size = id
                    .len()
                    .saturating_add(name.len())
                    .saturating_add(input.len());
                (size, (size as u64).div_ceil(4))
            }
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                ..
            } => {
                let size = tool_use_id
                    .len()
                    .saturating_add(tool_name.len())
                    .saturating_add(output.len());
                (size, (size as u64).div_ceil(4))
            }
        };
        bytes = bytes.saturating_add(block_bytes);
        tokens = tokens.saturating_add(block_tokens);
    }
    HistoryWeight { bytes, tokens }
}

fn text_weight(text: &str) -> (usize, u64) {
    (text.len(), (text.len() as u64).div_ceil(4).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn messages(count: usize) -> Vec<ConversationMessage> {
        (0..count)
            .map(|index| ConversationMessage::user_text(format!("message-{index}")))
            .collect()
    }

    #[test]
    fn page_is_bounded_and_preserves_cursor() {
        let history = SessionHistory::from_messages(
            messages(25),
            SessionHistoryConfig {
                chunk_messages: 4,
                chunk_bytes: usize::MAX,
                ..SessionHistoryConfig::default()
            },
        );
        let page = history.page(7, 5);
        assert_eq!(page.len(), 5);
        assert_eq!(page.cursor().position, 7);
        assert_eq!(page.cursor().revision, history.revision());
        assert_eq!(page.get(0), history.get(7));
        assert_eq!(page.get(4), history.get(11));
    }

    #[test]
    fn fork_shares_sealed_chunks_and_isolates_mutation() {
        let mut parent = SessionHistory::from_messages(
            messages(20),
            SessionHistoryConfig {
                chunk_messages: 4,
                chunk_bytes: usize::MAX,
                ..SessionHistoryConfig::default()
            },
        );
        let mut child = parent.fork();
        assert!(parent.shared_chunk_count_with(&child) >= 5);
        parent.append(ConversationMessage::user_text("parent"));
        child.append(ConversationMessage::user_text("child"));
        assert_ne!(parent.last(), child.last());
        assert_eq!(parent.len(), child.len());
    }

    #[test]
    fn replace_and_truncate_advance_revision() {
        let mut history =
            SessionHistory::from_messages(messages(10), SessionHistoryConfig::default());
        let initial = history.revision();
        history.truncate(4);
        assert_eq!(history.len(), 4);
        assert_ne!(history.revision(), initial);
        let truncated = history.revision();
        history.replace(messages(2));
        assert_eq!(history.len(), 2);
        assert_ne!(history.revision(), truncated);
    }

    #[test]
    fn bounded_views_scale_with_selection_not_total_history() {
        for total in [1_000, 10_000, 50_000] {
            let history = SessionHistory::from_messages(
                messages(total),
                SessionHistoryConfig {
                    chunk_messages: 128,
                    chunk_bytes: usize::MAX,
                    ..SessionHistoryConfig::default()
                },
            );
            let first = history.page(total / 2, 256);
            let second = history.page(total / 2, 256);
            assert_eq!(first.len(), 256);
            assert!(first.shared_segment_count_with(&second) >= 2);
            assert_eq!(first.materialize(), second.materialize());

            let fork = history.fork();
            assert_eq!(fork.len(), total);
            assert_eq!(history.shared_chunk_count_with(&fork), history.chunks.len());
        }
    }
}
