//! Per-chat text and media batching for Feishu platform.
//!
//! Accumulates messages for a configurable delay, then flushes up to max
//! messages/chars in a single batch. Matches Hermes' `FeishuBatchState` pattern.
//!
//! Two managers are provided:
//! - [`TextBatchManager`] — batches text messages with character-based splitting
//! - [`MediaBatchManager`] — batches media references with longer default delay

use crate::platform::adapter::PlatformResult;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Trait — decouples batch manager from any specific platform adapter
// ---------------------------------------------------------------------------

/// Sender callback for batched messages.
///
/// Implement this trait on your platform adapter (or a wrapper) to connect
/// the batch managers to the actual message-sending transport.
#[async_trait::async_trait]
pub trait BatchSender: Send + Sync {
    /// Send a single batched text (possibly one fragment of a split batch)
    /// to the given chat.
    async fn send_batch(&self, chat_id: &str, text: &str) -> PlatformResult<()>;
}

// ---------------------------------------------------------------------------
// Hermes-default constants
// ---------------------------------------------------------------------------

const DEFAULT_TEXT_BATCH_DELAY_MS: u64 = 600;
const DEFAULT_TEXT_BATCH_MAX_MESSAGES: usize = 8;
const DEFAULT_TEXT_BATCH_MAX_CHARS: usize = 4000;
const DEFAULT_MEDIA_BATCH_DELAY_MS: u64 = 800;

// ---------------------------------------------------------------------------
// TextBatchManager
// ---------------------------------------------------------------------------

/// Per-chat state kept inside the batch manager.
struct ChatState {
    messages: Vec<String>,
    /// Handle of the spawned flush timer, if one is running.
    timer: Option<tokio::task::JoinHandle<()>>,
}

/// Accumulates text messages per chat and flushes them after a configurable
/// delay. Long accumulated texts are split at sentence / paragraph boundaries
/// respecting UTF-8 character boundaries.
pub struct TextBatchManager {
    buffers: Arc<RwLock<HashMap<String, ChatState>>>,
    delay_ms: u64,
    max_messages: usize,
    max_chars: usize,
    sender: Arc<dyn BatchSender>,
}

impl TextBatchManager {
    /// Create a manager with the given batching parameters.
    ///
    /// * `delay_ms` — how long to wait after the first message before flushing
    /// * `max_messages` — hard cap on the number of individual messages in a batch
    /// * `max_chars` — soft cap; if the joined text exceeds this limit it is split
    /// * `sender` — callback invoked when a batch is ready
    pub fn new(
        delay_ms: u64,
        max_messages: usize,
        max_chars: usize,
        sender: Arc<dyn BatchSender>,
    ) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            delay_ms,
            max_messages,
            max_chars,
            sender,
        }
    }

    /// Create a manager with Hermes default constants.
    pub fn with_defaults(sender: Arc<dyn BatchSender>) -> Self {
        Self::new(
            DEFAULT_TEXT_BATCH_DELAY_MS,
            DEFAULT_TEXT_BATCH_MAX_MESSAGES,
            DEFAULT_TEXT_BATCH_MAX_CHARS,
            sender,
        )
    }

    /// Queue a text message for batching.
    ///
    /// Messages for the same `chat_id` are accumulated.  The first message
    /// received for a chat starts a delay timer; when it fires all accumulated
    /// messages are joined and sent (split if the total exceeds `max_chars`).
    pub async fn queue(&self, chat_id: &str, text: &str) {
        let chat_id = chat_id.to_string();

        {
            let mut bufs = self.buffers.write().await;
            let state = bufs.entry(chat_id.clone()).or_insert_with(|| ChatState {
                messages: Vec::new(),
                timer: None,
            });

            state.messages.push(text.to_string());

            // Truncate to max_messages (drop oldest).
            while state.messages.len() > self.max_messages {
                state.messages.remove(0);
            }

            // If a timer is already running we just appended to the buffer;
            // no need to spawn another one.
            if state.timer.is_some() {
                return;
            }

            // First message for this chat — spawn a flush timer.
            let buffers = Arc::clone(&self.buffers);
            let sender = Arc::clone(&self.sender);
            let delay = std::time::Duration::from_millis(self.delay_ms);
            let max_chars = self.max_chars;
            let cid = chat_id.clone();

            let handle = tokio::spawn(async move {
                tokio::time::sleep(delay).await;

                let messages: Vec<String> = {
                    let mut bufs = buffers.write().await;
                    if let Some(state) = bufs.get_mut(&cid) {
                        // Clear timer handle (it's about to finish).
                        state.timer = None;
                        // Take all messages so nobody else sends them twice.
                        std::mem::take(&mut state.messages)
                    } else {
                        // Flushed by flush_all before timer fired — nothing to do.
                        return;
                    }
                };

                if messages.is_empty() {
                    return;
                }

                let joined = messages.join("\n");
                let parts = split_long_text(&joined, max_chars);
                for part in parts {
                    let _ = sender.send_batch(&cid, &part).await;
                }
            });

            // Store the handle so flush_all can abort it.
            if let Some(state) = bufs.get_mut(&chat_id) {
                state.timer = Some(handle);
            }
        }
    }

    /// Flush ALL pending batches immediately.
    ///
    /// Cancels any running timers and sends every chat's accumulated messages
    /// right away.
    pub async fn flush_all(&self) {
        let pending: Vec<(String, Vec<String>)> = {
            let mut bufs = self.buffers.write().await;
            let mut out = Vec::with_capacity(bufs.len());

            for (cid, state) in bufs.iter_mut() {
                // Cancel the timer if one is running.
                if let Some(handle) = state.timer.take() {
                    handle.abort();
                }
                let msgs = std::mem::take(&mut state.messages);
                if !msgs.is_empty() {
                    out.push((cid.clone(), msgs));
                }
            }

            out
        };

        let max_chars = self.max_chars;
        for (cid, messages) in pending {
            let joined = messages.join("\n");
            let parts = split_long_text(&joined, max_chars);
            for part in parts {
                let _ = self.sender.send_batch(&cid, &part).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// MediaBatchManager
// ---------------------------------------------------------------------------

/// Accumulates media references per chat and flushes them after a configurable
/// delay.  Uses the same batching pattern as [`TextBatchManager`] but with a
/// longer default delay (800 ms) and no character-based splitting.
pub struct MediaBatchManager {
    buffers: Arc<RwLock<HashMap<String, ChatState>>>,
    delay_ms: u64,
    sender: Arc<dyn BatchSender>,
}

impl MediaBatchManager {
    /// Create a media batch manager.
    ///
    /// * `delay_ms` — how long to wait after the first media reference before
    ///   flushing
    /// * `sender` — callback invoked when a batch is ready
    pub fn new(delay_ms: u64, sender: Arc<dyn BatchSender>) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            delay_ms,
            sender,
        }
    }

    /// Create a manager with the Hermes default delay (800 ms).
    pub fn with_defaults(sender: Arc<dyn BatchSender>) -> Self {
        Self::new(DEFAULT_MEDIA_BATCH_DELAY_MS, sender)
    }

    /// Queue a media reference (file_key, URL, etc.) for batching.
    ///
    /// Messages for the same `chat_id` are accumulated.  The first message
    /// starts a delay timer; when it fires all accumulated references are
    /// joined (newline-separated) and sent as a single message.
    pub async fn queue(&self, chat_id: &str, text: &str) {
        let chat_id = chat_id.to_string();

        {
            let mut bufs = self.buffers.write().await;
            let state = bufs.entry(chat_id.clone()).or_insert_with(|| ChatState {
                messages: Vec::new(),
                timer: None,
            });

            state.messages.push(text.to_string());

            if state.timer.is_some() {
                return;
            }

            let buffers = Arc::clone(&self.buffers);
            let sender = Arc::clone(&self.sender);
            let delay = std::time::Duration::from_millis(self.delay_ms);
            let cid = chat_id.clone();

            let handle = tokio::spawn(async move {
                tokio::time::sleep(delay).await;

                let messages: Vec<String> = {
                    let mut bufs = buffers.write().await;
                    if let Some(state) = bufs.get_mut(&cid) {
                        state.timer = None;
                        std::mem::take(&mut state.messages)
                    } else {
                        return;
                    }
                };

                if messages.is_empty() {
                    return;
                }

                let joined = messages.join("\n");
                let _ = sender.send_batch(&cid, &joined).await;
            });

            if let Some(state) = bufs.get_mut(&chat_id) {
                state.timer = Some(handle);
            }
        }
    }

    /// Flush ALL pending media batches immediately.
    pub async fn flush_all(&self) {
        let pending: Vec<(String, Vec<String>)> = {
            let mut bufs = self.buffers.write().await;
            let mut out = Vec::with_capacity(bufs.len());

            for (cid, state) in bufs.iter_mut() {
                if let Some(handle) = state.timer.take() {
                    handle.abort();
                }
                let msgs = std::mem::take(&mut state.messages);
                if !msgs.is_empty() {
                    out.push((cid.clone(), msgs));
                }
            }

            out
        };

        for (cid, messages) in pending {
            let joined = messages.join("\n");
            let _ = self.sender.send_batch(&cid, &joined).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Long-text splitting
// ---------------------------------------------------------------------------

/// Split `text` into chunks no larger than `max_chars` (character count, not
/// bytes).  Prefers to split on:
///
/// 1. paragraph boundary (`\n\n`)
/// 2. line break (`\n`)
/// 3. sentence boundary (`. `, `! `, `? `)
/// 4. space
/// 5. hard character-position cut
///
/// All splits are on UTF-8 character boundaries.
fn split_long_text(text: &str, max_chars: usize) -> Vec<String> {
    let mut parts = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        let char_count = remaining.chars().count();
        if char_count <= max_chars {
            parts.push(remaining.to_string());
            break;
        }

        // Byte position of the (max_chars)-th character.
        let max_byte = remaining
            .char_indices()
            .nth(max_chars)
            .map(|(i, _)| i)
            .unwrap_or(remaining.len());

        let window = &remaining[..max_byte];
        let split_byte = find_best_split(window);

        parts.push(remaining[..split_byte].to_string());
        remaining = remaining[split_byte..].trim_start();
    }

    parts
}

/// Find the best split point within `window`, preferring natural boundaries
/// over hard cuts.  Returns a byte offset.
fn find_best_split(window: &str) -> usize {
    // 1. Paragraph break (double newline)
    if let Some(pos) = window.rfind("\n\n") {
        return pos + 2;
    }

    // 2. Single line break
    if let Some(pos) = window.rfind('\n') {
        return pos + 1;
    }

    // 3. Sentence boundary — period / exclamation / question followed by
    //    either a space or end-of-window.
    for delim in &[". ", "! ", "? "] {
        if let Some(pos) = window.rfind(delim) {
            return pos + delim.len();
        }
    }

    // 4. Any space
    if let Some(pos) = window.rfind(' ') {
        return pos + 1;
    }

    // 5. Hard cut — return the full window length (already at a char boundary
    //    because it was derived from char_indices).
    window.len()
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::adapter::PlatformResult;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // Mock sender — stores everything sent so tests can inspect it
    // -----------------------------------------------------------------------

    struct MockSender {
        sent: Mutex<Vec<(String, String)>>,
    }

    impl MockSender {
        fn new() -> Self {
            Self {
                sent: Mutex::new(Vec::new()),
            }
        }

        fn take(&self) -> Vec<(String, String)> {
            std::mem::take(&mut *self.sent.lock().unwrap())
        }
    }

    #[async_trait::async_trait]
    impl BatchSender for MockSender {
        async fn send_batch(&self, chat_id: &str, text: &str) -> PlatformResult<()> {
            self.sent
                .lock()
                .unwrap()
                .push((chat_id.to_string(), text.to_string()));
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // split_long_text — unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_split_short_text_is_unchanged() {
        let result = split_long_text("Hello", 4000);
        assert_eq!(result, vec!["Hello".to_string()]);
    }

    #[test]
    fn test_split_paragraph_boundary() {
        let text = "Line one.\n\nLine two.\n\nLine three.";
        let result = split_long_text(text, 15);
        // Split at \n\n; boundary chars stay with the first part.
        assert_eq!(
            result,
            vec!["Line one.\n\n", "Line two.\n\n", "Line three."]
        );
    }

    #[test]
    fn test_split_line_boundary() {
        let text = "First\nSecond\nThird\nFourth";
        let result = split_long_text(text, 10);
        // Each line fits within 10 chars, splits on \n boundaries.
        assert_eq!(result, vec!["First\n", "Second\n", "Third\n", "Fourth"]);
    }

    #[test]
    fn test_split_sentence_boundary() {
        let text = "Hello world. This is a test! More text here. Goodbye.";
        let result = split_long_text(text, 25);
        // Boundary characters (. / ! / ? followed by space) stay with first part.
        assert_eq!(result[0], "Hello world. ");
        assert_eq!(result[1], "This is a test! ");
        assert_eq!(result[2], "More text here. Goodbye.");
    }

    #[test]
    fn test_split_on_space_fallback() {
        let text = "word1 word2 word3 word4 word5 word6 word7";
        let result = split_long_text(text, 25);
        // Should split on space boundaries within the 25-char window.
        for part in &result {
            assert!(part.chars().count() <= 25, "part too long: {part}");
        }
        // Rejoined should be the same words (minus whitespace collapse).
        let rejoined: String = result.join(" ");
        assert!(rejoined.contains("word1"));
        assert!(rejoined.contains("word7"));
    }

    #[test]
    fn test_split_hard_cut_when_no_boundary() {
        // A string with no spaces, line breaks, or sentence markers.
        let text = "abcdefghijklmnopqrstuvwxyz0123456789";
        let result = split_long_text(text, 10);
        for part in &result {
            assert!(part.chars().count() <= 10, "part too long: {part}");
        }
        assert!(result.len() > 1, "should have been split");
    }

    #[test]
    fn test_split_respects_utf8_boundaries() {
        // Emoji are multi-byte UTF-8 characters.
        let text = "😀😃😄😁😆😅🤣😂😊😇🙂🙃😉😌😍🥰😘😗😙😚😋😛😝😜🤪";
        let result = split_long_text(text, 5);
        // Every part should be valid UTF-8 and no longer than 5 chars.
        for part in &result {
            assert!(part.chars().count() <= 5, "part too long: {part}");
            // Should re-encode without error.
            let _ = part.as_str();
        }
    }

    // -----------------------------------------------------------------------
    // TextBatchManager — integration tests
    // -----------------------------------------------------------------------

    /// Verify that multiple messages for the same chat are accumulated and
    /// sent as a single joined message.
    #[tokio::test]
    async fn test_batch_accumulates_messages() {
        let mock = Arc::new(MockSender::new());
        let mgr = TextBatchManager::new(
            50, // 50 ms delay — short for test
            8,
            4000,
            mock.clone() as Arc<dyn BatchSender>,
        );

        // Queue 3 messages for the same chat.
        mgr.queue("chat-1", "hello").await;
        mgr.queue("chat-1", "world").await;
        mgr.queue("chat-1", "foo").await;

        // Wait for the timer to fire.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let sent = mock.take();
        assert_eq!(sent.len(), 1, "should have sent one batch");
        assert_eq!(sent[0].0, "chat-1");
        assert_eq!(sent[0].1, "hello\nworld\nfoo");
    }

    /// Verify the timer fires after (approximately) the configured delay.
    #[tokio::test]
    async fn test_timer_fires_after_delay() {
        let mock = Arc::new(MockSender::new());
        // Use a short delay so the test runs quickly.
        let mgr = TextBatchManager::new(30, 8, 4000, mock.clone() as Arc<dyn BatchSender>);

        mgr.queue("chat-1", "msg1").await;

        // Sleep less than the delay — nothing sent yet.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(mock.take().is_empty(), "should not have fired yet");

        // Sleep past the delay — the timer should fire.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let sent = mock.take();
        assert_eq!(sent.len(), 1, "should have sent after delay");
        assert_eq!(sent[0].0, "chat-1");
        assert_eq!(sent[0].1, "msg1");
    }

    /// Verify long messages are split at natural boundaries.
    #[tokio::test]
    async fn test_long_message_is_split() {
        let mock = Arc::new(MockSender::new());
        let mgr = TextBatchManager::new(
            30,
            8,
            50, // very small max_chars to force splits
            mock.clone() as Arc<dyn BatchSender>,
        );

        // Build a message that will exceed 50 chars.
        let long_line = "The quick brown fox jumps over the lazy dog. ";
        let text = long_line.repeat(10); // ~440 chars
        mgr.queue("chat-1", &text).await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let sent = mock.take();
        assert!(
            sent.len() > 1,
            "long message should be split into multiple parts, got {}",
            sent.len()
        );
        for (chat_id, part) in &sent {
            assert_eq!(chat_id, "chat-1");
            assert!(
                part.chars().count() <= 50,
                "part too long ({} chars): {part}",
                part.chars().count()
            );
        }
    }

    /// Verify different chat_ids have independent buffers.
    #[tokio::test]
    async fn test_multiple_chat_ids_independent_buffers() {
        let mock = Arc::new(MockSender::new());
        let mgr = TextBatchManager::new(40, 8, 4000, mock.clone() as Arc<dyn BatchSender>);

        // Queue messages for two different chats.
        mgr.queue("chat-A", "A1").await;
        mgr.queue("chat-B", "B1").await;
        mgr.queue("chat-A", "A2").await;
        mgr.queue("chat-B", "B2").await;

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut sent = mock.take();
        sent.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(sent.len(), 2, "should have two batches (one per chat)");
        assert_eq!(sent[0].0, "chat-A");
        assert_eq!(sent[0].1, "A1\nA2");
        assert_eq!(sent[1].0, "chat-B");
        assert_eq!(sent[1].1, "B1\nB2");
    }

    /// Verify flush_all sends everything immediately and prevents duplicate
    /// sends from the now-cancelled timer.
    #[tokio::test]
    async fn test_flush_all_clears_everything() {
        let mock = Arc::new(MockSender::new());
        let mgr = TextBatchManager::new(100, 8, 4000, mock.clone() as Arc<dyn BatchSender>);

        mgr.queue("chat-1", "msg1").await;
        mgr.queue("chat-2", "msg2").await;
        mgr.queue("chat-1", "msg3").await;

        mgr.flush_all().await;

        let mut sent = mock.take();
        sent.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(sent.len(), 2, "should have flushed both chats");
        assert_eq!(sent[0].0, "chat-1");
        assert_eq!(sent[0].1, "msg1\nmsg3");
        assert_eq!(sent[1].0, "chat-2");
        assert_eq!(sent[1].1, "msg2");

        // Wait past the timer delay — it was aborted, nothing extra.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(mock.take().is_empty(), "nothing extra after flush_all");
    }

    /// Verify that when flush_all is called before the timer fires, the timer
    /// does not send a duplicate batch.
    #[tokio::test]
    async fn test_flush_all_cancels_pending_timer() {
        let mock = Arc::new(MockSender::new());
        let mgr = TextBatchManager::new(200, 8, 4000, mock.clone() as Arc<dyn BatchSender>);

        mgr.queue("chat-1", "hello").await;

        // Flush immediately (well before the 200 ms timer).
        mgr.flush_all().await;

        let first_batch = mock.take();
        assert_eq!(first_batch.len(), 1, "flush_all sent the batch");

        // Wait past the timer delay. The aborted timer must not send again.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        assert!(
            mock.take().is_empty(),
            "aborted timer must not send duplicate"
        );
    }

    // -----------------------------------------------------------------------
    // MediaBatchManager — integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_media_batch_accumulates() {
        let mock = Arc::new(MockSender::new());
        let mgr = MediaBatchManager::new(30, mock.clone() as Arc<dyn BatchSender>);

        mgr.queue("chat-1", "file_key_001").await;
        mgr.queue("chat-1", "file_key_002").await;

        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let sent = mock.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "chat-1");
        assert_eq!(sent[0].1, "file_key_001\nfile_key_002");
    }

    #[tokio::test]
    async fn test_media_flush_all() {
        let mock = Arc::new(MockSender::new());
        let mgr = MediaBatchManager::new(800, mock.clone() as Arc<dyn BatchSender>);

        mgr.queue("chat-A", "img_key_1").await;
        mgr.queue("chat-A", "img_key_2").await;

        mgr.flush_all().await;

        let sent = mock.take();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "chat-A");
        assert_eq!(sent[0].1, "img_key_1\nimg_key_2");
    }

    #[test]
    fn test_defaults_match_hermes() {
        assert_eq!(DEFAULT_TEXT_BATCH_DELAY_MS, 600);
        assert_eq!(DEFAULT_TEXT_BATCH_MAX_MESSAGES, 8);
        assert_eq!(DEFAULT_TEXT_BATCH_MAX_CHARS, 4000);
        assert_eq!(DEFAULT_MEDIA_BATCH_DELAY_MS, 800);
    }
}
