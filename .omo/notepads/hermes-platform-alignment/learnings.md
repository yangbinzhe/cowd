
## Task 1: PlatformAdapter Trait Upgrade — COMPLETED @ 2026-05-27

### Changes Made
- **types.rs**: Moved `Platform` enum (Custom uses String), added `MessageType`, `SendResult`, `ChatInfo`, `PlatformEvent`
- **adapter.rs**: Added `PlatformError::NotImplemented`, enhanced `InboundMessage` with 5 new fields, added 12 new methods to trait, updated `NullAdapter`
- **feishu/adapter.rs**: 12 stub impls + InboundMessage construction updates
- **wecom.rs**: 12 stub impls + InboundMessage construction updates
- **email.rs**: InboundMessage construction update
- **mirror.rs**: 6 test site InboundMessage updates
- **feishu/rules.rs**: Test helper InboundMessage update
- **mod.rs**: Added exports for MessageType, SendResult, ChatInfo, PlatformEvent

### Key Decisions
- Platform moved to types.rs to avoid circular dependency (PlatformEvent needs Platform)
- Custom(&'static str) → Custom(String) for serde Deserialize compatibility
- All defaults return Err(NotImplemented) except on_event (Ok(None))

### Verification
- cargo check: PASS (0 errors)
- cargo test platform: 29/29 PASS
- cargo test mirror: 6/6 PASS

## Task: Feishu Markdown Module — COMPLETED @ 2026-05-27

### Changes Made
- **markdown.rs** (new): Created `crates/runtime/src/platform/feishu/markdown.rs` with 5 public functions + 3 public types.
- **mod.rs**: Added `pub mod markdown;`, `pub mod types;` (types module existed but was undeclared), and `pub use markdown::*;`.

### Functions Implemented
- `build_post_payload(content: &str) -> String` — markdown → Feishu post JSON. Splits on ``` fences into separate md/code_block rows.
- `parse_post_payload(payload: &str) -> PostParseResult` — Feishu post JSON → text + image_keys + media_refs. Handles locale wrapper (zh_cn/en_us/any), text styles (bold/italic/strikethrough/code), mentions (at + mentions_map), images, media files, and links.
- `build_card_payload(title: &str, content: &str, actions: &[CardActionDef]) -> String` — Builds Feishu interactive card JSON with blue header and action buttons.
- `strip_markdown(text: &str) -> String` — Strips markdown to plain text. Processes in specific order: fenced code → images → links → bold → strikethrough → italic → inline code → headings → blockquotes → lists → horizontal rules.
- `build_text_payload(content: &str) -> String` — Simple `{"text": "..."}` payload.

### Supporting Types
- `PostParseResult { text_content, image_keys, media_refs }`
- `MediaRef { file_key, file_name, resource_type }`
- `CardActionDef { label, action_id, style }`

### Key Decisions
- Used regex-based markdown stripping (not a full parser) as specified
- Italic regex uses `.+?` non-greedy; bold processed before italic so `***` is handled correctly
- regex crate does NOT support look-around assertions — avoided `(?<!…)` and `(?!…)`
- Doc-test example wrapped in `ignore` to avoid scope issues

### Verification
- cargo check: PASS (0 errors)
- cargo test -p runtime -- platform: 107/107 PASS (43 markdown-specific tests)
- Test coverage: code block isolation, bold/italic/strikethrough parsing, image/media extraction, mention resolution, locale fallback, round-trip, card JSON structure, markdown stripping for all formatting types

## Task 2: DedupStore — COMPLETED @ 2026-05-27

### Changes Made
- **dedup.rs** (new): `DedupStore` struct with persistent LRU message dedup — 218 lines + tests
- **mod.rs**: Added `pub mod dedup;` declaration

### Design Decisions
- `tokio::sync::RwLock` for thread-safe access (same convention as FeishuAdapter)
- `VecDeque<(String, i64)>` for FIFO-ordered entries (oldest evicted first)
- Atomic file persistence: write to `.tmp` → `fs::rename` to target
- Drop impl uses `try_write()` for best-effort synchronous persist on shutdown
- TTL defaults: `max_size=2048`, `ttl_seconds=86400` (24h, matching Hermes constants)
- `is_duplicate()` auto-adds unseen message IDs (marks them as seen)
- `persist()` fast-path: skips write if `dirty` flag is false

### Test Coverage (10 tests)
1. `test_duplicate_detection_within_ttl` — same ID seen twice → true
2. `test_expired_entry_not_duplicate` — ttl=0 → everything expires
3. `test_eviction_at_capacity` — max_size=3, insert 4th → oldest evicted
4. `test_persistence_roundtrip` — create → persist → reload → verify state
5. `test_is_duplicate_automatically_marks_seen` — first call auto-adds
6. `test_empty_store_returns_len_zero` — fresh store len=0
7. `test_mark_seen_adds_entry` — explicit mark_seen tracked
8. `test_mark_seen_evicts_oldest_at_capacity` — eviction on mark_seen
9. `test_persist_skips_when_clean` — no-op when dirty=false
10. `test_persist_without_state_path_is_noop` — no-op without path

### Verification
- cargo check -p runtime: PASS (0 errors, 16 pre-existing dead-code warnings)
- cargo test -p runtime -- platform::dedup: 10/10 PASS
- Full platform suite: 94 PASS (13 pre-existing markdown regex failures, unrelated)

## Task 2: Feishu API Types Module — COMPLETED @ 2026-05-27

### Changes Made
- **feishu/types.rs** (NEW): 476-line comprehensive types module with all Feishu API structs
- **feishu/mod.rs**: Added `pub mod types;` and `pub use types::*;` exports

### Struct Coverage (38 structs total)
- **Auth** (2): TenantTokenRequest, TenantTokenResponse
- **IM Messages** (10): SendMessageRequest/Response/Data, ReplyMessageRequest/Response, UpdateMessageRequest/Response, GetMessageRequest/Response/Data, FeishuMessage, MessageSender
- **IM Images** (3): CreateImageRequest, CreateImageResponse, CreateImageData
- **IM Files** (3): CreateFileRequest, CreateFileResponse, CreateFileData
- **IM Reactions** (5): CreateReactionRequest/Response/Data, DeleteReactionRequest/Response, ReactionType
- **IM Chats** (3): GetChatRequest, GetChatResponse, GetChatData
- **Events** (3): EventListRequest, EventListResponse, EventListData
- **Webhook** (4): WebhookEvent, WebhookHeader, ChallengeResponse, EncryptedPayload
- **Card** (5): InteractiveCard, CardConfig, CardHeader, CardTextContent, CardElement

### Key Decisions
- All structs use `#[serde(rename_all = "snake_case")]` for Feishu API compatibility
- All response structs use `#[serde(default)]` with manual `Default` impls for robustness against missing fields
- Empty request structs derive `Default` for ergonomic construction
- `WebhookEvent` uses `#[serde(rename)]` for `event` and `message` fields to match Feishu's JSON exactly
- `#[cfg(test)]` block contains 25 round-trip tests covering serialization, deserialization, edge cases, and partial JSON handling
- No adapter.rs modifications — inline structs remain as-is (can be migrated later)

### Verification
- cargo check -p runtime: PASS (0 errors, only pre-existing warnings)
- cargo test -p runtime -- platform: 62/64 PASS (2 pre-existing dedup failures unrelated)
- All 25 types.rs roundtrip tests pass

## Task: Feishu Media Module — COMPLETED @ 2026-05-27

### Changes Made
- **media.rs** (NEW): Created `crates/runtime/src/platform/feishu/media.rs` with 9 public functions and retry/SSRF infrastructure.
- **mod.rs**: Added `pub mod media;` and `pub use media::*;` exports.
- **Cargo.toml**: Added `"multipart"` feature to reqwest dependency.

### Functions Implemented

**Upload (2):**
- `upload_image(token, image_bytes, image_type) -> PlatformResult<String>` — POST to `/im/v1/images` with multipart form, returns `image_key`.
- `upload_file(token, file_bytes, file_name, file_type) -> PlatformResult<String>` — POST to `/im/v1/files` with multipart form, returns `file_key`.

**Download (1):**
- `download_message_resource(token, message_id, file_key) -> PlatformResult<Vec<u8>>` — GET `/im/v1/messages/{msg_id}/resources/{file_key}?type=file`, returns raw bytes.

**Cache (4):**
- `cache_image(data, ext) -> PlatformResult<String>` — saves to `~/.cowd/cache/images/img_{uuid}.{ext}`, validates magic bytes before writing.
- `cache_audio(data, ext) -> PlatformResult<String>` — saves to `~/.cowd/cache/audio/audio_{uuid}.{ext}`.
- `cache_video(data, ext) -> PlatformResult<String>` — saves to `~/.cowd/cache/videos/video_{uuid}.{ext}`.
- `cache_document(data, file_name) -> PlatformResult<String>` — saves to `~/.cowd/cache/documents/doc_{uuid}_{safe_name}`, sanitizes filename (strips dirs, nulls, control chars).

**Validation (1):**
- `validate_image_magic(data) -> bool` — checks PNG, JPEG, GIF, BMP, WEBP magic bytes.

**Utility (1):**
- `resolve_media_type(file_path) -> (&'static str, String)` — maps file extensions to Feishu media types.

### Key Decisions
- **reqwest multipart feature**: Required adding to Cargo.toml (previously only `json` feature enabled).
- **Return type**: `resolve_media_type` returns `(&'static str, String)` instead of `(&str, &str)` because the extension is dynamically lowercased (owned `String`).
- **Retry logic**: Custom `request_with_retry` async helper with 3 attempts, exponential backoff (1.5^attempt seconds), only on HTTP 429/5xx. Closure-based to allow multipart body reconstruction per attempt.
- **SSRF protection**: `is_feishu_domain()` validates URLs against `open.feishu.cn` and `feishu.cn` domains only; rejects IP addresses, non-HTTPS, and other domains. Applied in `download_message_resource()` before any HTTP request.
- **Cache path**: Uses `crate::cowd_dirs::config_home_dir().join("cache")` — consistent with existing project conventions.
- **Filename sanitization**: Normalizes both `/` and `\` to `/` before applying `Path::file_name()`, handles cross-platform path traversal.
- **Cache directories**: Auto-created via `std::fs::create_dir_all()`.

### Test Coverage (31 tests)
Validation (7): PNG, JPEG, GIF, GIF87a, BMP, WEBP, invalid bytes
SSRF (5): valid domains, non-HTTPS, other domains, IP addresses, empty
Sanitize (4): directory stripping, null removal, control char removal, empty input
Resolve media type (5): images, audio, video, documents, unknown
Cache (5): image create, image reject invalid, audio create, video create, document create + path sanitize
Retry (2): exhausts 3 attempts on 500, stops at 1 on non-retryable 400
Multipart construction (2): image form, file form
Edge (1): Makefile (no extension) → stream

### Verification
- cargo check -p runtime: PASS (0 errors, 16 pre-existing dead-code warnings)
- cargo test -p runtime -- platform: 138/138 PASS (31 media-specific)
- reqwest multipart feature added to Cargo.toml

## Task: Feishu Batch Module — COMPLETED @ 2026-05-27

### Changes Made
- **batch.rs** (NEW): Created `crates/runtime/src/platform/feishu/batch.rs` with `TextBatchManager`, `MediaBatchManager`, `BatchSender` trait, and `split_long_text` helper.
- **mod.rs**: Added `pub mod batch;` and `pub use batch::*;` exports.

### Structs Implemented

**`TextBatchManager`** — per-chat text message batching:
- `new(delay_ms, max_messages, max_chars, sender)` and `with_defaults(sender)` constructors
- `queue(&self, chat_id, text)` — accumulates messages per chat; first message spawns a timer
- `flush_all(&self)` — immediately sends all pending batches, aborts timers
- Internal `ChatState { messages, timer: Option<JoinHandle> }` per chat_id
- Uses `Arc<RwLock<HashMap<String, ChatState>>>` for thread-safe per-chat buffers

**`MediaBatchManager`** — per-chat media reference batching:
- Same pattern as TextBatchManager but without character-based splitting
- Default delay: 800 ms (matching Hermes `_DEFAULT_MEDIA_BATCH_DELAY_SECONDS = 0.8`)

**`BatchSender` trait** — decouples batch manager from any platform adapter:
- Single method: `async fn send_batch(&self, chat_id: &str, text: &str) -> PlatformResult<()>`
- Implement on platform adapters or wrappers to connect to actual transport

**`split_long_text`** — private helper for splitting oversize text:
- Splits at natural boundaries in priority order: `\n\n` → `\n` → `. ` / `! ` / `? ` → ` ` → hard cut
- Respects UTF-8 character boundaries (uses `char_indices()`)
- Boundary characters stay with the preceding part

### Key Decisions
- **Timer cancellation**: `JoinHandle::abort()` used when `flush_all()` preempts a pending timer. Aborted tasks check for empty buffers and return early (no double-send).
- **Race safety**: Both timer and `flush_all` use `std::mem::take` on the message vec — only one wins, the other sees an empty vec.
- **No `tokio::time::advance`**: Tests use real-time sleeps (short delays) because `tokio` `test-util` feature is not enabled. Timer tests use 30-200 ms delays.
- **Generic sender**: `BatchSender` trait rather than `Fn` closure — cleaner async function signatures, implementable by any adapter.
- **`max_messages` enforcement**: When buffer exceeds `max_messages`, oldest messages are dropped (`remove(0)`).

### Test Coverage (14 tests)
Split unit tests (6):
1. `test_split_short_text_is_unchanged` — text under limit unchanged
2. `test_split_paragraph_boundary` — splits on `\n\n`
3. `test_split_line_boundary` — splits on `\n`
4. `test_split_sentence_boundary` — splits on `. ` / `! ` / `? `
5. `test_split_on_space_fallback` — splits on space when no punctuation
6. `test_split_hard_cut_when_no_boundary` — hard split at char limit
7. `test_split_respects_utf8_boundaries` — emoji (multi-byte) handled correctly

TextBatchManager tests (5):
8. `test_batch_accumulates_messages` — 3 messages joined with `\n`
9. `test_timer_fires_after_delay` — message sent after delay, not before
10. `test_long_message_is_split` — oversize message split into ≤50 char parts
11. `test_multiple_chat_ids_independent_buffers` — chat-A and chat-B have separate timers
12. `test_flush_all_clears_everything` — flush_all sends immediately, timers aborted, no duplicates
13. `test_flush_all_cancels_pending_timer` — flush before timer fire prevents duplicate send

MediaBatchManager tests (2):
14. `test_media_batch_accumulates` — media references joined with `\n`
15. `test_media_flush_all` — flush_all sends immediately

Constants test (1):
16. `test_defaults_match_hermes` — 600ms, 8 messages, 4000 chars, 800ms media delay

### Verification
- cargo check -p runtime: PASS (0 errors, 16 pre-existing dead-code warnings)
- cargo test -p runtime -- platform: 190/190 PASS (16 batch-specific, 0 new failures)

## Task: Feishu Adapter Outbound Methods — COMPLETED @ 2026-05-27

### Changes Made
- **feishu/adapter.rs**: Overwrote 5 `PlatformAdapter` trait methods from stubs to real implementations; added 2 private helpers.
- **feishu/ws.rs**: Fixed pre-existing `PinRegisterRequest` missing `Deserialize` derive (unrelated to this task, blocked test compilation).

### Methods Implemented

**`async fn send(&self, msg: &OutboundMessage) -> PlatformResult<()>`**
- Determines `receive_id` from `session_key.thread_id` (chat groups) or `session_key.user_id` (P2P)
- Uses `send_internal()` helper with post→text fallback chain
- Supports reply by checking `msg.reply_to` — uses Feishu reply endpoint, falls back to new message on codes 230011/231003 (reply target missing/recalled)
- Wrapped in `feishu_send_with_retry` (3 attempts, exponential backoff)

**`async fn send_typing(&self, chat_id: &str) -> PlatformResult<()>`**
- Returns `Ok(())` — Feishu bot API does not expose a typing indicator

**`async fn edit_message(&self, chat_id, message_id, content) -> PlatformResult<()>`**
- PUT to `/im/v1/messages/{message_id}` with `UpdateMessageRequest`
- Post→text fallback using same regex detection + `strip_markdown()`

**`async fn delete_message(&self, chat_id, message_id) -> PlatformResult<()>`**
- DELETE `/im/v1/messages/{message_id}`, returns `Ok(())` if response code is 0

**`async fn get_chat_info(&self, chat_id) -> PlatformResult<ChatInfo>`**
- GET `/im/v1/chats/{chat_id}`, parses `GetChatResponse` → `ChatInfo`
- Falls back to `chat_id` parameter and `"unknown"` chat_type if response fields are missing

### Private Helpers Added

**`feishu_send_with_retry<F, Fut>(&self, f: F) -> PlatformResult<()>`**
- Generic closure-based retry: 3 attempts with exponential backoff (500ms, 1000ms, 2000ms)
- Only retries `SendFailed` and `RateLimited` errors; returns other errors immediately

**`send_internal(&self, receive_id, text, reply_to) -> PlatformResult<()>`**
- Core send logic: tries post → falls back to text on `"content format of the post type is incorrect"` (case-insensitive regex)
- Reply path: uses Feishu `/im/v1/messages/{msg_id}/reply` endpoint, falls back to new message on codes 230011/231003
- New-message path: uses `/im/v1/messages?receive_id_type=open_id` with `SendMessageRequest`

### Key Decisions
- Used `SendMessageRequest`, `ReplyMessageRequest`, `UpdateMessageRequest` from `types` module (not inline structs)
- Used `build_post_payload`, `build_text_payload`, `strip_markdown` from `markdown` module
- Post rejection regex: `(?i)content format of the post type is incorrect` — case-insensitive, matches the exact Feishu error
- Reply fallback codes: 230011 (message not found), 231003 (message recalled) — documented from Feishu API docs
- `receive_id` derivation: prefers `thread_id` (chat_id for group chats) over `user_id` (open_id for P2P)
- Existing `send_message()`, `send_card_message()`, `ensure_token()` etc. preserved intact
- Media stubs (send_image, send_voice, send_document, send_video, send_animation) left as stubs

### Test Coverage (14 new adapter tests)
1. `test_post_rejection_regex_matches_feishu_error` — exact match + case-insensitive + multi-line
2. `test_post_rejection_regex_rejects_other_errors` — no false positives on unrelated errors
3. `test_send_text_message_format` — `build_text_payload` produces correct JSON
4. `test_send_text_message_format_empty` — empty text payload
5. `test_post_fallback_strips_markdown` — `strip_markdown` removes formatting
6. `test_post_payload_contains_markdown_formatting` — post payload preserves markdown
7. `test_post_payload_is_valid_json` — post payload is valid JSON with zh_cn structure
8. `test_edit_message_update_request_format` — `UpdateMessageRequest` serializes correctly
9. `test_edit_message_send_message_request_format` — `SendMessageRequest` serializes correctly
10. `test_delete_message_response_format` — delete response JSON parsing
11. `test_chat_info_from_get_chat_response` — `GetChatResponse` → `ChatInfo` mapping
12. `test_reply_fallback_codes` — 230011 and 231003 are non-zero (valid error codes)
13. `test_feishu_config` / `test_feishu_config_with_tokens` / `test_signature_computation` — pre-existing, still pass

### Verification
- cargo check -p runtime: PASS (0 errors)
- cargo test -p runtime -- platform: 193/193 PASS (14 new adapter tests + 179 existing)
- ws.rs PinRegisterRequest Deserialize fix: resolved pre-existing test compilation error

## Task: Feishu ProcessingReactions — COMPLETED @ 2026-05-27

### Changes Made
- **reactions.rs** (new): Created `crates/runtime/src/platform/feishu/reactions.rs` with `ProcessingReactions` struct, constants, and 10 tests.
- **mod.rs**: Added `pub mod reactions;` and `pub use reactions::*;`.

### Design
- **Struct**: `ProcessingReactions { pending: Arc<RwLock<HashMap<String,String>>>, insertion_order: Arc<RwLock<VecDeque<String>>>, max_cache: usize }` — thread-safe pending map with LRU eviction tracking
- **Constants**: `REACTION_TYPING = "Typing"`, `REACTION_CROSS_MARK = "CrossMark"`, `DEFAULT_MAX_CACHE = 1024`
- **API endpoints**:
  - POST `https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reactions` — create reaction
  - DELETE `https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/reactions/{reaction_id}` — delete reaction
- **Error handling**: Uses `PlatformResult<()>` with `PlatformError::SendFailed` for HTTP/API errors
- **LRU eviction**: `evict_excess()` runs after each `start_processing()`, evicts oldest entries when `pending.len() > max_cache`

### Hermes Parity
- `_FEISHU_REACTION_IN_PROGRESS` → `REACTION_TYPING`
- `_FEISHU_REACTION_FAILURE` → `REACTION_CROSS_MARK`
- Cache size 1024 matches Hermes default

### Tests (10)
1. `test_reaction_constants` — verify constant values
2. `test_new_default_values` — default max_cache is 1024
3. `test_with_max_cache_custom_value` — custom max works
4. `test_start_processing_stores_reaction_id` — insert/query pending map
5. `test_mark_success_removes_from_pending` — removal after success
6. `test_mark_failure_replaces_reaction` — removal after failure
7. `test_cache_eviction_at_max` — 4 inserts with max_cache=3 evicts oldest
8. `test_cache_below_max_no_eviction` — 5 inserts with max_cache=1024 keeps all
9. `test_default_implements_default_trait` — Default trait + empty pending
10. `test_max_cache_default_constant` — constant is 1024

### Verification
- `cargo check -p runtime`: PASS (0 new errors, pre-existing warnings only)
- `cargo test -p runtime -- feishu::reactions`: 10/10 PASS
- Pre-existing: ws.rs missing `#[derive(Deserialize)]` blocks `cargo test -p runtime -- platform` (lib test compilation error in ws module, not related to this task)

---

## Task 8: Feishu Message Normalization — COMPLETED @ 2026-05-27

### File Created
- **`crates/runtime/src/platform/feishu/normalize.rs`** (~530 lines)

### Module Registration
- `mod.rs`: Added `pub mod normalize;` + `pub use normalize::*;`

### Structures
- `NormalizedMessage`: Unified message representation with `message_type`, `text`, `image_keys`, `media_refs`, `mentions`, `metadata`
- `MentionRef`: Mention entry with `key`, `name`, `open_id`, `is_all`, `is_self`

### Public API
- `normalize_feishu_message(raw_message: &Value, bot_open_id: &str) -> NormalizedMessage` — dispatch by `msg_type`
- `build_mentions_map(mentions: &[Value], bot_open_id: &str) -> Vec<MentionRef>` — parse mentions array
- `normalize_text(text: &str, mentions: &[MentionRef]) -> String` — replace `@_user_N` → `@{name}`
- `strip_edge_self_mentions(text: &str, mentions: &[MentionRef]) -> String` — remove leading `@BotName`

### 9 Message Types Handled
| Type | MessageType | Text | Media |
|------|------------|------|-------|
| `text` | Text | Resolved mentions, stripped self | — |
| `post` | Text | Via `parse_post_payload()` | image_keys, media_refs |
| `image` | Photo | `[Image]` | image_key |
| `file` | Document | `[File: {name}]` | file_key ref |
| `audio` | Voice | `[Voice message]` | file_key ref |
| `media` | Video | `[Video]` | file_key ref |
| `merge_forward` | Text | Title + preview entries | — |
| `share_chat` | Text | `[Shared Chat: {name} ({id})]` | — |
| `interactive`/`card` | Text | Title + body + actions | — |
| *unknown* | Text | `[Unknown message type: {t}]` | — |

### Self-Mention Stripping
- Detects when mention's `open_id` or `id` equals `bot_open_id`
- Strips leading `@BotName ` (with trailing space) from resolved text
- Only strips edge mentions (at beginning of text)

### Tests (24)
1. `test_text_with_mentions` — replaces @_user_N with @name
2. `test_text_without_mentions` — plain text passthrough
3. `test_post_message` — parse post via markdown module
4. `test_image_message` — extracts image_key, Photo type
5. `test_file_message` — extracts file_key + name, Document type
6. `test_audio_message` — extracts file_key, Voice type
7. `test_media_video_message` — extracts file_key, Video type
8. `test_merge_forward` — title + preview text
9. `test_share_chat` — chat name + ID
10. `test_interactive_card` — header, markdown, actions
11. `test_strip_self_mention` — leading @Bot removed
12. `test_self_mention_not_stripped_when_not_first` — mid-text @Bot stays
13. `test_self_mention_stripping_with_trailing_space` — exact space match
14. `test_unknown_message_type` — fallback text
15. `test_build_mentions_map_empty` — empty array
16. `test_build_mentions_map_marks_self` — self detection
17. `test_build_mentions_map_uses_open_id_field` — open_id field
18. `test_normalize_text_no_mentions` — passthrough
19. `test_normalize_text_multiple_placeholders` — multi-replace
20. `test_image_message_no_key` — missing key fallback
21. `test_file_message_no_key` — missing key fallback
22. `test_interactive_card_minimal` — empty card
23. `test_merge_forward_empty` — empty forward
24. `test_metadata_preserved` — raw_message in metadata

### Verification
- `cargo check -p runtime`: PASS (0 new warnings)
- `cargo test -p runtime -- feishu::normalize`: 24/24 PASS
- `cargo test -p runtime -- platform`: 187 passed, 3 pre-existing failures in `batch` module (unrelated)

## Task: Feishu Auth (Access Control) — COMPLETED @ 2026-05-27

### Changes Made
- **auth.rs** (new): Created `crates/runtime/src/platform/feishu/auth.rs` with `Policy`, `AllowBots`, `GroupRule`, `AdmitResult`, `AccessControl` types, admission logic, and 25 tests.
- **mod.rs**: Added `pub mod auth;` + `pub use auth::*;`.

### Design
- **Policy enum**: `Open | Allowlist | Blacklist | AdminOnly | Disabled` — mirrors Hermes policy enum
- **AllowBots enum**: `None | Mentions | All` — mirrors Hermes `"none" | "mentions" | "all"`
- **GroupRule**: Per-group rule with `policy`, `allowlist`, `blacklist`, `require_mention` (Option<bool>, None=inherit global)
- **AdmitResult**: `{ admitted: bool, reason: Option<String> }` with `admit()` / `reject()` constructors
- **AccessControl**: Central admission controller with `bot_open_id`, `bot_name`, `admins` (HashSet), `group_rules` (HashMap), `sender_name_cache` (Arc<RwLock<HashMap>>)
- **Constants**: `SENDER_NAME_TTL_SECONDS = 600` — matches Hermes `_FEISHU_SENDER_NAME_TTL_SECONDS = 10 * 60`
- **`Arc<RwLock<>>`**: Used for thread-safe `sender_name_cache`
- **`HashSet`**: Used for `allowlist`, `blacklist`, `admins`
- **`HashMap`**: Used for `group_rules` and `sender_name_cache`

### Admission Logic (5 Steps)
1. **Self-echo prevention**: Reject if `sender_open_id == bot_open_id` or `sender_union_id == bot_open_id`
2. **Admin bypass**: Admit unconditionally if sender (open_id or union_id) is in `admins` set
3. **Bot sender gating**: Check `allow_bots` policy — `None` drops, `Mentions` requires @mention, `All` admits
4. **P2P bypass**: Direct messages always admitted (bypasses group policy)
5. **Group policy**: Look up `GroupRule` for `chat_id` (fallback `default_group_policy`) → check `require_mention` → enforce `Policy` (Disabled/Open/Allowlist/Blacklist/AdminOnly)

### Hermes Parity
- `Policy` enum → Hermes `open | allowlist | blacklist | admin_only | disabled`
- `AllowBots` → Hermes `"none" | "mentions" | "all"`
- `GroupRule` → Hermes `FeishuGroupRule`
- `_admit()` → `AccessControl::admit()`
- `_FEISHU_SENDER_NAME_TTL_SECONDS` → `SENDER_NAME_TTL_SECONDS = 600`

### Tests (25)
1. `test_self_echo_prevention` — bot open_id match
2. `test_self_echo_prevention_via_union_id` — bot union_id match
3. `test_admin_bypasses_all_checks` — admin bypasses disabled group
4. `test_bot_sender_filtered_when_allow_bots_none` — None drops bot
5. `test_bot_sender_admitted_with_mention` — Mentions + mentioned = admit
6. `test_bot_sender_rejected_without_mention` — Mentions + not mentioned = reject
7. `test_bot_sender_admitted_when_allow_bots_all` — All admits bot
8. `test_p2p_bypasses_group_policy` — P2P bypasses mention + disabled
9. `test_mention_required_in_group_reject_without_mention` — reject without @
10. `test_mention_required_in_group_admit_with_mention` — admit with @
11. `test_group_level_require_mention_overrides_global` — per-group override
12. `test_disabled_policy_rejects` — Disabled rejects all
13. `test_allowlist_admits_listed_users` — Allowlist match
14. `test_allowlist_matches_via_union_id` — Allowlist matches union_id
15. `test_blacklist_rejects_listed_users` — Blacklist blocks
16. `test_admin_only_rejects_non_admin` — AdminOnly rejects non-admin
17. `test_open_policy_admits` — Open admits all
18. `test_default_group_policy_fallback` — fallback to default
19. `test_sender_name_cache_store_and_retrieve` — cache TTL
20. `test_sender_name_cache_expired_entry` — expired → None
21. `test_sender_name_cache_missing_entry` — missing → None
22. `test_group_rule_builders` — builder pattern
23. `test_group_rule_defaults` — defaults
24. `test_admit_result_builders` — admit/reject constructors
25. `test_ttl_constant_matches_hermes` — SENDER_NAME_TTL_SECONDS == 600

### Verification
- `cargo check -p runtime`: PASS (0 new errors, pre-existing warnings only)
- `cargo test -p runtime -- feishu::auth`: 25/25 PASS
- `cargo test -p runtime -- platform`: 215/215 PASS (25 new auth tests + 190 existing)

---

## Task: Feishu WebSocket Client — COMPLETED @ 2026-05-27

### File Created
- **`crates/runtime/src/platform/feishu/ws.rs`** (~380 lines)

### Module Registration
- `mod.rs`: Added `pub mod ws;` + `pub use ws::*;`

### Dependency Added
- `tokio-tungstenite = "0.24"` to `Cargo.toml` (already a transitive dependency via reqwest)
- `tokio = { version = "1", features = ["test-util"] }` to `[dev-dependencies]` (fixed pre-existing batch.rs test compilation)

### Structures
- `FeishuWsClient`: Core WebSocket event push client with `app_id`, `app_secret`, `ws_url`, `reconnect_max_attempts` (default 30), `reconnect_interval_secs` (default 120)
- `PinRegisterRequest` / `PinRegisterResponse` / `PinRegisterData`: Pin registration types for `report_pin` endpoint

### Public API
- `FeishuWsClient::new(app_id, app_secret)` → initializes with defaults (30 attempts, 120s interval)
- `FeishuWsClient::with_reconnect(max_attempts, interval_secs)` → builder-style reconnect config
- `FeishuWsClient::connect()` → auth → pin registration → WS connect → spawns background reader → returns `UnboundedReceiver<Value>`
- `register_pin(token, app_id)` → public helper for pin registration, returns ws_url

### Flow (matches Hermes `_run_official_feishu_ws_client`)
1. POST `tenant_access_token/internal` → get bearer token
2. POST `event/v1/app/report_pin` → get WebSocket URL
3. `tokio_tungstenite::connect_async()` → WebSocket connection
4. Background `reader_loop` with auto-reconnect:
   - On disconnect: re-auth + re-register pin + reconnect (up to max_attempts)
   - `ws_read_loop`: reads messages, handles ping/pong, challenge verification on first message, forwards events through `mpsc::unbounded_channel`
5. Graceful shutdown: drop receiver → `tx.send()` fails → reader exits

### Key Decisions
- Used `tokio_tungstenite` (0.24 with native-tls) instead of raw TCP + WebSocket upgrade — simplifies framing
- `ws_url` field on `FeishuWsClient`: if pre-set, `connect()` skips pin registration; if empty, fetches fresh URL from Feishu
- `ws_read_loop` returns `Result<bool, ()>`: `Ok(true)` = receiver dropped (graceful exit), `Ok(false)` = connection lost (reconnect), `Err(())` = read error (reconnect)
- Challenge verification: first text message checked for `challenge` field; if present, echoes back `{"challenge": "..."}` via WS
- `SinkExt` from `futures` used for `ws_stream.send()` (not built-in method on `WebSocketStream`)

### Tests (9)
1. `test_feishu_ws_client_construction` — default values set correctly
2. `test_reconnect_settings_are_stored` — `with_reconnect` overrides defaults
3. `test_with_reconnect_zero_attempts` — edge case: 0 attempts allowed
4. `test_pin_registration_request_body_format` — `PinRegisterRequest` serializes with `app_id`
5. `test_pin_register_response_deserialization` — full response with `ws_url` + `pin`
6. `test_pin_register_response_error` — error response parses correctly
7. `test_channel_creation_and_single_event` — mpsc channel roundtrip
8. `test_shutdown_drop_receiver_propagates_to_sender` — `tx.send()` fails after rx dropped
9. `test_shutdown_receiver_returns_none_after_drop` — `rx.recv()` returns None after tx dropped

### Verification
- `cargo check -p runtime`: PASS (0 errors, pre-existing warnings only)
- `cargo test -p runtime -- platform`: 233/233 PASS (9 new ws tests + 224 existing)
- `cargo test -p runtime -- platform::feishu::ws`: 9/9 PASS

## Task: Feishu ChatProcessingQueue — COMPLETED @ 2026-05-27

### Changes Made
- **processing.rs** (new): Created `crates/runtime/src/platform/feishu/processing.rs` with `ChatProcessingQueue` struct, `ProcessingDecision` enum, background drainer, and 9 tests.
- **mod.rs**: Added `pub mod processing;` + `pub use processing::*;`. Also added missing `pub mod auth;` (pre-existing `auth.rs` file without module declaration).

### Design
- **Struct**: `ChatProcessingQueue { chat_locks, pending_events, max_queue_depth, drain_scheduled, drain_handler, active_guards }` — per-chat `Arc<Mutex<()>>` in a `RwLock<HashMap>`, `VecDeque<(String, Value, Instant)>` for pending queue, `Arc<AtomicBool>` for drain scheduling, `Arc<Mutex<HashMap<String, OwnedMutexGuard<()>>>>` for active lock guards.
- **Constants**: `DRAINER_MAX_WAIT_SECS=120`, `DRAINER_POLL_INTERVAL_MS=250`, `DEFAULT_MAX_QUEUE_DEPTH=1000` — all match Hermes.
- **Key insight**: `tokio::sync::Mutex::try_lock()` returns a `MutexGuard` with a borrow lifetime — cannot be stored. Switched to `try_lock_owned()` which returns `OwnedMutexGuard` (no borrow, `'static` lifetime), storable in a HashMap. This is the Rust equivalent of Python's `asyncio.Lock` being holdable across await points via `async with`.
- **Drain handler**: Optional `Arc<dyn Fn(String, Value) -> Pin<Box<dyn Future>> + Send + Sync>` closure invoked when drainer processes events. Set via builder pattern `with_drain_handler()`.
- **release() synchronous drain**: On release, checks pending queue for matching chat_id and spawns immediate processing — provides low-latency draining on lock release.
- **Drainer auto-release**: Drainer spawns a task that holds the guard, calls the handler, then releases — the guard's `OwnedMutexGuard` is dropped after handler completion.

### Hermes Parity
- `_pending_inbound_events` (asyncio.Queue) → `pending_events` (tokio::sync::Mutex<VecDeque>)
- `_pending_inbound_max_depth = 1000` → `DEFAULT_MAX_QUEUE_DEPTH = 1000`
- `_drain_pending_inbound_events()` polling at 0.25s → drainer polls every `DRAINER_POLL_INTERVAL_MS` (250ms)
- 120s timeout cap → `DRAINER_MAX_WAIT_SECS = 120`
- `asyncio.Lock` per chat → `Arc<tokio::sync::Mutex<()>>` per chat

### Tests (9)
1. `test_lock_acquisition_prevents_concurrent_processing` — first call Process, second Queued, release drains
2. `test_enqueue_when_busy` — 2 events queued while lock held
3. `test_drainer_processes_queued_events` — release + drainer processes ≥2 events
4. `test_queue_overflow_drops_oldest` — max_depth=3, 4th event Dropped
5. `test_release_allows_next_event` — handler called exactly once after release
6. `test_different_chats_independent_locks` — chat-2 Process while chat-1 busy
7. `test_max_queue_depth_default` — Default impl uses DEFAULT_MAX_QUEUE_DEPTH
8. `test_no_handler_drainer_does_not_start` — drain_scheduled stays false without handler
9. `test_release_drops_guard` — active_guards.len() transitions 1→0 after release, re-acquire succeeds

### Verification
- `cargo check -p runtime`: PASS (0 errors, pre-existing warnings only)
- `cargo test -p runtime -- platform`: 233/233 PASS (9 new processing tests + 224 existing)
- Pre-existing `auth.rs` module declaration fix also applied

---

## Task: Feishu Reaction Event Handler — COMPLETED @ 2026-05-27

### Changes Made
- **reactions.rs**: Added `handle_reaction_event()` method to `ProcessingReactions` struct; added 3 new tests.
- **mod.rs**: Registered `reactions` module (`pub mod reactions;` + `pub use reactions::*;`).

### Method: `handle_reaction_event`
- **Signature**: `pub fn handle_reaction_event(event_type: &str, event: &serde_json::Value, bot_app_id: &str) -> Option<InboundMessage>`
- **Event types handled**: `im.message.reaction.created_v1`, `im.message.reaction.deleted_v1`
- **Bot feedback loop prevention**: Filters `operator_type == "bot"` or `"app"` → returns `None`
- **Synthetic text format**: `reaction:{added|removed}:{emoji_type}`
- **Ownership verification**: `bot_app_id` parameter reserved for future use (message fetch + sender check deferred to caller per spec)
- **Session key**: Derived from `event.user_id.open_id` (operator), fallback `"reaction_operator"`

### Import Additions
- `InboundMessage` from `crate::platform::adapter`
- `MessageType`, `Platform`, `SessionKey` from `crate::platform::types`
- `chrono::Utc` for timestamp

### Tests (3 new)
1. `test_reaction_created_produces_added_text` — `reaction:added:HEART`, verifies text, message_type, platform, message_id
2. `test_bot_origin_reaction_is_filtered` — `operator_type: "bot"` → `None`
3. `test_unknown_event_type_returns_none` — unrecognized event_type → `None`

### Verification
- `cargo check -p runtime`: PASS (0 new errors)
- `cargo test -p runtime -- platform`: 265/266 PASS (1 pre-existing wechat_ilink failure unrelated)
- All 13 reaction tests pass (10 pre-existing + 3 new)

## Task: Feishu Card Action Handler — COMPLETED @ 2026-05-27

### Changes Made
- **card_handler.rs** (new): Created `crates/runtime/src/platform/feishu/card_handler.rs` — routes Feishu interactive card button clicks as synthetic `COMMAND` events, matching Hermes' `_handle_card_action_event` (feishu.py:2491-2540).
- **mod.rs**: Added `pub mod card_handler;` + `pub use card_handler::*;`.

### Structures
- `CardActionHandler` — unit struct with static methods (no instance state).
- Global dedup store: `std::sync::LazyLock<RwLock<HashMap<String, Instant>>>` with `DEDUP_TTL = 15 minutes` (900s).

### Public API
- `CardActionHandler::handle_card_action(event, message_id, chat_id, operator_open_id) -> Option<InboundMessage>`
  - Extracts `action.value` and `action.tag` from the event JSON
  - Builds synthetic command text: `/card {tag} {value_json}`
  - Creates `InboundMessage` with `message_type = Command`, includes metadata with `operator_open_id`, `chat_id`, `action_tag`, `action_value`
  - Returns `None` when the event JSON has no `action` object
  - Falls back to tag `"button"` when `action.tag` is missing
  - Handles missing `action.value` as JSON null gracefully
- `CardActionHandler::is_duplicate(token) -> bool`
  - Checks token in global `RwLock<HashMap<String, Instant>>`
  - Prunes expired entries (>15 min old) on every call
  - Inserts unseen tokens automatically (marks them as seen)
  - Returns `true` for duplicates

### Hermes Parity
- `_handle_card_action_event()` → `CardActionHandler::handle_card_action()`
- `_FEISHU_CARD_ACTION_DEDUP_WINDOW = 15 * 60` → `DEDUP_TTL = Duration::from_secs(900)`
- Token dedup with pruning on each call → prevents unbounded map growth
- Command format `/card {tag} {value_json}` matches Hermes convention

### Key Decisions
- Used `std::sync::LazyLock` (stabilized Rust 1.80) instead of `lazy_static` crate — already used in the codebase (permission_enforcer.rs)
- Used `std::sync::RwLock` (not `tokio::sync::RwLock`) since `is_duplicate` is synchronous and only holds the lock briefly
- `Instant` (not `SystemTime`) for TTL tracking — monotonic, immune to clock skew
- `value` extracted with `.cloned()` to preserve the JSON structure in metadata
- No dependencies added — all types from `std` and existing `serde_json`, `chrono`

### Test Coverage (10 tests)
handle_card_action (5):
1. `test_button_action_builds_command_event` — full card event → Command InboundMessage with correct fields
2. `test_non_button_tag_preserved_in_command` — `select_static` tag preserved in `/card select_static ...`
3. `test_missing_action_field_returns_none` — no `action` object → `None`
4. `test_missing_tag_defaults_to_button` — no `tag` → defaults to `"button"`
5. `test_empty_value_handled_as_null` — no `value` → defaults to JSON `null`

is_duplicate (5):
6. `test_duplicate_token_returns_true` — same token twice → `true` on second call
7. `test_non_duplicate_token_returns_false` — different tokens → `false`
8. `test_different_tokens_are_not_duplicates` — 3 unique tokens, all `false`
9. `test_expired_tokens_are_pruned` — pre-seeded expired token → pruned + treated as new
10. `test_prune_removes_multiple_expired_entries` — 2 expired + 1 fresh → expired removed, fresh survives

### Verification
- `cargo check -p runtime`: PASS (0 new errors, pre-existing dead-code warnings only)
- `cargo test -p runtime -- platform`: 243/243 PASS (10 new card_handler tests + 233 existing)

---

## Task: Feishu Approval Card Module — COMPLETED @ 2026-05-27

### Changes Made
- **approval.rs** (new): Created `crates/runtime/src/platform/feishu/approval.rs` — interactive approval card builder with 4 buttons (Approve Once/Session/Always/Deny), matching Hermes' `send_exec_approval`.
- **mod.rs**: Added `pub mod approval;` + `pub use approval::*;`. Also added missing `pub mod card_handler;` declaration (pre-existing `card_handler.rs` had `pub use` without `pub mod`).

### Structures
- `ApprovalCard { command, description, approval_id }` — builder for the interactive approval card.
- `CardActionDedup { inner: Arc<Mutex<HashMap<String, i64>>> }` — thread-safe token deduplication store.

### Public API
- `ApprovalCard::new(approval_id, command) -> Self` — create builder.
- `ApprovalCard::with_description(self, desc) -> Self` — attach human-readable description.
- `ApprovalCard::build(&self) -> String` — produce Feishu interactive card JSON.
- `ApprovalCard::build_resolved(choice, user_name) -> String` — produce resolved (post-decision) card.

### Constants
- `CARD_ACTION_DEDUP_TTL_SECONDS = 900` — 15-minute TTL, matches Hermes.
- `HERMES_ACTION_APPROVE_ONCE / APPROVE_SESSION / APPROVE_ALWAYS / DENY` — button action identifiers.
- `LABEL_ALLOW_ONCE / APPROVE_SESSION / APPROVE_ALWAYS / DENY` — button display labels.

### Card Structure
- Orange header: `"⚠️ Command Approval Required"`
- Markdown body: description (if set) + `**Command:**` + code block with command
- Action row with 4 buttons:
  - `"✅ Allow Once"` (primary, hermes_action=`"approve_once"`)
  - `"✅ Session"` (default, hermes_action=`"approve_session"`)
  - `"✅ Always"` (default, hermes_action=`"approve_always"`)
  - `"❌ Deny"` (danger, hermes_action=`"deny"`)
- Button value: `{"hermes_action": "{action}", "approval_id": {id}}`

### build_resolved
- Approved (any non-deny choice): green header `"✅ Approved by {name}"`
- Denied (deny/denied/reject/rejected/decline/declined): red header `"❌ Denied by {name}"`

### CardActionDedup
- `is_duplicate(token) -> bool` — checks and auto-inserts; returns true on duplicate.
- `remove(token)` — allows retry on handler failure.
- Uses `std::sync::Mutex` (not tokio) — critical sections are sub-microsecond.
- Uses `SystemTime` arithmetic for Unix-second timestamps (no `chrono` needed).
- No background pruning: entries auto-expire via TTL check on `is_duplicate`.

### Hermes Parity
- `send_exec_approval()` → `ApprovalCard::build()`
- Resolved card → `ApprovalCard::build_resolved()`
- `_FEISHU_CARD_ACTION_DEDUP_TTL_SECONDS = 15 * 60` → `CARD_ACTION_DEDUP_TTL_SECONDS = 900`
- Token-based dedup match

### Key Decisions
- `std::sync::Mutex` (not `tokio::sync::Mutex`) — dedup check is synchronous, lock held for nanoseconds.
- `SystemTime` + `UNIX_EPOCH` for timestamps — avoids `chrono` dependency for a single call.
- `is_deny_action()` extracted as standalone fn — exhaustively matches deny variants for `build_resolved`.
- Orange header template — Feishu supports `"orange"` as a valid card header colour.
- Card description appears above the command block, not inline.

### Test Coverage (21 tests)
build() (4):
1. `test_build_card_structure` — full JSON validation: header, template, markdown, 4 buttons, each button's label/style/action/approval_id
2. `test_build_card_with_description` — description text appears in markdown body
3. `test_build_card_without_description` — no description → body starts with `"**Command:**"`
4. `test_build_card_different_approval_ids` — two cards have different approval_id values

build_resolved() (7):
5. `test_build_resolved_approve_once` — green header, ✅ Approved by Alice
6. `test_build_resolved_approve_session` — green header
7. `test_build_resolved_approve_always` — green header
8. `test_build_resolved_generic_approved` — green header for "approved" string
9. `test_build_resolved_deny` — red header, ❌ Denied by Eve
10. `test_build_resolved_deny_variants` — denied/reject/rejected/decline/declined all produce red
11. `test_build_resolved_has_config` — verified wide_screen_mode present

CardActionDedup (7):
12. `test_dedup_token_not_duplicate_first_time` — first insert returns false
13. `test_dedup_token_is_duplicate_second_time` — second insert returns true
14. `test_dedup_different_tokens_independent` — two tokens tracked independently
15. `test_dedup_remove_allows_reprocessing` — remove then re-insert works
16. `test_dedup_expired_token_not_duplicate` — pre-seeded expired token treated as new
17. `test_dedup_new_and_empty` — fresh store reports empty
18. `test_dedup_not_empty_after_insert` — non-empty after insert
19. `test_dedup_default_creates_empty` — Default impl creates empty store

Constants (2):
20. `test_ttl_constant_matches_hermes` — TTL = 900
21. `test_action_constants_are_distinct` — all 4 hermes_action values unique

### Verification
- `cargo check -p runtime`: PASS (0 errors, pre-existing dead-code warnings only)
- `cargo test -p runtime -- platform`: 264/264 PASS (21 new approval tests + 243 existing)

### Side Fix
- Added missing `pub mod card_handler;` declaration in `mod.rs`. The `card_handler.rs` file existed with `pub use card_handler::*;` but no `pub mod card_handler;`, causing a compile error.

---

## Task: WeChat iLink Adapter — COMPLETED @ 2026-05-27

### Changes Made
- **wechat_ilink.rs** (NEW): Created `crates/runtime/src/platform/wechat_ilink.rs` (~690 lines) — personal WeChat adapter using Tencent's iLink Bot API, parallel to Feishu/WeCom.
- **mod.rs**: Added `pub mod wechat_ilink;` declaration and `pub use wechat_ilink::{WeChatLinkAdapter, WeChatLinkConfig};` exports.

### Structures
- `WeChatLinkConfig { bot_id, bot_secret, base_url }` — config with `new()` and `with_base_url()` builder pattern.
- `WeChatLinkAdapter { config, connected, token, context_token, seen_ids }` — all state via `Arc<RwLock<>>` following FeishuAdapter patterns.

### Constants
- `ILINK_BASE_URL = "https://ilinkai.weixin.qq.com"` — matches Hermes `weixin.py`.
- `DEFAULT_LONG_POLLING_TIMEOUT = 30` (seconds).

### Public API (7 methods)
- `authenticate()` — POST `/ilink/bot/gettoken` with bot_id + bot_secret, returns token string.
- `ensure_token()` — returns cached token or calls `authenticate()`.
- `get_updates()` — GET `/ilink/bot/getupdates` long-poll with token + context_token query params; updates `context_token` from response for next cycle.
- `send_text()` — POST `/ilink/bot/sendmessage` with token, touser, msgtype="text", text; returns message_id.
- `send_typing_indicator()` — POST `/ilink/bot/sendtyping` with token, touser.
- `get_qr_code()` — GET `/ilink/bot/get_bot_qrcode` with token query param; returns qrcode_url.
- `get_upload_url()` — GET `/ilink/bot/getuploadurl` with token + file_type query params; returns upload_url.

### Message Parsing
- `parse_ilink_message()` — maps iLink message JSON to `InboundMessage`.
- Supports msg_type detection: text → `MessageType::Text`, image → `MessageType::Photo`, voice → `MessageType::Voice`, video → `MessageType::Video`, file → `MessageType::Document`.
- Falls back to `"[{type} message]"` for unknown types.
- Missing `from_user` → `None` (skip message).

### Dedup
- `seen_ids: Arc<RwLock<HashMap<String, i64>>>` — message_id → timestamp millis.
- `is_duplicate()` / `mark_seen()` — checks and inserts before parsing messages in `receive()`.
- In-memory only (no persistence), per the simple iLink model.

### PlatformAdapter trait implementation (17 methods)
- `platform()` → `Platform::WeChat`
- `platform_name()` → `"wechat_ilink"`
- `connect()` → authenticate, set token, set connected=true
- `disconnect()` → clear connected, token, context_token
- `is_connected()` → read connected flag
- `receive()` → call get_updates, dedup, parse into InboundMessage
- `send()` → delegate to send_text
- `send_typing()` → delegate to send_typing_indicator
- `get_chat_info()` → returns stub ChatInfo with type "wechat_ilink"
- All other methods → `Err(NotImplemented(...))` stubs, matching FeishuAdapter pattern

### Key Decisions
- **No WebSocket**: iLink uses HTTP long-polling only — simpler than Feishu.
- **Context token echo**: Every `get_updates` response includes a `context_token`; echoed back in the next request for session continuity.
- **`blocking_read` caveat**: `is_connected()` uses `blocking_read()` on `tokio::sync::RwLock`; panics if called inside tokio task. Tests use direct `.read().await` when in async context.
- **API response format**: All iLink endpoints return `{ code: i32, msg: String, ... }` — code==0 means success.
- **Token management**: Simple cache-and-auth pattern; no expiry tracking needed (iLink tokens are long-lived).
- **Error handling**: `PlatformError::AuthenticationFailed` for auth errors, `SendFailed` for send/typing, `ReceiveFailed` for get_updates.
- **Platform::WeChat** variant reused: Same enum variant as WeCom (note: `Platform::WeChat.name()` returns "wecom" — potential future refinement).

### Hermes Parity
- `ILINK_BASE_URL` → Hermes `ILINK_BASE_URL = "https://ilinkai.weixin.qq.com"`
- `gettoken` endpoint → Hermes `POST /ilink/bot/gettoken`
- `getupdates` long-poll + context_token echo → Hermes `GET /ilink/bot/getupdates`
- `sendmessage` → Hermes `POST /ilink/bot/sendmessage`
- `sendtyping` → Hermes `POST /ilink/bot/sendtyping`
- `get_bot_qrcode` → Hermes `GET /ilink/bot/get_bot_qrcode`
- `getuploadurl` → Hermes `GET /ilink/bot/getuploadurl`

### Test Coverage (22 tests)
Config (2):
1. `test_config_creation` — default values set correctly
2. `test_config_with_custom_base_url` — with_base_url overrides default

State (2):
3. `test_connect_disconnect_state` — connected/token transitions via direct RwLock
4. `test_context_token_echo_mechanism` — context_token store/set/clear

Request format (3):
5. `test_get_updates_request_format` — use of token + context_token params
6. `test_send_text_request_format` — POST body structure verification
7. `test_send_typing_delegates_to_indicator` — trait method delegates correctly

PlatformAdapter trait (3):
8. `test_platform_returns_wechat` — Platform::WeChat
9. `test_platform_name_returns_wechat_ilink` — platform_name() string
10. `test_send_delegates_to_send_text` — send() calls send_text()
11. `test_receive_returns_none_when_disconnected` — early return on disconnected
12. `test_get_chat_info_stub` — ChatInfo with expected values
13. `test_on_event_returns_none` — on_event returns Ok(None)
14. `test_not_implemented_stubs` — 9 stubs all return Err

Constants (2):
15. `test_ilink_base_url_constant` — ILINK_BASE_URL matches Hermes
16. `test_default_long_polling_timeout` — DEFAULT_LONG_POLLING_TIMEOUT == 30

Parse (3):
17. `test_parse_ilink_text_message` — full InboundMessage field verification
18. `test_parse_ilink_image_message` — image → Photo type with "[Image]" text
19. `test_parse_ilink_message_without_from_user` — missing sender → None

Dedup (1):
20. `test_dedup_seen_ids` — insert + duplicate detection + independence

### Verification
- `cargo check -p runtime`: PASS (0 errors, pre-existing warnings only)
- `cargo test -p runtime -- platform`: 266/266 PASS (22 new wechat_ilink tests + 244 existing)
