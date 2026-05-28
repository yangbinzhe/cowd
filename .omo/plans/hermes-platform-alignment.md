# Hermes Platform Architecture Alignment Plan

## TL;DR

> **Quick Summary**: Upgrade cowd's platform adapters to Hermes-level parity for Feishu and WeChat (personal), plus a robust base `PlatformAdapter` trait. Excludes WeCom (Enterprise WeChat).
>
> **Deliverables**:
> - Enhanced `PlatformAdapter` trait (15+ methods, Hermes API parity)
> - Feishu adapter: WS push, message normalization (9 types), media pipeline, card approvals, batch splitting, dedup, group policies
> - WeChat (iLink) adapter: long-poll getupdates, AES CDN, full message lifecycle
> - DedupStore, Markdown↔Post conversion, reaction lifecycle
> - End-to-end test suite
>
> **Estimated Effort**: Large (2-3 weeks)
> **Parallel Execution**: YES — 4 waves
> **Critical Path**: Trait upgrade → Feishu WS → Normalization → Media pipeline → Card/Approval → Batch/Dedup → WeChat

---

## Context

### Original Request
Hermes (Nous Research) has mature Feishu/WeChat integration. Cowd needs to reach parity **without** WeCom.

### Interview Summary
Examined:
- Hermes `gateway/platforms/feishu.py` (~2700 lines) — Full Feishu bot adapter
- Hermes `gateway/platforms/weixin.py` (~2100 lines) — WeChat iLink Bot API adapter
- Hermes `gateway/platforms/base.py` (~1400 lines) — Base adapter ABC
- Hermes `gateway/config.py` — Platform enum + env var overrides
- Hermes `gateway/platforms/ADDING_A_PLATFORM.md` — 16-step integration checklist
- cowd `platform/adapter.rs` (170 lines) — Current trait (5 methods)
- cowd `platform/feishu/adapter.rs` (641 lines) — Current Feishu adapter
- cowd `platform/feishu/` — mod.rs, comment.rs, doc.rs, rules.rs

### Metis Review
Key gaps identified:
- **Trait gap**: Hermes ABC has 20+ methods; cowd trait has 5
- **SDK gap**: Hermes uses `lark_oapi`; cowd uses raw HTTP (adequate for Rust)
- **Media gap**: Hermes uploads/downloads images/audio/video/documents; cowd has nothing
- **Event gap**: Hermes handles 10+ Feishu event types; cowd handles 1
- **WeChat gap**: cowd has no personal WeChat account adapter at all
- **Lifecycle gap**: Hermes has per-chat locks, dedup, queuing, reactions; cowd has none

---

## Work Objectives

### Core Objective
Achieve Hermes-level platform integration parity for **Feishu** and **WeChat** within cowd's Rust architecture. WeCom explicitly excluded.

### Concrete Deliverables
1. Upgraded `PlatformAdapter` trait with 15+ methods + default impls
2. Feishu adapter with WebSocket push, message normalization (9 types), media pipeline, approval cards, markdown↔post, batch splitting, dedup, group policies
3. WeChat (iLink) adapter for personal WeChat accounts
4. DedupStore, Markdown converter, Reaction lifecycle utilities

### Definition of Done
- [ ] `cargo test -p cowd-runtime` passes all tests
- [ ] Feishu adapter can send/receive text, image, voice, document, card messages
- [ ] WeChat adapter connects to iLink API, polls getupdates, sends messages
- [ ] Message dedup survives process restart
- [ ] Approval card buttons trigger correct agent callbacks
- [ ] Group policies filter unauthorized users

### Must Have
- Enhanced `PlatformAdapter` trait: `send_typing`, `send_image`, `send_voice`, `send_document`, `send_video`, `edit_message`, `delete_message`, `get_chat_info`, `send_card`, `on_event`
- Feishu WebSocket mode (not just webhook)
- Feishu message normalization: text, post, image, file, audio, media, merge_forward, share_chat, interactive
- Feishu image upload + send with optional caption
- Feishu text batch splitting (configurable delay/max-messages/max-chars)
- Feishu message dedup with persistent LRU state
- Feishu per-chat serial processing
- Feishu processing lifecycle: setup + cleanup reactions
- Feishu card message send + button callback routing
- Feishu @mention gating for group chats
- WeChat iLink: long-poll `getupdates`, `sendmessage`, context_token echo, media CDN, QR login

### Must NOT Have (Guardrails)
- ❌ No WeCom (Enterprise WeChat) — explicitly excluded
- ❌ No real-time voice/video call handling
- ❌ No Feishu approval instance management — button routing only
- ❌ No multi-tenant ISV Feishu app — single bot only
- ❌ No native Rust `lark_oapi` — `reqwest` + typed wrappers is sufficient
- ❌ No platform plugin system — static enum is fine for now

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed.

### Test Decision
- **Infrastructure exists**: YES (cargo test)
- **Automated tests**: TDD
- **Framework**: `cargo test` (Rust native)

### QA Policy
Every task MUST include agent-executed QA scenarios.
- **API**: `curl` to mock Feishu server, verify response
- **Integration**: Mock HTTP server for round-trip tests
- **Unit**: `#[tokio::test]` for individual functions

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — start immediately):
├── Task 1: Upgrade PlatformAdapter trait (5→15+ methods)
├── Task 2: FeishuTypes — all request/response structs for Feishu API
├── Task 3: FeishuMediaClient — image/audio/file upload pipeline
├── Task 4: FeishuMarkdown — markdown↔Feishu post/card conversion
├── Task 5: DedupStore — persistent LRU + TTL dedup

Wave 2 (Feishu core — MAX PARALLEL):
├── Task 6: Feishu WS connection mode
├── Task 7: Feishu message normalization (9 types)
├── Task 8: Feishu outbound: send_text/image/voice/document/video
├── Task 9: Feishu reaction lifecycle (Typing/CrossMark)
├── Task 10: Feishu text batch splitting + media batch
├── Task 11: Feishu per-chat serial processing + pending queue
├── Task 12: Feishu group policy + @mention gating + allowlist
├── Task 13: Feishu edit_message + delete_message

Wave 3 (Advanced Feishu + WeChat):
├── Task 14: Feishu approval card (send + button callback routing)
├── Task 15: Feishu card action → COMMAND synthetic events
├── Task 16: Feishu reaction → TEXT synthetic events
├── Task 17: WeChat iLink adapter (personal WeChat)

Wave FINAL (Integration + verification):
├── Task F1: Feishu full integration test (send+receive round-trip)
├── Task F2: WeChat integration test (iLink mock)
├── Task F3: Documentation + config spec + env var reference
├── Task F4: Code quality + scope fidelity audit
```

Critical Path: Task 1 → Task 2 → Task 6 → Task 8 → Task 14 → F1

---

## TODOs

> **Per-task QA approach**: Tasks 2-17 below list `Acceptance Criteria` with specific, verifiable conditions. The implementing agent runs `cargo test`, mocks HTTP responses, or reads files to confirm each criterion. Evidence is saved to `.omo/evidence/task-{N}-{slug}.{ext}`. The Final Verification Wave (F1-F4) provides end-to-end integration coverage for all tasks together.

- [x] 1. **Upgrade `PlatformAdapter` trait** — `crates/runtime/src/platform/adapter.rs`

  **What to do**:
  - Add methods: `send_typing()`, `send_image()`, `send_voice()`, `send_document()`, `send_video()`, `send_animation()`, `edit_message()`, `delete_message()`, `get_chat_info()`, `send_card()`, `on_event()`
  - Add `MessageType` enum: `Text, Photo, Video, Audio, Voice, Document, Sticker, Command, Location`
  - Add `MessageEvent` struct with full fields matching Hermes `MessageEvent`
  - Add default implementations for optional methods (return not-implemented)
  - Keep `NullAdapter` working

  **Must NOT do**:
  - Don't break existing `FeishuAdapter` / `WeComAdapter` / `NullAdapter`
  - Don't change return types of existing methods

  **Recommended Agent Profile**: `deep` — architecture-level trait design

  **Parallelization**: NO (blocks everything)
  **Blocked By**: None

  **References**:
  - Hermes `base.py:848-946` (`MessageType`, `MessageEvent`, `SendResult` dataclasses)
  - Hermes `base.py:1206-1425` (`BasePlatformAdapter` ABC — all methods)

  **Acceptance Criteria**:
  - [ ] Trait compiles with `FeishuAdapter`, `WeComAdapter`, `NullAdapter`
  - [ ] `cargo test -p cowd-runtime` passes
  - [ ] `MessageType` enum has at least: Text, Photo, Video, Audio, Voice, Document, Sticker, Command

  **QA Scenarios**:
  ```
  Scenario: Trait compiles with all current adapters
    Tool: Bash
    Steps: cargo check -p cowd-runtime 2>&1
    Expected: No compilation errors
    Evidence: .omo/evidence/task-01-compile.txt

  Scenario: MessageType enum is complete
    Tool: Bash
    Steps: grep "MessageType" crates/runtime/src/platform/types.rs
    Expected: Contains Photo, Video, Audio, Voice, Document, Sticker, Command
    Evidence: .omo/evidence/task-01-message-types.txt
  ```

  **Commit**: YES
  - Message: `feat(platform): upgrade PlatformAdapter trait to Hermes parity`

---

- [x] 2. **FeishuTypes module** — `crates/runtime/src/platform/feishu/types.rs`

  **What to do**:
  - Request/response structs for:
    - `auth/v3/tenant_access_token` — token auth
    - `im/v1/messages` — send, reply, update, get, list
    - `im/v1/images` — create/upload image
    - `im/v1/files` — create/upload file
    - `im/v1/chats` — get, list
    - `im/v1/reactions` — create, delete
  - Webhook event types wrapper
  - Card message body types (interactive card JSON schema)
  - All structs derive Serialize + Deserialize

  **References**:
  - Hermes `feishu.py:86-127` (lark_oapi imports showing all Feishu API endpoints)
  - Hermes `feishu.py:1400-1507` (FeishuAdapterSettings — all config fields)

  **Acceptance Criteria**:
  - [ ] All structs (De)Serialize correctly via `serde_json::from_str` / `to_string`
  - [ ] `cargo test -p cowd-runtime` passes round-trip tests
  - [ ] API paths match Feishu Open API spec (verify in a `#[test]` that each endpoint URL is well-formed)

  **How to verify**:
  ```
  cargo test -p cowd-runtime --test feishu_types 2>&1
  grep -r "open.feishu.cn" crates/runtime/src/platform/feishu/types.rs
  ```
  Evidence: `.omo/evidence/task-02-serde-roundtrip.txt`

  **Commit**: YES (groups with 3)
  - Message: `feat(platform): add Feishu API types module`

---

- [x] 3. **FeishuMediaClient** — `crates/runtime/src/platform/feishu/media.rs`

  **What to do**:
  - Image upload: POST `im/v1/images` with multipart form data → return `image_key`
  - File upload: POST `im/v1/files` → return `file_key`
  - Download: GET `im/v1/messages/{id}/resources/{key}` → save to `~/.cowd/cache/`
  - Cache directories: `images/`, `audio/`, `videos/`, `documents/`
  - SSRF protection: reject private/internal IPs
  - Retry on transient failure (3 attempts with backoff)
  - Image magic-byte validation: reject non-image data (PNG magic `\x89PNG`, JPEG `\xff\xd8\xff`, etc.)

  **References**:
  - Hermes `feishu.py:1944-1992` (`send_image_file` — upload→send flow)
  - Hermes `feishu.py:2026-2058` (`send_animation` — degrade to document)
  - Hermes `base.py:509-550`, `650-665`, `750-824` (image/audio/video/document cache)

  **Acceptance Criteria**:
  - [ ] Upload returns valid `image_key` / `file_key`
  - [ ] Download writes to correct cache directory (`~/.cowd/cache/images/`)
  - [ ] Non-image data raises error
  - [ ] Retries on HTTP 429 / 5xx

  **How to verify**:
  ```
  cargo test -p cowd-runtime --test feishu_media 2>&1
  ```
  Evidence: `.omo/evidence/task-03-upload-download.txt`

  **Commit**: YES (groups with 2)

---

- [x] 4. **FeishuMarkdown** — `crates/runtime/src/platform/feishu/markdown.rs`

  **What to do**:
  - `build_post_payload(content) -> String` — Markdown → Feishu post JSON
    - Code blocks isolated in separate rows (``` fences)
    - Bold/**italic**/~~strikethrough~~/`code` / inline code all converted
    - @-mentions preserved
  - `parse_post_payload(payload) -> ParsedPost { text, image_keys, media_refs }` — reverse direction
  - `build_card_payload(title, content, actions) -> String` — interactive card JSON
  - `strip_markdown_to_plain_text(content) -> String` — fallback when post API rejects
  - `build_text_payload(content) -> String` — simple text message
  - Feishu-specific: `\` markdown escaping

  **References**:
  - Hermes `feishu.py:546-605` (`_build_markdown_post_payload`, `_build_markdown_post_rows` — code block isolation)
  - Hermes `feishu.py:607-793` (post parsing: `parse_feishu_post_payload`, `_render_post_element`, etc.)
  - Hermes `feishu.py:451-507` (markdown rendering helpers)
  - Hermes `feishu.py:509-524` (`_strip_markdown_to_plain_text`)

  **Acceptance Criteria**:
  - [ ] Code blocks in separate post rows (same behavior as Hermes)
  - [ ] Bold/italic/strikethrough/code all survive round-trip
  - [ ] @-mentions pass through correctly
  - [ ] Image references within post extracted as image_keys

  **How to verify**:
  ```
  cargo test -p cowd-runtime --test feishu_markdown 2>&1
  ```
  Evidence: `.omo/evidence/task-04-markdown-parse.txt`

  **Commit**: YES (groups with 2)

---

- [x] 5. **DedupStore** — `crates/runtime/src/platform/dedup.rs`

  **What to do**:
  - LRU-capped `message_id → seen_at (Unix timestamp)` map
  - Persist to `~/.cowd/dedup/feishu_seen_ids.json` on shutdown + periodic save
  - Load on startup
  - TTL: 24 hours (configurable)
  - Default max cache size: 2048 entries (configurable)
  - Thread-safe via `tokio::sync::RwLock`
  - `is_duplicate(message_id) -> bool` and `mark_seen(message_id)` methods

  **References**:
  - Hermes `feishu.py:1373-1375` (`_seen_message_ids`, `_seen_message_order`, `_dedup_state_path`)
  - Hermes `feishu.py:203` (`_FEISHU_DEDUP_TTL_SECONDS = 24*60*60`)
  - Hermes `feishu.py:195` (`_DEFAULT_DEDUP_CACHE_SIZE = 2048`)

  **Acceptance Criteria**:
  - [ ] Duplicate ID returns true within TTL
  - [ ] Expired IDs auto-evicted
  - [ ] LRU eviction at capacity
  - [ ] State survives restart (file load/save)

  **How to verify**:
  ```
  cargo test -p cowd-runtime --test dedup_store 2>&1
  ```
  Evidence: `.omo/evidence/task-05-dedup.txt`

  **Commit**: YES
  - Message: `feat(platform): add persistent message dedup store`

---

- [x] 6. **Feishu WebSocket connection** — `crates/runtime/src/platform/feishu/ws.rs`

  **What to do**:
  - WebSocket client to Feishu event push endpoint (using `tokio-tungstenite`)
  - Pin registration on connect
  - Challenge verification on WS open
  - Auto-reconnect: max 30 attempts, 120s interval (configurable)
  - Optional ping_interval / ping_timeout
  - Bridge WS events to tokio main runtime (channel-based)
  - Event type dispatch: route to proper handler based on `header.event_type`
  - Graceful shutdown: cancel all tasks, close WS cleanly

  **References**:
  - Hermes `feishu.py:1279-1339` (`_run_official_feishu_ws_client` — WS thread bridge, reconnect overrides)
  - Hermes `feishu.py:1540-1566` (`_build_event_handler` — event type registration)
  - Hermes `feishu.py:384-387` (ws_reconnect settings)

  **Acceptance Criteria**:
  - [ ] Connects to Feishu event push URL
  - [ ] Reconnects on disconnect with backoff
  - [ ] Events dispatched by type to correct handler
  - [ ] Clean shutdown

  **How to verify**:
  ```
  cargo test -p cowd-runtime --test feishu_ws 2>&1
  ```
  Evidence: `.omo/evidence/task-06-ws-connect.txt`

  **Commit**: YES
  - Message: `feat(platform): add Feishu WebSocket event push`

---

- [x] 7. **Feishu message normalization** — `crates/runtime/src/platform/feishu/normalize.rs`

  **What to do**:
  - Parse ALL inbound Feishu message types:
    - `text` → extract text, resolve @-mentions → `MessageType::Text`
    - `post` → parse rich text, extract text+image_keys+media_refs
    - `image` → extract image_key → download to cache → `MessageType::Photo`
    - `file` → extract file_key → download → `MessageType::Document`
    - `audio` → extract file_key → download → `MessageType::Voice`
    - `media` → extract file_key → download → `MessageType::Video`
    - `merge_forward` → extract title + entry previews
    - `share_chat` → extract chat name + ID
    - `interactive`/`card` → extract title, body, actions
  - Build mention map from `mentions[]` payload
  - Strip edge self-mentions (leading/trailing with punctuation boundary)
  - Return `NormalizedMessage { message_type, text, image_keys, media_refs, mentions, metadata }`

  **References**:
  - Hermes `feishu.py:800-870` (`normalize_feishu_message` — type dispatcher)
  - Hermes `feishu.py:881-956` (merge_forward, share_chat, interactive parsers)
  - Hermes `feishu.py:1193-1231` (`_build_mentions_map`)
  - Hermes `feishu.py:1234-1276` (`_strip_edge_self_mentions`)

  **Acceptance Criteria**:
  - [ ] All 9 message types produce correct output
  - [ ] Image/file/audio messages download media to cache
  - [ ] @-mentions resolved from mention map
  - [ ] Self-mentions stripped from edges

  **How to verify**:
  ```
  cargo test -p cowd-runtime --test feishu_normalize 2>&1
  ```
  Evidence: `.omo/evidence/task-07-normalization.txt`

  **Commit**: YES
  - Message: `feat(platform): add Feishu message normalization`

---

- [x] 8. **Feishu outbound pipeline** — `crates/runtime/src/platform/feishu/adapter.rs`

  **What to do**:

- [x] 9. **Feishu reaction lifecycle** — `crates/runtime/src/platform/feishu/reactions.rs`

  **What to do**:

- [x] 10. **Feishu text batch splitting** — `crates/runtime/src/platform/feishu/batch.rs`

  **What to do**:

- [x] 11. **Feishu per-chat serial processing** — `crates/runtime/src/platform/feishu/processing.rs`

  **What to do**:

- [x] 12. **Feishu group policy + access control** — `crates/runtime/src/platform/feishu/auth.rs`

  **What to do**:

- [x] 13. **Feishu edit_message + delete_message** — in `adapter.rs`

  **What to do**:
  - `edit_message(message_id, content)` → PUT `im/v1/messages/{message_id}`
  - Post→text fallback on API rejection (same as send)
  - `delete_message(message_id)` → DELETE `im/v1/messages/{message_id}`

  **References**:
  - Hermes `feishu.py:1751-1784` (`edit_message` with fallback)
  - Hermes `feishu.py:227` (`_FEISHU_REPLY_FALLBACK_CODES`)

  **Commit**: YES (groups with 8)

---

- [x] 14. **Feishu approval card** — `crates/runtime/src/platform/feishu/approval.rs`

- [x] 15. **Feishu card action → COMMAND events** — `crates/runtime/src/platform/feishu/card_handler.rs`

- [x] 16. **Feishu reaction → TEXT events** — in `reactions.rs`

  **What to do**:
  - On `im.message.reaction.created/deleted_v1`:
  - Verify message sender is this bot (check owner app_id)
  - Fetch target message via GET `im/v1/messages/{id}` to confirm ownership
  - Build TEXT event: `reaction:{added|removed}:{emoji_type}`
  - Route to message handler
  - Filter bot-self reactions (break feedback loop)

  **References**:
  - Hermes `feishu.py:2431-2489` (`_handle_reaction_event` — fetch message, verify sender, build synthetic event)

  **Acceptance Criteria**:
  - [ ] Reaction on bot message → TEXT event
  - [ ] Reaction on other bot's message ignored
  - [ ] Bot-self reactions filtered

  **How to verify**:
  ```bash
  cargo test -p cowd-runtime --test feishu_reaction_events 2>&1
  ```
  Evidence: `.omo/evidence/task-16-reaction-events.txt`

  **Commit**: YES (groups with 14)

---

- [x] 17. **WeChat iLink adapter** — `crates/runtime/src/platform/wechat_ilink.rs`

  **What to do**:
  - Build adapter for Tencent iLink Bot API (personal WeChat accounts)
  - HTTP API endpoints:
    - `GET ilink/bot/getupdates` — long-poll inbound messages
    - `POST ilink/bot/sendmessage` — outbound text
    - `POST ilink/bot/sendtyping` — typing indicator
    - `POST ilink/bot/getuploadurl` — media upload URL
  - AES-128-ECB CDN protocol for media files
  - QR login flow: `GET ilink/bot/get_bot_qrcode` → display QR → wait for scan
  - Context token echo: every outbound reply must echo latest `context_token`
  - Message dedup via message IDs
  - Implement `PlatformAdapter` trait methods

  **References**:
  - Hermes `weixin.py:1-80` (iLink API endpoints, imports, constants)
  - Hermes `weixin.py:69-78` (`ILINK_BASE_URL`, endpoints, constants)
  - Hermes `weixin.py` (full 2100-line adapter as pattern reference)

  **Acceptance Criteria**:
  - [ ] Connects to iLink API (mock)
  - [ ] Long-poll `getupdates` receives messages
  - [ ] `sendmessage` delivers to WeChat user
  - [ ] Context token correctly echoed
  - [ ] Media files route through encrypted CDN
  - [ ] QR login flow works

  **How to verify**:
  ```bash
  cargo test -p cowd-runtime --test wechat_ilink 2>&1
  ```
  Evidence: `.omo/evidence/task-17-ilink.txt`

  **Commit**: YES
  - Message: `feat(platform): add WeChat iLink Bot API adapter`

---

## Final Verification Wave

- [x] F1. **Feishu integration test** — VERDICT: APPROVE [21/21]
  - Output: `Scenarios [N/N pass] | VERDICT`

- [x] F2. **WeChat integration test** — VERDICT: APPROVE
- [x] F3. **Documentation + config** — Noted, docs in plan file + learnings.md
- [x] F4. **Code quality + scope audit** — VERDICT: APPROVE [266/266 tests, 0 errors]

---

## Commit Strategy

| Tasks | Commit Message |
|-------|---------------|
| 1, 2-4 | `feat(platform): upgrade PlatformAdapter trait + Feishu types/markdown/media` |
| 5 | `feat(platform): add persistent message dedup store` |
| 6 | `feat(platform): add Feishu WebSocket event push` |
| 7 | `feat(platform): add Feishu message normalization` |
| 8, 13 | `feat(platform): add Feishu media outbound + edit/delete` |
| 9 | `feat(platform): add Feishu reaction lifecycle` |
| 10 | `feat(platform): add Feishu text batch splitting` |
| 11 | `feat(platform): add Feishu per-chat serial processing` |
| 12 | `feat(platform): add Feishu group policy and access control` |
| 14, 16 | `feat(platform): add Feishu approval cards + reaction events` |
| 15 | `feat(platform): add Feishu card action command events` |
| 17 | `feat(platform): add WeChat iLink Bot API adapter` |

---

## Success Criteria

### Verification Commands
```bash
cargo test -p cowd-runtime 2>&1 | tail -20
cargo check --workspace 2>&1 | tail -5
```

### Final Checklist
- [ ] `PlatformAdapter` trait: 15+ methods with default impls
- [ ] Feishu adapter: WS connect, 9-type normalization, media send, card approvals, batch, dedup, access control
- [ ] WeChat adapter: iLink getupdates, sendmessage, context token, QR login
- [ ] All tests pass
- [ ] No WeCom code introduced
- [ ] All Must NOT Have guardrails enforced
