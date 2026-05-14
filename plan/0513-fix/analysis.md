# Cowd Bug Fix Plan — 2026-05-15

## Root Cause Analysis

### Issue 1: TUI Display Corruption After Responses

**Symptom:** TUI display becomes garbled/messy after response content starts appearing.

**Root Causes (3 layers):**

1. **No scroll state management** (`crates/rusty-claude-cli/src/tui/render.rs:141`)
   - All messages rendered as a single `Paragraph` widget with `Wrap { trim: false }`
   - When content exceeds viewport, ratatui clips but doesn't track scroll position
   - New messages push content up but the viewport doesn't follow — content appears "cut off" or overlapping

2. **Streaming render conflict** (`crates/rusty-claude-cli/src/tui/render.rs:111-142`)
   - `draw_messages()` rebuilds the entire message list from `app.messages` on every frame
   - During streaming, messages are updated incrementally but the full re-render causes flicker
   - The `tui_textarea` input widget (bottom) and `Paragraph` (middle) share the same terminal buffer — when the Paragraph grows beyond its allocated Rect, it bleeds into the input area

3. **Missing terminal reset on resize** (`crates/rusty-claude-cli/src/tui/mod.rs`)
   - No terminal clear/redraw on window resize events
   - ANSI escape codes from syntax-highlighted code blocks may leak into subsequent renders

**Fix Strategy:**
- Add scroll state tracking with `ratatui::widgets::Scrollbar` and `Scrollable` paragraph
- Implement "auto-scroll to bottom" behavior when new messages arrive
- Add terminal clear before each frame render to prevent bleed
- Clamp message display to allocated Rect with proper overflow handling

---

### Issue 2: Frontend UI Incomplete / Broken

**Symptom:** Can't login, most features unusable, poor interaction.

**Root Causes (4 critical bugs):**

1. **Auth payload mismatch** — **BLOCKING BUG**
   - Frontend (`webui/api.js:37`): `Api.login(pw)` sends `{password: pw}`
   - Backend (`server.rs:3720`): `LoginRequest` expects `{token: String}`
   - **Result:** Login always fails because the field name doesn't match — `password` is sent but `token` is expected

2. **No auth token propagation** — **BLOCKING BUG**
   - After login, the returned token is not stored or sent in subsequent API requests
   - `api.js` `req()` function never adds `Authorization: Bearer <token>` header
   - All protected API calls fail with 401 after auth is enabled

3. **Duplicate function definitions in messages.js** — **SILENT BUG**
   - `handleLine()` defined twice: lines 84-98 and 162-170
   - `dispatch()` defined twice: lines 100-121 and 172-188
   - JavaScript silently uses the second definition, which is a simplified version missing event type handling (`streamBuffer` tracking, `_event` field)
   - **Result:** SSE event routing is broken — tool events, thinking blocks, and approvals don't dispatch correctly

4. **Missing login UI flow**
   - No login modal or page exists in `index.html`
   - The `boot.js` doesn't check auth status on load
   - No visual feedback when auth fails

**Fix Strategy:**
- Fix auth payload: change frontend to send `{token: pw}` OR change backend to accept `{password}` (recommend: align frontend to backend since `token` is the correct concept)
- Add auth token storage and propagation in `api.js`
- Remove duplicate `handleLine`/`dispatch` functions from `messages.js`
- Add a login modal to `index.html` and auth check in `boot.js`

---

### Issue 3: API Error + Package Error

#### 3a: `reasoning_content` Error

**Error:** `The 'reasoning_content' in the thinking mode must be passed back to the API.`

**Root Cause:** Message conversion strips Thinking blocks in HTTP server path.

The DeepSeek thinking mode flow:
1. API response includes `reasoning_content` → converted to `OutputContentBlock::Thinking` (openai_compat.rs:515-529, 1167-1174) ✓
2. Stored as `ContentBlock::Thinking` in conversation history (runtime/conversation.rs:1684-1691) ✓
3. **BUG:** When building the next API request, `server.rs:1106-1112` (`OpenAiApiClient::build_message_request`) only handles `SessionContentBlock::Text` and skips everything else:
   ```rust
   SessionContentBlock::Text { text } => {
       Some(InputContentBlock::Text { text: text.clone() })
   }
   _ => None, // ← Thinking blocks STRIPPED here!
   ```
4. The `tools/src/executor.rs:3339-3344` correctly converts `ContentBlock::Thinking` to `InputContentBlock::Thinking` — but this path is only used by the CLI, not the HTTP server.

**Fix:** Add `SessionContentBlock::Thinking` handling in `server.rs` `build_message_request()`, matching the CLI path in `executor.rs`.

#### 3b: `cowd-memory` Package Not Found

**Error:** `package ID specification 'cowd-memory' did not match any packages`

**Root Cause:** Package name mismatch in workspace configuration.
- `crates/memory/Cargo.toml`: `name = "memory"`
- `Cargo.toml` workspace: `cc-memory = { path = "crates/memory" }` (workspace dependency alias)
- Something in the codebase or scripts is looking for `cowd-memory` but the package is named `memory`

**Fix:** Identify where `cowd-memory` is referenced and either:
- Rename the package to `cowd-memory` in `crates/memory/Cargo.toml`, OR
- Fix the reference to use the correct name `memory`

---

## Fix Plan

### Phase 1: Critical Path Fixes (blocking issues)

| # | File | Change | Issue |
|---|------|--------|-------|
| 1 | `webui/api.js` | Fix login payload: `{password}` → `{token}`; add auth token storage + Bearer header propagation | Issue 2 (#1, #2) |
| 2 | `webui/messages.js` | Remove duplicate `handleLine()` and `dispatch()` functions (lines 162-188) | Issue 2 (#3) |
| 3 | `crates/rusty-claude-cli/src/server.rs` | Add `SessionContentBlock::Thinking` handling in `build_message_request()` | Issue 3a |
| 4 | Identify + fix `cowd-memory` reference | Fix package name mismatch | Issue 3b |

### Phase 2: TUI Display Fixes

| # | File | Change | Issue |
|---|------|--------|-------|
| 5 | `crates/rusty-claude-cli/src/tui/render.rs` | Add scroll state to `draw_messages()`, use `Paragraph::scroll()` | Issue 1 (#1) |
| 6 | `crates/rusty-claude-cli/src/tui/render.rs` | Add `frame.render_widget(Clear, area)` before message paragraph | Issue 1 (#2) |
| 7 | `crates/rusty-claude-cli/src/tui/app.rs` | Add `scroll_offset: u16` field to `App` struct | Issue 1 (#1) |

### Phase 3: Frontend UX Improvements

| # | File | Change | Issue |
|---|------|--------|-------|
| 8 | `webui/index.html` | Add login modal overlay | Issue 2 (#4) |
| 9 | `webui/boot.js` | Add auth check on load, show login modal if needed | Issue 2 (#4) |
| 10 | `webui/api.js` | Add auth error handling (401 → show login) | Issue 2 (#2) |

---

## Verification Plan

### Issue 1 (TUI)
- [ ] Start `cowd` in REPL mode, send a long message that exceeds terminal height
- [ ] Verify messages scroll properly and input area stays visible
- [ ] Resize terminal window — verify no display corruption
- [ ] Send multiple messages with code blocks — verify ANSI codes don't leak

### Issue 2 (Frontend)
- [ ] Open webui, verify login modal appears (if auth enabled)
- [ ] Login with correct token — verify subsequent API calls succeed
- [ ] Login with wrong token — verify error message shown
- [ ] Send a message — verify SSE streaming works (text, tool cards, thinking blocks all render)
- [ ] Test all right panels: Memory, Skills, Crons, Settings, Gateway, Agents, Tools
- [ ] Test control center tabs: Config, Providers, Approval, History, Usage

### Issue 3 (API + Package)
- [ ] Use a DeepSeek thinking model — send 2+ messages in same session
- [ ] Verify no `reasoning_content` error on second message
- [ ] Run `cargo build` — verify no `cowd-memory` package error
- [ ] Run `cargo test` — verify existing tests pass

---

## Execution Order

1. **Issue 3b** (cowd-memory) — quick fix, unblocks build
2. **Issue 3a** (reasoning_content) — single file change, critical for API
3. **Issue 2 (#1, #2, #3)** (auth + messages.js) — critical for frontend usability
4. **Issue 1** (TUI scroll) — requires ratatui changes, moderate complexity
5. **Issue 2 (#4)** (login UI) — new UI elements, lowest priority
