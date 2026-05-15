# Cowd Bug Fix — Verification & Validation Report

## Execution Summary

| # | Issue | Status | Files Changed |
|---|-------|--------|---------------|
| 3b | `cowd-memory` package not found | ✅ Fixed | `crates/memory/Cargo.toml`, 3 dependent Cargo.toml files, workspace `Cargo.toml` |
| 3a | `reasoning_content` API error | ✅ Fixed | `crates/rusty-claude-cli/src/server.rs` |
| 2 | Auth payload mismatch + token propagation + duplicate functions | ✅ Fixed | `webui/api.js`, `webui/messages.js`, `webui/boot.js`, `webui/index.html` |
| 1 | TUI display corruption | ✅ Fixed | `crates/rusty-claude-cli/src/tui/render.rs`, `tui/app.rs`, `tui/input.rs`, `main.rs` |

## Detailed Verification Results

### Fix 3b: Package Renaming
- **Change**: Renamed `memory` → `cowd-memory` in `crates/memory/Cargo.toml`; all 3 dependents use `package = "cowd-memory"` to preserve `memory` crate name in code
- **Build**: ✅ `cargo check --workspace` passes
- **Test**: ✅ All 33 render tests pass

### Fix 3a: reasoning_content Error
- **Change**: Added `SessionContentBlock::Thinking` → `InputContentBlock::Thinking` conversion in `server.rs:build_message_request()`, matching the CLI path in `executor.rs`
- **Build**: ✅ Compiles
- **Verification**: The `openai_compat.rs` message-building code (lines 966-982) correctly converts `InputContentBlock::Thinking` back to `reasoning_content` in JSON payloads for DeepSeek

### Fix 2: Frontend Auth & SSE
- **api.js**: Login sends `{token}` (matching backend `LoginRequest`), auth token stored in localStorage + propagated via `Authorization: Bearer` header, 401 auto-redirects to login
- **messages.js**: Removed duplicate `handleLine`/`dispatch` functions that overrode the proper event-dispatch pipeline
- **index.html**: Added login modal, removed duplicate `id="token-usage"` element
- **boot.js**: Added auth check on load, `showLoginModal()` function with login flow
- **JS Syntax**: ✅ All 7 JS files pass `node -c` check

### Fix 1: TUI Scroll & Display
- **app.rs**: Added `scroll_offset: u16`, `auto_scroll: bool` fields
- **render.rs**: `draw_messages()` now renders a scrollable `Paragraph` with `Clear` before each frame, Scrollbar widget, auto-scroll-to-bottom on new content
- **input.rs**: Added PageUp/PageDown scroll keybindings
- **Build**: ✅ Compiles (no errors, pre-existing warnings only)
- **Tests**: ✅ 33/33 render tests pass

## Pre-existing Issues (NOT caused by our changes)
- Multiple deprecation warnings (`SqliteSessionStore`, `SessionStore`) — pre-existing
- `imap-proto v0.10.2` future-incompatibility warning — pre-existing
- Unused function/variant warnings — pre-existing

## Test Commands for Manual Verification

```bash
# Full workspace build
cargo build --workspace

# Render tests
cargo test --package rusty-claude-cli -- render

# TUI smoke test (requires terminal)
cargo run -- cowd serve --no-auth --port 8642
# Then: curl http://localhost:8642/health

# WebUI test (after serve is running)
# Open http://localhost:8642 in browser
# 1. Login modal should appear if no token
# 2. Enter token from config.yaml to login
# 3. Create a new session and send a message
# 4. Verify SSE streaming works (text, tools, thinking blocks)
# 5. Test all right panels: Memory, Skills, Crons, Settings, Gateway, Agents, Tools
# 6. Test control center tabs: Config, Providers, Approval, History, Usage

# DeepSeek thinking mode test
# Use config with deepseek model, send 2+ messages in same session
# Verify no "reasoning_content must be passed back" error
```
