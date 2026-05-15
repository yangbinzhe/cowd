# Cowd Bug Fix — Final Audit & Verification Report

## Execution Summary

| # | Issue | Root Cause | Fix | Status |
|---|-------|-----------|-----|--------|
| 3b | `cowd-memory` not found | Package named `memory`, user expected `cowd-memory` | Renamed package to `cowd-memory` with `package =` alias | ✅ |
| 3a | `reasoning_content` API 400 | HTTP server `build_message_request()` stripped Thinking blocks | Added `SessionContentBlock::Thinking` → `InputContentBlock::Thinking` conversion | ✅ |
| 2a | Login failed | Frontend sent `{password}` but backend expected `{token}` | Changed to `{token:token}` matching `LoginRequest` struct | ✅ |
| 2b | 401 after login | Auth token not propagated in API calls | Added token storage + `Authorization: Bearer` header in all requests | ✅ |
| 2c | SSE events broken | Duplicate `handleLine`/`dispatch` overrode event routing | Removed second copy, kept first with full event dispatch | ✅ |
| 2d | No login UI | Missing login modal | Added login modal to index.html + auth check in boot.js | ✅ |
| 1a | TUI display corruption | No scroll state, content overflow bleeds into input | Scrollable Paragraph + Clear + Scrollbar + auto-scroll | ✅ |
| — | **AUDIT FINDING**: SSE stream fetch missing auth | fetch() didn't include Authorization header | Added Bearer token to fetch headers | ✅ |
| — | **AUDIT FINDING**: WebSocket missing auth | Browser cannot set custom headers on WS | Backend: added `?token=` query param fallback for WS upgrade | ✅ |
| — | **AUDIT FINDING**: WebSocket frontend | No token sent to WebSocket | Frontend: added `?token=` to WebSocket URL | ✅ |
| — | **AUDIT FINDING**: Duplicate `id="token-usage"` | Two elements with same DOM id | Removed duplicate element from index.html | ✅ |

## Self-Audit Findings

During self-review, I discovered 4 additional bugs not in the original report:

1. **SSE stream missing auth header** (`messages.js:36`): The `fetch()` to `/api/sessions/:id/messages/stream` didn't include `Authorization` header. When auth is enabled, the stream would silently fail (401 caught by `.catch(()=>{scheduleReconnect()})` — infinite silent retry loop).

2. **WebSocket missing auth** (`messages.js:13` + `server.rs`): Browsers cannot set custom headers on WebSocket connections. The `/ws` endpoint is behind auth middleware. Solution: Backend `check_auth()` now also checks `?token=` query parameter for WebSocket upgrade requests. Frontend appends `?token=xxx` to WebSocket URL.

3. **Duplicate `id="token-usage"`** in `index.html`: Two elements sharing the same DOM id causes `document.getElementById()` to return only the first one. The second (in chat-header-right) was duplicating the first's function.

4. **TUI `Clear` widget before render**: Added `frame.render_widget(Clear, area)` before the message paragraph to prevent ANSI/tail bleed from previous frames into the current render area.

## Potential Unhandled Edge Cases (Documented, Not Critical)

These are pre-existing architecture limitations, not regression bugs:

1. **Auth token in localStorage**: If the server's `auth_token` changes between runs, the stored token in localStorage becomes stale. The 401 handler in `api.js` clears the token and shows login modal — acceptable UX.

2. **WebSocket reconnect after auth change**: If auth is enabled after the page loads, WS will close and reconnect will fail with 401. The WS `onclose` handler just sets `ws=null` without retry. Real-time session updates stop working, but REST API calls continue to work. This is a pre-existing limitation, not a regression.

3. **Scroll state on panel switch**: When switching between panels (Chat ↔ Files ↔ Memory etc.), the scroll offset is preserved. If the message list changes while on a different panel, auto-scroll won't apply until returning to Chat. This is acceptable behavior — the offset resets to bottom on first render when returning to Chat since `auto_scroll` is true.

## Verification Results

| Test | Result |
|------|--------|
| `cargo check --workspace` | ✅ Passes (pre-existing warnings only) |
| `cargo test --package rusty-claude-cli -- render` | ✅ 33/33 passed |
| All JS files `node -c` syntax | ✅ All 7 files pass |
| WebSocket auth query param | ✅ Backend `check_auth` updated, frontend URL updated |
| SSE stream auth header | ✅ `fetch()` includes Bearer token |
