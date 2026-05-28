# Cowd TUI 全栈重构方案

## TL;DR

> **Quick Summary**: 渐进增强现有 ratatui 架构，引入组件抽象层、布局引擎、键绑定系统（Which-Key）、Diff Viewer、Command Palette、Session Sidebar、File Tree、Dialog System、Prompt 增强——7 大特性全栈重构，保持 Rust 原生性能零 JS 依赖。
>
> **Deliverables**: 组件抽象层 | 布局引擎 | 键绑定+Which-Key | Diff Viewer | Command Palette | Session Sidebar | File Tree | Dialog System | Prompt 增强 | Theme Engine v2 | Event System v2
>
> **Estimated Effort**: XLarge
> **Parallel Execution**: YES - 5 waves
> **Critical Path**: Task 1 → 8 → 15 → 22 → 31 → F1-F4

---

## Context

### Original Request
用户要求全面分析 cowd TUI 现状，参考 opencode 和 hermes-agent 的 TUI 设计（偏好 opencode），重新设计完备、好用、稳定、极速的 TUI，确保所有已有能力被最佳发挥。

### Interview Summary
- **重构策略**: 渐进增强 ratatui——保留 Rust 原生，不引入 JS 运行时
- **目标特性**: Which-Key, Diff Viewer, Command Palette, Session Sidebar, File Tree, Dialog System, Prompt 增强 (TUI Plugin 排除)
- **测试策略**: TDD (RED → GREEN → REFACTOR)
- **重构深度**: 全栈（事件+状态+组件+渲染+键绑定）
- **模型选择**: 所有 agent 使用 deepseek-v4-pro

### 当前 cowd TUI 架构 (~2651 行 Rust)
```
crates/cowd-cli/src/tui/
├── app.rs (875)   - App 状态 60+字段 + TimelineEntry
├── render.rs (273) - 渲染入口，单函数调用链
├── input.rs (296)  - 扁平 match 键处理
├── events.rs (111) - TuiEvent + mpsc channel
├── callbacks.rs (116) - ToolCallback 桥接
├── skin.rs (70)    - SkinConfig YAML 7色
├── osc52.rs (97)   - 剪贴板 tmux/screen
├── md_renderer.rs (130) - pulldown-cmark+syntect
├── widgets/chat.rs (540) - 虚拟滚动+增量重建
└── widgets/status_bar.rs (117) - Token 进度条
```

### opencode TUI 参考 (packages/opencode/src/cli/cmd/tui/)
- Ink/React 组件树，Routes + 20+ Context Providers
- Which-Key (`which-key.tsx`), Diff Viewer (`diff-viewer*.tsx`), Command Palette (`command-palette.tsx`)
- Session Sidebar, Prompt (autocomplete+frecency+history), Dialog System (alert/confirm/select/prompt)

### 现有能力 100% 保留清单
流式渲染 | Thinking/Tool/Slash 折叠 | Markdown+语法高亮 | 搜索 | 输入历史 | 会话选择器 | 审批 | 帮助 | 模型切换 | 主题 | OSC52 | 通知 | Token 追踪 | 虚拟滚动 | 文件浏览 | 记忆/技能/Gateway/Delegate 面板 | 鼠标滚轮

---

## Work Objectives

### Core Objective
将 cowd TUI 从单体架构重构为组件化框架，实现 opencode 级 7 大特性，保持 ratatui 原生性能。

### Must Have
Component trait | Layout engine | Keybinding+Which-Key | Diff Viewer | Command Palette | Session Sidebar | File Tree | Dialog System | Prompt Enhancement | 100% 向后兼容

### Must NOT Have
JS 运行时 | TUI Plugin 系统 | Web 渲染 | 破坏 TuiEvent 通道 | >3 新 crate 依赖 | 未使用抽象

---

## Verification Strategy
- **Infrastructure exists**: YES | **Automated tests**: TDD | **Framework**: Rust `#[cfg(test)]` + rstest
- **QA Policy**: 每任务含 agent-executed QA scenarios，证据存 `.omo/evidence/`

---

## Execution Strategy

```
Wave 1 (Start Immediately - Foundation, 7 tasks PARALLEL):
Task 1-7: Component trait, Layout types, Keybind types, Dialog types, Event v2 types, Theme types, Test infra

Wave 2 (After Wave 1 - Core Engines, 7 tasks PARALLEL):
Task 8-14: Render engine, Layout engine, Keybind+WhichKey, Dialog manager, Event dispatcher, Theme engine, Prompt component

Wave 3 (After Wave 2 - UI Components, 7 tasks PARALLEL):
Task 15-21: ChatView, DiffViewer, FileTree, CommandPalette, SessionSidebar, StatusBar v2, Markdown v2

Wave 4 (After Wave 3 - Integration, 10 tasks PARALLEL):
Task 22-31: Integrate engine, Port TimelineEntry, Port input, Port panels, Port OSC52, Port picker→Dialog, Port approval→Dialog, Port help→WhichKey, Split view, Wire main.rs

Wave 5 (After Wave 4 - Polish, 6 tasks PARALLEL):
Task 32-37: Performance, Animations, Error recovery, Config migration, Accessibility, Integration tests

Wave FINAL: F1-F4 (4 parallel reviews → user okay)
```

---

## TODOs

- [x] 1. Component trait system — `Component`, `RenderContext`, `EventHandler`

  **What to do**: Create `components/base.rs`. Define `Component` trait (render/handle_event/focusable/id), `RenderContext` (wraps Frame+theme), `EventResult` enum, `ComponentId` newtype. TDD: `component_trait_render_called`, `event_result_propagate`.

  **Must NOT do**: No virtual DOM diffing, no lifecycle beyond render+handle_event.

  **Agent**: `quick` | **Wave**: 1 (1-7 parallel) | **Blocks**: 8, 14, 21

  **References**: `app.rs:9-43` TimelineEntry pattern, `render.rs:8-43` current draw(), ratatui docs

  **Acceptance (TDD)**: `cargo test -p cowd-cli -- tui::components::base` → 3+ tests PASS

  **QA**: 
  - Scenario: Label struct implements Component → render+handle_event → EventResult::NotConsumed
  - Evidence: `.omo/evidence/task-1-*.txt`

  **Commit**: YES (groups 1-7) | `feat(tui): add Component trait, RenderContext, EventResult types` | Files: `components/mod.rs`, `base.rs`, `base_test.rs`

- [x] 2. Layout engine types — `Split`, `Tab`, `Constraint`, `LayoutTree`

  **What to do**: Create `layout/types.rs`. `SplitDirection`, `Split` (ratio+direction+children), `LayoutNode` (Split|TabGroup|Panel|Leaf), `TabGroup`/`TabDef`, `Constraint`. TDD: `split_ratio`, `tabgroup_cycle`, `layout_node_accepts_component`.

  **Agent**: `quick` | **Wave**: 1 | **Blocks**: 9, 15, 16, 17, 19, 25, 30

  **References**: opencode `routes/session/index.tsx` layout, ratatui Layout API

  **Acceptance**: `cargo test -p cowd-cli -- tui::layout::types` → 3+ PASS. Split::compute_areas 正确分屏.

  **QA**: Split 50/50 Horizontal from Rect(80,24) → left=40w, right x=40 w=40. TabGroup 3 tabs cycle 0→1→2→0.

  **Commit**: NO (groups 1-7)

- [x] 3. Keybinding system types — `KeyChord`, `Action`, `KeyMap`, `ModalLayer`

  **What to do**: Create `keybind/types.rs`. `KeyChord(Vec<KeyEvent>)`, `Action` enum (Execute/TogglePanel/OpenDialog/Scroll/Copy/Quit/...), `KeyBinding`, `KeyMap::resolve()`, `ModalLayer`. TDD: `keychord_eq`, `keymap_resolve`, `multichord`, `modal_isolation`.

  **Agent**: `quick` | **Wave**: 1 | **Blocks**: 10, 14, 18, 24, 29

  **References**: opencode `keymap.tsx` bindings, `which-key.tsx` overlay, current `input.rs:33-207`

  **Acceptance**: `cargo test -p cowd-cli -- tui::keybind::types` → 4+ PASS. Ctrl+X Ctrl+S → Execute("save").

  **QA**: Multi-chord resolve correct. ModalLayer Enter=Confirm overrides base Enter=Submit.

  **Commit**: NO (groups 1-7)

- [x] 4. Dialog system types — `DialogKind`, `DialogState`, `DialogResult`, `DialogManager`

  **What to do**: Create `components/dialog.rs` types. `DialogKind` (Alert|Confirm|Select|Prompt), `DialogState`, `DialogResult` (Ok|Cancel|Yes|No|Selected), `DialogManager` stack. TDD: `stack_push_pop`, `alert_ok`, `confirm_yes`, `select_nav`.

  **Agent**: `quick` | **Wave**: 1 | **Blocks**: 11, 27, 28

  **References**: opencode `ui/dialog-alert.tsx`, `dialog-select.tsx`, current `render.rs:55-76` approval modal

  **Acceptance**: `cargo test -p cowd-cli -- tui::components::dialog` → 4+ PASS.

  **QA**: Confirm→Enter→Yes. Select Up/Down/Enter navigates correctly.

  **Commit**: NO (groups 1-7)

- [x] 5. Event system v2 types — `Priority`, `RoutedEvent`, `EventBus`

  **What to do**: Create `event/mod.rs`, `dispatcher.rs`. `EventPriority` (High|Normal|Low), `RoutedEvent` (target+event+priority), `EventBus` with priority queue. MUST preserve existing `TuiEvent` channel unchanged.

  **Agent**: `quick` | **Wave**: 1 | **Blocks**: 12, 18, 22

  **References**: `events.rs:44-54` channel types, `callbacks.rs:9-39` bridge

  **Acceptance**: `cargo test -p cowd-cli -- tui::event` → 3+ PASS. High prio dequeued before Low. Existing TuiEvent tests still pass.

  **QA**: Priority order Low→High→Normal → dequeued as High,Normal,Low. RoutedEvent targets component correctly.

  **Commit**: NO (groups 1-7)

- [x] 6. Theme engine types — `Palette`, `StyleSheet`, `Theme`, `ThemeLoader`

  **What to do**: Create `theme/mod.rs`, `palette.rs`. 16 base+8 semantic colors, `StyleSheet` (heading1-6, code_block, inline_code, tool_status, diff_add/del, search_highlight, borders), `ThemeLoader::migrate_from_skin()`. TDD: `palette_rgb`, `stylesheet_overrides`, `theme_yaml_roundtrip`, `skin_auto_migration`.

  **Agent**: `quick` | **Wave**: 1 | **Blocks**: 13, 20, 35

  **References**: `skin.rs:7-64` SkinConfig, `app.rs:262-279` Theme enum, `chat.rs:219-323` color patterns

  **Acceptance**: `cargo test -p cowd-cli -- tui::theme` → 4+ PASS. Old skin.yaml auto-migrates.

  **QA**: Theme YAML roundtrip preserves colors. SkinConfig→Theme migration: accent="#FF0000" → Color::Rgb(255,0,0).

  **Commit**: NO (groups 1-7)

- [x] 7. Test infrastructure setup — `MockTerminal`, `MockEventSender`, `tui_test!` macro

  **What to do**: Create `test_utils/mod.rs`. `MockTerminal<TestBackend>` (draw/assert_line_contains/assert_line_count), `MockEventSender` (press_key/press_chord/type_text), `tui_test!` macro, fixtures (app_with_messages/tool_calls/streaming).

  **Agent**: `deep` | **Wave**: 1 | **Blocks**: all subsequent tests

  **References**: `events.rs:56-111` test patterns, `callbacks.rs:42-116` callback tests, ratatui Terminal API, opencode `test/cli/tui/`

  **Acceptance**: `cargo test -p cowd-cli -- tui::test_utils` → 3+ PASS. `tui_test!` macro compiles and runs.

  **QA**: MockTerminal renders App with "Hello World" → assert_line_contains passes.

  **Commit**: YES | `test(tui): add TUI test harness` | `test_utils/mod.rs`

---

### Wave 2 — Core Engines (Tasks 8-14, ALL parallel)

- [x] 8. Component tree render engine — recursive rendering with LayoutTree

  **What to do**: Create `components/render_engine.rs`. `render_tree()` recurses LayoutNode→areas→Component::render. Handle Split/TabGroup/Panel/Leaf. TDD: `render_flat`, `render_split`, `render_tabgroup_active`.

  **Agent**: `deep` | **Wave**: 2 | **Blocks**: 15-20, 22 | **Blocked By**: 1

  **References**: Task 1 Component trait, Task 2 LayoutNode, `render.rs:8-43` current draw()

  **Acceptance**: Split(H,0.7) renders 56w+24w on 80-wide. TabGroup renders only active tab.

  **QA**: Split ratio correct. Tab switching via KeyEvent::Tab changes active.

  **Commit**: YES | `feat(tui): component tree render engine`

- [x] 9. Layout engine — TabBar, ResizeHandle, FocusManager

  **What to do**: Create `layout/engine.rs`. `TabBar` widget (labels+highlight+click), `ResizeHandle` (│/─), `Panel` rendering (Block+border+focus ring), `FocusManager` (next/prev/wrap).

  **Agent**: `deep` | **Wave**: 2 | **Blocks**: 15-17, 19, 25, 30 | **Blocked By**: 2

  **References**: Task 2 types, `render.rs:94-118` panel rendering, ratatui Tabs widget

  **Acceptance**: TabBar 5 tabs on 80-char. FocusManager next() wraps.

  **QA**: TabBar click on "Files" switches to idx 1. Focus A→B→C→A wraps.

  **Commit**: YES | `feat(tui): layout engine (TabBar, ResizeHandle, FocusManager)`

- [x] 10. Keybinding engine + Which-Key overlay — `Space` leader key, multi-chord, modal

  **What to do**: Create `keybind/engine.rs`, `which_key.rs`. `KeybindEngine` (handle_key/push_modal/pop_modal), pending chord accumulation+timeout. `WhichKey` overlay renders available bindings for current modal+prefix. `Space` triggers full binding list.

  **Agent**: `deep` | **Wave**: 2 | **Blocks**: 14, 18, 24, 29 | **Blocked By**: 3

  **References**: opencode `which-key.tsx` overlay layout, `keymap.tsx` definitions

  **Acceptance**: Space→f→s dispatches Execute("file_save"). WhichKey shows Space+f/g/s/... options.

  **QA**: Press Space → overlay visible with bindings list. Partial Space+g → narrowed to git bindings. Timeout flushes.

  **Commit**: YES | `feat(tui): keybinding engine with modal layers and Which-Key overlay`

- [x] 11. Dialog manager — render dialog stack with focus trap

  **What to do**: Extend `components/dialog.rs`. Render topmost dialog centered+backdrop dim. Per-kind: alert/confirm/select/prompt. Auto-size (80% screen max). Focus trap when stack non-empty.

  **Agent**: `visual-engineering` | **Wave**: 2 | **Blocks**: 27, 28 | **Blocked By**: 4

  **References**: opencode `ui/dialog.tsx`, `dialog-alert/confirm/select/prompt.tsx`

  **Acceptance**: Dialog centered on 80x24. All 4 kinds render correctly.

  **QA**: Alert centered, key 'a' consumed by dialog not app. Confirm→Y returns Yes. Select→↓→Enter returns Selected(1).

  **Commit**: YES | `feat(tui): dialog manager with alert/confirm/select/prompt`

- [x] 12. Event dispatcher v2 — priority queue + component routing

  **What to do**: Extend `event/dispatcher.rs`. `EventDispatcher::dispatch(event)` routes to target Component::handle_event. Priority queue drain (High→Normal→Low). Integrate with existing TuiEvent receiver (background events → RoutedEvent translation).

  **Agent**: `deep` | **Wave**: 2 | **Blocks**: 18, 22 | **Blocked By**: 5

  **References**: `events.rs:10-42` TuiEvent enum, `callbacks.rs:15-39` callback→event mapping

  **Acceptance**: High prio events processed before Low when both pending. Component routing works.

  **QA**: Send 3 events at different priorities → processed in correct order. Targeted event reaches correct component.

  **Commit**: YES | `feat(tui): event dispatcher v2 with priority queue and routing`

- [x] 13. Theme engine — hot reload, style computation, builtin themes

  **What to do**: Create `theme/engine.rs`. `ThemeEngine::load(path)` from YAML, `hot_reload()` detects file changes, `compute_style(context) -> Style` for StyleSheet lookup. Builtin dark/light themes. Colors from hex or named.

  **Agent**: `quick` | **Wave**: 2 | **Blocks**: 20, 35 | **Blocked By**: 6

  **References**: `skin.rs` SkinConfig, `chat.rs` color usage, opencode `context/theme.tsx`

  **Acceptance**: Hot reload detects theme.yaml change. compute_style("heading1") returns bold cyan.

  **QA**: Load dark theme → bg=Black fg=White. Toggle to light → bg=White fg=Black. Hot reload watches file.

  **Commit**: YES | `feat(tui): theme engine with hot reload and StyleSheet`

- [x] 14. Prompt component — autocomplete, frecency, @file, /command

  **What to do**: Create `components/prompt.rs`. Enhanced input wrapping tui-textarea. `AutocompleteEngine` (prefix→suggestions from file paths, commands, history). `FrecencyTracker` (frequency+recency scoring for sort). `@file` completion via glob. `/command` completion from SlashCommandSpec list. Inline suggestion preview (dimmed). Tab to accept.

  **Agent**: `deep` | **Wave**: 2 | **Blocks**: 22 | **Blocked By**: 1, 10

  **References**: opencode `prompt/index.tsx`, `autocomplete.tsx`, `frecency.tsx`, `history.tsx`, `cwd.ts`

  **Acceptance**: Type "@sr" → suggests "src/". Type "/sta" → suggests "/status". Frecency prioritizes recent+frequent.

  **QA**: @file autocomplete filters by prefix. /command shows matching slash commands. Tab accepts top suggestion. History navigates with Alt+↑↓.

  **Commit**: YES | `feat(tui): prompt component with autocomplete, frecency, @file, /command`

---

### Wave 3 — UI Components (Tasks 15-21, ALL parallel)

- [x] 15. ChatView component — timeline rendering, virtual scroll, incremental rebuild

  **What to do**: Create `components/chat_view.rs`. Port existing `widgets/chat.rs` logic to Component trait. TimelineEntry rendering (Message/Thinking/ToolCall/SlashOutput). Virtual scrolling (>3x viewport). Incremental rebuild (streaming tail). Scroll-to-entry. Loading spinner.

  **Agent**: `deep` | **Wave**: 3 | **Blocks**: 22, 23, 30 | **Blocked By**: 8, 9

  **References**: `widgets/chat.rs:10-540` entire current implementation, `md_renderer.rs` markdown→Lines

  **Acceptance**: All existing chat features work via Component trait. Virtual scroll triggers at >3x.

  **QA**: Send 50 messages → virtual scroll activates. Streaming text appends incrementally. Enter toggles expand/collapse. Scrollfollow auto.

  **Commit**: YES | `feat(tui): ChatView component (timeline, virtual scroll, incremental rebuild)`

- [x] 16. DiffViewer component — unified/split diff, syntax highlight, file tree

  **What to do**: Create `components/diff_viewer.rs`. Parse unified diff format. Render added lines (green bg), removed lines (red bg), context lines. Syntax highlight changed lines via syntect. File tree sidebar listing changed files with +/- counts. Toggle unified/split mode. Navigate between hunks.

  **Agent**: `visual-engineering` | **Wave**: 3 | **Blocks**: 30 | **Blocked By**: 8, 9

  **References**: opencode `diff-viewer.tsx`, `diff-viewer-ui.tsx`, `diff-viewer-file-tree.tsx`, `diff-viewer-file-tree-utils.ts`

  **Acceptance**: Unified diff renders with correct green/red coloring. File tree shows changed files.

  **QA**: Parse `git diff` output → render with correct colors. Hunk navigation with n/N. Toggle unified↔split.

  **Commit**: YES | `feat(tui): DiffViewer component (unified/split, syntax, file tree)`

- [x] 17. FileTree component — tree navigation, file preview, git status

  **What to do**: Create `components/file_tree.rs`. Recursive tree from directory entries. Expand/collapse folders. File icons (📁/📄/🔧). Git status overlay (M/A/D/?). File preview on selection (first 20 lines). Navigate with j/k, Enter to expand, l/h to open/close.

  **Agent**: `visual-engineering` | **Wave**: 3 | **Blocks**: 30 | **Blocked By**: 8, 9

  **References**: opencode `feature-plugins/sidebar/files.tsx`, current `render.rs:120-136` flat file browser

  **Acceptance**: Tree renders nested directories. Expand/collapse works. Preview shows file contents.

  **QA**: Navigate tree with j/k. Enter on folder expands. Enter on file shows preview. Git M/A status indicators visible.

  **Commit**: YES | `feat(tui): FileTree component (tree nav, file preview, git status)`

- [x] 18. CommandPalette component — fuzzy search, action dispatch

  **What to do**: Create `components/command_palette.rs`. `Ctrl+P` opens overlay with search input. Fuzzy match against all registered actions (slash commands, keybind actions, dialog triggers). Ranked results list. Enter dispatches Action via EventDispatcher. Esc closes.

  **Agent**: `quick` | **Wave**: 3 | **Blocks**: 22 | **Blocked By**: 8, 10, 12

  **References**: opencode `command-palette.tsx`, current 100+ slash commands from `commands` crate

  **Acceptance**: Ctrl+P opens palette. Typing "sess" shows session-related commands. Fuzzy "sc" matches "/search".

  **QA**: Open palette → type "diff" → shows "/diff", "git diff". Enter executes action. Esc closes cleanly.

  **Commit**: YES | `feat(tui): CommandPalette component (fuzzy search, action dispatch)`

- [x] 19. SessionSidebar component — session list, rename, delete, switch

  **What to do**: Create `components/session_sidebar.rs`. List sessions with date+message count. `Enter` switches session. `r` renames (inline edit). `d` deletes (with confirm dialog). `n` new session. Highlight current. Sort by last active.

  **Agent**: `quick` | **Wave**: 3 | **Blocks**: 30 | **Blocked By**: 8, 9

  **References**: opencode `routes/session/sidebar.tsx`, current `render.rs:72-92` session picker modal

  **Acceptance**: Session list renders with dates. Switch/rename/delete work. Confirm dialog on delete.

  **QA**: Select session with j/k → Enter switches. Press r → inline rename. Press d → confirm dialog → delete or cancel.

  **Commit**: YES | `feat(tui): SessionSidebar component (list, rename, delete, switch)`

- [x] 20. StatusBar component v2 — modular sections, dynamic content

  **What to do**: Port `widgets/status_bar.rs` to Component trait. Modular section registration (model, panel, spinner, tokens, search, history, notifications). Dynamic width allocation. Token progress bar. Color from Theme.

  **Agent**: `quick` | **Wave**: 3 | **Blocks**: 22 | **Blocked By**: 8, 13

  **References**: `widgets/status_bar.rs:1-117` entire current, Theme engine from Task 13

  **Acceptance**: All existing status bar info preserved. Section width adapts to terminal width.

  **QA**: Narrow terminal → sections truncate gracefully. Token bar shows ████░░░░. Notification overlays briefly.

  **Commit**: YES | `feat(tui): StatusBar v2 with modular sections and theme integration`

- [x] 21. Markdown renderer v2 — tables, task lists, blockquotes, links

  **What to do**: Extend `md_renderer.rs`. Add table rendering (pulldown_cmark table events → aligned columns with borders). Task list (`- [ ]` → ☐, `- [x]` → ☑). Blockquote (`>` prefix + dimmed color). Link rendering (`[text](url)` → colored text). Image alt text fallback. Nested list indentation.

  **Agent**: `quick` | **Wave**: 3 | **Blocks**: 15 (chat uses md_renderer) | **Blocked By**: 1

  **References**: `md_renderer.rs:1-130`, pulldown_cmark docs for table/tasklist/blockquote tags, opencode markdown patterns

  **Acceptance**: Table renders with aligned columns. Task list shows ☐/☑. Blockquote renders with `│` prefix.

  **QA**: Markdown table input → rendered with borders. `- [x] Done` → ☑ Done. `> quote` → dimmed with bar.

  **Commit**: YES | `feat(tui): markdown renderer v2 (tables, task lists, blockquotes, links)`

---

### Wave 4 — Integration & Porting (Tasks 22-31, ALL parallel)

- [x] 22. Integrate new engine with existing App → new `TuiState`

  **What to do**: Create `TuiState` struct merging App fields with new engine (LayoutTree, KeybindEngine, EventDispatcher, ThemeEngine, DialogManager). Bridge old `App::apply_event(TuiEvent)` → EventDispatcher. Preserve ALL App methods as TuiState methods.

  **Agent**: `deep` | **Wave**: 4 | **Blocks**: 23-31 | **Blocked By**: 8, 12

  **References**: `app.rs:127-217` App struct (60+ fields)

  **Acceptance**: All existing App methods work via TuiState. TuiEvent channel unchanged.

  **QA**: Create TuiState → add_message → timeline updated. apply_event(TextDelta) → streaming works. All App tests adapted and pass.

  **Commit**: YES | `feat(tui): integrate new engine with TuiState, preserve backward compat`

- [x] 23. Port TimelineEntry to component system — ChatView integration

  **What to do**: Wire TimelineEntry rendering through ChatView Component. Ensure Message/Thinking/ToolCall/SlashOutput all render via Component::render. Preserve expand/collapse cursor navigation.

  **Agent**: `deep` | **Wave**: 4 | **Blocks**: 30 | **Blocked By**: 15, 22

  **References**: `app.rs:9-96` TimelineEntry, `widgets/chat.rs:326-539` build_entry()

  **Acceptance**: All 4 TimelineEntry variants render correctly in ChatView. Cursor nav works.

  **QA**: Add Message→renders. Add Thinking→collapsed preview. Enter expands. ToolCall shows progress then result.

  **Commit**: YES | `feat(tui): port TimelineEntry to ChatView component`

- [x] 24. Port input handling to keybinding system

  **What to do**: Replace `input.rs` flat match with KeybindEngine dispatch. Map existing shortcuts (Enter/Ctrl+C/Tab/PgUp/Ctrl+T/Y/A/E/W/U/K/Z/M/Alt+↑↓) to KeyMap bindings. Preserve all behaviors.

  **Agent**: `quick` | **Wave**: 4 | **Blocks**: 31 | **Blocked By**: 10, 22

  **References**: `input.rs:16-296` all current handlers

  **Acceptance**: ALL existing keyboard shortcuts work identically through KeyMap.

  **QA**: Enter→submit. Ctrl+C→exit. Tab→next panel. Ctrl+T→toggle theme. Alt+↑→history. All unchanged.

  **Commit**: YES | `feat(tui): port input handling to keybinding system`

- [x] 25. Port panels (Gateway, Memory, Skills, Delegates) to Tab system

  **What to do**: Convert 5 panel modals (Gateway/Files/Memory/Skills/Delegates) to TabGroup tabs. Each panel becomes a Component. Tab switching via Tab key + click.

  **Agent**: `quick` | **Wave**: 4 | **Blocks**: 31 | **Blocked By**: 9, 22

  **References**: `render.rs:94-272` gateway/file/memory/skills/delegate panels, `app.rs:124-126` Panel enum

  **Acceptance**: All 5 panels accessible as tabs, not modals. Tab switching works.

  **QA**: Tab cycles through Chat→Gateway→Files→Memory→Skills→Delegates. Each renders correctly.

  **Commit**: YES | `feat(tui): port panels to Tab system`

- [x] 26. Port OSC52 clipboard — preserve with zero changes

  **What to do**: Verify OSC52 works in new architecture. `Ctrl+Y` → `Action::Copy` → `osc52::write_osc52_clipboard()`. tmux/screen multiplexer wrapping preserved.

  **Agent**: `quick` | **Wave**: 4 | **Blocks**: none | **Blocked By**: 22

  **References**: `osc52.rs:1-97`, `app.rs:548-557` copy_focused_content()

  **Acceptance**: Ctrl+Y copies focused entry content. Works in tmux.

  **QA**: Focus a Message → Ctrl+Y → paste in another terminal → content matches.

  **Commit**: NO (groups 26-29)

- [x] 27. Port session picker → Dialog system

  **What to do**: Replace `render.rs:72-92` picker modal with `DialogKind::Select`. Session list items, j/k navigation, Enter selects, Esc cancels.

  **Agent**: `quick` | **Wave**: 4 | **Blocks**: 31 | **Blocked By**: 11, 22

  **References**: `render.rs:72-92` picker, `app.rs:431-453` picker methods

  **Acceptance**: Session picker opens as Select dialog. j/k navigate. Enter selects session.

  **QA**: /resume → picker opens. Select session with j/k Enter. Esc cancels.

  **Commit**: NO (groups 26-29)

- [x] 28. Port approval flow → Dialog system

  **What to do**: Replace `render.rs:55-76` approval modal with `DialogKind::Confirm`. Tool name + input preview as message. Y=Yes, N=No/Esc=Cancel.

  **Agent**: `quick` | **Wave**: 4 | **Blocks**: 31 | **Blocked By**: 11, 22

  **References**: `render.rs:55-76`, `input.rs:284-295` approval handler

  **Acceptance**: Approval shows as Confirm dialog. Y approves, N denies.

  **QA**: Trigger approval → dialog shows tool+preview. Press Y → `__approval_approved__`. Press N → `__approval_denied__`.

  **Commit**: NO (groups 26-29)

- [x] 29. Port help panel → Which-Key overlay

  **What to do**: Replace `render.rs:138-199` static help modal with Which-Key overlay. `?` triggers Which-Key showing ALL available shortcuts dynamically from KeyMap. No hardcoded list.

  **Agent**: `quick` | **Wave**: 4 | **Blocks**: 31 | **Blocked By**: 10, 22

  **References**: `render.rs:138-199` help modal, opencode which-key rendering

  **Acceptance**: ? shows all current keybindings from KeyMap. Auto-updates when bindings change.

  **QA**: Press ? → overlay shows Enter/Shift+Enter/Esc/Ctrl+C/Tab/... all from KeyMap. Press ? again to close.

  **Commit**: YES | `feat(tui): port help panel to dynamic Which-Key overlay`

- [x] 30. Implement split view — Chat + Sidebar/File/Diff simultaneous

  **What to do**: Create default layout: `Split(Horizontal, 0.7)`: [ChatView, TabGroup(Files|SessionList|DiffViewer|Gateway)]. Allow toggling sidebar (Ctrl+B). Resize handle between chat and sidebar.

  **Agent**: `visual-engineering` | **Wave**: 4 | **Blocks**: 31 | **Blocked By**: 9, 15, 19

  **References**: opencode `routes/session/index.tsx` main layout with sidebar

  **Acceptance**: Default layout shows Chat (70%) + Sidebar TabGroup (30%). Ctrl+B toggles sidebar.

  **QA**: Start TUI → Chat+Sidebar visible. Ctrl+B hides sidebar → Chat full width. Ctrl+B again restores. Resize handle draggable.

  **Commit**: YES | `feat(tui): split view (Chat + Sidebar/File/Diff) with resize handle`

- [x] 31. Wire main.rs `run_tui_repl` with new TuiState engine

  **What to do**: Update `crates/cowd-cli/src/main.rs:run_tui_repl` (line ~2617). Replace `App::new()` with `TuiState::new()`. Replace `draw(&mut frame, &mut app)` with `render_tree()`. Replace `handle_input(&mut app)` with `KeybindEngine::handle_key()`. Preserve TuiEvent channel + StreamTurnRunner untouched.

  **Agent**: `deep` | **Wave**: 4 | **Blocks**: 32-37 | **Blocked By**: 22

  **References**: `main.rs:2617-2780` run_tui_repl + drain_tui_events

  **Acceptance**: `cowd --tui` launches with new engine. All existing functionality works.

  **QA**: Launch TUI → status bar shows Cowd. Type message → Enter → streaming response renders. All shortcuts work.

  **Commit**: YES | `feat(tui): wire main.rs run_tui_repl with new TuiState engine`

---

### Wave 5 — Polish & Performance (Tasks 32-37, ALL parallel)

- [x] 32. Performance optimism — frame timing, render caching, profiler

  **What to do**: Add frame timing (target 60fps idle, 30fps streaming). Render cache: skip re-render when msg_version unchanged. Profiler: log render times per component. Optimize line count pre-computation.

  **Agent**: `deep` | **Wave**: 5 | **Blocked By**: 31

  **Acceptance**: Idle CPU <5%. Frame times <16ms (60fps). No visible lag during streaming.

  **QA**: Start TUI idle → CPU low. Stream 1000 lines → no frame drops. Check profiler output.

  **Commit**: YES | `perf(tui): frame timing, render caching, profiler`

- [x] 33. Animation transitions — panel open/close, search highlight pulse

  **What to do**: Sidebar slide animation (width transitions over 8 frames). Search match highlight pulse (bright→dim over 4 frames). Dialog fade-in (opacity from 0→1 over 4 frames). Spinner smooth rotation.

  **Agent**: `visual-engineering` | **Wave**: 5 | **Blocked By**: 31

  **Acceptance**: Sidebar opens with smooth slide. Search matches pulse. Dialog appears with fade.

  **QA**: Ctrl+B → sidebar slides in/out. /search → match pulses briefly. Dialog opens with fade.

  **Commit**: YES | `feat(tui): animation transitions (panel, search, dialog)`

- [x] 34. Error recovery — panic handler, crash report, graceful degrade

  **What to do**: Custom panic hook: save terminal state before crash, restore on exit. Crash report: write stack trace to `~/.cowd/crash.log`. Graceful degrade: if component.render() panics, show error placeholder instead of crashing app.

  **Agent**: `deep` | **Wave**: 5 | **Blocked By**: 31

  **Acceptance**: Panic in component doesn't crash TUI. Terminal restored on crash. Crash log written.

  **QA**: Trigger panic in mock component → error placeholder shown, TUI continues. Check crash.log has stack trace.

  **Commit**: YES | `feat(tui): error recovery (panic handler, crash report, graceful degrade)`

- [x] 35. Config migration — old skin.yaml → new theme.yaml, config version bump

  **What to do**: Auto-detect old `skin.yaml` on startup → migrate to `theme.yaml` → write backup `skin.yaml.bak`. Config version field: `tui_version: 2`. Migration report printed on first run.

  **Agent**: `quick` | **Wave**: 5 | **Blocked By**: 13, 31

  **Acceptance**: Existing skin.yaml auto-migrated on first launch. Backup preserved. No data loss.

  **QA**: Place old skin.yaml → launch TUI → theme.yaml created → skin.yaml.bak saved → colors correct.

  **Commit**: YES | `feat(tui): config migration (skin.yaml → theme.yaml)`

- [x] 36. Accessibility — screen reader hints, high contrast mode

  **What to do**: Add ARIA-like labels to focusable components. High contrast theme (WCAG AA minimum contrast). Screen reader mode: output component tree as text for Orca/VoiceOver. `--tui-accessibility` flag.

  **Agent**: `quick` | **Wave**: 5 | **Blocked By**: 31

  **Acceptance**: High contrast theme has >4.5:1 contrast ratio. Screen reader receives component labels.

  **QA**: `cowd --tui --tui-accessibility` → high contrast theme loaded. Component labels present.

  **Commit**: YES | `feat(tui): accessibility (screen reader hints, high contrast mode)`

- [x] 37. Comprehensive integration tests — end-to-end TUI scenarios

  **What to do**: Test file: `tests/tui_integration.rs`. Scenarios: launch→type→stream, switch panels, session picker flow, approval flow, search flow, model switch, theme toggle, diff viewer, command palette. All via MockTerminal + MockEventSender.

  **Agent**: `deep` | **Wave**: 5 | **Blocked By**: 31

  **Acceptance**: `cargo test -p cowd-cli -- tui_integration` → 10+ tests PASS. Covers all 7 new features + existing features.

  **QA**: Full launch→chat→stream→quit cycle passes. Panel switch works. Session picker end-to-end.

  **Commit**: YES | `test(tui): comprehensive integration tests (10+ scenarios)`

---

## Final Verification Wave

- [x] F1. **Plan Compliance Audit** — `oracle`
  Read plan end-to-end. Verify all "Must Have" implemented (grep for each). Verify all "Must NOT Have" absent (grep for forbidden patterns). Check evidence files exist. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [37/37] | VERDICT: APPROVE/REJECT`

- [x] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo test -p cowd-cli` + `cargo clippy --workspace`. Review for `as any`/`@ts-ignore`, empty catches, console.log, commented-out code, unused imports. Check AI slop: excessive comments, over-abstraction, generic names.
  Output: `Build [PASS/FAIL] | Lint [PASS/FAIL] | Tests [N/N] | VERDICT`

- [x] F3. **Real Manual QA** — `unspecified-high` (+ `playwright` for tmux capture)
  Start clean state. Execute EVERY QA scenario from EVERY task. Test cross-task integration. Test edge cases: empty state, max scroll, rapid input, terminal resize, tmux nesting.
  Output: `Scenarios [N/N] | Integration [N/N] | Edge Cases [N] | VERDICT`

- [x] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual diff. Verify 1:1 — everything specified was built, nothing extra. Check "Must NOT do" compliance. Detect cross-task contamination. Flag unaccounted changes.
  Output: `Tasks [37/37] | Contamination [CLEAN/N] | Unaccounted [CLEAN/N] | VERDICT`

---

## Commit Strategy
- Wave 1 batch (Tasks 1-7): `feat(tui): foundation — Component/Layout/Keybind/Dialog/Event/Theme types + test infra`
- Wave 2 (8-14): per-task commits (7 commits)
- Wave 3 (15-21): per-task commits (7 commits)
- Wave 4 (22-31): per-task + grouped commits (7 commits)
- Wave 5 (32-37): per-task commits (6 commits)
- **Total**: ~28 commits

## Success Criteria
```bash
cargo test -p cowd-cli -- tui                      # ALL tests pass
cargo clippy --workspace -- -D warnings             # Zero warnings
cowd --tui                                          # Launch successfully
```
- [ ] All 37 tasks completed
- [ ] 7 new features functional
- [ ] All existing features preserved
- [ ] F1-F4 all APPROVE
