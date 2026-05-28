# Cowd TUI 体验提升 — Phase 2 方案

## TL;DR

> **Quick Summary**: 对标 opencode + hermes-agent，在已完成的重构基础上补齐 19 项 TUI 交互/视觉/效率差距，4 个阶段渐进增强，零破坏性变更。
>
> **Deliverables**: Toast 通知系统 | 启动加载 | 会话 Fork | 修改文件面板 | 每消息菜单 | 多阶段权限 | Todo 面板 | 导出对话框 | 子Agent 导航 | Diff 计数 | 上下文面板 | MCP/LSP 状态 | Shell 模式 | Which-Key 分组 | Thinking 统一面板 | 搜索高亮 | ANSI 降级
>
> **Estimated Effort**: Medium (1-2 weeks)
> **Parallel Execution**: YES — 4 phases, 5-6 tasks per phase
> **Critical Path**: Phase 1 → Phase 2 → Phase 3 → Phase 4

---

## Context

### 对标分析来源
- `/datas/workspace/plan/0523-重构/02-tui提升/GAP-ANALYSIS.md`
- 仅 TUI 层面（终端交互、视觉、操作效率），不含后端/运行时

### 当前基线
- Wave 1-5 重构已完成（618 tests, ~22K 行）
- Component trait、LayoutTree、KeybindEngine、DialogManager、ThemeEngine 等基础设施就绪

---

## Work Objectives

### Must Have (P0+P1, 10 项)
Toast 通知 | 启动加载 | 会话 Fork | 修改文件面板 | 每消息菜单 | 多阶段权限 | Todo 面板 | 导出对话框 | 子Agent 导航 | Diff 文件计数

### Must NOT Have
后端架构变更 | 新 crate 依赖 | 破坏现有 API | 异步渲染

---

## Verification Strategy
- **Test**: TDD (RED → GREEN → REFACTOR)
- **QA**: 每任务含 Agent-Executed QA

---

## Execution Strategy

```
Phase 1 (P0, 3 tasks PARALLEL): Toast, StartupLoading, SessionFork
Phase 2 (P1, 7 tasks PARALLEL): FileChanges, MsgMenu, MultiPerm, TodoPanel, ExportDlg, SubAgentNav, DiffCounts
Phase 3 (P2, 7 tasks PARALLEL): ContextPanel, McpLspStatus, ShellMode, AgentsOverlay, WhichKeyGroups, ThinkingUnified, SearchHighlight
Phase 4 (P3, 1 task): AnsiFallback
FINAL: F1-F4 review gates
```

---

## TODOs

### Phase 1 — P0 关键缺失 (3 tasks, ALL parallel)

- [x] 1. Toast 通知系统 — 多类型、定位、自动消失

  **What to do (TDD)**:
  - RED: 写测试 `toast_shows_info_variant`, `toast_auto_dismisses`, `toast_stacks_multiple`
  - GREEN: 创建 `components/toast.rs`
    - `ToastVariant` enum: Info | Success | Warning | Error
    - `ToastManager` struct: `toasts: VecDeque<Toast>`, `push(variant, title, message, duration_ms)`, `tick()`
    - 渲染: 独立于 StatusBar 的右上角 overlay, 每种 variant 不同颜色边框
    - 定位: 从 area 右上角计算，最大宽度 min(60, area.width - 4)
  - REFACTOR: 确保与 DialogManager 叠层不冲突（Dialog 优先级高于 Toast）

  **Must NOT**: 不覆盖状态栏，不与 Dialog 同层

  **References**: opencode `ui/toast.tsx` (variant 枚举 + 自动消失 + 绝对定位)

  **Acceptance**: `cargo test -p cowd-cli -- tui::components::toast` → 4+ PASS. Toast 渲染在右上角.

  **QA**: push Info→渲染右上角蓝色边框消息→3s tick→消失. push Error→红色边框. 同时 push 2条→堆叠显示.
  **Evidence**: `.omo/evidence/task-p2-1-*.txt`

- [x] 2. 启动加载指示器 — 延迟显示 + 最少保持

  **What to do (TDD)**:
  - RED: `startup_shows_after_delay`, `startup_hides_when_ready`, `startup_min_display_3s`
  - GREEN: 在 `state.rs` 或 `main.rs run_tui_repl` 中
    - `StartupLoading` struct: `show_delay_ms: 500`, `min_display_ms: 3000`, `phase: Loading | Finishing | Done`
    - 渲染: 底部居中 overlay, zIndex 最高, 显示 spinner + "Loading..." / "Finishing startup..."
  - REFACTOR: 确保与 render_tree 不冲突

  **Must NOT**: 正常启动（<500ms）时不显示，不阻塞事件循环

  **References**: opencode `startup-loading.tsx` (延迟显示 + hold 机制)

  **Acceptance**: `cargo test -p cowd-cli -- tui::startup_loading` → 3+ PASS. 快速启动不显示, 慢速启动显示后保持 3s.

  **QA**: 模拟慢启动→500ms后出现 loading→ready后显示"Finishing..."→3s后消失
  **Evidence**: `.omo/evidence/task-p2-2-*.txt`

- [x] 3. 会话 Fork 对话框 — 从对话中分支新会话

  **What to do (TDD)**:
  - RED: `fork_dialog_lists_user_messages`, `fork_full_session_creates_new`, `fork_at_message_creates_branched`
  - GREEN: 扩展 `SessionSidebar` 或新增 `DialogKind::ForkSession`
    - 列出所有用户消息: 文本预览（截断80字符）+ 时间戳
    - 首项 "Full session"（完整复制）
    - j/k 导航, Enter 选择分支点
    - 调用 session fork API（如不存在则显示 "API not available" 提示）
  - REFACTOR: 与 DialogManager 集成

  **Must NOT**: 不修改现有 session resume 逻辑, 不自动切换会话

  **References**: opencode `dialog-fork-from-timeline.tsx` (消息列表 + fork API)

  **Acceptance**: `cargo test -p cowd-cli -- tui::components::session_sidebar` → 5+ PASS. Fork 对话框列出所有用户消息.

  **QA**: 打开 Fork 对话框→j/k 导航消息列表→Enter 选择→显示 "Forked session: xxx". Esc 取消.
  **Evidence**: `.omo/evidence/task-p2-3-*.txt`

### Phase 2 — P1 交互效率提升 (7 tasks, ALL parallel)

- [x] 4. 修改文件列表侧边栏 — "AI 改了什么" 面板

  **What to do (TDD)**:
  - RED: `file_changes_lists_modified_files`, `file_changes_shows_add_del_counts`, `file_changes_collapses_over_limit`
  - GREEN: 创建 `components/file_changes_panel.rs`
    - 从 session diff 数据渲染文件列表
    - 每行: `📄 src/main.rs  +12 -3`
    - 超过 8 项可折叠（▼/▶）
    - 在侧边栏 TabGroup 添加 "Files" 标签
  - REFACTOR: 数据源接口抽象（当前用 mock，后续接入真实 session diff）

  **Must NOT**: 不做文件系统扫描（数据来自已有 session 状态）

  **References**: opencode `sidebar/files.tsx` (+/- 计数 + 折叠)

  **Acceptance**: `cargo test` → 4+ PASS. 侧边栏显示修改文件列表含 +/- 计数.

  **QA**: 加载 session diff→侧边栏显示 5 个文件+计数→第 6 个折叠. j/k 导航. Enter 跳转到 DiffViewer.
  **Evidence**: `.omo/evidence/task-p2-4-*.txt`

- [x] 5. 每消息操作菜单 — revert/copy/fork 上下文操作

  **What to do (TDD)**:
  - RED: `message_menu_shows_on_focus`, `revert_action_sets_flag`, `copy_action_copies_text`
  - GREEN: 扩展 `ChatView` 或 TimelineEntry 渲染
    - 聚焦可折叠条目时，Enter 展开；光标在消息上时，`Ctrl+O` 打开操作菜单
    - 菜单: `DialogKind::Select` with options: "Copy text", "Fork from here", "Revert to here"
    - Copy 调用 osc52；Fork 打开 Fork 对话框；Revert 设置待确认标志
  - REFACTOR: 确保与现有 Enter 展开逻辑不冲突

  **Must NOT**: 不修改 TimelineEntry 枚举, 不改变默认 Enter 行为

  **References**: opencode `dialog-message.tsx` (菜单选项 + revert 流程)

  **Acceptance**: `cargo test` → 4+ PASS. Ctrl+O 打开消息菜单.

  **QA**: 聚焦消息→Ctrl+O→Select 菜单出现→选择 "Copy"→剪贴板收到文本. 选择 "Fork"→Fork 对话框打开.
  **Evidence**: `.omo/evidence/task-p2-5-*.txt`

- [x] 6. 多阶段权限 UI — Allow Once / Always / Reject with reason

  **What to do (TDD)**:
  - RED: `permission_shows_three_buttons`, `allow_always_persists`, `reject_with_reason_shows_input`
  - GREEN: 替换现有 `render.rs` 审批模态
    - 新增 `DialogKind::Permission` 或扩展现有 Confirm
    - 三个按钮: [A]llow Once / [L] Always / [R]eject
    - Reject 时弹出 Prompt 输入框: "Tell the AI what to do instead"
    - 根据工具类型显示不同预览（edit→diff, shell→command, web→URL）
  - REFACTOR: 类型化权限提示（edit 工具显示 diff 预览）

  **Must NOT**: 不改变现有审批回调协议（__approval_approved__ / __approval_denied__ 向后兼容）

  **References**: opencode `permission.tsx` (三按钮 + 类型化预览)

  **Acceptance**: `cargo test` → 5+ PASS. 权限对话框显示三个选项.

  **QA**: 触发审批→显示 Allow Once/Always/Reject→按 R→输入 "use ls -la instead"→提交. 按 A→后续同类操作自动允许.
  **Evidence**: `.omo/evidence/task-p2-6-*.txt`

- [x] 7. Todo 侧边栏面板 — AI 工作计划可视化

  **What to do (TDD)**:
  - RED: `todo_panel_lists_items`, `todo_items_show_status`, `todo_panel_hides_when_empty`
  - GREEN: 创建 `components/todo_panel.rs`
    - 从 Timeline 中提取 `ToolCall(name="TodoWrite")` 结果
    - 解析 JSON: `[{content, status, priority}]`
    - 渲染: `☐ pending` / `⏳ in_progress` / `✅ completed` + 内容文本
    - 超过 2 项可折叠
    - 在侧边栏 TabGroup 添加 "Todo" 标签
  - REFACTOR: Todo 数据提取器抽象

  **Must NOT**: 不修改 ToolCall 枚举, 不假定 TodoWrite JSON 格式（容错解析）

  **References**: opencode `sidebar/todo.tsx` + `component/todo-item.tsx`

  **Acceptance**: `cargo test` → 4+ PASS. Todo 面板显示 AI 创建的任务列表.

  **QA**: 模拟 Timeline 含 TodoWrite→侧边栏显示 3 个 todo 项→2 个完成 1 个进行中. 无 todo 时面板隐藏.
  **Evidence**: `.omo/evidence/task-p2-7-*.txt`

- [x] 8. 导出对话框 — 可配置选项导出

  **What to do (TDD)**:
  - RED: `export_dialog_shows_options`, `toggle_option_changes_state`, `confirm_returns_options`
  - GREEN: 创建 `components/export_dialog.rs`
    - `DialogKind::ExportOptions { filename, include_thinking, include_tools, include_metadata }`
    - 渲染: 文本框（文件名）+ [x]/[ ] 复选框 × 3
    - Tab 切换焦点, Space 切换开关
    - Enter 确认, Esc 取消
  - REFACTOR: 在 SessionSidebar 添加 "Export" 操作入口

  **Must NOT**: 不实现实际导出逻辑（调用现有 export 功能）

  **References**: opencode `dialog-export-options.tsx` (复选框 + Tab/Space 交互)

  **Acceptance**: `cargo test` → 4+ PASS. 导出对话框显示可配置选项.

  **QA**: 打开 Export→显示文件名+3个复选框→Tab 切换到 "Include thinking"→Space 关闭→Enter 确认→返回 options. Esc 取消.
  **Evidence**: `.omo/evidence/task-p2-8-*.txt`

- [x] 9. 子 Agent 会话导航 — 跳转到子会话

  **What to do (TDD)**:
  - RED: `subagent_nav_shows_on_task_message`, `open_subagent_navigates`
  - GREEN: 扩展 `ChatView` 消息渲染
    - ToolCall(name="task") 完成时，输出中包含 subagent_session_id 时
    - 消息旁显示 `[Open Subagent]` 标签或操作入口
    - 点击/Enter 跳转到子会话（调用 session.switch）
  - REFACTOR: 子会话 Footer: "Subagent 2 of 5 | Parent | Prev | Next"

  **Must NOT**: 不修改 ToolCall 结构, 子会话 ID 不存在时静默跳过

  **References**: opencode `dialog-subagent.tsx` + `subagent-footer.tsx`

  **Acceptance**: `cargo test` → 4+ PASS. 子 Agent 消息显示导航入口.

  **QA**: 模拟 task ToolCall 含 subagent_id→消息旁显示 "[Open Subagent]"→Enter→切换到子会话.
  **Evidence**: `.omo/evidence/task-p2-9-*.txt`

- [x] 10. Diff Viewer 文件修改计数 — 审查标记

  **What to do (TDD)**:
  - RED: `diff_file_tree_shows_add_del_counts`, `mark_reviewed_toggles_check`, `reviewed_files_show_dimmed`
  - GREEN: 扩展 `components/diff_viewer.rs` 的文件树
    - 每个文件名后显示 `[+N -M]` 计数
    - 新增 `mark_reviewed(file_path)` — 按 'm' 标记已审查
    - 已审查文件变灰 + `✅` 前缀
  - REFACTOR: 在文件树 hover/focus 状态上集成

  **Must NOT**: 不修改 diff 解析逻辑, 不影响 unified/split 切换

  **References**: opencode `diff-viewer-file-tree.tsx` (计数 + 审查标记)

  **Acceptance**: `cargo test` → 3+ PASS. 文件树显示 +/- 计数.

  **QA**: 加载 diff→文件树显示 "src/main.rs [+12 -3]"→按 m→变灰+✅. 再按 m→恢复.
  **Evidence**: `.omo/evidence/task-p2-10-*.txt`

### Phase 3 — P2 视觉/体验增强 (7 tasks, ALL parallel)

- [x] 11. 上下文用量面板 — Token/%/Cost 可视化

  **What to do (TDD)**:
  - RED: `context_panel_shows_token_count`, `context_panel_shows_percent`, `context_panel_updates_on_tick`
  - GREEN: 创建 `components/context_panel.rs`
    - 显示: tokens used / context window / % used / cost ($)
    - 从 App.token_count, App.context_window 获取数据
    - 进度条: `████████░░ 75%`
    - 在侧边栏 TabGroup 添加 "Context" 标签
  - REFACTOR: 数据格式化（K/M 单位, USD 两位小数）

  **References**: opencode `sidebar/context.tsx` (tokens + % + cost 三行)

  **Acceptance**: `cargo test` → 3+ PASS. 面板显示 token 用量.

  **QA**: 设置 token_count=80000, context_window=100000→显示 "80K/100K (80%)". tick 后自动刷新.
  **Evidence**: `.omo/evidence/task-p2-11-*.txt`

- [x] 12. MCP/LSP 状态指示器 — Footer 增强

  **What to do (TDD)**:
  - RED: `mcp_status_shows_connected_count`, `lsp_status_shows_server_count`, `status_shows_permission_pending`
  - GREEN: 扩展 `StatusBar` 的模块化段落
    - 新增 `StatusSectionKind::McpStatus`, `::LspStatus`, `::PermissionStatus`
    - MCP: `● MCP:3` (绿) / `⊙ MCP:1/3` (黄-部分失败) / `● MCP:0` (灰)
    - LSP: `● LSP:2` (绿)
    - Permissions: `△ 2 Permissions` (黄)
    - Footer 右侧对齐
  - REFACTOR: 数据从 App 或 TuiState 获取

  **References**: opencode `footer.tsx` (MCP/LSP/permission 状态行)

  **Acceptance**: `cargo test` → 3+ PASS. Footer 显示 MCP/LSP 状态.

  **QA**: MCP connected=3→显示 "● MCP:3". permission pending=2→显示 "△ 2 Permissions".
  **Evidence**: `.omo/evidence/task-p2-12-*.txt`

- [x] 13. Shell 模式 Prompt — `!` 前缀快速命令

  **What to do (TDD)**:
  - RED: `shell_mode_activates_on_exclamation`, `shell_mode_changes_border_color`, `enter_executes_not_submits`
  - GREEN: 扩展 `Prompt` 组件
    - 检测输入是否以 `!` 开头
    - 激活 Shell 模式: 边框颜色变为黄色, 状态显示 "SHELL"
    - Enter 执行命令（调用 bash 执行）, 输出追加到 Timeline
    - 非 `!` 开头时自动退出 Shell 模式
  - REFACTOR: 与 KeybindEngine 集成（Shell 模式下 Enter 映射到 ExecuteShell）

  **References**: opencode `prompt/traits.ts` (shell mode + traits.capture)

  **Acceptance**: `cargo test` → 4+ PASS. ! 触发 Shell 模式.

  **QA**: 输入 "!ls -la"→边框变黄→Enter→输出追加到 Timeline→模式自动退出. 输入非!内容→恢复正常.
  **Evidence**: `.omo/evidence/task-p2-13-*.txt`

- [x] 14. agentsOverlay — 子 Agent 树可视化

  **What to do (TDD)**:
  - RED: `agents_overlay_shows_tree`, `agent_status_updates_on_event`, `agent_can_be_interrupted`
  - GREEN: 创建 `components/agents_overlay.rs`
    - 从 Timeline 中提取所有 ToolCall(name="task") 条目
    - 渲染树状结构: `├── Build: running (12s)` / `└── Test: done (3s, exit:0)`
    - 实时更新状态（spinner 动画进行中）
    - 按 'x' 中断运行中的子 Agent
  - REFACTOR: 状态流转 running→done/error

  **References**: hermes-agent `agentsOverlay.tsx` (树状结构 + 实时状态 + 交互)

  **Acceptance**: `cargo test` → 4+ PASS. 覆盖层显示子 Agent 树.

  **QA**: 模拟 3 个 task 运行中→覆盖层显示树: 1 running, 2 done. running 项有 spinner. 按 x→中断.
  **Evidence**: `.omo/evidence/task-p2-14-*.txt`

- [x] 15. Which-Key 标签分组 — 分类导航

  **What to do (TDD)**:
  - RED: `whichkey_groups_show_tabs`, `tab_switches_group`, `bindings_filter_by_group`
  - GREEN: 扩展 `keybind/which_key.rs`
    - 为 KeyBinding 添加 `group: &'static str` 字段
    - 分组: "Session", "Navigation", "Files", "Dialog", "System"
    - 渲染: 顶部 Tab 栏（←→切换分组）, 下方当前分组绑定列表
    - Tab/Shift+Tab 切换分组
  - REFACTOR: 更新 default_bindings() 中的分组标注

  **References**: opencode `which-key.tsx` (标签分组 + Tab 切换)

  **Acceptance**: `cargo test` → 4+ PASS. Which-Key 显示分组标签.

  **QA**: Space→Which-Key 显示 "Session | Navigation | Files | Dialog | System" Tab 栏. Tab 切换到 "Files"→显示文件相关快捷键.
  **Evidence**: `.omo/evidence/task-p2-15-*.txt`

- [x] 16. Thinking 统一面板 — spinner+reasoning+tools 一体化

  **What to do (TDD)**:
  - RED: `unified_thinking_shows_spinner`, `reasoning_appears_inline`, `tool_progress_in_same_panel`
  - GREEN: 创建 `components/thinking_panel.rs` 或扩展 ChatView
    - 当前 Turn 进行中时，用一个统一面板替换分散的 Thinking + ToolCall 条目
    - 面板内部分区: [spinner] [reasoning 流] [工具进度列表]
    - 统一背景色 + 边框
    - Turn 完成后折叠为摘要行
  - REFACTOR: 确保与增量重建兼容

  **References**: hermes-agent `thinking.tsx` (1202 行，统一面板设计)

  **Acceptance**: `cargo test` → 4+ PASS. Thinking 面板统一显示.

  **QA**: Turn 开始→一个面板显示 spinner + "Analyzing..."→工具调用追加到面板内→Turn 完成→面板折叠.
  **Evidence**: `.omo/evidence/task-p2-16-*.txt`

- [x] 17. 搜索高亮 — 屏幕缓冲区匹配

  **What to do (TDD)**:
  - RED: `search_highlight_inverts_matching_cells`, `highlight_handles_cjk_width`, `next_match_cycles`
  - GREEN: 扩展搜索功能
    - 在渲染循环后扫描已渲染行，查找匹配文本
    - 匹配字符反转前景/背景色（inverse video）
    - 正确处理双宽字符（CJK/emoji）
    - 当前匹配用黄色高亮（区别于其他匹配的反转色）
  - REFACTOR: 与现有 `/` 搜索系统集成

  **References**: hermes-agent `searchHighlight.ts` (后渲染缓冲区搜索 + 反转)

  **Acceptance**: `cargo test` → 4+ PASS. 搜索匹配项反色高亮.

  **QA**: /search "error"→匹配行中 "error" 反色显示, 当前匹配黄色. n/N 切换.
  **Evidence**: `.omo/evidence/task-p2-17-*.txt`

### Phase 4 — P3 终端兼容 (1 task)

- [x] 18. ANSI 颜色降级 — 非 truecolor 终端兼容

  **What to do (TDD)**:
  - RED: `ansi_fallback_detects_no_truecolor`, `ansi_fallback_chooses_best_8bit`, `truecolor_passthrough`
  - GREEN: 扩展 `theme/palette.rs` 或新增 `theme/ansi_fallback.rs`
    - 检测 `COLORTERM` 环境变量（truecolor/24bit 则直通）
    - 非 truecolor 时: RGB → 8-bit ANSI（256色调色板中最接近的颜色）
    - 颜色距离: 欧几里得距离 (ΔR²+ΔG²+ΔB²) 最小
    - 缓存已计算的颜色映射
  - REFACTOR: 与 ThemeEngine 集成

  **References**: hermes-agent `theme.ts` (ANSI normalization 函数)

  **Acceptance**: `cargo test` → 3+ PASS. 无 truecolor 时降级为 8-bit.

  **QA**: COLORTERM=truecolor→RGB 直通. unset COLORTERM→#FF8800 降级为 color202.
  **Evidence**: `.omo/evidence/task-p2-18-*.txt`

---

### Phase FINAL — 验证门 (F1-F4, ALL parallel)

- [x] F1. **Plan Compliance Audit** — `oracle`
  Verify all 18 tasks match plan. Must Have 10/10 (P0+P1). Must NOT Have: no backend changes, no new crates.
  Output: `Must Have [10/10] | Must NOT Have [3/3] | Tasks [18/18] | VERDICT: APPROVE`

- [x] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo test -p cowd-cli` + `cargo clippy`. Review new code for AI slop, dead code, anti-patterns.
  Output: `Build [PASS] | Tests [710/712] (2 pre-existing) | VERDICT: APPROVE`

- [x] F3. **Real Manual QA** — `unspecified-high`
  Execute ALL QA scenarios. Test cross-phase integration (Toast + Dialog + Loading 不冲突).
  Output: `Scenarios [18/18] | Integration [18/18] | VERDICT: APPROVE`

- [x] F4. **Scope Fidelity Check** — `deep`
  Verify 1:1 — each task's "What to do" matches actual diff. No scope creep. No contamination.
  Output: `Tasks [18/18] | Contamination [CLEAN] | VERDICT: APPROVE`

---

## Commit Strategy

- Phase 1 (Tasks 1-3): `feat(tui): toast notifications, startup loading, session fork`
- Phase 2 (Tasks 4-10): per-task commits (7 commits)
- Phase 3 (Tasks 11-17): per-task commits (7 commits)
- Phase 4 (Task 18): `feat(tui): ANSI color fallback for non-truecolor terminals`

## Success Criteria

```bash
cargo test -p cowd-cli -- tui    # ALL new tests pass
cowd --tui                        # Launch with all enhancements
```
- [x] 18 tasks completed
- [x] All P0+P1 (10 items) functional
- [x] Zero breaking changes
- [x] F1-F4 all APPROVE
