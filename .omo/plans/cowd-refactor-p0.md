# COWD v0.5 → v0.6: 技术债务清偿 + 架构边界修复

## TL;DR

> **Quick Summary**: cowd v0.5 完成了特性丰富的功能面（36 模块记忆、5 层存储、tree-sitter 代码索引、TUI、多平台适配），但代码质量基础设施滞后——存在 61 个未使用导入、2 组未完成迁移（SqliteSessionStore→UnifiedSessionStore 6 个调用点，run_turn→run_turn_async 18 个调用点）、内存-运行时边界模糊、平台适配器仅为骨架级别。本计划在 v0.6 阶段彻底清偿这些债务，建立代码质量门禁，然后释放架构到下一阶段演进。

> **Deliverables**:
> - 零废弃代码/未使用导入/未使用字段的代码库
> - 统一的会话存储层（UnifiedSessionStore 替代 SqliteSessionStore）
> - 异步运行时调用链（run_turn_async 替代 run_turn）
> - 清晰的 crate 边界（cowd-cli → runtime → memory，禁止跨层调用）
> - CI 质量门禁（#![deny(deprecated, unused_imports)]）
> - 平台适配器成熟度提升（Feishu 字段连接、ProviderChain 死代码清理）

> **Estimated Effort**: Medium (2-3 个工作阶段)
> **Parallel Execution**: YES - 7 waves
> **Critical Path**: Wave 1 (基础设施) → Wave 2-3 (迁移核心) → Wave 4-5 (边界修复) → Wave 6-7 (门禁+验证)

---

## Context

### 原始问题

用户要求：分析所有编译警告、废弃代码、未使用导入，总结整体问题，深层分析设计与项目演化的核心问题，给出完整实施建议和 TDD 执行计划。

### 分析摘要

三个背景 agent 联合分析：

| Agent | 焦点 | 关键发现 |
|-------|------|----------|
| **Explore #1** | 废弃代码审计 | SqliteSessionStore 6 处、run_turn 18 处、TextArea::widget 1 处、UnifiedSessionStore 根本不存在 |
| **Explore #2** | 未使用代码审计 | 61 个未使用导入、5 个未使用字段/变量、1 个完全死掉的 trait(FreshContextExt)、3 个死方法 |
| **Oracle** | 架构深度分析 | 未完成迁移文化、TUI 代码审查缺口、内存-运行时边界危机、平台适配器骨架状态、无强制门禁 |

### Oracle 核心诊断

> **v0.5 的代码库读起来像 v0.3 的架构状态。** 特性面令人印象深刻，但完成深度浅。定义性模式是"搭骨架然后走人"：启动迁移、添加字段预判未来、建模平台 schema，然后在将它们全部连接之前就发布了。

> **最高杠杆行动：** 完成会话存储统一化。它解除了内存-运行时边界清理的阻塞，进而解锁 Gates → 蜂群 → 平台。

---

## Work Objectives

### Core Objective

清偿 v0.5 积累的技术债务，建立代码质量门禁，为 v0.6+ 的 Memory 2.0 / Swarm 1.0 / Gates 2.0 演进奠定稳固架构基础。

### Concrete Deliverables

1. **零警告代码库** — 所有废弃 API 迁移完成，所有未使用导入/字段/变量移除，所有死方法/死 trait 清理
2. **统一会话存储** — UnifiedSessionStore 实现并替换所有 SqliteSessionStore 调用点
3. **异步运行时** — run_turn_async 替换所有 run_turn 调用点
4. **Crate 边界强化** — 消除 cowd-cli 对 memory 的直接类型依赖，通过 runtime 代理
5. **质量门禁** — `#![deny(deprecated)]` + `#![deny(unused_imports)]` 在关键 crate 启用
6. **平台适配器补完** — Feishu 死字段连接、ProviderChain 死代码清理

### Definition of Done

- [ ] `cargo build --release` 零警告
- [ ] `#![deny(deprecated)]` 在 memory/runtime/cowd-cli 启用后编译通过
- [ ] `cargo test --workspace` 全部通过
- [ ] UnifiedSessionStore 存在且被使用，SqliteSessionStore 标记移除
- [ ] run_turn_async 是唯一的运行时入口

### Must Have

- 所有发现的问题必须被追踪到（见下方完整目录）
- 每个修改必须有测试通过
- Crate 边界必须保持：cowd-cli → runtime → memory

### Must NOT Have (Guardrails)

- 不要在此次 refactor 中添加新特性（纯粹清理）
- 不要删除任何非废弃的公共 API（除非明确标记为内部）
- 不要修改 imap crate 版本（外部依赖，单独处理）
- 不要重写 Feishu/WeCom 适配器逻辑——只连接死字段

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** - ALL verification is agent-executed.

### Test Decision

- **Infrastructure exists**: YES
- **Automated tests**: Tests-after (清理后验证测试仍然通过)
- **Framework**: cargo test (native)

### QA Policy

Every task MUST include agent-executed verification:
- `cargo build --release` clean build
- `cargo test --workspace` all pass
- Codebase search for removed patterns (e.g., grep for SqliteSessionStore after removal)

---

## 完整问题目录

### Category A: 废弃代码迁移（17 个调用点 + 1 个定义）

| ID | 位置 | 行 | 问题 | 替换 |
|----|------|---|------|------|
| A1 | `crates/memory/src/store/session.rs` | 203-205 | SqliteSessionStore struct 定义废弃 | UnifiedSessionStore（尚未实现） |
| A2 | `crates/memory/src/lib.rs` | 72 | 公共重新导出 SqliteSessionStore | 移除 |
| A3 | `crates/cowd-cli/src/server/mod.rs` | 42 | 导入 SqliteSessionStore | 迁移到 UnifiedSessionStore |
| A4 | `crates/cowd-cli/src/server/mod.rs` | 312 | 字段类型 SqliteSessionStore | 迁移到 UnifiedSessionStore |
| A5 | `crates/cowd-cli/src/server/mod.rs` | 399 | 构造 SqliteSessionStore | 迁移到 UnifiedSessionStore |
| A6 | `crates/runtime/src/conversation.rs` | 924 | run_turn 定义废弃 | run_turn_async |
| A7 | `crates/cowd-cli/src/server/mod.rs` | 1463 | run_turn 调用 (SSE) | run_turn_async |
| A8 | `crates/cowd-cli/src/server/mod.rs` | 1613 | run_turn 调用 (non-streaming) | run_turn_async |
| A9 | `crates/cowd-cli/src/server/mod.rs` | 3031 | run_turn 调用 (summarize) | run_turn_async |
| A10 | `crates/cowd-cli/src/server/mod.rs` | 4049 | run_turn 调用 (doctor) | run_turn_async |
| A11 | `crates/cowd-cli/src/main.rs` | 332 | 间接 run_turn (run_turn_with_output) | run_turn_async |
| A12 | `crates/cowd-cli/src/main.rs` | 2822 | run_turn 调用 (TUI) | run_turn_async |
| A13 | `crates/cowd-cli/src/main.rs` | 3559/3607/3620 | run_turn 调用 (prompt 模式) | run_turn_async |
| A14 | `crates/cowd-cli/src/main.rs` | 3765/3853-3855/3874 | run_turn 调用 (slash commands) | run_turn_async |
| A15 | `crates/cowd-cli/src/main.rs` | 4482 | run_turn 调用 (internal_prompt) | run_turn_async |
| A16 | `crates/runtime/src/subagent_executor.rs` | 14 | run_turn 调用 | run_turn_async |
| A17 | `crates/tools/src/executor.rs` | 2584 | run_turn 调用 (run_agent_job) | run_turn_async |
| A18 | `crates/cowd-cli/src/tui/components/prompt.rs` | 934 | TextArea::widget() 废弃 | `&self.textarea` |

### Category B: 未使用导入（61 处，按 crate 分簇）

| ID | 位置 | 行 | 详情 |
|----|------|---|------|
| B1 | `crates/memory/src/layers/deep.rs` | 26 | MemoryMeta 从未引用 |
| B2 | `crates/memory/src/layers/project.rs` | 20 | walkdir::WalkDir 被完全限定路径替代 |
| B3 | `crates/memory/src/store/sqlite.rs` | 24 | SymbolEdgeType 仅在 #[cfg(test)] 中使用 |
| B4 | `crates/tools/src/executor.rs` | 400 | 重复 import std::path::Path（已导入于第 2 行） |
| B5-43 | `crates/cowd-cli/src/tui/*` | 多处 | 39 个未使用导入，散布在 TUI 组件中 |

**TUI 中的关键未使用导入（代表性示例，非穷举）：**

| 文件 | 未使用导入 |
|------|-----------|
| `tui/layout/engine.rs:16` | Component, EventResult, RenderContext |
| `tui/layout/mod.rs:5-6` | PanelDef, SplitDirection, Split, TabDef, TabGroup, RATIO_DEFAULT, RATIO_MAX, RATIO_MIN |
| `tui/components/agents_overlay.rs:5` | KeyEvent |
| `tui/components/file_changes_panel.rs:12` | Span |
| `tui/components/prompt.rs:24` | Wrap |
| `tui/components/todo_panel.rs:14,18` | KeyEvent, Span |
| `tui/input.rs:3` | MouseButton |
| `tui/keybind/types.rs:1` | KeyCode, KeyModifiers |
| `tui/keybind/mod.rs:8` | WhichKey |
| `tui/state.rs:52` | LayoutNode |
| `tui/theme/mod.rs:13` | clear_cache, detect_truecolor, rgb_to_ansi8 |
| `tui/theme/palette.rs:8` | serde::de::self |
| `tui/accessibility.rs:15` | ThemeLoader |
| `tui/mod.rs:29` | ComponentId, Component, EventResult, RenderContext |
| `tui/md_renderer.rs` | 多处未使用样式/颜色导入 |
| `tui/state.rs:1013` | KeyModifiers |
| `tui/state.rs:1384-1385` | Constraint, Direction, Layout, Stylize |
| `crates/cowd-cli/src/mcp_serve.rs:1` | std::env |

### Category C: 未使用变量/字段/方法（7 处）

| ID | 位置 | 行 | 问题 | 修复 |
|----|------|---|------|------|
| C1 | `crates/memory/src/cognitive.rs` | 164 | workspace_root 字段设置但从未读取 | 移除字段定义+赋值 |
| C2 | `crates/api/src/provider_chain.rs` | 199 | round_robin_index 字段（仅被死方法使用） | 移除或连接 |
| C3 | `crates/api/src/provider_chain.rs` | 235 | select_providers() 私有方法从未被调用 | 移除 |
| C4 | `crates/api/src/provider_chain.rs` | 269 | next_round_robin() 私有方法从未被调用 | 移除 |
| C5 | `crates/runtime/src/sandbox.rs` | 213 | cwd 参数在函数体中从未读取 | 移除参数 |
| C6 | `crates/runtime/src/conversation.rs` | 854 | prompt 上未使用的 mut | 移除 mut |
| C7 | `crates/memory/src/fresh_context.rs` | 351-362 | 整个 FreshContextExt trait 文件外完全未使用 + async fn 无 #[async_trait] | 移除 |

### Category D: 外部依赖问题（2 处）

| ID | 位置 | 问题 | 影响 |
|----|------|------|------|
| D1 | `Cargo.lock` (imap-proto v0.10.2) | 未来 Rust 版本将拒绝此 crate | 当前非阻塞，需监控 |
| D2 | `crates/runtime/Cargo.toml` | imap crate 作为 email 平台依赖 | 版本升级到 imap v3+ 可解决 |

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (基础准备 — 立即可开始):
├── 1. UnifiedSessionStore 实现 + SQLite 存储统一
├── 2. 修复 TextArea::widget() 废弃调用
├── 3. 修复 MemoryMeta 未使用导入 (deep.rs)
├── 4. 修复 WalkDir 未使用导入 (project.rs)
├── 5. 修复 Path 重复导入 (executor.rs)
├── 6. 修复 SymbolEdgeType 仅测试导入 (sqlite.rs)
└── 7. 修复未使用 mut (conversation.rs)

Wave 2 (废弃迁移 — 核心工作量):
├── 8. SqliteSessionStore 调用点迁移 (server/mod.rs 3处 + lib.rs)
├── 9. 移除废弃的 SqliteSessionStore struct 定义

Wave 3 (run_turn 异步化 — Server 路径):
├── 10. server/mod.rs run_turn → run_turn_async (4处: SSE/stream/summarize/doctor)
├── 11. subagent_executor.rs run_turn → run_turn_async
├── 12. tools/executor.rs run_turn → run_turn_async

Wave 4 (run_turn 异步化 — CLI 路径):
├── 13. main.rs LiveCli run_turn (5处: prompt/TUI/slash/internals)
└── 14. main.rs 间接路径 (run_turn_with_output, 3处 pipeline/subagent)

Wave 5 (边界修复 + 死代码清理):
├── 15. 移除 CognitiveContextManager.workspace_root 死字段
├── 16. 移除 FreshContextExt 死 trait
├── 17. ProviderChain 死代码清理 (select_providers, next_round_robin, round_robin_index)
├── 18. 修复 sandbox.rs 未使用 cwd 参数
└── 19. TUI 39 个未使用导入批量清理

Wave 6 (质量门禁 + 最终检查):
├── 20. 在关键 crate 启用 #![deny(deprecated)] 和 #![deny(unused_imports)]
└── 21. 全 workspace 零警告验证

Wave FINAL (4 并行审核 + 用户确认):
├── F1. 计划合规审计 (oracle)
├── F2. 代码质量审查 (unspecified-high)
├── F3. 集成测试运行验证 (unspecified-high)
└── F4. 范围正确性检查 (deep)

Critical Path: 1 → 8 → 9 → 10-12 → 13-14 → 15-19 → 20-21 → F1-F4
```

---

## TODOs

<!-- Task 1-7: Wave 1 — 基础准备 -->
- [ ] 1. 实现 UnifiedSessionStore + 统一 SqliteSessionStore 接口

  **What to do**:
  - 在 `crates/memory/src/session_store.rs` 创建新模块
  - 定义 `UnifiedSessionStore` struct，包装现有 SqliteSessionStore 的底层实现
  - 接口与现有调用兼容，但更清晰的 API 边界
  - 更新 `crates/memory/src/lib.rs` 导出
  - 将 `SqliteSessionStore` 的内部实现委托给 `UnifiedSessionStore`
  - 为 `SqliteSessionStore` 的 `db_path` 字段提供迁移路径

  **Must NOT do**:
  - 不要修改底层 SQLite schema
  - 不要重写已工作的 session 持久化逻辑

  **Reference**:
  - `crates/memory/src/store/session.rs:203-260` — 现有 SqliteSessionStore 实现
  - `crates/cowd-cli/src/server/mod.rs:42,312,399` — 三个需要迁移的调用点

  **Acceptance Criteria**:
  - [ ] UnifiedSessionStore struct 存在且公开可访问
  - [ ] `cargo build` 通过
  - [ ] SqliteSessionStore 仍然可用但标记为 deprecated

  **QA Scenarios**:
  ```
  Scenario: UnifiedSessionStore 创建和使用
    Tool: Bash
    Steps:
      1. 确认 `crates/memory/src/session_store.rs` 存在
      2. 确认 `pub use session_store::UnifiedSessionStore` 在 lib.rs 中
      3. 运行 `cargo test -p cowd-memory` 全部通过
    Evidence: .omo/evidence/task-1-session-store.{ext}
  ```

- [ ] 2. 修复 TextArea::widget() 废弃调用

  **What to do**:
  - 将 `crates/cowd-cli/src/tui/components/prompt.rs:934` 的 `self.textarea.widget()` 改为 `&self.textarea`

  **Reference**:
  - `crates/cowd-cli/src/tui/components/prompt.rs:934`

  **Acceptance Criteria**:
  - [ ] 修改后无 `use of deprecated method` 警告
  - [ ] TUI 编译通过

  **QA Scenarios**:
  ```
  Scenario: 检查废弃警告消失
    Tool: Bash
    Steps:
      1. grep 确认 "textbox.widget" 不再存在
      2. cargo build --release -p cowd-cli 2>&1 | grep "widget" → 无匹配
    Evidence: .omo/evidence/task-2-textarea.{ext}
  ```

- [ ] 3. 修复 deep.rs 未使用导入 (MemoryMeta)

  **What to do**:
  - 从 `crates/memory/src/layers/deep.rs:26` import 行移除 `MemoryMeta`

  **Reference**:
  - `crates/memory/src/layers/deep.rs:26`

  **Acceptance Criteria**:
  - [ ] grep "MemoryMeta" deep.rs 不再在 import 行出现
  - [ ] `cargo test -p cowd-memory` 通过

- [ ] 4. 修复 project.rs 未使用导入 (WalkDir)

  **What to do**:
  - 移除 `crates/memory/src/layers/project.rs:20` 的 `use walkdir::WalkDir;`
  - 保留完全限定路径 `walkdir::WalkDir::new` 调用

  **Reference**:
  - `crates/memory/src/layers/project.rs:20,200`

  **Acceptance Criteria**:
  - [ ] `cargo build --release -p cowd-memory` 无 WalkDir 警告

- [ ] 5. 修复 sqlite.rs 未使用导入 (SymbolEdgeType)

  **What to do**:
  - 将 `crates/memory/src/store/sqlite.rs:24` 的 `SymbolEdgeType` 移动到 `#[cfg(test)]` 块内
  - 确认非测试代码不需要此类型

  **Acceptance Criteria**:
  - [ ] `cargo build --release -p cowd-memory` 无 SymbolEdgeType 警告

- [ ] 6. 修复 executor.rs 重复导入 + sandbox.rs 未使用参数

  **What to do**:
  - B4: `executor.rs:400` — 移除重复的 `use std::path::Path;`
  - C5: `sandbox.rs:213` — 将 `cwd: &Path` 参数标记为 `_cwd` 或移除

  **Acceptance Criteria**:
  - [ ] `cargo build --release` 减 2 个警告

- [ ] 7. 修复 conversation.rs 未使用 mut + 其他小型清理

  **What to do**:
  - C6: 将 `conversation.rs:854` 的 `mut prompt` 改为 `prompt`

  **Acceptance Criteria**:
  - [ ] `cargo build --release` 减 1 个警告

- [ ] 8. SqliteSessionStore 调用点迁移

  **What to do**:
  - 将 `server/mod.rs:42` 从 `SqliteSessionStore` 导入改为 `UnifiedSessionStore`
  - 将 `server/mod.rs:312` 字段类型从 `Arc<SqliteSessionStore>` 改为 `Arc<UnifiedSessionStore>`
  - 将 `server/mod.rs:399` 构造从 `SqliteSessionStore::open(...)` 改为 `UnifiedSessionStore::open(...)`
  - 将 `lib.rs:72` 导出从 `SqliteSessionStore` 改为 `UnifiedSessionStore`
  - 将所有测试依赖更新为 UnifiedSessionStore

  **Reference**:
  - Task 1 的输出 — UnifiedSessionStore 实现

  **Acceptance Criteria**:
  - [ ] 所有 4 个生产调用点迁移完成
  - [ ] `cargo test --workspace` 全部通过

  **QA Scenarios**:
  ```
  Scenario: 无 SqliteSessionStore 生产引用
    Tool: Bash
    Steps:
      1. grep -r "SqliteSessionStore" crates/cowd-cli/src/ → 无匹配（排除测试）
      2. grep "SqliteSessionStore" crates/memory/src/lib.rs → 无匹配
      3. cargo build --release 2>&1 | grep "SqliteSessionStore" → 无匹配
    Evidence: .omo/evidence/task-8-migration.{ext}
  ```

- [ ] 9. 移除废弃的 SqliteSessionStore struct

  **What to do**:
  - 在 `store/session.rs` 中将 `SqliteSessionStore` 标记为 `#[deprecated]` → 添加 `#[allow(deprecated)]` 在桥接代码 → 最终移除 struct 定义和 impl 块
  - 保留兼容别名（可选）

  **Acceptance Criteria**:
  - [ ] SqliteSessionStore struct 定义移除
  - [ ] `cargo build --release` 通过

- [ ] 10. server/mod.rs run_turn → run_turn_async

  **What to do**:
  - 4 个调用点: SSE streaming (1463), non-streaming (1613), summarize (3031), doctor (4049)
  - 这些都在 Server 的 async 上下文中，可以直接替换为 `runtime.run_turn_async(...).await`
  - `run_turn_async()` 返回 `Pin<Box<dyn Stream>>` — 需要修改调用模式

  **Reference**:
  - `crates/runtime/src/conversation.rs:602` — run_turn_async 定义
  - `crates/cowd-cli/src/server/mod.rs:1463,1613,3031,4049`

  **Acceptance Criteria**:
  - [ ] 4 个调用点全部迁移
  - [ ] `cargo test -p cowd-cli` 通过

- [ ] 11. subagent_executor.rs run_turn → run_turn_async

  **What to do**:
  - `subagent_executor.rs:14` — 此函数是同步的（`SubAgentExecutor::execute_sync`）
  - 需要将调用包装在 `tokio::runtime::Handle::current().block_on(...)` 中
  - 或者将 `execute_sync` 改为 `async`（如果调用栈允许）

  **Reference**:
  - `crates/runtime/src/subagent_executor.rs:14`
  - `crates/tools/src/executor.rs:2584`（同样是同步上下文中的 run_turn）

- [ ] 12. tools/executor.rs run_turn → run_turn_async

  **What to do**:
  - `executor.rs:2584` — 同步上下文中的 run_turn
  - 与 task 11 相同策略：block_on 包装或调用改为 async

- [ ] 13. main.rs LiveCli run_turn → run_turn_async (直接路径)

  **What to do**:
  - 5 个直接 run_turn 调用点: 2822 (TUI), 3559 (run_turn 实现), 3607 (compact), 3620 (json), 4482 (internal)
  - TUI 路径 (2822) 在 `spawn_blocking` 中 — 需要重构
  - Prompt 路径 (3559/3607/3620) 在同步函数中 — block_on 包装

- [ ] 14. main.rs 间接路径迁移

  **What to do**:
  - 332 (run_turn_with_output) — 顶层 CLI 入口
  - 3765/3853-3855/3874 — slash command 路径

- [ ] 15. 移除 CognitiveContextManager.workspace_root 死字段

  **What to do**:
  - 移除 `cognitive.rs:164` 的 `workspace_root: Option<PathBuf>` 字段
  - 移除 `cognitive.rs:315` 的 `workspace_root: ws_root` 赋值
  - 保留 `workspace_root` 参数的传递（`MemoryOrchestrator` 和 `StateRebuilder` 仍然需要）

  **Reference**:
  - `crates/memory/src/cognitive.rs:164,315`

  **Acceptance Criteria**:
  - [ ] `self.workspace_root` 在 cognitive.rs 中不存在
  - [ ] `cargo test -p cowd-memory` 通过

  **QA Scenarios**:
  ```
  Scenario: 死字段已移除
    Tool: Bash
    Steps:
      1. grep -n "self\.workspace_root" crates/memory/src/cognitive.rs → 无匹配
      2. cargo test -p cowd-memory
    Evidence: .omo/evidence/task-15-workspace-root.{ext}
  ```

- [ ] 16. 移除 FreshContextExt 死 trait

  **What to do**:
  - 移除 `fresh_context.rs:351-362` 的整个 `FreshContextExt` trait 定义
  - 保留 `prepare_fresh_context` 作为普通函数（如果被引用）或完全移除
  - 确认 trait 在文件外确实未被引用

  **Reference**:
  - `crates/memory/src/fresh_context.rs:351-362`

  **Acceptance Criteria**:
  - [ ] FreshContextExt trait 不存在
  - [ ] `cargo test -p cowd-memory` 通过

- [ ] 17. ProviderChain 死代码清理

  **What to do**:
  - 移除 `provider_chain.rs:199` 的 `round_robin_index` 字段
  - 移除 `provider_chain.rs:235` 的 `select_providers()` 方法
  - 移除 `provider_chain.rs:269` 的 `next_round_robin()` 方法
  - 如果 ProviderChain 因此变得只有简单逻辑，考虑将其内联到调用方

  **Reference**:
  - `crates/api/src/provider_chain.rs:199,235,269`

- [ ] 18. 修复 sandbox.rs 未使用 cwd 参数

  **What to do**:
  - 将 `build_linux_sandbox_command(command: &str, cwd: &Path, ...)` 的 `cwd` 参数标记为弃用
  - 或从签名中移除（检查所有调用方）

- [ ] 19. TUI 39 个未使用导入批量清理

  **What to do**:
  - 逐个检查 TUI 文件中的未使用导入并移除
  - 关键文件列表见 Category B 表
  - 使用 `cargo fix --bin cowd -p cowd-cli --allow-dirty` 自动修复大部分
  - 然后手动检查剩余警告

  **Reference**:
  - 见上方 Category B 完整列表

- [ ] 20. 质量门禁: #![deny(deprecated)] 和 #![deny(unused_imports)]

  **What to do**:
  - 在 `crates/memory/src/lib.rs` 顶部添加 `#![deny(deprecated)]`
  - 在 `crates/runtime/src/lib.rs` 顶部添加 `#![deny(deprecated)]`
  - 在 `crates/cowd-cli/src/main.rs` 顶部添加 `#![deny(deprecated, unused_imports)]`
  - 运行 `cargo build --release` 确认零警告

  **Must NOT do**:
  - 不要在 workspace 级别设置 `#![deny(...)]` — 先在每个 crate 级别验证

  **Reference**:
  - Oracle 分析：缺乏强制是代码累积的核心原因

- [ ] 21. 全 workspace 零警告验证

  **What to do**:
  - 运行 `cargo build --release` 并捕获所有输出
  - 确认零 `warning:` 行
  - 运行 `cargo test --workspace` 全部通过

---

## Final Verification Wave

- [ ] F1. **Plan Compliance Audit** — 检查每个 TODO 是否按计划执行

- [ ] F2. **Code Quality Review** — 代码质量 + 边界检查

- [ ] F3. **Real Manual QA** — 全量测试通过 + 构建零警告

- [ ] F4. **Scope Fidelity Check** — 无超出范围的修改，无新特性引入

---

## Commit Strategy

- **1-7 (Wave 1)**: `refactor: clean unused imports and deprecated API calls` — 批量
- **8-9 (Session store)**: `refactor: migrate SqliteSessionStore to UnifiedSessionStore`
- **10-14 (run_turn async)**: `refactor: migrate run_turn to run_turn_async (server/cli/subagent/tools)`
- **15-19 (Dead code)**: `refactor: remove dead fields, traits, and TUI unused imports`
- **20-21 (Quality gates)**: `ci: enable #![deny(deprecated, unused_imports)]`

---

## Success Criteria

### Verification Commands
```bash
cargo build --release 2>&1 | grep -c "^warning"  # Expected: 0
cargo test --workspace 2>&1 | tail -5             # Expected: "test result: ok"
grep -r "SqliteSessionStore" crates/ --include="*.rs" | grep -v "#\[allow" | grep -v "test"  # Expected: 0
grep -r "run_turn(" crates/ --include="*.rs" | grep -v "run_turn_async" | grep -v "test" | grep -v "#\[allow"  # Expected: 0
grep -r "FreshContextExt" crates/ --include="*.rs"  # Expected: 0
```
