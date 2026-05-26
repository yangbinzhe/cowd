# TUI Memory & Architecture Enhancement Plan (TDD Mode)

## TL;DR

> **目标版本**: v0.7.0 — TUI 记忆可视化 + 会话恢复 + 侧边栏功能实现
> **核心交付**: 修复架构审计发现的 3 个关键断裂：TUI 记忆面板空白、跨会话记忆不可见、侧边栏 90% 占位符
> **并行执行**: YES — 4 Waves, 最大并行度 5

---

## 审计与设计

### 根因分析
1. **TUI 记忆不可见**: `TuiEvent` 无记忆事件类型 → 运行时记忆数据无法流向 UI
2. **侧边栏占位符**: 9/10 标签页是 `PlaceholderComponent`，从未实现
3. **会话恢复未接线**: `session_resume.rs` BM25 算法存在但仅用于测试

### 修复策略
1. **事件层**: 新增 `TuiEvent::MemoryUpdate/MemoryEntry` → 打通数据流
2. **组件层**: 实现记忆浏览组件 → 替换 PlaceholderComponent
3. **集成层**: 接线 `session_resume` + 记忆事件发射 → 跨会话生效

---

## 执行计划 (4 Waves)

### Wave 1 (基础 - 5 并行)
- T1: `TuiEvent::MemoryUpdate/MemoryEntry` 事件类型定义 [events.rs]
- T2: 记忆浏览组件 MemoryPanel [components/memory_panel.rs]
- T3: runtime 记忆事件发射 [conversation.rs + main.rs]
- T4: 会话恢复接线 session_resume [main.rs startup]
- T5: ProviderChain 删除 + SessionManager 清理 [api/lib.rs + runtime]

### Wave 2 (集成 - 3 并行, 依赖 T1)
- T6: TUI 事件接收记忆数据 [app.rs apply_event]
- T7: 替换 PlaceholderComponent → MemoryPanel [defaults.rs + render.rs]
- T8: 命令行记忆浏览 /memory 命令增强

### Wave 3 (增强 - 3 并行)
- T9: 文件树组件 FileTree [components/file_tree.rs]
- T10: 上下文面板实时更新
- T11: SubAgentExecutor 清理

### Wave 4 (验证)
- T12: 全量构建 + 测试 + TUI QA + 崩溃日志

---

## TODOs

- [ ] 1. **TuiEvent 记忆事件类型定义**

  **What**: 在 `crates/cowd-cli/src/tui/events.rs` 的 `TuiEvent` 枚举中添加:
  ```rust
  MemoryEntry { layer: String, content: String, relevance: f64 },
  MemoryUpdate { entries: Vec<(String, String, f64)>, status: String },
  MemoryStats { total_entries: usize, vector_count: usize, layers: Vec<String> },
  ```

  **Verify**: `cargo build --release` 0 errors

- [ ] 2. **MemoryPanel 组件实现**

  **What**: 创建 `crates/cowd-cli/src/tui/components/memory_panel.rs`:
  - 从 `app.memory_entries` 读取数据
  - 支持滚动浏览
  - 按 layer 分组显示
  - 显示 relevance 评分

  **Verify**: `cargo build --release` 0 errors

- [ ] 3. **Runtime 记忆事件发射**

  **What**: 在 `conversation.rs` 的 `prepare_memory_context()` 中:
  - 记忆查询结果通过 `stream_callback` 发送 `TuiEvent::MemoryUpdate`
  - `on_turn_end()` 结束时发送 `TuiEvent::MemoryStats`

  **Verify**: TUI 启动后发送消息，检查 app.memory_entries 非空

- [ ] 4. **会话恢复接线**

  **What**: 在 `main.rs` TUI 初始化流程中:
  - 调用 `SessionResume::new()` 加载历史记忆
  - 通过 `resume_recent()` 恢复最近会话记忆
  - 作为 `TuiEvent::MemoryUpdate` 发送到 TUI

  **Verify**: TUI 重启后记忆面板显示历史数据

- [ ] 5. **死代码清理**

  **What**: 
  - 删除 `crates/api/src/provider_chain.rs` (398行) + lib.rs 引用
  - 删除 `crates/runtime/src/session_manager.rs` (123行) + lib.rs 引用
  - 确认 `crates/memory/src/session_manager.rs` 不受影响

  **Verify**: `cargo build --release` 0 errors, `grep -rn "provider_chain\|InMemorySessionManager" crates/` 仅内存版本残留

- [ ] 6. **TUI 事件接收记忆数据**

  **What**: 在 `app.rs:apply_event()` 中添加:
  ```rust
  TuiEvent::MemoryUpdate { entries, status } => { ... }
  TuiEvent::MemoryEntry { layer, content, relevance } => { ... }
  TuiEvent::MemoryStats { ... } => { ... }
  ```

  **Verify**: TUI 发送消息后 `app.memory_entries` 非空

- [ ] 7. **PlaceholderComponent → MemoryPanel 替换**

  **What**: 
  - `defaults.rs`: `PlaceholderComponent { id: "memory" }` → `MemoryPanel`
  - `render.rs`: `draw_memory_panel` 更新为使用 MemoryPanel 组件
  - `mod.rs`: 注册 MemoryPanel 组件

  **Verify**: TUI 启动后记忆标签页显示实时数据

- [ ] 8. **命令行记忆增强**

  **What**: 增强 `/memory` slash 命令:
  - `/memory search <query>` → BM25 搜索
  - `/memory recent` → 显示最近记忆
  - `/memory stats` → 显示统计

  **Verify**: TUI 中输入 `/memory recent` 返回结果

- [ ] 9. **文件树组件**

  **What**: 创建基础 FileTree 组件替换 PlaceholderComponent:
  - 显示当前工作目录文件结构
  - 支持展开/折叠目录

  **Verify**: `cargo build --release` 0 errors

- [ ] 10. **上下文面板实时更新**

  **What**: 更新 context_panel 实时显示:
  - 当前上下文使用率
  - 记忆注入的条目数
  - 最近记忆查询结果

  **Verify**: 发送消息后面板更新

- [ ] 11. **SubAgentExecutor 实现或删除**

  **What**: 
  - 检查 `agent.rs` trait SubAgentExecutor 是否有实际调用
  - 如无: 删除 trait + StubExecutor
  - 如有: 保留并标记 TODO

  **Verify**: `cargo build --release` 0 errors

- [ ] 12. **全量验证**

  **What**:
  - `cargo clean && cargo build --release` → 0 errors, 0 warnings (excl. imap-proto)
  - `cargo test -p cowd-memory --lib` → 456 passed
  - TUI 启动 + 发送消息 → 0 new crash.log
  - 记忆面板显示数据
  - Git push + sync develop

  **Verify**: 所有检查通过

---

## Final Verification Wave

- [ ] F1. Plan compliance + memory panel working
- [ ] F2. Build + test + code quality
- [ ] F3. TUI QA + crash.log check
- [ ] F4. Scope fidelity

---

## Must Have
- TUI 记忆面板显示实时数据
- 会话恢复跨 TUI 重启
- ProviderChain + SessionManager 死代码删除

## Must NOT Have
- 不破坏现有 TUI 功能
- 不新增外部依赖
- 不改变公共 API
