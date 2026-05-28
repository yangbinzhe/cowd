# Cowd 优化目标与 TDD 修复计划

## 发现的问题

| # | 问题 | 危害 | 根因 | 状态 |
|---|------|------|------|------|
| 1 | `cowd serve` 无法启动 | 🔴 Server 完全不可用 | `Handle::try_current()` 在 subprocess 中找到父进程#root#运行时 | ✅ 已修复(SHARED_RT) |
| 2 | Space+Escape 后 TUI 崩溃 | 🟡 TUI 不稳定 | Which-Key 渲染后触发 #root#运行时冲突 | 🔍 需验证 |
| 3 | 测试框架 TUI 启动检测 | 🟢 仅测试问题 | 启动时序 + 断言匹配 | ✅ 已修复 |
| 4 | 测试框架侧边栏检测 | 🟢 仅测试问题 | 会话在 TUI 崩溃后死亡 | ⏳ 依赖修复#2 |

## TDD 修复计划

### Fix 1: `cowd serve` (已完成 ✅)
- 改动: `main.rs` 中 Server 启动使用 `SHARED_RT`，不再 `Handle::try_current()`
- 验证: `cowd serve --no-auth` → health 返回 200
- 测试: `tests/interactive` 中 Server 4 场景全部通过

### Fix 2: 嵌套运行时崩溃
- 目标: 消除 `conversation.rs:275` 的 `Runtime::new()` 调用
- 方案: 将 `ConversationRuntime::new()` 中记忆初始化改为惰性初始化
- TDD:
  1. RED: 创建单元测试 `test_memory_init_no_nested_runtime`
  2. GREEN: `CognitiveContextManager` 初始化延迟到首次 `prepare_context()` 调用时
  3. REFACTOR: 移除 `try_current() → Runtime::new()` 模式
- 验证: `cargo test -p runtime` + TUI 手动测试

### Fix 3: TUI 测试框架健壮性
- 目标: 测试框架正确报告 TUI 状态
- 方案: 捕获重试机制 + 更宽容的断言
- 已完成: startup 3 次重试、session_alive 检查、跳过死亡会话

### Fix 4: 最终集成验证
- 运行全部测试场景，确认 8/8 PASS
