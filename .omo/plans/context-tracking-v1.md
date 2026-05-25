# COWD v0.6: 上下文估算 + 模型使用追踪 + 设计缺陷修复

## TL;DR

> **Quick Summary**: 修复10项已验证的设计缺陷，打通 TokenUsage 链路（从 API 返回→TUI 展示），消除状态栏进度条和上下文面板永久性不可用问题。
>
> **Deliverables**:
> - 状态栏 token 进度条首次正常工作（context_window 从 0→真实值）
> - TUI 显示 Provider + Model + 真实成本（替代硬编码 $3/$15）
> - TokenUsage 数据从 API 实时流到 TUI（现在完全断裂）
> - 消除 3 处死代码，统一 3 个 TokenUsage 表示
> - 所有注册模型（10个）都有正确的 pricing 和 context_window

---

## Context

### 3 路分析结果综合

| 来源 | 覆盖 | 关键修正 |
|------|------|----------|
| **Explore agent** (10 patterns exhaustive) | 全代码库 10 种模式扫描 | 发现 `disabled_due_to_broken_imports` 不存在 —— 这是我不完善分析的错误断言 |
| **Oracle** (3 questions deep) | 架构层 7 个子问题 | 发现根本原因：`TuiEvent::TokenUsage` 在生成代码中**从未被发送**，只在测试中有 |
| **直接调查** (状态栏→events→App 链路) | TUI 数据流追踪 | 确认 `App.context_window` 永远为 0，`token_bar()` 永远返回 None |

### 之前分析的错误

1. ❌ 断言"5 个 `disabled_due_to_broken_imports` 编译警告"——当前代码库中**不存在**
2. ❌ 将问题定位为"context_window 未传递"——正确但不够根本。真正原因是 `TuiEvent::TokenUsage` 在生成代码中**从未发送**，不仅是 context_window 缺失
3. ❌ 遗漏了核心架构问题：API 返回的 `AssistantEvent::Usage(usage)` 在 `conversation.rs:679` 被捕获，但在 `main.rs:2846` 构建 TurnComplete **时丢失**

### 已验证的核心缺陷（10项）

**P0——功能完全不可用（2项）：**

| # | 缺陷 | 证实者 | 根因 |
|---|------|--------|------|
| **7** | 上下文进度条永久隐藏 | 探索+神谕+直接 | `App.context_window` 初始化为 0，从未设置。`token_bar()` 在 `window == 0` 时返回 None |
| **8** | 所有 token 计数器显示 0/0 | 神谕 (关键发现) | `TuiEvent::TokenUsage` 在生成代码中**从未调度** —— API 数据在 `main.rs:2846` 构建 TurnComplete 时丢弃 |

**P1——数据处理错误（3项）：**

| # | 缺陷 | 位置 | 问题 |
|---|------|------|------|
| **1** | TokenUsage 三种不相容表示 | `usage.rs:31(u32)` / `event_bus.rs:15(u64 2字段)` / `events.rs:29(u64 4字段不同名)` | Cache 字段在到达 events.rs 前丢失，名字不一致 |
| **5** | 上下文面板硬编码成本 | `context_panel.rs:81-84` | $3/$15 对所有模型错误，且不对称（累计 input vs 本轮 output） |
| **6** | 定价仅覆盖 3 个模型族 | `usage.rs:59` vs `MODEL_REGISTRY`(10条) | 7 个注册模型无声回退到 sonnet 定价 |

**P2——死代码/缺失功能（5项）：**

| # | 缺陷 | 位置 | 操作 |
|---|------|------|------|
| **2** | legacy widgets/status_bar.rs | `widgets/status_bar.rs` | 移除 |
| **3** | token_distribution() 未调用 | `context_profiler.rs:57` | 移除 |
| **4** | render_engine.rs 测试模块被注释 | `render_engine.rs:86-381` | 移除（12+测试，导入路径已损坏） |
| **9** | ProviderKind 不在 TUI 中 | 全局搜索 TUI 目录 0 命中 | 添加到状态栏 |
| **10** | available_models 从不填充 | `app.rs:332` 始终为空 | 从配置桥接 |

---

## Execution Strategy

### Wave 1（关键修复 —— 让 token 数据流动）

```
Wave 1: 打通 TokenUsage 链路（P0 级）
├── 1. ToolCallback trait 增加 on_usage(usage: TokenUsage) 方法
├── 2. TuiToolCallback 实现 on_usage → 发送 TuiEvent::TokenUsage
├── 3. conversation.rs 在记录 usage_tracker 后调用 callback.on_usage()
├── 4. 在 turn 启动时将 context_window 传递给 TUI
└── 5. 验证：status_bar 显示进度条 + token 计数器 > 0
```

### Wave 2（消除错误数据）

```
Wave 2: 修复定价和死代码
├── 6. 修复 context_panel.rs 成本计算（使用 pricing_for_model + 正确累计值）
├── 7. 扩展 pricing_for_model() 覆盖所有 10 个注册模型
├── 8. 统一 TokenUsage 三种表示（至少确保数据不丢失）
└── 9. 移除死代码
```

### Wave 3（展示增强）

```
Wave 3: UI 增强
├── 10. 状态栏增加 Provider 标签
├── 11. 状态栏增加成本显示
└── 12. 桥接 available_models 从配置到 TUI
```

---

## TODOs

- [ ] 1. ToolCallback trait 增加 on_usage() 方法

  **What to do**:
  - 位置：`crates/runtime/src/conversation.rs:117` (trait ToolCallback)
  - 增加 `fn on_usage(&self, usage: &TokenUsage) {}`（默认空实现，不影响现有实现）
  - 需要 `use crate::usage::TokenUsage;`
  - **参考 opencode**: `packages/app/src/components/session/session-context-metrics.ts:80` `getSessionContextMetrics()`

- [ ] 2. TuiToolCallback 实现 on_usage

  **What to do**:
  - 位置：`crates/cowd-cli/src/tui/callbacks.rs`
  - 通过 `self.tx.try_send(TuiEvent::TokenUsage { input, output, cache_create, cache_read })` 发送
  - 使用 `TokenUsage` 的 input_tokens, output_tokens, cache_creation_input_tokens, cache_read_input_tokens

- [ ] 3. conversation.rs 在记录 usage 后调用 callback

  **What to do**:
  - 位置：`crates/runtime/src/conversation.rs:701-702`
  - 在 `self.usage_tracker.record(usage);` 后增加 `if let Some(cb) = &self.tool_callback { cb.on_usage(&usage); }`

- [ ] 4. 在 turn 启动时将 context_window 传递给 TUI

  **What to do**:
  - 方案 A：在 `TuiEvent::TurnStarted` 或新增 `TuiEvent::ContextWindow(u64)` 中传递
  - 方案 B：在 `main.rs` turn runner 中从 `ConversationRuntime.model_context_window` 读取并设置 `app.context_window`
  - `model_context_window()` 在 `api/providers/mod.rs:337`，`ConversationRuntime` 在 `conversation.rs:215` 存储

- [ ] 5. 验证 status_bar 显示进度条

  **What to do**:
  - 手动验证：确认 `status_bar.rs:466` 的 `token_bar()` 收到 `window > 0`
  - 确认 `status_bar.rs:470` 的 `used = app.token_count` > 0（此时 TuiEvent::TokenUsage 已被调度）

- [ ] 6. 修复 context_panel.rs 成本计算

  **What to do**:
  - 替换 `context_panel.rs:81-84` 的硬编码 `$3/$15`
  - 使用 `runtime::usage::pricing_for_model(&self.model)` 获取正确定价
  - 使用 `self.input_tokens`（累计）和 `self.output_tokens`（累计，非 turn 级）计算
  - 参考 `usage.rs:59` pricing_for_model() 和 `usage.rs:101` estimate_cost_usd_with_pricing()

- [ ] 7. 扩展 pricing_for_model()

  **What to do**:
  - 为 `grok-*` 族（grok-3, grok-3-mini, grok-2, grok-mini, grok）添加定价
  - 为 `kimi-*` 族（kimi, kimi-latest）添加定价
  - 为 `model_token_limit()` 补全缺失的 4 个模型（grok, grok-mini, grok-2, kimi）
  - 参考 `providers/mod.rs:52-143` MODEL_REGISTRY

- [ ] 8. 统一 TokenUsage 三种表示

  **What to do**:
  - `event_bus.rs:15` 的 `TokenUsage { input_tokens: u64, output_tokens: u64 }` 增加 `cache_create: u64, cache_read: u64`
  - 确认 `events.rs:29` 的 `TuiEvent::TokenUsage` 字段名与 `usage::TokenUsage` 对应

- [ ] 9. 移除死代码

  **What to do**:
  - 文件：`widgets/status_bar.rs`（已证实被组件版完全替代）
  - 方法：`context_profiler.rs:57` `token_distribution()`（证实从未被调用）
  - 测试模块：`render_engine.rs:86-381`（12+ 测试已被注释，导入路径已损坏）

- [ ] 10. 状态栏增加 Provider 标签

  **What to do**:
  - 在 `status_bar.rs:284` 的 `panel_model_status` 中增加 provider 显示
  - 调用 `api::detect_provider_kind(&app.model)` → `api/provider/mod.rs:256`
  - 格式：`"Anthropic │ Chat │ ✓ Ready │ claude-sonnet-4"`

- [ ] 11. 状态栏增加成本显示

  **What to do**:
  - 在 `status_bar.rs sync_from_app()` 中增加新 section 或附加到 `token_count`
  - 显示格式：`"in:1.2K out:456 $0.023"`
  - 使用 `runtime::usage::pricing_for_model()` + `format_usd()`

- [ ] 12. 桥接 available_models 从配置到 TUI

  **What to do**:
  - 在 `main.rs` init TUI 时，从 config 加载模型列表
  - 设置 `App.available_models` 使 `next_model()` 正常工作
  - 通过 `TuiState::new()` 或新增 event 传递

---

## Final Verification

- [ ] F1. `cargo build --release` 零 cowd 警告
- [ ] F2. `cargo test -p cowd-memory --lib` 447/447 PASS
- [ ] F3. TUI 状态栏显示真实 token 进度条（非 0 context_window）
- [ ] F4. context_panel 显示有意义的模型成本
