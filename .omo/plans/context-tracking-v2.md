# COWD v0.6: 上下文长度精准追踪 —— 去除金额，聚焦 token 利用率

## TL;DR

> **Quick Summary**: 移除全部金额代码，替换为精确的上下文窗口使用率追踪。更新全部模型 context_window 数据为2026年5月最新值（原代码几乎全部过时），并允许 config 覆盖。实时 token 数据从 API 流到 TUI。
>
> **Deliverables**:
> - 移除全部金额/定价/成本代码（~200行）
> - 更新 ~30 个模型 context_window 为最新数据（原值偏差最高 8x）
> - 新增中国 Top 10+ 模型支持（原代码完全缺失）
> - config 支持 `model_context_windows` 覆盖默认值
> - 上下文进度条首次真实工作
> - 实时 token 数据从 API 流到 TUI（现在完全断裂）

---

## Context

### 当前 context_window 数据问题

2026年5月最新网络数据 vs 代码中写死的值：

| 模型 | 代码中的值 | **实际** | 偏差 |
|------|-----------|---------|------|
| claude-opus-4-6 | **200K** | **1M** | 5x 偏小 |
| claude-sonnet-4-6 | **200K** | **1M** | 5x 偏小 |
| grok-3 / grok-3-mini | **131K** | **1M** | 8x 偏小 |
| kimi-latest | **128K** | **256K** | 2x 偏小 |
| 中国模型 (qwen, deepseek-chat, glm等) | **缺失** | 128K~10M | ❌ 完全缺失 |
| 任意未注册模型 | **200K fallback** | 视模型而定 | ❓ |

### 数据源

| 来源 | URL | 更新时间 |
|------|-----|---------|
| Context Window Explorer | lmmarketcap.com/explorers/context-windows | 2026-05 |
| BenchLM.ai 对比 | benchlm.ai/blog/posts/context-window-comparison | 2026-04 |
| TokenMix 指南 | tokenmix.ai/blog/llm-context-window-explained | 2026-04 |
| Swfte 完整表格 | swfte.com/blog/llm-context-window-explained | 2026-05 |
| Anthropic 官方 | docs.anthropic.com | 2026-05 |
| 国产模型对比 | groundy.com Chinese AI Models | 2026-03 |

### Config 覆盖机制

```yaml
# config.yaml
model_context_windows:
  "my-custom-model": 32000
  "deepseek-v4-pro": 64000    # 覆盖内置 1M 默认值
```

查找优先级：`config 覆盖 > 静态表 > fallback (128K)`

---

## Work Objectives

### Core Objective

1. 移除全部金额代码
2. 建立完整 context_window 表（国际 + 中国模型）
3. config 支持 `model_context_windows` 覆盖
4. 打通 API token 数据 → TUI 链路
5. TUI 显示 Provider + Model + `123.4K/200K (62%)`

### Must Have

- 所有模型有正确的 context_window（无 hardcoded fallback）
- config.yaml 的 `model_context_windows` 可以覆盖任意模型
- 状态栏进度条显示真实比例
- 零金额代码残留

### Must NOT Have

- 不引入金额/定价概念
- 不嵌入真实 tokenizer

---

## 完整 context_window 表（2026年5月）

### Anthropic

| 模型名字符串 | context_window |
|---|---|
| claude-opus-4-7, claude-opus-4-6 | 1_000_000 |
| claude-sonnet-4-6, claude-sonnet-4 | 1_000_000 |
| claude-haiku-4-5-20251213, claude-haiku | 200_000 |

### OpenAI

| 模型 | context_window |
|---|---|
| gpt-5, gpt-5.5, gpt-5.4 | 1_000_000 |
| gpt-4.1, gpt-4.1-mini, gpt-4.1-nano | 1_000_000 |
| gpt-4o, gpt-4o-mini | 128_000 |
| o3, o3-mini, o4-mini | 128_000 |

### Google

| 模型 | context_window |
|---|---|
| gemini-3.1-pro, gemini-3.1-flash | 2_000_000 |
| gemini-2.5-pro, gemini-2.5-flash | 1_000_000 |

### xAI

| 模型 | context_window |
|---|---|
| grok-4, grok-4.1, grok-4.20 | 2_000_000 |
| grok-3, grok-3-mini | 1_000_000 |
| grok-2 | 128_000 |

### 中国模型

| 模型 | 厂商 | context_window |
|---|---|---|
| qwen-long | 阿里 | 10_000_000 |
| qwen3.5-plus, qwen3-coder-plus | 阿里 | 1_000_000 |
| qwen3-max | 阿里 | 262_144 |
| qwen-plus | 阿里 | 131_072 |
| qwen-turbo | 阿里 | 128_000 |
| deepseek-chat, deepseek-v4, deepseek-v4-pro | 深度求索 | 1_000_000 |
| deepseek-v4-flash | 深度求索 | 1_000_000 |
| deepseek-reasoner, deepseek-r1, deepseek-r1-0528 | 深度求索 | 128_000 |
| kimi, kimi-k2.6 | 月之暗面 | 262_144 |
| kimi-k2 | 月之暗面 | 131_072 |
| glm-4 | 智谱 | 128_000 |
| yi, yi-lightning | 零一万物 | 128_000 |
| minimax | MiniMax | 128_000 |
| mimi, mimi-v2.5 | 小米 | 1_000_000 |

### 其他

| 模型 | context_window |
|---|---|
| llama-4-maverick | 1_000_000 |
| llama-4-scout | 10_000_000 |
| mistral-large | 128_000 |

**fallback: 128_000**（比原来 200K 更保守）

---

## Execution Strategy

```
Wave 1 (金额清理 + context_window 表):
├── 1. runtime/usage.rs: 移除全部金额代码 (ModelPricing, estimate_cost*, format_usd)
├── 2. runtime/config.rs: RuntimeFeatureConfig 增加 model_context_windows: HashMap<String,u32>
├── 3. api/providers/mod.rs: 重建 context_window 表为完整 ~30 条 + 128K fallback
├── 4. api/providers/mod.rs: 查找逻辑改为: config 覆盖 > 静态表 > fallback
├── 5. context_panel.rs: 移除 cost_estimate()
└── 6. conversation.rs: 清理金额引用

Wave 2 (核心链路 — TokenUsage API→TUI):
├── 7. ToolCallback trait 增加 on_usage()
├── 8. TuiToolCallback 实现 → 发送 TuiEvent::TokenUsage
├── 9. conversation.rs 记录 usage 后调用 callback
└── 10. turn 启动时传递 model_context_window

Wave 3 (展示层):
├── 11. status_bar token_bar(): 显示 "███░ 123K/200K (62%)"
├── 12. panel_model_status: 增加 Provider 标签
├── 13. 移除死代码 (widgets/status_bar, token_distribution, render_engine disabled tests)

Wave FINAL:
├── F1. cargo build --release 零警告
├── F2. cargo test pass
├── F3. status_bar 显示真实上下文百分比
└── F4. 零金额代码 + 无错误模型 context_window
```

---

## TODOs

- [ ] 1. runtime/usage.rs: 移除全部金额代码

  **What to do**:
  - 移除: `ModelPricing`, `default_sonnet_tier()`, `UsageCostEstimate`, `total_cost_usd()`,
    `pricing_for_model()`, `estimate_cost_usd()`, `estimate_cost_usd_with_pricing()`,
    `summary_lines()`, `summary_lines_for_model()`, `format_usd()`
  - 保留: `TokenUsage`（纯计数）, `UsageTracker`, `TokenUsage::total_tokens()`
  - 保留: `UsageTracker::new()`, `from_session()`, `record()`, `current_turn_usage()`, `cumulative_usage()`

- [ ] 2. runtime/config.rs: 增加 model_context_windows 支持

  **What to do**:
  - 在 `RuntimeFeatureConfig` 增加 `model_context_windows: HashMap<String, u32>`
  - 添加解析逻辑（参考 `parse_optional_aliases`）从 config JSON 的 `model_context_windows` 字段
  - 添加 getter: `fn model_context_window(&self, model: &str) -> Option<u32>`
  - config-default.yaml 增加示例:
    ```yaml
    model_context_windows:
      "my-local-model": 32000
    ```

- [ ] 3. api/providers/mod.rs: 重建 model_context_window() 为完整表

  **What to do**:
  - 用 `const MODEL_CONTEXT_WINDOWS: &[(&str, u32)]` 替代现有 `model_token_limit()` 的部分
  - 包含全部 ~30 条数据（见上表）
  - fallback: 128_000（替代当前 200_000）
  - 保留 `model_token_limit()` 用于 max_output_tokens，但独立的 context_window 表

- [ ] 4. api/providers/mod.rs: 查找逻辑改为 config 覆盖优先

  **What to do**:
  - `model_context_window()` 新增参数接受 `Option<HashMap<String,u32>>` 或从 ConversationRuntime 读取覆盖
  - 优先: config_override.get(canonical_model)
  - 二级: 静态表匹配
  - 三级: 128K fallback
  - 参考: `resolve_model_alias()` → `model_context_window()` → 新增参数

- [ ] 5. context_panel.rs: 移除 cost_estimate()

  **What to do**:
  - 移除 `context_panel.rs:81-84` 的 `cost_estimate()` 方法
  - 保留进度条和百分比显示

- [ ] 6. conversation.rs: 清理金额引用

  **What to do**:
  - 移除 `estimate_cost_usd()` / `summary_lines()` 引用
  - 清理因此变得未使用的 imports

- [ ] 7. ToolCallback trait 增加 on_usage()

  **What to do**:
  - 位置: `conversation.rs:117` trait ToolCallback
  - 增加: `fn on_usage(&self, usage: &TokenUsage) {}`

- [ ] 8. TuiToolCallback 实现 on_usage

  **What to do**:
  - 位置: `callbacks.rs`
  - 通过 `self.tx.try_send(TuiEvent::TokenUsage { input, output, cache_create, cache_read })`

- [ ] 9. conversation.rs 在记录 usage 后调用 callback

  **What to do**:
  - 在 `self.usage_tracker.record(usage)` 后增加 `cb.on_usage(&usage)`

- [ ] 10. turn 启动时传递 model_context_window 到 TUI

  **What to do**:
  - ConversationRuntime 已在 `conversation.rs:215` 存储 `model_context_window`
  - 方法 A: `TuiEvent::TurnStarted` 增加 `context_window: u64` 字段
  - 方法 B: main.rs turn runner 获取并发送新 event

- [ ] 11. status_bar: token_bar() 显示真实百分比

  **What to do**:
  - `token_bar()` 收到 `context_window > 0` + `token_count > 0`
  - 显示: `"███░ 123K/200K (62%)"`
  - `token_count_section()`: `"in:1.2K out:456"`

- [ ] 12. panel_model_status 增加 Provider 标签

  **What to do**:
  - 调用 `api::detect_provider_kind(&app.model)`
  - 格式: `"Anthropic │ Chat │ ✓ Ready │ claude-sonnet-4"`

- [ ] 13. 移除死代码

  **What to do**:
  - `widgets/status_bar.rs` (117行, 被组件版替代)
  - `context_profiler.rs:57 token_distribution()` (未调用)
  - `render_engine.rs:86-381` 禁用测试模块
  - `widgets/render.rs` (dead_code)

---

## Final Verification

- [ ] F1. `cargo build --release` 零 cowd 警告
- [ ] F2. `cargo test -p cowd-memory --lib` pass
- [ ] F3. TUI 显示 Provider + 模型 + 上下文进度条
- [ ] F4. 零金额代码残留: `grep -ri "cost\|pricing\|usd"` 无匹配
