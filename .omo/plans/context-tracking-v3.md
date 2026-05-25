# COWD v0.6: 上下文精准追踪 + 模型表更新 + 配置覆盖

## 设计原则：TDD 三阶段

```
Phase 1: 目标定义      → 明确"什么算完成"
Phase 2: 测试先行      → 先写能验证目标的测试，确保不破坏现有功能
Phase 3: 实现          → 按 Wave 执行，每步对应测试
Phase 4: 目标对齐      → 验证实现是否达成原始目标
```

---

## P0: 完整现状备案（执行前快照）

经双 agent 互换验证后的当前真实状态：

### 基线
- `cargo build --release` → **0 cowd 警告**，1 imap-proto 外部依赖
- `cargo test -p cowd-memory --lib` → **447/447 PASS**

### 问题清单（全部用数据确认）

| # | 问题 | 当前状态 | 文件行 | 根因 |
|---|------|---------|--------|------|
| **P0a** | App.context_window 始终为 0 | 字段声明 `app.rs:161`，初始化为 0 `app.rs:305`，**从未被赋值** | `app.rs:161,305` | 无任何代码将 model_context_window 写到 App |
| **P0b** | TuiEvent::TokenUsage 永不发送 | 变体定义 `events.rs:29`，处理 `app.rs:855`，**无生成代码发送它** | `events.rs:29`, `app.rs:855`, `callbacks.rs` | ToolCallback 没有 on_usage，API token 数据留在 UsageTracker 中 |
| **P0c** | context_window 模型表 6条全部过时 | Claude 写 200K（实为 1M），Grok 写 131K（实为 1M），Kimi 写 128K（实为 256K） | `providers/mod.rs:312-339` | 从未更新，完全缺失中国模型（DeepSeek/Qwen/Kimi/GLM/Yi 等） |
| **P0d** | config 无 context_window 覆盖 | `config.rs` 中搜索 `model_context_window` — **0 匹配** | `config.rs` | 根本没有这个配置项 |
| **P0e** | 状态栏无 Provider | `status_bar.rs` 搜索 `provider` — **0 匹配** | `status_bar.rs:284` | 只显示 model 名字符串 |
| **P0f** | available_models 永不填充 | `app.rs:188` 声明，`app.rs:332` 初始化为空 Vec，**从不填充** | `app.rs:188,332` | 没有从 config/provider 桥接模型列表 |
| **P0g** | 金额代码全部存活 | `usage.rs:10-163` 所有 8 个符号定义，被 6 个文件调用 | `usage.rs` 全局 | 之前计划说"移除"，但检查发现全部在用 |
| **P0h** | 遗留 status_bar 仍被导入 | `widgets/status_bar.rs` 被 `render.rs:10` 导入，`render.rs:28` 调用 | `render.rs:10,28` | 未被旧版移除 |
| **P0i** | token_distribution 死 | `context_profiler.rs:57` 定义，**零调用** | `context_profiler.rs:57` | 写了但没用 |
| **P0j** | render_engine 测试实际活跃 | 注释说"disabled"但 `/* */` 是空注释，`#[cfg(test)]` 模块活跃 | `render_engine.rs:86-382` | 误导性注释 |

---

## Phase 1: 目标定义

### 核心目标

**在完全不破坏现有功能的前提下**，打通 token 数据从 API 返回到 TUI 展示的完整链路，并更新所有模型 context_window 数据为 2026 年 5 月最新值，同时允许用户通过 config 覆盖。

### 具体目标（验收标准）

| 目标 | 验收条件 |
|------|---------|
| G1. context_window 模型表更新 | `model_context_window("claude-sonnet-4-6") == 1_000_000`（原是 200K） |
| G2. 中国模型覆盖 | `model_context_window("deepseek-chat")` / `model_context_window("qwen-max")` / `model_context_window("glm-4")` 返回正确值（原是 `None => 200K fallback`） |
| G3. config 覆盖 | `RuntimeFeatureConfig.model_context_windows` 存 HashMap，`get("foo")` 查表优先于静态表 |
| G4. TokenUsage 流向 TUI | `callbacks.rs` 发送 `TuiEvent::TokenUsage`，`App::apply_event` 收到后更新 6 个计数器字段 |
| G5. context_window 到达 App | `App::context_window` 在某处被设为 `> 0`（不再永远为 0） |
| G6. 状态栏进度条可见 | `token_bar()` 返回 `Some(...)`（不再因 `window == 0` 返回 `None`） |
| G7. 状态栏显示 Provider | `panel_model_status` 含 `provider_label` |
| G8. 资金代码零影响 | 所有 `pricing_for_model`/`estimate_cost_usd` 等继续工作，测试通过 |
| G9. 死代码清理 | `token_distribution` 移除或加调用；render_engine 注释修正 |
| G10. 构建和测试 | `cargo build --release` 零警告，`cargo test -p cowd-memory --lib` 全部通过 |

### 不做什么（Guardrails）
- ❌ 不移除金额代码（全部在用，后续再讨论）
- ❌ 不引入真实 tokenizer
- ❌ 不改动现有 ToolCallback trait 签名（只新增默认空实现方法）
- ❌ 不修改 TuiEvent 现有变体（只新增 `ContextWindow` 变体）

---

## Phase 2: 测试先行

### T2.1 — model_context_window 测试

```rust
// 要加到 crates/api/src/providers/mod.rs 测试中
#[test]
fn model_context_window_updated_values() {
    assert_eq!(model_context_window("claude-opus-4-6"), 1_000_000);
    assert_eq!(model_context_window("claude-sonnet-4-6"), 1_000_000);
    assert_eq!(model_context_window("claude-haiku-4-5-20251213"), 200_000);
    assert_eq!(model_context_window("grok-3"), 1_000_000);
    assert_eq!(model_context_window("kimi-latest"), 262_144);
}

#[test]
fn model_context_window_chinese_models() {
    assert_eq!(model_context_window("deepseek-chat"), 1_000_000);
    assert_eq!(model_context_window("deepseek-v4-pro"), 1_000_000);
    assert_eq!(model_context_window("deepseek-r1"), 128_000);
    assert_eq!(model_context_window("qwen-max"), 262_144);
    assert_eq!(model_context_window("qwen-plus"), 131_072);
    assert_eq!(model_context_window("glm-4"), 128_000);
    assert_eq!(model_context_window("yi-lightning"), 128_000);
}

#[test]
fn model_context_window_fallback_reduced() {
    // 未知模型不再返回 200_000，返回 128_000
    assert_eq!(model_context_window("unknown-model-xyz"), 128_000);
}

#[test]
fn model_context_window_config_override_priority() {
    // config 覆盖 > 静态表 > fallback
    let mut overrides = HashMap::new();
    overrides.insert("claude-sonnet-4-6".to_string(), 500_000);
    overrides.insert("my-model".to_string(), 32_000);
    assert_eq!(model_context_window_with_overrides("claude-sonnet-4-6", Some(&overrides)), 500_000);
    assert_eq!(model_context_window_with_overrides("my-model", Some(&overrides)), 32_000);
    assert_eq!(model_context_window_with_overrides("grok-3", Some(&overrides)), 1_000_000); // 静态表
    assert_eq!(model_context_window_with_overrides("unknown-model", Some(&overrides)), 128_000); // fallback
}
```

### T2.2 — RuntimeFeatureConfig 测试

```rust
// 加到 crates/runtime/src/config.rs 测试
#[test]
fn parses_model_context_windows_from_config() {
    let json = r#"{"model_context_windows":{"claude-sonnet-4-6":500000,"my-model":32000}}"#;
    // ... 验证解析逻辑
}
```

### T2.3 — TuiEvent::ContextWindow 测试

```rust
// 加到 crates/cowd-cli/src/tui/events.rs 测试
#[test]
fn context_window_event_updates_app() {
    let mut app = App::new("test", "test");
    assert_eq!(app.context_window, 0);
    app.apply_event(TuiEvent::ContextWindow(200_000));
    assert_eq!(app.context_window, 200_000);
}

#[test]
fn token_usage_event_updates_all_counters() {
    let mut app = App::new("test", "test");
    app.apply_event(TuiEvent::TurnStarted);
    app.apply_event(TuiEvent::TokenUsage {
        input: 100, output: 50, cache_create: 10, cache_read: 5,
    });
    assert_eq!(app.input_tokens, 100);
    assert_eq!(app.output_tokens, 50);
    assert_eq!(app.token_count, 165);
    assert_eq!(app.turn_input_tokens, 100);
}
```

### T2.4 — status_bar token_bar 测试

```rust
// 加到 crates/cowd-cli/src/tui/components/status_bar.rs 测试
#[test]
fn token_bar_returns_some_when_window_nonzero() {
    let mut app = App::new("test", "test");
    app.context_window = 200_000;
    app.token_count = 50_000;
    let bar = token_bar(&app);
    assert!(bar.is_some());
    assert!(bar.unwrap().contains("25%"));
}

#[test]
fn token_bar_returns_none_when_window_zero() {
    let app = App::new("test", "test");
    // context_window = 0 (default)
    assert!(token_bar(&app).is_none());
}
```

### T2.5 — 现有功能零影响验证

```rust
// 确认价格计算继续工作
#[test]
fn pricing_code_still_works() {
    let pricing = pricing_for_model("claude-sonnet-4-6-20250514");
    assert!(pricing.is_some());
}

// 确认 ToolCallback 签名不变（新方法有默认实现）
#[test]
fn tool_callback_still_works() {
    // 编译验证：现有实现不因为添加 on_usage 而破坏
}
```

---

## Phase 3: 实现

### Wave 1: context_window 静态表更新 + config 覆盖

| 任务 | 文件 | 改动 |
|------|------|------|
| 1.1 | `providers/mod.rs:312-339` | 用 ~30 条完整表替换当前 6 条，fallback 改为 128K |
| 1.2 | `providers/mod.rs` | 新增 `model_context_window_with_overrides(model, overrides)` 函数 |
| 1.3 | `runtime/config.rs` | RuntimeFeatureConfig 增加 `model_context_windows: HashMap<String,u32>` + 解析 + getter |
| 1.4 | `runtime/conversation.rs` | `with_model_context_window` 增加传递 config 覆盖 |
| 1.5 | `main.rs:6534` | 将 config 覆盖合并到 context_window 查找 |

### Wave 2: TokenUsage 数据流打通

| 任务 | 文件 | 改动 |
|------|------|------|
| 2.1 | `conversation.rs:122` | ToolCallback trait 增加 `fn on_usage(&self, usage: &TokenUsage) {}`（默认空实现） |
| 2.2 | `conversation.rs:701` | 在 `usage_tracker.record(usage)` 后调用 `cb.on_usage(&usage)` |
| 2.3 | `callbacks.rs:35` | TuiToolCallback 实现 `on_usage` → `.try_send(TuiEvent::TokenUsage{...})` |
| 2.4 | `events.rs:27` | TuiEvent 增加 `ContextWindow(u64)` 变体 |
| 2.5 | `app.rs:860` | 处理 `TuiEvent::ContextWindow(u64)` → `self.context_window = val` |
| 2.6 | `main.rs:6557` | 在 `runtime.with_model_context_window(model_ctx)` 后发送 `TuiEvent::ContextWindow(model_ctx)` |

### Wave 3: 展示层 + 死代码

| 任务 | 文件 | 改动 |
|------|------|------|
| 3.1 | `status_bar.rs:284` | `panel_model_status` 增加 `detect_provider_kind` |
| 3.2 | `context_profiler.rs:57` | 移除 `token_distribution()` 方法（零调用） |
| 3.3 | `render_engine.rs:86` | 修复注释（测试实际是活跃的，改为准确说明） |
| 3.4 | `available_models` | 从 config 桥接 |
| 3.5 | `widgets/status_bar.rs` | 评估是否可移除（render.rs 仍在用它） |

---

## Phase 4: 目标对齐验证

### 逐目标确认

| 目标 | 验证方法 | 预期结果 |
|------|---------|---------|
| G1. 模型表更新 | `model_context_window("claude-sonnet-4-6")` | `== 1_000_000` |
| G2. 中国模型 | `model_context_window("deepseek-chat")` | `== 1_000_000` |
| G3. config 覆盖 | `model_context_window_with_overrides("x", &overrides)` | `overrides[x]` 优先 |
| G4. TokenUsage 到 TUI | `callbacks` 发送 + `App` 接收 | 6 个计数器字段 > 0 |
| G5. context_window 到 App | `App.context_window != 0` | 不再永远为 0 |
| G6. 进度条可见 | `token_bar(&app)` | `Some(...)` |
| G7. Provider 显示 | `status_bar` 含 provider 标签 | 格式 `"Anthropic │ ... │ model"` |
| G8. 金额代码零影响 | `cargo test -p cowd-memory --lib` | 447/447 PASS |
| G9. 死代码清理 | `token_distribution` 不存在或已挂接 | 无调用警告 |
| G10. 构建 | `cargo build --release` | 0 警告 |

### 回滚条件

若任一步骤导致 `cargo build --release` 出现新的 cowd 代码警告，或 `cargo test -p cowd-memory --lib` 出现新的失败，立即停止并分析根因。
