# 模型系统全盘重构 — TDD 实施方案

## 目标

1. ✅ 保持 `ANTHROPIC_MODEL` 向后兼容，添加 `COWD_MODEL`
2. ✅ 实现 Provider 故障转移运行时（`providerFallbacks`）
3. ✅ 动态模型注册（文件 + 在线更新能力）+ 全面定价表
4. ✅ 两套 Config 系统完全统一
5. ✅ 别名统一按配置文件为准
6. ✅ API key 以配置文件为准，环境变量仅作兼容
7. ✅ Prompt 缓存全模型生效，统一会话缓存，无随机缓存头

---

## Wave 1: 配置系统统一 (Config Unification)

### T1.1 消除两个 Config 系统

**当前状态**:
- `crates/config/src/lib.rs` → `UnifiedConfig` (serde 自动反序列化)
- `crates/runtime/src/config.rs` → `RuntimeConfig` (手写 YAML→JSON 解析)

**目标**: 只保留 `runtime::RuntimeConfig`，删除 `config::UnifiedConfig`

**TDD 测试**:

```rust
// 测试 1: 两套 config 加载相同的 YAML 产生相同的模型名
#[test]
fn test_both_configs_produce_same_model() {
    let yaml = r#"model: "claude-sonnet-4-6""#;
    let unified = config::UnifiedConfig::from_yaml(yaml);
    let runtime = runtime::RuntimeConfig::from_yaml(yaml);
    assert_eq!(unified.effective_model(), runtime.model());
}

// 测试 2: 统一后只有一套 config
#[test]
fn test_only_runtime_config_exists() {
    // 编译验证：config::UnifiedConfig 已不存在
    // 运行验证：config crate 不再包含 UnifiedConfig
}
```

**实施步骤**:
1. 读 `crates/config/src/lib.rs` 找出所有被外部引用的类型
2. 将这些类型迁移到 `crates/runtime/src/config.rs`
3. 删除 `crates/config/src/lib.rs` 中的 `UnifiedConfig`、`ProvidersConfig`
4. 更新所有 `use config::UnifiedConfig` → `use runtime::RuntimeConfig`
5. 删除 `config` crate 如果不再需要

### T1.2 统一 API key 优先级

**当前状态**: 环境变量优先 → 配置文件 → 默认值

**目标**: 配置文件优先 → 环境变量兜底（仅兼容）

**TDD 测试**:

```rust
// 测试 1: 配置文件 key 覆盖环境变量
#[test]
fn test_config_key_overrides_env() {
    temp_env::with_var("ANTHROPIC_API_KEY", Some("env-key"), || {
        let config = r#"providers: { anthropic: { api_key: "config-key" } }"#;
        let key = resolve_api_key("anthropic", &config);
        assert_eq!(key, "config-key"); // 配置文件优先
    });
}

// 测试 2: 无配置文件 key 时回退到环境变量
#[test]
fn test_fallback_to_env_when_no_config_key() {
    temp_env::with_var("ANTHROPIC_API_KEY", Some("env-key"), || {
        let config = r#"providers: { anthropic: {} }"#;
        let key = resolve_api_key("anthropic", &config);
        assert_eq!(key, "env-key"); // 环境变量兜底
    });
}
```

### T1.3 统一默认模型

**当前状态**: CLI `"claude-opus-4-6"` vs config `"claude-sonnet-4-20250514"`

**目标**: 统一为 `"claude-sonnet-4-6"`（与 config-default.yaml 一致）

---

## Wave 2: 模型别名 + 动态注册

### T2.1 别名统一为配置驱动

**当前状态**: CLI 内置 3 + api MODE_REGISTRY + 配置 aliases

**目标**: 只保留配置 aliases + 一个内置回退表

**TDD 测试**:

```rust
// 测试 1: 配置别名优先
#[test]
fn test_config_alias_overrides_builtin() {
    let config = r#"aliases: { sonnet: "custom-model" }"#;
    assert_eq!(resolve("sonnet", &config), "custom-model");
}

// 测试 2: 无配置时回退到内置表
#[test]
fn test_builtin_alias_when_no_config() {
    let config = r#"aliases: {}"#;
    assert_eq!(resolve("sonnet", &config), "claude-sonnet-4-6");
}

// 测试 3: 循环别名防递归
#[test]
#[should_panic(expected = "circular alias")]
fn test_circular_alias_detected() {
    let config = r#"aliases: { a: "b", b: "a" }"#;
    resolve("a", &config);
}
```

**实施步骤**:
1. 删除 `crates/cowd-cli/src/cli/mod.rs` 中的 `resolve_model_alias`
2. 删除 `crates/api/src/providers/mod.rs` 中的 `MODEL_REGISTRY`
3. 新建 `crates/runtime/src/model_registry.rs` 统一别名解析
4. 内置回退表：仅 `config-default.yaml` 中的内置别名

### T2.2 动态模型注册（文件 + 在线更新）

**设计**: `~/.cowd/models.yaml` — 包含了完整的模型列表、定价、token 限制

**文件格式** (YAML):
```yaml
# ~/.cowd/models.yaml — 模型注册表
# 可通过 `cowd models update` 从远程更新
version: "2026-05-27"
source: "https://raw.githubusercontent.com/eyeout/cowd/main/models.yaml"
models:
  claude-sonnet-4-6:
    provider: anthropic
    display_name: "Claude Sonnet 4.6"
    context_window: 200000
    max_output_tokens: 64000
    pricing:
      input_per_1m: 3.0
      output_per_1m: 15.0
      cache_write_per_1m: 3.75
      cache_read_per_1m: 0.30
    capabilities: [text, vision, tool_use, prompt_cache]
    
  deepseek-v4-pro:
    provider: deepseek
    display_name: "DeepSeek V4 Pro"
    context_window: 128000
    max_output_tokens: 32000
    pricing:
      input_per_1m: 0.55
      output_per_1m: 2.19
    capabilities: [text, tool_use]
    
  qwen-max:
    provider: dashscope
    display_name: "Qwen Max"
    context_window: 32768
    max_output_tokens: 8192
    pricing:
      input_per_1m: 2.0
      output_per_1m: 8.0
    capabilities: [text, tool_use]
    
  # ... 40+ 模型
```

**TDD 测试**:

```rust
// 测试 1: 从文件加载模型注册表
#[test]
fn test_load_model_registry_from_file() {
    let yaml = std::fs::read_to_string("~/.cowd/models.yaml").unwrap();
    let registry: ModelRegistry = serde_yaml::from_str(&yaml).unwrap();
    assert!(registry.models.contains_key("claude-sonnet-4-6"));
    assert_eq!(registry.models["claude-sonnet-4-6"].pricing.input_per_1m, 3.0);
}

// 测试 2: 在线更新模型注册表
#[tokio::test]
async fn test_update_model_registry_from_remote() {
    let registry = ModelRegistry::update_from_remote().await.unwrap();
    assert!(registry.version > "2026-01-01");
}

// 测试 3: 注册表中不存在的模型回退到默认
#[test]
fn test_unknown_model_uses_defaults() {
    let registry = ModelRegistry::default();
    let info = registry.get("nonexistent-model");
    assert_eq!(info.context_window, 128000); // 默认值
    assert_eq!(info.max_output_tokens, 64000);
}

// 测试 4: 定价计算精确到小数点
#[test]
fn test_pricing_calculation() {
    let registry = ModelRegistry::default();
    let info = registry.get("claude-sonnet-4-6");
    let cost = info.pricing.calculate_cost(15000, 5000, Some(8000), Some(2000));
    // input: 15000/1M * $3.0 = $0.045
    // output: 5000/1M * $15.0 = $0.075
    // cache_write: 8000/1M * $3.75 = $0.030
    // cache_read: 2000/1M * $0.30 = $0.0006
    // total ≈ $0.1506
    assert!((cost - 0.1506).abs() < 0.001);
}
```

**实施步骤**:
1. 创建 `~/.cowd/models.yaml` 含 40+ 模型完整信息
2. 创建 `crates/runtime/src/model_registry.rs`
3. 添加 `cowd models update` CLI 命令从远程拉取
4. 添加 `cowd models list` 列出已注册模型
5. GitHub raw URL 作为默认更新源

---

## Wave 3: Provider 故障转移

### T3.1 实现运行时故障转移链

**设计**:
- `providerFallbacks` 配置定义 primary + fallbacks 链
- 当 primary 返回 429/500/502/503 时，自动切换到下一个 fallback
- 使用与 `send_with_retry` 相同的退避策略
- 每个 fallback 尝试 `send_with_retry`（8 次重试）
- 故障转移状态记录到 tracing 日志

**TDD 测试**:

```rust
// 测试 1: primary 成功时不触发 fallback
#[tokio::test]
async fn test_no_fallback_when_primary_succeeds() {
    let chain = FallbackChain::new("primary-model", &["fallback-1", "fallback-2"]);
    let result = chain.try_send(|model| async {
        if model == "primary-model" { Ok(response) } else { panic!() }
    }).await;
    assert!(result.is_ok());
    assert_eq!(chain.attempts(), 1); // 只尝试了 primary
}

// 测试 2: primary 429 → 自动切换到 fallback-1
#[tokio::test]
async fn test_fallback_on_429() {
    let chain = FallbackChain::new("primary", &["fallback-1"]);
    let mut calls = 0;
    let result = chain.try_send(|model| async {
        calls += 1;
        if model == "primary" { Err(ProviderError::RateLimited) }
        else { Ok(response) }
    }).await;
    assert!(result.is_ok());
    assert_eq!(calls, 2); // primary + fallback-1
}

// 测试 3: 所有 fallback 耗尽后返回错误
#[tokio::test]
async fn test_exhausted_fallbacks_return_error() {
    let chain = FallbackChain::new("primary", &["fb-1", "fb-2"]);
    let result = chain.try_send(|_| async { Err(ProviderError::ServerError(500)) }).await;
    assert!(result.is_err());
    assert_eq!(chain.attempts(), 3); // 全部尝试过
}

// 测试 4: 故障转移状态写入 tracing 日志
#[test]
fn test_fallback_logs_include_model_names() {
    let chain = FallbackChain::new("primary", &["fb-1"]);
    // 验证 tracing span 包含 fallback 信息
}
```

**实施步骤**:
1. 创建 `crates/runtime/src/fallback_chain.rs`
2. `FallbackChain` 结构：primary model + `Vec<String>` fallbacks
3. `try_send` 方法：遍历链，每个模型调用 `send_with_retry`
4. 接入 `ConversationRuntime::run_turn_async` 的 LLM 调用点
5. 从 `RuntimeConfig.provider_fallbacks` 读取配置

---

## Wave 4: Prompt 缓存统一

### T4.1 全模型 Prompt 缓存

**当前状态**: 仅 Anthropic provider 有 PromptCache，OpenAI-compat 没有

**目标**: 所有 provider 共享统一的 Prompt 缓存层，不写随机缓存头

**TDD 测试**:

```rust
// 测试 1: OpenAI 模型也有缓存
#[tokio::test]
async fn test_openai_model_uses_prompt_cache() {
    let cache = PromptCache::new(session_id);
    let request = ApiRequest { model: "gpt-4.1-mini", messages, system, tools };
    let cached = cache.get_or_compute(&request).await;
    assert!(cached.is_some());
}

// 测试 2: 同一对话中重复请求命中缓存
#[tokio::test]
async fn test_repeated_request_hits_cache() {
    let cache = PromptCache::new(session_id);
    let request = ApiRequest { model: "claude-sonnet-4-6", messages, system, tools };
    let first = cache.get_or_compute(&request).await;
    let second = cache.get_or_compute(&request).await;
    assert_eq!(first.input_tokens, second.input_tokens); // 缓存命中
}

// 测试 3: 不同 system prompt 产生不同缓存
#[tokio::test]
async fn test_different_system_prompt_different_cache() {
    let cache = PromptCache::new(session_id);
    let r1 = ApiRequest { system: "You are a Rust expert", .. };
    let r2 = ApiRequest { system: "You are a Python expert", .. };
    assert_ne!(cache.compute_hash(&r1), cache.compute_hash(&r2));
}

// 测试 4: 缓存不包含随机元素
#[test]
fn test_cache_hash_is_deterministic() {
    let r1 = ApiRequest { model: "gpt-4", messages: vec![], system: "test", tools: vec![] };
    let h1 = PromptCache::compute_hash(&r1);
    let h2 = PromptCache::compute_hash(&r1);
    assert_eq!(h1, h2); // 确定性 → 无随机元素
}

// 测试 5: 跨模型缓存隔离
#[tokio::test]
async fn test_cross_model_cache_isolation() {
    let cache = PromptCache::new(session_id);
    let r1 = ApiRequest { model: "claude-sonnet-4-6", messages, system, tools };
    let r2 = ApiRequest { model: "gpt-4.1-mini", messages, system, tools };
    // 同一内容但不同模型 → 不同缓存条目
    assert_ne!(cache.compute_hash(&r1), cache.compute_hash(&r2));
}
```

**实施步骤**:
1. 将 PromptCache 从 `crates/api/src/` 移到 `crates/runtime/src/`
2. 在 `ProviderClient` 外层包装 `CachedProviderClient`
3. 缓存 Key = `hash(model + system_prompt + tools_schema + last_n_messages)`
4. 确保 Hash 输入不包含 timestamp、request_id、随机 nonce
5. TTL: 30s (匹配 Anthropic 原生行为)
6. 持久化到 `~/.cowd/prompt-cache/{session_id}/`

---

## Wave 5: 最终集成验证

### T5.1 端到端测试

```rust
// 测试 1: 完整链路 — 配置 → 别名 → provider → 故障转移 → 缓存
#[tokio::test]
async fn test_full_pipeline() {
    let config = RuntimeConfig::load("~/.cowd/config.yaml");
    let model = config.resolve_model("fast"); // 别名解析
    let provider = ProviderClient::from_config(&config, &model);
    let cached = CachedProviderClient::new(provider, session_id);
    
    // 发送请求，primary 失败时自动 fallback
    let response = FallbackChain::with_config(&config, &model)
        .try_send(|m| cached.send(ApiRequest { model: m, .. }))
        .await;
    
    assert!(response.is_ok());
}
```

### T5.2 测试评价闭环

| Wave | 测试数 | 评价标准 |
|------|--------|----------|
| T1 (Config 统一) | 6 | 两套 config 合并，编译通过，API key 优先级正确 |
| T2 (别名+注册) | 8 | 别名唯一来源=配置，动态注册表可读写 |
| T3 (故障转移) | 5 | 429→fallback, 链耗尽→报错, 日志记录 |
| T4 (缓存统一) | 6 | 全模型缓存, 确定性 hash, 跨会话持久化 |
| T5 (集成验证) | 3 | 端到端链路通过 |

---

## 执行顺序

```
Wave 1 (Config 统一): T1.1 + T1.2 + T1.3  [2-3h]
    ↓
Wave 2 (别名+注册): T2.1 + T2.2          [3-4h]
    ↓
Wave 3 (故障转移): T3.1                   [2-3h]
    ↓
Wave 4 (缓存统一): T4.1                   [2-3h]
    ↓
Wave 5 (集成验证): T5.1 + T5.2            [1-2h]

总计: 10-15h
```
