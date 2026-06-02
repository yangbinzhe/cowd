# 配置命名全局统一方案 v2 (snake_case, Oracle审定)

> 问题：代码解析器(camelCase) 与 YAML(snake_case) 键名严重不一致
> 方案：统一为 snake_case，全覆盖双格式桥接，零破坏性变更
> Oracle: NO-GO→GO (已修正7项)
> 工期：4-6h | 风险：中

---

## 一、问题规模

完整审计发现了 **21处关键不匹配**，涵盖：

| 类别 | 数量 | 严重度 | 影响 |
|------|------|--------|------|
| compression子键全失效 | 9处 | 🔴 严重 | YAML值静默被忽略 |
| memory子键失效 | 4处 | 🟡 中等 | `aaakIndexEnabled`, `coherenceThreshold` 等 |
| sandbox子键失效 | 1处 | 🟡 | `allowedMounts` vs `allowed_dirs` |
| plugins键失效 | 1处 | 🟡 | `externalDirectories` vs `external_dirs` |
| vector子键失效 | 2处 | 🟡 | `timeoutSecs`, `batchSize` |
| mcp路径错误 | 1处 | 🔴 严重 | `mcpServers` vs `mcp.servers` 路径完全不匹配 |
| platformType硬编码 | 1处 | 🔴 关键 | 本次报错根因 |
| config_validate.rs | ~30处 | 🟡 | 验证器schema用camelCase |
| init.rs迁移输出 | 1处 | 🟡 | 生成camelCase新配置 |
| 测试夹具 | ~20处 | 🟡 | JSON全用camelCase |

---

## 二、设计决策

### 统一目标：snake_case

**理由**：
1. Rust struct字段已是snake_case — 零修改
2. serde使用 `#[serde(rename_all = "lowercase")]` — JSON API已是snake_case
3. config-default.yaml 98%已是snake_case（仅2处camelCase）
4. 用户自然使用snake_case（YAML惯例）
5. 改为camelCase需要改50+ struct字段 → JSON API破坏性变更 → 成本3-5倍

### 过渡策略

1. 所有解析器统一使用 `optional_*_dual`（接受两种格式 + deprecation warning）
2. config-default.yaml 100% snake_case
3. 用户已有camelCase配置继续工作（带deprecation warning）
4. 未来版本可移除camelCase支持

---

## 三、实施步骤

### Phase 1: 修复关键bug + config-default.yaml (1h)

**Task 1.1: 修复 `platformType` 解析器**（本次报错根因）

```rust
// crates/runtime/src/config.rs:1886
// 修改前:
platform_type: expect_string(p, "platformType", &ctx)
    .or_else(|_| expect_string(p, "type", &ctx))

// 修改后:
platform_type: optional_string_dual(p, "platform_type", &ctx)?
    .unwrap_or_else(|| "api_server".to_string())
```

**Task 1.2: 修复 `apiUrl`/`apiKey` 双格式参数错误**

```rust
// config.rs:1731-1734 — BUG: 传入camelCase而非snake_case
// 修改前: optional_string_dual(v, "apiUrl", &ctx)?  ← 永远找不到YAML的api_url
// 修改后: optional_string_dual(v, "api_url", &ctx)?
// 同理: "apiKey" → "api_key", "timeoutSecs" → "timeout_secs", "batchSize" → "batch_size"
```

**Task 1.3: config-default.yaml 统一snake_case**

| 当前 | 改为 |
|------|------|
| `defaultMode` | `default_mode` |
| `platformType` | `platform_type` |

---

### Phase 2: 代码解析器全覆盖双格式 (3h)

**Task 2.0 (先执行): 新增缺失的 `_dual` 变体函数**

当前仅有 `optional_u32_dual` 和 `optional_bool_dual`。需要新增：
```rust
fn optional_f32_dual(object, snake_key, ctx) -> Option<f32>
fn optional_u64_dual(object, snake_key, ctx) -> Option<u64>
fn optional_usize_dual(object, snake_key, ctx) -> Option<usize>
```

**Task 2.1: compression段（9处，当前全部静默失效）**

```rust
// parse_optional_compression_config() 中所有调用:
// optional_u32(m, "toolResultMaxChars", ...)  → optional_u32_dual(m, "tool_result_max_chars", ...)
// optional_f32(m, "timeDecayFactor", ...)      → optional_f32_dual(m, "time_decay_factor", ...)
// optional_u32(m, "thresholdTokens", ...)       → optional_u32_dual(m, "threshold_tokens", ...)
// optional_u32(m, "preserveRecent", ...)        → optional_u32_dual(m, "preserve_recent", ...)
// optional_u32(m, "summaryMaxTokens", ...)      → optional_u32_dual(m, "summary_max_tokens", ...)
// optional_u32(m, "bufferTokens", ...)          → optional_u32_dual(m, "buffer_tokens", ...)
// optional_bool(m, "iterativeUpdate", ...)      → optional_bool_dual(m, "iterative_update", ...)
// optional_u32(b, "maxRetries", ...)            → optional_u32_dual(b, "max_retries", ...)
// optional_u32(b, "cooldownSecs", ...)          → optional_u32_dual(b, "cooldown_secs", ...)
//
// 关键: circuitBreaker → circuit_breaker 段键也需要修复:
// let Some(cb) = cmp.get("circuitBreaker")  → let Some(cb) = find_key_dual(cmp, "circuit_breaker", "compression")
```

**Task 2.2: memory段（4处）**

```rust
// optional_bool(m, "aaakIndexEnabled", ...)  → optional_bool_dual(m, "aaak_index_enabled", ...)
// optional_u32(m, "coherenceThreshold", ...) → optional_u32_dual(m, "coherence_threshold_bp", ...)
// optional_bool(e, "autoExtract", ...)       → optional_bool_dual(e, "auto_extract", ...)
```

**Task 2.3: sandbox/plugins/vector段（7处）**

```rust
// 1. optional_string_array(s, "allowedMounts", ...)        → optional_string_array_dual(s, "allowed_dirs", ...)
// 2. optional_string_array(plugins, "externalDirectories", ...) → optional_string_array_dual(plugins, "external_dirs", ...)
// 3. timeoutSecs → timeout_secs (已在Phase 1.2修复)
// 4. batchSize → batch_size (已在Phase 1.2修复)
// 5. optional_bool(sandbox, "namespaceRestrictions", ...)   → optional_bool_dual(sandbox, "namespace_restrictions", ...)
// 6. optional_bool(sandbox, "networkIsolation", ...)       → optional_bool_dual(sandbox, "network_isolation", ...)
// 7. optional_u32(plugins, "maxOutputTokens", ...)          → optional_u32_dual(plugins, "max_output_tokens", ...)
```

**Task 2.4: 删除旧Task 2.4（已移至2.0）**

**Task 2.5: 🔴 MCP路径修复（关键遗漏）**

```rust
// config.rs parse_mcp_config() — 同时检查两个路径:
// let servers = root.get("mcpServers")
//     .or_else(|| root.get("mcp").and_then(|m| m.get("servers")));

// 同时在 config_validate.rs 中添加 "mcp" 到 TOP_LEVEL_FIELDS:
// "mcp" → MCP_SERVERS_FIELDS 子字段验证
```

---

### Phase 3: config_validate.rs + init.rs + 测试同步 (1.5h)

**Task 3.1: config_validate.rs 同步（~30处FieldSpec）**

验证器当前使用camelCase键名。需要新增snake_case变体或改为 `find_key_dual`：
```rust
// 每个 FieldSpec 需要添加 snake_case 变体:
FieldSpec::new("clientId", FieldType::String, false, ...)
// 新增:
FieldSpec::new("client_id", FieldType::String, false, ...)
```

**Task 3.2: init.rs 修复**
```rust
// crates/cowd-cli/src/init.rs:119
// 修改前: writeln!(file, "defaultMode: acceptEdits")?;
// 修改后: writeln!(file, "default_mode: acceptEdits")?;
```

**Task 3.3: 测试夹具更新（~20处）**

搜索所有测试JSON字符串中的camelCase配置键，改为snake_case。如：
```rust
// config.rs test: "clientId" → "client_id"
// config.rs test: "authorizeUrl" → "authorize_url"
// executor.rs test: "defaultMode" → "default_mode"
```

---

### Phase 4: 全量回归验证 (1h)

```bash
cargo build --workspace
cargo test -p cowd-memory      # 330+ pass (不变)
cargo test -p runtime          # 980+ pass
cargo test --test integration_tests  # 20 pass
cowd version                    # ✅
cowd --help                     # ✅
```

---

## 四、TDD测试规格

### T1: 双格式兼容性（compression段）

```rust
#[test]
fn test_snake_case_compression_parses() { /* ... */ }

#[test]
fn test_camel_case_compression_backward_compat() { /* ... */ }
```

### T2: platformType 双格式

```rust
#[test]
fn test_platform_type_snake_case_parses() {
    let yaml = r#"gateway: { platforms: [{ platform_type: api_server, enabled: true }] }"#;
    // 验证: config.platforms[0].platform_type == "api_server"
}

#[test]
fn test_platform_type_camel_case_backward_compat() {
    let yaml = r#"gateway: { platforms: [{ platformType: api_server, enabled: true }] }"#;
    // 验证: config.platforms[0].platform_type == "api_server"
}
```

### T3: memory段双格式

```rust
#[test]
fn test_memory_aaak_index_enabled_snake_case_parses() {
    let yaml = r#"memory: { aaak_index_enabled: false }"#;
    // 验证: config.aaak_index_enabled == false
}
```

### T4: circuit_breaker段键双格式

```rust
#[test]
fn test_circuit_breaker_snake_case_section_key_parses() {
    let yaml = r#"compression: { circuit_breaker: { max_retries: 5 } }"#;
    // 验证: config.circuit_breaker.max_retries == 5
}
```

```bash
cowd version                          # snake_case配置
COWD_PERMISSION_MODE=acceptEdits cowd version  # 环境变量覆盖
```

---

## 五、验证清单

- [ ] `cowd version` ✅
- [ ] `cowd --solo` 可启动
- [ ] `cowd serve` 可启动
- [ ] compression YAML值生效（非默认值）
- [ ] circuit_breaker YAML值生效
- [ ] `cargo test --workspace` 通过
- [ ] 用户原有 camelCase 配置不报错（deprecation warning）
- [ ] config_validate 不报 "unknown field" 警告
- [ ] `cowd init` 生成 snake_case 配置

---

*方案生成: 2026-06-02 (Oracle审计修正版)*
