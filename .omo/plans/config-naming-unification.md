# 配置命名全局统一方案 (TDD)

> 问题：配置键名在代码(camelCase)与YAML(snake_case)间严重不一致，导致运行时解析失败
> 目标：统一为 snake_case，保持向后兼容，TDD验证
> 工期：2-3天 | 风险：中-高（影响所有入口点）

---

## 一、问题全景

### 1.1 当前状态

代码中存在**三种**不一致模式：

| 模式 | 代码期望 | YAML键名 | 示例 | 影响 |
|------|---------|---------|------|------|
| **A. 纯camelCase** | `platformType` | snake_case `platform_type` | ❌ 报错 | `expect_string`单格式 |
| **B. 双格式桥接** | `apiKey`/`api_key` | 两种均可 | ⚠️ 混乱 | `optional_string_dual` |
| **C. 纯snake_case** | `session_reset` | `session_reset` | ✅ 一致 | `optional_string_dual` |

### 1.2 根因

- `config-default.yaml` 混合使用 camelCase 和 snake_case
- 代码中部分解析器用 `expect_string`（仅camelCase），部分用 `optional_string_dual`（双格式）
- 双格式桥接是后期加的补丁，未全覆盖
- 用户本地配置使用 snake_case（YAML惯例）
- `platformType` 解析器未经过双格式桥接 → 本次报错的直接原因

### 1.3 不一致清单

**代码侧的camelCase（共约40+个）**:

| camelCase (代码) | snake_case (YAML) | 是否双格式？ |
|-----------------|------------------|-------------|
| `platformType` | `platform_type` | ❌ 否 |
| `apiKey` | `api_key` | ✅ dual |
| `apiUrl` | `api_url` | ❌ 否 |
| `clientId` | `client_id` | ❌ 否 |
| `authorizeUrl` | `authorize_url` | ❌ 否 |
| `tokenUrl` | `token_url` | ❌ 否 |
| `authServerMetadataUrl` | `auth_server_metadata_url` | ❌ 否 |
| `manualRedirectUrl` | `manual_redirect_url` | ❌ 否 |
| `toolCallTimeoutMs` | `tool_call_timeout_ms` | ❌ 否 |
| `externalDirectories` | `external_dirs` | ❌ 否 |
| `aaakIndexEnabled` | `aaak_index_enabled` | ❌ 否 |
| `autoExtract` | `auto_extract` | ❌ 否 |
| `iterativeUpdate` | `iterative_update` | ❌ 否 |
| `defaultMode` | `default_mode` | ❌ 否 |
| `auto_pass_low_risk` | `auto_pass_low_risk` | ✅ dual |
| `auto_pass_read_only` | `auto_pass_read_only` | ✅ dual |
| `solo_honor_critical` | `solo_honor_critical` | ✅ dual |
| `solo_mode` | `solo_mode` | ✅ dual |
| `session_reset` | `session_reset` | ✅ dual |
| `store_path` | `store_path` | ✅ dual |
| `timeoutSecs` | `timeout_secs` | ❌ 否 |
| `trusted_roots` | `trusted_roots` | ✅ dual |
| `networkIsolation` | `network_isolation` | ❌ 否 |
| `filesystemMode` | `filesystem_mode` | ❌ 否 |
| `installRoot` | `install_root` | ✅ dual |
| `registryPath` | `registry_path` | ✅ dual |
| `bundledRoot` | `bundled_root` | ✅ dual |
| `maxAttempts` | `max_attempts` | ❌ 否 |
| `maxRetries` | `max_retries` | ❌ 否 |
| `maxOutputTokens` | `max_output_tokens` | ❌ 否 |
| `preserveRecent` | `preserve_recent` | ❌ 否 |
| `thresholdTokens` | `threshold_tokens` | ❌ 否 |

---

## 二、解决方案

### 设计决策：统一使用 snake_case

**理由**:
1. YAML 业界惯例是 snake_case
2. config-default.yaml 已大量使用 snake_case
3. 双格式桥接代码用 snake_case 作为主键
4. 用户习惯在 YAML 中使用 snake_case

**过渡策略**: 代码同时接受两种格式（`optional_string_dual` 全覆盖），config-default.yaml 统一为 snake_case。未来版本移除 camelCase 支持。

### 原则
- **零破坏**: 现有 camelCase 配置文件继续工作
- **默认统一**: config-default.yaml 100% snake_case
- **全覆盖**: 所有解析器统一使用 `optional_string_dual`
- **TDD**: 每个修改前先写测试验证两种格式均能解析

---

## 三、TDD 测试计划

### T1: 双向兼容性测试

```rust
// crates/runtime/tests/config_tests.rs (新增)

#[test]
fn test_snake_case_config_parses() {
    let yaml = r#"
gateway:
  platforms:
    - platform_type: api_server
      enabled: true
      host: "127.0.0.1"
      port: 8642
"#;
    let config: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.platforms[0].platform_type, "api_server");
}

#[test]
fn test_camel_case_config_parses() {
    let yaml = r#"
gateway:
  platforms:
    - platformType: api_server
      enabled: true
      host: "127.0.0.1"
      port: 8642
"#;
    let config: GatewayConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.platforms[0].platform_type, "api_server");
}
```

### T2: 所有入口点测试

```bash
# CLI 基本命令
cowd version        # snake_case 配置下
cowd --help         # 无配置情况下
cowd serve          # Server模式

# 环境变量覆盖
COWD_PERMISSION_MODE=acceptEdits cowd version

# 本地配置覆盖
# 创建 .cowd/config.local.yaml 带 snake_case 配置 → 验证合并
```

---

## 四、实施步骤

### Phase 1: config-default.yaml 统一 (1h, quick)

**修改文件**: `config-default.yaml`

将所有残留的 camelCase 键名改为 snake_case：

| 当前 (camelCase) | 改为 (snake_case) |
|-----------------|------------------|
| `platformType` | `platform_type` |
| `external_dirs` | `external_dirs` ✅ 已经snake |
| `defaultMode` | `default_mode` |
| `soloMode` | `solo_mode` |
| `enabledPlugins` | `enabled_plugins` |
| etc... | |

**TDD**: T1 测试在此阶段通过

### Phase 2: 代码双格式解析器全覆盖 (3h, deep)

**修改文件**: `crates/runtime/src/config.rs`

将所有的 `expect_string` / `optional_string` 单格式解析替换为 `optional_string_dual`：

```rust
// 修改前:
platform_type: expect_string(p, "platformType", &ctx)
    .or_else(|_| expect_string(p, "type", &ctx))

// 修改后:
platform_type: optional_string_dual(p, "platform_type", &ctx)?
    .ok_or_else(|| ConfigError::Parse(format!("{ctx}: missing platform_type")))?
```

逐项替换清单（约 30 处）:
- `platformType` → `platform_type`
- `apiKey` → `api_key`
- `apiUrl` → `api_url`
- `clientId` → `client_id`
- 等等

**原则**: 
1. 保留 `to_camel_case` 转换逻辑（向后兼容）
2. `optional_string_dual` 以 snake_case 为主键
3. 错误消息使用 snake_case 名称

### Phase 3: 用户配置自动迁移 (2h, deep)

**新增工具**: 配置迁移脚本

```bash
# scripts/migrate_config.sh — 将用户配置从 camelCase 迁移到 snake_case
cowd doctor --migrate-config   # 新增子命令
```

或简单方案：在配置解析时同时接受两种格式（Phase 2 完成后自动生效），无需显式迁移。

### Phase 4: 全面测试 (2h)

- `cowd version` ✅
- `cowd --solo` ✅  
- `cowd serve` ✅
- `cowd --permission-mode acceptEdits` ✅
- 环境变量覆盖 ✅
- 本地配置合并 ✅
- T1 + T2 测试全部通过 ✅

---

## 五、TDD 验证清单

- [ ] T1: `test_snake_case_config_parses` 通过
- [ ] T1: `test_camel_case_config_parses` 通过
- [ ] T2: `cowd version` 通过
- [ ] T2: `cowd --help` 通过
- [ ] T2: `cowd serve` 启动成功
- [ ] 回归: `cargo test -p cowd-memory` 330+ pass
- [ ] 回归: `cargo test -p runtime` 980+ pass
- [ ] 用户原始 config.yaml 无需修改即可工作

---

*方案生成: 2026-06-02*
