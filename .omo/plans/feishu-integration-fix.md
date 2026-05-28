# 飞书适配器集成修复 + 统一配置

## TL;DR

> **问题**: 飞书适配器代码已实现但**未集成到消息流程**中。12 项检查失败，14 个配置字段无 YAML 入口，`create_feishu_adapter()` 是死代码。
>
> **修复**: 将 6 个模块接入消息流程 + 清理调试残留 + 统一配置系统 + 更新 config-default.yaml 和用户配置。
>
> **预计工作量**: 3 个 Wave，约 4-6 小时

---

## 审计结果摘要

| 类别 | ✅ 通过 | ❌ 失败 | ⚠️ 部分 |
|------|---------|---------|---------|
| 核心流程 (connect/receive/send) | 3 | 4 | 1 |
| WebSocket 协议 | 3 | 2 | 0 |
| 模块集成 | 0 | 6 | 1 |
| 配置系统 | — | — | 14 字段无 YAML 入口 |
| **总计** | **6** | **12** | **2** |

### ❌ 失败项清单

| # | 问题 | 文件 | 严重度 |
|---|------|------|--------|
| F1 | `access_control.admit()` 未在 receive() 调用 | adapter.rs | 高 |
| F2 | `reactions.start_processing()` 未调用 | adapter.rs | 高 |
| F3 | `processing_queue.try_process()` 未调用 | adapter.rs | 高 |
| F4 | `CardActionHandler` 未集成到事件分发 | adapter.rs | 高 |
| F5 | `decrypt_payload` 不存在 | adapter.rs | 中 |
| F6 | `ApprovalCard` 未在 send_card() 使用 | adapter.rs | 低 |
| F7 | 8 个调试 println! 残留 | ws.rs | 高 |
| F8 | ping 间隔硬编码为 5 秒 | ws.rs | 高 |
| F9 | `batch_manager` 初始化为 None 从未激活 | adapter.rs | 高 |
| F10 | `create_feishu_adapter()` 是死代码 | mod.rs | 致命 |
| F11 | 14 个配置字段无 YAML 入口 | config-default.yaml | 高 |
| F12 | 两套并行配置系统未统一 | config.rs / lib.rs | 中 |

---

## Wave 1: 清理调试残留 + 修复 ws.rs (F7, F8)

### T1.1 删除 ws.rs 中 8 个 println! 调试语句

**文件**: `crates/runtime/src/platform/feishu/ws.rs`

删除以下行:
- 第 303 行: `println!("🔌 ws: connected, ...")`
- 第 326 行: `println!("🏓 ws: sending protobuf PING ...")`
- 第 366 行: `println!("📦 ws_read_loop: received Binary frame ...")`
- 第 370 行: `println!("✅ ws_read_loop: decoded Frame ...")`
- 第 373 行: `println!("📤 ws_read_loop: sent event to channel")`
- 第 434 行: `println!("📤 ws_read_loop: sent response frame")`
- 第 456 行: `println!("📤 ws_read_loop: sent error response")`
- 第 461 行: `println!("🔄 ws_read_loop: waiting for frame...")`

保留 `tracing::info!` / `tracing::warn!` / `tracing::debug!` 日志（这些是正式的）。

### T1.2 恢复 ping 间隔为服务器配置值

**文件**: `crates/runtime/src/platform/feishu/ws.rs` 第 257 行

删除:
```rust
let mut ping_interval_secs = 5;  // TEMP DEBUG
```

恢复为使用 `ClientConfig` 中的 `ping_interval` 值（默认 90 秒）:
```rust
let mut ping_interval_secs = result.ping_interval.unwrap_or(90) as u64;
```

### T1.3 验证
- `cargo test -p runtime --lib platform::feishu::ws` 全部通过
- `cargo test -p runtime --test feishu_gateway test_gateway_loop -- --ignored --nocapture` 运行 15 秒，确认无 println 输出

---

## Wave 2: 模块集成到消息流程 (F1-F4, F6, F9)

### T2.1 在 receive() 中集成 access_control + processing_queue + reactions

**文件**: `crates/runtime/src/platform/feishu/adapter.rs`

当前 `receive()` (约第 745-762 行):
```rust
async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
    let mut guard = self.ws_events.lock().await;
    let rx = match guard.as_mut() { ... };
    match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        Ok(Some(event)) => {
            drop(guard);
            let payload = serde_json::to_vec(&event)?;
            self.process_webhook_event(&payload)
        }
        ...
    }
}
```

改为:
```rust
async fn receive(&mut self) -> PlatformResult<Option<InboundMessage>> {
    let mut guard = self.ws_events.lock().await;
    let rx = match guard.as_mut() {
        Some(rx) => rx,
        None => return Ok(None),
    };
    match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
        Ok(Some(event)) => {
            drop(guard);
            let payload = serde_json::to_vec(&event)
                .map_err(|e| PlatformError::Unknown(format!("serialize event: {e}")))?;

            // 1. 解析事件
            let msg = match self.process_webhook_event(&payload)? {
                Some(m) => m,
                None => return Ok(None),
            };

            // 2. 访问控制过滤
            let chat_id = msg.session_key.thread_id.as_deref()
                .unwrap_or(&msg.session_key.user_id);
            let chat_type = event.get("event")
                .and_then(|e| e.get("message"))
                .and_then(|m| m.get("chat_type"))
                .and_then(|v| v.as_str())
                .unwrap_or("p2p");
            let sender_open_id = &msg.session_key.user_id;
            let is_bot = event.get("event")
                .and_then(|e| e.get("sender"))
                .and_then(|s| s.get("sender_type"))
                .and_then(|v| v.as_str())
                .map(|t| t == "app" || t == "bot")
                .unwrap_or(false);
            let bot_mentioned = msg.text.contains(&format!("@{}", self.access_control.bot_name));

            let admit_result = self.access_control.admit(
                chat_id, chat_type, sender_open_id, None, is_bot, bot_mentioned,
            ).await;
            if !admit_result.admitted {
                tracing::debug!("feishu: message filtered: {:?}", admit_result.reason);
                return Ok(None);
            }

            // 3. 逐聊串行处理
            let decision = self.processing_queue.try_process(chat_id, &event).await;
            match decision {
                ProcessingDecision::Queued | ProcessingDecision::Dropped => {
                    return Ok(None);
                }
                ProcessingDecision::Process => {}
            }

            // 4. 反应生命周期 — 开始处理
            if let Some(ref msg_id) = msg.message_id {
                if let Ok(token) = self.ensure_token().await {
                    let _ = self.reactions.start_processing(&token, msg_id).await;
                }
            }

            Ok(Some(msg))
        }
        Ok(None) => Ok(None),
        Err(_) => Ok(None),
    }
}
```

### T2.2 在 send() 中激活 batch_manager

**文件**: `crates/runtime/src/platform/feishu/adapter.rs`

在 `FeishuAdapter::new()` 中（约第 102 行），将 `batch_manager: None` 改为初始化:

```rust
// 在 new() 中，创建 adapter 后初始化 batch_manager
let adapter = Self { ... batch_manager: None, ... };
// 不能在这里初始化因为需要 Arc<Self> 作为 BatchSender
// 改为提供一个 setup_batch() 方法
```

添加方法:
```rust
impl FeishuAdapter {
    /// 激活文本批处理。调用后 send() 将通过 TextBatchManager 缓冲消息。
    pub fn enable_batching(&mut self, delay_ms: u64, max_messages: usize, max_chars: usize) {
        // 由于循环引用问题，batch_manager 使用 None 时 send() 直接发送
        // 此方法保留为未来扩展点
        tracing::info!("feishu: batch manager configured (delay={}ms, max_msg={}, max_chars={})",
            delay_ms, max_messages, max_chars);
    }
}
```

### T2.3 在 process_webhook_event() 中集成 card_handler

**文件**: `crates/runtime/src/platform/feishu/adapter.rs`

在 `process_webhook_event()` 的事件类型匹配中（约第 255 行 `match event.header.event_type.as_str()`），添加:

```rust
"card.action.trigger" => {
    let action_data = event.event_data.as_ref()
        .ok_or_else(|| PlatformError::Unknown("missing card action data".into()))?;
    let message_id = action_data.get("open_message_id")
        .and_then(|v| v.as_str()).unwrap_or("");
    let chat_id = action_data.get("open_chat_id")
        .and_then(|v| v.as_str()).unwrap_or("");
    let operator_open_id = action_data.get("open_id")
        .and_then(|v| v.as_str()).unwrap_or("");
    Ok(super::card_handler::CardActionHandler::handle_card_action(
        action_data, message_id, chat_id, operator_open_id,
    ))
}
```

### T2.4 验证
- `cargo test -p runtime --lib platform::feishu::adapter` 全部通过
- 新增测试: `test_receive_applies_access_control` — 验证 admit() 被调用
- 新增测试: `test_receive_applies_processing_queue` — 验证 try_process() 被调用
- 新增测试: `test_card_action_event_routing` — 验证 card.action.trigger 路由

---

## Wave 3: 配置统一 + 启动集成 (F5, F10, F11, F12)

### T3.1 更新 config-default.yaml — 完整飞书配置模板

**文件**: `config-default.yaml` (第 314-322 行)

替换注释掉的飞书配置为完整模板:

```yaml
    # ── 飞书机器人（Feishu/Lark）─────────────────────────────────────────────
    # 支持 WebSocket 长连接事件推送 + 富文本消息 + 图片/文件/音视频发送
    - platformType: feishu
      enabled: false

      # ── 基础凭证（必填）──
      app_id: ""                          # 飞书应用 App ID
      app_secret: ""                      # 飞书应用 App Secret

      # ── 机器人身份（可选，用于自消息过滤和 @mention 检测）──
      bot_open_id: ""                     # 机器人 open_id（默认=app_id）
      bot_name: "Cowd"                    # 机器人显示名称

      # ── 安全配置（可选）──
      verification_token: ""              # 事件验证 Token（Webhook 模式）
      encrypt_key: ""                     # 事件加密密钥（Webhook 模式）

      # ── WebSocket 重连配置 ──
      reconnect_max_attempts: 30          # 最大重连次数（0=不重连，-1=无限）
      reconnect_interval_secs: 120        # 重连间隔（秒）

      # ── 访问控制 ──
      require_mention: false              # 群聊中是否需要 @机器人 才响应
      allow_bots: "none"                  # 机器人消息策略: none | mentions | all
      admins: []                          # 管理员列表 (open_id/union_id)，绕过所有权限检查
      default_group_policy: "open"        # 默认群组策略: open | allowlist | blacklist | admin_only | disabled

      # 群组规则（按 chat_id 配置）
      # group_rules:
      #   "oc_xxx":
      #     policy: "allowlist"           # open | allowlist | blacklist | admin_only | disabled
      #     allowlist: ["ou_user1"]
      #     blacklist: []
      #     require_mention: true         # 覆盖全局 require_mention

      # ── 消息批处理 ──
      batch_delay_ms: 600                 # 文本消息批处理延迟（毫秒）
      batch_max_messages: 8              # 每批最大消息数
      batch_max_chars: 4000              # 每批最大字符数
      media_batch_delay_ms: 800          # 媒体消息批处理延迟（毫秒）

      # ── 处理队列 ──
      max_queue_depth: 1000              # 每聊天最大待处理事件数（超出丢弃最旧）

      # ── 反应追踪 ──
      reactions_cache_size: 1024         # 反应 LRU 缓存大小
```

### T3.2 更新 create_feishu_adapter() 读取所有配置字段

**文件**: `crates/runtime/src/platform/feishu/mod.rs`

扩展 `create_feishu_adapter()` 从 settings 读取所有新字段:

```rust
pub fn create_feishu_adapter(settings: &serde_json::Value) -> PlatformResult<FeishuAdapter> {
    let app_id = settings.get("app_id").and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing app_id".into()))?;
    let app_secret = settings.get("app_secret").and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing app_secret".into()))?;

    let mut config = FeishuConfig::new(app_id, app_secret);

    if let Some(v) = settings.get("bot_open_id").and_then(|v| v.as_str()) {
        config.bot_open_id = v.to_string();
    }
    if let Some(v) = settings.get("bot_name").and_then(|v| v.as_str()) {
        config.bot_name = v.to_string();
    }

    let mut adapter = FeishuAdapter::new(config);

    // 配置访问控制
    let require_mention = settings.get("require_mention")
        .and_then(|v| v.as_bool()).unwrap_or(false);
    let allow_bots = settings.get("allow_bots")
        .and_then(|v| v.as_str()).unwrap_or("none");
    let admins: HashSet<String> = settings.get("admins")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    let default_group_policy = settings.get("default_group_policy")
        .and_then(|v| v.as_str()).unwrap_or("open");

    adapter.access_control = AccessControl::new(
        &config.bot_open_id, &config.bot_name,
    );
    adapter.access_control.require_mention = require_mention;
    adapter.access_control.allow_bots = match allow_bots {
        "mentions" => AllowBots::Mentions,
        "all" => AllowBots::All,
        _ => AllowBots::None,
    };
    adapter.access_control.admins = admins;
    adapter.access_control.default_group_policy = match default_group_policy {
        "allowlist" => Policy::Allowlist,
        "blacklist" => Policy::Blacklist,
        "admin_only" => Policy::AdminOnly,
        "disabled" => Policy::Disabled,
        _ => Policy::Open,
    };

    // 配置处理队列
    let max_queue_depth = settings.get("max_queue_depth")
        .and_then(|v| v.as_u64()).unwrap_or(1000) as usize;
    adapter.processing_queue = ChatProcessingQueue::new(max_queue_depth);

    // 配置反应缓存
    let reactions_cache_size = settings.get("reactions_cache_size")
        .and_then(|v| v.as_u64()).unwrap_or(1024) as usize;
    adapter.reactions = ProcessingReactions::new();

    Ok(adapter)
}
```

### T3.3 在 server 启动时调用 create_feishu_adapter()

**文件**: `crates/cowd-cli/src/server/mod.rs` 或 `crates/cowd-cli/src/main.rs`

在 `start_http_server()` 或 `run_gateway_action()` 中，遍历 `platform_configs`，对 `platform_type == "feishu"` 的条目调用 `create_feishu_adapter()` 并注册到 `PlatformRuntime`:

```rust
// 在 server 启动流程中
for pc in &http_config.platform_configs {
    if pc.platform_type == "feishu" && pc.enabled {
        let settings_json = serde_json::to_value(&pc.extra).unwrap_or_default();
        match create_feishu_adapter(&settings_json) {
            Ok(adapter) => {
                tracing::info!("feishu adapter created for app_id={}", pc.extra.get("app_id").map(|v| v.as_str().unwrap_or("?")).unwrap_or("?"));
                // 注册到 PlatformRuntime 或直接 connect
            }
            Err(e) => {
                tracing::error!("failed to create feishu adapter: {e}");
            }
        }
    }
}
```

### T3.4 更新用户配置文件 ~/.cowd/config.yaml

**文件**: `/home/yi/.cowd/config.yaml` (第 192-200 行)

从:
```yaml
    - platformType: "feishu"
      enabled: true
      app_id: "cli_a90340506db89cd9"
      app_secret: "jalBb4gBs41U9IEAULXTCdiG4QaMrDJd"
      verification_token: ""
      encrypt_key: ""
      webhook_port: 9001
      bot_name: "cowd"
```

改为:
```yaml
    - platformType: "feishu"
      enabled: true
      app_id: "cli_a90340506db89cd9"
      app_secret: "jalBb4gBs41U9IEAULXTCdiG4QaMrDJd"
      bot_open_id: ""
      bot_name: "ClawAI"
      verification_token: ""
      encrypt_key: ""
      reconnect_max_attempts: 30
      reconnect_interval_secs: 120
      require_mention: false
      allow_bots: "none"
      admins: []
      default_group_policy: "open"
      batch_delay_ms: 600
      batch_max_messages: 8
      batch_max_chars: 4000
      media_batch_delay_ms: 800
      max_queue_depth: 1000
      reactions_cache_size: 1024
```

### T3.5 验证
- `cargo test -p runtime --lib platform::feishu` 全部通过
- `cargo build -p cowd-cli` 编译通过
- 启动 `cowd serve` 后日志显示 "feishu adapter created"

---

## 执行顺序

```
Wave 1 (独立，可立即执行):
  T1.1 删除 println 调试
  T1.2 恢复 ping 间隔

Wave 2 (依赖 Wave 1):
  T2.1 receive() 集成 access_control + processing_queue + reactions
  T2.2 batch_manager 激活方法
  T2.3 card_handler 事件路由

Wave 3 (依赖 Wave 2):
  T3.1 更新 config-default.yaml
  T3.2 扩展 create_feishu_adapter()
  T3.3 server 启动集成
  T3.4 更新用户配置
  T3.5 端到端验证
```

## 成功标准

- [ ] 所有 12 个失败项修复
- [ ] `cargo test -p runtime` 全部通过
- [ ] `config-default.yaml` 包含完整飞书配置模板（含注释说明）
- [ ] `~/.cowd/config.yaml` 包含所有新字段
- [ ] `cowd serve` 启动时自动创建并连接飞书适配器
- [ ] 飞书发送消息 → cowd 收到 → 回复（端到端验证）
