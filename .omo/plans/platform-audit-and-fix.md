# Cowd Platform 模块全盘排查报告

## 一、致命问题（CRITICAL）

### C1. ws.rs — 完全缺少 Protobuf 帧协议实现
**严重程度**: 🔴 致命 — 飞书消息接收完全不可用

**根因分析**:
飞书 WS v2 协议使用 protobuf 序列化的 `Frame` 对象进行所有通信。Python SDK (`lark_oapi`) 的 `client.py` 第 215-216 行：
```python
frame = Frame()
frame.ParseFromString(msg)
```
所有帧（Ping/Pong/Event/Card）都是 protobuf Frame，不是普通 WebSocket 文本/二进制帧。

**当前代码问题** (`ws.rs` 第 291-325 行):
- 将 `Message::Binary` 当作 UTF-8 JSON 解析 → 永远失败
- 将 `Message::Text` 当作 JSON 解析 → 飞书不发 Text 帧
- 将 `Message::Ping` 当作 WebSocket 级别 Ping 处理 → 飞书发的是 protobuf Ping 帧

**Protobuf Frame 定义** (从 `pbbp2.proto` 反编译):
```protobuf
message Header {
  required string key = 1;
  required string value = 2;
}
message Frame {
  required uint64 SeqID = 1;
  required uint64 LogID = 2;
  required int32  service = 3;
  required int32  method = 4;     // 0=CONTROL, 1=DATA
  repeated Header headers = 5;
  optional string payload_encoding = 6;
  optional string payload_type = 7;
  optional bytes  payload = 8;    // 事件 JSON 在这里
  optional string LogIDNew = 9;
}
```

**Header 关键字段**:
- `type`: "event" / "card" / "ping" / "pong"
- `message_id`: 消息唯一 ID
- `trace_id`: 追踪 ID
- `sum`: 总分片数（>1 时需要合包）
- `seq`: 当前分片序号

### C2. ws.rs — 不发送响应帧
**严重程度**: 🔴 致命

Python SDK 在收到 DATA 帧后，必须发送一个响应帧回去（`client.py` 第 280-281 行）：
```python
frame.payload = JSON.marshal(resp).encode(UTF_8)
await self._write_message(frame.SerializeToString())
```
我们的代码不发送任何响应。飞书服务端可能因此停止推送事件。

### C3. ws.rs — 不发送 protobuf Ping 帧
**严重程度**: 🔴 致命

Python SDK 有一个 `_ping_loop` 定期发送 protobuf Ping 帧（`client.py` 第 129-139 行）：
```python
frame = _new_ping_frame(int(self._service_id))
await self._write_message(frame.SerializeToString())
```
我们的代码只处理 WebSocket 级别的 Ping/Pong，不发送 protobuf Ping。飞书服务端可能因为收不到心跳而断开连接。

### C4. ws.rs — Challenge 握手逻辑错误
**严重程度**: 🟡 中等

`ws_read_loop` 中的 `first_message` challenge 逻辑（第 356-365 行）不存在于 WS v2 协议中。Python SDK 不做 challenge 握手。这段代码是多余的，但不会导致问题（因为条件不满足会被跳过）。

## 二、高优先级问题（HIGH）

### H1. adapter.rs — 7 个媒体发送方法是空壳
**文件**: `feishu/adapter.rs` 第 855-875 行、1027 行

以下方法全部返回 `NotImplemented`:
- `send_image` — 未调用 `media::upload_image`
- `send_image_file` — 未调用 `media::upload_image`
- `send_voice` — 未调用 `media::upload_file`
- `send_document` — 未调用 `media::upload_file`
- `send_video` — 未调用 `media::upload_file`
- `send_animation` — 未实现
- `send_card` — 未调用 `approval::ApprovalCard`

`media.rs` 已有完整的 `upload_image()` 和 `upload_file()` 函数，但 adapter 没有调用它们。

### H2. adapter.rs — decrypt_payload 是占位符
**文件**: `feishu/adapter.rs` 第 344-359 行

```rust
fn decrypt_payload(&self, payload: &[u8], _key: &str) -> PlatformResult<Vec<u8>> {
    // ...
    // This is a placeholder - real implementation would use aes crate
    Ok(encrypted)  // 直接返回原始数据，没有解密！
}
```
AES-256-CBC 解密未实现。当飞书启用加密模式时，webhook 事件无法解析。

### H3. 模块间未集成
**文件**: adapter.rs 未使用以下已实现模块:
- `batch.rs` — 文本批处理（已实现但 adapter 不调用）
- `processing.rs` — 逐聊串行处理（已实现但 adapter 不调用）
- `auth.rs` — 群组策略/访问控制（已实现但 adapter 不调用）
- `reactions.rs` — 反应生命周期（已实现但 adapter 不调用）
- `approval.rs` — 审批卡片（已实现但 adapter 不调用）
- `card_handler.rs` — 卡片操作事件（已实现但 adapter 不调用）

这些模块有完整的单元测试，但在 adapter 的消息处理流程中完全没有被调用。

## 三、中等优先级问题（MEDIUM）

### M1. normalize.rs — 3 种消息类型是存根
**文件**: `feishu/normalize.rs`
- 第 649 行: `// 8. merge_forward stub` — 返回固定文本
- 第 666 行: `// 9. share_chat stub` — 返回固定文本
- 第 681 行: `// 10. interactive / card stub` — 返回固定文本

### M2. wechat_ilink.rs — send_image 是空壳
**文件**: `wechat_ilink.rs` 第 600 行
- `send_image` 返回 `NotImplemented`

## 四、低优先级问题（LOW）

### L1. 测试覆盖缺口
- `ws.rs` 的 `reader_loop` 和 `ws_read_loop` 没有单元测试（标注 "requires live WebSocket"）
- `adapter.rs` 的 `receive()` 没有单元测试
- 没有端到端集成测试验证完整的消息收发流程

---

## 五、TDD 执行计划

### Wave 0: Protobuf 帧协议（修复 C1-C3，解决飞书消息接收）

#### T0.1 添加 prost 依赖
- **文件**: `crates/runtime/Cargo.toml`
- **操作**: 添加 `prost = "0.13"` 和 `prost-build = "0.13"` (build-dep)
- **验证**: `cargo check -p runtime` 编译通过

#### T0.2 创建 protobuf Frame 定义
- **文件**: `crates/runtime/src/platform/feishu/proto.rs` (新建)
- **操作**: 
  - 手动定义 `Frame` 和 `Header` 结构（使用 prost derive 宏）
  - 或创建 `pbbp2.proto` 文件并用 `prost-build` 编译
- **结构**:
  ```rust
  #[derive(Clone, PartialEq, prost::Message)]
  pub struct Frame {
      #[prost(uint64, required, tag = "1")]
      pub seq_id: u64,
      #[prost(uint64, required, tag = "2")]
      pub log_id: u64,
      #[prost(int32, required, tag = "3")]
      pub service: i32,
      #[prost(int32, required, tag = "4")]
      pub method: i32,
      #[prost(message, repeated, tag = "5")]
      pub headers: Vec<Header>,
      #[prost(string, optional, tag = "6")]
      pub payload_encoding: Option<String>,
      #[prost(string, optional, tag = "7")]
      pub payload_type: Option<String>,
      #[prost(bytes, optional, tag = "8")]
      pub payload: Option<Vec<u8>>,
      #[prost(string, optional, tag = "9")]
      pub log_id_new: Option<String>,
  }
  
  #[derive(Clone, PartialEq, prost::Message)]
  pub struct Header {
      #[prost(string, required, tag = "1")]
      pub key: String,
      #[prost(string, required, tag = "2")]
      pub value: String,
  }
  ```
- **测试**: 
  - 序列化/反序列化 round-trip 测试
  - 用 Python SDK 生成的 protobuf 二进制数据进行解析测试
- **验证**: `cargo test -p runtime -- feishu::proto` 全部通过

#### T0.3 重写 ws.rs 的 ws_read_loop
- **文件**: `crates/runtime/src/platform/feishu/ws.rs`
- **操作**:
  - 删除旧的 `Message::Text`/`Message::Binary` JSON 解析逻辑
  - 所有 `Message::Binary` 帧用 `Frame::decode()` 解析为 protobuf Frame
  - 根据 `frame.method` 分发: 0=CONTROL, 1=DATA
  - CONTROL 帧: 检查 `type` header，PING/PONG 分别处理
  - DATA 帧: 提取 `payload` 字段作为事件 JSON，通过 channel 发送
  - 处理多分片消息 (`sum > 1` 时合包)
  - 收到 DATA 帧后发送响应帧回去
- **测试**:
  - 用 Python 生成 protobuf 帧 → Rust 解析验证
  - 模拟 DATA 帧 → 验证 channel 收到正确 JSON
  - 模拟多分片帧 → 验证合包正确
- **验证**: `cargo test -p runtime -- feishu::ws` 全部通过

#### T0.4 实现 protobuf Ping 心跳
- **文件**: `crates/runtime/src/platform/feishu/ws.rs`
- **操作**:
  - 在 `connect()` 中启动 ping_loop 后台任务
  - 定期（默认 90s，从 ClientConfig 获取）发送 protobuf Ping 帧
  - Ping 帧: method=0, type=ping, service=service_id
- **测试**: 验证 Ping 帧格式正确
- **验证**: `cargo test -p runtime -- feishu::ws::ping` 通过

#### T0.5 端到端验证
- **文件**: `crates/runtime/tests/feishu_ws_debug.rs`
- **操作**: 用修复后的 ws.rs 连接飞书，发送消息验证收到事件
- **验证**: 在飞书发送 "你好" → 控制台打印收到的事件 JSON

### Wave 1: 媒体发送集成（修复 H1）

#### T1.1 实现 send_image / send_image_file
- **文件**: `feishu/adapter.rs`
- **操作**: 调用 `media::upload_image()` → 用 image_key 发送图片消息
- **测试**: mock HTTP server 验证 upload + send 流程
- **验证**: `cargo test -p runtime -- feishu::adapter::send_image` 通过

#### T1.2 实现 send_voice / send_document / send_video
- **文件**: `feishu/adapter.rs`
- **操作**: 调用 `media::upload_file()` → 用 file_key 发送对应类型消息
- **测试**: mock HTTP server 验证
- **验证**: `cargo test -p runtime -- feishu::adapter::send_media` 通过

#### T1.3 实现 send_card
- **文件**: `feishu/adapter.rs`
- **操作**: 调用 `approval::ApprovalCard::build()` → 发送 interactive 消息
- **测试**: 验证卡片 JSON 格式正确
- **验证**: `cargo test -p runtime -- feishu::adapter::send_card` 通过

### Wave 2: 模块集成（修复 H3）

#### T2.1 集成 auth.rs 到消息接收流程
- **文件**: `feishu/adapter.rs`
- **操作**: 在 `receive()` 或消息处理回调中调用 `AccessControl::admit()` 过滤消息
- **测试**: 验证 allowlist/blacklist/mention 过滤正确
- **验证**: `cargo test -p runtime -- feishu::adapter::auth_integration` 通过

#### T2.2 集成 reactions.rs 到消息处理流程
- **文件**: `feishu/adapter.rs`
- **操作**: 消息处理开始时调用 `start_processing()`，结束时调用 `mark_success()`/`mark_failure()`
- **测试**: 验证 Typing/CrossMark 反应正确设置/清除
- **验证**: `cargo test -p runtime -- feishu::adapter::reaction_integration` 通过

#### T2.3 集成 batch.rs 到消息发送流程
- **文件**: `feishu/adapter.rs`
- **操作**: `send()` 通过 `TextBatchManager` 缓冲消息
- **测试**: 验证批处理延迟和分片正确
- **验证**: `cargo test -p runtime -- feishu::adapter::batch_integration` 通过

#### T2.4 集成 processing.rs 到消息接收流程
- **文件**: `feishu/adapter.rs`
- **操作**: 使用 `ChatProcessingQueue` 实现逐聊串行处理
- **测试**: 验证同一 chat_id 消息串行处理
- **验证**: `cargo test -p runtime -- feishu::adapter::processing_integration` 通过

#### T2.5 集成 card_handler.rs 和 approval.rs
- **文件**: `feishu/adapter.rs`
- **操作**: 在事件分发中处理 card_action_trigger 事件
- **测试**: 验证卡片按钮点击产生 COMMAND 事件
- **验证**: `cargo test -p runtime -- feishu::adapter::card_integration` 通过

### Wave 3: 补全存根（修复 H2, M1, M2）

#### T3.1 实现 decrypt_payload
- **文件**: `feishu/adapter.rs`
- **操作**: 使用 `aes` crate 实现 AES-256-CBC 解密
- **依赖**: 添加 `aes`, `cbc`, `base64` 到 Cargo.toml
- **测试**: 用已知密文验证解密正确
- **验证**: `cargo test -p runtime -- feishu::adapter::decrypt` 通过

#### T3.2 补全 normalize.rs 的 3 种消息类型
- **文件**: `feishu/normalize.rs`
- **操作**: 实现 merge_forward, share_chat, interactive 的完整解析
- **测试**: 用真实飞书消息 JSON 验证
- **验证**: `cargo test -p runtime -- feishu::normalize` 全部通过

#### T3.3 实现 wechat_ilink.rs 的 send_image
- **文件**: `wechat_ilink.rs`
- **操作**: 调用 iLink getuploadurl → 上传 → 发送
- **测试**: mock HTTP server 验证
- **验证**: `cargo test -p runtime -- wechat_ilink::send_image` 通过

### Wave 4: 集成测试和清理

#### T4.1 端到端飞书集成测试
- **文件**: `crates/runtime/tests/feishu_e2e_test.rs`
- **操作**: 完整的消息收发测试（需要飞书凭证）
- **验证**: 发送 "测试" → 收到事件 → 回复 "收到" → 验证回复成功

#### T4.2 清理无用代码
- **文件**: `feishu/adapter.rs`
- **操作**: 删除旧的 `receive_messages()` 长轮询代码（已被 WS 替代）
- **操作**: 删除 challenge 握手逻辑
- **验证**: `cargo test -p runtime` 全部通过

---

## 六、优先级排序

| 优先级 | 任务 | 预计工作量 | 阻塞关系 |
|--------|------|-----------|---------|
| P0 | T0.1-T0.5 (Protobuf 帧协议) | 4-6h | 阻塞所有飞书功能 |
| P1 | T1.1-T1.3 (媒体发送集成) | 2-3h | 依赖 T0 |
| P1 | T2.1-T2.5 (模块集成) | 3-4h | 依赖 T0 |
| P2 | T3.1-T3.3 (补全存根) | 2-3h | 独立 |
| P3 | T4.1-T4.2 (集成测试) | 1-2h | 依赖 T0-T3 |

**总计**: 12-18h 工作量
