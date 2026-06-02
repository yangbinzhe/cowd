# 两套后端运行时深度对比分析报告

> v0.8.7 | 逐行代码审计 | Oracle已验证数据

---

## 一、各自独立功能

### TUI独有（daemon完全没有）

| 能力 | 代码位置 | 说明 |
|------|---------|------|
| `stream_callback` (mpsc SyncSender) | `main.rs:7171` | TUI通过sync_channel接收实时CowdEvent |
| `ToolCallback` | `main.rs:7182-7183` | TUI获取工具启动/进度/完成事件 |
| `HookProgressReporter` | `main.rs:7185-7186` | TUI获取hook执行状态 |
| `CowdEventBus` | `main.rs:7188-7189` | 广播通道 — **daemon也有，但TUI额外订阅** |
| 工具审批 (`CliPermissionPrompter`) | `main.rs:7255` | TUI交互式工具审批 |
| 用户交互 (TUI渲染、键盘输入) | `tui/state.rs`, `tui/app.rs` | 整个TUI层 |
| `emit_output: true` | `main.rs:7168` | TUI需要控制台输出 |
| 静态文件服务 (WebUI) | — | TUI无此功能 |

### Daemon独有（TUI完全没有）

| 能力 | 代码位置 | 说明 |
|------|---------|------|
| HTTP API Server (:8642) | `daemon.rs:247-301` | axum HTTP + SSE + WebSocket |
| Unix Socket Listener | `daemon.rs:222` | TUI可以通过socket直连 |
| 平台适配器 (飞书/企微/邮件) | `daemon.rs:231-290` | 外部消息平台接入 |
| ActiveSessions管理 | `daemon.rs:134` | 多会话并发管理 |
| Session生命周期 | `daemon.rs:154-159` | 空闲超时/TTL自动清理 |
| 后台session清理任务 | `daemon.rs:155` | SessionLifecycle reaper |
| 静态文件服务 (WebUI) | `daemon.rs` 配置 | `cowd serve` 捆绑WebUI |
| `emit_output: false` | `daemon.rs:419` | Daemon不需要控制台输出 |

---

## 二、重复建设（完全相同，双份运行）

| 能力 | TUI | Daemon | 说明 |
|------|-----|--------|------|
| **Plugin初始化** | ✅ `build_runtime_plugin_state()` | ✅ 同路径 | 插件发现+聚合hooks |
| **MCP工具发现** | ✅ `build_runtime_mcp_state()` | ✅ 同路径 | 连接所有MCP服务器 |
| **ToolRegistry** | ✅ `GlobalToolRegistry::builtin()` | ✅ 同路径 | 50+内置工具 |
| **ConversationRuntime** | ✅ `new_with_features()` | ✅ 同路径 | 核心对话引擎 |
| **AnthropicRuntimeClient** | ✅ 新建 | ✅ 新建 | API客户端 |
| **PermissionPolicy** | ✅ 新建 | ✅ 新建 | 权限策略 |
| **SystemPrompt构建** | ✅ 完整 | ✅ 完整 | 项目上下文+指令文件 |
| **MemoryConfig解析** | ✅ | ✅ | 内存配置 |

**结论：8项基础设施完全重复。每次TUI启动都重建一次，daemon也重建一次。无人共享。**

---

## 三、真正共享的能力（只有一份）

| 能力 | 位置 | 说明 |
|------|------|------|
| `CognitiveContextManager` | `daemon.rs:137-146` | Daemon创建一个，但**TUI也自建一个** — 实际双份 |
| `UnifiedSessionStore` | `daemon.rs:150` | Session持久化 — daemon有，TUI无 |
| `SessionEventBus` | `daemon.rs:148` | SSE事件总线 — daemon有，TUI用CowdEventBus |

**结论：名义上共享，实际各建各的。零真正共享。**

---

## 四、量化对比

| 指标 | TUI | Daemon | 共享比例 |
|------|-----|--------|---------|
| 独立能力 | 6项 | 6项 | 0% |
| 重复建设 | 8项 | 8项 | 0%（各自独立初始化） |
| 可共享但未共享 | 1项 | 1项 | 0% |
| **总计** | **15项** | **15项** | **0%** |

---

## 五、统一方案

### 核心理念

**daemon是唯一Runtime宿主，TUI是纯视图层。** 

消除TUI的`build_runtime()`调用，改为通过Unix Socket连接daemon。

### 架构

```
daemon (cowd serve) — 唯一Runtime宿主:
┌────────────────────────────────────────────────────────────┐
│ ActiveSessions → ConversationRuntime (每个session一个)     │
│   ├── with_cowd_event_bus() ← 广播所有事件                │
│   ├── with_collaboration() ← 多Agent协作                  │
│   ├── with_jps_pipeline() ← 联合求解                      │
│   ├── Plugin+MCP Tools ← 工具注册                         │
│   └── CognitiveContextManager ← 内存系统                  │
│                                                            │
│ HTTP API (:8642) ← WebUI/外部调用                         │
│ Unix Socket (/tmp/cowd.sock) ← TUI连接                    │
│   命令: create_session, chat_stream, tool_approve/deny     │
└────────────────────────────────────────────────────────────┘
         ↕ Unix Socket (JSON Lines, 双向)
TUI (cowd) — 纯视图层:
┌────────────────────────────────────────────────────────────┐
│ DaemonClient → UnixStream连接                             │
│   接收: TextDelta/ToolStart/ToolComplete/TurnComplete      │
│   发送: chat_stream / tool_approve / tool_deny             │
│   ↓                                                        │
│ CowdEvent → state.apply_event() → 渲染                    │
│                                                            │
│ 回退: daemon不可用 → 自建runtime (保留现有路径)            │
└────────────────────────────────────────────────────────────┘
```

### TUI改动

**删除**: `LiveCli::new()` → `build_runtime()` → `prepare_turn_runtime()` — 整条8项重复建设的初始化链
**新增**: `DaemonClient::connect()` → Unix Socket直连

### Daemon改动

**新增3项TUI独有能力**:
1. `CowdEventBus`订阅 — 向socket客户端转发所有事件
2. `ToolCallback`注入 — 工具进度通过socket实时推送到TUI
3. 工具审批协议 — `tool_approve`/`tool_deny`双向命令

### 功能零损失

| 能力 | 统一前 | 统一后 | 实现 |
|------|--------|--------|------|
| 内存系统 | TUI自建 | daemon提供 | CognitiveContextManager在daemon |
| 流式输出 | TUI sync_channel | socket JSON Lines | chat_stream命令 |
| 工具进度 | ToolCallback | socket事件 | ToolStart/Progress/Complete |
| 工具审批 | CliPermissionPrompter | tool_approve协议 | 双向命令 |
| 多Agent协作 | TUI自建 | daemon提供 | 复用daemon runtime |
| 会话恢复 | TUI自建 | resume_session | daemon持久化 |
| 回退 | — | daemon不可用时自建 | 保留现有路径 |
| WebUI | daemon独有 | 不变 | 继续HTTP API |

---

## 六、工期

| 阶段 | 内容 | 工期 |
|------|------|------|
| Phase A | daemon新增CowdEventBus转发+ToolCallback+审批协议 | 6h |
| Phase B | TUI DaemonClient (socket连接+事件接收+命令发送) | 4h |
| Phase C | TUI接入 (移除build_runtime + 回退逻辑) | 3h |
| Phase D | 全量回归测试 | 3h |
| **总计** | | **16h** |
