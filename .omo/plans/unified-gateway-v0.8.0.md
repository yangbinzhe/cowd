# COWD 统一网关守护进程 — TDD 执行方案

## TL;DR

> **目标版本**: v0.8.0 — 统一网关守护进程
> **核心交付**: 
> - Session SQLite 统一存储，TUI/API 互通
> - 单进程网关守护进程 (cowd gateway start/stop/status)
> - API 完全能力对等: tools/plugins/MCP/memory/config 全部 API 化
> - Session 跨接口切换、打断、继续
> - systemd 服务注册
> **并行执行**: YES — 4 Waves, 最大并行度 6

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                cowd gateway daemon (单进程)                       │
│                                                                  │
│  SHARED_RT (4 workers)                                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              共享后端 (Arc 单例)                            │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │ UnifiedSessionStore (SQLite) ← 所有 Session 单源     │  │  │
│  │  │ ActiveSessions: HashMap<id, Arc<ConversationRuntime>>│  │  │
│  │  │ CognitiveContextManager  ← 统一记忆                  │  │  │
│  │  │ GlobalToolRegistry       ← 50+ 工具                 │  │  │
│  │  │ PluginRegistry           ← 插件管理                  │  │  │
│  │  │ McpState                 ← MCP 服务                  │  │  │
│  │  │ RuntimeConfig            ← 统一配置 (可 API 读写)    │  │  │
│  │  └─────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────┘  │
│           │                           │                          │
│  ┌────────▼────────┐       ┌──────────▼──────────────┐          │
│  │   TUI 前端       │       │    HTTP API 前端         │          │
│  │                  │       │                          │          │
│  │  任何 Session    │       │  POST /sessions/{id}/msg │          │
│  │  /switch <id>    │       │  GET  /sessions          │          │
│  │  /interrupt      │       │  GET  /tools             │          │
│  │  看到全部会话    │       │  GET  /memory            │          │
│  └──────────────────┘       │  GET  /config (读写)     │          │
│                              └──────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Session 统一 (JSONL → SQLite)

### T1. 统一 SessionStore 路径 [session_control.rs + SqliteSessionStore]
- TUI `SessionStore::from_cwd()` → `UnifiedSessionStore::open()`
- 统一路径: `~/.cowd/sessions.db`
- 启动时检测 JSONL 文件 → 自动迁移到 SQLite
- RED: 测试 JSONL 迁移 SQLite
- GREEN: 实现迁移逻辑
- VERIFY: `cargo test -p cowd-memory --lib`

### T2. 删除旧 JSONL SessionStore [session_control.rs]
- 移除 `SessionStore`、`SessionHandle`、`from_cwd` 等 JSONL 路径
- 所有调用点改为 `UnifiedSessionStore`
- VERIFY: `cargo build --release` 0 errors

### T3. Session 迁移命令行 [main.rs]
- `cowd migrate-sessions` → 扫描 ~/.cowd/sessions/projects/*/ → 导入 SQLite
- 显示迁移进度和统计

---

## Phase 2: Gateway 守护进程 + TUI/API 共存

### T4. Gateway CLI 命令 [main.rs]
```
cowd gateway start    → 启动守护进程 (后台化)
cowd gateway stop     → 停止
cowd gateway status   → 查看状态
cowd gateway run      → 前台运行 (systemd 用)
cowd gateway restart  → 重启
```
- PID 文件管理 (已有)
- systemd unit 文件生成: `cowd gateway install-systemd`

### T5. ActiveSessions 管理器 [gateway.rs 新文件]
```rust
pub struct ActiveSessions {
    sessions: Arc<RwLock<HashMap<String, Arc<ConversationRuntime>>>>,
    store: Arc<UnifiedSessionStore>,
    memory: Arc<CognitiveContextManager>,
    tools: Arc<GlobalToolRegistry>,
    plugins: Arc<PluginRegistry>,
    mcp: Arc<McpState>,
}
```
- `get_or_create(id) → Arc<ConversationRuntime>`
- `list() → Vec<SessionInfo>`
- `create() → (id, Arc<ConversationRuntime>)`
- `destroy(id)` — 清理资源
- `interrupt(id)` — 发送 abort 信号

### T6. TUI 接入 ActiveSessions [main.rs]
- TUI 启动时连接 gateway: `cowd` → 创建新 session 或列出已有
- `/session list` → 从 ActiveSessions 读取
- `/session switch <id>` → 切换活跃 session
- `/session new` → 创建新 session
- `/interrupt` → 打断当前 session turn (已有 HookAbortSignal)

### T7. API 路由重写 [mod.rs 精简版]
- 删除旧 `server/mod.rs` (~3000行)
- 新 API 路由 ~200 行, 直接使用 ActiveSessions:
```rust
// 全部路由统一格式:
.post("/api/sessions/:id/messages", |state, body| {
    state.sessions.get_or_create(id).run_turn_async(body)
})
.get("/api/sessions", |state| state.sessions.list())
.get("/api/tools", |state| state.tools.list_all())
.get("/api/memory", |state| state.memory.status())
.get("/api/config", |state| state.config.current())
.put("/api/config", |state, body| state.config.update(body))
```

---

## Phase 3: API 能力完全对等

### T8. 工具 API [tools 模块]
```
GET  /api/tools                   列出所有工具 + 描述
POST /api/tools/:name/execute     执行指定工具 (需要 session)
GET  /api/tools/:name             查看工具详情
```

### T9. 插件 API [plugins 模块]
```
GET  /api/plugins                 列出所有插件
POST /api/plugins                 安装插件
POST /api/plugins/:name/enable    启用
POST /api/plugins/:name/disable   禁用
DELETE /api/plugins/:name         卸载
```

### T10. MCP API [mcp 模块]
```
GET  /api/mcp/servers             列出 MCP 服务器
GET  /api/mcp/servers/:name       查看详情
GET  /api/mcp/servers/:name/tools 列出该服务器工具
POST /api/mcp/servers/:name/reload 重载发现
```

### T11. 记忆 API [memory 模块]  
```
GET  /api/memory                  记忆状态/统计
GET  /api/memory/layers           列出层信息
GET  /api/memory/search?q=xxx     搜索记忆 (已有)
GET  /api/memory/:layer           列出层条目
DELETE /api/memory/:id            删除条目
POST /api/memory/import           导入记忆
```

### T12. 配置 API [config 模块]
```
GET  /api/config                  完整配置 (脱敏)
GET  /api/config/:section         查看某个 section
PUT  /api/config/:section         更新某个 section
POST /api/config/reload           重载配置
GET  /api/config/schema           配置 schema 说明
```

### T13. 技能 API [skills 模块]
```
GET  /api/skills                  列出技能
GET  /api/skills/:name            查看详情
POST /api/skills                  新增技能
DELETE /api/skills/:name          删除
```

---

## Phase 4: Session 跨接口操作 + 验证

### T14. Session 打断/恢复 API
```
POST /api/sessions/:id/interrupt   打断 turn
POST /api/sessions/:id/continue    继续上次对话
GET  /api/sessions/:id/status      查看当前 turn 状态
```

### T15. WebSocket 实时通知
- Session 列表变更通知
- Turn 进度通知 (thinking/text/tool)
- 记忆更新通知

### T16. 删除旧 server 代码 + 清理
- 删除 `crates/cowd-cli/src/server/mod.rs` (~3000行)
- 删除 `UnifiedSessionStore` 旧引用
- 清理 dead imports

### T17. 全量 TDD 验证
- `cargo test` 全部通过
- TUI + API 并发 session 测试
- Session 切换测试
- 系统服务安装/启动测试

---

## 执行 Wave 规划

```
Wave 1 (基础 - 6 并行):
├── T1: SessionStore 统一 (SQLite)
├── T2: 删除旧 JSONL SessionStore
├── T3: Session 迁移命令
├── T5: ActiveSessions 管理器
├── T8: 工具 API
└── T9: 插件 API

Wave 2 (网关 - 4 并行, 依赖 T1-T5):
├── T4: Gateway CLI 命令
├── T6: TUI 接入 ActiveSessions
├── T7: API 路由重写 (删除旧 server)
└── T10: MCP API

Wave 3 (能力对等 - 4 并行):
├── T11: 记忆 API
├── T12: 配置 API
├── T13: 技能 API
└── T14: Session 打断/恢复 API

Wave 4 (收尾 - 3 并行):
├── T15: WebSocket 通知
├── T16: 删除旧代码
└── T17: 全量验证
```

**预估总改动**: -2450 行 (删除 3000 行旧 server, 新增 ~550 行 API 路由 + 工具)

---

## TDD 验证策略

每个 Task:
1. RED: 编写测试 → 验证当前行为缺失
2. GREEN: 最小实现 → 测试通过
3. REFACTOR: 清理

每个 Wave 结束:
- `cargo build --release` 0 errors
- `cargo test` 全部通过
- TUI 启动验证
- API 端点 curl 验证
