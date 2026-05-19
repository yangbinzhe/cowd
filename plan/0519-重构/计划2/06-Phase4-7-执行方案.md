# Cowd 计划2 — Phase 4-7 执行方案

## Phase 4: Server 异步 + SSE (6h)

### Step 4.1: 消除 block_on (2h)

**目标**：Server 路径中所有 `.block_on()` 替换为 async

**修改点**（基于 GitNexus conversation.rs:584-751 run_turn_async）：
1. `run_turn_async()` 作为 Server 的入口（现已是 async）
2. 删除 `send_message_stream_handler` 中的 `.block_on()` 调用
3. 工具执行回调改为 async

### Step 4.2: SSE 背压控制 (2h)

**新建文件**：`crates/cowd-cli/src/server/sse.rs`

```rust
pub struct SseManager {
    connections: Arc<DashMap<String, SseConnection>>,
    max_connections: usize,  // 默认 1000
    per_conn_buffer: usize,   // 默认 64
}

impl SseManager {
    pub fn add_client(&self, id: String) -> SseConnection;
    pub fn remove_client(&self, id: String);
    pub fn broadcast(&self, event: ServerEvent) -> usize; // 返回发送数量
    pub fn slow_client_detect(&self) -> Vec<String>;       // 慢消费者列表
}
```

### Step 4.3: 路由重构 (1.5h)

**目标**：将 server.rs 中的 20+ 路由端点组织到 `routes/` 子模块

```
crates/cowd-cli/src/server/routes/
├── session.rs    → /api/sessions/*
├── config.rs     → /api/config/*
├── memory.rs     → /api/memory/*
└── stream.rs     → /api/*/stream
```

### Step 4.4: 验证 (0.5h)

- [ ] `cargo test -p cowd-cli -- server`
- [ ] 并发测试：`wrk -c 100 -t 4 -d 30s http://localhost:8642/api/health`
- [ ] SSE 测试：100 个客户端同时连接

---

## Phase 5: Agent 协同增强 (8h)

### Step 5.1: MCP 协议 Agent 间通信 (3h)

**修改文件**：`crates/runtime/src/mcp_stdio.rs`

```rust
// 复用 MCPClient 作为 AgentTransport
pub struct AgentTransport {
    client: McpClient,
    heartbeat_interval: Duration,  // 默认 30s
    timeout: Duration,              // 默认 60s
}

impl AgentTransport {
    pub async fn send_task(&self, task: AgentTask) -> Result<TaskReceipt>;
    pub async fn poll_progress(&self, task_id: &str) -> Result<TaskProgress>;
    pub async fn cancel_task(&self, task_id: &str) -> Result<()>;
    pub async fn health_check(&self) -> Result<AgentHealth>;
}
```

### Step 5.2: Worker 持久化 (2h)

**新建文件**：`crates/runtime/src/worker_store.rs`

- Worker 注册信息持久化到 SQLite
- 重启后自动恢复 Worker 连接
- 任务重分配（Worker 离线时）

### Step 5.3: Cron 持久化 (2h)

**修改文件**：`crates/runtime/src/team_cron_registry.rs`

- CronJob 定义持久化到 SQLite
- 重启后恢复定时任务
- 支持 `last_run` / `next_run` 时间戳

### Step 5.4: 验证 (1h)

- [ ] Agent MCP 通信集成测试
- [ ] Worker 重启恢复测试
- [ ] Cron 持久化测试

---

## Phase 6: 记忆框架优化 (8h)

### Step 6.1: 存储后端抽象实现 (3h)

**新建文件**：`crates/memory/src/backend/`
```
backend/
├── mod.rs        → MemoryBackend trait
├── jsonl.rs      → 现状迁移
├── sqlite.rs     → 新增
└── hybrid.rs     → Jsonl + Sqlite 混合
```

### Step 6.2: 动态压缩策略 (2h)

**修改文件**：`crates/memory/src/compression/monitor.rs`

```rust
impl AdaptiveThreshold {
    pub fn for_context_window(ctx_size: u64) -> Self {
        match ctx_size {
            0..=32_000 => Self::aggressive(),    // 小窗口激进压缩
            32_001..=128_000 => Self::moderate(), // 中窗口适度压缩
            _ => Self::relaxed(),                 // 大窗口宽松压缩
        }
    }
}
```

### Step 6.3: 增量挖掘 (2h)

**修改文件**：`crates/memory/src/miner.rs`

- 基于文件 mtime 的增量更新
- 仅重新扫描修改过的文件

### Step 6.4: 验证 (1h)

- [ ] `cargo test -p memory` (252 tests)

---

## Phase 7: 配置统一 (4h)

### Step 7.1: ConfigLoader 迁移 (2h)

**修改文件**：
- `crates/runtime/src/config.rs` → 迁移逻辑到 `crates/config/src/loader.rs`
- `crates/runtime/src/config.rs` → 删除，改为 `use config::RuntimeConfig;`

### Step 7.2: 热重载 (1.5h)

**新建文件**：`crates/config/src/watcher.rs`

```rust
use notify::{Watcher, RecursiveMode, watcher};

pub struct ConfigWatcher {
    paths: Vec<PathBuf>,
    on_change: Box<dyn Fn(ConfigChangeEvent) + Send>,
}

impl ConfigWatcher {
    pub fn start<F>(paths: Vec<PathBuf>, on_change: F) -> Result<Self>
    where F: Fn(ConfigChangeEvent) + Send + 'static;
}
```

### Step 7.3: 验证 (0.5h)

- [ ] `cargo test -p config` (34 tests)
- [ ] 手动修改 config.yaml → 服务自动重载

---

## Phase 8-9: 测试与文档

### Phase 8: 测试补全 (8h) 详细见 `01-测试方案.md`

### Phase 9: 文档与清理 (4h)

- [ ] 每个 pub 函数有 rustdoc 注释
- [ ] 更新 ARCHITECTURE.md
- [ ] 更新 README.md 使用示例
- [ ] 删除死代码（标注 `#[deprecated]` 的函数）
- [ ] `cargo clippy --workspace --all-targets` 零 warning
