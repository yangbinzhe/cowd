# Cowd 计划2 — Phase 0: 基础抽象层

> 优先级：P0 | 前置条件：无 | 预计工时：6h
> GitNexus 影响分析：新增 4 个文件，无现有代码修改

## 执行步骤

### Step 0.1: 统一错误类型 (1h)

**目标**：定义 CowdError + ErrorChain + RecoveryStrategy

**新建文件**：`crates/runtime/src/error.rs`

**详细修改**：
```rust
// crates/runtime/src/error.rs (新建)
#[derive(Debug, thiserror::Error)]
pub enum CowdError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Config error: {0}")]
    Config(String),
    #[error("Provider error: {0}")]
    Provider(#[from] api::error::ApiError),
    #[error("Agent error: {0}")]
    Agent(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub enum RecoveryStrategy {
    Retry { max: u32, delay_ms: u64 },
    Fallback { reason: String },
    Abort,
    Ignore,
}
```

**验证**：`cargo test -p runtime -- error`

---

### Step 0.2: StorageBackend trait (2h)

**目标**：定义抽象存储接口，JSONL 实现迁移

**新建文件**：
- `crates/runtime/src/storage/mod.rs` — trait 定义
- `crates/runtime/src/storage/jsonl.rs` — JSONL 后端（从 session JSONL 迁移）
- `crates/runtime/src/storage/sqlite.rs` — SQLite 后端（Phase 2 实现）

```rust
// crates/runtime/src/storage/mod.rs
use async_trait::async_trait;
use crate::error::CowdError;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn write(&self, key: &str, value: &[u8]) -> Result<(), CowdError>;
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, CowdError>;
    async fn delete(&self, key: &str) -> Result<(), CowdError>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CowdError>;
    async fn flush(&self) -> Result<(), CowdError>;
}

pub enum StorageType {
    Jsonl { path: std::path::PathBuf },
    Sqlite { path: std::path::PathBuf },
}
```

**验证**：`cargo test -p runtime -- storage`

---

### Step 0.3: EventBus trait (2h)

**目标**：统一事件总线，支持 TUI/SSE/Agent 三种消费者

**新建文件**：`crates/runtime/src/event_bus.rs`

```rust
// crates/runtime/src/event_bus.rs
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    TextDelta { text: String, turn_id: String },
    ThinkingDelta { text: String },
    ToolStart { id: String, name: String },
    ToolProgress { id: String, progress: String },
    ToolComplete { id: String, summary: String, exit_code: Option<i32> },
    TurnComplete { summary: String },
    TurnError { error: String },
    TokenUsage { input: u64, output: u64 },
}

pub struct EventBus {
    tx: broadcast::Sender<RuntimeEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self { /* ... */ }
    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> { /* ... */ }
    pub fn publish(&self, event: RuntimeEvent) -> Result<(), CowdError> { /* ... */ }
}
```

**验证**：`cargo test -p runtime -- event_bus`

---

### Step 0.4: 模块注册 (1h)

**目标**：更新 `crates/runtime/src/lib.rs` 导出新模块

**修改文件**：`crates/runtime/src/lib.rs`

**修改点**：
- 添加 `pub mod error;`
- 添加 `pub mod storage;`
- 添加 `pub mod event_bus;`

**验证**：`cargo check -p runtime`

```
并行执行方案：
Step 0.1 ~ 0.4 为顺序依赖（trait 先定义，实现后编写）
但 Step 0.2 的 jsonl.rs 和 Step 0.3 的 event_bus.rs 可以并行编写（无交叉依赖）
```

## 修改影响范围（GitNexus impact）

| 文件 | 变更类型 | 影响模块 |
|------|---------|---------|
| `crates/runtime/src/error.rs` | 新建 | 无 |
| `crates/runtime/src/storage/mod.rs` | 新建 | 无 |
| `crates/runtime/src/storage/jsonl.rs` | 新建 | 无 |
| `crates/runtime/src/event_bus.rs` | 新建 | 无 |
| `crates/runtime/src/lib.rs` | 修改 | +4 行 pub mod |
| `runtime/Cargo.toml` | 修改 | +thiserror, +async-trait |

**风险等级**：LOW（纯新增，不影响现有代码）
