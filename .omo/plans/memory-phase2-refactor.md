# Phase 2 记忆框架重构 — 详细执行方案 (v0.7.8)

> Oracle审定: 5/5 GO | 工期: 1-2天 | 并行波: 3波

---

## 当前代码基线 (v0.7.8, commit efcf667)

所有文件引用和行号基于 v0.7.8 提交的**实际代码**。Oracle已逐行验证。

---

## 并行执行架构

```
Wave 1 (立即启动, 2任务并行):
├── Task 1: F6 清理双轨接口 [quick, 15min]
│   删除 memory_provider.rs + lib.rs 2行
│
└── Task 2: F9 代码符号持久化 [deep, 4-6h]
    SQLite 3张表 + 4个trait方法 + CodeIndexer 自动持久化
    └── 同时修改: store/mod.rs, store/sqlite.rs, code_indexer.rs

      ↓ (F6必须完成, 避免编译警告)

Wave 2 (F6+部分F9完成后, 2任务并行):
├── Task 3: F7 统一Token预算 [deep, 1-2h]
│   新增 BudgetCalculator + 替换3处调用
│   影响: config.rs, orchestrator.rs, cognitive.rs
│
└── Task 4: F8 统一Session隔离 [quick, 30min]
    替换 cognitive.rs:805-813 的过滤逻辑
    影响: cognitive.rs (1处替换)

      ↓ (无依赖关系, 可独立)

Wave 3 (Wave 2完成后):
└── Task 5: F10 热符号注入 [quick, 1h]
    cognitive.rs prepare_context 新增注入块
    影响: cognitive.rs (1处新增)

Wave FINAL (全部实现完成后):
├── F1: 全量回归测试
└── F2: Oracle合规审计
```

---

## Task 1: F6 — 清理双轨接口

**工期**: 15min | **风险**: 零 | **涉及文件**: 3个

### 问题

`crates/runtime/src/memory_provider.rs` (51行) 定义了 `MemoryProvider` trait + `BuiltinMemoryProvider`(全空实现) + `MemoryProviderManager`。
所有这些在整个代码库中零引用(仅在 `lib.rs:229,233` 声明和导出)。

### 修改清单

**文件1**: `/media/yi/Datas/workspace/cowd/crates/runtime/src/memory_provider.rs`
- **操作**: 删除整个文件 (51行)
- **内容**: 自包含, 无其他文件依赖

**文件2**: `/media/yi/Datas/workspace/cowd/crates/runtime/src/lib.rs`
- **操作**: 删除2行
- **第229行**: 删除 `pub mod memory_provider;`
- **第233行**: 删除 `pub use memory_provider::{MemoryProvider, BuiltinMemoryProvider, MemoryProviderManager};`

**文件3**: `/media/yi/Datas/workspace/cowd/crates/runtime/src/conversation.rs`
- **操作**: 无需修改 (Oracle确认零引用)

### 验证

```bash
cargo build --workspace          # 编译零错误
grep -r "memory_provider" crates/ --include="*.rs" | wc -l   # 输出: 0
```

---

## Task 2: F9 — 代码符号SQLite持久化

**工期**: 4-6h | **风险**: 中 | **涉及文件**: 3个

### 问题

`MemoryStore` trait (store/mod.rs) 有9个stub方法返回 `Err(not supported)`。
代码符号无法持久化, 调用图不可查询, 符号-记忆链接断裂。

### 本次实现范围 (4/9方法)

| 方法 | Phase 2 | 说明 |
|------|---------|------|
| `insert_symbol()` | ✅ | 持久化tree-sitter符号定义 |
| `search_symbols()` | ✅ | 按名称搜索符号 |
| `insert_edge()` | ✅ | 持久化调用/导入/继承边 |
| `link_symbol_to_memory()` | ✅ | 符号↔记忆关联 |
| `get_callers()` | ❌ | Phase 4 (需调用图完整) |
| `get_callees()` | ❌ | Phase 4 |
| `find_memories_by_symbol()` | ❌ | Phase 4 |
| `kv_put()` | ✅ | Phase 1已完成 |
| `kv_get()` | ✅ | Phase 1已完成 |

### 修改清单

#### 文件1: `crates/memory/src/store/sqlite.rs`

**Step 1.1** — 在 `init_schema()` 函数中添加3张表的建表语句:

在现有的 `CREATE TABLE IF NOT EXISTS kv_store` 之后添加:

```sql
-- 代码符号定义表
CREATE TABLE IF NOT EXISTS code_symbols (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    file_path TEXT NOT NULL,
    line INTEGER NOT NULL,
    signature TEXT,
    doc TEXT,
    language TEXT NOT NULL,
    fingerprint TEXT,
    indexed_at TEXT NOT NULL
);

-- 代码符号关系边表
CREATE TABLE IF NOT EXISTS code_edges (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    file_path TEXT,
    line INTEGER,
    confidence REAL DEFAULT 1.0
);

-- 符号-记忆关联表
CREATE TABLE IF NOT EXISTS symbol_memory_links (
    id TEXT PRIMARY KEY,
    symbol_id TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    turn_index INTEGER,
    reference_type TEXT NOT NULL,
    timestamp INTEGER NOT NULL
);

-- FTS5符号搜索索引
CREATE VIRTUAL TABLE IF NOT EXISTS code_symbols_fts USING fts5(
    name, signature, doc, content=code_symbols, content_rowid=rowid
);
```

**Step 1.2** — 在 `impl MemoryStore for SqliteStore` 块末尾添加4个方法实现:

```rust
// 在 kv_get() 实现之后添加:

async fn insert_symbol(&self, sym: &CodeSymbol) -> Result<()> {
    let store = self.clone();
    let conn_id = sym.id.clone();
    let name = sym.name.clone();
    let kind = sym.kind.as_str().to_string();
    let file_path = sym.file_path.clone();
    let line = sym.line as i64;
    let signature = sym.signature.clone();
    let doc = sym.doc.clone();
    let lang = sym.language.as_str().to_string();
    let fingerprint = sym.fingerprint.clone();
    let now = Utc::now().to_rfc3339();

    tokio::task::spawn_blocking(move || {
        let conn = store.conn()?;
        conn.execute(
            "INSERT OR REPLACE INTO code_symbols
             (id, name, kind, file_path, line, signature, doc, language, fingerprint, indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![conn_id, name, kind, file_path, line, signature, doc, lang, fingerprint, now],
        ).map_err(sql_err)?;
        Ok(())
    }).await.map_err(|e| MemoryError::Store(e.to_string()))?
}

async fn search_symbols(&self, query: &str, limit: usize) -> Result<Vec<CodeSymbol>> {
    let store = self.clone();
    let q = format!("%{query}%");
    let l = limit;
    tokio::task::spawn_blocking(move || {
        let conn = store.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, kind, file_path, line, signature, doc, language, fingerprint
             FROM code_symbols WHERE name LIKE ?1 LIMIT ?2"
        ).map_err(sql_err)?;
        let rows = stmt.query_map(rusqlite::params![q, l as i64], |row| {
            Ok(CodeSymbol {
                id: row.get(0)?,
                name: row.get(1)?,
                kind: SymbolKind::from_str(&row.get::<_, String>(2)?),
                file_path: row.get(3)?,
                line: row.get::<_, i64>(4)? as u32,
                signature: row.get(5)?,
                doc: row.get(6)?,
                language: IndexLanguage::from_str(&row.get::<_, String>(7)?),
                fingerprint: row.get(8)?,
            })
        }).map_err(sql_err)?;
        let results: Vec<CodeSymbol> = rows.filter_map(|r| r.ok()).collect();
        Ok(results)
    }).await.map_err(|e| MemoryError::Store(e.to_string()))?
}

async fn insert_edge(&self, edge: &SymbolEdge) -> Result<()> {
    let store = self.clone();
    let id = edge.id.clone();
    let source = edge.source_id.clone();
    let target = edge.target_id.clone();
    let etype = edge.edge_type.as_str().to_string();
    let fpath = edge.file_path.clone();
    let line = edge.line as i64;
    let conf = edge.confidence;

    tokio::task::spawn_blocking(move || {
        let conn = store.conn()?;
        conn.execute(
            "INSERT OR REPLACE INTO code_edges (id, source_id, target_id, edge_type, file_path, line, confidence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![id, source, target, etype, fpath, line, conf],
        ).map_err(sql_err)?;
        Ok(())
    }).await.map_err(|e| MemoryError::Store(e.to_string()))?
}

async fn link_symbol_to_memory(
    &self, symbol_id: &str, memory_id: &MemoryId,
    turn_index: Option<i32>, reference_type: &str, timestamp: i64,
) -> Result<()> {
    let store = self.clone();
    let sid = symbol_id.to_string();
    let mid = *memory_id;
    let reftype = reference_type.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = store.conn()?;
        conn.execute(
            "INSERT INTO symbol_memory_links (id, symbol_id, memory_id, turn_index, reference_type, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                uuid::Uuid::new_v4().to_string(),
                sid, mid.to_string(), turn_index, reftype, timestamp
            ],
        ).map_err(sql_err)?;
        Ok(())
    }).await.map_err(|e| MemoryError::Store(e.to_string()))?
}
```

#### 文件2: `crates/memory/src/code_indexer.rs`

在 `CodeIndexer` 的 `scan()` 方法完成符号/边构建后, 新增持久化调用:

找到 `scan()` 方法的末尾 (构建完 `self.symbols` 和 `self.edges` 之后):

```rust
// 如果传入了 store, 自动持久化符号和边
if let Some(ref store) = self.store {
    for sym in &self.symbols {
        if let Err(e) = store.insert_symbol(sym).await {
            tracing::warn!("code_indexer: failed to persist symbol {}: {}", sym.name, e);
        }
    }
    for edge in &self.edges {
        if let Err(e) = store.insert_edge(edge).await {
            tracing::warn!("code_indexer: failed to persist edge {:?}: {}", edge.edge_type, e);
        }
    }
    tracing::info!(
        symbols = self.symbols.len(),
        edges = self.edges.len(),
        "code_indexer: persisted to SQLite"
    );
}
```

注意: `CodeIndexer` 需要一个 `store: Option<Arc<dyn MemoryStore>>` 字段 (查看当前 `CodeIndexer` 结构确认)。
若当前没有此字段, 在 `scan()` 的参数中传入 `&dyn MemoryStore` 引用。

#### 文件3: `crates/memory/src/store/mod.rs`

无需修改 — trait的4个stub默认实现已经存在(第148-190行)。
SqliteStore的实现会自动覆盖这些默认实现。

### 验证

```bash
cargo build -p cowd-memory
cargo test -p cowd-memory -- code_indexer
# 确认 code_symbols 表在 memory.db 中被创建
```

---

## Task 3: F7 — 统一Token预算计算

**工期**: 1-2h | **风险**: 中 | **涉及文件**: 2个

### 问题

3处Token预算计算逻辑产生不同结果:
1. `orchestrator.rs:805 make_budget()` — 无角色乘数
2. `cognitive.rs:2030 compute_budget()` — 读current_agent → 委托compute_role_budget
3. `cognitive.rs:2041 compute_role_budget()` — 角色乘数 0.15-0.50

### 修改清单

#### 文件1: `crates/memory/src/config.rs`

在 `MemoryConfig` 的 `impl` 块之前新增 `BudgetCalculator`:

```rust
// 位置: MemoryConfig 的 impl 块之前 (约第419行的位置)

/// 统一的Token预算计算器, 消除分散在3处的计算逻辑
#[derive(Debug, Clone)]
pub struct BudgetCalculator {
    config: BudgetConfig,
}

impl BudgetCalculator {
    pub fn new(config: BudgetConfig) -> Self { Self { config } }

    /// 基础可用token = context_window - reserved_system - reserved_response
    pub fn base_available(&self) -> u64 {
        self.config.context_window
            .saturating_sub(self.config.reserved_system)
            .saturating_sub(self.config.reserved_response)
    }

    /// 构建 TokenBudget (无角色乘数, 用于层分配)
    pub fn make_budget(&self) -> TokenBudget {
        TokenBudget {
            total: self.config.context_window,
            reserved_system: self.config.reserved_system,
            reserved_response: self.config.reserved_response,
            allocated_memory: 0,
            allocated_conversation: 0,
            available: self.base_available(),
        }
    }

    /// 构建角色感知的 TokenBudget
    pub fn make_role_budget(&self, role: &str) -> TokenBudget {
        let multiplier = Self::role_multiplier(role);
        let role_available = (self.base_available() as f64 * multiplier) as u64;
        TokenBudget {
            total: self.config.context_window,
            reserved_system: self.config.reserved_system,
            reserved_response: self.config.reserved_response,
            allocated_memory: 0,
            allocated_conversation: 0,
            available: role_available,
        }
    }

    pub fn role_multiplier(role: &str) -> f64 {
        match role {
            "Planner" => 0.40,
            "Executor" => 0.25,
            "Reviewer" => 0.15,
            _ => 0.50,
        }
    }

    pub fn warning_tokens(&self) -> u64 {
        (self.base_available() as f64 * self.config.warning_threshold as f64) as u64
    }

    pub fn critical_tokens(&self) -> u64 {
        (self.base_available() as f64 * self.config.critical_threshold as f64) as u64
    }
}
```

#### 文件2: `crates/memory/src/orchestrator.rs`

**第805-819行** — 替换 `make_budget()`:

```rust
// 替换前:
fn make_budget(&self) -> TokenBudget {
    let c = &self.config.budget;
    let available = c.context_window - c.reserved_system - c.reserved_response;
    TokenBudget { total: c.context_window, ... }
}

// 替换后:
fn make_budget(&self) -> TokenBudget {
    BudgetCalculator::new(self.config.budget.clone()).make_budget()
}
```

#### 文件3: `crates/memory/src/cognitive.rs`

**第2030-2038行** — 替换 `compute_budget()`:

```rust
// 替换后:
fn compute_budget(&self) -> TokenBudget {
    let role = self.current_agent.lock()
        .and_then(|g| g.clone())
        .unwrap_or_else(|| "Orchestrator".to_string());
    BudgetCalculator::new(self.config.budget.clone()).make_role_budget(&role)
}
```

**第2041-2066行** — 删除 `compute_role_budget()` 整个方法。
**第2026-2029行** — 删除旧的文档注释 (已被 BudgetCalculator 的注释替代)。

### 验证

```bash
cargo build -p cowd-memory
# 验证 orchestrator 和 cognitive 的 budget 计算结果一致
```

---

## Task 4: F8 — 统一Session隔离到ContextFence

**工期**: 30min | **风险**: 低 | **涉及文件**: 1个

### 问题

`cognitive.rs:805-813` 直接基于 `entry.session_id == session_id` 过滤, 与 `ContextFence::allows()` 形成两套隔离机制。

### 修改清单

#### 文件: `crates/memory/src/cognitive.rs`

**替换第805-813行**:

```rust
// 替换前 (805-813行):
// ── Session isolation filter ──
if let Some(sid) = session_id {
    entries = entries.into_iter().filter(|e| {
        match &e.session_id {
            Some(entry_sid) => entry_sid == sid,
            None => true,
        }
    }).collect();
}

// 替换后:
// ── Session isolation filter (via ContextFence) ──
if let Some(sid) = session_id {
    let fence = crate::context_fence::fence_from_session(sid, &self.orchestrator.fence_registry());
    entries = crate::context_fence::filter_through_fence(entries, &fence);
}
```

注意: `fence_from_session()` 和 `filter_through_fence()` 已经存在于 `context_fence.rs` 并通过 `lib.rs` 导出。无需新增任何函数。

### 验证

```bash
cargo build -p cowd-memory
cargo test -p cowd-memory -- context_fence prepare_context
```

---

## Task 5: F10 — 热符号注入 prepare_context

**工期**: 1h | **风险**: 低 | **涉及文件**: 1个

### 问题

L1 `EssentialLayer` 有 `get_hot_symbols()` 方法(essential.rs:165), 但 `prepare_context()` 从不调用它。
`Orchestrator::note_file_access()` 正确提升符号热度, 但热度从未进入上下文。

### 修改清单

#### 文件: `crates/memory/src/cognitive.rs`

在 `prepare_context()` 方法的 Step 7b (ToolSandbox注入, 约969行) 之后, 新增 Step 7c:

**插入位置**: 第969行 `}` (Sandbox block结束) 和 `// ── Assemble PreparedContext` 注释之间。

```rust
        // ── Step 7b: Tool output sandbox injection ──
        {
            let sandbox = self.tool_sandbox.lock()...;
            // ... (existing Phase 1 F2 code)
        }

        // ── Step 7c: Hot code symbol injection ──
        {
            let hot_symbols = self.orchestrator.get_hot_symbols();
            if !hot_symbols.is_empty() {
                let entries_str: Vec<String> = hot_symbols.iter()
                    .take(5)
                    .map(|s| format!("{} (freq={:.1})", s.name, s.frequency))
                    .collect();
                entries.push(MemoryEntry {
                    id: uuid::Uuid::new_v4(),
                    layer: MemoryLayer::L1,
                    category: MemoryCategory::Reference,
                    priority: Priority::Normal,
                    source: MemorySource::AutoExtracted,
                    title: "Hot Code Symbols".into(),
                    content: format!("Frequently accessed symbols: {}", entries_str.join(", ")),
                    embedding: None,
                    tags: vec!["hot_symbols".into(), "code".into()],
                    relations: vec![],
                    confidence: 0.9,
                    access_count: 0, staleness: 0.0,
                    created_at: Utc::now(), updated_at: Utc::now(),
                    last_accessed_at: None,
                    scope: MemoryScope::default(),
                    session_id: None, source_agent: None,
                    visibility: crate::types::AgentVisibility::default(),
                });
            }
        }

        // ── Assemble PreparedContext ──
```

注意: `orchestrator.l1` 是私有字段, 需要在 `orchestrator.rs` 中新增一个公开的访问方法:

#### 文件追加: `crates/memory/src/orchestrator.rs`

在 `impl MemoryOrchestrator` 中添加(约在 `find_relevant_symbols` 方法之后):

```rust
    /// Return currently tracked hot code symbols, sorted by access frequency.
    #[must_use]
    pub fn get_hot_symbols(&self) -> Vec<crate::layers::essential::HotSymbol> {
        self.l1.get_hot_symbols()
    }
```

### 验证

```bash
cargo build -p cowd-memory
cargo test -p cowd-memory -- essential
# 手动验证: 多次访问同一文件后, prepare_context 输出包含热符号
```

---

## Final Verification Wave

### F1: 全量回归测试

```bash
cargo build --workspace
cargo test -p cowd-memory          # 确认测试数 ≥ 330
cargo test -p runtime
```

### F2: Oracle 合规审计

- [ ] `grep -r "memory_provider" crates/ --include="*.rs"` 输出为空
- [ ] `grep "make_budget\|compute_budget\|compute_role_budget" crates/memory/src/` 确认只有 BudgetCalculator 路径
- [ ] `grep "entry.session_id == sid" crates/memory/src/cognitive.rs` 输出为空
- [ ] `sqlite3 memory.db ".tables"` 包含 code_symbols, code_edges, symbol_memory_links
- [ ] `grep "get_hot_symbols" crates/memory/src/cognitive.rs` 有匹配

---

## Commit Strategy

```bash
# Task 1: F6
git add -A && git commit -m "refactor(memory): remove unused MemoryProvider dual-interface (F6)"
# Task 2: F9
git add -A && git commit -m "feat(memory): implement code symbol SQLite persistence (F9) — insert_symbol, search_symbols, insert_edge, link_symbol_to_memory"
# Task 3: F7
git add -A && git commit -m "refactor(memory): unify token budget calculation with BudgetCalculator (F7)"
# Task 4: F8
git add -A && git commit -m "refactor(memory): unify session isolation to ContextFence (F8)"
# Task 5: F10
git add -A && git commit -m "feat(memory): inject hot code symbols into prepare_context (F10)"
```
