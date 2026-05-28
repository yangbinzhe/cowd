# Cowd 记忆框架增强方案 — TDD 可执行计划

## TL;DR
在 cowd 的 5 层记忆系统中嵌入轻量 tree-sitter 代码索引引擎，实现 LLM 调用前自动符号上下文注入（-30~40% token），符号↔对话关联追溯，以及代码热符号快速路由。全部 TDD 模式，~500 行 Rust 新增代码，零外部新依赖。

---

## 一、基线审计

### 当前记忆架构 (10,667 行 / 73 文件)
```
L0 Identity  — 用户身份，跨会话不变
L1 Essential — 15条热记忆，频率衰减，自动提取
L2 Project   — 项目上下文，~3000 token 预算，跨会话共享
L3 Deep Recall — SQLite FTS5 全文搜索 + BM25/向量混合
L4 Shared    — 团队共享知识，shared_scope 隔离

CognitiveContextManager (facade)
├── MemoryOrchestrator
├── CompressionPipeline
├── DynamicLoader (relevance scoring)
├── BackgroundExtractor (实体/关系自动提取)
├── ClosetManager (指针索引)
├── ContextFence (规则过滤)
├── HybridSearcher (BM25+向量)
├── KnowledgeGraph (实体+三元组)
└── AAAK 压缩 (符号化压缩 70-85%)
```

### 已有但未充分利用
- `rusqlite` 已含 FTS5 (`modern-full` feature)
- `store/sqlite.rs` 已有 FTS 搜索接口
- `entity.rs` 已有 `KnowledgeGraph`（实体+三元组）
- `closet.rs` 已有 `RANK_BOOSTS` 指针路由
- `project_scope.rs` 已有 `build_project_kg` 函数（空壳）

---

## 二、增强目标

| 层次 | 当前 | 增强后 |
|------|------|--------|
| **L1** | 15条文本热记忆 | +3~5个代码热符号槽位 |
| **L2** | 项目文本上下文 (~3000 tokens) | +tree-sitter 代码图谱（符号表+调用图） |
| **L3** | FTS5 对话搜索 | +符号↔对话关联表 |
| **Closet** | 文本指针 | +代码符号指针 |
| **ContextProfiler** | 文本上下文注入 | +自动符号上下文注入 |

---

## 三、TDD 执行计划

### Phase 1 — 代码索引器 (3 tasks)

- [x] 1. `CodeIndexer` 核心结构 + tree-sitter 解析器管理

  **What to do (TDD)**:
  - RED: `test_parse_rust_function`, `test_parse_python_class`, `test_extract_calls`
  - GREEN: 创建 `crates/memory/src/code_indexer.rs`
    ```rust
    // 参考 CodeGraph extraction/index.ts:20-40
    // "Tree-sitter (deterministic) — parses source into a concrete syntax tree
    //  and extracts structural facts: imports, exports, function/class definitions,
    //  call sites, inheritance."
    
    pub struct CodeIndexer {
        parsers: HashMap<Language, tree_sitter::Parser>,
        db: Arc<SqliteStore>,  // 复用现有SQLite
    }
    
    pub struct CodeSymbol {
        id: SymbolId,
        name: String,
        kind: SymbolKind,      // Function, Class, Method, Struct, Enum, Interface
        file_path: String,
        line: usize,
        signature: String,
        doc: Option<String>,
    }
    
    pub enum SymbolEdge { Calls, Imports, Extends, Implements }
    
    impl CodeIndexer {
        pub fn new(db: Arc<SqliteStore>) -> Self;
        pub fn index_file(&mut self, path: &Path) -> Result<Vec<CodeSymbol>>;
        pub fn index_all(&mut self, root: &Path) -> Result<IndexStats>;
    }
    ```
  - 支持 5 种语言：Rust, Python, TypeScript, Go, Java
  - 参考 CodeGraph: `extraction/languages/` 每种语言独立解析器

  **Acceptance**: `cargo test -p cowd-memory -- code_indexer` → 5+ PASS

  **Reference**: CodeGraph `src/extraction/index.ts:1-40` — extraction orchestrator with `FILE_IO_BATCH_SIZE = 10`

- [x] 2. SQLite 符号表 + 调用图持久化

  **What to do (TDD)**:
  - RED: `test_insert_symbol`, `test_query_callers`, `test_query_callees`
  - GREEN: 扩展 `crates/memory/src/store/sqlite.rs`
    ```sql
    -- 参考 CodeGraph db/migrations.ts 的 FTS5 设计
    CREATE TABLE IF NOT EXISTS code_symbols (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        kind TEXT NOT NULL,
        file_path TEXT NOT NULL,
        line INTEGER NOT NULL,
        signature TEXT,
        doc TEXT,
        project_scope TEXT
    );
    
    CREATE VIRTUAL TABLE IF NOT EXISTS code_symbols_fts 
        USING fts5(name, signature, file_path, content=code_symbols);
    
    CREATE TABLE IF NOT EXISTS code_edges (
        source_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        edge_type TEXT NOT NULL,  -- 'calls', 'imports', 'extends', 'implements'
        file_path TEXT
    );
    ```
  - 新增 `MemoryStore` trait 方法:
    ```rust
    async fn insert_symbol(&self, sym: &CodeSymbol) -> Result<()>;
    async fn search_symbols(&self, query: &str, limit: usize) -> Result<Vec<CodeSymbol>>;
    async fn get_callers(&self, symbol_id: &str) -> Result<Vec<CodeSymbol>>;
    async fn get_callees(&self, symbol_id: &str) -> Result<Vec<CodeSymbol>>;
    ```

  **Acceptance**: `cargo test -p cowd-memory -- store::code_symbols` → 5+ PASS

  **Reference**: CodeGraph `src/db/queries.ts` — `searchNodes`, `getCallers`, `getCallees`

- [x] 3. 增量更新 + 文件监听

  **What to do (TDD)**:
  - RED: `test_incremental_reindex_changed_file`, `test_unchanged_file_skipped`
  - GREEN: 文件指纹（mtime + size hash）+ 增量更新
    ```rust
    // 参考 CodeGraph extraction/index.ts — fingerprint-based incremental
    // "Same input → same output, every run. Also powers fingerprint-based 
    //  change detection for incremental updates."
    
    impl CodeIndexer {
        fn compute_fingerprint(path: &Path) -> Option<Fingerprint>;
        fn reindex_if_changed(&mut self, path: &Path) -> Result<bool>;
        fn watch(&self, on_change: impl Fn(&Path));
    }
    ```

  **Acceptance**: `cargo test -p cowd-memory -- code_indexer::incremental` → 3+ PASS

### Phase 2 — 记忆层联动 (3 tasks)

- [x] 4. L2 Project 层 — 代码图谱集成

  **What to do (TDD)**:
  - RED: `test_project_layer_injects_code_symbols`, `test_auto_discover_on_init`
  - GREEN: 扩展 `crates/memory/src/layers/project.rs`
    ```rust
    impl ProjectLayer {
        // 新增字段
        code_indexer: Option<CodeIndexer>,
        
        // 增强: init 时自动索引项目源码
        pub async fn init_with_code_graph(&mut self, root: &Path) -> Result<()>;
        
        // 增强: prepare_context 时注入相关代码符号
        async fn inject_code_context(&self, query: &str, budget: &mut TokenBudget) -> Vec<CodeSymbol>;
    }
    ```
  - 触发时机: `cowd init` / 首次进入项目时

  **Acceptance**: `cargo test -p cowd-memory -- layers::project` → 3+ PASS

- [x] 5. L3 Deep Recall — 符号↔对话关联

  **What to do (TDD)**:
  - RED: `test_symbol_conversation_link`, `test_find_conversations_by_symbol`
  - GREEN: 新增 `symbol_references` 表 + 关联逻辑
    ```sql
    CREATE TABLE IF NOT EXISTS symbol_references (
        symbol_id TEXT NOT NULL,
        memory_id TEXT NOT NULL,
        turn_index INTEGER,
        reference_type TEXT,  -- 'discussed', 'modified', 'created'
        timestamp INTEGER NOT NULL DEFAULT (unixepoch())
    );
    ```
  - 在 `BackgroundExtractor` 中新增: 从对话中提取代码符号引用
  - 在 `MemoryOrchestrator` 中: 记录每次工具调用涉及的符号

  **Acceptance**: `cargo test -p cowd-memory -- layers::deep` → 3+ PASS

- [x] 6. L1 Essential + Closet — 代码热符号追踪

  **What to do (TDD)**:
  - RED: `test_hot_symbol_promotion`, `test_closet_code_pointer_lookup`
  - GREEN: 扩展 `closet.rs`
    ```rust
    // Closet 新增代码符号指针类型
    enum PointerKind { Memory, CodeSymbol }
    
    impl ClosetManager {
        fn add_code_pointer(&self, symbol_name: &str, drawer_key: &str, boost: f32);
        fn lookup_code_symbol(&self, name: &str) -> Option<CodeSymbol>;
    }
    ```
  - L1 扩展: 在 15 条热记忆中预留 3~5 个代码符号槽位
    ```rust
    // essential.rs 扩展
    fn promote_hot_symbol(&mut self, symbol: &CodeSymbol);
    fn decay_symbols(&mut self);
    ```

  **Acceptance**: `cargo test -p cowd-memory -- closet` → 3+ PASS

### Phase 3 — 上下文注入 + 影响分析 (2 tasks)

- [x] 7. ContextProfiler — 自动符号上下文注入

  **What to do (TDD)**:
  - RED: `test_auto_inject_relevant_symbols`, `test_no_injection_without_code_graph`
  - GREEN: 扩展 `cognitive.rs` 的 `CognitiveContextManager`
    ```rust
    impl CognitiveContextManager {
        // 新增: 在 LLM 调用前自动查询并注入相关代码符号
        async fn build_context_with_code(
            &self, 
            user_query: &str, 
            budget: &mut TokenBudget
        ) -> PreparedContext {
            // 1. 从用户查询提取关键词
            // 2. FTS5 搜索代码符号 (L2)
            // 3. 查询调用方 (L2 CodeGraph)
            // 4. 查询历史讨论 (L3 symbol_references)
            // 5. 注入到 PreparedContext 的 code_context 字段
        }
    }
    ```
  - 注入格式：
    ```
    ## Relevant Code Symbols
    - authenticate_user (src/auth.rs:42) — JWT token validation
      Called by: login_handler, api_middleware
      Last discussed: 2026-05-20 (session #42)
    ```
  - 参考 CodeGraph 的 `buildContext` 设计:
    > "One tool call returns entry points, related symbols, and code snippets — 
    >  no expensive exploration agents"

  **Acceptance**: `cargo test -p cowd-memory -- cognitive` → 4+ PASS

- [x] 8. 影响分析 — 编辑前符号级检查

  **What to do (TDD)**:
  - RED: `test_impact_analysis_returns_callers`, `test_impact_warning_on_edit`
  - GREEN: 新增影响分析接口
    ```rust
    // 参考 GitNexus 的 impact 工具设计
    pub struct ImpactReport {
        pub symbol: CodeSymbol,
        pub direct_callers: Vec<CodeSymbol>,     // d=1: WILL BREAK
        pub indirect_callers: Vec<CodeSymbol>,   // d=2: LIKELY AFFECTED
        pub transitive: Vec<CodeSymbol>,         // d=3: MAY NEED TESTING
        pub affected_files: Vec<String>,
    }
    
    impl CodeIndexer {
        pub fn get_impact(&self, symbol_id: &str, depth: usize) -> ImpactReport;
    }
    ```
  - 集成到 `ApprovalGate`: 编辑文件前自动检查影响范围

  **Acceptance**: `cargo test -p cowd-memory -- code_indexer::impact` → 3+ PASS

### Phase FINAL — 集成验证 (2 tasks)

- [x] F1. 整合测试 — 端到端记忆增强流程
  - 场景: init → index project → ask question → verify auto-injection → verify symbol↔conversation link
  - `cargo test -p cowd-memory -- integration` → 5+ PASS

- [x] F2. Token 基准 — 对比增强前后
  - 场景: 同一问题，对比有/无代码图谱时的 token 消耗
  - 目标: token 减少 >20%（对标 CodeGraph 的 35%）

---

## 四、参考源码对照

| 实现点 | CodeGraph 参考 | GitNexus 参考 |
|--------|---------------|---------------|
| tree-sitter 解析器管理 | `extraction/index.ts:7-40` (FILE_IO_BATCH_SIZE, PARSE_TIMEOUT) | `core/indexer/` (indexer orchestration) |
| SQLite 符号表 | `db/migrations.ts` (FTS5 表设计) | N/A (LadybugDB) |
| 查询接口 | `db/queries.ts` (searchNodes, getCallers, getCallees) | N/A |
| 影响分析 | `impact` tool (depth-based traversal) | `gitnexus_impact` (d=1/2/3 depth groups) |
| 增量更新 | `extraction/index.ts` (fingerprint-based) | `detect_changes` (git diff mapping) |
| 上下文构建 | `context/index.ts` (buildContext, format: markdown) | N/A |

## 五、预估效果

| 指标 | 当前 | 增强后 |
|------|------|--------|
| LLM 代码问题 token 消耗 | 基线 | -20~35% |
| "这个函数之前讨论过吗" | 文本搜索（不准） | 符号关联查询（精确） |
| "改了会坏什么" | 无自动检查 | 影响分析报告 |
| 代码符号查找 | LLM grep+read (~2-3次工具调用) | 本地查询 <1ms |
| 新项目上手 | 逐文件阅读 | 代码图谱全局视图 |
