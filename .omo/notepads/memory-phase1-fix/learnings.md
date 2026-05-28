
## Task 2 (F5): kv_put/kv_get + single connection migration

### Changes Made
1. **store/mod.rs**: Added `kv_put(&self, key, value) -> Result<()>` and `kv_get(&self, key) -> Result<Option<String>>` to `MemoryStore` trait with default Err implementations (lines 204-214).

2. **store/sqlite.rs**: 
   - Added `ensure_kv_table()` method that creates `kv_store` via `CREATE TABLE IF NOT EXISTS`.
   - Called from `open()`, `open_path()`, and `open_in_memory()` after `init_schema()`.
   - Implemented `kv_put`/`kv_get` in `#[async_trait] impl MemoryStore for SqliteStore` using `spawn_blocking`.

3. **cognitive.rs**:
   - REMOVED: `use store::sqlite::SqliteStore` import.
   - REMOVED: `sqlite_store: SqliteStore` field from `CognitiveContextManager`.
   - REMOVED: `let sqlite_store = SqliteStore::open(&config.store)?;` (second connection).
   - REPLACED Closet loading: `orchestrator.store().kv_get("closet").await`
   - REPLACED Seeds loading: `orchestrator.store().kv_get("seeds").await`
   - REPLACED Closet save: `self.orchestrator.store().kv_put("closet", &json).await`
   - REPLACED Seeds save: `self.orchestrator.store().kv_put("seeds", &json).await`

### Results
- `cargo build -p cowd-memory`: PASSES (only pre-existing warnings)
- `cargo test -p cowd-memory`: 330 pass, 106 fail — ALL failures are pre-existing `init_schema` "Execute returned results" error in rusqlite (verified by stashing changes and running tests on vanilla code)

### Notes
- The `save_closet()`, `load_closet()`, `save_seeds()`, `load_seeds()` methods on `SqliteStore` are preserved for backward compatibility.
- No generics in trait methods — kept dyn-compatible.
- No new crate dependencies.
- Closet/Seeds JSON serialization format unchanged.
