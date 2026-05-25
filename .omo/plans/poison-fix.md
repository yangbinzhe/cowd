# v0.5.3: PoisonError 防御 —— 79 处 .unwrap() 替换为 .into_inner()

## 目标

**用完全相同的代码路径、零性能开销的方式，消除 Mutex/RwLock 毒化导致的连锁崩溃。**

### 验收标准

| G | 标准 | 验证方式 |
|---|------|---------|
| G1 | 79 处 `.unwrap()` 全部替换为 `unwrap_or_else(\.into_inner\(\))` | 替换后 grep 不再出现 `\.lock\(\)\.unwrap\(\)` / `\.read\(\)\.unwrap\(\)` / `\.write\(\)\.unwrap\(\)` |
| G2 | 非 `.lock()/.read()/.write()` 的 `.unwrap()` 不动 | grep 计数：非锁 unwrap 前后一致 |
| G3 | `cargo build --release` 零错误、零新警告 | 构建验证 |
| G4 | `cargo test -p cowd-memory --lib` 456/456 PASS | 回归验证 |

### 不动的内容

- 非锁相关的 `.unwrap()`（如 `Option`, `Result` 等）—— 不动
- 业务逻辑 —— 不动
- 文件结构 —— 不动

---

## 涉及文件及数量

| 文件 | lock() | read() | write() | 合计 |
|------|--------|--------|---------|------|
| `conversation.rs` | 4 | 19 | 14 | **37** |
| `cognitive.rs` | 12 | 0 | 0 | **12** |
| `session_store.rs` | 13 | 0 | 0 | **13** |
| `project_scope.rs` | 9 | 0 | 0 | **9** |
| `compression/mod.rs` | 4 | 0 | 0 | **4** |
| `config.rs` | 3 | 0 | 0 | **3** |
| `ansi_fallback.rs` | 1 | 0 | 0 | **1** |
| **总计** | **46** | **19** | **14** | **79** |

---

## 执行步骤（TDD）

### Phase 1: 先写回归测试（确保修复不改行为）

- **现有 456 测试就是回归测试** —— 每次修改后运行 `cargo test -p cowd-memory --lib`

### Phase 2: 逐文件替换

文件间无依赖，可分 3 批并行：

**Wave 1（3 个文件并行，零依赖）：**
1. `session_store.rs`（13处）— 纯机械替换 `.lock().unwrap()` → `.lock().unwrap_or_else(\|e\| e.into_inner())`
2. `project_scope.rs`（9处）— 同上
3. `compression/mod.rs`（4处）— 同上

**Wave 2（3 个文件并行）：**
4. `config.rs`（3处）— 同上
5. `ansi_fallback.rs`（1处）— 同上
6. `cognitive.rs`（12处）— 同上

**Wave 3（1 个文件，最大）：**
7. `conversation.rs`（37处）— 3 种模式：`.lock().unwrap()` / `.read().unwrap()` / `.write().unwrap()`

### Phase 3: 验证

- `cargo build --release` → 0 错误，0 新警告
- `cargo test -p cowd-memory --lib` → 456/456 PASS
- grep 确认 `.lock().unwrap()` / `.read().unwrap()` / `.write().unwrap()` 已无

---

## 替换模式

唯一替换规则：

```
// 之前
.lock().unwrap()
.read().unwrap()
.write().unwrap()

// 之后
.lock().unwrap_or_else(|e| e.into_inner())
.read().unwrap_or_else(|e| e.into_inner())
.write().unwrap_or_else(|e| e.into_inner())
```

完全不改其他代码，不缩进，不换行，不改函数签名。
