# Phase 1: SHARED_RT 统一 Runtime — TDD 分步执行

## 总体目标

用一个全局 `multi_thread` Runtime 替代所有 `Runtime::new()` 和 `Builder::new_*().build()`，根除嵌套 Runtime 崩溃。

## 当前基线

```
cargo build --release → 0 错误, 0 cowd 警告
cargo test -p cowd-memory --lib → 456 PASS
crash.log → 每次 TUI turn 仍有 "Cannot start runtime within runtime" 条目
```

## TDD 分步计划

---

### T1: 创建 SHARED_RT 全局 Runtime

**目标**: 在 main.rs 中创建一个 `static SHARED_RT: LazyLock<Runtime>`，用 `multi_thread` + 4 workers 初始化。

**实现**: 
- 添加/替换 TOKIO_RT 为 SHARED_RT，配置 `multi_thread + 4 workers`
- 在 `run()` 函数开始处 force-initialize

**验证**:
- `cargo build --release` 通过
- `SHARED_RT.block_on(async { 42 }) == 42` — 在 status 命令中验证

---

### T2: TUI turn 使用 SHARED_RT

**目标**: main.rs:2848 不再创建新 Runtime，改用 `SHARED_RT.handle().clone().block_on()`

**实现**:
- 替换 `Builder::new_current_thread().build()` → `SHARED_RT.handle().clone()`
- 移除 `catch_unwind` 包装（无需防御）

**验证**:
- `cargo build --release` 通过
- TUI turn 执行成功（tmux 测试）
- 无新 crash 条目

---

### T3: 所有 Handle::current() 路径改用 SHARED_RT

**目标**: main.rs:3595/3646/3662/4527 的 `Handle::try_current()` → `SHARED_RT.handle().clone()`

**实现**:
- 4 处 CLI run_turn 路径替换

**验证**:
- `./target/release/cowd --solo prompt "hello"` 正常退出
- `./target/release/cowd --solo status` 正常显示

---

### T4: 移除所有 Runtime::new() 和 Builder::build()

**目标**: 全局只有 1 处 Runtime::new()（SHARED_RT 定义）

**实现**:
- bash.rs:101 → SHARED_RT.handle()
- executor.rs:3121 → SHARED_RT.handle()
- executor.rs:2584 → SHARED_RT.handle()
- main.rs:438 → SHARED_RT.handle()
- main.rs:3131 → SHARED_RT.handle()
- anthropic.rs:721 → SHARED_RT.handle()

**验证**:
- `grep "Runtime::new()\|Builder::new.*build()" crates/ --include="*.rs" | grep -v SHARED_RT | grep -v test` → 0 匹配

---

### T5: 回归验证

**目标**: 所有测试通过，构建零警告

**验证**:
- `cargo build --release` 零警告
- `cargo test -p cowd-memory --lib` 456/456
- `cargo test -p api --lib` 132/132
