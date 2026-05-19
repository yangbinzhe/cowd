# Cowd 计划2 — Phase 1: main.rs 拆分

> 优先级：P0 | 前置：Phase 0 | 工时：8h
> GitNexus 影响：main.rs (cowd-cli 入口) → 5 个独立文件

## 拆分策略

```
当前: crates/cowd-cli/src/main.rs (9000+ 行推测)

目标:
  crates/cowd-cli/src/main.rs        → 仅剩 100 行启动代码
  crates/cowd-cli/src/cli/mod.rs     → CLI 入口 (~400 行)
  crates/cowd-cli/src/cli/args.rs    → 参数解析 (~300 行)
  crates/cowd-cli/src/cli/repl.rs    → REPL 循环 (~400 行)
  crates/cowd-cli/src/tui/mod.rs     → TUI 入口增强 (~500 行)
  crates/cowd-cli/src/tui/turn.rs    → Turn 执行逻辑 (~600 行)
```

## 执行步骤

### Step 1.1: CLI 参数解析独立 (1.5h)

**新建文件**：`crates/cowd-cli/src/cli/mod.rs`, `crates/cowd-cli/src/cli/args.rs`

**从 main.rs 迁移**：
- `--model` / `--yolo` / `--port` / `--tui` / `--reasoning-effort` 等 30+ 参数
- `CliArgs` struct 定义
- 参数验证逻辑

**修改点**：`main.rs` 中删除对应的参数解析代码，替换为 `cli::args::parse()`

**验证**：`cargo test -p cowd-cli -- cli` 所有 CLI 解析测试通过

---

### Step 1.2: REPL 循环独立 (1.5h)

**新建文件**：`crates/cowd-cli/src/cli/repl.rs`

**从 main.rs 迁移**：
- `run_repl()` 函数
- 输入读取循环
- 斜杠命令分发
- `handle_repl_command()` 逻辑

**修改点**：`main.rs` 中 `run_repl()` 替换为 `cli::repl::run(state)`

**验证**：`cargo test -p cowd-cli -- repl`

---

### Step 1.3: TUI Turn 执行独立 (2h)

**新建文件**：`crates/cowd-cli/src/tui/turn.rs`

**从 main.rs 迁移**：
- `prepare_turn_runtime()` 
- `replace_runtime()`
- `persist_session()`
- `consume_stream()` → 改为 EventBus 发布（Phase 3 完成）
- `format_tool_call_start()` / `format_tool_result()`
- `describe_tool_progress()`

**并行子任务**：
- 1.3a: `prepare_turn_runtime` + `replace_runtime` → turn.rs
- 1.3b: `format_tool_*` + `describe_tool_progress` → turn.rs
- 1.3c: `consume_stream` → turn.rs (暂时保持 stdout 行为，Phase 3 改造)

**验证**：`cargo test -p cowd-cli -- turn`

---

### Step 1.4: main.rs 减肥 (2h)

**修改文件**：`crates/cowd-cli/src/main.rs`

**保留**：
```rust
fn main() -> Result<()> {
    let args = cli::args::parse();
    match args.mode {
        Mode::Repl => cli::repl::run(args),
        Mode::Tui => tui::run(args),
        Mode::Server => server::start(args),
        Mode::Prompt(text) => cli::single::run(args, text),
    }
}
```

**删除**：
- CLI 参数解析 → 已移到 `cli/args.rs`
- REPL 循环 → 已移到 `cli/repl.rs`
- TUI turn 执行 → 已移到 `tui/turn.rs`
- 消息格式化函数 → 已移到 `tui/turn.rs`
- Session 持久化 → 已移到 `tui/turn.rs`

**验证**：
- `cargo build -p cowd-cli` 编译成功
- `cargo test -p cowd-cli -- mock_parity_harness` 集成测试通过

---

### Step 1.5: 整合验证 (1h)

**验证清单**：
- [ ] `cargo check --workspace` zero errors
- [ ] `cargo test -p cowd-cli` 全量通过
- [ ] `cargo test -p runtime` 全量通过
- [ ] `cargo build --release` 成功
- [ ] `./target/release/cowd --help` 输出完整
- [ ] `./target/release/cowd --version` 正确

## 并行执行方案

```
Step 1.1 (args.rs) ──┐
                      ├── 并行（无交叉依赖）
Step 1.2 (repl.rs) ──┘
        ↓
Step 1.3 (turn.rs) ── 依赖 Step 1.1 (需要 args 类型)
        ↓
Step 1.4 (main.rs shrink) ── 依赖 Step 1.1 ~ 1.3
        ↓
Step 1.5 (verification)
```

## GitNexus 修改前影响分析

运行 `impact` 在 `run_tui_repl` 上确认不会破坏现有调用链：
- 调用者：`run_repl` (main.rs) — 保留引用不变
- 被调用：19 个模块 — 全部保留，只移动位置
