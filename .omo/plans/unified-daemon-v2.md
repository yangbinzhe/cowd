# 统一守护进程架构 —— 深度评估与 TDD 执行方案

## 当前架构评估（v0.5.3 现状）

### 目前做了什么
```
cowd CLI
  ├── TUI 模式 (cowd)
  │   └── 每次 turn: std::thread::spawn → 新 current_thread Runtime
  ├── 命令行模式 (cowd prompt "hello")
  │   └── TOKIO_RT (LazyLock) → Handle::current + try_current fallback
  └── Server 模式 (cowd serve)
      └── 独立 multi_thread Runtime
```

### 当前已修复的问题
- ✅ Prompt 模式: Handle::current → try_current + TOKIO_RT fallback
- ✅ TUI: catch_unwind 保护 turn 线程不崩溃
- ✅ 所有 block_in_place 清除
- ✅ 所有 Runtime::new 添加 try_current 守卫
- ✅ 79 处 PoisonError 防御
- ✅ 30+ 模型 context_window 表
- ✅ 456 测试通过

### 仍存在的问题
- ⚠️ TUI 每个 turn 创建/销毁 Runtime → tokio "Cannot start runtime within runtime" 在内部触发
- ⚠️ 每次 turn 产生 crash 日志条目（被 catch_unwind 捕获，TUI 不死）
- ⚠️ cognitive.rs 5 处 Handle::current().block_on() 在异步路径中不是纯 async
- ⚠️ 版本号未对齐（Cargo.toml 0.1.0 vs tag 0.5.3）

---

## 统一守护进程评估

### 架构对比

| 维度 | 当前 | 统一守护进程 |
|------|------|-------------|
| Runtime 数量 | 每 turn 1 个 current_thread + TOKIO_RT(全局) | 1 个 multi_thread |
| Turn 执行 | std::thread::spawn 独立线程 | Runtime 内部异步任务 |
| TUI 事件循环 | 同步 crossterm | tokio::select! 异步 |
| 内存开销 | 每 turn Runtime 创建/销毁 | 恒定 |
| Session 隔离 | 每进程独立 Session | 需要 Session Manager |
| TUI + API 共存 | 不支持 | 同时可用 |
| 崩溃恢复 | catch_unwind（治标） | 架构级消除（治本） |
| 代码改动量 | — | Phase1 小（~50行），Phase2+ 大（~1000行） |

### 真实利弊分析

**利：**

1. **根治崩溃**：单一 Runtime → 不存在嵌套 Runtime 场景。所有 `Handle::current().block_on()` 在 worker 池中执行，不触发 `enter_runtime` 冲突。

2. **Session 统一**：HTTP API / TUI / 飞书 / 企微 共享同一个 ConversationRuntime，所有接口看到相同的对话上下文。

3. **资源效率**：不再每次 turn 创建和销毁 Runtime（当前每次 turn 消耗 ~1MB + 线程栈）。

4. **平台一致**：`cowd serve` 已有飞书/企微/邮件适配器。统一守护进程后，TUI 也变成守护进程的一个客户端，架构天然统一。

**弊：**

1. **TUI 渲染阻塞风险**：当前 TUI 同步事件循环保证 60fps 渲染。改为异步 `tokio::select!` 后，如果 Runtime 被大模型推理占满，TUI 帧率可能下降。

2. **代码复杂度激增**：异步 TUI 需要处理 channel 通信、任务取消、超时管理——当前简单的 `std::thread::spawn + catch_unwind` 变为 `tokio::spawn + select + AbortHandle + timeout`。

3. **Session 管理**：当前 TUI 和 API 各自管理 Session。统一后需要一个 SessionManager 来处理并发多 Session（HTTP API 可能有 10 个并发用户，每个有自己的 Session）。

4. **调试困难**：单进程崩溃 = 全部失效。当前 TUI 崩溃只影响一个 turn，新架构一个 panic 可能带走整个守护进程。

5. **回退成本**：异步 TUI + 统一进程的改动量巨大（TUI 19 个组件 + state.rs + main.rs + conversation.rs），回退几乎等于重做。

### 分阶段方案（风险和收益平衡）

**不要一步到位做完全架构重构。** 分三阶段，每阶段都可独立验证。

---

## Phase 1: Runtime 统一（最小改动，最高收益）

**目标**：用一个 multi_thread Runtime 替代所有 Runtime::new() 和 TOKIO_RT，不改变 TUI 事件循环。

**改动**：
- main.rs: 创建一个 `static SHARED_RT: LazyLock<Runtime>` (替代 TOKIO_RT)
- 所有 `Builder::new_*().build()` 替换为 `SHARED_RT.handle().clone().block_on()`
- TUI turn 线程：不再创建新 Runtime，直接用 Handle::block_on()
- conversation.rs: 内部 `Handle::current().block_on()` → 在 SHARED_RT worker 池中执行，不嵌套

**改动量**：~80 行，影响 main.rs、bash.rs、executor.rs（主要是 Runtime::new 替换）

**验证**：
- TUI turn 不再崩溃（日志无新条目）
- cognitive.rs 的 Handle::current().block_on() 正常工作
- 性能无退化（multi_thread 替代 current_thread）

**TDD 测试点**：
- `SHARED_RT.handle().clone().block_on(run_turn_async(...))` 执行成功
- `Handle::current()` 在 turn 内部可用
- `Handle::current().block_on(inner_future)` 不触发嵌套 crash

**风险**：低。改动集中，可回退。

---

## Phase 2: Session 管理器（中等改动，功能增量）

**目标**：支持多 Session 并发管理，为 TUI + API 共存做准备。

**改动**：
- 新建 `crates/runtime/src/session_manager.rs`（已有基础，扩展）
- Session 模型：`SessionId → Arc<ConversationRuntime>`
- Session 生命周期：create / suspend / resume / archive / delete
- TUI Session 与 API Session 独立
- Memory 系统按 Session 隔离（利用现有 MemoryScope::Session）

**改动量**：~300 行，新增文件 + 修改 server/mod.rs

**验证**：
- 创建多个 Session 互不干扰
- TUI Session 和 API Session 各自独立
- Memory 查询仅返回当前 Session 的数据

**TDD 测试点**：
- 创建、暂停、恢复、删除 Session
- 两个并发 Session 的 token 计数独立
- Memory 写入不跨 Session 泄漏

**风险**：中。Session 逻辑分散在多个文件中。

---

## Phase 3: 异步 TUI + 守护进程合并（大改动，架构升级）

**目标**：TUI 和 Server 在同一个进程中运行。

**改动**：
- TUI 事件循环改为 `tokio::select!`
- Turn 执行从 `std::thread::spawn` 改为 `tokio::spawn`
- Server 和 TUI 共用同一个 ConversationRuntime 池
- Channel 通信替代共享状态

**改动量**：~1000 行，影响整个 TUI crate

**验证**：
- `cowd` 启动后，API 和 TUI 同时可用
- TUI 帧率保持 >30fps
- API 请求不阻塞 TUI 渲染
- 守护进程崩溃恢复

**TDD 测试点**：
- API POST /chat 和 TUI 键盘输入并发
- TUI 在大推理负载下不丢帧
- 守护进程 death + restart 恢复上次 Session

**风险**：高。涉及 TUI 全部组件重构。

---

## 推荐方案

**立即执行 Phase 1**（Runtime 统一）—— 根除崩溃，改动最小。

**v0.6.0 执行 Phase 2**（Session 管理器）—— 功能增量。

**v0.7.0+ 评估 Phase 3**（异步 TUI）—— 等 Phase 1+2 稳定后再做。

---

## Phase 1 TDD 执行计划

### G1: 共享 Runtime 可用
- `static SHARED_RT: LazyLock<Runtime>` 在 main.rs 定义
- main() 中 `SHARED_RT.block_on(async {})` 验证初始化

### G2: TUI turn 使用共享 Runtime
- `main.rs:2848` → 替换 `Builder::new_current_thread().build()` 为 `SHARED_RT.handle().clone()`
- 移除 `catch_unwind` 包装（不再需要）
- turn 执行成功

### G3: CLI run_turn 使用共享 Runtime
- `main.rs:3595/3646/3662/4527` → 替换 `Handle::try_current() + TOKIO_RT` 为 `SHARED_RT.handle().clone()`
- Prompt 模式正常执行

### G4: 零 Runtime 创建
- 所有 `Runtime::new()` 仅保留在 SHARED_RT 定义中的 1 处
- 其他位置全部替换为 Handle

### G5: 回归验证
- `cargo build --release` 零警告
- `cargo test -p cowd-memory --lib` 456/456 PASS
- TUI 交互无新 crash 日志（不再出现 "Cannot start runtime"）

---

## 开发分支策略

在 `develop` 分支上创建 feature 分支：

```bash
git checkout develop
git checkout -b feature/unified-runtime
# Phase 1 开发
# TDD: 写测试 → 实现 → 验证
# 合并回 develop
git checkout develop
git merge feature/unified-runtime
# 构建 v0.6.0-rc1
cargo build --release
```

develop 分支保持与 master 独立，Phase 1 完成后可以独立验证，不影响 master 的稳定版。
