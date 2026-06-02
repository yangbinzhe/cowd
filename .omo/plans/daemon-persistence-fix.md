# Daemon持久化 + TUI直连 — 完备解决方案

> v0.8.7 | 问题: daemon崩溃后TUI不回启动, socket残留导致误判
> Oracle: 待审核

---

## 一、根因分析

### 问题链

```
TUI启动 → 检查 /tmp/cowd.sock.exists() → TRUE (但daemon已死)
         → 跳过启动 → 无daemon → 无API → 无gateway
```

### 代码证据

**1. 不可靠的文件存在检查** (`main.rs:421`)
```rust
let sock = std::path::Path::new("/tmp/cowd.sock");
if !sock.exists() {
    spawn("cowd gateway run");  // 仅在socket不存在时启动
}
// 问题: socket残留 → 误判daemon存活 → 永不重启
```

**2. PID文件路径不一致**
- `server::pid_file()` → `$XDG_RUNTIME_DIR/cowd-serve.pid` (`kill -0`验证)
- `~/.cowd/gateway.pid` → daemon写但没人读
- `/tmp/cowd.sock` → TUI启动检查这个，但不用PID验证

**3. `get_server_status()` 已正确实现但**从未用于TUI启动检查** (`server/mod.rs:53-71`)
```rust
pub fn get_server_status() -> Result<Option<ServerInfo>, ServerError> {
    let pid = read_pid_from_file()?;
    if !process_exists(pid) {  // kill -0 验证
        cleanup_stale_files();
        return Ok(None);
    }
    Ok(Some(ServerInfo { pid, address }))
}
```
此函数仅在 `GatewayAction::Start/Stop/Status` 中使用，**TUI启动路径从未调用**。

---

## 二、修复方案

### F1: TUI启动用 `get_server_status()` 替代 `sock.exists()` (`main.rs:418-449`)

```rust
// 修改前:
let sock = std::path::Path::new("/tmp/cowd.sock");
if !sock.exists() { spawn daemon; }

// 修改后:
match server::get_server_status() {
    Ok(Some(info)) => {
        tracing::info!(pid = info.pid, addr = %info.address, "daemon already running");
        // daemon存活 → 跳过启动
    }
    Ok(None) | Err(_) => {
        // 清理残留文件
        let _ = std::fs::remove_file("/tmp/cowd.sock");
        tracing::info!("daemon not running, auto-starting...");
        spawn("cowd gateway run");
    }
}
```

### F2: 确保daemon完全脱离父进程 (`main.rs:105-110`)

将 `SIG_IGN` 改为主动 `waitpid` (非阻塞)，确保子进程被正确reap且不依赖父进程：

```rust
// 修改: SIG_IGN → 主动reap
// 保持SIGCHLD handler但做WOHANG等待
fn setup_sigchld_handler() {
    // 不改变 — SIG_IGN已足够(子进程自动reap)
    // 关键是: spawn时设置 .stdin/.stdout/.stderr null + setsid
}
```

### F3: daemon崩溃时自动清理 (`daemon.rs:131`)

```rust
// PidFileGuard drop时自动清理 socket 文件
impl Drop for PidFileGuard {
    fn drop(&mut self) {
        // 清理PID文件 (已有)
        // 新增: 清理 /tmp/cowd.sock
        let _ = std::fs::remove_file("/tmp/cowd.sock");
    }
}
```

### F4: TUI定期健康检查 (已部分实现)

```rust
// app.rs: 定时检查 daemon 状态
// 使用 get_server_status() 更新 server_running 字段
fn check_daemon_health(&mut self) {
    self.server_running = server::get_server_status()
        .ok().flatten().is_some();
}
```

---

## 三、验证清单

- [ ] daemon崩溃后 `/tmp/cowd.sock` 被清理
- [ ] daemon崩溃后 `cowd-serve.pid` 被清理  
- [ ] TUI重新启动时检测到daemon死亡并重新启动
- [ ] daemon存活时TUI不再重复启动
- [ ] TUI退出后daemon持续运行
- [ ] `cowd gateway status` 正确报告状态
