# Cowd

Cowd 是 Rust 原生的 AI Harness 核心仓库。当前核心版本：`0.9.353`。

本仓库只保留 AI Harness 内核、Gateway 服务、协议边界、事实/记忆/工具/技能/审批/会话等核心能力，以及可选的 TUI surface。WebUI、飞书、邮件、企微、微信 iLink 等非 TUI surface 统一迁移到独立仓库 `cowd-surface`。

## 架构边界

```text
cowd core
  crates/cli        极薄 CLI 入口，默认 debug 不编译 TUI
  crates/gateway    HTTP/SSE 服务入口，负责 RuntimeHost 与 SurfaceHost
  crates/runtime    AI Harness 运行时核心，不依赖 channel/surface SDK
  crates/surface    Surface JSONL 协议与 manifest 合同
  crates/tui        核心仓内唯一 UI surface，仅 full/release 联调时构建

cowd-surface
  surfaces/webui          WebUI 静态 surface
  surfaces/feishu         飞书 sidecar surface
  surfaces/email          邮件 sidecar surface
  surfaces/wecom          企微 sidecar surface
  surfaces/wechat-ilink   微信 iLink sidecar surface
```

核心原则：

- Runtime 不持有 channel，也不链接任何平台 SDK。
- Gateway 是唯一后端服务入口，负责 surface 发现、静态资源转发、callback 转发、health、events 和 JSONL sidecar 调度。
- TUI 和 WebUI 都只通过 Gateway HTTP/SSE API 使用核心能力。
- CLI 不做交互 UI，不承载业务执行器，只负责轻量命令、配置、诊断和 Gateway 启动。
- 默认开发/debug 构建不带 TUI，TUI 与 Gateway 分开开发；只有 TUI 联调、完整产品验证和正式 release 才构建 `--features full`。
- 非 TUI surface 不在 core workspace 编译，全部从 `cowd-surface` 按需独立构建和安装。

## 常用命令

默认开发检查，不带 TUI：

```bash
cargo fmt --all --check
cargo check
cargo check --workspace --exclude tui --no-default-features
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
cargo build -p cli --bin cowd
```

TUI 联调或完整构建：

```bash
cargo check -p cli --bin cowd --features full
cargo build -p cli --bin cowd --features full
```

Gateway 启动：

```bash
~/AI/cowd gateway
```

WebUI 由 `cowd-surface/surfaces/webui` 构建，Gateway 通过配置读取静态产物：

```yaml
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-surface/surfaces/webui/dist"
```

## 构建策略

- `cargo check` / `cargo build` 使用 workspace `default-members`，默认不包含 `crates/tui`。
- `crates/cli` 对 `crates/tui` 是 optional dependency。
- `--features full` 才会把 TUI surface 编进 `cowd`。
- `cowd-surface` 里的 sidecar 二进制独立构建，不进入 core 包体。

## 发布前验证

```bash
cargo fmt --all --check
cargo check
cargo check -p cli --bin cowd --features full
cargo test -p surface
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
```

外部 surface 仓库需要单独验证：

```bash
cd ../cowd-surface
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
cargo check --workspace --bins
```
