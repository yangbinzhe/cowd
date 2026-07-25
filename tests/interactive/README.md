# Cowd Interactive Test Framework

这是独立于主 workspace 的 TUI/Gateway 人工诊断工具。它使用 tmux 驱动真实
TUI，通过 HTTP 观察 Gateway，并可选使用 LLM 评价回复质量。

它不属于默认发布门禁，原因是部分场景依赖终端时序、真实 provider、固定端口和人工
视觉判断。确定性的协议、状态、持久化与发布闭环由主仓 `scripts/validate.sh` 分层负责。

## 结构

```text
tests/interactive/
├── Cargo.toml
├── README.md
├── SCENARIOS.md
└── src/
    ├── api.rs
    ├── llm.rs
    ├── main.rs
    ├── reporter.rs
    ├── server.rs
    ├── tui.rs
    └── scenarios/
```

实际场景及失败模式见 [SCENARIOS.md](SCENARIOS.md)。注册表
`src/scenarios/mod.rs` 是可执行场景的唯一来源。

## 使用

```bash
export COWD_BIN=/path/to/cowd
cd tests/interactive

cargo run -- --list
cargo run -- tui_startup
cargo run -- --scenarios tui_basic,cross_cut
cargo run -- --all
```

## 依赖

- `cowd`：通过 `COWD_BIN` 指定。
- `tmux`：驱动和捕获真实终端。
- `curl`：调用 Gateway API。
- Provider 凭据或本地模型：仅在运行 LLM 评价场景时需要。

## 治理规则

- 不允许以源码文件存在证明平台或业务能力。
- 不允许请求旧 API 后忽略返回内容。
- 不允许为同一个面板或会话列表重复启动一套场景。
- 场景只有在断言确定、资源隔离且失败模式唯一时，才可提升到默认门禁。
- 该 crate 通过自己的 `[workspace]` 保持独立，主仓 `cargo build --workspace`
  不会编译它。
