# Cowd Core

> `v0.9.671` · Rust 原生 AI Harness 执行内核与统一控制面

Cowd Core 负责对话、任务、团队、工具、上下文、记忆、现实数据和外部入口的统一治理。它把模型擅长的语义判断与确定性的身份、权限、依赖、并发、幂等、恢复和证据提交结合起来。

## 全局架构

```text
 Human / AI / External systems
       │
       ├─ TUI (Core)
       └─ WebUI / Feishu / Email / WeCom / WeChat / Sources (Cowd Edge)
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ Gateway                                                      │
│ Auth · HTTP/SSE · Capability Contract · SurfaceHost · AppHost│
└─────────────────────────────┬────────────────────────────────┘
                              │ RuntimeService
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ AI Harness Runtime                                           │
│ Session → Turn → Task → Mission → ExecutionGraph             │
│                    Team → Agent → Skill / Tool → Evidence     │
│ Context · Policy · Approval · Provider · Recovery · Evolution│
└───────────────┬───────────────────────┬──────────────────────┘
                │                       │
                ▼                       ▼
       Reality Core               Execution resources
  Fact · Memory · Matrix      Provider · Tools · MCP · Plugins
                │                       │
                └───────────┬───────────┘
                            ▼
          Unified SQLite or PostgreSQL persistence

Product apps such as MFG consume App SDK + Reality + Runtime contracts.
They do not enter or fork the kernel.
```

## 核心所有权

| 层 | 负责 | 主要包 |
|---|---|---|
| 合同 | 稳定 DTO、执行语义、模型协议和 APP ABI | `harness-contract` · `model-protocol` · `app-sdk` |
| Runtime | Session、Task、Mission、Agent/Team、上下文、权限、执行与恢复 | `runtime` · `session` |
| Reality | 事实语义、分层记忆、实体/关系/指标/证据 | `fact-kernel` · `memory` · `matrix-*` |
| 能力 | 薄工具、Skill、MCP 与 Plugin | `tools` · `skill` · `mcp` · `plugins` |
| 服务 | 认证、API、实时投影、Edge 和 APP 托管 | `gateway` · `surface` · `connector` · `app-host` |
| 入口 | 生命周期命令与终端控制面 | `cli` · `tui` |
| 存储 | 单一后端选择、连接池、迁移和领域实现 | `storage` · `*-sqlite` · `*-postgres` |

Gateway 是唯一外部服务入口，但不执行模型对话循环；Runtime 是唯一执行所有者，但不持有 Surface。Tools 只实现原子动作，不启动 Agent 或管理 Harness 生命周期。MFG 等业务 APP 只消费稳定合同。

## 一次任务如何运行

```text
input
  → Gateway authentication / idempotency
  → Session inbox and Task routing
  → context compilation and Reality recall
  → model strategy / team proposal
  → Runtime authority binding and DAG validation
  → governed concurrent Agent / Skill / Tool execution
  → verification, evidence, terminal commit
  → Gateway snapshot / delta / SSE projection
  → TUI, WebUI, or external-channel reply
```

运行中的补充消息进入 Turn Inbox，由 Runtime 在安全点判断合并当前 Turn、重规划、排队、创建新 Task 或取消。Session 保存完整对话和恢复历史；Task 表示可验收工作；Mission 聚合长期目标；ExecutionGraph 只保存一次调度事实。

## 快速使用

```bash
cowd config doctor
cowd doctor

cowd gateway start
cowd gateway status
cowd gateway doctor
cowd gateway open

# 正式 full 构建包含 TUI 与第一方 APP
cargo build -p cli --release --features full
```

`cowd` 无子命令时进入 TUI。CLI 不保留旧式 REPL 或第二套 `run_prompt` 循环；TUI、WebUI 和消息渠道都通过 Gateway 使用同一个 Runtime。

## 系统说明书

打开 [中文默认的 HTML 说明书](docs/manual/index.html)，页面右上角可切换英文：

| 分册 | 内容 |
|---|---|
| [系统总览](docs/manual/index.html) | 三仓关系、能力地图和架构不变量 |
| [架构与边界](docs/manual/architecture.html) | 所有权、合同/端口/实现、crate 地图和修改规则 |
| [Runtime 与协同执行](docs/manual/runtime.html) | Session/Task/Mission、Team/Agent、连续输入、并发和恢复 |
| [Reality、记忆与 Matrix](docs/manual/reality.html) | 运行时上下文、分层记忆、知识治理、事实与证据 |
| [Gateway、API 与 Surface](docs/manual/gateway.html) | API 合同、实时投影、可靠消息、Edge 与 APP Host |
| [快速使用与运维](docs/manual/operations.html) | 配置、Provider、权限、存储、排障和验证门禁 |

机器可读 API 参考见 [Gateway API 文档](docs/api/gateway-api-reference.md)，活跃架构与操作手册索引见 [docs/README.md](docs/README.md)。历史能力 Dashboard 仅作为阶段快照保留，不代表当前运行状态。
