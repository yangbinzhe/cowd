# Cowd 文档

Cowd 是一套运行时内核 + 多表面（TUI/WebUI/外部连接器）的工程与制造运营协作系统。文档按“架构 / 运维 / 手册”分层，旧的零散文档已归档到 `plan/archive-docs-20260811/`。

## 能力全貌

| 域 | 能力 | 文档 |
|---|---|---|
| 执行内核 | Session/Task/Execution/Team/Agent 生命周期、事件溯源、恢复 | [architecture](architecture/README.md) |
| 编排 | 语义编译、证据租约、blocked 自动恢复、多团队并行 | [architecture](architecture/README.md) |
| 权限与审批 | 权限上限、审批队列、skip/deny、按节点等待 | [architecture](architecture/README.md) |
| 存储 | PostgreSQL 默认、SQLite 冷启动回退、平迁与归档 | [architecture](architecture/README.md) |
| 记忆 | L0-L4 分层、L0 身份引导（角色/语言）、抽取与治理 | [architecture](architecture/README.md) |
| 实时 | live subscription、SSE 投影、断线恢复 | [operator](operator/README.md) |
| 部署 | 版本发布、存储命令、健康检查、常见故障 | [operator](operator/README.md) |
| 工具系统 | bash 异步/head-tail/环境策略、网络域策略、ast/vision/时间/上下文工具、command_category | [architecture](architecture/README.md) |

## 快速入口

- 系统全貌与版本：仓库顶层 [README](../README.md)。
- 架构设计：[architecture/README.md](architecture/README.md)。
- 运维与故障处理：[operator/README.md](operator/README.md)。
