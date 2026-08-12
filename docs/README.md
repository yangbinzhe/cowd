# Cowd 文档

Cowd 是一套运行时内核 + 多表面（TUI/WebUI/外部连接器）的工程与制造运营协作系统。文档按“架构 / 运维 / 手册”分层，旧的零散文档已归档到 `plan/archive-docs-20260811/`。

## v0.9.677 收口要点

- 编排契约：`runtime_capabilities` 暴露模板角色目录与最小合法提案示例；`can_execute_now` 改为对推荐提案做真实 preflight（含租约 pattern 冲突检测）；`route_input` 明确 unsupported；ceiling 修复项从拒绝 findings 拆分为 adjustments；拒绝信息带 `lease_pattern_available` 与可执行恢复提示。
- 前端交互：用户/系统消息可复制；每个最终结果可一键 fork 新 session（store 级防抖）；发布强制浏览器 smoke + dist 静态引用完整性；CI 新增 e2e。
- 权限与安全：网络域策略并入配置（env 优先、config 兜底），非法值启动拒绝（fail-closed Deny）；bash 只读判定全链取最高风险。
- 存储：新增进程级 SQLite 池计数（`memory::sqlite_pool_instance_count`），`cowd storage cleanup --sqlite-residuals` 引用感知归档；doctor 同步报告池数。
- 冷启动：mission summary 缓存后台预热（不创建默认 Mission、不阻塞 /readyz）；`scripts/manual/measure-cold-start.sh` 采样门禁（p95 ≤ 1.0s）。
- TUI：创建会话透传 `execution_policy_preset`。

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
