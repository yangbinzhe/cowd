# 架构

## 运行时

```text
Surface（TUI / WebUI / Connector）
  -> Gateway（鉴权、API、SSE、审批投影、容量）
    -> Runtime（Session/Task/Mission/Execution）
      -> 执行图（节点并行：model/team/agent/tool/approval/synthesize）
      -> Storage（PostgreSQL 默认；SQLite 仅冷启动回退）
```

- 会话输入经有界执行平面进入 Runtime，每会话串行、跨会话并行。
- 执行图节点用 `JoinSet` 并行；审批节点只阻塞自身依赖路径。
- 所有终态、证据、投影均来自 Runtime 事件溯源，Surface 只做投影。

## 编排与并行

- 模型只提出语义拓扑；Runtime 负责租约、权限、编译、终态。
- 会话内团队提案无条件获得 `session:` 证据租约；read-only 不再无租约可编译。
- `blocked/rejected` 携带 `RecoveryHint`，编译自动重试 ≤2 次（补租约/回绑默认 mission）。
- 并行 ceiling 自动抬升到提案宽度并记录 `parallel_ceiling_elevated_for_explicit_team`；真实并发仍受资源管理器上限约束。
- 团队 Agent 的 `team_board`/`evidence_retrieve` 通过 RuntimeExecutionHost 委托执行，不落入 ToolHost 无适配器分支。

## 审批

- 审批队列按 approval_id 等待，节点级阻塞；WebUI/TUI 可批准、拒绝、跳过。
- `skip` 只对只读/可逆节点放行且不产生 grant；写节点必须 deny 或等待。
- 审批等待状态通过 `/api/approval/pending` 与 SSE 投影，任意页面自动弹出并标注所属 session。

## 存储

- `storage.backend=auto`（默认）：PG 优先，冷启动不可用/未配置时回退 SQLite，写 `fallback.json` 并标记 degraded；禁止热切换与双写。
- `postgres` 为 fail-fast；`sqlite` 为纯本地模式。
- 平迁命令：`cowd storage plan|upgrade|migrate|verify|cutover`；回退后 `cowd storage adopt-postgres` 显式接管。
- 历史 SQLite 数据在 cutover 后归档（本机已归档并清理）。

## 记忆（L0-L4）

- L0 身份（角色/语言）只允许 User/System 写入；`memory.identity.role/language` 由系统启动时一次性写入 L0。
- L1 工作记忆、L2 项目、L3 深层、L4 共享；Assistant/Tool 不允许写 L0。
- 抽取失败会严格重试一次，仍失败则保留原始证据并降级。

