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

## 工具系统（v0.9.675）

### bash：异步执行与真实沙箱开关

- bash 走统一的 `ToolHostLease::execute_async` 入口：异步子进程、进程组回收（超时杀整组）、
  IO 排空 2s 上限、增量 progress 样本；其他工具回退到受校验的 `spawn_blocking` 路径，两条路径
  共用同一套 lease/权限/效果校验。
- 返回体积有界：stdout/stderr 各保留 head/tail 64KiB，超限返回截断标记，并把全量写入
  `persistedOutputPath` artifact 供 `evidence_retrieve` 读取。
- 沙箱开关已接线到真实字段：`dangerouslyDisableSandbox → require_kernel_hardening=false`、
  `isolateNetwork → network_enabled=false`、`allowedMounts → readable_roots`；
  装饰字段 `filesystemMode/namespaceRestrictions` 已从 schema 删除。
- `dangerouslyDisableSandbox=true` 会把 bash 效果抬升为 Process/User 审批类，绝不落入
  只读确定性放行路径。

### shell 环境策略（env）

`bash.env` 支持 `inherit: safe|all|none`、`includeOnly`、`exclude`、`set`。
默认 `safe` 只继承 locale/proxy 白名单；`all` 仍默认屏蔽 secrets（token/secret/password/
credential/api_key/access_key/private_key/auth 等）与 `COWD_*` 控制变量；
显式 `includeOnly` 可强制包含，`set` 提供受控覆盖。

### 网络域策略与搜索质量

- 统一网络域策略：`COWD_NETWORK_DOMAIN_MODE=allow|ask|deny` +
  `COWD_NETWORK_DOMAIN_ALLOW/BLOCK`。模型参数只能收窄、不能放宽；违规返回结构化
  `networkPolicy.violations` receipt。默认屏蔽私网/回环/link-local 目标。
- `web_search` 支持 `recency`（any/day/week/month/year）、publisher 去重、freshness 字段；
  `web_fetch` 同样执行域策略与重定向终态复核。

### 能力清单

| 工具 | 说明 |
|---|---|
| `ast_grep_search` | 语言→扩展名过滤 + 正则行匹配，防逃逸、有上限；`ast_search` 保留别名 |
| `vision_analyze` | 本地图像准备为多模态块（端到端测试覆盖） |
| `current_time` | UTC/RFC3339 时间（纯本地实现） |
| `get_context_remaining` | 由 Gateway 从 live execution ledger 回答窗口/用量/剩余 |
| `request_plugin_install` | 显式注册但执行时 fail-closed：插件安装属于运维控制面 |
| `tool.invocation.*` | 事件携带 `command_category`（read_only/write/network/...），活动树显示标签 |

### 沙箱能力矩阵

| 平台 | 能力 | 状态 |
|---|---|---|
| Linux | bwrap + Landlock/seccomp（inner role 可用时），无权限回退 | 支持 |
| Linux（无 inner role） | bwrap Restricted 降级 | 仅 `dangerouslyDisableSandbox=true` 允许 |
| Windows | WFP/受限令牌/ACL 隔离 | **不支持**（v0.9.675 明确放弃，见方案 W19） |
| macOS | 无 bwrap 等价物 | 不支持 |

## v0.9.676 收口

- 创建会话即可指定执行策略（`execution_policy_preset`），分支继承策略，全局默认执行模式可配置；
  审批超期可一键清理（`POST /api/approval/prune`，audited deny）。
- bash 组合只读命令（`ls && find 2>/dev/null | head` 等）不再进人工审批；写/网络/破坏性不变。
- mission/control 默认返回 graph 摘要（`detail=graph` 按需全量）；bash artifact 持久化到
  `~/.cowd/storage/bash-artifacts/` 并有 7 天 TTL（`cowd storage cleanup`）。
- embedding 默认 batch 20 + 400 自动降半；搜索 publisher 公共后缀感知；bash/并行阈值支持 env 覆盖。
- terminal ack 幂等收敛；L0 身份可通过 `/api/memory` 的 `layers_l0` 查看；doctor 报告 SQLite 残留。
- 沙箱测试支持 `COWD_SANDBOX_LAUNCHER_BINARY` 注入，CI 增加真实 bwrap 门禁。
