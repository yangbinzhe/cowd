# Storage Governance

状态：V581 已实施 process-wide selected backend、全域 SQLite→PostgreSQL 迁移证据与原子切换。

Cowd 把业务存储放在稳定 port 之后，并由 Gateway 启动时唯一的
`SelectedStorageTopology` 一次性选择 SQLite 或 PostgreSQL。业务 route、service、Runtime
turn 与 App 都不得在请求期间自行判断后端或重新打开数据库。

## Principles

- Runtime entry surfaces do not open concrete stores directly.
- Gateway routes call Gateway services；service 只持有已选择的 port。
- 一个进程只有一个 backend owner 和一个有总预算约束的 PostgreSQL `PoolSet`；关键写、
  在线读和后台任务使用相互隔离的连接池。
- Matrix facts, Memory records, Session state, Approval state, and tool
  execution evidence must remain addressable through stable service contracts.
- active cutover manifest 只证明首次 SQLite -> PostgreSQL 切换，不参与后续正常启动。
  PostgreSQL Runtime 启动只读校验当前二进制注册的 migration catalog，缺失或不一致即拒绝
  启动并要求离线 `cowd storage upgrade`，不隐式执行 DDL，也不回退 SQLite。
- Gateway 只有在配置成功解析且 `storage.backend` 本身为 `sqlite` 时才选择 SQLite。配置
  文件损坏、字段非法或 PostgreSQL 拓扑不完整时拒绝启动，禁止用空配置掩盖错误。

## Selected domains

| domain | owner | notes |
|---|---|---|
| session | `session` | unified session metadata and event log |
| memory / knowledge | `memory` | recall, layers, packets, knowledge fabric |
| runtime event / task | `runtime` / `gateway::TaskKernel` | execution evidence and durable task state |
| fact / growth | `fact-kernel` | facts and growth ledger |
| matrix | `matrix-repository` | structured facts, entities, relations, evidence |
| approval | `approval` | approval history and decisions |
| surface message | `surface` | inbox/outbox/delivery evidence |
| connector directory | `connector` | external resource directory |
| artifact | `runtime` | Resource、Tool raw 与 Evidence 的统一 CAS、授权读取、配额和 GC |
| App relational storage | each App bundle | App-owned schema, snapshot and migration hook |

## Runtime configuration

`storage.backend=auto` is the default: PostgreSQL is preferred, and SQLite is used
automatically when PostgreSQL is not configured or is unreachable at cold start.
Fallback is cold-start only, never a hot switch and never dual-write; the effective
backend and reason are recorded in `<config_home>/storage/fallback.json` and exposed
through health. `backend=postgres` keeps the fail-fast contract, `backend=sqlite` keeps
the pure local mode, and `cowd storage adopt-postgres` explicitly re-adopts PostgreSQL
after a fallback. PostgreSQL configuration contains only a logical identity and secret
reference; the resolved URL never enters config projection, health or cutover evidence.

```yaml
storage:
  backend: auto
  preferred: postgres
  fallback: sqlite
  fallbackProbeTimeoutMs: 3000
  sessionExecution:
    workers: 8
    queueCapacity: 64
  artifacts:
    compactThresholdBytes: 262144
    maxObjectBytes: 536870912
    totalQuotaBytes: 21474836480
    gcHighWaterBytes: 19327352832
    gcLowWaterBytes: 17179869184
    orphanGraceMs: 86400000
  postgres:
    logicalIdentity: cowd-primary
    secretRef: file:postgres-primary
    maxConnections: 48
    serverReserve: 8
    critical:
      maxConnections: 16
      minIdleConnections: 3
      checkoutTimeoutMs: 250
    onlineRead:
      maxConnections: 24
      minIdleConnections: 4
      checkoutTimeoutMs: 500
    background:
      maxConnections: 8
      minIdleConnections: 2
      checkoutTimeoutMs: 2000
```

`maxConnections` 是 Cowd 进程可使用的总预算，不是每个池的预算。三个 lane 的
`maxConnections` 可以全部省略，由系统按 `16:24:8` 的比例分配；也可以全部显式配置，
但三者之和必须等于总预算。启动时系统读取 PostgreSQL `max_connections`，扣除
`serverReserve` 后再次校准，保证数据库仍有管理和其他客户端所需的连接。不得只配置部分
lane，也不得继续使用旧的根级 `minIdleConnections` 或 `checkoutTimeoutMs`。

Artifact 元数据和小对象随 selected backend 使用 SQLite/PostgreSQL；大对象只通过
`StorageDomainId::Blobs` 的选定目录访问。公开 Resource/Evidence DTO 仅返回
`artifact://` selector、hash、大小、媒体类型和可见域，不返回宿主路径。Gateway 启动时会
幂等迁移旧 `storage/resources/objects` 与 Session 内联 raw evidence，并在
`~/.cowd/migrations/artifact-v1-report.json` 留下 hash 校验、游标和完成状态；迁移未完成
会阻止新旧语义混用。

`secretRef` accepts two process-boundary schemes:

- `file:SECRET_ID` reads `<config_home>/secrets/SECRET_ID`. `SECRET_ID` is one file name, symbolic
  links and path traversal are rejected, and Unix permissions must not grant group or other access.
  This is the default for a long-running local Gateway because daemon restarts do not depend on the
  invoking shell retaining an environment variable.
- `env:VARIABLE` reads an externally injected environment variable and remains the deployment
  contract for containers and managed service launchers.

Neither scheme exposes the resolved URL through config projection, health, Debug output or evidence.
The configuration file remains the topology truth and stores only the reference. A PostgreSQL App
lease receives a validated, schema-scoped executor. Each pooled connection records its current
namespace and changes `search_path` only when the next checkout targets another namespace. A public
checkout therefore pays no repeated reset query, while an App schema still cannot leak into core or
another App through a reused connection.

`PostgresExecutor` 只公开运行时安全的连接包装，不把同步驱动连接直接交给 async 调用方。
Repository 方法必须在 checkout 前显式选择 `critical`、`online_read` 或 `background`；
禁止根据 SQL 文本猜测工作负载，同一事务也不能跨 lane。关键输入、审批、副作用和终态写入
使用 `critical`，交互式历史和召回使用 `online_read`，治理、索引、导入导出和全量扫描使用
`background`。后台池耗尽不会占用关键写入连接。

同步驱动执行规则：
多线程 Tokio worker 使用 `block_in_place`，current-thread runtime 使用有界 OS 线程桥接。
生产 Gateway 也直接从 `SelectedStorageTopology` 组装 service，不先构造 SQLite baseline 再覆盖，
因此选择 PostgreSQL 后不会暗中创建第二套业务 SQLite executor。通用 App 的健康、路由和能力
由 `AppRegistry` 验证，核心 service readiness 不硬编码任何具体 App 名称。

## Offline cutover

Gateway 必须停止，且四步必须使用同一份配置、工作区和已编译产品：

```bash
cowd storage plan
cowd storage migrate
cowd storage verify
cowd storage cutover
```

- `plan` 只输出经过脱敏的源 inventory、目标逻辑身份、逐域和启用 App 清单。
- `migrate` 在 maintenance barrier 下并行迁移 11 个核心/应用域；每个域重新读取源和目标，
  只在 canonical digest 相等时写 staging evidence。
- `verify` 校验完整 evidence envelope，并通过生产 composition code 重开所有 PostgreSQL
  adapter 和启用 App storage。
- `cutover` 只把已验证 manifest 原子发布为 active；不修改配置、不双写、不自动回退。

离线 `migrate -> verify -> cutover` 的同一轮执行绑定 Cowd 版本、工作区、目标 logical
identity、目标 secret reference、App immutable source lock 和当前 enabled App 集合，防止
迁移中途更换代码。发布后的 active manifest 只保留首次切换的历史证据，不再进入正常启动
关键路径。后续二进制包含新 migration catalog 时，Gateway 停止后执行：

```bash
cowd storage upgrade
```

该命令以 maintenance 模式加载当前所有 PostgreSQL adapter 和已启用 App，在 advisory lock
和 checksum 门禁下幂等升级 schema。Gateway Runtime 模式只登记期望 catalog，组合完成后按
namespace 批量只读校验；它不创建 schema、不执行 migration transaction，也不读取旧 SQLite。
因此升级没有完成时会快速失败，完成后每次启动不再重复迁移校验事务。

本机 PostgreSQL Release 部署统一使用：

```bash
COWD_BIN=target/release/cowd scripts/release/deploy-postgres-to-ai.sh
```

该入口先验证候选版本，再停止旧 Gateway、原子安装单二进制、执行 `storage upgrade`、启动
Gateway，并运行 status 和 doctor。upgrade 或启动失败时保持 fail-closed，不回退 SQLite，
也不保留旧二进制备份。

## App ownership

Cowd 只编排通用 `StaticAppProduct` storage migration hook，不认识 App 的表或 DTO。拥有
relational requirement 的 App 必须在自己的 bundle 中 export/import/re-read canonical snapshot，
并返回 source/target digest evidence；enabled App 缺 hook 或 evidence 时 cutover fail closed。
PostgreSQL 中每个 App 使用由宿主分配的独立 schema，MFG 只是首个真实实现。

## Evolution boundary

新增 SQL 后端必须实现现有 domain port 与 canonical migration contract，不得让 route、TUI、
WebUI 或 App 业务层出现 backend 分支。文件型 vector index、blob 与 definitions 可以作为明确的
可重建 artifact 存在，但不能成为 PostgreSQL 模式下隐式写入的第二份业务真相。
