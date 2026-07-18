# Cowd Edge 并发、吞吐与锁终态优化方案

日期：2026-07-17

规划版本：V550-V553（以当前 `cowd-edge` V548 工作区为前序；按用户指定从 V550 开始）

落盘分支：Cowd `master`

范围声明：独立 App 方案已按用户要求冻结。本方案只处理现有 Edge、Gateway `SurfaceHost`、消息/Source Connector 及 Edge ingress 的运行性能，不修改 MFG 独立 App 架构。

平台范围：V550-V553 以 Linux 为交付和性能验收平台，使用 UDS 与现有 sandbox。Windows transport 不在本轮范围内，本方案不得被解读为已经完成 Windows managed Edge；恢复 Windows 支持时需为 named pipe/loopback authenticated H2 单独设计与评测。

事实基线：

- Cowd：`master@b76f47b205cd`，版本 `0.9.533`。
- cowd-edge：`develop@d2ed41725801`，工作区正在进行未提交 V548 修改。
- 本次只读分析了 cowd-edge 工作区，禁止覆盖、清理或混入其未提交内容。

## 1. 结论摘要

当前 managed Edge 虽然运行在 Tokio 多线程 runtime 上，但业务执行被以下结构人为串行化：

```text
stdin line reader
  -> decode JSON
  -> await handle_frame                # 处理完一条才读下一条
       -> adapter Mutex
            -> await provider/network  # 跨外部 I/O 长时间持有全局适配器锁
  -> stdout Mutex
       -> write + flush                # 响应和主动事件竞争一个锁
```

这不是“单进程必然慢”，而是逻辑并发模型错误。一个 Edge 进程可以使用多线程 Tokio、连接池、并发 worker 和按 key 有序执行；为同一个 provider 盲目启动多个进程反而会重复长轮询、重复消费、破坏幂等和 provider 限流。

终态决策：

1. **Managed Edge 从 stdio JSONL 迁移到 Unix Domain Socket 上的 HTTP/2。** 每个请求是独立 H2 stream，天然支持多路复用、流控、取消和流式 Source body，不再自行维护 stdin/stdout 全局锁和 pending correlation。
2. **stdio JSONL 仅允许 OneShot 生命周期。** 当前 9 个可执行 Message/Source Edge 全部是 `managed`，终态 manifest 必须使用 `uds-http2`；Gateway 的 managed JSONL 路径必须删除。
3. **一个逻辑 Connector 保持一个受监管进程，但进程内不使用全局业务锁。** Reader/acceptor 只负责接入，业务由有界并发 worker 执行。
4. **消息适配器拆除 `Arc<Mutex<Box<dyn PlatformAdapter>>>`。** Adapter 使用 `Arc<dyn PlatformAdapter>`；connect/disconnect/receive 改为 `&self` + 内部窄状态，唯一 receive task 与并发 sender 分离。
5. **Source 使用长期连接池和按 resource key 有序状态机。** 不再每次请求创建 SQLx Pool、Reqwest Client 或重复 token 请求。
6. **Gateway ingress 先持久化，再有界调度。** critical event 不再依赖可能 Lagged 的 broadcast，也不再每事件无界 `tokio::spawn`。
7. **只保留必要的序列化。** 生命周期变更、同会话消息顺序、同 watermark key 提交必须有序；无关会话、无关 Source resource、health 与普通业务请求必须并行。
8. **合并实现与编译产物，不合并逻辑实例。** Feishu Bitable/Lark Base 共用一个 `cowd-edge-bitable-source`；PostgreSQL/MySQL/MariaDB 共用一个 `cowd-edge-sql-source`；Feishu Message artifact 使用中性的 `cowd-edge-open-platform-message` 命名并支持静态地域 profile。当前 9 个逻辑 Connector 保留独立 manifest、配置、进程、指标与故障域，但可执行产物从 9 个收敛为 6 个。
9. **已有 runtime 只能保留一个。** V551 以现有 `PlatformRuntime` 为重构基底并收敛为唯一 `MessageConnectorRuntime`，生产 H2 handler 直接接线；旧 `MessageSidecarState`/stdio loop 整体删除，禁止再新增第三套 runtime。
10. **跨仓库 wire DTO 必须只有一个规范源。** 当前 Cowd `surface` 与 cowd-edge `edge-contract` 已发生真实字段漂移，Source DTO 也有手写镜像；V550/V552 必须改成规范生成物 + hash gate，而不是继续人工同步。

## 2. 当前代码事实与瓶颈等级

### 2.1 Managed JSONL 传输

| 代码事实 | 位置 | 影响 | 等级 |
|---|---|---|---|
| 9 个可执行 Message/Source manifest 全部是 `managed + stdio-jsonl` | `cowd-edge/connectors/{message,source}/*/surface.json` | 所有生产 Edge 走同一 managed JSONL 实现 | 事实 |
| Gateway 为 managed process 保存 `AsyncMutex<ChildStdin>` | `crates/gateway/src/surface_host/types.rs` | 每个请求竞争 stdin 锁并逐帧 flush | P1 |
| Gateway stdout reader 使用 `BufReader::lines()` | `surface_host/supervisor.rs::start_managed_process` | 无统一最大行长度；大帧会扩大内存 | P0 安全/稳定性 |
| Gateway 用 pending `HashMap<String, oneshot>` 相关响应 | `surface_host/invocation.rs::invoke_managed` | timeout 只返回错误，没有立即删除 pending；迟到/永不响应可积累 | P0 |
| managed child stderr 设置为 pipe 但没有 reader | `surface_host/supervisor.rs` | Edge 日志或 panic 输出填满 pipe 后可阻塞整个子进程 | P0 |
| 非法 stdout JSON 被直接 `continue` | `surface_host/supervisor.rs` | 协议错误无计数、无熔断，调用方只能超时 | P1 |
| JSONL 每次构造 String 并追加换行 | `crates/surface/src/lib.rs::SurfaceFrame::encode_jsonl` | 有序列化、分配和复制，但不是目前首要瓶颈 | P2 |

### 2.2 Message Edge

| 代码事实 | 位置 | 影响 | 等级 |
|---|---|---|---|
| 主循环 `await handle_frame` 后才读取下一行 | `edge-adapters/src/message_sidecar.rs::run_stdio_platform_message_connector` | 所有 send/action/health/configure 串行 | P0 |
| Adapter 类型为 `Arc<Mutex<Box<dyn PlatformAdapter>>>` | `MessageSidecarState` | 所有 provider 操作共享一个 coarse lock | P0 |
| send 在 adapter lock 内 await provider I/O | `message_sidecar.rs::send_text_frame` | 一个慢发送阻塞接收、health 和其他发送 | P0 |
| receive 在 adapter lock 内 await 长轮询/WebSocket/IMAP | `message_sidecar.rs::spawn_receive_loop` | WeChat 长轮询、Email IMAP、Feishu receive 可阻塞全部 outbound | P0 |
| stdout 是 `Arc<Mutex<Stdout>>` | `message_sidecar.rs::write_frame` | 响应和 inbound event 竞争同一异步锁 | P1 |
| `PlatformAdapter` 的 connect/disconnect/receive 使用 `&mut self` | `platform/adapter.rs` | 迫使外层将整个 adapter 放进 Mutex，虽然四个实现主体已使用内部原子/RwLock | P0 carrier |
| Feishu WS 生产路径使用 unbounded event/write channel | `platform/feishu/ws.rs`、`feishu/adapter.rs` | Gateway/下游变慢时内存可以无界增长 | P0 |
| Email poll 与 WeChat `get_updates` 可能返回多条，但 `receive()` 返回第一条即结束 | `platform/email.rs::receive`、`wechat_ilink.rs::receive` | 批内剩余消息没有进入稳定的 adapter inbound queue，吞吐提升前必须封堵数据损失 | P0 |
| Edge adapter 生产源码存在大量 `reqwest::Client::new()` | provider/source hot paths | 连接复用、TLS session 和 DNS 缓存无法稳定复用 | P1 |

代码扫描基线：`message_sidecar.rs` 有 23 个 async lock site，其中 adapter send/receive 两处明确跨 provider await 持锁。

### 2.3 Source Edge

| 代码事实 | 位置 | 影响 | 等级 |
|---|---|---|---|
| 主循环同样 inline await `handle_frame` | `source_sidecar.rs::run_stdio_source_connector` | read/schema/incremental/health 全串行 | P0 |
| `SourceSidecarState` 使用单 AsyncMutex | `source_sidecar.rs` | 当前多数锁很短，但并发化后 watermark 必须避免 lost update | P1 carrier |
| 每次数据库 read/schema 都创建 SQLx Pool | `source_db.rs::{read,discover}_{postgres,mysql}*` | 重复连接、认证、握手；并发越高放大越严重 | P0 |
| 每批读取额外执行 `COUNT(*)` | `source_db.rs::{read_postgres_batch,read_mysql_batch}` | 大表全量 count 可能远慢于实际 bounded batch | P0 |
| Bitable read/schema/token 每次创建 Reqwest Client | `source_sidecar.rs` | 失去 HTTP keep-alive 和连接池复用 | P1 |
| Bitable 每次读取重新获取 tenant token | `read_feishu_bitable_batch` | 额外远端往返并可能触发认证限流 | P0/P1 |
| Source 最多将 1000 rows 放在一个 JSONL frame | `SourceRecordBatch` + `record_batch_from_rows` | 大行造成分配、队头阻塞和峰值内存 | P1 |

代码扫描基线：`source_sidecar.rs` 有 20 个 async lock site；Edge adapter 源码共有约 35 个 `reqwest::Client::new()` occurrence（包含不同 provider 和内联测试，实施时必须逐个分类）；`source_db.rs` 有 4 条按请求创建 pool 的生产路径。

### 2.4 Gateway ingress

| 代码事实 | 位置 | 影响 | 等级 |
|---|---|---|---|
| critical Surface events 先进入容量 1024 的 broadcast | `SurfaceHost::with_configs_and_message_store` | receiver Lagged 时当前 dispatcher 只记录 warn 并跳过，持久化发生在 broadcast 之后 | P0 可靠性 |
| retry/reconcile 和 live receive 在同一个 select loop | `spawn_surface_ingress_dispatcher` | 五秒维护分支执行期间停止读取 live events | P0 |
| 每个 event/message 使用 `tokio::spawn` | `enqueue_surface_trigger_event`、dispatcher | 没有全局并发上限，突发时任务和内存无界增长 | P0 |
| session lock map 只增不删 | `surface_session_lock` | 长期运行按会话数增长 | P1 |
| per-session lock 持续到 Runtime admission 完成 | `handle_surface_message` | 同会话有序是正确的；不同会话应由有界调度器并行 | 必要 carrier |
| retry records 使用 for-loop 逐个 await | `retry_surface_trigger_events` | backlog 恢复吞吐受单任务限制 | P1 |
| `SurfaceMessageStore` 用一个 `std::Mutex<SurfaceMessageState>` | `surface_host/message_store.rs` | inbox/outbox/trigger/delivery 所有写操作共享全局锁 | P0 |
| 每次状态追加都在锁内重新 `OpenOptions::open` JSONL | `message_store.rs::append_record`，约 11 类调用点 | 同步文件 I/O 阻塞 async worker；突发吞吐和启动恢复随日志增长退化 | P0 |

### 2.5 重复实现、产物与合同边界审计

| 对象 | 代码事实 | 结论 |
|---|---|---|
| Feishu Bitable / Lark Base | 两个 binary 入口都只调用同一个 `run_stdio_source_connector`；仅 `surface_id`、`adapter_id` 和默认域名分别为 `open.feishu.cn` / `open.larksuite.com` | **合并编译产物与实现**；保留两个 driver profile 和两个逻辑实例 |
| Bitable 当前构建产物 | 本地 Debug 文件分别为 106,460,016 与 106,459,432 bytes，合计约 203 MiB；hash 不同只是因为入口常量/目标名进入产物 | 改为一个构建目标；安装包只承载一个 artifact，两个 manifest 均引用它 |
| Open Platform HTTP/auth | Bitable Source 的 `tenant_access_token()` 与 Feishu Message adapter 重复实现同一鉴权；Message 代码也已支持 Lark 域名，但当前只有 Feishu Message manifest；`FEISHU_API_BASE` 还是进程全局 `OnceLock` | 抽实例级 `OpenPlatformClient`，统一安全域名、client、token singleflight/expiry、响应解码；Message/Bitable 各自持有实例和凭据，不跨进程共享 token |
| PostgreSQL / MySQL / MariaDB | 三个 binary 入口合计只有常量选择；业务已经在 `source_db.rs::DatabaseDialect` 分派，MySQL/MariaDB 又共用同一实现 | **合并为一个 SQL Source artifact**，用受校验 profile 选择 dialect |
| 4 个 Message Connector | binary stub 很薄且共用 `message_sidecar`，但 Open Platform WS、Email SMTP/IMAP、WeCom crypto、WeChat iLink 的协议、依赖、凭据和限流模型不同 | 保留 4 个协议族 artifact 并共用 runtime；Feishu artifact 改中性 Open Platform 命名，未来 Lark Message manifest 必须复用它而非新建 binary |
| `MessageSidecarState` / `PlatformRuntime` | 生产 binary 使用前者；后者已有 bounded channel、adapter loop 和测试，但生产无调用方 | 以 `PlatformRuntime` 为基底收敛唯一 runtime；删除 stdio sidecar 状态机，不能两套并存 |
| Cowd `surface` / Edge `edge-contract` | `message.rs` 当前逐字相同；`lib.rs` 已漂移：Edge 缺 `ArchiveDeadLetters`、`PurgeArchivedEvents` 与 `SurfaceFrame::Send.idempotency_key` | 这是已发生的合同缺陷；建立 canonical schema/codegen/hash gate，禁止手工镜像 |
| Core / Edge Source DTO | `SourceReadPlan`、Field/Table/Batch/Cursor/Watermark 在 `connector::source` 与 `source_sidecar.rs` 重复定义 | 抽到 canonical wire schema 生成物；业务 helper 留在各自 owner，不复制 DTO |
| Core / Edge Source catalog | Core `builtin_source_adapter_manifests()` 又硬编码 PostgreSQL/MySQL/MariaDB/Feishu/Lark，Gateway edge/connector/matrix 路由仍以它判断外部能力；Edge manifest 另有一份真实安装声明 | Core 静态 catalog 只保留 CSV/JSONL/SQLite 等 builtin；外部 Source catalog 从 SurfaceHost discovered manifest/profile metadata 投影，WebUI/TUI/API 不维护第三份表 |
| Source action aliases | Edge 同时接受 `source.incremental.run`/`source.incremental_run` 等多组字符串；Gateway 生产调用只使用 dotted canonical action | H2 v2 使用确定端点/规范 action；无生产 caller 的 underscore/倒序 aliases 在迁移测试后删除 |

合并边界不是“同一个品牌就放进同一个进程”，而是按四层分别判断：

1. **业务算法层：** 相同则只保留一份实现。
2. **可执行依赖层：** 依赖集合与权限面相同才合并 artifact；Bitable 两端相同，四个 Message 协议族不同。品牌地域不同但协议族相同不应复制 artifact。
3. **运行实例层：** 账号、地域、限流、故障和凭据需要隔离时仍启动独立进程；一个 artifact 可以启动多个实例。
4. **产品身份层：** Feishu/Lark 的名称、默认域名、配置、审计、指标和 WebUI/TUI 展示不能混成一个不可区分的 Connector。

### 2.6 旧符号分类

| 符号/结构 | 分类 | 当前责任 | 终态决定 |
|---|---|---|---|
| `SurfaceFrame` / `StdioJsonl` | 活跃 carrier | OneShot 与 managed 共用 JSONL wire | 保留 OneShot；禁止 managed 使用，不按名称整类删除 |
| `ManagedSurfaceProcess.stdin/pending/events` | 活跃 carrier | managed request correlation 与 event 收集 | V550 由 H2 connection/event handles替换后删除 |
| `invoke_managed` | 活跃生产函数 | managed JSONL send/action/health | V550 调用方改 H2 后删除 |
| `Arc<Mutex<Box<dyn PlatformAdapter>>>` | 活跃 service dependency | Message lifecycle/send/receive共享 owner | V551 由 Arc adapter + runtime lanes 替换后删除 |
| `PlatformRuntime`、`NullAdapter`、AdapterFactory | runtime 生产未接线，现有 dependents 主要是库内测试；其中 bounded channel/adapter loop 可复用 | 与生产 `MessageSidecarState` 形成双 runtime | V551 将它重构/更名为唯一 `MessageConnectorRuntime` 并接线，随后删除旧 sidecar runtime；不可原样保留两套 |
| `SourceSidecarState` | 活跃状态 carrier | config、health、watermark、last run/error | 拆为 immutable backend generation、atomic health、keyed watermark owner后删除旧大锁结构 |
| `reqwest::Client::new()` / `PoolOptions::new()` | 混合 | constructor、hot path、内联测试均有匹配 | constructor 允许；hot path 删除；测试逐项分类 |
| `event_tx: broadcast::Sender` | 活跃 carrier | critical ingress 与 observer 共用 | 保留 observer fanout；critical owner 迁出后降级为 projection |
| `session_locks` | 活跃 ordering carrier | 保证同 session admission 顺序 | durable keyed claim替换后删除，不能直接去锁 |
| `SurfaceMessageState + append_record/rewrite_records` | 活跃持久化 carrier | JSONL 状态、恢复、幂等和投影 | 先迁移 SQLite WAL repository 与 JSONL importer，再删除全局内存锁/逐次 open |
| `tokio::spawn` | 混合 | supervisor 常驻任务、测试、per-event无界任务 | 常驻受监管任务保留；per-event spawn 删除 |

## 3. 第一性原理与锁边界

### 3.1 不追求“零锁”，追求“无错误的共享所有权”

以下串行是必要的：

- 单个 Unix socket 的 accept/connection driver 由一个 owner 管理。
- 同一 provider lifecycle 的 configure/connect/disconnect 状态转换。
- 同一聊天 session 的有副作用 outbound 顺序。
- 同一 Source resource/watermark key 的 incremental claim 与 commit。
- 日志、指标 counter 的原子更新。

以下串行是禁止的：

- health 等待任意 send/read/receive 完成。
- 无关 recipient/session 互相等待。
- 无关 Source resource 互相等待。
- 持有 adapter/state/stdout 全局 Mutex 跨 provider、数据库、HTTP、文件或 Runtime await。
- 通过一个串行 read loop 执行所有业务 handler。
- 通过无界 spawn“假装并发”。

### 3.2 一个进程不是一个线程

所有 Edge binary 显式使用：

```rust
#[tokio::main(flavor = "multi_thread")]
```

worker 数使用 Tokio 默认 CPU 拓扑或受控配置。一个逻辑 Connector 实例一个进程，保留清晰的账号、故障和凭据边界；进程内使用有界 worker、连接池和 keyed ordering。只有在未来具备 provider partition/lease 协议后才允许同 Connector 实例水平多副本；本方案不以重复启动进程掩盖锁设计问题。

### 3.3 一个 artifact 不等于一个实例

编译边界按依赖族收敛，运行边界按逻辑 Connector 隔离：

```text
cowd-edge-bitable-source artifact
├── process A + profile=feishu-bitable + Feishu account/config
└── process B + profile=lark-bitable   + Lark account/config

cowd-edge-open-platform-message artifact
└── process M + profile=feishu-message # 当前 manifest；未来 Lark profile 复用同 artifact

cowd-edge-sql-source artifact
├── process C + profile=postgres
├── process D + profile=mysql
└── process E + profile=mariadb
```

这样不会把两个地域账号塞进同一状态机，也不会让一个失败实例拖死另一个；同时只编译、签名、分发和审计一份相同实现。profile 只能从 binary 内置 allowlist 选择，不能通过任意动态类名加载代码。凭据仍通过每个逻辑 Connector 的独立 configure 通道传递，绝不写入 manifest、命令行或普通环境变量。

## 4. 终态架构

```text
Gateway SurfaceHost
├── ProcessSupervisor
│   ├── immutable executable + sandbox
│   ├── UDS permission/credential
│   └── stderr drain + lifecycle
├── EdgeH2ClientPool
│   ├── control streams
│   ├── concurrent action/send streams
│   ├── source streaming streams
│   └── one long-lived event stream
└── DurableIngressScheduler
    ├── persist/ack
    ├── bounded global concurrency
    └── per-session ordering

Managed Edge process
├── H2 UDS server
├── LifecycleCoordinator           # narrow serialized owner
├── MessageConnectorRuntime         # 由现有 PlatformRuntime 收敛而来
│   ├── one receive owner
│   ├── bounded concurrent senders
│   ├── per-session ordering
│   └── provider rate/token control
├── SourceConnectorRuntime
│   ├── shared HTTP/SQL pools
│   ├── bounded concurrent reads
│   ├── per-resource incremental lane
│   └── chunked response stream
└── EventReplayBuffer               # bounded, ack driven
```

## 5. Managed Edge UDS HTTP/2 合同

### 5.1 为什么替换 managed JSONL

继续扩展 JSONL 需要自己实现 pending map、优先级 writer、流控、取消、chunk correlation 和公平调度，本质是在重写一个不完整的多路复用协议。HTTP/2 已经提供独立 stream、RST cancellation、连接/stream flow control、header/body 限制和成熟实现，因此 managed Edge 直接使用 H2；JSON 只作为 DTO 编码，不再承担并发协议。

UDS 避免公网监听；每个 Edge generation 使用独立 socket，权限为当前用户可读写。Gateway 启动 Edge 时通过受控 FD/凭据文件传递短期 credential，禁止命令行和普通环境变量泄漏。

Manifest 校验必须强制生命周期与 transport 配对：`managed -> uds-http2`、`one-shot -> stdio-jsonl`、`builtin -> no process entry`。Gateway 为每次进程启动创建带随机 nonce 的 socket 路径，拒绝符号链接和遗留非 socket 文件；连接时同时校验目录权限、Linux peer credential 与短期 credential。Edge 退出或 Gateway shutdown 后原子清理 socket。

Gateway 是 wire 协议 authority：canonical schema 固定放在 Cowd `contracts/edge/v2/`。cowd-edge `contracts/edge/v2/` 只允许由同步脚本生成 vendored mirror，并记录 Cowd source commit 与 SHA-256；两仓 Rust binding 都由 schema 生成。CI 必须执行 clean regeneration + zero diff，禁止人工维护两份 struct。Golden vector/hash 是门禁，不再承担“靠测试猜两份手写定义是否一致”的职责。

### 5.2 端点

```text
POST /_cowd/edge/v2/handshake
POST /_cowd/edge/v2/configure
POST /_cowd/edge/v2/connect
POST /_cowd/edge/v2/disconnect
GET  /_cowd/edge/v2/health

POST /_cowd/edge/v2/message/send
POST /_cowd/edge/v2/actions/{action}
GET  /_cowd/edge/v2/events
POST /_cowd/edge/v2/events/ack

POST /_cowd/edge/v2/source/read
POST /_cowd/edge/v2/source/schema
POST /_cowd/edge/v2/source/incremental
POST /_cowd/edge/v2/source/watermark/commit
```

规则：

- control、小型 action 使用 JSON request/response。
- Source batch 使用流式 body：`start -> row chunks -> end`，每个 chunk 有 sequence、row count 和 rolling checksum；单 chunk 默认不超过 256 KiB。
- Gateway 可以为现有调用方有界收集为 `SourceRecordBatch`，Matrix ingest 路径应直接消费 chunk，避免重新拼成巨型 JSON。
- 请求取消通过 H2 stream reset 传播到 Edge cancellation token；超时后不得遗留 pending 状态或继续无限后台执行。
- event stream 使用带 `event_id/sequence` 的 NDJSON body；Gateway 持久化成功后 ack。断流重连从最后 ack 重放 bounded 未确认事件。
- H2 connection 并不决定业务并发：每类 operation 仍受 manifest/config 中的 semaphore、队列和 provider rate limit 约束。

### 5.3 资源与背压默认值

默认值必须可配置且有安全 clamp：

| 项目 | 默认 | 行为 |
|---|---:|---|
| control concurrency | 1 | 生命周期严格有序 |
| message send concurrency | 16 | provider 可降低 |
| source read concurrency | 8 | 受连接池上限共同约束 |
| per-session mutation concurrency | 1 | 不同 session 并行 |
| per-resource incremental concurrency | 1 | 不同 resource 并行 |
| event replay capacity | 4096 | 满时反压 receiver，不静默丢 critical event |
| request body | 1 MiB | 超限返回 413 |
| source chunk | 256 KiB | 流式，不形成巨型 frame |
| in-flight requests | 256 | 满时返回 retryable 429/`edge_overloaded` |

严格优先级会饿死 bulk stream，因此使用加权公平调度和 H2 stream flow control；health/control 保证预算，但 event/source 也必须持续取得发送窗口。

### 5.4 Managed artifact 与 driver profile 合同

V550 同时终结当前 `entry + lifecycle + transport` 可拼出非法组合的问题。Managed manifest 使用结构化 runtime spec；OneShot 才允许相对 `entry`：

```json
{
  "runtime": {
    "kind": "managed",
    "artifact": "cowd-edge-bitable-source",
    "transport": "uds-http2",
    "driver_profile": "feishu-bitable"
  }
}
```

约束：

- Gateway `ManagedArtifactResolver` 只从受信安装单元的只读 artifact 目录解析文件，不允许 manifest 用 `../` 逃逸或任意 PATH executable。
- artifact 只编译/签名/安装一份；多个 manifest 引用同一 artifact 时分别启动受监管进程。sandbox 将该 artifact 只读映射进各 Connector workspace。
- Gateway 在完成 UDS peer credential + 短期 credential 校验后，通过 bootstrap handshake 发送 `surface_id/driver_profile/capabilities/config_revision`；不是用 `argv[0]`、符号链接名称或未认证环境变量猜 profile。
- binary 必须返回其支持的 profile/capability；profile 不在静态 allowlist、manifest 声明超出 binary 能力或逻辑 `adapter_id` 不匹配时 fail closed。
- Bitable profile 只定义身份与默认 API base：`feishu-bitable -> open.feishu.cn`、`lark-bitable -> open.larksuite.com`；读取、鉴权、Schema、增量、事件、缓存与限流实现只有一份。
- Open Platform Message 使用静态地域 profile 机制；本轮 registry 只发布已有 `feishu-message` 逻辑 manifest，不凭空新增 Lark 产品入口。若后续新增 `lark-message`，它必须引用同一 `cowd-edge-open-platform-message`。
- SQL profile 选择 `Postgres/MySql/MariaDb` dialect；MySQL/MariaDB 共用 wire/pool 实现但保留独立指标标签和配置默认值。
- WebUI/TUI 按逻辑 manifest 展示、启用和配置，不能按 artifact 去重后丢掉 Feishu/Lark 两个入口。

Managed Connector 的 `artifact/driver_profile/adapter_id/default endpoint/capabilities/config_schema/source metadata` 由 cowd-edge `contracts/driver-profiles/*.json` 单点声明。构建时生成 9 份可安装 `surface.json` 投影和 binary profile allowlist；CI 执行 clean regeneration + zero diff。这样 manifest、binary handshake 与 Gateway catalog 不再各维护一张 capability 表。WebUI manifest 不属于 managed driver profile，继续独立维护。

## 6. Message Connector 并发模型

### 6.1 Adapter 合同改造

现有 `platform/runtime.rs::PlatformRuntime` 已经包含 bounded inbound/outbound channel、每 adapter loop、shutdown 和 dispatch ack，不能在其旁边再造一套新 runtime。V551 以它为唯一迁移基底：更名/收敛为 `MessageConnectorRuntime`，补齐 keyed ordering、lane 回收、rate limit、health snapshot 与 H2 endpoint adapter；生产 binary 改为直接构造它，随后删除 `message_sidecar.rs::MessageSidecarState` 与 stdio frame loop。

Feishu/Lark 共用的 Open Platform 能力提取为实例级 `OpenPlatformClient`：持有 profile/base URL、复用 `reqwest::Client`、tenant token cache/expiry/singleflight、SSRF allowlist 与统一响应解码。Message 与 Bitable backend generation 各自构造 client，不共享凭据或 token；删除生产态全局 `FEISHU_API_BASE: OnceLock<String>`，避免测试、未来同进程多实例或重配时的隐式首写胜出。

将：

```rust
async fn connect(&mut self)
async fn disconnect(&mut self)
async fn receive(&mut self)
```

改为：

```rust
async fn connect(&self)
async fn disconnect(&self)
async fn receive(&self)
async fn send(&self, ...)
```

四个生产 adapter 已主要通过 Atomic/RwLock/内部 channel 保存可变状态；需要将剩余 `try_reconnect(&mut self)` 等方法改为内部可变性。运行态保存 `Arc<dyn PlatformAdapter>`，删除外层 adapter Mutex。

约束：

- runtime 只启动一个 receive task，所以 `receive(&self)` 不代表允许并发 receive。
- receive task 独占 provider inbound cursor/channel，但不阻塞 sender。
- provider 一次 poll 返回的全部消息必须先进入 bounded inbound queue，再由唯一 receive owner 逐条交付；禁止 `.into_iter().next()` 丢弃批内剩余项。
- Feishu WebSocket event/write channel 改为 bounded channel；生产源码禁止 `UnboundedSender/Receiver`。满载必须反压或进入明确的 reconnect/replay 状态，不能无限占内存。
- sender 使用 global semaphore + per-session ordering lane。
- 纯只读 health 从 atomic snapshot 读取，不能调用可能等待 provider 的 adapter lock。
- token refresh 使用 singleflight；并发请求只能有一个 refresh，其他请求复用结果。
- provider 429 使用共享 rate limiter/backoff，不能让每个 worker独立重试形成惊群。
- Reqwest Client、SMTP transport、provider HTTP client 在 adapter 构造时建立并复用。

### 6.2 有序与并行

```text
session A: A1 -> A2 -> A3              # 保序
session B: B1 -> B2                    # 保序
A 与 B: 并发                           # 不互锁
health/control: 独立 lane              # 不等发送完成
receive: 独立 owner                    # 不持 sender 锁
```

per-session lane 必须在 idle 后回收，不能像当前 Gateway `session_locks` 一样永久增长。队列满时给 Gateway 明确 overload/retry-after，不能创建无界任务。

## 7. Source Connector 并发模型

### 7.1 长期资源

新增 `SourceBackendGeneration`：

```text
config revision
shared reqwest::Client
cached tenant token + expiry + singleflight
PgPool / MySqlPool
read semaphore
resource lane registry
health snapshot
```

Configure 建立候选 backend、验证连接后原子替换 generation；旧 generation 等在途请求完成后关闭。业务 handler 只 clone `Arc<SourceBackendGeneration>`，不得持 state lock 跨 await。

Source binary 按依赖族而不是品牌拆分：

- `cowd-edge-bitable-source`：`feishu-bitable` 与 `lark-bitable` 两个 profile，共用 Bitable backend generation、HTTP client、token singleflight、Schema/record/event 实现。
- `cowd-edge-sql-source`：`postgres`、`mysql`、`mariadb` 三个 profile，共用 Source runtime；dialect 只决定 pool/SQL 编码分支。

`SourceReadPlan/SourceRecordBatch/SourceWatermark` 等 wire DTO 从 canonical schema 生成。Core 与 Edge 可以拥有不同业务 helper，但不得再维护字段相同的手写 struct 副本。

### 7.2 数据库路径

- Pool 只在 backend generation 建立时创建，read/schema/incremental 共用。
- pool max/min/acquire timeout/idle timeout 受配置限制；默认 read concurrency 不大于 pool max。
- 默认 batch 不执行 `COUNT(*)`。需要总数时显式 `include_total=true`，并设置独立 timeout；`batch_row_count` 与 `source_total_count` 分字段，避免用一个 `row_count` 混合语义。
- schema metadata 按 config revision + table 缓存并有 TTL；DDL/配置变化可失效。
- bounded fetch + chunk encoder 直接向 response stream 写 row chunks，不先构造一个无限 Vec。

### 7.3 Watermark 正确性

- 普通 read/schema 可以并发。
- incremental run 以规范化 `adapter/resource/table` 为 key，只允许同 key 一个 active claim。
- watermark commit 带 `expected_revision`，使用 compare-and-swap；旧 revision 返回 conflict，不覆盖新进度。
- 不同 resource key 并发。
- keyed lane idle 后清理；registry 必须有上限和 metrics。

## 8. Gateway SurfaceHost 与 ingress

### 8.1 ProcessSupervisor

- managed process 不再持有 `ChildStdin`、stdout reader 和 pending oneshot map；改持 H2 connection handle、cancellation tree 和 event stream task。
- 必须持续 drain stderr，逐行加 `surface_id/pid` 后写 Gateway tracing；单行和总速率有限制，防日志洪泛。
- UDS 连接、handshake、health 失败进入现有 failure/circuit 体系。
- Gateway shutdown 先停止接收新请求，取消 H2 streams，调用 disconnect，最后 kill timeout。
- protocol major 不匹配 fail closed，并明确报告 Cowd/Edge version 与 contract hash。

### 8.2 DurableIngressScheduler

critical event 的路径调整为：

```text
Edge event stream
  -> validate limits/id/sequence
  -> persist inbox/trigger record
  -> ack Edge
  -> bounded ready queue
  -> global permit
  -> per-session claim
  -> Runtime admission
  -> completion/retry state
```

改造要求：

- broadcast 只用于已持久化事件的 UI/observability fanout，不再承载唯一业务事实。
- retry/reconcile 使用独立 maintenance task，不得阻塞 event stream intake。
- 删除 per-event 无界 spawn；使用固定 worker set/JoinSet + semaphore。
- 同 session 顺序保留，不同 session 并行。
- session lane/claim 在 idle 后释放，重启时以 durable inbox 恢复，而不是依赖内存锁。
- Lagged observer 只能丢观测投影，不能丢业务 ingress。

### 8.3 Surface 消息持久层

当前全局内存 Map + 多个 append-only JSONL 文件不适合作为高吞吐 durable claim owner。V553 将其迁移为 `SurfaceMessageRepository`：

- SQLite WAL 保存 inbox、outbox、trigger event、delivery event、session claim 和 retry index。
- 唯一 writer owner 在 blocking DB executor 中进行短事务和有界 group commit；async runtime 不执行同步文件 I/O。
- critical ingress ack 只在事务 commit 成功后发送。group commit 使用“最多 128 条或最多 5 ms”默认窗口并可安全配置；不是逐事件 fsync，也不是未落盘先 ack。
- 读路径使用独立只读连接/快照，不持 writer mutex；按 surface/status/session/next_retry 建索引。
- claim 使用事务/CAS，保证同 session 只有一个 active admission，不依赖永久内存锁。
- 首次启动将现有 `surface_*.jsonl` 幂等导入 SQLite，记录 import digest/offset；导入完成前保留原文件只读，验证后不再双写。
- migration 失败保持旧文件不变并拒绝删除；终态不保留 JSONL/SQLite 双生产写路径。

## 9. 所有权矩阵

| 能力 | 唯一 owner | 禁止出现的位置 |
|---|---|---|
| UDS/H2 wire contract/DTO | canonical Edge v2 schema + generated Cowd/Edge bindings | 两仓手写镜像、provider adapter 私自定义协议 |
| Edge process/UDS/circuit/shutdown | Gateway `SurfaceHost::ProcessSupervisor` | Message/Source 业务 handler |
| Managed artifact/profile resolution | Gateway `ManagedArtifactResolver` + binary profile allowlist | `argv[0]` 猜测、任意环境变量/动态类名 |
| 外部 Source catalog/capability projection | SurfaceHost discovered manifest + profile metadata | Core hard编码外部 adapter、WebUI/TUI 自建 provider 表 |
| H2 request admission/limits/cancellation | cowd-edge shared managed server runtime | 每个 binary 复制一套 |
| Open Platform HTTP/auth primitives | 实例级 `OpenPlatformClient` | Bitable/Message 各复制 token 请求、全局 base URL |
| Provider lifecycle/token/rate | Message adapter/runtime | Gateway |
| Message session ordering | MessageConnectorRuntime | stdout writer/global adapter Mutex |
| Source pool/config generation | SourceConnectorRuntime | 每个 read handler |
| Source watermark ordering | resource keyed lane + CAS repository | 全局 Source Mutex |
| Critical ingress durability | Gateway Surface message store | broadcast queue |
| Runtime event matching/scheduling | Runtime | Edge/Gateway transport |
| 性能 metrics | transport/runtime owner 产生，Gateway projection 聚合 | WebUI 推测 |

## 10. 分版本实施计划

### V550：Managed UDS/H2 并发传输基础闭环

**目标：** 真实 fixture managed Edge 可通过 UDS/H2 并发处理 control、action、stream 和 event；Gateway 不再用 managed stdin/stdout/pending map。

**目标 owner：** Cowd Gateway `SurfaceHost::ProcessSupervisor/EdgeH2Client` 与 cowd-edge shared managed H2 server；provider 业务不拥有 wire runtime。

**修改：** 分 Cowd control plane 与 cowd-edge data plane 同版完成，以下任一侧缺失均不得发布。

**Cowd 修改：**

- 新建 Cowd `contracts/edge/v2/` canonical schema/codegen/golden；`crates/surface` 使用 generated binding，新增 sealed runtime spec 与 `UdsHttp2`；`StdioJsonl` 限定 OneShot。
- `crates/gateway/src/surface_host/`：拆出 process supervisor、trusted artifact resolver、H2 client、event stream、stderr drain。
- SurfaceHost descriptor 增加规范化 driver/profile/source metadata 投影；Gateway connector/edge API 从发现结果返回外部 Source catalog，前端不依赖 Core 硬编码列表。
- `ManagedSurfaceProcess` 删除 `stdin/pending/events` carrier，替换为 connection/event/cancellation handles。

**cowd-edge 修改：**

- `edge-contract` 改用带 provenance 的 canonical schema vendored mirror/generated binding；修复当前已存在的 supervisor action 与 `idempotency_key` 漂移。
- 新建 declarative driver profile registry，由其生成 9 份 managed manifest 与 binary allowlist，删除手工重复的 artifact/profile/capability/config schema 常量。
- `edge-adapters` 新增通用 managed H2 server runtime、限制、auth、health metrics。
- 4 个协议族 Message artifact 切换到 managed H2 server，其中 `cowd-edge-feishu-message` 重命名为 `cowd-edge-open-platform-message`；Feishu/Lark Bitable 合并为 `cowd-edge-bitable-source`，Postgres/MySQL/MariaDB 合并为 `cowd-edge-sql-source`。最终 9 份逻辑 manifest 引用 6 个 artifact，并全部改为 sealed `managed + uds-http2 + driver_profile` runtime spec。
- Message/Source 现有业务 handler 在本版接到 H2 endpoint；在 V551/V552 解锁前，各自 data concurrency 可以暂时 clamp 为 1，但 health/control 必须是独立 stream，不能继续由 stdin loop 执行。
- 新增 delayed fixture Edge，验证真实跨进程并发、流式、取消和 overload。

**必须删除：** managed 生命周期的 `invoke_managed` JSONL 写/读、`AsyncMutex<ChildStdin>`、pending oneshot map、未消费 stderr pipe。OneShot JSONL 独立路径允许保留，但它的 stdout/stderr 必须同时有界消费，避免等待子进程时被输出 pipe 卡死。

**允许残留：** Message adapter coarse lock 和 Source per-request pool 属于 V551/V552 owner；它们不能出现在 transport owner 内，也不得阻止 fixture Edge 证明 H2 多路并发。生产 Message/Source data lane 在对应业务版前允许受控 concurrency=1，不能用未审计的并发提前制造 provider 或 watermark 竞态。

**发布/回滚：** protocol major 不提供 managed v1/v2 双生产 fallback。Cowd、6 个 Edge artifact 与 9 份逻辑 manifest 必须作为一个安装单元原子升级；失败回滚同样回退完整安装单元。混合版本只能 fail closed 并保持 Gateway 核心可用，不能静默退回旧 managed JSONL。

**验收：**

```bash
cargo test -p gateway managed_edge_h2 -- --nocapture
cargo test -p gateway managed_edge_cancellation -- --nocapture
cargo test -p surface edge_v2_contract -- --nocapture
cargo test -p edge-contract edge_v2_contract -- --nocapture
cargo test -p edge-adapters managed_server -- --nocapture
cargo test -p edge-adapters driver_profile_matrix --features source-db -- --nocapture
node scripts/generate-edge-contracts.mjs --check
node scripts/generate-driver-profiles.mjs --check
rg "ChildStdin|invoke_managed|pending:.*SurfaceFrame" crates/gateway/src/surface_host
rg '"lifecycle": "managed"' connectors -l | xargs rg '"transport": "stdio-jsonl"'
rg 'cowd-edge-(feishu|lark)-bitable-source|cowd-edge-(postgres|mysql|mariadb)-source' crates/edge-adapters connectors
```

最后三个扫描必须无生产匹配。Canonical contract/generated output hash 在 Cowd/cowd-edge 必须完全相同；9 份 manifest 的 profile matrix 必须逐项 handshake 成功，且 Feishu/Lark 可同时启动为两个独立进程。

**证据：** `docs/evidence/v550-edge-h2-transport.md`；两个仓库分别 commit/tag，对齐 `v0.9.550`。

### V551：Message Connector 无 coarse lock 并发闭环

**目标：** inbound receive、health 和不同 session outbound 并发；同 session 有序；任何 provider await 不持全局 adapter Mutex。

**目标 owner：** cowd-edge 由现有 `PlatformRuntime` 收敛而成的唯一 `MessageConnectorRuntime`，以及四个 provider adapter。

**修改：**

- 重构 `platform/adapter.rs` 与 Email/Feishu/WeCom/WeChat iLink 四个实现为 `&self` interior-state contract。
- 重构并更名现有 `platform/runtime.rs::PlatformRuntime`；Message state 使用 `Arc<dyn PlatformAdapter>`，建立唯一 receiver、bounded sender workers、per-session lanes，H2 endpoint 直接接线。
- 抽取实例级 `OpenPlatformClient`，Feishu Message 与 Bitable Source 复用鉴权/域名/HTTP/token 实现；删除生产全局 API base `OnceLock`，client/token state 归属各 runtime generation。
- `NullAdapter`、四个 AdapterFactory 和相关测试同步新 trait；不保留第二套 `&mut self` compatibility trait，也不保留未接线的备用 runtime。
- Email/WeChat batch poll 结果完整进入 bounded inbound queue；Feishu WS 的生产 unbounded channels 全部迁为 bounded。
- 复用 HTTP/provider clients，token refresh singleflight，共享 provider limiter。
- H2 endpoint 直接提交到 MessageConnectorRuntime，不通过旧 `handle_frame` 大 match。

**必须删除：** `Arc<Mutex<Box<dyn PlatformAdapter>>>`、`adapter.lock().await.send/receive`、`MessageSidecarState`/stdio handler，以及 H2 Message handler 内的全局业务串行 clamp。stdout Mutex 与 inline stdin loop 已在 V550 删除，禁止重新引入。

**允许残留：** Source per-request pool/全局 source state 由 V552 删除；Gateway ingress JSONL store/session lock 由 V553 删除。Message runtime 内不得留下 coarse lock 或无界 channel。

**回滚：** 不改变 v2 wire major；出现 provider 回归时完整回退 V551 cowd-edge binary，Gateway V550 H2 contract保持兼容。回退前必须 drain outbound、停止 receiver 并保留 Gateway durable inbox/outbox，不允许热替换时重复消费。

**验收：**

```bash
cargo test -p edge-adapters message_concurrency -- --nocapture
cargo test -p edge-adapters message_session_order -- --nocapture
cargo test -p edge-adapters message_receive_send_isolation -- --nocapture
cargo test -p edge-adapters message_batch_receive_no_loss -- --nocapture
cargo test -p edge-adapters message_bounded_backpressure -- --nocapture
rg "Arc<Mutex<Box<dyn PlatformAdapter>>>|adapter\.lock\(\)\.await" crates/edge-adapters/src
rg "Arc<Mutex<tokio::io::Stdout>>|run_stdio_platform_message_connector" crates/edge-adapters/src
rg "UnboundedSender|UnboundedReceiver|unbounded_channel" crates/edge-adapters/src/platform/feishu
rg "struct (PlatformRuntime|MessageSidecarState)" crates/edge-adapters/src
rg "FEISHU_API_BASE|async fn tenant_access_token" crates/edge-adapters/src
```

前三个源码扫描必须无生产匹配；runtime 扫描只允许唯一 `MessageConnectorRuntime` 定义；Open Platform 扫描只允许统一 client 内的方法，不得留下全局 base 或第二套 token 函数。测试也应迁移到 bounded fixture，避免继续证明旧模型。Delayed mock：32 个独立 session、每次 50 ms、并发上限 8，总时长必须不超过 350 ms；同 session 32 条必须保持顺序；32 个 200 ms 慢发送期间 health p95 不超过 25 ms。一次 poll 返回 100 条时必须交付 100 条，不能只保留第一条。

**真实环境：** 至少一个真实 push/WS provider 和一个真实 polling provider 完成收发并发、限流、断线重连、重复消息与顺序评测。

**证据：** `docs/evidence/v551-edge-message-concurrency.md`；tag `v0.9.551`。

### V552：Source 连接池、并发与流式闭环

**目标：** Source read/schema 跨 resource 有界并发，同 resource incremental 正确有序，大 batch 不再占用巨型 JSONL frame或重复建池。

**目标 owner：** cowd-edge `SourceConnectorRuntime/SourceBackendGeneration`；Cowd Gateway `SurfaceService::SourceRecordStream` 负责消费，不拥有 provider pool。

**修改：**

- 新增 SourceBackendGeneration、SQL/HTTP pool、token/schema cache、resource lanes 和 CAS watermark。
- 使用 Bitable/SQL 两个共享 artifact 和 5 个静态 driver profile；删除原 5 个品牌/dialect binary entry，所有 profile 使用参数化合同测试。
- Source H2 endpoint 使用 chunked stream。
- Gateway SurfaceService 增加 SourceRecordStream；Matrix ingest 消费 chunk；需要旧聚合 DTO 的只允许在明确上限内 collect。
- Core/Edge Source wire DTO 改由 canonical schema 生成，删除 `source_sidecar.rs` 的手写镜像；H2 确定端点替代无人使用的 action aliases。
- `connector::builtin_source_adapter_manifests()` 只保留真正的 Core builtin；`connector_routes`、`edge_routes`、Matrix Source plan/run 将外部 adapter 查询改线到 SurfaceHost catalog，缺失/disabled/profile mismatch 时明确 fail closed。
- 默认读取删除 COUNT；`SourceRecordBatch.row_count` 统一表示本批 rows 数量，新增可选 `source_total_count` 只在显式请求时返回。同步改线 `connector::SourceRecordBatch`、Gateway `surface_service`、`matrix_routes/source`、connector routes、生成 API 与 GatewayPage 消费方。

**必须删除：** read/schema hot path 内的 `PoolOptions::new().connect()`、每次 Bitable read 的 `Client::new()`/token 请求、全批单响应 JSON。

**允许残留：** Gateway critical ingress 的 broadcast-first、JSONL repository 和 session lock 只允许保留到 V553；Source transport、pool、watermark 正确性在本版必须完全封口。

**回滚：** watermark revision 是向后兼容的附加字段，升级前生成 watermark 快照；V552 binary 回退时必须拒绝覆盖更高 revision。`source_total_count` 为附加字段，旧消费方可忽略；`row_count` 语义迁移必须由合同测试和生成 API 同版封口。

**验收：**

```bash
cargo test -p edge-adapters source_pool_reuse --features source-db -- --nocapture
cargo test -p edge-adapters source_concurrency --features source-db -- --nocapture
cargo test -p edge-adapters source_watermark_cas --features source-db -- --nocapture
cargo test -p gateway source_record_stream -- --nocapture
rg "PoolOptions::new" crates/edge-adapters/src/source_db.rs
rg "reqwest::Client::new" crates/edge-adapters/src/source_sidecar.rs
rg "struct Source(ReadPlan|RecordBatch|Watermark|BatchCursor|FieldSchema|TableSchema)" crates/edge-adapters/src/source_sidecar.rs
rg "source\.incremental_run|source\.plan_incremental|source\.events\.normalize" crates/edge-adapters/src
rg '"(postgres|mysql|mariadb|feishu_bitable|lark_bitable)"' crates/connector/src/source.rs
```

前两个扫描匹配只允许 backend generation 构造器；后三个扫描必须无外部 Source 生产镜像（测试 fixture 可在明确模块内出现）。32 个 50 ms 独立 read、并发上限 8，总时长不超过 400 ms；同 key incremental 最大并发为 1，不同 key 必须观测到并发。连续 100 次 read 的数据库 pool 建立次数必须为 1 个 generation 一次。Feishu/Lark 同实现矩阵必须分别命中正确默认域名，两个实例并发运行时配置、token、watermark 与指标不得串扰。卸载/禁用某个 manifest 后 Gateway catalog 与 WebUI/TUI 能力投影必须随发现状态变化，不能继续显示静态可用。

**真实环境：** 至少 Postgres 或 MySQL/MariaDB 一个真实数据库、一个真实 Bitable provider；记录 cold/warm、1/8/32 concurrency、1 KiB/64 KiB/1 MiB batch 的吞吐与 p95/p99。

**证据：** `docs/evidence/v552-edge-source-throughput.md`；tag `v0.9.552`。

### V553：Durable ingress、有界调度与终态性能审计

**目标：** Edge 到 Runtime 的业务入口在突发、重试和维护期间无静默丢失、无无界任务增长，并完成端到端吞吐评测。

**目标 owner：** Gateway `SurfaceMessageRepository + DurableIngressScheduler`。

**修改：**

- event stream persist-before-ack。
- `SurfaceMessageStore` 迁移为 SQLite WAL repository、索引、事务 claim 与 bounded group commit；提供旧 JSONL 幂等 importer 和受控 reverse exporter。
- live intake、retry、terminal reconciliation 拆为独立任务。
- 有界 global worker + per-session durable claim，删除永久 session lock map。
- Gateway status/health 投影 transport queue、active requests、pool、overload、event lag。

**必须删除：** critical ingress 只经 broadcast、per-event unbounded spawn、永久增长的 `session_locks` map、maintenance 阻塞 receive loop、`Mutex<SurfaceMessageState>`、锁内 `append_record/rewrite_records` 生产路径。

**允许残留：** 无。本版是 Edge 性能终态封口，OneShot JSONL 仅作为已声明的不同生命周期边界存在。

**回滚：** JSONL 导入后不双写。切换 SQLite 前备份旧 JSONL 并完成 shadow count/hash 校验；切换后的回滚必须停止 Gateway，使用本版本提供的受审计 reverse exporter 将 SQLite 全量状态导回临时 JSONL，校验后原子替换，再启动旧版本。禁止直接启动旧 binary 读取过期 JSONL。证据必须覆盖导入、切流、新写入、reverse export、旧版读取五步。

**验收：**

```bash
cargo test -p gateway surface_ingress_burst -- --nocapture
cargo test -p gateway surface_ingress_ordering -- --nocapture
cargo test -p gateway surface_ingress_replay -- --nocapture
cargo test -p gateway surface_ingress_maintenance_isolation -- --nocapture
cargo test -p gateway surface_message_jsonl_migration -- --nocapture
cargo test -p gateway surface_message_jsonl_rollback -- --nocapture
cargo test -p gateway surface_message_group_commit -- --nocapture
rg "session_locks|tokio::spawn\(async move.*handle_surface_message" crates/gateway/src/surface_host/ingress.rs
rg "Mutex<SurfaceMessageState>|append_record\(|rewrite_records\(" crates/gateway/src/surface_host/message_store.rs
```

10,000 events / 100 sessions：业务记录零丢失、每 session 顺序正确、active workers 不超过配置上限、queue/RSS 在上限内；同时执行 retry/reconcile 不得造成 live intake Lagged。

**证据：** `docs/evidence/v553-edge-throughput-terminal-audit.md`；tag `v0.9.553`。

## 11. 删除预检

| 删除目标 | 当前 dependents/状态 | 替代 owner | 调用方改线 | 删除证明 |
|---|---|---|---|---|
| managed `ChildStdin + pending oneshot + stdout lines` | invoke/configure/health/action/send、event reader | EdgeH2ClientPool | SurfaceHost operation 改 H2 request/stream | Gateway source scan 无 managed JSONL carrier |
| 5 个 Source 品牌/dialect binary entry | 5 份薄入口，真实逻辑已共享 | `cowd-edge-bitable-source` + `cowd-edge-sql-source` + driver profile | 5 份逻辑 manifest 改引用 2 个 artifact | Cargo bin/manifest scan + profile matrix |
| Feishu/Bitable 重复 auth/client + 全局 API base | 两套 tenant token 请求、多个短命 client、`OnceLock` base | instance-scoped `OpenPlatformClient` | Message adapter 与 Bitable generation 注入 client | token request count、profile isolation、raw scan |
| 两仓手写 wire DTO 镜像 | Cowd `surface`/`connector` 与 Edge `edge-contract`/`source_sidecar` | canonical v2 schema + generated bindings | Gateway/Edge handler 全改 generated types | generated hash + drift mutation test |
| Core 外部 Source 静态 catalog | 5 个 sidecar adapter 在 Core 与 Edge 双重声明 | SurfaceHost discovered catalog | connector/edge/matrix API 查询 runtime registry | install/disable/uninstall projection test + raw scan |
| `Arc<Mutex<Box<dyn PlatformAdapter>>>` | configure/connect/disconnect/health/send/action/receive | MessageConnectorRuntime + `Arc<dyn PlatformAdapter>` | 四个 provider 实现改 interior state | adapter lock scan无匹配 |
| `MessageSidecarState` 与未接线 `PlatformRuntime` 双模型 | production stdio runtime + test/library runtime | 由后者收敛的唯一 `MessageConnectorRuntime` | 四个 binary/H2 endpoint 与测试只构造唯一 runtime | runtime definition/callsite scan |
| `Arc<Mutex<Stdout>>` | request response + inbound event | H2 response/event streams | endpoint 返回独立 response body | stdout scan无匹配 |
| Source per-request Pool | DB read/schema | SourceBackendGeneration | handler clone active pool | PoolOptions 只在 generation builder |
| Source per-request Client/token | Bitable read/schema | shared client + token singleflight | handler取 generation client/token | hot-path scan无匹配 |
| critical broadcast ingress | dispatcher/message/trigger event | persist-before-ack DurableIngressScheduler | observation broadcast 在持久化后 | burst/replay 零丢失测试 |
| permanent session lock map | message ordering | durable per-session claim/lane | inbox worker调度 | `session_locks` raw scan无匹配 |
| JSONL global message state/write | inbox/outbox/trigger/delivery persistence | SQLite WAL SurfaceMessageRepository | 原 API 调用方改 repository transaction/read model | old carrier scan + migration test |

OneShot JSONL 不属于删除目标：它一次只处理一个请求，没有共享进程吞吐问题；但必须设置最大 frame、stderr drain 和超时。任何 managed manifest 使用它都属于终态失败。

## 12. 完整性矩阵

| 目标 | owner | 真实调用方 | 测试 | 性能/残留证据 |
|---|---|---|---|---|
| H2 managed transport | Gateway + shared Edge server | send/action/health/source/events | fixture child process | 64 concurrent streams、取消、超限 |
| Artifact/profile 收敛 | ManagedArtifactResolver + binary profile registry | 9 logical manifests / 6 artifacts | 5 Source profile matrix + 双实例 | artifact count、签名/hash、配置隔离 |
| Open Platform 共用能力 | instance `OpenPlatformClient` | Feishu Message + Feishu/Lark Bitable | auth/domain/token matrix | client/token 构造数、无全局 base |
| 合同单一规范源 | canonical schema/codegen | Cowd Gateway + Edge handler | 双仓 golden/mutation | generated hash 零漂移 |
| 外部能力自动发现 | SurfaceHost catalog projection | connector/edge/matrix API、WebUI/TUI | install/disable/remove manifest | 无 Core/provider 前端静态表 |
| Message send/receive 隔离 | MessageConnectorRuntime | 4 个 provider | delayed mock + real provider | adapter coarse lock scan |
| Message session ordering | keyed lanes | outbound API/terminal reply | 交叉 session sequence | lane 回收/queue metrics |
| Source pool复用 | SourceBackendGeneration | DB/Bitable handlers | fake + real DB/provider | connect/token count |
| Source跨 resource并发 | Source scheduler | read/schema/incremental | delayed backends | max concurrency trace |
| Watermark正确性 | resource lane + CAS | incremental/commit | race/conflict/retry | revision evidence |
| Source流式传输 | H2 body + SurfaceService | Matrix ingest/API collect | chunk/cancel/checksum | peak RSS/bytes |
| Critical event durability | DurableIngressScheduler | Message/Source events | burst/replay/restart | zero loss/ordering |
| Surface durable store throughput | SQLite WAL repository | inbox/outbox/trigger/delivery APIs | migration/group commit/crash | tx/s、commit latency、WAL/RSS |
| 有界运行 | semaphores/queues | 全部 endpoints/workers | saturation/overload | queue/RSS caps |
| 可观测性 | owner metrics -> Gateway status | operator/WebUI/TUI | status contract | p50/p95/p99/queue/pool |

任何一行只有 DTO、endpoint 或 metrics 名称而没有真实 provider/fixture 调用，均视为未实现。

## 13. 性能评测合同

### 13.1 必须先记录基线

V550 修改前必须在 Release 构建记录当前 JSONL：

- 1/8/32/64 并发。
- 1 KiB、64 KiB、1 MiB payload。
- health、send/action、Source batch、event ingress。
- encode/decode、queue wait、handler、external I/O、write/read 分段耗时。
- CPU、RSS、分配量、open fd、数据库连接数、token请求数。

没有旧基线不能宣称“提升 N 倍”。Debug 结果不能用于吞吐结论。

### 13.2 终态硬指标

- 独立 I/O 型 Message/Source 场景在 concurrency=8 时吞吐至少为旧串行基线 4 倍。
- 64 个 concurrent control/action stream 无 response correlation error、无遗留任务。
- 慢业务流量期间 health p95 达到各版本规定预算，不被业务时长线性拖长。
- 1 MiB+ Source response 使用 chunk stream；small control 不得排在完整大 batch 之后。
- 过载时任务数、队列、RSS 有明确上限；返回可重试 overload，不 OOM、不静默丢 critical event。
- 小型单请求 p95 相比旧 JSONL 不能退化超过 15%；若 H2 固定开销超标，必须优化连接复用，不能用并发吞吐掩盖交互延迟退化。
- 真实 provider/DB 测试保留 trace、时间窗、配置摘要、收据和无秘密日志；mock 不冒充真实环境通过。

## 14. 工作量估算

本次估算按“最终 diff 增加 + 删除”计算，不把现有代码总行数冒充新增工作量。已审计的核心改造面约 63 个现有生产/manifest 文件、32,554 行（Cowd 关键 Surface/Gateway/Source 约 9,223 行，cowd-edge adapter/contract/manifest 约 23,331 行）；真正实施不会全量重写，但会跨两个仓库重构其中的大部分 owner 边界。

| 版本 | 主要工作 | 预计生产文件 | 预计测试/fixture/evidence 文件 | 预计 diff churn（增+删） |
|---|---|---:|---:|---:|
| V550 | canonical contract、sealed runtime manifest、artifact/profile resolver、UDS/H2 server/client、取消/流控、6 artifact/9 profile manifest | 18-25 | 10-15 | 5,000-7,500 行 |
| V551 | 现有 PlatformRuntime 收敛、4 provider trait/internal state、bounded queue、session lane、真实消息评测 | 12-18 | 8-12 | 4,000-6,500 行 |
| V552 | 两个共享 Source artifact、generation/pool/token/cache、stream、CAS watermark、Core/Edge DTO 改线 | 14-20 | 10-15 | 5,500-8,500 行 |
| V553 | SQLite WAL repository、迁移/回退、有界 ingress scheduler、metrics、burst/crash 评测 | 10-16 | 10-15 | 6,000-9,000 行 |
| **合计** | 去重后的终态实现 | **约 55-75 个唯一文件** | **约 35-50 个唯一文件** | **约 20,500-31,500 行** |

预计净新增生产代码约 7,000-11,000 行，测试/benchmark/fixture 约 5,000-8,000 行，旧 carrier/重复入口/手写 DTO 删除约 2,000-4,000 行；其余 churn 是调用方改线与 trait/合同迁移。单人完整工程量约 25-40 个专注工程日，其中真实 provider、真实数据库、故障注入和 Release 基准约占三分之一；凭据或测试环境不可用会影响日历时间，但不能用 mock 代替完成门禁。

Feishu/Lark 合并本身不是“两套 1,600 行逻辑变一套”，因为当前逻辑本来已经共享；它新增的主要工作是 profile/bootstrap/artifact resolver 与隔离测试，约 900-1,600 行 churn，删除的是薄 binary 入口。收益主要是少一个约 102 MiB 的当前 Debug artifact、少一次编译/签名/分发/漏洞审计目标，并杜绝两套入口未来分叉。SQL 三合一同理。上述数字已包含这部分，不另行叠加。

## 15. 实施工作树与发布门禁

1. 当前 cowd-edge V548 工作区未提交，实施前必须由原 owner 提交并形成稳定 commit。
2. Cowd 实施分支必须先快进到实际包含最新 Surface/MFG/Gateway 改动的基线，重新执行本方案代码扫描。
3. Cowd 与 cowd-edge 在 V550 contract 修改中必须同版发布；不能只升级一侧。
4. 独立 App 方案继续冻结。其原版本编号在恢复时必须重排，不能与本方案 V550-V553 tag 重复；本文件不修改其内容。
5. 每版均执行 commit/version/tag/push gate；未完成真实评测与 evidence 不得 tag。

## 16. 反向审计与修正记录

| 初始判断/风险 | 审计结论 | 修正后的硬约束 |
|---|---|---|
| “换成二进制编码即可提速” | 错误；当前 P0 是串行 handler、跨 I/O coarse lock、重复连接 | 先改变并发所有权和资源生命周期，编码仅按数据面选择 |
| “所有地方完全无锁” | 不科学；生命周期、同 session、同 watermark 必须有序 | 明确允许窄 owner/keyed lane，禁止跨外部 await 全局锁 |
| “多启动几个 Edge 进程” | 会重复消费、连接和副作用 | 一个 connector 一个进程，多线程/worker/pool；水平扩展需未来 partition lease |
| “Feishu/Lark 名字不同就必须两个 binary” | Bitable 入口只差逻辑 ID/默认域名，算法与依赖完全相同 | 一个 Bitable artifact + 两个 profile + 两个隔离进程/逻辑实例 |
| “所有 Source 都做成一个万能 binary” | 会把 SQLx/数据库依赖强加给 Bitable，也扩大权限/供应链面 | 按依赖族收敛为 Bitable 与 SQL 两个 artifact，而不是一个巨型 Source artifact |
| “4 个 Message 也一起合成一个 binary” | 协议、依赖、凭据与故障面不同，源码重复只在薄入口 | 保留 4 个协议族 artifact，共用唯一 Message runtime；Feishu/Lark 同属 Open Platform profile |
| 保留 `MessageSidecarState` 再新增并发 runtime | 现有 `PlatformRuntime` 已有可复用 bounded loop；并存必然双逻辑漂移 | 以现有 runtime 为基底改造并接线，旧 sidecar runtime 完整删除 |
| 两仓合同靠人工同步 + golden 测试 | 当前已经真实缺失三个字段/动作，说明人工镜像失效 | canonical schema/codegen 为规范源，golden/hash 只做门禁 |
| 二进制合并后继续保留 Core 外部 Source 静态表 | 安装/禁用状态、profile 与前端能力仍会漂移，自动发现只是表面接线 | Core 只声明 builtin；外部 catalog 唯一来自 SurfaceHost discovered manifests |
| 保留 JSONL 并自建多路复用 | 需要重写流控、取消、chunk、优先级和 correlation | managed 使用成熟 UDS HTTP/2；JSONL 只留 OneShot |
| H2 本身就会自动高吞吐 | 错误；无限 handler 会把压力推给 provider/DB | 每 operation 有 bounded concurrency、rate limit 和 overload |
| 严格优先 control | 可能饿死 event/source | 使用加权公平与 H2 stream flow control |
| Message receive/send 直接并发 | 同 session 可能乱序、provider 可能惊群 | 唯一 receive owner、per-session lane、token singleflight、共享 limiter |
| Source 全部并发 | watermark 会 lost update、重复 ingest | read跨 resource并发；incremental/commit按 key + revision CAS |
| event broadcast 加大容量即可 | 仍会在 Lagged/重启时丢唯一业务事实 | persist-before-ack，broadcast 降为观测投影 |
| persist-before-ack 继续写当前 JSONL store | 全局 Mutex、锁内 open/write 会成为新瓶颈，且无事务 claim | V553 使用 SQLite WAL、bounded group commit、索引/CAS，并提供双向受审计迁移而不双写 |
| 只测 mock | 不能反映 TLS、provider rate、DB handshake/pool | 每个业务版同时要求 deterministic mock 与至少一个真实目标 |
| 仅看平均 latency | 会掩盖 health HOL 和突发过载 | p50/p95/p99 + queue wait + handler + external I/O 分段 |

### 16.1 审计清单

| 维度 | 结果 |
|---|---|
| 当前 owner、调用方、状态 carrier 可追溯 | 通过；第 2 节覆盖 Gateway transport/ingress、Message、Source |
| 每项能力唯一 owner | 通过；第 9 节明确 protocol、process、provider、pool、ordering、durability |
| 同逻辑重复 artifact/runtime/DTO 已穷举 | 通过；第 2.5 节覆盖 Bitable、SQL、Message、runtime、wire DTO 与 action alias，并明确合并/保留边界 |
| 删除前有 replacement 与 rewiring | 通过；第 11 节逐项预检 |
| 非必要全局锁终态删除 | 通过；V550/V551/V552 有 raw scan |
| 必要顺序不因并发丢失 | 通过；session lane、resource lane、CAS 与测试门禁 |
| 背压有界且不静默丢 critical data | 通过；H2 flow control、capacity、overload、persist/ack |
| 真实调用而非空壳 | 通过；fixture child、4 provider contract、真实 provider/DB评测 |
| 跨仓库/工作树影响受控 | 通过；V548 先固化，v2 contract 同版发布，不触碰当前 dirty worktree |
| 方案是否过度扩展 | 通过；不重构 Runtime 调度、WebUI/TUI、独立 App；只投影必要 Edge metrics |

### 审计结论

**方案级通过。** UDS/H2 不是为了追逐协议新颖性，而是删除 managed JSONL 当前必须自行承担却没有正确实现的多路复用、取消、流控和大数据流。真正的业务吞吐由 Message adapter 解锁、Source pool 生命周期、keyed ordering 和 Gateway durable bounded ingress共同完成，任何一项缺失都不得宣称目标达成。

本结论不代表代码已经实施。实施前置条件是 cowd-edge V548 工作区先稳定提交，并在最新 Cowd 实施分支重新核对事实基线。

## 17. 完成定义

只有全部满足才算完成：

1. 所有 managed Message/Source Edge 使用 UDS HTTP/2，managed JSONL production path 删除。
2. health/control 不再等待无关 provider send/receive/source read。
3. Message 外层 adapter coarse Mutex 删除；跨 session 并行、同 session 保序。
4. Source pool/client/token按 generation 复用；跨 resource 并行、同 watermark key CAS 有序。
5. Source 大 batch 流式传输，不形成无限单帧/单 Vec 数据面。
6. child stderr 被持续消费；timeout/cancel 无 pending/task 残留。
7. critical event persist-before-ack；10,000 event burst 零业务丢失、任务/RSS有界。
8. deterministic 并发测试、真实 provider/DB、Release p50/p95/p99/吞吐/资源评测全部有证据。
9. Cowd/cowd-edge contract hash、版本、commit、tag 对齐并分别通过审查。
10. 独立 App、Runtime、WebUI/TUI 现有业务能力没有因 Edge 性能重构受到功能损失。
11. 9 个逻辑 Connector 只交付 6 个受签名 artifact；Feishu/Lark 与三个 SQL profile 共享实现但可并行启动，配置、token、watermark、指标和故障不串扰。
12. Message 只有一个生产 runtime；Cowd/Edge wire 与 Source DTO 只有一个 canonical schema source，无手写镜像或无人调用 action alias 残留。
13. Core 不再硬编码 5 个外部 Source adapter；Gateway、WebUI、TUI 与 Matrix 对外部能力的判断来自同一个 SurfaceHost discovered catalog，并能随安装/启用状态正确变化。
