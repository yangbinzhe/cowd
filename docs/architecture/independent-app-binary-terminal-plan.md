# Cowd 独立 App 二进制终态重构方案

日期：2026-07-17

规划版本：V546-V550

落盘分支：`master`

事实基线：`cowd-develop` 的 `develop@595a78a8` 及其未提交 V545 工作区；`cowd-edge` 的 `develop` 工作区版本 `0.9.545`

## 1. 决策摘要

本方案只解决一个问题：把 MFG 这一类上层业务应用变成独立二进制，使 Gateway 能自动发现、注册、代理和热更新，并让 WebUI、TUI 在应用就绪且当前用户具备权限时自动启用应用能力。

平台范围：V546-V550 以 Linux 为交付平台，使用 UDS、现有 Linux 子进程监管和 sandbox 能力；Windows 不在本轮实现与验收范围内，也不得以未验证的抽象声称已经支持。协议本身保持可移植，后续 Windows 可以将传输替换为命名管道或 loopback authenticated transport，但必须单独规划和评测。

终态采用以下设计：

```text
Cowd Gateway
├── Runtime / Matrix / Memory / Approval / Surface 等核心能力
├── SurfaceHost             # 继续管理 Edge；本方案不重构
└── AppHost                 # 新增，只管理独立业务 App
      └── cowd-app-mfg      # 独立进程、独立版本、独立生命周期
```

明确不采用：

- 不把 App 伪装成 Surface、Connector 或 Edge。
- 不建设统一的 Extension/Plugin 大平台。
- 不通过 Rust 动态库、`libloading` 或不稳定 ABI 加载业务代码。
- 不保留 Gateway 内置 MFG 与独立 MFG 两条生产路径。
- 不要求兼容旧 MFG 内部 Rust API、旧路由实现或旧前端静态注册方式。
- 不改造当前 Edge 二进制与 `SurfaceHost`；只允许复用已经验证过的进程监管经验，禁止通过继承 Surface 协议制造语义耦合。

终态依赖方向只有一条：

```text
Gateway 核心 API  ← HTTP + 受限 App 身份 →  cowd-app-mfg
用户请求          → Gateway 鉴权/授权     →  cowd-app-mfg
```

## 2. 第一性原理

1. 进程边界就是实现边界。Gateway 不链接 App 代码，App 崩溃、更新和回滚不改变 Gateway 进程。
2. Gateway 是唯一外部入口。认证、Principal、能力授权、跨平面策略和审计不能下放给 App。
3. App 自己拥有业务。MFG 领域、仓储、工作流、路由合同、实时投影和专属 UI 都归 MFG App。
4. 自动启用必须由运行事实驱动。只有 `discovered -> ready -> authorized` 的 App 才能出现在 Gateway、WebUI 和 TUI。
5. 前端不能重新产生编译耦合。WebUI 使用 App 提供的动态模块；TUI 使用通用 App ViewModel，不加载 App Rust 代码。
6. 热更新必须先验证新版本，再切流。候选版本失败时，上一代必须保持可用。
7. 独立二进制不等于独立数据孤岛。App 通过受限 Gateway API 消费 Matrix、Memory、Context、Approval、Cross Plane 和 Surface 等核心能力。

## 3. 当前代码事实

### 3.1 Edge 事实，仅作为边界参照

| 事实 | 证据 | 结论 |
|---|---|---|
| 独立 Edge 已存在 | `cowd-edge/crates/edge-adapters/Cargo.toml` 声明 9 个 `cowd-edge-*` binary target | 独立进程模型已经可行 |
| 当前有 6 个 Debug Edge 产物 | 101-110 MiB，带 debug info、未 strip，总计约 619 MiB | 体积不作为 App 架构依据，正式版本需单独记录 release/strip 结果 |
| 3 个数据库 Edge 需要 `source-db` feature | Postgres/MySQL/MariaDB binary target | Edge 的 feature 和分发仍归 Edge 项目，本方案不处理 |
| WebUI 是静态 Surface | `cowd-edge/surfaces/webui/surface.json`，`dist` 约 3.5 MiB | WebUI Shell 可以保留，MFG 页面应移出核心 bundle |
| Gateway 只做 Manifest reload | `crates/gateway/src/surface_host/registry.rs::reload_manifests` | 当前 Edge reload 不是本方案 App 热更新的实现模板 |
| 同 ID Managed Edge 不会替换在运行进程 | `managed_process` 遇到现存 ID 直接返回旧进程 | AppHost 必须使用 generation，而不是复制这一缺陷 |

### 3.2 MFG 后端事实

| 符号/边界 | 文件与调用方 | 分类 | 当前职责 | 终态决策 |
|---|---|---|---|---|
| `GatewayServices::mfg` | `crates/gateway/src/services/mod.rs`、`services/registry.rs`、所有 MFG handlers | 活跃 carrier/service dependency | Gateway 持有 MFG 服务和生命周期 | 删除；由 `AppRegistry` 持有独立进程 generation |
| `mfg_routes::router` | `crates/gateway/src/api_routes/mod.rs::api_router` | 活跃生产路由 | 静态注册约 104 条 MFG 路由 | 删除；统一 `/api/apps/:id/*path` 代理 |
| `MfgService` | `crates/gateway/src/services/mfg_service.rs` 及子模块 | 活跃业务服务 | 桥接 MFG Store、技能、跨平面、实时流和 reconciler | 迁入 `cowd-app-mfg`；Gateway 不保留 facade |
| `MfgReviewReconcilerLifecycle` | `mfg_service.rs`、`mfg_routes.rs::start_review_reconciler` | 活跃后台状态 carrier | 每两秒执行人工审查 Saga 修复 | 迁入 App；只有 active generation 获得后台任务租约 |
| `app-mfg` | `crates/app-mfg` | 活跃领域/仓储 carrier | 领域、SQLite、工作流、事件、Cockpit、技能 | 保留业务实现并迁入独立 App 包；不由 Gateway 依赖 |
| `app-mfg-contract` | Gateway、TUI、MFG | 活跃传输合同兼编译依赖 | 104 路由、DTO、能力、前端合同 | 合同归 App；通过运行时 JSON/OpenAPI 输出，不再作为 Gateway/TUI 依赖 |
| V545 live epoch/cursor/snapshot/delta | develop 未提交 `repository.rs`、`mfg_service/live.rs`、MFG routes | 活跃实时状态 carrier | SSE 恢复、作用域裁剪、cursor 安全 | 原样保留语义并迁入 App；成为热更新 resync 基础 |

代码规模基线：Gateway 有 17 个文件、约 354 处 `app_mfg/MfgService/mfg_routes` 直接引用；MFG 相关后端、合同和领域文件合计约 3.3 万行。领域与仓储不重写，主要迁移路由宿主和 Gateway 服务调用方式。

### 3.3 TUI 事实

| 符号/边界 | 文件与调用方 | 分类 | 终态决策 |
|---|---|---|---|
| `app-mfg-contract` 依赖 | `crates/tui/Cargo.toml` | 活跃编译依赖 | 删除 |
| MFG DTO、action、route 类型 | TUI 12 个文件、约 1111 处引用 | 活跃 UI carrier | 由通用 `AppSnapshot/AppAction/AppLiveEvent` 取代 |
| `mfg_operations_panel.rs` | TUI panel registry、state、runner | 活跃专属面板 | 删除；由通用 `ApplicationPanel` 渲染 App ViewModel |
| `/mfg`、MFG action 静态注册 | action/panel registry | 活跃导航入口 | 改为消费 `/api/apps` 自动注册 |

### 3.4 WebUI 事实

| 符号/边界 | 文件与调用方 | 分类 | 终态决策 |
|---|---|---|---|
| `webuiPagePlugins` 中的 MFG | `cowd-edge/surfaces/webui/src/plugins/registry.ts` | 活跃静态注册 | 删除；改为运行时 App 目录 |
| `MfgPage.vue` 和 MFG components/store/types | WebUI 27 个文件、约 943 处 MFG 引用 | 活跃专属 UI | 移入 MFG App 的 Web bundle |
| `/api/apps/mfg/*` 客户端方法 | `src/api/client.ts` | 活跃静态 API 客户端 | 从核心 WebUI 删除；归 App Web module |
| MFG 页面动态 import | 当前仍编译进 WebUI bundle | 名称上的 plugin，实质静态 | 替换为同源、版本化 ESM module 动态加载 |

## 4. 终态所有权矩阵

| 能力 | 唯一所有者 | 禁止出现的位置 |
|---|---|---|
| App 发现、启动、停止、健康、generation 切换 | Gateway `AppHost` | Runtime、SurfaceHost、MFG 业务代码 |
| 外部 HTTP/SSE 入口、认证、Principal、请求审计 | Gateway | MFG 自行接受公网连接或自行伪造 Principal |
| App manifest 和 MFG 路由/能力合同 | `cowd-app-mfg` | Gateway/TUI/WebUI 手写 MFG 清单 |
| MFG 领域、仓储、迁移、工作流、reconciler | `cowd-app-mfg` | Gateway services/api_routes |
| Matrix/Memory/Context/Approval/Cross Plane/Surface | Cowd 核心服务 | MFG 复制第二套实现 |
| App 身份和可调用核心能力 | Gateway App credential/policy | App 自行扩权 |
| WebUI App 页面 | MFG 内嵌的 Web ESM bundle | 核心 WebUI bundle |
| TUI App 表达 | TUI 通用 `ApplicationPanel` + App ViewModel | TUI 中的 MFG DTO/专属 Rust panel |
| App 活跃状态投影 | Gateway `/api/apps` | WebUI/TUI 各自猜测进程状态 |
| MFG live cursor/epoch | MFG App 持久层 | Gateway 生成或改写业务 cursor |

## 5. 最小架构

### 5.1 一个 App 一个二进制

开发期可以继续位于 Cowd workspace，但必须是独立 Cargo package 和独立产物：

```text
apps/mfg/
├── Cargo.toml                  # cowd-app-mfg binary
├── src/
│   ├── main.rs
│   ├── api/
│   ├── domain/
│   ├── repository/
│   ├── gateway_client.rs
│   └── lifecycle.rs
└── webui/                      # MFG 专属 Web module，构建后嵌入 binary
```

`apps/mfg` 可以作为 Cargo workspace member 共享版本和基础工具链，但不得进入核心 `default-members`；默认 `cargo build -p cowd` 不构建、也不链接 `cowd-app-mfg`。App 单独构建和安装：

```text
<cowd install root>/apps/cowd-app-mfg
<COWD_CONFIG_HOME>/apps/cowd-app-mfg
```

### 5.2 自描述而不是旁路配置复制

Gateway 按 `cowd-app-*` 发现可信目录中的可执行文件。发现阶段禁止直接执行源目录文件，必须先完成以下顺序：

1. 以只读方式打开候选，校验普通文件、所有者、权限、文件大小和符号链接策略。
2. 从同一个已打开文件描述符计算 SHA-256，并复制到临时 generation 文件。
3. `fsync` 后原子改名到不可变 digest 目录，再复核 digest 与可执行权限。
4. 只对不可变副本调用：

```text
cowd-app-mfg --cowd-manifest
```

stdout 只能输出一个 `CowdAppManifestV1` JSON。Manifest 最小字段：

```json
{
  "contract": "cowd.app.v1",
  "id": "mfg",
  "name": "MFG",
  "version": "0.9.547",
  "api_prefix": "/api/apps/mfg",
  "operations": [],
  "required_core_capabilities": [],
  "webui": {
    "route": "/apps/mfg",
    "label": "MFG",
    "module": "/ui/entry.js"
  },
  "tui": {
    "command": "/mfg",
    "label": "MFG",
    "snapshot": "/tui/snapshot",
    "actions": "/tui/actions",
    "live": "/live"
  }
}
```

约束：

- `id` 只允许小写字母、数字和连字符。
- `api_prefix` 必须严格等于 `/api/apps/{id}`。
- App 不能声明 `/api/runtime`、`/api/gateway` 等核心路径。
- 每个 operation 必须声明 method、相对 path、所需 capability、风险、输入/输出 schema 和 streaming 类型。
- operation path 必须是已规范化的绝对 App 内路径；禁止 `..`、重复分隔符、百分号编码绕过、通配符抢占和 `/_cowd` 前缀。
- 同一 App 内 operation 不得冲突；不同 App 天然由 prefix 隔离。
- Web route、TUI command 和 capability 名称必须经过 shadow registry 冲突检查；禁止占用核心 route/command，候选与 active App 冲突时拒绝候选而不是覆盖旧注册。
- Manifest 校验失败时不能启动 App，也不能替换 last-known-good generation。
- Manifest 探测使用空白工作目录、环境变量 allowlist、短超时、stdout/stderr 大小上限；探测模式不得监听端口、写业务数据或执行外部副作用。
- Manifest 声明的 contract major、App ID 和二进制文件名必须一致；不支持的 major 版本直接拒绝。

两个可信根目录采用确定性优先级：用户配置根高于安装根。同一 App ID 的低优先级候选作为 fallback，不与高优先级候选并行激活；同一优先级出现多个 ID 相同但路径不同的候选属于配置错误，保持 last-known-good 并报告冲突。Gateway 配置可以显式禁用 App，禁用优先于自动发现。正式分发候选必须通过 Cowd 发布签名/允许摘要策略；开发目录只有在显式 development 配置下才允许 unsigned binary，不能因为文件位于可扫描目录就自动获得执行信任。

### 5.3 运行协议

Gateway 为每个候选进程创建独立 Unix Domain Socket，启动 App 时注入：

```text
COWD_APP_SOCKET
COWD_APP_ID
COWD_APP_GENERATION
COWD_GATEWAY_INTERNAL_URL
COWD_APP_CREDENTIAL_FD
COWD_APP_REQUEST_VERIFY_KEY
COWD_CONFIG_HOME
```

App 在 socket 上提供普通 HTTP/1.1，支持 JSON、流式 body 和 SSE。系统端点固定为：

```text
GET  /_cowd/manifest
GET  /_cowd/health
POST /_cowd/lifecycle     # standby / activate / deactivate / shutdown
```

进程启动禁止经过 shell，必须对不可变绝对路径执行 `exec`、清空环境后注入 allowlist，并设置独立工作目录、umask、文件描述符上限、内存/CPU/进程数限制和退出回收。Linux 正式模式复用 Cowd 已有 sandbox launcher 的隔离能力，但使用独立 App policy：默认只读 App binary/runtime、读写自己的业务数据目录、访问本 generation UDS 与 Gateway 内部 UDS，禁止任意读取 Cowd 配置/凭据和直接公网访问；额外文件或网络能力必须由 manifest 声明并经管理员策略显式允许。自动启动场景下 sandbox 初始化失败必须 fail closed。

Gateway 仅注册一个通用代理入口：

```text
/api/apps/:app_id/*path
```

代理必须保留 method、query、必要 headers、body backpressure、SSE flush 和客户端取消；禁止将外部 Authorization 原样交给 App。

代理的安全规则必须集中实现并被黑盒测试覆盖：

- 请求必须匹配 active manifest 中登记的 method + 规范化 path；唯一例外是 manifest 明确登记的只读 WebUI asset。未声明 operation/asset 默认拒绝，WebUI asset 只允许 GET/HEAD 和受限内容类型。
- `/_cowd/*` 永远只允许 AppHost 通过 UDS 内部访问，外部代理不能穿透。
- 去除 hop-by-hop、伪造的身份/转发头和 App credential；Gateway 重新生成可信 request context、request ID 和转发元数据。
- 响应执行 header allowlist、`Content-Type` 校验、流量/并发/超时上限；SSE 使用单独的空闲与总时长策略。
- App 进程退出、Gateway 关闭或客户端取消时必须释放连接、socket、临时 generation 引用和子进程。

### 5.4 认证与回调核心能力

外部用户请求先由 Gateway 完成认证。Gateway 为单次请求签发短期 `CowdAppRequestContextV1`：

```text
request_id
app_id
generation
operation_id
principal_id
surface_id
profile_revision
granted_scopes
issued_at
expires_at
signature
```

App 只能信任签名后的上下文，不能从请求 body 接受 actor、scope 或 profile revision。

App 调用核心能力时使用两层证明：

- `COWD_APP_CREDENTIAL_FD` 指向的短期凭据：证明调用方是当前 MFG generation，并限制它可调用的核心 API。
- `CowdAppRequestContextV1`：涉及用户动作时证明有效 Principal；后台任务使用单独的 app-service actor 和 active lease。

MFG 不链接 Gateway Rust service，也不复制 Matrix/Memory/Approval 实现；通过 Gateway 的稳定核心 HTTP API Client 调用核心能力。迁移前必须建立“现有直接 service 调用 -> 稳定核心 API -> capability -> 请求/后台 actor -> 错误语义”的逐项覆盖矩阵；缺失 API 必须先以通用核心能力补齐，禁止为 MFG 在 Gateway 留私有 service bridge，也禁止用一个全能 internal endpoint 绕过授权。

App credential 只通过进程启动时的受控句柄或权限为当前用户可读的短期凭据文件传递；不得出现在命令行、日志、Manifest、`/api/apps` 或崩溃报告中。凭据按 generation 签发，deactivate/退出后撤销。App 回调 Gateway 时只能访问 manifest 声明且管理员策略允许的核心 API；禁止调用自身 `/api/apps/{id}` 代理形成递归。

### 5.5 WebUI 自动启用

核心 WebUI 启动和 App 状态变化时读取：

```text
GET /api/apps
```

只为 `status=ready` 且当前用户满足 App 入口 capability 的记录添加导航。Web module 通过 Gateway 同源代理加载：

```text
/api/apps/mfg/ui/entry.js?v=<content-digest>
```

MFG 二进制内嵌其构建后的 JS/CSS，导出最小 `CowdWebAppModuleV1`：

```text
id
route
component
dispose()
```

核心 WebUI 只提供 Shell、router、认证客户端和设计 token。MFG module 自己持有 MFG 页面、store、types、请求客户端和本地化文案。

安全与一致性要求：

- 只允许从 Gateway 同源 `/api/apps/{id}/ui/` 加载。
- URL 必须含 generation digest，禁止新旧模块共用缓存键。
- `/api/apps` 只返回当前会话的 effective contribution，不暴露 socket、进程参数、凭据或本地路径；接口必须经过 Gateway 会话认证。
- Web module 是与核心 WebUI 同权限执行的受信业务代码，只允许加载通过可信根、签名/所有者策略和 manifest 校验的 App；不可信 App 只能使用通用数据视图，不能贡献可执行前端模块。
- Gateway 返回严格 `Content-Type`、`X-Content-Type-Options: nosniff`、不可变 digest 缓存头和 module 内容摘要；Core WebUI 的 CSP 只开放同源 digest module，不开放任意远程 origin 或 `eval`。
- App module 只能通过 Core 提供的受限 host API 获取路由、会话化请求、设计 token 和通知；不得接收原始令牌。每次卸载必须调用 `dispose()` 并移除订阅、定时器和 route。
- App 下线时移除导航；当前正在显示的页面进入“应用已更新/不可用”状态并导航回 App 目录。
- 新 generation 激活后再发布新 module URL，禁止 UI 先于后端切换。

### 5.6 TUI 自动启用

TUI 不动态加载 Rust 代码。核心只新增一个通用 `ApplicationPanel`，消费：

```text
GET /api/apps
GET /api/apps/{id}/tui/snapshot
GET /api/apps/{id}/tui/actions
GET /api/apps/{id}/live
```

`AppSnapshotV1` 只提供有限、稳定的通用表现：

```text
status
metrics
notices
tables
details
timeline
actions
```

TUI 根据 manifest 自动注册 `/mfg` 和面板。App action 必须继续经过 Gateway capability/risk/confirmation 约束；TUI 只展示 Gateway 返回的有效动作，不根据 App 自述扩大权限。

如果未来需要任意专属终端 UI，应作为 App 自己的独立终端程序，不进入 Cowd TUI 进程；不在本方案范围内。

### 5.7 热发现和热更新

Gateway 监控两个可信 App 根目录，使用 debounce 后按内容 digest 判断变化。直接运行正在被写入的文件存在 TOCTOU 风险，因此发现后必须先复制到 Gateway 管理的不可变 generation 目录：

```text
<COWD_CONFIG_HOME>/runtime/apps/mfg/<sha256>/cowd-app-mfg
```

更新状态机：

```text
discovered
  -> manifest_validated
  -> candidate_spawned(standby)
  -> ready
  -> contract_shadow_registered
  -> activated
  -> traffic_swapped
  -> old_generation_draining
  -> old_generation_stopped
```

硬规则：

- 候选在 `standby` 状态不能执行 reconciler、定时任务或外部副作用。
- manifest、spawn、health、contract 中任一步失败，旧 generation 保持运行。
- Registry 的进程地址、route contract、capability contract、OpenAPI 和 UI contribution 必须在同一次原子切换中更新。
- 新请求立即进入新 generation；旧 HTTP 请求继续排空。
- 旧 SSE 可以在限定时间内继续服务，也可以发送 `resync_required` 后结束；V545 epoch/cursor 保证客户端能重建状态。
- active lease 切换后旧 generation 禁止产生新后台副作用。
- 新 generation 激活后设观察窗；观察窗内连续健康失败可原子切回仍保留的旧 generation。
- Gateway 自身重启时从 generation 元数据和源目录重新验证候选，不盲信遗留 PID/socket；只恢复一个 active generation。
- active 进程意外退出时，先按限速和熔断策略重启同一不可变 generation；超过阈值才回退 last-known-good，禁止无限快速重启。
- 删除 App 文件等同于受控下线：先隐藏 WebUI/TUI 入口、停止接新请求、排空，再停进程和移除投影。

### 5.8 数据所有权

MFG 业务数据固定在：

```text
<COWD_CONFIG_HOME>/apps/mfg/
```

二进制 generation 目录不得保存业务状态。候选进程只允许做只读兼容性与迁移预检，不得在切流前修改生产 schema。为了保证观察窗回滚成立，热更新只能执行旧、新 generation 同时兼容的 expand 型迁移；contract/破坏性迁移必须延迟到观察窗结束、旧 generation 退役且已有可验证的数据备份之后，作为独立受审计步骤执行。若新版本无法与旧 schema 双向兼容，则该版本明确标记为“需停机迁移”，不得伪装成可自动热回滚更新。

本方案不要求兼容旧 Rust API，但必须保留用户业务数据和已承诺的业务语义；“不兼容历史代码”不能被解释为可以无审计删除生产数据。

### 5.9 原子状态、可观测性与运维面

AppHost 对外只发布一个不可变 `ActiveAppSnapshot`，其中同时包含：

```text
app_id + version + digest + generation
process/socket
route/operation contract
effective capability contract
OpenAPI contribution
WebUI contribution
TUI contribution
active lease epoch
```

请求代理、`/api/apps`、OpenAPI、WebUI 和 TUI 都读取同一个 snapshot；禁止分别修改多张 registry 后依靠“最终一致”完成切换。旧 snapshot 由在途请求引用计数保活，排空后统一回收。

健康必须区分：

- `process_alive`：进程和 UDS 是否存活。
- `ready`：路由、数据目录、必要核心能力和依赖是否真的可服务。
- `active`：是否已取得本 generation 的流量与后台任务租约。
- `degraded`：可提供受限读取但部分依赖异常。

所有发现、拒绝、启动、激活、切换、回滚、下线和崩溃重启事件必须携带 `app_id/version/digest/generation/request_id` 写入结构化日志和指标；生命周期变化写入 Gateway 审计事件。`/api/apps` 向有权限的操作者返回无秘密的状态原因、最近失败阶段和 last-known-good 版本，使 WebUI/TUI 能展示“正在启动、更新、降级、回滚、不可用”，而不是静默隐藏运行过程。

## 6. 分版本实施计划

### V546：AppHost 基础闭环

**版本目标：** 建立一个真实可运行、可发现、可鉴权、可代理的独立 App 基础闭环，但不迁移 MFG。

**目标所有者：** Gateway `AppHost`。

**新增/修改：**

- 新增 `crates/app-contract`：Manifest、operation、App 状态、请求上下文、TUI ViewModel DTO。
- 新增 `crates/gateway/src/app_host/`：discovery、generation、process、registry、proxy、credential。
- 新增 `crates/gateway/src/api_routes/app_routes.rs`：`/api/apps` 与通用代理。
- 新增最小测试 App binary，仅用于黑盒测试，不进入正式分发。
- Gateway capability contract、route manifest、OpenAPI 支持从 active App generation 合并动态合同。

**必须完成：**

- 启动时自动发现。
- 可信根、不可变 staging、Manifest 探测限制、重复 ID 优先级和显式禁用语义。
- Manifest 严格校验。
- UDS HTTP 代理支持 JSON、body streaming、SSE、取消。
- operation 精确匹配、路径规范化、`/_cowd` 隔离、header 清洗和资源上限。
- Gateway 签发并校验 App request context。
- Generation-scoped Gateway credential 默认拒绝未声明能力，且不经命令行或目录接口泄漏。
- 单一 `ActiveAppSnapshot` 原子发布 route、capability、OpenAPI、WebUI、TUI 与 active lease。
- 结构化生命周期事件、状态原因、崩溃限速重启和 Gateway shutdown 清理。
- Ready 前不注册任何外部能力。
- AppHost 不引用 `surface::SurfaceManifest/SurfaceFrame/SurfaceHost`。

**允许残留：** 当前静态 MFG 路由和前端仍存在，因为本版本边界仅是通用 AppHost；V547 开始后不得继续保留后端双路径。

**验收：**

```bash
cargo test -p gateway app_host -- --nocapture
cargo test -p gateway app_proxy -- --nocapture
cargo test -p gateway app_auth -- --nocapture
cargo tree -p gateway --edges normal | rg "app-contract"
rg "SurfaceHost|SurfaceFrame|SurfaceManifest" crates/gateway/src/app_host crates/app-contract/src
```

最后一个扫描必须无匹配。使用真实子进程完成 GET、POST、SSE、取消、重复 ID、route/command 冲突、半写文件、符号链接替换、伪造 header、未登记 operation/asset、`/_cowd` 穿透和拒绝越权测试；仅构造 DTO 不算完成。

**Commit/Tag gate：** `v0.9.546`，证据写入 `docs/evidence/v546-independent-app-host.md`。

### V547：MFG 后端完全迁出 Gateway

**版本目标：** `cowd-app-mfg` 成为 MFG 后端唯一生产实现，Gateway 不再编译或持有任何 MFG 业务代码。

**目标所有者：** `apps/mfg`。

**迁移动作：**

- 建立 `apps/mfg` 独立 binary package。
- 迁入 `app-mfg` 领域、仓储、合同、路由、live、review reconciler 和技能编排。
- 先生成 MFG 当前全部 route、operation、capability、direct service call、后台副作用和错误码的机器可读基线；逐项建立迁移覆盖矩阵。
- 将 Gateway MFG handlers 对 `state.services.matrix/context/cross_plane/surface/approval/session/runtime` 的调用改为 App 内的 Gateway Client；任何缺失核心 API 必须先以通用、受能力治理的 Gateway API 补齐。
- MFG App 输出自己的 manifest、route contract 和 OpenAPI；TUI ViewModel 端点可以在本版就绪，但直到 V549 才发布 TUI contribution，WebUI contribution 直到 V548 的 module 可用后才发布，禁止目录提前暴露空入口。
- 将 V545 live cursor key、epoch、snapshot、delta、heartbeat、resync 全部迁入 App 数据目录。
- Gateway 动态合同投影以 App generation 为真源。

**删除动作：**

- 删除 `crates/gateway/src/api_routes/mfg_routes.rs` 及子模块。
- 删除 `crates/gateway/src/services/mfg_service.rs` 及子模块。
- 删除 `crates/gateway/src/services/mfg_skill_executor.rs`，其业务执行迁入 App。
- 删除 `GatewayServices::mfg`、`MfgService::new()` 和 reconciler 启动/关闭路径。
- 删除 Gateway 对 `app-mfg`、`app-mfg-contract` 的 Cargo 依赖。
- 删除 Gateway route/capability/OpenAPI 中所有 MFG 特判。
- `app-mfg`、`app-mfg-contract` 不再作为 Cowd 核心 library package 被 Gateway/TUI/default-members 构建；其代码成为 `cowd-app-mfg` 私有模块。`cowd-app-mfg` 本身可以保留为 workspace member，以便统一版本和工具链。
- 不保留 `/api/apps/mfg` compatibility handler；同一路径只由通用 App proxy 提供。

**不允许残留：** Gateway 内的 MFG DTO、MFG repository、MFG route、MFG service、MFG background task 或静态 MFG capability。

**验收：**

```bash
cargo tree -p gateway --edges normal | rg "app-mfg|app-mfg-contract"
rg "app_mfg|MfgService|mfg_routes|MfgReviewReconciler" crates/gateway/src crates/gateway/Cargo.toml
rg 'route\("/api/apps/mfg' crates/gateway/src
cargo build -p cowd-app-mfg
cargo check -p gateway --all-targets
```

前三个命令必须无匹配。迁移覆盖矩阵必须做到原 route/operation/capability/direct service call 每项均映射为“App owner + Gateway 通用 API 或 App 私有实现 + 测试 ID”，不得出现 `TBD`。对新旧 machine-readable route、capability 和 OpenAPI 基线做集合差异，任何删除或语义变化必须有明确产品决策，不能以“已重构”掩盖能力损失。黑盒测试必须通过 Gateway 访问真实 App 子进程完成：合同读取、Incident、Cockpit、mutation receipt、审批 Saga、live snapshot、SSE delta、后台 reconciler 和核心能力失败语义。

**Commit/Tag gate：** `v0.9.547`，证据写入 `docs/evidence/v547-mfg-backend-extraction.md`。

### V548：WebUI 动态 App 模块闭环

**版本目标：** MFG WebUI 只由 MFG App 提供；核心 WebUI 不再编译 MFG 页面或 API 类型。

**目标所有者：** `apps/mfg/webui`；核心 WebUI 只拥有 App module loader。

**迁移动作：**

- 将 MFG page、components、store、types、tests、i18n 和 client 移到 App Web bundle。
- 核心 WebUI 新增 `/api/apps` store 和动态同源 ESM loader。
- App ready/disabled/updated 事件驱动导航、route 和 module generation 更新。
- App module 使用核心 WebUI 提供的设计 token、session/auth client 和错误边界。
- MFG App binary 内嵌生产 Web bundle 并通过自身 `/ui/*` 提供。

**删除动作：**

- 删除 `webuiPagePlugins` 中的 MFG 静态项。
- 删除核心 WebUI 的 `MfgPage.vue`、MFG components/store/types/client、MFG 专属生成合同和 MFG 专属测试夹具。
- 删除核心 WebUI bundle 中的 MFG chunk。

**不允许残留：** 核心 WebUI production source 中的 `/api/apps/mfg` 字符串、`Mfg*` 类型或静态 MFG route。

**验收：**

```bash
rg "Mfg|MFG|/api/apps/mfg" surfaces/webui/src
npm run build
npm run test -- --run
```

扫描必须无匹配；Core 测试 fixture 使用中性 sample App，MFG 测试和 i18n 一并迁入 App，不能作为永久例外。构建产物 manifest/chunk 名称与内容再做一次 MFG 扫描。浏览器测试必须证明：无 App 时无 MFG 导航；App Ready 后出现；授权不足时不出现；更新后加载新 digest 并释放旧模块；CSP 拒绝非同源/非可信 module；App 下线后移除且当前页面可恢复。

**Commit/Tag gate：** Cowd `v0.9.548` 与 cowd-edge 对齐版本提交；证据写入 `docs/evidence/v548-webui-dynamic-app.md`。

### V549：TUI 通用 App Workbench 闭环

**版本目标：** TUI 根据 `/api/apps` 自动启用 MFG，不再静态编译 MFG 合同和专属面板。

**目标所有者：** TUI `ApplicationPanel`；MFG App 提供 ViewModel。

**迁移动作：**

- 新增 App catalog store、ApplicationPanel、通用 metrics/table/detail/timeline/action 渲染。
- Gateway client 增加通用 App catalog/snapshot/action/live API。
- command palette 和 panel registry 从 App catalog 动态生成 `/mfg`。
- action availability 只能使用 Gateway 返回的 effective capability/risk/confirmation。
- live 更新必须支持 generation/epoch/resync。

**删除动作：**

- 删除 TUI 对 `app-mfg-contract` 的 Cargo 依赖。
- 删除 `mfg_operations_panel.rs`。
- 删除 TUI 中全部 MFG DTO、route、action、protocol、store 和 runner 分支。
- 删除静态 MFG panel/action/command 注册。

**不允许残留：** TUI production source 中任何 `app_mfg_contract`、`Mfg*` Rust 类型或 `/api/apps/mfg` 硬编码。

**验收：**

```bash
cargo tree -p tui --edges normal | rg "app-mfg|app-mfg-contract"
rg "app_mfg_contract|Mfg[A-Z]|/api/apps/mfg" crates/tui/src crates/tui/Cargo.toml
cargo test -p tui application_panel -- --nocapture
cargo test -p tui app_catalog -- --nocapture
```

前两个命令必须无匹配。真实 PTY 交互评测必须证明：无 App 不显示 MFG；发现后自动出现 `/mfg`；授权不足不暴露动作；启动、断开、更新、回滚时状态和 generation 可见；高风险动作仍需确认；操作收据可见；SSE 断流后能按 epoch/cursor 重建而不是静默丢事件。

**Commit/Tag gate：** `v0.9.549`，证据写入 `docs/evidence/v549-tui-app-workbench.md`。

### V550：自动热更新、回滚与终态审计

**版本目标：** MFG 二进制替换时 Gateway、WebUI、TUI 不重启，能力合同、请求流量、UI module、TUI ViewModel 和后台租约作为同一 generation 原子切换。

**目标所有者：** Gateway `AppHost` 负责 generation 与原子发布；`cowd-app-mfg` lifecycle 负责 standby/active 与数据兼容；WebUI/TUI 只消费 active snapshot，不各自决定版本。

**必须完成：**

- 可信目录 watcher、debounce、digest 和不可变 generation staging。
- standby/activate/deactivate 生命周期。
- shadow contract validation 和原子 registry swap。
- HTTP drain、SSE resync/drain、后台 active lease handoff。
- last-known-good 与观察窗自动回滚。
- 数据迁移预检、expand-only 热更新约束、观察窗后的受审计 contract migration；不兼容 schema 的候选必须拒绝热更新。
- App 删除的受控下线。
- Gateway `/api/apps`、capability contract、OpenAPI、WebUI 和 TUI generation 一致性测试。
- Release/strip 二进制体积记录；证明 `cowd` 不再包含 MFG 依赖，记录 `cowd-app-mfg` 独立体积。

**不允许残留：** 手工重启 Gateway 才能更新 MFG、更新失败导致旧 App 下线、UI 与后端 generation 不一致、旧后台任务继续产生副作用。

**真实环境评测：**

1. 启动真实 Cowd Gateway 和真实 `cowd-app-mfg`。
2. 通过真实认证建立 WebUI、TUI 和 SSE 会话。
3. 完成一次真实 MFG 查询和一次受治理 mutation。
4. 如果 MFG skill 路径使用模型，使用真实 Provider 完成一次技能执行并保留请求、响应、工具与收据证据；不把 mock 模型计为此项通过。
5. 在持续请求和 SSE 连接存在时原子替换 App binary。
6. 验证新请求进入新 generation、旧请求完成、SSE resync、WebUI module digest 更新、TUI 状态连续。
7. 放入损坏/越权/启动失败候选，验证旧 generation 不受影响。
8. 激活一个健康后立即异常的候选，验证观察窗回滚。
9. 使用需要 expand migration 的候选验证新旧 generation 均可读写和回滚；使用破坏性迁移候选验证自动热更新被拒绝。
10. 删除 App，验证受控下线和 WebUI/TUI 入口移除。

**Commit/Tag gate：** `v0.9.550`，证据写入 `docs/evidence/v550-independent-app-terminal-audit.md`。

## 7. 删除预检

### 7.1 Gateway MFG 路由与服务

| 项目 | 内容 |
|---|---|
| 删除目标 | `mfg_routes*`、`mfg_service*`、`mfg_skill_executor.rs`、`GatewayServices::mfg` |
| 当前编译依赖 | `api_routes/mod.rs`、`services/mod.rs`、`services/registry.rs`、route/capability/OpenAPI tests |
| 删除会丢失的状态/副作用 | Store 访问、mutation 幂等、review reconciler、live cursor、Cross Plane/Surface 调用 |
| 替代 owner | `cowd-app-mfg` API、repository、lifecycle、Gateway Client |
| 调用方改线 | 外部路径改由 App proxy；核心能力调用改为受限 Gateway HTTP Client |
| 测试迁移 | MFG route/service 单测迁入 App；Gateway 保留黑盒 proxy/auth/contract 测试 |
| 删除证明 | Gateway dependency tree 和 `rg` 无 MFG 业务符号 |

### 7.2 TUI MFG 专属实现

| 项目 | 内容 |
|---|---|
| 删除目标 | `app-mfg-contract` 依赖、MFG DTO/store/runner/panel/action registry |
| 当前编译依赖 | app state、runtime control store、gateway client/runner、command/panel registry |
| 删除会丢失的状态/副作用 | MFG snapshot、live cursor、动作确认、收据、面板状态 |
| 替代 owner | 通用 App catalog/store/panel；MFG App ViewModel |
| 调用方改线 | 所有 MFG branches 改为 app id + generic DTO |
| 测试迁移 | 专属测试改为通用 fixture App + 真实 MFG 黑盒交互测试 |
| 删除证明 | TUI dependency tree 和生产源码无 MFG 类型/路径 |

### 7.3 核心 WebUI MFG bundle

| 项目 | 内容 |
|---|---|
| 删除目标 | 静态 MFG plugin registry、page/components/store/types/client/i18n |
| 当前编译依赖 | router、App shell、Pinia、API client、MFG component tree |
| 删除会丢失的状态/副作用 | 专属驾驶舱交互、live transport、mutation intent、review UI |
| 替代 owner | MFG App 内嵌 Web module |
| 调用方改线 | Core loader 消费 `/api/apps`；模块使用同源 proxy |
| 测试迁移 | MFG UI 测试迁入 App；核心 WebUI 测动态模块生命周期 |
| 删除证明 | Core WebUI source/build manifest 不含 MFG module/chunk |

## 8. 完整性矩阵

| 计划项 | 目标 owner | 真实调用方 | 删除目标 | 核心测试 | 最终证据 |
|---|---|---|---|---|---|
| App 自动发现 | Gateway AppHost | Gateway startup/watcher | 无 | 真实 fixture binary | V546/V550 evidence |
| App 请求代理 | Gateway | WebUI/TUI/API | 静态 MFG router | JSON/SSE/cancel | V546/V547 evidence |
| App 认证 | Gateway | proxy、App callback | body actor 与直传 bearer | 越权/过期/伪造 | V546 evidence |
| MFG 业务后端 | cowd-app-mfg | proxy | Gateway MFG service/routes | 真实 MFG black-box | V547 evidence |
| MFG 后台任务 | cowd-app-mfg active lease | review Saga | Gateway reconciler | generation lease | V547/V550 evidence |
| 动态能力合同 | Gateway 聚合 App manifest | WebUI/TUI/AI/OpenAPI | MFG 特判 | generation consistency | V546/V550 evidence |
| WebUI 自动启用 | Core WebUI loader + App module | 浏览器 | 静态 MFG bundle | browser lifecycle | V548 evidence |
| TUI 自动启用 | TUI ApplicationPanel | 终端用户 | MFG panel/types | PTY 交互 | V549 evidence |
| 热更新与回滚 | Gateway AppHost | 全部 surface | 手工 Gateway restart | 实际 binary replacement | V550 evidence |
| 业务数据连续性 | MFG App repository | 新旧 generation | binary-local state | migration/rollback | V550 evidence |

任何矩阵行只有 DTO、文档或未调用模块而没有真实调用方和测试时，视为未实现。

## 9. 方案硬门禁

### 9.1 依赖门禁

```bash
cargo tree -p gateway --edges normal | rg "app-mfg|app-mfg-contract"
cargo tree -p tui --edges normal | rg "app-mfg|app-mfg-contract"
```

终态必须无匹配。`cowd-app-mfg` 可以依赖 `app-contract` 和通用 HTTP client，但禁止依赖 `gateway`、`tui` 或 `surface_host`。

### 9.2 生产源码门禁

```bash
rg "app_mfg|MfgService|mfg_routes|MfgReviewReconciler" crates/gateway/src
rg "app_mfg_contract|Mfg[A-Z]|/api/apps/mfg" crates/tui/src
rg "Mfg|MFG|/api/apps/mfg" ../cowd-edge/surfaces/webui/src
```

分类规则：

- Gateway/TUI production source：不允许任何匹配。
- Core WebUI production source：不允许业务类型、路由和静态 MFG module；通用 App loader 的测试 fixture 不得使用 MFG 名称，避免假阳性。
- App 自身和迁移证据文档允许出现 MFG。
- 测试不能继续证明旧静态路径；旧 fixture 必须迁移或删除。

### 9.3 行为门禁

- App 未安装：Cowd 核心、WebUI、TUI 正常工作且无空 MFG 入口。
- App 安装并 Ready：Gateway route、capability、OpenAPI、WebUI、TUI 同时出现。
- App 授权不足：入口隐藏或只读，直接请求返回一致的拒绝结果。
- App 候选损坏：旧版本继续服务。
- App 更新成功：Gateway 不重启，前后端 generation 一致。
- App 下线：不丢失已持久化业务数据，不留下后台任务或失效导航。

### 9.4 体积和分发门禁

每个版本记录：

```bash
stat -c '%s %n' target/release/cowd target/release/cowd-app-mfg
file target/release/cowd target/release/cowd-app-mfg
cargo tree -p cowd-app-mfg --edges normal
```

没有测量基线前不设武断 MiB 上限，但必须证明：

- `cowd` 不再链接 MFG。
- `cowd-app-mfg` 不链接完整 Cowd CLI/TUI/Gateway。
- Debug、Release、strip 后大小分别记录，不能用 Debug 文件冒充分发大小。

## 10. 实施前工作树门禁

当前 `cowd-develop` 和 `cowd-edge` 都有其他人在进行的未提交 V545 工作。正式实施前必须：

1. 等待对应工作提交并记录基线 commit。
2. 重新执行代码事实扫描并更新本方案中的数量；数量变化不改变终态所有权。
3. 不跨 worktree 复用未提交文件，不清理、不 reset、不覆盖他人的修改。
4. 每个版本只在明确分配的实施 worktree 中完成，跨仓库修改按同一版本 evidence 关联，但分别提交。

## 11. 审查与审计记录

本方案在首次落盘后按沉浸式重构验收清单进行反向审计，并已纳入以下修正：

| 首稿风险 | 审计判断 | 已修正方案 |
|---|---|---|
| 直接运行正在更新的 App 文件 | 存在 TOCTOU 和半写文件风险 | 从同一文件描述符复制、落盘、复核 digest 后，只执行不可变 generation 副本 |
| `--cowd-manifest` 被误认为无害读取 | 实际仍会执行候选代码并可能泄漏环境或产生副作用 | 空环境 allowlist、sandbox、超时/输出上限、无业务数据权限；正式包要求签名/摘要策略 |
| 把 Edge reload 当作 App 热更新模板 | 同 ID Managed Edge 仍运行旧进程 | AppHost 使用 generation + shadow registry + 原子切换 |
| Manifest 更新后 UI 可能先于后端生效 | 会造成接口与页面版本错配 | route、contract、OpenAPI、Web module、TUI contribution 同一原子 generation 发布 |
| 候选进程启动即执行后台任务 | 新旧 reconciler 会重复产生副作用 | 候选只能 standby；active lease 后才能执行后台任务 |
| Gateway 转发原始 Bearer | App 获得超出需要的用户凭据 | 改为短期签名 request context，禁止透传外部 Authorization |
| App 使用 master Gateway token | 一次泄漏可调用全部核心能力 | 改为按 generation 签发、按 manifest 与管理员策略双重收敛的 credential，退役即撤销 |
| TUI 动态加载 Rust 组件 | ABI、崩溃和卸载风险不可接受 | 只保留通用 ApplicationPanel + 有限 ViewModel |
| WebUI 任意 URL 动态 import | 供应链和 CSP 风险，且 module 与 Shell 同权限执行 | 只允许可信 App 的 Gateway 同源 digest module，限定 CSP/host API，卸载必须 dispose |
| 热更新期间 SQLite 双写 | 迁移和后台任务可能冲突 | binary generation 不存业务数据；业务数据固定；active lease 单写；候选仅做只读预检 |
| 观察窗回滚与破坏性 schema migration 同时存在 | 旧 generation 可能已经无法读取新 schema，自动回滚只是名义能力 | 热更新仅允许 expand 型兼容迁移；contract migration 延迟到观察窗后；不兼容版本显式转停机流程 |
| MFG 直接调用 Gateway services，仅写“改 HTTP” | 可能没有等价核心 API，最后遗留私有 bridge 或功能降级 | V547 前建立 direct-call 覆盖矩阵；缺口先补通用受治理核心 API；新旧合同做机器集合差异 |
| AppHost 多张 registry 逐个刷新 | 进程、路由、OpenAPI 与 UI 会出现短暂跨代 | 只发布单一不可变 `ActiveAppSnapshot`，旧 snapshot 随在途请求排空 |
| App 退出与 Gateway 重启未定义恢复策略 | 可能遗留孤儿进程、失效 socket 或重启风暴 | 明确 shutdown 回收、启动重验证、同代限速重启、熔断后回退 last-known-good |
| Cargo workspace member 被等同于“编译进 Cowd” | 会误删共享工具链能力，且门禁表述不精确 | 允许 App 作为 workspace member，但禁止进入核心 default-members 和 Gateway/TUI dependency tree |
| V547 提前发布尚未落地的 UI contribution | WebUI/TUI 会自动出现空壳入口 | 各 contribution 只在对应 V548/V549 真正可用后发布 |
| 未声明平台范围 | 容易把 Linux UDS/sandbox 实现误报为跨平台完成 | V546-V550 明确 Linux 交付；Windows 单独规划，不计入本轮验收 |
| 删除旧 MFG 路径但测试仍调用旧 handler | 会产生“源码删除、测试证明旧架构”的假完成 | 每一删除项都指定测试迁移和无残留扫描 |
| 一开始抽象统一 Extension Runtime | 超出当前问题并破坏架构清晰度 | 明确 AppHost 与 SurfaceHost 平行，暂不抽公共父层 |

### 11.1 反向验收清单

| 审计维度 | 审计问题 | 修订后结果 |
|---|---|---|
| 代码事实 | 当前 owner、调用方、状态 carrier、Edge 边界是否可追溯 | 通过；第 3 节逐项记录，实施前按最终 V545 commit 重扫 |
| 终态所有权 | 每项能力是否只有一个 owner，是否存在 Gateway/MFG 双实现 | 通过；第 4 节唯一归属，V547 后禁止后端双路径 |
| 调用方改线 | 删除旧 service/route 后是否逐调用方有替代路径 | 通过；V547 增加 direct-call/API/capability/test 覆盖矩阵硬门禁 |
| 状态与副作用 | reconciler、live cursor、SQLite、active lease 是否有明确迁移者 | 通过；App 持有业务状态，单 active generation 产生副作用 |
| 删除预检 | 删除目标、依赖、状态、替代 owner、测试和证明是否齐全 | 通过；第 7 节覆盖 Gateway、TUI、WebUI 三个删除面 |
| 运行闭环 | 是否只有 DTO/Manifest 而没有真实进程、代理、UI/TUI 调用 | 通过；V546-V550 均要求真实 binary/UDS/浏览器/PTY 黑盒证据 |
| 热更新一致性 | route、contract、OpenAPI、UI、TUI、lease 是否可能跨代 | 通过；单一 `ActiveAppSnapshot` 原子切换 |
| 回滚真实性 | 候选失败和 schema 变化时旧代是否真能继续服务 | 通过；不可变 last-known-good + expand-only 热迁移 + 破坏性迁移拒绝门禁 |
| 安全边界 | 自动执行、凭据、代理路径、动态 JS、sandbox 是否 fail closed | 通过；第 5 节给出明确拒绝规则和 V546 黑盒攻击用例 |
| 跨 worktree 影响 | 是否会覆盖 develop/cowd-edge 正在进行的工作 | 通过；第 10 节要求先固化各自基线、分仓提交，Edge Host 不改造 |
| 版本封口 | 每一版是否有删除扫描、真实测试、证据、commit/tag gate | 通过；V546-V550 分别定义，不允许跨版借证据 |

### 审计结论

- **方案级目标完整性：通过。** 后端、Gateway 代理、能力合同、WebUI、TUI、实时流、数据和热更新均有唯一 owner、迁移路径、删除目标和真实验收。
- **依赖方向：通过。** Gateway 不依赖 MFG；MFG 只依赖轻量合同和 Gateway HTTP API；TUI/WebUI 不依赖 MFG 编译类型。
- **半成品风险：已封堵。** V547 后禁止后端双路径，V548/V549 分别删除前端静态路径，V550 不允许手工重启式伪热更新。
- **Edge 影响：受控。** 本方案不改变 Edge contract、Edge binary 或 SurfaceHost；避免与当前 cowd-edge Connector 工作互相放大。
- **可实施性：通过但属于高影响重构。** 主要成本集中在 Gateway MFG handler 改线、TUI 类型解耦和 WebUI 页面迁移；MFG 领域/仓储主体可保留。
- **本结论仅代表方案经代码事实与反向清单审计后可执行，不代表功能已经实现。** 实施开始前仍必须等待 V545 两个工作区形成稳定 commit，重新生成事实基线；若 API 覆盖矩阵出现无法由通用核心 API 承接的调用，应回到 V547 设计门，不得带着私有 bridge 继续推进。

## 12. 完成定义

只有以下条件全部满足，独立 App 重构才算终态完成：

1. `cowd-app-mfg` 是独立可执行文件，默认 Cowd 构建不链接它。
2. Gateway 启动和运行期间能自动发现、验证、启动、代理、停止和热更新 App。
3. Gateway 源码和依赖树中不存在 MFG 业务实现或静态路由。
4. MFG 所有核心业务、V545 live、reconciler 和仓储在 App 中真实运行。
5. WebUI 不编译 MFG 页面，通过 Ready App 的同源 ESM module 自动启用。
6. TUI 不依赖 MFG Rust 类型，通过通用 App ViewModel 自动启用。
7. capability contract、OpenAPI、WebUI、TUI 与进程地址来自同一 active generation。
8. 更新失败保留旧 generation；更新成功不重启 Gateway；观察窗异常可以回滚。
9. 权限、Principal、审批、跨平面和审计能力没有因进程拆分而降级。
10. 真实 Gateway、真实 MFG binary、真实 WebUI/TUI、真实 SSE 和适用时真实模型评测全部通过并有证据文件。
11. 每个版本完成代码审查、依赖扫描、残留扫描、针对性测试、真实评测、证据记录、提交与 tag gate。
