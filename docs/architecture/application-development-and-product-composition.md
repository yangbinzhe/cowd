# Cowd 通用应用（App）开发与产品组合规范

状态：架构规范；当前 MFG 是首个参考实现。本文定义以后所有 Cowd App 的共同边界，不把 MFG 当作特殊平台能力。

关联当前运行时实现：[已编译 App 的统一启停与构建模型](app-activation-and-build.md)。

## 1. 目标与边界

Cowd 是 AI Harness 内核，可以容纳多个业务 App。App 是建立在 Runtime、Memory、Matrix、Tool、Skill、Approval 与 Gateway 之上的业务产品能力，例如制造运营、工程交付、产品落地或其他领域工作台。

```text
Cowd Core
├── Runtime / Provider / Tool / Skill / Approval / Memory / Matrix
├── Gateway：唯一外部入口、身份、授权、审计和能力投影
├── TUI / WebUI：通用控制面与 App 呈现宿主
└── App product contributions
    ├── MFG                 # 第一个参考 App，不是唯一 App
    ├── Engineering App     # 后续业务 App
    └── ...
```

App 不是 Edge/Surface（渠道或界面适配进程）、Connector（外部资源适配）、Skill/Tool（AI 调用的原子能力），更不是可以由配置在运行时下载执行的未知插件。App 可使用这些能力，但拥有自己的领域模型、工作流和业务状态。

App 使用独立 Git 仓库开发，不等于已经采用独立运行二进制或热加载。前者是源码与所有权边界；后者是独立的部署/进程模型决策。本规范先统一所有 App 都需要的来源、产品组成与能力治理，不把两者混为一谈。

## 2. 两个独立控制面

必须严格区分“代码是否进入发行物”和“该代码是否在本次启动中提供服务”。

```text
来源与产品组成（构建期）                    运行配置（启动期）
Git + immutable revision / 本地开发路径      apps.<id>.enabled
                │                                      │
                ▼                                      ▼
Cargo / WebUI build graph                       Gateway AppRegistry
                │                                      │
                ▼                                      ├─ HTTP routes / skills / Auth catalog
Cowd 二进制 + WebUI static assets                ├─ OpenAPI / AI tools / capability contract
                                                       └─ TUI / WebUI app projection
```

| 问题 | 构建期控制 | 启动期控制 |
|---|---|---|
| App 代码是否存在于 `cowd` 二进制 | Cargo feature 与依赖图 | 否 |
| App 前端贡献是否存在于 WebUI 静态资源 | WebUI build profile | 否 |
| API、技能、授权、OpenAPI/AI tools 是否发布 | 否 | Gateway `AppRegistry` |
| TUI/WebUI 是否显示并请求 App | 静态代码可用性 | Gateway App catalog / manifest |
| 改动何时生效 | 重新构建并部署 | 修改配置后重启 Gateway |

当前 MFG 已完成构建期与启动期的统一控制：`apps/catalog.toml` + `apps/mfg/source.lock.toml` 生成 `cowd-product-apps`，`app-mfg` feature 决定代码是否静态链接；`apps.mfg.enabled` 决定已链接代码是否注册到 Gateway、TUI 与 WebUI。`--no-default-features` 的 Gateway/CLI 不含 MFG，`full` 显式选择 `tui-surface + app-mfg`。

授权目录也是这个统一投影的一部分：Gateway 从当前 App descriptor 构造通用 `AuthorizationCatalog`，Auth Broker 只按该目录保存 `app_profiles` 和重算后的能力快照。产品组成升级若改变该目录，必须提供凭据验证后的单次状态迁移、能力回退规则和 epoch/revision 失效语义；不得把历史 App enum、历史能力或更高权限作为长期兼容执行路径。V564 对 MFG 时代的 v2 状态完成了这一迁移，后续新 App 必须沿用这一通用目录边界。

| 项目 | 当前状态 | 后续实现边界 |
|---|---|---|
| AppRegistry、统一路由/技能/授权/界面投影 | 已实现，MFG 作为当前贡献 | 新 App 必须复用这一投影，不能新增旁路 |
| `apps.<id>.enabled` 配置解析 | 已是通用配置模型 | 每个已编译 App 接入同一策略 |
| `apps/catalog.toml`、来源锁与校验 | 已实现 | catalog 只接受显式条目、full SHA 与一致的 bundle/WebUI identity |
| 新 App 本地 path override | 未实现通用工具 | `apps.local.toml` + xtask 生成，不进入发行提交 |
| 新 App 静态产品接入 | 已实现 | `xtask apps sync/verify` 生成 bundle dependency、feature 与前端元数据 |
| App relational storage migration hook | 已实现 | App 自己提供 canonical copy/digest；Cowd 只编排通用 hook 和 evidence |
| Core / 单 App / full 可选二进制 | 已实现 | optional Cargo dependency + feature 传播 + 定向构建门 |

## 3. 唯一所有权

| 能力 | 所有者 | 禁止事项 |
|---|---|---|
| 模型调用、会话、上下文、审批、核心工具与事实能力 | Cowd Core | App 复制第二套 Runtime/Memory/Matrix/Approval |
| HTTP/SSE 外部入口、身份、能力授权、审计 | Gateway | App 自行接受公网请求或伪造用户身份 |
| 领域模型、业务仓储、领域工作流和专属后台任务 | 对应 App | Gateway 承载多个 App 的业务 service 分支 |
| App API、领域 Skill、领域 Action | 对应 App，经 Gateway 注册和治理后发布 | 绕过 Gateway 直接暴露入口 |
| WebUI 页面、TUI 表达和国际化内容 | App contribution；宿主提供通用壳和设计系统 | Core 复制业务状态或私自猜测 App 状态 |
| App 启用事实 | Gateway `AppRegistry` | TUI/WebUI 各自维护启停开关 |

每个 App 必须有稳定、小写的 `id`（推荐 `[a-z][a-z0-9-]*`），并以该 ID 作为配置键、路由前缀、能力命名空间和用户可见 catalog 身份。发布过的 ID 不得复用给另一种业务语义。

## 4. 源码来源、定位与版本锁定

### 开发模式：显式本地路径

开发 App 时，源码位置必须由开发者显式声明为本地 `path`；Cowd 不扫描磁盘，也不猜测仓库位置。这样修改 App 后可以直接由 Cargo 使用本地源码编译。

本地 override 只能存在于未提交的开发者配置中，不能作为团队发行来源。目标形态为：

```toml
# apps.local.toml（本地、gitignore）
[apps.demo]
path = "../cowd-app-demo"
```

当前通用 `apps.local.toml` 与自动生成工具尚未实现；在实现前，本地路径必须明确写入开发分支的 Cargo `path` dependency，且不得混入正式发布提交。

### 发行模式：Git URL + immutable revision

正式产品组成必须同时把产品身份写入 catalog、把来源写入提交的锁文件：

```toml
# apps/catalog.toml
[[apps]]
id = "demo"
source_lock = "demo/source.lock.toml"
feature = "app-demo"
rust_bundle_package = "cowd-app-demo-bundle"
rust_bundle_crate = "cowd_app_demo_bundle"
webui_package = "@cowd/app-demo-webui"

# apps/demo/source.lock.toml
schema = 1
app_id = "demo"
sdk_api = 1
[rust]
git = "https://example.invalid/organization/cowd-app-demo.git"
rev = "0123456789abcdef0123456789abcdef01234567"
packages = ["cowd-app-demo-contract", "cowd-app-demo-bundle"]
bundle_package = "cowd-app-demo-bundle"
bundle_crate = "cowd_app_demo_bundle"
[webui]
git = "https://example.invalid/organization/cowd-app-demo.git"
rev = "0123456789abcdef0123456789abcdef01234567"
package = "@cowd/app-demo-webui"
```

`rev` 必须是完整 Git commit。分支和 tag 都可能移动；commit 指向确定源码树，让同一 Cowd 提交在不同机器、不同时间得到同一 App 代码，也让故障、审计和回滚精确定位。commit 锁定不等于自动安全：新 revision 仍须做代码、许可证和依赖审查；它解决的是可复现性与可追溯性。

Cargo 在**构建期**依据审核后的 `git + rev` 拉取或复用缓存中的精确源码；Cowd 在**运行期**绝不拉取、编译或执行新的 App 源码。

`Cargo.lock` 固定 Rust 解析结果；catalog 与 source lock 则关联 App ID、Git 来源、bundle package/lib、前端贡献和 feature。`cargo run -p xtask -- apps verify --locked` 只校验，`sync` 才生成 `crates/product-apps/Cargo.toml`、`src/generated.rs` 与 `surfaces/webui/apps.generated.ts`。两者均不克隆、不安装、不在运行期加载代码。

## 5. 新 App 接入规约

每个新 App 必须完成以下项目，不能只新增 YAML 开关：

1. **定义产品身份**：不可复用的 App ID、显示名、领域所有者和能力边界。
2. **锁定来源**：开发时显式 `path`；发行时提交 `git + full commit + package` 锁文件。
3. **创建 App bundle**：在 App 自己的仓库创建发布 package `cowd-app-<id>-bundle`，只负责注册 descriptor、HTTP、Skill、Action 与 UI contribution；不得依赖 Gateway、Runtime 私有模块或 auth-broker。
4. **声明后端产品 feature**：Gateway dependency 必须为 optional；CLI 的 `app-<id>` feature 同时选择 Gateway/TUI contribution。`full` 只包含明确列出的、已审核 App。
5. **注册统一投影**：App 必须经 `AppRegistry` 进入 `/api/apps`、授权目录、路由清单、capability contract、OpenAPI 和 AI tools；禁止硬编码旁路。
6. **接入界面**：WebUI 构建时纳入该 App contribution，运行时依据 Gateway manifest 注册；TUI 同样依据 `/api/apps` 过滤已编译 contribution。两端不得自行决定 App 启停。
7. **声明配置**：默认配置提供 `apps.<id>.enabled`；禁用必须移除全部 App 对外合同，不得只隐藏菜单。
8. **验收**：覆盖启用、禁用、无授权、路由拒绝、Skill/AI tool 不发布、TUI/WebUI 不显示、重新启用恢复；证明领域依赖没有反向进入 Runtime。
9. **存储迁移**：声明 relational storage requirement 的 App 必须通过
   `StaticAppProduct::with_storage_migrator` 注册自身迁移 hook。App 只消费宿主发放的
   source/target lease，在自己的仓库内完成 canonical export/import/re-read/digest；不得把
   App schema、DTO 或 SQL 放进 Cowd。PostgreSQL lease 由宿主限定到独立 schema；启用 App
   缺 hook、目标非空不一致或 source/target digest 不同都必须阻止全局 cutover。

Cowd 的稳定目录为：

```text
apps/
  catalog.toml                   # 所有受支持 App 的显式产品元数据
  <id>/source.lock.toml           # 已锁定发行来源
crates/
  app-sdk/                        # App descriptor 与受限宿主合同
  app-host/                       # AppRegistry 与统一投影
  product-apps/                   # generated catalogue 的唯一 Cowd 产品组合入口
```

App 的业务仓库可以独立存在；是否作为 Cargo workspace member 是工程效率选择，不等于自动被编进 Cowd。真正的编入条件是 Cargo feature 与依赖图。

## 6. 构建产品矩阵

```text
cli
├── tui-surface
├── app-mfg  ──> gateway/app-mfg + tui/app-mfg
├── app-dev  ──> gateway/app-dev + tui/app-dev
└── full     ──> tui-surface + 明确列出的已审核 App feature
```

当前支持三种产品：

```bash
# Core：不含业务 App 后端
cargo build -p cli --no-default-features

# 定制产品：只包含指定 App
cargo build -p cli --no-default-features --features tui-surface,app-mfg

# 完整产品：只包含 full 明确列出的 App
cargo build -p cli --features full
```

`gateway`、`tui` 与 `cli` 以同名 `app-<id>` feature 传播；`full` 不会自动发现任何 App，而只组合 catalog 已生成、明确审核的 feature。App 仍在单一 Cowd 进程中静态链接，配置只可禁用已编入能力。

## 7. 安全与演进红线

- 不允许通过 `apps.<id>.source`、环境变量或远程 URL 在生产运行期装载源码。
- 不允许某 App 因目录发现而自动进入 release；产品组成必须显式、可审查。
- 不允许 App 直接获得 Gateway 主 token、用户 bearer 或未受限的 Core service 引用。
- 不允许 TUI/WebUI 因静态包中存在 App 代码而绕过 Gateway 实际启用状态。
- 不允许“禁用”仅删除导航但保留 API、Skill、AI tool 或授权能力。
- 不允许业务实现扩散到 Gateway、Runtime、TUI、WebUI 多处；App bundle 以外的层只保留通用宿主与合同。
- 不允许 App 直接解析 PostgreSQL URL、选择主数据库 schema，或绕过宿主 lease 创建第二个连接池。

这些约束换来可复现构建、可审计来源、可回滚版本、按产品裁剪体积和统一能力治理。新增 App 的机械成本应通过 catalog、生成工具和本地 override 降低，而不是牺牲运行期供应链边界。
