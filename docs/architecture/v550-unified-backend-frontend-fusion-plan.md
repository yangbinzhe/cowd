# V550 Cowd 双分支与 Edge 前后端统一融合方案

状态：已完成代码事实审计、三方合并模拟和前端影响审计；可作为后续融合实施的验收合同。

## 1. 目标与冻结基线

本轮唯一版本目标为 `0.9.550`。V580 编号作废，不创建 550 之外的新版本来规避融合问题。

冻结提交：

| 仓库/工作区 | 分支 | 提交 | 状态 | 角色 |
|---|---|---|---|---|
| `cowd` | `master` | `a9824047c5f46e31dccd2fc921e3fe7536575ca8` | 干净，领先远程 1 | 单一 Cowd 二进制、Managed Edge UDS/H2 客户端和监管 |
| `cowd-develop` | `develop` | `e028a551f072a264f5c7a80ea04d62751c2c9159` | 干净，领先远程 14 | MFG 合同、权限、Gateway、Runtime、TUI、策略和验收场景 |
| `cowd-edge` | `develop` | `fcd5c5424789b76ce301f7cf9d1ec97abe0bbff4` | 干净，领先远程 11 | Edge UDS/H2 server、6 artifact/9 profile、WebUI 源码与静态产物 |

Cowd 两分支共同基点为 `1e6e66acd65e04e54443acff33f211eb335bd90c`。相对该基点：

- master 有 2 个独有提交，52 个文件变化，约 `+3918/-802`。
- develop 有 14 个独有提交，239 个文件变化，约 `+60176/-6598`。
- 合并模拟只产生 4 个文本冲突，但有 12 个双侧修改文件和若干必须人工裁决的语义冲突。

实施时不得直接在现有两个 Cowd 工作区中解冲突。必须创建第三个临时 integration worktree，以 `a9824047` 为起点合入固定的 `e028a551`；另一工作区继续工作时，不允许重新读取移动中的分支名作为合并输入。

## 2. 终态架构裁决

融合完成后必须同时成立：

1. 发布物只有一个物理 `cowd` 可执行文件。
2. auth broker 与 sandbox launcher 仍由 `cowd __cowd_internal ...` 派生为独立进程，不恢复 helper 二进制。
3. MFG 的 `app-mfg-contract` 是跨 Gateway、auth broker、Runtime、TUI/WebUI 的唯一业务合同源。
4. Managed Edge 使用 sealed `SurfaceRuntimeSpec::Managed`，传输只允许 authenticated UDS/H2；stdio JSONL 只属于 OneShot。
5. Gateway 是 Edge 进程、H2 连接、Surface 投影和前端 API 的唯一宿主；WebUI/TUI 不直接依赖 Edge 传输细节。
6. WebUI 与 TUI 必须消费同一融合后端的 MFG、策略和 Surface 事实，不能靠静态 fixture 假装兼容。
7. `0.9.550` 的最终 tag 只能指向通过后端、TUI、WebUI、Edge 和跨进程验收的融合提交。

## 3. 三方冲突与解决矩阵

| 文件 | Git 结果 | 两侧有效修改 | 终态处理 |
|---|---|---|---|
| `Cargo.toml` | 文本冲突 | master 为 0.9.550；develop 增加 `app-mfg-contract` 与 `schemars` | 依赖和成员取并集，版本保持 `0.9.550` |
| `Cargo.lock` | 文本冲突 | 两侧依赖图均变化 | 禁止手工拼接；所有 manifest 完成后由 Cargo 重建 |
| `crates/auth-broker/src/lib.rs` | 文本冲突 | master 增加内部进程入口；develop 增加 MFG entitlement/Profile/迁移、同 UID 校验 | 合并 import 与全部业务逻辑；保留 `internal_process_entry`，让其调用融合后的 `serve_local` |
| `crates/cli/Cargo.toml` | 文本冲突 | master 依赖 sandbox launcher；develop 依赖 MFG contract、JSON | 依赖取并集；只保留 `cowd` bin |
| `crates/auth-broker/Cargo.toml` | 自动合并 | develop 增加 contract/rustix；master 删除独立 bin | 保留新增依赖，确认无 `[[bin]]` |
| `crates/cli/src/lib.rs` | 自动合并 | master 内部角色分发；develop 增加 Auth surface | 两者保留，内部角色仍是隐藏入口 |
| `crates/cli/src/main.rs` | 自动合并 | master 先注册/分发内部角色；develop 增加 `auth profile` | 保持当前自动合并顺序：internal dispatch 必须早于公开 `auth`/TUI/Gateway 解析 |
| `crates/gateway/Cargo.toml` | 自动合并 | master H2 依赖；develop MFG contract/schemars | 取并集 |
| `crates/gateway/src/api_routes/edge_routes.rs` | 自动合并 | master Managed Edge 投影；develop 主要为格式和测试调整 | 保留 Managed 语义；补齐明确 runtime spec 投影，禁止把 artifact 当 OneShot entry |
| `crates/gateway/src/runtime_host/mod.rs` | 自动合并 | master 内部 broker 子进程；develop MFG reconciler 关闭 | 保留内部 broker，并加入 `shutdown_review_reconciler()`；不得恢复 `auth_broker_binary()` |
| `scripts/scenarios/tui-smoke.sh` | 自动合并 | develop 强制 full TUI 构建 | 构建 `cli --features full`，移除多余 `-p auth-broker` helper 假设 |
| `scripts/v9-terminal-gate.sh` | 自动合并 | develop 增加受控策略 E2E fixture | 保留，继续限定在 E2E 环境变量范围内 |

## 4. 语义冲突与删除预检

### 4.1 外部 auth/sandbox helper 路径

分类：旧运行模型；不能进入终态。

develop 分支存在独立 `cowd-auth-broker`、`cowd-sandbox-launcher`、`COWD_AUTH_BROKER_BIN` 与旁路路径解析。三方合并会因 master 的删除动作自动排除两个 `main.rs` 和 `[[bin]]`，但以下 develop 新增脚本仍会把旧模型带回融合结果：

- `scripts/scenarios/auth-profile-migration.sh`
- `scripts/scenarios/auto-strategy-paired.sh`
- `scripts/scenarios/mfg-surface-acceptance.sh`
- `scripts/scenarios/openapi-generation.sh`
- `scripts/scenarios/with-mfg-surface-lane.sh`
- `scripts/version-gate.sh`

删除前置条件：

| 删除目标 | 当前作用 | 替代路径 | 调用方改线 | 验收扫描 |
|---|---|---|---|---|
| auth/sandbox `[[bin]]` 和 `src/main.rs` | 外部 helper 入口 | `cowd __cowd_internal auth-broker/sandbox-launcher` | Cargo 与安装器只构建/安装 `cowd` | manifests 无 `[[bin]]`，文件不存在 |
| `auth_broker_binary()` | 查找环境变量或旁置 helper | `sandbox_launcher::cowd_internal_process_command()` | RuntimeHost 保留 master 路径 | production source 无函数和环境变量 |
| 六个脚本中的 `AUTH_BROKER_BIN` | 启动独立 broker | 启动 `cowd gateway run`，由 Gateway 派生 broker；需要直测 broker 时调用隐藏内部角色 | 保留原 MFG 断言，不删业务测试 | 脚本无 `COWD_AUTH_BROKER_BIN` |
| `cargo build ... -p auth-broker` | 生成 helper | `cargo build -p cli --features full` | 场景统一使用 `target/debug/cowd` | 场景构建命令扫描 |

安装器中删除遗留 helper 文件的字符串，以及单二进制测试中用于构造遗留文件的字符串属于迁移/测试证据，允许保留；不得把它们误判为生产依赖。

### 4.2 认证测试夹具串扰

分类：活跃测试夹具缺陷。

`crates/gateway/src/api_routes/mod.rs` 使用固定的 `/tmp/cowd-gateway-test-auth`。master 与 develop 测试并行时，一个分支写入带 `core_profile_id` 的新状态，另一个分支读取旧结构，造成 139 个 Gateway 测试同时失败。隔离该目录后 master 的结果为 518 passed / 0 failed / 10 ignored。

终态修改：测试根目录必须包含进程 ID 和随机 nonce；同一测试进程仍通过 `OnceLock` 复用自己的 authority，不同 worktree/进程不得共享状态。验收要求两个工作区的 Gateway auth 测试可以并行运行。

### 4.3 Surface runtime 的 UI 语义

分类：活跃前端载体，不能仅靠兼容字段掩盖。

V550 中 Managed runtime 不再拥有 OneShot `entry`。当前 TUI 仍有 4 处逻辑在以下 3 个文件中使用 `entry.is_some()` 判断外部 Surface：

- `crates/tui/src/app_core/runtime_control_store.rs`
- `crates/tui/src/components/gateway_panel.rs`
- `crates/tui/src/components/surface_panel.rs`

这会把真实 UDS/H2 managed Edge 错标为 builtin。终态方案：

- 为 `SurfaceSummary` 增加单一 `is_managed()`/`is_external()` 判定，以 `lifecycle != builtin` 或结构化 runtime 为权威。
- transport 从后端 `transport` 字段读取；Managed 默认不得回落成 `stdio-jsonl`。
- Gateway Edge 投影增加明确的 `runtime_spec` 或等价的 `artifact/driver_profile/transport` 字段；`entry` 只表示 OneShot entry，不再承载 artifact。
- TUI 的 Surface/Gateway 面板显示 `uds-http2`、managed、artifact/profile 和真实进程状态。
- WebUI 已使用 `lifecycle` 计算 external surface，不需要为 H2 改 transport 客户端；若展示 runtime 详情，则消费新增结构化字段，不读取兼容 `entry`。

## 5. 能力所有权矩阵

| 能力 | 唯一 owner | 融合来源 | 调用方/投影 | 禁止残留 |
|---|---|---|---|---|
| 单一发布二进制与内部角色 | `cli` + `sandbox-launcher` library | master | Gateway RuntimeHost、安装器 | helper bins、环境变量路径 |
| MFG 公共 DTO/Profile/错误/Surface 合同 | `app-mfg-contract` | develop | auth broker、Gateway、Runtime、TUI/WebUI | 各层手写镜像合同 |
| MFG 领域状态与审查 Saga | `app-mfg` | develop | Gateway MFG service | Gateway 重复持久化业务真相 |
| MFG HTTP/SSE、权限裁剪和投影 | Gateway MFG routes/services | develop | WebUI、TUI | 客户端伪造 principal/capability |
| MFG Profile 签发与迁移 | `auth-broker` library | 两侧融合 | 内部 broker 进程、`cowd auth profile` | 外部 broker helper |
| 策略/Team 执行事实 | Runtime/Harness Contract | develop | Gateway projection、WebUI/TUI | UI 从文本推断策略 |
| Managed Edge wire contract | canonical `contracts/edge/v2/schema.json` 生成物 | master + cowd-edge | Cowd `surface`、Edge `edge-contract` | 两仓手写漂移 DTO |
| Edge 进程与 H2 client | Gateway SurfaceHost | master | Surface service/routes | stdin pending map、managed JSONL |
| Edge H2 server 与 driver profile | `cowd-edge` | edge V550 | 6 artifacts / 9 logical manifests | 每 profile 复制二进制 |
| Surface 操作 UI | WebUI/TUI | 两侧 | Gateway API | `entry` 充当生命周期事实 |

## 6. 融合实施阶段

所有阶段都属于 V550，不新增版本号。

### F0：隔离式合并准备

目标：不干扰现有 worktree，建立可重复的融合输入。

动作：

1. 验证三个冻结 SHA 和工作区状态。
2. 从 `a9824047` 创建临时 integration worktree/branch。
3. 以完整 SHA 合入 `e028a551`，使用 `--no-commit` 暴露冲突。
4. 保存 merge-tree 冲突清单和双侧文件清单作为证据。

验收：现有 master/develop worktree 的 HEAD 和文件状态均未变化。

### F1：合同、依赖和单二进制融合

目标：先形成唯一依赖图与运行入口。

动作：

1. 解决 root、CLI、auth broker 三个 manifest 冲突，版本固定为 0.9.550。
2. 融合 auth broker 内部入口与 MFG entitlement/Profile/迁移/同 UID 校验。
3. 保留 CLI 内部角色分发顺序和公开 `auth profile` 命令。
4. 确认 helper `main.rs`、`[[bin]]`、外部路径 resolver 不存在。
5. 重建 `Cargo.lock`。

验收：workspace 全目标编译；单二进制架构测试通过；Cargo metadata 不产生 helper executable target。

### F2：MFG 后端与 Managed Edge 接线闭合

目标：MFG 业务闭环与 H2 Edge 主机同时在线，关闭链路完整。

动作：

1. 保留 develop 的 app-mfg-contract、app-mfg、Gateway MFG routes/services、Runtime/Harness/Matrix 修改。
2. 保留 master 的 SurfaceRuntimeSpec、EdgeH2Client、supervisor、credential/UDS、ACK/replay/取消/限流。
3. RuntimeHost 关闭时依次停止 MFG reconciler、session/runtime bridge 和 managed Edge 进程，不遗留任务。
4. Edge route 同时投影业务状态与结构化 runtime spec。
5. 将六个新增 MFG/策略脚本改为单 `cowd` 二进制运行模型。
6. 将固定认证测试目录改为进程唯一目录。

验收：MFG routes、Profile CAS/confirmation、review reconciler、Edge H2 multiplex/cancel、Gateway shutdown 均有目标测试。

### F3：TUI 与 WebUI 补偿适配

目标：前端真实消费融合合同，不依赖旧字段或旧后端 SHA。

TUI：

1. 用 lifecycle/runtime 判定 managed/builtin，清除 3 个文件中的 4 处 `entry.is_some()` 业务判断。
2. Managed Surface 显示 `uds-http2`、artifact/profile、pid、restart/circuit 状态。
3. 保留 develop 的 MFG operations、live generation/re-auth、策略决策与 backlink。
4. 增加 managed manifest、MFG live 恢复、策略跨 Surface 一致性测试。

WebUI：

1. 以融合后端 SHA 运行 `generate:api`，检查生成文件是否有真实差异；有差异必须提交生成物和调用适配。
2. 保留已经完成的 MFG live authority 恢复、cockpit draft、Team/策略投影能力。
3. Surface 页面继续以 lifecycle 判定 external；新增 runtime detail 时只消费结构化字段。
4. 重新构建并提交 `surfaces/webui/dist`、根 `index.html` 与 hashed assets，禁止手工修改 bundle。
5. WebUI evidence 必须绑定融合后的 clean backend commit 和 `fcd5c54` 之后的最终 frontend commit。

当前事实证据：WebUI 单元测试 148/148 通过；绑定未融合 master 时 API gate 精确缺少 `GET /api/apps/mfg/contract` 和 `GET /api/apps/mfg/cockpit/report-reviews`，这两条路由均存在于 develop。融合后该差距必须归零。

### F4：跨仓合同、真实进程与前端验收

目标：证明三个提交组成一个可运行的 V550，而不是三个分别通过的局部版本。

动作与门禁：

1. Cowd：workspace check、auth broker、CLI 单二进制、Gateway、Surface、TUI、MFG/策略场景。
2. Edge：workspace check/test、9 进程 H2 矩阵、driver profile matrix。
3. 两仓 schema/generated Rust hash 完全相同。
4. 启动融合 `cowd gateway run`，加载 Edge V550 manifests 和真实 artifacts。
5. WebUI 全门禁绑定融合后端，执行 unit、i18n、API matrix、MFG contracts、capability parity、acceptance gate。
6. TUI PTY 场景验证 MFG、策略、Surface managed 状态和运行反馈。
7. 浏览器真实 Gateway 场景验证 MFG live 重认证、报告审查、策略投影和 Surface 页面。

只有全部通过后，才创建最终融合提交、`v0.9.550` tag 并推送。

## 7. 硬验收门禁

### 7.1 架构扫描

生产源和场景脚本禁止：

```text
COWD_AUTH_BROKER_BIN
auth_broker_binary
cowd-auth-broker executable target
cowd-sandbox-launcher executable target
Managed + stdio-jsonl
ManagedSurfaceProcess.stdin
invoke_managed JSONL path
TUI entry.is_some() lifecycle decisions
```

允许项必须单独分类：安装器清理遗留 helper、测试夹具字符串、OneShot stdio JSONL 合同测试。

### 7.2 测试矩阵

| 边界 | 必过测试 |
|---|---|
| 单二进制 | `cargo test -p cli --test single_binary_process_roles` |
| auth/Profile | auth broker 全测 + `auth-profile-migration.sh` |
| MFG 后端 | app-mfg-contract、app-mfg、Gateway MFG tests |
| Gateway 总体 | `cargo test -p gateway --lib`，不得依赖共享固定 `/tmp` |
| Surface/H2 | Surface contract + Gateway H2 multiplex/cancel |
| TUI | TUI tests + PTY smoke + MFG Surface lane |
| 策略 | deterministic 与真实 paired eval、WebUI/TUI 同投影 |
| Edge | 401 Rust tests、9 进程/72 health matrix |
| WebUI | 148 unit 起步，随后完整 npm gate 与真实浏览器 Gateway 场景 |
| 跨仓合同 | schema/generated hash、6 artifact/9 profile 一致性 |

### 7.3 提交与版本门禁

- Cowd 和 cowd-edge 的 canonical version、manifest version、生成证据均为 0.9.550。
- 最终证据记录准确的 backend/frontend/edge commit，不接受移动分支名或脏工作区。
- `git diff --check` 必须通过。
- Cowd 全量 fmt 的历史残留只允许按冻结基线列入 allowlist；融合修改不得新增格式差异，也不得顺手格式化无关代码。
- tag 前再次验证远程 branch/tag 指向；未推送不报告完成。

## 8. 允许残留与禁止残留

允许残留：

- V551 Message provider coarse-lock 优化。
- V552 Source pool/stream/watermark 优化。
- V553 Gateway durable ingress 后续能力。
- 已冻结的独立 App 二进制方案文档，不在 V550 实施。
- Cowd 冻结基线中与本轮无关的历史 rustfmt 差异。

禁止残留：

- 任意 helper 二进制运行路径。
- MFG 新旧 Profile/权限双真相。
- MFG routes 已在后端但 WebUI 生成 API/门禁仍缺失。
- TUI 将 Managed Edge 显示为 builtin 或 stdio JSONL。
- Cowd/Edge contract hash 不一致。
- 只用单元测试替代真实 Gateway、真实进程、PTY 或浏览器验收。

## 9. 审计结论

该方案满足“合同定义 → 所有权迁移 → 调用方重接 → 旧路径删除 → 硬扫描 → 行为证据”的闭环。4 个文本冲突均有唯一解决规则；8 个自动合并文件均已做语义裁决；helper、测试夹具串扰和 TUI runtime 判定三个不会被 Git 标记的风险已进入强制实施项。

方案不要求 WebUI 理解 UDS/H2 细节，但要求它绑定融合后的 MFG/API 合同；TUI 因直接呈现 Surface runtime，必须做字段语义补偿。最终 V550 是 Cowd 单二进制后端、MFG 业务主线、TUI/WebUI 和 cowd-edge Managed H2 的统一版本，而不是两分支简单叠加。
