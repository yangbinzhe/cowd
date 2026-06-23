# Cowd

Cowd 是 Rust 原生的 AI Harness 核心仓库。当前核心版本：`0.9.358`。

本仓库的目标不是实现一个单一聊天 CLI，而是建设一个可长期演进的 AI Harness 内核：统一承载模型调用、会话、上下文、记忆、事实、工具、技能、审批、任务推进、运行时治理和 surface 投影。CLI、TUI、WebUI、外部渠道都只是这个内核能力的不同入口和呈现方式。

非 TUI surface 已从 core 仓库迁出，统一进入独立仓库 `cowd-surface`。core 仓库只保留协议、Gateway 装载能力、AI Harness 核心能力，以及可选的 TUI surface。

## 1. 总体设计

### 1.1 核心定位

Cowd core 负责 AI Harness 的稳定内核，不负责把所有 UI 和平台 SDK 打进一个巨大二进制。

```text
用户入口
  CLI       极薄命令入口，负责配置、诊断、Gateway 启动等轻控制
  TUI       core 仓内唯一 UI surface，仅 full/release 联调时构建
  WebUI     cowd-surface 中的浏览器 surface
  Channel   cowd-surface 中的外部渠道 sidecar

Gateway
  HTTP/SSE API
  RuntimeHost
  SurfaceHost
  Surface static/callback/health/events

AI Harness core
  Session
  Runtime turn
  Context
  Approval
  Tools
  Skills
  MCP
  Provider
  Memory
  Matrix
  Task/Eval/Growth
  Telemetry

Fact/application layer
  fact-kernel
  memory
  matrix
  app-mfg
```

### 1.2 第一原则

- Runtime 不持有 channel，也不链接任何平台 SDK。
- Gateway 是唯一后端服务入口，负责 surface 发现、静态资源转发、callback 转发、health、events 和 JSONL sidecar 调度。
- TUI 和 WebUI 都只通过 Gateway HTTP/SSE API 使用核心能力。
- CLI 不做交互 UI，不承载业务执行器，只负责轻量命令、配置、诊断和 Gateway 启动。
- 默认开发/debug 构建不带 TUI，TUI 与 Gateway 分开开发。
- 只有 TUI 联调、完整产品验证和正式 release 才构建 `--features full`。
- 非 TUI surface 不在 core workspace 编译，全部从 `cowd-surface` 按需独立构建和交付。
- Memory 处理非结构化记忆和经验关联，Matrix 处理结构化事实、实体、关系和证据。
- MFG 是应用层，不是 AI Harness 内核。

## 2. 仓库边界

### 2.1 core 仓库

```text
crates/cli        极薄 CLI 入口，默认 debug 不编译 TUI
crates/gateway    HTTP/SSE 服务入口，负责 RuntimeHost 与 SurfaceHost
crates/runtime    AI Harness 运行时核心，不依赖 channel/surface SDK
crates/surface    Surface JSONL 协议与 manifest 合同
crates/tui        core 仓内唯一 UI surface，full 构建才进入 cowd
```

### 2.2 surface 仓库

```text
cowd-surface
  surfaces/webui          WebUI 静态 surface
  surfaces/feishu         飞书 sidecar surface
  surfaces/email          邮件 sidecar surface
  surfaces/wecom          企微 sidecar surface
  surfaces/wechat-ilink   微信 iLink sidecar surface
  crates/surface          Surface 协议镜像
  crates/surface-adapters 平台适配实现和 sidecar 二进制
```

WebUI、飞书、邮件、企微、微信 iLink 不再进入 core workspace。它们通过 `surface.json` 和 JSONL sidecar 协议被 Gateway 发现和调用。

## 3. Workspace 能力分层

### 3.1 Entry 层

| crate | 职责 |
|---|---|
| `crates/cli` | 极薄命令入口。默认构建不带 TUI，不依赖 runtime/memory。 |
| `crates/gateway` | 后台服务入口。承载 HTTP/SSE API、RuntimeHost、SurfaceHost 和服务编排。 |
| `crates/tui` | 终端 surface。只在 `--features full` 或显式选择时构建。 |
| `crates/surface` | Surface manifest、JSONL frame、静态资源、callback、health 合同。 |

### 3.2 AI Harness 层

| crate | 职责 |
|---|---|
| `crates/ai-kernel` | AI Harness 语义入口，承载策略、目标、工作图等核心语义。 |
| `crates/ai-task` | 任务结构、推进状态和任务级持久化。 |
| `crates/ai-eval` | 评测和能力验证边界。 |
| `crates/runtime` | 会话运行、上下文组装、工具/MCP/provider 调度、运行时控制。 |
| `crates/session` | session 合同和生命周期存储。 |
| `crates/approval` | 用户介入、审批记录、权限与审计。 |
| `crates/model-protocol` | 模型协议、prompt cache、usage 合同。 |
| `crates/provider` | OpenAI/Anthropic/DeepSeek/Qwen 等模型 provider 适配。 |
| `crates/mcp` | MCP stdio / lifecycle 合同。 |

### 3.3 Fact 层

| crate | 职责 |
|---|---|
| `crates/fact-kernel` | 事实语义核心，连接 Memory 和 Matrix 的事实表达。 |
| `crates/memory` | 非结构化记忆、多层召回、上下文包、经验沉淀。 |
| `crates/matrix/core` | 结构化事实、实体、关系、证据、水位和图语义。 |
| `crates/matrix/repository` | Matrix 持久化仓储。 |

Memory 更偏知识、经验、语义关联和上下文召回。Matrix 更偏结构化事实、实体关系、证据链、可计算推理和应用数据基础。二者通过 `fact-kernel` 建立可互相促进但不混淆的边界。

### 3.4 Tool / Skill / Connector 层

| crate | 职责 |
|---|---|
| `crates/tools` | 内置工具、工具 schema、工具执行和工具治理。 |
| `crates/skill/service` | skill catalog、projection、run、governance 和 API 服务。 |
| `crates/connector` | 外部资源、账号、服务、资源状态和 cross-plane 合同。 |
| `crates/channel` | Gateway 层使用的平台/channel 合同，不包含 SDK 实现。 |
| `crates/plugins` | plugin manifest、registry 和生命周期。 |

渠道自身的聊天、收发消息、长连接、静态资源等属于 surface/sidecar；渠道附带的文档操作、平台高级能力未来应作为 skill/tool 安装，而不是塞回 Runtime 或 Gateway。

### 3.5 Application 层

| crate | 职责 |
|---|---|
| `crates/app-mfg` | MFG 制造应用层。基于 Matrix/Memory，不属于内核。 |
| `crates/storage` | 通用 SQLite/存储基础。 |
| `crates/telemetry` | 事件和遥测基础类型。 |

## 4. Gateway 与 Surface

### 4.1 Gateway 职责

Gateway 是所有 UI 和外部 surface 使用 core 能力的后端服务入口。

它负责：

- 启动 RuntimeHost。
- 组装 GatewayServices。
- 暴露 HTTP/SSE API。
- 发现 surface manifest。
- 托管 WebUI 静态资源。
- 转发 surface callback/webhook/OAuth redirect。
- 管理 JSONL sidecar 生命周期。
- 收集 surface health/events。
- 将外部渠道的 ingress/egress 接入 Gateway 服务边界。

Gateway 不负责：

- 渲染 TUI/WebUI。
- 链接飞书、邮件、企微、微信等平台 SDK。
- 直接执行 AI turn 的内部细节。
- 作为第二套 runtime 或第二套会话状态。

### 4.2 Surface 协议

Surface 通过 `surface.json` 描述自己：

```json
{
  "schema": "cowd.surface.v1",
  "id": "feishu",
  "name": "Feishu Surface",
  "kind": "external-integration",
  "entry": "./cowd-surface-feishu",
  "transport": "stdio-jsonl",
  "lifecycle": "managed",
  "capabilities": ["ingress", "egress", "callback", "health"],
  "routes": [
    { "kind": "callback", "path": "/events", "method": "POST", "public": true }
  ],
  "resources": [],
  "health": { "mode": "jsonl", "interval_ms": 30000 },
  "default_enabled": false
}
```

Gateway 按 manifest 提供：

| API | 用途 |
|---|---|
| `GET /api/surfaces` | 已发现 surface 列表 |
| `GET /api/surfaces/:id/health` | 单个 surface health |
| `GET /api/surfaces/:id/events` | managed sidecar event buffer |
| `GET /api/surfaces/:id/routes` | surface route 摘要 |
| `GET /api/surfaces/:id/resources` | surface static resource 摘要 |
| `GET /s/:surface/*path` | surface 静态资源转发 |
| `GET|POST /surface-callback/:surface/*path` | callback/webhook 转发 |

### 4.3 WebUI

WebUI 不在 core 仓库。它位于：

```text
cowd-surface/surfaces/webui
```

Gateway 通过配置读取 WebUI 构建产物：

```yaml
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-surface/surfaces/webui/dist"
```

如果未配置 `gateway.webui_dir`，或者目录没有 `index.html`，Gateway 仍应健康启动，并在根路由返回 health/status，而不是失败退出。

### 4.4 TUI

TUI 是 core 仓内唯一 UI surface，但默认 debug 不编译。这样日常开发可以让 Gateway 和 TUI 分开演进，避免所有开发者都为终端渲染依赖付出编译成本。

```bash
# 默认开发，不带 TUI
cargo check
cargo build -p cli --bin cowd

# TUI 联调 / full 构建
cargo check -p cli --bin cowd --features full
cargo build -p cli --bin cowd --features full
```

TUI 的定位不是 WebUI 的终端复刻版，而是终端环境中的 `Terminal Control Surface`：

- 默认以键盘优先、正文优先、低噪声方式操控后端服务。
- 通过 Gateway HTTP/SSE attach session、订阅事件、发送消息、执行 cancel 和刷新投影。
- 支持 `Clean / Panorama` 两种显示语义：Clean 只保留正文和关键计数，Panorama 展开运行线索和证据。
- 顶部状态条展示 Gateway/session、turn 状态、display mode、context/memory/reality evidence 摘要。
- `Control Deck` 聚合 Gateway、Runtime readiness、session、lease、task、approval、surface、Reality Core、Fact Flow 和 degraded signal。
- TUI 不直接读取 runtime/channel/provider/store，不越过 Gateway 查内部表。

常用终端快捷键：

| 快捷键 | 用途 |
|---|---|
| `Alt+V` | 切换 Clean / Panorama |
| `Alt+E` | 打开 runtime/evidence panorama 面板 |
| `Alt+G` | 打开 Gateway Control Deck |
| `Ctrl+P` | 打开命令面板 |
| `Esc Esc` 或 `Ctrl+C Ctrl+C` | 当前 turn 中取消，空闲时退出 |

## 5. 主要 API 能力

### 5.1 健康与状态

| API | 用途 |
|---|---|
| `GET /health` | 简单健康 |
| `GET /healthz` | Gateway health |
| `GET /readyz` | ready 状态，包含 WebUI 静态资源状态 |
| `GET /api/webui/manifest` | WebUI 静态资源 manifest |

### 5.2 Session / Runtime

| API | 用途 |
|---|---|
| `GET /api/sessions` | session 列表 |
| `POST /api/sessions` | 创建 session |
| `GET /api/sessions/:id/events` | session event |
| `GET /api/sessions/:id/runs` | run 列表 |
| `POST /api/sessions/:id/messages` | 发送消息 |
| `GET /api/sessions/:id/stream` | SSE stream |
| `GET /api/runtime/timeline` | runtime timeline |
| `GET /api/runtime/control-plane` | 控制面摘要 |

### 5.3 Context / Memory

| API | 用途 |
|---|---|
| `GET /api/context/current` | 当前上下文 |
| `GET /api/evidence/resolve` | evidence ref 解析 |
| `GET /api/memory/status` | memory 状态 |
| `GET /api/memory/search` | memory 搜索 |
| `GET /api/memory/packet` | context packet |
| `GET /api/memory/entities` | entity |
| `GET /api/memory/triples` | triples |
| `POST /api/memory/facts/check` | fact check |

### 5.4 Skills

Skills API 分三层：Catalog、Projection、Action。

| API | 用途 |
|---|---|
| `GET /api/skills/catalog` | 技能全集 |
| `GET /api/skills/:id` | 技能详情 |
| `GET /api/skills/projection?surface=webui` | WebUI 投影 |
| `GET /api/skills/projection?surface=tui` | TUI 投影 |
| `GET /api/skills/projection?surface=cli` | CLI 投影 |
| `POST /api/skills/:id/actions/validate` | 校验 skill manifest/evidence/tools/quality gate |
| `POST /api/skills/:id/actions/plan` | 生成 skill plan |
| `POST /api/skills/:id/actions/run` | 执行 skill |
| `GET /api/skills/runs` | 最近 skill run |

### 5.5 Tools

| API | 用途 |
|---|---|
| `GET /api/tools` | 工具 registry |
| `POST /api/tools/execute` | 执行允许的工具 |
| `GET /api/tools/cache` | 工具缓存状态 |
| `POST /api/tools/batch-readonly` | 只读工具批处理 |
| `POST /api/tools/mutations/preview` | 变更预览 |
| `POST /api/tools/mutations/apply` | 变更应用 |
| `GET|POST /api/tools/checkpoints` | checkpoint 列表和创建 |
| `POST /api/tools/intent-plan` | intent plan |
| `POST /api/tools/context-fanout/plan` | context fanout plan |

### 5.6 Matrix / MFG

Matrix 是结构化事实引擎，MFG 是基于 Matrix/Memory 的制造应用层。

| API | 用途 |
|---|---|
| `GET /api/matrix/health` | Matrix store 健康 |
| `GET /api/matrix/entities` | entity 列表 |
| `POST /api/matrix/entities/upsert` | entity upsert |
| `POST /api/matrix/relations/upsert` | relation upsert |
| `POST /api/matrix/facts/ingest` | fact ingest |
| `GET /api/matrix/metrics` | metric 列表 |
| `GET /api/apps/mfg/app` | MFG app descriptor |
| `GET /api/apps/mfg/incidents` | incident 列表 |
| `POST /api/apps/mfg/incidents` | 创建 incident |
| `POST /api/apps/mfg/incidents/:id/analyze` | operational analysis |

## 6. 使用方式

### 6.1 默认开发

默认开发路径只验证 core，不编译 TUI：

```bash
cargo fmt --all --check
cargo check
cargo check --workspace --exclude tui --no-default-features
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
cargo build -p cli --bin cowd
```

### 6.2 Gateway

Gateway 是后台服务入口：

```bash
cargo run -p cli --bin cowd -- gateway
```

Gateway 启动后，TUI、WebUI 和外部 surface 都通过 Gateway API 使用核心能力。

### 6.3 TUI 联调

TUI 联调需要 full feature：

```bash
cargo run -p cli --bin cowd --features full
cargo run -p cli --bin cowd --features full -- tui
```

如果使用不带 TUI 的默认二进制请求 TUI，CLI 会明确提示该二进制未构建 TUI surface。

### 6.4 WebUI

WebUI 在 `cowd-surface` 构建：

```bash
cd ../cowd-surface
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
```

然后通过 `gateway.webui_dir` 指向 `surfaces/webui/dist`。

### 6.5 外部渠道

外部渠道在 `cowd-surface` 构建：

```bash
cd ../cowd-surface
cargo check --workspace --bins
cargo build --release -p surface-adapters --bins
```

每个渠道 surface 都通过 `surface.json` 暴露能力，不进入 core 依赖图。

## 7. Capability 与投影

Cowd 通过 capability registry 描述核心能力，并按 surface 投影。

```text
Capability Registry
  -> WebUI projection
  -> TUI projection
  -> CLI projection
  -> Surface manifest/status
```

设计要求：

- WebUI 是最强管理面，适合复杂表格、过滤、批量操作、治理证据和可视化。
- TUI 保持同一核心能力集，但以终端密度和键盘操作为优先；它用 Clean/Panorama、证据摘要和 Control Deck 实现对后端服务的快速掌控。
- CLI 只保留轻控制、配置、状态、诊断和启动，不做复杂业务管理。
- 外部渠道 surface 只负责消息入口、消息投递、callback、长连接和静态资源，不成为 Runtime 的一部分。

## 8. 配置

常见配置片段：

```yaml
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
gateway:
  enabled: true
  host: "127.0.0.1"
  port: 8642
  webui_dir: "/path/to/cowd-surface/surfaces/webui/dist"
```

模型/API 密钥属于配置和 secrets，不应成为顶层 auth 模块。Gateway 的 WebUI 静态资源配置是可选项，缺失时服务仍应可用。

## 9. 验证

core 发布前验证：

```bash
cargo fmt --all --check
cargo check
cargo check -p cli --bin cowd --features full
cargo test -p surface
cargo test -p tui
cargo test -p gateway --test gateway_runtimehost_architecture --no-default-features
scripts/scenarios/tui-daemon-attach.sh
```

surface 仓库验证：

```bash
cd ../cowd-surface
cargo fmt --all --check
cargo check --workspace --bins
npm --prefix surfaces/webui test
npm --prefix surfaces/webui run build
```

依赖边界验证重点：

```bash
cargo tree -p cli --edges normal | rg 'tui|ratatui|crossterm|syntect|tui-textarea'
cargo tree -p gateway --edges normal | rg 'surface-adapters|lettre|imap|mail-parser'
```

默认情况下第一条不应输出 TUI 渲染依赖；第二条不应输出平台 SDK。

## 10. 当前状态

- core workspace 已删除旧平台适配 crate。
- `crates/cli` 默认不编译 `crates/tui`。
- `crates/gateway` 保留 `channel` 合同和 `surface` 协议，但不依赖平台 SDK。
- `crates/runtime` 不依赖 channel/surface adapter。
- `cowd-surface` 承载 WebUI 和非 TUI sidecar。
- TUI 已形成 Clean/Panorama、Control Deck 和 Gateway attach 场景验证闭环。
- 版本标签：`v0.9.358`。
