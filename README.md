# Cowd

Rust 原生 AI Agent 运行时，提供 CLI、TUI、WebUI、HTTP Gateway、统一会话、记忆系统、工具执行、技能管理、Capability 投影、Structured Data Core、Surface Parity Contract、Matrix 事实引擎、MFG 应用层和生产 Release Gate。

当前内核版本：`0.9.296`

当前 WebUI 重构验收版本：`v0.9.245`

当前安装约定：

- 主程序：`~/AI/cowd`
- WebUI 静态资源：由配置项 `gateway.webui_dir` 指向外部构建产物目录
- 不再使用：`~/AI/bin`

---

## 1. 快速开始

### 1.1 直接使用已安装版本

```bash
~/AI/cowd --version
~/AI/cowd doctor
~/AI/cowd setup
```

### 1.2 启动 CLI / TUI

```bash
~/AI/cowd
```

默认进入交互式终端界面。也可以指定模型、会话、权限：

```bash
~/AI/cowd --model claude-sonnet-4-6
~/AI/cowd --session my-session
~/AI/cowd --permission-mode workspace-write
```

交互式输入：

```bash
~/AI/cowd
```

一次性 `prompt/run/chat` CLI 入口已经移除；请在 TUI 或 WebUI 中发起对话。

恢复会话：

```bash
~/AI/cowd --resume latest
```

进入后在 TUI 内使用 `/status`、`/diff`、`/session` 等交互命令。

### 1.3 启动 WebUI / Gateway

推荐从项目工作区或目标工作区启动：

```bash
cd /path/to/workspace
~/AI/cowd gateway start
```

默认 HTTP Gateway 由配置文件决定。常见本地配置如下：

```yaml
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
gateway:
  enabled: true
  sessionReset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
      auth:
        enabled: false
```

访问：

```text
http://127.0.0.1:8642/
```

健康检查：

```bash
curl http://127.0.0.1:8642/healthz
curl http://127.0.0.1:8642/readyz
```

Gateway 静态资源解析顺序：

1. 配置文件中的 `gateway.webui_dir`
2. 未配置或目录下无 `index.html` 时返回 Gateway 健康/接口状态

因此正式安装时主仓只需要：

```text
~/AI/cowd
```

---

## 2. 顶层框架逻辑

Cowd 的核心不是单一聊天 CLI，而是一个统一 runtime。CLI、TUI、WebUI 和外部渠道都只是 runtime 的不同投影。所有能力通过 Capability Registry 注册，按 WebUI/TUI/CLI 三表面投影，并由 Surface Parity Contract 保障一致性。

```text
用户入口
  ├─ CLI: cowd / cowd skills / cowd doctor / cowd gateway
  ├─ TUI: cowd 交互界面
  ├─ WebUI: Gateway 提供浏览器控制台
  └─ Channel: Feishu / WeCom / Email / MCP / future connectors

统一运行时
  ├─ Session Kernel
  ├─ Runtime Event Bus
  ├─ Context Runtime
  ├─ Memory System
  ├─ Tool Runtime
  ├─ Skill Registry + Skill Action API
  ├─ Capability Registry + Projection
  ├─ Structured Data Core
  ├─ Execution Outcome Bridge
  ├─ Connector Runtime
  ├─ Cross-plane Governance
  ├─ Release Gate
  └─ MFG Operating Intelligence

持久化
  ├─ SQLite Session Store
  ├─ Memory Stores
  ├─ Matrix SQLite Store
  └─ Config / Skills / Plugins 文件系统目录
```

关键原则：

- Session 是运行时根对象。
- SQLite 是会话状态事实源。
- WebUI/TUI 不发明独立状态，只投影 Gateway API/service contract。
- CLI 保持极简，不承担复杂状态管理。
- Skills 的发现、投影、执行统一走 `/api/skills/*`。
- MFG 的领域能力保留专用 API，同时通过 Skills Action API 对外统一。
- 所有能力通过 Capability Registry 声明，三表面投影由 Surface Parity Contract 保障。

---

## 3. Workspace 和 crate 结构

### 3.1 Cargo workspace

根目录 `Cargo.toml` 使用 workspace：

```text
crates/provider
crates/commands
crates/compat-harness
crates/cowd-cli
crates/memory
crates/plugins
crates/runtime
crates/telemetry
crates/tools
```

### 3.2 `crates/cowd-cli`

主二进制 crate，输出 `cowd`。

职责：

- CLI 参数解析
- 交互式 REPL / TUI 启动
- Gateway 启停
- HTTP API 路由聚合
- 静态 WebUI 服务
- Session lifecycle 接入
- Matrix/MFG API 接入
- Capability / Projection / Release Gate API 接入
- Structured Data API 接入
- TUI panel 状态构建
- install / doctor / setup / init / export / import-session 等本地命令

重要文件：

| 文件 | 说明 |
|---|---|
| `src/main.rs` | 主入口，CLI action 分发，TUI 启动，Gateway 启动，安装逻辑 |
| `src/daemon/mod.rs` | HTTP daemon / Gateway 服务 |
| `src/api_routes.rs` | API route 聚合 |
| `src/api_routes/*.rs` | 各业务域 API |
| `src/api_routes/cowd_routes.rs` | Capability、Projection、Release Gate、Structured Data API |
| `src/gateway_static.rs` | WebUI 静态资源解析 |
| `src/gateway_health.rs` | Gateway 健康和 ready 状态 |
| `src/session_kernel.rs` | Session 运行时基础 |
| `src/session_lifecycle_kernel.rs` | Session attach / lease / lifecycle |
| `src/runtime_protocol.rs` | 运行时协议类型 |
| `src/tui/*` | TUI 状态、布局、面板和控制客户端 |

### 3.3 `crates/runtime`

运行时核心库。

职责：

- 会话模型和执行上下文
- 工具调用 runtime 抽象
- MCP stdio / OAuth / connector 基础
- 平台和 channel 类型
- Capability Registry + Surface Projection
- Structured Data Core（Source、Mapping、Fact、Evidence、Ingest、Watermark）
- Execution Outcome Bridge（Tool/Agent/Task/Manufacturing）
- Graph Quality Contracts
- Tool Execution Plans
- Tool Memory（工具事实到记忆候选）
- Release Gate（证据驱动发布门）
- MFG 领域模型、store、skill pack
- server manufacturing domain pack + app descriptor
- skill plan/run/activation/dependency/memory 逻辑

重要目录：

| 路径 | 说明 |
|---|---|
| `src/capability.rs` | Capability 注册和声明 |
| `src/projection.rs` | 按表面投影能力 |
| `src/surface_contract.rs` | Surface Parity Contract |
| `src/structured_data.rs` | Structured Data 核心契约 |
| `src/execution_outcome.rs` | 执行结果统一桥接 |
| `src/graph_contract.rs` | Graph 质量契约 |
| `src/tool_execution_plan.rs` | 可解释批量工具执行计划 |
| `src/tool_memory.rs` | 工具事实到记忆候选的策略转换 |
| `src/release_gate.rs` | 证据驱动发布门 |
| `src/gates.rs` | PreFlight/Revision/Escalation/Abort Gate 机制 |
| `src/green_contract.rs` | Green 合约分级 |
| `src/matrix/*` | Matrix 结构化事实引擎 |
| `crates/app-mfg` | MFG 制造应用边界 |
| `src/mfg/*` | MFG 兼容桥，迁移期保留，后续从 runtime 内核移除 |
| `src/platform/*` | 平台和 channel 类型 |
| `src/mcp_stdio.rs` | MCP stdio 管理 |
| `src/connector.rs` | Connector 基础 |
| `src/session.rs` | session 基础类型 |
| `src/tool_runtime.rs` | 工具 runtime |

### 3.4 `crates/memory`

记忆与上下文系统。

职责：

- 多层记忆模型
- SQLite memory store
- FTS / BM25 / entity / triple
- 代码索引和项目知识图谱
- compression / handoff / rebuild
- write guard / audit / consistency

典型能力：

- session memory
- project memory
- shared memory
- code symbol indexing
- memory packet assembly
- fact checking
- context fence

### 3.5 `crates/commands`

命令和技能解析库。

职责：

- slash command parser
- `/skills`、`/agents`、`/mcp` 等命令处理
- `SkillRegistry`
- skill discovery
- skill install / view / list
- 兼容 `.cowd/skills`、`.agents/skills`、`.codex/skills`、`~/.cowd/skills`、`~/.codex/skills`、legacy `/commands`

CLI 的 skills 能力由这里收敛为极简：

```text
list
view <name>
install <path>
<skill> [args] 作为 prompt dispatch
```

复杂的 validate / plan / run / watch / governance 不放在 CLI 状态管理中，而由 WebUI/TUI + API 承担。

### 3.6 `crates/tools`

工具定义和内置工具集合。

职责：

- 文件读写
- shell 执行
- grep/glob/search
- memory 工具
- web fetch
- tool schema
- runtime tool registry

### 3.7 `crates/provider`

模型 Provider 适配层。

职责：

- Anthropic / OpenAI / DeepSeek / Qwen 等 API 适配
- streaming / non-streaming
- 请求签名和响应结构

### 3.8 `crates/plugins`

插件系统。

职责：

- plugin manifest
- plugin registry
- plugin install / enable / disable / uninstall
- 插件生命周期状态

### 3.9 `crates/telemetry`

遥测基础类型和 JSON 事件结构。

### 3.10 `crates/compat-harness`

兼容性测试和命令/tool 对齐验证。

---

## 4. WebUI 边界

WebUI 源码已拆分到独立 `cowd-webui` 仓库。主仓不再包含前端源码、Node 依赖、前端构建脚本或视觉测试脚本。

主仓负责：

- 提供 Gateway 静态托管配置项 `gateway.webui_dir`。
- 提供 `/api/webui/manifest` 说明当前静态资源状态。
- 保证未配置 WebUI 静态资源时 Gateway 仍可健康启动并返回 API。
- 提供 WebUI/TUI/CLI surface projection、capability parity、写操作契约和后端管理 API。

WebUI 仓库负责：

- Vue/Vite 应用源码、前端状态管理、页面组件和样式系统。
- API client、RawPayload 渲染、写操作回执、视觉审计和浏览器端 E2E。
- 构建产物 `dist/`，由主仓 Gateway 通过 `gateway.webui_dir` 托管。

WebUI 的定位：

- 浏览器端最强管理面，承担复杂状态、表格、过滤、详情、批量操作、证据 drill-down 和治理回执展示。
- 左侧一级 icon 是全局模块入口；一级模块不重复展示假二级菜单。
- 只有页面内部确实复杂时才使用模块内二级分区，例如 Skills 的 Catalog/Projection/Runs/Governance，MFG 的 Data Plane/Entities/Metrics/Incidents/Actions/Reports。
- 右侧 Companion 只承载与主区域互补的 Activity、Thinking、Workspace、Inspector；Workspace 支持目录浏览、预览、编辑、上传、重命名和删除。
- Chat 只保留对话主流程，用户消息右侧、系统/助手消息左侧，正文使用 Markdown 渲染。
- 所有写操作尽量返回 `RequestReceipt`，包含 request id、status、changed refs、audit ref、warnings 和 next actions。
- 原始 JSON 只能作为具名调试/证据/结果详情折叠展示，不能替代主管理界面。

---

## 5. API 和 Gateway

Gateway 暴露 HTTP API 和 WebUI 静态资源。

### 5.1 公共 API

| API | 用途 |
|---|---|
| `GET /health` | 简单健康 |
| `GET /healthz` | Gateway health |
| `GET /readyz` | ready 状态，包含 static WebUI source |
| `GET /api/webui/manifest` | WebUI 服务 manifest |
| `POST /api/auth/login` | token 登录 |
| `GET /api/auth/verify` | token 验证 |

### 5.2 Session / Message

| API | 用途 |
|---|---|
| `GET /api/sessions` | session 列表 |
| `POST /api/sessions` | 创建 session |
| `GET /api/sessions/search` | 搜索消息 |
| `GET /api/sessions/:id/events` | session event |
| `GET /api/sessions/:id/runs` | run 列表 |
| `POST /api/sessions/:id/compact` | compact |
| `GET /api/sessions/:id/stats` | session 统计 |
| `POST /api/sessions/:id/messages` | 发送消息 |
| `GET /api/sessions/:id/stream` | SSE stream |

### 5.3 Runtime / Context / Memory

| API | 用途 |
|---|---|
| `GET /api/runtime/timeline` | runtime timeline |
| `GET /api/runtime/control-plane` | 控制面摘要 |
| `GET /api/context/current` | 当前上下文 |
| `GET /api/evidence/resolve` | evidence ref 解析 |
| `GET /api/memory` | memory 总览 |
| `GET /api/memory/status` | memory 状态 |
| `GET /api/memory/search` | memory 搜索 |
| `GET /api/memory/packet` | context packet |
| `GET /api/memory/entities` | entity |
| `GET /api/memory/triples` | triples |
| `POST /api/memory/facts/check` | fact check |
| `POST /api/memory/facts/register` | fact register |

### 5.4 Skills API

Skills API 分三层：Catalog、Projection、Action。

Catalog：

| API | 用途 |
|---|---|
| `GET /api/skills/catalog` | 技能全集 |
| `GET /api/skills/catalog?scope=mfg` | 过滤 MFG skills |
| `GET /api/skills/:id` | 技能详情 |

Projection：

| API | 用途 |
|---|---|
| `GET /api/skills/projection?surface=webui` | WebUI 投影 |
| `GET /api/skills/projection?surface=tui` | TUI 投影 |
| `GET /api/skills/projection?surface=cli` | CLI 投影 |

Action：

| API | 用途 |
|---|---|
| `POST /api/skills/:id/actions/validate` | 校验 skill manifest / evidence / tools / quality gate |
| `POST /api/skills/:id/actions/plan` | 基于 incident 生成 skill plan |
| `POST /api/skills/:id/actions/run` | 基于 incident 执行 skill |
| `GET /api/skills/runs` | 最近 skill run |
| `GET /api/skills/runs/:id` | 单个 skill run 详情 |

MFG skill 示例：

```bash
curl http://127.0.0.1:8642/api/skills/catalog
curl http://127.0.0.1:8642/api/skills/mfg:supply-risk-analyst

curl -X POST http://127.0.0.1:8642/api/skills/mfg:supply-risk-analyst/actions/validate \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo-validate"}'

curl -X POST http://127.0.0.1:8642/api/skills/mfg:supply-risk-analyst/actions/plan \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo-plan","incident_id":"incident-id","limit":3}'

curl -X POST http://127.0.0.1:8642/api/skills/mfg:supply-risk-analyst/actions/run \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo-run","incident_id":"incident-id"}'
```

Local skill 的 action 当前返回 `unsupported_for_local_skill`。这是有意设计：local skill 在 CLI 侧保留 list/view/install/invoke，不承担 MFG 状态管理。

### 5.5 Cowd Capability / Structured Data API

Cowd 自身的能力、投影和结构化数据 API：

| API | 用途 |
|---|---|
| `GET /api/cowd/capabilities` | Capability Registry 全集 |
| `GET /api/cowd/projection?surface=webui` | 按表面投影 |
| `GET /api/cowd/surfaces` | Surface Parity Contract |
| `GET /api/cowd/release-gate` | Release Gate 报告 |
| `GET /api/cowd/structured/sources` | 结构化数据源列表 |
| `GET /api/cowd/structured/sources/:id` | 结构化数据源详情 |
| `POST /api/cowd/structured/ingest-plan` | 创建数据摄入计划 |
| `GET /api/cowd/structured/facts` | 结构化事实列表 |
| `GET /api/cowd/structured/evidence` | 结构化证据列表 |
| `GET /api/cowd/structured/watermarks` | 数据水位列表 |

### 5.6 Matrix/MFG API

Matrix API 是结构化事实引擎管理层；MFG API 是 `cowd-app-mfg` 制造应用层。MFG 不属于 cowd 内核，迁移期通过 runtime 兼容桥复用既有实现。

常用入口：

| API | 用途 |
|---|---|
| `GET /api/matrix/health` | Matrix store 健康 |
| `POST /api/apps/mfg/domain/server-manufacturing/seed` | 注入服务器制造领域种子 |
| `GET /api/apps/mfg/skills` | MFG skill pack |
| `GET /api/apps/mfg/command-center` | command center |
| `GET /api/apps/mfg/command-center/live` | live 队列 |
| `GET /api/matrix/entities` | entity 列表 |
| `POST /api/matrix/facts/ingest` | fact ingest |
| `GET /api/matrix/metrics` | metric 列表 |
| `GET /api/matrix/changes` | change event |
| `GET /api/matrix/attention/hot` | attention hot |
| `POST /api/matrix/evidence/build` | 构建 evidence packet |
| `POST /api/apps/mfg/incidents` | 创建 incident |
| `GET /api/apps/mfg/incidents` | incident 列表 |
| `POST /api/apps/mfg/incidents/:id/analyze` | operational analysis |
| `POST /api/apps/mfg/incidents/:id/skills/plan` | MFG 专用 skill plan |
| `POST /api/apps/mfg/incidents/:id/skills/:skill_id/run` | MFG 专用 skill run |
| `GET /api/apps/mfg/skill-runs/:id` | MFG skill run |

对外产品面推荐优先使用 `/api/skills/*` 的统一 action 协议。MFG 专用 API 保留为领域能力层和高级调试入口。

---

## 6. CLI 使用说明

CLI 的定位是极简控制和一次性任务，不承担复杂状态管理。

### 6.1 常用命令

```bash
~/AI/cowd --help
~/AI/cowd --version
~/AI/cowd status
~/AI/cowd sandbox
~/AI/cowd doctor
~/AI/cowd setup
~/AI/cowd init
```

### 6.2 TUI 对话

```bash
~/AI/cowd
~/AI/cowd --model claude-sonnet-4-6
```

CLI 不再提供一次性 `prompt/run/chat`。请在 TUI 或 WebUI 中输入消息。

### 6.3 Session

```bash
~/AI/cowd --resume latest
~/AI/cowd export conversation.md
~/AI/cowd import-session old-session.jsonl
```

`--resume` 只负责启动并附着 TUI；会话状态、压缩、切换等操作在 TUI 或 WebUI 中完成。

### 6.4 Skills

```bash
~/AI/cowd skills
~/AI/cowd skills list
~/AI/cowd skills view release
~/AI/cowd skills install ./my-skill
```

技能目录来源：

```text
.cowd/skills
.agents/skills
.codex/skills
~/.cowd/skills
~/.cowd/skills/omc-learned
~/.codex/skills
legacy /commands
```

### 6.5 Gateway

```bash
~/AI/cowd gateway start
~/AI/cowd gateway status
~/AI/cowd gateway stop
~/AI/cowd gateway restart
~/AI/cowd gateway logs
~/AI/cowd gateway repair
~/AI/cowd gateway open
```

---

## 7. TUI 使用说明

启动：

```bash
~/AI/cowd
```

TUI 是控制台式全能力入口，适合键盘操作和本地开发循环。

主要区域：

| 区域 | 用途 |
|---|---|
| Chat | 对话和任务执行（支持 inline thinking、结构化摘要、流式输出） |
| Files | workspace 文件浏览 |
| Memory | 记忆条目和上下文 |
| Runtime | runtime activity（含工具进程面板） |
| Context | 当前上下文、证据、推荐 |
| Skills | skill 全集、action availability |
| Agents | agent 和子任务 |
| Gateway | gateway 状态 |
| Diff | 工作区 diff |

常用 slash commands：

```text
/status
/diff
/doctor
/setup
/skills
/skills view <name>
/skills install <path>
/agents
/mcp list
/memory
/context show
/approvals list
/cross-plane summary
/session list
/session switch latest
/export conversation.md
```

Skills panel 控制台操作：

| 键 | 作用 |
|---|---|
| `j` / `↓` | 下移 |
| `k` / `↑` | 上移 |
| `/` | 搜索 |
| `Tab` | 切换分类或 scope |
| `Enter` | toggle 本地 enabled 状态 |
| `v` | validate action 提示 |
| `p` | plan action 提示 |
| `r` | run action 提示 |
| `w` | watch action 提示 |

说明：

- TUI 展示 WebUI 等同的核心能力全集。
- TUI 当前以控制台方式显示 action availability。
- 真正的 MFG skill action 通过统一 Skills API 和 incident id 执行。
- local skills 保持 view/import/invoke 边界，不做状态 run。
- TUI 支持 inline thinking 渲染和流式输出稳定性。
- 启动时保持状态和记忆面板稳定性。

---

## 8. WebUI 使用说明

启动 Gateway 后打开：

```text
http://127.0.0.1:8642/
```

WebUI 是 cowd 的浏览器增强管理面：

- session 浏览和搜索
- 聊天和 stream
- Workspace 文件浏览、预览、编辑、上传、重命名、删除
- Runtime timeline、provider reload、session lease、approval cockpit
- Context packet、预算、证据解析、recommendation
- Memory recall、entity/triple、fact check、maintenance、Structured Data Core
- Skills catalog、projection、validate、plan、run、runs、governance
- Agents team profile、task、phase、review、graph、persistent run evidence
- Tools registry、command history、risk preflight、tool action result
- Gateway connector、resource、identity、grant、cross-plane execution
- MFG manufacturing 应用全链路管理
- Audit、usage、release gate、governance evidence
- Settings token verify/clear、manifest、endpoint 状态

### 8.1 Skills 面板

入口：左侧 `Skills` icon。

能力：

- surface 选择：`WEBUI`、`TUI`、`CLI`
- search task
- incident id 输入
- skill catalog
- skill projection
- governance 摘要
- activation 摘要
- `View`
- `Validate`
- `Plan`
- `Run`
- Runs 队列
- Runs 过滤
- Watch 自动刷新
- Run detail 展开
- RequestReceipt 展示 validate/plan/run 的后端回执
- RawPayload 仅作为 run/detail/action result 的折叠证据

MFG skill 执行流程：

1. 在 MFG App 页面或 API 中创建 incident。
2. 在 Skills 面板输入 incident id。
3. 对目标 MFG skill 点 `Validate`。
4. 点 `Plan` 查看证据需求和 agent node plan。
5. 点 `Run` 生成 skill run。
6. 在 Runs 区域过滤、查看、展开详情。
7. 开启 `Watch` 观察后续运行记录。

### 8.2 Memory 面板

入口：左侧 `Memory` icon。

用途：

- Recall：搜索和构建当前上下文 packet
- Entities：实体、符号、关系、集群和链接
- Facts：事实注册、事实判断、冲突和证据
- Structured Core：`/api/cowd/structured/*` 的数据源、事实、证据、水位和 ingest plan
- Maintenance：过期候选、维护任务、压缩和修复

Structured Data Core 属于 cowd 内核，不属于 MFG。MFG 只消费和扩展制造领域 schema、workflow、metric、incident、report。

0.9.295 起，cowd 内核新增统一 Storage Registry：默认根目录为 `~/.cowd/storage/`，设置 `COWD_CONFIG_HOME` 时使用 `$COWD_CONFIG_HOME/storage/`。Registry 统一声明 `session.sqlite`、`memory.sqlite`、`matrix.sqlite`、`resource-directory.sqlite`、`tasks.sqlite`、`audit.sqlite`、`approval.sqlite`、`files/approval_history.json`、`files/always_approved.json`、`files/audit.jsonl` 和 `blobs/`。Gateway `/healthz` 与 `/readyz` 会展示 storage registry、migration 和 SQLite 锁诊断。

Memory 旧版默认存储曾使用 `~/.cowd/memory/memory.db`、`~/.cowd/memory/blobs`，更早版本也可能在项目工作目录生成 `memory.db` 或 `memory_blobs`。0.9.295 不会静默移动历史数据；需要保留历史数据时，应先备份旧文件，再迁移到统一 storage 目录，或在配置中显式设置历史路径。

### 8.3 Agents 面板

入口：左侧 `Agents` icon。

用途：

- 自动组队和 team profile 持久化
- task 启动、phase 启动、phase review、cancel/complete/failure
- agent graph 和 execution lane 查看
- review evidence 和 action result 回执
- profile create/copy/reuse/delete 写操作回执

### 8.4 Gateway 面板

入口：左侧 `Gateway` icon。

用途：

- connector summary、accounts、resources、MCP servers
- cross-plane readiness、identity binding、grant
- action preflight、dry-run/live execution
- audit 记录和 RequestReceipt

### 8.5 MFG App 页面

入口：左侧 `MFG` app icon。

MFG 是 manufacturing application layer on top of the cowd kernel。当前页面按真实业务分区：

- Overview：health、operating load、contract summary
- Data Plane：source pack、connector run、delta plan、ingest plan
- Manufacturing Ingestion：fact ingest、domain seed、metric/change/attention 结果
- Entities：entity upsert、source key resolve、relation upsert、impact graph
- Metrics：lineage、materialize、attention plan、compute job
- Evidence：evidence packet、quality gate
- Incident Room：incident create/open、room detail
- Actions：analysis、playbook、case promotion、cross-plane bridge
- Skills：manufacturing skill plan/run
- Reports：cockpit report generate/retry

高风险 live 写操作通过 `mfgWriteContracts.json` 治理，页面展示 execution mode、impact preview、payload editor、schema plan、receipt/audit 约束。当前审计要求所有写操作都必须有后端 route、UI 调用、测试证据或明确隔离策略。

### 8.6 Settings 面板

入口：左侧 `Settings` icon。

用途：

- 查看 WebUI manifest 和 Gateway endpoint
- 设置 API token
- 验证 token
- 清除 token
- 查看本地存储和连接状态

---

## 9. Capability 系统

Capability 系统是 v0.9.293 的顶层治理机制，用于声明、投影和验证各表面能力。

### 9.1 Capability Registry

所有能力在 `CowdCapabilityRegistry::core()` 中注册，按层级分类：

| 层级 | 能力 | 状态 |
|---|---|---|
| Kernel | Runtime Session | Available |
| Kernel | Context Runtime | Available |
| Kernel | Memory Runtime | Available |
| Kernel | Structured Data Core | Available |
| Kernel | Runtime Event | Available |
| Kernel | Skill Lifecycle | Available |
| Kernel | Connector Runtime | Available |
| Application | MFG Manufacturing Application | Preview |

### 9.2 Surface Projection

每项能力按 WebUI/TUI/CLI 三表面投影：

- **WebUI**：Enhanced 模式，支持 browse/filter/compare/batch_manage/audit
- **TUI**：Full 模式，支持 browse/inspect/trigger/diagnose
- **CLI**：Minimal 模式，支持 list/view/invoke

### 9.3 Surface Parity Contract

`GET /api/cowd/surfaces` 返回 `CowdSurfaceParityContract`，明确声明：

- WebUI 和 TUI 达到 full parity
- CLI 保持 minimal control 定位
- 每个表面应承载的能力数量和主要操作

### 9.4 Release Gate

`GET /api/cowd/release-gate` 返回证据驱动的 Release Gate 报告，检查：

- Structured indexes readiness
- Structured watermark persistence
- Execution outcome timeline availability
- Memory context bridge availability
- Graph skill quality contracts availability

---

## 10. Structured Data Core

Structured Data Core 是 v0.9.293 的通用结构化数据契约层，用于统一管理和运营结构化数据。

核心概念：

| 概念 | 说明 |
|---|---|
| Source | 外部数据源定义（name, domain, owner, access_mode, refresh_mode） |
| Mapping | 源到目标实体/事实的映射（source_ref, collection, target_kind, target_type） |
| Fact | 结构化事实（fact_type, entity, metric_key, measure_value, confidence） |
| Evidence | 证据包（refs, score, fact_refs, structured_refs） |
| Ingest Plan | 数据摄入计划（source_id, mappings, delta_signature, entity_mapping） |
| Watermark | 数据摄入水位标记（source_id, last_sync, delta_state） |

API 入口：`/api/cowd/structured/*`

---

## 11. Execution Outcome Bridge

Execution Outcome Bridge 将工具调用、Agent 执行、Task 执行和 Manufacturing 操作的运行结果统一桥接到 Runtime Timeline。

支持的执行结果类型：

| 类型 | 说明 |
|---|---|
| Tool | 工具调用执行结果 |
| Agent | Agent 执行结果 |
| Task | Task 执行结果 |
| StructuredIngest | 结构化数据摄入结果 |
| StructuredFact | 结构化事实写入结果 |
| StructuredEvidence | 结构化证据构建结果 |
| ManufacturingCompute | 制造领域计算任务结果 |
| ManufacturingAction | 制造领域动作执行结果 |
| SkillRun | Skill 执行结果 |

所有执行结果通过 `RuntimeEvent` 写入 runtime timeline，支持 WebUI 和 TUI 面板实时浏览。

---

## 12. Tool Execution & Memory

### 12.1 Tool Execution Plans

`ToolExecutionPlan` 提供可解释的批量工具执行计划：

- 按安全分类（Read / Filesystem / Network / Destructive / Approval）分配执行模式
- 四种执行模式：ParallelRead、LimitedParallel、SerialDestructive、Wave
- 显式依赖关系和并发控制

### 12.2 Tool Memory

`ToolMemoryCandidatePolicy` 将工具调用事实策略性转换为记忆候选：

- 捕获失败的工具调用
- 捕获慢速工具调用（>30s）
- 写入 MemoryPulse 管道

---

## 13. MFG 核心逻辑

MFG 是 Cowd 的结构化运营智能层，用来把企业运营事实转为可追踪的 evidence、incident、analysis、action、report。

核心链路：

```text
SourceSnapshot
  -> Entity / Relation
  -> Fact ingest
  -> Metric recompute
  -> Change event
  -> Attention
  -> Evidence packet
  -> Quality gate
  -> Incident
  -> Operational analysis
  -> Skill plan
  -> Skill run
  -> Execution outcome (→ Timeline)
  -> Cross-plane action
  -> Feedback / Recovery
  -> Memory case / Playbook
  -> Cockpit report
```

主要模块：

| 模块 | 职责 |
|---|---|
| source | 外部数据源快照 |
| entity | 实体统一和 source key |
| relation | 实体关系和影响传播 |
| fact | 事实摄入和去重 |
| metric | 指标定义和指标状态 |
| metric_graph | 指标依赖和 lineage |
| compute | 增量计算计划和 job |
| change | 指标变动事件 |
| attention | 注意力队列 |
| evidence | 有界证据包 |
| quality | 质量门 |
| incident | 事件生命周期 |
| analysis | 归因、影响和建议动作 |
| execution | dry_run / commit / feedback |
| cockpit | profile / projection / report / delivery |
| skill | MFG skill manifest、plan、run |
| app | MFG 应用描述器（domain, surface, skill pack），对外边界在 `crates/app-mfg` |
| store | MFG 应用 Store 外观，底层暂与 Matrix SQLite 共用 |

Matrix store 默认位置：

```text
<workspace>/.cowd/matrix.sqlite
```

---

## 14. Skills 核心逻辑

Skills 是 Cowd 当前统一能力管理的核心面。

### 14.1 数据来源

```text
MFG skill pack
  + local SkillRegistry
    + .cowd/skills
    + .agents/skills
    + .codex/skills
    + ~/.cowd/skills
    + ~/.codex/skills
    + legacy /commands
```

### 14.2 三层协议

```text
Catalog: 发现和详情
Projection: 按 WebUI/TUI/CLI 投影能力
Action: validate / plan / run / runs / run detail
```

### 14.3 三端分工

| 入口 | 定位 | 能力 |
|---|---|---|
| CLI | 极简控制 | list、view、install、invoke |
| TUI | 控制台全集 | 展示全集、搜索、action availability、键盘操作 |
| WebUI | 管理全集 | catalog、projection、validate、plan、run、watch、runs、detail、filter |

---

## 15. Connector 和 Cross-plane

Connector runtime 负责外部系统接入。

典型对象：

- account
- capability
- resource ref
- connector run
- platform channel
- service action

Cross-plane governance 负责跨系统动作治理。

典型对象：

- identity binding
- grant
- preflight
- policy decision
- execution receipt
- audit record

常用 API：

```text
GET  /api/connectors/summary
GET  /api/connectors/accounts
GET  /api/connectors/mcp/servers
GET  /api/cross-plane/summary
POST /api/cross-plane/preflight
POST /api/cross-plane/execute
GET  /api/cross-plane/audit
```

---

## 16. 配置

配置文件常见位置：

```text
~/.cowd/config.yaml
<workspace>/.cowd/config.yaml
$COWD_CONFIG_HOME/config.yaml
```

常用字段：

```yaml
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: true
gateway:
  enabled: true
  sessionReset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: 8642
      auth:
        enabled: false
```

---

## 17. 开发和验证

### 17.1 Rust

```bash
cargo fmt --check
cargo test -p commands skill --no-default-features
cargo test -p cowd-cli skill --no-default-features -- --test-threads=1
cargo build -p cowd-cli --no-default-features
cargo build --release -p cowd-cli --no-default-features
```

### 17.2 WebUI

WebUI 源码、npm 测试、Playwright/视觉审计在独立 `cowd-webui` 仓库内执行。主仓只验证 Gateway 托管契约：

```bash
scripts/scenarios/gateway-webui-contract.sh
```

联动外部 WebUI 构建产物时，在配置中设置：

```yaml
gateway:
  webui_dir: "/path/to/cowd-webui/dist"
```

### 17.3 场景脚本

Skills 统一面验收：

```bash
COWD_BIN=/path/to/cowd scripts/scenarios/skill-surface-unification.sh
```

安装版验收：

```bash
COWD_BIN=~/AI/cowd scripts/scenarios/skill-surface-unification.sh
```

Release Gate：

```bash
scripts/validate.sh release
```

### 17.4 浏览器评测

WebUI E2E 使用 Playwright。若本地 Playwright 浏览器未安装，可指定系统 Chromium：

```js
chromium.launch({
  headless: true,
  executablePath: "/snap/bin/chromium",
  args: ["--no-sandbox"]
})
```

---

## 18. 安装和发布

当前推荐安装方式：

```bash
cargo build --release -p cowd-cli --no-default-features
rm -f ~/AI/cowd
install -m 0755 target/release/cowd ~/AI/cowd
~/AI/cowd --version
```

不再复制到：

```text
~/AI/bin/cowd
```

也不再需要：

```text
~/AI/bin/
```

---

## 19. 当前能力状态

已完成：

- 三分支统一：`master`、`develop`、`dev-mfg`
- `~/AI/cowd` 安装
- WebUI 静态资源外置并通过 `gateway.webui_dir` 托管
- Skills catalog/projection/action API
- WebUI Skills 闭环：validate/plan/run/watch/runs/detail/filter
- TUI Skills action availability
- CLI skills 极简化
- MFG skill pack 接入统一 Skills API
- Capability Registry + Surface Projection
- Surface Parity Contract（WebUI=TUI full parity, CLI=minimal）
- Structured Data Core（Source/Mapping/Fact/Evidence/Ingest/Watermark）
- Execution Outcome Bridge（Tool/Agent/Task/Manufacturing → Timeline）
- Release Gate（证据驱动发布门）
- Graph Quality Contracts
- Tool Execution Plans（可解释批量执行计划）
- Tool Memory（工具事实→记忆候选策略转换）
- MFG Manufacturing Application Descriptor
- WebUI Cowd MFG Workbench 面板
- TUI 结构化摘要、inline thinking、流式输出稳定
- TUI 启动状态和记忆面板稳定
- WebUI Vue/Vite 重构：左侧模块 icon、右侧 Companion、Workspace、Inspector、模块化管理页面
- WebUI 写操作回执体系：runtime、context、memory、skills、agents、tools、gateway、MFG
- Agents team profile 后端持久化 CRUD
- MFG governed action workbench 和高风险写操作契约
- API matrix / capability parity / RawPayload / visual audit 最终门禁

后续增强不属于当前五版必须项，但可以继续提升：

- TUI 的 Skills Action API 快捷交互可以进一步减少按键路径。
- WebUI skill runs 可以增加批量对比、归档和跨 incident 分析。
- local skills 可以增加更细的安全扫描和 manifest 修复建议。
- MFG action 与 memory case/playbook 的闭环可以增加更丰富的趋势视图。

---

## 20. 常见问题

### WebUI 打开是 404

检查：

```bash
~/AI/cowd gateway start
curl http://127.0.0.1:8642/readyz
curl http://127.0.0.1:8642/api/webui/manifest
```

`readyz` 和 `/api/webui/manifest` 会显示 `gateway.webui_dir` 的配置状态。未配置或目录下没有 `index.html` 时，Gateway 仍应保持健康，只是不提供浏览器控制台静态页面。

### Skills Run 报 `incident_id is required`

`plan` 和 `run` 需要 MFG incident。先通过 Matrix/MFG API 或 WebUI MFG App 页面创建 incident，再在 Skills 面板输入 incident id。

### local skill 不能 Run

这是设计约束。local skill 由 CLI 管理导入、查看和 prompt dispatch，不承载 MFG 状态执行。

### `cargo clippy -D warnings` 报 memory 历史 lint

当前严格 clippy 会被 `cowd-memory` 既有 lint 阻断。针对 Skills 闭环的验证使用：

```bash
cargo fmt --check
cargo test -p cowd-cli skill --no-default-features -- --test-threads=1
COWD_BIN=~/AI/cowd scripts/scenarios/skill-surface-unification.sh
```
