# Cowd

Rust 原生 AI Agent 运行时，提供 CLI、TUI、WebUI、HTTP Gateway、统一会话、记忆系统、工具执行、技能管理、Connector、Cross-plane 治理和 IACC 结构化运营智能。

当前版本：`0.9.108`

当前安装约定：

- 主程序：`~/AI/cowd`
- WebUI 静态资源：`~/AI/webui`
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

一次性 prompt：

```bash
~/AI/cowd prompt "总结当前仓库结构"
~/AI/cowd --output-format json prompt "列出可用 skills"
~/AI/cowd --compact "总结 Cargo.toml"
```

恢复会话：

```bash
~/AI/cowd --resume latest
~/AI/cowd --resume latest /status /diff
```

### 1.3 启动 WebUI / Gateway

推荐从项目工作区或目标工作区启动：

```bash
cd /path/to/workspace
~/AI/cowd gateway run
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

1. 当前工作区的 `webui/`
2. 安装目录的 `~/AI/webui`
3. fallback 路径

因此正式安装时只需要：

```text
~/AI/cowd
~/AI/webui
```

---

## 2. 顶层框架逻辑

Cowd 的核心不是单一聊天 CLI，而是一个统一 runtime。CLI、TUI、WebUI 和外部渠道都只是 runtime 的不同投影。

```text
用户入口
  ├─ CLI: cowd prompt / cowd skills / cowd doctor / cowd gateway
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
  ├─ Connector Runtime
  ├─ Cross-plane Governance
  └─ IACC Operating Intelligence

持久化
  ├─ SQLite Session Store
  ├─ Memory Stores
  ├─ IACC SQLite Store
  └─ Config / Skills / Plugins 文件系统目录
```

关键原则：

- Session 是运行时根对象。
- SQLite 是会话状态事实源。
- WebUI/TUI 不发明独立状态，只投影 daemon/API contract。
- CLI 保持极简，不承担复杂状态管理。
- Skills 的发现、投影、执行统一走 `/api/skills/*`。
- IACC 的领域能力保留专用 API，同时通过 Skills Action API 对外统一。

---

## 3. Workspace 和 crate 结构

### 3.1 Cargo workspace

根目录 `Cargo.toml` 使用 workspace：

```text
crates/api
crates/commands
crates/compat-harness
crates/cowd-cli
crates/memory
crates/mock-anthropic-service
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
- IACC API 接入
- TUI panel 状态构建
- install / doctor / setup / init / export / import-session 等本地命令

重要文件：

| 文件 | 说明 |
|---|---|
| `src/main.rs` | 主入口，CLI action 分发，TUI 启动，Gateway 启动，安装逻辑 |
| `src/daemon/mod.rs` | HTTP daemon / Gateway 服务 |
| `src/api_routes.rs` | API route 聚合 |
| `src/api_routes/*.rs` | 各业务域 API |
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
- IACC 领域模型、store、skill pack
- server manufacturing domain pack
- skill plan/run 逻辑

重要目录：

| 路径 | 说明 |
|---|---|
| `src/iacc/*` | IACC 结构化运营智能 |
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

### 3.7 `crates/api`

模型 Provider 适配层。

职责：

- Anthropic / OpenAI / DeepSeek / Qwen 等 API 适配
- streaming / non-streaming
- 请求签名和响应结构
- mock parity 支持

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

### 3.11 `crates/mock-anthropic-service`

本地 mock 服务，用于 API 和兼容测试。

---

## 4. WebUI 结构

WebUI 位于 `webui/`，由 Gateway 静态服务直接提供。

| 文件 | 说明 |
|---|---|
| `index.html` | 页面骨架、侧栏、聊天区、右侧 panel、控制中心 |
| `api.js` | 所有 HTTP API client |
| `boot.js` | DOMContentLoaded、登录、模型选择、panel 恢复 |
| `state.js` | 浏览器状态 |
| `sessions.js` | session 列表、创建、搜索 |
| `messages.js` | 消息发送、stream、渲染 |
| `workspace.js` | 文件树和 workspace API |
| `panels.js` | 右侧功能面板，Memory、Runtime、Context、Skills、IACC、Gateway 等 |
| `commands.js` | WebUI slash command autocomplete 和执行 |
| `ui.js` | 通用 UI helper、toast、modal、markdown |
| `style.css` | 设计系统和全部面板样式 |
| `modules.test.js` | Vitest 单元测试 |
| `*.e2e.spec.js` | Playwright E2E 测试 |

WebUI 的定位：

- 浏览器端管理面
- 适合复杂状态、表格、过滤、详情、批量操作
- 当前 Skills 面板已经支持 catalog、projection、detail、validate、plan、run、runs、watch
- IACC 面板用于领域运营智能的 command center、incident、report、cockpit

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
| `GET /api/skills/catalog?scope=iacc` | 过滤 IACC skills |
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

IACC skill 示例：

```bash
curl http://127.0.0.1:8642/api/skills/catalog
curl http://127.0.0.1:8642/api/skills/iacc:supply-risk-analyst

curl -X POST http://127.0.0.1:8642/api/skills/iacc:supply-risk-analyst/actions/validate \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo-validate"}'

curl -X POST http://127.0.0.1:8642/api/skills/iacc:supply-risk-analyst/actions/plan \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo-plan","incident_id":"incident-id","limit":3}'

curl -X POST http://127.0.0.1:8642/api/skills/iacc:supply-risk-analyst/actions/run \
  -H 'content-type: application/json' \
  -d '{"request_id":"demo-run","incident_id":"incident-id"}'
```

Local skill 的 action 当前返回 `unsupported_for_local_skill`。这是有意设计：local skill 在 CLI 侧保留 list/view/install/invoke，不承担 IACC 状态管理。

### 5.5 IACC API

IACC API 是结构化运营智能的领域实现层。

常用入口：

| API | 用途 |
|---|---|
| `GET /api/iacc/health` | IACC store 健康 |
| `POST /api/iacc/domain/server-manufacturing/seed` | 注入服务器制造领域种子 |
| `GET /api/iacc/skills` | IACC skill pack |
| `GET /api/iacc/command-center` | command center |
| `GET /api/iacc/command-center/live` | live 队列 |
| `GET /api/iacc/entities` | entity 列表 |
| `POST /api/iacc/facts/ingest` | fact ingest |
| `GET /api/iacc/metrics` | metric 列表 |
| `GET /api/iacc/changes` | change event |
| `GET /api/iacc/attention/hot` | attention hot |
| `POST /api/iacc/evidence/build` | 构建 evidence packet |
| `POST /api/iacc/incidents` | 创建 incident |
| `GET /api/iacc/incidents` | incident 列表 |
| `POST /api/iacc/incidents/:id/analyze` | operational analysis |
| `POST /api/iacc/incidents/:id/skills/plan` | IACC 专用 skill plan |
| `POST /api/iacc/incidents/:id/skills/:skill_id/run` | IACC 专用 skill run |
| `GET /api/iacc/skill-runs/:id` | IACC skill run |

对外产品面推荐优先使用 `/api/skills/*` 的统一 action 协议。IACC 专用 API 保留为领域能力层和高级调试入口。

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

### 6.2 Prompt

```bash
~/AI/cowd prompt "解释当前项目"
~/AI/cowd "解释当前项目"
~/AI/cowd --output-format json prompt "列出模块"
~/AI/cowd --compact "只输出最终结论"
```

### 6.3 Session

```bash
~/AI/cowd --resume latest
~/AI/cowd --resume latest /status
~/AI/cowd export conversation.md
~/AI/cowd import-session old-session.jsonl
```

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
~/AI/cowd gateway run
~/AI/cowd gateway status
~/AI/cowd gateway stop
~/AI/cowd gateway restart
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
| Chat | 对话和任务执行 |
| Files | workspace 文件浏览 |
| Memory | 记忆条目和上下文 |
| Runtime | runtime activity |
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
- 真正的 IACC skill action 通过统一 Skills API 和 incident id 执行。
- local skills 保持 view/import/invoke 边界，不做状态 run。

---

## 8. WebUI 使用说明

启动 Gateway 后打开：

```text
http://127.0.0.1:8642/
```

WebUI 适合浏览器增强管理：

- session 浏览和搜索
- 聊天和 stream
- 文件浏览
- memory / context 可视化
- runtime timeline
- skills 管理
- IACC command center
- connector console
- cross-plane governance
- approvals
- settings

### 8.1 Skills 面板

入口：右侧 panel 中的 `Skills`。

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

IACC skill 执行流程：

1. 在 IACC 面板或 API 中创建 incident。
2. 在 Skills 面板输入 incident id。
3. 对目标 IACC skill 点 `Validate`。
4. 点 `Plan` 查看证据需求和 agent node plan。
5. 点 `Run` 生成 skill run。
6. 在 Runs 区域过滤、查看、展开详情。
7. 开启 `Watch` 观察后续运行记录。

### 8.2 IACC 面板

入口：右侧 panel 中的 `IACC`。

用途：

- IACC health
- command center
- cockpit report
- retry report
- incident / report 状态检查

### 8.3 Control Center

左下角控制中心入口用于：

- config
- providers
- approval
- history
- usage

---

## 9. IACC 核心逻辑

IACC 是 Cowd 的结构化运营智能层，用来把企业运营事实转为可追踪的 evidence、incident、analysis、action、report。

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
| skill | IACC skill manifest、plan、run |
| store | IACC SQLite store |

IACC store 默认位置：

```text
<workspace>/.cowd/iacc.sqlite
```

---

## 10. Skills 核心逻辑

Skills 是 Cowd 当前统一能力管理的核心面。

### 10.1 数据来源

```text
IACC skill pack
  + local SkillRegistry
    + .cowd/skills
    + .agents/skills
    + .codex/skills
    + ~/.cowd/skills
    + ~/.codex/skills
    + legacy /commands
```

### 10.2 三层协议

```text
Catalog: 发现和详情
Projection: 按 WebUI/TUI/CLI 投影能力
Action: validate / plan / run / runs / run detail
```

### 10.3 三端分工

| 入口 | 定位 | 能力 |
|---|---|---|
| CLI | 极简控制 | list、view、install、invoke |
| TUI | 控制台全集 | 展示全集、搜索、action availability、键盘操作 |
| WebUI | 管理全集 | catalog、projection、validate、plan、run、watch、runs、detail、filter |

---

## 11. Connector 和 Cross-plane

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

## 12. 配置

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

## 13. 开发和验证

### 13.1 Rust

```bash
cargo fmt --check
cargo test -p commands skill --no-default-features
cargo test -p cowd-cli skill --no-default-features -- --test-threads=1
cargo build -p cowd-cli --no-default-features
cargo build --release -p cowd-cli --no-default-features
```

### 13.2 WebUI

```bash
cd webui
npm ci
npm test
```

### 13.3 场景脚本

Skills 统一面验收：

```bash
COWD_BIN=/path/to/cowd scripts/v09136_skill_surface_unification_scenario.sh
```

安装版验收：

```bash
COWD_BIN=~/AI/cowd scripts/v09136_skill_surface_unification_scenario.sh
```

### 13.4 浏览器评测

WebUI E2E 使用 Playwright。若本地 Playwright 浏览器未安装，可指定系统 Chromium：

```js
chromium.launch({
  headless: true,
  executablePath: "/snap/bin/chromium",
  args: ["--no-sandbox"]
})
```

---

## 14. 安装和发布

当前推荐安装方式：

```bash
cargo build --release -p cowd-cli --no-default-features
rm -f ~/AI/cowd
install -m 0755 target/release/cowd ~/AI/cowd
rm -rf ~/AI/webui
mkdir -p ~/AI/webui
cp -a webui/. ~/AI/webui/
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

## 15. 当前能力状态

已完成：

- 三分支统一：`master`、`develop`、`dev-iacc`
- `~/AI/cowd` 安装
- `~/AI/webui` 安装
- Skills catalog/projection/action API
- WebUI Skills 闭环：validate/plan/run/watch/runs/detail/filter
- TUI Skills action availability
- CLI skills 极简化
- IACC skill pack 接入统一 Skills API

仍需后续增强：

- TUI 直接调用 Skills Action API 的完整交互流
- WebUI skill runs 的更强批量操作和对比视图
- candidate skills 的 promote/reject/archive
- local skills 的安全扫描和 manifest 校验报告
- IACC action 与 memory case/playbook 的更完整闭环展示

---

## 16. 常见问题

### WebUI 打开是 404

检查：

```bash
ls ~/AI/webui/index.html
~/AI/cowd gateway run
curl http://127.0.0.1:8642/readyz
```

`readyz` 中应看到 static WebUI source 指向 `installed:exe-dir/webui` 或当前工作区 `webui`。

### Skills Run 报 `incident_id is required`

`plan` 和 `run` 需要 IACC incident。先通过 IACC API 或 WebUI IACC 面板创建 incident，再在 Skills 面板输入 incident id。

### local skill 不能 Run

这是设计约束。local skill 由 CLI 管理导入、查看和 prompt dispatch，不承载 IACC 状态执行。

### `cargo clippy -D warnings` 报 memory 历史 lint

当前严格 clippy 会被 `cowd-memory` 既有 lint 阻断。针对 Skills 闭环的验证使用：

```bash
cargo fmt --check
cargo test -p cowd-cli skill --no-default-features -- --test-threads=1
cd webui && npm test
COWD_BIN=~/AI/cowd scripts/v09136_skill_surface_unification_scenario.sh
```

