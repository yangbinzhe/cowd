# Gateway Capability Contract 终态实施方案

日期：2026-07-04

## 目标

在 Gateway 内建立统一的接口能力合同层，让 HTTP API、人类文档、OpenAPI/Swagger、OpenAI tool schema、WebUI 能力矩阵和测试门禁都从同一个 Gateway-owned contract 派生。

本阶段不引入独立 contract crate，不把接口层放到 runtime/tools/surface。接口层的 source of truth 放在 `crates/gateway/src/api_routes`，由 Gateway 负责导出。

## 第一性原理

Gateway 是 Cowd 的 HTTP 控制面和 surface/edge 接入中枢。它需要让三类使用方明确知道系统能力：

1. 人类和前端：知道有哪些接口、如何调用、风险和状态是什么。
2. 测试与审计：能验证接口存在、能力合同完整、路由和合同不漂移。
3. 大模型：能感知系统具备哪些高级能力，并用结构化 tool schema 主动选择调用。

因此 Swagger/OpenAPI 和 OpenAI schema 不是两个手写系统，而是同一份 Gateway Capability Contract 的派生输出。

## 当前代码事实

| 能力 | 当前文件 | 事实 | 目标 |
|---|---|---|---|
| 路由发现 | `crates/gateway/src/api_routes/route_manifest.rs` | 已从 `api_routes/**/*.rs` 的 `.route(...)` 声明生成 runtime route manifest；当前补入了 `resource_routes.rs`，并修正末尾普通 `.get(...)` 误识别风险 | 扩展 manifest，带上 `source` 与 `handler`，作为 contract 路由覆盖基线 |
| 路由出口 | `crates/gateway/src/api_routes/core_routes.rs` | 已有 `/api/gateway/route-manifest` | 新增 `/api/gateway/capability-contract`、`/api/gateway/openapi.json`、`/api/gateway/openai-tools` |
| 文档生成 | `scripts/generate_gateway_api_docs.py` | 已生成 Markdown 全量接口表和关系文档，但仍是 route source 解析输出 | 文档明确运行时真源是 Gateway Capability Contract；脚本与 Rust route manifest 使用同一套 P1 规则，生成结果必须包含 contract/openapi/openai-tools 三个端点 |
| 前端消费 | `cowd-edge/surfaces/webui/src/api/client.ts` | 当前前端仍直接维护接口调用清单 | 本阶段不改 WebUI 代码，但后端 contract 要为下一阶段 WebUI capability matrix 消费做好接口 |
| OpenAPI | 无依赖 | 未引入 `utoipa/aide/schemars/swagger-ui` | 本阶段不引入重依赖，先由 Gateway 生成轻量 OpenAPI 3.1 JSON；Swagger UI 可后续以静态 surface 消费 `/api/gateway/openapi.json` |
| OpenAI schema | 无统一生成 | 模型不能从 Gateway 统一感知 API 能力 | 由 Gateway contract 生成 OpenAI-compatible tools schema，避免手写漂移 |

## 终态架构

```text
crates/gateway/src/api_routes
  ├── route_manifest.rs          # HTTP route baseline: method/path/group/source/handler
  ├── capability_contract.rs     # Gateway-owned capability contract + derived outputs
  └── core_routes.rs             # /api/gateway/* exports

Gateway Capability Contract
  ├── /api/gateway/route-manifest        # route existence and release gate
  ├── /api/gateway/capability-contract   # full contract for humans/frontends/tests/AI
  ├── /api/gateway/openapi.json          # OpenAPI 3.1 derived output
  └── /api/gateway/openai-tools          # OpenAI tool schema derived output
```

## 合同字段

每个 capability 必须包含：

| 字段 | 用途 |
|---|---|
| `id` | 稳定能力 ID，使用 domain + method + path 派生 |
| `domain` | runtime/session/memory/tools/surface 等领域 |
| `title` / `description` | 人类和 AI 可读说明 |
| `http.method` / `http.path` / `http.handler` / `http.source` | HTTP 入口与源码证据 |
| `input_schema` | JSON Schema 风格输入说明；对未知接口给出保守 object schema |
| `output_schema` | JSON Schema 风格响应说明；对未知接口给出保守 object schema |
| `auth` | public/bearer |
| `risk` | read/write/destructive/external/admin |
| `side_effects` | 是否改状态、调用外部、写文件、发消息 |
| `idempotency` | safe/idempotent/non_idempotent/unknown |
| `streaming` | none/sse/static |
| `surface_visibility` | webui/tui/llm/edge 是否可见 |
| `ai_affordance` | 模型何时应使用、注意事项、是否建议暴露为 tool |
| `tests` | 对应路由/合同测试线索 |

## 实施版本边界

### V1：Route Manifest 升级

目标：route manifest 成为能力合同的精确路由基线。

修改：

- `route_manifest.rs`
  - `GatewayRouteManifestEntry` 新增 `source`、`handler`。
  - `parse_routes` 返回 `(method, path, handler)`。
  - 单测验证：
    - resource routes 纳入清单。
    - handler/source 被导出。
    - 末尾普通 `.get(...)` 不误判。

验收：

```bash
cargo test -p gateway route_manifest -- --nocapture
```

### V2：Gateway Capability Contract

目标：在 Gateway 内建立完整 contract，不依赖外部文档或前端手写清单。

新增：

- `crates/gateway/src/api_routes/capability_contract.rs`

实现：

- `gateway_capability_contract()` 从 `gateway_route_manifest()` 生成所有路由 capability。
- 对所有路由生成保守 schema、风险、幂等、side effects、visibility。
- 对 P1 主干链路提供更具体的 override：
  - session/message/runtime/mission/tools/skills/context/memory/reality/surface/edge/connector/resource/approval。
- 生成 coverage：
  - `route_count`
  - `capability_count`
  - `p1_count`
  - `ai_visible_count`
  - `openapi_path_count`
  - `openai_tool_count`

验收：

- capability 数量必须等于 route manifest 唯一路由数。
- P1 主干链路必须有 LLM 可见 affordance。
- public 路由 auth 为 public，其余为 bearer。

### V3：Gateway 导出接口

目标：让合同成为运行时可发现能力。

修改：

- `core_routes.rs`
  - 新增：
    - `GET /api/gateway/capability-contract`
    - `GET /api/gateway/openapi.json`
    - `GET /api/gateway/openai-tools`

验收：

- 三个接口均被 `route_manifest` 识别。
- 测试通过：
  - contract endpoint 返回 route_count/capabilities。
  - OpenAPI endpoint 返回 `openapi: 3.1.0`、`paths`。
  - OpenAI tools endpoint 返回 `tools` 且所有 function 有 `name/description/parameters`。

### V4：文档生成对齐

目标：文档不再只是 Markdown 静态说明，而明确以 Gateway contract 为运行时真源；Markdown 只做人工阅读索引，OpenAPI/OpenAI schema 只由 Gateway contract 派生。

修改：

- `scripts/generate_gateway_api_docs.py`
  - 文案改为“Gateway 已提供派生 OpenAPI/OpenAI schema”，删除旧的待办式 OpenAPI 表述。
  - 总框架文档补充 contract 输出链路。
  - P1 criticality 规则与 `route_manifest.rs` 保持一致。
  - 生成文档必须包含 `/api/gateway/capability-contract`、`/api/gateway/openapi.json`、`/api/gateway/openai-tools`。
- `docs/api/gateway-api-reference.md`
- `docs/architecture/gateway-api-framework.md`
- `docs/README.md`
- `docs/architecture/gateway-api-inventory.md`

验收：

```bash
python3 scripts/generate_gateway_api_docs.py
python3 scripts/generate_gateway_api_docs.py
git diff --exit-code -- docs/api/gateway-api-reference.md docs/architecture/gateway-api-framework.md
```

### V5：终态审计

目标：确认不存在“只有文档没有接线”“只有 schema 没有路由”“route manifest 漏路由”的半成品。

验收：

```bash
rg "/api/gateway/capability-contract|/api/gateway/openapi.json|/api/gateway/openai-tools" crates/gateway/src docs scripts
cargo fmt --all -- --check
cargo test -p gateway route_manifest -- --nocapture
cargo test -p gateway capability_contract -- --nocapture
cargo test -p gateway gateway_capability_contract_endpoints_are_available -- --nocapture
cargo check -p gateway --all-targets
git diff --check
```

## 对抗性审查结论

| 风险 | 判断 | 处理 |
|---|---|---|
| 直接引入 Swagger 依赖导致重构过大 | 不做 | 本阶段先生成 OpenAPI JSON，不引入 UI 依赖 |
| OpenAI schema 与 HTTP API 分裂 | 必须避免 | OpenAI tools 由同一 contract 派生 |
| 数百个接口全量手写 schema 不现实 | 不手写 | 全路由自动保守 schema + P1 主干精细 override |
| 前端还不能自动消费 contract | 可接受 | 后端接口先就绪，WebUI 下一阶段可消费 |
| 文档生成和运行时 contract 漂移 | 必须控制 | 文档说明 runtime contract；脚本生成幂等；route manifest 单测覆盖；P1 规则和 contract 端点扫描作为硬门禁 |
| Gateway 变成业务执行器 | 禁止 | contract 只描述和导出能力，不执行 runtime 循环 |
| OpenAPI path 使用 Axum `:id` 导致 schema 不标准 | 必须修复 | OpenAPI projection 将 `:id`/`*path` 转换为 `{id}`/`{path}`，manifest 保留 Axum 路径 |
| OpenAI tools 暴露危险或不可控接口 | 必须修复 | tools 只暴露安全或经设计允许的能力，排除 destructive/admin/static/SSE/upload/download，且 function name 唯一 |

## 完成定义

本阶段只有在以下条件全部满足时才算完成：

1. Gateway 内存在统一 `capability_contract` 模块。
2. 所有 `route_manifest` 路由都出现在 capability contract 中。
3. OpenAPI 和 OpenAI tools 都由 contract 生成。
4. 三个 Gateway 接口真实接线并通过测试。
5. 文档更新为终态关系，不再保留旧的 OpenAPI 缺失表述。
6. 格式、单测、生成幂等、cargo check 通过。
