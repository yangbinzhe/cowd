# Gateway Capability Contract 终态落地证据

日期：2026-07-04

## 目标对齐

本阶段把接口能力层固定在 Gateway 内部，形成同一份 Gateway-owned Capability Contract，并由它派生运行时能力合同、OpenAPI 3.1 JSON 和 OpenAI-compatible tool schema。

## 代码证据

| 目标 | 代码证据 | 状态 |
|---|---|---|
| route manifest 成为路由基线 | `crates/gateway/src/api_routes/route_manifest.rs` 导出 `method/path/group/owner/criticality/stability/source/handler` | 已完成 |
| route manifest 覆盖 resource routes 与新增 contract routes | `ROUTE_SOURCES` 纳入 `resource_routes.rs`，`core_routes.rs` 新增三条 gateway contract 路由 | 已完成 |
| Gateway 内部拥有能力合同 | `crates/gateway/src/api_routes/capability_contract.rs` 新增 `gateway_capability_contract()` | 已完成 |
| OpenAPI 由合同派生 | `gateway_openapi_document()` 从 `gateway_capability_contract()` 生成 `openapi: 3.1.0` 与 `paths` | 已完成 |
| OpenAPI 路径符合规范 | Axum `:id` / `*path` 在 OpenAPI projection 中转换为 `{id}` / `{path}` | 已完成 |
| OpenAI tools 由合同派生 | `gateway_openai_tools()` 从 contract capability 生成 function tools | 已完成 |
| OpenAI tools 是安全子集 | 排除 destructive/admin/static/SSE/upload/download；保留只读和明确允许的动作类接口 | 已完成 |
| OpenAI tools 不暴露 raw/binary HTTP 路由 | `/raw` 路由不进入 tool schema；需要模型读文件时应走受控 JSON tool | 已完成 |
| OpenAI tool 名称稳定唯一 | tool name 限制 64 字符，冲突时稳定 hash 后缀，不静默丢弃 capability | 已完成 |
| P1 cross-plane 治理链路对模型可见 | `cross_plane` capability 纳入 LLM visibility，但危险动作仍受 risk/cautions/tool filtering 控制 | 已完成 |
| HTTP 端点真实接线 | `core_routes.rs` 暴露 `/api/gateway/capability-contract`、`/api/gateway/openapi.json`、`/api/gateway/openai-tools` | 已完成 |
| 文档与运行时关系一致 | `scripts/generate_gateway_api_docs.py` 和 `docs/api/gateway-api-reference.md` 指向 Gateway Capability Contract 作为运行时真源 | 已完成 |

## 验证证据

```bash
cargo test -p gateway route_manifest -- --nocapture
```

结果：3 passed。

```bash
cargo test -p gateway capability_contract -- --nocapture
```

结果：4 passed，覆盖 contract parity、OpenAPI projection、OpenAI tool 安全子集、HTTP 端点。

```bash
cargo test -p gateway gateway_capability_contract_endpoints_are_available -- --nocapture
```

结果：1 passed，证明三个 Gateway HTTP 端点可访问。

```bash
cargo check -p gateway --all-targets
```

结果：通过。

```bash
cargo fmt --all -- --check
git diff --check
python3 scripts/generate_gateway_api_docs.py
git diff --exit-code -- docs/api/gateway-api-reference.md docs/architecture/gateway-api-framework.md
```

结果：格式、空白、文档生成幂等均通过。

## 终态判断

本阶段没有引入独立接口 crate，没有把接口合同放到 runtime/tools/surface。Gateway 是接口合同唯一 owner；route manifest 是路由存在性基线；capability contract 是运行时能力真源；OpenAPI 和 OpenAI tools 是派生输出。

## Surface 消费闭环

2026-07-04 追加完成 WebUI/TUI 消费闭环：

- WebUI 通过 `src/api/client.ts`、`src/stores/app.ts`、`CapabilitySidebar.vue`、`GatewayPage.vue` 消费 `/api/gateway/capability-contract`、`/api/gateway/openapi.json`、`/api/gateway/openai-tools`。
- TUI 通过 `GatewayApiClient`、`RuntimeControlSnapshot`、`App`、`GatewayPanel` 消费 Gateway contract 与 OpenAI tools。
- TUI `GatewayPanel` 删除旧手写 HTTP endpoint 清单，改为 contract-derived routes/tools 摘要。
- WebUI `pageEndpoints()` 仅保留为 contract 不可用时的 fallback，不再作为默认能力真源。

## 审计修复

只读审计发现两项有效问题，均已修复：

1. `/api/cross-plane/*` 被标为 P1，但 `cross_plane` domain 未纳入 LLM visibility。现已纳入，并由 `capability_contract_covers_every_route` 测试覆盖。
2. `/raw` binary HTTP 路由可能进入 OpenAI tools。现已从 `expose_as_tool` 中排除，并由 `openai_tools_are_safe_subset_with_function_schema` 测试覆盖。
