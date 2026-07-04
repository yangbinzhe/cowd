# Gateway API 全量接口说明书

生成时间：2026-07-04

来源：`crates/gateway/src/api_routes/**/*.rs` 中实际 `axum::Router::route` 声明，并与 `crates/gateway/src/api_routes/route_manifest.rs` 的运行时清单方向保持一致。

当前共识别 `439` 个唯一 `method + path` 接口。

## Capability Contract / OpenAPI 状态

当前仓库未引入 `utoipa`、`aide`、`schemars`、`paperclip`、Swagger UI、Redoc 或 Scalar 这类 OpenAPI UI/注解框架。

Gateway 现在以 `/api/gateway/capability-contract` 作为运行时接口能力真源，并派生 `/api/gateway/openapi.json` 与 `/api/gateway/openai-tools`。`/api/gateway/route-manifest` 仍作为轻量路由存在性和发布门禁基线。

本文档由路由源码生成，运行时消费应优先读取 Gateway Capability Contract；如需 Swagger UI，可由静态 surface 直接消费 `/api/gateway/openapi.json`，不需要把接口 source of truth 移出 gateway。

## 通用约定

- 认证：除公共路由外，若 `auth_token` 已配置，则受保护接口要求 `Authorization: Bearer <token>`；WebUI 同源内部请求有特殊放行逻辑。
- 响应：查询类接口通常返回 JSON object；写接口成功返回 JSON receipt 或对象；错误通常返回 `{ "error": "..." }` 与对应 HTTP status。
- 路径参数：Axum 使用 `:id`、`:name`、`:surface` 等形式。
- Body：POST/PUT/PATCH 多数为 JSON；上传类接口使用 multipart。
- 稳定性：`/api/**` 视为稳定 HTTP API；`/s/:surface/*path` 属于 surface 静态资源转发。

## 分组目录

- [公共入口与认证](#公共入口与认证)：7 个接口
- [Cowd 核心投影与发布门禁](#cowd-核心投影与发布门禁)：14 个接口
- [Runtime 执行核心](#runtime-执行核心)：20 个接口
- [Session 生命周期](#session-生命周期)：18 个接口
- [对话消息与 SSE](#对话消息与-sse)：3 个接口
- [Mission Control / 多 Session 多 Agent 协同](#mission-control-/-多-session-多-agent-协同)：50 个接口
- [Agent 目录、组队与运行](#agent-目录、组队与运行)：20 个接口
- [Task 阶段化执行](#task-阶段化执行)：8 个接口
- [Context / Evidence](#context-/-evidence)：6 个接口
- [Memory / Knowledge](#memory-/-knowledge)：25 个接口
- [Reality Core](#reality-core)：10 个接口
- [Matrix 结构化事实](#matrix-结构化事实)：43 个接口
- [Growth / 自我演进](#growth-/-自我演进)：2 个接口
- [Tools 工具执行](#tools-工具执行)：16 个接口
- [Skills 技能体系](#skills-技能体系)：12 个接口
- [Approval 审批](#approval-审批)：7 个接口
- [Cross Plane 权限与动作](#cross-plane-权限与动作)：14 个接口
- [Surface 接入面](#surface-接入面)：24 个接口
- [Edge 热加载与外部包](#edge-热加载与外部包)：7 个接口
- [Channel / Platform](#channel-/-platform)：8 个接口
- [Connector 数据与服务连接](#connector-数据与服务连接)：11 个接口
- [Resource 附件资源](#resource-附件资源)：3 个接口
- [Workspace 文件工作区](#workspace-文件工作区)：14 个接口
- [Profile 配置画像](#profile-配置画像)：4 个接口
- [MFG 上层应用](#mfg-上层应用)：79 个接口
- [Harness Eval 评测](#harness-eval-评测)：8 个接口
- [Audit 审计](#audit-审计)：1 个接口
- [Slash 命令](#slash-命令)：5 个接口

## 公共入口与认证

健康检查、WebUI manifest 和认证入口；公共路由不经过统一 Bearer 中间件。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `POST` | `/api/auth/login` | 公共入口与认证 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `login_handler` | `public_routes.rs` | P2 |
| `POST` | `/api/auth/logout` | 公共入口与认证 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `logout_handler` | `public_routes.rs` | P2 |
| `GET` | `/api/auth/verify` | 公共入口与认证 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `verify_handler` | `public_routes.rs` | P2 |
| `GET` | `/api/webui/manifest` | 公共入口与认证 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `webui_manifest_handler` | `public_routes.rs` | P2 |
| `GET` | `/health` | 公共入口与认证 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `health_handler` | `public_routes.rs` | P2 |
| `GET` | `/healthz` | 公共入口与认证 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `gateway_health_handler` | `public_routes.rs` | P2 |
| `GET` | `/readyz` | 公共入口与认证 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `gateway_ready_handler` | `public_routes.rs` | P2 |

## Cowd 核心投影与发布门禁

Gateway 对外暴露的全局能力图、结构化事实投影、发布门禁和路由清单。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/cowd/capabilities` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `capabilities_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/projection` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `projection_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/release-gate` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `release_gate_handler` | `core_routes.rs` | P1 |
| `GET` | `/api/cowd/structured/evidence` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `structured_evidence_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/structured/facts` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `structured_facts_handler` | `core_routes.rs` | P2 |
| `POST` | `/api/cowd/structured/ingest-plan` | Cowd 核心投影与发布门禁 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `structured_ingest_plan_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/structured/sources` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `structured_sources_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/structured/sources/:id` | Cowd 核心投影与发布门禁 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `structured_source_get_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/structured/watermarks` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `structured_watermarks_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/cowd/surfaces` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `surfaces_handler` | `core_routes.rs` | P1 |
| `GET` | `/api/gateway/capability-contract` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `capability_contract_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/gateway/openai-tools` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `openai_tools_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/gateway/openapi.json` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `openapi_handler` | `core_routes.rs` | P2 |
| `GET` | `/api/gateway/route-manifest` | Cowd 核心投影与发布门禁 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `route_manifest_handler` | `core_routes.rs` | P2 |

## Runtime 执行核心

AI Harness 的运行状态、事件、控制平面、配置热加载、turn 提交和 session lease。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/runtime/config/effective` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_effective_config` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/config/reload` | 手动触发 Gateway/Runtime 配置重载 | - | - | JSON 或 Multipart，详见对应 Request struct | `reload_runtime_config` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/config/reload/status` | 查看配置热加载状态、错误、是否需要重启与最近一次应用结果 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_config_reload_status` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/control-plane` | 获取 Runtime 控制平面总览，供前端判断核心能力健康状态 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_control_plane` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/events` | Runtime 执行核心 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `get_runtime_events` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/events/recover` | Runtime 执行核心 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `recover_runtime_events` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/events/replay-report` | Runtime 执行核心 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `get_runtime_events_replay_report` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/providers/reload` | Runtime 执行核心 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `reload_runtime_providers` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/session-leases` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_session_leases` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/session-leases/acquire` | Runtime 执行核心 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `acquire_runtime_session_lease` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/session-leases/release` | Runtime 执行核心 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `release_runtime_session_lease` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/snapshot` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_snapshot` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/source-audit` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_source_audit` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/source-repair-plan` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_source_repair_plan` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/status` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_status` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/timeline` | 按 session 查询 Runtime 事件时间线 | - | 支持 Query 参数，详见 handler Params struct | - | `get_runtime_timeline` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/turns` | 查询或提交 Runtime turn | - | 可选 Query 视具体 handler 而定 | - | `get_runtime_turns` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/turns` | 查询或提交 Runtime turn | - | - | JSON 或 Multipart，详见对应 Request struct | `submit_runtime_turn` | `runtime_routes.rs` | P1 |
| `GET` | `/api/runtime/turns/:id` | Runtime 执行核心 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_runtime_turn` | `runtime_routes.rs` | P1 |
| `POST` | `/api/runtime/turns/:id/cancel` | Runtime 执行核心 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `cancel_runtime_turn` | `runtime_routes.rs` | P1 |

## Session 生命周期

持久会话、分叉、压缩、统计、事件、运行投影和 session 级管理。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/sessions` | Session 生命周期 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `list_sessions` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions` | Session 生命周期 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `create_session` | `session_routes.rs` | P1 |
| `DELETE` | `/api/sessions/:id` | Session 生命周期 删除接口 | id | - | 通常无 body 或仅 path/query | `delete_session` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id` | Session 生命周期 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_session` | `session_routes.rs` | P1 |
| `PATCH` | `/api/sessions/:id` | Session 生命周期 局部更新接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `update_session_handler` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/attach` | Session 生命周期 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `attach_session_handler` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/branch` | Session 生命周期 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `branch_session_handler` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/cancel` | Session 生命周期 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `cancel_session_turn_handler` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/compact` | Session 生命周期 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `compact_session_handler` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/detach` | Session 生命周期 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `detach_session_handler` | `session_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/ensure` | Session 生命周期 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `ensure_session_handler` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/events` | Session 生命周期 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `get_session_events` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/lifecycle` | Session 生命周期 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `session_lifecycle_handler` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/projection` | Session 生命周期 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_session_projection` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/replay` | Session 生命周期 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `replay_session_handler` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/runs` | 读取 session 关联 runtime run 和证据树 | id | 支持 Query 参数，详见 handler Params struct | - | `get_session_runs` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/stats` | 读取 session token、耗时和运行统计 | id | 支持 Query 参数，详见 handler Params struct | - | `get_session_stats_handler` | `session_routes.rs` | P1 |
| `GET` | `/api/sessions/search` | Session 生命周期 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `search_messages_handler` | `session_routes.rs` | P1 |

## 对话消息与 SSE

用户消息写入、历史消息读取和会话 SSE 流。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/sessions/:id/messages` | 读取或发送指定 session 的对话消息 | id | 可选 Query 视具体 handler 而定 | - | `get_session_messages` | `message_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/messages` | 读取或发送指定 session 的对话消息 | id | - | JSON 或 Multipart，详见对应 Request struct | `send_message` | `message_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/stream` | 订阅指定 session 的 SSE 实时输出 | id | 可选 Query 视具体 handler 而定 | - | `sse_stream_handler` | `message_routes.rs` | P1 |

## Mission Control / 多 Session 多 Agent 协同

Mission Runtime 的全局控制、跨 session 命令、team runtime、steward、审批和代理关系。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/mission/approvals` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mission_approvals_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/approvals` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `submit_mission_approval_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/approvals/:id/decision` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `decide_mission_approval_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control` | 读取或写入 Mission Control 全局控制状态 | - | 可选 Query 视具体 handler 而定 | - | `mission_control_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control` | 读取或写入 Mission Control 全局控制状态 | - | - | JSON 或 Multipart，详见对应 Request struct | `execute_mission_control_command_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/agents/:agent_id/events` | Mission Runtime 协同 查询接口 | agent_id | 支持 Query 参数，详见 handler Params struct | - | `agent_mission_events_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/command` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `execute_mission_control_command_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/sessions/bridge` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `bridge_mission_session_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/sessions/dispatch` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `dispatch_mission_sessions_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/stewards/:id/handoff` | Mission Runtime 协同 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mission_steward_scheduler_handoff_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/stewards/scheduler` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mission_steward_scheduler_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/stewards/scheduler` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tick_mission_steward_scheduler_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/teams` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `collaboration_runs_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/teams/:team_id/cancel` | Mission Runtime 协同 创建/动作接口 | team_id | - | JSON 或 Multipart，详见对应 Request struct | `cancel_team_runtime_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/teams/:team_id/evidence` | Mission Runtime 协同 查询接口 | team_id | 可选 Query 视具体 handler 而定 | - | `team_mission_evidence_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/teams/:team_id/execution` | Mission Runtime 协同 查询接口 | team_id | 可选 Query 视具体 handler 而定 | - | `team_execution_plan_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/teams/:team_id/execution` | Mission Runtime 协同 创建/动作接口 | team_id | - | JSON 或 Multipart，详见对应 Request struct | `tick_team_execution_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/teams/:team_id/handoff` | Mission Runtime 协同 创建/动作接口 | team_id | - | JSON 或 Multipart，详见对应 Request struct | `handoff_team_runtime_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/control/teams/:team_id/run` | Mission Runtime 协同 查询接口 | team_id | 可选 Query 视具体 handler 而定 | - | `collaboration_run_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/control/teams/:team_id/synthesis` | Mission Runtime 协同 创建/动作接口 | team_id | - | JSON 或 Multipart，详见对应 Request struct | `synthesize_team_runtime_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/projection` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mission_projection_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/proxies` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `upsert_mission_proxy_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/relations` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mission_relations_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/relations` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `add_mission_relation_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/route` | 将跨 session/agent 命令路由到目标 | - | - | JSON 或 Multipart，详见对应 Request struct | `route_mission_command_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/sessions` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mission_projection_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `start_mission_session_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/sessions/:id` | Mission Runtime 协同 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mission_session_detail_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/agents` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `attach_mission_agent_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/background` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `background_mission_session_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/close` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `close_mission_session_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/sessions/:id/inbox` | Mission Runtime 协同 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mission_session_inbox_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/sessions/:id/inbox/:command_id` | Mission Runtime 协同 查询接口 | id, command_id | 可选 Query 视具体 handler 而定 | - | `mission_session_command_detail_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/inbox/:command_id/cancel` | Mission Runtime 协同 创建/动作接口 | id, command_id | - | JSON 或 Multipart，详见对应 Request struct | `cancel_mission_session_command_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/inbox/:command_id/consume` | Mission Runtime 协同 创建/动作接口 | id, command_id | - | JSON 或 Multipart，详见对应 Request struct | `consume_mission_session_command_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/inbox/:command_id/retry` | Mission Runtime 协同 创建/动作接口 | id, command_id | - | JSON 或 Multipart，详见对应 Request struct | `retry_mission_session_command_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/pause` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `pause_mission_session_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/switch` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `switch_mission_session_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/teams` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `attach_mission_team_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/sessions/:id/teams/runtime` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `start_mission_team_runtime_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/stewards` | Mission Runtime 协同 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mission_stewards_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `start_mission_steward_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/stewards/:id` | Mission Runtime 协同 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mission_steward_detail_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards/:id/interrupt` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `interrupt_mission_steward_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards/:id/pause` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `pause_mission_steward_handler` | `mission_routes.rs` | P1 |
| `GET` | `/api/mission/stewards/:id/report` | Mission Runtime 协同 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mission_steward_report_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards/:id/resume` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `resume_mission_steward_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards/:id/takeover` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `takeover_mission_steward_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards/:id/tick` | Mission Runtime 协同 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `tick_mission_steward_handler` | `mission_routes.rs` | P1 |
| `POST` | `/api/mission/stewards/tick-all` | Mission Runtime 协同 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tick_all_mission_stewards_handler` | `mission_routes.rs` | P1 |

## Agent 目录、组队与运行

Agent catalog、team profile、自动发现、组队、信誉和 runtime agent 视图。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `POST` | `/api/agents/assemble` | Agent 目录、组队与运行 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `agent_assemble_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/catalog` | Agent 目录、组队与运行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `agent_catalog_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/directory` | Agent 目录、组队与运行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `agent_directory_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/discover` | Agent 目录、组队与运行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `agent_discover_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/reputation` | Agent 目录、组队与运行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `agent_reputation_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/runs` | Agent 目录、组队与运行 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `agent_runs_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/team-profiles` | Agent 目录、组队与运行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `agent_team_profiles_list_handler` | `agent_routes.rs` | P2 |
| `POST` | `/api/agents/team-profiles` | Agent 目录、组队与运行 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `agent_team_profile_create_handler` | `agent_routes.rs` | P2 |
| `DELETE` | `/api/agents/team-profiles/:id` | Agent 目录、组队与运行 删除接口 | id | - | 通常无 body 或仅 path/query | `agent_team_profile_delete_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/agents/team-profiles/:id` | Agent 目录、组队与运行 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `agent_team_profile_detail_handler` | `agent_routes.rs` | P2 |
| `PUT` | `/api/agents/team-profiles/:id` | Agent 目录、组队与运行 全量更新接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `agent_team_profile_update_handler` | `agent_routes.rs` | P2 |
| `GET` | `/api/runtime/agents` | Runtime 执行核心 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `runtime_agents_list_handler` | `agent_routes.rs` | P1 |
| `GET` | `/api/runtime/agents/:id` | Runtime 执行核心 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `runtime_agent_detail_handler` | `agent_routes.rs` | P1 |
| `POST` | `/api/runtime/agents/:id/cancel` | Runtime 执行核心 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `runtime_agent_cancel_handler` | `agent_routes.rs` | P1 |
| `GET` | `/api/runtime/agents/:id/events` | Runtime 执行核心 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `runtime_agent_events_handler` | `agent_routes.rs` | P1 |
| `POST` | `/api/runtime/agents/:id/input` | Runtime 执行核心 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `runtime_agent_input_handler` | `agent_routes.rs` | P1 |
| `POST` | `/api/runtime/agents/:id/interrupt` | Runtime 执行核心 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `runtime_agent_interrupt_handler` | `agent_routes.rs` | P1 |
| `POST` | `/api/runtime/agents/:id/shutdown` | Runtime 执行核心 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `runtime_agent_shutdown_handler` | `agent_routes.rs` | P1 |
| `GET` | `/api/tasks/:id/agent-graph` | Agent 目录、组队与运行 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `task_agent_graph_handler` | `agent_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/agent-graph` | Agent 目录、组队与运行 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `upsert_task_agent_graph_handler` | `agent_routes.rs` | P2 |

## Task 阶段化执行

任务启动、阶段、产物、审查、失败、完成和取消。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/tasks` | Task 阶段化执行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `tasks_status_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/cancel` | Task 阶段化执行 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `cancel_task_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/complete` | Task 阶段化执行 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `complete_task_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/failure` | Task 阶段化执行 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `record_task_failure_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/phases` | Task 阶段化执行 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `start_task_phase_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/phases/:phase_id/artifacts` | Task 阶段化执行 创建/动作接口 | id, phase_id | - | JSON 或 Multipart，详见对应 Request struct | `record_task_phase_artifact_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/:id/phases/:phase_id/review` | Task 阶段化执行 创建/动作接口 | id, phase_id | - | JSON 或 Multipart，详见对应 Request struct | `review_task_phase_handler` | `task_routes.rs` | P2 |
| `POST` | `/api/tasks/start` | Task 阶段化执行 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `start_task_handler` | `task_routes.rs` | P2 |

## Context / Evidence

当前上下文、上下文历史、推荐、压缩和证据引用解析。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/context/:envelope_id` | Context / Evidence 查询接口 | envelope_id | 可选 Query 视具体 handler 而定 | - | `get_context_envelope_handler` | `context_routes.rs` | P1 |
| `GET` | `/api/context/current` | 构建当前上下文 envelope | - | 支持 Query 参数，详见 handler Params struct | - | `context_current_handler` | `context_routes.rs` | P1 |
| `GET` | `/api/evidence/resolve` | 解析 evidence ref 到可展示证据 | - | 可选 Query 视具体 handler 而定 | - | `resolve_evidence_ref_handler` | `context_routes.rs` | P2 |
| `GET` | `/api/sessions/:id/context` | Context / Evidence 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_session_context_history` | `context_routes.rs` | P1 |
| `GET` | `/api/sessions/:id/context/recommendations` | Context / Evidence 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_context_recommendation_stats` | `context_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/context/recommendations` | Context / Evidence 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `record_context_recommendation_action` | `context_routes.rs` | P1 |

## Memory / Knowledge

记忆层、检索、packet、维护候选、实体、三元组、知识与性能状态。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/memory` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/:layer` | Memory/Knowledge 查询接口 | layer | 可选 Query 视具体 handler 而定 | - | `memory_layer_handler` | `memory_routes.rs` | P1 |
| `POST` | `/api/memory/:layer` | Memory/Knowledge 创建/动作接口 | layer | - | JSON 或 Multipart，详见对应 Request struct | `create_memory_entry_handler` | `memory_routes.rs` | P1 |
| `DELETE` | `/api/memory/:layer/:id` | Memory/Knowledge 删除接口 | layer, id | - | 通常无 body 或仅 path/query | `delete_memory_entry_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/clusters` | Memory/Knowledge 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `memory_clusters_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/entities` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_entities_handler` | `memory_routes.rs` | P1 |
| `PATCH` | `/api/memory/entry/:id` | Memory/Knowledge 局部更新接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `update_memory_entry_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/knowledge` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_knowledge_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/knowledge/health` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_knowledge_health_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/layers` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_layers_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/lifecycle/:id` | Memory/Knowledge 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `memory_lifecycle_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/links` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_links_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/maintenance` | 读取或生成记忆治理候选 | - | 支持 Query 参数，详见 handler Params struct | - | `memory_maintenance_handler` | `memory_routes.rs` | P1 |
| `POST` | `/api/memory/maintenance` | 读取或生成记忆治理候选 | - | - | JSON 或 Multipart，详见对应 Request struct | `scan_memory_maintenance_handler` | `memory_routes.rs` | P1 |
| `PATCH` | `/api/memory/maintenance/:id` | Memory/Knowledge 局部更新接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `update_memory_maintenance_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/packet` | 构建面向上下文注入的记忆 packet | - | 支持 Query 参数，详见 handler Params struct | - | `memory_packet_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/performance` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `performance_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/recall/explain` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_recall_explain_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/runtime` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_runtime_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/search` | Memory/Knowledge 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `memory_search_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/stats` | Memory/Knowledge 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `memory_stats_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/status` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_status_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/symbol-links` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_symbol_links_handler` | `memory_routes.rs` | P1 |
| `POST` | `/api/memory/symbol-links` | Memory/Knowledge 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `create_memory_symbol_link_handler` | `memory_routes.rs` | P1 |
| `GET` | `/api/memory/triples` | Memory/Knowledge 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `memory_triples_handler` | `memory_routes.rs` | P1 |

## Reality Core

Reality Core 的静态地图、动态 fact flow、promotions、governance、boundaries 和 evidence。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/reality/boundaries` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_boundaries_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/capabilities` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_capabilities_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/context/envelope` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_context_envelope_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/evidence/:id` | Reality Core 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `reality_evidence_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/flow` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_flow_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/governance` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_governance_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/promotions` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_promotions_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/recall/report` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_recall_report_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/static` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_static_handler` | `reality_routes.rs` | P1 |
| `GET` | `/api/reality/status` | Reality Core 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `reality_status_handler` | `reality_routes.rs` | P1 |

## Matrix 结构化事实

结构化源包、实体、事实、指标、变化、证据包和连接器运行。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/matrix/attention/hot` | Matrix 结构化事实 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `matrix_attention_hot_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/changes` | Matrix 结构化事实 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `matrix_changes_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/compute/jobs/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_compute_job_get_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/compute/jobs/:id/run` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_compute_job_run_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/compute/jobs/plan` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_compute_job_plan_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/connector-runs/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_connector_run_get_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/data-plane/health` | Matrix 结构化事实 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `matrix_data_plane_health_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/data-plane/ingest-plan` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_data_plane_ingest_plan_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/entities` | Matrix 结构化事实 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `matrix_entities_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/entities/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_entity_get_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/entities/:id/impact-path` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_entity_impact_path_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/entities/:id/relations` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_entity_relations_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/entities/conflict-decision` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_entity_conflict_decision_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/entities/match-candidate` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_entity_match_candidate_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/entities/resolve-source-key` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_entity_resolve_source_key_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/entities/upsert` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_entity_upsert_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/evidence/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_evidence_get_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/evidence/:id/context` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_evidence_context_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/evidence/:id/quality-gate` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_evidence_quality_gate_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/evidence/build` | 基于结构化事实构建 evidence packet | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_evidence_build_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/facts/ingest` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_fact_ingest_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/health` | Matrix 结构化事实 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `matrix_health_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/metric-dependencies/affected-by-fact-type` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_metric_affected_by_fact_type_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/metric-dependencies/upsert` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_metric_dependency_upsert_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/metrics` | Matrix 结构化事实 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `matrix_metrics_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/metrics/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_metric_detail_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/metrics/:id/lineage` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_metric_lineage_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/metrics/attention-plan` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_metric_attention_plan_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/metrics/recompute` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_metric_recompute_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/metrics/snapshots/materialize` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_metric_snapshot_materialize_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/quality-gates/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_quality_gate_get_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/relations/upsert` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_relation_upsert_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/source-packs/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_source_pack_get_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/connector-runs/plan` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_pack_connector_run_plan_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/connector-runs/run` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_pack_connector_run_execute_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/delta-plan` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_pack_delta_plan_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/ingest-file` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_pack_ingest_file_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/source-packs/:id/snapshots` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_source_pack_snapshots_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/snapshots/plan` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_snapshot_plan_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/snapshots/run` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_snapshot_run_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/:id/validate` | Matrix 结构化事实 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_pack_validate_handler` | `matrix_routes.rs` | P1 |
| `POST` | `/api/matrix/source-packs/upsert` | Matrix 结构化事实 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `matrix_source_pack_upsert_handler` | `matrix_routes.rs` | P1 |
| `GET` | `/api/matrix/source-snapshots/:id` | Matrix 结构化事实 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `matrix_source_snapshot_get_handler` | `matrix_routes.rs` | P1 |

## Growth / 自我演进

成长状态、成长事件与演进线索。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/growth/events` | Growth / 自我演进 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `growth_events_handler` | `growth_routes.rs` | P2 |
| `GET` | `/api/growth/status` | Growth / 自我演进 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `growth_status_handler` | `growth_routes.rs` | P2 |

## Tools 工具执行

工具注册表、单工具调用、只读批量、mutation 预览/提交、幂等与意图规划。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/config` | Tools 工具执行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `config_handler` | `system_routes.rs` | P2 |
| `PUT` | `/api/config` | Tools 工具执行 全量更新接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `update_config_handler` | `system_routes.rs` | P2 |
| `GET` | `/api/config/providers` | Tools 工具执行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `config_providers_handler` | `system_routes.rs` | P2 |
| `GET` | `/api/tools` | 工具系统 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `tools_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/batch-readonly` | 批量并行执行幂等只读工具 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_batch_readonly_handler` | `system_routes.rs` | P1 |
| `GET` | `/api/tools/cache` | 工具系统 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `tool_cache_handler` | `system_routes.rs` | P1 |
| `GET` | `/api/tools/checkpoints` | 工具系统 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `tool_checkpoints_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/checkpoints` | 工具系统 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_checkpoint_create_handler` | `system_routes.rs` | P1 |
| `GET` | `/api/tools/checkpoints/:id/diff` | 工具系统 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `tool_checkpoint_diff_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/checkpoints/:id/restore` | 工具系统 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `tool_checkpoint_restore_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/context-fanout/plan` | 工具系统 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_context_fanout_plan_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/execute` | 执行单个工具调用 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_execute_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/intent-plan` | 工具系统 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_intent_plan_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/mutations/apply` | 工具系统 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_mutation_apply_handler` | `system_routes.rs` | P1 |
| `POST` | `/api/tools/mutations/preview` | 工具系统 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `tool_mutation_preview_handler` | `system_routes.rs` | P1 |
| `GET` | `/api/usage` | Tools 工具执行 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `usage_handler` | `system_routes.rs` | P2 |

## Skills 技能体系

技能目录、投影、详情、文件、翻译、运行记录和 validate/plan/run 动作。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/skills/:id` | 技能系统 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `skill_get_handler` | `skill_routes.rs` | P2 |
| `POST` | `/api/skills/:id/actions/plan` | 技能系统 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `skill_action_plan_handler` | `skill_routes.rs` | P1 |
| `POST` | `/api/skills/:id/actions/run` | 技能系统 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `skill_action_run_handler` | `skill_routes.rs` | P1 |
| `POST` | `/api/skills/:id/actions/validate` | 技能系统 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `skill_action_validate_handler` | `skill_routes.rs` | P1 |
| `GET` | `/api/skills/:id/files` | 技能系统 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `skill_files_handler` | `skill_routes.rs` | P2 |
| `GET` | `/api/skills/:id/files/raw` | 技能系统 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `skill_file_raw_handler` | `skill_routes.rs` | P2 |
| `POST` | `/api/skills/:id/translate` | 技能系统 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `skill_translate_handler` | `skill_routes.rs` | P2 |
| `GET` | `/api/skills/catalog` | 技能系统 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `skills_catalog_handler` | `skill_routes.rs` | P2 |
| `POST` | `/api/skills/maintenance/evaluate` | 技能系统 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `skill_maintenance_evaluate_handler` | `skill_routes.rs` | P2 |
| `GET` | `/api/skills/projection` | 技能系统 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `skills_projection_handler` | `skill_routes.rs` | P2 |
| `GET` | `/api/skills/runs` | 技能系统 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `skill_runs_handler` | `skill_routes.rs` | P2 |
| `GET` | `/api/skills/runs/:id` | 技能系统 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `skill_run_detail_handler` | `skill_routes.rs` | P2 |

## Approval 审批

待审批、审批响应、风险收据、solo 策略、审批配置与历史。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/approval/config` | Approval 审批 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `approval_config_handler` | `approval_routes.rs` | P1 |
| `PUT` | `/api/approval/config` | Approval 审批 全量更新接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `update_approval_config_handler` | `approval_routes.rs` | P1 |
| `GET` | `/api/approval/history` | Approval 审批 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `approval_history_handler` | `approval_routes.rs` | P1 |
| `GET` | `/api/approval/pending` | 读取待审批请求 | - | 可选 Query 视具体 handler 而定 | - | `approval_pending_handler` | `approval_routes.rs` | P1 |
| `POST` | `/api/approval/respond` | 提交审批决策 | - | - | JSON 或 Multipart，详见对应 Request struct | `approval_respond_handler` | `approval_routes.rs` | P1 |
| `POST` | `/api/approval/risk-receipt` | Approval 审批 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `risk_receipt_handler` | `approval_routes.rs` | P1 |
| `POST` | `/api/approval/solo` | Approval 审批 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `toggle_solo_handler` | `approval_routes.rs` | P1 |

## Cross Plane 权限与动作

跨 plane 能力授权、风险评估、动作执行、幂等与审计。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/cross-plane/action/adapters` | Cross Plane 权限与动作 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `cross_plane_action_adapters_handler` | `cross_plane_routes.rs` | P1 |
| `POST` | `/api/cross-plane/action/execute` | Cross Plane 权限与动作 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `cross_plane_action_execute_handler` | `cross_plane_routes.rs` | P1 |
| `GET` | `/api/cross-plane/action/executions` | Cross Plane 权限与动作 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `cross_plane_action_executions_handler` | `cross_plane_routes.rs` | P1 |
| `POST` | `/api/cross-plane/action/preflight` | Cross Plane 权限与动作 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `cross_plane_action_preflight_handler` | `cross_plane_routes.rs` | P1 |
| `GET` | `/api/cross-plane/audit` | Cross Plane 权限与动作 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `cross_plane_audit_handler` | `cross_plane_routes.rs` | P1 |
| `GET` | `/api/cross-plane/grants` | Cross Plane 权限与动作 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `cross_plane_grants_handler` | `cross_plane_routes.rs` | P1 |
| `POST` | `/api/cross-plane/grants` | Cross Plane 权限与动作 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `cross_plane_create_grant_handler` | `cross_plane_routes.rs` | P1 |
| `DELETE` | `/api/cross-plane/grants/:id` | Cross Plane 权限与动作 删除接口 | id | - | 通常无 body 或仅 path/query | `cross_plane_revoke_grant_handler` | `cross_plane_routes.rs` | P1 |
| `GET` | `/api/cross-plane/identities` | Cross Plane 权限与动作 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `cross_plane_identities_handler` | `cross_plane_routes.rs` | P1 |
| `POST` | `/api/cross-plane/identities` | Cross Plane 权限与动作 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `cross_plane_create_identity_handler` | `cross_plane_routes.rs` | P1 |
| `DELETE` | `/api/cross-plane/identities/:id` | Cross Plane 权限与动作 删除接口 | id | - | 通常无 body 或仅 path/query | `cross_plane_revoke_identity_handler` | `cross_plane_routes.rs` | P1 |
| `POST` | `/api/cross-plane/identity/resolve` | Cross Plane 权限与动作 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `cross_plane_identity_resolve_handler` | `cross_plane_routes.rs` | P1 |
| `POST` | `/api/cross-plane/policy/simulate` | Cross Plane 权限与动作 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `cross_plane_policy_simulate_handler` | `cross_plane_routes.rs` | P1 |
| `GET` | `/api/cross-plane/summary` | Cross Plane 权限与动作 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `cross_plane_summary_handler` | `cross_plane_routes.rs` | P1 |

## Surface 接入面

Surface 注册表、健康、路由、资源、状态、事件、启动/停止/修复和消息投递。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/surfaces` | Surface 接入面 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `list_surfaces_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/action` | Surface 接入面 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `action_surface_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/deliveries` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_deliveries_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/events` | Surface 接入面 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `get_surface_events_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/health` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_health_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/health-check` | Surface 接入面 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `post_surface_health_check_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/inbox` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_inbox_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/inbox/:message_id/replay` | Surface 接入面 创建/动作接口 | id, message_id | - | JSON 或 Multipart，详见对应 Request struct | `replay_surface_inbox_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/outbox` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_outbox_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/outbox/:delivery_id/dead-letter` | Surface 接入面 创建/动作接口 | id, delivery_id | - | JSON 或 Multipart，详见对应 Request struct | `dead_letter_surface_outbox_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/outbox/:delivery_id/retry` | Surface 接入面 创建/动作接口 | id, delivery_id | - | JSON 或 Multipart，详见对应 Request struct | `retry_surface_outbox_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/repair` | 对 surface 执行人工修复动作 | id | - | JSON 或 Multipart，详见对应 Request struct | `repair_surface_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/resources` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_resources_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/restart` | 重启 managed surface | id | - | JSON 或 Multipart，详见对应 Request struct | `restart_surface_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/routes` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_routes_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/send` | Surface 接入面 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `send_surface_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/start` | 启动 managed surface | id | - | JSON 或 Multipart，详见对应 Request struct | `start_surface_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/:id/status` | Surface 接入面 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_surface_status_handler` | `surface_routes.rs` | P1 |
| `POST` | `/api/surfaces/:id/stop` | 停止 managed surface | id | - | JSON 或 Multipart，详见对应 Request struct | `stop_surface_handler` | `surface_routes.rs` | P1 |
| `GET` | `/api/surfaces/health` | Surface 接入面 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `surface_health_handler` | `surface_routes.rs` | P1 |
| `GET` | `/s/:surface/*path` | Surface 接入面 查询接口 | surface | 可选 Query 视具体 handler 而定 | - | `surface_static_handler` | `surface_routes.rs` | P2 |
| `GET` | `/surface-callback/:surface/*path` | Surface 接入面 查询接口 | surface | 可选 Query 视具体 handler 而定 | - | `surface_callback_handler` | `surface_routes.rs` | P2 |
| `POST` | `/surface-callback/:surface/*path` | Surface 接入面 创建/动作接口 | surface | - | JSON 或 Multipart，详见对应 Request struct | `surface_callback_handler` | `surface_routes.rs` | P2 |

## Edge 热加载与外部包

Edge 包发现、健康、热加载、surface/connector/resource 投影。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/edges` | Edge 热加载与外部包 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `edge_registry_handler` | `edge_routes.rs` | P2 |
| `GET` | `/api/edges/connectors` | Edge 热加载与外部包 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `edge_connectors_handler` | `edge_routes.rs` | P2 |
| `GET` | `/api/edges/connectors/message` | Edge 热加载与外部包 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `edge_message_connectors_handler` | `edge_routes.rs` | P2 |
| `GET` | `/api/edges/connectors/source` | Edge 热加载与外部包 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `edge_source_connectors_handler` | `edge_routes.rs` | P2 |
| `GET` | `/api/edges/health` | Edge 热加载与外部包 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `edge_health_handler` | `edge_routes.rs` | P2 |
| `POST` | `/api/edges/reload` | 重新发现 Edge 包和 connector/surface 资源 | - | - | JSON 或 Multipart，详见对应 Request struct | `edge_reload_handler` | `edge_routes.rs` | P2 |
| `GET` | `/api/edges/surfaces` | Edge 热加载与外部包 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `edge_surfaces_handler` | `edge_routes.rs` | P1 |

## Channel / Platform

平台、消息 channel、Feishu/WeChat 等渠道状态与治理。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/channels` | Channel / Platform 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `list_channels_handler` | `channel_routes.rs` | P2 |
| `POST` | `/api/channels/:name/repair` | Channel / Platform 创建/动作接口 | name | - | JSON 或 Multipart，详见对应 Request struct | `repair_channel_handler` | `channel_routes.rs` | P2 |
| `GET` | `/api/channels/:name/status` | Channel / Platform 查询接口 | name | 可选 Query 视具体 handler 而定 | - | `get_channel_status_handler` | `channel_routes.rs` | P2 |
| `GET` | `/api/channels/wechat-ilink/accounts` | Channel / Platform 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `wechat_ilink_accounts_handler` | `channel_routes.rs` | P2 |
| `POST` | `/api/channels/wechat-ilink/qr` | Channel / Platform 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `wechat_ilink_qr_start_handler` | `channel_routes.rs` | P2 |
| `POST` | `/api/channels/wechat-ilink/qr/poll` | Channel / Platform 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `wechat_ilink_qr_poll_handler` | `channel_routes.rs` | P2 |
| `GET` | `/api/platforms` | Channel / Platform 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `list_platforms_handler` | `channel_routes.rs` | P2 |
| `GET` | `/api/platforms/:name` | Channel / Platform 查询接口 | name | 可选 Query 视具体 handler 而定 | - | `get_platform_handler` | `channel_routes.rs` | P2 |

## Connector 数据与服务连接

外部资源连接、账号、capability、MCP、source/service/tool connector。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/connectors/accounts` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `connector_accounts_handler` | `connector_routes.rs` | P2 |
| `GET` | `/api/connectors/capabilities` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `connector_capabilities_handler` | `connector_routes.rs` | P2 |
| `GET` | `/api/connectors/mcp/servers` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mcp_servers_handler` | `connector_routes.rs` | P2 |
| `GET` | `/api/connectors/resources` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `connector_resources_handler` | `connector_routes.rs` | P1 |
| `POST` | `/api/connectors/resources/promote-memory` | Connector 外部连接 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `connector_resource_promote_memory_handler` | `connector_routes.rs` | P1 |
| `POST` | `/api/connectors/resources/revalidate` | Connector 外部连接 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `connector_resource_revalidate_handler` | `connector_routes.rs` | P1 |
| `GET` | `/api/connectors/services` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `connector_services_handler` | `connector_routes.rs` | P2 |
| `POST` | `/api/connectors/services/:service_id/execute` | Connector 外部连接 创建/动作接口 | service_id | - | JSON 或 Multipart，详见对应 Request struct | `connector_service_execute_handler` | `connector_routes.rs` | P2 |
| `GET` | `/api/connectors/services/:service_id/tools` | Connector 外部连接 查询接口 | service_id | 可选 Query 视具体 handler 而定 | - | `connector_service_tools_handler` | `connector_routes.rs` | P1 |
| `GET` | `/api/connectors/sources` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `connector_sources_handler` | `connector_routes.rs` | P2 |
| `GET` | `/api/connectors/summary` | Connector 外部连接 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `connector_summary_handler` | `connector_routes.rs` | P2 |

## Resource 附件资源

上传资源注册、资源详情和资源 evidence。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `POST` | `/api/resources` | 上传附件资源并注册到 runtime resource store | - | - | JSON 或 Multipart，详见对应 Request struct | `upload_resource_handler` | `resource_routes.rs` | P1 |
| `GET` | `/api/resources/:id` | 附件资源 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_resource_handler` | `resource_routes.rs` | P1 |
| `GET` | `/api/resources/:id/evidence` | 附件资源 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `get_resource_evidence_handler` | `resource_routes.rs` | P1 |

## Workspace 文件工作区

工作区浏览、文件/目录增删改、上传、下载、raw 读取和 session attachment。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/file/raw` | 读取 workspace 文件原始字节 | - | 支持 Query 参数，详见 handler Params struct | - | `raw_workspace_file_handler` | `workspace_routes.rs` | P2 |
| `GET` | `/api/sessions/:id/attachments` | Workspace 文件工作区 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `list_session_attachments_handler` | `workspace_routes.rs` | P1 |
| `POST` | `/api/sessions/:id/attachments` | Workspace 文件工作区 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `add_session_attachment_handler` | `workspace_routes.rs` | P1 |
| `DELETE` | `/api/sessions/:id/attachments/:ref_id` | Workspace 文件工作区 删除接口 | id, ref_id | - | 通常无 body 或仅 path/query | `delete_session_attachment_handler` | `workspace_routes.rs` | P1 |
| `POST` | `/api/upload` | Workspace 文件工作区 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `upload_workspace_file_handler` | `workspace_routes.rs` | P2 |
| `GET` | `/api/workspace` | Workspace 文件 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `workspace_handler` | `workspace_routes.rs` | P1 |
| `POST` | `/api/workspace/dirs` | Workspace 文件 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `create_workspace_dir_handler` | `workspace_routes.rs` | P1 |
| `GET` | `/api/workspace/download` | Workspace 文件 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `download_workspace_path_handler` | `workspace_routes.rs` | P1 |
| `DELETE` | `/api/workspace/files` | 列出、创建或删除 workspace 文件 | - | - | 通常无 body 或仅 path/query | `delete_workspace_path_handler` | `workspace_routes.rs` | P1 |
| `GET` | `/api/workspace/files` | 列出、创建或删除 workspace 文件 | - | 支持 Query 参数，详见 handler Params struct | - | `workspace_files_handler` | `workspace_routes.rs` | P1 |
| `POST` | `/api/workspace/files` | 列出、创建或删除 workspace 文件 | - | - | JSON 或 Multipart，详见对应 Request struct | `create_workspace_file_handler` | `workspace_routes.rs` | P1 |
| `GET` | `/api/workspace/meta` | Workspace 文件 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `workspace_meta_handler` | `workspace_routes.rs` | P1 |
| `POST` | `/api/workspace/rename` | Workspace 文件 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `rename_workspace_path_handler` | `workspace_routes.rs` | P1 |
| `GET` | `/api/workspaces` | Workspace 文件 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `workspaces_handler` | `workspace_routes.rs` | P1 |

## Profile 配置画像

配置画像列表、切换和删除。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/profiles` | Profile 配置画像 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `profiles_handler` | `profile_routes.rs` | P2 |
| `POST` | `/api/profiles` | Profile 配置画像 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `create_profile_handler` | `profile_routes.rs` | P2 |
| `DELETE` | `/api/profiles/:id` | Profile 配置画像 删除接口 | id | - | 通常无 body 或仅 path/query | `delete_profile_handler` | `profile_routes.rs` | P2 |
| `POST` | `/api/profiles/switch` | Profile 配置画像 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `switch_profile_handler` | `profile_routes.rs` | P2 |

## MFG 上层应用

制造领域应用：Reality 数据、事件、incident、playbook、case、analysis、action、report。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `POST` | `/api/apps/mfg/analyses/:analysis_id/actions/:action_id/execute` | MFG 应用 创建/动作接口 | analysis_id, action_id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_action_execute_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/analyses/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_analysis_get_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/app` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_app_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/cases/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_memory_case_get_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/cases/search` | MFG 应用 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `mfg_memory_case_search_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/cockpit/profiles/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_cockpit_profile_get_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/cockpit/profiles/:id/projection` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_cockpit_projection_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/cockpit/profiles/:id/reports/generate` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_cockpit_report_generate_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/cockpit/profiles/upsert` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_cockpit_profile_upsert_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/cockpit/reports/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_cockpit_report_get_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/cockpit/reports/:id/deliver` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_cockpit_report_deliver_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/cockpit/reports/:id/delivery-state` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_cockpit_report_delivery_state_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/cockpit/reports/:id/delivery/retry` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_cockpit_report_delivery_retry_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/cockpit/reports/schedules/run` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_cockpit_report_schedule_run_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/command-center` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_command_center_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/command-center/live` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_command_center_live_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/decision-trace` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_decision_trace_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/domain/server-manufacturing` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_server_manufacturing_domain_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/domain/server-manufacturing/seed` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_server_manufacturing_seed_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/executions/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_execution_get_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/executions/:id/cross-plane/execute` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_execution_cross_plane_bridge_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/executions/:id/feedback` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_execution_feedback_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/incidents` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_incidents_list_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/incidents` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_incident_create_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/incidents/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_incident_get_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/incidents/:id/analyze` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_incident_analyze_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/incidents/:id/cases/promote` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_incident_case_promote_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/incidents/:id/playbooks/recommend` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_incident_playbook_recommend_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/incidents/:id/room` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_incident_room_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/incidents/:id/skills` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_incident_skill_runs_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/incidents/:id/skills/:skill_id/run` | MFG 应用 创建/动作接口 | id, skill_id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_incident_skill_run_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/incidents/:id/skills/plan` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_incident_skill_plan_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/ontology/server-manufacturing` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_server_manufacturing_ontology_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/ontology/server-manufacturing/seed` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_server_manufacturing_ontology_seed_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/playbooks/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_playbook_get_handler` | `mfg_routes.rs` | P2 |
| `POST` | `/api/apps/mfg/playbooks/upsert` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_playbook_upsert_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/production/governance` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_production_governance_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/reality/attention/hot` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_reality_attention_hot_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/changes` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_reality_changes_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/compute/jobs/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_compute_job_get_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/compute/jobs/:id/run` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_compute_job_run_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/compute/jobs/plan` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_compute_job_plan_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/connector-runs/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_connector_run_get_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/data-plane/health` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_reality_data_plane_health_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/data-plane/ingest-plan` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_data_plane_ingest_plan_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/entities` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_reality_entities_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/entities/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_entity_get_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/entities/:id/impact-path` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_entity_impact_path_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/entities/:id/relations` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_entity_relations_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/entities/conflict-decision` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_entity_conflict_decision_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/entities/match-candidate` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_entity_match_candidate_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/entities/resolve-source-key` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_entity_resolve_source_key_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/entities/upsert` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_entity_upsert_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/evidence/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_evidence_get_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/evidence/:id/context` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_evidence_context_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/evidence/:id/quality-gate` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_evidence_quality_gate_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/evidence/build` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_evidence_build_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/facts/ingest` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_fact_ingest_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/health` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_reality_health_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/metric-dependencies/affected-by-fact-type` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_metric_affected_by_fact_type_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/metric-dependencies/upsert` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_metric_dependency_upsert_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/metrics` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_reality_metrics_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/metrics/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_metric_detail_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/metrics/:id/lineage` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_metric_lineage_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/metrics/attention-plan` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_metric_attention_plan_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/metrics/recompute` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_metric_recompute_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/metrics/snapshots/materialize` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_metric_snapshot_materialize_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/quality-gates/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_quality_gate_get_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/relations/upsert` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_relation_upsert_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/reality/source-packs/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_reality_source_pack_get_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/source-packs/:id/connector-runs/plan` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_source_pack_connector_run_plan_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/source-packs/:id/connector-runs/run` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_source_pack_connector_run_execute_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/source-packs/:id/delta-plan` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_source_pack_delta_plan_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/source-packs/:id/ingest-file` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_source_pack_ingest_file_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/source-packs/:id/validate` | MFG 应用 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_source_pack_validate_handler` | `mfg_routes.rs` | P1 |
| `POST` | `/api/apps/mfg/reality/source-packs/upsert` | MFG 应用 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `mfg_reality_source_pack_upsert_handler` | `mfg_routes.rs` | P1 |
| `GET` | `/api/apps/mfg/skill-runs/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_skill_run_get_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/skills` | MFG 应用 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `mfg_skills_handler` | `mfg_routes.rs` | P2 |
| `GET` | `/api/apps/mfg/skills/:id` | MFG 应用 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `mfg_skill_get_handler` | `mfg_routes.rs` | P2 |

## Harness Eval 评测

AI Harness 场景评测、运行、报告和最新报告。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/harness-eval/reports` | Harness Eval 评测 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `harness_eval_reports_handler` | `harness_eval_routes.rs` | P2 |
| `GET` | `/api/harness-eval/reports/:id` | Harness Eval 评测 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `harness_eval_report_detail_handler` | `harness_eval_routes.rs` | P2 |
| `GET` | `/api/harness-eval/reports/latest` | Harness Eval 评测 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `harness_eval_latest_report_handler` | `harness_eval_routes.rs` | P2 |
| `GET` | `/api/harness-eval/runs` | Harness Eval 评测 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `harness_eval_runs_handler` | `harness_eval_routes.rs` | P2 |
| `POST` | `/api/harness-eval/runs` | Harness Eval 评测 创建/动作接口 | - | - | JSON 或 Multipart，详见对应 Request struct | `harness_eval_start_run_handler` | `harness_eval_routes.rs` | P2 |
| `GET` | `/api/harness-eval/runs/:id` | Harness Eval 评测 查询接口 | id | 支持 Query 参数，详见 handler Params struct | - | `harness_eval_run_detail_handler` | `harness_eval_routes.rs` | P2 |
| `POST` | `/api/harness-eval/runs/:id/cancel` | Harness Eval 评测 创建/动作接口 | id | - | JSON 或 Multipart，详见对应 Request struct | `harness_eval_cancel_run_handler` | `harness_eval_routes.rs` | P2 |
| `GET` | `/api/harness-eval/scenarios` | Harness Eval 评测 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `harness_eval_scenarios_handler` | `harness_eval_routes.rs` | P2 |

## Audit 审计

跨模块审计导出。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/audit/export` | Audit 审计 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `audit_export_handler` | `audit_routes.rs` | P2 |

## Slash 命令

Slash 命令目录、详情、解析、分发和历史。

| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |
|---|---|---|---|---|---|---|---|---|
| `GET` | `/api/slash` | Slash 命令 查询接口 | - | 可选 Query 视具体 handler 而定 | - | `slash_catalog_handler` | `slash_routes.rs` | P2 |
| `GET` | `/api/slash/:id` | Slash 命令 查询接口 | id | 可选 Query 视具体 handler 而定 | - | `slash_detail_handler` | `slash_routes.rs` | P2 |
| `POST` | `/api/slash/dispatch` | 分发 slash 命令到后端动作 | - | - | JSON 或 Multipart，详见对应 Request struct | `slash_dispatch_handler` | `slash_routes.rs` | P2 |
| `GET` | `/api/slash/history` | Slash 命令 查询接口 | - | 支持 Query 参数，详见 handler Params struct | - | `slash_history_handler` | `slash_routes.rs` | P2 |
| `POST` | `/api/slash/resolve` | 解析用户输入中的 slash 命令意图 | - | - | JSON 或 Multipart，详见对应 Request struct | `slash_resolve_handler` | `slash_routes.rs` | P2 |
