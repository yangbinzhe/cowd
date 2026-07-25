#!/usr/bin/env python3
"""Generate Gateway API Markdown docs from axum route declarations."""

from __future__ import annotations

import datetime as _dt
import re
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
API_ROUTES = ROOT / "crates" / "gateway" / "src" / "api_routes"
API_DOC = ROOT / "docs" / "api" / "gateway-api-reference.md"
FRAMEWORK_DOC = ROOT / "docs" / "architecture" / "gateway-api-framework.md"


GROUP_ORDER = [
    "public",
    "core",
    "runtime",
    "session",
    "message",
    "mission",
    "agent",
    "task",
    "context",
    "memory",
    "reality",
    "matrix",
    "growth",
    "tool",
    "skill",
    "approval",
    "cross_plane",
    "surface",
    "edge",
    "channel",
    "connector",
    "resource",
    "workspace",
    "profile",
    "mfg",
    "harness_eval",
    "audit",
    "slash",
    "other",
]

GROUP_TITLES = {
    "public": "公共入口与认证",
    "core": "Cowd 核心投影与发布门禁",
    "runtime": "Runtime 执行核心",
    "session": "Session 生命周期",
    "message": "对话消息与 SSE",
    "mission": "Mission Control / 多 Session 多 Agent 协同",
    "agent": "Agent 目录、组队与运行",
    "task": "Task 阶段化执行",
    "context": "Context / Evidence",
    "memory": "Memory / Knowledge",
    "reality": "Reality Core",
    "matrix": "Matrix 结构化事实",
    "growth": "Growth / 自我演进",
    "tool": "Tools 工具执行",
    "skill": "Skills 技能体系",
    "approval": "Approval 审批",
    "cross_plane": "Cross Plane 权限与动作",
    "surface": "Surface 接入面",
    "edge": "Edge 热加载与外部包",
    "channel": "Channel / Platform",
    "connector": "Connector 数据与服务连接",
    "resource": "Resource 附件资源",
    "workspace": "Workspace 文件工作区",
    "profile": "Profile 配置画像",
    "mfg": "MFG 上层应用",
    "harness_eval": "Harness Eval 评测",
    "audit": "Audit 审计",
    "slash": "Slash 命令",
    "other": "其他",
}

GROUP_DESCRIPTIONS = {
    "public": "健康检查、WebUI manifest 和认证入口；公共路由不经过统一 Bearer 中间件。",
    "core": "Gateway 对外暴露的全局能力图、结构化事实投影、发布门禁和路由清单。",
    "runtime": "AI Harness 的运行状态、事件、控制平面、配置热加载、turn 提交和 session lease。",
    "session": "持久会话、分叉、压缩、统计、事件、运行投影和 session 级管理。",
    "message": "用户消息写入、历史消息读取和会话 SSE 流。",
    "mission": "Mission Runtime 的全局控制、跨 session 命令、team runtime、steward、审批和代理关系。",
    "agent": "Runtime-owned Agent Definition、Team Template、自动发现、组队、信誉和执行投影视图。",
    "task": "任务启动、阶段、产物、审查、失败、完成和取消。",
    "context": "当前上下文、上下文历史、推荐、压缩和证据引用解析。",
    "memory": "记忆层、检索、packet、维护候选、实体、三元组、知识与性能状态。",
    "reality": "Reality Core 的静态地图、动态 fact flow、promotions、governance、boundaries 和 evidence。",
    "matrix": "结构化源包、实体、事实、指标、变化、证据包和连接器运行。",
    "growth": "成长状态、成长事件与演进线索。",
    "tool": "工具注册表、单工具调用、只读批量、mutation 预览/提交、幂等与意图规划。",
    "skill": "技能目录、投影、详情、文件、翻译、运行记录和 validate/plan/run 动作。",
    "approval": "待审批、审批响应、风险收据、solo 策略、审批配置与历史。",
    "cross_plane": "跨 plane 能力授权、风险评估、动作执行、幂等与审计。",
    "surface": "Surface 注册表、健康、路由、资源、状态、事件、启动/停止/修复和消息投递。",
    "edge": "Edge 包发现、健康、热加载、surface/connector/resource 投影。",
    "channel": "平台、消息 channel、Feishu/WeChat 等渠道状态与治理。",
    "connector": "外部资源连接、账号、capability、MCP、source/service/tool connector。",
    "resource": "上传资源注册、资源详情和资源 evidence。",
    "workspace": "工作区浏览、文件/目录增删改、上传、下载、raw 读取和 session attachment。",
    "profile": "配置画像列表、切换和删除。",
    "mfg": "制造领域应用：Reality 数据、事件、incident、playbook、case、analysis、action、report。",
    "harness_eval": "AI Harness 场景评测、运行、报告和最新报告。",
    "audit": "跨模块审计导出。",
    "slash": "Slash 命令目录、详情、解析、分发和历史。",
}

PATH_PURPOSES = [
    ("/api/runtime/config/reload/status", "查看配置热加载状态、错误、是否需要重启与最近一次应用结果"),
    ("/api/runtime/config/reload", "手动触发 Gateway/Runtime 配置重载"),
    ("/api/runtime/control-plane", "获取 Runtime 控制平面总览，供前端判断核心能力健康状态"),
    ("/api/runtime/timeline", "按 session 查询 Runtime 事件时间线"),
    ("/api/runtime/turns", "查询或提交 Runtime turn"),
    ("/api/runtime/live-subscriptions", "创建或更新 Surface 多源实时订阅"),
    ("/api/runtime/live/:id", "通过单一 SSE 物理连接接收多源实时投影"),
    ("/api/sessions/:id/messages", "读取或发送指定 session 的对话消息"),
    ("/api/sessions/:id/stats", "读取 session token、耗时和运行统计"),
    ("/api/sessions/:id/runs", "读取 session 关联 runtime run 和证据树"),
    ("/api/context/current", "构建当前上下文 envelope"),
    ("/api/evidence/resolve", "解析 evidence ref 到可展示证据"),
    ("/api/memory/packet", "构建面向上下文注入的记忆 packet"),
    ("/api/memory/maintenance", "读取或生成记忆治理候选"),
    ("/api/matrix/evidence/build", "基于结构化事实构建 evidence packet"),
    ("/api/surfaces/:id/start", "启动 managed surface"),
    ("/api/surfaces/:id/stop", "停止 managed surface"),
    ("/api/surfaces/:id/restart", "重启 managed surface"),
    ("/api/surfaces/:id/repair", "对 surface 执行人工修复动作"),
    ("/api/edges/reload", "重新发现 Edge 包和 connector/surface 资源"),
    ("/api/workspace/files", "列出、创建或删除 workspace 文件"),
    ("/api/file/raw", "读取 workspace 文件原始字节"),
    ("/api/resources", "上传附件资源并注册到 runtime resource store"),
    ("/api/slash/resolve", "解析用户输入中的 slash 命令意图"),
    ("/api/slash/dispatch", "分发 slash 命令到后端动作"),
    ("/api/tools/batch-readonly", "批量并行执行幂等只读工具"),
    ("/api/tools/execute", "执行单个工具调用"),
    ("/api/mission/control", "读取或写入 Mission Control 全局控制状态"),
    ("/api/mission/route", "将跨 session/agent 命令路由到目标"),
    ("/api/approval/pending", "读取待审批请求"),
    ("/api/approval/respond", "提交审批决策"),
]


def group_for(rel: str) -> str:
    top = rel.split("/", 1)[0]
    if top.startswith("mfg_routes"):
        return "mfg"
    if top.startswith("matrix_routes"):
        return "matrix"
    if top.startswith("runtime_routes"):
        return "runtime"
    if top.startswith("connector_routes"):
        return "connector"
    if top.startswith("cross_plane_routes"):
        return "cross_plane"
    if top.startswith("harness_eval_routes"):
        return "harness_eval"
    if top.startswith("system_routes"):
        return "tool"
    if top.endswith("_routes.rs"):
        return top.removesuffix("_routes.rs")
    return "other"


def operation_kind(method: str) -> str:
    return {
        "GET": "查询",
        "POST": "创建/动作",
        "PUT": "全量更新",
        "PATCH": "局部更新",
        "DELETE": "删除",
    }.get(method, "调用")


def path_params(path: str) -> str:
    params = re.findall(r":([A-Za-z0-9_]+)", path)
    return ", ".join(params) if params else "-"


def query_hint(path: str, method: str) -> str:
    if method != "GET":
        return "-"
    if any(part in path for part in ["/search", "/timeline", "/events", "/runs", "/stats", "/files", "/raw", "/download", "/current", "/packet", "/export", "/history", "/maintenance", "/clusters"]):
        return "支持 Query 参数，详见 handler Params struct"
    return "可选 Query 视具体 handler 而定"


def body_hint(method: str) -> str:
    if method == "GET":
        return "-"
    if method == "DELETE":
        return "通常无 body 或仅 path/query"
    return "JSON 或 Multipart，详见对应 Request struct"


def purpose_for(path: str, group: str, method: str) -> str:
    for exact, purpose in PATH_PURPOSES:
        if path == exact:
            return purpose
    if path.startswith("/api/apps/mfg"):
        return f"MFG 应用 {operation_kind(method)}接口"
    if path.startswith("/api/matrix"):
        return f"Matrix 结构化事实 {operation_kind(method)}接口"
    if path.startswith("/api/mission"):
        return f"Mission Runtime 协同 {operation_kind(method)}接口"
    if path.startswith("/api/runtime"):
        return f"Runtime 执行核心 {operation_kind(method)}接口"
    if path.startswith("/api/memory"):
        return f"Memory/Knowledge {operation_kind(method)}接口"
    if path.startswith("/api/reality"):
        return f"Reality Core {operation_kind(method)}接口"
    if path.startswith("/api/surfaces") or path.startswith("/s/"):
        return f"Surface 接入面 {operation_kind(method)}接口"
    if path.startswith("/api/connectors"):
        return f"Connector 外部连接 {operation_kind(method)}接口"
    if path.startswith("/api/cross-plane"):
        return f"Cross Plane 权限与动作 {operation_kind(method)}接口"
    if path.startswith("/api/tools"):
        return f"工具系统 {operation_kind(method)}接口"
    if path.startswith("/api/skills"):
        return f"技能系统 {operation_kind(method)}接口"
    if path.startswith("/api/workspace") or path.startswith("/api/file"):
        return f"Workspace 文件 {operation_kind(method)}接口"
    if path.startswith("/api/resources"):
        return f"附件资源 {operation_kind(method)}接口"
    return f"{GROUP_TITLES.get(group, group)} {operation_kind(method)}接口"


def criticality(path: str) -> str:
    p1_tokens = [
        "/actions/",
        "/approval",
        "/context",
        "/cross-plane",
        "/matrix",
        "/memory",
        "/mission",
        "/reality",
        "/release-gate",
        "/resources",
        "/runtime",
        "/sessions",
        "/surfaces",
        "/tools",
        "/workspace",
    ]
    if any(token in path for token in p1_tokens):
        return "P1"
    return "P2"


def stability(path: str) -> str:
    if path.startswith("/api/"):
        return "stable"
    return "surface/static"


def parse_routes() -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for source_path in sorted(API_ROUTES.rglob("*.rs")):
        rel = source_path.relative_to(API_ROUTES).as_posix()
        if rel in {"mod.rs", "route_manifest.rs", "matrix_outcomes.rs", "mfg_outcomes.rs"}:
            continue
        source = source_path.read_text(encoding="utf-8")
        offset = 0
        while True:
            index = source.find(".route(", offset)
            if index < 0:
                break
            args_start = index + len(".route(")
            args_end = route_call_end(source, args_start)
            if args_end < 0:
                break
            window = source[args_start:args_end]
            offset = args_end
            path_match = re.match(r'\s*"([^"]+)"\s*,', window)
            if not path_match:
                continue
            path = path_match.group(1)
            for method, handler in re.findall(r"\b(get|post|put|patch|delete)\s*\(\s*([A-Za-z0-9_]+)", window):
                group = group_for(rel)
                entries.append(
                    {
                        "method": method.upper(),
                        "path": path,
                        "group": group,
                        "source": rel,
                        "handler": handler,
                        "purpose": purpose_for(path, group, method.upper()),
                        "path_params": path_params(path),
                        "query": query_hint(path, method.upper()),
                        "body": body_hint(method.upper()),
                        "criticality": criticality(path),
                        "stability": stability(path),
                    }
                )
    unique = {(entry["method"], entry["path"]): entry for entry in entries}
    return sorted(unique.values(), key=lambda item: (GROUP_ORDER.index(item["group"]) if item["group"] in GROUP_ORDER else 999, item["path"], item["method"]))


def route_call_end(source: str, start: int) -> int:
    depth = 1
    in_string = False
    escaped = False
    for index in range(start, len(source)):
        ch = source[index]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return index
    return -1


def table(rows: list[dict[str, str]]) -> str:
    lines = [
        "| 方法 | 路径 | 用途 | Path 参数 | Query | Body | Handler | Source | 级别 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| `{row['method']}` | `{row['path']}` | {row['purpose']} | {row['path_params']} | {row['query']} | {row['body']} | `{row['handler']}` | `{row['source']}` | {row['criticality']} |"
        )
    return "\n".join(lines)


def write_api_reference(routes: list[dict[str, str]]) -> None:
    API_DOC.parent.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for route in routes:
        grouped[route["group"]].append(route)
    today = _dt.date.today().isoformat()
    lines = [
        "# Gateway API 全量接口说明书",
        "",
        f"生成时间：{today}",
        "",
        "来源：`crates/gateway/src/api_routes/**/*.rs` 中实际 `axum::Router::route` 声明，并与 `crates/gateway/src/api_routes/route_manifest.rs` 的运行时清单方向保持一致。",
        "",
        f"当前共识别 `{len(routes)}` 个唯一 `method + path` 接口。",
        "",
        "## Capability Contract / OpenAPI 状态",
        "",
        "当前仓库未引入 `utoipa`、`aide`、`schemars`、`paperclip`、Swagger UI、Redoc 或 Scalar 这类 OpenAPI UI/注解框架。",
        "",
        "Gateway 现在以 `/api/gateway/capability-contract` 作为运行时接口能力真源，并派生 `/api/gateway/openapi.json` 与 `/api/gateway/openai-tools`。`/api/gateway/route-manifest` 仍作为轻量路由存在性和发布门禁基线。",
        "",
        "本文档由路由源码生成，运行时消费应优先读取 Gateway Capability Contract；如需 Swagger UI，可由静态 surface 直接消费 `/api/gateway/openapi.json`，不需要把接口 source of truth 移出 gateway。",
        "",
        "## 通用约定",
        "",
        "- 认证：除公共路由外，受保护接口要求 `Authorization: Bearer <token>`；Gateway 未配置认证凭据时 fail closed，不提供 WebUI 同源绕过。",
        "- 响应：查询类接口通常返回 JSON object；写接口成功返回 JSON receipt 或对象；错误通常返回 `{ \"error\": \"...\" }` 与对应 HTTP status。",
        "- 路径参数：Axum 使用 `:id`、`:name`、`:surface` 等形式。",
        "- Body：POST/PUT/PATCH 多数为 JSON；上传类接口使用 multipart。",
        "- 稳定性：`/api/**` 视为稳定 HTTP API；`/s/:surface/*path` 属于 surface 静态资源转发。",
        "",
        "## 分组目录",
        "",
    ]
    for group in GROUP_ORDER:
        if group in grouped:
            lines.append(f"- [{GROUP_TITLES.get(group, group)}](#{GROUP_TITLES.get(group, group).lower().replace(' ', '-')})：{len(grouped[group])} 个接口")
    lines.append("")

    for group in GROUP_ORDER:
        rows = grouped.get(group)
        if not rows:
            continue
        lines.extend(
            [
                f"## {GROUP_TITLES.get(group, group)}",
                "",
                GROUP_DESCRIPTIONS.get(group, ""),
                "",
                table(rows),
                "",
            ]
        )

    API_DOC.write_text("\n".join(lines), encoding="utf-8")


def write_framework_doc(routes: list[dict[str, str]]) -> None:
    FRAMEWORK_DOC.parent.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for route in routes:
        grouped[route["group"]].append(route)
    today = _dt.date.today().isoformat()
    lines = [
        "# Gateway API 总框架与关系设计",
        "",
        f"生成时间：{today}",
        "",
        "本文说明 Gateway API 的职责边界、接口关系和前后端使用逻辑。全量接口表见 [`docs/api/gateway-api-reference.md`](../api/gateway-api-reference.md)。",
        "",
        "## 设计定位",
        "",
        "Gateway 是 Cowd 的 HTTP 控制面和 Surface/Edge 接入中枢。它不应该成为第二套 AI 执行器，而是把 Runtime、Mission、Memory、Reality、Matrix、Tools、Skills、Surface、Connector 等后端能力统一暴露给 WebUI、TUI、外部 surface 和自动化系统。",
        "",
        "## 分层关系",
        "",
        "```mermaid",
        "flowchart TB",
        "  WebUI[WebUI Surface] --> Gateway[Gateway HTTP API]",
        "  TUI[TUI Surface] --> Gateway",
        "  Feishu[Message Surface / Feishu] --> Gateway",
        "  Edge[Cowd Edge: surfaces + connectors] --> Gateway",
        "  Gateway --> Runtime[Runtime / AI Harness]",
        "  Gateway --> Session[Session Kernel]",
        "  Gateway --> Mission[Mission Runtime]",
        "  Gateway --> Memory[Memory / Knowledge]",
        "  Gateway --> Reality[Reality Core]",
        "  Gateway --> Matrix[Matrix Structured Facts]",
        "  Gateway --> Tools[Tools]",
        "  Gateway --> Skills[Skills]",
        "  Gateway --> CrossPlane[Cross Plane Policy]",
        "  Gateway --> Workspace[Workspace / Resources]",
        "  Gateway --> SurfaceHost[Surface Host]",
        "  SurfaceHost --> Edge",
        "```",
        "",
        "## 接口族关系",
        "",
        "| 接口族 | 核心职责 | 上游消费者 | 下游能力 | 数量 |",
        "|---|---|---|---|---|",
    ]
    for group in GROUP_ORDER:
        rows = grouped.get(group)
        if not rows:
            continue
        consumers = {
            "public": "浏览器、探活、登录页",
            "runtime": "WebUI、TUI、Mission、调试工具",
            "session": "WebUI、TUI、Surface 消息入口",
            "message": "WebUI、TUI、Surface 消息入口",
            "mission": "WebUI、Runtime、Agent 协同",
            "surface": "WebUI、运维、Gateway supervisor",
            "edge": "WebUI、Gateway hot reload",
            "connector": "WebUI、Matrix、Reality、MFG",
            "workspace": "WebUI、TUI、Runtime 工具",
            "resource": "WebUI、Surface 附件、Runtime 上下文",
            "mfg": "WebUI MFG 应用",
        }.get(group, "WebUI、TUI、Runtime 或运维工具")
        downstream = GROUP_DESCRIPTIONS.get(group, GROUP_TITLES.get(group, group))
        lines.append(f"| {GROUP_TITLES.get(group, group)} | {downstream} | {consumers} | `{group}` services / kernels | {len(rows)} |")
    lines.extend(
        [
            "",
            "## 关键调用链路",
            "",
            "### 对话执行链路",
            "",
            "1. 前端通过 `/api/sessions` 创建或选择 session。",
            "2. 前端通过 `/api/sessions/:id/messages` 写入用户消息。",
            "3. Gateway 交给 Session/Runtime 处理，并通过 `/api/runtime/live/:id` 的单一 multiplex SSE 输出 Session、Execution 与 Mission 投影。",
            "4. Runtime 过程中产生 timeline、context envelope、tool events、memory recall、reality flow。",
            "5. 前端通过 `/api/runtime/timeline`、`/api/context/current`、`/api/reality/flow`、`/api/sessions/:id/runs` 做证据展示。",
            "",
            "### 多 Agent / 多 Session 协同链路",
            "",
            "1. `/api/mission/control` 暴露全局 mission projection。",
            "2. `/api/mission/route`、`/api/mission/sessions/:id/inbox`、`/api/mission/control/sessions/dispatch` 负责跨 session 命令流转。",
            "3. `/api/mission/control/teams/:team_id/*` 负责 team runtime、证据、handoff、synthesis 和取消。",
            "4. `/api/agents/*` 提供 agent 目录、组队模板、运行记录和 team profile。",
            "",
            "### Reality / Memory / Matrix 证据链路",
            "",
            "1. `/api/context/current` 生成本轮上下文 envelope。",
            "2. `/api/memory/packet`、`/api/memory/recall/explain` 提供非结构化记忆召回与解释。",
            "3. `/api/matrix/*` 管理结构化事实、实体、指标、变化和 evidence packet。",
            "4. `/api/reality/flow`、`/api/reality/promotions` 把运行时事实流和记忆/结构化事实 promotion 显性化。",
            "5. `/api/evidence/resolve`、`/api/reality/evidence/:id`、`/api/matrix/evidence/:id` 提供证据解析入口。",
            "",
            "### Surface / Edge / Connector 链路",
            "",
            "1. Edge 包提供 surfaces 和 connectors。",
            "2. Gateway 的 SurfaceHost 发现、启动、监控、修复这些外部进程或静态资源。",
            "3. `/api/edges/*` 展示 Edge 包发现和热加载状态。",
            "4. `/api/surfaces/*` 管理消息/静态 surface 生命周期。",
            "5. `/api/connectors/*` 管理数据源、MCP、service connector 和 connector resource。",
            "",
            "## 自动化文档策略",
            "",
            "- 当前可自动生成：`scripts/generate_gateway_api_docs.py` 从 Axum route 声明生成 Markdown 全量清单。",
            "- 当前运行时真源：`GET /api/gateway/capability-contract`，负责 method/path/source/handler/risk/visibility/schema/tool affordance 的统一导出。",
            "- 当前派生输出：`GET /api/gateway/openapi.json` 生成轻量 OpenAPI 3.1 JSON，`GET /api/gateway/openai-tools` 生成 OpenAI-compatible tool schema。",
            "- 推荐 UI：如需 Swagger/Scalar/Redoc，作为静态 surface 消费 `/api/gateway/openapi.json`；不要另建手写接口清单。",
            "",
            "## 设计约束",
            "",
            "- Gateway 路由只做 HTTP 适配、鉴权、参数解析和 service 调用，不承载第二套 AI 执行循环。",
            "- Runtime 是 AI Harness 执行核心；Gateway 只负责触发、观察、治理和投影。",
            "- WebUI/TUI/Feishu 等 surface 共享后端服务，不应产生多套业务执行路径。",
            "- Connector/Surface/Edge 可以热加载，但合同必须区分消息接入、静态资源、数据资源和自动化能力。",
            "- 管理台接口应优先提供稳定 projection，避免前端跨多个原始接口猜字段。",
        ]
    )
    FRAMEWORK_DOC.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    routes = parse_routes()
    write_api_reference(routes)
    write_framework_doc(routes)
    print(f"generated {len(routes)} routes")
    print(API_DOC.relative_to(ROOT))
    print(FRAMEWORK_DOC.relative_to(ROOT))


if __name__ == "__main__":
    main()
