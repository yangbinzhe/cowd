# Cowd 第三次深度分析 — GitNexus 知识图谱数据

> 日期：2026-05-19 | 工具：GitNexus MCP (query / context / cypher / impact)
> 索引数据：14,056 节点 | 29,189 边 | 1,268 社区 | 300 执行流

## 1. 项目社区结构（Community Analysis）

GitNexus Leiden 算法自动检测的功能区域及其凝聚力：

| 社区 | 符号数 | 凝聚力 | 核心职责 |
|------|--------|--------|---------|
| Cluster_405 | 72 | 0.876 | Conversation 运行时核心 |
| Webui (JS) | 62 | 1.000 | 浏览器前端 |
| Tests | 51 | 0.823 | 测试基础设施 |
| Js (inner) | 50 | 0.990 | 前端内部模块 |
| Cluster_168 | 49 | 0.695 | 构建/插件/工具测试 |
| Cluster_861 | 31 | 0.991 | **极高凝聚力**模块 |
| Cluster_864 | 30 | 0.922 | 高凝聚力核心 |
| Cluster_945 | 30 | 0.636 | Tool Executor（**凝聚度偏低**） |
| Cluster_781 | 28 | 0.791 | 配置验证 |
| Cluster_724 | 27 | 0.731 | 中型功能模块 |

**关键发现**：
- main.rs 调用 37+ 个不同模块（从 run_tui_repl 单个函数分析）
- 凝聚度低于 0.7 的社区需要重构（如 Cluster_945 / Cluster_168）
- WebUI 凝聚力 1.0 说明零依赖纯模块

## 2. 核心调用链（Key Call Chains）

### 2.1 TUI 入口 → 渲染管线

```
run_repl → run_tui_repl
  ├── tui_event_channel()        → (tx, rx) 事件通道
  ├── App::new()                 → TUI 状态初始化
  ├── refresh_panels()           → 面板刷新
  ├── drain_tui_events()         → 事件收集循环
  ├── handle_input()             → 用户输入处理
  ├── draw()                     → ratatui 渲染
  ├── prepare_turn_runtime()     → 模型调用准备
  ├── replace_runtime()          → 运行时替换
  └── persist_session()          → 会话持久化
```

### 2.2 HTTP Server 启动链路

```
run → start_http_server
  ├── find_webui_dir()           → WebUI 静态资源
  ├── init_app_state()           → HTTP 应用状态
  ├── axum Router 注册           → 20+ 路由端点
  ├── CompressionGuard::builder()→ 记忆压缩守护
  └── cleanup_sessions()         → 会话清理
```

### 2.3 工具执行链路

```
mvp_tool_specs (50+ tools)
  ├── ToolSafetyCategory::from_tool_name()  → 安全分类
  ├── SmartApprovalGate::evaluate()         → 审批门
  ├── GlobalToolRegistry                    → 工具注册表
  ├── allowed_tools_for_subagent()          → 子Agent权限过滤
  └── read_only_registry()                  → 只读工具集
```

## 3. 符号密度热图（Symbol Concentration）

关键文件及函数密度（GitNexus 提取）：

| 文件 | 关键符号 | 职责 |
|------|---------|------|
| `cowd-cli/src/main.rs` | run_tui_repl, run_repl, LiveCli (40+方法) | **过载：CLI+TUI+Server 混合** |
| `cowd-cli/src/server.rs` | start_http_server (292行), send_message_stream_handler | HTTP/SSE 服务 |
| `runtime/src/conversation.rs` | run_turn_async (167行), new_with_features | 对话引擎核心 |
| `tools/src/executor.rs` | run_team_create, build_agent_system_prompt | 工具+Agent执行 |
| `tools/src/tool_specs.rs` | mvp_tool_specs (866行!) | **巨型工具定义** |
| `memory/src/aaak_compression.rs` | compress/decompress | AAAK 压缩算法 |
| `memory/src/temporal_graph.rs` | temporal_relation | 时序知识图谱 |
| `memory/src/closet.rs` | build_and_search | 快速路由索引 |
| `config/src/lib.rs` | UnifiedConfig, resolve_provider | 统一配置 |
| `runtime/src/config.rs` | ConfigLoader.load (69行) | **另一套配置加载器** |

