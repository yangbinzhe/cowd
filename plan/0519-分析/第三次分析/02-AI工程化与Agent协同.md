# Cowd 第三次深度分析 — AI 工程化与 Agent 协同

> 基于 GitNexus：Wave 编排、子Agent委派、Worker 管理、Gates 质量控制、Task/Team/Cron 系统

## 1. Agent 系统架构

### 1.1 GitNexus 检测到的模块结构

| 模块 | 文件 | 关键符号 | 功能 |
|------|------|---------|------|
| Wave 编排 | wave.rs | test_wave_orchestrator, test_task_priority | 依赖图+并行执行 |
| Worker 管理 | worker_boot.rs | StartupEvidenceBundle.collect_from_worker | Worker 启动/信任/任务交付 |
| 子Agent | executor.rs | allowed_tools_for_subagent, build_agent_system_prompt | 权限收缩+System Prompt |
| Task 系统 | task_registry.rs | assign_team, creates_and_retrieves_team | 任务创建/分配/状态跟踪 |
| Team 系统 | team_cron_registry.rs | TeamRegistry.create | 团队创建/成员管理 |
| Cron 系统 | team_cron_registry.rs | 同上 | 定时任务调度 |
| Gates | gates (对应模块) | 四层门: PreFlight/Revision/Escalation/Abort | 质量把关 |
| 工具编排 | tool_orchestrator.rs | ToolSafetyCategory.from_tool_name | 安全分类+并发控制 |

### 1.2 Wave 并行编排引擎

```
DependencyGraph (依赖图构建+循环检测)
  ↓
WaveOrchestrator (Wave 构建/调度)
  ↓
同Wave内任务并行 | 跨Wave按依赖顺序
  ↓
ErrorPolicy (停止/跳过/忽略)
  ↓
WaveExecutor trait (可扩展任务执行器)
```

## 2. 设计优点

### 2.1 Worker 全生命周期管理
```
worker_boot.rs 实现了完整的 Worker 生命周期:
  boot → trust_gate → prompt_delivery → observe → resolve_trust → terminate
StartupEvidenceBundle 收集启动证据并验证
```

### 2.2 权限分级明确
```
子Agent权限收缩: allowed_tools_for_subagent() 定义了子Agent可见工具集
记忆写保护: sub_agent_cannot_write_l0_l1 — 子Agent不能修改核心记忆层
```

### 2.3 Team + Task + Cron 三系统并立
- Task: 一次性任务，支持分配和状态跟踪
- Team: 多Agent协作，SharedScope 隔离
- Cron: 定时重复任务

## 3. 缺陷与弊端

### 🔴 P0: 子Agent通信机制单一
**问题**：子Agent通过 stdin/stdout 通信，无结构化消息协议。
**影响**：
  - 无双向流式通信（子Agent无法推送进度）
  - 无超时/心跳机制（僵尸子Agent无法检测）
  - 错误恢复依赖子进程退出码，信息丢失
**GitNexus 数据**：mcp_stdio.rs 实现了 MCP 协议的 call_tool/shutdown，但 Agent 间未复用。

### 🔴 P0: 无分布式编排
**问题**：所有 Agent 在同一进程/机器执行，无跨节点调度。
**影响**：大规模并行重构场景下受限于单机资源。
**建议**：引入 Agent 注册中心 + 消息队列。

### 🟡 P1: Gates 系统与工具执行耦合不完整
**问题**：四层Gate（PreFlight/Revision/Escalation/Abort）在代码中定义完整，
  但与 GlobalToolRegistry 的集成仅在部分路径。
**GitNexus 数据**：tool_orchestrator.rs 实现了 ToolSafetyCategory，但 Gates 结果
  只影响审批流，不影响执行策略（如自动重试、降级）。

### 🟡 P1: Wave 编排缺少运行时动态调整
**问题**：DependencyGraph 在构建时静态确定，无法根据执行结果动态调整。
**影响**：若 Wave 2 中某任务失败，Wave 3-5 不会自动跳过或重分配。

### 🟢 P2: Team/Cron 系统缺少调度持久化
**问题**：Cron 任务在内存中调度，服务重启后丢失。
**GitNexus 数据**：team_cron_registry.rs 没有持久化接口。
**建议**：添加 SQLite 调度表。

## 4. Agent 协同最大化价值方案

1. **Agent 通信协议统一**：复用 MCP 协议作为 Agent 间通信标准（mcp_stdio.rs 已有实现）
2. **分布式 Worker Pool**：基于 Redis/gRPC 的跨机器 Worker 调度
3. **动态 Wave 编排**：支持运行时依赖解析和错误恢复策略
4. **Gates 集成 pipeline**：每个工具调用自动经过 Gate 链
5. **持久化调度**：Cron/Team 状态在重启后可恢复

