# 测试框架增强 — v0.7.5 TDD 执行方案

## Phase 0: 基础设施修复（2 并行）

### T0. tmux Drop 自动清理 [tui.rs]
- Add `Drop` impl for `TuiSession`: kill-session on drop
- Add `Drop` impl for `ServerProcess`: kill child on drop  
- 测试验证: panic 后 tmux session 不残留

### T0s. Server 进程 Drop 清理 [server.rs]
- ServerProcess: impl Drop { kill child }
- 移除手动 shutdown 调用，依赖 RAII

## Phase 1: TUI 面板测试（5 并行）

### T1. GatewayPanel 测试 [scenarios/tui_gateway.rs]
- 启动 TUI → 切到 Gateway panel → 验证显示 server status
- 验证 API 端点列表展示
- RED: 编写测试, GREEN: 实现

### T2. MemoryPanel 测试 [scenarios/tui_memory.rs]
- 启动 TUI → 切到 Memory panel → 验证记忆列表
- 测试搜索触发
- RED: 编写测试, GREEN: 实现

### T3. SkillsPanel 测试 [scenarios/tui_skills.rs]
- 启动 TUI → 切到 Skills panel → 验证分类展示
- 测试 Tab 切换分类
- RED: 编写测试, GREEN: 实现

### T4. ChatView 测试 [scenarios/tui_chat.rs]
- 发送消息 → 验证流式渲染
- 测试滚动
- RED: 编写测试, GREEN: 实现

### T5. Session 切换测试 [scenarios/tui_session.rs]
- /session list → 验证列表
- /session new → 验证新会话创建
- RED: 编写测试, GREEN: 实现

## Phase 2: Server API 测试（3 并行）

### T6. Gateway API 测试 [scenarios/server_gateway_api.rs]
- GET /api/memory → 验证 JSON 结构
- GET /api/tools → 验证工具列表
- GET /api/config → 验证配置结构
- RED: 编写测试, GREEN: 实现

### T7. Gateway 命令测试 [scenarios/server_gateway_cmd.rs]
- cowd gateway start → 验证进程启动
- cowd gateway status → 验证状态
- cowd gateway stop → 验证进程停止
- RED: 编写测试, GREEN: 实现

### T8. 迁移命令测试 [scenarios/server_migrate.rs]
- cowd migrate-sessions → 验证迁移
- RED: 编写测试, GREEN: 实现

## Phase 3: 交叉测试（2 并行）

### T9. ActiveSessions 交叉测试 [scenarios/cross_active.rs]
- Server 创建 session → TUI 看到并切换
- RED: 编写测试, GREEN: 实现

### T10. 全面板切换测试 [scenarios/tui_all_panels.rs]
- 遍历 10 个面板 → 每个验证显示
- 验证快捷键提示行存在
- RED: 编写测试, GREEN: 实现

## Phase 4: 验证

### T11. 全量运行
- 19 existing + 12 new = 31 scenarios pass
- tmux zero orphan sessions
- 报告每个场景耗时
