# COWD — AI 编程智能体框架

> **Rust 原生高性能 AI 编程智能体** | 273 源文件 · 173K 行 · 500+ 测试
> CLI / TUI / WebUI · 五层记忆 · 代码图谱 · MCP · 插件系统

---

## 项目规模

| Crate | 文件 | 行数 | 职责 |
|-------|------|------|------|
| `cowd-cli` | 68 | 50,749 | 主程序：CLI / TUI / Server 入口 |
| `runtime` | 81 | 45,131 | 运行时核心：会话、工具、权限、MCP、Gates |
| `memory` | 61 | 32,411 | 五层记忆 + tree-sitter 代码图谱 |
| `tools` | 8 | 10,633 | 50+ 内置工具规范 |
| `commands` | 7 | 8,926 | 100+ 斜杠命令 + 技能系统 |
| `api` | 13 | 7,939 | 多 Provider 模型适配层 |
| `plugins` | 3 | 4,288 | 插件注册与生命周期 |
| `config` | 2 | 2,162 | 统一配置管理 |
| 其他 | 5 | 1,814 | 遥测、兼容测试、Mock 服务 |
| **总计** | **273** | **173,053** | |

---

## TUI 终端界面

### 组件系统 (19 组件, ~31K 行)

```
┌─────────────────────────────┬──────────────────┐
│  ChatView (70%)             │  侧边栏 (30%)     │
│  · 消息时间线               │  Context | Changes │
│  · 流式渲染 + 虚拟滚动      │  Todo | Diff       │
│  · 搜索高亮                 │  Files | Sessions  │
│                             │  ── 6 个标签页     │
├─────────────────────────────┴──────────────────┤
│  Prompt (自动补全 + frecency + @file 引用)       │
├────────────────────────────────────────────────┤
│  状态栏 (模型 / Token / MCP / LSP / 权限)       │
└────────────────────────────────────────────────┘
+ Toast · Dialog · CommandPalette · WhichKey
+ ThinkingPanel · AgentsOverlay · QuestionForm
```

### 对标 opencode (交互模式 ~95% 覆盖)

| 能力 | 实现 | 能力 | 实现 |
|------|------|------|------|
| Component trait 抽象 | `base.rs` | Diff Viewer (unified/split) | `diff_viewer.rs` |
| LayoutTree 布局引擎 | `layout/` | Command Palette (Ctrl+P) | `command_palette.rs` |
| Keybind + Which-Key | `keybind/` | Session Sidebar | `session_sidebar.rs` |
| Dialog (Alert/Confirm/Select/Prompt) | `dialog.rs` | File Tree Browser | `file_tree.rs` |
| Toast 通知 | `toast.rs` | Export Dialog | `export_dialog.rs` |
| Theme Engine (热加载) | `theme/` | Session Fork | `session_sidebar.rs` |
| Prompt 自动补全 (frecency) | `prompt.rs` | Multi-Stage Permission | `dialog.rs` |
| Question 多问题表单 | `question_form.rs` | Sub-Agent 导航 | `chat_view.rs` |
| Startup Loading | `state.rs` | Clipboard (OSC52 + 图像) | `clipboard.rs` |
| Infinite Scroll | `app.rs` | ANSI 颜色降级 | `ansi_fallback.rs` |
| Animation 引擎 | `animation.rs` | Error Recovery | `error_recovery.rs` |
| Accessibility (WCAG AA) | `accessibility.rs` | Perf Profiler | `profiler.rs` |

---

## 记忆系统 (61 文件 · 32K 行)

### 五层记忆架构

```
L0 Identity   — 用户身份，跨会话不变
L1 Essential  — 15条热记忆 + 5个代码热符号槽位
L2 Project    — 项目上下文 (~3000 token) + tree-sitter 代码图谱
L3 Deep Recall — SQLite FTS5 全文搜索 + 符号↔对话关联
L4 Shared     — 团队共享知识
```

### 代码图谱 (NEW — tree-sitter 嵌入式引擎)

- **5 语言解析**: Rust, Python, TypeScript, Go, Java
- **符号提取**: 函数/类/方法/接口/结构体/枚举
- **关系图谱**: 调用/导入/继承/实现边
- **FTS5 全文搜索**: 符号名即时查找
- **增量索引**: 文件指纹 + 变更检测
- **上下文注入**: LLM 调用前自动注入相关代码符号 (-20~35% token)

### 其他记忆模块

| 模块 | 功能 | 模块 | 功能 |
|------|------|------|------|
| AAAK 压缩 | 符号化压缩 70-85% | Closet | 指针索引快速路由 |
| Background Extractor | 实体+关系自动提取 | Temporal Graph | 时序知识图谱 |
| Entity Registry | 实体消歧合并 | Context Fence | 规则过滤 |
| Fact Checker | 事实一致性验证 | Drift | 陈旧度检测 |
| Coherence | 记忆一致性 | Write Guard | 写保护+审计 |

---

## 运行时核心 (81 文件 · 45K 行)

| 类别 | 模块 |
|------|------|
| **会话** | session, session_control, conversation, cached_prompt, summary_compression |
| **权限** | permissions, approval_gate, permission_enforcer, policy_engine |
| **MCP** | mcp_server, mcp_client, mcp_stdio, mcp_tool_bridge, mcp_lifecycle_hardened |
| **工具** | tool_orchestrator, tool_dispatch, subagent_executor, task_graph |
| **Gates** | gates (PreFlight/Revision/Escalation/Abort + Impact Analysis) |
| **Wave** | wave (依赖图+并行编排引擎) |
| **平台** | platform (API Server/飞书/企微/邮件) |
| **Agent** | agent, worker_boot, task_registry, team_cron_registry |
| **其他** | sandbox, effect, bus, mirror, pairing, profile, recovery_recipes |

---

## Provider 支持

| Provider | 方式 |
|----------|------|
| Anthropic (Claude) | 原生 · OAuth + API Key · Prompt Caching |
| OpenAI 兼容 | GPT / OpenRouter / Ollama / StepFun |
| DeepSeek | reasoning_content 回传 |
| Qwen / DashScope | 前缀路由 |
| Grok / xAI | 模型别名 |
| Kimi / Moonshot | 前缀路由 |
| 自定义 | config.yaml 任意 OpenAI 兼容接口 |

---

## 快速开始

```bash
# 编译
cargo build --release

# TUI 终端模式（默认）
cowd

# 单次问答
cowd prompt "解释这个项目"

# 指定模型
cowd --model deepseek-v4-pro "写一个排序函数"

# 启动 Web 服务
cowd serve --port 8080

# 继续会话
cowd --resume latest
```

---

## 开发

```bash
# 测试
cargo test -p cowd-cli     # 812 tests
cargo test -p cowd-memory  # 447 tests
cargo test --workspace     # 500+ tests

# 编译
cargo build --release      # → target/release/cowd (~28MB)
```

---

## 许可证

MIT License
