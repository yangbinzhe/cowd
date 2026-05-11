# Cowd — AI 编程智能体框架

Rust 实现的高性能 AI 编程助手，支持 CLI / TUI / WebUI 三种交互模式。

## 快速开始

```bash
# 编译
cargo build --release

# CLI 交互模式
cowd

# TUI 全屏终端模式
cowd --tui

# 启动 Web 服务
cowd serve --port 8080

# 非交互式单次提问
cowd prompt "解释这个项目"

# 指定模型
cowd --model deepseek-v4-pro "写一个排序函数"
```

## 架构

```
crates/
├── api/           模型适配层 (Anthropic/OpenAI/DeepSeek/Qwen/Grok/Moonshot)
├── runtime/       运行时核心 (会话管理/配置/工具执行/MCP/平台)
├── tools/         20+ 内置工具 (bash/file/grep/glob/lsp/task/agent/cron)
├── commands/      技能系统 + 插件管理 + 斜杠命令
├── memory/        3层记忆系统 (L0身份/L1核心/L3深度召回)
├── memory-light/  轻量记忆提取器
├── plugins/       插件注册与生命周期
├── config/        统一配置管理
├── session-store/ SQLite 会话持久化
├── telemetry/     遥测事件追踪
├── rusty-claude-cli/  主程序入口 (CLI/TUI/Server)
└── compat-harness/    兼容性测试套件
```

## 核心能力矩阵

### 模型与 Provider

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| Anthropic (Claude) | ✅ | ✅ 状态栏 | ✅ 模型选择器 | OAuth + API Key |
| OpenAI 兼容 | ✅ | ✅ | ✅ | GPT/OpenRouter/Ollama |
| DeepSeek | ✅ | ✅ | ✅ | 内置路由 + reasoning_content 回传 |
| Qwen/DashScope | ✅ | ✅ | ✅ | qwen/* 前缀自动路由 |
| Grok/xAI | ✅ | ✅ | ✅ | 模型别名 |
| Kimi/Moonshot | ✅ | ✅ | ✅ | kimi/* 前缀 |
| Provider 链 | ✅ | ❌ | ❌ | 多 Provider 故障转移/负载均衡 |
| 自定义 Provider | ✅ | ❌ | ❌ | config.yaml 中配置 |

### Agent 编排

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| 对话循环 | ✅ | ✅ Chat面板 | ✅ 消息流 | 多轮对话 + 流式响应 |
| 工具调用 | ✅ 20+工具 | ✅ ToolCard | ✅ ToolCard | 展开/折叠 + 退出码 |
| 审批流 | ✅ SmartApprovalGate | ✅ Y/N模态 | ✅ 审批卡片 | 危险操作拦截 |
| YOLO 模式 | ✅ | ✅ 斜杠命令 | ✅ 斜杠命令 | 跳过所有审批 |
| 子 Agent 委派 | ✅ task/team/cron | ✅ Delegate面板 | ❌ | 并行任务执行 |
| 斜杠命令 | ✅ 67+ | ✅ | ✅ 自动补全 | /help /resume /compact 等 |
| 输出截断 | ✅ 16KB bash / 10MB文件 | ✅ | ✅ | 防止 token 爆炸 |

### 记忆系统

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| L0 身份层 | ✅ | ❌ | ❌ | 持久化身份信息 |
| L1 核心层 | ✅ | ❌ | ✅ Memory面板 | 近频高优先级记忆 |
| L3 深度召回 | ✅ | ❌ | ❌ | 全文搜索历史记忆 |
| 自动提取 | ✅ | ❌ | ❌ | 后台异步零 token 消耗 |
| 知识图谱三元组 | ✅ | ❌ | ❌ | 实体关系抽取 |
| 实体检测 | ✅ | ❌ | ❌ | 命名实体识别 |
| 记忆搜索 | ✅ | ❌ | ✅ WebUI | FTS5 全文搜索 |

### 上下文管理

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| 自动压缩 | ✅ threshold/buffer | ❌ /compact命令可用 | ⚠️ 仅API | 上下文80%触发 |
| Prompt 缓存 | ✅ | ❌ | ❌ | Anthropic 兼容 |
| Token 计数 | ✅ UsageTracker | ✅ 状态栏 | ✅ 状态栏 | 实时累计 |
| 成本估算 | ✅ estimate_cost_usd | ✅ 状态栏 | ❌ | 美元计价 |

### 技能系统

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| 技能安装/卸载 | ✅ | ❌ | ✅ Skills面板 | marketplace |
| 技能安全扫描 | ✅ 注入检测 | ❌ | ❌ | 代码安全检查 |
| 斜杠命令注册 | ✅ | ✅ | ❌ | /skills 等 |

### 平台与 Gateway

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| 飞书机器人 | ✅ 完整适配 | ✅ Gateway面板 | ❌ | 文档/评论/规则引擎 |
| 企业微信 | ✅ | ✅ Gateway面板 | ❌ | WeCom 适配 |
| 邮件 | ✅ SMTP/IMAP | ✅ Gateway面板 | ❌ | 邮件收发 |
| API Server | ✅ | ✅ | ✅ | REST API + WebUI |
| 跨平台会话 | ✅ SessionType | ✅ Gateway面板 | ❌ | 统一会话管理 |

### MCP 协议

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| MCP 服务器生命周期 | ✅ stdio/HTTP | ❌ | ❌ | 启动/停止/配置 |
| MCP 工具桥接 | ✅ | ❌ | ❌ | 工具发现与调用 |
| 资源读取 | ✅ | ❌ | ❌ | MCP resource 读取 |

### 插件系统

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| 插件注册 | ✅ PluginRegistry | ❌ | ❌ | 安装/卸载/启用 |
| 生命周期钩子 | ✅ init/shutdown | ❌ | ❌ | 前后工具钩子 |
| 市场 | ✅ | ❌ | ❌ | 发现和安装 |

### 开发与工具

| 能力 | 后端 | TUI | WebUI | 说明 |
|---|---|---|---|---|
| 文件读写 | ✅ read/write/edit | ✅ 文件浏览器面板 | ✅ 文件面板 | 含图片预览 |
| Bash 执行 | ✅ 沙箱模式 | ✅ ToolCard | ❌ | 16KB输出截断 |
| LSP 集成 | ✅ 7种操作 | ❌ | ❌ | 诊断/跳转/补全 |
| Git 操作 | ✅ commit/diff/pr | ❌ | ❌ | 品质门禁 |
| Web 搜索 | ✅ | ❌ | ❌ | URL抓取+搜索 |
| TODO 管理 | ✅ | ❌ | ❌ | 任务持久化 |

## 配置参考

配置文件位置（优先级从高到低）：
1. 命令行参数 `--model` `--permission-mode` 等
2. 环境变量 `COWD_MODEL` `ANTHROPIC_API_KEY` 等
3. `.cowd/config.local.yaml` (本地覆盖，git-ignored)
4. `.cowd/config.yaml` (项目级)
5. `~/.cowd/config.yaml` (用户级)

详见 `config-default.yaml` 包含所有可配置项的完整注释。

## 开发

```bash
# 运行测试（推荐逐 crate 运行，避免内存溢出）
cargo test -p api -p commands -p tools -p runtime -p plugins

# Clippy 检查
cargo clippy --workspace --all-targets

# 格式化
cargo fmt --all -- --check

# WebUI 测试
cd webui && npm test
```

## 测试状态

| Crate | 测试数 | 状态 |
|---|---|---|
| api | 129 | ✅ |
| tools | 101 | ✅ |
| commands | 51 | ✅ |
| runtime | ~550 | ✅ |
| plugins | 39 | ✅ |
| memory | ~190 | ✅ |
| config | 9 | ✅ |
| session-store | 10 | ⚠️ ignored (需FTS5) |
| WebUI | 11 | ✅ |

## 待接线（P1-P3）

| 优先级 | 能力 | 目标 |
|---|---|---|
| P1 | TUI 记忆搜索面板 | 查看/搜索 L0/L1/L3 记忆 |
| P1 | TUI 技能管理面板 | 安装/查看/卸载技能 |
| P1 | TUI MCP 状态面板 | 查看MCP服务器状态 |
| P2 | WebUI Agent 委派面板 | 子Agent进度显示 |
| P2 | TUI Prompt缓存状态 | 缓存命中率显示 |
| P3 | 全链路 Provider链 UI | 故障转移可视化 |

## 许可证

MIT
