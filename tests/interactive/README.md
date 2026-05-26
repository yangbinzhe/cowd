# Cowd Interactive Test Framework

TUI + Server 交叉交互测试框架，使用 tmux 控制终端 UI、curl 调用 HTTP API、大模型辅助生成用例和实时判断。

## 设计目标

- 覆盖 TUI 交互、Server API、交叉场景的全部核心功能
- 测试不参与 `cargo build` 编译（独立 workspace）
- 大模型深度介入：生成测试对话、实时判断输出、分析测试结果
- 脚本化自动化运行，输出自我审查报告

## 架构

```
tests/interactive/
├── Cargo.toml              ← 独立 workspace (不加入主 workspace)
├── README.md
├── src/
│   ├── main.rs             ← 入口
│   ├── tui.rs              ← tmux TUI 控制层
│   ├── api.rs              ← curl API 控制层
│   ├── server.rs           ← Server 进程生命周期
│   ├── reporter.rs         ← 测试报告 + 统计
│   ├── llm.rs              ← 大模型集成 (生成/判断/分析)
│   └── scenarios/
│       ├── mod.rs          ← 场景注册 (19 场景)
│       ├── tui_basic.rs    ← TUI 基础 (启动/对话/滚动/搜索)
│       ├── tui_interact.rs ← TUI 交互 (WhichKey/面板/历史/Toast)
│       ├── server_core.rs  ← Server 核心 (Health/会话/记忆)
│       ├── server_mgmt.rs  ← Server 管理 (工作区/平台/审批)
│       └── cross_cut.rs    ← 交叉测试 (TUI↔Server 双向验证)
└── SCENARIOS.md            ← 完整场景计划
```

## 环境要求

| 依赖 | 用途 | 备注 |
|------|------|------|
| `cowd` 二进制 | 被测试对象 | 通过 `COWD_BIN` 环境变量指定路径 |
| `tmux` | TUI 终端控制 | 发送按键、捕获屏幕、等待内容 |
| `curl` | HTTP API 调用 | Server 端测试 |
| `ANTHROPIC_API_KEY` (可选) | 大模型深度分析 | 设置后用于生成/判断/分析 |
| `Ollama` (可选) | 本地大模型替代 | http://localhost:11434 |

## 大模型深度集成

LLM 在三个层面介入测试流程：

### 1. 测试用例生成 (LLM 生成对话内容)

```rust
// 不再使用固定文本，LLM 生成上下文相关的测试对话
let conversation = llm::generate_conversation("file operations")?;
// → "Write a Rust function to read a CSV file"
// → 基于被测试能力动态生成，更真实
```

### 2. 实时输出判断 (LLM 验证输出质量)

```rust
// 不再只检查关键词，LLM 判断输出的语义正确性
let capture = tui.capture()?;
llm::validate_output(&capture, "contains assistant response")?;
// ✓ "好的，我来帮你..."
// ✗ "Error: connection refused"
```

### 3. 测试结果分析 (LLM 生成报告建议)

```rust
// 测试结束后，LLM 分析失败模式并给出修复建议
// 通过 ANTHROPIC_API_KEY 或本地 Ollama 自动触发
```

## 使用方法

```bash
# 设置 cowd 二进制路径
export COWD_BIN=/path/to/target/release/cowd

# 列出所有可用场景
cd tests/interactive && cargo run -- --list

# 运行单个场景
cargo run -- tui_startup

# 运行多个场景
cargo run -- --scenarios tui_basic,cross_cut

# 运行全部场景
cargo run -- --all

# 启用 LLM 深度分析
export ANTHROPIC_API_KEY=sk-...
cargo run -- tui_basic

# 使用本地 Ollama
# (无需设置 API KEY，自动检测 localhost:11434)
cargo run -- --all
```

## 场景清单

| 场景 | 类型 | 覆盖功能 | 用时 |
|------|------|----------|------|
| `tui_startup` | TUI | 启动标志、状态栏 | ~10s |
| `tui_chat` | TUI | 消息发送、流式回复 | ~60s |
| `tui_scroll_expand` | TUI | PgUp/PgDn、展开/折叠 | ~90s |
| `tui_search` | TUI | / 搜索高亮 | ~2s |
| `tui_sidebar_tabs` | TUI | Tab 切换侧边栏 | ~2s |
| `tui_whichkey` | TUI | Space 快捷键面板 | ~2s |
| `tui_cmd_palette` | TUI | Ctrl+P 命令面板 | ~2s |
| `tui_history` | TUI | Alt+↑ 输入历史 | ~5s |
| `tui_toast` | TUI | 通知触发 | ~2s |
| `tui_fork_export` | TUI | 会话 Fork + 导出 | ~3s |
| `tui_multi_input` | TUI | Shift+Enter + 主题切换 | ~3s |
| `server_health` | Server | Health、会话 CRUD | ~2s |
| `server_memory` | Server | 记忆搜索、配置 | ~2s |
| `server_workspace` | Server | 工作区文件、命令执行 | ~2s |
| `server_platform` | Server | 平台列表、审批配置 | ~2s |
| `cross_session_api` | Cross | TUI 发送→API 读取 | ~10s |
| `cross_memory` | Cross | TUI 触发记忆→API 搜索 | ~5s |
| `cross_approval` | Cross | TUI 审批→API 待审批 | ~2s |
| `cross_e2e` | Cross | 端到端完整对话 | ~120s |

## 添加新场景

1. 在 `src/scenarios/` 下新建文件
2. 实现 `has_scenario(name) -> bool` + `run(runner) -> Result<()>`
3. 在 `src/scenarios/mod.rs` 中注册
4. 在 `list()` 中添加描述

## 测试报告示例

```
═══ Test Report ═══
  Duration:     45.2s
  Total:        12
  Passed:       11
  Failed:       1
  Pass rate:    92%
  ─────────────────
  TUI tests:    6
  Server tests: 4
  Cross tests:  2
  FAILURES:
    ❌ cross_e2e: Timeout waiting for 'assistant'
  RECOMMENDATIONS:
    🟡 Warning: 8% failure rate. Minor issues detected.
    🔄 Cross-test failures detected. TUI↔Server sync may be broken.
  VERDICT: ❌ FAIL (1/12 failed)

── LLM Analysis ──
- The cross_e2e timeout suggests the TUI turn thread
  may not be completing within the expected window.
- Check that 'cowd' binary actually produces streaming output.
- Server API connectivity confirmed (server_health passed).
```

## 不参与编译的保证

`Cargo.toml` 包含独立的 `[workspace]` 声明，不在主 Cargo.toml 的 `members` 列表中。

```bash
# 只有主动 cd tests/interactive 才会编译这个 crate
cd tests/interactive && cargo build    # ✅ 单独编译
cargo build --workspace                # ❌ 不包含，不会编译
```
