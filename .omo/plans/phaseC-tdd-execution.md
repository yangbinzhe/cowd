# Phase C: 回归验证 + 最终审计 v2 (Oracle修正)

> Oracle修正: C1补齐4crates, C2改为mock+清单, C3扩展到22项, C4修正引用
> 工期: 8h

## C1: 全量编译+测试 (1h)

```bash
cargo build --workspace
cargo test --workspace
# 逐crate确认:
cargo test -p cowd-memory    # 490+ pass
cargo test -p runtime        # 1005 pass
cargo test -p cowd-cli       # 980 pass
cargo test -p tools          # 109 pass
cargo test -p api            # 124 pass
cargo test -p commands       # 51 pass
cargo test -p plugins        # 39 pass
cargo test -p config         # 配置验证
cargo test -p telemetry      # 遥测
```

## C2: Mock协议测试 + 手动验证清单 (3h)

### Mock单元测试（可自动化）

```rust
// crates/cowd-cli/tests/daemon_protocol_tests.rs

#[test]
fn test_create_session_json_roundtrip() { /* serde验证 */ }
#[test]
fn test_chat_stream_json_roundtrip() { /* serde验证 */ }
#[test]
fn test_parse_textdelta_from_json() { /* CowdEvent解析 */ }
#[test]
fn test_parse_turncomplete_from_json() { /* CowdEvent解析 */ }
```

### 手动验证清单（非TDD，人工确认）

- [ ] `cowd serve &` — daemon启动正常
- [ ] `curl :8642/health` → OK
- [ ] `cowd --solo` — TUI启动正常
- [ ] TUI发送消息 → 流式输出
- [ ] `/memory` 面板 → L0-L4层统计
- [ ] `/mcp` 面板 → 工具列表
- [ ] `cowd --resume latest` → 会话恢复

## C3: 能力完整性矩阵 (1h) — 22项

来自README架构: 10组记忆系统 + 5运行时子系统 + 7接入层

| 组 | 能力 | 验证 |
|----|------|------|
| 调度中枢 | CognitiveContextManager | daemon全局单例 |
| 调度中枢 | MemoryOrchestrator | 内存CRUD正常 |
| 范围隔离 | ProjectScopeManager | 项目scope过滤 |
| 代码智能 | CodeIndexer+ProjectKG+HotSymbols | tree-sitter索引 |
| 检索引擎 | FTS5+BM25+FreshContext+Relevance | 搜索响应 |
| 知识提取 | Extractor+Miner+ToolSandbox | on_turn_end提取 |
| 共享层 | SharedMemoryManager L4 | 团队记忆 |
| 审计控制 | VerbatimSink+WriteGuard+Drift+ContextRot | 漂移检测 |
| 重建恢复 | StateRebuilder+Handoff+Seeds | 会话恢复 |
| 压缩路由 | AAAK+Closet+CompressionPipeline | 3级压缩 |
| 一致性 | FactChecker+Coherence+EntityRegistry+ContextFence | 矛盾检测 |
| 运行时 | Wave引擎 | 任务分解 |
| 运行时 | SubAgent | 子Agent执行 |
| 运行时 | Gate流水线(5Gate) | 工具权限 |
| 运行时 | PolicyEngine | 策略规则 |
| 运行时 | Provider适配 | LLM API |
| 接入层 | TUI(9面板) | 渲染正常 |
| 接入层 | HTTP API :8642 | 健康检查 |
| 接入层 | WebUI | 静态服务 |
| 接入层 | 平台适配(飞书/企微/邮件) | 日志确认 |
| 接入层 | SSE实时推送 | EventBus→SessionEventBus |
| 接入层 | MCP协议(Stdio/SSE/Remote) | 工具列表 |
| 接入层 | Session持久化(SQLite) | session恢复 |

## C4: Oracle最终审计 (3h)

提交Oracle验证:
- Phase A: A1-A4 共4项 deliverable
- Phase B: B1+B2a 共2项 deliverable
- C1-C3 全部通过
- 代码与计划一致性
- 22项能力无损

## 验证

```bash
cargo build --workspace && cargo test --workspace
cargo test daemon_protocol
```
