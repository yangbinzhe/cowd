# Interactive 场景目录

本目录是人工或 nightly 诊断工具，不属于默认发布门禁。真实发布信心来自：

- `scripts/validate.sh contract`
- `scripts/validate.sh serial-global`
- `scripts/validate.sh scenario`
- `scripts/validate.sh surface`
- `scripts/validate.sh release`

## 场景分层

| 模块 | 目的 | 不重复的失败模式 |
| --- | --- | --- |
| `tui_basic` | 启动、输入、滚动、搜索 | 终端基础交互失效 |
| `tui_interact` | 快捷键、命令面板、历史、弹层 | 键盘操作与弹层状态失效 |
| `tui_gateway` | Gateway 状态面板 | TUI 无法呈现 Gateway 状态 |
| `tui_memory` | 记忆面板与命令 | 记忆投影不可见 |
| `tui_skills` | Skill 面板与导航 | Skill 投影不可达 |
| `tui_session_sidebar` | 会话侧栏 | 当前会话不可见或不可切换 |
| `tui_all_panels` | 人工遍历全部二级面板 | 面板可达性与终端布局回归 |
| `server_core` | health、session、memory、config | Gateway 核心诊断 |
| `server_gateway_api` | memory/tool/config API | API 诊断 |
| `server_gateway_cmd` | CLI Gateway 生命周期 | 启停与状态命令失效 |
| `server_send_message` | session 创建和消息 | 消息入口与持久会话失效 |
| `cross_cut` | TUI 发起、API 观察、真实回复 | 同一会话跨 Surface 不收敛 |

## 准入规则

新增场景必须满足：

1. 写清楚它独有的失败模式；已有默认门禁覆盖时不得再新增。
2. HTTP 必须检查状态和结构，不能只把请求成功当作业务成功。
3. TUI 等待超时必须失败，不允许 `.ok()` 吞错。
4. 只能清理自身创建的进程、端口和临时目录。
5. LLM judge 只能作为质量评价，不能替代确定性的协议、状态和持久化断言。
6. 平台 live 收发应进入对应 Edge/Surface 的真实连接测试，不能用源码文件存在代替。

## 使用

```bash
cd tests/interactive
cargo run -- --list
cargo run -- --scenarios tui_basic,cross_cut
```
