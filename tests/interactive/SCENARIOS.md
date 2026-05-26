# 交互式测试场景全集

## TUI 测试 (18 项)

| # | 场景 | 操作 | 验证 |
|---|------|------|------|
| 1 | 启动 | cowd 启动 | 出现 COWD logo |
| 2 | 输入+发送 | 输入文字按 Enter | 消息出现在 timeline |
| 3 | 滚动(PgUp/PgDn) | 多条消息后按 PgUp | 滚动偏移变化 |
| 4 | 展开/折叠 | Enter 切换 | 折叠条目标题变化 |
| 5 | 搜索 | `/` 输入关键词 Enter | 匹配条目被高亮 |
| 6 | 侧边栏标签切换 | Tab 循环 | 标签高亮切换 |
| 7 | Which-Key | 按 Space | 快捷键面板弹出 |
| 8 | 命令面板 Ctrl+P | Ctrl+P | 搜索框弹出 |
| 9 | Toast 通知 | 触发复制失败 | 右上角提示 |
| 10 | 状态栏信息 | 观察 | 显示模型/Token |
| 11 | 输入历史 Alt+↑/↓ | 输入后 Alt+↑ | 历史内容恢复 |
| 12 | 消息菜单 Ctrl+O | Ctrl+O | 菜单弹出 |
| 13 | 会话 Fork | f 键 | Fork 对话框弹出 |
| 14 | Export 导出 | Space e | 导出选项 |
| 15 | 主题切换 Ctrl+T | Ctrl+T | 颜色变化 |
| 16 | 模型切换 Ctrl+M | Ctrl+M | 状态栏模型变化 |
| 17 | 自动补全 Tab | 输入 `/` 后 Tab | 命令列表弹出 |
| 18 | 多行输入 Shift+Enter | Shift+Enter | 换行显示 |

## Server 测试 (12 项)

| # | 场景 | 操作 | 验证 |
|---|------|------|------|
| 19 | Health | GET /health | 200 OK |
| 20 | 创建会话 | POST /api/sessions | 返回 session_id |
| 21 | 列出会话 | GET /api/sessions | 列表含新会话 |
| 22 | 发送消息 | POST /api/sessions/:id/messages | 消息被接收 |
| 23 | 获取消息 | GET /api/sessions/:id/messages | 返回消息列表 |
| 24 | 记忆搜索 | GET /api/memory/search?q=xxx | 返回匹配 |
| 25 | 配置读取 | GET /api/config | 返回配置 JSON |
| 26 | 工作区文件 | GET /api/workspace/files | 返回文件列表 |
| 27 | 平台列表 | GET /api/platforms | 返回平台列表 |
| 28 | 命令执行 | POST /api/commands/execute | 命令输出 |
| 29 | 健康状态 | GET /v1/system/status | 返回状态 |
| 30 | 审批状态 | GET /api/approval/config | 返回审批配置 |

## 交叉测试 (10 项)

| # | 场景 | 操作 | 验证 |
|---|------|------|------|
| 31 | TUI发送→API验证 | TUI 发消息，API 查会话 | API 能查到会话和消息 |
| 32 | API创建→TUI验证 | API 创建会话，TUI 能恢复 | TUI 列表含新会话 |
| 33 | TUI记忆→API搜索 | TUI 对话中触发记忆 | API 搜索到记忆条目 |
| 34 | API写入记忆→TUI读取 | API 写记忆，TUI 查询 | TUI 注入该记忆 |
| 35 | TUI审批→API状态 | TUI 触发审批，API 查待审批 | API 返回 pending 状态 |
| 36 | TUI搜索→API验证 | TUI /search，API 查历史 | API 记忆搜索匹配 |
| 37 | 会话压缩 | TUI /compact，API 验证 | API 压缩后消息数减少 |
| 38 | TUI 配置修改 | TUI /config，API 读配置 | API 返回更新后配置 |
| 39 | 工作区文件同步 | TUI 侧边栏文件，API 文件列表 | 两者匹配 |
| 40 | 端到端对话 | TUI 完整对话→API 获取消息 | API 返回全部对话 |

总计：40 个测试场景
