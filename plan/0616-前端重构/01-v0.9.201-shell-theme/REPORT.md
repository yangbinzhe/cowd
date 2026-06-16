# v0.9.201 Shell 与 Theme 基线执行报告

执行时间：2026-06-16 08:35 Asia/Shanghai

## 目标回溯

本阶段目标是建立 WebUI 重构的最小可用基础：

- 新增 app shell。
- 新增左侧竖向 primary nav rail。
- 新增 Graphite-like 视觉 token。
- 保留右侧 panel 兼容行为。
- 建立自动截图与报告归档流程。

结论：已完成。

## 代码改动

- `webui/index.html`
  - 新增 `#app-shell`。
  - 新增 `#nav-rail`。
  - 为主工作区增加 `main-workspace` 类。
  - 默认 skin 初始化为 `graphite`。
  - 版本号默认值从 `0.7.4` 修正为 `0.9.200`。
- `webui/style.css`
  - 新增 shell、rail、Graphite-like token。
  - 调整按钮、session active、user bubble 的基础视觉。
  - 移动端 rail 宽度调整。
- `webui/ui.js`
  - 新增 `UI.syncRail()`。
  - `UI.switchPanel()` 支持 `chat`/空值收起右 panel 并同步 rail。
- `webui/boot.js`
  - 绑定 rail 点击。
  - 应用 `cowd-skin`。
  - 从 `/api/config.version` 更新页面版本号。
- `webui/workspace.js`
  - 修复文件树图标错误显示 HTML 实体的问题。
- `webui/modules.test.js`
  - 增加 shell/nav rail DOM 合约测试。
- `webui/webui-shell.e2e.spec.js`
  - 新增 shell 视觉 e2e 与截图输出。

## 测试结果

命令：

```bash
cd webui && npm test
```

结果：

- 1 个 test file 通过。
- 80 个测试通过。

命令：

```bash
cd webui && npm run test:e2e -- webui-shell.e2e.spec.js
```

结果：

- 1 个 Playwright 测试通过。
- 已验证 app shell、nav rail、Graphite skin、版本号展示、workspace panel 兼容行为。

## 截图

- `plan/0616-前端重构/screenshots/v0.9.201-desktop.png`
- `plan/0616-前端重构/screenshots/v0.9.201-mobile.png`

## 视觉检查

通过截图确认：

- 左侧 rail 已稳定显示。
- Graphite-like 暗色基线已生效。
- Workspace 入口可通过 rail 打开旧 panel。
- Desktop 无明显重叠。
- Mobile 下 rail 保持可见，右 panel 作为覆盖层保留旧行为。

视觉测试中发现并修复：

- Workspace mock 形态错误导致 `items.forEach is not a function`。
- 文件树图标显示为 `&xodot;` 文本。
- 页脚版本号仍显示 `0.7.4`。

## 未完成项

- v0.9.201 仍保留右侧 `#right-panel` 和横向 `#panel-tabs`，这是兼容策略，不是最终形态。
- rail 当前使用字母占位，后续 v0.9.202/v0.9.210 应统一替换为正式 icon 系统。
- 移动端目前仍沿用旧右侧 panel 覆盖行为，完整页面化从 v0.9.202 开始。

## 下一步

进入 v0.9.202：

- 将 rail 从“兼容入口”升级为真正的 view router。
- 新增 `#page-host` 或等价主页面容器。
- 将旧 right panel 渲染内容逐步迁移到完整页面。
- 更新 e2e 从 `#panel-tabs` 迁移到 `data-view`。

