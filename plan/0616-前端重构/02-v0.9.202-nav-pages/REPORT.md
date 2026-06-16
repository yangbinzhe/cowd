# v0.9.202 Nav Rail 与完整页面路由执行报告

执行时间：2026-06-16 08:39 Asia/Shanghai

## 目标回溯

本阶段目标：

- rail 点击进入中央完整页面。
- 保留旧 right panel 兼容入口。
- 复用旧 `Panels.renderXxx()` / `Workspace.render()`，不在本阶段重写业务页面。
- 输出 workbench desktop/mobile 截图。

结论：已完成。

## 代码改动

- `webui/index.html`
  - 新增 `#chat-view`。
  - 新增 `#workbench-view`。
  - 新增 `#workbench-header`、`#workbench-title`、`#workbench-subtitle`。
  - 新增 `#workbench-content`。
- `webui/style.css`
  - 新增 `.view-page`、`#workbench-view`、`#workbench-header`、`.workbench-content`。
  - 新增 `#app-shell.workbench-mode #sidebar{display:none}`，非 Chat 页面隐藏 session sidebar。
  - 补移动端 workbench 布局。
- `webui/ui.js`
  - 新增 `UI.switchView()`。
  - 新增 renderer 临时挂载机制：`UI.$('panel-content')` 在 workbench 渲染期间指向 `#workbench-content`。
  - 新增 workbench 标题元数据。
- `webui/boot.js`
  - rail 点击切换为 `UI.switchView()`。
  - 新增 Back to Chat 按钮绑定。
  - 记录 `cowd-active-view`。
- `webui/modules.test.js`
  - 增加 `#chat-view`、`#workbench-view`、`#workbench-content` DOM 合约。
- `webui/webui-shell.e2e.spec.js`
  - 更新为 v0.9.202 页面路由门禁。
  - 验证 rail Workspace 不再打开右 panel，而是进入中央 workbench。
  - 验证旧 `#btn-toggle-panel` 兼容入口仍可打开右 panel。

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

## 截图

- `plan/0616-前端重构/screenshots/v0.9.202-workbench-desktop.png`
- `plan/0616-前端重构/screenshots/v0.9.202-workbench-mobile.png`

## 视觉检查

通过截图确认：

- Desktop 下 workbench 占据完整主区域，只保留左侧 rail。
- Mobile 下 session sidebar 不再遮挡 workbench。
- Workspace 内容已经从旧右栏渲染到中央页面。
- Back to Chat 可见且可操作。

视觉测试中发现并修复：

- 首次截图中移动端 session sidebar 遮挡 workbench；已通过 `workbench-mode` 隐藏。

## 未完成项

- 业务页面内容仍是旧 panel renderer 的宽屏承载版，尚未按每个模块重排。
- 旧 `#right-panel` 和横向 `#panel-tabs` 仍保留，后续迁移完 e2e 后再清理。
- rail 仍使用字母占位，正式 icon 化留到后续视觉 polish。

## 下一步

进入 v0.9.203：

- Chat 页面视觉重塑。
- 工具/thinking 状态降噪。
- composer 与 header 重新排版。
- 修复旧 toggle 按钮实体显示问题。

