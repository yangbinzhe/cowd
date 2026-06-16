# v0.9.202 Nav Rail 与完整页面路由计划

## 目标

将 v0.9.201 的左侧 rail 从“旧 panel 兼容入口”升级为真正的完整页面路由：

1. rail 点击非 Chat 能进入中央完整页面。
2. 原 `Panels.renderXxx()` 与 `Workspace.render()` 先复用，不在本阶段大规模重写。
3. 旧 `#right-panel`、`#panel-tabs`、`#btn-toggle-panel` 保留，作为兼容入口。
4. 新页面提供标题、说明、主内容区域，为后续各业务页重构提供承载层。

## 关键设计

新增页面容器：

- `#chat-view`：承载现有 chat header、messages、composer。
- `#workbench-view`：承载完整管理页面。
- `#workbench-title`：当前页面标题。
- `#workbench-subtitle`：当前页面说明。
- `#workbench-content`：复用旧 panel renderer 的主挂载点。

兼容策略：

- 旧 `UI.switchPanel(name)` 继续控制右侧 panel。
- 新 `UI.switchView(name)` 控制中央页面。
- `UI.$('panel-content')` 在 `switchView()` 渲染期间临时指向 `#workbench-content`，让旧 renderer 可以先复用。
- 旧 e2e 仍可通过 `#btn-toggle-panel` + `#panel-tabs` 访问右栏。

## TDD 设计

新增或更新测试：

1. DOM 测试：`index.html` 存在 `#chat-view`、`#workbench-view`、`#workbench-content`。
2. e2e：点击 rail Workspace 后：
   - `#workbench-view` 可见。
   - `#chat-view` 隐藏。
   - `#right-panel` 保持隐藏。
   - `#workbench-content` 出现 Workspace 内容。
3. e2e：点击 rail Chat 后恢复 chat。
4. e2e：旧 `#btn-toggle-panel` 仍能打开右侧 panel。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.202-workbench-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.202-workbench-mobile.png`
4. 报告保存：
   - `plan/0616-前端重构/02-v0.9.202-nav-pages/REPORT.md`
   - `plan/0616-前端重构/reports/v0.9.202.md`

