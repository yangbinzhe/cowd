# v0.9.204 Workspace 完整页面计划

## 目标

将 Workspace 从旧右侧 panel 结构升级为适合完整页面的文件管理工作台：

1. 顶部路径、操作按钮、说明清晰。
2. 文件列表使用宽屏列表布局。
3. 文件/目录行具备清晰类型、名称、路径信息。
4. 仍兼容旧 right panel 挂载。

## 代码范围

- `webui/workspace.js`
- `webui/style.css`
- `webui/webui-shell.e2e.spec.js`

## TDD 设计

1. e2e 打开 Workspace workbench。
2. 验证 `.workspace-page`、`.workspace-file-list` 可见。
3. 验证文件和目录行存在。
4. 保存 desktop/mobile 截图。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.204-workspace-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.204-workspace-mobile.png`

