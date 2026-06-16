# v0.9.210 Settings + 全局 Polish 计划

## 目标

补齐 Settings 工作台，并收敛前面版本暴露出的通用视觉问题。

1. Settings 页面形成外观、模型与 provider、profile、安全与本地状态四个管理区。
2. Provider 状态可以在 Settings 中直接查看，不再只藏在 Control Center JSON。
3. Profile 管理保留创建、切换、删除和 restart 提示。
4. 全局 polish 处理空态高度、长文本、工作台按钮和移动端单列密度。

## 实施范围

1. `Panels.renderSettings()` 增加 settings page class、provider summary、安全状态区。
2. `style.css` 增加 settings workbench 布局和通用 panel polish。
3. `webui-shell.e2e.spec.js` 增加 Settings mock、断言、桌面/移动截图。
4. 不变更后端接口，不新增设置写入行为。

## TDD 验收

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 页面断言：
   - `#workbench-content.workbench-page-settings` 可见。
   - 内容包含 `Theme`、`Default Model`、`Providers`、`Profiles`、`Security`。
   - 内容包含 `claude-sonnet-4-6`、`anthropic`、`enterprise_ops`。
4. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.210-settings-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.210-settings-mobile.png`

## Polish 验收

- 工作台空态不再制造大面积无意义留白。
- Settings 按钮、选择器、输入框在桌面和移动端不溢出。
- 长 provider/profile 文本可换行或省略，不破坏卡片。
