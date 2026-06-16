# v0.9.205 Runtime + Context 完整页面计划

## 目标

将 Runtime 与 Context 从窄面板形态升级为页面级内核状态工作台：

1. Runtime 使用多列网格展示 state、tasks、approvals、runs、timeline、context、maintenance。
2. Context 使用页面级 section 展示 selected、omitted、segments、timeline、runs。
3. 保留现有 API 和 renderer，优先通过 workbench 页面类和 CSS 提升布局。
4. 新增 Runtime/Context 专属 e2e 和截图。

## TDD 设计

1. e2e 打开 Runtime，验证 `workbench-page-runtime` 和 Runtime Console 内容。
2. e2e 打开 Context，验证 `workbench-page-context` 和 Context Runtime 内容。
3. 保存 desktop/mobile 截图。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.205-runtime-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.205-context-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.205-runtime-mobile.png`

