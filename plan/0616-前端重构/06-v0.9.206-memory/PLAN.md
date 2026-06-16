# v0.9.206 Memory 完整页面计划

## 目标

将 Memory 从窄面板升级为完整记忆管理页面：

1. 状态、指标、维护、实体、关系、层级使用页面级卡片布局。
2. 保留现有 memory API 与 renderer。
3. 增加 Memory e2e 和截图。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.206-memory-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.206-memory-mobile.png`

