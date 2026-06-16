# v0.9.208 Gateway + Audit 页面计划

## 目标

将 Gateway 与 Audit 迁移为页面级工作台：

1. Gateway：连接器、账号、能力、资源、回执可扫描。
2. Audit：审计筛选、指标、记录可扫描。
3. 保留现有 renderer 和 API。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.208-gateway-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.208-audit-desktop.png`

