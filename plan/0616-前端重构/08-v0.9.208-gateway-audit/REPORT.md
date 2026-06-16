# v0.9.208 Gateway + Audit 验收报告

## 实现结果

本版本将 Gateway 与 Audit 从右侧标签式面板推进为左侧 rail 驱动的全页面工作台。

- Gateway 页面使用 `workbench-page-gateway` 独立布局，连接器控制台、跨平面控制、微信 iLink 状态和动作编排区域可以并列扫描。
- Audit 页面使用 `workbench-page-audit` 独立布局，筛选、统计指标、审计记录形成页面级操作区。
- 保留原有 Gateway、Audit renderer 与 API 行为，只补充页面布局类和视觉容器约束。
- Playwright 增加 Gateway、Audit 数据 mock 与页面断言，确保 rail 路由、标题、核心内容、截图输出均生效。

## TDD 门禁

- `cd webui && npm test`：通过，81 项。
- `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js`：通过，7 项。

## 视觉验收

截图已保存：

- `plan/0616-前端重构/screenshots/v0.9.208-gateway-desktop.png`
- `plan/0616-前端重构/screenshots/v0.9.208-audit-desktop.png`

目检结论：

- Gateway 和 Audit 均进入独立工作台页面，主内容不再被旧 tab 宽度压缩。
- 控件、按钮、输入框没有横向溢出。
- Audit 记录卡片与统计条可扫描。
- Gateway 在空连接器数据下左侧存在较大留白，属于空态密度问题，纳入 v0.9.210 polish 统一处理。

## 回溯

本版本达成 v0.9.208 计划目标，可以进入 v0.9.209 IACC 应用页面重构。
