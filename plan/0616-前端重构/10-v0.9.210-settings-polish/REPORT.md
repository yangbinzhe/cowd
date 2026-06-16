# v0.9.210 Settings + 全局 Polish 验收报告

## 实现结果

本版本补齐 Settings 工作台，并对全局工作台空态和移动端密度做收敛。

- Settings 页面拆分为 Theme、Default Model、Providers、Profiles、Security。
- Provider 摘要从 `/api/config/providers` 读取，展示 provider 数量、模型数、路由解析状态和 provider 列表。
- Profile 管理保留创建、切换、删除、active 标识和 restart 提示。
- Security 区展示浏览器侧 auth token 存储状态、origin、theme 本地状态。
- 全局工作台空态 padding 收窄，减少 Gateway 等空数据页面的大面积无意义留白。
- e2e 增加 Settings 桌面与移动端截图。

## TDD 门禁

- `cd webui && npm test`：通过，81 项。
- `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js`：通过，9 项。

## 视觉验收

截图已保存：

- `plan/0616-前端重构/screenshots/v0.9.210-settings-desktop.png`
- `plan/0616-前端重构/screenshots/v0.9.210-settings-mobile.png`

目检结论：

- 桌面端 Settings 四块内容分区清晰，provider 和 profile 可扫描。
- 移动端输入框、按钮、列表均按单列展示，没有横向溢出。
- 长模型、origin 文本在指标卡中省略，布局稳定。

## 回溯

本版本达成计划目标，可以进入 v0.9.211 最终门禁和整体审计。
