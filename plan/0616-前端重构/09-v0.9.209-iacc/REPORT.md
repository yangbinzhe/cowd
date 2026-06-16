# v0.9.209 IACC 应用工作台验收报告

## 实现结果

本版本将 IACC 页面升级为制造领域上层应用工作台，并在页面结构上强调 cowd 内核能力作为底座。

- IACC 应用描述展示应用 ID、应用层级、制造域和 cowd 能力引用。
- IACC 健康区展示 schema、cockpit、quality gate、execution、attention 指标。
- 结构化数据区展示 sources、facts、evidence、watermarks，确认制造数据使用 `/api/cowd/structured/*` 内核路由。
- Ingest Plan 保留规划入口。
- Report State 保留 cockpit report、delivery state、receipt、retry 能力。
- Shell e2e 补齐 IACC、结构化数据、report delivery mock，覆盖桌面和移动端截图。

## TDD 门禁

- `cd webui && npm test`：通过，81 项。
- `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js`：通过，8 项。

## 视觉验收

截图已保存：

- `plan/0616-前端重构/screenshots/v0.9.209-iacc-desktop.png`
- `plan/0616-前端重构/screenshots/v0.9.209-iacc-mobile.png`

目检结论：

- 桌面端 IACC 已形成应用概览、健康门禁、结构化数据、报告交付的清晰层次。
- 移动端按钮和输入框按单列展示，没有横向溢出。
- 长指标文本在小卡片内会省略，未破坏布局；后续 v0.9.210 可进一步优化密度和指标显示策略。

## 回溯

本版本达成计划目标，可以进入 v0.9.210 Settings + 全局 polish。
