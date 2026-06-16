# v0.9.209 IACC 应用工作台计划

## 目标

将 IACC 从旧面板堆叠升级为制造领域上层应用工作台，同时明确 cowd 内核数据能力是底座：

1. 应用概览：展示 IACC 应用边界、制造域、cowd 能力引用。
2. 健康门禁：展示 schema、cockpit、quality gate、execution、attention 指标。
3. 结构化数据内核：展示 sources、facts、evidence、watermarks，并保留 ingest plan。
4. 报告交付：展示 cockpit report 状态、回执、retry 能力。

## 实施范围

1. 不新增后端 API，不变更 IACC renderer 的数据合同。
2. 为 IACC mount 增加页面级 class，避免旧右侧面板布局继续影响全屏工作台。
3. 补齐 shell e2e mock，使 IACC 能在独立测试中稳定渲染应用、健康、结构化数据和报告交付。
4. 保存桌面和移动端截图。

## TDD 验收

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 页面断言：
   - `#workbench-content.workbench-page-iacc` 可见。
   - 内容包含 `IACC Workbench`、`iacc.manufacturing`、`cowd.structured_data.core`、`inventory_balance`、`dry_run_planned`。
4. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.209-iacc-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.209-iacc-mobile.png`

## 风险与控制

- 风险：IACC 内部仍复用 `panel-section`，容易出现嵌套卡片感。
- 控制：本版本只做页面骨架和关键视觉约束，末端密度、空态和统一卡片语言放入 v0.9.210 polish。
