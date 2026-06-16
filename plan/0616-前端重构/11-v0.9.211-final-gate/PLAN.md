# v0.9.211 最终门禁计划

## 目标

对 0616 前端重构进行最终验收、审计和归档，确认 WebUI 已按规划从旧右侧 tab 面板升级为左侧 rail + 全页面工作台。

## 检查范围

1. 代码范围：
   - `webui/index.html`
   - `webui/ui.js`
   - `webui/style.css`
   - `webui/panels.js`
   - `webui/workspace.js`
   - `webui/boot.js`
   - `webui/webui-shell.e2e.spec.js`
   - `webui/modules.test.js`
2. 页面范围：
   - Chat
   - Workspace
   - Runtime
   - Context
   - Memory
   - Skills
   - Agents
   - Tools
   - Gateway
   - IACC
   - Audit
   - Settings
3. 证据范围：
   - 各版本 `PLAN.md`
   - 各版本 `REPORT.md`
   - `reports/v0.9.201` 到 `reports/v0.9.210`
   - `screenshots/` 下全部桌面与移动端截图

## TDD 门禁

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e` 通过。
3. 截图数量和关键页面覆盖完整。
4. `git status --short` 中只允许保留与本次 WebUI 无关的既有 TUI 未提交文件。

## 输出

1. `plan/0616-前端重构/11-v0.9.211-final-gate/REPORT.md`
2. `plan/0616-前端重构/reports/v0.9.211.md`
3. `plan/0616-前端重构/FINAL-REVIEW.md`

## 验收结论标准

- 通过：离线测试全绿，页面覆盖完整，无阻断级视觉缺陷。
- 有条件通过：离线测试全绿，但仍有非阻断优化项。
- 不通过：核心路由、页面渲染、响应式布局或测试证据缺失。
