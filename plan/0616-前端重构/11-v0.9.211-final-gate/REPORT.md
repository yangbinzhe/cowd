# v0.9.211 最终门禁报告

## 门禁结果

结论：通过。

本次最终门禁确认 0616 前端重构已完成左侧 rail + 全页面工作台的主目标，并保留旧右侧 panel 的兼容测试入口。

## 测试结果

- `cd webui && npm test`：通过，81 项。
- `cd webui && npm run test:e2e`：通过，14 项。

说明：

- `npm run test:e2e` 使用 `webui/playwright.config.js`，默认忽略 `*.live.e2e.spec.js`。
- 第一次完整 e2e 暴露 3 个旧测试选择器问题：旧测试使用 `[data-panel="runtime"]`，在新 rail 加入后会选中左侧导航而非右侧 panel tab。
- 已将离线旧测试改为 `#panel-tabs button[data-panel="..."]`，明确验证旧侧栏兼容入口，复测 14 项全绿。

## 截图证据

截图目录：`plan/0616-前端重构/screenshots`

当前共 22 张截图，覆盖：

- shell baseline：桌面、移动端
- workbench routing：桌面、移动端
- chat chrome：桌面、移动端
- workspace：桌面、移动端
- runtime/context
- memory：桌面、移动端
- skills/agents/tools
- gateway/audit
- iacc：桌面、移动端
- settings：桌面、移动端

## 代码审计

已检查的关键点：

- `ui.js`：`switchView()` 和 `renderPanelToWorkbench()` 负责新页面路由，旧 `switchPanel()` 保留，兼容右侧 panel。
- `index.html`：左侧 rail 和 workbench 主视图存在，旧右侧 panel 仍可通过 chat header 进入。
- `style.css`：各能力页已有独立 `.workbench-page-*` 约束，移动端断点统一转单列。
- `panels.js`：IACC、Settings 的 renderer 保持现有 API 合同，只增强页面组织和信息展示。
- e2e：新增 shell 级渐进测试，覆盖 v0.9.202 到 v0.9.210 的关键页面。

## 匹配度

- 与 Hermes-webui 方向匹配：左侧垂直 rail 已成为主导航，原 tab 能力迁移为全页面工作台。
- 与 cowd 核心定位匹配：Runtime、Context、Memory、Skills、Agents、Tools、Gateway、Audit 都以 cowd 核心能力组织；IACC 明确为制造上层应用，结构化数据仍使用 cowd 内核路由。
- 与 TDD 要求匹配：每个版本均有 PLAN、REPORT、测试和截图证据。
- 与并行落地要求匹配：按页面域拆分版本，适合后续不同页面继续并行深化。

## 剩余非阻断项

- live e2e 依赖真实 gateway，不属于本次离线前端门禁，后续可单独做运行态验收。
- Settings 目前展示 provider/model 状态，没有新增 provider/model 写入配置能力；需要后端配置写入合同稳定后再扩展。
- 部分指标卡在窄屏会省略长文本，当前不破坏布局；后续可增加 hover/title 或详情抽屉。

## 结论

v0.9.211 最终门禁通过，本轮前端重构可以作为已完成版本提交。
