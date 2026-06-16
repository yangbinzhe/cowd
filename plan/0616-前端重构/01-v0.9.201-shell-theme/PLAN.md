# v0.9.201 Shell 与 Theme 基线计划

## 目标

建立 WebUI 重构的第一层基础：

1. 新增 Hermes 风格的 app shell 语义结构。
2. 新增左侧 nav rail 的静态骨架，但暂不强制迁移所有页面逻辑。
3. 新增克制的 Graphite-like 默认视觉 token。
4. 建立视觉截图测试与报告归档流程。
5. 保留现有右侧 panel 行为，避免第一阶段破坏功能。

## 代码范围

预计修改：

- `webui/index.html`
- `webui/style.css`
- `webui/ui.js`
- `webui/boot.js`
- `webui/modules.test.js`
- 新增 `webui/webui-shell.e2e.spec.js`

不修改：

- 后端 API。
- TUI/CLI。
- 当前未提交的 TUI 文件。

## TDD 设计

先新增或更新测试：

1. DOM shell 测试：页面包含 `#app-shell`、`#nav-rail`、`#main-workspace`。
2. rail item 测试：每个核心页面有 `data-view` 或 `data-panel` 映射。
3. theme token 测试：`document.documentElement` 支持 `data-theme` 与 `data-skin`。
4. 视觉 e2e：打开首页，保存 desktop 与 mobile 截图。

## 实现步骤

1. 将 body 内结构包裹为 `#app-shell`。
2. 新增 `#nav-rail`，先放 Chat、Workspace、Runtime、Memory、Context、Skills、Agents、Tools、Gateway、IACC、Audit、Settings。
3. 保留旧 `#right-panel` 和 `#panel-tabs`，但视觉上降低其优先级。
4. 新增 CSS token：
   - `--app-bg`
   - `--rail-bg`
   - `--panel-bg`
   - `--surface-subtle`
   - `--surface-hover`
   - `--focus-ring`
   - `--semantic-success/warn/error/info`
5. 新增 `data-skin="graphite"` 初始化逻辑，默认启用。
6. 增加截图脚本或 e2e 截图用例。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.201-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.201-mobile.png`
4. 报告保存：
   - `plan/0616-前端重构/01-v0.9.201-shell-theme/REPORT.md`
   - `plan/0616-前端重构/reports/v0.9.201.md`
5. 旧 panel 按钮和旧 e2e 不应被破坏。

