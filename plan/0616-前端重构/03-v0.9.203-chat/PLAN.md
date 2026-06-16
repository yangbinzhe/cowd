# v0.9.203 Chat 页面重塑计划

## 目标

优化 Chat 主工作区，使其与新的 Graphite workbench shell 协同：

1. Chat header 更像运行时工作台，而不是简单工具条。
2. composer 更稳定、更克制。
3. tool card / thinking card 去掉旧 HTML 实体字符，降低视觉噪声。
4. 保留现有消息发送、slash command、模型选择、旧 panel toggle 行为。

## 代码范围

预计修改：

- `webui/index.html`
- `webui/style.css`
- `webui/ui.js`
- `webui/modules.test.js`
- `webui/webui-shell.e2e.spec.js`

## TDD 设计

1. 单元测试：
   - `UI.addToolCard()` 不渲染 `&xutri;` 等错误实体。
   - `UI.addThinkCard()` 不渲染 `&xodot;` 等错误实体。
2. e2e：
   - Chat header、composer 可见。
   - 注入 tool/thinking card 后截图。
   - 保存 desktop/mobile 截图。

## 验收标准

1. `cd webui && npm test` 通过。
2. `cd webui && npm run test:e2e -- webui-shell.e2e.spec.js` 通过。
3. 截图保存：
   - `plan/0616-前端重构/screenshots/v0.9.203-chat-desktop.png`
   - `plan/0616-前端重构/screenshots/v0.9.203-chat-mobile.png`
4. 报告保存：
   - `plan/0616-前端重构/03-v0.9.203-chat/REPORT.md`
   - `plan/0616-前端重构/reports/v0.9.203.md`

