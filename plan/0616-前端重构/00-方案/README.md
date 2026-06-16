# Cowd WebUI Hermes 风格重构总方案

日期：2026-06-16
分支：dev-iacc
当前基线版本：0.9.200

## 目标

以 `agents/hermes-webui` 为参考，重构 Cowd WebUI 的信息架构、视觉语言和页面组织方式：

1. 将原右侧横向 tab 面板迁移为左侧竖向 icon rail。
2. 将原 `workspace / memory / runtime / context / skills / crons / agents / tools / iacc / gateway / audit / settings` 从 340px 窄面板升级为完整页面。
3. 保持 cowd 内核定位：runtime、memory、context、structured data、skills、agents、tools、gateway、audit 是内核能力管理面；IACC 是制造领域上层应用工作台。
4. 采用克制、可信、可长时间使用的工作台视觉语言，优先学习 Hermes 的 Graphite/Geist 类主题、左侧导航、统一标题栏、低噪声状态展示。
5. 每一步按 TDD 执行：先定义目标门禁，再实现，再运行自动测试、视觉截图和报告归档。

## 当前问题

Cowd 当前 WebUI 的主入口集中在：

- `webui/index.html`
- `webui/style.css`
- `webui/ui.js`
- `webui/panels.js`
- `webui/workspace.js`
- `webui/boot.js`
- `webui/commands.js`

现状问题：

1. `#right-panel` 固定 340px，无法承载 runtime、memory、context、gateway、IACC 这类完整管理能力。
2. `#panel-tabs` 横向拥挤，功能越多越难扫视。
3. `Panels.renderXxx()` 内容大多按窄面板设计，很多复杂信息只能堆叠，无法形成清晰工作流。
4. 当前主题接近 GitHub dark，颜色、层级、密度与“内核运行时工作台”定位不够匹配。
5. 视觉测试目前覆盖不足，缺少逐版本截图和验收报告沉淀。

## Hermes 可学习点

参考目录：`/media/yi/Datas/workspace/agents/hermes-webui`

重点参考文件：

- `static/style.css`
- `static/panels.js`
- `static/ui.js`
- `docs/UIUX-GUIDE.md`
- `THEMES.md`

可学习原则：

1. 左侧 rail 承担一级导航，点击后切换主工作区。
2. `panel-view` 模型让页面内容拥有完整空间，而不是都挤入右侧。
3. 统一 app titlebar 显示当前页面、会话或状态。
4. Graphite 类主题使用低饱和中性色、明确边界、少阴影、稳定字号。
5. 工具、thinking、runtime 事件属于元信息，应可查看但不压过正文。
6. Appearance 设置应拆成 theme / skin / font size / density 等稳定轴。

## 目标信息架构

一级导航：

- Chat：对话主工作区。
- Workspace：文件、目录、预览、上传、工作目录。
- Runtime：control-plane、leases、runs、approvals、timeline。
- Context：上下文包、选择/省略、预算、推荐、历史 envelope。
- Memory：状态、事实、实体、关系、网络图、维护。
- Skills：技能库、启用态、来源、运行历史。
- Agents：任务、workgraph、多 agent 状态、验收。
- Tools：工具注册表、schema、权限、调用历史。
- Gateway：连接器、账号、微信/飞书/邮件、回执、能力。
- IACC：制造上层应用，报告、结构化数据、事实判定、推演。
- Audit：审计记录、策略、cross-plane 派发、回放。
- Settings：外观、模型、provider、profile、安全、高级设置。

布局原则：

1. 左侧 `nav-rail` 固定窄栏，使用 icon + tooltip，不使用横向 tab。
2. Chat 页面保留 session sidebar；非 Chat 页面可折叠 session sidebar，让主页面拥有完整宽度。
3. 复杂页面采用“顶部工具栏 + 左列表 + 右详情/主内容”的工作台模式。
4. 移动端不强行展示多列，使用 rail 折叠、页面内详情下移或抽屉化。

## TDD 总门禁

每个版本完成前必须满足：

1. 有对应版本目录下的计划、目标、验收标准。
2. 自动测试至少运行 `cd webui && npm test`。
3. 涉及页面结构的版本必须运行 Playwright e2e 或新增专门视觉 e2e。
4. 必须生成截图，保存到 `plan/0616-前端重构/screenshots/` 或版本目录内。
5. 必须输出测试报告，保存到对应版本目录和 `plan/0616-前端重构/reports/`。
6. 每个版本完成后单独提交。

## 版本路线

- `0.9.201`：Shell/theme 基线、设计 token、截图测试基建。
- `0.9.202`：左侧 icon rail、视图切换、旧 panel 兼容层。
- `0.9.203`：Chat 页面重塑、工具/thinking 降噪、composer 优化。
- `0.9.204`：Workspace 完整页面。
- `0.9.205`：Runtime + Context 完整页面。
- `0.9.206`：Memory 完整页面。
- `0.9.207`：Skills + Agents + Tools 工作台。
- `0.9.208`：Gateway + Audit + Cross-plane 工作台。
- `0.9.209`：IACC 制造上层应用工作台。
- `0.9.210`：Settings、外观系统、全站 polish。
- `0.9.211`：全量视觉、性能、回归门禁。

## 并行策略

可并行的开发线：

- A 线：Shell、nav rail、theme tokens、页面容器。
- B 线：Chat、messages、tool/thinking cards、composer。
- C 线：Runtime、Context、Memory 的数据管理页面。
- D 线：Skills、Agents、Tools 的能力管理页面。
- E 线：Gateway、Audit、IACC 的业务/连接器页面。
- F 线：测试、截图、报告、视觉回归。

合并顺序：

1. A 线先入主干，提供页面骨架。
2. B/C/D/E 可在 A 后按版本逐步合入。
3. F 线每个版本同步更新，不能最后补。

## 风险控制

1. 不一次性删除旧 `Panels.renderXxx()`，先通过兼容层迁移。
2. 原 e2e 中依赖 `#panel-content` 的断言逐步迁移，迁移前保留兼容容器。
3. 每个版本只改变一组页面的结构，避免全站同时失稳。
4. IACC 页面不得承载 cowd 内核唯一入口；结构化数据内核能力必须能在 Memory/Context/Runtime 中观察。
5. 视觉变量统一在 `style.css` token 层，不在页面局部硬编码大量颜色。

