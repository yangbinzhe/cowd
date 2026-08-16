# Skill 供应链与能力边界

本文描述 Cowd Skill 的安装、发现、更新、回滚和运行边界。它是实现合同，不把 Skill、Tool、Plugin、MCP 或受管进程混为一类。

## 结论

Skill 是惰性的指令与工作流资源，不是权限主体，也不是可执行扩展的安装器。安装 Skill 只改变受管 Skill 目录；脚本、网络、子进程、文件写入和外部系统操作仍必须经过现有 Tool Host、授权、审批、sandbox、预算、取消和审计链。

下列分工必须保持唯一：

| 层 | 唯一职责 | 不得承担 |
|---|---|---|
| `skill` | 来源解析、包审计、不可变版本、激活指针、发现和内容投影 | 授权签发、脚本执行、MCP 进程托管 |
| `tools` | 向模型暴露 typed plan/commit/status/rollback/deactivate，并执行既有 Tool Host 授权 | 绕过审批直接写 Skill 根目录 |
| Auth/Approval | 判断当前主体是否可审查或提交供应链变更 | 从 Skill 声明推导或扩大权限 |
| Runtime | 在当前 turn 中选择并加载已准入 Skill | 下载、隐式安装、把 Skill 文本当事实证据 |
| Plugin/MCP | 安装和托管可执行扩展、服务及其依赖 | 伪装成 Prompt Skill 绕过供应链治理 |
| Gateway/CLI | 人类可操作的计划、提交、状态、回滚和停用界面 | 建立第二套复制/删除实现 |

这套划分与现有 Execution/Tool/Sandbox/Plugin/MCP 能力互补：Skill 提供“如何做”，Tool 与 Core 提供“允许做什么以及实际做了什么”。两者没有权限并集或隐式升级关系。

## 受管生命周期

受管根目录为：

```text
${COWD_CONFIG_HOME:-~/.cowd}/skill-store/v1/
  .lifecycle.lock
  <skill-id>/
    revisions/<sha256>/...       # 不可变包
    receipts/<install-id>.json   # 来源、扫描器、摘要、操作者和前序版本
    active.json                  # 唯一激活指针
    inactive/...                 # 停用证据
```

状态流只有：

```text
source
  -> acquire
  -> bounded extract/collect
  -> inspect + security scan
  -> plan(package digest)
  -> explicit commit(expected digest)
  -> immutable revision + durable receipt
  -> atomic active.json publish
  -> catalog refresh

active revision -> rollback(existing exact digest) -> new receipt + atomic pointer
active revision -> deactivate -> preserve revisions/receipts, remove active authority
```

计划与提交分离。提交会重新获取和重新扫描来源，并要求包摘要与已审查摘要一致；因此本地目录修改、GitHub 分支漂移和下载内容替换都会在写入前失败。高危扫描结果不可通过 `allow_warnings` 绕过；普通 warning 必须显式接受。

同一 Skill 的 revision 目录由 SHA-256 内容寻址。写入在同一文件系统的 staging 中完成，文件和目录 fsync 后 rename；receipt 持久化后才发布 `active.json`，最后 fsync 父目录。发现时重新核验 active pointer、receipt、包摘要、manifest、文件数量/字节数、包类型、扫描器和安全结论，证据或内容漂移即 fail closed。

## 来源与信任

支持的原生来源：

- 本地 `SKILL.md` 或包含它的目录；
- `github://owner/repo/path?ref=<ref>`；
- `https://github.com/owner/repo[/tree/<ref>/<path>]`；
- Gateway 的有界 tar 上传；
- Cowd 人工创建的 Skill。

GitHub 安装先由固定 `api.github.com` 解析不可变 commit SHA，再从固定 `codeload.github.com` 获取归档；不跟随跨主机重定向。token 只进入请求头，不写 source locator 或 receipt。receipt 保留无凭据 locator、请求 ref 和解析后的 commit。

本地模型工具只能读取当前 workspace 内的来源；任意主机路径不能借 Skill 安装成为数据外带通道。CLI/Gateway 的人类入口可以显式选择本地来源。远程包没有 license 声明或 LICENSE/COPYING 时产生法律审查 warning；来源记录不是版权许可，操作者仍须确认再分发和使用的权利。

任一来源都执行相同的闭集校验：

- 只接受 regular file/directory，拒绝 symlink、hard link、device 和路径逃逸；
- 拒绝大小写折叠冲突、非 UTF-8 路径、过深目录和重复条目；
- 文件数、单文件、解压总量和远端归档均有硬上限；
- 拒绝 `.git`、`node_modules`、`target` 等依赖/VCS/构建输出；
- 所有 UTF-8 文本均参与安全扫描，不只扫描 `SKILL.md`；
- `SKILL.md` 必须有非空 `name` 和 `description`；
- MCP server 或 sidecar 声明被归类为 executable extension，并要求转入 Plugin/MCP 生命周期。

## 权限、不信任和“全自动”

Skill manifest 不声明可授予能力。计划是只读或受限网络动作；commit、rollback、deactivate 是独立的持久化变更，使用高权限 Tool effect 和现有 Auth/Approval 审计。

“全自动”可以按现有策略减少普通交互确认，但不能覆盖以下硬门：包摘要变化、高危扫描、路径/资源上限、可执行扩展错层、主体权限不足或证据损坏。系统不会因为文本声称“用户已授权”就信任 Skill，也不会因 Skill 已安装而自动运行其中脚本。

超时字段统一为 `timeout_ms`，单位进入 schema、解析和错误信息；旧的模糊 `timeout` 字段由 deny-unknown 解码直接拒绝。这消除了模型把秒误当毫秒、导致安装辅助命令过早超时的歧义。Skill 安装本身不依赖 Bash/pipx fallback。

## 发现和遮蔽

Cowd 保留 Codex、Agents、Claude 和旧 Cowd 目录的只读兼容发现，不迁移或删除用户资产。优先级为：

1. workspace/project Skill；
2. Cowd managed store 的 active revision；
3. 旧用户与外部 agent Skill 根。

因此项目可以有意遮蔽用户 Skill，受管安装不会覆盖项目文件，旧来源也不会被新的删除接口误删。Gateway 配置指纹同时观察 `SKILL.md` 和 `active.json`，激活、回滚和停用会使 workspace Skill snapshot 失效；正在执行的 turn 继续使用其已固定快照，新 turn 获取新目录。

## 与 Codex、Hermes、OpenCode 的对照

对照基线是 2026-08-16 获取的上游代码：Codex `9ded177c`、Hermes Agent `2be18314`、OpenCode `0e99cb98`。

| 能力 | 可借鉴点 | Cowd 的终态取舍 |
|---|---|---|
| Codex | host/executor/orchestrator 多 authority provider；显式 mention；目录与 watcher；渐进读取 | 保留多来源、遮蔽和渐进加载，并增加受管写侧的摘要、receipt、回滚和权限门 |
| Hermes | Skills Hub、quarantine、安全扫描、人工确认、pending write；Skill/Tool 区分 | 采用“先审查再提交”和全包扫描；不采用扫描后直接覆盖活动目录，也不让 skill write gate 代替 Cowd Auth/Approval |
| OpenCode | permission-based availability；小目录摘要后按需加载；同源 URL/path 校验；staging/backup 更新 | 采用渐进上下文、同源/path 校验和 staging；以内容寻址 revision + 单 active pointer 替代可变 cache/version 文件 |

对标系统的发现和交互经验是有效的，但不能直接作为 Cowd 的企业供应链终态：仅靠 mutable directory、registry version 或 UI confirmation 无法同时证明当前生效字节、来源 commit、扫描结论和回滚目标。Cowd 增加的 evidence-bound lifecycle 正是对这些能力的补充，而不是重做模型提示或 Plugin 体系。

## 操作界面

CLI：

```text
cowd skill plan <source>
cowd skill install <source> --expected-digest <sha256:digest> [--allow-warnings]
cowd skill status <skill-id>
cowd skill rollback <skill-id> <sha256>
cowd skill remove <skill-id>
```

Gateway 对本地/GitHub 来源提供 `/api/skills/install/plan` 与 `/api/skills/install/commit`，对上传包提供 `/api/skills/install/upload/plan` 与 `/api/skills/install/upload/commit`；上传 commit 必须再次携带包字节和 plan 返回的精确摘要。模型使用 `skill_install_plan`、`skill_install_commit`、`skill_status`、`skill_rollback` 和 `skill_deactivate` typed tools。三个入口最终调用同一个 `SkillLifecycle`，不存在 CLI、WebUI 和模型各自复制文件的分叉实现。

`remove` 是停用，不删除 revision 或 receipt。物理清理由未来独立 retention policy 负责，不能混入用户操作路径。

## 不变量与验收

每次改动必须至少证明：

- 相同包产生相同 package digest；修改任意内容使已审查 commit 拒绝；
- 高危内容、链接、路径冲突、资源超限和 executable extension 被阻断；
- warning 未显式接受不能提交；
- receipt 或 active revision 漂移使发现失败；
- 更新只改变 active pointer，回滚只指向已有且重新核验的 revision；
- managed active revision 可发现，inactive 和历史 revision 不进入 catalog；
- project/legacy roots 保持只读，项目遮蔽次序不变；
- plan 工具是只读/网络 effect，commit/rollback/deactivate 是持久化高风险 effect；
- Skill 不新增 principal capability，脚本仍需走普通 Tool Host；
- `timeout_ms` 是 shell wire contract 的唯一超时字段；
- CLI、Gateway、模型工具和配置 watcher 使用同一 store/active pointer 语义。

这些不变量优先于“安装成功”的表面结果。任何层只能缩小权限和可见性，不能通过合并 Skill、Tool、Plugin 或 sandbox 责任来扩大能力。
