# Skill 供应链与能力边界

本文描述 Cowd Skill 的安装、发现、更新、回滚和运行边界。它是实现合同，不把 Skill、Tool、Plugin、MCP 或受管进程混为一类。

## 结论

Skill 是惰性的指令与工作流资源，不是权限主体，也不是可执行扩展的安装器。安装 Skill 只改变受管 Skill 目录；脚本、网络、子进程、文件写入和外部系统操作仍必须经过现有 Tool Host、授权、审批、sandbox、预算、取消和审计链。

下列分工必须保持唯一：

| 层 | 唯一职责 | 不得承担 |
|---|---|---|
| `skill` | 来源解析、包审计、不可变版本、安装 receipt、发现和内容投影 | 授权签发、脚本执行、MCP 进程托管 |
| `tools` | 向模型暴露 typed install_plan/install_commit/status，并执行既有 Tool Host 授权 | 绕过审批直接写 Skill 根目录 |
| Auth/Approval | 判断当前主体是否可审查或提交供应链变更 | 从 Skill 声明推导或扩大权限 |
| Runtime | 原生审批指针、撤权 epoch、当前 turn 的选择与精确 page-in | 下载、隐式安装、把 Skill 文本当事实证据 |
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
  -> inert staged inventory
  -> authenticated human first-install review (expected absent)
  -> original Runtime approval transaction + native pointer
  -> approved catalog / exact immutable page-in

approved revision -> maintenance usage/evidence gate -> human review -> native pointer CAS
approved revision -> human rollback review(exact original receipt) -> native pointer CAS
approved revision -> human revoke(expected generation) -> native pointer + advanced revocation epoch
```

计划与提交分离。提交会重新获取和重新扫描来源，并要求包摘要与已审查摘要一致；因此本地目录修改、GitHub 分支漂移和下载内容替换都会在写入前失败。高危扫描结果不可通过 `allow_warnings` 绕过；普通 warning 必须显式接受。

同一 Skill 的 revision 目录由 SHA-256 内容寻址。写入在同一文件系统 staging 中完成，文件和目录 fsync 后 rename，保存原安装 receipt。安装始终只是 stage；旧磁盘 `active.json` 不再提供执行权限。唯一批准状态由原 Runtime SkillRevisionGovernance 的事件事务保存，绑定 scope、revision、原 receipt digest、approval、generation 和 revocation epoch。

管理目录可以展示未批准版本；运行目录仅从原生批准指针读取其 receipt metadata。精确 page-in 重新核验完整包、manifest、文件数量/字节数、扫描和原 receipt，并使用同次捕获的 profile/正文，不能回读另一目录代替。缺失、旧式未知来源和读取错误不能成为批准。

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

Skill manifest 不声明可授予能力。计划是只读或受限网络动作；模型 install_commit 只能持久 stage。激活、回滚和撤权必须由认证人类经过原 Runtime 审批 owner，模型不提供这些操作。首次批准可在原指针缺失时 bootstrap；维护升级仍保留 usage/evidence 门，不能借首次批准绕过。

“全自动”可以按现有策略减少普通交互确认，但不能覆盖以下硬门：包摘要变化、高危扫描、路径/资源上限、可执行扩展错层、主体权限不足或证据损坏。系统不会因为文本声称“用户已授权”就信任 Skill，也不会因 Skill 已安装而自动运行其中脚本。

超时字段统一为 `timeout_ms`，单位进入 schema、解析和错误信息；旧的模糊 `timeout` 字段由 deny-unknown 解码直接拒绝。这消除了模型把秒误当毫秒、导致安装辅助命令过早超时的歧义。Skill 安装本身不依赖 Bash/pipx fallback。

## 发现和遮蔽

Cowd 保留 Codex、Agents、Claude 和旧 Cowd 目录的只读兼容发现，不迁移或删除用户资产。优先级为：

1. workspace/project Skill；
2. Cowd managed store 的安装 receipt 元数据；
3. 旧用户与外部 agent Skill 根。

这些次序只用于管理发现，不能把项目或 legacy 文件提升为执行授权，受管安装也不会覆盖它们。Runtime Source 的运行目录以原生批准为准，按真实 scope 隔离。

原 Agent admission 把批准 pin 与实际由 Skill 新增的工具差集放入原持久 Binding/digest；独立授权工具不能反推为 Skill 贡献。已绑定执行按原 pin 读取精确 profile/正文；未绑定新 turn 读取当前批准。普通升级允许尚未撤权的旧不可变 pin，撤权推进 epoch 后旧缓存和旧 pin 均拒绝。

最终完整 prompt packet 携带正文来源 pin；packing 淘汰的 packet 不进入实际读集。原 Provider dispatch fence 只对实际正文或实际 Skill 工具 schema 贡献核验来源，并在持久 wire 证据之后、HTTP 发送前再次核验。没有正文或工具贡献的请求不承担该依赖。外部 Process 共用启动前校验；这不宣称已启动外部进程的任意 HTTP 都由 Runtime fence 控制。

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
cowd skill approve-first <skill-id> <revision> <receipt-id> <request-id>
cowd skill activate <maintenance-draft> <revision> <receipt-id> <request-id>
cowd skill rollback <skill-id> <revision> <receipt-id> <request-id> <evidence>
cowd skill remove <skill-id> <expected-generation> <request-id> <reason>
cowd skill decide <review-id> <approve|deny> <reason>
cowd skill active <skill-id>
```

Gateway 对本地/GitHub 来源提供 `/api/skills/install/plan` 与 `/api/skills/install/commit`，对上传包提供 `/api/skills/install/upload/plan` 与 `/api/skills/install/upload/commit`；上传 commit 必须再次携带包字节和 plan 返回的精确摘要。模型使用原 `skill` 入口的 install_plan/install_commit/status。所有安装入口调用同一 `SkillLifecycle`；认证 CLI/Gateway 的批准、回滚与撤权调用同一原 Runtime 审批事务，不另写磁盘 active pointer。CLI 沿用现有 Gateway token/surface selector。DELETE 必须携 request_id、expected_generation 和 reason；即使正文损坏或已擦除，原稳定 Skill ID 仍可定位 native pointer 供人类撤权。

`remove` 是停用，不删除 revision 或 receipt。物理清理由未来独立 retention policy 负责，不能混入用户操作路径。

## 不变量与验收

每次改动必须至少证明：

- 相同包产生相同 package digest；修改任意内容使已审查 commit 拒绝；
- 高危内容、链接、路径冲突、资源超限和 executable extension 被阻断；
- warning 未显式接受不能提交；
- receipt metadata 漂移不能进入批准目录，完整包变更不能 page-in；
- 原批准指针 CAS、scope、exact receipt 和人类 lease 必须同时成立，拒绝决定不改变批准状态；
- 管理发现可展示历史 stage，运行目录只展示本 scope 的已批准版本；
- project/legacy roots 保持只读，项目遮蔽次序不变；
- 模型 commit 只 stage，未批准/错版本/错 scope/旧式未知 pin 均拒绝；
- Skill 不新增 principal capability，脚本仍需走普通 Tool Host；
- `timeout_ms` 是 shell wire contract 的唯一超时字段；
- CLI、Gateway、page-in 和实际 Provider 发送消费同一原批准来源；
- usage accepted 不等于 durable，强消费者读取原持久 receipt，原 worker 可关闭并 join；
- 真实模型、Process、冷恢复和完整业务场景留给冻结候选验收，局部短门不能替代。

这些不变量优先于“安装成功”的表面结果。任何层只能缩小权限和可见性，不能通过合并 Skill、Tool、Plugin 或 sandbox 责任来扩大能力。
