# Runtime 状态放大补偿版最终门禁

## 判定规则

任何一项缺少代码、测试或运行证据时，整体结论只能是未通过。单元测试不能替代真实
PostgreSQL 与已安装 Gateway 链路，文档声明不能替代代码扫描。

## 门禁表

| 门禁 | 状态 | 证据 |
| --- | --- | --- |
| PG prefix 与 collation 无关 | 通过 | `en_US.utf8` 隔离 PG 合同 |
| Evolution unresolved 可恢复且证据保留 | 通过 | projector test + 安装环境 125 组 lifecycle |
| Context schema v3 单一正文 | 通过 | serialization/artifact/hydration tests |
| 未知 API 返回 JSON 404 | 通过 | unit + 安装环境 HTTP |
| TUI 只消费 canonical API | 通过 | 旧 URL 扫描、脚本语法与受影响包测试 |
| WebUI mutation 精确失效 | 通过 | stale-response 与 cross-Session tests |
| Core/Edge 全量测试构建 | 通过 | `validation.md` |
| Release 安装与资源一致 | 通过 | 版本、health、doctor、资产 hash |
| Core/Edge/MFG source lock 一致 | 通过 | xtask verify、apps sync、Cargo.lock |
| 三仓版本与远端同步 | 封版门禁 | commit、annotated tag、push 后由 Git refs 验证 |

## 最终结论

代码、真实 PostgreSQL、安装态 Gateway、Edge sidecar 和 WebUI 资源门禁全部通过。功能
没有通过恢复旧接口、保留第二套投影或删除失败证据获得通过；Runtime 热状态、Session
durability、Evolution lifecycle、Context evidence 与 Surface 消费链均保持单一 owner。

唯一尚需在文档提交后执行的动作是 Git 封版门禁：提交 Core/Edge、创建中文 annotated
`v0.9.646` tag、推送并验证远端 refs。若任一 Git 门禁失败，不得对外宣称版本封闭。
