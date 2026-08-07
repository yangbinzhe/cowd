# Runtime 状态放大补偿版基线

## 仓库

| 仓库 | 基线 HEAD | 版本 | 状态 |
| --- | --- | --- | --- |
| cowd | `b44037f748fd8c83a6a04aa524bf33587296d07f` | 0.9.645 | 接续上一轮未封版实现 |
| cowd-edge | `e403539cdf6c817e1a12fa7d1b58215c94cbd737` | 0.9.645 | 接续上一轮 WebUI 源码与构建产物 |

本补偿版不回退已有改动。封版前以最终 `git diff`、测试结果和生产验证替换本文件中的
过程状态。

## 真实缺口证据

- PostgreSQL 数据库 collation 为非 `C`；`prefix + U+10FFFF` 上界不能覆盖现有
  `evolution:*` stream。
- 生产事件中存在 115 条 signal recorded 和 115 条 projector failed，缺少
  diagnosis/mission/proposal；Evolution API 返回空集合。
- 54 条 ContextEnvelope 事件约 807 KiB；新 schema 同时保存 selected 与
  派生 dynamic tail。
- 三套 TUI 场景脚本仍调用删除的 Session projection URL。
- 未知 `/api/*` 当前返回 WebUI index 的 `200 text/html`。
- WebUI execution command 没有可推导的 Session id，因此不会失效目标 Session 的
  execution projection read。

## 基线判定

上一版证据中的“全部终态门禁通过”不成立。本目录后续的 implementation、
validation 和 final-gate 必须以本补偿方案的实际结果更新，不得预填通过。
