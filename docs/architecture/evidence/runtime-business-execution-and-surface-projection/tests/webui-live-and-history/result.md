# WebUI 实时与历史测试

- 命令：`npm test`
- 退出码：`0`
- 测试文件：48/48 通过。
- 测试项：375/375 通过。
- 发布浏览器门禁：22/22 通过，0 unexpected、0 flaky、0 skipped，耗时 24.4s。
- 覆盖：
  - canonical-only Team/Agent/Skill/Tool 拓扑；
  - 历史 Turn 与当前 live Turn 使用同一 renderer；
  - snapshot/delta/live 单调归并、重连和 terminal 收敛；
  - business summary 与 technical full consumer 分离和释放；
  - 业务图不解析 ID、不根据时间重叠补父子关系；
  - i18n、页面能力、raw payload、API capability 和 acceptance gates。
  - 双标签页使用独立 observer，writer reaffirmation 与消息 mutation 原子串行；
  - 3 个历史 Turn 各自加载 canonical Execution tree；
  - 真实 Gateway Team 策略在 Runtime、Mission、MFG 三个界面保持一致；
  - 桌面、断点和移动端的 Chat、Runtime、Mission 无横向溢出和关键控件裁切。

发布门禁使用提交：

```text
cowd      bf3724098fe313d5b31123a348c9e8e537ab9e01
cowd-edge aed45c3777b17df65bd80051789279093870f8f6
MFG       72652c10f2e9e0e379973644eded5d57fb7b35cf
```

最终结果：通过。
