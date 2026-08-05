# WebUI 实时与历史测试

- 命令：`npm test`
- 退出码：`0`
- 测试文件：48/48 通过。
- 测试项：374/374 通过。
- Vitest 耗时：7.29s。
- 覆盖：
  - canonical-only Team/Agent/Skill/Tool 拓扑；
  - 历史 Turn 与当前 live Turn 使用同一 renderer；
  - snapshot/delta/live 单调归并、重连和 terminal 收敛；
  - business summary 与 technical full consumer 分离和释放；
  - 业务图不解析 ID、不根据时间重叠补父子关系；
  - i18n、页面能力、raw payload、API capability 和 acceptance gates。

最终结果：通过。
