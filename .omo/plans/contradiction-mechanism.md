# 矛盾检测与决策机制分析

## 三层检测机制 (FactChecker)

```
check_triple(triple)
    │
    ├── 1. 类型检查 (Type Check)
    │   ├── "child_of"  → 仅允许 EntityType::Person
    │   ├── "subsidiary_of" → 仅允许 EntityType::Organization
    │   ├── "uses/depends_on" → 仅允许 Project/Tool
    │   └── 不匹配 → confidence × 0.5 (max 0.3)
    │
    ├── 2. 一致性检查 (Consistency Check)
    │   ├── child_of/parent_of: 与已注册的 parent 字段比较
    │   ├── partner_of: 与已注册的 partner 字段比较
    │   ├── full_name/known_as: 与已注册的 full_name 比较
    │   └── 不一致 → confidence × 0.3 (max 0.2)
    │
    └── 3. 时序检查 (Temporal Check)
        └── valid_until != None → 前一条已被作废 → 新条目有效
            否则 → 与前一条冲突 (通过KG查询外部实现)
```

## 跨Agent冲突检测 (detect_conflict)

```
条件: 同一 (subject, predicate) + 不同 object + 不同 source_agent + 未作废

得分: score = 0.4×confidence + 0.3×recency (时效因子) + 0.3×agent_weight

Agent权重:
  Orchestrator = 1.0 (最高)
  Reviewer     = 0.8
  Executor     = 0.6
  unknown      = 0.4
```

## 共识检测 (detect_consensus)

```
3个以上不同 Agent 对同一 (subject, predicate, object) 达成一致 → consensus_confidence = 0.95
## 效果

当前系统: DETECT ONLY — 发现矛盾后:
  1. check_triple() → 返回 FactCheckResult { is_consistent: false, contradiction: String, suggested_correction: String }
  2. detect_conflict() → 返回冲突三元组 + 得分
  3. detect_consensus() → 返回是否达成共识
  4. ⚠️ 以上结果仅以日志输出，无自动化决断

缺失的闭环:
  - 无自动裁决: 矛盾时不知道信任哪个来源
  - 无自动修正: 不会用 consensus(0.95) 覆盖旧条目
  - 无自动淘汰: detect_consensus=false 的条目不会降权或移除
  - 无冲突收敛: 多次矛盾不会触发人工干预

结论: 检测能力到位，但冲突后的「决策回路」未闭合。FactChecker 当前是一个只读观察者，不具备修正权。
