# 记忆校对与自动修正 — TDD 执行方案

## 现状
FactChecker 仅检测不修正，矛盾写入日志后无动作。

## 要实现的闭环

```
FactChecker.detect_conflict() → 得分判定 → 裁决 → 修正 → 反馈
```

## TDD 执行计划

### Phase 1: 裁决引擎

- [ ] 1.1 新文件: `crates/memory/src/resolution.rs`

  RED: `test_resolve_by_consensus`, `test_resolve_by_confidence`, `test_resolve_by_agent_weight`

  GREEN: `ConflictResolver` 结构:
  ```rust
  pub struct ConflictResolver {
      weights: HashMap<String, f32>,
  }

  pub enum Verdict {
      KeepExisting,      // 保留旧条目
      ReplaceWithNew,    // 用新条目替换
      PromoteConsensus,  // 共识提升到 0.95
      FlagForReview,     // 标记待人工
  }
  ```

  裁决逻辑:
  1. 3+ Agent 共识 → PromoteConsensus
  2. 单个 Agent + confidence>0.7 → KeepExisting
  3. 新 Agent + confidence>0.8 or agent_weight>0.8 → ReplaceWithNew
  4. 都不满足 → FlagForReview

### Phase 2: 修正执行器

- [ ] 2.1 扩展 `FactChecker`

  RED: `test_auto_correction`, `test_conflict_pruning`

  GREEN: 新增方法:
  ```rust
  impl FactChecker {
      pub fn auto_correct(&mut self, store: &dyn MemoryStore) -> Result<CorrectionReport>;
  }
  ```
  - 对每对冲突调用 resolve() → 执行修正
  - 返回报告: { corrected: N, pruned: N, flagged: N }

### Phase 3: 集成到记忆管道

- [ ] 3.1 在 on_turn_end() 中插入修正步骤

  RED: `test_correction_in_on_turn_end`

  GREEN: 在 extract() → remember() 之后，自动调用 fact_checker.auto_correct()

### Phase FINAL

- [ ] F1. 测试覆盖: cargo test -p cowd-memory -- resolution → 7+ PASS
- [ ] F2. 回归: cargo test -p cowd-memory → 450+ PASS

## 参考源码
- crates/memory/src/fact_checker.rs (437行) — 检测逻辑
- crates/memory/src/coherence.rs — 一致性检查
- crates/memory/src/drift.rs — 过期检测
