# 0821 delegation strike ledger

| Agent | Phase / scope | Strike | Resolution |
|---|---|---:|---|
| P2-facts | P2 durable receipt snapshot and single evaluator | 0 | Completed and accepted after scoped diff, residual, format, targeted-test and Runtime-lib-check review |

Rule: a scope or goal deviation is recorded and immediately stopped. A second deviation by the
same agent closes its task; its unreviewed output is rejected. No agent may modify files outside
the exact allowlist recorded in `0821-file-ownership.tsv`.
