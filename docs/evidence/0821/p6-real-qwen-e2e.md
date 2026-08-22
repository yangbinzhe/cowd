# P6 real-Qwen end-to-end evidence

## Final gate

The final deep-real gate passed against `qwen3.7-plus` through a locally
isolated Gateway:

```text
COWD_EVAL_KEEP_GATEWAY_ARTIFACTS=1 COWD_EVAL_TIMEOUT_SECS=900 \
  scripts/scenarios/harness-eval-real-qwen.sh
```

The runner creates a temporary Gateway configuration, local bearer token and
SQLite state. The provider configuration stores only `env:DASHSCOPE_API_KEY`;
the credential itself is never written to configuration, output or this
record. The source workspace remains the read-only evaluation fixture.

Final report artifact (outside the repository):

```text
/tmp/cowd-real-qwen-evidence.ql6rxB/runs/v0.9.703-1787421026-mission-harness-deep/report.json
sha256 25f2bc0cbc747739b987343cab66d0f730947094bee3a73c85d24945ce5c8e6b
```

## Observed results

- Overall report status and all 19 report gates: `passed`.
- The real-model gate recorded 34 provider rounds for `qwen3.7-plus`.
- Isolated Gateway health check: `passed`.
- All four live scenarios passed: direct terminal, tool evidence, single
  architecture baseline and mandatory Team projection.
- The mandatory Team projection had one completed Team and four completed
  Agents, with no failed Agent or Team; its architecture-quality score was
  9/9.
- Public projection traversal, rather than root-only metrics, is the
  authoritative evidence for child-Team Agent completion.

## Regression coverage added for the final closure

- `ProviderClient` resolves a configured `env:NAME` secret only at the
  transport boundary and fails closed for missing or malformed references.
- Production runtime-tool catalog validation covers the collaboration
  escalation effect resolver.
- Live scenario aggregation uses public child Team task displays for both
  acceptance and paired-comparison capability evidence.
- Architecture evaluation rejects whole-answer claims of no source evidence
  while permitting a scoped open question when the answer contains actual
  checked source evidence.
