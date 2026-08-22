# P6 real-Qwen supplemental end-to-end evidence

## Increment gate

This controlled deep-real increment passed against `qwen3.7-plus` through a
locally isolated Gateway:

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

- The increment report status and all 19 harness-report gates: `passed`.
- The real-model gate recorded 34 provider rounds for `qwen3.7-plus`.
- Isolated Gateway health check: `passed`.
- All four live scenarios passed: direct terminal, tool evidence, single
  architecture baseline and mandatory Team projection.
- The mandatory Team projection had one completed Team and four completed
  Agents, with no failed Agent or Team; its architecture-quality score was
  9/9.
- Public projection traversal, rather than root-only metrics, is the
  authoritative evidence for child-Team Agent completion.

## Scope boundary

This is **not** the authority-plan A12 release gate. Section 22.3 requires a
single controlled scenario with at least two Teams, independent parallel work
and a merge, escalation, same-session continuation, cross-session deny/allow,
approval, PostgreSQL query summary, Surface screenshot, candidate/config/event
hashes and performance evidence. The scenario above proves only the listed
Gateway/harness increment. `completion-audit-2026-08-23.md` tracks the
remaining authority gates and must be satisfied before claiming P6 closure.

## 2026-08-23 repeat run

The same isolated Gateway lane was repeated after the P4 continuation and
resource-pressure fixes:

```text
COWD_EVAL_KEEP_GATEWAY_ARTIFACTS=1 COWD_EVAL_TIMEOUT_SECS=900 \
  scripts/scenarios/harness-eval-real-qwen.sh
```

The run used `qwen3.7-plus`, completed with `status=passed`, and its report
gate passed all 19 required checks. It recorded 36 real provider rounds and
four complete live Gateway scenarios. The mandatory Team scenario projected
one completed Team and four completed Agents, with no failed Team or Agent.

```text
/tmp/cowd-real-qwen-evidence.aGhGBN/runs/v0.9.703-1787436069-mission-harness-deep/report.json
sha256 cdab5257610a746cbb2f6da38cc34e48a35dd9b8f599eb2bafb8574b47447187
evidence-manifest sha256 5b5a9104eb387da6d070601dd7e8e45efe395e3f75f796c3bef09dd4e1719699
```

This repeat confirms the real-model increment, not P6 release closure: it did
not exercise the required two-Team merge, escalation, same-session and
cross-session continuation deny/allow, approval, PostgreSQL, or Surface gates.

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

## 2026-08-23 merge-gate hardening

The earlier live Team scenario was insufficiently strict: it accepted one
completed Team with three Agents, so it could not distinguish a genuine
cross-Team merge from an ordinary Team subgraph. The harness now requires its
explicit collaboration scenario to produce all of the following from public
execution projections:

- at least three completed Teams and six completed Agents;
- two fully claimed typed Program edges (both a delivery receipt and a consumer
  claim receipt), which represent the two upstream facts entering the merge;
- the same architecture-quality and source-evidence checks as before.

`architecture_acceptance_requires_claimed_fan_in_for_multi_team_merge` covers
the positive fan-in case. A merely delivered edge is deliberately rejected:
until it is claimed by the consumer attempt it cannot prove that the merge Team
was allowed to consume the fact. This makes the next real-Qwen run a meaningful
A12 sub-gate instead of another one-Team increment. It has not yet been run
against a provider at this candidate SHA, so this section records the stricter
gate rather than a successful real-model result.
