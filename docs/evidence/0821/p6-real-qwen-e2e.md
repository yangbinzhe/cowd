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

## 2026-08-23 strict-gate real-Qwen result and root cause

The strict gate was run once from candidate `b2162162` against the isolated
Gateway and `qwen3.7-plus`:

```text
COWD_EVAL_KEEP_GATEWAY_ARTIFACTS=1 COWD_EVAL_TIMEOUT_SECS=1800 \
  scripts/scenarios/harness-eval-real-qwen.sh
```

It correctly failed, rather than accepting a prose claim of collaboration.
The direct, tool-evidence and single-architecture scenarios passed. The Team
scenario made 22 provider rounds and 143 read-only tool calls, then completed
one Team with four Agents; it produced zero claimed cross-Team edges. Its
quality score was 6/9, and the suite status was `failed`.

```text
/tmp/cowd-real-qwen-evidence.CqEd00/runs/v0.9.703-1787438996-mission-harness-deep/report.json
sha256 5f4c3a4d807bd764db704a7545016d24e486649f40a4c0b0d6f04330a9538b0d

live-scenarios/004-live_team_projection.json
sha256 4f39ac709613f5d78383488e11a28e6f5a1b40c6148f7b278ac03b91563f8ea9
```

The durable projection identified the actual cause: the Chinese/English mixed
phrase `三个协作 Team` was parsed as `required_team_count=1`, so the
deterministic selected-Team path created one obligation and no edges. This is a
contract parsing/compilation failure, not provider inactivity or an early-stop
condition. The follow-up candidate adds a narrow cardinality parser for that
qualified form and carries an explicit fan-in constraint into the semantic
proposal, where the final declared Team gets typed dependencies on its declared
predecessors. The repair has unit and compiler coverage but requires a fresh
real-Qwen run before it can close this gate.

## 2026-08-23 candidate-freshness correction

The first rerun after that repair also reported one Team, but it did **not**
exercise candidate `05de14d0`: `harness-eval-real-qwen.sh` started the existing
`target/debug/cowd` Gateway and only then compiled `harness-eval`. The Gateway
therefore served a stale Runtime binary. Its retained report is diagnostic of
the runner defect, not evidence that the parser repair failed:

```text
/tmp/cowd-real-qwen-evidence.UJPUdl/runs/v0.9.703-1787439975-mission-harness-deep/report.json
```

The scenario script now rebuilds the default `cowd` binary before starting the
isolated Gateway and exports its SHA-256 into the evaluation environment. An
explicit `COWD_BIN` is treated as an operator-supplied immutable artifact and
must already be executable. The next provider run is consequently bound to the
current source candidate; this correction is not itself a passing real-model
result.

## 2026-08-23 fresh-binary fan-in execution result

After the candidate-freshness repair, the strict scenario exposed and then
closed two runtime defects: directory-scoped Focus verification incorrectly
called `read_file` on the directory itself, and the Supervisor's direct child
join omitted the child Team's terminal acceptance/artifact facts. The latter
left a merge Team permanently blocked even when both producers had completed.

Candidate `c5258653` was rebuilt into Gateway binary
`0f1ab221478bbcfb1df7641053a6100cc5c908d18b8c95188a7e85308cb4ad9c`
and then executed against real `qwen3.7-plus`. The structural collaboration
sub-gate now passes: the public projection records three completed Teams, nine
completed Agents, zero failed Agents/Teams, two claimed typed handoff edges,
and a completed Program lifecycle. This is real execution evidence, not an
inferred topology result.

```text
/tmp/cowd-real-qwen-evidence.ExlMS6/runs/v0.9.703-1787442943-mission-harness-deep/report.json
sha256 984ae2561f7f27cb1d587387eee362c1d24f5fbd5844832602b1b18af1b8fe56

live-scenarios/004-live_team_projection.json
sha256 8aaaacad848e9f65d7a7f21bc5d9efc57af111596392446bef1501146203db8d
```

The whole suite remains failed and therefore does **not** close P6. Its
remaining defect is the root final presentation: it retained an earlier model
claim that the Teams could not start, although the canonical Program completed
afterward. The architecture-quality evaluator correctly rejects that stale,
contradictory text (5/9). The next repair must make root presentation consume
the completed Program's canonical terminal result instead of an earlier model
answer; weakening the quality gate would conceal the defect.

## 2026-08-23 strict-gate rerun: structural pass, terminal-format failure

Candidate `0e6b19f8` was rebuilt into Gateway binary
`84b64905bcc99d94557417fd1cbe17c490d3969f669f7ee45cb3027e284ab318` and
executed against real `qwen3.7-plus` with the same isolated-Gateway command
and an 1800-second process ceiling. This run is deliberately recorded as a
failure, not as P6 evidence of completion:

```text
/tmp/cowd-real-qwen-evidence.OoJZbh/runs/v0.9.703-1787444861-mission-harness-deep/report.json
sha256 f59036a035591f80be69c3640e5911041a2eba32160daa7c118d1f6e97a04b17

live-scenarios/004-live_team_projection.json
sha256 19e2660769333b8a41f63a338d5ba97da8b6f11e15bf5fbe9ceee83bcc9ecac7
```

The durable/public facts passed: three completed Teams, nine completed Agents,
two claimed cross-Team edges, no failed Team or Agent, and real provider/model
`qwen3.7-plus` without fallback. The suite nevertheless failed because its
terminal architecture-quality score was 7/9. The accepted Markdown conclusion
correctly said that all three Teams completed, but `objective_requires_strict_json`
classified the prompt's phrase "JSON, Markdown headings, or `Field: value`"
as a strict-JSON contract merely because it mentioned JSON. `Synthesize` then
rejected the valid Markdown `assistant_json` candidate and asked the narrator
to explain the default placeholder "Execution ended without a qualified root
answer candidate." The narrator consequently contradicted the durable Team
facts.

The following candidate narrows that format classifier to explicit strict-JSON
requirements and has a unit regression for JSON as one alternative among
Markdown/field formats. It must receive a fresh real-Qwen run; this failed run
does not close the structural sub-gate or P6.

## 2026-08-23 strict-gate real-Qwen pass

Candidate `5480a363` repaired the last root-presentation bridge without
changing the Team terminal-result contract: the reducer retains a bounded,
runtime-derived evidence bundle only after every non-reducer worker has
completed with durable evidence and the Team delivery envelope is fully
satisfied. The parent carries that summary separately from its mechanical
`delivery-envelope:` result reference, and the root can therefore publish
verified completed-Team evidence if the provider path would otherwise replace
it with a contradictory answer.

The isolated-Gateway real run passed all four production scenarios with the
requested `qwen3.7-plus` model and no fallback. In the strict collaboration
scenario it recorded three completed Teams, nine completed Agents, zero Team
or Agent failures, two claimed typed cross-Team handoff edges, and a 9/9
architecture-quality score. The root response included the deterministic
delivery-risk status (`no unresolved delivery-contract findings`) alongside
the checked source evidence; it did not claim that completed Teams were
unexecuted.

```text
Gateway binary sha256 592ddb920c91a7ca8f3c051873d09827614d08c60b665521033d08202f406cd3
/tmp/cowd-real-qwen-evidence.m8Gu9L/runs/v0.9.703-1787448771-mission-harness-deep/report.json
sha256 6d0ebe3f89b0c61782bc6022117290010fa19302d44f6514cb4c63152f8f96b6

live-scenarios/004-live_team_projection.json
sha256 aeeb4ae2d5b8f049046a242cf0630bfe2faea6b64c2ce40a60944e2a16b38950
```

This closes the strict real-Qwen Team/fan-in presentation sub-gate only. P6
release closure still requires the remaining cross-session, approval,
PostgreSQL, and Surface evidence gates from the approved plan.
