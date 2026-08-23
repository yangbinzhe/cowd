# 0821 completion audit — 2026-08-23

## Authority and frozen baseline

The implementation authority is sections 13--22 of
`/media/yi/Datas/workspace/plan/0821-自主编排与跨Agent跨团队协同-版本审计与框架升级方案.md`.
The earlier P0--P6 increment notes are evidence for their stated boundaries,
not a substitute for the authority's B1--B12 and A12 closure.

| Repository | Ref | Tree/index/worktree state |
|---|---|---|
| Cowd | `integration/0821-terminal` / `b27d8ca0bc4bbbd03fc919bd08cdbdb72fc81c66` | clean |
| Cowd Edge | `master` / `be5cebe4810120fc85038ee786eed781f97de746` | clean |
| Cowd App MFG | `master` / `3d47526a37154ad58f3ecf9e174f229f10090a7d` | clean |

For all three repositories at this snapshot, the index diff hash and worktree
diff hash are SHA-256 of the empty stream and the untracked-file count is zero.

## Requirement-to-current-state matrix

| Authority gate / phase | Current proof | Status | Required owner/action |
|---|---|---|---|
| P0 cross-repo baseline, ownership and generated-manifest freeze | `0821-file-ownership.tsv` and `collaboration-baseline.json` now freeze the three checked trees, plan/ownership digests and the five Edge generated outputs; the JSON explicitly records that the release tuple is not verified | baseline complete; release tuple intentionally open | P6 release owner |
| B1 / P1 N-Team durable admission | Coordinator and durable obligations exist; P1 targeted tests record admission behavior | partial; B1 exact gate not evidenced | Runtime Coordinator |
| B2 / P3 ephemeral Team | Snapshot/AddTeam behavior is implemented and tested | partial; only the documented increment is closed | Runtime Coordinator + Team compiler |
| B3 / P2 cross-Team delivery | `CrossTeamEdgeState` and `CrossTeamInputContract` are contracts only. Runtime has no delivery/claim receipt owner or consumer activation path. | missing | Runtime Coordinator + graph transaction |
| B4 / P3 dynamic escalation | one fenced AddTeam escalation is wired; Retire, ChangeEdge, NarrowObjective and SetParallelismHint have no compile/submit path | incomplete | Runtime Coordinator |
| B5 / P4 continuation | `collaboration_continuation.rs` has resolution primitives, but no evidence of Program-first exact-set ingress, duplicate claim, deny/allow and ambiguity gate | partial/unproven | Runtime Coordinator + Host ingress |
| B6--B8 / P1/P4 capacity and recovery | lower-level graph/resource tests exist; no 0821 matrix proof joins Program resource reservations, restart/fault cases and fairness | unproven | Runtime Resource/Recovery owner |
| B9 / P2 acceptance and facts | evaluator/receipt foundations exist; no reverse-chain proof for Agent/root/Verify/Reducer plus cross-Team delivery | partial/unproven | Runtime acceptance owner |
| B10 / P5 approval and Surface | current P5 evidence explicitly leaves operator Program timeline/audit aggregation open; generated Edge/API consumer closure not evidenced | incomplete | Gateway + Edge |
| B11 / lifecycle projection | opaque node payload projection is implemented, but Program/Team/edge/wait/reason/resource/escalation typed projection is not proven end-to-end | incomplete | Gateway/Runtime projectors + Edge |
| B12 / recovery fault matrix | only startup approval-wait recovery is recorded; required registration, task-link, edge claim, receipt, terminal, continuation and cancel races are not evidenced | incomplete | Runtime recovery owner |
| P6 deletion/performance/cross-repo gates | `builtin_team_template_summaries` remains an active production selection source in `crates/runtime/src/infrastructure/capability_manifest.rs`; no 100x10, PostgreSQL, Edge/MFG or deletion audit evidence | incomplete | P3/P5/P6 owners |
| A12 real Qwen | `p6-real-qwen-e2e.md` proves a real model run and one Team with four Agents. It does not prove >=2 Teams, escalation, continuation, cross-session deny/allow, approval, PostgreSQL query summary or Surface screenshot. | incomplete | Integrated P6 owner |

## Code facts that block closure

| Symbol / path | Category | Current responsibility | Required decision |
|---|---|---|---|
| `CollaborationIntentPatchOperation::{RetireTeam, ChangeEdge, NarrowObjective, SetParallelismHint}` in `harness-contract/src/execution_graph/contract.rs` | defined but unwired contract | validates model-bound patch shapes | compile and submit through the Coordinator, or delete only if the authority removes the operation (it does not) |
| `compile_add_team_patch` in `crates/runtime/src/orchestration/collaboration_coordinator.rs` | wired narrow path | only accepts `AddTeam` | replace with operation-complete fenced patch compiler |
| `merge_collaboration_program` in `crates/runtime/src/execution_core/graph/commit_service.rs` | active additive-only carrier | rejects reuse/deletion and only adds Teams/edges | introduce governed non-additive revision behavior with cancellation/effect fences |
| `CrossTeamEdgeState` / `CrossTeamInputContract` | active carrier contract | no durable delivery or consumer-claim receipt is produced | add Coordinator-owned transaction/event/recovery chain |
| `builtin_team_template_summaries` in `crates/runtime/src/infrastructure/capability_manifest.rs` | active production legacy | feeds capability-manifest Team selection summaries | replace selection reader with published catalog + valid ephemeral snapshot, then delete selection usage |
| `docs/evidence/0821/p5-public-projection-boundary.md` residual | evidence-only | says timeline/audit aggregation remains | implement typed projection/API/Edge consumer and retire residual |

## Next closed boundary

The earliest missing business boundary is P2/B3: a Coordinator-owned,
revision-fenced cross-Team edge delivery/claim protocol. It must carry only
authorized artifact/evidence references and producer receipt identity, commit
delivery and consumer claim durably and idempotently, wake the consumer through
the existing ExecutionGraph dependency path, recover after restart, and project
the resulting edge state. AddTeam-only revisions must not be presented as a
substitute for this boundary.

No version/tag/deployment or claim of completed autonomous cross-Team
orchestration is justified at this audit point.

## Follow-up audit — candidate `eadc526a` / `5480a363`

The table above is the pre-fan-in snapshot and must not be used to describe
the current Team delivery path. The following additional evidence was verified
on the integration candidate:

- B1 durable Team obligations: `collaboration_coordinator_persists_every_compiled_team_obligation_before_admission` passed.
- B3 delivery/claim: `cross_team_edge_delivery_and_claim_are_fenced_by_node_attempts` and
  `terminal_producer_without_required_cross_team_facts_blocks_edge_durably`
  passed. The real Qwen strict fan-in scenario independently recorded three
  completed Teams, nine completed Agents, and two claimed edges.
- B4 implemented patch paths: atomically fenced retirement and objective
  narrowing tests passed. This does not by itself prove every patch variant's
  crash matrix.
- B5 deterministic continuation primitives passed same-session selection,
  duplicate CAS, cross-session deny/allow handoff authorization, and target
  session policy reauthorization tests. A single integrated real-provider
  scenario that exercises every continuation branch is still absent.
- P5 bounded performance evidence passed on this candidate: reference Surface
  contract (100 signed bundles, 256 singleflight callers, Gateway hot route
  3421 RPS versus direct 3789 RPS) and both release Runtime projection probes
  (2,000 foreground samples and a 512-item catch-up/10,000-sample probe).
  The deterministic SQLite admission-scale regression
  `one_hundred_programs_with_ten_teams_persist_all_admitted_obligations` also
  passed: 100 persisted Program roots / 1,000 admitted Team obligations in
  13.36 seconds, with 145 ms p95 per Program and an exact 100-item
  nonterminal index. It is an admission/durability scale check, not a claim
  that PostgreSQL query counts or the no-live-definition-N+1 projection gate
  have been measured.
  One immediately preceding reference-transport attempt missed its throughput
  floor under local contention; the retry passed without a threshold change,
  so performance repeatability remains a release risk rather than a closed
  100x10 database gate.

The P6 release remains **incomplete**. `COWD_TEST_POSTGRES_URL` and
`COWD_TEST_POSTGRES_TARGET_URL` are not configured in this environment, so
the required real PostgreSQL contract/query evidence cannot be fabricated.
The required 100 Program × 10 Team database measurement is also not present.
`cowd-edge` is locally four commits ahead of `origin/master`; until that
candidate and generated Edge/MFG consumer output are independently audited,
there is no single cross-repository release tuple. Finally, the real-Qwen
fan-in pass does not include the required Agent escalation, same-session
continuation, cross-session deny/allow, and approval in one controlled E2E.

## Follow-up audit — Edge and MFG consumer candidates

The independent consumer checks below were executed after the preceding
integration-candidate audit. They establish that the checked consumer trees are
buildable and contract-clean; they do **not** turn their independently selected
backend SHA into the Cowd integration candidate's release tuple.

`collaboration-baseline.json` is the machine-readable P0 snapshot for these
same checked trees. It records the plan/ownership digests, clean index and
worktree hashes, untracked counts, and all five generated Edge contract output
hashes. Its `release_tuple_verified` value is deliberately `false` and lists
the incompatible backend provenance and unpublished Edge candidate as blockers.

- Cowd Edge `04b63861e9e332576d08a2f81326942b22c92e9a` was clean and four
  commits ahead of `origin/master`. Its full WebUI gate passed: 53 test files /
  438 tests, i18n source and coverage gates, API matrix, presentation contract,
  capability parity, raw-payload, secondary-section and acceptance gates. A
  production `pnpm build` also passed. The gate's recorded backend provenance
  is Cowd `master` `b3d381ddd3c0c3d591f72f4d1f8fa9ede0b3a9e8`, not this
  integration candidate, so it is consumer health evidence only.
- Cowd App MFG `3d47526a37154ad58f3ecf9e174f229f10090a7d` was clean.
  Its WebUI `npm run typecheck`, `npm test` (13 files / 91 tests and the
  production browser gate), and `npm run build` all passed.
- The Edge and MFG checks add the previously missing independent consumer
  evidence, but neither generates nor validates a shared three-repository
  manifest. Edge remains unpublished locally and the backend identity above is
  different from the integration candidate; the cross-repository release tuple
  therefore remains unverified.
