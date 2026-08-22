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
| P0 cross-repo baseline, ownership and generated-manifest freeze | `0821-file-ownership.tsv` exists, but no `collaboration-baseline.json` or three-repo generated-output manifest is present | incomplete | P0 evidence/baseline owner |
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
