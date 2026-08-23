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
| P6 deletion/performance/cross-repo gates | The exact-symbol deletion gate proves the retired Host entry point, builtin Team selection summary, role/slot encodings and monetary pricing contracts are absent; 100x10 SQLite admission and Edge/MFG consumer evidence are recorded, but PostgreSQL and a shared release tuple remain open | partial | P6 release owner / PostgreSQL operator |
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

The repeatable local deletion command is
`bash scripts/test/collaboration-deletion-gate.sh`. It is deliberately
exact-symbol based: generic technical uses of words such as `cost` or `role`
are not treated as monetary pricing or role-string dispatch.

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

## Terminal implementation candidate — deterministic closure

This update closes the source-level control-plane residual that the preceding
audit intentionally left open. A root strategy that requires collaboration now
persists `runtime.control_plane.required` after the root model node commits,
with the requested Team count and required `runtime_orchestrate` tool choice.
It cannot create a Program from task-understanding text. Without a verified
proposal receipt it produces zero Program/Team instances and, after one
bounded repair, persists `runtime.control_plane.missing_proposal` as a typed
blocked receipt. The former Host natural-language Program compiler and its
test-only compiler path have been removed.

The runtime authority layer also preserves an AI-declared builtin Team role
topology while replacing its resource scopes with Runtime-owned leases. This
fixes the former failure in which a declared multi-role Team was silently
reduced to one focus by a narrow resource estimate; template validation still
rejects undeclared, unknown or dependency-incomplete roles.

The deterministic candidate evidence completed before real-provider execution:

- `cargo test -p runtime --lib -- --test-threads=1`: 1,819 passed, 2 ignored.
- `cargo test --workspace -- --test-threads=1`: passed after explicit Cowd and
  managed-worker-launcher build; the installer atomic-replacement contract
  passed.
- `bash scripts/test/postgres-contract.sh`: passed against disposable local
  PostgreSQL source/target databases, which were removed after the run.
- `bash scripts/test/collaboration-deletion-gate.sh`, `cargo fmt --all --
  --check`, `git diff --check`, and `cargo check -p runtime --all-targets`:
  passed.
- Cowd Edge: WebUI 53 files / 438 tests plus its contract gates, production
  WebUI build and release sidecar build passed. Cowd App MFG: Rust workspace,
  WebUI typecheck, 13 files / 91 tests, browser gate and production build
  passed. The MFG Rust build reports nine pre-existing dead-code warnings in
  its database adapter; they are outside this Cowd candidate and do not affect
  its test result.

The next and final evidence boundary is one clean-SHA Token Plan run of the
strict real-model scenario. Its report must independently prove the multi-Team
fan-in, escalation, continuation, cross-session policy and projection facts;
no earlier real-model report is reused for a changed candidate.

## Provider protocol repair — preflight for the next candidate

The first clean Token Plan run of the preceding candidate did **not** pass and
is not acceptance evidence. Its preserved report is
`target/acceptance/real-qwen/runs/v0.9.704-1787479938-mission-harness-deep/`;
the isolated Gateway recorded a provider HTTP 400 before either failed scenario
completed a model round: the configured model rejected an explicit
`tool_choice` while its thinking mode was active. The active route and model in
that report were `qwen-tokenplan` and `qwen3.8-max`, respectively.

The repair is a data-driven protocol boundary, not a new model-name branch:

- `~/.cowd/models.yaml` declares `no_explicit_tool_choice` and
  `openai_compat_enable_thinking` for the configured hybrid models. The real
  evaluator copies that registry into its isolated HOME, so it exercises the
  same declared route and capabilities as the interactive Gateway.
- Runtime captures those configured capabilities in its immutable provider
  request profile and passes them to the provider request as transport-local
  metadata. A one-request `reasoning_effort=none` override is honored only
  when that capability is present; it then becomes the provider's documented
  `enable_thinking=false` wire field.
- The capability profile already maps `no_explicit_tool_choice` to omission of
  the unsupported field. Runtime still constrains the tool exposure and checks
  the durable control-plane receipt, so compatibility fallback cannot invent a
  Team or weaken governed admission.

The changed dependency cone has deterministic provider-wire and Runtime
behavior tests. After it is committed as a clean candidate, the strict real
scenario will be run exactly once for that SHA; only that result may reopen the
release/tag decision.

## Strategy-lease coherence repair — preflight for the final candidate

The second clean Token Plan run reached real Team/Agent control-plane behavior
without a provider protocol error, but it did not pass the strict release gate.
Its preserved report is
`target/acceptance/real-qwen/runs/v0.9.704-1787482602-mission-harness-deep/`.
Three of five production scenarios passed; the two remaining live scenarios
were rejected with `model_proposal_conflicts_with_strategy_lease`. The model
first read a `Collaborate` lease from `runtime_capabilities`, then its typed
`runtime_orchestrate` proposal was checked against `Execute`. This is a
Runtime coherence fault, not a malformed model proposal and not a provider
failure.

The final candidate makes the lease boundary atomic:

- Every admitted turn immediately publishes its sole Runtime-owned decision to
  the Gateway executor transport cache. Capability discovery and a later
  proposal therefore begin from the same lease identity.
- When the root has an explicit Team contract, Runtime pins that admitted
  decision to `Team`/`Collaborate` before it exposes the control-plane tool
  set. The pin is durable as a strategy-selected event and preserves the lease
  identity across the revision.
- This does not synthesize a Program, a Team name, a role topology or a
  proposal. The model must still submit the typed control-plane proposal and
  only its verified durable receipt can admit execution.

The local regression set passed before candidate commit:

- `explicit_root_collaboration_contract_pins_the_admitted_lease_before_control_plane`
  proves that the required contract retains the one admitted lease while
  selecting `Collaborate` and a Team candidate.
- `model_team_proposal_retargets_within_the_same_strategy_lease`,
  `host_does_not_materialize_required_teams_before_root_control_plane_receipt`,
  and Gateway lease-reuse coverage all pass.
- `cargo fmt --all -- --check` and `cargo check -p runtime --all-targets`
  pass.

The next strict real-model run is reserved for the exact clean commit containing
this repair. No report from either earlier SHA is used as final acceptance
evidence.

## Custom-template focus preservation repair — preflight for the next candidate

The third clean Token Plan run preserved the strategy lease repair but did not
pass the strict live Gateway gate. Its preserved report is
`target/acceptance/real-qwen/runs/v0.9.704-1787484772-mission-harness-deep/`.
The provider route remained `qwen-tokenplan` / `qwen3.8-max`; there was no
provider protocol failure and no strategy-lease conflict. It passed every
deterministic, context-governance, mission-closure, next-generation and
complex-scenario gate, while live Gateway scenarios were 3/5.

The raw durable tool receipts isolate the remaining failure precisely. The
model supplied typed focus objects for each Team node of the workspace
multi-role templates. `bind_semantic_resource_authority` then erased those
focuses for `workspace/` and `user/` templates in an attempt to avoid
injecting Runtime builtin roles. The downstream template compiler correctly
received an empty focus set and rejected a multi-role template as ambiguous.
The failure was therefore an internal loss of valid model semantic topology,
not malformed model JSON.

The repair preserves model-declared custom-template focus ids, role ids,
objectives and evidence responsibilities. Runtime binds their authority only
to the already-bounded node resource lease; it never invents a role, template,
Team name or dependency. An empty focus set remains empty and a multi-role
template still fails closed, so ambiguity does not become an implicit whole-
template expansion.

The changed regression proves that a custom write-capable template retains its
declared two-role topology and that each role receives only a Runtime-derived
subset of the node lease. Existing builtin escalation and root strategy-lease
tests also pass. The next real run is reserved for the exact clean commit of
this repair.
