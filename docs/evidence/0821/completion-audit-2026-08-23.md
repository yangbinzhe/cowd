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

## Evaluator authorization-verifier repair — preflight for the next candidate

The fourth clean Token Plan run reached the repaired custom-template path but
did not pass the strict release gate. Its preserved report is
`target/acceptance/real-qwen/runs/v0.9.704-1787486325-mission-harness-deep/`.
The prior focus-loss and strategy-lease failure signatures are absent. Instead,
the evaluator's own deterministic governed read tools failed before the live
Teams could retain source-evidence receipts.

The failure was narrow and fail-closed: `run_eval_tool_call` obtained a lease
signed by `AuthorizationNegotiator`, but constructed its local `ToolHost`
without the corresponding signature verifier. `ToolHost` therefore correctly
rejected the lease as unverifiable. Production Gateway hosts already install
the same verifier.

The repair installs
`AuthorizationNegotiator::verify_lease_signature` on only the evaluator's
local host. It neither bypasses authorization nor relaxes the read-only policy;
unsigned or forged leases remain rejected. The evaluator's full real-tool
evidence regression now passes. A new real-model scenario is still required
for the exact clean commit; this preflight is not release acceptance evidence.

## Durable custom Team delivery repair — preflight for the next candidate

The fifth clean Token Plan run reached the real root control-plane and started
the requested Team work, but its Team verifier rejected completed source work
with `team_delivery_unsatisfied`. The preserved report is
`target/acceptance/real-qwen/runs/v0.9.704-1787490613-mission-harness-deep/`.
The concrete missing labels were `evidence_paths` and `findings_summary`.

These labels are custom Team acceptance criteria. The typed Team compiler
already maps such criteria to bounded Runtime evidence checks; however, the
Team-level verifier made a second and incompatible demand that a model summary
contain JSON properties with exactly those names. That is a model-format
dependency, not a missing-evidence condition.

The candidate aligns the verifier with the frozen typed contract:

- a custom, non-structured Team label is satisfied only after every role's
  own typed acceptance is Runtime-satisfied and at least one materialized,
  durable evidence receipt is retained;
- known structured fields, source verification, review, upstream evidence and
  explicit `evidence_scope:` criteria retain their existing strict checks;
- no model text, JSON compatibility key or inferred Team topology is accepted
  as evidence.

The local Qwen Token Plan registry was also corrected from the stale
`no_explicit_tool_choice` capability. The root control request already sends
the provider-compatible `enable_thinking=false` override; the registry now
allows the existing C1 required native tool-choice gate to reach the provider
for the configured Qwen models. This is a configuration capability change,
not a provider-name branch in production code.

Preflight passed:

- `cargo test -p runtime execution_core::graph::executors::verify::tests`
- `cargo test -p runtime team_instantiation`
- `cargo test -p runtime --lib orchestration_phase_gate_exposes_only_control_plane_tools`
- `cargo check -p runtime --all-targets`
- formatting and whitespace checks.

The next strict real-model run is reserved for the exact clean commit of this
candidate. It remains the sole authority for release/tag eligibility.

## Root control-plane capability-to-proposal transition — preflight for the next candidate

The next isolated Token Plan candidate was intentionally stopped after the
first failing collaboration scenario, before the unrelated escalation scenario
could spend another provider run. Gateway logged the configured route as
`qwen-tokenplan` / `qwen3.8-max`; the raw retained Gateway log is under the
isolated temporary run directory created by the harness. Direct evidence,
tool evidence and the single-architecture scenario passed.

The collaboration failure was neither a malformed Team request nor a provider
transport problem. C1 correctly exposed only `runtime_capabilities` and
`runtime_orchestrate` and correctly used required native tool choice. The
model made the permitted capability query, then returned prose rather than a
proposal. The existing one repair re-exposed the same two-tool pair, allowing
the same harmless query to consume the required action again. Runtime then
correctly recorded `missing_control_plane_proposal` with zero admitted Teams,
but it had not prevented the avoidable loop.

This candidate adds a three-state Runtime-owned control-plane transition:

- `capability_or_proposal`: the initial request exposes exactly the C1 pair;
- after a successful, committed capability receipt, `proposal_only`: the next
  request exposes and requires only `runtime_orchestrate`;
- only a successful typed Team proposal advances to `proposal_submitted`.

The phase is staged until the ToolBatch graph commit succeeds, persisted as a
Session event with the turn reference, and rehydrated from that durable event
before any later provider exposure. It does not select a template, team name,
role, focus, dependency or program on the model's behalf. A failed capability
call or failed proposal leaves the prior restriction intact; verified Team
execution remains the independent terminal acceptance condition.

Preflight passed:

- `cargo test -p runtime --lib capability_receipt_advances_root_control_plane_to_proposal_only`
- `cargo test -p runtime --lib only_a_successful_team_proposal_satisfies_root_control_plane_action`
- `cargo test -p runtime --lib orchestration_phase_gate_exposes_only_control_plane_tools`
- `cargo test -p runtime --lib host_does_not_materialize_required_teams_before_root_control_plane_receipt`
- `cargo check -p runtime --all-targets`
- formatting and whitespace checks.

The next real candidate must demonstrate the full multi-Team path, including
the subsequent escalation and continuation scenarios, before any release
claim, version change, tag or push is permitted.

### Candidate result and named-action follow-up

The first execution of the phase-transition candidate was stopped immediately
after the strict multi-Team scenario failed, before the harness could spend
provider requests on unrelated later scenarios. Its isolated artifacts are
under `/tmp/cowd-real-qwen-gateway.K8gS99`; the compiled Gateway SHA was
`5fb7ce45e76a54af259efcae28acb3ec9fce37b71ad49a95b9eab2eeab9234a4` and
startup confirmed the configured route `qwen-tokenplan` / `qwen3.8-max`.

The new phase event did occur: a successful `runtime_capabilities` receipt was
followed by the `proposal_only` restriction rather than another capability
lookup. The provider nevertheless returned prose on the one-tool continuation
and the bounded root repair ended honestly with
`missing_control_plane_proposal`, zero verified Teams. This rules out a
topology/compiler failure for that scenario and isolates a wire-level
compliance weakness: generic `tool_choice=required` asks for any tool even
when the exposed set happens to contain one function.

The follow-up changes only that wire constraint. Runtime now carries an
optional one-shot required function name. For `proposal_only`, it sends the
OpenAI-compatible named function selection for `runtime_orchestrate`; the
provider must enter the native orchestration schema, while the model still
supplies the whole typed proposal and Runtime still verifies admission and
Team evidence. The adapter fails closed if the named schema is not exposed.

Additional preflight:

- `cargo test -p runtime --lib named_governed_action_uses_provider_function_selection`
- the prior root control-plane and provider capability tests
- `cargo check -p runtime --all-targets`, formatting and whitespace checks.

This remains a preflight correction, not a release result. The next candidate
is again entitled to one clean full real scenario run only.

## 2026-08-24 local closure rerun — real-provider boundary remains blocked

The current dirty integration worktree was revalidated after the managed
escalation receipt, root control-tool contract, provider-output tolerance and
maintenance-projection fixes.  This is local evidence only; it does not create
a release tuple or substitute for the authority-plan §22.3 real-provider run.

- `cargo test -p runtime --lib -- --test-threads=1`: 1,827 passed, 0 failed,
  2 ignored.
- `cargo test -p gateway --lib -- --test-threads=1`: 795 passed, 0 failed,
  12 explicitly external/serial ignored.
- `cargo run -p harness-eval -- full --budget full`: passed, 18/18 report
  gates; report
  `/home/yi/.config/cowd/harness-eval/reports/runs/v0.9.704-1787526143-mission-harness-full/report.json`.
- `bash scripts/test/reference-app-performance.sh`: passed.  Its latest
  report records 100 signed bundles, 256 singleflight callers, direct hot
  2790.41 RPS versus Gateway 3652.77 RPS, and bounded stream TTFB/cancel.
- `bash scripts/test/runtime-projection-performance.sh`: passed.  The active
  catch-up probe observed 566.033 ms baseline versus 570.682 ms projected
  foreground mean, with p95/p99 unchanged at the test precision.
- `one_hundred_programs_with_ten_teams_persist_all_admitted_obligations`:
  passed in 10.19 seconds, p95 112.953 ms per Program, 100 indexed Programs
  and 1,000 admitted Team obligations.
- `bash scripts/test/postgres-contract.sh`: passed against two isolated,
  disposable local PostgreSQL databases, including migration/restart/fence,
  Session, Surface and cross-database copy contracts.  Both databases were
  dropped after the run.
- `bash scripts/scenarios/openapi-generation.sh check`, formatting,
  `cargo check -p runtime -p gateway --all-targets`, and
  `bash scripts/test/collaboration-deletion-gate.sh`: passed.

The real Qwen command was intentionally attempted only through
`scripts/scenarios/harness-eval-real-qwen.sh`'s configured Token Plan path.
It failed before startup because `COWD_EVAL_TOKEN_PLAN_API_KEY` is absent from
the current process.  No credential was searched for, logged or substituted,
and no fallback provider was used.  Therefore §22.3/A12 remains blocked:
there is still no clean current-candidate proof of the required multi-Team
merge, managed escalation, same-session continuation, cross-session
deny/allow, approval, PostgreSQL query summary and Surface screenshot.

## 2026-08-24 real-provider repair candidate — root workstream cardinality

The subsequent clean-candidate Qwen Token Plan run did execute against
`qwen3.8-max` (21 live provider rounds). It failed honestly: the three-Team
scenario submitted one semantic workstream containing three `focuses`, so the
Runtime correctly rejected it with
`explicit_team_requirement_count_mismatch:required=3:proposed=1`. The managed
escalation scenario separately used an unpublished focus role (`solution`),
which the Runtime correctly rejected during Team-template resolution.

The failure is not an implicit-Team or topology repair. The root control-plane
instruction now makes the wire contract explicit: each workstream represents
exactly one Team, dependencies carry the parallel/merge ordering, and root
submissions must omit `focuses` because it is not a Team list and role binding
belongs to Runtime. The public schema description carries the same boundary.

Preflight for this repair candidate passed:

- `cargo test -p runtime --lib root_collaboration_instruction_makes_team_cardinality_and_focus_boundary_explicit -- --test-threads=1`
- `cargo test -p runtime --lib capability_receipt_advances_root_control_plane_to_proposal_only -- --test-threads=1`
- `cargo test -p runtime --lib only_a_successful_team_proposal_satisfies_root_control_plane_action -- --test-threads=1`
- `cargo test -p harness-contract --lib narrow_collaboration_decision_converts_without_runtime_owned_fields -- --test-threads=1`
- `cargo check -p runtime -p harness-contract --all-targets`, formatting,
  whitespace, and `scripts/test/collaboration-deletion-gate.sh`.

This is a repair candidate only. The next clean real-provider run must still
pass every live scenario before §22.3/A12 can be closed; no tag or push is
authorized by this record.

### Follow-up: root focus partitions are not template authority

The next real run confirmed the workstream-cardinality repair: the provider
submitted distinct workstreams. It still over-specified root `focuses` with a
partial template-role set, causing the compiler to reject missing template
dependency endpoints (`workstream -> coordinator` and
`researcher -> synthesizer`). That rejection is correct, but a root semantic
decision must not be able to select template roles at all.

`ModelCollaborationControlDecision::into_runtime_orchestration_input` now
accepts the legacy `focuses` input only for transport compatibility and
intentionally discards it before producing Team graph nodes. The decision
retains workstream identity, objective, dependencies, evidence contract,
output artifacts and the explicit managed-Agent escalation requirement;
Runtime alone selects template roles and validates their dependencies.

The conversion regression uses a deliberately invalid provider role and
asserts that the compiled Team node has no model-authored focus partition.
`cargo test -p harness-contract --lib
narrow_collaboration_decision_converts_without_runtime_owned_fields --
--test-threads=1`, the root control-plane regression, formatting, and
`cargo check -p runtime -p harness-contract --all-targets` passed. A fresh
clean real-provider run remains required.

### Follow-up: preserve Runtime-owned role selection through compilation

The next clean real-provider run still reported a partial-template dependency
error even though the root converter had discarded the provider `focuses`.
Tracing the typed request found a second authority bypass: Gateway supplied no
selection mode, `bind_strategy` unconditionally rewrote that to
`ModelAssisted`, and the Team compiler then unconditionally emitted that mode.
The instantiator correctly treats a model-assisted empty multi-role request as
ambiguous, but it must not see that legacy mode for a root collaboration
decision whose concrete roles are Runtime-owned.

The narrow Gateway control-tool boundary now marks only
`submit_collaboration_decision` as `Explicit`; ordinary
`runtime_orchestrate` remains model-assisted. Strategy binding preserves an
already supplied selection mode, and the compiler carries it into the
`TeamInstantiationRequest`. Explicit mode activates the immutable full
template, which is then dependency-validated by Runtime. No provider field
can name or omit a template role.

Focused proof passed:

- `cargo test -p gateway --lib root_collaboration_decision_uses_runtime_owned_team_role_selection -- --test-threads=1`
- `cargo test -p runtime --lib root_collaboration_role_selection_survives_strategy_binding -- --test-threads=1`
- `cargo test -p harness-contract --lib narrow_collaboration_decision_converts_without_runtime_owned_fields -- --test-threads=1`
- `cargo check -p runtime -p gateway --all-targets`, formatting, and
  whitespace checks.

This repair is deliberately confined to the root collaboration tool. A fresh
clean full real-provider run is still the release authority.

### 2026-08-24 live run observation — execution progress, not an opaque wait

The clean `cbf9b6ee` candidate reached the real Qwen route and admitted root
collaboration Programs, Team bindings and provider-backed child Agents. The
role-selection repair therefore passed the previously failing compiler
boundary. The full deep harness did not finish within its configured
15-minute outer deadline: its timeout shut down the isolated Gateway while
later cross-Team/managed follow-up work remained active. Runtime durably
recorded the resulting cancellations; no report was written, so this run is
not release evidence.

The failure exposed an operator-observability gap, not a reason to relax a
Team, approval, or acceptance check. The real-provider scenario launcher now
polls the authenticated public Mission Control summary during the run and
emits compact progress records containing cursor/revision, Team and Agent
states, pending approvals, recovery count, and readiness actions. It never
prints a credential, prompt, tool input, or model output. This makes nested
Team execution and a stuck/blocked state visible while it is happening.

The launcher no longer applies an implicit whole-suite wall-clock timeout. The
five deep scenarios are serial and each has its own progress-aware maximum and
cleanup path, so a generic outer deadline could terminate a later Team graph
that was still healthy. An operator may still set `COWD_EVAL_TIMEOUT_SECS` when
needed; otherwise the suite runs to a terminal per-scenario report. A fresh
clean real run remains required once provider credentials are rotated.

### 2026-08-24 approval surface — confirmation is not a blocker

The durable approval queue already applies the required continuation contract:
a request with `blocks_execution=false` receives a risk-scaled deadline and a
`ContinueAlternative` timeout policy. It therefore cannot pin Team or session
execution; a user decision is a veto/promotion decision, not a generic stop
sign. Autonomous and trust-all profiles are separately auto-approved by the
approval router with an auditable receipt.

The authenticated Gateway projection now adds `interaction_mode` and
`timeout_behavior`, and the TUI Approval Cockpit renders blocking approvals and
non-blocking confirmations separately. A confirmation is labelled as continuing
execution and exposes its timeout continuation behavior; an unknown legacy wire
shape is deliberately treated as blocking rather than silently downgraded.

This makes the policy observable without weakening a genuine execution hold.
Focused Gateway projection and TUI rendering/parser tests pass; full
real-provider validation remains deferred until exposed provider credentials
are rotated and the isolated route can be safely re-run.

### 2026-08-24 real-provider capacity closure

The local Qwen Token Plan route was reconfigured and passed Cowd's redacted
configuration validation. A new deep-real run reached the configured route,
but the provider returned its typed `insufficient_quota` response. This is an
external account-capacity condition, not an authentication, approval, Team, or
execution-graph failure.

The run exposed a convergence defect: generic HTTP 429 was classified as
transient overload even when the provider explicitly declared account/plan
quota exhaustion. Provider error handling now recognizes quota-exhaustion
markers, suppresses same-model retries, and does not report that condition as
downstream overload. The isolated deep run was repeated against the immutable
candidate: all five live scenarios reached durable terminal reports in roughly
1.1 seconds each, with no pending approval or recovery wait. The report is an
honest failed gate because no provider capacity was available; it is evidence
of failure convergence, not release success.

Offline closure remains green: Provider's full unit suite, Runtime's serial
library suite, TUI's full library suite, Gateway projection coverage, and the
full Harness report all passed. The live report can become a success gate only
after the provider account has usable quota again; no local implementation can
manufacture that external capacity.

### 2026-08-24 CC Switch Anthropic streaming bridge compatibility

Cowd can route a configured `anthropic` provider through a loopback CC Switch
bridge, where the bridge converts the request to OpenAI Responses and keeps
its OAuth credential outside Cowd's configuration. The bridge returned valid
Anthropic SSE frames, but its `message_start` frame omitted the optional empty
`content` array. Cowd previously made that field mandatory and rejected the
entire stream before any text or tool frame could be processed.

`MessageResponse.content` now defaults to an empty list during deserialization.
The later `content_block_*` frames remain the authority for streamed content.
The regression test exercises precisely that omitted-field frame. This is a
wire-compatibility relaxation only; non-streaming response validation and all
subsequent streamed content parsing remain unchanged.

Focused proof passed:

- `cargo test -p provider streaming_message_start_accepts_an_omitted_empty_content_array --lib`
- `cargo check -p provider --all-targets`
- `cargo fmt --all -- --check`

A fresh clean deep-real run through the configured loopback bridge is required
to establish the full business-chain gate.

### 2026-08-24 CC Switch Anthropic streaming tool-input compatibility

The rerun passed the direct terminal scenario and then reached streamed tool
calls in the runtime scenarios. CC Switch emitted a standard incremental
Anthropic tool sequence: `content_block_start` named the `tool_use` but omitted
its `input`, and later `input_json_delta` frames supplied the JSON arguments.
Cowd required `input` at the start frame, so it rejected the stream before its
existing delta accumulator could apply the deferred arguments.

`OutputContentBlock::ToolUse.input` now defaults to JSON null when the start
frame omits it. This preserves strictness for all other fields and leaves the
existing incremental argument parsing as the source of the completed tool
input. A regression test deserializes the exact deferred-input start frame.

Focused proof passed:

- `cargo test -p provider streaming_ --lib`
- `cargo check -p provider --all-targets`
- `cargo fmt --all -- --check`

The next clean deep-real run is the authority for actual tool execution,
orchestration, and Team-flow behavior through the bridge.
