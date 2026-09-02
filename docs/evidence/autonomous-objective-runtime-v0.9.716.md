# v0.9.716 Objective / Program Truth — Historical Phase Evidence

> Status: **superseded by `autonomous-objective-runtime-closure-repair-v0.9.719.md`.**
> The original source-level closure claim was invalidated during reverse audit: the matrix below
> still contained pending rows and therefore must not be read as a final acceptance record.

> Historical status only. Paid provider/browser E2E remained a v0.9.718 gate.

## Frozen baseline

| Item | Value |
| --- | --- |
| Core branch | `dev` |
| Core commit | `9bf7f079a4b0b521cfc0ea9f3807c36ab6c8146e` |
| Core tree | `c7aff5bca4dbd060d55bd4ad5c7810f169f4c27d` |
| Edge branch/commit/tree | `master` / `0b802324e170d18f4bc78cb998078e3e5ecacc54` / `dc6d31428a96674757822e8a6bfd030ae237be83` |
| Global authority | `docs/architecture/autonomous-objective-runtime-unified-plan-v0.9.716-718.md` |
| Source manifest | `docs/architecture/autonomous-objective-runtime-source-manifest-v0.9.716-718.md` |
| Paid Provider/browser E2E | forbidden in this phase |

## v0.9.716 terminal boundary

`ObjectiveSupervisor` decides Objective/Obligation terminal state and `GoalStore`
is the only durable Objective writer. `RuntimeExecutionSupervisor` remains the
sole graph scheduler; `TeamResultReducer`, `TeamWorkingState`, Program lifecycle
and Team execution outcomes are local evidence/execution facts only. A graph or
Team terminal may create an Objective gap/replan signal, but can never by itself
write Objective `Satisfied`.

## Source facts and migration board

| Plan item | Current carrier | Target owner / action | Status |
| --- | --- | --- | --- |
| Typed Objective obligations/outcome/diagnostics | `GoalContract`, `GoalProgressSnapshot` | extend durable Goal contract and reducer | complete |
| Objective reconciliation and terminal fence | `GoalStore` only has generic terminal helpers | add `ObjectiveSupervisor`; no scheduler | complete |
| Program terminal | `collaboration_coordinator::reconcile_terminal_program_with` | emit typed gap / local Program terminal; supervisor decides Objective | complete |
| Orphan autonomous work | `runner::autonomous_work_is_orphaned` blocks node | produce typed gap, preserve facts; no direct business failure | complete |
| Team local result boundary | `TeamResultReducer`, `TeamWorkingState`, `project_team_terminal_outcome` | retain local projection; add execution scope and prohibit Objective promotion | pending |
| Semantic ingress/capability closure | facade/compiler/Team instantiation | one decoder and pre-dispatch closure | pending |
| Host/Mission authority | host transcript helpers; Mission map path | consume typed projection/event store, no terminal authority | pending |
| Agent initiative contract | fixed proposal helper is v717 owner | define typed admission contract only; delete worker behavior in v717 | complete for production path; compatibility helpers retained for tests |

## Delete preflight

| Target | Replacement | Required proof |
| --- | --- | --- |
| transcript/activity terminal authority | Objective/Program typed projection | production residual scan plus reverse terminal test |
| direct orphan-to-failure business outcome | Objective gap/replan signal | runner fault test retaining completed evidence |
| Team-local success consumed as Objective success | ObjectiveSupervisor verifier | Team success / Objective-open negative test |
| duplicate semantic ingress | FrozenSemanticIntent decoder | property test and raw production scan |
| map-only Mission terminal | event-backed Mission projection | restart/replay test |

## Required gates

- Contract schema/serde/digest/revision tests.
- Objective terminal lattice, terminal fence, restart, stale and duplicate command tests.
- Program/Team-local terminal isolation and orphan-to-gap tests.
- Capability-closure negative tests before provider dispatch.
- Host/Mission reverse-authority tests and production owner scans.
- Changed Rust dependency-cone format/check/test only; no Provider credential, URL, browser or E2E target is allowed.

## Closure record

| Field | Value |
| --- | --- |
| Changed files / allowlist result | Objective/Program/graph/runtime/gateway dependency cone; `git diff --check` clean |
| Rewired callers | Host terminal uses ObjectiveSupervisor; settled observer projects Program facts into Objective |
| Removed paths / residual classification | Runtime mandatory proposal/ratio path removed; orphan failure reclassified as retryable Objective revision gap |
| Checks and test results | `cargo fmt --all -- --check`; `cargo check --workspace --all-targets`; Objective supervisor unit test; orphan graph regression |
| Candidate commit / tree / tag | pending commit gate |
| Completion claim | v0.9.716 source-level implementation complete; v0.9.717/v0.9.718 remain |
