# v0.9.705 Semantic Compiler Evidence

## Phase board

| Gate | State | Evidence |
| --- | --- | --- |
| Contract and compiler implementation | Passed | v2 semantic contract, deterministic compiler, immutable semantic provenance, and exact Tool/Skill binding implemented |
| Contract-caller migration | Passed | All durable program constructors state the semantic provenance field; Team snapshots persist the added Tool/Skill constraints |
| Static fallback audit | Passed | `rg` found no retired lowering, fallback binding, defaulted Agent ref, clipping carrier, or obsolete unknown-Agent fixture |
| Unit and integration suite | Passed | Compiler 5/5; Team template compiler 19/19; Runtime library 1852 passed, 0 failed, 2 ignored; workspace `--all-targets` check passed |
| Gateway and browser scenario evidence | Passed / deferred by phase | Gateway bootstrap schema 3/3 passed; browser/provider scenarios remain the explicitly scheduled v0.9.707 end-to-end gate |

## Allowlist amendment

The explicit `CollaborationProgram.semantic_intent` field requires callers that
construct a durable program to state whether provenance exists. The compiler
identified `crates/runtime/src/execution_core/graph/commit_service.rs` as such a
caller. Its scope is restricted to preserving or explicitly clearing that
additive field; no commit-service lifecycle, scheduling, or authority rule is
changed. The v0.9.705 plan has been amended accordingly before the caller is
edited.

The same inspection found that Team role snapshots did not carry a semantic
Tool/Skill narrowing. `TeamRoleTaskContract` is therefore extended only with
additive, default-empty immutable constraint lists. Legacy revisions retain
their existing capability-derived behavior; v2 compilation populates the lists
and instantiation verifies and applies them before a task packet is created.

A second source audit identified durable Team-definition serialization and a
small set of colocated request fixtures as mandatory callers of the additive
field and the v2 request shape. The v0.9.705 allowlist now explicitly limits
those files to persistence/default migration and compile-checked fixture
updates; no lifecycle, authority, scheduling, or selection ownership moves
through that amendment.

## Closure rule

This file is evidence-in-progress, not a pass claim. It is completed only after
the static scans, focused tests, workspace checks, version gate, and end-to-end
scenario evidence are recorded with their commands and results.

## Executed evidence

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `git diff --check` | Passed |
| retired-path static scan | Passed; no obsolete lowering/fallback/clipping symbols or test fixture remained |
| `cargo test -p runtime intent_compiler --lib --quiet` | 5 passed, 0 failed |
| `cargo test -p runtime team_template_candidate --lib --quiet` | 19 passed, 0 failed |
| `cargo test -p runtime --lib --quiet` | 1852 passed, 0 failed, 2 ignored |
| `cargo check --workspace --all-targets` | Passed in 1m 19s |
| `cargo test -p gateway runtime_bootstrap::tests --lib --quiet` | 3 passed, 0 failed |
| `cowd-edge`: authenticated `npm run generate:api` | Passed; reviewed generated OpenAPI projection adds the existing `TeamExecutionTerminal` carrier |
| `cowd-edge`: `npm run test:api-matrix` | Passed; generic APP API matrix and production-residual gate both reported no failures |
| `cowd-edge`: `npm run build` | Passed; Vite production build completed |

## Version-close record

| Repository | Phase commit | Tree | Annotated tag |
| --- | --- | --- | --- |
| `cowd-0821-terminal` | `4816b5c802219ba48ae45b4ae105310ac92a880d` | `e4af1497b6de2fb6cfba168b4b9a0098de0cd3dc` | `v0.9.705` (retargeted to this evidence-record commit) |
| `cowd-edge` | `0a2a0183f1e60898c388807b6559435f2f175250` | `994ee69c455636b3495aa3ec108ecceb0d655ad2` | `v0.9.705` |

Both worktrees were clean at phase close. No remote was pushed.
