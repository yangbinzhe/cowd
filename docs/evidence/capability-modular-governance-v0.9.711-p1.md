# v0.9.711 P1 capability authority and duplicate discovery

Status: passed

## Execution board

- phase/version: P1 / v0.9.711
- terminal goal: replace the ambiguous lifecycle owner boolean with capability-level participation and one global owner per state authority
- prerequisite: P0 commit `dc01e01e`
- new contract owner: `harness-contract::governance`
- Runtime binding owner: `runtime::module_authority` and `runtime::module_map`
- delete target: `RuntimeModuleDescriptor.lifecycle_owner`
- permanent gates: `runtime_capability_authority`, `capability-authority-gate.sh`, `xtask architecture duplicate-authority`, `xtask architecture duplicate-capability`
- write allowlist: harness governance contract, Runtime module authority/map/tests, Harness Eval coverage projection, xtask duplicate gates, governance registries, test inventory and this evidence
- forbidden scope: no Runtime execution, Session state, Gateway carrier, route, storage or TUI behavior changes

## Result

All 112 Runtime module descriptors have one or more explicit `CapabilityRoleBinding` records. Each record names its capability, state authority, scope, lifecycle role and writer kind.

| Invariant | Result |
| --- | ---: |
| Runtime module descriptors | 112 |
| legacy `lifecycle_owner` writers | 0 |
| globally registered state authorities | 43 |
| unregistered authority references | 0 |
| duplicate authority IDs | 0 |
| classified duplicate capability groups | 5 |

Local roles distinguish Authority, Coordinator, Worker, Projector and Adapter. External ports cannot declare Authority. Only Authority can use `WriterKind::Canonical`; Projector and Adapter are limited to projection/read-only semantics.

The former 53 booleans were not preserved as 53 renamed owners. They were regrouped by actual state machine, including conversation turn, execution graph/resource, collaboration program, provider transport/policy/catalog, tool effect/plan/policy, Mission, Session, Agent, Team, approval, context, reality, event store, policy/security/configuration and infrastructure authorities. External owners for managed workers, harness policy/evaluation, MCP, Memory, sandbox and Surface are registered without being copied into Runtime.

## Duplicate discovery

The permanent duplicate policy records source spans, owners, phase disposition and source digests. Four SQLite/PostgreSQL pairs are classified as adapters that currently duplicate semantic operations, while Gateway parallel Session carriers are classified as an active carrier duplicate. The gate parses Rust functions and requires semantic-duplicate classifications to have real shared operation names; a same-name type alone is insufficient.

## Gates executed

- `cargo check -p harness-contract -p runtime -p harness-eval -p xtask --all-targets`: passed
- `cargo test -p harness-contract governance --lib`: 1 passed
- `cargo test -p runtime --test runtime_module_architecture --test runtime_capability_authority`: 3 passed
- `cargo test -p harness-eval harness_capability_coverage_requires_runtime_state_authorities --lib`: 1 passed
- `cargo xtask architecture audit --check`: passed; 112/479/53/115 preserved
- legacy `lifecycle_owner:` production scan: zero
- structural limit gate: all P1 functions and module groups below limits
- `cargo fmt --all -- --check`: passed
- `git diff --check`: passed

## Completion state

P1 is provisionally complete. Harness capability coverage retains its stable output shape, but its lifecycle module field is now derived from unique local Authorities instead of the removed boolean.
