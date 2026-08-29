# v0.9.711 P0 immutable capability and performance baseline

Status: passed

## Execution board

- phase/version: P0 / v0.9.711
- terminal goal: machine-readable capability, source, structural, duplicate-authority and performance baselines without changing business behavior
- current owner: handwritten source scans and historical test inventory
- new owner: `cargo xtask architecture` plus frozen governance manifests
- delete targets: none in P0
- must wire to: release inventory, P1 authority migration, P2-P6 source-size closure and P7 reverse diff
- evidence: this document and `test-reports/performance-v0.9.711/baseline.json`
- prerequisite snapshot: Core `a78592f09cb325aa3e7e203a103bd69f7d1b2138`, Edge `a17e0f012858d61af193cc7170cb28f99d54b063`
- Core tree: `5fb8ee3d63124afb2b06674c7118979325101952`
- Edge tree: `28c92b36ed2435feb420fbd1b4f3e3402f5b15e4`
- plan digest: `225b4286d5504bce28259302328d849384663064448b3a419fde4f8e1c4399a1`
- write allowlist: xtask architecture module, governance manifests, performance harness, test inventory, Cargo lock/dependency metadata and this evidence
- out of scope: all Runtime/Gateway/TUI/Surface/storage business implementation

## Capability inventory

The generated baseline is `tests/test-governance/capability-baseline-v0.9.710.json`.

| Capability family | Frozen result |
| --- | ---: |
| Runtime module descriptors | 112 |
| legacy lifecycle owner candidates | 53 |
| Gateway method/path pairs | 479 |
| callable Tool specs | 53 |
| Tool effect resolver | independently hashed |
| Edge acceptance entries | 115 |
| oversized handwritten Core sources | 22 |
| oversized generated Edge sources | 1 |

The approved plan initially counted the effect resolver as a 54th callable Tool. Machine inventory proved that `mvp_tool_specs()` has 53 real Tool IDs. The plan and audit were corrected; no fake Tool was added to satisfy the former count.

## Performance baseline

Report: `test-reports/performance-v0.9.711/baseline.json`

Report SHA-256: `8255b29e9cae9eb09b75c6786786d42c6041e34c7b95ed4b3365288ccc9b84a6`

Each workload used 3 warmups and 20 measured executions on the frozen business tree.

| Workload | median seconds | p95 | p99 | 95% median interval |
| --- | ---: | ---: | ---: | --- |
| active-session | 0.258469 | 0.307823 | 0.313797 | 0.252392-0.264546 |
| session-activation | 1.193063 | 1.611858 | 1.629313 | 1.105522-1.280605 |
| six-team deterministic coordinator | 2.633041 | 3.393266 | 3.393591 | 2.473758-2.792324 |
| TUI bounded projection stream | 0.192935 | 0.218504 | 0.225272 | 0.189898-0.195973 |
| route generation/check | 0.253937 | 0.270649 | 0.273860 | 0.248226-0.259647 |
| backend pagination | 0.152456 | 0.172207 | 0.182831 | 0.149870-0.155040 |

These process-level measurements are regression sentinels. P3-P6 must additionally add operation-level counters for throughput/allocation claims; P7 may not infer a microbenchmark improvement from compiler or Cargo overhead.

## Gates executed

- `cargo check -p xtask --all-targets`: passed
- `cargo test -p xtask`: 9 passed
- `cargo xtask architecture inventory --check`: 112/479/53/115, passed
- `cargo xtask architecture source-size --check`: 22 transitional + 1 generated, passed
- `cargo xtask architecture structural-limits --check`: passed
- `cargo xtask architecture duplicate-authority --check`: 53 classified legacy candidates, passed
- all six performance workload filters were proven to execute real tests; the initial zero-test TUI filter was rejected and replaced
- `cargo fmt --all -- --check`: passed
- `git diff --check`: passed

## Completion state

P0 is provisionally complete. It changes tooling, policy and evidence only. No Runtime, Gateway, TUI, Surface, provider, tool execution or persistence business source changed.
