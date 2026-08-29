# v0.9.711 P2 Route, Schema, and Surface SSOT Evidence

## Scope and provenance

- Approved plan: `/media/yi/Datas/workspace/plan/0830-cowd-v0.9.711-capability-conserving-modular-governance/plan.md`
- Approved plan SHA-256: `225b4286d5504bce28259302328d849384663064448b3a419fde4f8e1c4399a1`
- Parent phase commit: `b36a0c20203c40d6412efab122ac52ad2b22934e`
- Core branch: `master`
- Edge branch/base/result: `master` / `a17e0f012858d61af193cc7170cb28f99d54b063` /
  `34b96868ad65cc4e13f9c8c3351893525f25be89`

## Outcome

`surface::gateway_api` is the public transport authority. It declares 438 path
templates and 482 unique method/path routes. Gateway route registrations now
consume Surface path keys; Gateway owns only its 482 handler bindings and
schema/writer enrichment. TUI production code consumes the same typed keys and
contains zero raw `/api/` route literals. Edge was regenerated from the current
source Gateway and embeds both OpenAPI and Surface catalog digests.

The frozen P0 count of 479 was corrected to 482. The deleted build parser
skipped `route_registry.rs`, although Runtime mounted its three execution
projection routes. The complete correction is recorded in
`tests/test-governance/gateway-route-baseline-correction-v0.9.711.yaml`; no route
was added or removed by the correction.

## Deleted duplicate authorities

- Gateway `build.rs` Rust route-source parser and generated OUT_DIR registry.
- Literal Axum registration paths across all route modules.
- Method/path strings in `TypedRouteSpec`; schema metadata now references a
  Surface route key.
- 198 production TUI path definitions (all production raw path matches are
  zero); dynamic skill and APP operations select a closed typed route set.
- Inline 13k-line API test module and inline Strategy tests. All tests were
  moved without changing their names or shared fixture scope.

Validation-rich hand-authored schemas are explicitly governed as one
exceptional family (`gateway.api-contract`, 67 public components) with a golden
test. All derivable canonical domain schemas continue through `JsonSchema`.

## Structural evidence

- `crates/gateway/src/api_routes/mod.rs`: 14,767 -> 1,483 lines.
- `crates/harness-contract/src/strategy/mod.rs`: 5,191 -> 3,718 lines.
- Six API test shards: 2,181-2,278 lines each; historical shared test scope is
  preserved with `include!`.
- Surface catalog: 4,704 lines; Gateway binding inventory: 2,450 lines.
- Source-size exceptions for the two completed P2 files were deleted.
- Structural gate now compares violations to the frozen Git baseline and fails
  only new or enlarged debt. New files still have no baseline allowance.

## Contract and generated-artifact evidence

- Surface route catalog digest:
  `44b9c84d1577964eb1f4f9a4ca2f2cf60a12574940aa75245fd4a2227f8d3f01`
- Edge source OpenAPI digest:
  `fe026468985e6ed89c93f48fd71c4858e65ecdb866a8bd0cdc355a23f224351e`
- `route_contract_parity`: Surface = Gateway binding = OpenAPI, 482 routes.
- Edge generator ran against a current-source isolated Gateway; the old 8642
  installation described by the handoff was not used.

## Verification

- `tests/architecture/route-contract-gate.sh`: pass.
- `cargo test -p gateway api_routes:: --lib -- --nocapture`: 228 passed, 2
  script-owned ignored, 0 failed.
- `cargo test -p tui gateway -- --nocapture`: 94 passed, 0 failed.
- `cargo test -p harness-contract strategy:: -- --nocapture`: 55 passed, 0
  failed.
- `npm --prefix surfaces/webui test`: 53 files / 439 unit tests passed; i18n,
  governance (115 entries), API matrix, presentation, capability parity, raw
  payload, secondary sections, and acceptance gates passed.
- `cargo xtask architecture audit`: 112 Runtime modules, 482 routes, 53 tools,
  115 Edge acceptance entries, 43 authorities, 5 classified duplicate
  candidates; source-size and structural gates passed.
- `git diff --check`: pass.

## Performance

Candidate report:
`test-reports/performance-v0.9.711/p2-candidate.json`, SHA-256
`e62ba53f481eef77ceb8de36ad50f11d854c41064b72558497806f10084d8616`,
worktree digest
`b037a7396f7edd1b188a0de9146f82adbfa039c8a7b2e889d117e283a487cf78`.

Median change versus the frozen P0 baseline:

- active session: +1.16%
- session activation: +0.55%
- deterministic six-team: +15.91%
- TUI refresh: -3.48%
- route generation: +4.72%
- backend page: -4.37%

All six are within the 10% no-regression phase gate. These are process-level
sentinels; they are not used to claim operation-level improvements reserved for
P3-P6.

## Residual plan scope

P2 is closed. `tui/gateway/gateway_client.rs` remains a transitional carrier
for P6, but its route authority has already moved. P3-P7 remain unimplemented
and are not claimed by this evidence.
