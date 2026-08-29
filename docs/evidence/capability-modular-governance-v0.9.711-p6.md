# Capability-conserving modular governance v0.9.711 — P6 evidence

Date: 2026-08-30
Approved plan SHA-256: `225b4286d5504bce28259302328d849384663064448b3a419fde4f8e1c4399a1`

## Result

P6 replaces implicit terminal state penetration with an explicit composition
root. `App` now owns six domain read-model slices and `TuiState` owns five UI
slices. `Deref`/`DerefMut` are absent. Gateway work is represented by typed
effects carrying session authority generations, and completion writes are
admitted only by the authority selector and reducer/domain-method boundary.

The three transitional TUI source-size exceptions were retired. Final source
sizes are:

- `app_core/app.rs`: 4,952 lines;
- `app_core/state.rs`: 4,997 lines;
- `gateway/gateway_client.rs`: 4,909 lines;
- `gateway/runner.rs`: 4,709 lines.

No state composition struct has 40 or more fields. Version-diff functions pass
the 250-line structural limit. Inline mega tests were moved into domain test
modules, and live/client/input responsibilities were extracted behind typed
module boundaries.

## Correctness and capability conservation

- `cargo test -p tui --offline -- --test-threads=1`: 1,085 passed, zero failed;
  seven documentation examples remained intentionally ignored.
- The suite covers 80/96/120-column goldens, compact/wide rendering, resize,
  Unicode/IME-safe keyboard input, approvals, session selection, slow async
  completion, stale-generation rejection and capability revocation.
- `cargo xtask architecture audit --check`: passed with 112 Runtime modules,
  482 routes, 53 tools, 115 Edge capabilities, zero legacy owners, zero
  duplicate-capability candidates and zero duplicate-authority violations.
- `cargo xtask architecture source-size --check`: passed with zero
  transitional exceptions; the only oversized source is the registered Edge
  generated client.
- `cargo xtask architecture structural-limits --check`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

The inventory equals the frozen P0 capability baseline. P6 adds no route,
tool, provider or persistence authority and removes none.

## Refresh and frame performance

Unchanged Gateway session catalogues are fingerprinted from borrowed JSON
fields before `SessionSummary` materialization. An unchanged refresh performs
no summary allocation, no sidebar replacement and no redraw. The deterministic
20-refresh test materializes 128 summaries once instead of the legacy 2,560,
a 95% reduction, exceeding the 15% allocation/parse target.

Frozen 3-warmup/20-sample results against v0.9.710 baseline:

| Workload | Median | p95 | Median improvement |
|---|---:|---:|---:|
| active session | 0.249700 s | 0.285349 s | +3.39% |
| session activation | 0.983160 s | 1.564513 s | +17.59% |
| six-Team collaboration | 2.215811 s | 3.393171 s | +15.85% |
| TUI refresh | 0.179405 s | 0.209858 s | +7.01% |
| route generation | 0.228738 s | 0.274334 s | +9.92% |
| backend page | 0.144829 s | 0.161924 s | +5.00% |

All six workloads pass the 5% no-regression gate. Candidate report SHA-256:
`d42347a25838258f646c6edcfd672555cf85544b7e567b20c10ff858920b6623`.

## Clean-tree production gate

The real Gateway/provider PTY acceptance intentionally runs after this P6
implementation is committed because the harness rejects an uncommitted source
tree. Its artifact path, idle CPU, viewport and interaction results are appended
in a follow-up evidence-only commit; no implementation change is permitted
between the clean-tree run and that evidence record.
