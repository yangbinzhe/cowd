# v0.9.711 P5 Persistence Semantics and Dual-Backend Evidence

Date: 2026-08-30
Plan: `plan/0830-cowd-v0.9.711-capability-conserving-modular-governance/plan.md`
Plan SHA-256: `225b4286d5504bce28259302328d849384663064448b3a419fde4f8e1c4399a1`

## Semantic ownership

- Session ingress decision encoding, terminal transcript/fence validation,
  lifecycle admission, and bounded-query policy now live under
  `session::persistence::domain`. SQLite and PostgreSQL retain only their SQL,
  row mapping, transaction, and backend error boundaries.
- Runtime transaction/event/activity-binding, terminal fence, decision lease,
  and request-hash validation now have one Runtime-owned implementation. This
  corrected a real divergence: PostgreSQL had accepted incomplete activity
  bindings that SQLite rejected.
- Runtime PostgreSQL event, task, and artifact stores are separate adapters;
  Matrix SQLite is split by entity, metric, evidence, and scenario behavior;
  Memory cognitive behavior is split into recall, write, and maintenance.
- All existing public facades remain stable: `UnifiedSessionStore`,
  `RuntimeEventStore`, Matrix repository/store ports, and Memory stores were not
  replaced by parallel authorities.

## Duplicate and source governance

- All four P5 duplicate-capability exceptions were retired. Architecture audit
  result: `classified_candidates=0`, `legacy_owners=0`, and 43 registered state
  authorities.
- All six P5 source-size exceptions were retired. The source-size result is
  `oversized=4 transitional=3 generated=1`; the three transitional files are
  the planned P6 TUI targets.
- Largest changed production files are below 5,000 lines: Session PostgreSQL
  facade 3,959; Runtime SQLite event adapter 3,471; Session SQLite facade 3,466;
  Matrix SQLite facade 2,727; Runtime PostgreSQL event adapter 2,508; Memory
  cognitive facade 2,231.
- `cargo xtask architecture audit --check`, source-size, structural-limits,
  and `git diff --check` all passed.

## Dual-backend conformance

The real PostgreSQL gate ran against a disposable database whose `public`
schema was reset before every case. All 42 cases passed. Coverage includes:

- Session generation, terminal cursor CAS, 32-way semantic idempotency,
  lifecycle recovery/tombstone uniqueness, branch atomicity, outbox failure and
  retry, lease/revision fencing, restart, migration, and selected-range reads;
- Runtime multi-stream transactions, activity bindings, terminal outbox,
  projection lanes, task concurrency, artifact scope, migration and restart;
- Matrix and Memory real adapter semantics, storage pool isolation, connector,
  Fact, Surface, and Gateway cutover contracts;
- new Runtime, Matrix, and Memory SQLite-to-PostgreSQL migration snapshots with
  exact canonical digest equality.

Offline full regressions also passed:

- Session: 111 library tests, 23 consolidated integration contracts, and 4 doc tests;
- Memory: 514 library tests plus every integration/doc target (3 explicit
  release-only performance tests and 4 examples remain ignored by design);
- Matrix repository: 18 passing library tests, 2 real-PostgreSQL tests ignored
  in offline mode, plus the compiled conformance target;
- Runtime: 1,910 library tests passed, 2 existing ignored, and every integration
  and doc target passed;
- Session/Runtime/Memory PostgreSQL offline targets passed, while every ignored
  real-database case was executed separately by the 42-case PostgreSQL gate.

## Batch and page performance

PostgreSQL `insert_messages_batch` now performs one JSONB recordset upsert in a
single transaction instead of one client/server statement per row. A permanent
real-database phase gate compares the new path with the former loop for three
alternating 400-row runs. The final full-gate median was:

- legacy loop: 190.703 ms;
- recordset batch: 112.625 ms;
- improvement: 40.94% (required: at least 15%).

Offset page reads now locate the page start through the narrow
`(session_id, sequence)` index and fetch full message bodies by keyset in both
SQLite and PostgreSQL, without changing offset semantics.

The immutable six-workload suite also remained globally green. Candidate:
`test-reports/performance-v0.9.711/p5-candidate.json`, SHA-256
`4605d59a5ce97a058df27508bf0614067368eadb1246dc2d12b91fc1aa9c45ed`,
worktree digest
`4180a7da21699935883fa2afa8be63f21a11c1dd91c3b49527f919091cb83878`.

| Workload | Median change | p95 change |
| --- | ---: | ---: |
| active-session | +2.31% | +11.92% |
| session-activation | +2.13% | +0.13% |
| six-team | +16.48% | -11.72% |
| tui-refresh | +5.37% | +9.02% |
| route-generation | +10.73% | -1.74% |
| backend-page | +6.85% | +8.22% |

Every median satisfies the 5% no-regression gate. The targeted real backend
batch improvement satisfies the P5 15% throughput objective.

## Phase decision

P5 is closed: business semantics have one owner, backend dialects remain
optimized and independently replaceable, migration digests and behavior traces
agree on real databases, duplicate/size exceptions are retired, and both global
and targeted performance gates pass without reducing capability or test depth.
