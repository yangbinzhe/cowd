# v0.9.723 release evidence

Candidate status: deterministic code gates passed; installed live E2E pending.

- Core PG-only source gate: passed with zero violations.
- Core dependency tree: no SQLite package or feature.
- Runtime all-target regression: passed (1780 Runtime unit tests plus integration targets; externally classified tests are recorded separately).
- Gateway full concurrency regression: passed (770 passed, 11 externally classified).
- PostgreSQL conformance: Runtime, Session, Memory, Matrix, Fact, Connector, and Surface reconstruction/concurrency contracts passed.
- Session PostgreSQL batch path: 25.07% faster than the legacy per-record comparison in the paired test.
- Runtime 64-work-item completion pump: passed.
- Projection paired foreground/catch-up performance: passed without material foreground regression.
- Memory performance: 1000-entry search 0.588ms; cached context p95 0.001ms in the recorded run.
- Edge: 448 tests and production build passed.
- MFG consumer: 109 tests, browser/build gate, and Rust workspace tests passed.

Release status: pending
