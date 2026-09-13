# v0.9.725 release evidence

Candidate status: passed for the deterministic local release scope.

- `scripts/validate.sh release` passed on 2026-09-13: formatting, build,
  installation, doctor, full product smoke, OpenAPI generation, and TUI daemon
  attachment all completed successfully on the frozen candidate.
- `scripts/test/postgres-contract.sh` passed against the isolated local
  PostgreSQL fixture.
- The PostgreSQL-backed Runtime, Memory, permission, and local Skill scenarios
  passed, as did the Gateway/WebUI and TUI surface scenarios.
- `scripts/test/reference-app.sh` passed with the same isolated PostgreSQL
  fixture.
- The Edge release browser gate (`npm run test:e2e:release`) passed with an
  empty failure set, and the Edge unit/contract suite (`npm test`) passed.
- Deterministic workspace regression: `cargo test --workspace --all-targets
  --no-fail-fast` reported 5629 passed / 2 failed; the two failures are the
  `managed-worker-launcher` real-kernel Landlock probes, which are unsatisfied
  by this container's kernel isolation and are recorded as environment
  limitations rather than product regressions.

Release-closure fixes included in this candidate: real Agent-first worker
terminal authority, protocol non-closure handling, projection stale-revision
recovery across the read-model and live execution checkpoints, PostgreSQL-only
storage-fixture retirement, and the read lease for read-only source-analysis
scenarios.

Real-provider certification remains external evidence and is not claimed by
this local release record. The real-model collaboration gates (two-agent and
two-team autonomous convergence) remain open and are tracked separately; this
record only certifies the deterministic local release scope.

Release status: passed
