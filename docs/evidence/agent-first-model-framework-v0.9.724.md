# v0.9.724 release evidence

Candidate status: passed for the deterministic local release scope.

- `scripts/validate.sh release` passed on 2026-09-10: formatting, build,
  installation, doctor, full product smoke, OpenAPI generation, and TUI daemon
  attachment all completed successfully.
- `scripts/test/postgres-contract.sh` passed against the isolated local
  PostgreSQL fixture on 2026-09-10.
- The four PostgreSQL-backed Runtime, Memory, permission, and local Skill
  scenarios passed, as did the Gateway/WebUI and TUI surface scenarios.
- `scripts/test/reference-app.sh` passed with the same isolated PostgreSQL
  fixture, including Gateway catalog, supervisor, invocation, stream, and TUI
  proxy coverage.
- Real-provider certification remains external evidence and is not claimed by
  this local release record.

Release status: passed
