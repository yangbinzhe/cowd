# Capability-conserving modular governance v0.9.711 — P7 evidence

Release status: passed

Date: 2026-08-30

Approved plan SHA-256:
`48d2f94d2c0c14deabb9e9d704167e99f4cd1b2bab20afb03ea35969c3aac012`.

## Deterministic closure

- `cargo fmt --all -- --check`, `cargo check --workspace --all-targets`,
  `scripts/validate.sh full-regression`, and `scripts/validate.sh all` passed.
- `cargo xtask architecture audit` passed with 112 Runtime modules, 482 routes,
  53 tools, 115 Edge capabilities, 43 state authorities, zero legacy owners,
  zero duplicate authorities, and zero duplicate-capability candidates.
- The real PostgreSQL contract suite passed every registered case, including
  Session/Runtime fencing, outbox, restart, migration and concurrency; Runtime,
  Matrix and Memory SQLite/PostgreSQL snapshots had identical canonical digests.
  The 400-row PostgreSQL batch path improved by 33.40% over the legacy loop.
- Edge `check` passed 53 Vitest files / 439 tests, 3,195 Chinese and 3,195 English
  keys, 115 governed acceptance requirements, production build, and the release
  browser gate. The production-preview visual audit passed all 1,806 route,
  viewport, zoom and long-content combinations with no hard finding.
- Production TUI PTY acceptance passed multi-session/two-Surface observation,
  streaming, restart, valid and malicious DSML, authorization, generation
  fencing and a real 10k-Session workload. The 10k tail/search/RSS observations
  were 1,890 ms / 301 ms / 96,212 KiB.

## Provider-account and paid-evaluation closure

Provider errors now carry `Request`, `Account`, or `Configuration` scope from
the Provider authority. A failed account is fenced across every model node in
the admitted turn; separately configured accounts remain eligible. Host emits
one deterministic blocked terminal for account/configuration failures without
replan, switch, or a paid narrator call. Offline replay covers the seven observed
failure families before any live run.

Each paid invocation admits exactly one registered scenario and requires an
explicit shared total-token lease. The canonical runner rebuilds CLI and harness,
binds commit/tree/source archive/route/scenario/binary provenance, rejects native
Bailian generation, and accepts only Qwen Token Plan or the configured DeepSeek
route. Live certification is recorded outside the repository under the approved
plan root; publication is forbidden unless those reports match this clean release
candidate and pass in the order small smoke, group-theory research, then 6-Team
collaboration.

## Performance and capability conservation

The corrected Route/OpenAPI benchmark compares the uncached authority path with
the cached production path on the same clean commit and catalog. Three warmups
and 20 samples per side produced medians of 2.616473543 s and 0.256787570 s:
90.19% faster, above the required 20%. Cached/uncached OpenAPI and app capability
contracts are exactly equal. Earlier phase-specific gates proved active-Session
publication, Session activation, deterministic 6-Team throughput, TUI allocation
and PostgreSQL batch improvements; the frozen six-workload suite stayed inside
the 5% regression ceiling.

`tests/test-governance/capability-final-v0.9.711.json` is the final machine
inventory. Its reverse diff against the v0.9.710 baseline must show no lost route,
tool, Provider protocol, persistence contract, TUI/WebUI capability, Team/Agent
collaboration, Skill/Tool coordination, information transfer, or template
customization authority.
