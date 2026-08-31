# Collaboration Execution Obligation v0.9.713 Evidence

Status: deterministic implementation gates complete; immutable live acceptance
is recorded in the external execution ledger tied to the candidate SHA.

## Frozen baseline

- Core: `ba9ba63bee443de801656f3c81db4a7b272e223b`, tag `v0.9.712`, clean.
- Edge: `299c2f6206f218072a4979c05e182a2e469e6e68`, tag `v0.9.711`, clean.
- Installed Core before replacement: `0.9.712`, SHA `ba9ba63`.

## Completed deterministic evidence

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets`: passed.
- harness-contract strategy tests: 61 passed, including generic responsibility
  units, explicit exact cardinality, automatic minimum cardinality and invalid
  obligation rejection.
- Runtime library: 1933 passed, 0 failed, 2 ignored in the full package run.
- Harness Eval library: 140 passed, 0 failed.
- Automatic proposal collapse, durable event/recovery/downgrade, provider
  exposure, root control-plane admission and terminal cardinality tests passed.
- TUI library: 1086 passed, 0 failed under the deterministic single-thread
  lane. A parallel whole-workspace run perturbed one pre-existing global
  performance counter by one; its exact test and complete single-thread TUI
  package both passed.
- Two pre-existing lease-clock tests also failed only during the CPU-saturated
  whole-workspace lane; both exact single-thread reruns passed, and the complete
  Runtime package had already passed. No changed file owns those clocks.
- Edge execution-lineage tests: 19 passed. Complete WebUI unit suite: 53 files,
  440 tests passed. I18n, governance, API matrix, presentation, capability,
  payload, secondary-section and acceptance gates passed.
- Edge production build passed.
- `git diff --check`, Bash syntax and enforcement-pattern scans passed.

## Static-analysis baseline disclosure

`cargo clippy --workspace --all-targets -- -D warnings` is not a green baseline
on Rust 1.94: it reports existing repository-wide lint debt in unmodified
provider, recovery, team, skill and test sources (large error variants, argument
counts, test `expect`, type complexity and similar policy lints). The changed
strategy lint exposed by that run was corrected. This phase does not mislabel
the repository-wide Clippy command as passed and does not broaden a collaboration
correctness patch into an unrelated lint refactor.

## Immutable live evidence ownership

The candidate must remain clean for the real-provider harness. Installation,
DeepSeek-only scenario reports and same-session browser parity are therefore
written below the external audited plan directory and ignored harness report
root, keyed by the implementation commit and binary digest. They are not
backfilled into this tracked file after the candidate is committed.
