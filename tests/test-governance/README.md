# Cowd Test Governance

This directory tracks which tests are default gates, manual diagnostics,
nightly checks, or delete candidates.

The goal is to keep the test suite focused on critical control points:

- stable kernel contracts stay in Rust unit/contract tests;
- high-risk cross-module behavior uses a small number of golden paths;
- tests that mutate process-global env/cwd/provider/session state run in the
  serial-global lane instead of the default parallel Rust test lane;
- interactive, live, LLM-judged, visual, and exploratory tests stay out of
  release gates until promoted deliberately;
- new default tests must replace overlapping coverage instead of adding another
  layer of duplicate validation.

`test-inventory.yaml` is the source of truth for the current V1 classification.

Current measured inventory:

- workspace Rust test entries: 3529;
- gateway lib test entries: 371;
- gateway serial-global entries: 10, normally about 2-6 seconds once compiled;
- standalone cold-target `scripts/validate.sh serial-global` was 51 seconds on
  2026-06-19, with the first test paying the gateway compile cost.

Use `scripts/validate.sh serial-global` for the serial global-state lane. The
legacy `scripts/test/gateway-slow.sh` entrypoint is a compatibility alias; the
canonical script is `scripts/test/gateway-global-env.sh`.
