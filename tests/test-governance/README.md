# Cowd Test Governance

This directory tracks which tests are default gates, manual diagnostics,
nightly checks, or delete candidates.

The goal is to keep the test suite focused on critical control points:

- stable kernel contracts stay in Rust unit/contract tests;
- high-risk cross-module behavior uses a small number of golden paths;
- interactive, live, LLM-judged, visual, and exploratory tests stay out of
  release gates until promoted deliberately;
- new default tests must replace overlapping coverage instead of adding another
  layer of duplicate validation.

`test-inventory.yaml` is the source of truth for the current V1 classification.
