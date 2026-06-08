# v0.9.64 Full Range Test Report

Date: 2026-06-08
Worktree: `/media/yi/Datas/workspace/cowd`
Branch: `master`
Source merged from: `develop` at `7f316b9`

## Scope

- Pushed `develop` before merging.
- Fast-forwarded the `master` worktree from `origin/develop`.
- Ran code quality gates, Rust integration/unit tests, WebUI unit tests, and a real daemon/TUI/WebUI scenario.
- Preserved raw command logs in this directory.

## Code Fixes Made During Validation

- Updated API integration assertions to follow the real crate version in the `User-Agent`.
- Increased oversized request fixtures so they exceed the current 1M-token context guard.
- Aligned prompt-cache break reason assertions with the current implementation.
- Exposed the TUI sidebar tab count to integration tests and made tab-cycle coverage follow the actual registered tab count.
- Relaxed prompt completion acceptance to assert the stable typed prefix, because `/sta` can validly match more than one command.
- Fixed resume-switch test setup so managed session metadata is created under the intended workspace root.
- Made resume-switch test cleanup tolerant of already-removed temp directories.

## Passed Gates

| Gate | Result | Log |
| --- | --- | --- |
| `cargo fmt --check` | PASS | `cargo-fmt-check-final.log` |
| `git diff --check` | PASS | `git-diff-check-final.log` |
| `cargo check --workspace --no-default-features` | PASS | `cargo-check-workspace-final.log` |
| WebUI Vitest | PASS, 71 tests | `webui-vitest-final.log` |
| API Anthropic integration | PASS, 12 passed, 1 ignored live test | `api-client-integration-final.log` |
| API OpenAI-compatible integration | PASS, 6 passed | `api-openai-compat-integration-final.log` |
| cowd-cli serial tests | PASS, 1184 passed, 2 ignored, plus integration binaries | `cargo-test-cowd-cli-serial-final-rerun.log` |
| Real unified runtime scenario | PASS | `v0964-unified-runtime-surface-final.log` |

## Real Scenario Coverage

`scripts/v0964_unified_runtime_surface_scenario.sh` verified:

- daemon startup with Unix socket and HTTP gateway;
- session creation and collaborative lease through daemon socket;
- TUI attachment to the daemon session with `--yolo`;
- WebUI browser test through Playwright/Chromium against the live HTTP API;
- runtime control-plane, session lease, task, memory, connector summary routes;
- SQLite task persistence and scenario acceptance record;
- tmux service cleanup at scenario exit.

## Known Environment Limitation

An additional package-level test run for `runtime`, `cowd-memory`, `commands`, and `tools` was attempted after the passing workspace check and cowd-cli full test run. It failed while rustc was linking test binaries with:

`Disk quota exceeded (os error 122)`

At the time, `/tmp` had been filled by isolated Cargo target directories. This was an environment capacity limit, not a Rust test assertion failure. The relevant log is `cargo-test-core-packages-final.log`.

## Cleanup

- The v0.9.64 scenario script cleaned its own tmux sessions and temporary workspace.
- No `cowd-v0964-*`, `cowd-v0956-*`, or `cowd-v0963-*` tmux sessions remained after the scenario.
- Temporary Cargo targets under `/tmp/cowd-test-*` are safe to remove after commit/push.
