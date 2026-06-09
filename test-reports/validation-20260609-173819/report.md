# Cowd Segmented Validation - 20260609-173819

- workspace: `/media/yi/Datas/workspace/cowd`
- target: `/tmp/cowd-target-v0972-speed`
- scope: `full`
- cargo incremental: `0`
- cargo jobs: `12`
- /tmp available KB at start: `15795852`

## Commands

| step | status | seconds | target before | target after |
| --- | ---: | ---: | ---: | ---: |
| `cargo_fmt` | 0 | 1 | 15344527566 | 15344527566 |
| `cargo_test_api` | 0 | 15 | 15344527566 | 15343922353 |
| `cargo_test_commands` | 0 | 1 | 15343922353 | 15343862457 |
| `cargo_test_compat-harness` | 0 | 14 | 15343862457 | 15344685030 |
| `cargo_test_cowd-memory` | 0 | 9 | 15344685030 | 15344685030 |
| `cargo_test_mock-anthropic-service` | 0 | 1 | 15344685030 | 15344685030 |
| `cargo_test_plugins` | 0 | 0 | 15344685030 | 15344685030 |
| `cargo_test_runtime` | 0 | 25 | 15344685030 | 15344820719 |
| `cargo_test_telemetry` | 0 | 0 | 15344820719 | 15344820719 |
| `cargo_test_tools` | 0 | 9 | 15344820719 | 15344969567 |
| `cargo_test_cowd-cli` | 0 | 41 | 15344969567 | 15344162714 |
| `cargo_build_cli` | 0 | 14 | 15344162714 | 15344162714 |
| `webui_npm_test` | 0 | 2 | 15344162714 | 15344162714 |
| `webui_e2e` | 0 | 3 | 15344162714 | 15344162714 |

## Failures

No command failures.
