# Cowd Segmented Validation - 20260609-172644

- workspace: `/media/yi/Datas/workspace/cowd`
- target: `/tmp/cowd-target-v0972-speed`
- scope: `core`
- cargo incremental: `0`
- cargo jobs: `12`
- /tmp available KB at start: `26645592`

## Commands

| step | status | seconds | target before | target after |
| --- | ---: | ---: | ---: | ---: |
| `cargo_fmt` | 0 | 1 | 4310994400 | 4310994400 |
| `cargo_test_api` | 0 | 24 | 4310994400 | 5718292268 |
| `cargo_test_commands` | 0 | 2 | 5718292268 | 5903028454 |
| `cargo_test_cowd-memory` | 0 | 23 | 5903028454 | 8475874519 |
| `cargo_test_runtime` | 0 | 30 | 8475874519 | 9994364078 |
| `cargo_test_tools` | 0 | 27 | 9994364078 | 10659300531 |
| `cargo_test_cowd-cli_core` | 0 | 48 | 10659300531 | 13529190545 |
| `webui_npm_test` | 0 | 2 | 13529190545 | 13529190545 |

## Failures

No command failures.
