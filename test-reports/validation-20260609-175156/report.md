# Cowd Segmented Validation - 20260609-175156

- workspace: `/media/yi/Datas/workspace/cowd`
- target: `/tmp/cowd-target-v0973-speed`
- scope: `fast`
- cargo incremental: `0`
- cargo jobs: `12`
- /tmp available KB at start: `23883144`

## Commands

| step | status | seconds | target before | target after |
| --- | ---: | ---: | ---: | ---: |
| `cargo_fmt` | 0 | 1 | 7007440245 | 7007440245 |
| `cargo_test_plugins` | 0 | 3 | 7007440245 | 7082653465 |
| `cargo_test_telemetry` | 0 | 1 | 7082653465 | 7093110910 |
| `cargo_test_memory_tuner` | 0 | 10 | 7093110910 | 9493545695 |
| `cargo_test_runtime_worker_state` | 0 | 32 | 9493545695 | 11639662218 |
| `webui_npm_test` | 0 | 2 | 11639662218 | 11639662218 |

## Failures

No command failures.
