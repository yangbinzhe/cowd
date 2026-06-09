# Cowd Segmented Validation - 20260609-182523

- workspace: `/media/yi/Datas/workspace/cowd-develop`
- target: `/tmp/cowd-target-v0975-speed`
- scope: `fast`
- cargo incremental: `0`
- cargo jobs: `12`
- /tmp available KB at start: `28522912`

## Commands

| step | status | seconds | target before | target after |
| --- | ---: | ---: | ---: | ---: |
| `cargo_fmt` | 0 | 1 | 2217321414 | 2217321414 |
| `cargo_test_plugins` | 0 | 3 | 2217321414 | 2300356658 |
| `cargo_test_telemetry` | 0 | 1 | 2300356658 | 2310813775 |
| `cargo_test_memory_tuner` | 0 | 15 | 2310813775 | 5007771407 |
| `cargo_test_runtime_worker_state` | 0 | 32 | 5007771407 | 7153944492 |
| `webui_npm_test` | 0 | 1 | 7153944492 | 7153944492 |

## Failures

No command failures.
