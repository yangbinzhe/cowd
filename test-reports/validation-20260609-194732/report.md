# Cowd Segmented Validation - 20260609-194732

- workspace: `/media/yi/Datas/workspace/cowd-develop`
- target: `/tmp/cowd-target-v0976-setup`
- scope: `fast`
- cargo incremental: `0`
- cargo jobs: `12`
- /tmp available KB at start: `30632140`

## Commands

| step | status | seconds | target before | target after |
| --- | ---: | ---: | ---: | ---: |
| `cargo_fmt` | 0 | 1 | 0 | 0 |
| `cargo_test_plugins` | 0 | 3 | 0 | 92560946 |
| `cargo_test_telemetry` | 0 | 1 | 92560946 | 103019857 |
| `cargo_test_memory_tuner` | 0 | 19 | 103019857 | 3192497200 |
| `cargo_test_runtime_worker_state` | 0 | 33 | 3192497200 | 5520375646 |
| `webui_npm_test` | 0 | 1 | 5520375646 | 5520375646 |

## Failures

No command failures.
