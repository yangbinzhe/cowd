# Cowd Segmented Validation - 20260609-181208

- workspace: `/media/yi/Datas/workspace/cowd-develop`
- target: `/tmp/cowd-target-v0974-speed`
- scope: `fast`
- cargo incremental: `0`
- cargo jobs: `12`
- /tmp available KB at start: `21546280`

## Commands

| step | status | seconds | target before | target after |
| --- | ---: | ---: | ---: | ---: |
| `cargo_fmt` | 0 | 1 | 9328742078 | 9328742078 |
| `cargo_test_plugins` | 0 | 3 | 9328742078 | 9407803839 |
| `cargo_test_telemetry` | 0 | 1 | 9407803839 | 9418260688 |
| `cargo_test_memory_tuner` | 0 | 15 | 9418260688 | 12043768909 |
| `cargo_test_runtime_worker_state` | 0 | 0 | 12043768909 | 12043768909 |
| `webui_npm_test` | 0 | 2 | 12043768909 | 12043768909 |

## Failures

No command failures.
