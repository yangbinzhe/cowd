# Harness Certification Manifest v1

`harness-eval certify` is the terminal evidence gate. Quick and full evaluation
remain regression lanes; they cannot certify a production chain.

## Invariants

- Verify the immutable fixture digest before executing the scenario.
- Reject a non-empty output directory so a prior run cannot satisfy a new run.
- Execute the top-level scenario command before collecting independent sources.
- Snapshot file-backed sources before execution and reject unchanged evidence.
- Collect every source before evaluating any expectation.
- Preserve the exact manifest and every collected byte stream with SHA-256.
- Never derive an observed value from an expected value.
- A missing, timed-out, non-2xx, or non-zero required source fails closed.
- Cross-source identity checks compare two independently collected values.
- Commands execute directly without a shell, relative to the manifest
  directory unless `cwd` is absolute.

## Required Scenario Contract

Every manifest declares:

- `id`, `run_id`, `capability`, immutable `fixture`, and deterministic `seed`
- one direct `command`
- `expected_events` and `forbidden_events`
- required file-backed `evidence_paths`
- dynamic `timeout_policy`
- optional expected `failure_code`
- `provider_requirement`: `none`, `configured`, or `real`
- sorted `load_levels`
- an immutable `baseline_commit`
- `pass_thresholds` matching every numeric check

The scenario process receives these environment variables:
`COWD_CERTIFICATION_ID`, `COWD_CERTIFICATION_RUN_ID`,
`COWD_CERTIFICATION_CAPABILITY`, `COWD_CERTIFICATION_FIXTURE`,
`COWD_CERTIFICATION_FIXTURE_SHA256`, `COWD_CERTIFICATION_SEED`,
`COWD_CERTIFICATION_BASELINE_COMMIT`,
`COWD_CERTIFICATION_PROVIDER_REQUIREMENT`,
`COWD_CERTIFICATION_LOAD_LEVELS`, and
`COWD_CERTIFICATION_OUTPUT_DIR`.

## Sources

Supported source kinds are `runtime_events`, `database_state`, `process_log`,
`provider_trace`, `tool_trace`, `surface_receipt`, and `runtime_health`.
Collectors are `file`, `http_json`, and `command`. HTTP and command collectors
default to a 30-second collection timeout; production manifests should set an
explicit timeout appropriate to the probe.

## Selectors And Comparisons

Selectors:

- `json_pointer`
- `json_pointer_length`
- `event_kind_count`
- `text`
- `http_status`
- `exit_code`
- `byte_length`

Comparisons:

- `exists`
- `non_empty`
- `equals`
- `contains`
- `at_least`
- `at_most`
- `equals_observed`

`equals_observed` is the reverse-chain primitive. Use it to prove that a
Surface receipt, durable database record, Runtime event, and provider/tool
trace refer to the same execution instead of merely proving that each source
contains some plausible value.

Run:

```bash
cargo run -p harness-eval -- certify \
  --manifest crates/harness-eval/templates/certification-manifest-v1.json \
  --output /tmp/cowd-certification
```

The JSON template intentionally names files that a scenario driver must
materialize and contains a placeholder command and baseline. Replace those
values for a real run. Missing or unchanged observations, a changed fixture,
stale output, or a failed scenario command fail closed; the template is not a
self-passing fixture.
