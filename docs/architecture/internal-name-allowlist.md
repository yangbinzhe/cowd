# Internal Name Allowlist

This file defines where the project name `cowd` is intentionally stable and
where it must not be used as an internal Rust boundary name.

## Policy

- External user-facing names may keep `cowd`.
- Wire schemas, config keys, environment variables, data directories, and log
  file names may keep `cowd` when changing them would break operator workflows.
- Rust crate paths, module names, service names, and new internal imports should
  not use `cowd_` or `cowd-*` unless explicitly listed below.
- New exceptions require an owner and either `external-stable` or an
  `owned-boundary-reviewed` marker.

## External Stable Names

| name | kind | owner | status |
|---|---|---|---|
| `cowd` | binary name | CLI entrypoint | external-stable |
| `COWD_CONFIG_HOME` | environment variable | config | external-stable |
| `.cowd` | project/user config directory | config | external-stable |
| `cowd-serve.pid` | local process status file | gateway | external-stable |
| `cowd-surface` | external surface monorepo for WebUI and non-TUI surfaces | surface | external-stable |
| `cowd_capabilities` | HTTP response field | gateway API | external-stable |
| `cowd_projection` | HTTP response field | gateway API | external-stable |
| `cowd_surfaces` | HTTP response field | gateway API | external-stable |
| `cowd_release_gate` | HTTP response field | gateway API | external-stable |

## Reviewed Internal Exceptions

| name | kind | owner | status | rationale |
|---|---|---|---|---|
| `runtime::cowd_dirs` | Rust module | runtime | owned-boundary-reviewed | directory helpers expose stable external paths |
| `runtime::cowd_event` | Rust module | runtime | owned-boundary-reviewed | event DTO names are part of the Gateway/TUI SSE contract |
| `crates/gateway` | crate directory | entrypoints | owned-boundary-reviewed | implementation crate retained as `gateway` while entry crates own user-facing boundaries |

## Forbidden For New Code

- package aliases such as `cowd_app_mfg::`, `cowd_memory::`, or `cowd_storage::`
- new modules named `cowd_*`
- new service names prefixed with `Cowd` when the enclosing project context is
  already cowd

## Review Gate

Every architecture review should run:

```bash
rg -n "cowd_app_mfg|cowd_memory|cowd_storage|mod cowd_|pub mod cowd_|use cowd_" crates --glob '*.rs'
```

Matches must either be removed or mapped to this allowlist.
