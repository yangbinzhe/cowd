# Internal Name Allowlist

This file defines where the project name `cowd` is intentionally stable and
where it must not be used as an internal Rust boundary name.

## Policy

- External user-facing names may keep `cowd`.
- Wire schemas, config keys, environment variables, data directories, and log
  file names may keep `cowd` when changing them would break operator workflows.
- Rust crate paths, module names, service names, and new internal imports should
  not use `cowd_` or `cowd-*` unless explicitly listed below.
- New exceptions require an owner and a delete-by version, or the
  `external-stable` marker.

## External Stable Names

| name | kind | owner | status |
|---|---|---|---|
| `cowd` | binary name | CLI entrypoint | external-stable |
| `COWD_CONFIG_HOME` | environment variable | config | external-stable |
| `.cowd` | project/user config directory | config | external-stable |
| `cowd-serve.pid` | local process status file | gateway | external-stable |
| `cowd-webui` | external WebUI repository/package name | webui | external-stable |
| `cowd-memory` | published/internal package name during migration | memory | delete_by=0.9.305 |
| `cowd-app-mfg` | package name during MFG app extraction | app-mfg | delete_by=0.9.305 |
| `cowd_capabilities` | HTTP response field | gateway API | external-stable |
| `cowd_projection` | HTTP response field | gateway API | external-stable |
| `cowd_surfaces` | HTTP response field | gateway API | external-stable |
| `cowd_release_gate` | HTTP response field | gateway API | external-stable |

## Temporary Internal Exceptions

| name | kind | owner | delete_by | rationale |
|---|---|---|---|---|
| `runtime::cowd_dirs` | Rust module | runtime | 0.9.305 | directory helpers still expose stable external paths |
| `runtime::cowd_event` | Rust module | runtime | 0.9.305 | event bus rename must be coordinated with TUI/Gateway SSE DTOs |
| `cowd_storage` | Rust dependency alias | runtime | 0.9.305 | avoids conflict while storage governance settles |
| `cowd_memory` | Rust crate import | memory | 0.9.305 | Rust package rename requires broad test and docs update |
| `crates/cowd-cli` | crate directory | entrypoints | 0.9.305 | temporary `entrypoint_legacy` library while implementation moves into `cli/gateway/tui` |

## Forbidden For New Code

- `cowd_cli::`
- `cowd_app_mfg::`
- `cowd_memory::` outside memory crate tests or migration allowlist
- new modules named `cowd_*`
- new service names prefixed with `Cowd` when the enclosing project context is
  already cowd

## Review Gate

Every architecture review should run:

```bash
rg -n "cowd_cli|cowd_app_mfg|cowd_memory|mod cowd_|pub mod cowd_|use cowd_" crates --glob '*.rs'
```

Matches must either be removed or mapped to this allowlist.
