#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ "${COWD_AI_HARNESS_LIVE:-0}" != "1" ]]; then
  cat <<'MSG'
Provider live validation is disabled.

Set COWD_AI_HARNESS_LIVE=1 to run one bounded real provider request using
the active runtime model or COWD_AI_HARNESS_LIVE_MODEL.
MSG
  exit 0
fi

timeout "${COWD_AI_HARNESS_LIVE_TIMEOUT:-90s}" \
  cargo test -p provider --test provider_config_live_smoke \
    -- --ignored --nocapture provider_config_live_smoke_returns_structured_health_signal
