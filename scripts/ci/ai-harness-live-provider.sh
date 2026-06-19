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

MODE="${COWD_AI_HARNESS_LIVE_MODE:-smoke}"

case "$MODE" in
  smoke)
    FILTER="provider_config_live_smoke_returns_structured_health_signal"
    ;;
  stream)
    FILTER="provider_live_stream_contract_is_ordered"
    ;;
  drift)
    FILTER="provider_live_structured_output_is_stable"
    ;;
  routing)
    FILTER="provider_live_routing_respects_simple_complex_and_risk"
    ;;
  all-light)
    FILTER=""
    ;;
  *)
    echo "unknown COWD_AI_HARNESS_LIVE_MODE: $MODE" >&2
    echo "expected one of: smoke, stream, drift, routing, all-light" >&2
    exit 2
    ;;
esac

if [[ -n "$FILTER" ]]; then
  timeout "${COWD_AI_HARNESS_LIVE_TIMEOUT:-120s}" \
    cargo test -p provider --test provider_config_live_smoke \
      -- --ignored --nocapture "$FILTER"
else
  timeout "${COWD_AI_HARNESS_LIVE_TIMEOUT:-180s}" \
    cargo test -p provider --test provider_config_live_smoke \
      -- --ignored --nocapture
fi
