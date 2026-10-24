#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ "${COWD_AI_HARNESS_LIVE:-0}" != "1" ]]; then
  cat <<'MSG'
NotStarted: provider live validation is disabled.

Set COWD_AI_HARNESS_LIVE=1 to run one bounded real provider request using
the active runtime model or COWD_AI_HARNESS_LIVE_MODEL.
MSG
  exit 2
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

report="$(python3 scripts/test/cargo-selection.py report)"
if [[ -n "$FILTER" ]]; then
  timeout "${COWD_AI_HARNESS_LIVE_TIMEOUT:-120s}" \
    python3 scripts/test/cargo-selection.py run --package provider --test provider_config_live_smoke \
      --filter "$FILTER" --ignored --entry 'scripts/validate.sh manual live-provider' \
      --nocapture --output "$report"
else
  timeout "${COWD_AI_HARNESS_LIVE_TIMEOUT:-180s}" \
    python3 scripts/test/cargo-selection.py run --package provider --test provider_config_live_smoke \
      --all-in-target --ignored --entry 'scripts/validate.sh manual live-provider' \
      --nocapture --output "$report"
fi
