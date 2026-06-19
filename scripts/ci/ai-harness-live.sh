#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [[ "${COWD_AI_HARNESS_LIVE:-0}" != "1" ]]; then
  cat <<'MSG'
AI harness live validation is disabled.

Set COWD_AI_HARNESS_LIVE=1 and COWD_AI_HARNESS_LIVE_COMMAND to run a real API-backed deep scenario.
This lane is intentionally opt-in because it may consume model/API quota.
MSG
  exit 0
fi

if [[ -n "${COWD_AI_HARNESS_LIVE_COMMAND:-}" ]]; then
  bash -lc "$COWD_AI_HARNESS_LIVE_COMMAND"
else
  scripts/ci/ai-harness-live-provider.sh
fi
