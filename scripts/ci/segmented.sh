#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LANE="${1:-${COWD_VALIDATION_SCOPE:-all}}"

case "$LANE" in
  fast) LANE="unit-fast" ;;
  core) LANE="contract" ;;
  live) LANE="scenario" ;;
  full) LANE="all" ;;
esac

exec "$ROOT/scripts/validate.sh" "$LANE"
