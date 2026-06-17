#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"

run() {
  echo "+ $*"
  "$@"
}

run env COWD_SCENARIO_SKIP_BUILD=1 scripts/scenarios/tui-daemon-attach.sh
