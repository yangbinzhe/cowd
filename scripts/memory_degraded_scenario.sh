#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHROMIUM="${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-/snap/bin/chromium}"

cd "$ROOT"

cargo test -p cowd-cli memory_without_config_returns_disabled -- --nocapture

(cd "$ROOT/webui" && npm test)
(cd "$ROOT/webui" && env PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="$CHROMIUM" npx playwright test tasks-workbench.e2e.spec.js --browser=chromium)

echo "memory degraded scenario passed"
