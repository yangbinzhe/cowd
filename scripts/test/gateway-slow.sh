#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
echo "scripts/test/gateway-slow.sh is kept as a compatibility alias." >&2
echo "Use scripts/test/gateway-global-env.sh for the canonical serial global-state lane." >&2
exec "$ROOT/scripts/test/gateway-global-env.sh" "$@"
