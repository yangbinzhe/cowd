#!/usr/bin/env bash
# Only close resources whose existing PG owner validates a closeout receipt.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$repo/scripts/test/build-env.sh"
printf '%s\n' 'before:'
df -Pk / "$COWD_BUILD_ROOT"
status=0
if (($#)); then
  if (($# != 2)); then echo 'usage: reclaim.sh [owner-report closeout-receipt]' >&2; exit 2; fi
  python3 "$repo/scripts/test/pg-owned-run.py" --close-retained --report "$1" --closeout-receipt "$2" || status=$?
fi
printf '%s\n' 'after:'
df -Pk / "$COWD_BUILD_ROOT"
exit "$status"
