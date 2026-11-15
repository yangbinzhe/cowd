#!/usr/bin/env bash
# Recover only locally verifiable preflight conditions; report unsafe cases.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$repo/scripts/test/build-env.sh"
gate="${1:?gate required}"
shift
case "$gate" in
  private-root)
    path="${1:?root required}"
    python3 - "$path" <<'PY'
import os, pathlib, stat, sys
p = pathlib.Path(sys.argv[1])
if not p.is_absolute(): raise SystemExit('root must be absolute')
if not p.exists(): p.mkdir(mode=0o700, parents=False)
s = p.lstat()
if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid():
    raise SystemExit('root is not an owned directory')
try:
    os.chmod(p, 0o700, follow_symlinks=False)
except OSError as error:
    print(f'warning: cannot set private root mode: {error}', file=sys.stderr)
s = p.lstat()
if not stat.S_ISDIR(s.st_mode) or s.st_uid != os.getuid():
    raise SystemExit('private root identity changed')
if stat.S_IMODE(s.st_mode) != 0o700:
    print('warning: private root mode unavailable', file=sys.stderr)
PY
    ;;
  port)
    python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    print(s.getsockname()[1])
PY
    ;;
  evidence-path)
    python3 - "${1:?source root required}" "$COWD_GATE_LOG_DIR" <<'PY'
import os, pathlib, sys, time
root = pathlib.Path(sys.argv[1]).resolve()
directory = pathlib.Path(sys.argv[2]).resolve()
if directory.is_relative_to(root):
    raise SystemExit('configured evidence directory is inside source tree')
print(directory / f'selection-{time.time_ns()}-{os.getpid()}.json')
PY
    ;;
  disk) "$repo/scripts/test/reclaim.sh" "$@" ;;
  *) printf 'unblock: unsupported gate %s; retained unchanged\n' "$gate" >&2; exit 2 ;;
esac
