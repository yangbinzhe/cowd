#!/usr/bin/env bash
# Recover only locally verifiable preflight conditions; report unsafe cases.
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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
  disk)
    pilot="${COWD_PILOT_ROOT:?COWD_PILOT_ROOT required}"
    evidence="${COWD_EVIDENCE_ROOT:?COWD_EVIDENCE_ROOT required}"
    mkdir -p -- "$evidence"
    exec "$repo/scripts/test/reclaim.sh" "$pilot" "$evidence"
    ;;
  pg)
    if (($# != 2)); then
      echo 'unblock: pg requires owner report and closeout receipt' >&2
      exit 2
    fi
    exec "$repo/scripts/test/pg-owned-run.py" --close-retained \
      --report "$1" --closeout-receipt "$2"
    ;;
  tmpfs)
    parent="${COWD_TEST_PRIVATE_PARENT:-/dev/shm}"
    python3 - "$parent" <<'PY'
import os, pathlib, stat, sys
p = pathlib.Path(sys.argv[1])
if not p.is_dir() or p.stat().st_uid != os.getuid():
    raise SystemExit('private parent is not an owned directory')
try:
    os.chmod(p, 0o700, follow_symlinks=False)
except OSError as error:
    print(f'warning: cannot set private parent mode: {error}', file=sys.stderr)
if stat.S_IMODE(p.stat().st_mode) not in (0o700, 0o500, 0o600):
    raise SystemExit('private parent has unexpected mode')
print(p)
PY
    ;;
  lock)
    path="${1:?lock path required}"
    python3 - "$path" <<'PY'
import fcntl, os, pathlib, stat, sys
p = pathlib.Path(sys.argv[1])
s = p.lstat()
if not stat.S_ISREG(s.st_mode) or s.st_uid != os.getuid():
    raise SystemExit('lock is not an owned regular file')
os.chmod(p, 0o600, follow_symlinks=False)
with p.open('a+') as handle:
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('lock is still active')
    fcntl.flock(handle, fcntl.LOCK_UN)
PY
    ;;
  *)
    printf 'unblock: unsupported gate %s; retained unchanged\n' "$gate" >&2
    exit 2
    ;;
esac
