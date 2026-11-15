#!/usr/bin/env bash
# Archive only explicitly owned, dead pilot roots; never delete unowned evidence.
set -euo pipefail
exec python3 - "$@" <<'PY'
import fcntl, json, os, shutil, sys, tarfile, tempfile
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit('usage: reclaim.sh PILOT_ROOT EVIDENCE_ROOT')
root, evidence = (Path(x).resolve(strict=True) for x in sys.argv[1:])
if evidence == root or evidence.is_relative_to(root):
    raise SystemExit('evidence must be outside pilot root')
for candidate in root.glob('cowd-pilot-*'):
    try:
        st = candidate.lstat()
        if candidate.is_symlink() or not candidate.is_dir() or st.st_uid != os.getuid():
            continue
        marker = candidate / '.cowd-pilot-owner.json'
        lock = candidate / '.cowd-pilot.lock'
        if marker.is_symlink() or lock.is_symlink():
            continue
        with lock.open('r+') as held:
            try:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue
            owner = json.loads(marker.read_text())
            pid, start = int(owner['pid']), str(owner['starttime'])
            try:
                current = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(') ', 1)[1].split()[19]
            except FileNotFoundError:
                current = None
            if current == start:
                continue
            evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=candidate.name + '-', suffix='.tar.tmp', dir=evidence)
            os.close(fd)
            try:
                with tarfile.open(tmp, 'w') as archive:
                    archive.add(candidate, arcname=candidate.name, recursive=True)
                with open(tmp, 'rb') as archived:
                    os.fsync(archived.fileno())
                dest = Path(tmp[:-4])
                os.rename(tmp, dest)
                directory = os.open(evidence, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                if candidate.lstat().st_ino != st.st_ino or candidate.lstat().st_dev != st.st_dev:
                    raise RuntimeError('pilot root identity changed')
                shutil.rmtree(candidate)
                print(f'archived {candidate} -> {dest}', flush=True)
            finally:
                if os.path.exists(tmp): os.unlink(tmp)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f'skipped {candidate}: {error}', file=sys.stderr)
PY
