#!/usr/bin/env bash
# Ensure the delegated cgroup v2 root used by resident APPs exists with the
# memory/cpu/pids controllers enabled. Run before `cowd gateway start` when
# the configured apps.cgroup_root has not been created yet (e.g. after a host
# reboot). Requires a systemd user session with delegated controllers.
set -euo pipefail

ROOT="${COWD_APP_CGROUP_ROOT:-/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/cowd-gateway.service}"

if [[ ! -d /sys/fs/cgroup ]] || [[ "$(stat -fc %T /sys/fs/cgroup)" != "cgroup2fs" ]]; then
  echo "cgroup v2 is not available on this host" >&2
  exit 1
fi

mkdir -p "$ROOT"

for controller in memory cpu pids; do
  if grep -qw "$controller" "$ROOT/cgroup.controllers" 2>/dev/null; then
    if ! grep -qw "$controller" "$ROOT/cgroup.subtree_control" 2>/dev/null; then
      printf '+%s\n' "$controller" > "$ROOT/cgroup.subtree_control"
    fi
  fi
done

echo "cgroup root ready: $ROOT"
echo "subtree_control: $(tr '\n' ' ' < "$ROOT/cgroup.subtree_control")"
