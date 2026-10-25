#!/usr/bin/env bash
set -euo pipefail

: "${COWD_TEST_POSTGRES_URL:?set COWD_TEST_POSTGRES_URL to an isolated disposable PostgreSQL database}"

# Run the deep-real Harness lane through an isolated, short-lived Gateway.
#
# The evaluated route is resolved from the installed Cowd configuration. The
# isolated Gateway receives a minimal copy of that route, but never a literal
# credential. This prevents a scenario from silently testing a different
# provider from the interactive Gateway.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CALLER_HOME="$HOME"
CARGO_HOME_DIR="${CARGO_HOME:-$CALLER_HOME/.cargo}"
RUSTUP_HOME_DIR="${RUSTUP_HOME:-$CALLER_HOME/.rustup}"
SOURCE_CONFIG_HOME="${COWD_EVAL_SOURCE_CONFIG_HOME:-${COWD_CONFIG_HOME:-$CALLER_HOME/.cowd}}"
SOURCE_CONFIG_FILE="$SOURCE_CONFIG_HOME/config.yaml"
SOURCE_MODELS_FILE="$SOURCE_CONFIG_HOME/models.yaml"
MODEL="${COWD_EVAL_MODEL:-}"
BIN="${COWD_BIN:-$ROOT/target/debug/cowd}"
HARNESS_BIN="${COWD_EVAL_HARNESS_BIN:-}"
FROZEN_MODE=0
if [[ -n "$HARNESS_BIN" ]]; then
  : "${COWD_BIN:?frozen evaluation requires both COWD_BIN and COWD_EVAL_HARNESS_BIN}"
  FROZEN_MODE=1
fi
PORT="${COWD_EVAL_GATEWAY_PORT:-18657}"
BASE_URL="http://127.0.0.1:$PORT"
TOKEN="harness-eval-real-qwen-${RANDOM}-${RANDOM}"
umask 077
RUN_ROOT="$(python3 - <<'PY'
import os, stat, tempfile
parent = f'/run/user/{os.getuid()}'
fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
s = os.fstat(fd)
if s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o700:
    raise SystemExit('runtime parent must be owned and actually0700')
root = tempfile.mkdtemp(prefix='cowd-real-pilot-', dir=parent)
s = os.stat(root, follow_symlinks=False)
if s.st_uid != os.getuid() or stat.S_IMODE(s.st_mode) != 0o700:
    raise SystemExit('runtime root is not private')
print(root)
PY
)"
PRIVATE_RUNTIME="$RUN_ROOT/run"
mkdir -m 700 "$PRIVATE_RUNTIME"
read -r RUNTIME_DEVICE RUNTIME_INODE < <(stat -c '%d %i' "$PRIVATE_RUNTIME")
SHUTDOWN_NONCE="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
# Keep evidence under ignored build artifacts by default so a matching run can
# be reused. A caller may choose another durable evidence root explicitly.
EVIDENCE_ROOT="${COWD_AI_HARNESS_REPORT_DIR:-$ROOT/target/acceptance/real-qwen}"
CONFIG_HOME="$RUN_ROOT/config"
# Source-analysis scenarios default to the candidate tree. Artifact-producing
# scenarios may point the Gateway at an explicit disposable workspace without
# changing where the evaluator binary is built or where evidence is stored.
WORKSPACE="${COWD_EVAL_WORKSPACE:-$ROOT}"
GATEWAY_LOG="$RUN_ROOT/gateway.log"
GATEWAY_PID=""
GATEWAY_ORIGINAL_PID=""
PROGRESS_PID=""
DEEP_MONITOR_PID=""
HARNESS_PID=""
GATEWAY_STARTTIME=""
HARNESS_STARTTIME=""
OWNER_PID="$BASHPID"
declare -A CHILD_START=() CHILD_GROUP=()
CLEANUP_DONE=0
GATEWAY_STOP_SECONDS=120
GATEWAY_KILL_SECONDS=5
MONITOR_STOP_SECONDS=2
MONITOR_KILL_SECONDS=2
HARNESS_STOP_SECONDS=10
HARNESS_KILL_SECONDS=2
GATEWAY_WAIT_STATUS="not_started"
INPUT_BIN=""
INPUT_HARNESS_BIN=""
GATEWAY_BINARY_SHA256=""
HARNESS_BINARY_SHA256=""
DEEP_MONITOR_STALL_SECS="${COWD_EVAL_DEEP_MONITOR_STALL_SECS:-600}"
# A single-stream local inference server cannot serve concurrent requests: the
# runtime's default admission ceiling (provider target 64 / max 256, agents 42)
# queues them behind each other, which shows up as multi-minute round latency
# and starved agents. The in-flight ceiling is resolved after the provider is
# known (local providers default to one); set COWD_EVAL_MAX_CONCURRENCY to
# override explicitly.
MAX_CONCURRENCY="${COWD_EVAL_MAX_CONCURRENCY:-}"
DEEP_MONITOR_LAST_MILESTONE=""
DEEP_MONITOR_LAST_CHANGE_TS=0
PROVIDER_ID=""
PROVIDER_BASE_URL=""
PROVIDER_PROTOCOL=""
PROVIDER_CREDENTIAL=""
STAGED_CREDENTIAL_REF=""
STAGED_CREDENTIAL_ENV=""

# One invocation owns exactly one paid scenario.  Live opt-in is sufficient to
# run it: an optional provider-token value is advisory accounting only, never
# a scenario-specific quality floor or a task-execution stop condition.
SCENARIO_ID="${COWD_EVAL_LIVE_SCENARIOS:-}"
MAX_PROVIDER_TOKENS="${COWD_EVAL_MAX_PROVIDER_TOKENS:-unbounded-explicit-opt-in}"
[[ -n "$SCENARIO_ID" && "$SCENARIO_ID" != *,* ]] || {
  echo 'set COWD_EVAL_LIVE_SCENARIOS to exactly one paid scenario id' >&2
  exit 2
}
case "$SCENARIO_ID" in
  live_direct_terminal|live_tool_evidence|live_single_architecture_baseline|live_one_team_execution|live_small_two_team_collaboration|live_explicit_team_requirement|live_team_projection|live_agent_escalation|live_group_theory_ai_research_simulation|live_qwen38_large_scale_collaboration|live_autonomous_collaboration_deepseek|live_minimal_batch_read|live_minimal_compute_reduce) ;;
  *) echo "unsupported paid live scenario: $SCENARIO_ID" >&2; exit 2 ;;
esac

# Fixed evaluator corpus follows the exact registered scenario, never caller paths.
case "$SCENARIO_ID" in
  live_one_team_execution) EVALUATION_CORPUS_ID=live-one-team-v1 ;;
  *) EVALUATION_CORPUS_ID=live-scenarios-v1 ;;
esac

# Functions below stay in this original launch owner; no daemon or second
# Runtime lifecycle is introduced. Local doubles extract these same functions.
curl() { command curl --connect-timeout 2 --max-time 5 "$@"; }

track_child() {
  local pid=$1 group=${2:-0} start
  start="$(python3 - "$pid" "$OWNER_PID" "$group" <<'PY'
import pathlib, sys, time
pid, parent, group = map(int, sys.argv[1:])
for _ in range(100):
    try:
        fields = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        if int(fields[1]) != parent:
            raise SystemExit('not a direct owned child')
        if group and (int(fields[2]) != pid or int(fields[3]) != pid):
            time.sleep(.01)
            continue
        print(fields[19])
        break
    except FileNotFoundError:
        raise SystemExit('owned child exited before identity capture')
else:
    raise SystemExit('monitor did not enter its own process group')
PY
)" || return 1
  CHILD_START[$pid]="$start"
  CHILD_GROUP[$pid]="$group"
}

owned_running() {
  python3 - "$1" "$2" <<'PY'
import pathlib, sys
try:
    fields = pathlib.Path(f'/proc/{int(sys.argv[1])}/stat').read_text().rsplit(') ', 1)[1].split()
    ok = fields[19] == sys.argv[2] and fields[0] not in ('Z', 'X')
except (FileNotFoundError, ProcessLookupError):
    ok = False
raise SystemExit(0 if ok else 1)
PY
}

verify_child_image() {
  local pid=$1 expected=$2
  python3 - "$pid" "${CHILD_START[$pid]}" "$expected" "$RUN_ROOT/child-$pid.json" <<'PY'
import hashlib, json, pathlib, sys, time
pid, start, expected, output = sys.argv[1:]
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    try:
        fields = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
        if fields[19] != start or fields[0] in ('Z', 'X'):
            break
        with open(f'/proc/{pid}/exe', 'rb') as image:
            actual = hashlib.file_digest(image, 'sha256').hexdigest()
        if actual == expected:
            with open(output, 'x') as report:
                json.dump({'pid': int(pid), 'starttime': int(start),
                           'image_sha256': 'sha256:' + actual}, report)
            raise SystemExit(0)
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        pass
    time.sleep(.01)
raise SystemExit('owned child startup image not confirmed')
PY
}

group_has_members() {
  python3 - "$1" <<'PY'
import pathlib, sys
group = int(sys.argv[1])
for entry in pathlib.Path('/proc').iterdir():
    if not entry.name.isdigit():
        continue
    try:
        f = (entry / 'stat').read_text().rsplit(') ', 1)[1].split()
        if int(f[2]) == group and int(f[3]) == group:
            raise SystemExit(0)
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        continue
raise SystemExit(1)
PY
}

signal_owned() {
  local pid=$1 signal=$2 group=${3:-0}
  if [[ "$group" == 1 ]]; then
    python3 - "$pid" "${CHILD_START[$pid]:-missing}" "$signal" <<'PY'
import os, pathlib, signal, sys
pid, start, sig = sys.argv[1:]; pid = int(pid)
try:
    f = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(') ',1)[1].split()
    if f[19] != start or int(f[2]) != pid or int(f[3]) != pid:
        raise SystemExit('monitor group identity changed')
except FileNotFoundError:
    # A surviving Linux process group/session pins its original leader PID
    # number even after the leader is reaped. It cannot be reused meanwhile.
    found = False
    for entry in pathlib.Path('/proc').iterdir():
        if not entry.name.isdigit(): continue
        try: f = (entry/'stat').read_text().rsplit(') ',1)[1].split()
        except (FileNotFoundError, ProcessLookupError, PermissionError): continue
        found |= int(f[2]) == pid and int(f[3]) == pid
    if not found: raise SystemExit(0)
try: os.killpg(pid, getattr(signal, 'SIG'+sig))
except ProcessLookupError: pass
PY
    return
  fi
  if ! owned_running "$pid" "${CHILD_START[$pid]:-missing}"; then
    return 1
  fi
  kill -s "$signal" -- "$pid"
}

stop_owned() {
  local variable=$1 term_seconds=$2 kill_seconds=$3
  local pid=${!variable} deadline forced=0 group reaped=0
  LAST_WAIT_STATUS=not_started
  [[ -n "$pid" ]] || return 0
  [[ -n "${CHILD_START[$pid]:-}" ]] || return 1
  group=${CHILD_GROUP[$pid]}
  if owned_running "$pid" "${CHILD_START[$pid]}" || { [[ "$group" == 1 ]] && group_has_members "$pid"; }; then
    signal_owned "$pid" TERM "$group" || return 1
  fi
  deadline=$((SECONDS + term_seconds))
  while :; do
    if [[ "$reaped" == 0 ]] && ! owned_running "$pid" "${CHILD_START[$pid]}"; then
      # Only this shell's original Child is waited, once, after it has exited.
      if wait "$pid"; then LAST_WAIT_STATUS=0; else LAST_WAIT_STATUS=$?; fi
      printf -v "$variable" '%s' ''
      reaped=1
      [[ "$LAST_WAIT_STATUS" != 127 ]] || return 1
    fi
    if [[ "$reaped" == 1 ]] && { [[ "$group" != 1 ]] || ! group_has_members "$pid"; }; then
      break
    fi
    if ((SECONDS >= deadline)); then
      [[ "$forced" == 0 ]] || return 1
      forced=1
      signal_owned "$pid" KILL "$group" || return 1
      deadline=$((SECONDS + kill_seconds))
    fi
    sleep 0.05
  done
  [[ "$forced" == 0 ]]
}

prepare_binaries() {
  local target=${CARGO_TARGET_DIR:-$ROOT/target}
  [[ "$target" == /* ]] || target="$ROOT/$target"
  if [[ "$FROZEN_MODE" == 0 ]]; then
    if [[ -z "${COWD_BIN:-}" ]]; then
      (cd "$ROOT" && cargo build -p cli --bin cowd) || return 1
      BIN="$target/debug/cowd"
    fi
    (cd "$ROOT" && cargo build -p harness-eval --bin harness-eval) || return 1
    HARNESS_BIN="$target/debug/harness-eval"
  fi
  local bindings
  bindings="$(python3 - "$BIN" "$HARNESS_BIN" "$RUN_ROOT" <<'PY'
import hashlib, json, os, pathlib, stat, sys
root = pathlib.Path(sys.argv[3]); (root / 'bin').mkdir(mode=0o700)
bindings = []
def consume(path, size=None, output=None):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as f:
        meta = os.fstat(f.fileno())
        if not stat.S_ISREG(meta.st_mode) or (size is not None and meta.st_size != size):
            raise ValueError('binary extent/type changed')
        left = meta.st_size; h = hashlib.sha256()
        while left:
            data = f.read(min(left, 1024*1024))
            if not data: raise ValueError('binary truncated')
            h.update(data); left -= len(data)
            if output is not None: output.write(data)
        if f.read(1): raise ValueError('binary grew')
        return h.hexdigest(), meta.st_size
for name, source in zip(('gateway', 'harness'), sys.argv[1:3]):
    path = pathlib.Path(source).resolve(strict=True)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise SystemExit('explicit binary must be an executable regular file')
    expected, size = consume(path)
    frozen = root / 'bin' / name
    with frozen.open('xb') as out:
        actual, _ = consume(path, size, out)
    frozen.chmod(0o500)
    copied, _ = consume(frozen, size)
    if actual != expected or copied != expected:
        raise SystemExit('binary changed while freezing')
    bindings.append({'input': str(path), 'frozen': str(frozen), 'sha256': expected, 'bytes':size})
with (root / 'binary-bindings.json').open('x') as out:
    json.dump(bindings, out)
for binding in bindings:
    print(binding['input']); print(binding['frozen']); print(binding['sha256'])
PY
)" || return 1
  local -a fields
  mapfile -t fields <<<"$bindings"
  [[ "${#fields[@]}" == 6 ]] || return 1
  INPUT_BIN=${fields[0]}; BIN=${fields[1]}; GATEWAY_BINARY_SHA256=${fields[2]}
  INPUT_HARNESS_BIN=${fields[3]}; HARNESS_BIN=${fields[4]}; HARNESS_BINARY_SHA256=${fields[5]}
}

verify_binary_bindings() {
  [[ -n "$INPUT_BIN" && -n "$INPUT_HARNESS_BIN" ]] || return 0
  python3 - "$RUN_ROOT/binary-bindings.json" <<'PY'
import hashlib, json, os, pathlib, stat, sys
for binding in json.loads(pathlib.Path(sys.argv[1]).read_text()):
    for key in ('input', 'frozen'):
        fd = os.open(binding[key], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as f:
            meta = os.fstat(f.fileno()); left = binding['bytes']; h = hashlib.sha256()
            if not stat.S_ISREG(meta.st_mode) or meta.st_size != left:
                raise SystemExit('binary extent/type changed during run')
            while left:
                data = f.read(min(left,1024*1024))
                if not data: raise SystemExit('binary truncated during run')
                h.update(data); left -= len(data)
            if f.read(1) or h.hexdigest() != binding['sha256']:
                raise SystemExit('binary binding changed during run')
PY
}

validate_shutdown_receipt() {
  python3 - "$PRIVATE_RUNTIME" "$RUNTIME_DEVICE" "$RUNTIME_INODE" "$SHUTDOWN_NONCE" \
    "$GATEWAY_ORIGINAL_PID" "$GATEWAY_STARTTIME" "$GATEWAY_BINARY_SHA256" "$RUN_ROOT" <<'PY'
import hashlib, json, os, stat, sys
path, device, inode, nonce, pid, start, image, root = sys.argv[1:]
def require(ok, reason):
    if not ok: raise ValueError(reason)
def number(v):
    require(type(v) is int and 0 <= v <= 18446744073709551615, 'missing/invalid u64 counter')
    return v
def strings(v):
    require(type(v) is list and all(type(s) is str for s in v), 'missing/invalid string array')
    return v
def digest(v):
    return type(v) is str and len(v) == 71 and v.startswith('sha256:') and all(c in '0123456789abcdef' for c in v[7:])
try:
    d = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before = os.fstat(d)
    require((before.st_dev, before.st_ino) == (int(device), int(inode)) and before.st_uid == os.getuid() and stat.S_IMODE(before.st_mode) == 0o700, 'private runtime identity changed')
    f = os.open(f'shutdown-{nonce}.v1.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=d)
    meta = os.fstat(f)
    require(stat.S_ISREG(meta.st_mode) and meta.st_uid == os.getuid() and stat.S_IMODE(meta.st_mode) == 0o600 and meta.st_size <= 2*1024*1024, 'receipt file is not private bounded regular')
    with os.fdopen(f, 'rb') as stream: raw = stream.read(2*1024*1024+1)
    require(len(raw) <= 2*1024*1024, 'receipt grew beyond bound')
    current = os.stat(path, follow_symlinks=False)
    require((current.st_dev, current.st_ino) == (before.st_dev, before.st_ino) and stat.S_ISDIR(current.st_mode) and stat.S_IMODE(current.st_mode) == 0o700 and current.st_uid == os.getuid(), 'runtime directory replaced during read')
    value = json.loads(raw)
    require(type(value['schema_version']) is int and value['schema_version'] == 1, 'unknown schema')
    identity = value['identity']
    require(identity['nonce'] == nonce and type(identity['pid']) is int and identity['pid'] == int(pid) and type(identity['starttime']) is int and identity['starttime'] == int(start) and identity['image_sha256'] == 'sha256:' + image, 'launch identity mismatch')
    require(digest(identity['config_sha256']), 'invalid selected-config digest')
    report = value['report']; strings(report['phases'])
    require(not strings(report['failures']), 'Gateway shutdown failures')
    require(type(report['execution_started']) is bool, 'execution_started missing or invalid')
    execution, active, memory = (report[k] for k in ('execution', 'active_turns', 'memory'))
    require(not report['execution_started'] or execution is not None, 'started execution report missing')
    if execution is not None:
        for key in ('accepted','completed','failed','preserved_graphs','cancelled_graphs','forced_aborts','remaining_keys'): number(execution[key])
        require(execution['forced_aborts'] == 0 and execution['remaining_keys'] == 0 and not strings(execution['errors']), 'execution not drained')
        require(type(execution['owners']) is dict, 'owners missing')
        for owner in execution['owners'].values():
            for key in ('submitted','active','completed','failed','aborted'): number(owner[key])
            require(owner['active'] == 0, 'active owner remains')
    if active is not None:
        number(active['cancelled']); number(active['drained'])
        require(not strings(active['remaining_turn_ids']), 'turns remain')
    if memory is not None:
        number(memory['joined_tasks']); number(memory['forced_aborts'])
        require(type(memory['watcher_joined']) is bool and memory['forced_aborts'] == 0 and not strings(memory['errors']), 'memory not drained')
    tasks = report['tasks']
    for key in ('joined','panicked','forced_aborts','rejected'): number(tasks[key])
    require(tasks['panicked'] == 0 and tasks['forced_aborts'] == 0, 'tasks not drained')
    with open(root + '/shutdown-validation.json','x') as out:
        json.dump({'status':'passed','receipt_sha256':hashlib.sha256(raw).hexdigest(), 'selected_config_digest_only':True, 'identity':identity},out)
except Exception as error:
    print('shutdown receipt rejected: ' + type(error).__name__, file=sys.stderr)
    raise SystemExit(1)
PY
}

cleanup() {
  local status=${1:-$?} secondary=0
  [[ "$CLEANUP_DONE" == 0 ]] || return
  CLEANUP_DONE=1
  trap - EXIT
  trap '' INT TERM
  # Cleanup is explicit under set -e; never discard a secondary failure.
  if stop_owned DEEP_MONITOR_PID "$MONITOR_STOP_SECONDS" "$MONITOR_KILL_SECONDS"; then :; else secondary=1; fi
  if stop_owned PROGRESS_PID "$MONITOR_STOP_SECONDS" "$MONITOR_KILL_SECONDS"; then :; else secondary=1; fi
  if stop_owned HARNESS_PID "$HARNESS_STOP_SECONDS" "$HARNESS_KILL_SECONDS"; then :; else secondary=1; fi
  if [[ -n "$GATEWAY_PID" ]]; then
    if stop_owned GATEWAY_PID "$GATEWAY_STOP_SECONDS" "$GATEWAY_KILL_SECONDS"; then :; else secondary=1; fi
    GATEWAY_WAIT_STATUS=$LAST_WAIT_STATUS
    [[ "$GATEWAY_WAIT_STATUS" == 0 ]] || secondary=1
    if validate_shutdown_receipt; then :; else secondary=1; fi
  fi
  if verify_binary_bindings; then :; else secondary=1; fi
  local -a tracked=()
  local child
  for child in "${!CHILD_START[@]}"; do tracked+=("$child:${CHILD_START[$child]}:${CHILD_GROUP[$child]}"); done
  if python3 - "$RUN_ROOT" "$status" "$secondary" "$GATEWAY_WAIT_STATUS" "${tracked[@]}" <<'PY'
import json, pathlib, sys
root, original, cleanup, gateway_exit = sys.argv[1:5]
owned = {int(p): (start, group == '1') for p,start,group in (v.split(':') for v in sys.argv[5:])}
remaining = []
groups = {pid for pid, (_,group) in owned.items() if group}
for entry in pathlib.Path('/proc').iterdir():
    if not entry.name.isdigit(): continue
    pid = int(entry.name)
    try: f = (entry/'stat').read_text().rsplit(') ',1)[1].split()
    except (FileNotFoundError, ProcessLookupError, PermissionError): continue
    if (pid in owned and f[19] == owned[pid][0]) or (int(f[2]) in groups and int(f[3]) == int(f[2])):
        remaining.append({'pid':pid,'state':f[0],'starttime':f[19],'pgid':int(f[2]),'sid':int(f[3])})
with (pathlib.Path(root)/'cleanup-result.json').open('x') as out:
    json.dump({'business_exit':int(original),'cleanup_failed':bool(int(cleanup)) or bool(remaining), 'gateway_wait_status':gateway_exit,'remaining_owned':remaining},out)
raise SystemExit(1 if remaining else 0)
PY
  then :; else secondary=1; fi
  [[ "$status" != 0 || "$secondary" == 0 ]] || status=1
  # Raw config/logs/receipts remain private; never copy them to a mode-blind mount.
  if [[ "$status" != 0 || "$FROZEN_MODE" == 1 || "${COWD_EVAL_KEEP_GATEWAY_ARTIFACTS:-}" == 1 ]]; then
    printf 'isolated Gateway artifacts: %s\n' "$RUN_ROOT" >&2
  else
    rm -rf -- "$RUN_ROOT"
  fi
  printf 'Harness evidence: %s\n' "$EVIDENCE_ROOT" >&2
  exit "$status"
}
trap 'cleanup "$?"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

launch_gateway() {
  (
    cd "$WORKSPACE"
    exec env "${gateway_env[@]}" "$BIN" gateway run
  ) >"$GATEWAY_LOG" 2>&1 &
  GATEWAY_PID=$!
  GATEWAY_ORIGINAL_PID=$GATEWAY_PID
  track_child "$GATEWAY_PID" || return 1
  GATEWAY_STARTTIME=${CHILD_START[$GATEWAY_PID]}
  verify_child_image "$GATEWAY_PID" "$GATEWAY_BINARY_SHA256"
}

launch_harness() {
  env \
    COWD_CONFIG_HOME="$CONFIG_HOME" \
    XDG_RUNTIME_DIR="$PRIVATE_RUNTIME" \
    CARGO_HOME="$CARGO_HOME_DIR" \
    RUSTUP_HOME="$RUSTUP_HOME_DIR" \
    COWD_API_TOKEN="$TOKEN" \
    COWD_EVAL_GATEWAY_URL="$BASE_URL" \
    COWD_EVAL_REAL_MODEL=1 \
    COWD_EVAL_EVIDENCE_KEY="$EVIDENCE_KEY" \
    COWD_EVAL_BINARY_SHA256="$GATEWAY_BINARY_SHA256" \
    COWD_EVAL_HARNESS_BINARY_SHA256="$HARNESS_BINARY_SHA256" \
    COWD_EVAL_CANDIDATE_SHA="$CANDIDATE_SHA" \
    COWD_EVAL_CANDIDATE_SOURCE_SHA256="$CANDIDATE_SOURCE_SHA256" \
    COWD_EVAL_TARGET_REPO_DIRTY_STATE="$CANDIDATE_STATE" \
    COWD_AI_HARNESS_REPORT_DIR="$EVIDENCE_ROOT" \
    "$HARNESS_BIN" deep-real --provider "$MODEL" --budget full --allow-real-model &
  HARNESS_PID=$!
  track_child "$HARNESS_PID" || return 1
  HARNESS_STARTTIME=${CHILD_START[$HARNESS_PID]}
  verify_child_image "$HARNESS_PID" "$HARNESS_BINARY_SHA256"
}

monitor_entry() {
  # Run in the group's actual leader, without another asynchronous worker or
  # inherited wait trap. The parent waits this leader AND checks the group.
  trap 'trap - TERM INT; exit 143' TERM INT
  "$1"
}

start_monitor() {
  local function=$1 variable=$2 pid
  export -f monitor_entry "$function" curl owned_running
  export ROOT TOKEN BASE_URL GATEWAY_PID GATEWAY_STARTTIME HARNESS_PID HARNESS_STARTTIME
  export EVIDENCE_ROOT COWD_TEST_POSTGRES_URL DEEP_MONITOR_STALL_SECS PRIVATE_RUNTIME
  export DEEP_MONITOR_LAST_MILESTONE DEEP_MONITOR_LAST_CHANGE_TS
  setsid bash -c 'monitor_entry "$1"' -- "$function" &
  pid=$!
  printf -v "$variable" '%s' "$pid"
  if track_child "$pid" 1 2>"$RUN_ROOT/monitor-$pid-identity.stderr"; then
    return 0
  fi
  # A finished short scenario may let its monitor return before /proc capture.
  # Reap only the exact original Child, with no group left and no live PID;
  # never wait an unconfirmed running process or signal a reused identity.
  if [[ ! -e "/proc/$pid/stat" ]] && ! group_has_members "$pid"; then
    local status
    if wait "$pid"; then status=0; else status=$?; fi
    printf -v "$variable" '%s' ''
    return "$status"
  fi
  return 1
}

# A real-provider evaluation can legitimately run several nested Team graphs.
# Keep the operator informed through the same public Mission Control projection
# used by Surfaces, rather than leaving only an opaque provider wait. The
# compact line deliberately contains state/counters only: it never reads
# generated config, headers, credentials, prompts, or model/tool payloads.
monitor_progress() {
  local interval="${COWD_EVAL_PROGRESS_INTERVAL_SECS:-15}"
  while owned_running "$GATEWAY_PID" "$GATEWAY_STARTTIME"; do
    curl -fsS \
      -H "Authorization: Bearer $TOKEN" \
      "$BASE_URL/api/mission/control/summary" 2>/dev/null \
      | jq -c '{
          event: "harness_eval_progress",
          cursor: .summary.cursor,
          revision: .summary.revision,
          teams: [.summary.projection.teams[]? | {
            team_id,
            status,
            agent_count
          }],
          agents: [.summary.projection.agents[]? | {
            agent_id,
            team_id,
            status
          }],
          pending_approvals: (.summary.projection.summary.pending_approval_count // 0),
          recovery_required: (.summary.projection.summary.recovery_required_count // 0),
          readiness: (.summary.projection.control_readiness.actions // [])
        }' \
      || true
    curl -fsS \
      -H "Authorization: Bearer $TOKEN" \
      "$BASE_URL/api/sessions/executions" 2>/dev/null \
      | jq -c '{
          event: "harness_eval_execution_progress",
          sessions: [.items[]? | {
            session_id,
            latest_execution_id,
            active_execution_ids,
            executions: [.executions[]? | {
              execution_id,
              turn_id,
              status,
              graph_id,
              updated_at_ms
            }]
          }]
        }' \
      || true
    sleep "$interval"
  done
}

# Deep semantic monitoring. Surface progress counters count events; a rework
# loop or a redundant-read loop emits events forever while the Objective never
# advances. Two stop paths, both evidence-backed:
#
#  1. ESCALATION (primary). The monitor verifies each report against the
#     previous one. The same finding, repeated COWD_MONITOR_ESCALATE_AFTER
#     times in a row with no convergence milestone in between, is a loop the
#     monitor has confirmed by watching it persist - it stops the run.
#     Run one31 printed that finding ten times across six minutes while the
#     run burned its whole window: the detector saw it, nothing counted it.
#  2. SILENCE (backstop). Semantic milestones frozen for
#     COWD_EVAL_DEEP_MONITOR_STALL_SECS AND no durable event at all in that
#     window, for a run that has gone quiet (Qwen v11: 13 steps in 14 minutes,
#     frozen).
#
# Event liveness deliberately gates only the silence path. A repeated-tool-call
# loop emits an event every few seconds, so requiring silence made the one loop
# shape that most needed stopping unstoppable - the gate protected the run from
# the operator instead of the reverse.
monitor_semantic_health() {
  local interval="${COWD_EVAL_DEEP_MONITOR_INTERVAL_SECS:-60}"
  local monitor="$ROOT/scripts/diagnostics/deep-run-monitor.py"
  [[ -f "$monitor" ]] || return 0
  # The verification state is what makes a repeated finding count. It lives in
  # the evidence directory so a post-mortem can replay the escalation; a
  # read-only evidence root degrades to a temp file rather than losing the
  # escalation entirely.
  local state_file="$EVIDENCE_ROOT/deep-monitor-state.json"
  # The evidence root is reused across runs, so a state file left by the
  # previous run would carry its repeat count into this one: a fresh run whose
  # first report matches the old run's last finding would escalate on its first
  # interval. The verification state belongs to this run; start it empty.
  rm -f "$state_file" 2>/dev/null || true
  mkdir -p "$EVIDENCE_ROOT" 2>/dev/null || state_file="$PRIVATE_RUNTIME/deep-monitor-state.json"
  while owned_running "$HARNESS_PID" "$HARNESS_STARTTIME"; do
    local report state milestones findings now escalate repeat_count state_persisted
    # A monitor that cannot read the database is itself a finding. Silently
    # skipping the turn made a crashing monitor indistinguishable from a quiet
    # one: on a live run the monitor raised on every iteration, this loop
    # treated each empty report as "no data" and continued, and the deep monitor
    # emitted four lines across a 35-minute run that was working the whole time.
    # Emit the failure instead of swallowing it.
    if ! report="$(timeout --foreground --signal=TERM --kill-after=2s 30s python3 "$monitor" "$COWD_TEST_POSTGRES_URL" --investigate --state-file "$state_file" 2>/dev/null)"; then
      printf '{"event":"harness_eval_deep_monitor_error","reason":"deep-run-monitor failed; semantic health is unobserved this interval"}\n' >&2
      sleep "$interval"
      continue
    fi
    [[ -n "$report" ]] || { sleep "$interval"; continue; }
    state="$(printf '%s' "$report" | jq -r '.conclusion.state // ""' 2>/dev/null || true)"
    # Only convergence toward the Objective counts as a semantic milestone.
    # `task_submits` is work-phase churn: it rises while the program spins (one
    # frozen run submitted three tasks and never asked to finish), so counting
    # it hides a real freeze behind activity. Reviews, requests, reopens,
    # reworks and verdicts are the milestones.
    milestones="$(printf '%s' "$report" | jq -r '[.verdicts,.reopens,.objective_reviews,.completion_requests,.reworks] | @csv' 2>/dev/null || true)"
    # How long since the last durable event of ANY kind. Semantic milestones are
    # frozen for the whole work phase by design - before the first completion
    # request there is no Objective milestone to move - so using them alone as
    # the freeze signal stops a run that is still doing real work. One run was
    # killed at 605s while actively publishing and reading, purely because no
    # completion request had been issued yet. Event liveness is the honest
    # freeze signal.
    local last_event_age
    last_event_age="$(printf '%s' "$report" | jq -r '.last_event_age_s // 0' 2>/dev/null || echo 0)"
    findings="$(printf '%s' "$report" | jq -c '.conclusion.findings' 2>/dev/null || echo '[]')"
    # The monitor's own verification: has it told us this same thing before?
    escalate="$(printf '%s' "$report" | jq -r '.verification.escalate // false' 2>/dev/null || echo false)"
    repeat_count="$(printf '%s' "$report" | jq -r '.verification.repeat_count // 0' 2>/dev/null || echo 0)"
    state_persisted="$(printf '%s' "$report" | jq -r '.verification.state_persisted // "true"' 2>/dev/null || echo true)"
    # An escalation that cannot be counted is the failure this mechanism exists
    # to remove, so say so instead of letting the run spin unobserved.
    if [[ "$state_persisted" == "false" ]]; then
      printf '{"event":"harness_eval_deep_monitor_error","reason":"deep-monitor state not persisted; repeated findings cannot escalate"}\n' >&2
    fi
    now="$(date +%s)"
    if [[ -n "$milestones" && "$milestones" != "$DEEP_MONITOR_LAST_MILESTONE" ]]; then
      DEEP_MONITOR_LAST_MILESTONE="$milestones"
      DEEP_MONITOR_LAST_CHANGE_TS="$now"
    fi
    printf '{"event":"harness_eval_deep_monitor","state":"%s","milestones":"%s","stall_for_s":%d,"repeat_count":%d,"findings":%s}\n' \
      "$state" "$milestones" "$(( now - DEEP_MONITOR_LAST_CHANGE_TS ))" "${repeat_count:-0}" "$findings"
    if [[ "$DEEP_MONITOR_LAST_CHANGE_TS" -gt 0 ]]; then
      local stalled_for=$(( now - DEEP_MONITOR_LAST_CHANGE_TS ))
      # Stop path 1: the monitor has now reported the same finding N times in a
      # row with no completion request and no verdict in between. Its counts
      # only grew while it repeated, which is the definition of a confirmed
      # loop. Event liveness is NOT required here: a loop that keeps calling
      # tools keeps events flowing, and that is precisely the run to stop.
      if [[ "$state" == "SPINNING" && "$escalate" == "true" ]]; then
        printf '%s' "$report" >"$EVIDENCE_ROOT/deep-monitor-final-report.json" 2>/dev/null || true
        printf '{"event":"harness_eval_deep_monitor_stop","reason":"confirmed spinning: the same finding repeated %s consecutive reports with no convergence milestone","state":"%s","stall_for_s":%d,"findings":%s}\n' \
          "${repeat_count:-0}" "$state" "$stalled_for" "$findings" >&2
        if owned_running "$HARNESS_PID" "$HARNESS_STARTTIME"; then kill -TERM "$HARNESS_PID"; fi
        return 0
      fi
      # Stop path 2 (backstop): confirmed waste by silence - an active finding
      # AND semantic milestones frozen for the stall window AND no durable
      # event at all in that window. A single recoverable rework never trips
      # it, and a run that is still emitting events is left to path 1.
      if [[ "$stalled_for" -ge "$DEEP_MONITOR_STALL_SECS" && "$state" == "SPINNING" \
            && "${last_event_age:-0}" -ge "$DEEP_MONITOR_STALL_SECS" ]]; then
        printf '%s' "$report" >"$EVIDENCE_ROOT/deep-monitor-final-report.json" 2>/dev/null || true
        printf '{"event":"harness_eval_deep_monitor_stop","reason":"confirmed spinning: findings present, semantic milestones frozen and no durable event in the stall window","state":"%s","stall_for_s":%d,"findings":%s}\n' \
          "$state" "$stalled_for" "$findings" >&2
        if owned_running "$HARNESS_PID" "$HARNESS_STARTTIME"; then kill -TERM "$HARNESS_PID"; fi
        return 0
      fi
    fi
    sleep "$interval"
  done
}

yaml_model_field() {
  local file="$1"
  local model="$2"
  local field="$3"
  awk -v target="$model" -v key="$field" '
    function scalar(line, pos, value) {
      pos = index(line, ":")
      value = substr(line, pos + 1)
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+#.*$/, "", value)
      sub(/^\047/, "", value); sub(/\047$/, "", value)
      sub(/^\"/, "", value); sub(/\"$/, "", value)
      return value
    }
    $0 ~ "^[[:space:]]*" target ":[[:space:]]*$" { matched = 1; next }
    matched && $0 ~ "^[[:space:]]{2}[^[:space:]]" { exit }
    matched && $0 ~ "^[[:space:]]{4}" key ":[[:space:]]*" { print scalar($0); exit }
  ' "$file"
}

yaml_provider_field() {
  local file="$1"
  local provider="$2"
  local field="$3"
  awk -v target="$provider" -v key="$field" '
    function scalar(line, pos, value) {
      pos = index(line, ":")
      value = substr(line, pos + 1)
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+#.*$/, "", value)
      sub(/^\047/, "", value); sub(/\047$/, "", value)
      sub(/^\"/, "", value); sub(/\"$/, "", value)
      return value
    }
    /^providers:[[:space:]]*$/ { providers = 1; next }
    providers && $0 ~ "^[[:space:]]{2}" target ":[[:space:]]*$" { matched = 1; next }
    providers && matched && $0 ~ "^[^[:space:]]" { exit }
    providers && matched && $0 ~ "^[[:space:]]{2}[^[:space:]]" { exit }
    providers && matched && $0 ~ "^[[:space:]]{4}" key ":[[:space:]]*" { print scalar($0); exit }
  ' "$file"
}

resolve_installed_route() {
  [[ -f "$SOURCE_CONFIG_FILE" && -f "$SOURCE_MODELS_FILE" ]] || {
    echo "real-model evaluation requires ${SOURCE_CONFIG_FILE} and ${SOURCE_MODELS_FILE}" >&2
    exit 2
  }
  if [[ -z "$MODEL" ]]; then
    MODEL="$(awk '/^model:[[:space:]]*/ { value = $0; sub(/^model:[[:space:]]*/, "", value); sub(/[[:space:]]+#.*$/, "", value); gsub(/^\"|\"$/, "", value); gsub(/^\047|\047$/, "", value); print value; exit }' "$SOURCE_CONFIG_FILE")"
  fi
  [[ -n "$MODEL" ]] || {
    echo "real-model evaluation cannot resolve the configured default model" >&2
    exit 2
  }
  PROVIDER_ID="$(yaml_model_field "$SOURCE_MODELS_FILE" "$MODEL" provider)"
  [[ -n "$PROVIDER_ID" ]] || {
    echo "model ${MODEL} has no provider mapping in ${SOURCE_MODELS_FILE}" >&2
    exit 2
  }
  model_lower="${MODEL,,}"
  provider_lower="${PROVIDER_ID,,}"
  if [[ "$provider_lower" == *bailian* || "$provider_lower" == *dashscope* ]]; then
    echo "native Bailian generation is forbidden; Bailian is reserved for embedding only" >&2
    exit 2
  fi
  if [[ "$model_lower" == qwen* && "$provider_lower" != "qwen-tokenplan" && "$provider_lower" != "local-ninfer" && "$provider_lower" != "local-strata" ]]; then
    echo "Qwen generation must use qwen-tokenplan or a configured local inference provider, got ${PROVIDER_ID}" >&2
    exit 2
  fi
  if [[ "$model_lower" == deepseek* && "$provider_lower" != "deepseek" && "$provider_lower" != "qwen-tokenplan" ]]; then
    echo "DeepSeek generation must use the configured deepseek provider, got ${PROVIDER_ID}" >&2
    exit 2
  fi

# Resolve the in-flight request ceiling now that the provider is known.
if [[ -z "$MAX_CONCURRENCY" ]]; then
  case "$provider_lower" in
    *local*|*ninfer*|*strata*) MAX_CONCURRENCY=1 ;;
  esac
fi
if [[ -n "$MAX_CONCURRENCY" ]]; then
  printf 'In-flight request ceiling: %s (provider %s)
' "$MAX_CONCURRENCY" "$PROVIDER_ID" >&2
fi
  PROVIDER_BASE_URL="$(yaml_provider_field "$SOURCE_CONFIG_FILE" "$PROVIDER_ID" base_url)"
  PROVIDER_PROTOCOL="$(yaml_provider_field "$SOURCE_CONFIG_FILE" "$PROVIDER_ID" protocol)"
  PROVIDER_CREDENTIAL="$(yaml_provider_field "$SOURCE_CONFIG_FILE" "$PROVIDER_ID" api_key)"
  [[ -n "$PROVIDER_BASE_URL" && -n "$PROVIDER_CREDENTIAL" ]] || {
    echo "provider ${PROVIDER_ID} must declare base_url and api_key in ${SOURCE_CONFIG_FILE}" >&2
    exit 2
  }
  case "$PROVIDER_CREDENTIAL" in
    env:*) STAGED_CREDENTIAL_REF="$PROVIDER_CREDENTIAL" ;;
    file:*)
      echo "provider ${PROVIDER_ID} uses an unsupported file credential reference; use Cowd's env: credential reference" >&2
      exit 2
      ;;
    *)
      # The user configuration is the credential authority. A literal value
      # is passed only in the child process environment and is never written
      # to the generated isolated configuration or evidence artifacts.
      STAGED_CREDENTIAL_ENV="COWD_ISOLATED_EVAL_PROVIDER_API_KEY"
      STAGED_CREDENTIAL_REF="env:${STAGED_CREDENTIAL_ENV}"
      ;;
  esac
}

resolve_installed_route
if [[ "$SCENARIO_ID" == "live_autonomous_collaboration_deepseek" && "${MODEL,,}" != deepseek* ]]; then
  echo "autonomous collaboration acceptance requires an explicit DeepSeek model, got ${MODEL}" >&2
  exit 2
fi
[[ -d "$WORKSPACE" ]] || {
  echo "evaluation workspace does not exist: $WORKSPACE" >&2
  exit 2
}
WORKSPACE="$(cd "$WORKSPACE" && pwd -P)"
command -v curl >/dev/null || { echo 'curl is required.' >&2; exit 2; }
command -v ss >/dev/null || { echo 'ss is required.' >&2; exit 2; }
command -v jq >/dev/null || { echo 'jq is required.' >&2; exit 2; }
if [[ -n "$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all)" ]]; then
  if [[ "${COWD_EVAL_EXPERIMENTAL:-0}" != "1" ]]; then
    echo 'real-model evaluation requires a clean candidate worktree; run deterministic checks on the current changes, then evaluate the immutable candidate once.' >&2
    echo 'set COWD_EVAL_EXPERIMENTAL=1 to measure the working tree as-is (evidence is then marked experimental and is never reused).' >&2
    exit 2
  fi
  # Experiment mode. Faithfully fingerprint what ACTUALLY ran - HEAD plus the
  # dirty working tree and untracked files - instead of refusing, so the
  # evidence stays honest: it cannot be mistaken for a certification of the
  # committed source, and two different dirty states cannot collide on one key.
  CANDIDATE_STATE="experimental-dirty"
  TMP_INDEX="$(mktemp -u)"
  GIT_INDEX_FILE="$TMP_INDEX" git -C "$ROOT" read-tree HEAD >/dev/null 2>&1
  GIT_INDEX_FILE="$TMP_INDEX" git -C "$ROOT" add -A >/dev/null 2>&1
  STATE_TREE="$(GIT_INDEX_FILE="$TMP_INDEX" git -C "$ROOT" write-tree 2>/dev/null || true)"
  rm -f "$TMP_INDEX"
  CANDIDATE_SHA="$(git -C "$ROOT" rev-parse HEAD)"
  if [[ -n "$STATE_TREE" ]]; then
    # The field is named sha256 and the report gate validates it as one, so the value
    # has to be a sha256. A git tree id is 40 hex chars: it does separate two dirty
    # states, but it can never satisfy the gate, so a dirty-tree run always fails
    # `evidence_manifest_complete`. Hashing the tree content keeps the property the
    # comment asks for and makes the field honest about its name.
    CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" cat-file tree "$STATE_TREE" | sha256sum | awk '{print $1}')"
  else
    CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" status --porcelain=v1 --untracked-files=all | sha256sum | awk '{print $1}')"
  fi
else
  CANDIDATE_STATE="clean"
  CANDIDATE_SHA="$(git -C "$ROOT" rev-parse HEAD)"
  CANDIDATE_SOURCE_SHA256="$(git -C "$ROOT" archive --format=tar "$CANDIDATE_SHA" | sha256sum | awk '{print $1}')"
fi
ROUTE_FINGERPRINT="$(printf '%s\n' "$MODEL" "$PROVIDER_ID" "$PROVIDER_BASE_URL" "$PROVIDER_PROTOCOL" | sha256sum | awk '{print $1}')"
SCENARIO_FINGERPRINT="$(printf '%s\n' "$SCENARIO_ID" "$MAX_PROVIDER_TOKENS" "${COWD_EVAL_AUTONOMOUS_AGENT_SCALE:-}" | sha256sum | awk '{print $1}')"
prepare_binaries
verify_binary_bindings
printf 'Gateway binary sha256: %s\nHarness binary sha256: %s\n' "$GATEWAY_BINARY_SHA256" "$HARNESS_BINARY_SHA256" >&2
EVIDENCE_KEY="$(printf '%s\n' "deep-real-v5" "$CANDIDATE_STATE" "$CANDIDATE_SHA" "$CANDIDATE_SOURCE_SHA256" "$ROUTE_FINGERPRINT" "$SCENARIO_FINGERPRINT" "$GATEWAY_BINARY_SHA256" "$HARNESS_BINARY_SHA256" | sha256sum | awk '{print $1}')"
if [[ "${COWD_EVAL_FORCE_RERUN:-0}" != "1" && -d "$EVIDENCE_ROOT/runs" ]]; then
  prior_evidence="$({
    find "$EVIDENCE_ROOT/runs" -type f -name report.json -exec jq -r --arg key "$EVIDENCE_KEY" '
      if .evidence_manifest.execution_key == $key then
        if .status == "passed" and .authorized_real_model == true then
          "passed\t" + (.result_package_dir // input_filename)
        else
          "gap\t" + (.status // "unknown") + "\t" + (.result_package_dir // input_filename)
        end
      else empty end
    ' {} + 2>/dev/null
  } | head -n 1)"
  case "$prior_evidence" in
    passed$'\t'*)
      if [[ "$FROZEN_MODE" == 1 ]]; then
        echo 'existing evidence requires separate audit; frozen pilot not dispatched' >&2
        exit 3
      fi
      printf 'reusing matching real-model evidence: %s\n' "${prior_evidence#*$'\t'}"
      exit 0
      ;;
    gap$'\t'*)
      printf 'matching evidence has an unresolved gap; inspect it before any rerun: %s\n' "${prior_evidence#*$'\t'}" >&2
      printf 'set COWD_EVAL_FORCE_RERUN=1 only after the candidate, route, fixture, or external provider state has changed.\n' >&2
      exit 3
      ;;
  esac
fi
if ss -ltn | rg -q ":${PORT}\\b"; then
  echo "isolated Gateway port $PORT is already in use" >&2
  exit 2
fi
cd "$ROOT"

mkdir -p "$CONFIG_HOME"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "$MODEL"
providers:
  "$PROVIDER_ID":
    base_url: "$PROVIDER_BASE_URL"
    api_key: "$STAGED_CREDENTIAL_REF"
EOF
if [[ -n "$PROVIDER_PROTOCOL" ]]; then
  cat >>"$CONFIG_HOME/config.yaml" <<EOF
    protocol: "$PROVIDER_PROTOCOL"
EOF
fi
cat >>"$CONFIG_HOME/config.yaml" <<EOF
    models:
      - "$MODEL"
runtime:
  # The in-flight MODEL-REQUEST ceiling belongs to provider admission only.
  # Collapsing agent/graph parallelism here would deadlock the supervisor:
  # a root parked on the Agentic barrier holds its owned-task permit for the
  # whole wait (proven in Qwen v10/v11: the delegated agent never got a permit,
  # the barrier never released, the no-progress fuse fired at +300s). Keep the
  # supervisor at its default parallelism and let the provider admission below
  # serialize requests to a single-stream server.
  control:
    agent:
      max_parallel_agents: ${COWD_EVAL_AGENT_PARALLELISM:-42}
  resources:
    provider:
      global:
        minimum: ${MAX_CONCURRENCY:-12}
        target: ${MAX_CONCURRENCY:-64}
        maximum: ${MAX_CONCURRENCY:-256}
        interactiveReserve: $(( ${MAX_CONCURRENCY:-9} > 1 ? ${MAX_CONCURRENCY:-9} - 1 : 0 ))
permissions:
  # Permission must match the scenario's declared business profile. Read-only
  # research scenarios (e.g. group theory) run with COWD_EVAL_DEFAULT_MODE=read-only;
  # autonomous scenarios keep the disposable-workspace default.
  default_mode: "${COWD_EVAL_DEFAULT_MODE:-danger-full-access}"
memory:
  enabled: true
storage:
  backend: postgres
  postgres:
    logicalIdentity: "cowd-harness-eval-test"
    secretRef: "env:COWD_TEST_POSTGRES_URL"
gateway:
  enabled: true
  session_reset: none
  platforms:
    - platformType: api_server
      enabled: true
      host: 127.0.0.1
      port: $PORT
      auth:
        enabled: true
        token: "$TOKEN"
EOF

if env COWD_CONFIG_HOME="$CONFIG_HOME" "$BIN" storage upgrade >/dev/null 2>&1; then
  :
else
  status=$?
  echo 'isolated storage upgrade failed (secretRef: env:COWD_TEST_POSTGRES_URL)' >&2
  exit "$status"
fi

gateway_env=(
  COWD_CONFIG_HOME="$CONFIG_HOME"
  COWD_LOG_STDERR=1
  COWD_EVAL_HARNESS=1
  "COWD_EVAL_CORPUS_ID=$EVALUATION_CORPUS_ID"
  XDG_RUNTIME_DIR="$PRIVATE_RUNTIME"
  COWD_TEST_SHUTDOWN_RECEIPT_NONCE="$SHUTDOWN_NONCE"
  COWD_MODEL_TEMPERATURE=0
)
# Optional objective-level wall-clock budget for required-Team collaboration
# turns. Kept below the scenario safety wait so a non-converging turn ends as an
# honest partial instead of hitting the external timeout.
if [[ -n "${COWD_COLLABORATION_BUDGET_SECS:-}" ]]; then
  gateway_env+=("COWD_COLLABORATION_BUDGET_SECS=$COWD_COLLABORATION_BUDGET_SECS")
fi
if [[ -n "$STAGED_CREDENTIAL_ENV" ]]; then
  gateway_env+=("$STAGED_CREDENTIAL_ENV=$PROVIDER_CREDENTIAL")
fi
launch_gateway
# The literal credential, if present, is now only held by the isolated child.
unset PROVIDER_CREDENTIAL
printf '%s\n' "$GATEWAY_PID" >"$RUN_ROOT/gateway.pid"
for _ in {1..240}; do
  if curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null 2>&1; then
    break
  fi
  owned_running "$GATEWAY_PID" "$GATEWAY_STARTTIME" || {
    echo "isolated Gateway exited; inspect protected diagnostics: $GATEWAY_LOG" >&2
    exit 1
  }
  sleep 0.25
done
curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null

# The disposable credential starts with the product-safe operator profile.
# Normal evaluation requests only mission.observe, while timeout cleanup needs
# runtime.maintenance.manage to cancel the exact lineage owned by this actor.
# Promote through the broker's preview/confirmation protocol so this harness
# never duplicates the manager capability list.
current_entitlement="$(printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  "$BIN" auth profile show)"
profile_epoch="$(jq -r '.credential_epoch' <<<"$current_entitlement")"
profile_revision="$(jq -r '.profile_revision' <<<"$current_entitlement")"
app_profiles="$(jq -r \
  '.app_profiles | to_entries | map("\(.key)=\(.value)") | join(",")' \
  <<<"$current_entitlement")"
profile_preview="$(printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  "$BIN" auth profile preview --core-profile core_manager --apps "$app_profiles")"
profile_confirmation="$(jq -r '.confirmation_digest' <<<"$profile_preview")"
printf '%s\n' "$TOKEN" | env \
  COWD_CONFIG_HOME="$CONFIG_HOME" \
  "$BIN" auth profile set \
    --core-profile core_manager \
    --apps "$app_profiles" \
    --expected-epoch "$profile_epoch" \
    --expected-revision "$profile_revision" \
    --confirm "$profile_confirmation" >/dev/null

# Process liveness precedes projector warm-up and control-plane latency
# stabilization. Starting the evaluator in that interval creates a false
# semantic-health failure even when the same isolated Gateway becomes ready a
# moment later. Gate scenario admission on the exact fail-closed contracts the
# evaluator will record; do not weaken or retry a failed scenario afterward.
SEMANTIC_READY_TIMEOUT_SECS="${COWD_EVAL_SEMANTIC_READY_TIMEOUT_SECS:-180}"
semantic_ready=0
semantic_deadline=$((SECONDS + SEMANTIC_READY_TIMEOUT_SECS))
control_plane_health='{}'
surface_health='{}'
while ((SECONDS < semantic_deadline)); do
  control_plane_health="$(curl -fsS \
    -H "Authorization: Bearer $TOKEN" \
    "$BASE_URL/api/runtime/control-plane" 2>/dev/null || printf '{}')"
  surface_health="$(curl -fsS \
    -H "Authorization: Bearer $TOKEN" \
    "$BASE_URL/api/surfaces/health" 2>/dev/null || printf '{}')"
  if jq -e '
      .readiness.production_ready == true and
      .readiness.required_blocked == 0
    ' >/dev/null <<<"$control_plane_health" \
    && jq -e '
      .host.status == "ready" and
      .host.failed_count == 0 and
      .host.circuit_open_count == 0 and
      .host.task_ownership.overloaded == false
    ' >/dev/null <<<"$surface_health"; then
    semantic_ready=1
    break
  fi
  owned_running "$GATEWAY_PID" "$GATEWAY_STARTTIME" || {
    echo "isolated Gateway exited; inspect protected diagnostics: $GATEWAY_LOG" >&2
    exit 1
  }
  sleep 0.25
done
if [[ "$semantic_ready" -ne 1 ]]; then
  jq -cn \
    --argjson control "$control_plane_health" \
    --argjson surface "$surface_health" \
    '{
      error: "isolated Gateway did not reach semantic readiness",
      control_plane: ($control.readiness // {}),
      surface_host: ($surface.host // {})
    }' >&2
  exit 1
fi
start_monitor monitor_progress PROGRESS_PID

cd "$ROOT"
# A deep real-provider run contains serial scenarios, each with its own
# Runtime-aware progress and inactivity bounds. A wall-clock cap must be
# handled inside the evaluator so timeout cleanup can commit a durable Session
# cancellation receipt. Never wrap the evaluator in GNU timeout: killing the
# observer must not silently become a business cancellation or strand a root.
if [[ -n "${COWD_EVAL_TIMEOUT_SECS:-}" && -z "${COWD_EVAL_SCENARIO_TIMEOUT_SECS:-}" ]]; then
  export COWD_EVAL_SCENARIO_TIMEOUT_SECS="$COWD_EVAL_TIMEOUT_SECS"
fi
verify_binary_bindings
launch_harness

# Run the evaluator in the background so the deep semantic monitor can stop a
# run that burns steps without advancing the Objective. `wait` preserves the
# evaluator's exit status for the caller.
start_monitor monitor_semantic_health DEEP_MONITOR_PID
if wait "$HARNESS_PID"; then EVAL_STATUS=0; else EVAL_STATUS=$?; fi
HARNESS_PID=""
exit "$EVAL_STATUS"
