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
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
[[ "$TARGET_ROOT" == /* ]] || TARGET_ROOT="$ROOT/$TARGET_ROOT"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
HARNESS_BIN="${COWD_EVAL_HARNESS_BIN:-$TARGET_ROOT/debug/harness-eval}"
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
# Keep each actual report under build artifacts by default. A caller may
# choose another durable evidence root explicitly; prior reports stay intact.
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
# Optional experiment ceiling. Only the U5 contention experiment below requires
# capacity1; other scenarios use the configured production capacity by default.
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
  live_u8_order_history_original_clause|live_u7_persistent_memory_recall|live_u7_memory_correction_delete_restart|live_u5_shared_session_fairness|live_u5_active_cancel_isolation|live_direct_terminal|live_tool_evidence|live_single_architecture_baseline|live_one_team_execution|live_small_two_team_collaboration|live_explicit_team_requirement|live_team_projection|live_agent_escalation|live_group_theory_ai_research_simulation|live_qwen38_large_scale_collaboration|live_autonomous_collaboration_deepseek|live_minimal_batch_read|live_minimal_compute_reduce) ;;
  *) echo "unsupported paid live scenario: $SCENARIO_ID" >&2; exit 2 ;;
esac

# F and C preregister one real provider slot in this disposable configuration.
# Both sessions must still produce the original Capacity/blocker witness to pass.
if [[ "$SCENARIO_ID" == live_u5_shared_session_fairness || "$SCENARIO_ID" == live_u5_active_cancel_isolation ]]; then
  [[ -z "$MAX_CONCURRENCY" || "$MAX_CONCURRENCY" == 1 ]] || { echo 'U5 F/C requires its preregistered capacity1' >&2; exit 2; }
  MAX_CONCURRENCY=1
fi

# U7 uses the existing non-file corpus: no new observer path grant.
if [[ "$SCENARIO_ID" == live_u7_persistent_memory_recall || "$SCENARIO_ID" == live_u7_memory_correction_delete_restart ]]; then
  [[ -n "${COWD_EVAL_U7_FIXTURE_FILE:-}" && -z "${COWD_EVAL_RESUME_SESSION:-}" ]] || {
    echo 'U7 requires a fresh Session and a private fixture output path' >&2
    exit 2
  }
fi

# Fixed evaluator corpus follows the exact registered scenario, never caller paths.
EVALUATION_CORPUS_ID="$(python3 - "$ROOT/crates/harness-contract/src/evaluation_profiles.json" "$SCENARIO_ID" <<'PYPROFILE'
import json, sys
with open(sys.argv[1], encoding='utf-8') as source:
    profiles = json.load(source)
print(next((profile['corpus_id'] for profile in profiles['profiles']
            if sys.argv[2] in profile['selectors']), profiles['default_corpus_id']))
PYPROFILE
)"

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
  if [[ ( "$SCENARIO_ID" == live_u5_shared_session_fairness || "$SCENARIO_ID" == live_u5_active_cancel_isolation ) && "$status" == 75 && "$secondary" == 0 ]]; then
    if finalize_u5 "$status"; then status=0; else status=1; fi
  fi
  # Raw config/logs/receipts remain private; never copy them to a mode-blind mount.
  if [[ "$status" != 0 || "${COWD_EVAL_KEEP_GATEWAY_ARTIFACTS:-1}" == 1 ]]; then
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

# Exact original host -> its own Harness descriptor. No caller JSON path.
prepare_u5_host() {
  python3 - "$PRIVATE_RUNTIME" "$OWNER_PID" "$GATEWAY_PID" "$GATEWAY_STARTTIME" \
    "$GATEWAY_BINARY_SHA256" "$HARNESS_BINARY_SHA256" "$CANDIDATE_SHA" \
    "$CANDIDATE_SOURCE_SHA256" "$SHUTDOWN_NONCE" "$PORT" "$BASE_URL" "$EVIDENCE_KEY" <<'PYHOST'
import hashlib,json,os,pathlib,stat,sys,time
root,parent,pid,start,image,harness,candidate,source,nonce,port,url,namespace=sys.argv[1:]
deadline=time.monotonic()+5
parent,pid,start,port=map(int,(parent,pid,start,port))
def require(ok,why):
    if not ok: raise SystemExit(why)
def proc(p):
    f=pathlib.Path(f'/proc/{p}/stat').read_text().rsplit(') ',1)[1].split()
    require(f[0] not in ('Z','X'),'owned Child exited')
    return int(f[1]),int(f[19])
def budget(): require(time.monotonic()<deadline,'owned listener deadline')
meta=os.stat(root,follow_symlinks=False)
require(stat.S_ISDIR(meta.st_mode) and meta.st_uid==os.getuid() and stat.S_IMODE(meta.st_mode)==0o700,'private root identity')
require(proc(pid)==(parent,start),'wrong original Child')
actual=hashlib.sha256()
with open(f'/proc/{pid}/exe','rb') as image_file:
    while True:
        budget();chunk=image_file.read(1024*1024)
        if not chunk: break
        actual.update(chunk)
require(actual.hexdigest()==image,'actual Child image changed')
exe=os.stat(f'/proc/{pid}/exe')
# Initial frozen actual-image SHA is already verified by verify_child_image.
child=json.loads(pathlib.Path(root).parent.joinpath(f'child-{pid}.json').read_text())
require(child=={'pid':pid,'starttime':start,'image_sha256':'sha256:'+image},'missing original image verification')
sockets=set()
for n,fd in enumerate(pathlib.Path(f'/proc/{pid}/fd').iterdir()):
    budget();require(n<4096,'too many owned descriptors')
    try: link=os.readlink(fd)
    except FileNotFoundError: continue
    if link.startswith('socket:[') and link.endswith(']'): sockets.add(int(link[8:-1]))
with open(f'/proc/{pid}/net/tcp','rb') as f: raw=f.read(1024*1024+1)
budget();require(len(raw)<=1024*1024,'listener table too large')
matching=[]
for line in raw.decode().splitlines()[1:]:
    f=line.split()
    if f[1]==f'0100007F:{port:04X}' and f[3]=='0A' and int(f[9]) in sockets: matching.append(int(f[9]))
require(len(matching)==1,'listener not uniquely held by original Child')
require(proc(pid)==(parent,start),'original Child changed')
value=dict(nonce=nonce,launcher_pid=parent,launcher_start=proc(parent)[1],pid=pid,starttime=start,
    image_sha256='sha256:'+image,exe_dev=exe.st_dev,exe_ino=exe.st_ino,listener_inode=matching[0],port=port,
    candidate=candidate,source_sha256=source,namespace=namespace+':'+pathlib.Path(root).parent.name,
    base_url=url,harness_sha256=harness,runtime_dev=meta.st_dev,runtime_ino=meta.st_ino)
raw=json.dumps(value).encode();require(len(raw)<=16384,'host handoff too large')
fd=os.open(pathlib.Path(root)/'u5-host.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
with os.fdopen(fd,'wb') as out: out.write(raw);out.flush();os.fsync(out.fileno());os.fchmod(out.fileno(),0o400)
PYHOST
}

# This only appends a final receipt. Original report, exit75 and cleanup stay intact.
finalize_u5() {
  python3 - "$PRIVATE_RUNTIME" "$RUN_ROOT" "$EVIDENCE_ROOT" "$1" "$SCENARIO_ID" <<'PYFINAL'
import hashlib,json,os,pathlib,stat,sys
private,root,evidence,status,scenario=map(str,sys.argv[1:])
def need(ok,reason):
    if not ok: raise ValueError(reason)
def read(path,limit,mode=None):
    f=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(f,'rb') as stream:
        m=os.fstat(stream.fileno())
        need(stat.S_ISREG(m.st_mode) and (mode is None or (m.st_uid==os.getuid() and stat.S_IMODE(m.st_mode)==mode)),'unsafe receipt')
        raw=stream.read(limit+1);need(len(raw)<=limit,'receipt exceeds bound')
    return json.loads(raw),hashlib.sha256(raw).hexdigest()
try:
    need(status=='75','not the reserved pending exit')
    host,host_sha=read(pathlib.Path(private)/'u5-host.json',16384,0o400)
    inner,inner_sha=read(pathlib.Path(private)/'u5-inner-receipt.json',16384,0o600)
    need(scenario in ('live_u5_shared_session_fairness','live_u5_active_cancel_isolation'),'not exact U5 scenario')
    cancellation=scenario=='live_u5_active_cancel_isolation'
    need(type(inner['schema_version']) is int and inner['schema_version']==1 and inner['disposition']=='pending_outer_cleanup' and inner['scenario']==scenario,'not exact U5 pending')
    owner=inner['owner']
    need(all(owner[k]==host[k] for k in owner) and set(owner)=={'nonce','pid','starttime','image_sha256','candidate','namespace','source_sha256','harness_sha256'},'wrong host binding')
    report_path=pathlib.Path(inner['report_path'])
    need(report_path.resolve().is_relative_to(pathlib.Path(evidence).resolve()),'report outside own evidence root')
    report,report_sha=read(report_path,8*1024*1024)
    need(report_sha==inner['report_sha256'] and report['kind']=='harness_eval.u5_internal' and report['status']=='pending_outer_cleanup' and report['run_id']==inner['run_id'] and report['u5_host']==owner,'wrong original report')
    need(report['live_gateway_scenarios']['selected_scenario_ids']==[scenario],'wrong original selected scenario')
    gate=report['report_gate']
    names=({'u5_exact_focused_selection','u5_cancelled_leaf_authorized_physical_and_other_leaf_business_permissions_and_ACK','u5_original_physical_cancellation_and_settlement','u5_original_usage_accounted','u5_host_binding_present'} if cancellation else
        {'u5_exact_focused_selection','u5_two_original_leaf_business_permissions_and_ACK','u5_original_capacity_witness','u5_original_usage_complete','u5_host_binding_present'})
    need(gate['status']=='pending_outer_cleanup' and gate['failed']==0 and len(gate['items'])==5 and {i['name'] for i in gate['items']}==names and all(i['required'] is True and i['status']=='passed' for i in gate['items']),'internal hardgate failed')
    need(inner['usage']==report['u5_usage'],'usage receipt mismatch')
    usage=inner['usage']
    need(usage['source']=='original_provider_call_id_deduplicated' and type(usage['physical_calls']) is int and usage['physical_calls']>0,'unknown physical usage source')
    dimensions=['input_tokens','output_tokens','cache_creation_input_tokens','cache_read_input_tokens','total_tokens']
    if cancellation and usage.get('usage_status')=='unknown':
        unknown=usage.get('unknown_call_ids')
        need(type(unknown) is list and 0<len(unknown)<=usage['physical_calls'] and len(set(unknown))==len(unknown) and all(type(k) is str and k for k in unknown),'unknown usage needs exact original call identities')
        need(all(k in usage and (usage[k] is None or (type(usage[k]) is int and usage[k]>=0)) for k in dimensions) and usage['total_tokens'] is None,'unknown usage was replaced by zero/total')
    else:
        need(not cancellation or (usage.get('usage_status')=='known' and usage.get('unknown_call_ids')==[]),'missing C usage disposition')
        need(all(type(usage.get(k)) is int and usage[k]>=0 for k in dimensions) and sum(usage[k] for k in dimensions[:-1])==usage['total_tokens'],'incomplete typed usage receipt')
    cleanup,cleanup_sha=read(pathlib.Path(root)/'cleanup-result.json',65536)
    need(cleanup['business_exit']==75 and cleanup['cleanup_failed'] is False and cleanup['gateway_wait_status']=='0' and cleanup['remaining_owned']==[],'outer resource return failed')
    v1,v1_sha=read(pathlib.Path(root)/'shutdown-validation.json',65536)
    need(v1['status']=='passed' and all(v1['identity'][k]==host[k] for k in ['nonce','pid','starttime','image_sha256']),'wrong V1 identity')
    bindings,_=read(pathlib.Path(root)/'binary-bindings.json',65536)
    need(len(bindings)==2 and 'sha256:'+bindings[0]['sha256']==host['image_sha256'] and bindings[1]['sha256']==host['harness_sha256'],'wrong original dual image binding')
    result=dict(schema_version=1,status='passed',scope='focused_U5_C_local_only' if cancellation else 'focused_U5_F_only',original_harness_exit=75,
        run_id=inner['run_id'],owner=owner,inner_sha256=inner_sha,report_sha256=report_sha,
        shutdown_validation_sha256=v1_sha,shutdown_receipt_sha256=v1['receipt_sha256'],cleanup_sha256=cleanup_sha,
        host_sha256=host_sha,usage=inner['usage'])
    if cancellation:
        result.update(remote_server_inference_stop='unknown',remote_billing_stop='unknown')
    # verify_binary_bindings has just independently rehashed actual immutable images.
    path=pathlib.Path(root)/'u5-final-acceptance.v1.json'
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as out: json.dump(result,out);out.flush();os.fsync(out.fileno())
except Exception as error:
    print('U5 final receipt rejected: '+str(error),file=sys.stderr);raise SystemExit(1)
PYFINAL
}

launch_harness() {
  local -a u5_env=()
  if [[ "$SCENARIO_ID" == live_u5_shared_session_fairness || "$SCENARIO_ID" == live_u5_active_cancel_isolation ]]; then
    [[ ! -e "/proc/$BASHPID/fd/9" ]] || { echo 'U5 reserved FD already occupied' >&2; return 1; }
    prepare_u5_host || return 1
    exec 9<"$PRIVATE_RUNTIME/u5-host.json"
    u5_env=(COWD_EVAL_U5_NONCE="$SHUTDOWN_NONCE")
  fi
  env "${u5_env[@]}" \
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
  if [[ "$SCENARIO_ID" == live_u5_shared_session_fairness || "$SCENARIO_ID" == live_u5_active_cancel_isolation ]]; then exec 9<&-; fi
  track_child "$HARNESS_PID" || return 1
  HARNESS_STARTTIME=${CHILD_START[$HARNESS_PID]}
  verify_child_image "$HARNESS_PID" "$HARNESS_BINARY_SHA256"
}

# One concrete U7 boundary, still owned and reaped by this original parent.
# It never accepts a PID, command, path or credential from the model/HTTP.
u7_restart_gateway() {
  local request="$RUN_ROOT/u7-restart/request.json" old_pid=$GATEWAY_PID old_start=$GATEWAY_STARTTIME config_sha new_sha index
  [[ "$SCENARIO_ID" == live_u7_memory_correction_delete_restart && ! -e "$RUN_ROOT/u7-restart/returned.json" ]] || return 1
  python3 - "$request" "$CONFIG_HOME/config.yaml" <<'PYU7REQUEST' || return 1
import hashlib,json,os,stat,sys,uuid
p=sys.argv[1];m=os.stat(p,follow_symlinks=False)
assert stat.S_ISREG(m.st_mode) and m.st_uid==os.getuid() and stat.S_IMODE(m.st_mode)==0o600 and m.st_size<16384
x=json.load(open(p));assert x['kind']=='u7_deleted_memory_restart' and x['phase_a_business_passed'] is True and x['active_readback_absent'] is True
uuid.UUID(x['memory_id'])
assert x['config_sha256']==hashlib.sha256(open(sys.argv[2],'rb').read()).hexdigest()
assert all(isinstance(x[k],str) and x[k] for k in ['phase_a_session','phase_a_turn','phase_a_execution'])
PYU7REQUEST
  config_sha="$(sha256sum "$CONFIG_HOME/config.yaml" | awk '{print $1}')"
  stop_owned DEEP_MONITOR_PID "$MONITOR_STOP_SECONDS" "$MONITOR_KILL_SECONDS" || return 1
  stop_owned PROGRESS_PID "$MONITOR_STOP_SECONDS" "$MONITOR_KILL_SECONDS" || return 1
  stop_owned GATEWAY_PID "$GATEWAY_STOP_SECONDS" "$GATEWAY_KILL_SECONDS" || return 1
  [[ "$LAST_WAIT_STATUS" == 0 ]] || return 1
  validate_shutdown_receipt || return 1
  mv "$RUN_ROOT/shutdown-validation.json" "$RUN_ROOT/u7-first-shutdown-validation.json" || return 1
  cp "$GATEWAY_LOG" "$RUN_ROOT/u7-first-gateway.log" || return 1
  PRIVATE_RUNTIME="$RUN_ROOT/u7-second-run"
  mkdir -m 700 "$PRIVATE_RUNTIME" || return 1
  read -r RUNTIME_DEVICE RUNTIME_INODE < <(stat -c '%d %i' "$PRIVATE_RUNTIME")
  SHUTDOWN_NONCE="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  for index in "${!gateway_env[@]}"; do
    case "${gateway_env[$index]}" in
      XDG_RUNTIME_DIR=*) gateway_env[$index]="XDG_RUNTIME_DIR=$PRIVATE_RUNTIME" ;;
      COWD_TEST_SHUTDOWN_RECEIPT_NONCE=*) gateway_env[$index]="COWD_TEST_SHUTDOWN_RECEIPT_NONCE=$SHUTDOWN_NONCE" ;;
    esac
  done
  launch_gateway || return 1
  [[ "$GATEWAY_PID" != "$old_pid" ]] || return 1
  local ready=0 attempt
  for attempt in {1..240}; do
    if curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/healthz" >/dev/null 2>&1; then ready=1; break; fi
    owned_running "$GATEWAY_PID" "$GATEWAY_STARTTIME" || return 1
    sleep 0.25
  done
  [[ "$ready" == 1 ]] || return 1
  new_sha="$(sha256sum "$CONFIG_HOME/config.yaml" | awk '{print $1}')"
  [[ "$new_sha" == "$config_sha" ]] || return 1
  python3 - "$RUN_ROOT/u7-restart" "$old_pid" "$old_start" "$GATEWAY_PID" "$GATEWAY_STARTTIME" "$config_sha" "$RUN_ROOT/u7-first-shutdown-validation.json" <<'PYU7RETURN' || return 1
import hashlib,json,os,pathlib,sys
root,old,oldstart,new,newstart,config,validation=sys.argv[1:];root=pathlib.Path(root)
request=(root/'request.json').read_bytes();v=json.loads(request)
x=dict(kind='u7_owned_restart_returned',memory_id=v['memory_id'],request_sha256=hashlib.sha256(request).hexdigest(),old_pid=int(old),old_starttime=int(oldstart),new_pid=int(new),new_starttime=int(newstart),same_config_sha256=config,first_shutdown_validation=json.load(open(validation)),ready=True)
# Atomic publication: the observer never reads a partially written return.
path=root/'returned.tmp';fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as out:json.dump(x,out);out.flush();os.fsync(out.fileno())
os.rename(path,root/'returned.json')
PYU7RETURN
  start_monitor monitor_progress PROGRESS_PID || return 1
  start_monitor monitor_semantic_health DEEP_MONITOR_PID || return 1
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
# Semantic diagnostics are observations only. Repetition, similarity and quiet
# intervals are not independent business failure oracles and cannot stop a run.
# Explicit evaluator deadlines and the owned cleanup path still bound execution.
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
      # Record repeated heuristic findings without changing the execution.
      if [[ "$state" == "SPINNING" && "$escalate" == "true" ]]; then
        printf '%s' "$report" >"$EVIDENCE_ROOT/deep-monitor-observation.json" 2>/dev/null || true
        printf '{"event":"harness_eval_deep_monitor_observation","reason":"heuristic finding repeated %s consecutive reports with no convergence milestone","state":"%s","stall_for_s":%d,"findings":%s}\n' \
          "${repeat_count:-0}" "$state" "$stalled_for" "$findings" >&2
        # Heuristic observation only. The evaluator's explicit run deadline and
        # original cleanup owner decide termination, never repetition similarity.
      fi
      # Record a quiet interval independently; it is not proof of failure.
      if [[ "$stalled_for" -ge "$DEEP_MONITOR_STALL_SECS" && "$state" == "SPINNING" \
            && "${last_event_age:-0}" -ge "$DEEP_MONITOR_STALL_SECS" ]]; then
        printf '%s' "$report" >"$EVIDENCE_ROOT/deep-monitor-observation.json" 2>/dev/null || true
        printf '{"event":"harness_eval_deep_monitor_observation","reason":"heuristic quiet interval with findings and no observed milestone","state":"%s","stall_for_s":%d,"findings":%s}\n' \
          "$state" "$stalled_for" "$findings" >&2
        # Heuristic observation only. The evaluator's explicit run deadline and
        # original cleanup owner decide termination, never repetition similarity.
      fi
    fi
    sleep "$interval"
  done
}

yaml_model_field() {
  local file="$1"
  local model="$2"
  local field="$3"
  local raw="${4:-0}"
  awk -v target="$model" -v key="$field" -v raw="$raw" '
    function scalar(line, pos, value) {
      pos = index(line, ":")
      value = substr(line, pos + 1)
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+#.*$/, "", value)
      if (raw) return value == "" ? "null" : value
      sub(/^\047/, "", value); sub(/\047$/, "", value)
      sub(/^\"/, "", value); sub(/\"$/, "", value)
      return value
    }
    /^  [^[:space:]]/ {
      if (matched) exit
      name = $0
      sub(/^  /, "", name); sub(/:[[:space:]]*$/, "", name)
      sub(/^\047/, "", name); sub(/\047$/, "", name)
      sub(/^\"/, "", name); sub(/\"$/, "", name)
      matched = name == target
      next
    }
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
  # Preserve the declared YAML type. Missing remains unknown; malformed/invalid
  # declarations still reach the original ConfigLoader validation unchanged.
  MODEL_CONTEXT_WINDOW="$(yaml_model_field "$SOURCE_MODELS_FILE" "$MODEL" context_window 1)"
  [[ -n "$PROVIDER_ID" ]] || {
    echo "model ${MODEL} has no provider mapping in ${SOURCE_MODELS_FILE}" >&2
    exit 2
  }
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

write_isolated_config() {
mkdir -p "$CONFIG_HOME"
cat >"$CONFIG_HOME/config.yaml" <<EOF
model: "$MODEL"
EOF
if [[ -n "$MODEL_CONTEXT_WINDOW" ]]; then
  # Keep non-integer declarations invalid. The generic YAML loader narrows
  # floating numbers to integers; a quoted invalid scalar must never gain a window.
  local capacity_value="$MODEL_CONTEXT_WINDOW"
  if [[ ! "$capacity_value" =~ ^(0|[1-9][0-9]*)$ ]]; then
    capacity_value="$(jq -cn --arg value "$capacity_value" '$value')"
  fi
  printf 'model_context_windows:\n  %s: %s\n' \
    "$(jq -cn --arg model "$MODEL" '$model')" "$capacity_value" >>"$CONFIG_HOME/config.yaml"
fi
cat >>"$CONFIG_HOME/config.yaml" <<EOF
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
  background:
    evolution: ${COWD_EVAL_BACKGROUND_EVOLUTION:-false}
    growth: ${COWD_EVAL_BACKGROUND_GROWTH:-false}
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

}

resolve_installed_route
[[ -d "$WORKSPACE" ]] || {
  echo "evaluation workspace does not exist: $WORKSPACE" >&2
  exit 2
}
WORKSPACE="$(cd "$WORKSPACE" && pwd -P)"
command -v curl >/dev/null || { echo 'curl is required.' >&2; exit 2; }
command -v ss >/dev/null || { echo 'ss is required.' >&2; exit 2; }
command -v jq >/dev/null || { echo 'jq is required.' >&2; exit 2; }
# Source annotations are optional caller metadata. Never invoke Git or invent
# a clean/source identity from an executable or from a directory name.
CANDIDATE_STATE="${COWD_EVAL_TARGET_REPO_DIRTY_STATE:-unknown}"
CANDIDATE_SHA="${COWD_EVAL_CANDIDATE_SHA:-unknown}"
CANDIDATE_SOURCE_SHA256="${COWD_EVAL_CANDIDATE_SOURCE_SHA256:-}"
ROUTE_FINGERPRINT="$(printf '%s\n' "$MODEL" "$PROVIDER_ID" "$PROVIDER_BASE_URL" "$PROVIDER_PROTOCOL" | sha256sum | awk '{print $1}')"
SCENARIO_FINGERPRINT="$(printf '%s\n' "$SCENARIO_ID" "$MAX_PROVIDER_TOKENS" "${COWD_EVAL_AUTONOMOUS_AGENT_SCALE:-}" | sha256sum | awk '{print $1}')"
prepare_binaries
verify_binary_bindings
printf 'Gateway binary sha256: %s\nHarness binary sha256: %s\n' "$GATEWAY_BINARY_SHA256" "$HARNESS_BINARY_SHA256" >&2
EVIDENCE_KEY="$(printf '%s\n' "deep-real-v6" "$CANDIDATE_STATE" "$CANDIDATE_SHA" "$CANDIDATE_SOURCE_SHA256" "$ROUTE_FINGERPRINT" "$SCENARIO_FINGERPRINT" "$GATEWAY_BINARY_SHA256" "$HARNESS_BINARY_SHA256" | sha256sum | awk '{print $1}')"
# Historical reports remain evidence. Their presence neither substitutes for
# this execution nor prevents a new run requested by its owner.
if ss -ltn | rg -q ":${PORT}\\b"; then
  echo "isolated Gateway port $PORT is already in use" >&2
  exit 2
fi
cd "$ROOT"

write_isolated_config

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

# Aggregate subsystem readiness is an observation, not a universal scenario
# prerequisite. A scenario must receive and validate its own actual API results.
control_status=unknown
surface_status=unknown
if control_plane_health="$(curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/runtime/control-plane" 2>/dev/null)"; then
  control_status=observed
fi
if surface_health="$(curl -fsS -H "Authorization: Bearer $TOKEN" "$BASE_URL/api/surfaces/health" 2>/dev/null)"; then
  surface_status=observed
fi
jq -cn --arg control_status "$control_status" --arg surface_status "$surface_status" \
  --arg control "${control_plane_health:-}" --arg surface "${surface_health:-}" \
  '{control_status:$control_status,surface_status:$surface_status,
    control_plane:($control | try fromjson catch null),
    surface_host:($surface | try fromjson catch null)}' >"$RUN_ROOT/readiness-observation.json"
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
if [[ "$SCENARIO_ID" == live_u7_memory_correction_delete_restart ]]; then
  mkdir -m 700 "$RUN_ROOT/u7-restart"
  export COWD_EVAL_U7_RESTART_DIR="$RUN_ROOT/u7-restart"
fi
launch_harness

# The semantic monitor only observes progress. Explicit evaluator deadlines and
# the original cleanup owner remain responsible for termination; wait preserves
# the actual evaluator status.
start_monitor monitor_semantic_health DEEP_MONITOR_PID
if [[ "$SCENARIO_ID" == live_u7_memory_correction_delete_restart ]]; then
  while owned_running "$HARNESS_PID" "$HARNESS_STARTTIME"; do
    if [[ -f "$RUN_ROOT/u7-restart/request.json" && ! -f "$RUN_ROOT/u7-restart/returned.json" ]]; then
      u7_restart_gateway || { echo 'original U7 retained-root restart failed' >&2; exit 1; }
    fi
    sleep 0.1
  done
fi
if wait "$HARNESS_PID"; then EVAL_STATUS=0; else EVAL_STATUS=$?; fi
HARNESS_PID=""
exit "$EVAL_STATUS"
