#!/usr/bin/env bash
set -euo pipefail
umask 077

mock_record() {
  printf '%s\n' "$1" >>"$MOCK_EVENTS"
}

mock_forbidden() {
  mock_record forbidden
  return 97
}

psql() {
  [[ "$#" == 5 && "$1" == "$COWD_PG_BASE_URL" && "$2" == -v && "$4" == -c ]] || {
    mock_record invalid-psql-arguments
    return 96
  }
  printf '%s\n' "$COWD_PG_BASE_URL" "$MOCK_QUERY_SECRET"
  printf '%s\n' "$COWD_PG_BASE_URL" "$MOCK_PASSWORD" >&2
  case "$5" in
    'CREATE DATABASE "'*'"')
      [[ "$3" == ON_ERROR_STOP=1 ]] || return 96
      mock_record create
      return "$MOCK_CREATE_STATUS"
      ;;
    'DROP DATABASE IF EXISTS "'*'" WITH (FORCE)')
      [[ "$3" == ON_ERROR_STOP=0 ]] || return 96
      mock_record drop
      return "$MOCK_DROP_STATUS"
      ;;
    *) mock_forbidden ;;
  esac
}

# The original entry now copies actual inputs without a Git subprocess.
git() { mock_forbidden; }
tar() {
  if [[ "$1" == --exclude=* ]]; then
    mock_record add
    return "$MOCK_WORKTREE_STATUS"
  fi
  [[ "$1" == -C && "$2" == "$MOCK_CASE_ROOT/workspaces/"* && "$3" == -xf && "$4" == - ]] || return 96
  cat >/dev/null
}
rm() {
  [[ "$1" == -rf && "$2" == -- && "$3" == "$MOCK_CASE_ROOT/workspaces/"* ]] || return 96
  mock_record remove
  command rm "$@"
}

bash() {
  [[ "$#" == 1 && "$1" == scripts/scenarios/harness-eval-real-qwen.sh ]] || {
    mock_record invalid-child-command
    return 96
  }
  local expected_url
  if [[ -n "${COWD_EVAL_REUSE_DB_URL:-}" ]]; then
    expected_url="$COWD_EVAL_REUSE_DB_URL"
  else
    expected_url="${COWD_PG_BASE_URL%/*}/${COWD_EVAL_WORKSPACE##*/}"
  fi
  [[ "$COWD_TEST_POSTGRES_URL" == "$expected_url" ]] || {
    mock_record invalid-child-secret
    return 96
  }
  mock_record child
  mock_record secret-env-ok
  printf '%s\n' 'mock evaluator completed'
  return "$MOCK_CHILD_STATUS"
}

cargo() { mock_forbidden; }
curl() { mock_forbidden; }
cowd() { mock_forbidden; }
python3() { mock_forbidden; }
createdb() { mock_forbidden; }
dropdb() { mock_forbidden; }

if [[ "${1:-}" == --worker ]]; then
  source "$MOCK_FIXTURE/scripts/scenarios/run-live-isolated.sh"
  exit
fi

if [[ "${1:-}" == --source-worker ]]; then
  python3() { command python3 "$@"; }
  source "$MOCK_FUNCTIONS"
  resolve_installed_route
  [[ -z "$MAX_CONCURRENCY" ]]
  source "$MOCK_SOURCE_BLOCK"
  [[ "$CANDIDATE_SHA" == unknown && "$CANDIDATE_STATE" == unknown && -z "$CANDIDATE_SOURCE_SHA256" ]]
  [[ -n "$GATEWAY_BINARY_SHA256" && -n "$HARNESS_BINARY_SHA256" && -n "$EVIDENCE_KEY" ]]
  if [[ "${MOCK_RUN_BINARY:-0}" == 1 ]]; then "$BIN"; fi
  exit
fi

if [[ "${1:-}" == --capacity-worker ]]; then
  source "$MOCK_FUNCTIONS"
  if [[ "${MOCK_CAPACITY_ENTRY:-main}" == paired ]]; then
    resolve_installed_provider_route
    source "$MOCK_CONFIG_BLOCK"
  else
    resolve_installed_route
    write_isolated_config
  fi
  exit
fi

if [[ "${1:-}" == --diagnostic-worker ]]; then
  env() {
    printf '%s\n' "$MOCK_PASSWORD"
    printf '%s\n' "$MOCK_QUERY_SECRET" >&2
    return "$MOCK_DIAGNOSTIC_STATUS"
  }
  owned_running() { return 1; }
  source "$MOCK_BLOCK"
  exit
fi

MODE="${1:-source}"
SOURCE_ROOT="${2:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
[[ "$MODE" == source || "$MODE" == mutant ]] || { printf 'invalid mode\n' >&2; exit 2; }
SELF="$(realpath -- "${BASH_SOURCE[0]}")"
SOURCE_ROOT="$(realpath -- "$SOURCE_ROOT")"
[[ -f "$SOURCE_ROOT/scripts/scenarios/run-live-isolated.sh" ]] || exit 2
SANDBOX="$(mktemp -d "${TMPDIR:-/tmp}/cowd-live-secret-test.XXXXXX")"
trap 'command rm -rf -- "$SANDBOX"' EXIT
FIXTURE="$SANDBOX/fixture"
mkdir -p "$FIXTURE/scripts/scenarios"
SOURCE_HASH_BEFORE="$(sha256sum "$SOURCE_ROOT/scripts/scenarios/run-live-isolated.sh" "$SOURCE_ROOT/scripts/scenarios/harness-eval-real-qwen.sh")"
cp -- "$SOURCE_ROOT/scripts/scenarios/run-live-isolated.sh" "$FIXTURE/scripts/scenarios/"
cp -- "$SOURCE_ROOT/scripts/scenarios/harness-eval-real-qwen.sh" "$FIXTURE/scripts/scenarios/"
if [[ "$MODE" == mutant ]]; then
  sed '/echo "RETAINED eval database: /a\    echo "$DB_URL" >\&2' \
    "$FIXTURE/scripts/scenarios/run-live-isolated.sh" >"$SANDBOX/mutant.sh"
  mv -- "$SANDBOX/mutant.sh" "$FIXTURE/scripts/scenarios/run-live-isolated.sh"
fi
/bin/bash -n "$FIXTURE/scripts/scenarios/run-live-isolated.sh"
/bin/bash -n "$FIXTURE/scripts/scenarios/harness-eval-real-qwen.sh"
ASSERTIONS=0
FAILURES=0
CASES=0

assert_equal() {
  ASSERTIONS=$((ASSERTIONS + 1))
  if [[ "$2" != "$3" ]]; then
    FAILURES=$((FAILURES + 1))
    printf 'FAIL %s\n' "$1" >&2
  fi
}

assert_contains() {
  local content
  content="$(<"$2")"
  ASSERTIONS=$((ASSERTIONS + 1))
  if [[ "$content" != *"$3"* ]]; then
    FAILURES=$((FAILURES + 1))
    printf 'FAIL %s\n' "$1" >&2
  fi
}

assert_absent() {
  local content
  content="$(<"$2")"
  ASSERTIONS=$((ASSERTIONS + 1))
  if [[ "$content" == *"$3"* ]]; then
    FAILURES=$((FAILURES + 1))
    printf 'FAIL %s\n' "$1" >&2
  fi
}

event_count() {
  local event total=0
  while IFS= read -r event; do
    if [[ "$event" == "$2" ]]; then total=$((total + 1)); fi
  done <"$1"
  printf '%s' "$total"
}

run_case() {
  local name="$1" scenario="$2" keep="$3" reuse="$4" child_status="$5"
  local create_status="$6" worktree_status="$7" drop_status="$8" expected_status="$9"
  local expected_create="${10}" expected_drop="${11}" expected_child="${12}" retained="${13}"
  local remove_status="${14:-0}" existing_workspace="${15:-0}"
  local scheme="${16:-postgres}"
  local case_root="$SANDBOX/$name" result_status=0 secret stdout_file stderr_file expected_add expected_remove
  local fake_password=V3_MOCK_PASSWORD_NOT_A_REAL_SECRET fake_query=V3_MOCK_QUERY_NOT_A_REAL_SECRET
  local base_url="$scheme"'://'"v3_mock_user:$fake_password@db.invalid:1/maintenance"
  local reuse_url=''
  mkdir -p "$case_root/workspaces"
  : >"$case_root/events"
  stdout_file="$case_root/stdout"
  stderr_file="$case_root/stderr"
  if [[ "$reuse" != none ]]; then
    reuse_url="${base_url%/*}/retained_db"
    if [[ "$reuse" == query ]]; then reuse_url+="?password=$fake_query&application_name=sidecar"; fi
  fi
  if [[ "$existing_workspace" == 1 ]]; then
    mkdir -p "$case_root/workspaces/retained_db"
    printf '%s\n' original-workspace-content >"$case_root/workspaces/retained_db/preserved.txt"
  fi
  if env -i \
    PATH=/usr/bin:/bin HOME="$case_root" TMPDIR="$case_root" \
    MOCK_FIXTURE="$FIXTURE" MOCK_CASE_ROOT="$case_root" MOCK_EVENTS="$case_root/events" \
    MOCK_PASSWORD="$fake_password" MOCK_QUERY_SECRET="$fake_query" \
    MOCK_CREATE_STATUS="$create_status" MOCK_DROP_STATUS="$drop_status" \
    MOCK_WORKTREE_STATUS="$worktree_status" MOCK_REMOVE_STATUS="$remove_status" \
    MOCK_CHILD_STATUS="$child_status" \
    COWD_PG_BASE_URL="$base_url" COWD_EVAL_REUSE_DB_URL="$reuse_url" \
    COWD_EVAL_LIVE_SCENARIOS="$scenario" COWD_EVAL_KEEP_DB="$keep" \
    COWD_EVAL_WORKSPACE_ROOT="$case_root/workspaces" \
    /bin/bash "$SELF" --worker >"$stdout_file" 2>"$stderr_file"; then
    result_status=0
  else
    result_status=$?
  fi
  CASES=$((CASES + 1))
  assert_equal "$name/exit" "$expected_status" "$result_status"
  assert_equal "$name/create" "$expected_create" "$(event_count "$case_root/events" create)"
  assert_equal "$name/drop" "$expected_drop" "$(event_count "$case_root/events" drop)"
  assert_equal "$name/child" "$expected_child" "$(event_count "$case_root/events" child)"
  assert_equal "$name/secret-env" "$expected_child" "$(event_count "$case_root/events" secret-env-ok)"
  for secret in "$fake_password" "$fake_query" v3_mock_user 'postgres://' 'postgresql://' 'password='; do
    assert_absent "$name/stdout-no-secret" "$stdout_file" "$secret"
    assert_absent "$name/stderr-no-secret" "$stderr_file" "$secret"
  done
  for secret in forbidden invalid-psql-arguments invalid-git-arguments invalid-remove-arity invalid-child-command invalid-child-secret; do
    assert_absent "$name/no-unexpected-command" "$case_root/events" "$secret"
  done
  expected_add=1
  if [[ "$create_status" != 0 || "$existing_workspace" == 1 ]]; then expected_add=0; fi
  expected_remove=0
  if [[ "$expected_add" == 1 && ( "$retained" == 0 || "$worktree_status" != 0 ) ]]; then expected_remove=1; fi
  assert_equal "$name/add" "$expected_add" "$(event_count "$case_root/events" add)"
  assert_equal "$name/remove" "$expected_remove" "$(event_count "$case_root/events" remove)"
  local workspace_count=0 workspace
  for workspace in "$case_root/workspaces/"*; do
    if [[ -d "$workspace" ]]; then workspace_count=$((workspace_count + 1)); fi
  done
  local expected_workspaces=0
  if [[ "$retained" == 1 && "$create_status" == 0 && "$worktree_status" == 0 ]]; then expected_workspaces=1; fi
  assert_equal "$name/workspace-count" "$expected_workspaces" "$workspace_count"
  if [[ "$existing_workspace" == 1 ]]; then
    assert_contains "$name/reused-content-preserved" "$case_root/workspaces/retained_db/preserved.txt" original-workspace-content
  fi
  if [[ "$retained" == 1 ]]; then
    assert_contains "$name/retained-message" "$stderr_file" 'RETAINED eval database:'
    if [[ "$reuse" == none ]]; then
      assert_contains "$name/secret-ref" "$stderr_file" 'database secretRef: env:COWD_PG_BASE_URL + retained database identity'
    else
      assert_contains "$name/secret-ref" "$stderr_file" 'database secretRef: env:COWD_EVAL_REUSE_DB_URL'
    fi
    assert_contains "$name/secure-resume" "$stderr_file" 'resolve COWD_EVAL_REUSE_DB_URL from the original secret source'
  elif [[ "$drop_status" == 0 ]]; then
    assert_contains "$name/drop-message" "$stderr_file" 'dropped fresh eval database:'
  else
    assert_contains "$name/drop-failure" "$stderr_file" 'run owner must reconcile cleanup'
    assert_absent "$name/no-false-drop-success" "$stderr_file" 'dropped fresh eval database:'
  fi
  if [[ "$create_status" != 0 ]]; then
    assert_contains "$name/create-failure" "$stderr_file" 'failed to create fresh eval database:'
  fi
  if [[ "$reuse" == query ]]; then
    assert_equal "$name/query-not-in-workspace" yes "$([[ -d "$case_root/workspaces/retained_db" ]] && printf yes || printf no)"
  fi
}

run_case explicit_drop live_tool_evidence 0 none 0 0 0 0 0 1 1 1 0
run_case default_light live_tool_evidence '' none 0 0 0 0 0 1 1 1 0
run_case explicit_keep live_tool_evidence 1 none 0 0 0 0 0 1 0 1 1
run_case keep_child_failure live_tool_evidence 1 none 42 0 0 0 42 1 0 1 1
run_case default_deepseek live_autonomous_collaboration_deepseek '' none 0 0 0 0 0 1 0 1 1
run_case default_qwen live_qwen38_large_scale_collaboration '' none 0 0 0 0 0 1 0 1 1
run_case default_group live_group_theory_ai_research_simulation '' none 0 0 0 0 0 1 0 1 1
run_case override_heavy_drop live_autonomous_collaboration_deepseek 0 none 0 0 0 0 0 1 1 1 0
run_case reuse_success live_tool_evidence 0 plain 0 0 0 0 0 0 0 1 1
run_case reuse_postgresql live_tool_evidence 0 plain 0 0 0 0 0 0 0 1 1 0 0 postgresql
run_case reuse_failure live_tool_evidence 0 plain 42 0 0 0 42 0 0 1 1
run_case reuse_query live_tool_evidence 0 query 0 0 0 0 0 0 0 1 1
run_case reuse_existing live_tool_evidence 0 plain 0 0 0 0 0 0 0 1 1 0 1
run_case reuse_worktree_failure live_tool_evidence 0 plain 0 0 61 0 2 0 0 0 1
run_case drop_child_failure live_tool_evidence 0 none 42 0 0 0 42 1 1 1 0
run_case create_failure_drop live_tool_evidence 0 none 0 53 0 0 53 1 1 0 0
run_case create_failure_keep live_tool_evidence 1 none 0 53 0 0 53 1 0 0 1
run_case worktree_failure_drop live_tool_evidence 0 none 0 0 61 0 2 1 1 0 0
run_case worktree_failure_keep live_tool_evidence 1 none 0 0 61 0 2 1 0 0 1
run_case drop_failure_success live_tool_evidence 0 none 0 0 0 67 0 1 1 1 0
run_case drop_failure_child_failure live_tool_evidence 0 none 42 0 0 67 42 1 1 1 0

run_invalid_reuse_case() {
  local name="$1" reuse_url="$2" result_status=0
  local case_root="$SANDBOX/$name"
  mkdir -p "$case_root/workspaces"
  : >"$case_root/events"
  if env -i PATH=/usr/bin:/bin HOME="$case_root" TMPDIR="$case_root" \
    MOCK_FIXTURE="$FIXTURE" MOCK_CASE_ROOT="$case_root" MOCK_EVENTS="$case_root/events" \
    MOCK_PASSWORD=V3_MOCK_PASSWORD_NOT_A_REAL_SECRET MOCK_QUERY_SECRET=V3_MOCK_QUERY_NOT_A_REAL_SECRET \
    MOCK_CREATE_STATUS=0 MOCK_DROP_STATUS=0 MOCK_WORKTREE_STATUS=0 MOCK_REMOVE_STATUS=0 MOCK_CHILD_STATUS=0 \
    COWD_PG_BASE_URL=postgres://v3_mock_user:V3_MOCK_PASSWORD_NOT_A_REAL_SECRET@db.invalid:1/maintenance \
    COWD_EVAL_REUSE_DB_URL="$reuse_url" COWD_EVAL_LIVE_SCENARIOS=live_tool_evidence \
    COWD_EVAL_KEEP_DB=0 COWD_EVAL_WORKSPACE_ROOT="$case_root/workspaces" \
    /bin/bash "$SELF" --worker >"$case_root/stdout" 2>"$case_root/stderr"; then
    result_status=0
  else
    result_status=$?
  fi
  CASES=$((CASES + 1))
  assert_equal "$name/rejected" 2 "$result_status"
  assert_equal "$name/no-external-calls" '' "$(<"$case_root/events")"
  assert_contains "$name/generic-error" "$case_root/stderr" 'reused eval database requires'
  for secret in "$reuse_url" v3_mock_user V3_MOCK_PASSWORD_NOT_A_REAL_SECRET V3_MOCK_QUERY_NOT_A_REAL_SECRET 'postgres://' 'password='; do
    assert_absent "$name/stdout-no-secret" "$case_root/stdout" "$secret"
    assert_absent "$name/stderr-no-secret" "$case_root/stderr" "$secret"
  done
}

run_invalid_reuse_case reuse_missing_database postgres://v3_mock_user:V3_MOCK_PASSWORD_NOT_A_REAL_SECRET@db.invalid:1
run_invalid_reuse_case reuse_empty_database postgres://v3_mock_user:V3_MOCK_PASSWORD_NOT_A_REAL_SECRET@db.invalid:1/
run_invalid_reuse_case reuse_fragment_database 'postgres://v3_mock_user:V3_MOCK_PASSWORD_NOT_A_REAL_SECRET@db.invalid:1/db#password=V3_MOCK_QUERY_NOT_A_REAL_SECRET'
run_invalid_reuse_case reuse_keyword_dsn 'host=db.invalid user=v3_mock_user password=V3_MOCK_PASSWORD_NOT_A_REAL_SECRET'
run_invalid_reuse_case reuse_workspace_escape postgres://v3_mock_user:V3_MOCK_PASSWORD_NOT_A_REAL_SECRET@db.invalid:1/..

for diagnostic in storage-upgrade storage-upgrade-success gateway-startup; do
  diagnostic_root="$SANDBOX/$diagnostic"
  mkdir -p "$diagnostic_root"
  if [[ "$diagnostic" == storage-upgrade* ]]; then
    awk '
      /^env COWD_CONFIG_HOME=.*storage upgrade/ || /^if env COWD_CONFIG_HOME=.*storage upgrade/ { active = 1 }
      active && /^gateway_env=/ { exit }
      active { print }
    ' "$FIXTURE/scripts/scenarios/harness-eval-real-qwen.sh" >"$diagnostic_root/block.sh"
    expected_diagnostic_status=53
    if [[ "$diagnostic" == storage-upgrade-success ]]; then expected_diagnostic_status=0; fi
    assert_contains "$diagnostic/selected-nonzero" "$diagnostic_root/block.sh" 'storage upgrade'
  else
    desired_block=1
    awk -v desired="$desired_block" '
      /^  owned_running "\$GATEWAY_PID".*\|\| \{/ { matched++; active = 1 }
      active && matched == desired { print }
      active && /^  }/ { active = 0 }
    ' "$FIXTURE/scripts/scenarios/harness-eval-real-qwen.sh" >"$diagnostic_root/block.sh"
    expected_diagnostic_status=1
    assert_contains "$diagnostic/selected-nonzero" "$diagnostic_root/block.sh" 'owned_running'
  fi
  printf '%s\n' V3_MOCK_PASSWORD_NOT_A_REAL_SECRET V3_MOCK_QUERY_NOT_A_REAL_SECRET >"$diagnostic_root/gateway.log"
  diagnostic_status=0
  if env -i PATH=/usr/bin:/bin HOME="$diagnostic_root" TMPDIR="$diagnostic_root" \
    MOCK_BLOCK="$diagnostic_root/block.sh" \
    MOCK_PASSWORD=V3_MOCK_PASSWORD_NOT_A_REAL_SECRET MOCK_QUERY_SECRET=V3_MOCK_QUERY_NOT_A_REAL_SECRET \
    MOCK_DIAGNOSTIC_STATUS="$expected_diagnostic_status" \
    CONFIG_HOME="$diagnostic_root/config" BIN=/DO_NOT_EXECUTE_SIDECAR_MOCK \
    GATEWAY_PID=DO_NOT_KILL_SIDECAR_MOCK GATEWAY_STARTTIME=NOT_AN_ACTUAL_STARTTIME GATEWAY_LOG="$diagnostic_root/gateway.log" \
    /bin/bash "$SELF" --diagnostic-worker >"$diagnostic_root/stdout" 2>"$diagnostic_root/stderr"; then
    diagnostic_status=0
  else
    diagnostic_status=$?
  fi
  CASES=$((CASES + 1))
  assert_equal "$diagnostic/exit" "$expected_diagnostic_status" "$diagnostic_status"
  for secret in V3_MOCK_PASSWORD_NOT_A_REAL_SECRET V3_MOCK_QUERY_NOT_A_REAL_SECRET; do
    assert_absent "$diagnostic/stdout-no-secret" "$diagnostic_root/stdout" "$secret"
    assert_absent "$diagnostic/stderr-no-secret" "$diagnostic_root/stderr" "$secret"
  done
  if [[ "$diagnostic" == storage-upgrade ]]; then
    assert_contains "$diagnostic/generic-failure" "$diagnostic_root/stderr" 'isolated storage upgrade failed (secretRef: env:COWD_TEST_POSTGRES_URL)'
  elif [[ "$diagnostic" == storage-upgrade-success ]]; then
    assert_absent "$diagnostic/no-false-failure" "$diagnostic_root/stderr" 'isolated storage upgrade failed'
  else
    assert_contains "$diagnostic/log-reference" "$diagnostic_root/stderr" 'inspect protected diagnostics:'
    assert_equal "$diagnostic/log-retained" yes "$([[ -f "$diagnostic_root/gateway.log" ]] && printf yes || printf no)"
  fi
done
# Exercise the original source/route/binary block, not a reimplementation.
# Git is poisoned above, and none of these cases launches a Gateway or Provider.
command python3 - "$SOURCE_ROOT/scripts/scenarios/harness-eval-real-qwen.sh" "$SANDBOX" <<'PYSOURCE'
from pathlib import Path
import sys
s = Path(sys.argv[1]).read_text(); root = Path(sys.argv[2])
functions = s[s.index('prepare_binaries() {'):s.index('validate_shutdown_receipt() {')]
functions += s[s.index('yaml_model_field() {'):s.index('\nresolve_installed_route\n')]
(root / 'source-functions.sh').write_text(functions)
(root / 'source-block.sh').write_text(s[s.index('# Source annotations are optional'):s.index('\nif ss -ltn')])
PYSOURCE
for source_case in no_git non_repository dirty existing_evidence missing_binary child_failure; do
  source_root="$SANDBOX/source-$source_case"
  mkdir -p "$source_root/run" "$source_root/config" "$source_root/evidence/runs/prior"
  printf '%s\n' 'prior report must not block or substitute for this execution' >"$source_root/evidence/runs/prior/report.json"
  printf '%s\n' 'local edit' >"$source_root/Cargo.toml"
  if [[ "$source_case" == dirty ]]; then mkdir "$source_root/.git"; fi
  cp /bin/true "$source_root/cowd"
  cp /bin/true "$source_root/harness"
  if [[ "$source_case" == missing_binary ]]; then command rm "$source_root/cowd"; fi
  if [[ "$source_case" == child_failure ]]; then cp /bin/false "$source_root/cowd"; fi
  # Deliberately use formerly forbidden names/provider pairing. The configured
  # route is authoritative; names cannot silently select a different account.
  cat >"$source_root/config/config.yaml" <<'CONFIG'
model: qwen-custom
providers:
  configured-route:
    base_url: http://127.0.0.1:9/v1
    protocol: responses
    api_key: not-a-real-account
CONFIG
  cat >"$source_root/config/models.yaml" <<'MODELS'
models:
  qwen-custom:
    provider: configured-route
MODELS
  source_status=0
  if env -i PATH=/usr/bin:/bin HOME="$source_root" \
    MOCK_EVENTS="$source_root/events" MOCK_FUNCTIONS="$SANDBOX/source-functions.sh" \
    MOCK_SOURCE_BLOCK="$SANDBOX/source-block.sh" MOCK_RUN_BINARY="$([[ "$source_case" == child_failure ]] && printf 1 || printf 0)" \
    ROOT="$source_root" WORKSPACE="$source_root" RUN_ROOT="$source_root/run" \
    SOURCE_CONFIG_FILE="$source_root/config/config.yaml" SOURCE_MODELS_FILE="$source_root/config/models.yaml" \
    MODEL='' MAX_CONCURRENCY='' STAGED_CREDENTIAL_ENV='' PROVIDER_CREDENTIAL='' \
    SCENARIO_ID=live_autonomous_collaboration_deepseek MAX_PROVIDER_TOKENS=unbounded \
    BIN="$source_root/cowd" HARNESS_BIN="$source_root/harness" \
    EVIDENCE_ROOT="$source_root/evidence" \
    /bin/bash "$SELF" --source-worker >"$source_root/stdout" 2>"$source_root/stderr"; then
    source_status=0
  else source_status=$?; fi
  CASES=$((CASES + 1))
  expected_source_status=0
  if [[ "$source_case" == missing_binary || "$source_case" == child_failure ]]; then expected_source_status=1; fi
  assert_equal "$source_case/source-exit" "$expected_source_status" "$source_status"
  assert_equal "$source_case/no-git-or-external" no "$([[ -e "$source_root/events" ]] && printf yes || printf no)"
  assert_contains "$source_case/prior-evidence-preserved" "$source_root/evidence/runs/prior/report.json" 'prior report'
done
# The same original generator must carry only this model's declared window.
for capacity_case in valid missing quoted fraction foreign; do
  capacity_root="$SANDBOX/capacity-$capacity_case"
  mkdir -p "$capacity_root/home" "$capacity_root/source" "$capacity_root/config"
  cp "$SANDBOX/source-no_git/config/config.yaml" "$capacity_root/source/config.yaml"
  {
    printf 'models:\n'
    if [[ "$capacity_case" == foreign ]]; then
      printf '  qwenXcustom:\n    provider: configured-route\n    context_window: 16384\n'
    fi
    printf '  qwen-custom:\n    provider: configured-route\n'
    case "$capacity_case" in
      valid) printf '    context_window: 8192\n' ;;
      quoted) printf '    context_window: "8192"\n' ;;
      fraction) printf '    context_window: 8192.5\n' ;;
    esac
  } >"$capacity_root/source/models.yaml"
  capacity_status=0
  env -i PATH=/usr/bin:/bin HOME="$capacity_root/home" \
    MOCK_EVENTS="$capacity_root/events" MOCK_FUNCTIONS="$SANDBOX/source-functions.sh" \
    SOURCE_CONFIG_FILE="$capacity_root/source/config.yaml" SOURCE_MODELS_FILE="$capacity_root/source/models.yaml" \
    CONFIG_HOME="$capacity_root/config" MODEL=qwen-custom MAX_CONCURRENCY=1 \
    STAGED_CREDENTIAL_ENV='' PORT=19998 TOKEN=offline-capacity-proof \
    /bin/bash "$SELF" --capacity-worker >"$capacity_root/stdout" 2>"$capacity_root/stderr" || capacity_status=$?
  CASES=$((CASES + 1))
  assert_equal "$capacity_case/capacity-generator" 0 "$capacity_status"
  assert_equal "$capacity_case/no-external" no "$([[ -e "$capacity_root/events" ]] && printf yes || printf no)"
  assert_absent "$capacity_case/no-literal-credential" "$capacity_root/config/config.yaml" not-a-real-account
  case "$capacity_case" in
    valid) assert_contains "$capacity_case/declared-window" "$capacity_root/config/config.yaml" '"qwen-custom": 8192' ;;
    missing|foreign) assert_absent "$capacity_case/no-invented-window" "$capacity_root/config/config.yaml" model_context_windows ;;
    quoted) assert_contains "$capacity_case/invalid-type-retained" "$capacity_root/config/config.yaml" '\"8192\"' ;;
    fraction) assert_contains "$capacity_case/no-numeric-truncation" "$capacity_root/config/config.yaml" '"8192.5"' ;;
  esac
done
SOURCE_HASH_AFTER="$(sha256sum "$SOURCE_ROOT/scripts/scenarios/run-live-isolated.sh" "$SOURCE_ROOT/scripts/scenarios/harness-eval-real-qwen.sh")"
assert_equal source-files-unchanged "$SOURCE_HASH_BEFORE" "$SOURCE_HASH_AFTER"
assert_equal nonzero-case-count yes "$([[ "$CASES" -gt 0 ]] && printf yes || printf no)"
assert_equal nonzero-assertion-count yes "$([[ "$ASSERTIONS" -gt 0 ]] && printf yes || printf no)"
printf 'mode=%s cases=%d assertions=%d failures=%d real_db_calls=0 real_provider_calls=0\n' "$MODE" "$CASES" "$ASSERTIONS" "$FAILURES"
[[ "$FAILURES" == 0 ]]
