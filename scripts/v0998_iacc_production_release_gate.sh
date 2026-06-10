#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
export CARGO_TARGET_DIR="$TARGET_ROOT"

run_step() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_script() {
  local script="$1"
  printf '\n==> %s\n' "$script"
  "$ROOT/$script"
}

run_webui_module_tests() {
  if [[ -x "$ROOT/webui/node_modules/.bin/vitest" ]]; then
    printf '\n==> webui module tests\n'
    (cd "$ROOT/webui" && npm test -- --pool forks --poolOptions.forks.singleFork=true)
    return
  fi

  local external_modules="${IACC_WEBUI_NODE_MODULES:-}"
  if [[ -z "$external_modules" && -d /media/yi/Ext/cowd-webui-deps/node_modules ]]; then
    external_modules="/media/yi/Ext/cowd-webui-deps/node_modules"
  fi

  if [[ -n "$external_modules" && -x "$external_modules/.bin/vitest" ]]; then
    local tmp_root="${TMPDIR:-/tmp}"
    local webui_copy
    webui_copy="$(mktemp -d "$tmp_root/cowd-v0998-webui.XXXXXX")"
    cleanup_webui_copy() {
      rm -rf "$webui_copy" >/dev/null 2>&1 || true
    }
    trap 'trap - RETURN; cleanup_webui_copy' RETURN

    printf '\n==> webui module tests with external node_modules\n'
    cp -a "$ROOT/webui/." "$webui_copy/"
    rm -rf "$webui_copy/node_modules"
    ln -s "$external_modules" "$webui_copy/node_modules"
    (cd "$webui_copy" && PATH="$external_modules/.bin:$PATH" npm test -- --pool forks --poolOptions.forks.singleFork=true)
    return
  fi

  if [[ "${IACC_V0998_REQUIRE_WEBUI_TESTS:-0}" == "1" ]]; then
    echo "webui node_modules are missing; run npm ci in webui or set IACC_WEBUI_NODE_MODULES" >&2
    exit 1
  fi

  printf '\n==> webui module tests skipped; install webui dependencies, set IACC_WEBUI_NODE_MODULES, or set IACC_V0998_REQUIRE_WEBUI_TESTS=1 in CI\n'
}

check_iacc_health_capability() {
  local bin="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
  local port="${COWD_V0998_PORT:-18718}"
  local base_url="http://127.0.0.1:$port"
  local session="cowd-v0998-iacc-$$"
  local tmp_root="${TMPDIR:-/tmp}"
  local tmp_dir
  tmp_dir="$(mktemp -d "$tmp_root/cowd-v0998-iacc.XXXXXX")"
  local workdir="$tmp_dir/workspace"
  local config_home="$tmp_dir/config"
  local home_dir="$tmp_dir/home"
  local log="$tmp_dir/gateway.log"

  cleanup_health_check() {
    if command -v tmux >/dev/null 2>&1; then
      tmux kill-session -t "$session" >/dev/null 2>&1 || true
    fi
    rm -rf "$tmp_dir" >/dev/null 2>&1 || true
  }

  trap 'trap - RETURN; cleanup_health_check' RETURN

  if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is required for v0.9.98 IACC production health check" >&2
    exit 1
  fi
  if [[ ! -x "$bin" ]]; then
    echo "cowd binary not found at $bin; build it first or set COWD_BIN" >&2
    exit 1
  fi
  if ss -ltnp | rg -q ":$port\\b"; then
    echo "port $port is already in use" >&2
    exit 1
  fi

  mkdir -p "$workdir/.cowd" "$config_home" "$home_dir/.cowd"
  ln -s "$ROOT/webui" "$workdir/webui"
  cat >"$config_home/config.yaml" <<EOF
model: "claude-sonnet-4-6"
permissions:
  defaultMode: "dontAsk"
memory:
  enabled: false
gateway:
  enabled: true
  sessionReset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $port
      auth:
        enabled: false
EOF
  cp "$config_home/config.yaml" "$home_dir/.cowd/config.yaml"
  cp "$config_home/config.yaml" "$workdir/.cowd/config.yaml"

  tmux new-session -d -s "$session" \
    "bash -lc \"cd '$workdir' && \
      export COWD_CONFIG_HOME='$config_home' && \
      export HOME='$home_dir' && \
      '$bin' gateway run >'$log' 2>&1\""

  for _ in {1..100}; do
    if curl -fsS "$base_url/health" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done

  curl -fsS "$base_url/healthz" | rg -q '"gateway":"daemon-http-gateway"'
  curl -fsS "$base_url/api/iacc/health" | rg -q '"expected_schema_version":17'
  curl -fsS "$base_url/api/iacc/health" | rg -q '"production_operation_package"'
}

run_step cargo fmt --all --check
run_step cargo test -p cowd-cli iacc --no-default-features -- --test-threads=1
run_step cargo build -p cowd-cli --no-default-features

printf '\n==> IACC production health capability\n'
check_iacc_health_capability

run_script scripts/v0977_iacc_foundation_scenario.sh
run_script scripts/v0978_iacc_metric_attention_scenario.sh
run_script scripts/v0979_iacc_evidence_agent_context_scenario.sh
run_script scripts/v0980_iacc_operational_analysis_scenario.sh
run_script scripts/v0981_iacc_action_feedback_scenario.sh
run_script scripts/v0984_iacc_entity_relation_scenario.sh
run_script scripts/v0985_iacc_server_manufacturing_domain_scenario.sh
run_script scripts/v0986_iacc_metric_dependency_graph_scenario.sh
run_script scripts/v0987_iacc_incremental_compute_job_scenario.sh

if [[ "${IACC_V0998_SKIP_SCALE_BENCHMARK:-0}" != "1" ]]; then
  run_script scripts/v0988_iacc_scale_benchmark.sh
else
  printf '\n==> scripts/v0988_iacc_scale_benchmark.sh skipped by IACC_V0998_SKIP_SCALE_BENCHMARK=1\n'
fi

run_script scripts/v0989_iacc_quality_gate_scenario.sh
run_script scripts/v0990_iacc_cross_plane_action_bridge_scenario.sh
run_script scripts/v0991_iacc_cockpit_projection_scenario.sh
run_script scripts/v0992_iacc_cockpit_report_snapshot_scenario.sh
run_script scripts/v0993_iacc_cockpit_report_delivery_bridge_scenario.sh
run_script scripts/v0994_iacc_report_delivery_payload_templates_scenario.sh
run_script scripts/v0995_iacc_cockpit_report_schedule_runner_scenario.sh
run_script scripts/v0996_iacc_report_delivery_retry_state_scenario.sh
run_script scripts/v0997_iacc_webui_report_visibility_scenario.sh

run_webui_module_tests

printf '\nIACC v0.9.98 production release gate passed.\n'
