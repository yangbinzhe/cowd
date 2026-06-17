#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_V09136_PORT:-18756}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-v09136-skills-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-v09136-skills.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"

cleanup() {
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for v0.9.136 skill surface scenario" >&2
  exit 1
fi

if [[ ! -x "$BIN" ]]; then
  echo "cowd binary not found at $BIN; build it first or set COWD_BIN" >&2
  exit 1
fi

if ss -ltnp | rg -q ":$PORT\\b"; then
  echo "port $PORT is already in use" >&2
  exit 1
fi

mkdir -p "$WORKDIR/.cowd/skills/release" "$CONFIG_HOME" "$HOME_DIR/.cowd"
ln -s "$ROOT/webui" "$WORKDIR/webui"

cat >"$WORKDIR/.cowd/skills/release/SKILL.md" <<'EOF'
---
name: release
description: Prepare changelog and publish release tags
tags: [git, release]
related_skills: [test]
---
# Release
EOF

cli_list_json="$(cd "$WORKDIR" && COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" "$BIN" --output-format json skills)"
printf '%s' "$cli_list_json" | rg -q '"kind": "skills"'
printf '%s' "$cli_list_json" | rg -q '"action": "list"'
printf '%s' "$cli_list_json" | rg -q '"name": "release"'

cli_view_text="$(cd "$WORKDIR" && COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" "$BIN" skills view release)"
printf '%s' "$cli_view_text" | rg -q 'Name             release'
printf '%s' "$cli_view_text" | rg -q 'Status           ready'

cli_managed_json="$(cd "$WORKDIR" && COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" "$BIN" --output-format json skills create release-copy)"
printf '%s' "$cli_managed_json" | rg -q '"topic": "managed"'
printf '%s' "$cli_managed_json" | rg -q 'CLI supports only list, view, install, and invocation'

cat >"$CONFIG_HOME/config.yaml" <<EOF
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
      port: $PORT
      auth:
        enabled: false
EOF
cp "$CONFIG_HOME/config.yaml" "$HOME_DIR/.cowd/config.yaml"
cp "$CONFIG_HOME/config.yaml" "$WORKDIR/.cowd/config.yaml"

tmux new-session -d -s "$SESSION" \
  "bash -lc \"cd '$WORKDIR' && \
    export COWD_CONFIG_HOME='$CONFIG_HOME' && \
    export HOME='$HOME_DIR' && \
    '$BIN' gateway run >'$LOG' 2>&1\""

for _ in {1..100}; do
  if curl -fsS "$BASE_URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/healthz" | rg -q '"gateway":"daemon-http-gateway"'

catalog_json="$(curl -fsS "$BASE_URL/api/skills/catalog")"
printf '%s' "$catalog_json" | rg -q '"kind":"skills.catalog"'
printf '%s' "$catalog_json" | rg -q '"id":"iacc:supply-risk-analyst"'
printf '%s' "$catalog_json" | rg -q '"id":"local:release"'

iacc_catalog_json="$(curl -fsS "$BASE_URL/api/skills/catalog?scope=iacc")"
printf '%s' "$iacc_catalog_json" | rg -q '"scope":"iacc"'
printf '%s' "$iacc_catalog_json" | rg -vq '"scope":"local"'

webui_projection_json="$(curl -fsS "$BASE_URL/api/skills/projection?surface=webui&query=prepare%20git%20release%20changelog")"
printf '%s' "$webui_projection_json" | rg -q '"kind":"skills.projection"'
printf '%s' "$webui_projection_json" | rg -q '"surface":"webui"'
printf '%s' "$webui_projection_json" | rg -q '"governance.bulk"'
printf '%s' "$webui_projection_json" | rg -F -q '"tool_fact_model":"tool.execution_plan + tool.invocation.runtime_event"'
printf '%s' "$webui_projection_json" | rg -q '"kind":"skills.activation"'
printf '%s' "$webui_projection_json" | rg -q '"name":"release"'

tui_projection_json="$(curl -fsS "$BASE_URL/api/skills/projection?surface=tui")"
printf '%s' "$tui_projection_json" | rg -q '"surface":"tui"'
printf '%s' "$tui_projection_json" | rg -q '"run.watch"'

cli_projection_json="$(curl -fsS "$BASE_URL/api/skills/projection?surface=cli")"
printf '%s' "$cli_projection_json" | rg -q '"surface":"cli"'
printf '%s' "$cli_projection_json" | rg -q '"skill.import"'
printf '%s' "$cli_projection_json" | rg -vq '"skill.run"'

curl -fsS "$BASE_URL/api/skills/iacc:supply-risk-analyst" | rg -q '"kind":"skills.detail"'
curl -fsS "$BASE_URL/api/skills/local:release" | rg -q '"kind":"skills.detail"'

curl -fsS "$BASE_URL/api/iacc/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

packet_json="$(curl -fsS "$BASE_URL/api/iacc/evidence/build" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09136-packet","session_id":"session-v09136","problem_statement":"GPU shortage and delivery risk for server build plan"}')"
printf '%s' "$packet_json" | rg -q '"kind":"iacc.evidence.packet"'
packet_id="$(printf '%s' "$packet_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/iacc/incidents" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09136-incident","session_id":"session-v09136","title":"GPU shortage and delivery risk","evidence_packet_id":"'"$packet_id"'"}')"
printf '%s' "$incident_json" | rg -q '"kind":"iacc.incident"'
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["incident"]["incident_id"])')"

curl -fsS "$BASE_URL/api/iacc/incidents/$incident_id/analyze" -X POST | rg -q '"kind":"iacc.operational_analysis"'

validate_json="$(curl -fsS "$BASE_URL/api/skills/iacc:supply-risk-analyst/actions/validate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09136-validate","session_id":"session-v09136"}')"
printf '%s' "$validate_json" | rg -q '"kind":"skills.action.validate"'
printf '%s' "$validate_json" | rg -q '"status":"pass"'

plan_json="$(curl -fsS "$BASE_URL/api/skills/iacc:supply-risk-analyst/actions/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09136-plan","session_id":"session-v09136","incident_id":"'"$incident_id"'","limit":3}')"
printf '%s' "$plan_json" | rg -q '"kind":"skills.action.plan"'
printf '%s' "$plan_json" | rg -q '"supply-risk-analyst"'

run_json="$(curl -fsS "$BASE_URL/api/skills/iacc:supply-risk-analyst/actions/run" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09136-run","session_id":"session-v09136","incident_id":"'"$incident_id"'"}')"
printf '%s' "$run_json" | rg -q '"kind":"skills.action.run"'
skill_run_id="$(printf '%s' "$run_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["skill_run"]["execution_id"])')"

curl -fsS "$BASE_URL/api/skills/runs" | rg -q '"kind":"skills.runs"'
curl -fsS "$BASE_URL/api/skills/runs/$skill_run_id" | rg -q '"kind":"skills.run"'

local_validate_json="$(curl -fsS "$BASE_URL/api/skills/local:release/actions/validate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"v09136-local-validate","session_id":"session-v09136"}')"
printf '%s' "$local_validate_json" | rg -q '"unsupported_for_local_skill"'
