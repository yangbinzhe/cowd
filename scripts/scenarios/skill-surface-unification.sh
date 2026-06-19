#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_ROOT="${CARGO_TARGET_DIR:-$ROOT/target}"
BIN="${COWD_BIN:-$TARGET_ROOT/debug/cowd}"
PORT="${COWD_SKILL_SURFACE_PORT:-18756}"
BASE_URL="http://127.0.0.1:$PORT"
SESSION="cowd-skill-surface-$$"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR="$(mktemp -d "$TMP_ROOT/cowd-skill-surface.XXXXXX")"
WORKDIR="$TMP_DIR/workspace"
CONFIG_HOME="$TMP_DIR/config"
HOME_DIR="$TMP_DIR/home"
LOG="$TMP_DIR/gateway.log"
FAILED=0

cleanup() {
  if [[ "$FAILED" == "1" && "${COWD_SKILL_SURFACE_KEEP_TMP:-}" == "1" ]]; then
    echo "preserving skill surface temp dir: $TMP_DIR" >&2
    return
  fi
  if command -v tmux >/dev/null 2>&1; then
    tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

on_error() {
  local status=$?
  FAILED=1
  echo "skill surface scenario failed with status $status" >&2
  echo "----- temp dir -----" >&2
  echo "$TMP_DIR" >&2
  echo "----- gateway log -----" >&2
  sed -n '1,260p' "$LOG" >&2 || true
  echo "----- captured json -----" >&2
  for json in "$TMP_DIR"/*.json; do
    [[ -f "$json" ]] || continue
    echo "### $(basename "$json")" >&2
    python3 -m json.tool "$json" 2>/dev/null | sed -n '1,160p' >&2 || sed -n '1,160p' "$json" >&2
  done
  echo "-----------------------" >&2
  exit "$status"
}
trap on_error ERR

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for skill surface scenario" >&2
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

cat >"$WORKDIR/.cowd/skills/release/SKILL.md" <<'EOF'
---
name: release
description: Prepare changelog and publish release tags
tags: [git, release]
related_skills: [test]
---
# Release
EOF

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

curl -fsS "$BASE_URL/healthz" | rg -q '"gateway":"gateway-runtime-host"'

catalog_json="$(curl -fsS "$BASE_URL/api/skills/catalog")"
printf '%s' "$catalog_json" | rg -q '"kind":"skills.catalog"'
printf '%s' "$catalog_json" | rg -q '"id":"mfg:supply-risk-analyst"'
printf '%s' "$catalog_json" | rg -q '"id":"local:release"'

mfg_catalog_json="$(curl -fsS "$BASE_URL/api/skills/catalog?scope=mfg")"
printf '%s' "$mfg_catalog_json" | rg -q '"scope":"mfg"'
printf '%s' "$mfg_catalog_json" | rg -vq '"scope":"local"'

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

curl -fsS "$BASE_URL/api/skills/mfg:supply-risk-analyst" | rg -q '"kind":"skills.detail"'
curl -fsS "$BASE_URL/api/skills/local:release" | rg -q '"kind":"skills.detail"'

curl -fsS "$BASE_URL/api/apps/mfg/domain/server-manufacturing/seed" -X POST | rg -q '"metric_dependency_count":5'

fact_json="$(curl -fsS "$BASE_URL/api/matrix/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-fact","session_id":"session-skill-surface","facts":[{"fact_id":"fact-skill-surface-gpu-shortage","snapshot_id":"snapshot-skill-surface","fact_type":"supply.material_shortage","entity_refs":["component:gpu-a"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W24"},"measures":{"short_qty":42},"source_ref":"connector:mock.docs:gpu-shortage","confidence":0.91}]}')"
attention_id="$(printf '%s' "$fact_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["attention"][0]["attention_id"])')"

packet_json="$(curl -fsS "$BASE_URL/api/matrix/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"skill-surface-packet\",\"session_id\":\"session-skill-surface\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"GPU shortage and delivery risk for server build plan\"}")"
printf '%s' "$packet_json" | rg -q '"kind":"matrix.evidence.packet"'
printf '%s' "$packet_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); refs=(data.get("packet") or {}).get("source_refs") or []; assert refs, "expected structured evidence packet source_refs"; assert any((item.get("reference") or item.get("kind")) for item in refs), "expected structured evidence refs to carry reference or kind"'
packet_id="$(printf '%s' "$packet_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["packet"]["packet_id"])')"

incident_json="$(curl -fsS "$BASE_URL/api/apps/mfg/incidents" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-incident","session_id":"session-skill-surface","title":"GPU shortage and delivery risk","evidence_packet_id":"'"$packet_id"'"}')"
printf '%s' "$incident_json" | rg -q '"kind":"mfg.incident"'
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["incident"]["incident_id"])')"

curl -fsS "$BASE_URL/api/apps/mfg/incidents/$incident_id/analyze" -X POST | rg -q '"kind":"mfg.operational_analysis"'

validate_json="$(curl -fsS "$BASE_URL/api/skills/mfg:supply-risk-analyst/actions/validate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-validate","session_id":"session-skill-surface"}')"
printf '%s' "$validate_json" | rg -q '"kind":"skills.action.validate"'
printf '%s' "$validate_json" | rg -q '"status":"pass"'

plan_json="$(curl -fsS "$BASE_URL/api/skills/mfg:supply-risk-analyst/actions/plan" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-plan","session_id":"session-skill-surface","incident_id":"'"$incident_id"'","limit":3}')"
printf '%s' "$plan_json" | rg -q '"kind":"skills.action.plan"'
printf '%s' "$plan_json" | rg -q '"supply-risk-analyst"'

run_json="$(curl -fsS "$BASE_URL/api/skills/mfg:supply-risk-analyst/actions/run" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-run","session_id":"session-skill-surface","incident_id":"'"$incident_id"'"}')"
printf '%s' "$run_json" | rg -q '"kind":"skills.action.run"'
skill_run_id="$(printf '%s' "$run_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["skill_run"]["execution_id"])')"

curl -fsS "$BASE_URL/api/skills/runs" | rg -q '"kind":"skills.runs"'
curl -fsS "$BASE_URL/api/skills/runs/$skill_run_id" | rg -q '"kind":"skills.run"'

local_validate_json="$(curl -fsS "$BASE_URL/api/skills/local:release/actions/validate" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-local-validate","session_id":"session-skill-surface"}')"
printf '%s' "$local_validate_json" | rg -q '"unsupported_for_local_skill"'
