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
API_TOKEN="skill-surface-$$_credential"

curl() {
  command curl -H "Authorization: Bearer $API_TOKEN" "$@"
}

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

capture_json() {
  local name="$1"
  shift
  curl -fsS "$@" | tee "$TMP_DIR/$name.json"
}

select_manager_profile() {
  local current epoch revision confirmation
  local probe_stdout="$TMP_DIR/profile-manager.probe.json"
  local probe_stderr="$TMP_DIR/profile-manager.probe.stderr"

  current="$(
    printf '%s\n' "$API_TOKEN" | env \
      COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" \
      "$BIN" auth profile show
  )"
  epoch="$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["credential_epoch"])')"
  revision="$(printf '%s' "$current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["profile_revision"])')"

  if printf '%s\n' "$API_TOKEN" | env \
    COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" \
    "$BIN" auth profile set \
      --core-profile core_manager --apps mfg=mfg_manager \
      --expected-epoch "$epoch" --expected-revision "$revision" \
      --confirm invalid >"$probe_stdout" 2>"$probe_stderr"; then
    echo "manager profile confirmation probe unexpectedly succeeded" >&2
    return 1
  fi
  confirmation="$(sed -n 's/.*confirmation=\([^[:space:]]*\).*/\1/p' "$probe_stderr" | head -1)"
  if [[ -z "$confirmation" ]]; then
    echo "manager profile confirmation digest was not emitted" >&2
    return 1
  fi
  printf '%s\n' "$API_TOKEN" | env \
    COWD_CONFIG_HOME="$CONFIG_HOME" HOME="$HOME_DIR" \
    "$BIN" auth profile set \
      --core-profile core_manager --apps mfg=mfg_manager \
      --expected-epoch "$epoch" --expected-revision "$revision" \
      --confirm "$confirmation" >"$TMP_DIR/profile-manager.json"
}

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
providers:
  scenario:
    base_url: "http://127.0.0.1:1"
    api_key: "skill-surface-provider-key"
    protocol: "completions"
    models:
      - "claude-sonnet-4-6"
permissions:
  default_mode: "danger-full-access"
memory:
  enabled: false
gateway:
  enabled: true
  session_reset: "none"
  platforms:
    - platformType: "api_server"
      enabled: true
      host: "127.0.0.1"
      port: $PORT
      auth:
        enabled: true
        token: "$API_TOKEN"
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

capture_json healthz "$BASE_URL/healthz" | rg -q '"gateway":"gateway-runtime-host"'

catalog_json="$(capture_json skills_catalog "$BASE_URL/api/skills/catalog")"
printf '%s' "$catalog_json" | rg -q '"kind":"skills.catalog"'
printf '%s' "$catalog_json" | rg -q '"id":"mfg:supply-risk-analyst"'
printf '%s' "$catalog_json" | rg -q '"id":"local:release"'

mfg_catalog_json="$(capture_json skills_catalog_mfg "$BASE_URL/api/skills/catalog?scope=mfg")"
printf '%s' "$mfg_catalog_json" | rg -q '"scope":"mfg"'
printf '%s' "$mfg_catalog_json" | rg -vq '"scope":"local"'

webui_projection_json="$(capture_json skills_projection_webui "$BASE_URL/api/skills/projection?surface=webui&query=prepare%20git%20release%20changelog")"
printf '%s' "$webui_projection_json" | rg -q '"kind":"skills.projection"'
printf '%s' "$webui_projection_json" | rg -q '"surface":"webui"'
printf '%s' "$webui_projection_json" | rg -q '"governance.bulk"'
printf '%s' "$webui_projection_json" | rg -F -q '"tool_fact_model":"tool.execution_plan + tool.invocation.runtime_event"'
printf '%s' "$webui_projection_json" | rg -q '"kind":"skills.activation"'
printf '%s' "$webui_projection_json" | rg -q '"name":"release"'

tui_projection_json="$(capture_json skills_projection_tui "$BASE_URL/api/skills/projection?surface=tui")"
printf '%s' "$tui_projection_json" | rg -q '"surface":"tui"'
printf '%s' "$tui_projection_json" | rg -q '"skill.maintenance.review"'
printf '%s' "$tui_projection_json" | rg -q '"run.watch"'

cli_projection_json="$(capture_json skills_projection_cli "$BASE_URL/api/skills/projection?surface=cli")"
printf '%s' "$cli_projection_json" | rg -q '"surface":"cli"'
printf '%s' "$cli_projection_json" | rg -q '"skill.import"'
printf '%s' "$cli_projection_json" | rg -vq '"skill.run"'

capture_json skill_detail_mfg "$BASE_URL/api/skills/mfg:supply-risk-analyst" | rg -q '"kind":"skills.detail"'
capture_json skill_detail_local "$BASE_URL/api/skills/local:release" | rg -q '"kind":"skills.detail"'

# Mutating MFG routes are deliberately unavailable to the default viewer.
# Exercise the production entitlement CAS/confirmation path instead of
# weakening the APP capability boundary for a test.
select_manager_profile
python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); assert data["core_profile_id"] == "core_manager", data; assert data["app_profiles"]["mfg"] == "mfg_manager", data' \
  "$TMP_DIR/profile-manager.json"

capture_json mfg_seed "$BASE_URL/api/apps/mfg/domain/server-manufacturing/seed" \
  -X POST \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: skill-surface-seed-$$" \
  -d '{}' | rg -q '"metric_dependency_count":5'

capture_json matrix_entity_component "$BASE_URL/api/matrix/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-component","session_id":"session-skill-surface","entity":{"entity_id":"entity-component-gpu-h100","entity_type":"component","canonical_key":"gpu-h100","display_name":"GPU H100","source_keys":[],"attributes":{"domain":"server_manufacturing"},"confidence":0.98}}' \
  | rg -q '"entity_id":"entity-component-gpu-h100"'
capture_json matrix_entity_order "$BASE_URL/api/matrix/entities/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-order","session_id":"session-skill-surface","entity":{"entity_id":"entity-order-skill-surface","entity_type":"customer_order","canonical_key":"skill-surface-order","display_name":"Skill Surface Order","source_keys":[],"attributes":{"domain":"server_manufacturing"},"confidence":0.97}}' \
  | rg -q '"entity_id":"entity-order-skill-surface"'
capture_json matrix_relation_impact "$BASE_URL/api/matrix/relations/upsert" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-relation","session_id":"session-skill-surface","relation":{"relation_id":"relation-skill-surface-gpu-order","relation_type":"affects","from_entity_id":"entity-component-gpu-h100","to_entity_id":"entity-order-skill-surface","attributes":{"reason":"material_shortage"},"confidence":0.96}}' \
  | rg -q '"relation_id":"relation-skill-surface-gpu-order"'

fact_json="$(capture_json matrix_fact_ingest "$BASE_URL/api/matrix/facts/ingest" \
  -H 'content-type: application/json' \
  -d '{"request_id":"skill-surface-fact","session_id":"session-skill-surface","facts":[{"fact_id":"fact-skill-surface-gpu-shortage","snapshot_id":"snapshot-skill-surface","fact_type":"supply.material_shortage","entity_refs":["entity-component-gpu-h100"],"metric_key":"material_shortage_risk","dimensions":{"week":"2026-W24","entity_id":"entity-component-gpu-h100"},"measures":{"short_qty":42},"source_ref":"connector:local.docs:gpu-shortage","confidence":0.91}]}')"
attention_id="$(printf '%s' "$fact_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["attention"][0]["attention_id"])')"

recompute_json="$(capture_json matrix_metric_recompute "$BASE_URL/api/matrix/metrics/recompute" \
  -X POST \
  -H 'content-type: application/json' \
  -d '{}')"
printf '%s' "$recompute_json" | rg -q '"kind":"matrix.metrics.recompute"'
attention_id="$(printf '%s' "$recompute_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); print(data["result"]["attention"][0]["attention_id"])')"

packet_json="$(capture_json matrix_evidence_packet "$BASE_URL/api/matrix/evidence/build" \
  -H 'content-type: application/json' \
  -d "{\"request_id\":\"skill-surface-packet\",\"session_id\":\"session-skill-surface\",\"attention_id\":\"$attention_id\",\"problem_statement\":\"GPU shortage and delivery risk for server build plan\"}")"
printf '%s' "$packet_json" | rg -q '"kind":"matrix.evidence.packet"'
printf '%s' "$packet_json" | python3 -c 'import json,sys; data=json.load(sys.stdin); refs=(data.get("packet") or {}).get("source_refs") or []; assert refs, "expected structured evidence packet source_refs"; assert any((item.get("reference") or item.get("kind")) for item in refs), "expected structured evidence refs to carry reference or kind"'
printf '%s' "$packet_json" | python3 -c 'import json,sys; packet=json.load(sys.stdin)["packet"]; assert packet["metric_evidence"], "expected recomputed metric evidence"; assert packet["change_evidence"][0]["entity_ref"] == "entity-component-gpu-h100", packet["change_evidence"]'
packet_id="$(printf '%s' "$packet_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["packet"]["packet_id"])')"

incident_json="$(capture_json mfg_incident "$BASE_URL/api/apps/mfg/incidents" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: skill-surface-incident-$$" \
  -d '{"request_id":"skill-surface-incident","session_id":"session-skill-surface","title":"GPU shortage and delivery risk","evidence_packet_id":"'"$packet_id"'"}')"
printf '%s' "$incident_json" | rg -q '"kind":"mfg.incident"'
incident_id="$(printf '%s' "$incident_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["incident"]["incident_id"])')"

capture_json mfg_incident_analysis "$BASE_URL/api/apps/mfg/incidents/$incident_id/analyze" \
  -X POST \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: skill-surface-analysis-$$" \
  -d '{}' | rg -q '"kind":"mfg.operational_analysis"'

plan_json="$(capture_json mfg_skill_plan "$BASE_URL/api/apps/mfg/incidents/$incident_id/skills/plan" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: skill-surface-plan-$$" \
  -d '{"request_id":"skill-surface-plan","session_id":"session-skill-surface","limit":3}')"
printf '%s' "$plan_json" | rg -q '"kind":"mfg.skill.plan"'
printf '%s' "$plan_json" | rg -q '"supply-risk-analyst"'

incident_current_json="$(capture_json mfg_incident_current "$BASE_URL/api/apps/mfg/incidents/$incident_id")"
incident_revision="$(printf '%s' "$incident_current_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["incident"]["revision"])')"
run_json="$(capture_json mfg_skill_run "$BASE_URL/api/apps/mfg/incidents/$incident_id/skills/supply-risk-analyst/run" \
  -H 'content-type: application/json' \
  -H "Idempotency-Key: skill-surface-run-$$" \
  -d "{\"request_id\":\"skill-surface-run\",\"session_id\":\"session-skill-surface\",\"expected_revision\":$incident_revision}")"
printf '%s' "$run_json" | rg -q '"kind":"mfg.skill.run"'
printf '%s' "$run_json" | python3 -c 'import json,sys; run=json.load(sys.stdin)["skill_run"]; results=run["tool_results"]; assert results and all(item["status"] == "completed" for item in results), results; impact=next(item for item in results if item["tool_name"] == "mfg.entity_impact_trace"); items=impact["result"]["items"]; assert items and items[0]["status"] == "completed", items; assert items[0]["impact_trace"]["hops"], items[0]["impact_trace"]'
skill_run_id="$(printf '%s' "$run_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["skill_run"]["execution_id"])')"

capture_json mfg_skill_runs "$BASE_URL/api/apps/mfg/incidents/$incident_id/skills" | rg -q '"kind":"mfg.skill.run_list"'
capture_json mfg_skill_run_detail "$BASE_URL/api/apps/mfg/skill-runs/$skill_run_id" | rg -q '"kind":"mfg.skill.run"'
