#!/usr/bin/env bash
# P5/T7 cold-start sampler: stop -> start -> healthz -> first mission summary
# request, repeated N times. Requires operator-provided start/stop commands;
# never touches a gateway it was not told to manage.
#
# Usage:
#   COWD_START_CMD="..." COWD_STOP_CMD="..." scripts/manual/measure-cold-start.sh
#
# Env:
#   COWD_HEALTH_URL   health endpoint (default http://127.0.0.1:8642/healthz)
#   COWD_MISSION_URL  first request endpoint (default .../api/mission/control?detail=summary)
#   COWD_TOKEN        optional Bearer token
#   COWD_ITERATIONS   sample count (default 10)
#   COWD_OUTPUT       JSON evidence path (default ./evidence/cold-start-v0.9.677.json)
#   COWD_STRICT=1     fail when mission p95 > 1000ms
set -euo pipefail

: "${COWD_START_CMD:?COWD_START_CMD is required}"
: "${COWD_STOP_CMD:?COWD_STOP_CMD is required}"
HEALTH_URL="${COWD_HEALTH_URL:-http://127.0.0.1:8642/healthz}"
MISSION_URL="${COWD_MISSION_URL:-http://127.0.0.1:8642/api/mission/control?detail=summary}"
ITERATIONS="${COWD_ITERATIONS:-10}"
OUTPUT="${COWD_OUTPUT:-./evidence/cold-start-v0.9.677.json}"
STRICT="${COWD_STRICT:-0}"

auth_curl() {
  if [[ -n "${COWD_TOKEN:-}" ]]; then
    curl -fsS -H "Authorization: Bearer ${COWD_TOKEN}" "$@"
  else
    curl -fsS "$@"
  fi
}

wait_healthy() {
  for _ in $(seq 1 240); do
    if auth_curl "${HEALTH_URL}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

health_ms=()
mission_ms=()
for i in $(seq 1 "${ITERATIONS}"); do
  echo "iteration ${i}/${ITERATIONS}: stopping gateway"
  eval "${COWD_STOP_CMD}"
  sleep 2
  start_ns=$(date +%s%N)
  echo "iteration ${i}/${ITERATIONS}: starting gateway"
  eval "${COWD_START_CMD}" &
  if ! wait_healthy; then
    echo "iteration ${i}: gateway did not become healthy within 120s" >&2
    eval "${COWD_STOP_CMD}" || true
    exit 1
  fi
  health_ns=$(date +%s%N)
  health_ms+=("$(( (health_ns - start_ns) / 1000000 ))")
  mission_start_ns=$(date +%s%N)
  if ! auth_curl "${MISSION_URL}" >/dev/null 2>&1; then
    echo "iteration ${i}: first mission request failed" >&2
    eval "${COWD_STOP_CMD}" || true
    exit 1
  fi
  mission_end_ns=$(date +%s%N)
  mission_ms+=("$(( (mission_end_ns - mission_start_ns) / 1000000 ))")
  echo "iteration ${i}: health=${health_ms[-1]}ms mission=${mission_ms[-1]}ms"
done

sort_numeric() {
  printf '%s\n' "$@" | sort -n
}

percentile() {
  local pct="$1"
  shift
  local values
  values=$(sort_numeric "$@")
  local count
  count=$(printf '%s\n' "${values}" | wc -l | tr -d ' ')
  local index
  index=$(( (count * pct + 99) / 100 ))
  printf '%s\n' "${values}" | sed -n "${index}p"
}

mean_ms() {
  local total=0
  local count=0
  for value in "$@"; do
    total=$(( total + value ))
    count=$(( count + 1 ))
  done
  echo $(( total / count ))
}

mkdir -p "$(dirname "${OUTPUT}")"
HEALTH_P95=$(percentile 95 "${health_ms[@]}")
MISSION_P95=$(percentile 95 "${mission_ms[@]}")
cat > "${OUTPUT}" <<JSON
{
  "schema": "cowd.cold_start.sample.v1",
  "version": "0.9.677",
  "iterations": ${ITERATIONS},
  "health_url": "${HEALTH_URL}",
  "mission_url": "${MISSION_URL}",
  "health_ms": [$(IFS=,; echo "${health_ms[*]}")],
  "mission_ms": [$(IFS=,; echo "${mission_ms[*]}")],
  "summary": {
    "health_mean_ms": $(mean_ms "${health_ms[@]}"),
    "health_p95_ms": ${HEALTH_P95},
    "mission_mean_ms": $(mean_ms "${mission_ms[@]}"),
    "mission_p95_ms": ${MISSION_P95},
    "threshold_ms": 1000
  }
}
JSON

echo "cold-start evidence written to ${OUTPUT}"
echo "mission p95=${MISSION_P95}ms (threshold 1000ms)"
if [[ "${STRICT}" == "1" ]] && (( MISSION_P95 > 1000 )); then
  echo "FAIL: mission projection cold start exceeds 1.0s p95" >&2
  exit 1
fi
