#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"

run_step() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_script() {
  local script="$1"
  printf '\n==> %s\n' "$script"
  "$ROOT/$script"
}

run_step cargo fmt --all --check
run_step cargo test -p cowd-cli iacc --no-default-features -- --test-threads=1
run_step cargo build -p cowd-cli --no-default-features

run_script scripts/v09100_iacc_memory_case_playbook_scenario.sh
run_script scripts/v09101_iacc_digital_employee_skill_pack_scenario.sh
run_script scripts/v09102_iacc_command_center_incident_room_scenario.sh
run_script scripts/v09103_iacc_source_pack_large_data_scenario.sh
run_script scripts/v09105_iacc_data_plane_adapter_scenario.sh
run_script scripts/v09106_iacc_connector_runtime_scenario.sh
run_script scripts/v09107_iacc_ontology_entity_governance_scenario.sh
run_script scripts/v09108_iacc_metric_attention_snapshot_scenario.sh
run_script scripts/v09109_iacc_skill_runtime_scenario.sh
run_script scripts/v0998_iacc_production_release_gate.sh

printf '\nIACC v0.9.109 skill runtime gate passed.\n'
