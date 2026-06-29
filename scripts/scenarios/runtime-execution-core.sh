#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "== Runtime Execution Core static scenario gate =="
echo "Checking model-visible runtime capabilities and orchestration wiring."

rg "runtime_orchestrate" crates/runtime/src crates/gateway/src
rg "ExecutionModeCatalog|RuntimeExecutionDecision|RewooEvidencePlan|ToolDagPlan|DeliberationPlan|ReflexionRecord" crates/runtime/src
rg "runtime-execution-core-scenario-spec" crates/harness-eval/templates scripts tests || true

echo "Scenario package template:"
echo "  crates/harness-eval/templates/runtime-execution-core-scenario-spec.md"
echo
echo "For a real model run, collect:"
echo "  report.md summary.json request-response/ tool-calls/ runtime-events/ evidence/ token-usage/ traces/"

