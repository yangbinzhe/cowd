#!/usr/bin/env node

// G67: pg_only_dependency_fixture_retirement
// Executable SQLite drivers, constructors, fixtures and backend selections are
// release blockers. Historical prose is intentionally outside this source
// gate; executable scripts, source and Cargo manifests are not.

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const root = new URL("../../", import.meta.url).pathname;
const files = execFileSync("rg", [
  "--files",
  "crates",
  "scripts",
  "-g", "*.rs",
  "-g", "Cargo.toml",
  "-g", "*.sh",
  "-g", "*.mjs",
  "-g", "*.yaml",
  "-g", "*.yml",
], { cwd: root, encoding: "utf8" }).trim().split("\n").filter(Boolean);

const forbidden = [
  /\brusqlite\b/,
  /\br2d2_sqlite\b/,
  /\bsqlite-pool-tracker\b/,
  /\bfact-sqlite\b/,
  /\bSqlite(?:Store|Executor|Repository|Connection|Pool|Session|Runtime|Surface|Task)/,
  /\bopen_in_memory\s*\(/,
  /backend\s*:\s*["']?sqlite\b/i,
];
const violations = [];
for (const file of files) {
  if (file.endsWith("scripts/test/pg-only-gate.mjs")) continue;
  // Boundary-policy inventories and their checker intentionally name banned
  // dependencies in order to reject them; they are assertions, not adapters.
  if (file.endsWith("scripts/architecture/check-boundaries.sh")) continue;
  if (file.endsWith("src/core/boundary_policy.rs")) continue;
  if (file.endsWith("src/app_core/boundary_policy.rs")) continue;
  const lines = readFileSync(new URL(`../../${file}`, import.meta.url), "utf8").split("\n");
  lines.forEach((line, index) => {
    if (forbidden.some((pattern) => pattern.test(line))) {
      violations.push(`${file}:${index + 1}`);
    }
  });
}

let dependencyViolation = false;
try {
  const tree = execFileSync("cargo", ["tree", "--workspace", "--all-features", "--edges", "all", "--locked"], {
    cwd: root,
    encoding: "utf8",
  });
  dependencyViolation = /(^|\s)(rusqlite|r2d2_sqlite|libsqlite3-sys|fact-sqlite|sqlite-pool-tracker)\b/m.test(tree);
} catch (error) {
  console.error("unable to inspect the Cargo dependency graph");
  process.exit(2);
}

const report = {
  gate: "pg_only_dependency_fixture_retirement",
  dependency_violation: dependencyViolation,
  source_violation_count: violations.length,
  source_violations: violations,
};
console.log(JSON.stringify(report, null, 2));
if (dependencyViolation || violations.length > 0) process.exit(1);
