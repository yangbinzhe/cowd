#!/usr/bin/env node

// Exercise the actual gate entrypoints; command doubles are restricted to
// disposable fixtures and are not evidence of database integration coverage.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "cowd-pg-gates-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const dir of ["scripts/test", "crates/example/src", "bin"]) {
    mkdirSync(join(root, dir), { recursive: true });
  }
  for (const name of ["pg-only-gate.mjs", "pg-test-env.sh"]) {
    copyFileSync(new URL(name, import.meta.url), join(root, "scripts/test", name));
  }
  writeFileSync(join(root, "crates/example/src/lib.rs"), "pub fn clean() {}\n");
  writeFileSync(join(root, "bin/cargo"), `#!/bin/sh
printf '%s\\n' "$@" > "$GATE_ARGS"
printf '%s\\n' "$GATE_TREE"
exit "$GATE_CARGO_EXIT"
`, { mode: 0o700 });
  const env = { ...process.env, PATH: `${root}/bin:${process.env.PATH}`, GATE_ARGS: join(root, "cargo-args"), GATE_TREE: "example v0.0.0", GATE_CARGO_EXIT: "0" };
  delete env.COWD_TEST_POSTGRES_URL;
  return { root, env };
}

function sourceGate(root, env) {
  const result = spawnSync(process.execPath, [join(root, "scripts/test/pg-only-gate.mjs")], { env, encoding: "utf8", timeout: 10000 });
  assert.ifError(result.error);
  return result;
}

test("G67 clean source passes and inspects locked all-feature all-edge dependencies", t => {
  const { root, env } = fixture(t);
  const result = sourceGate(root, env);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).source_violation_count, 0);
  assert.deepEqual(readFileSync(env.GATE_ARGS, "utf8").trim().split("\n"), ["tree", "--workspace", "--all-features", "--edges", "all", "--locked"]);
});

// Split forbidden spellings so the executable negative-test fixture itself
// does not contain the retired dependency or constructor as executable code.
const banned = [
  ["driver", 'use ' + 'rus' + 'qlite::Connection;'],
  ["pool driver", 'use ' + 'r2d2_' + 'sqlite::Pool;'],
  ["constructor", 'let store = ' + 'Sqlite' + 'Store::new();'],
  ["memory fixture", 'let store = ' + 'open_in_' + 'memory();'],
  ["backend", 'backend: ' + 'sql' + 'ite'],
];
for (const [name, source] of banned) {
  test(`G67 rejects ${name} in executable source`, t => {
    const { root, env } = fixture(t);
    writeFileSync(join(root, "crates/example/src/lib.rs"), `${source}\n`);
    const result = sourceGate(root, env);
    assert.equal(result.status, 1, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout).source_violations, ["crates/example/src/lib.rs:1"]);
  });
}

test("G67 rejects a forbidden transitive dependency even with clean source", t => {
  const { root, env } = fixture(t);
  env.GATE_TREE = "example v0.0.0\n  " + "libsqlite" + "3-sys v0.0.0";
  const result = sourceGate(root, env);
  assert.equal(result.status, 1, result.stderr);
  assert.equal(JSON.parse(result.stdout).dependency_violation, true);
});

test("G67 fails closed when dependency inspection fails", t => {
  const { root, env } = fixture(t);
  env.GATE_CARGO_EXIT = "42";
  const result = sourceGate(root, env);
  assert.equal(result.status, 2);
  assert.match(result.stderr, /unable to inspect/);
});

test("G67 fails closed when source enumeration fails", t => {
  const { root, env } = fixture(t);
  writeFileSync(join(root, "bin/rg"), "#!/bin/sh\nexit 9\n", { mode: 0o700 });
  const result = sourceGate(root, env);
  assert.notEqual(result.status, 0);
  assert.equal(result.stdout, "", "a scan failure must not emit a passing report");
});

function pgGate(root, env, args = ["bash", "-c", "echo DOWNSTREAM_RAN"]) {
  const result = spawnSync("bash", [join(root, "scripts/test/pg-test-env.sh"), ...args], { env, encoding: "utf8", timeout: 10000 });
  assert.ifError(result.error);
  return result;
}

test("G68 missing database configuration rejects downstream execution", t => {
  const { root, env } = fixture(t);
  const result = pgGate(root, env);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /set COWD_TEST_POSTGRES_URL/);
  assert.doesNotMatch(result.stdout, /DOWNSTREAM_RAN/);
});

test("G68 failed connection rejects downstream execution", t => {
  const { root, env } = fixture(t);
  env.COWD_TEST_POSTGRES_URL = "postgresql://127.0.0.1:1/unreachable";
  writeFileSync(join(root, "bin/psql"), "#!/bin/sh\nexit 7\n", { mode: 0o700 });
  const result = pgGate(root, env);
  assert.equal(result.status, 7);
  assert.doesNotMatch(result.stdout, /DOWNSTREAM_RAN/);
});

test("G68 rejects absent downstream command", t => {
  const { root, env } = fixture(t);
  env.COWD_TEST_POSTGRES_URL = "postgresql://fixture/contract";
  assert.equal(pgGate(root, env, []).status, 2);
});

test("G68 forwards the verified command and its failure status", t => {
  const { root, env } = fixture(t);
  env.COWD_TEST_POSTGRES_URL = "postgresql://fixture/contract";
  writeFileSync(join(root, "bin/psql"), "#!/bin/sh\nexit 0\n", { mode: 0o700 });
  const result = pgGate(root, env, ["bash", "-c", "echo DOWNSTREAM_RAN; exit 23"]);
  assert.equal(result.status, 23);
  assert.match(result.stdout, /DOWNSTREAM_RAN/);
});

function contractGateFixture(t, mode) {
  const { root, env } = fixture(t);
  copyFileSync(new URL("postgres-contract.sh", import.meta.url), join(root, "scripts/test/postgres-contract.sh"));
  env.CONTRACT_MODE = mode;
  env.COWD_TEST_POSTGRES_URL = "postgresql://fixture/contract";
  env.COWD_PG_MATRIX_REPORT = join(root, "matrix.json");
  env.CONTRACT_TRACE = join(root, "trace");
  writeFileSync(env.COWD_PG_MATRIX_REPORT, "{}\n");
  writeFileSync(env.CONTRACT_TRACE, "");
  writeFileSync(join(root, "bin/cargo"), `#!/usr/bin/env bash
set -eu
[[ "$1" == pkgid ]] && exit 0
printf '%s\\n' "$*" >> "$CONTRACT_TRACE"
selected=""
for arg in "$@"; do
  [[ "$arg" == -- ]] && break
  selected="$arg"
done
if [[ " $* " == *" --list "* ]]; then
  case "$CONTRACT_MODE" in
    missing) echo '0 tests, 0 benchmarks' ;;
    duplicate) printf 'one::%s: test\\ntwo::%s: test\\n' "$selected" "$selected" ;;
    *) printf 'tests::%s: test\\n' "$selected" ;;
  esac
else
  [[ " $* " == *" --exact "* ]] || exit 41
  if [[ "$CONTRACT_MODE" == zero ]]; then
    echo 'test result: ok. 0 passed; 0 failed;'
  else
    echo 'test result: ok. 1 passed; 0 failed;'
  fi
fi
`, { mode: 0o700 });
  const result = spawnSync("bash", [join(root, "scripts/test/postgres-contract.sh")], {
    env, encoding: "utf8", timeout: 30000,
  });
  assert.ifError(result.error);
  return { result, trace: readFileSync(env.CONTRACT_TRACE, "utf8") };
}

for (const mode of ["missing", "duplicate"]) {
  test(`PG contract rejects ${mode} listed tests before execution`, t => {
    const { result, trace } = contractGateFixture(t, mode);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /requires exactly one listed ignored test/);
    assert.doesNotMatch(trace, /--exact/);
  });
}

test("PG contract rejects zero executed tests after a matching listing", t => {
  const { result } = contractGateFixture(t, "zero");
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /did not execute exactly one passing test/);
});

test("PG contract inventory includes durable embedding recovery and uses exact all-feature execution", t => {
  const { result, trace } = contractGateFixture(t, "pass");
  assert.equal(result.status, 0, result.stderr);
  const lines = trace.trim().split("\n");
  assert.ok(lines.every(line => line.startsWith("test --locked --all-features ")));
  assert.equal(lines.filter(line => line.includes(" --list ")).length,
    lines.filter(line => line.includes(" --exact ")).length);
  assert.match(trace, /--lib tests::real_postgres_embedding_partial_progress_survives_store_and_client_reopen -- --exact --ignored/);
});
