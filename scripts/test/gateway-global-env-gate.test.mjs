import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

// Isolated command doubles test the runner, never Gateway or PostgreSQL behavior.
function run(t, summary, exitCode = '0') {
  const root = mkdtempSync(join(tmpdir(), 'cowd-global-env-gate-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, 'scripts/test'), { recursive: true });
  mkdirSync(join(root, 'bin'));
  copyFileSync(new URL('gateway-global-env.sh', import.meta.url), join(root, 'scripts/test/gateway-global-env.sh'));
  writeFileSync(join(root, 'bin/cargo'), '#!/bin/sh\nprintf "%s\\n" "$@" > "$GATE_ARGS"\nprintf "%s\\n" "$GATE_SUMMARY"\nexit "$GATE_EXIT"\n', { mode: 0o700 });
  const args = join(root, 'args');
  const result = spawnSync('bash', [join(root, 'scripts/test/gateway-global-env.sh'), 'fixture_case'], {
    env: { ...process.env, PATH: `${root}/bin:${process.env.PATH}`, COWD_REPORT_DIR: join(root, 'report'), GATE_ARGS: args, GATE_SUMMARY: summary, GATE_EXIT: exitCode },
    encoding: 'utf8', timeout: 10_000,
  });
  assert.ifError(result.error);
  return { ...result, args: readFileSync(args, 'utf8').split('\n') };
}

test('global-env runner requires one exact locked test', t => {
  const result = run(t, 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out;');
  assert.equal(result.status, 0, result.stderr);
  for (const flag of ['--locked', '--exact', '--ignored', '--test-threads=1', 'tests::fixture_case']) assert.ok(result.args.includes(flag));
});
for (const [label, summary] of [
  ['missing', 'test result: ok. 0 passed; 0 failed; 0 ignored;'],
  ['ambiguous', 'test result: ok. 2 passed; 0 failed; 0 ignored;'],
  ['ignored', 'test result: ok. 0 passed; 0 failed; 1 ignored;'],
  ['no summary', 'compiler finished'],
]) test(`global-env runner rejects ${label}`, t => assert.notEqual(run(t, summary).status, 0));
test('global-env runner propagates Cargo failure despite a passing-looking summary', t => {
  assert.notEqual(run(t, 'test result: ok. 1 passed; 0 failed; 0 ignored;', '101').status, 0);
});
