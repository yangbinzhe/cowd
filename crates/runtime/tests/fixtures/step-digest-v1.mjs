import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

function length(value) {
  const bytes = Buffer.alloc(8);
  bytes.writeBigUInt64BE(BigInt(value));
  return bytes;
}

function encodedBytes(bytes) {
  return Buffer.concat([length(bytes.length), bytes]);
}

function textBytes(text) {
  if (!text.isWellFormed()) throw new Error('unpaired Unicode surrogate');
  return Buffer.from(text);
}

function encode(value) {
  if (value === null) return Buffer.from('0');
  if (value === false) return Buffer.from('f');
  if (value === true) return Buffer.from('t');
  if (typeof value === 'string') return Buffer.concat([Buffer.from('s'), encodedBytes(textBytes(value))]);
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || Math.abs(value) > Number.MAX_SAFE_INTEGER) throw new Error('unsafe number');
    const bytes = Buffer.alloc(8);
    bytes.writeDoubleBE(Object.is(value, -0) ? 0 : value);
    return Buffer.concat([Buffer.from('n'), bytes]);
  }
  if (Array.isArray(value)) return Buffer.concat([Buffer.from('a'), length(value.length), ...value.map(encode)]);
  const entries = Object.entries(value).sort(([left], [right]) => Buffer.compare(textBytes(left), textBytes(right)));
  return Buffer.concat([Buffer.from('o'), length(entries.length), ...entries.flatMap(([key, entry]) => [encodedBytes(textBytes(key)), encode(entry)])]);
}

export function digest(value) {
  const version = Buffer.alloc(4);
  version.writeUInt32BE(1);
  return 'sha256:' + createHash('sha256').update(Buffer.concat([
    Buffer.from('cowd:canonical-json\0'), version, encodedBytes(Buffer.from('golden')), encodedBytes(encode(value)),
  ])).digest('hex');
}

const fixtures = JSON.parse(readFileSync(new URL('./step-digest-v1.json', import.meta.url), 'utf8'));
for (const fixture of fixtures) assert.equal(digest(JSON.parse(fixture.json)), fixture.digest, fixture.name);
assert.throws(() => digest(Number.MAX_SAFE_INTEGER + 1));
assert.throws(() => digest('\ud800'));
console.log(JSON.stringify({ passed: fixtures.length + 2, failed: 0 }));
