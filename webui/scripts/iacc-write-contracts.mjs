import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '..', '..');
const webuiRoot = resolve(__dirname, '..');
const planRoot = resolve(repoRoot, '..', 'plan', '0616-前端彻底重构', '10-模块化管理重构方案');
const version = process.env.COWD_VERSION || 'v0.9.243';
const sourcePath = resolve(webuiRoot, 'src', 'data', 'iaccWriteContracts.json');
const reportPath = resolve(planRoot, 'reports', `${version}-iacc-write-contracts.json`);

const contracts = JSON.parse(await readFile(sourcePath, 'utf8'));

const requiredDomains = ['Data Plane', 'Facts', 'Entities', 'Metrics', 'Evidence', 'Incidents', 'Cockpit'];
const requiredKeys = ['id', 'domain', 'title', 'endpoint', 'method', 'current_return', 'plan', 'dry_run', 'live', 'receipt', 'audit_ref', 'changed_refs', 'approval_required', 'kernel_boundary'];

const failures = [];
const domains = new Set();

for (const contract of contracts) {
  domains.add(contract.domain);
  for (const key of requiredKeys) {
    if (!(key in contract) || contract[key] === '' || contract[key] === null) {
      failures.push(`${contract.id || 'unknown'} missing ${key}`);
    }
  }
  if (contract.live && !contract.receipt) failures.push(`${contract.id} live endpoint lacks receipt requirement`);
  if (contract.live && !contract.live_policy) failures.push(`${contract.id} live endpoint lacks live_policy`);
  if (contract.endpoint.includes('/api/iacc/') && !String(contract.kernel_boundary || '').includes('cowd')) {
    failures.push(`${contract.id} does not explain cowd/IACC boundary`);
  }
}

for (const domain of requiredDomains) {
  if (!domains.has(domain)) failures.push(`missing governed domain ${domain}`);
}

const report = {
  version,
  generated_at: new Date().toISOString(),
  source: sourcePath,
  gates: {
    required_domains: requiredDomains,
    required_keys: requiredKeys,
    status: failures.length ? 'fail' : 'pass',
    failures,
  },
  summary: {
    contract_count: contracts.length,
    domains: Array.from(domains).sort(),
    live_count: contracts.filter((contract) => contract.live).length,
    approval_required_count: contracts.filter((contract) => contract.approval_required).length,
    quarantined_count: contracts.filter((contract) => String(contract.live_policy || '').includes('quarantined')).length,
  },
  contracts,
};

await mkdir(dirname(reportPath), { recursive: true });
await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);

if (failures.length) {
  console.error(`IACC write contract gate failed:\n${failures.map((failure) => `- ${failure}`).join('\n')}`);
  process.exit(1);
}

console.log(`IACC write contract gate passed: ${reportPath}`);
