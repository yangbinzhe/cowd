#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { chromium } from 'playwright';

const repoRoot = path.resolve(new URL('../../', import.meta.url).pathname);
const planRoot = process.env.COWD_PLAN_ROOT || path.resolve(repoRoot, '../plan/0616-前端彻底重构/10-模块化管理重构方案');
const version = process.env.COWD_VERSION || 'v0.9.245';
const baseUrl = process.env.COWD_VISUAL_BASE_URL || 'http://127.0.0.1:5195';
const screenshotDir = path.join(planRoot, 'screenshots', version);
const reportPath = path.join(planRoot, 'reports', `${version}-visual-audit.md`);

const routes = [
  { id: 'chat', path: '/#/chat' },
  { id: 'runtime', path: '/#/runtime' },
  { id: 'memory', path: '/#/memory' },
  { id: 'skills', path: '/#/skills' },
  { id: 'agents', path: '/#/agents' },
  { id: 'gateway', path: '/#/gateway' },
  { id: 'iacc', path: '/#/iacc' },
  { id: 'audit', path: '/#/audit' },
  { id: 'settings', path: '/#/settings' },
];

const viewports = [
  { id: 'mobile', width: 375, height: 812 },
  { id: 'tablet', width: 768, height: 1024 },
  { id: 'desktop', width: 1440, height: 960 },
  { id: 'wide', width: 1920, height: 1080 },
];

fs.mkdirSync(screenshotDir, { recursive: true });
fs.mkdirSync(path.dirname(reportPath), { recursive: true });

const browser = await chromium.launch({
  executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || '/snap/bin/chromium',
  args: ['--no-sandbox'],
});

const rows = [];
const failures = [];

try {
  for (const viewport of viewports) {
    const page = await browser.newPage({ viewport });
    page.setDefaultTimeout(15_000);
    for (const route of routes) {
      const url = `${baseUrl}${route.path}`;
      const fileName = `${route.id}-${viewport.id}-${viewport.width}.png`;
      const filePath = path.join(screenshotDir, fileName);
      let status = 'pass';
      let note = 'main shell rendered';
      try {
        await page.goto(url, { waitUntil: 'networkidle' });
        await page.locator('.app-shell').waitFor({ state: 'visible' });
        const mainVisible = await page.locator('.content-stage, .chat-main, main, [role="main"]').first().isVisible().catch(() => false);
        const bodyText = (await page.locator('body').innerText()).trim();
        if (!mainVisible || bodyText.length < 80) {
          status = 'review';
          note = 'shell loaded but content density is low';
        }
        await page.screenshot({ path: filePath, fullPage: true });
      } catch (error) {
        status = 'fail';
        note = error instanceof Error ? error.message : String(error);
        failures.push(`${route.id}/${viewport.id}: ${note}`);
        await page.screenshot({ path: filePath, fullPage: true }).catch(() => {});
      }
      rows.push({
        route: route.id,
        viewport: `${viewport.width}x${viewport.height}`,
        status,
        screenshot: path.relative(planRoot, filePath),
        note,
      });
    }
    await page.close();
  }
} finally {
  await browser.close();
}

const markdown = [
  `# ${version} Visual Audit`,
  '',
  `Generated: ${new Date().toISOString()}`,
  `Base URL: ${baseUrl}`,
  '',
  `Status: ${failures.length ? 'fail' : 'pass'}`,
  '',
  '| Route | Viewport | Status | Screenshot | Note |',
  '|---|---:|---|---|---|',
  ...rows.map((row) => `| ${row.route} | ${row.viewport} | ${row.status} | ${row.screenshot} | ${row.note.replace(/\|/g, '/')} |`),
  '',
  '## Findings',
  '',
  ...(failures.length ? failures.map((failure) => `- ${failure}`) : ['- No blank shell or navigation failure observed in the audited viewports.']),
  '',
].join('\n');

fs.writeFileSync(reportPath, markdown);

if (failures.length) {
  console.error(`Visual audit failed:\n${failures.map((failure) => `- ${failure}`).join('\n')}`);
  process.exit(1);
}

console.log(`Visual audit written to ${reportPath}`);
