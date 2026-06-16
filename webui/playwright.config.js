import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: ['webui-next.e2e.spec.js'],
  testIgnore: ['*.live.e2e.spec.js'],
  use: {
    baseURL: 'http://127.0.0.1:9241',
    serviceWorkers: 'block',
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || '/snap/bin/chromium',
    },
  },
  webServer: {
    command: 'npm run dev -- --port 9241',
    url: 'http://127.0.0.1:9241/index.html',
    reuseExistingServer: !process.env.CI,
    timeout: 20_000,
  },
});
