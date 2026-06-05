import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: ['*.e2e.spec.js'],
  use: {
    baseURL: 'http://127.0.0.1:9241',
    serviceWorkers: 'block',
  },
  webServer: {
    command: 'python3 -m http.server 9241 --bind 127.0.0.1',
    url: 'http://127.0.0.1:9241/index.html',
    reuseExistingServer: !process.env.CI,
    timeout: 10_000,
  },
});
