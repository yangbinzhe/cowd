const CACHE = 'cowd-v3';
const ASSETS = [
  '/',
  'index.html',
  'style.css',
  'api.js',
  'ui.js',
  'messages.js',
  'sessions.js',
  'workspace.js',
  'panels.js',
  'commands.js',
  'boot.js',
  'assets/logo.svg',
  'assets/favicon.svg'
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  const url = new URL(e.request.url);
  const networkFirst = ['.html', '.js', '.css'].some(ext => url.pathname.endsWith(ext)) || url.pathname === '/';
  if (networkFirst) {
    e.respondWith(
      fetch(e.request)
        .then(response => {
          const copy = response.clone();
          caches.open(CACHE).then(c => c.put(e.request, copy));
          return response;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});
