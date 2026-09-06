// A completely clean Service Worker that immediately bypasses and deletes old caches
self.addEventListener('install', (e) => {
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(keys.map((key) => caches.delete(key)));
    })
  );
  self.clients.claim();
});

self.addEventListener('fetch', (e) => {
  // Always fetch from the network first to guarantee fresh code
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});
