import { vi } from 'vitest';

vi.stubGlobal('fetch', vi.fn(() =>
  Promise.resolve({
    ok: true,
    json: () => Promise.resolve({}),
    text: () => Promise.resolve(''),
  })
));

// Provide minimal DOM for boot.js
if (typeof document !== 'undefined') {
  document.body.innerHTML = `
    <div id="toast"></div>
    <div id="chat-header"></div>
    <div id="right-panel" class="hidden"></div>
    <div id="panel-tabs">
      <button data-panel="sessions"></button>
      <button data-panel="close"></button>
    </div>
    <textarea id="chat-input"></textarea>
    <button id="btn-send"></button>
    <button id="btn-new-session"></button>
    <div id="chat-messages"></div>
  `;
}
