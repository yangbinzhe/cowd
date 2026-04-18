/**
 * cc-webui -- sessions.js
 * Session list rendering, CRUD, switching.
 */

// ── Load sessions ──────────────────────────────────────────────────────────

async function loadSessions() {
  try {
    const r = await fetch('/api/sessions');
    const data = await r.json();
    window.S._allSessions = data.sessions || [];
    renderSessionList(window.S._allSessions);
  } catch (e) {
    const el = document.getElementById('sessionsLoading');
    if (el) el.textContent = 'Failed to load sessions';
  }
}


// ── Render session list ────────────────────────────────────────────────────

function renderSessionList(sessions) {
  const list = document.getElementById('sessionList');
  if (!list) return;

  if (!sessions.length) {
    list.innerHTML = '<div class="sessions-loading" style="color:var(--muted);text-align:center;padding:24px 12px">No conversations yet</div>';
    return;
  }

  list.innerHTML = sessions.map(s => {
    const active = window.S.session && window.S.session.id === s.id ? ' active' : '';
    const ago = _timeAgo(s.updated_at);
    return `
      <div class="session-item${active}" data-id="${esc(s.id)}" onclick="switchSession('${esc(s.id)}')">
        <div style="flex:1;min-width:0">
          <div class="session-title">${esc(s.title || 'Untitled')}</div>
          <div class="session-meta">${ago}</div>
        </div>
        <button class="session-delete" onclick="event.stopPropagation();deleteSession('${esc(s.id)}')" title="Delete">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
        </button>
      </div>
    `;
  }).join('');
}


// ── Filter sessions ────────────────────────────────────────────────────────

function filterSessions(query) {
  const all = window.S._allSessions || [];
  if (!query.trim()) {
    renderSessionList(all);
    return;
  }
  const q = query.toLowerCase();
  renderSessionList(all.filter(s => (s.title || '').toLowerCase().includes(q)));
}


// ── Switch session ─────────────────────────────────────────────────────────

async function switchSession(sessionId) {
  if (window.S.session && window.S.session.id === sessionId) return;
  closeMobileSidebar();

  try {
    const r = await fetch(`/api/sessions/${sessionId}`);
    const session = await r.json();
    window.S.session = session;

    // Update topbar
    const titleEl = document.getElementById('topbarTitle');
    const metaEl = document.getElementById('topbarMeta');
    if (titleEl) titleEl.textContent = session.title || 'Untitled';
    if (metaEl) metaEl.textContent = `${(session.messages || []).length} messages`;

    // Show/hide delete button
    const delBtn = document.getElementById('btnDeleteSession');
    if (delBtn) delBtn.style.display = '';

    // Render messages
    renderMessages(session.messages || []);

    // Highlight in sidebar
    document.querySelectorAll('.session-item').forEach(el => {
      el.classList.toggle('active', el.dataset.id === sessionId);
    });

    updateInfoPanel();
    scrollToBottom(false);
  } catch (e) {
    showToast('Failed to load session');
  }
}


// ── New session ────────────────────────────────────────────────────────────

async function newSession() {
  try {
    const r = await fetch('/api/sessions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'create' }),
    });
    const session = await r.json();
    window.S.session = session;
    window.S._allSessions = [
      { id: session.id, title: session.title, updated_at: session.updated_at, message_count: 0 },
      ...(window.S._allSessions || []),
    ];
    renderSessionList(window.S._allSessions);

    // Clear messages
    const inner = document.getElementById('msgInner');
    if (inner) inner.innerHTML = '';
    const empty = document.getElementById('emptyState');
    if (empty) empty.style.display = 'flex';

    // Update topbar
    const titleEl = document.getElementById('topbarTitle');
    const metaEl = document.getElementById('topbarMeta');
    if (titleEl) titleEl.textContent = 'New conversation';
    if (metaEl) metaEl.textContent = 'Start a new conversation';

    const delBtn = document.getElementById('btnDeleteSession');
    if (delBtn) delBtn.style.display = 'none';

    // Highlight
    document.querySelectorAll('.session-item').forEach(el => {
      el.classList.toggle('active', el.dataset.id === session.id);
    });

    updateInfoPanel();
    const msgEl = document.getElementById('msg');
    if (msgEl) msgEl.focus();
  } catch (e) {
    showToast('Failed to create session');
  }
}


// ── Delete session ─────────────────────────────────────────────────────────

async function deleteSession(sessionId) {
  if (!confirm('Delete this conversation?')) return;
  try {
    await fetch('/api/sessions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'delete', session_id: sessionId }),
    });
    window.S._allSessions = (window.S._allSessions || []).filter(s => s.id !== sessionId);
    renderSessionList(window.S._allSessions);

    if (window.S.session && window.S.session.id === sessionId) {
      window.S.session = null;
      const inner = document.getElementById('msgInner');
      if (inner) inner.innerHTML = '';
      const empty = document.getElementById('emptyState');
      if (empty) empty.style.display = 'flex';
      const delBtn = document.getElementById('btnDeleteSession');
      if (delBtn) delBtn.style.display = 'none';
      const titleEl = document.getElementById('topbarTitle');
      if (titleEl) titleEl.textContent = 'cc — AI Assistant';
    }
    showToast('Conversation deleted');
  } catch (e) {
    showToast('Failed to delete session');
  }
}


// ── Delete current session ─────────────────────────────────────────────────

function deleteCurrentSession() {
  if (!window.S.session) return;
  deleteSession(window.S.session.id);
}


// ── Rename session ─────────────────────────────────────────────────────────

async function renameSession(sessionId, newTitle) {
  if (!sessionId || !newTitle || !newTitle.trim()) return;
  try {
    await fetch('/api/sessions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'rename', session_id: sessionId, title: newTitle.trim() }),
    });
    // 更新本地缓存
    const sess = (window.S._allSessions || []).find(s => s.id === sessionId);
    if (sess) sess.title = newTitle.trim();
    renderSessionList(window.S._allSessions || []);
    // 如果是当前会话，更新顶栏
    if (window.S.session && window.S.session.id === sessionId) {
      window.S.session.title = newTitle.trim();
      const titleEl = document.getElementById('topbarTitle');
      if (titleEl) titleEl.textContent = newTitle.trim();
    }
    showToast('会话已重命名');
  } catch (e) {
    showToast('重命名失败');
  }
}


// ── Rename current session (prompt) ──────────────────────────────────────────

function renameCurrentSession() {
  if (!window.S.session) return;
  const current = window.S.session.title || '';
  const newTitle = window.prompt('输入新会话名称：', current);
  if (newTitle !== null && newTitle.trim()) {
    renameSession(window.S.session.id, newTitle);
  }
}


// ── Render messages ────────────────────────────────────────────────────────

function renderMessages(messages) {
  const inner = document.getElementById('msgInner');
  const empty = document.getElementById('emptyState');
  if (!inner) return;

  inner.innerHTML = '';

  if (!messages.length) {
    if (empty) empty.style.display = 'flex';
    return;
  }
  if (empty) empty.style.display = 'none';

  messages.forEach(m => {
    if (m.role === 'user' || m.role === 'assistant') {
      appendMessage(m.role, m.content, m.timestamp, false);
    }
  });
}


// ── Append a single message ────────────────────────────────────────────────

function appendMessage(role, content, timestamp, animate = true) {
  const inner = document.getElementById('msgInner');
  const empty = document.getElementById('emptyState');
  if (!inner) return null;
  if (empty) empty.style.display = 'none';

  const row = document.createElement('div');
  row.className = `msg-row ${role}`;
  if (!animate) row.style.animation = 'none';

  const bubble = document.createElement('div');
  bubble.className = 'msg-bubble';

  if (role === 'assistant') {
    bubble.innerHTML = MD.render(content || '');
  } else {
    // User messages: plain text with HTML escape
    bubble.textContent = content || '';
  }

  const timeEl = document.createElement('div');
  timeEl.className = 'msg-time';
  timeEl.textContent = timestamp ? _fmtTime(timestamp * 1000) : '';

  row.appendChild(bubble);
  row.appendChild(timeEl);
  inner.appendChild(row);

  return bubble;
}


// ── Helpers ────────────────────────────────────────────────────────────────

function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                  .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function _timeAgo(ts) {
  if (!ts) return '';
  const diff = Date.now() / 1000 - ts;
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function _fmtTime(ms) {
  const d = new Date(ms);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}
