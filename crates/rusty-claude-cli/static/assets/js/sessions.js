/**
 * Cowd Sessions - Session Management Module
 */

const Sessions = {
  container: null,

  init() {
    this.container = document.getElementById('sessionItems');
  },

  async loadSessions() {
    if (!window.api?.isAuthenticated()) {
      return;
    }

    try {
      window.appState?.set('isLoading', true);
      const sessions = await window.api?.listSessions();
      window.appState?.set('sessions', sessions);
      this.renderSessions();
    } catch (error) {
      console.error('Failed to load sessions:', error);
      window.Toast?.error('加载会话失败');
    } finally {
      window.appState?.set('isLoading', false);
    }
  },

  renderSessions() {
    if (!this.container) return;

    const sessions = window.appState?.get('sessions') || [];

    if (sessions.length === 0) {
      this.container.innerHTML = `
        <div class="session-empty" style="padding: 24px; text-align: center; color: var(--text-dim);">
          ${window.i18nInstance?.t('sessions.empty')}
        </div>
      `;
      return;
    }

    this.container.innerHTML = sessions.map(session => `
      <div class="session-item ${window.appState?.get('currentSession')?.id === session.id ? 'active' : ''}"
           data-session-id="${session.id}">
        <span class="session-icon">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
          </svg>
        </span>
        <span class="session-title">${this.escapeHtml(session.title || '新对话')}</span>
        <span class="session-time">${this.formatTime(session.updated_at)}</span>
      </div>
    `).join('');

    // Bind click handlers
    this.container.querySelectorAll('.session-item').forEach(el => {
      el.addEventListener('click', () => {
        const sessionId = el.dataset.sessionId;
        this.selectSession(sessionId);
      });

      // Right-click for context menu
      el.addEventListener('contextmenu', (e) => {
        e.preventDefault();
        const sessionId = el.dataset.sessionId;
        this.showContextMenu(e, sessionId);
      });
    });
  },

  async selectSession(sessionId) {
    const sessions = window.appState?.get('sessions') || [];
    const session = sessions.find(s => s.id === sessionId);

    if (!session) return;

    window.appState?.set('currentSession', session);

    // Update UI
    this.container?.querySelectorAll('.session-item').forEach(el => {
      el.classList.toggle('active', el.dataset.sessionId === sessionId);
    });

    // Load messages
    window.Messages?.loadMessages(sessionId);

    // Switch to chat panel
    window.panelManager?.show('chat');
  },

  async createSession(title = null) {
    try {
      const session = await window.api?.createSession(title);
      window.appState?.update('sessions', sessions => [session, ...(sessions || [])]);
      this.renderSessions();
      return session;
    } catch (error) {
      window.Toast?.error('创建会话失败');
      throw error;
    }
  },

  async deleteSession(sessionId) {
    try {
      await window.api?.deleteSession(sessionId);
      window.appState?.update('sessions', sessions =>
        (sessions || []).filter(s => s.id !== sessionId)
      );

      // Clear if deleted current session
      if (window.appState?.get('currentSession')?.id === sessionId) {
        window.appState?.set('currentSession', null);
        window.Messages?.newChat();
      }

      this.renderSessions();
      window.Toast?.success('会话已删除');
    } catch (error) {
      window.Toast?.error('删除会话失败');
    }
  },

  showContextMenu(event, sessionId) {
    // Remove existing menu
    const existing = document.querySelector('.session-context-menu');
    if (existing) existing.remove();

    const menu = document.createElement('div');
    menu.className = 'session-context-menu';
    menu.innerHTML = `
      <button class="menu-item delete">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <polyline points="3 6 5 6 21 6"></polyline>
          <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
        </svg>
        ${window.i18nInstance?.t('sessions.delete')}
      </button>
    `;

    menu.style.cssText = `
      position: fixed;
      top: ${event.clientY}px;
      left: ${event.clientX}px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 4px;
      z-index: 1000;
      min-width: 150px;
      box-shadow: 0 4px 12px var(--shadow);
    `;

    const style = document.createElement('style');
    style.textContent = `
      .menu-item {
        display: flex;
        align-items: center;
        gap: 8px;
        width: 100%;
        padding: 8px 12px;
        background: none;
        border: none;
        border-radius: var(--radius-sm);
        color: var(--text);
        font-size: var(--font-size-sm);
        cursor: pointer;
        text-align: left;
      }
      .menu-item:hover {
        background: var(--surface-hover);
      }
      .menu-item.delete {
        color: var(--red);
      }
      .menu-item.delete:hover {
        background: rgba(255, 107, 107, 0.1);
      }
    `;

    document.head.appendChild(style);
    document.body.appendChild(menu);

    // Bind delete
    menu.querySelector('.delete').addEventListener('click', () => {
      menu.remove();
      if (confirm(t('sessions.deleteConfirm'))) {
        this.deleteSession(sessionId);
      }
    });

    // Close on click outside
    const closeMenu = (e) => {
      if (!menu.contains(e.target)) {
        menu.remove();
        document.removeEventListener('click', closeMenu);
      }
    };

    setTimeout(() => {
      document.addEventListener('click', closeMenu);
    }, 0);
  },

  formatTime(timestamp) {
    if (!timestamp) return '';

    const date = new Date(timestamp);
    const now = new Date();
    const diff = now - date;

    // Less than 1 minute
    if (diff < 60000) {
      return '刚刚';
    }

    // Less than 1 hour
    if (diff < 3600000) {
      return Math.floor(diff / 60000) + '分钟前';
    }

    // Less than 1 day
    if (diff < 86400000) {
      return Math.floor(diff / 3600000) + '小时前';
    }

    // Less than 7 days
    if (diff < 604800000) {
      return Math.floor(diff / 86400000) + '天前';
    }

    // Otherwise show date
    return date.toLocaleDateString('zh-CN', {
      month: 'short',
      day: 'numeric'
    });
  },

  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
};

// Export
window.Sessions = Sessions;
