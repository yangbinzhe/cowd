/**
 * boot.js - Application initialization
 * 负责启动应用、加载数据和初始化所有模块
 */

// Application bootstrap
class App {
  constructor() {
    this.initialized = false;
    this.initPromises = [];
  }

  /**
   * Initialize the application
   */
  async init() {
    if (this.initialized) return;

    console.log('[Cowd] Initializing application...');

    try {
      // Wait for DOM
      await this.waitForDOM();

      // Initialize UI
      this.initUI();

      // Initialize theme
      this.initTheme();

      // Initialize i18n
      this.initI18n();

      // Check authentication
      await this.checkAuth();

      // Initialize data if authenticated
      if (api.isAuthenticated()) {
        await this.loadInitialData();
      }

      // Setup event listeners
      this.setupEventListeners();

      // Mark as initialized
      this.initialized = true;
      console.log('[Cowd] Application initialized');

      // Dispatch ready event
      window.dispatchEvent(new CustomEvent('app:ready'));

    } catch (error) {
      console.error('[Cowd] Initialization failed:', error);
      this.handleInitError(error);
    }
  }

  /**
   * Wait for DOM to be ready
   */
  waitForDOM() {
    return new Promise(resolve => {
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', resolve);
      } else {
        resolve();
      }
    });
  }

  /**
   * Initialize basic UI elements
   */
  initUI() {
    // Setup message input
    const messageInput = document.getElementById('messageInput');
    if (messageInput) {
      messageInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          chatInput.send();
        }
      });
    }

    // Setup command input
    const commandInput = document.getElementById('commandInput');
    if (commandInput) {
      commandInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          const value = commandInput.value.trim();
          if (value.startsWith('/')) {
            commandManager.execute(value);
            commandInput.value = '';
          }
        }
      });
    }

    // Setup login form
    const loginForm = document.getElementById('loginForm');
    if (loginForm) {
      loginForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const token = document.getElementById('tokenInput').value.trim();
        if (token) {
          await this.login(token);
        }
      });
    }

    // Setup logout button
    const logoutBtn = document.getElementById('logoutBtn');
    if (logoutBtn) {
      logoutBtn.addEventListener('click', () => this.logout());
    }

    // Setup new chat button
    const newChatBtn = document.getElementById('newChatBtn');
    if (newChatBtn) {
      newChatBtn.addEventListener('click', () => this.newChat());
    }

    // Setup clear chat button
    const clearChatBtn = document.getElementById('clearChatBtn');
    if (clearChatBtn) {
      clearChatBtn.addEventListener('click', () => messageRenderer.clearMessages());
    }

    // Setup theme toggle
    const themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
      themeToggle.addEventListener('click', () => window.ThemeManager.cycleTheme());
    }

    // Setup sidebar toggle
    const sidebarToggle = document.getElementById('sidebarToggle');
    if (sidebarToggle) {
      sidebarToggle.addEventListener('click', () => panelManager.toggleSidebar());
    }
  }

  /**
   * Initialize theme
   */
  initTheme() {
    const savedTheme = localStorage.getItem('cowd-theme') || 'dark';
    window.ThemeManager.setTheme(savedTheme);
  }

  /**
   * Initialize i18n
   */
  initI18n() {
    const savedLocale = localStorage.getItem('locale') || 'zh-CN';
    i18nInstance.setLocale(savedLocale);
    i18nInstance.updateAll();
  }

  /**
   * Check authentication status
   */
  async checkAuth() {
    const token = localStorage.getItem('cowd-token');
    if (!token) {
      this.showLoginModal();
      return false;
    }

    api.setToken(token);

    try {
      const user = await api.verifyToken();
      appState.set('authenticated', true);
      appState.set('user', user);
      this.hideLoginModal();
      return true;
    } catch (e) {
      console.warn('[Cowd] Token verification failed:', e);
      this.logout();
      return false;
    }
  }

  /**
   * Load initial data
   */
  async loadInitialData() {
    console.log('[Cowd] Loading initial data...');

    // Load in parallel
    await Promise.all([
      this.loadSessions(),
      this.loadConfig(),
      this.loadMemory(),
      this.loadPlatforms(),
      this.loadWorkspaces(),
    ]);

    console.log('[Cowd] Initial data loaded');
  }

  /**
   * Load sessions
   */
  async loadSessions() {
    appState.setLoading('sessions', true);
    try {
      const sessions = await api.listSessions();
      appState.set('sessions', sessions);
      await renderSessions(sessions);
    } catch (e) {
      console.error('[Cowd] Failed to load sessions:', e);
    } finally {
      appState.setLoading('sessions', false);
    }
  }

  /**
   * Load configuration
   */
  async loadConfig() {
    appState.setLoading('config', true);
    try {
      const config = await api.getConfig();
      appState.set('config', config);

      const providers = await api.getProviders();
      appState.set('providers', providers);

      await renderConfig(config, providers);
    } catch (e) {
      console.error('[Cowd] Failed to load config:', e);
    } finally {
      appState.setLoading('config', false);
    }
  }

  /**
   * Load memory
   */
  async loadMemory() {
    appState.setLoading('memory', true);
    try {
      const memory = await api.getMemory();
      appState.set('memory', memory);
      await renderMemory(memory);
    } catch (e) {
      console.error('[Cowd] Failed to load memory:', e);
    } finally {
      appState.setLoading('memory', false);
    }
  }

  /**
   * Load platforms
   */
  async loadPlatforms() {
    appState.setLoading('platforms', true);
    try {
      const platforms = await api.listPlatforms();
      appState.set('platforms', platforms);
      await renderPlatforms(platforms);
    } catch (e) {
      console.error('[Cowd] Failed to load platforms:', e);
    } finally {
      appState.setLoading('platforms', false);
    }
  }

  /**
   * Load workspaces
   */
  async loadWorkspaces() {
    try {
      await workspaceManager.loadWorkspaces();
      await workspaceManager.getCurrentWorkspace();
    } catch (e) {
      console.error('[Cowd] Failed to load workspaces:', e);
    }
  }

  /**
   * Setup global event listeners
   */
  setupEventListeners() {
    // Auth events
    window.addEventListener('auth:logout', () => {
      this.showLoginModal();
      messageRenderer.clearMessages();
      appState.set('sessions', []);
      renderSessions([]);
    });

    // Stream events
    window.addEventListener('stream:chunk', (e) => {
      messageRenderer.handleStreamChunk(e.detail);
    });

    // Theme change
    window.addEventListener('theme:change', (e) => {
      console.log('[Cowd] Theme changed to:', e.detail.theme);
    });

    // Panel change
    window.addEventListener('panel:change', (e) => {
      console.log('[Cowd] Panel changed to:', e.detail.panel);
      this.onPanelChange(e.detail.panel);
    });

    // Online/offline
    window.addEventListener('online', () => {
      Toast.success(i18nInstance.t('status.connected'));
    });

    window.addEventListener('offline', () => {
      Toast.warning(i18nInstance.t('status.disconnected'));
    });

    // Before unload
    window.addEventListener('beforeunload', () => {
      api.disconnect();
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
      // Escape to close modals
      if (e.key === 'Escape') {
        this.closeAllModals();
      }

      // Cmd/Ctrl + K for command palette
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        const commandInput = document.getElementById('commandInput');
        if (commandInput) {
          panelManager.show('chat');
          commandInput.focus();
        }
      }

      // Cmd/Ctrl + N for new chat
      if ((e.metaKey || e.ctrlKey) && e.key === 'n') {
        e.preventDefault();
        this.newChat();
      }
    });
  }

  /**
   * Handle panel change
   * @param {string} panel - Panel name
   */
  async onPanelChange(panel) {
    switch (panel) {
      case 'sessions':
        await this.loadSessions();
        break;
      case 'memory':
        await this.loadMemory();
        break;
      case 'config':
        await this.loadConfig();
        break;
      case 'platform':
        await this.loadPlatforms();
        break;
    }
  }

  /**
   * Handle initialization errors
   * @param {Error} error - Error object
   */
  handleInitError(error) {
    Toast.error('初始化失败: ' + error.message);

    // Show error state in UI
    const mainContent = document.querySelector('.main-content');
    if (mainContent) {
      mainContent.innerHTML = `
        <div class="error-state">
          <h2>初始化错误</h2>
          <p>${error.message}</p>
          <button onclick="location.reload()">重新加载</button>
        </div>
      `;
    }
  }

  /**
   * Show login modal
   */
  showLoginModal() {
    const modal = document.getElementById('loginModal');
    if (modal) {
      modal.classList.add('active');
      const tokenInput = document.getElementById('tokenInput');
      if (tokenInput) {
        setTimeout(() => tokenInput.focus(), 100);
      }
    }
  }

  /**
   * Hide login modal
   */
  hideLoginModal() {
    const modal = document.getElementById('loginModal');
    if (modal) {
      modal.classList.remove('active');
    }
  }

  /**
   * Login with token
   * @param {string} token - Access token
   */
  async login(token) {
    const loginBtn = document.querySelector('#loginForm button[type="submit"]');
    const originalText = loginBtn?.textContent;
    if (loginBtn) {
      loginBtn.textContent = i18nInstance.t('common.loading');
      loginBtn.disabled = true;
    }

    try {
      await api.login(token);
      appState.set('authenticated', true);
      appState.set('token', token);

      this.hideLoginModal();
      await this.loadInitialData();

      Toast.success(i18nInstance.t('common.success'));
    } catch (e) {
      Toast.error(i18nInstance.t('auth.loginError'));
      console.error('[Cowd] Login failed:', e);
    } finally {
      if (loginBtn) {
        loginBtn.textContent = originalText;
        loginBtn.disabled = false;
      }
    }
  }

  /**
   * Logout
   */
  logout() {
    api.logout();
    appState.set('authenticated', false);
    appState.set('user', null);
    appState.set('token', null);

    messageRenderer.clearMessages();
    appState.set('sessions', []);
    appState.set('currentSession', null);

    this.showLoginModal();
  }

  /**
   * Start new chat
   */
  async newChat() {
    try {
      const session = await api.createSession({
        title: i18nInstance.t('chat.newChat'),
      });

      appState.set('currentSession', session.id);
      messageRenderer.clearMessages();

      // Add to sessions list
      const sessions = [session, ...appState.get('sessions')];
      appState.set('sessions', sessions);
      await renderSessions(sessions);

      // Switch to chat panel
      panelManager.show('chat');

      // Focus input
      const messageInput = document.getElementById('messageInput');
      if (messageInput) {
        messageInput.focus();
      }
    } catch (e) {
      Toast.error(e.message);
    }
  }

  /**
   * Close all modals
   */
  closeAllModals() {
    document.querySelectorAll('.modal.active').forEach(modal => {
      modal.classList.remove('active');
    });

    document.querySelectorAll('.context-menu').forEach(menu => {
      menu.remove();
    });
  }
}

// Helper functions for rendering panels
async function renderSessions(sessions) {
  const container = document.getElementById('sessionsList');
  if (!container) return;

  container.innerHTML = sessions.length === 0
    ? `<div class="empty-state">${i18nInstance.t('sessions.empty')}</div>`
    : '';

  sessions.forEach(session => {
    const item = document.createElement('div');
    item.className = 'session-item';
    item.setAttribute('data-session-id', session.id);

    const time = new Date(session.updatedAt || session.createdAt);
    const timeStr = formatRelativeTime(time);

    item.innerHTML = `
      <div class="session-title">${escapeHtml(session.title)}</div>
      <div class="session-time">${timeStr}</div>
    `;

    item.addEventListener('click', () => selectSession(session));
    container.appendChild(item);
  });
}

async function renderConfig(config, providers) {
  // Config panel is rendered via HTML forms
  // This function can be used to update reactive elements
}

async function renderMemory(memory) {
  // Memory panel is rendered via HTML
  // This function can be used to update memory visualizations
}

async function renderPlatforms(platforms) {
  // Platform panel is rendered via HTML
  // This function can be used to update platform status
}

// Helper functions
function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function formatRelativeTime(date) {
  const now = new Date();
  const diff = now - date;
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days = Math.floor(diff / 86400000);

  if (minutes < 1) return '刚刚';
  if (minutes < 60) return `${minutes}分钟前`;
  if (hours < 24) return `${hours}小时前`;
  if (days < 7) return `${days}天前`;
  return date.toLocaleDateString();
}

async function selectSession(session) {
  appState.set('currentSession', session.id);

  // Update UI
  document.querySelectorAll('.session-item').forEach(item => {
    item.classList.toggle('active', item.getAttribute('data-session-id') === session.id);
  });

  // Load messages
  appState.setLoading('messages', true);
  try {
    const messages = await api.getMessages(session.id);
    appState.set('messages', messages);
    messageRenderer.loadMessages(messages);
    panelManager.show('chat');
  } catch (e) {
    Toast.error(e.message);
  } finally {
    appState.setLoading('messages', false);
  }
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { App };
}

// Auto-initialize when DOM is ready
const app = new App();
document.addEventListener('DOMContentLoaded', () => app.init());
