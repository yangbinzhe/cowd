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
      if (window.api.isAuthenticated()) {
        await this.loadInitialData();
        // P1-8: Check onboarding status
        await this.checkOnboarding();
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
    // Setup message input (HTML id is 'inputArea')
    const inputArea = document.getElementById('inputArea');
    if (inputArea) {
      inputArea.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          window.Messages.send();
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
      clearChatBtn.addEventListener('click', () => window.messageRenderer?.clearMessages());
    }

    // Setup theme toggle
    const themeToggle = document.getElementById('themeToggle');
    if (themeToggle) {
      themeToggle.addEventListener('click', () => window.ThemeManager.cycleTheme());
    }

    // Setup sidebar toggle (toggle collapsed class on sidebar)
    const sidebarToggle = document.getElementById('sidebarToggle');
    if (sidebarToggle) {
      sidebarToggle.addEventListener('click', () => {
        const sidebar = document.getElementById('sidebar');
        if (sidebar) sidebar.classList.toggle('collapsed');
      });
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
    if (window.i18nInstance) {
      window.i18nInstance.setLocale(savedLocale);
      window.i18nInstance.updateAll();
    }
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

    window.api.setToken(token);

    try {
      const user = await window.api.verifyToken();
      window.appState.set('authenticated', true);
      window.appState.set('user', user);
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
    window.appState.setLoading('sessions', true);
    try {
      const sessions = await window.api.listSessions();
      window.appState.set('sessions', sessions);
      await renderSessions(sessions);
    } catch (e) {
      console.error('[Cowd] Failed to load sessions:', e);
    } finally {
      window.appState.setLoading('sessions', false);
    }
  }

  /**
   * Load configuration
   */
  async loadConfig() {
    window.appState.setLoading('config', true);
    try {
      const config = await window.api.getConfig();
      window.appState.set('config', config);

      const providers = await window.api.getProviders();
      window.appState.set('providers', providers);

      await renderConfig(config, providers);
    } catch (e) {
      console.error('[Cowd] Failed to load config:', e);
    } finally {
      window.appState.setLoading('config', false);
    }
  }

  /**
   * Load memory
   */
  async loadMemory() {
    window.appState.setLoading('memory', true);
    try {
      const memory = await window.api.getMemory();
      window.appState.set('memory', memory);
      await renderMemory(memory);
    } catch (e) {
      console.error('[Cowd] Failed to load memory:', e);
    } finally {
      window.appState.setLoading('memory', false);
    }
  }

  /**
   * Load platforms
   */
  async loadPlatforms() {
    window.appState.setLoading('platforms', true);
    try {
      const platforms = await window.api.listPlatforms();
      window.appState.set('platforms', platforms);
      await renderPlatforms(platforms);
    } catch (e) {
      console.error('[Cowd] Failed to load platforms:', e);
    } finally {
      window.appState.setLoading('platforms', false);
    }
  }

  /**
   * Load workspaces
   */
  async loadWorkspaces() {
    try {
      if (window.workspaceManager) {
        await window.workspaceManager.loadWorkspaces();
        await window.workspaceManager.getCurrentWorkspace();
      }
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
      window.messageRenderer?.clearMessages();
      window.appState.set('sessions', []);
      renderSessions([]);
    });

    // Stream events
    window.addEventListener('stream:chunk', (e) => {
      window.messageRenderer?.handleStreamChunk(e.detail);
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
      window.Toast?.success(window.i18nInstance?.t('status.connected') || 'Connected');
    });

    window.addEventListener('offline', () => {
      window.Toast?.warning(window.i18nInstance?.t('status.disconnected') || 'Disconnected');
    });

    // Before unload
    window.addEventListener('beforeunload', () => {
      window.api?.disconnect();
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
        const inputArea = document.getElementById('inputArea');
        if (inputArea) {
          window.panelManager?.show('chat');
          inputArea.focus();
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
    window.Toast?.error('初始化失败: ' + error.message);

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
      loginBtn.textContent = window.i18nInstance?.t('common.loading') || 'Loading...';
      loginBtn.disabled = true;
    }

    try {
      await window.api.login(token);
      window.appState.set('authenticated', true);
      window.appState.set('token', token);

      this.hideLoginModal();
      await this.loadInitialData();

      window.Toast?.success(window.i18nInstance?.t('common.success') || 'Success');
    } catch (e) {
      window.Toast?.error(window.i18nInstance?.t('auth.loginError') || 'Login failed');
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
    window.api.logout();
    window.appState.set('authenticated', false);
    window.appState.set('user', null);
    window.appState.set('token', null);

    window.messageRenderer?.clearMessages();
    window.appState.set('sessions', []);
    window.appState.set('currentSession', null);

    this.showLoginModal();
  }

  /**
   * Start new chat
   */
  async newChat() {
    try {
      const session = await window.api.createSession({
        title: window.i18nInstance?.t('chat.newChat') || 'New Chat',
      });

      window.appState.set('currentSession', session.id);
      window.messageRenderer?.clearMessages();

      // Add to sessions list
      const sessions = [session, ...window.appState.get('sessions')];
      window.appState.set('sessions', sessions);
      await renderSessions(sessions);

      // Switch to chat panel
      window.panelManager?.show('chat');

      // Focus input
      const inputArea = document.getElementById('inputArea');
      if (inputArea) {
        inputArea.focus();
      }
    } catch (e) {
      window.Toast?.error(e.message);
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
  const container = document.getElementById('sessionItems');
  if (!container) return;

  container.innerHTML = sessions.length === 0
    ? `<div class="empty-state">${window.i18nInstance?.t('sessions.empty') || 'No sessions'}</div>`
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
  window.appState.set('currentSession', session.id);

  // Update UI
  document.querySelectorAll('.session-item').forEach(item => {
    item.classList.toggle('active', item.getAttribute('data-session-id') === session.id);
  });

  // Load messages
  window.appState.setLoading('messages', true);
  try {
    const messages = await window.api.getMessages(session.id);
    window.appState.set('messages', messages);
    window.messageRenderer?.loadMessages(messages);
    window.panelManager?.show('chat');
  } catch (e) {
    window.Toast?.error(e.message);
  } finally {
    window.appState.setLoading('messages', false);
  }
}

// ── P1-8: Onboarding Wizard ──────────────────────────────────────────────────

/**
 * Check if onboarding is needed and show wizard
 */
async checkOnboarding() {
  try {
    const resp = await fetch('/api/onboarding/status', {
      headers: { 'Authorization': `Bearer ${localStorage.getItem('cowd_token') || ''}` }
    });
    if (!resp.ok) return;
    const data = await resp.json();
    if (data.needs_onboarding) {
      this.showOnboardingWizard();
    }
  } catch (e) {
    console.warn('[Cowd] Onboarding check failed:', e);
  }
}

/**
 * Show the onboarding wizard modal
 */
showOnboardingWizard() {
  // Don't show if already visible
  if (document.getElementById('onboardingModal')) return;

  const modal = document.createElement('div');
  modal.id = 'onboardingModal';
  modal.className = 'modal';
  modal.style.display = 'flex';
  modal.innerHTML = `
    <div class="modal-content onboarding-content">
      <div class="modal-header">
        <h2>Welcome to Cowd</h2>
        <p>Let's set up your AI assistant in a few steps.</p>
      </div>
      <div id="onboarding-steps">
        <div class="onboarding-step active" data-step="1">
          <h3>Step 1: Choose Provider</h3>
          <div class="form-group">
            <label>AI Provider</label>
            <select id="ob-provider" class="onboarding-input">
              <option value="openai">OpenAI</option>
              <option value="anthropic">Anthropic</option>
              <option value="custom">Custom (OpenAI-compatible)</option>
            </select>
          </div>
          <div class="form-group" id="ob-custom-url-group" style="display:none">
            <label>API Base URL</label>
            <input type="text" id="ob-base-url" placeholder="https://api.example.com/v1" class="onboarding-input">
          </div>
          <button class="btn primary" onclick="window.app.onboardingNext(1)">Next</button>
        </div>
        <div class="onboarding-step" data-step="2">
          <h3>Step 2: API Key</h3>
          <div class="form-group">
            <label>API Key</label>
            <input type="password" id="ob-api-key" placeholder="sk-..." class="onboarding-input">
          </div>
          <div class="form-group">
            <label>Default Model (optional)</label>
            <input type="text" id="ob-model" placeholder="gpt-4o / claude-sonnet-4-20250514" class="onboarding-input">
          </div>
          <div id="ob-test-result" class="onboarding-test-result"></div>
          <button class="btn secondary" onclick="window.app.onboardingPrev(2)">Back</button>
          <button class="btn primary" onclick="window.app.onboardingTest()">Test & Continue</button>
        </div>
        <div class="onboarding-step" data-step="3">
          <h3>Step 3: Ready!</h3>
          <p>Your configuration will be saved. You can always change it later in Settings.</p>
          <button class="btn secondary" onclick="window.app.onboardingPrev(3)">Back</button>
          <button class="btn primary" onclick="window.app.onboardingSave()">Save & Start</button>
        </div>
      </div>
      <div class="onboarding-progress">
        <div class="progress-bar"><div class="progress-fill" id="ob-progress" style="width:33%"></div></div>
      </div>
    </div>
  `;
  document.body.appendChild(modal);

  // Toggle custom URL field
  document.getElementById('ob-provider').addEventListener('change', (e) => {
    document.getElementById('ob-custom-url-group').style.display = e.target.value === 'custom' ? 'block' : 'none';
  });
}

onboardingNext(fromStep) {
  document.querySelectorAll('.onboarding-step').forEach(s => s.classList.remove('active'));
  const next = document.querySelector(`.onboarding-step[data-step="${fromStep + 1}"]`);
  if (next) next.classList.add('active');
  document.getElementById('ob-progress').style.width = `${((fromStep) / 3) * 100}%`;
}

onboardingPrev(fromStep) {
  document.querySelectorAll('.onboarding-step').forEach(s => s.classList.remove('active'));
  const prev = document.querySelector(`.onboarding-step[data-step="${fromStep - 1}"]`);
  if (prev) prev.classList.add('active');
  document.getElementById('ob-progress').style.width = `${((fromStep - 2) / 3) * 100}%`;
}

async onboardingTest() {
  const provider = document.getElementById('ob-provider').value;
  const apiKey = document.getElementById('ob-api-key').value.trim();
  const model = document.getElementById('ob-model').value.trim();
  const baseUrl = document.getElementById('ob-base-url')?.value?.trim();
  const resultEl = document.getElementById('ob-test-result');

  if (!apiKey) {
    resultEl.innerHTML = '<span class="ob-error">Please enter an API key.</span>';
    return;
  }

  resultEl.innerHTML = '<span class="ob-pending">Validating...</span>';

  try {
    const resp = await fetch('/api/onboarding/test', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${localStorage.getItem('cowd_token') || ''}`
      },
      body: JSON.stringify({ provider, api_key: apiKey, model: model || undefined, base_url: baseUrl || undefined })
    });
    const data = await resp.json();
    if (data.success) {
      resultEl.innerHTML = '<span class="ob-success">Key format validated!</span>';
      this.onboardingNext(2);
    } else {
      resultEl.innerHTML = `<span class="ob-error">${this.escapeHtml(data.error || 'Validation failed')}</span>`;
    }
  } catch (e) {
    resultEl.innerHTML = `<span class="ob-error">Test failed: ${this.escapeHtml(e.message)}</span>`;
  }
}

async onboardingSave() {
  const provider = document.getElementById('ob-provider').value;
  const apiKey = document.getElementById('ob-api-key').value.trim();
  const model = document.getElementById('ob-model').value.trim() || (provider === 'anthropic' ? 'claude-sonnet-4-20250514' : 'gpt-4o');
  const baseUrl = document.getElementById('ob-base-url')?.value?.trim();

  try {
    await window.api.updateConfig({
      providers: {
        default: provider,
        [provider]: {
          api_key: apiKey,
          model: model,
          ...(baseUrl ? { base_url: baseUrl } : {})
        }
      }
    });
  } catch (e) {
    console.warn('[Cowd] Onboarding save failed:', e);
  }

  // Close modal
  const modal = document.getElementById('onboardingModal');
  if (modal) modal.remove();

  // Reload config
  await this.loadConfig();
}

escapeHtml(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { App };
}

// Auto-initialize when DOM is ready
const app = new App();
window.app = app;
document.addEventListener('DOMContentLoaded', () => app.init());
