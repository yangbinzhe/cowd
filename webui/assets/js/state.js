/**
 * Cowd State - Application State Management
 */

class AppState {
  constructor() {
    this._state = {
      currentSession: null,
      sessions: [],
      messages: [],
      isLoading: false,
      isStreaming: false,
      currentStream: null,
      config: null,
      memory: null,
      platforms: []
    };

    this._listeners = new Map();
  }

  /**
   * Get current state
   */
  get(key) {
    if (key) {
      return this._state[key];
    }
    return { ...this._state };
  }

  /**
   * Set state
   */
  set(key, value) {
    const oldValue = this._state[key];
    this._state[key] = value;
    this._notify(key, value, oldValue);
  }

  /**
   * Update state partially
   */
  update(key, updater) {
    const oldValue = this._state[key];
    const newValue = typeof updater === 'function'
      ? updater(oldValue)
      : { ...oldValue, ...updater };
    this._state[key] = newValue;
    this._notify(key, newValue, oldValue);
  }

  /**
   * Subscribe to state changes
   */
  subscribe(key, callback) {
    if (!this._listeners.has(key)) {
      this._listeners.set(key, new Set());
    }
    this._listeners.get(key).add(callback);

    // Return unsubscribe function
    return () => {
      this._listeners.get(key)?.delete(callback);
    };
  }

  /**
   * Notify listeners of state change
   */
  _notify(key, newValue, oldValue) {
    this._listeners.get(key)?.forEach(cb => cb(newValue, oldValue));
    this._listeners.get('*')?.forEach(cb => cb(key, newValue, oldValue));
  }

  /**
   * Reset state
   */
  reset() {
    const keys = Object.keys(this._state);
    this._state = {
      currentSession: null,
      sessions: [],
      messages: [],
      isLoading: false,
      isStreaming: false,
      currentStream: null,
      config: null,
      memory: null,
      platforms: []
    };
    keys.forEach(key => this._notify(key, this._state[key], null));
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Theme Management
// ═══════════════════════════════════════════════════════════════════════════

const ThemeManager = {
  THEMES: ['dark', 'light', 'slate'],

  init() {
    const saved = localStorage.getItem('cowd-theme') || 'dark';
    this.setTheme(saved);
  },

  getTheme() {
    return localStorage.getItem('cowd-theme') || 'dark';
  },

  setTheme(theme) {
    if (!this.THEMES.includes(theme)) {
      console.warn(`Unknown theme: ${theme}, using dark`);
      theme = 'dark';
    }

    localStorage.setItem('cowd-theme', theme);
    document.documentElement.dataset.theme = theme;

    // Update toggle button
    const toggle = document.getElementById('themeToggle');
    if (toggle) {
      toggle.title = this.getThemeTitle(theme);
    }
  },

  cycleTheme() {
    const current = this.getTheme();
    const index = this.THEMES.indexOf(current);
    const next = this.THEMES[(index + 1) % this.THEMES.length];
    this.setTheme(next);
    return next;
  },

  getThemeTitle(theme) {
    const titles = {
      dark: '切换到浅色主题',
      light: '切换到 Slate 主题',
      slate: '切换到深色主题'
    };
    return titles[theme] || '切换主题';
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// Toast Notifications
// ═══════════════════════════════════════════════════════════════════════════

const Toast = {
  show(message, type = 'info', duration = 4000) {
    const container = document.getElementById('toastContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;

    const icons = {
      success: '✓',
      error: '✕',
      info: 'ℹ',
      warning: '⚠'
    };

    toast.innerHTML = `
      <span class="toast-icon">${icons[type] || icons.info}</span>
      <span class="toast-message">${message}</span>
    `;

    container.appendChild(toast);

    // Auto remove
    setTimeout(() => {
      toast.style.animation = 'fadeIn 0.3s ease reverse';
      setTimeout(() => toast.remove(), 300);
    }, duration);

    return toast;
  },

  success(message, duration) {
    return this.show(message, 'success', duration);
  },

  error(message, duration) {
    return this.show(message, 'error', duration);
  },

  info(message, duration) {
    return this.show(message, 'info', duration);
  },

  warning(message, duration) {
    return this.show(message, 'warning', duration);
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// Initialize Global State
// ═══════════════════════════════════════════════════════════════════════════

window.state = new AppState();
window.ThemeManager = ThemeManager;
window.Toast = Toast;
