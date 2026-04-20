/**
 * Cowd i18n - Internationalization Module
 * 支持中文和英文
 */

const i18n = {
  'zh-CN': {
    // Navigation
    'nav.chat': '聊天',
    'nav.sessions': '会话',
    'nav.memory': '记忆',
    'nav.config': '配置',
    'nav.platform': '平台',

    // Welcome
    'welcome.title': '欢迎使用 Cowd',
    'welcome.subtitle': '我是您的 AI 编程助手，可以帮助您完成代码编写、调试、重构等任务。',

    // Chat
    'chat.placeholder': '输入消息，或使用 / 命令...',
    'chat.send': '发送',
    'chat.thinking': '思考中...',
    'chat.hint.enter': '按 Enter 发送，Shift+Enter 换行',
    'chat.hint.context': '上下文',

    // Sessions
    'sessions.recent': '最近会话',
    'sessions.new': '新会话',
    'sessions.empty': '暂无会话记录',
    'sessions.delete': '删除会话',
    'sessions.deleteConfirm': '确定要删除这个会话吗？',

    // Memory
    'memory.refresh': '刷新',
    'memory.layer0': '身份层',
    'memory.layer1': '精华层',
    'memory.layer2': '项目层',
    'memory.layer3': '深层',
    'memory.entries': '条目',
    'memory.tokens': 'Token',

    // Config
    'config.api': 'API 配置',
    'config.provider': 'Provider',
    'config.model': '模型',
    'config.theme': '主题',
    'config.theme.dark': '深色',
    'config.theme.light': '浅色',
    'config.theme.slate': 'Slate',
    'config.language': '语言',
    'config.save': '保存配置',
    'config.saved': '配置已保存',

    // Platform
    'platform.status': '状态',
    'platform.connected': '已连接',
    'platform.disconnected': '未连接',
    'platform.connect': '连接',
    'platform.disconnect': '断开',
    'platform.settings': '设置',

    // Auth
    'auth.login': '登录',
    'auth.token': '访问令牌',
    'auth.tokenPlaceholder': '输入访问令牌...',
    'auth.error': '令牌无效，请检查后重试',
    'auth.footer': '通过设置中的令牌管理生成访问令牌',

    // Common
    'common.confirm': '确认',
    'common.cancel': '取消',
    'common.save': '保存',
    'common.delete': '删除',
    'common.edit': '编辑',
    'common.close': '关闭',
    'common.loading': '加载中...',
    'common.error': '错误',
    'common.success': '成功',
    'common.retry': '重试',

    // Errors
    'error.network': '网络错误，请检查连接',
    'error.server': '服务器错误，请稍后重试',
    'error.unknown': '未知错误',
  },

  'en': {
    // Navigation
    'nav.chat': 'Chat',
    'nav.sessions': 'Sessions',
    'nav.memory': 'Memory',
    'nav.config': 'Settings',
    'nav.platform': 'Platform',

    // Welcome
    'welcome.title': 'Welcome to Cowd',
    'welcome.subtitle': 'I am your AI coding assistant. I can help you with code writing, debugging, refactoring, and more.',

    // Chat
    'chat.placeholder': 'Type a message, or use / commands...',
    'chat.send': 'Send',
    'chat.thinking': 'Thinking...',
    'chat.hint.enter': 'Press Enter to send, Shift+Enter for new line',
    'chat.hint.context': 'Context',

    // Sessions
    'sessions.recent': 'Recent Sessions',
    'sessions.new': 'New Session',
    'sessions.empty': 'No sessions yet',
    'sessions.delete': 'Delete Session',
    'sessions.deleteConfirm': 'Are you sure you want to delete this session?',

    // Memory
    'memory.refresh': 'Refresh',
    'memory.layer0': 'Identity Layer',
    'memory.layer1': 'Essential Layer',
    'memory.layer2': 'Project Layer',
    'memory.layer3': 'Deep Layer',
    'memory.entries': 'Entries',
    'memory.tokens': 'Tokens',

    // Config
    'config.api': 'API Configuration',
    'config.provider': 'Provider',
    'config.model': 'Model',
    'config.theme': 'Theme',
    'config.theme.dark': 'Dark',
    'config.theme.light': 'Light',
    'config.theme.slate': 'Slate',
    'config.language': 'Language',
    'config.save': 'Save Settings',
    'config.saved': 'Settings saved',

    // Platform
    'platform.status': 'Status',
    'platform.connected': 'Connected',
    'platform.disconnected': 'Disconnected',
    'platform.connect': 'Connect',
    'platform.disconnect': 'Disconnect',
    'platform.settings': 'Settings',

    // Auth
    'auth.login': 'Login',
    'auth.token': 'Access Token',
    'auth.tokenPlaceholder': 'Enter access token...',
    'auth.error': 'Invalid token, please check and try again',
    'auth.footer': 'Generate access token in Settings',

    // Common
    'common.confirm': 'Confirm',
    'common.cancel': 'Cancel',
    'common.save': 'Save',
    'common.delete': 'Delete',
    'common.edit': 'Edit',
    'common.close': 'Close',
    'common.loading': 'Loading...',
    'common.error': 'Error',
    'common.success': 'Success',
    'common.retry': 'Retry',

    // Errors
    'error.network': 'Network error, please check your connection',
    'error.server': 'Server error, please try again later',
    'error.unknown': 'Unknown error',
  }
};

/**
 * Get current language
 */
function getCurrentLang() {
  return localStorage.getItem('cowd-lang') || 'zh-CN';
}

/**
 * Set language
 */
function setLanguage(lang) {
  localStorage.setItem('cowd-lang', lang);
  document.documentElement.lang = lang;
  updateAllTranslations();
}

/**
 * Translate a key
 */
function t(key) {
  const lang = getCurrentLang();
  return i18n[lang]?.[key] || key;
}

/**
 * Update all elements with data-i18n attribute
 */
function updateAllTranslations() {
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    el.textContent = t(key);
  });

  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    const key = el.getAttribute('data-i18n-placeholder');
    el.placeholder = t(key);
  });

  document.querySelectorAll('[data-i18n-title]').forEach(el => {
    const key = el.getAttribute('data-i18n-title');
    el.title = t(key);
  });
}

// I18n Instance
class I18nInstance {
  constructor() {
    this.currentLocale = 'zh-CN';
  }

  setLocale(locale) {
    this.currentLocale = locale;
    updateAllTranslations();
  }

  t(key) {
    return t(key);
  }

  updateAll() {
    updateAllTranslations();
  }
}

// Export for use in other modules
window.i18n = {
  t,
  getCurrentLang,
  setLanguage,
  updateAllTranslations
};

// Create global instance
window.i18nInstance = new I18nInstance();
