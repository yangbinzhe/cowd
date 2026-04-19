/**
 * commands.js - Command handling and execution
 * 处理命令历史、自动补全和执行
 */

class CommandManager {
  constructor() {
    this.commands = new Map();
    this.history = [];
    this.historyIndex = -1;
    this.aliases = new Map();
    this.init();
  }

  /**
   * Initialize command manager
   */
  init() {
    // Register built-in commands
    this.registerBuiltInCommands();

    // Load history from localStorage
    this.loadHistory();

    // Listen for keyboard in command input
    const input = document.getElementById('commandInput');
    if (input) {
      input.addEventListener('keydown', (e) => this.handleKeyDown(e));
    }
  }

  /**
   * Register built-in commands
   */
  registerBuiltInCommands() {
    // Session commands
    this.register('new', this.cmdNewSession, {
      description: '创建新会话',
      usage: '/new [title]',
    });

    this.register('clear', this.cmdClear, {
      description: '清空当前对话',
      usage: '/clear',
    });

    this.register('sessions', this.cmdListSessions, {
      description: '列出所有会话',
      usage: '/sessions',
    });

    // Memory commands
    this.register('memory', this.cmdMemory, {
      description: '记忆管理',
      usage: '/memory [layer] [query]',
      layers: ['working', 'personal', 'project', 'global'],
    });

    this.register('remember', this.cmdRemember, {
      description: '添加到记忆',
      usage: '/remember <content>',
    });

    this.register('forget', this.cmdForget, {
      description: '从记忆中删除',
      usage: '/forget <query>',
    });

    // Config commands
    this.register('set', this.cmdSetConfig, {
      description: '设置配置项',
      usage: '/set <key> <value>',
    });

    this.register('get', this.cmdGetConfig, {
      description: '获取配置项',
      usage: '/get [key]',
    });

    this.register('theme', this.cmdTheme, {
      description: '切换主题',
      usage: '/theme [dark|light|slate]',
    });

    // Platform commands
    this.register('connect', this.cmdConnect, {
      description: '连接平台',
      usage: '/connect <platform>',
      platforms: ['feishu', 'slack', 'dingtalk'],
    });

    this.register('disconnect', this.cmdDisconnect, {
      description: '断开平台连接',
      usage: '/disconnect <platform>',
    });

    // Workspace commands
    this.register('cd', this.cmdWorkspace, {
      description: '切换工作区',
      usage: '/cd [workspace]',
    });

    this.register('ls', this.cmdListFiles, {
      description: '列出文件',
      usage: '/ls [path]',
    });

    // System commands
    this.register('help', this.cmdHelp, {
      description: '显示帮助',
      usage: '/help [command]',
    });

    this.register('history', this.cmdHistory, {
      description: '显示命令历史',
      usage: '/history',
    });

    this.register('alias', this.cmdAlias, {
      description: '设置命令别名',
      usage: '/alias <name> <command>',
    });

    this.register('export', this.cmdExport, {
      description: '导出数据',
      usage: '/export [type]',
      types: ['sessions', 'memory', 'config'],
    });

    // Register aliases
    this.registerAlias('ls', 'list');
    this.registerAlias('cd', 'workspace');
    this.registerAlias('rm', 'forget');
  }

  /**
   * Register a command
   * @param {string} name - Command name
   * @param {Function} handler - Command handler
   * @param {Object} meta - Command metadata
   */
  register(name, handler, meta = {}) {
    this.commands.set(name, {
      handler,
      description: meta.description || '',
      usage: meta.usage || `/${name}`,
      aliases: meta.aliases || [],
      ...meta,
    });
  }

  /**
   * Register command alias
   * @param {string} alias - Alias name
   * @param {string} command - Original command
   */
  registerAlias(alias, command) {
    this.aliases.set(alias, command);
  }

  /**
   * Get command
   * @param {string} name - Command name
   * @returns {Object|null} Command object
   */
  get(name) {
    // Check aliases first
    const original = this.aliases.get(name) || name;
    return this.commands.get(original);
  }

  /**
   * Execute command
   * @param {string} input - Command input
   * @returns {Promise<Object>} Execution result
   */
  async execute(input) {
    const trimmed = input.trim();
    if (!trimmed.startsWith('/')) {
      return { error: 'Commands must start with /' };
    }

    const parts = trimmed.slice(1).match(/(?:[^\s"]+|"[^"]*")+/g) || [];
    const name = parts[0].toLowerCase();
    const args = parts.slice(1).map(arg => arg.replace(/^"|"$/g, ''));

    // Resolve alias
    const command = this.get(name);
    if (!command) {
      return { error: `Unknown command: /${name}` };
    }

    // Add to history
    this.addToHistory(trimmed);

    try {
      const result = await command.handler(args);
      return { success: true, result };
    } catch (error) {
      return { error: error.message };
    }
  }

  /**
   * Handle keyboard input
   * @param {KeyboardEvent} e - Keyboard event
   */
  handleKeyDown(e) {
    const input = e.target;

    switch (e.key) {
      case 'ArrowUp':
        e.preventDefault();
        this.navigateHistory(-1, input);
        break;
      case 'ArrowDown':
        e.preventDefault();
        this.navigateHistory(1, input);
        break;
      case 'Tab':
        e.preventDefault();
        this.autocomplete(input);
        break;
      case 'Enter':
        if (!e.shiftKey) {
          e.preventDefault();
          this.runFromInput(input);
        }
        break;
    }
  }

  /**
   * Run command from input element
   * @param {HTMLInputElement} input - Input element
   */
  async runFromInput(input) {
    const value = input.value.trim();
    if (!value) return;

    input.value = '';

    const result = await this.execute(value);
    this.displayResult(result);
  }

  /**
   * Navigate command history
   * @param {number} direction - Direction (-1 up, 1 down)
   * @param {HTMLInputElement} input - Input element
   */
  navigateHistory(direction, input) {
    if (this.history.length === 0) return;

    this.historyIndex += direction;
    this.historyIndex = Math.max(0, Math.min(this.historyIndex, this.history.length - 1));

    input.value = this.history[this.historyIndex] || '';
    input.focus();
  }

  /**
   * Add command to history
   * @param {string} command - Command string
   */
  addToHistory(command) {
    // Remove duplicate
    const index = this.history.indexOf(command);
    if (index > -1) {
      this.history.splice(index, 1);
    }

    this.history.unshift(command);
    this.historyIndex = -1;

    // Limit history size
    if (this.history.length > 100) {
      this.history = this.history.slice(0, 100);
    }

    this.saveHistory();
  }

  /**
   * Save history to localStorage
   */
  saveHistory() {
    localStorage.setItem('command_history', JSON.stringify(this.history.slice(0, 50)));
  }

  /**
   * Load history from localStorage
   */
  loadHistory() {
    try {
      const saved = localStorage.getItem('command_history');
      if (saved) {
        this.history = JSON.parse(saved);
      }
    } catch (e) {
      console.error('Failed to load command history:', e);
    }
  }

  /**
   * Autocomplete command
   * @param {HTMLInputElement} input - Input element
   */
  autocomplete(input) {
    const value = input.value;
    if (!value.startsWith('/')) return;

    const partial = value.slice(1).toLowerCase();
    const matches = Array.from(this.commands.keys())
      .filter(cmd => cmd.startsWith(partial));

    if (matches.length === 1) {
      input.value = `/${matches[0]} `;
    } else if (matches.length > 1) {
      // Show suggestions
      this.showSuggestions(matches);
    }
  }

  /**
   * Show command suggestions
   * @param {Array} commands - Matching commands
   */
  showSuggestions(commands) {
    // Remove existing suggestions
    const existing = document.getElementById('commandSuggestions');
    if (existing) existing.remove();

    const container = document.createElement('div');
    container.id = 'commandSuggestions';
    container.className = 'command-suggestions';

    commands.forEach(cmd => {
      const cmdObj = this.get(cmd);
      const item = document.createElement('div');
      item.className = 'suggestion-item';
      item.innerHTML = `<code>/${cmd}</code><span>${cmdObj.description}</span>`;
      item.addEventListener('click', () => {
        document.getElementById('commandInput').value = `/${cmd} `;
        container.remove();
        document.getElementById('commandInput').focus();
      });
      container.appendChild(item);
    });

    const input = document.getElementById('commandInput');
    input.parentElement.appendChild(container);
  }

  /**
   * Display command result
   * @param {Object} result - Command result
   */
  displayResult(result) {
    if (result.error) {
      Toast.error(result.error);
      return;
    }

    if (result.result) {
      // Add result message to chat
      const message = {
        id: Date.now(),
        role: 'system',
        content: typeof result.result === 'string'
          ? result.result
          : JSON.stringify(result.result, null, 2),
        timestamp: Date.now(),
      };
      messageRenderer.addMessage(message);
    }
  }

  /**
   * Get all commands
   * @returns {Array} Command list
   */
  getAll() {
    return Array.from(this.commands.entries()).map(([name, cmd]) => ({
      name,
      ...cmd,
    }));
  }

  // ==================== Built-in Commands ====================

  async cmdNewSession(args) {
    const title = args[0] || '新会话';
    const session = await api.createSession({ title });
    appState.set('currentSession', session.id);
    await loadSessions();
    return `已创建新会话: ${title}`;
  }

  async cmdClear(args) {
    messageRenderer.clearMessages();
    return '对话已清空';
  }

  async cmdListSessions(args) {
    const sessions = await api.listSessions();
    if (sessions.length === 0) {
      return '暂无会话';
    }
    return sessions.map(s => `• ${s.title} (${s.id})`).join('\n');
  }

  async cmdMemory(args) {
    const layer = args[0] || 'all';
    const query = args.slice(1).join(' ');

    if (query) {
      const results = await api.searchMemory(query);
      return results.length > 0
        ? results.map(r => `• [${r.layer}] ${r.content}`).join('\n')
        : '未找到匹配的记忆';
    }

    if (layer === 'all') {
      const memory = await api.getMemory();
      return Object.entries(memory)
        .map(([l, items]) => `[${l}] ${items.length} 条`)
        .join('\n');
    }

    const items = await api.getMemoryLayer(layer);
    return items.map(i => `• ${i.content}`).join('\n');
  }

  async cmdRemember(args) {
    const content = args.join(' ');
    if (!content) {
      return '用法: /remember <content>';
    }
    await api.addMemory('working', { content });
    return '已添加到工作记忆';
  }

  async cmdForget(args) {
    const query = args.join(' ');
    // This would need implementation in backend
    return `搜索删除: ${query}`;
  }

  async cmdSetConfig(args) {
    const [key, ...valueParts] = args;
    const value = valueParts.join(' ');
    if (!key) {
      return '用法: /set <key> <value>';
    }
    await api.updateConfig({ [key]: value });
    return `已设置 ${key} = ${value}`;
  }

  async cmdGetConfig(args) {
    const key = args[0];
    const config = await api.getConfig();
    if (key) {
      return config[key] !== undefined ? String(config[key]) : '配置项不存在';
    }
    return JSON.stringify(config, null, 2);
  }

  async cmdTheme(args) {
    const theme = args[0] || window.ThemeManager.getTheme();
    const themes = ['dark', 'light', 'slate'];
    if (!themes.includes(theme)) {
      return `可用主题: ${themes.join(', ')}`;
    }
    window.ThemeManager.setTheme(theme);
    return `已切换到 ${theme} 主题`;
  }

  async cmdConnect(args) {
    const platform = args[0];
    if (!platform) {
      return '用法: /connect <platform>';
    }
    // Connection would be handled by platform-specific UI
    panelManager.show('platform');
    return `请在平台面板中完成 ${platform} 的连接`;
  }

  async cmdDisconnect(args) {
    const platform = args[0];
    if (!platform) {
      return '用法: /disconnect <platform>';
    }
    await api.disconnectPlatform(platform);
    return `已断开 ${platform}`;
  }

  async cmdWorkspace(args) {
    const workspace = args[0];
    if (workspace) {
      const workspaces = await api.listWorkspaces();
      const target = workspaces.find(w => w.id === workspace || w.name === workspace);
      if (target) {
        await api.setWorkspace(target.id);
        appState.set('workspace', target);
        return `已切换到工作区: ${target.name}`;
      }
      return '工作区不存在';
    }
    const current = await api.getWorkspace();
    return `当前工作区: ${current.name}`;
  }

  async cmdListFiles(args) {
    const path = args[0] || '';
    const files = await api.listFiles(path);
    if (files.length === 0) {
      return '目录为空';
    }
    return files.map(f => `${f.type === 'dir' ? '📁' : '📄'} ${f.name}`).join('\n');
  }

  async cmdHelp(args) {
    const cmdName = args[0];
    if (cmdName) {
      const cmd = this.get(cmdName);
      if (cmd) {
        return `${cmdName}\n  ${cmd.description}\n  用法: ${cmd.usage}`;
      }
      return `未知命令: ${cmdName}`;
    }

    const commands = this.getAll();
    return '可用命令:\n' + commands.map(c =>
      `  ${c.usage}\n    ${c.description}`
    ).join('\n');
  }

  async cmdHistory(args) {
    if (this.history.length === 0) {
      return '暂无历史';
    }
    return this.history.slice(0, 20).map((cmd, i) => `${i + 1}. ${cmd}`).join('\n');
  }

  async cmdAlias(args) {
    const [name, ...cmdParts] = args;
    const command = cmdParts.join(' ');
    if (!name || !command) {
      return '用法: /alias <name> <command>';
    }
    this.registerAlias(name, command);
    return `已设置别名: ${name} -> ${command}`;
  }

  async cmdExport(args) {
    const type = args[0] || 'all';
    const data = {};

    if (type === 'all' || type === 'sessions') {
      data.sessions = await api.listSessions();
    }
    if (type === 'all' || type === 'memory') {
      data.memory = await api.getMemory();
    }
    if (type === 'all' || type === 'config') {
      data.config = await api.getConfig();
    }

    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `cowd-export-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);

    return `已导出 ${type} 数据`;
  }
}

// Create global instance
const commandManager = new CommandManager();

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { CommandManager, commandManager };
}
