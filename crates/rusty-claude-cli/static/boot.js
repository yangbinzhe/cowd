/**
 * cc-webui -- boot.js
 * 应用初始化、配置加载、全局状态建立。
 */

// ── 全局状态 ────────────────────────────────────────────────────────────────

window.S = {
  session: null,
  sessions: [],
  _allSessions: [],
  selectedModel: 'claude-3-5-sonnet-20241022',
  settings: {},
  config: {},
};

// ── 启动流程 ────────────────────────────────────────────────────────────────

async function boot() {
  try {
    // 1. 加载配置
    const configResp = await fetch('/api/config');
    const config = await configResp.json();
    window.S.config = config;
    window.S.settings = config.settings || {};

    // 恢复已保存的主题
    const savedTheme = localStorage.getItem('cc-theme');
    if (savedTheme) applyTheme(savedTheme);

    // 2. 设置默认模型
    const defaultModel = config.settings?.model
      || config.settings?.default_model
      || config.default_model
      || 'claude-3-5-sonnet-20241022';
    window.S.selectedModel = defaultModel;

    // 3. 填充所有模型选择器
    if (config.models && config.models.length) {
      _populateAllModelSelects(config.models, defaultModel);
    } else {
      // 尝试从 /api/models 单独获取
      try {
        const modResp = await fetch('/api/models');
        const modData = await modResp.json();
        const models = modData.models || [];
        if (models.length) {
          _populateAllModelSelects(models, modData.default || defaultModel);
          window.S.selectedModel = modData.default || defaultModel;
        }
      } catch (_) {}
    }
    updateModelChip(window.S.selectedModel);

    // 4. 加载命令列表（用于自动完成）
    try {
      const cmdResp = await fetch('/api/commands');
      const cmdData = await cmdResp.json();
      window.S.commands = cmdData.commands || [];
    } catch (_) {
      window.S.commands = [];
    }

    // 5. 填充右侧面板设置初始值
    _applySettingsToRightPanel(window.S.settings);

    // 6. 初始化右侧面板 Tabs（panels.js）
    if (typeof Panels !== 'undefined' && Panels.initPanelTabs) {
      Panels.initPanelTabs();
    }

    // 7. 初始化文件浏览器（workspace.js）
    if (typeof Workspace !== 'undefined') {
      Workspace.init();
      Workspace.loadDirectory('');
    }

    // 8. 检查网关状态
    checkGatewayStatus();

    // 9. 加载会话列表
    await loadSessions();

    // 自动选中最近会话或新建
    const sessions = window.S._allSessions;
    if (sessions.length > 0) {
      await switchSession(sessions[0].id);
    } else {
      await newSession();
    }

    // 10. 初始化输入框事件（messages.js 中的 initComposer）
    initComposer();

    // 11. 恢复右侧面板折叠状态
    _restoreRightPanelState();

    // 12. 全局快捷键
    document.addEventListener('keydown', e => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        newSession();
      }
      // Esc 关闭设置浮层
      if (e.key === 'Escape') {
        const overlay = document.getElementById('settingsOverlay');
        if (overlay && overlay.style.display !== 'none') {
          overlay.style.display = 'none';
        }
      }
    });

    // 13. 定时检查网关状态
    setInterval(checkGatewayStatus, 30000);

    console.log('cc-webui 初始化完成');

  } catch (e) {
    console.error('Boot 失败:', e);
    // 降级：仍然初始化输入框
    initComposer();
    loadSessions();
  }
}


// ── 填充所有模型选择器 ───────────────────────────────────────────────────────

function _populateAllModelSelects(models, selectedModel) {
  const selIds = ['modelSelect', 'topbarModelSelect', 'rightPanelModel', 'settingsModel'];
  selIds.forEach(id => {
    const sel = document.getElementById(id);
    if (sel) _fillModelSelect(sel, models, selectedModel);
  });
}

function _fillModelSelect(sel, models, selectedModel) {
  // 按 provider 分组
  const groups = {};
  models.forEach(m => {
    const prov = m.provider || m.provider_name || 'other';
    if (!groups[prov]) groups[prov] = [];
    groups[prov].push(m);
  });

  sel.innerHTML = '';
  for (const [prov, mods] of Object.entries(groups)) {
    const og = document.createElement('optgroup');
    og.label = prov.charAt(0).toUpperCase() + prov.slice(1);
    mods.forEach(m => {
      const opt = document.createElement('option');
      opt.value = m.id;
      opt.textContent = m.display || m.name || m.id;
      if (m.id === selectedModel) opt.selected = true;
      og.appendChild(opt);
    });
    sel.appendChild(og);
  }

  if (selectedModel) sel.value = selectedModel;
}


// ── 网关状态检查 ──────────────────────────────────────────────────────────────

async function checkGatewayStatus() {
  try {
    const r = await fetch('/api/health', { cache: 'no-store' });
    const data = await r.json();
    setGatewayStatus(data.status === 'ok');
  } catch (e) {
    setGatewayStatus(false);
  }
}


// ── 右侧面板折叠/展开 ──────────────────────────────────────────────────────────

function toggleRightPanel() {
  const panel = document.getElementById('rightPanel');
  if (!panel) return;
  const collapsed = panel.classList.toggle('collapsed');
  localStorage.setItem('cc-right-panel-collapsed', collapsed ? '1' : '0');
}

function _restoreRightPanelState() {
  if (localStorage.getItem('cc-right-panel-collapsed') === '1') {
    const panel = document.getElementById('rightPanel');
    if (panel) panel.classList.add('collapsed');
  }
}


// ── Tab 切换（右侧面板）──────────────────────────────────────────────────────

function switchRightTab(tabName) {
  // 更新 tab 按钮激活状态（index.html 中按钮用 .tab-btn）
  document.querySelectorAll('.tab-btn[data-tab]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tab === tabName);
  });

  // 切换 tab 内容可见性
  const tabMap = {
    files:    'tabFiles',
    skills:   'tabSkills',
    memory:   'tabMemory',
    settings: 'tabSettings',
  };
  Object.entries(tabMap).forEach(([name, id]) => {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('active', name === tabName);
  });

  // 懒加载内容
  if (tabName === 'skills') {
    if (typeof Panels !== 'undefined') Panels.loadSkills();
  } else if (tabName === 'memory') {
    if (typeof Panels !== 'undefined') Panels.loadMemoryStatus();
  } else if (tabName === 'files') {
    if (typeof Workspace !== 'undefined') {
      Workspace.init();
      Workspace.loadDirectory('');
    }
  }
}


// ── 文件浏览器（index.html 中的 inline 调用） ────────────────────────────────

function loadFileTree() {
  if (typeof Workspace !== 'undefined') {
    Workspace.refresh();
  }
}

function navigateUp() {
  // Workspace 模块内部处理，这里仅作兼容调用
  if (typeof Workspace !== 'undefined') {
    Workspace.loadDirectory('');
  }
}

function closeFilePreview() {
  if (typeof Workspace !== 'undefined') {
    Workspace.closePreview();
  }
}


// ── 记忆面板刷新 ───────────────────────────────────────────────────────────

function refreshMemoryStatus() {
  if (typeof Panels !== 'undefined') {
    Panels.loadMemoryStatus(true);
  }
}


// ── 命令自动完成（与 index.html 中的 #cmdDropdown 配合）────────────────────

function getMatchingCommands(prefix) {
  const commands = window.S.commands || [];
  if (!prefix) return commands.slice(0, 10);
  const q = prefix.toLowerCase();
  return commands.filter(c =>
    c.name.toLowerCase().includes(q) ||
    (c.description && c.description.toLowerCase().includes(q))
  );
}

function showCommandDropdown(matches) {
  const dd = document.getElementById('cmdDropdown');
  if (!dd) return;

  dd.innerHTML = matches.map((c, i) => `
    <div class="cmd-item${i === 0 ? ' selected' : ''}" data-cmd="${_escAttr(c.name)}" onclick="selectCommand('${_escAttr(c.name)}')">
      <span class="cmd-name">${_escHtml(c.name)}</span>
      <span class="cmd-desc">${_escHtml(c.description || '')}</span>
    </div>
  `).join('');
  dd.classList.add('open');
}

function hideCommandDropdown() {
  const dd = document.getElementById('cmdDropdown');
  if (dd) {
    dd.classList.remove('open');
    dd.innerHTML = '';
  }
}

function selectCommand(cmdName) {
  const msgEl = document.getElementById('msg');
  if (msgEl) {
    msgEl.value = cmdName + ' ';
    msgEl.focus();
    autoResize(msgEl);
  }
  hideCommandDropdown();
}

function selectCommandDropdownItem() {
  const dd = document.getElementById('cmdDropdown');
  if (!dd) return;
  const selected = dd.querySelector('.cmd-item.selected');
  if (selected) {
    const cmd = selected.dataset.cmd;
    if (cmd) selectCommand(cmd);
  }
}

function navigateCommandDropdown(dir) {
  const dd = document.getElementById('cmdDropdown');
  if (!dd) return;
  const items = Array.from(dd.querySelectorAll('.cmd-item'));
  if (!items.length) return;
  const curIdx = items.findIndex(i => i.classList.contains('selected'));
  let nextIdx = curIdx + dir;
  if (nextIdx < 0) nextIdx = items.length - 1;
  if (nextIdx >= items.length) nextIdx = 0;
  items.forEach((i, idx) => i.classList.toggle('selected', idx === nextIdx));
  items[nextIdx].scrollIntoView({ block: 'nearest' });
}


// ── 本地命令处理 ──────────────────────────────────────────────────────────────

function executeLocalCommand(text) {
  const parts = text.trim().split(/\s+/);
  const cmd = parts[0].toLowerCase();
  const arg = parts.slice(1).join(' ');

  switch (cmd) {
    case '/new':
      newSession();
      return true;
    case '/clear':
      if (window.S.session) {
        const inner = document.getElementById('msgInner');
        if (inner) inner.innerHTML = '';
        const empty = document.getElementById('emptyState');
        if (empty) empty.style.display = 'flex';
        showToast('已清除对话显示');
      }
      return true;
    case '/model':
      if (arg) {
        onModelChange(arg);
        showToast('模型已切换: ' + arg);
      }
      return true;
    case '/help':
      _showHelpMessage();
      return true;
    case '/temperature':
    case '/temp': {
      const v = parseFloat(arg);
      if (!isNaN(v) && v >= 0 && v <= 2) {
        const sliders = ['paramTemp', 'settingsTemp'];
        sliders.forEach(id => {
          const el = document.getElementById(id);
          if (el) el.value = v;
        });
        const tempVal = document.getElementById('tempVal');
        if (tempVal) tempVal.textContent = v;
        const settingsTempVal = document.getElementById('settingsTempVal');
        if (settingsTempVal) settingsTempVal.textContent = v;
        showToast('Temperature 设为 ' + v);
      }
      return true;
    }
    case '/tokens': {
      const v = parseInt(arg);
      if (!isNaN(v) && v > 0) {
        const els = ['paramMaxTokens', 'settingsMaxTokens'];
        els.forEach(id => {
          const el = document.getElementById(id);
          if (el) el.value = v;
        });
        showToast('Max tokens 设为 ' + v);
      }
      return true;
    }
    case '/system':
      if (arg) {
        const el = document.getElementById('settingsSystemPrompt');
        if (el) { el.value = arg; showToast('系统提示词已更新'); }
      }
      return true;
    case '/status':
      _showStatusMessage();
      return true;
    default:
      return false;
  }
}

function _showHelpMessage() {
  const commands = window.S.commands || [];
  const lines = ['## 可用命令\n'];
  commands.forEach(c => {
    lines.push(`**${c.name}**${c.args ? ' `' + c.args + '`' : ''} — ${c.description || ''}`);
  });
  const inner = document.getElementById('msgInner');
  if (!inner) return;
  const empty = document.getElementById('emptyState');
  if (empty) empty.style.display = 'none';
  const row = document.createElement('div');
  row.className = 'msg-row assistant';
  const bubble = document.createElement('div');
  bubble.className = 'msg-bubble';
  bubble.innerHTML = MD.render(lines.join('\n'));
  row.appendChild(bubble);
  inner.appendChild(row);
  scrollToBottom();
}

function _showStatusMessage() {
  const S = window.S;
  const cfg = S.config || {};
  const lines = [
    '## 系统状态\n',
    `**模型:** ${S.selectedModel || '—'}`,
    `**网关:** ${cfg.gateway_url || '—'}`,
    `**工作目录:** ${cfg.workspace || '—'}`,
    `**记忆系统:** ${cfg.memory_enabled ? '已启用' : '未启用'}`,
    `**当前会话:** ${S.session ? S.session.id.slice(0, 8) + '…' : '无'}`,
    `**消息数:** ${(S.session?.messages || []).length}`,
  ];
  const inner = document.getElementById('msgInner');
  if (!inner) return;
  const empty = document.getElementById('emptyState');
  if (empty) empty.style.display = 'none';
  const row = document.createElement('div');
  row.className = 'msg-row assistant';
  const bubble = document.createElement('div');
  bubble.className = 'msg-bubble';
  bubble.innerHTML = MD.render(lines.join('\n'));
  row.appendChild(bubble);
  inner.appendChild(row);
  scrollToBottom();
}


// ── 右侧面板设置（保存） ──────────────────────────────────────────────────────

function saveRightPanelSettings() {
  const settings = {};

  const modelEl = document.getElementById('rightPanelModel');
  if (modelEl && modelEl.value) {
    settings.model = modelEl.value;
    onModelChange(modelEl.value);
  }

  const tempEl = document.getElementById('settingsTemp');
  if (tempEl) settings.temperature = parseFloat(tempEl.value);

  const maxTokensEl = document.getElementById('settingsMaxTokens');
  if (maxTokensEl) settings.maxTokens = parseInt(maxTokensEl.value);

  const topPEl = document.getElementById('settingsTopP');
  if (topPEl) settings.topP = parseFloat(topPEl.value);

  const sysPromptEl = document.getElementById('settingsSystemPrompt');
  if (sysPromptEl) settings.systemPrompt = sysPromptEl.value;

  const themeEl = document.getElementById('settingsTheme');
  if (themeEl) { settings.theme = themeEl.value; applyTheme(themeEl.value); }

  const sendKeyEl = document.getElementById('settingsSendKey');
  if (sendKeyEl) {
    settings.send_key = sendKeyEl.value;
    window._sendKey = sendKeyEl.value;
  }

  const gwEl = document.getElementById('settingsGatewayUrl');
  if (gwEl && gwEl.value) settings.gateway_url = gwEl.value;

  fetch('/api/settings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(settings),
  }).then(r => r.json()).then(() => {
    if (window.S) window.S.settings = { ...window.S.settings, ...settings };
    showToast('设置已保存');
  }).catch(() => showToast('保存失败'));
}


// ── 将设置值填入右侧面板 ────────────────────────────────────────────────────

function _applySettingsToRightPanel(settings) {
  if (!settings) return;

  const tempEl = document.getElementById('settingsTemp');
  const tempValEl = document.getElementById('settingsTempVal');
  if (tempEl && settings.temperature != null) {
    tempEl.value = settings.temperature;
    if (tempValEl) tempValEl.textContent = settings.temperature;
  }

  const maxTokensEl = document.getElementById('settingsMaxTokens');
  if (maxTokensEl && settings.maxTokens != null) {
    maxTokensEl.value = settings.maxTokens;
  }

  const topPEl = document.getElementById('settingsTopP');
  const topPValEl = document.getElementById('settingsTopPVal');
  if (topPEl && settings.topP != null) {
    topPEl.value = settings.topP;
    if (topPValEl) topPValEl.textContent = settings.topP;
  }

  const sysPromptEl = document.getElementById('settingsSystemPrompt');
  if (sysPromptEl && settings.systemPrompt) {
    sysPromptEl.value = settings.systemPrompt;
  }

  const themeEl = document.getElementById('settingsTheme');
  if (themeEl) {
    themeEl.value = localStorage.getItem('cc-theme') || 'dark';
  }

  const sendKeyEl = document.getElementById('settingsSendKey');
  if (sendKeyEl && settings.send_key) {
    sendKeyEl.value = settings.send_key;
    window._sendKey = settings.send_key;
  }

  const gwEl = document.getElementById('settingsGatewayUrl');
  if (gwEl && settings.gateway_url) {
    gwEl.value = settings.gateway_url;
  }
}


// ── 工具函数 ──────────────────────────────────────────────────────────────────

function _escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function _escAttr(s) {
  return String(s).replace(/'/g, '&#39;').replace(/"/g, '&quot;');
}


// ── 启动 ───────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', boot);
