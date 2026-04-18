/**
 * cc-webui -- ui.js
 * DOM 工具、Markdown 渲染、主题、Toast、设置面板。
 */

// ── Markdown 渲染器 ────────────────────────────────────────────────────────

const MD = {
  render(text) {
    if (!text) return '';
    return this._fullRender(text);
  },

  _fullRender(text) {
    // 提取代码块 — 最先处理
    const blocks = [];
    let s = text.replace(/```(\w*)\n?([\s\S]*?)```/g, (_, lang, code) => {
      const idx = blocks.length;
      const escaped = code
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      const lbl = lang || 'text';
      blocks.push(
        `<pre><div class="code-header"><span>${this._escHtml(lbl)}</span>` +
        `<button class="copy-code-btn" onclick="copyCode(this)">复制</button></div>` +
        `<code class="lang-${this._escHtml(lbl)}">${escaped}</code></pre>`
      );
      return `\x00B${idx}\x00`;
    });

    // 提取行内代码
    const inlines = [];
    s = s.replace(/`([^`\n]+)`/g, (_, c) => {
      const idx = inlines.length;
      inlines.push(`<code>${this._escHtml(c)}</code>`);
      return `\x00I${idx}\x00`;
    });

    // 转义剩余 HTML
    s = s.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    s = s.replace(/&(?!amp;|lt;|gt;|quot;|#\d+;)/g, '&amp;');

    // 表格
    s = this._renderTables(s);

    // 水平线
    s = s.replace(/^-{3,}$/gm, '<hr>');
    s = s.replace(/^\*{3,}$/gm, '<hr>');

    // 标题
    s = s.replace(/^######\s+(.+)$/gm, '<h6>$1</h6>');
    s = s.replace(/^#####\s+(.+)$/gm, '<h5>$1</h5>');
    s = s.replace(/^####\s+(.+)$/gm, '<h4>$1</h4>');
    s = s.replace(/^###\s+(.+)$/gm, '<h3>$1</h3>');
    s = s.replace(/^##\s+(.+)$/gm, '<h2>$1</h2>');
    s = s.replace(/^#\s+(.+)$/gm, '<h1>$1</h1>');

    // 引用块
    s = s.replace(/^&gt;\s?(.+)$/gm, '<blockquote>$1</blockquote>');

    // 粗体+斜体
    s = s.replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>');
    s = s.replace(/___(.+?)___/g, '<strong><em>$1</em></strong>');
    s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/__(.+?)__/g, '<strong>$1</strong>');
    s = s.replace(/\*(?!\s)(.+?)(?<!\s)\*/g, '<em>$1</em>');
    s = s.replace(/_(?!\s)(.+?)(?<!\s)_/g, '<em>$1</em>');
    s = s.replace(/~~(.+?)~~/g, '<del>$1</del>');

    // 链接
    s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g,
      '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');

    // 无序列表
    s = s.replace(/^(\s*)[*\-+]\s+(.+)$/gm, (_, indent, content) => {
      const depth = Math.floor(indent.length / 2);
      return `<li data-depth="${depth}">${content}</li>`;
    });
    s = this._wrapLists(s, 'ul');

    // 有序列表
    s = s.replace(/^(\s*)\d+\.\s+(.+)$/gm, (_, indent, content) => {
      const depth = Math.floor(indent.length / 2);
      return `<li data-depth="${depth}" data-ol="1">${content}</li>`;
    });
    s = this._wrapLists(s, 'ol');

    // 段落：双换行
    s = s.replace(/\n{2,}/g, '\n\n');
    const parts = s.split(/\n\n+/);
    s = parts.map(p => {
      p = p.trim();
      if (!p) return '';
      if (/^<(h[1-6]|ul|ol|li|pre|blockquote|hr|table)/.test(p)) return p;
      return `<p>${p.replace(/\n/g, '<br>')}</p>`;
    }).join('\n');

    // 还原代码块和行内代码
    s = s.replace(/\x00B(\d+)\x00/g, (_, i) => blocks[+i]);
    s = s.replace(/\x00I(\d+)\x00/g, (_, i) => inlines[+i]);

    return s;
  },

  _escHtml(t) {
    return t.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
  },

  _renderTables(s) {
    return s.replace(
      /^\|(.+)\|\n\|[-| :]+\|\n((?:\|.+\|\n?)+)/gm,
      (_, header, body) => {
        const ths = header.split('|').filter(c => c.trim())
          .map(c => `<th>${c.trim()}</th>`).join('');
        const rows = body.trim().split('\n').map(row => {
          const tds = row.split('|').filter(c => c.trim())
            .map(c => `<td>${c.trim()}</td>`).join('');
          return `<tr>${tds}</tr>`;
        }).join('');
        return `<table><thead><tr>${ths}</tr></thead><tbody>${rows}</tbody></table>`;
      }
    );
  },

  _wrapLists(s, tag) {
    return s.replace(/((?:<li[^>]*>.*<\/li>\n?)+)/g, m => `<${tag}>${m}</${tag}>`);
  },
};


// ── 复制代码 ───────────────────────────────────────────────────────────────

function copyCode(btn) {
  const code = btn.closest('pre').querySelector('code');
  navigator.clipboard.writeText(code.textContent || code.innerText).then(() => {
    btn.textContent = '已复制!';
    setTimeout(() => { btn.textContent = '复制'; }, 2000);
  }).catch(() => showToast('复制失败'));
}


// ── Toast ───────────────────────────────────────────────────────────────────

let _toastTimer = null;
function showToast(msg, duration = 2500) {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = msg;
  el.classList.add('visible');
  clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => el.classList.remove('visible'), duration);
}


// ── 主题 ────────────────────────────────────────────────────────────────────

function applyTheme(t) {
  document.documentElement.dataset.theme = t || 'dark';
  localStorage.setItem('cc-theme', t || 'dark');
  // 同步所有主题选择器
  ['settingsTheme', 'overlaySettingsTheme'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = t || 'dark';
  });
}


// ── 设置浮层 ────────────────────────────────────────────────────────────────

function toggleSettings() {
  const el = document.getElementById('settingsOverlay');
  if (!el) return;
  const open = el.style.display !== 'none';
  el.style.display = open ? 'none' : 'flex';
  if (!open) {
    loadSettingsIntoPanel();
  }
}

function loadSettingsIntoPanel() {
  const settings = window.S?.settings || {};

  // 主题
  const themeEl = document.getElementById('overlaySettingsTheme');
  if (themeEl) themeEl.value = localStorage.getItem('cc-theme') || 'dark';

  // 发送快捷键
  const sendKeyEl = document.getElementById('overlaySettingsSendKey');
  if (sendKeyEl) sendKeyEl.value = settings.send_key || 'enter';

  // 网关地址
  const gwEl = document.getElementById('settingsGatewayUrlOverlay');
  if (gwEl) gwEl.value = settings.gateway_url || '';

  // 模型选择
  const modelEl = document.getElementById('settingsModel');
  if (modelEl) {
    const mainSel = document.getElementById('modelSelect');
    if (mainSel && mainSel.innerHTML) modelEl.innerHTML = mainSel.innerHTML;
    modelEl.value = settings.model || settings.default_model || window.S?.selectedModel || '';
  }
}

function saveSettings() {
  const settings = {};

  const themeEl = document.getElementById('overlaySettingsTheme') || document.getElementById('settingsTheme');
  if (themeEl) { settings.theme = themeEl.value; applyTheme(themeEl.value); }

  const sendKeyEl = document.getElementById('overlaySettingsSendKey') || document.getElementById('settingsSendKey');
  if (sendKeyEl) {
    settings.send_key = sendKeyEl.value;
    window._sendKey = sendKeyEl.value;
  }

  const modelEl = document.getElementById('settingsModel');
  if (modelEl && modelEl.value) {
    settings.model = modelEl.value;
    settings.default_model = modelEl.value;
    onModelChange(modelEl.value);
  }

  const gwEl = document.getElementById('settingsGatewayUrlOverlay') || document.getElementById('settingsGatewayUrl');
  if (gwEl && gwEl.value) settings.gateway_url = gwEl.value;

  fetch('/api/settings', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(settings),
  }).then(() => {
    if (window.S) window.S.settings = { ...window.S.settings, ...settings };
    showToast('设置已保存');
    // 关闭浮层
    const overlay = document.getElementById('settingsOverlay');
    if (overlay) overlay.style.display = 'none';
  }).catch(() => showToast('保存失败'));
}


// ── 模型 chip ───────────────────────────────────────────────────────────────

function updateModelChip(modelId) {
  const chip = document.getElementById('modelChip');
  if (!chip) return;
  const sel = document.getElementById('modelSelect');
  if (sel) {
    const opt = sel.querySelector(`option[value="${modelId}"]`);
    chip.textContent = opt ? opt.textContent : modelId;
  } else {
    chip.textContent = modelId;
  }
}

function onModelChange(value) {
  if (!value) return;
  if (window.S) window.S.selectedModel = value;
  updateModelChip(value);
  // 同步所有模型选择器
  ['modelSelect', 'topbarModelSelect', 'rightPanelModel', 'settingsModel'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = value;
  });
}


// ── 信息面板（兼容旧代码） ───────────────────────────────────────────────────

function toggleInfoPanel() {
  const panel = document.getElementById('infopanel');
  if (panel) panel.style.display = panel.style.display === 'none' ? '' : 'none';
}

function updateInfoPanel() {
  const S = window.S;
  if (!S) return;

  const modelEl = document.getElementById('infoModel');
  if (modelEl) modelEl.textContent = S.selectedModel || '—';

  const sessionEl = document.getElementById('infoSession');
  if (sessionEl) sessionEl.textContent = S.session ? S.session.id.slice(0, 8) + '…' : '无会话';

  const countEl = document.getElementById('infoMsgCount');
  if (countEl) countEl.textContent = (S.session?.messages || []).length;
}

function setGatewayStatus(online) {
  const dot = document.getElementById('gatewayDot');
  const text = document.getElementById('gatewayText');
  if (dot) dot.className = 'status-dot ' + (online ? 'online' : 'offline');
  if (text) text.textContent = online ? '在线' : '离线（使用 CLI 回退）';
}


// ── 移动端侧边栏 ────────────────────────────────────────────────────────────

function toggleMobileSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('mobileOverlay');
  if (!sidebar) return;
  sidebar.classList.toggle('open');
  if (overlay) overlay.classList.toggle('visible');
}

function closeMobileSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('mobileOverlay');
  if (sidebar) sidebar.classList.remove('open');
  if (overlay) overlay.classList.remove('visible');
}


// ── 快捷提示处理 ────────────────────────────────────────────────────────────

function usesuggestion(btn) {
  const msgEl = document.getElementById('msg');
  if (msgEl) {
    msgEl.value = btn.textContent.trim();
    msgEl.focus();
    autoResize(msgEl);
  }
}


// ── 输入框自适应高度 ────────────────────────────────────────────────────────

function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 200) + 'px';
}


// ── 滚动到底部 ──────────────────────────────────────────────────────────────

function scrollToBottom(smooth = true) {
  const el = document.getElementById('messages');
  if (!el) return;
  el.scrollTo({ top: el.scrollHeight, behavior: smooth ? 'smooth' : 'instant' });
}
