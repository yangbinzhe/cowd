/**
 * cc-webui -- workspace.js
 * 文件浏览器：目录树、文件预览、面包屑导航、搜索过滤。
 * 全局命名空间: window.Workspace
 */

window.Workspace = (() => {

  // ── 内部状态 ───────────────────────────────────────────────────────────────

  let _currentPath = '';          // 当前浏览目录的绝对路径
  let _rootPath = '';             // 工作区根目录（首次加载时记录）
  let _expandedDirs = new Set();  // 已展开的目录集合（绝对路径）
  let _dirCache = {};             // 目录内容缓存 { path: entries[] }
  let _searchQuery = '';          // 当前搜索关键词
  let _previewPath = '';          // 当前预览文件路径
  let _initialized = false;

  // 图片扩展名
  const IMAGE_EXTS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico', '.bmp']);
  // Markdown 扩展名
  const MD_EXTS = new Set(['.md', '.markdown', '.mdown']);
  // 敏感文件（不可预览，仅显示名称）
  const SENSITIVE_FILES = new Set(['.env', '.env.local', '.env.production', '.netrc', '.htpasswd']);

  // ── localStorage 展开状态持久化 ──────────────────────────────────────────

  function _saveExpanded() {
    try {
      const key = 'cc-webui-expanded:' + _rootPath;
      localStorage.setItem(key, JSON.stringify([..._expandedDirs]));
    } catch (_) {}
  }

  function _restoreExpanded() {
    try {
      const key = 'cc-webui-expanded:' + _rootPath;
      const raw = localStorage.getItem(key);
      _expandedDirs = raw ? new Set(JSON.parse(raw)) : new Set();
    } catch (_) {
      _expandedDirs = new Set();
    }
  }

  // ── 文件扩展名工具 ──────────────────────────────────────────────────────

  function _ext(name) {
    const i = name.lastIndexOf('.');
    return i >= 0 ? name.slice(i).toLowerCase() : '';
  }

  function _isSensitive(name) {
    return SENSITIVE_FILES.has(name) || name.endsWith('.pem') || name.endsWith('.key');
  }

  // ── API 调用 ─────────────────────────────────────────────────────────────

  async function _apiDir(path) {
    const url = '/api/workspace' + (path ? '?path=' + encodeURIComponent(path) : '');
    const r = await fetch(url);
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      throw new Error(d.error || `HTTP ${r.status}`);
    }
    return r.json();
  }

  async function _apiFile(path) {
    const r = await fetch('/api/workspace/file?path=' + encodeURIComponent(path));
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      throw new Error(d.error || `HTTP ${r.status}`);
    }
    return r.json();
  }

  // ── 加载目录 ─────────────────────────────────────────────────────────────

  async function loadDirectory(path) {
    const container = document.getElementById('wsTreeContainer');
    if (!container) return;

    // 首次加载：记录根目录并恢复展开状态
    if (!_rootPath && !path) {
      try {
        const data = await _apiDir('');
        _rootPath = data.path;
        _currentPath = data.path;
        _restoreExpanded();
        _dirCache[data.path] = data.entries || [];
        _renderTree(container);
        _renderBreadcrumb(data.path);
        return;
      } catch (e) {
        container.innerHTML = `<div class="ws-error">加载失败：${_esc(e.message)}</div>`;
        return;
      }
    }

    const targetPath = path || _rootPath;
    _currentPath = targetPath;

    try {
      const data = await _apiDir(targetPath);
      _dirCache[targetPath] = data.entries || [];
      if (!_rootPath) {
        _rootPath = data.path;
        _restoreExpanded();
      }
      _renderTree(container);
      _renderBreadcrumb(targetPath);
    } catch (e) {
      container.innerHTML = `<div class="ws-error">加载失败：${_esc(e.message)}</div>`;
    }
  }

  // ── 渲染目录树 ───────────────────────────────────────────────────────────

  function _renderTree(container) {
    const entries = _dirCache[_currentPath] || [];
    const filtered = _filterEntries(entries);

    if (!filtered.length) {
      container.innerHTML = '<div class="ws-empty">此目录为空</div>';
      return;
    }

    container.innerHTML = '';
    const ul = document.createElement('ul');
    ul.className = 'ws-tree';
    _buildTree(ul, _currentPath, filtered, 0);
    container.appendChild(ul);
  }

  function _filterEntries(entries) {
    if (!_searchQuery) return entries;
    const q = _searchQuery.toLowerCase();
    return entries.filter(e => e.name.toLowerCase().includes(q));
  }

  function _buildTree(ul, parentPath, entries, depth) {
    for (const entry of entries) {
      const fullPath = parentPath + '/' + entry.name;
      const li = document.createElement('li');
      li.className = 'ws-item ws-' + entry.type;
      li.dataset.path = fullPath;
      li.dataset.type = entry.type;

      const row = document.createElement('div');
      row.className = 'ws-row';
      row.style.paddingLeft = (depth * 16 + 8) + 'px';

      if (entry.type === 'dir') {
        const expanded = _expandedDirs.has(fullPath);
        const arrow = document.createElement('span');
        arrow.className = 'ws-arrow' + (expanded ? ' open' : '');
        arrow.innerHTML = '&#9654;'; // ▶
        row.appendChild(arrow);

        const icon = document.createElement('span');
        icon.className = 'ws-icon';
        icon.textContent = expanded ? '📂' : '📁';
        row.appendChild(icon);

        const name = document.createElement('span');
        name.className = 'ws-name';
        name.textContent = entry.name;
        row.appendChild(name);

        row.onclick = () => _toggleDir(li, fullPath, depth);
        li.appendChild(row);

        // 如果已展开，渲染子节点
        if (expanded && _dirCache[fullPath]) {
          const childUl = document.createElement('ul');
          childUl.className = 'ws-subtree';
          _buildTree(childUl, fullPath, _dirCache[fullPath], depth + 1);
          li.appendChild(childUl);
        } else if (expanded) {
          // 懒加载子目录
          const childUl = document.createElement('ul');
          childUl.className = 'ws-subtree';
          childUl.innerHTML = '<li class="ws-loading">加载中…</li>';
          li.appendChild(childUl);
          _loadSubDir(fullPath, childUl, depth + 1);
        }
      } else {
        // 文件节点
        const spacer = document.createElement('span');
        spacer.className = 'ws-arrow-spacer';
        row.appendChild(spacer);

        const icon = document.createElement('span');
        icon.className = 'ws-icon';
        icon.textContent = _fileIcon(entry.name);
        row.appendChild(icon);

        const name = document.createElement('span');
        name.className = 'ws-name';
        name.textContent = entry.name;
        row.appendChild(name);

        if (entry.size != null) {
          const size = document.createElement('span');
          size.className = 'ws-size';
          size.textContent = _humanSize(entry.size);
          row.appendChild(size);
        }

        if (!_isSensitive(entry.name)) {
          row.onclick = () => previewFile(fullPath);
        } else {
          row.title = '敏感文件，不可预览';
          row.style.opacity = '0.5';
          row.style.cursor = 'not-allowed';
        }
        li.appendChild(row);
      }

      ul.appendChild(li);
    }
  }

  async function _toggleDir(li, path, depth) {
    const arrow = li.querySelector('.ws-arrow');
    const icon = li.querySelector('.ws-icon');

    if (_expandedDirs.has(path)) {
      // 折叠
      _expandedDirs.delete(path);
      _saveExpanded();
      if (arrow) { arrow.classList.remove('open'); }
      if (icon) icon.textContent = '📁';
      const sub = li.querySelector('.ws-subtree');
      if (sub) sub.remove();
    } else {
      // 展开
      _expandedDirs.add(path);
      _saveExpanded();
      if (arrow) { arrow.classList.add('open'); }
      if (icon) icon.textContent = '📂';

      const childUl = document.createElement('ul');
      childUl.className = 'ws-subtree';

      if (_dirCache[path]) {
        _buildTree(childUl, path, _dirCache[path], depth + 1);
      } else {
        childUl.innerHTML = '<li class="ws-loading">加载中…</li>';
        li.appendChild(childUl);
        await _loadSubDir(path, childUl, depth + 1);
        return;
      }
      li.appendChild(childUl);
    }
  }

  async function _loadSubDir(path, targetUl, depth) {
    try {
      const data = await _apiDir(path);
      _dirCache[path] = data.entries || [];
      targetUl.innerHTML = '';
      if (!data.entries || !data.entries.length) {
        targetUl.innerHTML = '<li class="ws-empty" style="padding-left:' + (depth * 16 + 8) + 'px">空目录</li>';
      } else {
        _buildTree(targetUl, path, data.entries, depth);
      }
    } catch (e) {
      targetUl.innerHTML = `<li class="ws-error" style="padding-left:${depth * 16 + 8}px">加载失败</li>`;
    }
  }

  // ── 面包屑 ──────────────────────────────────────────────────────────────

  function _renderBreadcrumb(path) {
    const el = document.getElementById('wsBreadcrumb');
    if (!el) return;

    if (!_rootPath || !path) {
      el.innerHTML = '';
      return;
    }

    // 计算相对于根目录的路径段
    let rel = '';
    try {
      if (path.startsWith(_rootPath)) {
        rel = path.slice(_rootPath.length);
      }
    } catch (_) {}

    const segments = rel ? rel.split('/').filter(Boolean) : [];
    const parts = [];

    // 根目录
    const rootName = _rootPath.split('/').filter(Boolean).pop() || '/';
    parts.push({ name: rootName, path: _rootPath });

    // 子路径
    let accumulated = _rootPath;
    for (const seg of segments) {
      accumulated += '/' + seg;
      parts.push({ name: seg, path: accumulated });
    }

    el.innerHTML = parts.map((p, i) => {
      if (i === parts.length - 1) {
        return `<span class="ws-crumb ws-crumb-cur">${_esc(p.name)}</span>`;
      }
      return `<span class="ws-crumb ws-crumb-link" onclick="Workspace.loadDirectory('${_esc(p.path)}')">${_esc(p.name)}</span><span class="ws-crumb-sep">/</span>`;
    }).join('');
  }

  // ── 文件预览 ─────────────────────────────────────────────────────────────

  async function previewFile(path) {
    const panel = document.getElementById('wsPreviewPanel');
    const pathEl = document.getElementById('wsPreviewPath');
    const contentEl = document.getElementById('wsPreviewContent');
    if (!panel || !contentEl) return;

    _previewPath = path;
    const fileName = path.split('/').pop();

    if (pathEl) pathEl.textContent = fileName;
    panel.style.display = '';
    contentEl.innerHTML = '<div class="ws-preview-loading">加载中…</div>';

    // 高亮当前文件
    document.querySelectorAll('.ws-item.ws-file .ws-row').forEach(r => {
      r.classList.toggle('active', r.closest('[data-path]')?.dataset.path === path);
    });

    const ext = _ext(fileName);

    try {
      const data = await _apiFile(path);

      if (IMAGE_EXTS.has(ext)) {
        // 图片：通过数据展示（如果有 base64）或路径
        contentEl.innerHTML = `<div class="ws-preview-image"><img src="/api/workspace/file?path=${encodeURIComponent(path)}&raw=1" alt="${_esc(fileName)}" style="max-width:100%;max-height:400px;border-radius:6px"></div>`;
        return;
      }

      const lines = (data.content || '').split('\n');
      const isTruncated = lines.length > 100 || (data.size || 0) > 50000;
      const displayContent = isTruncated ? lines.slice(0, 100).join('\n') : (data.content || '');

      if (MD_EXTS.has(ext)) {
        // Markdown 渲染
        const rendered = typeof MD !== 'undefined' ? MD.render(displayContent) : _esc(displayContent).replace(/\n/g, '<br>');
        contentEl.innerHTML = `
          <div class="ws-preview-md">${rendered}</div>
          ${isTruncated ? '<div class="ws-preview-truncated">⚠ 文件较大，仅显示前 100 行</div>' : ''}
        `;
      } else {
        // 代码文件
        const lang = data.language || '';
        const safe = _esc(displayContent);
        contentEl.innerHTML = `
          <div class="ws-preview-meta">
            <span class="ws-lang-badge">${_esc(lang || ext || 'text')}</span>
            <span class="ws-line-count">${data.lines || lines.length} 行</span>
            <button class="ws-copy-btn" onclick="Workspace.copyPreview()">复制</button>
          </div>
          <pre class="ws-preview-code"><code>${safe}</code></pre>
          ${isTruncated ? '<div class="ws-preview-truncated">⚠ 文件较大，仅显示前 100 行</div>' : ''}
        `;
      }
    } catch (e) {
      contentEl.innerHTML = `<div class="ws-error">预览失败：${_esc(e.message)}</div>`;
    }
  }

  function copyPreview() {
    const code = document.querySelector('#wsPreviewContent code, #wsPreviewContent .ws-preview-md');
    if (!code) return;
    const text = code.textContent || code.innerText || '';
    navigator.clipboard.writeText(text).then(() => {
      if (typeof showToast === 'function') showToast('已复制到剪贴板');
    }).catch(() => {
      if (typeof showToast === 'function') showToast('复制失败');
    });
  }

  function closePreview() {
    const panel = document.getElementById('wsPreviewPanel');
    if (panel) panel.style.display = 'none';
    _previewPath = '';
    document.querySelectorAll('.ws-item.ws-file .ws-row.active').forEach(r => r.classList.remove('active'));
  }

  // ── 搜索过滤 ─────────────────────────────────────────────────────────────

  function filterFiles(query) {
    _searchQuery = (query || '').trim();
    const container = document.getElementById('wsTreeContainer');
    if (container) _renderTree(container);
  }

  // ── 刷新 ──────────────────────────────────────────────────────────────────

  function refresh() {
    _dirCache = {};
    loadDirectory(_currentPath || '');
  }

  // ── 工具函数 ─────────────────────────────────────────────────────────────

  function _esc(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function _humanSize(bytes) {
    if (bytes == null) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  }

  function _fileIcon(name) {
    const ext = _ext(name);
    const icons = {
      '.py': '🐍', '.js': '📜', '.ts': '📜', '.jsx': '⚛️', '.tsx': '⚛️',
      '.html': '🌐', '.css': '🎨', '.scss': '🎨', '.json': '📋',
      '.yaml': '📋', '.yml': '📋', '.toml': '📋', '.ini': '⚙️', '.cfg': '⚙️',
      '.md': '📝', '.markdown': '📝', '.txt': '📄', '.log': '📃',
      '.sh': '⚡', '.bash': '⚡', '.zsh': '⚡', '.fish': '⚡',
      '.rs': '🦀', '.go': '🐹', '.java': '☕', '.c': '🔧', '.cpp': '🔧',
      '.h': '🔧', '.hpp': '🔧', '.rb': '💎', '.php': '🐘',
      '.png': '🖼️', '.jpg': '🖼️', '.jpeg': '🖼️', '.gif': '🖼️',
      '.svg': '🖼️', '.webp': '🖼️', '.ico': '🖼️',
      '.zip': '📦', '.tar': '📦', '.gz': '📦', '.bz2': '📦',
      '.pdf': '📕', '.env': '🔒', '.gitignore': '👁️',
    };
    return icons[ext] || '📄';
  }

  // ── 初始化 ───────────────────────────────────────────────────────────────

  function init() {
    if (_initialized) return;
    _initialized = true;

    // 搜索框事件绑定
    const searchEl = document.getElementById('wsSearch');
    if (searchEl) {
      searchEl.addEventListener('input', () => filterFiles(searchEl.value));
      searchEl.addEventListener('keydown', e => {
        if (e.key === 'Escape') { searchEl.value = ''; filterFiles(''); }
      });
    }

    // 刷新按钮
    const refreshBtn = document.getElementById('wsRefreshBtn');
    if (refreshBtn) refreshBtn.addEventListener('click', refresh);

    // 关闭预览按钮
    const closeBtn = document.getElementById('wsPreviewClose');
    if (closeBtn) closeBtn.addEventListener('click', closePreview);
  }

  // ── 公开 API ─────────────────────────────────────────────────────────────

  return {
    init,
    loadDirectory,
    previewFile,
    closePreview,
    copyPreview,
    filterFiles,
    refresh,
  };

})();
