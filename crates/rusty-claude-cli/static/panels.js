/**
 * cc-webui -- panels.js
 * 右侧面板管理：文件浏览 | 技能 | 记忆 | 设置
 * 全局命名空间: window.Panels
 */

window.Panels = (() => {

  // ── 内部状态 ──────────────────────────────────────────────────────────────

  let _activeTab = null;       // 当前激活的 tab 名
  let _skillsData = null;      // 技能数据缓存
  let _memoryData = null;      // 记忆数据缓存
  let _initialized = false;

  // tab 名称到面板容器 ID 的映射
  const TAB_PANELS = {
    workspace: 'panelWorkspace',
    skills:    'panelSkills',
    memory:    'panelMemory',
    settings:  'panelSettings',
  };

  // ── HTML 转义 ─────────────────────────────────────────────────────────────

  function _esc(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  // ── Tab 切换 ──────────────────────────────────────────────────────────────

  async function switchTab(name) {
    if (!TAB_PANELS[name]) return;

    _activeTab = name;

    // 更新 tab 按钮激活状态
    document.querySelectorAll('.right-tab-btn').forEach(btn => {
      btn.classList.toggle('active', btn.dataset.tab === name);
    });

    // 切换面板可见性
    Object.entries(TAB_PANELS).forEach(([tabName, panelId]) => {
      const el = document.getElementById(panelId);
      if (el) el.style.display = tabName === name ? '' : 'none';
    });

    // 懒加载面板数据
    if (name === 'workspace') {
      if (typeof Workspace !== 'undefined') {
        Workspace.init();
        await Workspace.loadDirectory('');
      }
    } else if (name === 'skills') {
      await loadSkills();
    } else if (name === 'memory') {
      await loadMemoryStatus();
    }
  }

  // ── 技能面板 ──────────────────────────────────────────────────────────────

  async function loadSkills(force) {
    if (_skillsData && !force) {
      _renderSkills(_skillsData);
      return;
    }

    const box = document.getElementById('skillsList');
    if (!box) return;

    box.innerHTML = '<div class="panel-loading">加载中…</div>';

    try {
      const r = await fetch('/api/skills');
      const data = await r.json();
      _skillsData = data.skills || [];
      _renderSkills(_skillsData);
    } catch (e) {
      box.innerHTML = `<div class="panel-error">加载失败：${_esc(e.message)}</div>`;
    }
  }

  function _renderSkills(skills) {
    const box = document.getElementById('skillsList');
    if (!box) return;

    const searchEl = document.getElementById('skillsSearch');
    const query = searchEl ? (searchEl.value || '').toLowerCase().trim() : '';

    const filtered = query
      ? skills.filter(s =>
          (s.name || '').toLowerCase().includes(query) ||
          (s.description || '').toLowerCase().includes(query)
        )
      : skills;

    if (!filtered.length) {
      box.innerHTML = '<div class="panel-empty">暂无技能' + (query ? '（无匹配结果）' : '') + '</div>';
      return;
    }

    box.innerHTML = '';
    for (const skill of filtered) {
      const card = document.createElement('div');
      card.className = 'skill-card' + (skill.enabled === false ? ' disabled' : '');

      const statusClass = skill.enabled === false ? 'disabled' : 'enabled';
      const statusText = skill.enabled === false ? '已禁用' : '已启用';

      card.innerHTML = `
        <div class="skill-card-header">
          <span class="skill-name">${_esc(skill.name)}</span>
          <span class="skill-status ${statusClass}">${statusText}</span>
        </div>
        ${skill.description
          ? `<p class="skill-desc">${_esc(skill.description)}</p>`
          : ''}
      `;
      box.appendChild(card);
    }
  }

  function filterSkills() {
    if (_skillsData) _renderSkills(_skillsData);
  }

  // ── 记忆面板 ──────────────────────────────────────────────────────────────

  async function loadMemoryStatus(force) {
    if (_memoryData && !force) {
      _renderMemory(_memoryData);
      return;
    }

    const box = document.getElementById('memoryContent');
    if (!box) return;

    box.innerHTML = '<div class="panel-loading">加载中…</div>';

    try {
      const r = await fetch('/api/memory/status');
      const data = await r.json();
      _memoryData = data;
      _renderMemory(data);
    } catch (e) {
      box.innerHTML = `<div class="panel-error">加载失败：${_esc(e.message)}</div>`;
    }
  }

  function _renderMemory(data) {
    const box = document.getElementById('memoryContent');
    if (!box) return;

    if (!data.enabled) {
      box.innerHTML = `
        <div class="memory-disabled">
          <div class="memory-disabled-icon">🧠</div>
          <div class="memory-disabled-text">记忆系统未启用</div>
          <div class="memory-disabled-hint">在 config.yaml 中设置 <code>memory.enabled: true</code> 来启用</div>
        </div>
      `;
      return;
    }

    const layers = data.layers || {};
    const vector = data.vector || {};
    const stats = data.stats || {};
    const extraction = data.extraction || {};

    // 层级配置
    const layerDefs = [
      { key: 'l0', label: 'L0 身份层', detail: layers.l0 ? (layers.l0.enabled ? '已启用' : '已禁用') : '—', badgeClass: 'l0' },
      { key: 'l1', label: 'L1 核心知识', detail: layers.l1 ? `最大 ${layers.l1.maxTokens || '—'} tokens` : '—', badgeClass: 'l1' },
      { key: 'l2', label: 'L2 扩展知识', detail: layers.l2 ? `最大 ${layers.l2.maxTokens || '—'} tokens` : '—', badgeClass: 'l2' },
      { key: 'l3', label: 'L3 语义检索', detail: layers.l3 ? `检索上限 ${layers.l3.searchLimit || '—'}` : '—', badgeClass: 'l3' },
      { key: 'l4', label: 'L4 时序归档', detail: layers.l4 ? (layers.l4.enabled ? '已启用' : '已禁用') : '—', badgeClass: 'l4' },
    ];

    const layersHtml = layerDefs.map(l => `
      <div class="memory-layer" data-layer="${l.key}">
        <span class="layer-badge ${l.badgeClass}">${_esc(l.label)}</span>
        <span class="layer-detail">${_esc(l.detail)}</span>
      </div>
    `).join('');

    // 向量状态
    const vectorHtml = vector.enabled ? `
      <div class="memory-section">
        <div class="memory-section-title">向量搜索</div>
        <div class="memory-vector-info">
          <div class="memory-info-row">
            <span class="info-label">嵌入模型</span>
            <span class="info-val">${_esc(vector.model || '未配置')}</span>
          </div>
          <div class="memory-info-row">
            <span class="info-label">维度</span>
            <span class="info-val">${vector.dimension > 0 ? vector.dimension : '自动探测'}</span>
          </div>
          ${vector.apiUrl ? `<div class="memory-info-row"><span class="info-label">API</span><span class="info-val" title="${_esc(vector.apiUrl)}">${_esc(vector.apiUrl.replace(/\/\/[^/]+/, '//<host>'))}</span></div>` : ''}
        </div>
      </div>
    ` : `
      <div class="memory-section">
        <div class="memory-section-title">向量搜索</div>
        <div class="memory-vector-disabled">未启用（仅使用关键词检索）</div>
      </div>
    `;

    box.innerHTML = `
      <div class="memory-overview">
        <div class="memory-header">
          <span class="memory-icon">🧠</span>
          <span class="memory-title">认知记忆系统</span>
          <span class="memory-status-badge enabled">运行中</span>
        </div>

        <div class="memory-section">
          <div class="memory-section-title">记忆层级</div>
          <div class="memory-layers">${layersHtml}</div>
        </div>

        ${vectorHtml}

        <div class="memory-section">
          <div class="memory-section-title">存储统计</div>
          <div class="memory-stats">
            <div class="memory-info-row">
              <span class="info-label">存储路径</span>
              <span class="info-val memory-path" title="${_esc(data.storePath || '')}">${_esc(data.storePath || '—')}</span>
            </div>
            <div class="memory-info-row">
              <span class="info-label">条目数</span>
              <span class="info-val">${stats.totalEntries != null ? stats.totalEntries : '—'}</span>
            </div>
            <div class="memory-info-row">
              <span class="info-label">占用空间</span>
              <span class="info-val">${_esc(stats.totalSize || '—')}</span>
            </div>
            <div class="memory-info-row">
              <span class="info-label">自动提取</span>
              <span class="info-val">${extraction.autoExtract ? '已启用' : '已禁用'}</span>
            </div>
          </div>
        </div>

        <button class="memory-refresh-btn" onclick="Panels.loadMemoryStatus(true)">
          ↻ 刷新状态
        </button>
      </div>
    `;
  }

  // ── 初始化 Tab ────────────────────────────────────────────────────────────

  function initPanelTabs() {
    if (_initialized) return;
    _initialized = true;

    // 绑定 tab 按钮点击
    document.querySelectorAll('.right-tab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const tab = btn.dataset.tab;
        if (tab) switchTab(tab);
      });
    });

    // 技能搜索框
    const skillSearch = document.getElementById('skillsSearch');
    if (skillSearch) {
      skillSearch.addEventListener('input', filterSkills);
      skillSearch.addEventListener('keydown', e => {
        if (e.key === 'Escape') { skillSearch.value = ''; filterSkills(); }
      });
    }

    // 技能刷新按钮
    const skillRefresh = document.getElementById('skillsRefreshBtn');
    if (skillRefresh) skillRefresh.addEventListener('click', () => loadSkills(true));

    // 默认激活第一个 tab（文件浏览）
    const firstTab = document.querySelector('.right-tab-btn[data-tab]');
    if (firstTab && firstTab.dataset.tab) {
      // 不自动加载，等用户点击或外部调用
      firstTab.classList.add('active');
      const firstPanelId = TAB_PANELS[firstTab.dataset.tab];
      if (firstPanelId) {
        const panel = document.getElementById(firstPanelId);
        if (panel) panel.style.display = '';
        // 隐藏其他面板
        Object.entries(TAB_PANELS).forEach(([name, id]) => {
          if (name !== firstTab.dataset.tab) {
            const el = document.getElementById(id);
            if (el) el.style.display = 'none';
          }
        });
        _activeTab = firstTab.dataset.tab;
      }
    }
  }

  // ── 外部调用接口 ─────────────────────────────────────────────────────────

  function getActiveTab() {
    return _activeTab;
  }

  // ── 公开 API ─────────────────────────────────────────────────────────────

  return {
    initPanelTabs,
    switchTab,
    loadSkills,
    loadMemoryStatus,
    filterSkills,
    getActiveTab,
  };

})();
