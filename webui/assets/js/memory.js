/**
 * Cowd Memory - Memory Panel Module
 */

const Memory = {
  statsContainer: null,
  layersContainer: null,
  searchMode: 'hybrid', // P0-3: Default search mode
  currentView: 'layers', // 3C-2: layers or graph

  init() {
    this.statsContainer = document.getElementById('memoryStats');
    this.layersContainer = document.getElementById('memoryLayers');

    // Bind refresh button
    const refreshBtn = document.getElementById('refreshMemoryBtn');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', () => this.loadMemory());
    }

    // P0-3: Initialize memory search
    this.initSearch();

    // 3C-2: Bind view tabs
    document.querySelectorAll('[data-memview]').forEach(tab => {
      tab.addEventListener('click', () => {
        const view = tab.dataset.memview;
        this.switchView(view);
      });
    });

    // P1-1: Event delegation for entry edit/delete buttons
    document.addEventListener('click', (e) => {
      const editBtn = e.target.closest('.entry-edit-btn');
      if (editBtn) {
        e.preventDefault();
        this.editEntry(editBtn.dataset.id);
        return;
      }
      const deleteBtn = e.target.closest('.entry-delete-btn');
      if (deleteBtn) {
        e.preventDefault();
        this.deleteEntry(deleteBtn.dataset.id);
        return;
      }
    });
  },

  // ═══════════════════════════════════════════════════════════════════
  // Memory Search (P0-3: BM25 hybrid search)
  // ═══════════════════════════════════════════════════════════════════

  initSearch() {
    // Add search UI to memory panel header if not present
    const panelMemory = document.getElementById('panelMemory');
    if (!panelMemory) return;

    // Check if search bar already exists
    if (document.getElementById('memorySearchBar')) return;

    const searchHtml = `
      <div id="memorySearchBar" class="memory-search-bar">
        <input type="text" id="memorySearchInput" placeholder="${window.i18nInstance?.t('memory.searchPlaceholder') || '搜索记忆...'}">
        <select id="memorySearchMode" class="memory-search-mode">
          <option value="hybrid" selected>Hybrid</option>
          <option value="vector">Semantic</option>
          <option value="bm25">Keyword</option>
        </select>
        <button id="memorySearchBtn" class="btn secondary" title="Search">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"></circle>
            <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
          </svg>
        </button>
      </div>
      <div id="memorySearchResults" class="memory-search-results" style="display:none;"></div>
    `;

    // Insert after the panel header
    const header = panelMemory.querySelector('.panel-header');
    if (header) {
      header.insertAdjacentHTML('afterend', searchHtml);
    }

    // Bind events
    const searchBtn = document.getElementById('memorySearchBtn');
    const searchInput = document.getElementById('memorySearchInput');
    const searchMode = document.getElementById('memorySearchMode');

    if (searchBtn) {
      searchBtn.addEventListener('click', () => this.performSearch());
    }
    if (searchInput) {
      searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') this.performSearch();
      });
    }
    if (searchMode) {
      searchMode.addEventListener('change', (e) => {
        this.searchMode = e.target.value;
      });
    }
  },

  async performSearch() {
    const input = document.getElementById('memorySearchInput');
    const resultsContainer = document.getElementById('memorySearchResults');
    if (!input || !resultsContainer) return;

    const query = input.value.trim();
    if (!query) return;

    resultsContainer.style.display = 'block';
    resultsContainer.innerHTML = '<div class="memory-search-loading">Searching...</div>';

    try {
      const result = await window.api?.searchMemory(query, {
        mode: this.searchMode,
        limit: 10,
      });

      const results = result?.results || [];
      const mode = result?.mode || this.searchMode;

      if (results.length === 0) {
        resultsContainer.innerHTML = `
          <div class="memory-search-empty">${window.i18nInstance?.t('memory.noResults') || '没有找到匹配的记忆'}</div>
        `;
        return;
      }

      resultsContainer.innerHTML = `
        <div class="memory-search-header">
          <span>${results.length} results</span>
          <span class="memory-search-mode-badge badge-${mode}">${mode}</span>
        </div>
        ${results.map(r => this.renderSearchResult(r)).join('')}
      `;
    } catch (error) {
      console.error('[Cowd Memory] Search failed:', error);
      resultsContainer.innerHTML = `<div class="memory-search-error">Search failed: ${error.message || 'Unknown error'}</div>`;
    }
  },

  renderSearchResult(r) {
    const content = r.content || '';
    const truncated = content.length > 200 ? content.substring(0, 200) + '...' : content;
    const source = r.source || 'vector';
    const confidence = r.confidence != null ? (r.confidence * 100).toFixed(1) : '-';
    const bm25Score = r.bm25_score != null ? (r.bm25_score * 100).toFixed(1) : '-';
    const hybridScore = r.hybrid_score != null ? (r.hybrid_score * 100).toFixed(1) : '-';

    return `
      <div class="memory-result" data-id="${this.escapeHtml(r.id || '')}">
        <div class="result-header">
          <span class="result-source badge-${source}">${source}</span>
          ${r.hybrid_score != null ? `<span class="result-score">${hybridScore}%</span>` : ''}
        </div>
        <div class="result-content">${this.escapeHtml(truncated)}</div>
        <div class="result-meta">
          ${r.hybrid_score != null ? `Vec: ${confidence}% | BM25: ${bm25Score}%` : `Confidence: ${confidence}%`}
          ${r.layer ? ` | ${this.escapeHtml(r.layer)}` : ''}
        </div>
      </div>
    `;
  },

  // ═══════════════════════════════════════════════════════════════════
  // Memory Loading & Rendering
  // ═══════════════════════════════════════════════════════════════════

  async loadMemory() {
    if (!window.api?.isAuthenticated()) return;

    try {
      const [stats, layers] = await Promise.all([
        window.api?.getMemoryStats(),
        window.api?.getMemoryLayers()
      ]);

      window.appState?.set('memory', { stats, layers });
      this.renderMemory(stats, layers);
    } catch (error) {
      console.error('Failed to load memory:', error);
      window.Toast?.error('加载记忆失败');
    }
  },

  renderMemory(stats, layers) {
    this.renderStats(stats);
    this.renderLayers(layers);
  },

  renderStats(stats) {
    if (!this.statsContainer) return;

    const totalEntries = stats?.total_entries || 0;
    const totalTokens = stats?.total_tokens || 0;
    const layers = stats?.layers || {};

    this.statsContainer.innerHTML = `
      <div class="memory-stat">
        <div class="value">${totalEntries.toLocaleString()}</div>
        <div class="label">${window.i18nInstance?.t('memory.entries') || 'Entries'}</div>
      </div>
      <div class="memory-stat">
        <div class="value">${totalTokens.toLocaleString()}</div>
        <div class="label">${window.i18nInstance?.t('memory.tokens') || 'Tokens'}</div>
      </div>
      <div class="memory-stat">
        <div class="value">${layers.l0?.count || 0}</div>
        <div class="label">L0</div>
      </div>
      <div class="memory-stat">
        <div class="value">${layers.l1?.count || 0}</div>
        <div class="label">L1</div>
      </div>
      <div class="memory-stat">
        <div class="value">${layers.l2?.count || 0}</div>
        <div class="label">L2</div>
      </div>
      <div class="memory-stat">
        <div class="value">${layers.l3?.count || 0}</div>
        <div class="label">L3</div>
      </div>
      <div class="memory-stat">
        <div class="value">${layers.l4?.count || 0}</div>
        <div class="label">L4</div>
      </div>
    `;
  },

  renderLayers(layers) {
    if (!this.layersContainer) return;

    const _t = (key, fallback) => window.i18nInstance?.t(key) || fallback;
    const layerNames = {
      l0: _t('memory.layer0', 'L0 - Identity'),
      l1: _t('memory.layer1', 'L1 - Essential'),
      l2: _t('memory.layer2', 'L2 - Project'),
      l3: _t('memory.layer3', 'L3 - Session'),
      l4: _t('memory.layer4', 'L4 - Deep Archive')
    };

    this.layersContainer.innerHTML = ['l0', 'l1', 'l2', 'l3', 'l4'].map(layer => `
      <div class="memory-layer">
        <h3>${layerNames[layer]}</h3>
        <div class="layer-content" id="layer${layer.charAt(1)}Content">
          ${this.renderLayerContent(layers?.[layer])}
        </div>
      </div>
    `).join('');
  },

  renderLayerContent(layerData) {
    if (!layerData) {
      return '<p style="color: var(--text-dim);">暂无数据</p>';
    }

    if (layerData.entries && layerData.entries.length > 0) {
      return layerData.entries.slice(0, 10).map(entry => `
        <div class="memory-entry" data-entry-id="${this.escapeHtml(entry.id || '')}" style="padding: 8px 0; border-bottom: 1px solid var(--border);">
          <div style="display:flex;justify-content:space-between;align-items:start;">
            <div style="font-size: var(--font-size-sm); color: var(--text); flex:1; min-width:0;">
              ${this.escapeHtml(entry.content?.substring(0, 120) || entry.title || '')}
            </div>
            <div class="entry-actions">
              <button class="action-btn entry-edit-btn" data-id="${this.escapeHtml(entry.id || '')}" title="Edit">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path></svg>
              </button>
              <button class="action-btn entry-delete-btn" data-id="${this.escapeHtml(entry.id || '')}" title="Delete">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path></svg>
              </button>
            </div>
          </div>
          <div style="font-size: var(--font-size-sm); color: var(--text-dim); margin-top: 4px;">
            ${entry.priority ? `<span class="priority-badge priority-${entry.priority?.toLowerCase?.() || 'normal'}">${this.escapeHtml(entry.priority || '')}</span>` : ''}
            ${entry.tokens || 0} tokens
            ${entry.tags?.length ? ` | ${entry.tags.map(t => '#' + this.escapeHtml(t)).join(' ')}` : ''}
          </div>
        </div>
      `).join('');
    }

    return '<p style="color: var(--text-dim);">暂无数据</p>';
  },

  // ═══════════════════════════════════════════════════════════════════
  // P1-1: Memory Entry Edit / Delete
  // ═══════════════════════════════════════════════════════════════════

  async editEntry(entryId) {
    if (!entryId) return;

    try {
      const entry = await window.api?.getMemoryEntry(entryId);
      if (!entry) {
        window.Toast?.error('找不到该记忆条目');
        return;
      }

      // Create edit modal
      const modal = document.createElement('div');
      modal.className = 'modal active';
      modal.id = 'memoryEditModal';
      modal.innerHTML = `
        <div class="modal-content" style="max-width:600px;">
          <div class="modal-header">
            <h2>编辑记忆</h2>
          </div>
          <div class="form-group">
            <label>内容</label>
            <textarea id="editMemoryContent" class="edit-textarea" style="min-height:120px;">${this.escapeHtml(entry.content || '')}</textarea>
          </div>
          <div class="form-group">
            <label>标签 (逗号分隔)</label>
            <input type="text" id="editMemoryTags" value="${this.escapeHtml((entry.tags || []).join(', '))}">
          </div>
          <div class="form-group">
            <label>优先级</label>
            <select id="editMemoryPriority">
              <option value="Critical" ${entry.priority === 'Critical' ? 'selected' : ''}>Critical</option>
              <option value="High" ${entry.priority === 'High' ? 'selected' : ''}>High</option>
              <option value="Normal" ${entry.priority === 'Normal' || !entry.priority ? 'selected' : ''}>Normal</option>
              <option value="Low" ${entry.priority === 'Low' ? 'selected' : ''}>Low</option>
            </select>
          </div>
          <div class="form-actions">
            <button class="btn secondary" id="editMemoryCancel">取消</button>
            <button class="btn primary" id="editMemorySave">保存</button>
          </div>
        </div>
      `;

      document.body.appendChild(modal);

      // Bind save
      modal.querySelector('#editMemorySave').addEventListener('click', async () => {
        const content = modal.querySelector('#editMemoryContent').value.trim();
        const tagsStr = modal.querySelector('#editMemoryTags').value.trim();
        const priority = modal.querySelector('#editMemoryPriority').value;
        const tags = tagsStr ? tagsStr.split(',').map(t => t.trim()).filter(Boolean) : undefined;

        try {
          await window.api?.updateMemory(entryId, { content, tags, priority });
          window.Toast?.success('记忆已更新');
          modal.remove();
          this.loadMemory();
        } catch (e) {
          window.Toast?.error('更新失败: ' + (e.message || ''));
        }
      });

      // Bind cancel
      modal.querySelector('#editMemoryCancel').addEventListener('click', () => modal.remove());

    } catch (e) {
      window.Toast?.error('加载失败: ' + (e.message || ''));
    }
  },

  async deleteEntry(entryId) {
    if (!entryId) return;

    if (!confirm('确定要删除这条记忆吗？此操作不可撤销。')) return;

    try {
      await window.api?.deleteMemory(entryId);
      window.Toast?.success('记忆已删除');
      this.loadMemory();
    } catch (e) {
      window.Toast?.error('删除失败: ' + (e.message || ''));
    }
  },

  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  },

  // ═══════════════════════════════════════════════════════════════════
  // 3C-2: View switching (layers / graph)
  // ═══════════════════════════════════════════════════════════════════

  switchView(view) {
    this.currentView = view;

    // Update tab active states
    document.querySelectorAll('[data-memview]').forEach(tab => {
      tab.classList.toggle('active', tab.dataset.memview === view);
    });

    const layersView = document.getElementById('memoryLayersView');
    const graphView = document.getElementById('memoryGraphView');

    if (view === 'graph') {
      if (layersView) layersView.style.display = 'none';
      if (graphView) graphView.style.display = '';
      this.loadGraph();
    } else {
      if (layersView) layersView.style.display = '';
      if (graphView) graphView.style.display = 'none';
    }
  },

  async loadGraph() {
    if (!window.KnowledgeGraph) {
      const container = document.getElementById('memoryGraphContainer');
      if (container) {
        container.innerHTML = '<div style="padding:20px;color:var(--text-dim);">知识图谱模块未加载</div>';
      }
      return;
    }
    try {
      const graph = await window.api?.getKnowledgeGraph();
      window.KnowledgeGraph.render('memoryGraphContainer', graph);
    } catch (e) {
      console.error('[Cowd Memory] Graph load failed:', e);
      const container = document.getElementById('memoryGraphContainer');
      if (container) {
        container.innerHTML = '<div style="padding:20px;color:var(--text-dim);">加载知识图谱失败</div>';
      }
    }
  }
};

// Export
window.Memory = Memory;
