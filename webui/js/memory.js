/**
 * Cowd Memory - Memory Panel Module
 */

const Memory = {
  statsContainer: null,
  layersContainer: null,

  init() {
    this.statsContainer = document.getElementById('memoryStats');
    this.layersContainer = document.getElementById('memoryLayers');

    // Bind refresh button
    const refreshBtn = document.getElementById('refreshMemoryBtn');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', () => this.loadMemory());
    }
  },

  async loadMemory() {
    if (!api.isAuthenticated()) return;

    try {
      const [stats, layers] = await Promise.all([
        api.getMemoryStats(),
        api.getMemoryLayers()
      ]);

      state.set('memory', { stats, layers });
      this.renderMemory(stats, layers);
    } catch (error) {
      console.error('Failed to load memory:', error);
      Toast.error('加载记忆失败');
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
        <div class="label">${t('memory.entries')}</div>
      </div>
      <div class="memory-stat">
        <div class="value">${totalTokens.toLocaleString()}</div>
        <div class="label">${t('memory.tokens')}</div>
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
    `;
  },

  renderLayers(layers) {
    if (!this.layersContainer) return;

    const layerNames = {
      l0: t('memory.layer0'),
      l1: t('memory.layer1'),
      l2: t('memory.layer2'),
      l3: t('memory.layer3')
    };

    this.layersContainer.innerHTML = ['l0', 'l1', 'l2', 'l3'].map(layer => `
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
      return layerData.entries.slice(0, 5).map(entry => `
        <div class="memory-entry" style="padding: 8px 0; border-bottom: 1px solid var(--border);">
          <div style="font-size: var(--font-size-sm); color: var(--text);">
            ${this.escapeHtml(entry.content?.substring(0, 100) || '')}...
          </div>
          <div style="font-size: var(--font-size-sm); color: var(--text-dim); margin-top: 4px;">
            ${entry.tokens || 0} tokens
          </div>
        </div>
      `).join('');
    }

    return '<p style="color: var(--text-dim);">暂无数据</p>';
  },

  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
};

// Export
window.Memory = Memory;
