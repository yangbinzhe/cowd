/**
 * Cowd Platform - Platform Management Module
 */

const Platform = {
  container: null,

  init() {
    this.container = document.getElementById('platformList');
  },

  async loadPlatforms() {
    if (!api.isAuthenticated()) return;

    try {
      const platforms = await api.listPlatforms();
      state.set('platforms', platforms);
      this.renderPlatforms(platforms);
    } catch (error) {
      console.error('Failed to load platforms:', error);
      Toast.error('加载平台失败');
    }
  },

  renderPlatforms(platforms) {
    if (!this.container) return;

    if (!platforms || platforms.length === 0) {
      this.container.innerHTML = `
        <div class="platform-empty" style="padding: 48px; text-align: center; color: var(--text-dim);">
          <p>暂无平台配置</p>
          <p style="margin-top: 8px; font-size: var(--font-size-sm);">
            在配置文件中添加平台设置
          </p>
        </div>
      `;
      return;
    }

    this.container.innerHTML = platforms.map(platform => `
      <div class="platform-card" data-platform-id="${platform.id}">
        <div class="platform-header">
          <div class="platform-name">
            <div class="platform-icon">${this.getPlatformIcon(platform.type)}</div>
            <span>${platform.name}</span>
          </div>
          <div class="platform-status">
            <span class="status-dot ${platform.connected ? '' : 'disconnected'}"></span>
            <span>${platform.connected ? t('platform.connected') : t('platform.disconnected')}</span>
          </div>
        </div>

        <div class="platform-info" style="margin-top: 12px; font-size: var(--font-size-sm); color: var(--text-muted);">
          ${platform.description || ''}
        </div>

        <div class="platform-actions">
          ${platform.connected
            ? `<button class="btn secondary" onclick="Platform.disconnect('${platform.id}')">${t('platform.disconnect')}</button>`
            : `<button class="btn primary" onclick="Platform.connect('${platform.id}')">${t('platform.connect')}</button>`
          }
          <button class="btn secondary" onclick="Platform.showSettings('${platform.id}')">${t('platform.settings')}</button>
        </div>
      </div>
    `).join('');
  },

  getPlatformIcon(type) {
    const icons = {
      feishu: '📱',
      slack: '💬',
      discord: '🎮',
      telegram: '✈️',
      email: '📧',
      wecom: '💼',
      dingtalk: '💬',
      github: '🐙',
      gitlab: '🦊'
    };
    return icons[type] || '🌐';
  },

  async connect(platformId) {
    try {
      await api.updatePlatform(platformId, { connected: true });
      Toast.success('平台连接成功');
      this.loadPlatforms();
    } catch (error) {
      Toast.error('连接失败');
    }
  },

  async disconnect(platformId) {
    try {
      await api.updatePlatform(platformId, { connected: false });
      Toast.success('平台已断开');
      this.loadPlatforms();
    } catch (error) {
      Toast.error('断开失败');
    }
  },

  showSettings(platformId) {
    // TODO: Show platform settings modal
    console.log('Show settings for:', platformId);
  }
};

// Export
window.Platform = Platform;
