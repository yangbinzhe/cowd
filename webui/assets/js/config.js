/**
 * Cowd Config - Configuration Panel Module
 */

const Config = {
  container: null,

  init() {
    this.container = document.getElementById('configSections');
  },

  async loadConfig() {
    if (!api.isAuthenticated()) return;

    try {
      const config = await api.getConfig();
      state.set('config', config);
      this.renderConfig(config);
    } catch (error) {
      console.error('Failed to load config:', error);
    }
  },

  renderConfig(config) {
    if (!this.container) return;

    const providers = config?.providers || [];
    const currentProvider = config?.current_provider || providers[0]?.name || 'anthropic';

    this.container.innerHTML = `
      <!-- API 配置 -->
      <div class="config-section">
        <h3>${t('config.api')}</h3>

        <div class="config-item">
          <div>
            <label>${t('config.provider')}</label>
            <p class="description">选择 AI 模型提供商</p>
          </div>
          <select id="configProvider">
            ${providers.map(p => `
              <option value="${p.name}" ${p.name === currentProvider ? 'selected' : ''}>
                ${p.name}
              </option>
            `).join('')}
          </select>
        </div>

        <div class="config-item">
          <div>
            <label>API Key</label>
            <p class="description">输入 API 密钥</p>
          </div>
          <input type="password" id="configApiKey" placeholder="sk-..." value="${config?.api_key || ''}">
        </div>

        <div class="config-item">
          <div>
            <label>${t('config.model')}</label>
            <p class="description">选择使用的模型</p>
          </div>
          <select id="configModel">
            ${(providers.find(p => p.name === currentProvider)?.models || []).map(m => `
              <option value="${m}" ${config?.model === m ? 'selected' : ''}>
                ${m}
              </option>
            `).join('')}
          </select>
        </div>
      </div>

      <!-- 界面配置 -->
      <div class="config-section">
        <h3>界面</h3>

        <div class="config-item">
          <div>
            <label>${t('config.theme')}</label>
            <p class="description">选择界面主题</p>
          </div>
          <select id="configTheme">
            <option value="dark" ${window.ThemeManager.getTheme() === 'dark' ? 'selected' : ''}>${t('config.theme.dark')}</option>
            <option value="light" ${window.ThemeManager.getTheme() === 'light' ? 'selected' : ''}>${t('config.theme.light')}</option>
            <option value="slate" ${window.ThemeManager.getTheme() === 'slate' ? 'selected' : ''}>${t('config.theme.slate')}</option>
          </select>
        </div>

        <div class="config-item">
          <div>
            <label>${t('config.language')}</label>
            <p class="description">选择界面语言</p>
          </div>
          <select id="configLanguage">
            <option value="zh-CN" ${getCurrentLang() === 'zh-CN' ? 'selected' : ''}>中文</option>
            <option value="en" ${getCurrentLang() === 'en' ? 'selected' : ''}>English</option>
          </select>
        </div>
      </div>

      <!-- 保存按钮 -->
      <div class="config-section">
        <button class="btn primary" id="saveConfigBtn">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path>
            <polyline points="17 21 17 13 7 13 7 21"></polyline>
            <polyline points="7 3 7 8 15 8"></polyline>
          </svg>
          ${t('config.save')}
        </button>
      </div>
    `;

    // Bind event handlers
    this.bindHandlers();
  },

  bindHandlers() {
    // Theme change
    const themeSelect = document.getElementById('configTheme');
    if (themeSelect) {
      themeSelect.addEventListener('change', (e) => {
        window.ThemeManager.setTheme(e.target.value);
      });
    }

    // Language change
    const langSelect = document.getElementById('configLanguage');
    if (langSelect) {
      langSelect.addEventListener('change', (e) => {
        setLanguage(e.target.value);
        this.loadConfig(); // Re-render with new language
      });
    }

    // Provider change - update models
    const providerSelect = document.getElementById('configProvider');
    if (providerSelect) {
      providerSelect.addEventListener('change', (e) => {
        this.updateModels(e.target.value);
      });
    }

    // Save button
    const saveBtn = document.getElementById('saveConfigBtn');
    if (saveBtn) {
      saveBtn.addEventListener('click', () => this.saveConfig());
    }
  },

  async updateModels(providerName) {
    const modelSelect = document.getElementById('configModel');
    if (!modelSelect) return;

    // TODO: Fetch models for provider
    // For now, show a loading state
    modelSelect.innerHTML = '<option>加载中...</option>';
  },

  async saveConfig() {
    const provider = document.getElementById('configProvider')?.value;
    const apiKey = document.getElementById('configApiKey')?.value;
    const model = document.getElementById('configModel')?.value;

    try {
      await api.updateConfig({
        provider,
        api_key: apiKey,
        model
      });
      Toast.success(t('config.saved'));
    } catch (error) {
      Toast.error('保存配置失败');
    }
  }
};

// Export
window.Config = Config;
