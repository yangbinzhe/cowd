(() => {
  const config = window.COWD_DOCS || {};
  const root = document.documentElement;
  const body = document.body;
  const languageKey = 'cowd-docs-language';
  const themeKey = 'cowd-docs-theme';

  function stored(key) {
    try { return localStorage.getItem(key); } catch { return null; }
  }

  function persist(key, value) {
    try { localStorage.setItem(key, value); } catch { /* file:// privacy mode */ }
  }

  const initialLanguage = stored(languageKey) === 'en' ? 'en' : 'zh';
  const initialTheme = stored(themeKey)
    || (window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');

  function textFor(item, language) {
    return language === 'en' ? item.en : item.zh;
  }

  function applyLanguage(language) {
    const normalized = language === 'en' ? 'en' : 'zh';
    root.lang = normalized === 'en' ? 'en' : 'zh-CN';
    root.dataset.language = normalized;
    document.querySelectorAll('[data-zh][data-en]').forEach((node) => {
      node.textContent = node.dataset[normalized] || '';
    });
    document.querySelectorAll('[data-zh-label][data-en-label]').forEach((node) => {
      node.setAttribute('aria-label', node.dataset[`${normalized}Label`] || '');
      node.setAttribute('title', node.dataset[`${normalized}Label`] || '');
    });
    document.querySelectorAll('[data-zh-placeholder][data-en-placeholder]').forEach((node) => {
      node.setAttribute('placeholder', node.dataset[`${normalized}Placeholder`] || '');
    });
    document.querySelectorAll('[data-language]').forEach((button) => {
      button.setAttribute('aria-pressed', String(button.dataset.language === normalized));
    });
    const page = (config.nav || []).find((entry) => entry.id === body.dataset.page);
    if (page && config.product) {
      document.title = `${textFor(page, normalized)} · ${textFor(config.product, normalized)}`;
    } else if (config.title) {
      document.title = textFor(config.title, normalized);
    }
    persist(languageKey, normalized);
  }

  function applyTheme(theme) {
    const normalized = theme === 'dark' ? 'dark' : 'light';
    root.dataset.theme = normalized;
    const toggle = document.querySelector('[data-theme-toggle]');
    if (toggle) toggle.dataset.activeTheme = normalized;
    persist(themeKey, normalized);
  }

  function icon(name) {
    const glyphs = {
      overview: '◎', architecture: '⌘', runtime: '↯', reality: '◇', gateway: '⇄', operations: '✓',
      edge: '↔', message: '✦', source: '▦', webui: '▤', workflow: '⇢', data: '◫', surfaces: '▥'
    };
    return glyphs[name] || '·';
  }

  function renderChrome() {
    const language = root.dataset.language || initialLanguage;
    const current = body.dataset.page || 'index';
    const header = document.querySelector('[data-docs-header]');
    const sidebar = document.querySelector('[data-docs-sidebar]');
    if (header) {
      header.innerHTML = `
        <button class="icon-button menu-button" type="button" data-nav-toggle data-zh-label="打开目录" data-en-label="Open navigation" aria-label="打开目录">☰</button>
        <a class="brand-link" href="index.html">
          <span class="brand-mark">${config.mark || 'C'}</span>
          <span class="brand-copy"><strong data-zh="${config.product.zh}" data-en="${config.product.en}">${textFor(config.product, language)}</strong><span data-zh="${config.subtitle.zh}" data-en="${config.subtitle.en}">${textFor(config.subtitle, language)}</span></span>
        </a>
        <span class="header-spacer"></span>
        <div class="header-actions">
          <div class="segmented" aria-label="Language">
            <button type="button" data-language="zh" aria-pressed="true">中</button>
            <button type="button" data-language="en" aria-pressed="false">EN</button>
          </div>
          <button class="icon-button" type="button" data-theme-toggle data-zh-label="切换明暗主题" data-en-label="Toggle color theme" aria-label="切换明暗主题">◐</button>
        </div>`;
    }
    if (sidebar) {
      const nav = (config.nav || []).map((entry) => `
        <a href="${entry.href}" ${entry.id === current ? 'aria-current="page"' : ''} data-search-text="${entry.zh} ${entry.en}">
          <span class="nav-glyph" aria-hidden="true">${icon(entry.icon)}</span><span data-zh="${entry.zh}" data-en="${entry.en}">${textFor(entry, language)}</span>
        </a>`).join('');
      sidebar.innerHTML = `
        <label class="sidebar-search"><span aria-hidden="true">⌕</span><input type="search" data-nav-search data-zh-label="筛选文档" data-en-label="Filter documents" data-zh-placeholder="筛选文档" data-en-placeholder="Filter documents" placeholder="筛选文档"></label>
        <section class="sidebar-group"><h2 data-zh="系统说明书" data-en="SYSTEM MANUAL">${language === 'en' ? 'SYSTEM MANUAL' : '系统说明书'}</h2><nav class="sidebar-nav" aria-label="Documentation">${nav}</nav></section>
        <p class="sidebar-meta"><span data-zh="代码基线" data-en="Code baseline">${language === 'en' ? 'Code baseline' : '代码基线'}</span><br><code>${config.version || ''}</code><br>${config.commit ? `<code>${config.commit}</code>` : ''}</p>`;
    }
  }

  root.dataset.language = initialLanguage;
  root.dataset.theme = initialTheme;
  renderChrome();
  applyLanguage(initialLanguage);
  applyTheme(initialTheme);

  document.addEventListener('click', (event) => {
    const languageButton = event.target.closest('[data-language]');
    if (languageButton) applyLanguage(languageButton.dataset.language);
    const themeButton = event.target.closest('[data-theme-toggle]');
    if (themeButton) applyTheme(root.dataset.theme === 'dark' ? 'light' : 'dark');
    if (event.target.closest('[data-nav-toggle]')) body.dataset.navOpen = body.dataset.navOpen === 'true' ? 'false' : 'true';
    if (event.target.closest('[data-nav-dismiss]') || event.target.closest('.sidebar-nav a')) body.dataset.navOpen = 'false';
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') body.dataset.navOpen = 'false';
  });

  const search = document.querySelector('[data-nav-search]');
  if (search) {
    search.addEventListener('input', () => {
      const query = search.value.trim().toLocaleLowerCase();
      document.querySelectorAll('.sidebar-nav a').forEach((link) => {
        link.hidden = Boolean(query) && !link.dataset.searchText.toLocaleLowerCase().includes(query);
      });
    });
  }

  document.querySelectorAll('pre').forEach((block) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'copy-code';
    button.dataset.zh = '复制';
    button.dataset.en = 'Copy';
    button.textContent = initialLanguage === 'en' ? 'Copy' : '复制';
    button.addEventListener('click', async () => {
      const code = block.querySelector('code')?.textContent || block.textContent || '';
      try {
        await navigator.clipboard.writeText(code.replace(button.textContent, '').trim());
        button.textContent = root.dataset.language === 'en' ? 'Copied' : '已复制';
        window.setTimeout(() => applyLanguage(root.dataset.language), 1200);
      } catch {
        button.textContent = root.dataset.language === 'en' ? 'Unavailable' : '不可用';
      }
    });
    block.append(button);
  });
})();
