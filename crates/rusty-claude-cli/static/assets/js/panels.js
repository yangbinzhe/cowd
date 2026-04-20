/**
 * Cowd Panels - Panel Navigation Module
 */

// Panel Manager
const Panels = {
  currentPanel: 'chat',

  init() {
    // Bind nav tabs
    document.querySelectorAll('.nav-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        const panel = tab.dataset.panel;
        this.show(panel);
      });
    });

    // Keyboard shortcuts
    document.addEventListener('keydown', (e) => {
      // Cmd/Ctrl + 1-6 for panel navigation
      if ((e.metaKey || e.ctrlKey) && e.key >= '1' && e.key <= '6') {
        e.preventDefault();
        const panels = ['chat', 'sessions', 'memory', 'config', 'platform', 'cron'];
        const index = parseInt(e.key) - 1;
        if (index < panels.length && panels[index]) {
          this.show(panels[index]);
        }
      }
    });
  },

  show(panelId) {
    // Update nav tabs
    document.querySelectorAll('.nav-tab').forEach(tab => {
      tab.classList.toggle('active', tab.dataset.panel === panelId);
    });

    // Update panels
    document.querySelectorAll('.panel').forEach(panel => {
      panel.classList.toggle('active', panel.id === `panel${this.capitalize(panelId)}`);
    });

    this.currentPanel = panelId;

    // Load panel data
    this.onPanelChange(panelId);
  },

  capitalize(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
  },

  onPanelChange(panel) {
    switch (panel) {
      case 'sessions':
        window.Sessions?.loadSessions();
        break;
      case 'memory':
        window.Memory?.loadMemory();
        break;
      case 'config':
        window.Config?.loadConfig();
        break;
      case 'platform':
        window.Platform?.loadPlatforms();
        break;
      case 'cron':
        window.Cron?.loadCrons();
        break;
    }
  }
};

// Initialize panels when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  Panels.init();
});

// Export
window.Panels = Panels;
window.panelManager = Panels;
