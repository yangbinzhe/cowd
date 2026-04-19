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
      // Cmd/Ctrl + 1-5 for panel navigation
      if ((e.metaKey || e.ctrlKey) && e.key >= '1' && e.key <= '5') {
        e.preventDefault();
        const panels = ['chat', 'sessions', 'memory', 'config', 'platform'];
        const index = parseInt(e.key) - 1;
        if (panels[index]) {
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
        Sessions.loadSessions();
        break;
      case 'memory':
        Memory.loadMemory();
        break;
      case 'config':
        Config.loadConfig();
        break;
      case 'platform':
        Platform.loadPlatforms();
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
