/**
 * workspace.js - Workspace management
 * 处理工作区切换、文件浏览和项目管理
 */

class WorkspaceManager {
  constructor() {
    this.currentWorkspace = null;
    this.workspaces = [];
    this.fileTree = null;
    this.init();
  }

  /**
   * Initialize workspace manager
   */
  init() {
    // Listen for workspace changes
    window.addEventListener('workspace:change', (e) => {
      this.onWorkspaceChange(e.detail);
    });

    // Listen for file operations
    window.addEventListener('file:open', (e) => {
      this.openFile(e.detail.path);
    });

    window.addEventListener('file:create', (e) => {
      this.createFile(e.detail);
    });

    window.addEventListener('file:delete', (e) => {
      this.deleteFile(e.detail.path);
    });

    window.addEventListener('file:rename', (e) => {
      this.renameFile(e.detail.oldPath, e.detail.newPath);
    });
  }

  /**
   * Load workspaces from API
   */
  async loadWorkspaces() {
    try {
      this.workspaces = await api.listWorkspaces();
      appState.set('workspaces', this.workspaces);
      return this.workspaces;
    } catch (e) {
      console.error('Failed to load workspaces:', e);
      return [];
    }
  }

  /**
   * Get current workspace
   */
  async getCurrentWorkspace() {
    try {
      this.currentWorkspace = await api.getWorkspace();
      appState.set('workspace', this.currentWorkspace);
      return this.currentWorkspace;
    } catch (e) {
      console.error('Failed to get current workspace:', e);
      return null;
    }
  }

  /**
   * Switch to workspace
   * @param {string} workspaceId - Workspace ID
   */
  async switchWorkspace(workspaceId) {
    try {
      const workspace = await api.setWorkspace(workspaceId);
      this.currentWorkspace = workspace;
      appState.set('workspace', workspace);

      // Refresh file tree
      await this.loadFileTree();

      Toast.success(`已切换到工作区: ${workspace.name}`);
      return workspace;
    } catch (e) {
      Toast.error(`切换工作区失败: ${e.message}`);
      return null;
    }
  }

  /**
   * Create new workspace
   * @param {Object} data - Workspace data
   */
  async createWorkspace(data) {
    try {
      const workspace = await api.request('/workspaces', {
        method: 'POST',
        body: JSON.stringify(data),
      });

      this.workspaces.push(workspace);
      Toast.success(`已创建工作区: ${workspace.name}`);
      return workspace;
    } catch (e) {
      Toast.error(`创建工作区失败: ${e.message}`);
      return null;
    }
  }

  /**
   * Delete workspace
   * @param {string} workspaceId - Workspace ID
   */
  async deleteWorkspace(workspaceId) {
    try {
      await api.request(`/workspaces/${workspaceId}`, {
        method: 'DELETE',
      });

      this.workspaces = this.workspaces.filter(w => w.id !== workspaceId);
      Toast.success('工作区已删除');
    } catch (e) {
      Toast.error(`删除工作区失败: ${e.message}`);
    }
  }

  /**
   * Load file tree for current workspace
   * @param {string} path - Directory path (optional)
   */
  async loadFileTree(path = '') {
    try {
      const files = await api.listFiles(path);
      this.fileTree = this.buildFileTree(files);
      appState.set('fileTree', this.fileTree);
      return this.fileTree;
    } catch (e) {
      console.error('Failed to load file tree:', e);
      return null;
    }
  }

  /**
   * Build hierarchical file tree from flat list
   * @param {Array} files - Flat file list
   * @returns {Object} Hierarchical tree
   */
  buildFileTree(files) {
    const root = { name: '/', children: {}, type: 'dir' };

    files.forEach(file => {
      const parts = file.path.split('/').filter(Boolean);
      let current = root;

      parts.forEach((part, index) => {
        if (!current.children[part]) {
          current.children[part] = {
            name: part,
            path: '/' + parts.slice(0, index + 1).join('/'),
            type: index === parts.length - 1 ? file.type : 'dir',
            children: {},
          };
        }
        current = current.children[part];
      });
    });

    return root;
  }

  /**
   * Render file tree to DOM
   * @param {HTMLElement} container - Container element
   * @param {Object} tree - File tree
   * @param {number} depth - Current depth
   */
  renderFileTree(container, tree = null, depth = 0) {
    tree = tree || this.fileTree;
    if (!tree || !container) return;

    const indent = depth * 16;
    const entries = Object.entries(tree.children);

    entries.sort((a, b) => {
      // Directories first
      if (a[1].type !== b[1].type) {
        return a[1].type === 'dir' ? -1 : 1;
      }
      return a[0].localeCompare(b[0]);
    });

    entries.forEach(([name, file]) => {
      const item = document.createElement('div');
      item.className = `file-item file-${file.type}`;
      item.style.paddingLeft = `${indent + 8}px`;
      item.setAttribute('data-path', file.path);

      const icon = file.type === 'dir' ? '📁' : this.getFileIcon(name);
      item.innerHTML = `
        <span class="file-icon">${icon}</span>
        <span class="file-name">${name}</span>
      `;

      // Click handler
      item.addEventListener('click', () => {
        if (file.type === 'dir') {
          this.toggleDirectory(file.path, item);
        } else {
          this.openFile(file.path);
        }
      });

      // Context menu
      item.addEventListener('contextmenu', (e) => {
        e.preventDefault();
        this.showContextMenu(e, file);
      });

      container.appendChild(item);

      // Render children for directories
      if (file.type === 'dir' && Object.keys(file.children).length > 0) {
        const childContainer = document.createElement('div');
        childContainer.className = 'file-children';
        childContainer.style.display = 'none';
        container.appendChild(childContainer);
      }
    });
  }

  /**
   * Toggle directory expansion
   * @param {string} path - Directory path
   * @param {HTMLElement} item - Directory item element
   */
  toggleDirectory(path, item) {
    const childContainer = item.nextElementSibling;
    if (childContainer && childContainer.classList.contains('file-children')) {
      const isExpanded = childContainer.style.display !== 'none';
      childContainer.style.display = isExpanded ? 'none' : 'block';
      item.querySelector('.file-icon').textContent = isExpanded ? '📁' : '📂';

      // Load children if expanding and not loaded
      if (!isExpanded && childContainer.children.length === 0) {
        this.loadDirectory(path, childContainer);
      }
    }
  }

  /**
   * Load directory contents
   * @param {string} path - Directory path
   * @param {HTMLElement} container - Container for children
   */
  async loadDirectory(path, container) {
    try {
      const files = await api.listFiles(path);
      const subtree = this.buildFileTree(files);
      subtree.children = subtree.children || {};
      this.renderFileTree(container, subtree, 0);
    } catch (e) {
      console.error('Failed to load directory:', e);
    }
  }

  /**
   * Get file icon based on extension
   * @param {string} filename - File name
   * @returns {string} Icon emoji
   */
  getFileIcon(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const icons = {
      // Code
      js: '📜', ts: '📜', jsx: '⚛️', tsx: '⚛️',
      py: '🐍', rb: '💎', go: '🔵', rs: '🦀',
      java: '☕', c: '⚙️', cpp: '⚙️', h: '⚙️',
      // Web
      html: '🌐', css: '🎨', scss: '🎨', json: '📋',
      // Data
      md: '📝', txt: '📄', pdf: '📕',
      // Config
      yml: '⚙️', yaml: '⚙️', toml: '⚙️', xml: '⚙️',
      // Media
      png: '🖼️', jpg: '🖼️', jpeg: '🖼️', gif: '🖼️', svg: '🖼️',
      mp4: '🎬', mp3: '🎵', wav: '🎵',
      // Archives
      zip: '📦', tar: '📦', gz: '📦',
      // Default
      default: '📄',
    };
    return icons[ext] || icons.default;
  }

  /**
   * Open file
   * @param {string} path - File path
   */
  async openFile(path) {
    window.dispatchEvent(new CustomEvent('file:opened', { detail: { path } }));

    // Highlight in file tree
    document.querySelectorAll('.file-item').forEach(item => {
      item.classList.toggle('active', item.getAttribute('data-path') === path);
    });
  }

  /**
   * Create file
   * @param {Object} data - File data
   */
  async createFile(data) {
    try {
      await api.request('/workspace/files', {
        method: 'POST',
        body: JSON.stringify(data),
      });
      await this.loadFileTree();
      Toast.success(`已创建文件: ${data.name}`);
    } catch (e) {
      Toast.error(`创建文件失败: ${e.message}`);
    }
  }

  /**
   * Delete file
   * @param {string} path - File path
   */
  async deleteFile(path) {
    if (!confirm(`确定要删除 ${path} 吗?`)) return;

    try {
      await api.request(`/workspace/files?path=${encodeURIComponent(path)}`, {
        method: 'DELETE',
      });
      await this.loadFileTree();
      Toast.success('文件已删除');
    } catch (e) {
      Toast.error(`删除文件失败: ${e.message}`);
    }
  }

  /**
   * Rename file
   * @param {string} oldPath - Original path
   * @param {string} newPath - New path
   */
  async renameFile(oldPath, newPath) {
    try {
      await api.request('/workspace/files/rename', {
        method: 'PUT',
        body: JSON.stringify({ oldPath, newPath }),
      });
      await this.loadFileTree();
      Toast.success('文件已重命名');
    } catch (e) {
      Toast.error(`重命名失败: ${e.message}`);
    }
  }

  /**
   * Show file context menu
   * @param {Event} event - Mouse event
   * @param {Object} file - File object
   */
  showContextMenu(event, file) {
    // Remove existing menu
    const existing = document.getElementById('fileContextMenu');
    if (existing) existing.remove();

    const menu = document.createElement('div');
    menu.id = 'fileContextMenu';
    menu.className = 'context-menu';
    menu.style.left = `${event.clientX}px`;
    menu.style.top = `${event.clientY}px`;

    const items = [
      { label: '打开', action: () => this.openFile(file.path) },
      { label: '重命名', action: () => this.showRenameDialog(file) },
      { type: 'separator' },
      { label: '删除', action: () => this.deleteFile(file.path), danger: true },
    ];

    items.forEach(item => {
      if (item.type === 'separator') {
        menu.appendChild(document.createElement('hr'));
      } else {
        const btn = document.createElement('button');
        btn.textContent = item.label;
        if (item.danger) btn.classList.add('danger');
        btn.addEventListener('click', () => {
          item.action();
          menu.remove();
        });
        menu.appendChild(btn);
      }
    });

    document.body.appendChild(menu);

    // Close on click outside
    const closeMenu = (e) => {
      if (!menu.contains(e.target)) {
        menu.remove();
        document.removeEventListener('click', closeMenu);
      }
    };
    setTimeout(() => document.addEventListener('click', closeMenu), 0);
  }

  /**
   * Show rename dialog
   * @param {Object} file - File object
   */
  showRenameDialog(file) {
    const newName = prompt('输入新名称:', file.name);
    if (newName && newName !== file.name) {
      const newPath = file.path.replace(file.name, newName);
      this.renameFile(file.path, newPath);
    }
  }

  /**
   * Handle workspace change event
   * @param {Object} workspace - New workspace
   */
  onWorkspaceChange(workspace) {
    this.currentWorkspace = workspace;
    appState.set('workspace', workspace);
    this.loadFileTree();
  }

  /**
   * Refresh current workspace
   */
  async refresh() {
    await this.getCurrentWorkspace();
    await this.loadFileTree();
  }
}

// Create global instance
const workspaceManager = new WorkspaceManager();

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { WorkspaceManager, workspaceManager };
}

// Export to window
window.workspaceManager = workspaceManager;
