/**
 * Cowd File Upload - Drag-drop & button file upload manager
 *
 * Supports uploading files to the workspace via multipart/form-data,
 * with size limits, dangerous extension filtering, and drag-drop UI.
 */

const FileUploadManager = {
  maxSize: 20 * 1024 * 1024, // 20MB
  dragCounter: 0,

  init() {
    const messagesEl = document.getElementById('messages');
    const inputArea = document.getElementById('inputArea');

    // Drag-drop on messages area
    if (messagesEl) {
      messagesEl.addEventListener('dragenter', (e) => this._onDragEnter(e));
      messagesEl.addEventListener('dragover', (e) => this._onDragOver(e));
      messagesEl.addEventListener('dragleave', (e) => this._onDragLeave(e));
      messagesEl.addEventListener('drop', (e) => this._onDrop(e));
    }

    // Attach button
    const attachBtn = document.getElementById('attachBtn');
    if (attachBtn) {
      attachBtn.addEventListener('click', () => this._openFileDialog());
    }
  },

  _onDragEnter(e) {
    e.preventDefault();
    this.dragCounter++;
    const messagesEl = document.getElementById('messages');
    if (messagesEl) messagesEl.classList.add('drag-over');
  },

  _onDragOver(e) {
    e.preventDefault();
  },

  _onDragLeave(e) {
    e.preventDefault();
    this.dragCounter--;
    if (this.dragCounter === 0) {
      const messagesEl = document.getElementById('messages');
      if (messagesEl) messagesEl.classList.remove('drag-over');
    }
  },

  async _onDrop(e) {
    e.preventDefault();
    this.dragCounter = 0;
    const messagesEl = document.getElementById('messages');
    if (messagesEl) messagesEl.classList.remove('drag-over');

    const files = Array.from(e.dataTransfer.files);
    for (const file of files) {
      await this.uploadFile(file);
    }
  },

  _openFileDialog() {
    const input = document.createElement('input');
    input.type = 'file';
    input.multiple = true;
    input.onchange = async (e) => {
      const files = Array.from(e.target.files);
      for (const file of files) {
        await this.uploadFile(file);
      }
    };
    input.click();
  },

  async uploadFile(file) {
    // Size check
    if (file.size > this.maxSize) {
      window.Toast?.error(
        `文件过大: ${file.name} (${(file.size / 1024 / 1024).toFixed(1)}MB > 20MB)`
      );
      return null;
    }

    // Show upload progress
    const progressEl = this._showUploadProgress(file);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const token = window.api?.token || '';
      const response = await fetch('/api/upload', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
        },
        body: formData,
      });

      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        throw new Error(error.error || `Upload failed: HTTP ${response.status}`);
      }

      const result = await response.json();
      this._showUploadComplete(progressEl, result);

      // Append attachment marker to input
      const inputArea = document.getElementById('inputArea');
      if (inputArea) {
        const current = inputArea.value;
        if (!current.includes(result.filename)) {
          inputArea.value = current + `\n[Attached: ${result.filename}]`;
        }
      }

      return result;
    } catch (e) {
      this._showUploadError(progressEl, e.message);
      return null;
    }
  },

  _showUploadProgress(file) {
    const messagesEl = document.getElementById('messages');
    if (!messagesEl) return null;

    const el = document.createElement('div');
    el.className = 'upload-item';
    el.innerHTML = `
      <span class="upload-icon">\uD83D\uDCC4</span>
      <span class="upload-name">${this._escapeHtml(file.name)}</span>
      <span class="upload-size">${(file.size / 1024).toFixed(1)}KB</span>
      <div class="upload-progress"><div class="progress-bar" style="width:30%"></div></div>
      <span class="upload-status">Uploading...</span>
    `;
    messagesEl.appendChild(el);
    messagesEl.scrollTop = messagesEl.scrollHeight;
    return el;
  },

  _showUploadComplete(el, result) {
    if (!el) return;
    const bar = el.querySelector('.progress-bar');
    if (bar) bar.style.width = '100%';
    const status = el.querySelector('.upload-status');
    if (status) status.textContent = 'Done';
    el.classList.add('upload-complete');

    // Auto-remove after 3 seconds
    setTimeout(() => {
      if (el.parentNode) {
        el.style.transition = 'opacity 0.3s ease';
        el.style.opacity = '0';
        setTimeout(() => el.remove(), 300);
      }
    }, 3000);
  },

  _showUploadError(el, message) {
    if (!el) return;
    const status = el.querySelector('.upload-status');
    if (status) status.textContent = `Error: ${message}`;
    el.classList.add('upload-error');
  },

  _escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
};

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
  FileUploadManager.init();
});

// Export
window.FileUploadManager = FileUploadManager;
