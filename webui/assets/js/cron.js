/**
 * Cowd Cron - Cron Job Management Module (P1-5)
 * Provides CRUD UI for scheduled tasks with 4 time format support.
 */

const Cron = {
  container: null,

  init() {
    this.container = document.getElementById('panelCron');
    if (!this.container) return;
    this.bindEvents();
  },

  bindEvents() {
    // Create form submit
    const form = this.container.querySelector('#cron-create-form');
    if (form) {
      form.addEventListener('submit', (e) => {
        e.preventDefault();
        this.createCron();
      });
    }

    // Delegated clicks for job actions
    this.container.addEventListener('click', (e) => {
      const btn = e.target.closest('[data-action]');
      if (!btn) return;
      const action = btn.dataset.action;
      const id = btn.closest('[data-cron-id]')?.dataset.cronId;
      if (!id) return;

      switch (action) {
        case 'run': this.runCron(id); break;
        case 'pause': this.pauseCron(id); break;
        case 'resume': this.resumeCron(id); break;
        case 'delete': this.deleteCron(id); break;
      }
    });
  },

  async loadCrons() {
    if (!this.container) return;
    try {
      const result = await window.api.listCrons();
      this.renderCrons(result.jobs || []);
    } catch (e) {
      console.error('Failed to load cron jobs:', e);
      this.renderError('Failed to load cron jobs');
    }
  },

  renderCrons(jobs) {
    const list = this.container.querySelector('#cron-list');
    if (!list) return;

    if (jobs.length === 0) {
      list.innerHTML = '<div class="cron-empty">No cron jobs configured. Create one below.</div>';
      return;
    }

    list.innerHTML = jobs.map(job => `
      <div class="cron-item ${job.enabled ? '' : 'cron-paused'}" data-cron-id="${job.id}">
        <div class="cron-item-header">
          <span class="cron-name">${this.escapeHtml(job.name)}</span>
          <span class="cron-schedule-badge">${this.escapeHtml(job.schedule.toString())}</span>
          <span class="cron-status ${job.enabled ? 'status-active' : 'status-paused'}">
            ${job.enabled ? 'Active' : 'Paused'}
          </span>
        </div>
        <div class="cron-item-body">
          <div class="cron-prompt">${this.escapeHtml(job.prompt)}</div>
          <div class="cron-meta">
            ${job.next_run_at ? `<span class="cron-next">Next: ${this.formatTime(job.next_run_at)}</span>` : '<span class="cron-next">No next run</span>'}
            ${job.last_run_at ? `<span class="cron-last">Last: ${this.formatTime(job.last_run_at)}</span>` : ''}
            <span class="cron-runs">Runs: ${job.run_count}</span>
            <span class="cron-grace">Grace: ${job.grace_window_secs}s</span>
          </div>
        </div>
        <div class="cron-item-actions">
          <button class="btn btn-sm btn-primary" data-action="run" title="Run now">Run</button>
          ${job.enabled
            ? '<button class="btn btn-sm btn-warning" data-action="pause" title="Pause">Pause</button>'
            : '<button class="btn btn-sm btn-success" data-action="resume" title="Resume">Resume</button>'
          }
          <button class="btn btn-sm btn-danger" data-action="delete" title="Delete">Delete</button>
        </div>
      </div>
    `).join('');
  },

  async createCron() {
    const name = this.container.querySelector('#cron-name')?.value?.trim();
    const schedule = this.container.querySelector('#cron-schedule')?.value?.trim();
    const prompt = this.container.querySelector('#cron-prompt')?.value?.trim();
    const grace = parseInt(this.container.querySelector('#cron-grace')?.value) || 60;

    if (!name || !schedule || !prompt) {
      this.showStatus('Please fill in name, schedule, and prompt.', 'error');
      return;
    }

    try {
      await window.api.createCron({ name, schedule, prompt, grace_window_secs: grace });
      this.showStatus('Cron job created successfully.', 'success');
      // Clear form
      const form = this.container.querySelector('#cron-create-form');
      if (form) form.reset();
      this.loadCrons();
    } catch (e) {
      this.showStatus('Failed to create cron: ' + (e.message || e), 'error');
    }
  },

  async runCron(id) {
    try {
      await window.api.runCron(id);
      this.showStatus('Cron job triggered.', 'success');
      this.loadCrons();
    } catch (e) {
      this.showStatus('Failed to run cron: ' + (e.message || e), 'error');
    }
  },

  async pauseCron(id) {
    try {
      await window.api.pauseCron(id);
      this.showStatus('Cron job paused.', 'success');
      this.loadCrons();
    } catch (e) {
      this.showStatus('Failed to pause cron: ' + (e.message || e), 'error');
    }
  },

  async resumeCron(id) {
    try {
      await window.api.resumeCron(id);
      this.showStatus('Cron job resumed.', 'success');
      this.loadCrons();
    } catch (e) {
      this.showStatus('Failed to resume cron: ' + (e.message || e), 'error');
    }
  },

  async deleteCron(id) {
    if (!confirm('Delete this cron job?')) return;
    try {
      await window.api.deleteCron(id);
      this.showStatus('Cron job deleted.', 'success');
      this.loadCrons();
    } catch (e) {
      this.showStatus('Failed to delete cron: ' + (e.message || e), 'error');
    }
  },

  showStatus(msg, type) {
    const el = this.container.querySelector('#cron-status');
    if (!el) return;
    el.textContent = msg;
    el.className = `cron-status-msg ${type}`;
    setTimeout(() => { el.textContent = ''; el.className = 'cron-status-msg'; }, 3000);
  },

  renderError(msg) {
    const list = this.container.querySelector('#cron-list');
    if (list) list.innerHTML = `<div class="cron-error">${this.escapeHtml(msg)}</div>`;
  },

  formatTime(isoStr) {
    try {
      const d = new Date(isoStr);
      return d.toLocaleString();
    } catch {
      return isoStr;
    }
  },

  escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }
};

document.addEventListener('DOMContentLoaded', () => {
  Cron.init();
});

window.Cron = Cron;
