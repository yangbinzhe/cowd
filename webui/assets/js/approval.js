/**
 * Cowd Approval - Dangerous Command Approval System
 *
 * Handles approval workflow for destructive/dangerous commands detected
 * by the backend DestructivePatternDetector. Approval requests are pushed
 * via SSE, displayed as interactive cards with countdown timers, risk-level
 * coloring, and persistence selection.
 */

// ═══════════════════════════════════════════════════════════════════════════
// ApprovalCard - Individual approval request UI component
// ═══════════════════════════════════════════════════════════════════════════

class ApprovalCard {
  /**
   * @param {Object} request - The approval request from backend
   * @param {string} request.id - Unique request ID
   * @param {string} request.command - The original command text
   * @param {string} request.normalized_command - Normalized command
   * @param {number} request.risk_level - Risk level (0=Low,1=Medium,2=High,3=Critical)
   * @param {string[]} request.matched_patterns - List of matched pattern names
   * @param {string} request.description - Human-readable description
   * @param {number} request.timeout_secs - Timeout in seconds
   * @param {Function} onRespond - Callback when user responds (requestId, verdict, persistence)
   */
  constructor(request, onRespond) {
    this.request = request;
    this.onRespond = onRespond;
    this.element = null;
    this.countdownInterval = null;
    this.safetyLockTimeout = null;
    this.remainingSeconds = request.timeout_secs;
    this.safetyLockRemaining = 3; // 3-second safety lock
    this.responded = false;

    this._create();
    this._startCountdown();
    this._startSafetyLock();
  }

  /** Risk level display configuration */
  static get RISK_CONFIG() {
    return {
      0: { label: '低风险', cssClass: 'risk-low', color: 'var(--green)', icon: '\u26A0' },
      1: { label: '中风险', cssClass: 'risk-medium', color: 'var(--gold)', icon: '\u26A0' },
      2: { label: '高风险', cssClass: 'risk-high', color: 'var(--accent)', icon: '\u2757' },
      3: { label: '严重风险', cssClass: 'risk-critical', color: 'var(--red)', icon: '\uD83D\uDEA8' },
    };
  }

  /** Create the card DOM element */
  _create() {
    const risk = ApprovalCard.RISK_CONFIG[this.request.risk_level] || ApprovalCard.RISK_CONFIG[0];
    const _t = (key, fallback) => window.i18nInstance?.t(key) || fallback;

    const card = document.createElement('div');
    card.className = `approval-card ${risk.cssClass}`;
    card.dataset.requestId = this.request.id;

    card.innerHTML = `
      <div class="approval-header">
        <span class="approval-risk-badge ${risk.cssClass}">
          <span class="risk-icon">${risk.icon}</span>
          <span class="risk-label">${risk.label}</span>
        </span>
        <span class="approval-countdown" data-timer="${this.request.timeout_secs}">
          ${this._formatTime(this.remainingSeconds)}
        </span>
      </div>
      <div class="approval-command">
        <code>${this._escapeHtml(this.request.command)}</code>
      </div>
      <div class="approval-description">${this._escapeHtml(this.request.description)}</div>
      <div class="approval-patterns">
        ${this.request.matched_patterns.map(p => `<span class="pattern-tag">${this._escapeHtml(p)}</span>`).join('')}
      </div>
      <div class="approval-persistence">
        <label>${_t('approval.persistence', '批准范围')}:</label>
        <select class="persistence-select">
          <option value="once">${_t('approval.once', '仅此次')}</option>
          <option value="session">${_t('approval.session', '本次会话')}</option>
          <option value="always">${_t('approval.always', '永久允许')}</option>
        </select>
      </div>
      <div class="approval-actions">
        <button class="btn approval-deny-btn" disabled data-action="deny">
          <span class="safety-lock-text">${_t('approval.safetyLock', '等待')} (${this.safetyLockRemaining}s)</span>
          ${_t('approval.deny', '拒绝')}
        </button>
        <button class="btn approval-approve-btn" disabled data-action="approve">
          <span class="safety-lock-text">${_t('approval.safetyLock', '等待')} (${this.safetyLockRemaining}s)</span>
          ${_t('approval.approve', '批准')}
        </button>
      </div>
      <div class="approval-progress">
        <div class="approval-progress-bar" style="width: 100%"></div>
      </div>
    `;

    // Bind button handlers (will be enabled after safety lock)
    const approveBtn = card.querySelector('[data-action="approve"]');
    const denyBtn = card.querySelector('[data-action="deny"]');

    approveBtn.addEventListener('click', () => this.respond('Approved'));
    denyBtn.addEventListener('click', () => this.respond('Denied'));

    this.element = card;
  }

  /** 3-second safety lock to prevent accidental approval */
  _startSafetyLock() {
    const approveBtn = this.element.querySelector('[data-action="approve"]');
    const denyBtn = this.element.querySelector('[data-action="deny"]');

    this.safetyLockTimeout = setInterval(() => {
      this.safetyLockRemaining--;

      if (this.safetyLockRemaining <= 0) {
        clearInterval(this.safetyLockTimeout);
        // Enable buttons
        approveBtn.disabled = false;
        denyBtn.disabled = false;
        // Remove safety lock text
        approveBtn.querySelector('.safety-lock-text').textContent = '';
        denyBtn.querySelector('.safety-lock-text').textContent = '';
      } else {
        approveBtn.querySelector('.safety-lock-text').textContent = `(${this.safetyLockRemaining}s) `;
        denyBtn.querySelector('.safety-lock-text').textContent = `(${this.safetyLockRemaining}s) `;
      }
    }, 1000);
  }

  /** Start countdown timer */
  _startCountdown() {
    const countdownEl = this.element.querySelector('.approval-countdown');
    const progressBar = this.element.querySelector('.approval-progress-bar');
    const totalSeconds = this.request.timeout_secs;

    this.countdownInterval = setInterval(() => {
      this.remainingSeconds--;

      if (this.remainingSeconds <= 0) {
        this._onTimeout();
        return;
      }

      // Update countdown display
      countdownEl.textContent = this._formatTime(this.remainingSeconds);

      // Update progress bar
      const pct = (this.remainingSeconds / totalSeconds) * 100;
      progressBar.style.width = `${pct}%`;

      // Color transition: green -> yellow -> red
      if (this.remainingSeconds <= 10) {
        countdownEl.classList.add('urgent');
        progressBar.classList.add('urgent');
      } else if (this.remainingSeconds <= 30) {
        countdownEl.classList.add('warning');
        progressBar.classList.add('warning');
      }
    }, 1000);
  }

  /** Handle timeout */
  _onTimeout() {
    this.cleanup();
    this.element.classList.add('timed-out');
    const _t = (key, fallback) => window.i18nInstance?.t(key) || fallback;
    this.element.querySelector('.approval-actions').innerHTML =
      `<div class="approval-timed-out">${_t('approval.timedOut', '已超时，命令已被自动拒绝')}</div>`;
  }

  /** Respond to the approval request */
  respond(verdict) {
    if (this.responded) return;
    this.responded = true;

    const persistenceSelect = this.element.querySelector('.persistence-select');
    const persistence = persistenceSelect ? persistenceSelect.value : 'once';

    // Update UI
    this.cleanup();

    if (verdict === 'Approved') {
      this.element.classList.add('approved');
      this.element.querySelector('.approval-actions').innerHTML =
        '<div class="approval-responded approved-text">\u2705 已批准</div>';
    } else {
      this.element.classList.add('denied');
      this.element.querySelector('.approval-actions').innerHTML =
        '<div class="approval-responded denied-text">\u274C 已拒绝</div>';
    }

    // Call the callback
    if (this.onRespond) {
      this.onRespond(this.request.id, verdict, persistence);
    }

    // Auto-remove after 3 seconds
    setTimeout(() => {
      if (this.element && this.element.parentNode) {
        this.element.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
        this.element.style.opacity = '0';
        this.element.style.transform = 'translateX(20px)';
        setTimeout(() => this.element.remove(), 300);
      }
    }, 3000);
  }

  /** Clean up intervals and timeouts */
  cleanup() {
    if (this.countdownInterval) {
      clearInterval(this.countdownInterval);
      this.countdownInterval = null;
    }
    if (this.safetyLockTimeout) {
      clearInterval(this.safetyLockTimeout);
      this.safetyLockTimeout = null;
    }
  }

  /** Format seconds to MM:SS */
  _formatTime(seconds) {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return `${m}:${s.toString().padStart(2, '0')}`;
  }

  /** Escape HTML entities */
  _escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  /** Destroy the card completely */
  destroy() {
    this.cleanup();
    if (this.element && this.element.parentNode) {
      this.element.remove();
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// ApprovalManager - Manages all active approval cards
// ═══════════════════════════════════════════════════════════════════════════

const ApprovalManager = {
  /** @type {Map<string, ApprovalCard>} Active approval cards by request ID */
  cards: new Map(),

  /** @type {HTMLElement} Container for approval cards in the chat */
  container: null,

  /** Initialize the approval system */
  init() {
    // Find or create the approval container inside messages
    this.container = document.getElementById('approvalContainer');
    if (!this.container) {
      this.container = document.createElement('div');
      this.container.id = 'approvalContainer';
      this.container.className = 'approval-container';
      // Insert after messages container
      const messages = document.getElementById('messages');
      if (messages) {
        messages.parentNode.insertBefore(this.container, messages.nextSibling);
      }
    }
  },

  /**
   * Handle an incoming approval request from SSE
   * @param {Object} request - The approval request data
   */
  handleApprovalRequest(request) {
    if (!this.container) this.init();

    // Check if we already have this request
    if (this.cards.has(request.id)) {
      return;
    }

    const card = new ApprovalCard(request, (requestId, verdict, persistence) => {
      this._sendResponse(requestId, verdict, persistence);
      // 3C-4: Record to history
      this.recordHistory(requestId, verdict, persistence, request.command, request.risk_level);
    });

    this.cards.set(request.id, card);

    // Insert card into the messages area (before composer)
    const messagesEl = document.getElementById('messages');
    if (messagesEl) {
      messagesEl.appendChild(card.element);
      // Scroll to see the approval
      messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    // Also show a toast notification
    window.Toast?.info(
      `${ApprovalCard.RISK_CONFIG[request.risk_level]?.label || '未知风险'}: 需要批准执行命令`,
      5000
    );

    // Play notification sound if available
    this._notifyUser(request);
  },

  /**
   * Send approval response to the backend API
   */
  async _sendResponse(requestId, verdict, persistence) {
    try {
      await window.api?.respondApproval(requestId, verdict, persistence);
      this.cards.delete(requestId);
    } catch (error) {
      console.error('[Cowd Approval] Failed to send response:', error);
      window.Toast?.error('批准响应发送失败: ' + (error.message || '未知错误'));
    }
  },

  /**
   * Notify user via browser notification API
   */
  _notifyUser(request) {
    if (!('Notification' in window)) return;

    if (Notification.permission === 'granted') {
      new Notification('Cowd - 需要命令批准', {
        body: `${request.command.substring(0, 100)}`,
        tag: request.id,
      });
    } else if (Notification.permission !== 'denied') {
      Notification.requestPermission();
    }
  },

  /**
   * Remove all active approval cards
   */
  clearAll() {
    for (const [id, card] of this.cards) {
      card.destroy();
    }
    this.cards.clear();
  },

  /**
   * Get count of pending approvals
   */
  getPendingCount() {
    return this.cards.size;
  },

  // ═══════════════════════════════════════════════════════════════════
  // 3C-4: Approval History
  // ═══════════════════════════════════════════════════════════════════

  /** @type {Array} Historical approval records */
  history: [],

  /** Record an approval response into history */
  recordHistory(requestId, verdict, persistence, command, riskLevel) {
    this.history.unshift({
      id: requestId,
      verdict,
      persistence,
      command: command || '',
      risk_level: riskLevel || 0,
      timestamp: new Date().toISOString()
    });

    // Keep last 50 entries
    if (this.history.length > 50) {
      this.history = this.history.slice(0, 50);
    }
  },

  /** Get approval history */
  getHistory() {
    return this.history;
  },

  /** Get approval statistics */
  getStats() {
    const total = this.history.length;
    const approved = this.history.filter(h => h.verdict === 'Approved').length;
    const denied = total - approved;
    const byRisk = [0, 1, 2, 3].map(level => ({
      level,
      count: this.history.filter(h => h.risk_level === level).length
    }));
    return { total, approved, denied, byRisk };
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// Initialize on DOM ready
// ═══════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
  ApprovalManager.init();
});

// Export to global scope
window.ApprovalCard = ApprovalCard;
window.ApprovalManager = ApprovalManager;
