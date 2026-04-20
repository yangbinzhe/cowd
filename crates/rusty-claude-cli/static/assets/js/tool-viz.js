/**
 * Cowd Tool Visualization - Tool execution cards + Thinking filter
 *
 * Provides real-time visualization of tool execution via SSE events,
 * and filters thinking/reasoning tags from streamed content.
 *
 * Inspired by hermes-agent stream_consumer.py (thinking tag filter)
 * and hermes-webui messages.js (tool card lifecycle).
 */

// ═══════════════════════════════════════════════════════════════════════════
// ToolCard - Individual tool execution visualization card
// ═══════════════════════════════════════════════════════════════════════════

class ToolCard {
  /** Tool name to icon mapping */
  static get ICONS() {
    return {
      'execute_bash': '\u2699\uFE0F',
      'bash': '\u2699\uFE0F',
      'shell': '\u2699\uFE0F',
      'read_file': '\uD83D\uDCC4',
      'write_file': '\u270F\uFE0F',
      'edit_file': '\u270F\uFE0F',
      'grep': '\uD83D\uDD0D',
      'search': '\uD83D\uDD0D',
      'list_files': '\uD83D\uDCC2',
      'web_fetch': '\uD83C\uDF10',
      'calculator': '\uD83E\uDDEE',
      'default': '\uD83D\uDD27',
    };
  }

  /**
   * @param {string} id - Tool execution ID
   * @param {string} name - Tool name
   * @param {string} preview - Command preview text
   */
  constructor(id, name, preview) {
    this.id = id;
    this.name = name;
    this.state = 'running'; // running | complete | failed
    this.expanded = true;   // Running: expanded, Complete: auto-collapse
    this.element = this._create(preview);
  }

  /** Create the card DOM element */
  _create(preview) {
    const icon = ToolCard.ICONS[this.name] || ToolCard.ICONS.default;
    const _t = (key, fallback) => window.i18nInstance?.t(key) || fallback;

    const card = document.createElement('div');
    card.className = 'tool-card running';
    card.dataset.toolId = this.id;

    // Truncate preview for display
    const displayPreview = preview.length > 200 ? preview.substring(0, 200) + '...' : preview;

    card.innerHTML = `
      <div class="tool-header">
        <span class="tool-icon">${icon}</span>
        <span class="tool-name">${this._escapeHtml(this.name)}</span>
        <span class="tool-status">
          <span class="tool-spinner"></span>
          <span class="status-text">${_t('tool.running', 'Running...')}</span>
        </span>
        <span class="tool-toggle">\u25BC</span>
      </div>
      <div class="tool-body">
        <div class="tool-preview">
          <div class="preview-label">${_t('tool.command', 'Command')}:</div>
          <pre><code>${this._escapeHtml(displayPreview)}</code></pre>
        </div>
        <div class="tool-progress" style="display:none;"></div>
        <div class="tool-result" style="display:none;">
          <div class="result-label">${_t('tool.result', 'Result')}:</div>
          <pre><code></code></pre>
        </div>
      </div>
    `;

    // Click header to toggle expand/collapse
    const header = card.querySelector('.tool-header');
    header.addEventListener('click', () => this.toggleExpand());

    return card;
  }

  /** Toggle expand/collapse */
  toggleExpand() {
    this.expanded = !this.expanded;
    const body = this.element.querySelector('.tool-body');
    const toggle = this.element.querySelector('.tool-toggle');
    body.style.display = this.expanded ? 'block' : 'none';
    toggle.textContent = this.expanded ? '\u25BC' : '\u25B6';
  }

  /** Update progress text */
  updateProgress(text) {
    const progressEl = this.element.querySelector('.tool-progress');
    progressEl.style.display = 'block';
    progressEl.textContent = text;
    this.element.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  /**
   * Set the tool result and mark as complete/failed
   * @param {string} summary - Result summary text
   * @param {number|null} exitCode - Exit code (0=success, non-zero=failure)
   */
  setResult(summary, exitCode) {
    this.state = exitCode === 0 || exitCode === null ? 'complete' : 'failed';
    this.element.className = `tool-card ${this.state}`;

    // Update status display
    const statusEl = this.element.querySelector('.tool-status');
    const statusText = this.element.querySelector('.status-text');
    const spinner = this.element.querySelector('.tool-spinner');
    if (spinner) spinner.remove();

    const _t = (key, fallback) => window.i18nInstance?.t(key) || fallback;
    if (this.state === 'complete') {
      statusText.textContent = _t('tool.completed', 'Completed');
    } else {
      statusText.textContent = _t('tool.failed', 'Failed') + ` (exit ${exitCode})`;
    }

    // Set result text (truncate if too long)
    const resultEl = this.element.querySelector('.tool-result');
    resultEl.style.display = 'block';
    const codeEl = resultEl.querySelector('code');
    const displayText = summary.length > 2000
      ? summary.substring(0, 2000) + '\n... (truncated)'
      : summary;
    codeEl.textContent = displayText;

    // Auto-collapse after completion (inspired by hermes-webui)
    if (this.expanded) {
      setTimeout(() => this.toggleExpand(), 500);
    }
  }

  /** Escape HTML */
  _escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// ToolCardManager - Manages all active tool cards in the chat
// ═══════════════════════════════════════════════════════════════════════════

const ToolCardManager = {
  /** @type {Map<string, ToolCard>} Active tool cards by tool execution ID */
  cards: new Map(),

  /**
   * Handle tool_start SSE event
   * @param {Object} data - {id, name, preview}
   */
  handleToolStart(data) {
    if (!data || !data.id) return;
    if (this.cards.has(data.id)) return; // Already exists

    const card = new ToolCard(data.id, data.name, data.preview || '');

    this.cards.set(data.id, card);

    // Append to messages container
    const messagesEl = document.getElementById('messages');
    if (messagesEl) {
      messagesEl.appendChild(card.element);
      messagesEl.scrollTop = messagesEl.scrollHeight;
    }
  },

  /**
   * Handle tool_progress SSE event
   * @param {Object} data - {id, name, progress}
   */
  handleToolProgress(data) {
    if (!data || !data.id) return;
    const card = this.cards.get(data.id);
    if (card) {
      card.updateProgress(data.progress || '');
    }
  },

  /**
   * Handle tool_complete SSE event
   * @param {Object} data - {id, name, result_summary, exit_code}
   */
  handleToolComplete(data) {
    if (!data || !data.id) return;
    const card = this.cards.get(data.id);
    if (card) {
      card.setResult(data.result_summary || '', data.exit_code);
      this.cards.delete(data.id);
    }
  },

  /** Clear all active tool cards */
  clearAll() {
    this.cards.clear();
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// ThinkingFilter - Filters thinking/reasoning tags from streamed content
// Inspired by hermes-agent stream_consumer.py 5-pair tag filter
// ═══════════════════════════════════════════════════════════════════════════

const THINKING_TAGS = [
  { open: '<thinking>', close: '</thinking>' },
  { open: '\u{1F4AD}', close: '\u{1F4AC}' },     // 💭 ... 💬
  { open: '<reflection>', close: '</reflection>' },
  { open: '<reasoning>', close: '</reasoning>' },
  { open: '<scratchpad>', close: '</scratchpad>' },
];

class ThinkingFilter {
  constructor() {
    this.state = 'outside'; // outside | inside
    this.buffer = '';
    this.depth = 0;
  }

  /**
   * Process incoming text chunk, filtering out thinking blocks.
   * @param {string} text - Incoming text chunk
   * @returns {{output: string, thinkingBuffer: string}} - Filtered output + captured thinking
   */
  process(text) {
    let output = '';
    let remaining = text;

    while (remaining.length > 0) {
      if (this.state === 'outside') {
        // Check for opening tags
        let foundOpen = false;
        for (const tag of THINKING_TAGS) {
          if (remaining.startsWith(tag.open)) {
            this.state = 'inside';
            this.depth++;
            remaining = remaining.slice(tag.open.length);
            foundOpen = true;
            break;
          }
        }
        if (!foundOpen) {
          // Check for partial tag match at end
          let partialMatch = false;
          for (const tag of THINKING_TAGS) {
            if (tag.open.startsWith(remaining) && remaining.length < tag.open.length) {
              partialMatch = true;
              break;
            }
          }
          if (partialMatch) {
            this.buffer += remaining;
            break;
          }
          output += remaining[0];
          remaining = remaining.slice(1);
        }
      } else {
        // Inside thinking block
        let foundClose = false;
        for (const tag of THINKING_TAGS) {
          if (remaining.startsWith(tag.close)) {
            this.depth--;
            if (this.depth === 0) {
              this.state = 'outside';
            }
            remaining = remaining.slice(tag.close.length);
            foundClose = true;
            break;
          }
          if (remaining.startsWith(tag.open)) {
            this.depth++;
            remaining = remaining.slice(tag.open.length);
            foundClose = true;
            break;
          }
        }
        if (!foundClose) {
          this.buffer += remaining[0];
          remaining = remaining.slice(1);
        }
      }
    }

    return { output, thinkingBuffer: this.buffer };
  }

  /** Reset the filter state */
  reset() {
    this.state = 'outside';
    this.buffer = '';
    this.depth = 0;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// Initialize on DOM ready
// ═══════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
  // ToolCardManager doesn't need explicit init, it works on-demand
});

// Export to global scope
window.ToolCard = ToolCard;
window.ToolCardManager = ToolCardManager;
window.ThinkingFilter = ThinkingFilter;
window.THINKING_TAGS = THINKING_TAGS;
