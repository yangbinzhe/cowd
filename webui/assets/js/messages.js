/**
 * Cowd Messages - Chat Messages Module
 */

// Message Renderer
const Messages = {
  container: null,
  abortController: null,

  init() {
    this.container = document.getElementById('messages');
    if (!this.container) return;

    // Bind send button
    const sendBtn = document.getElementById('sendBtn');
    const inputArea = document.getElementById('inputArea');

    if (sendBtn) {
      sendBtn.addEventListener('click', () => this.send());
    }

    if (inputArea) {
      // Auto-resize
      inputArea.addEventListener('input', () => {
        inputArea.style.height = 'auto';
        inputArea.style.height = Math.min(inputArea.scrollHeight, 200) + 'px';
      });

      // Send on Enter (without Shift)
      inputArea.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          this.send();
        }
      });
    }

    // Bind new chat button
    const newChatBtn = document.getElementById('newChatBtn');
    if (newChatBtn) {
      newChatBtn.addEventListener('click', () => this.newChat());
    }

    // Bind suggestions
    document.querySelectorAll('.suggestion').forEach(el => {
      el.addEventListener('click', () => {
        const prompt = el.dataset.prompt;
        if (prompt) {
          inputArea.value = prompt;
          this.send();
        }
      });
    });

    // Update send button state
    state.subscribe('isStreaming', (isStreaming) => {
      const sendBtn = document.getElementById('sendBtn');
      if (sendBtn) {
        sendBtn.disabled = isStreaming;
      }
    });
  },

  async send() {
    const inputArea = document.getElementById('inputArea');
    const sendBtn = document.getElementById('sendBtn');

    if (!inputArea || !inputArea.value.trim()) return;

    // Check authentication
    if (!api.isAuthenticated()) {
      this.showLoginModal();
      return;
    }

    const content = inputArea.value.trim();
    inputArea.value = '';
    inputArea.style.height = 'auto';

    // Create or use current session
    let session = state.get('currentSession');
    if (!session) {
      try {
        session = await api.createSession();
        state.set('currentSession', session);
        state.update('sessions', sessions => [session, ...(sessions || [])]);
        Sessions.renderSessions();
      } catch (error) {
        Toast.error('创建会话失败');
        return;
      }
    }

    // Add user message
    this.addMessage({
      role: 'user',
      content
    });

    // Show thinking indicator
    this.showThinking();

    // Update button state
    state.set('isStreaming', true);

    // Cancel any existing stream
    if (this.abortController) {
      this.abortController.abort();
    }
    this.abortController = new AbortController();

    try {
      const result = await api.sendMessage(session.id, content, {
        signal: this.abortController.signal,
        onChunk: (chunk, fullContent) => {
          this.updateAssistantMessage(fullContent);
        },
        onComplete: (fullContent, data) => {
          this.hideThinking();
          this.updateAssistantMessage(fullContent, true);

          // Add to messages state
          const assistantMsg = {
            role: 'assistant',
            content: fullContent,
            timestamp: new Date().toISOString()
          };

          state.update('messages', msgs => [...(msgs || []), assistantMsg]);

          // Update context indicator
          if (data.usage) {
            this.updateContextIndicator(data.usage);
          }
        },
        onError: (error) => {
          this.hideThinking();
          this.showError(error.message);
        }
      });
    } catch (error) {
      if (error.name !== 'AbortError') {
        this.hideThinking();
        this.showError(error.message || '发送消息失败');
      }
    } finally {
      state.set('isStreaming', false);
    }
  },

  addMessage(msg) {
    if (!this.container) return;

    // Remove welcome message if exists
    const welcome = this.container.querySelector('.welcome-message');
    if (welcome) {
      welcome.remove();
    }

    const messageEl = this.createMessageElement(msg);
    this.container.appendChild(messageEl);
    this.scrollToBottom();

    // Update state
    state.update('messages', msgs => [...(msgs || []), msg]);
  },

  createMessageElement(msg) {
    const div = document.createElement('div');
    div.className = `message ${msg.role}`;

    const initials = msg.role === 'user' ? 'U' : 'AI';
    const roleName = msg.role === 'user' ? '你' : 'Cowd';

    div.innerHTML = `
      <div class="message-avatar">${initials}</div>
      <div class="message-content">
        <div class="message-role">${roleName}</div>
        <div class="message-text">${this.formatContent(msg.content)}</div>
      </div>
    `;

    return div;
  },

  showThinking() {
    if (!this.container) return;

    const div = document.createElement('div');
    div.className = 'message assistant thinking';
    div.id = 'thinkingMessage';
    div.innerHTML = `
      <div class="message-avatar">AI</div>
      <div class="thinking-dots">
        <span></span>
        <span></span>
        <span></span>
      </div>
      <span class="thinking-text">${t('chat.thinking')}</span>
    `;

    this.container.appendChild(div);
    this.scrollToBottom();
  },

  hideThinking() {
    const thinking = document.getElementById('thinkingMessage');
    if (thinking) {
      thinking.remove();
    }
  },

  updateAssistantMessage(content, done = false) {
    const thinking = document.getElementById('thinkingMessage');
    if (thinking) {
      thinking.classList.remove('thinking');
      thinking.innerHTML = `
        <div class="message-avatar">AI</div>
        <div class="message-content">
          <div class="message-role">Cowd</div>
          <div class="message-text">${this.formatContent(content)}</div>
        </div>
      `;
      thinking.id = '';
      thinking.className = 'message assistant';

      if (done) {
        // Apply syntax highlighting
        thinking.querySelectorAll('pre code').forEach(block => {
          Prism.highlightElement(block);
        });
      }

      this.scrollToBottom();
    }
  },

  showError(message) {
    const errorDiv = document.createElement('div');
    errorDiv.className = 'message assistant';
    errorDiv.innerHTML = `
      <div class="message-avatar">!</div>
      <div class="message-content" style="border-color: var(--red);">
        <div class="message-role" style="color: var(--red);">错误</div>
        <div class="message-text">${this.escapeHtml(message)}</div>
      </div>
    `;
    this.container.appendChild(errorDiv);
    this.scrollToBottom();
  },

  showLoginModal() {
    const modal = document.getElementById('loginModal');
    if (modal) {
      modal.classList.add('active');
      const tokenInput = document.getElementById('loginToken');
      if (tokenInput) {
        tokenInput.focus();
      }
    }
  },

  formatContent(content) {
    if (!content) return '';

    // Escape HTML
    let formatted = this.escapeHtml(content);

    // Code blocks
    formatted = formatted.replace(/```(\w+)?\n([\s\S]*?)```/g, (match, lang, code) => {
      const language = lang || 'plaintext';
      return `<pre><code class="language-${language}">${code.trim()}</code></pre>`;
    });

    // Inline code
    formatted = formatted.replace(/`([^`]+)`/g, '<code>$1</code>');

    // Bold
    formatted = formatted.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

    // Italic
    formatted = formatted.replace(/\*([^*]+)\*/g, '<em>$1</em>');

    // Line breaks
    formatted = formatted.replace(/\n/g, '<br>');

    return formatted;
  },

  escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  },

  updateContextIndicator(usage) {
    const indicator = document.getElementById('contextIndicator');
    if (!indicator || !usage) return;

    const total = usage.total_tokens || 0;
    const max = 200000; // Approximate context window

    let percentage = (total / max) * 100;
    let levelClass = '';

    if (percentage > 90) {
      levelClass = 'critical';
    } else if (percentage > 70) {
      levelClass = 'warning';
    }

    indicator.innerHTML = `
      <span class="token-count">${total.toLocaleString()} tokens</span>
      <span class="token-bar">
        <span class="token-fill ${levelClass}" style="width: ${Math.min(percentage, 100)}%"></span>
      </span>
    `;
  },

  scrollToBottom() {
    if (this.container) {
      this.container.scrollTop = this.container.scrollHeight;
    }
  },

  newChat() {
    // Clear messages
    state.set('currentSession', null);
    state.set('messages', []);

    // Clear UI
    if (this.container) {
      this.container.innerHTML = `
        <div class="welcome-message">
          <div class="welcome-icon">
            <svg viewBox="0 0 32 32" width="64" height="64">
              <defs>
                <linearGradient id="welcomeBg" x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" style="stop-color:#e94560"/>
                  <stop offset="100%" style="stop-color:#16213e"/>
                </linearGradient>
              </defs>
              <rect width="32" height="32" rx="6" fill="url(#welcomeBg)"/>
              <text x="16" y="22" font-family="Arial, sans-serif" font-size="18" font-weight="bold" fill="white" text-anchor="middle">C</text>
            </svg>
          </div>
          <h2>${t('welcome.title')}</h2>
          <p>${t('welcome.subtitle')}</p>
          <div class="suggestions">
            <div class="suggestion" data-prompt="帮我分析当前项目的架构">
              <span class="suggestion-icon">📊</span>
              <span>分析项目架构</span>
            </div>
            <div class="suggestion" data-prompt="帮我写一个 Hello World 程序">
              <span class="suggestion-icon">👋</span>
              <span>写 Hello World</span>
            </div>
            <div class="suggestion" data-prompt="解释这段代码的逻辑">
              <span class="suggestion-icon">🔍</span>
              <span>解释代码</span>
            </div>
            <div class="suggestion" data-prompt="帮我重构这个函数">
              <span class="suggestion-icon">♻️</span>
              <span>重构代码</span>
            </div>
          </div>
        </div>
      `;

      // Rebind suggestions
      document.querySelectorAll('.suggestion').forEach(el => {
        el.addEventListener('click', () => {
          const prompt = el.dataset.prompt;
          if (prompt) {
            const inputArea = document.getElementById('inputArea');
            if (inputArea) {
              inputArea.value = prompt;
              this.send();
            }
          }
        });
      });
    }

    // Clear context indicator
    const indicator = document.getElementById('contextIndicator');
    if (indicator) {
      indicator.innerHTML = '';
    }

    // Switch to chat panel
    Panels.show('chat');
  },

  loadMessages(sessionId) {
    // TODO: Load messages for session
  }
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  Messages.init();
});

// Export
window.Messages = Messages;
