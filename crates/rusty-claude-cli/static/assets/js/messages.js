/**
 * Cowd Messages - Chat Messages Module
 */

// Message Renderer
const Messages = {
  container: null,
  abortController: null,
  thinkingFilter: null,  // P0-2: ThinkingFilter instance for streaming
  messageList: [],       // Tracked message list for edit/regenerate
  currentSessionId: null,

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
    window.appState?.subscribe('isStreaming', (isStreaming) => {
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
    if (!window.api?.isAuthenticated()) {
      this.showLoginModal();
      return;
    }

    const content = inputArea.value.trim();
    inputArea.value = '';
    inputArea.style.height = 'auto';

    // Create or use current session
    let session = window.appState?.get('currentSession');
    if (!session) {
      try {
        session = await window.api?.createSession();
        window.appState?.set('currentSession', session);
        window.appState?.update('sessions', sessions => [session, ...(sessions || [])]);
        if (window.Sessions) window.Sessions.renderSessions();
      } catch (error) {
        window.Toast?.error('创建会话失败');
        return;
      }
    }
    this.currentSessionId = session.id;

    // Add user message
    this.addMessage({
      role: 'user',
      content
    });

    // Show thinking indicator
    this.showThinking();

    // Update button state
    window.appState?.set('isStreaming', true);

    // Cancel any existing stream
    if (this.abortController) {
      this.abortController.abort();
    }
    this.abortController = new AbortController();

    // P0-2: Initialize ThinkingFilter for this conversation turn
    this.thinkingFilter = window.ThinkingFilter ? new window.ThinkingFilter() : null;
    let filteredContent = '';

    try {
      const result = await window.api?.sendMessage(session.id, content, {
        signal: this.abortController.signal,
        onChunk: (chunk, fullContent) => {
          // P0-2: Filter thinking tags from streamed content
          if (this.thinkingFilter) {
            const { output } = this.thinkingFilter.process(chunk);
            filteredContent += output;
            this.updateAssistantMessage(filteredContent);
          } else {
            this.updateAssistantMessage(fullContent);
          }
        },
        onComplete: (fullContent, data) => {
          this.hideThinking();
          // Use filtered content if available, otherwise raw content
          const displayContent = filteredContent || fullContent;
          this.updateAssistantMessage(displayContent, true);

          // Add to messages state
          const assistantMsg = {
            role: 'assistant',
            content: displayContent,
            timestamp: new Date().toISOString()
          };

          this.addMessageToState(assistantMsg);

          // P0-前端层4: Auto-generate session title on first exchange
          this._autoGenerateTitle(session.id, content);

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
      window.appState?.set('isStreaming', false);
    }
  },

  addMessage(msg) {
    if (!this.container) return;

    // Remove welcome message if exists
    const welcome = this.container.querySelector('.welcome-message');
    if (welcome) {
      welcome.remove();
    }

    const index = this.messageList.length;
    const messageEl = this.createMessageElement(msg, index);
    this.container.appendChild(messageEl);
    this.scrollToBottom();

    // Track message
    this.messageList.push(msg);
    window.appState?.update('messages', msgs => [...(msgs || []), msg]);
  },

  addMessageToState(msg) {
    this.messageList.push(msg);
    window.appState?.update('messages', msgs => [...(msgs || []), msg]);
  },

  createMessageElement(msg, index) {
    const div = document.createElement('div');
    div.className = `message ${msg.role}`;
    if (index !== undefined) {
      div.dataset.msgIndex = index;
    }

    const initials = msg.role === 'user' ? 'U' : 'AI';
    const roleName = msg.role === 'user' ? '你' : 'Cowd';

    div.innerHTML = `
      <div class="message-avatar">${initials}</div>
      <div class="message-content">
        <div class="message-role">${roleName}</div>
        <div class="message-text">${this.renderMd(msg.content)}</div>
      </div>
    `;

    // Add action buttons
    const actions = this._createMessageActions(msg, index);
    if (actions) {
      div.appendChild(actions);
    }

    return div;
  },

  // ═══════════════════════════════════════════════════════════════════
  // P0-前端层2: Message Edit / Regenerate / Copy
  // ═══════════════════════════════════════════════════════════════════

  _createMessageActions(msg, index) {
    if (index === undefined) return null;
    const actions = document.createElement('div');
    actions.className = 'message-actions';

    if (msg.role === 'user') {
      actions.innerHTML = `
        <button class="action-btn edit-btn" title="Edit">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
            <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
          </svg>
        </button>
      `;
      actions.querySelector('.edit-btn').addEventListener('click', () => this._editMessage(index));
    } else if (msg.role === 'assistant') {
      actions.innerHTML = `
        <button class="action-btn regen-btn" title="Regenerate">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="23 4 23 10 17 10"></polyline>
            <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"></path>
          </svg>
        </button>
        <button class="action-btn copy-btn" title="Copy">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
          </svg>
        </button>
      `;
      actions.querySelector('.regen-btn').addEventListener('click', () => this._regenerateMessage(index));
      actions.querySelector('.copy-btn').addEventListener('click', () => this._copyMessage(index));
    }

    return actions;
  },

  async _editMessage(index) {
    const msg = this.messageList[index];
    if (!msg || msg.role !== 'user') return;

    const msgEl = this.container.querySelector(`[data-msg-index="${index}"]`);
    if (!msgEl) return;
    const contentEl = msgEl.querySelector('.message-text');
    if (!contentEl) return;

    const originalText = msg.content;

    // Replace content with editable textarea
    contentEl.innerHTML = `
      <textarea class="edit-textarea">${this.escapeHtml(originalText)}</textarea>
      <div class="edit-actions">
        <button class="btn-save">Save & Resend</button>
        <button class="btn-cancel">Cancel</button>
      </div>
    `;

    const textarea = contentEl.querySelector('.edit-textarea');
    textarea.focus();

    // Save & Resend
    contentEl.querySelector('.btn-save').addEventListener('click', async () => {
      const newText = textarea.value.trim();
      if (!newText) return;

      // 1. Update local message
      this.messageList[index].content = newText;

      // 2. Splice messages from index+1 onwards via API
      const sessionId = this.currentSessionId;
      if (sessionId) {
        try {
          await window.api?.spliceMessages(sessionId, index + 1);
        } catch (e) {
          console.warn('[Cowd] Splice failed:', e);
        }
      }

      // 3. Remove all messages after this one from UI and state
      this.messageList = this.messageList.slice(0, index + 1);
      // Remove DOM elements after the edited message
      const allMsgEls = this.container.querySelectorAll('.message');
      for (const el of allMsgEls) {
        const elIndex = parseInt(el.dataset.msgIndex);
        if (elIndex > index) el.remove();
      }

      // Update content display
      contentEl.innerHTML = this.renderMd(newText);

      // 4. Re-send the edited message
      this.showThinking();
      window.appState?.set('isStreaming', true);

      this.thinkingFilter = window.ThinkingFilter ? new window.ThinkingFilter() : null;
      let filteredContent = '';

      try {
        await window.api?.sendMessage(sessionId, newText, {
          onChunk: (chunk, fullContent) => {
            if (this.thinkingFilter) {
              const { output } = this.thinkingFilter.process(chunk);
              filteredContent += output;
              this.updateAssistantMessage(filteredContent);
            } else {
              this.updateAssistantMessage(fullContent);
            }
          },
          onComplete: (fullContent) => {
            this.hideThinking();
            const displayContent = filteredContent || fullContent;
            this.updateAssistantMessage(displayContent, true);
            this.addMessageToState({
              role: 'assistant',
              content: displayContent,
              timestamp: new Date().toISOString()
            });
          },
          onError: (error) => {
            this.hideThinking();
            this.showError(error.message);
          }
        });
      } catch (error) {
        if (error.name !== 'AbortError') {
          this.hideThinking();
          this.showError(error.message || '重新发送失败');
        }
      } finally {
        window.appState?.set('isStreaming', false);
      }
    });

    // Cancel - restore original content
    contentEl.querySelector('.btn-cancel').addEventListener('click', () => {
      contentEl.innerHTML = this.renderMd(originalText);
    });
  },

  async _regenerateMessage(index) {
    if (index <= 0) return;
    const userMsg = this.messageList[index - 1];
    if (!userMsg || userMsg.role !== 'user') return;

    const sessionId = this.currentSessionId;
    if (!sessionId) return;

    // 1. Splice from this assistant message onwards
    try {
      await window.api?.spliceMessages(sessionId, index);
    } catch (e) {
      console.warn('[Cowd] Splice failed:', e);
    }

    // 2. Remove this and all following messages from UI and state
    this.messageList = this.messageList.slice(0, index);
    const allMsgEls = this.container.querySelectorAll('.message');
    for (const el of allMsgEls) {
      const elIndex = parseInt(el.dataset.msgIndex);
      if (elIndex >= index) el.remove();
    }

    // 3. Re-send the user message
    this.showThinking();
    window.appState?.set('isStreaming', true);

    this.thinkingFilter = window.ThinkingFilter ? new window.ThinkingFilter() : null;
    let filteredContent = '';

    try {
      await window.api?.sendMessage(sessionId, userMsg.content, {
        onChunk: (chunk, fullContent) => {
          if (this.thinkingFilter) {
            const { output } = this.thinkingFilter.process(chunk);
            filteredContent += output;
            this.updateAssistantMessage(filteredContent);
          } else {
            this.updateAssistantMessage(fullContent);
          }
        },
        onComplete: (fullContent) => {
          this.hideThinking();
          const displayContent = filteredContent || fullContent;
          this.updateAssistantMessage(displayContent, true);
          this.addMessageToState({
            role: 'assistant',
            content: displayContent,
            timestamp: new Date().toISOString()
          });
        },
        onError: (error) => {
          this.hideThinking();
          this.showError(error.message);
        }
      });
    } catch (error) {
      if (error.name !== 'AbortError') {
        this.hideThinking();
        this.showError(error.message || '重新生成失败');
      }
    } finally {
      window.appState?.set('isStreaming', false);
    }
  },

  _copyMessage(index) {
    const msg = this.messageList[index];
    if (!msg) return;
    navigator.clipboard.writeText(msg.content).then(() => {
      window.Toast?.success('已复制到剪贴板');
    }).catch(() => {
      window.Toast?.error('复制失败');
    });
  },

  // ═══════════════════════════════════════════════════════════════════
  // P0-前端层3: Enhanced Markdown Rendering
  // ═══════════════════════════════════════════════════════════════════

  renderMd(text) {
    if (!text) return '';
    let html = text;

    // 1. Code blocks: ```lang → <pre><code class="language-X"> with copy button
    html = html.replace(/```(\w+)?\n([\s\S]*?)```/g, (_, lang, code) => {
      const language = lang || 'plaintext';
      const escaped = this.escapeHtml(code.trim());
      return `<pre class="code-block"><code class="language-${language}">${escaped}</code><button class="copy-code-btn" data-action="copy-code">Copy</button></pre>`;
    });

    // 2. Inline code: `code` → <code>
    html = html.replace(/`([^`]+)`/g, '<code class="inline-code">$1</code>');

    // 3. Tables
    html = this._renderTable(html);

    // 4. Mermaid diagrams: ```mermaid → <div class="mermaid">
    html = html.replace(/<pre class="code-block"><code class="language-mermaid">([\s\S]*?)<\/code>/g,
      (_, content) => `<div class="mermaid">${content}</div>`
    );

    // 5. Headers: # H1 → <h2>, ## H2 → <h3>, ### H3 → <h4>
    html = html.replace(/^### (.+)$/gm, '<h4>$1</h4>');
    html = html.replace(/^## (.+)$/gm, '<h3>$1</h3>');
    html = html.replace(/^# (.+)$/gm, '<h2>$1</h2>');

    // 6. Bold/Italic
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // 7. Links: [text](url)
    html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');

    // 8. Unordered lists: - item → <li>
    html = html.replace(/^- (.+)$/gm, '<li>$1</li>');

    // 9. Ordered lists: 1. item → <ol><li>
    html = html.replace(/^\d+\. (.+)$/gm, '<li class="ol-item">$1</li>');

    // 10. Horizontal rule: --- → <hr>
    html = html.replace(/^---$/gm, '<hr>');

    // 11. Blockquotes: > text → <blockquote>
    html = html.replace(/^> (.+)$/gm, '<blockquote>$1</blockquote>');

    // 12. Line breaks (but not inside pre/code blocks)
    html = html.replace(/\n/g, '<br>');

    // Wire up copy-code buttons via event delegation
    return html;
  },

  _renderTable(text) {
    const lines = text.split('\n');
    let inTable = false;
    let result = [];
    let headerDone = false;

    for (const line of lines) {
      if (line.trim().startsWith('|') && line.trim().endsWith('|')) {
        if (!inTable) {
          result.push('<table class="md-table">');
          inTable = true;
          headerDone = false;
        }

        // Skip separator rows
        if (/^\|[\s\-:|]+\|$/.test(line.trim())) {
          headerDone = true;
          continue;
        }

        const cells = line.split('|').filter(c => c.trim() !== '');
        const tag = !headerDone ? 'th' : 'td';
        const row = cells.map(c => `<${tag}>${c.trim()}</${tag}>`).join('');
        result.push(`<tr>${row}</tr>`);
      } else {
        if (inTable) {
          result.push('</table>');
          inTable = false;
        }
        result.push(line);
      }
    }
    if (inTable) result.push('</table>');
    return result.join('\n');
  },

  // ═══════════════════════════════════════════════════════════════════
  // P0-前端层4: Auto-generate session title
  // ═══════════════════════════════════════════════════════════════════

  async _autoGenerateTitle(sessionId, firstUserMessage) {
    if (!sessionId || !firstUserMessage) return;

    // Only generate if current title is default/empty
    const session = window.appState?.get('currentSession');
    if (session && session.title && session.title !== 'New Chat' && session.title !== '新对话') {
      return; // Title already set
    }

    // Strip generic prefixes
    const genericStarts = ['帮我', '请', 'Let me help', 'Can you', 'How do', 'What is', '帮我写', '请帮我', 'Write', 'Create', 'Explain'];
    let title = firstUserMessage.trim();
    for (const start of genericStarts) {
      if (title.startsWith(start)) {
        title = title.slice(start.length).trim();
        break;
      }
    }

    // Truncate to 40 chars
    if (title.length > 40) {
      title = title.slice(0, 40) + '...';
    }

    if (!title) return;

    try {
      await window.api?.updateSession(sessionId, { title });

      // Update sidebar display
      const sessionEl = document.querySelector(`[data-session-id="${sessionId}"] .session-title`);
      if (sessionEl) sessionEl.textContent = title;

      // Update state
      if (session) {
        session.title = title;
        window.appState?.set('currentSession', session);
      }
    } catch (e) {
      console.warn('[Cowd] Title generation failed:', e);
    }
  },

  // ═══════════════════════════════════════════════════════════════════
  // Core message display
  // ═══════════════════════════════════════════════════════════════════

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
      <span class="thinking-text">${window.i18nInstance?.t('chat.thinking') || '思考中...'}</span>
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
      const index = this.messageList.length; // Will be appended
      thinking.innerHTML = `
        <div class="message-avatar">AI</div>
        <div class="message-content">
          <div class="message-role">Cowd</div>
          <div class="message-text">${this.renderMd(content)}</div>
        </div>
      `;
      thinking.id = '';
      thinking.className = 'message assistant';
      thinking.dataset.msgIndex = index;

      if (done) {
        // Apply syntax highlighting
        thinking.querySelectorAll('pre code').forEach(block => {
          window.Prism?.highlightElement(block);
        });

        // Add action buttons
        const actions = this._createMessageActions({ role: 'assistant', content }, index);
        if (actions) thinking.appendChild(actions);
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

  // Legacy formatContent now uses renderMd
  formatContent(content) {
    return this.renderMd(content);
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
    this.messageList = [];
    this.currentSessionId = null;
    window.appState?.set('currentSession', null);
    window.appState?.set('messages', []);

    // Clear UI
    if (this.container) {
      const _t = (key, fallback) => window.i18nInstance?.t(key) || fallback;
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
          <h2>${_t('welcome.title', '欢迎使用 Cowd')}</h2>
          <p>${_t('welcome.subtitle', '选择下方建议或直接输入问题')}</p>
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
    window.panelManager?.show('chat');
  },

  loadMessages(messagesOrId) {
    if (!this.container) return;

    // Remove welcome message if exists
    const welcome = this.container.querySelector('.welcome-message');
    if (welcome) welcome.remove();

    // If given a session ID, fetch messages first
    if (typeof messagesOrId === 'string') {
      this.currentSessionId = messagesOrId;
      window.api.getMessages(messagesOrId).then(data => {
        const messages = data.messages || data || [];
        this._renderMessagesList(messages);
      }).catch(e => {
        console.error('[Cowd] Failed to load messages:', e);
      });
      return;
    }

    // If given messages array directly
    this._renderMessagesList(messagesOrId || []);
  },

  _renderMessagesList(messages) {
    if (!this.container) return;

    // Reset tracked messages
    this.messageList = [];

    // Clear existing messages (except welcome)
    this.container.querySelectorAll('.message').forEach(el => el.remove());

    // Render each message
    for (const msg of messages) {
      const index = this.messageList.length;
      const messageEl = this.createMessageElement(msg, index);
      this.container.appendChild(messageEl);
      this.messageList.push(msg);
    }

    // Apply syntax highlighting
    this.container.querySelectorAll('pre code').forEach(block => {
      window.Prism?.highlightElement(block);
    });

    this.scrollToBottom();
  },

  clearMessages() {
    this.messageList = [];
    if (!this.container) return;
    this.container.querySelectorAll('.message').forEach(el => el.remove());
  },

  handleStreamChunk(detail) {
    // Handle stream chunk event from window event
    if (detail && detail.content) {
      this.updateAssistantMessage(detail.content);
    }
  },

  // ═══════════════════════════════════════════════════════════════════
  // SSE Approval Event Handling
  // ═══════════════════════════════════════════════════════════════════

  handleApprovalRequest(data) {
    if (!data || !data.id) {
      console.warn('[Cowd Messages] Invalid approval request data:', data);
      return;
    }
    if (window.ApprovalManager) {
      window.ApprovalManager.handleApprovalRequest(data);
    } else {
      console.warn('[Cowd Messages] ApprovalManager not available');
    }
  },

  handleApprovalResolved(data) {
    if (!data || !data.request_id) return;
    if (window.ApprovalManager && window.ApprovalManager.cards.has(data.request_id)) {
      const card = window.ApprovalManager.cards.get(data.request_id);
      if (data.verdict === 'TimedOut') {
        card._onTimeout();
      } else {
        card.respond(data.verdict);
      }
      window.ApprovalManager.cards.delete(data.request_id);
    }
  },

  // ═══════════════════════════════════════════════════════════════════
  // SSE Tool Visualization Event Handling (P0-2)
  // ═══════════════════════════════════════════════════════════════════

  handleToolStart(data) {
    if (!data || !data.id) return;
    if (window.ToolCardManager) {
      window.ToolCardManager.handleToolStart(data);
    }
  },

  handleToolProgress(data) {
    if (!data || !data.id) return;
    if (window.ToolCardManager) {
      window.ToolCardManager.handleToolProgress(data);
    }
  },

  handleToolComplete(data) {
    if (!data || !data.id) return;
    if (window.ToolCardManager) {
      window.ToolCardManager.handleToolComplete(data);
    }
  }
};

// ═══════════════════════════════════════════════════════════════════
// P0-前端层5: SSE Connection Manager (INFLIGHT recovery)
// ═══════════════════════════════════════════════════════════════════

class SSEConnectionManager {
  constructor() {
    this.eventSource = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.baseDelay = 1000;
    this.maxDelay = 30000;
    this.inflightMessageId = null;
    this.sessionId = null;
    this.callbacks = null;
  }

  connect(sessionId, callbacks) {
    this.disconnect();
    this.sessionId = sessionId;
    this.callbacks = callbacks;
    this.reconnectAttempts = 0;

    const url = `${window.api?.baseUrl || '/api'}/sessions/${sessionId}/stream`;
    this.eventSource = new EventSource(url);

    this.eventSource.onopen = () => {
      this.reconnectAttempts = 0;
      console.log('[Cowd SSE] Connected');
    };

    this.eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        callbacks.onData?.(data);

        // Track INFLIGHT state
        if (data.type === 'assistant_start') {
          this.inflightMessageId = data.id;
        } else if (data.type === 'message_stop' || data.type === 'done') {
          this.inflightMessageId = null;
        }
      } catch (e) {
        console.warn('[Cowd SSE] Parse error:', e);
      }
    };

    this.eventSource.onerror = () => {
      console.error('[Cowd SSE] Connection error');
      this.eventSource.close();

      const delay = Math.min(
        this.baseDelay * Math.pow(2, this.reconnectAttempts),
        this.maxDelay
      );
      this.reconnectAttempts++;

      if (this.reconnectAttempts <= this.maxReconnectAttempts) {
        callbacks.onReconnecting?.(delay);
        setTimeout(() => {
          this.connect(sessionId, callbacks);
          if (this.inflightMessageId) {
            this._recoverInflight(sessionId);
          }
        }, delay);
      } else {
        callbacks.onDisconnected?.();
      }
    };

    // P1-6: Listen for context_usage events
    this.eventSource.addEventListener('context_usage', (event) => {
      try {
        const data = JSON.parse(event.data);
        renderContextRing(data.percentage || 0);
      } catch (e) {
        console.warn('[Cowd SSE] context_usage parse error:', e);
      }
    });

    // P1-7: Listen for thinking events
    this.eventSource.addEventListener('thinking', (event) => {
      try {
        const data = JSON.parse(event.data);
        callbacks.onThinkingDelta?.(data);
      } catch (e) {
        console.warn('[Cowd SSE] thinking parse error:', e);
      }
    });
  }

  async _recoverInflight(sessionId) {
    try {
      const data = await window.api?.getMessages(sessionId);
      const messages = data?.messages || data || [];
      const last = messages[messages.length - 1];
      if (last && last.role === 'assistant' && last.status === 'complete') {
        this.inflightMessageId = null;
        // Re-render the completed message
        if (window.Messages) {
          window.Messages.loadMessages(messages);
        }
      }
    } catch (e) {
      console.error('[Cowd SSE] Failed to recover inflight:', e);
    }
  }

  disconnect() {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
  }
}

// Global copy-code handler via event delegation
document.addEventListener('click', (e) => {
  const btn = e.target.closest('[data-action="copy-code"]');
  if (btn) {
    const codeEl = btn.previousElementSibling;
    if (codeEl) {
      navigator.clipboard.writeText(codeEl.textContent).then(() => {
        btn.textContent = 'Copied!';
        setTimeout(() => btn.textContent = 'Copy', 2000);
      }).catch(() => {
        btn.textContent = 'Failed';
        setTimeout(() => btn.textContent = 'Copy', 2000);
      });
    }
  }
});

// Initialize When DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  Messages.init();
});

// ── P1-6: Context Pressure Ring ──────────────────────────────────────────────

function renderContextRing(percentage) {
  let container = document.querySelector('.context-ring-container');
  if (!container) {
    container = document.createElement('div');
    container.className = 'context-ring-container';
    document.body.appendChild(container);
  }

  const clamped = Math.min(100, Math.max(0, percentage));
  const circumference = 2 * Math.PI * 40; // r=40
  const offset = circumference * (1 - clamped / 100);
  const color = clamped < 50 ? '#22c55e' : clamped < 80 ? '#eab308' : '#ef4444';

  container.innerHTML = `
    <svg viewBox="0 0 100 100" class="context-ring">
      <circle cx="50" cy="50" r="40" fill="none" stroke="var(--color-border, #2a2a4e)" stroke-width="6"/>
      <circle cx="50" cy="50" r="40" fill="none" stroke="${color}"
              stroke-width="6" stroke-dasharray="${circumference}"
              stroke-dashoffset="${offset}" stroke-linecap="round"
              transform="rotate(-90 50 50)"/>
      <text x="50" y="50" text-anchor="middle" dominant-baseline="central"
            fill="${color}" font-size="18" font-weight="bold">
        ${clamped.toFixed(0)}%
      </text>
    </svg>
  `;
}

// ── P1-7: Thinking Block Renderer ────────────────────────────────────────────

function renderThinkingBlock(content) {
  return `<div class="thinking-block collapsed">
    <div class="thinking-header">Thinking Process</div>
    <div class="thinking-content">${content}</div>
  </div>`;
}

// Export
window.Messages = Messages;
window.messageRenderer = Messages;
window.SSEConnectionManager = SSEConnectionManager;
