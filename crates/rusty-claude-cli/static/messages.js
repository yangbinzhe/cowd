/**
 * cc-webui -- messages.js
 * SSE 事件处理、消息发送、流式显示。
 * 支持：参数传递 (temperature/max_tokens/top_p/system_prompt)、取消、命令处理
 */

// 当前流状态
let _currentStreamId = null;
let _streamingBubble = null;
let _streamBuffer = '';


// ── 获取当前参数 ────────────────────────────────────────────────────────────

function _getParams() {
  const settings = window.S?.settings || {};
  // 从参数面板或右侧设置面板读取，优先参数面板
  const paramTemp = document.getElementById('paramTemp');
  const paramMaxTokens = document.getElementById('paramMaxTokens');
  const paramTopP = document.getElementById('paramTopP');
  const settingsTemp = document.getElementById('settingsTemp');
  const settingsMaxTokens = document.getElementById('settingsMaxTokens');
  const settingsTopP = document.getElementById('settingsTopP');
  const settingsSysPrompt = document.getElementById('settingsSystemPrompt');

  return {
    temperature: parseFloat(
      (paramTemp?.value) || (settingsTemp?.value) || settings.temperature || 0.7
    ),
    max_tokens: parseInt(
      (paramMaxTokens?.value) || (settingsMaxTokens?.value) || settings.maxTokens || 4096
    ),
    top_p: parseFloat(
      (paramTopP?.value) || (settingsTopP?.value) || settings.topP || 0.9
    ),
    system_prompt: (settingsSysPrompt?.value) || settings.systemPrompt || '',
  };
}


// ── 发送消息 ────────────────────────────────────────────────────────────────

async function handleSend() {
  const msgEl = document.getElementById('msg');
  if (!msgEl) return;
  const text = msgEl.value.trim();
  if (!text) return;
  if (_currentStreamId) return; // 已在流式中

  // 检查是否是本地命令（以 / 开头）
  if (text.startsWith('/') && typeof executeLocalCommand === 'function') {
    if (executeLocalCommand(text)) {
      msgEl.value = '';
      autoResize(msgEl);
      hideCommandDropdown();
      return;
    }
  }

  msgEl.value = '';
  autoResize(msgEl);
  hideCommandDropdown();

  // 关闭参数面板
  const paramPanel = document.getElementById('paramPanel');
  if (paramPanel) paramPanel.style.display = 'none';
  const paramBtn = document.getElementById('btnParams');
  if (paramBtn) paramBtn.classList.remove('active');

  // 确保有会话
  if (!window.S.session) {
    await newSession();
  }

  const sessionId = window.S.session.id;
  const model = window.S.selectedModel || 'claude-3-5-sonnet-20241022';
  const params = _getParams();

  // 追加用户消息气泡
  appendMessage('user', text, Date.now() / 1000);
  scrollToBottom();

  // 更新本地会话缓存
  if (window.S.session.messages) {
    window.S.session.messages.push({ role: 'user', content: text, timestamp: Math.floor(Date.now() / 1000) });
  }

  // 显示状态栏
  setStatus('思考中…');

  // 创建助手气泡占位
  const bubble = appendMessage('assistant', '', Date.now() / 1000);
  _streamingBubble = bubble;
  _streamBuffer = '';

  // 发起 SSE 流
  try {
    const resp = await fetch('/api/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        session_id: sessionId,
        message: text,
        model,
        temperature: params.temperature,
        max_tokens: params.max_tokens,
        top_p: params.top_p,
        system_prompt: params.system_prompt,
      }),
    });

    if (!resp.ok) {
      const err = await resp.text();
      showError(`请求失败: ${err}`);
      _cleanupStream();
      return;
    }

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let lineBuffer = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      lineBuffer += decoder.decode(value, { stream: true });

      // 处理完整 SSE 行
      const lines = lineBuffer.split('\n');
      lineBuffer = lines.pop(); // 最后一个可能不完整

      let currentEvent = '';
      for (const line of lines) {
        if (line.startsWith('event:')) {
          currentEvent = line.slice(6).trim();
        } else if (line.startsWith('data:')) {
          const dataStr = line.slice(5).trim();
          try {
            const data = JSON.parse(dataStr);
            _handleSseEvent(currentEvent, data, sessionId);
          } catch (e) {
            // 忽略解析错误
          }
          currentEvent = '';
        }
      }
    }
  } catch (e) {
    if (e.name !== 'AbortError') {
      showError(`连接错误: ${e.message}`);
    }
  } finally {
    _cleanupStream();
  }
}


// ── SSE 事件处理 ────────────────────────────────────────────────────────────

function _handleSseEvent(event, data, sessionId) {
  switch (event) {
    case 'session':
      // 服务器确认 session_id 和 stream_id
      _currentStreamId = data.stream_id;
      if (data.session_id && window.S.session) {
        window.S.session.id = data.session_id;
      }
      // 流开始后显示取消按钮
      const cancelBtn = document.getElementById('btnCancel');
      if (cancelBtn) cancelBtn.style.display = 'flex';
      break;

    case 'token':
      // 流式 token
      _streamBuffer += data.text || '';
      if (_streamingBubble) {
        _streamingBubble.innerHTML = MD.render(_streamBuffer);
        scrollToBottom();
      }
      break;

    case 'status':
      setStatus(data.text || '处理中…');
      break;

    case 'tool_call':
      // 工具调用事件
      setStatus(`工具: ${data.name || '执行中'}${data.preview ? ' · ' + data.preview.slice(0, 40) : ''}`);
      break;

    case 'done':
      // 流完成
      const finalContent = _streamBuffer;
      if (finalContent && _streamingBubble) {
        _streamingBubble.innerHTML = MD.render(finalContent);
      }
      // 保存助手消息
      if (finalContent && sessionId) {
        _saveAssistantMessage(sessionId, finalContent, data.title);
      }
      setStatus('');
      _cleanupStream();
      break;

    case 'cancel':
      setStatus('');
      showToast('已取消生成', 'info');
      _cleanupStream();
      break;

    case 'apperror':
    case 'error':
      showError(data.message || '发生错误', data.hint);
      setStatus('');
      _cleanupStream();
      break;

    default:
      break;
  }
}


// ── 保存助手消息 ────────────────────────────────────────────────────────────

async function _saveAssistantMessage(sessionId, content, newTitle) {
  try {
    const r = await fetch('/api/sessions/message', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId, role: 'assistant', content }),
    });
    const data = await r.json();
    if (data.title && window.S.session) {
      window.S.session.title = data.title;
      // 更新会话列表
      const s = (window.S._allSessions || []).find(s => s.id === sessionId);
      if (s) {
        s.title = data.title;
        s.updated_at = Date.now() / 1000;
        s.message_count = (s.message_count || 0) + 2;
      }
      renderSessionList(window.S._allSessions || []);
      // 更新顶栏
      const titleEl = document.getElementById('topbarTitle');
      if (titleEl) titleEl.textContent = data.title;
      updateInfoPanel();
    }
    // 更新本地消息缓存
    if (window.S.session) {
      if (!window.S.session.messages) window.S.session.messages = [];
      window.S.session.messages.push({
        role: 'assistant',
        content,
        timestamp: Math.floor(Date.now() / 1000),
      });
      const metaEl = document.getElementById('topbarMeta');
      if (metaEl) metaEl.textContent = `${window.S.session.messages.length} 条消息`;
    }
  } catch (e) {
    console.error('保存助手消息失败:', e);
  }
}


// ── 取消流 ──────────────────────────────────────────────────────────────────

async function cancelCurrentStream() {
  if (!_currentStreamId) return;
  const sid = _currentStreamId;
  try {
    await fetch('/api/cancel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stream_id: sid }),
    });
  } catch (e) {
    // 忽略取消错误
  }
  _cleanupStream();
  setStatus('');
  showToast('已取消', 'info');
}


// ── 清理流状态 ──────────────────────────────────────────────────────────────

function _cleanupStream() {
  _currentStreamId = null;
  _streamingBubble = null;
  _streamBuffer = '';
  // 隐藏取消按钮
  const cancelBtn = document.getElementById('btnCancel');
  if (cancelBtn) cancelBtn.style.display = 'none';
  // 恢复发送按钮和输入框
  const sendBtn = document.getElementById('btnSend');
  if (sendBtn) sendBtn.disabled = false;
  const msgEl = document.getElementById('msg');
  if (msgEl) msgEl.disabled = false;
}


// ── 状态栏 ──────────────────────────────────────────────────────────────────

function setStatus(text) {
  const bar = document.getElementById('activityBar');
  const textEl = document.getElementById('activityText');
  const cancelBtn = document.getElementById('btnCancel');
  if (!bar) return;

  if (!text) {
    bar.style.display = 'none';
    if (cancelBtn) cancelBtn.style.display = 'none';
    return;
  }
  bar.style.display = 'block';
  if (textEl) textEl.textContent = text;
  // 取消按钮只在有活跃流时显示
  if (cancelBtn) cancelBtn.style.display = _currentStreamId ? 'flex' : 'none';
}


// ── 错误显示 ────────────────────────────────────────────────────────────────

function showError(msg, hint) {
  const inner = document.getElementById('msgInner');
  if (!inner) { showToast(msg, 'error'); return; }

  const empty = document.getElementById('emptyState');
  if (empty) empty.style.display = 'none';

  const row = document.createElement('div');
  row.className = 'msg-row assistant';
  row.innerHTML = `
    <div class="msg-bubble" style="border-color:rgba(252,129,129,.3);background:rgba(252,129,129,.05)">
      <span style="color:var(--red)">⚠ 错误:</span> ${esc(msg)}
      ${hint ? `<div style="font-size:12px;color:var(--muted);margin-top:6px">${esc(hint)}</div>` : ''}
    </div>
  `;
  inner.appendChild(row);
  scrollToBottom();
}


// ── 键盘处理 ────────────────────────────────────────────────────────────────

function initComposer() {
  const msgEl = document.getElementById('msg');
  if (!msgEl) return;

  msgEl.addEventListener('input', () => {
    autoResize(msgEl);
    // 命令自动完成
    const text = msgEl.value;
    if (text.startsWith('/') && text.indexOf('\n') === -1) {
      const prefix = text.slice(1);
      const matches = getMatchingCommands(prefix);
      if (matches.length) showCommandDropdown(matches);
      else hideCommandDropdown();
    } else {
      hideCommandDropdown();
    }
  });

  msgEl.addEventListener('keydown', e => {
    const settings = window.S?.settings || {};
    const sendKey = settings.send_key || window._sendKey || 'enter';

    // 命令下拉导航
    const dd = document.getElementById('cmdDropdown');
    const dropdownOpen = dd && dd.classList.contains('open');
    if (dropdownOpen) {
      if (e.key === 'ArrowUp') { e.preventDefault(); navigateCommandDropdown(-1); return; }
      if (e.key === 'ArrowDown') { e.preventDefault(); navigateCommandDropdown(1); return; }
      if (e.key === 'Tab') { e.preventDefault(); selectCommandDropdownItem(); return; }
      if (e.key === 'Escape') { e.preventDefault(); hideCommandDropdown(); return; }
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); selectCommandDropdownItem(); return; }
    }

    if (sendKey === 'enter') {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    } else {
      // ctrl+enter
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        handleSend();
      }
    }
  });
}


// ── 参数面板切换 ────────────────────────────────────────────────────────────

function toggleParamPanel() {
  const panel = document.getElementById('paramPanel');
  const btn = document.getElementById('btnParams');
  if (!panel) return;
  const open = panel.style.display !== 'none';
  panel.style.display = open ? 'none' : 'block';
  if (btn) btn.classList.toggle('active', !open);
}
