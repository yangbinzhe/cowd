function showToast(msg, type) {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.className = type;
    el.classList.add('show');
    setTimeout(() => el.classList.remove('show'), 3000);
}

const connStatus = document.createElement('span');
connStatus.id = 'connection-status';
connStatus.textContent = 'Not connected';
connStatus.style.cssText = 'color:#888;font-size:11px;margin-left:12px';
document.getElementById('chat-header').appendChild(connStatus);

function updateConnStatus(connected) {
    connStatus.textContent = connected ? 'Connected' : 'Disconnected';
    connStatus.style.color = connected ? '#00c853' : '#e94560';
}
updateConnStatus(false);

document.getElementById('btn-new-session').addEventListener('click', async () => {
    try {
        Chat.setLoading(true);
        Stream.disconnect();
        Chat.clearMessages();
        const d = await API.newSession();
        Sessions.setActive(d.session_id);
        Chat.setLoading(false);
    Chat.addMessage('system', `New session: ${d.session_id.slice(0, 8)}...`);
    Stream.connect();
    updateConnStatus(true);
    showToast('New session created', 'success');
} catch (e) {
    Chat.setLoading(false);
    updateConnStatus(false);
    showToast(e.message, 'error');
}
});

const input = document.getElementById('chat-input');
input.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        document.getElementById('btn-send').click();
    }
});

document.getElementById('btn-send').addEventListener('click', async () => {
    const text = input.value.trim();
    if (!text || !API.sessionId) return;
    input.value = '';

    Chat.addMessage('user', text);
    Chat.setLoading(true);

    try {
        await API.submitPrompt(text);
    } catch (e) {
        Chat.setLoading(false);
        Chat.addMessage('system', `Error: ${e.message}`);
    }
});

Stream.on('messageDelta', text => {
    const existing = document.querySelector('.message.assistant.streaming');
    if (existing) {
        existing.textContent += text;
        existing.scrollIntoView({ behavior: 'smooth' });
    } else {
        const el = Chat.addMessage('assistant', text);
        el.classList.add('streaming');
    }
});

Stream.on('toolStart', (id, name) => Chat.addToolCard(id, name));
Stream.on('toolComplete', (id, output) => Chat.updateToolCard(id, output, true));

Stream.on('promptComplete', data => {
    const el = document.querySelector('.message.assistant.streaming');
    if (el) el.classList.remove('streaming');
    if (data && data.usage) Chat.addTokens(data.usage.total_tokens || 0);
    Sessions.load();
});

document.querySelector('#panel-tabs button[data-panel="close"]')
    .addEventListener('click', () => document.getElementById('right-panel').classList.add('hidden'));

document.querySelectorAll('#panel-tabs button[data-panel]:not([data-panel="close"])')
    .forEach(btn => btn.addEventListener('click', () => {
        document.getElementById('right-panel').classList.remove('hidden');
        document.querySelectorAll('#panel-tabs button[data-panel]').forEach(b => b.classList.remove('tab-active'));
        btn.classList.add('tab-active');
    }));

Sessions.load();
