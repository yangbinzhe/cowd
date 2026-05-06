const Chat = (() => {
    const container = document.getElementById('chat-messages');
    const spinnerEl = document.getElementById('loading-indicator');
    const tokenEl = document.getElementById('token-usage');
    let totalTokens = 0;

    function escapeHtml(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function renderMarkdown(md) {
        let out = md
            .replace(/```(\w+)?\n([\s\S]*?)```/g, (_, lang, code) =>
                `<pre class="code-block" data-lang="${lang||'code'}"><code>${escapeHtml(code.trim())}</code></pre>`)
            .replace(/`([^`]+)`/g, '<code class="inline-code">$1</code>')
            .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
            .replace(/\*([^*]+)\*/g, '<em>$1</em>')
            .replace(/^### (.+)$/gm, '<h3>$1</h3>')
            .replace(/^## (.+)$/gm, '<h2>$1</h2>')
            .replace(/^# (.+)$/gm, '<h1>$1</h1>')
            .replace(/^- (.+)$/gm, '<li>$1</li>')
            .replace(/\n/g, '<br>');
        return out;
    }

    function addMessage(role, content) {
        const el = document.createElement('div');
        el.className = `message ${role}`;
        el.innerHTML = `<div class="msg-role">${role.toUpperCase()}</div>${renderMarkdown(content)}`;
        container.appendChild(el);
        el.scrollIntoView({ behavior: 'smooth' });
        return el;
    }

    function addToolCard(toolId, toolName) {
        const el = document.createElement('div');
        el.className = 'tool-card';
        el.id = `tool-${toolId}`;
        el.innerHTML = `<div class="tool-header" data-tool="${toolId}">
            <span>+</span>
            <span class="tool-name">${toolName}</span>
            <span class="tool-status running">running</span>
        </div><div class="tool-body"></div>`;
        el.querySelector('.tool-header').addEventListener('click', () => el.classList.toggle('open'));
        container.appendChild(el);
        el.scrollIntoView({ behavior: 'smooth' });
        return el;
    }

    function updateToolCard(toolId, output, done = false) {
        const card = document.getElementById(`tool-${toolId}`);
        if (!card) return;
        card.querySelector('.tool-body').textContent = output;
        if (done) {
            const status = card.querySelector('.tool-status');
            status.textContent = 'done';
            status.className = 'tool-status done';
        }
    }

    function setLoading(loading) {
        spinnerEl.classList.toggle('hidden', !loading);
    }

    function addTokens(n) { totalTokens += n; tokenEl.textContent = `${Math.round(totalTokens/1000)}k tk`; }

    function clearMessages() { container.innerHTML = ''; totalTokens = 0; tokenEl.textContent = '0 tk'; }

    return { addMessage, addToolCard, updateToolCard, setLoading, addTokens, clearMessages };
})();
