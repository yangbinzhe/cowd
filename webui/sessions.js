const Sessions = (() => {
    const listEl = document.getElementById('session-list');
    const searchEl = document.getElementById('session-search');
    let sessions = [];
    let activeId = null;

    async function load() {
        try {
            const data = await API.listSessions();
            sessions = data.sessions || [];
            render();
        } catch (e) {
            console.warn('Failed to load sessions:', e);
        }
    }

    function render() {
        const q = (searchEl.value || '').toLowerCase();
        const filtered = sessions.filter(s =>
            !q || (s.title || '').toLowerCase().includes(q) || s.id.toLowerCase().includes(q));

        listEl.innerHTML = '';
        filtered.forEach(s => {
            const li = document.createElement('li');
            li.className = `session-item${s.id === activeId ? ' active' : ''}`;
            li.innerHTML = `
                <span class="session-title">${s.title || 'Untitled'}</span>
                <div class="session-meta">
                    <span>${new Date(s.started_at * 1000).toLocaleDateString()}</span>
                    <span>${formatTokens(s.input_tokens + s.output_tokens)}</span>
                </div>
                <div class="session-actions">
                    <button class="btn-resume" data-id="${s.id}">Resume</button>
                    <button class="btn-delete" data-id="${s.id}">Delete</button>
                </div>`;
            li.addEventListener('click', e => {
                if (e.target.classList.contains('btn-resume')) resume(s.id);
                else if (e.target.classList.contains('btn-delete')) remove(s.id);
                else resume(s.id);
            });
            listEl.appendChild(li);
        });
    }

    async function resume(id) {
        try {
            Stream.disconnect();
            Chat.clearMessages();
            Chat.setLoading(true);
            const d = await API.resumeSession(id);
            activeId = id;
            render();
            Chat.setLoading(false);
            Chat.addMessage('system', `Resumed session: ${id.slice(0, 8)}...`);
            Stream.connect();
            showToast('Session resumed', 'success');
        } catch (e) {
            Chat.setLoading(false);
            showToast(e.message, 'error');
        }
    }

    async function remove(id) {
        if (!confirm('Delete this session?')) return;
        try {
            await API.deleteSession(id);
            sessions = sessions.filter(s => s.id !== id);
            if (activeId === id) activeId = null;
            render();
            showToast('Session deleted', 'success');
        } catch (e) {
            showToast(e.message, 'error');
        }
    }

    function setActive(id) { activeId = id; render(); }

    searchEl.addEventListener('input', render);

    function formatTokens(n) {
        if (n > 1000000) return `${(n/1e6).toFixed(1)}M tk`;
        if (n > 1000) return `${(n/1e3).toFixed(1)}K tk`;
        return `${n} tk`;
    }

    return { load, setActive };
})();
