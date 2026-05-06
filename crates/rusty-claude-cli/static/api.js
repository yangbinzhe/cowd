const API = (() => {
    const base = '';
    let sid = null;

    return {
        get base() { return base; },
        get sessionId() { return sid; },
        set sessionId(v) { sid = v; },

        async request(method, path, body) {
            const opts = { method, headers: { 'Content-Type': 'application/json' } };
            if (body) opts.body = JSON.stringify(body);
            const r = await fetch(`${base}${path}`, opts);
            if (!r.ok) throw new Error(await r.text());
            return r.json();
        },

        async newSession(model) {
            const d = await this.request('POST', '/api/sessions', { model: model || 'claude-sonnet-4-6' });
            sid = d.id || d.session_id;
            return d;
        },

        async listSessions() {
            const d = await this.request('GET', '/api/sessions');
            return { sessions: (d || []).map(s => ({ id: s.id, title: s.title || ('Session ' + s.id?.slice(0,8)), started_at: s.created_at || Date.now()/1000, input_tokens: 0, output_tokens: 0 })) };
        },

        async deleteSession(id) {
            return this.request('DELETE', `/api/sessions/${id}`);
        },

        async submitPrompt(text) {
            return this.request('POST', `/api/sessions/${sid}/messages`, { content: text, role: 'user' });
        },

        getStreamUrl() {
            return `${base}/api/sessions/${sid}/messages/stream`;
        }
    };
})();
