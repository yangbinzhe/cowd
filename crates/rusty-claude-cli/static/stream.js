const Stream = (() => {
    let abortController = null;
    let reconnectTimer = null;
    let callbacks = {};

    async function connect() {
        if (!API.sessionId) return;
        disconnect();
        abortController = new AbortController();
        try {
            const resp = await fetch(API.getStreamUrl(), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ listen: true }),
                signal: abortController.signal,
            });
            if (!resp.ok) { scheduleReconnect(); return; }
            const reader = resp.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';
                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        const payload = line.slice(6);
                        try {
                            if (payload === '[DONE]') continue;
                            const d = JSON.parse(payload);
                            if (d.type === 'content_block_delta' && d.delta?.text) {
                                if (callbacks.messageDelta) callbacks.messageDelta(d.delta.text);
                            } else if (d.content) {
                                if (callbacks.messageDelta) callbacks.messageDelta(d.content);
                            }
                        } catch (_) {
                            if (callbacks.messageDelta) callbacks.messageDelta(payload);
                        }
                    }
                }
            }
        } catch (e) {
            if (e.name !== 'AbortError') scheduleReconnect();
        }
    }

    function disconnect() {
        if (abortController) { abortController.abort(); abortController = null; }
        if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    }

    function scheduleReconnect() {
        if (reconnectTimer) return;
        reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, 3000);
    }

    function on(event, fn) { callbacks[event] = fn; }

    return { connect, disconnect, on };
})();
