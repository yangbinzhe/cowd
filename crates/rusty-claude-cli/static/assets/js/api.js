/**
 * Cowd API - API Communication Module
 * 处理与后端的所有 API 通信
 */

// =============================================================================
// API 配置 - 支持多层配置
// =============================================================================

const API_CONFIG = {
    // API 基础地址
    get baseUrl() {
        // 1. 最高优先级：窗口变量
        if (window.COWD_API_BASE) {
            return window.COWD_API_BASE.replace(/\/$/, '');
        }
        // 2. 默认：同源
        return '';
    },

    // WebSocket 地址
    get wsUrl() {
        // 1. 窗口变量
        if (window.COWD_WS_URL) {
            return window.COWD_WS_URL;
        }
        // 2. 自动构建
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.COWD_WS_HOST || location.host;
        return `${proto}//${host}/ws`;
    },

    // Session 事件 WebSocket 地址
    get wsSessionsUrl() {
        // 1. 窗口变量
        if (window.COWD_WS_SESSIONS_URL) {
            return window.COWD_WS_SESSIONS_URL;
        }
        // 2. 自动构建
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.COWD_WS_HOST || location.host;
        return `${proto}//${host}/ws/sessions`;
    },

    // Gateway 地址 (带协议)
    get gatewayUrl() {
        if (window.COWD_GATEWAY_URL) {
            return window.COWD_GATEWAY_URL;
        }
        return `${location.protocol}//${location.host}`;
    },

    // 超时配置 (毫秒)
    timeout: window.COWD_API_TIMEOUT || 30000,

    // 重试次数
    retries: window.COWD_API_RETRIES || 3,

    // 认证 Token 存储键
    tokenStorageKey: 'cowd-token',
};

class CowdApi {
  constructor() {
    this.baseUrl = API_CONFIG.baseUrl + '/api';
    this.token = localStorage.getItem(API_CONFIG.tokenStorageKey) || '';
    this._listeners = {};
  }

  /**
   * Set auth token
   */
  setToken(token) {
    this.token = token;
    localStorage.setItem(API_CONFIG.tokenStorageKey, token);
  }

  /**
   * Clear auth token
   */
  clearToken() {
    this.token = '';
    localStorage.removeItem(API_CONFIG.tokenStorageKey);
  }

  /**
   * Check if authenticated
   */
  isAuthenticated() {
    return !!this.token;
  }

  /**
   * Get headers for requests
   */
  _getHeaders() {
    const headers = {
      'Content-Type': 'application/json'
    };
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }
    return headers;
  }

  /**
   * Generic request handler
   */
  async _request(method, endpoint, data = null, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    const config = {
      method,
      headers: this._getHeaders()
    };

    if (data && method !== 'GET') {
      config.body = JSON.stringify(data);
    }

    if (options.signal) {
      config.signal = options.signal;
    }

    try {
      const response = await fetch(url, config);

      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        throw new ApiError(
          error.message || `HTTP ${response.status}`,
          response.status,
          error
        );
      }

      // Handle SSE responses
      if (options.stream && response.body) {
        return response.body;
      }

      return response.json();
    } catch (error) {
      if (error.name === 'AbortError') {
        throw new ApiError('Request cancelled', 0);
      }
      throw error;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Auth API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Login with token
   */
  async login(token) {
    const result = await this._request('POST', '/auth/login', { token });
    this.setToken(result.token || token);
    return result;
  }

  /**
   * Logout
   */
  async logout() {
    try {
      await this._request('POST', '/auth/logout');
    } finally {
      this.clearToken();
    }
  }

  /**
   * Verify current token
   */
  async verifyToken() {
    try {
      const result = await this._request('GET', '/auth/verify');
      return result;
    } catch {
      this.clearToken();
      return null;
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // Sessions API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * List all sessions
   */
  async listSessions() {
    return this._request('GET', '/sessions');
  }

  /**
   * Get session by ID
   */
  async getSession(id) {
    return this._request('GET', `/sessions/${id}`);
  }

  /**
   * Create new session
   * @param {Object|string} options - Title string or { title, model }
   */
  async createSession(options = null) {
    const data = typeof options === 'string' ? { title: options } : (options || {});
    return this._request('POST', '/sessions', data);
  }

  /**
   * Delete session
   */
  async deleteSession(id) {
    return this._request('DELETE', `/sessions/${id}`);
  }

  /**
   * Update session
   */
  async updateSession(id, data) {
    return this._request('PATCH', `/sessions/${id}`, data);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Messages API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Get messages for session
   */
  async getMessages(sessionId, options = {}) {
    const params = new URLSearchParams();
    if (options.limit) params.set('limit', options.limit);
    if (options.before) params.set('before', options.before);

    const query = params.toString() ? `?${params.toString()}` : '';
    return this._request('GET', `/sessions/${sessionId}/messages${query}`);
  }

  /**
   * Splice (delete) messages from index onwards
   * Used for message editing and regeneration
   */
  async spliceMessages(sessionId, index) {
    return this._request('DELETE', `/sessions/${sessionId}/messages/${index}`);
  }

  /**
   * Send message (streaming)
   */
  async sendMessage(sessionId, content, options = {}) {
    const { onChunk, onComplete, onError, signal } = options;

    const response = await fetch(
      `${this.baseUrl}/sessions/${sessionId}/messages`,
      {
        method: 'POST',
        headers: this._getHeaders(),
        body: JSON.stringify({ content }),
        signal
      }
    );

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new ApiError(
        error.message || `HTTP ${response.status}`,
        response.status,
        error
      );
    }

    if (response.headers.get('Content-Type')?.includes('text/event-stream')) {
      return this._handleSSE(response, { onChunk, onComplete, onError });
    }

    return response.json();
  }

  /**
   * Handle Server-Sent Events (supports named events like tool_start, tool_progress, tool_complete)
   */
  async _handleSSE(response, callbacks) {
    const { onChunk, onComplete, onError } = callbacks;
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let fullContent = '';
    let currentEvent = '';  // Track current SSE event type

    try {
      while (true) {
        const { done, value } = await reader.read();

        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          // Track named event type
          if (line.startsWith('event: ')) {
            currentEvent = line.slice(7).trim();
            continue;
          }

          if (line.startsWith('data: ')) {
            const rawData = line.slice(6);
            try {
              const data = JSON.parse(rawData);

              // Handle named SSE events (P0-2 tool visualization)
              if (currentEvent === 'tool_start') {
                window.Messages?.handleToolStart(data);
                currentEvent = '';
                continue;
              } else if (currentEvent === 'tool_progress') {
                window.Messages?.handleToolProgress(data);
                currentEvent = '';
                continue;
              } else if (currentEvent === 'tool_complete') {
                window.Messages?.handleToolComplete(data);
                currentEvent = '';
                continue;
              } else if (currentEvent === 'approval_request') {
                window.Messages?.handleApprovalRequest(data);
                currentEvent = '';
                continue;
              } else if (currentEvent === 'approval_resolved') {
                window.Messages?.handleApprovalResolved(data);
                currentEvent = '';
                continue;
              }

              // Default data events (chat streaming)
              if (data.type === 'chunk') {
                fullContent += data.content;
                onChunk?.(data.content, fullContent);
              } else if (data.type === 'done') {
                onComplete?.(fullContent, data);
              } else if (data.type === 'error') {
                onError?.(new Error(data.message));
              }
            } catch {
              // Ignore parse errors for partial data
            }
            currentEvent = '';  // Reset after processing data line
          }
        }
      }
    } catch (error) {
      onError?.(error);
    }

    return { content: fullContent };
  }

  // ═══════════════════════════════════════════════════════════════════
  // Memory API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Get memory statistics
   */
  async getMemoryStats() {
    return this._request('GET', '/memory/stats');
  }

  /**
   * Get memory layers
   */
  async getMemoryLayers() {
    return this._request('GET', '/memory/layers');
  }

  /**
   * Search memory
   */
  async searchMemory(query, options = {}) {
    const params = new URLSearchParams({ q: query });
    if (options.limit) params.set('limit', options.limit);
    if (options.layer) params.set('layer', options.layer);
    // P0-3: Support search mode (vector/bm25/hybrid)
    if (options.mode) params.set('mode', options.mode);

    return this._request('GET', `/memory/search?${params}`);
  }

  /**
   * Add memory entry
   */
  async addMemory(content, layer = 'essential') {
    return this._request('POST', '/memory', { content, layer });
  }

  /**
   * Delete memory entry
   */
  async deleteMemory(id) {
    return this._request('DELETE', `/memory/${id}`);
  }

  /**
   * Get memory overview (stats + layers combined)
   */
  async getMemory() {
    const [stats, layers] = await Promise.all([
      this.getMemoryStats().catch(() => ({})),
      this.getMemoryLayers().catch(() => ([]))
    ]);
    return { stats, layers };
  }

  /**
   * Get single memory entry
   */
  async getMemoryEntry(id) {
    return this._request('GET', `/memory/${id}`);
  }

  /**
   * Update memory entry
   */
  async updateMemory(id, data) {
    return this._request('PATCH', `/memory/entry/${id}`, data);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Config API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Get configuration
   */
  async getConfig() {
    return this._request('GET', '/config');
  }

  /**
   * Update configuration
   */
  async updateConfig(data) {
    return this._request('PUT', '/config', data);
  }

  /**
   * Get available models
   */
  async getModels() {
    return this._request('GET', '/models');
  }

  /**
   * Get available providers
   */
  async getProviders() {
    return this._request('GET', '/providers');
  }

  // ═══════════════════════════════════════════════════════════════════
  // Platform API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * List platforms
   */
  async listPlatforms() {
    return this._request('GET', '/platforms');
  }

  /**
   * Get platform status
   */
  async getPlatformStatus(platformId) {
    return this._request('GET', `/platforms/${platformId}/status`);
  }

  /**
   * Update platform config
   */
  async updatePlatform(platformId, config) {
    return this._request('PATCH', `/platforms/${platformId}`, config);
  }

  /**
   * Disconnect a platform
   */
  async disconnectPlatform(platformId) {
    return this._request('POST', `/platforms/${platformId}/disconnect`);
  }

  /**
   * Connect a platform
   */
  async connectPlatform(platformId, config) {
    return this._request('POST', `/platforms/${platformId}/connect`, config);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Approval API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Get pending approval requests
   */
  async getPendingApprovals() {
    return this._request('GET', '/approval/pending');
  }

  /**
   * Respond to an approval request
   * @param {string} requestId - The approval request ID
   * @param {string} verdict - 'Approved' or 'Denied'
   * @param {string} persistence - 'once', 'session', or 'always'
   */
  async respondApproval(requestId, verdict, persistence) {
    return this._request('POST', '/approval/respond', {
      request_id: requestId,
      verdict,
      persistence,
    });
  }

  // ═══════════════════════════════════════════════════════════════════
  // Health API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Health check
   */
  async health() {
    return this._request('GET', '/health');
  }

  /**
   * Get system status
   */
  async getStatus() {
    return this._request('GET', '/status');
  }

  // ═══════════════════════════════════════════════════════════════════
  // Workspace API
  // ═══════════════════════════════════════════════════════════════════

  /**
   * List workspaces
   */
  async listWorkspaces() {
    return this._request('GET', '/workspaces');
  }

  /**
   * Get workspace details
   */
  async getWorkspace(name) {
    if (!name) {
      return this._request('GET', '/workspace');
    }
    return this._request('GET', `/workspaces/${encodeURIComponent(name)}`);
  }

  /**
   * List files in workspace
   */
  async listFiles(workspace, path = '') {
    const params = path ? `?path=${encodeURIComponent(path)}` : '';
    return this._request('GET', `/workspaces/${encodeURIComponent(workspace)}/files${params}`);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Connection Management
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Disconnect all active connections (SSE, WebSocket)
   */
  disconnect() {
    if (this._eventSource) {
      this._eventSource.close();
      this._eventSource = null;
    }
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
    this._token = '';
    localStorage.removeItem(API_CONFIG.tokenStorageKey);
  }

  // ═══════════════════════════════════════════════════════════════════
  // Event System
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Subscribe to events
   */
  on(event, callback) {
    if (!this._listeners[event]) {
      this._listeners[event] = [];
    }
    this._listeners[event].push(callback);
    return () => this.off(event, callback);
  }

  /**
   * Unsubscribe from events
   */
  off(event, callback) {
    if (!this._listeners[event]) return;
    this._listeners[event] = this._listeners[event].filter(cb => cb !== callback);
  }

  /**
   * Emit event
   */
  emit(event, data) {
    if (!this._listeners[event]) return;
    this._listeners[event].forEach(cb => cb(data));
  }

  // ═══════════════════════════════════════════════════════════════════
  // WebSocket Connections
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Connect to session events WebSocket
   * @param {Object} callbacks - Event callbacks
   * @returns {WebSocket} WebSocket instance
   */
  connectSessionEvents(callbacks = {}) {
    const { onMessage, onOpen, onClose, onError } = callbacks;
    const ws = new WebSocket(API_CONFIG.wsSessionsUrl);

    ws.onopen = () => {
      console.log('Session events WebSocket connected');
      onOpen?.();
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onMessage?.(data);
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e);
      }
    };

    ws.onclose = () => {
      console.log('Session events WebSocket disconnected');
      onClose?.();
    };

    ws.onerror = (error) => {
      console.error('Session events WebSocket error:', error);
      onError?.(error);
    };

    return ws;
  }

  // ── P1-5: Cron Scheduler API ──────────────────────────────────────────────

  /** List all cron jobs */
  async listCrons() {
    return this._request('GET', '/crons');
  }

  /** Create a new cron job */
  async createCron({ name, schedule, prompt, grace_window_secs }) {
    return this._request('POST', '/crons', { name, schedule, prompt, grace_window_secs: grace_window_secs || 60 });
  }

  /** Delete a cron job */
  async deleteCron(id) {
    return this._request('DELETE', `/crons/${id}`);
  }

  /** Manually trigger a cron job run */
  async runCron(id) {
    return this._request('POST', `/crons/${id}/run`);
  }

  /** Pause a cron job */
  async pauseCron(id) {
    return this._request('POST', `/crons/${id}/pause`);
  }

  /** Resume a cron job */
  async resumeCron(id) {
    return this._request('POST', `/crons/${id}/resume`);
  }

  // ── 3C-3/3C-4: Knowledge Graph, Cron Logs & Approval History API ─────────

  /**
   * Get knowledge graph data for visualization.
   * Backend exposes /memory/entities and /memory/triples;
   * we combine them into the { nodes, edges } format the frontend expects.
   */
  async getKnowledgeGraph() {
    const [entitiesRes, triplesRes] = await Promise.all([
      this._request('GET', '/memory/entities').catch(() => ({ entities: [] })),
      this._request('GET', '/memory/triples').catch(() => ({ triples: [] }))
    ]);

    const entities = entitiesRes.entities || entitiesRes || [];
    const triples = triplesRes.triples || triplesRes || [];

    const nodes = entities.map(e => ({
      id: e.id,
      label: e.name || e.label || e.id,
      layer: e.layer || 'default',
      type: e.type || 'entity',
      ...e
    }));

    const edges = triples.map(t => ({
      source: t.subject || t.source,
      target: t.object || t.target,
      label: t.predicate || t.label || t.relation || '',
      ...t
    }));

    return { nodes, edges };
  }

  /**
   * Get cron execution logs.
   * NOTE: Backend does not yet expose /crons/logs.
   * Returns empty data gracefully so the UI shows "暂无执行记录".
   */
  async getCronLogs(params = {}) {
    const query = new URLSearchParams({ limit: params.limit || 20, offset: params.offset || 0 }).toString();
    return this._request('GET', `/crons/logs?${query}`);
  }

  /**
   * Get execution logs for a specific cron job.
   */
  async getCronJobLogs(cronId, params = {}) {
    const query = new URLSearchParams({ limit: params.limit || 20, offset: params.offset || 0 }).toString();
    return this._request('GET', `/crons/${cronId}/logs?${query}`);
  }

  /**
   * Get approval history.
   */
  async getApprovalHistory(params = {}) {
    const query = new URLSearchParams({ limit: params.limit || 20, offset: params.offset || 0 }).toString();
    return this._request('GET', `/approval/history?${query}`);
  }
}

/**
 * API Error class
 */
class ApiError extends Error {
  constructor(message, status, data = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.data = data;
  }
}

// Create global instance
window.api = new CowdApi();
window.ApiError = ApiError;

// Debug info
console.log('Cowd API Client initialized:', {
    baseUrl: API_CONFIG.baseUrl + '/api',
    wsUrl: API_CONFIG.wsUrl,
    wsSessionsUrl: API_CONFIG.wsSessionsUrl,
    gatewayUrl: API_CONFIG.gatewayUrl,
    timeout: API_CONFIG.timeout,
    retries: API_CONFIG.retries,
});
