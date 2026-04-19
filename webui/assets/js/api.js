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
   */
  async createSession(title = null) {
    return this._request('POST', '/sessions', { title });
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
   * Handle Server-Sent Events
   */
  async _handleSSE(response, callbacks) {
    const { onChunk, onComplete, onError } = callbacks;
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let fullContent = '';

    try {
      while (true) {
        const { done, value } = await reader.read();

        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));

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
