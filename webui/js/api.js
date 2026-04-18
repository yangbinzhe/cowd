/**
 * Cowd API - API Communication Module
 * 处理与后端的所有 API 通信
 */

class CowdApi {
  constructor() {
    this.baseUrl = '/api';
    this.token = localStorage.getItem('cowd-token') || '';
    this._listeners = {};
  }

  /**
   * Set auth token
   */
  setToken(token) {
    this.token = token;
    localStorage.setItem('cowd-token', token);
  }

  /**
   * Clear auth token
   */
  clearToken() {
    this.token = '';
    localStorage.removeItem('cowd-token');
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
