// M8: window.State — reactive state management for vanilla JS WebUI.
// Derived from hermes-webui state management + opencode component pattern.
// Enables data-driven rendering without a framework dependency.

(function() {
  const SEP = '.';

  class StateManager {
    constructor() {
      this._data = {
        sessions: [],
        activeSessionId: null,
        panels: { active: 'memory' },
        stream: { toolCards: [], text: '', streaming: false },
        memory: { entries: null, entities: null, triples: null },
        skills: null, crons: null, platforms: null,
      };
      this._listeners = {};
    }

    get(path) {
      if (!path || path === '') return this._data;
      const keys = path.split(SEP);
      let obj = this._data;
      for (const k of keys) {
        if (obj == null || typeof obj !== 'object') return undefined;
        obj = obj[k];
      }
      return obj;
    }

    set(path, value) {
      const keys = path.split(SEP);
      let obj = this._data;
      for (let i = 0; i < keys.length - 1; i++) {
        if (!(keys[i] in obj)) obj[keys[i]] = {};
        obj = obj[keys[i]];
      }
      obj[keys[keys.length - 1]] = value;
      this._notify(path, value);
    }

    on(path, callback) {
      if (!this._listeners[path]) this._listeners[path] = [];
      this._listeners[path].push(callback);
      return () => { this._listeners[path] = this._listeners[path].filter(c => c !== callback); };
    }

    _notify(path, value) {
      if (this._listeners[path]) {
        for (const cb of this._listeners[path]) cb(value);
      }
      const parentPath = path.substring(0, path.lastIndexOf(SEP));
      if (parentPath && this._listeners[parentPath]) {
        for (const cb of this._listeners[parentPath]) cb(this.get(parentPath));
      }
    }

    session() { return this.get('sessions').find(s => s.id === this.get('activeSessionId')); }
  }

  window.State = new StateManager();
})();
