window.Api = (()=>{
  const BASE = '';
  let sid = null;
  let authToken = localStorage.getItem('cowd-auth-token') || null;

  function buildHeaders(){
    const h = {'Content-Type':'application/json'};
    if(authToken) h['Authorization'] = 'Bearer ' + authToken;
    return h;
  }

  async function req(method, path, body){
    const opts = {method, headers: buildHeaders()};
    if(body)opts.body=JSON.stringify(body);
    const r=await fetch(BASE+path,opts);
    if(r.status===401){
      authToken=null;
      localStorage.removeItem('cowd-auth-token');
      if(typeof window.showLoginModal==='function')window.showLoginModal();
      throw new Error('Authentication required. Please login.');
    }
    if(!r.ok){const t=await r.text();throw new Error(t||`${r.status} ${r.statusText}`)}
    return r.json();
  }

  async function reqRaw(method,path,body){
    const opts={method,headers:buildHeaders()};
    if(body)opts.body=JSON.stringify(body);
    const r=await fetch(BASE+path,opts);
    if(r.status===401){
      authToken=null;
      localStorage.removeItem('cowd-auth-token');
      throw new Error('Authentication required');
    }
    if(!r.ok){const t=await r.text();throw new Error(t||`${r.status} ${r.statusText}`)}
    return r;
  }

  function normalizeSession(raw){
    const id = raw.id || raw.session_id || '';
    const meta = raw.metadata || {};
    return {
      id,
      title: raw.title || meta.title || 'Session ' + id.slice(0, 8),
      started_at: raw.created_at || raw.created_at_ms || Date.now(),
      updated_at: raw.updated_at || raw.updated_at_ms || raw.last_activity || raw.created_at || raw.created_at_ms || Date.now(),
      model: raw.model || '',
      status: raw.status || 'active',
      input_tokens: raw.input_tokens || 0,
      output_tokens: raw.output_tokens || 0
    };
  }

  function blockText(block){
    if(!block)return '';
    if(typeof block === 'string')return block;
    return block.text || block.content || block.output || block.thinking || '';
  }

  function normalizeMessage(raw){
    const blocks = raw.blocks || [];
    return {
      ...raw,
      content: raw.content || blocks.map(blockText).filter(Boolean).join(''),
      blocks
    };
  }

  function normalizeEvent(raw){
    const payload = raw.payload || {};
    return {
      ...raw,
      type: raw.type || raw.event_type || payload.type || '',
      sequence: raw.sequence ?? payload.sequence ?? 0,
      payload
    };
  }

  return {
    get base(){return BASE},
    get sid(){return sid},
    set sid(v){sid=v},
    get token(){return authToken},
    set token(v){authToken=v;if(v)localStorage.setItem('cowd-auth-token',v);else localStorage.removeItem('cowd-auth-token')},

    // ── Sessions ──
    async listSessions(opts){
      const o=opts||{};
      const params = new URLSearchParams();
      ['q','model','status','sort','order','limit','offset'].forEach(function(k){
        if(o[k] !== undefined && o[k] !== null && o[k] !== '')params.set(k,o[k]);
      });
      const query = params.toString() ? '?' + params.toString() : '';
      const d=await req('GET','/api/sessions'+query);
      const rows = Array.isArray(d) ? d : (d.sessions || []);
      return {
        sessions: rows.map(normalizeSession),
        total: d.total ?? rows.length,
        offset: d.offset || 0,
        limit: d.limit || rows.length
      };
    },
    async createSession(model){const d=await req('POST','/api/sessions',{model:model||'claude-sonnet-4-6'});sid=d.id||d.session_id;return d},
    async getSession(id){return req('GET','/api/sessions/'+(id||sid))},
    async deleteSession(id){return req('DELETE','/api/sessions/'+(id||sid))},
    async compactSession(id){return req('POST','/api/sessions/'+(id||sid)+'/compact')},
    async getMessages(id, opts){
      const o=opts||{};
      const params = new URLSearchParams();
      if(o.offset !== undefined)params.set('offset', o.offset);
      if(o.from_seq !== undefined)params.set('from_seq', o.from_seq);
      if(o.limit !== undefined)params.set('limit', o.limit);
      const query = params.toString() ? '?' + params.toString() : '';
      const d=await req('GET','/api/sessions/'+(id||sid)+'/messages'+query);
      const rows = Array.isArray(d) ? d : (d.messages || []);
      return rows.map(normalizeMessage);
    },
    async getEvents(id, opts){
      const p = opts || {};
      const params = new URLSearchParams();
      if(p.from_seq !== undefined)params.set('from_seq', p.from_seq);
      if(p.limit !== undefined)params.set('limit', p.limit);
      const query = params.toString() ? '?' + params.toString() : '';
      const d=await req('GET','/api/sessions/'+(id||sid)+'/events'+query);
      const rows = Array.isArray(d) ? d : (d.events || []);
      return rows.map(normalizeEvent);
    },
    async sendMessage(text,id){return req('POST','/api/sessions/'+(id||sid)+'/messages',{content:text,role:'user'})},
    getStreamUrl(id){return BASE+'/api/sessions/'+(id||sid)+'/stream'},

    // ── Auth ──
    async login(token){
      const r=await req('POST','/api/auth/login',{token:token});
      if(r.success&&r.token){authToken=r.token;localStorage.setItem('cowd-auth-token',r.token)}
      return r;
    },
    async verifyAuth(){return req('GET','/api/auth/verify')},
    async logout(){authToken=null;localStorage.removeItem('cowd-auth-token');return req('POST','/api/auth/logout')},

    // ── Config ──
    async getConfig(){return req('GET','/api/config')},
    async updateConfig(cfg){return req('PUT','/api/config',cfg)},
    async getProviders(){return req('GET','/api/config/providers')},
    async listProfiles(){return req('GET','/api/profiles')},
    async createProfile(name){return req('POST','/api/profiles',{name})},
    async switchProfile(profile){return req('POST','/api/profiles/switch',{profile})},
    async deleteProfile(id){return req('DELETE','/api/profiles/'+encodeURIComponent(id))},

    // ── Memory ──
    async memoryStatus(){return req('GET','/api/memory/status')},
    async memoryStats(){return req('GET','/api/memory/stats')},
    async listMemoryLayers(){return req('GET','/api/memory/layers')},
    async searchMemory(q){return req('GET','/api/memory/search?q='+encodeURIComponent(q))},
    async recallExplain(q,limit){return req('GET','/api/memory/recall/explain?q='+encodeURIComponent(q)+'&limit='+(limit||10))},
    async memoryPacket(q,opts){
      const o=opts||{};
      const params=new URLSearchParams();
      params.set('q',q||'');
      if(o.max_items)params.set('max_items',o.max_items);
      if(o.max_tokens)params.set('max_tokens',o.max_tokens);
      return req('GET','/api/memory/packet?'+params.toString());
    },
    async memoryLinks(){return req('GET','/api/memory/links')},
    async memoryMaintenance(opts){
      const o=opts||{};
      const params=new URLSearchParams();
      if(o.status)params.set('status',o.status);
      if(o.kind)params.set('kind',o.kind);
      if(o.source)params.set('source',o.source);
      if(o.limit)params.set('limit',o.limit);
      const query=params.toString();
      return req('GET','/api/memory/maintenance'+(query?'?'+query:''));
    },
    async scanMemoryMaintenance(opts){return req('POST','/api/memory/maintenance',opts||{})},
    async updateMemoryMaintenance(id,status){return req('PATCH','/api/memory/maintenance/'+encodeURIComponent(id),{status})},
    async currentContext(opts){
      const o=opts||{};
      const params=new URLSearchParams();
      if(o.q)params.set('q',o.q);
      if(o.session_id)params.set('session_id',o.session_id);
      if(o.profile)params.set('profile',o.profile);
      const query=params.toString();
      return req('GET','/api/context/current'+(query?'?'+query:''));
    },
    async contextHistory(sessionId,opts){
      const sid=sessionId||this.sid;
      const o=opts||{};
      const params=new URLSearchParams();
      if(o.from_seq!==undefined)params.set('from_seq',o.from_seq);
      if(o.limit!==undefined)params.set('limit',o.limit);
      params.set('include_envelopes',o.include_envelopes===true?'true':'false');
      const query=params.toString();
      return req('GET','/api/sessions/'+encodeURIComponent(sid)+'/context'+(query?'?'+query:''));
    },
    async runtimeRuns(sessionId,opts){
      const sid=sessionId||this.sid;
      const o=opts||{};
      const params=new URLSearchParams();
      if(o.from_seq!==undefined)params.set('from_seq',o.from_seq);
      if(o.limit!==undefined)params.set('limit',o.limit);
      const query=params.toString();
      return req('GET','/api/sessions/'+encodeURIComponent(sid)+'/runs'+(query?'?'+query:''));
    },
    async runtimeTimeline(sessionId,opts){
      const sid=sessionId||this.sid;
      const o=opts||{};
      const params=new URLSearchParams();
      params.set('session_id',sid||'');
      if(o.from_seq!==undefined)params.set('from_seq',o.from_seq);
      if(o.limit!==undefined)params.set('limit',o.limit);
      return req('GET','/api/runtime/timeline?'+params.toString());
    },
    async runtimeEffectiveConfig(){
      return req('GET','/api/runtime/config/effective');
    },
    async runtimeControlPlane(){
      return req('GET','/api/runtime/control-plane');
    },
    async runtimeReloadProviders(){
      return req('POST','/api/runtime/providers/reload');
    },
    async contextEnvelope(envelopeId){
      return req('GET','/api/context/'+encodeURIComponent(envelopeId));
    },
    async resolveEvidence(ref,opts){
      const o=opts||{};
      const params=new URLSearchParams();
      params.set('ref',ref||'');
      if(o.session_id||this.sid)params.set('session_id',o.session_id||this.sid);
      return req('GET','/api/evidence/resolve?'+params.toString());
    },
    async recordContextRecommendation(sessionId,payload){
      const sid=sessionId||this.sid;
      return req('POST','/api/sessions/'+encodeURIComponent(sid)+'/context/recommendations',payload||{});
    },
    async contextRecommendationStats(sessionId,opts){
      const sid=sessionId||this.sid;
      const o=opts||{};
      const params=new URLSearchParams();
      if(o.from_seq!==undefined)params.set('from_seq',o.from_seq);
      if(o.limit!==undefined)params.set('limit',o.limit);
      const query=params.toString();
      return req('GET','/api/sessions/'+encodeURIComponent(sid)+'/context/recommendations'+(query?'?'+query:''));
    },
    async getMemoryLayer(layer){return req('GET','/api/memory/'+layer)},
    async createMemoryEntry(layer,entry){return req('POST','/api/memory/'+layer,entry)},
    async updateMemoryEntry(id,entry){return req('PATCH','/api/memory/entry/'+id,entry)},
    async deleteMemoryEntry(layer,id){return req('DELETE','/api/memory/'+layer+'/'+id)},
    async linkSymbolToMemory(symbol_id,memory_id,opts){
      const o=opts||{};
      return req('POST','/api/memory/symbol-links',{
        symbol_id,
        memory_id,
        turn_index:o.turn_index,
        reference_type:o.reference_type||'reference'
      });
    },
    async findMemoriesBySymbol(symbol){
      const d=await req('GET','/api/memory/symbol-links?symbol='+encodeURIComponent(symbol));
      return Array.isArray(d) ? d : (d.entries || []);
    },
    async listEntities(){
      const d=await req('GET','/api/memory/entities');
      return Array.isArray(d) ? d : (d.entities || []);
    },
    async detectEntities(text){return req('POST','/api/memory/entities/detect',{text})},
    async listTriples(){
      const d=await req('GET','/api/memory/triples');
      return Array.isArray(d) ? d : (d.triples || []);
    },
    async addTriple(s,p,o){return req('POST','/api/memory/triples',{subject:s,predicate:p,object:o})},
    async checkFacts(facts){return req('POST','/api/memory/facts/check',{facts})},
    async registerFacts(facts){return req('POST','/api/memory/facts/register',{facts})},
    async auditFacts(){return req('GET','/api/memory/facts/audit')},

    // ── Tasks ──
    async taskStatus(){return req('GET','/api/tasks')},
    async startTask(objective,yoloMode){return req('POST','/api/tasks/start',{objective,yolo_mode:!!yoloMode})},
    async startTaskPhase(id,phase){return req('POST','/api/tasks/'+encodeURIComponent(id)+'/phases',phase)},
    async recordTaskPhaseArtifact(id,phaseId,artifact){return req('POST','/api/tasks/'+encodeURIComponent(id)+'/phases/'+encodeURIComponent(phaseId)+'/artifacts',artifact)},
    async reviewTaskPhase(id,phaseId,result,completed){return req('POST','/api/tasks/'+encodeURIComponent(id)+'/phases/'+encodeURIComponent(phaseId)+'/review',{result,completed:!!completed})},
    async cancelTask(id){return req('POST','/api/tasks/'+encodeURIComponent(id)+'/cancel')},
    async completeTask(id){return req('POST','/api/tasks/'+encodeURIComponent(id)+'/complete')},
    async recordTaskFailure(id,reason){return req('POST','/api/tasks/'+encodeURIComponent(id)+'/failure',{reason})},

    // ── Skills ──
    async listSkills(){return req('GET','/v1/skills')},
    async installSkill(name,src){return req('POST','/v1/skills/install',{name,source:src})},
    async viewSkill(name){return req('GET','/v1/skills/'+name)},
    async uninstallSkill(name){return req('DELETE','/v1/skills/'+name)},
    async invokeSkill(name,args){return req('POST','/v1/skills/'+name+'/invoke',{args})},
    async toggleSkill(name){return req('POST','/v1/skills/'+name+'/toggle')},

    // ── Crons ──
    async listCrons(){return req('GET','/api/crons')},
    async createCron(cron){return req('POST','/api/crons',cron)},
    async deleteCron(id){return req('DELETE','/api/crons/'+id)},
    async runCron(id){return req('POST','/api/crons/'+id+'/run')},
    async pauseCron(id){return req('POST','/api/crons/'+id+'/pause')},
    async resumeCron(id){return req('POST','/api/crons/'+id+'/resume')},
    async listCronLogs(id){return req('GET','/api/crons/'+(id||'logs'))},
    async listAllCronLogs(){return req('GET','/api/crons/logs')},

    // ── Workspace ──
    async getWorkspace(){return req('GET','/api/workspace')},
    async listWorkspaces(){return req('GET','/api/workspaces')},
    async listFiles(dir){const p=dir?'?dir='+encodeURIComponent(dir):'';return req('GET','/api/workspace/files'+p)},
    async createFile(path,content){return req('POST','/api/workspace/files',{path,content})},
    async getRawFile(path){return reqRaw('GET','/api/file/raw?path='+encodeURIComponent(path))},
    async uploadFile(formData){const r=await fetch(BASE+'/api/upload',{method:'POST',headers:authToken?{'Authorization':'Bearer '+authToken}:{},body:formData});if(!r.ok)throw new Error(await r.text());return r.json()},

    // ── Approval ──
    async pendingApprovals(){return req('GET','/api/approval/pending')},
    async respondApproval(id,approved,persistence){
      const body={id,approved};
      if(persistence)body.persistence=persistence;
      return req('POST','/api/approval/respond',body);
    },
    async getApprovalConfig(){return req('GET','/api/approval/config')},
    async updateApprovalConfig(cfg){return req('PUT','/api/approval/config',cfg)},
    async toggleSolo(){return req('POST','/api/approval/solo')},
    async approvalHistory(){return req('GET','/api/approval/history')},

    // ── Audit ──
    async exportAudit(opts){
      const o=opts||{};
      const params = new URLSearchParams();
      if(o.source)params.set('source', o.source);
      if(o.limit !== undefined)params.set('limit', o.limit);
      if(o.offset !== undefined)params.set('offset', o.offset);
      const query = params.toString() ? '?' + params.toString() : '';
      return req('GET','/api/audit/export'+query);
    },

    // ── Commands ──
    async listCommands(){return req('GET','/api/commands')},
    async commandHistory(){return req('GET','/api/commands/history')},
    async executeCommand(cmd,args){return req('POST','/api/commands/execute',{command:cmd,args})},

    // ── Platforms ──
    async listPlatforms(){return req('GET','/api/platforms')},
    async getPlatform(name){return req('GET','/api/platforms/'+name)},

    // ── Cross-plane policy ──
    async crossPlaneSummary(){return req('GET','/api/cross-plane/summary')},
    async crossPlaneIdentities(){return req('GET','/api/cross-plane/identities')},
    async createCrossPlaneIdentity(identity){return req('POST','/api/cross-plane/identities',identity)},
    async revokeCrossPlaneIdentity(id){return req('DELETE','/api/cross-plane/identities/'+encodeURIComponent(id))},
    async resolveCrossPlaneIdentity(identityRef){return req('POST','/api/cross-plane/identity/resolve',{identity_ref:identityRef})},
    async crossPlaneGrants(){return req('GET','/api/cross-plane/grants')},
    async createCrossPlaneGrant(grant){return req('POST','/api/cross-plane/grants',grant)},
    async revokeCrossPlaneGrant(id){return req('DELETE','/api/cross-plane/grants/'+encodeURIComponent(id))},
    async crossPlaneAudit(){return req('GET','/api/cross-plane/audit')},
    async crossPlaneActionAdapters(){return req('GET','/api/cross-plane/action/adapters')},
    async simulateCrossPlanePolicy(action){return req('POST','/api/cross-plane/policy/simulate',action)},
    async preflightCrossPlaneAction(action){return req('POST','/api/cross-plane/action/preflight',action)},
    async executeCrossPlaneAction(request){return req('POST','/api/cross-plane/action/execute',request)},
    async wechatIlinkAccounts(){return req('GET','/api/channels/wechat-ilink/accounts')},
    async startWechatIlinkQr(body){return req('POST','/api/channels/wechat-ilink/qr',body||{})},
    async pollWechatIlinkQr(body){return req('POST','/api/channels/wechat-ilink/qr/poll',body)},

    // ── Usage ──
    async getUsage(){return req('GET','/api/usage')},

    // ── Progress ──
    async getProgress(){return {progress: 0}},
  };
})();

window.API = window.Api;
