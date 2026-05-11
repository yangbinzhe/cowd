window.Api = (()=>{
  const BASE = '';
  let sid = null;

  async function req(method, path, body){
    const opts = {method, headers:{'Content-Type':'application/json'}};
    if(body)opts.body=JSON.stringify(body);
    const r=await fetch(BASE+path,opts);
    if(!r.ok){const t=await r.text();throw new Error(t||`${r.status} ${r.statusText}`)}
    return r.json();
  }

  async function reqRaw(method,path,body){
    const opts={method,headers:{'Content-Type':'application/json'}};
    if(body)opts.body=JSON.stringify(body);
    const r=await fetch(BASE+path,opts);
    if(!r.ok){const t=await r.text();throw new Error(t||`${r.status} ${r.statusText}`)}
    return r;
  }

  return {
    get base(){return BASE},
    get sid(){return sid},
    set sid(v){sid=v},

    // ── Sessions ──
    async listSessions(){const d=await req('GET','/api/sessions');return{sessions:(d||[]).map(s=>({id:s.id,title:s.title||'Session '+(s.id||'').slice(0,8),started_at:s.created_at||Date.now()/1000,model:s.model,input_tokens:s.input_tokens||0,output_tokens:s.output_tokens||0}))}},
    async createSession(model){const d=await req('POST','/api/sessions',{model:model||'claude-sonnet-4-6'});sid=d.id||d.session_id;return d},
    async getSession(id){return req('GET','/api/sessions/'+(id||sid))},
    async deleteSession(id){return req('DELETE','/api/sessions/'+(id||sid))},
    async compactSession(id){return req('POST','/api/sessions/'+(id||sid)+'/compact')},
    async getMessages(id){return req('GET','/api/sessions/'+(id||sid)+'/messages')},
    async sendMessage(text,id){return req('POST','/api/sessions/'+(id||sid)+'/messages',{content:text,role:'user'})},
    getStreamUrl(id){return BASE+'/api/sessions/'+(id||sid)+'/messages/stream'},

    // ── Auth ──
    async login(pw){return req('POST','/api/auth/login',{password:pw})},
    async verifyAuth(){return req('GET','/api/auth/verify')},
    async logout(){return req('POST','/api/auth/logout')},

    // ── Config ──
    async getConfig(){return req('GET','/api/config')},
    async updateConfig(cfg){return req('PUT','/api/config',cfg)},
    async getProviders(){return req('GET','/api/config/providers')},

    // ── Memory ──
    async memoryStatus(){return req('GET','/api/memory')},
    async memoryStats(){return req('GET','/api/memory/stats')},
    async listMemoryLayers(){return req('GET','/api/memory/layers')},
    async searchMemory(q){return req('GET','/api/memory/search?q='+encodeURIComponent(q))},
    async getMemoryLayer(layer){return req('GET','/api/memory/'+layer)},
    async createMemoryEntry(layer,entry){return req('POST','/api/memory/'+layer,entry)},
    async updateMemoryEntry(id,entry){return req('PATCH','/api/memory/entry/'+id,entry)},
    async deleteMemoryEntry(layer,id){return req('DELETE','/api/memory/'+layer+'/'+id)},
    async listEntities(){return req('GET','/api/memory/entities')},
    async detectEntities(text){return req('POST','/api/memory/entities/detect',{text})},
    async listTriples(){return req('GET','/api/memory/triples')},
    async addTriple(s,p,o){return req('POST','/api/memory/triples',{subject:s,predicate:p,object:o})},
    async checkFacts(facts){return req('POST','/api/memory/facts/check',{facts})},
    async registerFacts(facts){return req('POST','/api/memory/facts/register',{facts})},
    async auditFacts(){return req('GET','/api/memory/facts/audit')},

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
    async uploadFile(formData){const r=await fetch(BASE+'/api/upload',{method:'POST',body:formData});if(!r.ok)throw new Error(await r.text());return r.json()},

    // ── Approval ──
    async pendingApprovals(){return req('GET','/api/approval/pending')},
    async respondApproval(id,approved){return req('POST','/api/approval/respond',{id,approved})},
    async getApprovalConfig(){return req('GET','/api/approval/config')},
    async updateApprovalConfig(cfg){return req('PUT','/api/approval/config',cfg)},
    async toggleYolo(){return req('POST','/api/approval/yolo')},
    async approvalHistory(){return req('GET','/api/approval/history')},

    // ── Commands ──
    async listCommands(){return req('GET','/api/commands')},
    async commandHistory(){return req('GET','/api/commands/history')},
    async executeCommand(cmd,args){return req('POST','/api/commands/execute',{command:cmd,args})},

    // ── Platforms ──
    async listPlatforms(){return req('GET','/api/platforms')},
    async getPlatform(name){return req('GET','/api/platforms/'+name)},

    // ── Usage ──
    async getUsage(){return req('GET','/api/usage')},
  };
})();

window.API = window.Api;
