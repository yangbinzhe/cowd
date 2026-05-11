window.Sessions = (()=>{
  let sessions=[];
  let activeId=null;
  let totalTokens=0;

  async function load(){
    try{
      const data=await Api.listSessions();
      sessions=data.sessions||[];
      render();
    }catch(e){UI.showToast('Failed to load sessions','error')}
  }

  function render(){
    const list=UI.$('session-list');
    list.innerHTML='';
    sessions.forEach(s=>{
      const li=UI.el('li','session-item'+(s.id===activeId?' active':''));
      const title=UI.el('span','si-title');
      title.textContent=s.title||'Untitled';
      const model=UI.el('span','si-model');
      model.textContent=s.model||'';
      const menu=UI.el('span','si-menu');
      const delBtn=UI.el('button');
      delBtn.innerHTML='&xutrif;';
      delBtn.title='Delete';
      delBtn.onclick=e=>{e.stopPropagation();deleteSession(s.id)};
      menu.appendChild(delBtn);
      li.appendChild(title);li.appendChild(model);li.appendChild(menu);
      li.onclick=()=>selectSession(s.id);
      list.appendChild(li);
    });
  }

  async function selectSession(id){
    activeId=id;Api.sid=id;
    render();
    UI.$('chat-messages').innerHTML='';
    Messages.disconnect();
    try{
      const msgs=await Api.getMessages(id);
      if(msgs&&msgs.length)msgs.forEach(m=>{
        if(m.role==='user')Messages.addUserMessage(m.content);
        else if(m.role==='assistant'){
          const el=UI.el('div','message assistant');
          const body=UI.el('div','msg-body');
          body.innerHTML=UI.renderMd(m.content||'');
          el.appendChild(body);
          UI.$('chat-messages').appendChild(el);
        }
      });
    }catch(e){}
    Messages.connect();
  }

  async function deleteSession(id){
    try{await Api.deleteSession(id);sessions=sessions.filter(s=>s.id!==id);if(id===activeId){activeId=null;Api.sid=null}render()}catch(e){UI.showToast(e.message,'error')}
  }

  async function createSession(model){
    try{
      const d=await Api.createSession(model||UI.$('model-selector').value||'claude-sonnet-4-6');
      activeId=d.id||d.session_id;
      Api.sid=activeId;
      UI.$('chat-messages').innerHTML='';
      Messages.disconnect();
      Messages.addSystemMessage('New session: '+(activeId||'').slice(0,8)+'...');
      load();
      Messages.connect();
    }catch(e){UI.showToast(e.message,'error')}
  }

  function addTokens(n){
    totalTokens+=n;
    var el=document.getElementById('token-usage');
    if(el)el.textContent=totalTokens+' tk';
    var costEl=document.getElementById('cost-display');
    if(costEl&&totalTokens>0){
      var est=(totalTokens/1000000*15).toFixed(4);
      costEl.textContent='~$'+est;
    }
  }

  async function searchSessions(q){
    if(!q){load();return}
    sessions=sessions.filter(s=>(s.title||'').toLowerCase().includes(q.toLowerCase()));
    render();
  }

  return{load,render,selectSession,deleteSession,createSession,addTokens,searchSessions,sessions:()=>sessions};
})();
