window.Sessions = (()=>{
  let sessions=[];
  let activeId=null;
  let totalTokens=0;
  let total=0;
  let query='';
  let loadSeq=0;
  const pageSize=20;

  async function load(opts){
    const o=opts||{};
    const append=!!o.append;
    const seq=++loadSeq;
    try{
      const data=await Api.listSessions({
        q: query,
        sort: 'updated_at',
        order: 'desc',
        limit: pageSize,
        offset: append ? sessions.length : 0
      });
      if(seq!==loadSeq)return;
      total=data.total||0;
      sessions=append?sessions.concat(data.sessions||[]):data.sessions||[];
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
    if(sessions.length<total){
      const more=UI.el('li','session-item session-load-more');
      more.textContent='Load more ('+sessions.length+'/'+total+')';
      more.onclick=()=>load({append:true});
      list.appendChild(more);
    }
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
      const selector=UI.$('model-selector');
      const selectedModel=selector&&selector.value?selector.value:'';
      const d=await Api.createSession(model||selectedModel);
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
    query=(q||'').trim();
    await load();
  }

  return{load,render,selectSession,deleteSession,createSession,addTokens,searchSessions,sessions:()=>sessions,total:()=>total};
})();
