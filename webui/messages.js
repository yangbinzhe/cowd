window.Messages = (()=>{
  let abortController=null, reconnectTimer=null, reconnectAttempts=0;
  const callbacks={};
  let streamBuffer='';
  let activeToolId=null;
  let activeStreamEl=null;
  function approve(id,approved){
    Api.respondApproval(id,approved).catch(()=>{});
  }

  function connect(){
    if(!Api.sid)return;
    disconnect();
    abortController=new AbortController();
    try{
      const headers = Api.token ? {'Content-Type':'application/json', 'Authorization':'Bearer '+Api.token} : {'Content-Type':'application/json'};
      fetch(Api.getStreamUrl(),{
        method:'POST',
        headers: headers,
        body:JSON.stringify({listen:true}),
        signal:abortController.signal,
      }).then(resp=>{
        if(!resp.ok){scheduleReconnect();return}
        reconnectAttempts=0;
        updateConnStatus(true);
        const reader=resp.body.getReader();
        const decoder=new TextDecoder();
        let buf='';
        function pump(){
          reader.read().then(({done,value})=>{
            if(done){scheduleReconnect();return}
            buf+=decoder.decode(value,{stream:true});
            const lines=buf.split('\n');
            buf=lines.pop()||'';
            lines.forEach(handleLine);
            pump();
          }).catch(()=>{scheduleReconnect()});
        }
        pump();
      }).catch(()=>{scheduleReconnect()});
    }catch(e){scheduleReconnect()}
  }

  function disconnect(){
    if(abortController){abortController.abort();abortController=null}
    if(reconnectTimer){clearTimeout(reconnectTimer);reconnectTimer=null}
    updateConnStatus(false);
  }

  function scheduleReconnect(){
    if(reconnectTimer)return;
    reconnectAttempts++;
    const delay=Math.min(1000*Math.pow(2,reconnectAttempts-1),30000);
    updateConnStatus(false);
    reconnectTimer=setTimeout(()=>{reconnectTimer=null;connect()},delay);
  }

  function updateConnStatus(connected){
    const el=document.getElementById('connection-status');
    if(!el)return;
    el.textContent=connected?'Connected':'Reconnecting ('+reconnectAttempts+')';
    el.style.color=connected?'var(--success)':'var(--warn)';
  }

  function handleLine(line){
    if(line.startsWith('event: ')){
      streamBuffer=line.slice(7).trim();
      return;
    }
    if(!line.startsWith('data: '))return;
    const payload=line.slice(6);
    if(payload==='[DONE]'){streamBuffer='';return;}
    try{
      const data=JSON.parse(payload);
      if(streamBuffer)data._event=streamBuffer;
      dispatch(data);
    }catch(e){}
    streamBuffer='';
  }

  function dispatch(data){
    if(callbacks.all)callbacks.all(data);
    const evt=data._event||data.type||data.event||'';
    switch(evt){
      case'tool_start':
        handleToolStart(data);break;
      case'messageDelta':case'content_block_delta':case'text_delta':
        handleMessageDelta(data);break;
      case'tool_use':case'content_block_start':
        handleToolUse(data);break;
      case'tool_result':case'tool_complete':
        handleToolResult(data);break;
      case'messageStop':case'message_stop':
        handleMessageStop(data);break;
      case'thinking':case'reasoning':
        handleThinking(data);break;
      case'approval':case'approval_required':
        handleApproval(data);break;
      default:
        if(callbacks.default)callbacks.default(data);
    }
  }

  function handleToolStart(data){
    activeToolId=data.id||'';
    const el=activeStreamEl||getOrCreateStreamEl();
    if(el){
      const card=UI.el('div','tool-card');
      card.id='tool-card-'+activeToolId;
      card.innerHTML='<div class="tool-card-header"><span class="tool-name">&xutri; '+UI.esc(data.name||'tool')+'</span><span class="tool-status running">running</span></div>';
      const body=UI.el('div','tool-card-body');
      body.textContent=UI.esc(data.preview||'')||'Working...';
      card.appendChild(body);
      el.appendChild(card);
    }
  }

  function handleApproval(data){
    const el=activeStreamEl||getOrCreateStreamEl();
    if(el){
      const card=UI.el('div','tool-card');
      card.style.borderColor='var(--warn)';
      card.innerHTML='<div class="tool-card-header" style="color:var(--warn)">&#x26A0; Approval Required</div>';
      const body=UI.el('div','tool-card-body');
      body.innerHTML='<p>'+UI.esc(data.tool||data.action||'Unknown action')+'</p>';
      const approveBtn=UI.el('button','btn-primary');
      approveBtn.textContent='Approve';
      approveBtn.onclick=async()=>{
        try{await Api.respondApproval(data.id||data.approval_id,true);card.remove()}catch(e){UI.showToast(e.message,'error')}
      };
      const denyBtn=UI.el('button','btn-danger');
      denyBtn.textContent='Deny';
      denyBtn.style.marginLeft='8px';
      denyBtn.onclick=async()=>{
        try{await Api.respondApproval(data.id||data.approval_id,false);card.remove()}catch(e){UI.showToast(e.message,'error')}
      };
      body.appendChild(approveBtn);body.appendChild(denyBtn);
      card.appendChild(body);
      el.appendChild(card);
    }
  }

  function handleMessageDelta(data){
    const delta=data.delta||data.text||'';
    const el=activeStreamEl||getOrCreateStreamEl();
    if(el){
      const body=el.querySelector('.msg-body')||el;
      streamBuffer+=delta;
      body.innerHTML=UI.renderMd(streamBuffer);
    }
    scrollToBottom();
  }

  function handleToolUse(data){
    const id=data.id||data.tool_use_id||'t'+Date.now();
    const name=data.name||data.tool_name||'tool';
    activeToolId=id;
    const el=activeStreamEl||getOrCreateStreamEl();
    if(el){
      const card=UI.addToolCard(id,name,'running');
      el.appendChild(card);
    }
  }

  function handleToolResult(data){
    const id=data.id||data.tool_use_id||activeToolId||'t0';
    const output=data.output||data.content||'';
    UI.updateToolCard(id,output,'complete');
    activeToolId=null;
    scrollToBottom();
  }

  function handleMessageStop(data){
    const el=activeStreamEl;
    if(el){
      el.id='';
      el.classList.remove('streaming');
    }
    streamBuffer='';
    activeStreamEl=null;
    if(data&&data.usage)Sessions.addTokens(data.usage.total_tokens||data.usage.output_tokens||0);
    Sessions.load();
  }

  function handleThinking(data){
    const content=data.content||data.text||data.reasoning||'';
    if(!content)return;
    const el=activeStreamEl||getOrCreateStreamEl();
    if(el){
      const card=UI.addThinkCard(content);
      el.appendChild(card);
    }
  }

  function getOrCreateStreamEl(){
    if(activeStreamEl)return activeStreamEl;
    const el=UI.el('div','message assistant streaming');
    el.id='streaming';
    const body=UI.el('div','msg-body');
    el.appendChild(body);
    UI.$('chat-messages').appendChild(el);
    activeStreamEl=el;
    return el;
  }

  async function send(text){
    if(!Api.sid||!text.trim())return;
    UI.$('chat-input').value='';
    addUserMessage(text);
    UI.$('loading-indicator').classList.remove('hidden');
    streamBuffer='';
    try{
      await Api.sendMessage(text);
      connect();
    }catch(e){
      UI.showToast(e.message,'error');
    }finally{
      UI.$('loading-indicator').classList.add('hidden');
    }
  }

  function addUserMessage(text){
    const el=UI.el('div','message user');
    const body=UI.el('div','msg-body');
    body.textContent=text;
    el.appendChild(body);
    UI.$('chat-messages').appendChild(el);
    scrollToBottom();
  }

  function addSystemMessage(text){
    const el=UI.el('div','message system');
    el.textContent=text;
    UI.$('chat-messages').appendChild(el);
    scrollToBottom();
  }

  function scrollToBottom(){
    const mc=UI.$('chat-messages');
    mc.scrollTop=mc.scrollHeight;
  }

  function on(event,fn){callbacks[event]=fn}

  return{connect,disconnect,send,addUserMessage,addSystemMessage,on,approve};
})();
