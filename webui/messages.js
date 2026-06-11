window.Messages = (()=>{
  let abortController=null, reconnectTimer=null, reconnectAttempts=0;
  const callbacks={};
  let currentEvent='', messageBuffer='';
  let activeToolId=null;
  let activeStreamEl=null;
  let activeThinkCard=null;
  let thinkCount=0;
  let answerStarted=false;
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
        method:'GET',
        headers: headers,
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
    el.textContent=connected?'Connected':(reconnectAttempts?'Reconnecting ('+reconnectAttempts+')':'Disconnected');
    el.style.color=connected?'var(--success)':'var(--warn)';
  }

  function handleLine(line){
    if(line.startsWith('event: ')){
      currentEvent=line.slice(7).trim();
      return;
    }
    if(!line.startsWith('data: '))return;
    const payload=line.slice(6);
    if(payload==='[DONE]'){messageBuffer='';return;}
    try{
      const data=JSON.parse(payload);
      if(currentEvent)data._event=currentEvent;
      dispatch(data);
    }catch(e){}
    currentEvent='';
  }

  function dispatch(data){
    data=normalizeEventData(data);
    if(callbacks.all)callbacks.all(data);
    const evt=data._event||data.type||data.event||'';
    switch(evt){
      case'Connected':
        updateConnStatus(true);break;
      case'tool_start':
      case'ToolStart':
        handleToolStart(data);break;
      case'messageDelta':case'content_block_delta':case'text_delta':case'TextDelta':
        handleMessageDelta(data);break;
      case'tool_use':case'content_block_start':
        handleToolUse(data);break;
      case'tool_progress':case'ToolProgress':
        handleToolProgress(data);break;
      case'tool_result':case'tool_complete':case'ToolComplete':
        handleToolResult(data);break;
      case'messageStop':case'message_stop':case'TurnComplete':
        handleMessageStop(data);break;
      case'thinking':case'reasoning':case'ThinkingDelta':
        handleThinking(data);break;
      case'approval':case'approval_required':
        handleApproval(data);break;
      default:
        if(callbacks.default)callbacks.default(data);
    }
  }

  function normalizeEventData(data){
    const payload=(data&&typeof data.payload==='object'&&data.payload)?data.payload:{};
    const block=data.content_block||payload.content_block||{};
    const delta=data.delta||payload.delta||{};
    const merged=Object.assign({}, payload, data);
    if(block&&block.type==='tool_use'){
      merged.type=merged.type||'tool_use';
      merged.id=merged.id||block.id;
      merged.name=merged.name||block.name;
      merged.input=merged.input||block.input;
    }
    if(delta&&delta.text&&!merged.text)merged.text=delta.text;
    if(delta&&delta.thinking&&!merged.thinking)merged.thinking=delta.thinking;
    if(!merged.type&&merged.event_type)merged.type=merged.event_type;
    return merged;
  }

  function handleToolStart(data){
    activeToolId=data.id||data.tool_use_id||'t'+Date.now();
    const el=getOrCreateStreamEl();
    if(el){
      const card=UI.addToolCard(activeToolId,data.name||data.tool_name||'tool','running');
      el.appendChild(card);
      if(data.preview)UI.updateToolCard(activeToolId,data.preview,'running');
    }
  }

  function handleToolProgress(data){
    const id=data.id||data.tool_use_id||activeToolId||'t0';
    const progress=data.progress||data.output||data.summary||'Working...';
    UI.updateToolCard(id,progress,'running');
    scrollToBottom();
  }

  function handleApproval(data){
    const el=getOrCreateStreamEl();
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
    const delta=data.delta||data.text||data.content||'';
    const el=getOrCreateStreamEl();
    if(el){
      const body=el.querySelector('.msg-body')||el;
      messageBuffer+=delta;
      body.innerHTML=UI.renderMd(messageBuffer);
      if(!answerStarted){
        answerStarted=true;
        collapseProcessCards(el);
      }
    }
    scrollToBottom();
  }

  function collapseProcessCards(el){
    if(!el)return;
    el.querySelectorAll('.think-card').forEach(c=>{
      const b=c.querySelector('.think-card-body');
      if(b&&!b.classList.contains('collapsed'))b.classList.add('collapsed');
    });
    el.querySelectorAll('.tool-card').forEach(c=>{
      const b=c.querySelector('.tool-card-body');
      if(b&&!b.classList.contains('collapsed'))b.classList.add('collapsed');
      c.classList.add('dimmed');
    });
  }

  function handleToolUse(data){
    const id=data.id||data.tool_use_id||'t'+Date.now();
    const name=data.name||data.tool_name||'tool';
    activeToolId=id;
    const el=getOrCreateStreamEl();
    if(el){
      const card=UI.addToolCard(id,name,'running');
      el.appendChild(card);
    }
  }

  function handleToolResult(data){
    const id=data.id||data.tool_use_id||activeToolId||'t0';
    const output=data.output||data.content||data.summary||data.error||'';
    const failed=data.is_error||data.error||data.status==='error'||(typeof data.exit_code==='number'&&data.exit_code!==0);
    UI.updateToolCard(id,output,failed?'error':'complete');
    activeToolId=null;
    scrollToBottom();
  }

  function handleMessageStop(data){
    const el=activeStreamEl;
    if(el){
      el.id='';
      el.classList.remove('streaming');
      const response=data.response||data.text||data.assistant_text;
      if(response && !messageBuffer){
        const body=el.querySelector('.msg-body')||el;
        body.innerHTML=UI.renderMd(response);
      }
    }
    messageBuffer='';
    activeStreamEl=null;
    activeThinkCard=null;
    thinkCount=0;
    answerStarted=false;
    if(data&&data.usage)Sessions.addTokens(data.usage.total_tokens||data.usage.output_tokens||0);
    Sessions.load();
  }

  function handleThinking(data){
    const content=data.content||data.text||data.reasoning||'';
    if(!content)return;
    const el=getOrCreateStreamEl();
    if(!el)return;
    thinkCount++;
    if(!activeThinkCard||!activeThinkCard.isConnected){
      activeThinkCard=UI.addThinkCard(content);
      el.appendChild(activeThinkCard);
    }else{
      UI.updateThinkCard(activeThinkCard,content,thinkCount);
    }
    scrollToBottom();
  }

  function getOrCreateStreamEl(){
    if(activeStreamEl&&activeStreamEl.isConnected)return activeStreamEl;
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
    messageBuffer='';
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

  return{connect,disconnect,send,addUserMessage,addSystemMessage,on,approve,_dispatch:dispatch,_handleLine:handleLine};
})();
