window.addEventListener('DOMContentLoaded',function(){
  applyTheme();
  Sessions.load();
  loadModelSelector();

  document.getElementById('btn-new-session').addEventListener('click',function(){
    Sessions.createSession();
  });

  document.getElementById('btn-send').addEventListener('click',function(){
    const text=document.getElementById('chat-input').value.trim();
    if(!text||!Api.sid)return;
    if(text.startsWith('/')){
      const parts=text.split(/\s+/);
      const cmd=parts[0];
      const args=parts.slice(1).join(' ');
      Commands.execute(cmd,args);
      document.getElementById('chat-input').value='';
    }else{
      Messages.send(text);
    }
  });

  document.getElementById('chat-input').addEventListener('keydown',function(e){
    if(e.key==='Enter'&&!e.shiftKey){
      e.preventDefault();
      document.getElementById('btn-send').click();
    }
  });

  let slashTimer;
  document.getElementById('chat-input').addEventListener('input',function(){
    const val=this.value;
    clearTimeout(slashTimer);
    if(val.startsWith('/')){
      slashTimer=setTimeout(()=>Commands.renderAutocomplete(val),100);
    }else{
      document.getElementById('slash-dropdown').classList.add('hidden');
    }
  });

  document.getElementById('btn-slash').addEventListener('click',function(){
    const input=document.getElementById('chat-input');
    input.value='/';
    input.focus();
    Commands.renderAutocomplete('/');
  });

  document.getElementById('session-search').addEventListener('input',function(){
    Sessions.searchSessions(this.value);
  });

  document.getElementById('model-selector').addEventListener('change',function(){
    Api.sid=null;
    Sessions.createSession(this.value);
  });

  document.querySelectorAll('#panel-tabs button[data-panel]').forEach(function(btn){
    btn.addEventListener('click',function(){
      const panel=this.dataset.panel;
      if(panel==='close'){UI.switchPanel(null);return}
      UI.switchPanel(panel);
    });
  });

  document.getElementById('btn-toggle-panel').addEventListener('click',function(){
    const panel=document.getElementById('right-panel');
    panel.classList.toggle('hidden');
    localStorage.setItem('cowd-panel-open',panel.classList.contains('hidden')?'closed':'open');
  });

  document.getElementById('btn-control-center').addEventListener('click',function(){
    UI.openModal('control-center');
    UI.switchCCTab('config');
  });

  document.querySelectorAll('#control-center .modal-tabs button').forEach(function(btn){
    btn.addEventListener('click',function(){
      UI.switchCCTab(this.dataset.cc);
    });
  });

  restorePanelState();
});

function restorePanelState(){
  const state=localStorage.getItem('cowd-panel-open');
  const panel=document.getElementById('right-panel');
  if(state==='open')panel.classList.remove('hidden');
  else panel.classList.add('hidden');
}

function applyTheme(){
  const theme=localStorage.getItem('cowd-theme')||'dark';
  if(theme==='system'){
    const pref=window.matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light';
    document.documentElement.dataset.theme=pref;
  }else{
    document.documentElement.dataset.theme=theme;
  }
  window.matchMedia('(prefers-color-scheme:dark)').addEventListener('change',function(){
    if(localStorage.getItem('cowd-theme')==='system')applyTheme();
  });
}

async function loadModelSelector(){
  const sel=document.getElementById('model-selector');
  sel.innerHTML='';
  try{
    const cfg=await Api.getConfig();
    const models=[
      cfg.model||'claude-sonnet-4-6',
      'claude-haiku-4-5',
      'claude-opus-4-6',
      'deepseek-v4-pro',
      'deepseek-v4-flash',
      'grok-3',
      'grok-3-mini',
      'qwen3-max',
      'qwen3-coder-next'
    ];
    models.forEach(function(m){
      const opt=document.createElement('option');
      opt.value=m;opt.textContent=m;
      if(m===(cfg.model||models[0]))opt.selected=true;
      sel.appendChild(opt);
    });
    if(cfg.aliases){
      Object.entries(cfg.aliases).forEach(function(a){
        const opt=document.createElement('option');
        opt.value=a[1];opt.textContent=a[0]+' &rarr; '+a[1];
        sel.appendChild(opt);
      });
    }
  }catch(e){
    ['claude-sonnet-4-6','claude-haiku-4-5','claude-opus-4-6'].forEach(function(m){
      const opt=document.createElement('option');
      opt.value=m;opt.textContent=m;
      sel.appendChild(opt);
    });
  }
}
