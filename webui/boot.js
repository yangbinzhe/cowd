window.addEventListener('DOMContentLoaded',async function(){
  applyTheme();
  bindUiEvents();

  try{
    await Api.verifyAuth();
  }catch(e){
    Api.token=null;
    showLoginModal();
    return;
  }

  Sessions.load();
  loadModelSelector();
  restorePanelState();
});

let uiEventsBound = false;

function bindUiEvents(){
  if(uiEventsBound)return;
  uiEventsBound = true;
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

  document.querySelectorAll('#nav-rail .rail-item[data-view]').forEach(function(btn){
    btn.addEventListener('click',function(){
      UI.switchView(this.dataset.view);
      localStorage.setItem('cowd-active-view',this.dataset.view);
    });
  });

  var backToChat=document.getElementById('btn-workbench-chat');
  if(backToChat)backToChat.addEventListener('click',function(){
    UI.switchView('chat');
    localStorage.setItem('cowd-active-view','chat');
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
}

function restorePanelState(){
  const state=localStorage.getItem('cowd-panel-open');
  const panel=document.getElementById('right-panel');
  if(state==='open'){
    panel.classList.remove('hidden');
    UI.syncRail('workspace');
  }else{
    panel.classList.add('hidden');
    UI.syncRail('chat');
  }
  const activeView=localStorage.getItem('cowd-active-view');
  if(activeView&&activeView!=='chat'){
    UI.switchView(activeView);
  }
}

function applyTheme(){
  const theme=localStorage.getItem('cowd-theme')||'dark';
  const skin=localStorage.getItem('cowd-skin')||'graphite';
  const media = typeof window.matchMedia === 'function'
    ? window.matchMedia('(prefers-color-scheme:dark)')
    : null;
  if(theme==='system'){
    const pref=media&&media.matches?'dark':'light';
    document.documentElement.dataset.theme=pref;
  }else{
    document.documentElement.dataset.theme=theme;
  }
  document.documentElement.dataset.skin=skin;
  if(media&&typeof media.addEventListener==='function'){
    media.addEventListener('change',function(){
      if(localStorage.getItem('cowd-theme')==='system')applyTheme();
    });
  }
}

async function loadModelSelector(){
  const sel=document.getElementById('model-selector');
  sel.innerHTML='';
  try{
    const cfg=await Api.getConfig();
    const versionEl=document.getElementById('version');
    if(versionEl&&cfg.version)versionEl.textContent=cfg.version;
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

window.showLoginModal = function(){
  var modal = document.getElementById('login-modal');
  if(!modal) return;
  modal.classList.remove('hidden');
  var input = document.getElementById('login-token');
  var errEl = document.getElementById('login-error');
  if(input) input.focus();
  if(errEl) errEl.style.display = 'none';

  var btn = document.getElementById('btn-login');
  if(btn) btn.onclick = async function(){
    var token = input ? input.value.trim() : '';
    if(!token){
      if(errEl){errEl.textContent='Token is required';errEl.style.display='block'}
      return;
    }
    try{
      var r = await Api.login(token);
      if(r.success){
        modal.classList.add('hidden');
        Sessions.load();
        loadModelSelector();
      }else{
        if(errEl){errEl.textContent=r.message||'Login failed';errEl.style.display='block'}
      }
    }catch(e){
      if(errEl){errEl.textContent=e.message;errEl.style.display='block'}
    }
  };

  if(input){
    input.addEventListener('keydown', function(e){
      if(e.key==='Enter') btn && btn.click();
    });
  }
};
