window.Commands = (()=>{
  const registry=[
    {cmd:'/help',desc:'Show help'},
    {cmd:'/compact',desc:'Compact conversation context'},
    {cmd:'/clear',desc:'Clear conversation'},
    {cmd:'/model',desc:'Switch model',args:'model_name'},
    {cmd:'/theme',desc:'Switch theme',args:'dark|light|system'},
    {cmd:'/workspace',desc:'Open workspace panel'},
    {cmd:'/memory',desc:'Open memory panel'},
    {cmd:'/context',desc:'Open context panel'},
    {cmd:'/skills',desc:'Open skills panel'},
    {cmd:'/crons',desc:'Open crons panel'},
    {cmd:'/settings',desc:'Open settings'},
    {cmd:'/export',desc:'Export session as JSON'},
    {cmd:'/solo',desc:'Toggle SOLO approval mode'},
  ];

  function getMatches(input){
    if(!input.startsWith('/'))return[];
    return registry.filter(c=>c.cmd.startsWith(input.toLowerCase()));
  }

  function renderAutocomplete(input){
    const dd=UI.$('slash-dropdown');
    const matches=getMatches(input);
    if(!matches.length){dd.classList.add('hidden');return}
    dd.innerHTML='';
    dd.classList.remove('hidden');
    matches.forEach(m=>{
      const item=UI.el('div','slash-item');
      item.innerHTML='<strong>'+m.cmd+'</strong> <span style="color:var(--text3);font-size:11px">'+m.desc+'</span>';
      item.onclick=()=>{
        UI.$('chat-input').value=m.cmd+' ';
        dd.classList.add('hidden');
        UI.$('chat-input').focus();
      };
      dd.appendChild(item);
    });
  }

  async function execute(cmd,args){
    switch(cmd){
      case'/help':Messages.addSystemMessage(registry.map(c=>c.cmd+' - '+c.desc).join('<br>'));break;
      case'/compact':try{await Api.compactSession();Messages.addSystemMessage('Compacted')}catch(e){UI.showToast(e.message,'error')};break;
      case'/clear':UI.$('chat-messages').innerHTML='';break;
      case'/model':if(args){UI.$('model-selector').value=args;UI.showToast('Model switched to '+args)}break;
      case'/theme':document.documentElement.dataset.theme=args;localStorage.setItem('cowd-theme',args);UI.showToast('Theme: '+args);break;
      case'/workspace':Workspace.render();break;
      case'/memory':Panels.renderMemory();break;
      case'/context':Panels.renderContext();break;
      case'/skills':Panels.renderSkills();break;
      case'/crons':Panels.renderCrons();break;
      case'/settings':Panels.renderSettings();break;
      case'/solo':try{await Api.toggleSolo();UI.showToast('SOLO toggled','success')}catch(e){UI.showToast(e.message,'error')};break;
      case'/export':exportSession();break;
      default:Messages.send(cmd+' '+(args||''));break;
    }
  }

  async function exportSession(){
    try{
      const msgs=await Api.getMessages();
      const blob=new Blob([JSON.stringify(msgs,null,2)],{type:'application/json'});
      const a=document.createElement('a');
      a.href=URL.createObjectURL(blob);
      a.download='session-'+(Api.sid||'export').slice(0,8)+'.json';
      a.click();
    }catch(e){UI.showToast(e.message,'error')}
  }

  return{registry,renderAutocomplete,execute,getMatches};
})();
