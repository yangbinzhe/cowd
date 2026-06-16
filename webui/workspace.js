window.Workspace = (()=>{
  let currentDir='';
  let fileTree=[];

  async function render(dir){
    if(dir!==undefined)currentDir=dir||'';
    const cont=UI.$('panel-content');
    cont.innerHTML='';
    cont.classList.add('workspace-page');
    const panel=UI.$('right-panel');
    if(panel)panel.classList.remove('hidden');
    document.querySelectorAll('#panel-tabs button[data-panel]').forEach(function(btn){
      btn.classList.toggle('tab-active',btn.dataset.panel==='workspace');
    });

    const hero=UI.el('div','workspace-hero');
    const heroText=UI.el('div','workspace-hero-text');
    heroText.innerHTML='<h3>Workspace</h3><p>Browse, preview, and create files in the active Cowd workspace.</p>';
    const pathBadge=UI.el('div','workspace-path');
    pathBadge.textContent=currentDir||'/ (root)';
    hero.appendChild(heroText);
    hero.appendChild(pathBadge);
    cont.appendChild(hero);

    const actions=UI.el('div','workspace-actions');
    const newFileBtn=UI.el('button','btn-secondary');
    newFileBtn.textContent='+ New File';
    newFileBtn.onclick=createFilePrompt;
    const refreshBtn=UI.el('button','btn-secondary');
    refreshBtn.textContent='Refresh';
    refreshBtn.onclick=()=>render();
    actions.appendChild(newFileBtn);
    actions.appendChild(refreshBtn);
    cont.appendChild(actions);

    const treeSec=UI.el('div','workspace-list-section');
    treeSec.innerHTML='<div class="workspace-list-head"><h3>Files</h3><span>Type</span></div>';
    const tree=UI.el('div','workspace-file-list');
    tree.id='file-tree';
    cont.appendChild(treeSec);
    treeSec.appendChild(tree);

    try{
      const data=await Api.listFiles(currentDir);
      fileTree=data.files||data||[];
      renderTree(tree,fileTree);
    }catch(e){
      tree.innerHTML='<div class="panel-empty">'+UI.esc(e.message)+'</div>';
    }
  }

  function renderTree(parent,items){
    if(!items.length){
      parent.appendChild(UI.el('div','panel-empty','No files in this directory'));
      return;
    }
    items.forEach(f=>{
      const isDir=!!(f.is_dir||f.type==='dir');
      const item=UI.el('div','workspace-file-row');
      const icon=UI.el('span','workspace-file-kind');
      icon.textContent=isDir?'DIR':'FILE';
      const info=UI.el('span','workspace-file-info');
      const name=UI.el('b');
      name.textContent=f.name||f.path||'';
      const path=UI.el('small');
      path.textContent=(currentDir?currentDir+'/':'')+(f.name||f.path||'');
      info.appendChild(name);
      info.appendChild(path);
      item.appendChild(icon);
      item.appendChild(info);
      if(isDir){
        item.onclick=()=>render((currentDir?currentDir+'/':'')+(f.name||f.path));
        item.style.cursor='pointer';
      }else{
        item.onclick=()=>previewFile(f);
      }
      parent.appendChild(item);
    });
  }

  async function previewFile(f){
    const path=(currentDir?currentDir+'/':'')+(f.name||f.path);
    const cont=UI.$('panel-content');
    const sec=UI.el('div','panel-section');
    sec.innerHTML='<h3>'+UI.esc(path)+'</h3>';
    const pre=UI.el('pre');
    pre.style.cssText='max-height:300px;overflow:auto;background:var(--bg);padding:12px;border-radius:var(--radius);font-size:12px';
    pre.textContent='Loading...';
    sec.appendChild(pre);
    cont.appendChild(sec);
    try{
      const r=await Api.getRawFile(path);
      const text=await r.text();
      pre.textContent=text;
      const ext=(path.split('.').pop()||'').toLowerCase();
      if(['png','jpg','jpeg','gif','svg','webp'].includes(ext)){
        const img=UI.el('img');
        img.src=URL.createObjectURL(await r.blob());
        img.style.maxWidth='100%';
        sec.appendChild(img);
      }
    }catch(e){
      pre.textContent='Error: '+e.message;
    }
  }

  function createFilePrompt(){
    const cont=UI.$('panel-content');
    const sec=UI.el('div','panel-section');
    sec.innerHTML='<h3>New File</h3>';
    const form=UI.el('div','panel-form');
    const nameInput=UI.el('input');
    nameInput.placeholder='path/relative/to/workspace/filename.ext';
    const contentArea=UI.el('textarea');
    contentArea.rows=5;
    contentArea.placeholder='File content...';
    const btn=UI.el('button','btn-primary');
    btn.textContent='Create';
    btn.onclick=async()=>{
      try{await Api.createFile(nameInput.value,contentArea.value);UI.showToast('File created','success');render()}catch(e){UI.showToast(e.message,'error')}
    };
    form.appendChild(nameInput);
    form.appendChild(contentArea);
    form.appendChild(btn);
    sec.appendChild(form);
    cont.appendChild(sec);
  }

  return{render};
})();
