window.Workspace = (()=>{
  let currentDir='';
  let fileTree=[];

  async function render(dir){
    if(dir!==undefined)currentDir=dir||'';
    const cont=UI.$('panel-content');
    cont.innerHTML='';
    UI.switchPanel('workspace');

    const bread=UI.el('div','panel-section');
    bread.innerHTML='<h3>Workspace</h3>';
    const bc=UI.el('div');bc.style.cssText='font-size:11px;color:var(--text3);padding:4px 0';
    bc.textContent=currentDir||'/ (root)';
    bread.appendChild(bc);
    cont.appendChild(bread);

    const actions=UI.el('div','panel-section');
    const newFileBtn=UI.el('button','btn-secondary');
    newFileBtn.textContent='+ New File';
    newFileBtn.onclick=createFilePrompt;
    const refreshBtn=UI.el('button','btn-secondary');
    refreshBtn.textContent='Refresh';
    refreshBtn.onclick=()=>render();
    refreshBtn.style.marginLeft='8px';
    actions.appendChild(newFileBtn);
    actions.appendChild(refreshBtn);
    cont.appendChild(actions);

    const treeSec=UI.el('div','panel-section');
    treeSec.innerHTML='<h3>Files</h3>';
    const tree=UI.el('div');
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
    items.forEach(f=>{
      const item=UI.el('div','panel-item');
      const icon=UI.el('span','pi-icon');
      icon.textContent=f.is_dir?'&xodot; ':'&xodot; ';
      const name=UI.el('span','pi-name');
      name.textContent=f.name||f.path||'';
      item.appendChild(icon);item.appendChild(name);
      if(f.is_dir||f.type==='dir'){
        item.onclick=()=>render((currentDir?currentDir+'/':'')+(f.name||f.path));
        item.style.cursor='pointer';
        item.style.fontWeight='600';
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
