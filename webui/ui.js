window.UI = (()=>{
  let activePanel = null, activeCC = null;

  function $(id){return document.getElementById(id)}
  function el(tag,cls,html){const e=document.createElement(tag);if(cls)e.className=cls;if(html)e.innerHTML=html;return e}
  function clear(id){const e=$(id);if(e)e.innerHTML=''}

  function showToast(msg,type=''){
    const t=$( 'toast');
    const d=el('div','toast-msg '+(type||''));
    d.textContent=msg;t.appendChild(d);
    setTimeout(()=>d.remove(),3500);
  }

  function renderMd(text){
    if(!text)return'';
    let h=text.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    h=h.replace(/```(\w*)\n([\s\S]*?)```/g,function(_,lang,code){
      return'<pre><code class="language-'+(lang||'text')+'">'+code.replace(/^\n/,'')+'</code></pre>';
    });
    h=h.replace(/`([^`]+)`/g,'<code>$1</code>');
    h=h.replace(/^\#{1,6}\s+(.+)$/gm,function(_,t){const l=_.match(/^#+/)[0].length;return'<h'+(l+2)+'>'+t+'</h'+(l+2)+'>'});
    h=h.replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>');
    h=h.replace(/\*(.+?)\*/g,'<em>$1</em>');
    h=h.replace(/\[MEDIA:([^\]]+)\]/g,'<div class="media-block"><img src="$1" loading="lazy" style="max-width:100%;border-radius:var(--radius);cursor:zoom-in" onclick="UI.previewMedia(\'$1\')" onerror="this.style.display=\'none\'"></div>');
    h=h.replace(/!\[([^\]]*)\]\(([^)]+)\)/g,'<div class="media-block"><img src="$2" alt="$1" loading="lazy" style="max-width:100%;border-radius:var(--radius);cursor:zoom-in" onclick="UI.previewMedia(\'$2\')" onerror="this.style.display=\'none\'"></div>');
    h=h.replace(/\[([^\]]+)\]\(([^)]+)\)/g,'<a href="$2" target="_blank">$1</a>');
    h=h.replace(/^\s*[-*]\s+(.+)$/gm,'<li>$1</li>');
    h=h.replace(/((?:<li>.*<\/li>\n?)+)/g,'<ul>$1</ul>');
    h=h.replace(/^>\s?(.+)$/gm,'<blockquote>$1</blockquote>');
    h=h.replace(/\$\$([\s\S]*?)\$\$/g,function(_,tex){
      try{return window.katex?katex.renderToString(tex,{displayMode:true,throwOnError:false}):'<pre>'+tex+'</pre>'}catch(e){return'<pre>'+tex+'</pre>'}
    });
    h=h.replace(/\$(.+?)\$/g,function(_,tex){
      try{return window.katex?katex.renderToString(tex,{throwOnError:false}):'<em>'+tex+'</em>'}catch(e){return'<em>'+tex+'</em>'}
    });
    h=h.replace(/\n{2,}/g,'</p><p>');
    h='<p>'+h+'</p>';
    h=h.replace(/<p>\s*<\/p>/g,'');
    return h;
  }

  function previewMedia(url){
    const overlay=el('div');
    overlay.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:300;display:flex;align-items:center;justify-content:center;cursor:zoom-out';
    overlay.onclick=function(){overlay.remove()};
    const img=el('img');
    img.src=url;img.style.cssText='max-width:95vw;max-height:95vh;border-radius:var(--radius-lg)';
    overlay.appendChild(img);
    document.body.appendChild(overlay);
  }

  function highlightCode(el){
    if(window.Prism)Prism.highlightAllUnder(el);
  }

  function addToolCard(id,name,status){
    const wrapper=el('div','tool-card');
    wrapper.id='tool-'+id;
    const hdr=el('div','tool-card-header');
    const st=status||'running';
    hdr.innerHTML='<span class="tool-name">&xutri; '+esc(name)+'</span><span class="tool-status '+esc(st)+'">'+esc(st)+'</span>';
    hdr.onclick=function(){
      const body=wrapper.querySelector('.tool-card-body');
      body.classList.toggle('collapsed');
    };
    const body=el('div','tool-card-body');
    body.textContent='Working...';
    wrapper.appendChild(hdr);
    wrapper.appendChild(body);
    return wrapper;
  }

  function updateToolCard(id,output,status){
    const card=$('tool-'+id);
    if(!card)return;
    if(status==='error')card.classList.add('error');
    else card.classList.remove('error');
    const st=card.querySelector('.tool-status');
    const next=status||'complete';
    st.textContent=next;
    st.className='tool-status '+next;
    const body=card.querySelector('.tool-card-body');
    if(output)body.innerHTML='<pre>'+esc(output)+'</pre>';
  }

  function addThinkCard(content){
    const wrapper=el('div','think-card');
    const hdr=el('div','think-card-header');
    hdr.innerHTML='&xodot; Thinking';
    hdr.onclick=function(){
      const body=wrapper.querySelector('.think-card-body');
      body.classList.toggle('collapsed');
    };
    const body=el('div','think-card-body collapsed');
    body.textContent=content;
    wrapper.appendChild(hdr);
    wrapper.appendChild(body);
    return wrapper;
  }

  function switchPanel(name){
    if(activePanel===name){$('right-panel').classList.add('hidden');activePanel=null;return}
    const panel=$('right-panel');
    panel.classList.remove('hidden');
    activePanel=name;
    document.querySelectorAll('#panel-tabs button[data-panel]').forEach(b=>{
      b.classList.toggle('tab-active',b.dataset.panel===name);
    });
    if(typeof Workspace!=='undefined'&&name==='workspace')Workspace.render();
    if(typeof Panels!=='undefined'){
      if(name==='memory')Panels.renderMemory();
      else if(name==='context')Panels.renderContext();
      else if(name==='skills')Panels.renderSkills();
      else if(name==='crons')Panels.renderCrons();
      else if(name==='agents')Panels.renderAgents();
      else if(name==='tools')Panels.renderTools();
      else if(name==='gateway')Panels.renderGateway();
      else if(name==='audit')Panels.renderAudit();
      else if(name==='settings')Panels.renderSettings();
    }
  }

  function openModal(id){
    $(id).classList.remove('hidden');
  }
  function closeModal(id){
    $(id).classList.add('hidden');
  }

  function switchCCTab(name){
    activeCC=name;
    document.querySelectorAll('#control-center .modal-tabs button').forEach(b=>{
      b.classList.toggle('active',b.dataset.cc===name);
    });
    if(typeof Panels!=='undefined'){
      if(name==='config')Panels.renderCCConfig();
      else if(name==='providers')Panels.renderCCProviders();
      else if(name==='approval')Panels.renderCCApproval();
      else if(name==='history')Panels.renderCCHistory();
      else if(name==='usage')Panels.renderCCUsage();
    }
  }

  function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

  return {$,el,clear,showToast,renderMd,highlightCode,previewMedia,addToolCard,updateToolCard,addThinkCard,switchPanel,openModal,closeModal,switchCCTab,esc};
})();
